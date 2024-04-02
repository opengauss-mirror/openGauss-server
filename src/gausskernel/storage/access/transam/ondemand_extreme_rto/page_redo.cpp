/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * page_redo.cpp
 * PageRedoWorker is a thread of execution that replays data page logs.
 * It provides a synchronization mechanism for replaying logs touching
 * multiple pages.
 *
 * In the current implementation, logs modifying the same page must
 * always be replayed by the same worker.  There is no mechanism for
 * an idle worker to "steal" work from a busy worker.
 *
 * IDENTIFICATION
 * src/gausskernel/storage/access/transam/parallel_recovery/page_redo.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <stdio.h>
#include <string.h>

#include "postgres.h"
#include "knl/knl_variable.h"
#include "gs_thread.h"
#include "miscadmin.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xlogutils.h"
#include "access/xlogproc.h"
#include "access/nbtree.h"
#include "catalog/storage_xlog.h"
#include "ddes/dms/ss_dms_recovery.h"
#include "ddes/dms/ss_dms_bufmgr.h"
#include "gssignal/gs_signal.h"
#include "libpq/pqsignal.h"
#include "postmaster/postmaster.h"
#include "storage/buf/bufmgr.h"
#include "storage/ipc.h"
#include "storage/freespace.h"
#include "storage/smgr/smgr.h"
#include "storage/smgr/relfilenode_hash.h"
#include "storage/standby.h"
#include "storage/pmsignal.h"
#include "utils/guc.h"
#include "utils/palloc.h"
#include "portability/instr_time.h"
#include "postmaster/startup.h"
#include "postmaster/pagerepair.h"
#include "catalog/storage.h"
#include <pthread.h>
#include <sched.h>
#include "commands/dbcommands.h"
#include "commands/tablespace.h"
#include "access/ondemand_extreme_rto/page_redo.h"
#include "access/ondemand_extreme_rto/dispatcher.h"
#include "access/ondemand_extreme_rto/txn_redo.h"
#include "access/ondemand_extreme_rto/xlog_read.h"
#include "access/ondemand_extreme_rto/redo_utils.h"
#include "pgstat.h"
#include "access/ondemand_extreme_rto/batch_redo.h"
#include "access/multi_redo_api.h"
#include "replication/walreceiver.h"
#include "replication/datareceiver.h"
#include "pgxc/barrier.h"
#include "storage/file/fio_device.h"
#ifdef ENABLE_MOT
#include "storage/mot/mot_fdw.h"
#endif

#ifdef EXTREME_RTO_DEBUG
#include <execinfo.h>
#include <stdio.h>

#include <stdlib.h>

#include <unistd.h>

#endif

#ifdef ENABLE_UT
#include "utils/utesteventutil.h"
#define STATIC
#else
#define STATIC static
#endif

namespace ondemand_extreme_rto {
static const int MAX_PARSE_BUFF_NUM = PAGE_WORK_QUEUE_SIZE * 10 * 3;
static const int MAX_LOCAL_BUFF_NUM = PAGE_WORK_QUEUE_SIZE * 10 * 3;

static const char *const PROCESS_TYPE_CMD_ARG = "--forkpageredo";
static char g_AUXILIARY_TYPE_CMD_ARG[16] = {0};

THR_LOCAL PageRedoWorker *g_redoWorker = NULL;
THR_LOCAL RecordBufferState *g_recordbuffer = NULL;
RedoItem g_redoEndMark = { false, false, NULL, 0, NULL, 0 };
RedoItem g_terminateMark = { false, false, NULL, 0, NULL, 0 };
RedoItem g_GlobalLsnForwarder;
RedoItem g_cleanupMark;
RedoItem g_closefdMark;
RedoItem g_cleanInvalidPageMark;
RedoItem g_forceDistributeMark;
RedoItem g_hashmapPruneMark;

uint32 g_ondemandXLogParseMemFullValue = 0;
uint32 g_ondemandXLogParseMemApproachFullVaule = 0;
uint32 g_ondemandRealtimeBuildQueueFullValue = 0;

static const int PAGE_REDO_WORKER_ARG = 3;
static const int REDO_SLEEP_50US = 50;
static const int REDO_SLEEP_100US = 100;

static void ApplySinglePageRecord(RedoItem *);
static void InitGlobals();
static void LastMarkReached();
static void SetupSignalHandlers();
static void SigHupHandler(SIGNAL_ARGS);
static ThreadId StartWorkerThread(PageRedoWorker *);

void RedoThrdWaitForExit(const PageRedoWorker *wk);
void AddRefRecord(void *rec);
void SubRefRecord(void *rec);
void GlobalLsnUpdate();
static void TrxnMangerQueueCallBack();
#ifdef USE_ASSERT_CHECKING
void RecordBlockCheck(void *rec, XLogRecPtr curPageLsn, uint32 blockId, bool replayed);
#endif
void AddRecordReadBlocks(void *rec, uint32 readblocks);

static void AddTrxnHashmap(void *item);
static void AddSegHashmap(void *item);
static bool checkBlockRedoDoneFromHashMap(LWLock *lock, HTAB *hashMap, RedoItemTag redoItemTag, bool *getSharedLock);
static bool tryLockHashMap(LWLock *lock, HTAB *hashMap, RedoItemTag redoItemTag, bool *noNeedRedo);
static void PageManagerPruneIfRealtimeBuildFailover();
static void RealtimeBuildReleaseRecoveryLatch(int code, Datum arg);
static void OnDemandPageManagerRedoSegParseState(XLogRecParseState *preState);

RefOperate recordRefOperate = {
    AddRefRecord,
    SubRefRecord,
#ifdef USE_ASSERT_CHECKING
    RecordBlockCheck,
#endif
    AddRecordReadBlocks,
};

static void OndemandUpdateXLogParseMemUsedBlkNum()
{
    uint32 usedblknum = 0;
    for (uint32 i = 0; i < g_dispatcher->pageLineNum; i++) {
        usedblknum += pg_atomic_read_u32(&g_dispatcher->pageLines[i].batchThd->parseManager.memctl.usedblknum);
    }
    pg_atomic_write_u32(&g_dispatcher->parseManager.memctl.usedblknum, usedblknum);
}

static inline bool OndemandXLogParseMemFull()
{
    return (pg_atomic_read_u32(&g_dispatcher->parseManager.memctl.usedblknum) > g_ondemandXLogParseMemFullValue);
}

static inline bool OndemandXLogParseMemApproachFull()
{
    return (pg_atomic_read_u32(&g_dispatcher->parseManager.memctl.usedblknum) > g_ondemandXLogParseMemApproachFullVaule);
}

static inline bool OndemandTrxnQueueFullInRealtimeBuild()
{
    return (SPSCGetQueueCount(g_dispatcher->trxnQueue) > g_ondemandRealtimeBuildQueueFullValue);
}

static inline bool OndemandSegQueueFullInRealtimeBuild()
{
    return (SPSCGetQueueCount(g_dispatcher->segQueue) > g_ondemandRealtimeBuildQueueFullValue);
}

void UpdateRecordGlobals(RedoItem *item, HotStandbyState standbyState)
{
    t_thrd.xlog_cxt.ReadRecPtr = item->record.ReadRecPtr;
    t_thrd.xlog_cxt.EndRecPtr = item->record.EndRecPtr;
    t_thrd.xlog_cxt.expectedTLIs = item->expectedTLIs;
    /* apply recoveryinfo will change standbystate see UpdateRecordGlobals */
    t_thrd.xlog_cxt.standbyState = standbyState;
    t_thrd.xlog_cxt.XLogReceiptTime = item->syncXLogReceiptTime;
    t_thrd.xlog_cxt.XLogReceiptSource = item->syncXLogReceiptSource;
    u_sess->utils_cxt.RecentXmin = item->RecentXmin;
    t_thrd.xlog_cxt.server_mode = item->syncServerMode;
}

/* Run from the dispatcher thread. */
PageRedoWorker *StartPageRedoWorker(PageRedoWorker *worker)
{
    Assert(worker);
    uint32 id = worker->id;
    ThreadId threadId = StartWorkerThread(worker);
    if (threadId == 0) {
        ereport(WARNING, (errmsg("Cannot create page-redo-worker thread: %u, %m.", id)));
        DestroyPageRedoWorker(worker);
        return NULL;
    } else {
        ereport(LOG, (errmsg("StartPageRedoWorker successfully create page-redo-worker id: %u, threadId:%lu.", id,
                             worker->tid.thid)));
    }
    g_instance.comm_cxt.predo_cxt.pageRedoThreadStatusList[id].threadId = threadId;
    SpinLockAcquire(&(g_instance.comm_cxt.predo_cxt.rwlock));
    uint32 state = pg_atomic_read_u32(&(g_instance.comm_cxt.predo_cxt.pageRedoThreadStatusList[id].threadState));
    if (state != PAGE_REDO_WORKER_READY) {
        g_instance.comm_cxt.predo_cxt.pageRedoThreadStatusList[id].threadState = PAGE_REDO_WORKER_START;
    }
    SpinLockRelease(&(g_instance.comm_cxt.predo_cxt.rwlock));
    return worker;
}

void RedoWorkerQueueCallBack()
{
    RedoInterruptCallBack();
}

bool RedoWorkerIsUndoSpaceWorker()
{
    return g_redoWorker->isUndoSpaceWorker;
}

/* Run from the dispatcher thread. */
PageRedoWorker *CreateWorker(uint32 id, bool inRealtimeBuild)
{
    PageRedoWorker *tmp = (PageRedoWorker *)palloc0(sizeof(PageRedoWorker) + ONDEMAND_EXTREME_RTO_ALIGN_LEN);
    PageRedoWorker *worker;
    worker = (PageRedoWorker *)TYPEALIGN(ONDEMAND_EXTREME_RTO_ALIGN_LEN, tmp);
    worker->selfOrinAddr = tmp;
    worker->id = id;
    worker->index = 0;
    worker->tid.thid = InvalidTid;
    worker->proc = NULL;
    worker->initialServerMode = (ServerMode)t_thrd.xlog_cxt.server_mode;
    worker->initialTimeLineID = t_thrd.xlog_cxt.ThisTimeLineID;
    worker->expectedTLIs = t_thrd.xlog_cxt.expectedTLIs;
    worker->recoveryTargetTLI = t_thrd.xlog_cxt.recoveryTargetTLI;
    worker->recoveryRestoreCommand = t_thrd.xlog_cxt.recoveryRestoreCommand;
    worker->ArchiveRecoveryRequested = t_thrd.xlog_cxt.ArchiveRecoveryRequested;
    worker->StandbyModeRequested = t_thrd.xlog_cxt.StandbyModeRequested;
    worker->InArchiveRecovery = t_thrd.xlog_cxt.InArchiveRecovery;
    worker->InRecovery = t_thrd.xlog_cxt.InRecovery;
    worker->ArchiveRestoreRequested = t_thrd.xlog_cxt.ArchiveRestoreRequested;
    worker->minRecoveryPoint = t_thrd.xlog_cxt.minRecoveryPoint;

    worker->pendingHead = NULL;
    worker->pendingTail = NULL;
    worker->queue = SPSCBlockingQueueCreate(PAGE_WORK_QUEUE_SIZE, RedoWorkerQueueCallBack);
    worker->lastCheckedRestartPoint = InvalidXLogRecPtr;
    worker->lastReplayedEndRecPtr = InvalidXLogRecPtr;
    worker->standbyState = (HotStandbyState)t_thrd.xlog_cxt.standbyState;
    worker->StandbyMode = t_thrd.xlog_cxt.StandbyMode;
    worker->latestObservedXid = t_thrd.storage_cxt.latestObservedXid;
    worker->DataDir = t_thrd.proc_cxt.DataDir;
    worker->RecentXmin = u_sess->utils_cxt.RecentXmin;
    worker->xlogInvalidPages = NULL;
    worker->redoItemHashCtrl = NULL;
    PosixSemaphoreInit(&worker->phaseMarker, 0);
    worker->oldCtx = NULL;
    worker->fullSyncFlag = 0;
#if (!defined __x86_64__) && (!defined __aarch64__)
    SpinLockInit(&worker->ptrLck);
#endif
    worker->parseManager.memctl.isInit = false;
    worker->parseManager.parsebuffers = NULL;

    worker->nextPrunePtr = InvalidXLogRecPtr;
    worker->inRealtimeBuild = inRealtimeBuild;
    worker->currentHtabBlockNum = 0;
    return worker;
}

/* Run from the dispatcher thread. */
static ThreadId StartWorkerThread(PageRedoWorker *worker)
{
    worker->tid.thid = initialize_util_thread(PAGEREDO, worker);
    return worker->tid.thid;
}

/* Run from the dispatcher thread. */
void DestroyPageRedoWorker(PageRedoWorker *worker)
{
    PosixSemaphoreDestroy(&worker->phaseMarker);
    SPSCBlockingQueueDestroy(worker->queue);
    XLogRedoBufferDestoryFunc(&(worker->bufferManager));
    XLogParseBufferDestoryFunc(&(worker->parseManager));
    pfree(worker->selfOrinAddr);
}

/* automic write for lastReplayedReadRecPtr and lastReplayedEndRecPtr */
void SetCompletedReadEndPtr(PageRedoWorker *worker, XLogRecPtr readPtr, XLogRecPtr endPtr)
{
    volatile PageRedoWorker *tmpWk = worker;
#if defined(__x86_64__) || defined(__aarch64__)
    uint128_u exchange;
    uint128_u current;
    uint128_u compare = atomic_compare_and_swap_u128((uint128_u *)&tmpWk->lastReplayedReadRecPtr);

    Assert(sizeof(tmpWk->lastReplayedReadRecPtr) == 8);
    Assert(sizeof(tmpWk->lastReplayedEndRecPtr) == 8);

    exchange.u64[0] = (uint64)readPtr;
    exchange.u64[1] = (uint64)endPtr;

loop:
    current = atomic_compare_and_swap_u128((uint128_u *)&tmpWk->lastReplayedReadRecPtr, compare, exchange);
    if (!UINT128_IS_EQUAL(compare, current)) {
        UINT128_COPY(compare, current);
        goto loop;
    }
#else
    SpinLockAcquire(&tmpWk->ptrLck);
    tmpWk->lastReplayedReadRecPtr = readPtr;
    tmpWk->lastReplayedEndRecPtr = endPtr;
    SpinLockRelease(&tmpWk->ptrLck);
#endif /* __x86_64__ || __aarch64__ */
}

/* automic write for lastReplayedReadRecPtr and lastReplayedEndRecPtr */
void GetCompletedReadEndPtr(PageRedoWorker *worker, XLogRecPtr *readPtr, XLogRecPtr *endPtr)
{
    volatile PageRedoWorker *tmpWk = worker;
#if defined(__x86_64__) || defined(__aarch64__)
    uint128_u compare = atomic_compare_and_swap_u128((uint128_u *)&tmpWk->lastReplayedReadRecPtr);
    Assert(sizeof(tmpWk->lastReplayedReadRecPtr) == 8);
    Assert(sizeof(tmpWk->lastReplayedEndRecPtr) == 8);

    *readPtr = (XLogRecPtr)compare.u64[0];
    *endPtr = (XLogRecPtr)compare.u64[1];
#else
    SpinLockAcquire(&tmpWk->ptrLck);
    *readPtr = tmpWk->lastReplayedReadRecPtr;
    *endPtr = tmpWk->lastReplayedEndRecPtr;
    SpinLockRelease(&tmpWk->ptrLck);
#endif /* __x86_64__ || __aarch64__ */
}

/* Run from both the dispatcher and the worker thread. */
bool IsPageRedoWorkerProcess(int argc, char *argv[])
{
    return strcmp(argv[1], PROCESS_TYPE_CMD_ARG) == 0;
}

/* Run from the worker thread. */
void AdaptArgvForPageRedoWorker(char *argv[])
{
    if (g_AUXILIARY_TYPE_CMD_ARG[0] == 0)
        sprintf_s(g_AUXILIARY_TYPE_CMD_ARG, sizeof(g_AUXILIARY_TYPE_CMD_ARG), "-x%d", PageRedoProcess);
    argv[3] = g_AUXILIARY_TYPE_CMD_ARG;
}

/* Run from the worker thread. */
void GetThreadNameIfPageRedoWorker(int argc, char *argv[], char **threadNamePtr)
{
    if (*threadNamePtr == NULL && IsPageRedoWorkerProcess(argc, argv))
        *threadNamePtr = "PageRedoWorker";
}

/* Run from the worker thread. */
uint32 GetMyPageRedoWorkerIdWithLock()
{
    bool isWorkerStarting = false;
    SpinLockAcquire(&(g_instance.comm_cxt.predo_cxt.rwlock));
    isWorkerStarting = ((g_instance.comm_cxt.predo_cxt.state == REDO_STARTING_BEGIN) ? true : false);
    SpinLockRelease(&(g_instance.comm_cxt.predo_cxt.rwlock));
    if (!isWorkerStarting) {
        ereport(WARNING, (errmsg("GetMyPageRedoWorkerIdWithLock Page-redo-worker exit.")));
        proc_exit(0);
    }

    return g_redoWorker->id;
}

/* Run from any worker thread. */
PGPROC *GetPageRedoWorkerProc(PageRedoWorker *worker)
{
    return worker->proc;
}

void HandlePageRedoInterrupts()
{
    if (t_thrd.page_redo_cxt.got_SIGHUP) {
        t_thrd.page_redo_cxt.got_SIGHUP = false;
        ProcessConfigFile(PGC_SIGHUP);
    }

    if (t_thrd.page_redo_cxt.shutdown_requested) {
        ereport(LOG, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                      errmsg("page worker id %u exit for request", g_redoWorker->id)));

        pg_atomic_write_u32(&(g_instance.comm_cxt.predo_cxt.pageRedoThreadStatusList[g_redoWorker->id].threadState),
                            PAGE_REDO_WORKER_EXIT);

        proc_exit(1);
    }
}

void ReferenceRedoItem(void *item)
{
    RedoItem *redoItem = (RedoItem *)item;
    AddRefRecord(&redoItem->record);
}

void DereferenceRedoItem(void *item)
{
    RedoItem *redoItem = (RedoItem *)item;
    SubRefRecord(&redoItem->record);
}

void ReferenceRecParseState(XLogRecParseState *recordstate)
{
    ParseBufferDesc *descstate = (ParseBufferDesc *)((char *)recordstate - sizeof(ParseBufferDesc));
    (void)pg_atomic_fetch_add_u32(&(descstate->refcount), 1);
}

void DereferenceRecParseState(XLogRecParseState *recordstate)
{
    ParseBufferDesc *descstate = (ParseBufferDesc *)((char *)recordstate - sizeof(ParseBufferDesc));
    (void)pg_atomic_fetch_sub_u32(&(descstate->refcount), 1);
}

#define STRUCT_CONTAINER(type, membername, ptr) ((type *)((char *)(ptr)-offsetof(type, membername)))

#ifdef USE_ASSERT_CHECKING
void RecordBlockCheck(void *rec, XLogRecPtr curPageLsn, uint32 blockId, bool replayed)
{
    XLogReaderState *record = (XLogReaderState *)rec;
    if (record->blocks[blockId].forknum != MAIN_FORKNUM) {
        return;
    }

    if (replayed) {
        uint32 rmid = XLogRecGetRmid(record);
        uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

        if (curPageLsn == InvalidXLogRecPtr && (rmid == RM_HEAP2_ID || rmid == RM_HEAP_ID || rmid == RM_HEAP3_ID)) {
            uint32 shiftSize = 32;
            ereport(LOG, (errmsg("pass checked, record lsn:%X/%X, type: %u %u", 
                static_cast<uint32>(record->EndRecPtr >> shiftSize), static_cast<uint32>(record->EndRecPtr),
                record->decoded_record->xl_rmid, record->decoded_record->xl_info)));
        } else if (!(rmid == RM_HEAP2_ID && info == XLOG_HEAP2_VISIBLE) &&
            !(rmid == RM_HEAP_ID && info == XLOG_HEAP_NEWPAGE)) {
            Assert(XLByteLE(record->EndRecPtr, curPageLsn));
        }
    }

    Assert(blockId < (XLR_MAX_BLOCK_ID + 1));
    record->blocks[blockId].replayed = 1;
}

#endif

void AddRecordReadBlocks(void *rec, uint32 readblocks)
{
    XLogReaderState *record = (XLogReaderState *)rec;
    record->readblocks += readblocks;
}

void AddRefRecord(void *rec)
{
    pg_memory_barrier();
#ifndef EXTREME_RTO_DEBUG
    (void)pg_atomic_fetch_add_u32(&((XLogReaderState *)rec)->refcount, 1);
#else
    uint32 relCount = pg_atomic_fetch_add_u32(&((XLogReaderState *)rec)->refcount, 1);

    const int stack_size = 5;
    const int max_out_put_buf = 4096;
    void *buffer[stack_size];
    int nptrs;
    char output[max_out_put_buf];
    char **strings;
    nptrs = backtrace(buffer, stack_size);
    strings = backtrace_symbols(buffer, nptrs);

    int ret = sprintf_s(output, sizeof(output), "before add relcount %u lsn %X/%X call back trace: \n", relCount,
                        (uint32)(((XLogReaderState *)rec)->EndRecPtr >> 32),
                        (uint32)(((XLogReaderState *)rec)->EndRecPtr));
    securec_check_ss_c(ret, "\0", "\0");
    for (int i = 0; i < nptrs; ++i) {
        ret = strcat_s(output, max_out_put_buf - strlen(output), strings[i]);
        securec_check_ss_c(ret, "\0", "\0");
        ret = strcat_s(output, max_out_put_buf - strlen(output), "\n");
        securec_check_ss_c(ret, "\0", "\0");
    }

    free(strings);
    ereport(LOG, (errcode(ERRCODE_DATA_CORRUPTED), errmsg(" AddRefRecord print: %s", output)));

#endif
}

void SubRefRecord(void *rec)
{
    pg_memory_barrier();
    Assert(((XLogReaderState *)rec)->refcount != 0);
    uint32 relCount = pg_atomic_sub_fetch_u32(&((XLogReaderState *)rec)->refcount, 1);
#ifdef EXTREME_RTO_DEBUG
    const int stack_size = 5;
    const int max_out_put_buf = 4096;
    void *buffer[stack_size];
    int nptrs;
    char output[max_out_put_buf];
    char **strings;
    nptrs = backtrace(buffer, stack_size);
    strings = backtrace_symbols(buffer, nptrs);

    int ret = sprintf_s(output, sizeof(output), "after sub relcount %u lsn %X/%X call back trace:\n", relCount,
                        (uint32)(((XLogReaderState *)rec)->EndRecPtr >> 32),
                        (uint32)(((XLogReaderState *)rec)->EndRecPtr));
    securec_check_ss_c(ret, "\0", "\0");
    for (int i = 0; i < nptrs; ++i) {
        ret = strcat_s(output, max_out_put_buf - strlen(output), strings[i]);
        securec_check_ss_c(ret, "\0", "\0");
        ret = strcat_s(output, max_out_put_buf - strlen(output), "\n");
        securec_check_ss_c(ret, "\0", "\0");
    }
    free(strings);
    ereport(LOG, (errcode(ERRCODE_DATA_CORRUPTED), errmsg(" SubRefRecord print: %s", output)));

#endif

    if (relCount == 0) {
        RedoItem *item = STRUCT_CONTAINER(RedoItem, record, rec);
        FreeRedoItem(item);
    }
}

bool BatchRedoParseItemAndDispatch(RedoItem *item)
{
    uint32 blockNum = 0;
    XLogRecParseState *recordblockstate = XLogParseToBlockForExtermeRTO(&item->record, &blockNum);
    if (recordblockstate == NULL) {
        if (blockNum == 0) {
            return false;
        }
        return true; /*  out of mem */
    }

    PageRedoPipeline *myRedoLine = &g_dispatcher->pageLines[g_redoWorker->slotId];
    AddPageRedoItem(myRedoLine->managerThd, recordblockstate);
    return false;
}

void BatchRedoDistributeEndMark(void)
{
    PageRedoPipeline *myRedoLine = &g_dispatcher->pageLines[g_redoWorker->slotId];
    SendPageRedoEndMark(myRedoLine->managerThd);
}

void BatchRedoProcLsnForwarder(RedoItem *lsnForwarder)
{
    SetCompletedReadEndPtr(g_redoWorker, lsnForwarder->record.ReadRecPtr, lsnForwarder->record.EndRecPtr);
    (void)pg_atomic_sub_fetch_u32(&lsnForwarder->record.refcount, 1);

    PageRedoPipeline *myRedoLine = &g_dispatcher->pageLines[g_redoWorker->slotId];
    AddPageRedoItem(myRedoLine->managerThd, lsnForwarder);
}

void BatchRedoProcCleanupMark(RedoItem *cleanupMark)
{
    PageRedoPipeline *myRedoLine = &g_dispatcher->pageLines[g_redoWorker->slotId];
    g_redoWorker->xlogInvalidPages = XLogGetInvalidPages();
    AddPageRedoItem(myRedoLine->managerThd, cleanupMark);
    ereport(LOG, (errcode(ERRCODE_LOG), errmsg("[ForceFinish]BatchRedoProcCleanupMark has cleaned InvalidPages")));
}

#ifdef ENABLE_DISTRIBUTE_TEST
// inject delay to slow the process and also can be used as UT mock stub
void InjectDelayWaitRedoPageManagerQueueEmpty()
{
    const uint32 sleepTime = 1000000;
    ereport(LOG, (errmsg("ProcessRedoPageManagerQueueEmpty sleep")));
    pg_usleep(sleepTime);
}
#endif

void WaitAllRedoWorkerQueueEmpty()
{
    if ((get_real_recovery_parallelism() <= 1) || (GetBatchCount() == 0)) {
        return;
    }
    for (uint j = 0; j < g_dispatcher->pageLineNum; ++j) {
        PageRedoPipeline *myRedoLine = &g_dispatcher->pageLines[j];

        for (uint32 i = 0; i < myRedoLine->redoThdNum; ++i) {
            while (!SPSCBlockingQueueIsEmpty(myRedoLine->redoThd[i]->queue)) {
                RedoInterruptCallBack();
            }
        }
    }
}

void BatchRedoSendMarkToPageRedoManager(RedoItem *sendMark)
{
    PageRedoPipeline *myRedoLine = &g_dispatcher->pageLines[g_redoWorker->slotId];
    AddPageRedoItem(myRedoLine->managerThd, sendMark);
}

static void BatchRedoProcIfXLogParseMemFull()
{
    while (SS_ONDEMAND_RECOVERY_HASHMAP_FULL) {
        if (SS_ONDEMAND_REALTIME_BUILD_NORMAL) {
            BatchRedoSendMarkToPageRedoManager(&g_hashmapPruneMark);
        } else {
            BatchRedoSendMarkToPageRedoManager(&g_forceDistributeMark);
        }
        RedoInterruptCallBack();
        pg_usleep(100000);     // 100 ms
    }
}

bool BatchRedoDistributeItems(void **eleArry, uint32 eleNum)
{
    bool parsecomplete = false;
    for (uint32 i = 0; i < eleNum; i++) {
        if (eleArry[i] == (void *)&g_redoEndMark) {
            return true;
        } else if (eleArry[i] == (void *)&g_GlobalLsnForwarder) {
            BatchRedoProcLsnForwarder((RedoItem *)eleArry[i]);
        } else if (eleArry[i] == (void *)&g_cleanupMark) {
            BatchRedoProcCleanupMark((RedoItem *)eleArry[i]);
        } else if (eleArry[i] == (void *)&g_closefdMark) {
            smgrcloseall();
        } else if (eleArry[i] == (void *)&g_cleanInvalidPageMark) {
            forget_range_invalid_pages((void *)eleArry[i]);
        } else {
            BatchRedoProcIfXLogParseMemFull();
            GetRedoStartTime(g_redoWorker->timeCostList[TIME_COST_STEP_3]);
            RedoItem *item = (RedoItem *)eleArry[i];
            UpdateRecordGlobals(item, g_redoWorker->standbyState);
            CountAndGetRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_3],
                g_redoWorker->timeCostList[TIME_COST_STEP_4]);
            do {
                parsecomplete = BatchRedoParseItemAndDispatch(item);
                RedoInterruptCallBack();
            } while (parsecomplete);
            CountRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_4]);
            DereferenceRedoItem(item);
        }
    }

    return false;
}

void OndemandInitXLogParseBuffer(RedoParseManager *src, RefOperate *refOperate, InterruptFunc interruptOperte)
{
    g_redoWorker->parseManager.parsebuffers = src->parsebuffers;
    g_redoWorker->parseManager.refOperate = refOperate;

    g_redoWorker->parseManager.memctl.totalblknum = src->memctl.totalblknum;
    g_redoWorker->parseManager.memctl.usedblknum = 0;
    g_redoWorker->parseManager.memctl.itemsize = src->memctl.itemsize;
    g_redoWorker->parseManager.memctl.firstfreeslot = InvalidBuffer;
    g_redoWorker->parseManager.memctl.firstreleaseslot = InvalidBuffer;
    g_redoWorker->parseManager.memctl.memslot = src->memctl.memslot;
    g_redoWorker->parseManager.memctl.doInterrupt = interruptOperte;

    g_redoWorker->parseManager.memctl.isInit = true;

    g_parseManager = &g_redoWorker->parseManager;
}

void BatchRedoMain()
{
    void **eleArry;
    uint32 eleNum;

    (void)RegisterRedoInterruptCallBack(HandlePageRedoInterrupts);
    OndemandInitXLogParseBuffer(&g_dispatcher->parseManager, &recordRefOperate, RedoInterruptCallBack);
    GetRedoStartTime(g_redoWorker->timeCostList[TIME_COST_STEP_1]);
    while (SPSCBlockingQueueGetAll(g_redoWorker->queue, &eleArry, &eleNum)) {
        CountAndGetRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_1], g_redoWorker->timeCostList[TIME_COST_STEP_2]);
        bool isEnd = BatchRedoDistributeItems(eleArry, eleNum);
        CountRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_2]);
        SPSCBlockingQueuePopN(g_redoWorker->queue, eleNum);
        if (isEnd)
            break;

        RedoInterruptCallBack();
        ADD_ABNORMAL_POSITION(1);
        GetRedoStartTime(g_redoWorker->timeCostList[TIME_COST_STEP_1]);
    }

    RedoThrdWaitForExit(g_redoWorker);
}

uint32 GetWorkerId(const RedoItemTag *redoItemTag, uint32 workerCount)
{
    if (workerCount != 0) {
        return tag_hash(redoItemTag, sizeof(RedoItemTag)) % workerCount;
    }
    return 0;
}

uint32 GetWorkerId(const uint32 attId, const uint32 workerCount)
{
    if (workerCount != 0) {
        return attId % workerCount;
    }
    return 0;
}

void RedoPageManagerDistributeToAllOneBlock(XLogRecParseState *ddlParseState)
{
    PageRedoPipeline *myRedoLine = &g_dispatcher->pageLines[g_redoWorker->slotId];
    const uint32 WorkerNumPerMng = myRedoLine->redoThdNum;

    ddlParseState->nextrecord = NULL;

    for (uint32 i = 0; i < WorkerNumPerMng; ++i) {
        XLogRecParseState *newState = XLogParseBufferCopy(ddlParseState);
        AddPageRedoItem(myRedoLine->redoThd[i], newState);
    }
}

void ReleaseRecParseState(PageRedoPipeline *myRedoLine, HTAB *redoItemHash, RedoItemHashEntry *redoItemEntry, uint32 workId)
{
    XLogRecParseState *cur_state = redoItemEntry->head;
    XLogRecParseState *releaseHeadState = redoItemEntry->head;
    XLogRecParseState *releaseTailState = NULL;
    unsigned int del_from_hash_item_num = 0;
    unsigned int new_hash;
    LWLock *xlog_partition_lock;

    /* Items that have been replayed(refcount == 0) can be released */
    while (cur_state != NULL) {
        ParseBufferDesc *descstate = (ParseBufferDesc *)((char *)cur_state - sizeof(ParseBufferDesc));
        unsigned int refCount = pg_atomic_read_u32(&descstate->refcount);

        if (refCount == 0) {
            releaseTailState = cur_state;
            del_from_hash_item_num++;
            cur_state = (XLogRecParseState *)(cur_state->nextrecord);
        } else {
            break;
        }
    }

    new_hash = XlogTrackTableHashCode(&redoItemEntry->redoItemTag);
    xlog_partition_lock = XlogTrackMappingPartitionLock(new_hash);

    if (del_from_hash_item_num > 0) {
        Assert(releaseTailState != NULL);
        (void)LWLockAcquire(xlog_partition_lock, LW_EXCLUSIVE);
        redoItemEntry->head = (XLogRecParseState *)releaseTailState->nextrecord;
        releaseTailState->nextrecord = NULL;
        XLogBlockParseStateRelease(releaseHeadState);
        redoItemEntry->redoItemNum -= del_from_hash_item_num;
        LWLockRelease(xlog_partition_lock);
    }

    if (redoItemEntry->redoItemNum == 0) {
        (void)LWLockAcquire(xlog_partition_lock, LW_EXCLUSIVE);
        if (hash_search(redoItemHash, (void *)&redoItemEntry->redoItemTag, HASH_REMOVE, NULL) == NULL) {
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("redo item hash table corrupted")));
        }
        LWLockRelease(xlog_partition_lock);
    }

    return;
}

void RedoPageManagerDistributeToRedoThd(PageRedoPipeline *myRedoLine,
    HTAB *redoItemHash, RedoItemHashEntry *redoItemEntry, uint32 workId)
{
    XLogRecParseState *cur_state = redoItemEntry->head;
    XLogRecParseState *distribute_head = NULL;
    XLogRecParseState *distribute_tail = NULL;
    int distribute_item_num = 0;

    while (cur_state != NULL) {
        if (cur_state->distributeStatus != XLOG_NO_DISTRIBUTE) {
            cur_state = (XLogRecParseState *)cur_state->nextrecord;
            continue;
        }

        if (distribute_head == NULL) {
            distribute_head = cur_state;
        }
        cur_state->distributeStatus = XLOG_MID_DISTRIBUTE;
        distribute_tail = cur_state;
        distribute_item_num++;
        cur_state = (XLogRecParseState *)cur_state->nextrecord;
    }

    if (distribute_item_num > 0) {
        distribute_head->distributeStatus = XLOG_HEAD_DISTRIBUTE;
        distribute_tail->distributeStatus = XLOG_TAIL_DISTRIBUTE;
        AddPageRedoItem(myRedoLine->redoThd[workId], distribute_head);
    }

    return;
}

static void WaitSegRedoWorkersQueueEmpty()
{
    while (!SPSCBlockingQueueIsEmpty(g_dispatcher->auxiliaryLine.segRedoThd->queue)) {
        RedoInterruptCallBack();
    }
}

void RedoPageManagerDistributeBlockRecord(XLogRecParseState *parsestate)
{
    PageManagerPruneIfRealtimeBuildFailover();
    WaitSegRedoWorkersQueueEmpty();
    PageRedoPipeline *myRedoLine = &g_dispatcher->pageLines[g_redoWorker->slotId];
    const uint32 WorkerNumPerMng = myRedoLine->redoThdNum;
    HASH_SEQ_STATUS status;
    RedoItemHashEntry *redoItemEntry = NULL;
    HTAB *curMap = g_instance.comm_cxt.predo_cxt.redoItemHashCtrl[g_redoWorker->slotId]->hTab;
    hash_seq_init(&status, curMap);

    while ((redoItemEntry = (RedoItemHashEntry *)hash_seq_search(&status)) != NULL) {
        uint32 workId = GetWorkerId(&redoItemEntry->redoItemTag, WorkerNumPerMng);
        ReleaseRecParseState(myRedoLine, curMap, redoItemEntry, workId);
        RedoPageManagerDistributeToRedoThd(myRedoLine, curMap, redoItemEntry, workId);
    }

    if (parsestate != NULL) {
        RedoPageManagerDistributeToAllOneBlock(parsestate);
    }
}

void WaitCurrentPipeLineRedoWorkersQueueEmpty()
{
    PageRedoPipeline *myRedoLine = &g_dispatcher->pageLines[g_redoWorker->slotId];
    const uint32 WorkerNumPerMng = myRedoLine->redoThdNum;

    for (uint32 i = 0; i < WorkerNumPerMng; ++i) {
        while (!SPSCBlockingQueueIsEmpty(myRedoLine->redoThd[i]->queue)) {
            RedoInterruptCallBack();
        }
    }
}

static void ReleaseReplayedInParse(PageRedoPipeline* myRedoLine, uint32 workerNum)
{
    HASH_SEQ_STATUS status;
    RedoItemHashEntry *redoItemEntry = NULL;
    HTAB *curMap = g_instance.comm_cxt.predo_cxt.redoItemHashCtrl[g_redoWorker->slotId]->hTab;
    hash_seq_init(&status, curMap);

    while ((redoItemEntry = (RedoItemHashEntry *)hash_seq_search(&status)) != NULL) {
        if (g_redoWorker->slotId == GetSlotId(redoItemEntry->redoItemTag.rNode, 0, 0, GetBatchCount())) {
            uint32 workId = GetWorkerId(&redoItemEntry->redoItemTag, workerNum);
            ReleaseRecParseState(myRedoLine, curMap, redoItemEntry, workId);
        }
    }
}

static void WaitAndTryReleaseWorkerReplayedRec(PageRedoPipeline *myRedoLine, uint32 workerNum)
{
    bool queueIsEmpty = false;
    while (!queueIsEmpty) {
        queueIsEmpty = true;
        for (uint32 i = 0; i < workerNum; i++) {
            if (!RedoWorkerIsIdle(myRedoLine->redoThd[i])) {
                queueIsEmpty = false;
                ReleaseReplayedInParse(myRedoLine, workerNum);
                pg_usleep(50000L);
                break;
            }
        }
    }
}

void PageManagerDispatchEndMarkAndWait()
{
    PageRedoPipeline *myRedoLine = &g_dispatcher->pageLines[g_redoWorker->slotId];
    const uint32 WorkerNumPerMng = get_page_redo_worker_num_per_manager();

    SendPageRedoEndMark(myRedoLine->htabThd);
    if (g_redoWorker->slotId == SEG_PROC_PIPELINE_SLOT) {
        SendPageRedoEndMark(g_dispatcher->auxiliaryLine.segRedoThd);
    }
    for (uint32 i = 0; i < WorkerNumPerMng; ++i)
        SendPageRedoEndMark(myRedoLine->redoThd[i]);

    /* Need to release the item replayed in time */
    WaitAndTryReleaseWorkerReplayedRec(myRedoLine, WorkerNumPerMng);
    for (uint32 i = 0; i < myRedoLine->redoThdNum; i++) {
        WaitPageRedoWorkerReachLastMark(myRedoLine->redoThd[i]);
    }
    if (g_redoWorker->slotId == SEG_PROC_PIPELINE_SLOT) {
        WaitPageRedoWorkerReachLastMark(g_dispatcher->auxiliaryLine.segRedoThd);
    }
    WaitPageRedoWorkerReachLastMark(myRedoLine->htabThd);

    ReleaseReplayedInParse(myRedoLine, WorkerNumPerMng);
}

void RedoPageManagerDdlAction(XLogRecParseState *parsestate)
{
    switch (parsestate->blockparse.blockhead.block_valid) {
        case BLOCK_DATA_DROP_DATABASE_TYPE:
            xlog_db_drop(parsestate->blockparse.blockhead.end_ptr, parsestate->blockparse.blockhead.dbNode,
                parsestate->blockparse.blockhead.spcNode);
            break;
        case BLOCK_DATA_CREATE_DATABASE_TYPE:
            xlog_db_create(parsestate->blockparse.blockhead.dbNode, parsestate->blockparse.blockhead.spcNode,
                           parsestate->blockparse.extra_rec.blockdatabase.src_db_id,
                           parsestate->blockparse.extra_rec.blockdatabase.src_tablespace_id);
            break;
        case BLOCK_DATA_DROP_TBLSPC_TYPE:
            xlog_drop_tblspc(parsestate->blockparse.blockhead.spcNode);
            break;
        case BLOCK_DATA_SEG_FILE_EXTEND_TYPE:
            {
                Assert(0);
            }
            break;
        case BLOCK_DATA_SEG_SPACE_DROP:
        case BLOCK_DATA_SEG_FULL_SYNC_TYPE:
        case BLOCK_DATA_SEG_EXTEND:
            {
                /* PARSE_TYPE_SEG will handle by seg worker, function SSProcSegPageCommonRedo */
                Assert(GetCurrentXLogRecParseType(parsestate) == PARSE_TYPE_DDL);
            }
            ProcSegPageCommonRedo(parsestate);
            break;
        default:
            break;
    }
}

void RedoPageManagerSmgrClose(XLogRecParseState *parsestate)
{
    switch (parsestate->blockparse.blockhead.block_valid) {
        case BLOCK_DATA_DROP_DATABASE_TYPE:
            smgrcloseall();
            break;
        case BLOCK_DATA_SEG_FULL_SYNC_TYPE:
            ProcSegPageJustFreeChildState(parsestate);
            break;
        default:
            break;
    }
}

void RedoPageManagerSyncDdlAction(XLogRecParseState *parsestate)
{
    /* at this monent, all worker queue is empty ,just find out which one will do it */
    uint32 expected = 0;
    const uint32 pipelineNum = g_dispatcher->pageLineNum;
    pg_atomic_compare_exchange_u32(&g_dispatcher->syncEnterCount, &expected, pipelineNum);
    uint32 entershareCount = pg_atomic_sub_fetch_u32(&g_dispatcher->syncEnterCount, 1);

    MemoryContext oldCtx = MemoryContextSwitchTo(g_redoWorker->oldCtx);
    if (entershareCount == 0) {
        /* do actual work */
        RedoPageManagerDdlAction(parsestate);
    } else {
        RedoPageManagerSmgrClose(parsestate);
        do {
            RedoInterruptCallBack();
            entershareCount = pg_atomic_read_u32(&g_dispatcher->syncEnterCount);
        } while (entershareCount != 0);
    }
    (void)MemoryContextSwitchTo(oldCtx);

    expected = 0;
    pg_atomic_compare_exchange_u32(&g_dispatcher->syncExitCount, &expected, pipelineNum);
    uint32 exitShareCount = pg_atomic_sub_fetch_u32(&g_dispatcher->syncExitCount, 1);
    while (exitShareCount != 0) {
        RedoInterruptCallBack();
        exitShareCount = pg_atomic_read_u32(&g_dispatcher->syncExitCount);
    }

    parsestate->nextrecord = NULL;
    XLogBlockParseStateRelease(parsestate);
}

void RedoPageManagerDoDropAction(XLogRecParseState *parsestate, HTAB *hashMap)
{
    XLogRecParseState *newState = XLogParseBufferCopy(parsestate);
    PRTrackClearBlock(newState, hashMap);
    RedoPageManagerDistributeBlockRecord(parsestate);
    WaitCurrentPipeLineRedoWorkersQueueEmpty();
    RedoPageManagerSyncDdlAction(parsestate);
}

void RedoPageManagerDoSmgrAction(XLogRecParseState *recordblockstate)
{
    RedoBufferInfo bufferinfo = {0};
    void *blockrecbody;
    XLogBlockHead *blockhead;

    blockhead = &recordblockstate->blockparse.blockhead;
    blockrecbody = &recordblockstate->blockparse.extra_rec;

    XLogBlockInitRedoBlockInfo(blockhead, &bufferinfo.blockinfo);

    MemoryContext oldCtx = MemoryContextSwitchTo(g_redoWorker->oldCtx);
    XLogBlockDdlDoSmgrAction(blockhead, blockrecbody, &bufferinfo);
    (void)MemoryContextSwitchTo(oldCtx);

    recordblockstate->nextrecord = NULL;
    XLogBlockParseStateRelease(recordblockstate);
}

void RedoPageManagerDoDataTypeAction(XLogRecParseState *parsestate, HTAB *hashMap)
{
    XLogBlockDdlParse *ddlrecparse = NULL;
    XLogBlockParseGetDdlParse(parsestate, ddlrecparse);
    
    if (ddlrecparse->blockddltype == BLOCK_DDL_DROP_RELNODE ||
        ddlrecparse->blockddltype == BLOCK_DDL_TRUNCATE_RELNODE) {
        XLogRecParseState *newState = XLogParseBufferCopy(parsestate);
        PRTrackClearBlock(newState, hashMap);
        RedoPageManagerDistributeBlockRecord(parsestate);
        WaitCurrentPipeLineRedoWorkersQueueEmpty();
    }

    RedoPageManagerDoSmgrAction(parsestate);

}

static void RedoItemHashPruneWithoutLock(HTAB *redoItemHash, RedoItemHashEntry *redoItemEntry, XLogRecPtr pruneLsn,
    bool updateStat)
{
    XLogRecParseState *cur_state = redoItemEntry->head;
    XLogRecParseState *releaseHeadState = redoItemEntry->head;
    XLogRecParseState *releaseTailState = NULL;
    unsigned int del_from_hash_item_num = 0;

    while (cur_state != NULL) {
        XLogRecPtr curRedoItemLsn = XLogBlockHeadGetLSN(&cur_state->blockparse.blockhead);
        if (XLByteLT(curRedoItemLsn, pruneLsn)) {
            releaseTailState = cur_state;
            del_from_hash_item_num++;
            cur_state = (XLogRecParseState *)(cur_state->nextrecord);
        } else {
            break;
        }
    }

    if (del_from_hash_item_num > 0) {
        if (releaseTailState != NULL) {
            redoItemEntry->head = (XLogRecParseState *)releaseTailState->nextrecord;
            releaseTailState->nextrecord = NULL;
        } else {
            redoItemEntry->head = NULL;
        }
        XLogBlockParseStateRelease(releaseHeadState);
        redoItemEntry->redoItemNum -= del_from_hash_item_num;
    }

    if (redoItemEntry->redoItemNum == 0) {
        if (hash_search(redoItemHash, (void *)&redoItemEntry->redoItemTag, HASH_REMOVE, NULL) == NULL) {
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("redo item hash table corrupted")));
        }
    }

    pg_memory_barrier();
    if ((del_from_hash_item_num > 0) && updateStat) {
        (void)pg_atomic_sub_fetch_u32(&g_dispatcher->pageLines[g_redoWorker->slotId].managerThd->currentHtabBlockNum,
            del_from_hash_item_num);
    }
}

static void PageManagerAddRedoItemToSegWorkers(RedoItem *item)
{
    if (g_redoWorker->slotId == SEG_PROC_PIPELINE_SLOT) {
        AddPageRedoItem(g_dispatcher->auxiliaryLine.segRedoThd, item);
    }
}

static void PageManagerAddRedoItemToHashMapManager(RedoItem *item)
{
    PageRedoPipeline *myRedoLine = &g_dispatcher->pageLines[g_redoWorker->slotId];
    AddPageRedoItem(myRedoLine->htabThd, item);
}

void PageManagerProcHashmapPrune()
{
    XLogRecPtr ckptPtr = pg_atomic_read_u64(&g_dispatcher->ckptRedoPtr);
    if (XLByteLE(ckptPtr, g_redoWorker->nextPrunePtr)) {
        RedoInterruptCallBack();
        return;
    }

    XLogRecPtr prunePtr = InvalidXLogRecPtr;
    PageManagerAddRedoItemToHashMapManager(&g_hashmapPruneMark);
    do {
        prunePtr = pg_atomic_read_u64(&g_dispatcher->pageLines[g_redoWorker->slotId].htabThd->nextPrunePtr);
        RedoInterruptCallBack();
        pg_usleep(100000L);   /* 100 ms */
    } while (XLByteLT(prunePtr, ckptPtr));
    g_redoWorker->nextPrunePtr = ckptPtr;
}

static void OndemandMergeHashMap(HTAB *srcHashmap, HTAB *dstHashmap)
{
    HASH_SEQ_STATUS status;
    RedoItemHashEntry *srcEntry = NULL;
    hash_seq_init(&status, srcHashmap);

    while ((srcEntry = (RedoItemHashEntry *)hash_seq_search(&status)) != NULL) {
        PRTrackAddBatchBlock(srcEntry->head, srcEntry->tail, srcEntry->redoItemNum, dstHashmap, false);
    }
}

void PageManagerMergeHashMapInRealtimeBuild()
{
    ondemand_htab_ctrl_t *procHtabCtrl = g_instance.comm_cxt.predo_cxt.redoItemHashCtrl[g_redoWorker->slotId];
    ondemand_htab_ctrl_t *targetHtabCtrl = g_dispatcher->pageLines[g_redoWorker->slotId].managerThd->redoItemHashCtrl;
    ondemand_htab_ctrl_t *nextHtabCtrl = procHtabCtrl;
    g_dispatcher->pageLines[g_redoWorker->slotId].managerThd->redoItemHashCtrl =
        g_instance.comm_cxt.predo_cxt.redoItemHashCtrl[g_redoWorker->slotId];
    while (nextHtabCtrl != targetHtabCtrl) {
        nextHtabCtrl = (ondemand_htab_ctrl_t *)nextHtabCtrl->nextHTabCtrl;
        OndemandMergeHashMap(nextHtabCtrl->hTab, procHtabCtrl->hTab);
        pfree(nextHtabCtrl);
    }
}

void PageManagerProcLsnForwarder(RedoItem *lsnForwarder)
{
    (void)pg_atomic_sub_fetch_u32(&lsnForwarder->record.refcount, 1);

    PageManagerAddRedoItemToSegWorkers(lsnForwarder);
    PageManagerAddRedoItemToHashMapManager(lsnForwarder);
    PageRedoPipeline *myRedoLine = &g_dispatcher->pageLines[g_redoWorker->slotId];
    const uint32 WorkerNumPerMng = myRedoLine->redoThdNum;

    for (uint32 i = 0; i < WorkerNumPerMng; ++i) {
        AddPageRedoItem(myRedoLine->redoThd[i], lsnForwarder);
    }

    PageManagerPruneIfRealtimeBuildFailover();
    /* wait hashmapmng prune and segworker distribute segrecord to hashmap */
    uint32 refCount;
    do {
        refCount = pg_atomic_read_u32(&g_GlobalLsnForwarder.record.refcount);
        RedoInterruptCallBack();
    } while (refCount != 0);

    SetCompletedReadEndPtr(g_redoWorker, lsnForwarder->record.ReadRecPtr, lsnForwarder->record.EndRecPtr);
    RedoPageManagerDistributeBlockRecord(NULL);
}

void PageManagerDistributeBcmBlock(XLogRecParseState *preState)
{
    PageRedoPipeline *myRedoLine = &g_dispatcher->pageLines[g_redoWorker->slotId];
    const uint32 WorkerNumPerMng = myRedoLine->redoThdNum;
    uint32 workId = GetWorkerId((uint32)preState->blockparse.blockhead.forknum, WorkerNumPerMng);
    preState->distributeStatus = XLOG_HEAD_DISTRIBUTE;
    AddPageRedoItem(myRedoLine->redoThd[workId], preState);
}

void PageManagerProcCleanupMark(RedoItem *cleanupMark)
{
    PageRedoPipeline *myRedoLine = &g_dispatcher->pageLines[g_redoWorker->slotId];
    const uint32 WorkerNumPerMng = myRedoLine->redoThdNum;
    g_redoWorker->xlogInvalidPages = XLogGetInvalidPages();
    PageManagerAddRedoItemToSegWorkers(cleanupMark);
    for (uint32 i = 0; i < WorkerNumPerMng; ++i) {
        AddPageRedoItem(myRedoLine->redoThd[i], cleanupMark);
    }
    ereport(LOG, (errcode(ERRCODE_LOG), errmsg("[ForceFinish]PageManagerProcCleanupMark has cleaned InvalidPages")));
}

void PageManagerProcClosefdMark(RedoItem *closefdMark)
{
    PageManagerAddRedoItemToSegWorkers(closefdMark);
    smgrcloseall();
}

void PageManagerProcCleanInvalidPageMark(RedoItem *cleanInvalidPageMark)
{
    PageManagerAddRedoItemToSegWorkers(cleanInvalidPageMark);
    forget_range_invalid_pages((void *)cleanInvalidPageMark);
}

void PageManagerProcCheckPoint(XLogRecParseState *parseState)
{
    Assert(IsCheckPoint(parseState));
    RedoPageManagerDistributeBlockRecord(parseState);
    bool needWait = parseState->isFullSync;
    if (needWait) {
        pg_atomic_write_u32(&g_redoWorker->fullSyncFlag, 1);
    }

    XLogBlockParseStateRelease(parseState);
    uint32 val = pg_atomic_read_u32(&g_redoWorker->fullSyncFlag);
    while (val != 0) {
        RedoInterruptCallBack();
        val = pg_atomic_read_u32(&g_redoWorker->fullSyncFlag);
    }

#ifdef USE_ASSERT_CHECKING
    int printLevel = WARNING;
#else
    int printLevel = DEBUG1;
#endif
    if (log_min_messages <= printLevel) {
        GetThreadBufferLeakNum();
    }
}

void PageManagerProcCreateTableSpace(XLogRecParseState *parseState)
{
    RedoPageManagerDistributeBlockRecord(NULL);
    bool needWait = parseState->isFullSync;
    if (needWait) {
        pg_atomic_write_u32(&g_redoWorker->fullSyncFlag, 1);
    }

    XLogBlockParseStateRelease(parseState);
    uint32 val = pg_atomic_read_u32(&g_redoWorker->fullSyncFlag);
    while (val != 0) {
        RedoInterruptCallBack();
        val = pg_atomic_read_u32(&g_redoWorker->fullSyncFlag);
    }
}

void OnDemandPageManagerProcSegFullSyncState(XLogRecParseState *parsestate)
{
    MemoryContext oldCtx = MemoryContextSwitchTo(g_redoWorker->oldCtx);
    RedoPageManagerDdlAction(parsestate);
    (void)MemoryContextSwitchTo(oldCtx);

    parsestate->nextrecord = NULL;
    XLogBlockParseStateRelease(parsestate);
}

static void SSProcSegPageRedoInSegPageRedoChildState(XLogRecParseState *redoblockstate)
{
    if (SS_ONDEMAND_REALTIME_BUILD_NORMAL) {
        BufferTag bufferTag;
        XLogRecPtr pageLsn = InvalidXLogRecPtr;
        XLogBlockHeadGetBufferTag(&redoblockstate->blockparse.blockhead, &bufferTag);
        if (SSRequestPageInOndemandRealtimeBuild(&bufferTag, redoblockstate->blockparse.blockhead.end_ptr, &pageLsn) ||
            !SSXLogParseRecordNeedReplayInOndemandRealtimeBuild(redoblockstate)) {
#ifdef USE_ASSERT_CHECKING
            bool willinit = (XLogBlockDataGetBlockFlags((XLogBlockDataParse *)&redoblockstate->blockparse.extra_rec) &
                            BKPBLOCK_WILL_INIT);
            DoRecordCheck(redoblockstate, pageLsn, !willinit);
#endif
            ereport(DEBUG1, (errmsg("[On-demand] standby node request page success during ondemand realtime build, "
                "spc/db/rel/bucket fork-block: %u/%u/%u/%d %d-%u, recordlsn: %X/%X, pagelsn: %X/%X",
                bufferTag.rnode.spcNode, bufferTag.rnode.dbNode, bufferTag.rnode.relNode, bufferTag.rnode.bucketNode,
                bufferTag.forkNum, bufferTag.blockNum, (uint32)(redoblockstate->blockparse.blockhead.end_ptr >> 32),
                (uint32)redoblockstate->blockparse.blockhead.end_ptr, (uint32)(pageLsn >> 32), (uint32)pageLsn)));
            XLogBlockParseStateRelease(redoblockstate);
            return;
        }
        ereport(DEBUG1, (errmsg("[On-demand] standby node request page failed during ondemand realtime build, "
            "spc/db/rel/bucket fork-block: %u/%u/%u/%d %d-%u, recordlsn: %X/%X, pagelsn: %X/%X",
            bufferTag.rnode.spcNode, bufferTag.rnode.dbNode, bufferTag.rnode.relNode, bufferTag.rnode.bucketNode,
            bufferTag.forkNum, bufferTag.blockNum, (uint32)(redoblockstate->blockparse.blockhead.end_ptr >> 32),
            (uint32)redoblockstate->blockparse.blockhead.end_ptr, (uint32)(pageLsn >> 32), (uint32)pageLsn)));
    }
    SegPageRedoChildState(redoblockstate);
}

// for less ondmeand recovery memory consume
static void SSReleaseRefRecordWithoutReplay(XLogRecParseState *redoblockstate)
{
    RedoItem *item = GetRedoItemPtr((XLogReaderState *)redoblockstate->refrecord);
#ifdef USE_ASSERT_CHECKING
    DoRecordCheck(redoblockstate, InvalidXLogRecPtr, false);
#endif
    DereferenceRedoItem(item);
    redoblockstate->refrecord = NULL;
}

static void SSProcPageRedoInSegPageRedoChildState(XLogRecParseState *redoblockstate)
{
    AddSegHashmap(redoblockstate);
}

static void SSSegPageRedoChildState(XLogRecParseState *childStateList)
{
    BufferTag bufferTag;
    XLogRecParseState *procState = childStateList;
    while (procState != NULL) {
        XLogRecParseState *redoblockstate = procState;
        procState = (XLogRecParseState *)procState->nextrecord;
        redoblockstate->nextrecord = NULL;
        XLogBlockHeadGetBufferTag(&redoblockstate->blockparse.blockhead, &bufferTag);
        if (IsSegmentPhysicalRelNode(bufferTag.rnode)) {
            SSProcSegPageRedoInSegPageRedoChildState(redoblockstate);
        } else {
            SSProcPageRedoInSegPageRedoChildState(redoblockstate);
        }
    }
}

static void SSProcSegPageCommonRedo(XLogRecParseState *parseState)
{
    uint8 info = XLogBlockHeadGetInfo(&parseState->blockparse.blockhead) & ~XLR_INFO_MASK;
    switch (info) {
        // has child list
        case XLOG_SEG_ATOMIC_OPERATION:
        case XLOG_SEG_SEGMENT_EXTEND:
        case XLOG_SEG_INIT_MAPPAGE:
        case XLOG_SEG_INIT_INVRSPTR_PAGE:
        case XLOG_SEG_ADD_NEW_GROUP:
            {
                XLogRecParseState *child =
                    (XLogRecParseState *)parseState->blockparse.extra_rec.blocksegfullsyncrec.childState;
                SSSegPageRedoChildState(child);
                break;
            }
        case XLOG_SEG_CREATE_EXTENT_GROUP:
        case XLOG_SEG_SPACE_SHRINK:
        case XLOG_SEG_NEW_PAGE:
        case XLOG_SEG_SPACE_DROP:
            Assert(!SS_ONDEMAND_REALTIME_BUILD_NORMAL);
            ProcSegPageCommonRedo(parseState);
            break;
        default:
            ereport(PANIC, (errmsg("SSProcSegPageCommonRedo: unknown op code %u", info)));
            break;
    }
}

void OnDemandSegWorkerProcSegFullSyncState(XLogRecParseState *parsestate)
{
    MemoryContext oldCtx = MemoryContextSwitchTo(g_redoWorker->oldCtx);
    SSProcSegPageCommonRedo(parsestate);
    (void)MemoryContextSwitchTo(oldCtx);

    parsestate->nextrecord = NULL;
    XLogBlockParseStateRelease(parsestate);
}

void OnDemandSegWorkerProcSegPipeLineSyncState(XLogRecParseState *parseState)
{
    MemoryContext oldCtx = MemoryContextSwitchTo(g_redoWorker->oldCtx);
    SSProcSegPageCommonRedo(parseState);
    (void)MemoryContextSwitchTo(oldCtx);

    XLogBlockParseStateRelease(parseState);
}

static void WaitNextBarrier(XLogRecParseState *parseState)
{
    bool needWait = parseState->isFullSync;
    if (needWait) {
        pg_atomic_write_u32(&g_redoWorker->fullSyncFlag, 1);
    }
     
    XLogBlockParseStateRelease(parseState);
    uint32 val = pg_atomic_read_u32(&g_redoWorker->fullSyncFlag);
    while (val != 0) {
        RedoInterruptCallBack();
        val = pg_atomic_read_u32(&g_redoWorker->fullSyncFlag);
    }
}

static void OnDemandSegWorkerRedoSegParseState(XLogRecParseState *preState)
{
    switch (preState->blockparse.blockhead.block_valid) {
        case BLOCK_DATA_SEG_EXTEND:
            OnDemandSegWorkerProcSegPipeLineSyncState(preState);
            break;
        case BLOCK_DATA_SEG_FULL_SYNC_TYPE:
            OnDemandSegWorkerProcSegFullSyncState(preState);
            break;
        case BLOCK_DATA_SEG_FILE_EXTEND_TYPE:
        default:
            {
                Assert(0);
            }
            break;
    }
}

static void OnDemandDispatchSegParseStateToSegWorker(XLogRecParseState *preState)
{
    Assert(g_redoWorker->slotId == SEG_PROC_PIPELINE_SLOT);
    AddPageRedoItem(g_dispatcher->auxiliaryLine.segRedoThd, preState);
}

static void OnDemandPageManagerProcSegParseState(XLogRecParseState *preState, XLogRecParseType type)
{
    if (type == PARSE_TYPE_SEG) {
        OnDemandDispatchSegParseStateToSegWorker(preState);
        return;
    }

    Assert(!SS_ONDEMAND_REALTIME_BUILD_NORMAL);
    WaitSegRedoWorkersQueueEmpty();
    OnDemandPageManagerRedoSegParseState(preState);
}

static bool WaitPrimaryDoCheckpointAndAllPRTrackEmpty(XLogRecParseState *preState, HTAB *redoItemHash)
{
    if (SS_ONDEMAND_REALTIME_BUILD_DISABLED) {
        return false;
    }

    bool waitDone = false;
    XLogRecPtr ddlSyncPtr = preState->blockparse.blockhead.end_ptr;

    // notify dispatcher thread and wait for primary checkpoint
    XLogRecPtr syncRecordPtr;
    do {
        syncRecordPtr = pg_atomic_read_u64(&g_dispatcher->syncRecordPtr);
        if (XLByteLT(ddlSyncPtr, syncRecordPtr)) {
            break;
        }
    } while (!pg_atomic_compare_exchange_u64(&g_dispatcher->syncRecordPtr, &syncRecordPtr, ddlSyncPtr));

    do {
        if (pg_atomic_read_u32(&g_redoWorker->currentHtabBlockNum) == 0) {
            // exit if hashmap manager already clear all hashmap
            waitDone = true;
            break;
        } else if (SS_ONDEMAND_REALTIME_BUILD_FAILOVER) {
            // exit if primary node crash
            waitDone = false;
            break;
        }
        PageManagerProcHashmapPrune();
        pg_usleep(100000L); /* 100 ms */
    } while (true);

    // clear all blocks in hashmap
    g_redoWorker->nextPrunePtr = pg_atomic_read_u64(&g_dispatcher->ckptRedoPtr);

    return waitDone;
}

static void PageManagerPruneIfRealtimeBuildFailover()
{
    if (SS_ONDEMAND_REALTIME_BUILD_FAILOVER && g_redoWorker->inRealtimeBuild) {
        PageManagerProcHashmapPrune();
        PageManagerMergeHashMapInRealtimeBuild();
        g_redoWorker->inRealtimeBuild = false;
    }
}

void ReleaseBlockParseStateIfNotReplay(XLogRecParseState *preState)
{
#ifdef USE_ASSERT_CHECKING
    XLogRecParseState *nextBlockState = preState;
    while (nextBlockState != NULL) {
        DoRecordCheck(nextBlockState, InvalidXLogRecPtr, false);
        nextBlockState = (XLogRecParseState *)(nextBlockState->nextrecord);
    }
#endif
    XLogBlockParseStateRelease(preState);
}

static void OndemandSwitchHTABIfBlockNumUpperLimit()
{
    if (SS_ONDEMAND_REALTIME_BUILD_NORMAL && (g_redoWorker->currentHtabBlockNum > ONDEMAND_HASHTAB_SWITCH_LIMIT)) {
        ondemand_htab_ctrl_t *oldHTabCtrl = g_dispatcher->pageLines[g_redoWorker->slotId].managerThd->redoItemHashCtrl;
        ondemand_htab_ctrl_t *newHTabCtrl = PRRedoItemHashInitialize(g_instance.comm_cxt.redoItemCtx);

        oldHTabCtrl->nextHTabCtrl = (void *)newHTabCtrl;
        g_dispatcher->pageLines[g_redoWorker->slotId].managerThd->redoItemHashCtrl = newHTabCtrl;
        g_redoWorker->currentHtabBlockNum = 0;
    }
}

static void OnDemandPageManagerRedoSegParseState(XLogRecParseState *preState)
{
    Assert(g_redoWorker->slotId == 0);
    switch (preState->blockparse.blockhead.block_valid) {
        case BLOCK_DATA_SEG_FULL_SYNC_TYPE:
            OnDemandPageManagerProcSegFullSyncState(preState);
            break;
        case BLOCK_DATA_SEG_EXTEND:
        case BLOCK_DATA_SEG_FILE_EXTEND_TYPE:
        default:
            {
                Assert(0);
            }
            break;
    }
}

void PageManagerRedoParseState(XLogRecParseState *preState)
{
    PageManagerPruneIfRealtimeBuildFailover();
    if (XLByteLT(preState->blockparse.blockhead.end_ptr, g_redoWorker->nextPrunePtr)) {
        ReleaseBlockParseStateIfNotReplay(preState);
        return;
    }

    HTAB *hashMap = g_dispatcher->pageLines[g_redoWorker->slotId].managerThd->redoItemHashCtrl->hTab;
    XLogRecParseType type = GetCurrentXLogRecParseType(preState);
    if (type == PARSE_TYPE_DDL && WaitPrimaryDoCheckpointAndAllPRTrackEmpty(preState, hashMap)) {
        ReleaseBlockParseStateIfNotReplay(preState);
        return;
    }

    switch (preState->blockparse.blockhead.block_valid) {
        case BLOCK_DATA_MAIN_DATA_TYPE:
        case BLOCK_DATA_UNDO_TYPE:
        case BLOCK_DATA_VM_TYPE:
        case BLOCK_DATA_FSM_TYPE:
            GetRedoStartTime(g_redoWorker->timeCostList[TIME_COST_STEP_3]);
            PRTrackAddBlock(preState, hashMap, false);
            g_dispatcher->pageLines[g_redoWorker->slotId].managerThd->currentHtabBlockNum++;
            SetCompletedReadEndPtr(g_redoWorker, preState->blockparse.blockhead.start_ptr,
                preState->blockparse.blockhead.end_ptr);
            SSReleaseRefRecordWithoutReplay(preState);
            g_redoWorker->redoItemHashCtrl->maxRedoItemPtr = preState->blockparse.blockhead.end_ptr;
            CountRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_3]);
            break;
        case BLOCK_DATA_DDL_TYPE:
            GetRedoStartTime(g_redoWorker->timeCostList[TIME_COST_STEP_4]);
            RedoPageManagerDoDataTypeAction(preState, hashMap);
            CountRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_4]);
            break;
        case BLOCK_DATA_SEG_EXTEND:
            GetRedoStartTime(g_redoWorker->timeCostList[TIME_COST_STEP_4]);
            OnDemandPageManagerProcSegParseState(preState, type);
            CountRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_4]);
            break;
        case BLOCK_DATA_DROP_DATABASE_TYPE:
            GetRedoStartTime(g_redoWorker->timeCostList[TIME_COST_STEP_5]);
            RedoPageManagerDoDropAction(preState, hashMap);
            CountRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_5]);
            break;
        case BLOCK_DATA_DROP_TBLSPC_TYPE:
            /* just make sure any other ddl before drop tblspc is done */
            XLogBlockParseStateRelease(preState);
            break;
        case BLOCK_DATA_CREATE_DATABASE_TYPE:
            GetRedoStartTime(g_redoWorker->timeCostList[TIME_COST_STEP_6]);
            RedoPageManagerDistributeBlockRecord(NULL);
            /* wait until queue empty */
            WaitCurrentPipeLineRedoWorkersQueueEmpty();
            /* do atcual action */
            RedoPageManagerSyncDdlAction(preState);
            CountRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_6]);
            break;
        case BLOCK_DATA_SEG_FILE_EXTEND_TYPE:
        case BLOCK_DATA_SEG_FULL_SYNC_TYPE:
            GetRedoStartTime(g_redoWorker->timeCostList[TIME_COST_STEP_8]);
            OnDemandPageManagerProcSegParseState(preState, type);
            CountRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_8]);
            break;
        case BLOCK_DATA_CREATE_TBLSPC_TYPE:
            GetRedoStartTime(g_redoWorker->timeCostList[TIME_COST_STEP_7]);
            PageManagerProcCreateTableSpace(preState);
            CountRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_7]);
            break;
        case BLOCK_DATA_XLOG_COMMON_TYPE:
            PageManagerProcCheckPoint(preState);
            break;
        case BLOCK_DATA_NEWCU_TYPE:
            RedoPageManagerDistributeBlockRecord(NULL);
            PageManagerDistributeBcmBlock(preState);
            break;
        case BLOCK_DATA_SEG_SPACE_DROP:
        case BLOCK_DATA_SEG_SPACE_SHRINK:
            GetRedoStartTime(g_redoWorker->timeCostList[TIME_COST_STEP_8]);
            RedoPageManagerDistributeBlockRecord(preState);
            WaitCurrentPipeLineRedoWorkersQueueEmpty();
            RedoPageManagerSyncDdlAction(preState);
            CountRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_8]);
            break;
        case BLOCK_DATA_BARRIER_TYPE:
            RedoPageManagerDistributeBlockRecord(preState);
            WaitNextBarrier(preState);
            break;
        default:
            XLogBlockParseStateRelease(preState);
            break;
    }
    OndemandSwitchHTABIfBlockNumUpperLimit();
}

#ifdef USE_ASSERT_CHECKING
static void OndemandCheckHashMapDistributeDone()
{
    Assert(g_instance.comm_cxt.predo_cxt.redoItemHashCtrl[g_redoWorker->slotId] ==
        g_dispatcher->pageLines[g_redoWorker->slotId].managerThd->redoItemHashCtrl);
    HASH_SEQ_STATUS status;
    RedoItemHashEntry *redoItemEntry = NULL;
    HTAB *hashMap = g_dispatcher->pageLines[g_redoWorker->slotId].managerThd->redoItemHashCtrl->hTab;
    hash_seq_init(&status, hashMap);

    while ((redoItemEntry = (RedoItemHashEntry *)hash_seq_search(&status)) != NULL) {
        XLogRecParseState *procState = redoItemEntry->head;
        while (procState != NULL) {
            XLogRecParseState *nextState = (XLogRecParseState *)procState->nextrecord;
            Assert(procState->distributeStatus != XLOG_NO_DISTRIBUTE);
            if (nextState != NULL) {
                Assert(XLByteLE(procState->blockparse.blockhead.end_ptr, nextState->blockparse.blockhead.end_ptr));
            }
            procState = nextState;
        }
    }
}
#endif

bool PageManagerRedoDistributeItems(void **eleArry, uint32 eleNum)
{
    for (uint32 i = 0; i < eleNum; i++) {
        if (eleArry[i] == (void *)&g_redoEndMark) {
#ifdef USE_ASSERT_CHECKING
            OndemandCheckHashMapDistributeDone();
#endif
            return true;
        } else if (eleArry[i] == (void *)&g_GlobalLsnForwarder) {
            PageManagerProcLsnForwarder((RedoItem *)eleArry[i]);
            continue;
        } else if (eleArry[i] == (void *)&g_cleanupMark) {
            PageManagerProcCleanupMark((RedoItem *)eleArry[i]);
            continue;
        } else if (eleArry[i] == (void *)&g_closefdMark) {
            PageManagerProcClosefdMark((RedoItem *)eleArry[i]);
            continue;
        } else if (eleArry[i] == (void *)&g_cleanInvalidPageMark) {
            PageManagerProcCleanInvalidPageMark((RedoItem *)eleArry[i]);
            continue;
        } else if (eleArry[i] == (void *)&g_hashmapPruneMark) {
            PageManagerProcHashmapPrune();
            continue;
        } else if (eleArry[i] == (void *)&g_forceDistributeMark) {
            Assert(!SS_ONDEMAND_REALTIME_BUILD_NORMAL);
            // double check
            if (SS_ONDEMAND_RECOVERY_HASHMAP_FULL) {
                ereport(WARNING, (errcode(ERRCODE_LOG),
                    errmsg("[On-demand] Parse buffer num approach critical value, distribute block record by force,"
                        " slotid %d, usedblknum %d, totalblknum %d", g_redoWorker->slotId,
                        pg_atomic_read_u32(&g_dispatcher->parseManager.memctl.usedblknum),
                        g_dispatcher->parseManager.memctl.totalblknum)));
                RedoPageManagerDistributeBlockRecord(NULL);
            }
            continue;
        }
        XLogRecParseState *recordblockstate = (XLogRecParseState *)eleArry[i];
        XLogRecParseState *nextState = recordblockstate;
        do {
            XLogRecParseState *preState = nextState;
            nextState = (XLogRecParseState *)nextState->nextrecord;
            preState->nextrecord = NULL;
#ifdef ENABLE_UT
            TestXLogRecParseStateEventProbe(UTEST_EVENT_RTO_PAGEMGR_REDO_BEFORE_DISTRIBUTE_ITEMS,
                __FUNCTION__, preState);
#endif

            PageManagerRedoParseState(preState);
#ifdef ENABLE_UT
            TestXLogRecParseStateEventProbe(UTEST_EVENT_RTO_PAGEMGR_REDO_AFTER_DISTRIBUTE_ITEMS,
                __FUNCTION__, preState);
#endif
        } while (nextState != NULL);
    }

    return false;
}

void RedoPageManagerMain()
{
    void **eleArry;
    uint32 eleNum;

    (void)RegisterRedoInterruptCallBack(HandlePageRedoInterrupts);

    GetRedoStartTime(g_redoWorker->timeCostList[TIME_COST_STEP_1]);
    while (SPSCBlockingQueueGetAll(g_redoWorker->queue, &eleArry, &eleNum)) {
        CountAndGetRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_1], g_redoWorker->timeCostList[TIME_COST_STEP_2]);
        bool isEnd = PageManagerRedoDistributeItems(eleArry, eleNum);
        SPSCBlockingQueuePopN(g_redoWorker->queue, eleNum);
        CountRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_2]);
        if (isEnd)
            break;

        RedoInterruptCallBack();
        ADD_ABNORMAL_POSITION(5);
        GetRedoStartTime(g_redoWorker->timeCostList[TIME_COST_STEP_1]);
    }

    RedoThrdWaitForExit(g_redoWorker);
}

bool IsXactXlog(const XLogReaderState *record)
{
    if (XLogRecGetRmid(record) != RM_XACT_ID) {
        return false;
    }
    return true;
}

void TrxnManagerProcLsnForwarder(RedoItem *lsnForwarder)
{
    SetCompletedReadEndPtr(g_redoWorker, lsnForwarder->record.ReadRecPtr, lsnForwarder->record.EndRecPtr);
    (void)pg_atomic_sub_fetch_u32(&lsnForwarder->record.refcount, 1);

    AddPageRedoItem(g_dispatcher->trxnLine.redoThd, lsnForwarder);
}

void TrxnManagerProcCleanupMark(RedoItem *cleanupMark)
{
    g_redoWorker->xlogInvalidPages = XLogGetInvalidPages();
    AddPageRedoItem(g_dispatcher->trxnLine.redoThd, cleanupMark);
    ereport(LOG, (errcode(ERRCODE_LOG), errmsg("[ForceFinish]TrxnManagerProcCleanupMark has cleaned InvalidPages")));
}

static void TrxnManagerProcHashMapPrune()
{
    if (SS_ONDEMAND_REALTIME_BUILD_DISABLED || !g_redoWorker->inRealtimeBuild) {
        return;
    }

    XLogRecPtr prunePtr = pg_atomic_read_u64(&g_dispatcher->ckptRedoPtr);
    if (XLByteLT(g_redoWorker->nextPrunePtr, prunePtr)) {
        while (!SPSCBlockingQueueIsEmpty(g_dispatcher->trxnQueue)) {
            RedoItem *item = (RedoItem *)SPSCBlockingQueueTop(g_dispatcher->trxnQueue);
            if (XLByteLT(prunePtr, item->record.EndRecPtr)) {
                break;
            }
            DereferenceRedoItem(item);
            SPSCBlockingQueuePop(g_dispatcher->trxnQueue);
        }
        g_redoWorker->nextPrunePtr = prunePtr;
    }
}

static void TrxnManagerPruneAndDistributeIfRealtimeBuildFailover()
{
    if (SS_ONDEMAND_REALTIME_BUILD_FAILOVER && g_redoWorker->inRealtimeBuild) {
        TrxnManagerProcHashMapPrune();
        while (!SPSCBlockingQueueIsEmpty(g_dispatcher->trxnQueue)) {
            RedoItem *item = (RedoItem *)SPSCBlockingQueueTop(g_dispatcher->trxnQueue);
            AddPageRedoItem(g_dispatcher->trxnLine.redoThd, item);
            SPSCBlockingQueuePop(g_dispatcher->trxnQueue);
        }
        g_redoWorker->inRealtimeBuild = false;
    }
}

static void TrxnManagerPruneIfQueueFullInRealtimeBuild()
{
    while (SS_ONDEMAND_RECOVERY_TRXN_QUEUE_FULL && SS_ONDEMAND_REALTIME_BUILD_NORMAL) {
        TrxnManagerProcHashMapPrune();
        RedoInterruptCallBack();
    }
}

static void TrxnManagerAddTrxnRecord(RedoItem *item, bool syncRecord)
{
    if (g_redoWorker->inRealtimeBuild) {
        if (syncRecord) {
            if (XactHasSegpageRelFiles(&item->record)) {
                uint32 expected = 1;
                pg_atomic_compare_exchange_u32((volatile uint32 *)&(g_dispatcher->segpageXactDoneFlag), &expected, 0);
            }
            TrxnManagerProcHashMapPrune();
            DereferenceRedoItem(item);
        } else {
            AddTrxnHashmap(item);
        }
    } else {
        AddPageRedoItem(g_dispatcher->trxnLine.redoThd, item);
    }
}

bool TrxnManagerDistributeItemsBeforeEnd(RedoItem *item)
{
    bool exitFlag = false;
    if (item == &g_redoEndMark) {
        exitFlag = true;
    } else if (item == (RedoItem *)&g_GlobalLsnForwarder) {
        TrxnManagerPruneAndDistributeIfRealtimeBuildFailover();
        TrxnManagerProcLsnForwarder(item);
    } else if (item == (RedoItem *)&g_cleanupMark) {
        TrxnManagerProcCleanupMark(item);
    } else if (item == (void *)&g_closefdMark) {
        smgrcloseall();
    } else if (item == (void *)&g_cleanInvalidPageMark) {
        forget_range_invalid_pages((void *)item);
    } else if (item == (void *)&g_hashmapPruneMark) {
        TrxnManagerProcHashMapPrune();
    } else {
        if (XLByteLT(item->record.EndRecPtr, g_redoWorker->nextPrunePtr)) {
            DereferenceRedoItem(item);
            return exitFlag;
        }

        bool syncRecord = false;
        GetRedoStartTime(g_redoWorker->timeCostList[TIME_COST_STEP_4]);
        if (IsTableSpaceDrop(&item->record) || IsTableSpaceCreate(&item->record) || IsBarrierRelated(&item->record) ||
            (IsXactXlog(&item->record) && XactWillRemoveRelFiles(&item->record))) {
            uint32 relCount;
            do {
                syncRecord = true;
                RedoInterruptCallBack();
                relCount = pg_atomic_read_u32(&item->record.refcount);
            } while (relCount != 1);
        }
        CountAndGetRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_4], g_redoWorker->timeCostList[TIME_COST_STEP_5]);
#ifdef ENABLE_UT
        TestXLogReaderProbe(UTEST_EVENT_RTO_TRXNMGR_DISTRIBUTE_ITEMS,
            __FUNCTION__, &item->record);
#endif
        TrxnManagerPruneIfQueueFullInRealtimeBuild();
        TrxnManagerPruneAndDistributeIfRealtimeBuildFailover();
        TrxnManagerAddTrxnRecord(item, syncRecord);
        CountRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_5]);
    }
    return exitFlag;
}

void GlobalLsnUpdate()
{
    t_thrd.xlog_cxt.standbyState = g_redoWorker->standbyState;
    GetRedoStartTime(g_redoWorker->timeCostList[TIME_COST_STEP_6]);
    if (LsnUpdate()) {
        ExtremRtoUpdateMinCheckpoint();
        CheckRecoveryConsistency();
    }
    CountRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_6]);
}

bool LsnUpdate()
{
    XLogRecPtr minStart = MAX_XLOG_REC_PTR;
    XLogRecPtr minEnd = MAX_XLOG_REC_PTR;
    GetReplayedRecPtr(&minStart, &minEnd);
    if ((minEnd != MAX_XLOG_REC_PTR) && (minStart != MAX_XLOG_REC_PTR)) {
        SetXLogReplayRecPtr(minStart, minEnd);
        return true;
    }
    return false;
}

static void TrxnMangerQueueCallBack()
{
    GlobalLsnUpdate();
    HandlePageRedoInterrupts();
}

void TrxnManagerMain()
{
    (void)RegisterRedoInterruptCallBack(TrxnMangerQueueCallBack);
    t_thrd.xlog_cxt.max_page_flush_lsn = get_global_max_page_flush_lsn();
    ereport(LOG,
            (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
             errmsg("TrxnManagerMain: first get_global_max_page_flush_lsn %08X/%08X",
                    (uint32)(t_thrd.xlog_cxt.max_page_flush_lsn >> 32), (uint32)(t_thrd.xlog_cxt.max_page_flush_lsn))));
    while (true) {
        GetRedoStartTime(g_redoWorker->timeCostList[TIME_COST_STEP_3]);
        if (FORCE_FINISH_ENABLED && t_thrd.xlog_cxt.max_page_flush_lsn == MAX_XLOG_REC_PTR) {
            t_thrd.xlog_cxt.max_page_flush_lsn = get_global_max_page_flush_lsn();
            if (t_thrd.xlog_cxt.max_page_flush_lsn != MAX_XLOG_REC_PTR) {
                ereport(LOG, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                              errmsg("TrxnManagerMain: second get_global_max_page_flush_lsn %08X/%08X",
                                     (uint32)(t_thrd.xlog_cxt.max_page_flush_lsn >> 32),
                                     (uint32)(t_thrd.xlog_cxt.max_page_flush_lsn))));
            }
        }
        CountRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_3]);
        if (!SPSCBlockingQueueIsEmpty(g_redoWorker->queue)) {
            GetRedoStartTime(g_redoWorker->timeCostList[TIME_COST_STEP_1]);
            RedoItem *item = (RedoItem *)SPSCBlockingQueueTop(g_redoWorker->queue);
            CountAndGetRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_1],
                g_redoWorker->timeCostList[TIME_COST_STEP_2]);
            bool isEnd = TrxnManagerDistributeItemsBeforeEnd(item);
            CountRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_2]);
            SPSCBlockingQueuePop(g_redoWorker->queue);

            if (isEnd) {
                break;
            }
        } else {
            long sleeptime = 80 * 1000;
            pg_usleep(sleeptime);
        }

        ADD_ABNORMAL_POSITION(2);
        RedoInterruptCallBack();
    }

    RedoThrdWaitForExit(g_redoWorker);
    GlobalLsnUpdate();
}

void TrxnWorkerProcLsnForwarder(RedoItem *lsnForwarder)
{
    SetCompletedReadEndPtr(g_redoWorker, lsnForwarder->record.ReadRecPtr, lsnForwarder->record.EndRecPtr);
    (void)pg_atomic_sub_fetch_u32(&lsnForwarder->record.refcount, 1);
}

void TrxnWorkNotifyRedoWorker()
{
    for (uint32 i = 0; i < g_dispatcher->allWorkersCnt; ++i) {
        if (g_dispatcher->allWorkers[i]->role == REDO_PAGE_WORKER ||
            g_dispatcher->allWorkers[i]->role == REDO_PAGE_MNG) {
            pg_atomic_write_u32(&(g_dispatcher->allWorkers[i]->fullSyncFlag), 0);
        }
    }
}

void TrxnWorkrProcCleanupMark(RedoItem *cleanupMark)
{
    g_redoWorker->xlogInvalidPages = XLogGetInvalidPages();
    ereport(LOG, (errcode(ERRCODE_LOG), errmsg("[ForceFinish]TrxnWorkrProcCleanupMark has cleaned InvalidPages")));
}

bool CheckFullSyncCheckpoint(RedoItem *item)
{
    if (!IsCheckPoint(&(item->record))) {
        return true;
    }

    if (XLByteLE(item->record.ReadRecPtr, t_thrd.shemem_ptr_cxt.ControlFile->checkPoint)) {
        return true;
    }

    return false;
}

static void TrxnWorkerQueueCallBack()
{
    if (XLByteLT(t_thrd.xlog_cxt.minRecoveryPoint, g_redoWorker->minRecoveryPoint)) {
        t_thrd.xlog_cxt.minRecoveryPoint = g_redoWorker->minRecoveryPoint;
    }
    HandlePageRedoInterrupts();
}

void TrxnWorkMain()
{
#ifdef ENABLE_MOT
    MOTBeginRedoRecovery();
#endif
    (void)RegisterRedoInterruptCallBack(TrxnWorkerQueueCallBack);
    if (ParseStateWithoutCache()) {
        XLogRedoBufferInitFunc(&(g_redoWorker->bufferManager), MAX_LOCAL_BUFF_NUM, &recordRefOperate,
                               RedoInterruptCallBack);
    }

    RedoItem *item = NULL;
    GetRedoStartTime(g_redoWorker->timeCostList[TIME_COST_STEP_1]);
    while ((item = (RedoItem *)SPSCBlockingQueueTop(g_redoWorker->queue)) != &g_redoEndMark) {
        CountAndGetRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_1], g_redoWorker->timeCostList[TIME_COST_STEP_2]);
        if ((void *)item == (void *)&g_GlobalLsnForwarder) {
            TrxnWorkerProcLsnForwarder((RedoItem *)item);
            SPSCBlockingQueuePop(g_redoWorker->queue);
        } else if ((void *)item == (void *)&g_cleanupMark) {
            TrxnWorkrProcCleanupMark((RedoItem *)item);
            SPSCBlockingQueuePop(g_redoWorker->queue);
        } else if ((void *)item == (void *)&g_closefdMark) {
            smgrcloseall();
            SPSCBlockingQueuePop(g_redoWorker->queue);
        } else if ((void *)item == (void *)&g_cleanInvalidPageMark) {
            forget_range_invalid_pages((void *)item);
            SPSCBlockingQueuePop(g_redoWorker->queue);
        } else {
            t_thrd.xlog_cxt.needImmediateCkp = item->needImmediateCheckpoint;
            bool fullSync = item->record.isFullSync;
            GetRedoStartTime(g_redoWorker->timeCostList[TIME_COST_STEP_3]);
            ApplySinglePageRecord(item);
            CountAndGetRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_3],
                g_redoWorker->timeCostList[TIME_COST_STEP_4]);
            SPSCBlockingQueuePop(g_redoWorker->queue);
            SetCompletedReadEndPtr(g_redoWorker, item->record.ReadRecPtr, item->record.EndRecPtr);
            CountAndGetRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_4],
                g_redoWorker->timeCostList[TIME_COST_STEP_5]);
            if (fullSync) {
                Assert(CheckFullSyncCheckpoint(item));
                TrxnWorkNotifyRedoWorker();
            }

            if (XactHasSegpageRelFiles(&item->record)) {
                uint32 expected = 1;
                pg_atomic_compare_exchange_u32((volatile uint32 *)&(g_dispatcher->segpageXactDoneFlag), &expected, 0);
            }
            CountRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_5]);
            DereferenceRedoItem(item);
            RedoInterruptCallBack();
        }
        ADD_ABNORMAL_POSITION(3);
        CountAndGetRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_2], g_redoWorker->timeCostList[TIME_COST_STEP_1]);
    }

    SPSCBlockingQueuePop(g_redoWorker->queue);
    if (ParseStateWithoutCache())
        XLogRedoBufferDestoryFunc(&(g_redoWorker->bufferManager));
#ifdef ENABLE_MOT
    MOTEndRedoRecovery();
#endif
}

void RedoPageWorkerCheckPoint(const XLogRecParseState *redoblockstate)
{
    CheckPoint checkPoint;
    Assert(IsCheckPoint(redoblockstate));
    XLogSynAllBuffer();
    Assert(redoblockstate->blockparse.extra_rec.blockxlogcommon.maindatalen >= sizeof(checkPoint));
    errno_t rc = memcpy_s(&checkPoint, sizeof(checkPoint),
                          redoblockstate->blockparse.extra_rec.blockxlogcommon.maindata, sizeof(checkPoint));
    securec_check(rc, "\0", "\0");
    if (IsRestartPointSafe(checkPoint.redo)) {
        pg_atomic_write_u64(&g_redoWorker->lastCheckedRestartPoint, redoblockstate->blockparse.blockhead.start_ptr);
    }

    UpdateTimeline(&checkPoint);

#ifdef USE_ASSERT_CHECKING
    int printLevel = WARNING;
#else
    int printLevel = DEBUG1;
#endif
    if (log_min_messages <= printLevel) {
        GetThreadBufferLeakNum();
    }
}

static void SegWorkerRedoAllSegBlockRecord()
{
    RedoTimeCost timeCost1;
    RedoTimeCost timeCost2;

    while (!SPSCBlockingQueueIsEmpty(g_dispatcher->segQueue)) {
        XLogRecParseState *segRecord = (XLogRecParseState *)SPSCBlockingQueueTop(g_dispatcher->segQueue);
        RedoBufferInfo bufferinfo = {0};
        (void)XLogBlockRedoForExtremeRTO(segRecord, &bufferinfo, false, timeCost1, timeCost2);
        if (bufferinfo.pageinfo.page != NULL) {
            MarkSegPageRedoChildPageDirty(&bufferinfo);
        }
        XLogBlockParseStateRelease(segRecord);
        SPSCBlockingQueuePop(g_dispatcher->segQueue);
    }
}

void PageWorkerProcLsnForwarder(RedoItem *lsnForwarder)
{
    (void)pg_atomic_sub_fetch_u32(&lsnForwarder->record.refcount, 1);

    /* wait all worker proc done */
    uint32 refCount;
    do {
        refCount = pg_atomic_read_u32(&g_GlobalLsnForwarder.record.refcount);
        RedoInterruptCallBack();
    } while (refCount != 0);
    SetCompletedReadEndPtr(g_redoWorker, lsnForwarder->record.ReadRecPtr, lsnForwarder->record.EndRecPtr);
}

void SegWorkerProcLsnForwarder(RedoItem *lsnForwarder)
{
    uint32 refCount;
    do {
        refCount = pg_atomic_read_u32(&g_GlobalLsnForwarder.record.refcount);
        RedoInterruptCallBack();
    } while (refCount != 1);

    // prune done, redo all seg block record
    SegWorkerRedoAllSegBlockRecord();

    SetCompletedReadEndPtr(g_redoWorker, lsnForwarder->record.ReadRecPtr, lsnForwarder->record.EndRecPtr);
    (void)pg_atomic_sub_fetch_u32(&lsnForwarder->record.refcount, 1);
}

bool XlogNeedUpdateFsm(XLogRecParseState *procState, RedoBufferInfo *bufferinfo)
{
    XLogBlockHead *blockhead = &procState->blockparse.blockhead;
    if (bufferinfo->pageinfo.page == NULL || !(bufferinfo->dirtyflag) || blockhead->forknum != MAIN_FORKNUM ||
        XLogBlockHeadGetValidInfo(blockhead) != BLOCK_DATA_MAIN_DATA_TYPE || blockhead->bucketNode != InvalidBktId) {
        return false;
    }

    Size freespace = PageGetHeapFreeSpace(bufferinfo->pageinfo.page);

    RmgrId rmid = XLogBlockHeadGetRmid(blockhead);
    if (rmid == RM_HEAP2_ID) {
        uint8 info = XLogBlockHeadGetInfo(blockhead) & ~XLR_INFO_MASK;
        if (info == XLOG_HEAP2_CLEAN) {
            return true;
        } else if ((info == XLOG_HEAP2_MULTI_INSERT) && (freespace < BLCKSZ / 5)) {
            return true;
        }

    } else if (rmid == RM_HEAP_ID) {
        uint8 info = XLogBlockHeadGetInfo(blockhead) & ~XLR_INFO_MASK;
        if ((info == XLOG_HEAP_INSERT || info == XLOG_HEAP_UPDATE) && (freespace < BLCKSZ / 5)) {
            return true;
        }
    }

    return false;
}

void RedoPageWorkerRedoBcmBlock(XLogRecParseState *procState)
{
    RmgrId rmid = XLogBlockHeadGetRmid(&procState->blockparse.blockhead);
    if (rmid == RM_HEAP2_ID) {
        RelFileNode node;
        node.spcNode = procState->blockparse.blockhead.spcNode;
        node.dbNode = procState->blockparse.blockhead.dbNode;
        node.relNode = procState->blockparse.blockhead.relNode;
        node.bucketNode = procState->blockparse.blockhead.bucketNode;
        node.opt = procState->blockparse.blockhead.opt;
        XLogBlockNewCuParse *newCuParse = &(procState->blockparse.extra_rec.blocknewcu);
        uint8 info = XLogBlockHeadGetInfo(&procState->blockparse.blockhead) & ~XLR_INFO_MASK;
        switch (info & XLOG_HEAP_OPMASK) {
            case XLOG_HEAP2_BCM: {
                xl_heap_bcm *xlrec = (xl_heap_bcm *)(newCuParse->main_data);
                heap_bcm_redo(xlrec, node, procState->blockparse.blockhead.end_ptr);
                break;
            }
            case XLOG_HEAP2_LOGICAL_NEWPAGE: {
                Assert(IsHeapFileNode(node));
                xl_heap_logical_newpage *xlrec = (xl_heap_logical_newpage *)(newCuParse->main_data);
                char *cuData = newCuParse->main_data + SizeOfHeapLogicalNewPage;
                heap_xlog_bcm_new_page(xlrec, node, cuData);
                break;
            }
            default:
                break;
        }
    }
}

LWLock* OndemandGetXLogPartitionLock(BufferDesc* bufHdr, ForkNumber forkNum, BlockNumber blockNum) {
    LWLock *xlog_partition_lock = NULL;
    ondemand_extreme_rto::RedoItemTag redoItemTag;
    INIT_REDO_ITEM_TAG(redoItemTag, bufHdr->tag.rnode, forkNum, blockNum);
    uint32 slotId = GetSlotId(redoItemTag.rNode, 0, 0, GetBatchCount());
    HTAB *hashMap = g_instance.comm_cxt.predo_cxt.redoItemHashCtrl[slotId]->hTab;
    if (hashMap == NULL) {
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                        errmsg("redo item hash table corrupted, there has invalid hashtable.")));
    }

    /* get partition lock by redoItemTag */
    unsigned int partitionLockHash = XlogTrackTableHashCode(&redoItemTag);
    xlog_partition_lock = XlogTrackMappingPartitionLock(partitionLockHash);

    return xlog_partition_lock;
}

/**
 * Check the block if need to redo and try hashmap lock. 
 * There are three kinds of result as follow:
 * 1. ONDEMAND_HASHMAP_ENTRY_REDO_DONE: the recordes of this buffer redo done.
 * 2. ONDEMAND_HASHMAP_ENTRY_REDOING: the reordes of this buffer is redoing
 * 3. ONDEMAND_HASHMAP_ENTRY_NEED_REDO: the recordes of this buffer has not been redone,
 *    so get hashmap entry lock.
 */
int checkBlockRedoStateAndTryHashMapLock(BufferDesc* bufHdr, ForkNumber forkNum, BlockNumber blockNum) {
    LWLock *xlog_partition_lock = NULL;
    RedoBufferInfo bufferInfo;
    ondemand_extreme_rto::RedoItemTag redoItemTag;
    dms_buf_ctrl_t *buf_ctrl = GetDmsBufCtrl(bufHdr->buf_id);
    bool noNeedRedo = false;
    bool getSharedLock = false;

    /* buffer redo done, no need to check */
    if (buf_ctrl->state & BUF_ONDEMAND_REDO_DONE) {
        return ONDEMAND_HASHMAP_ENTRY_REDO_DONE;
    }

    /* get hashmap by redoItemTag */
    INIT_REDO_ITEM_TAG(redoItemTag, bufHdr->tag.rnode, forkNum, blockNum);
    uint32 slotId = GetSlotId(redoItemTag.rNode, 0, 0, GetBatchCount());
    HTAB *hashMap = g_instance.comm_cxt.predo_cxt.redoItemHashCtrl[slotId]->hTab;
    if (hashMap == NULL) {
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                        errmsg("redo item hash table corrupted, there has invalid hashtable.")));
    }

    /* get partition lock by redoItemTag */
    unsigned int partitionLockHash = XlogTrackTableHashCode(&redoItemTag);
    xlog_partition_lock = XlogTrackMappingPartitionLock(partitionLockHash);

    /* if buffer has be redone, set state to REDO_DONE and return REDO_DONE */
    if (checkBlockRedoDoneFromHashMap(xlog_partition_lock, hashMap, redoItemTag, &getSharedLock)) {
        buf_ctrl->state |= BUF_ONDEMAND_REDO_DONE;
        ereport(DEBUG1, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
            errmsg("checkBlockRedoStateAndTryHashMapLock, block redo done or need to redo: spc/db/rel/bucket fork-block: %u/%u/%u/%d %d-%u.",
                redoItemTag.rNode.spcNode, redoItemTag.rNode.dbNode, redoItemTag.rNode.relNode, redoItemTag.rNode.bucketNode,
                redoItemTag.forkNum, redoItemTag.blockNum)));
        return ONDEMAND_HASHMAP_ENTRY_REDO_DONE;
    /* get partition lock failed, means other process is redoing this buffer, retun REDOING */
    } else if (!getSharedLock) {
        return ONDEMAND_HASHMAP_ENTRY_REDOING;
    /* if get redoItemEntry lock, the buffer has not been redone before, return NOT_REDO */
    } else if (tryLockHashMap(xlog_partition_lock, hashMap, redoItemTag, &noNeedRedo)) {
        ereport(DEBUG1, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                errmsg("checkBlockRedoStateAndTryHashMapLock, block need to redo and has got partition lock: spc/db/rel/bucket fork-block: %u/%u/%u/%d %d-%u.",
                        redoItemTag.rNode.spcNode, redoItemTag.rNode.dbNode, redoItemTag.rNode.relNode, redoItemTag.rNode.bucketNode,
                        redoItemTag.forkNum, redoItemTag.blockNum)));
        return ONDEMAND_HASHMAP_ENTRY_NEED_REDO;
    /* buffer has be redone, set state to REDO_DONE and return REDO_DONE */
    } else if (noNeedRedo){
        buf_ctrl->state |= BUF_ONDEMAND_REDO_DONE;
        ereport(DEBUG1, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
            errmsg("checkBlockRedoStateAndTryHashMapLock, block redo done or need to redo: spc/db/rel/bucket fork-block: %u/%u/%u/%d %d-%u.",
                redoItemTag.rNode.spcNode, redoItemTag.rNode.dbNode, redoItemTag.rNode.relNode, redoItemTag.rNode.bucketNode,
                redoItemTag.forkNum, redoItemTag.blockNum)));
        return ONDEMAND_HASHMAP_ENTRY_REDO_DONE;
    }

    /* get partition lock failed, means other process is redoing this buffer, retun REDOING */
    return ONDEMAND_HASHMAP_ENTRY_REDOING;
}

/*
 * Check if the block has been redo done, return true if redo done, return false if may be need to redo,
 * getSharedLock = false if other process is holding partition lock, may be other process is redoing.
 */
bool checkBlockRedoDoneFromHashMap(LWLock *lock, HTAB *hashMap, RedoItemTag redoItemTag, bool *getSharedLock)
{
    bool hashFound = false;
    if (!LWLockConditionalAcquire(lock, LW_SHARED)) {
        *getSharedLock = false;
        return false;
    }
    *getSharedLock = true;

    RedoItemHashEntry *entry = (RedoItemHashEntry *)hash_search(hashMap, (void *)&redoItemTag, HASH_FIND, &hashFound);

    /* Page is already up-to-date, no need to replay. */
    if (!hashFound || entry->redoItemNum == 0 || entry->redoDone) {
        LWLockRelease(lock);
        return true;
    }
    LWLockRelease(lock);
    return false;
}

/*
 * Try to get partition exclusive lock, and return if has got it.
 * noNeedRedo = true if no need to redo and release to partition lock.
 */
bool tryLockHashMap(LWLock *lock, HTAB *hashMap, RedoItemTag redoItemTag, bool *noNeedRedo) {
    bool hashFound = false;

    if (!LWLockConditionalAcquire(lock, LW_EXCLUSIVE)) {
        *noNeedRedo = false;
        return false;
    }
    RedoItemHashEntry *entry = (RedoItemHashEntry *)hash_search(hashMap, (void *)&redoItemTag, HASH_FIND, &hashFound);

    // Page is already up-to-date, no need to lock.
    if (!hashFound || entry->redoItemNum == 0 || entry->redoDone) {
        LWLockRelease(lock);
        *noNeedRedo = true;
        return false;
    }
    return true;
}

// if holdLock is false, only lock if need redo; otherwise, lock anyway
bool checkBlockRedoDoneFromHashMapAndLock(LWLock **lock, RedoItemTag redoItemTag, RedoItemHashEntry **redoItemEntry,
    bool holdLock)
{
    bool hashFound = false;
    uint32 id = GetSlotId(redoItemTag.rNode, 0, 0, GetBatchCount());
    HTAB *hashMap = g_instance.comm_cxt.predo_cxt.redoItemHashCtrl[id]->hTab;
    if (hashMap == NULL) {
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                        errmsg("redo item hash table corrupted, there has invalid hashtable.")));
    }
    unsigned int new_hash = XlogTrackTableHashCode(&redoItemTag);
    *lock = XlogTrackMappingPartitionLock(new_hash);

    /*
     * Backends may have locked when bufferalloc, if the block need to be redone
     * in ondemand, so need to lock again.
     */
    if (LWLockHeldByMe(*lock)) {
        Assert(LWLockHeldByMeInMode(*lock, LW_EXCLUSIVE));
        ereport(DEBUG1, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                errmsg("checkBlockRedoDoneFromHashMapAndLock, partitionLock has been locked when pinbuffer: spc/db/rel/bucket fork-block: %u/%u/%u/%d %d-%u.",
                        redoItemTag.rNode.spcNode, redoItemTag.rNode.dbNode, redoItemTag.rNode.relNode, redoItemTag.rNode.bucketNode,
                        redoItemTag.forkNum, redoItemTag.blockNum)));
        RedoItemHashEntry *entry = (RedoItemHashEntry *)hash_search(hashMap, (void *)&redoItemTag, HASH_FIND, &hashFound);

        /* Page is already up-to-date, no need to replay. */
        if (!hashFound || entry->redoItemNum == 0 || entry->redoDone) {
            if (!holdLock) {
                LWLockRelease(*lock);
                *lock = NULL;
            }
            return true;
        }

        if (redoItemEntry != NULL) {
            *redoItemEntry = entry;
        }
        return false;
    }

    (void)LWLockAcquire(*lock, LW_SHARED);
    RedoItemHashEntry *entry = (RedoItemHashEntry *)hash_search(hashMap, (void *)&redoItemTag, HASH_FIND, &hashFound);

    /* Page is already up-to-date, no need to replay. */
    if (!hashFound || entry->redoItemNum == 0 || entry->redoDone) {
        if (!holdLock) {
            LWLockRelease(*lock);
            *lock = NULL;
        }
        return true;
    }

    // switch to exclusive lock in replay
    LWLockRelease(*lock);
    (void)LWLockAcquire(*lock, LW_EXCLUSIVE);
    
    // check again
    if (entry->redoItemNum == 0 || entry->redoDone) {
        if (!holdLock) {
            LWLockRelease(*lock);
            *lock = NULL;
        }
        return true;
    }

    if (redoItemEntry != NULL) {
        *redoItemEntry = entry;
    }

    ereport(DEBUG1, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
        errmsg("checkBlockRedoDoneFromHashMapAndLock, block need to redo and has got partition lock: spc/db/rel/bucket fork-block: %u/%u/%u/%d %d-%u.",
                redoItemTag.rNode.spcNode, redoItemTag.rNode.dbNode, redoItemTag.rNode.relNode, redoItemTag.rNode.bucketNode,
                redoItemTag.forkNum, redoItemTag.blockNum)));
    return false;
}

static inline void XLogRecGetRedoItemTag(XLogRecParseState *redoblockstate, RedoItemTag *redoItemTag)
{
    XLogBlockParse *blockparse = &(redoblockstate->blockparse);

    redoItemTag->rNode.dbNode = blockparse->blockhead.dbNode;
    redoItemTag->rNode.relNode = blockparse->blockhead.relNode;
    redoItemTag->rNode.spcNode = blockparse->blockhead.spcNode;
    redoItemTag->rNode.bucketNode = blockparse->blockhead.bucketNode;
    redoItemTag->rNode.opt = blockparse->blockhead.opt;

    redoItemTag->forkNum = blockparse->blockhead.forknum;
    redoItemTag->blockNum = blockparse->blockhead.blkno;
}

static inline bool IsXLogRecSameRedoBlock(XLogRecParseState *redoblockstate1, XLogRecParseState *redoblockstate2)
{
    RedoItemTag redoItemTag1;
    RedoItemTag redoItemTag2;

    if (redoblockstate1 == NULL || redoblockstate2 == NULL) {
        return false;
    }

    XLogRecGetRedoItemTag(redoblockstate1, &redoItemTag1);
    XLogRecGetRedoItemTag(redoblockstate2, &redoItemTag2);

    if (memcmp(&redoItemTag1, &redoItemTag2, sizeof(RedoItemTag)) != 0) {
        return false;
    }
    return true;
}

static inline bool IsProcInHashMap(XLogRecParseState *procState)
{
    bool result = false;
    switch (XLogBlockHeadGetValidInfo(&procState->blockparse.blockhead)) {
        case BLOCK_DATA_MAIN_DATA_TYPE:
        case BLOCK_DATA_UNDO_TYPE:
        case BLOCK_DATA_VM_TYPE:
        case BLOCK_DATA_FSM_TYPE:
            result = true;
            break;
        default:
            break;
    }

    return result;
}

void RedoPageWorkerMain()
{
    (void)RegisterRedoInterruptCallBack(HandlePageRedoInterrupts);

    if (ParseStateWithoutCache()) {
        XLogRedoBufferInitFunc(&(g_redoWorker->bufferManager), MAX_LOCAL_BUFF_NUM, &recordRefOperate,
                               RedoInterruptCallBack);
    }

    XLogRecParseState *redoblockstateHead = NULL;
    LWLock *xlog_partition_lock = NULL;
    GetRedoStartTime(g_redoWorker->timeCostList[TIME_COST_STEP_1]);
    while ((redoblockstateHead = (XLogRecParseState *)SPSCBlockingQueueTop(g_redoWorker->queue)) !=
           (XLogRecParseState *)&g_redoEndMark) {
        CountAndGetRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_1], g_redoWorker->timeCostList[TIME_COST_STEP_2]);
        if ((void *)redoblockstateHead == (void *)&g_cleanupMark) {
            g_redoWorker->xlogInvalidPages = XLogGetInvalidPages();
            SPSCBlockingQueuePop(g_redoWorker->queue);
            ereport(LOG, (errcode(ERRCODE_LOG), errmsg("[ForceFinish]RedoPageWorkerMain has cleaned InvalidPages")));
            continue;
        }

        if ((void *)redoblockstateHead == (void *)&g_closefdMark) {
            smgrcloseall();
            SPSCBlockingQueuePop(g_redoWorker->queue);
            continue;
        }

        if ((void *)redoblockstateHead == (void *)&g_cleanInvalidPageMark) {
            forget_range_invalid_pages((void *)redoblockstateHead);
            SPSCBlockingQueuePop(g_redoWorker->queue);
            continue;
        }
        if ((void *)redoblockstateHead == (void *)&g_GlobalLsnForwarder) {
            PageWorkerProcLsnForwarder((RedoItem *)redoblockstateHead);
            SPSCBlockingQueuePop(g_redoWorker->queue);
            continue;
        }
        RedoBufferInfo bufferinfo = {0};
        bool notfound = false;
        bool updateFsm = false;
        bool needRelease = true;
        bool redoDone = false;

        XLogRecParseState *procState = redoblockstateHead;
        XLogRecParseState *reloadBlockState = NULL;
        Assert(procState->distributeStatus != XLOG_NO_DISTRIBUTE);

        MemoryContext oldCtx = MemoryContextSwitchTo(g_redoWorker->oldCtx);
        while (procState != NULL) {
            XLogRecParseState *redoblockstate = procState;
            // nextrecord will be redo in backwards position
            procState = (procState->distributeStatus == XLOG_TAIL_DISTRIBUTE) ?
                NULL : (XLogRecParseState *)procState->nextrecord;
            if (xlog_partition_lock == NULL && SS_ONDEMAND_BUILD_DONE && IsProcInHashMap(redoblockstate)) {
                RedoItemTag redoItemTag;
                XLogRecGetRedoItemTag(redoblockstate, &redoItemTag);
                redoDone = checkBlockRedoDoneFromHashMapAndLock(&xlog_partition_lock, redoItemTag, NULL, true);
            }

            if (redoDone) {
                Assert(xlog_partition_lock != NULL);
                needRelease = false;
                DereferenceRecParseState(redoblockstate);
                SetCompletedReadEndPtr(g_redoWorker, redoblockstate->blockparse.blockhead.start_ptr,
                    redoblockstate->blockparse.blockhead.end_ptr);
                goto redo_done;
            }

            switch (XLogBlockHeadGetValidInfo(&redoblockstate->blockparse.blockhead)) {
                case BLOCK_DATA_MAIN_DATA_TYPE:
                case BLOCK_DATA_UNDO_TYPE:
                case BLOCK_DATA_VM_TYPE:
                case BLOCK_DATA_FSM_TYPE:
                    needRelease = false;
                    GetRedoStartTime(g_redoWorker->timeCostList[TIME_COST_STEP_3]);
                    // reload from disk, because RedoPageManager already release refrecord in on-demand build stage
                    reloadBlockState = OndemandRedoReloadXLogRecord(redoblockstate);
                    notfound = XLogBlockRedoForExtremeRTO(reloadBlockState, &bufferinfo, notfound, 
                        g_redoWorker->timeCostList[TIME_COST_STEP_4], g_redoWorker->timeCostList[TIME_COST_STEP_5]);
                    OndemandRedoReleaseXLogRecord(reloadBlockState);
                    DereferenceRecParseState(redoblockstate);
                    SetCompletedReadEndPtr(g_redoWorker, redoblockstate->blockparse.blockhead.start_ptr,
                                                   redoblockstate->blockparse.blockhead.end_ptr);
                    CountRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_3]);
                    break;
                case BLOCK_DATA_XLOG_COMMON_TYPE:
                    GetRedoStartTime(g_redoWorker->timeCostList[TIME_COST_STEP_6]);
                    RedoPageWorkerCheckPoint(redoblockstate);
                    SetCompletedReadEndPtr(g_redoWorker, redoblockstate->blockparse.blockhead.start_ptr,
                                           redoblockstate->blockparse.blockhead.end_ptr);
                    CountRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_6]);
                    break;
                case BLOCK_DATA_DDL_TYPE:
                    GetRedoStartTime(g_redoWorker->timeCostList[TIME_COST_STEP_6]);
                    XLogForgetDDLRedo(redoblockstate);
                    SetCompletedReadEndPtr(g_redoWorker, redoblockstate->blockparse.blockhead.start_ptr,
                                           redoblockstate->blockparse.blockhead.end_ptr);
                    CountRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_6]);
                    break;
                case BLOCK_DATA_DROP_DATABASE_TYPE:
                    GetRedoStartTime(g_redoWorker->timeCostList[TIME_COST_STEP_6]);
                    XLogDropDatabase(redoblockstate->blockparse.blockhead.dbNode);
                    SetCompletedReadEndPtr(g_redoWorker, redoblockstate->blockparse.blockhead.start_ptr,
                                           redoblockstate->blockparse.blockhead.end_ptr);
                    CountRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_6]);
                    break;
                case BLOCK_DATA_NEWCU_TYPE:
                    GetRedoStartTime(g_redoWorker->timeCostList[TIME_COST_STEP_6]);
                    RedoPageWorkerRedoBcmBlock(redoblockstate);
                    CountRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_6]);
                    break;
                case BLOCK_DATA_SEG_SPACE_DROP:
                    GetRedoStartTime(g_redoWorker->timeCostList[TIME_COST_STEP_6]);
                    XLogDropSegmentSpace(redoblockstate->blockparse.blockhead.spcNode,
                                         redoblockstate->blockparse.blockhead.dbNode);
                    SetCompletedReadEndPtr(g_redoWorker, redoblockstate->blockparse.blockhead.start_ptr,
                                           redoblockstate->blockparse.blockhead.end_ptr);
                    CountRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_6]);
                    break;
                case BLOCK_DATA_SEG_SPACE_SHRINK:
                    XLogDropSpaceShrink(redoblockstate);
                    SetCompletedReadEndPtr(g_redoWorker, redoblockstate->blockparse.blockhead.start_ptr,
                                           redoblockstate->blockparse.blockhead.end_ptr);
                    break;
                case BLOCK_DATA_BARRIER_TYPE:
                    GetRedoStartTime(g_redoWorker->timeCostList[TIME_COST_STEP_6]);
                    SetCompletedReadEndPtr(g_redoWorker, redoblockstate->blockparse.blockhead.start_ptr,
                                           redoblockstate->blockparse.blockhead.end_ptr);
                    CountRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_6]);
                    break;
                default:
                    break;
            }

redo_done:
            if (xlog_partition_lock != NULL && !IsXLogRecSameRedoBlock(redoblockstate, procState)) {
                LWLockRelease(xlog_partition_lock);
                xlog_partition_lock = NULL;
                redoDone = false;
            }
        }
        (void)MemoryContextSwitchTo(oldCtx);
        GetRedoStartTime(g_redoWorker->timeCostList[TIME_COST_STEP_7]);
        updateFsm = XlogNeedUpdateFsm(redoblockstateHead, &bufferinfo);
        bool needWait = redoblockstateHead->isFullSync;
        if (needWait) {
            pg_atomic_write_u32(&g_redoWorker->fullSyncFlag, 1);
        }
        if (needRelease) {
            XLogBlockParseStateRelease(redoblockstateHead);
        }
        /* the same page */
        ExtremeRtoFlushBuffer(&bufferinfo, updateFsm);
        SPSCBlockingQueuePop(g_redoWorker->queue);
        CountRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_7]);
        pg_memory_barrier();
        GetRedoStartTime(g_redoWorker->timeCostList[TIME_COST_STEP_8]);
        uint32 val = pg_atomic_read_u32(&g_redoWorker->fullSyncFlag);
        while (val != 0) {
            RedoInterruptCallBack();
            val = pg_atomic_read_u32(&g_redoWorker->fullSyncFlag);
        }
        CountRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_8]);
        RedoInterruptCallBack();
        CountAndGetRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_2], g_redoWorker->timeCostList[TIME_COST_STEP_1]);
        ADD_ABNORMAL_POSITION(4);
    }

    SPSCBlockingQueuePop(g_redoWorker->queue);
    if (ParseStateWithoutCache())
        XLogRedoBufferDestoryFunc(&(g_redoWorker->bufferManager));
}

void PutRecordToReadQueue(XLogReaderState *recordreader)
{
    SPSCBlockingQueuePut(g_dispatcher->readLine.readPageThd->queue, recordreader);
}

inline void InitXLogRecordReadBuffer(XLogReaderState **initreader)
{
    XLogReaderState *newxlogreader;
    XLogReaderState *readstate = g_dispatcher->rtoXlogBufState.initreader;
    newxlogreader = NewReaderState(readstate);
    g_dispatcher->rtoXlogBufState.initreader = NULL;
    PutRecordToReadQueue(readstate);
    SetCompletedReadEndPtr(g_redoWorker, readstate->ReadRecPtr, readstate->EndRecPtr);
    *initreader = newxlogreader;
}

void StartupSendFowarder(RedoItem *item)
{
    for (uint32 i = 0; i < g_dispatcher->pageLineNum; ++i) {
        AddPageRedoItem(g_dispatcher->pageLines[i].batchThd, item);
    }

    AddPageRedoItem(g_dispatcher->trxnLine.managerThd, item);
    AddPageRedoItem(g_dispatcher->auxiliaryLine.ctrlThd, item);
}

void SendLsnFowarder()
{
    // update and read in the same thread, so no need atomic operation
    g_GlobalLsnForwarder.record.ReadRecPtr = g_redoWorker->lastReplayedReadRecPtr;
    g_GlobalLsnForwarder.record.EndRecPtr = g_redoWorker->lastReplayedEndRecPtr;
    g_GlobalLsnForwarder.record.refcount = get_real_recovery_parallelism() - XLOG_READER_NUM;
    g_GlobalLsnForwarder.record.isDecode = true;
    PutRecordToReadQueue(&g_GlobalLsnForwarder.record);
}

static inline bool ReadPageWorkerStop()
{
    return g_dispatcher->recoveryStop;
}

void PushToWorkerLsn()
{
    if (!IsExtremeRtoRunning()) {
        return;
    }

    uint32 refCount;
    do {
        refCount = pg_atomic_read_u32(&g_GlobalLsnForwarder.record.refcount);
        RedoInterruptCallBack();
    } while (refCount != 0 && !ReadPageWorkerStop());
    SendLsnFowarder();
}

void ResetRtoXlogReadBuf(XLogRecPtr targetPagePtr)
{
    uint32 startreadworker = pg_atomic_read_u32(&(g_recordbuffer->readWorkerState));
    if (startreadworker == WORKER_STATE_STOP) {
        WalRcvCtlAcquireExitLock();
        WalRcvCtlBlock *walrcb = getCurrentWalRcvCtlBlock();

        if (walrcb == NULL) {
            WalRcvCtlReleaseExitLock();
            return;
        }

        int64 walwriteoffset;
        XLogRecPtr startptr;
        SpinLockAcquire(&walrcb->mutex);
        walwriteoffset = walrcb->walWriteOffset;
        startptr = walrcb->walStart;
        SpinLockRelease(&walrcb->mutex);

        if (XLByteLT(startptr, targetPagePtr)) {
            WalRcvCtlReleaseExitLock();
            return;
        }


        for (uint32 i = 0; i < MAX_ALLOC_SEGNUM; ++i) {
            pg_atomic_write_u32(&(g_recordbuffer->xlogsegarray[i].bufState), NONE);
        }

        XLogSegNo segno;
        XLByteToSeg(targetPagePtr, segno);
        g_recordbuffer->xlogsegarray[g_recordbuffer->applyindex].segno = segno;
        g_recordbuffer->xlogsegarray[g_recordbuffer->applyindex].readlen = targetPagePtr % XLogSegSize;

        pg_atomic_write_u32(&(g_recordbuffer->readindex), g_recordbuffer->applyindex);
        pg_atomic_write_u32(&(g_recordbuffer->xlogsegarray[g_recordbuffer->readindex].bufState), APPLYING);

        pg_atomic_write_u32(&(g_recordbuffer->readWorkerState), WORKER_STATE_RUN);
        WalRcvCtlReleaseExitLock();
    }
}

RecordBufferAarray *GetCurrentSegmentBuf(XLogRecPtr targetPagePtr)
{
    Assert(g_recordbuffer->applyindex < MAX_ALLOC_SEGNUM);
    uint32 applyindex = g_recordbuffer->applyindex;
    RecordBufferAarray *cursegbuffer = &g_recordbuffer->xlogsegarray[applyindex];
    uint32 bufState = pg_atomic_read_u32(&(cursegbuffer->bufState));

    if (bufState != APPLYING) {
        return NULL;
    }
    uint32 targetPageOff = (targetPagePtr % XLogSegSize);
    XLogSegNo targetSegNo;
    XLByteToSeg(targetPagePtr, targetSegNo);
    if (cursegbuffer->segno == targetSegNo) {
        cursegbuffer->segoffset = targetPageOff;
        return cursegbuffer;
    } else if (cursegbuffer->segno + 1 == targetSegNo) {
        Assert(targetPageOff == 0);
        pg_atomic_write_u32(&(cursegbuffer->bufState), APPLIED);
        if ((applyindex + 1) == MAX_ALLOC_SEGNUM) {
            applyindex = 0;
        } else {
            applyindex++;
        }

        pg_atomic_write_u32(&(g_recordbuffer->applyindex), applyindex);
        cursegbuffer = &g_recordbuffer->xlogsegarray[applyindex];
        bufState = pg_atomic_read_u32(&(cursegbuffer->bufState));
        if (bufState != APPLYING) {
            return NULL;
        }

        Assert(cursegbuffer->segno == targetSegNo);
        cursegbuffer->segoffset = targetPageOff;
        return cursegbuffer;
    } else {
        ereport(WARNING, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                          errmsg("SetReadBufferForExtRto targetPagePtr:%lu", targetPagePtr)));
        DumpExtremeRtoReadBuf();
        t_thrd.xlog_cxt.failedSources |= XLOG_FROM_STREAM;
        return NULL;
    }
}

static const int MAX_WAIT_TIMS = 512;

bool XLogPageReadForExtRto(XLogReaderState *xlogreader, XLogRecPtr targetPagePtr, int reqLen)
{
    uint32 startreadworker = pg_atomic_read_u32(&(g_recordbuffer->readWorkerState));
    if (startreadworker == WORKER_STATE_RUN) {
        RecordBufferAarray *cursegbuffer = GetCurrentSegmentBuf(targetPagePtr);
        if (cursegbuffer == NULL) {
            return false;
        }

        uint32 readlen = pg_atomic_read_u32(&(cursegbuffer->readlen));

        uint32 waitcount = 0;
        while (readlen < (cursegbuffer->segoffset + reqLen)) {
            readlen = pg_atomic_read_u32(&(cursegbuffer->readlen));
            if (waitcount >= MAX_WAIT_TIMS) {
                return false;
            }
            waitcount++;
        }

        Assert(cursegbuffer->segoffset == (targetPagePtr % XLogSegSize));
        xlogreader->readBuf = cursegbuffer->readsegbuf + cursegbuffer->segoffset;
        return true;
    }

    return false;
}

void XLogReadWorkerSegFallback(XLogSegNo lastRplSegNo)
{
    errno_t errorno = EOK;
    uint32 readindex = pg_atomic_read_u32(&(g_recordbuffer->readindex));
    uint32 applyindex = pg_atomic_read_u32(&(g_recordbuffer->applyindex));
    RecordBufferAarray *readseg = &g_recordbuffer->xlogsegarray[readindex];
    RecordBufferAarray *applyseg = &g_recordbuffer->xlogsegarray[applyindex];

    ereport(LOG, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                  errmsg("XLogReadWorkerSegFallback: readindex: %u, readseg[%lu,%lu,%u,%u], applyindex: %u,"
                         "applyseg[%lu,%lu,%u,%u]",
                         readindex, readseg->segno, readseg->segoffset, readseg->readlen, readseg->bufState, applyindex,
                         applyseg->segno, applyseg->segoffset, applyseg->readlen, applyseg->bufState)));

    pg_atomic_write_u32(&(g_recordbuffer->readindex), applyindex);
    pg_atomic_write_u32(&(readseg->bufState), APPLIED);
    applyseg->segno = lastRplSegNo;
    applyseg->readlen = applyseg->segoffset;
    errorno = memset_s(applyseg->readsegbuf, XLogSegSize, 0, XLogSegSize);
    securec_check(errorno, "", "");
}

bool CloseReadFile()
{
    if (t_thrd.xlog_cxt.readFile >= 0) {
        close(t_thrd.xlog_cxt.readFile);
        t_thrd.xlog_cxt.readFile = -1;
        return true;
    }
    return false;
}

void DispatchCleanupMarkToAllRedoWorker()
{
    for (uint32 i = 0; i < g_dispatcher->allWorkersCnt; i++) {
        PageRedoWorker *worker = g_dispatcher->allWorkers[i];
        if (worker->role == REDO_PAGE_WORKER) {
            SPSCBlockingQueuePut(worker->queue, &g_cleanupMark);
        }
    }
}

void DispatchClosefdMarkToAllRedoWorker()
{
    for (uint32 i = 0; i < g_dispatcher->allWorkersCnt; i++) {
        PageRedoWorker *worker = g_dispatcher->allWorkers[i];
        if (worker->role == REDO_PAGE_WORKER || worker->role == REDO_PAGE_MNG ||
            worker->role == REDO_TRXN_MNG || worker->role == REDO_TRXN_WORKER) {
            SPSCBlockingQueuePut(worker->queue, &g_closefdMark);
        }
    }
}

void DispatchCleanInvalidPageMarkToAllRedoWorker(RepairFileKey key)
{
    for (uint32 i = 0; i < g_dispatcher->allWorkersCnt; i++) {
        PageRedoWorker *worker = g_dispatcher->allWorkers[i];
        if (worker->role == REDO_PAGE_WORKER) {
            errno_t rc = memcpy_s((char*)&g_cleanInvalidPageMark,
                sizeof(RepairFileKey), (char*)&key, sizeof(RepairFileKey));
            securec_check(rc, "", "");
            SPSCBlockingQueuePut(worker->queue, &g_cleanInvalidPageMark);
        }
    }
}

void WaitAllRedoWorkerIdle()
{
    instr_time startTime;
    instr_time endTime;
    bool allIdle = false;
    INSTR_TIME_SET_CURRENT(startTime);
    ereport(LOG, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                  errmsg("WaitAllRedoWorkerIdle begin, startTime: %lu us", INSTR_TIME_GET_MICROSEC(startTime))));
    while (!allIdle) {
        allIdle = true;
        for (uint32 i = 0; i < g_dispatcher->allWorkersCnt; i++) {
            PageRedoWorker *worker = g_dispatcher->allWorkers[i];
            if (worker->role == REDO_READ_WORKER || worker->role == REDO_READ_MNG) {
                continue;
            }
            if (!RedoWorkerIsIdle(worker)) {
                allIdle = false;
                break;
            }
        }
        RedoInterruptCallBack();
    }
    INSTR_TIME_SET_CURRENT(endTime);
    INSTR_TIME_SUBTRACT(endTime, startTime);
    ereport(LOG, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                  errmsg("WaitAllRedoWorkerIdle end, cost time: %lu us", INSTR_TIME_GET_MICROSEC(endTime))));
}

void WaitAllReplayWorkerIdle()
{
    instr_time startTime;
    instr_time endTime;
    bool allIdle = false;
    INSTR_TIME_SET_CURRENT(startTime);
    ereport(LOG, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                  errmsg("WaitAllReplayWorkerIdle begin, startTime: %lu us", INSTR_TIME_GET_MICROSEC(startTime))));
    while (!allIdle) {
        allIdle = true;
        for (uint32 i = 0; i < g_dispatcher->allWorkersCnt; i++) {
            PageRedoWorker *worker = g_dispatcher->allWorkers[i];
            if (worker->role == REDO_READ_WORKER || worker->role == REDO_READ_MNG ||
                worker->role == REDO_READ_PAGE_WORKER) {
                continue;
            }
            if (!RedoWorkerIsIdle(worker)) {
                allIdle = false;
                break;
            }
        }
        RedoInterruptCallBack();
    }
    INSTR_TIME_SET_CURRENT(endTime);
    INSTR_TIME_SUBTRACT(endTime, startTime);
    ereport(LOG, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                  errmsg("WaitAllReplayWorkerIdle end, cost time: %lu us", INSTR_TIME_GET_MICROSEC(endTime))));
}


void XLogForceFinish(XLogReaderState *xlogreader, TermFileData *term_file)
{
    bool closed = false;
    uint32 termId = term_file->term;
    XLogSegNo lastRplSegNo;

    pg_atomic_write_u32(&(ondemand_extreme_rto::g_recordbuffer->readWorkerState), ondemand_extreme_rto::WORKER_STATE_STOPPING);
    while (pg_atomic_read_u32(&(ondemand_extreme_rto::g_recordbuffer->readWorkerState)) != WORKER_STATE_STOP) {
        RedoInterruptCallBack();
    };
    ShutdownWalRcv();
    ShutdownDataRcv();
    pg_atomic_write_u32(&(g_recordbuffer->readSource), XLOG_FROM_PG_XLOG);

    PushToWorkerLsn();
    g_cleanupMark.record.isDecode = true;
    PutRecordToReadQueue(&g_cleanupMark.record);
    WaitAllRedoWorkerIdle();

    XLogRecPtr lastRplReadLsn;
    XLogRecPtr lastRplEndLsn = GetXLogReplayRecPtr(NULL, &lastRplReadLsn);
    XLogRecPtr receivedUpto = GetWalRcvWriteRecPtr(NULL);
    XLogRecPtr endRecPtr = xlogreader->EndRecPtr;
    ereport(WARNING, (errcode(ERRCODE_LOG), errmsg("[ForceFinish]ArchiveXlogForForceFinishRedo in extremeRTO "
        "lastRplReadLsn:%08X/%08X, lastRplEndLsn:%08X/%08X, receivedUpto:%08X/%08X, ReadRecPtr:%08X/%08X, "
        "EndRecPtr:%08X/%08X, readOff:%u, latestValidRecord:%08X/%08X",
        (uint32)(lastRplReadLsn >> 32), (uint32)lastRplReadLsn,(uint32)(lastRplEndLsn >> 32), (uint32)lastRplEndLsn,
        (uint32)(receivedUpto >> 32), (uint32)receivedUpto,(uint32)(xlogreader->ReadRecPtr >> 32), 
        (uint32)xlogreader->ReadRecPtr, (uint32)(xlogreader->EndRecPtr >> 32), (uint32)xlogreader->EndRecPtr, 
        xlogreader->readOff, (uint32)(latestValidRecord >> 32), (uint32)latestValidRecord)));
    DumpExtremeRtoReadBuf();
    xlogreader->readOff = INVALID_READ_OFF;
    XLByteToSeg(endRecPtr, lastRplSegNo);
    XLogReadWorkerSegFallback(lastRplSegNo);

    closed = CloseReadFile();
    CopyXlogForForceFinishRedo(lastRplSegNo, termId, xlogreader, endRecPtr);
    RenameXlogForForceFinishRedo(lastRplSegNo, xlogreader->readPageTLI, termId);
    if (closed) {
        ReOpenXlog(xlogreader);
    }
    t_thrd.xlog_cxt.invaildPageCnt = 0;
    XLogCheckInvalidPages();
    SetSwitchHistoryFile(endRecPtr, receivedUpto, termId);
    t_thrd.xlog_cxt.invaildPageCnt = 0;
    set_wal_rcv_write_rec_ptr(endRecPtr);
    t_thrd.xlog_cxt.receivedUpto = endRecPtr;
    pg_atomic_write_u32(&(g_instance.comm_cxt.localinfo_cxt.is_finish_redo), 0);
    ereport(WARNING,
            (errcode(ERRCODE_LOG), errmsg("[ForceFinish]ArchiveXlogForForceFinishRedo in extremeRTO is over")));
}

static void DoCleanUpReadPageWorkerQueue(SPSCBlockingQueue *queue)
{
    while (!SPSCBlockingQueueIsEmpty(queue)) {
        XLogReaderState *xlogreader = reinterpret_cast<XLogReaderState *>(SPSCBlockingQueueTake(queue));
        if (xlogreader == reinterpret_cast<XLogReaderState *>(&(g_redoEndMark.record)) || 
            xlogreader == reinterpret_cast<XLogReaderState *>(&(g_GlobalLsnForwarder.record)) ||
            xlogreader == reinterpret_cast<XLogReaderState *>(&(g_cleanupMark.record))) {
            if (xlogreader == reinterpret_cast<XLogReaderState *>(&(g_GlobalLsnForwarder.record))) {
                pg_atomic_write_u32(&g_GlobalLsnForwarder.record.refcount, 0);
            }
            continue;
        }
    
        RedoItem *item = GetRedoItemPtr(xlogreader);
        FreeRedoItem(item);
    }
}

void CleanUpReadPageWorkerQueue()
{
    SPSCBlockingQueue *queue = g_dispatcher->readLine.readPageThd->queue;
    uint32 state;
    do {
        DoCleanUpReadPageWorkerQueue(queue);
        RedoInterruptCallBack();
        state = pg_atomic_read_u32(&ondemand_extreme_rto::g_dispatcher->rtoXlogBufState.readPageWorkerState);
    } while (state != WORKER_STATE_EXIT);
    /* Processing the state change after the queue is cleared */
    DoCleanUpReadPageWorkerQueue(queue);
}

void ExtremeRtoStopHere()
{
    if ((get_real_recovery_parallelism() > 1) && (GetBatchCount() > 0)) {
        g_dispatcher->recoveryStop = true;
        CleanUpReadPageWorkerQueue();
    }
}

static void CheckAndDoForceFinish(XLogReaderState *xlogreader)
{
    TermFileData term_file;
    if (CheckForForceFinishRedoTrigger(&term_file)) {
        ereport(WARNING,
                (errmsg("[ForceFinish] force finish triggered in XLogReadPageWorkerMain, ReadRecPtr:%08X/%08X, "
                        "EndRecPtr:%08X/%08X, StandbyMode:%u, startup_processing:%u, dummyStandbyMode:%u",
                        (uint32)(t_thrd.xlog_cxt.ReadRecPtr >> 32), (uint32)t_thrd.xlog_cxt.ReadRecPtr,
                        (uint32)(t_thrd.xlog_cxt.EndRecPtr >> 32), (uint32)t_thrd.xlog_cxt.EndRecPtr,
                        t_thrd.xlog_cxt.StandbyMode, t_thrd.xlog_cxt.startup_processing, dummyStandbyMode)));
        XLogForceFinish(xlogreader, &term_file);
    }
}

/* read xlog for parellel */
void XLogReadPageWorkerMain()
{
    XLogReaderState *xlogreader = NULL;

    (void)RegisterRedoInterruptCallBack(HandlePageRedoInterrupts);

    g_recordbuffer = &g_dispatcher->rtoXlogBufState;
    GetRecoveryLatch();
    if (ENABLE_ONDEMAND_REALTIME_BUILD) {
        on_shmem_exit(RealtimeBuildReleaseRecoveryLatch, 0);
    }
    /* init readstate */
    InitXLogRecordReadBuffer(&xlogreader);

    pg_atomic_write_u32(&(g_recordbuffer->readPageWorkerState), WORKER_STATE_RUN);

    XLogRecord *record = XLogParallelReadNextRecord(xlogreader);
    while (record != NULL) {
        if (ReadPageWorkerStop()) {
            break;
        }
        GetRedoStartTime(g_redoWorker->timeCostList[TIME_COST_STEP_3]);
        XLogReaderState *newxlogreader = NewReaderState(xlogreader);
        CountAndGetRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_3], g_redoWorker->timeCostList[TIME_COST_STEP_4]);
        PutRecordToReadQueue(xlogreader);
        CountAndGetRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_4], g_redoWorker->timeCostList[TIME_COST_STEP_5]);
        xlogreader = newxlogreader;

        g_redoWorker->lastReplayedReadRecPtr = xlogreader->ReadRecPtr;
        g_redoWorker->lastReplayedEndRecPtr = xlogreader->EndRecPtr;

        if (FORCE_FINISH_ENABLED) {
            CheckAndDoForceFinish(xlogreader);
        }
        CountAndGetRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_5], g_redoWorker->timeCostList[TIME_COST_STEP_1]);
        record = XLogParallelReadNextRecord(xlogreader);
        CountAndGetRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_1], g_redoWorker->timeCostList[TIME_COST_STEP_2]);
        CountRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_2]);
        RedoInterruptCallBack();
        ADD_ABNORMAL_POSITION(8);
    }

    uint32 workState = pg_atomic_read_u32(&(g_recordbuffer->readWorkerState));
    while (workState == WORKER_STATE_STOPPING) {
        workState = pg_atomic_read_u32(&(g_recordbuffer->readWorkerState));
    }

    if (workState != WORKER_STATE_EXITING && workState != WORKER_STATE_EXIT) {
        pg_atomic_write_u32(&(g_recordbuffer->readWorkerState), WORKER_STATE_EXITING);
    }
    if (!ReadPageWorkerStop()) {
        /* notify exit */
        PushToWorkerLsn();
        g_redoEndMark.record = *xlogreader;
        g_redoEndMark.record.isDecode = true;
        PutRecordToReadQueue((XLogReaderState *)&g_redoEndMark.record);
    }

    ReLeaseRecoveryLatch();
    pg_atomic_write_u32(&(g_recordbuffer->readPageWorkerState), WORKER_STATE_EXIT);
}

void HandleReadWorkerRunInterrupts()
{
    if (t_thrd.page_redo_cxt.got_SIGHUP) {
        t_thrd.page_redo_cxt.got_SIGHUP = false;
        ProcessConfigFile(PGC_SIGHUP);
    }

    if (t_thrd.page_redo_cxt.shutdown_requested) {
        ereport(LOG, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                      errmsg("page worker id %u exit for request", g_redoWorker->id)));

        pg_atomic_write_u32(&(g_instance.comm_cxt.predo_cxt.pageRedoThreadStatusList[g_redoWorker->id].threadState),
                            PAGE_REDO_WORKER_EXIT);

        proc_exit(1);
    }
}

static void InitReadBuf(uint32 bufIndex, XLogSegNo segno)
{
    if (bufIndex == MAX_ALLOC_SEGNUM) {
        bufIndex = 0;
    }
    const uint32 sleepTime = 50; /* 50 us */
    RecordBufferAarray *nextreadseg = &g_recordbuffer->xlogsegarray[bufIndex];
    pg_memory_barrier();

    uint32 bufState = pg_atomic_read_u32(&(nextreadseg->bufState));
    uint32 startreadworker = pg_atomic_read_u32(&(g_recordbuffer->readWorkerState));
    while (bufState == APPLYING && startreadworker == WORKER_STATE_RUN) {
        pg_usleep(sleepTime);
        RedoInterruptCallBack();
        bufState = pg_atomic_read_u32(&(nextreadseg->bufState));
        startreadworker = pg_atomic_read_u32(&(g_recordbuffer->readWorkerState));
    }

    nextreadseg->readlen = 0;
    nextreadseg->segno = segno;
    nextreadseg->segoffset = 0;
    pg_atomic_write_u32(&(nextreadseg->bufState), APPLYING);
    pg_atomic_write_u32(&(g_recordbuffer->readindex), bufIndex);
}

static void XLogReadWorkRun()
{
    static uint32 waitcount = 0;
    const uint32 sleepTime = 100; /* 50 us */
    XLogSegNo targetSegNo;
    uint32 writeoffset;
    uint32 reqlen;
    GetRedoStartTime(g_redoWorker->timeCostList[TIME_COST_STEP_1]);
    uint32 readindex = pg_atomic_read_u32(&(g_recordbuffer->readindex));
    Assert(readindex < MAX_ALLOC_SEGNUM);
    pg_memory_barrier();
    RecordBufferAarray *readseg = &g_recordbuffer->xlogsegarray[readindex];

    XLogRecPtr receivedUpto = GetWalRcvWriteRecPtr(NULL);
    XLByteToSeg(receivedUpto, targetSegNo);

    if (targetSegNo < readseg->segno) {
        pg_usleep(sleepTime);
        return;
    }

    writeoffset = readseg->readlen;
    if (targetSegNo != readseg->segno) {
        reqlen = XLogSegSize - writeoffset;
    } else {
        uint32 targetPageOff = receivedUpto % XLogSegSize;
        if (targetPageOff <= writeoffset) {
            pg_usleep(sleepTime);
            return;
        }
        reqlen = targetPageOff - writeoffset;
        if (reqlen < XLOG_BLCKSZ) {
            waitcount++;
            uint32 flag = pg_atomic_read_u32(&g_readManagerTriggerFlag);
            if (waitcount < MAX_WAIT_TIMS && flag == TRIGGER_NORMAL) {
                pg_usleep(sleepTime);
                return;
            }
        }
    }

    waitcount = 0;
    char *readBuf = readseg->readsegbuf + writeoffset;
    XLogRecPtr targetSartPtr = readseg->segno * XLogSegSize + writeoffset;
    uint32 readlen = 0;
    GetRedoStartTime(g_redoWorker->timeCostList[TIME_COST_STEP_2]);
    bool result = XLogReadFromWriteBuffer(targetSartPtr, reqlen, readBuf, &readlen);
    CountRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_2]);
    if (!result) {
        return;
    }

    pg_atomic_write_u32(&(readseg->readlen), (writeoffset + readlen));
    if (readseg->readlen == XLogSegSize) {
        GetRedoStartTime(g_redoWorker->timeCostList[TIME_COST_STEP_3]);
        InitReadBuf(readindex + 1, readseg->segno + 1);
        CountRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_3]);
    }

    CountRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_1]);
}

void XLogReadManagerResponseSignal(uint32 tgigger)
{
    switch (tgigger) {
        case TRIGGER_PRIMARY:
            break;
        case TRIGGER_FAILOVER:
            if (t_thrd.xlog_cxt.is_cascade_standby) {
                SendPostmasterSignal(PMSIGNAL_UPDATE_PROMOTING);
                t_thrd.xlog_cxt.is_cascade_standby = false;
                if (t_thrd.postmaster_cxt.HaShmData->is_cross_region) {
                    t_thrd.xlog_cxt.is_hadr_main_standby = true;
                    SpinLockAcquire(&t_thrd.postmaster_cxt.HaShmData->mutex);
                    t_thrd.postmaster_cxt.HaShmData->is_hadr_main_standby = true;
                    SpinLockRelease(&t_thrd.postmaster_cxt.HaShmData->mutex);
                }
                t_thrd.xlog_cxt.failover_triggered = false;
                SendNotifySignal(NOTIFY_STANDBY, g_instance.pid_cxt.StartupPID);
                SendPostmasterSignal(PMSIGNAL_UPDATE_NORMAL);
                ereport(LOG, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                          errmsg("failover standby ready, notify postmaster to change state.")));
                break;
            }
            t_thrd.xlog_cxt.failover_triggered = true;
            SendPostmasterSignal(PMSIGNAL_UPDATE_PROMOTING);
            ereport(LOG, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                          errmsg("failover ready, notify postmaster to change state.")));
            break;
        case TRIGGER_SWITCHOVER:
            t_thrd.xlog_cxt.switchover_triggered = true;
            SendPostmasterSignal(PMSIGNAL_UPDATE_PROMOTING);
            ereport(LOG, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                          errmsg("switchover ready, notify postmaster to change state.")));
            break;
        default:
            break;
    }
}

void XLogReadManagerProcInterrupt()
{
    if (t_thrd.page_redo_cxt.shutdown_requested) {
        ereport(LOG, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                      errmsg("page worker id %u exit for request", g_redoWorker->id)));

        pg_atomic_write_u32(&(g_instance.comm_cxt.predo_cxt.pageRedoThreadStatusList[g_redoWorker->id].threadState),
                            PAGE_REDO_WORKER_EXIT);

        proc_exit(1);
    }

    if (t_thrd.page_redo_cxt.got_SIGHUP) {
        t_thrd.page_redo_cxt.got_SIGHUP = false;
        ProcessConfigFile(PGC_SIGHUP);
    }
}

void WaitPageReadWorkerExit()
{
    uint32 state;
    do {
        state = pg_atomic_read_u32(&ondemand_extreme_rto::g_dispatcher->rtoXlogBufState.readPageWorkerState);
        RedoInterruptCallBack();
    } while (state != WORKER_STATE_EXIT);
}

static void HandleExtremeRtoCascadeStandbyPromote(uint32 trigger)
{
    if (!t_thrd.xlog_cxt.is_cascade_standby || t_thrd.xlog_cxt.server_mode != STANDBY_MODE ||
        !IS_DN_MULTI_STANDYS_MODE()) {
        return;
    }

    ShutdownWalRcv();
    pg_atomic_write_u32(&g_dispatcher->rtoXlogBufState.waitRedoDone, 1);
    WakeupRecovery();
    XLogReadManagerResponseSignal(trigger);
    pg_atomic_write_u32(&g_startupTriggerState, TRIGGER_NORMAL);
}

bool XLogReadManagerCheckSignal()
{
    uint32 trigger = pg_atomic_read_u32(&g_startupTriggerState);
    load_server_mode();
    if (g_dispatcher->smartShutdown || trigger == TRIGGER_PRIMARY || trigger == TRIGGER_SWITCHOVER ||
        (trigger == TRIGGER_FAILOVER && t_thrd.xlog_cxt.server_mode == STANDBY_MODE) ||
        t_thrd.xlog_cxt.server_mode == PRIMARY_MODE) {
        ereport(LOG, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                      errmsg("XLogReadManagerCheckSignal: smartShutdown:%u, trigger:%u, server_mode:%u",
                             g_dispatcher->smartShutdown, trigger, t_thrd.xlog_cxt.server_mode)));
        if (t_thrd.xlog_cxt.is_cascade_standby && t_thrd.xlog_cxt.server_mode == STANDBY_MODE &&
            IS_DN_MULTI_STANDYS_MODE() && (trigger == TRIGGER_SWITCHOVER || trigger == TRIGGER_FAILOVER)) {
            HandleExtremeRtoCascadeStandbyPromote(trigger);
            return false;
        }
        ShutdownWalRcv();
        if (g_dispatcher->smartShutdown) {
            pg_atomic_write_u32(&g_readManagerTriggerFlag, TRIGGER_SMARTSHUTDOWN);
        } else {
            pg_atomic_write_u32(&g_readManagerTriggerFlag, trigger);
        }
        WakeupRecovery();
        WaitPageReadWorkerExit();
        XLogReadManagerResponseSignal(trigger);
        return true;
    }
    return false;
}

void StartRequestXLogFromStream()
{
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    XLogRecPtr expectLsn = pg_atomic_read_u64(&g_dispatcher->rtoXlogBufState.expectLsn);
    if (walrcv->receivedUpto == InvalidXLogRecPtr || 
        (expectLsn != InvalidXLogRecPtr && XLByteLE(walrcv->receivedUpto, expectLsn))) {
        uint32 readWorkerstate = pg_atomic_read_u32(&(g_recordbuffer->readWorkerState));
        if (readWorkerstate == WORKER_STATE_RUN) {
            pg_atomic_write_u32(&(g_recordbuffer->readWorkerState), WORKER_STATE_STOPPING);
        }
        SpinLockAcquire(&walrcv->mutex);
        walrcv->receivedUpto = 0;
        SpinLockRelease(&walrcv->mutex);
        XLogRecPtr targetRecPtr = pg_atomic_read_u64(&g_dispatcher->rtoXlogBufState.targetRecPtr);
        CheckMaxPageFlushLSN(targetRecPtr);

        uint32 shiftSize = 32;
        if (IS_OBS_DISASTER_RECOVER_MODE && !IsRoachRestore()) {
            ereport(LOG, (errmsg("request xlog stream from obs at %X/%X.", (uint32)(targetRecPtr >> shiftSize), 
                                 (uint32)targetRecPtr)));
            RequestXLogStreaming(&targetRecPtr, 0, REPCONNTARGET_OBS, 0);
        } else if (IS_SHARED_STORAGE_STANBY_MODE && !IS_SHARED_STORAGE_MAIN_STANDBY_MODE) {
#ifndef ENABLE_MULTIPLE_NODES
            rename_recovery_conf_for_roach();
#endif
            ereport(LOG, (errmsg("request xlog stream from shared storage at %X/%X.", 
                                (uint32)(targetRecPtr >> shiftSize),
                                (uint32)targetRecPtr)));
            RequestXLogStreaming(&targetRecPtr, 0, REPCONNTARGET_SHARED_STORAGE, 0);
        } else {
#ifndef ENABLE_MULTIPLE_NODES
            rename_recovery_conf_for_roach();
#endif
            ereport(LOG, (errmsg("request xlog stream at %X/%X.", (uint32)(targetRecPtr >> shiftSize),
                                 (uint32)targetRecPtr)));
            RequestXLogStreaming(&targetRecPtr, t_thrd.xlog_cxt.PrimaryConnInfo, REPCONNTARGET_PRIMARY,
                                u_sess->attr.attr_storage.PrimarySlotName);
        }
    }
}

void SegWorkerMain()
{
    (void)RegisterRedoInterruptCallBack(HandlePageRedoInterrupts);

    XLogRecParseState *redoblockstateHead = NULL;
    GetRedoStartTime(g_redoWorker->timeCostList[TIME_COST_STEP_1]);
    while ((redoblockstateHead = (XLogRecParseState *)SPSCBlockingQueueTop(g_redoWorker->queue)) !=
           (XLogRecParseState *)&g_redoEndMark) {
        CountAndGetRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_1], g_redoWorker->timeCostList[TIME_COST_STEP_2]);
        if ((void *)redoblockstateHead == (void *)&g_cleanupMark) {
            g_redoWorker->xlogInvalidPages = XLogGetInvalidPages();
            SPSCBlockingQueuePop(g_redoWorker->queue);
            ereport(LOG, (errcode(ERRCODE_LOG), errmsg("[ForceFinish]SegWorkerMain has cleaned InvalidPages")));
            continue;
        } else if ((void *)redoblockstateHead == (void *)&g_closefdMark) {
            smgrcloseall();
            SPSCBlockingQueuePop(g_redoWorker->queue);
            continue;
        } else if ((void *)redoblockstateHead == (void *)&g_cleanInvalidPageMark) {
            forget_range_invalid_pages((void *)redoblockstateHead);
            SPSCBlockingQueuePop(g_redoWorker->queue);
            continue;
        } else if ((void *)redoblockstateHead == (void *)&g_GlobalLsnForwarder) {
            SegWorkerProcLsnForwarder((RedoItem *)redoblockstateHead);
            SPSCBlockingQueuePop(g_redoWorker->queue);
            continue;
        }

        Assert(GetCurrentXLogRecParseType(redoblockstateHead) == PARSE_TYPE_SEG);
        GetRedoStartTime(g_redoWorker->timeCostList[TIME_COST_STEP_3]);
        if (XLByteLT(g_redoWorker->nextPrunePtr, redoblockstateHead->blockparse.blockhead.end_ptr)) {
            OnDemandSegWorkerRedoSegParseState(redoblockstateHead);
        } else {
            ReleaseBlockParseStateIfNotReplay(redoblockstateHead);
        }
        CountAndGetRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_3], g_redoWorker->timeCostList[TIME_COST_STEP_4]);
        SPSCBlockingQueuePop(g_redoWorker->queue);
        SetCompletedReadEndPtr(g_redoWorker, redoblockstateHead->blockparse.blockhead.start_ptr,
            redoblockstateHead->blockparse.blockhead.end_ptr);
        CountRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_4]);

        RedoInterruptCallBack();
        ADD_ABNORMAL_POSITION(11);
        CountAndGetRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_2], g_redoWorker->timeCostList[TIME_COST_STEP_1]);
    }

    SPSCBlockingQueuePop(g_redoWorker->queue);
    RedoThrdWaitForExit(g_redoWorker);
}

static void HashMapManagerProcHashmapPrune(HTAB *redoItemHash, XLogRecPtr prunePtr, bool updateStat)
{
    if (redoItemHash == NULL) {
        return;
    }

    HASH_SEQ_STATUS status;
    RedoItemHashEntry *redoItemEntry = NULL;
    HTAB *curMap = redoItemHash;
    hash_seq_init(&status, curMap);

    while ((redoItemEntry = (RedoItemHashEntry *)hash_seq_search(&status)) != NULL) {
        RedoItemHashPruneWithoutLock(curMap, redoItemEntry, prunePtr, updateStat);
    }
}

static void HashMapManagerProcLsnForwarder(RedoItem *lsnForwarder)
{
    SetCompletedReadEndPtr(g_redoWorker, lsnForwarder->record.ReadRecPtr, lsnForwarder->record.EndRecPtr);
    (void)pg_atomic_sub_fetch_u32(&lsnForwarder->record.refcount, 1);
    uint32 refCount;
    do {
        refCount = pg_atomic_read_u32(&g_GlobalLsnForwarder.record.refcount);
        RedoInterruptCallBack();
    } while (refCount != 0);
}

void HashMapManagerMain()
{
    (void)RegisterRedoInterruptCallBack(HandlePageRedoInterrupts);

    /**
     * Each pipelint has a redoItem HashMap linked list. When the size of tail hashmap is up to limit, pageRedoManager
     * will init a new redoItem hashMap and put it to the tail of linked List, hashmap manager is used to clean hashmap entry.
     */
    do {
        bool pruneMax = false;
        bool updateStat = true;
        if (!SPSCBlockingQueueIsEmpty(g_redoWorker->queue)) {
            GetRedoStartTime(g_redoWorker->timeCostList[TIME_COST_STEP_4]);
            RedoItem *item = (RedoItem *)SPSCBlockingQueueTop(g_redoWorker->queue);
            if (item == &g_redoEndMark) {
                break;
            } else if (item == &g_GlobalLsnForwarder) {
                HashMapManagerProcLsnForwarder(item);
            } else if (item == &g_hashmapPruneMark) {
                pruneMax = true;
            }
            SPSCBlockingQueuePop(g_redoWorker->queue);
            CountRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_4]);
        }

        XLogRecPtr ckptRedoPtr = pg_atomic_read_u64(&g_dispatcher->ckptRedoPtr);
        // step1: prune seg record queue
        GetRedoStartTime(g_redoWorker->timeCostList[TIME_COST_STEP_1]);
        while ((g_redoWorker->slotId == SEG_PROC_PIPELINE_SLOT) && !SPSCBlockingQueueIsEmpty(g_dispatcher->segQueue)) {
            XLogRecParseState *segRecord = (XLogRecParseState *)SPSCBlockingQueueTop(g_dispatcher->segQueue);
            if (XLByteLT(ckptRedoPtr, segRecord->blockparse.blockhead.end_ptr)) {
                break;
            }
#ifdef USE_ASSERT_CHECKING
            DoRecordCheck(segRecord, InvalidXLogRecPtr, false);
#endif
            XLogBlockParseStateRelease(segRecord);
            SPSCBlockingQueuePop(g_dispatcher->segQueue);
        }

        /**
         * step2: prune idle hashmap
         *
         * If one redoItem hashmap's maxRedoItem < checkkpoint redo point, all entrys
         * are no logger useful, so we can free this hashmap.
         */
        CountAndGetRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_1], t_thrd.xlog_cxt.timeCost[TIME_COST_STEP_2]);
        // the head of redoItem hashmap linked list
        ondemand_htab_ctrl_t *nextHtabCtrl = g_instance.comm_cxt.predo_cxt.redoItemHashCtrl[g_redoWorker->slotId];
        // the tail of redoItem hashmap linked list
        ondemand_htab_ctrl_t *targetHtabCtrl = g_dispatcher->pageLines[g_redoWorker->slotId].managerThd->redoItemHashCtrl;
        while (nextHtabCtrl != targetHtabCtrl) {
            ondemand_htab_ctrl_t *procHtabCtrl = nextHtabCtrl;
            nextHtabCtrl = (ondemand_htab_ctrl_t *)nextHtabCtrl->nextHTabCtrl;
            if (XLByteLT(procHtabCtrl->maxRedoItemPtr, ckptRedoPtr)) {
                PRTrackAllClear(procHtabCtrl->hTab);
                pg_atomic_write_u64(&g_redoWorker->nextPrunePtr, procHtabCtrl->maxRedoItemPtr);
                pfree(procHtabCtrl);
                g_instance.comm_cxt.predo_cxt.redoItemHashCtrl[g_redoWorker->slotId] = nextHtabCtrl;
            } else {
                updateStat = false;
                break;
            }
        }

        // step3: prune current hashmap
        CountAndGetRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_2], t_thrd.xlog_cxt.timeCost[TIME_COST_STEP_3]);
        if (pruneMax) {
            HashMapManagerProcHashmapPrune(nextHtabCtrl->hTab, ckptRedoPtr, updateStat);
            pg_atomic_write_u64(&g_redoWorker->nextPrunePtr, ckptRedoPtr);
        }
        CountRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_3]);

        RedoInterruptCallBack();
        ADD_ABNORMAL_POSITION(12);
        pg_usleep(500000L);    /* 500 ms */
    } while (true);

    SPSCBlockingQueuePop(g_redoWorker->queue);
    RedoThrdWaitForExit(g_redoWorker);
 }

void OndemandCtrlWorkerMain()
{
    (void)RegisterRedoInterruptCallBack(HandlePageRedoInterrupts);

    do {
        if (!SPSCBlockingQueueIsEmpty(g_redoWorker->queue)) {
            GetRedoStartTime(g_redoWorker->timeCostList[TIME_COST_STEP_3]);
            RedoItem *item = (RedoItem *)SPSCBlockingQueueTop(g_redoWorker->queue);
            if (item == &g_redoEndMark) {
                break;
            } else if (item == &g_GlobalLsnForwarder) {
                PageWorkerProcLsnForwarder(item);
            } else {
                Assert(0);
            }
            SPSCBlockingQueuePop(g_redoWorker->queue);
            CountRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_3]);
        }

        GetRedoStartTime(g_redoWorker->timeCostList[TIME_COST_STEP_1]);
        OndemandUpdateXLogParseMemUsedBlkNum();

        CountAndGetRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_1], g_redoWorker->timeCostList[TIME_COST_STEP_2]);
        OndemandRequestPrimaryDoCkptIfNeed();
        CountRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_2]);

        RedoInterruptCallBack();
        ADD_ABNORMAL_POSITION(13);
        pg_usleep(500000L);    /* 500 ms */
    } while (true);

    SPSCBlockingQueuePop(g_redoWorker->queue);
    RedoThrdWaitForExit(g_redoWorker);
}

void XLogReadManagerMain()
{
    const long sleepShortTime = 100000L;
    const long sleepLongTime = 1000000L;
    g_recordbuffer = &g_dispatcher->rtoXlogBufState;
    uint32 xlogReadManagerState = READ_MANAGER_RUN;

    (void)RegisterRedoInterruptCallBack(XLogReadManagerProcInterrupt);

    while (xlogReadManagerState == READ_MANAGER_RUN) {
        RedoInterruptCallBack();
        XLogRecPtr replay = InvalidXLogRecPtr;
        bool exitStatus = XLogReadManagerCheckSignal();
        if (exitStatus) {
            break;
        }

        replay = GetXLogReplayRecPtr(NULL, NULL);
        handleRecoverySusPend(replay);

        xlogReadManagerState = pg_atomic_read_u32(&g_dispatcher->rtoXlogBufState.xlogReadManagerState);
        ADD_ABNORMAL_POSITION(7);
        if (t_thrd.xlog_cxt.server_mode == STANDBY_MODE) {
            uint32 readSource = pg_atomic_read_u32(&g_dispatcher->rtoXlogBufState.readSource);
            uint32 failSource = pg_atomic_read_u32(&g_dispatcher->rtoXlogBufState.failSource);
            if (readSource & XLOG_FROM_STREAM) {
                uint32 disableConnectionNode = 
                    pg_atomic_read_u32(&g_instance.comm_cxt.localinfo_cxt.need_disable_connection_node);
                bool retryConnect = ((!disableConnectionNode) || (IS_SHARED_STORAGE_MODE && disableConnectionNode &&
                    !knl_g_get_redo_finish_status() &&
                    !pg_atomic_read_u32(&t_thrd.walreceiverfuncs_cxt.WalRcv->rcvDoneFromShareStorage)));
                if (!WalRcvInProgress() && g_instance.pid_cxt.WalReceiverPID == 0 && retryConnect) {
                    StartRequestXLogFromStream();
                } else {
                    if (disableConnectionNode) {
                        if (IS_SHARED_STORAGE_MODE && WalRcvIsRunning()) {
                            ShutdownWalRcv();
                        }

                        if (!WalRcvInProgress() && !knl_g_get_redo_finish_status()) {
                            pg_atomic_write_u32(&g_dispatcher->rtoXlogBufState.waitRedoDone, 1);
                            WakeupRecovery();
                            pg_usleep(sleepLongTime);
                        } else if (knl_g_get_redo_finish_status()) {
                            pg_atomic_write_u32(&g_instance.comm_cxt.localinfo_cxt.need_disable_connection_node, false);
                            pg_usleep(sleepLongTime);
                        }
                        
                    }
                }
            }

            if (failSource & XLOG_FROM_STREAM) {
                ShutdownWalRcv();
                pg_atomic_write_u32(&(ondemand_extreme_rto::g_dispatcher->rtoXlogBufState.failSource), 0);
            }
        }
        pg_usleep(sleepShortTime);
    }
}

static void ReadWorkerStopCallBack(int code, Datum arg)
{
    pg_atomic_write_u32(&(g_recordbuffer->readWorkerState), WORKER_STATE_EXIT);
    if (t_thrd.xlog_cxt.readFile >= 0) {
        close(t_thrd.xlog_cxt.readFile);
        t_thrd.xlog_cxt.readFile = -1;
    }
}

void XLogReadWorkerMain()
{
    uint32 startreadworker;
    const uint32 sleepTime = 50; /* 50 us */

    on_shmem_exit(ReadWorkerStopCallBack, 0);
    (void)RegisterRedoInterruptCallBack(HandleReadWorkerRunInterrupts);

    g_recordbuffer = &g_dispatcher->rtoXlogBufState;
    startreadworker = pg_atomic_read_u32(&(g_recordbuffer->readWorkerState));
    while (startreadworker != WORKER_STATE_EXITING) {
        if (startreadworker == WORKER_STATE_RUN) {
            XLogReadWorkRun();
        } else {
            pg_usleep(sleepTime);
        }

        RedoInterruptCallBack();
        startreadworker = pg_atomic_read_u32(&(g_recordbuffer->readWorkerState));
        if (startreadworker == WORKER_STATE_STOPPING) {
            pg_atomic_write_u32(&(g_recordbuffer->readWorkerState), WORKER_STATE_STOP);
        }
        ADD_ABNORMAL_POSITION(6);
    };
    /* notify manger to exit */
    pg_atomic_write_u32(&(g_recordbuffer->readWorkerState), WORKER_STATE_EXIT);
}

int RedoMainLoop()
{
    g_redoWorker->oldCtx = MemoryContextSwitchTo(g_instance.comm_cxt.predo_cxt.parallelRedoCtx);

    instr_time startTime;
    instr_time endTime;

    INSTR_TIME_SET_CURRENT(startTime);
    switch (g_redoWorker->role) {
        case REDO_BATCH:
            BatchRedoMain();
            break;
        case REDO_PAGE_MNG:
            RedoPageManagerMain();
            break;
        case REDO_PAGE_WORKER:
            RedoPageWorkerMain();
            break;
        case REDO_TRXN_MNG:
            TrxnManagerMain();
            break;
        case REDO_TRXN_WORKER:
            TrxnWorkMain();
            break;
        case REDO_READ_WORKER:
            XLogReadWorkerMain();
            break;
        case REDO_READ_PAGE_WORKER:
            XLogReadPageWorkerMain();
            break;
        case REDO_READ_MNG:
            XLogReadManagerMain();
            break;
        case REDO_SEG_WORKER:
            SegWorkerMain();
            break;
        case REDO_HTAB_MNG:
            HashMapManagerMain();
            break;
        case REDO_CTRL_WORKER:
            OndemandCtrlWorkerMain();
            break;
        default:
            break;
    }

    INSTR_TIME_SET_CURRENT(endTime);
    INSTR_TIME_SUBTRACT(endTime, startTime);

    /*
     * We need to get the exit code here before we allow the dispatcher
     * to proceed and change the exit code.
     */
    int exitCode = GetDispatcherExitCode();
    g_redoWorker->xlogInvalidPages = XLogGetInvalidPages();
    g_redoWorker->committingCsnList = XLogReleaseAndGetCommittingCsnList();
    const char *role_name = RedoWokerRole2Str(g_redoWorker->role);

    ereport(LOG, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                  errmsg("worker[%d]: rolename = %s, exitcode = %d, total elapsed = %ld", g_redoWorker->id, role_name,
                         exitCode, INSTR_TIME_GET_MICROSEC(endTime))));

    (void)MemoryContextSwitchTo(g_redoWorker->oldCtx);

    return exitCode;
}

void ParallelRedoThreadRegister()
{
    bool isWorkerStarting = false;
    SpinLockAcquire(&(g_instance.comm_cxt.predo_cxt.rwlock));
    isWorkerStarting = ((g_instance.comm_cxt.predo_cxt.state == REDO_STARTING_BEGIN) ? true : false);
    if (isWorkerStarting) {
        pg_atomic_write_u32(&(g_instance.comm_cxt.predo_cxt.pageRedoThreadStatusList[g_redoWorker->id].threadState),
                            PAGE_REDO_WORKER_READY);
    }
    SpinLockRelease(&(g_instance.comm_cxt.predo_cxt.rwlock));
    if (!isWorkerStarting) {
        ereport(LOG, (errmsg("ParallelRedoThreadRegister Page-redo-worker %u exit.", (uint32)isWorkerStarting)));
        SetPageWorkStateByThreadId(PAGE_REDO_WORKER_EXIT);
        proc_exit(0);
    }
}

const char *RedoWokerRole2Str(RedoRole role)
{
    switch (role) {
        case REDO_BATCH:
            return "redo_batch";
            break;
        case REDO_PAGE_MNG:
            return "redo_manager";
            break;
        case REDO_PAGE_WORKER:
            return "redo_woker";
            break;
        case REDO_TRXN_MNG:
            return "trxn_manager";
            break;
        case REDO_TRXN_WORKER:
            return "trxn_worker";
            break;
        case REDO_READ_WORKER:
            return "read_worker";
            break;
        case REDO_READ_PAGE_WORKER:
            return "read_page_woker";
            break;
        case REDO_READ_MNG:
            return "read_manager";
            break;
        case REDO_SEG_WORKER:
            return "seg_worker";
            break;
        case REDO_HTAB_MNG:
            return "htab_manager";
            break;
        case REDO_CTRL_WORKER:
            return "redo_ctrl";
            break;
        default:
            return "unkown";
            break;
    }
}

void WaitStateNormal()
{
    do {
        RedoInterruptCallBack();
    } while (g_instance.comm_cxt.predo_cxt.state < REDO_IN_PROGRESS);
}

/* Run from the worker thread. */
void ParallelRedoThreadMain()
{
    ParallelRedoThreadRegister();
    ereport(LOG, (errmsg("Page-redo-worker thread %u started, role:%u, slotId:%u.", g_redoWorker->id,
                         g_redoWorker->role, g_redoWorker->slotId)));
    // regitster default interrupt call back
    (void)RegisterRedoInterruptCallBack(HandlePageRedoInterrupts);
    SetupSignalHandlers();
    InitGlobals();
    
    ResourceManagerStartup();
    InitRecoveryLockHash();
    WaitStateNormal();
    EnableSyncRequestForwarding();

    int retCode = RedoMainLoop();
    StandbyReleaseAllLocks();
    ResourceManagerStop();
    ereport(LOG, (errmsg("Page-redo-worker thread %u terminated, role:%u, slotId:%u, retcode %u.", g_redoWorker->id,
                         g_redoWorker->role, g_redoWorker->slotId, retCode)));
    LastMarkReached();

    pg_atomic_write_u32(&(g_instance.comm_cxt.predo_cxt.pageRedoThreadStatusList[g_redoWorker->id].threadState),
                        PAGE_REDO_WORKER_EXIT);
    proc_exit(0);
}

static void PageRedoShutdownHandler(SIGNAL_ARGS)
{
    t_thrd.page_redo_cxt.shutdown_requested = 1;
}

static void PageRedoQuickDie(SIGNAL_ARGS)
{
    int status = 2;
    gs_signal_setmask(&t_thrd.libpq_cxt.BlockSig, NULL);
    on_exit_reset();
    exit(status);
}

static void PageRedoUser1Handler(SIGNAL_ARGS)
{
    t_thrd.page_redo_cxt.check_repair = true;
}

static void PageRedoUser2Handler(SIGNAL_ARGS)
{
    t_thrd.page_redo_cxt.sleep_long = 1;
}

/* Run from the worker thread. */
static void SetupSignalHandlers()
{
    (void)gspqsignal(SIGHUP, SigHupHandler);
    (void)gspqsignal(SIGINT, SIG_IGN);
    (void)gspqsignal(SIGTERM, PageRedoShutdownHandler);
    (void)gspqsignal(SIGQUIT, PageRedoQuickDie);
    (void)gspqsignal(SIGPIPE, SIG_IGN);
    (void)gspqsignal(SIGUSR1, PageRedoUser1Handler);
    (void)gspqsignal(SIGUSR2, PageRedoUser2Handler);
    (void)gspqsignal(SIGCHLD, SIG_IGN);
    (void)gspqsignal(SIGTTIN, SIG_IGN);
    (void)gspqsignal(SIGTTOU, SIG_IGN);
    (void)gspqsignal(SIGCONT, SIG_IGN);
    (void)gspqsignal(SIGWINCH, SIG_IGN);
    (void)gspqsignal(SIGURG, print_stack);
    if (g_instance.attr.attr_storage.EnableHotStandby) {
        (void)gspqsignal(SIGALRM, handle_standby_sig_alarm); /* ignored unless InHotStandby */
    } else {
        (void)gspqsignal(SIGALRM, SIG_IGN);
    }

    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();
}

/* Run from the worker thread. */
static void SigHupHandler(SIGNAL_ARGS)
{
    t_thrd.page_redo_cxt.got_SIGHUP = true;
}

/* Run from the worker thread. */
static void InitGlobals()
{
    t_thrd.utils_cxt.CurrentResourceOwner = ResourceOwnerCreate(NULL, "ExtremeRtoParallelRedoThread",
        THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));

    t_thrd.xlog_cxt.server_mode = g_redoWorker->initialServerMode;
    t_thrd.xlog_cxt.ThisTimeLineID = g_redoWorker->initialTimeLineID;
    t_thrd.xlog_cxt.expectedTLIs = g_redoWorker->expectedTLIs;
    /* apply recoveryinfo will change standbystate see UpdateRecordGlobals */
    t_thrd.xlog_cxt.standbyState = g_redoWorker->standbyState;
    t_thrd.xlog_cxt.StandbyMode = g_redoWorker->StandbyMode;
    t_thrd.xlog_cxt.InRecovery = true;
    t_thrd.xlog_cxt.startup_processing = true;
    t_thrd.proc_cxt.DataDir = g_redoWorker->DataDir;
    u_sess->utils_cxt.RecentXmin = g_redoWorker->RecentXmin;
    g_redoWorker->proc = t_thrd.proc;
    t_thrd.storage_cxt.latestObservedXid = g_redoWorker->latestObservedXid;
    t_thrd.xlog_cxt.recoveryTargetTLI = g_redoWorker->recoveryTargetTLI;
    t_thrd.xlog_cxt.recoveryRestoreCommand= g_redoWorker->recoveryRestoreCommand;
    t_thrd.xlog_cxt.ArchiveRecoveryRequested = g_redoWorker->ArchiveRecoveryRequested;
    t_thrd.xlog_cxt.StandbyModeRequested = g_redoWorker->StandbyModeRequested;
    t_thrd.xlog_cxt.InArchiveRecovery = g_redoWorker->InArchiveRecovery;
    t_thrd.xlog_cxt.InRecovery = g_redoWorker->InRecovery;
    t_thrd.xlog_cxt.ArchiveRestoreRequested = g_redoWorker->ArchiveRestoreRequested;
    t_thrd.xlog_cxt.minRecoveryPoint = g_redoWorker->minRecoveryPoint;
    t_thrd.xlog_cxt.curFileTLI = t_thrd.xlog_cxt.ThisTimeLineID;
}

void WaitRedoWorkersQueueEmpty()
{
    bool queueIsEmpty = false;
    while (!queueIsEmpty) {
        queueIsEmpty = true;
        for (uint32 i = 0; i < g_dispatcher->allWorkersCnt; i++) {
            PageRedoWorker *worker = g_dispatcher->allWorkers[i];
            if (worker->role == REDO_TRXN_WORKER || worker->role == REDO_PAGE_WORKER) {
                if (!RedoWorkerIsIdle(worker)) {
                    queueIsEmpty = false;
                    break;
                }
            }
        }
        RedoInterruptCallBack();
    }
}

void RedoThrdWaitForExit(const PageRedoWorker *wk)
{
    uint32 sd = wk->slotId;
    switch (wk->role) {
        case REDO_BATCH:
            SendPageRedoEndMark(g_dispatcher->pageLines[sd].managerThd);
            WaitPageRedoWorkerReachLastMark(g_dispatcher->pageLines[sd].managerThd);
            break;
        case REDO_PAGE_MNG:
            PageManagerDispatchEndMarkAndWait();
            break;
        case REDO_PAGE_WORKER:
            break; /* Don't need to wait for anyone */
        case REDO_TRXN_MNG:
            SendPageRedoEndMark(g_dispatcher->trxnLine.redoThd);
            WaitRedoWorkersQueueEmpty();
            WaitPageRedoWorkerReachLastMark(g_dispatcher->trxnLine.redoThd);
            break;
        case REDO_TRXN_WORKER:
            break; /* Don't need to wait for anyone */
        case REDO_SEG_WORKER:
            break; /* Don't need to wait for anyone */
        case REDO_HTAB_MNG:
            break; /* Don't need to wait for anyone */
        case REDO_CTRL_WORKER:
            break; /* Don't need to wait for anyone */
        default:
            break;
    }
}

/* Run from the txn worker thread. */
XLogRecPtr GetCompletedRecPtr(PageRedoWorker *worker)
{
    return pg_atomic_read_u64(&worker->lastReplayedEndRecPtr);
}

/* Run from the worker thread. */
static void ApplySinglePageRecord(RedoItem *item)
{
    XLogReaderState *record = &item->record;

    MemoryContext oldCtx = MemoryContextSwitchTo(g_redoWorker->oldCtx);
    ApplyRedoRecord(record);
    (void)MemoryContextSwitchTo(oldCtx);
}

/* Run from the worker thread. */
static void LastMarkReached()
{
    PosixSemaphorePost(&g_redoWorker->phaseMarker);
}

/* Run from the dispatcher thread. */
void WaitPageRedoWorkerReachLastMark(PageRedoWorker *worker)
{
    PosixSemaphoreWait(&worker->phaseMarker);
}

/* Run from the dispatcher thread. */
void AddPageRedoItem(PageRedoWorker *worker, void *item)
{
    SPSCBlockingQueuePut(worker->queue, item);
}

static void AddTrxnHashmap(void *item)
{
    SPSCBlockingQueuePut(g_dispatcher->trxnQueue, item);
}

static void AddSegHashmap(void *item)
{
    SPSCBlockingQueuePut(g_dispatcher->segQueue, item);
}

/* Run from the dispatcher thread. */
bool SendPageRedoEndMark(PageRedoWorker *worker)
{
    return SPSCBlockingQueuePut(worker->queue, &g_redoEndMark);
}

/* Run from the dispatcher thread. */
bool SendPageRedoWorkerTerminateMark(PageRedoWorker *worker)
{
    return SPSCBlockingQueuePut(worker->queue, &g_terminateMark);
}

/* Run from the txn worker thread. */
void UpdatePageRedoWorkerStandbyState(PageRedoWorker *worker, HotStandbyState newState)
{
    /*
     * Here we only save the new state into the worker struct.
     * The actual update of the worker thread's state occurs inside
     * the apply loop.
     */
    worker->standbyState = newState;
}

/* Run from the dispatcher thread. */
void *GetXLogInvalidPages(PageRedoWorker *worker)
{
    return worker->xlogInvalidPages;
}

bool RedoWorkerIsIdle(PageRedoWorker *worker)
{
    return SPSCBlockingQueueIsEmpty(worker->queue);
}

void DumpPageRedoWorker(PageRedoWorker *worker)
{
    ereport(LOG, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                  errmsg("[REDO_LOG_TRACE]RedoWorker common info: id %u, tid %lu, "
                         "lastCheckedRestartPoint %lu, lastReplayedEndRecPtr %lu standbyState %u",
                         worker->id, worker->tid.thid, worker->lastCheckedRestartPoint, worker->lastReplayedEndRecPtr,
                         (uint32)worker->standbyState)));
    DumpQueue(worker->queue);
}

void DumpExtremeRtoReadBuf()
{
    if (g_dispatcher == NULL) {
        return;
    }

    ereport(LOG,
            (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
             errmsg("DumpExtremeRtoReadBuf: startworker %u, readindex %u, applyindex %u, readSource %u, failSource %u",
                    g_dispatcher->rtoXlogBufState.readWorkerState, g_dispatcher->rtoXlogBufState.readindex,
                    g_dispatcher->rtoXlogBufState.applyindex, g_dispatcher->rtoXlogBufState.readSource,
                    g_dispatcher->rtoXlogBufState.failSource)));

    for (uint32 i = 0; i < MAX_ALLOC_SEGNUM; ++i) {
        ereport(LOG, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                      errmsg("DumpExtremeRtoReadBuf: buf %u, state %u, readlen %u, segno %lu, segoffset %lu", i,
                             g_dispatcher->rtoXlogBufState.xlogsegarray[i].bufState,
                             g_dispatcher->rtoXlogBufState.xlogsegarray[i].readlen,
                             g_dispatcher->rtoXlogBufState.xlogsegarray[i].segno,
                             g_dispatcher->rtoXlogBufState.xlogsegarray[i].segoffset)));
    }
}

bool XactHasSegpageRelFiles(XLogReaderState *record)
{
    int nrels = 0;
    ColFileNode *xnodes = NULL;

    if (XLogRecGetRmid(record) != RM_XACT_ID) {
        return false;
    }

    bool compress = (bool)(XLogRecGetInfo(record) & XLR_REL_COMPRESS);
    XactGetRelFiles(record, &xnodes, &nrels);

    for (int32 idx = 0; idx < nrels; idx++) {
        ColFileNode colFileNode;
        if (compress) {
            ColFileNode *colFileNodeRel = xnodes + idx;
            ColFileNodeFullCopy(&colFileNode, colFileNodeRel);
        } else {
            ColFileNodeRel *colFileNodeRel = ((ColFileNodeRel *)xnodes) + idx;
            ColFileNodeCopy(&colFileNode, colFileNodeRel);
        }
        if (!IsValidColForkNum(colFileNode.forknum) && IsSegmentFileNode(colFileNode.filenode)) {
            return true;
        }
    }

    return false;
}

/* RecordBadBlockAndPushToRemote
 *               If the bad page has been stored, record the xlog. If the bad page
 * has not been stored, need push to page repair thread hash table and record to
 * recovery thread hash table.
 */
void RecordBadBlockAndPushToRemote(XLogBlockDataParse *datadecode, PageErrorType error_type,
    XLogRecPtr old_lsn, XLogPhyBlock pblk)
{
    return;
}

/* ClearPageRepairHashTbl
 *         drop table, or truncate table, need clear the page repair hashTbl, if the
 * repair page Filenode match  need remove.
 */
void ClearRecoveryThreadHashTbl(const RelFileNode &node, ForkNumber forknum, BlockNumber minblkno,
    bool segment_shrink)
{
    return;
}

/* BatchClearPageRepairHashTbl
 *           drop database, or drop segmentspace, need clear the page repair hashTbl,
 * if the repair page key dbNode match and spcNode match, need remove.
 */
void BatchClearRecoveryThreadHashTbl(Oid spcNode, Oid dbNode)
{
    return;
}

static bool OndemandNeedHandleSyncRecord()
{
    return (XLByteLT(pg_atomic_read_u64(&g_dispatcher->ckptRedoPtr), pg_atomic_read_u64(&g_dispatcher->syncRecordPtr)));
}

static XLogRecPtr RequestPrimaryCkptAndUpdateCkptRedoPtr()
{
    XLogRecPtr ckptRedoPtr = SSOndemandRequestPrimaryCkptAndGetRedoLsn();
    UpdateCheckpointRedoPtrForPrune(ckptRedoPtr);
    return ckptRedoPtr;
}

static void OndemandPauseRedoAndRequestPrimaryDoCkpt(OndemandCheckPauseCB activatePauseFunc,
    OndemandCheckPauseCB inactivatePauseFunc, OndemandRefreshPauseStatusCB refreshPauseStatusFunc,
    ondemand_recovery_pause_status_t pauseState)
{
    while (activatePauseFunc() && SS_ONDEMAND_REALTIME_BUILD_NORMAL) {
        g_instance.dms_cxt.SSRecoveryInfo.ondemand_recovery_pause_status = pauseState;
        (void)RequestPrimaryCkptAndUpdateCkptRedoPtr();

        if ((inactivatePauseFunc != NULL) && !inactivatePauseFunc()) {
            break;
        }

        if (refreshPauseStatusFunc != NULL) {
            refreshPauseStatusFunc();
        }

        RedoInterruptCallBack();
        pg_usleep(100000L);	/* 100 ms */
    }
    g_instance.dms_cxt.SSRecoveryInfo.ondemand_recovery_pause_status = NOT_PAUSE;
}

void OndemandRequestPrimaryDoCkptIfNeed()
{
    if (!SS_ONDEMAND_REALTIME_BUILD_NORMAL) {
        return;
    }

    /* check whether parse mem is not enough */
    OndemandPauseRedoAndRequestPrimaryDoCkpt(&OndemandXLogParseMemApproachFull, &OndemandXLogParseMemFull,
        &OndemandUpdateXLogParseMemUsedBlkNum, PAUSE_FOR_PRUNE_HASHMAP);

    /* check whether trxn record queue is full */
    OndemandPauseRedoAndRequestPrimaryDoCkpt(&OndemandTrxnQueueFullInRealtimeBuild, NULL, NULL,
        PAUSE_FOR_PRUNE_TRXN_QUEUE);

    /* check whether seg record queue is full */
    OndemandPauseRedoAndRequestPrimaryDoCkpt(&OndemandSegQueueFullInRealtimeBuild, NULL, NULL,
        PAUSE_FOR_PRUNE_SEG_QUEUE);

    /* check whether redo workers need handle sync record */
    OndemandPauseRedoAndRequestPrimaryDoCkpt(&OndemandNeedHandleSyncRecord, NULL, NULL,
        PAUSE_FOR_SYNC_REDO);
}

bool SSXLogParseRecordNeedReplayInOndemandRealtimeBuild(XLogRecParseState *redoblockstate)
{
    XLogRecPtr ckptRedoPtr = g_redoWorker->nextPrunePtr;
    if (XLByteLT(redoblockstate->blockparse.blockhead.end_ptr, ckptRedoPtr) || SS_ONDEMAND_REALTIME_BUILD_SHUTDOWN) {
        return false;
    }
    return true;
}

void GetOndemandRecoveryStatus(ondemand_recovery_stat *stat)
{
    if (IsExtremeRtoRunning()) {
        XLogRecPtr tmpStart = MAX_XLOG_REC_PTR;
        XLogRecPtr tmpEnd = MAX_XLOG_REC_PTR;
        OndemandUpdateXLogParseMemUsedBlkNum();
        for (uint32 i = 0; i < g_dispatcher->allWorkersCnt; ++i) {
            if (g_dispatcher->allWorkers[i]->role == REDO_READ_PAGE_WORKER) {
                GetCompletedReadEndPtr(g_dispatcher->allWorkers[i], &tmpStart, &tmpEnd);
                break;
            }
        }
        stat->checkpointPtr = pg_atomic_read_u64(&g_dispatcher->ckptRedoPtr);
        stat->replayedPtr = tmpEnd;
        stat->hmpUsedBlkNum = pg_atomic_read_u32(&g_dispatcher->parseManager.memctl.usedblknum);
        stat->hmpTotalBlkNum = g_dispatcher->parseManager.memctl.totalblknum;
        stat->trxnQueueNum = SPSCGetQueueCount(g_dispatcher->trxnQueue);
        stat->segQueueNum = SPSCGetQueueCount(g_dispatcher->segQueue);
    } else {
        stat->checkpointPtr = InvalidXLogRecPtr;
        stat->replayedPtr = InvalidXLogRecPtr;
        stat->hmpUsedBlkNum = 0;
        stat->hmpTotalBlkNum = 0;
        stat->trxnQueueNum = 0;
        stat->segQueueNum = 0;
    }
    stat->inOndemandRecovery = SS_IN_ONDEMAND_RECOVERY;
    stat->ondemandRecoveryStatus = g_instance.dms_cxt.SSRecoveryInfo.cluster_ondemand_status;
    stat->realtimeBuildStatus = g_instance.dms_cxt.SSRecoveryInfo.ondemand_realtime_build_status;
    stat->recoveryPauseStatus = g_instance.dms_cxt.SSRecoveryInfo.ondemand_recovery_pause_status;
}

void RealtimeBuildReleaseRecoveryLatch(int code, Datum arg) {
    if (ENABLE_ONDEMAND_REALTIME_BUILD) {
        volatile Latch* latch = &t_thrd.shemem_ptr_cxt.XLogCtl->recoveryWakeupLatch;
        if (latch->owner_pid == t_thrd.proc_cxt.MyProcPid && latch->is_shared) {
            DisownLatch(latch);
        }
    }
}

}  // namespace ondemand_extreme_rto