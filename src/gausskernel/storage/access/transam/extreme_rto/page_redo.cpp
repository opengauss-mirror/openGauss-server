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
 * src/gausskernel/storage/access/transam/extreme_rto/page_redo.cpp
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
#include "access/multi_redo_api.h"
#include "catalog/storage_xlog.h"
#include "gssignal/gs_signal.h"
#include "libpq/pqsignal.h"
#include "postmaster/postmaster.h"
#include "storage/ipc.h"
#include "storage/freespace.h"
#include "storage/smgr/smgr.h"
#include "storage/smgr/relfilenode_hash.h"
#include "storage/standby.h"
#include "storage/pmsignal.h"
#include "storage/procarray.h"
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
#include "access/extreme_rto/page_redo.h"
#include "access/extreme_rto/dispatcher.h"
#include "access/extreme_rto/standby_read/lsn_info_meta.h"
#include "access/extreme_rto/txn_redo.h"
#include "access/extreme_rto/xlog_read.h"
#include "pgstat.h"
#include "access/extreme_rto/batch_redo.h"
#include "access/multi_redo_api.h"
#include "replication/walreceiver.h"
#include "replication/datareceiver.h"
#include "replication/ss_disaster_cluster.h"
#include "pgxc/barrier.h"
#include "storage/file/fio_device.h"
#include "utils/timestamp.h"
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

namespace extreme_rto {
static const int MAX_PARSE_BUFF_NUM = PAGE_WORK_QUEUE_SIZE * 10 * 3;
static const int MAX_LOCAL_BUFF_NUM = PAGE_WORK_QUEUE_SIZE * 10 * 3;
static const int MAX_CLEAR_SMGR_NUM = 50000;

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

static const int PAGE_REDO_WORKER_ARG = 3;
static const int REDO_SLEEP_50US = 50;
static const int REDO_SLEEP_100US = 100;
static const int EXRTO_STANDBY_READ_TIME_INTERVAL = 1 * 1000;

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

RefOperate recordRefOperate = {
    AddRefRecord,
    SubRefRecord,
#ifdef USE_ASSERT_CHECKING
    RecordBlockCheck,
#endif
    AddRecordReadBlocks,
};

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
    if (g_redoWorker == NULL) {
        return false;
    }
    return g_redoWorker->isUndoSpaceWorker;
}

/* Run from the dispatcher thread. */
PageRedoWorker *CreateWorker(uint32 id)
{
    PageRedoWorker *tmp = (PageRedoWorker *)palloc0(sizeof(PageRedoWorker) + EXTREME_RTO_ALIGN_LEN);
    PageRedoWorker *worker;
    worker = (PageRedoWorker *)TYPEALIGN(EXTREME_RTO_ALIGN_LEN, tmp);
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
    PosixSemaphoreInit(&worker->phaseMarker, 0);
    worker->oldCtx = NULL;
    worker->fullSyncFlag = 0;
#if (!defined __x86_64__) && (!defined __aarch64__)
    SpinLockInit(&worker->ptrLck);
#endif
    worker->parseManager.memctl.isInit = false;
    worker->parseManager.parsebuffers = NULL;
    worker->remoteReadPageNum = 0;
    worker->badPageHashTbl = BadBlockHashTblCreate();
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
#if defined(__x86_64__) || defined(__aarch64__) && !defined(__USE_SPINLOCK)
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
#if defined(__x86_64__) || defined(__aarch64__) && !defined(__USE_SPINLOCK)
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

void redo_worker_release_all_locks()
{
    Assert(t_thrd.proc != NULL);
 
    /* If waiting, get off wait queue (should only be needed after error) */
    LockErrorCleanup();
 
    /* Release standard locks, including session-level if aborting */
    LockReleaseAll(DEFAULT_LOCKMETHOD, true);
 
    /*
     * User locks are not released by transaction end, so be sure to release
     * them explicitly.
     */
    LockReleaseAll(USER_LOCKMETHOD, true);
}

/* Run from any worker thread. */
PGPROC *GetPageRedoWorkerProc(PageRedoWorker *worker)
{
    return worker->proc;
}

void HandlePageRedoPageRepair(RepairBlockKey key, XLogPhyBlock pblk)
{
    RecordBadBlockAndPushToRemote(g_redoWorker->curRedoBlockState, CRC_CHECK_FAIL, InvalidXLogRecPtr, pblk);
}

void HandlePageRedoInterruptsImpl(uint64 clearRedoFdCountInc = 1)
{
    if (t_thrd.page_redo_cxt.got_SIGHUP) {
        t_thrd.page_redo_cxt.got_SIGHUP = false;
        ProcessConfigFile(PGC_SIGHUP);
    }

    if (t_thrd.page_redo_cxt.check_repair && g_instance.pid_cxt.PageRepairPID != 0) {
        SeqCheckRemoteReadAndRepairPage();
        t_thrd.page_redo_cxt.check_repair = false;
    }

    if (t_thrd.page_redo_cxt.shutdown_requested) {
        ereport(LOG, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                      errmsg("page worker id %u exit for request", g_redoWorker->id)));

        pg_atomic_write_u32(&(g_instance.comm_cxt.predo_cxt.pageRedoThreadStatusList[g_redoWorker->id].threadState),
                            PAGE_REDO_WORKER_EXIT);

        proc_exit(1);
    }
}

void HandlePageRedoInterrupts()
{
    HandlePageRedoInterruptsImpl();
}

void clean_smgr(uint64 &clear_redo_fd_count)
{
    const uint64 clear_redo_fd_count_mask = 0x3FFFFF;
    clear_redo_fd_count += 1;
    if (clear_redo_fd_count > clear_redo_fd_count_mask && GetSMgrRelationHash() != NULL) {
        clear_redo_fd_count = 0;
        long hash_num = hash_get_num_entries(GetSMgrRelationHash());
        if (hash_num >= MAX_CLEAR_SMGR_NUM) {
            ereport(LOG,
                (errmsg("smgr close all: clear_redo_fd_count:%lu, hash_num:%ld,clear_redo_fd_count_mask :%lu",
                    clear_redo_fd_count,
                    hash_num,
                    clear_redo_fd_count_mask)));
            smgrcloseall();
        }
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
        } else if (eleArry[i] == (void *)&g_cleanInvalidPageMark) {
            forget_range_invalid_pages((void *)eleArry[i]);
        } else {
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

void BatchRedoMain()
{
    void **eleArry;
    uint32 eleNum;

    (void)RegisterRedoInterruptCallBack(HandlePageRedoInterrupts);
    XLogParseBufferInitFunc(&(g_redoWorker->parseManager), MAX_PARSE_BUFF_NUM, &recordRefOperate,
                            RedoInterruptCallBack);
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
    XLogParseBufferDestoryFunc(&(g_redoWorker->parseManager));
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

void RedoPageManagerDistributeBlockRecord(XLogRecParseState *record_block_state)
{
    PageRedoPipeline *myRedoLine = &g_dispatcher->pageLines[g_redoWorker->slotId];
    const uint32 WorkerNumPerMng = myRedoLine->redoThdNum;
    uint32 work_id;
    RelFileNode rel_node;
    ForkNumber fork_num;
    BlockNumber blk_no;
    RedoItemTag redo_item_tag;

    PRXLogRecGetBlockTag(record_block_state, &rel_node, &blk_no, &fork_num);
    INIT_REDO_ITEM_TAG(redo_item_tag, rel_node, fork_num, blk_no);
    work_id = GetWorkerId(&redo_item_tag, WorkerNumPerMng);
    record_block_state->nextrecord = NULL;
    AddPageRedoItem(myRedoLine->redoThd[work_id], record_block_state);
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

void DispatchEndMarkToRedoWorkerAndWait()
{
    PageRedoPipeline *myRedoLine = &g_dispatcher->pageLines[g_redoWorker->slotId];
    const uint32 WorkerNumPerMng = get_page_redo_worker_num_per_manager();
    for (uint32 i = 0; i < WorkerNumPerMng; ++i)
        SendPageRedoEndMark(myRedoLine->redoThd[i]);

    for (uint32 i = 0; i < myRedoLine->redoThdNum; i++) {
        WaitPageRedoWorkerReachLastMark(myRedoLine->redoThd[i]);
    }
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

void RedoPageManagerDoDatabaseAction(XLogRecParseState *parsestate)
{
    RedoPageManagerDistributeToAllOneBlock(parsestate);
    WaitCurrentPipeLineRedoWorkersQueueEmpty();
    RedoPageManagerSmgrClose(parsestate);
 
    bool need_wait = parsestate->isFullSync;
    if (need_wait) {
        pg_atomic_write_u32(&g_redoWorker->fullSyncFlag, 1);
    }
    parsestate->nextrecord = NULL;
    XLogBlockParseStateRelease(parsestate);
 
    uint32 val = pg_atomic_read_u32(&g_redoWorker->fullSyncFlag);
    while (val != 0) {
        RedoInterruptCallBack();
        val = pg_atomic_read_u32(&g_redoWorker->fullSyncFlag);
    }
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
    bool need_wait = recordblockstate->isFullSync;
    if (need_wait) {
        pg_atomic_write_u32(&g_redoWorker->fullSyncFlag, 1);
    }
    XLogBlockParseStateRelease(recordblockstate);
    uint32 val = pg_atomic_read_u32(&g_redoWorker->fullSyncFlag);
    while (val != 0) {
        RedoInterruptCallBack();
        val = pg_atomic_read_u32(&g_redoWorker->fullSyncFlag);
    }
}

void RedoPageManagerDoDataTypeAction(XLogRecParseState *parsestate)
{
    XLogBlockDdlParse *ddlrecparse = NULL;
    XLogBlockParseGetDdlParse(parsestate, ddlrecparse);
    
    if (ddlrecparse->blockddltype == BLOCK_DDL_DROP_RELNODE ||
        ddlrecparse->blockddltype == BLOCK_DDL_TRUNCATE_RELNODE) {
        RedoPageManagerDistributeToAllOneBlock(parsestate);
        WaitCurrentPipeLineRedoWorkersQueueEmpty();
    }

    RedoPageManagerDoSmgrAction(parsestate);

}

void PageManagerProcLsnForwarder(RedoItem *lsnForwarder)
{
    SetCompletedReadEndPtr(g_redoWorker, lsnForwarder->record.ReadRecPtr, lsnForwarder->record.EndRecPtr);
    (void)pg_atomic_sub_fetch_u32(&lsnForwarder->record.refcount, 1);

    PageRedoPipeline *myRedoLine = &g_dispatcher->pageLines[g_redoWorker->slotId];
    const uint32 WorkerNumPerMng = myRedoLine->redoThdNum;

    for (uint32 i = 0; i < WorkerNumPerMng; ++i) {
        AddPageRedoItem(myRedoLine->redoThd[i], lsnForwarder);
    }
}

void PageManagerDistributeBcmBlock(XLogRecParseState *preState)
{
    PageRedoPipeline *myRedoLine = &g_dispatcher->pageLines[g_redoWorker->slotId];
    const uint32 WorkerNumPerMng = myRedoLine->redoThdNum;
    uint32 workId = GetWorkerId((uint32)preState->blockparse.blockhead.forknum, WorkerNumPerMng);
    AddPageRedoItem(myRedoLine->redoThd[workId], preState);
}

void PageManagerProcCleanupMark(RedoItem *cleanupMark)
{
    PageRedoPipeline *myRedoLine = &g_dispatcher->pageLines[g_redoWorker->slotId];
    const uint32 WorkerNumPerMng = myRedoLine->redoThdNum;
    g_redoWorker->xlogInvalidPages = XLogGetInvalidPages();
    for (uint32 i = 0; i < WorkerNumPerMng; ++i) {
        AddPageRedoItem(myRedoLine->redoThd[i], cleanupMark);
    }
    ereport(LOG, (errcode(ERRCODE_LOG), errmsg("[ForceFinish]PageManagerProcCleanupMark has cleaned InvalidPages")));
}

void PageManagerProcCheckPoint(XLogRecParseState *parseState)
{
    Assert(IsCheckPoint(parseState));
    RedoPageManagerDistributeToAllOneBlock(parseState);
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

void page_manager_proc_common_type(XLogRecParseState *parse_state)
{
    if (IsCheckPoint(parse_state)) {
        PageManagerProcCheckPoint(parse_state);
    } else if (is_backup_end(parse_state)) {
        RedoPageManagerDistributeToAllOneBlock(parse_state);
        XLogBlockParseStateRelease(parse_state);
    } else {
        Assert(0);
        XLogBlockParseStateRelease(parse_state);
    }
}

void PageManagerProcCreateTableSpace(XLogRecParseState *parseState)
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

void PageManagerProcSegFullSyncState(XLogRecParseState *parseState)
{
    WaitCurrentPipeLineRedoWorkersQueueEmpty();
    RedoPageManagerSyncDdlAction(parseState);
}

void PageManagerProcSegPipeLineSyncState(XLogRecParseState *parseState)
{
    if (SS_DISASTER_STANDBY_CLUSTER) {
        PageRedoPipeline *myRedoLine = &g_dispatcher->pageLines[g_redoWorker->slotId];
        const uint32 WorkerNumPerMng = myRedoLine->redoThdNum;
        uint32 work_id = WorkerNumPerMng - 1;
        parseState->nextrecord = NULL;
        AddPageRedoItem(myRedoLine->redoThd[work_id], parseState);
    } else {
        WaitCurrentPipeLineRedoWorkersQueueEmpty();
        MemoryContext oldCtx = MemoryContextSwitchTo(g_redoWorker->oldCtx);

        RedoPageManagerDdlAction(parseState);

        (void)MemoryContextSwitchTo(oldCtx);
        XLogBlockParseStateRelease(parseState);
    }
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

void redo_page_manager_do_cleanup_action(XLogRecParseState *parse_state)
{
    if (!IS_EXRTO_READ_OPT || !pm_state_is_hot_standby()) {
        return;
    }
 
    RelFileNode tmp_node;
    tmp_node.spcNode = parse_state->blockparse.blockhead.spcNode;
    tmp_node.dbNode = parse_state->blockparse.blockhead.dbNode;
    tmp_node.relNode = parse_state->blockparse.blockhead.relNode;
    tmp_node.bucketNode = parse_state->blockparse.blockhead.bucketNode;
    tmp_node.opt = parse_state->blockparse.blockhead.opt;
    XLogRecPtr lsn = parse_state->blockparse.blockhead.end_ptr;
    TransactionId removed_xid = parse_state->blockparse.extra_rec.clean_up_info.removed_xid;
 
    LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);
    UpdateCleanUpInfo(removed_xid, lsn);
    LWLockRelease(ProcArrayLock);
    ResolveRecoveryConflictWithSnapshot(removed_xid, tmp_node, lsn);
}

void PageManagerRedoParseState(XLogRecParseState *preState)
{
    switch (preState->blockparse.blockhead.block_valid) {
        case BLOCK_DATA_MAIN_DATA_TYPE:
        case BLOCK_DATA_UNDO_TYPE:
        case BLOCK_DATA_VM_TYPE:
        case BLOCK_DATA_FSM_TYPE:
            GetRedoStartTime(g_redoWorker->timeCostList[TIME_COST_STEP_3]);
            RedoPageManagerDistributeBlockRecord(preState);
            CountRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_3]);
            break;
        case BLOCK_DATA_DDL_TYPE:
            GetRedoStartTime(g_redoWorker->timeCostList[TIME_COST_STEP_4]);
            RedoPageManagerDoDataTypeAction(preState);
            CountRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_4]);
            break;
        case BLOCK_DATA_SEG_EXTEND:
            GetRedoStartTime(g_redoWorker->timeCostList[TIME_COST_STEP_4]);
            PageManagerProcSegPipeLineSyncState(preState);
            CountRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_4]);
            break;
        case BLOCK_DATA_CREATE_DATABASE_TYPE:
        case BLOCK_DATA_DROP_DATABASE_TYPE:
            GetRedoStartTime(g_redoWorker->timeCostList[TIME_COST_STEP_5]);
            RedoPageManagerDoDatabaseAction(preState);
            CountRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_5]);
            break;
        case BLOCK_DATA_DROP_TBLSPC_TYPE:
            /* just make sure any other ddl before drop tblspc is done */
            XLogBlockParseStateRelease(preState);
            break;
        case BLOCK_DATA_SEG_FILE_EXTEND_TYPE:
            GetRedoStartTime(g_redoWorker->timeCostList[TIME_COST_STEP_6]);
            RedoPageManagerDistributeBlockRecord(NULL);
            /* wait until queue empty */
            WaitCurrentPipeLineRedoWorkersQueueEmpty();
            /* do atcual action */
            RedoPageManagerSyncDdlAction(preState);
            CountRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_6]);
            break;
        case BLOCK_DATA_SEG_FULL_SYNC_TYPE:
            GetRedoStartTime(g_redoWorker->timeCostList[TIME_COST_STEP_8]);
            PageManagerProcSegFullSyncState(preState);
            CountRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_8]);
            break;
        case BLOCK_DATA_CREATE_TBLSPC_TYPE:
            GetRedoStartTime(g_redoWorker->timeCostList[TIME_COST_STEP_7]);
            PageManagerProcCreateTableSpace(preState);
            CountRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_7]);
            break;
        case BLOCK_DATA_XLOG_COMMON_TYPE:
            page_manager_proc_common_type(preState);
            break;
        case BLOCK_DATA_NEWCU_TYPE:
            PageManagerDistributeBcmBlock(preState);
            break;
        case BLOCK_DATA_SEG_SPACE_DROP:
        case BLOCK_DATA_SEG_SPACE_SHRINK:
            GetRedoStartTime(g_redoWorker->timeCostList[TIME_COST_STEP_8]);
            RedoPageManagerDistributeToAllOneBlock(preState);
            WaitCurrentPipeLineRedoWorkersQueueEmpty();
            RedoPageManagerSyncDdlAction(preState);
            CountRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_8]);
            break;
        case BLOCK_DATA_BARRIER_TYPE:
            RedoPageManagerDistributeToAllOneBlock(preState);
            WaitNextBarrier(preState);
            break;
        case BLOCK_DATA_XACTDATA_TYPE:
            RedoPageManagerDistributeToAllOneBlock(preState);
            XLogBlockParseStateRelease(preState);
            break;
        case BLOCK_DATA_CLEANUP_TYPE:
            redo_page_manager_do_cleanup_action(preState);
            XLogBlockParseStateRelease(preState);
            break;
        default:
            XLogBlockParseStateRelease(preState);
            break;
    }
}

bool PageManagerRedoDistributeItems(XLogRecParseState *record_block_state)
{
    if (record_block_state == (void *)&g_redoEndMark) {
        return true;
    } else if (record_block_state == (void *)&g_GlobalLsnForwarder) {
        PageManagerProcLsnForwarder((RedoItem *)record_block_state);
        return false;
    } else if (record_block_state == (void *)&g_cleanupMark) {
        PageManagerProcCleanupMark((RedoItem *)record_block_state);
        return false;
    } else if (record_block_state == (void *)&g_cleanInvalidPageMark) {
        forget_range_invalid_pages((void *)record_block_state);
        return false;
    }

    XLogRecParseState *next_state = record_block_state;
    do {
        XLogRecParseState *pre_state = next_state;
        next_state = (XLogRecParseState *)next_state->nextrecord;
        pre_state->nextrecord = NULL;
#ifdef ENABLE_UT
        TestXLogRecParseStateEventProbe(UTEST_EVENT_RTO_PAGEMGR_REDO_BEFORE_DISTRIBUTE_ITEMS,
            __FUNCTION__, pre_state);
#endif

        PageManagerRedoParseState(pre_state);
#ifdef ENABLE_UT
        TestXLogRecParseStateEventProbe(UTEST_EVENT_RTO_PAGEMGR_REDO_AFTER_DISTRIBUTE_ITEMS,
            __FUNCTION__, pre_state);
#endif
    } while (next_state != NULL);

    return false;
}

void RedoPageManagerMain()
{
    XLogRecParseState *record_block_state = NULL;
    uint64 clear_redo_fd_count = 0;

    (void)RegisterRedoInterruptCallBack(HandlePageRedoInterrupts);
    XLogParseBufferInitFunc(&(g_redoWorker->parseManager), MAX_PARSE_BUFF_NUM, &recordRefOperate,
                            RedoInterruptCallBack);

    GetRedoStartTime(g_redoWorker->timeCostList[TIME_COST_STEP_1]);
    while ((record_block_state = (XLogRecParseState *)SPSCBlockingQueueTop(g_redoWorker->queue)) !=
           (XLogRecParseState *)&g_redoEndMark) {
        ErrorContextCallback err_context;
        err_context.callback = rm_redo_error_callback;
        err_context.arg = (void *)record_block_state->refrecord;
        err_context.previous = t_thrd.log_cxt.error_context_stack;
        t_thrd.log_cxt.error_context_stack = &err_context;

        CountAndGetRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_1],
                            g_redoWorker->timeCostList[TIME_COST_STEP_2]);
        PageManagerRedoDistributeItems(record_block_state);
        t_thrd.log_cxt.error_context_stack = err_context.previous;
        SPSCBlockingQueuePop(g_redoWorker->queue);
        CountRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_2]);
        RedoInterruptCallBack();
        clean_smgr(clear_redo_fd_count);
        ADD_ABNORMAL_POSITION(5);
        GetRedoStartTime(g_redoWorker->timeCostList[TIME_COST_STEP_1]);
    }

    SPSCBlockingQueuePop(g_redoWorker->queue);
    RedoThrdWaitForExit(g_redoWorker);
    XLogParseBufferDestoryFunc(&(g_redoWorker->parseManager));
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

bool TrxnManagerDistributeItemsBeforeEnd(RedoItem *item)
{
    bool exitFlag = false;
    if (item == &g_redoEndMark) {
        exitFlag = true;
    } else if (item == (RedoItem *)&g_GlobalLsnForwarder) {
        TrxnManagerProcLsnForwarder(item);
    } else if (item == (RedoItem *)&g_cleanupMark) {
        TrxnManagerProcCleanupMark(item);
    } else if (item == (void *)&g_closefdMark) {
        smgrcloseall();
    } else if (item == (void *)&g_cleanInvalidPageMark) {
        forget_range_invalid_pages((void *)item);
    } else {
        GetRedoStartTime(g_redoWorker->timeCostList[TIME_COST_STEP_4]);
        if (IsCheckPoint(&item->record) || IsTableSpaceDrop(&item->record) || IsTableSpaceCreate(&item->record) ||
            (IsXactXlog(&item->record) && xact_has_invalid_msg_or_delete_file(&item->record)) ||
            IsBarrierRelated(&item->record) || IsDataBaseDrop(&item->record) || IsDataBaseCreate(&item->record) ||
            IsSmgrTruncate(&item->record)) {
            uint32 relCount;
            do {
                RedoInterruptCallBack();
                relCount = pg_atomic_read_u32(&item->record.refcount);
            } while (relCount != 1);
        }
        CountAndGetRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_4], g_redoWorker->timeCostList[TIME_COST_STEP_5]);
#ifdef ENABLE_UT
        TestXLogReaderProbe(UTEST_EVENT_RTO_TRXNMGR_DISTRIBUTE_ITEMS,
            __FUNCTION__, &item->record);
#endif
        AddPageRedoItem(g_dispatcher->trxnLine.redoThd, item);
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
    uint32 refcout = pg_atomic_read_u32(&lsnForwarder->record.refcount);
    while (refcout > 1) {
        refcout = pg_atomic_read_u32(&lsnForwarder->record.refcount);
        RedoInterruptCallBack();
    }
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
            exrto_generate_snapshot(g_redoWorker->lastReplayedReadRecPtr);
        } else if (unlikely((void *)item == (void *)&g_cleanupMark)) {
            TrxnWorkrProcCleanupMark((RedoItem *)item);
            SPSCBlockingQueuePop(g_redoWorker->queue);
        } else if (unlikely((void *)item == (void *)&g_cleanInvalidPageMark)) {
            forget_range_invalid_pages((void *)item);
            SPSCBlockingQueuePop(g_redoWorker->queue);
        } else {
            if (IsSmgrTruncate(&item->record)) {
                // need generate a new snapshot before truncate, and lsn is larger than the actual value
                exrto_generate_snapshot(item->record.EndRecPtr);
            }
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

            if (IsCheckPoint(&item->record) || (IsXactXlog(&item->record) &&
                xact_has_invalid_msg_or_delete_file(&item->record)) || IsBarrierRelated(&item->record) ||
                IsDataBaseDrop(&item->record)) {
                exrto_generate_snapshot(g_redoWorker->lastReplayedEndRecPtr);
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

void PageWorkerProcLsnForwarder(RedoItem *lsnForwarder)
{
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

void redo_page_worker_proc_common_record(XLogRecParseState *stat)
{
    if (IsCheckPoint(stat)) {
        RedoPageWorkerCheckPoint(stat);
    }
}

void RedoPageWorkerMain()
{
    (void)RegisterRedoInterruptCallBack(HandlePageRedoInterrupts);

    if (ParseStateWithoutCache()) {
        XLogRedoBufferInitFunc(&(g_redoWorker->bufferManager), MAX_LOCAL_BUFF_NUM, &recordRefOperate,
                               RedoInterruptCallBack);
    }

    uint64 clear_redo_fd_count = 0;
    XLogRecParseState *redoblockstateHead = NULL;
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

        XLogRecParseState *procState = redoblockstateHead;

        MemoryContext oldCtx = MemoryContextSwitchTo(g_redoWorker->oldCtx);
        while (procState != NULL) {
            XLogRecParseState *redoblockstate = procState;
            g_redoWorker->curRedoBlockState = (XLogBlockDataParse*)(&redoblockstate->blockparse.extra_rec);
            procState = (XLogRecParseState *)procState->nextrecord;

            ErrorContextCallback err_context;
            err_context.callback = rm_redo_error_callback;
            err_context.arg = (void *)redoblockstate->refrecord;
            err_context.previous = t_thrd.log_cxt.error_context_stack;
            t_thrd.log_cxt.error_context_stack = &err_context;

            switch (XLogBlockHeadGetValidInfo(&redoblockstate->blockparse.blockhead)) {
                case BLOCK_DATA_MAIN_DATA_TYPE:
                case BLOCK_DATA_UNDO_TYPE:
                case BLOCK_DATA_VM_TYPE:
                case BLOCK_DATA_FSM_TYPE:
                    GetRedoStartTime(g_redoWorker->timeCostList[TIME_COST_STEP_3]);
                    notfound = XLogBlockRedoForExtremeRTO(redoblockstate, &bufferinfo, notfound, 
                        g_redoWorker->timeCostList[TIME_COST_STEP_4], g_redoWorker->timeCostList[TIME_COST_STEP_5]);
                    CountRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_3]);
                    break;
                case BLOCK_DATA_XLOG_COMMON_TYPE:
                    GetRedoStartTime(g_redoWorker->timeCostList[TIME_COST_STEP_6]);
                    redo_page_worker_proc_common_record(redoblockstate);
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
                case BLOCK_DATA_XACTDATA_TYPE:
                    GetRedoStartTime(g_redoWorker->timeCostList[TIME_COST_STEP_6]);
                    SetCompletedReadEndPtr(g_redoWorker, redoblockstate->blockparse.blockhead.start_ptr,
                                           redoblockstate->blockparse.blockhead.end_ptr);
                    CountRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_6]);
                    break;
                case BLOCK_DATA_SEG_EXTEND:
                    ProcSegPageCommonRedo(redoblockstate);
                    break;
                default:
                    break;
            }
            t_thrd.log_cxt.error_context_stack = err_context.previous;
        }
        (void)MemoryContextSwitchTo(oldCtx);
        GetRedoStartTime(g_redoWorker->timeCostList[TIME_COST_STEP_7]);
        updateFsm = XlogNeedUpdateFsm(redoblockstateHead, &bufferinfo);
        bool needWait = redoblockstateHead->isFullSync;
        if (needWait) {
            pg_atomic_write_u32(&g_redoWorker->fullSyncFlag, 1);
        }
        XLogBlockParseStateRelease(redoblockstateHead);
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
        clean_smgr(clear_redo_fd_count);
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

void PushToWorkerLsn(bool force)
{
    const uint32 max_record_count = PAGE_WORK_QUEUE_SIZE;
    static uint32 cur_recor_count = 0;
    if (!IsExtremeRtoRunning()) {
        return;
    }
    cur_recor_count++;
    if (unlikely(force)) {
        uint32 refCount;
        do {
            refCount = pg_atomic_read_u32(&g_GlobalLsnForwarder.record.refcount);
            RedoInterruptCallBack();
        } while (refCount != 0 && !ReadPageWorkerStop());
        cur_recor_count = 0;
        SendLsnFowarder();
    } else {
        if (g_instance.attr.attr_storage.EnableHotStandby && pm_state_is_hot_standby()) {
            if (!exceed_send_lsn_forworder_interval()) {
                return;
            }
        } else {
            if (cur_recor_count < max_record_count) {
                return;
            }
        }

        if (pg_atomic_read_u32(&g_GlobalLsnForwarder.record.refcount) != 0) {
            return;
        }
        SendLsnFowarder();
        cur_recor_count = 0;
    }
}

inline bool send_lsn_forwarder_for_check_to_hot_standby(XLogRecPtr lsn)
{
    if (t_thrd.xlog_cxt.reachedConsistency) {
        // means has send lsn forwarder for consistenstcy check
        return false;
    }
    if (XLogRecPtrIsInvalid(t_thrd.xlog_cxt.minRecoveryPoint)) {
        return false;
    }
 
    if (XLByteLT(lsn, t_thrd.xlog_cxt.minRecoveryPoint)) {
        return false;
    }
 
    t_thrd.xlog_cxt.reachedConsistency = true;
    return true;
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

    pg_atomic_write_u32(&(extreme_rto::g_recordbuffer->readWorkerState), extreme_rto::WORKER_STATE_STOPPING);
    while (pg_atomic_read_u32(&(extreme_rto::g_recordbuffer->readWorkerState)) != WORKER_STATE_STOP) {
        RedoInterruptCallBack();
    };
    ShutdownWalRcv();
    ShutdownDataRcv();
    pg_atomic_write_u32(&(g_recordbuffer->readSource), XLOG_FROM_PG_XLOG);

    PushToWorkerLsn(true);
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
        state = pg_atomic_read_u32(&extreme_rto::g_dispatcher->rtoXlogBufState.readPageWorkerState);
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
    /* init readstate */
    InitXLogRecordReadBuffer(&xlogreader);

    pg_atomic_write_u32(&(g_recordbuffer->readPageWorkerState), WORKER_STATE_RUN);
    if (IsRecoveryDone()) {
        t_thrd.xlog_cxt.readSource = XLOG_FROM_STREAM;
        t_thrd.xlog_cxt.XLogReceiptSource = XLOG_FROM_STREAM;
        pg_atomic_write_u32(&(g_recordbuffer->readSource), XLOG_FROM_STREAM);
    }

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
        PushToWorkerLsn(send_lsn_forwarder_for_check_to_hot_standby(g_redoWorker->lastReplayedEndRecPtr));
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
        PushToWorkerLsn(true);
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
        state = pg_atomic_read_u32(&extreme_rto::g_dispatcher->rtoXlogBufState.readPageWorkerState);
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
                pg_atomic_write_u32(&(extreme_rto::g_dispatcher->rtoXlogBufState.failSource), 0);
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

    if (g_instance.pid_cxt.PageRepairPID != 0) {
        (void)RegisterRedoPageRepairCallBack(HandlePageRedoPageRepair);
    }

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

    ereport(LOG, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                  errmsg("worker[%d]: exitcode = %d, total elapsed = %ld", g_redoWorker->id, exitCode,
                         INSTR_TIME_GET_MICROSEC(endTime))));

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
    t_thrd.page_redo_cxt.redo_worker_ptr = g_redoWorker;
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
    if (g_redoWorker->role == REDO_TRXN_WORKER) {
        redo_worker_release_all_locks();
    }
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
            DispatchEndMarkToRedoWorkerAndWait();
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
        default:
            break;
    }
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


void RepairPageAndRecoveryXLog(BadBlockRecEnt* page_info, const char *page)
{
    RedoBufferInfo buffer;
    RedoBufferTag blockinfo;
    bool updateFsm = false;
    bool notfound = false;
    errno_t rc;
    BufferDesc *bufDesc = NULL;
    RedoTimeCost timeCost1;
    RedoTimeCost timeCost2;

    blockinfo.rnode = page_info->key.relfilenode;
    blockinfo.forknum = page_info->key.forknum;
    blockinfo.blkno = page_info->key.blocknum;
    blockinfo.pblk = page_info->pblk;

    /* read page to buffer pool by RBM_ZERO_AND_LOCK mode and get buffer lock */
    (void)XLogReadBufferForRedoBlockExtend(&blockinfo, RBM_ZERO_AND_LOCK, false, &buffer,
        page_info->rec_max_lsn, InvalidXLogRecPtr, false, WITH_NORMAL_CACHE);

    rc = memcpy_s(buffer.pageinfo.page, BLCKSZ, page, BLCKSZ);
    securec_check(rc, "", "");

    MarkBufferDirty(buffer.buf);
    bufDesc = GetBufferDescriptor(buffer.buf - 1);
    bufDesc->extra->lsn_on_disk = PageGetLSN(buffer.pageinfo.page);
    UnlockReleaseBuffer(buffer.buf);

    /* recovery the page xlog */
    rc = memset_s(&buffer, sizeof(RedoBufferInfo), 0, sizeof(RedoBufferInfo));
    securec_check(rc, "", "");

    XLogRecParseState *procState = page_info->head;
    MemoryContext oldCtx = MemoryContextSwitchTo(g_redoWorker->oldCtx);
    while (procState != NULL) {
        XLogRecParseState *redoblockstate = procState;
        procState = (XLogRecParseState *)procState->nextrecord;
        (void)XLogBlockRedoForExtremeRTO(redoblockstate, &buffer, notfound, timeCost1, timeCost2);
    }
    (void)MemoryContextSwitchTo(oldCtx);
    updateFsm = XlogNeedUpdateFsm(page_info->head, &buffer);
    XLogBlockParseStateRelease(page_info->head);
    ExtremeRtoFlushBuffer(&buffer, updateFsm);
}

HTAB* BadBlockHashTblCreate()
{
    HASHCTL ctl;
    errno_t rc;

    rc = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
    securec_check(rc, "", "");

    ctl.keysize = sizeof(RepairBlockKey);
    ctl.entrysize = sizeof(BadBlockRecEnt);
    ctl.hash = RepairBlockKeyHash;
    ctl.match = RepairBlockKeyMatch;
    return hash_create("recovery thread bad block hashtbl", MAX_REMOTE_READ_INFO_NUM, &ctl,
                       HASH_ELEM | HASH_FUNCTION | HASH_COMPARE);
}


/* ClearPageRepairHashTbl
 *         drop table, or truncate table, need clear the page repair hashTbl, if the
 * repair page Filenode match  need remove.
 */
void ClearRecoveryThreadHashTbl(const RelFileNode &node, ForkNumber forknum, BlockNumber minblkno,
    bool segment_shrink)
{
    HTAB *bad_hash = g_redoWorker->badPageHashTbl;
    bool found = false;
    BadBlockRecEnt *entry = NULL;
    HASH_SEQ_STATUS status;

    hash_seq_init(&status, bad_hash);
    while ((entry = (BadBlockRecEnt *)hash_seq_search(&status)) != NULL) {
        if (BlockNodeMatch(entry->key, entry->pblk, node, forknum, minblkno, segment_shrink)) {
            if (hash_search(bad_hash, &(entry->key), HASH_REMOVE, &found) == NULL) {
                ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                    errmsg("recovery thread bad page hash table corrupted")));
            }
            g_redoWorker->remoteReadPageNum--;
        }
    }

    return;
}

/* BatchClearPageRepairHashTbl
 *           drop database, or drop segmentspace, need clear the page repair hashTbl,
 * if the repair page key dbNode match and spcNode match, need remove.
 */
void BatchClearRecoveryThreadHashTbl(Oid spcNode, Oid dbNode)
{
    HTAB *bad_hash = g_redoWorker->badPageHashTbl;
    bool found = false;
    BadBlockRecEnt *entry = NULL;
    HASH_SEQ_STATUS status;

    hash_seq_init(&status, bad_hash);
    while ((entry = (BadBlockRecEnt *)hash_seq_search(&status)) != NULL) {
        if (dbNodeandSpcNodeMatch(&(entry->key.relfilenode), spcNode, dbNode)) {
            if (hash_search(bad_hash, &(entry->key), HASH_REMOVE, &found) == NULL) {
                ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("page repair hash table corrupted")));
            }
            g_redoWorker->remoteReadPageNum--;
        }
    }

    return;
}


/* ClearSpecificsPageEntryAndMem
 *           If the page has been repair, need remove entry of bad page hashtable,
 * and release the xlog record mem.
 */
void ClearSpecificsPageEntryAndMem(BadBlockRecEnt *entry)
{
    HTAB *bad_hash = g_redoWorker->badPageHashTbl;
    bool found = false;
    uint32 need_repair_num = 0;
    HASH_SEQ_STATUS status;
    BadBlockRecEnt *temp_entry = NULL;

    if ((BadBlockRecEnt*)hash_search(bad_hash, &(entry->key), HASH_REMOVE, &found) == NULL) {
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("recovery thread bad block hash table corrupted")));
    }

    hash_seq_init(&status, bad_hash);
    while ((temp_entry = (BadBlockRecEnt *)hash_seq_search(&status)) != NULL) {
        need_repair_num++;
    }

    if (need_repair_num == 0) {
        XLogParseBufferDestoryFunc(&(g_redoWorker->parseManager));
    }
}

/* RecordBadBlockAndPushToRemote
 *               If the bad page has been stored, record the xlog. If the bad page
 * has not been stored, need push to page repair thread hash table and record to
 * recovery thread hash table.
 */
void RecordBadBlockAndPushToRemote(XLogBlockDataParse *datadecode, PageErrorType error_type,
    XLogRecPtr old_lsn, XLogPhyBlock pblk)
{
    bool found = false;
    RepairBlockKey key;
    gs_thread_t tid;
    XLogBlockParse *block = STRUCT_CONTAINER(XLogBlockParse, extra_rec, datadecode);
    XLogRecParseState *state = STRUCT_CONTAINER(XLogRecParseState, blockparse, block);

    key.relfilenode.spcNode = state->blockparse.blockhead.spcNode;
    key.relfilenode.dbNode = state->blockparse.blockhead.dbNode;
    key.relfilenode.relNode = state->blockparse.blockhead.relNode;
    key.relfilenode.bucketNode = state->blockparse.blockhead.bucketNode;
    key.relfilenode.opt = state->blockparse.blockhead.opt;
    key.forknum = state->blockparse.blockhead.forknum;
    key.blocknum = state->blockparse.blockhead.blkno;

    tid = gs_thread_get_cur_thread();
    found = PushBadPageToRemoteHashTbl(key, error_type, old_lsn, pblk, tid.thid);

    if (found) {
        /* store the record for recovery */
        HTAB *bad_hash = g_redoWorker->badPageHashTbl;
        bool thread_found = false;
        BadBlockRecEnt *remoteReadInfo = (BadBlockRecEnt*)hash_search(bad_hash, &(key), HASH_FIND, &thread_found);
        Assert(thread_found);
        if (!thread_found) {
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("recovery thread bad block hash table corrupted")));
        }
        XLogRecParseState *newState = XLogParseBufferCopy(state);
        newState->nextrecord = NULL;
        remoteReadInfo->tail->nextrecord = newState;
        remoteReadInfo->tail = newState;
        remoteReadInfo->rec_max_lsn = newState->blockparse.blockhead.end_ptr;
    } else {
        HTAB *bad_hash = g_redoWorker->badPageHashTbl;
        bool thread_found = false;
        BadBlockRecEnt *remoteReadInfo = (BadBlockRecEnt*)hash_search(bad_hash, &(key), HASH_ENTER, &thread_found);

        Assert(!thread_found);
        if (g_parseManager == NULL) {
            XLogParseBufferInitFunc(&(g_redoWorker->parseManager),
                MAX_PARSE_BUFF_NUM, &recordRefOperate, RedoInterruptCallBack);
        }
        XLogRecParseState *newState = XLogParseBufferCopy(state);
        newState->nextrecord = NULL;

        remoteReadInfo->key = key;
        remoteReadInfo->pblk = pblk;
        remoteReadInfo->rec_min_lsn = newState->blockparse.blockhead.end_ptr;
        remoteReadInfo->rec_max_lsn = newState->blockparse.blockhead.end_ptr;
        remoteReadInfo->head = newState;
        remoteReadInfo->tail = newState;
        g_redoWorker->remoteReadPageNum++;

        if (g_redoWorker->remoteReadPageNum >= MAX_REMOTE_READ_INFO_NUM) {
            ereport(WARNING, (errmsg("recovery thread found %d error block.", g_redoWorker->remoteReadPageNum)));
        }
    }

    return;
}

void CheckRemoteReadAndRepairPage(BadBlockRecEnt *entry)
{
    XLogRecPtr rec_min_lsn = InvalidXLogRecPtr;
    XLogRecPtr rec_max_lsn = InvalidXLogRecPtr;
    bool check = false;
    RepairBlockKey key;

    key = entry->key;
    rec_min_lsn = entry->rec_min_lsn;
    rec_max_lsn = entry->rec_max_lsn;
    check = CheckRepairPage(key, rec_min_lsn, rec_max_lsn, g_redoWorker->page);
    if (check) {
        /* copy page to buffer pool, and recovery the stored xlog */
        RepairPageAndRecoveryXLog(entry, g_redoWorker->page);
        /* clear page repair thread hash table */
        ClearSpecificsPageRepairHashTbl(key);
        /* clear this thread invalid page hash table */
        forget_specified_invalid_pages(key);
        /* clear thread bad block hash entry */
        ClearSpecificsPageEntryAndMem(entry);
        g_redoWorker->remoteReadPageNum--;
    }
}

void SeqCheckRemoteReadAndRepairPage()
{
    BadBlockRecEnt *entry = NULL;
    HASH_SEQ_STATUS status;

    HTAB *bad_hash = g_redoWorker->badPageHashTbl;

    hash_seq_init(&status, bad_hash);
    while ((entry = (BadBlockRecEnt *)hash_seq_search(&status)) != NULL) {
        CheckRemoteReadAndRepairPage(entry);
    }
}

bool exceed_send_lsn_forworder_interval()
{
    TimestampTz last_time;
    TimestampTz now_time;

    last_time = g_instance.comm_cxt.predo_cxt.exrto_send_lsn_forworder_time;
    now_time = GetCurrentTimestamp();
    if (!TimestampDifferenceExceeds(last_time, now_time, EXRTO_STANDBY_READ_TIME_INTERVAL)) {
        return false;
    }
    g_instance.comm_cxt.predo_cxt.exrto_send_lsn_forworder_time = now_time;
    return true;
}

}  // namespace extreme_rto