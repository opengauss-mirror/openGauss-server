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
#include "gssignal/gs_signal.h"
#include "libpq/pqsignal.h"
#include "postmaster/postmaster.h"
#include "storage/ipc.h"
#include "storage/freespace.h"
#include "storage/smgr.h"
#include "storage/pmsignal.h"

#include "utils/guc.h"
#include "utils/palloc.h"
#include "portability/instr_time.h"

#include "catalog/storage.h"
#include <pthread.h>
#include <sched.h>
#include "commands/dbcommands.h"
#include "commands/tablespace.h"
#include "access/extreme_rto/page_redo.h"
#include "access/extreme_rto/dispatcher.h"
#include "access/extreme_rto/txn_redo.h"
#include "pgstat.h"
#include "access/extreme_rto/batch_redo.h"
#include "access/multi_redo_api.h"
#include "replication/walreceiver.h"
#include "storage/mot/mot_fdw.h"
#include "replication/datareceiver.h"

#ifdef EXTREME_RTO_DEBUG
#include <execinfo.h>
#include <stdio.h>

#include <stdlib.h>

#include <unistd.h>

#endif

namespace extreme_rto {
static const int MAX_PARSE_BUFF_NUM = PAGE_WORK_QUEUE_SIZE * 10 * 3;
static const int MAX_LOCAL_BUFF_NUM = PAGE_WORK_QUEUE_SIZE * 10 * 3;

static const char* const PROCESS_TYPE_CMD_ARG = "--forkpageredo";
static char g_AUXILIARY_TYPE_CMD_ARG[16] = {0};
static const uint32 REDO_WORKER_ALIGN_LEN = 16; /* need 128-bit aligned */

THR_LOCAL PageRedoWorker* g_redoWorker = NULL;
THR_LOCAL RecordBufferState* g_recordbuffer = NULL;
RedoItem g_redoEndMark = {false, false, false, false, 0};
static RedoItem g_terminateMark = {false, false, false, false, 0};
RedoItem g_GlobalLsnForwarder;

static const int PAGE_REDO_WORKER_ARG = 3;
static const int REDO_SLEEP_50US = 50;
static const int REDO_SLEEP_100US = 100;

static void ApplySinglePageRecord(RedoItem*);
static void InitGlobals();
static void LastMarkReached();
static void SetupSignalHandlers();
static void SigHupHandler(SIGNAL_ARGS);
static ThreadId StartWorkerThread(PageRedoWorker*);

void RedoThrdWaitForExit(const PageRedoWorker* wk);
void AddRefRecord(void* rec);
void SubRefRecord(void* rec);
void GlobalLsnUpdate();

RefOperate recordRefOperate = {AddRefRecord, SubRefRecord};

void UpdateRecordGlobals(RedoItem* item, HotStandbyState standbyState)
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
PageRedoWorker* StartPageRedoWorker(PageRedoWorker* worker)
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

/* Run from the dispatcher thread. */
PageRedoWorker *CreateWorker(uint32 id)
{
    PageRedoWorker *tmp = (PageRedoWorker *)palloc0(sizeof(PageRedoWorker) + REDO_WORKER_ALIGN_LEN);
    PageRedoWorker *worker;
    worker = (PageRedoWorker *)TYPEALIGN(REDO_WORKER_ALIGN_LEN, tmp);
    worker->selfOrinAddr = tmp;
    worker->id = id;
    worker->index = 0;
    worker->tid.thid = InvalidTid;
    worker->proc = NULL;
    worker->initialServerMode = (ServerMode)t_thrd.xlog_cxt.server_mode;
    worker->initialTimeLineID = t_thrd.xlog_cxt.ThisTimeLineID;
    worker->expectedTLIs = t_thrd.xlog_cxt.expectedTLIs;
    worker->recoveryTargetTLI = t_thrd.xlog_cxt.recoveryTargetTLI;

    worker->ArchiveRecoveryRequested = t_thrd.xlog_cxt.ArchiveRecoveryRequested;
    worker->StandbyModeRequested = t_thrd.xlog_cxt.StandbyModeRequested;
    worker->InArchiveRecovery = t_thrd.xlog_cxt.InArchiveRecovery;
    worker->InRecovery = t_thrd.xlog_cxt.InRecovery;
    worker->ArchiveRestoreRequested = t_thrd.xlog_cxt.ArchiveRestoreRequested;
    worker->minRecoveryPoint = t_thrd.xlog_cxt.minRecoveryPoint;

    worker->pendingHead = NULL;
    worker->pendingTail = NULL;
    worker->queue = SPSCBlockingQueueCreate(PAGE_WORK_QUEUE_SIZE);
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
    worker->bufferPinWaitBufId = -1;
#if (!defined __x86_64__) && (!defined __aarch64__)
    SpinLockInit(&worker->ptrLck);
#endif
    worker->parseManager.memctl.isInit = false;
    worker->parseManager.parsebuffers = NULL;
    return worker;
}

/* Run from the dispatcher thread. */
static ThreadId StartWorkerThread(PageRedoWorker* worker)
{
    worker->tid.thid = initialize_util_thread(PAGEREDO, worker);
    return worker->tid.thid;
}

/* Run from the dispatcher thread. */
void DestroyPageRedoWorker(PageRedoWorker* worker)
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
    volatile PageRedoWorker* tmpWk = worker;
#if defined(__x86_64__) || defined(__aarch64__)
    uint128_u compare;
    uint128_u exchange;
    uint128_u current;

    compare = atomic_compare_and_swap_u128((uint128_u*)&tmpWk->lastReplayedReadRecPtr);
    Assert(sizeof(tmpWk->lastReplayedReadRecPtr) == 8);
    Assert(sizeof(tmpWk->lastReplayedEndRecPtr) == 8);

    exchange.u64[0] = (uint64)readPtr;
    exchange.u64[1] = (uint64)endPtr;
loop:
    current = atomic_compare_and_swap_u128((uint128_u*)&tmpWk->lastReplayedReadRecPtr, compare, exchange);
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
void GetCompletedReadEndPtr(PageRedoWorker* worker, XLogRecPtr* readPtr, XLogRecPtr* endPtr)
{
    volatile PageRedoWorker* tmpWk = worker;
#if defined(__x86_64__) || defined(__aarch64__)
    uint128_u compare = atomic_compare_and_swap_u128((uint128_u*)&tmpWk->lastReplayedReadRecPtr);
    Assert(sizeof(tmpWk->lastReplayedReadRecPtr) == 8);
    Assert(sizeof(tmpWk->lastReplayedEndRecPtr) == 8);

    *readPtr = (XLogRecPtr)compare.u64[0];
    *endPtr = (XLogRecPtr)compare.u64[1];
#else
    SpinLockAcquire(&tmpWk->ptrLck);
    readPtr = tmpWk->lastReplayedReadRecPtr;
    endPtr = tmpWk->lastReplayedEndRecPtr;
    SpinLockRelease(&tmpWk->ptrLck);
#endif /* __x86_64__ || __aarch64__ */
}

/* Run from both the dispatcher and the worker thread. */
bool IsPageRedoWorkerProcess(int argc, char* argv[])
{
    return strcmp(argv[1], PROCESS_TYPE_CMD_ARG) == 0;
}

/* Run from the worker thread. */
void AdaptArgvForPageRedoWorker(char* argv[])
{
    if (g_AUXILIARY_TYPE_CMD_ARG[0] == 0)
        sprintf_s(g_AUXILIARY_TYPE_CMD_ARG, sizeof(g_AUXILIARY_TYPE_CMD_ARG), "-x%d", PageRedoProcess);
    argv[3] = g_AUXILIARY_TYPE_CMD_ARG;
}

/* Run from the worker thread. */
void GetThreadNameIfPageRedoWorker(int argc, char* argv[], char** threadNamePtr)
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
PGPROC* GetPageRedoWorkerProc(PageRedoWorker* worker)
{
    return worker->proc;
}

void HandlePageRedoInterrupts()
{
    if (t_thrd.page_redo_cxt.got_SIGHUP) {
        t_thrd.page_redo_cxt.got_SIGHUP = false;
        ProcessConfigFile(PGC_SIGHUP);
    }

    if (AmStartupProcess()) {
        /* check for primary */
        uint32 triggeredstate = pg_atomic_read_u32(&(g_startupTriggerState));
        uint32 newtriggered = (uint32)CheckForSatartupStatus();
        if (newtriggered != extreme_rto::TRIGGER_NORMAL && triggeredstate != newtriggered) {
            ereport(LOG, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
              errmsg("HandlePageRedoInterrupts:g_startupTriggerState set from %u to %u", 
              triggeredstate, newtriggered)));
            pg_atomic_write_u32(&(g_startupTriggerState), newtriggered);
        }

        if (t_thrd.startup_cxt.got_SIGHUP) {
            t_thrd.startup_cxt.got_SIGHUP = false;
            ProcessConfigFile(PGC_SIGHUP);
        }

        /*
         * Check if we were requested to exit without finishing recovery.
         */
        if (t_thrd.startup_cxt.shutdown_requested) {
            if (g_instance.status != SmartShutdown) {
                proc_exit(1);
            } else {
                g_dispatcher->smartShutdown = true;
            }
        }
    }

    if (t_thrd.page_redo_cxt.shutdown_requested) {
        ereport(LOG,
            (errmodule(MOD_REDO),
                errcode(ERRCODE_LOG),
                errmsg("page worker id %u exit for request", g_redoWorker->id)));

        pg_atomic_write_u32(&(g_instance.comm_cxt.predo_cxt.pageRedoThreadStatusList[g_redoWorker->id].threadState),
            PAGE_REDO_WORKER_EXIT);

        proc_exit(1);
    }
}

void ReferenceRedoItem(void* item)
{
    RedoItem* redoItem = (RedoItem*)item;
    AddRefRecord(&redoItem->record);
}

void DereferenceRedoItem(void* item)
{
    RedoItem* redoItem = (RedoItem*)item;
    SubRefRecord(&redoItem->record);
}

void DoRelCreate(XLogRecParseState* recordblockstate)
{
    RedoBufferInfo bufferinfo = {0};
    XLogBlockRedoForExtremeRTO(recordblockstate, &bufferinfo, false);

    recordblockstate->nextrecord = NULL;
    XLogBlockParseStateRelease(recordblockstate);
}

#define STRUCT_CONTAINER(type, membername, ptr) ((type*)((char*)(ptr)-offsetof(type, membername)))

void AddRefRecord(void* rec)
{
    (void)pg_atomic_fetch_add_u32(&((XLogReaderState*)rec)->refcount, 1);
#ifdef EXTREME_RTO_DEBUG
    const int stack_size = 5;
    const int max_out_put_buf = 1024;
    void* buffer[stack_size];
    int nptrs;
    char output[max_out_put_buf];
    char** strings;
    nptrs = backtrace(buffer, stack_size);
    strings = backtrace_symbols(buffer, nptrs);

    int ret = snprintf_s(output,
        sizeof(output),
        sizeof(output) - 1,
        "before add relcount %u lsn %X/%X call back trace: ",
        relCount,
        (uint32)(((XLogReaderState*)rec)->EndRecPtr >> 32),
        (uint32)(((XLogReaderState*)rec)->EndRecPtr));
    securec_check_ss_c(ret, "\0", "\0");
    for (int i = 0; i < nptrs; ++i) {
        ret = strcat_s(output, max_out_put_buf - strlen(output), strings[i]);
        securec_check_ss_c(ret, "\0", "\0");
    }
    free(strings);
    ereport(LOG, (errcode(ERRCODE_DATA_CORRUPTED), errmsg(" AddRefRecord print: %s", output)));

#endif
}

void SubRefRecord(void* rec)
{
    Assert(((XLogReaderState*)rec)->refcount != 0);
    uint32 relCount = pg_atomic_sub_fetch_u32(&((XLogReaderState*)rec)->refcount, 1);
#ifdef EXTREME_RTO_DEBUG
    const int stack_size = 5;
    const int max_out_put_buf = 1024;
    void* buffer[stack_size];
    int nptrs;
    char output[max_out_put_buf];
    char** strings;
    nptrs = backtrace(buffer, stack_size);
    strings = backtrace_symbols(buffer, nptrs);

    int ret = snprintf_s(output,
        sizeof(output),
        sizeof(output) - 1,
        "after sub relcount %u lsn %X/%X call back trace:",
        relCount,
        (uint32)(((XLogReaderState*)rec)->EndRecPtr >> 32),
        (uint32)(((XLogReaderState*)rec)->EndRecPtr));
    securec_check_ss_c(ret, "\0", "\0");
    for (int i = 0; i < nptrs; ++i) {
        ret = strcat_s(output, max_out_put_buf - strlen(output), strings[i]);
        securec_check_ss_c(ret, "\0", "\0");
    }
    free(strings);
    ereport(LOG, (errcode(ERRCODE_DATA_CORRUPTED), errmsg(" SubRefRecord print: %s", output)));

#endif

    if (relCount == 0) {
        RedoItem* item = STRUCT_CONTAINER(RedoItem, record, rec);
        FreeRedoItem(item);
    }
}

bool BatchRedoParseItemAndDispatch(RedoItem* item)
{
    uint32 blockNum = 0;
    XLogRecParseState* recordblockstate = XLogParseToBlockForExtermeRTO(&item->record, &blockNum);
    if (recordblockstate == NULL) {
        if (blockNum == 0) {
            return false;
        }
        return true; /*  out of mem */
    }

    PageRedoPipeline* myRedoLine = &g_dispatcher->pageLines[g_redoWorker->slotId];
    AddPageRedoItem(myRedoLine->managerThd, recordblockstate);
    return false;
}

void BatchRedoDistributeEndMark(void)
{
    PageRedoPipeline* myRedoLine = &g_dispatcher->pageLines[g_redoWorker->slotId];
    SendPageRedoEndMark(myRedoLine->managerThd);
}

void BatchRedoProcLsnForwarder(RedoItem* lsnForwarder)
{
    SetCompletedReadEndPtr(g_redoWorker, lsnForwarder->record.ReadRecPtr, lsnForwarder->record.EndRecPtr);
    (void)pg_atomic_sub_fetch_u32(&lsnForwarder->record.refcount, 1);

    PageRedoPipeline* myRedoLine = &g_dispatcher->pageLines[g_redoWorker->slotId];
    AddPageRedoItem(myRedoLine->managerThd, lsnForwarder);
}

bool BatchRedoDistributeItems(void** eleArry, uint32 eleNum)
{
    bool parsecomplete = false;
    for (uint32 i = 0; i < eleNum; i++) {
        if (eleArry[i] == (void*)&g_redoEndMark) {
            return true;
        } else if (eleArry[i] == (void*)&g_GlobalLsnForwarder) {
            BatchRedoProcLsnForwarder((RedoItem*)eleArry[i]);
        } else {
            RedoItem* item = (RedoItem*)eleArry[i];
            UpdateRecordGlobals(item, g_redoWorker->standbyState);

            do {
                parsecomplete = BatchRedoParseItemAndDispatch(item);
            } while (parsecomplete);

            DereferenceRedoItem(item);
        }
    }

    return false;
}

void BatchRedoMain()
{
    void** eleArry;
    uint32 eleNum;
	knl_thread_set_name("RedoBatch");
    XLogParseBufferInitFunc(&(g_redoWorker->parseManager), MAX_PARSE_BUFF_NUM, &recordRefOperate, 
        HandlePageRedoInterrupts);
    while (SPSCBlockingQueueGetAll(g_redoWorker->queue, &eleArry, &eleNum)) {
        bool isEnd = BatchRedoDistributeItems(eleArry, eleNum);
        SPSCBlockingQueuePopN(g_redoWorker->queue, eleNum);
        if (isEnd)
            break;

        HandlePageRedoInterrupts();
        ADD_ABNORMAL_POSITION(1);
    }
    // need notify redo page manager
    BatchRedoDistributeEndMark();
    RedoThrdWaitForExit(g_redoWorker);
    XLogParseBufferDestoryFunc(&(g_redoWorker->parseManager));
}

uint32 GetWorkerId(const RedoItemTag* redoItemTag, uint32 workerCount)
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

void RedoPageManagerDistributeToAllOneBlock(XLogRecParseState* ddlParseState)
{
    PageRedoPipeline* myRedoLine = &g_dispatcher->pageLines[g_redoWorker->slotId];
    const uint32 WorkerNumPerMng = myRedoLine->redoThdNum;

    ddlParseState->nextrecord = NULL;

    for (uint32 i = 0; i < WorkerNumPerMng; ++i) {
        XLogRecParseState* newState = XLogParseBufferCopy(ddlParseState);
        AddPageRedoItem(myRedoLine->redoThd[i], newState);
    }
}

void RedoPageManagerDistributeBlockRecord(HTAB* redoItemHash, XLogRecParseState* parsestate)
{
    PageRedoPipeline* myRedoLine = &g_dispatcher->pageLines[g_redoWorker->slotId];
    const uint32 WorkerNumPerMng = myRedoLine->redoThdNum;
    HASH_SEQ_STATUS status;
    RedoItemHashEntry* redoItemEntry = NULL;
    HTAB* curMap = redoItemHash;
    hash_seq_init(&status, curMap);

    while ((redoItemEntry = (RedoItemHashEntry*)hash_seq_search(&status)) != NULL) {
        uint32 workId = GetWorkerId(&redoItemEntry->redoItemTag, WorkerNumPerMng);
        AddPageRedoItem(myRedoLine->redoThd[workId], redoItemEntry->head);

        if (hash_search(curMap, (void*)&redoItemEntry->redoItemTag, HASH_REMOVE, NULL) == NULL)
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("hash table corrupted")));
    }

    if (parsestate != NULL) {
        RedoPageManagerDistributeToAllOneBlock(parsestate);
    }
}

void RedoPageManagerDistributeToModifyBlock(XLogRecParseState* ddlParseState, HTAB* hashMap)
{
    XLogBlockDdlParse* ddlrecparse = &ddlParseState->blockparse.extra_rec.blockddlrec;
    if (ddlrecparse->blockddltype == BLOCK_DDL_TRUNCATE_RELNODE) {
        /* dispatch to fsmindex and vmindx */
        PRTrackChangeBlock(ddlParseState, hashMap);
    }
}

void WaitAllRedoWorkerQueueEmpty()
{
    PageRedoPipeline* myRedoLine = &g_dispatcher->pageLines[g_redoWorker->slotId];
    const uint32 WorkerNumPerMng = myRedoLine->redoThdNum;

    for (uint32 i = 0; i < WorkerNumPerMng; ++i) {
        while (!SPSCBlockingQueueIsEmpty(myRedoLine->redoThd[i]->queue)) {
            HandlePageRedoInterrupts();
        }
    }
}

void DispatchEndMarkToRedoWorker()
{
    PageRedoPipeline* myRedoLine = &g_dispatcher->pageLines[g_redoWorker->slotId];
    const uint32 WorkerNumPerMng = get_page_redo_worker_num_per_manager();
    for (uint32 i = 0; i < WorkerNumPerMng; ++i)
        SendPageRedoEndMark(myRedoLine->redoThd[i]);
}

void RedoPageManagerDdlAction(XLogRecParseState* parsestate)
{
    switch (parsestate->blockparse.blockhead.block_valid) {
        case BLOCK_DATA_DROP_DATABASE_TYPE:
            xlog_db_drop(parsestate->blockparse.blockhead.dbNode, parsestate->blockparse.blockhead.spcNode);
            break;
        case BLOCK_DATA_CREATE_TBLSPC_TYPE:
            xlog_create_tblspc(parsestate->blockparse.blockhead.spcNode,
                parsestate->blockparse.extra_rec.blocktblspc.tblPath,
                parsestate->blockparse.extra_rec.blocktblspc.isRelativePath);
            break;
        case BLOCK_DATA_CREATE_DATABASE_TYPE:
            xlog_db_create(parsestate->blockparse.blockhead.dbNode, parsestate->blockparse.blockhead.spcNode,
                         parsestate->blockparse.extra_rec.blockdatabase.src_db_id,
                         parsestate->blockparse.extra_rec.blockdatabase.src_tablespace_id);
            break;
        case BLOCK_DATA_DROP_TBLSPC_TYPE:
            xlog_drop_tblspc(parsestate->blockparse.blockhead.spcNode);
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
        default:
            break;
    }
}

void RedoPageManagerSyncDdlAction(XLogRecParseState* parsestate)
{
    /* at this monent, all worker queue is empty ,just find out which one will do it */
    uint32 expected = 0;
    const uint32 pipelineNum = g_dispatcher->pageLineNum;
    pg_atomic_compare_exchange_u32(&g_dispatcher->syncEnterCount, &expected, pipelineNum);
    uint32 entershareCount = pg_atomic_sub_fetch_u32(&g_dispatcher->syncEnterCount, 1);
    if (entershareCount == 0) {
        /* do actual work */
        RedoPageManagerDdlAction(parsestate);
    } else {
        RedoPageManagerSmgrClose(parsestate);
        do {
            entershareCount = pg_atomic_read_u32(&g_dispatcher->syncEnterCount);
        } while (entershareCount != 0);
    }

    expected = 0;
    pg_atomic_compare_exchange_u32(&g_dispatcher->syncExitCount, &expected, pipelineNum);
    uint32 exitShareCount = pg_atomic_sub_fetch_u32(&g_dispatcher->syncExitCount, 1);
    if (exitShareCount == 0) {
        parsestate->nextrecord = NULL;
        XLogBlockParseStateRelease(parsestate);
    } else {
        do {
            exitShareCount = pg_atomic_read_u32(&g_dispatcher->syncExitCount);
        } while (exitShareCount != 0);
    }
}

void RedoPageManagerDoDropAction(XLogRecParseState* parsestate, HTAB* hashMap)
{
    XLogRecParseState* newState = XLogParseBufferCopy(parsestate);
    PRTrackChangeBlock(newState, hashMap);
    RedoPageManagerDistributeBlockRecord(hashMap, parsestate);
    WaitAllRedoWorkerQueueEmpty();
    RedoPageManagerSyncDdlAction(parsestate);
}

void RedoPageManagerDoRelAction(XLogRecParseState* recordblockstate)
{
    RedoBufferInfo bufferinfo = {0};
    void* blockrecbody;
    XLogBlockHead* blockhead;

    blockhead = &recordblockstate->blockparse.blockhead;
    blockrecbody = &recordblockstate->blockparse.extra_rec;

    XLogBlockInitRedoBlockInfo(blockhead, &bufferinfo.blockinfo);
    XLogBlockDdlDoRealAction(blockhead, blockrecbody, &bufferinfo);
    recordblockstate->nextrecord = NULL;
    XLogBlockParseStateRelease(recordblockstate);
}

void RedoPageManagerDoDataTypeAction(XLogRecParseState* parsestate, HTAB* hashMap)
{
    XLogBlockDdlParse* ddlrecparse = &parsestate->blockparse.extra_rec.blockddlrec;

    if (ddlrecparse->blockddltype == BLOCK_DDL_DROP_RELNODE ||
        ddlrecparse->blockddltype == BLOCK_DDL_TRUNCATE_RELNODE) {
        XLogRecParseState* newState = XLogParseBufferCopy(parsestate);
        PRTrackChangeBlock(newState, hashMap);
        RedoPageManagerDistributeBlockRecord(hashMap, parsestate);
        WaitAllRedoWorkerQueueEmpty();
    }

    RedoPageManagerDoRelAction(parsestate);
}

void PageManagerProcLsnForwarder(RedoItem* lsnForwarder)
{
    SetCompletedReadEndPtr(g_redoWorker, lsnForwarder->record.ReadRecPtr, lsnForwarder->record.EndRecPtr);
    (void)pg_atomic_sub_fetch_u32(&lsnForwarder->record.refcount, 1);

    PageRedoPipeline* myRedoLine = &g_dispatcher->pageLines[g_redoWorker->slotId];
    const uint32 WorkerNumPerMng = myRedoLine->redoThdNum;

    for (uint32 i = 0; i < WorkerNumPerMng; ++i) {
        AddPageRedoItem(myRedoLine->redoThd[i], lsnForwarder);
    }
}

void PageManagerDistributeBcmBlock(XLogRecParseState* preState)
{
    PageRedoPipeline* myRedoLine = &g_dispatcher->pageLines[g_redoWorker->slotId];
    const uint32 WorkerNumPerMng = myRedoLine->redoThdNum;
    uint32 workId = GetWorkerId((uint32)preState->blockparse.blockhead.forknum, WorkerNumPerMng);
    AddPageRedoItem(myRedoLine->redoThd[workId], preState);
}

bool PageManagerRedoDistributeItems(void** eleArry, uint32 eleNum)
{
    HTAB *hashMap = g_dispatcher->pageLines[g_redoWorker->slotId].managerThd->redoItemHash;

    for (uint32 i = 0; i < eleNum; i++) {
        if (eleArry[i] == (void*)&g_redoEndMark) {
            RedoPageManagerDistributeBlockRecord(hashMap, NULL);
            return true;
        } else if (eleArry[i] == (void*)&g_GlobalLsnForwarder) {
            RedoPageManagerDistributeBlockRecord(hashMap, NULL);
            PageManagerProcLsnForwarder((RedoItem*)eleArry[i]);
            continue;
        }
        XLogRecParseState *recordblockstate = (XLogRecParseState *)eleArry[i];
        XLogRecParseState* nextState = recordblockstate;
        do {
            XLogRecParseState* preState = nextState;
            nextState = (XLogRecParseState*)nextState->nextrecord;
            preState->nextrecord = NULL;

            switch (preState->blockparse.blockhead.block_valid) {
                case BLOCK_DATA_HEAP_TYPE:
                case BLOCK_DATA_VM_TYPE:
                case BLOCK_DATA_FSM_TYPE:
                    PRTrackChangeBlock(preState, hashMap);
                    break;
                case BLOCK_DATA_DDL_TYPE:
                    RedoPageManagerDoDataTypeAction(preState, hashMap);
                    break;
                case BLOCK_DATA_DROP_DATABASE_TYPE:
                    RedoPageManagerDoDropAction(preState, hashMap);
                    break;
                case BLOCK_DATA_DROP_TBLSPC_TYPE:
                case BLOCK_DATA_CREATE_DATABASE_TYPE:
                case BLOCK_DATA_CREATE_TBLSPC_TYPE:
                    RedoPageManagerDistributeBlockRecord(hashMap, NULL);
                    /* wait until queue empty */
                    WaitAllRedoWorkerQueueEmpty();
                    /* do atcual action */
                    RedoPageManagerSyncDdlAction(preState);
                    break;
                case BLOCK_DATA_XLOG_COMMON_TYPE:
                    RedoPageManagerDistributeBlockRecord(hashMap, preState);
                    XLogBlockParseStateRelease(preState);
                    break;
                case BLOCK_DATA_NEWCU_TYPE:
                    RedoPageManagerDistributeBlockRecord(hashMap, NULL);
                    PageManagerDistributeBcmBlock(preState);
                    break;
                default:
                    XLogBlockParseStateRelease(preState);
                    break;
            }
        } while (nextState != NULL);
    }
    RedoPageManagerDistributeBlockRecord(hashMap, NULL);
    return false;
}

void RedoPageManagerMain()
{
    void **eleArry;
    uint32 eleNum;
	knl_thread_set_name("RedoPageMgr");
    XLogParseBufferInitFunc(&(g_redoWorker->parseManager), MAX_PARSE_BUFF_NUM, &recordRefOperate, 
        HandlePageRedoInterrupts);
    while (SPSCBlockingQueueGetAll(g_redoWorker->queue, &eleArry, &eleNum)) {
        bool isEnd = PageManagerRedoDistributeItems(eleArry, eleNum);
        SPSCBlockingQueuePopN(g_redoWorker->queue, eleNum);
        if (isEnd)
            break;

        HandlePageRedoInterrupts();
        ADD_ABNORMAL_POSITION(5);
    }
    // need notify page redo
    DispatchEndMarkToRedoWorker();
    RedoThrdWaitForExit(g_redoWorker);
    XLogParseBufferDestoryFunc(&(g_redoWorker->parseManager));
}

bool IsXactXlog(const XLogReaderState* record)
{
    if (XLogRecGetRmid(record) != RM_XACT_ID) {
        return false;
    }
    return true;
}

void TrxnManagerProcLsnForwarder(RedoItem* lsnForwarder)
{
    SetCompletedReadEndPtr(g_redoWorker, lsnForwarder->record.ReadRecPtr, lsnForwarder->record.EndRecPtr);
    (void)pg_atomic_sub_fetch_u32(&lsnForwarder->record.refcount, 1);

    AddPageRedoItem(g_dispatcher->trxnLine.redoThd, lsnForwarder);
}

bool TrxnManagerDistributeItemsBeforeEnd(void** eleArry, uint32 eleNum)
{
    bool exitFlag = false;
    for (uint32 i = 0; i < eleNum; i++) {
        if (eleArry[i] == &g_redoEndMark) {
            exitFlag = true;
            // prepare to exit
            SendPageRedoEndMark(g_dispatcher->trxnLine.redoThd);
            break;
        } else if (eleArry[i] == (void*)&g_GlobalLsnForwarder) {
            TrxnManagerProcLsnForwarder((RedoItem*)eleArry[i]);
            continue;
        }

        RedoItem* item = (RedoItem*)eleArry[i];

        if (IsCheckPoint(&item->record) || IsTableSpaceDrop(&item->record) ||
            (IsXactXlog(&item->record) && XactWillRemoveRelFiles(&item->record))) {
            uint32 relCount;
            do {
                relCount = pg_atomic_read_u32(&item->record.refcount);
                HandlePageRedoInterrupts();
            } while (relCount != 1);
        }

        AddPageRedoItem(g_dispatcher->trxnLine.redoThd, item);

        HandlePageRedoInterrupts();
    }
    GlobalLsnUpdate();
    return exitFlag;
}

void GlobalLsnUpdate()
{
    t_thrd.xlog_cxt.standbyState = g_redoWorker->standbyState; 
    if (LsnUpdate()) {
        ExtremRtoUpdateMinCheckpoint();
        CheckRecoveryConsistency();
    }
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

void TrxnManagerMain()
{
    knl_thread_set_name("RedoTxnMgr");
    while (true) {
        HandlePageRedoInterrupts();
        if (!SPSCBlockingQueueIsEmpty(g_redoWorker->queue)) {
            void** eleArry;
            uint32 eleNum;
            SPSCBlockingQueueGetAll(g_redoWorker->queue, &eleArry, &eleNum);
            bool isEnd = TrxnManagerDistributeItemsBeforeEnd(eleArry, eleNum);
            SPSCBlockingQueuePopN(g_redoWorker->queue, eleNum);

            if (isEnd)
                break;
        } else {
            long sleeptime = 150 * 1000;
            pg_usleep(sleeptime);
            GlobalLsnUpdate();
        }
        HandlePageRedoInterrupts();
        ADD_ABNORMAL_POSITION(2);
    }

    RedoThrdWaitForExit(g_redoWorker);
    GlobalLsnUpdate();
}

void TrxnWorkerProcLsnForwarder(RedoItem* lsnForwarder)
{
    SetCompletedReadEndPtr(g_redoWorker, lsnForwarder->record.ReadRecPtr, lsnForwarder->record.EndRecPtr);
    (void)pg_atomic_sub_fetch_u32(&lsnForwarder->record.refcount, 1);
}

void TrxnWorkNotifyRedoWorker()
{
    for (uint32 i = 0; i < g_dispatcher->allWorkersCnt; ++i) {
        if (g_dispatcher->allWorkers[i]->role == REDO_PAGE_WORKER) {
            pg_atomic_write_u32(&(g_dispatcher->allWorkers[i]->fullSyncFlag), 0);
        }
    }
}

void TrxnWorkMain()
{
    knl_thread_set_name("RedoTxnWorker");
    RedoItem *item = nullptr;
    MOTBeginRedoRecovery();
    if (ParseStateWithoutCache()) {
        XLogRedoBufferInitFunc(&(g_redoWorker->bufferManager), MAX_LOCAL_BUFF_NUM, &recordRefOperate, 
            HandlePageRedoInterrupts);
    }
    while ((item = (RedoItem *)SPSCBlockingQueueTop(g_redoWorker->queue)) != &g_redoEndMark) {
        if ((void *)item == (void *)&g_GlobalLsnForwarder) {
            TrxnWorkerProcLsnForwarder((RedoItem *)item);
            SPSCBlockingQueuePop(g_redoWorker->queue);
        } else {
            t_thrd.xlog_cxt.needImmediateCkp = item->needImmediateCheckpoint;
            bool fullSync = item->record.isFullSyncCheckpoint;
            ApplySinglePageRecord(item);
            SPSCBlockingQueuePop(g_redoWorker->queue);
            SetCompletedReadEndPtr(g_redoWorker, item->record.ReadRecPtr, item->record.EndRecPtr);
            DereferenceRedoItem(item);
            if (fullSync) {
                TrxnWorkNotifyRedoWorker();
            }
            HandlePageRedoInterrupts();
        }
        ADD_ABNORMAL_POSITION(3);
    }
    SPSCBlockingQueuePop(g_redoWorker->queue);
    if (ParseStateWithoutCache())
        XLogRedoBufferDestoryFunc(&(g_redoWorker->bufferManager));
    MOTEndRedoRecovery();
}

void RedoPageWorkerCheckPoint(const XLogRecParseState* redoblockstate)
{
    CheckPoint checkPoint;

    XLogSynAllBuffer();
    errno_t rc = memcpy_s(&checkPoint,
        sizeof(checkPoint),
        redoblockstate->blockparse.extra_rec.blockxlogcommon.maindata,
        sizeof(checkPoint));
    securec_check(rc, "\0", "\0");
    if (IsRestartPointSafe(checkPoint.redo)) {
        pg_atomic_write_u64(&g_redoWorker->lastCheckedRestartPoint, redoblockstate->blockparse.blockhead.end_ptr);
    }
    UpdateTimeline(&checkPoint);
}

void PageWorkerProcLsnForwarder(RedoItem* lsnForwarder)
{
    SetCompletedReadEndPtr(g_redoWorker, lsnForwarder->record.ReadRecPtr, lsnForwarder->record.EndRecPtr);
    (void)pg_atomic_sub_fetch_u32(&lsnForwarder->record.refcount, 1);
}

bool XlogNeedUpdateFsm(XLogRecParseState* procState, RedoBufferInfo* bufferinfo)
{
    XLogBlockHead* blockhead = &procState->blockparse.blockhead;
    if (bufferinfo->pageinfo.page == NULL || !(bufferinfo->dirtyflag) || blockhead->forknum != MAIN_FORKNUM ||
        XLogBlockHeadGetValidInfo(blockhead) != BLOCK_DATA_HEAP_TYPE) {
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

void RedoPageWorkerRedoBcmBlock(XLogRecParseState* procState)
{

    RmgrId rmid = XLogBlockHeadGetRmid(&procState->blockparse.blockhead);
    if (rmid == RM_HEAP2_ID) {
        RelFileNode node;
        node.spcNode = procState->blockparse.blockhead.spcNode;
        node.dbNode = procState->blockparse.blockhead.dbNode;
        node.relNode = procState->blockparse.blockhead.relNode;
        node.bucketNode = procState->blockparse.blockhead.bucketNode;
        XLogBlockNewCuParse* newCuParse = &(procState->blockparse.extra_rec.blocknewcu);
        uint8 info = XLogBlockHeadGetInfo(&procState->blockparse.blockhead) & ~XLR_INFO_MASK;
        switch (info & XLOG_HEAP_OPMASK) {
            case XLOG_HEAP2_BCM: {
                xl_heap_bcm* xlrec = (xl_heap_bcm*)(newCuParse->main_data);
                heap_bcm_redo(xlrec, node, procState->blockparse.blockhead.end_ptr);
                break;
            }
            case XLOG_HEAP2_LOGICAL_NEWPAGE: {
                Assert(node.bucketNode == InvalidBktId);
                xl_heap_logical_newpage* xlrec = (xl_heap_logical_newpage*)(newCuParse->main_data);
                char* cuData = newCuParse->main_data + SizeOfHeapLogicalNewPage;
                heap_xlog_bcm_new_page(xlrec, node, cuData);
                break;
            }
            default:
                break;
        }
    }
}

void RedoPageWorkerMain()
{
    knl_thread_set_name("RedoPageWorker");
    XLogRecParseState *redoblockstateHead = nullptr;
    if (ParseStateWithoutCache()) {
        XLogRedoBufferInitFunc(&(g_redoWorker->bufferManager), MAX_LOCAL_BUFF_NUM, &recordRefOperate,
            HandlePageRedoInterrupts);
    }
    while ((redoblockstateHead = (XLogRecParseState *)SPSCBlockingQueueTop(g_redoWorker->queue)) !=
           (XLogRecParseState *)&g_redoEndMark) {

        if ((void *)redoblockstateHead == (void *)&g_GlobalLsnForwarder) {
            PageWorkerProcLsnForwarder((RedoItem *)redoblockstateHead);
            SPSCBlockingQueuePop(g_redoWorker->queue);
            continue;
        }
        RedoBufferInfo bufferinfo = {0};
        bool notfound = false;
        bool updateFsm = false;

        XLogRecParseState *procState = redoblockstateHead;

        while (procState != NULL) {
            XLogRecParseState *redoblockstate = procState;
            procState = (XLogRecParseState *)procState->nextrecord;

            switch (XLogBlockHeadGetValidInfo(&redoblockstate->blockparse.blockhead)) {
                case BLOCK_DATA_HEAP_TYPE:
                case BLOCK_DATA_VM_TYPE:
                case BLOCK_DATA_FSM_TYPE:
                    notfound = XLogBlockRedoForExtremeRTO(redoblockstate, &bufferinfo, notfound);
                    break;
                case BLOCK_DATA_XLOG_COMMON_TYPE:
                    RedoPageWorkerCheckPoint(redoblockstate);
                    SetCompletedReadEndPtr(g_redoWorker, redoblockstate->blockparse.blockhead.start_ptr, 
                        redoblockstate->blockparse.blockhead.end_ptr);
                    break;
                case BLOCK_DATA_DDL_TYPE:
                    XLogForgetDDLRedo(redoblockstate);
                    SetCompletedReadEndPtr(g_redoWorker, redoblockstate->blockparse.blockhead.start_ptr,
                        redoblockstate->blockparse.blockhead.end_ptr);
                    break;
                case BLOCK_DATA_DROP_DATABASE_TYPE:
                    XLogDropDatabase(redoblockstate->blockparse.blockhead.dbNode);
                    SetCompletedReadEndPtr(g_redoWorker, redoblockstate->blockparse.blockhead.start_ptr,
                        redoblockstate->blockparse.blockhead.end_ptr);
                    break;
                case BLOCK_DATA_NEWCU_TYPE:
                    RedoPageWorkerRedoBcmBlock(redoblockstate);
                    break;
                default:
                    break;
            }
        }

        updateFsm = XlogNeedUpdateFsm(redoblockstateHead, &bufferinfo);
        bool needWait = redoblockstateHead->isFullSyncCheckpoint;
        if (needWait) {
            pg_atomic_write_u32(&g_redoWorker->fullSyncFlag, 1);
        }
        XLogBlockParseStateRelease(redoblockstateHead);
        /* the same page */
        ExtremeRtoFlushBuffer(&bufferinfo, updateFsm);
        SPSCBlockingQueuePop(g_redoWorker->queue);

        pg_memory_barrier();
        uint32 val = pg_atomic_read_u32(&g_redoWorker->fullSyncFlag);
        while (val != 0) {
            val = pg_atomic_read_u32(&g_redoWorker->fullSyncFlag);
            HandlePageRedoInterrupts();
        }
        HandlePageRedoInterrupts();
        ADD_ABNORMAL_POSITION(4);
    }
    if (ParseStateWithoutCache())
        XLogRedoBufferDestoryFunc(&(g_redoWorker->bufferManager));
}

void PutRecordToReadQueue(XLogReaderState *recordreader)
{
    bool putresult = false;

    do {
        putresult = SPSCBlockingQueuePut(g_dispatcher->readLine.readPageThd->queue, recordreader);
    } while (!putresult);

    return;
}

inline void InitXLogRecordReadBuffer(XLogReaderState** initreader)
{
    XLogReaderState* newxlogreader;
    XLogReaderState* readstate = g_dispatcher->recordstate.initreader;
    newxlogreader = NewReaderState(readstate);
    g_dispatcher->recordstate.initreader = NULL;
    PutRecordToReadQueue(readstate);
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
    g_GlobalLsnForwarder.record.ReadRecPtr = t_thrd.xlog_cxt.ReadRecPtr;
    g_GlobalLsnForwarder.record.EndRecPtr = t_thrd.xlog_cxt.EndRecPtr;
    g_GlobalLsnForwarder.record.refcount = get_real_recovery_parallelism() - XLOG_READER_NUM;
    g_GlobalLsnForwarder.record.isDecode = true;

    PutRecordToReadQueue(&g_GlobalLsnForwarder.record);
}

void PushToWorkerLsn(bool force)
{
    const uint32 max_record_count = 8192;
    static uint32 cur_recor_count = 0;

    cur_recor_count++;

    if (!IsExtremeRedo() || !IsMultiThreadRedoRunning()) {
        return;
    }

    if (force) {
        uint32 refCount;
        do {
            refCount = pg_atomic_read_u32(&g_GlobalLsnForwarder.record.refcount);
            HandlePageRedoInterrupts();
        } while (refCount != 0);
        cur_recor_count = 0;
        SendLsnFowarder();
    } else {
        uint32 refCount = pg_atomic_read_u32(&g_GlobalLsnForwarder.record.refcount);

        if (refCount != 0 || cur_recor_count < max_record_count) {
            return;
        }

        SendLsnFowarder();
    }
}

bool SetReadBufferForExtRto(XLogReaderState* state, XLogRecPtr pageptr, int reqLen)
{
    XLogSegNo targetSegNo;

    XLByteToSeg(pageptr, targetSegNo);
    uint32 targetPageOff = (pageptr % XLOG_SEG_SIZE);

    uint32 applyindex = g_recordbuffer->applyindex;
    Assert(applyindex < MAX_ALLOC_SEGNUM);
    RecordBufferAarray* cursegbuffer = &g_recordbuffer->xlogsegarray[applyindex];
    if (targetSegNo == cursegbuffer->segno && targetPageOff == cursegbuffer->segoffset) {
        state->readBuf = cursegbuffer->readsegbuf + cursegbuffer->segoffset;
    } else if (targetSegNo == cursegbuffer->segno) {
        cursegbuffer->segoffset = targetPageOff;
        state->readBuf = cursegbuffer->readsegbuf + cursegbuffer->segoffset;
    } else {
        uint32 workerstate = pg_atomic_read_u32(&(g_recordbuffer->readWorkerState));

        if (workerstate == WORKER_STATE_RUN) {
            if (targetSegNo == (cursegbuffer->segno + 1)) {
                Assert(targetPageOff == 0);
                pg_atomic_write_u32(&(cursegbuffer->bufState), APPLIED);
                if ((applyindex + 1) == MAX_ALLOC_SEGNUM) {
                    applyindex = 0;
                } else {
                    applyindex++;
                }
                cursegbuffer = &g_recordbuffer->xlogsegarray[applyindex];
                cursegbuffer->segoffset = targetPageOff;
                state->readBuf = cursegbuffer->readsegbuf + cursegbuffer->segoffset;
                pg_memory_barrier();
                pg_atomic_write_u32(&(g_recordbuffer->applyindex), applyindex);
            } else {
                ereport(WARNING, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                                  errmsg("SetReadBufferForExtRto EndRecPtr:%lu pageptr:%lu, reqLen:%d",
                                         state->EndRecPtr, pageptr, reqLen)));
                DumpExtremeRtoReadBuf();
                t_thrd.xlog_cxt.failedSources |= XLOG_FROM_STREAM;
                return false;
            }
        } else {
            cursegbuffer->segno = targetSegNo;
            cursegbuffer->segoffset = targetPageOff;
            state->readBuf = cursegbuffer->readsegbuf + cursegbuffer->segoffset;
        }
    }

    return true;
}

void UpdateReadBufferForExtRto(XLogReaderState *state)
{
    Assert(g_recordbuffer->applyindex < MAX_ALLOC_SEGNUM);
    RecordBufferAarray *cursegbuffer = &g_recordbuffer->xlogsegarray[g_recordbuffer->applyindex];
    Assert(state->readOff == cursegbuffer->segoffset);
    Assert(state->readSegNo == cursegbuffer->segno);

    if (pg_atomic_read_u32(&(g_recordbuffer->readWorkerState)) != WORKER_STATE_RUN) {
        cursegbuffer->readlen = state->readOff + state->readLen;
        g_recordbuffer->readindex = g_recordbuffer->applyindex;
    }

    return;
}

static const int MAX_WAIT_TIMS = 512;

bool XLogPageReadForExtRto(XLogRecPtr targetPagePtr, int reqLen, char *readBuf)
{
    uint32 startreadworker;

    startreadworker = pg_atomic_read_u32(&(g_recordbuffer->readWorkerState));

    if (startreadworker == WORKER_STATE_STOP) {
        if (!XLogReadFromWriteBufferForFirst(targetPagePtr, reqLen, readBuf)) {
            return false;
        }

        for (uint32 i = 0; i < MAX_ALLOC_SEGNUM; ++i) {
            pg_atomic_write_u32(&(g_recordbuffer->xlogsegarray[i].bufState), NONE);
        }

        /* first read from walbuffer, start up reader */
        pg_atomic_write_u32(&(g_recordbuffer->readindex), g_recordbuffer->applyindex);
        pg_atomic_write_u32(&(g_recordbuffer->xlogsegarray[g_recordbuffer->readindex].bufState), APPLYING);
        pg_memory_barrier();
        pg_atomic_write_u32(&(g_recordbuffer->readWorkerState), WORKER_STATE_RUN);

        return true;
    } else if (startreadworker == WORKER_STATE_RUN) {
        /* read from buffer */
        XLogSegNo targetSegNo;
        uint32 targetPageOff = targetPagePtr % XLogSegSize;

        XLByteToSeg(targetPagePtr, targetSegNo);

        Assert(g_recordbuffer->applyindex < MAX_ALLOC_SEGNUM);
        RecordBufferAarray* cursegbuffer = &g_recordbuffer->xlogsegarray[g_recordbuffer->applyindex];
        uint32 bufState = pg_atomic_read_u32(&(cursegbuffer->bufState));
        if (bufState != APPLYING) {
            return false;
        }

        Assert(targetPageOff == cursegbuffer->segoffset);
        Assert(readBuf == (cursegbuffer->readsegbuf + targetPageOff));

        pg_memory_barrier();
        uint32 readlen = pg_atomic_read_u32(&(cursegbuffer->readlen));

        uint32 waitcount = 0;
        while (readlen < (targetPageOff + reqLen)) {
            readlen = pg_atomic_read_u32(&(cursegbuffer->readlen));
            if (waitcount >= MAX_WAIT_TIMS) {
                return false;
            }
            waitcount++;
        }
        return true;
    }

    return false;
}

/* read xlog for parellel */
void XLogReadPageWorkerMain()
{
    XLogReaderState *xlogreader = nullptr;
    XLogReaderState *newxlogreader = nullptr;

    knl_thread_set_name("ReadPageWorker");

    g_recordbuffer = &g_dispatcher->recordstate;
    GetRecoveryLatch();
    /* init readstate */
    InitXLogRecordReadBuffer(&xlogreader);

    pg_atomic_write_u32(&(g_recordbuffer->readPageWorkerState), WORKER_STATE_RUN);
    if (IsRecoveryDone()) {
        t_thrd.xlog_cxt.readSource = XLOG_FROM_STREAM;
        t_thrd.xlog_cxt.XLogReceiptSource = XLOG_FROM_STREAM;
        pg_atomic_write_u32(&(g_recordbuffer->readSource), XLOG_FROM_STREAM);
    }
    XLogRecord* record = XLogParallelReadNextRecord(xlogreader);
    while (record != NULL) {
        newxlogreader = NewReaderState(xlogreader);
        PutRecordToReadQueue(xlogreader);
        xlogreader = newxlogreader;

        SetCompletedReadEndPtr(g_redoWorker, xlogreader->ReadRecPtr, xlogreader->EndRecPtr);

        HandlePageRedoInterrupts();
        record = XLogParallelReadNextRecord(xlogreader);
        PushToWorkerLsn(false);
        ADD_ABNORMAL_POSITION(8);
    };
    uint32 workState = pg_atomic_read_u32(&(g_recordbuffer->readWorkerState));
    while (workState == WORKER_STATE_STOPPING) {
        workState = pg_atomic_read_u32(&(g_recordbuffer->readWorkerState));
    }

    if (workState != WORKER_STATE_EXITING && workState != WORKER_STATE_EXIT) {
        pg_atomic_write_u32(&(g_recordbuffer->readWorkerState), WORKER_STATE_EXITING);
    }
    /* notify exit */
    PushToWorkerLsn(true);
    g_redoEndMark.record = *xlogreader;
    g_redoEndMark.record.isDecode = true;
    PutRecordToReadQueue((XLogReaderState*)&g_redoEndMark.record);
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
        bufState = pg_atomic_read_u32(&(nextreadseg->bufState));
        startreadworker = pg_atomic_read_u32(&(g_recordbuffer->readWorkerState));
        HandleReadWorkerRunInterrupts();
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
    const uint32 sleepTime = 50; /* 50 us */
    XLogSegNo targetSegNo;
    uint32 writeoffset;
    uint32 reqlen;

    uint32 readindex = pg_atomic_read_u32(&(g_recordbuffer->readindex));
    Assert(readindex < MAX_ALLOC_SEGNUM);
    pg_memory_barrier();
    RecordBufferAarray *readseg = &g_recordbuffer->xlogsegarray[readindex];

    XLogRecPtr receivedUpto = GetWalRcvWriteRecPtr(NULL);
    XLByteToSeg(receivedUpto, targetSegNo);

    if (targetSegNo < readseg->segno) {
        return;
    }
    
    if (targetSegNo != readseg->segno) {
        writeoffset = readseg->readlen;
        reqlen = XLOG_SEG_SIZE - writeoffset;
    } else {
        uint32 targetPageOff;
        targetPageOff = receivedUpto % XLOG_SEG_SIZE;
        writeoffset = readseg->readlen;
        if ((writeoffset + XLOG_BLCKSZ) < targetPageOff) {
            reqlen = targetPageOff - writeoffset;
        } else {
            waitcount++;
            if ((waitcount >= MAX_WAIT_TIMS) && readseg->readlen < targetPageOff) {
                reqlen = targetPageOff - writeoffset;
                waitcount = 0;
            } else {
                /* sleep, wait a moment */
                pg_usleep(sleepTime);
                return;
            }
        }
    }
    char *readBuf = readseg->readsegbuf + writeoffset;
    XLogRecPtr targetSartPtr = readseg->segno * XLOG_SEG_SIZE + writeoffset;
    uint32 readlen;
    bool result = XLogReadFromWriteBuffer(targetSartPtr, reqlen, readBuf, &readlen);
    if (!result) {
        return;
    }

    pg_atomic_write_u32(&(readseg->readlen), (writeoffset + readlen));
    if (readseg->readlen == XLOG_SEG_SIZE) {
        InitReadBuf(readindex + 1, readseg->segno + 1);
    }
}


void XLogReadManagerResponseSignal(uint32 tgigger)
{
    switch (tgigger) {
        case TRIGGER_PRIMARY:
            break;
        case TRIGGER_FAILOVER:
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
        state = pg_atomic_read_u32(&extreme_rto::g_dispatcher->recordstate.readPageWorkerState);
        XLogReadManagerProcInterrupt();
    } while (state != WORKER_STATE_EXIT);
}

bool XLogReadManagerCheckSignal()
{
    XLogReadManagerProcInterrupt();
    
    uint32 trigger = pg_atomic_read_u32(&(extreme_rto::g_startupTriggerState));
    load_server_mode();
    if (g_dispatcher->smartShutdown || trigger == TRIGGER_PRIMARY || trigger == TRIGGER_SWITCHOVER ||
        (trigger == TRIGGER_FAILOVER && t_thrd.xlog_cxt.server_mode == STANDBY_MODE) 
        || t_thrd.xlog_cxt.server_mode == PRIMARY_MODE) {
        ereport(LOG, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
            errmsg("XLogReadManagerCheckSignal: smartShutdown:%u, trigger:%u, server_mode:%u",
            g_dispatcher->smartShutdown, trigger, t_thrd.xlog_cxt.server_mode)));
        ShutdownWalRcv();
        if (g_dispatcher->smartShutdown) {
            pg_atomic_write_u32(&g_readManagerTriggerFlag, TRIGGER_SMARTSHUTDOWN);
        } else {
            pg_atomic_write_u32(&g_readManagerTriggerFlag, trigger);
        }
        WaitPageReadWorkerExit();
        XLogReadManagerResponseSignal(trigger);
        return true;
    }
    return false;
}

void XLogReadManagerMain()
{
    const long sleepShortTime = 150000L;
    const long sleepLongTime = 1500000L;
    g_recordbuffer = &g_dispatcher->recordstate;
    bool exitStatus = false;
    uint32 xlogReadManagerState = READ_MANAGER_RUN;
    knl_thread_set_name("RedoReadManager");
    while (!exitStatus && xlogReadManagerState == READ_MANAGER_RUN) {
        exitStatus = XLogReadManagerCheckSignal();
        xlogReadManagerState = pg_atomic_read_u32(&g_dispatcher->recordstate.xlogReadManagerState);
        ADD_ABNORMAL_POSITION(7);
        if (t_thrd.xlog_cxt.server_mode == STANDBY_MODE) {
            uint32 readSource = pg_atomic_read_u32(&g_dispatcher->recordstate.readSource);
            uint32 failSource = pg_atomic_read_u32(&g_dispatcher->recordstate.failSource);
            if (readSource & XLOG_FROM_STREAM) {
                if (!WalRcvInProgress() && g_instance.pid_cxt.WalReceiverPID == 0 && 
                    !g_instance.comm_cxt.localinfo_cxt.need_disable_connection_node) {
                    volatile WalRcvData* walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
                    XLogRecPtr expectLsn = pg_atomic_read_u64(&g_dispatcher->recordstate.expectLsn);
                    if (walrcv->receivedUpto == InvalidXLogRecPtr || 
                        (expectLsn != InvalidXLogRecPtr && XLByteLE(walrcv->receivedUpto, expectLsn))) {
                        uint32 readWorkerstate = pg_atomic_read_u32(&(g_recordbuffer->readWorkerState));
                        if (readWorkerstate == WORKER_STATE_RUN) {
                            pg_atomic_write_u32(&(g_recordbuffer->readWorkerState), WORKER_STATE_STOPPING);
                        }
                        SpinLockAcquire(&walrcv->mutex);
                        walrcv->receivedUpto = 0;
                        SpinLockRelease(&walrcv->mutex);
                        XLogRecPtr targetRecPtr = pg_atomic_read_u64(&g_dispatcher->recordstate.targetRecPtr);
                        ereport(LOG, (errmsg("request xlog stream at %X/%X.", (uint32)(targetRecPtr >> 32), 
                            (uint32)targetRecPtr)));
                        RequestXLogStreaming(&targetRecPtr, t_thrd.xlog_cxt.PrimaryConnInfo, 
                            REPCONNTARGET_PRIMARY, u_sess->attr.attr_storage.PrimarySlotName);
                    }
                } else {
                    if (g_instance.comm_cxt.localinfo_cxt.need_disable_connection_node) {
                        g_instance.comm_cxt.localinfo_cxt.need_disable_connection_node = false;
                        if (!WalRcvInProgress()) {
                            volatile WalRcvData* walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
                            XLogRecPtr expectLsn = pg_atomic_read_u64(&g_dispatcher->recordstate.expectLsn);
                            if (expectLsn != InvalidXLogRecPtr && XLByteLE(walrcv->receivedUpto, expectLsn)) {
                                pg_usleep(sleepLongTime);
                            }
                        }
                    }
                }
            }

            if (failSource & XLOG_FROM_STREAM) {
                ShutdownWalRcv();
                pg_atomic_write_u32(&(extreme_rto::g_dispatcher->recordstate.failSource), 0);
            }
        }
        pg_usleep(sleepShortTime);
    }
}


static void ReadWorkerStopCallBack(int code, Datum arg)
{
    pg_atomic_write_u32(&(g_recordbuffer->readWorkerState), WORKER_STATE_EXIT);
}

void XLogReadWorkerMain()
{
    uint32 startreadworker;    
    const uint32 sleepTime = 50; /* 50 us */
    
    on_shmem_exit(ReadWorkerStopCallBack, 0);
    knl_thread_set_name("RedoReadWorker");
    g_recordbuffer = &g_dispatcher->recordstate;
    startreadworker = pg_atomic_read_u32(&(g_recordbuffer->readWorkerState));
    while (startreadworker != WORKER_STATE_EXITING) {
        if (startreadworker == WORKER_STATE_RUN) {
            XLogReadWorkRun();
        } else {
            pg_usleep(sleepTime);
        }

        HandleReadWorkerRunInterrupts();
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

void WaitStateNormal()
{
    do {
        HandlePageRedoInterrupts();
    } while (g_instance.comm_cxt.predo_cxt.state < REDO_IN_PROGRESS);
}

/* Run from the worker thread. */
void ParallelRedoThreadMain()
{
    ParallelRedoThreadRegister();
    ereport(LOG, (errmsg("Page-redo-worker thread %u started, role:%u, slotId:%u.", g_redoWorker->id,
                         g_redoWorker->role, g_redoWorker->slotId)));

    SetupSignalHandlers();
    InitGlobals();
    ResourceManagerStartup();
    WaitStateNormal();
    SetForwardFsyncRequests();

    int retCode = RedoMainLoop();
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
    (void)gspqsignal(SIGALRM, SIG_IGN);
    (void)gspqsignal(SIGPIPE, SIG_IGN);
    (void)gspqsignal(SIGUSR1, SIG_IGN);
    (void)gspqsignal(SIGUSR2, PageRedoUser2Handler);
    (void)gspqsignal(SIGCHLD, SIG_IGN);
    (void)gspqsignal(SIGTTIN, SIG_IGN);
    (void)gspqsignal(SIGTTOU, SIG_IGN);
    (void)gspqsignal(SIGCONT, SIG_IGN);
    (void)gspqsignal(SIGWINCH, SIG_IGN);

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

    t_thrd.xlog_cxt.ArchiveRecoveryRequested = g_redoWorker->ArchiveRecoveryRequested;
    t_thrd.xlog_cxt.StandbyModeRequested = g_redoWorker->StandbyModeRequested;
    t_thrd.xlog_cxt.InArchiveRecovery = g_redoWorker->InArchiveRecovery;
    t_thrd.xlog_cxt.InRecovery = g_redoWorker->InRecovery;
    t_thrd.xlog_cxt.ArchiveRestoreRequested = g_redoWorker->ArchiveRestoreRequested;
    t_thrd.xlog_cxt.minRecoveryPoint = g_redoWorker->minRecoveryPoint;
}

void RedoThrdWaitForExit(const PageRedoWorker* wk)
{
    uint32 sd = wk->slotId;
    PageRedoPipeline* pl = nullptr;
    uint32 i;
    switch (wk->role) {
        case REDO_BATCH:
            SendPageRedoEndMark(g_dispatcher->pageLines[sd].managerThd);
            WaitPageRedoWorkerReachLastMark(g_dispatcher->pageLines[sd].managerThd);
            break;
        case REDO_PAGE_MNG:
            pl = &(g_dispatcher->pageLines[sd]);
            for (i = 0; i < pl->redoThdNum; i++) {
                SendPageRedoEndMark(pl->redoThd[i]);
            }
            for (i = 0; i < pl->redoThdNum; i++) {
                WaitPageRedoWorkerReachLastMark(pl->redoThd[i]);
            }
            break;
        case REDO_PAGE_WORKER:
            break; /* Don't need to wait for anyone */
        case REDO_TRXN_MNG:
            SendPageRedoEndMark(g_dispatcher->trxnLine.redoThd);
            WaitPageRedoWorkerReachLastMark(g_dispatcher->trxnLine.redoThd);
            GlobalLsnUpdate();
            break;
        case REDO_TRXN_WORKER:
            break; /* Don't need to wait for anyone */
        default:
            break;
    }
}

/* Run from the worker thread. */
static void ApplySinglePageRecord(RedoItem* item)
{
    XLogReaderState* record = &item->record;
    bool bOld = item->oldVersion;

    MemoryContext oldCtx = MemoryContextSwitchTo(g_redoWorker->oldCtx);
    ApplyRedoRecord(record, bOld);
    (void)MemoryContextSwitchTo(oldCtx);
}

/* Run from the worker thread. */
static void LastMarkReached()
{
    PosixSemaphorePost(&g_redoWorker->phaseMarker);
}

/* Run from the dispatcher thread. */
void WaitPageRedoWorkerReachLastMark(PageRedoWorker* worker)
{
    PosixSemaphoreWait(&worker->phaseMarker);
}

/* Run from the dispatcher thread. */
void AddPageRedoItem(PageRedoWorker* worker, void* item)
{
    SPSCBlockingQueuePut(worker->queue, item);
}

/* Run from the dispatcher thread. */
bool SendPageRedoEndMark(PageRedoWorker* worker)
{
    return SPSCBlockingQueuePut(worker->queue, &g_redoEndMark);
}

/* Run from the dispatcher thread. */
bool SendPageRedoWorkerTerminateMark(PageRedoWorker* worker)
{
    return SPSCBlockingQueuePut(worker->queue, &g_terminateMark);
}

/* Run from the dispatcher thread. */
bool ProcessPendingPageRedoItems(PageRedoWorker* worker)
{
    if (worker->pendingHead == NULL) {
        return true;
    }

    if (SPSCBlockingQueuePut(worker->queue, worker->pendingHead)) {
        worker->pendingHead = NULL;
        worker->pendingTail = NULL;

        return true;
    }

    return false;
}

/* Run from the txn worker thread. */
void UpdatePageRedoWorkerStandbyState(PageRedoWorker* worker, HotStandbyState newState)
{
    /*
     * Here we only save the new state into the worker struct.
     * The actual update of the worker thread's state occurs inside
     * the apply loop.
     */
    worker->standbyState = newState;
}

/* Run from the txn worker thread. */
XLogRecPtr GetCompletedRecPtr(PageRedoWorker *worker)
{
    return pg_atomic_read_u64(&worker->lastReplayedEndRecPtr);
}

void SetWorkerRestartPoint(PageRedoWorker *worker, XLogRecPtr restartPoint)
{
    pg_atomic_write_u64((uint64 *)&worker->lastCheckedRestartPoint, restartPoint);
}

/* Run from the dispatcher thread. */
void* GetXLogInvalidPages(PageRedoWorker* worker)
{
    return worker->xlogInvalidPages;
}

bool RedoWorkerIsIdle(PageRedoWorker* worker)
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

// TEST_LOG
void DumpItem(RedoItem* item, const char* funcName)
{
    return;
    if (item == &g_redoEndMark || item == &g_terminateMark) {
        return;
    }
    ereport(DEBUG4, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                     errmsg("[REDO_LOG_TRACE]DiagLogRedoRecord: %s, ReadRecPtr:%lu,EndRecPtr:%lu,"
                            "oldVersion:%u,"
                            "imcheckpoint:%u, shareCount:%d,"
                            "designatedWorker:%u, recordXTime:%lu, refCount:%u, replayed:%d,"
                            "syncXLogReceiptSource:%d, RecentXmin:%lu, syncServerMode:%u",
                            funcName, item->record.ReadRecPtr, item->record.EndRecPtr, item->oldVersion,
                            item->needImmediateCheckpoint, item->shareCount, item->designatedWorker, item->recordXTime,
                            item->refCount, item->replayed,

                            item->syncXLogReceiptSource, item->RecentXmin, item->syncServerMode)));
    DiagLogRedoRecord(&(item->record), funcName);
}

void DumpExtremeRtoReadBuf()
{
    if (g_dispatcher == NULL) {
        return;
    }

    ereport(LOG, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                  errmsg("DumpExtremeRtoReadBuf: startworker %u, readindex %u, applyindex %u",
                         g_dispatcher->recordstate.readWorkerState, g_dispatcher->recordstate.readindex,
                         g_dispatcher->recordstate.applyindex)));

    for (uint32 i = 0; i < MAX_ALLOC_SEGNUM; ++i) {
        ereport(LOG, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                      errmsg("DumpExtremeRtoReadBuf: buf %u, state %u, readlen %u, segno %lu, segoffset %lu", i,
                             g_dispatcher->recordstate.xlogsegarray[i].bufState,
                             g_dispatcher->recordstate.xlogsegarray[i].readlen,
                             g_dispatcher->recordstate.xlogsegarray[i].segno,
                             g_dispatcher->recordstate.xlogsegarray[i].segoffset)));
    }
}

}  // namespace extreme_rto
