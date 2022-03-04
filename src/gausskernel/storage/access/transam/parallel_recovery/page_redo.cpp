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
#include "catalog/storage_xlog.h"
#include "gssignal/gs_signal.h"
#include "libpq/pqsignal.h"
#include "postmaster/postmaster.h"
#include "postmaster/pagerepair.h"
#include "storage/ipc.h"
#include "access/nbtree.h"
#include "utils/guc.h"
#include "utils/palloc.h"
#include "portability/instr_time.h"
#include "catalog/storage.h"
#include <pthread.h>
#include <sched.h>
#include "commands/dbcommands.h"
#include "access/parallel_recovery/page_redo.h"
#include "access/parallel_recovery/dispatcher.h"
#include "access/multi_redo_api.h"
#include "pgstat.h"

namespace parallel_recovery {

static const char *const PROCESS_TYPE_CMD_ARG = "--forkpageredo";
static char g_AUXILIARY_TYPE_CMD_ARG[16] = {0};
static const uint32 REDO_WORKER_ALIGN_LEN = 16; /* need 128-bit aligned */

THR_LOCAL PageRedoWorker *g_redoWorker = NULL;

static RedoItem g_redoEndMark = {};
static RedoItem g_terminateMark = {};
static RedoItem g_cleanupMark = { false, false, false, false, 0};
static RedoItem g_closefdMark = { false, false, false, false, 0};
static RedoItem g_cleanInvalidPageMark = { false, false, false, false, 0};

static const int PAGE_REDO_WORKER_ARG = 3;
static const int REDO_SLEEP_50US = 50;
static const int REDO_SLEEP_100US = 100;

static void AddSafeRecoveryRestartPoint(XLogRecPtr restartPoint);
static void ApplyAndFreeRedoItem(RedoItem *);
static void ApplyMultiPageRecord(RedoItem *, bool replayUndo = false);
static int ApplyRedoLoop();
static void ApplySinglePageRecord(RedoItem *, bool replayUndo = false);
static PageRedoWorker *CreateWorker(uint32);
static void DeleteExpiredRecoveryRestartPoint();
static void InitGlobals();
static void LastMarkReached();
static void SetupSignalHandlers();
static void SigHupHandler(SIGNAL_ARGS);
static ThreadId StartWorkerThread(PageRedoWorker *);
static void ApplyMultiPageShareWithTrxnRecord(RedoItem *item);
static void ApplyMultiPageSyncWithTrxnRecord(RedoItem *item);
static void ApplyMultiPageAllWorkerRecord(RedoItem *item);
static void ApplyRecordWithoutSyncUndoLog(RedoItem *item);

bool DoPageRedoWorkerReplayUndo()
{
    return g_redoWorker->replay_undo;
}

RedoWorkerRole GetRedoWorkerRole()
{
    return g_redoWorker->role;
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
PageRedoWorker *StartPageRedoWorker(uint32 id)
{
    PageRedoWorker *worker = CreateWorker(id);

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



/* Run from the dispatcher thread. */
static PageRedoWorker *CreateWorker(uint32 id)
{
    PageRedoWorker *tmp = (PageRedoWorker *)palloc0(sizeof(PageRedoWorker) + REDO_WORKER_ALIGN_LEN);
    PageRedoWorker *worker;
    uint32             parallelism = get_real_recovery_parallelism();
    uint32             undoZidWorkersNum = get_recovery_undozidworkers_num();

    worker = (PageRedoWorker *)TYPEALIGN(REDO_WORKER_ALIGN_LEN, tmp);

    /* Use the second half as undo log workers */
    if (SUPPORT_USTORE_UNDO_WORKER &&
        id >= (parallelism - undoZidWorkersNum)) {
        worker->role = UndoLogZidWorker;
    } else {
        worker->role = DataPageWorker;
    }

    worker->selfOrinAddr = tmp;
    worker->id = id;
    worker->originId = id;
    worker->index = 0;
    worker->tid.thid = InvalidTid;
    worker->proc = NULL;
    worker->initialServerMode = (ServerMode)t_thrd.xlog_cxt.server_mode;
    worker->initialTimeLineID = t_thrd.xlog_cxt.ThisTimeLineID;
    worker->pendingHead = NULL;
    worker->pendingTail = NULL;
    worker->queue = SPSCBlockingQueueCreate(PAGE_WORK_QUEUE_SIZE, RedoWorkerQueueCallBack);
    worker->safePointHead = NULL;
    worker->lastCheckedRestartPoint = 0;
    worker->lastReplayedEndRecPtr = 0;
    worker->standbyState = (HotStandbyState)t_thrd.xlog_cxt.standbyState;
    worker->DataDir = t_thrd.proc_cxt.DataDir;
    worker->RecentXmin = u_sess->utils_cxt.RecentXmin;
    worker->btreeIncompleteActions = NULL;
    worker->xlogInvalidPages = NULL;
    PosixSemaphoreInit(&worker->phaseMarker, 0);
    worker->statMulpageCnt = 0;
    worker->statWaitReach = 0;
    worker->statWaitReplay = 0;
    worker->oldCtx = NULL;
    worker->bufferPinWaitBufId = -1;
    worker->remoteReadPageNum = 0;
    
    worker->badPageHashTbl = BadBlockHashTblCreate();

    pg_atomic_write_u32(&(worker->readyStatus), PAGE_REDO_WORKER_INVALID);
#if (!defined __x86_64__) && (!defined __aarch64__)
    SpinLockInit(&worker->ptrLck);
#endif
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
    pfree(worker->selfOrinAddr);
}

/* Run from both the dispatcher and the worker thread. */
bool IsPageRedoWorkerProcess(int argc, char *argv[])
{
    return strcmp(argv[1], PROCESS_TYPE_CMD_ARG) == 0;
}

/* Run from the worker thread. */
void AdaptArgvForPageRedoWorker(char *argv[])
{
    if (g_AUXILIARY_TYPE_CMD_ARG[0] == 0) {
        sprintf_s(g_AUXILIARY_TYPE_CMD_ARG, sizeof(g_AUXILIARY_TYPE_CMD_ARG), "-x%d", PageRedoProcess);
    }
    argv[3] = g_AUXILIARY_TYPE_CMD_ARG;
}

/* Run from the worker thread. */
void GetThreadNameIfPageRedoWorker(int argc, char *argv[], char **threadNamePtr)
{
    if (*threadNamePtr == NULL && IsPageRedoWorkerProcess(argc, argv)) {
        *threadNamePtr = "PageRedoWorker";
    }
}

/* Run from the worker thread. */
uint32 GetMyPageRedoWorkerOrignId()
{
    bool isWorkerStarting = false;
    SpinLockAcquire(&(g_instance.comm_cxt.predo_cxt.rwlock));
    isWorkerStarting = ((g_instance.comm_cxt.predo_cxt.state == REDO_STARTING_BEGIN) ? true : false);
    SpinLockRelease(&(g_instance.comm_cxt.predo_cxt.rwlock));
    if (!isWorkerStarting) {
        ereport(WARNING, (errmsg("GetMyPageRedoWorkerOrignId Page-redo-worker exit.")));
        proc_exit(0);
    }

    return g_redoWorker->originId;
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
    if (t_thrd.page_redo_cxt.check_repair) {
        SeqCheckRemoteReadAndRepairPage();
        t_thrd.page_redo_cxt.check_repair = false;
    }

    if (t_thrd.page_redo_cxt.shutdown_requested) {
        ereport(LOG,
                (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                 errmsg("page worker(id %u, originId %u) exit for request", g_redoWorker->id, g_redoWorker->originId)));

        pg_atomic_write_u32(
            &(g_instance.comm_cxt.predo_cxt.pageRedoThreadStatusList[g_redoWorker->originId].threadState),
            PAGE_REDO_WORKER_EXIT);

        proc_exit(1);
    }
}

/* HandleRedoPageRepair
 *           if the page crc verify failed, call the function record the bad block.
 */
void HandleRedoPageRepair(RepairBlockKey key, XLogPhyBlock pblk)
{
    XLogReaderState *record = g_redoWorker->current_item;
    RecordBadBlockAndPushToRemote(record, key, CRC_CHECK_FAIL, InvalidXLogRecPtr, pblk);
    return;
}

/* Run from the worker thread. */
void PageRedoWorkerMain()
{
    bool isWorkerStarting = false;
    SpinLockAcquire(&(g_instance.comm_cxt.predo_cxt.rwlock));
    isWorkerStarting = ((g_instance.comm_cxt.predo_cxt.state == REDO_STARTING_BEGIN) ? true : false);
    if (isWorkerStarting) {
        pg_atomic_write_u32(
            &(g_instance.comm_cxt.predo_cxt.pageRedoThreadStatusList[g_redoWorker->originId].threadState),
            PAGE_REDO_WORKER_READY);
    }
    SpinLockRelease(&(g_instance.comm_cxt.predo_cxt.rwlock));
    if (!isWorkerStarting) {
        ereport(WARNING, (errmsg("PageRedoWorkerMain Page-redo-worker %u exit.", (uint32)isWorkerStarting)));
        SetPageWorkStateByThreadId(PAGE_REDO_WORKER_EXIT);
        proc_exit(0);
    }
    ereport(LOG, (errmsg("Page-redo-worker thread %u started.", g_redoWorker->id)));

    SetupSignalHandlers();
    (void)RegisterRedoInterruptCallBack(HandlePageRedoInterrupts);
    (void)RegisterRedoPageRepairCallBack(HandleRedoPageRepair);

    InitGlobals();
    ResourceManagerStartup();

    g_redoWorker->oldCtx = MemoryContextSwitchTo(g_instance.comm_cxt.predo_cxt.parallelRedoCtx);

    int retCode = ApplyRedoLoop();

    (void)MemoryContextSwitchTo(g_redoWorker->oldCtx);

    ResourceManagerStop();
    ereport(LOG, (errmsg("Page-redo-worker thread %u terminated, retcode %d.", g_redoWorker->id, retCode)));
    LastMarkReached();
    pg_atomic_write_u32(&(g_instance.comm_cxt.predo_cxt.pageRedoThreadStatusList[g_redoWorker->originId].threadState),
                        PAGE_REDO_WORKER_EXIT);
    proc_exit(0);
}

static void PageRedoShutdownHandler(SIGNAL_ARGS)
{
    t_thrd.page_redo_cxt.shutdown_requested = true;
}

static void PageRedoSigUser1Handler(SIGNAL_ARGS)
{
    t_thrd.page_redo_cxt.check_repair = true;
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
    t_thrd.page_redo_cxt.sleep_long = true;
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
    (void)gspqsignal(SIGUSR1, PageRedoSigUser1Handler);
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
    t_thrd.utils_cxt.CurrentResourceOwner = ResourceOwnerCreate(NULL, "ParallelRedoThread",
        THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));

    t_thrd.xlog_cxt.server_mode = g_redoWorker->initialServerMode;
    t_thrd.xlog_cxt.ThisTimeLineID = g_redoWorker->initialTimeLineID;
    /* apply recoveryinfo will change standbystate see UpdateRecordGlobals */
    t_thrd.xlog_cxt.standbyState = g_redoWorker->standbyState;
    t_thrd.xlog_cxt.InRecovery = true;
    t_thrd.xlog_cxt.startup_processing = true;
    t_thrd.proc_cxt.DataDir = g_redoWorker->DataDir;
    u_sess->utils_cxt.RecentXmin = g_redoWorker->RecentXmin;
    g_redoWorker->proc = t_thrd.proc;
}

void ApplyProcHead(RedoItem *head)
{
    while (head != NULL) {
        RedoItem *cur = head;
        g_redoWorker->current_item = &cur->record;
        head = head->nextByWorker[g_redoWorker->id + 1];
        ApplyAndFreeRedoItem(cur);
    }
}

/* Run from the worker thread. */
static int ApplyRedoLoop()
{
    RedoItem *head;
    instr_time startTime;
    instr_time endTime;

    INSTR_TIME_SET_CURRENT(startTime);
    GetRedoStartTime(g_redoWorker->timeCostList[TIME_COST_STEP_1]);
    while ((head = (RedoItem *)SPSCBlockingQueueTop(g_redoWorker->queue)) != &g_redoEndMark) {
        CountRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_1]);
        if (head == &g_cleanupMark) {
            g_redoWorker->btreeIncompleteActions = btree_get_incomplete_actions();
            g_redoWorker->xlogInvalidPages = XLogGetInvalidPages();
        } else if (head == &g_closefdMark) {
            smgrcloseall();
        } else if (head == &g_cleanInvalidPageMark) {
            forget_range_invalid_pages((void*)head);
        } else {
            ApplyProcHead(head);
        }
        SPSCBlockingQueuePop(g_redoWorker->queue);
        RedoInterruptCallBack();
        GetRedoStartTime(g_redoWorker->timeCostList[TIME_COST_STEP_1]);
    }
    SPSCBlockingQueuePop(g_redoWorker->queue);

    INSTR_TIME_SET_CURRENT(endTime);
    INSTR_TIME_SUBTRACT(endTime, startTime);

    ereport(LOG, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                  errmsg("worker[%u]: multipage cnt = %u, wait reach elapsed %lu us, "
                         "wait replay elapsed %lu us, total elapsed = %lu",
                         g_redoWorker->id, g_redoWorker->statMulpageCnt, g_redoWorker->statWaitReach,
                         g_redoWorker->statWaitReplay, INSTR_TIME_GET_MICROSEC(endTime))));

    /*
     * We need to get the exit code here before we allow the dispatcher
     * to proceed and change the exit code.
     */
    int exitCode = GetDispatcherExitCode();
    g_redoWorker->btreeIncompleteActions = btree_get_incomplete_actions();
    g_redoWorker->xlogInvalidPages = XLogGetInvalidPages();

    return exitCode;
}

/* Run from the worker thread. */
static void ApplyAndFreeRedoItem(RedoItem *item)
{
    GetRedoStartTime(g_redoWorker->timeCostList[TIME_COST_STEP_3]);
    UpdateRecordGlobals(item, g_redoWorker->standbyState);
    CountRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_3]);
    /*
     * We need to save the LSN here because the following apply will
     * free the item.
     */
    XLogRecPtr endLSN = item->record.EndRecPtr;
    XLogRecPtr readLSN = item->record.ReadRecPtr;

    if (item->replay_undo) {
        GetRedoStartTime(g_redoWorker->timeCostList[TIME_COST_STEP_4]);
        g_redoWorker->replay_undo = (g_redoWorker->role == UndoLogZidWorker);
        ApplyRecordWithoutSyncUndoLog(item);
        SetCompletedReadEndPtr(g_redoWorker, readLSN, endLSN);
        CountRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_4]);
    } else if (IsLSNMarker(item)) {
        FreeRedoItem(item);
        SetCompletedReadEndPtr(g_redoWorker, readLSN, endLSN);
    } else if (item->sharewithtrxn) {
        GetRedoStartTime(g_redoWorker->timeCostList[TIME_COST_STEP_5]);
        ApplyMultiPageShareWithTrxnRecord(item);
        CountRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_5]);
    } else if (item->blockbytrxn) {
        GetRedoStartTime(g_redoWorker->timeCostList[TIME_COST_STEP_6]);
        ApplyMultiPageSyncWithTrxnRecord(item);
        CountRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_6]);
    } else if (item->shareCount == 1) {
        GetRedoStartTime(g_redoWorker->timeCostList[TIME_COST_STEP_7]);
        ApplySinglePageRecord(item);
        SetCompletedReadEndPtr(g_redoWorker, readLSN, endLSN);
        CountRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_7]);
    } else {
        if (item->designatedWorker == ALL_WORKER) {
            GetRedoStartTime(g_redoWorker->timeCostList[TIME_COST_STEP_8]);
            ApplyMultiPageAllWorkerRecord(item);
            CountRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_8]);
        } else {
            GetRedoStartTime(g_redoWorker->timeCostList[TIME_COST_STEP_9]);
            ApplyMultiPageRecord(item);
            SetCompletedReadEndPtr(g_redoWorker, readLSN, endLSN);
            CountRedoTime(g_redoWorker->timeCostList[TIME_COST_STEP_9]);
        }
    }
}

/* Run from the worker thread */
static void ApplyRecordWithoutSyncUndoLog(RedoItem *item)
{
    /* No need to sync with undo log */
    if (g_redoWorker->replay_undo) {
        Assert(g_redoWorker->role == UndoLogZidWorker);
        MemoryContext oldCtx = MemoryContextSwitchTo(g_redoWorker->oldCtx);
        ApplyRedoRecord(&item->record);
        (void)MemoryContextSwitchTo(oldCtx);
        uint32 shrCount = pg_atomic_read_u32(&item->shareCount);
        uint32 trueRefCount = pg_atomic_add_fetch_u32(&item->trueRefCount, 1);
        uint32 undoZidWorkersNum = get_recovery_undozidworkers_num();

        /* The last one to replay the item */
        if (trueRefCount == (shrCount + undoZidWorkersNum))
            FreeRedoItem(item);

    } else {

        /* When the worker role is DataPageWorker */
        Assert(g_redoWorker->role == DataPageWorker);

        if (g_instance.attr.attr_storage.EnableHotStandby) {
            XLogReaderState *record = &item->record;
            XLogRecPtr lrRead;
            XLogRecPtr lrEnd;

            GetReplayedRecPtrFromUndoWorkers(&lrRead, &lrEnd);
        
            while (XLByteLT(lrEnd, record->EndRecPtr)) {
                GetReplayedRecPtrFromUndoWorkers(&lrRead, &lrEnd);
                RedoInterruptCallBack();
            }
        }
    
        if (item->shareCount == 1)
            ApplySinglePageRecord(item, true);
        else
            ApplyMultiPageRecord(item, true);
    }
}

/* Run from the worker thread. */
static void ApplySinglePageRecord(RedoItem *item, bool replayUndo)
{
    XLogReaderState *record = &item->record;
    long readbufcountbefore = u_sess->instr_cxt.pg_buffer_usage->local_blks_read;
    MemoryContext oldCtx = MemoryContextSwitchTo(g_redoWorker->oldCtx);
    ApplyRedoRecord(record);
    (void)MemoryContextSwitchTo(oldCtx);
    record->readblocks = u_sess->instr_cxt.pg_buffer_usage->local_blks_read - readbufcountbefore;

    if (replayUndo) {
        uint32 shrCount = pg_atomic_read_u32(&item->shareCount);
        uint32 trueRefCount = pg_atomic_add_fetch_u32(&item->trueRefCount, 1);
        uint32 undoZidWorkersNum = get_recovery_undozidworkers_num();
        if (trueRefCount == (shrCount + undoZidWorkersNum)) {
            FreeRedoItem(item);
        }
    } else {
        FreeRedoItem(item);
    }
}

static void AddSafeRecoveryRestartPoint(XLogRecPtr restartPoint)
{
    SafeRestartPoint *oldHead = (SafeRestartPoint *)pg_atomic_read_uintptr((uintptr_t *)&g_redoWorker->safePointHead);
    SafeRestartPoint *newPoint = (SafeRestartPoint *)palloc(sizeof(SafeRestartPoint));
    newPoint->next = oldHead;
    newPoint->restartPoint = restartPoint;

    pg_atomic_write_uintptr((uintptr_t *)&g_redoWorker->safePointHead, (uintptr_t)newPoint);

    DeleteExpiredRecoveryRestartPoint();
}

static void DeleteExpiredRecoveryRestartPoint()
{
    XLogRecPtr expireLSN = pg_atomic_read_u64(&g_redoWorker->lastCheckedRestartPoint);
    SafeRestartPoint *prev = (SafeRestartPoint *)pg_atomic_read_uintptr((uintptr_t *)&g_redoWorker->safePointHead);
    SafeRestartPoint *cur = prev->next;
    while (cur != NULL) {
        if (cur->restartPoint <= expireLSN) {
            prev->next = NULL;
            break;
        }
        prev = cur;
        cur = cur->next;
    }
    while (cur != NULL) {
        prev = cur;
        cur = cur->next;
        pfree(prev);
    }
}

static void WaitAndApplyMultiPageRecord(RedoItem *item)
{
    uint64 blockcnt = 0;

    /* Wait until every worker has reached this record. */
    pgstat_report_waitevent(WAIT_EVENT_PREDO_APPLY);
    while (pg_atomic_read_u32(&item->refCount) != item->shareCount) {
        blockcnt++;
        if ((blockcnt & OUTPUT_WAIT_COUNT) == OUTPUT_WAIT_COUNT) {
            XLogRecPtr LatestReplayedRecPtr = GetXLogReplayRecPtr(NULL);
            ereport(WARNING, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                              errmsg("[REDO_LOG_TRACE]MultiPageRecord:recordEndLsn:%lu, blockcnt:%lu, "
                                     "Workerid:%u, shareCount:%u, refcount:%u, LatestReplayedRecPtr:%lu",
                                     item->record.EndRecPtr, blockcnt, g_redoWorker->id, item->shareCount,
                                     item->refCount, LatestReplayedRecPtr)));
        }
        RedoInterruptCallBack();
    };
    pgstat_report_waitevent(WAIT_EVENT_END);

    MemoryContext oldCtx = MemoryContextSwitchTo(g_redoWorker->oldCtx);
    /* Apply the record and wake up other workers. */
    ApplyRedoRecord(&item->record);
    (void)MemoryContextSwitchTo(oldCtx);

    pg_memory_barrier();

    pg_atomic_write_u32(&item->replayed, 1);
}

static void WaitForApplyMultiPageRecord(RedoItem *item)
{
    uint64 blockcnt = 0;

    /* Wait until the record has been applied. */
    pgstat_report_waitevent(WAIT_EVENT_PREDO_APPLY);
    while (pg_atomic_read_u32(&item->replayed) == 0) {
        blockcnt++;
        if ((blockcnt & OUTPUT_WAIT_COUNT) == OUTPUT_WAIT_COUNT) {
            XLogRecPtr LatestReplayedRecPtr = GetXLogReplayRecPtr(NULL);
            ereport(WARNING, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                              errmsg("[REDO_LOG_TRACE]MultiPageRecord:recordEndLsn:%lu, blockcnt:%lu, Workerid:%u,"
                                     " replayed:%u, LatestReplayedRecPtr:%lu",
                                     item->record.EndRecPtr, blockcnt, g_redoWorker->id, item->replayed,
                                     LatestReplayedRecPtr)));
        }
        RedoInterruptCallBack();
    };
    pgstat_report_waitevent(WAIT_EVENT_END);
}

static void ApplyMultiPageRecord(RedoItem *item, bool replayUndo)
{
    g_redoWorker->statMulpageCnt++;
    uint32 refCount = pg_atomic_add_fetch_u32(&item->refCount, 1);

    /*
     * We are responsible for applying this record if we are the designated
     * worker or if there is no designated woker and we are the last worker
     * who reached this record.
     */
    uint32 designatedWorker = item->designatedWorker;
    bool isLast = (refCount == item->shareCount);
    bool doApply = false;

    /* 
     * UStore parallel replay want to make sure the redo worker dispatcher by data page rnode 
     * to replay the record. Update could has two block, for this fix will use the first block
     */
    if (item->designatedWorker == USTORE_WORKER) {
        XLogReaderState *record = &(item->record);
        DecodedBkpBlock *block = &record->blocks[0];

        /* Currently, concurrent reply can be performed only by filenode. */
        uint32 desire_page_worker_id = GetWorkerId(block->rnode, 0, 0);
        doApply = (desire_page_worker_id == GetMyPageRedoWorkerId());
    } else {
        doApply = (designatedWorker == GetMyPageRedoWorkerId() || (designatedWorker == ANY_WORKER && isLast));
    }

    if (doApply) {
        WaitAndApplyMultiPageRecord(item);
    } else {
        WaitForApplyMultiPageRecord(item);
    }

    /*
     * If we are the last worker leaving this record, we should destroy
     * the copied record.
     */
    if (replayUndo) {
        uint32 shrCount = pg_atomic_read_u32(&item->shareCount);
        uint32 trueRefCount = pg_atomic_add_fetch_u32(&item->trueRefCount, 1);
        uint32 undoZidWorkersNum = get_recovery_undozidworkers_num();
        if (trueRefCount == (shrCount + undoZidWorkersNum))
            FreeRedoItem(item);
    } else if (pg_atomic_sub_fetch_u32(&item->refCount, 1) == 0) {
        FreeRedoItem(item);
    }
}

static void ApplyMultiPageAllWorkerRecord(RedoItem *item)
{
    g_redoWorker->statMulpageCnt++;
    XLogReaderState *record = &item->record;
    XLogRecPtr endLSN = item->record.EndRecPtr;
    XLogRecPtr readLSN = item->record.ReadRecPtr;
    (void)pg_atomic_add_fetch_u32(&item->refCount, 1);
    errno_t rc;

    if (IsCheckPoint(record)) {
        CheckPoint checkPoint;
        rc = memcpy_s(&checkPoint, sizeof(checkPoint), XLogRecGetData(record), sizeof(checkPoint));
        securec_check(rc, "\0", "\0");

        if (HasTimelineUpdate(record)) {
            UpdateTimeline(&checkPoint);
        }

        if (IsRestartPointSafe(checkPoint.redo)) {
            AddSafeRecoveryRestartPoint(record->ReadRecPtr);
        }
    } else if (IsDataBaseDrop(record)) {
        xl_dbase_drop_rec *xlrec = (xl_dbase_drop_rec *)XLogRecGetData(record);
        MemoryContext oldCtx = MemoryContextSwitchTo(g_redoWorker->oldCtx);
        /* process invalid db and close files */
        XLogDropDatabase(xlrec->db_id);
        (void)MemoryContextSwitchTo(oldCtx);
    } else if (IsSegPageShrink(record)) {
        XLogDataSpaceShrink *xlog_data = (XLogDataSpaceShrink *)XLogRecGetData(record);
        /* forget metadata buffer that uses physical block number */
        XLogTruncateRelation(xlog_data->rnode, xlog_data->forknum, xlog_data->target_size);
        /* forget data buffer that uses logical block number */
        XLogTruncateSegmentSpace(xlog_data->rnode, xlog_data->forknum, xlog_data->target_size);
    } else if (IsSegPageDropSpace(record)) {
        char *data = (char *)XLogRecGetData(record);
        Oid spcNode = *(Oid *)data;
        Oid dbNode = *(Oid *)(data + sizeof(Oid));
        XLogDropSegmentSpace(spcNode, dbNode);
    } else {
        ereport(PANIC, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                        errmsg("[REDO_LOG_TRACE]ApplyMultiPageAllWorkerRecord encounter fatal error:"
                               "rmgrID:%u, info:%u",
                               (uint32)XLogRecGetRmid(&item->record), (uint32)XLogRecGetInfo(&item->record))));
    }

    pg_memory_barrier();

    SetCompletedReadEndPtr(g_redoWorker, readLSN, endLSN);
    /*
     * the trxn is the last worker leaving this record, when trxn apply it will destroy
     * the copied record.
     */
}

static void ApplyMultiPageShareWithTrxnRecord(RedoItem *item)
{
    (void)pg_atomic_add_fetch_u32(&item->refCount, 1);
    uint32 designatedWorker = item->designatedWorker;
    XLogRecPtr endLSN = item->record.EndRecPtr;
    XLogRecPtr readLSN = item->record.ReadRecPtr;

    g_redoWorker->statMulpageCnt++;

    if ((designatedWorker != ALL_WORKER) && (designatedWorker != TRXN_WORKER)) {
        ereport(PANIC, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                        errmsg("[REDO_LOG_TRACE]ApplyMultiPageShareWithTrxnRecord encounter fatal error:rmgrID:%u, "
                               "info:%u, designatedWorker:%u",
                               (uint32)XLogRecGetRmid(&item->record), (uint32)XLogRecGetInfo(&item->record),
                               designatedWorker)));
    }

    if (designatedWorker == ALL_WORKER) {
        XLogReaderState *record = &item->record;

        MemoryContext oldCtx = MemoryContextSwitchTo(g_redoWorker->oldCtx);
        /* pageworker  need to execute ? */
        if (XactWillRemoveRelFiles(record)) {
            xactApplyXLogDropRelation(record);
        } else if (IsSmgrTruncate(record)) {
            /* process invalid pages */
            smgrApplyXLogTruncateRelation(record);
        } else {
            /* panic error */
            ereport(PANIC, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                            errmsg("[REDO_LOG_TRACE]ApplyMultiPageShareWithTrxnRecord encounter fatal error:"
                                   "rmgrID:%u, info:%u",
                                   (uint32)XLogRecGetRmid(&item->record), (uint32)XLogRecGetInfo(&item->record))));
        }
        (void)MemoryContextSwitchTo(oldCtx);
    }

    pg_memory_barrier();
    /* update lsn and wait */
    SetCompletedReadEndPtr(g_redoWorker, readLSN, endLSN);

    /* Wait until the record has been applied in trxn thread. */
    uint64 blockcnt = 0;
    pgstat_report_waitevent(WAIT_EVENT_PREDO_APPLY);

    while (pg_atomic_read_u32(&item->replayed) == 0) {
        blockcnt++;
        if ((blockcnt & OUTPUT_WAIT_COUNT) == OUTPUT_WAIT_COUNT) {
            XLogRecPtr LatestReplayedRecPtr = GetXLogReplayRecPtr(NULL);
            ereport(WARNING, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                              errmsg("[REDO_LOG_TRACE]MultiPageShare %u:pagerreplayedLsn:%lu, "
                                     "blockcnt:%lu, Workerid:%u, replayed:%u, LatestReplayedRecPtr:%lu",
                                     designatedWorker, endLSN, blockcnt, g_redoWorker->id, item->replayed,
                                     LatestReplayedRecPtr)));
        }
        RedoInterruptCallBack();
    }

    pgstat_report_waitevent(WAIT_EVENT_END);
    /*
     * If we are the last worker leaving this record, we should destroy
     * the copied record.
     */
    if (pg_atomic_sub_fetch_u32(&item->refCount, 1) == 0) {
        FreeRedoItem(item);
    }

    return;
}

/* Run from the worker thread. */
static void ApplyMultiPageSyncWithTrxnRecord(RedoItem *item)
{
    XLogRecPtr endLSN = item->record.EndRecPtr;
    XLogRecPtr readLSN = item->record.ReadRecPtr;
    XLogRecPtr LatestReplayedRecPtr;

    g_redoWorker->statMulpageCnt++;

    if (item->shareCount == 1) {
        ApplySinglePageRecord(item);
    } else {
        ApplyMultiPageRecord(item);
    }

    pg_memory_barrier();
    SetCompletedReadEndPtr(g_redoWorker, readLSN, endLSN);

    /* block if trxn hasn't update the trnx lsn */
    LatestReplayedRecPtr = GetXLogReplayRecPtr(NULL);

    uint64 blockcnt = 0;
    pgstat_report_waitevent(WAIT_EVENT_PREDO_APPLY);
    while (XLogRecPtrIsInvalid(LatestReplayedRecPtr) || XLByteLT(LatestReplayedRecPtr, endLSN)) {
        /* block if trxn hasn‘t update the last replayed lsn */
        LatestReplayedRecPtr = GetXLogReplayRecPtr(NULL, NULL);
        blockcnt++;
        if ((blockcnt & OUTPUT_WAIT_COUNT) == OUTPUT_WAIT_COUNT) {
            ereport(LOG, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                          errmsg("[REDO_LOG_TRACE]MultiPageSync:pagerreplayedLsn:%lu, blockcnt:%lu, Workerid:%u, "
                                 "LatestReplayedRecPtr:%lu",
                                 endLSN, blockcnt, g_redoWorker->id, LatestReplayedRecPtr)));
        }
        RedoInterruptCallBack();
    }
    pgstat_report_waitevent(WAIT_EVENT_END);
    return;
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
void AddPageRedoItem(PageRedoWorker *worker, RedoItem *item)
{
    /*
     * Based on the performance profiling, calls to semaphores inside
     * the blocking queue took a lot of CPU time.  To reduce this
     * overhead, we add items first to a pending list and then when
     * there are enough pending items we add the whole list to the
     * queue.
     */
    if (worker->pendingHead == NULL) {
        worker->pendingHead = item;
    } else {
        worker->pendingTail->nextByWorker[worker->id + 1] = item;
    }
    item->nextByWorker[worker->id + 1] = NULL;
    worker->pendingTail = item;
}

/* Run from the dispatcher thread. */
bool SendPageRedoEndMark(PageRedoWorker *worker)
{
    return SPSCBlockingQueuePut(worker->queue, &g_redoEndMark);
}

/* Run from the dispatcher thread. */
bool SendPageRedoClearMark(PageRedoWorker *worker)
{
    return SPSCBlockingQueuePut(worker->queue, &g_cleanupMark);
}

bool SendPageRedoClosefdMark(PageRedoWorker *worker)
{
    return SPSCBlockingQueuePut(worker->queue, &g_closefdMark);
}


bool SendPageRedoCleanInvalidPageMark(PageRedoWorker *worker, RepairFileKey key)
{
    errno_t rc = memcpy_s((char*)&g_cleanInvalidPageMark, sizeof(RepairFileKey), (char*)&key, sizeof(RepairFileKey));
    securec_check(rc, "", "");

    return SPSCBlockingQueuePut(worker->queue, &g_cleanInvalidPageMark);
}


bool SendPageRedoWorkerTerminateMark(PageRedoWorker *worker)
{
    return SPSCBlockingQueuePut(worker->queue, &g_terminateMark);
}

/* Run from the dispatcher thread. */
bool ProcessPendingPageRedoItems(PageRedoWorker *worker)
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
void UpdatePageRedoWorkerStandbyState(PageRedoWorker *worker, HotStandbyState newState)
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
    pg_read_barrier();
    return pg_atomic_read_u64(&worker->lastReplayedEndRecPtr);
}

/* automic write for lastReplayedReadRecPtr and lastReplayedEndRecPtr */
void SetCompletedReadEndPtr(PageRedoWorker *worker, XLogRecPtr readPtr, XLogRecPtr endPtr)
{
    volatile PageRedoWorker *tmpWk = worker;
#if defined(__x86_64__) || defined(__aarch64__)
    uint128_u exchange;
    uint128_u current;
    uint128_u compare = atomic_compare_and_swap_u128((uint128_u *)&tmpWk->lastReplayedReadRecPtr);
    Assert(sizeof(tmpWk->lastReplayedEndRecPtr) == 8);
    Assert(sizeof(tmpWk->lastReplayedReadRecPtr) == 8);

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
#endif /* __x86_64__ */
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
#endif /* __x86_64__ */
}

/* Run from the txn worker thread. */
bool IsRecoveryRestartPointSafe(PageRedoWorker *worker, XLogRecPtr restartPoint)
{
    SafeRestartPoint *point = (SafeRestartPoint *)pg_atomic_read_uintptr((uintptr_t *)&worker->safePointHead);

    bool safe = false;
    while (point != NULL) {
        if (XLByteEQ(restartPoint, point->restartPoint)) {
            safe = true;
            break;
        }
        point = point->next;
    }

    return safe;
}

void SetWorkerRestartPoint(PageRedoWorker *worker, XLogRecPtr restartPoint)
{
    pg_atomic_write_u64((uint64 *)&worker->lastCheckedRestartPoint, restartPoint);
}

void *GetBTreeIncompleteActions(PageRedoWorker *worker)
{
    return worker->btreeIncompleteActions;
}

void ClearBTreeIncompleteActions(PageRedoWorker *worker)
{
    worker->btreeIncompleteActions = NULL;
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
    ereport(
        LOG,
        (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
         errmsg("[REDO_LOG_TRACE]RedoWorker common info: id %u, originId %u tid %lu,"
                "lastCheckedRestartPoint %lu, lastReplayedReadRecPtr %lu, lastReplayedEndRecPtr %lu, standbyState %u",
                worker->id, worker->originId, worker->tid.thid, worker->lastCheckedRestartPoint,
                worker->lastReplayedReadRecPtr, worker->lastReplayedEndRecPtr, (uint32)worker->standbyState)));
    DumpQueue(worker->queue);
}

void redo_dump_worker_queue_info()
{
    PageRedoWorker *redoWorker = NULL;

    for (uint32 i = 0; i < g_dispatcher->pageWorkerCount; i++) {
        redoWorker = (g_dispatcher->pageWorkers[i]);
        ereport(LOG,
                (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                 errmsg("[REDO_STATS]redo_dump_all_stats: the redo worker queue statistic during redo are as follows : "
                        "worker info id:%u, originId:%u, tid:%lu, queue_usage:%u, "
                        "queue_max_usage:%u, redo_rec_count:%lu",
                        redoWorker->id, redoWorker->originId, redoWorker->tid.thid,
                        SPSCGetQueueCount(redoWorker->queue), pg_atomic_read_u32(&(redoWorker->queue->maxUsage)),
                        pg_atomic_read_u64(&(redoWorker->queue->totalCnt)))));
    }
}

/* Pending items should be pushed to all workers' queues before invoking this function */
void WaitAllPageWorkersQueueEmpty()
{
    if ((get_real_recovery_parallelism() > 1) && (GetPageWorkerCount() > 0)) {
        for (uint32 i = 0; i < g_dispatcher->pageWorkerCount; i++) {
            while (!SPSCBlockingQueueIsEmpty(g_dispatcher->pageWorkers[i]->queue)) {
                HandlePageRedoInterrupts();
            }
        }
    }
}

void RepairPageAndRecoveryXLog(BadBlockRecEnt *page_info, const char *page)
{
    RedoBufferInfo buffer;
    RedoBufferTag blockinfo;
    errno_t rc;
    RedoItem *item = NULL;

    blockinfo.rnode = page_info->key.relfilenode;
    blockinfo.forknum = page_info->key.forknum;
    blockinfo.blkno = page_info->key.blocknum;
    blockinfo.pblk = page_info->pblk;

    /* read page to buffer pool by RBM_ZERO_AND_LOCK mode and get buffer lock */
    (void)XLogReadBufferForRedoBlockExtend(&blockinfo, RBM_ZERO_AND_LOCK, false, &buffer,
        page_info->rec_max_lsn, InvalidXLogRecPtr, false, WITH_NORMAL_CACHE, false);

    rc = memcpy_s(buffer.pageinfo.page, BLCKSZ, page, BLCKSZ);
    securec_check(rc, "", "");
    MarkBufferDirty(buffer.buf);
    UnlockReleaseBuffer(buffer.buf);

    /* recovery the page xlog */
    item = page_info->head;
    while (item != NULL) {
        RedoItem *next = item->remoteNext;
        ApplySinglePageRecord(item);
        item = next;
    }
}

HTAB* BadBlockHashTblCreate()
{
    HASHCTL ctl;
    errno_t rc;

    rc = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
    securec_check(rc, "", "");

    ctl.keysize = sizeof(RepairBlockKey);
    ctl.entrysize = sizeof(BadBlockRecEnt);
    ctl.hash = tag_hash;

    return hash_create("recovery thread bad block hashtbl", MAX_REMOTE_READ_INFO_NUM, &ctl, HASH_ELEM | HASH_FUNCTION);
}

/* RecordBadBlockAndPushToRemote
 *               If the bad page has been stored, record the xlog. If the bad page
 * has not been stored, need push to page repair thread hash table and record to
 * recovery thread hash table.
 */
void RecordBadBlockAndPushToRemote(XLogReaderState *record, RepairBlockKey key,
    PageErrorType error_type, XLogRecPtr old_lsn, XLogPhyBlock pblk)
{
    bool found = false;
    BadBlockRecEnt *remoteReadInfo = NULL;
    RedoItem *newItem = NULL;
    bool thread_found = false;
    HTAB *bad_hash = NULL;
    gs_thread_t tid = gs_thread_get_cur_thread();

    if (AmPageRedoWorker()) {
        bad_hash = g_redoWorker->badPageHashTbl;
    } else {
        Assert(AmStartupProcess());
        bad_hash = g_instance.startup_cxt.badPageHashTbl;
    }

    found = PushBadPageToRemoteHashTbl(key, error_type, old_lsn, pblk, tid.thid);

    if (found) {
         /* store the record for recovery */
        remoteReadInfo = (BadBlockRecEnt*)hash_search(bad_hash, &(key), HASH_FIND, &thread_found);
        newItem = (RedoItem *)palloc_extended(MAXALIGN(sizeof(RedoItem)) +
                sizeof(RedoItem *) * (GetPageWorkerCount() + 1), MCXT_ALLOC_NO_OOM | MCXT_ALLOC_ZERO);

        InitReaderStateByOld(&newItem->record, record, true);
        CopyDataFromOldReader(&newItem->record, record);
        newItem->need_free = true;

        Assert(thread_found);
        if (!thread_found) {
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("recovery thread bad block hash table corrupted")));
        }
        newItem->remoteNext = NULL;
        remoteReadInfo->tail->remoteNext = newItem;
        remoteReadInfo->tail = newItem;
        remoteReadInfo->rec_max_lsn = newItem->record.EndRecPtr;
    } else {
        remoteReadInfo = (BadBlockRecEnt*)hash_search(bad_hash, &(key), HASH_ENTER, &thread_found);
        Assert(!thread_found);
        newItem = (RedoItem *)palloc_extended(MAXALIGN(sizeof(RedoItem)) +
                sizeof(RedoItem *) * (GetPageWorkerCount() + 1), MCXT_ALLOC_NO_OOM | MCXT_ALLOC_ZERO);

        InitReaderStateByOld(&newItem->record, record, true);
        CopyDataFromOldReader(&newItem->record, record);
        newItem->need_free = true;

        remoteReadInfo->key.relfilenode = key.relfilenode;
        remoteReadInfo->key.forknum = key.forknum;
        remoteReadInfo->key.blocknum = key.blocknum;
        remoteReadInfo->pblk = pblk;
        remoteReadInfo->rec_min_lsn = newItem->record.EndRecPtr;
        remoteReadInfo->rec_max_lsn = newItem->record.EndRecPtr;
        newItem->remoteNext = NULL;
        remoteReadInfo->head = newItem;
        remoteReadInfo->tail = newItem;

        if (AmPageRedoWorker()) {
            g_redoWorker->remoteReadPageNum++;
            if (g_redoWorker->remoteReadPageNum >= MAX_REMOTE_READ_INFO_NUM) {
                ereport(WARNING, (errmsg("recovery thread found %d error block.", g_redoWorker->remoteReadPageNum)));
            }
        } else {
            Assert(AmStartupProcess());
            g_instance.startup_cxt.remoteReadPageNum++;
            if (g_instance.startup_cxt.remoteReadPageNum >= MAX_REMOTE_READ_INFO_NUM) {
                ereport(WARNING, (errmsg("startup thread found %d error block.",
                    g_instance.startup_cxt.remoteReadPageNum)));
            }
        }

        return;
    }

    return;
}

/* ClearPageRepairHashTbl
 *         drop table, or truncate table, need clear the page repair hashTbl, if the
 * repair page Filenode match, need remove.
 */
void ClearRecoveryThreadHashTbl(const RelFileNode &node, ForkNumber forknum, BlockNumber minblkno,
    bool segment_shrink)
{
    HTAB *bad_hash = NULL;
    bool found = false;
    BadBlockRecEnt *entry = NULL;
    HASH_SEQ_STATUS status;

    if (AmPageRedoWorker()) {
        bad_hash = g_redoWorker->badPageHashTbl;
    } else {
        Assert(AmStartupProcess());
        bad_hash = g_instance.startup_cxt.badPageHashTbl;
    }

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
    HTAB *bad_hash = NULL;
    bool found = false;
    BadBlockRecEnt *entry = NULL;
    HASH_SEQ_STATUS status;

    if (AmPageRedoWorker()) {
        bad_hash = g_redoWorker->badPageHashTbl;
    } else {
        Assert(AmStartupProcess());
        bad_hash = g_instance.startup_cxt.badPageHashTbl;
    }

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
    HTAB *bad_hash = NULL;
    bool found = false;

    if (AmPageRedoWorker()) {
        bad_hash = g_redoWorker->badPageHashTbl;
    } else {
        Assert(AmStartupProcess());
        bad_hash = g_instance.startup_cxt.badPageHashTbl;
    }

    if ((BadBlockRecEnt*)hash_search(bad_hash, &(entry->key), HASH_REMOVE, &found) == NULL) {
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("recovery thread bad block hash table corrupted")));
    }
}

/* CheckRemoteReadAndRepairPage
 *           check the page state of page repair hashtbl, if can repair,
 * repair the page and recovery xlog, clean the page info.
 */
void CheckRemoteReadAndRepairPage(BadBlockRecEnt *entry)
{
    RepairBlockKey key;
    XLogRecPtr rec_min_lsn = InvalidXLogRecPtr;
    XLogRecPtr rec_max_lsn = InvalidXLogRecPtr;
    bool check = false;
    char *page = NULL;

    key = entry->key;
    rec_min_lsn = entry->rec_min_lsn;
    rec_max_lsn = entry->rec_max_lsn;

    if (AmPageRedoWorker()) {
        page = g_redoWorker->page;
    } else {
        Assert(AmStartupProcess());
        page = g_instance.startup_cxt.page;
    }

    check = CheckRepairPage(key, rec_min_lsn, rec_max_lsn, page);
    if (check) {
        /* copy page to buffer pool, and recovery the stored xlog */
        RepairPageAndRecoveryXLog(entry, page);
        /* clear this thread invalid page hash table */
        forget_specified_invalid_pages(key);
        /* clear thread bad block hash entry */
        ClearSpecificsPageEntryAndMem(entry);
        /* clear page repair thread hash table */
        ClearSpecificsPageRepairHashTbl(key);
        g_redoWorker->remoteReadPageNum--;
    }

    return;
}


void SeqCheckRemoteReadAndRepairPage()
{
    BadBlockRecEnt *entry = NULL;
    HASH_SEQ_STATUS status;
    HTAB *bad_hash = NULL;

    if (AmPageRedoWorker()) {
        bad_hash = g_redoWorker->badPageHashTbl;
    } else {
        Assert(AmStartupProcess());
        bad_hash = g_instance.startup_cxt.badPageHashTbl;
    }

    hash_seq_init(&status, bad_hash);
    while ((entry = (BadBlockRecEnt *)hash_seq_search(&status)) != NULL) {
        CheckRemoteReadAndRepairPage(entry);
    }
}

}  // namespace parallel_recovery
