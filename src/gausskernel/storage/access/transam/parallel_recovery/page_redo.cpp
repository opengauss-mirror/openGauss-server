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
#include "catalog/storage_xlog.h"
#include "gssignal/gs_signal.h"
#include "libpq/pqsignal.h"
#include "postmaster/postmaster.h"
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
#include "pgstat.h"
#include "access/multi_redo_api.h"

namespace parallel_recovery {

static const char *const PROCESS_TYPE_CMD_ARG = "--forkpageredo";
static char g_AUXILIARY_TYPE_CMD_ARG[16] = {0};
static const uint32 REDO_WORKER_ALIGN_LEN = 16; /* need 128-bit aligned */

THR_LOCAL PageRedoWorker *g_redoWorker = NULL;

static RedoItem g_redoEndMark = {};
static RedoItem g_terminateMark = {};
static RedoItem g_cleanupMark = { false, false, false, false, 0 };

static const int PAGE_REDO_WORKER_ARG = 3;
static const int REDO_SLEEP_50US = 50;
static const int REDO_SLEEP_100US = 100;

static void AddSafeRecoveryRestartPoint(XLogRecPtr restartPoint);
static void ApplyAndFreeRedoItem(RedoItem *);
static void ApplyMultiPageRecord(RedoItem *);
static int ApplyRedoLoop();
static void ApplySinglePageRecord(RedoItem *);
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
    worker = (PageRedoWorker *)TYPEALIGN(REDO_WORKER_ALIGN_LEN, tmp);
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
    uint32 isSkipItem;
    while (head != NULL) {
        RedoItem *cur = head;
        head = head->nextByWorker[g_redoWorker->id + 1];
        isSkipItem = pg_atomic_read_u32(&g_redoWorker->skipItemFlg);
        if (isSkipItem == PAGE_REDO_WORKER_APPLY_ITEM) {
            ApplyAndFreeRedoItem(cur);
        } else {
            OnlyFreeRedoItem(cur);
        }
    }
}

/* Run from the worker thread. */
static int ApplyRedoLoop()
{
    RedoItem *head;
    instr_time startTime;
    instr_time endTime;

    INSTR_TIME_SET_CURRENT(startTime);

    while ((head = (RedoItem *)SPSCBlockingQueueTop(g_redoWorker->queue)) != &g_redoEndMark) {
        if (head == &g_cleanupMark) {
            g_redoWorker->btreeIncompleteActions = btree_get_incomplete_actions();
            g_redoWorker->xlogInvalidPages = XLogGetInvalidPages();
        } else {
            ApplyProcHead(head);
        }
        SPSCBlockingQueuePop(g_redoWorker->queue);
        RedoInterruptCallBack();
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
    UpdateRecordGlobals(item, g_redoWorker->standbyState);

    /*
     * We need to save the LSN here because the following apply will
     * free the item.
     */
    XLogRecPtr endLSN = item->record.EndRecPtr;
    XLogRecPtr readLSN = item->record.ReadRecPtr;

    if (IsLSNMarker(item)) {
        FreeRedoItem(item);
        SetCompletedReadEndPtr(g_redoWorker, readLSN, endLSN);
    } else if (item->sharewithtrxn)
        ApplyMultiPageShareWithTrxnRecord(item);
    else if (item->blockbytrxn)
        ApplyMultiPageSyncWithTrxnRecord(item);
    else if (item->shareCount == 1) {
        ApplySinglePageRecord(item);
        SetCompletedReadEndPtr(g_redoWorker, readLSN, endLSN);
    } else {
        if (item->designatedWorker == ALL_WORKER) {
            ApplyMultiPageAllWorkerRecord(item);
        } else {
            ApplyMultiPageRecord(item);
            SetCompletedReadEndPtr(g_redoWorker, readLSN, endLSN);
        }
    }
}

/* Run from the worker thread. */
void OnlyFreeRedoItem(RedoItem *item)
{
    if (pg_atomic_read_u32(&item->freed) == 0) {
        FreeRedoItem(item);
    }
}

/* Run from the worker thread. */
static void ApplySinglePageRecord(RedoItem *item)
{
    XLogReaderState *record = &item->record;
    bool bOld = item->oldVersion;

    MemoryContext oldCtx = MemoryContextSwitchTo(g_redoWorker->oldCtx);
    ApplyRedoRecord(record, bOld);
    (void)MemoryContextSwitchTo(oldCtx);

    FreeRedoItem(item);
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

static void ApplyMultiPageRecord(RedoItem *item)
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
    bool doApply = (designatedWorker == GetMyPageRedoWorkerId() || (designatedWorker == ANY_WORKER && isLast));
    uint64 blockcnt = 0;

    if (doApply) {
        pgstat_report_waitevent(WAIT_EVENT_PREDO_APPLY);
        /* Wait until every worker has reached this record. */
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
        ApplyRedoRecord(&item->record, item->oldVersion);
        (void)MemoryContextSwitchTo(oldCtx);

        pg_memory_barrier();

        pg_atomic_write_u32(&item->replayed, 1);
    } else {
        pgstat_report_waitevent(WAIT_EVENT_PREDO_APPLY);
        /* Wait until the record has been applied. */
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

    /*
     * If we are the last worker leaving this record, we should destroy
     * the copied record.
     */
    if (pg_atomic_sub_fetch_u32(&item->refCount, 1) == 0) {
        FreeRedoItem(item);
    }
}

static void ApplyMultiPageAllWorkerRecord(RedoItem *item)
{
    g_redoWorker->statMulpageCnt++;
    XLogReaderState *record = &item->record;
    bool bOld = item->oldVersion;
    XLogRecPtr endLSN = item->record.EndRecPtr;
    XLogRecPtr readLSN = item->record.ReadRecPtr;
    (void)pg_atomic_add_fetch_u32(&item->refCount, 1);
    errno_t rc;

    if (IsCheckPoint(record)) {
        CheckPoint checkPoint;
        if (bOld) {
            checkPoint = update_checkpoint(record);
        } else {
            rc = memcpy_s(&checkPoint, sizeof(checkPoint), XLogRecGetData(record), sizeof(checkPoint));
            securec_check(rc, "\0", "\0");
        }

        if (HasTimelineUpdate(record, bOld)) {
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

}  // namespace parallel_recovery
