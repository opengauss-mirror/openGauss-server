/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * multi_redo_api.cpp
 *      Defines GUC options for parallel recovery.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/transam/multi_redo_api.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <stdio.h>
#include <unistd.h>

#include "postgres.h"
#include "knl/knl_variable.h"
#include "utils/guc.h"

#include "access/multi_redo_settings.h"
#include "access/multi_redo_api.h"
#include "access/parallel_recovery/dispatcher.h"
#include "access/parallel_recovery/page_redo.h"
#include "access/xlog_internal.h"

bool g_supportHotStandby = true;   /* don't support consistency view */
uint32 g_startupTriggerState = TRIGGER_NORMAL;

void StartUpMultiRedo(XLogReaderState *xlogreader, uint32 privateLen)
{
    if (IsExtremeRedo()) {
        ExtremeStartRecoveryWorkers(xlogreader, privateLen);
    } else if (IsParallelRedo()) {
        parallel_recovery::StartRecoveryWorkers(xlogreader->ReadRecPtr);
    }
}

bool IsMultiThreadRedoRunning()
{
    return ((get_real_recovery_parallelism() > 1 && parallel_recovery::g_dispatcher != 0) ||
            IsExtremeMultiThreadRedoRunning());
}

void DispatchRedoRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime)
{
    if (IsExtremeRedo()) {
        ExtremeDispatchRedoRecordToFile(record, expectedTLIs, recordXTime);
    } else if (IsParallelRedo()) {
        parallel_recovery::DispatchRedoRecordToFile(record, expectedTLIs, recordXTime);
    } else {
        g_instance.startup_cxt.current_record = record;
        uint32 term = XLogRecGetTerm(record);
        if (term > g_instance.comm_cxt.localinfo_cxt.term_from_xlog) {
            g_instance.comm_cxt.localinfo_cxt.term_from_xlog = term;
        }

        long readbufcountbefore = u_sess->instr_cxt.pg_buffer_usage->shared_blks_read;
        ApplyRedoRecord(record);
        record->readblocks = u_sess->instr_cxt.pg_buffer_usage->shared_blks_read - readbufcountbefore;
        CountXLogNumbers(record);
        SetXLogReplayRecPtr(record->ReadRecPtr, record->EndRecPtr);
        CheckRecoveryConsistency();
    }
}

void GetThreadNameIfMultiRedo(int argc, char *argv[], char **threadNamePtr)
{
    if (IsExtremeRedo()) {
        ExtremeGetThreadNameIfPageRedoWorker(argc, argv, threadNamePtr);
    } else if (IsParallelRedo()) {
        parallel_recovery::GetThreadNameIfPageRedoWorker(argc, argv, threadNamePtr);
    }
}

PGPROC *MultiRedoThreadPidGetProc(ThreadId pid)
{
    if (IsExtremeRedo()) {
        return ExtremeStartupPidGetProc(pid);
    } else {
        return parallel_recovery::StartupPidGetProc(pid);
    }
}

void MultiRedoUpdateStandbyState(HotStandbyState newState)
{
    if (IsExtremeRedo()) {
        ExtremeUpdateStandbyState(newState);
    } else if (IsParallelRedo()) {
        parallel_recovery::UpdateStandbyState(newState);
    }
}

void MultiRedoUpdateMinRecovery(XLogRecPtr newMinRecoveryPoint)
{
    if (IsExtremeRedo()) {
        ExtremeUpdateMinRecoveryForTrxnRedoThd(newMinRecoveryPoint);
    }
}

uint32 MultiRedoGetWorkerId()
{
    if (IsExtremeRedo()) {
        return ExtremeGetMyPageRedoWorkerIdWithLock();
    } else if (IsParallelRedo()) {
        return parallel_recovery::GetMyPageRedoWorkerOrignId();
    } else {
        ereport(ERROR, (errmsg("MultiRedoGetWorkerId parallel redo and extreme redo is close, should not be here!")));
    }
    return 0;
}

bool IsAllPageWorkerExit()
{
    if (get_real_recovery_parallelism() > 1) {
        for (uint32 i = 0; i < g_instance.comm_cxt.predo_cxt.totalNum; ++i) {
            uint32 state = pg_atomic_read_u32(&(g_instance.comm_cxt.predo_cxt.pageRedoThreadStatusList[i].threadState));
            if (state != PAGE_REDO_WORKER_INVALID) {
                return false;
            }
        }
        g_instance.comm_cxt.predo_cxt.totalNum = 0;
    }

    if (g_instance.pid_cxt.exrto_recycler_pid != 0) {
        return false;
    }
    ereport(LOG,
            (errmodule(MOD_REDO), errcode(ERRCODE_LOG), errmsg("page workers all exit or not open parallel redo")));

    return true;
}

void SetPageRedoWorkerIndex(int index)
{
    if (IsExtremeRedo()) {
        ExtremeSetPageRedoWorkerIndex(index);
    } else if (IsParallelRedo()) {
        parallel_recovery::g_redoWorker->index = index;
    }
}

int GetPageRedoWorkerIndex(int index)
{
    if (IsExtremeRedo()) {
        return ExtremeGetPageRedoWorkerIndex();
    } else if (IsParallelRedo()) {
        return parallel_recovery::g_redoWorker->index;
    } else {
        return 0;
    }
}

PageRedoExitStatus CheckExitPageWorkers(ThreadId pid)
{
    PageRedoExitStatus checkStatus = NOT_PAGE_REDO_THREAD;

    for (uint32 i = 0; i < g_instance.comm_cxt.predo_cxt.totalNum; ++i) {
        if (g_instance.comm_cxt.predo_cxt.pageRedoThreadStatusList[i].threadId == pid) {
            checkStatus = PAGE_REDO_THREAD_EXIT_NORMAL;
            uint32 state = pg_atomic_read_u32(&(g_instance.comm_cxt.predo_cxt.pageRedoThreadStatusList[i].threadState));
            ereport(LOG, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                          errmsg("page worker thread %lu exit, state %u", pid, state)));
            if (state == PAGE_REDO_WORKER_READY) {
                checkStatus = PAGE_REDO_THREAD_EXIT_ABNORMAL;
            }
            pg_atomic_write_u32(&(g_instance.comm_cxt.predo_cxt.pageRedoThreadStatusList[i].threadState),
                                PAGE_REDO_WORKER_INVALID);
            g_instance.comm_cxt.predo_cxt.pageRedoThreadStatusList[i].threadId = 0;
            break;
        }
    }

    return checkStatus;
}

void ProcTxnWorkLoad(bool force)
{
    if (IsParallelRedo()) {
        parallel_recovery::ProcessTrxnRecords(force);
    }
}

/* Run from the worker thread. */
void SetMyPageRedoWorker(knl_thread_arg *arg)
{
    if (IsExtremeRedo()) {
        ExtremeSetMyPageRedoWorker(arg);
    } else if (IsParallelRedo()) {
        parallel_recovery::g_redoWorker = (parallel_recovery::PageRedoWorker *)arg->payload;
    }
}

/* Run from the worker thread. */
uint32 GetMyPageRedoWorkerId()
{
    if (IsExtremeRedo()) {
        return ExtremeGetMyPageRedoWorkerId();
    } else if (IsParallelRedo()) {
        return parallel_recovery::g_redoWorker->id;
    } else {
        return 0;
    }
}

void MultiRedoMain()
{
    pgstat_report_appname("PageRedo");
    pgstat_report_activity(STATE_IDLE, NULL);
    if (IsExtremeRedo()) {
        ExtremeParallelRedoThreadMain();
    } else if (IsParallelRedo()) {
        parallel_recovery::PageRedoWorkerMain();
    } else {
        ereport(ERROR, (errmsg("MultiRedoMain parallel redo and extreme redo is close, should not be here!")));
    }
}

static void recoveryWakeupDelayLatchOp(RecoveryDelayLatchOperation op, int wakeEvents, long waitTime)
{
    switch (op) {
        case LATCH_INIT: {
            InitSharedLatch(&t_thrd.shemem_ptr_cxt.XLogCtl->recoveryWakeupDelayLatch);
            break;
        }
        case LATCH_SET: {
            SetLatch(&t_thrd.shemem_ptr_cxt.XLogCtl->recoveryWakeupDelayLatch);
            break;
        }
        case LATCH_RESET: {
            ResetLatch(&t_thrd.shemem_ptr_cxt.XLogCtl->recoveryWakeupDelayLatch);
            break;
        }
        case LATCH_OWN: {
            OwnLatch(&t_thrd.shemem_ptr_cxt.XLogCtl->recoveryWakeupDelayLatch);
            break;
        }
        case LATCH_DISOWN: {
            DisownLatch(&t_thrd.shemem_ptr_cxt.XLogCtl->recoveryWakeupDelayLatch);
            break;
        }
        case LATCH_WAIT: {
            WaitLatch(&t_thrd.shemem_ptr_cxt.XLogCtl->recoveryWakeupDelayLatch, wakeEvents, waitTime);
            break;
        }
        default:
            break;
    }
}

void RecoveryDelayLatchOp(RecoveryDelayLatchOperation op, int wakeEvents, long waitTime)
{
    if (IsExtremeRedo()) {
        ExtremeRedoDelayLatchOp(op, wakeEvents, waitTime);
    } else {
        recoveryWakeupDelayLatchOp(op, wakeEvents, waitTime);
    }
}

static TimestampTz* GetDelayUntilTime(void)
{
    if (IsExtremeRedo()) {
        return ExtremeRedoGetlayUntilTime();
    } else {
        return &(u_sess->attr.attr_storage.recoveryDelayUntilTime);
    }
}

/*
 * When recovery_min_apply_delay is set, we wait long enough to make sure
 * certain record types are applied at least that interval behind the master.
 *
 * Returns true if we waited.
 *
 * Note that the delay is calculated between the WAL record log time and
 * the current time on standby. We would prefer to keep track of when this
 * standby received each WAL record, which would allow a more consistent
 * approach and one not affected by time synchronisation issues, but that
 * is significantly more effort and complexity for little actual gain in
 * usability.
 */
static bool RecoveryApplyDelay(const XLogReaderState* record, TimestampTz& xtime)
{
    uint8 info;
    long secs;
    int microsecs;
    long waitTime = 0;
    TimestampTz* recoveryDelayUntilTime = NULL;

    /* nothing to do if no delay configured or nothing to do if crash recovery is requested */
    if (!t_thrd.xlog_cxt.InRecovery || (u_sess->attr.attr_storage.recovery_min_apply_delay <= 0) ||
        !t_thrd.xlog_cxt.ArchiveRecoveryRequested || !t_thrd.xlog_cxt.reachedConsistency ||
        XLogRecGetRmid(record) != RM_XACT_ID) {
        return false;
    }

    KeepWalrecvAliveWhenRecoveryDelay();

    info = XLogRecGetInfo(record) & (~XLR_INFO_MASK);
    if (info == XLOG_XACT_COMMIT_COMPACT) {
        xl_xact_commit_compact* xlrec = (xl_xact_commit_compact*)XLogRecGetData(record);
        xtime = xlrec->xact_time;
    } else if (info == XLOG_XACT_COMMIT) {
        xl_xact_commit* xlrec = (xl_xact_commit*)XLogRecGetData(record);
        xtime = xlrec->xact_time;
    } else if (info == XLOG_XACT_ABORT || info == XLOG_XACT_ABORT_WITH_XID) {
        xl_xact_abort* xlrec = (xl_xact_abort*)XLogRecGetData(record);
        xtime = xlrec->xact_time;
    } else {
        return false;
    }

    recoveryDelayUntilTime = GetDelayUntilTime();
    Assert(recoveryDelayUntilTime != NULL);
    *recoveryDelayUntilTime = TimestampTzPlusMilliseconds(xtime, u_sess->attr.attr_storage.recovery_min_apply_delay);

    /* Exit without arming the latch if it's already past time to apply this record */
    TimestampDifference(GetCurrentTimestamp(), *recoveryDelayUntilTime, &secs, &microsecs);
    if (secs <= 0 && microsecs <= 0) {
        return false;
    }

    while (true) {
        RecoveryDelayLatchOp(LATCH_RESET);

        /* might change the trigger file's location */
        RedoInterruptCallBack();
        KeepWalrecvAliveWhenRecoveryDelay();
        if (CheckForFailoverTrigger() || CheckForSwitchoverTrigger() || CheckForStandbyTrigger()) {
            break;
        }

        /* recovery_min_apply_delay maybe change, so we need recalculate recoveryDelayUntilTime */
        *recoveryDelayUntilTime =
            TimestampTzPlusMilliseconds(xtime, u_sess->attr.attr_storage.recovery_min_apply_delay);

        /* Wait for difference between GetCurrentTimestamp() and recoveryDelayUntilTime */
        TimestampDifference(GetCurrentTimestamp(), *recoveryDelayUntilTime, &secs, &microsecs);

        /* NB: We're ignoring waits below min_apply_delay's resolution. */
        if (secs <= 0 && microsecs / USECS_PER_MSEC <= 0) {
            break;
        }

        /* delay 1s every time */
        waitTime = (secs >= 1) ? MSECS_PER_SEC : (microsecs / USECS_PER_MSEC);
        RecoveryDelayLatchOp(LATCH_WAIT, WL_LATCH_SET | WL_TIMEOUT, waitTime);
    }
    return true;
}

void EndDispatcherContext()
{
    if (IsExtremeRedo()) {
        ExtremeEndDispatcherContext();

    } else if (IsParallelRedo()) {
        (void)MemoryContextSwitchTo(parallel_recovery::g_dispatcher->oldCtx);
    }
}

void SwitchToDispatcherContext()
{
    (void)MemoryContextSwitchTo(g_instance.comm_cxt.predo_cxt.parallelRedoCtx);
}

void FreeAllocatedRedoItem()
{
    if (IsExtremeRedo()) {
        ExtremeFreeAllocatedRedoItem();

    } else if (IsParallelRedo()) {
        parallel_recovery::FreeAllocatedRedoItem();
    }
}

uint32 GetRedoWorkerCount()
{
    if (IsExtremeRedo()) {
        return ExtremeGetAllWorkerCount();

    } else if (IsParallelRedo()) {
        return parallel_recovery::GetPageWorkerCount();
    }

    return 0;
}

void **GetXLogInvalidPagesFromWorkers()
{
    if (IsExtremeRedo()) {
        return ExtremeGetXLogInvalidPagesFromWorkers();

    } else if (IsParallelRedo()) {
        return parallel_recovery::GetXLogInvalidPagesFromWorkers();
    }

    return NULL;
}

void SendRecoveryEndMarkToWorkersAndWaitForFinish(int code)
{
    if (IsExtremeRedo()) {
        return ExtremeSendRecoveryEndMarkToWorkersAndWaitForFinish(code);

    } else if (IsParallelRedo()) {
        return parallel_recovery::SendRecoveryEndMarkToWorkersAndWaitForFinish(code);
    }
}

RedoWaitInfo GetRedoIoEvent(int32 event_id)
{
    if (IsExtremeRedo()) {
        return ExtremeRedoGetIoEvent(event_id);
    } else {
        return parallel_recovery::redo_get_io_event(event_id);
    }
}

void GetRedoWorkerStatistic(uint32 *realNum, RedoWorkerStatsData *worker, uint32 workerLen)
{
    if (IsExtremeRedo()) {
        return ExtremeRedoGetWorkerStatistic(realNum, worker, workerLen);
    } else {
        return parallel_recovery::redo_get_worker_statistic(realNum, worker, workerLen);
    }
}

void GetRedoWorkerTimeCount(RedoWorkerTimeCountsInfo **workerCountInfoList, uint32 *realNum)
{
    if (IsExtremeRedo()) {
        ExtremeRedoGetWorkerTimeCount(workerCountInfoList, realNum);
    } else if (IsParallelRedo()) {
        parallel_recovery::redo_get_worker_time_count(workerCountInfoList, realNum);
    } else {
        *realNum = 0;
    }
}

void CountXLogNumbers(XLogReaderState *record)
{
    const uint32 type_shift = 4;
    RmgrId rm_id = XLogRecGetRmid(record);
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    if (rm_id == RM_HEAP_ID || rm_id == RM_HEAP2_ID || rm_id == RM_HEAP3_ID) {
        info = info & XLOG_HEAP_OPMASK;
    } else if (rm_id == RM_UHEAP_ID || rm_id == RM_UHEAP2_ID) {
        info = info & XLOG_UHEAP_OPMASK;
    }

    info = (info >> type_shift);
    Assert(info < MAX_XLOG_INFO_NUM);
    Assert(rm_id < RM_NEXT_ID);
    (void)pg_atomic_add_fetch_u64(&g_instance.comm_cxt.predo_cxt.xlogStatics[rm_id][info].total_num, 1);

    if (record->max_block_id >= 0) {
        (void)pg_atomic_add_fetch_u64(&g_instance.comm_cxt.predo_cxt.xlogStatics[rm_id][info].extra_num,
                                      record->readblocks);
    } else if (rm_id == RM_XACT_ID) {
        ColFileNode *xnodes = NULL;
        int nrels = 0;
        XactGetRelFiles(record, &xnodes, &nrels);
        if (nrels > 0) {
            (void)pg_atomic_add_fetch_u64(&g_instance.comm_cxt.predo_cxt.xlogStatics[rm_id][info].extra_num, nrels);
        }
    }
}

void ResetXLogStatics()
{
    errno_t rc = memset_s((void *)g_instance.comm_cxt.predo_cxt.xlogStatics,
        sizeof(g_instance.comm_cxt.predo_cxt.xlogStatics), 0, sizeof(g_instance.comm_cxt.predo_cxt.xlogStatics));
    securec_check(rc, "\0", "\0");
}

void DiagLogRedoRecord(XLogReaderState *record, const char *funcName)
{
    uint8 info;
    RelFileNode oldRn = { 0 };
    RelFileNode newRn = { 0 };
    BlockNumber oldblk = InvalidBlockNumber;
    BlockNumber newblk = InvalidBlockNumber;
    bool newBlkExistFlg = false;
    bool oldBlkExistFlg = false;
    ForkNumber oldFk = InvalidForkNumber;
    ForkNumber newFk = InvalidForkNumber;
    StringInfoData buf;

    /* Support redo old version xlog during upgrade (Just the runningxact log with chekpoint online ) */
    uint32 rmid = XLogRecGetRmid(record);
    info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    initStringInfo(&buf);
    RmgrTable[rmid].rm_desc(&buf, record);

    if (XLogRecGetBlockTag(record, 0, &newRn, &newFk, &newblk)) {
        newBlkExistFlg = true;
    }
    if (XLogRecGetBlockTag(record, 1, &oldRn, &oldFk, &oldblk)) {
        oldBlkExistFlg = true;
    }
    ereport(DEBUG4, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
        errmsg("[REDO_LOG_TRACE]DiagLogRedoRecord: %s, ReadRecPtr:%lu,EndRecPtr:%lu,"
            "newBlkExistFlg:%d,"
            "newRn(spcNode:%u, dbNode:%u, relNode:%u),newFk:%d,newblk:%u,"
            "oldBlkExistFlg:%d,"
            "oldRn(spcNode:%u, dbNode:%u, relNode:%u),oldFk:%d,oldblk:%u,"
            "info:%u, rm_name:%s, desc:%s,"
            "max_block_id:%d",
            funcName, record->ReadRecPtr, record->EndRecPtr, newBlkExistFlg, newRn.spcNode, newRn.dbNode, newRn.relNode,
            newFk, newblk, oldBlkExistFlg, oldRn.spcNode, oldRn.dbNode, oldRn.relNode, oldFk, oldblk, (uint32)info,
            RmgrTable[rmid].rm_name, buf.data, record->max_block_id)));
    pfree_ext(buf.data);
}

void ApplyRedoRecord(XLogReaderState* record)
{
    TimestampTz xtime = 0;
    ErrorContextCallback errContext;
    bool ret = false;

    if (ENABLE_DMS && !SS_PERFORMING_SWITCHOVER && XLogRecGetRmid(record) == RM_XACT_ID) {
        ret = SSRecoveryApplyDelay();
    } else {
        ret = RecoveryApplyDelay(record, xtime);
    }
    if (ret) {
        volatile XLogCtlData* xlogctl = t_thrd.shemem_ptr_cxt.XLogCtl;
        if (xlogctl->recoveryPause) {
            RecoveryPausesHere();
        }
    }

    errContext.callback = rm_redo_error_callback;
    errContext.arg = (void *)record;
    errContext.previous = t_thrd.log_cxt.error_context_stack;
    t_thrd.log_cxt.error_context_stack = &errContext;
    if (module_logging_is_on(MOD_REDO)) {
        DiagLogRedoRecord(record, "ApplyRedoRecord");
    }
    RmgrTable[XLogRecGetRmid(record)].rm_redo(record);

    /*
     * If xtime is still 0 and this is a transaction record, extract the
     * transaction timestamp. This ensures pg_last_xact_replay_timestamp()
     * returns a value even when recovery_min_apply_delay is not configured.
     */
    if (xtime == 0 && XLogRecGetRmid(record) == RM_XACT_ID) {
        uint8 info = XLogRecGetInfo(record) & (~XLR_INFO_MASK);
        char* recordData = XLogRecGetData(record);

        if (recordData != NULL) {
            if (info == XLOG_XACT_COMMIT_COMPACT) {
                xl_xact_commit_compact* xlrec = (xl_xact_commit_compact*)recordData;
                xtime = xlrec->xact_time;
            } else if (info == XLOG_XACT_COMMIT) {
                xl_xact_commit* xlrec = (xl_xact_commit*)recordData;
                xtime = xlrec->xact_time;
            } else if (info == XLOG_XACT_ABORT || info == XLOG_XACT_ABORT_WITH_XID) {
                xl_xact_abort* xlrec = (xl_xact_abort*)recordData;
                xtime = xlrec->xact_time;
            }
        }
    }

    if (xtime != 0) {
        SetLatestXTime(xtime);
    }
    t_thrd.log_cxt.error_context_stack = errContext.previous;
}
