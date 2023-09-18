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
        if (XLogRecGetRmid(record) == RM_XACT_ID)
            SetLatestXTime(recordXTime);
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

void ApplyRedoRecord(XLogReaderState *record)
{
    ErrorContextCallback errContext;
    errContext.callback = rm_redo_error_callback;
    errContext.arg = (void *)record;
    errContext.previous = t_thrd.log_cxt.error_context_stack;
    t_thrd.log_cxt.error_context_stack = &errContext;
    if (module_logging_is_on(MOD_REDO)) {
        DiagLogRedoRecord(record, "ApplyRedoRecord");
    }
    RmgrTable[XLogRecGetRmid(record)].rm_redo(record);

    t_thrd.log_cxt.error_context_stack = errContext.previous;
}

