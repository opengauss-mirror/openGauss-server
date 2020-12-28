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
#include "access/extreme_rto/dispatcher.h"
#include "access/parallel_recovery/dispatcher.h"
#include "access/extreme_rto/page_redo.h"
#include "access/parallel_recovery/page_redo.h"


#ifdef ENABLE_MULTIPLE_NODES
bool g_supportHotStandby = false;   /* don't support consistency view */
#else
bool g_supportHotStandby = true;   /* don't support consistency view */
#endif


void StartUpMultiRedo(XLogReaderState *xlogreader, uint32 privateLen)
{
    if (IsExtremeRedo()) {
        extreme_rto::StartRecoveryWorkers(xlogreader, privateLen);
    } else if (IsParallelRedo()) {
        parallel_recovery::StartRecoveryWorkers();
    }
}

bool IsMultiThreadRedoRunning()
{
    return (get_real_recovery_parallelism() > 1 &&
            (extreme_rto::g_dispatcher != 0 || parallel_recovery::g_dispatcher != 0));
}

bool IsExtremeRtoRunning()
{
    return (get_real_recovery_parallelism() > 1 && extreme_rto::g_dispatcher != 0 &&
            extreme_rto::g_dispatcher->pageLineNum > 0);
}


bool IsExtremeRtoSmartShutdown()
{
    if (!IsExtremeRtoRunning()) {
        return false;
    }

    if (extreme_rto::g_dispatcher->smartShutdown) {
        extreme_rto::g_dispatcher->smartShutdown =false;
        return true;
    }
    return false;
}

void ExtremeRtoRedoManagerSendEndToStartup()
{
    if (!IsExtremeRtoRunning()) {
        return;
    }

    extreme_rto::g_redoEndMark.record.isDecode = true;
    extreme_rto::PutRecordToReadQueue((XLogReaderState *)&extreme_rto::g_redoEndMark.record);
}

bool IsExtremeRtoReadWorkerRunning()
{
    if (!IsExtremeRtoRunning()) {
        return false;
    }

    uint32 readWorkerState = pg_atomic_read_u32(&extreme_rto::g_dispatcher->rtoXlogBufState.readWorkerState);
    if (readWorkerState == extreme_rto::WORKER_STATE_STOP || readWorkerState == extreme_rto::WORKER_STATE_EXIT) {
        return false;
    }

    return true;
}

void DispatchRedoRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime)
{
    if (IsExtremeRedo()) {
        extreme_rto::DispatchRedoRecordToFile(record, expectedTLIs, recordXTime);
    } else if (IsParallelRedo()) {
        parallel_recovery::DispatchRedoRecordToFile(record, expectedTLIs, recordXTime);
    } else {
        parallel_recovery::ApplyRedoRecord(record, t_thrd.xlog_cxt.redo_oldversion_xlog);
        if (XLogRecGetRmid(record) == RM_XACT_ID)
            SetLatestXTime(recordXTime);
        SetXLogReplayRecPtr(record->ReadRecPtr, record->EndRecPtr);
        CheckRecoveryConsistency();
    }
}

void GetThreadNameIfMultiRedo(int argc, char *argv[], char **threadNamePtr)
{
    if (IsExtremeRedo()) {
        extreme_rto::GetThreadNameIfPageRedoWorker(argc, argv, threadNamePtr);
    } else if (IsParallelRedo()) {
        parallel_recovery::GetThreadNameIfPageRedoWorker(argc, argv, threadNamePtr);
    }
}

PGPROC *MultiRedoThreadPidGetProc(ThreadId pid)
{
    if (IsExtremeRedo()) {
        return extreme_rto::StartupPidGetProc(pid);
    } else {
        return parallel_recovery::StartupPidGetProc(pid);
    }
}

void MultiRedoUpdateStandbyState(HotStandbyState newState)
{
    if (IsExtremeRedo()) {
        extreme_rto::UpdateStandbyState(newState);
    } else if (IsParallelRedo()) {
        parallel_recovery::UpdateStandbyState(newState);
    }
}

uint32 MultiRedoGetWorkerId()
{
    if (IsExtremeRedo()) {
        return extreme_rto::GetMyPageRedoWorkerIdWithLock();
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
    ereport(LOG,
            (errmodule(MOD_REDO), errcode(ERRCODE_LOG), errmsg("page workers all exit or not open parallel redo")));

    return true;
}

void SetPageRedoWorkerIndex(int index)
{
    if (IsExtremeRedo()) {
        extreme_rto::g_redoWorker->index = index;
    } else if (IsParallelRedo()) {
        parallel_recovery::g_redoWorker->index = index;
    }
}

int GetPageRedoWorkerIndex(int index)
{
    if (IsExtremeRedo()) {
        return extreme_rto::g_redoWorker->index;
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
        extreme_rto::g_redoWorker = (extreme_rto::PageRedoWorker *)arg->payload;
    } else if (IsParallelRedo()) {
        parallel_recovery::g_redoWorker = (parallel_recovery::PageRedoWorker *)arg->payload;
    }
}

/* Run from the worker thread. */
uint32 GetMyPageRedoWorkerId()
{
    if (IsExtremeRedo()) {
        return extreme_rto::g_redoWorker->id;
    } else if (IsParallelRedo()) {
        return parallel_recovery::g_redoWorker->id;
    } else {
        return 0;
    }
}

void MultiRedoMain()
{
    if (IsExtremeRedo()) {
        extreme_rto::ParallelRedoThreadMain();
    } else if (IsParallelRedo()) {
        parallel_recovery::PageRedoWorkerMain();
    } else {
        ereport(ERROR, (errmsg("MultiRedoMain parallel redo and extreme redo is close, should not be here!")));
    }
}

void EndDispatcherContext()
{
    if (IsExtremeRedo()) {
        (void)MemoryContextSwitchTo(extreme_rto::g_dispatcher->oldCtx);

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
        extreme_rto::FreeAllocatedRedoItem();

    } else if (IsParallelRedo()) {
        parallel_recovery::FreeAllocatedRedoItem();
    }
}

uint32 GetRedoWorkerCount()
{
    if (IsExtremeRedo()) {
        return extreme_rto::GetAllWorkerCount();

    } else if (IsParallelRedo()) {
        return parallel_recovery::GetPageWorkerCount();
    }

    return 0;
}

void **GetXLogInvalidPagesFromWorkers()
{
    if (IsExtremeRedo()) {
        return extreme_rto::GetXLogInvalidPagesFromWorkers();

    } else if (IsParallelRedo()) {
        return parallel_recovery::GetXLogInvalidPagesFromWorkers();
    }

    return NULL;
}

void SendRecoveryEndMarkToWorkersAndWaitForFinish(int code)
{
    if (IsExtremeRedo()) {
        return extreme_rto::SendRecoveryEndMarkToWorkersAndWaitForFinish(code);

    } else if (IsParallelRedo()) {
        return parallel_recovery::SendRecoveryEndMarkToWorkersAndWaitForFinish(code);
    }
}

RedoWaitInfo GetRedoIoEvent(int32 event_id)
{
    if (IsExtremeRedo()) {
        return extreme_rto::redo_get_io_event(event_id);
    } else {
        return parallel_recovery::redo_get_io_event(event_id);
    }
}

void GetRedoWrokerStatistic(uint32 *realNum, RedoWorkerStatsData *worker, uint32 workerLen)
{
    if (IsExtremeRedo()) {
        return extreme_rto::redo_get_wroker_statistic(realNum, worker, workerLen);
    } else {
        return parallel_recovery::redo_get_wroker_statistic(realNum, worker, workerLen);
    }
}
