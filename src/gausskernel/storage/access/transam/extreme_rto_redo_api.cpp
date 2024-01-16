/*
 * Copyright (c) 2023 Huawei Technologies Co.,Ltd.
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
 * extreme_rto_redo_api.cpp
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/transam/extreme_rto_redo_api.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <stdio.h>
#include <unistd.h>

#include "postgres.h"
#include "knl/knl_variable.h"
#include "utils/guc.h"
#include "access/multi_redo_api.h"

#include "access/extreme_rto/dispatcher.h"
#include "access/extreme_rto/page_redo.h"
#include "access/extreme_rto/redo_item.h"
#include "access/extreme_rto/posix_semaphore.h"
#include "access/extreme_rto/spsc_blocking_queue.h"
#include "access/extreme_rto/xlog_read.h"
#include "access/ondemand_extreme_rto/dispatcher.h"
#include "access/ondemand_extreme_rto/page_redo.h"
#include "access/ondemand_extreme_rto/redo_item.h"
#include "access/ondemand_extreme_rto/posix_semaphore.h"
#include "access/ondemand_extreme_rto/spsc_blocking_queue.h"
#include "access/ondemand_extreme_rto/xlog_read.h"


ExtremeRtoRedoType g_extreme_rto_type = DEFAULT_EXTREME_RTO;

typedef struct f_extreme_rto_redo {
    void (*wait_all_replay_worker_idle)(void);
    void (*dispatch_clean_invalid_page_mark_to_all_redo_worker)(RepairFileKey key);
    void (*dispatch_closefd_mark_to_all_redo_worker)(void);
    void (*record_bad_block_and_push_to_remote)(XLogBlockDataParse *datadecode, PageErrorType error_type,
        XLogRecPtr old_lsn, XLogPhyBlock pblk);
    void (*check_committing_csn_list)(void);
    XLogRecord *(*read_next_xlog_record)(XLogReaderState **xlogreaderptr, int emode);
    void (*extreme_rto_stop_here)(void);
    void (*wait_all_redo_worker_queue_empty)(void);
    XLogRecPtr (*get_safe_min_check_point)(void);
    void (*clear_recovery_thread_hash_tbl)(const RelFileNode &node, ForkNumber forknum, BlockNumber minblkno,
        bool segment_shrink);
    void (*batch_clear_recovery_thread_hash_tbl)(Oid spcNode, Oid dbNode);
    bool (*redo_worker_is_undo_space_worker)(void);
    void (*start_recovery_workers)(XLogReaderState *xlogreader, uint32 privateLen);
    void (*dispatch_redo_record_to_file)(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime);
    void (*get_thread_name_if_page_redo_worker)(int argc, char *argv[], char **threadNamePtr);
    PGPROC *(*startup_pid_get_proc)(ThreadId pid);
    void (*update_standby_state)(HotStandbyState newState);
    void (*update_min_recovery_for_trxn_redo_thd)(XLogRecPtr newMinRecoveryPoint);
    uint32 (*get_my_page_redo_worker_id_with_lock)(void);
    void (*parallel_redo_thread_main)(void);
    void (*free_allocated_redo_item)(void);
    uint32 (*get_all_worker_count)(void);
    void **(*get_xlog_invalid_pages_from_workers)(void);
    void (*send_recovery_end_mark_to_workers_and_wait_for_finish)(int code);
    RedoWaitInfo (*redo_get_io_event)(int32 event_id);
    void (*redo_get_worker_statistic)(uint32 *realNum, RedoWorkerStatsData *worker, uint32 workerLen);
    void (*redo_get_worker_time_count)(RedoWorkerTimeCountsInfo **workerCountInfoList, uint32 *realNum);
} f_extreme_rto_redo;

static const f_extreme_rto_redo extreme_rto_redosw[] = {
    /* extreme redo */
    {
        extreme_rto::WaitAllReplayWorkerIdle,
        extreme_rto::DispatchCleanInvalidPageMarkToAllRedoWorker,
        extreme_rto::DispatchClosefdMarkToAllRedoWorker,
        extreme_rto::RecordBadBlockAndPushToRemote,
        extreme_rto::CheckCommittingCsnList,
        extreme_rto::ReadNextXLogRecord,
        extreme_rto::ExtremeRtoStopHere,
        extreme_rto::WaitAllRedoWorkerQueueEmpty,
        extreme_rto::GetSafeMinCheckPoint,
        extreme_rto::ClearRecoveryThreadHashTbl,
        extreme_rto::BatchClearRecoveryThreadHashTbl,
        extreme_rto::RedoWorkerIsUndoSpaceWorker,
        extreme_rto::StartRecoveryWorkers,
        extreme_rto::DispatchRedoRecordToFile,
        extreme_rto::GetThreadNameIfPageRedoWorker,
        extreme_rto::StartupPidGetProc,
        extreme_rto::UpdateStandbyState,
        extreme_rto::UpdateMinRecoveryForTrxnRedoThd,
        extreme_rto::GetMyPageRedoWorkerIdWithLock,
        extreme_rto::ParallelRedoThreadMain,
        extreme_rto::FreeAllocatedRedoItem,
        extreme_rto::GetAllWorkerCount,
        extreme_rto::GetXLogInvalidPagesFromWorkers,
        extreme_rto::SendRecoveryEndMarkToWorkersAndWaitForFinish,
        extreme_rto::redo_get_io_event,
        extreme_rto::redo_get_worker_statistic,
        extreme_rto::redo_get_worker_time_count,
    },

    /* ondemand extreme redo */
    {
        ondemand_extreme_rto::WaitAllReplayWorkerIdle,
        ondemand_extreme_rto::DispatchCleanInvalidPageMarkToAllRedoWorker,
        ondemand_extreme_rto::DispatchClosefdMarkToAllRedoWorker,
        ondemand_extreme_rto::RecordBadBlockAndPushToRemote,
        ondemand_extreme_rto::CheckCommittingCsnList,
        ondemand_extreme_rto::ReadNextXLogRecord,
        ondemand_extreme_rto::ExtremeRtoStopHere,
        ondemand_extreme_rto::WaitAllRedoWorkerQueueEmpty,
        ondemand_extreme_rto::GetSafeMinCheckPoint,
        ondemand_extreme_rto::ClearRecoveryThreadHashTbl,
        ondemand_extreme_rto::BatchClearRecoveryThreadHashTbl,
        ondemand_extreme_rto::RedoWorkerIsUndoSpaceWorker,
        ondemand_extreme_rto::StartRecoveryWorkers,
        ondemand_extreme_rto::DispatchRedoRecordToFile,
        ondemand_extreme_rto::GetThreadNameIfPageRedoWorker,
        ondemand_extreme_rto::StartupPidGetProc,
        ondemand_extreme_rto::UpdateStandbyState,
        ondemand_extreme_rto::UpdateMinRecoveryForTrxnRedoThd,
        ondemand_extreme_rto::GetMyPageRedoWorkerIdWithLock,
        ondemand_extreme_rto::ParallelRedoThreadMain,
        ondemand_extreme_rto::FreeAllocatedRedoItem,
        ondemand_extreme_rto::GetAllWorkerCount,
        ondemand_extreme_rto::GetXLogInvalidPagesFromWorkers,
        NULL,
        ondemand_extreme_rto::redo_get_io_event,
        ondemand_extreme_rto::redo_get_worker_statistic,
        ondemand_extreme_rto::redo_get_worker_time_count,
    },
};

void ExtremeWaitAllReplayWorkerIdle()
{
    (*(extreme_rto_redosw[g_extreme_rto_type].wait_all_replay_worker_idle))();
}

void ExtremeDispatchCleanInvalidPageMarkToAllRedoWorker(RepairFileKey key)
{
    (*(extreme_rto_redosw[g_extreme_rto_type].dispatch_clean_invalid_page_mark_to_all_redo_worker))(key);
}

void ExtremeDispatchClosefdMarkToAllRedoWorker()
{
    (*(extreme_rto_redosw[g_extreme_rto_type].dispatch_closefd_mark_to_all_redo_worker))();
}

void ExtremeRecordBadBlockAndPushToRemote(XLogBlockDataParse *datadecode, PageErrorType error_type,
    XLogRecPtr old_lsn, XLogPhyBlock pblk)
{
    (*(extreme_rto_redosw[g_extreme_rto_type].record_bad_block_and_push_to_remote))(datadecode, error_type, old_lsn,
                                                                                    pblk);
}

void ExtremeCheckCommittingCsnList()
{
    (*(extreme_rto_redosw[g_extreme_rto_type].check_committing_csn_list))();
}

XLogRecord *ExtremeReadNextXLogRecord(XLogReaderState **xlogreaderptr, int emode)
{
    return (*(extreme_rto_redosw[g_extreme_rto_type].read_next_xlog_record))(xlogreaderptr, emode);
}

void ExtremeExtremeRtoStopHere()
{
    (*(extreme_rto_redosw[g_extreme_rto_type].extreme_rto_stop_here))();
}

void ExtremeWaitAllRedoWorkerQueueEmpty()
{
    (*(extreme_rto_redosw[g_extreme_rto_type].wait_all_redo_worker_queue_empty))();
}

XLogRecPtr ExtremeGetSafeMinCheckPoint()
{
    return (*(extreme_rto_redosw[g_extreme_rto_type].get_safe_min_check_point))();
}

void ExtremeClearRecoveryThreadHashTbl(const RelFileNode &node, ForkNumber forknum, BlockNumber minblkno,
    bool segment_shrink)
{
    (*(extreme_rto_redosw[g_extreme_rto_type].clear_recovery_thread_hash_tbl))(node, forknum, minblkno, segment_shrink);
}

void ExtremeBatchClearRecoveryThreadHashTbl(Oid spcNode, Oid dbNode)
{
    (*(extreme_rto_redosw[g_extreme_rto_type].batch_clear_recovery_thread_hash_tbl))(spcNode, dbNode);
}

bool ExtremeRedoWorkerIsUndoSpaceWorker()
{
    return (*(extreme_rto_redosw[g_extreme_rto_type].redo_worker_is_undo_space_worker))();
}

void ExtremeStartRecoveryWorkers(XLogReaderState *xlogreader, uint32 privateLen)
{
    (*(extreme_rto_redosw[g_extreme_rto_type].start_recovery_workers))(xlogreader, privateLen);
}

void ExtremeDispatchRedoRecordToFile(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime)
{
    (*(extreme_rto_redosw[g_extreme_rto_type].dispatch_redo_record_to_file))(record, expectedTLIs, recordXTime);
}

void ExtremeGetThreadNameIfPageRedoWorker(int argc, char *argv[], char **threadNamePtr)
{
    (*(extreme_rto_redosw[g_extreme_rto_type].get_thread_name_if_page_redo_worker))(argc, argv, threadNamePtr);
}

PGPROC *ExtremeStartupPidGetProc(ThreadId pid)
{
    return (*(extreme_rto_redosw[g_extreme_rto_type].startup_pid_get_proc))(pid);
}

void ExtremeUpdateStandbyState(HotStandbyState newState)
{
    (*(extreme_rto_redosw[g_extreme_rto_type].update_standby_state))(newState);
}

void ExtremeUpdateMinRecoveryForTrxnRedoThd(XLogRecPtr newMinRecoveryPoint)
{
    (*(extreme_rto_redosw[g_extreme_rto_type].update_min_recovery_for_trxn_redo_thd))(newMinRecoveryPoint);
}

uint32 ExtremeGetMyPageRedoWorkerIdWithLock()
{
    return (*(extreme_rto_redosw[g_extreme_rto_type].get_my_page_redo_worker_id_with_lock))();
}

void ExtremeParallelRedoThreadMain()
{
    (*(extreme_rto_redosw[g_extreme_rto_type].parallel_redo_thread_main))();
}

void ExtremeFreeAllocatedRedoItem()
{
    (*(extreme_rto_redosw[g_extreme_rto_type].free_allocated_redo_item))();
}

uint32 ExtremeGetAllWorkerCount()
{
    return (*(extreme_rto_redosw[g_extreme_rto_type].get_all_worker_count))();
}

void **ExtremeGetXLogInvalidPagesFromWorkers()
{
    return (*(extreme_rto_redosw[g_extreme_rto_type].get_xlog_invalid_pages_from_workers))();
}

void ExtremeSendRecoveryEndMarkToWorkersAndWaitForFinish(int code)
{
    (*(extreme_rto_redosw[g_extreme_rto_type].send_recovery_end_mark_to_workers_and_wait_for_finish))(code);
}

RedoWaitInfo ExtremeRedoGetIoEvent(int32 event_id)
{
    return (*(extreme_rto_redosw[g_extreme_rto_type].redo_get_io_event))(event_id);
}

void ExtremeRedoGetWorkerStatistic(uint32 *realNum, RedoWorkerStatsData *worker, uint32 workerLen)
{
    (*(extreme_rto_redosw[g_extreme_rto_type].redo_get_worker_statistic))(realNum, worker, workerLen);
}

void ExtremeRedoGetWorkerTimeCount(RedoWorkerTimeCountsInfo **workerCountInfoList, uint32 *realNum)
{
    (*(extreme_rto_redosw[g_extreme_rto_type].redo_get_worker_time_count))(workerCountInfoList, realNum);
}

void ExtremeEndDispatcherContext()
{
    switch (g_extreme_rto_type) {
        case DEFAULT_EXTREME_RTO:
            (void)MemoryContextSwitchTo(extreme_rto::g_dispatcher->oldCtx);
            break;
        case ONDEMAND_EXTREME_RTO:
            (void)MemoryContextSwitchTo(ondemand_extreme_rto::g_dispatcher->oldCtx);
            break;
        default:
            Assert(true);
    }
}

void ExtremeSetPageRedoWorkerIndex(int index)
{
    switch (g_extreme_rto_type) {
        case DEFAULT_EXTREME_RTO:
            extreme_rto::g_redoWorker->index = index;
            break;
        case ONDEMAND_EXTREME_RTO:
            ondemand_extreme_rto::g_redoWorker->index = index;
            break;
        default:
            Assert(true);
    }
}

int ExtremeGetPageRedoWorkerIndex()
{
    int result = 0;

    switch (g_extreme_rto_type) {
        case DEFAULT_EXTREME_RTO:
            result = extreme_rto::g_redoWorker->index;
            break;
        case ONDEMAND_EXTREME_RTO:
            result = ondemand_extreme_rto::g_redoWorker->index;
            break;
        default:
            Assert(true);
    }

    return result;
}

void ExtremeSetMyPageRedoWorker(knl_thread_arg *arg)
{
    switch (g_extreme_rto_type) {
        case DEFAULT_EXTREME_RTO:
            extreme_rto::g_redoWorker = (extreme_rto::PageRedoWorker *)arg->payload;
            break;
        case ONDEMAND_EXTREME_RTO:
            ondemand_extreme_rto::g_redoWorker = (ondemand_extreme_rto::PageRedoWorker *)arg->payload;
            break;
        default:
            Assert(true);
    }
}

uint32 ExtremeGetMyPageRedoWorkerId()
{
    uint32 result = 0;

    switch (g_extreme_rto_type) {
        case DEFAULT_EXTREME_RTO:
            result = extreme_rto::g_redoWorker->id;
            break;
        case ONDEMAND_EXTREME_RTO:
            result = ondemand_extreme_rto::g_redoWorker->id;
            break;
        default:
            Assert(true);
    }

    return result;
}

bool IsExtremeMultiThreadRedoRunning()
{
    return (get_real_recovery_parallelism() > 1 &&
            (extreme_rto::g_dispatcher != 0 || ondemand_extreme_rto::g_dispatcher != 0 ));
}

bool IsExtremeRtoRunning()
{
    bool result = false;

    switch (g_extreme_rto_type) {
        case DEFAULT_EXTREME_RTO:
            result = (get_real_recovery_parallelism() > 1 && extreme_rto::g_dispatcher != 0 &&
                extreme_rto::g_dispatcher->pageLineNum > 0);
            break;
        case ONDEMAND_EXTREME_RTO:
            result = (get_real_recovery_parallelism() > 1 && ondemand_extreme_rto::g_dispatcher != 0 &&
                ondemand_extreme_rto::g_dispatcher->pageLineNum > 0);
            break;
        default:
            Assert(true);
    }

    return result;
}

bool IsExtremeRtoSmartShutdown()
{
    if (!IsExtremeRtoRunning()) {
        return false;
    }

    bool result = false;
    
    switch (g_extreme_rto_type) {
        case DEFAULT_EXTREME_RTO:
            if (extreme_rto::g_dispatcher->smartShutdown) {
                extreme_rto::g_dispatcher->smartShutdown = false;
                result = true;
            }
            break;
        case ONDEMAND_EXTREME_RTO:
            if (ondemand_extreme_rto::g_dispatcher->smartShutdown) {
                ondemand_extreme_rto::g_dispatcher->smartShutdown = false;
                result = true;
            }
            break;
        default:
            Assert(true);
    }

    return result;
}

void ExtremeRtoRedoManagerSendEndToStartup()
{
    if (!IsExtremeRtoRunning()) {
        return;
    }

    switch (g_extreme_rto_type) {
        case DEFAULT_EXTREME_RTO:
            extreme_rto::g_redoEndMark.record.isDecode = true;
            extreme_rto::PutRecordToReadQueue((XLogReaderState *)&extreme_rto::g_redoEndMark.record);
            break;
        case ONDEMAND_EXTREME_RTO:
            ondemand_extreme_rto::g_redoEndMark.record.isDecode = true;
            ondemand_extreme_rto::PutRecordToReadQueue((XLogReaderState *)&ondemand_extreme_rto::g_redoEndMark.record);
            break;
        default:
            Assert(true);
    }
}