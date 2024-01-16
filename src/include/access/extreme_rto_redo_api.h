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
 * ---------------------------------------------------------------------------------------
 *
 * extreme_rto_redo_api.h
 *
 *
 * IDENTIFICATION
 *        src/include/access/extreme_rto_redo_api.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef EXTREME_RTO_REDO_API_H
#define EXTREME_RTO_REDO_API_H

#include "access/xlogproc.h"
#include "access/redo_statistic.h"
#include "access/ondemand_extreme_rto/redo_utils.h"

typedef enum {
    DEFAULT_EXTREME_RTO,
    ONDEMAND_EXTREME_RTO,
} ExtremeRtoRedoType;

extern ExtremeRtoRedoType g_extreme_rto_type;

inline void SetDefaultExtremeRtoMode()
{
    g_extreme_rto_type = DEFAULT_EXTREME_RTO;
}

inline void SetOndemandExtremeRtoMode()
{
    g_extreme_rto_type = ONDEMAND_EXTREME_RTO;
}

inline bool IsDefaultExtremeRtoMode()
{
    return (g_extreme_rto_type == DEFAULT_EXTREME_RTO);
}

inline bool IsOndemandExtremeRtoMode()
{
    return (g_extreme_rto_type == ONDEMAND_EXTREME_RTO);
}
void ExtremeWaitAllReplayWorkerIdle();
void ExtremeDispatchCleanInvalidPageMarkToAllRedoWorker(RepairFileKey key);
void ExtremeDispatchClosefdMarkToAllRedoWorker();
void ExtremeRecordBadBlockAndPushToRemote(XLogBlockDataParse *datadecode, PageErrorType error_type,
    XLogRecPtr old_lsn, XLogPhyBlock pblk);
void ExtremeCheckCommittingCsnList();
XLogRecord *ExtremeReadNextXLogRecord(XLogReaderState **xlogreaderptr, int emode);
void ExtremeExtremeRtoStopHere();
void ExtremeWaitAllRedoWorkerQueueEmpty();
XLogRecPtr ExtremeGetSafeMinCheckPoint();
void ExtremeClearRecoveryThreadHashTbl(const RelFileNode &node, ForkNumber forknum, BlockNumber minblkno,
    bool segment_shrink);
void ExtremeBatchClearRecoveryThreadHashTbl(Oid spcNode, Oid dbNode);
bool ExtremeRedoWorkerIsUndoSpaceWorker();
void ExtremeStartRecoveryWorkers(XLogReaderState *xlogreader, uint32 privateLen);
void ExtremeDispatchRedoRecordToFile(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime);
void ExtremeGetThreadNameIfPageRedoWorker(int argc, char *argv[], char **threadNamePtr);
PGPROC *ExtremeStartupPidGetProc(ThreadId pid);
void ExtremeUpdateStandbyState(HotStandbyState newState);
void ExtremeUpdateMinRecoveryForTrxnRedoThd(XLogRecPtr newMinRecoveryPoint);
uint32 ExtremeGetMyPageRedoWorkerIdWithLock();
void ExtremeParallelRedoThreadMain();
void ExtremeFreeAllocatedRedoItem();
uint32 ExtremeGetAllWorkerCount();
void **ExtremeGetXLogInvalidPagesFromWorkers();
void ExtremeSendRecoveryEndMarkToWorkersAndWaitForFinish(int code);
RedoWaitInfo ExtremeRedoGetIoEvent(int32 event_id);
void ExtremeRedoGetWorkerStatistic(uint32 *realNum, RedoWorkerStatsData *worker, uint32 workerLen);
void ExtremeRedoGetWorkerTimeCount(RedoWorkerTimeCountsInfo **workerCountInfoList, uint32 *realNum);
void ExtremeEndDispatcherContext();
void ExtremeSetPageRedoWorkerIndex(int index);
int ExtremeGetPageRedoWorkerIndex();
void ExtremeSetMyPageRedoWorker(knl_thread_arg *arg);
uint32 ExtremeGetMyPageRedoWorkerId();
bool IsExtremeMultiThreadRedoRunning();
bool IsExtremeRtoRunning();
bool IsExtremeRtoSmartShutdown();
void ExtremeRtoRedoManagerSendEndToStartup();

#endif