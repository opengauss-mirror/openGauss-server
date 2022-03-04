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
 * ---------------------------------------------------------------------------------------
 *
 * multi_redo_api.h
 *
 *
 * IDENTIFICATION
 *        src/include/access/multi_redo_api.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef MULTI_REDO_API_H
#define MULTI_REDO_API_H

#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/xlog.h"
#include "access/xlogreader.h"
#include "nodes/pg_list.h"
#include "storage/proc.h"
#include "access/redo_statistic.h"




typedef enum {
    NOT_PAGE_REDO_THREAD,
    PAGE_REDO_THREAD_EXIT_NORMAL,
    PAGE_REDO_THREAD_EXIT_ABNORMAL,
} PageRedoExitStatus;

extern bool g_supportHotStandby;

const static bool SUPPORT_FPAGE_DISPATCH = true; /*  support file dispatch if true, else support page dispatche */
const static bool SUPPORT_USTORE_UNDO_WORKER = true; /* support USTORE has undo redo worker, support page dispatch */

const static bool SUPPORT_DFS_BATCH = false; 
const static bool SUPPORT_COLUMN_BATCH = true;   /* don't support column batch redo */

static const uint32 UNDO_WORKER_FRACTION = 2;

static const uint32 PAGE_REDO_WORKER_INVALID = 0;
static const uint32 PAGE_REDO_WORKER_START = 1;
static const uint32 PAGE_REDO_WORKER_READY = 2;
static const uint32 PAGE_REDO_WORKER_EXIT = 3;

static inline int get_real_recovery_parallelism()
{
    return g_instance.attr.attr_storage.real_recovery_parallelism;
}

static inline int get_recovery_undozidworkers_num()
{
    return 1;
}

inline bool IsExtremeRedo()
{
    return g_instance.comm_cxt.predo_cxt.redoType == EXTREME_REDO && (get_real_recovery_parallelism() > 1);
}

inline bool IsParallelRedo()
{
    return g_instance.comm_cxt.predo_cxt.redoType == PARALLEL_REDO && (get_real_recovery_parallelism() > 1);
}


static inline bool IsMultiThreadRedo()
{
    return (get_real_recovery_parallelism() > 1);
}

uint32 GetRedoWorkerCount();

bool IsMultiThreadRedoRunning();
bool IsExtremeRtoRunning();
void DispatchRedoRecord(XLogReaderState* record, List* expectedTLIs, TimestampTz recordXTime);
void GetThreadNameIfMultiRedo(int argc, char* argv[], char** threadNamePtr);

PGPROC* MultiRedoThreadPidGetProc(ThreadId pid);
void MultiRedoUpdateStandbyState(HotStandbyState newState);
uint32 MultiRedoGetWorkerId();
bool IsAllPageWorkerExit();
void SetPageRedoWorkerIndex(int index);
int GetPageRedoWorkerIndex(int index);
PageRedoExitStatus CheckExitPageWorkers(ThreadId pid);
void SetMyPageRedoWorker(knl_thread_arg* arg);
uint32 GetMyPageRedoWorkerId();
void MultiRedoMain();
void StartUpMultiRedo(XLogReaderState* xlogreader, uint32 privateLen);

void ProcTxnWorkLoad(bool force);
void EndDispatcherContext();
void SwitchToDispatcherContext();

void FreeAllocatedRedoItem();
void** GetXLogInvalidPagesFromWorkers();
void SendRecoveryEndMarkToWorkersAndWaitForFinish(int code);
RedoWaitInfo GetRedoIoEvent(int32 event_id);
void GetRedoWrokerStatistic(uint32* realNum, RedoWorkerStatsData* worker, uint32 workerLen);
bool IsExtremeRtoSmartShutdown();
void ExtremeRtoRedoManagerSendEndToStartup();
void CountXLogNumbers(XLogReaderState *record);
void ApplyRedoRecord(XLogReaderState* record);
void DiagLogRedoRecord(XLogReaderState *record, const char *funcName);
void GetRedoWorkerTimeCount(RedoWorkerTimeCountsInfo **workerCountInfoList, uint32 *realNum);
void ResetXLogStatics();

static inline void GetRedoStartTime(RedoTimeCost &cost)
{
    cost.startTime = GetCurrentTimestamp();
}

static inline void CountRedoTime(RedoTimeCost &cost)
{
    cost.totalDuration += GetCurrentTimestamp() - cost.startTime;
    cost.counter += 1;
}

static inline void CountAndGetRedoTime(RedoTimeCost &curCost, RedoTimeCost &nextCost)
{
    uint64 curTime = GetCurrentTimestamp();
    curCost.totalDuration += curTime - curCost.startTime;
    curCost.counter += 1;
    nextCost.startTime = curTime;
}

#endif
