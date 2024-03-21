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
#include "access/extreme_rto_redo_api.h"
#include "postmaster/postmaster.h"

#ifdef ENABLE_LITE_MODE
#define ENABLE_ONDEMAND_RECOVERY false
#define ENABLE_ONDEMAND_REALTIME_BUILD false
#else
#define ENABLE_ONDEMAND_RECOVERY (ENABLE_DMS && IsExtremeRedo() \
    && g_instance.attr.attr_storage.dms_attr.enable_ondemand_recovery)
#define ENABLE_ONDEMAND_REALTIME_BUILD (ENABLE_ONDEMAND_RECOVERY \
    && g_instance.attr.attr_storage.dms_attr.enable_ondemand_realtime_build)
#endif

typedef enum {
    NOT_PAGE_REDO_THREAD,
    PAGE_REDO_THREAD_EXIT_NORMAL,
    PAGE_REDO_THREAD_EXIT_ABNORMAL,
} PageRedoExitStatus;

extern bool g_supportHotStandby;
extern uint32 g_startupTriggerState;

const static bool SUPPORT_FPAGE_DISPATCH = true; /*  support file dispatch if true, else support page dispatche */
const static bool SUPPORT_USTORE_UNDO_WORKER = true; /* support USTORE has undo redo worker, support page dispatch */

const static bool SUPPORT_DFS_BATCH = false; 
const static bool SUPPORT_COLUMN_BATCH = true;   /* don't support column batch redo */

static const uint32 UNDO_WORKER_FRACTION = 2;

static const uint32 PAGE_REDO_WORKER_INVALID = 0;
static const uint32 PAGE_REDO_WORKER_START = 1;
static const uint32 PAGE_REDO_WORKER_READY = 2;
static const uint32 PAGE_REDO_WORKER_EXIT = 3;
static const uint32 BIG_RECORD_LENGTH = XLOG_BLCKSZ * 16;

#define IS_EXRTO_READ (IsExtremeRedo() && g_instance.attr.attr_storage.EnableHotStandby && IsDefaultExtremeRtoMode())
#define IS_EXRTO_RECOVERY_IN_PROGRESS (RecoveryInProgress() && IsExtremeRedo() && IsDefaultExtremeRtoMode())
#define IS_EXRTO_STANDBY_READ (pm_state_is_hot_standby() && IS_EXRTO_READ)
#define IS_EXRTO_READ_OPT \
    (g_instance.attr.attr_storage.EnableHotStandby && g_instance.attr.attr_storage.enable_exrto_standby_read_opt)

inline bool is_exrto_standby_read_worker()
{
    return (t_thrd.role == WORKER || t_thrd.role == THREADPOOL_WORKER || t_thrd.role == THREADPOOL_STREAM ||
        t_thrd.role == STREAM_WORKER);
}

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
    if (ENABLE_DMS && SS_STANDBY_PROMOTING) {
        /* SS switchover promote replays 1 record, hence no PR/ERTO needed */
        return false;
    }
    return g_instance.comm_cxt.predo_cxt.redoType == EXTREME_REDO && (get_real_recovery_parallelism() > 1);
}

inline bool IsParallelRedo()
{
    if (ENABLE_DMS && SS_STANDBY_PROMOTING) {
        /* SS switchover promote replays 1 record, hence no PR/ERTO needed */
        return false;
    }
    return g_instance.comm_cxt.predo_cxt.redoType == PARALLEL_REDO && (get_real_recovery_parallelism() > 1);
}

static inline bool IsMultiThreadRedo()
{
    return (get_real_recovery_parallelism() > 1);
}

inline bool is_index_only_disabled_in_astore()
{
    return (RecoveryInProgress() &&
            (IsParallelRedo() ||
            (IsExtremeRedo() && g_instance.attr.attr_storage.enable_exrto_standby_read_opt)));
}

uint32 GetRedoWorkerCount();
bool IsMultiThreadRedoRunning();
void DispatchRedoRecord(XLogReaderState* record, List* expectedTLIs, TimestampTz recordXTime);
void GetThreadNameIfMultiRedo(int argc, char* argv[], char** threadNamePtr);

PGPROC* MultiRedoThreadPidGetProc(ThreadId pid);
void MultiRedoUpdateStandbyState(HotStandbyState newState);
void MultiRedoUpdateMinRecovery(XLogRecPtr newMinRecoveryPoint);
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
void GetRedoWorkerStatistic(uint32* realNum, RedoWorkerStatsData* worker, uint32 workerLen);
void CountXLogNumbers(XLogReaderState *record);
void ApplyRedoRecord(XLogReaderState* record);
void DiagLogRedoRecord(XLogReaderState *record, const char *funcName);
void GetRedoWorkerTimeCount(RedoWorkerTimeCountsInfo **workerCountInfoList, uint32 *realNum);
void ResetXLogStatics();

static inline void GetRedoStartTime(RedoTimeCost &cost)
{
    if(!g_instance.attr.attr_storage.enable_time_report)
        return;
    cost.startTime = GetCurrentTimestamp();
}

static inline void CountRedoTime(RedoTimeCost &cost)
{
    if(!g_instance.attr.attr_storage.enable_time_report)
        return;
    cost.totalDuration += GetCurrentTimestamp() - cost.startTime;
    cost.counter += 1;
}

static inline void CountAndGetRedoTime(RedoTimeCost &curCost, RedoTimeCost &nextCost)
{
    uint64 curTime = 0;

    if(!g_instance.attr.attr_storage.enable_time_report)
        return;

    curTime = GetCurrentTimestamp();
    curCost.totalDuration += curTime - curCost.startTime;
    curCost.counter += 1;
    nextCost.startTime = curTime;
}

#endif
