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
 * dispatcher.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/access/parallel_recovery/dispatcher.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef PARALLEL_RECOVERY_DISPATCHER_H
#define PARALLEL_RECOVERY_DISPATCHER_H

#include "gs_thread.h"
#include "postgres.h"
#include "knl/knl_variable.h"
#include "access/xlog.h"
#include "access/xlogreader.h"
#include "nodes/pg_list.h"
#include "storage/proc.h"

#include "access/parallel_recovery/redo_item.h"
#include "access/parallel_recovery/page_redo.h"
#include "access/parallel_recovery/txn_redo.h"
#include "access/redo_statistic.h"

#define INVALID_WORKER_ID -1

namespace parallel_recovery {

typedef struct _DispatchFix {
    int dispatchRandom;
    XLogRecPtr lastCheckLsn;
}DispatchFix;

typedef struct LogDispatcher {
    MemoryContext oldCtx;
    PageRedoWorker** pageWorkers; /* Array of page redo workers. */
    uint32 pageWorkerCount;       /* Number of page redo workers that are ready to work. */
    uint32 totalWorkerCount;      /* Number of page redo workers started. */
    TxnRedoWorker* txnWorker;     /* Txn redo worker. */
    RedoItem* freeHead;           /* Head of freed-item list. */
    RedoItem* freeStateHead;
    RedoItem* allocatedRedoItem;
    int32 pendingCount; /* Number of records pending. */
    int32 pendingMax;   /* The max. pending count per batch. */
    int exitCode;       /* Thread exit code. */
    uint64 totalCostTime;
    uint64 txnCostTime; /* txn cost time */
    uint64 pprCostTime;
    uint32 maxItemNum;
    uint32 curItemNum;
    TimestampTz lastDispatchTime; /* last time we dispatch record list to works */

    uint32* chosedWorkerIds;
    uint32 chosedWorkerCount;
    uint32 readyWorkerCnt;
    XLogRecPtr dispatchReadRecPtr; /* start of dispatch record read */
    XLogRecPtr dispatchEndRecPtr;  /* end of dispatch record read */
    bool checkpointNeedFullSync;
    RedoInterruptCallBackFunc oldStartupIntrruptFunc;
    XLogRedoNumStatics xlogStatics[RM_NEXT_ID][MAX_XLOG_INFO_NUM];
    RedoTimeCost *startupTimeCost;
    DispatchFix dispatchFix;
    bool full_sync_dispatch;
} LogDispatcher;

extern LogDispatcher* g_dispatcher;

#ifdef ENABLE_MULTIPLE_NODES
const static bool SUPPORT_HOT_STANDBY = false;   /* don't support consistency view */
#else
const static bool SUPPORT_HOT_STANDBY = true;
#endif
const static bool SUPPORT_FPAGE_DISPATCH = true; /*  support file dispatch if true, else support page dispatche */

const static uint64 OUTPUT_WAIT_COUNT = 0x7FFFFFF;
const static uint64 PRINT_ALL_WAIT_COUNT = 0x7FFFFFFFF;


void StartRecoveryWorkers(XLogRecPtr startLsn);

/* RedoItem lifecycle. */
void DispatchRedoRecordToFile(XLogReaderState* record, List* expectedTLIs, TimestampTz recordXTime);
void ProcessPendingRecords(bool fullSync = false);
void ProcessTrxnRecords(bool fullSync = false);
void FreeRedoItem(RedoItem* item);

/* Dispatcher phases. */
void SendRecoveryEndMarkToWorkersAndWaitForFinish(int code);

/* Dispatcher states. */
int GetDispatcherExitCode();
bool DispatchPtrIsNull();
uint32 GetPageWorkerCount();
bool OnHotStandBy();
PGPROC* StartupPidGetProc(ThreadId pid);

/* Run-time aggregated page worker states. */
bool IsRecoveryRestartPointSafeForWorkers(XLogRecPtr restartPoint);

void UpdateStandbyState(HotStandbyState newState);

/* Redo end state saved by each page worker. */
void** GetXLogInvalidPagesFromWorkers();

/* Other utility functions. */
uint32 GetWorkerId(const RelFileNode& node, BlockNumber block, ForkNumber forkNum);
XLogReaderState* NewReaderState(XLogReaderState* readerState, bool bCopyState = false);
void FreeAllocatedRedoItem();
void GetReplayedRecPtrFromWorkers(XLogRecPtr *readPtr, XLogRecPtr *endPtr);
void GetReplayingRecPtrFromWorkers(XLogRecPtr *endPtr);
void GetReplayedRecPtrFromUndoWorkers(XLogRecPtr *readPtr, XLogRecPtr *endPtr);
List* CheckImcompleteAction(List* imcompleteActionList);
void SetPageWorkStateByThreadId(uint32 threadState);
RedoWaitInfo redo_get_io_event(int32 event_id);
void redo_get_worker_statistic(uint32* realNum, RedoWorkerStatsData* worker, uint32 workerLen);
extern void redo_dump_all_stats();
void WaitRedoWorkerIdle();
void SendClearMarkToAllWorkers();
void SendClosefdMarkToAllWorkers();
void SendCleanInvalidPageMarkToAllWorkers(RepairFileKey key);
extern void SetStartupBufferPinWaitBufId(int bufid);
extern void GetStartupBufferPinWaitBufId(int *bufids, uint32 len);
extern uint32 GetStartupBufferPinWaitBufLen();
extern void InitReaderStateByOld(XLogReaderState *newState, XLogReaderState *oldState, bool isNew);
extern void CopyDataFromOldReader(XLogReaderState *newReaderState, XLogReaderState *oldReaderState);

bool TxnQueueIsEmpty(TxnRedoWorker* worker);
void redo_get_worker_time_count(RedoWorkerTimeCountsInfo **workerCountInfoList, uint32 *realNum);
bool in_full_sync_dispatch(void);
}

#endif
