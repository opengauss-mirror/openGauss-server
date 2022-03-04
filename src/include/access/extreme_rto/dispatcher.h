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
 *        src/include/access/extreme_rto/dispatcher.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef EXTREME_RTO_DISPATCHER_H
#define EXTREME_RTO_DISPATCHER_H

#include "gs_thread.h"
#include "postgres.h"
#include "knl/knl_variable.h"
#include "access/xlog.h"
#include "access/xlogreader.h"
#include "nodes/pg_list.h"
#include "storage/proc.h"
#include "access/redo_statistic.h"
#include "access/extreme_rto/redo_item.h"
#include "access/extreme_rto/page_redo.h"
#include "access/extreme_rto/txn_redo.h"

namespace extreme_rto {

typedef struct {
    PageRedoWorker *batchThd;   /* BatchRedoThread */
    PageRedoWorker *managerThd; /* PageRedoManager */
    PageRedoWorker **redoThd;   /* RedoThreadPool */
    uint32 redoThdNum;
    uint32 *chosedRTIds; /* chosedRedoThdIds */
    uint32 chosedRTCnt;  /* chosedRedoThdCount */
} PageRedoPipeline;

typedef struct {
    PageRedoWorker *managerThd; /* TrxnRedoManager */
    PageRedoWorker *redoThd;    /* TrxnRedoWorker */
} TrxnRedoPipeline;

typedef struct ReadPipeline {
    PageRedoWorker *managerThd;  /* readthrd */
    PageRedoWorker *readPageThd; /* readthrd */
    PageRedoWorker *readThd;     /* readthrd */
} ReadPipeline;

#define MAX_XLOG_READ_BUFFER (0xFFFFF) /* 8k uint */

#define MAX_ALLOC_SEGNUM (4) /* 16* 4 */

typedef enum {
    WORKER_STATE_STOP = 0,
    WORKER_STATE_RUN,
    WORKER_STATE_STOPPING,
    WORKER_STATE_EXIT,
    WORKER_STATE_EXITING,
} ReadWorkersState;

typedef enum {
    TRIGGER_NORMAL = 0,
    TRIGGER_PRIMARY,
    TRIGGER_STADNBY,
    TRIGGER_FAILOVER,
    TRIGGER_SWITCHOVER,
    TRIGGER_SMARTSHUTDOWN,
} Enum_TriggeredState;

typedef enum {
    NONE,
    APPLYING,
    APPLIED,
} ReadBufState;

typedef enum {
    READ_MANAGER_STOP,
    READ_MANAGER_RUN,
} XLogReadManagerState;

typedef struct RecordBufferAarray {
    XLogSegNo segno;
    XLogRecPtr segoffset;
    uint32 readlen;
    char *readsegbuf;
    uint32 bufState;
} RecordBufferAarray;

#ifdef USE_ASSERT_CHECKING
#define LSN_CHECK_BUF_SIZE (128*1024*1024)
typedef struct {
    uint64 curPosition;
    XLogRecPtr curLsn;
#if (!defined __x86_64__) && (!defined __aarch64__)
    /* protects lastReplayedReadRecPtr and lastReplayedEndRecPtr */
    slock_t ptrLck;
#endif
    uint32 lsnCheckBuf[LSN_CHECK_BUF_SIZE];
}LsnCheckCtl;

#endif

typedef struct RecordBufferState {
    XLogReaderState *initreader;
    uint32 readWorkerState;
    uint32 readPageWorkerState;
    uint32 readSource;
    uint32 failSource;
    uint32 xlogReadManagerState;
    uint32 applyindex;
    uint32 readindex;
    RecordBufferAarray xlogsegarray[MAX_ALLOC_SEGNUM];
    char *readsegbuf;
    char *readBuf;
    char *errormsg_buf;
    void *readprivate;
    XLogRecPtr targetRecPtr;
    XLogRecPtr expectLsn;
    uint32 waitRedoDone;
} RecordBufferState;

typedef struct {
    MemoryContext oldCtx;
    PageRedoPipeline *pageLines;
    uint32 pageLineNum;        /* PageLineNum */
    uint32 *chosedPageLineIds; /* chosedPageLineIds */
    uint32 chosedPLCnt;        /* chosedPageLineCount */
    TrxnRedoPipeline trxnLine;
    ReadPipeline readLine;
    RecordBufferState rtoXlogBufState;
    PageRedoWorker **allWorkers; /* Array of page redo workers. */
    uint32 allWorkersCnt;
    RedoItem *freeHead; /* Head of freed-item list. */
    RedoItem *freeStateHead;
    RedoItem *allocatedRedoItem;
    int exitCode; /* Thread exit code. */
    uint64 totalCostTime;
    uint64 txnCostTime; /* txn cost time */
    uint64 pprCostTime;
    uint32 maxItemNum;
    uint32 curItemNum;

    uint32 syncEnterCount;
    uint32 syncExitCount;

    volatile uint32 batchThrdEnterNum;
    volatile uint32 batchThrdExitNum;
    
    volatile uint32 segpageXactDoneFlag;

    pg_atomic_uint32 standbyState; /* sync standbyState from trxn worker to startup */

    bool needImmediateCheckpoint;
    bool needFullSyncCheckpoint;
    volatile sig_atomic_t smartShutdown;
#ifdef USE_ASSERT_CHECKING
    void *originLsnCheckAddr;
    LsnCheckCtl *lsnCheckCtl;
    slock_t updateLck;
#endif
    RedoInterruptCallBackFunc oldStartupIntrruptFunc;
    volatile bool recoveryStop;
    volatile XLogRedoNumStatics xlogStatics[RM_NEXT_ID][MAX_XLOG_INFO_NUM];
    RedoTimeCost *startupTimeCost;
} LogDispatcher;

typedef struct {
    bool (*rm_dispatch)(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime);
    bool (*rm_loginfovalid)(XLogReaderState *record, uint8 minInfo, uint8 maxInfo);
    RmgrId rm_id;
    uint8 rm_mininfo;
    uint8 rm_maxinfo;
} RmgrDispatchData;

extern LogDispatcher *g_dispatcher;
extern RedoItem g_GlobalLsnForwarder;
extern RedoItem g_cleanupMark;
extern THR_LOCAL RecordBufferState *g_recordbuffer;

const static uint64 OUTPUT_WAIT_COUNT = 0x7FFFFFF;
const static uint64 PRINT_ALL_WAIT_COUNT = 0x7FFFFFFFF;
extern RedoItem g_redoEndMark;
extern RedoItem g_terminateMark;
extern uint32 g_startupTriggerState;
extern uint32 g_readManagerTriggerFlag;

inline int get_batch_redo_num()
{
    return g_instance.attr.attr_storage.batch_redo_num;
}

inline int get_page_redo_worker_num_per_manager()
{
    return g_instance.attr.attr_storage.recovery_redo_workers_per_paser_worker;
}

inline int get_trxn_redo_manager_num()
{
    return TRXN_REDO_MANAGER_NUM;
}

inline int get_trxn_redo_worker_num()
{
    return TRXN_REDO_WORKER_NUM;
}

void StartRecoveryWorkers(XLogReaderState *xlogreader, uint32 privateLen);

/* RedoItem lifecycle. */
void DispatchRedoRecordToFile(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime);
void ProcessPendingRecords(bool fullSync = false);
void FreeRedoItem(RedoItem *item);

/* Dispatcher phases. */
void SendRecoveryEndMarkToWorkersAndWaitForFinish(int code);

/* Dispatcher states. */
int GetDispatcherExitCode();
bool DispatchPtrIsNull();
uint32 GetBatchCount();
uint32 GetAllWorkerCount();
PGPROC *StartupPidGetProc(ThreadId pid);
extern void SetStartupBufferPinWaitBufId(int bufid);
extern void GetStartupBufferPinWaitBufId(int *bufids, uint32 len);
void UpdateStandbyState(HotStandbyState newState);

/* Redo end state saved by each page worker. */
void **GetXLogInvalidPagesFromWorkers();

/* Other utility functions. */
uint32 GetSlotId(const RelFileNode node, BlockNumber block, ForkNumber forkNum, uint32 workerCount);
bool XactHasSegpageRelFiles(XLogReaderState *record);
XLogReaderState *NewReaderState(XLogReaderState *readerState);
void FreeAllocatedRedoItem();
List *CheckImcompleteAction(List *imcompleteActionList);
void SetPageWorkStateByThreadId(uint32 threadState);
void GetReplayedRecPtr(XLogRecPtr *startPtr, XLogRecPtr *endPtr);
void StartupSendFowarder(RedoItem *item);
XLogRecPtr GetSafeMinCheckPoint();
RedoWaitInfo redo_get_io_event(int32 event_id);
void redo_get_wroker_statistic(uint32 *realNum, RedoWorkerStatsData *worker, uint32 workerLen);
void CheckCommittingCsnList();
void redo_get_wroker_time_count(RedoWorkerTimeCountsInfo **workerCountInfoList, uint32 *realNum);
void DumpDispatcher();

}  // namespace extreme_rto

extreme_rto::Enum_TriggeredState CheckForSatartupStatus(void);

#endif
