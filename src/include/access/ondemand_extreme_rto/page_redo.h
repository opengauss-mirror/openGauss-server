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
 * page_redo.h
 *
 *
 *
 * IDENTIFICATION
 *        src/include/access/ondemand_extreme_rto/page_redo.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef ONDEMAND_EXTREME_RTO_PAGE_REDO_H
#define ONDEMAND_EXTREME_RTO_PAGE_REDO_H

#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/ondemand_extreme_rto/redo_item.h"
#include "access/ondemand_extreme_rto/batch_redo.h"
#include "nodes/pg_list.h"
#include "storage/proc.h"

#include "access/ondemand_extreme_rto/posix_semaphore.h"
#include "access/ondemand_extreme_rto/spsc_blocking_queue.h"
#include "access/xlogproc.h"
#include "postmaster/pagerepair.h"

namespace ondemand_extreme_rto {

#define ONDEMAND_DISTRIBUTE_RATIO 0.95
#define ONDEMAND_FORCE_PRUNE_RATIO 0.99
#define ONDEMAND_HASHTAB_SWITCH_LIMIT 100000
#define SEG_PROC_PIPELINE_SLOT 0

#define ONDEMAND_HASHMAP_ENTRY_REDO_DONE 0
#define ONDEMAND_HASHMAP_ENTRY_REDOING 1
#define ONDEMAND_HASHMAP_ENTRY_NEED_REDO 2

static const uint32 PAGE_WORK_QUEUE_SIZE = 65536;
static const uint32 REALTIME_BUILD_RECORD_QUEUE_SIZE = 4194304;

static const uint32 ONDEMAND_EXTREME_RTO_ALIGN_LEN = 16; /* need 128-bit aligned */
static const uint32 MAX_REMOTE_READ_INFO_NUM = 100;
static const uint32 ADVANCE_GLOBALLSN_INTERVAL = 1; /* unit second */

extern uint32 g_ondemandXLogParseMemFullValue;
extern uint32 g_ondemandXLogParseMemApproachFullVaule;
extern uint32 g_ondemandRealtimeBuildQueueFullValue;

typedef bool (*OndemandCheckPauseCB)(void);
typedef void (*OndemandRefreshPauseStatusCB)(void);

typedef enum {
    REDO_BATCH,
    REDO_PAGE_MNG,
    REDO_PAGE_WORKER,
    REDO_TRXN_MNG,
    REDO_TRXN_WORKER,
    REDO_READ_WORKER,
    REDO_READ_PAGE_WORKER,
    REDO_READ_MNG,
    REDO_SEG_WORKER,
    REDO_HTAB_MNG,
    REDO_CTRL_WORKER,
    REDO_ROLE_NUM,
} RedoRole;

typedef struct BadBlockRecEnt {
    RepairBlockKey key;
    XLogPhyBlock pblk;
    XLogRecPtr rec_min_lsn;
    XLogRecPtr rec_max_lsn;
    XLogRecParseState *head;
    XLogRecParseState *tail;
} BadBlockRecEnt;

struct PageRedoWorker {
    /*
     * The last successfully applied log record's end position + 1 as an
     * atomic uint64.  The type of a log record's position is XLogRecPtr.
     * Here the position is stored as an uint64 so it can be read and
     * written atomically.
     */
    XLogRecPtr lastReplayedReadRecPtr;
    XLogRecPtr lastReplayedEndRecPtr;
#if (!defined(__x86_64__) && !defined(__aarch64__)) || defined(__USE_SPINLOCK)
    /* protects lastReplayedReadRecPtr and lastReplayedEndRecPtr */
    slock_t ptrLck;
#endif
    PageRedoWorker *selfOrinAddr;
    /* Worker id. */
    uint32 id;
    int index;
    /* Thread id */
    gs_thread_t tid;
    /* The proc struct of this worker thread. */
    PGPROC *proc;
    RedoRole role;
    uint32 slotId;
    bool isUndoSpaceWorker;
    /* ---------------------------------------------
     * Initial context
     *
     * Global variable values at worker creation time.
     */

    /* Initial server mode from the dispatcher. */
    ServerMode initialServerMode;
    /* Initial timeline ID from the dispatcher. */
    TimeLineID initialTimeLineID;
    List *expectedTLIs;
    /* ---------------------------------------------
     * Redo item queue.
     *
     * Redo items are provided by the dispatcher and consumed by each
     * worker.  See AddPageRedoItem() for the use of the additional
     * pending list.
     */

    /* The head of the pending item list. */
    RedoItem *pendingHead;
    /* The tail of the pending item list. */
    RedoItem *pendingTail;
    /* To-be-replayed log-record-list queue. */
    SPSCBlockingQueue *queue;

    /*
     * The last recovery restart point seen by the txn worker.  Restart
     * points before this is useless and can be removed.
     */
    XLogRecPtr lastCheckedRestartPoint;
    /* min recovery point */
    XLogRecPtr minRecoveryPoint;
    /* ---------------------------------------------
     * Per-worker run-time context
     *
     * States maintained by each individual page-redo worker during
     * log replay.  These are read by the txn-redo worker.
     */

    /* ---------------------------------------------
     * Global run-time context
     *
     * States maintained outside page-redo worker during log replay.
     * Updates to these states must be synchronized to all page-redo workers.
     */

    /*
     * Global standbyState set by the txn worker.
     */
    HotStandbyState standbyState;
    TransactionId latestObservedXid;
    bool StandbyMode;
    char *DataDir;

    TransactionId RecentXmin;
    /* ---------------------------------------------
     * Redo end context
     *
     * Thread-local variable values saved after log replay has completed.
     * These values are collected by each redo worker at redo end and
     * are used by the dispatcher.
     */
    /* XLog invalid pages. */
    void *xlogInvalidPages;

    void *committingCsnList;

    /* ---------------------------------------------
     * Phase barrier.
     *
     * A barrier for synchronizing the dispatcher and page redo worker
     * between different phases.
     */

    /* Semaphore marking the completion of the current phase. */
    PosixSemaphore phaseMarker;
    MemoryContext oldCtx;

    ondemand_htab_ctrl_t *redoItemHashCtrl; // The tail of redoItem hashmap, while is actually in use.
    TimeLineID recoveryTargetTLI;
    bool ArchiveRecoveryRequested;
    bool StandbyModeRequested;
    bool InArchiveRecovery;
    bool ArchiveRestoreRequested;
    bool InRecovery;
    char* recoveryRestoreCommand;
    uint32 fullSyncFlag;
    RedoParseManager parseManager;
    RedoBufferManager bufferManager;
    RedoTimeCost timeCostList[TIME_COST_NUM];
    char page[BLCKSZ];

    /* for ondemand realtime build */
    XLogRecPtr nextPrunePtr;
    bool inRealtimeBuild;
    uint32 currentHtabBlockNum;
};


extern THR_LOCAL PageRedoWorker *g_redoWorker;

/* Worker lifecycle. */
PageRedoWorker *StartPageRedoWorker(PageRedoWorker *worker);
void DestroyPageRedoWorker(PageRedoWorker *worker);

/* Thread creation utility functions. */
bool IsPageRedoWorkerProcess(int argc, char *argv[]);
void AdaptArgvForPageRedoWorker(char *argv[]);
void GetThreadNameIfPageRedoWorker(int argc, char *argv[], char **threadNamePtr);

extern bool RedoWorkerIsUndoSpaceWorker();
uint32 GetMyPageRedoWorkerIdWithLock();
PGPROC *GetPageRedoWorkerProc(PageRedoWorker *worker);

/* Worker main function. */
void ParallelRedoThreadRegister();
void ParallelRedoThreadMain();

/* Dispatcher phases. */
bool SendPageRedoEndMark(PageRedoWorker *worker);
void WaitPageRedoWorkerReachLastMark(PageRedoWorker *worker);

/* Redo processing. */
void AddPageRedoItem(PageRedoWorker *worker, void *item);

uint64 GetCompletedRecPtr(PageRedoWorker *worker);
void UpdatePageRedoWorkerStandbyState(PageRedoWorker *worker, HotStandbyState newState);

/* Redo end states. */
void ClearBTreeIncompleteActions(PageRedoWorker *worker);
void *GetXLogInvalidPages(PageRedoWorker *worker);
bool RedoWorkerIsIdle(PageRedoWorker *worker);
void DumpPageRedoWorker(PageRedoWorker *worker);
PageRedoWorker *CreateWorker(uint32 id, bool inRealtimeBuild);
extern void UpdateRecordGlobals(RedoItem *item, HotStandbyState standbyState);
void ReferenceRedoItem(void *item);
void DereferenceRedoItem(void *item);
void ReferenceRecParseState(XLogRecParseState *recordstate);
void DereferenceRecParseState(XLogRecParseState *recordstate);
void PushToWorkerLsn();
void GetCompletedReadEndPtr(PageRedoWorker *worker, XLogRecPtr *readPtr, XLogRecPtr *endPtr);
void SetReadBufferForExtRto(XLogReaderState *state, XLogRecPtr pageptr, int reqLen);
void DumpExtremeRtoReadBuf();
void PutRecordToReadQueue(XLogReaderState *recordreader);
bool LsnUpdate();
void ResetRtoXlogReadBuf(XLogRecPtr targetPagePtr);
bool XLogPageReadForExtRto(XLogReaderState *xlogreader, XLogRecPtr targetPagePtr, int reqLen);
void ExtremeRtoStopHere();
void WaitAllRedoWorkerQueueEmpty();
void WaitAllReplayWorkerIdle();
void DispatchClosefdMarkToAllRedoWorker();
void DispatchCleanInvalidPageMarkToAllRedoWorker(RepairFileKey key);
void ClearRecoveryThreadHashTbl(const RelFileNode &node, ForkNumber forknum, BlockNumber minblkno,
    bool segment_shrink);
void BatchClearRecoveryThreadHashTbl(Oid spcNode, Oid dbNode);
void RecordBadBlockAndPushToRemote(XLogBlockDataParse *datadecode, PageErrorType error_type,
    XLogRecPtr old_lsn, XLogPhyBlock pblk);
const char *RedoWokerRole2Str(RedoRole role);
LWLock* OndemandGetXLogPartitionLock(BufferDesc* bufHdr, ForkNumber forkNum, BlockNumber blockNum);
int checkBlockRedoStateAndTryHashMapLock(BufferDesc* bufHdr, ForkNumber forkNum, BlockNumber blockNum);
bool checkBlockRedoDoneFromHashMapAndLock(LWLock **lock, RedoItemTag redoItemTag, RedoItemHashEntry **redoItemEntry,
    bool holdLock);
void RedoWorkerQueueCallBack();
void OndemandRequestPrimaryDoCkptIfNeed();
void GetOndemandRecoveryStatus(ondemand_recovery_stat *stat);
void ReleaseBlockParseStateIfNotReplay(XLogRecParseState *preState);
bool SSXLogParseRecordNeedReplayInOndemandRealtimeBuild(XLogRecParseState *redoblockstate);

}  // namespace ondemand_extreme_rto
#endif
