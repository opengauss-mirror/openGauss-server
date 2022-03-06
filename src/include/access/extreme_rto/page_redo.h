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
 *        src/include/access/extreme_rto/page_redo.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef EXTREME_RTO_PAGE_REDO_H
#define EXTREME_RTO_PAGE_REDO_H

#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/extreme_rto/redo_item.h"
#include "nodes/pg_list.h"
#include "storage/proc.h"

#include "access/extreme_rto/posix_semaphore.h"
#include "access/extreme_rto/spsc_blocking_queue.h"
#include "access/xlogproc.h"
#include "postmaster/pagerepair.h"

namespace extreme_rto {

static const uint32 PAGE_WORK_QUEUE_SIZE = 8192;

static const uint32 EXTREME_RTO_ALIGN_LEN = 16; /* need 128-bit aligned */
static const uint32 MAX_REMOTE_READ_INFO_NUM = 100;

typedef enum {
    REDO_BATCH,
    REDO_PAGE_MNG,
    REDO_PAGE_WORKER,
    REDO_TRXN_MNG,
    REDO_TRXN_WORKER,
    REDO_READ_WORKER,
    REDO_READ_PAGE_WORKER,
    REDO_READ_MNG,
    REDO_ROLE_NUM,
} RedoRole;

typedef struct BadBlockRecEnt{
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
#if (!defined __x86_64__) && (!defined __aarch64__)
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

    HTAB *redoItemHash;
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
    uint32 remoteReadPageNum;
    HTAB *badPageHashTbl;
    char page[BLCKSZ];
    XLogBlockDataParse *curRedoBlockState;
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

void UpdatePageRedoWorkerStandbyState(PageRedoWorker *worker, HotStandbyState newState);

/* Redo end states. */
void ClearBTreeIncompleteActions(PageRedoWorker *worker);
void *GetXLogInvalidPages(PageRedoWorker *worker);
bool RedoWorkerIsIdle(PageRedoWorker *worker);
void DumpPageRedoWorker(PageRedoWorker *worker);
PageRedoWorker *CreateWorker(uint32 id);
extern void UpdateRecordGlobals(RedoItem *item, HotStandbyState standbyState);
void ReferenceRedoItem(void *item);
void DereferenceRedoItem(void *item);
void PushToWorkerLsn(bool force);
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

const char *RedoWokerRole2Str(RedoRole role);


/* block or file repair function */
HTAB* BadBlockHashTblCreate();
void RepairPageAndRecoveryXLog(BadBlockRecEnt *page_info, const char *page);
void CheckRemoteReadAndRepairPage(BadBlockRecEnt *entry);
void ClearSpecificsPageEntryAndMem(BadBlockRecEnt *entry);
void ClearRecoveryThreadHashTbl(const RelFileNode &node, ForkNumber forknum, BlockNumber minblkno,
    bool segment_shrink);
void BatchClearRecoveryThreadHashTbl(Oid spcNode, Oid dbNode);
void RecordBadBlockAndPushToRemote(XLogBlockDataParse *datadecode, PageErrorType error_type,
    XLogRecPtr old_lsn, XLogPhyBlock pblk);
void SeqCheckRemoteReadAndRepairPage();

}  // namespace extreme_rto
#endif
