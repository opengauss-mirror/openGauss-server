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
 *        src/include/access/parallel_recovery/page_redo.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef PARALLEL_RECOVERY_PAGE_REDO_H
#define PARALLEL_RECOVERY_PAGE_REDO_H

#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/parallel_recovery/redo_item.h"
#include "nodes/pg_list.h"
#include "storage/proc.h"

#include "access/parallel_recovery/posix_semaphore.h"
#include "access/parallel_recovery/spsc_blocking_queue.h"
#include "postmaster/pagerepair.h"

namespace parallel_recovery {


static const uint32 PAGE_WORK_QUEUE_SIZE = 4096;

static const uint32 PAGE_REDO_WORKER_APPLY_ITEM = 0;
static const uint32 PAGE_REDO_WORKER_SKIP_ITEM = 1;
static const uint32 MAX_REMOTE_READ_INFO_NUM = 100;

struct SafeRestartPoint {
    SafeRestartPoint* next;
    XLogRecPtr restartPoint;
};

typedef struct BadBlockRecEnt{
    RepairBlockKey key;
    XLogPhyBlock pblk;
    XLogRecPtr rec_min_lsn;
    XLogRecPtr rec_max_lsn;
    RedoItem *head;
    RedoItem *tail;
} BadBlockRecEnt;

typedef enum {
    DataPageWorker,
    UndoLogZidWorker,
} RedoWorkerRole;
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
    PageRedoWorker* selfOrinAddr;
    /* Worker id. */
    uint32 id;
    /* Worker id. */
    uint32 originId;
    int index;
    /* Thread id */
    gs_thread_t tid;
    /* The proc struct of this worker thread. */
    PGPROC* proc;

    /* Role of page redo worker */
    RedoWorkerRole role;

    bool replay_undo; /* only apply undo */

    /* ---------------------------------------------
     * Initial context
     *
     * Global variable values at worker creation time.
     */

    /* Initial server mode from the dispatcher. */
    ServerMode initialServerMode;
    /* Initial timeline ID from the dispatcher. */
    TimeLineID initialTimeLineID;

    /* ---------------------------------------------
     * Redo item queue.
     *
     * Redo items are provided by the dispatcher and consumed by each
     * worker.  See AddPageRedoItem() for the use of the additional
     * pending list.
     */

    /* The head of the pending item list. */
    RedoItem* pendingHead;
    /* The tail of the pending item list. */
    RedoItem* pendingTail;
    /* To-be-replayed log-record-list queue. */
    SPSCBlockingQueue* queue;

    /* ---------------------------------------------
     * Safe restart point handling.
     */

    /*
     * A list of safe recovery restart point seen by this worker.
     * The restart points are listed in reverse LSN order to ease the
     * lock-free implementation.
     */
    SafeRestartPoint* safePointHead;
    /*
     * The last recovery restart point seen by the txn worker.  Restart
     * points before this is useless and can be removed.
     */
    XLogRecPtr lastCheckedRestartPoint;

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

    char* DataDir;

    TransactionId RecentXmin;
    /* ---------------------------------------------
     * Redo end context
     *
     * Thread-local variable values saved after log replay has completed.
     * These values are collected by each redo worker at redo end and
     * are used by the dispatcher.
     */

    /* B-Tree incomplete actions. */
    void* btreeIncompleteActions;
    /* XLog invalid pages. */
    void* xlogInvalidPages;

    /* ---------------------------------------------
     * Phase barrier.
     *
     * A barrier for synchronizing the dispatcher and page redo worker
     * between different phases.
     */

    /* Semaphore marking the completion of the current phase. */
    PosixSemaphore phaseMarker;

    uint32 statMulpageCnt;
    uint64 statWaitReach;
    uint64 statWaitReplay;
    pg_atomic_uint32 readyStatus;
    MemoryContext oldCtx;
    int bufferPinWaitBufId;
    RedoTimeCost timeCostList[TIME_COST_NUM];
    uint32 remoteReadPageNum;
    HTAB *badPageHashTbl;
    char page[BLCKSZ];
    XLogReaderState *current_item;
};

extern THR_LOCAL PageRedoWorker* g_redoWorker;

/* Worker lifecycle. */
PageRedoWorker* StartPageRedoWorker(uint32 id);
void DestroyPageRedoWorker(PageRedoWorker* worker);

/* Thread creation utility functions. */
bool IsPageRedoWorkerProcess(int argc, char* argv[]);
void AdaptArgvForPageRedoWorker(char* argv[]);
void GetThreadNameIfPageRedoWorker(int argc, char* argv[], char** threadNamePtr);

extern bool DoPageRedoWorkerReplayUndo();

uint32 GetMyPageRedoWorkerOrignId();
PGPROC* GetPageRedoWorkerProc(PageRedoWorker* worker);

/* Worker main function. */
void PageRedoWorkerMain();

/* Dispatcher phases. */
bool SendPageRedoEndMark(PageRedoWorker* worker);
bool SendPageRedoClearMark(PageRedoWorker* worker);
bool SendPageRedoClosefdMark(PageRedoWorker *worker);
bool SendPageRedoCleanInvalidPageMark(PageRedoWorker *worker, RepairFileKey key);

void WaitPageRedoWorkerReachLastMark(PageRedoWorker* worker);

/* Redo processing. */
void AddPageRedoItem(PageRedoWorker* worker, RedoItem* item);
bool ProcessPendingPageRedoItems(PageRedoWorker* worker);

/* Run-time worker states. */
uint64 GetCompletedRecPtr(PageRedoWorker* worker);
bool IsRecoveryRestartPointSafe(PageRedoWorker* worker, XLogRecPtr restartPoint);
void SetWorkerRestartPoint(PageRedoWorker* worker, XLogRecPtr restartPoint);

void UpdatePageRedoWorkerStandbyState(PageRedoWorker* worker, HotStandbyState newState);

/* Redo end states. */
void* GetBTreeIncompleteActions(PageRedoWorker* worker);
void ClearBTreeIncompleteActions(PageRedoWorker* worker);
void* GetXLogInvalidPages(PageRedoWorker* worker);
bool RedoWorkerIsIdle(PageRedoWorker* worker);
void DumpPageRedoWorker(PageRedoWorker* worker);
void SetPageRedoWorkerIndex(int index);
void SetCompletedReadEndPtr(PageRedoWorker* worker, XLogRecPtr readPtr, XLogRecPtr endPtr);
void GetCompletedReadEndPtr(PageRedoWorker* worker, XLogRecPtr *readPtr, XLogRecPtr *endPtr);
void UpdateRecordGlobals(RedoItem* item, HotStandbyState standbyState);
void redo_dump_worker_queue_info();
void WaitAllPageWorkersQueueEmpty();

/* recovery thread handle bad block function */
HTAB* BadBlockHashTblCreate();
void RepairPageAndRecoveryXLog(BadBlockRecEnt *page_info, const char *page);
void RecordBadBlockAndPushToRemote(XLogReaderState *record, RepairBlockKey key,
    PageErrorType error_type, XLogRecPtr old_lsn, XLogPhyBlock pblk);
void CheckRemoteReadAndRepairPage(BadBlockRecEnt *entry);
void SeqCheckRemoteReadAndRepairPage();
void ClearRecoveryThreadHashTbl(const RelFileNode &node, ForkNumber forknum, BlockNumber minblkno,
    bool segment_shrink);
void BatchClearRecoveryThreadHashTbl(Oid spcNode, Oid dbNode);
void ClearSpecificsPageEntryAndMem(BadBlockRecEnt *entry);


}
#endif
