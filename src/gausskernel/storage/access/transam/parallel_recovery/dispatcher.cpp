/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * dispatcher.cpp
 * Parallel recovery has a centralized log dispatcher which runs inside
 * the StartupProcess.  The dispatcher is responsible for managing the
 * life cycle of PageRedoWorkers and the TxnRedoWorker, analyzing log
 * records and dispatching them to workers for processing.
 *
 * IDENTIFICATION
 * src/gausskernel/storage/access/transam/parallel_recovery/dispatcher.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "postmaster/startup.h"
#include "access/clog.h"
#include "access/xact.h"
#include "access/xlog_internal.h"
#include "access/xloginsert.h"
#include "access/nbtree.h"
#include "access/ubtree.h"
#include "access/hash_xlog.h"
#include "access/xlogreader.h"
#include "access/gist_private.h"
#include "access/multixact.h"
#include "access/spgist_private.h"
#include "access/gin_private.h"
#include "access/xlogutils.h"
#include "access/gin.h"
#include "access/ustore/knl_uredo.h"

#include "catalog/storage_xlog.h"
#include "storage/buf/buf_internals.h"
#include "storage/ipc.h"
#include "storage/standby.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/palloc.h"
#include "utils/guc.h"
#include "utils/relmapper.h"

#include "portability/instr_time.h"

#include "access/parallel_recovery/dispatcher.h"
#include "access/parallel_recovery/page_redo.h"
#include "access/multi_redo_api.h"

#include "access/parallel_recovery/txn_redo.h"
#include "access/parallel_recovery/spsc_blocking_queue.h"
#include "access/parallel_recovery/redo_item.h"

#include "catalog/storage.h"
#include <sched.h>
#include "utils/memutils.h"

#include "commands/dbcommands.h"
#include "commands/tablespace.h"
#include "commands/sequence.h"

#include "replication/slot.h"
#include "replication/ddlmessage.h"
#include "gssignal/gs_signal.h"
#include "utils/atomic.h"
#include "pgstat.h"
#include "access/xlogreader.h"

#ifdef PGXC
#include "pgxc/pgxc.h"
#endif

extern THR_LOCAL bool redo_oldversion_xlog;

namespace parallel_recovery {
typedef struct RmgrDispatchData {
    bool (*rm_dispatch)(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime);
    bool (*rm_loginfovalid)(XLogReaderState *record, uint8 minInfo, uint8 maxInfo);
    RmgrId rm_id;
    uint8 rm_mininfo;
    uint8 rm_maxinfo;
} RmgrDispatchData;

LogDispatcher *g_dispatcher = NULL;

static const int XLOG_INFO_SHIFT_SIZE = 4; /* xlog info flag shift size */

static const int32 MAX_PENDING = 1;
static const int32 ITEM_QUQUE_SIZE_RATIO = 5;

static const uint32 EXIT_WAIT_DELAY = 100; /* 100 us */

static const int UNDO_START_BLK = 1;
static const int UHEAP_UPDATE_UNDO_START_BLK = 2; 
static const uint32 XLOG_FPI_FOR_HINT_VERSION_NUM = 92658;
static const XLogRecPtr DISPATCH_FIX_SIZE = (XLogRecPtr)1024 * 1024 * 1024 * 2;

typedef void *(*GetStateFunc)(PageRedoWorker *worker);

static void AddWorkerToSet(uint32);
static void **CollectStatesFromWorkers(GetStateFunc);
static void GetWorkerIds(XLogReaderState *record, uint32 designatedWorker, bool rnodedispatch);
static LogDispatcher *CreateDispatcher();
static void DestroyRecoveryWorkers();

static void DispatchRecordWithPages(XLogReaderState *, List *, bool);
static void DispatchRecordWithoutPage(XLogReaderState *, List *);
static void DispatchTxnRecord(XLogReaderState *, List *, TimestampTz, bool);
static void GetWorkersIdWithOutUndoBuffer(XLogReaderState *record);
static void StartPageRedoWorkers(uint32);
static void StopRecoveryWorkers(int, Datum);
static bool XLogWillChangeStandbyState(XLogReaderState *);
static bool StandbyWillChangeStandbyState(XLogReaderState *);
static void DispatchToSpecPageWorker(XLogReaderState *record, List *expectedTLIs, bool waittrxnsync);

static bool DispatchXLogRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime);
static bool DispatchXactRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime);
static bool DispatchSmgrRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime);
static bool DispatchCLogRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime);
static bool DispatchHashRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime);
static bool DispatchCompresseShrinkRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime);
static bool DispatchDataBaseRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime);
static bool DispatchTableSpaceRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime);
static bool DispatchMultiXactRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime);
static bool DispatchRelMapRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime);
static bool DispatchStandbyRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime);
static bool DispatchHeap2Record(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime);
static bool DispatchHeapRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime);
static bool DispatchSeqRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime);
static bool DispatchGinRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime);
static bool DispatchGistRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime);
static bool DispatchSpgistRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime);
static bool DispatchRepSlotRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime);
static bool DispatchHeap3Record(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime);
static bool DispatchDefaultRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime);
static bool DispatchBarrierRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime);

static bool DispatchBtreeRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime);

static bool DispatchUBTreeRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime);
static bool DispatchUBTree2Record(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime);

#ifdef ENABLE_MOT
static bool DispatchMotRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime);
#endif
static bool DispatchSegpageSmgrRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime);
static bool DispatchRepOriginRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime);

static bool RmgrRecordInfoValid(XLogReaderState *record, uint8 minInfo, uint8 maxInfo);
static bool RmgrGistRecordInfoValid(XLogReaderState *record, uint8 minInfo, uint8 maxInfo);
/* Ustore table */
static bool DispatchUHeapRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime);
static bool DispatchUHeap2Record(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime);
static bool DispatchUHeapUndoRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime);
static bool DispatchUndoActionRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime);
static bool DispatchRollbackFinishRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime);
static bool DispatchLogicalDDLMsgRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime);
static uint32 GetUndoSpaceWorkerId(int zid);
static void HandleStartupProcInterruptsForParallelRedo(void);
static bool timeoutForDispatch(void);

RedoWaitInfo redo_get_io_event(int32 event_id);

/* dispatchTable must consistent with RmgrTable */
static const RmgrDispatchData g_dispatchTable[RM_MAX_ID + 1] = {
    { DispatchXLogRecord, RmgrRecordInfoValid, RM_XLOG_ID, XLOG_CHECKPOINT_SHUTDOWN, XLOG_DELAY_XLOG_RECYCLE },
    { DispatchXactRecord, RmgrRecordInfoValid, RM_XACT_ID, XLOG_XACT_COMMIT, XLOG_XACT_ABORT_WITH_XID },
    { DispatchSmgrRecord, RmgrRecordInfoValid, RM_SMGR_ID, XLOG_SMGR_CREATE, XLOG_SMGR_TRUNCATE },
    { DispatchCLogRecord, RmgrRecordInfoValid, RM_CLOG_ID, CLOG_ZEROPAGE, CLOG_TRUNCATE },
    { DispatchDataBaseRecord, RmgrRecordInfoValid, RM_DBASE_ID, XLOG_DBASE_CREATE, XLOG_DBASE_DROP },
    { DispatchTableSpaceRecord, RmgrRecordInfoValid, RM_TBLSPC_ID, XLOG_TBLSPC_CREATE, XLOG_TBLSPC_RELATIVE_CREATE },
    { DispatchMultiXactRecord, RmgrRecordInfoValid,
      RM_MULTIXACT_ID, XLOG_MULTIXACT_ZERO_OFF_PAGE, XLOG_MULTIXACT_CREATE_ID },
    { DispatchRelMapRecord, RmgrRecordInfoValid, RM_RELMAP_ID, XLOG_RELMAP_UPDATE, XLOG_RELMAP_UPDATE },
    { DispatchStandbyRecord, RmgrRecordInfoValid, RM_STANDBY_ID, XLOG_STANDBY_LOCK, XLOG_STANDBY_CSN_ABORTED},

    { DispatchHeap2Record, RmgrRecordInfoValid, RM_HEAP2_ID, XLOG_HEAP2_FREEZE, XLOG_HEAP2_LOGICAL_NEWPAGE },
    { DispatchHeapRecord, RmgrRecordInfoValid, RM_HEAP_ID, XLOG_HEAP_INSERT, XLOG_HEAP_INPLACE },
    { DispatchBtreeRecord, RmgrRecordInfoValid, RM_BTREE_ID, XLOG_BTREE_INSERT_LEAF, XLOG_BTREE_DEDUP },
    { DispatchHashRecord, RmgrRecordInfoValid, RM_HASH_ID, XLOG_HASH_INIT_META_PAGE, XLOG_HASH_VACUUM_ONE_PAGE },
    { DispatchGinRecord, RmgrRecordInfoValid, RM_GIN_ID, XLOG_GIN_CREATE_INDEX, XLOG_GIN_VACUUM_DATA_LEAF_PAGE },
    /* XLOG_GIST_PAGE_DELETE is not used and info isn't continus  */
    { DispatchGistRecord, RmgrGistRecordInfoValid, RM_GIST_ID, 0, 0 },
    { DispatchSeqRecord, RmgrRecordInfoValid, RM_SEQ_ID, XLOG_SEQ_LOG, XLOG_SEQ_LOG },
    { DispatchSpgistRecord, RmgrRecordInfoValid, RM_SPGIST_ID, XLOG_SPGIST_CREATE_INDEX, XLOG_SPGIST_VACUUM_REDIRECT },
    { DispatchRepSlotRecord, RmgrRecordInfoValid, RM_SLOT_ID, XLOG_SLOT_CREATE, XLOG_TERM_LOG },
    { DispatchHeap3Record, RmgrRecordInfoValid, RM_HEAP3_ID, XLOG_HEAP3_NEW_CID, XLOG_HEAP3_INVALID },
    { DispatchBarrierRecord, RmgrRecordInfoValid, RM_BARRIER_ID, XLOG_BARRIER_CREATE, XLOG_BARRIER_SWITCHOVER },

#ifdef ENABLE_MOT
    { DispatchMotRecord, NULL, RM_MOT_ID, 0, 0},
#endif
    { DispatchUHeapRecord, RmgrRecordInfoValid, RM_UHEAP_ID, XLOG_UHEAP_INSERT, XLOG_UHEAP_MULTI_INSERT },
    { DispatchUHeap2Record, RmgrRecordInfoValid, RM_UHEAP2_ID, XLOG_UHEAP2_BASE_SHIFT, XLOG_UHEAP2_EXTEND_TD_SLOTS },
    { DispatchUHeapUndoRecord, RmgrRecordInfoValid, RM_UNDOLOG_ID, XLOG_UNDO_EXTEND, XLOG_UNDO_DISCARD },
    { DispatchUndoActionRecord, RmgrRecordInfoValid, RM_UHEAPUNDO_ID, 
        XLOG_UHEAPUNDO_PAGE, XLOG_UHEAPUNDO_ABORT_SPECINSERT },
    { DispatchRollbackFinishRecord, RmgrRecordInfoValid, RM_UNDOACTION_ID, XLOG_ROLLBACK_FINISH, XLOG_ROLLBACK_FINISH },
    { DispatchUBTreeRecord, RmgrRecordInfoValid, RM_UBTREE_ID, XLOG_UBTREE_INSERT_LEAF, XLOG_UBTREE_PRUNE_PAGE},
    { DispatchUBTree2Record, RmgrRecordInfoValid, RM_UBTREE2_ID, XLOG_UBTREE2_SHIFT_BASE,
        XLOG_UBTREE2_FREEZE },
    { DispatchSegpageSmgrRecord, RmgrRecordInfoValid, RM_SEGPAGE_ID, XLOG_SEG_ATOMIC_OPERATION, 
        XLOG_SEG_NEW_PAGE },
    { DispatchRepOriginRecord, RmgrRecordInfoValid, RM_REPLORIGIN_ID, XLOG_REPLORIGIN_SET, XLOG_REPLORIGIN_DROP },
    { DispatchCompresseShrinkRecord, RmgrRecordInfoValid, RM_COMPRESSION_REL_ID, XLOG_CFS_SHRINK_OPERATION,
        XLOG_CFS_SHRINK_OPERATION },
    { DispatchLogicalDDLMsgRecord, RmgrRecordInfoValid, RM_LOGICALDDLMSG_ID, XLOG_LOGICAL_DDL_MESSAGE,
        XLOG_LOGICAL_DDL_MESSAGE },
};

/* Run from the dispatcher and txn worker thread. */
bool OnHotStandBy()
{
    return t_thrd.xlog_cxt.standbyState >= STANDBY_INITIALIZED;
}

void RearrangeWorkers()
{
    PageRedoWorker *tmpReadyPageWorkers[MOST_FAST_RECOVERY_LIMIT] = {};
    PageRedoWorker *tmpUnReadyPageWorkers[MOST_FAST_RECOVERY_LIMIT] = {};

    uint32 nextReadyIndex = 0;
    uint32 nextunReadyIndex = 0;
    for (uint32 i = 0; i < g_instance.comm_cxt.predo_cxt.totalNum; ++i) {
        uint32 state = pg_atomic_read_u32(&(g_instance.comm_cxt.predo_cxt.pageRedoThreadStatusList[i].threadState));
        if (state == PAGE_REDO_WORKER_READY) {
            tmpReadyPageWorkers[nextReadyIndex] = g_dispatcher->pageWorkers[i];
            ++nextReadyIndex;
        } else {
            tmpUnReadyPageWorkers[nextunReadyIndex] = g_dispatcher->pageWorkers[i];
            ++nextunReadyIndex;
        }
    }

    for (uint32 i = 0; i < nextReadyIndex; ++i) {
        ereport(LOG, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
            errmsg("RearrangeWorkers, rearrange ready workers originWorkerId :%u, threadId:%lu, "
                "newWorkerId:%u",
                tmpReadyPageWorkers[i]->id, tmpReadyPageWorkers[i]->tid.thid, i)));
        g_dispatcher->pageWorkers[i] = tmpReadyPageWorkers[i];
        g_dispatcher->pageWorkers[i]->id = i;
    }

    for (uint32 i = 0; i < nextunReadyIndex; ++i) {
        ereport(LOG, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
            errmsg("RearrangeWorkers, rearrange ready workers originWorkerId :%u, threadId:%lu, "
                "newWorkerId:%u",
                tmpUnReadyPageWorkers[i]->id, tmpUnReadyPageWorkers[i]->tid.thid, i)));
        g_dispatcher->pageWorkers[i + nextReadyIndex] = tmpUnReadyPageWorkers[i];
    }

    g_dispatcher->pageWorkerCount = nextReadyIndex;
}

const int REDO_WAIT_SLEEP_TIME = 5000; /* 5ms */
const int MAX_REDO_WAIT_LOOP = 24000;  /* 5ms*24000 is 2min */

uint32 GetReadyWorker()
{
    uint32 readyWorkerCnt = 0;

    for (uint32 i = 0; i < g_instance.comm_cxt.predo_cxt.totalNum; i++) {
        uint32 state = pg_atomic_read_u32(&(g_instance.comm_cxt.predo_cxt.pageRedoThreadStatusList[i].threadState));
        if (state == PAGE_REDO_WORKER_READY) {
            ++readyWorkerCnt;
        }
    }
    return readyWorkerCnt;
}

void WaitWorkerReady()
{
    uint32 waitLoop = 0;
    uint32 readyWorkerCnt = 0;
    /* MAX wait 2min */
    for (waitLoop = 0; waitLoop < MAX_REDO_WAIT_LOOP; ++waitLoop) {
        readyWorkerCnt = GetReadyWorker();
        if (readyWorkerCnt == g_instance.comm_cxt.predo_cxt.totalNum) {
            ereport(LOG, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                errmsg("WaitWorkerReady total worker count:%u, readyWorkerCnt:%u", g_dispatcher->totalWorkerCount,
                    readyWorkerCnt)));
            break;
        }
        pg_usleep(REDO_WAIT_SLEEP_TIME);
    }
    SpinLockAcquire(&(g_instance.comm_cxt.predo_cxt.rwlock));
    g_instance.comm_cxt.predo_cxt.state = REDO_STARTING_END;
    SpinLockRelease(&(g_instance.comm_cxt.predo_cxt.rwlock));
    readyWorkerCnt = GetReadyWorker();
    if (waitLoop == MAX_REDO_WAIT_LOOP && readyWorkerCnt == 0) {
        ereport(PANIC, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
            errmsg("WaitWorkerReady failed, no worker is ready for work. totalWorkerCount :%u",
                g_dispatcher->totalWorkerCount)));
    }

    ereport(LOG, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
        errmsg("WaitWorkerReady total worker count:%u, readyWorkerCnt:%u", g_dispatcher->totalWorkerCount,
            readyWorkerCnt)));
    RearrangeWorkers();
}

void CheckAlivePageWorkers()
{
    for (uint32 i = 0; i < MOST_FAST_RECOVERY_LIMIT; ++i) {
        if (g_instance.comm_cxt.predo_cxt.pageRedoThreadStatusList[i].threadState != PAGE_REDO_WORKER_INVALID) {
            ereport(PANIC, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                errmsg("CheckAlivePageWorkers: thread %lu is still alive",
                    g_instance.comm_cxt.predo_cxt.pageRedoThreadStatusList[i].threadId)));
        }
        g_instance.comm_cxt.predo_cxt.pageRedoThreadStatusList[i].threadId = 0;
    }
    g_instance.comm_cxt.predo_cxt.totalNum = 0;
}

/* Run from the dispatcher thread. */
void StartRecoveryWorkers(XLogRecPtr startLsn)
{
    if (get_real_recovery_parallelism() > 1) {
        CheckAlivePageWorkers();
        g_dispatcher = CreateDispatcher();
        g_dispatcher->oldCtx = MemoryContextSwitchTo(g_instance.comm_cxt.predo_cxt.parallelRedoCtx);
        g_dispatcher->txnWorker = StartTxnRedoWorker();
        if (g_dispatcher->txnWorker != NULL) {
            Assert(g_instance.comm_cxt.predo_cxt.buffer_pin_wait_buf_len == 0 ||
                   g_instance.comm_cxt.predo_cxt.buffer_pin_wait_buf_len == get_real_recovery_parallelism());
            if (g_instance.comm_cxt.predo_cxt.buffer_pin_wait_buf_ids == NULL) {
                g_instance.comm_cxt.predo_cxt.buffer_pin_wait_buf_ids = (int *)MemoryContextAllocZero(
                    INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), get_real_recovery_parallelism() * sizeof(int));
                g_instance.comm_cxt.predo_cxt.buffer_pin_wait_buf_len = get_real_recovery_parallelism();
            }
            StartPageRedoWorkers(get_real_recovery_parallelism());
        }
        ereport(LOG, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
            errmsg("[PR]: max=%d, thrd=%d, workers=%u", g_instance.attr.attr_storage.max_recovery_parallelism,
                get_real_recovery_parallelism(), g_dispatcher->pageWorkerCount)));
        WaitWorkerReady();
        SpinLockAcquire(&(g_instance.comm_cxt.predo_cxt.rwlock));
        g_instance.comm_cxt.predo_cxt.state = REDO_IN_PROGRESS;
        SpinLockRelease(&(g_instance.comm_cxt.predo_cxt.rwlock));
        g_dispatcher->dispatchFix.lastCheckLsn = startLsn;
        on_shmem_exit(StopRecoveryWorkers, 0);
    }
}

void DumpDispatcher()
{
    knl_parallel_redo_state state;
    state = g_instance.comm_cxt.predo_cxt.state;
    if ((get_real_recovery_parallelism() > 1) && (GetPageWorkerCount() > 0)) {
        ereport(LOG, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
            errmsg("[REDO_LOG_TRACE]dispatcher : pageWorkerCount %u, state %u, curItemNum %u, maxItemNum %u",
                g_dispatcher->pageWorkerCount, (uint32)state, g_dispatcher->curItemNum, g_dispatcher->maxItemNum)));

        for (uint32 i = 0; i < g_dispatcher->pageWorkerCount; ++i) {
            DumpPageRedoWorker(g_dispatcher->pageWorkers[i]);
        }

        DumpTxnWorker(g_dispatcher->txnWorker);
    }
}

List *CheckImcompleteAction(List *imcompleteActionList)
{
    uint32 npageworkers = GetPageWorkerCount();
    for (uint32 i = 0; i < npageworkers; ++i) {
        List *perWorkerList = (List *)GetBTreeIncompleteActions(g_dispatcher->pageWorkers[i]);
        imcompleteActionList = lappend3(imcompleteActionList, perWorkerList);

        /* memory leak */
        ClearBTreeIncompleteActions(g_dispatcher->pageWorkers[i]);
    }
    return imcompleteActionList;
}

/* Run from the dispatcher thread. */
static LogDispatcher *CreateDispatcher()
{
    MemoryContext ctx = AllocSetContextCreate(g_instance.instance_context, "ParallelRecoveryDispatcher",
        ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE, SHARED_CONTEXT);

    LogDispatcher *newDispatcher = (LogDispatcher *)MemoryContextAllocZero(ctx, sizeof(LogDispatcher));

    g_instance.comm_cxt.predo_cxt.parallelRedoCtx = ctx;
    SpinLockAcquire(&(g_instance.comm_cxt.predo_cxt.rwlock));
    g_instance.comm_cxt.predo_cxt.state = REDO_STARTING_BEGIN;
    SpinLockRelease(&(g_instance.comm_cxt.predo_cxt.rwlock));

    newDispatcher->totalCostTime = 0;
    newDispatcher->txnCostTime = 0;
    newDispatcher->pprCostTime = 0;
    newDispatcher->dispatchReadRecPtr = 0;
    newDispatcher->dispatchEndRecPtr = 0;
    newDispatcher->startupTimeCost = t_thrd.xlog_cxt.timeCost;
    newDispatcher->full_sync_dispatch = !g_instance.attr.attr_storage.enable_batch_dispatch;
    return newDispatcher;
}

/* Run from the dispatcher thread. */
static void StartPageRedoWorkers(uint32 parallelism)
{
    g_dispatcher->pageWorkers = (PageRedoWorker **)palloc(sizeof(PageRedoWorker *) * parallelism);

    /* This is necessary to avoid the cache coherence problem. */
    /* Because we are using atomic operation to do the synchronization. */
    uint32 started;
    for (started = 0; started < parallelism; started++) {
        g_dispatcher->pageWorkers[started] = StartPageRedoWorker(started);
        if (g_dispatcher->pageWorkers[started] == NULL)
            break;
    }

    if (started == 0) {
        ereport(PANIC, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
            errmsg("[REDO_LOG_TRACE]StartPageRedoWorkers we need at least one worker thread")));
    }

    g_dispatcher->totalWorkerCount = started;
    g_instance.comm_cxt.predo_cxt.totalNum = started;
    /* (worker num + txn) * (per thread queue num) * 10 */
    g_dispatcher->maxItemNum = (started + 1) * PAGE_WORK_QUEUE_SIZE * ITEM_QUQUE_SIZE_RATIO;

    g_dispatcher->chosedWorkerIds = (uint32 *)palloc(sizeof(uint32) * started);

    g_dispatcher->chosedWorkerCount = 0;
    g_dispatcher->oldStartupIntrruptFunc = RegisterRedoInterruptCallBack(HandleStartupProcInterruptsForParallelRedo);
}

static void ResetChosedWorkerList()
{
    g_dispatcher->chosedWorkerCount = 0;

    for (uint32 i = 0; i < g_dispatcher->pageWorkerCount; ++i) {
        g_dispatcher->chosedWorkerIds[i] = 0;
    }
}

bool DispathCouldExit()
{
    for (uint32 i = 0; i < g_instance.comm_cxt.predo_cxt.totalNum; ++i) {
        uint32 state = pg_atomic_read_u32(&(g_instance.comm_cxt.predo_cxt.pageRedoThreadStatusList[i].threadState));
        if (state == PAGE_REDO_WORKER_READY) {
            return false;
        }
    }

    return true;
}

void SetPageWorkStateByThreadId(uint32 threadState)
{
    gs_thread_t curThread = gs_thread_get_cur_thread();
    for (uint32 i = 0; i < g_instance.comm_cxt.predo_cxt.totalNum; ++i) {
        if (g_instance.comm_cxt.predo_cxt.pageRedoThreadStatusList[i].threadId == curThread.thid) {
            pg_atomic_write_u32(&(g_instance.comm_cxt.predo_cxt.pageRedoThreadStatusList[i].threadState), threadState);
            break;
        }
    }
}

void SendSingalToPageWorker(int signal)
{
    for (uint32 i = 0; i < g_instance.comm_cxt.predo_cxt.totalNum; ++i) {
        uint32 state = pg_atomic_read_u32(&(g_instance.comm_cxt.predo_cxt.pageRedoThreadStatusList[i].threadState));
        if (state == PAGE_REDO_WORKER_READY) {
            int err = gs_signal_send(g_instance.comm_cxt.predo_cxt.pageRedoThreadStatusList[i].threadId, signal);
            if (0 != err) {
                ereport(WARNING, (errmsg("Dispatch kill(pid %lu, signal %d) failed: \"%s\",",
                    g_instance.comm_cxt.predo_cxt.pageRedoThreadStatusList[i].threadId, signal, gs_strerror(err))));
            }
        }
    }
}

/* Run from the dispatcher thread. */
static void StopRecoveryWorkers(int code, Datum arg)
{
    ereport(LOG, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
        errmsg("parallel redo workers are going to stop, "
            "code:%d, arg:%lu",
            code, DatumGetUInt64(arg))));
    SendSingalToPageWorker(SIGTERM);

    uint64 count = 0;
    while (!DispathCouldExit()) {
        ++count;
        if ((count & OUTPUT_WAIT_COUNT) == OUTPUT_WAIT_COUNT) {
            ereport(WARNING,
                (errmodule(MOD_REDO), errcode(ERRCODE_LOG), errmsg("StopRecoveryWorkers wait page work exit")));
            if ((count & PRINT_ALL_WAIT_COUNT) == PRINT_ALL_WAIT_COUNT) {
                DumpDispatcher();
                ereport(PANIC,
                    (errmodule(MOD_REDO), errcode(ERRCODE_LOG), errmsg("StopRecoveryWorkers wait too long!!!")));
            }
            pg_usleep(EXIT_WAIT_DELAY);
        }
    }

    FreeAllocatedRedoItem();
    DestroyRecoveryWorkers();
    ereport(LOG, (errmodule(MOD_REDO), errcode(ERRCODE_LOG), errmsg("parallel redo(startup) thread exit")));
}

/* Run from the dispatcher thread. */
static void DestroyRecoveryWorkers()
{
    SpinLockAcquire(&(g_instance.comm_cxt.predo_cxt.destroy_lock));
    if (g_dispatcher != NULL) {
        for (uint32 i = 0; i < g_dispatcher->totalWorkerCount; i++)
            DestroyPageRedoWorker(g_dispatcher->pageWorkers[i]);
        if (g_dispatcher->txnWorker != NULL)
            DestroyTxnRedoWorker(g_dispatcher->txnWorker);
        if (g_dispatcher->chosedWorkerIds != NULL) {
            pfree(g_dispatcher->chosedWorkerIds);
            g_dispatcher->chosedWorkerIds = NULL;
        }
        if (get_real_recovery_parallelism() > 1) {
            MemoryContextSwitchTo(g_dispatcher->oldCtx);
            MemoryContextDelete(g_instance.comm_cxt.predo_cxt.parallelRedoCtx);
            g_instance.comm_cxt.predo_cxt.parallelRedoCtx = NULL;
        }
        g_dispatcher = NULL;
    }
    SpinLockRelease(&(g_instance.comm_cxt.predo_cxt.destroy_lock));
}

static bool RmgrRecordInfoValid(XLogReaderState *record, uint8 minInfo, uint8 maxInfo)
{
    uint8 info = (XLogRecGetInfo(record) & (~XLR_INFO_MASK));

    if ((XLogRecGetRmid(record) == RM_HEAP2_ID) || (XLogRecGetRmid(record) == RM_HEAP_ID)) {
        info = (info & XLOG_HEAP_OPMASK);
    }
    if (XLogRecGetRmid(record) == RM_MULTIXACT_ID) {
        info = (info & XLOG_MULTIXACT_MASK);
    }

    if ((XLogRecGetRmid(record) == RM_UHEAP_ID) || (XLogRecGetRmid(record) == RM_UNDOLOG_ID) ||
        (XLogRecGetRmid(record) == RM_UHEAPUNDO_ID) || (XLogRecGetRmid(record) == RM_UNDOACTION_ID)) {
        info = (info & XLOG_UHEAP_OPMASK);
    }

    info = (info >> XLOG_INFO_SHIFT_SIZE);
    minInfo = (minInfo >> XLOG_INFO_SHIFT_SIZE);
    maxInfo = (maxInfo >> XLOG_INFO_SHIFT_SIZE);

    if ((info >= minInfo) && (info <= maxInfo)) {
        return true;
    }
    return false;
}

static bool RmgrGistRecordInfoValid(XLogReaderState *record, uint8 minInfo, uint8 maxInfo)
{
    uint8 info = (XLogRecGetInfo(record) & (~XLR_INFO_MASK));
    if ((info == XLOG_GIST_PAGE_UPDATE) || (info == XLOG_GIST_PAGE_SPLIT) || (info == XLOG_GIST_CREATE_INDEX)) {
        return true;
    }
    return false;
}

static bool timeoutForDispatch(void)
{
    int         parallel_recovery_timeout = 0;
    TimestampTz current_time = 0;
    TimestampTz dispatch_limit_time = 0;

    current_time = GetCurrentTimestamp();
    
    parallel_recovery_timeout = g_instance.attr.attr_storage.parallel_recovery_timeout;
    dispatch_limit_time = TimestampTzPlusMilliseconds(g_dispatcher->lastDispatchTime,
                                                            parallel_recovery_timeout);
    if(current_time >= dispatch_limit_time)
        return true;
    return false;
}

void CheckDispatchCount(XLogRecPtr lastCheckLsn)
{
    uint64 maxCount = 0;
    uint64 totalCount = 0;
    g_dispatcher->dispatchFix.lastCheckLsn = lastCheckLsn;
    for (uint32 i = 0; i < g_dispatcher->pageWorkerCount; ++i) {
        PageRedoWorker *worker = g_dispatcher->pageWorkers[i];
        uint64 totalCnt = pg_atomic_read_u64(&worker->queue->totalCnt);
        uint64 incCount = totalCnt - worker->queue->lastTotalCnt;
        if (incCount > maxCount) {
            maxCount = incCount;
        }
        worker->queue->lastTotalCnt = totalCnt;
        totalCount += incCount;
    }

    if (totalCount == 0) {
        return;
    }

    const uint64 persent = 100;
    const uint64 scale = 74;
    uint64 currentScale = maxCount * persent / totalCount;
    // if one thread redo 80% records, we should adjust it
    if (currentScale > scale) {
        ApplyReadyTxnLogRecords(g_dispatcher->txnWorker, true);
        g_dispatcher->dispatchFix.dispatchRandom = (int)random();
        ereport(LOG, (errmodule(MOD_REDO), errmsg("[REDO_LOG_TRACE]CheckDispatchCount config random to %d",
            g_dispatcher->dispatchFix.dispatchRandom)));
    }
}



/* Run from the dispatcher thread. */
void DispatchRedoRecordToFile(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime)
{
    bool isNeedFullSync = false;
    bool fatalerror = false;
    uint32 indexid = (uint32)-1;
    uint32 rmid = XLogRecGetRmid(record);
    uint32 term = XLogRecGetTerm(record);
    int dispatch_batch = 0;

    if (term > g_instance.comm_cxt.localinfo_cxt.term_from_xlog) {
        g_instance.comm_cxt.localinfo_cxt.term_from_xlog = term;
    }
    t_thrd.xlog_cxt.redoItemIdx = 0;
    if ((get_real_recovery_parallelism() > 1) && (GetPageWorkerCount() > 0)) {
        if (rmid <= RM_MAX_ID) {
            indexid = g_dispatchTable[rmid].rm_id;
            if ((indexid != rmid) ||
                ((g_dispatchTable[rmid].rm_loginfovalid != NULL) && (g_dispatchTable[rmid].rm_loginfovalid(record,
                    g_dispatchTable[rmid].rm_mininfo, g_dispatchTable[rmid].rm_maxinfo) == false))) {
                /* it's invalid info */
                fatalerror = true;
            }
        } else {
            fatalerror = true;
        }

        ResetChosedWorkerList();

        if (fatalerror != true) {
            isNeedFullSync = g_dispatchTable[rmid].rm_dispatch(record, expectedTLIs, recordXTime);
        } else {
            isNeedFullSync = DispatchDefaultRecord(record, expectedTLIs, recordXTime);
            isNeedFullSync = true;
        }

        g_dispatcher->dispatchReadRecPtr = record->ReadRecPtr;
        g_dispatcher->dispatchEndRecPtr = record->EndRecPtr;

        dispatch_batch = g_instance.attr.attr_storage.enable_batch_dispatch ?
                                            g_instance.attr.attr_storage.parallel_recovery_batch : 1;
        if (isNeedFullSync)
            ProcessPendingRecords(true);
        else if (++g_dispatcher->pendingCount >= dispatch_batch || timeoutForDispatch()) {
            ProcessPendingRecords();
            if ((g_dispatcher->dispatchEndRecPtr - g_dispatcher->dispatchFix.lastCheckLsn) > DISPATCH_FIX_SIZE) {
                CheckDispatchCount(g_dispatcher->dispatchEndRecPtr);
            }
        }

        if (fatalerror == true) {
            /* output panic error info */
            DumpDispatcher();
            ereport(PANIC, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                errmsg("[REDO_LOG_TRACE]DispatchRedoRecord encounter fatal error:rmgrID:%u, info:%u, indexid:%u", rmid,
                    (uint32)XLogRecGetInfo(record), indexid)));
        }
    } else {
        ereport(PANIC, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
            errmsg("[REDO_LOG_TRACE]DispatchRedoRecord could not be here config recovery num %d, work num %u",
                get_real_recovery_parallelism(), GetPageWorkerCount())));
    }
}

/* *
 * process record need sync with page worker and trxn thread
 * trxnthreadexe is true when the record need execute on trxn thread
 * pagethredexe is true when the record need execute on pageworker thread
 */
static void DispatchSyncTxnRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime,
    uint32 designatedWorker)
{
    RedoItem *item = CreateRedoItem(record, (g_dispatcher->chosedWorkerCount + 1), designatedWorker, expectedTLIs,
        recordXTime, true);

    item->sharewithtrxn = true;
    item->blockbytrxn = false;

    if ((g_dispatcher->chosedWorkerCount != 1) && (XLogRecGetRmid(&item->record) != RM_XACT_ID)) {
        ereport(WARNING, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
            errmsg("[REDO_LOG_TRACE]DispatchSyncTxnRecord maybe some error:rmgrID:%u, info:%u, workerCount:%u",
                (uint32)XLogRecGetRmid(&item->record), (uint32)XLogRecGetInfo(&item->record),
                g_dispatcher->chosedWorkerCount)));
    }

    for (uint32 i = 0; i < g_dispatcher->pageWorkerCount; ++i) {
        if (g_dispatcher->chosedWorkerIds[i] > 0) {
            AddPageRedoItem(g_dispatcher->pageWorkers[i], item);
        }
    }

    /* ensure eyery pageworker is receive recored to update pageworker Lsn
     * trxn record's recordtime must set , see SetLatestXTime
     */
    AddTxnRedoItem(g_dispatcher->txnWorker, item);
    return;
}

static void DispatchToOnePageWorker(XLogReaderState *record, const RelFileNode &rnode, List *expectedTLIs)
{
    /* for bcm different attr need to dispath to the same page redo thread */
    uint32 workerId = GetWorkerId(rnode, 0, 0);
    AddPageRedoItem(g_dispatcher->pageWorkers[workerId], CreateRedoItem(record, 1, ANY_WORKER, expectedTLIs, 0, true));
}

/* *
* The transaction worker waits until every page worker has replayed
* all records before this.  We dispatch a LSN marker to every page
* worker so they can update their progress.
*
* We need to dispatch to page workers first, because the transaction
* worker runs in the dispatcher thread and may block wait on page
* workers.
* ensure eyery pageworker is receive recored to update pageworker Lsn
* trxn record's recordtime must set , see SetLatestXTime

*/
static void DispatchTxnRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime, bool imcheckpoint)
{
    if (g_instance.attr.attr_storage.enable_batch_dispatch) {
        for (uint32 i = 0; i < g_dispatcher->pageWorkerCount; i++) {
            RedoItem *item = CreateLSNMarker(record, expectedTLIs, false);
            AddPageRedoItem(g_dispatcher->pageWorkers[i], item);
        }
    }
    RedoItem *trxnItem = CreateRedoItem(record, 1, ANY_WORKER, expectedTLIs, recordXTime, true);
    trxnItem->imcheckpoint = imcheckpoint; /* immdiate checkpoint set imcheckpoint  */
    AddTxnRedoItem(g_dispatcher->txnWorker, trxnItem);
}

static void GetWorkersIdWithOutUndoBuffer(XLogReaderState *record)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    uint8 op = info & XLOG_UHEAP_OPMASK;
    int undoStartingBlk = UNDO_START_BLK;

    if (op == XLOG_UHEAP_UPDATE) {
        undoStartingBlk = UHEAP_UPDATE_UNDO_START_BLK;
    }

    for (int i = 0; i < undoStartingBlk; i++) {
        DecodedBkpBlock *block = &record->blocks[i];
        uint32 pageWorkerId = 0;

        if (!(block->in_use))
            continue;
        /* Dispatch by relfilenode or page */
        if (SUPPORT_FPAGE_DISPATCH && !SUPPORT_USTORE_UNDO_WORKER)
            pageWorkerId = GetWorkerId(block->rnode, 0, 0);
        else
            pageWorkerId = GetWorkerId(block->rnode, block->blkno, 0);

        AddWorkerToSet(pageWorkerId);
    }
}

/* Run  from the dispatcher thread. */
static bool DispatchBarrierRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime)
{
    DispatchTxnRecord(record, expectedTLIs, recordXTime, false);
    return true;
}

/* Run  from the dispatcher thread. */
static bool DispatchRepSlotRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime)
{
    DispatchTxnRecord(record, expectedTLIs, recordXTime, false);
    return false;
}

/* Run  from the dispatcher thread. */
static bool DispatchHeap3Record(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime)
{
    uint8 info = ((XLogRecGetInfo(record) & (~XLR_INFO_MASK)) & XLOG_HEAP_OPMASK);

    if (info == XLOG_HEAP3_INVALID) {
        DispatchRecordWithPages(record, expectedTLIs, SUPPORT_FPAGE_DISPATCH);
    } else {
        DispatchTxnRecord(record, expectedTLIs, recordXTime, false);
    }
    return false;
}

/* record of rmid or info error, we inter this function to make every worker run to this position */
static bool DispatchDefaultRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime)
{
    DispatchTxnRecord(record, expectedTLIs, recordXTime, false);
    return true;
}

/* Run from the dispatcher thread. */
static bool DispatchXLogRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime)
{
    bool isNeedFullSync = false;
    uint8 info = (XLogRecGetInfo(record) & (~XLR_INFO_MASK));

    if (IsCheckPoint(record)) {
        RedoItem *item =
            CreateRedoItem(record, (g_dispatcher->pageWorkerCount + 1), ALL_WORKER, expectedTLIs, recordXTime, true);
        for (uint32 i = 0; i < g_dispatcher->pageWorkerCount; ++i) {
            /*
             * A check point record may save a recovery restart point or
             * update the timeline.
             */
            AddPageRedoItem(g_dispatcher->pageWorkers[i], item);
        }
        /* ensure eyery pageworker is receive recored to update pageworker Lsn
         * trxn record's recordtime must set , see SetLatestXTime
         */
        AddTxnRedoItem(g_dispatcher->txnWorker, item);

        isNeedFullSync = g_dispatcher->checkpointNeedFullSync || XLogWillChangeStandbyState(record);
        g_dispatcher->checkpointNeedFullSync = false;
    } else if ((info == XLOG_FPI) || (info == XLOG_FPI_FOR_HINT)) {
        if (t_thrd.proc->workingVersionNum >= XLOG_FPI_FOR_HINT_VERSION_NUM) {
            Size mainDataLen = XLogRecGetDataLen(record);
            if ((mainDataLen == 0) || (mainDataLen != 0 &&
                (*(uint8 *) XLogRecGetData(record)) != XLOG_FPI_FOR_HINT_UHEAP)) {
                if (SUPPORT_FPAGE_DISPATCH) {
                    DispatchRecordWithPages(record, expectedTLIs, true);
                } else {
                    /* fullpagewrite include btree, so need strong sync */
                    DispatchRecordWithoutPage(record, expectedTLIs);
                }
            } else {
                GetWorkersIdWithOutUndoBuffer(record);
            }
        } else {
            if (SUPPORT_FPAGE_DISPATCH) {
                DispatchRecordWithPages(record, expectedTLIs, true);
            } else {
                DispatchRecordWithoutPage(record, expectedTLIs); /* fullpagewrite include btree, so need strong sync */
            }
        }
    } else {
        /* process in trxn thread and need to sync to other pagerredo thread */
        DispatchTxnRecord(record, expectedTLIs, recordXTime, false);
    }
    return isNeedFullSync;
}

/* Run  from the dispatcher thread. */
static bool DispatchRelMapRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime)
{
    /* page redo worker directly use relnode, will not use relmapfile */
    DispatchTxnRecord(record, expectedTLIs, recordXTime, false);
    return false;
}

/* Run  from the dispatcher thread. */
static bool DispatchXactRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime)
{
    if (XactWillRemoveRelFiles(record)) {
        for (uint32 i = 0; i < g_dispatcher->pageWorkerCount; i++) {
            AddWorkerToSet(i);
        }

        /* sync with trxn thread */
        /* trx execute drop action, pageworker forger invalid page,
         * pageworker first exe and update lastcomplateLSN
         * then trx thread exe
         * first pageworker execute and update lsn, then trxn thread */
        DispatchSyncTxnRecord(record, expectedTLIs, recordXTime, ALL_WORKER);
    } else {
        /* process in trxn thread and need to sync to other pagerredo thread */
        DispatchTxnRecord(record, expectedTLIs, recordXTime, false);
    }

    return false;
}

/* Run from the dispatcher thread. */
static bool DispatchStandbyRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime)
{
    /* change standbystate, must be full sync, see UpdateStandbyState */
    bool isNeedFullSync = StandbyWillChangeStandbyState(record);

    DispatchTxnRecord(record, expectedTLIs, recordXTime, false);

    return isNeedFullSync;
}

/* Run from the dispatcher thread. */
static bool DispatchMultiXactRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime)
{
    /* page worker will not use multixact */
    DispatchTxnRecord(record, expectedTLIs, recordXTime, false);

    return true;
}

/* Run from the dispatcher thread. */
static void DispatchRecordWithoutPage(XLogReaderState *record, List *expectedTLIs)
{
    RedoItem *item = CreateRedoItem(record, g_dispatcher->pageWorkerCount, ANY_WORKER, expectedTLIs, 0, true);
    for (uint32 i = 0; i < g_dispatcher->pageWorkerCount; i++)
        AddPageRedoItem(g_dispatcher->pageWorkers[i], item);
}

/* Run from the dispatcher thread. */
static void DispatchRecordWithPages(XLogReaderState *record, List *expectedTLIs, bool rnodedispatch)
{
    GetWorkerIds(record, ANY_WORKER, rnodedispatch);

    RedoItem *item = CreateRedoItem(record, g_dispatcher->chosedWorkerCount, ANY_WORKER, expectedTLIs, 0, true);
    for (uint32 i = 0; i < g_dispatcher->pageWorkerCount; i++) {
        if (g_dispatcher->chosedWorkerIds[i] > 0) {
            AddPageRedoItem(g_dispatcher->pageWorkers[i], item);
        }
    }
}

static bool DispatchHeapRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime)
{
    if (unlikely((XLogRecGetInfo(record) & XLOG_HEAP_OPMASK) == XLOG_HEAP_INPLACE)) {
        DispatchRecordWithoutPage(record, expectedTLIs);
    } else if (record->max_block_id >= 0) {
        DispatchRecordWithPages(record, expectedTLIs, SUPPORT_FPAGE_DISPATCH);
    } else {
        DispatchRecordWithoutPage(record, expectedTLIs);
    }

    return false;
}

static bool DispatchSeqRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime)
{
    DispatchRecordWithPages(record, expectedTLIs, SUPPORT_FPAGE_DISPATCH);

    return false;
}

static bool DispatchDataBaseRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime)
{
    bool isNeedFullSync = false;

    if (IsDataBaseDrop(record)) {
        RedoItem *item =
            CreateRedoItem(record, (g_dispatcher->pageWorkerCount + 1), ALL_WORKER, expectedTLIs, recordXTime, true);
        item->imcheckpoint = true;
        for (uint32 i = 0; i < g_dispatcher->pageWorkerCount; i++) {
            AddPageRedoItem(g_dispatcher->pageWorkers[i], item);
        }
        /* ensure eyery pageworker is receive recored to update pageworker Lsn
         * trxn record's recordtime must set , see SetLatestXTime
         */
        AddTxnRedoItem(g_dispatcher->txnWorker, item);
        isNeedFullSync = true;
    } else {
        /* database dir may impact many rel so need to sync to all pageworks */
        DispatchRecordWithoutPage(record, expectedTLIs);

        RedoItem *txnItem = CreateLSNMarker(record, expectedTLIs, false);
        /* ensure eyery pageworker is receive recored to update pageworker Lsn
         * recordtime not set ,  SetLatestXTime is not need to process
         */
        txnItem->imcheckpoint = true; /* immdiate checkpoint set true  */
        AddTxnRedoItem(g_dispatcher->txnWorker, txnItem);
        g_dispatcher->checkpointNeedFullSync = true;
    }

    return isNeedFullSync;
}

static bool DispatchTableSpaceRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime)
{
    bool isNeedFullSync = false;
    uint8 info = (XLogRecGetInfo(record) & (~XLR_INFO_MASK));

    if (info == XLOG_TBLSPC_DROP) {
        DispatchTxnRecord(record, expectedTLIs, recordXTime, true);
        isNeedFullSync = true;
    } else {
        /* tablespace dir may impact many rel so need to sync to all pageworks */
        DispatchRecordWithoutPage(record, expectedTLIs);

        RedoItem *trxnItem = CreateLSNMarker(record, expectedTLIs, false);
        /* ensure eyery pageworker is receive recored to update pageworker Lsn
         * recordtime not set ,  SetLatestXTime is not need to process
         */
        trxnItem->imcheckpoint = true; /* immdiate checkpoint set true  */
        AddTxnRedoItem(g_dispatcher->txnWorker, trxnItem);
    }

    return isNeedFullSync;
}

static bool DispatchSmgrRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime)
{
    bool isNeedFullSync = false;
    uint8 info = (XLogRecGetInfo(record) & (~XLR_INFO_MASK));
    if (info == XLOG_SMGR_CREATE) {
        /* only need to dispatch to one page worker */
        /* for parallel performance */
        if (SUPPORT_FPAGE_DISPATCH) {
            xl_smgr_create *xlrec = (xl_smgr_create *)XLogRecGetData(record);
            RelFileNode rnode;
            RelFileNodeCopy(rnode, xlrec->rnode, XLogRecGetBucketId(record));
            rnode.opt = GetCreateXlogFileNodeOpt(record);

            DispatchToOnePageWorker(record, rnode, expectedTLIs);
        } else {
            DispatchRecordWithoutPage(record, expectedTLIs);
        }
    } else if (IsSmgrTruncate(record)) {
        /*
         * SMGR_TRUNCATE acquires relation exclusive locks.
         * We need to force a full sync on it on stand by.
         *
         * Plus, it affects invalid pages bookkeeping.  We also need
         * to send it to all page workers.
         */
        /* for parallel performance */
        if (SUPPORT_FPAGE_DISPATCH && !SUPPORT_USTORE_UNDO_WORKER) {
            uint32 id;
            xl_smgr_truncate *xlrec = (xl_smgr_truncate *)XLogRecGetData(record);
            RelFileNode rnode;
            RelFileNodeCopy(rnode, xlrec->rnode, XLogRecGetBucketId(record));
            rnode.opt = GetTruncateXlogFileNodeOpt(record);
            id = GetWorkerId(rnode, 0, 0);
            AddWorkerToSet(id);
        } else {
            for (uint32 i = 0; i < g_dispatcher->pageWorkerCount; i++) {
                AddWorkerToSet(i);
            }
        }

        /* sync with trxn thread */
        /* trx truncate drop action, pageworker forger invalid page,
         * pageworker first exe and update lastcomplateLSN
         * then trx thread exe
         * first pageworker execute and update lsn, then trxn thread */
        DispatchSyncTxnRecord(record, expectedTLIs, recordXTime, ALL_WORKER);
    }

    return isNeedFullSync;
}

static void DispatchSegTruncate(XLogReaderState* record, List* expectedTLIs, TimestampTz recordXTime)
{
    if (SUPPORT_FPAGE_DISPATCH) {
        RelFileNode rnode;
        BlockNumber blknum;
        XLogRecGetBlockTag(record, 0, &rnode, NULL, &blknum);
        rnode.relNode = blknum;
        uint32 id = GetWorkerId(rnode, 0, 0);
        AddWorkerToSet(id);
    } else {
        for (uint32 i = 0; i < g_dispatcher->pageWorkerCount; i++) {
            AddWorkerToSet(i);
        }
    }
    
    DispatchSyncTxnRecord(record, expectedTLIs, recordXTime, ALL_WORKER);
}

static void DispatchNblocksRecord(XLogReaderState* record, List* expectedTLIs)
{
    XLogDataSegmentExtend *dataSegExtendInfo = (XLogDataSegmentExtend *)XLogRecGetBlockData(record, 0, NULL);

    RelFileNode rnode;
    XLogRecGetBlockTag(record, 0, &rnode, NULL, NULL);
    rnode.relNode = dataSegExtendInfo->main_fork_head;
    rnode.bucketNode = SegmentBktId;

    DispatchToOnePageWorker(record, rnode, expectedTLIs);
}

/*
 * Current Transaction manager replays XLog that requires full sync too slowly, because it generates LSN markers,
 * pushes LSN markers to all page workers, makes all page workers update their local lastReplayedEndRecPtr, and
 * itself repeatly reads all workers' lastReplayedEndRecPtr to wait the xlog is sychronized. There are too many
 * cache ping-pong leading to poor performance. 
 * 
 * Unfortunately, DDL on segment-page relations will generate a lot XLogs. If the xlog is a synchronization point,
 * we do not push it to Transaction Manager. Instead, we waiting all page workers' queues being empty, and replay
 * the xlog immediately. If the xlog can be delayed, we push it to the Transaction Manager and replayed by the
 * Transaction Manager later.
 */
static void DealWithSegpageSyncRecord(XLogReaderState* record, List* expectedTLIs, TimestampTz recordXTime)
{
    MoveTxnItemToApplyQueue(g_dispatcher->txnWorker);
    if (TxnQueueIsEmpty(g_dispatcher->txnWorker)) {
        for (uint32 i = 0; i < g_dispatcher->pageWorkerCount; i++) {
            pgstat_report_waitevent(WAIT_EVENT_PREDO_PROCESS_PENDING);
            while (!ProcessPendingPageRedoItems(g_dispatcher->pageWorkers[i])) {
                RedoInterruptCallBack();
            }
            pgstat_report_waitevent(WAIT_EVENT_END);
        }
    } else {
        /* ProcessPendingRecords will push all pending redo items to their queues. We do not need do this later */
        ProcessPendingRecords(true);
    }

    /* Waiting all page workers done. */
    WaitAllPageWorkersQueueEmpty();

    /* Now, all proceding items are done, safe to replay the xlog */
    RedoItem* item = CreateRedoItem(record, 1, ANY_WORKER, expectedTLIs, recordXTime, true);
    MemoryContext oldCtx = MemoryContextSwitchTo(g_dispatcher->oldCtx);
    ApplyRedoRecord(&item->record);
    (void)MemoryContextSwitchTo(oldCtx);
    FreeRedoItem(item);
}

static bool DispatchSegpageSmgrRecord(XLogReaderState* record, List* expectedTLIs, TimestampTz recordXTime)
{
    bool isNeedFullSync = false;
    
    uint8 info = (XLogRecGetInfo(record) & (~XLR_INFO_MASK));

    if (info == XLOG_SEG_ATOMIC_OPERATION || info == XLOG_SEG_CREATE_EXTENT_GROUP || info == XLOG_SEG_INIT_MAPPAGE ||
        info == XLOG_SEG_INIT_INVRSPTR_PAGE || info == XLOG_SEG_ADD_NEW_GROUP || info == XLOG_SEG_NEW_PAGE) {
        DealWithSegpageSyncRecord(record, expectedTLIs, recordXTime);
    } else if (info == XLOG_SEG_SPACE_SHRINK || info == XLOG_SEG_SPACE_DROP) {
        /* xlog of space shrink and drop should be dispatched to page workers to forget invalid pages */
        RedoItem *item =
            CreateRedoItem(record, (g_dispatcher->pageWorkerCount + 1), ALL_WORKER, expectedTLIs, recordXTime, true);
        for (uint32 i = 0; i < g_dispatcher->pageWorkerCount; i++) {
            AddPageRedoItem(g_dispatcher->pageWorkers[i], item);
        }
        AddTxnRedoItem(g_dispatcher->txnWorker, item);
        isNeedFullSync = true;
    } else if (info == XLOG_SEG_SEGMENT_EXTEND) {
        DispatchNblocksRecord(record, expectedTLIs);
    } else if (info == XLOG_SEG_TRUNCATE) {
        DispatchSegTruncate(record, expectedTLIs, recordXTime);
    } else {
        ereport(PANIC,
                (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                 errmsg("[REDO_LOG_TRACE] xlog info %u doesn't belong to segpage.", info)));
    }
    return isNeedFullSync;
}

/* Run from the dispatcher thread. */
static bool DispatchRepOriginRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime)
{
    DispatchTxnRecord(record, expectedTLIs, recordXTime, false);
    return false;
}

/* Run from the dispatcher thread. */
static bool DispatchCLogRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime)
{
    DispatchTxnRecord(record, expectedTLIs, recordXTime, false);
    return false;
}

/* Run from the dispatcher thread. */
static bool DispatchHashRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime)
{
    bool isNeedFullSync = false;

    /* index not support mvcc, so we need to sync with trx thread when the record is vacuum */
    if (IsHashVacuumPages(record) && g_supportHotStandby) {
        GetWorkerIds(record, ANY_WORKER, true);
        /* sync with trxn thread */
        /* only need to process in pageworker  thread, wait trxn sync */
        /* pageworker exe, trxn don't need exe */
        DispatchToSpecPageWorker(record, expectedTLIs, true);
    } else {
        DispatchRecordWithPages(record, expectedTLIs, true);
    }

    return isNeedFullSync;
}

/* for cfs row-compression. */
static bool DispatchCompresseShrinkRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime)
{
    DispatchTxnRecord(record, expectedTLIs, recordXTime, false);
    return true;
}

static bool DispatchLogicalDDLMsgRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime)
{
    DispatchTxnRecord(record, expectedTLIs, recordXTime, false);
    return true;
}

static bool DispatchBtreeRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime)
{
    uint8 info = (XLogRecGetInfo(record) & (~XLR_INFO_MASK));
    if (info == XLOG_BTREE_REUSE_PAGE) {
        DispatchTxnRecord(record, expectedTLIs, recordXTime, false);
    } else {
        DispatchRecordWithPages(record, expectedTLIs, true);
    }
    return false;
}

static bool DispatchUBTreeRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime)
{
    uint8 info = (XLogRecGetInfo(record) & (~XLR_INFO_MASK));
    if (info == XLOG_UBTREE_REUSE_PAGE) {
        DispatchTxnRecord(record, expectedTLIs, recordXTime, false);
    } else {
        DispatchRecordWithPages(record, expectedTLIs, true);
    }
    return false;
}

static bool DispatchUBTree2Record(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime)
{
    DispatchRecordWithPages(record, expectedTLIs, true);
    return false;
}

/* Run from the dispatcher thread. */
static bool DispatchGinRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime)
{
    bool isNeedFullSync = false;
    uint8 info = (XLogRecGetInfo(record) & (~XLR_INFO_MASK));

    if (info == XLOG_GIN_DELETE_LISTPAGE) {
        ginxlogDeleteListPages *data = (ginxlogDeleteListPages *)XLogRecGetData(record);
        /* output warning */
        if (data->ndeleted != record->max_block_id) {
            ereport(WARNING, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                errmsg("[REDO_LOG_TRACE]DispatchGinRecord warnninginfo:ndeleted:%d, max_block_id:%d", data->ndeleted,
                    record->max_block_id)));
        }
    }

    /* index not support mvcc, so we need to sync with trx thread when the record is vacuum */
    if (IsGinVacuumPages(record) && SUPPORT_HOT_STANDBY) {
        GetWorkerIds(record, ANY_WORKER, true);
        /* sync with trxn thread */
        /* only need to process in pageworker  thread, wait trxn sync */
        /* pageworker exe, trxn don't need exe */
        DispatchToSpecPageWorker(record, expectedTLIs, true);
    } else {
        DispatchRecordWithPages(record, expectedTLIs, true);
    }

    return isNeedFullSync;
}

/* Run from the dispatcher thread. */
static bool DispatchGistRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime)
{
    uint8 info = (XLogRecGetInfo(record) & (~XLR_INFO_MASK));
    bool isNeedFullSync = false;

    if (info == XLOG_GIST_PAGE_SPLIT) {
        gistxlogPageSplit *xldata = (gistxlogPageSplit *)XLogRecGetData(record);
        /* output warning */
        if (xldata->npage != record->max_block_id) {
            ereport(WARNING, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                errmsg("[REDO_LOG_TRACE]DispatchGistRecord warnninginfo:npage:%hu, max_block_id:%d", xldata->npage,
                record->max_block_id)));
        }
    }

    /* index not support mvcc, so we need to sync with trx thread when the record is vacuum */
    if (IsGistPageUpdate(record) && SUPPORT_HOT_STANDBY) {
        GetWorkerIds(record, ANY_WORKER, true);
        /* sync with trx thread */
        /* only need to process in pageworker  thread, wait trxn sync */
        /* pageworker exe, trxn don't need exe */
        DispatchToSpecPageWorker(record, expectedTLIs, true);
    } else {
        DispatchRecordWithPages(record, expectedTLIs, true);
    }

    return isNeedFullSync;
}

/* Run from the dispatcher thread. */
static bool DispatchSpgistRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime)
{
    /* index not support mvcc, so we need to sync with trx thread when the record is vacuum */
    if (IsSpgistVacuum(record) && SUPPORT_HOT_STANDBY) {
        uint8 info = (XLogRecGetInfo(record) & (~XLR_INFO_MASK));

        GetWorkerIds(record, ANY_WORKER, true);
        /* sync with trx thread */
        if ((info == XLOG_SPGIST_VACUUM_REDIRECT) && (InHotStandby)) {
            /* trxn thread first reslove confilict snapshot ,then do the page action */
            /* first pageworker update lsn, then trxn thread exe */
            DispatchSyncTxnRecord(record, expectedTLIs, recordXTime, TRXN_WORKER);
        } else {
            /* only need to process in pageworker  thread, wait trxn sync */
            /* pageworker exe, trxn don't need exe */
            DispatchToSpecPageWorker(record, expectedTLIs, true);
        }
    } else {
        DispatchRecordWithPages(record, expectedTLIs, true);
    }
    return false;
}

/* *
 * dispatch record to a specified thread
 */
static void DispatchToSpecPageWorker(XLogReaderState *record, List *expectedTLIs, bool waittrxnsync)
{
    RedoItem *item = CreateRedoItem(record, g_dispatcher->chosedWorkerCount, ANY_WORKER, expectedTLIs, 0, true);

    item->sharewithtrxn = false;
    item->blockbytrxn = waittrxnsync;

    if (g_dispatcher->chosedWorkerCount != 1) {
        ereport(WARNING, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
            errmsg("[REDO_LOG_TRACE]DispatchToSpecPageWorker maybe some error:rmgrID:%u, info:%u, workerCount:%u",
                (uint32)XLogRecGetRmid(&item->record), (uint32)XLogRecGetInfo(&item->record),
                g_dispatcher->chosedWorkerCount)));
    }

    for (uint32 i = 0; i < g_dispatcher->pageWorkerCount; i++) {
        if (g_dispatcher->chosedWorkerIds[i] > 0) {
            AddPageRedoItem(g_dispatcher->pageWorkers[i], item);
        }
    }

    /* ensure eyery pageworker is receive recored to update pageworker Lsn
     * recordtime not set ,  SetLatestXTime is not need to process
     */
    AddTxnRedoItem(g_dispatcher->txnWorker, CreateLSNMarker(record, expectedTLIs, false));
}

static bool DispatchHeap2VacuumRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime)
{
    /*
     * don't support consistency view
     */
    bool isNeedFullSync = false;
    uint8 info = ((XLogRecGetInfo(record) & (~XLR_INFO_MASK)) & XLOG_HEAP_OPMASK);
    if (info == XLOG_HEAP2_CLEANUP_INFO) {
        xl_heap_cleanup_info* xlrec = (xl_heap_cleanup_info*)XLogRecGetData(record);
        RelFileNode tmp_node;
        RelFileNodeCopy(tmp_node, xlrec->node, (int2)XLogRecGetBucketId(record));

        DispatchToOnePageWorker(record, tmp_node, expectedTLIs);
    } else {
        DispatchRecordWithPages(record, expectedTLIs, SUPPORT_FPAGE_DISPATCH);
    }

    return isNeedFullSync;
}

/* Run from the dispatcher thread. */
static bool DispatchHeap2Record(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime)
{
    bool isNeedFullSync = false;

    uint8 info = ((XLogRecGetInfo(record) & (~XLR_INFO_MASK)) & XLOG_HEAP_OPMASK);
    if (info == XLOG_HEAP2_MULTI_INSERT) {
        DispatchRecordWithPages(record, expectedTLIs, SUPPORT_FPAGE_DISPATCH);
    } else if (info == XLOG_HEAP2_BCM) {
        /* we use renode as dispatch key, so the same relation will dispath to the same page redo thread
         * although they have different fork num
         */
        /* for parallel redo performance */
        if (SUPPORT_FPAGE_DISPATCH) {
            xl_heap_bcm *xlrec = (xl_heap_bcm *)XLogRecGetData(record);

            RelFileNode tmp_node;
            RelFileNodeCopy(tmp_node, xlrec->node, XLogRecGetBucketId(record));

            DispatchToOnePageWorker(record, tmp_node, expectedTLIs);
        } else {
            DispatchRecordWithoutPage(record, expectedTLIs);
        }
    } else if (info == XLOG_HEAP2_LOGICAL_NEWPAGE) {
        if (IS_DN_MULTI_STANDYS_MODE()) {
            xl_heap_logical_newpage *xlrec = (xl_heap_logical_newpage *)XLogRecGetData(record);

            if (xlrec->type == COLUMN_STORE && xlrec->hasdata) {
                /* for parallel redo performance */
                if (SUPPORT_FPAGE_DISPATCH) {
                    RelFileNode tmp_node;
                    RelFileNodeCopy(tmp_node, xlrec->node, XLogRecGetBucketId(record));

                    DispatchToOnePageWorker(record, tmp_node, expectedTLIs);
                } else {
                    DispatchRecordWithoutPage(record, expectedTLIs);
                }
            } else {
                RedoItem *item = CreateRedoItem(record, 1, ANY_WORKER, expectedTLIs, 0, true);
                FreeRedoItem(item);
            }
        } else {
            if (!g_instance.attr.attr_storage.enable_mix_replication) {
                DispatchTxnRecord(record, expectedTLIs, recordXTime, false);
                isNeedFullSync = true;
            } else {
                RedoItem *item = CreateRedoItem(record, 1, ANY_WORKER, expectedTLIs, 0, true);
                FreeRedoItem(item);
            }
        }
    } else {
        isNeedFullSync = DispatchHeap2VacuumRecord(record, expectedTLIs, recordXTime);
    }

    return isNeedFullSync;
}

#ifdef ENABLE_MOT
/* Run from the dispatcher thread. */
static bool DispatchMotRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime)
{
    DispatchTxnRecord(record, expectedTLIs, recordXTime, false);
    return false;
}
#endif

/* Run from the dispatcher thread. */
static void GetWorkerIds(XLogReaderState *record, uint32 designatedWorker, bool rnodedispatch)
{
    uint32 id;
    for (int i = 0; i <= record->max_block_id; i++) {
        DecodedBkpBlock *block = &record->blocks[i];

        if (block->in_use != true) {
            /* blk number is not continue */
            continue;
        }
        if (rnodedispatch)
            id = GetWorkerId(block->rnode, 0, 0);
        else
            id = GetWorkerId(block->rnode, block->blkno, 0);

        AddWorkerToSet(id);
    }

    if ((designatedWorker != ANY_WORKER)) {
        if (designatedWorker < GetPageWorkerCount()) {
            AddWorkerToSet(designatedWorker);
        } else {
            /* output  error info */
        }
    }
}

/* *
 * count worker id  by hash
 */
uint32 GetWorkerId(const RelFileNode &node, BlockNumber block, ForkNumber forkNum)
{
    uint32 workerCount = GetPageWorkerCount();
    uint32 undoZidWorkersNum = get_recovery_undozidworkers_num();
    if (SUPPORT_USTORE_UNDO_WORKER)
        workerCount = workerCount - undoZidWorkersNum;

    if (workerCount == 0)
        return ANY_WORKER;

    BufferTag tag;
    INIT_BUFFERTAG(tag, node, forkNum, block);
    tag.rnode.bucketNode = g_dispatcher->dispatchFix.dispatchRandom;
    return tag_hash(&tag, sizeof(tag)) % workerCount;
}

static void AddWorkerToSet(uint32 id)
{
    if (id >= g_dispatcher->pageWorkerCount) {
        ereport(PANIC, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
            errmsg("[REDO_LOG_TRACE]AddWorkerToSet:input work id error, id:%u, work num %u", id,
                g_dispatcher->pageWorkerCount)));
        return;
    }

    if (g_dispatcher->chosedWorkerIds[id] == 0) {
        g_dispatcher->chosedWorkerCount += 1;
    }
    ++(g_dispatcher->chosedWorkerIds[id]);
}

/* Run from the dispatcher thread. */
static bool XLogWillChangeStandbyState(XLogReaderState *record)
{
    /*
     * If standbyState has reached SNAPSHOT_READY, it will not change
     * anymore.  Otherwise, it will change if the log record's redo
     * function calls ProcArrayApplyRecoveryInfo().
     */
    if ((t_thrd.xlog_cxt.standbyState < STANDBY_INITIALIZED) ||
        (t_thrd.xlog_cxt.standbyState == STANDBY_SNAPSHOT_READY))
        return false;

    if ((XLogRecGetRmid(record) == RM_XLOG_ID) &&
        ((XLogRecGetInfo(record) & (~XLR_INFO_MASK)) == XLOG_CHECKPOINT_SHUTDOWN)) {
        return true;
    }

    return false;
}

/* Run from the dispatcher thread. */
static bool StandbyWillChangeStandbyState(XLogReaderState *record)
{
    /*
     * If standbyState has reached SNAPSHOT_READY, it will not change
     * anymore.  Otherwise, it will change if the log record's redo
     * function calls ProcArrayApplyRecoveryInfo().
     */
    if ((t_thrd.xlog_cxt.standbyState < STANDBY_SNAPSHOT_READY) && (XLogRecGetRmid(record) == RM_STANDBY_ID) &&
        ((XLogRecGetInfo(record) & (~XLR_INFO_MASK)) == XLOG_RUNNING_XACTS)) {
        /* change standbystate, must be full sync, see UpdateStandbyState */
        return true;
    }

    if ((XLogRecGetRmid(record) == RM_STANDBY_ID) &&
        ((XLogRecGetInfo(record) & (~XLR_INFO_MASK)) == XLOG_STANDBY_LOCK)) {
        return true;
    }

    return false;
}

/* Run from the dispatcher thread. */
/* fullSync: true. wait for other workers, transaction need it */
/*        : false not wait for other workers  */
void ProcessPendingRecords(bool fullSync)
{
    if(fullSync)
        g_dispatcher->full_sync_dispatch = true;
    g_dispatcher->lastDispatchTime = GetCurrentTimestamp();
    if ((get_real_recovery_parallelism() > 1) && (GetPageWorkerCount() > 0)) {
        for (uint32 i = 0; i < g_dispatcher->pageWorkerCount; i++) {
            uint64 blockcnt = 0;
            pgstat_report_waitevent(WAIT_EVENT_PREDO_PROCESS_PENDING);
            while (!ProcessPendingPageRedoItems(g_dispatcher->pageWorkers[i])) {
                blockcnt++;
                ApplyReadyTxnLogRecords(g_dispatcher->txnWorker, false);
                if ((blockcnt & OUTPUT_WAIT_COUNT) == OUTPUT_WAIT_COUNT) {
                    ereport(LOG, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                        errmsg("[REDO_LOG_TRACE]ProcessPendingRecords:replayedLsn:%lu, blockcnt:%lu, "
                            "WorkerCount:%u, readEndLSN:%lu",
                            GetXLogReplayRecPtr(NULL, NULL), blockcnt, g_dispatcher->pageWorkerCount,
                            t_thrd.xlog_cxt.EndRecPtr)));
                    if ((blockcnt & PRINT_ALL_WAIT_COUNT) == PRINT_ALL_WAIT_COUNT) {
                        DumpDispatcher();
                    }
                }
                RedoInterruptCallBack();
            }
            pgstat_report_waitevent(WAIT_EVENT_END);
        }
        MoveTxnItemToApplyQueue(g_dispatcher->txnWorker);
        ApplyReadyTxnLogRecords(g_dispatcher->txnWorker, fullSync);
        g_dispatcher->pendingCount = 0;
    }
    if(fullSync)
        g_dispatcher->full_sync_dispatch = false;
}

/* Run from the dispatcher thread. */
/* fullSync: true. wait for other workers, transaction need it */
/*        : false not wait for other workers  */
void ProcessTrxnRecords(bool fullSync)
{
    if ((get_real_recovery_parallelism() > 1) && (GetPageWorkerCount() > 0)) {
        if (g_instance.attr.attr_storage.enable_batch_dispatch)
            ProcessPendingRecords(fullSync);
        else
            ApplyReadyTxnLogRecords(g_dispatcher->txnWorker, fullSync);

        if (fullSync && (IsTxnWorkerIdle(g_dispatcher->txnWorker))) {
            /* notify pageworker sleep long time */
            SendSingalToPageWorker(SIGUSR2);
        }
    }
}

/* Run from each page worker thread. */
void FreeRedoItem(RedoItem *item)
{
    if (item->need_free) {
        XLogReaderState *tmpRec = &(item->record);

        if (tmpRec->readRecordBuf) {
            pfree(tmpRec->readRecordBuf);
            tmpRec->readRecordBuf = NULL;
        }

        pfree(item);
        return;
    }

    if (!IsLSNMarker(item)) {
        CountXLogNumbers(&item->record);
    }

    if (item->record.readRecordBufSize > BIG_RECORD_LENGTH) {
        pfree(item->record.readRecordBuf);
        item->record.readRecordBuf = NULL;
        item->record.readRecordBufSize = 0;
    }
    pg_write_barrier();
    RedoItem *oldHead = (RedoItem *)pg_atomic_read_uintptr((uintptr_t *)&g_dispatcher->freeHead);
    uint32 freed = pg_atomic_read_u32(&item->freed);
    if (freed != 0) { /* if it happens, there must be problems! check it */
        ereport(WARNING, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
            errmsg("FreeRedoItem failed, freed:%u, sharewithtrxn:%u, blockbytrxn:%u, imcheckpoint:%u, "
            "shareCount:%u, designatedWorker:%u, refCount:%u, replayed:%u, readPtr:%lu, endPtr:%lu",
                freed, item->sharewithtrxn, item->blockbytrxn, item->imcheckpoint, item->shareCount,
                item->designatedWorker, item->refCount, item->replayed, item->record.ReadRecPtr,
                item->record.EndRecPtr)));
        return;
    }

    pg_atomic_write_u32(&item->freed, 1);

    do {
        item->freeNext = oldHead;
    } while (!pg_atomic_compare_exchange_uintptr((uintptr_t *)&g_dispatcher->freeHead, (uintptr_t *)&oldHead,
        (uintptr_t)item));
}

void InitReaderStateByOld(XLogReaderState *newState, XLogReaderState *oldState, bool isNew)
{
    newState->ReadRecPtr = oldState->ReadRecPtr;
    newState->EndRecPtr = oldState->EndRecPtr;
    newState->readSegNo = oldState->readSegNo;
    newState->readOff = oldState->readOff;
    newState->readPageTLI = oldState->readPageTLI;
    newState->curReadSegNo = oldState->curReadSegNo;
    newState->curReadOff = oldState->curReadOff;
    newState->latestPagePtr = oldState->latestPagePtr;
    newState->latestPageTLI = oldState->latestPageTLI;
    newState->currRecPtr = oldState->currRecPtr;
    newState->readLen = oldState->readLen;
    newState->readBuf = oldState->readBuf;
    newState->preReadStartPtr = oldState->preReadStartPtr;
    newState->preReadBuf = oldState->preReadBuf;

    if (isNew) {
        newState->readRecordBuf = NULL;
        newState->readRecordBufSize = 0;
        newState->errormsg_buf = oldState->errormsg_buf;
        newState->isPRProcess = oldState->isPRProcess;
        newState->read_page = oldState->read_page;
        newState->system_identifier = oldState->system_identifier;
        newState->private_data = oldState->private_data;
    }

    newState->main_data = NULL;
    newState->main_data_len = 0;
    newState->main_data_bufsz = 0;
    for (int i = 0; i <= newState->max_block_id; i++) {
        newState->blocks[i].data = NULL;
        newState->blocks[i].data_len = 0;
        newState->blocks[i].data_bufsz = 0;
        newState->blocks[i].in_use = false;
        newState->blocks[i].has_image = false;
        newState->blocks[i].has_data = false;
        newState->blocks[i].tdeinfo = NULL;
#ifdef USE_ASSERT_CHECKING
        newState->blocks[i].replayed = 0;
#endif
    }
    newState->max_block_id = -1;
    newState->refcount = 0;
    newState->isDecode = false;
    newState->isFullSync = false;
    newState->readblocks = 0;
}

static XLogReaderState *GetXlogReader(XLogReaderState *readerState)
{
    XLogReaderState *retReaderState = NULL;
    bool isNew = false;
    uint64 count = 0;
    pgstat_report_waitevent(WAIT_EVENT_PREDO_APPLY);
    do {
        if (g_dispatcher->freeStateHead != NULL) {
            retReaderState = &g_dispatcher->freeStateHead->record;
            g_dispatcher->freeStateHead = g_dispatcher->freeStateHead->freeNext;
        } else {
            RedoItem *head =
                (RedoItem *)pg_atomic_exchange_uintptr((uintptr_t *)&g_dispatcher->freeHead, (uintptr_t)NULL);
            if (head != NULL) {
                retReaderState = &head->record;
                g_dispatcher->freeStateHead = head->freeNext;
            } else if (g_dispatcher->maxItemNum > g_dispatcher->curItemNum) {
                RedoItem *item = (RedoItem *)palloc_extended(MAXALIGN(sizeof(RedoItem)) +
                    sizeof(RedoItem *) * (GetPageWorkerCount() + 1),
                    MCXT_ALLOC_NO_OOM | MCXT_ALLOC_ZERO);
                if (item != NULL) {
                    retReaderState = &item->record;
                    item->allocatedNext = g_dispatcher->allocatedRedoItem;
                    g_dispatcher->allocatedRedoItem = item;
                    isNew = true;
                    ++(g_dispatcher->curItemNum);
                }
            }

            ++count;
            if ((count & OUTPUT_WAIT_COUNT) == OUTPUT_WAIT_COUNT) {
                ereport(WARNING, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                    errmsg("GetXlogReader Allocated record buffer failed!, cur item:%u, max item:%u",
                        g_dispatcher->curItemNum, g_dispatcher->maxItemNum)));
                if ((count & PRINT_ALL_WAIT_COUNT) == PRINT_ALL_WAIT_COUNT) {
                    DumpDispatcher();
                }
            }
            if (retReaderState == NULL) {
                ProcessTrxnRecords(false);
                RedoInterruptCallBack();
            }
        }
    } while (retReaderState == NULL);
    pgstat_report_waitevent(WAIT_EVENT_END);

    InitReaderStateByOld(retReaderState, readerState, isNew);

    return retReaderState;
}

void CopyDataFromOldReader(XLogReaderState *newReaderState, XLogReaderState *oldReaderState)
{
    errno_t rc = EOK;
    if ((newReaderState->readRecordBuf == NULL) ||
        (oldReaderState->readRecordBufSize > newReaderState->readRecordBufSize)) {
        if (!allocate_recordbuf(newReaderState, oldReaderState->readRecordBufSize)) {
            ereport(PANIC,
                (errmodule(MOD_REDO),
                    errcode(ERRCODE_LOG),
                    errmsg("Allocated record buffer failed!, cur item:%u, max item:%u",
                        g_dispatcher->curItemNum,
                        g_dispatcher->maxItemNum)));
        }
    }

    rc = memcpy_s(newReaderState->readRecordBuf,
        newReaderState->readRecordBufSize,
        oldReaderState->readRecordBuf,
        oldReaderState->readRecordBufSize);
    securec_check(rc, "\0", "\0");
    newReaderState->decoded_record = (XLogRecord *)newReaderState->readRecordBuf;
    newReaderState->max_block_id = oldReaderState->max_block_id;
    rc = memcpy_s(newReaderState->blocks, sizeof(DecodedBkpBlock) * (XLR_MAX_BLOCK_ID + 1), oldReaderState->blocks,
        sizeof(DecodedBkpBlock) * (XLR_MAX_BLOCK_ID + 1));
    securec_check(rc, "\0", "\0");
    for (int i = 0; i <= newReaderState->max_block_id; i++) {
        if (oldReaderState->blocks[i].has_image)
            newReaderState->blocks[i].bkp_image = (char *)((uintptr_t)newReaderState->decoded_record +
                ((uintptr_t)oldReaderState->blocks[i].bkp_image - (uintptr_t)oldReaderState->decoded_record));
        if (oldReaderState->blocks[i].has_data) {
            newReaderState->blocks[i].data = (char *)((uintptr_t)newReaderState->decoded_record +
                         ((uintptr_t)oldReaderState->blocks[i].data - (uintptr_t)oldReaderState->decoded_record));
            newReaderState->blocks[i].data_len = oldReaderState->blocks[i].data_len;
        }
    }
    if (oldReaderState->main_data_len > 0) {
        newReaderState->main_data =
            (char*)((uintptr_t)newReaderState->decoded_record +
                    ((uintptr_t)oldReaderState->main_data - (uintptr_t)oldReaderState->decoded_record));
        newReaderState->main_data_len = oldReaderState->main_data_len;
    }
}

XLogReaderState *NewReaderState(XLogReaderState *readerState, bool bCopyState)
{
    Assert(readerState != NULL);
    if (!readerState->isPRProcess)
        return readerState;
    if (DispatchPtrIsNull())
        ereport(PANIC, (errmodule(MOD_REDO), errcode(ERRCODE_LOG), errmsg("NewReaderState Dispatch is null")));

    XLogReaderState *retReaderState = GetXlogReader(readerState);

    if (bCopyState) {
        CopyDataFromOldReader(retReaderState, readerState);
    }
    return retReaderState;
}

void FreeAllocatedRedoItem()
{
    while ((g_dispatcher != NULL) && (g_dispatcher->allocatedRedoItem != NULL)) {
        RedoItem *pItem = g_dispatcher->allocatedRedoItem;
        g_dispatcher->allocatedRedoItem = pItem->allocatedNext;
        XLogReaderState *tmpRec = &(pItem->record);

        if (tmpRec->readRecordBuf) {
            pfree(tmpRec->readRecordBuf);
            tmpRec->readRecordBuf = NULL;
        }

        pfree(pItem);
    }
}

/* Run from the dispatcher thread. */
void SendRecoveryEndMarkToWorkersAndWaitForFinish(int code)
{
    ereport(LOG, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
        errmsg("[REDO_LOG_TRACE]SendRecoveryEndMarkToWorkersAndWaitForFinish, ready to stop redo workers, code: %d",
            code)));
    if ((get_real_recovery_parallelism() > 1) && (GetPageWorkerCount() > 0)) {
        pg_atomic_write_u32((uint32 *)&g_dispatcher->exitCode, (uint32)code);
        ApplyReadyTxnLogRecords(g_dispatcher->txnWorker, true);
        for (uint32 i = 0; i < g_dispatcher->pageWorkerCount; i++) {
            uint64 blockcnt = 0;
            while (!SendPageRedoEndMark(g_dispatcher->pageWorkers[i])) {
                blockcnt++;
                ApplyReadyTxnLogRecords(g_dispatcher->txnWorker, false);
                if ((blockcnt & OUTPUT_WAIT_COUNT) == OUTPUT_WAIT_COUNT) {
                    ereport(WARNING, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                        errmsg("[REDO_LOG_TRACE]RecoveryEndMark:replayedLsn:%lu, blockcnt:%lu, WorkerCount:%u",
                            GetXLogReplayRecPtr(NULL, NULL), blockcnt, g_dispatcher->pageWorkerCount)));
                }
            }
        }

        ApplyReadyTxnLogRecords(g_dispatcher->txnWorker, true);
        for (uint32 i = 0; i < g_dispatcher->pageWorkerCount; i++)
            WaitPageRedoWorkerReachLastMark(g_dispatcher->pageWorkers[i]);
        SpinLockAcquire(&(g_instance.comm_cxt.predo_cxt.rwlock));
        g_instance.comm_cxt.predo_cxt.state = REDO_DONE;
        SpinLockRelease(&(g_instance.comm_cxt.predo_cxt.rwlock));
        ereport(LOG, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
            errmsg("[REDO_LOG_TRACE]SendRecoveryEndMarkToWorkersAndWaitForFinish, disptach total elapsed: %lu,"
                " txn elapsed: %lu, process pending record elapsed: %lu code: %d",
                g_dispatcher->totalCostTime, g_dispatcher->txnCostTime, g_dispatcher->pprCostTime, code)));
        (void)RegisterRedoInterruptCallBack(g_dispatcher->oldStartupIntrruptFunc);
    }
}

/* Run from each page worker and the txn worker thread. */
int GetDispatcherExitCode()
{
    return (int)pg_atomic_read_u32((uint32 *)&g_dispatcher->exitCode);
}

/* Run from the dispatcher thread. */
uint32 GetPageWorkerCount()
{
    return g_dispatcher == NULL ? 0 : g_dispatcher->pageWorkerCount;
}

bool DispatchPtrIsNull()
{
    return (g_dispatcher == NULL);
}

/* Run from each page worker thread. */
PGPROC *StartupPidGetProc(ThreadId pid)
{
    if (pid == g_instance.proc_base->startupProcPid)
        return g_instance.proc_base->startupProc;
    if ((get_real_recovery_parallelism() > 1) && (GetPageWorkerCount() > 0)) {
        for (uint32 i = 0; i < g_dispatcher->pageWorkerCount; i++) {
            PGPROC *proc = GetPageRedoWorkerProc(g_dispatcher->pageWorkers[i]);
            if (pid == proc->pid)
                return proc;
        }
    }
    return NULL;
}


/*
 * Used from bufgr to share the value of the buffer that Startup waits on,
 * or to reset the value to "not waiting" (-1). This allows processing
 * of recovery conflicts for buffer pins. Set is made before backends look
 * at this value, so locking not required, especially since the set is
 * an atomic integer set operation.
 */
void SetStartupBufferPinWaitBufId(int bufid)
{
    if (g_instance.proc_base->startupProcPid == t_thrd.proc->pid) {
        g_instance.proc_base->startupBufferPinWaitBufId = bufid;
    }
    if ((get_real_recovery_parallelism() > 1) && (GetPageWorkerCount() > 0)) {
        for (uint32 i = 0; i < g_dispatcher->pageWorkerCount; i++) {
            PGPROC *proc = GetPageRedoWorkerProc(g_dispatcher->pageWorkers[i]);
            if (t_thrd.proc->pid == proc->pid) {
                g_instance.comm_cxt.predo_cxt.buffer_pin_wait_buf_ids[i] = bufid;
                break;
            }
        }
    }
}

uint32 GetStartupBufferPinWaitBufLen()
{
    uint32 buf_len = 1;
    if ((get_real_recovery_parallelism() > 1) && (GetPageWorkerCount() > 0)) {
        buf_len += g_instance.comm_cxt.predo_cxt.buffer_pin_wait_buf_len;
    }
    return buf_len;
}


/*
 * Used by backends when they receive a request to check for buffer pin waits.
 */
void GetStartupBufferPinWaitBufId(int *bufids, uint32 len)
{
    if (g_dispatcher != NULL) {
        for (uint32 i = 0; i < len - 1; i++) {
            bufids[i] = g_instance.comm_cxt.predo_cxt.buffer_pin_wait_buf_ids[i];
        }
        bufids[len - 1] = g_instance.proc_base->startupBufferPinWaitBufId;
    }
}

void GetReplayedRecPtrFromUndoWorkers(XLogRecPtr *readPtr, XLogRecPtr *endPtr)
{
    XLogRecPtr minRead = MAX_XLOG_REC_PTR;
    XLogRecPtr minEnd = MAX_XLOG_REC_PTR;
    uint32 workerCount = GetPageWorkerCount(); 
    uint32 undoZidWorkersNum = get_recovery_undozidworkers_num();
    int firstUndoLogWorker = (workerCount - undoZidWorkersNum);

    for (uint32 i = firstUndoLogWorker; i < workerCount; i++) {
        XLogRecPtr read;
        XLogRecPtr end;
        GetCompletedReadEndPtr(g_dispatcher->pageWorkers[i], &read, &end);
        if (XLByteLT(end, minEnd)) {
            minEnd = end;
            minRead = read;
        }
    }

    *readPtr = minRead;
    *endPtr = minEnd;
}

void GetReplayingRecPtrFromWorkers(XLogRecPtr *endPtr)
{
    XLogRecPtr minEnd = MAX_XLOG_REC_PTR;

    for (uint32 i = 0; i < g_dispatcher->pageWorkerCount; i++) {
        if (!RedoWorkerIsIdle(g_dispatcher->pageWorkers[i])) {
            XLogRecPtr end = GetReplyingRecPtr(g_dispatcher->pageWorkers[i]);
            if (XLByteLT(end, minEnd)) {
                minEnd = end;
            }
        }
    }

    *endPtr = minEnd;
}

void GetReplayedRecPtrFromWorkers(XLogRecPtr *readPtr, XLogRecPtr *endPtr)
{
    XLogRecPtr minRead = MAX_XLOG_REC_PTR;
    XLogRecPtr minEnd = MAX_XLOG_REC_PTR;

    for (uint32 i = 0; i < g_dispatcher->pageWorkerCount; i++) {
        if (!RedoWorkerIsIdle(g_dispatcher->pageWorkers[i])) {
            XLogRecPtr read;
            XLogRecPtr end;
            GetCompletedReadEndPtr(g_dispatcher->pageWorkers[i], &read, &end);
            if (XLByteLT(end, minEnd)) {
                minEnd = end;
                minRead = read;
            }
        }
    }

    *readPtr = minRead;
    *endPtr = minEnd;
}

/* Run from the txn worker thread. */
bool IsRecoveryRestartPointSafeForWorkers(XLogRecPtr restartPoint)
{
    bool safe = true;
    if ((get_real_recovery_parallelism() > 1) && (GetPageWorkerCount() > 0)) {
        for (uint32 i = 0; i < g_dispatcher->pageWorkerCount; i++)
            if (!IsRecoveryRestartPointSafe(g_dispatcher->pageWorkers[i], restartPoint)) {
                ereport(LOG, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                    errmsg("[REDO_LOG_TRACE]IsRecoveryRestartPointSafeForWorkers: workerId:%u, restartPoint:%lu", i,
                    restartPoint)));
                safe = false;
            }
        if (safe) {
            for (uint32 i = 0; i < g_dispatcher->pageWorkerCount; i++) {
                SetWorkerRestartPoint(g_dispatcher->pageWorkers[i], restartPoint);
            }
        }
    }

    return safe;
}

/* Run from the dispatcher and txn worker thread. */
void UpdateStandbyState(HotStandbyState newState)
{
    if ((get_real_recovery_parallelism() > 1) && (GetPageWorkerCount() > 0)) {
        for (uint32 i = 0; i < g_dispatcher->pageWorkerCount; i++)
            UpdatePageRedoWorkerStandbyState(g_dispatcher->pageWorkers[i], newState);
    }
}

/* Run from the dispatcher thread. */
void **GetXLogInvalidPagesFromWorkers()
{
    return CollectStatesFromWorkers(GetXLogInvalidPages);
}

/* Run from the dispatcher thread. */
static void **CollectStatesFromWorkers(GetStateFunc getStateFunc)
{
    if (g_dispatcher->pageWorkerCount > 0) {
        void **stateArray = (void **)palloc(sizeof(void *) * g_dispatcher->pageWorkerCount);
        for (uint32 i = 0; i < g_dispatcher->pageWorkerCount; i++)
            stateArray[i] = getStateFunc(g_dispatcher->pageWorkers[i]);
        return stateArray;
    } else
        return NULL;
}

void redo_get_worker_time_count(RedoWorkerTimeCountsInfo **workerCountInfoList, uint32 *realNum)
{
    SpinLockAcquire(&(g_instance.comm_cxt.predo_cxt.rwlock));
    knl_parallel_redo_state state = g_instance.comm_cxt.predo_cxt.state;
    SpinLockRelease(&(g_instance.comm_cxt.predo_cxt.rwlock));

    if (state != REDO_IN_PROGRESS || !g_instance.attr.attr_storage.enable_time_report) {
        *realNum = 0;
        return;
    }

    PageRedoWorker *redoWorker = NULL;
    SpinLockAcquire(&(g_instance.comm_cxt.predo_cxt.destroy_lock));
    if (g_dispatcher == NULL) {
        SpinLockRelease(&(g_instance.comm_cxt.predo_cxt.destroy_lock));
        *realNum = 0;
        return;
    }
    *realNum = g_dispatcher->pageWorkerCount + 1;
    RedoWorkerTimeCountsInfo *workerList =
        (RedoWorkerTimeCountsInfo *)palloc0((*realNum) * sizeof(RedoWorkerTimeCountsInfo));
    errno_t rc;
    const uint32 workerNumSize = 2;
    uint32 cur_pos = 0;
    const char *workName = "pagewoker";
    const char *startupName = "startup";
    for (uint32 i = 0; i < g_dispatcher->pageWorkerCount; ++i) {
        redoWorker = (g_dispatcher->pageWorkers[i]);
        workerList[cur_pos].worker_name = (char*)palloc(strlen(workName) + workerNumSize + 1);
        rc = sprintf_s(workerList[cur_pos].worker_name, strlen(workName) + workerNumSize + 1, "%s%u", workName, i);
        securec_check_ss(rc, "\0", "\0");
        workerList[cur_pos++].time_cost = redoWorker->timeCostList;
    }

    workerList[cur_pos].worker_name = (char*)palloc(strlen(startupName) + 1);
    rc = sprintf_s(workerList[cur_pos].worker_name, strlen(startupName) + 1, "%s", startupName);
    securec_check_ss(rc, "\0", "\0");
    workerList[cur_pos++].time_cost = g_dispatcher->startupTimeCost;
    SpinLockRelease(&(g_instance.comm_cxt.predo_cxt.destroy_lock));
    *workerCountInfoList = workerList;
    Assert(*realNum == cur_pos);
}

void redo_get_worker_statistic(uint32 *realNum, RedoWorkerStatsData *worker, uint32 workerLen)
{
    PageRedoWorker *redoWorker = NULL;
    SpinLockAcquire(&(g_instance.comm_cxt.predo_cxt.destroy_lock));
    if (g_dispatcher == NULL) {
        SpinLockRelease(&(g_instance.comm_cxt.predo_cxt.destroy_lock));
        *realNum = 0;
        return;
    }
    *realNum = g_dispatcher->pageWorkerCount;
    for (uint32 i = 0; i < g_dispatcher->pageWorkerCount; i++) {
        redoWorker = (g_dispatcher->pageWorkers[i]);
        worker[i].id = redoWorker->id;
        worker[i].queue_usage = SPSCGetQueueCount(redoWorker->queue);
        worker[i].queue_max_usage = (uint32)(pg_atomic_read_u32(&((redoWorker->queue)->maxUsage)));
        worker[i].redo_rec_count = (uint32)(pg_atomic_read_u64(&((redoWorker->queue)->totalCnt)));
    }
    SpinLockRelease(&(g_instance.comm_cxt.predo_cxt.destroy_lock));
}

RedoWaitInfo redo_get_io_event(int32 event_id)
{
    WaitStatisticsInfo tmp_io = {};
    RedoWaitInfo result_info = {};
    PgBackendStatus *beentry = NULL;
    int index = MAX_BACKEND_SLOT + StartupProcess;

    if (IS_PGSTATE_TRACK_UNDEFINE || t_thrd.shemem_ptr_cxt.BackendStatusArray == NULL) {
        return result_info;
    }

    beentry = t_thrd.shemem_ptr_cxt.BackendStatusArray + index;
    tmp_io = beentry->waitInfo.event_info.io_info[event_id - WAIT_EVENT_BUFFILE_READ];
    result_info.total_duration = tmp_io.total_duration;
    result_info.counter = tmp_io.counter;
    SpinLockAcquire(&(g_instance.comm_cxt.predo_cxt.destroy_lock));
    if (g_dispatcher == NULL || event_id == WAIT_EVENT_WAL_READ || event_id == WAIT_EVENT_PREDO_PROCESS_PENDING ||
        g_dispatcher->pageWorkers == NULL || g_instance.comm_cxt.predo_cxt.state != REDO_IN_PROGRESS) {
        SpinLockRelease(&(g_instance.comm_cxt.predo_cxt.destroy_lock));
        return result_info;
    }

    for (uint32 i = 0; i < g_dispatcher->pageWorkerCount; i++) {
        if (g_dispatcher->pageWorkers[i] == NULL) {
            break;
        }
        index = g_dispatcher->pageWorkers[i]->index;
        beentry = t_thrd.shemem_ptr_cxt.BackendStatusArray + index;
        tmp_io = beentry->waitInfo.event_info.io_info[event_id - WAIT_EVENT_BUFFILE_READ];
        result_info.total_duration += tmp_io.total_duration;
        result_info.counter += tmp_io.counter;
    }
    SpinLockRelease(&(g_instance.comm_cxt.predo_cxt.destroy_lock));
    return result_info;
}

void redo_dump_all_stats()
{
    RedoPerf *redo = &(g_instance.comm_cxt.predo_cxt.redoPf);
    uint64 redoBytes = redo->read_ptr - redo->redo_start_ptr;
    int64 curr_time = GetCurrentTimestamp();
    uint64 totalTime = curr_time - redo->redo_start_time;
    uint64 speed = 0; /* KB/s */
    if (totalTime > 0) {
        speed = (redoBytes / totalTime) * US_TRANSFER_TO_S / BYTES_TRANSFER_KBYTES;
    }
    ereport(LOG, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
        errmsg("[REDO_STATS]redo_dump_all_stats: the basic statistic during redo are as follows : "
            "redo_start_ptr:%08X/%08X, redo_start_time:%ld, min_recovery_point:%08X/%08X, "
            "read_ptr:%08X/%08X, last_replayed_read_Ptr:%08X/%08X, speed:%lu KB/s",
            (uint32)(redo->redo_start_ptr >> 32), (uint32)redo->redo_start_ptr, redo->redo_start_time,
            (uint32)(redo->min_recovery_point >> 32), (uint32)redo->min_recovery_point, (uint32)(redo->read_ptr >> 32),
            (uint32)redo->read_ptr, (uint32)(redo->last_replayed_end_ptr >> 32), (uint32)redo->last_replayed_end_ptr,
            speed)));

    uint32 type;
    RedoWaitInfo tmp_info;
    for (type = 0; type < WAIT_REDO_NUM; type++) {
        tmp_info = redo_get_io_event(redo_get_event_type_by_wait_type(type));
        ereport(LOG, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
            errmsg("[REDO_STATS]redo_dump_all_stats %s: the event io statistic during redo are as follows : "
                "total_duration:%ld, counter:%ld",
                redo_get_name_by_wait_type(type), tmp_info.total_duration, tmp_info.counter)));
    }

    if (g_dispatcher != NULL) {
        redo_dump_worker_queue_info();
    }
}

void WaitRedoWorkerIdle()
{
    bool allIdle = false;
    instr_time startTime;
    instr_time endTime;
    INSTR_TIME_SET_CURRENT(startTime);
    ereport(LOG, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
        errmsg("WaitRedoWorkerIdle begin, startTime: %lu us", INSTR_TIME_GET_MICROSEC(startTime))));
    while (!allIdle) {
        allIdle = true;
        for (uint32 i = 0; i < g_dispatcher->pageWorkerCount; i++) {
            if (!RedoWorkerIsIdle(g_dispatcher->pageWorkers[i])) {
                allIdle = false;
                break;
            }
        }
    }
    INSTR_TIME_SET_CURRENT(endTime);
    INSTR_TIME_SUBTRACT(endTime, startTime);
    ereport(LOG, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
        errmsg("WaitRedoWorkerIdle end, cost time: %lu us", INSTR_TIME_GET_MICROSEC(endTime))));
}

void SendClearMarkToAllWorkers()
{
    for (uint32 i = 0; i < g_dispatcher->pageWorkerCount; i++) {
        SendPageRedoClearMark(g_dispatcher->pageWorkers[i]);
    }
}

void SendClosefdMarkToAllWorkers()
{
    for (uint32 i = 0; i < g_dispatcher->pageWorkerCount; i++) {
        SendPageRedoClosefdMark(g_dispatcher->pageWorkers[i]);
    }
}

void SendCleanInvalidPageMarkToAllWorkers(RepairFileKey key)
{
    for (uint32 i = 0; i < g_dispatcher->pageWorkerCount; i++) {
        SendPageRedoCleanInvalidPageMark(g_dispatcher->pageWorkers[i], key);
    }
}


static void AddUndoSpaceAndTransGrpWorkersForUHeapRecord(XLogReaderState *record, XlUndoHeader *xlundohdr,
    undo::XlogUndoMeta *xlundometa, const char *commandString)
{
    uint32 undoWorkerId = INVALID_WORKER_ID;
    TransactionId fxid = XLogRecGetXid(record);

    undoWorkerId = GetUndoSpaceWorkerId(UNDO_PTR_GET_ZONE_ID(xlundohdr->urecptr));
    AddWorkerToSet(undoWorkerId);
    elog(DEBUG1, "Dispatch %s xid(%lu) lsn(%016lx) undo worker zid %lu, undoWorkerId %d", commandString,
        fxid, record->EndRecPtr, UNDO_PTR_GET_ZONE_ID(xlundohdr->urecptr), undoWorkerId);


    /* Get redo workers with pages without undo */
    GetWorkersIdWithOutUndoBuffer(record);
}

static void CreateAndAddRedoItemForUHeapRecord(XLogReaderState *record, List *expectedTLIs, bool hasUndoAction)
{
    uint32 workerNum = g_dispatcher->chosedWorkerCount;
    bool enableUndologRedoworker = SUPPORT_USTORE_UNDO_WORKER;
    RedoItem *item = NULL;
    uint32 undoWorkerNum = get_recovery_undozidworkers_num();

    if (enableUndologRedoworker && hasUndoAction) {
        item = CreateRedoItem(record, workerNum - undoWorkerNum, ANY_WORKER, expectedTLIs, 0, true);
        item->replay_undo = true;
    } else {
        if (enableUndologRedoworker)
            item = CreateRedoItem(record, workerNum, ANY_WORKER, expectedTLIs, 0, true);
        else
            item = CreateRedoItem(record, workerNum, USTORE_WORKER, expectedTLIs, 0, true);
    }

    for (uint32 i = 0; i < g_dispatcher->pageWorkerCount; i++) {
        if (g_dispatcher->chosedWorkerIds[i] > 0) {
            AddPageRedoItem(g_dispatcher->pageWorkers[i], item);
            elog(DEBUG1, "Dispatch page worker %d", i);
        }
    }
}
static char *ReachXlUndoHeaderEnd(XlUndoHeader *xlundohdr)
{
    char *currLogPtr = ((char *)xlundohdr + SizeOfXLUndoHeader);
    if ((xlundohdr->flag & XLOG_UNDO_HEADER_HAS_SUB_XACT) != 0) {
        currLogPtr += sizeof(bool); 
    }
    if ((xlundohdr->flag & XLOG_UNDO_HEADER_HAS_BLK_PREV) != 0) {
        currLogPtr += sizeof(UndoRecPtr);
    }
    if ((xlundohdr->flag & XLOG_UNDO_HEADER_HAS_PREV_URP) != 0 ) {
        currLogPtr += sizeof(UndoRecPtr);
    }
    if ((xlundohdr->flag & XLOG_UNDO_HEADER_HAS_PARTITION_OID) != 0) {
        currLogPtr += sizeof(Oid);
    }
    return currLogPtr;
}

static bool DispatchUHeapRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    uint8 op = info & XLOG_UHEAP_OPMASK;
    bool needsCreateItem = true; /* Need to set to false if need to skip undo */
    bool hasUndoAction = true;

    /* We need to handle freeze TD slot later */
    switch (op) {
        case XLOG_UHEAP_INSERT: {
            XlUHeapInsert *xlrec = (XlUHeapInsert *)XLogRecGetData(record);
            XlUndoHeader *xlundohdr = (XlUndoHeader *)((char *)xlrec + SizeOfUHeapInsert);
            char *currLogPtr = ReachXlUndoHeaderEnd(xlundohdr);
            undo::XlogUndoMeta *xlundometa = (undo::XlogUndoMeta *)((char *)currLogPtr);
            AddUndoSpaceAndTransGrpWorkersForUHeapRecord(record, xlundohdr, xlundometa, "INSERT");
            break;
        }
        case XLOG_UHEAP_DELETE: {
            XlUHeapDelete *xlrec = (XlUHeapDelete *)XLogRecGetData(record);
            XlUndoHeader *xlundohdr = (XlUndoHeader *)((char *)xlrec + SizeOfUHeapDelete);
            char *currLogPtr = ReachXlUndoHeaderEnd(xlundohdr);
            undo::XlogUndoMeta *xlundometa = (undo::XlogUndoMeta *)((char *)currLogPtr);
            AddUndoSpaceAndTransGrpWorkersForUHeapRecord(record, xlundohdr, xlundometa, "DELETE");
            break;
        }
        case XLOG_UHEAP_UPDATE: {
            XlUHeapUpdate *xlrec = (XlUHeapUpdate *)XLogRecGetData(record);
            XlUndoHeader *xlundohdr = (XlUndoHeader *)((char *)xlrec + SizeOfUHeapUpdate);
            char *currLogPtr = ReachXlUndoHeaderEnd(xlundohdr);
            if (xlrec->flags & XLZ_NON_INPLACE_UPDATE) {
                XlUndoHeader *newxlundohdr = (XlUndoHeader *)((char *)currLogPtr);
                currLogPtr = ReachXlUndoHeaderEnd(newxlundohdr);
            }
            undo::XlogUndoMeta *xlundometa = (undo::XlogUndoMeta *)currLogPtr;
            AddUndoSpaceAndTransGrpWorkersForUHeapRecord(record, xlundohdr, xlundometa, "UPDATE");
            break;
        }
        case XLOG_UHEAP_FREEZE_TD_SLOT:
        case XLOG_UHEAP_INVALID_TD_SLOT:
        case XLOG_UHEAP_CLEAN: {
            GetWorkersIdWithOutUndoBuffer(record);
            hasUndoAction = false;
            break;
        }
        case XLOG_UHEAP_MULTI_INSERT: {
            XlUndoHeader *xlundohdr = (XlUndoHeader *)XLogRecGetData(record);
            char *currLogPtr = ReachXlUndoHeaderEnd(xlundohdr);
            currLogPtr = (char *)currLogPtr + sizeof(UndoRecPtr);
            undo::XlogUndoMeta *xlundometa = (undo::XlogUndoMeta *)currLogPtr;
            AddUndoSpaceAndTransGrpWorkersForUHeapRecord(record, xlundohdr, xlundometa, "MULTI_INSERT");
            break;
        }
        default:
            elog(ERROR, "Invalid op in DispatchUHeapRecord");
    }

    if (needsCreateItem) {
        CreateAndAddRedoItemForUHeapRecord(record, expectedTLIs, hasUndoAction);
    }

    return false;
}

static void CreateAndAddRedoItemForUHeap2Record(XLogReaderState *record, List *expectedTLIs)
{
    uint32 workerNum = g_dispatcher->chosedWorkerCount;
    RedoItem *item = NULL;

    if (!SUPPORT_USTORE_UNDO_WORKER)
        item = CreateRedoItem(record, workerNum, ANY_WORKER, expectedTLIs, 0, true);
    else
        item = CreateRedoItem(record, workerNum, USTORE_WORKER, expectedTLIs, 0, true);

    for (uint32 i = 0; i < g_dispatcher->pageWorkerCount; i++) {
        if (g_dispatcher->chosedWorkerIds[i] > 0) {
            AddPageRedoItem(g_dispatcher->pageWorkers[i], item);
            elog(DEBUG1, "Dispatch page worker %d", i);
        }
    }
}

static bool DispatchUHeap2Record(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    uint8 op = info & XLOG_UHEAP_OPMASK;
    bool needsCreateItem = true; /* Need to set to false if need to skip undo */
    TransactionId fxid = XLogRecGetXid(record);

    /*
     * These operations don't overwrite MVCC data so no conflict processing is
     * required. The ones in heap2 rmgr do.
     */
    switch (op) {
        case XLOG_UHEAP2_BASE_SHIFT: {
            XlUHeapBaseShift *xlrec = (XlUHeapBaseShift *)XLogRecGetData(record);
            elog(DEBUG1, "Dispatch BASE_SHIFT xid(%lu) lsn(%016lx) delta %ld", fxid, record->EndRecPtr, xlrec->delta);

            GetWorkersIdWithOutUndoBuffer(record);
            break;
        }
        case XLOG_UHEAP2_FREEZE: {
            XlUHeapFreeze *xlrec = (XlUHeapFreeze *)XLogRecGetData(record);
            elog(DEBUG1, "Dispatch FREEZE xid(%lu) lsn(%016lx) cutoff_xid %lu", fxid, record->EndRecPtr,
                xlrec->cutoff_xid);

            GetWorkersIdWithOutUndoBuffer(record);
            break;
        }
        case XLOG_UHEAP2_EXTEND_TD_SLOTS: {
            elog(DEBUG1, "Dispatch EXTEND_TD_SLOTS xid(%lu) lsn(%016lx)", fxid, record->EndRecPtr);
            GetWorkersIdWithOutUndoBuffer(record);
            break;
        }
        default: {
            elog(ERROR, "Invalid op in DispatchUHeap2Record: %u", (uint8)op);
        }
    }

    if (needsCreateItem) {
        CreateAndAddRedoItemForUHeap2Record(record, expectedTLIs);
    }
    return false;
}

static void CreateAndAddRedoItemForUndoActionRecords(XLogReaderState *record, List *expectedTLIs,
    const char *commandString)
{
    RedoItem *item = NULL;
    if (!SUPPORT_USTORE_UNDO_WORKER)
        item = CreateRedoItem(record, g_dispatcher->chosedWorkerCount, USTORE_WORKER, expectedTLIs, 0, true);
    else
        item = CreateRedoItem(record, g_dispatcher->chosedWorkerCount, ANY_WORKER, expectedTLIs, 0, true);

    for (uint32 i = 0; i < g_dispatcher->pageWorkerCount; i++) {
        if (g_dispatcher->chosedWorkerIds[i] > 0) {
            AddPageRedoItem(g_dispatcher->pageWorkers[i], item);
            elog(DEBUG1, "Dispatch %s page worker %d", commandString, i);
        }
    }
}

static bool DispatchUndoActionRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    uint8 op = info & XLOG_UHEAP_OPMASK;

    if (op == XLOG_UHEAPUNDO_RESET_SLOT) {
        GetWorkersIdWithOutUndoBuffer(record);
        CreateAndAddRedoItemForUndoActionRecords(record, expectedTLIs, "UHEAPUNDO_RESET_SLOT");
    } else if (op == XLOG_UHEAPUNDO_PAGE) {
        GetWorkersIdWithOutUndoBuffer(record);
        CreateAndAddRedoItemForUndoActionRecords(record, expectedTLIs, "XLOG_UHEAPUNDO_PAGE");
    } else if (op == XLOG_UHEAPUNDO_ABORT_SPECINSERT) {
        GetWorkersIdWithOutUndoBuffer(record);
        CreateAndAddRedoItemForUndoActionRecords(record, expectedTLIs, "XLOG_UHEAPUNDO_ABORT_SPECINSERT");
    }

    return false;
}

static void CreateAndAddRedoItemForUHeapUndoRecord(XLogReaderState *record, List *expectedTLIs)
{
    RedoItem *item = CreateRedoItem(record, g_dispatcher->chosedWorkerCount, ANY_WORKER, expectedTLIs, 0, true);
    for (uint32 i = 0; i < g_dispatcher->pageWorkerCount; i++) {
        if (g_dispatcher->chosedWorkerIds[i] > 0) {
            AddPageRedoItem(g_dispatcher->pageWorkers[i], item);
            elog(DEBUG1, "Dispatch page worker %d", i);
        }
    }
}

static bool DispatchUHeapUndoRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    uint8 op = info & XLOG_UHEAP_OPMASK;

    switch (op) {
        case XLOG_UNDO_DISCARD: {
            undo::XlogUndoDiscard *xlrec = (undo::XlogUndoDiscard *)XLogRecGetData(record);
            uint32 undoWorkerId = GetUndoSpaceWorkerId(UNDO_PTR_GET_ZONE_ID(xlrec->startSlot));
            AddWorkerToSet(undoWorkerId);
            elog(DEBUG1, "Dispatch UNDO_DISCARD xid(%lu) lsn(%016lx) undo worker zid %d, undoWorkerId %d",
                XLogRecGetXid(record), record->EndRecPtr, (int)UNDO_PTR_GET_ZONE_ID(xlrec->startSlot), undoWorkerId);
            break;
        }
        case XLOG_UNDO_UNLINK:
        case XLOG_SLOT_UNLINK: {
            undo::XlogUndoUnlink *xlrec = (undo::XlogUndoUnlink *)XLogRecGetData(record);
            int zoneId = UNDO_PTR_GET_ZONE_ID(xlrec->head);
            uint32 undoWorkerId = GetUndoSpaceWorkerId(zoneId);
            AddWorkerToSet(undoWorkerId);
            elog(DEBUG1, "Dispatch UNDO_UNLINK xid(%lu) lsn(%016lx) undo worker zid %d, undoWorkerId %d",
                XLogRecGetXid(record), record->EndRecPtr, zoneId, undoWorkerId);

            break;
        }
        case XLOG_UNDO_EXTEND:
        case XLOG_SLOT_EXTEND: {
            undo::XlogUndoExtend *xlrec = (undo::XlogUndoExtend *)XLogRecGetData(record);
            int zoneId = UNDO_PTR_GET_ZONE_ID(xlrec->tail);
            uint32 undoWorkerId = GetUndoSpaceWorkerId(zoneId);
            AddWorkerToSet(undoWorkerId);
            elog(DEBUG1, "Dispatch UNDO_EXTEND xid(%lu) lsn(%016lx) undo worker zid %d, undoWorkerId %d",
                XLogRecGetXid(record), record->EndRecPtr, zoneId, undoWorkerId);

            break;
        }
        default: {
            elog(ERROR, "Invalid op in DispatchUHeapUndoRecord: %u", (uint8)op);
        }
    }

    CreateAndAddRedoItemForUHeapUndoRecord(record, expectedTLIs);

    return false;
}

static bool DispatchRollbackFinishRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    uint8 op = info & XLOG_UHEAP_OPMASK;

    if (op == XLOG_ROLLBACK_FINISH) {
        undo::XlogRollbackFinish *xlrec = (undo::XlogRollbackFinish *)XLogRecGetData(record);
        uint32 undoWorkerId = 0;
        undoWorkerId = GetUndoSpaceWorkerId(UNDO_PTR_GET_ZONE_ID(xlrec->slotPtr));
        AddWorkerToSet(undoWorkerId);
        elog(DEBUG1, "Dispatch ROLLBACK_FINISH xid(%lu) lsn(%016lx) undo worker zid %d, undoWorkerId %d",
            XLogRecGetXid(record), record->EndRecPtr, (int)UNDO_PTR_GET_ZONE_ID(xlrec->slotPtr), undoWorkerId);

        RedoItem *item = CreateRedoItem(record, g_dispatcher->chosedWorkerCount, ANY_WORKER, expectedTLIs, 0, true);
        for (uint32 i = 0; i < g_dispatcher->pageWorkerCount; i++) {
            if (g_dispatcher->chosedWorkerIds[i] > 0) {
                AddPageRedoItem(g_dispatcher->pageWorkers[i], item);
                elog(DEBUG1, "Dispatch page worker %d", i);
            }
        }
    }

    return false;
}

static uint32 GetUndoSpaceWorkerId(int zid)
{
    uint32 workerCount = GetPageWorkerCount();
    uint32 undoZidWorkersNum = get_recovery_undozidworkers_num();
    int firstUndoLogWorker = (workerCount - undoZidWorkersNum);

    if (workerCount == 0 || undoZidWorkersNum == 0) {
        return ANY_WORKER;
    }

    if (SUPPORT_USTORE_UNDO_WORKER) {
        return (tag_hash(&zid, sizeof(zid)) % undoZidWorkersNum + firstUndoLogWorker);
    } else {
        return (tag_hash(&zid, sizeof(zid)) % workerCount);
    }
}

/* Handle SIGHUP and SIGTERM signals of startup process */
static void HandleStartupProcInterruptsForParallelRedo(void)
{
    /*
     * Check if we were requested to re-read config file.
     */
    if (t_thrd.startup_cxt.got_SIGHUP) {
        t_thrd.startup_cxt.got_SIGHUP = false;
        ProcessConfigFile(PGC_SIGHUP);
        SendSingalToPageWorker(SIGHUP);
    }

    if (ENABLE_DMS && t_thrd.startup_cxt.shutdown_requested && SmartShutdown != g_instance.status &&
        g_instance.dms_cxt.SSRecoveryInfo.startup_need_exit_normally) {
        crps_destory_ctxs();
        proc_exit(0);
    }

    /*
     * Check if we were requested to exit without finishing recovery.
     */
    if (t_thrd.startup_cxt.shutdown_requested && SmartShutdown != g_instance.status) {
        proc_exit(1);
    }

    /*
     * Emergency bailout if postmaster has died.  This is to avoid the
     * necessity for manual cleanup of all postmaster children.
     */
    if (IsUnderPostmaster && !PostmasterIsAlive())
        gs_thread_exit(1);
}

bool in_full_sync_dispatch(void)
{
    if (!g_dispatcher || !g_instance.attr.attr_storage.enable_batch_dispatch)
        return true;
    return g_dispatcher->full_sync_dispatch;
}

}
