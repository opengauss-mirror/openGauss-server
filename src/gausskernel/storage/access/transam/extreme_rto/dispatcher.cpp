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
 * -------------------------------------------------------------------------
 *
 * dispatcher.cpp
 *      Parallel recovery has a centralized log dispatcher which runs inside
 *      the StartupProcess.  The dispatcher is responsible for managing the
 *      life cycle of PageRedoWorkers and the TxnRedoWorker, analyzing log
 *      records and dispatching them to workers for processing.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/transam/extreme_rto/dispatcher.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"
#include "postmaster/startup.h"
#include "access/clog.h"
#include "access/xact.h"
#include "access/xlog_internal.h"
#include "access/nbtree.h"
#include "access/xlogreader.h"
#include "access/gist_private.h"
#include "access/multixact.h"
#include "access/spgist_private.h"
#include "access/gin_private.h"
#include "access/xlogutils.h"
#include "access/gin.h"

#include "catalog/storage_xlog.h"
#include "storage/buf_internals.h"
#include "storage/ipc.h"
#include "storage/standby.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/palloc.h"
#include "utils/guc.h"
#include "utils/relmapper.h"

#include "portability/instr_time.h"

#include "access/extreme_rto/dispatcher.h"
#include "access/extreme_rto/page_redo.h"
#include "access/multi_redo_api.h"

#include "access/extreme_rto/txn_redo.h"
#include "access/extreme_rto/spsc_blocking_queue.h"
#include "access/extreme_rto/redo_item.h"
#include "access/extreme_rto/batch_redo.h"

#include "catalog/storage.h"
#include <sched.h>
#include "utils/memutils.h"

#include "commands/dbcommands.h"
#include "commands/tablespace.h"
#include "commands/sequence.h"

#include "replication/slot.h"
#include "gssignal/gs_signal.h"
#include "utils/atomic.h"
#include "pgstat.h"

#ifdef PGXC
#include "pgxc/pgxc.h"
#endif

extern THR_LOCAL bool redo_oldversion_xlog;

namespace extreme_rto {
LogDispatcher* g_dispatcher = NULL;

static const int XLOG_INFO_SHIFT_SIZE = 4; /* xlog info flag shift size */

static const int32 MAX_PENDING = 1;
static const int32 MAX_PENDING_STANDBY = 1;
static const int32 ITEM_QUQUE_SIZE_RATIO = 10;

static const uint32 EXIT_WAIT_DELAY = 100; /* 100 us */
uint32 g_triggeredstate = TRIGGER_NORMAL;

typedef void* (*GetStateFunc)(PageRedoWorker* worker);

static void AddSlotToPLSet(uint32);
static void** CollectStatesFromWorkers(GetStateFunc);
static void GetSlotIds(XLogReaderState* record, uint32 designatedSlot, bool rnodedispatch);
static LogDispatcher* CreateDispatcher();
static void DestroyRecoveryWorkers();

static void DispatchRecordWithPages(XLogReaderState*, List*, bool);
static void DispatchRecordWithoutPage(XLogReaderState*, List*);
static void DispatchTxnRecord(XLogReaderState*, List*, TimestampTz, bool, bool);
static void StartPageRedoWorkers(uint32);
static void StopRecoveryWorkers(int, Datum);
static bool XLogWillChangeStandbyState(const XLogReaderState*);
static bool StandbyWillChangeStandbyState(const XLogReaderState*);
static void DispatchToSpecPageWorker(XLogReaderState* record, List* expectedTLIs, bool waittrxnsync);

static bool DispatchXLogRecord(XLogReaderState* record, List* expectedTLIs, TimestampTz recordXTime);
static bool DispatchXactRecord(XLogReaderState* record, List* expectedTLIs, TimestampTz recordXTime);
static bool DispatchSmgrRecord(XLogReaderState* record, List* expectedTLIs, TimestampTz recordXTime);
static bool DispatchCLogRecord(XLogReaderState* record, List* expectedTLIs, TimestampTz recordXTime);
static bool DispatchHashRecord(XLogReaderState* record, List* expectedTLIs, TimestampTz recordXTime);
static bool DispatchDataBaseRecord(XLogReaderState* record, List* expectedTLIs, TimestampTz recordXTime);
static bool DispatchTableSpaceRecord(XLogReaderState* record, List* expectedTLIs, TimestampTz recordXTime);
static bool DispatchMultiXactRecord(XLogReaderState* record, List* expectedTLIs, TimestampTz recordXTime);
static bool DispatchRelMapRecord(XLogReaderState* record, List* expectedTLIs, TimestampTz recordXTime);
static bool DispatchStandbyRecord(XLogReaderState* record, List* expectedTLIs, TimestampTz recordXTime);
static bool DispatchHeap2Record(XLogReaderState* record, List* expectedTLIs, TimestampTz recordXTime);
static bool DispatchHeapRecord(XLogReaderState* record, List* expectedTLIs, TimestampTz recordXTime);
static bool DispatchSeqRecord(XLogReaderState* record, List* expectedTLIs, TimestampTz recordXTime);
static bool DispatchGinRecord(XLogReaderState* record, List* expectedTLIs, TimestampTz recordXTime);
static bool DispatchGistRecord(XLogReaderState* record, List* expectedTLIs, TimestampTz recordXTime);
static bool DispatchSpgistRecord(XLogReaderState* record, List* expectedTLIs, TimestampTz recordXTime);
static bool DispatchRepSlotRecord(XLogReaderState* record, List* expectedTLIs, TimestampTz recordXTime);
static bool DispatchHeap3Record(XLogReaderState* record, List* expectedTLIs, TimestampTz recordXTime);
static bool DispatchDefaultRecord(XLogReaderState* record, List* expectedTLIs, TimestampTz recordXTime);
#ifdef ENABLE_MULTIPLE_NODES
static bool DispatchBarrierRecord(XLogReaderState* record, List* expectedTLIs, TimestampTz recordXTime);
#endif
static bool DispatchMotRecord(XLogReaderState* record, List* expectedTLIs, TimestampTz recordXTime);
static bool DispatchBtreeRecord(XLogReaderState* record, List* expectedTLIs, TimestampTz recordXTime);
static bool RmgrRecordInfoValid(XLogReaderState* record, uint8 minInfo, uint8 maxInfo);
static bool RmgrGistRecordInfoValid(XLogReaderState* record, uint8 minInfo, uint8 maxInfo);
static XLogReaderState* GetXlogReader(XLogReaderState* readerState);
void CopyDataFromOldReader(XLogReaderState* newReaderState, const XLogReaderState* oldReaderState);

/* dispatchTable must consistent with RmgrTable */
static const RmgrDispatchData g_dispatchTable[RM_MAX_ID + 1] = {
    {DispatchXLogRecord, RmgrRecordInfoValid, RM_XLOG_ID, XLOG_CHECKPOINT_SHUTDOWN, XLOG_FPI},
    {DispatchXactRecord, RmgrRecordInfoValid, RM_XACT_ID, XLOG_XACT_COMMIT, XLOG_XACT_COMMIT_COMPACT},
    {DispatchSmgrRecord, RmgrRecordInfoValid, RM_SMGR_ID, XLOG_SMGR_CREATE, XLOG_SMGR_TRUNCATE},
    {DispatchCLogRecord, RmgrRecordInfoValid, RM_CLOG_ID, CLOG_ZEROPAGE, CLOG_TRUNCATE},
    {DispatchDataBaseRecord, RmgrRecordInfoValid, RM_DBASE_ID, XLOG_DBASE_CREATE, XLOG_DBASE_DROP},
    {DispatchTableSpaceRecord, RmgrRecordInfoValid, RM_TBLSPC_ID, XLOG_TBLSPC_CREATE, XLOG_TBLSPC_RELATIVE_CREATE},
    {DispatchMultiXactRecord,
        RmgrRecordInfoValid,
        RM_MULTIXACT_ID,
        XLOG_MULTIXACT_ZERO_OFF_PAGE,
        XLOG_MULTIXACT_INT64_PAGENO},
    {DispatchRelMapRecord, RmgrRecordInfoValid, RM_RELMAP_ID, XLOG_RELMAP_UPDATE, XLOG_RELMAP_UPDATE},
#ifndef ENABLE_MULTIPLE_NODES
    {DispatchStandbyRecord, RmgrRecordInfoValid, RM_STANDBY_ID, XLOG_STANDBY_LOCK, XLOG_STANDBY_CSN_ABORTED},
#else
    {DispatchStandbyRecord, RmgrRecordInfoValid, RM_STANDBY_ID, XLOG_STANDBY_LOCK, XLOG_STANDBY_CSN},
#endif
    {DispatchHeap2Record, RmgrRecordInfoValid, RM_HEAP2_ID, XLOG_HEAP2_FREEZE, XLOG_HEAP2_LOGICAL_NEWPAGE},
    {DispatchHeapRecord, RmgrRecordInfoValid, RM_HEAP_ID, XLOG_HEAP_INSERT, XLOG_HEAP_INPLACE},
    {DispatchBtreeRecord, RmgrRecordInfoValid, RM_BTREE_ID, XLOG_BTREE_INSERT_LEAF, XLOG_BTREE_REUSE_PAGE},
    {DispatchHashRecord, NULL, RM_HASH_ID, 0, 0},
    {DispatchGinRecord, RmgrRecordInfoValid, RM_GIN_ID, XLOG_GIN_CREATE_INDEX, XLOG_GIN_VACUUM_DATA_LEAF_PAGE},
    /* XLOG_GIST_PAGE_DELETE is not used and info isn't continus  */
    {DispatchGistRecord, RmgrGistRecordInfoValid, RM_GIST_ID, 0, 0},
    {DispatchSeqRecord, RmgrRecordInfoValid, RM_SEQ_ID, XLOG_SEQ_LOG, XLOG_SEQ_LOG},
    {DispatchSpgistRecord, RmgrRecordInfoValid, RM_SPGIST_ID, XLOG_SPGIST_CREATE_INDEX, XLOG_SPGIST_VACUUM_REDIRECT},
    {DispatchRepSlotRecord, RmgrRecordInfoValid, RM_SLOT_ID, XLOG_SLOT_CREATE, XLOG_TERM_LOG},
    {DispatchHeap3Record, RmgrRecordInfoValid, RM_HEAP3_ID, XLOG_HEAP3_NEW_CID, XLOG_HEAP3_REWRITE},
#ifdef ENABLE_MULTIPLE_NODES
    {DispatchBarrierRecord, NULL, RM_BARRIER_ID, 0, 0},
#endif
    {DispatchMotRecord, NULL, RM_MOT_ID, 0, 0},
};

void UpdateDispatcherStandbyState(HotStandbyState* state)
{
    if ((get_real_recovery_parallelism() > 1) && (GetBatchCount() > 0)) {
        *state = (HotStandbyState)pg_atomic_read_u32(&(g_dispatcher->standbyState));
    }
}

/* Run from the dispatcher and txn worker thread. */
bool OnHotStandBy()
{
    return t_thrd.xlog_cxt.standbyState >= STANDBY_INITIALIZED;
}

const int REDO_WAIT_SLEEP_TIME = 5000; /* 5ms */
const int MAX_REDO_WAIT_LOOP = 24000;  /* 5ms*24000 = 2min */

uint32 GetReadyWorker()
{
    uint32 readyWorkerCnt = 0;

    for (uint32 i = 0; i < g_instance.comm_cxt.predo_cxt.totalNum; i++) {
        uint32 state = pg_atomic_read_u32(&(g_instance.comm_cxt.predo_cxt.pageRedoThreadStatusList[i].threadState));
        if (state >= PAGE_REDO_WORKER_READY) {
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
            ereport(LOG,
                (errmodule(MOD_REDO),
                    errcode(ERRCODE_LOG),
                    errmsg("WaitWorkerReady total worker count:%u, readyWorkerCnt:%u",
                        g_dispatcher->allWorkersCnt,
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
        ereport(PANIC,
            (errmodule(MOD_REDO),
                errcode(ERRCODE_LOG),
                errmsg("WaitWorkerReady failed, no worker is ready for work. totalWorkerCount :%u",
                    g_dispatcher->allWorkersCnt)));
    }

    /* RTO_DEMO */
    if (readyWorkerCnt != g_dispatcher->allWorkersCnt) {
        ereport(PANIC,
            (errmodule(MOD_REDO),
                errcode(ERRCODE_LOG),
                errmsg("WaitWorkerReady total thread count:%u, readyWorkerCnt:%u, not all thread ready",
                    g_dispatcher->allWorkersCnt,
                    readyWorkerCnt)));
    }
}

void CheckAlivePageWorkers()
{
    for (uint32 i = 0; i < MAX_RECOVERY_THREAD_NUM; ++i) {
        if (g_instance.comm_cxt.predo_cxt.pageRedoThreadStatusList[i].threadState != PAGE_REDO_WORKER_INVALID) {
            ereport(PANIC,
                (errmodule(MOD_REDO),
                    errcode(ERRCODE_LOG),
                    errmsg("CheckAlivePageWorkers: thread %lu is still alive",
                        g_instance.comm_cxt.predo_cxt.pageRedoThreadStatusList[i].threadId)));
        }
        g_instance.comm_cxt.predo_cxt.pageRedoThreadStatusList[i].threadId = 0;
    }
    g_instance.comm_cxt.predo_cxt.totalNum = 0;
}

void AllocRecordReadBuffer(XLogReaderState* xlogreader, uint32 privateLen)
{
    XLogReaderState* initreader;
    errno_t errorno = EOK;

    initreader = GetXlogReader(xlogreader);
    initreader->isPRProcess = true;
    g_dispatcher->recordstate.startreadworker = READ_WORKER_STOP;
    g_dispatcher->recordstate.readsegbuf = (char*)palloc0(XLOG_SEG_SIZE * MAX_ALLOC_SEGNUM);
    g_dispatcher->recordstate.readprivate = (void*)palloc0(MAXALIGN(privateLen));
    errorno = memset_s(g_dispatcher->recordstate.readprivate, MAXALIGN(privateLen), 0, MAXALIGN(privateLen));
    securec_check(errorno, "", "");
    g_dispatcher->recordstate.errormsg_buf = (char*)palloc0(MAX_ERRORMSG_LEN + 1);
    g_dispatcher->recordstate.errormsg_buf[0] = '\0';

    char* readsegbuf = g_dispatcher->recordstate.readsegbuf;
    for (uint32 i = 0; i < MAX_ALLOC_SEGNUM; i++) {
        g_dispatcher->recordstate.xlogsegarray[i].readsegbuf = readsegbuf;
        readsegbuf += XLOG_SEG_SIZE;
        g_dispatcher->recordstate.xlogsegarray[i].bufState = NONE;
    }

    g_dispatcher->recordstate.applyindex = 0;

    g_dispatcher->recordstate.readindex = 0;

    g_dispatcher->recordstate.xlogsegarray[0].segno = xlogreader->readSegNo;
    g_dispatcher->recordstate.xlogsegarray[0].segoffset = xlogreader->readOff;
    g_dispatcher->recordstate.xlogsegarray[0].readlen = xlogreader->readOff + xlogreader->readLen;

    initreader->readBuf =
        g_dispatcher->recordstate.xlogsegarray[0].readsegbuf + g_dispatcher->recordstate.xlogsegarray[0].segoffset;

    errorno = memcpy_s(initreader->readBuf, XLOG_BLCKSZ, xlogreader->readBuf, xlogreader->readLen);
    securec_check(errorno, "", "");
    initreader->errormsg_buf = g_dispatcher->recordstate.errormsg_buf;
    initreader->private_data = g_dispatcher->recordstate.readprivate;
    CopyDataFromOldReader(initreader, xlogreader);
    g_dispatcher->recordstate.initreader = initreader;

    g_recordbuffer = &g_dispatcher->recordstate;
}

/* Run from the dispatcher thread. */
void StartRecoveryWorkers(XLogReaderState* xlogreader, uint32 privateLen)
{
    if (get_real_recovery_parallelism() > 1) {
        if (t_thrd.xlog_cxt.StandbyModeRequested) {
            ReLeaseRecoveryLatch();
        }
        CheckAlivePageWorkers();
        g_dispatcher = CreateDispatcher();
        g_dispatcher->oldCtx = MemoryContextSwitchTo(g_instance.comm_cxt.predo_cxt.parallelRedoCtx);
        g_dispatcher->maxItemNum = (get_real_recovery_parallelism() + 1) * PAGE_WORK_QUEUE_SIZE * ITEM_QUQUE_SIZE_RATIO;
        /* alloc for record readbuf */
        AllocRecordReadBuffer(xlogreader, privateLen);
        StartPageRedoWorkers(get_real_recovery_parallelism());

        ereport(LOG,
            (errmodule(MOD_REDO),
                errcode(ERRCODE_LOG),
                errmsg("[PR]: max=%d, thrd=%d",
                    g_instance.attr.attr_storage.max_recovery_parallelism,
                    get_real_recovery_parallelism())));
        WaitWorkerReady();
        SpinLockAcquire(&(g_instance.comm_cxt.predo_cxt.rwlock));
        g_instance.comm_cxt.predo_cxt.state = REDO_IN_PROGRESS;
        SpinLockRelease(&(g_instance.comm_cxt.predo_cxt.rwlock));
        on_shmem_exit(StopRecoveryWorkers, 0);
    }
}

void DumpDispatcher()
{
    knl_parallel_redo_state state;
    PageRedoPipeline* pl = NULL;
    state = g_instance.comm_cxt.predo_cxt.state;
    if ((get_real_recovery_parallelism() > 1) && (GetBatchCount() > 0)) {
        ereport(LOG,
            (errmodule(MOD_REDO),
                errcode(ERRCODE_LOG),
                errmsg("[REDO_LOG_TRACE]dispatcher : totalWorkerCount %d, state %u, curItemNum %u, maxItemNum %u",
                    get_real_recovery_parallelism(),
                    (uint32)state,
                    g_dispatcher->curItemNum,
                    g_dispatcher->maxItemNum)));

        for (uint32 i = 0; i < g_dispatcher->pageLineNum; ++i) {
            pl = &(g_dispatcher->pageLines[i]);
            DumpPageRedoWorker(pl->batchThd);
            DumpPageRedoWorker(pl->managerThd);
            for (uint32 j = 0; j < pl->redoThdNum; j++) {
                DumpPageRedoWorker(pl->redoThd[j]);
            }
        }
        DumpPageRedoWorker(g_dispatcher->trxnLine.managerThd);
        DumpPageRedoWorker(g_dispatcher->trxnLine.redoThd);
        DumpXlogCtl();
    }
}

List* CheckImcompleteAction(List* imcompleteActionList)
{
    /* RTO_DEMO test */
    return NULL;
}

/* Run from the dispatcher thread. */
static LogDispatcher* CreateDispatcher()
{
    MemoryContext ctx = AllocSetContextCreate(t_thrd.top_mem_cxt,
        "ParallelRecoveryDispatcher",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE,
        SHARED_CONTEXT);

    LogDispatcher* newDispatcher = (LogDispatcher*)MemoryContextAllocZero(ctx, sizeof(LogDispatcher));

    g_instance.comm_cxt.predo_cxt.parallelRedoCtx = ctx;
    SpinLockAcquire(&(g_instance.comm_cxt.predo_cxt.rwlock));
    g_instance.comm_cxt.predo_cxt.state = REDO_STARTING_BEGIN;
    SpinLockRelease(&(g_instance.comm_cxt.predo_cxt.rwlock));
    if (OnHotStandBy())
        newDispatcher->pendingMax = MAX_PENDING_STANDBY;
    else
        newDispatcher->pendingMax = MAX_PENDING; /* one batch, one recorder */
    newDispatcher->totalCostTime = 0;
    newDispatcher->txnCostTime = 0;
    newDispatcher->pprCostTime = 0;
    newDispatcher->syncEnterCount = 0;
    newDispatcher->syncExitCount = 0;

    pg_atomic_init_u32(&(newDispatcher->standbyState), STANDBY_INITIALIZED);
    return newDispatcher;
}

void RedoRoleInit(PageRedoWorker** dstWk, PageRedoWorker* srcWk, RedoRole role, uint32 slotId)
{
    *dstWk = srcWk;
    (*dstWk)->role = role;
    (*dstWk)->slotId = slotId;

    if (role == REDO_PAGE_MNG) {
        srcWk->redoItemHash = PRRedoItemHashInitialize(CurrentMemoryContext);
    }
}

/* Run from the dispatcher thread. */
static void StartPageRedoWorkers(uint32 totalThrdNum)
{
    uint32 batchNum = get_batch_redo_num();
    uint32 batchWorkerPerMng = get_page_redo_worker_num_per_manager();
    uint32 workerCnt = 0;
    PageRedoWorker** tmpWorkers;
    uint32 started;
    ereport(LOG,
        (errmsg("StartPageRedoWorkers, totalThrdNum:%u, "
                "batchNum:%u, batchWorkerPerMng is %u",
            totalThrdNum,
            batchNum,
            batchWorkerPerMng)));

    g_dispatcher->allWorkers = (PageRedoWorker**)palloc(sizeof(PageRedoWorker*) * totalThrdNum);
    g_dispatcher->allWorkersCnt = totalThrdNum;
    g_dispatcher->pageLines = (PageRedoPipeline*)palloc(sizeof(PageRedoPipeline) * batchNum);

    for (started = 0; started < totalThrdNum; started++) {
        g_dispatcher->allWorkers[started] = CreateWorker(started);
        if (g_dispatcher->allWorkers[started] == NULL) {
            ereport(PANIC,
                (errmodule(MOD_REDO),
                    errcode(ERRCODE_LOG),
                    errmsg("[REDO_LOG_TRACE]StartPageRedoWorkers CreateWorker failed, started:%u", started)));
        }
    }
    tmpWorkers = g_dispatcher->allWorkers;
    for (uint32 i = 0; i < batchNum; i++) {
        RedoRoleInit(&(g_dispatcher->pageLines[i].batchThd), tmpWorkers[workerCnt++], REDO_BATCH, i);
        RedoRoleInit(&(g_dispatcher->pageLines[i].managerThd), tmpWorkers[workerCnt++], REDO_PAGE_MNG, i);
        g_dispatcher->pageLines[i].redoThd = (PageRedoWorker**)palloc(sizeof(PageRedoWorker*) * batchWorkerPerMng);
        g_dispatcher->pageLines[i].chosedRTIds = (uint32*)palloc(sizeof(uint32) * batchWorkerPerMng);
        g_dispatcher->pageLines[i].chosedRTCnt = 0;
        for (uint32 j = 0; j < batchWorkerPerMng; j++) {
            RedoRoleInit(&(g_dispatcher->pageLines[i].redoThd[j]), tmpWorkers[workerCnt++], REDO_PAGE_WORKER, j);
        }
        g_dispatcher->pageLines[i].redoThdNum = batchWorkerPerMng;
    }

    RedoRoleInit(&(g_dispatcher->trxnLine.managerThd), tmpWorkers[workerCnt++], REDO_TRXN_MNG, 0);
    RedoRoleInit(&(g_dispatcher->trxnLine.redoThd), tmpWorkers[workerCnt++], REDO_TRXN_WORKER, 0);

    RedoRoleInit(&(g_dispatcher->readLine.managerThd), tmpWorkers[workerCnt++], REDO_READ_MNG, 0);
    RedoRoleInit(&(g_dispatcher->readLine.readThd), tmpWorkers[workerCnt++], REDO_READ_WORKER, 0);

    for (started = 0; started < totalThrdNum; started++) {
        if (StartPageRedoWorker(g_dispatcher->allWorkers[started]) == NULL) {
            ereport(PANIC,
                (errmodule(MOD_REDO),
                    errcode(ERRCODE_LOG),
                    errmsg("[REDO_LOG_TRACE]StartPageRedoWorkers StartPageRedoWorker failed, started:%u", started)));
        }
    }

    Assert(totalThrdNum == workerCnt);
    g_dispatcher->pageLineNum = batchNum;
    g_instance.comm_cxt.predo_cxt.totalNum = workerCnt;
    g_dispatcher->chosedPageLineIds = (uint32*)palloc(sizeof(uint32) * batchNum);
    g_dispatcher->chosedPLCnt = 0;
}

static void ResetChosedPageLineList()
{
    g_dispatcher->chosedPLCnt = 0;

    for (uint32 i = 0; i < g_dispatcher->pageLineNum; ++i) {
        g_dispatcher->chosedPageLineIds[i] = 0;
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
                ereport(WARNING,
                    (errmsg("Dispatch kill(pid %lu, signal %d) failed: \"%s\",",
                        g_instance.comm_cxt.predo_cxt.pageRedoThreadStatusList[i].threadId,
                        signal,
                        gs_strerror(err))));
            }
        }
    }
}

/* Run from the dispatcher thread. */
static void StopRecoveryWorkers(int code, Datum arg)
{
    ereport(LOG,
        (errmodule(MOD_REDO),
            errcode(ERRCODE_LOG),
            errmsg("parallel redo workers are going to stop, "
                   "code:%d, arg:%lu",
                code,
                DatumGetUInt64(arg))));
    SendSingalToPageWorker(SIGTERM);

    uint64 count = 0;
    while (!DispathCouldExit()) {
        ++count;
        if ((count & OUTPUT_WAIT_COUNT) == OUTPUT_WAIT_COUNT) {
            ereport(WARNING,
                (errmodule(MOD_REDO), errcode(ERRCODE_LOG), errmsg("StopRecoveryWorkers wait page work exit")));
            if ((count & PRINT_ALL_WAIT_COUNT) == PRINT_ALL_WAIT_COUNT) {
                DumpDispatcher();
                ereport(
                    PANIC, (errmodule(MOD_REDO), errcode(ERRCODE_LOG), errmsg("StopRecoveryWorkers wait too long!!!")));
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
    if (g_dispatcher != NULL) {
        for (uint32 i = 0; i < g_dispatcher->pageLineNum; i++) {
            DestroyPageRedoWorker(g_dispatcher->pageLines[i].batchThd);
            DestroyPageRedoWorker(g_dispatcher->pageLines[i].managerThd);
            for (uint32 j = 0; j < g_dispatcher->pageLines[i].redoThdNum; j++) {
                DestroyPageRedoWorker(g_dispatcher->pageLines[i].redoThd[j]);
            }
            if (g_dispatcher->pageLines[i].chosedRTIds != NULL) {
                pfree(g_dispatcher->pageLines[i].chosedRTIds);
            }
        }
        DestroyPageRedoWorker(g_dispatcher->trxnLine.managerThd);
        DestroyPageRedoWorker(g_dispatcher->trxnLine.redoThd);

        DestroyPageRedoWorker(g_dispatcher->readLine.managerThd);
        DestroyPageRedoWorker(g_dispatcher->readLine.readThd);
        pfree(g_dispatcher->recordstate.readsegbuf);
        pfree(g_dispatcher->recordstate.errormsg_buf);
        pfree(g_dispatcher->recordstate.readprivate);

        if (get_real_recovery_parallelism() > 1) {
            (void)MemoryContextSwitchTo(g_dispatcher->oldCtx);
            MemoryContextDelete(g_instance.comm_cxt.predo_cxt.parallelRedoCtx);
            g_instance.comm_cxt.predo_cxt.parallelRedoCtx = NULL;
        }
        g_dispatcher = NULL;
    }
}

static bool RmgrRecordInfoValid(XLogReaderState* record, uint8 minInfo, uint8 maxInfo)
{
    uint8 info = (XLogRecGetInfo(record) & (~XLR_INFO_MASK));

    if ((XLogRecGetRmid(record) == RM_HEAP2_ID) || (XLogRecGetRmid(record) == RM_HEAP_ID)) {
        info = (info & XLOG_HEAP_OPMASK);
    }

    info = (info >> XLOG_INFO_SHIFT_SIZE);
    minInfo = (minInfo >> XLOG_INFO_SHIFT_SIZE);
    maxInfo = (maxInfo >> XLOG_INFO_SHIFT_SIZE);

    if ((info >= minInfo) && (info <= maxInfo)) {
        return true;
    }
    return false;
}

static bool RmgrGistRecordInfoValid(XLogReaderState* record, uint8 minInfo, uint8 maxInfo)
{
    uint8 info = (XLogRecGetInfo(record) & (~XLR_INFO_MASK));

    if ((info == XLOG_GIST_PAGE_UPDATE) || (info == XLOG_GIST_PAGE_SPLIT) || (info == XLOG_GIST_CREATE_INDEX)) {
        return true;
    }

    return false;
}

/* Run from the dispatcher thread. */
void DispatchRedoRecordToFile(XLogReaderState* record, List* expectedTLIs, TimestampTz recordXTime)
{
    bool fatalerror = false;
    uint32 indexid;
    uint32 rmid = XLogRecGetRmid(record);
    uint32 term = XLogRecGetTerm(record);
    if (term > g_instance.comm_cxt.localinfo_cxt.term) {
        g_instance.comm_cxt.localinfo_cxt.term = term;
    }
    t_thrd.xlog_cxt.redoItemIdx = 0;
    if ((get_real_recovery_parallelism() > 1) && (GetBatchCount() > 0)) {
        if (rmid <= RM_MAX_ID) {
            indexid = g_dispatchTable[rmid].rm_id;
            if ((indexid != rmid) ||
                ((g_dispatchTable[rmid].rm_loginfovalid != NULL) &&
                    (g_dispatchTable[rmid].rm_loginfovalid(
                         record, g_dispatchTable[rmid].rm_mininfo, g_dispatchTable[rmid].rm_maxinfo) == false))) {
                /* it's invalid info */
                fatalerror = true;
            }
        } else {
            fatalerror = true;
        }

        ResetChosedPageLineList();
        pg_atomic_write_u64(&(g_instance.comm_cxt.predo_cxt.endRecPtr), record->EndRecPtr);
        /* RTO_DEMO */
        if (fatalerror != true) {
            g_dispatchTable[rmid].rm_dispatch(record, expectedTLIs, recordXTime);
        } else {
            DispatchDefaultRecord(record, expectedTLIs, recordXTime);
        }
    } else {
        ereport(PANIC,
            (errmodule(MOD_REDO),
                errcode(ERRCODE_LOG),
                errmsg("[REDO_LOG_TRACE]DispatchRedoRecord could not be here config recovery num %d, work num %u",
                    get_real_recovery_parallelism(),
                    GetBatchCount())));
    }
}

/**
 * process record need sync with page worker and trxn thread
 * trxnthreadexe is true when the record need execute on trxn thread
 * pagethredexe is true when the record need execute on pageworker thread
 */
static void DispatchSyncTxnRecord(
    XLogReaderState* record, List* expectedTLIs, TimestampTz recordXTime, uint32 designatedWorker)
{
    RedoItem* item = GetRedoItemPtr(record);
    ReferenceRedoItem(item);
    item->sharewithtrxn = true;
    item->blockbytrxn = false;

    if ((g_dispatcher->chosedPLCnt != 1) && (XLogRecGetRmid(&item->record) != RM_XACT_ID)) {
        ereport(WARNING,
            (errmodule(MOD_REDO),
                errcode(ERRCODE_LOG),
                errmsg("[REDO_LOG_TRACE]DispatchSyncTxnRecord maybe some error:rmgrID:%u, info:%u, workerCount:%u",
                    XLogRecGetRmid(&item->record),
                    XLogRecGetInfo(&item->record),
                    g_dispatcher->chosedPLCnt)));
    }

    for (uint32 i = 0; i < g_dispatcher->pageLineNum; ++i) {
        if (g_dispatcher->chosedPageLineIds[i] > 0) {
            ReferenceRedoItem(item);
            AddPageRedoItem(g_dispatcher->pageLines[i].batchThd, item);
        }
    }

    /* ensure eyery pageworker is receive recored to update pageworker Lsn
     * trxn record's recordtime must set , see SetLatestXTime
     */
    AddTxnRedoItem(g_dispatcher->trxnLine.managerThd, item);
    return;
}

static void DispatchToOnePageWorker(XLogReaderState* record, const RelFileNode rnode, List* expectedTLIs)
{
    /* for bcm different attr need to dispath to the same page redo thread */
    uint32 slotId = GetSlotId(rnode, 0, 0, GetBatchCount());
    RedoItem* item = GetRedoItemPtr(record);
    ReferenceRedoItem(item);
    AddPageRedoItem(g_dispatcher->pageLines[slotId].batchThd, item);
}

/**
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
static void DispatchTxnRecord(
    XLogReaderState* record, List* expectedTLIs, TimestampTz recordXTime, bool imcheckpoint, bool isForceAll = false)
{
    RedoItem* trxnItem = GetRedoItemPtr(record);
    ReferenceRedoItem(trxnItem);
    trxnItem->imcheckpoint = imcheckpoint; /* immdiate checkpoint set imcheckpoint  */
    AddTxnRedoItem(g_dispatcher->trxnLine.managerThd, trxnItem);
}

#ifdef ENABLE_MULTIPLE_NODES
/* Run  from the dispatcher thread. */
static bool DispatchBarrierRecord(XLogReaderState* record, List* expectedTLIs, TimestampTz recordXTime)
{
    DispatchTxnRecord(record, expectedTLIs, recordXTime, false);
    return false;
}
#endif

static bool DispatchMotRecord(XLogReaderState* record, List* expectedTLIs, TimestampTz recordXTime)
{
    DispatchTxnRecord(record, expectedTLIs, recordXTime, false);
    return false;
}

/* Run  from the dispatcher thread. */
static bool DispatchRepSlotRecord(XLogReaderState* record, List* expectedTLIs, TimestampTz recordXTime)
{
    DispatchTxnRecord(record, expectedTLIs, recordXTime, false);
    return false;
}

/* Run  from the dispatcher thread. */
static bool DispatchHeap3Record(XLogReaderState* record, List* expectedTLIs, TimestampTz recordXTime)
{
    DispatchTxnRecord(record, expectedTLIs, recordXTime, false);
    return false;
}

/* record of rmid or info error, we inter this function to make every worker run to this position */
static bool DispatchDefaultRecord(XLogReaderState* record, List* expectedTLIs, TimestampTz recordXTime)
{
    DispatchTxnRecord(record, expectedTLIs, recordXTime, false, true);
    return true;
}

/* Run from the dispatcher thread. */
static bool DispatchXLogRecord(XLogReaderState* record, List* expectedTLIs, TimestampTz recordXTime)
{
    bool isNeedFullSync = false;
    uint8 info = (XLogRecGetInfo(record) & (~XLR_INFO_MASK));

    if (IsCheckPoint(record)) {
        isNeedFullSync = XLogWillChangeStandbyState(record);
        RedoItem* item = GetRedoItemPtr(record);
        ReferenceRedoItem(item);
        for (uint32 i = 0; i < g_dispatcher->pageLineNum; ++i) {
            /*
             * A check point record may save a recovery restart point or
             * update the timeline.
             */
            ReferenceRedoItem(item);
            AddPageRedoItem(g_dispatcher->pageLines[i].batchThd, item);
        }
        /* ensure eyery pageworker is receive recored to update pageworker Lsn
         * trxn record's recordtime must set , see SetLatestXTime
         */
        AddTxnRedoItem(g_dispatcher->trxnLine.managerThd, item);

    } else if ((info == XLOG_FPI) || (info == XLOG_FPI_FOR_HINT)) {
        if (SUPPORT_FPAGE_DISPATCH) {
            DispatchRecordWithPages(record, expectedTLIs, true);

        } else {
            DispatchRecordWithoutPage(record, expectedTLIs); /* fullpagewrite include btree, so need strong sync */
        }
    } else {
        /* process in trxn thread and need to sync to other pagerredo thread */
        DispatchTxnRecord(record, expectedTLIs, recordXTime, false);
    }

    return isNeedFullSync;
}

/* Run  from the dispatcher thread. */
static bool DispatchRelMapRecord(XLogReaderState* record, List* expectedTLIs, TimestampTz recordXTime)
{
    /* page redo worker directly use relnode, will not use relmapfile */
    DispatchTxnRecord(record, expectedTLIs, recordXTime, false);
    return false;
}

/* Run  from the dispatcher thread. */
static bool DispatchXactRecord(XLogReaderState* record, List* expectedTLIs, TimestampTz recordXTime)
{
    if (XactWillRemoveRelFiles(record)) {
        /* for parallel performance */
        if (SUPPORT_FPAGE_DISPATCH) {
            int nrels = 0;
            ColFileNodeRel* xnodes = NULL;
            XactGetRelFiles(record, &xnodes, &nrels);
            for (int i = 0; ((i < nrels) && (xnodes != NULL)); ++i) {
                ColFileNode node;
                ColFileNodeRel* nodeRel = xnodes + i;
                ColFileNodeCopy(&node, nodeRel);
                uint32 id = GetSlotId(node.filenode, 0, 0, GetBatchCount());
                AddSlotToPLSet(id);
            }
        } else {
            for (uint32 i = 0; i < g_dispatcher->pageLineNum; i++) {
                AddSlotToPLSet(i);
            }
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
static bool DispatchStandbyRecord(XLogReaderState* record, List* expectedTLIs, TimestampTz recordXTime)
{
    /* change standbystate, must be full sync, see UpdateStandbyState */
    bool isNeedFullSync = StandbyWillChangeStandbyState(record);

    DispatchTxnRecord(record, expectedTLIs, recordXTime, false, isNeedFullSync);

    return isNeedFullSync;
}

/* Run from the dispatcher thread. */
static bool DispatchMultiXactRecord(XLogReaderState* record, List* expectedTLIs, TimestampTz recordXTime)
{
    /* page worker will not use multixact */
    DispatchTxnRecord(record, expectedTLIs, recordXTime, false);

    return false;
}

/* Run from the dispatcher thread. */
static void DispatchRecordWithoutPage(XLogReaderState* record, List* expectedTLIs)
{
    RedoItem* item = GetRedoItemPtr(record);
    ReferenceRedoItem(item);
    for (uint32 i = 0; i < g_dispatcher->pageLineNum; i++) {
        ReferenceRedoItem(item);
        AddPageRedoItem(g_dispatcher->pageLines[i].batchThd, item);
    }
    DereferenceRedoItem(item);
}

/* Run from the dispatcher thread. */
static void DispatchRecordWithPages(XLogReaderState* record, List* expectedTLIs, bool rnodedispatch)
{
    GetSlotIds(record, ANY_WORKER, rnodedispatch);

    RedoItem* item = GetRedoItemPtr(record);
    ReferenceRedoItem(item);
    for (uint32 i = 0; i < g_dispatcher->pageLineNum; i++) {
        if (g_dispatcher->chosedPageLineIds[i] > 0) {
            ReferenceRedoItem(item);
            AddPageRedoItem(g_dispatcher->pageLines[i].batchThd, item);
        }
    }
    DereferenceRedoItem(item);
}

static bool DispatchHeapRecord(XLogReaderState* record, List* expectedTLIs, TimestampTz recordXTime)
{
    if (record->max_block_id >= 0)
        DispatchRecordWithPages(record, expectedTLIs, SUPPORT_FPAGE_DISPATCH);
    else
        DispatchRecordWithoutPage(record, expectedTLIs);

    return false;
}

static bool DispatchSeqRecord(XLogReaderState* record, List* expectedTLIs, TimestampTz recordXTime)
{
    DispatchRecordWithPages(record, expectedTLIs, SUPPORT_FPAGE_DISPATCH);

    return false;
}

static bool DispatchDataBaseRecord(XLogReaderState* record, List* expectedTLIs, TimestampTz recordXTime)
{
    bool isNeedFullSync = false;

    if (IsDataBaseDrop(record)) {
        isNeedFullSync = true;
        RedoItem* item = GetRedoItemPtr(record);
        ReferenceRedoItem(item);
        item->imcheckpoint = true;
        for (uint32 i = 0; i < g_dispatcher->pageLineNum; i++) {
            ReferenceRedoItem(item);
            AddPageRedoItem(g_dispatcher->pageLines[i].batchThd, item);
        }
        DereferenceRedoItem(item);
    } else {
        /* database dir may impact many rel so need to sync to all pageworks */
        DispatchRecordWithoutPage(record, expectedTLIs);
    }

    return isNeedFullSync;
}

static bool DispatchTableSpaceRecord(XLogReaderState* record, List* expectedTLIs, TimestampTz recordXTime)
{
    bool isNeedFullSync = false;
    uint8 info = (XLogRecGetInfo(record) & (~XLR_INFO_MASK));

    if (info == XLOG_TBLSPC_DROP) {
        isNeedFullSync = true;
        DispatchTxnRecord(record, expectedTLIs, recordXTime, true, isNeedFullSync);
    } else {
        /* tablespace dir may impact many rel so need to sync to all pageworks */
        DispatchRecordWithoutPage(record, expectedTLIs);
    }

    return isNeedFullSync;
}

static bool DispatchSmgrRecord(XLogReaderState* record, List* expectedTLIs, TimestampTz recordXTime)
{
    bool isNeedFullSync = false;
    uint8 info = (XLogRecGetInfo(record) & (~XLR_INFO_MASK));

    if (info == XLOG_SMGR_CREATE) {
        /* only need to dispatch to one page worker */
        /* for parallel performance */
        if (SUPPORT_FPAGE_DISPATCH) {
            xl_smgr_create* xlrec = (xl_smgr_create*)XLogRecGetData(record);
            RelFileNode rnode;
            RelFileNodeCopy(rnode, xlrec->rnode, XLogRecGetBucketId(record));
            DispatchToOnePageWorker(record, rnode, expectedTLIs);
        } else {
            DispatchRecordWithoutPage(record, expectedTLIs);
        }
    } else if (IsSmgrTruncate(record)) {
        if (SUPPORT_FPAGE_DISPATCH) {
            uint32 id;
            xl_smgr_truncate* xlrec = (xl_smgr_truncate*)XLogRecGetData(record);
            RelFileNode rnode;
            RelFileNodeCopy(rnode, xlrec->rnode, XLogRecGetBucketId(record));
            id = GetSlotId(rnode, 0, 0, GetBatchCount());
            AddSlotToPLSet(id);
        } else {
            for (uint32 i = 0; i < g_dispatcher->pageLineNum; i++) {
                AddSlotToPLSet(i);
            }
        }

        DispatchToSpecPageWorker(record, expectedTLIs, false);
    }

    return isNeedFullSync;
}

/* Run from the dispatcher thread. */
static bool DispatchCLogRecord(XLogReaderState* record, List* expectedTLIs, TimestampTz recordXTime)
{
    DispatchTxnRecord(record, expectedTLIs, recordXTime, false);
    return false;
}

/* Run from the dispatcher thread. */
static bool DispatchHashRecord(XLogReaderState* record, List* expectedTLIs, TimestampTz recordXTime)
{
    DispatchTxnRecord(record, expectedTLIs, recordXTime, false, true);
    return true;
}

/* Run from the dispatcher thread. */
static bool DispatchBtreeHotStandby(XLogReaderState* record, List* expectedTLIs, TimestampTz recordXTime)
{
    bool isNeedFullSync = false;

    /* index not support mvcc, so we need to sync with trx thread when the record is vacuum */
    if (IsBtreeVacuum(record)) {
        uint32 id;
        uint8 info = (XLogRecGetInfo(record) & (~XLR_INFO_MASK));

        if (info == XLOG_BTREE_REUSE_PAGE) {
            if (!(InHotStandby)) {
                /* if not in hotstandby don't need to process */
                return isNeedFullSync;
            }

            xl_btree_reuse_page* xlrec = (xl_btree_reuse_page*)XLogRecGetData(record);
            RelFileNode tmp_node;
            RelFileNodeCopy(tmp_node, xlrec->node, XLogRecGetBucketId(record));
            id = GetSlotId(tmp_node, 0, 0, GetBatchCount());
            AddSlotToPLSet(id);
        } else if (info == XLOG_BTREE_VACUUM) {
            GetSlotIds(record, ANY_WORKER, true);

            if (HotStandbyActiveInReplay() && IS_SINGLE_NODE) {
                RelFileNode thisrnode;
                BlockNumber thisblkno;

                bool getTagSuccess = XLogRecGetBlockTag(record, 0, &thisrnode, NULL, &thisblkno);
                if (getTagSuccess) {
                    xl_btree_vacuum* xlrec = (xl_btree_vacuum*)XLogRecGetData(record);
                    /* for performance reserve */
                    for (BlockNumber blkno = xlrec->lastBlockVacuumed + 1; blkno < thisblkno; blkno++) {
                        id = GetSlotId(thisrnode, 0, 0, GetBatchCount());
                        AddSlotToPLSet(id);
                    }
                }
            }
        } else {
            GetSlotIds(record, ANY_WORKER, true);
        }

        /* sync with trxn thread */
        if (info == XLOG_BTREE_REUSE_PAGE) {
            /* only need to process in trx  thread, pageworker only update lsn */
            DispatchSyncTxnRecord(record, expectedTLIs, recordXTime, TRXN_WORKER);
        } else {
            /* only need to process in pageworker  thread, wait trxn sync */
            /* pageworker exe, trxn don't need exe */
            DispatchToSpecPageWorker(record, expectedTLIs, true);
        }
    } else {
        DispatchRecordWithPages(record, expectedTLIs, true);
    }

    return isNeedFullSync;
}

static bool DispatchBtreeRecord(XLogReaderState* record, List* expectedTLIs, TimestampTz recordXTime)
{
    if (SUPPORT_HOT_STANDBY) {
        DispatchBtreeHotStandby(record, expectedTLIs, recordXTime);
    } else {
        uint8 info = (XLogRecGetInfo(record) & (~XLR_INFO_MASK));
        if (info == XLOG_BTREE_REUSE_PAGE) {
            DispatchTxnRecord(record, expectedTLIs, recordXTime, false);
        } else {
            DispatchRecordWithPages(record, expectedTLIs, true);
        }
    }

    return false;
}

/* Run from the dispatcher thread. */
static bool DispatchGinRecord(XLogReaderState* record, List* expectedTLIs, TimestampTz recordXTime)
{
    bool isNeedFullSync = false;
    uint8 info = (XLogRecGetInfo(record) & (~XLR_INFO_MASK));

    if (info == XLOG_GIN_DELETE_LISTPAGE) {
        ginxlogDeleteListPages* data = (ginxlogDeleteListPages*)XLogRecGetData(record);
        /* output warning */
        if (data->ndeleted != record->max_block_id) {
            ereport(WARNING,
                (errmodule(MOD_REDO),
                    errcode(ERRCODE_LOG),
                    errmsg("[REDO_LOG_TRACE]DispatchGinRecord warnninginfo:ndeleted:%d, max_block_id:%d",
                        data->ndeleted,
                        record->max_block_id)));
        }
    }

    /* index not support mvcc, so we need to sync with trx thread when the record is vacuum */
    if (IsGinVacuumPages(record) && SUPPORT_HOT_STANDBY) {
        GetSlotIds(record, ANY_WORKER, true);
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
static bool DispatchGistRecord(XLogReaderState* record, List* expectedTLIs, TimestampTz recordXTime)
{
    uint8 info = (XLogRecGetInfo(record) & (~XLR_INFO_MASK));
    bool isNeedFullSync = false;

    if (info == XLOG_GIST_PAGE_SPLIT) {
        gistxlogPageSplit* xldata = (gistxlogPageSplit*)XLogRecGetData(record);
        /* output warning */
        if (xldata->npage != record->max_block_id) {
            ereport(WARNING,
                (errmodule(MOD_REDO),
                    errcode(ERRCODE_LOG),
                    errmsg("[REDO_LOG_TRACE]DispatchGistRecord warnninginfo:npage:%u, max_block_id:%d",
                        xldata->npage,
                        record->max_block_id)));
        }
    }

    /* index not support mvcc, so we need to sync with trx thread when the record is vacuum */
    if (IsGistPageUpdate(record) && SUPPORT_HOT_STANDBY) {
        GetSlotIds(record, ANY_WORKER, true);
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
static bool DispatchSpgistRecord(XLogReaderState* record, List* expectedTLIs, TimestampTz recordXTime)
{
    /* index not support mvcc, so we need to sync with trx thread when the record is vacuum */
    if (IsSpgistVacuum(record) && SUPPORT_HOT_STANDBY) {
        uint8 info = (XLogRecGetInfo(record) & (~XLR_INFO_MASK));

        GetSlotIds(record, ANY_WORKER, true);
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

/**
 *  dispatch record to a specified thread
 */
static void DispatchToSpecPageWorker(XLogReaderState* record, List* expectedTLIs, bool waittrxnsync)
{
    RedoItem* item = GetRedoItemPtr(record);
    ReferenceRedoItem(item);
    item->sharewithtrxn = false;
    item->blockbytrxn = waittrxnsync;

    if (g_dispatcher->chosedPLCnt != 1) {
        ereport(WARNING,
            (errmodule(MOD_REDO),
                errcode(ERRCODE_LOG),
                errmsg("[REDO_LOG_TRACE]DispatchToSpecPageWorker maybe some error:rmgrID:%u, info:%u, workerCount:%u",
                    XLogRecGetRmid(&item->record),
                    XLogRecGetInfo(&item->record),
                    g_dispatcher->chosedPLCnt)));
    }

    for (uint32 i = 0; i < g_dispatcher->pageLineNum; i++) {
        if (g_dispatcher->chosedPageLineIds[i] > 0) {
            ReferenceRedoItem(item);
            AddPageRedoItem(g_dispatcher->pageLines[i].batchThd, item);
        }
    }

    DereferenceRedoItem(item);
}

static bool DispatchHeap2VacuumRecord(XLogReaderState* record, List* expectedTLIs, TimestampTz recordXTime)
{
    /*
     * don't support consistency view
     */
    bool isNeedFullSync = false;
    uint8 info = ((XLogRecGetInfo(record) & (~XLR_INFO_MASK)) & XLOG_HEAP_OPMASK);

    if (info == XLOG_HEAP2_CLEANUP_INFO) {
        DispatchTxnRecord(record, expectedTLIs, recordXTime, false);
    } else {
        DispatchRecordWithPages(record, expectedTLIs, SUPPORT_FPAGE_DISPATCH);
    }

    return isNeedFullSync;
}

static bool DispatchHeap2VacuumHotStandby(XLogReaderState* record, List* expectedTLIs, TimestampTz recordXTime)
{
    /*
     * base on mvcc , except vacumm action the record is always exist
     * so many redo action can only execute in trx redo thread
     * vacumm action must execute in full sync
     */
    bool isNeedFullSync = false;
    bool isSyncWithTrxn = false;
    uint8 info = ((XLogRecGetInfo(record) & (~XLR_INFO_MASK)) & XLOG_HEAP_OPMASK);

    if (info == XLOG_HEAP2_CLEANUP_INFO) {
        if (InHotStandby) {
            /* for parallel redo performance */
            if (SUPPORT_FPAGE_DISPATCH) {
                uint32 id;
                xl_heap_cleanup_info* xlrec = (xl_heap_cleanup_info*)XLogRecGetData(record);
                RelFileNode tmp_node;
                RelFileNodeCopy(tmp_node, xlrec->node, XLogRecGetBucketId(record));
                id = GetSlotId(tmp_node, 0, 0, GetBatchCount());
                AddSlotToPLSet(id);
            } else {
                for (uint32 i = 0; i < g_dispatcher->pageLineNum; i++)
                    AddSlotToPLSet(i);
            }
            isSyncWithTrxn = true;
        } else {
            return false;
        }
    } else {
        GetSlotIds(record, ANY_WORKER, SUPPORT_FPAGE_DISPATCH);

        if (info == XLOG_HEAP2_CLEAN) {
            xl_heap_clean* xlrec = (xl_heap_clean*)XLogRecGetData(record);
            if (InHotStandby && TransactionIdIsValid(xlrec->latestRemovedXid))
                isSyncWithTrxn = true;
        } else {
            isSyncWithTrxn = InHotStandby;
        }
    }

    if (isSyncWithTrxn) {
        /* sync with trxn thread */
        /* trxn thread first reslove confilict snapshot ,then do the page action */
        /* first pageworker update lsn, then trxn thread exe */
        DispatchSyncTxnRecord(record, expectedTLIs, recordXTime, TRXN_WORKER);
    } else {
        /* pageworker exe, trxn don't need exe */
        DispatchToSpecPageWorker(record, expectedTLIs, false);
    }

    return isNeedFullSync;
}

/* Run from the dispatcher thread. */
static bool DispatchHeap2Record(XLogReaderState* record, List* expectedTLIs, TimestampTz recordXTime)
{
    bool isNeedFullSync = false;

    uint8 info = ((XLogRecGetInfo(record) & (~XLR_INFO_MASK)) & XLOG_HEAP_OPMASK);

    if ((info == XLOG_HEAP2_MULTI_INSERT) || (info == XLOG_HEAP2_PAGE_UPGRADE)) {
        DispatchRecordWithPages(record, expectedTLIs, SUPPORT_FPAGE_DISPATCH);
    } else if (info == XLOG_HEAP2_BCM) {
        /* we use renode as dispatch key, so the same relation will dispath to the same page redo thread
         * although they have different fork num
         */
        /* for parallel redo performance */
        if (SUPPORT_FPAGE_DISPATCH) {
            xl_heap_bcm* xlrec = (xl_heap_bcm*)XLogRecGetData(record);
            RelFileNode tmp_node;
            RelFileNodeCopy(tmp_node, xlrec->node, XLogRecGetBucketId(record));
            DispatchToOnePageWorker(record, tmp_node, expectedTLIs);
        } else {
            DispatchRecordWithoutPage(record, expectedTLIs);
        }
    } else if (info == XLOG_HEAP2_LOGICAL_NEWPAGE) {
        if (IS_DN_MULTI_STANDYS_MODE()) {
            xl_heap_logical_newpage* xlrec = (xl_heap_logical_newpage*)XLogRecGetData(record);

            if (xlrec->type == COLUMN_STORE && xlrec->hasdata) {
                /* for parallel redo performance */
                if (SUPPORT_FPAGE_DISPATCH) {
                    RelFileNode tmp_node;
                    RelFileNodeCopy(tmp_node, xlrec->node, XLogRecGetBucketId(record));
                    DispatchToOnePageWorker(record, tmp_node, expectedTLIs);
                } else
                    DispatchRecordWithoutPage(record, expectedTLIs);
            }
        } else {
            if (!g_instance.attr.attr_storage.enable_mix_replication) {
                isNeedFullSync = true;
                DispatchTxnRecord(record, expectedTLIs, recordXTime, false, isNeedFullSync);
            }
        }
    } else {
        if (SUPPORT_HOT_STANDBY)
            isNeedFullSync = DispatchHeap2VacuumHotStandby(record, expectedTLIs, recordXTime);
        else
            isNeedFullSync = DispatchHeap2VacuumRecord(record, expectedTLIs, recordXTime);
    }

    return isNeedFullSync;
}

/* Run from the dispatcher thread. */
static void GetSlotIds(XLogReaderState* record, uint32 designatedWorker, bool rnodedispatch)
{
    uint32 id;
    for (int i = 0; i <= record->max_block_id; i++) {
        DecodedBkpBlock* block = &record->blocks[i];

        if (block->in_use != true) {
            /* blk number is not continue */
            continue;
        }
        if (rnodedispatch)
            id = GetSlotId(block->rnode, 0, 0, GetBatchCount());
        else
            id = GetSlotId(block->rnode, block->blkno, 0, GetBatchCount());

        AddSlotToPLSet(id);
    }

    if ((designatedWorker != ANY_WORKER)) {
        if (designatedWorker < GetBatchCount()) {
            AddSlotToPLSet(designatedWorker);
        } else {
            /* output  error info */
        }
    }
}

/**
 * count slot id  by hash
 */
uint32 GetSlotId(const RelFileNode node, BlockNumber block, ForkNumber forkNum, uint32 workerCount)
{
    if (workerCount == 0)
        return ANY_WORKER;

    return (node.relNode & 0xF) % workerCount;
}

static void AddSlotToPLSet(uint32 id)
{
    if (id >= g_dispatcher->pageLineNum) {
        ereport(PANIC,
            (errmodule(MOD_REDO),
                errcode(ERRCODE_LOG),
                errmsg("[REDO_LOG_TRACE]AddWorkerToSet:input work id error, id:%u, batch work num %u",
                    id,
                    g_dispatcher->pageLineNum)));
        return;
    }

    if (g_dispatcher->chosedPageLineIds[id] == 0) {
        g_dispatcher->chosedPLCnt += 1;
    }
    ++(g_dispatcher->chosedPageLineIds[id]);
}

/* Run from the dispatcher and each page worker thread. */
bool XactWillRemoveRelFiles(XLogReaderState* record)
{
    /*
     * Relation files under tablespace folders are removed only from
     * applying transaction log record.
     */
    int nrels = 0;
    ColFileNodeRel* xnodes = NULL;

    if (XLogRecGetRmid(record) != RM_XACT_ID) {
        return false;
    }

    XactGetRelFiles(record, &xnodes, &nrels);

    return (nrels > 0);
}

/* Run from the dispatcher thread. */
static bool XLogWillChangeStandbyState(const XLogReaderState* record)
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
static bool StandbyWillChangeStandbyState(const XLogReaderState* record)
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

    return false;
}

/* Run from each page worker thread. */
void FreeRedoItem(RedoItem* item)
{
    RedoItem* oldHead = (RedoItem*)pg_atomic_read_uintptr((uintptr_t*)&g_dispatcher->freeHead);

    do {
        item->freeNext = oldHead;
    } while (!pg_atomic_compare_exchange_uintptr(
        (uintptr_t*)&g_dispatcher->freeHead, (uintptr_t*)&oldHead, (uintptr_t)item));
}

void InitReaderStateByOld(XLogReaderState* newState, XLogReaderState* oldState, bool isNew)
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
    for (int i = 0; i <= XLR_MAX_BLOCK_ID; i++) {
        newState->blocks[i].data = NULL;
        newState->blocks[i].data_len = 0;
        newState->blocks[i].data_bufsz = 0;
    }
    newState->refcount = 0;
    newState->isDecode = false;
}

static XLogReaderState* GetXlogReader(XLogReaderState* readerState)
{
    XLogReaderState* retReaderState = NULL;
    bool isNew = false;
    uint64 count = 0;
    do {
        if (g_dispatcher->freeStateHead != NULL) {
            retReaderState = &g_dispatcher->freeStateHead->record;
            g_dispatcher->freeStateHead = g_dispatcher->freeStateHead->freeNext;
        } else {
            RedoItem* head =
                (RedoItem*)pg_atomic_exchange_uintptr((uintptr_t*)&g_dispatcher->freeHead, (uintptr_t)NULL);
            if (head != NULL) {
                retReaderState = &head->record;
                g_dispatcher->freeStateHead = head->freeNext;
            } else if (g_dispatcher->maxItemNum > g_dispatcher->curItemNum) {
                RedoItem* item =
                    (RedoItem*)palloc_extended(MAXALIGN(sizeof(RedoItem)) + sizeof(RedoItem*) * GetAllWorkerCount() +
                                                   sizeof(bool) * GetAllWorkerCount(),
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
                ereport(WARNING,
                    (errmodule(MOD_REDO),
                        errcode(ERRCODE_LOG),
                        errmsg("GetXlogReader Allocated record buffer failed!, cur item:%u, max item:%u",
                            g_dispatcher->curItemNum,
                            g_dispatcher->maxItemNum)));
                if ((count & PRINT_ALL_WAIT_COUNT) == PRINT_ALL_WAIT_COUNT) {
                    DumpDispatcher();
                }
            }
            if (retReaderState == NULL) {
                HandleStartupProcInterrupts();
            }
        }
    } while (retReaderState == NULL);

    InitReaderStateByOld(retReaderState, readerState, isNew);

    return retReaderState;
}

void CopyDataFromOldReader(XLogReaderState* newReaderState, const XLogReaderState* oldReaderState)
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
    newReaderState->decoded_record = (XLogRecord*)newReaderState->readRecordBuf;

    for (int i = 0; i <= newReaderState->max_block_id; i++) {
        if (newReaderState->blocks[i].has_image)
            newReaderState->blocks[i].bkp_image =
                (char*)((uintptr_t)newReaderState->decoded_record +
                        ((uintptr_t)oldReaderState->blocks[i].bkp_image - (uintptr_t)oldReaderState->decoded_record));
        if (newReaderState->blocks[i].has_data) {
            newReaderState->blocks[i].data = oldReaderState->blocks[i].data;
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

XLogReaderState* NewReaderState(XLogReaderState* readerState, bool bCopyState)
{
    Assert(readerState != NULL);
    if (!readerState->isPRProcess)
        return readerState;
    if (DispatchPtrIsNull())
        ereport(PANIC, (errmodule(MOD_REDO), errcode(ERRCODE_LOG), errmsg("NewReaderState Dispatch is null")));

    XLogReaderState* retReaderState = GetXlogReader(readerState);
    if (bCopyState) {
        CopyDataFromOldReader(retReaderState, readerState);
    }
    return retReaderState;
}

void FreeAllocatedRedoItem()
{
    while ((g_dispatcher != NULL) && (g_dispatcher->allocatedRedoItem != NULL)) {
        RedoItem* pItem = g_dispatcher->allocatedRedoItem;
        g_dispatcher->allocatedRedoItem = pItem->allocatedNext;
        XLogReaderState* tmpRec = &(pItem->record);

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
    ereport(LOG,
        (errmodule(MOD_REDO),
            errcode(ERRCODE_LOG),
            errmsg("[REDO_LOG_TRACE]SendRecoveryEndMarkToWorkersAndWaitForFinish, ready to stop redo workers, code: %d",
                code)));
    if ((get_real_recovery_parallelism() > 1) && (GetBatchCount() > 0)) {
        uint32 i;
        PageRedoPipeline* pl = g_dispatcher->pageLines;
        /* send end mark */
        for (i = 0; i < g_dispatcher->pageLineNum; i++) {
            SendPageRedoEndMark(pl[i].batchThd);
        }
        SendPageRedoEndMark(g_dispatcher->trxnLine.managerThd);

        /* wait */
        for (i = 0; i < g_dispatcher->pageLineNum; i++) {
            WaitPageRedoWorkerReachLastMark(pl[i].batchThd);
        }

        WaitPageRedoWorkerReachLastMark(g_dispatcher->readLine.managerThd);
        WaitPageRedoWorkerReachLastMark(g_dispatcher->readLine.readThd);
        WaitPageRedoWorkerReachLastMark(g_dispatcher->trxnLine.managerThd);
    }
}

/* Run from each page worker and the txn worker thread. */
int GetDispatcherExitCode()
{
    return (int)pg_atomic_read_u32((uint32*)&g_dispatcher->exitCode);
}

/* Run from the dispatcher thread. */
uint32 GetAllWorkerCount()
{
    return g_dispatcher == NULL ? 0 : g_dispatcher->allWorkersCnt;
}

/* Run from the dispatcher thread. */
uint32 GetBatchCount()
{
    return g_dispatcher == NULL ? 0 : g_dispatcher->pageLineNum;
}

bool DispatchPtrIsNull()
{
    return (g_dispatcher == NULL);
}

/* Run from each page worker thread. */
PGPROC* StartupPidGetProc(ThreadId pid)
{
    if (pid == g_instance.proc_base->startupProcPid)
        return g_instance.proc_base->startupProc;
    if ((get_real_recovery_parallelism() > 1) && (GetBatchCount() > 0)) {
        for (uint32 i = 0; i < g_dispatcher->allWorkersCnt; i++) {
            PGPROC* proc = GetPageRedoWorkerProc(g_dispatcher->allWorkers[i]);
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
    if ((get_real_recovery_parallelism() > 1) && (GetBatchCount() > 0)) {
        for (uint32 i = 0; i < g_dispatcher->allWorkersCnt; i++) {
            PGPROC* proc = GetPageRedoWorkerProc(g_dispatcher->allWorkers[i]);
            if (t_thrd.proc->pid == proc->pid) {
                g_dispatcher->allWorkers[i]->bufferPinWaitBufId = bufid;
                break;
            }
        }
    }
}

uint32 GetStartupBufferPinWaitBufLen()
{
    uint32 len = 1;
    if ((get_real_recovery_parallelism() > 1) && (GetBatchCount() > 0)) {
        len += g_dispatcher->allWorkersCnt;
    }
    return len;
}

/* Used by backends when they receive a request to check for buffer pin waits. */
void GetStartupBufferPinWaitBufId(int* bufids, uint32 len)
{
    for (uint32 i = 0; i < len - 1; i++) {
        bufids[i] = g_dispatcher->allWorkers[i]->bufferPinWaitBufId;
    }
    bufids[len - 1] = g_instance.proc_base->startupBufferPinWaitBufId;
}

/* Run from the dispatcher and txn worker thread. */
void UpdateStandbyState(HotStandbyState newState)
{
    PageRedoPipeline* pl = NULL;
    if ((get_real_recovery_parallelism() > 1) && (GetBatchCount() > 0)) {
        for (uint32 i = 0; i < g_dispatcher->pageLineNum; i++) {
            pl = &(g_dispatcher->pageLines[i]);
            UpdatePageRedoWorkerStandbyState(pl->batchThd, newState);
            UpdatePageRedoWorkerStandbyState(pl->managerThd, newState);
            for (uint32 j = 0; j < pl->redoThdNum; j++) {
                UpdatePageRedoWorkerStandbyState(pl->redoThd[j], newState);
            }
        }
        UpdatePageRedoWorkerStandbyState(g_dispatcher->trxnLine.managerThd, newState);
        UpdatePageRedoWorkerStandbyState(g_dispatcher->trxnLine.redoThd, newState);
        UpdatePageRedoWorkerStandbyState(g_dispatcher->readLine.managerThd, newState);
        UpdatePageRedoWorkerStandbyState(g_dispatcher->readLine.readThd, newState);
        pg_atomic_write_u32(&(g_dispatcher->standbyState), newState);
    }
}

/* Run from the dispatcher thread. */
void** GetXLogInvalidPagesFromWorkers()
{
    return CollectStatesFromWorkers(GetXLogInvalidPages);
}

/* Run from the dispatcher thread. */
static void** CollectStatesFromWorkers(GetStateFunc getStateFunc)
{
    if (g_dispatcher->allWorkersCnt > 0) {
        void** stateArray = (void**)palloc(sizeof(void*) * g_dispatcher->allWorkersCnt);
        for (uint32 i = 0; i < g_dispatcher->allWorkersCnt; i++)
            stateArray[i] = getStateFunc(g_dispatcher->allWorkers[i]);
        return stateArray;
    } else
        return NULL;
}

void DiagLogRedoRecord(XLogReaderState* record, const char* funcName)
{
    uint8 info;
    RelFileNode oldRn = {0};
    RelFileNode newRn = {0};
    BlockNumber oldblk = InvalidBlockNumber;
    BlockNumber newblk = InvalidBlockNumber;
    bool newBlkExistFlg = false;
    bool oldBlkExistFlg = false;
    ForkNumber oldFk = InvalidForkNumber;
    ForkNumber newFk = InvalidForkNumber;
    StringInfoData buf;

    /* Support  redo old version xlog during upgrade (Just the runningxact log with chekpoint online ) */
    uint32 rmid = redo_oldversion_xlog ? ((XLogRecordOld*)record->decoded_record)->xl_rmid : XLogRecGetRmid(record);
    info = redo_oldversion_xlog ? ((((XLogRecordOld*)record->decoded_record)->xl_info) & ~XLR_INFO_MASK)
                                : (XLogRecGetInfo(record) & ~XLR_INFO_MASK);

    initStringInfo(&buf);
    RmgrTable[rmid].rm_desc(&buf, record);

    if (XLogRecGetBlockTag(record, 0, &newRn, &newFk, &newblk)) {
        newBlkExistFlg = true;
    }
    if (XLogRecGetBlockTag(record, 1, &oldRn, &oldFk, &oldblk)) {
        oldBlkExistFlg = true;
    }
    ereport(DEBUG4,
        (errmodule(MOD_REDO),
            errcode(ERRCODE_LOG),
            errmsg("[REDO_LOG_TRACE]DiagLogRedoRecord: %s, ReadRecPtr:%lu,EndRecPtr:%lu,"
                   "newBlkExistFlg:%u,"
                   "newRn(spcNode:%u, dbNode:%u, relNode:%u),newFk:%d,newblk:%u,"
                   "oldBlkExistFlg:%d,"
                   "oldRn(spcNode:%u, dbNode:%u, relNode:%u),oldFk:%d,oldblk:%u,"
                   "info:%u,redo_oldversion_xlog:%u, rm_name:%s, desc:%s,"
                   "max_block_id:%d",
                funcName,
                record->ReadRecPtr,
                record->EndRecPtr,
                newBlkExistFlg,
                newRn.spcNode,
                newRn.dbNode,
                newRn.relNode,
                newFk,
                newblk,
                oldBlkExistFlg,
                oldRn.spcNode,
                oldRn.dbNode,
                oldRn.relNode,
                oldFk,
                oldblk,
                info,
                redo_oldversion_xlog,
                RmgrTable[rmid].rm_name,
                buf.data,
                record->max_block_id)));
    pfree_ext(buf.data);
}

XLogRecPtr GetSafeMinCheckPoint()
{
    XLogRecPtr minSafeCheckPoint = MAX_XLOG_REC_PTR;
    for (uint32 i = 0; i < g_dispatcher->allWorkersCnt; ++i) {
        if (g_dispatcher->allWorkers[i]->role == REDO_PAGE_WORKER) {
            if (g_dispatcher->allWorkers[i]->lastCheckedRestartPoint < minSafeCheckPoint) {
                minSafeCheckPoint = g_dispatcher->allWorkers[i]->lastCheckedRestartPoint;
            }
        }
    }

    return minSafeCheckPoint;
}

void GetReplayedRecPtr(XLogRecPtr* startPtr, XLogRecPtr* endPtr)
{
    XLogRecPtr minStart = MAX_XLOG_REC_PTR;
    XLogRecPtr minEnd = MAX_XLOG_REC_PTR;
    for (uint32 i = 0; i < g_dispatcher->allWorkersCnt; ++i) {
        if ((g_dispatcher->allWorkers[i]->role == REDO_PAGE_WORKER) ||
            (g_dispatcher->allWorkers[i]->role == REDO_TRXN_WORKER)) {
            XLogRecPtr tmpStart = MAX_XLOG_REC_PTR;
            XLogRecPtr tmpEnd = MAX_XLOG_REC_PTR;
            GetCompletedReadEndPtr(g_dispatcher->allWorkers[i], &tmpStart, &tmpEnd);
            if (XLByteLT(tmpEnd, minEnd)) {
                minStart = tmpStart;
                minEnd = tmpEnd;
            }
        }
    }
    *startPtr = minStart;
    *endPtr = minEnd;
}

}  // namespace extreme_rto
