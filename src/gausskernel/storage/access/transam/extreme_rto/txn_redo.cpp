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
 * txn_redo.cpp
 *      TxnRedoWorker runs in the dispatcher thread to easy the management
 *      of transaction status and global variables.  In principle, we can
 *      run the TxnRedoWorker in a separate thread, but we don't do it for
 *      now for simplicity.
 *      To ensure read consistency on hot-standby replicas, transactions on
 *      replicas must commit in the same order as the master.  This is the
 *      main reason to use a dedicated worker to replay transaction logs.
 *      To ensure data consistency within a transaction, the transaction
 *      commit log must be replayed after all data logs for the transaction
 *      have been replayed by PageRedoWorkers.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/transam/extreme_rto/txn_redo.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"
#include "postmaster/startup.h"
#include "access/xlog.h"
#include "utils/palloc.h"
#include "utils/guc.h"
#include "portability/instr_time.h"

#include "access/extreme_rto/dispatcher.h"
#include "access/extreme_rto/txn_redo.h"
#include "access/xlogreader.h"
#include "pgstat.h"
#include "storage/standby.h"
#include "catalog/pg_control.h"

namespace extreme_rto {
struct TxnRedoWorker {
    RedoItem* pendingHead; /* The head of the RedoItem list. */
    RedoItem* pendingTail; /* The tail of the RedoItem list. */
    RedoItem* procHead;
    RedoItem* procTail;
};

TxnRedoWorker* StartTxnRedoWorker()
{
    TxnRedoWorker* worker = (TxnRedoWorker*)palloc(sizeof(TxnRedoWorker));
    worker->pendingHead = NULL;
    worker->pendingTail = NULL;

    worker->procHead = NULL;
    worker->procTail = NULL;
    return worker;
}

bool IsTxnWorkerIdle(TxnRedoWorker* worker)
{
    return ((worker->procHead == NULL) && (worker->procTail == NULL));
}

void DestroyTxnRedoWorker(TxnRedoWorker* worker)
{
    pfree(worker);
}

XLogRecPtr GetReplayedRecPtrFromWorkers()
{
    XLogRecPtr minLastReplayedEndRecPtr = MAX_XLOG_REC_PTR;

    for (uint32 i = 0; i < g_dispatcher->pageLineNum; i++) {
        if (!RedoWorkerIsIdle(g_dispatcher->pageLines[i].batchThd)) {
            XLogRecPtr lastReplayedLSN = GetCompletedRecPtr(g_dispatcher->pageLines[i].batchThd);
            if (XLByteLT(lastReplayedLSN, minLastReplayedEndRecPtr)) {
                minLastReplayedEndRecPtr = lastReplayedLSN;
            }
        }
    }
    return minLastReplayedEndRecPtr;
}

XLogRecPtr TrxnStageGetReplayedRecPtrFromWorkers()
{
    XLogRecPtr minLastReplayedEndRecPtr = MAX_XLOG_REC_PTR;

    for (uint32 i = 0; i < g_dispatcher->pageLineNum; i++) {
        if (!RedoWorkerIsIdle(g_dispatcher->pageLines[i].batchThd)) {
            XLogRecPtr lastReplayedLSN = GetCompletedRecPtr(g_dispatcher->pageLines[i].batchThd);
            if (XLByteLT(lastReplayedLSN, minLastReplayedEndRecPtr)) {
                minLastReplayedEndRecPtr = lastReplayedLSN;
            }
        }
    }
    if (!RedoWorkerIsIdle(g_dispatcher->trxnLine.redoThd)) {
        XLogRecPtr lastReplayedLSN = GetCompletedRecPtr(g_dispatcher->trxnLine.redoThd);
        if (XLByteLT(lastReplayedLSN, minLastReplayedEndRecPtr)) {
            minLastReplayedEndRecPtr = lastReplayedLSN;
        }
    }

    return minLastReplayedEndRecPtr;
}

XLogRecPtr TestStageGetReplayedRecPtrFromWorkers()
{
    XLogRecPtr minLastReplayedEndRecPtr = MAX_XLOG_REC_PTR;

    for (uint32 i = 0; i < g_dispatcher->pageLineNum; i++) {
        if (!RedoWorkerIsIdle(g_dispatcher->pageLines[i].batchThd)) {
            XLogRecPtr lastReplayedLSN = GetCompletedRecPtr(g_dispatcher->pageLines[i].batchThd);
            if (XLByteLT(lastReplayedLSN, minLastReplayedEndRecPtr)) {
                minLastReplayedEndRecPtr = lastReplayedLSN;
            }
        }
    }
    if (!RedoWorkerIsIdle(g_dispatcher->trxnLine.redoThd)) {
        XLogRecPtr lastReplayedLSN = GetCompletedRecPtr(g_dispatcher->trxnLine.redoThd);
        if (XLByteLT(lastReplayedLSN, minLastReplayedEndRecPtr)) {
            minLastReplayedEndRecPtr = lastReplayedLSN;
        }
    }
    return minLastReplayedEndRecPtr;
}

void TestSetLastReplayedPtr(XLogRecPtr ReadRecPtr, XLogRecPtr EndRecPtr)
{
    XLogRecPtr tmpEndPtr = TestStageGetReplayedRecPtrFromWorkers();
    if (XLByteLT(tmpEndPtr, EndRecPtr)) {
        SetXLogReplayRecPtr(tmpEndPtr, tmpEndPtr);
    } else {
        SetXLogReplayRecPtr(ReadRecPtr, EndRecPtr);
    }
}

void TrxnSetLastReplayedPtr(XLogRecPtr ReadRecPtr, XLogRecPtr EndRecPtr)
{
    XLogRecPtr tmpEndPtr = TrxnStageGetReplayedRecPtrFromWorkers();
    if (XLByteLT(tmpEndPtr, EndRecPtr)) {
        SetXLogReplayRecPtr(tmpEndPtr, tmpEndPtr);
    } else {
        SetXLogReplayRecPtr(ReadRecPtr, EndRecPtr);
    }
}

extern THR_LOCAL PageRedoWorker* g_redoWorker;
void TestSetLastReplayedPtrComm()
{
    if (g_redoWorker->role != REDO_TRXN_MNG) {
        return;
    }
    XLogRecPtr endPtr = pg_atomic_read_u64(&(g_instance.comm_cxt.predo_cxt.endRecPtr));
    TestSetLastReplayedPtr(endPtr, endPtr);
}

void AddTxnRedoItem(PageRedoWorker* worker, void* item)
{
    (void)SPSCBlockingQueuePut(worker->queue, item);
}

void ApplyReadyTxnShareLogRecords(RedoItem* item)
{
    if (item->shareCount <= 1) {
        ereport(PANIC,
            (errmodule(MOD_REDO),
                errcode(ERRCODE_LOG),
                errmsg("[REDO_LOG_TRACE]ApplyReadyTxnShareLogRecords encounter fatal error:rmgrID:%u, "
                       "info:%u, sharcount:%u",
                    XLogRecGetRmid(&item->record),
                    XLogRecGetInfo(&item->record),
                    item->shareCount)));
    }

    (void)pg_atomic_add_fetch_u32(&item->refCount, 1);
    if ((item->designatedWorker == ALL_WORKER) || (item->designatedWorker == TRXN_WORKER)) {
        pg_memory_barrier();
        /* must be the last one */
        MemoryContext oldCtx = MemoryContextSwitchTo(g_dispatcher->oldCtx);
        ApplyRedoRecord(&item->record, item->oldVersion);
        (void)MemoryContextSwitchTo(oldCtx);

        pg_memory_barrier();

        pg_atomic_write_u32(&item->replayed, 1); /* notify other pageworker continue */
    } else {
        /* panic error */
        ereport(PANIC,
            (errmodule(MOD_REDO),
                errcode(ERRCODE_LOG),
                errmsg("[REDO_LOG_TRACE]ApplyReadyTxnShareLogRecords encounter fatal error:rmgrID:%u, "
                       "info:%u, designatedWorker:%u",
                    XLogRecGetRmid(&item->record),
                    XLogRecGetInfo(&item->record),
                    item->designatedWorker)));
    }

    FreeRedoItem(item);
}

void ApplyReadyAllShareLogRecords(RedoItem* item)
{
    if (item->shareCount != (GetBatchCount() + 1)) {
        /* panic */
        ereport(PANIC,
            (errmodule(MOD_REDO),
                errcode(ERRCODE_LOG),
                errmsg("[REDO_LOG_TRACE]ApplyReadyAllShareLogRecords encounter fatal error:rmgrID:%u, "
                       "info:%u, sharcount:%u",
                    XLogRecGetRmid(&item->record),
                    XLogRecGetInfo(&item->record),
                    item->shareCount)));
    }

    uint32 refCount = pg_atomic_add_fetch_u32(&item->refCount, 1);

    pg_memory_barrier();
    uint64 waitCount = 0;
    pgstat_report_waitevent(WAIT_EVENT_PREDO_APPLY);
    /* must be the last one */
    while (refCount != item->shareCount) {
        ++waitCount;
        refCount = pg_atomic_read_u32(&item->refCount);
        if ((waitCount & OUTPUT_WAIT_COUNT) == OUTPUT_WAIT_COUNT) {
            ereport(PANIC,
                (errmodule(MOD_REDO),
                    errcode(ERRCODE_LOG),
                    errmsg("[REDO_LOG_TRACE]ApplyReadyAllShareLogRecords encounter fatal error:rmgrID:%u, info:%u, "
                           "sharcount:%u, refcount:%u",
                        XLogRecGetRmid(&item->record),
                        XLogRecGetInfo(&item->record),
                        item->shareCount,
                        refCount)));
        }
        HandleStartupProcInterrupts();
    }
    pgstat_report_waitevent(WAIT_EVENT_END);
    MemoryContext oldCtx = MemoryContextSwitchTo(g_dispatcher->oldCtx);
    ApplyRedoRecord(&item->record, item->oldVersion);
    (void)MemoryContextSwitchTo(oldCtx);

    FreeRedoItem(item);
}

void ProcTxnItem(RedoItem* item)
{
    XLogRecPtr EndRecPtr = item->record.EndRecPtr; /* end+1 of last record read */
    TimestampTz recordXTime = item->recordXTime;
    bool imcheckpoint = item->imcheckpoint;

    if (!IsLSNMarker(item)) {
        if (item->sharewithtrxn) {
            ApplyReadyTxnShareLogRecords(item);
        } else if ((item->shareCount == (GetBatchCount() + 1)) && (item->designatedWorker == ALL_WORKER)) {
            /* checkpoint and drop database */
            ApplyReadyAllShareLogRecords(item);
        } else {
            MemoryContext oldCtx = MemoryContextSwitchTo(g_dispatcher->oldCtx);
            ApplyRedoRecord(&item->record, item->oldVersion);
            (void)MemoryContextSwitchTo(oldCtx);
            FreeRedoItem(item);
        }
        if (recordXTime != 0) {
            SetLatestXTime(recordXTime);
        }
    } else {
        FreeRedoItem(item);
    }

    pg_atomic_write_u64(&g_redoWorker->lastReplayedEndRecPtr, EndRecPtr);
    /* update immediate checkpoint */
    if (imcheckpoint)
        t_thrd.xlog_cxt.needImmediateCkp = true;
}

void TrxnMngProc(RedoItem* item)
{
    AddTxnRedoItem(g_dispatcher->trxnLine.redoThd, item);
}

void TrxnWorkerProc(RedoItem* item)
{
    ProcTxnItem(item);
}
}  // namespace extreme_rto
