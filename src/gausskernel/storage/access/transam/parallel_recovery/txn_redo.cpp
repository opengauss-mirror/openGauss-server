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
 *        src/gausskernel/storage/access/transam/parallel_recovery/txn_redo.cpp
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

#include "access/parallel_recovery/dispatcher.h"
#include "access/parallel_recovery/txn_redo.h"
#include "access/multi_redo_api.h"
#include "pgstat.h"

namespace parallel_recovery {

static inline void redoLogTrace(RedoItem *item)
{
    ereport(LOG, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                  errmsg("[REDO_LOG_TRACE]pendingHead : sharewithtrxn %u, blockbytrxn %u, imcheckpoint %u, "
                         "shareCount %u, designatedWorker %u, refCount %u, replayed %u, ReadRecPtr %lu, "
                         "EndRecPtr %lu",
                         (uint32)item->sharewithtrxn, (uint32)item->blockbytrxn, (uint32)item->imcheckpoint,
                         item->shareCount, item->designatedWorker, item->refCount, item->replayed,
                         item->record.ReadRecPtr, item->record.EndRecPtr)));
}

struct TxnRedoWorker {
    RedoItem *pendingHead; /* The head of the RedoItem list. */
    RedoItem *pendingTail; /* The tail of the RedoItem list. */
    RedoItem *procHead;
    RedoItem *procTail;
};

TxnRedoWorker *StartTxnRedoWorker()
{
    TxnRedoWorker *worker = (TxnRedoWorker *)palloc(sizeof(TxnRedoWorker));
    worker->pendingHead = NULL;
    worker->pendingTail = NULL;

    worker->procHead = NULL;
    worker->procTail = NULL;
    return worker;
}

bool IsTxnWorkerIdle(TxnRedoWorker *worker)
{
    return ((worker->procHead == NULL) && (worker->procTail == NULL));
}

void DestroyTxnRedoWorker(TxnRedoWorker *worker)
{
    pfree(worker);
}

void AddTxnRedoItem(TxnRedoWorker *worker, RedoItem *item)
{
    /*
     * TxnRedoItems are never shared with other workers.
     * Simply use the next pointer for worker 0.
     */
    if (worker->pendingHead == NULL) {
        worker->pendingHead = item;
    } else {
        worker->pendingTail->nextByWorker[0] = item;
    }
    item->nextByWorker[0] = NULL;
    worker->pendingTail = item;
}

void ApplyReadyTxnShareLogRecords(RedoItem *item)
{
    if (item->shareCount <= 1) {
        ereport(PANIC, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                        errmsg("[REDO_LOG_TRACE]ApplyReadyTxnShareLogRecords encounter fatal error:rmgrID:%u, "
                               "info:%u, sharcount:%u",
                               (uint32)XLogRecGetRmid(&item->record), (uint32)XLogRecGetInfo(&item->record),
                               item->shareCount)));
    }

    uint32 refCount = pg_atomic_add_fetch_u32(&item->refCount, 1);
    if ((item->designatedWorker == ALL_WORKER) || (item->designatedWorker == TRXN_WORKER)) {
        pg_memory_barrier();
        /* must be the last one */
        uint64 waitCount = 0;
        pgstat_report_waitevent(WAIT_EVENT_PREDO_APPLY);
        while (refCount != item->shareCount) {
            ++waitCount;
            refCount = pg_atomic_read_u32(&item->refCount);

            if ((waitCount & OUTPUT_WAIT_COUNT) == OUTPUT_WAIT_COUNT) {
                ereport(
                    PANIC,
                    (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                     errmsg("[REDO_LOG_TRACE]ApplyReadyTxnShareLogRecords encounter fatal error:rmgrID:%u, info:%u, "
                            "sharcount:%u, refcount:%u",
                            (uint32)XLogRecGetRmid(&item->record), (uint32)XLogRecGetInfo(&item->record),
                            item->shareCount, refCount)));
            }
            RedoInterruptCallBack();
        }
        pgstat_report_waitevent(WAIT_EVENT_END);

        MemoryContext oldCtx = MemoryContextSwitchTo(g_dispatcher->oldCtx);
        ApplyRedoRecord(&item->record);
        (void)MemoryContextSwitchTo(oldCtx);

        pg_memory_barrier();

        pg_atomic_write_u32(&item->replayed, 1); /* notify other pageworker continue */
    } else {
        /* panic error */
        ereport(PANIC, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                        errmsg("[REDO_LOG_TRACE]ApplyReadyTxnShareLogRecords encounter fatal error:rmgrID:%u, "
                               "info:%u, designatedWorker:%u",
                               (uint32)XLogRecGetRmid(&item->record), (uint32)XLogRecGetInfo(&item->record),
                               item->designatedWorker)));
    }

    /*
     * If we are the last worker leaving this record, we should destroy
     * the copied record.
     */
    if (pg_atomic_sub_fetch_u32(&item->refCount, 1) == 0) {
        FreeRedoItem(item);
    }
}

void ApplyReadyAllShareLogRecords(RedoItem *item)
{
    if (item->shareCount != (GetPageWorkerCount() + 1)) {
        /* panic */
        ereport(PANIC, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                        errmsg("[REDO_LOG_TRACE]ApplyReadyAllShareLogRecords encounter fatal error:rmgrID:%u, "
                               "info:%u, sharcount:%u",
                               (uint32)XLogRecGetRmid(&item->record), (uint32)XLogRecGetInfo(&item->record),
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
                    (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                     errmsg("[REDO_LOG_TRACE]ApplyReadyAllShareLogRecords encounter fatal error:rmgrID:%u, info:%u, "
                            "sharcount:%u, refcount:%u",
                            (uint32)XLogRecGetRmid(&item->record), (uint32)XLogRecGetInfo(&item->record),
                            item->shareCount, refCount)));
        }
        RedoInterruptCallBack();
    }
    pgstat_report_waitevent(WAIT_EVENT_END);
    MemoryContext oldCtx = MemoryContextSwitchTo(g_dispatcher->oldCtx);
    ApplyRedoRecord(&item->record);
    (void)MemoryContextSwitchTo(oldCtx);

    FreeRedoItem(item);
}

void MoveTxnItemToApplyQueue(TxnRedoWorker *worker)
{
    if (worker->pendingHead == NULL) {
        return;
    }
    if (worker->procHead == NULL) {
        worker->procHead = worker->pendingHead;
    } else {
        worker->procTail->nextByWorker[0] = worker->pendingHead;
    }

    worker->procTail = worker->pendingTail;
    worker->pendingHead = NULL;
    worker->pendingTail = NULL;
}

static RedoItem *ProcTxnItem(RedoItem *item)
{
    RedoItem *nextitem = item->nextByWorker[0];
    XLogRecPtr ReadRecPtr = item->record.ReadRecPtr; /* start of last record read */
    XLogRecPtr EndRecPtr = item->record.EndRecPtr;   /* end+1 of last record read */
    TimestampTz recordXTime = item->recordXTime;
    bool imcheckpoint = item->imcheckpoint;

    if (!IsLSNMarker(item)) {
        if (item->sharewithtrxn) {
            ApplyReadyTxnShareLogRecords(item);
        } else if ((item->shareCount == (GetPageWorkerCount() + 1)) && (item->designatedWorker == ALL_WORKER)) {
            /* checkpoint and drop database */
            ApplyReadyAllShareLogRecords(item);
        } else {
            MemoryContext oldCtx = MemoryContextSwitchTo(g_dispatcher->oldCtx);
            ApplyRedoRecord(&item->record);
            (void)MemoryContextSwitchTo(oldCtx);
            FreeRedoItem(item);
        }
        if (recordXTime != 0) {
            SetLatestXTime(recordXTime);
        }
    } else {
        FreeRedoItem(item);
    }

    SetXLogReplayRecPtr(ReadRecPtr, EndRecPtr);
    CheckRecoveryConsistency();
    /* update immediate checkpoint */
    if (imcheckpoint) {
        t_thrd.xlog_cxt.needImmediateCkp = true;
    }
    return nextitem;
}

void ApplyReadyTxnLogRecords(TxnRedoWorker *worker, bool forceAll)
{
    RedoItem *item = worker->procHead;
    GetRedoStartTime(t_thrd.xlog_cxt.timeCost[TIME_COST_STEP_5]);
    while (item != NULL) {
        XLogReaderState *record = &item->record;
        XLogRecPtr lrEnd;

        if (forceAll) {
            GetRedoStartTime(t_thrd.xlog_cxt.timeCost[TIME_COST_STEP_6]);
            XLogRecPtr lrRead; /* lastReplayedReadPtr */
            GetReplayedRecPtrFromWorkers(&lrRead, &lrEnd);
            /* we need to get lastCompletedPageLSN as soon as possible,so */
            /* we can not sleep here. */
            XLogRecPtr oldReplayedPageLSN = InvalidXLogRecPtr;
            while (XLByteLT(lrEnd, record->EndRecPtr)) {
                /* update lastreplaylsn */
                if (!XLByteEQ(oldReplayedPageLSN, lrEnd)) {
                    SetXLogReplayRecPtr(lrRead, lrEnd);
                    oldReplayedPageLSN = lrEnd;
                }
                GetReplayedRecPtrFromWorkers(&lrRead, &lrEnd);
                RedoInterruptCallBack();
            }
            CountRedoTime(t_thrd.xlog_cxt.timeCost[TIME_COST_STEP_6]);
        }

        GetReplayedRecPtrFromWorkers(&lrEnd);
        /*
         * Make sure we can replay this record.  This check is necessary
         * on the master and on the hot backup after it reaches consistency.
         */
        if (XLByteLE(record->EndRecPtr, lrEnd)) {
            item = ProcTxnItem(item);
        } else {
            break;
        }
    }
    /*
     * On hot backup, update replay location.  Note that the begin
     * LSN is not accurate here.  The begin LSN is only used to
     * confirm the flush location of the hot standby.
     */
    if (item == NULL) {
        XLogRecPtr oldReplayedPageLSN = InvalidXLogRecPtr;
        XLogRecPtr lrRead;
        XLogRecPtr lrEnd;
        do {
            GetReplayedRecPtrFromWorkers(&lrRead, &lrEnd);
            if (XLByteLT(g_dispatcher->dispatchEndRecPtr, lrEnd)) {
                lrEnd = g_dispatcher->dispatchEndRecPtr;
                lrRead = g_dispatcher->dispatchReadRecPtr;
            }
            if (!XLByteEQ(oldReplayedPageLSN, lrEnd)) {
                SetXLogReplayRecPtr(lrRead, lrEnd);
                oldReplayedPageLSN = lrEnd;
            }
            RedoInterruptCallBack();
        } while (forceAll && XLByteLT(lrEnd, g_dispatcher->dispatchEndRecPtr));
    }

    worker->procHead = item;
    if (item == NULL) {
        worker->procTail = NULL;
    }
    CountRedoTime(t_thrd.xlog_cxt.timeCost[TIME_COST_STEP_5]);
}

void DumpTxnWorker(TxnRedoWorker *txnWorker)
{
    if (txnWorker->pendingHead != NULL) {
        redoLogTrace(txnWorker->pendingHead);
    }

    if (txnWorker->procHead != NULL) {
        redoLogTrace(txnWorker->procHead);
    }

    DumpXlogCtl();
}

bool TxnQueueIsEmpty(TxnRedoWorker* worker)
{
    return worker->procHead == NULL;
}

}  // namespace parallel_recovery
