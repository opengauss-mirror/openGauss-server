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
 * knl_uundorecycle.cpp
 *    c++ code
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/ustore/undo/knl_uundorecycle.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "access/transam.h"
#include "access/ustore/undo/knl_uundoapi.h"
#include "access/ustore/undo/knl_uundozone.h"
#include "access/ustore/undo/knl_uundospace.h"
#include "access/ustore/undo/knl_uundotxn.h"
#include "access/ustore/undo/knl_uundotype.h"
#include "access/ustore/undo/knl_uundoxlog.h"
#include "access/ustore/knl_undorequest.h"
#include "access/ustore/knl_whitebox_test.h"
#include "access/multi_redo_api.h"
#include "access/extreme_rto/page_redo.h"
#include "gssignal/gs_signal.h"
#include "knl/knl_thread.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/lock/lwlock.h"
#include "threadpool/threadpool.h"
#include "utils/dynahash.h"
#include "utils/postinit.h"
#include "utils/gs_bitmap.h"
#include "pgstat.h"
#include "access/ustore/knl_uvisibility.h"

#define TRANS_PARTITION_LINEAR_SPARE_TIME(degree) \
    (degree > 3000 ? 3000 : degree)

#define ALLOCSET_UNDO_RECYCLE_MAXSIZE ALLOCSET_DEFAULT_MAXSIZE * 4

typedef struct UndoXmins {
    TransactionId oldestXmin;
    TransactionId recycleXmin;
} UndoXmins;

namespace undo {
const int EXIT_NUMBER = 2;
const float FORCE_RECYCLE_PERCENT = 0.8;
const int FORCE_RECYCLE_RETRY_TIMES = 5;
const float FORCE_RECYCLE_PUSH_PERCENT = 0.2;
const long SLOT_BUFFER_CACHE_SIZE = 16384;
const int UNDO_RECYCLE_TIMEOUT_DELTA = 50;

static uint64 g_recycleLoops = 0;
static int g_forceRecycleSize = 0;
static UndoSlotBufferCache *g_slotBufferCache;

void UndoQuickdie(SIGNAL_ARGS)
{
    gs_signal_setmask(&t_thrd.libpq_cxt.BlockSig, NULL);
    on_exit_reset();
    exit(EXIT_NUMBER);
}

void UndoShutdownHandler(SIGNAL_ARGS)
{
    int save_errno = errno;
    t_thrd.undorecycler_cxt.shutdown_requested = true;
    if (t_thrd.proc)
        SetLatch(&t_thrd.proc->procLatch);
    errno = save_errno;
}

bool AsyncRollback(UndoZone *zone, UndoSlotPtr recycle, TransactionSlot *slot)
{
    if (slot->NeedRollback()) {
        if (zone->GetPersitentLevel() == UNDO_TEMP || zone->GetPersitentLevel() == UNDO_UNLOGGED) {
            return true;
        }
        if (!u_sess->attr.attr_storage.enable_ustore_async_rollback) {
            return true;
        }
        UndoRecPtr prev = GetPrevUrp(slot->EndUndoPtr());
        AddRollbackRequest(slot->XactId(), prev, slot->StartUndoPtr(),
            slot->DbId(), recycle);
#ifdef DEBUG_UHEAP
        UHEAPSTAT_COUNT_UNDO_SPACE_UNRECYCLE();
#endif
        return true;
    }
    return false;
}

bool VerifyFrozenXidAdvance(TransactionId oldestXmin, TransactionId globalFrozenXid, VerifyLevel level)
{
    if (u_sess->attr.attr_storage.ustore_verify_level <= USTORE_VERIFY_DEFAULT) {
        return true;
    }
    if (TransactionIdIsNormal(globalFrozenXid) && TransactionIdFollows(globalFrozenXid,  oldestXmin)) {
        ereport(PANIC, (errmodule(MOD_UNDO),
            errmsg(UNDOFORMAT(
                "Advance frozen xid failed, globalFrozenXid %lu is bigger than oldestXmin %lu."),
                globalFrozenXid, oldestXmin)));
    }
    if (TransactionIdIsNormal(globalFrozenXid) &&
        TransactionIdIsNormal(g_instance.undo_cxt.globalFrozenXid) &&
        TransactionIdPrecedes(globalFrozenXid, g_instance.undo_cxt.globalFrozenXid)) {
        ereport(PANIC, (errmodule(MOD_UNDO),
            errmsg(UNDOFORMAT(
                "Advance frozen xid failed, globalFrozenXid %lu is smaller than globalFrozenXid %lu."),
                globalFrozenXid, g_instance.undo_cxt.globalFrozenXid)));
    }
    if (TransactionIdIsNormal(globalFrozenXid) &&
        TransactionIdIsNormal(g_instance.undo_cxt.globalRecycleXid) &&
        TransactionIdPrecedes(globalFrozenXid, g_instance.undo_cxt.globalRecycleXid)) {
        ereport(PANIC, (errmodule(MOD_UNDO),
            errmsg(UNDOFORMAT(
                "Advance frozen xid failed, globalFrozenXid %lu is smaller than globalRecycleXid %lu."),
                globalFrozenXid, g_instance.undo_cxt.globalRecycleXid)));
    }
    return true;
}

bool VerifyRecycleXidAdvance(TransactionId globalFrozenXid, TransactionId oldestRecycleXid, VerifyLevel level)
{
    if (u_sess->attr.attr_storage.ustore_verify_level <= USTORE_VERIFY_DEFAULT) {
        return true;
    }
    if (TransactionIdIsNormal(oldestRecycleXid) && TransactionIdFollows(oldestRecycleXid,  globalFrozenXid)) {
        ereport(PANIC, (errmodule(MOD_UNDO),
            errmsg(UNDOFORMAT(
                "Advance recycle xid failed, oldestRecycleXid %lu is bigger than globalFrozenXid %lu."),
                oldestRecycleXid, globalFrozenXid)));
    }
    if (TransactionIdIsNormal(oldestRecycleXid) &&
        TransactionIdIsNormal(g_instance.undo_cxt.globalRecycleXid) &&
        TransactionIdPrecedes(oldestRecycleXid, g_instance.undo_cxt.globalRecycleXid)) {
        ereport(PANIC, (errmodule(MOD_UNDO),
            errmsg(UNDOFORMAT(
                "Advance recycle xid failed, oldestRecycleXid %lu is smaller than globalRecycleXid %lu."),
                globalFrozenXid, g_instance.undo_cxt.globalRecycleXid)));
    }
    return true;
}

static void RecheckUndoRecycleXid(UndoZone *zone, TransactionSlot *slot, UndoSlotPtr slotPtr)
{
    TransactionId globalFronzenXid = pg_atomic_read_u64(&g_instance.undo_cxt.globalFrozenXid);
    TransactionId globalRecycleXid = pg_atomic_read_u64(&g_instance.undo_cxt.globalRecycleXid);
    TransactionId slotXid = slot->XactId();
    TransactionId recycleXmin = InvalidTransactionId;
    TransactionId oldestXmin = GetOldestXminForUndo(&recycleXmin);
    int elogLevel = WARNING;
    if (TransactionIdOlderThanAllUndo(globalFronzenXid) ||
        (TransactionIdIsValid(slotXid) && TransactionIdOlderThanFrozenXid(slotXid) &&
         !UHeapTransactionIdDidCommit(slotXid) && slot->NeedRollback())) {
        elogLevel = PANIC;
    }
    ereport(elogLevel,
            (errmodule(MOD_UNDO),
             errmsg(UNDOFORMAT("recycle slot xid preceeds than globalFrozenXid: "
                               "zone %d frozenXid %lu, frozenSlotPtr %lu, recycleXid %lu, recycleSlotPtr %lu, "
                               "slot %lu xid %lu, needRollback %d, oldestXmin %lu, recycleXmin %lu, "
                               "globalFrozenXid %lu, globalRecycleXid %lu."),
                    zone->GetZoneId(), zone->GetFrozenXid(), zone->GetFrozenSlotPtr(), zone->GetRecycleXid(),
                    zone->GetRecycleTSlotPtr(), slotPtr, slotXid, slot->NeedRollback(), oldestXmin, recycleXmin,
                    globalFronzenXid, globalRecycleXid)));
    return;
}

void AdvanceFrozenXid(UndoZone *zone, TransactionId *oldestFozenXid,
    TransactionId oldestXmin, UndoSlotPtr *oldestFrozenSlotPtr)
{
    TransactionSlot *slot = NULL;
    UndoSlotPtr frozenSlotPtr = zone->GetFrozenSlotPtr();
    UndoSlotPtr recycle = zone->GetRecycleTSlotPtr();
    UndoSlotPtr allocate = zone->GetAllocateTSlotPtr();
    UndoSlotPtr currentSlotPtr =  frozenSlotPtr > recycle ? frozenSlotPtr : recycle;
    UndoSlotPtr start = INVALID_UNDO_SLOT_PTR;
    while (currentSlotPtr < allocate) {
        UndoSlotBuffer& slotBuf = g_slotBufferCache->FetchTransactionBuffer(currentSlotPtr);
        slotBuf.PrepareTransactionSlot(currentSlotPtr);
        start = currentSlotPtr;
        bool finishAdvanceXid = false;
        while (slotBuf.BufBlock() == UNDO_PTR_GET_BLOCK_NUM(currentSlotPtr) && (currentSlotPtr < allocate)) {
            slot = slotBuf.FetchTransactionSlot(currentSlotPtr);
    WHITEBOX_TEST_STUB(UNDO_RECYCL_ESPACE_FAILED, WhiteboxDefaultErrorEmit);
            pg_read_barrier();
            if (!TransactionIdIsValid(slot->XactId())) {
                *oldestFozenXid = pg_atomic_read_u64(&g_instance.undo_cxt.globalFrozenXid);
                RecheckUndoRecycleXid(zone, slot, currentSlotPtr);
                finishAdvanceXid = true;
                break;
            }

            if (TransactionIdPrecedes(slot->XactId(), pg_atomic_read_u64(&g_instance.undo_cxt.globalFrozenXid))) {
                RecheckUndoRecycleXid(zone, slot, currentSlotPtr);
            }
            zone->SetFrozenXid(slot->XactId());
            if (slot->StartUndoPtr() == INVALID_UNDO_REC_PTR) {
                finishAdvanceXid = true;
                break;
            }
#ifdef DEBUG_UHEAP
            UHEAPSTAT_COUNT_UNDO_SPACE_RECYCLE();
#endif
            ereport(DEBUG1, (errmodule(MOD_UNDO),
                errmsg(UNDOFORMAT(
                    "zone id %d, currentSlotPtr %lu. slot xactid %lu, oldestXmin %lu"),
                    zone->GetZoneId(), currentSlotPtr, slot->XactId(), oldestXmin)));
            *oldestFrozenSlotPtr = currentSlotPtr;

            if (!UHeapTransactionIdDidCommit(slot->XactId()) &&
                TransactionIdPrecedes(slot->XactId(), oldestXmin)) {
                if (AsyncRollback(zone, currentSlotPtr, slot)) {
                    *oldestFozenXid = slot->XactId();
                    zone->SetFrozenXid(slot->XactId());
                    ereport(DEBUG1, (errmodule(MOD_UNDO),
                        errmsg(UNDOFORMAT(
                            "Transaction add to async rollback queue, zone: %d, oldestFozenXid: %lu,"
                            "oldestFrozenSlotPtr %lu. oldestXmin %lu"),
                            zone->GetZoneId(), *oldestFozenXid, *oldestFrozenSlotPtr, oldestXmin)));
                    finishAdvanceXid = true;
                    break;
                }
            }
            if (TransactionIdFollowsOrEquals(slot->XactId(), oldestXmin)) {
                ereport(DEBUG1, (errmodule(MOD_UNDO),
                    errmsg(UNDOFORMAT(
                        "zone %d, slotxid %lu, oldestXmin %lu, oldestFozenXid %lu, oldestFrozenSlotPtr %lu."),
                        zone->GetZoneId(), slot->XactId(), oldestXmin, *oldestFozenXid, *oldestFrozenSlotPtr)));
                finishAdvanceXid = true;
                zone->SetFrozenXid(oldestXmin);
                break;
            }
            currentSlotPtr = GetNextSlotPtr(currentSlotPtr);
            *oldestFrozenSlotPtr = currentSlotPtr;
            if (slotBuf.BufBlock() != UNDO_PTR_GET_BLOCK_NUM(currentSlotPtr)) {
                g_slotBufferCache->RemoveSlotBuffer(start);
                slotBuf.Release();
            }
        }
        if (finishAdvanceXid) {
            break;
        }
    }
}

bool RecycleUndoSpace(UndoZone *zone, TransactionId recycleXmin, TransactionId frozenXid,
    TransactionId *oldestRecycleXid, TransactionId forceRecycleXid)
{
    UndoSlotPtr recycle = zone->GetRecycleTSlotPtr();
    UndoSlotPtr allocate = zone->GetAllocateTSlotPtr();
    TransactionSlot *slot = NULL;
    UndoRecPtr endUndoPtr = INVALID_UNDO_REC_PTR;
    UndoRecPtr oldestEndUndoPtr = INVALID_UNDO_REC_PTR;
    bool forceRecycle = false;
    bool needWal = false;
    TransactionId recycleXid = InvalidTransactionId;
    bool undoRecycled = false;
    bool result = false;
    UndoSlotPtr start = INVALID_UNDO_SLOT_PTR;
    TransactionId globalRecycleXid = pg_atomic_read_u64(&g_instance.undo_cxt.globalRecycleXid);
    
    *oldestRecycleXid = recycleXmin < frozenXid ? recycleXmin : frozenXid;
    if (zone->GetPersitentLevel() == UNDO_PERMANENT) {
        needWal = true;
    }
    Assert(recycle <= allocate);
    while (recycle < allocate) {
        UndoSlotBuffer& slotBuf = g_slotBufferCache->FetchTransactionBuffer(recycle);
        UndoRecPtr startUndoPtr = INVALID_UNDO_REC_PTR;
        start = recycle;
        slotBuf.PrepareTransactionSlot(recycle);
        undoRecycled = false;
        Assert(slotBuf.BufBlock() == UNDO_PTR_GET_BLOCK_NUM(recycle));
        while (slotBuf.BufBlock() == UNDO_PTR_GET_BLOCK_NUM(recycle) && (recycle < allocate)) {
            slot = slotBuf.FetchTransactionSlot(recycle);

    WHITEBOX_TEST_STUB(UNDO_RECYCL_ESPACE_FAILED, WhiteboxDefaultErrorEmit);
            pg_read_barrier();
            if (!TransactionIdIsValid(slot->XactId())) {
                RecheckUndoRecycleXid(zone, slot, recycle);
                break;
            }
            if (slot->StartUndoPtr() == INVALID_UNDO_REC_PTR) {
                break;
            }
            if (TransactionIdPrecedes(slot->XactId(), globalRecycleXid)) {
                RecheckUndoRecycleXid(zone, slot, recycle);
            }

#ifdef DEBUG_UHEAP
            UHEAPSTAT_COUNT_UNDO_SPACE_RECYCLE();
#endif
            if (TransactionIdPrecedes(slot->XactId(), recycleXmin)) {
                Assert(forceRecycle == false);

#ifdef ENABLE_WHITEBOX
                if (TransactionIdPrecedes(slot->XactId(), frozenXid)) {
                    if (!UHeapTransactionIdDidCommit(slot->XactId()) && slot->NeedRollback()) {
                        ereport(PANIC, (errmodule(MOD_UNDO),
                        errmsg(UNDOFORMAT(
                            "Recycle visibility check wrong: zone %d "
                            "transaction slot %lu xid %lu slot->XactId() %lu, globalRecycleXid %lu."),
                            zone->GetZoneId(), recycle, recycleXid, slot->XactId(), globalRecycleXid)));
                    }
                }
#endif

                if (TransactionIdFollowsOrEquals(slot->XactId(), frozenXid)) {
                    break;
                }
            } else {
                bool forceRecycleXidCheck = (TransactionIdIsNormal(forceRecycleXid)
                    && TransactionIdPrecedes(slot->XactId(), forceRecycleXid));
                if (!forceRecycleXidCheck) {
                    break;
                }
                bool isInProgress = TransactionIdIsInProgress(slot->XactId());
                if (isInProgress) {
                    break;
                }
                bool slotTranactionStateCheck = (!UHeapTransactionIdDidCommit(slot->XactId())
                    && !isInProgress && slot->NeedRollback());
                if (slotTranactionStateCheck) {
                    AsyncRollback(zone, recycle, slot);
                    break;
                }
                ereport(LOG, (errmodule(MOD_UNDO), errmsg(UNDOFORMAT("ForceRecycle: slot=%lu, slotxid=%lu, " 
                    "recyclexid=%lu, recycleXmin=%lu, startptr=%lu, endptr=%lu."), recycle, slot->XactId(), 
                    forceRecycleXid, recycleXmin, UNDO_PTR_GET_OFFSET(slot->StartUndoPtr()),
                    UNDO_PTR_GET_OFFSET(slot->EndUndoPtr()))));
                forceRecycle = true;
            }
#ifdef DEBUG_UHEAP
            UHEAPSTAT_COUNT_UNDO_SPACE_RECYCLE();
#endif
            ereport(DEBUG1, (errmodule(MOD_UNDO),
                errmsg(UNDOFORMAT("recycle zone %d, transaction slot %lu xid %lu start ptr %lu end ptr %lu."),
                    zone->GetZoneId(), recycle, slot->XactId(), 
                    slot->StartUndoPtr(), slot->EndUndoPtr())));
            if (!startUndoPtr) {
                startUndoPtr = slot->StartUndoPtr();
            }
            if (!forceRecycle) {
                oldestEndUndoPtr = slot->EndUndoPtr();
            }
            endUndoPtr = slot->EndUndoPtr();
            recycleXid = slot->XactId();
            undoRecycled = true;
            recycle = GetNextSlotPtr(recycle);
            if (slotBuf.BufBlock() != UNDO_PTR_GET_BLOCK_NUM(recycle)) {
                g_slotBufferCache->RemoveSlotBuffer(start);
                slotBuf.Release();
            }
        }
        if (undoRecycled) {
            Assert(TransactionIdIsValid(recycleXid) && (zone->GetRecycleXid() < recycleXid));
            zone->LockUndoZone();
            if (!zone->CheckRecycle(startUndoPtr, endUndoPtr, false)) {
                ereport(PANIC, (errmodule(MOD_UNDO), errmsg(UNDOFORMAT("zone %d recycle start %lu >= recycle end %lu."),
                        zone->GetZoneId(), startUndoPtr, endUndoPtr)));
            }
            if (IS_VALID_UNDO_REC_PTR(oldestEndUndoPtr)) {
                int startZid = UNDO_PTR_GET_ZONE_ID(startUndoPtr);
                int endZid = UNDO_PTR_GET_ZONE_ID(oldestEndUndoPtr);
                if (unlikely(startZid != endZid)) {
                    Assert(UNDO_PTR_GET_OFFSET(endUndoPtr) == UNDO_LOG_MAX_SIZE);
                    oldestEndUndoPtr = MAKE_UNDO_PTR(startZid, UNDO_LOG_MAX_SIZE);
                }
                zone->SetDiscardURecPtr(oldestEndUndoPtr);
            }

            START_CRIT_SECTION();
            zone->SetRecycleXid(recycleXid);
            *oldestRecycleXid = recycleXid;
            zone->SetForceDiscardURecPtr(endUndoPtr);
            zone->SetRecycleTSlotPtr(recycle);
            Assert(zone->GetForceDiscardURecPtr() <= zone->GetInsertURecPtr());
            result = true;

            if (needWal) {
                zone->MarkDirty();
                XlogUndoDiscard xlrec;
                xlrec.endSlot = recycle;
                xlrec.startSlot = start;
                xlrec.recycleLoops = g_recycleLoops;
                xlrec.recycledXid = recycleXid;
                xlrec.oldestXmin = recycleXmin;
                xlrec.endUndoPtr = endUndoPtr;
                XLogRecPtr lsn = WriteUndoXlog(&xlrec, XLOG_UNDO_DISCARD);
                zone->SetLSN(lsn);
                ereport(DEBUG1,
                    (errmodule(MOD_UNDO), errmsg(UNDOFORMAT("zone %d recycle slot start %lu end %lu from slot %lu "
                                                            "to slot %lu lsn %lu xid %lu loops %lu oldestXmin %lu."),
                        zone->GetZoneId(), startUndoPtr, endUndoPtr, start, recycle, 
                        lsn, recycleXid, g_recycleLoops, recycleXmin)));
            }
            END_CRIT_SECTION();

            zone->UnlockUndoZone();
            zone->ReleaseSpace(startUndoPtr, endUndoPtr, &g_forceRecycleSize);
            zone->ReleaseSlotSpace(start, recycle, &g_forceRecycleSize);
        } else {
            /* zone has nothing to recycle. */
            break;
        }
    }
    return result;
}

static void UpdateRecyledXid(TransactionId* recycleMaxXIDs, uint32 *count, TransactionId recycleXid)
{
    if (recycleXid != InvalidTransactionId) {
        uint32 idx = *count;
        recycleMaxXIDs[idx] = recycleXid;
        *count = idx + 1;
    }
}

static void UpdateXidHavingUndo(TransactionId *recycleMaxXIDs, uint32 count, TransactionId *oldestXidHavingUndo,
    TransactionId oldestXmin)
{
    uint32 idx = 0;
    if (count > 0) {
        for (idx = 0; idx < count; idx++) {
            if (*oldestXidHavingUndo == InvalidTransactionId) {
                *oldestXidHavingUndo = recycleMaxXIDs[idx];
            } else {
                /* get oldest xid in recycleMaxXIDs */
                if (TransactionIdFollows(*oldestXidHavingUndo, recycleMaxXIDs[idx])) {
                    *oldestXidHavingUndo = recycleMaxXIDs[idx];
                }
            }
        }
    } else {
        *oldestXidHavingUndo = oldestXmin;
    }
}

static bool NeedForceRecycle(void)
{
    int totalSize = (int)pg_atomic_read_u32(&g_instance.undo_cxt.undoTotalSize);
    int limitSize = (int)(u_sess->attr.attr_storage.undo_space_limit_size * FORCE_RECYCLE_PERCENT);
    int metaSize = (int)g_instance.undo_cxt.undoMetaSize;
    Assert(totalSize >= 0 && limitSize >= 0 && metaSize >= 0);
    g_forceRecycleSize = totalSize + metaSize - limitSize;
    if (g_forceRecycleSize >= 0) {
        ereport(DEBUG1, (errmodule(MOD_UNDO),
            errmsg(UNDOFORMAT("Need ForceRecycle: undoTotalSize=%d, metaSize=%d, limitSize=%d, forceRecycleSize=%d."),
                totalSize, metaSize, limitSize, g_forceRecycleSize)));
        return true;
    }
    return false;
}

static TransactionId GetForceRecycleXid(TransactionId oldestXmin, int retry)
{
    TransactionId forceRecycleXid = InvalidTransactionId;
    TransactionId cxid = t_thrd.xact_cxt.ShmemVariableCache->nextXid;
    Assert(cxid >= oldestXmin);
    if (oldestXmin == cxid || retry > FORCE_RECYCLE_RETRY_TIMES) {
        return InvalidTransactionId;
    }
    forceRecycleXid = (cxid - oldestXmin) * retry * FORCE_RECYCLE_PUSH_PERCENT + oldestXmin;
    return forceRecycleXid;
}

/* SIGHUP: set flag to re-read config file at next convenient time */
static void UndoRecycleSigHupHandler(SIGNAL_ARGS)
{
    int save_errno = errno;
    t_thrd.undorecycler_cxt.got_SIGHUP = true;
    if (t_thrd.proc)
        SetLatch(&t_thrd.proc->procLatch);
    errno = save_errno;
}

static void ShutDownRecycle(TransactionId *recycleMaxXIDs)
{
    ereport(LOG, (errmodule(MOD_UNDO), errmsg(UNDOFORMAT("UndoRecycler: shutting down"))));
    pfree(recycleMaxXIDs);
    g_slotBufferCache->Destory();
    ResourceOwnerRelease(t_thrd.utils_cxt.CurrentResourceOwner, RESOURCE_RELEASE_BEFORE_LOCKS, false, true);
    proc_exit(0);
}

static void WaitRecycleThread(uint64 nonRecycled)
{
    int rc = 0;
    rc = WaitLatch(&t_thrd.proc->procLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, 
        TRANS_PARTITION_LINEAR_SPARE_TIME(nonRecycled));
    /* Clear any already-pending wakeups */
    ResetLatch(&t_thrd.proc->procLatch);
    if (((unsigned int)rc) & WL_POSTMASTER_DEATH) {
        gs_thread_exit(1);
    }
}

static void RecycleWaitIfNotUsed()
{
   if (!g_instance.attr.attr_storage.enable_ustore
#ifdef ENABLE_MULTIPLE_NODES
        || true
#endif
        ) {
        uint64 nonRecycled = 0;
        while (true) {
            if (t_thrd.undorecycler_cxt.shutdown_requested) {
                ereport(LOG, (errmodule(MOD_UNDO), errmsg(UNDOFORMAT("UndoRecycler: shutting down"))));
                ResourceOwnerRelease(t_thrd.utils_cxt.CurrentResourceOwner, RESOURCE_RELEASE_BEFORE_LOCKS, false, true);
                proc_exit(0);
            }
            nonRecycled++;
            WaitRecycleThread(nonRecycled);
        }
    }
}

void exrto_standby_release_space(UndoZone *zone, TransactionId recycle_xid, UndoRecPtr start_undo_ptr,
    UndoRecPtr end_undo_ptr, UndoSlotPtr recycle_exrto)
{
    UndoRecPtr oldest_end_undo_ptr = end_undo_ptr;
    Assert(TransactionIdIsValid(recycle_xid) && (zone->get_recycle_xid_exrto() < recycle_xid));
    zone->LockUndoZone();
    if (!zone->CheckRecycle(start_undo_ptr, end_undo_ptr, true)) {
        ereport(PANIC, (errmodule(MOD_UNDO),
            errmsg(UNDOFORMAT("zone %d recycle start %lu >= recycle end %lu."),
            zone->GetZoneId(), start_undo_ptr, end_undo_ptr)));
    }
    if (IS_VALID_UNDO_REC_PTR(oldest_end_undo_ptr)) {
        int start_zid = UNDO_PTR_GET_ZONE_ID(start_undo_ptr);
        int end_zid = UNDO_PTR_GET_ZONE_ID(oldest_end_undo_ptr);
        if (unlikely(start_zid != end_zid)) {
            oldest_end_undo_ptr = MAKE_UNDO_PTR(start_zid, UNDO_LOG_MAX_SIZE);
        }
        zone->set_discard_urec_ptr_exrto(oldest_end_undo_ptr);
    }

    ereport(DEBUG1, (errmodule(MOD_STANDBY_READ),
                     errmsg("exrto_standby_release_space: zone %d recycle_xid %lu recycle start "
                            "%lu recycle end %lu recycle_tslot %lu.",
                            zone->GetZoneId(), recycle_xid, start_undo_ptr, end_undo_ptr, recycle_exrto)));
    zone->set_recycle_xid_exrto(recycle_xid);
    zone->set_force_discard_urec_ptr_exrto(end_undo_ptr);
    zone->set_recycle_tslot_ptr_exrto(recycle_exrto);
    zone->UnlockUndoZone();
    zone->ReleaseSpace(start_undo_ptr, end_undo_ptr, &g_forceRecycleSize);
    zone->ReleaseSlotSpace(0, recycle_exrto, &g_forceRecycleSize);
}

bool is_undo_slot_exist(UndoSlotPtr slot_ptr)
{
    bool ret = false;
    RelFileNode rnode;
    UNDO_PTR_ASSIGN_REL_FILE_NODE(rnode, slot_ptr, UNDO_SLOT_DB_OID);
    SMgrRelation reln = smgropen(rnode, InvalidBackendId);
    if (smgrexists(reln, UNDO_FORKNUM, (BlockNumber)UNDO_PTR_GET_BLOCK_NUM(slot_ptr))) {
        ret = true;
    }
    smgrclose(reln);
    return ret;
}

bool exrto_standby_recycle_space(UndoZone *zone, TransactionId recycle_xmin)
{
    UndoSlotPtr recycle_exrto = zone->get_recycle_tslot_ptr_exrto();
    UndoSlotPtr recycle_primary = zone->GetRecycleTSlotPtr();
    undo::TransactionSlot *slot = NULL;
    UndoRecPtr end_undo_ptr = INVALID_UNDO_REC_PTR;
    TransactionId recycle_xid = InvalidTransactionId;
    bool undo_recycled = false;
    bool result = false;
    UndoSlotPtr start = INVALID_UNDO_SLOT_PTR;
    ereport(DEBUG1, (errmodule(MOD_UNDO),
        errmsg(UNDOFORMAT("exrto_standby_recycle_space zone_id:%d, recycle_xmin:%lu, recycle_exrto:%lu, "
            "recycle_primary:%lu."),
            zone->GetZoneId(), recycle_xmin, recycle_exrto, recycle_primary)));
 
    while (recycle_exrto < recycle_primary) {
        uint64 start_segno = (uint)((UNDO_PTR_GET_OFFSET(recycle_exrto)) / UNDO_META_SEGMENT_SIZE);
        uint64 end_segno = (uint)((UNDO_PTR_GET_OFFSET(recycle_exrto)) / UNDO_META_SEGMENT_SIZE + 1);
        if (!is_undo_slot_exist(recycle_exrto)) {
            zone->ForgetUndoBuffer(
                start_segno * UNDO_META_SEGMENT_SIZE, end_segno * UNDO_META_SEGMENT_SIZE, UNDO_DB_OID);
            ereport(WARNING,
                (errmodule(MOD_UNDO),
                    errmsg(UNDOFORMAT("exrto_standby_recycle_space zone_id:%d, recycle_xmin:%lu, recycle_exrto:%lu, "
                                      "recycle_primary:%lu, undo slot not exist."),
                        zone->GetZoneId(),
                        recycle_xmin,
                        recycle_exrto,
                        recycle_primary)));
            recycle_exrto = GetNextSlotPtr(recycle_exrto);
            continue;
        }

        UndoSlotBuffer& slot_buf = g_slotBufferCache->FetchTransactionBuffer(recycle_exrto);
        UndoRecPtr start_undo_ptr = INVALID_UNDO_REC_PTR;
        start = recycle_exrto;
        slot_buf.PrepareTransactionSlot(recycle_exrto);
        undo_recycled = false;
        Assert(slot_buf.BufBlock() == UNDO_PTR_GET_BLOCK_NUM(recycle_exrto));
        while (slot_buf.BufBlock() == UNDO_PTR_GET_BLOCK_NUM(recycle_exrto) && (recycle_exrto < recycle_primary)) {
            slot = slot_buf.FetchTransactionSlot(recycle_exrto);
            if (!TransactionIdIsValid(slot->XactId())) {
                break;
            }
            if (slot->StartUndoPtr() == INVALID_UNDO_REC_PTR) {
                break;
            }
 
            if (TransactionIdFollowsOrEquals(slot->XactId(), recycle_xmin)) {
                break;
            }
            ereport(DEBUG1, (errmodule(MOD_UNDO),
                errmsg(UNDOFORMAT("recycle zone %d, exrto transaction slot %lu xid %lu start ptr %lu end ptr %lu."),
                    zone->GetZoneId(), recycle_exrto, slot->XactId(),
                    slot->StartUndoPtr(), slot->EndUndoPtr())));
            if (!start_undo_ptr) {
                start_undo_ptr = slot->StartUndoPtr();
            }
            end_undo_ptr = slot->EndUndoPtr();
            recycle_xid = slot->XactId();
            undo_recycled = true;
            recycle_exrto = GetNextSlotPtr(recycle_exrto);
            /* if next recycle_exrto is in different slot_buf, release current slot_buf. */
            if (slot_buf.BufBlock() != UNDO_PTR_GET_BLOCK_NUM(recycle_exrto)) {
                g_slotBufferCache->RemoveSlotBuffer(start);
                slot_buf.Release();
            }
        }
        if (undo_recycled) {
            exrto_standby_release_space(zone, recycle_xid, start_undo_ptr, end_undo_ptr, recycle_exrto);
            result = true;
        } else {
            /* zone has nothing to recycle. */
            break;
        }
    }
    return result;
}
 
bool exrto_standby_recycle_undo_zone()
{
    uint32 idx = 0;
    bool recycled = false;
    if (g_instance.undo_cxt.uZoneCount == 0 || g_instance.undo_cxt.uZones == NULL) {
        return recycled;
    }
    TransactionId recycle_xmin = exrto_calculate_recycle_xmin_for_undo();
    for (idx = 0; idx < PERSIST_ZONE_COUNT && !t_thrd.undorecycler_cxt.shutdown_requested; idx++) {
        UndoZone *zone = (UndoZone *)g_instance.undo_cxt.uZones[idx];
        if (zone == NULL) {
            continue;
        }
        if (zone->Used_exrto()) {
            if (exrto_standby_recycle_space(zone, recycle_xmin)) {
                recycled = true;
            }
        }
    }
    smgrcloseall();
    return recycled;
}
 
/* recycle residual_undo_file which may be leftover by exrto read in standby */
void exrto_recycle_residual_undo_file(char *FuncName)
{
    uint32 idx = 0;
    uint64 record_file_cnt = 0;
    uint64 slot_file_cnt = 0;
    (void)LWLockAcquire(ExrtoRecycleResidualUndoLock, LW_EXCLUSIVE);
    if (g_instance.undo_cxt.is_exrto_residual_undo_file_recycled) {
        LWLockRelease(ExrtoRecycleResidualUndoLock);
        ereport(LOG, (errmodule(MOD_UNDO),
            errmsg(UNDOFORMAT("exrto_recycle_residual_undo_file skip, FuncName:%s."), FuncName)));
        return;
    }
    g_instance.undo_cxt.is_exrto_residual_undo_file_recycled = true;
    LWLockRelease(ExrtoRecycleResidualUndoLock);
    ereport(LOG, (errmodule(MOD_UNDO),
        errmsg(UNDOFORMAT("exrto_recycle_residual_undo_file begin uZoneCount is %u, FuncName:%s."),
            g_instance.undo_cxt.uZoneCount, FuncName)));
    if (g_instance.undo_cxt.uZoneCount == 0 || g_instance.undo_cxt.uZones == NULL) {
        g_instance.undo_cxt.is_exrto_residual_undo_file_recycled = true;
        ereport(LOG, (errmodule(MOD_UNDO),
            errmsg(UNDOFORMAT("exrto_recycle_residual_undo_file uZoneCount is zero or uZones is null."))));
        return;
    }
    for (idx = 0; idx < PERSIST_ZONE_COUNT && !t_thrd.undorecycler_cxt.shutdown_requested; idx++) {
        UndoZone *zone = (UndoZone *)g_instance.undo_cxt.uZones[idx];
        if (zone == NULL) {
            continue;
        }
        record_file_cnt += zone->release_residual_record_space();
        slot_file_cnt += zone->release_residual_slot_space();
    }
    smgrcloseall();
    ereport(LOG, (errmodule(MOD_UNDO),
        errmsg(UNDOFORMAT("exrto_recycle_residual_undo_file release record_file_cnt:%lu, "
            "slot_file_cnt:%lu."), record_file_cnt, slot_file_cnt)));
}
 
void recycle_wait(bool recycled, uint64 *non_recycled)
{
    if (!recycled) {
        *non_recycled += UNDO_RECYCLE_TIMEOUT_DELTA;
        WaitRecycleThread(*non_recycled);
    } else {
        *non_recycled = 0;
    }
}

void UndoRecycleMain() 
{
    sigjmp_buf localSigjmpBuf;
    bool recycled = false;
    TransactionId oldestXidHavingUndo = InvalidTransactionId;
    TransactionId oldestFrozenXidInUndo = InvalidTransactionId;
    uint64 nonRecycled = 50;
    TransactionId *recycleMaxXIDs = NULL;
    
    t_thrd.utils_cxt.CurrentResourceOwner = ResourceOwnerCreate(NULL, "undo recycler",
        THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));
    MemoryContext undoRecycleContext = AllocSetContextCreate(t_thrd.top_mem_cxt,
        "Undo Recycler",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_UNDO_RECYCLE_MAXSIZE);
    (void)MemoryContextSwitchTo(undoRecycleContext);
    
    ereport(LOG, (errmodule(MOD_UNDO), errmsg(UNDOFORMAT("undo recycle started."))));
    /*
     * Properly accept or ignore signals the postmaster might send us.
     */
    (void)gspqsignal(SIGHUP, UndoRecycleSigHupHandler);
    (void)gspqsignal(SIGINT, SIG_IGN);  /* request shutdown */
    (void)gspqsignal(SIGTERM, UndoShutdownHandler); /* request shutdown */
    (void)gspqsignal(SIGQUIT, UndoQuickdie);       /* hard crash time */
    (void)gspqsignal(SIGALRM, SIG_IGN);
    (void)gspqsignal(SIGPIPE, SIG_IGN);
    (void)gspqsignal(SIGUSR1, SIG_IGN);
    (void)gspqsignal(SIGUSR2, SIG_IGN);
    (void)gspqsignal(SIGURG, print_stack);
    /*
     * Reset some signals that are accepted by postmaster but not here
     */

    (void)gspqsignal(SIGCHLD, SIG_DFL);
    (void)gspqsignal(SIGTTIN, SIG_DFL);
    (void)gspqsignal(SIGTTOU, SIG_DFL);
    (void)gspqsignal(SIGCONT, SIG_DFL);
    (void)gspqsignal(SIGWINCH, SIG_DFL);

    /* Unblock signals (they were blocked when the postmaster forked us) */
    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();
    /*
     * If an exception is encountered, processing resumes here.
     *
     * This code is a stripped down version of PostgresMain error recovery.
     */
    if (sigsetjmp(localSigjmpBuf, 1) != 0) {
        smgrcloseall();
        /* Since not using PG_TRY, must reset error stack by hand */
        t_thrd.log_cxt.error_context_stack = NULL;
        /* Prevent interrupts while cleaning up */
        HOLD_INTERRUPTS();
        /* Report the error to the server log */
        EmitErrorReport();
        /* release resource held by lsc */
        AtEOXact_SysDBCache(false);
        LWLockReleaseAll();
        pgstat_report_waitevent(WAIT_EVENT_END);
        AbortBufferIO();
        UnlockBuffers();
        /* buffer pins are released here: */
        ResourceOwnerRelease(t_thrd.utils_cxt.CurrentResourceOwner, RESOURCE_RELEASE_BEFORE_LOCKS, false, true);
        /* we needn't bother with the other ResourceOwnerRelease phases */
        AtEOXact_Buffers(false);
        AtEOXact_SMgr();
        /*
         * Now return to normal top-level context and clear ErrorContext for
         * next time.
         */
        (void)MemoryContextSwitchTo(undoRecycleContext);
        FlushErrorState();
        /* Flush any leaked data in the top-level context */
        MemoryContextResetAndDeleteChildren(undoRecycleContext);
        /* Now we can allow interrupts again */
        RESUME_INTERRUPTS();
        /*
         * Sleep at least 1 second after any error.  A write error is likely
         * to be repeated, and we don't want to be filling the error logs as
         * fast as we can.
         */
        pg_usleep(1000000L);
    }

    /* We can now handle ereport(ERROR) */
    t_thrd.log_cxt.PG_exception_stack = &localSigjmpBuf;
    /*
     * Unblock signals (they were blocked when the postmaster forked us)
     */
    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();

    pgstat_report_appname("undo recycler");
    pgstat_report_activity(STATE_IDLE, NULL);

    RecycleWaitIfNotUsed();
    recycleMaxXIDs = (TransactionId*)palloc0(sizeof(TransactionId) * UNDO_ZONE_COUNT);
    if (recycleMaxXIDs == NULL) {
        ereport(PANIC, (errmsg(UNDOFORMAT("undo cannot alloc xids memory."))));
    }
    g_slotBufferCache = New(undoRecycleContext) UndoSlotBufferCache(undoRecycleContext, SLOT_BUFFER_CACHE_SIZE);
    if (g_slotBufferCache == NULL) {
        ereport(PANIC, (errmsg(UNDOFORMAT("undo cannot alloc buffer cache memory."))));
    }
    if (!TransactionIdIsValid(g_instance.undo_cxt.globalFrozenXid)) {
        g_instance.undo_cxt.globalFrozenXid = pg_atomic_read_u64(&g_instance.undo_cxt.globalRecycleXid);
    }
    /* sleep 10s, ensure that the snapcapturer thread can give the undoRecycleMain thread a valid recycleXmin */
    pg_usleep(10000000L);
    ereport(LOG, (errmodule(MOD_UNDO),
        errmsg(UNDOFORMAT("sleep 10s, ensure  the snapcapturer can give the undorecyclemain a valid recycleXmin."))));
    exrto_recycle_residual_undo_file("recycle_main");
    t_thrd.undorecycler_cxt.is_recovery_in_progress = RecoveryInProgress();
    while (true) {
        if (t_thrd.undorecycler_cxt.got_SIGHUP) {
            t_thrd.undorecycler_cxt.got_SIGHUP = false;
            ProcessConfigFile(PGC_SIGHUP);
        }
        if (t_thrd.undorecycler_cxt.shutdown_requested) {
            ShutDownRecycle(recycleMaxXIDs);
        }
        bool is_in_progress = RecoveryInProgress();
        if (is_in_progress != t_thrd.undorecycler_cxt.is_recovery_in_progress) {
            ereport(LOG, (errmodule(MOD_UNDO),
                errmsg(UNDOFORMAT("recycle_main: stop undo recycler because recovery_in_progress change "
                    "from %u to %u."), t_thrd.undorecycler_cxt.is_recovery_in_progress, is_in_progress)));
            ShutDownRecycle(recycleMaxXIDs);
        }
        if (!t_thrd.undorecycler_cxt.is_recovery_in_progress) {
            TransactionId recycleXmin = InvalidTransactionId;
            TransactionId oldestXmin = GetOldestXminForUndo(&recycleXmin);
            if (!TransactionIdIsValid(recycleXmin) ||
                TransactionIdPrecedes(oldestXmin, g_instance.undo_cxt.globalFrozenXid)) {
                WaitRecycleThread(nonRecycled);
                continue;
            }
            recycled = false;
            oldestXidHavingUndo = InvalidTransactionId;
            oldestFrozenXidInUndo = oldestXmin;
            uint32 idx = 0;
            uint32 retry = 1;
            TransactionId recycleXid;
            TransactionId forceRecycleXid = InvalidTransactionId;
            uint32 recycleMaxXIDCount = 0;
            errno_t ret = 0;
            bool isAnyZoneUsed = false;
            if (g_instance.undo_cxt.uZoneCount != 0) {
                if (NeedForceRecycle()) {
                    forceRecycleXid = GetForceRecycleXid(recycleXmin, retry);
                }
                retry:
                ret = memset_s(recycleMaxXIDs, sizeof(TransactionId) * UNDO_ZONE_COUNT, 0,
                    sizeof(TransactionId) * UNDO_ZONE_COUNT);
                securec_check(ret, "\0", "\0");
                recycleMaxXIDCount = 0;
                isAnyZoneUsed = false;
                for (idx = 0; idx < UNDO_ZONE_COUNT && !t_thrd.undorecycler_cxt.shutdown_requested; idx++) {
                    UndoZone *zone = UndoZoneGroup::GetUndoZone(idx, false);
                    TransactionId frozenXid = oldestXmin;
                    if (zone == NULL) {
                        continue;
                    }
                    recycleXid = InvalidTransactionId;
                    if (zone->Used()) {
                        UndoSlotPtr oldestFrozenSlotPtr = zone->GetFrozenSlotPtr();
                        if (!TransactionIdIsValid(zone->GetFrozenXid())) {
                            zone->SetFrozenXid(pg_atomic_read_u64(&g_instance.undo_cxt.globalFrozenXid));
                        }
                        AdvanceFrozenXid(zone, &frozenXid, oldestXmin, &oldestFrozenSlotPtr);
                        zone->SetFrozenSlotPtr(oldestFrozenSlotPtr);
                        ereport(DEBUG1, (errmodule(MOD_UNDO), errmsg(
                            UNDOFORMAT("oldestFrozenXidInUndo: oldestFrozenXidInUndo=%lu, frozenXid=%lu"
                            " before RecycleUndoSpace"),
                            oldestFrozenXidInUndo, frozenXid)));
                        if (RecycleUndoSpace(zone, recycleXmin, frozenXid, &recycleXid, forceRecycleXid)) {
                            recycled = true;
                        }
                        isAnyZoneUsed = true;
                        ereport(DEBUG1, (errmodule(MOD_UNDO), errmsg(
                            UNDOFORMAT("oldestFrozenXidInUndo: oldestFrozenXidInUndo=%lu, frozenXid=%lu"
                            " after RecycleUndoSpace"),
                            oldestFrozenXidInUndo, frozenXid)));
                        oldestFrozenXidInUndo = oldestFrozenXidInUndo > frozenXid ? frozenXid : oldestFrozenXidInUndo;
                        UpdateRecyledXid(recycleMaxXIDs, &recycleMaxXIDCount, recycleXid);
                    }
                }
            }
            smgrcloseall();
            if (isAnyZoneUsed) {
                VerifyFrozenXidAdvance(oldestXmin, oldestFrozenXidInUndo, USTORE_VERIFY_FAST);
                ereport(DEBUG1, (errmodule(MOD_UNDO), errmsg(
                    UNDOFORMAT("oldestFrozenXidInUndo for update: oldestFrozenXidInUndo=%lu"),
                    oldestFrozenXidInUndo)));
                pg_atomic_write_u64(&g_instance.undo_cxt.globalFrozenXid, oldestFrozenXidInUndo);
            }
            if (t_thrd.undorecycler_cxt.shutdown_requested) {
                ShutDownRecycle(recycleMaxXIDs);
            }
            if (g_instance.undo_cxt.uZoneCount != 0) {
                UpdateXidHavingUndo(recycleMaxXIDs, recycleMaxXIDCount, &oldestXidHavingUndo, recycleXmin);
                if (g_forceRecycleSize > 0) {
                    retry++;
                    forceRecycleXid = GetForceRecycleXid(recycleXmin, retry);
                    if (TransactionIdIsValid(forceRecycleXid)) {
                        ereport(DEBUG1, (errmodule(MOD_UNDO), errmsg(
                            UNDOFORMAT("ForceRecycle: recycleXid=%lu, oldestXmin=%lu, forceRecycleSize=%d, retry=%u."),
                            forceRecycleXid, recycleXmin, g_forceRecycleSize, retry)));
                        goto retry;
                    }
                }
                ereport(DEBUG1, (errmodule(MOD_UNDO),
                    errmsg(UNDOFORMAT("update globalRecycleXid = %lu."), oldestXidHavingUndo)));
                g_recycleLoops++;
            }
            if (oldestXidHavingUndo != InvalidTransactionId) {
                TransactionId globalRecycleXid = pg_atomic_read_u64(&g_instance.undo_cxt.globalRecycleXid);
                if (TransactionIdPrecedes(oldestXidHavingUndo, globalRecycleXid)) {
                    ereport(WARNING, (errmsg(UNDOFORMAT("curr xid having undo %lu < global globalRecycleXid %lu."),
                        oldestXidHavingUndo, globalRecycleXid)));
                }
                if (TransactionIdPrecedes(recycleXmin, oldestXidHavingUndo)) {
                    oldestXidHavingUndo = recycleXmin;
                }
                VerifyRecycleXidAdvance(oldestFrozenXidInUndo, oldestXidHavingUndo, USTORE_VERIFY_FAST);
                if (TransactionIdFollows(oldestXidHavingUndo, globalRecycleXid)) {
                    ereport(LOG, (errmodule(MOD_UNDO), errmsg(
                        UNDOFORMAT("update globalRecycleXid: oldestXmin=%lu, recycleXmin=%lu, "
                        "globalFrozenXid=%lu, globalRecycleXid=%lu, newRecycleXid=%lu."),
                        oldestXmin, recycleXmin, g_instance.undo_cxt.globalFrozenXid,
                        globalRecycleXid, oldestXidHavingUndo)));
                    pg_atomic_write_u64(&g_instance.undo_cxt.globalRecycleXid, oldestXidHavingUndo);
                }
            }
        } else if (IS_EXRTO_STANDBY_READ) {
            recycled = exrto_standby_recycle_undo_zone();
        }
        recycle_wait(recycled, &nonRecycled);
    }
    ShutDownRecycle(recycleMaxXIDs);
}
} // namespace undo
