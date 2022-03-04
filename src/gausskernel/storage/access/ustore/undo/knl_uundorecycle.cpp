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
        UndoRecPtr prev = GetPrevUrp(slot->EndUndoPtr());
        AddRollbackRequest(slot->XactId(), prev, slot->StartUndoPtr(),
            slot->DbId(), recycle);
#ifdef DEBUG_UHEAP
        UHEAPSTAT_COUNT_UNDO_SPACE_UNRECYCLE();
#endif
        ereport(LOG, (errmodule(MOD_UNDO),
            errmsg(UNDOFORMAT(
                "rollback zone %d slot %lu: xid %lu, start %lu, "
                "end %lu, dbid %u. recyclexid %lu loops %lu"),
                zone->GetZoneId(), recycle, slot->XactId(), slot->StartUndoPtr(), slot->EndUndoPtr(),
                slot->DbId(), slot->XactId(), g_recycleLoops)));
        return true;
    }
    return false;
}

void AdvanceFrozenXid(UndoZone *zone, TransactionId *oldestFozenXid,
    TransactionId oldestXmin, UndoSlotPtr *oldestFrozenSlotPtr)
{
    TransactionSlot *slot = NULL;
    UndoSlotPtr frozenSlotPtr = zone->GetFrozenSlotPtr();
    UndoSlotPtr recycle = zone->GetRecycle();
    UndoSlotPtr allocate = zone->GetAllocate();
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

            if (slot->StartUndoPtr() == INVALID_UNDO_REC_PTR) {
                ereport(LOG, (errmodule(MOD_UNDO),
                    errmsg(UNDOFORMAT(
                        "invalid slotptr zone %d transaction slot %lu loops %lu."),
                        zone->GetZoneId(), currentSlotPtr, g_recycleLoops)));
                finishAdvanceXid = true;
                break;
            }
#ifdef DEBUG_UHEAP
            UHEAPSTAT_COUNT_UNDO_SPACE_RECYCLE();
#endif
            pg_read_barrier();
            if (!TransactionIdIsValid(slot->XactId())) {
                ereport(PANIC, (errmodule(MOD_UNDO),
                    errmsg(UNDOFORMAT(
                        "recycle xid invalid: zone %d transaction slot %lu loops %lu."),
                        zone->GetZoneId(), currentSlotPtr, g_recycleLoops)));
            }
            ereport(DEBUG1, (errmodule(MOD_UNDO),
                errmsg(UNDOFORMAT(
                    "zone id %d, currentSlotPtr %lu. slot xactid %lu, oldestXmin %lu"),
                    zone->GetZoneId(), currentSlotPtr, slot->XactId(), oldestXmin)));
            *oldestFrozenSlotPtr = currentSlotPtr;
            if (!UHeapTransactionIdDidCommit(slot->XactId()) &&
                TransactionIdPrecedes(slot->XactId(), oldestXmin)) {
                if (AsyncRollback(zone, currentSlotPtr, slot)) {
                    *oldestFozenXid = slot->XactId();
                    ereport(LOG, (errmodule(MOD_UNDO),
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
    UndoSlotPtr recycle = zone->GetRecycle();
    UndoSlotPtr allocate = zone->GetAllocate();
    TransactionSlot *slot = NULL;
    UndoRecPtr endUndoPtr = INVALID_UNDO_REC_PTR;
    UndoRecPtr oldestEndUndoPtr = INVALID_UNDO_REC_PTR;
    bool forceRecycle = false;
    bool needWal = false;
    TransactionId recycleXid = InvalidTransactionId;
    bool undoRecycled = false;
    bool result = false;
    UndoSlotPtr start = INVALID_UNDO_SLOT_PTR;
    TransactionId oldestXidInUndo = pg_atomic_read_u64(&g_instance.undo_cxt.oldestXidInUndo);
    
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

            if (slot->StartUndoPtr() == INVALID_UNDO_REC_PTR) {
                break;
            }
#ifdef DEBUG_UHEAP
            UHEAPSTAT_COUNT_UNDO_SPACE_RECYCLE();
#endif
            pg_read_barrier();
            if (!TransactionIdIsValid(slot->XactId()) || TransactionIdPrecedes(slot->XactId(), oldestXidInUndo)) {
                ereport(PANIC, (errmodule(MOD_UNDO),
                    errmsg(UNDOFORMAT(
                        "recycle xid invalid: zone %d transaction slot %lu, slot XactId %lu, "
                        "xid %lu loops %lu, oldestXidInUndo %lu."),
                        zone->GetZoneId(), recycle, slot->XactId(), recycleXid, g_recycleLoops, oldestXidInUndo)));
            }
            if (TransactionIdPrecedes(slot->XactId(), recycleXmin)) {
                Assert(forceRecycle == false);

#ifdef ENABLE_WHITEBOX
                if (TransactionIdPrecedes(slot->XactId(),frozenXid)) {
                    if (!UHeapTransactionIdDidCommit(slot->XactId()) && slot->NeedRollback()) {
                        ereport(PANIC, (errmodule(MOD_UNDO),
                        errmsg(UNDOFORMAT(
                            "Recycle visibility check wrong: zone %d "
                            "transaction slot %lu xid %lu slot->XactId() %lu, oldestXidInUndo %lu."),
                            zone->GetZoneId(), recycle, recycleXid, slot->XactId(), oldestXidInUndo)));
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
            Assert(TransactionIdIsValid(recycleXid));
            if (zone->GetRecycleXid() >= recycleXid) {
                ereport(PANIC,
                    (errmodule(MOD_UNDO), errmsg(UNDOFORMAT("zone %d forcerecycle xid %lu >= recycle xid %lu."),
                        zone->GetZoneId(), zone->GetRecycleXid(), recycleXid)));
            }
            zone->LockUndoZone();
            if (!zone->CheckRecycle(startUndoPtr, endUndoPtr)) {
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
                zone->SetDiscard(oldestEndUndoPtr);
            }
            zone->SetRecycleXid(recycleXid);
            zone->SetForceDiscard(endUndoPtr);
            if (zone->GetForceDiscard() > zone->GetInsert()) {
                ereport(WARNING, (errmodule(MOD_UNDO), errmsg(UNDOFORMAT("zone %d discard %lu > insert %lu."),
                    zone->GetZoneId(), zone->GetForceDiscard(), zone->GetInsert())));
            }

            START_CRIT_SECTION();
            zone->SetRecycle(recycle);
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
        ereport(LOG, (errmodule(MOD_UNDO),
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

void UndoRecycleMain() 
{
    sigjmp_buf localSigjmpBuf;
    bool recycled = false;
    bool isFirstRecycle = false;
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

    while (true) {
        if (t_thrd.undorecycler_cxt.got_SIGHUP) {
            t_thrd.undorecycler_cxt.got_SIGHUP = false;
            ProcessConfigFile(PGC_SIGHUP);
        }
        if (!RecoveryInProgress()) {
            TransactionId recycleXmin = InvalidTransactionId;
            TransactionId oldestXmin = GetOldestXminForUndo(&recycleXmin);
            isFirstRecycle = !TransactionIdIsValid(g_instance.undo_cxt.oldestFrozenXid);
            if (!TransactionIdIsValid(recycleXmin) ||
            TransactionIdPrecedes(oldestXmin, g_instance.undo_cxt.oldestFrozenXid)) {
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
                    UndoPersistence upersistence = UNDO_PERMANENT;
                    GET_UPERSISTENCE(idx, upersistence);
                    UndoZone *zone = UndoZoneGroup::GetUndoZone(idx, false, upersistence);
                    TransactionId frozenXid = oldestXmin;
                    if (zone == NULL) {
                        continue;
                    }
                    recycleXid = InvalidTransactionId;
                    if (zone->Used()) {
                        UndoSlotPtr oldestFrozenSlotPtr = zone->GetFrozenSlotPtr();
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
            if (isAnyZoneUsed) {
                ereport(LOG, (errmodule(MOD_UNDO), errmsg(
                    UNDOFORMAT("oldestFrozenXidInUndo for update: oldestFrozenXidInUndo=%lu"),
                    oldestFrozenXidInUndo)));
                pg_atomic_write_u64(&g_instance.undo_cxt.oldestFrozenXid, oldestFrozenXidInUndo);
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
                    errmsg(UNDOFORMAT("update oldestXidInUndo = %lu."), oldestXidHavingUndo)));
                g_recycleLoops++;
            }
            if (oldestXidHavingUndo != InvalidTransactionId && !isFirstRecycle) {
                TransactionId oldestXidInUndo = pg_atomic_read_u64(&g_instance.undo_cxt.oldestXidInUndo);
                if (TransactionIdPrecedes(oldestXidHavingUndo, oldestXidInUndo)) {
                    ereport(PANIC, (errmsg(UNDOFORMAT("curr xid having undo %lu < global oldestXidInUndo %lu."), 
                        oldestXidHavingUndo, oldestXidInUndo)));
                }
                pg_atomic_write_u64(&g_instance.undo_cxt.oldestXidInUndo, oldestXidHavingUndo);
            }
            if (!recycled) {
                nonRecycled += UNDO_RECYCLE_TIMEOUT_DELTA;
                WaitRecycleThread(nonRecycled);
            } else {
                nonRecycled = 0;
            }
        } else {
            WaitRecycleThread(nonRecycled);
        }
    }
    ShutDownRecycle(recycleMaxXIDs);
}
} // namespace undo
