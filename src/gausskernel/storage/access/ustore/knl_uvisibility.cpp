/* -------------------------------------------------------------------------
 *
 * knl_uvisibility.cpp
 * Tuple visibility interfaces of inplace update engine.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 * src/gausskernel/storage/access/ustore/knl_uvisibility.cpp
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "pgstat.h"
#include "nodes/relation.h"
#include "utils/datum.h"
#include "utils/snapmgr.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/predicate.h"
#include "storage/lmgr.h"
#include "access/xact.h"
#include "access/transam.h"
#include "access/ustore/knl_uheap.h"
#include "access/ustore/knl_utuple.h"
#include "access/ustore/knl_uhio.h"
#include "access/ustore/knl_utils.h"
#include "access/ustore/knl_uverify.h"
#include "access/ustore/knl_uvisibility.h"
#include "access/ustore/undo/knl_uundoapi.h"
#include "access/ustore/knl_whitebox_test.h"

static UTupleTidOp UHeapTidOpFromInfomask(uint16 infomask);
static UVersionSelector UHeapTupleSatisfies(UTupleTidOp op, Snapshot snapshot,
    UHeapTupleTransInfo *uinfo, int *snapshot_requests, Buffer buffer);
static UVersionSelector UHeapSelectVersionMVCC(UTupleTidOp op, TransactionId xid, Snapshot snapshot, Buffer buffer);
static UVersionSelector UHeapSelectVersionNow(UTupleTidOp op, TransactionId xid);
static UVersionSelector UHeapCheckCID(UTupleTidOp op, CommandId tuple_cid, CommandId visibility_cid,
    bool* has_cur_xact_write = NULL);
static UVersionSelector UHeapSelectVersionSelf(UTupleTidOp op, TransactionId xid);
static UVersionSelector UHeapSelectVersionDirty(UTupleTidOp op, const UHeapTupleTransInfo *uinfo,
    Snapshot snapshot, int *snapshotRequests);
static UVersionSelector UHeapSelectVersionUpdate(UTupleTidOp op, TransactionId xid, CommandId visibility_cid);

static UndoTraversalState GetTupleFromUndoRecord(UndoRecPtr urecPtr, TransactionId xid, Buffer buffer,
    OffsetNumber offnum, UHeapDiskTuple hdr, UHeapTuple *tuple, bool *freeTuple,
    UHeapTupleTransInfo *uinfo, ItemPointer ctid, TransactionId *lastXid, UndoRecPtr *urp);
static bool GetTupleFromUndo(UndoRecPtr urecAdd, UHeapTuple currentTuple, UHeapTuple *visibleTuple, Snapshot snapshot,
    CommandId curcid, Buffer buffer, OffsetNumber offnum, ItemPointer ctid, int tdSlot);

static void UHeapPageGetNewCtid(Buffer buffer, Oid reloid, ItemPointer ctid, UHeapTupleTransInfo *xactinfo,
    Snapshot snapshot);

typedef enum {
    UHEAPTUPLESTATUS_LOCKED,
    UHEAPTUPLESTATUS_MULTI_LOCKED,
    UHEAPTUPLESTATUS_INPLACE_UPDATED,
    UHEAPTUPLESTATUS_DELETED,
    UHEAPTUPLESTATUS_INSERTED
} UHeapTupleStatus;

static UHeapTupleStatus UHeapTupleGetStatus(const UHeapTuple utup)
{
    UHeapDiskTuple utuple = utup->disk_tuple;
    uint16 infomask = utuple->flag;
    TransactionId locker = UHeapTupleGetRawXid(utup);
    Assert(utuple->reserved == 0);
    if (UHeapTupleHasMultiLockers(infomask)) {
        return UHEAPTUPLESTATUS_MULTI_LOCKED;
    } else if ((SINGLE_LOCKER_XID_IS_EXCL_LOCKED(infomask) || SINGLE_LOCKER_XID_IS_SHR_LOCKED(infomask)) &&
        TransactionIdIsNormal(locker) && !TransactionIdOlderThanFrozenXid(locker)) {
        Assert(!UHEAP_XID_IS_TRANS(utuple->flag));
        return UHEAPTUPLESTATUS_LOCKED; // locked by select-for-update or select-for-share
    } else if (infomask & UHEAP_INPLACE_UPDATED) {
        return UHEAPTUPLESTATUS_INPLACE_UPDATED; // modified or locked by lock-for-update
    } else if ((infomask & (UHEAP_UPDATED | UHEAP_DELETED)) != 0) {
        return UHEAPTUPLESTATUS_DELETED;
    }

    return UHEAPTUPLESTATUS_INSERTED;
}

TransactionId UDiskTupleGetModifiedXid(UHeapDiskTuple diskTup, Page page)
{
    if (TransactionIdIsNormal(diskTup->xid) && UHEAP_XID_IS_TRANS(diskTup->flag) &&
        !UHEAP_XID_IS_LOCK(diskTup->flag)) {
        UHeapPageHeaderData *upage = (UHeapPageHeaderData *)page;
        TransactionId tupXid = ShortTransactionIdToNormal(upage->pd_xid_base, diskTup->xid);
        if (TransactionIdFollows(tupXid, t_thrd.xact_cxt.ShmemVariableCache->nextXid)) {
            /* This code is used to resolve the residual xid problem. */
            diskTup->xid = (ShortTransactionId)FrozenTransactionId;
            tupXid = InvalidTransactionId;
        }
        return tupXid;
    }
    return InvalidTransactionId;
}

/*
 * UHeapTupleSatisfies
 *
 * Determine whether (a) the current version of the tuple is visible to the
 * snapshot, (b) no version of the tuple is visible to the snapshot, or
 * (c) the previous version of the tuple should be looked up into the undo
 * log to determine which version, if any, is visible.
 *
 * This function can only handle certain types of snapshots; it is a helper
 * function for UHeapTupleFetch, not a general-purpose facility.
 */
static UVersionSelector UHeapTupleSatisfies(UTupleTidOp op, Snapshot snapshot,
    UHeapTupleTransInfo *uinfo, int *snapshot_requests, Buffer buffer)
{
    UVersionSelector selector = UVERSION_NONE;

    /* Attempt to make a visibility determination. */
    if (uinfo->td_slot == UHEAPTUP_SLOT_FROZEN) {
        /*
         * The tuple is not associated with a transaction slot that is new
         * enough to matter, so all changes previously made to the tuple are
         * now all-visible.  If the last operation performed was a delete or a
         * non-inplace update, the tuple is now effectively gone; if it was an
         * insert or an inplace update, use the current version.
         */
        selector = (op == UTUPLETID_GONE) ? UVERSION_NONE : UVERSION_CURRENT;
    } else if (snapshot->satisfies == SNAPSHOT_MVCC || IsVersionMVCCSnapshot(snapshot)) {
        /*
         * NOTICE: We distinguish SNAPSHOT_VERSION_MVCC and SNAPSHOT_MVCC
         * in UHeapSelectVersionMVCC codes.
         */
        selector = UHeapSelectVersionMVCC(op, uinfo->xid, snapshot, buffer);
    } else if (snapshot->satisfies == SNAPSHOT_NOW) {
        selector = UHeapSelectVersionNow(op, uinfo->xid);
    } else if (snapshot->satisfies == SNAPSHOT_SELF) {
        selector = UHeapSelectVersionSelf(op, uinfo->xid);
    } else if (snapshot->satisfies == SNAPSHOT_DIRTY) {
        selector = UHeapSelectVersionDirty(op, uinfo, snapshot, snapshot_requests);
    } else if (snapshot->satisfies == SNAPSHOT_TOAST) {
        /* 
         * We can only get here when a toast tuple is pruned and rp is marked as deleted,
         * we need the newest older version in undo
         */
        selector = UVERSION_OLDER;
    } else {
        elog(ERROR, "unsupported snapshot style %d", (int)snapshot->satisfies);
    }

    return selector;
}

static TransactionId UHeapTupleGetModifiedXid(UHeapTuple tup)
{
    if (TransactionIdIsNormal(tup->disk_tuple->xid) && UHEAP_XID_IS_TRANS(tup->disk_tuple->flag) &&
        !UHEAP_XID_IS_LOCK(tup->disk_tuple->flag)) {
        TransactionId tupXid = ShortTransactionIdToNormal(tup->t_xid_base, tup->disk_tuple->xid);
        if (TransactionIdFollows(tupXid, t_thrd.xact_cxt.ShmemVariableCache->nextXid)) {
            /* This code is used to resolve the residual xid problem. */
            tup->disk_tuple->xid = (ShortTransactionId)FrozenTransactionId;
            tupXid = InvalidTransactionId;
        }
        return tupXid;
    }
    return InvalidTransactionId;
}

/*
 * UHeapTupleSatisfiesVisibility
 * True iff uheap tuple satisfies a time qual.
 *
 * Notes: Assumes uheap tuple is valid, and buffer at least share locked.
 *
 */
bool UHeapTupleSatisfiesVisibility(UHeapTuple uhtup, Snapshot snapshot, Buffer buffer, TransactionId *tdXmin)
{
    Assert(uhtup != NULL);
    if (snapshot->satisfies == SNAPSHOT_DIRTY) {
        snapshot->xmin = snapshot->xmax = InvalidTransactionId;
        snapshot->subxid = InvalidSubTransactionId;
    }
    if (snapshot->satisfies == SNAPSHOT_ANY || snapshot->satisfies == SNAPSHOT_TOAST) {
        return true;
    }

    UHeapTupleData utupledata;
    UHeapTuple utuple = &utupledata;
    ItemPointerCopy(&uhtup->ctid, &utuple->ctid);
    UVersionSelector uheapselect = UVERSION_NONE;
    int snapshotRequests = 0;
    UTupleTidOp op;
    TransactionId fxid = GetTopTransactionIdIfAny();
    TransactionId lockerXid = InvalidTransactionId;
    TransactionId tupXid = InvalidTransactionId;
    Page dp = BufferGetPage(buffer);
    bool tupleIsExclusivelyLocked = false;
    UHeapTupleTransInfo tdinfo;
    UHeapTupleTransInfo oldTdInfo;
    int tdSlot;
    bool isInvalidSlot = false;
    bool haveTransInfo = false;
    bool isFlashBack = IsVersionMVCCSnapshot(snapshot) ? true : false;
    UndoTraversalState state = UNDO_TRAVERSAL_DEFAULT;
    OffsetNumber offnum = ItemPointerGetOffsetNumber(&uhtup->ctid);
    if (offnum > UHeapPageGetMaxOffsetNumber(dp)) {
        ereport(PANIC, (errmodule(MOD_USTORE),
                        errmsg("the number of tuples in page is %hu, exceeds the maximum count of page.", offnum)));
    }
    RowPtr *rp = UPageGetRowPtr(dp, offnum);
    BlockNumber blockno = BufferGetBlockNumber(buffer);

    /*
     * First, determine the transaction slot for this tuple and whether the
     * transaction slot has been reused (i.e. is flagged as invalid).  For
     * normal items, this information is stored in the tuple header; for
     * deleted ones, it is stored in the row pointer.
     */
    if (RowPtrIsNormal(rp)) {
        utuple->disk_tuple = (UHeapDiskTuple)UPageGetRowData(dp, rp);
        Assert(utuple->disk_tuple->reserved == 0);
        utuple->disk_tuple_size = RowPtrGetLen(rp);
        tdSlot = UHeapTupleHeaderGetTDSlot(utuple->disk_tuple);
        UHeapTupleCopyBaseFromPage(utuple, dp);
        tupXid = UHeapTupleGetModifiedXid(utuple);
        isInvalidSlot = UHeapTupleHasInvalidXact(utuple->disk_tuple->flag);
    } else if (RowPtrIsDeleted(rp)) {
        utuple = NULL;
        tdSlot = RowPtrGetTDSlot(rp);
        isInvalidSlot = (RowPtrGetVisibilityInfo(rp) & ROWPTR_XACT_INVALID);
    } else {
        /*
         * If this RowPtr is neither normal nor dead, it must be unused.  In
         * that case, there is no version of the tuple visible here, so we can
         * exit quickly.
         */
        Assert(!RowPtrIsUsed(rp));
        return false;
    }
    /* Get the current TD information on the current page */
    GetTDSlotInfo(buffer, tdSlot, &tdinfo);
    oldTdInfo = tdinfo;
    if (tdinfo.td_slot != UHEAPTUP_SLOT_FROZEN) {
        if (utuple != NULL && TransactionIdIsNormal(fxid) && IsMVCCSnapshot(snapshot) &&
            SINGLE_LOCKER_XID_IS_EXCL_LOCKED(utuple->disk_tuple->flag)) {
            Assert(UHEAP_XID_IS_EXCL_LOCKED(utuple->disk_tuple->flag));
            Assert(!UHEAP_XID_IS_TRANS(utuple->disk_tuple->flag));
            lockerXid = UHeapTupleGetRawXid(utuple);
            tupleIsExclusivelyLocked = true;
        }
        uint64 globalFrozenXid = isFlashBack ? pg_atomic_read_u64(&g_instance.undo_cxt.globalRecycleXid) :
        pg_atomic_read_u64(&g_instance.undo_cxt.globalFrozenXid);
        if (RecoveryInProgress()) {
            /* in hot standby mode, if globalRecycleXid advance during query, it may cause data inconsistency */
            globalFrozenXid = pg_atomic_read_u64(&g_instance.undo_cxt.hotStandbyRecycleXid);
        }
        if (TransactionIdIsValid(tdinfo.xid) && TransactionIdPrecedes(tdinfo.xid, globalFrozenXid)) {
            /* The slot is old enough that we can treat it as frozen. */
            tdinfo.td_slot = UHEAPTUP_SLOT_FROZEN;
        } else if (tupleIsExclusivelyLocked && TransactionIdEquals(fxid, lockerXid)) {
            /* If we're able to do a SELECT FOR UPDATE on this tuple then it should be visible */
            tdinfo.td_slot = UHEAPTUP_SLOT_FROZEN;
        } else if (isInvalidSlot) {
            TransactionIdStatus hintstatus;

            /*
             * The slot has been reused, but we can still skip reading the
             * undo if the XID we got from the transaction slot is visible to
             * our snapshot.  The real XID has to have committed before that
             * one, so it will be visible to our snapshot as well.
             */
            if (IsMVCCSnapshot(snapshot) && TransactionIdIsValid(tdinfo.xid) &&
                UHeapXidVisibleInSnapshot(tdinfo.xid, snapshot, &hintstatus,
                (RecoveryInProgress() ? buffer : InvalidBuffer), NULL)) {
                tdinfo.td_slot = UHEAPTUP_SLOT_FROZEN;
            } else if (TransactionIdIsValid(tupXid) && UHeapTransactionIdDidCommit(tupXid)) {
                tdinfo.xid = tupXid;
                if (tdinfo.xid < globalFrozenXid) {
                    tdinfo.td_slot = UHEAPTUP_SLOT_FROZEN;
                }
            } else {
                TransactionId lastXid = InvalidTransactionId;
                UndoRecPtr urp = INVALID_UNDO_REC_PTR;
                /* Fetch the tuple's transaction information from the undo */
                state = FetchTransInfoFromUndo(blockno, ItemPointerGetOffsetNumber(&utuple->ctid),
                    InvalidTransactionId, &tdinfo, NULL, !isFlashBack, &lastXid, &urp);
                if (state == UNDO_TRAVERSAL_COMPLETE) {
                    if (TransactionIdOlderThanAllUndo(tdinfo.xid)) {
                        tdinfo.td_slot = UHEAPTUP_SLOT_FROZEN;
                    }
                    haveTransInfo = true;
                } else if (state == UNDO_TRAVERSAL_STOP || state == UNDO_TRAVERSAL_END ||
                    state == UNDO_TRAVERSAL_ENDCHAIN) {
                    tdinfo.td_slot = UHEAPTUP_SLOT_FROZEN;
                } else if (state == UNDO_TRAVERSAL_ABORT) {
                    if (IsMVCCSnapshot(snapshot) && TransactionIdIsValid(lastXid) &&
                        UHeapXidVisibleInSnapshot(lastXid, snapshot, &hintstatus,
                        (RecoveryInProgress() ? buffer : InvalidBuffer), NULL)) {
                        tdinfo.td_slot = UHEAPTUP_SLOT_FROZEN;
                    } else {
                        int zoneId = (int)UNDO_PTR_GET_ZONE_ID(urp);
                        undo::UndoZone *uzone = undo::UndoZoneGroup::GetUndoZone(zoneId, false);
                        ereport(ERROR, (errmodule(MOD_UNDO), errmsg(
                            "snapshot too old! the undo record has been force discard. "
                            "Reason: Not MVCC snapshot/lastXid Invalid/lastXid invisible to snapshot. "
                            "LogInfo: undo state %d, tuple flag %u, tupXid %lu. "
                            "OldTd: tdxid %lu, tdid %d, undoptr %lu. NewTd: tdxid %lu, tdid %d, undoptr %lu. "
                            "TransInfo: xid %lu, oid %u, tid(%u, %u), lastXid %lu, "
                            "globalRecycleXid %lu, globalFrozenXid %lu. "
                            "ZoneInfo: urp: %lu, zid %d, insertURecPtr %lu, forceDiscardURecPtr %lu, "
                            "discardURecPtr %lu, recycleXid %lu. "
                            "Snapshot: type %d, xmin %lu.",
                            state, PtrGetVal(PtrGetVal(utuple, disk_tuple), flag), tupXid,
                            oldTdInfo.xid, oldTdInfo.td_slot, oldTdInfo.urec_add,
                            tdinfo.xid, tdinfo.td_slot, tdinfo.urec_add,
                            GetTopTransactionIdIfAny(), PtrGetVal(utuple, table_oid), blockno, offnum, lastXid,
                            pg_atomic_read_u64(&g_instance.undo_cxt.globalRecycleXid),
                            pg_atomic_read_u64(&g_instance.undo_cxt.globalFrozenXid),
                            urp, zoneId, PtrGetVal(uzone, GetInsertURecPtr()),
                            PtrGetVal(uzone, GetForceDiscardURecPtr()),
                            PtrGetVal(uzone, GetDiscardURecPtr()), PtrGetVal(uzone, GetRecycleXid()),
                            PtrGetVal(snapshot, satisfies), PtrGetVal(snapshot, xmin))));
                    }
                }
            }
        }
    }
    if (utuple) {
        op = UHeapTidOpFromInfomask(utuple->disk_tuple->flag);
    } else {
        op = UTUPLETID_GONE;
    }
    uheapselect = UHeapTupleSatisfies(op, snapshot, &tdinfo, &snapshotRequests, buffer);
    if (uheapselect == UVERSION_CHECK_CID) {
        if (!haveTransInfo) {
            state = FetchTransInfoFromUndo(blockno, ItemPointerGetOffsetNumber(&utuple->ctid),
                InvalidTransactionId, &tdinfo, NULL, false, NULL, NULL);
            Assert(state != UNDO_TRAVERSAL_ABORT);
            haveTransInfo = true;
        }
        if (tdinfo.cid == InvalidCommandId) {
            ereport(PANIC, (errmodule(MOD_USTORE), errmsg(
                "invalid cid! "
                "LogInfo: undo state %d, tuple flag %u, tupXid %lu. "
                "OldTd: tdxid %lu, tdid %d, undoptr %lu. NewTd: tdxid %lu, tdid %d, undoptr %lu. "
                "TransInfo: xid %lu, oid %u, tid(%u, %u). globalrecyclexid %lu. "
                "Snapshot: type %d, xmin %lu.",
                state, utuple->disk_tuple->flag, tupXid,
                oldTdInfo.xid, oldTdInfo.td_slot, oldTdInfo.urec_add,
                tdinfo.xid, tdinfo.td_slot, tdinfo.urec_add,
                GetTopTransactionIdIfAny(), utuple->table_oid, blockno, offnum,
                pg_atomic_read_u64(&g_instance.undo_cxt.globalRecycleXid),
                snapshot->satisfies, snapshot->xmin)));
        }
        uheapselect = UHeapCheckCID(op, tdinfo.cid, snapshot->curcid);
    }
    if (tdXmin != NULL) {
        *tdXmin = tdinfo.xid;
    }
    if (uheapselect == UVERSION_OLDER || uheapselect == UVERSION_NONE) {
        return false;
    }
    return true;
}

/*
 * UHeapSelectVersionMVCC
 *
 * Decide, for a given MVCC snapshot, whether we should return the current
 * version of a tuple, an older version, or no version at all.  We only have
 * the XID available here, so if the CID turns out to be relevant, we must
 * return UVERSION_CHECK_CID; caller is responsible for calling UHeapCheckCID
 * with the appropriate CID to obtain a final answer.
 */
static UVersionSelector UHeapSelectVersionMVCC(UTupleTidOp op, TransactionId xid, Snapshot snapshot, Buffer buffer)
{
    TransactionIdStatus hintstatus;
    /* IMPORTANT: Version snapshot is independent of the current transaction. */
    if (!IsVersionMVCCSnapshot(snapshot) && TransactionIdIsCurrentTransactionId(xid)) {
        /*
         * This transaction is still running and belongs to the current
         * session.  If the current CID has been used to stamp a tuple or the
         * snapshot belongs to an older CID, then we need the CID for this
         * tuple to make a final visibility decision.
         */
        if (GetCurrentCommandIdUsed() || GetCurrentCommandId(false) >= snapshot->curcid)
            return UVERSION_CHECK_CID;

        /* Nothing has changed since our scan started. */
        return ((op == UTUPLETID_GONE) ? UVERSION_NONE : UVERSION_CURRENT);
    }
    if (!XidVisibleInSnapshot(xid, snapshot, &hintstatus, (RecoveryInProgress() ? buffer : InvalidBuffer), NULL)) {
        /*
         * The XID is not visible to us, either because it aborted or because
         * it's in our MVCC snapshot.  If this is a new tuple, that means we
         * can't see it at all; otherwise, we need to check older versions.
         */
        return ((op == UTUPLETID_NEW) ? UVERSION_NONE : UVERSION_OLDER);
    }

    /* The XID is visible to us. */
    return ((op == UTUPLETID_GONE) ? UVERSION_NONE : UVERSION_CURRENT);
}

/*
 * UHeapSelectVersionNow
 */
static UVersionSelector UHeapSelectVersionNow(UTupleTidOp op, TransactionId xid)
{
    if (TransactionIdIsCurrentTransactionId(xid)) {
        /*
         * This transaction is still running and belongs to the current
         * session. SnapshotNow always attached with current Command Id.
         * Nothing has changed since our scan started.
         */
        return ((op == UTUPLETID_GONE) ? UVERSION_NONE : UVERSION_CURRENT);
    }
    if (!UHeapTransactionIdDidCommit(xid)) {
        /* The XID is not visible to us */
        return ((op == UTUPLETID_NEW) ? UVERSION_NONE : UVERSION_OLDER);
    }
    /* The XID is visible to us. */
    return ((op == UTUPLETID_GONE) ? UVERSION_NONE : UVERSION_CURRENT);
}

/*
 * InplaceHeapSelectVersionSelf
 *
 * Decide, using SnapshotSelf visibility rules, whether we should return the
 * current version of a tuple, an older version, or no version at all.
 */
static UVersionSelector UHeapSelectVersionSelf(UTupleTidOp op, TransactionId xid)
{
    if (op == UTUPLETID_GONE) {
        if (TransactionIdIsCurrentTransactionId(xid))
            return UVERSION_NONE;
        else if (TransactionIdIsInProgress(xid))
            return UVERSION_OLDER;
        else if (UHeapTransactionIdDidCommit(xid))
            return UVERSION_NONE;
        else
            return UVERSION_OLDER; /* transaction is aborted */
    } else if (op == UTUPLETID_MODIFIED) {
        if (TransactionIdIsCurrentTransactionId(xid))
            return UVERSION_CURRENT;
        else if (TransactionIdIsInProgress(xid))
            return UVERSION_OLDER;
        else if (UHeapTransactionIdDidCommit(xid))
            return UVERSION_CURRENT;
        else
            return UVERSION_OLDER; /* transaction is aborted */
    } else {
        if (TransactionIdIsCurrentTransactionId(xid))
            return UVERSION_CURRENT;
        else if (TransactionIdIsInProgress(xid))
            return UVERSION_NONE;
        else if (UHeapTransactionIdDidCommit(xid))
            return UVERSION_CURRENT;
        else
            return UVERSION_NONE; /* transaction is aborted */
    }

    /* should never get here */
    pg_unreachable();
}

/*
 * UHeapSelectVersionDirty
 * Returns the visible version of tuple (including effects of open
 * transactions) if any, NULL otherwise.
 *
 * Here, we consider the effects of: all committed and in-progress transactions (as of the current instant)
 * previous commands of this transaction
 * changes made by the current command
 *
 * This is essentially like InplaceHeapTupleSatisfiesSelf as far as effects of
 * the current transaction and committed/aborted xacts are concerned.
 * However, we also include the effects of other xacts still in progress.
 *
 * The tuple will be considered visible iff: (a) Latest operation on tuple is Delete or non-inplace-update and the
 * current transaction is in progress.
 *                                           (b) Latest operation on tuple is Insert, In-Place update or tuple is
 * locked and the transaction that has performed operation is current
 * transaction or is in-progress or is committed.
 */

static UVersionSelector UHeapSelectVersionDirty(const UTupleTidOp op,
    const UHeapTupleTransInfo *uinfo, Snapshot snapshot, int *snapshotRequests)
{
    if (op == UTUPLETID_GONE) {
        if (TransactionIdIsCurrentTransactionId(uinfo->xid))
            return UVERSION_NONE;
        else if (TransactionIdIsInProgress(uinfo->xid)) {
            snapshot->xmax = uinfo->xid;
            if (uinfo->urec_add != INVALID_UNDO_REC_PTR)
                *snapshotRequests |= SNAPSHOT_REQUESTS_SUBXID;
            return UVERSION_CURRENT;
        } else if (UHeapTransactionIdDidCommit(uinfo->xid)) {
            /* tuple is deleted or non-inplace-updated */
            return UVERSION_NONE;
        }
        /* transaction is aborted */
        return UVERSION_OLDER;
    } else if (op == UTUPLETID_MODIFIED) {
        if (TransactionIdIsCurrentTransactionId(uinfo->xid)) {
            return UVERSION_CURRENT;
        } else if (TransactionIdIsInProgress(uinfo->xid)) {
            snapshot->xmax = uinfo->xid;
            if (uinfo->urec_add != INVALID_UNDO_REC_PTR)
                *snapshotRequests |= SNAPSHOT_REQUESTS_SUBXID;
            return UVERSION_CURRENT; /* being updated */
        } else if (UHeapTransactionIdDidCommit(uinfo->xid)) {
            return UVERSION_CURRENT; /* tuple is updated by someone else */
        }
        /* transaction is aborted */
        return UVERSION_OLDER;
    } else {
        if (TransactionIdIsCurrentTransactionId(uinfo->xid))
            return UVERSION_CURRENT;
        else if (TransactionIdIsInProgress(uinfo->xid)) {
            snapshot->xmin = uinfo->xid;
            if (uinfo->urec_add != INVALID_UNDO_REC_PTR)
                *snapshotRequests |= SNAPSHOT_REQUESTS_SUBXID;
            return UVERSION_CURRENT; /* in insertion by other */
        } else if (UHeapTransactionIdDidCommit(uinfo->xid)) {
            return UVERSION_CURRENT;
        }
        /* inserting transaction aborted */
        return UVERSION_NONE;
    }
    /* should never get here */
    pg_unreachable();
}

/*
 * InplaceHeapCheckCID
 *
 * For a tuple whose xid satisfies TransactionIdIsCurrentTransactionId(xid),
 * this function makes a determination about tuple visibility based on CID.
 */
static UVersionSelector UHeapCheckCID(UTupleTidOp op, CommandId tuple_cid, CommandId visibility_cid,
    bool* has_cur_xact_write)
{
    if (op == UTUPLETID_GONE) {
        if (tuple_cid >= visibility_cid) {
            return UVERSION_OLDER; /* deleted after scan started */
        } else {
            if (has_cur_xact_write != NULL) {
                *has_cur_xact_write = true;
            }
            return UVERSION_NONE; /* deleted before scan started */
        }
    } else if (op == UTUPLETID_MODIFIED) {
        if (tuple_cid >= visibility_cid)
            return UVERSION_OLDER; /* updated/locked after scan started */
        else {
            if (has_cur_xact_write != NULL) {
                *has_cur_xact_write = true;
            }
            return UVERSION_CURRENT; /* updated/locked before scan started */
        }
    } else {
        if (tuple_cid >= visibility_cid)
            return UVERSION_NONE; /* inserted after scan started */
        else {
            if (has_cur_xact_write != NULL) {
                *has_cur_xact_write = true;
            }
            return UVERSION_CURRENT; /* inserted before scan started */
        }
    }

    /* should never get here */
    pg_unreachable();
}

static UVersionSelector UHeapSelectVersionUpdate(UTupleTidOp op, TransactionId xid, CommandId visibility_cid)
{
    /* Shouldn't be looking at a delete or non-inplace update. */
    Assert(op != UTUPLETID_GONE);

    if (TransactionIdIsCurrentTransactionId(xid)) {
        /*
         * This transaction is still running and belongs to the current
         * session.  If the current CID has been used to stamp a tuple or the
         * snapshot belongs to an older CID, then we need the CID for this
         * tuple to make a final visibility decision.
         */
        if (GetCurrentCommandIdUsed() || GetCurrentCommandId(false) != visibility_cid)
            return UVERSION_CHECK_CID;

        /* Nothing has changed since our scan started. */
        return UVERSION_CURRENT;
    }

    if (TransactionIdIsInProgress(xid) || !UHeapTransactionIdDidCommit(xid)) {
        /* The XID is still in progress, or aborted; we can't see it. */
        return ((op == UTUPLETID_NEW) ? UVERSION_NONE : UVERSION_OLDER);
    }

    /* The XID is visible to us. */
    return UVERSION_CURRENT;
}

static void UHeapPageGetNewCtid(Buffer buffer, Oid reloid, ItemPointer ctid, UHeapTupleTransInfo *xactinfo,
    Snapshot snapshot)
{
    int tdSlot;
    RowPtr *rp;
    Page page;
    OffsetNumber offnum = ItemPointerGetOffsetNumber(ctid);
    BlockNumber blkno = ItemPointerGetBlockNumber(ctid);

    page = BufferGetPage(buffer);
    rp = UPageGetRowPtr(page, offnum);

    tdSlot = RowPtrGetTDSlot(rp);

    GetTDSlotInfo(buffer, tdSlot, xactinfo);

    /* Get new ctid from undo */
    TransactionId lastXid = InvalidTransactionId;
    UndoRecPtr urp = INVALID_UNDO_REC_PTR;
    UndoTraversalState state = FetchTransInfoFromUndo(blkno, offnum,
        InvalidTransactionId, xactinfo, ctid, false, &lastXid, &urp);
    if (state == UNDO_TRAVERSAL_ABORT) {
        int zoneId = (int)UNDO_PTR_GET_ZONE_ID(urp);
        undo::UndoZone *uzone = undo::UndoZoneGroup::GetUndoZone(zoneId, false);
        ereport(ERROR, (errmodule(MOD_UNDO), errmsg(
            "snapshot too old! the undo record has been force discard. "
            "Reason: Need fetch ctid from undo. "
            "LogInfo: undo state %d. "
            "TDInfo: tdxid %lu, tdid %d, undoptr %lu. "
            "TransInfo: xid %lu, oid %u, tid(%u, %u), lastXid %lu, "
            "globalRecycleXid %lu, globalFrozenXid %lu."
            "ZoneInfo: urp: %lu, zid %d, insertURecPtr %lu, forceDiscardURecPtr %lu, "
            "discardURecPtr %lu, recycleXid %lu. "
            "Snapshot: type %d, xmin %lu.",
            state, PtrGetVal(xactinfo, xid), PtrGetVal(xactinfo, td_slot), PtrGetVal(xactinfo, urec_add),
            GetTopTransactionIdIfAny(), reloid, blkno, offnum, lastXid,
            pg_atomic_read_u64(&g_instance.undo_cxt.globalRecycleXid),
            pg_atomic_read_u64(&g_instance.undo_cxt.globalFrozenXid),
            urp, zoneId, PtrGetVal(uzone, GetInsertURecPtr()), PtrGetVal(uzone, GetForceDiscardURecPtr()),
            PtrGetVal(uzone, GetDiscardURecPtr()), PtrGetVal(uzone, GetRecycleXid()),
            PtrGetVal(snapshot, satisfies), PtrGetVal(snapshot, xmin))));
    }
}

static void UHeapTupleGetSubXid(Buffer buffer, OffsetNumber offnum, UndoRecPtr urecptr, SubTransactionId *subxid)
{
    VerifyMemoryContext();
    UndoRecord *urec = New(CurrentMemoryContext)UndoRecord();
    char *end = NULL;

    *subxid = InvalidSubTransactionId;
    urec->SetUrp(urecptr);
    urec->SetMemoryContext(CurrentMemoryContext);

    UndoTraversalState rc = FetchUndoRecord(urec, InplaceSatisfyUndoRecord, BufferGetBlockNumber(buffer), offnum,
        InvalidTransactionId, false, NULL);
    if (rc != UNDO_TRAVERSAL_COMPLETE) {
        goto out;
    }

    if (urec->ContainSubXact()) {
        /* subxid is at the end of rawdata */
        end = (char *)(urec->Rawdata()->data) + urec->Rawdata()->len;
        *subxid = *(SubTransactionId *)(end - sizeof(SubTransactionId));
    }

out:
    DELETE_EX(urec);
}

static bool UHeapTupleSatisfiesDelta(UHeapTuple uhtup, Snapshot snapshot, 
    Buffer buffer, bool savedTuple, bool visibleNow)
{
    if (!visibleNow) {
        return false;
    }

    if (!UHeapTupleSatisfiesVisibility(uhtup, snapshot, buffer)) {
        return true;
    }

    return false;
}

static bool UHeapTupleSatisfiesLost(UHeapTuple uhtup, Snapshot snapshot, 
    Buffer buffer, bool savedTuple, bool visible, bool tupleFromUndo)
{
    if (visible && tupleFromUndo) {
        return true;
    }

    return false;
}

static bool UHeapTupleSatisfiesVersionMVCC(UHeapTuple uhtup, Snapshot snapshot, 
    Buffer buffer, bool savedTuple, bool visible, bool tupleFromUndo)
{
    if (snapshot->satisfies == SNAPSHOT_DELTA) {
        return UHeapTupleSatisfiesDelta(uhtup, snapshot, buffer, savedTuple, visible);
    } else if (snapshot->satisfies == SNAPSHOT_LOST) {
        return UHeapTupleSatisfiesLost(uhtup, snapshot, buffer, savedTuple, visible, tupleFromUndo);
    } else {
        return visible;
    }
}

bool UHeapTupleFetch(Relation rel, Buffer buffer, OffsetNumber offnum, Snapshot snapshot, UHeapTuple *visibleTuple,
    ItemPointer newCtid, bool keepTup, UHeapTupleTransInfo *savedUinfo, bool *gotTdInfo, const UHeapTuple *savedTuple,
    int16 lastVar, bool *boolArr, bool* has_cur_xact_write)
{
    Page dp = BufferGetPage(buffer);
    RowPtr *rp = UPageGetRowPtr(dp, offnum);
    int tdSlot, savedTdSlot;
    int snapshotRequests = 0;
    UHeapTuple utuple = NULL;
    UTupleTidOp op;
    bool isInvalidSlot;
    bool isFrozen = false;
    bool haveTransInfo = false;
    UVersionSelector uheapselect = UVERSION_NONE;
    UHeapTupleTransInfo tdinfo;
    UHeapTupleTransInfo oldTdInfo;
    bool valid = true;
    bool tupleFromUndo = false;
    TransactionId tupXid = InvalidTransactionId;
    UndoTraversalState state = UNDO_TRAVERSAL_DEFAULT;
    Snapshot savedSnapshot = NULL;
    BlockNumber blockno = BufferGetBlockNumber(buffer);
    bool isFlashBack = u_sess->exec_cxt.isFlashBack || IsVersionMVCCSnapshot(snapshot);
    bool isLogical = (t_thrd.role == WAL_DB_SENDER || t_thrd.role == LOGICAL_READ_RECORD ||
        t_thrd.role == PARALLEL_DECODE) && snapshot->satisfies == SNAPSHOT_TOAST;

    if (isFlashBack && !(snapshot->satisfies == SNAPSHOT_TOAST || IsVersionMVCCSnapshot(snapshot))) {
        ereport(DEBUG1, (errmsg("the u_sess->exec_cxt.isFlashBack is true, not cleaned.")));
    }

    if (snapshot->satisfies == SNAPSHOT_DELTA) {
        savedSnapshot = snapshot;
        snapshot = (Snapshot)snapshot->user_data;
    }

    /*
     * If caller wants SNAPSHOT_DIRTY semantics, certain fields need to be
     * cleared up front.  We may set them again later to pass back various
     * bits of information to the caller.
     */
    if (snapshot->satisfies == SNAPSHOT_DIRTY) {
        snapshot->xmin = snapshot->xmax = InvalidTransactionId;
        snapshot->subxid = InvalidSubTransactionId;
    }

    /*
     * First, determine the transaction slot for this tuple and whether the
     * transaction slot has been reused (i.e. is flagged as invalid).  For
     * normal items, this information is stored in the tuple header; for
     * deleted ones, it is stored in the row pointer.
     */
    if (RowPtrIsNormal(rp)) {
        Assert(lastVar >= -1);
        utuple = savedTuple ? *savedTuple : UHeapGetTuplePartial(rel, buffer, offnum, lastVar, boolArr);
        Assert(utuple->tupTableType == UHEAP_TUPLE);
        tdSlot = UHeapTupleHeaderGetTDSlot(utuple->disk_tuple);
        isInvalidSlot = UHeapTupleHasInvalidXact(utuple->disk_tuple->flag);
        UHeapTupleCopyBaseFromPage(utuple, dp);
        tupXid = UHeapTupleGetModifiedXid(utuple);
        savedTdSlot = tdSlot;
    } else if (RowPtrIsDeleted(rp)) {
        utuple = NULL;
        tdSlot = RowPtrGetTDSlot(rp);
        isInvalidSlot = (RowPtrGetVisibilityInfo(rp) & ROWPTR_XACT_INVALID);
        savedTdSlot = tdSlot;
    } else {
        /*
         * If this RowPtr is neither normal nor dead, it must be unused.  In
         * that case, there is no version of the tuple visible here, so we can
         * exit quickly.
         */
        Assert(!RowPtrIsUsed(rp));
        if (visibleTuple)
            *visibleTuple = NULL;
        return false;
    }

    /*
     * If this is a SNAPSHOT_ANY snapshot, the current version of the tuple is
     * always the visible one.
     *
     * For SNAPSHOT_TOAST, if the tuple has been pruned out or vacuumed, then
     * we go to undo to fetch the latest version. Otherwise, the current version
     * is the visible one.
     */
    if (snapshot->satisfies == SNAPSHOT_ANY || (snapshot->satisfies == SNAPSHOT_TOAST && utuple != NULL)) {
        goto out;
    }

    /* Get the current TD information on the current page */
    GetTDSlotInfo(buffer, tdSlot, &tdinfo);
    oldTdInfo = tdinfo;
    if (!TransactionIdIsValid(tdinfo.xid) && IS_VALID_UNDO_REC_PTR(tdinfo.urec_add) &&
        !isInvalidSlot) {
        ereport(PANIC, (errmodule(MOD_USTORE), errmsg(
            "td xid invalid but tuple not has reused flag! "
            "LogInfo: tuple flag %u, tupXid %lu. "
            "TdInfo: tdid %d, undoptr %lu. "
            "TransInfo: xid %lu, oid %u, tid(%u, %u). globalrecyclexid %lu. "
            "Snapshot: type %d, xmin %lu.",
            utuple->disk_tuple->flag, tupXid, tdinfo.td_slot, tdinfo.urec_add,
            GetTopTransactionIdIfAny(), utuple->table_oid, blockno, offnum,
            pg_atomic_read_u64(&g_instance.undo_cxt.globalRecycleXid),
            snapshot->satisfies, snapshot->xmin)));
    }

    if (tdinfo.td_slot != UHEAPTUP_SLOT_FROZEN) {
        uint64 oldestRecycleXidHavingUndo = pg_atomic_read_u64(&g_instance.undo_cxt.globalRecycleXid);
        uint64 oldestXidHavingUndo = (isFlashBack || isLogical) ?
            oldestRecycleXidHavingUndo : pg_atomic_read_u64(&g_instance.undo_cxt.globalFrozenXid);
        if (RecoveryInProgress()) {
            /* in hot standby mode, if globalRecycleXid advance during query, it may cause data inconsistency */
            oldestXidHavingUndo = pg_atomic_read_u64(&g_instance.undo_cxt.hotStandbyRecycleXid);
        } 
        if (TransactionIdIsValid(tdinfo.xid) && TransactionIdPrecedes(tdinfo.xid, oldestXidHavingUndo)) {
            if (TransactionIdOlderThanAllUndo(tdinfo.xid)) {
                isFrozen = true;
            }
            /* The slot is old enough that we can treat it as frozen. */
            tdinfo.td_slot = UHEAPTUP_SLOT_FROZEN;
#ifdef DEBUG_UHEAP
            UHEAPSTAT_COUNT_VISIBILITY_CHECK_WITH_XID(VISIBILITY_CHECK_SUCCESS_OLDEST_XID);
#endif
        } else if (isInvalidSlot) {
            TransactionIdStatus hintstatus;

            /*
             * The slot has been reused, but we can still skip reading the
             * undo if the XID we got from the transaction slot is visible to
             * our snapshot.  The real XID has to have committed before that
             * one, so it will be visible to our snapshot as well.
             */
            if (TransactionIdIsValid(tdinfo.xid) && (snapshot->satisfies == SNAPSHOT_NOW || (IsMVCCSnapshot(snapshot) &&
                UHeapXidVisibleInSnapshot(tdinfo.xid, snapshot, &hintstatus,
                (RecoveryInProgress() ? buffer : InvalidBuffer), NULL)))) {
                isFrozen = false;
                tdinfo.td_slot = UHEAPTUP_SLOT_FROZEN;
#ifdef DEBUG_UHEAP
                UHEAPSTAT_COUNT_VISIBILITY_CHECK_WITH_XID(VISIBILITY_CHECK_SUCCESS_INVALID_SLOT);
#endif
            } else if (TransactionIdIsValid(tupXid) && UHeapTransactionIdDidCommit(tupXid)) {
                tdinfo.xid = tupXid;
                if (tdinfo.xid < oldestXidHavingUndo) {
                    tdinfo.td_slot = UHEAPTUP_SLOT_FROZEN;
                }
            } else {
                TransactionId lastXid = InvalidTransactionId;
                UndoRecPtr urp = INVALID_UNDO_REC_PTR;
                /* Fetch the tuple's transaction information from the undo */
                state = FetchTransInfoFromUndo(blockno, offnum, InvalidTransactionId,
                    &tdinfo, newCtid, !isFlashBack, &lastXid, &urp);
                if (state == UNDO_TRAVERSAL_COMPLETE) {
                    savedTdSlot = tdinfo.td_slot;
                    haveTransInfo = true;
                    if (TransactionIdOlderThanAllUndo(tdinfo.xid)) {
                        isFrozen = true;
                        tdinfo.td_slot = UHEAPTUP_SLOT_FROZEN;
                    }
                } else if (state == UNDO_TRAVERSAL_END || state == UNDO_TRAVERSAL_ENDCHAIN ||
                    state == UNDO_TRAVERSAL_STOP) {
                    tdinfo.td_slot = UHEAPTUP_SLOT_FROZEN;
                } else if (state == UNDO_TRAVERSAL_ABORT) {
                    if (IsMVCCSnapshot(snapshot) && TransactionIdIsValid(lastXid) &&
                        UHeapXidVisibleInSnapshot(lastXid, snapshot, &hintstatus,
                        (RecoveryInProgress() ? buffer : InvalidBuffer), NULL)) {
                        tdinfo.td_slot = UHEAPTUP_SLOT_FROZEN;
                    } else {
                        int zoneId = (int)UNDO_PTR_GET_ZONE_ID(urp);
                        undo::UndoZone *uzone = undo::UndoZoneGroup::GetUndoZone(zoneId, false);
                        ereport(ERROR, (errmodule(MOD_UNDO), errmsg(
                            "snapshot too old! the undo record has been force discard. "
                            "Reason: Not MVCC snapshot/lastXid Invalid/lastXid invisible to snapshot. "
                            "LogInfo: undo state %d, tuple flag %u, tupXid %lu. "
                            "OldTd: tdxid %lu, tdid %d, undoptr %lu. NewTd: tdxid %lu, tdid %d, undoptr %lu. "
                            "TransInfo: xid %lu, oid %u, tid(%u, %u), "
                            "lastXid %lu, globalrecyclexid %lu, globalFrozenXid %lu. "
                            "ZoneInfo: urp: %lu, zid %d, insertURecPtr %lu, forceDiscardURecPtr %lu, "
                            "discardURecPtr %lu, recycleXid %lu. "
                            "Snapshot: type %d, xmin %lu.",
                            state, PtrGetVal(PtrGetVal(utuple, disk_tuple), flag), tupXid,
                            oldTdInfo.xid, oldTdInfo.td_slot, oldTdInfo.urec_add,
                            tdinfo.xid, tdinfo.td_slot, tdinfo.urec_add,
                            GetTopTransactionIdIfAny(), PtrGetVal(utuple, table_oid), blockno, offnum, lastXid,
                            pg_atomic_read_u64(&g_instance.undo_cxt.globalRecycleXid),
                            pg_atomic_read_u64(&g_instance.undo_cxt.globalFrozenXid),
                            urp, zoneId, PtrGetVal(uzone, GetInsertURecPtr()),
                            PtrGetVal(uzone, GetForceDiscardURecPtr()),
                            PtrGetVal(uzone, GetDiscardURecPtr()), PtrGetVal(uzone, GetRecycleXid()),
                            PtrGetVal(snapshot, satisfies), PtrGetVal(snapshot, xmin))));
                    }
                }
            }
        }
    } else {
        isFrozen = true;
#ifdef DEBUG_UHEAP
        UHEAPSTAT_COUNT_VISIBILITY_CHECK_WITH_XID(VISIBILITY_CHECK_SUCCESS_FROZEN_SLOT);
#endif
    }

    if (isFrozen) {
        if (RowPtrIsNormal(rp)) {
            UHeapDiskTuple item = (UHeapDiskTuple)UPageGetRowData(dp, rp);
            UHeapTupleHeaderSetTDSlot(item, UHEAPTUP_SLOT_FROZEN);
        }
    }

    /* Attempt to make a visibility determination. */
    if (utuple == NULL) {
        op = UTUPLETID_GONE;
    } else {
        uint16 infomask = utuple->disk_tuple->flag;
        op = UHeapTidOpFromInfomask(infomask);
    }

    uheapselect = UHeapTupleSatisfies(op, snapshot, &tdinfo, &snapshotRequests, buffer);
    /* If necessary, check CID against snapshot. */
    if (uheapselect == UVERSION_CHECK_CID) {
        /* UHeapUNDO : Fetch the tuple's transaction information from the undo */
        if (!haveTransInfo) {
            state = FetchTransInfoFromUndo(blockno, offnum, InvalidTransactionId,
                &tdinfo, newCtid, false, NULL, NULL);
            Assert(state != UNDO_TRAVERSAL_ABORT);
            haveTransInfo = true;
            savedTdSlot = tdinfo.td_slot;
        }
        if (tdinfo.cid == InvalidCommandId) {
            ereport(PANIC, (errmodule(MOD_USTORE), errmsg(
                "invalid cid! "
                "LogInfo: undo state %d, tuple flag %u, tupXid %lu. "
                "OldTd: tdxid %lu, tdid %d, undoptr %lu. NewTd: tdxid %lu, tdid %d, undoptr %lu. "
                "TransInfo: xid %lu, oid %u, tid(%u, %u). globalrecyclexid %lu. "
                "Snapshot: type %d, xmin %lu.",
                state, utuple->disk_tuple->flag, tupXid,
                oldTdInfo.xid, oldTdInfo.td_slot, oldTdInfo.urec_add,
                tdinfo.xid, tdinfo.td_slot, tdinfo.urec_add,
                GetTopTransactionIdIfAny(), utuple->table_oid, blockno, offnum,
                pg_atomic_read_u64(&g_instance.undo_cxt.globalRecycleXid),
                snapshot->satisfies, snapshot->xmin)));
        }
        uheapselect = UHeapCheckCID(op, tdinfo.cid, snapshot->curcid, has_cur_xact_write);
    }

    /*
     * If we decided that we need to consult the undo log to figure out what
     * version our snapshot can see, call GetTupleFromUndo to fetch it.
     */
    if (uheapselect == UVERSION_OLDER || (utuple == NULL && uheapselect == UVERSION_NONE && keepTup)) {
        UHeapTuple priorTuple = NULL;

        /* 
         * Fetch the full tuple from page if utuple was from a partial seq scan. 
         * This is important as the undo has the difference between the old and new tuple in the page.
         */
        if (utuple != NULL && lastVar != -1 && !savedTuple) {
            pfree(utuple);
            utuple = UHeapGetTuplePartial(rel, buffer, offnum, -1, boolArr);
        }
        GetTupleFromUndo(tdinfo.urec_add, utuple, &priorTuple, snapshot, snapshot->curcid, buffer,
            offnum, newCtid, tdinfo.td_slot);
        if (utuple != NULL && utuple != priorTuple && !savedTuple)
            pfree(utuple);

        utuple = priorTuple;

        tupleFromUndo = true;
    }

    /* If we decide that no tuple is visible, free the tuple we built here. */
    if (uheapselect == UVERSION_NONE && utuple != NULL) {
        /*
         * Don't free the tuple when we need the latest version even if it's invisible
         * will be useful when a tuple has been non-inplace updated by other transaction
         * and then you lock the tuple again
         * We will retain the not-visible tuple if the caller asked us to do
         * so, but that won't change the visibility status.
         */
        if (keepTup) {
            valid = false;
        } else {
            if (!savedTuple)
                pfree(utuple);
            utuple = NULL;
        }
    }

    /* Get the subxid when caller requests to do so. */
    if ((snapshotRequests & SNAPSHOT_REQUESTS_SUBXID) != 0) {
        /* Return InvalidSubTransactionId because there isnt subtxn support.
         * It should tell the caller there is no subtxn it needs to wait on.
         */
        snapshot->subxid = InvalidSubTransactionId;
        UHeapTupleGetSubXid(buffer, offnum, tdinfo.urec_add, &snapshot->subxid);
    }

    /*
     * If this tuple has been subjected to a non-inplace update, try to
     * retrieve the new CTID if the caller wants it.  When the tuple has been
     * moved to a completely different partition, the new CTID is not
     * meaningful, so we skip trying to find it in that case.
     */
    if (newCtid && !haveTransInfo && (uheapselect == UVERSION_NONE || snapshot->satisfies == SNAPSHOT_ANY) &&
        utuple != NULL && !UHeapTupleIsMoved(utuple->disk_tuple->flag) &&
        UHeapTupleIsUpdated(utuple->disk_tuple->flag)) {
        TransactionId lastXid = InvalidTransactionId;
        UndoRecPtr urp = INVALID_UNDO_REC_PTR;
        state = FetchTransInfoFromUndo(blockno, offnum, InvalidTransactionId,
            &tdinfo, newCtid, false, &lastXid, &urp);
        if (state == UNDO_TRAVERSAL_ABORT) {
            int zoneId = (int)UNDO_PTR_GET_ZONE_ID(urp);
            undo::UndoZone *uzone = undo::UndoZoneGroup::GetUndoZone(zoneId, false);
            ereport(ERROR, (errmodule(MOD_UNDO), errmsg(
                "snapshot too old! the undo record has been force discard. "
                "Reason: Need fetch ctid from undo."
                "LogInfo: undo state %d, tuple flag %u, tupXid %lu. "
                "OldTd: tdxid %lu, tdid %d, undoptr %lu. NewTd: tdxid %lu, tdid %d, undoptr %lu. "
                "TransInfo: xid %lu, oid %u, tid(%u, %u), lastXid %lu, "
                "globalRecycleXid %lu, globalFrozenXid %lu. "
                "ZoneInfo: urp: %lu, zid %d, insertURecPtr %lu, forceDiscardURecPtr %lu, "
                "discardURecPtr %lu, recycleXid %lu. "
                "Snapshot: type %d, xmin %lu.",
                state, PtrGetVal(PtrGetVal(utuple, disk_tuple), flag), tupXid,
                oldTdInfo.xid, oldTdInfo.td_slot, oldTdInfo.urec_add,
                tdinfo.xid, tdinfo.td_slot, tdinfo.urec_add,
                GetTopTransactionIdIfAny(), PtrGetVal(utuple, table_oid), blockno, offnum, lastXid,
                pg_atomic_read_u64(&g_instance.undo_cxt.globalRecycleXid),
                pg_atomic_read_u64(&g_instance.undo_cxt.globalFrozenXid),
                urp, zoneId, PtrGetVal(uzone, GetInsertURecPtr()), PtrGetVal(uzone, GetForceDiscardURecPtr()),
                PtrGetVal(uzone, GetDiscardURecPtr()), PtrGetVal(uzone, GetRecycleXid()),
                PtrGetVal(snapshot, satisfies), PtrGetVal(snapshot, xmin))));
        }
        haveTransInfo = true;
        savedTdSlot = tdinfo.td_slot;
    }

    /*
     * We're all done. Make sure that either caller gets the tuple, or it gets
     * freed.
     */
out:
    if ((haveTransInfo || isFrozen) && savedUinfo) {
        *gotTdInfo = true;

        savedUinfo->td_slot = isFrozen ? UHEAPTUP_SLOT_FROZEN : savedTdSlot;
        savedUinfo->xid = tdinfo.xid;
        savedUinfo->cid = tdinfo.cid;
        savedUinfo->urec_add = tdinfo.urec_add;
    }

    bool visible = (utuple != NULL && valid);
    if (savedSnapshot != NULL || IsVersionMVCCSnapshot(snapshot)) {
        snapshot = savedSnapshot ? savedSnapshot : snapshot;
        visible = UHeapTupleSatisfiesVersionMVCC(utuple, snapshot, 
            buffer, savedTuple, visible, tupleFromUndo);
    }

    if (visibleTuple) {
        *visibleTuple = utuple;
    } else if (utuple && !savedTuple)
        pfree(utuple);

    if (visibleTuple && *visibleTuple) {
        Assert(ItemPointerIsValid(&((*visibleTuple)->ctid)));
    }

    return visible;
}

static UTupleTidOp UHeapTidOpFromInfomask(uint16 infomask)
{
    if ((infomask & (UHEAP_INPLACE_UPDATED)) != 0)
        return UTUPLETID_MODIFIED;
    if ((infomask & (UHEAP_UPDATED | UHEAP_DELETED)) != 0)
        return UTUPLETID_GONE;
    return UTUPLETID_NEW;
}

static void FronzenTDInfo(UHeapTupleTransInfo *txactinfo)
{
    txactinfo->xid = InvalidTransactionId;
    txactinfo->cid = InvalidCommandId;
    txactinfo->urec_add = INVALID_UNDO_REC_PTR;
    txactinfo->td_slot = UHEAPTUP_SLOT_FROZEN;
}

/*
 * UHeapTupleSatisfiesUpdate
 *
 * The return value for this API are same as HeapTupleSatisfiesUpdate.
 * However, there is a notable difference in the way to determine visibility
 * of tuples.  We need to traverse undo record chains to determine the
 * visibility of tuple.
 *
 * For multilockers, the visibility can be determined by the information
 * present on tuple.  See UHeapTupleSatisfiesMVCC.  Also, this API returns
 * TM_Ok, if the strongest locker is committed which means
 * the caller need to take care of waiting for other lockers in such a case.
 *
 * ctid - returns the ctid of visible tuple if the tuple is either deleted or
 * updated.  ctid needs to be retrieved from undo tuple.
 * td_slot - returns the transaction slot of the transaction that has
 * modified the visible tuple.
 * xid - returns the xid that has modified the visible tuple.
 * subxid - returns the subtransaction id, if any, that has modified the
 * visible tuple.  We fetch the subxid from undo only when it is required,
 * i.e. when the caller would wait on it to finish.
 * cid - returns the cid of visible tuple.
 * single_locker_xid - returns the xid of a single in-progress locker, if any.
 * single_locker_trans_slot - returns the transaction slot of a single
 * in-progress locker, if any.
 * lock_allowed - allow caller to lock the tuple if it is in-place updated
 * in_place_updated - returns whether the current visible version of tuple is
 * updated in place.
 */
TM_Result UHeapTupleSatisfiesUpdate(Relation rel, Snapshot snapshot, ItemPointer tid, UHeapTuple utuple,
    CommandId cid, Buffer buffer, ItemPointer ctid, UHeapTupleTransInfo *tdinfo, SubTransactionId *updateSubXid, 
    TransactionId *lockerXid, SubTransactionId *lockerSubXid, bool avoidVisCheck, bool multixidIsMyself, 
    bool *inplaceUpdated, bool selfVisible, bool isLockForUpdate, TransactionId conflictXid, bool isUpsert)
{
    BlockNumber blocknum = ItemPointerGetBlockNumber(tid);
    OffsetNumber offnum = ItemPointerGetOffsetNumber(tid);
    Page page = BufferGetPage(buffer);
    RowPtr *rp = UPageGetRowPtr(page, offnum);
    CommandId curCid = GetCurrentCommandId(false);
    bool doFetchCid = false;
    bool fetchSubXid = false;
    bool hasCtid = false;
    TransactionId tupXid = InvalidTransactionId;
    TM_Result result = TM_Invisible;
    *lockerXid = InvalidTransactionId;
    *updateSubXid = InvalidTransactionId;
    *lockerSubXid = InvalidSubTransactionId;
    uint32 needSync = 0;
    UndoTraversalState state = UNDO_TRAVERSAL_DEFAULT;
    UHeapTupleTransInfo oldTdInfo;

    Assert(utuple->tupTableType == UHEAP_TUPLE);

    utuple->table_oid = RelationGetRelid(rel);
    utuple->ctid = *tid;

    *inplaceUpdated = false;
    if (ctid != NULL) {
        *ctid = *tid;
    }

    if (RowPtrIsDeleted(rp)) {
        utuple->disk_tuple = NULL;
        utuple->disk_tuple_size = 0;

        UHeapPageGetNewCtid(buffer, utuple->table_oid, ctid, tdinfo, snapshot);

        return TM_Updated;
    }

    Assert(RowPtrIsNormal(rp));

    // read data from page
    utuple->disk_tuple = (UHeapDiskTuple)UPageGetRowData(page, rp);
    utuple->disk_tuple_size = RowPtrGetLen(rp);
    UHeapTupleCopyBaseFromPage(utuple, page);

    UHeapDiskTuple tupleData = utuple->disk_tuple;

    int tdSlot = UHeapTupleHeaderGetTDSlot(tupleData);

    doFetchCid = GetCurrentCommandIdUsed() || curCid != cid;

    /* Fetch transactional info of tuple */
    GetTDSlotInfo(buffer, tdSlot, tdinfo);
    oldTdInfo.td_slot = tdinfo->td_slot;
    oldTdInfo.urec_add = tdinfo->urec_add;
    oldTdInfo.xid = tdinfo->xid;

    if (UHeapTupleHasInvalidXact(tupleData->flag)) {
        tupXid = UHeapTupleGetModifiedXid(utuple);

        /* If slot has been reused, then fetch the transaction information from the Undo */
        if (tdinfo->td_slot == UHEAPTUP_SLOT_FROZEN ||
            (TransactionIdIsValid(tdinfo->xid) && TransactionIdOlderThanAllUndo(tdinfo->xid))) {
            FronzenTDInfo(tdinfo);
        } else if (TransactionIdIsValid(tupXid) && UHeapTransactionIdDidCommit(tupXid)) {
            tdinfo->xid = tupXid;
        } else {
            TransactionId lastXid = InvalidTransactionId;
            UndoRecPtr urp = INVALID_UNDO_REC_PTR;
            state = FetchTransInfoFromUndo(blocknum, offnum, InvalidTransactionId, tdinfo, ctid,
                false, &lastXid, &urp);
            if (state == UNDO_TRAVERSAL_ABORT) {
                if (!TransactionIdIsValid(lastXid) || !UHeapTransactionIdDidCommit(lastXid)) {
                    int zoneId = (int)UNDO_PTR_GET_ZONE_ID(urp);
                    undo::UndoZone *uzone = undo::UndoZoneGroup::GetUndoZone(zoneId, false);
                    ereport(ERROR, (errmodule(MOD_UNDO), errmsg(
                        "snapshot too old! the undo record has been force discard. "
                        "Reason: Need fetch old transinfo from undo. "
                        "LogInfo: undo state %d, tuple flag %u, tupXid %lu. "
                        "OldTd: tdxid %lu, tdid %d, undoptr %lu. NewTd: tdxid %lu, tdid %d, undoptr %lu. "
                        "TransInfo: xid %lu, oid %u, tid(%u, %u), lastXid %lu, "
                        "globalRecycleXid %lu, globalFrozenXid %lu. "
                        "ZoneInfo: urp: %lu, zid %d, insertURecPtr %lu, forceDiscardURecPtr %lu, "
                        "discardURecPtr %lu, recycleXid %lu. "
                        "Snapshot: type %d, xmin %lu.",
                        state, PtrGetVal(PtrGetVal(utuple, disk_tuple), flag), tupXid,
                        oldTdInfo.xid, oldTdInfo.td_slot, oldTdInfo.urec_add,
                        PtrGetVal(tdinfo, xid), PtrGetVal(tdinfo, td_slot), PtrGetVal(tdinfo, urec_add),
                        GetTopTransactionIdIfAny(), PtrGetVal(utuple, table_oid), blocknum, offnum, lastXid,
                        pg_atomic_read_u64(&g_instance.undo_cxt.globalRecycleXid),
                        pg_atomic_read_u64(&g_instance.undo_cxt.globalFrozenXid),
                        urp, zoneId, PtrGetVal(uzone, GetInsertURecPtr()), PtrGetVal(uzone, GetForceDiscardURecPtr()),
                        PtrGetVal(uzone, GetDiscardURecPtr()), PtrGetVal(uzone, GetRecycleXid()),
                        PtrGetVal(snapshot, satisfies), PtrGetVal(snapshot, xmin))));
                }
                tdinfo->xid = lastXid;
            } else {
                hasCtid = true;
            }
        }
    } else if (doFetchCid && TransactionIdIsCurrentTransactionId(tdinfo->xid)) {
        state = FetchTransInfoFromUndo(blocknum, offnum, InvalidTransactionId, tdinfo, ctid, false, NULL, NULL);
        Assert(state != UNDO_TRAVERSAL_ABORT);
        hasCtid = true;
    }

    UHeapTupleStatus tupleStatus = UHeapTupleGetStatus(utuple);
    /* tuple is no longer locked by a single locker */
    if (tupleStatus != UHEAPTUPLESTATUS_LOCKED && SINGLE_LOCKER_XID_IS_EXCL_LOCKED(tupleData->flag)) {
        Assert(!UHEAP_XID_IS_TRANS(utuple->disk_tuple->flag));
        UHeapTupleHeaderClearSingleLocker(tupleData);
    }

restart:
    /* tuple is locked by multiple transactions */
    if (tupleStatus == UHEAPTUPLESTATUS_MULTI_LOCKED) {
        *inplaceUpdated = true;

        /*
         * Tuple is locked by multixid and xids other than current transaction has been terminated
         * See UHeapWait for detail.
         */
        if (multixidIsMyself) {
            elog(DEBUG5, "UHeapTupleSatisfiesUpdate[OK], multixact %ld, multixidIsMyself %d",
                UHeapTupleGetRawXid(utuple), multixidIsMyself);
            /* Since we can lock the tuple, tuple must be visible to us */
            return TM_Ok;
        } else {
            if (tdinfo->td_slot == UHEAPTUP_SLOT_FROZEN || TransactionIdOlderThanFrozenXid(tdinfo->xid)) {
                elog(DEBUG5, "UHeapTupleSatisfiesUpdate[FROZEN], multixact %ld, multixidIsMyself %d",
                    UHeapTupleGetRawXid(utuple), multixidIsMyself);
                return TM_BeingModified;
            } else {
                Assert(UHeapTransactionIdDidCommit(tdinfo->xid));
                if (TransactionIdIsValid(conflictXid) && isUpsert) {
                    bool isUpdatedByOthers = (conflictXid != tdinfo->xid);
                    return isUpdatedByOthers ? TM_Updated : TM_BeingModified;
                }
                if (avoidVisCheck || CommittedXidVisibleInSnapshot(tdinfo->xid, snapshot, buffer)) {
                    elog(DEBUG5, "UHeapTupleSatisfiesUpdate[BeingUpdated], multixact %ld, multixidIsMyself %d",
                        UHeapTupleGetRawXid(utuple), multixidIsMyself);
                    return TM_BeingModified;
                } else {
                    elog(DEBUG5, "UHeapTupleSatisfiesUpdate[UPDATED], multixact %ld, multixidIsMyself %d",
                        UHeapTupleGetRawXid(utuple), multixidIsMyself);
                    return TM_Updated;
                }
            }
        }
    /* tuple is locked */
    } else if (tupleStatus == UHEAPTUPLESTATUS_LOCKED) {
        // tuple is locked, then the data is either inplace updated or newly-inserted
        bool isLockerSubXact = ((utuple->disk_tuple->flag & SINGLE_LOCKER_XID_IS_SUBXACT) != 0);

        // 1. get locker info
        Assert(UHEAP_XID_IS_EXCL_LOCKED(tupleData->flag) || UHEAP_XID_IS_SHR_LOCKED(tupleData->flag));

        TransactionId locker = UHeapTupleGetRawXid(utuple);

        *inplaceUpdated = true;

        if (TransactionIdIsCurrentTransactionId(locker)) {
            if (!isLockForUpdate) {
                // let waiter handle this case, possibly already locked
                *lockerXid = locker;
                result = TM_BeingModified;
            } else {
                // skip alreadyLocked check, acquire lock anyway
                result = TM_Ok;
            }
        } else if (TransactionIdIsInProgress(locker, NULL, false, false, !isLockerSubXact)) {
            *lockerXid = locker;
            result = TM_BeingModified;
        } else { /* if locker xid is commited, just consider previous updater */
            // no active locker on tuple, since we have acquired exclusive lock on buffer, simply clear the locker tdid
            UHeapTupleHeaderClearSingleLocker(tupleData);

            if (tdinfo->td_slot == UHEAPTUP_SLOT_FROZEN || TransactionIdOlderThanFrozenXid(tdinfo->xid)) {
                result = TM_Ok;
            } else if (UHeapTransactionIdDidCommit(tdinfo->xid)) {
                if (TransactionIdIsValid(conflictXid) && isUpsert) {
                    bool isUpdatedByOthers = (conflictXid != tdinfo->xid);
                    return isUpdatedByOthers ? TM_Updated : TM_Ok;
                }
                if (avoidVisCheck || CommittedXidVisibleInSnapshot(tdinfo->xid, snapshot, buffer)) {
                    result = TM_Ok;
                } else {
                    result = TM_Updated; // caller will do refetch, lock and EPQ
                }
            } else { // aborted
                result = TM_BeingModified;
            }
        } 
    } else if (tupleStatus == UHEAPTUPLESTATUS_DELETED) {
        // tuple is deleted or non-inplace updated
        // tuple can pass visibility test so DELETE operation on it cannot be all-visible
        Assert(tdinfo->td_slot != UHEAPTUP_SLOT_FROZEN);

        if (TransactionIdIsCurrentTransactionId(tdinfo->xid)) {
            if (doFetchCid && tdinfo->cid >= cid) {
                result = TM_SelfModified;
            } else {
                result = TM_Invisible;
            }
        } else if (TransactionIdIsInProgress(tdinfo->xid, NULL, false, false, true)) {
            // deleter is still active, caller should wait it until it commits or aborts
            result = TM_BeingModified;
            fetchSubXid = true;
        } else if (UHeapTransactionIdDidCommit(tdinfo->xid)) {
            result = TM_Updated;
        } else {
            // set as being modified, and caller will rely on UHeapWait to handle this case
            result = TM_BeingModified;
        }
    } else if (tupleStatus == UHEAPTUPLESTATUS_INPLACE_UPDATED) {
        // tuple is inplace updated
        *inplaceUpdated = true;

        if (tdinfo->td_slot == UHEAPTUP_SLOT_FROZEN || TransactionIdOlderThanFrozenXid(tdinfo->xid)) {
            result = TM_Ok;
        } else if (TransactionIdIsCurrentTransactionId(tdinfo->xid)) {
            if (doFetchCid && tdinfo->cid > cid) {
                result = TM_SelfModified;
            } else if (doFetchCid && tdinfo->cid == cid && !selfVisible) {
                result = TM_SelfUpdated;
            } else {
                result = TM_Ok;
            }
        } else if (TransactionIdIsInProgress(tdinfo->xid, NULL, false, false, true)) {
            result = TM_BeingModified;
            fetchSubXid = true;
        } else if (UHeapTransactionIdDidCommit(tdinfo->xid)) {
            if (TransactionIdIsValid(conflictXid) && isUpsert) {
                bool isUpdatedByOthers = (conflictXid != tdinfo->xid);
                return isUpdatedByOthers ? TM_Updated : TM_Ok;
            }
            if (avoidVisCheck || CommittedXidVisibleInSnapshot(tdinfo->xid, snapshot, buffer)) {
                result = TM_Ok;
            } else {
                result = TM_Updated;
            }
        } else { // aborted
            result = TM_BeingModified;
        }
    } else {
        if (tdinfo->td_slot == UHEAPTUP_SLOT_FROZEN || TransactionIdOlderThanFrozenXid(tdinfo->xid)) {
            result = TM_Ok;
        } else if (TransactionIdIsCurrentTransactionId(tdinfo->xid)) {
            if (doFetchCid && tdinfo->cid > cid) {
                result = TM_Invisible;
            } else if (doFetchCid && tdinfo->cid == cid && !selfVisible) {
                result = TM_SelfCreated;
            } else {
                result = TM_Ok;
            }
        } else if (TransactionIdIsInProgress(tdinfo->xid, &needSync, false, false, true, false)) {
            result = TM_Invisible;
            if (needSync) {
                needSync = 0;
                SyncLocalXidWait(tdinfo->xid);
                goto restart;
            }
        } else if (UHeapTransactionIdDidCommit(tdinfo->xid)) {
            result = TM_Ok;
        } else { // aborted
            result = TM_Invisible;
        }
    }

    // fetch ctid when tuple is non-inplace updated
    if (ctid && !hasCtid && UHeapTupleIsUpdated(tupleData->flag) && !UHeapTupleIsMoved(tupleData->flag)) {
        TransactionId lastXid = InvalidTransactionId;
        UndoRecPtr urp = INVALID_UNDO_REC_PTR;
        state = FetchTransInfoFromUndo(blocknum, offnum, InvalidTransactionId, tdinfo, ctid, false, &lastXid, &urp);
        if (state == UNDO_TRAVERSAL_ABORT) {
            int zoneId = (int)UNDO_PTR_GET_ZONE_ID(urp);
            undo::UndoZone *uzone = undo::UndoZoneGroup::GetUndoZone(zoneId, false);
            ereport(ERROR, (errmodule(MOD_UNDO), errmsg(
                "snapshot too old! the undo record has been force discard. "
                "Reason: Need fetch ctid from undo. "
                "LogInfo: undo state %d, tuple flag %u, tupXid %lu. "
                "OldTd: tdxid %lu, tdid %d, undoptr %lu. NewTd: tdxid %lu, tdid %d, undoptr %lu. "
                "TransInfo: xid %lu, oid %u, tid(%u, %u), lastXid %lu, "
                "globalRecycleXid %lu, globalFrozenXid %lu. "
                "ZoneInfo: urp: %lu, zid %d, insertURecPtr %lu, forceDiscardURecPtr %lu, "
                "discardURecPtr %lu, recycleXid %lu. "
                "Snapshot: type %d, xmin %lu.",
                state, PtrGetVal(PtrGetVal(utuple, disk_tuple), flag), tupXid,
                oldTdInfo.xid, oldTdInfo.td_slot, oldTdInfo.urec_add,
                PtrGetVal(tdinfo, xid), PtrGetVal(tdinfo, td_slot), PtrGetVal(tdinfo, urec_add),
                GetTopTransactionIdIfAny(), PtrGetVal(utuple, table_oid), blocknum, offnum, lastXid,
                pg_atomic_read_u64(&g_instance.undo_cxt.globalRecycleXid),
                pg_atomic_read_u64(&g_instance.undo_cxt.globalFrozenXid),
                urp, zoneId, PtrGetVal(uzone, GetInsertURecPtr()), PtrGetVal(uzone, GetForceDiscardURecPtr()),
                PtrGetVal(uzone, GetDiscardURecPtr()), PtrGetVal(uzone, GetRecycleXid()),
                PtrGetVal(snapshot, satisfies), PtrGetVal(snapshot, xmin))));
        }
        if (ctid->ip_posid == 0) {
            ereport(PANIC, (errmodule(MOD_USTORE), errmsg(
                "invalid ctid! "
                "LogInfo: undo state %d, tuple flag %u, tupXid %lu. "
                "OldTd: tdxid %lu, tdid %d, undoptr %lu. NewTd: tdxid %lu, tdid %d, undoptr %lu. "
                "TransInfo: xid %lu, oid %u, tid(%u, %u). globalrecyclexid %lu. "
                "Snapshot: type %d, xmin %lu.",
                state, utuple->disk_tuple->flag, tupXid,
                oldTdInfo.xid, oldTdInfo.td_slot, oldTdInfo.urec_add,
                tdinfo->xid, tdinfo->td_slot, tdinfo->urec_add,
                GetTopTransactionIdIfAny(), utuple->table_oid, blocknum, offnum,
                pg_atomic_read_u64(&g_instance.undo_cxt.globalRecycleXid),
                snapshot->satisfies, snapshot->xmin)));
        }
    }

    // fetch subxid if any from undo
    if (fetchSubXid) {
        Assert(!TransactionIdIsValid(*lockerXid));
        UHeapTupleGetSubXid(buffer, offnum, tdinfo->urec_add, updateSubXid);
    }

    return result;
}

/*
 * UHeapTupleGetTransInfo - Retrieve transaction information of transaction
 * 			that has modified the tuple.
 *
 * nobuflock indicates whether caller has lock on the buffer 'buf'. If nobuflock
 * is false, we rely on the supplied tuple uhtup to fetch the slot and undo
 * information. Otherwise, we take buffer lock and fetch the actual tuple.
 *
 * snapshot will be used to avoid fetching tuple transaction id from the
 * undo if the transaction slot is reused.  So caller should pass a valid
 * snapshot where it's just fetching the xid for the visibility purpose.
 * InvalidSnapshot indicates that we need the xid of reused transaction
 * slot even if it is not in the snapshot, this is required to store its
 * value in undo record, otherwise, that can break the visibility for
 * other concurrent session holding old snapshot.
 */
UndoTraversalState UHeapTupleGetTransInfo(Buffer buf, OffsetNumber offnum, UHeapTupleTransInfo *txactinfo,
    bool* has_cur_xact_write, TransactionId *lastXid, UndoRecPtr *urp)
{
    RowPtr *rp;
    Page page;
    bool isInvalidSlot = false;
    TransactionId tupXid = InvalidTransactionId;
    UndoTraversalState state = UNDO_TRAVERSAL_DEFAULT;

    page = BufferGetPage(buf);
    rp = UPageGetRowPtr(page, offnum);
    Assert(RowPtrIsNormal(rp) || RowPtrIsDeleted(rp));
    if (!RowPtrIsDeleted(rp)) {
        UHeapDiskTuple hdr = (UHeapDiskTuple)UPageGetRowData(page, rp);
        txactinfo->td_slot = UHeapTupleHeaderGetTDSlot(hdr);
        tupXid = UDiskTupleGetModifiedXid(hdr, page);
        if (UHeapTupleHasInvalidXact(hdr->flag))
            isInvalidSlot = true;
    } else {
        /*
         * If it's deleted and pruned, we fetch the slot and undo information
         * from the item pointer itself.
         */
        txactinfo->td_slot = RowPtrGetTDSlot(rp);
        if (RowPtrGetVisibilityInfo(rp) & ROWPTR_XACT_INVALID)
            isInvalidSlot = true;
    }

    GetTDSlotInfo(buf, txactinfo->td_slot, txactinfo);

    /*
     * It is quite possible that the item is showing some valid transaction
     * slot, but actual slot has been frozen. This can happen when the tuple
     * is in active state and the entry's xid is older than globalRecycleXid
     */
    if (txactinfo->td_slot == UHEAPTUP_SLOT_FROZEN)
        return state;

    /*
     * We need to fetch all the transaction related information from undo
     * record for the tuples that point to a slot that gets invalidated for
     * reuse at some point of time.  See PageFreezeTransSlots.
     */
    if (isInvalidSlot) {
        /*
         * We are intentionally avoiding to fetch the transaction information
         * from undo even when the tuple has invalid_xact_slot marking as if
         * the slot's current xid is all-visible, then the xid prior to it
         * must be all-visible.
         */
        if (TransactionIdIsValid(txactinfo->xid) && TransactionIdOlderThanAllUndo(txactinfo->xid)) {
            FronzenTDInfo(txactinfo);
        } else if (TransactionIdIsValid(tupXid) && UHeapTransactionIdDidCommit(tupXid)) {
            txactinfo->xid = tupXid;
        } else {
            state = FetchTransInfoFromUndo(BufferGetBlockNumber(buf), offnum,
                InvalidTransactionId, txactinfo, NULL, false, lastXid, urp);
        }
    }
    return state;
}

/*
 * UHeapTupleGetTransXid - Retrieve just the XID that last modified the tuple.
 */
TransactionId UHeapTupleGetTransXid(UHeapTuple uhtup, Buffer buf, bool nobuflock, bool* has_cur_xact_write)
{
    UHeapTupleTransInfo txactinfo;
    UHeapTupleData mytup;
    ItemPointer tid = &(uhtup->ctid);
    OffsetNumber offnum = ItemPointerGetOffsetNumber(tid);
    BlockNumber blkno = ItemPointerGetBlockNumber(tid);
    errno_t rc;

    Assert(uhtup->tupTableType == UHEAP_TUPLE);

    if (nobuflock) {
        Page page;
        RowPtr *rp;

        LockBuffer(buf, BUFFER_LOCK_SHARE);

        page = BufferGetPage(buf);
        rp = UPageGetRowPtr(page, offnum);
        /*
         * ZBORKED: Why is there only handling here for the !ItemIdIsDeleted
         * case?  Maybe we should have a completely separate function for the
         * nbuflock case that does Assert(!ItemIdIsDeleted(lp)).
         */
        if (!RowPtrIsDeleted(rp)) {
            /*
             * If the tuple is updated such that its transaction slot has been
             * changed, then we will never be able to get the correct tuple
             * from undo. To avoid, that we get the latest tuple from page
             * rather than relying on it's in-memory copy.
             *
             * ZBORKED: It should probably be the caller's job to ensure that
             * we are passed the correct tuple, rather than our job to go
             * re-fetch it.
             */
            rc = memcpy_s(&mytup, sizeof(UHeapTupleData), uhtup, sizeof(UHeapTupleData));
            securec_check(rc, "\0", "\0");
            mytup.disk_tuple = (UHeapDiskTuple)UPageGetRowData(page, rp);
            mytup.disk_tuple_size = RowPtrGetLen(rp);
            uhtup = &mytup;
        }
    }

    TransactionId lastXid = InvalidTransactionId;
    UndoRecPtr urp = INVALID_UNDO_REC_PTR;
    UndoTraversalState state = UHeapTupleGetTransInfo(buf, offnum, &txactinfo, NULL, &lastXid, &urp);
    if (state == UNDO_TRAVERSAL_ABORT) {
        int zoneId = (int)UNDO_PTR_GET_ZONE_ID(urp);
        undo::UndoZone *uzone = undo::UndoZoneGroup::GetUndoZone(zoneId, false);
        ereport(ERROR, (errmodule(MOD_UNDO), errmsg(
            "snapshot too old! the undo record has been force discard. "
            "Reason: Need fetch transinfo from undo. "
            "LogInfo: undo state %d, tuple flag %u. "
            "TDInfo: tdxid %lu, tdid %d, undoptr %lu. "
            "TransInfo: xid %lu, oid %u, tid(%u, %u), lastXid %lu, "
            "globalRecycleXid %lu, globalFrozenXid %lu. "
            "ZoneInfo: urp: %lu, zid %d, insertURecPtr %lu, forceDiscardURecPtr %lu, "
            "discardURecPtr %lu, recycleXid %lu.",
            state, PtrGetVal(PtrGetVal(uhtup, disk_tuple), flag),
            txactinfo.xid, txactinfo.td_slot, txactinfo.urec_add,
            GetTopTransactionIdIfAny(), PtrGetVal(uhtup, table_oid), blkno, offnum, lastXid,
            pg_atomic_read_u64(&g_instance.undo_cxt.globalRecycleXid),
            pg_atomic_read_u64(&g_instance.undo_cxt.globalFrozenXid),
            urp, zoneId, PtrGetVal(uzone, GetInsertURecPtr()), PtrGetVal(uzone, GetForceDiscardURecPtr()),
            PtrGetVal(uzone, GetDiscardURecPtr()), PtrGetVal(uzone, GetRecycleXid()))));
    }

    /* Release any buffer lock we acquired. */
    if (nobuflock)
        LockBuffer(buf, BUFFER_LOCK_UNLOCK);

    if (txactinfo.td_slot == UHEAPTUP_SLOT_FROZEN)
        return FrozenTransactionId;

    return txactinfo.xid;
}

/*
 * UHeapTupleSatisfiesOldestXmin
 * The tuple will be considered visible if it is visible to any open
 * transaction.
 *
 * utuple is an input/output parameter.  The caller must send the palloc'ed
 * data.  This function can get a tuple from undo to return in which case it
 * will free the memory passed by the caller.
 *
 * xid is an output parameter. It is set to the latest committed/in-progress
 * xid that inserted/modified the tuple.
 * If the latest transaction for the tuple aborted, we fetch a prior committed
 * version of the tuple and return the prior committed xid and status as
 * HEAPTUPLE_LIVE.
 * If the latest transaction for the tuple aborted and it also inserted
 * the tuple, we return the aborted transaction id and status as
 * HEAPTUPLE_DEAD. In this case, the caller *should* never mark the
 * corresponding item id as dead. Because, when undo action for the same will
 * be performed, we need the item pointer.
 */
UHTSVResult UHeapTupleSatisfiesOldestXmin(UHeapTuple uhtup, TransactionId oldestXmin, Buffer buffer,
    bool resolve_abort_in_progress, UHeapTuple *preabort_tuple, TransactionId *xid, SubTransactionId *subxid,
    Relation rel, bool *inplaceUpdated, TransactionId *lastXid)
{
    UHeapDiskTuple tuple = uhtup->disk_tuple;
    UHeapTupleTransInfo uinfo;
    TransactionIdStatus xidstatus = XID_INPROGRESS;
    bool hasXidstatus = false;
    OffsetNumber offnum = ItemPointerGetOffsetNumber(&uhtup->ctid);

    Assert(uhtup->tupTableType == UHEAP_TUPLE);
    Assert(ItemPointerIsValid(&uhtup->ctid));
    Assert(uhtup->table_oid != InvalidOid);

    /* Get transaction id */
    UndoTraversalState state = UHeapTupleGetTransInfo(buffer, offnum, &uinfo, NULL, NULL);
    *xid = uinfo.xid;

    if ((tuple->flag & UHEAP_DELETED) || (tuple->flag & UHEAP_UPDATED)) {
        /*
         * The tuple is deleted and must be all visible if the transaction
         * slot is cleared or latest xid that has changed the tuple precedes
         * smallest xid that has undo.
         */
        if (state == UNDO_TRAVERSAL_ABORT) {
            *xid = InvalidTransactionId;
            return UHEAPTUPLE_RECENTLY_DEAD;
        }

        if (uinfo.td_slot == UHEAPTUP_SLOT_FROZEN || TransactionIdOlderThanAllUndo(uinfo.xid)) {
            return UHEAPTUPLE_DEAD;
        }

        if (TransactionIdIsCurrentTransactionId(uinfo.xid)) {
            return UHEAPTUPLE_DELETE_IN_PROGRESS;
        }

        xidstatus = TransactionIdGetStatus(*xid);
        hasXidstatus = true;

        if (xidstatus == XID_INPROGRESS) {
            /* Get Sub transaction id */
            if (subxid)
                UHeapTupleGetSubXid(buffer, offnum, uinfo.urec_add, subxid);

            return UHEAPTUPLE_DELETE_IN_PROGRESS;
        } else if (xidstatus == XID_COMMITTED) {
            /*
             * Deleter committed, but perhaps it was recent enough that some
             * open transactions could still see the tuple.
             */
            if (!TransactionIdPrecedes(uinfo.xid, oldestXmin)) {
                return UHEAPTUPLE_RECENTLY_DEAD;
            }

            /* Otherwise, it's dead and removable */
            return UHEAPTUPLE_DEAD;
        } else { /* transaction is aborted */
            if (!resolve_abort_in_progress) {
                return UHEAPTUPLE_ABORT_IN_PROGRESS;
            }

            /*
             * For aborted transactions, we need to fetch the tuple from undo
             * chain.  It should be OK to use SnapshotSelf semantics because
             * we know that the latest transaction is aborted; the previous
             * transaction therefore can't be current or in-progress or for
             * that matter aborted.  It seems like even SnapshotAny semantics
             * would be OK here, but GetTupleFromUndo doesn't know about
             * those.
             *
             * ZBORKED: This code path needs tests.  I was not able to hit it
             * in either automated or manual testing.
             */

            UHeapTuple undoTuple;
            GetTupleFromUndo(uinfo.urec_add, uhtup, &undoTuple, SnapshotSelf, InvalidCommandId, buffer, offnum, NULL,
                uinfo.td_slot);

            if (preabort_tuple) {
                *preabort_tuple = undoTuple;
            } else if (undoTuple != NULL && undoTuple != uhtup) {
                pfree(undoTuple);
            }

            if (undoTuple != NULL) {
                return UHEAPTUPLE_LIVE;
            } else {
                // If the transaction that inserted the tuple got aborted, we
                // should return the aborted transaction id.
                return UHEAPTUPLE_DEAD;
            }
        }
    }

    if (state == UNDO_TRAVERSAL_ABORT) {
        if (inplaceUpdated && (tuple->flag & UHEAP_INPLACE_UPDATED)) {
            *inplaceUpdated = true;
        }
        *xid = InvalidTransactionId;
        return UHEAPTUPLE_LIVE;
    }

    /*
     * The tuple must be all visible if the transaction slot is cleared or
     * latest xid that has changed the tuple precedes smallest xid that has
     * undo.
     */
    if (uinfo.td_slot == UHEAPTUP_SLOT_FROZEN || TransactionIdOlderThanAllUndo(uinfo.xid)) {
        if (inplaceUpdated && (tuple->flag & UHEAP_INPLACE_UPDATED)) {
            *inplaceUpdated = true;
        }
        return UHEAPTUPLE_LIVE;
    }

    if (TransactionIdIsCurrentTransactionId(uinfo.xid)) {
        return UHEAPTUPLE_INSERT_IN_PROGRESS;
    }

    if (!hasXidstatus) {
        xidstatus = TransactionIdGetStatus(*xid);
    }

    if (xidstatus == XID_INPROGRESS) {
        /* Get Sub transaction id */
        if (subxid) {
            UHeapTupleGetSubXid(buffer, offnum, uinfo.urec_add, subxid);
        }
        return UHEAPTUPLE_INSERT_IN_PROGRESS; /* in insertion by other */
    } else if (xidstatus == XID_COMMITTED) {
        if (inplaceUpdated && (tuple->flag & UHEAP_INPLACE_UPDATED)) {
            *inplaceUpdated = true;
        }
        return UHEAPTUPLE_LIVE;
    } else { /* transaction is aborted */
        if (!resolve_abort_in_progress) {
            return UHEAPTUPLE_ABORT_IN_PROGRESS;
        }

        if (tuple->flag & UHEAP_INPLACE_UPDATED) {
            /*
             * For aborted transactions, we need to fetch the tuple from undo
             * chain.  It should be OK to use SnapshotSelf semantics because
             * we know that the latest transaction is aborted; the previous
             * transaction therefore can't be current or in-progress or for
             * that matter aborted.  It seems like even SnapshotAny semantics
             * would be OK here, but GetTupleFromUndo doesn't know about
             * those.
             *
             * ZBORKED: This code path needs tests.  I was not able to hit it
             * in either automated or manual testing.
             */
            UHeapTuple undoTuple;
            GetTupleFromUndo(uinfo.urec_add, uhtup, &undoTuple, SnapshotSelf, InvalidCommandId, buffer, offnum, NULL,
                uinfo.td_slot);

            if (preabort_tuple) {
                *preabort_tuple = undoTuple;
            } else if (undoTuple != NULL && undoTuple != uhtup) {
                pfree(undoTuple);
            }

            if (undoTuple != NULL) {
                return UHEAPTUPLE_LIVE;
            }
        }

        /*
         * If the transaction that inserted the tuple got aborted, we should
         * return the aborted transaction id.
         */
        return UHEAPTUPLE_DEAD;
    }

    return UHEAPTUPLE_LIVE;
}

UndoTraversalState FetchTransInfoFromUndo(BlockNumber blocknum, OffsetNumber offnum, TransactionId xid,
    UHeapTupleTransInfo *txactinfo, ItemPointer newCtid, bool needByPass, TransactionId *lastXid, UndoRecPtr *urp)
{
    VerifyMemoryContext();
    UndoRecord *urec = New(CurrentMemoryContext)UndoRecord();

    /*
     * If caller wants the CTID of the latest version of the tuple, set it to
     * that of the tuple we're looking up for starters.  If it's been the
     * subject of a non-in-place update, we'll fix that later.
     */
    if (newCtid)
        ItemPointerSet(newCtid, blocknum, offnum);

    urec->Reset(txactinfo->urec_add);
    urec->SetMemoryContext(CurrentMemoryContext);
    UndoTraversalState rc = FetchUndoRecord(urec, InplaceSatisfyUndoRecord, blocknum, offnum, xid,
        needByPass, lastXid);
    if (urp != NULL)
        *urp = urec->Urp();
    /* The undo record has been discarded. It should be all-visible. */
    if (rc == UNDO_TRAVERSAL_END || rc == UNDO_TRAVERSAL_STOP || rc == UNDO_TRAVERSAL_ENDCHAIN) {
        FronzenTDInfo(txactinfo);
        goto out;
    } else if (rc != UNDO_TRAVERSAL_COMPLETE) {
        goto out;
    }

    txactinfo->xid = urec->Xid();
    txactinfo->cid = urec->Cid();

    /* If this is a non-in-place update, update ctid if requested. */
    if (newCtid && urec->Utype() == UNDO_UPDATE) {
        Assert(urec->Rawdata() != NULL);

        /* ItemPointerData is at the end of RawData() if not subtransaction, otherwise, also minus size of subxid */
        char *end = (char *)(urec->Rawdata()->data) + urec->Rawdata()->len -
            (urec->ContainSubXact() ? sizeof(SubTransactionId) : 0);
        ItemPointerCopy((ItemPointer)(end - sizeof(ItemPointerData)), newCtid);
    }

out:
    DELETE_EX(urec);
    return rc;
}


/*
 * InplaceHeapTupleIsSurelyDead
 *
 * Similar to HeapTupleIsSurelyDead, but for UHeap tuples.
 */
bool UHeapTupleIsSurelyDead(UHeapTuple uhtup, Buffer buffer, OffsetNumber offnum,
    const UHeapTupleTransInfo *cachedTdInfo, const bool useCachedTdInfo)
{
    UHeapTupleTransInfo tdinfo;
    UndoTraversalState state = UNDO_TRAVERSAL_DEFAULT;

    if (uhtup != NULL && UHeapTidOpFromInfomask(uhtup->disk_tuple->flag) != UTUPLETID_GONE)
        return false;

    /*
     * Get transaction information.
     * Here we use a cached transaction information if there is any.
     */
    if (useCachedTdInfo) {
        tdinfo = *cachedTdInfo;
    } else {
        state = UHeapTupleGetTransInfo(buffer, offnum, &tdinfo, NULL, NULL);
    }

    if (state == UNDO_TRAVERSAL_ABORT) {
        return false;
    }
    /*
     * The tuple is deleted and must be all visible if the transaction slot is
     * cleared or latest xid that has changed the tuple precedes smallest xid
     * that has undo.
     */
    if (tdinfo.td_slot == UHEAPTUP_SLOT_FROZEN || TransactionIdOlderThanAllUndo(tdinfo.xid))
        return true;

    return false; /* Tuple is still alive */
}

static UndoTraversalState GetTupleFromUndoRecord(UndoRecPtr urecPtr, TransactionId xid, Buffer buffer,
    OffsetNumber offnum, UHeapDiskTuple hdr, UHeapTuple *tuple, bool *freeTuple,
    UHeapTupleTransInfo *uinfo, ItemPointer ctid, TransactionId *lastXid, UndoRecPtr *urp)
{
    BlockNumber blkno = BufferGetBlockNumber(buffer);
    VerifyMemoryContext();
    UndoRecord *urec = New(CurrentMemoryContext)UndoRecord();
    urec->SetUrp(urecPtr);
    urec->SetMemoryContext(CurrentMemoryContext);

    UndoTraversalState state = FetchUndoRecord(urec, InplaceSatisfyUndoRecord, blkno,
        offnum, xid, false, lastXid);
    if (state != UNDO_TRAVERSAL_COMPLETE) {
        DELETE_EX(urec);
        return state;
    }

    uinfo->td_slot = UpdateTupleHeaderFromUndoRecord(urec, hdr, BufferGetPage(buffer));

    /*
     * If the tuple is being updated or deleted, the payload contains a whole
     * new tuple.  If the caller wants it, extract it.
     */
    int undotype = urec->Utype();

    Assert(tuple != NULL);

    if (undotype == UNDO_UPDATE || undotype == UNDO_DELETE) {
        StringInfo urecPayload = urec->Rawdata();
        Assert(urecPayload);

        if (*tuple != NULL)
            Assert((*tuple)->tupTableType == UHEAP_TUPLE);

        /* ItemPointerData is at the end of RawData() for UNDO_UPDATE records */
        uint32 extraDataLen = (urec->Utype() == UNDO_UPDATE) ? sizeof(ItemPointerData) : 0;
        if (urec->ContainSubXact()) {
            extraDataLen += sizeof(SubTransactionId);
        }

        uint32 undoDataLen = urecPayload->len - extraDataLen;
        uint32 tupleDataLen = undoDataLen + OffsetTdId;
        UHeapTuple htup = (UHeapTuple)uheaptup_alloc(UHeapTupleDataSize + tupleDataLen);
        htup->disk_tuple_size = tupleDataLen;
        ItemPointerSet(&htup->ctid, urec->Blkno(), urec->Offset());
        htup->disk_tuple = (UHeapDiskTuple)((char *)htup + UHeapTupleDataSize);
        htup->table_oid = (urec->Partitionoid() == InvalidOid) ? urec->Reloid() : urec->Partitionoid();
        htup->xc_node_id = u_sess->pgxc_cxt.PGXCNodeIdentifier;

        errno_t rc = memcpy_s((char*)htup->disk_tuple + OffsetTdId, undoDataLen, urecPayload->data, undoDataLen);
        securec_check_c(rc, "\0", "\0");
        htup->disk_tuple->xid = (ShortTransactionId)FrozenTransactionId;

        if (*freeTuple) {
            pfree(*tuple);
        }
        *tuple = htup;
        *freeTuple = true;
    } else if (undotype == UNDO_INPLACE_UPDATE) {
        StringInfo urecPayload = urec->Rawdata();
        Assert(urecPayload);
        Assert(urecPayload->len > 0);
        uint16 *prefixlenPtr = NULL;
        uint16 *suffixlenPtr = NULL;
        uint16 prefixlen = 0;
        uint16 suffixlen = 0;
        int subxidSize = urec->ContainSubXact() ? sizeof(SubTransactionId) : 0;
        UHeapDiskTuple diskTuple = (*tuple)->disk_tuple;
        uint8 *tHoffPtr = (uint8 *)urecPayload->data;
        uint8 tHoff = *tHoffPtr;
        char *curUndodataPtr = urecPayload->data + sizeof(uint8) + tHoff - OffsetTdId;
        int readSize = sizeof(uint8) + tHoff - OffsetTdId;
        uint8 *flagsPtr = (uint8 *)curUndodataPtr;
        uint8 flags = *flagsPtr;
        curUndodataPtr += sizeof(uint8);
        readSize += sizeof(uint8);

        if (flags & UREC_INPLACE_UPDATE_XOR_PREFIX) {
            prefixlenPtr = (uint16 *)(curUndodataPtr);
            curUndodataPtr += sizeof(uint16);
            readSize += sizeof(uint16);
            prefixlen = *prefixlenPtr;
        }
        if (flags & UREC_INPLACE_UPDATE_XOR_SUFFIX) {
            suffixlenPtr = (uint16 *)(curUndodataPtr);
            curUndodataPtr += sizeof(uint16);
            readSize += sizeof(uint16);
            suffixlen = *suffixlenPtr;
        }

        char *oldDisktuple = (char *)palloc0(urecPayload->len - readSize - subxidSize + prefixlen + suffixlen + tHoff);
        errno_t rc = memcpy_s(oldDisktuple + OffsetTdId, tHoff - OffsetTdId, urecPayload->data + sizeof(uint8),
            tHoff - OffsetTdId);
        securec_check_c(rc, "\0", "\0");
        ((UHeapDiskTupleData*)oldDisktuple)->xid = (ShortTransactionId)FrozenTransactionId;
        char *curOldDisktuplePtr = oldDisktuple + tHoff;

        /* copy the perfix to oldDisktuple */
        if (flags & UREC_INPLACE_UPDATE_XOR_PREFIX) {
            rc = memcpy_s(curOldDisktuplePtr, prefixlen, (char *)diskTuple + diskTuple->t_hoff, prefixlen);
            securec_check_c(rc, "\0", "\0");
            curOldDisktuplePtr += prefixlen;
        }

        char *newp = (char *)diskTuple + diskTuple->t_hoff + prefixlen;
        char *curUndoDataP = urecPayload->data + readSize;
        int oldlen = urecPayload->len - readSize - subxidSize + prefixlen + suffixlen;
        int newlen = (*tuple)->disk_tuple_size - diskTuple->t_hoff;
        int oldDataLen = oldlen - prefixlen - suffixlen;

        if (oldDataLen > 0) {
            rc = memcpy_s(curOldDisktuplePtr, oldDataLen, curUndoDataP, oldDataLen);
            securec_check_c(rc, "\0", "\0");
            curOldDisktuplePtr += (oldDataLen);
        }

        if (flags & UREC_INPLACE_UPDATE_XOR_SUFFIX) {
            rc = memcpy_s(curOldDisktuplePtr, suffixlen, newp + newlen - prefixlen - suffixlen, suffixlen);
            securec_check_c(rc, "\0", "\0");
        }
        uint32 newTupleLength = oldlen + tHoff;
        UHeapTuple htup = (UHeapTuple)uheaptup_alloc(UHeapTupleDataSize + newTupleLength);
        htup->disk_tuple_size = newTupleLength;
        ItemPointerSet(&htup->ctid, urec->Blkno(), urec->Offset());
        htup->disk_tuple = (UHeapDiskTuple)((char *)htup + UHeapTupleDataSize);
        htup->table_oid = (urec->Partitionoid() == InvalidOid) ? urec->Reloid() : urec->Partitionoid();
        htup->xc_node_id = u_sess->pgxc_cxt.PGXCNodeIdentifier;
        rc = memcpy_s(htup->disk_tuple, newTupleLength, oldDisktuple, newTupleLength);
        securec_check_c(rc, "\0", "\0");

        if (*freeTuple) {
            pfree(*tuple);
        }
        *tuple = htup;
        *freeTuple = true;
        pfree(oldDisktuple);
    }

    uinfo->urec_add = urec->Blkprev();
    uinfo->cid = InvalidCommandId;
    uinfo->xid = urec->OldXactId();

    /* If this is a non-in-place update, update ctid if requested. */
    if (ctid && urec->Utype() == UNDO_UPDATE) {
        Assert(urec->Rawdata() != NULL);

        /* ItemPointerData is at the end of RawData() */
        char *end = (char *)urec->Rawdata()->data + urec->Rawdata()->len -
            (urec->ContainSubXact() ? sizeof(SubTransactionId) : 0);
        ItemPointerCopy((ItemPointer)(end - sizeof(ItemPointerData)), ctid);
    }
    DELETE_EX(urec);

    return state;
}

void UHeapUpdateTDInfo(int tdSlot, Buffer buffer, OffsetNumber offnum, UHeapTupleTransInfo* uinfo)
{
    UHeapTupleTransInfo tmpUinfo;

    GetTDSlotInfo(buffer, tdSlot, &tmpUinfo);

    uinfo->td_slot = tmpUinfo.td_slot;
    uinfo->urec_add = tmpUinfo.urec_add;
}

static inline UVersionSelector UHeapCheckUndoSnapshot(Snapshot snapshot, UHeapTupleTransInfo uinfo,
                                                      CommandId curcid, UTupleTidOp op, Buffer buffer)
{
    if (snapshot == NULL) {
        return UHeapSelectVersionUpdate(op, uinfo.xid, curcid);
    } else if (IsMVCCSnapshot(snapshot)) {
        return UHeapSelectVersionMVCC(op, uinfo.xid, snapshot, buffer);
    } else if (snapshot->satisfies == SNAPSHOT_NOW) {
        return UHeapSelectVersionNow(op, uinfo.xid);
    }
    return UHeapSelectVersionSelf(op, uinfo.xid);
}

static bool GetTupleFromUndo(UndoRecPtr urecAdd, UHeapTuple currentTuple, UHeapTuple *visibleTuple, Snapshot snapshot,
    CommandId curcid, Buffer buffer, OffsetNumber offnum, ItemPointer ctid, int tdSlot)
{
    TransactionId prevUndoXid = InvalidTransactionId;
    int prevTdSlotId = tdSlot;
    UHeapTupleTransInfo uinfo;
    bool freeTuple = false;
    BlockNumber blkno = BufferGetBlockNumber(buffer);
    UHeapDiskTupleData hdr;
    UndoTraversalState state = UNDO_TRAVERSAL_DEFAULT;

#ifdef DEBUG_UHEAP
    UHEAPSTAT_COUNT_TUPLE_OLD_VERSION_VISITS();
#endif

    if (currentTuple != NULL) {
        /* Sanity check. */
        Assert(ItemPointerGetOffsetNumber(&currentTuple->ctid) == offnum);

        /*
         * We must set up 'hdr' to point to be a copy of the header bytes from
         * the most recent version of the tuple.  This is because in the
         * special case where the undo record we find is an UNDO_INSERT
         * record, we modify the existing bytes rather than overwriting them
         * completely.  If currentTuple == NULL, then the current version of
         * the tuple has been deleted or subjected to a non-in-place update,
         * so the first record we find won't be UNDO_INSERT.
         *
         * ZBORKED: We should really change this to get rid of the special
         * case for UNDO_INSERT, either by making it so that this function
         * doesn't get called in that case, or by making it so that it doesn't
         * need the newer tuple header bytes, or some other clever trick. That
         * would eliminate a substantial amount of complexity and ugliness
         * here.
         */
        errno_t rc = memcpy_s(&hdr, SizeOfUHeapDiskTupleData, currentTuple->disk_tuple, SizeOfUHeapDiskTupleData);
        securec_check_c(rc, "\0", "\0");

        /* Initially, result tuple is same as input tuple. */
        if (visibleTuple != NULL)
            *visibleTuple = currentTuple;
    }

    /*
     * If caller wants the CTID of the latest version of the tuple, set it to
     * that of the tuple we're looking up for starters.  If it's been the
     * subject of a non-in-place update, GetTupleFromUndoRecord will adjust
     * the value later.
     */
    if (ctid)
        ItemPointerSet(ctid, blkno, offnum);

    /*
     * tuple is modified after the scan is started, fetch the prior record
     * from undo to see if it is visible. loop until we find the visible
     * version.
     */
    while (1) {
        TransactionId lastXid = InvalidTransactionId;
        UndoRecPtr urp = INVALID_UNDO_REC_PTR;
        state = GetTupleFromUndoRecord(urecAdd, prevUndoXid, buffer, offnum, &hdr, visibleTuple,
            &freeTuple, &uinfo, ctid, &lastXid, &urp);
        int zoneId = (int)UNDO_PTR_GET_ZONE_ID(urp);
        undo::UndoZone *uzone = undo::UndoZoneGroup::GetUndoZone(zoneId, false);
        if (state == UNDO_TRAVERSAL_ABORT) {
            ereport(ERROR, (errmodule(MOD_UNDO), errmsg(
                "snapshot too old! the undo record has been force discard. "
                "Reason: Need fetch undo record. "
                "LogInfo: undo state %d, VisibleTuple flag %u, currentTuple flag %u. "
                "TDInfo: tdxid %lu, tdid %d, undoptr %lu. "
                "TransInfo: xid %lu, oid %u, tid(%u, %u), lastXid %lu,  "
                "globalRecycleXid %lu, globalFrozenXid %lu. "
                "ZoneInfo: urp: %lu, zid %d, insertURecPtr %lu, forceDiscardURecPtr %lu, "
                "discardURecPtr %lu, recycleXid %lu. "
                "Snapshot: type %d, xmin %lu.",
                state, PtrGetVal(PtrGetVal(*visibleTuple, disk_tuple), flag),
                PtrGetVal(PtrGetVal(currentTuple, disk_tuple), flag),
                uinfo.xid, uinfo.td_slot, uinfo.urec_add,
                GetTopTransactionIdIfAny(), PtrGetVal(*visibleTuple, table_oid), blkno, offnum, lastXid,
                pg_atomic_read_u64(&g_instance.undo_cxt.globalRecycleXid),
                pg_atomic_read_u64(&g_instance.undo_cxt.globalFrozenXid),
                urp, zoneId, PtrGetVal(uzone, GetInsertURecPtr()), PtrGetVal(uzone, GetForceDiscardURecPtr()),
                PtrGetVal(uzone, GetDiscardURecPtr()), PtrGetVal(uzone, GetRecycleXid()),
                PtrGetVal(snapshot, satisfies), PtrGetVal(snapshot, xmin))));
        } else if (state == UNDO_TRAVERSAL_END || state == UNDO_TRAVERSAL_ENDCHAIN) {
            TransactionId tupXid = InvalidTransactionId;
            Oid tableOid = InvalidOid;
            int tupTdid = InvalidTDSlotId;
            if (currentTuple != NULL) {
                tupXid = UHeapTupleGetModifiedXid(currentTuple);
                tableOid = currentTuple->table_oid;
                tupTdid = UHeapTupleHeaderGetTDSlot(currentTuple->disk_tuple);
            }
            ereport(ERROR, (errmodule(MOD_USTORE), errmsg(
                "snapshot too old! "
                "Reason: Need fetch undo record. "
                "LogInfo: urp %lu, undo state %d, tuple flag %u, tupTd %d, tupXid %lu. "
                "Td: tdxid %lu, tdid %d, undoptr %lu. "
                "TransInfo: xid %lu, oid %u, tid(%u, %u), "
                "globalRecycleXid %lu, globalFrozenXid %lu. "
                "ZoneInfo: urp: %lu, zid %d, insertURecPtr %lu, forceDiscardURecPtr %lu, "
                "discardURecPtr %lu, recycleXid %lu. "
                "Snapshot: type %d, xmin %lu.",
                urecAdd, state, hdr.flag, tupTdid, tupXid,
                uinfo.xid, uinfo.td_slot, uinfo.urec_add,
                GetTopTransactionIdIfAny(), tableOid, blkno, offnum,
                pg_atomic_read_u64(&g_instance.undo_cxt.globalRecycleXid),
                pg_atomic_read_u64(&g_instance.undo_cxt.globalFrozenXid),
                urp, zoneId, PtrGetVal(uzone, GetInsertURecPtr()), PtrGetVal(uzone, GetForceDiscardURecPtr()),
                PtrGetVal(uzone, GetDiscardURecPtr()), PtrGetVal(uzone, GetRecycleXid()),
                snapshot->satisfies, snapshot->xmin)));
        } else if (state != UNDO_TRAVERSAL_COMPLETE || uinfo.td_slot == UHEAPTUP_SLOT_FROZEN ||
            TransactionIdOlderThanAllUndo(uinfo.xid)) {
            break;
        }

        UVersionSelector selector = UVERSION_NONE;
        bool haveCid = false;
        /*
         * Change the undo chain if the undo tuple is stamped with a
         * different transaction.
         */
        if (uinfo.td_slot != prevTdSlotId) {
            /* Get the right urecptr to start with */
            UHeapUpdateTDInfo(uinfo.td_slot, buffer, offnum, &uinfo);
        }

        UTupleTidOp op = UHeapTidOpFromInfomask(hdr.flag);

        /* can't further operate on deleted or non-inplace-updated tuple */
        Assert(op != UTUPLETID_GONE);

        if (uinfo.cid != InvalidCommandId) {
            haveCid = true;
        }

        /*
         * The tuple must be all visible if the transaction slot is cleared or
         * latest xid that has changed the tuple is too old that it is
         * all-visible or it precedes smallest xid that has undo.
         *
         * For snapshot_toast, the first undo tuple is the visible one
         */
        if (uinfo.td_slot == UHEAPTUP_SLOT_FROZEN || TransactionIdEquals(uinfo.xid, FrozenTransactionId) ||
            TransactionIdOlderThanAllUndo(uinfo.xid) || (snapshot != NULL && snapshot->satisfies == SNAPSHOT_TOAST))
            break;

        /* Preliminary visibility check, without relying on the CID. */
        selector = UHeapCheckUndoSnapshot(snapshot, uinfo, curcid, op, buffer);
        /* If necessary, get and check CID. */
        if (selector == UVERSION_CHECK_CID) {
            if (!haveCid) {
                state = FetchTransInfoFromUndo(blkno, offnum, uinfo.xid, &uinfo, NULL, false, NULL, NULL);
                Assert(state != UNDO_TRAVERSAL_ABORT);
                haveCid = true;
            }

            /* OK, now we can make a final visibility decision. */
            selector = UHeapCheckCID(op, uinfo.cid, curcid);
        }

        /* Return the current version, or nothing, if appropriate. */
        if (selector == UVERSION_CURRENT)
            break;
        if (selector == UVERSION_NONE) {
            if (visibleTuple != NULL) {
                if (freeTuple)
                    pfree(*visibleTuple);
                *visibleTuple = NULL;
            }
            return false;
        }

        /* Need to check next older version, so loop around. */
        Assert(selector == UVERSION_OLDER);
        urecAdd = uinfo.urec_add;
        prevUndoXid = uinfo.xid;
        prevTdSlotId = uinfo.td_slot;
    }

    /* Copy latest header reconstructed from undo back into tuple. */
    if (visibleTuple != NULL && *visibleTuple != NULL) {
        errno_t rc = memcpy_s((*visibleTuple)->disk_tuple, SizeOfUHeapDiskTupleData, &hdr, SizeOfUHeapDiskTupleData);
        securec_check_c(rc, "\0", "\0");
        FastVerifyUTuple((*visibleTuple)->disk_tuple, buffer);
    }

    return true;
}


/* check output of UHeapTupleSatisfiesOldestXmin return false if tuple visible */
static bool HtsvCheck(const UHTSVResult htsvResult, TransactionId * const xid, bool *visible,
    const bool tupleUHeapUpdated)
{
    switch (htsvResult) {
        case UHEAPTUPLE_LIVE:
            if (tupleUHeapUpdated) {
                /*
                 * If xid is invalid, then we know that slot is frozen and
                 * tuple will be visible so we can return false.
                 */
                if (*xid == InvalidTransactionId) {
                    Assert(*visible);
                    return false;
                }

                /*
                 * We can't rely on callers visibility information for
                 * in-place updated tuples because they consider the tuple as
                 * visible if any version of the tuple is visible whereas we
                 * want to know the status of current tuple.  In case of
                 * aborted transactions, it is quite possible that the
                 * rollback actions aren't yet applied and we need the status
                 * of last committed transaction;
                 * InplaceHeapTupleSatisfiesOldestXmin returns us that information.
                 */
                if (XidIsConcurrent(*xid))
                    *visible = false;
            }
            if (*visible)
                return false;
            break;
        case UHEAPTUPLE_RECENTLY_DEAD:
            if (!*visible)
                return false;
            break;
        case UHEAPTUPLE_DELETE_IN_PROGRESS:
            break;
        case UHEAPTUPLE_INSERT_IN_PROGRESS:
            break;
        case UHEAPTUPLE_DEAD:
            return false;
        default:

            /*
             * The only way to get to this default clause is if a new value is
             * added to the enum type without adding it to this switch
             * statement.  That's a bug, so elog.
             */
            elog(ERROR, "unrecognized return value from UHeapTupleSatisfiesOldestXmin: %u", htsvResult);

            /*
             * In spite of having all enum values covered and calling elog on
             * this default, some compilers think this is a code path which
             * allows xid to be used below without initialization. Silence
             * that warning.
             */
            *xid = InvalidTransactionId;
    }
    return true;
}


/*
 * This is a helper function for CheckForSerializableConflictOut.
 *
 * Check to see whether the tuple has been written to by a concurrent
 * transaction, either to create it not visible to us, or to delete it
 * while it is visible to us.  The "visible" bool indicates whether the
 * tuple is visible to us, while InplaceHeapTupleSatisfiesOldestXmin checks what
 * else is going on with it. The caller should have a share lock on the buffer.
 */
bool UHeapTupleHasSerializableConflictOut(bool visible, Relation relation, ItemPointer tid, Buffer buffer,
    TransactionId *xid)
{
    UHTSVResult htsvResult;
    OffsetNumber offnum = ItemPointerGetOffsetNumber(tid);
    UHeapTuple tuple;
    bool tupleUHeapUpdated = false;

    Assert(ItemPointerGetBlockNumber(tid) == BufferGetBlockNumber(buffer));

    Page dp = BufferGetPage(buffer);
    /* check for bogus TID */
    Assert((offnum >= FirstOffsetNumber) && (offnum <= UHeapPageGetMaxOffsetNumber(dp)));

    RowPtr *lp = UPageGetRowPtr(dp, offnum);
    /* check for unused or dead items */
    Assert(RowPtrIsNormal(lp) || RowPtrIsDeleted(lp));

    /*
     * If the record is deleted and pruned, its place in the page might have
     * been taken by another of its kind.
     */
    if (RowPtrIsDeleted(lp)) {
        /*
         * If the tuple is still visible to us, then we've a conflict.
         * Because, the transaction that deleted the tuple already got
         * committed.
         */
        if (visible) {
            Snapshot snap = GetTransactionSnapshot();
            UHeapTupleFetch(relation, buffer, offnum, snap, &tuple, NULL, false);
            *xid = UHeapTupleGetTransXid(tuple, buffer, false);
            pfree(tuple);
            return true;
        } else {
            return false;
        }
    }

    tuple = UHeapGetTuple(relation, buffer, offnum);
    if (tuple->disk_tuple->flag & UHEAP_INPLACE_UPDATED)
        tupleUHeapUpdated = true;

    htsvResult = UHeapTupleSatisfiesOldestXmin(tuple, u_sess->utils_cxt.TransactionXmin, buffer, true,
        NULL, xid, NULL, relation);
    pfree(tuple);

    if (!HtsvCheck(htsvResult, xid, &visible, tupleUHeapUpdated)) {
        return false;
    }

    Assert(TransactionIdIsValid(*xid));
    Assert(TransactionIdFollowsOrEquals(*xid, u_sess->utils_cxt.TransactionXmin));

    /*
     * Find top level xid.  Bail out if xid is too early to be a conflict, or
     * if it's our own xid.
     */
    if (TransactionIdEquals(*xid, GetTopTransactionIdIfAny()))
        return false;
    if (TransactionIdPrecedes(*xid, u_sess->utils_cxt.TransactionXmin))
        return false;
    if (TransactionIdEquals(*xid, GetTopTransactionIdIfAny()))
        return false;

    return true;
}

void UHeapTupleCheckVisible(Snapshot snapshot, UHeapTuple tuple, Buffer buffer)
{
    if (!IsolationUsesXactSnapshot())
        return;
    LockBuffer(buffer, BUFFER_LOCK_SHARE);
    TransactionId tdXmin = InvalidTransactionId;
    if (!UHeapTupleSatisfiesVisibility(tuple, snapshot, buffer, &tdXmin)) {
        if (!TransactionIdIsCurrentTransactionId(tdXmin)) {
            LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
            ereport(ERROR, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                errmsg("could not serialize access due to concurrent update")));
        }
    }
    LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
}
