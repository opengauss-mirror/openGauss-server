/* -------------------------------------------------------------------------
 *
 * knl_uheap.cpp
 * Implement the access interfaces of inplace update engine.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 * src/gausskernel/storage/access/ustore/knl_uheap.cpp
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "pgstat.h"
#include "nodes/relation.h"
#include "utils/datum.h"
#include "utils/snapmgr.h"
#include "storage/procarray.h"
#include "storage/predicate.h"
#include "storage/lmgr.h"
#include "storage/lock/lock.h"
#include "storage/freespace.h"
#include "access/tuptoaster.h"
#include "access/xact.h"
#include "access/transam.h"
#include "access/ustore/knl_uheap.h"
#include "access/ustore/knl_umultilocker.h"
#include "access/ustore/knl_utuple.h"
#include "access/ustore/knl_uhio.h"
#include "access/ustore/knl_undorequest.h"
#include "access/ustore/knl_uvisibility.h"
#include "access/ustore/undo/knl_uundoapi.h"
#include "access/ustore/knl_uundorecord.h"
#include "access/ustore/knl_utils.h"
#include "nodes/execnodes.h"
#include "access/ustore/knl_utuple.h"
#include "access/ustore/knl_utuptoaster.h"
#include "access/ustore/knl_whitebox_test.h"
#include <stdlib.h>

static Bitmapset *UHeapDetermineModifiedColumns(Relation relation, Bitmapset *interesting_cols, UHeapTuple oldtup,
    UHeapTuple newtup);
static void TtsUHeapMaterialize(TupleTableSlot *slot);
static void LogUHeapInsert(UHeapWALInfo *walinfo, Relation rel);
static void LogUPageExtendTDSlots(Buffer buf, uint8 currTDSlots, uint8 numExtended);
static void LogUHeapDelete(UHeapWALInfo *walinfo);
static void LogUHeapUpdate(UHeapWALInfo *oldTupWalinfo, UHeapWALInfo *newTupWalinfo, bool isInplaceUpdate,
    int undoXorDeltaSize, char *xlogXorDelta, uint16 xorPrefixlen, uint16 xorSurfixlen, Relation rel);
static void LogUHeapMultiInsert(UHeapMultiInsertWALInfo *multiWalinfo, bool skipUndo, char *scratch,
    UndoRecPtr *urpvec);
static void UHeapPagePruneFSM(Relation relation, Buffer buffer, TransactionId fxid, Page page, BlockNumber blkno);
static Page GetPageBuffer(Relation relation, BlockNumber blkno, Buffer &buffer)
{
    buffer = ReadBuffer(relation, blkno);
    return BufferGetPage(buffer);
}
static bool UHeapPageXidMinMax(Page page, bool multi, ShortTransactionId *min, ShortTransactionId *max)
{
    bool found = false;
    OffsetNumber offnum = InvalidOffsetNumber;
    OffsetNumber maxoff = UHeapPageGetMaxOffsetNumber(page);

    for (offnum = FirstOffsetNumber; offnum <= maxoff; offnum = OffsetNumberNext(offnum)) {
        UHeapDiskTuple utuple;

        RowPtr *rowptr = UPageGetRowPtr(page, offnum);

        /* skip tuples which has been pruned */
        if (!RowPtrIsNormal(rowptr)) {
            continue;
        }
        utuple = (UHeapDiskTuple)UPageGetRowData(page, rowptr);
        if (!SINGLE_LOCKER_XID_IS_EXCL_LOCKED(utuple->flag) && !SINGLE_LOCKER_XID_IS_SHR_LOCKED(utuple->flag) &&
            !UHeapTupleHasMultiLockers(utuple->flag)) {
            continue;
        }

        /* If multi=true, we should only count in tuple marked as multilocked */
        if (TransactionIdIsNormal(utuple->xid) && (!multi || UHeapTupleHasMultiLockers(utuple->flag))) {
            if (!found) {
                found = true;
                *min = *max = utuple->xid;
            } else {
                *min = Min(*min, utuple->xid);
                *max = Max(*max, utuple->xid);
            }
        }
    }

    return found;
}

static void LogUHeapPageShiftBase(Buffer buffer, Page page, bool multi, int64 delta)
{
    if (BufferIsValid(buffer)) {
        // log WAL
        XLogRecPtr recptr;
        XlUHeapBaseShift xlrec;

        START_CRIT_SECTION();
        MarkBufferDirty(buffer);

        xlrec.multi = multi;
        xlrec.delta = delta;

        XLogBeginInsert();
        XLogRegisterData((char *)&xlrec, SizeOfUHeapBaseShift);

        XLogRegisterBuffer(0, buffer, REGBUF_STANDARD);

        recptr = XLogInsert(RM_UHEAP2_ID, XLOG_UHEAP2_BASE_SHIFT);

        PageSetLSN(page, recptr);

        END_CRIT_SECTION();
    }
}

void UHeapPageShiftBase(Buffer buffer, Page page, bool multi, int64 delta)
{
    UHeapPageHeaderData *uheappage = (UHeapPageHeaderData *)page;
    OffsetNumber offnum, maxoff;

    /* base left shift, minimum is 0 */
    if (delta < 0) {
        if (!multi) {
            if ((int64)(uheappage->pd_xid_base + delta) < 0) {
                delta = -(int64)(uheappage->pd_xid_base);
            }
        } else {
            if ((int64)(uheappage->pd_multi_base + delta) < 0) {
                delta = -(int64)(uheappage->pd_multi_base);
            }
        }
    }

    maxoff = UHeapPageGetMaxOffsetNumber(page);
    for (offnum = FirstOffsetNumber; offnum <= maxoff; offnum = OffsetNumberNext(offnum)) {
        RowPtr *rowptr = UPageGetRowPtr(page, offnum);
        UHeapDiskTuple utuple;

        if (!RowPtrIsNormal(rowptr)) {
            continue;
        }

        utuple = (UHeapDiskTuple)UPageGetRowData(page, rowptr);
        if (!SINGLE_LOCKER_XID_IS_EXCL_LOCKED(utuple->flag) && !SINGLE_LOCKER_XID_IS_SHR_LOCKED(utuple->flag) &&
            !UHeapTupleHasMultiLockers(utuple->flag)) {
            utuple->xid = (ShortTransactionId)FrozenTransactionId;
            continue;
        }

        if (TransactionIdIsNormal(utuple->xid) && (!multi || UHeapTupleHasMultiLockers(utuple->flag))) {
            utuple->xid -= delta;
        }
    }

    if (!multi) {
        uheappage->pd_xid_base += delta;
    } else {
        uheappage->pd_multi_base += delta;
    }

    LogUHeapPageShiftBase(buffer, page, multi, delta);
}

static int FreezeSingleUHeapPage(Relation relation, Buffer buffer)
{
    Page page = BufferGetPage(buffer);
    OffsetNumber offnum = InvalidOffsetNumber;
    OffsetNumber maxoff = InvalidOffsetNumber;
    UHeapTupleData utuple;
    int nfrozen = 0;
    OffsetNumber frozen[MaxOffsetNumber];
    TransactionId latestRemovedXid = InvalidTransactionId;
    RelationBuffer relbuf = {relation, buffer};

    // get cutoff xid
    TransactionId oldestXmin = GetOldestXmin(relation, false, true);

    UHeapPagePruneGuts(&relbuf, oldestXmin, InvalidOffsetNumber, 0, false, false, &latestRemovedXid, NULL);

    /*
     * Now scan the page to collect vacuumable items and check for tuples
     * requiring freezing.
     */
    maxoff = UHeapPageGetMaxOffsetNumber(page);

    // cutoff xid for ustore

    for (offnum = FirstOffsetNumber; offnum <= maxoff; offnum = OffsetNumberNext(offnum)) {
        RowPtr *rowptr = UPageGetRowPtr(page, offnum);

        if (!RowPtrIsNormal(rowptr)) {
            continue;
        }

        utuple.disk_tuple = (UHeapDiskTuple)UPageGetRowData(page, rowptr);
        utuple.disk_tuple_size = RowPtrGetLen(rowptr);
        utuple.table_oid = RelationGetRelid(relation);
        UHeapTupleCopyBaseFromPage(&utuple, page);
        // XXX bucket id

        if (UHeapTupleHasMultiLockers(utuple.disk_tuple->flag)) {
            continue;
        }
        TransactionId xid = UHeapTupleGetRawXid(&utuple);
        if (TransactionIdIsNormal(xid) && TransactionIdPrecedes(xid, oldestXmin)) {
            // freeze tuple
            UHeapTupleSetRawXid((&utuple), FrozenTransactionId);

            frozen[nfrozen++] = offnum;
        }
    }

    Assert(nfrozen < maxoff);

    if (nfrozen > 0) {
        START_CRIT_SECTION();

        MarkBufferDirty(buffer);

        if (RelationNeedsWAL(relation)) {
            XLogRecPtr recptr;

            XlUHeapFreeze xlrec;
            xlrec.cutoff_xid = oldestXmin;
            XLogBeginInsert();
            XLogRegisterData((char *)&xlrec, SizeOfUHeapFreeze);

            XLogRegisterBuffer(0, buffer, REGBUF_STANDARD);
            XLogRegisterBufData(0, (char *)frozen, nfrozen * sizeof(OffsetNumber));

            recptr = XLogInsert(RM_UHEAP2_ID, XLOG_UHEAP2_FREEZE);

            PageSetLSN(page, recptr);
        }

        END_CRIT_SECTION();
    }

    return nfrozen;
}

static void UHeapPageShiftBaseAndDirty(const bool needWal, Buffer buffer, Page page, bool multi, int64 delta)
{
    UHeapPageShiftBase(needWal ? buffer : InvalidBuffer, page, multi, delta);
    MarkBufferDirty(buffer);
}

bool UHeapPagePrepareForXid(Relation relation, Buffer buffer, TransactionId xid, bool pageReplication, bool multi)
{
    Page page = BufferGetPage(buffer);
    TransactionId base = 0;
    ShortTransactionId min = 0;
    ShortTransactionId max = 0;
    bool needWal = pageReplication ? false : RelationNeedsWAL(relation);

    UHeapPageHeaderData *uheappage = (UHeapPageHeaderData *)page;

    /*
     * if the first change to pd_xid_base or pd_multi_base fails ,
     * will attempt to freeze this page.
     */
    for (int i = 0; i <= 1; i++) {
        base = multi ? uheappage->pd_multi_base : uheappage->pd_xid_base;

        /* We fit the current base xid */
        if (xid >= base + FirstNormalTransactionId && xid <= base + MaxShortTransactionId) {
            return false;
        }

        /* No items on the page */
        if (!UHeapPageXidMinMax(page, multi, &min, &max)) {
            int64 delta = (xid - FirstNormalTransactionId) - multi ? uheappage->pd_multi_base : uheappage->pd_xid_base;
            UHeapPageShiftBaseAndDirty(needWal, buffer, page, multi, delta);
            return false;
        }

        /* Can we just shift base on the page */
        if (xid < base + FirstNormalTransactionId) {
            int64 freeDelta = MaxShortTransactionId - max;
            int64 requiredDelta = (base + FirstNormalTransactionId) - xid;

            if (requiredDelta <= freeDelta) {
                UHeapPageShiftBaseAndDirty(needWal, buffer, page, multi, -(freeDelta + requiredDelta) / 2);
                return true;
            }
        } else {
            int64 freeDelta = min - FirstNormalTransactionId;
            int64 requiredDelta = xid - (base + MaxShortTransactionId);

            if (requiredDelta <= freeDelta) {
                UHeapPageShiftBaseAndDirty(needWal, buffer, page, multi, (freeDelta + requiredDelta) / 2);
                return true;
            }
        }

        if (i == 0) {
            /* Have to try freezing the page... */
            (void)FreezeSingleUHeapPage(relation, buffer);

            Assert(0);
        }
    }

    if (BufferIsValid(buffer)) {
        UnlockReleaseBuffer(buffer);
    }

    ereport(ERROR, (errcode(ERRCODE_CANNOT_MODIFY_XIDBASE),
        errmsg("Can't fit xid into page. relation \"%s\", now xid is %lu, base is %lu, min is %u, max is %u",
            RelationGetRelationName(relation), xid, base, min, max)));

    return false;
}

static bool IsLockModeConflicting(LockTupleMode mode1, LockTupleMode mode2)
{
    if (mode1 == LockTupleShared) {
        return mode2 == LockTupleExclusive;
    } else {
        return true;
    }
}

/*
 * UHeap equivalent to fastgetattr
 *
 * This is formatted so oddly so that the correspondence to the macro
 * definition in access/htup.h is maintained.
 */
Datum UHeapFastGetAttr(UHeapTuple tup, int attnum, TupleDesc tupleDesc, bool *isnull)
{
    /*
     * These two pointers are used to fetch an attribute pointed to by cached offset (attcacheoff)
     * However, the cached offset of the first attribute is initialized to 0 in certain code paths and
     * we only want to use the cached value with tp if the first byte pointed to by data pointer (tp)
     * is not a pad byte (which is the case for fixed length and pre-aligned variable length attributes).
     * If it is, then we should use the cached value with dp which is the aligned position of tp.
     * See comments in att_align_pointer()
     */
    char *tp = (char *)(tup)->disk_tuple + (tup)->disk_tuple->t_hoff;
    char *dp = ((tupleDesc)->attrs[0]->attlen >= 0) ?
        tp :
        (char *)att_align_pointer(tp, (tupleDesc)->attrs[(attnum)-1]->attalign, -1, tp);

    return ((attnum) > 0 ? ((*(isnull) = false), UHeapDiskTupNoNulls(tup->disk_tuple) ?
        ((tupleDesc)->attrs[(attnum)-1]->attcacheoff >= 0 ?
        (fetchatt((tupleDesc)->attrs[(attnum)-1], (dp + (tupleDesc)->attrs[(attnum)-1]->attcacheoff))
            ) :
        (UHeapNoCacheGetAttr((tup), (attnum), (tupleDesc)))) :
        (att_isnull((attnum)-1, (tup)->disk_tuple->data) ? ((*(isnull) = true), (Datum)NULL) :
                                                           (UHeapNoCacheGetAttr((tup), (attnum), (tupleDesc))))) :
                           ((Datum)NULL));
}

enum UHeapDMLType {
    UHEAP_INSERT,
    UHEAP_UPDATE,
    UHEAP_DELETE,
};

template<UHeapDMLType dmlType> void PgStatCountDML(Relation rel, const bool useInplaceUpdate, const bool slotReused)
{
    switch (dmlType) {
        case UHEAP_INSERT: {
            pgstat_count_heap_insert(rel, 1);
            break;
        }
        case UHEAP_UPDATE: {
            /*
             * As of now, we only count non-inplace updates as that are required to
             * decide whether to trigger autovacuum.
             */
            if (!useInplaceUpdate) {
                /*
                 * If we've performed non-inplace update because of
                 * slotReused optimization, we shouldn't increase the
                 * update stats else, it'll trigger autovacuum unnecessarily. But, we
                 * want to autoanalyze the table periodically.  Hence, we increase the
                 * insert count.
                 */
                if (!slotReused)
                    pgstat_count_heap_update(rel, false);
                else
                    pgstat_count_heap_insert(rel, 1);
            } else {
                PgstatCountHeapUpdateInplace(rel);
            }
            break;
        }
        case UHEAP_DELETE: {
            pgstat_count_heap_delete(rel);
            break;
        }
        default: {
            Assert(0);
            break;
        }
    }
}

template<UHeapDMLType dmlType> void UHeapFinalizeDML(Relation rel, Buffer buffer, Buffer* newbuf, UHeapTuple utuple,
                                                     UHeapTuple tuple, ItemPointer tid, const bool hasTupLock,
                                                     const bool useInplaceUpdate, const bool slotReused)
{
    UndoPersistence persistence = UndoPersistenceForRelation(rel);

    UHeapResetPreparedUndo(persistence, INVALID_ZONE_ID);

    if (newbuf != NULL && *newbuf != buffer) {
        UnlockReleaseBuffer(*newbuf);
    }

    if (dmlType != UHEAP_DELETE) {
        UnlockReleaseBuffer(buffer);
    } else {
        /*
         * For UHEAP_DELETE, we still need tuple data to do UHeapToastDelete,
         * keep the pin to avoid it's evicted earlier
         */
        LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
    }

    PgStatCountDML<dmlType>(rel, useInplaceUpdate, slotReused);

    switch (dmlType) {
        case UHEAP_INSERT:
        case UHEAP_UPDATE: {
            if (tuple != utuple) {
                utuple->ctid = tuple->ctid;
                UHeapFreeTuple(tuple);
            }
            break;
        }
        case UHEAP_DELETE: {
            if (UHeapTupleHasExternal(utuple)) {
                UHeapToastDelete(rel, utuple);
            }
            ReleaseBuffer(buffer);
            break;
        }
        default: {
            Assert(0);
            break;
        }
    }

    if (hasTupLock) {
        UnlockTuple(rel, tid, ExclusiveLock);
    }
}

static void UHeapPagePruneFSM(Relation relation, Buffer buffer, TransactionId fxid, Page page, BlockNumber blkno)
{
    bool hasPruned = UHeapPagePruneOptPage(relation, buffer, fxid);
#ifdef DEBUG_UHEAP
    UHEAPSTAT_COUNT_OP_PRUNEPAGE(del, 1);
    if (hasPruned) {
        UHEAPSTAT_COUNT_OP_PRUNEPAGE_SUC(del, 1);
    }
#endif

    if (hasPruned) {
        Size freespace = PageGetUHeapFreeSpace(page);
        double thres = RelationGetTargetPageFreeSpacePrune(relation, HEAP_DEFAULT_FILLFACTOR);
        double prob = FSM_UPDATE_HEURISTI_PROBABILITY * freespace / thres;
        RecordPageWithFreeSpace(relation, blkno, freespace);
        if (rand() % 100 >= 100.0 - prob * 100.0) {
#ifdef DEBUG_UHEAP
            UHEAPSTAT_COUNT_OP_PRUNEPAGE_SPC(del, freespace);
#endif
            UpdateFreeSpaceMap(relation, blkno, blkno, freespace, false);
        }
    }
}

Oid UHeapInsert(RelationData *rel, UHeapTupleData *utuple, CommandId cid, BulkInsertState bistate)
{
    Page page;
    bool lockReacquired = false;
    int tdSlot = InvalidTDSlotId;
    Buffer buffer = InvalidBuffer;
    UndoRecPtr prevUrecptr = INVALID_UNDO_REC_PTR;
    UndoRecPtr urecPtr = INVALID_UNDO_REC_PTR;
    undo::XlogUndoMeta xlum;
    UHeapPageHeaderData *phdr = NULL;
    UHeapTuple tuple;
    BlockNumber blkno = 0;
    TransactionId minXidInTDSlots = InvalidTransactionId;
    uint16 lower;
    bool switchBuf = false;
    bool aggressiveTDSearch = false;
    BlockNumber firstBlock = InvalidBlockNumber;

    WHITEBOX_TEST_STUB(UHEAP_INSERT_FAILED, WhiteboxDefaultErrorEmit);
    if (utuple == NULL) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("The insert tuple is NULL")));
    }
    Assert(utuple->tupTableType == UHEAP_TUPLE);
    TransactionId fxid = GetTopTransactionId();

    /* Prepare the tuple for insertion */
    tuple = UHeapPrepareInsert(rel, utuple, 0);

    /* Prepare Undo record before buffer lock since undo record length is fixed */
    UndoPersistence persistence = UndoPersistenceForRelation(rel);
    Oid relOid = RelationIsPartition(rel) ? rel->parentId : RelationGetRelid(rel);
    Oid partitionOid = RelationIsPartition(rel) ? RelationGetRelid(rel) : InvalidOid;
    urecPtr = UHeapPrepareUndoInsert(relOid, partitionOid, RelationGetRelFileNode(rel),
        RelationGetRnodeSpace(rel), persistence, InvalidBuffer, fxid, cid,
        INVALID_UNDO_REC_PTR, INVALID_UNDO_REC_PTR, InvalidBlockNumber, NULL, &xlum);

    UHeapResetWaitTimeForTDSlot();
#ifdef DEBUG_UHEAP
    UHEAPSTAT_COUNT_DML();
#endif

    /* Get buffer page from buffer pool and reserve TD slot */
reacquire_buffer:
    buffer = RelationGetBufferForUTuple(rel, tuple->disk_tuple_size, InvalidBuffer, 0, bistate, switchBuf);

    Assert(buffer != InvalidBuffer);

    /*
     * Do aggressive TD slot search after switching buffer
     * and somehow ended up at the first block.
    */
    BlockNumber currBlock = BufferGetBlockNumber(buffer);
    if (firstBlock == InvalidBlockNumber) {
        firstBlock = currBlock;
    } else if (switchBuf && firstBlock == currBlock) {
        aggressiveTDSearch = true;
    }

    page = BufferGetPage(buffer);
    Assert(PageIsValid(page));
    phdr = (UHeapPageHeaderData *)page;
    ereport(DEBUG5, (errmsg("Ins1: Rel: %s, Buf: %d, Space: %d, tuplen: %d",
        RelationGetRelationName(rel), buffer, phdr->pd_upper - phdr->pd_lower, tuple->disk_tuple_size)));

    lower = phdr->pd_lower;

    tdSlot = UHeapPageReserveTransactionSlot(rel, buffer, fxid, &prevUrecptr,
        &lockReacquired, InvalidBuffer, NULL, &minXidInTDSlots, aggressiveTDSearch);

    /*
     * It is possible that available space on the page changed
     * as part of TD reservation operation. If so, go back and reacquire the buffer.
     */
    if (lockReacquired || (lower < phdr->pd_lower)) {
        UnlockReleaseBuffer(buffer);
        switchBuf = false;
        goto reacquire_buffer;
    }

    if (tdSlot == InvalidTDSlotId) {
        UnlockReleaseBuffer(buffer);
        UHeapSleepOrWaitForTDSlot(minXidInTDSlots, fxid, true);
        // cant switch buffer anymore to avoid bouncing between blocks
        switchBuf = !aggressiveTDSearch;
        goto reacquire_buffer;
    }

    ereport(DEBUG5, (errmsg("Ins2: Rel: %s, Buf: %d, Space: %d, tuplen: %d",
        RelationGetRelationName(rel), buffer, phdr->pd_upper - phdr->pd_lower, tuple->disk_tuple_size)));

    UHeapPagePruneFSM(rel, buffer, fxid, page, blkno);

    /* transaction slot must be reserved before adding tuple to page */
    Assert(tdSlot != InvalidTDSlotId);

    /*
     * See heap_insert to know why checking conflicts is important before
     * actually inserting the tuple.
     */
    CheckForSerializableConflictIn(rel, NULL, InvalidBuffer);

    /* Prepare Undo record */
    UndoRecord *urec = u_sess->ustore_cxt.undo_records[0];
    urec->SetBlkprev(prevUrecptr);
    urec->SetBlkno(BufferGetBlockNumber(buffer));
#ifdef USE_ASSERT_CHECKING
    CheckTupleValidity(rel, tuple);
#endif

    /* No ereport(ERROR) from here till changes are logged */
    START_CRIT_SECTION();

    UHeapTupleHeaderSetTDSlot(tuple->disk_tuple, tdSlot);
    UHeapTupleHeaderSetLockerTDSlot(tuple->disk_tuple, InvalidTDSlotId);

    UHeapTupleSetRawXid(tuple, FrozenTransactionId);

    /* Put utuple into buffer page */
    RelationPutUTuple(rel, buffer, tuple);

    UHeapRecordPotentialFreeSpace(rel, buffer, -1 * SHORTALIGN(tuple->disk_tuple_size));

    /* Update the UndoRecord now that we know where the tuple is located on the Page */
    UndoRecord *undorec = (*u_sess->ustore_cxt.urecvec)[0];
    Assert(undorec->Blkno() == ItemPointerGetBlockNumber(&(tuple->ctid)));
    undorec->SetOffset(ItemPointerGetOffsetNumber(&(tuple->ctid)));

    /* Insert the Undo record into the undo store */
    InsertPreparedUndo(u_sess->ustore_cxt.urecvec);

    UndoRecPtr oldPrevUrp = GetCurrentTransactionUndoRecPtr(persistence);
    SetCurrentTransactionUndoRecPtr(urecPtr, persistence);

    UHeapPageSetUndo(buffer, tdSlot, fxid, urecPtr);

    MarkBufferDirty(buffer);

    undo::PrepareUndoMeta(&xlum, persistence, u_sess->ustore_cxt.urecvec->LastRecord(),
        u_sess->ustore_cxt.urecvec->LastRecordSize());
    undo::UpdateTransactionSlot(fxid, &xlum, u_sess->ustore_cxt.urecvec->FirstRecord(), persistence);
    /* Generate xlog, insert xlog and set page lsn */
    if (RelationNeedsWAL(rel)) {
        UHeapWALInfo insWalInfo = { 0 };
        uint8 xlUndoHeaderFlag = 0;
        TransactionId currentXid = InvalidTransactionId;

        if (prevUrecptr != INVALID_UNDO_REC_PTR) {
            xlUndoHeaderFlag |= XLOG_UNDO_HEADER_HAS_BLK_PREV;
        }
        if ((urec->Uinfo() & UNDO_UREC_INFO_TRANSAC) != 0) {
            xlUndoHeaderFlag |= XLOG_UNDO_HEADER_HAS_PREV_URP;
        }
        if (RelationIsPartition(rel)) {
            xlUndoHeaderFlag |= XLOG_UNDO_HEADER_HAS_PARTITION_OID;
        }
        if (IsSubTransaction() && RelationIsLogicallyLogged(rel)) {
            xlUndoHeaderFlag |= XLOG_UNDO_HEADER_HAS_CURRENT_XID;
            currentXid = GetCurrentTransactionId();
        }

        /* insert operation and page information */
        insWalInfo.buffer = buffer;
        insWalInfo.utuple = tuple;
        insWalInfo.partitionOid = partitionOid;
        insWalInfo.urecptr = urecPtr;
        insWalInfo.blkprev = prevUrecptr;
        insWalInfo.prevurp = oldPrevUrp;
        insWalInfo.flag = xlUndoHeaderFlag;
        insWalInfo.xid = currentXid;

        /* undo meta information */
        insWalInfo.hZone = (undo::UndoZone *)g_instance.undo_cxt.uZones[t_thrd.undo_cxt.zids[persistence]];
        Assert(insWalInfo.hZone != NULL);

        insWalInfo.xlum = &xlum;

        /* do the actual logging */
        LogUHeapInsert(&insWalInfo, rel);
    }
    undo::FinishUndoMeta(&xlum, persistence);

    END_CRIT_SECTION();
    /* Clean up */
    UHeapFinalizeDML<UHEAP_INSERT>(rel, buffer, NULL, utuple, tuple, NULL, false, false, false);

    return InvalidOid;
}

static TransactionId UHeapFetchInsertXidGuts(UndoRecord *urec, UHeapTupleTransInfo uheapinfo, const UHeapTuple uhtup,
                                             Buffer buffer, UHeapDiskTupleData hdr)
{
    int tdId = InvalidTDSlotId;
    BlockNumber blk = ItemPointerGetBlockNumber(&uhtup->ctid);
    OffsetNumber offnum = ItemPointerGetOffsetNumber(&uhtup->ctid);

    while (true) {
        urec->Reset(uheapinfo.urec_add);
        int rc = FetchUndoRecord(urec, InplaceSatisfyUndoRecord, blk, offnum, uheapinfo.xid);
        if (rc == UNDO_RET_FAIL) {
            /*
             * Undo record could be null only when it's undo log is/about to
             * be discarded. We cannot use any assert for checking is the log
             * is actually discarded, since UndoFetchRecord can return NULL
             * for the records which are not yet discarded but are about to be
             * discarded.
             */
            return FrozenTransactionId;
        }

        /*
         * If we have valid undo record, then check if we have reached the
         * insert log and return the corresponding transaction id.
         */
        if (urec->Utype() == UNDO_INSERT || urec->Utype() == UNDO_MULTI_INSERT ||
            urec->Utype() == UNDO_INPLACE_UPDATE) {
            return urec->Xid();
        }

        tdId = UpdateTupleHeaderFromUndoRecord(urec, &hdr, BufferGetPage(buffer));

        uheapinfo.xid = urec->OldXactId();
        uheapinfo.urec_add = urec->Blkprev();

        if (!IS_VALID_UNDO_REC_PTR(uheapinfo.urec_add)) {
            return FrozenTransactionId;
        }

        /*
         * Change the undo chain if the undo tuple is stamped with the
         * different transaction slot.
         */
        if (tdId != uheapinfo.td_slot) {
            UHeapUpdateTDInfo(tdId, buffer, offnum, &uheapinfo);
        }
    }
}

TransactionId UHeapFetchInsertXid(UHeapTuple uhtup, Buffer buffer)
{
    TransactionId result;
    UHeapDiskTupleData hdr;
    UHeapTupleTransInfo uheapinfo;

    Assert(uhtup->tupTableType == UHEAP_TUPLE);

    uheapinfo.urec_add = INVALID_UNDO_REC_PTR;
    uheapinfo.td_slot = uhtup->disk_tuple->td_id;

    GetTDSlotInfo(buffer, uheapinfo.td_slot, &uheapinfo);
    errno_t rc = memcpy_s(&hdr, SizeOfUHeapDiskTupleData, uhtup->disk_tuple, SizeOfUHeapDiskTupleData);
    securec_check_c(rc, "\0", "\0");
    uheapinfo.xid = InvalidTransactionId;

    UndoRecord *urec = New(CurrentMemoryContext) UndoRecord();

    result = UHeapFetchInsertXidGuts(urec, uheapinfo, uhtup, buffer, hdr);

    DELETE_EX(urec);

    return result;
}


void RelationPutUTuple(Relation relation, Buffer buffer, UHeapTupleData *tuple)
{
    OffsetNumber offNum = InvalidOffsetNumber;
    UHeapBufferPage bufpage = {buffer, NULL};

    offNum =
        UPageAddItem(relation, &bufpage, (Item)tuple->disk_tuple, tuple->disk_tuple_size, InvalidOffsetNumber, false);
    if (offNum == InvalidOffsetNumber)
        elog(PANIC, "failed to add tuple to page");
    ItemPointerSet(&(tuple->ctid), BufferGetBlockNumber(buffer), offNum);
}

UHeapTuple UHeapPrepareInsert(Relation rel, UHeapTupleData *tuple, int options)
{
    Assert(tuple->tupTableType == UHEAP_TUPLE);

    tuple->disk_tuple->flag &= ~UHEAP_VIS_STATUS_MASK;
    tuple->disk_tuple->td_id = UHEAPTUP_SLOT_FROZEN;
    tuple->disk_tuple->locker_td_id = UHEAPTUP_SLOT_FROZEN;
    tuple->table_oid = RelationGetRelid(rel);

    if (rel->rd_rel->relkind != RELKIND_RELATION) {
        /* toast table entries should never be recursively toasted */
        Assert(!UHeapTupleHasExternal(tuple));
        return tuple;
    } else if (UHeapTupleHasExternal(tuple) || tuple->disk_tuple_size > UTOAST_TUPLE_THRESHOLD) {
        /* Toast insert or update */
        return UHeapToastInsertOrUpdate(rel, tuple, NULL, options);
    } else {
        return tuple;
    }
}

/*
 * ComputeNewXidInfomask - Given the old values of tuple header's infomask,
 * compute the new values for tuple header which includes lock mode, new
 * infomask and transaction slot.
 *
 * We don't clear the multi lockers bit in this function as for that we need
 * to ensure that all the lockers are gone.  Unfortunately, it is not easy to
 * do that as we need to traverse all the undo chains for the current page to
 * ensure the same and doing it here which is quite common code path doesn't
 * seem advisable.  We clear this bit lazily when we detect the conflict and
 * we anyway need to traverse the undo chains for the page.
 *
 * We ensure that the tuple always point to the transaction slot of latest
 * inserter/updater except for cases where we lock first and then update the
 * tuple (aka locks via EvalPlanQual mechanism).  This is because for visibility
 * checks, we only need inserter/updater's xact information.  Keeping their
 * slot on the tuple avoids the overheads of fetching xact information from
 * undo during visibility checks.  Also, note that the latest inserter/updater
 * can be an aborted transaction whose rollback actions are still pending.
 *
 * For example, say after a committed insert/update, a new request arrives to
 * lock the tuple in key share mode, we will keep the inserter's/updater's slot
 * on the tuple and set the multi-locker and key-share bit.  If the inserter/
 * updater is already known to be having a frozen slot (visible to every one),
 * we will set the key-share locker bit and the tuple will indicate a frozen
 * slot.  Similarly, for a new updater, if the tuple has a single locker, then
 * the undo will have a frozen tuple and for multi-lockers, the undo of updater
 * will have previous inserter/updater slot; in both cases the new tuple will
 * point to the updaters slot.  Now, the rollback of a single locker will set
 * the frozen slot on tuple and the rollback of multi-locker won't change slot
 * information on tuple.  We don't want to keep the slot of locker on the
 * tuple as after rollback, we will lose track of last updater/inserter.
 *
 * When we are locking for the purpose of updating the tuple, we don't need
 * to preserve previous updater's information and we also keep the latest
 * slot on tuple.  This is only true when there are no previous lockers on
 * the tuple.
 */
static void ComputeNewXidInfomask(UHeapTuple uhtup, Buffer buf, TransactionId tupXid, int tupTdSlot,
    uint16 oldInfomask, TransactionId addToXid, int tdSlot, TransactionId singleLockerXid, LockTupleMode mode,
    LockOper lockoper, uint16 *resultInfomask, int *resultTdSlot)
{
    int newTdSlot = tdSlot;
    uint16 newInfomask = 0;
    bool oldTupleHasUpdate = false;
    bool transactionIdisInProgressForTupXid = false;
    bool isUpdate = (lockoper == ForUpdate || lockoper == LockForUpdate);
    uint32 needSync = 0;

    Assert(TransactionIdIsValid(addToXid));

    if (lockoper == ForUpdate && TransactionIdIsCurrentTransactionId(singleLockerXid))
        mode = LockTupleExclusive;
    else if (UHeapTupleHasMultiLockers(oldInfomask)) {
        UGetMultiLockInfo(oldInfomask, tupXid, tupTdSlot, addToXid, &newInfomask, &newTdSlot, &mode,
            &oldTupleHasUpdate, lockoper);
    } else {
    restart:
        transactionIdisInProgressForTupXid = TransactionIdIsInProgress(tupXid, &needSync, true, false, true, false);
        /* Since recording csnlog and clog before removing xid from procarray, need sync until proc remove the according
         * xid. In case that we treat the tupXid is still in progress */
        if (needSync) {
            needSync = 0;
            SyncLocalXidWait(tupXid);
            goto restart;
        }
        if ((IsUHeapTupleModified(oldInfomask) && transactionIdisInProgressForTupXid)) {
            UGetMultiLockInfo(oldInfomask, tupXid, tupTdSlot, addToXid, &newInfomask, &newTdSlot, &mode,
                &oldTupleHasUpdate, lockoper);
        } else if (!isUpdate && TransactionIdIsInProgress(singleLockerXid, NULL, true, false, true)) {
            LockTupleMode oldMode;

            /*
             * When there is a single in-progress locker on the tuple and previous
             * inserter/updater became all visible, we've to set multi-locker flag
             * and highest lock mode. If current transaction tries to reacquire a
             * lock, we don't set multi-locker flag.
             */
            Assert(UHEAP_XID_IS_LOCKED_ONLY(oldInfomask));
            if (singleLockerXid != addToXid) {
                elog(PANIC, "Set infomask UHEAP_MULTI_LOCKERS."); // not fall through here in ustore
                newInfomask |= UHEAP_MULTI_LOCKERS;
                newTdSlot = tupTdSlot;
            }

            oldMode = GetOldLockMode(oldInfomask);
            /* Acquire the strongest of both. */
            if (mode < oldMode)
                mode = oldMode;

            /* Keep the old tuple slot as it is */
            newTdSlot = tupTdSlot;
        } else if (!isUpdate && transactionIdisInProgressForTupXid) {
            /*
             * Normally if the tuple is not modified and the current transaction
             * is in progress, the other transaction can't lock the tuple except
             * itself.
             *
             * However, this can happen while locking the updated tuple chain.  We
             * keep the transaction slot of original tuple as that will allow us
             * to check the visibility of tuple by just referring the current
             * transaction slot.
             */
            Assert((tupXid == addToXid));

            if (tupXid != addToXid) {
                elog(PANIC, "Set infomask UHEAP_MULTI_LOCKERS."); // not fall through here in ustore
                newInfomask |= UHEAP_MULTI_LOCKERS;
            }

            newTdSlot = tupTdSlot;
        } else if (!isUpdate && tupTdSlot == UHEAPTUP_SLOT_FROZEN) {
            /*
             * It's a frozen update or insert, so the locker must not change the
             * slot on a tuple.  The lockmode to be used on tuple is computed
             * below. There could be a single committed/aborted locker
             * (multilocker case is handled in the first condition). In that case,
             * we can ignore the locker. If the locker is still in progress, it'll
             * be handled in above case.
             */
            newTdSlot = UHEAPTUP_SLOT_FROZEN;
        } else if (!isUpdate && !UHEAP_XID_IS_LOCKED_ONLY(oldInfomask) && tupTdSlot != UHEAPTUP_SLOT_FROZEN) {
            /*
             * It's a committed update/insert or an aborted update whose rollback
             * action is still pending, so we gotta preserve him as updater of the
             * tuple.  Also, indicate that tuple has multiple lockers.
             *
             * Note that tuple xid could be invalid if the undo records
             * corresponding to the tuple transaction is discarded.  In that case,
             * it can be considered as committed.
             */
            elog(PANIC, "Set infomask UHEAP_MULTI_LOCKERS."); // not fall through here in ustore
            newInfomask |= UHEAP_MULTI_LOCKERS;
            oldTupleHasUpdate = true;

            if (UHeapTupleIsInPlaceUpdated(oldInfomask))
                newInfomask |= UHEAP_INPLACE_UPDATED;
            else if (UHeapTupleIsUpdated(oldInfomask))
                newInfomask |= UHEAP_UPDATED;
            else {
                /* This is a freshly inserted tuple. */
                oldTupleHasUpdate = false;
            }

            if (!oldTupleHasUpdate) {
                /*
                 * This is a freshly inserted tuple, allow to set the requested
                 * lock mode on tuple.
                 */
            } else {
                LockTupleMode oldMode;

                if (UHEAP_XID_IS_EXCL_LOCKED(oldInfomask))
                    oldMode = LockTupleExclusive;
                else if (UHEAP_XID_IS_NOKEY_EXCL_LOCKED(oldInfomask))
                    oldMode = LockTupleNoKeyExclusive;
                else {
                    /*
                     * Tuple must not be locked in any other mode as we are here
                     * because either the tuple is updated or inserted and the
                     * corresponding transaction is committed.
                     */
                    Assert(!(UHEAP_XID_IS_KEYSHR_LOCKED(oldInfomask) || UHEAP_XID_IS_SHR_LOCKED(oldInfomask)));

                    oldMode = LockTupleNoKeyExclusive;
                }

                if (mode < oldMode)
                    mode = oldMode;
            }

            newTdSlot = tupTdSlot;
        } else if (!isUpdate && UHEAP_XID_IS_LOCKED_ONLY(oldInfomask) && tupTdSlot != UHEAPTUP_SLOT_FROZEN) {
            LockTupleMode oldMode;

            /*
             * This case arises for committed/aborted non-inplace updates where
             * the newly inserted tuple is marked as locked-only, but multi-locker
             * bit is not set.
             *
             * Note that tuple xid could be invalid if the undo records
             * corresponding to the tuple transaction is discarded.  In that case,
             * it can be considered as committed.
             */
            elog(PANIC, "Set infomask UHEAP_MULTI_LOCKERS."); // not fall through here in ustore
            newInfomask |= UHEAP_MULTI_LOCKERS;
            /* The tuple is locked-only. */
            Assert(!(oldInfomask & (UHEAP_DELETED | UHEAP_UPDATED | UHEAP_INPLACE_UPDATED)));
            oldMode = GetOldLockMode(oldInfomask);
            /* Acquire the strongest of both. */
            if (mode < oldMode)
                mode = oldMode;

            /* Keep the old tuple slot as it is */
            newTdSlot = tupTdSlot;
        } else if (isUpdate && TransactionIdIsValid(singleLockerXid) && !TransactionIdDidCommit(singleLockerXid)) {
            LockTupleMode oldMode;

            /*
             * There can be a non-conflicting in-progress key share locker on the
             * tuple and we want to update the tuple in no-key exclusive mode.  In
             * that case, we should set the multilocker flag as well.
             *
             * Note that, the single locker xid can be aborted whose rollback
             * actions are still pending.  The scenario should be handled in the
             * same way as an in-progress single locker, i.e., we should set the
             * multilocker flag accordingly.  Else, the rollback of single locker
             * might resotre the infomask of the tuple incorrectly.
             */
            Assert(UHEAP_XID_IS_LOCKED_ONLY(oldInfomask));
            if (singleLockerXid != addToXid) {
                elog(PANIC, "Set infomask UHEAP_MULTI_LOCKERS."); // not fall through here in ustore
                newInfomask |= UHEAP_MULTI_LOCKERS;

                /*
                 * If the tuple has multilocker and we're locking the tuple for
                 * update, we insert multilocker type of undo instead of
                 * lock-for-update undo.  For multilocker undo, we keep the old
                 * tuple slot as it is.
                 */
                if (lockoper == LockForUpdate)
                    newTdSlot = tupTdSlot;
            }

            oldMode = GetOldLockMode(oldInfomask);
            if (oldMode == LockTupleExclusive) {
                /*
                 * singleLockerXid is aborted and this xid is from t_xid,
                 * see inplaceheap_lock_tuple_guts
                 * LockOnly exclusive lock doesn't do actual rollback to data page.
                 */
            } else {
                /* Acquire the strongest of both. */
                Assert(singleLockerXid == addToXid || mode > oldMode);
                if (mode < oldMode)
                    mode = oldMode;
            }
        }
    }
    /*
     * For LockOnly mode and LockForUpdate mode with multilocker flag on the
     * tuple, we keep the old transaction slot as it is.  Since we're not
     * changing the xid slot in the tuple, we shouldn't remove the existing
     * (if any) invalid xact flag from the tuple.
     */
    if (!isUpdate || ((lockoper == LockForUpdate) && UHeapTupleHasMultiLockers(newInfomask))) {
        if (UHeapTupleHasInvalidXact(oldInfomask))
            newInfomask |= UHEAP_INVALID_XACT_SLOT;
    }

    if (isUpdate && !UHeapTupleHasMultiLockers(newInfomask)) {
        if (lockoper == LockForUpdate) {
            /*
             * When we are locking for the purpose of updating the tuple, we
             * don't need to preserve previous updater's information.
             */
            newInfomask |= UHEAP_XID_LOCK_ONLY;
            if (mode == LockTupleExclusive)
                newInfomask |= UHEAP_XID_EXCL_LOCK;
            else
                newInfomask |= UHEAP_XID_NOKEY_EXCL_LOCK;
        } else if (mode == LockTupleExclusive)
            newInfomask |= UHEAP_XID_EXCL_LOCK;
    } else {
        if (lockoper != ForUpdate && !oldTupleHasUpdate)
            newInfomask |= UHEAP_XID_LOCK_ONLY;
        switch (mode) {
            case LockTupleKeyShare:
                newInfomask |= UHEAP_XID_KEYSHR_LOCK;
                break;
            case LockTupleShared:
                newInfomask |= UHEAP_XID_SHR_LOCK;
                break;
            case LockTupleNoKeyExclusive:
                newInfomask |= UHEAP_XID_NOKEY_EXCL_LOCK;
                break;
            case LockTupleExclusive:
                newInfomask |= UHEAP_XID_EXCL_LOCK;
                break;
            default:
                elog(ERROR, "invalid lock mode");
        }
    }

    *resultInfomask = newInfomask;

    if (resultTdSlot) {
        *resultTdSlot = newTdSlot;
    }

    /*
     * We store the reserved transaction slot only when we update the tuple.
     * For lock only, we keep the old transaction slot in the tuple.
     */
    Assert(isUpdate || newTdSlot == tupTdSlot);
}

static bool TestPriorXmaxGuts(UHeapTupleTransInfo *tdinfo, const UHeapTuple tuple, UHeapDiskTupleData *tupHdr,
    Buffer buffer, TransactionId priorXmax)
{
    bool valid = false;
    UndoRecord *urec = New(CurrentMemoryContext) UndoRecord();
    ItemPointer tid = &(tuple->ctid);
    BlockNumber  blkno  = ItemPointerGetBlockNumber(tid);
    OffsetNumber offnum = ItemPointerGetOffsetNumber(tid);
    int rc PG_USED_FOR_ASSERTS_ONLY;

    do {
        int prev_trans_slot_id = tdinfo->td_slot;
        Assert(prev_trans_slot_id != UHEAPTUP_SLOT_FROZEN);

        urec->Reset(tdinfo->urec_add);

        rc = FetchUndoRecord(urec, InplaceSatisfyUndoRecord, blkno, offnum, tdinfo->xid);

        // the tuple cannot be all-visible, at least the current snapshot cannot see this tuple
        Assert(rc != UNDO_RET_FAIL);

        tdinfo->td_slot = UpdateTupleHeaderFromUndoRecord(urec, tupHdr, BufferGetPage(buffer));

        if (TransactionIdEquals(priorXmax, urec->Xid())) {
            valid = true;
            break; 
        }

        tdinfo->xid = urec->OldXactId();
        tdinfo->urec_add = urec->Blkprev();
        
        // switch to undo chain of a different TD
        if (prev_trans_slot_id != tdinfo->td_slot) {
            UHeapUpdateTDInfo(tdinfo->td_slot, buffer, offnum, tdinfo);
        }

        // td is reused, then check undo for trans info
        if (UHeapTupleHasInvalidXact(tupHdr->flag)) {
            FetchTransInfoFromUndo(blkno, offnum, tdinfo->xid, tdinfo, NULL);
        }        
    } while (tdinfo->urec_add != 0);

    DELETE_EX(urec);

    return valid;
}

static bool TestPriorXmax(Relation relation, Buffer buffer, Snapshot snapshot, UHeapTuple tuple,
    TransactionId priorXmax, bool lockBuffer, bool keepTup)
{
    Assert(tuple->tupTableType == UHEAP_TUPLE);

    UHeapTuple visibleTup = NULL;
    UHeapDiskTupleData tupHdr;
    ItemPointer tid = &(tuple->ctid);
    OffsetNumber offnum = ItemPointerGetOffsetNumber(tid);
    bool valid = false;
    UHeapTupleTransInfo tdinfo;

    errno_t ret;

    if (lockBuffer) {
        LockBuffer(buffer, BUFFER_LOCK_SHARE);
    }

    UHeapTupleFetch(relation, buffer, offnum, snapshot, &visibleTup, NULL, keepTup);

    if (visibleTup == NULL) {
        if (lockBuffer) {
            LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
        }

        return false;
    }

    UHeapTupleGetTransInfo(buffer, offnum, &tdinfo);

    if (TransactionIdEquals(priorXmax, tdinfo.xid)) {
        valid = true;
        pfree(visibleTup);
        goto cleanup;
    }

    // the latest version might not be updated by priorXmax, so needs to fetch undo to check it
    // we only need tuple header
    ret = memcpy_s(&tupHdr, SizeOfUHeapDiskTupleData, visibleTup->disk_tuple, SizeOfUHeapDiskTupleData);
    securec_check_c(ret, "\0", "\0");
    pfree(visibleTup);

    // follow the undo chain
    tdinfo.xid = InvalidTransactionId;

    valid = TestPriorXmaxGuts(&tdinfo, tuple, &tupHdr, buffer, priorXmax);

cleanup:
    if (lockBuffer) {
        LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
    }

    return valid;
}

static void KeepOrReleaseBuffer(const bool keepBuf, Buffer *buf, Buffer buffer)
{
    if (keepBuf) {
        *buf = buffer;
    } else {
        ReleaseBuffer(buffer);
        *buf = InvalidBuffer;
    }
}

static void UHeapFetchHandleInvalid(UHeapTuple resTup, const bool keepTup, const UHeapTuple pageTup, UHeapTuple tuple)
{
    if (resTup != NULL) {
        Assert(keepTup);
        if (pageTup != resTup) {
            UHeapCopyTupleWithBuffer(resTup, tuple);
            UHeapFreeTuple(resTup);
        }
    } else {
        tuple->disk_tuple = NULL;
    }
}

bool UHeapFetch(Relation relation, Snapshot snapshot, ItemPointer tid, UHeapTuple tuple, Buffer *buf, bool keepBuf,
    bool keepTup)
{
    UHeapTuple resTup = NULL;
    Buffer buffer = ReadBuffer(relation, ItemPointerGetBlockNumber(tid));
    OffsetNumber offnum = ItemPointerGetOffsetNumber(tid);
    bool isValid;
    ItemPointerData ctid = *tid;

    /* Caller must provide a pre-allocated buffer */
    Assert(tuple && tuple->disk_tuple);

    WHITEBOX_TEST_STUB(UHEAP_FETCH_FAILED, WhiteboxDefaultErrorEmit);

    LockBuffer(buffer, BUFFER_LOCK_SHARE);
    Page page = BufferGetPage(buffer);

    // check out-of-range items
    if (offnum < FirstOffsetNumber || offnum > UHeapPageGetMaxOffsetNumber(page)) {
        LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

        KeepOrReleaseBuffer(keepBuf, buf, buffer);

        tuple->disk_tuple = NULL;
        return false;
    }

    RowPtr *rp = UPageGetRowPtr(page, offnum);
    UHeapTuple pageTup = (RowPtrIsNormal(rp)) ? UHeapGetTuple(relation, buffer, offnum, tuple) : NULL;

    isValid = UHeapTupleFetch(relation, buffer, offnum, snapshot, &resTup, &ctid, keepTup, NULL, NULL, &pageTup);

    if (resTup != NULL)
        Assert(resTup->tupTableType == UHEAP_TUPLE);

    if (ItemPointerIsValid(&ctid) && (snapshot == SnapshotAny || !isValid)) {
        *tid = ctid; // the new ctid
    }

    LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
    if (resTup) {
        resTup->xc_node_id = u_sess->pgxc_cxt.PGXCNodeIdentifier;
    }

    if (isValid) {
        *buf = buffer;
        if (pageTup != resTup) {
            UHeapCopyTupleWithBuffer(resTup, tuple);
            UHeapFreeTuple(resTup);
        }

        return true;
    }

    // tuple is invalid
    KeepOrReleaseBuffer(keepBuf, buf, buffer);
    UHeapFetchHandleInvalid(resTup, keepTup, pageTup, tuple);

    return false;
}

bool UHeapFetchRow(Relation relation, ItemPointer tid, Snapshot snapshot, TupleTableSlot *slot, UHeapTuple utuple)
{
    Buffer buffer;

    ExecClearTuple(slot);

    if (UHeapFetch(relation, snapshot, tid, utuple, &buffer, false, false)) {
        ExecStoreTuple(UHeapCopyTuple(utuple), slot, InvalidBuffer, true);
        ReleaseBuffer(buffer);

        return true;
    }

    return false;
}

/*
 * Similar to EvalPlanQualFetch for heap
 */

UHeapTuple UHeapLockUpdated(CommandId cid, Relation relation,
                           LockTupleMode lock_mode, ItemPointer tid,
                           TransactionId priorXmax, Snapshot snapshot, bool isSelectForUpdate)
{
    SnapshotData snapshotDirty;
    snapshotDirty.satisfies = SNAPSHOT_DIRTY;
    snapshotDirty.xmax = InvalidTransactionId;
    TM_Result result;
    UHeapTuple copyTuple = NULL;
    TM_FailureData tmfd;
    Buffer buffer = InvalidBuffer;
    bool eval = false;
    UHeapTupleData utuple;

    union {
        UHeapDiskTupleData hdr;
        char data[MaxPossibleUHeapTupleSize];
    } tbuf;

    errno_t errorno = EOK;
    errorno = memset_s(&tbuf, sizeof(tbuf), 0, sizeof(tbuf));
    securec_check(errorno, "\0", "\0");
    utuple.disk_tuple = &(tbuf.hdr);

    for (;;) {
        bool fetched = false;

        Assert(utuple.tupTableType == UHEAP_TUPLE);

        fetched = UHeapFetch(relation, &snapshotDirty, tid, &utuple, &buffer, true, true);
        Buffer bufferFromFetch PG_USED_FOR_ASSERTS_ONLY = buffer;

        // buffer lock is released
        if (fetched) {
            // 1. verify priorXmax this step is similar to heap, but it needs to check TD or
            //    undo because buffer lock is release after UHeapFetch gets a valid tuple.
            if (!TestPriorXmax(relation, buffer, &snapshotDirty, &utuple, priorXmax, true, true)) {
                goto out;
            }
            // 2. wait the current updater if any  to terminate and then refetch the latest one
            if (snapshotDirty.subxid != InvalidSubTransactionId && TransactionIdIsValid(snapshotDirty.xmax)) {
                ReleaseBuffer(buffer);
                SubXactLockTableWait(snapshotDirty.xmax, snapshotDirty.subxid);
                continue;
            } else if (TransactionIdIsValid(snapshotDirty.xmax)) {
                ReleaseBuffer(buffer);
                XactLockTableWait(snapshotDirty.xmax);
                continue;
            }

            /*
             * If tuple was inserted by our own transaction, we have to
             * do a CID check. If inserted by our own command ID,
             * then we cannot see the tuple, so we should ignore it.
             * Otherwise UHeapLockTuple() will throw an error, and so
             * would any later attempt to update or delete the tuple.
             */
            if (TransactionIdIsCurrentTransactionId(priorXmax)) {
                LockBuffer(buffer, BUFFER_LOCK_SHARE);

                CommandId tupCid = UHeapTupleGetCid(&utuple, buffer);
                if (tupCid >= cid) {
                    LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
                    goto out;
                }
                LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
            }

            utuple.ctid = *tid;

            // 4. try to lock it
            result = UHeapLockTuple(relation, &utuple, &buffer,
                cid, // estate->es_output_cid,
                lock_mode, false, &tmfd, true, eval,
                snapshot, // estate->es_snapshot,
                isSelectForUpdate);

            ReleaseBuffer(buffer);

            /* Make sure we release the correct buffer later */
            Assert(bufferFromFetch == buffer);

            // 5. handle locking result
            switch (result) {
                case TM_SelfUpdated:
                case TM_SelfModified:
                    Assert(copyTuple == NULL);
                    goto out;

                case TM_Ok:
                    break; // successfully locked, don't release buffer before copy the tuple

                case TM_Updated:
                    if (IsolationUsesXactSnapshot()) {
                        ReleaseBuffer(buffer);
                        ereport(ERROR, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                            errmsg("could not serialize access due to concurrent update")));
                    }

                    if (ItemPointerEquals(&tmfd.ctid, &(utuple.ctid))
                        && !tmfd.in_place_updated_or_locked) {
                        Assert(copyTuple == NULL);
                        goto out;
                    }

                    ReleaseBuffer(buffer);
                    eval = true;

                    /* Fetch the next tid */
                    *tid = tmfd.ctid;
                    priorXmax = tmfd.xmax;
                    continue;

                case TM_Deleted:
                    if (IsolationUsesXactSnapshot()) {
                        ReleaseBuffer(buffer);
                        ereport(ERROR, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                            errmsg("could not serialize access due to concurrent update")));
                    }

                    goto out;

                default:
                    Assert(0);
            }

            // store the tuple
            copyTuple = UHeapCopyTuple(&utuple);
            break; /* exit the for loop */
        }

        // utuple is null, tuple is deleted
        if (utuple.disk_tuple == NULL) {
            Assert(copyTuple == NULL);
            goto out;
        }

        // validate priorXmax
        if (!TestPriorXmax(relation, buffer, &snapshotDirty, &utuple, priorXmax, true, true)) {
            Assert(copyTuple == NULL);
            goto out;
        }

        // itempointer equals, and didn't pass the snapshotDirty protocol, then it's deleted
        if (ItemPointerEquals(&(utuple.ctid), tid)) {
            Assert(copyTuple == NULL);
            goto out;
        }

        // update priorXmax
        priorXmax = UHeapTupleGetTransXid(&utuple, buffer, true);
        ReleaseBuffer(buffer);
    }

out:
    ReleaseBuffer(buffer);

    /* Return the locked tuple */
    return copyTuple;
}

/*
 * UHeapWait
 * helper function called by UHeapUpdate, UHeapDelete, UHeapLockTuple
 * to check write-write  conflict
 * - if needs recheck, return false
 * - otherwise, return true
 *
 * mode - delete/update/select-for-update => LockTupleExclusive
 * select-for-share                => LockTupleShared
 *
 * We consider about two kind of concurrent modifier on the tuple: 1. updater/deleter 2. locker
 * which is given by (updateXid, updateSubXid) and (lockerXid, lockerSubXid) respectively.
 * updateSubXid and lockerSubXid must be set as InvalidSubTransactionId if the modifier is not a
 * sub-transaction.
 *
 * If any updater/deleter exclusively locks the tuple, there shouldn't exist a valid lockerXid on tuple.
 * So if the lockerXid is valid, that means the updateXid is meaningless for us to wait. The only active
 * modifier can only be the locker.
 */
static bool UHeapWait(Relation relation, Buffer buffer, UHeapTuple utuple, LockTupleMode mode, bool nowait,
    TransactionId updateXid, TransactionId lockerXid, SubTransactionId updateSubXid, SubTransactionId lockerSubXid,
    bool *hasTupLock, bool *multixidIsMySelf)
{
    Assert(utuple->tupTableType == UHEAP_TUPLE);
    uint16 flag = utuple->disk_tuple->flag;

    LockTupleMode curMode = UHEAP_XID_IS_EXCL_LOCKED(flag) ? LockTupleExclusive : LockTupleShared;
    LOCKMODE tupleLockType = (mode == LockTupleShared) ? ShareLock : ExclusiveLock;
    bool isLockSingleLocker = SINGLE_LOCKER_XID_IS_EXCL_LOCKED(utuple->disk_tuple->flag) ||
        SINGLE_LOCKER_XID_IS_SHR_LOCKED(utuple->disk_tuple->flag);
    if (UHeapTupleHasMultiLockers(utuple->disk_tuple->flag)) {
        Assert(curMode == LockTupleShared);

        // if we reach here, the tuple must be visible to us, see details in UHeapTupleSatisfiesUpdate
        if (!IsLockModeConflicting(curMode, mode)) {
            // we can lock the tuple
            return true;
        }

        // xwait is multixid
        MultiXactId xwait = (MultiXactId)UHeapTupleGetRawXid(utuple);

        elog(DEBUG5, "curxid %ld, uheaptuple(flag=%d, tid(%d,%d)), wait multixid = %ld, xid is %d, multibase is %ld",
            GetTopTransactionId(), utuple->disk_tuple->flag, ItemPointerGetBlockNumber(&utuple->ctid),
            ItemPointerGetOffsetNumber(&utuple->ctid), xwait, utuple->disk_tuple->xid, utuple->t_multi_base);

        LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
        if (!(*hasTupLock)) {
            if (nowait) {
                if (!ConditionalLockTuple(relation, &(utuple->ctid), tupleLockType)) {
                    ereport(ERROR, (errcode(ERRCODE_LOCK_NOT_AVAILABLE),
                        errmsg("could not obtain lock on row in relation \"%s\"", RelationGetRelationName(relation))));
                }
            } else {
                LockTuple(relation, &(utuple->ctid), tupleLockType, true);
            }

            *hasTupLock = true;
        }

        // wait multixid
        if (nowait) {
            if (!ConditionalMultiXactIdWait((MultiXactId)xwait, GetMXactStatusForLock(mode, false), NULL)) {
                ereport(ERROR, (errcode(ERRCODE_LOCK_NOT_AVAILABLE),
                    errmsg("could not obtain lock on row in relation \"%s\"", RelationGetRelationName(relation))));
            }
        } else {
            MultiXactIdWait(xwait, GetMXactStatusForLock(mode, false), NULL);
        }

        // reacquire lock
        LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

        // fetch the tuple, note: the tuple could be pruned
        Page page = BufferGetPage(buffer);
        OffsetNumber offnum = ItemPointerGetOffsetNumber(&utuple->ctid);
        RowPtr *rp = UPageGetRowPtr(page, offnum);
        if (RowPtrIsDeleted(rp)) {
            return false;
        }

        utuple->disk_tuple = (UHeapDiskTuple)UPageGetRowData(page, rp);
        utuple->disk_tuple_size = RowPtrGetLen(rp);
        UHeapTupleCopyBaseFromPage(utuple, page);

        // tuple is not modified during waiting and it is lock-only
        if (UHeapTupleHasMultiLockers(utuple->disk_tuple->flag) &&
            TransactionIdEquals(xwait, UHeapTupleGetRawXid(utuple))) {
            /*
             * Clear the multilock flag if multixid is definitely terminated.
             * If the xwait contains current transaction, we should keep the multixid on tuple
             * to protect the tuple from being modified in case the buffer lock is released
             */
            if (!MultiXactIdIsCurrent(xwait)) {
                utuple->disk_tuple->flag &= ~UHEAP_MULTI_LOCKERS;
            } else {
                *multixidIsMySelf = true;
            }
        }

        return false; // recheck
    } else if (TransactionIdIsCurrentTransactionId(updateXid) || TransactionIdIsCurrentTransactionId(lockerXid)) {
        return true;
    } else {
        bool isSubXact = false;

        Assert(TransactionIdIsValid(updateXid) || TransactionIdIsValid(lockerXid));
        if (IsLockModeConflicting(curMode, mode)) {
            uint16 infomask = utuple->disk_tuple->flag;

            /*
             * Wait for the transaction to end. But obtain the
             * tuple lock so we can maintain our priority.
             * Also, release the buffer lock while waiting to avoid deadlock.
             */
            LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

            /*
             * The caller should release the lock.
             * Hence we need to tell them we acquired the lock tuple here.
             */
            if (!(*hasTupLock)) {
                if (nowait) {
                    if (!ConditionalLockTuple(relation, &(utuple->ctid), tupleLockType)) {
                        ereport(ERROR, (errcode(ERRCODE_LOCK_NOT_AVAILABLE), errmsg(
                            "could not obtain lock on row in relation \"%s\"", RelationGetRelationName(relation))));
                    }
                } else {
                    LockTuple(relation, &(utuple->ctid), tupleLockType, true);
                }

                *hasTupLock = true;
            }

            /* Figure out which xid to wait for and wait for it to finish */
            TransactionId topXid = TransactionIdIsValid(lockerXid) ? lockerXid : updateXid;
            SubTransactionId subXid = (InvalidSubTransactionId != lockerSubXid) ? lockerSubXid : updateSubXid;
            elog(DEBUG5, "curxid %ld, uheaptuple(flag=%d, tid(%d,%d)), wait (xid = %ld subxid = %ld)",
                GetTopTransactionId(), infomask, ItemPointerGetBlockNumber(&utuple->ctid),
                ItemPointerGetOffsetNumber(&utuple->ctid), topXid, subXid);

            if (nowait) {
                if (InvalidSubTransactionId != subXid) {
                    if (!ConditionalSubXactLockTableWait(topXid, subXid)) {
                        ereport(ERROR, (errcode(ERRCODE_LOCK_NOT_AVAILABLE), errmsg(
                            "could not obtain lock on row in relation \"%s\"", RelationGetRelationName(relation))));
                    }

                    isSubXact = true;
                } else {
                    if (!ConditionalXactLockTableWait(topXid)) {
                        ereport(ERROR, (errcode(ERRCODE_LOCK_NOT_AVAILABLE), errmsg(
                            "could not obtain lock on row in relation \"%s\"", RelationGetRelationName(relation))));
                    }
                }
            } else {
                if (InvalidSubTransactionId != subXid) {
                    SubXactLockTableWait(topXid, subXid);
                    isSubXact = true;
                } else {
                    XactLockTableWait(topXid, true);
                }
            }

            LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

            // a single locker only transaction shouldn't exec pending undo
            if (!isSubXact && !isLockSingleLocker && TransactionIdIsValid(topXid) && !TransactionIdDidCommit(topXid)) {
                UHeapExecPendingUndoActions(relation, buffer, topXid);
            }

            return false; // recheck
        } else {
            return true;
        }
    }

    return false; // shouldn't reach here
}

/*
 * Callers: UHeapUpdate, UHeapLockTuple
 * This function will do locking, UNDO and WAL logging part.
 */
static void UHeapExecuteLockTuple(Relation relation, Buffer buffer, UHeapTuple utuple, LockTupleMode mode,
    bool clearMultiXact)
{
    Assert(utuple->tupTableType == UHEAP_TUPLE);
    TransactionId xid = InvalidTransactionId;
    TransactionId xidOnTup = InvalidTransactionId;
    TransactionId curxid = InvalidTransactionId;

    if (mode == LockTupleExclusive) {
        xid = GetCurrentTransactionId();
    } else if (mode == LockTupleShared) {
        xidOnTup = UHeapTupleGetRawXid(utuple);
        curxid = GetTopTransactionId();
        if (IsSubTransaction()) {
            curxid = GetCurrentTransactionId();
        }

    }

    START_CRIT_SECTION();

    if (clearMultiXact)
        utuple->disk_tuple->flag &= ~UHEAP_MULTI_LOCKERS;

    uint16 infomask = 0, oldinfomask = utuple->disk_tuple->flag;

    // return new tuple header to caller
    utuple->disk_tuple->flag &= ~UHEAP_LOCK_STATUS_MASK;
    utuple->disk_tuple->flag |= infomask;
    UHeapTupleHeaderClearSingleLocker(utuple->disk_tuple);

    TransactionId xidbase = InvalidTransactionId;
    bool multi = false;

    if (mode == LockTupleExclusive) {
        if (IsSubTransaction()) {
            utuple->disk_tuple->flag |= SINGLE_LOCKER_XID_IS_SUBXACT;
        }
        utuple->disk_tuple->flag |= SINGLE_LOCKER_XID_EXCL_LOCK;
    } else if (mode == LockTupleShared) {
        // Already locked by one transaction in share mode and that xid is still running
        if (SINGLE_LOCKER_XID_IS_SHR_LOCKED(oldinfomask) && TransactionIdIsInProgress(xidOnTup)) {
            // create a multixid
            MultiXactIdSetOldestMember();
            xid = MultiXactIdCreate(xidOnTup, MultiXactStatusForShare, curxid, MultiXactStatusForShare);
            multi = true;
            utuple->disk_tuple->flag |= UHEAP_MULTI_LOCKERS;
            elog(DEBUG5, "locker %ld + locker %ld = multi %ld", curxid, xidOnTup, xid);
        } else if (UHeapTupleHasMultiLockers(oldinfomask)) {
            /*
             * Already locked by multiple transactions
             * expand multixid to contain the current transaction id.
             */
            MultiXactIdSetOldestMember();
            xid = MultiXactIdExpand((MultiXactId)xidOnTup, curxid, MultiXactStatusForShare);
            multi = true;
            utuple->disk_tuple->flag |= UHEAP_MULTI_LOCKERS;
            elog(DEBUG5, "locker %ld + multi %ld = multi %ld", curxid, xidOnTup, xid);
        } else {
            /*
             * There's no lockers on tuple.
             * Mark tuple as locked by one xid.
             */
            xid = curxid;
            utuple->disk_tuple->flag |= SINGLE_LOCKER_XID_SHR_LOCK;
            if (IsSubTransaction()) {
                utuple->disk_tuple->flag |= SINGLE_LOCKER_XID_IS_SUBXACT;
            }
            elog(DEBUG5, "single shared locker %ld", xid);
        }
    } else {
        Assert(0);
    }

    (void)UHeapPagePrepareForXid(relation, buffer, xid, false, multi);
    UHeapTupleCopyBaseFromPage(utuple, BufferGetPage(buffer));
    xidbase = multi ? utuple->t_multi_base : utuple->t_xid_base;
    utuple->disk_tuple->xid = NormalTransactionIdToShort(xidbase, xid);

    MarkBufferDirty(buffer);

    END_CRIT_SECTION();
}

/*
 * IsTupleLockedByUs
 * xid can be an either an updater or locker
 *
 * IF lock is LockForUpdate (i.e. ExecConflictUpdate)
 * should acquire a lock anyway even if curxid is updater or locker from select-from-update
 * IF lock is SELECT-FOR-UPDATE / SELECT-FOR-SHARE
 * should skip locking if curxid is updater or both kinds of locker in the required mode
 */
static bool IsTupleLockedByUs(UHeapTuple utuple, TransactionId xid, LockTupleMode mode)
{
    if (!TransactionIdIsNormal(xid)) {
        return false;
    }

    if (UHeapTupleHasMultiLockers(utuple->disk_tuple->flag)) {
        if (mode == LockTupleShared && MultiXactIdIsCurrent((MultiXactId)utuple->disk_tuple->xid)) {
            return true;
        } else {
            return false;
        }
    } else if (TransactionIdIsCurrentTransactionId(xid)) {
        if (mode == LockTupleShared) {
            if (UHEAP_XID_IS_EXCL_LOCKED(utuple->disk_tuple->flag) ||
                UHEAP_XID_IS_SHR_LOCKED(utuple->disk_tuple->flag)) {
                return true;
            }
        } else {
            Assert(mode == LockTupleExclusive);

            if (UHEAP_XID_IS_EXCL_LOCKED(utuple->disk_tuple->flag)) {
                return true;
            }
        }
    }

    return false;
}


TM_Result UHeapLockTuple(Relation relation, UHeapTuple tuple, Buffer* buffer,
                 CommandId cid, LockTupleMode mode, bool nowait, TM_FailureData *tmfd,
                 bool follow_updates, bool eval, Snapshot snapshot,
                 bool isSelectForUpdate, bool allowLockSelf, bool isUpsert, TransactionId conflictXid)
{
    RowPtr *rp = NULL;
    UHeapTupleData utuple;
    Page           page;
    BlockNumber    blkno;
    OffsetNumber   offnum;
    ItemPointerData         ctid;
    ItemPointer             tid = &tuple->ctid;
    bool           clearMultiXact = false; // shall we clear the multi-xact bit? this is used for shared lockers
    TM_Result      result;
    TransactionId  updateXid      = InvalidTransactionId,
                   lockerXid      = InvalidTransactionId;
    SubTransactionId updateSubXid = InvalidSubTransactionId,
                     lockerSubXid = InvalidSubTransactionId;
    bool           inplaceUpdatedOrLocked = false;
    bool           hasTupLock = false;
    UHeapTupleTransInfo tdinfo;
    bool multixidIsMyself = false;

    if (mode == LockTupleShared && !isSelectForUpdate) {
        ereport(ERROR, (errmsg("UStore only supports share lock from select-for-share statement.")));
    }

    // lock buffer and fetch tuple row pointer
    blkno = ItemPointerGetBlockNumber(tid);
    *buffer = ReadBuffer(relation, blkno);

    WHITEBOX_TEST_STUB(UHEAP_LOCK_TUPLE_FAILED, WhiteboxDefaultErrorEmit);

    LockBuffer(*buffer, BUFFER_LOCK_EXCLUSIVE);

    page = BufferGetPage(*buffer);

    offnum = ItemPointerGetOffsetNumber(tid);
    rp = UPageGetRowPtr(page, offnum);

    UHeapResetWaitTimeForTDSlot();

check_tup_satisfies_update:
    result = UHeapTupleSatisfiesUpdate(relation, snapshot, tid, &utuple, cid, *buffer, &ctid, &tdinfo, &updateSubXid,
        &lockerXid, &lockerSubXid, eval, multixidIsMyself, &inplaceUpdatedOrLocked, allowLockSelf, 
        !isSelectForUpdate, conflictXid, isUpsert);
    updateXid = tdinfo.xid;
    multixidIsMyself = false;

    if (result == TM_Invisible) {
        Assert(0);
    } else if (result == TM_BeingModified || result == TM_Ok) {
#ifdef ENABLE_WHITEBOX
        if (result != TM_Ok) {
            ereport(WARNING, (errmsg("UHeapLockTuple returned %d", result)));
        }
#endif

        bool alreadyLocked = false;
        TransactionId xwait = InvalidTransactionId;

        // make a copy in case buffer lock is released
        UHeapCopyTupleWithBuffer(&utuple, tuple);

        if (!TransactionIdIsValid(lockerXid)) {
            xwait = tdinfo.xid;
        } else {
            xwait = lockerXid;
        }

        /*
         * Check if tuple has already been locked by us in the required mode
         * if LockForUpdate, need to lock the tuple even if the updater is current transaction
         * to ensure tuple can be updated even though cid == curcid
         */
        alreadyLocked = IsTupleLockedByUs(tuple, isSelectForUpdate ? xwait : lockerXid, mode);
        if (alreadyLocked) {
            result = TM_Ok;
            goto cleanup;
        }

        if (result != TM_Ok) {
            // wait for remaining updater/locker to terminate
            if (!UHeapWait(relation, *buffer, &utuple, mode, nowait, updateXid, lockerXid, updateSubXid, lockerSubXid,
                &hasTupLock, &multixidIsMyself)) {
                goto check_tup_satisfies_update;
            }
        }

        result = TM_Ok;
    } else if ((result == TM_Updated && utuple.disk_tuple != NULL) || result == TM_SelfUpdated) {
        UHeapCopyTupleWithBuffer(&utuple, tuple);
    }

   /*
    * We need to re-fetch the row information since it might
    * have changed due to TD extension by the contending transaction
    * for the same page.
    */
    rp = UPageGetRowPtr(page, offnum);
    if (result != TM_Ok) {
        tmfd->in_place_updated_or_locked = inplaceUpdatedOrLocked;

        /*
         * ctid should be pointing to the next tuple in the update chain in
         * case of non-inplace update. Otherwise, it points to the current tuple.
         */
        if (!RowPtrIsDeleted(rp) && UHeapTupleIsMoved(utuple.disk_tuple->flag))
            ItemPointerSetMovedPartitions(&tmfd->ctid);
        else
            tmfd->ctid = ctid;

        if (RowPtrIsDeleted(rp)) {
            tuple->disk_tuple_size = 0; // let caller know this tid is marked deleted
            tuple->table_oid = RelationGetRelid(relation);
            tuple->ctid = utuple.ctid;
        }

        tmfd->xmax = updateXid;

        if (result == TM_SelfModified || result == TM_SelfUpdated)
            tmfd->cmax = tdinfo.cid;
        else
            tmfd->cmax = InvalidCommandId;

        goto cleanup;
    }

    // operate lock, calling UHeapExecuteLockTuple
    // / 1. prepare tuple data
    Assert(!RowPtrIsDeleted(rp));
    utuple.disk_tuple = (UHeapDiskTuple)UPageGetRowData(page, rp);
    utuple.disk_tuple_size = RowPtrGetLen(rp);
    utuple.ctid = *tid;

    /* clear multi-xact when our lock mode is exclusive */
    clearMultiXact = (mode == LockTupleExclusive);
    (void)UHeapExecuteLockTuple(relation, *buffer, &utuple, mode, clearMultiXact);

    // return the locked tuple to caller
    UHeapCopyTupleWithBuffer(&utuple, tuple);

cleanup:
    if (result == TM_Updated && !tmfd->in_place_updated_or_locked && ItemPointerEquals(&tmfd->ctid, &tuple->ctid)) {
        result = TM_Deleted;
    }

    // do some cleaning
    // / 1. unlock buffer
    LockBuffer(*buffer, BUFFER_LOCK_UNLOCK);

    // / 2. unlock tuple if any
    if (hasTupLock)
        UnlockTuple(relation, tid, (mode == LockTupleShared) ? ShareLock : ExclusiveLock);

#ifdef ENABLE_WHITEBOX
    if (result != TM_Ok) {
        ereport(WARNING, (errmsg("UHeapLockTuple returned %d", result)));
    }
#endif

    return result;
}

bool TableFetchAndStore(Relation scanRelation, Snapshot snapshot, Tuple tuple, Buffer *userbuf, bool keepBuf,
    bool keepTup, TupleTableSlot *slot, Relation statsRelation)
{
    if (RelationIsUstoreFormat(scanRelation)) {
        if (UHeapFetch(scanRelation, snapshot, &((UHeapTuple)tuple)->ctid, (UHeapTuple)tuple, 
            userbuf, keepBuf, keepTup)) {

            /*
             * store the scanned tuple in the scan tuple slot of the scan
             * state.  Eventually we will only do this and not return a tuple.
             * Note: we pass 'false' because tuples returned by amgetnext are
             * pointers onto disk pages and were not created with palloc() and
             * so should not be pfree_ext()'d.
             */
            if (slot != NULL) {
                ExecStoreTuple(tuple, slot, InvalidBuffer, false); // tuple to store and the slot to store in.
            }
            return true;
        }
    } else {



        if (heap_fetch(scanRelation, snapshot, (HeapTuple)tuple, userbuf, keepBuf, NULL)) {
            /*
             * store the scanned tuple in the scan tuple slot of the scan
             * state.  Eventually we will only do this and not return a tuple.
             * Note: we pass 'false' because tuples returned by amgetnext are
             * pointers onto disk pages and were not created with palloc() and
             * so should not be pfree_ext()'d.
             */
            if (slot != NULL) {
                ExecStoreTuple(tuple, /* tuple to store */
                    slot,               /* slot to store in */
                    *userbuf,           /* buffer associated with tuple  */
                    false);             /* don't pfree */
            }
            return true;
        }
    }

    return false;
}


TM_Result UHeapDelete(Relation relation, ItemPointer tid, CommandId cid, Snapshot crosscheck, Snapshot snapshot,
    bool wait, TupleTableSlot** oldslot, TM_FailureData *tmfd, bool changingPart, bool allowDeleteSelf)
{
    UHeapTupleData utuple;
    Buffer buffer;
    UndoRecPtr prevUrecptr;
    int transSlotId;
    uint16 newInfomask;
    bool lockReacquired;
    TransactionId fxid = GetTopTransactionId();
    UndoRecPtr urecptr = INVALID_UNDO_REC_PTR;
    undo::XlogUndoMeta xlum;
    ItemPointerData ctid;
    bool inplaceUpdatedOrLocked = false;
    bool hasTupLock = false;
    TransactionId updateXid = InvalidTransactionId;
    TransactionId lockerXid = InvalidTransactionId;
    SubTransactionId updateSubXid = InvalidSubTransactionId;
    SubTransactionId lockerSubXid = InvalidSubTransactionId;
    SubTransactionId subxid = InvalidSubTransactionId;
    TM_Result result;
    UHeapTupleTransInfo tdinfo;
    uint16 tempInfomask, newInfoMask;
    int newTDSlotId;
    StringInfoData undotup;
    bool multixidIsMyself = false;
    TransactionId minXidInTDSlots = InvalidTransactionId;

    Assert(ItemPointerIsValid(tid));
    
    BlockNumber blkno = ItemPointerGetBlockNumber(tid);
    Page page = GetPageBuffer(relation, blkno, buffer);

    WHITEBOX_TEST_STUB(UHEAP_DELETE_FAILED, WhiteboxDefaultErrorEmit);

    LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

    OffsetNumber offnum = ItemPointerGetOffsetNumber(tid);
    RowPtr *rp = UPageGetRowPtr(page, offnum);
    Assert(RowPtrIsNormal(rp) || RowPtrIsDeleted(rp));

    UHeapPagePruneFSM(relation, buffer, fxid, page, blkno);

    UHeapResetWaitTimeForTDSlot();

check_tup_satisfies_update:
    result = UHeapTupleSatisfiesUpdate(relation, snapshot, tid, &utuple, cid, buffer, &ctid, &tdinfo, &updateSubXid,
        &lockerXid, &lockerSubXid, false, multixidIsMyself, &inplaceUpdatedOrLocked, allowDeleteSelf);
    updateXid = tdinfo.xid;
    tmfd->xmin = tdinfo.xid;
    multixidIsMyself = false;

    if (result == TM_Invisible) {
        UnlockReleaseBuffer(buffer);
        ereport(defence_errlevel(), (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg("UHeapDelete: attempted to delete invisible tuple")));
    } else if ((result == TM_BeingModified) && wait) {

#ifdef ENABLE_WHITEBOX
        ereport(WARNING, (errmsg("UHeapDelete returned %d", result)));
#endif

        if (!UHeapWait(relation, buffer, &utuple, LockTupleExclusive, false, updateXid, lockerXid,
            updateSubXid, lockerSubXid, &hasTupLock, &multixidIsMyself)) {
            goto check_tup_satisfies_update;
        }

        result = TM_Ok;
    } else if (result == TM_Updated && utuple.disk_tuple != NULL &&
        UHeapTupleHasMultiLockers(utuple.disk_tuple->flag)) {
        // This may not be valid yet that we do not have a Shared locker at the moment
        Assert(0);
    }

    if (crosscheck != InvalidSnapshot && result == TM_Ok) {
        /* Perform additional check for transaction-snapshot mode RI updates */
        if (!UHeapTupleFetch(relation, buffer, offnum, crosscheck, NULL, NULL, false))
            result = TM_Updated;
    }

    /*
     * We need to re-fetch the row information since it might
     * have changed due to TD extension by the contending transaction
     * for the same page.
     */
    rp = UPageGetRowPtr(page, offnum);
    if (result != TM_Ok) {
        Assert(result == TM_SelfModified || result == TM_SelfUpdated || result == TM_Updated || result == TM_Deleted ||
            result == TM_BeingModified || result == TM_SelfCreated);
        Assert(RowPtrIsDeleted(rp) || IsUHeapTupleModified(utuple.disk_tuple->flag));

        /* Fill in the tmfd that the caller could use */
        /* If item id is deleted, tuple can't be marked as moved. */
        if (!RowPtrIsDeleted(rp) && UHeapTupleIsMoved(utuple.disk_tuple->flag))
            ItemPointerSetMovedPartitions(&tmfd->ctid);
        else
            tmfd->ctid = ctid;

        tmfd->xmax = updateXid;
        tmfd->in_place_updated_or_locked = inplaceUpdatedOrLocked;
        tmfd->cmax = ((result == TM_SelfModified) || (result == TM_SelfUpdated)) ? tdinfo.cid : InvalidCommandId;

        if (result == TM_Updated && !tmfd->in_place_updated_or_locked && ItemPointerEquals(&tmfd->ctid, tid)) {
            result = TM_Deleted;
        }

        UnlockReleaseBuffer(buffer);

        if (hasTupLock)
            UnlockTuple(relation, &(utuple.ctid), ExclusiveLock);

#ifdef ENABLE_WHITEBOX
        ereport(WARNING, (errmsg("UHeapDelete returned %d", result)));
#endif

        return result;
    }

    /*
     * Acquire subtransaction lock, if current transaction is a
     * subtransaction.
     */
    if (IsSubTransaction()) {
        subxid = GetCurrentSubTransactionId();
        SubXactLockTableInsert(subxid);
    }

    transSlotId = UHeapPageReserveTransactionSlot(relation, buffer, fxid,
        &prevUrecptr, &lockReacquired, InvalidBuffer, NULL, &minXidInTDSlots);

    /*
     * We need to re-fetch the row information since it might
     * have changed due to TD extension as part of
     * the above call to UHeapPageReserveTransactionSlot().
     */
    rp = UPageGetRowPtr(page, offnum);

    if (lockReacquired) {
        goto check_tup_satisfies_update;
    }

    if (transSlotId == InvalidTDSlotId) {
        LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
        UHeapSleepOrWaitForTDSlot(minXidInTDSlots, fxid);
        LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
        goto check_tup_satisfies_update;
    }

    /* transaction slot must be reserved before adding tuple to page */
    Assert(transSlotId != InvalidTDSlotId);

    /*
     * It's possible that tuple slot is now marked as frozen. Hence, we
     * refetch the tuple here.
     */
    Assert(!RowPtrIsDeleted(rp));
    utuple.disk_tuple = (UHeapDiskTuple)UPageGetRowData(page, rp);
    utuple.disk_tuple_size = RowPtrGetLen(rp);

    /* create the old tuple for caller */
    if (oldslot) {
        *oldslot = MakeSingleTupleTableSlot(relation->rd_att, false, TAM_USTORE);
        TupleDesc rowDesc = (*oldslot)->tts_tupleDescriptor;

        UHeapTuple oldtupCopy = UHeapCopyTuple(&utuple);
        // deform the old tuple to oldslot
        UHeapDeformTupleGuts(oldtupCopy, rowDesc, (*oldslot)->tts_values, (*oldslot)->tts_isnull, rowDesc->natts);

        ExecStoreTuple(oldtupCopy, *oldslot, InvalidBuffer, true);
        (*oldslot)->tts_tuple = (Tuple)oldtupCopy;
    }

    /*
     * If the slot is marked as frozen, the latest modifier of the tuple must
     * be frozen.
     */
    if (UHeapTupleHeaderGetTDSlot((UHeapDiskTuple)(utuple.disk_tuple)) == UHEAPTUP_SLOT_FROZEN) {
        tdinfo.td_slot = UHEAPTUP_SLOT_FROZEN;
        tdinfo.xid = InvalidTransactionId;
    }

    utuple.disk_tuple->flag &= ~UHEAP_MULTI_LOCKERS;
    tempInfomask = utuple.disk_tuple->flag;

    /* Compute the new flag to store into the tuple. */
    ComputeNewXidInfomask(&utuple, buffer, tdinfo.xid, tdinfo.td_slot, tempInfomask, fxid, transSlotId, lockerXid,
        LockTupleExclusive, ForUpdate, &newInfoMask, &newTDSlotId);

    Assert(newTDSlotId == transSlotId);

    if (TransactionIdOlderThanAllUndo(tdinfo.xid)) {
        tdinfo.xid = FrozenTransactionId;
    }
    utuple.table_oid = RelationGetRelid(relation);
    utuple.xc_node_id = u_sess->pgxc_cxt.PGXCNodeIdentifier;

    HeapTupleData test;
    test.t_len = utuple.disk_tuple_size;
    test.t_self = utuple.ctid;
    test.t_tableOid = utuple.table_oid;
    test.t_xc_node_id = utuple.xc_node_id;

    CheckForSerializableConflictIn(relation, &test, buffer);

    utuple.disk_tuple_size = test.t_len;
    utuple.ctid = test.t_self;
    utuple.table_oid = test.t_tableOid;
    utuple.xc_node_id = test.t_xc_node_id;

    /* Save the previous updated information in the undo record */
    TD oldTD;
    oldTD.xactid = tdinfo.xid;
    oldTD.undo_record_ptr = tdinfo.urec_add;

    /* Prepare Undo */
    UndoPersistence persistence = UndoPersistenceForRelation(relation);
    Oid relOid = RelationIsPartition(relation) ? relation->parentId : RelationGetRelid(relation);

    Oid partitionOid = RelationIsPartition(relation) ? RelationGetRelid(relation) : InvalidOid;
    urecptr = UHeapPrepareUndoDelete(relOid, partitionOid, RelationGetRelFileNode(relation),
        RelationGetRnodeSpace(relation), persistence, buffer, offnum, fxid, subxid, cid,
        prevUrecptr, INVALID_UNDO_REC_PTR, &oldTD, &utuple, InvalidBlockNumber, NULL, &xlum);
    initStringInfo(&undotup);
    appendBinaryStringInfo(&undotup, (char *)utuple.disk_tuple, utuple.disk_tuple_size);
    /* No ereport(ERROR) from here till changes are logged */
    START_CRIT_SECTION();
    InsertPreparedUndo(u_sess->ustore_cxt.urecvec);
    UndoRecPtr oldPrevUrp = GetCurrentTransactionUndoRecPtr(persistence);
    SetCurrentTransactionUndoRecPtr(urecptr, persistence);

    UHeapPageSetUndo(buffer, transSlotId, fxid, urecptr);

    /*
     * If this transaction commits, the tuple will become DEAD sooner or
     * later.  If the transaction finally aborts, the subsequent page pruning
     * will be a no-op and the hint will be cleared.
     */
    UPageSetPrunable(page, fxid);

    UHeapRecordPotentialFreeSpace(relation, buffer, SHORTALIGN(utuple.disk_tuple_size));

    /* Fixme: Temporary hack, this value should come from ComputeNewXidInfomask() */
    newInfomask = UHEAP_XID_EXCL_LOCK;

    UHeapTupleHeaderSetTDSlot(utuple.disk_tuple, transSlotId);
    utuple.disk_tuple->flag &= ~UHEAP_VIS_STATUS_MASK;
    utuple.disk_tuple->flag |= UHEAP_DELETED | newInfomask;

    utuple.disk_tuple->flag &= ~SINGLE_LOCKER_XID_IS_LOCK;

    /* Signal that this is actually a move into another partition */
    if (changingPart)
        UHeapTupleHeaderSetMovedPartitions(utuple.disk_tuple);

    MarkBufferDirty(buffer);
    undo::PrepareUndoMeta(&xlum, persistence, u_sess->ustore_cxt.urecvec->LastRecord(), 
        u_sess->ustore_cxt.urecvec->LastRecordSize());
    undo::UpdateTransactionSlot(fxid, &xlum, u_sess->ustore_cxt.urecvec->FirstRecord(), persistence);
    /* do xlog stuff */
    if (RelationNeedsWAL(relation)) {
        UHeapWALInfo delWalInfo = { 0 };
        uint8 xlUndoHeaderFlag = 0;
        UndoRecord *urec = u_sess->ustore_cxt.undo_records[0];
        TransactionId currentXid = InvalidTransactionId;

        if (prevUrecptr != INVALID_UNDO_REC_PTR) {
            xlUndoHeaderFlag |= XLOG_UNDO_HEADER_HAS_BLK_PREV;
        }
        if ((urec->Uinfo() & UNDO_UREC_INFO_TRANSAC) != 0) {
            xlUndoHeaderFlag |= XLOG_UNDO_HEADER_HAS_PREV_URP;
        }
        if (RelationIsPartition(relation)) {
            xlUndoHeaderFlag |= XLOG_UNDO_HEADER_HAS_PARTITION_OID;
        }
        if (subxid != InvalidSubTransactionId) {
            xlUndoHeaderFlag |= XLOG_UNDO_HEADER_HAS_SUB_XACT;

            if (RelationIsLogicallyLogged(relation)) {
                xlUndoHeaderFlag |= XLOG_UNDO_HEADER_HAS_CURRENT_XID;
                currentXid = GetCurrentTransactionId();
            }
        }
        delWalInfo.oldUTuple = undotup;
        delWalInfo.relOid = relOid;
        delWalInfo.partitionOid = partitionOid;
        delWalInfo.oldXid = oldTD.xactid;
        delWalInfo.oldurecadd = oldTD.undo_record_ptr;
        delWalInfo.hasSubXact = (subxid != InvalidSubTransactionId);
        delWalInfo.buffer = buffer;
        delWalInfo.utuple = &utuple;
        delWalInfo.prevurp = oldPrevUrp;
        delWalInfo.urecptr = urecptr;
        delWalInfo.blkprev = prevUrecptr;
        delWalInfo.td_id = transSlotId;
        delWalInfo.flag = xlUndoHeaderFlag;
        delWalInfo.xid = currentXid;

        /* undo meta information */
        delWalInfo.hZone = (undo::UndoZone *)g_instance.undo_cxt.uZones[t_thrd.undo_cxt.zids[persistence]];
        Assert(delWalInfo.hZone != NULL);

        delWalInfo.xlum = &xlum;

        LogUHeapDelete(&delWalInfo);
    }
    undo::FinishUndoMeta(&xlum, persistence);

    END_CRIT_SECTION();

    pfree(undotup.data);
    UHeapFinalizeDML<UHEAP_DELETE>(relation, buffer, NULL, &utuple, NULL, &(utuple.ctid), hasTupLock, false, false);

    return TM_Ok;
}

/*
 * UHeapUpdate - update a tuple
 *
 * This function either updates the tuple in-place or it deletes the old
 * tuple and new tuple for non-in-place updates.  Additionally this function
 * inserts an undo record and updates the undo pointer in page header.
 *
 * For input and output values, see heap_update.
 */
TM_Result UHeapUpdate(Relation relation, Relation parentRelation, ItemPointer otid, UHeapTuple newtup, CommandId cid,
    Snapshot crosscheck, Snapshot snapshot, bool wait, TupleTableSlot **oldslot, TM_FailureData *tmfd,
    bool *indexkey_update_flag, Bitmapset **modifiedIdxAttrs, bool allow_inplace_update)
{
    TM_Result result = TM_Ok;
    TransactionId fxid;
    TransactionId xid = GetTopTransactionId();
    fxid = xid;
    TransactionId saveTupXid;
    TransactionId oldestXidHavingUndo;
    TransactionId singleLockerXid = InvalidTransactionId;
    Bitmapset *inplaceUpdAttrs = NULL;
    Bitmapset *keyAttrs = NULL;
    Bitmapset *interestingAttrs = NULL;
    Bitmapset *modifiedAttrs = NULL;
    RowPtr *lp;
    StringInfoData undotup;
    UHeapTupleData oldtup;
    UHeapTuple uheaptup;
    UndoRecPtr urecptr;
    UndoRecPtr newUrecptr;
    UndoRecPtr prevUrecptr = INVALID_UNDO_REC_PTR;
    UndoRecPtr newPrevUrecptr;
    Page page;
    BlockNumber block;
    ItemPointerData ctid;
    Buffer buffer;
    Buffer newbuf;
    Size newtupsize = 0;
    Size oldtupsize = 0;
    Size pagefree = 0;
    int oldtupNewTransSlot = InvalidTDSlotId;
    int newtupTransSlot = InvalidTDSlotId;
    int resultTransSlotId = InvalidTDSlotId;
    OffsetNumber oldOffnum = 0;
    bool haveTupleLock = false;
    bool isIndexUpdated = false;
    bool useInplaceUpdate = false;
    bool checkedLockers = false;
    bool lockerRemains = false;
    bool anyMultiLockerMemberAlive = false;
    bool lockReacquired = false;
    bool oldbufLockReacquired = false;
    bool needToast = false;
    bool hasSubXactLock = false;
    bool slotReused = false;
    bool inplaceUpdated = false;
    bool doReacquire = false;
    UHeapTupleTransInfo txactinfo;
    uint16 oldInfomask = 0;
    uint16 newInfomask = 0;
    uint16 tempInfomask = 0;
    uint16 infomaskOldTuple = 0;
    uint16 infomaskNewTuple = 0;
    TransactionId lockerXid = InvalidTransactionId;
    SubTransactionId lockerSubXid = InvalidSubTransactionId;
    SubTransactionId updateSubXid = InvalidSubTransactionId;
    SubTransactionId subxid = InvalidSubTransactionId;
    char *xlogXorDelta = NULL;
    undo::XlogUndoMeta xlum;
    bool hasPruned = false;
    bool alreadyLocked = false;
    bool multixidIsMyself = false;
    BlockNumber blkno = 0;
    LockTupleMode lockmode;
    TransactionId minXidInTDSlots = InvalidTransactionId;
    bool oldBufLockReleased = false;

    Assert(newtup->tupTableType == UHEAP_TUPLE);
    Assert(ItemPointerIsValid(otid));

    /*
     * Fetch the list of attributes to be checked for various operations.
     *
     * For in-place update considerations, this is wasted effort if we fail to
     * update or have to put the new tuple on a different page.  But we must
     * compute the list before obtaining buffer lock --- in the worst case, if
     * we are doing an update on one of the relevant system catalogs, we could
     * deadlock if we try to fetch the list later.  Note, that as of now
     * system catalogs are always stored in heap, so we might not hit the
     * deadlock case, but it can be supported in future.  In any case, the
     * relcache caches the data so this is usually pretty cheap.
     *
     * Note that we get a copy here, so we need not worry about relcache flush
     * happening midway through.
     */
    if (parentRelation != NULL) {
        // For partitioned table, we use the parent relation to calc hot_attrs.

        Assert(RELATION_IS_PARTITIONED(parentRelation));
        inplaceUpdAttrs = RelationGetIndexAttrBitmap(parentRelation, INDEX_ATTR_BITMAP_ALL);
        keyAttrs = RelationGetIndexAttrBitmap(parentRelation, INDEX_ATTR_BITMAP_IDENTITY_KEY);
    } else {
        inplaceUpdAttrs = RelationGetIndexAttrBitmap(relation, INDEX_ATTR_BITMAP_ALL);
        keyAttrs = RelationGetIndexAttrBitmap(relation, INDEX_ATTR_BITMAP_IDENTITY_KEY);
    }
    block = ItemPointerGetBlockNumber(otid);
    page = GetPageBuffer(relation, block, buffer);

    interestingAttrs = NULL;
    interestingAttrs = bms_add_members(interestingAttrs, inplaceUpdAttrs);
    interestingAttrs = bms_add_members(interestingAttrs, keyAttrs);

    WHITEBOX_TEST_STUB(UHEAP_UPDATE_FAILED, WhiteboxDefaultErrorEmit);

    LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

    oldOffnum = ItemPointerGetOffsetNumber(otid);
    lp = UPageGetRowPtr(page, oldOffnum);
    Assert(RowPtrIsNormal(lp) || RowPtrIsDeleted(lp));

    UHeapResetWaitTimeForTDSlot();

check_tup_satisfies_update:
    checkedLockers = false;
    lockerRemains = false;
    anyMultiLockerMemberAlive = true;

    result = UHeapTupleSatisfiesUpdate(relation, snapshot, otid, &oldtup, cid, buffer, &ctid, &txactinfo, &updateSubXid,
        &lockerXid, &lockerSubXid, false, multixidIsMyself, &inplaceUpdated);

    multixidIsMyself = false;

    /*
     * The oldUpdaterXid is either the inserting xid or the previous updater xid.
     * If txactinfo.xid is 0, then it either means the the TD slot was frozen or we were not
     * able to fetch its transaction information from undo because it has been discarded.
     * In either case, it is too old to matter so tell the undo chain traversal to stop here.
     */
    TransactionId oldUpdaterXid = TransactionIdIsValid(txactinfo.xid) ? txactinfo.xid : FrozenTransactionId;
    tmfd->xmin = oldUpdaterXid;

    /* Determine columns modified by the update. Should be recomputed after we re-fetch the old tuple */
    if (oldtup.disk_tuple != NULL) {
        if (modifiedAttrs != NULL) {
            bms_free(modifiedAttrs);
        }
        modifiedAttrs = UHeapDetermineModifiedColumns(relation, interestingAttrs, &oldtup, newtup);
    }

    lockmode = LockTupleExclusive;

    if (result == TM_Invisible) {
        UnlockReleaseBuffer(buffer);
        ereport(PANIC, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg("attempted to update invisible inplace heap tuple")));
    } else if ((result == TM_BeingModified) && wait) {
#ifdef ENABLE_WHITEBOX
        ereport(WARNING, (errmsg("UHeapUpdate returned %d", result)));
#endif

        TransactionId xwait = InvalidTransactionId;

        if (TransactionIdIsValid(lockerXid)) {
            xwait = lockerXid;
        } else {
            xwait = txactinfo.xid;
        }

        // Check if tuple has already been locked by us in the required mode
        alreadyLocked = IsTupleLockedByUs(&oldtup, xwait, lockmode);

        if (!UHeapWait(relation, buffer, &oldtup, lockmode, false, txactinfo.xid, lockerXid, updateSubXid,
            lockerSubXid, &haveTupleLock, &multixidIsMyself)) {
            goto check_tup_satisfies_update;
        }

        result = TM_Ok;
    } else if (result == TM_Ok) {
        /*
         * There is no active locker on the tuple, so we avoid grabbing the
         * lock on new tuple.
         */
        checkedLockers = true;
        lockerRemains = false;
    }

    if (crosscheck != InvalidSnapshot && result == TM_Ok) {
        /* Perform additional check for transaction-snapshot mode RI updates */
        if (!UHeapTupleFetch(relation, buffer, oldOffnum, crosscheck, NULL, NULL, false))
            result = TM_Updated;
    }

   /*
    * We need to re-fetch the row information since it might
    * have changed due to TD extension by the contending transaction
    * for the same page.
    */
    lp = UPageGetRowPtr(page, oldOffnum);
    if (result != TM_Ok) {
        Assert(result == TM_SelfModified || result == TM_SelfUpdated || result == TM_Updated || result == TM_Deleted ||
            result == TM_BeingModified);
        if (!RowPtrIsDeleted(lp) && oldtup.disk_tuple == NULL) {
            ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("oldtup.disk_tuple is NULL")));
        }
        Assert(RowPtrIsDeleted(lp) || IsUHeapTupleModified(oldtup.disk_tuple->flag));
        /* If item id is deleted, tuple can't be marked as moved. */
        if (!RowPtrIsDeleted(lp) && UHeapTupleIsMoved(oldtup.disk_tuple->flag))
            ItemPointerSetMovedPartitions(&tmfd->ctid);
        else
            tmfd->ctid = ctid;
        tmfd->xmax = txactinfo.xid;
        if (result == TM_SelfModified || result == TM_SelfUpdated)
            tmfd->cmax = txactinfo.cid;
        else
            tmfd->cmax = InvalidCommandId;
        tmfd->in_place_updated_or_locked = inplaceUpdated;

        if (result == TM_Updated &&
            ItemPointerEquals(&tmfd->ctid, otid) &&
            !tmfd->in_place_updated_or_locked) {
            result = TM_Deleted;
        }

        UnlockReleaseBuffer(buffer);
        if (haveTupleLock) {
            UnlockTuple(relation, &(oldtup.ctid), ExclusiveLock);
        }

        bms_free(inplaceUpdAttrs);
        bms_free(keyAttrs);
        *indexkey_update_flag =
            !UHeapTupleIsInPlaceUpdated(((UHeapTuple)newtup)->disk_tuple->flag) || *modifiedIdxAttrs != NULL;

#ifdef ENABLE_WHITEBOX
        ereport(WARNING, (errmsg("UHeapUpdate returned %d", result)));
#endif

        return result;
    }

    /* the new tuple is ready, except for this: */
    newtup->table_oid = RelationGetRelid(relation);
    newtup->xc_node_id = u_sess->pgxc_cxt.PGXCNodeIdentifier;

    isIndexUpdated = bms_overlap(modifiedAttrs, inplaceUpdAttrs);
    if (modifiedIdxAttrs != NULL) {
        *modifiedIdxAttrs = isIndexUpdated ? bms_intersect(modifiedAttrs, inplaceUpdAttrs) : NULL;
    }

    if (relation->rd_rel->relkind != RELKIND_RELATION) {
        /* toast table entries should never be recursively toasted */
        Assert(!UHeapTupleHasExternal(&oldtup));
        Assert(!UHeapTupleHasExternal(newtup));
        needToast = false;
    } else {
        needToast = (newtup->disk_tuple_size >= UTOAST_TUPLE_THRESHOLD || UHeapTupleHasExternal(&oldtup) ||
            UHeapTupleHasExternal(newtup));
    }

    oldtupsize = SHORTALIGN(oldtup.disk_tuple_size);
    newtupsize = SHORTALIGN(newtup->disk_tuple_size);

    /*
     * An in-place update is only possible if no attribute that have been moved to
     * an external TOAST table.If the new tuple is no larger than the old one, that's enough;
     * otherwise, we also need sufficient free space to be available in the page.
     */
    if (needToast) {
        useInplaceUpdate = false;
#ifdef DEBUG_UHEAP
        if (isIndexUpdated)
            UHEAPSTAT_COUNT_NONINPLACE_UPDATE_CAUSE(INDEX_UPDATED);
        else
            UHEAPSTAT_COUNT_NONINPLACE_UPDATE_CAUSE(TOAST);
#endif
        hasPruned = UHeapPagePruneOptPage(relation, buffer, fxid);
#ifdef DEBUG_UHEAP
        UHEAPSTAT_COUNT_OP_PRUNEPAGE(upd, 1);
        if (hasPruned) {
            UHEAPSTAT_COUNT_OP_PRUNEPAGE_SUC(upd, 1);
        }
#endif
        blkno = BufferGetBlockNumber(buffer);

        /* Now that we are done with the page, get its available space */
        if (hasPruned) {
            Size freespace = PageGetUHeapFreeSpace(page);
            double thres = RelationGetTargetPageFreeSpacePrune(relation, HEAP_DEFAULT_FILLFACTOR);
            double prob = FSM_UPDATE_HEURISTI_PROBABILITY * freespace / thres;
            RecordPageWithFreeSpace(relation, blkno, freespace);
            if (rand() % 100 >= 100.0 - prob * 100.0) {
#ifdef DEBUG_UHEAP
                UHEAPSTAT_COUNT_OP_PRUNEPAGE_SPC(upd, freespace);
#endif

                UpdateFreeSpaceMap(relation, blkno, blkno, freespace, false);
            }
        }
    } else if (newtupsize <= oldtupsize) {
        useInplaceUpdate = true;
        if (allow_inplace_update == false) {
            useInplaceUpdate = false;
        }
    } else {
        /* Pass delta space required to accommodate the new tuple. */
        useInplaceUpdate = UHeapPagePruneOpt(relation, buffer, oldOffnum, newtupsize - oldtupsize);

        if (allow_inplace_update == false) {
            useInplaceUpdate = false;
        }

#ifdef DEBUG_UHEAP
        if (!useInplaceUpdate)
            UHEAPSTAT_COUNT_NONINPLACE_UPDATE_CAUSE(PAGE_PRUNE_FAILED);
#endif
        /* The page might have been modified, so refresh disk_tuple */
        oldtup.disk_tuple = (UHeapDiskTuple)UPageGetRowData(page, lp);
    }
    /*
     * Acquire subtransaction lock, if current transaction is a
     * subtransaction.
     */
    hasSubXactLock = IsSubTransaction();
    if (hasSubXactLock) {
        subxid = GetCurrentSubTransactionId();
        SubXactLockTableInsert(subxid);
    }

    /*
     * The transaction information of tuple needs to be set in transaction
     * slot, so needs to reserve the slot before proceeding with the actual
     * operation.  It will be costly to wait for getting the slot, but we do
     * that by releasing the buffer lock.
     */
    oldtupNewTransSlot = UHeapPageReserveTransactionSlot(relation, buffer, fxid, &prevUrecptr,
        &lockReacquired, InvalidBuffer, &slotReused, &minXidInTDSlots);

    /*
     * We need to re-fetch the row information since it might
     * have changed due to TD extension as part of
     * the above call to UHeapPageReserveTransactionSlot().
     */
    lp = UPageGetRowPtr(page, oldOffnum);
    pagefree = PageGetUHeapFreeSpace(page);

    if (lockReacquired) {
        goto check_tup_satisfies_update;
    }

    if (oldtupNewTransSlot == InvalidTDSlotId) {
        LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
        UHeapSleepOrWaitForTDSlot(minXidInTDSlots, fxid);
        LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
        goto check_tup_satisfies_update;
    }

    /* transaction slot must be reserved before adding tuple to page */
    Assert(oldtupNewTransSlot != InvalidTDSlotId);

    /*
     * It's possible that tuple slot is now marked as frozen. Hence, we
     * refetch the tuple here.
     */
    Assert(!RowPtrIsDeleted(lp));
    oldtup.disk_tuple = (UHeapDiskTuple)UPageGetRowData(page, lp);
    oldtup.disk_tuple_size = RowPtrGetLen(lp);

    /*
     * Using a transaction slot of transaction that is still not all-visible
     * will lead to undo access during tuple visibility checks and that sucks
     * the performance.  To avoid accessing undo, we perform non-inplace
     * updates so as to distribute the tuple across pages so that we don't
     * face scarcity of transaction slots on the page.  However, we must have
     * a hard limit for this optimization, else the number of blocks will
     * increase without any bound.
     */
    if (slotReused) {
        BlockNumber nblocks = RelationGetNumberOfBlocks(relation);

#ifdef DEBUG_UHEAP
        UHEAPSTAT_COUNT_NONINPLACE_UPDATE_CAUSE(SLOT_REUSED);
#endif

        if (nblocks <= NUM_BLOCKS_FOR_NON_INPLACE_UPDATES) {
            useInplaceUpdate = false;
#ifdef DEBUG_UHEAP
            UHEAPSTAT_COUNT_NONINPLACE_UPDATE_CAUSE(nblocks_LESS_THAN_NBLOCKS);
#endif
        } else
            slotReused = false;
    }

#ifdef DEBUG_UHEAP
    if (!useInplaceUpdate)
        UHEAPSTAT_COUNT_UPDATE(NON_INPLACE_UPDATE);
    else
        UHEAPSTAT_COUNT_UPDATE(INPLACE_UPDATE);
#endif

    /*
     * If the slot is marked as frozen, the latest modifier of the tuple must
     * be frozen.
     */
    if (UHeapTupleHeaderGetTDSlot((UHeapDiskTuple)(oldtup.disk_tuple)) == UHEAPTUP_SLOT_FROZEN) {
        txactinfo.td_slot = UHEAPTUP_SLOT_FROZEN;
        txactinfo.xid = InvalidTransactionId;
    }

    /*
     * Save the xid that has updated the tuple to compute infomask for tuple.
     */
    saveTupXid = txactinfo.xid;

    /*
     * If the last transaction that has updated the tuple is already too old,
     * then consider it as frozen which means it is all-visible.  This ensures
     * that we don't need to store epoch in the undo record to check if the
     * undo tuple belongs to previous epoch and hence all-visible.  See
     * comments atop of file inplaceheapam_visibility.c.
     */
    oldestXidHavingUndo = pg_atomic_read_u64(&g_instance.proc_base->oldestXidInUndo);
    if (TransactionIdPrecedes(txactinfo.xid, oldestXidHavingUndo)) {
        txactinfo.xid = FrozenTransactionId;
        oldUpdaterXid = FrozenTransactionId;
    }

    Assert(!UHeapTupleIsUpdated(oldtup.disk_tuple->flag));

    /*
     * updated tuple doesn't fit on current page or the toaster needs to be
     * activated or transaction slot has been reused.  To prevent concurrent
     * sessions from updating the tuple, we have to temporarily mark it
     * locked, while we release the page lock.
     */
    Assert(!slotReused || !useInplaceUpdate);
    if (slotReused || (!useInplaceUpdate && newtupsize > pagefree) || needToast) {
        BlockNumber oldblk, newblk;
        TD oldTD;

        oldTD.xactid = oldUpdaterXid;
        oldTD.undo_record_ptr = txactinfo.urec_add;

        if (!alreadyLocked) {
            (void)UHeapExecuteLockTuple(relation, buffer, &oldtup, LockTupleExclusive, true);
        }

        LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

        /*
         * Let the toaster do its thing, if needed.
         *
         * Note: below this point, UHeaptup is the data we actually intend to
         * store into the relation; newtup is the caller's original untoasted
         * data.
         */
        if (needToast) {
            uheaptup = UHeapToastInsertOrUpdate(relation, newtup, &oldtup, 0);
            newtupsize = SHORTALIGN(uheaptup->disk_tuple_size);
        } else {
            uheaptup = newtup;
        }
    reacquire_buffer:

        /*
         * If we have reused the transaction slot, we must use new page to
         * perform non-inplace update in a separate page so as to reduce
         * contention on transaction slots.
         */
        if (slotReused || newtupsize > pagefree) {
            Assert(!useInplaceUpdate);
            newbuf = RelationGetBufferForUTuple(relation, uheaptup->disk_tuple_size, buffer, 0, NULL);
        } else {
            /* Re-acquire the lock on the old tuple's page. */
            LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
            /* Re-check using the up-to-date free space */
            pagefree = PageGetUHeapFreeSpace(page);
            if (newtupsize > pagefree) {
                /*
                 * Rats, it doesn't fit anymore.  We must now unlock and
                 * relock to avoid deadlock.  Fortunately, this path should
                 * seldom be taken.
                 */
                LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
                newbuf = RelationGetBufferForUTuple(relation, uheaptup->disk_tuple_size, buffer, 0, NULL);
            } else {
                /* OK, it fits here, so we're done. */
                newbuf = buffer;
            }
        }

        oldblk = BufferGetBlockNumber(buffer);
        newblk = BufferGetBlockNumber(newbuf);
        /*
         * If we have got the new block than reserve the slot in same order in
         * which buffers are locked (ascending).
         */
        if (oldblk == newblk) {
            uint16 lower;
            Page npage = BufferGetPage(newbuf);

            lower = ((UHeapPageHeaderData *)npage)->pd_lower;
            newtupTransSlot = UHeapPageReserveTransactionSlot(relation, newbuf, fxid, &newPrevUrecptr,
                &lockReacquired, InvalidBuffer, NULL, &minXidInTDSlots);

            /*
             * It is possible that available space on the page changed
             * as part of TD reservation operation. If so, go back and reacquire the buffer.
             */
            if (lower < ((UHeapPageHeaderData *)npage)->pd_lower) {
                elog(DEBUG5, "Do Reacquire1 Rel: %s, lower: %d, new_lower: %d, newbuf: %d",
                    RelationGetRelationName(relation), lower, ((UHeapPageHeaderData *)npage)->pd_lower, newbuf);
                doReacquire = true;
            }

            /*
             * It is possible we grabbed a different TD slot not equal to oldtupNewTransSlot when
             * alreadyLocked == true. Hence make sure to refresh prevUrecptr and oldtupNewTransSlot.
             */
            prevUrecptr = newPrevUrecptr;
            oldtupNewTransSlot = newtupTransSlot;
        } else {
            uint16 obufLower;
            uint16 nbufLower;
            Page npage = BufferGetPage(newbuf);

            obufLower = ((UHeapPageHeaderData *)page)->pd_lower;
            nbufLower = ((UHeapPageHeaderData *)npage)->pd_lower;

            /* Reserve TD slots for the new as well as the old page */
            UHeapReserveDualPageTDSlot(relation, buffer, newbuf, fxid, &prevUrecptr, &newPrevUrecptr,
                                       &oldtupNewTransSlot, &newtupTransSlot, &lockReacquired,
                                       &oldbufLockReacquired, &minXidInTDSlots, &oldBufLockReleased);

            /*
             * It is possible that available space on the page changed
             * as part of TD reservation operation. If so, go back and reacquire the buffer.
             */
            if (obufLower < ((UHeapPageHeaderData *)page)->pd_lower ||
                nbufLower < ((UHeapPageHeaderData *)npage)->pd_lower) {
                elog(DEBUG5,
                    "Do Reacquire2 Rel: %s, olower: %d, onew_lower: %d, nlower: %d, nnew_lower: %d, oldbuf:%d, newbuf: "
                    "%d",
                    RelationGetRelationName(relation), obufLower, ((UHeapPageHeaderData *)page)->pd_lower, nbufLower,
                    ((UHeapPageHeaderData *)npage)->pd_lower, buffer, newbuf);
                doReacquire = true;
            }
        }

        if (lockReacquired || oldbufLockReacquired || doReacquire || newtupTransSlot == InvalidTDSlotId ||
            oldtupNewTransSlot == InvalidTDSlotId) {
            /*
             * If non in-place update is happening on two different buffers,
             * then release the new buffer, and release the lock on old
             * buffer. Else, only release the lock on old buffer.
             */
            if (buffer != newbuf) {
                if (!oldBufLockReleased) {
                    LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
                } else {
                    BufferDesc *buf_hdr PG_USED_FOR_ASSERTS_ONLY;

                    /*
                     * Old buffer should be valid and should not locked
                     * because we already released lock on the old buffer in
                     * UHeapPageFreezeTransSlots.
                     */
                    Assert(BufferIsValid(buffer));
                    buf_hdr = GetBufferDescriptor(buffer - 1);
                    Assert(!(LWLockHeldByMeInMode(BufferDescriptorGetContentLock(buf_hdr), LW_EXCLUSIVE)));
                }

                if (!oldbufLockReacquired) {
                    /* Release the new buffer. */
                    UnlockReleaseBuffer(newbuf);
                } else {
                    Assert(!oldBufLockReleased);

                    BufferDesc *buf_hdr PG_USED_FOR_ASSERTS_ONLY;
                    Assert(BufferIsValid(newbuf));
                    buf_hdr = GetBufferDescriptor(newbuf - 1);
                    Assert(!(LWLockHeldByMeInMode(BufferDescriptorGetContentLock(buf_hdr), LW_EXCLUSIVE)));

                    ReleaseBuffer(newbuf);
                }
            } else
                LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

            // sleep when there is no available td slot
            if (newtupTransSlot == InvalidTDSlotId || oldtupNewTransSlot == InvalidTDSlotId) {
                UHeapSleepOrWaitForTDSlot(minXidInTDSlots, fxid);
            }

            doReacquire = false;
            oldbufLockReacquired = false;
            goto reacquire_buffer;
        }

        /*
         * We need to re-fetch the row information since it might
         * have changed due to TD extension as part of
         * the above call to UHeapPageReserveTransactionSlot().
         */
        lp = UPageGetRowPtr(page, oldOffnum);

        /*
         * After we release the lock on page, it could be pruned.  As we have
         * lock on the tuple, it couldn't be removed underneath us, but its
         * position could be changes, so need to refresh the tuple position.
         *
         * XXX Though the length of the tuple wouldn't have changed, but there
         * is no harm in refreshing it for the sake of consistency of code.
         */
        oldtup.disk_tuple = (UHeapDiskTuple)UPageGetRowData(page, lp);
        oldtup.disk_tuple_size = RowPtrGetLen(lp);
    } else {
        /* No TOAST work needed, and it'll fit on same page */
        newbuf = buffer;
        newtupTransSlot = oldtupNewTransSlot;
        uheaptup = newtup;
    }

    /* Till now, we know whether we will delete the old index */
    if (oldslot && (*modifiedIdxAttrs != NULL || !useInplaceUpdate)) {
        *oldslot = MakeSingleTupleTableSlot(relation->rd_att, false, TAM_USTORE);
        TupleDesc rowDesc = (*oldslot)->tts_tupleDescriptor;

        UHeapTuple oldtupCopy = UHeapCopyTuple(&oldtup);
        // deform the old tuple to oldslot
        UHeapDeformTupleGuts(oldtupCopy, rowDesc, (*oldslot)->tts_values, (*oldslot)->tts_isnull, rowDesc->natts);

        ExecStoreTuple(oldtupCopy, *oldslot, InvalidBuffer, true);
        (*oldslot)->tts_tuple = (Tuple)oldtupCopy;
    }

    HeapTupleData test;
    test.t_len = oldtup.disk_tuple_size;
    test.t_self = oldtup.ctid;
    test.t_tableOid = oldtup.table_oid;
    test.t_xc_node_id = oldtup.xc_node_id;

    CheckForSerializableConflictIn(relation, &test, buffer);

    oldtup.disk_tuple_size = test.t_len;
    oldtup.ctid = test.t_self;
    oldtup.table_oid = test.t_tableOid;
    oldtup.xc_node_id = test.t_xc_node_id;
    /* Prepare an undo record for this operation. */
    /* Save the previous updated information in the undo record */
    TD oldTD;
    oldTD.xactid = oldUpdaterXid;
    oldTD.undo_record_ptr = txactinfo.urec_add;
    UndoPersistence persistence = UndoPersistenceForRelation(relation);
    Oid relOid = RelationIsPartition(relation) ? relation->parentId : RelationGetRelid(relation);
    Oid partitionOid = RelationIsPartition(relation) ? RelationGetRelid(relation) : InvalidOid;

    /* calculate xor delta for inplaceupdate, to allocate correct undo size */
    int undoXorDeltaSize = 0;
    uint16 prefixlen = 0;
    uint16 suffixlen = 0;
    uint8 xorDeltaFlags = 0;
    char *oldp = (char *)oldtup.disk_tuple + oldtup.disk_tuple->t_hoff;
    char *newp = (char *)uheaptup->disk_tuple + uheaptup->disk_tuple->t_hoff;
    int oldlen = oldtup.disk_tuple_size - oldtup.disk_tuple->t_hoff;
    int newlen = uheaptup->disk_tuple_size - uheaptup->disk_tuple->t_hoff;
    int minlen = Min(oldlen, newlen);

    if (useInplaceUpdate && !RelationIsLogicallyLogged(relation)) {
        char *oldpTmp = NULL;
        char *newpTmp = NULL;

        oldpTmp = oldp;
        newpTmp = newp;
        for (prefixlen = 0; prefixlen < minlen; prefixlen++, oldpTmp++, newpTmp++) {
            if (*oldpTmp != *newpTmp) {
                break;
            }
        }

        if (prefixlen < MIN_SAVING_LEN) {
            prefixlen = 0;
        } else {
            xorDeltaFlags |= UREC_INPLACE_UPDATE_XOR_PREFIX;
        }

        int minlenWithNoPrefixlen = minlen - prefixlen;
        oldpTmp = &(oldp[oldlen - 1]);
        newpTmp = &(newp[newlen - 1]);
        for (suffixlen = 0; suffixlen < minlenWithNoPrefixlen; suffixlen++, oldpTmp--, newpTmp--) {
            if (*oldpTmp != *newpTmp)
                break;
        }

        if (suffixlen < MIN_SAVING_LEN) {
            suffixlen = 0;
        } else {
            xorDeltaFlags |= UREC_INPLACE_UPDATE_XOR_SUFFIX;
        }

        if (prefixlen > 0)
            undoXorDeltaSize += sizeof(uint16);
        if (suffixlen > 0)
            undoXorDeltaSize += sizeof(uint16);
    }

    /* The first sizeof(uint8) is space for t_hoff and the second sizeof(uint8) is space for prefix and suffix flag
     */
    undoXorDeltaSize += sizeof(uint8) + oldtup.disk_tuple->t_hoff - OffsetTdId + sizeof(uint8);
    undoXorDeltaSize += oldlen - prefixlen - suffixlen;

    urecptr = UHeapPrepareUndoUpdate(relOid, partitionOid, RelationGetRelFileNode(relation), 
        RelationGetRnodeSpace(relation), persistence, buffer, newbuf,
        ItemPointerGetOffsetNumber(&oldtup.ctid), fxid, subxid, cid, prevUrecptr, newPrevUrecptr,
        INVALID_UNDO_REC_PTR, &oldTD, &oldtup, useInplaceUpdate,
        &newUrecptr, undoXorDeltaSize, InvalidBlockNumber, InvalidBlockNumber, NULL, &xlum);
    initStringInfo(&undotup);
    appendBinaryStringInfo(&undotup, (char *)oldtup.disk_tuple, oldtup.disk_tuple_size);

    /* Calculate XOR detla and write in the undo raw data */
    UndoRecord *undorec = (*u_sess->ustore_cxt.urecvec)[0];

    if (useInplaceUpdate) {
        appendBinaryStringInfo(undorec->Rawdata(), (char *)&(oldtup.disk_tuple->t_hoff), sizeof(uint8));
        appendBinaryStringInfo(undorec->Rawdata(), (char *)oldtup.disk_tuple + OffsetTdId,
            oldtup.disk_tuple->t_hoff - OffsetTdId);
        appendBinaryStringInfo(undorec->Rawdata(), (char *)&xorDeltaFlags, sizeof(uint8));
    }

    oldtup.disk_tuple->flag &= ~UHEAP_MULTI_LOCKERS;
    tempInfomask = oldtup.disk_tuple->flag;

    /*
     * We can't rely on anyMultiLockerMemberAlive to clear the multi
     * locker bit, if the lock on the buffer is released in between.
     */
    if (buffer == newbuf) {
        /*
         * If all the members were lockers and are all gone, we can do away
         * with the MULTI_LOCKERS bit.
         */
        if (UHeapTupleHasMultiLockers(tempInfomask) && !anyMultiLockerMemberAlive)
            tempInfomask &= ~UHEAP_MULTI_LOCKERS;
    }

    /* Compute the new xid and infomask to store into the tuple. */
    ComputeNewXidInfomask(&oldtup, buffer, saveTupXid, txactinfo.td_slot, tempInfomask, xid, oldtupNewTransSlot,
        singleLockerXid, lockmode, ForUpdate, &oldInfomask, &resultTransSlotId);

    /*
     * There must not be any stronger locker than the current operation,
     * otherwise it would have waited for it to finish.
     */
    Assert(resultTransSlotId == oldtupNewTransSlot);

    newInfomask = 0;

    if (useInplaceUpdate) {
        infomaskOldTuple = infomaskNewTuple = oldInfomask | newInfomask | UHEAP_INPLACE_UPDATED;
    } else {
        infomaskOldTuple = oldInfomask | UHEAP_UPDATED;
        infomaskNewTuple = newInfomask;
    }
    /* No ereport(ERROR) from here till changes are logged */
    START_CRIT_SECTION();
    /*
     * A page can be pruned for non-inplace updates or inplace updates that
     * results in shorter tuples.  If this transaction commits, the tuple will
     * become DEAD sooner or later.  If the transaction finally aborts, the
     * subsequent page pruning will be a no-op and the hint will be cleared.
     */
    if (!useInplaceUpdate || (uheaptup->disk_tuple_size < oldtup.disk_tuple_size)) {
        UPageSetPrunable(page, xid);
    }

    /* oldtup should be pointing to right place in page */
    Assert(oldtup.disk_tuple == (UHeapDiskTuple)UPageGetRowData(page, lp));

    UHeapTupleHeaderSetTDSlot(oldtup.disk_tuple, resultTransSlotId);
    oldtup.disk_tuple->flag &= ~UHEAP_VIS_STATUS_MASK;
    oldtup.disk_tuple->flag |= infomaskOldTuple;

    infomaskNewTuple &= ~UHEAP_XID_LOCK_ONLY;
    infomaskNewTuple &= ~SINGLE_LOCKER_XID_IS_LOCK;

    /* keep the new tuple copy updated for the caller */
    UHeapTupleHeaderSetTDSlot(uheaptup->disk_tuple, newtupTransSlot);
    uheaptup->disk_tuple->flag &= ~UHEAP_VIS_STATUS_MASK;
    uheaptup->disk_tuple->flag |= infomaskNewTuple;
    uheaptup->xc_node_id = u_sess->pgxc_cxt.PGXCNodeIdentifier;
    if (useInplaceUpdate) {
        if (prefixlen > 0) {
            appendBinaryStringInfo(undorec->Rawdata(), (char *)&prefixlen, sizeof(uint16));
        }

        if (suffixlen > 0) {
            appendBinaryStringInfo(undorec->Rawdata(), (char *)&suffixlen, sizeof(uint16));
        }

        /* Do a XOR delta between the end of prefixlen and start of suffixlen */
        appendBinaryStringInfo(undorec->Rawdata(), oldp + prefixlen, oldlen - prefixlen - suffixlen);

        xlogXorDelta = (char *)palloc(undoXorDeltaSize);
        errno_t rc = memcpy_s(xlogXorDelta, undoXorDeltaSize, undorec->Rawdata()->data, undoXorDeltaSize);
        securec_check(rc, "\0", "\0");

        if (undoXorDeltaSize != undorec->Rawdata()->len) {
            elog(PANIC, "xor data mismatch in undo and xlog, undo size %d, xlog size %d", undorec->Rawdata()->len,
                undoXorDeltaSize);
        }

        if (hasSubXactLock) {
            undorec->SetUinfo(UNDO_UREC_INFO_CONTAINS_SUBXACT);
            appendBinaryStringInfo(undorec->Rawdata(), (char *)&subxid, sizeof(SubTransactionId));
        }

        /*
         * For inplace updates, we copy the entire data portion including null
         * bitmap of new tuple.
         *
         * For the special case where we are doing inplace updates even when
         * the new tuple is bigger, we need to adjust the old tuple's location
         * so that new tuple can be copied at that location as it is.
         */
        RowPtrChangeLen(lp, uheaptup->disk_tuple_size);
        rc = memcpy_s((char *)oldtup.disk_tuple + SizeOfUHeapDiskTupleData,
            uheaptup->disk_tuple_size - SizeOfUHeapDiskTupleData,
            (char *)uheaptup->disk_tuple + SizeOfUHeapDiskTupleData,
            uheaptup->disk_tuple_size - SizeOfUHeapDiskTupleData);
        securec_check(rc, "\0", "\0");
        /*
         * Copy everything from new tuple in infomask apart from visibility
         * flags.
         */
        oldtup.disk_tuple->flag = oldtup.disk_tuple->flag & UHEAP_VIS_STATUS_MASK;
        oldtup.disk_tuple->flag |= (uheaptup->disk_tuple->flag & ~UHEAP_VIS_STATUS_MASK);
        /* Copy number of attributes in tuple. */
        UHeapTupleHeaderSetNatts(oldtup.disk_tuple, UHeapTupleHeaderGetNatts(newtup->disk_tuple));
        /* also update the tuple length and self pointer */
        oldtup.disk_tuple_size = uheaptup->disk_tuple_size;
        oldtup.disk_tuple->t_hoff = uheaptup->disk_tuple->t_hoff;
        ItemPointerCopy(&oldtup.ctid, &uheaptup->ctid);
    } else {
#ifdef USE_ASSERT_CHECKING
        CheckTupleValidity(relation, uheaptup);
#endif

        /* insert tuple at new location */
        RelationPutUTuple(relation, newbuf, uheaptup);

        /* Update the UndoRecord now that we know where the tuple is located on the Page */
        UndoRecord *undorec = (*u_sess->ustore_cxt.urecvec)[1];
        Assert(undorec->Blkno() == ItemPointerGetBlockNumber(&(uheaptup->ctid)));
        undorec->SetOffset(ItemPointerGetOffsetNumber(&(uheaptup->ctid)));

        /*
         * Let other transactions know where to find the updated version of the
         * old tuple by saving the new tuple CTID on the old tuple undo record.
         */
        UndoRecord *oldTupleUndoRec = (*u_sess->ustore_cxt.urecvec)[0];
        appendBinaryStringInfo(oldTupleUndoRec->Rawdata(), (char *)&(uheaptup->ctid), sizeof(ItemPointerData));

        if (hasSubXactLock) {
            oldTupleUndoRec->SetUinfo(UNDO_UREC_INFO_CONTAINS_SUBXACT);
            appendBinaryStringInfo(oldTupleUndoRec->Rawdata(), (char *)&subxid, sizeof(SubTransactionId));
        }

        /* update the potential freespace */
        UHeapRecordPotentialFreeSpace(relation, buffer, SHORTALIGN(oldtupsize));
        UHeapRecordPotentialFreeSpace(relation, newbuf, -1 * SHORTALIGN(newtupsize));
    }

    InsertPreparedUndo(u_sess->ustore_cxt.urecvec);

    UndoRecPtr oldPrevUrp = GetCurrentTransactionUndoRecPtr(persistence);
    UndoRecPtr oldPrevUrpInsert = INVALID_UNDO_REC_PTR;

    if (useInplaceUpdate) {
        SetCurrentTransactionUndoRecPtr(urecptr, persistence);
        UHeapPageSetUndo(buffer, oldtupNewTransSlot, fxid, urecptr);
    } else {
        SetCurrentTransactionUndoRecPtr(urecptr, persistence);
        oldPrevUrpInsert = GetCurrentTransactionUndoRecPtr(persistence);
        SetCurrentTransactionUndoRecPtr(newUrecptr, persistence);
        if (newbuf == buffer) {
            UHeapPageSetUndo(buffer, oldtupNewTransSlot, fxid, newUrecptr);
        } else {
            /* set transaction slot information for old page */
            UHeapPageSetUndo(buffer, oldtupNewTransSlot, fxid, urecptr);

            /* set transaction slot information for new page */
            UHeapPageSetUndo(newbuf, newtupTransSlot, fxid, newUrecptr);

            MarkBufferDirty(newbuf);
        }
    }

    MarkBufferDirty(buffer);
    undo::PrepareUndoMeta(&xlum, persistence, u_sess->ustore_cxt.urecvec->LastRecord(), 
        u_sess->ustore_cxt.urecvec->LastRecordSize());
    undo::UpdateTransactionSlot(fxid, &xlum, u_sess->ustore_cxt.urecvec->FirstRecord(), persistence);
    /* XLOG stuff */
    if (RelationNeedsWAL(relation)) {
        UHeapWALInfo oldupWalInfo = { 0 };
        UHeapWALInfo newupWalInfo = { 0 };
        uint8 oldXlUndoHeaderFlag = 0;
        uint8 newXlUndoHeaderFlag = 0;
        UndoRecord *oldurec = u_sess->ustore_cxt.undo_records[0];
        UndoRecord *newurec = u_sess->ustore_cxt.undo_records[1];
        TransactionId currentXid = InvalidTransactionId;

        if ((oldurec->Uinfo() & UNDO_UREC_INFO_TRANSAC) != 0) {
            oldXlUndoHeaderFlag |= XLOG_UNDO_HEADER_HAS_PREV_URP;
        }
        if ((newurec->Uinfo() & UNDO_UREC_INFO_TRANSAC) != 0) {
            newXlUndoHeaderFlag |= XLOG_UNDO_HEADER_HAS_PREV_URP;
        }
        if (prevUrecptr != INVALID_UNDO_REC_PTR) {
            oldXlUndoHeaderFlag |= XLOG_UNDO_HEADER_HAS_BLK_PREV;
        }
        if (newurec->Blkprev() != INVALID_UNDO_REC_PTR) {
            newXlUndoHeaderFlag |= XLOG_UNDO_HEADER_HAS_BLK_PREV;
        }
        if (hasSubXactLock) {
            oldXlUndoHeaderFlag |= XLOG_UNDO_HEADER_HAS_SUB_XACT;

            if (RelationIsLogicallyLogged(relation)) {
                oldXlUndoHeaderFlag |= XLOG_UNDO_HEADER_HAS_CURRENT_XID;
                currentXid = GetCurrentTransactionId();
            }
        }
        if (partitionOid != InvalidOid) {
            oldXlUndoHeaderFlag |= XLOG_UNDO_HEADER_HAS_PARTITION_OID;
            newXlUndoHeaderFlag |= XLOG_UNDO_HEADER_HAS_PARTITION_OID;
        }
        oldupWalInfo.buffer = buffer;
        oldupWalInfo.oldUTuple = undotup;
        oldupWalInfo.utuple = &oldtup;
        oldupWalInfo.urecptr = urecptr;
        oldupWalInfo.prevurp = oldPrevUrp;
        oldupWalInfo.blkprev = prevUrecptr;
        oldupWalInfo.xlum = &xlum;
        oldupWalInfo.hasSubXact = hasSubXactLock;
        oldupWalInfo.hZone = NULL;
        oldupWalInfo.td_id = oldtupNewTransSlot;
        oldupWalInfo.relOid = relOid;
        oldupWalInfo.partitionOid = partitionOid;
        oldupWalInfo.oldXid = txactinfo.xid;
        oldupWalInfo.oldurecadd = txactinfo.urec_add;
        oldupWalInfo.flag = oldXlUndoHeaderFlag;
        oldupWalInfo.xid = currentXid;

        newupWalInfo.buffer = newbuf;
        newupWalInfo.utuple = uheaptup;
        newupWalInfo.urecptr = newUrecptr;
        newupWalInfo.prevurp = oldPrevUrpInsert;
        newupWalInfo.xlum = &xlum;
        newupWalInfo.hZone = NULL;
        newupWalInfo.td_id = newtupTransSlot;
        newupWalInfo.blkprev = (*u_sess->ustore_cxt.urecvec)[1]->Blkprev();
        newupWalInfo.relOid = relOid;
        newupWalInfo.partitionOid = partitionOid;
        newupWalInfo.relfilenode = RelationGetRelFileNode(relation);
        newupWalInfo.flag = newXlUndoHeaderFlag;

        /* undo meta information */
        oldupWalInfo.hZone = (undo::UndoZone *)g_instance.undo_cxt.uZones[t_thrd.undo_cxt.zids[persistence]];
        Assert(oldupWalInfo.hZone != NULL);

        LogUHeapUpdate(&oldupWalInfo, &newupWalInfo, useInplaceUpdate, undoXorDeltaSize, xlogXorDelta, prefixlen,
            suffixlen, relation);
    }

    undo::FinishUndoMeta(&xlum, persistence);
    if (useInplaceUpdate) {
        pfree(xlogXorDelta);
    }

    END_CRIT_SECTION();
    /* be tidy */
    pfree(undotup.data);
    UHeapFinalizeDML<UHEAP_UPDATE>(relation, buffer, &newbuf, newtup, uheaptup, &(oldtup.ctid),
                                   haveTupleLock, useInplaceUpdate, slotReused);

    bms_free(inplaceUpdAttrs);
    bms_free(interestingAttrs);
    bms_free(modifiedAttrs);
    bms_free(keyAttrs);

    *indexkey_update_flag =
        !UHeapTupleIsInPlaceUpdated(((UHeapTuple)newtup)->disk_tuple->flag) || *modifiedIdxAttrs != NULL;
    return TM_Ok;
}


UHeapTuple ExecGetUHeapTupleFromSlot(TupleTableSlot *slot)
{
    if (slot == NULL)
        return NULL;

    if (!slot->tts_tuple) {
        TtsUHeapMaterialize(slot);
    }

    if (slot->tts_tuple != NULL)
        Assert(((UHeapTuple)slot->tts_tuple)->tupTableType == UHEAP_TUPLE);

    return (UHeapTuple)slot->tts_tuple;
}

/*
 * UHeapMultiInsert        - insert multiple tuples into a uheap
 *
 * Similar to heap_multi_insert(), but inserts uheap tuples
 */
void UHeapMultiInsert(Relation relation, UHeapTuple *tuples, int ntuples, CommandId cid, int options,
    BulkInsertState bistate)
{
    UHeapTuple *uheaptuples = NULL;
    int i;
    int ndone;
    char *scratch = NULL;
    uint16 lower;
    Page page;
    Size saveFreeSpace;
    UndoPersistence persistence = UndoPersistenceForRelation(relation);
    TransactionId fxid = GetTopTransactionId();
    UHeapPageHeaderData *phdr = NULL;
    URecVector *urecvec = NULL;
    TransactionId minXidInTDSlots = InvalidTransactionId;

    /* needwal can also be passed in by options */
    bool needwal = RelationNeedsWAL(relation);
    bool skipUndo = false;

    saveFreeSpace = RelationGetTargetPageFreeSpace(relation, HEAP_DEFAULT_FILLFACTOR);

    /* Toast and set header data in all the tuples */
    uheaptuples = (UHeapTupleData **)palloc(ntuples * sizeof(UHeapTuple));
    for (i = 0; i < ntuples; i++) {
        tuples[i] = UHeapPrepareInsert(relation, tuples[i], 0);
        uheaptuples[i] = tuples[i];
    }

    /*
     * Allocate some memory to use for constructing the WAL record. Using
     * palloc() within a critical section is not safe, so we allocate this
     * beforehand. This has consideration that offset ranges and tuples to be
     * stored in page will have size lesser than BLCKSZ. This is true since a
     * uheap page contains page header and td slots in special area
     * which are not stored in scratch area. In future, if we reduce the
     * number of td slots, we may need to allocate twice the
     * BLCKSZ of scratch area.
     */
    if (needwal) {
        errno_t rc;
        scratch = (char *)palloc(BLCKSZ);
        rc = memset_s(scratch, BLCKSZ, 0, BLCKSZ);
        securec_check_c(rc, "\0", "\0");
    }

    CheckForSerializableConflictIn(relation, NULL, InvalidBuffer);

    ndone = 0;

    while (ndone < ntuples) {
        undo::XlogUndoMeta xlum;
        Buffer buffer = InvalidBuffer;
        int nthispage = 0;
        int tdSlot = InvalidTDSlotId;
        UndoRecPtr urecPtr = INVALID_UNDO_REC_PTR, prevUrecptr = INVALID_UNDO_REC_PTR,
                   first_urecptr = INVALID_UNDO_REC_PTR;
        OffsetNumber maxRequiredOffset;
        bool lockReacquired = false;
        UHeapFreeOffsetRanges *ufreeOffsetRanges = NULL;
        bool switchBuf = false;
        bool aggressiveTDSearch = false;
        BlockNumber firstBlock = InvalidBlockNumber;

        CHECK_FOR_INTERRUPTS();

        /* IO collector and IO scheduler */
        if (ENABLE_WORKLOAD_CONTROL)
            IOSchedulerAndUpdate(IO_TYPE_WRITE, 1, IO_TYPE_ROW);

        WHITEBOX_TEST_STUB(UHEAP_MULTI_INSERT_FAILED, WhiteboxDefaultErrorEmit);

        UHeapResetWaitTimeForTDSlot();

reacquire_buffer:
        buffer = RelationGetBufferForUTuple(relation, uheaptuples[ndone]->disk_tuple_size, InvalidBuffer, options, 
            bistate, switchBuf);
        page = BufferGetPage(buffer);
        phdr = (UHeapPageHeaderData *)page;

        /*
         * Do aggressive TD slot search after switching buffer
         * and somehow ended up at the first block.
        */
        BlockNumber currBlock = BufferGetBlockNumber(buffer);
        if (firstBlock == InvalidBlockNumber) {
            firstBlock = currBlock;
        } else if (switchBuf && firstBlock == currBlock) {
            aggressiveTDSearch = true;
        }

        /*
         * Get the unused offset ranges in the page. This is required for
         * deciding the number of undo records to be prepared later.
         */
        ufreeOffsetRanges = UHeapGetUsableOffsetRanges(buffer, &uheaptuples[ndone], ntuples - ndone, saveFreeSpace);

        /*
         * We've ensured at least one tuple fits in the page. So, there'll be
         * at least one offset range.
         */
        Assert(ufreeOffsetRanges->nranges > 0);

        maxRequiredOffset = ufreeOffsetRanges->endOffset[ufreeOffsetRanges->nranges - 1];

        if (!skipUndo) {
            lower = phdr->pd_lower;
            tdSlot = UHeapPageReserveTransactionSlot(relation, buffer, fxid, &prevUrecptr, &lockReacquired, 
                InvalidBuffer, NULL, &minXidInTDSlots, aggressiveTDSearch);
            /*
             * It is possible that available space on the page changed
             * as part of TD reservation operation. If so, go back and reacquire the buffer.
             */
            if (lockReacquired || lower < phdr->pd_lower) {
                UnlockReleaseBuffer(buffer);
                switchBuf = false;
                goto reacquire_buffer;
            }

            if (tdSlot == InvalidTDSlotId) {
                UnlockReleaseBuffer(buffer);
                UHeapSleepOrWaitForTDSlot(minXidInTDSlots, fxid, true);
                // cant switch buffer anymore to avoid bouncing between blocks
                switchBuf = !aggressiveTDSearch;
                goto reacquire_buffer;
            }

            Assert(tdSlot != InvalidTDSlotId);

            Oid relOid = RelationIsPartition(relation) ? relation->parentId : RelationGetRelid(relation);
            Oid partitionOid = RelationIsPartition(relation) ? RelationGetRelid(relation) : InvalidOid;

            urecPtr = UHeapPrepareUndoMultiInsert(relOid, partitionOid, RelationGetRelFileNode(relation),
                RelationGetRnodeSpace(relation), persistence, buffer, ufreeOffsetRanges->nranges, fxid, cid,
                prevUrecptr, INVALID_UNDO_REC_PTR, &urecvec, &first_urecptr, NULL, InvalidBlockNumber, NULL, &xlum);
        }

        /* No ereport(ERROR) from here till changes are logged */
        START_CRIT_SECTION();
        /*
         * RelationGetBufferForUTuple has ensured that the first tuple fits.
         * Keep calm and put that on the page, and then as many other tuples
         * as fit.
         */
        nthispage = 0;
        UndoRecPtr urpvec[ufreeOffsetRanges->nranges];
        for (i = 0; i < ufreeOffsetRanges->nranges; i++) {
            OffsetNumber offnum;

            for (offnum = ufreeOffsetRanges->startOffset[i]; offnum <= ufreeOffsetRanges->endOffset[i]; offnum++) {
                UHeapTuple uheaptup;

                if (ndone + nthispage == ntuples)
                    break;

                uheaptup = uheaptuples[ndone + nthispage];

                /* Make sure that the tuple fits in the page. */
                Size pagefreespace = PageGetUHeapFreeSpace(page);
                if (pagefreespace < uheaptup->disk_tuple_size + saveFreeSpace)
                    break;
                UHeapTupleHeaderSetTDSlot(uheaptup->disk_tuple, tdSlot);
                UHeapTupleHeaderSetLockerTDSlot(uheaptup->disk_tuple, InvalidTDSlotId);
#ifdef USE_ASSERT_CHECKING
                CheckTupleValidity(relation, uheaptup);
#endif
                RelationPutUTuple(relation, buffer, uheaptup);

                UHeapRecordPotentialFreeSpace(relation, buffer, -1 * SHORTALIGN(uheaptup->disk_tuple_size));

                /*
                 * Let's make sure that we've decided the offset ranges
                 * correctly.
                 */
                Assert(offnum == ItemPointerGetOffsetNumber(&(uheaptup->ctid)));

                nthispage++;
            }

            /*
             * Store the offset ranges in undo payload. We've not calculated
             * the end offset for the last range previously. Hence, we set it
             * to offnum - 1. There is no harm in doing the same for previous
             * undo records as well.
             */
            ufreeOffsetRanges->endOffset[i] = offnum - 1;
            if (!skipUndo) {
                urpvec[i] = (*urecvec)[i]->Urp();
                initStringInfo((*urecvec)[i]->Rawdata());
                appendBinaryStringInfo((*urecvec)[i]->Rawdata(), (char *)&ufreeOffsetRanges->startOffset[i],
                    sizeof(OffsetNumber));
                appendBinaryStringInfo((*urecvec)[i]->Rawdata(), (char *)&ufreeOffsetRanges->endOffset[i],
                    sizeof(OffsetNumber));
            }

            elog(DEBUG1, "start offset: %d, end offset: %d", ufreeOffsetRanges->startOffset[i],
                ufreeOffsetRanges->endOffset[i]);
        }

        UndoRecPtr oldPrevUrp = GetCurrentTransactionUndoRecPtr(persistence);

        if (!skipUndo) {
            /* Insert the undo */
            InsertPreparedUndo(urecvec);

            for (i = 0; i < ufreeOffsetRanges->nranges; i++) {
                UndoRecPtr urecptr = (*urecvec)[i]->Urp();
                Assert(IS_VALID_UNDO_REC_PTR(urecptr));
                SetCurrentTransactionUndoRecPtr(urecptr, persistence);
            }

            /*
             * We're sending the undo record for debugging purpose. So, just
             * send the last one.
             */
            UHeapPageSetUndo(buffer, tdSlot, fxid, urecPtr);
            undo::PrepareUndoMeta(&xlum, persistence, urecvec->LastRecord(), urecvec->LastRecordSize());
            undo::UpdateTransactionSlot(fxid, &xlum, urecvec->FirstRecord(), persistence);
        }

        MarkBufferDirty(buffer);

        /* XLOG stuff */
        if (needwal) {
            UHeapMultiInsertWALInfo insWalInfo;
            UHeapWALInfo genWalInfo;
            uint8 xlUndoHeaderFlag = 0;
            TransactionId currentXid = InvalidTransactionId;
            
            if (((*urecvec)[0]->Uinfo() & (*urecvec)[0]->Urp()) != 0) {
                xlUndoHeaderFlag |= XLOG_UNDO_HEADER_HAS_PREV_URP;
            }
            if (prevUrecptr != INVALID_UNDO_REC_PTR) {
                xlUndoHeaderFlag |= XLOG_UNDO_HEADER_HAS_BLK_PREV;
            }
            if (RelationIsPartition(relation)) {
                xlUndoHeaderFlag |= XLOG_UNDO_HEADER_HAS_PARTITION_OID;
            }
            if (IsSubTransaction() && RelationIsLogicallyLogged(relation)) {
                xlUndoHeaderFlag |= XLOG_UNDO_HEADER_HAS_CURRENT_XID;
                currentXid = GetCurrentTransactionId();
            }

            genWalInfo.buffer = buffer;
            genWalInfo.utuple = NULL;
            genWalInfo.urecptr = first_urecptr;
            genWalInfo.blkprev = prevUrecptr;
            genWalInfo.prevurp = oldPrevUrp;
            genWalInfo.td_id = tdSlot;
            genWalInfo.xlum = &xlum;
            genWalInfo.flag = xlUndoHeaderFlag;
            genWalInfo.partitionOid = RelationGetRelid(relation);
            genWalInfo.xid = currentXid;
            genWalInfo.hZone = (undo::UndoZone *)g_instance.undo_cxt.uZones[t_thrd.undo_cxt.zids[persistence]];

            insWalInfo.genWalInfo = &genWalInfo;
            insWalInfo.relation = relation;
            insWalInfo.utuples = uheaptuples;
            insWalInfo.ufree_offsets = ufreeOffsetRanges;
            insWalInfo.ntuples = ntuples;
            insWalInfo.curpage_ntuples = nthispage;
            insWalInfo.ndone = ndone;
            insWalInfo.lastURecptr = urecPtr;
            LogUHeapMultiInsert(&insWalInfo, skipUndo, scratch, urpvec);
        }

        if (!skipUndo) {
            undo::FinishUndoMeta(&xlum, persistence);
        }

        END_CRIT_SECTION();

        pfree(ufreeOffsetRanges);
        UnlockReleaseBuffer(buffer);
        if (!skipUndo) {
            urecvec->Reset();
            UHeapResetPreparedUndo(persistence, INVALID_ZONE_ID);
            urecvec->Destroy();
        }

        ndone += nthispage;
        switchBuf = false;
    }

    /*
     * We're done with the actual inserts.  Check for conflicts again, to
     * ensure that all rw-conflicts in to these inserts are detected.  Without
     * this final check, a sequential scan of the heap may have locked the
     * table after the "before" check, missing one opportunity to detect the
     * conflict, and then scanned the table before the new tuples were there,
     * missing the other chance to detect the conflict.
     *
     * For heap inserts, we only need to check for table-level SSI locks. Our
     * new tuples can't possibly conflict with existing tuple locks, and heap
     * page locks are only consolidated versions of tuple locks; they do not
     * lock "gaps" as index page locks do.  So we don't need to specify a
     * buffer when making the call.
     */
    CheckForSerializableConflictIn(relation, NULL, InvalidBuffer);

    /*
     * Copy ctid fields back to the caller's original tuples. This does
     * nothing for untoasted tuples (tuples[i] == heaptuples[i)], but it's
     * probably faster to always copy than check.
     */
    for (i = 0; i < ntuples; i++)
        tuples[i]->ctid = uheaptuples[i]->ctid;

    pgstat_count_heap_insert(relation, ntuples);
}

static void TtsUHeapMaterialize(TupleTableSlot *slot)
{
    MemoryContext oldContext;

    Assert(!slot->tts_isempty);

    /* If already materialized nothing to do. */
    if (slot->tts_shouldFree)
        return;

    slot->tts_shouldFree = true;

    oldContext = MemoryContextSwitchTo(slot->tts_mcxt);

    /*
     * The tuple contained in this slot is not allocated in the memory
     * context of the given slot (else it would have TTS_SHOULDFREE set).
     * Copy the tuple into the given slot's memory context.
     */
    slot->tts_tuple = UHeapFormTuple(slot->tts_tupleDescriptor, slot->tts_values, slot->tts_isnull);

    /* Let the caller know this contains a UHeap tuple now */
    slot->tts_tupslotTableAm = TAM_USTORE;

    MemoryContextSwitchTo(oldContext);

    /*
     * Have to deform from scratch, otherwise tts_values[] entries could point
     * into the non-materialized tuple (which might be gone when accessed).
     */
    slot->tts_nvalid = 0;
}
/*
 * UPageGetTDSlotId - Get the TD slot for the given epoch and xid.
 */
int UPageGetTDSlotId(Buffer buf, TransactionId fxid, UndoRecPtr *urecAdd)
{
    Page page;
    int slotNo;
    int tdCount;
    UHeapPageTDData *tdPtr;

    page = BufferGetPage(buf);
    tdPtr = (UHeapPageTDData *)PageGetTDPointer(page);
    tdCount = UPageGetTDSlotCount(page);
    /* Check if the required slot exists on the page. */
    for (slotNo = 0; slotNo < tdCount; slotNo++) {
        TD *thistrans = &tdPtr->td_info[slotNo];

#ifdef DEBUG_USTORE
        elog(LOG,
            "UPageGetTDSlotId LOG_UNDO_RECORD_ACTIONS: "
            "TransSlot: %d, urecPtr: %d, TransactionId: %d, TopTransactionId: %d",
            slotNo + 1, thistrans->undo_record_ptr, thistrans->xactid, GetTopTransactionId());
#endif

        if (TransactionIdEquals(thistrans->xactid, fxid)) {
            *urecAdd = thistrans->undo_record_ptr;

            return slotNo + 1;
        }
    }

    return InvalidTDSlotId;
}

static bool UHeapPageReserveTransactionSlotReuseLoop(int *pslotNo, Page page, UndoRecPtr *urecPtr, bool *slotReused)
{
    int slotNo;
    int tdCount = UPageGetTDSlotCount(page);
    UHeapPageTDData *tdPtr = (UHeapPageTDData *)PageGetTDPointer(page);

    for (slotNo = 0; slotNo < tdCount; slotNo++) {
        TD *thistrans = &tdPtr->td_info[slotNo];

        if (!TransactionIdIsValid(thistrans->xactid)) {
            *urecPtr = thistrans->undo_record_ptr;

            if (slotReused && (*urecPtr != INVALID_UNDO_REC_PTR)) {
                *slotReused = true;
            }
#ifdef DEBUG_UHEAP
            if (*urecPtr != INVALID_UNDO_REC_PTR) {
                /* Got a slot after invalidation */
                UHEAPSTAT_COUNT_GET_TRANSSLOT_FROM(TRANSSLOT_FREE_AFTER_INVALIDATION);
            } else {
                /* Got a slot after freezing */
                UHEAPSTAT_COUNT_GET_TRANSSLOT_FROM(TRANSSLOT_FREE_AFTER_FREEZING);
            }
#endif
            *pslotNo = slotNo;
            return true;
        }
    }

    return false;
}

/*
 * UHeapPageReserveTransactionSlot - Reserve the transaction slot in page.
 *
 * This function returns transaction slot number if either the page already
 * has some slot that contains the transaction info or there is an empty
 * slot or it manages to reuse some existing slot ; otherwise returns
 * InvalidTDSlotId.
 *
 * Note that we always return array location of slot plus one as zeroth slot
 * number is reserved for frozen slot number (UHEAPTUP_SLOT_FROZEN).
 *
 * If we've reserved a transaction slot of a committed but not all-visible
 * transaction, we set slotReused as true, false otherwise.
 * 
 * aggressiveSearch - we try to reuse td slots from committed and aborted txns.
 * If none, we extend the td slots beyond the initial threshold
 */
int UHeapPageReserveTransactionSlot(Relation relation, Buffer buf, TransactionId fxid,
    UndoRecPtr *urecPtr, bool *lockReacquired, Buffer otherBuf, bool *slotReused,
    TransactionId *minXid, bool aggressiveSearch)
{
    Page page = BufferGetPage(buf);
    int latestFreeTDSlot = InvalidTDSlotId;
    int slotNo;
    int nExtended;
    int tdCount = UPageGetTDSlotCount(page);
    UHeapPageTDData *tdPtr = (UHeapPageTDData *)PageGetTDPointer(page);
    TransactionId currMinXid = MaxTransactionId;

    *lockReacquired = false;

    /*
     * For temp relations, we don't have to check all the slots since no other
     * backend can access the same relation. If a slot is available, we return
     * it from here. Else, we freeze the slot in PageFreezeTransSlots.
     *
     * XXX For temp tables, oldestXidInUndo is not relevant as
     * the undo for them can be discarded on commit.  Hence, comparing xid
     * with oldestXidInUndo during visibility checks can lead to
     * incorrect behavior.  To avoid that, we can mark the tuple as frozen for
     * any previous transaction id.  In that way, we don't have to compare the
     * previous xid of tuple with oldestXidInUndo.
     */
    if (RELATION_IS_LOCAL(relation)) {
        /* We can't access temp tables of other backends. */
        Assert(!RELATION_IS_OTHER_TEMP(relation));

        slotNo = 0;
        TD *thistrans = &tdPtr->td_info[slotNo];

        if (TransactionIdEquals(thistrans->xactid, fxid)) {
            *urecPtr = thistrans->undo_record_ptr;
            return (slotNo + 1);
        } else if (!TransactionIdIsValid(thistrans->xactid))
            latestFreeTDSlot = slotNo;
    } else {
        for (slotNo = 0; slotNo < tdCount; slotNo++) {
            TD *thistrans = &tdPtr->td_info[slotNo];

            if (TransactionIdIsValid(thistrans->xactid)) {
                if (TransactionIdEquals(thistrans->xactid, fxid)) {
                    /* Already reserved by ourself */
                    *urecPtr = thistrans->undo_record_ptr;
#ifdef DEBUG_UHEAP
                    UHEAPSTAT_COUNT_GET_TRANSSLOT_FROM(TRANSSLOT_RESERVED_BY_CURRENT_XID);
#endif
                    return (slotNo + 1);
                } else {
                    currMinXid = Min(currMinXid, thistrans->xactid);
                }
            } else if (latestFreeTDSlot == InvalidTDSlotId) {
                /* Got an available slot */
                latestFreeTDSlot = slotNo;
            }
        }
    }

    if (latestFreeTDSlot >= 0) {
        *urecPtr = tdPtr->td_info[latestFreeTDSlot].undo_record_ptr;
        return (latestFreeTDSlot + 1);
    }

    /*
     * The caller waits for the oldest xid to avoid infinite loop of sleep and recheck.
     *
     * Note it may be worth spreading out threads to wait for different xids but for now
     * we force all of them to wait for the oldest xid. The next time before waiting
     * for oldest xid again, each will scan all slots to see if any are free so this may
     * not be an issue.
     */
    *minXid = currMinXid;

    /* no transaction slot available, try to reuse some existing slot */
    if (UHeapPageFreezeTransSlots(relation, buf, lockReacquired, NULL, otherBuf, aggressiveSearch)) {
        /*
         * If the lock is reacquired inside, then we allow callers to reverify
         * the condition whether then can still perform the required
         * operation.
         */
        if (*lockReacquired)
            return InvalidTDSlotId;

        if (UHeapPageReserveTransactionSlotReuseLoop(&slotNo, page, urecPtr, slotReused)) {
            return (slotNo + 1);
        }

        /*
         * After freezing transaction slots, we should get at least one free
         * slot.
         */
        Assert(false);
    }

    if (!aggressiveSearch && tdCount >= TD_THRESHOLD_FOR_PAGE_SWITCH) {
        /*
         * Do not extend TD array if the TD allocation request is
         * for an insert statement and the page already has
         * TD_THRESHOLD_FOR_PAGE_SWITCH TD slots. Since we do not support
         * TD array shrinking, we insert on a different page
         * with available TDs.
         */
        ereport(DEBUG5, (errmsg("Could not extend TD slots beyond threshold Rel: %s, blkno: %d",
            RelationGetRelationName(relation), BufferGetBlockNumber(buf))));
        return InvalidTDSlotId;
    }
    /*
     * Unable to find an unused TD slot or reuse one.
     * Try to extend the ITL array now.
     */
    nExtended = UPageExtendTDSlots(relation, buf);
    if (nExtended > 0) {
        /*
         * We just extended the number of slots.
         * Return first slot from the extended ones.
         */
        ereport(DEBUG5, (errmsg("TD array extended by %d slots for Rel: %s, blkno: %d",
            nExtended, RelationGetRelationName(relation), BufferGetBlockNumber(buf))));
        return (tdCount + 1);
    }
    ereport(DEBUG5, (errmsg("Could not extend TD array for Rel: %s, blkno: %d",
        RelationGetRelationName(relation), BufferGetBlockNumber(buf))));

#ifdef DEBUG_UHEAP
    UHEAPSTAT_COUNT_GET_TRANSSLOT_FROM(TRANSSLOT_CANNOT_GET);
#endif

    /* no transaction slot available */
    return InvalidTDSlotId;
}

/*
 * UHeapPageFreezeTransSlots - Make the transaction slots available for reuse.
 *
 * This function tries to free up some existing transaction slots so that
 * they can be reused.  To reuse the slot, it needs to ensure one of the below
 * conditions: (a) the xid is committed, all-visible and doesn't have pending rollback
 * to perform.
 *             (b) if the xid is committed, then ensure to mark a special flag on the
 * tuples that are modified by that xid on the current page.
 *             (c) if the xid is rolled back, then ensure that rollback is performed or
 * at least undo actions for this page have been replayed.
 *
 * For committed/aborted transactions, we simply clear the xid from the
 * transaction slot and undo record pointer is kept as it is to ensure that
 * we don't break the undo chain for that slot. We also mark the tuples that
 * are modified by committed xid with a special flag indicating that slot for
 * this tuple is reused.  The special flag is just an indication that the
 * transaction information of the transaction that has modified the tuple can
 * be retrieved from the undo.
 *
 * If we don't do so, then after that slot got reused for some other
 * unrelated transaction, it might become tricky to traverse the undo chain.
 * In such a case, it is quite possible that the particular tuple has not
 * been modified, but it is still pointing to transaction slot which has been
 * reused by new transaction and that transaction is still not committed.
 * During the visibility check for such a tuple, it can appear that the tuple
 * is modified by current transaction which is clearly wrong and can lead to
 * wrong results.  One such case would be when we try to fetch the commandid
 * for that tuple to check the visibility, it will fetch the commandid for a
 * different transaction that is already committed.
 *
 * The basic principle used here is to ensure that we can always fetch the
 * transaction information of tuple until it is frozen (committed and
 * all-visible).
 *
 * This also ensures that we are consistent with how other operations work in
 * UHeap i.e. the tuple always reflect the current state.
 *
 * We don't need any special handling for the tuples that are locked by
 * multiple transactions (aka tuples that have MULTI_LOCKERS bit set).
 * Basically, we always maintain either strongest lockers or latest lockers
 * (when all the lockers are of same mode) transaction slot on the tuple.
 * In either case, we should be able to detect the visibility of tuple based
 * on the latest locker information.
 *
 * use_aborted_slot indicates whether we can reuse the slot of aborted
 * transaction or not.
 *
 * This function assumes that the caller already has Exclusive lock on the
 * buffer.
 *
 * otherBuf will be valid only in case of non in-place update in two
 * different buffers and otherBuf will be old buffer.  Caller of
 * UHeapReserveDualPageTDSlot will not try to release lock again.
 *
 * aggressiveFreeze will not only consider xids older than oldestXidInUndo but also 
 * try to reuse slots from committed and aborted transactions.
 * 
 * This function returns true if it manages to free some transaction slot,
 * false otherwise.
 */
bool UHeapPageFreezeTransSlots(Relation relation, Buffer buf, bool *lockReacquired, TD *transinfo, 
    Buffer otherBuf, bool aggressiveFreeze)
{
    int nFrozenSlots = 0;
    int *completedXactSlots = NULL;
    uint16 nCompletedXactSlots = 0;
    int *abortedXactSlots = NULL;
    int nAbortedXactSlots = 0;
    bool result = false;

    Page page = BufferGetPage(buf);
    int numSlots = GetTDCount((UHeapPageHeaderData *)page);
    UHeapPageTDData *tdPtr = (UHeapPageTDData *)PageGetTDPointer(page);
    transinfo = tdPtr->td_info;
    TransactionId oldestXid = pg_atomic_read_u64(&g_instance.proc_base->oldestXidInUndo);

    /*
     * Clear the slot information from tuples.  The basic idea is to collect
     * all the transaction slots that can be cleared.  Then traverse the page
     * to see if any tuple has marking for any of the slots, if so, just clear
     * the slot information from the tuple.
     *
     * For temp relations, we can freeze the first slot since no other backend
     * can access the same relation.
     */
    int *frozenSlots = (int *)palloc0(numSlots * sizeof(int));
    if (RELATION_IS_LOCAL(relation)) {
        frozenSlots[nFrozenSlots++] = 0;
    } else {
        for (int slotNo = 0; slotNo < numSlots; slotNo++) {
            TransactionId slotXactid = transinfo[slotNo].xactid;
            /*
             * Transaction slot can be considered frozen if it belongs to
             * transaction id is old enough that it is all visible.
             */

            if (TransactionIdPrecedes(slotXactid, oldestXid)) {
                frozenSlots[nFrozenSlots++] = slotNo;
            }
        }
    }

    if (nFrozenSlots > 0) {
        TransactionId latestfxid = InvalidTransactionId;
        int slotNo;

        START_CRIT_SECTION();

        /* clear the transaction slot info on tuples */
        UHeapFreezeOrInvalidateTuples(buf, nFrozenSlots, frozenSlots, true);

        /* Initialize the frozen slots. */
        for (int i = 0; i < nFrozenSlots; i++) {
            TD *thistrans;

            slotNo = frozenSlots[i];
            thistrans = &transinfo[slotNo];

            /* Remember the latest xid. */
            if (TransactionIdFollows(thistrans->xactid, latestfxid))
                latestfxid = thistrans->xactid;

            thistrans->xactid = InvalidTransactionId;
            thistrans->undo_record_ptr = INVALID_UNDO_REC_PTR;
        }

        MarkBufferDirty(buf);

        /*
         * xlog Stuff
         *
         * Log all the frozenSlots number for which we need to clear the
         * transaction slot information.  Also, note down the latest xid
         * corresponding to the frozen slots. This is required to ensure that
         * no standby query conflicts with the frozen xids.
         */
        if (RelationNeedsWAL(relation)) {
            XlUHeapFreezeTdSlot xlrec = { 0 };
            XLogRecPtr recptr;

            XLogBeginInsert();

            xlrec.nFrozen = nFrozenSlots;
            xlrec.latestFrozenXid = latestfxid;

            XLogRegisterData((char *)&xlrec, SizeOfUHeapFreezeTDSlot);

            /*
             * We need the frozen slots information when WAL needs to
             * be applied on the page..
             */
            XLogRegisterData((char *)frozenSlots, nFrozenSlots * sizeof(int));
            XLogRegisterBuffer(0, buf, REGBUF_STANDARD);

            recptr = XLogInsert(RM_UHEAP_ID, XLOG_UHEAP_FREEZE_TD_SLOT);
            PageSetLSN(page, recptr);
        }

        END_CRIT_SECTION();

        result = true;
        goto cleanup;
    }

    /* Some callers want a fast way to check if there is a reusable td slot or not. */
    if (!aggressiveFreeze) {
        goto cleanup;
    }

    Assert(!RELATION_IS_LOCAL(relation));
    completedXactSlots = (int *)palloc0(numSlots * sizeof(int));
    abortedXactSlots = (int *)palloc0(numSlots * sizeof(int));

    /*
     * Try to reuse transaction slots of committed/aborted transactions. This
     * is just like above but it will maintain a link to the previous
     * transaction undo record in this slot.  This is to ensure that if there
     * is still any alive snapshot to which this transaction is not visible,
     * it can fetch the record from undo and check the visibility.
     */
    for (int slotNo = 0; slotNo < numSlots; slotNo++) {
        TransactionId slotXid = transinfo[slotNo].xactid;

        if (!TransactionIdIsInProgress(slotXid, NULL, false, false, true)) {
            if (TransactionIdDidCommit(slotXid))
                completedXactSlots[nCompletedXactSlots++] = slotNo;
            else
                abortedXactSlots[nAbortedXactSlots++] = slotNo;
        }
    }

    if (nCompletedXactSlots > 0) {
        int i;
        int slotNo;

        START_CRIT_SECTION();

        /* clear the transaction slot info on tuples */
        UHeapFreezeOrInvalidateTuples(buf, nCompletedXactSlots, completedXactSlots, false);

        /*
         * Clear the xid information from the slot but keep the undo record
         * pointer as it is so that undo records of the transaction are
         * accessible by traversing slot's undo chain even though the slots
         * are reused.
         */
        for (i = 0; i < nCompletedXactSlots; i++) {
            slotNo = completedXactSlots[i];
            transinfo[slotNo].xactid = InvalidTransactionId;
        }
        MarkBufferDirty(buf);

        /*
         * Xlog Stuff
         */
        if (RelationNeedsWAL(relation)) {
            XLogBeginInsert();

            XLogRegisterData((char *)&nCompletedXactSlots, sizeof(uint16));
            XLogRegisterData((char *)completedXactSlots, nCompletedXactSlots * sizeof(int));

            XLogRegisterBuffer(0, buf, REGBUF_STANDARD);

            XLogRecPtr recptr = XLogInsert(RM_UHEAP_ID, XLOG_UHEAP_INVALID_TD_SLOT);
            PageSetLSN(page, recptr);
        }

        END_CRIT_SECTION();

        result = true;
        goto cleanup;
    } else if (nAbortedXactSlots) {
        int i;
        UndoRecPtr *urecptr = (UndoRecPtr *)palloc(nAbortedXactSlots * sizeof(UndoRecPtr));
        TransactionId *fxid = (TransactionId *)palloc(nAbortedXactSlots * sizeof(TransactionId));

        /* Collect slot information before releasing the lock. */
        for (i = 0; i < nAbortedXactSlots; i++) {
            TD *thistrans = &transinfo[abortedXactSlots[i]];

            urecptr[i] = thistrans->undo_record_ptr;
            fxid[i] = thistrans->xactid;
        }

        /*
         * We need to release and the lock before applying undo actions for a
         * page as we might need to traverse the long undo chain for a page.
         */
        LockBuffer(buf, BUFFER_LOCK_UNLOCK);

        /*
         * Release the lock on the other buffer to avoid deadlock as we need
         * to relock the new buffer again.  We could optimize here by
         * releasing the lock on old buffer conditionally (when the old block
         * number is bigger than new block number), but that would complicate
         * the handling.  If we ever want to deal with it, we need to ensure
         * that after reacquiring lock on new page, it is still a heap page
         * and also we need to pass this information to the caller.
         */
        if (BufferIsValid(otherBuf))
            LockBuffer(otherBuf, BUFFER_LOCK_UNLOCK);

        for (i = 0; i < nAbortedXactSlots; i++) {
            ExecuteUndoActionsPage(urecptr[i], relation, buf, fxid[i]);
        }

        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
        *lockReacquired = true;

        pfree(urecptr);
        pfree(fxid);

        result = true;
        goto cleanup;
    }

cleanup:
    if (frozenSlots != NULL)
        pfree(frozenSlots);
    if (completedXactSlots != NULL)
        pfree(completedXactSlots);
    if (abortedXactSlots != NULL)
        pfree(abortedXactSlots);

    return result;
}

static bool UHeapFreezeOrInvalidateTuplesSetTd(const RowPtr *rowptr, const Page page, int *tdSlot, int *lockerTdId)
{
    UHeapDiskTuple tupHdr;

    if (RowPtrIsDead(rowptr))
        return true;

    if (!RowPtrIsUsed(rowptr)) {
        if (!RowPtrHasPendingXact(rowptr))
            return true;
        *tdSlot = RowPtrGetTDSlot(rowptr);
    } else if (RowPtrIsDeleted(rowptr)) {
        *tdSlot = RowPtrGetTDSlot(rowptr);
    } else {
        tupHdr = (UHeapDiskTuple)UPageGetRowData(page, rowptr);
        *tdSlot = UHeapTupleHeaderGetTDSlot(tupHdr);
        *lockerTdId = UHeapTupleHeaderGetLockerTDSlot(tupHdr);
    }

    return false;
}

/*
 * UHeapFreezeOrInvalidateTuples - Clear the slot information or set
 * invalid_xact flags.
 *
 * Process all the tuples on the page and match their transaction slot with
 * the input slot array, if tuple is pointing to the slot then set the tuple
 * slot as UHEAPTUP_SLOT_FROZEN if is frozen is true otherwise set
 * UHEAP_INVALID_XACT_SLOT flag on the tuple
 */
void UHeapFreezeOrInvalidateTuples(Buffer buf, int nSlots, const int *slots, bool isFrozen)
{
    Page page = BufferGetPage(buf);

    /* clear the slot info from tuples */
    OffsetNumber maxoff = UHeapPageGetMaxOffsetNumber(page);

    for (OffsetNumber offnum = FirstOffsetNumber; offnum <= maxoff; offnum = OffsetNumberNext(offnum)) {
        UHeapDiskTuple tupHdr;
        int tdSlot, locker_td_id;

        RowPtr *rowptr = UPageGetRowPtr(page, offnum);

        locker_td_id = InvalidTDSlotId;

        if (UHeapFreezeOrInvalidateTuplesSetTd(rowptr, page, &tdSlot, &locker_td_id)) {
            continue;
        }

        /*
         * The slot number on tuple is always array location of slot plus
         * one, so we need to subtract one here before comparing it with
         * frozen slots.  See PageReserveTransactionSlot.
         */
        tdSlot -= 1;
        locker_td_id -= 1;

        for (int i = 0; i < nSlots; i++) {
            if (locker_td_id == slots[i]) {
                tupHdr = (UHeapDiskTuple)UPageGetRowData(page, rowptr);
                UHeapTupleHeaderSetLockerTDSlot(tupHdr, UHEAPTUP_SLOT_FROZEN);
                tupHdr->flag &= ~UHEAP_XID_LOCK_ONLY;
            }

            if (tdSlot == slots[i]) {
                /*
                 * Set transaction slots of tuple as frozen to indicate tuple
                 * is all visible and mark the deleted itemids as dead.
                 */
                if (isFrozen) {
                    if (!RowPtrIsUsed(rowptr)) {
                        /*
                         * This must be unused entry which has xact
                         * information.
                         */
                        Assert(RowPtrHasPendingXact(rowptr));

                        /*
                         * The pending xact must be committed if the
                         * corresponding slot is being marked as frozen.  So,
                         * clear the pending xact and transaction slot
                         * information from itemid.
                         */
                        RowPtrSetUnused(rowptr);
                    } else if (RowPtrIsDeleted(rowptr)) {
                        /*
                         
                         * the corresponding slot is being marked as frozen.
                         * So, marking it as dead.
                         */
                        RowPtrSetDead(rowptr);
                    } else {
                        tupHdr = (UHeapDiskTuple)UPageGetRowData(page, rowptr);
                        UHeapTupleHeaderSetTDSlot(tupHdr, UHEAPTUP_SLOT_FROZEN);
                    }
                } else {
                    /*
                     * We just append the invalid xact flag in the
                     * tuple/itemid to indicate that for this tuple/itemid we
                     * need to fetch the transaction information from undo
                     * record.  Also, we ensure to clear the transaction
                     * information from unused itemid.
                     */
                    if (!RowPtrIsUsed(rowptr)) {
                        /*
                         * This must be unused entry which has xact
                         * information.
                         */
                        Assert(RowPtrHasPendingXact(rowptr));

                        /*
                         * The pending xact is committed.  So, clear the
                         * pending xact and transaction slot information from
                         * itemid.
                         */
                        RowPtrSetUnused(rowptr);
                    } else if (RowPtrIsDeleted(rowptr))
                        RowPtrSetInvalidXact(rowptr);
                    else {
                        tupHdr = (UHeapDiskTuple)UPageGetRowData(page, rowptr);
                        tupHdr->flag |= UHEAP_INVALID_XACT_SLOT;
                    }
                }
            }
        }
    }
}

/*
 * UHeapReserveDualPageTDSlot - Reserve the transaction slots on old and
 * new buffer.
 */
void UHeapReserveDualPageTDSlot(Relation relation, Buffer oldbuf, Buffer newbuf, TransactionId fxid,
    UndoRecPtr *oldbufPrevUrecptr, UndoRecPtr *newbufPrevUrecptr, int *oldbufTransSlotId, int *newbufTransSlotId,
    bool *lockReacquired, bool *oldbufLockReacquired, TransactionId *minXidInTDSlots, bool *oldBufLockReleased)
{
    Page oldHeapPage;
    Page newHeapPage;

    *oldBufLockReleased = false;
    oldHeapPage = BufferGetPage(oldbuf);
    newHeapPage = BufferGetPage(newbuf);

    /* Reserve the transaction slot for new buffer. */
    *newbufTransSlotId = UHeapPageReserveTransactionSlot(relation, newbuf, fxid,
        newbufPrevUrecptr, lockReacquired, oldbuf, NULL, minXidInTDSlots);

    /*
     * Try again if the buffer lock is released and reacquired. Or if we
     * are not able to reserve any slot.
     * 
     * If we have reacquired the lock while reserving a slot, then
     * we would have already released lock on the old buffer.  See
     * otherBuf handling in UHeapPageFreezeTransSlots.
     */
    if (*lockReacquired || (*newbufTransSlotId == InvalidTDSlotId)) {
        *oldBufLockReleased = *lockReacquired;
        return;
    }

    /* Get the transaction slot for old buffer. */
    *oldbufTransSlotId = UHeapPageReserveTransactionSlot(relation, oldbuf, fxid, oldbufPrevUrecptr,
        oldbufLockReacquired, newbuf, NULL, minXidInTDSlots);
}

/*
 * UHeapPageSetUndo - Set the transaction information pointer for a given
 * TD slot.
 */
void UHeapPageSetUndo(Buffer buffer, int transSlotId, TransactionId fxid, UndoRecPtr urecptr)
{
    int tdCount;
    Page page = BufferGetPage(buffer);
    UHeapPageHeaderData *phdr PG_USED_FOR_ASSERTS_ONLY = (UHeapPageHeaderData *)page;
    tdCount = UPageGetTDSlotCount(page);

    Assert(transSlotId != InvalidTDSlotId);
    Assert(transSlotId <= phdr->td_count);

    UHeapPageTDData *tdPtr = (UHeapPageTDData *)PageGetTDPointer(page);
    TD *transinfo = tdPtr->td_info;

    /*
     * Set the required information in the TD slot.
     */
    if (transSlotId <= tdCount) {
        TD *thistrans = &transinfo[transSlotId - 1];

        thistrans->xactid = fxid;
        thistrans->undo_record_ptr = urecptr;
    }
}


/*
 * Given two versions of the same "flag" for a tuple, compare them and
 * return whether the relevant status for a tuple xid has changed.  This is
 * used after a buffer lock has been released and reacquired: we want to ensure
 * that the tuple state continues to be the same it was when we previously
 * examined it.
 *
 * Note the xid field itself must be compared separately.
 */
static inline bool XidInfomaskChanged(uint16 newInfomask, uint16 oldInfomask)
{
    const uint16 interesting = UHEAP_MULTI_LOCKERS | UHEAP_XID_LOCK_ONLY | UHEAP_LOCK_MASK;

    if ((newInfomask & interesting) != (oldInfomask & interesting))
        return true;

    return false;
}


/*
 * UHeapDetermineModifiedColumns - Check which columns are being updated.
 * This is same as HeapDetermineModifiedColumns except that it takes
 * UHeapTuple as input.
 */
static Bitmapset *UHeapDetermineModifiedColumns(Relation relation, Bitmapset *interesting_cols, UHeapTuple oldtup,
    UHeapTuple newtup)
{
    return UHeapTupleAttrEquals(RelationGetDescr(relation), interesting_cols, oldtup, newtup);
}

CommandId UHeapTupleGetCid(UHeapTuple utuple, Buffer buffer)
{
    int tdId = UHeapTupleHeaderGetTDSlot(utuple->disk_tuple);

    /* Get the latest urecptr from the page */
    UHeapTupleTransInfo tdinfo;
    GetTDSlotInfo(buffer, tdId, &tdinfo);

    if (tdinfo.td_slot == UHEAPTUP_SLOT_FROZEN || TransactionIdOlderThanAllUndo(tdinfo.xid)) {
        return InvalidCommandId;
    }

    Assert(IS_VALID_UNDO_REC_PTR(tdinfo.urec_add));
    UndoRecord *urec = New(CurrentMemoryContext)UndoRecord();
    urec->Reset(tdinfo.urec_add);

    int rc = FetchUndoRecord(urec, InplaceSatisfyUndoRecord, ItemPointerGetBlockNumber(&utuple->ctid),
        ItemPointerGetOffsetNumber(&utuple->ctid), InvalidTransactionId);
    if (rc == UNDO_RET_FAIL) {
        return InvalidCommandId;
    }

    CommandId currentCid = urec->Cid();

    DELETE_EX(urec);

    return currentCid;
}


void GetTDSlotInfo(Buffer buf, int tdId, UHeapTupleTransInfo *tdinfo)
{
    if (tdId == UHEAPTUP_SLOT_FROZEN) {
        tdinfo->td_slot = tdId;
        tdinfo->cid = InvalidCommandId;
        tdinfo->xid = InvalidTransactionId;
        tdinfo->urec_add = INVALID_UNDO_REC_PTR;
    } else {
        Page page = BufferGetPage(buf);
        UHeapPageTDData *tdPtr = (UHeapPageTDData *)PageGetTDPointer(page);
        UHeapPageHeaderData *phdr PG_USED_FOR_ASSERTS_ONLY = (UHeapPageHeaderData *)page;
        if (tdId < 1 || tdId > phdr->td_count) {
            ereport(PANIC, (errmsg("An out of bounds access was made to the array td_info")));
        }
        TD *thistrans = &tdPtr->td_info[tdId - 1];

        tdinfo->td_slot = tdId;
        tdinfo->cid = InvalidCommandId;
        tdinfo->urec_add = thistrans->undo_record_ptr;
        tdinfo->xid = thistrans->xactid;
    }
}

void UHeapResetPreparedUndo(UndoPersistence persistType, int zid)
{
    /* Reset undo records. */
    u_sess->ustore_cxt.urecvec->Reset();

    /* We've filled up half of the undo_buffers. Unlock the Undo buffers we have locked
     * then unpin all the undo buffers in the undo_buffer array.
     */
    if (u_sess->ustore_cxt.undo_buffer_idx >= ((MAX_UNDO_BUFFERS / 2) - 1)) {
        for (int i = 0; i < u_sess->ustore_cxt.undo_buffer_idx; i++) {
            ResourceOwnerForgetBuffer(t_thrd.utils_cxt.TopTransactionResourceOwner,
                u_sess->ustore_cxt.undo_buffers[i].buf);

            ResourceOwnerRememberBuffer(t_thrd.utils_cxt.CurrentResourceOwner, u_sess->ustore_cxt.undo_buffers[i].buf);

            ReleaseBuffer(u_sess->ustore_cxt.undo_buffers[i].buf);

            u_sess->ustore_cxt.undo_buffers[i].inUse = false;
        }

        u_sess->ustore_cxt.undo_buffer_idx = 0;
    } else {
        for (int i = 0; i < u_sess->ustore_cxt.undo_buffer_idx; i++) {
            if (BufferIsValid(u_sess->ustore_cxt.undo_buffers[i].buf)) {
                BufferDesc *bufdesc = GetBufferDescriptor(u_sess->ustore_cxt.undo_buffers[i].buf - 1);
                if (LWLockHeldByMeInMode(BufferDescriptorGetContentLock(bufdesc), LW_EXCLUSIVE)) {
                    elog(PANIC, "buffer %d is not unlocked", u_sess->ustore_cxt.undo_buffers[i].buf);
                }
                u_sess->ustore_cxt.undo_buffers[i].inUse = false;
                u_sess->ustore_cxt.undo_buffers[i].zero = false;
            }
        }
    }
}

UndoRecPtr UHeapPrepareUndoInsert(Oid relOid, Oid partitionOid, Oid relfilenode, Oid tablespace,
    UndoPersistence persistence, Buffer buffer, TransactionId xid, CommandId cid, UndoRecPtr prevurpInOneBlk,
    UndoRecPtr prevurpInOneXact, BlockNumber blk, XlUndoHeader *xlundohdr, undo::XlogUndoMeta *xlundometa)
{
    UndoRecord *urec = u_sess->ustore_cxt.undo_records[0];
    URecVector *urecvec = u_sess->ustore_cxt.urecvec;
    Assert(tablespace != InvalidOid);
    urec->SetUtype(UNDO_INSERT);
    urec->SetXid(xid);
    urec->SetCid(cid);
    urec->SetReloid(relOid);
    urec->SetPartitionoid(partitionOid);
    urec->SetBlkprev(prevurpInOneBlk);
    urec->SetRelfilenode(relfilenode);
    urec->SetTablespace(tablespace);

    if (t_thrd.xlog_cxt.InRecovery) {
        urec->SetBlkno(blk);
    } else {
        if (BufferIsValid(buffer)) {
            urec->SetBlkno(BufferGetBlockNumber(buffer));
        } else {
            urec->SetBlkno(InvalidBlockNumber);
        }
    }

    urec->SetOffset(InvalidOffsetNumber);
    urec->SetPrevurp(t_thrd.xlog_cxt.InRecovery ? prevurpInOneXact : GetCurrentTransactionUndoRecPtr(persistence));
    urec->SetNeedInsert(true);

    /* Tell Undo chain traversal this record does not have any older version */
    urec->SetOldXactId(FrozenTransactionId);

    int status = PrepareUndoRecord(urecvec, persistence, xlundohdr, xlundometa);
    /* Do not continue if there was a failure during Undo preparation */
    if (status != UNDO_RET_SUCC) {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("Failed to generate UndoRecord")));
    }

    UndoRecPtr urecptr = urec->Urp();
    Assert(IS_VALID_UNDO_REC_PTR(urecptr));

    return urecptr;
}

/*
 * UHeapPrepareUndoMultiInsert will allocate space for a URecVector and return it back,
 * caller is responsible for free the space
 */
UndoRecPtr UHeapPrepareUndoMultiInsert(Oid relOid, Oid partitionOid, Oid relfilenode, Oid tablespace,
    UndoPersistence persistence, Buffer buffer, int nranges, TransactionId xid, CommandId cid,
    UndoRecPtr prevurpInOneBlk, UndoRecPtr prevurpInOneXact, URecVector **urecvec_ptr, UndoRecPtr *first_urecptr,
    UndoRecPtr *urpvec, BlockNumber blk, XlUndoHeader *xlundohdr, undo::XlogUndoMeta *xlundometa)
{
    URecVector *urecvec = New(CurrentMemoryContext)URecVector();
    urecvec->Initialize(nranges, true);
    UndoRecord *undoRecord = NULL;
    int i = 0;
    Assert(tablespace != InvalidOid);
    for (i = 0; i < nranges; i++) {
        undoRecord = New(CurrentMemoryContext)UndoRecord();
        undoRecord->SetUtype(UNDO_MULTI_INSERT);
        undoRecord->SetUinfo(UNDO_UREC_INFO_PAYLOAD);
        undoRecord->SetXid(xid);
        undoRecord->SetCid(cid);
        undoRecord->SetReloid(relOid);
        undoRecord->SetPartitionoid(partitionOid);
        undoRecord->SetBlkprev(prevurpInOneBlk);
        undoRecord->SetRelfilenode(relfilenode);
        undoRecord->SetTablespace(tablespace);

        if (t_thrd.xlog_cxt.InRecovery) {
            undoRecord->SetBlkno(blk);
        } else {
            if (BufferIsValid(buffer)) {
                undoRecord->SetBlkno(BufferGetBlockNumber(buffer));
            } else {
                undoRecord->SetBlkno(InvalidBlockNumber);
            }
        }
        undoRecord->SetOffset(InvalidOffsetNumber);
        undoRecord->SetPrevurp(t_thrd.xlog_cxt.InRecovery ? prevurpInOneXact :
                                                            GetCurrentTransactionUndoRecPtr(persistence));
        undoRecord->SetNeedInsert(true);
        undoRecord->SetOldXactId(FrozenTransactionId);
        undoRecord->Rawdata()->len = 2 * sizeof(OffsetNumber);
        undoRecord->SetUrp(INVALID_UNDO_REC_PTR);

        if (t_thrd.xlog_cxt.InRecovery) {
            Assert(urpvec && IS_VALID_UNDO_REC_PTR(urpvec[i]));
            undoRecord->SetUrp(urpvec[i]);
        }
        urecvec->PushBack(undoRecord);
    }

    int status = PrepareUndoRecord(urecvec, persistence, xlundohdr, xlundometa);
    /* Do not continue if there was a failure during Undo preparation */
    if (status != UNDO_RET_SUCC) {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("Failed to generate UndoRecord")));
    }

    /* Tell Undo chain traversal this record does not have any older version */
    UndoRecPtr urecptr = prevurpInOneBlk;
    UndoRecPtr prevurp = t_thrd.xlog_cxt.InRecovery ? prevurpInOneXact : GetCurrentTransactionUndoRecPtr(persistence);
    for (i = 0; i < nranges; i++) {
        (*urecvec)[i]->SetBlkprev(urecptr);
        (*urecvec)[i]->SetPrevurp(prevurp);

        urecptr = (*urecvec)[i]->Urp();
        prevurp = (*urecvec)[i]->Urp();

        Assert(IS_VALID_UNDO_REC_PTR(urecptr));
    }

    if (!t_thrd.xlog_cxt.InRecovery) {
        Assert(first_urecptr && !IS_VALID_UNDO_REC_PTR(*first_urecptr));
        *first_urecptr = (*urecvec)[0]->Urp();
    }
    *urecvec_ptr = urecvec;
    return urecptr;
}


UndoRecPtr UHeapPrepareUndoDelete(Oid relOid, Oid partitionOid, Oid relfilenode, Oid tablespace,
    UndoPersistence persistence, Buffer buffer, OffsetNumber offnum, TransactionId xid, SubTransactionId subxid,
    CommandId cid, UndoRecPtr prevurpInOneBlk, UndoRecPtr prevurpInOneXact, _in_ TD *oldtd, UHeapTuple oldtuple,
    BlockNumber blk, XlUndoHeader *xlundohdr, undo::XlogUndoMeta *xlundometa)
{
    Assert(oldtuple->tupTableType == UHEAP_TUPLE);
    Assert(tablespace != InvalidOid);
    UndoRecord *urec = u_sess->ustore_cxt.undo_records[0];
    URecVector *urecvec = u_sess->ustore_cxt.urecvec;

    urec->SetUtype(UNDO_DELETE);
    urec->SetUinfo(UNDO_UREC_INFO_PAYLOAD);
    urec->SetXid(xid);
    urec->SetCid(cid);
    urec->SetReloid(relOid);
    urec->SetPartitionoid(partitionOid);
    urec->SetBlkprev(prevurpInOneBlk);
    urec->SetRelfilenode(relfilenode);
    urec->SetTablespace(tablespace);

    if (t_thrd.xlog_cxt.InRecovery) {
        urec->SetBlkno(blk);
    } else {
        if (BufferIsValid(buffer)) {
            urec->SetBlkno(BufferGetBlockNumber(buffer));
        } else {
            urec->SetBlkno(InvalidBlockNumber);
        }
    }

    urec->SetOffset(offnum);
    urec->SetPrevurp(t_thrd.xlog_cxt.InRecovery ? prevurpInOneXact : GetCurrentTransactionUndoRecPtr(persistence));
    urec->SetOldXactId(oldtd->xactid);
    urec->SetNeedInsert(true);

    /* Copy over the entire tuple data to the undorecord */
    initStringInfo(urec->Rawdata());
    appendBinaryStringInfo(urec->Rawdata(), ((char *)oldtuple->disk_tuple + OffsetTdId),
                           oldtuple->disk_tuple_size - OffsetTdId);
    if (subxid != InvalidSubTransactionId) {
        urec->SetUinfo(UNDO_UREC_INFO_CONTAINS_SUBXACT);
        appendBinaryStringInfo(urec->Rawdata(), (char *)&subxid, sizeof(SubTransactionId));
    }

    int status = PrepareUndoRecord(urecvec, persistence, xlundohdr, xlundometa);

    /* Do not continue if there was a failure during Undo preparation */
    if (status != UNDO_RET_SUCC) {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("Failed to generate UndoRecord")));
    }

    UndoRecPtr urecptr = urec->Urp();
    Assert(IS_VALID_UNDO_REC_PTR(urecptr));

    return urecptr;
}

UndoRecPtr UHeapPrepareUndoLock(Oid relOid, Oid partitionOid, Oid relfilenode, Oid tablespace,
    UndoPersistence persistence, Buffer buffer, OffsetNumber offnum, TransactionId xid, SubTransactionId subxid,
    CommandId cid, UndoRecPtr prevurpInOneBlk, UndoRecPtr prevurpInOneXact, _in_ TD *oldtd, UHeapTuple oldtuple,
    BlockNumber blk, XlUndoHeader *xlundohdr, undo::XlogUndoMeta *xlundometa)
{
    Assert(oldtuple->tupTableType == UHEAP_TUPLE);

    UndoRecord *urec = u_sess->ustore_cxt.undo_records[0];
    URecVector *urecvec = u_sess->ustore_cxt.urecvec;

    /* Just to keep compiler quite */
    urec->SetUtype(UNDO_XID_LOCK_ONLY);
    urec->SetUinfo(UNDO_UREC_INFO_PAYLOAD);
    urec->SetXid(xid);
    urec->SetCid(cid);
    urec->SetReloid(relOid);
    urec->SetPartitionoid(partitionOid);
    urec->SetBlkprev(prevurpInOneBlk);
    urec->SetRelfilenode(relfilenode);
    urec->SetTablespace(tablespace);

    if (t_thrd.xlog_cxt.InRecovery) {
        urec->SetBlkno(blk);
    } else {
        if (BufferIsValid(buffer)) {
            urec->SetBlkno(BufferGetBlockNumber(buffer));
        } else {
            urec->SetBlkno(InvalidBlockNumber);
        }
    }
    urec->SetOffset(offnum);
    urec->SetPrevurp(t_thrd.xlog_cxt.InRecovery ? prevurpInOneXact : GetCurrentTransactionUndoRecPtr(persistence));
    if (oldtd) {
        urec->SetOldXactId(oldtd->xactid);
    }
    urec->SetNeedInsert(true);

    /* Copy over the entire tuple header to the undorecord */
    initStringInfo(urec->Rawdata());
    appendBinaryStringInfo(urec->Rawdata(), (char *)oldtuple->disk_tuple + OffsetTdId,
        SizeOfUHeapDiskTupleHeaderExceptXid);
    if (subxid != InvalidSubTransactionId) {
        urec->SetUinfo(UNDO_UREC_INFO_CONTAINS_SUBXACT);
        appendBinaryStringInfo(urec->Rawdata(), (char *)&subxid, sizeof(SubTransactionId));
    }

    bool status = PrepareUndoRecord(urecvec, persistence, xlundohdr, xlundometa);

    /* Do not continue if there was a failure during Undo preparation */
    if (status != UNDO_RET_SUCC) {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("Failed to generate UndoRecord")));
    }

    UndoRecPtr urecptr = urec->Urp();
    Assert(IS_VALID_UNDO_REC_PTR(urecptr));

    return urecptr;
}

/*
 * Return the TD slot id assigned to xid on the Page, if any.
 * Return InvalidTDSlotId if there isn't any.
 */
int UHeapPageGetTDSlotId(Buffer buffer, TransactionId xid, UndoRecPtr *urp)
{
    Page page = BufferGetPage(buffer);
    UHeapPageHeaderData *phdr = (UHeapPageHeaderData *)page;
    Assert(phdr->td_count > 0);

    UHeapPageTDData *tdPtr = (UHeapPageTDData *)PageGetTDPointer(page);
    TD *transinfo = tdPtr->td_info;

    for (int slotNo = 0; slotNo < phdr->td_count; slotNo++) {
        TD *thistrans = &transinfo[slotNo];

        if (TransactionIdEquals(thistrans->xactid, xid)) {
            *urp = thistrans->undo_record_ptr;
            return slotNo + 1;
        }
    }

    return InvalidTDSlotId;
}

static void PopulateXLUndoHeader(XlUndoHeader *xlundohdr, const UHeapWALInfo *walinfo, const Relation rel)
{
    if (rel != NULL) {
        xlundohdr->relOid = RelationIsPartition(rel) ? rel->parentId : RelationGetRelid(rel);
    } else {
        xlundohdr->relOid = walinfo->relOid;
    }

    xlundohdr->urecptr = walinfo->urecptr;
    xlundohdr->flag = walinfo->flag;
}

static void PopulateXLUHeapHeader(XlUHeapHeader *xlhdr, const UHeapDiskTuple diskTuple)
{
    xlhdr->td_id = diskTuple->td_id;
    xlhdr->locker_td_id = diskTuple->locker_td_id;
    xlhdr->flag2 = diskTuple->flag2;
    xlhdr->flag = diskTuple->flag;
    xlhdr->t_hoff = diskTuple->t_hoff;
}

static void LogUHeapInsert(UHeapWALInfo *walinfo, Relation rel)
{
    XlUndoHeader xlundohdr;
    XlUHeapInsert xlrec;
    XlUHeapHeader xlhdr;
    XLogRecPtr recptr;
    Buffer buffer = walinfo->buffer;
    Page page = BufferGetPage(buffer);
    uint8 info = XLOG_UHEAP_INSERT;
    int bufflags = 0;

    /*
     * If this is the single and first tuple on page, we can reinit the
     * page instead of restoring the whole thing.  Set flag, and hide
     * buffer references from XLogInsert.
     */
    UHeapTuple tuple = walinfo->utuple;
    Assert(tuple->tupTableType == UHEAP_TUPLE);
    if (ItemPointerGetOffsetNumber(&(tuple->ctid)) == FirstOffsetNumber &&
        UHeapPageGetMaxOffsetNumber(page) == FirstOffsetNumber) {
        info |= XLOG_UHEAP_INIT_PAGE;
        bufflags |= REGBUF_WILL_INIT;
    }
    if (rel->rd_rel->relkind == RELKIND_TOASTVALUE) {
        info |= XLOG_UHEAP_INIT_TOAST_PAGE;
    }
    xlrec.offnum = ItemPointerGetOffsetNumber(&tuple->ctid);
    Assert(ItemPointerGetBlockNumber(&tuple->ctid) == BufferGetBlockNumber(buffer));

    /*
     * For logical decoding, we need the tuple even if we're doing a full
     * page write, so make sure it's included even if we take a full-page
     * image. (XXX We could alternatively store a pointer into the FPW).
     */
    if (RelationIsLogicallyLogged(rel)) {
        xlrec.flags |= XLOG_UHEAP_CONTAINS_NEW_TUPLE;
        bufflags |= REGBUF_KEEP_DATA;
    }

    PopulateXLUndoHeader(&xlundohdr, walinfo, rel);

    XLogBeginInsert();
    XLogRegisterData((char *)&xlrec, SizeOfUHeapInsert);

    XLogRegisterData((char *)&xlundohdr, SizeOfXLUndoHeader);
    if ((walinfo->flag & XLOG_UNDO_HEADER_HAS_BLK_PREV) != 0) {
        ereport(DEBUG5, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("blkprev=%lu", walinfo->blkprev)));
        Assert(walinfo->blkprev != INVALID_UNDO_REC_PTR);
        XLogRegisterData((char *)&(walinfo->blkprev), sizeof(UndoRecPtr));
    }

    if ((walinfo->flag & XLOG_UNDO_HEADER_HAS_PREV_URP) != 0) {
        XLogRegisterData((char *)&(walinfo->prevurp), sizeof(UndoRecPtr));
    }

    if ((walinfo->flag & XLOG_UNDO_HEADER_HAS_PARTITION_OID) != 0) {
        XLogRegisterData((char *)&(walinfo->partitionOid), sizeof(Oid));
    }

    if ((walinfo->flag & XLOG_UNDO_HEADER_HAS_CURRENT_XID) != 0) {
        XLogRegisterData((char *)&(walinfo->xid), sizeof(TransactionId));
    }

    /* Cross: Write the whole XlogUndoMeta struct into xlog record for now,
     * may need to remove some attributes
     */
    /* Cross: need additional flags for undo meta classification */
    undo::LogUndoMeta(walinfo->xlum);

    if (info & XLOG_UHEAP_INIT_PAGE) {
        // the xid used to initialize the page
        UHeapPageHeaderData *uheappage = (UHeapPageHeaderData *)page;
        XLogRegisterData((char *)&uheappage->pd_xid_base, sizeof(TransactionId));
        XLogRegisterData((char *)&uheappage->td_count, sizeof(uint16));
    }

    PopulateXLUHeapHeader(&xlhdr, tuple->disk_tuple);

    /*
     * note we mark xlhdr as belonging to buffer; if XLogInsert decides to
     * write the whole page to the xlog, we don't need to store
     * xl_heap_header in the xlog.
     */
    XLogRegisterBuffer(0, buffer, REGBUF_STANDARD | bufflags);
    XLogRegisterBufData(0, (char *)&xlhdr, SizeOfUHeapHeader);
    /* PG73FORMAT: write bitmap [+ padding] [+ oid] + data */
    XLogRegisterBufData(0, (char *)tuple->disk_tuple + offsetof(UHeapDiskTupleData, data),
        tuple->disk_tuple_size - offsetof(UHeapDiskTupleData, data));

    /* filtering by origin on a row level is much more efficient */
    XLogIncludeOrigin();

    recptr = XLogInsert(RM_UHEAP_ID, info);

    PageSetLSN(page, recptr);
    SetUndoPageLSN(u_sess->ustore_cxt.urecvec, recptr);
    undo::SetUndoMetaLSN(walinfo->xlum, recptr);
}

static void LogUHeapDelete(UHeapWALInfo *walinfo)
{
    XlUHeapDelete xlrec;
    XLogRecPtr recptr;
    XlUHeapHeader xlhdr;
    XlUndoHeader xlundohdr;
    // undo xlog stuff
    xlundohdr.relOid = walinfo->relOid;
    xlundohdr.urecptr = walinfo->urecptr;
    xlundohdr.flag = walinfo->flag;
    UHeapTuple utuple = walinfo->utuple;
    Assert(utuple->tupTableType == UHEAP_TUPLE);
    Buffer buffer = walinfo->buffer;
    Page page = BufferGetPage(buffer);

    xlrec.offnum = ItemPointerGetOffsetNumber(&(utuple->ctid));
    xlrec.flag = utuple->disk_tuple->flag;
    xlrec.td_id = walinfo->td_id;
    xlrec.oldxid = walinfo->oldXid;

    UHeapDiskTuple oldTup = (UHeapDiskTuple)walinfo->oldUTuple.data;
    xlhdr.td_id = oldTup->td_id;
    xlhdr.locker_td_id = oldTup->locker_td_id;
    xlhdr.flag = oldTup->flag;
    xlhdr.flag2 = oldTup->flag2;
    xlhdr.t_hoff = oldTup->t_hoff;

    XLogBeginInsert();
    XLogRegisterData((char *)&xlrec, SizeOfUHeapDelete);
    XLogRegisterBuffer(0, buffer, REGBUF_STANDARD);

    XLogRegisterData((char *)&xlundohdr, SizeOfXLUndoHeader);
   
    if ((walinfo->flag & XLOG_UNDO_HEADER_HAS_SUB_XACT) != 0) {
        XLogRegisterData((char *)&(walinfo->hasSubXact), sizeof(bool));
    }
    if ((walinfo->flag & XLOG_UNDO_HEADER_HAS_BLK_PREV) != 0) {
        Assert(walinfo->blkprev != INVALID_UNDO_REC_PTR);
        XLogRegisterData((char *)&(walinfo->blkprev), sizeof(UndoRecPtr));
    }
    if ((walinfo->flag & XLOG_UNDO_HEADER_HAS_PREV_URP) != 0) {
        XLogRegisterData((char *)&(walinfo->prevurp), sizeof(UndoRecPtr));
    }
    if ((walinfo->flag & XLOG_UNDO_HEADER_HAS_PARTITION_OID) != 0) {
        XLogRegisterData((char *)&(walinfo->partitionOid), sizeof(Oid));
    }
    if ((walinfo->flag & XLOG_UNDO_HEADER_HAS_CURRENT_XID) != 0) {
        XLogRegisterData((char *)&(walinfo->xid), sizeof(TransactionId));
    }

    undo::LogUndoMeta(walinfo->xlum);

    XLogRegisterData((char *)&xlhdr, SizeOfUHeapHeader);
    XLogRegisterData((char *)utuple->disk_tuple + offsetof(UHeapDiskTupleData, data),
        utuple->disk_tuple_size - offsetof(UHeapDiskTupleData, data));

    /* filtering by origin on a row level is much more efficient */
    XLogIncludeOrigin();

    recptr = XLogInsert(RM_UHEAP_ID, XLOG_UHEAP_DELETE);

    PageSetLSN(page, recptr);
    SetUndoPageLSN(u_sess->ustore_cxt.urecvec, recptr);
    undo::SetUndoMetaLSN(walinfo->xlum, recptr);
}

static void LogUHeapUpdate(UHeapWALInfo *oldTupWalinfo, UHeapWALInfo *newTupWalinfo, bool isInplaceUpdate,
    int undoXorDeltaSize, char *xlogXorDelta, uint16 xorPrefixlen, uint16 xorSurfixlen, Relation rel)
{
    char *oldp = NULL;
    char *newp = NULL;
    int oldlen;
    int newlen;
    int bufflags = REGBUF_STANDARD;
    uint32 oldTupLen = 0;
    Page page = NULL;
    uint8 info = XLOG_UHEAP_UPDATE;
    uint16 prefixlen = 0;
    uint16 suffixlen = 0;
    UHeapTuple difftup = NULL;
    UHeapDiskTuple oldTup = NULL;
    UHeapTuple inplaceTup = NULL;
    UHeapTuple nonInplaceNewTup = NULL;
    XlUHeapUpdate xlrec;
    XLogRecPtr recptr = InvalidXLogRecPtr;
    XlUHeapHeader oldXlhdr;
    XlUHeapHeader newXlhdr;
    XlUndoHeader xlundohdr;
    XlUndoHeader xlnewundohdr;

    Assert(oldTupWalinfo->oldUTuple.data);
    oldTup = (UHeapDiskTupleData *)oldTupWalinfo->oldUTuple.data;
    oldTupLen = oldTupWalinfo->oldUTuple.len;
    inplaceTup = oldTupWalinfo->utuple;
    Assert(inplaceTup->tupTableType == UHEAP_TUPLE);
    nonInplaceNewTup = newTupWalinfo->utuple;
    if (isInplaceUpdate) {
        /*
         * For inplace updates the old tuple is in undo record and the new
         * tuple is replaced in page where old tuple was present.
         */
        oldp = (char *)oldTup + oldTup->t_hoff;
        oldlen = oldTupLen - oldTup->t_hoff;
        newp = (char *)inplaceTup->disk_tuple + inplaceTup->disk_tuple->t_hoff;
        newlen = inplaceTup->disk_tuple_size - inplaceTup->disk_tuple->t_hoff;

        difftup = inplaceTup;
    } else if (oldTupWalinfo->buffer == newTupWalinfo->buffer) {
        oldp = (char *)inplaceTup->disk_tuple + inplaceTup->disk_tuple->t_hoff;
        oldlen = inplaceTup->disk_tuple_size - inplaceTup->disk_tuple->t_hoff;
        newp = (char *)nonInplaceNewTup->disk_tuple + nonInplaceNewTup->disk_tuple->t_hoff;
        newlen = nonInplaceNewTup->disk_tuple_size - nonInplaceNewTup->disk_tuple->t_hoff;

        difftup = nonInplaceNewTup;
    } else {
        difftup = nonInplaceNewTup;
    }

    /*
     * If the old and new tuple are on the same page, we only need to log the
     * parts of the new tuple that were changed.  That saves on the amount of
     * WAL we need to write.  Currently, we just count any unchanged bytes in
     * the beginning and end of the tuple.  That's quick to check, and
     * perfectly covers the common case that only one field is updated.
     *
     * We could do this even if the old and new tuple are on different pages,
     * but only if we don't make a full-page image of the old page, which is
     * difficult to know in advance.  Also, if the old tuple is corrupt for
     * some reason, it would allow the corruption to propagate the new page,
     * so it seems best to avoid.  Under the general assumption that most
     * updates tend to create the new tuple version on the same page, there
     * isn't much to be gained by doing this across pages anyway.
     *
     * Skip this if we're taking a full-page image of the new page, as we
     * don't include the new tuple in the WAL record in that case.  Also
     * disable if wal_level='logical', as logical decoding needs to be able to
     * read the new tuple in whole from the WAL record alone.
     */
    if (oldTupWalinfo->buffer == newTupWalinfo->buffer && !XLogCheckBufferNeedsBackup(newTupWalinfo->buffer) &&
        !RelationIsLogicallyLogged(rel)) {
        if (isInplaceUpdate) {
            prefixlen = xorPrefixlen;
            suffixlen = xorSurfixlen;
        } else {
            int minlen = Min(oldlen, newlen);

            Assert(oldp != NULL && newp != NULL);

            /* Check for common prefix between undo and old tuple */
            for (prefixlen = 0; prefixlen < minlen; prefixlen++) {
                if (oldp[prefixlen] != newp[prefixlen]) {
                    break;
                }
            }

            /*
             * Storing the length of the prefix takes 2 bytes, so we need to save
             * at least 3 bytes or there's no point.
             */
            if (prefixlen < MIN_SAVING_LEN) {
                prefixlen = 0;
            }

            /* Same for suffix */
            for (suffixlen = 0; suffixlen < minlen - prefixlen; suffixlen++) {
                if (oldp[oldlen - suffixlen - 1] != newp[newlen - suffixlen - 1])
                    break;
            }

            if (suffixlen < MIN_SAVING_LEN) {
                suffixlen = 0;
            }
        }
    }

    /*
     * Store the information required to generate undo record during replay.
     */
    xlundohdr.relOid = oldTupWalinfo->relOid;
    xlundohdr.urecptr = oldTupWalinfo->urecptr;
    xlundohdr.flag = oldTupWalinfo->flag;

    xlrec.old_offnum = ItemPointerGetOffsetNumber(&inplaceTup->ctid);
    xlrec.new_offnum = ItemPointerGetOffsetNumber(&difftup->ctid);
    xlrec.old_tuple_td_id = inplaceTup->disk_tuple->td_id;
    xlrec.old_tuple_flag = inplaceTup->disk_tuple->flag;
    xlrec.flags = 0;
    xlrec.oldxid = oldTupWalinfo->oldXid;

    if (prefixlen > 0) {
        xlrec.flags |= XLZ_UPDATE_PREFIX_FROM_OLD;
    }

    if (suffixlen > 0) {
        xlrec.flags |= XLZ_UPDATE_SUFFIX_FROM_OLD;
    }

    if (RelationIsLogicallyLogged(rel)) {
        xlrec.flags |= XLOG_UHEAP_CONTAINS_OLD_HEADER;
    }

    if (!isInplaceUpdate) {
        page = BufferGetPage(newTupWalinfo->buffer);

        xlrec.flags |= XLZ_NON_INPLACE_UPDATE;

        xlnewundohdr.relOid = newTupWalinfo->relOid;
        xlnewundohdr.urecptr = newTupWalinfo->urecptr;
        xlnewundohdr.flag = newTupWalinfo->flag;

        Assert(newTupWalinfo->utuple);

        /* If new tuple is the single and first tuple on page... */
        if (ItemPointerGetOffsetNumber(&(newTupWalinfo->utuple->ctid)) == FirstOffsetNumber &&
            UHeapPageGetMaxOffsetNumber(page) == FirstOffsetNumber) {
            info |= XLOG_UHEAP_INIT_PAGE;
            bufflags |= REGBUF_WILL_INIT;
        }
        if (rel->rd_rel->relkind == RELKIND_TOASTVALUE) {
            info |= XLOG_UHEAP_INIT_TOAST_PAGE;
        }
    }

    xlrec.flags |= XLZ_HAS_UPDATE_UNDOTUPLE;
    oldXlhdr.td_id = oldTup->td_id;
    oldXlhdr.locker_td_id = oldTup->locker_td_id;
    oldXlhdr.flag = oldTup->flag;
    oldXlhdr.flag2 = oldTup->flag2;
    oldXlhdr.t_hoff = oldTup->t_hoff;

    XLogBeginInsert();
    XLogRegisterData((char *)&xlrec, SizeOfUHeapUpdate);
    XLogRegisterData((char *)&xlundohdr, SizeOfXLUndoHeader);
    if ((oldTupWalinfo->flag & XLOG_UNDO_HEADER_HAS_SUB_XACT) != 0) {
        XLogRegisterData((char *)&(oldTupWalinfo->hasSubXact), sizeof(bool));
    }
    if ((oldTupWalinfo->flag & XLOG_UNDO_HEADER_HAS_BLK_PREV) != 0) {
        Assert(oldTupWalinfo->blkprev != INVALID_UNDO_REC_PTR);
        XLogRegisterData((char *)&(oldTupWalinfo->blkprev), sizeof(UndoRecPtr));
    }
    if ((oldTupWalinfo->flag & XLOG_UNDO_HEADER_HAS_PREV_URP) != 0) {
        XLogRegisterData((char *)&(oldTupWalinfo->prevurp), sizeof(UndoRecPtr));
    }
    if ((oldTupWalinfo->flag & XLOG_UNDO_HEADER_HAS_PARTITION_OID) != 0) {
        XLogRegisterData((char *)&(oldTupWalinfo->partitionOid), sizeof(Oid));
    }
    if ((oldTupWalinfo->flag & XLOG_UNDO_HEADER_HAS_CURRENT_XID) != 0) {
        XLogRegisterData((char *)&(oldTupWalinfo->xid), sizeof(TransactionId));
    }

    if (!isInplaceUpdate) {
        XLogRegisterData((char *)&xlnewundohdr, SizeOfXLUndoHeader);
        if ((newTupWalinfo->flag & XLOG_UNDO_HEADER_HAS_BLK_PREV) != 0) {
            Assert(newTupWalinfo->blkprev != INVALID_UNDO_REC_PTR);
            XLogRegisterData((char *)&(newTupWalinfo->blkprev), sizeof(UndoRecPtr));
        }
        if ((newTupWalinfo->flag & XLOG_UNDO_HEADER_HAS_PREV_URP) != 0) {
            XLogRegisterData((char *)&(newTupWalinfo->prevurp), sizeof(UndoRecPtr));
        }
        if ((newTupWalinfo->flag & XLOG_UNDO_HEADER_HAS_PARTITION_OID) != 0) {
            XLogRegisterData((char *)&(newTupWalinfo->partitionOid), sizeof(Oid));
        }
    }

    undo::LogUndoMeta(oldTupWalinfo->xlum);

    XLogRegisterBuffer(0, newTupWalinfo->buffer, bufflags);

    if (oldTupWalinfo->buffer != newTupWalinfo->buffer) {
        Assert(!isInplaceUpdate);
        XLogRegisterBuffer(1, oldTupWalinfo->buffer, REGBUF_STANDARD);
    }

    if (info & XLOG_UHEAP_INIT_PAGE) {
        UHeapPageHeaderData *uheappage = (UHeapPageHeaderData *)page;
        XLogRegisterData((char *)&uheappage->pd_xid_base, sizeof(TransactionId));
        XLogRegisterData((char *)&uheappage->td_count, sizeof(uint16));
    }

    if (xlrec.flags & XLZ_HAS_UPDATE_UNDOTUPLE) {
        if (!isInplaceUpdate) {
            XLogRegisterData((char *)&oldXlhdr, SizeOfUHeapHeader);
            /* PG73FORMAT: write bitmap [+ padding] [+ oid] + data */
            XLogRegisterData((char *)oldTup + SizeOfUHeapDiskTupleData, oldTupLen - SizeOfUHeapDiskTupleData);
        } else {
            XLogRegisterData((char *)&undoXorDeltaSize, sizeof(int));
            XLogRegisterData(xlogXorDelta, undoXorDeltaSize);
            if ((xlrec.flags & XLOG_UHEAP_CONTAINS_OLD_HEADER) != 0) {
                XLogRegisterData((char *)&oldXlhdr, SizeOfUHeapHeader);
                XLogRegisterData((char *)oldTup + SizeOfUHeapDiskTupleData, oldTupLen - SizeOfUHeapDiskTupleData);
            }
        }
    }

    /*
     * Prepare WAL data for the new tuple.
     */
    if (!isInplaceUpdate) {
        if (prefixlen > 0) {
            XLogRegisterBufData(0, (char *)&prefixlen, sizeof(uint16));
        }
        if (suffixlen > 0) {
            XLogRegisterBufData(0, (char *)&suffixlen, sizeof(uint16));
        }
    }

    newXlhdr.td_id = difftup->disk_tuple->td_id;
    newXlhdr.locker_td_id = difftup->disk_tuple->locker_td_id;
    newXlhdr.flag2 = difftup->disk_tuple->flag2;
    newXlhdr.flag = difftup->disk_tuple->flag;
    newXlhdr.t_hoff = difftup->disk_tuple->t_hoff;
    Assert(SizeOfUHeapDiskTupleData + prefixlen + suffixlen <= difftup->disk_tuple_size);

    /*
     * PG73FORMAT: write bitmap [+ padding] [+ oid] + data
     *
     * The 'data' doesn't include the common prefix or suffix.
     */
    XLogRegisterBufData(0, (char *)&newXlhdr, SizeOfUHeapHeader);
    if (prefixlen == 0) {
        XLogRegisterBufData(0, ((char *)difftup->disk_tuple) + SizeOfUHeapDiskTupleData,
            difftup->disk_tuple_size - SizeOfUHeapDiskTupleData - suffixlen);
    } else {
        /*
         * Have to write the null bitmap and data after the common prefix as
         * two separate rdata entries.
         */
        /* bitmap [+ padding] [+ oid] */
        if (difftup->disk_tuple->t_hoff - SizeOfUHeapDiskTupleData > 0) {
            XLogRegisterBufData(0, ((char *)difftup->disk_tuple) + SizeOfUHeapDiskTupleData,
                difftup->disk_tuple->t_hoff - SizeOfUHeapDiskTupleData);
        }

        /* data after common prefix */
        XLogRegisterBufData(0, ((char *)difftup->disk_tuple) + difftup->disk_tuple->t_hoff + prefixlen,
            difftup->disk_tuple_size - difftup->disk_tuple->t_hoff - prefixlen - suffixlen);
    }

    /* filtering by origin on a row level is much more efficient */
    XLogIncludeOrigin();

    recptr = XLogInsert(RM_UHEAP_ID, info);

    if (newTupWalinfo->buffer != oldTupWalinfo->buffer) {
        PageSetLSN(BufferGetPage(newTupWalinfo->buffer), recptr);
    }

    PageSetLSN(BufferGetPage(oldTupWalinfo->buffer), recptr);
    SetUndoPageLSN(u_sess->ustore_cxt.urecvec, recptr);
    undo::SetUndoMetaLSN(oldTupWalinfo->xlum, recptr);
}

/*
 * log_uheap_clean - Perform XLogInsert for a uheap-clean operation.
 *
 * Caller must already have modified the buffer and marked it dirty.
 *
 * We also include latestRemovedXid, which is the greatest XID present in
 * the removed tuples. That allows recovery processing to cancel or wait
 * for long standby queries that can still see these tuples.
 */
XLogRecPtr LogUHeapClean(Relation reln, Buffer buffer, OffsetNumber target_offnum, Size space_required,
    OffsetNumber *nowdeleted, int ndeleted, OffsetNumber *nowdead, int ndead, OffsetNumber *nowunused, int nunused,
    TransactionId latestRemovedXid, bool pruned)
{
    XLogRecPtr recptr;
    XlUHeapClean xlRec;

    /* Caller should not call me on a non-WAL-logged relation */
    Assert(RelationNeedsWAL(reln));

    xlRec.latestRemovedXid = latestRemovedXid;
    xlRec.ndeleted = ndeleted;
    xlRec.ndead = ndead;
    xlRec.flags = 0;
    XLogBeginInsert();

    if (pruned) {
        xlRec.flags |= XLZ_CLEAN_ALLOW_PRUNING;
    }

    XLogRegisterData((char *)&xlRec, SizeOfUHeapClean);

    /* Register the offset information. */
    if (target_offnum != InvalidOffsetNumber) {
        xlRec.flags |= XLZ_CLEAN_CONTAINS_OFFSET;
        XLogRegisterData((char *)&target_offnum, sizeof(OffsetNumber));
        XLogRegisterData((char *)&space_required, sizeof(space_required));
    }

    XLogRegisterBuffer(0, buffer, REGBUF_STANDARD);

    /*
     * The OffsetNumber arrays are not actually in the buffer, but we pretend
     * that they are.  When XLogInsert stores the whole buffer, the offset
     * arrays need not be stored too.  Note that even if all three arrays are
     * empty, we want to expose the buffer as a candidate for whole-page
     * storage, since this record type implies a defragmentation operation
     * even if no item pointers changed state.
     */
    if (ndeleted > 0) {
        XLogRegisterBufData(0, (char *)nowdeleted, ndeleted * sizeof(OffsetNumber) * 2);
    }
    if (ndead > 0) {
        XLogRegisterBufData(0, (char *)nowdead, ndead * sizeof(OffsetNumber));
    }
    if (nunused > 0) {
        XLogRegisterBufData(0, (char *)nowunused, nunused * sizeof(OffsetNumber));
    }

    recptr = XLogInsert(RM_UHEAP_ID, XLOG_UHEAP_CLEAN);

    return recptr;
}

/*
 * UHeapExecPendingUndoActions - apply any pending rollback on the input buffer
 *
 * xid - Transaction id for which pending actions need to be applied.
 * If the TD slots in the given buffer does not contain this xid,
 * then we consider the undo actions are already applied and the slot
 * has been reused by a different transaction.
 *
 * It expects the caller has an exclusive lock on the relation.
 */
bool UHeapExecPendingUndoActions(Relation relation, Buffer buffer, TransactionId xid)
{
    /* Check if the TD slots still has this xid */
    UndoRecPtr slotUrecPtr = INVALID_UNDO_REC_PTR;
    int tdSlotId = UHeapPageGetTDSlotId(buffer, xid, &slotUrecPtr);
    if (tdSlotId == InvalidTDSlotId) {
        return false;
    }

    /* It's either we're the one applying our own undo actions
     * or we're trying to apply undo action of a completed transaction.
     */
    Assert(TransactionIdIsCurrentTransactionId(xid) || !TransactionIdIsInProgress(xid, NULL, false, false));
    /*
     * Apply Undo Actions if the transaction is aborted. To check abort,
     * we can call TransactionIdDidAbort but this will not always give
     * the proper status. For instance, if this xact was running at the time of
     * crash, and after restart, status of this transaction will not be
     * aborted but we should still consider it as aborted because it dit not commit.
     */
    if (TransactionIdIsValid(xid) && !TransactionIdDidCommit(xid) &&
        !TransactionIdIsInProgress(xid, NULL, false, false)) {
        /*
         * Release the buffer lock here to prevent deadlock.
         * This is because the actual rollback will reacquire the lock.
         */
        LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
        ExecuteUndoActionsPage(slotUrecPtr, relation, buffer, xid);
        LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

        /* We better not find this xid in any td slot anymore */
        Assert(InvalidTDSlotId == UHeapPageGetTDSlotId(buffer, xid, &slotUrecPtr));
    }

    return true;
}

UndoRecPtr UHeapPrepareUndoUpdate(Oid relOid, Oid partitionOid, Oid relfilenode, Oid tablespace,
    UndoPersistence persistence, Buffer buffer, Buffer newbuffer, OffsetNumber offnum, TransactionId xid,
    SubTransactionId subxid, CommandId cid, UndoRecPtr prevurpInOneBlk, UndoRecPtr newprevurpInOneBlk,
    UndoRecPtr prevurpInOneXact, _in_ TD *oldtd, UHeapTuple oldtuple, bool isInplaceUpdate,
    UndoRecPtr *new_urec, int undoXorDeltaSize, BlockNumber oldblk,
    BlockNumber newblk, XlUndoHeader *xlundohdr, undo::XlogUndoMeta *xlundometa)
{
    UndoRecPtr urecptr = INVALID_UNDO_REC_PTR;
    UndoRecord *urec = u_sess->ustore_cxt.undo_records[0];
    UndoRecord *urecNew = u_sess->ustore_cxt.undo_records[1];
    URecVector *urecvec = u_sess->ustore_cxt.urecvec;

    Assert(tablespace != InvalidOid);
    Assert(oldtuple->tupTableType == UHEAP_TUPLE);

    if (t_thrd.xlog_cxt.InRecovery)
        Assert(oldblk != InvalidBlockNumber && newblk != InvalidBlockNumber);

    urec->SetXid(xid);
    urec->SetCid(cid);
    urec->SetReloid(relOid);
    urec->SetPartitionoid(partitionOid);
    urec->SetRelfilenode(relfilenode);
    urec->SetTablespace(tablespace);
    urec->SetBlkprev(prevurpInOneBlk);
    urec->SetBlkno(t_thrd.xlog_cxt.InRecovery ? oldblk : BufferGetBlockNumber(buffer));
    urec->SetOffset(offnum);
    urec->SetPrevurp(t_thrd.xlog_cxt.InRecovery ? prevurpInOneXact : GetCurrentTransactionUndoRecPtr(persistence));
    urec->SetNeedInsert(true);
    urec->SetOldXactId(oldtd->xactid);

    /* Tell the Undo subsystem how much rawdata we need */
    Size payloadLen = (Size)oldtuple->disk_tuple_size - OffsetTdId;

    if (isInplaceUpdate) {
        urec->SetUtype(UNDO_INPLACE_UPDATE);
        urec->SetUinfo(UNDO_UREC_INFO_PAYLOAD);
    } else {
        urec->SetUtype(UNDO_UPDATE);
        urec->SetUinfo(UNDO_UREC_INFO_PAYLOAD);

        /* Prepare undo record for the new tuple */
        urecNew->SetXid(xid);
        urecNew->SetCid(cid);
        urecNew->SetReloid(relOid);
        urecNew->SetPartitionoid(partitionOid);
        urecNew->SetRelfilenode(relfilenode);
        urecNew->SetTablespace(tablespace);
        urecNew->SetBlkprev(newprevurpInOneBlk);
        urecNew->SetBlkno(t_thrd.xlog_cxt.InRecovery ? newblk : BufferGetBlockNumber(newbuffer));
        urecNew->SetOffset(offnum);
        urecNew->SetNeedInsert(true);
        urecNew->SetUtype(UNDO_INSERT);
        urecNew->SetOldXactId(xid);

        /* Non-inplace updates contains the ctid after the tuple data */
        payloadLen += sizeof(ItemPointerData);
    }

    if (isInplaceUpdate)
        urec->Rawdata()->len = undoXorDeltaSize;
    else
        urec->Rawdata()->len = payloadLen;

    if (subxid != InvalidSubTransactionId) {
        urec->Rawdata()->len += sizeof(SubTransactionId);
    }

    /* Now allocate the Undo record with the correct size */
    int status = PrepareUndoRecord(urecvec, persistence, xlundohdr, xlundometa);

    /* Do not continue if there was a failure during Undo preparation */
    if (status != UNDO_RET_SUCC) {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("Failed to generate UndoRecord")));
    }

    urecptr = urec->Urp();
    Assert(IS_VALID_UNDO_REC_PTR(urecptr));

    /* Once the memory for the UndoRecord is allocated, copy the tuple */
    initStringInfo(urec->Rawdata());
    if (!isInplaceUpdate) {
        appendBinaryStringInfo(urec->Rawdata(), (char *)oldtuple->disk_tuple + OffsetTdId,
                               oldtuple->disk_tuple_size - OffsetTdId);
        Assert(urec->Rawdata()->len == (int)(oldtuple->disk_tuple_size - OffsetTdId));
    }

    /* Set the undo record for the new tuple in case of non-inplace update */
    if (!isInplaceUpdate) {
        if (new_urec != NULL) {
            *new_urec = urecNew->Urp();
            Assert(IS_VALID_UNDO_REC_PTR(*new_urec));
        }

        /* In case of non-inplace update on the same page, previous
         * undo record pointer should be the one for updated(old) record. */
        if (t_thrd.xlog_cxt.InRecovery) {
            if (oldblk == newblk)
                urecNew->SetBlkprev(urecptr);
        } else {
            if (buffer == newbuffer) {
                urecNew->SetBlkprev(urecptr);
            }
        }

        urecNew->SetPrevurp(urecptr);
    }


    return urecptr;
}

static void LogUHeapMultiInsert(UHeapMultiInsertWALInfo *multiWalinfo, bool skipUndo, char *scratch,
    UndoRecPtr *urpvec)
{
    XlUndoHeader xlundohdr = { 0 };
    XLogRecPtr recptr;
    XlUHeapMultiInsert *xlrec;
    uint8 info = XLOG_UHEAP_MULTI_INSERT;
    char *tupledata;
    char *scratchptr = scratch;
    int nranges = multiWalinfo->ufree_offsets->nranges;
    int bufflags = 0, i, totaldatalen;
    errno_t rc;
    bool init;
    Page page = BufferGetPage(multiWalinfo->genWalInfo->buffer);

    /*
     * Store the information required to generate undo record during replay.
     * All undo records have same information apart from the payload data.
     * Hence, we can copy the same from the last record.
     */
    xlundohdr.relOid = RelationIsPartition(multiWalinfo->relation) ? multiWalinfo->relation->parentId :
                                                                      multiWalinfo->relation->rd_id;
    xlundohdr.urecptr = multiWalinfo->genWalInfo->urecptr;
    xlundohdr.flag = multiWalinfo->genWalInfo->flag;

    /* allocate XlUHeapMultiInsert struct from the scratch area */
    xlrec = (XlUHeapMultiInsert *)scratchptr;
    if (skipUndo)
        xlrec->flags |= XLZ_INSERT_IS_FROZEN;
    xlrec->ntuples = multiWalinfo->curpage_ntuples;
    scratchptr += SizeOfUHeapMultiInsert;

    /* copy the offset ranges as well */
    rc = memcpy_s((char *)scratchptr, sizeof(int), (char *)&nranges, sizeof(int));
    securec_check(rc, "\0", "\0");
    scratchptr += sizeof(int);

    rc = memcpy_s((char *)scratchptr, sizeof(UndoRecPtr) * nranges, (char *)urpvec, sizeof(UndoRecPtr) * nranges);
    securec_check(rc, "\0", "\0");
    scratchptr += sizeof(UndoRecPtr) * nranges;

    rc = memcpy_s((char *)scratchptr, (sizeof(OffsetNumber) * nranges),
        (char *)&multiWalinfo->ufree_offsets->startOffset[0], (sizeof(OffsetNumber) * nranges));
    securec_check(rc, "\0", "\0");
    scratchptr += (sizeof(OffsetNumber) * nranges);

    rc = memcpy_s((char *)scratchptr, (sizeof(OffsetNumber) * nranges),
        (char *)&multiWalinfo->ufree_offsets->endOffset[0], (sizeof(OffsetNumber) * nranges));
    securec_check(rc, "\0", "\0");
    scratchptr += (sizeof(OffsetNumber) * nranges);

    /* the rest of the scratch space is used for tuple data */
    tupledata = scratchptr;

    if (RelationIsLogicallyLogged(multiWalinfo->relation)) {
        xlrec->flags |= XLOG_UHEAP_CONTAINS_NEW_TUPLE;
        bufflags |= REGBUF_KEEP_DATA;
    }

    /*
     * Write out an xl_multi_insert_tuple and the tuple data itself for each
     * tuple.
     */
    for (i = 0; i < multiWalinfo->curpage_ntuples; i++) {
        UHeapTuple uheaptup = multiWalinfo->utuples[multiWalinfo->ndone + i];
        XlMultiInsertUTuple *tuphdr;
        int datalen;

        /* xl_multi_insert_tuple needs two-byte alignment. */
        tuphdr = (XlMultiInsertUTuple *)(scratchptr);
        scratchptr = ((char *)tuphdr) + SizeOfMultiInsertUTuple;
        tuphdr->xid = uheaptup->disk_tuple->xid;
        tuphdr->td_id = uheaptup->disk_tuple->td_id;
        tuphdr->locker_td_id = uheaptup->disk_tuple->locker_td_id;
        tuphdr->flag = uheaptup->disk_tuple->flag;
        tuphdr->flag2 = uheaptup->disk_tuple->flag2;
        tuphdr->t_hoff = uheaptup->disk_tuple->t_hoff;

        /* write bitmap [+ padding] [+ oid] + data */
        datalen = uheaptup->disk_tuple_size - SizeOfUHeapDiskTupleData;
        rc = memcpy_s(scratchptr, datalen, (char *)uheaptup->disk_tuple + SizeOfUHeapDiskTupleData, datalen);
        securec_check(rc, "\0", "\0");
        tuphdr->datalen = datalen;
        scratchptr += datalen;
    }
    totaldatalen = scratchptr - tupledata;
    Assert((scratchptr - scratch) < BLCKSZ);

    /*
     * If the page was previously empty, we can reinitialize the page instead
     * of restoring the whole thing. XXX - why don't check slot info?
     */
    init = (ItemPointerGetOffsetNumber(&(multiWalinfo->utuples[multiWalinfo->ndone]->ctid)) == FirstOffsetNumber &&
        UHeapPageGetMaxOffsetNumber(page) == FirstOffsetNumber + multiWalinfo->curpage_ntuples - 1);
    if (init) {
        info |= XLOG_UHEAP_INIT_PAGE;
        bufflags |= REGBUF_WILL_INIT;
    }
    if (multiWalinfo->relation->rd_rel->relkind == RELKIND_TOASTVALUE) {
        info |= XLOG_UHEAP_INIT_TOAST_PAGE;
    }

    XLogBeginInsert();

    /* copy undo related info in maindata */
    XLogRegisterData((char *)&xlundohdr, SizeOfXLUndoHeader);
    if ((multiWalinfo->genWalInfo->flag & XLOG_UNDO_HEADER_HAS_BLK_PREV) != 0) {
        Assert(multiWalinfo->genWalInfo->blkprev != INVALID_UNDO_REC_PTR);
        XLogRegisterData((char *)&(multiWalinfo->genWalInfo->blkprev), sizeof(UndoRecPtr));
    }
    if ((multiWalinfo->genWalInfo->flag & XLOG_UNDO_HEADER_HAS_PREV_URP) != 0) {
        XLogRegisterData((char *)&(multiWalinfo->genWalInfo->prevurp), sizeof(UndoRecPtr));
    }
    if ((multiWalinfo->genWalInfo->flag & XLOG_UNDO_HEADER_HAS_PARTITION_OID) != 0) {
        XLogRegisterData((char *)&(multiWalinfo->genWalInfo->partitionOid), sizeof(Oid));
    }
    if ((multiWalinfo->genWalInfo->flag & XLOG_UNDO_HEADER_HAS_CURRENT_XID) != 0) {
        XLogRegisterData((char *)&(multiWalinfo->genWalInfo->xid), sizeof(TransactionId));
    }

    XLogRegisterData((char *)&(multiWalinfo->lastURecptr), sizeof(multiWalinfo->lastURecptr));
    undo::LogUndoMeta(multiWalinfo->genWalInfo->xlum);

    if (info & XLOG_UHEAP_INIT_PAGE) {
        UHeapPageHeaderData *uheappage = (UHeapPageHeaderData *)page;
        XLogRegisterData((char *)&uheappage->pd_xid_base, sizeof(TransactionId));
        XLogRegisterData((char *)&uheappage->td_count, sizeof(uint16));
    }

    /* copy xl_multi_insert_tuple in maindata */
    XLogRegisterData((char *)xlrec, tupledata - scratch);

    XLogRegisterBuffer(0, multiWalinfo->genWalInfo->buffer, REGBUF_STANDARD | bufflags);

    /* copy tuples in block data */
    XLogRegisterBufData(0, tupledata, totaldatalen);

    /* filtering by origin on a row level is much more efficient */
    XLogIncludeOrigin();

    recptr = XLogInsert(RM_UHEAP_ID, info);

    PageSetLSN(page, recptr);
    if (!skipUndo) {
        SetUndoPageLSN(u_sess->ustore_cxt.urecvec, recptr);
        undo::SetUndoMetaLSN(multiWalinfo->genWalInfo->xlum, recptr);
    }
}

/*
 * UHeapAbortSpeculative
 *
 * This function is used to abort an insert right away.
 *
 * Need to fetch the corresponding undo record to undo the TD.
 */
void UHeapAbortSpeculative(Relation relation, UHeapTuple utuple)
{
    ItemPointer tid = &utuple->ctid;
    BlockNumber blkno = ItemPointerGetBlockNumber(tid);
    OffsetNumber offnum = ItemPointerGetOffsetNumber(tid);
    Buffer buffer = InvalidBuffer;
    TransactionId fxid = GetTopTransactionIdIfAny();
    RowPtr *rp = NULL;
    UHeapDiskTuple diskTuple = NULL;
    int tdSlot = InvalidTDSlotId;
    UHeapTupleTransInfo tdinfo;
    UndoRecord *urec = NULL;
    int rc PG_USED_FOR_ASSERTS_ONLY;
    Page page = NULL;
    int zoneId;

    buffer = ReadBuffer(relation, blkno);
    LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

    page = BufferGetPage(buffer);
    rp = UPageGetRowPtr(page, offnum);
    diskTuple = (UHeapDiskTuple)UPageGetRowData(page, rp);

    tdSlot = UHeapTupleHeaderGetTDSlot(diskTuple);
    Assert(tdSlot != UHEAPTUP_SLOT_FROZEN && !UHeapTupleHasInvalidXact(diskTuple->flag));

    /* Get TD info */
    GetTDSlotInfo(buffer, tdSlot, &tdinfo);
    Assert(tdinfo.xid == fxid);

    /* Fetch Undo record */
    urec = New(CurrentMemoryContext)UndoRecord();
    urec->SetUrp(tdinfo.urec_add);
    rc = FetchUndoRecord(urec, InplaceSatisfyUndoRecord, blkno, offnum, tdinfo.xid);

    /* the tuple cannot be all-visible because it's inserted by current transaction */
    Assert(rc != UNDO_RET_FAIL);
    Assert(urec->Utype() == UNDO_INSERT && urec->Offset() == offnum && urec->Xid() == tdinfo.xid);

    START_CRIT_SECTION();

    /* Apply undo action for an INSERT */
    ExecuteUndoForInsert(relation, buffer, urec->Offset(), urec->Xid());

    int nline = UHeapPageGetMaxOffsetNumber(page);
    bool needPageInit = true;
    UndoRecPtr prevUrp = urec->Blkprev();
    TransactionId xid = fxid;

    for (int offset = FirstOffsetNumber; offset <= nline; offset++) {
        RowPtr *localRp = UPageGetRowPtr(page, offset);
        if (RowPtrIsUsed(localRp) || RowPtrHasPendingXact(localRp)) {
            needPageInit = false;
            break;
        }
    }

    zoneId = (int)UNDO_PTR_GET_ZONE_ID(urec->Urp());

    DELETE_EX(urec);

    /* Set undo ptr and xid */
    if (!IS_VALID_UNDO_REC_PTR(prevUrp)) {
        xid = InvalidTransactionId;
    } else {
        UndoRecord *urecOld = New(CurrentMemoryContext)UndoRecord();
        urecOld->SetUrp(prevUrp);
        rc = FetchUndoRecord(urecOld, NULL, InvalidBlockNumber, InvalidOffsetNumber,
            InvalidTransactionId);
        if (rc == UNDO_RET_FAIL || urecOld->Xid() != fxid) {
            xid = InvalidTransactionId;
        }
        DELETE_EX(urecOld);
    }

    UHeapPageSetUndo(buffer, tdSlot, xid, prevUrp);

    MarkBufferDirty(buffer);

    TransactionId xidbase = u_sess->utils_cxt.RecentXmin - FirstNormalTransactionId;

    if (RelationNeedsWAL(relation)) {
        uint8 flags = 0;
        XlUHeapUndoAbortSpecInsert walinfo;

        DECLARE_NODE_COUNT();
        walinfo.offset = ItemPointerGetOffsetNumber(&utuple->ctid);
        walinfo.zone_id = zoneId;

        if (needPageInit) {
            flags |= XLU_ABORT_SPECINSERT_INIT_PAGE;
        }

        if (xid != InvalidTransactionId) {
            flags |= XLU_ABORT_SPECINSERT_XID_VALID;
        }

        if (!IS_VALID_UNDO_REC_PTR(prevUrp)) {
            flags |= XLU_ABORT_SPECINSERT_PREVURP_VALID;
        }

        /*
         * UHeapAbortSpeculative should be called due to index specConflict,
         * it may not be necessary to store relhasindex
         * but it could be more flexible if we store it.
         */
        if (RelationGetForm(relation)->relhasindex) {
            flags |= XLU_ABORT_SPECINSERT_REL_HAS_INDEX;
        }

        XLogBeginInsert();

        XLogRegisterData((char *)&flags, sizeof(uint8));
        XLogRegisterData((char *)&walinfo, SizeOfUHeapUndoAbortSpecInsert);

        if (flags & XLU_ABORT_SPECINSERT_INIT_PAGE) {
            XLogRegisterData((char *)&xidbase, sizeof(TransactionId));
            uint16 tdCount = UPageGetTDSlotCount(page);
            XLogRegisterData((char *)&tdCount, sizeof(uint16));
        }

        if (flags & XLU_ABORT_SPECINSERT_XID_VALID) {
            XLogRegisterData((char *)&xid, sizeof(TransactionId));
        }

        if (flags & XLU_ABORT_SPECINSERT_PREVURP_VALID) {
            XLogRegisterData((char *)&prevUrp, sizeof(UndoRecPtr));
        }

        XLogRegisterBuffer(0, buffer, REGBUF_STANDARD);
        XLogRecPtr recptr = XLogInsert(RM_UHEAPUNDO_ID, XLOG_UHEAPUNDO_ABORT_SPECINSERT);
        PageSetLSN(page, recptr);

        // relation shouldn't be a toast table
        Assert(UPageGetTDSlotCount(page) >= RelationGetInitTd(relation));
    }

    if (needPageInit) {
        XLogRecPtr lsn = PageGetLSN(page);
        UPageInit<UPAGE_HEAP>(page, BufferGetPageSize(buffer), UHEAP_SPECIAL_SIZE, RelationGetInitTd(relation));
        PageSetLSN(page, lsn);

        UHeapPageHeaderData *uheappage = (UHeapPageHeaderData *)page;
        uheappage->pd_xid_base = xidbase;
        uheappage->pd_multi_base = 0;
    }

    END_CRIT_SECTION();

    UnlockReleaseBuffer(buffer);

    return;
}

/*
 * SimpleUHeapDelete - delete a uheap tuple
 *
 * This routine may be used to delete a tuple when concurrent updates of
 * the target tuple are not expected (for example, because we have a lock
 * on the relation associated with the tuple).  Any failure is reported
 * via ereport().
 */
void SimpleUHeapDelete(Relation relation, ItemPointer tid, Snapshot snapshot, TupleTableSlot** oldslot)
{
    TM_Result result;
    TM_FailureData tmfd;

    result =
        UHeapDelete(relation, tid, GetCurrentCommandId(true), InvalidSnapshot, snapshot, true, /* wait for commit */
            oldslot, &tmfd, false, true);
    switch (result) {
        case TM_SelfUpdated:
        case TM_SelfModified:
            /* Tuple was already updated in current command? */
            elog(ERROR, "tuple already updated by self");
            break;

        case TM_Ok:
            /* done successfully */
            break;

        case TM_Updated:
            elog(ERROR, "tuple concurrently updated");
            break;

        case TM_Deleted:
            elog(ERROR, "tuple concurrently deleted");
            break;

        default:
            elog(ERROR, "unrecognized UHeapDelete status: %u", result);
            break;
    }
}

void UHeapSleepOrWaitForTDSlot(TransactionId xWait, TransactionId myXid /* debug purposes only */,bool isInsert)
{
    if (!u_sess->ustore_cxt.tdSlotWaitActive) {
        Assert(u_sess->ustore_cxt.tdSlotWaitFinishTime == 0);
        u_sess->ustore_cxt.tdSlotWaitFinishTime = TimestampTzPlusMilliseconds(GetCurrentTimestamp(),
                                                                              TD_RESERVATION_TIMEOUT_MS);
        u_sess->ustore_cxt.tdSlotWaitActive = true;
    }

    TimestampTz now = GetCurrentTimestamp();
    if (u_sess->ustore_cxt.tdSlotWaitFinishTime <= now) {
            Assert(TransactionIdIsValid(xWait));
            XactLockTableWait(xWait);
    } else if (!isInsert) {
        pg_usleep(10000L);
    }
}

void UHeapResetWaitTimeForTDSlot()
{
    u_sess->ustore_cxt.tdSlotWaitActive = false;
    u_sess->ustore_cxt.tdSlotWaitFinishTime = 0;
}

/*
 * UPageExtendTDSlots - Extend the number of TD slots in the uheap page.
 *
 * Depending upon the available space and the formula, we extend the number of TD slots
 * and return the first TD slot to the caller. Header is updated with the new
 * TD slot information and free space start marker.
 */
uint8 UPageExtendTDSlots(Relation relation, Buffer buf)
{
    /*
     * 1) Find the current number of TD slots
     * 2) Find how much the row pointers can be moved
     * 3) If there is enough room on the page, move line pointers forward
     * 4) Initialize the newly added TD slots with default values
     * 5) Update the number of TD slots in the page header
     * 6) Update the value of pd_lower and pd_upper
     */

    char *start;
    char *end;
    int i;
    Page page;
    uint8 currTDSlots;
    uint16 freeSpace;
    size_t linePtrSize;
    errno_t ret = EOK;
    TD *thistrans = NULL;
    UHeapPageTDData *tdPtr = NULL;

    page = BufferGetPage(buf);
    UHeapPageHeaderData *phdr = (UHeapPageHeaderData *)page;
    tdPtr = (UHeapPageTDData *)PageGetTDPointer(page);
    currTDSlots = phdr->td_count;
    uint8 numExtended = TD_SLOT_INCREMENT_SIZE;

    if (currTDSlots < TD_THRESHOLD_FOR_PAGE_SWITCH) {
        /* aggressive extension if current td slots is less than the initial threshold */
        numExtended = Min(currTDSlots, TD_THRESHOLD_FOR_PAGE_SWITCH - currTDSlots);
    } else if (currTDSlots >= UHEAP_MAX_TD) {
        /*
         * Cannot extend beyond max allowed count
         */
        ereport(DEBUG5, (errmsg("TD slot count exceeded max allowed. Rel: %s, blkno: %d",
            RelationGetRelationName(relation), BufferGetBlockNumber(buf))));
        return 0;
    }

    /*
     * Check the amount of available space for extension of
     * TD array. In case of insufficient space, extend
     * according to free space
     */
    freeSpace = phdr->pd_upper - phdr->pd_lower;
    if (freeSpace < (numExtended * sizeof(TD))) {
        numExtended = freeSpace / sizeof(TD);
    }

    numExtended = Min(numExtended, UHEAP_MAX_TD - currTDSlots);
    if (numExtended == 0) {
        /*
         * No room for extension
         */
        ereport(DEBUG5, (errmsg("TD slots cannot be extended due to insufficient space Rel: %s, blkno: %d",
            RelationGetRelationName(relation), BufferGetBlockNumber(buf))));
        return 0;
    }

    /*
     * Move the line pointers ahead in the page to make room for
     * added transaction slots.
     */
    start = ((char *)page) + UPageGetRowPtrOffset(page);
    end = page + phdr->pd_lower;
    linePtrSize =  end - start;

    START_CRIT_SECTION();

    ret = memmove_s((char*)start + (numExtended * sizeof(TD)), linePtrSize, start, linePtrSize);
    securec_check(ret, "", "");

    /*
     * Initialize the new TD slots
     */
    for (i = currTDSlots; i < currTDSlots + numExtended; i++) {
        thistrans = &tdPtr->td_info[i];
        thistrans->xactid = InvalidTransactionId;
        thistrans->undo_record_ptr = INVALID_UNDO_REC_PTR;
    }

    /*
     * Reinitialize number of TD slots and begining
     * of free space in the header
     */
    phdr->td_count = currTDSlots + numExtended;
    phdr->pd_lower += numExtended * sizeof(TD);

    MarkBufferDirty(buf);

    if (RelationNeedsWAL(relation)) {
        LogUPageExtendTDSlots(buf, currTDSlots, numExtended);
    }

    END_CRIT_SECTION();

    return numExtended;
}

static void LogUPageExtendTDSlots(Buffer buf, uint8 currTDSlots, uint8 numExtended)
{
    Page page;

    page = BufferGetPage(buf);
    XlUHeapExtendTdSlots xlrec = {0};
    XLogRecPtr recptr;

    XLogBeginInsert();

    xlrec.nExtended = numExtended;
    xlrec.nPrevSlots = currTDSlots;

    XLogRegisterData((char *) &xlrec, SizeOfUHeapExtendTDSlot);
    XLogRegisterBuffer(0, buf, REGBUF_STANDARD);

    recptr = XLogInsert(RM_UHEAP2_ID, XLOG_UHEAP2_EXTEND_TD_SLOTS);
    PageSetLSN(page, recptr);
}
