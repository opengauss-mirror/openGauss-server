/*
 * Copyright (c) 2024 Huawei Technologies Co.,Ltd.
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
 * --------------------------------------------------------------------------------------
 *
 * ubtpcrtd.cpp
 *        Relaize the management of transaction directory slots for ubtree pcr page.
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/ubtreepcr/ubtpcrtd.cpp
 *
 * --------------------------------------------------------------------------------------
 */


#include "access/ubtreepcr.h"
#include "storage/procarray.h"
#include "storage/lmgr.h"


static bool UBTreeFreezeOrInvalidIndexTuplesSetTd(const ItemId itemId, const IndexTupleTrx trx, uint8 *tdSlot)
{
    if (ItemIdIsDead(itemId))
        return true;

    if (!ItemIdIsUsed(itemId)) {
        return true;
    } 
    
    *tdSlot = trx->tdSlot;
    Assert((*tdSlot <= UBTREE_MAX_TD_COUNT) && (*tdSlot >= 0));

    return false;
}


/*
 * UBTreeFreezeOrInvalidIndexTuples  
 * 
 * Clear the slot information or set invalid_xact flags.
 * 
 */
void UBTreeFreezeOrInvalidIndexTuples(Buffer buf, int nSlots, const uint8 *slots, bool isFrozen)
{
    Page page = BufferGetPage(buf);

    /* clear the slot info from tuples */
    OffsetNumber maxoff = UBTreePCRPageGetMaxOffsetNumber(page);
    uint8 map[UBTREE_MAX_TD_COUNT + 1] = { 0 };

    for (int i = 0;i < nSlots;i ++) {
        map[slots[i]] = 1;
    }

    OffsetNumber offnum = P_FIRSTDATAKEY((UBTPCRPageOpaque)PageGetSpecialPointer(page));
    for (; offnum <= maxoff; offnum = OffsetNumberNext(offnum)) {
        ItemId itemId = (ItemId)UBTreePCRGetRowPtr(page, offnum);
        IndexTuple itup = (IndexTuple)UBTreePCRGetIndexTuple(page, offnum);
        IndexTupleTrx trx = (IndexTupleTrx)UBTreePCRGetIndexTupleTrx(itup);
        uint8 tdSlot = UBTreeInvalidTDSlotId;

        if (UBTreeFreezeOrInvalidIndexTuplesSetTd(itemId, trx, &tdSlot)) {
            continue;
        }

        /*
         * The slot number on tuple is always array location of slot plus
         * one, so we need to subtract one here before comparing it with
         * frozen slots.  See PageReserveTransactionSlot.
         */
        if (map[tdSlot] == 1) {
            /*
             * Set transaction slots of tuple as frozen to indicate tuple
             * is all visible and mark the deleted itemids as dead.
             */
            if (isFrozen) {
                if (IsUBTreePCRItemDeleted(trx)) {
                    ItemIdSetDead(itemId);
                } else {
                    IndexItemIdSetFrozen(itemId);
                    UBTreePCRSetIndexTupleTrxSlot(trx, UBTreeFrozenTDSlotId);
                    UBTreePCRClearIndexTupleTrxInvalid(trx);
                }
            } else {
                /*
                 * We just set the invalid slot flag in the
                 * IndexTupleTrxData to indicate that for this 
                 * index tuple we need to fetch the transaction 
                 * information from undo record.
                 */
                UBTreePCRSetIndexTupleTrxInvalid(trx);
            }
        }
    }
}

/*
 * UBTreePageFreezeTransationSlots 
 * 
 * Make the transaction slots available for reuse.
 * 
 * Return reused tdSlot
 */
uint8 UBTreePageFreezeTransationSlots(Relation relation, Buffer buf, TransactionId* minXid)
{
    uint8 reuseSlot = UBTreeInvalidTDSlotId;
    Page page = BufferGetPage(buf);
    uint8 numSlots = UBTreePageGetTDSlotCount(page);
    TransactionId oldestXid = pg_atomic_read_u64(&g_instance.undo_cxt.globalRecycleXid);

    /*
     * Clear the slot information from tuples.  The basic idea is to collect
     * all the transaction slots that can be cleared.  Then traverse the page
     * to see if any tuple has marking for any of the slots, if so, just clear
     * the slot information from the tuple.
     *
     * For temp relations, we can freeze the first slot since no other backend
     * can access the same relation.
     */
    uint8 frozenSlots[UBTREE_MAX_TD_COUNT] = { 0 };
    uint8 nFrozenSlots = 0;
    if (RELATION_IS_LOCAL(relation)) {
        frozenSlots[nFrozenSlots++] = 0;
    } else {
        for (uint8 slotNo = 0; slotNo < numSlots; slotNo++) {
            UBTreeTD thisTrans = UBTreePCRGetTD(page, slotNo + 1);
            TransactionId slotXactid = thisTrans->xactid;
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
        uint8 slotNo;

        START_CRIT_SECTION();

        /* clear the transaction slot info on indextuples */
        UBTreeFreezeOrInvalidIndexTuples(buf, nFrozenSlots, frozenSlots, true);

        /* Initialize the frozen slots. */
        for (uint8 i = 0; i < nFrozenSlots; i++) {
            UBTreeTD thisTrans;

            slotNo = frozenSlots[i];
            thisTrans = UBTreePCRGetTD(page, slotNo + 1);

            /* Remember the latest xid. */
            if (TransactionIdFollows(thisTrans->xactid, latestfxid)){
                latestfxid = thisTrans->xactid;
            }
            thisTrans->setFrozen();
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
            // TODO: Realize xlog for freeze TD 
        }

        END_CRIT_SECTION();

        return frozenSlots[nFrozenSlots - 1];
    }

    Assert(!RELATION_IS_LOCAL(relation));

    uint8 nCompletedXactSlots = 0;
    uint8 nAbortedXactSlots = 0;
    uint8 completedXactSlots[UBTREE_MAX_TD_COUNT] = { 0 };
    uint8 abortedXactSlots[UBTREE_MAX_TD_COUNT] = { 0 };
    /*
     * Try to reuse transaction slots of committed/aborted transactions. This
     * is just like above but it will maintain a link to the previous
     * transaction undo record in this slot.  This is to ensure that if there
     * is still any alive snapshot to which this transaction is not visible,
     * it can fetch the record from undo and check the visibility.
     */
    for (int slotNo = 0; slotNo < numSlots; slotNo++) {
        UBTreeTD thisTrans = UBTreePCRGetTD(page, slotNo + 1);
        TransactionId slotXid = thisTrans->xactid;
        if (UBTreePCRTDIsCommited(thisTrans) || UHeapTransactionIdDidCommit(slotXid)) {
            completedXactSlots[nCompletedXactSlots++] = slotNo;
        } else if (!TransactionIdIsInProgress(slotXid, NULL, false, true)) {
            if (UHeapTransactionIdDidCommit(slotXid)) {
                completedXactSlots[nCompletedXactSlots++] = slotNo;
            } else {
                abortedXactSlots[nAbortedXactSlots++] = slotNo;
            }
        }
    }

    if (nAbortedXactSlots > 0) {
        for (uint8 i = 0; i < nAbortedXactSlots; i ++) {
            ExecuteUndoActionsForUBTreePage(relation, buf, abortedXactSlots[nAbortedXactSlots]);
        }
        reuseSlot = abortedXactSlots[nAbortedXactSlots - 1];
    }

    if (nCompletedXactSlots > 0) {
        START_CRIT_SECTION();

        /* clear the transaction slot info on tuples */
        UBTreeFreezeOrInvalidIndexTuples(buf, nCompletedXactSlots, completedXactSlots, false);

        /*
         * Clear the xid information from the slot but keep the undo record
         * pointer as it is so that undo records of the transaction are
         * accessible by traversing slot's undo chain even though the slots
         * are reused.
         */
        UBTPCRPageOpaque opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
        TransactionId lastCommitXid;
        for (uint8 i = 0; i < nCompletedXactSlots; i++) {
            UBTreeTD thisTrans = UBTreePCRGetTD(page, completedXactSlots[i] + 1);
            Assert(TransactionIdIsValid(thisTrans->xactid));
            UBTreePCRTDClearStatus(thisTrans, TD_ACTIVE);
            UBTreePCRTDSetStatus(thisTrans, TD_COMMITED);
            lastCommitXid = opaque->last_commit_xid;
            if (!TransactionIdIsValid(lastCommitXid) || TransactionIdFollows(thisTrans->xactid, lastCommitXid)) {
                opaque->last_commit_xid = thisTrans->xactid;
            }
        }
        MarkBufferDirty(buf);

        /*
         * Xlog Stuff
         */
        if (RelationNeedsWAL(relation)) {
            // TODO: Realize xlog for freeze TD
        }

        END_CRIT_SECTION();

        reuseSlot = completedXactSlots[nCompletedXactSlots - 1];
    }

    return reuseSlot;
}

/*
 * UBTreeExtendTDSlots
 *
 * Extend the number of TD slots in the uheap page.
 *
 * Depending upon the available space and the formula, we extend the number of TD slots
 * and return the first TD slot to the caller. Header is updated with the new
 * TD slot information and free space start marker.
 */
uint8 UBTreeExtendTDSlots(Relation relation, Buffer buf)
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
    Page page = BufferGetPage(buf);
    uint8 currTDSlots = UBTreePageGetTDSlotCount(page);
    PageHeader phdr = (PageHeader)page;
    UBTPCRPageOpaque opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
    uint16 freeSpace = PageGetFreeSpace(page);
    size_t linePtrSize;
    errno_t ret = EOK;
    uint8 numExtended = UBTREE_TD_SLOT_INCREMENT_SIZE;

    if (currTDSlots < UBTREE_TD_THRESHOLD_FOR_PAGE_SWITCH) {
        /* aggressive extension if current td slots is less than the initial threshold */
        numExtended = Min(currTDSlots, UBTREE_TD_THRESHOLD_FOR_PAGE_SWITCH - currTDSlots);
    } else if (currTDSlots >= UBTREE_MAX_TD_COUNT) {
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
    if (freeSpace < (numExtended * sizeof(UBTreeTDData))) {
        numExtended = freeSpace / sizeof(UBTreeTDData);
    }

    numExtended = Min(numExtended, UBTREE_MAX_TD_COUNT - currTDSlots);
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
    start = ((char *)page) + UBTreePCRGetRowPtrOffset(page);
    end = ((char *)page) + phdr->pd_lower;
    linePtrSize =  end - start;

    START_CRIT_SECTION();

    ret = memmove_s((char*)start + (numExtended * sizeof(UBTreeTDData)), linePtrSize, start, linePtrSize);
    securec_check(ret, "", "");

    /*
     * Initialize the new TD slots
     */
    for (int i = currTDSlots; i < currTDSlots + numExtended; i++) {
        UBTreeTD thisTrans = UBTreePCRGetTD(page, i + 1);
        thisTrans->xactid = InvalidTransactionId;
        thisTrans->undoRecPtr = INVALID_UNDO_REC_PTR;
    }

    /*
     * Reinitialize number of TD slots and begining
     * of free space in the header
     */
    opaque->td_count = currTDSlots + numExtended;
    phdr->pd_lower += numExtended * sizeof(UBTreeTDData);

    MarkBufferDirty(buf);

    if (RelationNeedsWAL(relation)) {
        // TODO: 补充xlog
        //LogUPageExtendTDSlots(buf, currTDSlots, numExtended);
    }

    END_CRIT_SECTION();

    return numExtended;
}

void UBTreePCRHandlePreviousTD(Relation rel, Buffer buf, uint8 *slotNo, IndexTupleTrx trx, bool *needRetry)
{
    if (UBTreeTDSlotIsNormal(*slotNo) && !IsUBTreePCRTDReused(trx)) {
        Page page = BufferGetPage(buf);
        UBTreeTD td = UBTreePCRGetTD(page, *slotNo);
        TransactionId xid = td->xactid;
        if (TransactionIdIsValid(xid) && !UBTreePCRTDIsFrozen(td) && !UBTreePCRTDIsCommited(td)) {
            if (TransactionIdIsCurrentTransactionId(xid)) {
                return;
            }
            if (TransactionIdIsInProgress(xid, NULL, false, true, true)) {
                _bt_relbuf(rel, buf);
                XactLockTableWait(xid);
                *needRetry = true;
                return;
            }

            if (!TransactionIdDidCommit(xid)) {
                ExecuteUndoActionsForUBTreePage(rel, buf, *slotNo);
            }
            *slotNo = trx->tdSlot;
        }
    }
}

/*
 * UBTreePageReserveTransactionSlot 
 *
 * Reserve the transaction slot in ubtree page.
 *
 * This function returns transaction slot number if either the page already
 * has some slot that contains the transaction info or there is an empty
 * slot or it manages to reuse some existing slot ; otherwise returns
 * InvalidTDSlotId.
 *
 * Note that we always return array location of slot plus one as zeroth slot
 * number is reserved for frozen slot number.
 *
 * If we've reserved a transaction slot of a committed but not all-visible
 * transaction, we set slotReused as true, false otherwise.
 * 
 */
uint8 UBTreePageReserveTransactionSlot(Relation relation, Buffer buf, TransactionId fxid, 
    UBTreeTD oldTd, TransactionId *minXid)
{
    Page page = BufferGetPage(buf);
    uint8 latestFreeTDSlot = UBTreeInvalidTDSlotId;
    uint8 slotNo;
    uint8 nExtended;
    uint8 tdCount = UBTreePageGetTDSlotCount(page);

    TransactionId currMinXid = MaxTransactionId;

    // TODO: uheap有pgstat report，后面考虑下我们要不要加

    /*
     * For temp relations, we don't have to check all the slots since no other
     * backend can access the same relation. If a slot is available, we return
     * it from here. Else, we freeze the slot in PageFreezeTransSlots.
     *
     * XXX For temp tables, globalRecycleXid is not relevant as
     * the undo for them can be discarded on commit.  Hence, comparing xid
     * with globalRecycleXid during visibility checks can lead to
     * incorrect behavior.  To avoid that, we can mark the tuple as frozen for
     * any previous transaction id.  In that way, we don't have to compare the
     * previous xid of tuple with globalRecycleXid.
     */
    if (RELATION_IS_LOCAL(relation)) {
        /* We can't access temp tables of other backends. */
        Assert(!RELATION_IS_OTHER_TEMP(relation));
        slotNo = 0;
        UBTreeTD thisTrans = UBTreePCRGetTD(page, slotNo + 1);

        if (TransactionIdEquals(thisTrans->xactid, fxid)) {
            *oldTd = *thisTrans;
            return (slotNo + 1);
        } else if (!TransactionIdIsValid(thisTrans->xactid)){
            latestFreeTDSlot = slotNo;
        }
    } else {
        for (slotNo = 0; slotNo < tdCount; slotNo++) {
            UBTreeTD thisTrans = UBTreePCRGetTD(page, slotNo + 1);

            if (TransactionIdIsValid(thisTrans->xactid)) {
                if (TransactionIdEquals(thisTrans->xactid, fxid)) {
                    /* Already reserved by ourself */
                    *oldTd = *thisTrans;
                    return (slotNo + 1);
                } else {
                    currMinXid = Min(currMinXid, thisTrans->xactid);
                }
            } else if (latestFreeTDSlot == UBTreeInvalidTDSlotId) {
                /* Got an available slot */
                latestFreeTDSlot = slotNo;
            }
        }
    }

    if (UBTreeTDSlotIsNormal(latestFreeTDSlot + 1)) {
        UBTreeTD thisTrans = UBTreePCRGetTD(page, latestFreeTDSlot + 1);
        *oldTd = *thisTrans;
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
    uint8 reuseSlot = UBTreePageFreezeTransationSlots(relation, buf, minXid);
    if (reuseSlot != UBTreeInvalidTDSlotId) {
        return reuseSlot + 1;
    }

    nExtended = UBTreeExtendTDSlots(relation, buf);
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
    /* no transaction slot available */
    return UBTreeInvalidTDSlotId;
}
