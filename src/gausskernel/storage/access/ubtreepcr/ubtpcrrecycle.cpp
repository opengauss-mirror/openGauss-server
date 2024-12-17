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
 * ubtpcrrecycle.cpp
 *       Recycle logic for ubtree par page.
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/ubtreepcr/ubtpcrrecycle.cpp
 *
 * --------------------------------------------------------------------------------------
 */

#include "postgres.h"
#include "pgstat.h"
#include "access/ubtree.h"
#include "access/ubtreepcr.h"
#include "access/nbtree.h"
#include "storage/predicate.h"
#include "postmaster/autovacuum.h"

void PruneFirstDataKey(Page page, ItemId itemid, OffsetNumber offNum, IndexTupleTrx itrx,
    bool*frozenTDMap, UBTPCRPruneState* prstate, UndoPersistence upersistence)
{
    UBTPCRPageOpaque opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
    Assert(IsUBTreePCRItemDeleted(itrx));
    Assert(offNum == P_FIRSTDATAKEY(opaque));
    TransactionId globalRecycleXid = pg_atomic_read_u64(&g_instance.undo_cxt.globalRecycleXid);
    TransactionId xmax = GetXidFromTrx(page, itrx, frozenTDMap);

    if (IsUBTreePCRTDReused(itrx)) {
        xmax = opaque->last_commit_xid;
    }

    if (TransactionIdPrecedes(xmax, globalRecycleXid)) {
        RecordDeadTuple(prstate, offNum, xmax);
        return;
    } else {
        uint8 curTdId = UBTreePCRGetIndexTupleTrxSlot(itrx);
        UBTreeTD curTd = (UBTreeTD)UBTreePCRGetTD(page, curTdId);
        IndexTuple itup = (IndexTuple)UBTreePCRGetIndexTuple(page, offNum);
        /* not match situation above, msut fetch the accurate xmax from undo */
        UBTreeLatestChangeInfo uInfo;
        UndoRecord *urec = New(CurrentMemoryContext)UndoRecord();
        urec->SetUrp(curTd->undoRecPtr);
        urec->SetMemoryContext(CurrentMemoryContext);
        // to do:UBTreeFetchLatestChangeFromUndo
        if (UBTreeFetchLatestChangeFromUndo(itup, urec, &uInfo, upersistence)) {
            xmax = uInfo.xid;
        } else {
            xmax = FrozenTransactionId;
        }

        DELETE_EX(urec);
        if (TransactionIdPrecedes(xmax, globalRecycleXid)) {
            RecordDeadTuple(prstate, offNum, xmax);
            return;
        }
    }
}

/*
 *  UBTreePCRPrunePageOpt() -- Optional prune a index page
 *
 * This function trying to prune the page if possible, and always do UBTreePCRPageRepairFragmentation()
 * if we deleted any dead tuple.
 *
 * The passed tryDelete indicate whether we just try to delete the whole page. When this value is
 * true, we will do nothing if there is an alive tuple in the page, and delete the whole page if all
 * tuples are dead.
 *
 * IMPORTANT: Make sure we've got the write lock on the buf before call this function.
 *
 *      Note: The page must be a leaf page.
 */
bool UBTreePCRPrunePageOpt(Relation rel, Buffer buf, bool tryDelete, BTStack del_blknos,
    bool pruneDelete)
{
    Page page = BufferGetPage(buf);
    UBTPCRPageOpaque opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);

    /*
     * We can't write WAL in recovery mode, so there's no point trying to
     * clean the page. The master will likely issue a cleaning WAL record soon
     * anyway, so this is no particular loss.
     */
    if (RecoveryInProgress()) {
        if (tryDelete)
            _bt_relbuf(rel, buf);
        return false;
    }

    if (!P_ISLEAF(opaque)) {
        /* we can't prune internal pages */
        if (tryDelete)
            _bt_relbuf(rel, buf);
        return false;
    }

    if (tryDelete && P_RIGHTMOST(opaque)) {
        /* can't delete the right most page */
        _bt_relbuf(rel, buf);
        return false;
    }

    TransactionId globalFrozenXid = pg_atomic_read_u64(&g_instance.undo_cxt.globalFrozenXid);

    /*
     * Should not prune page when use local snapshot, InitPostgres e.g.
     * If use local snapshot RecentXmin might not consider xid cn send later,
     * wrong xid status might be judged in TransactionIdIsInProgress,
     * and wrong tuple infomask might be set in HeapTupleSatisfiesVacuum.
     * So if useLocalSnapshot is ture in postmaster env, we don't prune page.
     * Keep prune page can be done in single mode (standlone --single), so just in PostmasterEnvironment.
     */
    if ((t_thrd.xact_cxt.useLocalSnapshot && IsPostmasterEnvironment) ||
        g_instance.attr.attr_storage.IsRoachStandbyCluster || u_sess->attr.attr_common.upgrade_mode == 1) {
        if (tryDelete)
            _bt_relbuf(rel, buf);
        return false;
    }

    /*
     * PCR prune condition   
     */
    if (!P_ISHALFDEAD(opaque) && !UBTreePCRSatisfyPruneCondition(rel, buf, globalFrozenXid, pruneDelete)) {
        if (tryDelete)
            _bt_relbuf(rel, buf);
        return false;
    }

    /* Ok to prune */
    if (tryDelete) {
        /* if we are trying to delete a page, prune it first (we need WAL), then check empty and delete */
        UBTreePCRPagePrune(rel, buf, globalFrozenXid, NULL, pruneDelete);

        Assert(!P_RIGHTMOST(opaque));
        if (UBTPCRPageGetMaxOffsetNumber(page) == 1) {
            /* already empty (only HIKEY left), ok to delete */
            return UBTreePCRPageDel(rel, buf, del_blknos) > 0;
        } else {
            _bt_relbuf(rel, buf);
            return false;
        }
    }

    return UBTreePCRPagePrune(rel, buf, globalFrozenXid, NULL, pruneDelete);
}

/*
 * UBTreePCRPruneItem()  --  Check a item's status.
 *
 * Return true if item is not dead and not deleted. 
 */
bool UBTreePCRPruneItem(Page page, OffsetNumber offNum, ItemId itemid, TransactionId globalFrozenXid,
    bool* frozenTDMap, bool pruneDelete, UBTPCRPruneState* prstate, UndoPersistence upersistence)
{
    UBTPCRPageOpaque opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
    IndexTuple itup = (IndexTuple)UBTreePCRGetIndexTuple(page, offNum);
    IndexTupleTrx itrx = (IndexTupleTrx)UBTreePCRGetIndexTupleTrx(itup);
    uint8 curTdId = UBTreePCRGetIndexTupleTrxSlot(itrx);
    UBTreeTD curTd = (UBTreeTD)UBTreePCRGetTD(page, curTdId);
    if (IsUBTreePCRItemDeleted(itrx)) {
        if (offNum == P_FIRSTDATAKEY(opaque)) {
            PruneFirstDataKey(page, itemid, offNum, itrx, frozenTDMap, prstate, upersistence);
            return false;
        }

        TransactionId xmax = GetXidFromTrx(page, itrx, frozenTDMap);
        if (TransactionIdPrecedes(xmax, globalFrozenXid)) {
            RecordDeadTuple(prstate, offNum, xmax);
            return false;
        } else if (pruneDelete) {
            if (IsUBTreePCRTDReused(itrx)) {
                RecordDeadTuple(prstate, offNum, xmax);
                return false;
            }
            if (!UBTreePCRTDIsActive(curTd)) {
                /* xid is committed, we have done rollback action before
                to deal with aborted xid. */
                RecordDeadTuple(prstate, offNum, xmax);
                return false;
            }
        } else if (IsUBTreePCRTDReused(itrx)) {
            // 如果不进行强制清理，进行reused td清理（td复用了 上一个xid 必然可见，last_commit_xid也必然可见，不需要查找准确的xid）
            /* TD is reused by others and the latest xid on this TD is not Frozen, try use last_commit_xid */
            xmax = opaque->last_commit_xid;
            if (TransactionIdPrecedes(xmax, globalFrozenXid)) {
                RecordDeadTuple(prstate, offNum, xmax);
                return false;
            } else {
                /* not match situation above, msut fetch the accurate xmax from undo */
                UBTreeLatestChangeInfo uInfo;
                UndoRecord *urec = New(CurrentMemoryContext)UndoRecord();
                urec->SetUrp(curTd->undoRecPtr);
                urec->SetMemoryContext(CurrentMemoryContext);
                // to do:UBTreeFetchLatestChangeFromUndo
                if (UBTreeFetchLatestChangeFromUndo(itup, urec, &uInfo, upersistence)) {
                    xmax = uInfo.xid;
                } else {
                    xmax = FrozenTransactionId;
                }

                DELETE_EX(urec);
                if (TransactionIdPrecedes(xmax, globalFrozenXid)) {
                    RecordDeadTuple(prstate, offNum, xmax);
                    return false;
                }
            }
        }
    } 
    return !IsUBTreePCRItemDeleted(itrx);
}

/*
 *  UBTreePCRPagePrune() -- Apply page pruning and reorder the PCR page.
 *  
 * 
 */
bool UBTreePCRPagePrune(Relation rel, Buffer buf, TransactionId globalFrozenXid, OidRBTree *invisibleParts, bool pruneDelete) 
{
    if (pruneDelete) {
        pruneDelete = !UBTreeIsToastIndex(rel);
    }
    Page page = BufferGetPage(buf);
    UBTPCRPruneState prstate = {0};
    BlockNumber blkno = BufferGetBlockNumber(buf);
    TransactionId recycleXid = pg_atomic_read_u64(&g_instance.undo_cxt.globalRecycleXid);
    TransactionId latestFrozenXid = InvalidTransactionId;
    UndoPersistence upersistence = UndoPersistenceForRelation(rel);

    /* step 1: rollback abort transactions and record frozen TD */
    int tdCount = UBTreePageGetTDSlotCount(page);
    bool frozenTDMap[UBTREE_MAX_TD_COUNT] = {false};
    for (int slot = 0; slot < tdCount; slot++) {
        UBTreeTD curTd = (UBTreeTD)UBTreePCRGetTD(page, slot + 1);
        if (UBTreePCRTDIsFrozen(curTd)) {
            /* clean up unsed ir frozen xid */
            frozenTDMap[slot] = true;
            UBTreePCRTDClearStatus(curTd, TD_DELETE);
            continue; 
        }
        
        if (UBTreePCRTDIsActive(curTd)) {
            /* xid is in-progress */
            continue;
        /* not active: committed or (aborted)rollback */
        } else if (!UBTreePCRTDIsCommited(curTd)) {
            /* xid is aborted */
            ExecuteUndoActionsForUBTreePage(rel, buf, slot + 1);
            Assert(!UBTreePCRTDIsActive(curTd));
            Assert(UBTreePCRTDIsCommited(curTd) == IS_VALID_UNDO_REC_PTR(curTd->undoRecPtr));
        }

        /* xid is commited */
        if (TransactionIdIsValid(curTd->xactid) && TransactionIdPrecedes(curTd->xactid, recycleXid)) {
            frozenTDMap[slot] = true;
            UBTreePCRTDClearStatus(curTd, TD_DELETE);
            if (TransactionIdFollows(curTd->xactid, latestFrozenXid)) {
                latestFrozenXid = curTd->xactid;
            }
        } else {
            if (pruneDelete) {
                UBTreePCRTDClearStatus(curTd, TD_DELETE);
            }
        }
    }

    /* step 2: count the number of frozen and commited td, which are used for td compact */
    int canCompactCount = ComputeCompactTDCount(tdCount, frozenTDMap);

    /* step 3: scan page, travse all tuples */
    int activeTupleCount = 0;
    UBTPCRPageOpaque opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
    OffsetNumber maxOff = UBTPCRPageGetMaxOffsetNumber(page);
    for (OffsetNumber offNum = P_FIRSTDATAKEY(opaque); offNum <= maxOff;
        offNum = OffsetNumberNext(offNum)) {
        ItemId itemid = UBTreePCRGetRowPtr(page, offNum);
        /* nothing to do if slot is alraeady dead, just count previousDeadlen */
        if (ItemIdIsDead(itemid)) {
            prstate.previousDead[prstate.previousDeadlen++] = offNum;
            continue;
        }

        /* global index need remove tuples of invisivle partition */
        if (RelationIsGlobalIndex(rel) && invisibleParts != NULL) {
            AttrNumber partitionOidAttr = IndexRelationGetNumberOfAttributes(rel);
            TupleDesc tupdesc = RelationGetDescr(rel);
            IndexTuple itup = (IndexTuple)UBTreePCRGetIndexTupleByItemId(page, itemid);
            bool isNull = false;
            Oid partOid = DatumGetUInt32(index_getattr(itup, partitionOidAttr, tupdesc, &isNull));
            Assert(!isNull);

            if (OidRBTreeMemberOid(invisibleParts, partOid)) {
                /* record dead */
                prstate.nowDead[prstate.nowDeadLen++] = offNum;
                continue;
            }
        }

        /* check visibility and record dead tuples */
        if (UBTreePCRPruneItem(page, offNum, itemid, globalFrozenXid, frozenTDMap, pruneDelete,
                &prstate, upersistence)) {
            activeTupleCount++;
        }
    }

    START_CRIT_SECTION();

    /* step 4: mark dead tuple, compact td, reorder and clean up fragmentatioin */
    bool hasPruned = false;
    bool needCompactTd = (tdCount > UBTREE_DEFAULT_TD_COUNT && canCompactCount > 0);
    if (prstate.previousDeadlen > 0 || prstate.nowDeadLen > 0) {
        /* setup flags and try to repair page fragmentation */
        WaitState oldStatus = pgstat_report_waitstatus(STATE_PRUNE_INDEX);
        UBTreePCRPrunePageExecute(page, prstate.previousDead, prstate.previousDeadlen);
        UBTreePCRPrunePageExecute(page, prstate.nowDead, prstate.nowDeadLen);
        if (needCompactTd) {
            CompactTd(page, tdCount, canCompactCount, frozenTDMap);
        } else {
            canCompactCount = 0;
            FreezeTd(page, frozenTDMap, tdCount);
        }

        UBTreePCRPageRepairFragmentation(rel, blkno, page);

        hasPruned = true;
        opaque->activeTupleCount = activeTupleCount;
        
        MarkBufferDirty(buf);
        
        /*xlog stuff*/
        if (RelationNeedsWAL(rel)) {
            XLogRecPtr recptr;
            xl_ubtree3_prune_page xlrec;
            xlrec.pruneTupleCount = prstate.previousDeadlen + prstate.nowDeadLen;
            xlrec.activeTupleCount = activeTupleCount;
            xlrec.canCompactTdCount = canCompactCount;
            xlrec.latestRemovedXid = prstate.latestRemovedXid;
            xlrec.latestFrozenXid = globalFrozenXid;

            XLogBeginInsert();
            XLogRegisterData((char*)&xlrec, SizeOfUBtree3Prunepage);
            XLogRegisterData((char*)&prstate.previousDead, prstate.previousDeadlen * sizeof(OffsetNumber));
            XLogRegisterData((char*)&prstate.nowDead, prstate.nowDeadLen * sizeof(OffsetNumber));
            XLogRegisterData((char*)&frozenTDMap, tdCount * sizeof(bool));

            XLogRegisterBuffer(0, buf, REGBUF_STANDARD);

            recptr = XLogInsert(RM_UBTREE3_ID, XLOG_UBTREE3_PRUNE_PAGE_PCR);

            PageSetLSN(page, recptr);
        }
        pgstat_report_waitstatus(oldStatus);
    } else {
        if (opaque->activeTupleCount != activeTupleCount) {
            opaque->activeTupleCount = activeTupleCount;
            MarkBufferDirtyHint(buf, true);
        }
    }

    END_CRIT_SECTION();
    // to do: verify stuff 待处理
    return hasPruned;
}

bool UBTreeIsToastIndex(Relation rel)
{
    if (rel->rd_amcache != NULL) {
        BTMetaPageData *metaData = (BTMetaPageData*)rel->rd_amcache;
        return OidIsValid(metaData->btm_reltoastrelid);
    }
    if (OidIsValid(rel->rd_rel->reltoastrelid)) {
        return true;
    }

    Buffer metaBuf = _bt_getbuf(rel, BTREE_METAPAGE, BT_READ);
    Page metaPage = BufferGetPage(metaBuf);
    BTMetaPageData *metaData = BTPageGetMeta(metaPage);
    bool isToast = OidIsValid(metaData->btm_reltoastrelid);
    rel->rd_rel->reltoastrelid = metaData->btm_reltoastrelid;
    _bt_relbuf(rel, metaBuf);
    return isToast;
}

/* compact continuously frozen and committed td from back to front */
int ComputeCompactTDCount(int tdCount, bool* frozenTDMap)
{
    int canCompactCount = 0;
    for (int slot = tdCount - 1; slot >= 0; slot--) {
        if (!frozenTDMap[slot]) {
            break;
        }
        canCompactCount++;
    }
    return canCompactCount;
}

TransactionId GetXidFromTrx(Page page, IndexTupleTrx itrx, bool* frozenTDMap)
{
    uint8 tdId = UBTreePCRGetIndexTupleTrxSlot(itrx);
    TransactionId xid = InvalidTransactionId;
    if (tdId == UBTreeFrozenTDSlotId || frozenTDMap[tdId - 1]) {
        xid = FrozenTransactionId;
    } else {
        xid = ((UBTreeTD)UBTreePCRGetTD(page, tdId))->xactid;
    }
    return xid;
}

void RecordDeadTuple(UBTPCRPruneState* prstate, OffsetNumber offNum, TransactionId xid)
{
    prstate->nowDead[prstate->nowDeadLen++] = offNum;
    if (TransactionIdFollows(xid, prstate->latestRemovedXid)) {
        prstate->latestRemovedXid = xid;
    }
}

void UBTreePCRPrunePageExecute(Page page, OffsetNumber* deadOff, int deadLen)
{
    /* travse all tuples */
    for (int i = 0; i < deadLen; i++) {
        ItemId itemid = UBTreePCRGetRowPtr(page, deadOff[i]);
        ItemIdMarkDead(itemid);
    }
}

void CompactTd(Page page, int tdCount, int canCompactCount, bool* frozenTDMap)
{
    UBTPCRPageOpaque opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
    OffsetNumber maxOff = UBTPCRPageGetMaxOffsetNumber(page);
    for (OffsetNumber offNum = P_FIRSTDATAKEY(opaque); offNum <= maxOff;
        offNum = OffsetNumberNext(offNum)) {
        ItemId itemid = UBTreePCRGetRowPtr(page, offNum);
        IndexTuple itup = (IndexTuple)UBTreePCRGetIndexTupleByItemId(page, itemid);
        IndexTupleTrx itrx = (IndexTupleTrx)UBTreePCRGetIndexTupleTrx(itup);
        uint8 tdId = UBTreePCRGetIndexTupleTrxSlot(itrx);
        if (tdId != UBTreeFrozenTDSlotId && frozenTDMap[tdId - 1]) {
            UBTreePCRSetIndexTupleTrxSlot(itrx, UBTreeFrozenTDSlotId);
        }
    }

    /* if canCompactCount is too larger, reduce compact td number to half */
    if (canCompactCount >= UBTREE_TD_THRESHOLD_FOR_PAGE_SWITCH) {
        canCompactCount /= 2;
    }

    int newTdCount = UBTREE_DEFAULT_TD_COUNT > (tdCount - canCompactCount) ?
        UBTREE_DEFAULT_TD_COUNT : (tdCount - canCompactCount);
    for (int slot = 0; slot < newTdCount; slot++) {
        if (frozenTDMap[slot]) {
            UBTreeTD curTd = (UBTreeTD)UBTreePCRGetTD(page, slot + 1);
            /* reset td to frozen */
            curTd->setFrozen();
        }
    }
    int newTdOff = SizeOfPageHeaderData + UBTreePCRTdSlotSize(newTdCount);
    PageHeader pghdr = (PageHeader)page;
    int oldTdOff = SizeOfPageHeaderData + UBTreePCRTdSlotSize(tdCount);
    int itemidSize = pghdr->pd_lower - oldTdOff;
    /* move rowPtr from behind the old td to behind the new td. */
    errno_t rc = memmove_s(page + newTdOff, BLCKSZ - newTdOff, page + oldTdOff, itemidSize);
    securec_check(rc, "", "");
    pghdr->pd_lower = newTdOff + itemidSize;
    opaque->td_count = newTdCount;
}

void FreezeTd(Page page, bool* frozenTDMap, int tdCount)
{
    for (int slot = 0; slot < tdCount; slot++) {
        if (!frozenTDMap[slot]) {
            continue;
        }
        UBTreeTD curTd = (UBTreeTD)UBTreePCRGetTD(page, slot + 1);
        /* reset td to frozen */
        curTd->setFrozen();
    }

    UBTPCRPageOpaque opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
    OffsetNumber maxOff = UBTPCRPageGetMaxOffsetNumber(page);
    for (OffsetNumber offNum = P_FIRSTDATAKEY(opaque); offNum <= maxOff;
        offNum = OffsetNumberNext(offNum)) {
        ItemId itemid = UBTreePCRGetRowPtr(page, offNum);
        IndexTuple itup = (IndexTuple)UBTreePCRGetIndexTupleByItemId(page, itemid);
        IndexTupleTrx itrx = (IndexTupleTrx)UBTreePCRGetIndexTupleTrx(itup);
        uint8 tdId = UBTreePCRGetIndexTupleTrxSlot(itrx);
        if (tdId < tdCount && tdId != UBTreeFrozenTDSlotId && frozenTDMap[tdId - 1]) {
            UBTreePCRSetIndexTupleTrxSlot(itrx, UBTreeFrozenTDSlotId);
        }
    }
}

void UBTreePCRPageRepairFragmentation(Relation rel, BlockNumber blkno, Page page)
{
    Offset pdLower = ((PageHeader)page)->pd_lower;
    Offset pdUpper = ((PageHeader)page)->pd_upper;
    Offset pdSpecial = ((PageHeader)page)->pd_special;

    const char *relName = (rel ? RelationGetRelationName(rel) : "Unknown During Redo");
    if ((unsigned int)(pdLower) < SizeOfPageHeaderData || pdLower > pdUpper || pdUpper > pdSpecial ||
        pdSpecial > BLCKSZ || (unsigned int)(pdSpecial) != MAXALIGN(pdSpecial))
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("corrupted page pointers: lower = %d, upper = %d, special = %d. rel \"%s\", blkno %u",
                       pdLower, pdUpper, pdSpecial, relName, blkno)));
    
    UBTPCRPageOpaque opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
    int maxOff = UBTPCRPageGetMaxOffsetNumber(page);
    int nstorage = 0;
    Size totallen = 0;
    itemIdSortData itemidbase[MaxIndexTuplesPerPage];
    itemIdSort itemidptr = itemidbase;
    for (int i = FirstOffsetNumber; i <= maxOff; i++) {
        ItemId lp = UBTreePCRGetRowPtr(page, i);
        /* Include active and frozen tuples */
        // ItemIdHasStorage
        if (!ItemIdIsDead(lp)) {
            nstorage++;
            itemidptr->offsetindex = nstorage;
            itemidptr->itemoff = ItemIdGetOffset(lp);
            itemidptr->olditemid = *lp;
            if (itemidptr->itemoff < (int)pdUpper || itemidptr->itemoff >= (int)pdSpecial)
                ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                        errmsg("corrupted item pointer: %d. rel \"%s\", blkno %u",
                               itemidptr->itemoff, relName, blkno)));
            itemidptr->alignedlen = MAXALIGN(ItemIdGetLength(lp));
            totallen += itemidptr->alignedlen;
            itemidptr++;
        }
    }
    if (totallen > (Size)(pdSpecial - pdLower))
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("corrupted item lengths: total %u, available space %d. rel \"%s\", blkno %u",
                       (unsigned int)totallen, pdSpecial - pdLower, relName, blkno)));

    /* sort itemIdSortData array into decreasing itemoff order */
    qsort((char*)itemidbase, nstorage, sizeof(itemIdSortData), UBTreePCRItemOffCompare);

    /* compactify page and install new itemids */
    Offset upper = pdSpecial;
    itemidptr = itemidbase;
    for (int i = 0; i < nstorage; i++, itemidptr++) {
        ItemId lp = UBTreePCRGetRowPtr(page, itemidptr->offsetindex);
        upper -= itemidptr->alignedlen;
        *lp = itemidptr->olditemid;
        lp->lp_off = upper;
        if (upper != itemidptr->itemoff) {
            errno_t rc = memmove_s((char *)page + upper, itemidptr->alignedlen, (char *)page + itemidptr->itemoff,
                                itemidptr->alignedlen);
            securec_check(rc, "\0", "\0");
        }
    }

    /* Mark the page as not containing any LP_DEAD items, which means vacuum is not needed */
    opaque->btpo_flags &= ~BTP_HAS_GARBAGE;

    ((PageHeader)page)->pd_lower = GetPageHeaderSize(page) + UBTreePCRTdSlotSize(UBTreePageGetTDSlotCount(page)) +
        nstorage * sizeof(ItemIdData);
    ((PageHeader)page)->pd_upper = upper;
}

int UBTreePCRItemOffCompare(const void* itemIdp1, const void* itemIdp2)
{
    /* Sort in decreasing itemoff order */
    return ((itemIdSort)itemIdp2)->itemoff - ((itemIdSort)itemIdp1)->itemoff;
}

/*
 * UBTreePCRPageDel() -- Delete a page from the b-tree, if legal to do so.
 *
 * This action unlinks the page from the b-tree structure, removing all
 * pointers leading to it --- but not touching its own left and right links.
 * The page cannot be physically reclaimed right away, since other processes
 * may currently be trying to follow links leading to the page; they have to
 * be allowed to use its right-link to recover.  See nbtree/README.
 *
 * On entry, the target buffer must be pinned and locked (either read or write
 * lock is OK).  This lock and pin will be dropped before exiting.
 *
 * Returns the number of pages successfully deleted (zero if page cannot
 * be deleted now; could be more than one if parent or sibling pages were
 * deleted too).
 *
 * NOTE: this leaks memory.  Rather than trying to clean up everything
 * carefully, it's better to run it in a temp context that can be reset
 * frequently.
 */
int UBTreePCRPageDel(Relation rel, Buffer buf, BTStack del_blknos)
{
    int ndeleted = 0;
    BlockNumber rightsib;
    bool rightsib_empty = false;
    Page page;
    UBTPCRPageOpaque opaque;

    /*
     * "stack" is a search stack leading (approximately) to the target page.
     * It is initially NULL, but when iterating, we keep it to avoid
     * duplicated search effort.
     *
     * Also, when "stack" is not NULL, we have already checked that the
     * current page is not the right half of an incomplete split, i.e. the
     * left sibling does not have its INCOMPLETE_SPLIT flag set.
     */
    BTStack stack = NULL;

    for (;;) {
        page = BufferGetPage(buf);
        opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
        /*
         * Internal pages are never deleted directly, only as part of deleting
         * the whole branch all the way down to leaf level.
         */
        if (!P_ISLEAF(opaque)) {
            /*
             * Pre-9.4 page deletion only marked internal pages as half-dead,
             * but now we only use that flag on leaf pages. The old algorithm
             * was never supposed to leave half-dead pages in the tree, it was
             * just a transient state, but it was nevertheless possible in
             * error scenarios. We don't know how to deal with them here. They
             * are harmless as far as searches are considered, but inserts
             * into the deleted keyspace could add out-of-order downlinks in
             * the upper levels. Log a notice, hopefully the admin will notice
             * and reindex.
             */
            if (P_ISHALFDEAD(opaque)) {
                ereport(LOG, (errcode(ERRCODE_INDEX_CORRUPTED),
                    errmsg("index \"%s\" contains a half-dead internal page", RelationGetRelationName(rel)),
                    errhint("This can be caused by an interrupted VACUUM in version 9.3 or older, before upgrade. "
                            "Please REINDEX it.")));
            }
            _bt_relbuf(rel, buf);
            return ndeleted;
        }

        /*
         * We can never delete rightmost pages nor root pages.  While at it,
         * check that page is not already deleted and is empty.
         *
         * To keep the algorithm simple, we also never delete an incompletely
         * split page (they should be rare enough that this doesn't make any
         * meaningful difference to disk usage):
         *
         * The INCOMPLETE_SPLIT flag on the page tells us if the page is the
         * left half of an incomplete split, but ensuring that it's not the
         * right half is more complicated.  For that, we have to check that
         * the left sibling doesn't have its INCOMPLETE_SPLIT flag set.  On
         * the first iteration, we temporarily release the lock on the current
         * page, and check the left sibling and also construct a search stack
         * to.  On subsequent iterations, we know we stepped right from a page
         * that passed these tests, so it's OK.
         */
        if (P_RIGHTMOST(opaque) || P_ISROOT(opaque) || P_ISDELETED(opaque) ||
            P_FIRSTDATAKEY(opaque) <= UBTPCRPageGetMaxOffsetNumber(page) || P_INCOMPLETE_SPLIT(opaque)) {
            /* Should never fail to delete a half-dead page */
            Assert(!P_ISHALFDEAD(opaque));

            _bt_relbuf(rel, buf);
            return ndeleted;
        }

        /*
         * First, remove downlink pointing to the page (or a parent of the
         * page, if we are going to delete a taller branch), and mark the page
         * as half-dead.
         */
        if (!P_ISHALFDEAD(opaque)) {
            /*
             * We need an approximate pointer to the page's parent page.  We
             * use a variant of the standard search mechanism to search for
             * the page's high key; this will give us a link to either the
             * current parent or someplace to its left (if there are multiple
             * equal high keys, which is possible with !heapkeyspace indexes).
             *
             * Also check if this is the right-half of an incomplete split
             * (see comment above).
             */
            if (!stack) {
                BTScanInsert itup_key;
                ItemId itemid;
                IndexTuple targetkey;
                Buffer lbuf;
                BlockNumber leftsib;

                itemid = UBTreePCRGetRowPtr(page, P_HIKEY);
                targetkey = CopyIndexTuple((IndexTuple)UBTreePCRGetIndexTupleByItemId(page, itemid));

                leftsib = opaque->btpo_prev;

                /*
                 * To avoid deadlocks, we'd better drop the leaf page lock
                 * before going further.
                 */
                LockBuffer(buf, BUFFER_LOCK_UNLOCK);

                /*
                 * Fetch the left sibling, to check that it's not marked with
                 * INCOMPLETE_SPLIT flag.  That would mean that the page
                 * to-be-deleted doesn't have a downlink, and the page
                 * deletion algorithm isn't prepared to handle that.
                 */
                if (!P_LEFTMOST(opaque)) {
                    UBTPCRPageOpaque lopaque;
                    Page lpage;

                    lbuf = _bt_getbuf(rel, leftsib, BT_READ);
                    lpage = BufferGetPage(lbuf);
                    lopaque = (UBTPCRPageOpaque)PageGetSpecialPointer(lpage);
                    /*
                     * If the left sibling is split again by another backend,
                     * after we released the lock, we know that the first
                     * split must have finished, because we don't allow an
                     * incompletely-split page to be split again.  So we don't
                     * need to walk right here.
                     */
                    if (lopaque->btpo_next == BufferGetBlockNumber(buf) && P_INCOMPLETE_SPLIT(lopaque)) {
                        ReleaseBuffer(buf);
                        _bt_relbuf(rel, lbuf);
                        return ndeleted;
                    }
                    _bt_relbuf(rel, lbuf);
                }

                /* we need an insertion scan key for the search, so build one */
                itup_key = UBTreeMakeScanKey(rel, targetkey);
                /* find the leftmost leaf page with matching pivot/high key */
                itup_key->pivotsearch = true;
                stack = UBTreePCRSearch(rel, itup_key, &lbuf, BT_READ, true);
                /* don't need a lock or second pin on the page */
                _bt_relbuf(rel, lbuf);

                /*
                 * Re-lock the leaf page, and start over, to re-check that the
                 * page can still be deleted.
                 */
                LockBuffer(buf, BT_WRITE);
                continue;
            }
            if (!UBTreePCRMarkPageHalfDead(rel, buf, stack)) {
                _bt_relbuf(rel, buf);
                return ndeleted;
            }
        }

        /*
         * Then unlink it from its siblings.  Each call to
         * _bt_unlink_halfdead_page unlinks the topmost page from the branch,
         * making it shallower.  Iterate until the leaf page is gone.
         */
        rightsib_empty = false;
        while (P_ISHALFDEAD(opaque)) {
            /* will check for interrupts, once lock is released */
            if (!UBTreePCRUnlinkHalfDeadPage(rel, buf, &rightsib_empty, del_blknos)) {
                /* _bt_unlink_halfdead_page already released buffer */
                return ndeleted;
            }
            if (del_blknos != NULL) {
                del_blknos = del_blknos->bts_parent;
            }
            ndeleted++;
        }

        rightsib = opaque->btpo_next;

        _bt_relbuf(rel, buf);

        /*
         * Check here, as calling loops will have locks held, preventing
         * interrupts from being processed.
         */
        CHECK_FOR_INTERRUPTS();

        // to do or not:gauss包含vacuum flag清楚逻辑，我们不需要

        /*
         * The page has now been deleted. If its right sibling is completely
         * empty, it's possible that the reason we haven't deleted it earlier
         * is that it was the rightmost child of the parent. Now that we
         * removed the downlink for this page, the right sibling might now be
         * the only child of the parent, and could be removed. It would be
         * picked up by the next vacuum anyway, but might as well try to
         * remove it now, so loop back to process the right sibling.
         */
        if (!rightsib_empty) {
            break;
        }

        buf = _bt_getbuf(rel, rightsib, BT_WRITE);
    }

    return ndeleted;
}

/*
 *  UBTreePCRSatisfyPruneCondition()
 *  -- Traverse all tds to judge whether the page need to be pruned and reflesh all symbols.
 */
 bool UBTreePCRSatisfyPruneCondition(Relation rel, Buffer buf, TransactionId globalFrozenXid,
    bool pruneDelete)
{
    bool needPrune = false;
    Page page = BufferGetPage(buf);
    int frozenTdCount = 0;
    int tdCount = UBTreePageGetTDSlotCount(page);
    uint8 ncompletedXactSlots = 0;
    uint8 *completedXactSlots = NULL;
    TransactionId globalRecycleXid = pg_atomic_read_u64(&g_instance.undo_cxt.globalRecycleXid);
    completedXactSlots = (uint8 *)palloc0(tdCount *sizeof(uint8));
    for (int slot = 0; slot < tdCount; slot++) {
        UBTreeTD curTd = (UBTreeTD)UBTreePCRGetTD(page, slot + 1);
        /* td is unused or frozen */
        if (UBTreePCRTDIsFrozen(curTd) || TransactionIdPrecedes(curTd->xactid, globalRecycleXid)) {
            if (UBTreePCRTDIsDelete(curTd)) {
                needPrune = true;
            }
            /* mark td frozen symbol */
            UBTreePCRTDSetStatus(curTd, TD_FROZEN);
            frozenTdCount++;
            continue;
        }
        /* find consecutive frozen tds */
        frozenTdCount = 0;
        Assert(TransactionIdIsValid(curTd->xactid));

        /*
         * if committed, set is_ative = 0, is_commited = 1,
         * if aborted, set is_ative = 0, is_commited = 0, page need to be pruned
         * if in progress, assert is_active == 1, is_commited == 0;
         */
        if (UBTreePCRTDIsCommited(curTd)) {
            Assert(!UBTreePCRTDIsActive(curTd));
        } else if (TransactionIdPrecedes(curTd->xactid, globalFrozenXid)) {
            UBTreePCRTDClearStatus(curTd, TD_ACTIVE);
            UBTreePCRTDSetStatus(curTd, TD_COMMITED);
            if (UBTreePCRTDIsDelete(curTd)) {
                needPrune = true;
            }
            completedXactSlots[ncompletedXactSlots++] = slot;
        } else if (TransactionIdDidCommit(curTd->xactid)) {
            UBTreePCRTDClearStatus(curTd, TD_ACTIVE);
            UBTreePCRTDSetStatus(curTd, TD_COMMITED);
            completedXactSlots[ncompletedXactSlots++] = slot;
        } else if (!TransactionIdIsInProgress(curTd->xactid)) {
            UBTreePCRTDClearStatus(curTd, TD_ACTIVE);
            if (TransactionIdDidCommit(curTd->xactid)) {
                UBTreePCRTDSetStatus(curTd, TD_COMMITED);
                completedXactSlots[ncompletedXactSlots++] = slot;
            } else {
                /* abort situation, needPrune is true */
                needPrune = true;
            }
        } else {
            Assert(UBTreePCRTDIsActive(curTd) && !UBTreePCRTDIsCommited(curTd));
        }

        /* remove commited xid which deleted some tuple */
        if (pruneDelete && UBTreePCRTDIsDelete(curTd) && UBTreePCRTDIsCommited(curTd)) {
            needPrune = true;
        }
    }

    if (ncompletedXactSlots > 0) {
        UBTPCRPageOpaque opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);

        START_CRIT_SECTION();

        UBTreeFreezeOrInvalidIndexTuples(buf, ncompletedXactSlots, completedXactSlots, false);

        TransactionId latestCommittedXid = opaque->last_commit_xid;
        for (uint8 i = 0; i < ncompletedXactSlots; i++) {
            UBTreeTD curTd = (UBTreeTD)UBTreePCRGetTD(page, completedXactSlots[i] + 1);
            if (!TransactionIdIsValid(latestCommittedXid) ||
                TransactionIdFollows(curTd->xactid, latestCommittedXid)) {
                    latestCommittedXid = curTd->xactid;
            }
            Assert(TransactionIdIsValid(curTd->xactid));
        }
        /* update latest commited xid for reuse */
        opaque->last_commit_xid = latestCommittedXid;
        MarkBufferDirty(buf);

        /* xlog stuff */
        if (RelationNeedsWAL(rel)) {
            XLogRecPtr recptr;
            XLogBeginInsert();
            XLogRegisterData((char*)&ncompletedXactSlots, sizeof(uint8));
            XLogRegisterData((char*)completedXactSlots, ncompletedXactSlots * sizeof(uint8));
            XLogRegisterBuffer(0, buf, REGBUF_STANDARD);
            recptr = XLogInsert(RM_UBTREE3_ID, XLOG_UBTREE3_REUSE_TD_SLOT);

            PageSetLSN(page, recptr);

        }
        END_CRIT_SECTION();
    }

    if (completedXactSlots != NULL) {
        pfree(completedXactSlots);
    }
    if(tdCount > UBTREE_DEFAULT_TD_COUNT && frozenTdCount > 0) {
        needPrune = true;
    }
    return needPrune;
}

void UBTreePCRTryRecycleEmptyPage(Relation rel)
{
    bool firstTrySucceed = UBTreePCRTryRecycleEmptyPageInternal(rel);
    if (firstTrySucceed) {
        /* try to recycle the second page */
        UBTreePCRTryRecycleEmptyPageInternal(rel);
    }
}

bool UBTreePCRTryRecycleEmptyPageInternal(Relation rel)
{
    UBTRecycleQueueAddress addr;
    Buffer buf = UBTreePCRGetAvailablePage(rel, RECYCLE_EMPTY_FORK, &addr, NULL);
    if (!BufferIsValid(buf)) {
        return false; /* no available page to recycle */
    }
    Page page = BufferGetPage(buf);
    UBTPCRPageOpaque opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
    if (P_ISDELETED(opaque)) {
        /* deleted by other routine earlier, skip */
        _bt_relbuf(rel, buf);
        UBTreeRecycleQueueDiscardPage(rel, addr);
        return true;
    }

    BTStack dummy_del_blknos = (BTStack) palloc0(sizeof(BTStackData));
    if (UBTreePCRPrunePageOpt(rel, buf, true, dummy_del_blknos, true)) {
        BTStack cur_del_blkno = dummy_del_blknos->bts_parent;
        while (cur_del_blkno){
            /* successfully deleted, move to freed page queue */
            UBTreeRecycleQueueAddPage(rel, RECYCLE_FREED_FORK, cur_del_blkno->bts_blkno, ReadNewTransactionId());
            cur_del_blkno = cur_del_blkno->bts_parent;
        }
    }
    _bt_freestack(dummy_del_blknos);
    /* whether the page can be deleted or not, discard from the queue */
    UBTreeRecycleQueueDiscardPage(rel, addr);
    return true;
}

IndexTuple UBTreeFetchTupleFromUndoRecord(UndoRecord* urec)
{
    Assert(urec->Rawdata()->data != NULL);
    return (IndexTuple)((char*)(urec->Rawdata()->data));
}

bool UBTreeFetchLatestChangeFromUndo(IndexTuple itup, UndoRecord* urec,
    UBTreeLatestChangeInfo* uInfo, UndoPersistence upersistence)
{
    for(;;) {
        UndoTraversalState state = FetchUndoRecord(urec, NULL, InvalidBlockNumber, InvalidOffsetNumber,
                    InvalidTransactionId, false, NULL);
        if (state != UNDO_TRAVERSAL_COMPLETE) {
            return false;
        }
        if (UBTreeItupEquals(itup, UBTreeFetchTupleFromUndoRecord(urec))) {
            break;
        }
        urec->Reset2Blkprev();
    }
    /* at now, we get the latest change from undo. */
    uInfo->undoType = urec->Utype();
    uInfo->xid = urec->Xid();
    uInfo->cid = urec->Cid();
    uInfo->prevXid = urec->OldXactId();
    return true;
}

Buffer UBTreePCRGetAvailablePage(Relation rel, UBTRecycleForkNumber forkNumber, UBTRecycleQueueAddress *addr,
    UBTreeGetNewPageStats *stats)
{
    TimestampTz startTime = 0;
    TransactionId oldestXmin = pg_atomic_read_u64(&g_instance.undo_cxt.globalRecycleXid);
    Buffer queueBuf = RecycleQueueGetEndpointPage(rel, forkNumber, true, BT_READ);
    Buffer indexBuf = InvalidBuffer;
    bool continueScan = false;
    for (BlockNumber bufCount = 0; bufCount < URQ_GET_PAGE_MAX_RETRY_TIMES; bufCount++) {
        if (stats) {
            startTime = GetCurrentTimestamp();
        }
        indexBuf = GetAvailablePageOnPage(rel, forkNumber, queueBuf, oldestXmin, addr, &continueScan, stats);
        UBTreeRecordGetNewPageCost(stats, URQ_GET_PAGE, startTime);
        if (!continueScan) {
            break;
        }
        queueBuf = StepNextPage(rel, queueBuf);
    }
    LockBuffer(queueBuf, BUFFER_LOCK_UNLOCK);

    if (indexBuf != InvalidBuffer) {
        return indexBuf;
    }

    /* no useful page, but we will check non-tracked page if we are finding free pages */
    ReleaseBuffer(queueBuf);
    addr->queueBuf = InvalidBuffer;

    if (forkNumber == RECYCLE_EMPTY_FORK) {
        return InvalidBuffer;
    }

    if (stats) {
        stats->checkNonTrackedPagesCount++;
    }

    /* no available page found, but we can check new created pages */
    BlockNumber nblocks = RelationGetNumberOfBlocks(rel);
    bool metaChanged = false;

    const BlockNumber metaBlockNumber = forkNumber;
    Buffer metaBuf = ReadRecycleQueueBuffer(rel, metaBlockNumber);
    LockBuffer(metaBuf, BT_READ);
    UBTRecycleMeta metaData = (UBTRecycleMeta)PageGetContents(BufferGetPage(metaBuf));
    for (BlockNumber curBlkno = metaData->nblocksUpper; curBlkno < nblocks; curBlkno++) {
        if (t_thrd.int_cxt.QueryCancelPending || t_thrd.int_cxt.ProcDiePending) {
            ereport(ERROR, (errmsg("Received cancel interrupt while getting available page.")));
        }
        if (metaData->nblocksUpper > curBlkno) {
            continue;
        }
        if (metaData->nblocksUpper <= curBlkno) {
            metaData->nblocksUpper = curBlkno + 1; /* update nblocks in meta */
            metaChanged = true;
        }
        indexBuf = ReadBuffer(rel, curBlkno);
        if (ConditionalLockBuffer(indexBuf)) {
            if (PageIsNew(BufferGetPage(indexBuf))) {
                break;
            }
            LockBuffer(indexBuf, BUFFER_LOCK_UNLOCK);
        }
        ReleaseBuffer(indexBuf);
        indexBuf = InvalidBuffer;
    }

    if (metaChanged) {
        MarkBufferDirtyHint(metaBuf, false);
    }
    UnlockReleaseBuffer(metaBuf);

    addr->queueBuf = InvalidBuffer; /* it's not allocated from any queue page */
    return indexBuf;
}

/*
 * UBTreePCRIsPageHalfDead() -- Returns true, if the given block has the half-dead flag set.
 */
bool UBTreePCRIsPageHalfDead(Relation rel, BlockNumber blk)
{
    Buffer buf;
    Page page;
    UBTPCRPageOpaque opaque;
    bool result;

    buf = _bt_getbuf(rel, blk, BT_READ);
    page = BufferGetPage(buf);
    opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);

    result = P_ISHALFDEAD(opaque);
    _bt_relbuf(rel, buf);

    return result;
}

bool UBTreePCRLockBranchParent(Relation rel, BlockNumber child, BTStack stack, Buffer *topparent,
    OffsetNumber *topoff, BlockNumber *target, BlockNumber *rightsib)
{
    BlockNumber parent;
    OffsetNumber poffset;
    OffsetNumber maxoff;
    Buffer pbuf;
    Page page;
    UBTPCRPageOpaque opaque;
    BlockNumber leftsib;

    /*
     * Locate the downlink of "child" in the parent, updating the stack entry
     * if needed.  This is how !heapkeyspace indexes deal with having
     * non-unique high keys in leaf level pages.  Even heapkeyspace indexes
     * can have a stale stack due to insertions into the parent.
     */
    stack->bts_btentry = *target;
    pbuf = UBTreePCRGetStackBuf(rel, stack);
    if (pbuf == InvalidBuffer) {
        elog(ERROR, "failed to re-find parent key in index \"%s\" for deletion target page %u",
             RelationGetRelationName(rel), child);
    }
    parent = stack->bts_blkno;
    poffset = stack->bts_offset;

    page = BufferGetPage(pbuf);
    opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
    maxoff = UBTPCRPageGetMaxOffsetNumber(page);
    /*
     * If the target is the rightmost child of its parent, then we can't
     * delete, unless it's also the only child.
     */
    if (poffset >= maxoff) {
        /* It's rightmost child... */
        if (poffset == P_FIRSTDATAKEY(opaque)) {
            /*
             * It's only child, so safe if parent would itself be removable.
             * We have to check the parent itself, and then recurse to test
             * the conditions at the parent's parent.
             */
            if (P_RIGHTMOST(opaque) || P_ISROOT(opaque) || P_INCOMPLETE_SPLIT(opaque)) {
                _bt_relbuf(rel, pbuf);
                return false;
            }

            *target = parent;
            *rightsib = opaque->btpo_next;
            leftsib = opaque->btpo_prev;

            _bt_relbuf(rel, pbuf);

            /*
             * Like in _bt_pagedel, check that the left sibling is not marked
             * with INCOMPLETE_SPLIT flag.  That would mean that there is no
             * downlink to the page to be deleted, and the page deletion
             * algorithm isn't prepared to handle that.
             */
            if (leftsib != P_NONE) {
                Buffer lbuf;
                Page lpage;
                UBTPCRPageOpaque lopaque;

                lbuf = _bt_getbuf(rel, leftsib, BT_READ);
                lpage = BufferGetPage(lbuf);
                lopaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
                /*
                 * If the left sibling was concurrently split, so that its
                 * next-pointer doesn't point to the current page anymore, the
                 * split that created the current page must be completed. (We
                 * don't allow splitting an incompletely split page again
                 * until the previous split has been completed)
                 */
                if (lopaque->btpo_next == parent && P_INCOMPLETE_SPLIT(lopaque)) {
                    _bt_relbuf(rel, lbuf);
                    return false;
                }
                _bt_relbuf(rel, lbuf);
            }

            return UBTreePCRLockBranchParent(rel, parent, stack->bts_parent, topparent, topoff, target, rightsib);
        } else {
            /* Unsafe to delete */
            _bt_relbuf(rel, pbuf);
            return false;
        }
    } else {
        /* Not rightmost child, so safe to delete */
        *topparent = pbuf;
        *topoff = poffset;
        return true;
    }
}


/*
 * First stage of page deletion.  Remove the downlink to the top of the
 * branch being deleted, and mark the leaf page as half-dead.
 */
bool UBTreePCRMarkPageHalfDead(Relation rel, Buffer leafbuf, BTStack stack)
{
    BlockNumber leafblkno;
    BlockNumber leafrightsib;
    BlockNumber target;
    BlockNumber rightsib;
    ItemId itemid;
    Page page;
    UBTPCRPageOpaque opaque;
    Buffer topparent;
    OffsetNumber topoff;
    OffsetNumber nextoffset;
    IndexTuple itup;
    IndexTupleData trunctuple;
    errno_t rc;

    WHITEBOX_TEST_STUB("UBTreeMarkPageHalfDead", WhiteboxDefaultErrorEmit);

    page = BufferGetPage(leafbuf);
    opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);

    Assert(!P_RIGHTMOST(opaque) && !P_ISROOT(opaque) && !P_ISDELETED(opaque) && !P_ISHALFDEAD(opaque) &&
           P_ISLEAF(opaque) && P_FIRSTDATAKEY(opaque) > UBTPCRPageGetMaxOffsetNumber(page));

    /*
     * Save info about the leaf page.
     */
    leafblkno = BufferGetBlockNumber(leafbuf);
    leafrightsib = opaque->btpo_next;

    /*
     * Before attempting to lock the parent page, check that the right sibling
     * is not in half-dead state.  A half-dead right sibling would have no
     * downlink in the parent, which would be highly confusing later when we
     * delete the downlink that follows the current page's downlink. (I
     * believe the deletion would work correctly, but it would fail the
     * cross-check we make that the following downlink points to the right
     * sibling of the delete page.)
     */
    if (UBTreePCRIsPageHalfDead(rel, leafrightsib)) {
        elog(DEBUG1, "could not delete page %u because its right sibling %u is half-dead", leafblkno, leafrightsib);
        return false;
    }

    /*
     * We cannot delete a page that is the rightmost child of its immediate
     * parent, unless it is the only child --- in which case the parent has to
     * be deleted too, and the same condition applies recursively to it. We
     * have to check this condition all the way up before trying to delete,
     * and lock the final parent of the to-be-deleted subtree.
     *
     * However, we won't need to repeat the above _bt_is_page_halfdead() check
     * for parent/ancestor pages because of the rightmost restriction. The
     * leaf check will apply to a right "cousin" leaf page rather than a
     * simple right sibling leaf page in cases where we actually go on to
     * perform internal page deletion. The right cousin leaf page is
     * representative of the left edge of the subtree to the right of the
     * to-be-deleted subtree as a whole.  (Besides, internal pages are never
     * marked half-dead, so it isn't even possible to directly assess if an
     * internal page is part of some other to-be-deleted subtree.)
     */
    rightsib = leafrightsib;
    target = leafblkno;
    if (!UBTreePCRLockBranchParent(rel, leafblkno, stack, &topparent, &topoff, &target, &rightsib)) {
        return false;
    }

    /*
     * Check that the parent-page index items we're about to delete/overwrite
     * contain what we expect.  This can fail if the index has become corrupt
     * for some reason.  We want to throw any error before entering the
     * critical section --- otherwise it'd be a PANIC.
     *
     * The test on the target item is just an Assert because
     * _bt_lock_branch_parent should have guaranteed it has the expected
     * contents.  The test on the next-child downlink is known to sometimes
     * fail in the field, though.
     */
    page = BufferGetPage(topparent);
    opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);

#ifdef USE_ASSERT_CHECKING
    itemid = UBTreePCRGetRowPtr(page, topoff);
    itup = (IndexTuple)UBTreePCRGetIndexTupleByItemId(page, itemid);
    Assert(UBTreeTupleGetDownLink(itup) == target);
#endif

    nextoffset = OffsetNumberNext(topoff);
    itemid = UBTreePCRGetRowPtr(page, nextoffset);
    itup = (IndexTuple)UBTreePCRGetIndexTupleByItemId(page, itemid);
    if (UBTreeTupleGetDownLink(itup) != rightsib) {
        OffsetNumber topparentblkno = BufferGetBlockNumber(topparent);
        _bt_relbuf(rel, topparent);
        Buffer rbuf = _bt_getbuf(rel, rightsib, BT_READ);
        Page rpage = BufferGetPage(rbuf);
        UBTPCRPageOpaque ropaque = (UBTPCRPageOpaque)PageGetSpecialPointer(rpage);
        if (P_ISHALFDEAD(ropaque)) {
            /* right page already deleted by concurrent worker, do not continue */
            _bt_relbuf(rel, rbuf);
            return false;
        }
        elog(ERROR, "right sibling %u of block %u is not next child %u of block %u in index \"%s\"",
             rightsib, target, UBTreeTupleGetDownLink(itup) != rightsib,
             topparentblkno, RelationGetRelationName(rel));
    }

    /*
     * Any insert which would have gone on the leaf block will now go to its
     * right sibling.
     */
    PredicateLockPageCombine(rel, leafblkno, leafrightsib);

    /* No ereport(ERROR) until changes are logged */
    START_CRIT_SECTION();

    /*
     * Update parent.  The normal case is a tad tricky because we want to
     * delete the target's downlink and the *following* key.  Easiest way is
     * to copy the right sibling's downlink over the target downlink, and then
     * delete the following item.
     */
    page = BufferGetPage(topparent);
    opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);

    itemid = UBTreePCRGetRowPtr(page, topoff);
    itup = (IndexTuple)UBTreePCRGetIndexTupleByItemId(page, itemid);
    UBTreeTupleSetDownLink(itup, rightsib);

    nextoffset = OffsetNumberNext(topoff);
    UBTreePCRPageIndexTupleDelete(page, nextoffset);

    /*
     * Mark the leaf page as half-dead, and stamp it with a pointer to the
     * highest internal page in the branch we're deleting.  We use the tid of
     * the high key to store it.
     */
    page = BufferGetPage(leafbuf);
    opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
    opaque->btpo_flags |= BTP_HALF_DEAD;

    UBTreePCRPageIndexTupleDelete(page, P_HIKEY);
    Assert(UBTPCRPageGetMaxOffsetNumber(page) == 0);
    rc = memset_s(&trunctuple, sizeof(IndexTupleData), 0, sizeof(IndexTupleData));
    securec_check(rc, "\0", "\0");
    trunctuple.t_info = sizeof(IndexTupleData);
    if (target != leafblkno) {
        UBTreeTupleSetTopParent(&trunctuple, target);
    } else {
        UBTreeTupleSetTopParent(&trunctuple, InvalidBlockNumber);
    }

    if (UBTPCRPageAddItem(page, (Item)&trunctuple, sizeof(IndexTupleData), P_HIKEY, false) == InvalidOffsetNumber) {
        elog(ERROR, "could not add dummy high key to half-dead page");
    }

    /* Must mark buffers dirty before XLogInsert */
    MarkBufferDirty(topparent);
    MarkBufferDirty(leafbuf);

    /* XLOG stuff */
    if (RelationNeedsWAL(rel)) {
        xl_btree_mark_page_halfdead xlrec;
        XLogRecPtr recptr;

        xlrec.poffset = topoff;
        xlrec.leafblk = leafblkno;
        if (target != leafblkno) {
            xlrec.topparent = target;
        } else {
            xlrec.topparent = InvalidBlockNumber;
        }
        XLogBeginInsert();
        XLogRegisterBuffer(0, leafbuf, REGBUF_WILL_INIT);
        XLogRegisterBuffer(1, topparent, REGBUF_STANDARD);

        page = BufferGetPage(leafbuf);
        opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
        xlrec.leftblk = opaque->btpo_prev;
        xlrec.rightblk = opaque->btpo_next;

        XLogRegisterData((char *)&xlrec, SizeOfBtreeMarkPageHalfDead);

        recptr = XLogInsert(RM_UBTREE4_ID, XLOG_UBTREE4_MARK_PAGE_HALFDEAD);

        page = BufferGetPage(topparent);
        PageSetLSN(page, recptr);
        page = BufferGetPage(leafbuf);
        PageSetLSN(page, recptr);
    }

    END_CRIT_SECTION();

    _bt_relbuf(rel, topparent);
    return true;
}

void UBTreePCRDoReserveDeletion(Page page)
{
    UBTPCRPageOpaque opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);

    bool isVacuumProcess = ((t_thrd.pgxact->vacuumFlags & PROC_IN_VACUUM) || IsAutoVacuumWorkerProcess());
    if (isVacuumProcess && !TransactionIdIsValid(GetTopTransactionIdIfAny())) {
        ((UBTPCRPageOpaque)opaque)->btpo_flags |= BTP_VACUUM_DELETING;
        opaque->xact = ReadNewTransactionId();
    } else {
        opaque->xact = GetTopTransactionId();
    }
}

bool UBTreePCRReserveForDeletion(Relation rel, Buffer buf)
{
    Page page = BufferGetPage(buf);
    UBTPCRPageOpaque opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);

    const int MAX_RETRY_TIMES = 1000;
    const int WAIT_TIME = 500;
    int times = MAX_RETRY_TIMES;
    while (times--) {
        if (P_ISDELETED((UBTPCRPageOpaque)opaque)) {
            return false;
        }

        if (P_VACUUM_DELETING((UBTPCRPageOpaque)opaque)) {
            TransactionId oldestXmin = pg_atomic_read_u64(&g_instance.undo_cxt.globalRecycleXid);
            if (TransactionIdPrecedes(opaque->xact, oldestXmin)) {
                opaque->xact = InvalidTransactionId;
                ((UBTPCRPageOpaque)opaque)->btpo_flags &= ~BTP_VACUUM_DELETING;
            }
        }

        /* try to reserve for the deleteion */
        if (!TransactionIdIsValid(opaque->xact)) {
            UBTreePCRDoReserveDeletion(page);
            return true;
        }
        /* someone else is deleting, need wait for it */
        TransactionId previousXact = opaque->xact;
        if (TransactionIdIsCurrentTransactionId(previousXact)) {
            /* already reserved by self, continue */
            return true;
        }
        if (!TransactionIdIsInProgress(previousXact, NULL)) {
            /* previous worker abort, we can still reserve for deletion */
            UBTreePCRDoReserveDeletion(page);
            return true;
        }

        LockBuffer(buf, BUFFER_LOCK_UNLOCK);
        pg_usleep(WAIT_TIME); /* 0.5 ms */
        LockBuffer(buf, BT_WRITE);
    }
    return false;
}



/*
 * Unlink a page in a branch of half-dead pages from its siblings.
 *
 * If the leaf page still has a downlink pointing to it, unlinks the highest
 * parent in the to-be-deleted branch instead of the leaf page.  To get rid
 * of the whole branch, including the leaf page itself, iterate until the
 * leaf page is deleted.
 *
 * Returns 'false' if the page could not be unlinked (shouldn't happen).
 * If the (new) right sibling of the page is empty, *rightsib_empty is set
 * to true.
 *
 * Must hold pin and lock on leafbuf at entry (read or write doesn't matter).
 * On success exit, we'll be holding pin and write lock.  On failure exit,
 * we'll release both pin and lock before returning (we define it that way
 * to avoid having to reacquire a lock we already released).
 */
bool UBTreePCRUnlinkHalfDeadPage(Relation rel, Buffer leafbuf, bool *rightsib_empty, BTStack del_blknos)
{
    BlockNumber leafblkno = BufferGetBlockNumber(leafbuf);
    BlockNumber leafleftsib;
    BlockNumber leafrightsib;
    BlockNumber target;
    BlockNumber leftsib;
    BlockNumber rightsib;
    Buffer lbuf = InvalidBuffer;
    Buffer buf;
    Buffer rbuf;
    Buffer metabuf = InvalidBuffer;
    Page metapg = NULL;
    BTMetaPageData *metad = NULL;
    ItemId itemid;
    Page page;
    UBTPCRPageOpaque opaque;
    bool rightsib_is_rightmost = false;
    int targetlevel;
    IndexTuple leafhikey;
    BlockNumber nextchild;

    WHITEBOX_TEST_STUB("UBTreeUnlinkHalfDeadPage", WhiteboxDefaultErrorEmit);

    if (!UBTreePCRReserveForDeletion(rel, leafbuf)) {
        _bt_relbuf(rel, leafbuf);
        return false; /* concurrent worker finished this deletion already */
    }

    page = BufferGetPage(leafbuf);
    opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);

    Assert(P_ISLEAF(opaque) && P_ISHALFDEAD(opaque));

    /*
     * Remember some information about the leaf page.
     */
    itemid = UBTreePCRGetRowPtr(page, P_HIKEY);
    leafhikey = (IndexTuple)UBTreePCRGetIndexTupleByItemId(page, itemid);
    leafleftsib = opaque->btpo_prev;
    leafrightsib = opaque->btpo_next;

    LockBuffer(leafbuf, BUFFER_LOCK_UNLOCK);

    /*
     * Check here, as calling loops will have locks held, preventing
     * interrupts from being processed.
     */
    CHECK_FOR_INTERRUPTS();

    /*
     * If the leaf page still has a parent pointing to it (or a chain of
     * parents), we don't unlink the leaf page yet, but the topmost remaining
     * parent in the branch.  Set 'target' and 'buf' to reference the page
     * actually being unlinked.
     */
    target = UBTreeTupleGetTopParent(leafhikey);
    if (target != InvalidBlockNumber) {
        Assert(target != leafblkno);

        /* fetch the block number of the topmost parent's left sibling */
        buf = _bt_getbuf(rel, target, BT_READ);
        page = BufferGetPage(buf);
        opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
        leftsib = opaque->btpo_prev;
        targetlevel = opaque->btpo.level;

        /*
         * To avoid deadlocks, we'd better drop the target page lock before
         * going further.
         */
        LockBuffer(buf, BUFFER_LOCK_UNLOCK);
    } else {
        target = leafblkno;

        buf = leafbuf;
        leftsib = leafleftsib;
        targetlevel = 0;
    }

    /*
     * We have to lock the pages we need to modify in the standard order:
     * moving right, then up.  Else we will deadlock against other writers.
     *
     * So, first lock the leaf page, if it's not the target.  Then find and
     * write-lock the current left sibling of the target page.  The sibling
     * that was current a moment ago could have split, so we may have to move
     * right.  This search could fail if either the sibling or the target page
     * was deleted by someone else meanwhile; if so, give up.  (Right now,
     * that should never happen, since page deletion is only done in VACUUM
     * and there shouldn't be multiple VACUUMs concurrently on the same
     * table.) UPDATE: we surly have concurrent page deletion in UBTree.
     */
    if (target != leafblkno) {
        LockBuffer(leafbuf, BT_WRITE);
    }
    if (leftsib != P_NONE) {
        lbuf = _bt_getbuf(rel, leftsib, BT_WRITE);
        page = BufferGetPage(lbuf);
        opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
        while (P_ISDELETED(opaque) || opaque->btpo_next != target) {
            /*
             * Before we follow the link from the page that was the left
             * sibling mere moments ago, validate its right link.  This
             * reduces the opportunities for loop to fail to ever make any
             * progress in the presence of index corruption.
             *
             * Note: we rely on the assumption that there can only be one
             * vacuum process running at a time (against the same index).
             */
            bool leftSibValid = true;
            if (P_RIGHTMOST(opaque) || P_ISDELETED(opaque) || leftsib == opaque->btpo_next) {
                leftSibValid = false;
            }

            leftsib = opaque->btpo_next;
            _bt_relbuf(rel, lbuf);

            if (!leftSibValid) {
                if (target != leafblkno) {
                    /* we have only a pin on target, but pin+lock on leafbuf */
                    ReleaseBuffer(buf);
                    _bt_relbuf(rel, leafbuf);
                } else {
                    /* we have only a pin on leafbuf */
                    ReleaseBuffer(leafbuf);
                }

                ereport(LOG, (errcode(ERRCODE_INDEX_CORRUPTED),
                         errmsg_internal("valid left sibling for deletion target could not be located: "
                                         "left sibling %u of target %u with leafblkno %u in index \"%s\"",
                                         leftsib, target, leafblkno,
                                         RelationGetRelationName(rel))));
                return false;
            }

            CHECK_FOR_INTERRUPTS();

            /* step right one page */
            lbuf = _bt_getbuf(rel, leftsib, BT_WRITE);
            page = BufferGetPage(lbuf);
            opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
        }
    } else {
        lbuf = InvalidBuffer;
    }

    /*
     * Next write-lock the target page itself.  It should be okay to take just
     * a write lock not a superexclusive lock, since no scans would stop on an
     * empty page.
     */
    LockBuffer(buf, BT_WRITE);
    page = BufferGetPage(buf);
    opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
    /*
     * Check page is still empty etc, else abandon deletion.  This is just for
     * paranoia's sake; a half-dead page cannot resurrect because there can be
     * only one vacuum process running at a time.
     */
    if (P_RIGHTMOST(opaque) || P_ISROOT(opaque) || P_ISDELETED(opaque)) {
        if (BufferIsValid(lbuf)) {
            _bt_relbuf(rel, lbuf);
        }
        _bt_relbuf(rel, buf);
        return false;
    }
    if (opaque->btpo_prev != leftsib) {
        if (BufferIsValid(lbuf)) {
            _bt_relbuf(rel, lbuf);
        }
        _bt_relbuf(rel, buf);
        return false;
    }

    if (target == leafblkno) {
        if (P_FIRSTDATAKEY(opaque) <= UBTPCRPageGetMaxOffsetNumber(page) || !P_ISLEAF(opaque) || !P_ISHALFDEAD(opaque)) {
            if (BufferIsValid(lbuf)) {
                _bt_relbuf(rel, lbuf);
            }
            _bt_relbuf(rel, buf);
            return false;
        }
        nextchild = InvalidBlockNumber;
    } else {
        if (P_FIRSTDATAKEY(opaque) != UBTPCRPageGetMaxOffsetNumber(page) || P_ISLEAF(opaque)) {
            if (BufferIsValid(lbuf)) {
                _bt_relbuf(rel, lbuf);
            }
            _bt_relbuf(rel, buf);
            return false;
        }

        /* remember the next non-leaf child down in the branch. */
        itemid = UBTreePCRGetRowPtr(page, P_FIRSTDATAKEY(opaque));
        nextchild = UBTreeTupleGetDownLink((IndexTuple) PageGetItem(page, itemid));
        if (nextchild == leafblkno) {
            nextchild = InvalidBlockNumber;
        }
    }

    /*
     * And next write-lock the (current) right sibling.
     */
    rightsib = opaque->btpo_next;
    rbuf = _bt_getbuf(rel, rightsib, BT_WRITE);
    page = BufferGetPage(rbuf);
    opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
    if (opaque->btpo_prev != target) {
        elog(ERROR,
             "right sibling's left-link doesn't match: "
             "block %u links to %u instead of expected %u in index \"%s\"",
             rightsib, opaque->btpo_prev, target, RelationGetRelationName(rel));
    }
    rightsib_is_rightmost = P_RIGHTMOST(opaque);
    *rightsib_empty = (P_FIRSTDATAKEY(opaque) > UBTPCRPageGetMaxOffsetNumber(page));

    /*
     * If we are deleting the next-to-last page on the target's level, then
     * the rightsib is a candidate to become the new fast root. (In theory, it
     * might be possible to push the fast root even further down, but the odds
     * of doing so are slim, and the locking considerations daunting.)
     *
     * We don't support handling this in the case where the parent is becoming
     * half-dead, even though it theoretically could occur.
     *
     * We can safely acquire a lock on the metapage here --- see comments for
     * _bt_newroot().
     */
    if (leftsib == P_NONE && rightsib_is_rightmost) {
        page = BufferGetPage(rbuf);
        opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
        if (P_RIGHTMOST(opaque)) {
            /* rightsib will be the only one left on the level */
            metabuf = _bt_getbuf(rel, BTREE_METAPAGE, BT_WRITE);
            metapg = BufferGetPage(metabuf);
            metad = BTPageGetMeta(metapg);
            /*
             * The expected case here is btm_fastlevel == targetlevel+1; if
             * the fastlevel is <= targetlevel, something is wrong, and we
             * choose to overwrite it to fix it.
             */
            if (metad->btm_fastlevel > (uint32)targetlevel + 1) {
                /* no update wanted */
                _bt_relbuf(rel, metabuf);
                metabuf = InvalidBuffer;
            }
        }
    }

    /*
     * Here we begin doing the deletion.
     */

    /* No ereport(ERROR) until changes are logged */
    START_CRIT_SECTION();

    /*
     * Update siblings' side-links.  Note the target page's side-links will
     * continue to point to the siblings.  Asserts here are just rechecking
     * things we already verified above.
     */
    if (BufferIsValid(lbuf)) {
        page = BufferGetPage(lbuf);
        opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
        Assert(opaque->btpo_next == target);
        opaque->btpo_next = rightsib;
    }
    page = BufferGetPage(rbuf);
    opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
    Assert(opaque->btpo_prev == target);
    opaque->btpo_prev = leftsib;

    /*
     * If we deleted a parent of the targeted leaf page, instead of the leaf
     * itself, update the leaf to point to the next remaining child in the
     * branch.
     */
    if (target != leafblkno) {
        if (nextchild == leafblkno) {
            UBTreeTupleSetTopParent(leafhikey, InvalidBlockNumber);
        } else {
            UBTreeTupleSetTopParent(leafhikey, nextchild);
        }
    }

    /*
     * Mark the page itself deleted.  It can be recycled when all current
     * transactions are gone.  Storing GetTopTransactionId() would work, but
     * we're in VACUUM and would not otherwise have an XID.  Having already
     * updated links to the target, ReadNewTransactionId() suffices as an
     * upper bound.  Any scan having retained a now-stale link is advertising
     * in its PGXACT an xmin less than or equal to the value we read here.  It
     * will continue to do so, holding back RecentGlobalXmin, for the duration
     * of that scan.
     */
    page = BufferGetPage(buf);
    opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
    opaque->btpo_flags &= ~BTP_HALF_DEAD;
    opaque->btpo_flags |= BTP_DELETED;
    ((UBTPCRPageOpaque)opaque)->xact = ReadNewTransactionId();

    /* And update the metapage, if needed */
    if (BufferIsValid(metabuf)) {
        metad->btm_fastroot = rightsib;
        metad->btm_fastlevel = targetlevel;
        MarkBufferDirty(metabuf);
    }

    /* Must mark buffers dirty before XLogInsert */
    MarkBufferDirty(rbuf);
    MarkBufferDirty(buf);
    if (BufferIsValid(lbuf)) {
        MarkBufferDirty(lbuf);
    }
    if (target != leafblkno) {
        MarkBufferDirty(leafbuf);
    }

    /* XLOG stuff */
    if (RelationNeedsWAL(rel)) {
        xl_btree_unlink_page xlrec;
        xl_btree_metadata xlmeta;
        uint8 xlinfo;
        XLogRecPtr recptr;

        XLogBeginInsert();

        XLogRegisterBuffer(0, buf, REGBUF_WILL_INIT);
        if (BufferIsValid(lbuf))
            XLogRegisterBuffer(1, lbuf, REGBUF_STANDARD);
        XLogRegisterBuffer(2, rbuf, REGBUF_STANDARD);
        if (target != leafblkno)
            XLogRegisterBuffer(3, leafbuf, REGBUF_WILL_INIT);

        /* information on the unlinked block */
        xlrec.leftsib = leftsib;
        xlrec.rightsib = rightsib;
        xlrec.btpo_xact = opaque->btpo.xact_old;

        /* information needed to recreate the leaf block (if not the target) */
        xlrec.leafleftsib = leafleftsib;
        xlrec.leafrightsib = leafrightsib;
        xlrec.topparent = nextchild;

        XLogRegisterData((char *)&xlrec, SizeOfBtreeUnlinkPage);

        if (BufferIsValid(metabuf)) {
            XLogRegisterBuffer(4, metabuf, REGBUF_WILL_INIT | REGBUF_STANDARD);

            xlmeta.root = metad->btm_root;
            xlmeta.level = metad->btm_level;
            xlmeta.fastroot = metad->btm_fastroot;
            xlmeta.fastlevel = metad->btm_fastlevel;
            xlmeta.version = metad->btm_version;

            XLogRegisterBufData(4, (char *)&xlmeta, sizeof(xl_btree_metadata));
            xlinfo = XLOG_UBTREE4_UNLINK_PAGE_META;
        } else {
            xlinfo = XLOG_UBTREE4_UNLINK_PAGE;
        }

        recptr = XLogInsert(RM_UBTREE4_ID, xlinfo);

        if (BufferIsValid(metabuf)) {
            PageSetLSN(metapg, recptr);
        }
        page = BufferGetPage(rbuf);
        PageSetLSN(page, recptr);
        page = BufferGetPage(buf);
        PageSetLSN(page, recptr);
        if (BufferIsValid(lbuf)) {
            page = BufferGetPage(lbuf);
            PageSetLSN(page, recptr);
        }
        if (target != leafblkno) {
            page = BufferGetPage(leafbuf);
            PageSetLSN(page, recptr);
        }
    }

    END_CRIT_SECTION();

    /* release metapage */
    if (BufferIsValid(metabuf)) {
        _bt_relbuf(rel, metabuf);
    }

    /* release siblings */
    if (BufferIsValid(lbuf)) {
        _bt_relbuf(rel, lbuf);
    }
    _bt_relbuf(rel, rbuf);

    /*
     * Release the target, if it was not the leaf block.  The leaf is always
     * kept locked.
     */
    if (target != leafblkno) {
        _bt_relbuf(rel, buf);
    }
    if (del_blknos != NULL) {
        BTStack del_blkno = (BTStack)palloc0(sizeof(BTStackData));
        del_blkno->bts_blkno = target;
        del_blknos->bts_parent = del_blkno;
    }

    return true;
}