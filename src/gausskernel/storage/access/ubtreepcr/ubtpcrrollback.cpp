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
 * ubtpcrrollback.cpp
 *        Relaize the management of rollback for ubtree pcr page.
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/ubtreepcr/ubtpcrrollback.cpp
 *
 * --------------------------------------------------------------------------------------
 */

#include "access/ubtreepcr.h"
#include "access/xlogproc.h"
#include "access/ustore/knl_undorequest.h"
#include "storage/buf/crbuf.h"

/*
 * compare two indextuple, itup1 can't be pivot.
 */
static int CompareIndexTuple(Relation rel, IndexTuple itup1, IndexTuple itup2)
{
    TupleDesc itupdesc = RelationGetDescr(rel);
    int16* indoption = rel->rd_indoption;
    int keyNum1 = UBTreeTupleGetNAtts(itup1, rel);
    int keyNum2 = UBTreeTupleGetNAtts(itup2, rel);
    int minNum = Min(keyNum1, keyNum2);

    bool isnull1;
    bool isnull2;
    int result;

    if (!UBTreeTupleIsPivot(itup1) && UBTreeTupleIsPivot(itup2)) {
        Assert(keyNum1 >= keyNum2);
    } else if (!UBTreeTupleIsPivot(itup1) && !UBTreeTupleIsPivot(itup2)) {
        Assert(keyNum1 == keyNum2);
    } else {
        Assert(false);
    }

    for (int i = 1; i <= minNum; i++) {
        Datum datum1 = index_getattr(itup1, i, itupdesc, &isnull1);
        Datum datum2 = index_getattr(itup2, i, itupdesc, &isnull2);

        /* if both null, consider them equal, otherwise depend on INDOPTION_NULLS_FIRST */
        if (isnull1) {
            if (isnull2) {
                result = 0;
            } else {
                result = indoption[i-1] & INDOPTION_NULLS_FIRST ? -1 : 1;
            }
        } else if (isnull2) {
            result = indoption[i-1] & INDOPTION_NULLS_FIRST ? 1 : -1;
        } else {
            Oid sortFunction = index_getprocid(rel, i, BTORDER_PROC);
            FmgrInfo finfo;
            fmgr_info(sortFunction, &finfo);
            result = DatumGetInt32(FunctionCall2Coll(&finfo, rel->rd_indcollation[i - 1], datum1, datum2));
            if (indoption[i - 1] & INDOPTION_DESC) {
                result *= -1;
            }
        }

        if (result != 0) {
            return result;
        }
    }

    /**
     * If all non-truncated key are equal (except TID).
     * Treat truncated attributes as minus infinity.
     */
    if (keyNum1 > keyNum2) {
        return 1;
    }

    ItemPointer tid1 = UBTreeTupleGetHeapTID(itup1);
    ItemPointer tid2 = UBTreeTupleGetHeapTID(itup2);

    Assert(tid1 != NULL);

    if (tid2 == NULL) {
        return 1;
    }

    return ItemPointerCompare(tid1, tid2);
}

static uint8 UBTreePageGetTDId(Page page, TransactionId xid, UndoRecPtr *urp)
{
    int tdCount = UBTreePageGetTDSlotCount(page);
    for (int tdid = 1; tdid <= tdCount; tdid++) {
        UBTreeTD td = UBTreePCRGetTD(page, tdid);
        if (TransactionIdEquals(td->xactid, xid)) {
            *urp = td->undoRecPtr;
            return tdid;
        }
    }
    return UBTreeInvalidTDSlotId;
}

static OffsetNumber SearchTupleOffnum(Relation rel, Page page, IndexTuple itup, BTScanInsert itupKey, uint8 tdid)
{
    itupKey->nextkey = false;
    OffsetNumber start = UBTreePCRBinarySearch(rel, itupKey, page);
    itupKey->nextkey = true;
    OffsetNumber end = UBTreePCRBinarySearch(rel, itupKey, page);
    IndexTuple curItup;
    UBTreeItemId itemid;
    Size size = IndexTupleSize(itup);
    for (OffsetNumber i = start; i < end; i++) {
        curItup = UBTreePCRGetIndexTuple(page, i);
        itemid = UBTreePCRGetRowPtr(page, i);
        if (itemid->lp_td_id == tdid && size == IndexTupleSize(curItup)&& UBTreeItupEquals(itup, curItup)) {
            return i;
        }
    }
    return InvalidOffsetNumber;
}

bool UBTreePCRIsKeyEqual(Relation idxrel, IndexTuple itup, BTScanInsert itupKey)
{
    TupleDesc itupdesc = RelationGetDescr(idxrel);
    ScanKey scankey = itupKey->scankeys;
    /*
     * Index tuple shouldn't be truncated.	Despite we technically could
     * compare truncated tuple as well, this function should be only called
     * for regular non-truncated leaf tuples and P_HIKEY tuple on
     * rightmost leaf page.
     */
    for (int i = 1; i <= itupKey->keysz; i++, scankey++) {
        AttrNumber attno = scankey->sk_attno;
        Assert(attno == i);
        bool datumIsNull = false;
        bool skeyIsNull = ((scankey->sk_flags & SK_ISNULL) ? true : false);
        Datum datum = index_getattr(itup, attno, itupdesc, &datumIsNull);

        if (datumIsNull && skeyIsNull)
            continue; /* NULL equal NULL */
        if (datumIsNull != skeyIsNull)
            return false; /* NOT_NULL not equal NULL */

        if (DatumGetInt32(FunctionCall2Coll(&scankey->sk_func,
                                            scankey->sk_collation, datum, scankey->sk_argument)) != 0) {
            return false;
        }
    }

    /* if we get here, the keys are equal */
    return true;
}

static bool IndexTupleBelongToRightPage(Relation rel, Page page, IndexTuple itup, BTScanInsert itupKey, Oid partOid)
{
    UBTPCRPageOpaque opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
    if (P_RIGHTMOST(opaque)) {
        return false;
    }

    if (P_ISDELETED(opaque)) {
        return true;
    }

    int cmpResult = CompareIndexTuple(rel, itup, UBTreePCRGetIndexTuple(page, P_HIKEY));
    if (cmpResult != 0) {
        return cmpResult > 0;
    }

    OffsetNumber curOffset = UBTreePCRPageGetMaxOffsetNumber(page);

    while (curOffset > P_HIKEY) {
        IndexTuple curTuple = UBTreePCRGetIndexTuple(page, curOffset);
        if (!ItemPointerEquals(&curTuple->t_tid, &itup->t_tid)) {
            return true;
        }
        if (!UBTreePCRIsKeyEqual(rel, curTuple, itupKey)) {
            return true;
        }
        if (RelationIsGlobalIndex(rel)) {
            bool isnull = false;
            Oid curPartOid = DatumGetUInt32(index_getattr(curTuple, IndexRelationGetNumberOfAttributes(rel),
                RelationGetDescr(rel), &isnull));
            if (curPartOid == partOid) {
                return false;
            }
        } else {
            return false;
        }
        curOffset--;
    }
    return true;
}

static bool CheckTupleBelongToThisPage(Relation rel, Page page, IndexTuple itup, IndexTuple firstTuple)
{
    int result = 0;
    UBTPCRPageOpaque opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
    if (!P_LEFTMOST(opaque)) {
        OffsetNumber max = UBTreePCRPageGetMaxOffsetNumber(page);
        if (max < P_FIRSTDATAKEY(opaque)) {
            return false;
        }
        result = CompareIndexTuple(rel, itup,
            firstTuple != NULL ? firstTuple : UBTreePCRGetIndexTuple(page, P_FIRSTDATAKEY(opaque)));
    }

    if (result < 0) {
        return false;
    }

    if (P_RIGHTMOST(opaque)) {
        return true;
    } else {
        return CompareIndexTuple(rel, itup, UBTreePCRGetIndexTuple(page, P_HIKEY)) <= 0;
    }
}

static void ExecuteRollback(Relation rel, BlockNumber blkno, Page page, OffsetNumber offnum, UndoRecord *urec,
    bool isCRPage)
{
    UBTPCRPageOpaque opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
    UBTreeItemId itemid = UBTreePCRGetRowPtr(page, offnum);
    UBTreeUndoInfo undoinfo = FetchUndoInfoFromUndoRecord(urec);
    uint8 prevTDid = (undoinfo->prev_td_id >= opaque->td_count) ? UBTreeFrozenTDSlotId : undoinfo->prev_td_id;
    UBTreeTD curTD = UBTreePCRGetTD(page, itemid->lp_td_id);
    if (urec->Utype() == UNDO_UBT_INSERT) {
        /* mark tuple deleted */
        UBTreePCRSetIndexTupleDeleted(itemid);
        UBTreePCRTDSetStatus(curTD, TD_DELETE);
        if (prevTDid == UBTreeFrozenTDSlotId) {
            ItemIdMarkDead(itemid);
        } else {
            Assert(prevTDid != UBTreeInvalidTDSlotId);
            UBTreeTD td = UBTreePCRGetTD(page, prevTDid);
            if (UBTreePCRTDIsCommited(td)) {
                UBTreePCRSetIndexTupleTDInvalid(itemid);
            } else if (UBTreePCRTDIsFrozen(td)) {
                ItemIdMarkDead(itemid);
            } else {
                TransactionId prevXid = urec->OldXactId();
                if (!TransactionIdIsNormal(prevXid) || prevXid != td->xactid) {
                    UBTreePCRSetIndexTupleTDInvalid(itemid);
                }
            }
        }
        UBTreePCRSetIndexTupleTDSlot(itemid, prevTDid);
        opaque->activeTupleCount--;
    } else if (urec->Utype() == UNDO_UBT_DELETE) {
        /* clear deleted flag */
        UBTreePCRClearIndexTupleDeleted(itemid);
        if (prevTDid == UBTreeFrozenTDSlotId) {
            IndexItemIdSetFrozen(itemid);
        } else {
            Assert(prevTDid != UBTreeInvalidTDSlotId);
            UBTreeTD td = UBTreePCRGetTD(page, prevTDid);
            if (UBTreePCRTDIsCommited(td)) {
                UBTreePCRSetIndexTupleTDInvalid(itemid);
            } else if (UBTreePCRTDIsFrozen(td)) {
                IndexItemIdSetFrozen(itemid);
            } else {
                TransactionId prevXid = urec->OldXactId();
                if (!TransactionIdIsNormal(prevXid) || prevXid != td->xactid) {
                    UBTreePCRSetIndexTupleTDInvalid(itemid);
                }
            }
        }
        UBTreePCRSetIndexTupleTDSlot(itemid, prevTDid);
        UBTreeItemIdSetNormal(itemid, itemid->lp_off);
        opaque->activeTupleCount++;
    } else {
        ereport(PANIC, (errmsg("unknown undo type, rnode[%u,%u,%u], blkno:%u, "
            "urp:%lu, undotype:%u", rel->rd_node.spcNode, rel->rd_node.dbNode, rel->rd_node.relNode,
            blkno, urec->Urp(), urec->Utype())));
    }
}

static void PruneCRPage(Relation rel, Page page)
{
    Offset pdLower = ((PageHeader)page)->pd_lower;
    Offset pdUpper = ((PageHeader)page)->pd_upper;
    Offset pdSpecial = ((PageHeader)page)->pd_special;

    if ((unsigned int)(pdLower) < SizeOfPageHeaderData || pdLower > pdUpper || pdUpper > pdSpecial ||
        pdSpecial > BLCKSZ || (unsigned int)(pdSpecial) != MAXALIGN(pdSpecial))
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("corrupted page pointers: lower = %d, upper = %d, special = %d",
            pdLower, pdUpper, pdSpecial)));

    int maxOff = UBTreePCRPageGetMaxOffsetNumber(page);
    int num = 0;
    Size totallen = 0;

    UBTreeItemIdSortData itemidbase[MaxIndexTuplesPerPage];
    UBTreeItemIdSort itemidptr = itemidbase;

    for (int i = FirstOffsetNumber; i <= maxOff; i++) {
        UBTreeItemId lp = UBTreePCRGetRowPtr(page, i);
        if (!ItemIdIsDead(lp)) {
            num++;
            itemidptr->offsetindex = num;
            itemidptr->itemoff = ItemIdGetOffset(lp);
            itemidptr->olditemid = *lp;
            if (itemidptr->itemoff < (int)pdUpper || itemidptr->itemoff >= (int)pdSpecial)
                ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                    errmsg("corrupted item pointer: %d.", itemidptr->itemoff)));
            itemidptr->alignedlen = MAXALIGN(IndexTupleSize(UBTreePCRGetIndexTuple(page, i)));
            totallen += itemidptr->alignedlen;
            itemidptr++;
        }
    }

    if (num == maxOff) {
        ereport(ERROR, (errmsg("fail to prune cr page, no dead index tuple on page.")));
    }

    if (totallen > (Size)(pdSpecial - pdLower))
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("corrupted item lengths: total %u, available space %d.",
            (unsigned int)totallen, pdSpecial - pdLower)));

    /* sort UBTreeItemIdSortData array into decreasing itemoff order */
    qsort((char*)itemidbase, num, sizeof(UBTreeItemIdSortData), UBTreePCRItemOffCompare);

    /* compactify page and install new itemids */
    Offset upper = pdSpecial;
    itemidptr = itemidbase;
    for (int i = 0; i < num; i++) {
        UBTreeItemId lp = UBTreePCRGetRowPtr(page, itemidptr->offsetindex);
        upper -= itemidptr->alignedlen;
        *lp = itemidptr->olditemid;
        lp->lp_off = upper;
        if (upper != itemidptr->itemoff) {
            errno_t rc = memmove_s((char *)page + upper, itemidptr->alignedlen, (char *)page + itemidptr->itemoff,
                itemidptr->alignedlen);
            securec_check(rc, "\0", "\0");
        }
        itemidptr++;
    }

    ((PageHeader)page)->pd_lower = GetPageHeaderSize(page) + UBTreePCRTdSlotSize(UBTreePageGetTDSlotCount(page)) +
        num * sizeof(UBTreeItemIdData);
    ((PageHeader)page)->pd_upper = upper;
}

static OffsetNumber RestorePrunedTuple(Relation rel, Page page, IndexTuple itup, uint8 tdid)
{
    Size itemsz = IndexTupleSize(itup);
    if (PageGetFreeSpace(page) < itemsz) {
        PruneCRPage(rel, page);
        if (unlikely(PageGetFreeSpace(page) < itemsz)) {
            ereport(ERROR, (errmsg("still no space after prune cr page, can not restore pruned tuple.")));
        }
    }
    BTScanInsert itupKey = UBTreeMakeScanKey(rel, itup);
    OffsetNumber offnum = UBTreePCRBinarySearch(rel, itupKey, page);
    offnum = UBTPCRPageAddItem(page, (Item)itup, itemsz, offnum, false);
    UBTreeItemId itemid = UBTreePCRGetRowPtr(page, offnum);
    UBTreePCRSetIndexTupleTDSlot(itemid, tdid);
    UBTreePCRClearIndexTupleTDInvalid(itemid);
    UBTreePCRClearIndexTupleDeleted(itemid);
    pfree(itupKey);
    return offnum;
}

static OffsetNumber RollbackOneUndoRecord(Relation rel, BlockNumber blkno, Page page, UndoRecord *urec, bool isCRPage,
    uint8 tdid, IndexTuple firstTuple)
{
    IndexTuple itup = FetchTupleFromUndoRecord(urec);
    if (!CheckTupleBelongToThisPage(rel, page, itup, firstTuple)) {
        ereport(DEBUG5, (errmsg("pcr rollback skip tuple, tuple not belong to this page, rnode[%u,%u,%u], blkno:%u, "
            "urp:%lu", rel->rd_node.spcNode, rel->rd_node.dbNode, rel->rd_node.relNode, blkno, urec->Urp())));
        return InvalidOffsetNumber;
    }

    BTScanInsert itupKey = UBTreeMakeScanKey(rel, itup);
    Oid partOid = InvalidOid;
    if (RelationIsGlobalIndex(rel)) {
        bool isnull = false;
        partOid = DatumGetUInt32(index_getattr(itup, IndexRelationGetNumberOfAttributes(rel),
            RelationGetDescr(rel), &isnull));
        Assert(!isnull);
        Assert(partOid != InvalidOid);
    }

    OffsetNumber offnum = SearchTupleOffnum(rel, page, itup, itupKey, tdid);
    pfree(itupKey);

    if (offnum == InvalidOffsetNumber) {
        if (isCRPage) {
            if (urec->Utype() == UNDO_UBT_INSERT) {
                ereport(DEBUG5, (errmsg("pcr rollback cr page skip rollback, inserted tuple not found, "
                    "rnode[%u,%u,%u], blkno:%u, urp:%lu", rel->rd_node.spcNode, rel->rd_node.dbNode,
                    rel->rd_node.relNode, blkno, urec->Urp())));
                return offnum;
            } else {
                return RestorePrunedTuple(rel, page, itup, tdid);
            }
        } else {
            ereport(DEBUG5, (errmsg("pcr rollback skip rollback, tuple not found, rnode[%u,%u,%u], blkno:%u, "
                "urp:%lu", rel->rd_node.spcNode, rel->rd_node.dbNode, rel->rd_node.relNode, blkno, urec->Urp())));
            return offnum;
        }
    }
    ExecuteRollback(rel, blkno, page, offnum, urec, isCRPage);
    return offnum;
}

void RollbackCRPage(IndexScanDesc scan, Page page, uint8 tdid, CommandId *page_cid, CommandId cid, IndexTuple itup)
{
    Relation rel = scan->indexRelation;
    Snapshot snapshot = scan->xs_snapshot;
    UBTreeTD td = UBTreePCRGetTD(page, tdid);
    TransactionId xid = td->xactid;
    UndoRecPtr urecptr = td->undoRecPtr;
    UndoRecord *urec = New(CurrentMemoryContext)UndoRecord();
    urec->SetMemoryContext(CurrentMemoryContext);
    urec->SetUrp(td->undoRecPtr);
    if (page_cid != NULL) {
        *page_cid = InvalidCommandId;
    }
    
    int rollbackCount = 0;
    while (true) {
        UndoTraversalState state = FetchUndoRecord(urec, NULL, InvalidBlockNumber, InvalidOffsetNumber,
            InvalidTransactionId, false, NULL);

        urecptr = urec->Urp();
        if (state == UNDO_TRAVERSAL_ABORT) {
            int zoneId = (int)UNDO_PTR_GET_ZONE_ID(urec->Urp());
            undo::UndoZone *uzone = undo::UndoZoneGroup::GetUndoZone(zoneId, false);
            ereport(ERROR, (errmodule(MOD_UNDO), errmsg(
                "snapshot too old! the undo record has been force discard. "
                "Reason: PCR index RollbackCRPage. "
                "LogInfo: undo state %d. "
                "globalRecycleXid %lu, globalFrozenXid %lu. "
                "ZoneInfo: urp: %lu, zid %d, insertURecPtr %lu, forceDiscardURecPtr %lu, "
                "discardURecPtr %lu, recycleXid %lu. "
                "Snapshot: type %d, xmin %lu.",
                state,
                pg_atomic_read_u64(&g_instance.undo_cxt.globalRecycleXid),
                pg_atomic_read_u64(&g_instance.undo_cxt.globalFrozenXid),
                urec->Urp(), zoneId, PtrGetVal(uzone, GetInsertURecPtr()),
                PtrGetVal(uzone, GetForceDiscardURecPtr()),
                PtrGetVal(uzone, GetDiscardURecPtr()), PtrGetVal(uzone, GetRecycleXid()),
                PtrGetVal(snapshot, satisfies), PtrGetVal(snapshot, xmin))));
        } else if (state != UNDO_TRAVERSAL_COMPLETE) {
            xid = InvalidTransactionId;
            urecptr = INVALID_UNDO_REC_PTR;
            break;
        }

        Assert(TransactionIdIsValid(urec->Xid()));
        if (urec->Xid() != xid) {
            xid = urec->Xid();
            break;
        }

        if (cid != InvalidCommandId && urec->Cid() < cid) {
            if (page_cid != NULL) {
                *page_cid = urec->Cid();
            }
            break;
        }
        RollbackOneUndoRecord(rel, InvalidBlockNumber, page, urec, true, tdid, itup);
        rollbackCount++;
        if (rollbackCount % CR_ROLLBACL_COUNT_THRESHOLD == 0) {
            CHECK_FOR_INTERRUPTS();
        }

        Assert(urecptr == urec->Urp());

        if (!IS_VALID_UNDO_REC_PTR(urec->Blkprev())) {
            xid = InvalidTransactionId;
            urecptr = INVALID_UNDO_REC_PTR;
            break;
        }
        urec->Reset2Blkprev();
    }

    Assert(TransactionIdIsValid(xid) == IS_VALID_UNDO_REC_PTR(urecptr));

    td->xactid = xid;
    td->undoRecPtr = urecptr;
    if (!TransactionIdIsValid(xid)) {
        UBTreePCRTDSetStatus(td, TD_FROZEN);
    }
    UBTreePCRTDClearStatus(td, TD_CSN);
    DELETE_EX(urec);
}


static void SetTDInfo(Relation rel, Page page, int tdid, TransactionId xid, UndoRecPtr urecptr)
{
    Assert(tdid != UBTreeInvalidTDSlotId);
    Assert(tdid <= UBTreePageGetTDSlotCount(page));
    Assert(TransactionIdIsValid(xid) == IS_VALID_UNDO_REC_PTR(urecptr));

    UBTreeTD td = UBTreePCRGetTD(page, tdid);

    if (!TransactionIdIsValid(xid)) {
        UBTreePCRTDSetStatus(td, TD_FROZEN);
    } else if (xid != td->xactid) {
        UBTreePCRTDSetStatus(td, TD_COMMITED);
    }
    UBTreePCRTDClearStatus(td, TD_ACTIVE);
    td->undoRecPtr = urecptr;
    td->xactid = xid;
}

int UBTreePCRRollback(URecVector *urecvec, int startIdx, int endIdx, TransactionId xid, Oid reloid, Oid partitionoid,
    BlockNumber blkno, bool isFullChain, int preRetCode, Oid *preReloid, Oid *prePartitionoid)
{
    Assert(startIdx == endIdx);
    if (preReloid != NULL && prePartitionoid != NULL) {
        if (*preReloid == reloid && *prePartitionoid == partitionoid && preRetCode == ROLLBACK_OK_NOEXIST) {
            return ROLLBACK_OK_NOEXIST;
        }
        *preReloid = reloid;
        *prePartitionoid = partitionoid;
    }

    UndoRelationData relationData = { 0 };

    UndoRecord *undorec = (*urecvec)[startIdx];
    Oid relfilenode = undorec->Relfilenode();
    Oid tablespace = undorec->Tablespace();

    /*
     * We always try to lock the relation.  If the relation is already gone,
     * then we can skip processing the undo actions.
     */
    if (!UHeapUndoActionsOpenRelation(reloid, partitionoid, &relationData)) {
        elog(LOG, "Either the relation or the partition is dropped.");
        return ROLLBACK_OK_NOEXIST;
    }

    /*
     * When a transaction does a DDL, it is possible that either relid/partitionoid maps to the 
     * old relfilenode during rollback and hence it does not match the 
     * relfilenode in the undorecord anymore. But we know we need to do the rollback
     * on the relfilenode pointed to by the undorecord because that's where we did the operation.
     * In this case, we need to find the correct relid that maps to this relfilenode.
     */
    if (RelationGetRelFileNode(relationData.relation) != relfilenode ||
        RelationGetRnodeSpace(relationData.relation) != tablespace) {
        elog(LOG, "The open relation does not match undorecord. expected:(%d/%d) actual:(%d/%d) for block: %d",
             RelationGetRnodeSpace(relationData.relation),
             RelationGetRelFileNode(relationData.relation),
             tablespace, relfilenode, blkno);

        /* close the recently opened relation before opening a new one */
        UHeapUndoActionsCloseRelation(&relationData);

        RelFileNode targetNode = { 0 };
        targetNode.spcNode = tablespace;
        targetNode.relNode = relfilenode;

        if (!UHeapUndoActionsFindRelidByRelfilenode(&targetNode, &reloid, &partitionoid)) {
            elog(LOG, "The relation is dropped");
            return ROLLBACK_OK_NOEXIST;
        }

        /* Now try opening the relation using the "correct" relid */
        if (!UHeapUndoActionsOpenRelation(reloid, partitionoid, &relationData)) {
            elog(LOG, "Either the relation or the partition is dropped after retry.");
            return ROLLBACK_OK_NOEXIST;
        }
    }

    /*
     * This is possible if the underlying relation is truncated just
     * before taking the relation lock above.
     */
    if (RelationGetNumberOfBlocks(relationData.relation) <= blkno) {
        UHeapUndoActionsCloseRelation(&relationData);
        elog(LOG, "relation is already truncated.");
        return ROLLBACK_OK_NOEXIST;
    }

    Relation rel = relationData.relation;
    BlockNumber curBlkno = blkno;
    Buffer buffer = ReadBuffer(relationData.relation, curBlkno);
    LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
    Page page = BufferGetPage(buffer);
    UBTPCRPageOpaque opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
    IndexTuple itup = FetchTupleFromUndoRecord(undorec);
    BTScanInsert itupKey = UBTreeMakeScanKey(rel, itup);
    Oid partOid = InvalidOid;

	if (!RecoveryInProgress())
        CRBufferUnused(buffer);

    if (RelationIsGlobalIndex(rel)) {
        bool isnull = false;
        partOid = DatumGetUInt32(index_getattr(itup, IndexRelationGetNumberOfAttributes(rel),
            RelationGetDescr(rel), &isnull));
        Assert(!isnull);
        Assert(partOid != InvalidOid);
    }

    UBTreeRedoRollbackAction redoRollbackAction;

    while (IndexTupleBelongToRightPage(rel, page, itup, itupKey, partOid)) {
        do {
            curBlkno = opaque->btpo_next;
            buffer = _bt_relandgetbuf(rel, buffer, opaque->btpo_next, BT_WRITE);
            page = BufferGetPage(buffer);
            opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
        } while (P_IGNORE(opaque));
    }

    UndoRecPtr urecPtr = INVALID_UNDO_REC_PTR;
    uint8 tdid = UBTreePageGetTDId(page, xid, &urecPtr);
    if (tdid != UBTreeInvalidTDSlotId && undorec->Urp() > urecPtr) {
        tdid = UBTreeInvalidTDSlotId;
    }

    if (tdid != UBTreeInvalidTDSlotId) {
        OffsetNumber offnum = SearchTupleOffnum(rel, page, itup, itupKey, tdid);
        if (offnum != InvalidOffsetNumber) {
            START_CRIT_SECTION();
            ExecuteRollback(rel, curBlkno, page, offnum, undorec, false);
            redoRollbackAction.rollback_items.append(page, offnum);
            TransactionId xidInTD = xid;
            UndoRecPtr urecptrInTD = undorec->Blkprev();
            if (!IS_VALID_UNDO_REC_PTR(urecptrInTD)) {
                xidInTD = InvalidTransactionId;
            } else {
                UndoRecord *urec = New(CurrentMemoryContext)UndoRecord();
                urec->SetMemoryContext(CurrentMemoryContext);
                urec->SetUrp(urecptrInTD);
                UndoTraversalState state = FetchUndoRecord(urec, NULL, InvalidBlockNumber, InvalidOffsetNumber,
                    InvalidTransactionId, false, NULL);
                if (state != UNDO_TRAVERSAL_COMPLETE || urec->Xid() == InvalidTransactionId) {
                    xidInTD = InvalidTransactionId;
                    urecptrInTD = INVALID_UNDO_REC_PTR;
                } else if (urec->Xid() != xidInTD) {
                    xidInTD = urec->Xid();
                }
                DELETE_EX(urec);
            }
            SetTDInfo(rel, page, tdid, xidInTD, urecptrInTD);
            redoRollbackAction.xid = xid;
            redoRollbackAction.td_id = tdid;
            redoRollbackAction.td = *(UBTreePCRGetTD(page, tdid));
            MarkBufferDirty(buffer);
            ereport(DEBUG5, (errmsg("pcr rollback one undorecord, rnode[%u,%u,%u], blkno:%u, "
                "xid:%lu, urp:%lu, undotype:%u", rel->rd_node.spcNode, rel->rd_node.dbNode, rel->rd_node.relNode,
                blkno, xid, undorec->Urp(), undorec->Utype())));
            if (RelationNeedsWAL(rel)) {
                XLogRecPtr recptr;
                xl_ubtree3_rollback_txn xlrec;
                xlrec.xid = redoRollbackAction.xid;
                xlrec.n_rollback = redoRollbackAction.rollback_items.next_item;
                xlrec.td_id = redoRollbackAction.td_id;
                XLogBeginInsert();
                XLogRegisterData((char *)&xlrec, sizeOfUbtree3RollbackTxn);
                XLogRegisterData((char *)&redoRollbackAction.td, sizeof(UBTreeTDData));
                if (xlrec.n_rollback > 0) {
                    XLogRegisterData((char *)redoRollbackAction.rollback_items.items,
                        sizeof(UBTreeRedoRollbackItemData) * xlrec.n_rollback);
                }
                XLogRegisterBuffer(0, buffer, REGBUF_STANDARD);
                recptr = XLogInsert(RM_UBTREE3_ID, XLOG_UBTREE3_ROLLBACK_TXN);
                PageSetLSN(page, recptr);
            }
            END_CRIT_SECTION();
            redoRollbackAction.rollback_items.release();
            if (opaque->activeTupleCount == 0 && curBlkno != InvalidBlockNumber) {
                UBTreeRecordEmptyPage(rel, curBlkno, opaque->last_delete_xid);
            }
        } else {
            ereport(DEBUG5, (errmsg("pcr rollback skip tuple, tuple not on the page, rnode[%u,%u,%u], blkno:%u, "
                "xid:%lu, urp:%lu, undotype:%u", rel->rd_node.spcNode, rel->rd_node.dbNode, rel->rd_node.relNode,
                blkno, xid, undorec->Urp(), undorec->Utype())));
        }
    }

    pfree(itupKey);
    UnlockReleaseBuffer(buffer);
    /* Close the relation. */
    UHeapUndoActionsCloseRelation(&relationData);

    return ROLLBACK_OK;
}

/*
* ExecuteUndoActionsForUBTreePage
*     Eexute rollback for ubtree page.
*/
void ExecuteUndoActionsForUBTreePage(Relation rel, Buffer buffer, uint8 tdid)
{
    Page page = BufferGetPage(buffer);
    BlockNumber blkno = BufferGetBlockNumber(buffer);
    UBTreeTD td = UBTreePCRGetTD(page, tdid);
    TransactionId xid = td->xactid;
    UndoRecPtr urecptr = td->undoRecPtr;
    int undoApplySize = GetUndoApplySize();

    do {
        URecVector *urecvec = FetchUndoRecordRange(&urecptr, INVALID_UNDO_REC_PTR, undoApplySize, true);
        UBTreeRedoRollbackAction redoRollbackAction;
        redoRollbackAction.xid = xid;
        redoRollbackAction.td_id = tdid;
        START_CRIT_SECTION();
        for (int i = 0; i <  urecvec->Size(); i++) {
            Assert((*urecvec)[i]->Xid() == xid);
            OffsetNumber offnum = RollbackOneUndoRecord(rel, blkno, page, (*urecvec)[i], false, tdid, NULL);
            if (offnum != InvalidOffsetNumber) {
                redoRollbackAction.rollback_items.append(page, offnum);
            }
        }
        TransactionId xidInTD = xid;
        UndoRecPtr urecptrInTD = INVALID_UNDO_REC_PTR;
        if (urecvec->Size() == 0) {
            Assert(!IS_VALID_UNDO_REC_PTR(urecptr));
            xidInTD = InvalidTransactionId;
        } else {
            UndoRecord *urec = (*urecvec)[urecvec->Size() - 1];
            urecptrInTD = urec->Blkprev();
            if (!IS_VALID_UNDO_REC_PTR(urecptrInTD)) {
                xidInTD = InvalidTransactionId;
            } else {
                urec->Reset2Blkprev();
                UndoTraversalState state = FetchUndoRecord(urec, NULL, InvalidBlockNumber, InvalidOffsetNumber,
                    InvalidTransactionId, false, NULL);
                if (state != UNDO_TRAVERSAL_COMPLETE) {
                    xidInTD = InvalidTransactionId;
                    urecptrInTD = INVALID_UNDO_REC_PTR;
                } else if (urec->Xid() != xidInTD) {
                    xidInTD = urec->Xid();
                }
            }
        }
        SetTDInfo(rel, page, tdid, xidInTD, urecptrInTD);
        redoRollbackAction.td = *(UBTreePCRGetTD(page, tdid));
        MarkBufferDirty(buffer);
        if (RelationNeedsWAL(rel)) {
            XLogRecPtr recptr;
            xl_ubtree3_rollback_txn xlrec;
            xlrec.xid = redoRollbackAction.xid;
            xlrec.n_rollback = redoRollbackAction.rollback_items.next_item;
            xlrec.td_id = redoRollbackAction.td_id;
            XLogBeginInsert();
            XLogRegisterData((char *)&xlrec, sizeOfUbtree3RollbackTxn);
            XLogRegisterData((char *)&redoRollbackAction.td, sizeof(UBTreeTDData));
            if (xlrec.n_rollback > 0) {
                XLogRegisterData((char *)redoRollbackAction.rollback_items.items,
                    sizeof(UBTreeRedoRollbackItemData) * xlrec.n_rollback);
            }

            XLogRegisterBuffer(0, buffer, REGBUF_STANDARD);

            recptr = XLogInsert(RM_UBTREE3_ID, XLOG_UBTREE3_ROLLBACK_TXN);
            PageSetLSN(page, recptr);
        }
        END_CRIT_SECTION();
        redoRollbackAction.rollback_items.release();

        DELETE_EX(urecvec);
        UBTPCRPageOpaque opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
        if (opaque->activeTupleCount == 0 && blkno != InvalidBlockNumber) {
            UBTreeRecordEmptyPage(rel, blkno, opaque->last_delete_xid);
        }
        if (xid != td->xactid) {
            break;
        }
    } while (true);
}
