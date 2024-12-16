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
 * ubtpcrinsert.cpp
 *  DML functions for ubtree pcr page.
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/ubtreepcr/ubtpcrinsert.cpp
 *
 * --------------------------------------------------------------------------------------
 */


#include "access/ubtreepcr.h"

bool UBTreePCRDoInsert(Relation rel, IndexTuple itup, IndexUniqueCheck checkUnique, Relation heapRel)
{
    return false;
}

bool UBTreePCRDoDelete(Relation rel, IndexTuple itup, bool isRollbackIndex)
{
    bool found = true;
    BTScanInsert itupKey;
    Buffer buf;
    volatile Page page = NULL;
    OffsetNumber offset;
    BTStack stack = NULL;
    TransactionId fxid = GetTopTransactionId();

    /* Create an insertion scan key to search itup. */
    itupKey = UBTreeMakeScanKey(rel, itup);
    UBTreeResetWaitTimeForTDSlot();

DELETE_RETRY:
    /*
     * STEP 1: Look for the first page that may contain this tuple
     */
    stack = UBTreePCRSearch(rel, itupKey, &buf, BT_WRITE, false);

    /*
     * STEP 2: Look for the exact tuple that needs to be deleted.
     * 
     * Note: 1. The index keys, heap tid, and part oid of the found tuple must exactly match those of itup.
     *       2. Retry this step if page has been pruned.
     */ 
    uint8 tdslot = UBTreeInvalidTDSlotId;
    UBTreeUndoInfoData undoInfo;
    while (true) {
        /* Get the offset number of the tuple. */
        offset = UBTreePCRBinarySearch(rel, itupKey, BufferGetPage(buf));

        /* Look for the tuple with same keys, tie and part oid */
        offset = UBTreePCRFindDeleteLoc(rel, &buf, offset, itupKey, itup);
        /* Get the page and disable optimization */
        page = BufferGetPage(buf);
        if (offset == InvalidOffsetNumber) {
            ReportFailedToDelete(rel, buf, itup, "can't find it");
        }
        /* shift xid-base if nece */

        TransactionId minXid = MaxTransactionId;
        bool needRetry = false;
        /* Preparation tdslot */
        tdslot = PreparePCRDelete(rel, buf, offset, &undoInfo, &minXid, &needRetry);         
        if (needRetry) {
            /* in this case we have unlock and release the buf to wait for inprogress xid, thus need to retry */
            goto DELETE_RETRY;
        }
        if (tdslot != UBTreeInvalidTDSlotId) {
            break;
        } else {
            _bt_relbuf(rel, buf);
            UHeapSleepOrWaitForTDSlot(minXid, false);
        }
        buf = InvalidBuffer;
        goto DELETE_RETRY;
    }
    // TODO
    undo::XlogUndoMeta xlum;
    UndoRecPtr urecPtr = INVALID_UNDO_REC_PTR;

    /* Prepare the undo space before acquiring lock */
    UndoPersistence persistence = UndoPersistenceForRelation(rel);
    Oid relOid = RelationIsPartition(rel) ? rel->parentId : RelationGetRelid(rel);
    Oid partitionOid = RelationIsPartition(rel) ? RelationGetRelid(rel) : InvalidOid;

    urecPtr = UBTreePCRPrepareUndoDelete(relOid, partitionOid, RelationGetRelFileNode(rel),
        RelationGetRnodeSpace(rel), persistence, InvalidBuffer, GetTopTransactionId(), GetCurrentCommandId(false), INVALID_UNDO_REC_PTR, 
        itup, InvalidBlockNumber, NULL, &xlum, &undoInfo);

    /* STEP3: mark xmax for the target tuple */
    UBTreePCRPageDel(rel, buf, offset, isRollbackIndex, tdslot, urecPtr, &xlum);

    UHeapResetPreparedUndo();
    /* be tidy */
    pfree(itupKey);
    return found;
}

bool UBTreePCRPagePruneOpt(Relation rel, Buffer buf, bool tryDelete, BTStack del_blknos)
{
    return false;
}

bool UBTreePCRPagePrune(Relation rel, Buffer buf, TransactionId oldestXmin, OidRBTree *invisibleParts)
{
    return false;
}

bool UBTreePCRPruneItem(Page page, OffsetNumber offnum, TransactionId oldestXmin, IndexPruneState* prstate, bool isToast)
{
    return false;
}

void UBTreePCRPagePruneExecute(Page page, OffsetNumber* nowdead, int ndead, IndexPruneState* prstate, TransactionId oldest_xmin)
{
    return;
}

void UBTreePCRPageRepairFragmentation(Relation rel, BlockNumber blkno, Page page)
{
    return;
}

void UBTreePCRInsertParent(Relation rel, Buffer buf, Buffer rbuf, BTStack stack, bool is_root, bool is_only)
{
    return;
}

void UBTreePCRFinishSplit(Relation rel, Buffer lbuf, BTStack stack)
{
    return;
}

Buffer UBTreePCRGetStackBuf(Relation rel, BTStack stack)
{
    return NULL;
}

void UBTreeResetWaitTimeForTDSlot() 
{
    t_thrd.ustore_cxt.tdSlotWaitActive = false;
    t_thrd.ustore_cxt.tdSlotWaitFinishTime = 0;
}

OffsetNumber UBTreePCRFindDeleteLoc(Relation rel, Buffer* bufP, OffsetNumber offset, BTScanInsert itup_key, IndexTuple itup)
{
    OffsetNumber maxoff;
    Page page;
    UBTPCRPageOpaque opaque;

    page = BufferGetPage(*bufP);
    opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
    maxoff = UBTreePcrPageGetMaxOffsetNumber(page);

    AttrNumber partitionOidAttr = IndexRelationGetNumberOfAttributes(rel);
    Oid partOid = InvalidOid;

    /* Get partOid for global partition index */
    if (RelationIsGlobalIndex(rel)) {
        bool isnull = false;
        TupleDesc tupdesc = RelationGetDescr(rel);
        partOid = DatumGetUInt32(index_getattr(itup, partitionOidAttr, tupdesc, &isnull));
        Assert(!isnull);
    }

    /* Scan over all equal tuples, looking for itup */
    for (;;) {
        ItemId curitemid;
        IndexTuple curitup;
        BlockNumber nblkno;

        if (offset <= maxoff) {
            curitemid = UBTreePCRGetRowPtr(page, offset);
            if (!ItemIdIsDead(curitemid)) {
                /* 
                 * _bt_compare returns 0 for (1,NULL) and (1, NULL) - this's 
                 * how we handling NULLs - and so we must not use _bt_compare
                 * in real comparison, but only for ording/finding items on
                 * pages.
                 */
                if (!UBTreePCRIsEqual(rel, page, offset, itup_key->keysz, itup_key->scankeys)) {
                    /* 
                     * we've traversed all the equal tuples, but we haven't found itup
                     */
                    return InvalidOffsetNumber;
                }
                curitup = (IndexTuple)UBTreePCRGetIndexTuple(page, offset);
                if (UBTreeHasIncluding(rel)) {          // diff
                    Size itupSize = IndexTupleSize(itup);
                    if (itupSize == IndexTupleSize(curitup) && memcmp(curitup, itup, itupSize) == 0) {
                        return offset;
                    }
                } else if (UBTreePCRIndexTupleMatches(rel, page, offset, itup, NULL, partOid)) {        
                    return offset;
                }
            }
        }

        /*
         * Advance to the next tuple to continue checking
         */
        if (offset < maxoff) {
            offset = OffsetNumberNext(offset);
        } else {
            int highkeycmp;

            /* 
             * if scankey == highkey we gotta check the next page too
             */
            if (P_RIGHTMOST(opaque)) {
                break;
            }
            highkeycmp = UBTreePCRCompare(rel, itup_key, page, P_HIKEY);
            Assert(highkeycmp <= 0);
            if (highkeycmp != 0) {
                break;
            }
            /* 
             * Check the next non-dead page if scankey equals to the current page's highkey
             */
            for (;;) {
                nblkno = opaque->btpo_next;
                *bufP = _bt_relandgetbuf(rel, *bufP, nblkno, BT_WRITE);
                page = BufferGetPage(*bufP);
                opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
                if (!P_IGNORE(opaque)) {
                    break;
                }
                if (P_RIGHTMOST(opaque)) {
                    ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                            errmsg("fell off the end of index \"%s\" at blkno %u",
                                    RelationGetRelationName(rel), nblkno)));
                }
            }
            maxoff = UBTreePcrPageGetMaxOffsetNumber(page);
            offset = P_FIRSTDATAKEY(opaque);
        }
    }
    /*
     * No corresponding itup was found, release buffer and return InvalidOffsetNumber
     */
    return InvalidOffsetNumber;
}

// UBTreePCRIndexTupleMatches(rel, page, offset, itup, NULL, partOid)
/* return true if the curItup is exactly what we want (keys, tid, partOid, etc.)*/
bool UBTreePCRIndexTupleMatches(Relation rel, Page page, OffsetNumber offnum, IndexTuple target_itup, BTScanInsert itup_key, Oid target_part_oid) {
    IndexTuple curItup = (IndexTuple)UBTreePCRGetIndexTuple(page, offnum);
    if (itup_key && !UBTreePCRIsEqual(rel, page, offnum, itup_key->keysz, itup_key->scankeys)) {
        return false;
    }
    if (!ItemPointerEquals(&curItup->t_tid, &target_itup->t_tid)) {
        return false;
    }
    if (RelationIsGlobalIndex(rel)) {
        bool isnull = false;
        TupleDesc tupdesc = RelationGetDescr(rel);
        AttrNumber partitionOidAttr = IndexRelationGetNumberOfAttributes(rel);
        Oid cur_part_oid = DatumGetUInt32(index_getattr(curItup, partitionOidAttr, tupdesc, &isnull));
        if (cur_part_oid != target_part_oid) {
            return false;
        }
    }
    return true;
}

uint8 PreparePCRDelete(Relation rel, Buffer buf, OffsetNumber offnum, UBTreeUndoInfo undoInfo, TransactionId* minXid, bool* needRetry)
{
    /* Get TD slot */
    UBTreeTDData oldTDData;

    TransactionId fxid = GetTopTransactionId();       
    uint8 tdslot;
    tdslot = UBtreePageReserveTransactionSlot(rel, buf, fxid, &oldTDData, minXid);
    if (tdslot == UBTreeInvalidTDSlotId) {
        return UBTreeInvalidTDSlotId;
    }

    /* Get tuple's previouse operation's xact info */
    Page page = BufferGetPage(buf);
    ItemId iid = (ItemId)UBTreePCRGetRowPtr(page, offnum);
    IndexTuple itup = (IndexTuple)UBTreePCRGetIndexTuple(page, offnum);
    IndexTupleTrx trx = (IndexTupleTrx)UBTreePCRGetIndexTupleTrx(itup);
    uint8 slotNo = trx->tdSlot;
    UBTreePCRHandlePreviousTD(rel, buf, &slotNo, trx, needRetry);       // TODO : 并发事物处理 
    if (*needRetry) {
        return tdslot;
    }

    UBTreeTD thisTrans = UBTreePCRGetTD(page, slotNo);
    
    if (UBTreePCRTDIdIsNormal(slotNo)) {    
        TransactionId xid = thisTrans->xactid;
        TransactionId oldestXmin = pg_atomic_read_u64(&g_instance.undo_cxt.globalRecycleXid);
        if (TransactionIdIsValid(xid) && TransactionIdPrecedes(xid, oldestXmin)) {
            IndexItemIdSetFrozen(iid);
            UBTreePCRSetIndexTupleTrxSlot(trx, UBTreeFrozenTDSlotId);
            slotNo = UBTreeFrozenTDSlotId;
        }
    }

    if (IsUBTreePCRItemDeleted(trx)) {      
        ereport(PANIC,(errmodule(MOD_UBTREE),
                errmsg("failed to delete index tuple, index \"%s\", blkno %u", RelationGetRelationName(rel), BufferGetBlockNumber(buf)),
                errcause("UBTree PCR page is corrupted"),
                erraction("Please try to reindex to fix it")));
    }

    TransactionId prevXid = FrozenTransactionId;
    if (slotNo != UBTreeFrozenTDSlotId) {
        thisTrans = UBTreePCRGetTD(page, slotNo);
        if (thisTrans->xactid == fxid) {
            /* self delete case */
            prevXid = fxid;
        } else if (thisTrans->xactid != InvalidTransactionId) {
            /* must be current xid or committed txn */
            prevXid = thisTrans->xactid;  
        }
    }

    undoInfo->prev_td_id = slotNo;

    /* Update undo record */
    URecVector *urecvec = t_thrd.ustore_cxt.urecvec;
    UndoRecord *urec = (*urecvec)[0];
    urec->SetBlkprev(oldTDData.undoRecPtr);
    urec->SetOldXactId(prevXid);
    urec->SetOffset(offnum);
    return tdslot;
}

UndoRecPtr UBTreePCRPrepareUndoDelete(Oid relOid, Oid partitionOid, Oid relfilenode, Oid tablespace,
    UndoPersistence persistence, Buffer buffer, TransactionId xid, CommandId cid, UndoRecPtr prevurpInOneXact, 
    IndexTuple oldtuple, BlockNumber blk, XlUndoHeader *xlundohdr, undo::XlogUndoMeta *xlundometa, UBTreeUndoInfo undoInfo)
{
    URecVector *urecvec = t_thrd.ustore_cxt.urecvec;
    UndoRecord *urec = (*urecvec)[0];

    urec->SetUtype(UNDO_UBT_DELETE);
    urec->SetUinfo(UNDO_UREC_INFO_PAYLOAD);
    urec->SetXid(xid);
    urec->SetCid(cid);
    urec->SetReloid(relOid);
    urec->SetPartitionoid(partitionOid);
    urec->SetBlkprev(prevurpInOneXact);
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

    urec->SetPrevurp(t_thrd.xlog_cxt.InRecovery ? prevurpInOneXact : GetCurrentTransactionUndoRecPtr(persistence));
    urec->SetNeedInsert(true);

    /* Copy over the entire tuple data to the undorecord */
    MemoryContext oldCxt = MemoryContextSwitchTo(urec->mem_context());
    initStringInfo(urec->Rawdata());
    MemoryContextSwitchTo(oldCxt);
    appendBinaryStringInfo(urec->Rawdata(), (char *)undoInfo, SizeOfUBTreeUndoInfoData);
    appendBinaryStringInfo(urec->Rawdata(), (char *)oldtuple, IndexTupleSize(oldtuple));

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
 * UBTreeIsEqual() -- used in _bt_doinsert in check for duplicates.
 *
 * This is very similar to _bt_compare, except for NULL and negative infinity
 * handling. Rule is simple: NOT_NULL not equal NULL, NULL equal NULL.
 */
bool UBTreePCRIsEqual(Relation idxrel, Page page, OffsetNumber offnum, int keysz, ScanKey scankey)
{
    TupleDesc itupdesc = RelationGetDescr(idxrel);
    IndexTuple itup;
    int i;

    /* Better be comparing to a leaf item */
    Assert(P_ISLEAF((UBTPCRPageOpaque)PageGetSpecialPointer(page)));
    Assert(offnum >= P_FIRSTDATAKEY((UBTPCRPageOpaque) PageGetSpecialPointer(page)));

    itup = (IndexTuple)UBTreePCRGetIndexTuple(page, offnum);
    /*
     * Index tuple shouldn't be truncated.	Despite we technically could
     * compare truncated tuple as well, this function should be only called
     * for regular non-truncated leaf tuples and P_HIKEY tuple on
     * rightmost leaf page.
     */
    for (i = 1; i <= keysz; i++, scankey++) {
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