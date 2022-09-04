/* -------------------------------------------------------------------------
 *
 * knl_undoaction.cpp
 * undo action aka rollback action for UStore
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 * src/gausskernel/storage/access/ustore/knl_undoaction.cpp
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/heapam.h"
#include "access/ustore/knl_uheap.h"
#include "access/ustore/knl_undorequest.h"
#include "access/ustore/knl_uredo.h"
#include "access/ustore/knl_uvisibility.h"
#include "access/ustore/undo/knl_uundozone.h"
#include "access/ustore/undo/knl_uundoapi.h"
#include "access/ustore/knl_uverify.h"
#include "access/ustore/knl_whitebox_test.h"
#include "access/xlog_internal.h"
#include "catalog/pg_partition_fn.h"
#include "miscadmin.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/relfilenodemap.h"

static UHeapDiskTuple CopyTupleFromUndoRecord(UndoRecord *undorecord, Buffer buffer);
static void RestoreXactFromUndoRecord(UndoRecord *undorecord, Buffer buffer, UHeapDiskTuple tuple);
static void LogUHeapUndoActions(UHeapUndoActionWALInfo *walInfo, Relation rel);

static int GetUndoApplySize()
{
    uint64 undoApplySize = (uint64)u_sess->attr.attr_memory.maintenance_work_mem * 1024L;
    uint64 applySize = MAX_UNDO_APPLY_SIZE > undoApplySize ? undoApplySize : MAX_UNDO_APPLY_SIZE;
    Assert(applySize <= MAX_UNDO_APPLY_SIZE);
    return (int)applySize;
}

/*
 * execute_undo_actions - Execute the undo actions
 *
 * xid - Transaction id that is getting rolled back.
 * fromUrecptr - undo record pointer from where to start applying undo action.
 * toUrecptr   - undo record pointer upto which point apply undo action.
 * isTopTxn    - true if rollback is for top transaction.
 */
void ExecuteUndoActions(TransactionId fullXid, UndoRecPtr fromUrecptr, UndoRecPtr toUrecptr,
    UndoSlotPtr slotPtr, bool isTopTxn)
{
    Assert(toUrecptr != INVALID_UNDO_REC_PTR && fromUrecptr != INVALID_UNDO_REC_PTR);
    Assert(slotPtr != INVALID_UNDO_REC_PTR);
    Assert(fullXid  != InvalidTransactionId);
    int undoApplySize = GetUndoApplySize();
    UndoRecPtr urecPtr = fromUrecptr;
    VerifyMemoryContext();
    UndoRecord *urec = New(CurrentMemoryContext)UndoRecord();
    urec->SetUrp(toUrecptr);
    urec->SetMemoryContext(CurrentMemoryContext);

    if (isTopTxn) {
        /*
         * It is important here to fetch the latest undo record and validate
         * if the actions are already executed.  The reason is that it is
         * possible that discard worker or backend might try to execute the
         * rollback request which is already executed.  For ex., after discard
         * worker fetches the record and found that this transaction need to
         * be rolledback, backend might concurrently execute the actions and
         * remove the request from rollback hash table. The similar problem
         * can happen if the discard worker first pushes the request, the undo
         * worker processed it and backend tries to process it some later
         * point.
         */
        UndoTraversalState rc = FetchUndoRecord(urec, NULL, InvalidBlockNumber, InvalidOffsetNumber,
            InvalidTransactionId, false, NULL);
        /* already processed. */
        if (rc != UNDO_TRAVERSAL_COMPLETE) {
            DELETE_EX(urec);
            return;
        }
    }

    DELETE_EX(urec);

    /*
     * Fetch the multiple undo records which can fit into uur_segment; sort
     * them in order of reloid and block number then apply them together
     * page-wise. Repeat this until we get invalid undo record pointer.
     */
     
    int preRetCode = 0;
    Oid preReloid = InvalidOid;
    Oid prePartitionoid = InvalidOid;
    do {
        bool containsFullChain = false;
        int startIndex = 0;
        Oid currReloid = InvalidOid;
        Oid currPartitionoid = InvalidOid;
        Oid currRelfilenode = InvalidOid;
        Oid currTablespace = InvalidOid;
        BlockNumber currBlkno = InvalidBlockNumber;

        /*
         * If urecPtr is not valid means we have complete all undo actions
         * for this transaction, otherwise we need to fetch the next batch of
         * the undo records.
         */
        if (!IS_VALID_UNDO_REC_PTR(urecPtr))
            break;

        /*
         * Fetch multiple undo record in bulk.
         * This will return a URecVector which contains an array of UndoRecords.
         */
        URecVector *urecvec = FetchUndoRecordRange(&urecPtr, toUrecptr, undoApplySize, false);
        if (urecvec->Size() == 0)
            break;

        if (isTopTxn && !IS_VALID_UNDO_REC_PTR(urecPtr)) {
            containsFullChain = true;
        } else {
            containsFullChain = false;
        }
        /*
         * Sort the URecVector by block number so can apply multiple undo
         * action per block.
         */
        urecvec->SortByBlkNo();
        int i = 0;
        for (i = 0; i < urecvec->Size(); i++) {
            UndoRecord *uur = (*urecvec)[i];

            if (currRelfilenode != InvalidOid && (currTablespace != uur->Tablespace() ||
                currRelfilenode != uur->Relfilenode() || currBlkno != uur->Blkno())) {
                preRetCode = RmgrTable[RM_UHEAP_ID].rm_undo(urecvec, startIndex, i - 1, fullXid,
                    currReloid, currPartitionoid, currBlkno, containsFullChain,
                    preRetCode, &preReloid, &prePartitionoid);
                startIndex = i;
            }
            currReloid = uur->Reloid();
            currPartitionoid = uur->Partitionoid();
            currBlkno = uur->Blkno();
            currRelfilenode = uur->Relfilenode();
            currTablespace = uur->Tablespace();
        }

        /* Apply the remaining ones */
        preRetCode = RmgrTable[RM_UHEAP_ID].rm_undo(urecvec, startIndex, i - 1, fullXid, currReloid, currPartitionoid,
            currBlkno, containsFullChain, preRetCode, &preReloid, &prePartitionoid);

        DELETE_EX(urecvec);
    } while (true);

    if (isTopTxn) {
        undo::UpdateRollbackFinish(slotPtr);
    }
}

/*
 * ExecuteUndoActionsPage
 *
 * This is similar to ExecuteUndoActions but wont rollback the entire the
 * transaction but will only rollback all changes made by fxid on the given
 * buffer.
 *
 * fromUrp  - UndoRecPtr from where to start applying the undo.
 * rel      - relation descriptor for which undo to be applied.
 * buffer   - buffer for which undo to be processed.
 * xid     - aborted transaction id whose effects needs to be reverted.
 */
void ExecuteUndoActionsPage(UndoRecPtr fromUrp, Relation rel, Buffer buffer, TransactionId xid)
{
    UndoRecPtr urp = fromUrp;
    int undoApplySize = GetUndoApplySize();

    /*
     * Fetch the multiple undo records which can fit into undoApplySize;
     * then apply them together.
     */
    int preRetCode = 0;
    Oid preReloid = InvalidOid;
    Oid prePartitionoid = InvalidOid;
    do {
        if (!IS_VALID_UNDO_REC_PTR(urp))
            break;

        URecVector *urecvec =
            FetchUndoRecordRange(&urp, INVALID_UNDO_REC_PTR, undoApplySize, true);

        if (urecvec->Size() == 0) {
            break;
        }

        Oid relOid = RelationIsPartition(rel) ? GetBaseRelOidOfParition(rel) : RelationGetRelid(rel);
        Oid partitionOid = RelationIsPartition(rel) ? RelationGetRelid(rel) : InvalidOid;

        preRetCode = RmgrTable[RM_UHEAP_ID].rm_undo(urecvec, 0, urecvec->Size() - 1, xid, relOid, partitionOid,
            BufferGetBlockNumber(buffer), IS_VALID_UNDO_REC_PTR(urp) ? false : true,
            preRetCode, &preReloid, &prePartitionoid);

        DELETE_EX(urecvec);
    } while (true);

    /*
     * Clear the transaction id from the slot.  We expect that if the undo
     * actions are applied by execute_undo_actions_page then it would have
     * cleared the xid, otherwise we will clear it here.
     */
    UndoRecPtr slotUrecPtr = INVALID_UNDO_REC_PTR;
    LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
    Page page = BufferGetPage(buffer);
    int tdSlotId = UHeapPageGetTDSlotId(buffer, xid, &slotUrecPtr);
    /*
     * If someone has already cleared the transaction info, then we don't
     * need to do anything.
     */
    if (tdSlotId != InvalidTDSlotId) {
        START_CRIT_SECTION();
        ereport(PANIC, (errmodule(MOD_USTORE),
            errmsg("Repeatedly set transaction information in tdSlot, "
            "reloid(%u), partoid(%u), xid(%lu), block(%u), tdSlot(%d), fromurp(%lu), sloturp(%lu)",
            RelationIsPartition(rel) ? GetBaseRelOidOfParition(rel) : RelationGetRelid(rel),
            RelationIsPartition(rel) ? RelationGetRelid(rel) : InvalidOid, xid, BufferGetBlockNumber(buffer),
            tdSlotId, fromUrp, slotUrecPtr)));

        /* Clear the xid from the slot */
        UHeapPageSetUndo(buffer, tdSlotId, InvalidTransactionId, slotUrecPtr);

        MarkBufferDirty(buffer);

        if (RelationNeedsWAL(rel)) {
            DECLARE_NODE_COUNT();
            int zid = (int)UNDO_PTR_GET_ZONE_ID(fromUrp);

            XlUHeapUndoResetSlot xlrec;
            xlrec.urec_ptr = slotUrecPtr;
            xlrec.td_slot_id = tdSlotId;
            xlrec.zone_id = zid;

            XLogBeginInsert();

            XLogRegisterData((char *)&xlrec, SizeOfUHeapUndoResetSlot);
            XLogRegisterBuffer(0, buffer, REGBUF_STANDARD);

            XLogRecPtr recptr = XLogInsert(RM_UHEAPUNDO_ID, XLOG_UHEAPUNDO_RESET_SLOT);
            PageSetLSN(page, recptr);
        }

        END_CRIT_SECTION();
    }

    UPageVerifyParams verifyParams;
        if (unlikely(ConstructUstoreVerifyParam(USTORE_VERIFY_MOD_UPAGE, USTORE_VERIFY_COMPLETE,
            (char *) &verifyParams, rel, page, BufferGetBlockNumber(buffer), NULL, NULL, InvalidXLogRecPtr))) {
            ExecuteUstoreVerify(USTORE_VERIFY_MOD_UPAGE, (char *) &verifyParams);
    }
    LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
}


int UHeapUndoActions(URecVector *urecvec, int startIdx, int endIdx, TransactionId xid, Oid reloid, Oid partitionoid,
    BlockNumber blkno, bool isFullChain, int preRetCode, Oid *preReloid, Oid *prePartitionoid)
{
    if (preReloid != NULL && prePartitionoid != NULL) {
        if (*preReloid == reloid && *prePartitionoid == partitionoid && preRetCode == ROLLBACK_OK_NOEXIST) {
            return ROLLBACK_OK_NOEXIST;
        }
        *preReloid = reloid;
        *prePartitionoid = partitionoid;
    }

    UndoRelationData relationData = { 0 };

    UndoRecord *firstUndoRecord = (*urecvec)[startIdx];
    UndoRecord *lastUndoRecord = (*urecvec)[endIdx];
    Oid relfilenode = firstUndoRecord->Relfilenode();
    Oid tablespace = firstUndoRecord->Tablespace();

    Assert(relfilenode == lastUndoRecord->Relfilenode());
    Assert(tablespace == lastUndoRecord->Tablespace());

    WHITEBOX_TEST_STUB(UHEAP_UNDO_ACTION_FAILED, WhiteboxDefaultErrorEmit);

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
    
    Buffer buffer = ReadBuffer(relationData.relation, blkno);
    LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
    Page page = BufferGetPage(buffer);

    /*
     * If undo action has been already applied for this page then skip the
     * process altogether.  If we didn't find a slot corresponding to xid, we
     * consider the transaction is already rolled back.
     *
     * The logno of slot's undo record pointer must be same as the logno of
     * undo record to be applied.
     */
    UndoRecPtr slotUrecPtr = INVALID_UNDO_REC_PTR;
    int tdSlotId = UHeapPageGetTDSlotId(buffer, xid, &slotUrecPtr);

    UndoRecPtr firstUrp = firstUndoRecord->Urp();
    UndoRecPtr slotPrevUrp = lastUndoRecord->Blkprev();
    if (tdSlotId == InvalidTDSlotId ||
        (UNDO_PTR_GET_ZONE_ID(slotUrecPtr) != UNDO_PTR_GET_ZONE_ID(firstUrp)) ||
        (UNDO_PTR_GET_ZONE_ID(slotUrecPtr) == UNDO_PTR_GET_ZONE_ID(slotPrevUrp) &&
        slotUrecPtr <= slotPrevUrp)) {
        UnlockReleaseBuffer(buffer);
        /* Close the relation. */
        UHeapUndoActionsCloseRelation(&relationData);
        return ROLLBACK_ERR;
    }

    START_CRIT_SECTION();
    UndoRecPtr prevUrp = slotUrecPtr;
    bool needPageInit = false;
    OffsetNumber xlogMinLPOffset = MaxOffsetNumber;
    OffsetNumber xlogMaxLPOffset = InvalidOffsetNumber;
    Offset xlogCopyStartOffset = BLCKSZ;
    Offset xlogCopyEndOffset = 0;

    for (int i = startIdx; i <= endIdx; i++) {
        UndoRecord *undorecord = (*urecvec)[i];
        uint8 undotype = undorecord->Utype();

        /*
         * If the current UndoRecPtr on the slot is less than the
         * UndoRecPtr of the current undorecord, then it means this undorecord
         * has been applied.
         */

        if (prevUrp == 0) {
            slotPrevUrp = 0;
            break;
        }

        if (prevUrp != undorecord->Urp()) {
            slotPrevUrp = prevUrp;
            continue;
        }

        Assert(prevUrp == undorecord->Urp());
        prevUrp = undorecord->Blkprev();
        slotPrevUrp = prevUrp;

        TransactionId oldestXid PG_USED_FOR_ASSERTS_ONLY =
            pg_atomic_read_u64(&g_instance.undo_cxt.globalRecycleXid);

        /* Either globalRecycleXid is zero or it is always <= than aborting xid */
        Assert(!TransactionIdIsValid(oldestXid) || TransactionIdPrecedesOrEquals(
            pg_atomic_read_u64(&g_instance.undo_cxt.globalRecycleXid), undorecord->Xid()));

        /* Store the minimum and maximum LP offsets of all undorecords */
        if (undorecord->Offset() > InvalidOffsetNumber && undorecord->Offset() < xlogMinLPOffset) {
            xlogMinLPOffset = undorecord->Offset();
        }
        if (undorecord->Offset() <= MaxOffsetNumber && undorecord->Offset() > xlogMaxLPOffset) {
            xlogMaxLPOffset = undorecord->Offset();
        }

        switch (undotype) {
            case UNDO_INSERT: {
                ExecuteUndoForInsert(relationData.relation, buffer, undorecord->Offset(), undorecord->Xid());

                int nline = UHeapPageGetMaxOffsetNumber(page);
                needPageInit = true;
                for (int offset = FirstOffsetNumber; offset <= nline; offset++) {
                    RowPtr *rp = UPageGetRowPtr(page, offset);
                    if (RowPtrIsUsed(rp)) {
                        needPageInit = false;
                        break;
                    }
                }

                break;
            }
            case UNDO_MULTI_INSERT: {
                OffsetNumber start_offset;
                OffsetNumber end_offset;
                OffsetNumber iter_offset;
                int i, nline;

                Assert(undorecord->Rawdata() != NULL);
                start_offset = ((OffsetNumber *)undorecord->Rawdata()->data)[0];
                end_offset = ((OffsetNumber *)undorecord->Rawdata()->data)[1];

                /* Store the minimum and maximum LP offsets */
                if (start_offset < xlogMinLPOffset) {
                    Assert(start_offset > InvalidOffsetNumber);
                    xlogMinLPOffset = start_offset;
                }
                if (end_offset > xlogMaxLPOffset) {
                    Assert(end_offset <= MaxOffsetNumber);
                    xlogMaxLPOffset = end_offset;
                }

                for (iter_offset = start_offset; iter_offset <= end_offset; iter_offset++) {
                    ExecuteUndoForInsert(relationData.relation, buffer, iter_offset, undorecord->Xid());
                }

                nline = UHeapPageGetMaxOffsetNumber(page);
                needPageInit = true;
                for (i = FirstOffsetNumber; i <= nline; i++) {
                    RowPtr *rp = UPageGetRowPtr(page, i);
                    if (RowPtrIsUsed(rp)) {
                        needPageInit = false;
                        break;
                    }
                }

                break;
            }

            case UNDO_DELETE:
            case UNDO_UPDATE:
            case UNDO_INPLACE_UPDATE: {
                UHeapDiskTuple tuple = CopyTupleFromUndoRecord(undorecord, buffer);
                RestoreXactFromUndoRecord(undorecord, buffer, tuple);

                /* Store the page offsets where we start and end updating tuples */
                RowPtr *rp = UPageGetRowPtr(page, undorecord->Offset());
                if (rp->offset < xlogCopyStartOffset) {
                    Assert(rp->offset > SizeOfUHeapPageHeaderData); 
                    xlogCopyStartOffset = rp->offset;
                }
                if (rp->offset + rp->len > xlogCopyEndOffset) {
                    Assert(rp->offset + rp->len <= BLCKSZ);
                    xlogCopyEndOffset = rp->offset + rp->len;
                }

                break;
            }
            case UNDO_ITEMID_UNUSED:
            default:
                ereport(PANIC, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE), errmsg("Unsupported Rollback Action")));
        }
    }

    /*
     * If this is the first undo record created by this top transaction xid,
     * then it means we hit the last of the undo chain
     * (because we rollback from last undo record created to the first one).
     * So set the TD xid to InvalidTransactionId.
     * It may not be the first undo record created by this xid,
     * but it could be the first one it created on this block.
     * In that case, isFullChain is false, but we also need to reset
     * TD xid to InvalidTransactionId to indicate that the
     * "rollback of this xid on this block is complete."
     */
    if (slotPrevUrp != lastUndoRecord->Blkprev()) {
        ereport(LOG, (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("rollback xid %lu, oid %u, blk %u. slotPrevUrp %lu != lastBlkprev %lu, "
            "check whether subtransactions have been rolled back.",
            lastUndoRecord->Xid(), lastUndoRecord->Reloid(), lastUndoRecord->Blkno(),
            slotPrevUrp, lastUndoRecord->Blkprev())));
    }
    if (isFullChain || !IS_VALID_UNDO_REC_PTR(slotPrevUrp)) {
        xid = InvalidTransactionId;
    } else if (IS_VALID_UNDO_REC_PTR(slotPrevUrp)) {
        VerifyMemoryContext();
        UndoRecord *urec = New(CurrentMemoryContext)UndoRecord();
        urec->SetUrp(slotPrevUrp);
        urec->SetMemoryContext(CurrentMemoryContext);
        UndoTraversalState rc =
            FetchUndoRecord(urec, NULL, InvalidBlockNumber, InvalidOffsetNumber, InvalidTransactionId,
            false, NULL);
        if (rc != UNDO_TRAVERSAL_COMPLETE || urec->Xid() != xid) {
            xid = InvalidTransactionId;
        }
        DELETE_EX(urec);
    }

    UHeapPageSetUndo(buffer, tdSlotId, xid, slotPrevUrp);

    MarkBufferDirty(buffer);

    if (RelationNeedsWAL(relationData.relation)) {
        UHeapUndoActionWALInfo walInfo;
        UHeapPageHeaderData *phdr = (UHeapPageHeaderData *)page;

        walInfo.buffer = buffer;
        walInfo.xlogMinLPOffset = xlogMinLPOffset;
        walInfo.xlogMaxLPOffset = xlogMaxLPOffset;
        walInfo.xlogCopyStartOffset = xlogCopyStartOffset;
        walInfo.xlogCopyEndOffset = xlogCopyEndOffset;
        walInfo.tdSlotId = tdSlotId;
        walInfo.xid = xid;
        walInfo.slotPrevUrp = slotPrevUrp;
        walInfo.needInit = needPageInit;

        /*
         * NOTE: If more page headers are updated by rollback,
         * they should be added to this list
         */
        walInfo.pd_flags = phdr->pd_flags;
        walInfo.potential_freespace = phdr->potential_freespace;
        walInfo.pd_prune_xid = phdr->pd_prune_xid;

        LogUHeapUndoActions(&walInfo, relationData.relation);
    }

    if (needPageInit) {
        XLogRecPtr lsn = PageGetLSN(page);

        if (relationData.relation->rd_rel->relkind == RELKIND_TOASTVALUE) {
            UPageInit<UPAGE_TOAST>(page, BufferGetPageSize(buffer), UHEAP_SPECIAL_SIZE);
        } else {
            UPageInit<UPAGE_HEAP>(page, BufferGetPageSize(buffer),
                              UHEAP_SPECIAL_SIZE, RelationGetInitTd(relationData.relation));
        }
        PageSetLSN(page, lsn);

        UHeapPageHeaderData *uheappage = (UHeapPageHeaderData *)page;
        uheappage->pd_xid_base = u_sess->utils_cxt.RecentXmin - FirstNormalTransactionId;
        uheappage->pd_multi_base = 0;
    }

    END_CRIT_SECTION();
    UPageVerifyParams verifyParams;
    if (unlikely(ConstructUstoreVerifyParam(USTORE_VERIFY_MOD_UPAGE, USTORE_VERIFY_COMPLETE,
        (char *) &verifyParams, relationData.relation, page, blkno, NULL, NULL, InvalidXLogRecPtr))) {
        ExecuteUstoreVerify(USTORE_VERIFY_MOD_UPAGE, (char *) &verifyParams);
    }

    UnlockReleaseBuffer(buffer);

    /* Close the relation. */
    UHeapUndoActionsCloseRelation(&relationData);

    return ROLLBACK_OK;
}

/*
 * ExecuteUndoForInsert
 *
 * UndoRecords do not have the tuple data. In order to "rollback"
 * inserts, we just need to mark the Line Pointer (LP) as invalid.
 * ie: if the Relation has an index, then we mark the LP as DEAD so
 * it is invalid but cant be reused yet. Otherwise, we can mark
 * the LP as UNUSED.
 * Note: DEAD LPs are marked UNUSED when there are no more index entries
 * pointing to the LP. This is done during vacuum.
 */
void ExecuteUndoForInsert(Relation rel, Buffer buffer, OffsetNumber off, TransactionId xid)
{
    Page page = BufferGetPage(buffer);
    RowPtr *rp = UPageGetRowPtr(page, off);

    /* Rollback insert - increment the potential space for the Page */
    UHeapRecordPotentialFreeSpace(buffer, SHORTALIGN(rp->len));

    if (RelationGetForm(rel)->relhasindex) {
        RowPtrSetDead(rp);
    } else {
        RowPtrSetUnused(rp);
        PageSetHasFreeLinePointers(page);
    }

    UPageSetPrunable(page, xid);
}

/*
 * ExecuteUndoForInsertRecovery
 *
 * UndoRecords do not have the tuple data. In order to "rollback"
 * inserts, we just need to mark the Line Pointer (LP) as invalid.
 * ie: if the Relation has an index, then we mark the LP as DEAD so
 * it is invalid but cant be reused yet. Otherwise, we can mark
 * the LP as UNUSED.
 * Note: DEAD LPs are marked UNUSED when there are no more index entries
 * pointing to the LP. This is done during vacuum.
 */
void ExecuteUndoForInsertRecovery(Buffer buffer, OffsetNumber off, TransactionId xid, bool relhasindex, int *tdid)
{
    Page page = BufferGetPage(buffer);
    RowPtr *rp = UPageGetRowPtr(page, off);

    Assert(RowPtrIsNormal(rp));

    UHeapDiskTuple diskTuple = (UHeapDiskTuple)UPageGetRowData(page, rp);

    *tdid = UHeapTupleHeaderGetTDSlot(diskTuple);

    /* Rollback insert - increment the potential space for the Page */
    UHeapRecordPotentialFreeSpace(buffer, SHORTALIGN(rp->len));

    if (relhasindex) {
        RowPtrSetDead(rp);
    } else {
        RowPtrSetUnused(rp);
        PageSetHasFreeLinePointers(page);
    }

    UPageSetPrunable(page, xid);
}

/*
 * CopyTupleFromUndoRecord
 * undorecord should contain the entire tuple and the offset on the page it
 * came from.
 */
static UHeapDiskTuple CopyTupleFromUndoRecord(UndoRecord *undorecord, Buffer buffer)
{
    Assert(undorecord != NULL);

    OffsetNumber offnum = undorecord->Offset();
    Assert(offnum != InvalidOffsetNumber);

    Page page = BufferGetPage(buffer);
    RowPtr *rp = UPageGetRowPtr(page, offnum);
    if ((offnum == InvalidOffsetNumber) || !RowPtrIsNormal(rp)) {
        ereport(PANIC, (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("undorecord %lu xid %lu undotype %u need rollback but rp invalid: "
            "reloid %u, blk %u, offnum %u, rpflag %u, rpoffset %u, rplen %u.",
            undorecord->Urp(), undorecord->Xid(), undorecord->Utype(), undorecord->Reloid(),
            undorecord->Blkno(), offnum, rp->flags, rp->offset, rp->len)));
    }

    UHeapDiskTuple diskTuple = (UHeapDiskTuple)UPageGetRowData(page, rp);

    uint8 undoType = undorecord->Utype();
    switch (undoType) {
        case UNDO_DELETE:
        case UNDO_UPDATE: {
            StringInfoData *undoData = undorecord->Rawdata();
            Assert(undoData != NULL);
            Assert(undoData->len > 0);

            /*
             * The Rawdata of UNDO_UPDATE contains not just the tuple
             * but also the new tuple's CTID.
             */
            uint32 extraDataLen = (undoType == UNDO_UPDATE) ? sizeof(ItemPointerData) : 0;
            if (undorecord->ContainSubXact()) {
                extraDataLen += sizeof(SubTransactionId);
            }

            uint32 undoTupleLen = undoData->len - extraDataLen;
            uint32 newTupleLength = undoTupleLen + OffsetTdId;
            RowPtrChangeLen(rp, newTupleLength);

            errno_t rc = memcpy_s((char*)diskTuple + OffsetTdId, undoTupleLen, undoData->data, undoTupleLen);
            securec_check(rc, "", "");
            diskTuple->xid = (ShortTransactionId)FrozenTransactionId;

            /* Rollback delete/update - decrement the potential space for the Page */
            UHeapRecordPotentialFreeSpace(buffer, -1 * SHORTALIGN(newTupleLength));

            break;
        }
        case UNDO_INPLACE_UPDATE: {
            StringInfoData *undoData = undorecord->Rawdata();
            Assert(undoData != NULL);
            Assert(undoData->len > 0);
            uint16 *prefixlen_ptr;
            uint16 *suffixlen_ptr;
            uint8 *t_hoff_ptr;
            uint8 *flags_ptr;
            uint16 prefixlen = 0;
            uint16 suffixlen = 0;
            uint8 t_hoff = 0;
            uint8 flags = 0;
            char *cur_undodata_ptr = NULL;
            int read_size = 0;
            int subxid_size = undorecord->ContainSubXact() ? sizeof(SubTransactionId) : 0;
            errno_t rc;

            t_hoff_ptr = (uint8 *)undoData->data;
            t_hoff = *t_hoff_ptr;
            cur_undodata_ptr = undoData->data + sizeof(uint8) + t_hoff - OffsetTdId;
            read_size = sizeof(uint8) + t_hoff - OffsetTdId;

            flags_ptr = (uint8 *)cur_undodata_ptr;
            flags = *flags_ptr;
            cur_undodata_ptr += sizeof(uint8);
            read_size += sizeof(uint8);

            if (flags & UREC_INPLACE_UPDATE_XOR_PREFIX) {
                prefixlen_ptr = (uint16 *)(cur_undodata_ptr);
                cur_undodata_ptr += sizeof(uint16);
                read_size += sizeof(uint16);
                prefixlen = *prefixlen_ptr;
            }

            if (flags & UREC_INPLACE_UPDATE_XOR_SUFFIX) {
                suffixlen_ptr = (uint16 *)(cur_undodata_ptr);
                cur_undodata_ptr += sizeof(uint16);
                read_size += sizeof(uint16);
                suffixlen = *suffixlen_ptr;
            }

            char *cur_old_disktuple_ptr = NULL;
            char *old_disktuple =
                (char *)palloc(undoData->len - read_size - subxid_size + prefixlen + suffixlen + t_hoff);
            rc = memcpy_s(old_disktuple + OffsetTdId, t_hoff - OffsetTdId, undoData->data + sizeof(uint8),
                t_hoff - OffsetTdId);
            securec_check(rc, "", "");
            ((UHeapDiskTupleData*)old_disktuple)->xid = (ShortTransactionId)FrozenTransactionId;
            cur_old_disktuple_ptr = old_disktuple + t_hoff;

            /* copy the perfix to old_disktuple */
            if (flags & UREC_INPLACE_UPDATE_XOR_PREFIX) {
                rc = memcpy_s(cur_old_disktuple_ptr, prefixlen, (char *)diskTuple + diskTuple->t_hoff, prefixlen);
                securec_check(rc, "", "");
                cur_old_disktuple_ptr += prefixlen;
            }

            char *newp = (char *)diskTuple + diskTuple->t_hoff + prefixlen;
            char *cur_undo_data_p = undoData->data + read_size;
            int oldlen = undoData->len - read_size - subxid_size + prefixlen + suffixlen;
            int newlen = rp->len - diskTuple->t_hoff;
            int oldDataLen = oldlen - prefixlen - suffixlen;

            if (oldDataLen > 0) {
                rc = memcpy_s(cur_old_disktuple_ptr, oldDataLen, cur_undo_data_p, oldDataLen);
                securec_check(rc, "", "");
                cur_old_disktuple_ptr += (oldDataLen);
            }

            if (flags & UREC_INPLACE_UPDATE_XOR_SUFFIX) {
                rc = memcpy_s(cur_old_disktuple_ptr, suffixlen, newp + newlen - prefixlen - suffixlen, suffixlen);
                securec_check(rc, "", "");
            }
            uint32 newTupleLength = oldlen + t_hoff;
            RowPtrChangeLen(rp, newTupleLength);

            rc = memcpy_s(diskTuple, newTupleLength, old_disktuple, newTupleLength);
            securec_check(rc, "", "");
            pfree(old_disktuple);
            break;
        }
        case UNDO_ITEMID_UNUSED:
        case UNDO_INSERT:
        case UNDO_MULTI_INSERT: {
            ereport(PANIC, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                errmsg("invalid undo record type for restoring tuple.")));
            break;
        }
        default:
            ereport(PANIC, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE), errmsg("Unsupported Rollback Action")));
    }

    return diskTuple;
}


/*
 * RestoreXactFromUndoRecord - restore xact related information on the tuple
 *
 * It checks whether we can mark the tuple as frozen based on certain
 * conditions. It also sets the invalid xact flag on the tuple when previous
 * xid from the undo record doesn't match with the slot xid.
 */
static void RestoreXactFromUndoRecord(UndoRecord *undorecord, Buffer buffer, UHeapDiskTuple tuple)
{
    Assert(undorecord);

    TransactionId prevXid = undorecord->OldXactId();

    UHeapTupleTransInfo tdinfo;
    int tdSlot = UHeapTupleHeaderGetTDSlot(tuple);
    GetTDSlotInfo(buffer, tdSlot, &tdinfo);

    if (TransactionIdEquals(prevXid, FrozenTransactionId) || tdinfo.td_slot == UHEAPTUP_SLOT_FROZEN) {
        UHeapTupleHeaderSetTDSlot(tuple, UHEAPTUP_SLOT_FROZEN);
    } else if (prevXid != tdinfo.xid) {
        tuple->flag |= UHEAP_INVALID_XACT_SLOT;
    }
}

int UpdateTupleHeaderFromUndoRecord(UndoRecord *urec, UHeapDiskTuple diskTuple, Page page)
{
    Assert(urec != NULL);

    if (urec->Utype() == UNDO_INSERT || urec->Utype() == UNDO_MULTI_INSERT) {
        /*
         * Ensure to clear the visibility related information from the tuple.
         * This is required for the cases where the passed in tuple has lock
         * only flags set on it.
         */
        diskTuple->flag &= ~UHEAP_VIS_STATUS_MASK;
        diskTuple->xid = (ShortTransactionId)FrozenTransactionId;
    } else if (urec->Utype() == UNDO_INPLACE_UPDATE) {
        Assert(urec->Rawdata() != NULL);
        Assert(urec->Rawdata()->len >= (int)SizeOfUHeapDiskTupleData);
        errno_t rc = memcpy_s((char *)diskTuple + OffsetTdId, SizeOfUHeapDiskTupleHeaderExceptXid,
            urec->Rawdata()->data + sizeof(uint8), SizeOfUHeapDiskTupleHeaderExceptXid);
        securec_check(rc, "", "");
        diskTuple->xid = (ShortTransactionId)FrozenTransactionId;
    } else {
        Assert(urec->Rawdata() != NULL);
        Assert(urec->Rawdata()->len >= (int)SizeOfUHeapDiskTupleHeaderExceptXid);
        errno_t rc = memcpy_s(((char *)diskTuple + OffsetTdId), SizeOfUHeapDiskTupleHeaderExceptXid,
            urec->Rawdata()->data, SizeOfUHeapDiskTupleHeaderExceptXid);
        securec_check(rc, "", "");
        diskTuple->xid = (ShortTransactionId)FrozenTransactionId;
    }

    return UHeapTupleHeaderGetTDSlot(diskTuple);
}

static void LogUHeapUndoActions(UHeapUndoActionWALInfo *walInfo, Relation rel)
{
    uint8 flags = 0;
    Page page = BufferGetPage(walInfo->buffer);

    if (walInfo->needInit) {
        flags |= XLU_INIT_PAGE;

        if (rel->rd_rel->relkind == RELKIND_TOASTVALUE) {
            flags |= XLOG_UHEAP_INIT_TOAST_PAGE;
        }
    }

    if (walInfo->potential_freespace > BLCKSZ) {
        ereport(PANIC, (errmodule(MOD_USTORE), errmsg(
            "Invalid potential freespace(%u)", walInfo->potential_freespace)));
    }

    XLogBeginInsert();
    XLogRegisterData((char *)&flags, sizeof(uint8));
    UHeapPageHeaderData *uheappage = (UHeapPageHeaderData *)page;
    if (walInfo->needInit) {
        XLogRegisterData((char *)&uheappage->pd_xid_base, sizeof(TransactionId));
        XLogRegisterData((char *)&uheappage->td_count, sizeof(uint16));
    } else {
        /* Check the fields written to the xlog in advance */
        if (walInfo->xlogMinLPOffset <= (OffsetNumber) InvalidOffsetNumber ||
            walInfo->xlogMaxLPOffset >= (OffsetNumber) MaxOffsetNumber) {
            ereport(PANIC, (errmodule(MOD_USTORE), errmsg(
                "Invalid lp offsetnumber, min(%u), max(%u).", walInfo->xlogMinLPOffset, walInfo->xlogMaxLPOffset)));
        }
        if (walInfo->tdSlotId == InvalidTDSlotId || walInfo->tdSlotId > uheappage->td_count) {
            ereport(PANIC, (errmodule(MOD_USTORE), errmsg("Invalid rollback tdSlot(%d).", walInfo->tdSlotId)));
        }
        if (walInfo->xlogCopyStartOffset <= (Offset) (SizeOfUHeapPageHeaderData + SizeOfUHeapTDData(uheappage)) ||
            walInfo->xlogCopyEndOffset < (Offset) 0 ||
            walInfo->xlogCopyStartOffset > (Offset) BLCKSZ ||
            walInfo->xlogCopyEndOffset > (Offset) BLCKSZ) {
            ereport(PANIC, (errmodule(MOD_USTORE),
                errmsg("Invalid rollback start and end offset in page, (start, end)=(%d, %d).",
                walInfo->xlogCopyStartOffset,
                walInfo->xlogCopyEndOffset)));
        }

        /* Store updated line pointers data */
        XLogRegisterData((char *)&walInfo->xlogMinLPOffset, sizeof(OffsetNumber));
        XLogRegisterData((char *)&walInfo->xlogMaxLPOffset, sizeof(OffsetNumber));

        if (walInfo->xlogMaxLPOffset >= walInfo->xlogMinLPOffset) {
            size_t lpSize = (walInfo->xlogMaxLPOffset - walInfo->xlogMinLPOffset + 1) * sizeof(RowPtr);
            XLogRegisterData((char *)UPageGetRowPtr(page, walInfo->xlogMinLPOffset), lpSize);

            /* Store updated tuples data */
            XLogRegisterData((char *)&walInfo->xlogCopyStartOffset, sizeof(Offset));
            XLogRegisterData((char *)&walInfo->xlogCopyEndOffset, sizeof(Offset));

            if (walInfo->xlogCopyEndOffset > walInfo->xlogCopyStartOffset) {
                size_t dataSize = walInfo->xlogCopyEndOffset - walInfo->xlogCopyStartOffset;
                XLogRegisterData((char *)page + walInfo->xlogCopyStartOffset, dataSize);
            }

            /* Store updated page headers */
            XLogRegisterData((char *)&walInfo->pd_prune_xid, sizeof(TransactionId));
            XLogRegisterData((char *)&walInfo->pd_flags, sizeof(uint16));
            XLogRegisterData((char *)&walInfo->potential_freespace, sizeof(uint16));
        }

        /* Store updated TD slot information */
        XLogRegisterData((char *)&walInfo->tdSlotId, sizeof(int));
        XLogRegisterData((char *)&walInfo->xid, sizeof(TransactionId));
        XLogRegisterData((char *)&walInfo->slotPrevUrp, sizeof(UndoRecPtr));
    }

    XLogRegisterBuffer(0, walInfo->buffer, REGBUF_STANDARD);

    XLogRecPtr recptr = XLogInsert(RM_UHEAPUNDO_ID, XLOG_UHEAPUNDO_PAGE);

    PageSetLSN(page, recptr);
}

/*
 * Open reloid and partitionid and save the Relation to relationData.
 * Return false if reloid / partitionid could not be opened.
*/
bool UHeapUndoActionsOpenRelation(Oid reloid, Oid partitionoid, UndoRelationData *relationData)
{
    relationData->rel = try_relation_open(reloid, RowExclusiveLock);
    if (relationData->rel == NULL) {
        return false;
    }

    if (RelationIsPartitioned(relationData->rel)) {
        relationData->isPartitioned = true;
        relationData->partition = tryPartitionOpen(relationData->rel, partitionoid, RowExclusiveLock);
        if (relationData->partition == NULL) {
            relation_close(relationData->rel, RowExclusiveLock);
            return false;
        }
        relationData->relation = partitionGetRelation(relationData->rel, relationData->partition);
    } else {
        relationData->relation = relationData->rel;
    }

    if (RELATION_IS_OTHER_TEMP(relationData->relation) && u_sess->catalog_cxt.myTempNamespace == InvalidOid &&
        u_sess->catalog_cxt.myTempToastNamespace == InvalidOid) {
        UHeapUndoActionsCloseRelation(relationData);
        ereport(DEBUG1, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
            errmsg("Modifications to temporary tables do not need to be rolled back after the session exits.")));
        return false;
    }
    return true;
}

void UHeapUndoActionsCloseRelation(UndoRelationData *relationData)
{
    if (relationData->isPartitioned) {
        releaseDummyRelation(&relationData->relation);
        partitionClose(relationData->rel, relationData->partition, RowExclusiveLock);
        relation_close(relationData->rel, RowExclusiveLock);
    } else {
        relation_close(relationData->relation, RowExclusiveLock);
    }

    relationData->rel = NULL;
    relationData->relation = NULL;
    relationData->partition = NULL;
    relationData->isPartitioned = false;
}

bool UHeapUndoActionsFindRelidByRelfilenode(RelFileNode *relfilenode, Oid *reloid, Oid *partitionoid)
{
    /* Find the relid mapped to the relfilenode data. 
     * On a partition table, this relid is the parent id, so we need to search
     * for the correct partitionoid after we get the parent id.
    */
    Oid partitionId = InvalidOid;
    Oid realRelid = RelidByRelfilenodeCache(relfilenode->spcNode, relfilenode->relNode);
    if (realRelid == InvalidOid) {
        realRelid = RelidByRelfilenode(relfilenode->spcNode, relfilenode->relNode, false);
    }
    if (realRelid == InvalidOid) {
        Oid partitionReltoastrelid = InvalidOid;
        realRelid = PartitionRelidByRelfilenodeCache(relfilenode->spcNode,
            relfilenode->relNode, partitionReltoastrelid);
        if (realRelid == InvalidOid) {
            realRelid = PartitionRelidByRelfilenode(relfilenode->spcNode,
                relfilenode->relNode, partitionReltoastrelid, NULL, false);
        }
        if (realRelid == InvalidOid) {
            return false;
        }

        /* Given the parent id, search which partition is using the relfilenode. */
        List *partitionsList = searchPgPartitionByParentId(PART_OBJ_TYPE_TABLE_PARTITION, realRelid);
        ListCell *cell = NULL;
        foreach (cell, partitionsList) {
            HeapTuple tuple = (HeapTuple)lfirst(cell);
            if (HeapTupleIsValid(tuple)) {
                Form_pg_partition rd_part = (Form_pg_partition)GETSTRUCT(tuple);
                /* A Partitioned table could have partitions on different tablespaces */
                if (rd_part->relfilenode == relfilenode->relNode &&
                    rd_part->reltablespace == relfilenode->spcNode) {
                    partitionId = HeapTupleGetOid(tuple);
                    break;
                }
            }
        }

        freePartList(partitionsList);

        if (partitionId == InvalidOid) {
            return false;
        }
    }

    *reloid = realRelid;
    *partitionoid = partitionId;
    return true;
}
