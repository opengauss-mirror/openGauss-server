/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * ubtdump.cpp
 *    Dump debug info for UBtree.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/ubtree/ubtdump.cpp
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/nbtree.h"
#include "access/subtrans.h"
#include "access/transam.h"
#include "access/ubtree.h"
#include "utils/builtins.h"
#include "storage/procarray.h" 


static void UBTreeVerifyTupleKey(Relation rel, Page page, BlockNumber blkno, OffsetNumber offnum,
    OffsetNumber firstPos, OffsetNumber lastPos);
static void UBTreeVerifyRowptrNonDML(Relation rel, Page page, BlockNumber blkno);
static void UBTreeVerifyHeader(PageHeaderData* page, Relation rel, BlockNumber blkno, uint16 pageSize, uint16 headerSize);
static void UBTreeVerifyRowptr(PageHeaderData* header, Page page, BlockNumber blkno, OffsetNumber offset,
    ItemIdSort indexSortPtr, const char *indexName, Relation rel);

void UBTreeVerifyIndex(Relation rel, TupleDesc *tupDesc, Tuplestorestate *tupstore, uint32 cols)
{
    uint32 errVerified = 0;
    UBTPageOpaqueInternal opaque = NULL;
    BTScanInsert cmpKeys = UBTreeMakeScanKey(rel, NULL);
    Buffer buf = UBTreeGetRoot(rel, BT_READ);
    if (!BufferIsValid(buf)) {
        pfree(cmpKeys);
        return; /* empty index */
    }
    /* find the left most leaf page */
    while (true) {
        Page page = BufferGetPage(buf);
        opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);
        if (P_ISLEAF(opaque)) {
            break; /* it's a leaf page, we are done */
        }
        OffsetNumber offnum = P_FIRSTDATAKEY(opaque);
        ItemId itemid = PageGetItemId(page, offnum);
        IndexTuple itup = (IndexTuple) PageGetItem(page, itemid);
        BlockNumber blkno = UBTreeTupleGetDownLink(itup);
        /* drop the read lock on the parent page, acquire one on the child */
        buf = _bt_relandgetbuf(rel, buf, blkno, BT_READ);
    }
    /* we got a leaf page, but now sure it's the left most page */
    while (!P_LEFTMOST(opaque)) {
        BlockNumber blkno = opaque->btpo_prev;
        buf = _bt_relandgetbuf(rel, buf, blkno, BT_READ);
        Page page = BufferGetPage(buf);
        opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);
    }
    /* now we can scan over the whole tree to verify each page */
    IndexTuple prevHikey = NULL;
    while (true) {
        Page page = BufferGetPage(buf);
        opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);
        BlockNumber blkno = opaque->btpo_next;
        if (P_IGNORE(opaque)) {
            buf = _bt_relandgetbuf(rel, buf, blkno, BT_READ);
            continue;
        }
        int erroCode = UBTreeVerifyOnePage(rel, page, cmpKeys, prevHikey);
        if (erroCode != VERIFY_NORMAL) {
            UBTreeVerifyRecordOutput(VERIFY_MAIN_PAGE, BufferGetBlockNumber(buf), erroCode, tupDesc, tupstore, cols);
            errVerified++;
        }
        if (P_RIGHTMOST(opaque) || P_LEFTMOST(opaque)) {
            break;
        }
        prevHikey = (IndexTuple)PageGetItem(page, PageGetItemId(page, P_HIKEY));
        buf = _bt_relandgetbuf(rel, buf, blkno, BT_READ);
    }
    _bt_relbuf(rel, buf);
    pfree(cmpKeys);
    /* last, we need to verify the recycle queue */
    errVerified += UBTreeVerifyRecycleQueue(rel, tupDesc, tupstore, cols);
    /* every page is ok , output normal state */
    if (errVerified == 0) {
        UBTreeVerifyRecordOutput(VERIFY_MAIN_PAGE, 0, VERIFY_NORMAL, tupDesc, tupstore, cols);
    }
}

void UBTreeVerifyRecordOutput(uint blkType, BlockNumber blkno, int errorCode,
    TupleDesc *tupDesc, Tuplestorestate *tupstore, uint32 cols)
{
    Assert(cols == UBTREE_VERIFY_OUTPUT_PARAM_CNT);
    bool nulls[cols] = {false};
    Datum values[cols];
    values[0] = CStringGetTextDatum(UBTGetVerifiedPageTypeStr(blkType));
    values[1] = UInt32GetDatum(blkno);
    values[UBTREE_VERIFY_OUTPUT_PARAM_CNT - 1] = CStringGetTextDatum(UBTGetVerifiedResultStr((uint32) errorCode));
    tuplestore_putvalues(tupstore, *tupDesc, values, nulls);
}

static Buffer StepNextRecyclePage(Relation rel, Buffer buf)
{
    Page page = BufferGetPage(buf);
    UBTRecycleQueueHeader header = GetRecycleQueueHeader(page, BufferGetBlockNumber(buf));
    BlockNumber nextBlkno = header->nextBlkno;
    Buffer nextBuf = ReadRecycleQueueBuffer(rel, nextBlkno);
    UnlockReleaseBuffer(buf);
    LockBuffer(nextBuf, BT_READ);
    return nextBuf;
}
uint32 UBTreeVerifyRecycleQueueFork(Relation rel, UBTRecycleForkNumber forkNum, TupleDesc *tupDesc,
    Tuplestorestate *tupstore, uint32 cols)
{
    uint32 errVerified = 0;
    RelationOpenSmgr(rel);
    BlockNumber urqBlocks = rel->rd_smgr->smgr_fsm_nblocks;
    BlockNumber forkMetaBlkno = forkNum;
    Buffer metaBuf = ReadRecycleQueueBuffer(rel, forkMetaBlkno);
    LockBuffer(metaBuf, BT_READ);
    UBTRecycleMeta metaData = (UBTRecycleMeta)PageGetContents(BufferGetPage(metaBuf));
    BlockNumber headBlkno = metaData->headBlkno;
    BlockNumber tailBlkno = metaData->tailBlkno;
    if (headBlkno > urqBlocks) {
        errVerified++;
        UBTreeVerifyRecordOutput(VERIFY_RECYCLE_QUEUE_PAGE, Int32GetDatum(forkMetaBlkno),
            VERIFY_RECYCLE_QUEUE_HEAD_ERROR, tupDesc, tupstore, cols);
    }
    if (tailBlkno > urqBlocks) {
        errVerified++;
        UBTreeVerifyRecordOutput(VERIFY_RECYCLE_QUEUE_PAGE, Int32GetDatum(forkMetaBlkno),
            VERIFY_RECYCLE_QUEUE_TAIL_ERROR, tupDesc, tupstore, cols);
    }
    BlockNumber nblocks = RelationGetNumberOfBlocks(rel);
    if (metaData->nblocksUpper > nblocks) {
        errVerified++;
        UBTreeVerifyRecordOutput(VERIFY_RECYCLE_QUEUE_PAGE, Int32GetDatum(forkMetaBlkno), VERIFY_INCONSISTENT_USED_PAGE,
            tupDesc, tupstore, cols);
    }

    UnlockReleaseBuffer(metaBuf);
    /* check that we can traverse from head to tail and back to head again */
    uint32 visitedPages = 0;
    bool tailVisited = false;
    Buffer buf = ReadRecycleQueueBuffer(rel, headBlkno);
    LockBuffer(buf, BT_READ);
    while (true) {
        if (BufferGetBlockNumber(buf) == tailBlkno) {
            tailVisited = true;
        }
        buf = StepNextRecyclePage(rel, buf);
        if (visitedPages++ > urqBlocks) {
            errVerified++;
            UBTreeVerifyRecordOutput(VERIFY_RECYCLE_QUEUE_PAGE, Int32GetDatum(forkMetaBlkno),
                VERIFY_RECYCLE_QUEUE_ENDLESS, tupDesc, tupstore, cols);
            UnlockReleaseBuffer(buf);
            return errVerified;
        }
        if (BufferGetBlockNumber(buf) == headBlkno) {
            break;
        }
    }

    UnlockReleaseBuffer(buf);
    if (!tailVisited) {
        errVerified++;
        UBTreeVerifyRecordOutput(VERIFY_RECYCLE_QUEUE_PAGE, Int32GetDatum(forkMetaBlkno),
            VERIFY_RECYCLE_QUEUE_TAIL_MISSED, tupDesc, tupstore, cols);
        return errVerified;
    }
    /* check that each entry and free list are well arranged */
    buf = ReadRecycleQueueBuffer(rel, headBlkno);
    LockBuffer(buf, BT_READ);
    Page page = BufferGetPage(buf);
    UBTRecycleQueueHeader header = GetRecycleQueueHeader(page, BufferGetBlockNumber(buf));
    /* follow the chain to find the Head page */
    while ((header->flags & URQ_HEAD_PAGE) == 0) {
        buf = StepNextRecyclePage(rel, buf);
        page = BufferGetPage(buf);
        header = GetRecycleQueueHeader(page, BufferGetBlockNumber(buf));
    }
    /* now we traverse the whole queue from the head page */
    while (true) {
        errVerified += UBTreeRecycleQueuePageDump(rel, buf, false, tupDesc, tupstore, cols);
        /* exit if current page is tail page*/
        if ((header->flags & URQ_TAIL_PAGE) != 0) {
            break;
        }
        /* move to the next page*/
        buf = StepNextRecyclePage(rel, buf);
        page = BufferGetPage(buf);
        header = GetRecycleQueueHeader(page, BufferGetBlockNumber(buf));
    }
    UnlockReleaseBuffer(buf);
    RelationCloseSmgr(rel);
    return errVerified;
}
    
uint32 UBTreeVerifyRecycleQueue(Relation rel, TupleDesc *tupDesc, Tuplestorestate *tupstore, uint32 cols)
{
    uint32 errVerified = 0;
    RelationOpenSmgr(rel);
    BlockNumber urqBlocks = rel->rd_smgr->smgr_fsm_nblocks;
    if (urqBlocks < minRecycleQueueBlockNumber) {
        UBTreeVerifyRecordOutput(VERIFY_RECYCLE_QUEUE_PAGE, 0, VERIFY_RECYCLE_QUEUE_PAGE_TOO_LESS, tupDesc, tupstore,
            cols);
        RelationCloseSmgr(rel);
        errVerified++;
        return errVerified;
    }
    errVerified += UBTreeVerifyRecycleQueueFork(rel, RECYCLE_EMPTY_FORK, tupDesc, tupstore, cols);
    errVerified += UBTreeVerifyRecycleQueueFork(rel, RECYCLE_FREED_FORK, tupDesc, tupstore, cols);
    RelationCloseSmgr(rel);
    return errVerified;
}
int UBTreeVerifyOnePage(Relation rel, Page page, BTScanInsert cmpKeys, IndexTuple prevHikey)
{
    UBTPageOpaqueInternal opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);
    /* get compare info */
    TupleDesc tupdes = RelationGetDescr(rel);
    int keysz = IndexRelationGetNumberOfKeyAttributes(rel);
    OffsetNumber firstPos = P_FIRSTDATAKEY(opaque);
    OffsetNumber lastPos = PageGetMaxOffsetNumber(page);
    if (lastPos < firstPos) {
        return VERIFY_NORMAL; /* empty */
    }
    /* compare last key and HIKEY first */
    if (!P_RIGHTMOST(opaque)) {
        IndexTuple lastKey = (IndexTuple)PageGetItem(page, PageGetItemId(page, lastPos));
        IndexTuple hikey = (IndexTuple)PageGetItem(page, PageGetItemId(page, P_HIKEY));
        /* we must hold: hikey > lastKey, it's equals to !(hikey <= lastKey) */
        if (_bt_index_tuple_compare(tupdes, cmpKeys->scankeys, keysz, hikey, lastKey)) {
            return VERIFY_HIKEY_ERROR;
        }
    }
    /* if prevHikey passed in, we also need to check it */
    if (prevHikey) {
        IndexTuple firstKey = (IndexTuple)PageGetItem(page, PageGetItemId(page, firstPos));
        /* we must hold: previous hikey <= firstKey */
        if (!_bt_index_tuple_compare(tupdes, cmpKeys->scankeys, keysz, prevHikey, firstKey)) {
            return VERIFY_PREV_HIKEY_ERROR;
        }
    }
    /* now check key orders */
    IndexTuple curKey = (IndexTuple)PageGetItem(page, PageGetItemId(page, firstPos));
    for (OffsetNumber nxtPos = OffsetNumberNext(firstPos); nxtPos <= lastPos; nxtPos = OffsetNumberNext(nxtPos)) {
        IndexTuple nextKey = (IndexTuple)PageGetItem(page, PageGetItemId(page, nxtPos));
        /* current key must <= next key */
        if (!_bt_index_tuple_compare(tupdes, cmpKeys->scankeys, keysz, curKey, nextKey)) {
            return VERIFY_ORDER_ERROR;
        }
        curKey = nextKey;
    }
    /* now check transaction info */
    if (P_ISLEAF(opaque)) {
        TransactionId maxXid = ReadNewTransactionId();
        for (OffsetNumber pos = firstPos; pos <= lastPos; pos = OffsetNumberNext(pos)) {
            IndexTuple itup = (IndexTuple)PageGetItem(page, PageGetItemId(page, pos));
            UstoreIndexXid uxid = (UstoreIndexXid)UstoreIndexTupleGetXid(itup);
            if (TransactionIdFollowsOrEquals(opaque->xid_base, maxXid)) {
                return VERIFY_XID_BASE_TOO_LARGE;
            }
            TransactionId xmin = ShortTransactionIdToNormal(opaque->xid_base, uxid->xmin);
            TransactionId xmax = ShortTransactionIdToNormal(opaque->xid_base, uxid->xmax);
            if (TransactionIdFollowsOrEquals(xmin, maxXid)) {
                return VERIFY_XID_TOO_LARGE;
            }
            if (TransactionIdFollowsOrEquals(xmax, maxXid)) {
                return VERIFY_XID_TOO_LARGE;
            }
            MemoryContext currentContext = CurrentMemoryContext;
            PG_TRY();
            {
                if (TransactionIdDidCommit(xmax) && !TransactionIdDidCommit(xmin)) {
                    PG_TRY_RETURN(VERIFY_XID_ORDER_ERROR);
                }
                CommitSeqNo csn1 = TransactionIdGetCommitSeqNo(xmin, false, false, false, NULL);
                CommitSeqNo csn2 = TransactionIdGetCommitSeqNo(xmax, false, false, false, NULL);
                if (COMMITSEQNO_IS_COMMITTED(csn1) && COMMITSEQNO_IS_COMMITTED(csn2) &&
                    (csn1 > csn2)) {
                    PG_TRY_RETURN(VERIFY_CSN_ORDER_ERROR);
                }
                bool xminCommittedByCSN = COMMITSEQNO_IS_COMMITTED(csn1);
                bool xmaxCommittedByCSN = COMMITSEQNO_IS_COMMITTED(csn2);
                if (TransactionIdDidCommit(xmin) != xminCommittedByCSN ||
                    TransactionIdDidCommit(xmax) != xmaxCommittedByCSN) {
                    PG_TRY_RETURN(VERIFY_INCONSISTENT_XID_STATUS);
                }
            }
            PG_CATCH();
            {
                (void)MemoryContextSwitchTo(currentContext);
                /* hit some errors when fetching xid status */
                FlushErrorState();
                return VERIFY_XID_STATUS_ERROR;
            }
            PG_END_TRY();
        }
    }
    return VERIFY_NORMAL;
}

void UBTreeDumpRecycleQueueFork(Relation rel, UBTRecycleForkNumber forkNum, TupleDesc *tupDesc,
    Tuplestorestate *tupstore, uint32 cols)
{
    BlockNumber forkMetaBlkno = forkNum;
    Buffer metaBuf = ReadRecycleQueueBuffer(rel, forkMetaBlkno);
    LockBuffer(metaBuf, BT_READ);
    UBTRecycleMeta metaData = (UBTRecycleMeta)PageGetContents(BufferGetPage(metaBuf));
    BlockNumber headBlkno = metaData->headBlkno;
    UnlockReleaseBuffer(metaBuf);

    /* check that we can traverse from head to tail and back to head again */
    Buffer buf = ReadRecycleQueueBuffer(rel, headBlkno);
    LockBuffer(buf, BT_READ);
    Page page = BufferGetPage(buf);
    UBTRecycleQueueHeader header = GetRecycleQueueHeader(page, BufferGetBlockNumber(buf));

    /* now we traverse the whole queue from the head page */
    while (true) {
        (void)UBTreeRecycleQueuePageDump(rel, buf, true, tupDesc, tupstore, cols);
        /* exit if current page is tail page*/
        if ((header->flags & URQ_TAIL_PAGE) != 0) {
            break;
        }
        /* move to the next page*/
        buf = StepNextRecyclePage(rel, buf);
        page = BufferGetPage(buf);
        header = GetRecycleQueueHeader(page, BufferGetBlockNumber(buf));
    }
    UnlockReleaseBuffer(buf);
}

char* UBTGetVerifiedPageTypeStr(uint32 type)
{
    switch (type) {
        case VERIFY_MAIN_PAGE:
            return "main page";
        case VERIFY_RECYCLE_QUEUE_PAGE:
            return "recycle queue page";
        default:
            return "unknown page";
    }
}

char* UBTGetVerifiedResultStr(uint32 type)
{
    switch (type) {
        case VERIFY_XID_BASE_TOO_LARGE:
            return "xid base is too large";
        case VERIFY_XID_TOO_LARGE:
            return "xid is too large";
        case VERIFY_HIKEY_ERROR:
            return "hikey error";
        case VERIFY_PREV_HIKEY_ERROR:
            return "prev hikey eror";
        case VERIFY_ORDER_ERROR:
            return "index order error";
        case VERIFY_XID_ORDER_ERROR:
            return "xid order error";
        case VERIFY_CSN_ORDER_ERROR:
            return "csn order error";
        case VERIFY_INCONSISTENT_XID_STATUS:
            return "inconsistent xid status";
        case VERIFY_XID_STATUS_ERROR:
            return "xid status error";
        case VERIFY_RECYCLE_QUEUE_HEAD_ERROR:
            return "recycle queue head error";
        case VERIFY_RECYCLE_QUEUE_TAIL_ERROR:
            return "recycle queue tail error";
        case VERIFY_INCONSISTENT_USED_PAGE:
            return "inconsistent used page";
        case VERIFY_RECYCLE_QUEUE_ENDLESS:
            return "recycle queue endless";
        case VERIFY_RECYCLE_QUEUE_TAIL_MISSED:
            return "recycle queue tail missed";
        case VERIFY_RECYCLE_QUEUE_PAGE_TOO_LESS:
            return "recycle queue page too less";
        case VERIFY_RECYCLE_QUEUE_OFFSET_ERROR:
            return "recycle queue offset error";
        case VERIFY_RECYCLE_QUEUE_XID_TOO_LARGE:
            return "xid of recycle queue too large";
        case VERIFY_RECYCLE_QUEUE_UNEXPECTED_TAIL:
            return "unexpected tail of recycle queue";
        case VERIFY_RECYCLE_QUEUE_FREE_LIST_ERROR:
            return "recycle queue freelist error";
        case VERIFY_RECYCLE_QUEUE_FREE_LIST_INVALID_OFFSET:
            return "invalid offset of recycle queue freelist";
        case VERIFY_NORMAL:
            return "normal";
        default:
            return "unknown verified results";
    }
}

static bool UBTreeVerifyTupleTransactionStatus(Relation rel, BlockNumber blkno, OffsetNumber offnum,
    TransactionIdStatus xminStatus, TransactionIdStatus xmaxStatus,
    TransactionId xmin, TransactionId xmax, CommitSeqNo xminCSN, CommitSeqNo xmaxCSN)
{
    bool tranStatusError = false;
    switch (xminStatus) {
        case XID_COMMITTED:
            tranStatusError = (xmaxStatus == XID_COMMITTED && xminCSN > xmaxCSN && xmaxCSN != COMMITSEQNO_FROZEN);
            break;
        case XID_INPROGRESS:
            tranStatusError = (xmaxStatus == XID_COMMITTED && TransactionIdIsValid(xmax));
            break;
        case XID_ABORTED:
            tranStatusError = (xminStatus == XID_ABORTED && xmaxStatus != XID_ABORTED);
            break;

        default:
            break;
    }

    if (tranStatusError) {
        RelFileNode rNode = rel ? rel->rd_node : RelFileNode{InvalidOid, InvalidOid, InvalidOid};
        ereport(ustore_verify_errlevel(), (errcode(ERRCODE_DATA_CORRUPTED),errmsg(
            "[Verify UBTree] xmin or xmax status invalid, xmin=%lu, xmax=%lu, xminStatus=%d, "
            "xmaxStatus=%d, xminCSN=%lu, xmaxCSN=%lu, rnode[%u,%u,%u], block %u, offnum %u.",
            xmin, xmax, xminStatus, xmaxStatus, xminCSN, xmaxCSN,
            rNode.spcNode, rNode.dbNode, rNode.relNode, blkno, offnum)));
        return false;
    }
    return true;
}
 
static int ItemCompare(const void *item1, const void *item2)
{
    return ((ItemIdSort)item1)->start - ((ItemIdSort)item2)->start;
}

void UBTreeVerifyHikey(Relation rel, Page page, BlockNumber blkno)
{
    CHECK_VERIFY_LEVEL(USTORE_VERIFY_FAST)
 
    UBTPageOpaqueInternal opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);

    if (P_RIGHTMOST(opaque))
        return;

    RelFileNode rNode = rel ? rel->rd_node : RelFileNode{InvalidOid, InvalidOid, InvalidOid};
    if (P_ISLEAF(opaque) ? (opaque->btpo.level != 0) : (opaque->btpo.level == 0)) {
        ereport(ustore_verify_errlevel(), (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("UBTREEVERIFY corrupted. level %u, flag %u, rnode[%u,%u,%u], block %u.",
            opaque->btpo.level, opaque->btpo_flags, rNode.spcNode, rNode.dbNode, rNode.relNode, blkno)));
        return;
    }
 
    OffsetNumber lastPos = PageGetMaxOffsetNumber(page);
    if (P_ISLEAF(opaque) ? (lastPos <= P_HIKEY) : (lastPos <= P_FIRSTKEY))
        return;

    IndexTuple lastTuple = (IndexTuple)PageGetItem(page, PageGetItemId(page, lastPos));
    BTScanInsert itupKey = UBTreeMakeScanKey(rel, lastTuple);
    if (UBTreeCompare(rel, itupKey, page, P_HIKEY, InvalidBuffer) <= 0) {
        pfree(itupKey);
        return;
    }
    pfree(itupKey);

    Datum values[INDEX_MAX_KEYS];
    bool isnull[INDEX_MAX_KEYS];
    index_deform_tuple(lastTuple, RelationGetDescr(rel), values, isnull);
    char *keyDesc = BuildIndexValueDescription(rel, values, isnull);
    ereport(ustore_verify_errlevel(), (errcode(ERRCODE_DATA_CORRUPTED),
        errmsg("UBTREEVERIFY corrupted key %s with HIKEY compare in rel %s, rnode[%u,%u,%u], block %u.",
        (keyDesc ? keyDesc : "(UNKNOWN)"), (rel && rel->rd_rel ? RelationGetRelationName(rel) : "Unknown"),
        rNode.spcNode, rNode.dbNode, rNode.relNode, blkno)));
    
}

void UBTreeVerifyPageXid(Relation rel, BlockNumber blkno, TransactionId xidBase, TransactionId pruneXid)
{
    CHECK_VERIFY_LEVEL(USTORE_VERIFY_FAST)
 
    const char *indexName = (rel && rel->rd_rel ? RelationGetRelationName(rel) : "unknown");
    RelFileNode rNode = rel ? rel->rd_node : RelFileNode{InvalidOid, InvalidOid, InvalidOid};
    if (TransactionIdFollows(xidBase, t_thrd.xact_cxt.ShmemVariableCache->nextXid) ||
        TransactionIdPrecedes(xidBase + MaxShortTransactionId, t_thrd.xact_cxt.ShmemVariableCache->nextXid)) {
        ereport(ustore_verify_errlevel(), (errcode(ERRCODE_DATA_CORRUPTED),errmsg(
            "[Verify UBTree] ubtree's page xid_base invalid: indexName=%s, xid_base=%lu, nextxid=%lu, "
            "rnode[%u,%u,%u], block %u.",
            indexName, xidBase, t_thrd.xact_cxt.ShmemVariableCache->nextXid,
            rNode.spcNode, rNode.dbNode, rNode.relNode, blkno)));
        return;
    }
    if (TransactionIdFollows(pruneXid, t_thrd.xact_cxt.ShmemVariableCache->nextXid)) {
        ereport(ustore_verify_errlevel(), (errcode(ERRCODE_DATA_CORRUPTED),errmsg(
            "[Verify UBTree] ubtree's page prune_xid invalid: indexName=%s, xid_base=%lu, nextxid=%lu, "
            "rnode[%u,%u,%u], block %u.",
            indexName, pruneXid, t_thrd.xact_cxt.ShmemVariableCache->nextXid,
            rNode.spcNode, rNode.dbNode, rNode.relNode, blkno)));
        return;
    }
}

static void UBTreeVerifyTupleTransactionInfo(Relation rel, BlockNumber blkno, Page page,
    OffsetNumber offnum, bool fromInsert, TransactionId xidBase)
{
    CHECK_VERIFY_LEVEL(USTORE_VERIFY_FAST)

    if (offnum == InvalidOffsetNumber)
        return;
    
    IndexTuple tuple = (IndexTuple)PageGetItem(page, PageGetItemId(page, offnum));
    UstoreIndexXid uxid = (UstoreIndexXid)UstoreIndexTupleGetXid(tuple);
    TransactionId xid = fromInsert ?
        ShortTransactionIdToNormal(xidBase, uxid->xmin) : ShortTransactionIdToNormal(xidBase, uxid->xmax);
    RelFileNode rNode = rel ? rel->rd_node : RelFileNode{InvalidOid, InvalidOid, InvalidOid};

    if (TransactionIdIsNormal(xid) && !TransactionIdIsCurrentTransactionId(xid)) {
        ereport(ustore_verify_errlevel(), (errcode(ERRCODE_DATA_CORRUPTED), 
            errmodule(MOD_USTORE), errmsg("[Verify UBTree] tuple xid %s invalid: indexName=%s, xid=%lu, "
            "rnode[%u,%u,%u], block %u, offnum %u.",
            (fromInsert ? "xmin" : "xmax"), (rel && rel->rd_rel ? RelationGetRelationName(rel) : "Unknown"), xid,
            rNode.spcNode, rNode.dbNode, rNode.relNode, blkno, offnum)));
    }
}

static void UBTreeVerifyAllTuplesTransactionInfo(Relation rel, Page page, BlockNumber blkno,
    OffsetNumber startoffset, bool fromInsert, TransactionId xidBase)
{
    CHECK_VERIFY_LEVEL(USTORE_VERIFY_COMPLETE)
 
    TransactionId maxXmax = InvalidTransactionId;
    TransactionId minCommittedXmax = MaxTransactionId;
    TransactionId pruneXid = ShortTransactionIdToNormal(xidBase, ((PageHeader)page)->pd_prune_xid);
    OffsetNumber maxoff = PageGetMaxOffsetNumber(page);
    TransactionId oldestXmin = u_sess->utils_cxt.RecentGlobalDataXmin;
    RelFileNode rNode = rel ? rel->rd_node : RelFileNode{InvalidOid, InvalidOid, InvalidOid};

    for (OffsetNumber offnum = startoffset; offnum <= maxoff; offnum = OffsetNumberNext(offnum)) {
        ItemId itemid = PageGetItemId(page, offnum);
        IndexTuple tuple = (IndexTuple)PageGetItem(page, itemid);
        UstoreIndexXid uxid = (UstoreIndexXid)UstoreIndexTupleGetXid(tuple);
        TransactionId xmin = ShortTransactionIdToNormal(xidBase, uxid->xmin);
        TransactionId xmax = ShortTransactionIdToNormal(xidBase, uxid->xmax);

        if (TransactionIdFollows(Max(xmin, xmax), t_thrd.xact_cxt.ShmemVariableCache->nextXid)) {
            ereport(ustore_verify_errlevel(), (errcode(ERRCODE_DATA_CORRUPTED),errmsg(
                "[Verify UBTree] index tuple xid(xmin/xmax) is bigger than nextXid:  "
                "xmin=%lu, xmax=%lu, nextxid=%lu, xid_base=%lu, rnode[%u,%u,%u], block %u, offnum %u.",
                xmin, xmax, t_thrd.xact_cxt.ShmemVariableCache->nextXid, xidBase,
                rNode.spcNode, rNode.dbNode, rNode.relNode, blkno, offnum)));
            return;
        }

        uint32 base = u_sess->attr.attr_storage.ustore_verify ? MaxShortTransactionId : 0;
        if (TransactionIdIsNormal(xmin) && !IndexItemIdIsFrozen(itemid) &&
            TransactionIdPrecedes(xmin + base, oldestXmin)) {
            ereport(ustore_verify_errlevel(), (errcode(ERRCODE_DATA_CORRUPTED),errmsg(
                "[Verify UBTree] index tuple xmin invalid: xmin=%lu, oldest_xmin=%lu, xid_base=%lu, "
                "rnode[%u,%u,%u], block %u, offnum %u.",
                xmin, oldestXmin, xidBase, rNode.spcNode, rNode.dbNode, rNode.relNode, blkno, offnum)));
            return;
        }
        if (TransactionIdIsNormal(xmax) && !ItemIdIsDead(itemid) &&
            TransactionIdPrecedes(xmax + base, oldestXmin)) {
            ereport(ustore_verify_errlevel(), (errcode(ERRCODE_DATA_CORRUPTED),errmsg(
                "[Verify UBTree] index tuple xmin invalid: xmax=%lu, oldest_xmin=%lu, xid_base=%lu, "
                "rnode[%u,%u,%u], block %u, offnum %u.",
                xmax, oldestXmin, xidBase, rNode.spcNode, rNode.dbNode, rNode.relNode, blkno, offnum)));
            return;
        }
        if (!u_sess->attr.attr_storage.ustore_verify) {
            continue;
        }
        TransactionIdStatus xminStatus = UBTreeCheckXid(xmin);
        CommitSeqNo xminCSN = TransactionIdGetCommitSeqNo(xmin, false, false, false, NULL);
        TransactionIdStatus xmaxStatus = UBTreeCheckXid(xmax);
        CommitSeqNo xmaxCSN = TransactionIdGetCommitSeqNo(xmax, false, false, false, NULL);
 
        if (xminStatus == XID_INPROGRESS && xmaxStatus != XID_INPROGRESS && TransactionIdIsValid(xmax)) {
            xminStatus = UBTreeCheckXid(xmin);
            xminCSN = TransactionIdGetCommitSeqNo(xmin, false, false, false, NULL);
        }
 
        if (xmaxStatus == XID_COMMITTED && TransactionIdPrecedes(xmax, minCommittedXmax)) {
            minCommittedXmax = xmax;
        }

        if (TransactionIdFollows(xmax, maxXmax)) {
            maxXmax = xmax;
        }
        if (!UBTreeVerifyTupleTransactionStatus(rel, blkno, offnum, xminStatus, xmaxStatus,
            xmin, xmax, xminCSN, xmaxCSN)) {
            return;
        }
    }
 
    UBTPageOpaque uopaque = (UBTPageOpaque)PageGetSpecialPointer(page);
    UBTPageOpaqueInternal ubtOpaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);
    if (TransactionIdFollows(uopaque->xact, t_thrd.xact_cxt.ShmemVariableCache->nextXid)) {
        ereport(ustore_verify_errlevel(), (errcode(ERRCODE_DATA_CORRUPTED),errmsg(
            "[Verify UBTree] xact xid is bigger than nextXid: xact=%lu, nextxid=%lu, rnode[%u,%u,%u], block %u.",
            uopaque->xact, t_thrd.xact_cxt.ShmemVariableCache->nextXid,
            rNode.spcNode, rNode.dbNode, rNode.relNode, blkno)));
        return;
    }
    if (!u_sess->attr.attr_storage.ustore_verify) {
        return;
    }
    if (minCommittedXmax != MaxTransactionId && TransactionIdIsValid(pruneXid) &&
        TransactionIdFollows(minCommittedXmax, pruneXid)) {
        ereport(ustore_verify_errlevel(), (errcode(ERRCODE_DATA_CORRUPTED),errmsg(
            "[Verify UBTree] min_committed_xmax is bigger than prune_xid: prune_xid=%lu, minCommittedXmax=%lu, "
            "rnode[%u,%u,%u], block %u.",
            pruneXid, minCommittedXmax, rNode.spcNode, rNode.dbNode, rNode.relNode, blkno)));
        return;
    }
 
    if (TransactionIdIsValid(maxXmax) && TransactionIdIsValid(ubtOpaque->last_delete_xid) &&
        TransactionIdFollows(maxXmax, ubtOpaque->last_delete_xid)) {
        ereport(ustore_verify_errlevel(), (errcode(ERRCODE_DATA_CORRUPTED),errmsg(
            "[Verify UBTree] max_xmax is bigger than last_delete_xid: last_delete_xid on page=%lu, actual value=%lu, "
            "rnode[%u,%u,%u], block %u.",
            ubtOpaque->last_delete_xid, maxXmax, rNode.spcNode, rNode.dbNode, rNode.relNode, blkno)));
    }
}

void UBTreeVerifyRowptrDML(Relation rel, Page page, BlockNumber blkno, OffsetNumber offnum)
{
    if (u_sess->attr.attr_storage.ustore_verify) {
        return UBTreeVerifyRowptrNonDML(rel, page, blkno);
    } 
    if (offnum == InvalidOffsetNumber) {
        return;
    }
    CHECK_VERIFY_LEVEL(USTORE_VERIFY_FAST)
 
    const char *indexName = (rel && rel->rd_rel ? RelationGetRelationName(rel) : "unknown");
    UBTPageOpaqueInternal opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);
    OffsetNumber firstPos = P_FIRSTDATAKEY(opaque);
    OffsetNumber lastPos = PageGetMaxOffsetNumber(page);
    if (firstPos > lastPos) {
        return;
    }
    ItemIdSort indexSortPtr = (ItemIdSort)palloc0(sizeof(ItemIdSortData));
    UBTreeVerifyRowptr((PageHeaderData*)page, page, blkno, offnum, indexSortPtr, indexName, rel);
    pfree(indexSortPtr);
 
    UBTreeVerifyTupleKey(rel, page, blkno, offnum, firstPos, lastPos);
}

void UBTreeVerifyItems(Relation rel, BlockNumber blkno, TupleDesc desc, BTScanInsert cmpKeys, int keysz,
    IndexTuple currKey, IndexTuple nextKey, UBTPageOpaqueInternal opaque)
{
    CHECK_VERIFY_LEVEL(USTORE_VERIFY_FAST)
 
    if (_bt_index_tuple_compare(desc, cmpKeys->scankeys, keysz, currKey, nextKey))
        return;

    char *currkeyDesc = NULL;
    char *nextkeyDesc = NULL;
    Datum values[INDEX_MAX_KEYS];
    bool isnull[INDEX_MAX_KEYS];
    RelFileNode rNode = rel ? rel->rd_node : RelFileNode{InvalidOid, InvalidOid, InvalidOid};

    if (P_ISLEAF(opaque)) {
        index_deform_tuple(currKey, RelationGetDescr(rel), values, isnull);
        currkeyDesc = BuildIndexValueDescription(rel, values, isnull);
        index_deform_tuple(nextKey, RelationGetDescr(rel), values, isnull);
        nextkeyDesc = BuildIndexValueDescription(rel, values, isnull);
    }
    ereport(ustore_verify_errlevel(), (errcode(ERRCODE_DATA_CORRUPTED),errmsg(
        "[Verify UBTree] nextkey >= currkey, nextkey: %s, currkey : %s, rnode[%u,%u,%u], block %u.",
        (nextkeyDesc ? nextkeyDesc : "(unknown)"), (currkeyDesc ? currkeyDesc : "(unknown)"),
        rNode.spcNode, rNode.dbNode, rNode.relNode, blkno)));
}

static void UBTreeVerifyTupleKey(Relation rel, Page page, BlockNumber blkno, OffsetNumber offnum,
    OffsetNumber firstPos, OffsetNumber lastPos)
{
    CHECK_VERIFY_LEVEL(USTORE_VERIFY_COMPLETE)
 
    UBTPageOpaqueInternal opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);
    TupleDesc desc = RelationGetDescr(rel);
    int keySize = IndexRelationGetNumberOfKeyAttributes(rel);
    BTScanInsert cmpKeys = UBTreeMakeScanKey(rel, NULL);
    IndexTuple currKey = (IndexTuple)PageGetItem(page, PageGetItemId(page, offnum));
    if (offnum > firstPos) {
        ItemId itemId = PageGetItemId(page, OffsetNumberPrev(offnum));
        IndexTuple prev_key = (IndexTuple)PageGetItem(page, itemId);
        UBTreeVerifyItems(rel, blkno, desc, cmpKeys, keySize, prev_key, currKey, opaque);
    }
    if (offnum < lastPos) {
        ItemId itemId = PageGetItemId(page, OffsetNumberNext(offnum));
        IndexTuple next_key = (IndexTuple)PageGetItem(page, itemId);
        UBTreeVerifyItems(rel, blkno, desc, cmpKeys, keySize, currKey, next_key, opaque);
    }
    pfree(cmpKeys);
}

static void UBTreeVerifyRowptrNonDML(Relation rel, Page page, BlockNumber blkno)
{
    CHECK_VERIFY_LEVEL(USTORE_VERIFY_COMPLETE)
 
    const char *indexName = (rel && rel->rd_rel ? RelationGetRelationName(rel) : "unknown");
    TupleDesc desc = RelationGetDescr(rel);
    int keysz = IndexRelationGetNumberOfKeyAttributes(rel);
    ItemIdSortData itemidBase[MaxIndexTuplesPerPage]; 
    ItemIdSort sortPtr = itemidBase;
    UBTPageOpaqueInternal opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);
    OffsetNumber firstPos = P_FIRSTDATAKEY(opaque);
    OffsetNumber lastPos = PageGetMaxOffsetNumber(page);

    if (firstPos > lastPos) {
        return;
    }
    
    BTScanInsert cmpKeys = UBTreeMakeScanKey(rel, NULL);
    UBTreeVerifyRowptr((PageHeaderData*)page, page, blkno, firstPos, sortPtr, indexName, rel);
    IndexTuple currKey = (IndexTuple)PageGetItem(page, PageGetItemId(page, firstPos));
    OffsetNumber nextPos = OffsetNumberNext(firstPos);
    sortPtr++;
    RelFileNode rNode = rel ? rel->rd_node : RelFileNode{InvalidOid, InvalidOid, InvalidOid};
    while (nextPos <= lastPos) {
        ItemId itemid = PageGetItemId(page, nextPos);
        IndexTuple nextKey = (IndexTuple)PageGetItem(page, itemid);
        if (P_ISLEAF(opaque) || nextPos > firstPos + 1) {
            if (!_bt_index_tuple_compare(desc, cmpKeys->scankeys, keysz, currKey, nextKey)) {
                Datum values[INDEX_MAX_KEYS];
                bool isnull[INDEX_MAX_KEYS];
                char *currkeyDesc = NULL;
                char *nextkeyDesc = NULL;
                if (P_ISLEAF(opaque)) {
                    index_deform_tuple(currKey, RelationGetDescr(rel), values, isnull);
                    currkeyDesc = BuildIndexValueDescription(rel, values, isnull);
                    index_deform_tuple(nextKey, RelationGetDescr(rel), values, isnull);
                    nextkeyDesc = BuildIndexValueDescription(rel, values, isnull);
                }
                ereport(ustore_verify_errlevel(), (errcode(ERRCODE_DATA_CORRUPTED), errmsg(
                    "[Verify UBTree] nextkey >= currkey, nextkey: %s, currkey : %s, indexName=%s, "
                    "rnode[%u,%u,%u], block %u.",
                    (nextkeyDesc ? nextkeyDesc : "(unknown)"), (currkeyDesc ? currkeyDesc : "(unknown)"), indexName,
                    rNode.spcNode, rNode.dbNode, rNode.relNode, blkno)));
                pfree(cmpKeys);
                return;
            }
        }
        currKey = nextKey;
        UBTreeVerifyRowptr((PageHeaderData*)page, page, blkno, nextPos, sortPtr, indexName, rel);
        nextPos = OffsetNumberNext(nextPos);
        sortPtr++;
    }
 
    int storageNum = sortPtr - itemidBase;
    if (storageNum <= 1) {
        pfree(cmpKeys);
        return;
    }
 
    qsort((char*)itemidBase, storageNum, sizeof(ItemIdSortData), ItemCompare);
 
    for (int i = 0; i < storageNum - 1; i++) {
        ItemIdSort tempPtr1 = &itemidBase[i];
        ItemIdSort tempPtr2 = &itemidBase[i + 1];
        if (tempPtr1->end > tempPtr2->start) {
            ereport(ustore_verify_errlevel(), (errcode(ERRCODE_DATA_CORRUPTED),errmsg(
                "[Verify UBTree] Ubtree ItemIdSort conflict: indexName=%s, ptr1offset %u, "
                "ptr1start = %u, ptr1end = %u, ptr2offset = %u, ptr2start = %u, ptr2end = %u, "
                "rnode[%u,%u,%u], block %u.",
                indexName, tempPtr1->offset, tempPtr1->start, tempPtr1->end,
                tempPtr2->offset, tempPtr2->start, tempPtr2->end,
                rNode.spcNode, rNode.dbNode, rNode.relNode, blkno)));
            pfree(cmpKeys);
            return;
        }
    }
 
    pfree(cmpKeys);
}

void UBTreeVerifyPage(Relation rel, Page page, BlockNumber blkno, OffsetNumber offnum, bool fromInsert)
{
    BYPASS_VERIFY(USTORE_VERIFY_MOD_UBTREE, rel);
 
    CHECK_VERIFY_LEVEL(USTORE_VERIFY_FAST)
 
    BTPageOpaqueInternal opaque = (BTPageOpaqueInternal)PageGetSpecialPointer(page);
    if (P_IGNORE(opaque)) {
        return;
    }
 
    UBTreeVerifyHeader((PageHeaderData*)page, rel, blkno, PageGetPageSize((PageHeader)page), GetPageHeaderSize(page));
    UBTreeVerifyHikey(rel, page, blkno);
    UBTreeVerifyRowptrDML(rel, page, blkno, offnum);
    UBTPageOpaqueInternal ubtOpaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);
    if (!P_ISLEAF(ubtOpaque)) {
        return;
    }
    TransactionId xidBase = ubtOpaque->xid_base;
    UBTreeVerifyPageXid(rel, blkno, xidBase, ShortTransactionIdToNormal(xidBase, ((PageHeader)page)->pd_prune_xid));
    UBTreeVerifyTupleTransactionInfo(rel, blkno, page, offnum, fromInsert, xidBase);
}

static void UBTreeVerifyHeader(PageHeaderData* page, Relation rel, BlockNumber blkno, uint16 pageSize, uint16 headerSize)
{
    CHECK_VERIFY_LEVEL(USTORE_VERIFY_FAST)

    RelFileNode rNode = rel ? rel->rd_node : RelFileNode{InvalidOid, InvalidOid, InvalidOid};
    if (pageSize != BLCKSZ || (page->pd_flags & ~PD_VALID_FLAG_BITS) != 0 || page->pd_lower < headerSize ||
        page->pd_lower > page->pd_upper || page->pd_upper > page->pd_special || page->pd_special > BLCKSZ) {
        const char *indexName = (rel && rel->rd_rel ? RelationGetRelationName(rel) : "unknown");
        ereport(ustore_verify_errlevel(), (errcode(ERRCODE_DATA_CORRUPTED),errmsg(
            "[Verify UBTree] index page header invalid: indexName=%s, size=%u,"
            "flags=%u, lower=%u, upper=%u, special=%u, rnode[%u,%u,%u], block %u.", indexName, headerSize,
            page->pd_flags, page->pd_lower, page->pd_upper, page->pd_special,
            rNode.spcNode, rNode.dbNode, rNode.relNode, blkno)));
    }
}

static void UBTreeVerifyRowptr(PageHeaderData* header, Page page, BlockNumber blkno, OffsetNumber offset,
    ItemIdSort indexSortPtr, const char *indexName, Relation rel)
{
    CHECK_VERIFY_LEVEL(USTORE_VERIFY_FAST)
 
    ItemId itemId = PageGetItemId(page, offset);
    unsigned rpStart = ItemIdGetOffset(itemId);
    Size rpLen = ItemIdGetLength(itemId);
    RelFileNode rNode = rel ? rel->rd_node : RelFileNode{InvalidOid, InvalidOid, InvalidOid};

    if (!ItemIdIsUsed(itemId)) {
        ereport(ustore_verify_errlevel(), (errcode(ERRCODE_DATA_CORRUPTED),errmsg(
            "[Verify UBTree] row pointer is unused: indexName=%s, "
            "rowPtr startOffset=%u, rowPtr len=%lu, rnode[%u,%u,%u], block %u, offnum %u.",
            indexName, rpStart, rpLen, rNode.spcNode, rNode.dbNode, rNode.relNode, blkno, offset)));
        return;
    }
    if (!ItemIdHasStorage(itemId)) {
        ereport(ustore_verify_errlevel(), (errcode(ERRCODE_DATA_CORRUPTED),errmsg(
            "[Verify UBTree] row pointer has no storage: indexName=%s, "
            "rowPtr startOffset=%u, rowPtr len=%lu, rnode[%u,%u,%u], block %u, offnum %u.",
            indexName, rpStart, rpLen, rNode.spcNode, rNode.dbNode, rNode.relNode, blkno, offset)));
        return;
    }
    indexSortPtr->start = rpStart;
    indexSortPtr->end = indexSortPtr->start + SHORTALIGN(ItemIdGetLength(itemId));
    indexSortPtr->offset = offset;
    if (indexSortPtr->start < header->pd_upper || indexSortPtr->end > header->pd_special) {
        ereport(ustore_verify_errlevel(), (errcode(ERRCODE_DATA_CORRUPTED),errmsg(
            "[Verify UBTree] The item corresponding to row pointer exceeds the range of item stored in the page: "
            "indexName=%s, rowPtr startOffset=%u, rowPtr len=%lu, rnode[%u,%u,%u], block %u, offnum %u.",
            indexName, rpStart, rpLen, rNode.spcNode, rNode.dbNode, rNode.relNode, blkno, offset)));
        return;
    }
    int tupleSize = IndexTupleSize((IndexTuple)PageGetItem(page, itemId));
    if (tupleSize > (int)rpLen) {
        ereport(ustore_verify_errlevel(), (errcode(ERRCODE_DATA_CORRUPTED),errmsg(
            "[Verify UBTree] tuple size is bigger than item's len: indexName=%s, "
            "tuple size=%d, rowPtr len=%lu, rnode[%u,%u,%u], block %u, offnum %u.",
            indexName, tupleSize, rpLen, rNode.spcNode, rNode.dbNode, rNode.relNode, blkno, offset)));
        return;
    }
}

void UBTreeVerifyAll(Relation rel, Page page, BlockNumber blkno, OffsetNumber offnum, bool fromInsert)
{
    BYPASS_VERIFY(USTORE_VERIFY_MOD_UBTREE, rel);
 
    CHECK_VERIFY_LEVEL(USTORE_VERIFY_FAST)
    UBTreeVerifyPage(rel, page, blkno, offnum, fromInsert);
    BTPageOpaqueInternal opaque = (BTPageOpaqueInternal)PageGetSpecialPointer(page);
    if (P_IGNORE(opaque)) {
        return;
    }
    UBTPageOpaqueInternal ubtOpaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);
    if (!P_ISLEAF(ubtOpaque)) {
        return;
    }
    TransactionId xidBase = ubtOpaque->xid_base;
    UBTreeVerifyAllTuplesTransactionInfo(rel, page, blkno, P_FIRSTDATAKEY(ubtOpaque), fromInsert, xidBase);
}
