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

typedef RpSort ItemIdSort;
typedef RpSortData ItemIdSortData;

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
        buf = StepNextRecyclePage(rel, buf);
        page = BufferGetPage(buf);
        header = GetRecycleQueueHeader(page, BufferGetBlockNumber(buf));
        if ((header->flags & URQ_TAIL_PAGE) == 0) {
            break;
        }
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
        buf = StepNextRecyclePage(rel, buf);
        page = BufferGetPage(buf);
        header = GetRecycleQueueHeader(page, BufferGetBlockNumber(buf));
        if ((header->flags & URQ_TAIL_PAGE) == 0) {
            break;
        }
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

bool VerifyIndexPageHeader(Relation rel, Page page)
{
    PageHeader phdr = (PageHeader)page;
    uint16 pdLower = phdr->pd_lower;
    uint16 pdUpper = phdr->pd_upper;
    uint16 pdSpecial = phdr->pd_special;
    if (PageGetPageSize(phdr) != BLCKSZ || (phdr->pd_flags & ~PD_VALID_FLAG_BITS) != 0 ||
        pdLower < GetPageHeaderSize(page) || pdLower > pdUpper || pdUpper > pdSpecial || pdSpecial > BLCKSZ) {
        ereport(defence_errlevel(), (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("UBTREEVERIFY index page header invalid: rel %s, size %lu, flags %u, lower %u, upper %u,"
                   "special %u", NameStr(rel->rd_rel->relname), PageGetPageSize(phdr), phdr->pd_flags, pdLower,
                   pdUpper, pdSpecial)));
    }
    return true;
}

bool VerifyIndexTuple(Relation rel, Page page, OffsetNumber offset)
{
    ItemId itemId = PageGetItemId(page, offset);
    IndexTuple ituple = (IndexTuple)PageGetItem(page, itemId);
    int tupSize = IndexTupleSize(ituple);
    if (tupSize > (int)ItemIdGetLength(itemId)) {
        ereport(defence_errlevel(), (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("UBTREEVERIFY corrupted tuple: rel %s, tupsize = %d, rpsize = %u.",
                   NameStr(rel->rd_rel->relname), tupSize, ItemIdGetLength(itemId))));
    }
    return true;
}

bool VerifyIndexOneItemId(Relation rel, Page page, OffsetNumber offset, ItemIdSort itemIdSortPtr)
{
    ItemId itemId = PageGetItemId(page, offset);
    PageHeader phdr = (PageHeader)page;
    uint16 pdUpper = phdr->pd_upper;
    uint16 pdSpecial = phdr->pd_special;
    if (!ItemIdIsUsed(itemId)) {
        ereport(defence_errlevel(), (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("UBTREEVERIFY corrupted unused line pointer: rel %s, offset = %u, rpstart = %u, rplen = %u.",
                   NameStr(rel->rd_rel->relname), offset, ItemIdGetOffset(itemId), ItemIdGetLength(itemId))));
    }
    if (!ItemIdHasStorage(itemId)) {
        ereport(defence_errlevel(), (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("UBTREEVERIFY corrupted no storage line pointer: rel %s, offset = %u, rpstart = %u, rplen = %u.",
                   NameStr(rel->rd_rel->relname), offset, ItemIdGetOffset(itemId), ItemIdGetLength(itemId))));
    }
    itemIdSortPtr->start = ItemIdGetOffset(itemId);
    itemIdSortPtr->end = itemIdSortPtr->start + SHORTALIGN(ItemIdGetLength(itemId));
    itemIdSortPtr->offset = offset;
    if (itemIdSortPtr->start < pdUpper || itemIdSortPtr->end > pdSpecial) {
        ereport(defence_errlevel(), (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("UBTREEVERIFY corrupted normal line pointer: rel %s, offset = %u, rpstart = %u, rplen = %u.",
                   NameStr(rel->rd_rel->relname), offset, ItemIdGetOffset(itemId), ItemIdGetLength(itemId))));
    }
    VerifyIndexTuple(rel, page, offset);
    return true;
}

static int ItemCompare(const void *item1, const void *item2)
{
    return ((ItemIdSort)item1)->start - ((ItemIdSort)item2)->start;
}

bool VerifyIndexPageItemId(Relation rel, Page page, VerifyLevel level)
{
    if (level <= USTORE_VERIFY_FAST) {
        return true;
    }
    TupleDesc tupdes = RelationGetDescr(rel);
    int keysz = IndexRelationGetNumberOfKeyAttributes(rel);
    ItemIdSortData itemIdBase[MaxIndexTuplesPerPage];
    ItemIdSort itemIdSortPtr = itemIdBase;
    UBTPageOpaqueInternal opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);
    OffsetNumber firstPos = P_FIRSTDATAKEY(opaque);
    OffsetNumber lastPos = PageGetMaxOffsetNumber(page);
    if (firstPos > lastPos) {
        return true; /* empty page */
    }
    
    /* check key orders */
    BTScanInsert cmpKeys = UBTreeMakeScanKey(rel, NULL);
    VerifyIndexOneItemId(rel, page, firstPos, itemIdSortPtr);
    itemIdSortPtr++;
    IndexTuple curKey = (IndexTuple)PageGetItem(page, PageGetItemId(page, firstPos));
    for (OffsetNumber nxtPos = OffsetNumberNext(firstPos); nxtPos <= lastPos; nxtPos = OffsetNumberNext(nxtPos)) {
        ItemId itemId = PageGetItemId(page, nxtPos);
        IndexTuple nextKey = (IndexTuple)PageGetItem(page, itemId);
        if (P_ISLEAF(opaque) || nxtPos > firstPos + 1) {
            /* current key must <= next key */
            if (!_bt_index_tuple_compare(tupdes, cmpKeys->scankeys, keysz, curKey, nextKey)) {
                Datum values[INDEX_MAX_KEYS];
                bool isnull[INDEX_MAX_KEYS];
                char *curKeyDesc = NULL;
                char *nextKeyDesc = NULL;
                if (P_ISLEAF(opaque)) {
                    index_deform_tuple(curKey, RelationGetDescr(rel), values, isnull);
                    curKeyDesc = BuildIndexValueDescription(rel, values, isnull);
                    index_deform_tuple(nextKey, RelationGetDescr(rel), values, isnull);
                    nextKeyDesc = BuildIndexValueDescription(rel, values, isnull);
                }
                ereport(defence_errlevel(), (errcode(ERRCODE_DATA_CORRUPTED),
                        errmsg("UBTREEVERIFY corrupted key order %s %s, rel %s",
                        (curKeyDesc ? curKeyDesc : "(UNKNOWN)"),
                        (nextKeyDesc ? nextKeyDesc : "(UNKNOWN)"),
                        NameStr(rel->rd_rel->relname))));
            }
        }
        curKey = nextKey;
        VerifyIndexOneItemId(rel, page, nxtPos, itemIdSortPtr);
        itemIdSortPtr++;
    }

    int nstorage = itemIdSortPtr - itemIdBase;
    if (nstorage <= 1) {
        pfree(cmpKeys);
        return true;
    }

    qsort((char *)itemIdBase, nstorage, sizeof(ItemIdSortData), ItemCompare);

    for (int i = 0; i < nstorage - 1; i++) {
        ItemIdSort tempPtr1 = &itemIdBase[i];
        ItemIdSort tempPtr2 = &itemIdBase[i + 1];
        if (tempPtr1->end > tempPtr2->start) {
            ereport(defence_errlevel(), (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("UBTREEVERIFY corrupted line pointer: rel %s tempPtr1offset %u, tempPtr1start = %u, "
                       "tempPtr1end = %u, tempPtr2offset = %u, tempPtr2start = %u, tempPtr2end = %u.",
                       NameStr(rel->rd_rel->relname),
                       tempPtr1->offset, tempPtr1->start, tempPtr1->end,
                       tempPtr2->offset, tempPtr2->start, tempPtr2->end)));
        }
    }

    pfree(cmpKeys);
    return true;
}

bool VerifyIndexHiKeyAndOpaque(Relation rel, Page page)
{
    UBTPageOpaqueInternal opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);
    if (P_ISLEAF(opaque) ? (opaque->btpo.level != 0) : (opaque->btpo.level == 0)) {
        ereport(defence_errlevel(), (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("UBTREEVERIFY corrupted rel %s, level %u, flag %u", NameStr(rel->rd_rel->relname),
                       opaque->btpo.level, opaque->btpo_flags)));
    }

    /* compare last key and HIKEY */
    OffsetNumber lastPos = PageGetMaxOffsetNumber(page);
    /* note that the first data key of internal pages has no value */
    if (!P_RIGHTMOST(opaque) && (P_ISLEAF(opaque) ? (lastPos > P_HIKEY) : (lastPos > P_FIRSTKEY))) {
        IndexTuple lastTuple = (IndexTuple)PageGetItem(page, PageGetItemId(page, lastPos));

        /* we must hold: hikey >= lastKey */
        BTScanInsert itupKey = UBTreeMakeScanKey(rel, lastTuple);
        if (UBTreeCompare(rel, itupKey, page, P_HIKEY, InvalidBuffer) > 0) {
            Datum values[INDEX_MAX_KEYS];
            bool isnull[INDEX_MAX_KEYS];
            index_deform_tuple(lastTuple, RelationGetDescr(rel), values, isnull);
            char *keyDesc = BuildIndexValueDescription(rel, values, isnull);
            ereport(defence_errlevel(), (errcode(ERRCODE_DATA_CORRUPTED),
                    errmsg("UBTREEVERIFY corrupted key %s with HIKEY compare in rel %s",
                    (keyDesc ? keyDesc : "(UNKNOWN)"), NameStr(rel->rd_rel->relname))));
        }
        pfree(itupKey);
    }
    return true;
}

bool UBTreeVerifyITupleTransactionStatus(TransactionIdStatus xminStatus, TransactionIdStatus xmaxStatus,
    TransactionId xmin, TransactionId xmax, CommitSeqNo xminCSN, CommitSeqNo xmaxCSN)
{
    if (xminStatus == XID_INPROGRESS && xmaxStatus == XID_COMMITTED && TransactionIdIsValid(xmax)) {
        return false;
    }
    if (xminStatus == XID_ABORTED && xmaxStatus != XID_ABORTED) {
        return false;
    }
    if (xminStatus == XID_COMMITTED && xmaxStatus == XID_COMMITTED) {
        if (xminCSN > xmaxCSN && xmaxCSN != COMMITSEQNO_FROZEN) {
            return false;
        }
    }
    return true;
}

bool VerifyIndexTransactionInfo(Relation rel, Page page, VerifyLevel level, GPIScanDesc gpiScan)
{
    UBTPageOpaqueInternal opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);
    if (!P_ISLEAF(opaque)) {
        return true;
    }
    TransactionId xid_base = opaque->xid_base;
    TransactionId pruneXid = ShortTransactionIdToNormal(xid_base, ((PageHeader)page)->pd_prune_xid);
    if (TransactionIdFollows(xid_base, t_thrd.xact_cxt.ShmemVariableCache->nextXid)) {
        ereport(defence_errlevel(), (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("UBTREEVERIFY xid_base invalid: rel %s, xid_base = %lu, nextxid = %lu.",
                       NameStr(rel->rd_rel->relname), xid_base, t_thrd.xact_cxt.ShmemVariableCache->nextXid)));
    }
    if (TransactionIdFollows(pruneXid, t_thrd.xact_cxt.ShmemVariableCache->nextXid)) {
        ereport(defence_errlevel(), (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("UBTREEVERIFY prune_xid invalid: rel %s, prune_xid = %lu, nextxid = %lu.",
                       NameStr(rel->rd_rel->relname), pruneXid, t_thrd.xact_cxt.ShmemVariableCache->nextXid)));
    }
    if (level <= USTORE_VERIFY_FAST) {
        return true;
    }

    /* stat info for prune_xid and last_delete_xid */
    TransactionId maxXmax = InvalidTransactionId;
    TransactionId minCommittedXmax = MaxTransactionId;

    OffsetNumber maxoff = PageGetMaxOffsetNumber(page);
    for (OffsetNumber offnum = P_FIRSTDATAKEY(opaque); offnum <= maxoff; offnum = OffsetNumberNext(offnum)) {
        ItemId iid = PageGetItemId(page, offnum);
        IndexTuple itup = (IndexTuple)PageGetItem(page, iid);
        UstoreIndexXid uxid = (UstoreIndexXid)UstoreIndexTupleGetXid(itup);

        /* fetch trans info */
        TransactionId xmin = ShortTransactionIdToNormal(xid_base, uxid->xmin);
        TransactionId xmax = ShortTransactionIdToNormal(xid_base, uxid->xmax);
        if (TransactionIdFollows(Max(xmin, xmax), t_thrd.xact_cxt.ShmemVariableCache->nextXid)) {
            ereport(defence_errlevel(), (errcode(ERRCODE_DATA_CORRUPTED),
                    errmsg("UBTREEVERIFY itup xid invalid: rel %s, xmin/xmax %lu/%lu, nextxid %lu, xid-base %lu.",
                           NameStr(rel->rd_rel->relname), xmin, xmax, t_thrd.xact_cxt.ShmemVariableCache->nextXid,
                           xid_base)));
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
        if (!UBTreeVerifyITupleTransactionStatus(xminStatus, xmaxStatus, xmin, xmax, xminCSN, xmaxCSN)) {
            ereport(defence_errlevel(), (errcode(ERRCODE_DATA_CORRUPTED),
                    errmsg("UBTREEVERIFY xmin xmax status invalid, rel %s, xmin %lu, xmax %lu, xminStatus %d,"
                           "xmaxStatus %d, xminCSN %lu, xmaxCSN %lu.",
                           NameStr(rel->rd_rel->relname), xmin, xmax, xminStatus, xmaxStatus, xminCSN, xmaxCSN)));
        }
    }

    if (minCommittedXmax != MaxTransactionId && TransactionIdIsValid(pruneXid) &&
        TransactionIdFollows(minCommittedXmax, pruneXid)) {
        ereport(defence_errlevel(), (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("UBTREEVERIFY prune_xid invalid, rel = %s, prune_xid on page = %lu, actual value = %lu.",
                       NameStr(rel->rd_rel->relname), pruneXid, minCommittedXmax)));
    }

    if (TransactionIdIsValid(maxXmax) && TransactionIdIsValid(opaque->last_delete_xid) &&
        TransactionIdFollows(maxXmax, opaque->last_delete_xid)) {
        ereport(defence_errlevel(), (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("UBTREEVERIFY last_delete_xid invalid, rel = %s, last_delete_xid on page = %lu, "
                       "actual value = %lu.",
                       NameStr(rel->rd_rel->relname), opaque->last_delete_xid, maxXmax)));
    }

    /* skip unique check for non-unique index */
    if (!rel->rd_index->indisunique && !rel->rd_index->indisprimary) {
        return true;
    }
    /* skip unique check if gpiScan of GPI is not given */
    if (RelationIsGlobalIndex(rel) && !gpiScan) {
        return true;
    }
    return true;
}

bool UBTreePageVerify(UBtreePageVerifyParams *verifyParams)
{
    if (verifyParams == NULL) {
        return false;
    }

    Relation rel = verifyParams->bvInfo.rel;
    Page page = verifyParams->page;
    VerifyLevel level = verifyParams->bvInfo.vLevel;
    GPIScanDesc gpiScan = verifyParams->gpiScan;
    BTPageOpaqueInternal opaque = (BTPageOpaqueInternal)PageGetSpecialPointer(page);
    if (P_IGNORE(opaque)) {
        return true;
    }

    VerifyIndexPageHeader(rel, page);
    VerifyIndexHiKeyAndOpaque(rel, page);
    VerifyIndexPageItemId(rel, page, level);
    VerifyIndexTransactionInfo(rel, page, level, gpiScan);
    return true;
}

