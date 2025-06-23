/*
* Copyright (c) 2025 Huawei Technologies Co.,Ltd.
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
 * bm25build.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/datavec/bm25build.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <pthread.h>
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "storage/buf/block.h"
#include "utils/memutils.h"
#include "postmaster/bgworker.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/tableam.h"
#include "utils/builtins.h"
#include "access/datavec/bm25.h"

#define CALLBACK_ITEM_POINTER HeapTuple hup

/*
 * Initialize the build state
 */
static void InitBM25BuildState(BM25BuildState *buildstate, Relation heap, Relation index, IndexInfo *indexInfo,
    ForkNumber forkNum)
{
    buildstate->heap = heap;
    buildstate->index = index;
    buildstate->indexInfo = indexInfo;
    buildstate->forkNum = forkNum;

    buildstate->reltuples = 0;
    buildstate->indtuples = 0;

    /* Get support functions */
    buildstate->procinfo = nullptr;
    buildstate->collation = index->rd_indcollation[0];

    buildstate->bm25leader = nullptr;

    buildstate->tmpCtx =
        AllocSetContextCreate(CurrentMemoryContext, "bm25 build temporary context", ALLOCSET_DEFAULT_SIZES);
}

static BlockNumber CreateBM25CommonPage(Relation index, ForkNumber forkNum, bool building)
{
    Buffer buf;
    Page page;
    GenericXLogState *state = nullptr;
    BlockNumber blkno = InvalidBlockNumber;

    buf = BM25NewBuffer(index, forkNum);
    BM25GetPage(index, &page, buf, &state, building);
    BM25InitPage(buf, page);
    blkno = BufferGetBlockNumber(buf);
    BM25CommitBuf(buf, &state, building);
    return blkno;
}

static void InsertItemToTokenMetaList(Relation index, BM25EntryPages &bm25EntryPages, uint32 bucketIdx,
    BM25TokenData &tokenData, BM25PageLocationInfo &tokenMetaLocation, ForkNumber forkNum, bool building)
{
    BlockNumber firstTokenMetasBlkno = InvalidBlockNumber;
    Page cpageBucket;
    GenericXLogState *bucketState = nullptr;
    OffsetNumber bucketOffno = (bucketIdx / BM25_BUCKET_PAGE_ITEM_SIZE) + 1;
    uint32 bucketIdxInItem = bucketIdx % BM25_BUCKET_PAGE_ITEM_SIZE;
    Buffer cbufBucket = ReadBuffer(index, bm25EntryPages.hashBucketsPage);
    LockBuffer(cbufBucket, BUFFER_LOCK_SHARE);
    cpageBucket = BufferGetPage(cbufBucket);
    BM25HashBucketPage bucketInfo = (BM25HashBucketPage)PageGetItem(cpageBucket,
        PageGetItemId(cpageBucket, bucketOffno));
    /* add new page, change to BUFFER_LOCK_EXCLUSIVE */
    if (bucketInfo->bucketBlkno[bucketIdxInItem] == InvalidBlockNumber) {
        LockBuffer(cbufBucket, BUFFER_LOCK_UNLOCK);
        LockBuffer(cbufBucket, BUFFER_LOCK_EXCLUSIVE);
        BM25GetPage(index, &cpageBucket, cbufBucket, &bucketState, building);
        BM25HashBucketPage bucketInfoTmp = (BM25HashBucketPage)PageGetItem(cpageBucket,
            PageGetItemId(cpageBucket, bucketOffno));
        if (bucketInfoTmp->bucketBlkno[bucketIdxInItem] == InvalidBlockNumber) {
            bucketInfoTmp->bucketBlkno[bucketIdxInItem] = CreateBM25CommonPage(index, forkNum, building);
        }
        firstTokenMetasBlkno = bucketInfoTmp->bucketBlkno[bucketIdxInItem];
        BM25CommitBuf(cbufBucket, &bucketState, building);
    } else {
        firstTokenMetasBlkno = bucketInfo->bucketBlkno[bucketIdxInItem];
        UnlockReleaseBuffer(cbufBucket);
    }

    /* find tokenMeta */
    BlockNumber nextblkno = firstTokenMetasBlkno;
    BlockNumber curblkno = firstTokenMetasBlkno;
    Buffer cbuf;
    Page cpage;
    GenericXLogState *state = nullptr;
    while (BlockNumberIsValid(nextblkno)) {
        OffsetNumber maxoffno;
        cbuf = ReadBuffer(index, nextblkno);
        LockBuffer(cbuf, BUFFER_LOCK_EXCLUSIVE);
        BM25GetPage(index, &cpage, cbuf, &state, building);
        if (FindTokenMeta(tokenData, tokenMetaLocation, cbuf, cpage)) {
            if (!building) {
                GenericXLogAbort(state);
            }
            UnlockReleaseBuffer(cbuf);
            return;
        }
        curblkno = nextblkno;
        nextblkno = BM25PageGetOpaque(cpage)->nextblkno;
        if (!BlockNumberIsValid(nextblkno)) {
            break;
        }
        if (!building) {
            GenericXLogAbort(state);
        }
        UnlockReleaseBuffer(cbuf);
    }

    /* tokenMetaItem is not found, add new one */
    uint32 itemSize = MAXALIGN(sizeof(BM25TokenMetaItem));
    /* Ensure free space */
    if (PageGetFreeSpace(cpage) < itemSize) {
        BM25AppendPage(index, &cbuf, &cpage, forkNum, &state, building);
        curblkno = BufferGetBlockNumber(cbuf);
    }

    BM25TokenMetaPage tokenMeta = (BM25TokenMetaPage)palloc0(itemSize);
    errno_t rc = strncpy_s(tokenMeta->token, BM25_MAX_TOKEN_LEN, tokenData.tokenValue, BM25_MAX_TOKEN_LEN - 1);
    securec_check_c(rc, "\0", "\0");
    tokenMeta->tokenId = BM25AllocateTokenId(index);
    tokenData.tokenId = tokenMeta->tokenId;
    tokenMeta->hashValue = tokenData.hashValue;
    tokenMeta->maxScore = 0;
    tokenMeta->postingBlkno = InvalidBlockNumber;
    tokenMeta->lastInsertBlkno = InvalidBlockNumber;
    OffsetNumber offno = PageAddItem(cpage, (Item)tokenMeta, itemSize, InvalidOffsetNumber, false, false);
    if (offno == InvalidOffsetNumber) {
        pfree(tokenMeta);
        if (!building)
            GenericXLogAbort(state);
        UnlockReleaseBuffer(cbuf);
        elog(ERROR, "failed to add index item [BM25TokenMeta] to \"%s\"", RelationGetRelationName(index));
    }
    tokenMetaLocation.blkno = BufferGetBlockNumber(cbuf);
    tokenMetaLocation.offno = offno;
    BM25CommitBuf(cbuf, &state, building);
    pfree(tokenMeta);
    return;
}

static FORCE_INLINE int ComparePostingFunc(const void *left, const void *right)
{
    BM25TokenPostingPage leftToken = (BM25TokenPostingPage)left;
    BM25TokenPostingPage rightToken = (BM25TokenPostingPage)right;
    return leftToken->docId - rightToken->docId;
}

static void ReorderPosting(Relation index, BlockNumber postingBlkno, uint32 docCount, bool building = true)
{
    BM25TokenPostingPage postings = (BM25TokenPostingPage)palloc0(sizeof(BM25TokenPostingItem) * docCount);
    BlockNumber nextblkno = postingBlkno;
    uint32 docIdx = 0;
    Buffer cbuf;
    Page cpage;
    GenericXLogState *state = nullptr;
    while (BlockNumberIsValid(nextblkno)) {
        OffsetNumber maxoffno;
        cbuf = ReadBuffer(index, nextblkno);
        LockBuffer(cbuf, BUFFER_LOCK_SHARE);
        cpage = BufferGetPage(cbuf);
        maxoffno = PageGetMaxOffsetNumber(cpage);
        for (OffsetNumber offno = FirstOffsetNumber; offno <= maxoffno; offno = OffsetNumberNext(offno)) {
            BM25TokenPostingPage item = (BM25TokenPostingPage)PageGetItem(cpage, PageGetItemId(cpage, offno));
            if (docIdx < docCount) {
                postings[docIdx] = *item;
                docIdx++;
            }
        }
        nextblkno = BM25PageGetOpaque(cpage)->nextblkno;
        UnlockReleaseBuffer(cbuf);
    }
    qsort(postings, (size_t)docIdx, sizeof(BM25TokenPostingItem), ComparePostingFunc);

    // rewrite to posting list
    docIdx = 0;
    nextblkno = postingBlkno;
    while (BlockNumberIsValid(nextblkno)) {
        OffsetNumber maxoffno;
        cbuf = ReadBuffer(index, nextblkno);
        LockBuffer(cbuf, BUFFER_LOCK_EXCLUSIVE);
        BM25GetPage(index, &cpage, cbuf, &state, building);
        maxoffno = PageGetMaxOffsetNumber(cpage);
        for (OffsetNumber offno = FirstOffsetNumber; offno <= maxoffno; offno = OffsetNumberNext(offno)) {
            BM25TokenPostingPage item = (BM25TokenPostingPage)PageGetItem(cpage, PageGetItemId(cpage, offno));
            if (docIdx < docCount) {
                *item = postings[docIdx];
                docIdx++;
            }
        }
        nextblkno = BM25PageGetOpaque(cpage)->nextblkno;
        BM25CommitBuf(cbuf, &state, building);
    }
    pfree_ext(postings);
    return;
}

static void InsertItemToPostingList(Relation index, BM25PageLocationInfo &tokenMetaLocation,
    BM25TokenData &tokenData, uint32 docLength, uint32 docId, float score, ForkNumber forkNum, bool building)
{
    Page cpageTokenMeta;
    BlockNumber postingBlkno = InvalidBlockNumber;
    uint32 docCount = 0;
    GenericXLogState *metaState = nullptr;
    Buffer cbufTokenMeta = ReadBuffer(index, tokenMetaLocation.blkno);
    LockBuffer(cbufTokenMeta, BUFFER_LOCK_EXCLUSIVE);
    BM25GetPage(index, &cpageTokenMeta, cbufTokenMeta, &metaState, building);
    BM25TokenMetaPage tokenMeta = (BM25TokenMetaPage)PageGetItem(cpageTokenMeta,
        PageGetItemId(cpageTokenMeta, tokenMetaLocation.offno));
    if (tokenMeta->postingBlkno == InvalidBlockNumber) {
        tokenMeta->postingBlkno = CreateBM25CommonPage(index, forkNum, building);
        tokenMeta->lastInsertBlkno = tokenMeta->postingBlkno;
    }
    postingBlkno = tokenMeta->postingBlkno;
    tokenMeta->maxScore = (score > tokenMeta->maxScore) ? score : tokenMeta->maxScore;
    (tokenMeta->docCount)++;
    docCount = tokenMeta->docCount;

    BlockNumber insertPage = tokenMeta->lastInsertBlkno;
    Buffer cbuf;
    Page cpage;
    GenericXLogState *state = nullptr;
    for (;;) {
        OffsetNumber maxoffno;
        cbuf = ReadBuffer(index, insertPage);
        LockBuffer(cbuf, BUFFER_LOCK_EXCLUSIVE);
        BM25GetPage(index, &cpage, cbuf, &state, building);

        if (PageGetFreeSpace(cpage) >= MAXALIGN(sizeof(BM25TokenPostingItem))) {
            break;
        }
        insertPage = BM25PageGetOpaque(cpage)->nextblkno;
        if (BlockNumberIsValid(insertPage)) {
            if (!building)
                GenericXLogAbort(state);
            UnlockReleaseBuffer(cbuf);
        } else {
            Buffer newbuf;
            Page newpage;
            newbuf = BM25NewBuffer(index, forkNum);
            insertPage = BufferGetBlockNumber(newbuf);
            BM25PageGetOpaque(cpage)->nextblkno = insertPage;
            BM25CommitBuf(cbuf, &state, building);
            BM25GetPage(index, &cpage, newbuf, &state, building);
            BM25InitPage(newbuf, cpage);
            cbuf = newbuf;
            break;
        }
    }

    tokenMeta->lastInsertBlkno = insertPage;
    BM25TokenPostingPage postingItem = (BM25TokenPostingPage)palloc0(MAXALIGN(sizeof(BM25TokenPostingItem)));
    postingItem->docId = docId;
    postingItem->docLength = (uint16)(docLength > PG_UINT16_MAX ? PG_UINT16_MAX : docLength);
    postingItem->freq = (uint16)(tokenData.tokenFreq > PG_UINT16_MAX ? PG_UINT16_MAX : tokenData.tokenFreq);
    OffsetNumber offno = PageAddItem(cpage, (Item)postingItem, MAXALIGN(sizeof(BM25TokenPostingItem)),
        InvalidOffsetNumber, false, false);
    if (offno == InvalidOffsetNumber) {
        pfree(postingItem);
        if (!building)
            GenericXLogAbort(state);
        UnlockReleaseBuffer(cbuf);
        elog(ERROR, "failed to add index item [BM25TokenPostingItem] to \"%s\"", RelationGetRelationName(index));
    }
    BM25CommitBuf(cbuf, &state, building);
    if (!building) {
        ReorderPosting(index, postingBlkno, docCount, false);
    }

    BM25CommitBuf(cbufTokenMeta, &metaState, building);
    return;
}

static void InsertToIvertedList(Relation index, uint32 docId, BM25TokenizedDocData &tokenizedDoc, float avgdl,
    BM25EntryPages &bm25EntryPages, ForkNumber forkNum, bool building)
{
    BM25Scorer scorer = BM25Scorer(u_sess->attr.attr_sql.bm25_k1, u_sess->attr.attr_sql.bm25_b, avgdl);
    float docLen = 0;
    for (uint32 tokenIdx = 0; tokenIdx < tokenizedDoc.tokenCount; tokenIdx++) {
        float freqVal = tokenizedDoc.tokenDatas[tokenIdx].tokenFreq;
        docLen += freqVal;
        float score = scorer.GetDocBM25Score(freqVal, docLen);
        uint32 bucketIdx = tokenizedDoc.tokenDatas[tokenIdx].hashValue %
            (bm25EntryPages.maxHashBucketCount * BM25_BUCKET_PAGE_ITEM_SIZE);
        BM25PageLocationInfo tokenMetaLocation{0};
        InsertItemToTokenMetaList(index, bm25EntryPages, bucketIdx, tokenizedDoc.tokenDatas[tokenIdx],
            tokenMetaLocation, forkNum, building);
        InsertItemToPostingList(index, tokenMetaLocation, tokenizedDoc.tokenDatas[tokenIdx], tokenizedDoc.docLength,
            docId, score, forkNum, building);
    }
    return;
}

static void FreeBuildState(BM25BuildState *buildstate)
{
    MemoryContextDelete(buildstate->tmpCtx);
}

static void AllocateForwardIdxForToken(Relation index, uint32 tokenCount, BM25EntryPages &bm25EntryPages, uint64 *start,
    uint64 *end, ForkNumber forkNum, bool building)
{
    Buffer buf;
    Page page;
    GenericXLogState *state = nullptr;
    Buffer metabuf;
    Page metaPage;
    GenericXLogState *metaState = nullptr;
    BM25DocForwardMetaPage metaForwardPage;

    /* open forward list */
    metabuf = ReadBuffer(index, bm25EntryPages.docForwardPage);
    LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);
    BM25GetPage(index, &metaPage, metabuf, &metaState, building);
    metaForwardPage = BM25PageGetDocForwardMeta(metaPage);
    /* init first data page of forward list */
    if (metaForwardPage->capacity == 0) {
        buf = BM25NewBuffer(index, forkNum);
        BM25GetPage(index, &page, buf, &state, building);
        BM25InitPage(buf, page);
        BlockNumber newblk = BufferGetBlockNumber(buf);
        metaForwardPage->startPage = newblk;
        metaForwardPage->lastPage = newblk;
        metaForwardPage->size = 0;
        metaForwardPage->capacity = BM25_DOC_FORWARD_MAX_COUNT_IN_PAGE;
        metaForwardPage->docForwardBlknoTable = InvalidBlockNumber;
        metaForwardPage->docForwardBlknoInsertPage = InvalidBlockNumber;
        RecordDocForwardBlkno2DocForwardBlknoTable(index, metaForwardPage, newblk, building, forkNum);
        BM25CommitBuf(buf, &state, building);
    }
    /* need expand data page for forward list */
    if (metaForwardPage->capacity - metaForwardPage->size < tokenCount) {
        buf = ReadBuffer(index, metaForwardPage->lastPage);
        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
        BM25GetPage(index, &page, buf, &state, building);
        /* start append */
        while (metaForwardPage->capacity - metaForwardPage->size < tokenCount) {
            BM25AppendPage(index, &buf, &page, MAIN_FORKNUM, &state, building);
            BlockNumber newblk = BufferGetBlockNumber(buf);
            metaForwardPage->lastPage = newblk;
            metaForwardPage->capacity += BM25_DOC_FORWARD_MAX_COUNT_IN_PAGE;
            RecordDocForwardBlkno2DocForwardBlknoTable(index, metaForwardPage, newblk, building, forkNum);
        }
        BM25CommitBuf(buf, &state, building);
    }
    *start = metaForwardPage->size;
    *end = metaForwardPage->size + tokenCount - 1;
    metaForwardPage->size += tokenCount;

    BM25CommitBuf(metabuf, &metaState, building);
}

static BlockNumber SeekForwardBlknoForToken(Relation index, BlockNumber startBlkno, BlockNumber step)
{
    Buffer buf;
    Page page;
    BlockNumber curBlkno = startBlkno;
    for (int i = 0; i < step; ++i) {
        buf = ReadBuffer(index, curBlkno);
        LockBuffer(buf, BUFFER_LOCK_SHARE);
        page = BufferGetPage(buf);
        curBlkno = BM25PageGetOpaque(page)->nextblkno;
        UnlockReleaseBuffer(buf);
    }
    return curBlkno;
}

static void InsertDocForwardItem(Relation index, uint32 docId, BM25TokenizedDocData &tokenizedDoc,
    BM25EntryPages &bm25EntryPages, uint64 forwardStart, uint64 forwardEnd, ForkNumber forkNum, bool building)
{
    Buffer buf;
    Page page;
    Buffer metabuf;
    Page metapage;
    uint16 offset;
    uint64 tokenIdx = forwardStart;
    BlockNumber forwardBlkno;
    BlockNumber docForwardBlknoTable;
    BlockNumber curBlockIdx;
    BlockNumber preBlockIdx;
    GenericXLogState *state = nullptr;
    BM25DocForwardMetaPage metaForwardPage;

    /* open forward list */
    metabuf = ReadBuffer(index, bm25EntryPages.docForwardPage);
    LockBuffer(metabuf, BUFFER_LOCK_SHARE);
    metapage = BufferGetPage(metabuf);
    metaForwardPage = BM25PageGetDocForwardMeta(metapage);
    docForwardBlknoTable = metaForwardPage->docForwardBlknoTable;
    UnlockReleaseBuffer(metabuf);

    curBlockIdx = tokenIdx / BM25_DOC_FORWARD_MAX_COUNT_IN_PAGE;
    preBlockIdx = curBlockIdx;
    forwardBlkno = SeekBlocknoForForwardToken(index, tokenIdx, docForwardBlknoTable);
    buf = ReadBuffer(index, forwardBlkno);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    BM25GetPage(index, &page, buf, &state, building);
    for (int i = 0; i < tokenizedDoc.tokenCount; i++) {
        Assert(forwardEnd >= tokenIdx);
        curBlockIdx = tokenIdx / BM25_DOC_FORWARD_MAX_COUNT_IN_PAGE;
        if (curBlockIdx != preBlockIdx) {
            forwardBlkno = BM25PageGetOpaque(page)->nextblkno;
            BM25CommitBuf(buf, &state, building);
            buf = ReadBuffer(index, forwardBlkno);
            LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
            BM25GetPage(index, &page, buf, &state, building);
            preBlockIdx = curBlockIdx;
        }
        offset = tokenIdx % BM25_DOC_FORWARD_MAX_COUNT_IN_PAGE;
        BM25DocForwardItem *forwardItem =
            (BM25DocForwardItem*)((char *)page + sizeof(PageHeaderData) + offset * BM25_DOCUMENT_FORWARD_ITEM_SIZE);
        forwardItem->tokenId = tokenizedDoc.tokenDatas[i].tokenId;
        forwardItem->tokenHash = tokenizedDoc.tokenDatas[i].hashValue;
        forwardItem->docId = docId;
        tokenIdx++;
    }
    BM25CommitBuf(buf, &state, building);
}

static void ExpandDocumentListCapacityIfNeed(Relation index, BM25DocMetaPage docMetaPage, uint32 docId,
    ForkNumber forkNum, bool building)
{
    Buffer buf;
    Page page;
    GenericXLogState *state = nullptr;
    /* start doc page */
    if (docMetaPage->docCapacity == 0) {
        buf = BM25NewBuffer(index, forkNum);
        BM25GetPage(index, &page, buf, &state, building);
        BM25InitPage(buf, page);
        BlockNumber newblk = BufferGetBlockNumber(buf);
        docMetaPage->startDocPage = newblk;
        docMetaPage->lastDocPage = newblk;
        docMetaPage->docCapacity = BM25_DOCUMENT_MAX_COUNT_IN_PAGE;
        docMetaPage->docBlknoTable = InvalidBlockNumber;
        docMetaPage->docBlknoInsertPage = InvalidBlockNumber;
        RecordDocBlkno2DocBlknoTable(index, docMetaPage, newblk, building, forkNum);
        BM25CommitBuf(buf, &state, building);
    }
    /* need expand new page */
    if (docMetaPage->docCapacity <= docId) {
        buf = ReadBuffer(index, docMetaPage->lastDocPage);
        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
        BM25GetPage(index, &page, buf, &state, building);
        /* start append */
        while (docMetaPage->docCapacity <= docId) {
            BM25AppendPage(index, &buf, &page, MAIN_FORKNUM, &state, building);
            BlockNumber newblk = BufferGetBlockNumber(buf);
            docMetaPage->lastDocPage = newblk;
            docMetaPage->docCapacity += BM25_DOCUMENT_MAX_COUNT_IN_PAGE;
            RecordDocBlkno2DocBlknoTable(index, docMetaPage, newblk, building, forkNum);
        }
        BM25CommitBuf(buf, &state, building);
    }
}

static void InsertDocumentItem(Relation index, uint32 docId, BM25TokenizedDocData &tokenizedDoc, ItemPointerData &ctid,
    BM25EntryPages &bm25EntryPages, ForkNumber forkNum, bool building)
{
    Buffer buf;
    Page page;
    GenericXLogState *state = nullptr;
    BlockNumber docBlkno;
    BlockNumber docBlknoTable;
    uint16 docOffset;
    Buffer metabuf;
    Page metapage;
    GenericXLogState *metaState = nullptr;
    BM25DocMetaPage docMetaPage;
    uint64 forwardStart;
    uint64 forwardEnd;

    /* open document list */
    metabuf = ReadBuffer(index, bm25EntryPages.documentMetaPage);
    LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);
    BM25GetPage(index, &metapage, metabuf, &metaState, building);
    docMetaPage = BM25PageGetDocMeta(metapage);
    ExpandDocumentListCapacityIfNeed(index, docMetaPage, docId, forkNum, building);
    docBlknoTable = docMetaPage->docBlknoTable;
    BM25CommitBuf(metabuf, &metaState, building);

    /* write doc info into target blk */
    docBlkno = SeekBlocknoForDoc(index, docId, docBlknoTable);
    docOffset = docId % BM25_DOCUMENT_MAX_COUNT_IN_PAGE;
    buf = ReadBuffer(index, docBlkno);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    BM25GetPage(index, &page, buf, &state, building);
    BM25DocumentItem *docItem =
        (BM25DocumentItem*)((char *)page + sizeof(PageHeaderData) + docOffset * BM25_DOCUMENT_ITEM_SIZE);
    unsigned short infomask = 0 | BM25_DOCUMENT_ITEM_SIZE;
    docItem->ctid.t_tid = ctid;
    docItem->ctid.t_info = infomask;
    docItem->docId = docId;
    docItem->docLength = tokenizedDoc.docLength;
    docItem->isActived = true;
    if (docItem->tokenEndIdx == 0) {
        AllocateForwardIdxForToken(index, tokenizedDoc.tokenCount, bm25EntryPages, &forwardStart, &forwardEnd,
            forkNum, building);
        docItem->tokenStartIdx = forwardStart;
        docItem->tokenEndIdx = forwardEnd;
    } else {
        forwardStart = docItem->tokenStartIdx;
        forwardEnd = docItem->tokenEndIdx;
    }
    BM25CommitBuf(buf, &state, building);

    InsertDocForwardItem(index, docId, tokenizedDoc, bm25EntryPages, forwardStart, forwardEnd, forkNum, building);
}

static bool BM25InsertDocument(Relation index, Datum *values, ItemPointerData &ctid, BM25EntryPages &bm25EntryPages,
    ForkNumber forkNum, bool building)
{
    CHECK_FOR_INTERRUPTS();
    MemoryContext tempCtx = AllocSetContextCreate(CurrentMemoryContext,
                                                  "temp bm25 index context",
                                                  ALLOCSET_DEFAULT_MINSIZE,
                                                  ALLOCSET_DEFAULT_INITSIZE,
                                                  ALLOCSET_DEFAULT_MAXSIZE);
    MemoryContext oldCtx = MemoryContextSwitchTo(tempCtx);
    /* new */
    BM25TokenizedDocData tokenizedDoc = BM25DocumentTokenize(TextDatumGetCString(values[0]));
    if (tokenizedDoc.tokenCount == 0) {
        MemoryContextSwitchTo(oldCtx);
        MemoryContextDelete(tempCtx);
        return false;
    }
    uint32 docId = BM25AllocateDocId(index, building, tokenizedDoc.tokenCount);
    float avgdl = 1.f;
    BM25IncreaseDocAndTokenCount(index, tokenizedDoc.docLength, avgdl, building);
    InsertToIvertedList(index, docId, tokenizedDoc, avgdl, bm25EntryPages, forkNum, building);
    InsertDocumentItem(index, docId, tokenizedDoc, ctid, bm25EntryPages, forkNum, building);
    if (tokenizedDoc.tokenDatas != nullptr) {
        pfree(tokenizedDoc.tokenDatas);
    }
    MemoryContextSwitchTo(oldCtx);
    MemoryContextDelete(tempCtx);
    return true;
}

/*
 * Callback for building
 */
static void BM25BuildCallback(Relation index, CALLBACK_ITEM_POINTER, Datum *values, const bool *isnull,
    bool tupleIsAlive, void *state)
{
    BM25BuildState *buildstate = (BM25BuildState *)state;
    MemoryContext oldCtx;

    /* Skip nulls */
    if (isnull[0]) {
        return;
    }

    /* Use memory context since detoast can allocate */
    oldCtx = MemoryContextSwitchTo(buildstate->tmpCtx);

    /* insert document */
    BM25InsertDocument(index, values, hup->t_self, buildstate->bm25EntryPages, buildstate->forkNum, true);
    buildstate->indtuples++;

    /* Reset memory context */
    MemoryContextSwitchTo(oldCtx);
    MemoryContextReset(buildstate->tmpCtx);
}

static BlockNumber CreateDocMetaPage(Relation index, ForkNumber forkNum)
{
    Page page;
    GenericXLogState *state;
    BlockNumber metaBlkbo;
    BM25DocMetaPage docMetaPage;

    // create matepage
    Buffer buf = BM25NewBuffer(index, forkNum);
    page = BufferGetPage(buf);
    BM25InitPage(buf, page);

    docMetaPage = BM25PageGetDocMeta(page);
    docMetaPage->startDocPage = InvalidBlockNumber;
    docMetaPage->lastDocPage = InvalidBlockNumber;
    docMetaPage->docCapacity = 0;

    BM25PageGetOpaque(page)->nextblkno = InvalidBlockNumber;
    BM25PageGetOpaque(page)->page_id = BM25_PAGE_ID;
    BM25PageGetOpaque(page)->unused = 0;

    ((PageHeader)page)->pd_lower = ((char *)docMetaPage + sizeof(BM25DocumentMetaPageData)) - (char *)page;

    metaBlkbo = BufferGetBlockNumber(buf);
    MarkBufferDirty(buf);
    UnlockReleaseBuffer(buf);
    return metaBlkbo;
}

static BlockNumber CreateDocForwardMetaPage(Relation index, ForkNumber forkNum)
{
    Page page;
    BlockNumber metaBlkbo;
    BM25DocForwardMetaPage forwardMetaPage;

    // create matepage
    Buffer buf = BM25NewBuffer(index, forkNum);
    page = BufferGetPage(buf);
    BM25InitPage(buf, page);

    forwardMetaPage = BM25PageGetDocForwardMeta(page);
    forwardMetaPage->startPage = InvalidBlockNumber;
    forwardMetaPage->lastPage = InvalidBlockNumber;
    forwardMetaPage->size = 0;
    forwardMetaPage->capacity = 0;

    BM25PageGetOpaque(page)->nextblkno = InvalidBlockNumber;
    BM25PageGetOpaque(page)->page_id = BM25_PAGE_ID;
    BM25PageGetOpaque(page)->unused = 0;

    ((PageHeader)page)->pd_lower = ((char *)forwardMetaPage + sizeof(BM25DocForwardMetaPageData)) - (char *)page;

    metaBlkbo = BufferGetBlockNumber(buf);
    MarkBufferDirty(buf);
    UnlockReleaseBuffer(buf);
    return metaBlkbo;
}

static BlockNumber CreateLockPage(Relation index, ForkNumber forkNum)
{
    Buffer buf;
    Page page;
    GenericXLogState *state;
    BlockNumber lockBlkno;

    buf = ReadBufferExtended(index, forkNum, P_NEW, RBM_NORMAL, NULL);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    state = GenericXLogStart(index);
    page = GenericXLogRegisterBuffer(state, buf, GENERIC_XLOG_FULL_IMAGE);
    BM25InitPage(buf, page);
    lockBlkno = BufferGetBlockNumber(buf);
    GenericXLogFinish(state);
    UnlockReleaseBuffer(buf);
    return lockBlkno;
}

/* pre_initialize hashbuckets page for performance */
static uint32 InitHashBucketsPage(Relation index, BlockNumber hashBucketsBlkno)
{
    Buffer buf;
    Page page;
    GenericXLogState *state = nullptr;
    buf = ReadBuffer(index, hashBucketsBlkno);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    state = GenericXLogStart(index);
    page = GenericXLogRegisterBuffer(state, buf, GENERIC_XLOG_FULL_IMAGE);
    uint32 itemSize = sizeof(BM25HashBucketItem);
    BM25HashBucketPage item = (BM25HashBucketPage)palloc0(sizeof(BM25HashBucketItem));
    uint32 bucketId = 0;
    while (PageGetFreeSpace(page) >= itemSize) {
        /* 2 BlockNumber in each BM25HashBucketItem (8-byte alignment) */
        item->bucketBlkno[0] = InvalidBlockNumber;
        item->bucketBlkno[1] = InvalidBlockNumber;
        OffsetNumber offno = PageAddItem(page, (Item)item, itemSize, InvalidOffsetNumber, false, false);
        if (offno == InvalidOffsetNumber) {
            pfree(item);
            GenericXLogAbort(state);
            UnlockReleaseBuffer(buf);
            elog(ERROR, "failed to add index item [BM25HashBucketBlkno] to \"%s\"", RelationGetRelationName(index));
        }
        bucketId++;
    }
    BM25PageGetOpaque(page)->nextblkno = InvalidBlockNumber;
    GenericXLogFinish(state);
    UnlockReleaseBuffer(buf);
    pfree(item);
    elog(LOG, "Init [BM25HashBucket] maxBucketCount %u.", bucketId);
    return bucketId;
}

static BM25EntryPages CreateEntryPages(Relation index, ForkNumber forkNum)
{
    BM25EntryPages entryPages;
    entryPages.lockPage = CreateLockPage(index, forkNum);
    entryPages.documentMetaPage = CreateDocMetaPage(index, forkNum);
    entryPages.docForwardPage = CreateDocForwardMetaPage(index, forkNum);
    entryPages.hashBucketsPage = CreateBM25CommonPage(index, forkNum, true);
    entryPages.docmentFreePage = CreateBM25CommonPage(index, forkNum, true);
    entryPages.docmentFreeInsertPage = CreateBM25CommonPage(index, forkNum, true);
    entryPages.maxHashBucketCount = InitHashBucketsPage(index, entryPages.hashBucketsPage);
    return entryPages;
}

/*
 * Create the metapage
 */
static void CreateMetaPage(Relation index, BM25BuildState *buildstate, ForkNumber forkNum)
{
    Buffer buf;
    Page page;
    GenericXLogState *state;
    BM25MetaPage metap;

    buf = ReadBufferExtended(index, forkNum, P_NEW, RBM_NORMAL, NULL);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    state = GenericXLogStart(index);
    page = GenericXLogRegisterBuffer(state, buf, GENERIC_XLOG_FULL_IMAGE);
    BM25InitPage(buf, page);

    /* Set metapage data */
    metap = BM25PageGetMeta(page);
    metap->magicNumber = BM25_MAGIC_NUMBER;
    metap->version = BM25_VERSION;
    metap->entryPageList = CreateEntryPages(index, forkNum);
    metap->documentCount = 0;
    metap->tokenCount = 0;
    metap->nextDocId = 0;
    metap->nextTokenId = 0;

    buildstate->bm25EntryPages = metap->entryPageList;

    ((PageHeader)page)->pd_lower = ((char *)metap + sizeof(BM25MetaPageData)) - (char *)page;

    GenericXLogFinish(state);
    UnlockReleaseBuffer(buf);
}

static BM25Shared *BM25ParallelInitshared(BM25BuildState *buildstate)
{
    BM25Shared *bm25shared = nullptr;

    /* Store shared build state, for which we reserved space */
    bm25shared =
        (BM25Shared *)MemoryContextAllocZero(INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), sizeof(BM25Shared));

    /* Initialize immutable state */
    bm25shared->heaprelid = RelationGetRelid(buildstate->heap);
    bm25shared->indexrelid = RelationGetRelid(buildstate->index);

    bm25shared->bm25EntryPages = buildstate->bm25EntryPages;
    SpinLockInit(&bm25shared->mutex);
    /* Initialize mutable state */
    bm25shared->nparticipantsdone = 0;
    bm25shared->reltuples = 0;
    HeapParallelscanInitialize(&bm25shared->heapdesc, buildstate->heap);
    return bm25shared;
}

static void BM25ParallelScanAndInsert(Relation heapRel, Relation indexRel, BM25Shared *bm25shared)
{
    BM25BuildState buildstate;
    TableScanDesc scan;
    double reltuples;
    IndexInfo *indexInfo;

    /* Join parallel scan */
    indexInfo = BuildIndexInfo(indexRel);
    InitBM25BuildState(&buildstate, heapRel, indexRel, indexInfo, MAIN_FORKNUM);
    buildstate.bm25EntryPages = bm25shared->bm25EntryPages;

    scan = tableam_scan_begin_parallel(heapRel, &bm25shared->heapdesc);
    reltuples = tableam_index_build_scan(heapRel, indexRel, indexInfo, true, BM25BuildCallback,
        (void *)&buildstate, scan);

    /* Record statistics */
    SpinLockAcquire(&bm25shared->mutex);
    bm25shared->nparticipantsdone++;
    bm25shared->reltuples += reltuples;
    SpinLockRelease(&bm25shared->mutex);

    FreeBuildState(&buildstate);
}

void BM25ParallelBuildMain(const BgWorkerContext *bwc)
{
    BM25Shared *bm25shared = nullptr;
    Relation heapRel;
    Relation indexRel;

    /* Look up shared state */
    bm25shared = (BM25Shared *)bwc->bgshared;

    /* Open relations within worker */
    heapRel = heap_open(bm25shared->heaprelid, NoLock);
    indexRel = index_open(bm25shared->indexrelid, NoLock);

    /* Perform inserts */
    BM25ParallelScanAndInsert(heapRel, indexRel, bm25shared);

    /* Close relations within worker */
    index_close(indexRel, NoLock);
    heap_close(heapRel, NoLock);
}

static void BM25EndParallel(BM25Leader *bm25leader)
{
    pfree_ext(bm25leader);
    BgworkerListSyncQuit();
}

/*
 * Begin parallel build
 */
static void BM25BeginParallel(BM25BuildState *buildstate, int request)
{
    BM25Shared *bm25shared = nullptr;
    BM25Leader *bm25leader = (BM25Leader *)palloc0(sizeof(BM25Leader));

    Assert(request > 0);

    bm25shared = BM25ParallelInitshared(buildstate);
    /* Launch workers, saving status for leader/caller */
    bm25leader->nparticipanttuplesorts = LaunchBackgroundWorkers(request, bm25shared, BM25ParallelBuildMain, NULL);
    bm25leader->bm25shared = bm25shared;

    /* If no workers were successfully launched, back out (do serial build) */
    if (bm25leader->nparticipanttuplesorts == 0) {
        BM25EndParallel(bm25leader);
        return;
    }

    /* Log participants */
    ereport(DEBUG1, (errmsg("using %d parallel workers", bm25leader->nparticipanttuplesorts)));

    /* Save leader state now that it's clear build will be parallel */
    buildstate->bm25leader = bm25leader;
}

static double ParallelHeapScan(BM25BuildState *buildstate, int *nparticipanttuplesorts)
{
    BM25Shared *bm25shared = buildstate->bm25leader->bm25shared;
    double reltuples;

    BgworkerListWaitFinish(&buildstate->bm25leader->nparticipanttuplesorts);
    pg_memory_barrier();

    *nparticipanttuplesorts = buildstate->bm25leader->nparticipanttuplesorts;
    reltuples = bm25shared->reltuples;

    return reltuples;
}

static void ReorderBucket(Relation index, BlockNumber bucketBlkno)
{
    // loop buckets
    BlockNumber nextblkno = bucketBlkno;
    Buffer cbuf;
    Page cpage;
    while (BlockNumberIsValid(nextblkno)) {
        OffsetNumber maxoffno;
        cbuf = ReadBuffer(index, nextblkno);
        LockBuffer(cbuf, BUFFER_LOCK_SHARE);
        cpage = BufferGetPage(cbuf);
        maxoffno = PageGetMaxOffsetNumber(cpage);
        for (OffsetNumber offno = FirstOffsetNumber; offno <= maxoffno; offno = OffsetNumberNext(offno)) {
            BM25TokenMetaPage item = (BM25TokenMetaPage)PageGetItem(cpage, PageGetItemId(cpage, offno));
            ReorderPosting(index, item->postingBlkno, item->docCount);
        }
        nextblkno = BM25PageGetOpaque(cpage)->nextblkno;
        UnlockReleaseBuffer(cbuf);
    }
    return;
}

void ParallelReorderMain(const BgWorkerContext *bwc)
{
    Relation heapRel;
    Relation indexRel;

    /* Look up shared state */
    BM25ReorderShared *reorderShared = (BM25ReorderShared *)bwc->bgshared;
    uint32 curThreadId = pg_atomic_add_fetch_u32(&reorderShared->curThreadId, 1);

    /* Open relations within worker */
    heapRel = heap_open(reorderShared->heaprelid, NoLock);
    indexRel = index_open(reorderShared->indexrelid, NoLock);

    BM25PageLocationInfo startLocation = reorderShared->startPageLocation[curThreadId - 1];
    ereport(LOG, (errmsg("launch reorder background threadId: %d.", curThreadId)));

    // loop buckets
    BlockNumber hashBucketsBlkno = startLocation.blkno;
    OffsetNumber startoffno = startLocation.offno;
    bool isStartPage = true;
    bool isEnd = false;
    uint32 scanBucketCount = 0;
    Buffer cbuf;
    Page cpage;
    if (BlockNumberIsValid(hashBucketsBlkno)) {
        OffsetNumber maxoffno;
        cbuf = ReadBuffer(indexRel, hashBucketsBlkno);
        LockBuffer(cbuf, BUFFER_LOCK_SHARE);
        cpage = BufferGetPage(cbuf);
        maxoffno = PageGetMaxOffsetNumber(cpage);
        for (OffsetNumber offno = startoffno; offno <= maxoffno; offno = OffsetNumberNext(offno)) {
            BM25HashBucketPage bucketInfo = (BM25HashBucketPage)PageGetItem(cpage, PageGetItemId(cpage, offno));
            scanBucketCount++;
            ReorderBucket(indexRel, bucketInfo->bucketBlkno[0]);
            ReorderBucket(indexRel, bucketInfo->bucketBlkno[1]);
            if (scanBucketCount >= reorderShared->batchCount) {
                break;
            }
        }
        UnlockReleaseBuffer(cbuf);
    }

    /* Close relations within worker */
    index_close(indexRel, NoLock);
    heap_close(heapRel, NoLock);
}

static void BM25InitReorderShared(BM25ReorderShared *reorderShared, BM25BuildState *buildstate,
    BlockNumber hashBucketsPage, uint32 &reorderParallelNum, uint32 batchHashBucketCount)
{
    reorderShared->startPageLocation =
        (BM25PageLocationInfo*)palloc0(sizeof(BM25PageLocationInfo) * reorderParallelNum);
    for (uint32 idx = 0; idx < reorderParallelNum; ++idx) {
        reorderShared->startPageLocation[idx].blkno = InvalidBlockNumber;
        reorderShared->startPageLocation[idx].offno = InvalidOffsetNumber;
    }
    reorderShared->batchCount = batchHashBucketCount;
    reorderShared->heaprelid = RelationGetRelid(buildstate->heap);
    reorderShared->indexrelid = RelationGetRelid(buildstate->index);
    pg_atomic_init_u32(&reorderShared->curThreadId, 0);

    Buffer cbuf;
    Page cpage;
    uint32 curHashBucketCount = 0;
    uint32 curBatchIdx = 0;
    if (BlockNumberIsValid(hashBucketsPage)) {
        OffsetNumber maxoffno;
        cbuf = ReadBuffer(buildstate->index, hashBucketsPage);
        LockBuffer(cbuf, BUFFER_LOCK_SHARE);
        cpage = BufferGetPage(cbuf);
        maxoffno = PageGetMaxOffsetNumber(cpage);
        for (OffsetNumber offno = FirstOffsetNumber; offno <= maxoffno; offno = OffsetNumberNext(offno)) {
            if (curHashBucketCount == 0 && curBatchIdx < reorderParallelNum) {
                reorderShared->startPageLocation[curBatchIdx].blkno = hashBucketsPage;
                reorderShared->startPageLocation[curBatchIdx].offno = offno;
                curBatchIdx++;
            }
            curHashBucketCount++;
            if (curHashBucketCount >= batchHashBucketCount) {
                curHashBucketCount = 0;
            }
        }
        UnlockReleaseBuffer(cbuf);
    }
    reorderParallelNum = curBatchIdx;
    return;
}

static void BuildBM25Index(BM25BuildState *buildstate, ForkNumber forkNum)
{
    int parallelWorkers = 0;

    /* Calculate parallel workers */
    if (buildstate->heap != NULL) {
        parallelWorkers = PlanCreateIndexWorkers(buildstate->heap, buildstate->indexInfo);
    }

    /* Attempt to launch parallel worker scan when required */
    if (parallelWorkers > 0) {
        BM25BeginParallel(buildstate, parallelWorkers);
    }

    if (buildstate->heap != NULL) {
        if (!buildstate->bm25leader) {
        serial_build:
        buildstate->reltuples = tableam_index_build_scan(buildstate->heap, buildstate->index, buildstate->indexInfo,
            false, BM25BuildCallback, (void *)buildstate, NULL);
        } else {
            int nruns;
            buildstate->reltuples = ParallelHeapScan(buildstate, &nruns);
            if (nruns == 0) {
                /* failed to startup any bgworker, retry to do serial build */
                goto serial_build;
            }
        }
    }

    if (buildstate->bm25leader) {
        uint32 hashBucketCount = buildstate->bm25leader->bm25shared->bm25EntryPages.maxHashBucketCount;
        BlockNumber hashBucketsPage = buildstate->bm25leader->bm25shared->bm25EntryPages.hashBucketsPage;
        BM25EndParallel(buildstate->bm25leader);

        /* reorder posting list */
        uint32 reorderParallelNum = hashBucketCount < parallelWorkers ? hashBucketCount : parallelWorkers;
        if (reorderParallelNum == 0) {
            return;
        }
        uint32 batchHashBucketCount = (hashBucketCount / reorderParallelNum) + 1;
        BM25ReorderShared *reorderShared =
            (BM25ReorderShared *)MemoryContextAllocZero(INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE),
                sizeof(BM25ReorderShared));
        BM25InitReorderShared(reorderShared, buildstate, hashBucketsPage, reorderParallelNum, batchHashBucketCount);

        int successWorkers = LaunchBackgroundWorkers(reorderParallelNum, reorderShared, ParallelReorderMain, NULL);
        ereport(LOG, (errmsg("launch reorder background workers: %d.", successWorkers)));
        if (successWorkers == 0) {
            pfree_ext(reorderShared->startPageLocation);
            pfree_ext(reorderShared);
            ereport(ERROR, (errmsg("Failed to launch background workers: ParallelReorderMain")));
        }
        BgworkerListWaitFinish(&successWorkers);
        pfree_ext(reorderShared->startPageLocation);
        BgworkerListSyncQuit();
    }
    return;
}

/*
 * Build the index
 */
static void BuildIndex(Relation heap, Relation index, IndexInfo *indexInfo, BM25BuildState *buildstate,
                       ForkNumber forkNum)
{
    InitBM25BuildState(buildstate, heap, index, indexInfo, forkNum);
    CreateMetaPage(index, buildstate, forkNum);

    BuildBM25Index(buildstate, forkNum);

    if (RelationNeedsWAL(index) || forkNum == INIT_FORKNUM)
        LogNewpageRange(index, forkNum, 0, RelationGetNumberOfBlocksInFork(index, forkNum), false);

    FreeBuildState(buildstate);
}

static void BuildIndexCheck(Relation index)
{
    TupleDesc tupleDesc = RelationGetDescr(index);
    FormData_pg_attribute* attrs = tupleDesc->attrs;
    for (int i = 0; i < tupleDesc->natts; ++i) {
        if (attrs[i].atttypid != TEXTOID) {
            elog(ERROR, "bm25 index is only supported for datatype: text.");
        }
    }
    return;
}

IndexBuildResult* bm25build_internal(Relation heap, Relation index, IndexInfo *indexInfo)
{
    IndexBuildResult *result;
    BM25BuildState buildstate;

    BuildIndexCheck(index);
    BuildIndex(heap, index, indexInfo, &buildstate, MAIN_FORKNUM);

    result = (IndexBuildResult *)palloc(sizeof(IndexBuildResult));
    result->heap_tuples = buildstate.reltuples;
    result->index_tuples = buildstate.indtuples;
    return result;
}

void bm25buildempty_internal(Relation index)
{
    IndexBuildResult *result;
    BM25BuildState buildstate;

    BuildIndexCheck(index);
    BuildIndex(NULL, index, NULL, &buildstate, MAIN_FORKNUM);
}

bool bm25insert_internal(Relation index, Datum *values, ItemPointer heapCtid)
{
    BM25MetaPageData meta;
    BM25GetMetaPageInfo(index, &meta);

    BM25InsertDocument(index, values, *heapCtid, meta.entryPageList, MAIN_FORKNUM, false);
    return true;
}
