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
 * bm25scan.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/datavec/bm25scan.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "access/xlog.h"
#include "access/sdir.h"
#include "zlib.h"
#include "access/relscan.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "catalog/index.h"
#include "access/tableam.h"
#include "db4ai/bayesnet.h"
#include "access/datavec/bm25heap.h"
#include "access/datavec/bm25.h"

const uint32 DEFAULT_EXPAND_TIME = 8;
const float BM25_DEFAULT_OFFSET = 0.5f;

typedef struct BM25QueryToken {
    BlockNumber tokenPostingBlock;
    float qTokenMaxScore;
    float qTokenIDFVal;
} BM25QueryToken;

typedef struct BM25QueryTokensInfo {
    BM25QueryToken *queryTokens;
    uint32 size;
} BM25QueryTokensInfo;

static void FindBucketsLocation(Page page, BM25TokenizedDocData &tokenizedQuery, BlockNumber *bucketsLocation,
    uint32 maxHashBucketCount, uint32 &bucketFoundCount)
{
    OffsetNumber maxoffno = PageGetMaxOffsetNumber(page);
    for (size_t tokenIdx = 0; tokenIdx < tokenizedQuery.tokenCount; tokenIdx++) {
        uint32 bucketIdx = tokenizedQuery.tokenDatas[tokenIdx].hashValue %
            (maxHashBucketCount * BM25_BUCKET_PAGE_ITEM_SIZE);
        BM25HashBucketPage bucketInfo =
            (BM25HashBucketPage)PageGetItem(page, PageGetItemId(page, (bucketIdx / BM25_BUCKET_PAGE_ITEM_SIZE) + 1));
        if (bucketsLocation[tokenIdx] == InvalidBlockNumber &&
            bucketInfo->bucketBlkno[bucketIdx % BM25_BUCKET_PAGE_ITEM_SIZE] != InvalidBlockNumber) {
            bucketsLocation[tokenIdx] = bucketInfo->bucketBlkno[bucketIdx % BM25_BUCKET_PAGE_ITEM_SIZE];
            bucketFoundCount++;
        }
    }
    return;
}

static void FindTokenInfo(BM25MetaPageData &meta, Page page, BM25TokenizedDocData &tokenizedQuery,
    BM25QueryToken *queryTokens, size_t tokenIdx, uint32 &tokenFoundCount)
{
    OffsetNumber maxoffno = PageGetMaxOffsetNumber(page);
    for (OffsetNumber offnoTokenMeta = FirstOffsetNumber; offnoTokenMeta <= maxoffno; offnoTokenMeta++) {
        BM25TokenMetaPage tokenMeta = (BM25TokenMetaPage)PageGetItem(page, PageGetItemId(page, offnoTokenMeta));
        if ((tokenMeta->hashValue == tokenizedQuery.tokenDatas[tokenIdx].hashValue) &&
            (strncmp(tokenMeta->token, tokenizedQuery.tokenDatas[tokenIdx].tokenValue, BM25_MAX_TOKEN_LEN - 1) == 0)) {
            queryTokens[tokenIdx].qTokenMaxScore = tokenMeta->maxScore;
            queryTokens[tokenIdx].tokenPostingBlock = tokenMeta->postingBlkno;
            queryTokens[tokenIdx].qTokenIDFVal = tokenizedQuery.tokenDatas[tokenIdx].tokenFreq *
                std::log((1 + ((float)meta.documentCount - (float)tokenMeta->docCount + BM25_DEFAULT_OFFSET) /
                ((float)tokenMeta->docCount + BM25_DEFAULT_OFFSET)));
            tokenFoundCount++;
            if (tokenFoundCount >= tokenizedQuery.tokenCount)
                return;
        }
    }
    return;
}

static BM25QueryToken *ScanIndexForTokenInfo(Relation index, const char *sentence, uint32 &tokenCount,
    uint32 &tokenFoundCount, bool cutForSearch = false)
{
    BM25TokenizedDocData tokenizedQuery = BM25DocumentTokenize(sentence, cutForSearch);
    if (tokenizedQuery.tokenCount == 0) {
        tokenCount = 0;
        tokenFoundCount = 0;
        return nullptr;
    }
    tokenCount = tokenizedQuery.tokenCount;
    BM25QueryToken *queryTokens = (BM25QueryToken*)palloc0(sizeof(BM25QueryToken) * tokenizedQuery.tokenCount);
    BlockNumber *bucketsLocation = (BlockNumber*)palloc0(sizeof(BlockNumber) * tokenizedQuery.tokenCount);
    for (size_t tokenIdx = 0; tokenIdx < tokenizedQuery.tokenCount; tokenIdx++) {
        queryTokens[tokenIdx].tokenPostingBlock = InvalidBlockNumber;
        bucketsLocation[tokenIdx] = InvalidBlockNumber;
    }

   /* scan index for queryToken info */
    uint32 bucketFoundCount = 0;
    BM25MetaPageData meta;
    BM25GetMetaPageInfo(index, &meta);
    BlockNumber hashBucketsBlkno = meta.entryPageList.hashBucketsPage;
    Buffer cHashBucketsbuf;
    Page cHashBucketspage;

    if (bucketFoundCount < tokenizedQuery.tokenCount && BlockNumberIsValid(hashBucketsBlkno)) {
        cHashBucketsbuf = ReadBuffer(index, hashBucketsBlkno);
        LockBuffer(cHashBucketsbuf, BUFFER_LOCK_SHARE);
        cHashBucketspage = BufferGetPage(cHashBucketsbuf);
        FindBucketsLocation(cHashBucketspage, tokenizedQuery, bucketsLocation, meta.entryPageList.maxHashBucketCount,
            bucketFoundCount);
        UnlockReleaseBuffer(cHashBucketsbuf);
    }

    tokenFoundCount = 0;
    for (size_t tokenIdx = 0; tokenIdx < tokenizedQuery.tokenCount; tokenIdx++) {
        if (!BlockNumberIsValid(bucketsLocation[tokenIdx])) {
            continue;
        }
        Buffer cTokenMetasbuf;
        Page cTokenMetaspage;
        BlockNumber nextTokenMetasBlkno = bucketsLocation[tokenIdx];
        while (tokenFoundCount < tokenizedQuery.tokenCount && BlockNumberIsValid(nextTokenMetasBlkno)) {
            cTokenMetasbuf = ReadBuffer(index, nextTokenMetasBlkno);
            LockBuffer(cTokenMetasbuf, BUFFER_LOCK_SHARE);
            cTokenMetaspage = BufferGetPage(cTokenMetasbuf);
            FindTokenInfo(meta, cTokenMetaspage, tokenizedQuery, queryTokens, tokenIdx, tokenFoundCount);
            nextTokenMetasBlkno = BM25PageGetOpaque(cTokenMetaspage)->nextblkno;
            UnlockReleaseBuffer(cTokenMetasbuf);
        }
    }
    pfree(bucketsLocation);
    if (tokenizedQuery.tokenDatas != nullptr) {
        pfree(tokenizedQuery.tokenDatas);
    }
    if (tokenFoundCount == 0) {
        pfree(queryTokens);
        return nullptr;
    }
    return queryTokens;
}

static BM25QueryTokensInfo GetQueryTokens(Relation index, const char* sentence)
{
    uint32 tokenCount = 0;
    uint32 tokenFoundCount = 0;
    BM25QueryToken *queryTokens = ScanIndexForTokenInfo(index, sentence, tokenCount, tokenFoundCount);
    if (queryTokens == nullptr) {
        /* no token found, try to use cutForSearch to get tokens */
        queryTokens = ScanIndexForTokenInfo(index, sentence, tokenCount, tokenFoundCount, true);
    }
    if (queryTokens == nullptr) {
        BM25QueryTokensInfo tokensInfo{0};
        tokensInfo.queryTokens = nullptr;
        tokensInfo.size = 0;
        return tokensInfo;
    }

    BM25QueryToken *resQueryTokens = (BM25QueryToken*)palloc0(sizeof(BM25QueryToken) * tokenFoundCount);
    uint32 tokenFillIdx = 0;
    for (size_t tokenIdx = 0; tokenIdx < tokenCount; tokenIdx++) {
        if (!BlockNumberIsValid(queryTokens[tokenIdx].tokenPostingBlock)) {
            continue;
        }
        resQueryTokens[tokenFillIdx] = queryTokens[tokenIdx];
        tokenFillIdx++;
        if (tokenFillIdx >= tokenFoundCount) {
            break;
        }
    }
    pfree(queryTokens);
    BM25QueryTokensInfo tokensInfo{0};
    tokensInfo.queryTokens = resQueryTokens;
    tokensInfo.size = tokenFoundCount;
    return tokensInfo;
}

struct BM25ScanScoreHashEntry {
    bool isOccupied;
    uint32 hash;
    char* scoreKey;
    float score;

    void SetValues(uint32 hashVal, char* doc, float docScore)
    {
        isOccupied = true;
        hash = hashVal;
        scoreKey = doc;
        score = docScore;
    }
};

struct BM25ScanDocScoreHashTable : public BaseObject {
    static constexpr uint32 INIT_TABLE_CAPACITY = 16;
    static constexpr uint32 INIT_TABLE_SHIFT = 4;
    static constexpr uint8_t MAX_TABLE_SHIFT = 63;
public:
    BM25ScanDocScoreHashTable(uint32 maxDocCount)
    {
        size_t capacity = INIT_TABLE_CAPACITY;
        uint8_t shift = INIT_TABLE_SHIFT;
        while (shift < MAX_TABLE_SHIFT && capacity < maxDocCount) {
            capacity <<= 1;
            shift++;
        }
        capacity <<= 1;
        scoreArray = (BM25ScanScoreHashEntry*)palloc0(sizeof(BM25ScanScoreHashEntry) * capacity);
        scoreHashCapacity = capacity;
    }

    uint32 GetDocHash(const char* doc)
    {
        uint32_t crc = crc32(0, Z_NULL, 0);
        crc = crc32(crc, reinterpret_cast<const Bytef*>(doc), strlen(doc));
        return crc;
    }

    uint32 GetHashBucketIdxByHash(uint32 hash)
    {
        return (uint32)(hash % scoreHashCapacity);
    }

    void AddScore(float score, char *doc)
    {
        uint32 hash = GetDocHash(doc);
        uint32 bucketIdx = GetHashBucketIdxByHash(hash);
        while (bucketIdx < scoreHashCapacity) {
            BM25ScanScoreHashEntry* entry = &scoreArray[bucketIdx];
            if (!entry->isOccupied) {
                entry->SetValues(hash, doc, score);
                break;
            }
            bucketIdx = (bucketIdx + 1) % scoreHashCapacity;
        }
    }

    float SearchScoreForDoc(char *doc, bool *findDoc)
    {
        uint32 hash = GetDocHash(doc);
        uint32 bucketIdx = GetHashBucketIdxByHash(hash);

        while (bucketIdx < scoreHashCapacity) {
            BM25ScanScoreHashEntry* entry = &scoreArray[bucketIdx];
            if (entry->isOccupied) {
                if (entry->hash == hash && strcmp(entry->scoreKey, doc) == 0) {
                    *findDoc = true;
                    return entry->score;
                }
            } else {
                *findDoc = false;
                return 0.0;
            }
            bucketIdx = (bucketIdx + 1) % scoreHashCapacity;
        }
        return 0.0;
    }

    void Destroy()
    {
        pfree_ext(scoreArray);
    }

private:
    BM25ScanScoreHashEntry* scoreArray;
    size_t scoreHashCapacity;
};

void BM25ScanCursor::Next(bool isInit)
{
    BM25TokenPostingPage postingItem;
    bool found = false;

    if (isInit) {
        Assert(BlockNumberIsValid(curBlkno));
        buf = ReadBuffer(index, curBlkno);
        LockBuffer(buf, BUFFER_LOCK_SHARE);
        page = BufferGetPage(buf);
    }

    while (BlockNumberIsValid(curBlkno)) {
        OffsetNumber maxoffno = PageGetMaxOffsetNumber(page);
        OffsetNumber nextoffno = InvalidOffsetNumber;
        if (!OffsetNumberIsValid(curOffset)) {
            nextoffno = FirstOffsetNumber;
        } else {
            nextoffno = OffsetNumberNext(curOffset);
        }
        while (OffsetNumberIsValid(nextoffno) && nextoffno <= maxoffno) {
            postingItem = (BM25TokenPostingPage)PageGetItem(page, PageGetItemId(page, nextoffno));
            uint32 docId = postingItem->docId;
            bool filtered = (docIdfilter[docId >> 3] >> (docId % 8)) & 1;
            if (filtered) {
                nextoffno = OffsetNumberNext(nextoffno);
                continue;
            }
            curDocId = postingItem->docId;
            tokenFreqInDoc = postingItem->freq;
            curDocLength = postingItem->docLength;
            curOffset = nextoffno;
            found = true;
            break;
        }
        if (found) {
            break;
        }
        curBlkno = BM25PageGetOpaque(page)->nextblkno;
        curOffset = InvalidOffsetNumber;
        UnlockReleaseBuffer(buf);
        buf = InvalidBuffer;
        if (BlockNumberIsValid(curBlkno)) {
            buf = ReadBuffer(index, curBlkno);
            LockBuffer(buf, BUFFER_LOCK_SHARE);
            page = BufferGetPage(buf);
        }
    }

    if (!BlockNumberIsValid(curBlkno)) {
        curDocId = BM25_INVALID_DOC_ID;
    }
}

void BM25ScanCursor::Seek(uint32 docId)
{
    if (docId == curDocId) {
        return;
    }

    Assert(docId != BM25_INVALID_DOC_ID);
    while (BlockNumberIsValid(curBlkno)) {
        OffsetNumber maxoffno = PageGetMaxOffsetNumber(page);
        for (OffsetNumber offno = curOffset; offno <= maxoffno; offno = OffsetNumberNext(offno)) {
            BM25TokenPostingPage postingItem = (BM25TokenPostingPage)PageGetItem(page, PageGetItemId(page, offno));
            if (postingItem->docId >= docId) {
                curDocId = postingItem->docId;
                tokenFreqInDoc = postingItem->freq;
                curDocLength = postingItem->docLength;
                curOffset = offno;
                return;
            }
        }
        curBlkno = BM25PageGetOpaque(page)->nextblkno;
        UnlockReleaseBuffer(buf);
        buf = InvalidBuffer;
        if (BlockNumberIsValid(curBlkno)) {
            buf = ReadBuffer(index, curBlkno);
            LockBuffer(buf, BUFFER_LOCK_SHARE);
            page = BufferGetPage(buf);
            curOffset = FirstOffsetNumber;
        }
    }
    if (!BlockNumberIsValid(curBlkno)) {
        curDocId = BM25_INVALID_DOC_ID;
    }
}

void BM25ScanCursor::Close()
{
    if (BufferIsValid(buf)) {
        UnlockReleaseBuffer(buf);
    }
    docIdfilter = nullptr;
}

static Vector<BM25ScanCursor> MakeBM25ScanCursors(Relation index, BM25QueryToken* queryTokens, uint32 querySize,
    unsigned char* docIdMask)
{
    Vector<BM25ScanCursor> cursors;
    for (int i = 0; i < querySize; ++i) {
        BM25ScanCursor cursor(index, queryTokens[i].tokenPostingBlock,
            queryTokens[i].qTokenMaxScore * queryTokens[i].qTokenIDFVal * u_sess->attr.attr_sql.max_score_ratio,
            queryTokens[i].qTokenIDFVal, docIdMask);
        cursors.push_back(cursor);
    }
    return cursors;
}

static void CloseCursors(Vector<BM25ScanCursor> &cursors)
{
    for (int i = 0; i < cursors.size(); ++i) {
        cursors[i].Close();
    }
    cursors.clear();
}

static void SearchTaat(Relation index, BM25QueryTokensInfo &queryTokenInfo, MaxMinHeap& heap,
    uint32 maxDocId, BM25Scorer& scorer, unsigned char* docIdMask)
{
    BM25QueryToken *queryTokens = queryTokenInfo.queryTokens;
    uint32 querySize = queryTokenInfo.size;
    Vector<BM25ScanCursor> cursors = MakeBM25ScanCursors(index, queryTokens, querySize, docIdMask);
    Vector<float> scores(maxDocId);
    for (size_t i = 0; i < querySize; ++i) {
        BM25ScanCursor* cursor = &cursors[i];
        while (cursor->curDocId < maxDocId) {
            scores[cursor->curDocId] += cursor->qTokenIDFVal *
                scorer.GetDocBM25Score(cursor->tokenFreqInDoc, cursor->curDocLength);
            cursor->Next();
        }
        cursor->Close();
    }
    for (size_t i = 0; i < maxDocId; ++i) {
        if (scores[i] != 0) {
            heap.push(i, scores[i]);
        }
    }
    scores.clear();
}

static FORCE_INLINE int CompareQueryTokenFunc(const void *left, const void *right)
{
    BM25QueryToken* leftToken = (BM25QueryToken*)left;
    BM25QueryToken* rightToken = (BM25QueryToken*)right;
    return rightToken->qTokenIDFVal * rightToken->qTokenMaxScore - leftToken->qTokenIDFVal * leftToken->qTokenMaxScore;
}

static void SearchDaatMaxscore(Relation index, BM25QueryTokensInfo &queryTokenInfo, MaxMinHeap& heap,
    uint32 maxDocId, BM25Scorer& scorer, unsigned char* docIdMask)
{
    BM25QueryToken *queryTokens = queryTokenInfo.queryTokens;
    uint32 querySize = queryTokenInfo.size;
    qsort(queryTokens, (size_t)querySize, sizeof(BM25QueryToken), CompareQueryTokenFunc);

    Vector<BM25ScanCursor> cursors = MakeBM25ScanCursors(index, queryTokens, querySize, docIdMask);

    float threshold = heap.full() ? heap.top().val : 0;

    Vector<float> upperBounds(cursors.size());
    float boundSum = 0.0;
    for (size_t i = cursors.size() - 1; i + 1 > 0; --i) {
        boundSum += cursors[i].qTokenMaxScore;
        upperBounds[i] = boundSum;
    }

    uint32 nextCandDodId = maxDocId;
    for (size_t i = 0; i < cursors.size(); ++i) {
        if (cursors[i].curDocId < nextCandDodId) {
            nextCandDodId = cursors[i].curDocId;
        }
    }

    size_t firstNeIdx = cursors.size();
    while (firstNeIdx != 0 && upperBounds[firstNeIdx - 1] <= threshold) {
        --firstNeIdx;
        if (firstNeIdx == 0) {
            return;
        }
    }

    float currCandScore = 0.0f;
    uint32 currCandDocId = 0;

    while (currCandDocId < maxDocId) {
        bool foundCand = false;
        while (!foundCand) {
            // start find from next_vec_id
            if (nextCandDodId >= maxDocId) {
                CloseCursors(cursors);
                return;
            }
            // get current candidate vector
            currCandDocId = nextCandDodId;
            currCandScore = 0.0f;
            // update next_cand_vec_id
            nextCandDodId = maxDocId;

            for (size_t i = 0; i < firstNeIdx; ++i) {
                if (cursors[i].curDocId == currCandDocId) {
                    currCandScore += cursors[i].qTokenIDFVal *
                        scorer.GetDocBM25Score(cursors[i].tokenFreqInDoc, cursors[i].curDocLength);
                    cursors[i].Next();
                }
                if (cursors[i].curDocId < nextCandDodId) {
                    nextCandDodId = cursors[i].curDocId;
                }
            }

            foundCand = true;
            for (size_t i = firstNeIdx; i < cursors.size(); ++i) {
                if (currCandScore + upperBounds[i] <= threshold) {
                    foundCand = false;
                    break;
                }
                cursors[i].Seek(currCandDocId);
                if (cursors[i].curDocId == currCandDocId) {
                    currCandScore += cursors[i].qTokenIDFVal *
                        scorer.GetDocBM25Score(cursors[i].tokenFreqInDoc, cursors[i].curDocLength);
                }
            }
        }

        if (currCandScore > threshold) {
            heap.push(currCandDocId, currCandScore);
            threshold = heap.full() ? heap.top().val : 0;
            while (firstNeIdx != 0 && upperBounds[firstNeIdx - 1] <= threshold) {
                --firstNeIdx;
                if (firstNeIdx == 0) {
                    CloseCursors(cursors);
                    return;
                }
            }
        }
    }
    CloseCursors(cursors);
}

static FORCE_INLINE int CompareBM25ScanDataByDocId(const void *left, const void *right)
{
    BM25ScanData* leftRes = (BM25ScanData*)left;
    BM25ScanData* rightRes = (BM25ScanData*)right;
    return leftRes->docId - rightRes->docId;
}

static FORCE_INLINE int CompareBM25ScanDataByScore(const void *left, const void *right)
{
    BM25ScanData* leftRes = (BM25ScanData*)left;
    BM25ScanData* rightRes = (BM25ScanData*)right;
    return rightRes->score - leftRes->score > 0 ? 1 : -1;
}

static void DocIdsGetHeapCtids(Relation index, BM25EntryPages &entryPages, BM25ScanOpaque so)
{
    Buffer buf;
    Page page;
    uint32 curBlkno;
    uint32 curdDocId;
    qsort(so->candDocs, (size_t)so->candNums, sizeof(BM25ScanData), CompareBM25ScanDataByDocId);

    /* doc meta page */
    buf = ReadBuffer(index, entryPages.documentMetaPage);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    BM25DocMetaPage docMetaPage = BM25PageGetDocMeta(BufferGetPage(buf));
    curBlkno = docMetaPage->docBlknoTable;
    UnlockReleaseBuffer(buf);

    for (int i = 0; i < so->candNums; ++i) {
        curdDocId = so->candDocs[i].docId;
        if (curdDocId == BM25_INVALID_DOC_ID) {
            continue;
        }

        BlockNumber docBlkno = SeekBlocknoForDoc(index, curdDocId, curBlkno);
        uint16 offset = curdDocId % BM25_DOCUMENT_MAX_COUNT_IN_PAGE;
        Assert(BlockNumberIsValid(docBlkno));
        buf = ReadBuffer(index, docBlkno);
        LockBuffer(buf, BUFFER_LOCK_SHARE);
        page = BufferGetPage(buf);

        BM25DocumentItem *docItem = (BM25DocumentItem*)((char *)page + sizeof(PageHeaderData) +
            offset * BM25_DOCUMENT_ITEM_SIZE);
        if (!docItem->isActived) {
            UnlockReleaseBuffer(buf);
            elog(ERROR, "Read invalid doc.");
        }
        so->candDocs[i].heapCtid = docItem->ctid.t_tid;
        UnlockReleaseBuffer(buf);
    }
    qsort(so->candDocs, (size_t)so->candNums, sizeof(BM25ScanData), CompareBM25ScanDataByScore);
}

static void BM25IndexScan(Relation index, BM25QueryTokensInfo &queryTokenInfo, uint32 docNums,
    float avgdl, BM25ScanOpaque so)
{
    if (queryTokenInfo.size == 0) {
        return;
    }
    BM25Scorer scorer = BM25Scorer(u_sess->attr.attr_sql.bm25_k1, u_sess->attr.attr_sql.bm25_b, avgdl);

    size_t capacity = so->expectedCandNums == 0 ? docNums : so->expectedCandNums;
    MaxMinHeap heap;
    heap.InitHeap(capacity);
    if (so->expectedCandNums == 0) {
        SearchTaat(index, queryTokenInfo, heap, docNums, scorer, so->docIdMask);
    } else {
        SearchDaatMaxscore(index, queryTokenInfo, heap, docNums, scorer, so->docIdMask);
    }

    uint32 docId;
    int64 size = heap.size();
    so->candDocs = (BM25ScanData*)palloc0(sizeof(BM25ScanData) * size);
    for (int64 i = size - 1; i >= 0; --i) {
        docId = heap.top().id;
        so->candDocs[i].docId = docId;
        so->candDocs[i].score = heap.top().val;
        so->candNums++;
        so->docIdMask[docId >> 3] |= 1 << (docId % 8);
        heap.pop();
    }
}

static void ContructScanScoreKeys(Relation index, BM25ScanOpaque so)
{
    IndexScanDesc scan;
    Oid heapRelOid;
    Relation heapRel;
    HeapTuple heapTuple;
    Datum values[INDEX_MAX_KEYS];
    bool isnull[INDEX_MAX_KEYS];
    TupleTableSlot* slot = NULL;
    IndexInfo* indexInfo;

    scan = RelationGetIndexScan(index, 0, 0);
    heapRelOid = IndexGetRelation(RelationGetRelid(index), false);
    heapRel = heap_open(heapRelOid, AccessShareLock);
    scan->heapRelation = heapRel;
    scan->xs_snapshot = GetActiveSnapshot();
    scan->xs_heapfetch = tableam_scan_index_fetch_begin(heapRel);
    indexInfo = BuildIndexInfo(index);

    char* scoreKey = nullptr;

    MemoryContext oldCtx;

    u_sess->bm25_ctx.scoreHashTable = New(CurrentMemoryContext) BM25ScanDocScoreHashTable(so->candNums);
    for (int i = 0; i < so->candNums; ++i) {
        scan->xs_ctup.t_self = so->candDocs[i].heapCtid;
        heapTuple = (HeapTuple)IndexFetchTuple(scan);
        if (heapTuple == NULL) {
            continue;
        }
        slot = MakeSingleTupleTableSlot(RelationGetDescr(heapRel));
        (void)ExecStoreTuple(heapTuple, slot, InvalidBuffer, false);
        FormIndexDatum(indexInfo, slot, NULL, values, isnull);
        scoreKey = text_to_cstring(DatumGetVarCharPP(values[0]));
        if (scoreKey == NULL) {
            continue;
        }
        u_sess->bm25_ctx.scoreHashTable->AddScore(so->candDocs[i].score, scoreKey);
        ExecDropSingleTupleTableSlot(slot);
    }

    heap_close(heapRel, AccessShareLock);
    if (scan->xs_heapfetch) {
        tableam_scan_index_fetch_end(scan->xs_heapfetch);
    }
    if (BufferIsValid(scan->xs_cbuf)) {
        ReleaseBuffer(scan->xs_cbuf);
        scan->xs_cbuf = InvalidBuffer;
    }
    IndexScanEnd(scan);
}

IndexScanDesc bm25beginscan_internal(Relation index, int nkeys, int norderbys)
{
    IndexScanDesc scan;
    BM25ScanOpaque so;
    BM25MetaPageData bm25MetaData;

    scan = RelationGetIndexScan(index, nkeys, norderbys);
    BM25GetMetaPageInfo(index, &bm25MetaData);
    so = (BM25ScanOpaque)palloc(sizeof(BM25ScanOpaqueData));
    so->cursor = 0;
    so->candDocs = nullptr;
    so->candNums = 0;
    so->expectedCandNums = u_sess->attr.attr_sql.enable_bm25_taat ? 0 : u_sess->attr.attr_sql.bm25_topk;
    so->expandedTimes = 0;
    so->docIdMaskSize = bm25MetaData.documentCount / 8 + 1;
    so->docIdMask = (unsigned char*)palloc0(sizeof(unsigned char) * (so->docIdMaskSize));

    scan->opaque = so;
    return scan;
}

void bm25rescan_internal(IndexScanDesc scan, ScanKey keys, int nkeys, ScanKey orderbys, int norderbys)
{
    BM25ScanOpaque so = (BM25ScanOpaque)scan->opaque;
    so->cursor = 0;

    if (keys && scan->numberOfKeys > 0) {
        errno_t rc = memmove_s(scan->keyData, scan->numberOfKeys * sizeof(ScanKeyData),
            keys, scan->numberOfKeys * sizeof(ScanKeyData));
        securec_check(rc, "\0", "\0");
    }

    if (orderbys && scan->numberOfOrderBys > 0) {
        errno_t rc = memmove_s(scan->orderByData, scan->numberOfOrderBys * sizeof(ScanKeyData),
            orderbys, scan->numberOfOrderBys * sizeof(ScanKeyData));
        securec_check(rc, "\0", "\0");
    }
}

static bool CheckIfNeedExpandSearch(BM25ScanOpaque so)
{
    // new scan
    if (so->cursor == 0) {
        return true;
    }

    // taat scan
    if (so->expectedCandNums == 0) {
        return false;
    }

    // no more cands
    if (so->candNums < so->expectedCandNums) {
        return false;
    }

    if (so->cursor == so->candNums && so->expandedTimes < DEFAULT_EXPAND_TIME) {
        so->cursor = 0;
        so->expectedCandNums *= 2;
        so->candNums = 0;
        pfree_ext(so->candDocs);
        so->expandedTimes++;
        DELETE_EX(u_sess->bm25_ctx.scoreHashTable);
        return true;
    }

    if (so->cursor == so->candNums && so->expandedTimes >= DEFAULT_EXPAND_TIME) {
        so->cursor = 0;
        so->expectedCandNums = 0;
        so->candNums = 0;
        pfree_ext(so->candDocs);
        DELETE_EX(u_sess->bm25_ctx.scoreHashTable);
        return true;
    }

    return false;
}

bool bm25gettuple_internal(IndexScanDesc scan, ScanDirection dir)
{
    /*
     * Index can be used to scan backward, but Postgres doesn't support
     * backward scan on operators
     */
    Assert(ScanDirectionIsForward(dir));

    BM25MetaPageData meta;
    BM25GetMetaPageInfo(scan->indexRelation, &meta);
    BM25ScanOpaque so = (BM25ScanOpaque)scan->opaque;
    if (meta.documentCount == 0) {
        return false;
    }

    bool needSearch = CheckIfNeedExpandSearch(so);
    if (needSearch) {
        ArrayType *arr = NULL;
        if (scan->orderByData != NULL) {
            arr = DatumGetArrayTypeP(scan->orderByData[0].sk_argument);
        } else if (scan->keyData != NULL) {
            arr = DatumGetArrayTypeP(scan->keyData[0].sk_argument);
        }
        BM25QueryTokensInfo queryTokenInfo = GetQueryTokens(scan->indexRelation, TextDatumGetCString(
            PointerGetDatum(arr)));
        float avgdl = meta.tokenCount / meta.documentCount;
        BM25IndexScan(scan->indexRelation, queryTokenInfo, meta.documentCount, avgdl, so);
        DocIdsGetHeapCtids(scan->indexRelation, meta.entryPageList, so);
        ContructScanScoreKeys(scan->indexRelation, so);
        if (queryTokenInfo.queryTokens != nullptr) {
            pfree(queryTokenInfo.queryTokens);
            queryTokenInfo.queryTokens = nullptr;
        }
    }

    bool found = false;
    if (so->cursor < so->candNums) {
        scan->xs_ctup.t_self = so->candDocs[so->cursor].heapCtid;
        scan->xs_recheck = false;
        so->cursor++;
        found = true;
    }
    return found;
}

void bm25endscan_internal(IndexScanDesc scan)
{
    BM25ScanOpaque so = (BM25ScanOpaque)scan->opaque;
    pfree_ext(so->docIdMask);
    pfree_ext(so->candDocs);
    pfree_ext(so);
    if (u_sess->bm25_ctx.scoreHashTable != NULL) {
        DELETE_EX(u_sess->bm25_ctx.scoreHashTable);
    }
    scan->opaque = NULL;
}

Datum bm25_scores_textarr(PG_FUNCTION_ARGS)
{
    ereport(ERROR, (errmsg("Textarr not support for BM25 index currently.")));
}

Datum bm25_scores_text(PG_FUNCTION_ARGS)
{
    if (u_sess->bm25_ctx.scoreHashTable == NULL) {
        ereport(ERROR, (errmsg("No BM25 index is used to the scan, please check the plan.")));
    }
    char* leftKey = text_to_cstring(DatumGetVarCharPP(PG_GETARG_DATUM(0)));
    char* rightKey = text_to_cstring(DatumGetVarCharPP(PG_GETARG_DATUM(1)));
    bool findLeft = false;
    bool findRight = false;
    float leftScore = u_sess->bm25_ctx.scoreHashTable->SearchScoreForDoc(leftKey, &findLeft);
    float rightScore = u_sess->bm25_ctx.scoreHashTable->SearchScoreForDoc(rightKey, &findRight);
    if (!findLeft && !findRight) {
        pfree_ext(leftKey);
        pfree_ext(rightKey);
        DELETE_EX(u_sess->bm25_ctx.scoreHashTable);
        ereport(ERROR, (errmsg("No result not found in bm25scan hash table.")));
    }
    float score = leftScore > rightScore ? leftScore : rightScore;
    pfree_ext(leftKey);
    pfree_ext(rightKey);
    PG_RETURN_FLOAT8(score);
}