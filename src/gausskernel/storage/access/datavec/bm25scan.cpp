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
#include "access/datavec/varblock.h"
#include "storage/buf/bufmgr.h"
#include "storage/buf/bufpage.h"

const uint32 DEFAULT_EXPAND_TIME = 8;
const float BM25_DEFAULT_OFFSET = 0.5f;

/* docId mask bitmap: one bit per document, packed byte-wise */
#define BM25_DOCID_MASK_BITS_PER_BYTE 8u
typedef struct GlobalDfEntry {
    char token[BM25_MAX_TOKEN_LEN];
    uint32 df;
} GlobalDfEntry;

typedef struct BM25QueryToken {
    BlockNumber tokenPostingBlock;
    float qTokenMaxScore;
    float qTokenIDFVal;
    ItemPointerData postingChainHead;
    bool useVarBlock;
} BM25QueryToken;

typedef struct BM25QueryTokensInfo {
    BM25QueryToken *queryTokens;
    uint32 size;
} BM25QueryTokensInfo;
static HTAB* ParseGlobalDfMap(const char* dfStr)
{
    HASHCTL hashCtl;
    errno_t rc = memset_s(&hashCtl, sizeof(hashCtl), 0, sizeof(hashCtl));
    securec_check(rc, "\0", "\0");
    hashCtl.keysize = BM25_MAX_TOKEN_LEN;
    hashCtl.entrysize = sizeof(GlobalDfEntry);
    hashCtl.hcxt = CurrentMemoryContext;
    HTAB* map = hash_create("BM25 GlobalDf", 32, &hashCtl,
                            HASH_ELEM | HASH_CONTEXT);

    char* str = pstrdup(dfStr);
    char* saveptr = NULL;
    char* pair = strtok_r(str, ",", &saveptr);
    while (pair != NULL) {
        char* colon = strrchr(pair, ':');
        if (colon != NULL && colon != pair) {
            *colon = '\0';
            char* term = pair;
            uint32 df = (uint32)atoi(colon + 1);
            if (df > 0) {
                bool found = false;
                GlobalDfEntry* entry = (GlobalDfEntry*)hash_search(
                    map, term, HASH_ENTER, &found);
                if (entry != NULL) {
                    rc = strncpy_s(entry->token, BM25_MAX_TOKEN_LEN,
                                   term, BM25_MAX_TOKEN_LEN - 1);
                    securec_check(rc, "\0", "\0");
                    entry->df = df;
                }
            }
        }
        pair = strtok_r(NULL, ",", &saveptr);
    }
    pfree(str);
    return map;
}

static uint32 LookupGlobalDf(HTAB* dfMap, const char* token)
{
    if (dfMap == NULL || token == NULL) return 0;
    char key[BM25_MAX_TOKEN_LEN];
    errno_t rc = memset_s(key, BM25_MAX_TOKEN_LEN, 0, BM25_MAX_TOKEN_LEN);
    securec_check(rc, "\0", "\0");
    rc = strncpy_s(key, BM25_MAX_TOKEN_LEN, token, BM25_MAX_TOKEN_LEN - 1);
    securec_check(rc, "\0", "\0");
    bool found = false;
    GlobalDfEntry* entry = (GlobalDfEntry*)hash_search(
        dfMap, key, HASH_FIND, &found);
    return (found && entry != NULL) ? entry->df : 0;
}

static void DestroyGlobalDfMap(HTAB* dfMap)
{
    if (dfMap != NULL) {
        hash_destroy(dfMap);
    }
}
static void FindBucketsLocation(Page page, BM25TokenizedDocData &tokenizedQuery, BlockNumber *bucketsLocation,
    uint32 maxHashBucketCount, uint32 &bucketFoundCount)
{
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
            uint32 N = meta.documentCount;
            uint32 df = tokenMeta->docCount;
            if (u_sess->attr.attr_sql.enable_bm25_global_idf &&
                u_sess->attr.attr_sql.bm25_global_doc_count > 0 && u_sess->bm25_ctx.globalDfMap != NULL) {
                uint32 gdf = LookupGlobalDf(u_sess->bm25_ctx.globalDfMap, tokenMeta->token);
                if (gdf > 0) {
                    N = (uint32)u_sess->attr.attr_sql.bm25_global_doc_count;
                    df = gdf;
                }
            }
            queryTokens[tokenIdx].qTokenIDFVal = tokenizedQuery.tokenDatas[tokenIdx].tokenFreq * 
                        std::log((1 + ((float)N - (float)df + BM25_DEFAULT_OFFSET) / ((float)df + BM25_DEFAULT_OFFSET)));
            if (meta.version >= BM25_VERSION_VARBLOCK_POSTING && ItemPointerIsValid(&tokenMeta->postingChainHead)) {
                queryTokens[tokenIdx].postingChainHead = tokenMeta->postingChainHead;
                queryTokens[tokenIdx].useVarBlock = true;
                queryTokens[tokenIdx].tokenPostingBlock = InvalidBlockNumber;
            } else {
                queryTokens[tokenIdx].tokenPostingBlock = tokenMeta->postingBlkno;
                queryTokens[tokenIdx].useVarBlock = false;
                ItemPointerSetInvalid(&queryTokens[tokenIdx].postingChainHead);
            }
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
    BM25TokenizedDocData tokenizedQuery = BM25DocumentTokenize(sentence, Bm25GetDictPath(index), cutForSearch);
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
        ItemPointerSetInvalid(&queryTokens[tokenIdx].postingChainHead);
        queryTokens[tokenIdx].useVarBlock = false;
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
        bool hasPosting = BlockNumberIsValid(queryTokens[tokenIdx].tokenPostingBlock) ||
            (queryTokens[tokenIdx].useVarBlock && ItemPointerIsValid(&queryTokens[tokenIdx].postingChainHead));
        if (!hasPosting) {
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
    /* Only tokens with real postings are filled into resQueryTokens. */
    tokensInfo.size = tokenFillIdx;
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
    BM25ScanDocScoreHashTable(uint32 maxDocCount, const char* query)
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
        queryString = pg_strdup(query);
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

    bool CheckQuery(const char *inputQuery)
    {
        if (inputQuery != nullptr && strcmp(inputQuery, queryString) == 0) {
            return true;
        }
        return false;
    }

    void Destroy()
    {
        pfree_ext(scoreArray);
    }

private:
    BM25ScanScoreHashEntry* scoreArray;
    char* queryString;
    size_t scoreHashCapacity;
};

static inline bool BM25IsDocFiltered(const unsigned char *docIdfilter, uint32 docId)
{
    return docIdfilter &&
        ((docIdfilter[docId / BM25_DOCID_MASK_BITS_PER_BYTE] >> (docId % BM25_DOCID_MASK_BITS_PER_BYTE)) & 1u);
}

static bool BM25NextFromVarBlock(BM25ScanCursor *cursor)
{
    while (ItemPointerIsValid(&cursor->curChunkCtid)) {
        if (!BufferIsValid(cursor->buf)) {
            cursor->buf = ReadBufferExtended(cursor->index, MAIN_FORKNUM,
                ItemPointerGetBlockNumber(&cursor->curChunkCtid), RBM_NORMAL, NULL);
            LockBuffer(cursor->buf, BUFFER_LOCK_SHARE);
            cursor->page = BufferGetPage(cursor->buf);
        }
        ItemId id = PageGetItemId(cursor->page, ItemPointerGetOffsetNumber(&cursor->curChunkCtid));
        VarBlockChunkHeader *hdr = (VarBlockChunkHeader *)PageGetItem(cursor->page, id);
        char *payload = (char *)hdr + sizeof(VarBlockChunkHeader);
        uint32 len = hdr->payload_len;
        while (cursor->curPayloadOffset + BM25_POSTING_ITEM_ALIGNED_SIZE <= len) {
            BM25TokenPostingItem *item = (BM25TokenPostingItem *)(payload + cursor->curPayloadOffset);
            uint32 docId = item->docId;
            cursor->curPayloadOffset += BM25_POSTING_ITEM_ALIGNED_SIZE;
            if (BM25IsDocFiltered(cursor->docIdfilter, docId)) {
                continue;
            }
            cursor->curDocId = item->docId;
            cursor->tokenFreqInDoc = (float)item->freq;
            cursor->curDocLength = (float)item->docLength;
            return true;
        }
        cursor->curChunkCtid = hdr->next_ctid;
        cursor->curPayloadOffset = 0;
        UnlockReleaseBuffer(cursor->buf);
        cursor->buf = InvalidBuffer;
        cursor->page = NULL;
    }
    return false;
}

static bool BM25SeekInVarBlock(BM25ScanCursor *cursor, uint32 docId)
{
    /*
     * VarBlock postings are stored in a sorted chain.
     * In DAAT(MaxScore), Seek requests usually move forward (non-decreasing docId).
     * Avoid resetting to head on every Seek; only reset on backward seek.
     */
    if (cursor->curDocId == BM25_INVALID_DOC_ID) {
        return false;
    }

    if (docId < cursor->curDocId) {
        cursor->curChunkCtid = cursor->postingChainHead;
        cursor->curPayloadOffset = 0;
        if (BufferIsValid(cursor->buf)) {
            UnlockReleaseBuffer(cursor->buf);
            cursor->buf = InvalidBuffer;
            cursor->page = NULL;
        }
    }

    while (ItemPointerIsValid(&cursor->curChunkCtid)) {
        if (!BufferIsValid(cursor->buf)) {
            cursor->buf = ReadBufferExtended(cursor->index, MAIN_FORKNUM,
                ItemPointerGetBlockNumber(&cursor->curChunkCtid), RBM_NORMAL, NULL);
            LockBuffer(cursor->buf, BUFFER_LOCK_SHARE);
            cursor->page = BufferGetPage(cursor->buf);
        }
        ItemId id = PageGetItemId(cursor->page, ItemPointerGetOffsetNumber(&cursor->curChunkCtid));
        VarBlockChunkHeader *hdr = (VarBlockChunkHeader *)PageGetItem(cursor->page, id);
        char *payload = (char *)hdr + sizeof(VarBlockChunkHeader);
        uint32 len = hdr->payload_len;
        for (; cursor->curPayloadOffset + BM25_POSTING_ITEM_ALIGNED_SIZE <= len;
            cursor->curPayloadOffset += BM25_POSTING_ITEM_ALIGNED_SIZE) {
            BM25TokenPostingItem *item = (BM25TokenPostingItem *)(payload + cursor->curPayloadOffset);
            if (item->docId < docId || BM25IsDocFiltered(cursor->docIdfilter, item->docId)) {
                continue;
            }
            cursor->curDocId = item->docId;
            cursor->tokenFreqInDoc = (float)item->freq;
            cursor->curDocLength = (float)item->docLength;
            cursor->curPayloadOffset += BM25_POSTING_ITEM_ALIGNED_SIZE;
            return true;
        }
        cursor->curChunkCtid = hdr->next_ctid;
        cursor->curPayloadOffset = 0;
        UnlockReleaseBuffer(cursor->buf);
        cursor->buf = InvalidBuffer;
        cursor->page = NULL;
    }
    return false;
}

static bool BM25SeekInPostingPages(BM25ScanCursor *cursor, uint32 docId)
{
    while (BlockNumberIsValid(cursor->curBlkno)) {
        OffsetNumber maxoffno = PageGetMaxOffsetNumber(cursor->page);
        for (OffsetNumber offno = cursor->curOffset; offno <= maxoffno; offno = OffsetNumberNext(offno)) {
            BM25TokenPostingPage postingItem =
                (BM25TokenPostingPage)PageGetItem(cursor->page, PageGetItemId(cursor->page, offno));
            uint32 hitDocId = postingItem->docId;
            if (hitDocId < docId || BM25IsDocFiltered(cursor->docIdfilter, hitDocId)) {
                continue;
            }
            cursor->curDocId = hitDocId;
            cursor->tokenFreqInDoc = postingItem->freq;
            cursor->curDocLength = postingItem->docLength;
            cursor->curOffset = offno;
            return true;
        }
        cursor->curBlkno = BM25PageGetOpaque(cursor->page)->nextblkno;
        UnlockReleaseBuffer(cursor->buf);
        cursor->buf = InvalidBuffer;
        if (BlockNumberIsValid(cursor->curBlkno)) {
            cursor->buf = ReadBuffer(cursor->index, cursor->curBlkno);
            LockBuffer(cursor->buf, BUFFER_LOCK_SHARE);
            cursor->page = BufferGetPage(cursor->buf);
            cursor->curOffset = FirstOffsetNumber;
        }
    }
    return false;
}

static void BM25NextFromPostingPages(BM25ScanCursor *cursor, bool isInit)
{
    BM25TokenPostingPage postingItem;
    bool found = false;

    if (!BlockNumberIsValid(cursor->curBlkno)) {
        cursor->curDocId = BM25_INVALID_DOC_ID;
        return;
    }

    if (isInit) {
        cursor->buf = ReadBuffer(cursor->index, cursor->curBlkno);
        LockBuffer(cursor->buf, BUFFER_LOCK_SHARE);
        cursor->page = BufferGetPage(cursor->buf);
    }

    while (BlockNumberIsValid(cursor->curBlkno)) {
        OffsetNumber maxoffno = PageGetMaxOffsetNumber(cursor->page);
        OffsetNumber nextoffno = OffsetNumberIsValid(cursor->curOffset) ?
            OffsetNumberNext(cursor->curOffset) : FirstOffsetNumber;
        while (OffsetNumberIsValid(nextoffno) && nextoffno <= maxoffno) {
            postingItem = (BM25TokenPostingPage)PageGetItem(cursor->page, PageGetItemId(cursor->page, nextoffno));
            uint32 docId = postingItem->docId;
            if (BM25IsDocFiltered(cursor->docIdfilter, docId)) {
                nextoffno = OffsetNumberNext(nextoffno);
                continue;
            }
            cursor->curDocId = postingItem->docId;
            cursor->tokenFreqInDoc = postingItem->freq;
            cursor->curDocLength = postingItem->docLength;
            cursor->curOffset = nextoffno;
            found = true;
            break;
        }
        if (found) {
            break;
        }
        cursor->curBlkno = BM25PageGetOpaque(cursor->page)->nextblkno;
        cursor->curOffset = InvalidOffsetNumber;
        UnlockReleaseBuffer(cursor->buf);
        cursor->buf = InvalidBuffer;
        if (BlockNumberIsValid(cursor->curBlkno)) {
            cursor->buf = ReadBuffer(cursor->index, cursor->curBlkno);
            LockBuffer(cursor->buf, BUFFER_LOCK_SHARE);
            cursor->page = BufferGetPage(cursor->buf);
        }
    }

    if (!BlockNumberIsValid(cursor->curBlkno)) {
        cursor->curDocId = BM25_INVALID_DOC_ID;
    }
}

void BM25ScanCursor::Next(bool isInit)
{
    if (useVarBlock) {
        if (!BM25NextFromVarBlock(this)) {
            curDocId = BM25_INVALID_DOC_ID;
        }
        return;
    }

    BM25NextFromPostingPages(this, isInit);
}

void BM25ScanCursor::Seek(uint32 docId)
{
    if (curDocId != BM25_INVALID_DOC_ID && docId <= curDocId) {
        return;
    }

    Assert(docId != BM25_INVALID_DOC_ID);

    if (useVarBlock) {
        if (!BM25SeekInVarBlock(this, docId)) {
            curDocId = BM25_INVALID_DOC_ID;
        }
        return;
    }

    if (!BM25SeekInPostingPages(this, docId)) {
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
    float maxScoreRatio = u_sess->attr.attr_sql.max_score_ratio;
    for (uint32 i = 0; i < querySize; ++i) {
        if (queryTokens[i].useVarBlock && ItemPointerIsValid(&queryTokens[i].postingChainHead)) {
            cursors.push_back(BM25ScanCursor(index, &queryTokens[i].postingChainHead,
                queryTokens[i].qTokenMaxScore * queryTokens[i].qTokenIDFVal * maxScoreRatio,
                queryTokens[i].qTokenIDFVal, docIdMask));
        } else {
            cursors.push_back(BM25ScanCursor(index, queryTokens[i].tokenPostingBlock,
                queryTokens[i].qTokenMaxScore * queryTokens[i].qTokenIDFVal * maxScoreRatio,
                queryTokens[i].qTokenIDFVal, docIdMask));
        }
    }
    return cursors;
}

static void CloseCursors(Vector<BM25ScanCursor> &cursors)
{
    for (uint32 i = 0; i < cursors.size(); ++i) {
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
            CloseCursors(cursors);
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
    uint32 a = leftRes->docId;
    uint32 b = rightRes->docId;
    return (a < b) ? -1 : (a > b) ? 1 : 0;
}

static FORCE_INLINE int CompareBM25ScanDataByScore(const void *left, const void *right)
{
    BM25ScanData* leftRes = (BM25ScanData*)left;
    BM25ScanData* rightRes = (BM25ScanData*)right;
    return rightRes->score - leftRes->score > 0 ? 1 : -1;
}

static void DocIdsGetHeapCtids(Relation index, BM25EntryPages &entryPages, BM25ScanOpaque so, uint32 indexVersion)
{
    Buffer buf;
    Page page;
    uint32 curBlkno;
    uint32 curdDocId;
    const Size docAreaOff = BM25PageDocumentAreaOffset(indexVersion);
    qsort(so->candDocs, (size_t)so->candNums, sizeof(BM25ScanData), CompareBM25ScanDataByDocId);

    /* doc meta page */
    buf = ReadBuffer(index, entryPages.documentMetaPage);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    BM25DocMetaPage docMetaPage = BM25PageGetDocMeta(BufferGetPage(buf));
    curBlkno = docMetaPage->docBlknoTable;
    UnlockReleaseBuffer(buf);

    for (uint32 i = 0; i < so->candNums; ++i) {
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

        BM25DocumentItem *docItem =
            (BM25DocumentItem*)((char *)page + docAreaOff + offset * BM25_DOCUMENT_ITEM_SIZE);
        if (!docItem->isActived) {
            so->candDocs[i].docId = BM25_INVALID_DOC_ID;
            UnlockReleaseBuffer(buf);
            continue;
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
        so->docIdMask[docId / BM25_DOCID_MASK_BITS_PER_BYTE] |= 1u << (docId % BM25_DOCID_MASK_BITS_PER_BYTE);
        heap.pop();
    }
}

static void ConstructScanScoreKeys(Relation index, BM25ScanOpaque so, const char* queryString)
{
    IndexScanDesc scan;
    Oid heapRelOid;
    Relation heapRel;
    HeapTuple heapTuple;
    char* scoreKey = nullptr;
    Datum values[INDEX_MAX_KEYS];
    bool isnull[INDEX_MAX_KEYS];
    TupleTableSlot* slot = NULL;
    EState* estate = NULL;
    ExprContext* econtext = NULL;
    List* predicate = NIL;
    IndexInfo* indexInfo;

    scan = RelationGetIndexScan(index, 0, 0);
    heapRelOid = IndexGetRelation(RelationGetRelid(index), false);
    heapRel = heap_open(heapRelOid, AccessShareLock);
    scan->heapRelation = heapRel;
    scan->xs_snapshot = GetActiveSnapshot();
    scan->xs_heapfetch = tableam_scan_index_fetch_begin(heapRel);
    u_sess->bm25_ctx.scoreHashTable = New(CurrentMemoryContext) BM25ScanDocScoreHashTable(so->candNums, queryString);
    for (uint32 i = 0; i < so->candNums; ++i) {
        if (so->candDocs[i].docId == BM25_INVALID_DOC_ID) {
            continue;
        }

        scan->xs_ctup.t_self = so->candDocs[i].heapCtid;
        heapTuple = (HeapTuple)IndexFetchTuple(scan);
        if (heapTuple == NULL) {
            continue;
        }

        estate = CreateExecutorState();
        econtext = GetPerTupleExprContext(estate);
        slot = MakeSingleTupleTableSlot(RelationGetDescr(heapRel));
        econtext->ecxt_scantuple = slot;
        indexInfo = BuildIndexInfo(index);

        if (estate->es_is_flt_frame) {
            predicate = (List*)ExecPrepareQualByFlatten(indexInfo->ii_Predicate, estate);
        } else {
            predicate = (List*)ExecPrepareExpr((Expr *)indexInfo->ii_Predicate, estate);
        }

        (void)ExecStoreTuple(heapTuple, slot, InvalidBuffer, false);

        if (predicate != NIL) {
            if (!ExecQual(predicate, econtext)) {
                ExecDropSingleTupleTableSlot(slot);
                FreeExecutorState(estate);
                pfree(indexInfo);
                continue;
            }
        }

        FormIndexDatum(indexInfo, slot, estate, values, isnull);
        scoreKey = text_to_cstring(DatumGetVarCharPP(values[0]));
        if (scoreKey != NULL) {
            u_sess->bm25_ctx.scoreHashTable->AddScore(so->candDocs[i].score, scoreKey);
        }
        ExecDropSingleTupleTableSlot(slot);
        FreeExecutorState(estate);
        pfree(indexInfo);
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
    if (bm25MetaData.lastBacthInsertFailed) {
        elog(ERROR, "Last batch insert document failed, scanned score maybe affected, "
            "please reindex or recreate bm25 index [%s].", index->rd_rel->relname);
    }

    so = (BM25ScanOpaque)palloc(sizeof(BM25ScanOpaqueData));
    so->cursor = 0;
    so->candDocs = nullptr;
    so->candNums = 0;
    so->expectedCandNums = u_sess->attr.attr_sql.enable_bm25_taat ? 0 : u_sess->attr.attr_sql.bm25_topk;
    so->expandedTimes = 0;
    so->docIdMaskSize = bm25MetaData.nextDocId / 8 + 1;
    so->docIdMask = (unsigned char*)palloc0(sizeof(unsigned char) * (so->docIdMaskSize));

    scan->opaque = so;
    u_sess->bm25_ctx.globalDfMap = NULL;
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
        if (scan->orderByData != NULL && !(scan->orderByData[0].sk_flags & SK_ISNULL)) {
            arr = DatumGetArrayTypeP(scan->orderByData[0].sk_argument);
        } else if (scan->keyData != NULL && !(scan->keyData[0].sk_flags & SK_ISNULL)) {
            arr = DatumGetArrayTypeP(scan->keyData[0].sk_argument);
        }
        if (arr == NULL) {
            ereport(ERROR, (errmsg("Query is null, can not find any document.")));
        }
        char* queryString = TextDatumGetCString(PointerGetDatum(arr));

        if (u_sess->attr.attr_sql.enable_bm25_global_idf &&
            u_sess->attr.attr_sql.bm25_global_doc_count > 0 && u_sess->attr.attr_sql.bm25_global_df != NULL &&
            u_sess->attr.attr_sql.bm25_global_df[0] != '\0' && u_sess->bm25_ctx.globalDfMap == NULL) {
            u_sess->bm25_ctx.globalDfMap = ParseGlobalDfMap(u_sess->attr.attr_sql.bm25_global_df);
        }
        BM25QueryTokensInfo queryTokenInfo = GetQueryTokens(scan->indexRelation, queryString);
        if (queryTokenInfo.size == 0) {
            return false;
        }

        float avgdl = (meta.tokenCount * 1.0) / meta.documentCount;
        BM25IndexScan(scan->indexRelation, queryTokenInfo, meta.nextDocId, avgdl, so);
        DocIdsGetHeapCtids(scan->indexRelation, meta.entryPageList, so, meta.version);
        ConstructScanScoreKeys(scan->indexRelation, so, queryString);
        if (queryTokenInfo.queryTokens != nullptr) {
            pfree(queryTokenInfo.queryTokens);
            queryTokenInfo.queryTokens = nullptr;
        }
    }

    bool found = false;
    while (so->cursor < so->candNums && so->candDocs[so->cursor].docId == BM25_INVALID_DOC_ID) {
        so->cursor++;
    }
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
    if (u_sess->bm25_ctx.globalDfMap != NULL) {
        DestroyGlobalDfMap(u_sess->bm25_ctx.globalDfMap);
        u_sess->bm25_ctx.globalDfMap = NULL;
    }
    scan->opaque = NULL;
}

static bool ExpressionContainVar(Node* node, void* context)
{
    if (node == NULL) {
        return false;
    } else if (IsA(node, Var)) {
        return true;
    }

    return expression_tree_walker(node, (bool (*)())ExpressionContainVar, context);
}

static bool DocIsInLeftKey(List* args)
{
    Node* node = (Node*)linitial(args);
    return ExpressionContainVar(node, NULL);
}

static void GetQueryAndDoc(PG_FUNCTION_ARGS, char* &query, char* &doc)
{
    bool* fnExtra = nullptr;
    bool docInLeft = false;
    List* args = NULL;
    Node* expr = NULL;

    if (fcinfo->flinfo->fn_extra) {
        docInLeft = *(bool*)fcinfo->flinfo->fn_extra;
    } else {
        expr = (Node*)fcinfo->flinfo->fn_expr;
        if (expr && IsA(expr, OpExpr)) {
            args = ((OpExpr*)expr)->args;
        } else if (expr && IsA(expr, FuncExpr)) {
            args = ((FuncExpr*)expr)->args;
        }

        if (args == NULL) {
            ereport(ERROR, (errmsg(
                "Unexpected Node type, \"%s\".", expr ? nodeTagToString(nodeTag(expr)) : "UnknownTag")));
        }

        if (DocIsInLeftKey(args)) {
            docInLeft = true;
        }
        MemoryContext oldcontext = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);
        fnExtra = (bool*)palloc0(sizeof(bool));
        *fnExtra = docInLeft;
        fcinfo->flinfo->fn_extra = fnExtra;
        MemoryContextSwitchTo(oldcontext);
    }

    if (docInLeft) {
        doc = text_to_cstring(DatumGetVarCharPP(PG_GETARG_DATUM(0)));
        query = text_to_cstring(DatumGetVarCharPP(PG_GETARG_DATUM(1)));
    } else {
        query = text_to_cstring(DatumGetVarCharPP(PG_GETARG_DATUM(0)));
        doc = text_to_cstring(DatumGetVarCharPP(PG_GETARG_DATUM(1)));
    }
}

Datum bm25_scores_textarr(PG_FUNCTION_ARGS)
{
    ereport(ERROR, (errmsg("Textarr not support for BM25 index currently.")));
    PG_RETURN_NULL();
}

Datum bm25_scores_text(PG_FUNCTION_ARGS)
{
    if (u_sess->bm25_ctx.scoreHashTable == NULL) {
        ereport(ERROR, (errmsg("No BM25 index is used to the scan, please check the plan.")));
    }

    bool findDoc = false;
    char* doc = nullptr;
    char* query = nullptr;

    GetQueryAndDoc(fcinfo, query, doc);
    if (!u_sess->bm25_ctx.scoreHashTable->CheckQuery(query)) {
        pfree_ext(query);
        pfree_ext(doc);
        DELETE_EX(u_sess->bm25_ctx.scoreHashTable);
        ereport(ERROR, (errmsg("Incorrect query string, please check the statement.")));
    }

    float score = u_sess->bm25_ctx.scoreHashTable->SearchScoreForDoc(doc, &findDoc);

    if (!findDoc) {
        pfree_ext(query);
        pfree_ext(doc);
        DELETE_EX(u_sess->bm25_ctx.scoreHashTable);
        ereport(ERROR, (errmsg("No result not found in bm25scan hash table.")));
    }
    pfree_ext(query);
    pfree_ext(doc);
    PG_RETURN_FLOAT8(score);
}