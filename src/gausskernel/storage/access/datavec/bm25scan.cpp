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
#include "utils/hsearch.h"
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
#include "nodes/plannodes.h"
#include "nodes/primnodes.h"
#include "executor/exec/execdesc.h"
#include "executor/spi.h"
#include "utils/guc.h"

const uint32 DEFAULT_EXPAND_TIME = 8;
const float BM25_DEFAULT_OFFSET = 0.5f;

/* Skip the global-avgdl rescore when local and global avgdl differ by less than this ratio. */
const float BM25_GLOBAL_AVGDL_SKIP_RATIO = 0.01f;

/* docId mask bitmap: one bit per document, packed byte-wise */
#define BM25_DOCID_MASK_BITS_PER_BYTE 8u

/* Distributed global IDF: parsed scan-local view of bm25_global_stat. */
typedef struct GlobalDfEntry {
    char term[BM25_MAX_TOKEN_LEN];
    uint32 df;
} GlobalDfEntry;

/*
 * The bm25_global_stat wire format uses ';', ',' and ':' as separators. Terms containing
 * a separator cannot be represented without escaping, so omit them from the statistics
 * payload and let scans fall back to their local df.
 */
static inline bool Bm25TermIsDfEncodable(const char *term)
{
    if (term == NULL || term[0] == '\0' || strlen(term) >= BM25_MAX_TOKEN_LEN) {
        return false;
    }
    for (const unsigned char *cursor = (const unsigned char *)term; *cursor != '\0'; ++cursor) {
        if (*cursor == ';' || *cursor == ',' || *cursor == ':' ||
            *cursor == ' ' || (*cursor >= '\t' && *cursor <= '\r')) {
            return false;
        }
    }
    return true;
}

static void DestroyGlobalDfMap(HTAB *globalDfMap)
{
    if (globalDfMap != NULL) {
        hash_destroy(globalDfMap);
    }
}

static bool ParseBm25GlobalUint64(const char *value, uint64 *parsed)
{
    if (value == NULL || value[0] == '\0') {
        return false;
    }
    for (const char *cursor = value; *cursor != '\0'; ++cursor) {
        if (*cursor < '0' || *cursor > '9') {
            return false;
        }
    }
    errno = 0;
    char *end = NULL;
    unsigned long long number = strtoull(value, &end, 10);
    if (errno == ERANGE || end == value || *end != '\0') {
        return false;
    }
    *parsed = (uint64)number;
    return true;
}

static HTAB *BuildGlobalDfMap(MemoryContext scanMcxt, uint64 *globalDocumentCount,
    uint64 *globalTokenCount)
{
    const char *raw = u_sess->attr.attr_sql.bm25_global_stat;
    if (raw == NULL || raw[0] == '\0') {
        return NULL;
    }

    *globalDocumentCount = 0;
    *globalTokenCount = 0;
    char *buf = pstrdup(raw);
    char *firstSemi = strchr(buf, ';');
    char *secondSemi = firstSemi == NULL ? NULL : strchr(firstSemi + 1, ';');
    if (firstSemi == NULL || secondSemi == NULL || firstSemi == buf ||
        secondSemi == firstSemi + 1 || secondSemi[1] == '\0' ||
        strchr(secondSemi + 1, ';') != NULL || secondSemi[1] == ',' ||
        strstr(secondSemi + 1, ",,") != NULL || raw[strlen(raw) - 1] == ',') {
        pfree(buf);
        return NULL;
    }
    *firstSemi = '\0';
    *secondSemi = '\0';
    uint64 documentCount = 0;
    uint64 tokenCount = 0;
    if (strncmp(buf, "N=", 2) != 0 || strncmp(firstSemi + 1, "T=", 2) != 0 ||
        !ParseBm25GlobalUint64(buf + 2, &documentCount) ||
        !ParseBm25GlobalUint64(firstSemi + 3, &tokenCount) ||
        documentCount == 0 || documentCount > UINT_MAX || tokenCount < documentCount) {
        pfree(buf);
        return NULL;
    }

    HASHCTL ctl;
    errno_t rc = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
    securec_check(rc, "\0", "\0");
    ctl.keysize = BM25_MAX_TOKEN_LEN;
    ctl.entrysize = sizeof(GlobalDfEntry);
    ctl.hcxt = scanMcxt;
    HTAB *globalDfMap = hash_create("BM25 Global DF", 32, &ctl, HASH_ELEM | HASH_CONTEXT);

    char *saveptr = NULL;
    char *pair = strtok_r(secondSemi + 1, ",", &saveptr);
    while (pair != NULL) {
        char *colon = strchr(pair, ':');
        if (colon == NULL || colon == pair || strchr(colon + 1, ':') != NULL) {
            DestroyGlobalDfMap(globalDfMap);
            pfree(buf);
            return NULL;
        }
        *colon = '\0';
        uint64 df = 0;
        if (!Bm25TermIsDfEncodable(pair) || !ParseBm25GlobalUint64(colon + 1, &df) ||
            df == 0 || df > documentCount || df > UINT_MAX) {
            DestroyGlobalDfMap(globalDfMap);
            pfree(buf);
            return NULL;
        }
        char key[BM25_MAX_TOKEN_LEN] = {0};
        rc = strncpy_s(key, BM25_MAX_TOKEN_LEN, pair, BM25_MAX_TOKEN_LEN - 1);
        securec_check(rc, "\0", "\0");
        bool found = false;
        GlobalDfEntry *entry = (GlobalDfEntry *)hash_search(globalDfMap, key, HASH_ENTER, &found);
        if (entry == NULL || found) {
            DestroyGlobalDfMap(globalDfMap);
            pfree(buf);
            return NULL;
        }
        entry->df = (uint32)df;
        pair = strtok_r(NULL, ",", &saveptr);
    }
    *globalDocumentCount = documentCount;
    *globalTokenCount = tokenCount;
    pfree(buf);
    return globalDfMap;
}

static uint32 LookupGlobalDf(HTAB *globalDfMap, const char *token)
{
    if (globalDfMap == NULL) {
        return 0;
    }
    char key[BM25_MAX_TOKEN_LEN] = {0};
    errno_t rc = strncpy_s(key, BM25_MAX_TOKEN_LEN, token, BM25_MAX_TOKEN_LEN - 1);
    securec_check(rc, "\0", "\0");
    bool found = false;
    GlobalDfEntry *entry = (GlobalDfEntry *)hash_search(globalDfMap, key, HASH_FIND, &found);
    return found ? entry->df : 0;
}

static inline bool Bm25GlobalStatsEnabled(BM25ScanOpaque so)
{
    return u_sess->attr.attr_sql.enable_bm25_global_idf &&
        so->globalDfMap != NULL && so->globalDocumentCount > 0;
}

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
    BM25QueryToken *queryTokens, size_t tokenIdx, uint32 &tokenFoundCount, BM25ScanOpaque so)
{
    OffsetNumber maxoffno = PageGetMaxOffsetNumber(page);
    for (OffsetNumber offnoTokenMeta = FirstOffsetNumber; offnoTokenMeta <= maxoffno; offnoTokenMeta++) {
        BM25TokenMetaPage tokenMeta = (BM25TokenMetaPage)PageGetItem(page, PageGetItemId(page, offnoTokenMeta));
        if ((tokenMeta->hashValue == tokenizedQuery.tokenDatas[tokenIdx].hashValue) &&
            (strncmp(tokenMeta->token, tokenizedQuery.tokenDatas[tokenIdx].tokenValue, BM25_MAX_TOKEN_LEN - 1) == 0)) {
            /*
             * Distributed global IDF: use global N/df for this term when its global df is provided
             * (self-consistent); otherwise fall back to local N/df to avoid mixing scales.
             */
            float docN = (float)meta.documentCount;
            float docFreq = (float)tokenMeta->docCount;
            if (Bm25GlobalStatsEnabled(so)) {
                uint32 gdf = LookupGlobalDf(so->globalDfMap, tokenMeta->token);
                if (gdf >= 1 && gdf <= so->globalDocumentCount) {
                    docN = (float)so->globalDocumentCount;
                    docFreq = (float)gdf;
                }
            }
            queryTokens[tokenIdx].qTokenIDFVal = tokenizedQuery.tokenDatas[tokenIdx].tokenFreq *
                std::log((1 + (docN - docFreq + BM25_DEFAULT_OFFSET) / (docFreq + BM25_DEFAULT_OFFSET)));
            queryTokens[tokenIdx].qTokenMaxScore = tokenMeta->maxScore;
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
    uint32 &tokenFoundCount, BM25ScanOpaque so, bool cutForSearch = false)
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
            FindTokenInfo(meta, cTokenMetaspage, tokenizedQuery, queryTokens, tokenIdx, tokenFoundCount, so);
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

static BM25QueryTokensInfo GetQueryTokens(Relation index, const char* sentence, BM25ScanOpaque so)
{
    uint32 tokenCount = 0;
    uint32 tokenFoundCount = 0;
    BM25QueryToken *queryTokens = ScanIndexForTokenInfo(index, sentence, tokenCount, tokenFoundCount, so);
    if (queryTokens == nullptr) {
        /* no token found, try to use cutForSearch to get tokens */
        queryTokens = ScanIndexForTokenInfo(index, sentence, tokenCount, tokenFoundCount, so, true);
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

/*
 * Distributed global avgdl (rescore mode): candidates are retrieved and pruned under the
 * local avgdl, so the build-time maxScore upper bounds stay valid. Only the returned
 * candidates are then rescored with the global avgdl, making the final scores comparable
 * across shards when the CN merges per-shard topK results.
 */
static void RescoreCandidatesWithGlobalAvgdl(Relation index, BM25QueryTokensInfo &queryTokenInfo,
    BM25ScanOpaque so, float globalAvgdl)
{
    if (so->candNums == 0 || queryTokenInfo.size == 0) {
        return;
    }
    BM25Scorer scorer = BM25Scorer(u_sess->attr.attr_sql.bm25_k1, u_sess->attr.attr_sql.bm25_b, globalAvgdl);
    /* ascend by docId so each posting list is walked forward exactly once */
    qsort(so->candDocs, (size_t)so->candNums, sizeof(BM25ScanData), CompareBM25ScanDataByDocId);
    for (uint32 i = 0; i < so->candNums; ++i) {
        so->candDocs[i].score = 0.0f;
    }
    /* fresh cursors without the docId mask: candidates are already masked as returned */
    Vector<BM25ScanCursor> cursors = MakeBM25ScanCursors(index, queryTokenInfo.queryTokens,
        queryTokenInfo.size, nullptr);
    for (uint32 t = 0; t < cursors.size(); ++t) {
        BM25ScanCursor *cursor = &cursors[t];
        for (uint32 i = 0; i < so->candNums; ++i) {
            uint32 docId = so->candDocs[i].docId;
            if (docId == BM25_INVALID_DOC_ID) {
                continue;
            }
            cursor->Seek(docId);
            if (cursor->curDocId == docId) {
                so->candDocs[i].score += cursor->qTokenIDFVal *
                    scorer.GetDocBM25Score(cursor->tokenFreqInDoc, cursor->curDocLength);
            }
        }
    }
    CloseCursors(cursors);
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
    so->globalDfMap = NULL;
    so->scanMcxt = CurrentMemoryContext;
    so->globalDocumentCount = 0;
    so->globalTokenCount = 0;

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
        if (scan->orderByData != NULL && !(scan->orderByData[0].sk_flags & SK_ISNULL)) {
            arr = DatumGetArrayTypeP(scan->orderByData[0].sk_argument);
        } else if (scan->keyData != NULL && !(scan->keyData[0].sk_flags & SK_ISNULL)) {
            arr = DatumGetArrayTypeP(scan->keyData[0].sk_argument);
        }
        if (arr == NULL) {
            ereport(ERROR, (errmsg("Query is null, can not find any document.")));
        }
        char* queryString = TextDatumGetCString(PointerGetDatum(arr));
        /* Build global term->df map before tokenization (FindTokenInfo reads it for global IDF). */
        if (u_sess->attr.attr_sql.enable_bm25_global_idf && so->globalDfMap == NULL) {
            so->globalDfMap = BuildGlobalDfMap(so->scanMcxt, &so->globalDocumentCount,
                &so->globalTokenCount);
        }
        BM25QueryTokensInfo queryTokenInfo = GetQueryTokens(scan->indexRelation, queryString, so);
        if (queryTokenInfo.size == 0) {
            return false;
        }

        /*
         * Global avgdl (rescore mode): retrieval and pruning always run under the local
         * avgdl so build-time maxScore bounds stay valid; only the returned candidates get
         * rescored with the global avgdl. Skip the rescore when the two avgdls are close
         * enough that scores are already comparable. TAAT scans have no pruning bounds,
         * so they can simply score with the global avgdl directly.
         */
        float localAvgdl = (meta.tokenCount * 1.0) / meta.documentCount;
        float globalAvgdl = 0.0f;
        bool useGlobalAvgdl = Bm25GlobalStatsEnabled(so) && so->globalTokenCount > 0;
        if (useGlobalAvgdl) {
            globalAvgdl = (double)so->globalTokenCount / (double)so->globalDocumentCount;
            if (fabs(globalAvgdl / localAvgdl - 1.0) < BM25_GLOBAL_AVGDL_SKIP_RATIO) {
                useGlobalAvgdl = false;
            }
        }
        bool isTaat = (so->expectedCandNums == 0);
        float scanAvgdl = (useGlobalAvgdl && isTaat) ? globalAvgdl : localAvgdl;
        BM25IndexScan(scan->indexRelation, queryTokenInfo, meta.nextDocId, scanAvgdl, so);
        if (useGlobalAvgdl && !isTaat) {
            RescoreCandidatesWithGlobalAvgdl(scan->indexRelation, queryTokenInfo, so, globalAvgdl);
        }
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
    DestroyGlobalDfMap(so->globalDfMap);
    so->globalDfMap = NULL;
    pfree_ext(so);
    if (u_sess->bm25_ctx.scoreHashTable != NULL) {
        DELETE_EX(u_sess->bm25_ctx.scoreHashTable);
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

/* <&> BM25 ordering operator OID (see pg_operator.data) */
#define BM25_ORDER_BY_OP_OID 6208

/*
 * Collect THIS shard's local BM25 stats for a query: documentCount (N), tokenCount (T),
 * and per query-term df, appended to dfList in GUC format "term:df,term:df,...".
 */
static void CollectLocalBm25Stats(Relation index, const char *query, uint32 *ndocs,
    uint64 *ntokens, StringInfo dfList)
{
    BM25MetaPageData meta;
    BM25GetMetaPageInfo(index, &meta);
    *ndocs = meta.documentCount;
    *ntokens = meta.tokenCount;

    BM25TokenizedDocData tq = BM25DocumentTokenize(query, Bm25GetDictPath(index), true);
    if (tq.tokenCount == 0) {
        return;
    }
    BlockNumber *buckets = (BlockNumber *)palloc0(sizeof(BlockNumber) * tq.tokenCount);
    for (uint32 i = 0; i < tq.tokenCount; i++) {
        buckets[i] = InvalidBlockNumber;
    }
    uint32 bucketFound = 0;
    BlockNumber hbBlk = meta.entryPageList.hashBucketsPage;
    if (BlockNumberIsValid(hbBlk)) {
        Buffer hbBuf = ReadBuffer(index, hbBlk);
        LockBuffer(hbBuf, BUFFER_LOCK_SHARE);
        FindBucketsLocation(BufferGetPage(hbBuf), tq, buckets,
            meta.entryPageList.maxHashBucketCount, bucketFound);
        UnlockReleaseBuffer(hbBuf);
    }
    for (uint32 i = 0; i < tq.tokenCount; i++) {
        if (!Bm25TermIsDfEncodable(tq.tokenDatas[i].tokenValue)) {
            continue;
        }
        bool dup = false;
        for (uint32 j = 0; j < i; j++) {
            if (strncmp(tq.tokenDatas[i].tokenValue, tq.tokenDatas[j].tokenValue,
                BM25_MAX_TOKEN_LEN - 1) == 0) {
                dup = true;
                break;
            }
        }
        if (dup) {
            continue;
        }
        uint32 df = 0;
        BlockNumber nb = buckets[i];
        while (BlockNumberIsValid(nb)) {
            Buffer tbuf = ReadBuffer(index, nb);
            LockBuffer(tbuf, BUFFER_LOCK_SHARE);
            Page tpage = BufferGetPage(tbuf);
            OffsetNumber maxoff = PageGetMaxOffsetNumber(tpage);
            for (OffsetNumber off = FirstOffsetNumber; off <= maxoff; off++) {
                BM25TokenMetaPage tm = (BM25TokenMetaPage)PageGetItem(tpage, PageGetItemId(tpage, off));
                if (tm->hashValue == tq.tokenDatas[i].hashValue &&
                    strncmp(tm->token, tq.tokenDatas[i].tokenValue, BM25_MAX_TOKEN_LEN - 1) == 0) {
                    df = tm->docCount;
                    break;
                }
            }
            nb = BM25PageGetOpaque(tpage)->nextblkno;
            UnlockReleaseBuffer(tbuf);
            if (df > 0) {
                break;
            }
        }
        appendStringInfo(dfList, "%s%s:%u", (dfList->len > 0 ? "," : ""), tq.tokenDatas[i].tokenValue, df);
    }
    pfree(buckets);
    if (tq.tokenDatas != NULL) {
        pfree(tq.tokenDatas);
    }
}

/*
 * bm25_shard_stat(index regclass, query text) -> text
 *
 * Returns THIS shard's local BM25 statistics, encoded as "N=<N>;T=<T>;<term:df,...>".
 * A CN aggregates this across all DNs (sum N, sum T, sum df per term) to obtain global
 * stats. On a single node it returns the full (=global) statistics directly.
 */
Datum bm25_shard_stat(PG_FUNCTION_ARGS)
{
    Oid indexOid = PG_GETARG_OID(0);
    char *query = text_to_cstring(PG_GETARG_TEXT_PP(1));
    Relation index = index_open(indexOid, AccessShareLock);

    uint32 ndocs = 0;
    uint64 ntokens = 0;
    StringInfoData df;
    initStringInfo(&df);
    CollectLocalBm25Stats(index, query, &ndocs, &ntokens, &df);

    index_close(index, AccessShareLock);
    pfree(query);

    StringInfoData buf;
    initStringInfo(&buf);
    appendStringInfo(&buf, "N=%u;T=%lu;%s", ndocs, (unsigned long)ntokens, df.data);
    PG_RETURN_TEXT_P(cstring_to_text(buf.data));
}

/*
 * bm25_table_stat(table_pattern text, query text [, column_name text
 *                 [, schema_name text]]) -> text
 *
 * Optimized collection: tokenizes the query ONCE, then iterates over all BM25 shard
 * indexes whose base table matches the given regex pattern on this DN. If column_name
 * is omitted, each matched table must have exactly one BM25 index; if it is supplied,
 * each table must have exactly one BM25 index on that column. Aggregates N / T /
 * per-term df internally and returns "N=<N>;T=<T>;<term:df,...>".
 */
Datum bm25_table_stat(PG_FUNCTION_ARGS)
{
    char *tablePattern = text_to_cstring(PG_GETARG_TEXT_PP(0));
    char *query = text_to_cstring(PG_GETARG_TEXT_PP(1));
    char *columnName = (PG_NARGS() > 2 && !PG_ARGISNULL(2))
        ? text_to_cstring(PG_GETARG_TEXT_PP(2))
        : NULL;
    char *schemaName = (PG_NARGS() > 3 && !PG_ARGISNULL(3))
        ? text_to_cstring(PG_GETARG_TEXT_PP(3))
        : NULL;

    StringInfoData sql;
    initStringInfo(&sql);
    appendStringInfo(&sql,
        "SELECT i.indexrelid, i.indrelid, t.relname FROM pg_index i "
        "JOIN pg_class c ON i.indexrelid = c.oid "
        "JOIN pg_am a ON c.relam = a.oid "
        "JOIN pg_class t ON i.indrelid = t.oid "
        "JOIN pg_namespace n ON t.relnamespace = n.oid "
        "JOIN pg_attribute attr ON attr.attrelid = t.oid AND attr.attnum = i.indkey[0] "
        "WHERE a.amname = 'bm25' AND t.relname ~ $1 "
        "AND i.indisvalid AND i.indisready AND i.indnatts = 1 "
        "AND NOT attr.attisdropped AND ($2 IS NULL OR attr.attname = $2) "
        "AND ($3 IS NULL OR n.nspname = $3) "
        "ORDER BY i.indrelid, i.indexrelid");

    Oid argTypes[3] = {TEXTOID, TEXTOID, TEXTOID};
    Datum argValues[3] = {PG_GETARG_DATUM(0), (Datum)0, (Datum)0};
    char nulls[4] = {' ', columnName == NULL ? 'n' : ' ',
                     schemaName == NULL ? 'n' : ' ', '\0'};
    if (columnName != NULL) {
        argValues[1] = CStringGetTextDatum(columnName);
    }
    if (schemaName != NULL) {
        argValues[2] = CStringGetTextDatum(schemaName);
    }

    MemoryContext callerContext = CurrentMemoryContext;
    int spiResult = SPI_connect();
    if (spiResult != SPI_OK_CONNECT) {
        ereport(ERROR, (errmsg("could not connect to SPI for BM25 statistics")));
    }
    int ret = SPI_execute_with_args(sql.data, 3, argTypes, argValues, nulls, true, 0, NULL);
    if (ret != SPI_OK_SELECT || SPI_processed == 0) {
        SPI_finish();
        pfree(tablePattern);
        pfree(query);
        pfree_ext(columnName);
        pfree_ext(schemaName);
        pfree(sql.data);
        PG_RETURN_TEXT_P(cstring_to_text("N=0;T=0;"));
    }

    int indexCount = (int)SPI_processed;
    Oid *indexOids = (Oid *)MemoryContextAlloc(callerContext, indexCount * sizeof(Oid));
    Oid previousTableOid = InvalidOid;
    for (int i = 0; i < indexCount; i++) {
        bool indexIsNull = false;
        bool tableIsNull = false;
        Datum indexDatum = SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1, &indexIsNull);
        indexOids[i] = indexIsNull ? InvalidOid : DatumGetObjectId(indexDatum);
        Datum tableDatum = SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 2, &tableIsNull);
        Oid tableOid = tableIsNull ? InvalidOid : DatumGetObjectId(tableDatum);
        if (OidIsValid(tableOid) && tableOid == previousTableOid) {
            char *spiTableName = SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 3);
            char *tableName = MemoryContextStrdup(callerContext, spiTableName);
            SPI_finish();
            pfree(indexOids);
            pfree(tablePattern);
            pfree(query);
            pfree(sql.data);
            if (columnName == NULL) {
                ereport(ERROR,
                    (errcode(ERRCODE_AMBIGUOUS_PARAMETER),
                        errmsg("multiple BM25 indexes found on table \"%s\"; specify column_name", tableName)));
            } else {
                ereport(ERROR,
                    (errcode(ERRCODE_AMBIGUOUS_PARAMETER),
                        errmsg("multiple BM25 indexes found for column \"%s\" on table \"%s\"",
                            columnName, tableName)));
            }
        }
        previousTableOid = tableOid;
    }
    SPI_finish();
    pfree(sql.data);

    uint64 totalN = 0;
    uint64 totalT = 0;
    BM25TokenizedDocData tq = {NULL, 0, 0};
    bool tokenized = false;

    /* Per-term df accumulator, summed over every shard index on this node. */
    uint32 *dfAccum = NULL;
    uint32 tokenCount = 0;
    /* Which tokens to report: first occurrence only, and encodable in the df format. */
    bool *wanted = NULL;

    for (int idx = 0; idx < indexCount; idx++) {
        if (!OidIsValid(indexOids[idx])) {
            continue;
        }
        Relation index = index_open(indexOids[idx], AccessShareLock);
        BM25MetaPageData meta;
        BM25GetMetaPageInfo(index, &meta);
        totalN += meta.documentCount;
        totalT += meta.tokenCount;

        if (!tokenized) {
            tq = BM25DocumentTokenize(query, Bm25GetDictPath(index), true);
            tokenized = true;
            if (tq.tokenCount > 0) {
                tokenCount = tq.tokenCount;
                dfAccum = (uint32 *)palloc0(tokenCount * sizeof(uint32));
                wanted = (bool *)palloc0(tokenCount * sizeof(bool));
                for (uint32 i = 0; i < tokenCount; i++) {
                    if (!Bm25TermIsDfEncodable(tq.tokenDatas[i].tokenValue)) {
                        continue;
                    }
                    bool dup = false;
                    for (uint32 j = 0; j < i; j++) {
                        if (strncmp(tq.tokenDatas[i].tokenValue, tq.tokenDatas[j].tokenValue,
                            BM25_MAX_TOKEN_LEN - 1) == 0) {
                            dup = true;
                            break;
                        }
                    }
                    wanted[i] = !dup;
                }
            }
        }

        if (tokenCount == 0) {
            index_close(index, AccessShareLock);
            continue;
        }

        BlockNumber *buckets = (BlockNumber *)palloc0(sizeof(BlockNumber) * tokenCount);
        for (uint32 i = 0; i < tokenCount; i++) {
            buckets[i] = InvalidBlockNumber;
        }
        uint32 bucketFound = 0;
        BlockNumber hbBlk = meta.entryPageList.hashBucketsPage;
        if (BlockNumberIsValid(hbBlk)) {
            Buffer hbBuf = ReadBuffer(index, hbBlk);
            LockBuffer(hbBuf, BUFFER_LOCK_SHARE);
            FindBucketsLocation(BufferGetPage(hbBuf), tq, buckets,
                meta.entryPageList.maxHashBucketCount, bucketFound);
            UnlockReleaseBuffer(hbBuf);
        }
        for (uint32 i = 0; i < tokenCount; i++) {
            if (!wanted[i]) {
                continue;
            }
            uint32 df = 0;
            BlockNumber nb = buckets[i];
            while (BlockNumberIsValid(nb)) {
                Buffer tbuf = ReadBuffer(index, nb);
                LockBuffer(tbuf, BUFFER_LOCK_SHARE);
                Page tpage = BufferGetPage(tbuf);
                OffsetNumber maxoff = PageGetMaxOffsetNumber(tpage);
                for (OffsetNumber off = FirstOffsetNumber; off <= maxoff; off++) {
                    BM25TokenMetaPage tm = (BM25TokenMetaPage)PageGetItem(tpage, PageGetItemId(tpage, off));
                    if (tm->hashValue == tq.tokenDatas[i].hashValue &&
                        strncmp(tm->token, tq.tokenDatas[i].tokenValue, BM25_MAX_TOKEN_LEN - 1) == 0) {
                        df = tm->docCount;
                        break;
                    }
                }
                nb = BM25PageGetOpaque(tpage)->nextblkno;
                UnlockReleaseBuffer(tbuf);
                if (df > 0) {
                    break;
                }
            }
            dfAccum[i] += df;
        }
        pfree(buckets);
        index_close(index, AccessShareLock);
    }

    StringInfoData result;
    initStringInfo(&result);
    appendStringInfo(&result, "N=%lu;T=%lu;", (unsigned long)totalN, (unsigned long)totalT);
    if (tokenCount > 0 && dfAccum != NULL) {
        bool first = true;
        for (uint32 i = 0; i < tokenCount; i++) {
            if (!wanted[i]) {
                continue;
            }
            appendStringInfo(&result, "%s%s:%u", (first ? "" : ","),
                             tq.tokenDatas[i].tokenValue, dfAccum[i]);
            first = false;
        }
    }

    if (tq.tokenDatas != NULL) {
        pfree(tq.tokenDatas);
    }
    if (dfAccum != NULL) {
        pfree(dfAccum);
    }
    if (wanted != NULL) {
        pfree(wanted);
    }
    pfree(indexOids);
    pfree(tablePattern);
    pfree(query);
    pfree_ext(columnName);
    pfree_ext(schemaName);

    PG_RETURN_TEXT_P(cstring_to_text(result.data));
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