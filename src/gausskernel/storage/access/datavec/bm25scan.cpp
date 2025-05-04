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
#include "access/datavec/bm25.h"

typedef struct BM25QueryToken {
    BlockNumber tokenPostingBlock;
    float qTokenMaxScore;
    float qTokenIDFVal;
} BM25QueryToken;

typedef struct BM25QueryTokensInfo {
    BM25QueryToken *queryTokens;
    uint32 size;
} BM25QueryTokensInfo;

static void FindBucketsLocation(Page page, BM25TokenizedDocData &tokenizedQuery, BlockNumber *bucketsLocation, uint32 &bucketFoundCount)
{
    OffsetNumber maxoffno = PageGetMaxOffsetNumber(page);
    for (OffsetNumber offnoBucket = FirstOffsetNumber; offnoBucket <= maxoffno; offnoBucket++) {
        BM25HashBucketPage bucket = (BM25HashBucketPage)PageGetItem(page, PageGetItemId(page, offnoBucket));
        for (size_t tokenIdx = 0; tokenIdx < tokenizedQuery.tokenCount; tokenIdx++) {
            if (bucketsLocation[tokenIdx] == InvalidBlockNumber &&
                bucket->bucketId == (tokenizedQuery.tokenDatas[tokenIdx].hashValue % BM25_BUCKET_MAX_NUM)) {
                bucketsLocation[tokenIdx] = bucket->bucketBlkno;
                bucketFoundCount++;
                if (bucketFoundCount >= tokenizedQuery.tokenCount)
                    return;
            }
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
        if (strncmp(tokenMeta->token, tokenizedQuery.tokenDatas[tokenIdx].tokenValue, BM25_MAX_TOKEN_LEN - 1) == 0) {
            queryTokens[tokenIdx].qTokenMaxScore = tokenMeta->maxScore;
            queryTokens[tokenIdx].tokenPostingBlock = tokenMeta->postingBlkno;
            queryTokens[tokenIdx].qTokenIDFVal = tokenizedQuery.tokenDatas[tokenIdx].tokenFreq *
                std::log((1 + ((float)meta.documentCount - (float)tokenMeta->docCount + 0.5) / ((float)tokenMeta->docCount + 0.5)));
            tokenFoundCount++;
            if (tokenFoundCount >= tokenizedQuery.tokenCount)
                return;
        }
    }
    return;
}

static BM25QueryTokensInfo GetQueryTokens(Relation index, const char* sentence)
{
    BM25TokenizedDocData tokenizedQuery = BM25DocumentTokenize(sentence);
    if (tokenizedQuery.tokenCount == 0) {
        BM25QueryTokensInfo emptyTokensInfo{0};
        emptyTokensInfo.queryTokens = nullptr;
        emptyTokensInfo.size = 0;
        return emptyTokensInfo;
    }
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
    BlockNumber nextHashBucketsBlkno = hashBucketsBlkno;
    Buffer cHashBucketsbuf;
    Page cHashBucketspage;

    while (bucketFoundCount < tokenizedQuery.tokenCount && BlockNumberIsValid(nextHashBucketsBlkno)) {
        OffsetNumber maxoffno;
        cHashBucketsbuf = ReadBuffer(index, nextHashBucketsBlkno);
        LockBuffer(cHashBucketsbuf, BUFFER_LOCK_SHARE);
        cHashBucketspage = BufferGetPage(cHashBucketsbuf);
        FindBucketsLocation(cHashBucketspage, tokenizedQuery, bucketsLocation, bucketFoundCount);
        nextHashBucketsBlkno = BM25PageGetOpaque(cHashBucketspage)->nextblkno;
        UnlockReleaseBuffer(cHashBucketsbuf);
    }

    uint32 tokenFoundCount = 0;
    for (size_t tokenIdx = 0; tokenIdx < tokenizedQuery.tokenCount; tokenIdx++) {
        if (!BlockNumberIsValid(bucketsLocation[tokenIdx])) {
            continue;
        }
        Buffer cTokenMetasbuf;
        Page cTokenMetaspage;
        BlockNumber nextTokenMetasBlkno = bucketsLocation[tokenIdx];
        while (tokenFoundCount < tokenizedQuery.tokenCount && BlockNumberIsValid(nextTokenMetasBlkno)) {
            OffsetNumber maxoffno;
            cTokenMetasbuf = ReadBuffer(index, nextTokenMetasBlkno);
            LockBuffer(cTokenMetasbuf, BUFFER_LOCK_SHARE);
            cTokenMetaspage = BufferGetPage(cTokenMetasbuf);
            FindTokenInfo(meta, cTokenMetaspage, tokenizedQuery, queryTokens, tokenIdx, tokenFoundCount);
            nextTokenMetasBlkno = BM25PageGetOpaque(cTokenMetaspage)->nextblkno;
            UnlockReleaseBuffer(cTokenMetasbuf);
        }
    }
    BM25QueryToken *resQueryTokens = (BM25QueryToken*)palloc0(sizeof(BM25QueryToken) * tokenFoundCount);
    uint32 tokenFillIdx = 0;
    for (size_t tokenIdx = 0; tokenIdx < tokenizedQuery.tokenCount; tokenIdx++) {
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
    pfree(bucketsLocation);
    BM25QueryTokensInfo tokensInfo{0};
    tokensInfo.queryTokens = resQueryTokens;
    tokensInfo.size = tokenFoundCount;
    return tokensInfo;
}

IndexScanDesc bm25beginscan_internal(Relation index, int nkeys, int norderbys)
{
    IndexScanDesc scan = RelationGetIndexScan(index, nkeys, norderbys);
    return scan;
}

void bm25rescan_internal(IndexScanDesc scan, ScanKey keys, int nkeys, ScanKey orderbys, int norderbys)
{
    return;
}

bool bm25gettuple_internal(IndexScanDesc scan, ScanDirection dir)
{
    return false;
}

void bm25endscan_internal(IndexScanDesc scan)
{
    return;
}


Datum bm25_scores_textarr(PG_FUNCTION_ARGS)
{
    PG_RETURN_FLOAT8(0.0);
}

Datum bm25_scores_text(PG_FUNCTION_ARGS)
{
    PG_RETURN_FLOAT8(0.0);
}