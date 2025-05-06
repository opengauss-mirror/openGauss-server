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
 * bm25_vec_inverted_index.h
 *
 * IDENTIFICATION
 *        src/include/access/datavec/bm25_vec_inverted_index.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef BM25_H
#define BM25_H

#include "postgres.h"
#include "access/genam.h"
#include "access/generic_xlog.h"
#include "lib/pairingheap.h"
#include "nodes/execnodes.h"
#include "access/generic_xlog.h"
#include "catalog/pg_operator.h"
#include "port.h" /* for random() */
#include "access/datavec/vecindex.h"

#define BM25_VERSION 1
#define BM25_MAGIC_NUMBER 0x14FF1A8
#define BM25_PAGE_ID 0xFF85

#define BM25PageGetOpaque(page) ((BM25PageOpaque)PageGetSpecialPointer(page))
#define BM25PageGetMeta(page) ((BM25MetaPageData *)PageGetContents(page))
#define BM25PageGetDocMeta(page) ((BM25DocumentMetaPageData *)PageGetContents(page))
#define BM25PageGetDocForwardMeta(page) ((BM25DocForwardMetaPageData *)PageGetContents(page))

/* Preserved page numbers */
#define BM25_METAPAGE_BLKNO 0
#define BM25_INVALID_DOC_ID 0xFFFFFFFF
#define BM25_PAGE_DATASIZE 8000
#define BM25_DOCUMENT_ITEM_SIZE (MAXALIGN(sizeof(BM25DocumentItem)))
#define BM25_DOCUMENT_MAX_COUNT_IN_PAGE (BM25_PAGE_DATASIZE / BM25_DOCUMENT_ITEM_SIZE)
#define BM25_DOCUMENT_FORWARD_ITEM_SIZE (MAXALIGN(sizeof(BM25DocForwardItem)))
#define BM25_DOC_FORWARD_MAX_COUNT_IN_PAGE (BM25_PAGE_DATASIZE / BM25_DOCUMENT_FORWARD_ITEM_SIZE)
#define BM25_MAX_TOKEN_LEN 100
#define BM25_BUCKET_MAX_NUM 1000

typedef struct BM25ScanData {
    uint32 docId;
    float score;
    ItemPointerData heapCtid;
} BM25ScanData;

typedef struct BM25TokenData {
    uint32 hashValue;
    uint32 tokenId;
    uint32 tokenFreq;
    char tokenValue[BM25_MAX_TOKEN_LEN];
} BM25TokenData;

typedef struct BM25TokenizedDocData {
    BM25TokenData* tokenDatas;
    uint32 tokenCount;
    uint32 docLength;
} BM25TokenizedDocData;

typedef struct BM25ScanOpaqueData {
    uint32 cursor;
    BM25ScanData* candDocs;
    uint32 candNums;
    uint32 expectedCandNums;
    uint32 expandedTimes;
    unsigned char* docIdMask;
    uint32 docIdMaskSize;
} BM25ScanOpaqueData;

typedef BM25ScanOpaqueData *BM25ScanOpaque;

typedef struct BM25EntryPages {
    BlockNumber documentMetaPage;
    BlockNumber docForwardPage;
    BlockNumber hashBucketsPage;
    uint32 hashBucketCount;
} BM25EntryPages;

typedef struct BM25MetaPageData {
    uint32 magicNumber;
    uint32 version;
    BM25EntryPages entryPageList;

    /* statics */
    uint32 documentCount;
    uint64 tokenCount;
    uint32 nextDocId;
    uint32 nextTokenId;
} BM25MetaPageData;

typedef BM25MetaPageData *BM25MetaPage;

typedef struct BM25PageOpaqueData {
    BlockNumber nextblkno;
    uint16 unused;
    uint16 page_id; /* for identification of BM25 indexes */
} BM25PageOpaqueData;

typedef BM25PageOpaqueData *BM25PageOpaque;

typedef struct BM25DocumentMetaPageData {
    BlockNumber startDocPage;
    BlockNumber lastDocPage;
    uint32 docCapacity;
} BM25DocumentMetaPageData;

typedef BM25DocumentMetaPageData *BM25DocMetaPage;

typedef struct BM25DocForwardMetaPageData {
    BlockNumber startPage;
    BlockNumber lastPage;
    uint64 size;
    uint64 capacity;
} BM25DocForwardMetaPageData;

typedef BM25DocForwardMetaPageData *BM25DocForwardMetaPage;

typedef struct BM25DocForwardItem {
    uint32 tokenId;
    uint32 tokenHash;
} BM25DocForwardItem;

typedef struct BM25DocumentItem {
    IndexTupleData ctid;
    uint32 docId;
    uint32 docLength;
    bool isActived;
    uint64 tokenStartIdx;
    uint64 tokenEndIdx;
} BM25DocumentItem;

typedef struct BM25HashBucketItem {
    uint32 bucketId;
    BlockNumber bucketBlkno;
} BM25HashBucketItem;

typedef BM25HashBucketItem *BM25HashBucketPage;

typedef struct BM25TokenMetaItem {
    uint32 tokenId;
    uint32 docCount;
    BlockNumber postingBlkno;
    float maxScore;
    char token[BM25_MAX_TOKEN_LEN];
} BM25TokenMetaItem;

typedef BM25TokenMetaItem *BM25TokenMetaPage;

typedef struct BM25TokenPostingItem {
    uint32 docId;
    uint16 docLength;
    uint16 freq;
} BM25TokenPostingItem;

typedef BM25TokenPostingItem *BM25TokenPostingPage;

typedef struct BM25PageLocationInfo {
    BlockNumber blkno;
    OffsetNumber offno;
} BM25PageLocationInfo;

typedef struct BM25Shared {
    /* Immutable state */
    Oid heaprelid;
    Oid indexrelid;

    /* Mutex for mutable state */
    slock_t mutex;

    /* Mutable state */
    int nparticipantsdone;
    double reltuples;

    BM25EntryPages bm25EntryPages;

    ParallelHeapScanDescData heapdesc;
} BM25Shared;

typedef struct BM25Leader {
    int nparticipanttuplesorts;
    BM25Shared *bm25shared;
} BM25Leader;

typedef struct BM25ReorderShared {
    BM25PageLocationInfo *startPageLocation;
    uint32 batchCount;
    uint32 curThreadId;
    Oid heaprelid;
    Oid indexrelid;
} BM25ReorderShared;

typedef struct BM25BuildState {
    /* Info */
    Relation heap;
    Relation index;
    IndexInfo *indexInfo;
    ForkNumber forkNum;

    BM25EntryPages bm25EntryPages;

    /* Statistics */
    double indtuples;
    double reltuples;

    /* Support functions */
    FmgrInfo *procinfo;
    Oid collation;

    /* Memory */
    MemoryContext tmpCtx;

    BM25Leader *bm25leader;
} BM25BuildState;

struct BM25ScanCursor {
public:
	BM25ScanCursor() {}
    BM25ScanCursor(Relation indexRel, const BlockNumber postingBlock, float tokenMaxScore, float idfVal,
        unsigned char* docIdMask)
        : tokenPostingBlock(postingBlock),
          qTokenMaxScore(tokenMaxScore),
          qTokenIDFVal(idfVal),
          curBlkno(postingBlock),
          curOffset(InvalidOffsetNumber),
          curDocId(BM25_INVALID_DOC_ID),
          tokenFreqInDoc(0.0f),
          curDocLength(0.0f),
          index(indexRel),
          docIdfilter(docIdMask)
    {
        Next(true);
    }

    void Next(bool isInit = false);
    void Seek(uint32 docId);
    void Close();

    BlockNumber tokenPostingBlock; /* first block number of inverted list */
    float qTokenMaxScore;
    float qTokenIDFVal;

    /* update when iterator */
    BlockNumber curBlkno;
    OffsetNumber curOffset;
    uint32 curDocId;
    float tokenFreqInDoc; /* number of the token in cur doc */
    float curDocLength; /* number of the token in cur doc */
    Relation index;
    unsigned char* docIdfilter;
    Buffer buf;
    Page page;
};  // struct BM25ScanCursor

struct BM25Scorer : public BaseObject {
public:
    float m_k1;
    float m_b;
    float m_avgdl;

    float GetDocBM25Score(float tf, float docLen) {
        return tf * (m_k1 + 1) / (tf + m_k1 * (1 - m_b + m_b * (docLen / m_avgdl)));
    }

    BM25Scorer(float k1, float b, float avgdl)
        : m_k1(k1), m_b(b), m_avgdl(avgdl) {
    }
};  // struct BM25Scorer

/* Methods */
Buffer BM25NewBuffer(Relation index, ForkNumber forkNum);
void BM25InitPage(Buffer buf, Page page);
void BM25InitRegisterPage(Relation index, Buffer *buf, Page *page, GenericXLogState **state);
void BM25CommitBuffer(Buffer buf, GenericXLogState *state);
void BM25AppendPage(Relation index, Buffer *buf, Page *page, ForkNumber forkNum, bool unlockOldBuf = true);
void BM25GetMetaPageInfo(Relation index, BM25MetaPage metap);
uint32 BM25AllocateDocId(Relation index);
uint32 BM25AllocateTokenId(Relation index);
void BM25IncreaseDocAndTokenCount(Relation index, uint32 tokenCount, float &avgdl);
BlockNumber SeekBlocknoForDoc(Relation index, uint32 docId, BlockNumber startBlkno, BlockNumber step);
bool FindHashBucket(uint32 bucketId, BM25PageLocationInfo &bucketLocation, Buffer buf, Page page);
bool FindTokenMeta(BM25TokenData &tokenData, BM25PageLocationInfo &tokenMetaLocation, Buffer buf, Page page);
BM25TokenizedDocData BM25DocumentTokenize(const char* doc);

Datum bm25build(PG_FUNCTION_ARGS);
Datum bm25buildempty(PG_FUNCTION_ARGS);
Datum bm25insert(PG_FUNCTION_ARGS);
Datum bm25beginscan(PG_FUNCTION_ARGS);
Datum bm25rescan(PG_FUNCTION_ARGS);
Datum bm25gettuple(PG_FUNCTION_ARGS);
Datum bm25endscan(PG_FUNCTION_ARGS);
Datum bm25bulkdelete(PG_FUNCTION_ARGS);
Datum bm25vacuumcleanup(PG_FUNCTION_ARGS);
Datum bm25costestimate(PG_FUNCTION_ARGS);
Datum bm25options(PG_FUNCTION_ARGS);
Datum bm25_scores_textarr(PG_FUNCTION_ARGS);
Datum bm25_scores_text(PG_FUNCTION_ARGS);

extern IndexBuildResult* bm25build_internal(Relation heap, Relation index, IndexInfo *indexInfo);
extern void bm25rescan_internal(IndexScanDesc scan, ScanKey keys, int nkeys, ScanKey orderbys, int norderbys);
extern void bm25buildempty_internal(Relation index);
extern bool bm25insert_internal(Relation index, Datum *values, ItemPointer heapCtid);
extern IndexScanDesc bm25beginscan_internal(Relation rel, int nkeys, int norderbys);
extern bool bm25gettuple_internal(IndexScanDesc scan, ScanDirection dir);
extern void bm25endscan_internal(IndexScanDesc scan);

#endif //BM25_H
