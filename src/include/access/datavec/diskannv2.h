/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
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
 * diskannv2.h
 *
 *        DiskANN RaBitQ format (meta page version 2).
 *
 *        Index layout (block numbers ascending):
 *          0            meta page        DiskAnnV2MetaPageData
 *          1            extension lock page
 *          2 ..         transform region PcaSerialize stream (mean[dimIn] then M[dimOut][dimIn])
 *                                        split into 8000-byte page payloads
 *          ..           code region      MAXALIGN(16 + codeBytes) slots: RabitqVectorBits
 *          ..           graph region     320-byte slots: adjacency (R = 64) + heap TIDs (<= 10)
 *          ..           tail chunks      1024 nodes each: code pages followed by graph pages,
 *                                        appended by INSERT under the block-1 extension lock
 *        Original vectors are not stored; exact rerank reads them from the heap.
 *
 *        Every page keeps DiskAnnPageOpaqueData (pageId = DISKANN_PAGE_ID) in its
 *        special area; pageType tells the page kinds apart.
 *
 * IDENTIFICATION
 *        src/include/access/datavec/diskannv2.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef DISKANNV2_H
#define DISKANNV2_H

#include "postgres.h"
#include "access/genam.h"
#include "access/relscan.h"
#include "access/sdir.h"
#include "storage/buf/bufpage.h"
#include "storage/item/itemptr.h"
#include "utils/rel.h"
#include "access/datavec/diskann.h"
#include "access/datavec/rabitq.h"
#include "access/datavec/vectortransformer.h"

#define DISKANN_V2_INVALID_NODE 0xFFFFFFFFu

/* fixed graph geometry */
#define DISKANN_V2_DEGREE 64
#define DISKANN_V2_CHUNK_NODES 1024

/* PCA training sample cap */
#define DISKANN_V2_SAMPLE_CAP 65536

/* region page layout: slots / stream payload start at a 64B page offset */
#define DISKANN_V2_PAGE_DATA_OFFSET 64
#define DISKANN_V2_CODE_SLOT_ALIGN MAXIMUM_ALIGNOF /* code slots are MAXALIGNed like the HNSW / IVF RaBitQ codes */
#define DISKANN_V2_CACHE_LINE 64                   /* prefetch stride */
#define DISKANN_V2_CODE_FACTOR_BYTES 16 /* sizeof(FactorDataBits): normSqr + dpMul + ipMu + resSqr */
#define DISKANN_V2_GRAPH_SLOT 320
#define DISKANN_V2_GRAPH_SLOTS_PER_PAGE 25
#define DISKANN_V2_XFORM_PAGE_BYTES 8000 /* transform stream payload per page */

/* query quantization width used by INSERT (scans take rbq_query_bits) */
#define DISKANN_V2_INSERT_QUERY_BITS 8

/* usable payload bytes per code / graph page */
#define DISKANN_V2_PAGE_USABLE \
    (BLCKSZ - DISKANN_V2_PAGE_DATA_OFFSET - MAXALIGN(sizeof(DiskAnnPageOpaqueData)))
static inline Size DiskAnnV2PageUsable(void)
{
    return DISKANN_V2_PAGE_USABLE;
}

/* DiskAnnPageOpaqueData.pageType values. v1 pages leave pageType at 0. */
typedef enum DiskAnnV2PageType {
    DISKANN_V2_PAGE_META = 1,
    DISKANN_V2_PAGE_LOCK = 2,
    DISKANN_V2_PAGE_XFORM = 3,
    DISKANN_V2_PAGE_CODE = 4,
    DISKANN_V2_PAGE_GRAPH = 5
} DiskAnnV2PageType;

typedef struct DiskAnnV2Extent {
    BlockNumber startBlk;
    uint32 nblocks;
} DiskAnnV2Extent;

/*
 * On-disk meta page (block 0). The first two fields share their offsets with
 * DiskAnnMetaPageData so the version can be read before choosing a format.
 * Slot geometry is not stored: it is derived from dimOut / rabitqBits.
 */
typedef struct DiskAnnV2MetaPageData {
    uint32 magicNumber; /* DISKANN_MAGIC_NUMBER */
    uint32 version;     /* DISKANN_VERSION_V2 */

    uint16 dimIn;
    uint16 dimOut;
    uint8 distType;    /* DIS_L2 / DIS_IP / DIS_COSINE */
    uint8 rabitqBits;  /* 1 or 2 */
    uint16 graphDegree; /* DISKANN_V2_DEGREE */

    uint32 indexSize; /* candidate list size L (build + insert) */

    uint32 nextNodeId;   /* allocation high-water mark = node count */
    uint32 frozenNodeId; /* traversal entry, DISKANN_V2_INVALID_NODE while empty */

    uint32 tailNodeStart;  /* first node addressed through fixed chunks */
    BlockNumber tailStart; /* block of tail chunk 0 (valid even when count = 0) */
    uint32 tailChunkCount; /* fully initialized chunks published to readers */

    DiskAnnV2Extent xform; /* transform region */
    DiskAnnV2Extent code;  /* initial code region */
    DiskAnnV2Extent graph; /* initial graph region */
} DiskAnnV2MetaPageData;
typedef DiskAnnV2MetaPageData* DiskAnnV2MetaPage;

/* runtime view of the meta page: on-disk fields plus derived slot geometry */
typedef struct DiskAnnV2Meta {
    uint16 dimIn;
    uint16 dimOut;
    uint8 distType;
    uint8 rabitqBits;
    uint16 graphDegree;
    uint32 indexSize;
    uint32 nextNodeId;
    uint32 frozenNodeId;
    uint32 tailNodeStart;
    BlockNumber tailStart;
    uint32 tailChunkCount;
    DiskAnnV2Extent xform;
    DiskAnnV2Extent code;
    DiskAnnV2Extent graph;

    uint16 codeSlotSize;
    uint16 codeSlotsPerPage;
    uint16 graphSlotSize;
    uint16 graphSlotsPerPage;
} DiskAnnV2Meta;

/*
 * Code region slot = one RabitqVectorBits of the rabitq module: the 16-byte
 * factor head (normSqr, dpMul, ipMu, resSqr) followed by `bits` planes of
 * rbqPlaneBytes(dimOut) bytes each (1-bit: s_hi; 2-bit: s_hi then s_lo).
 * Slot = MAXALIGN(16 + codeBytes); padding is zero. Encoding and distance
 * estimation are ComputeVectorRBQCodeBits / ComputeRbqDistanceBits /
 * ComputeRbqCodeDistanceBits, DiskANN only packs slots.
 */
typedef RabitqVectorBits DiskAnnV2CodeSlot;

/*
 * Graph region slot (320B): adjacency + heap TIDs of the node. tidCount = 0
 * is a tombstone: the node keeps routing traversals but returns no rows.
 */
typedef struct DiskAnnV2GraphSlot {
    uint16 count;   /* valid entries in nexts */
    uint8 tidCount; /* valid entries in heaptids, 0 = tombstone */
    uint8 flags;    /* reserved */
    uint32 nexts[DISKANN_V2_DEGREE];
    ItemPointerData heaptids[DISKANN_HEAPTIDS];
} DiskAnnV2GraphSlot;

static inline DiskAnnV2MetaPageData *DiskAnnV2PageGetMeta(Page page)
{
    return (DiskAnnV2MetaPageData *)PageGetContents(page);
}

/* ------------------------------------------------------------ geometry */

/* codeBytes = bits * ceil(D / 8): the rabitq module's byte-packed planes */
static inline Size DiskAnnV2CodeBytes(int dimOut, int bits)
{
    return rbqCodeBytesBits(dimOut, bits);
}

/* codeSlot = MAXALIGN(16 + codeBytes) */
static inline Size DiskAnnV2CodeSlotBytes(int dimOut, int bits)
{
    return TYPEALIGN(DISKANN_V2_CODE_SLOT_ALIGN, DISKANN_V2_CODE_FACTOR_BYTES + DiskAnnV2CodeBytes(dimOut, bits));
}

static inline uint32 DiskAnnV2CodeSlotsPerPage(Size codeSlot)
{
    if (codeSlot == 0) {
        return 0;
    }
    return (uint32)(DiskAnnV2PageUsable() / codeSlot);
}

/* slot offset inside its page */
static inline Size DiskAnnV2SlotOffset(uint32 slotSize, uint32 slotNo)
{
    return DISKANN_V2_PAGE_DATA_OFFSET + (Size)slotNo * slotSize;
}

/* pages of one region inside a 1024-node tail chunk */
static inline uint32 DiskAnnV2ChunkCodePages(const DiskAnnV2Meta* meta)
{
    return (DISKANN_V2_CHUNK_NODES + meta->codeSlotsPerPage - 1) / meta->codeSlotsPerPage;
}

static inline uint32 DiskAnnV2ChunkGraphPages(const DiskAnnV2Meta* meta)
{
    return (DISKANN_V2_CHUNK_NODES + meta->graphSlotsPerPage - 1) / meta->graphSlotsPerPage;
}

static inline uint32 DiskAnnV2ChunkPages(const DiskAnnV2Meta* meta)
{
    return DiskAnnV2ChunkCodePages(meta) + DiskAnnV2ChunkGraphPages(meta);
}

/* nodes addressable through the initial regions plus the published chunks */
static inline uint64 DiskAnnV2NodeCapacity(const DiskAnnV2Meta* meta)
{
    return (uint64)meta->tailNodeStart + (uint64)meta->tailChunkCount * DISKANN_V2_CHUNK_NODES;
}

/* exact squared L2 in the original space */
static inline float DiskAnnV2ExactL2(const float* a, const float* b, int dim)
{
    double acc = 0;
    for (int c = 0; c < dim; c++) {
        double d = (double)a[c] - b[c];
        acc += d * d;
    }
    return (float)acc;
}

/* ------------------------------------------------------------ shared utils (diskannv2utils.cpp) */

typedef struct DiskAnnV2CreateMetaArgs {
    int dimIn;
    int dimOut;
    uint8 distType;
    uint32 indexSize;
    uint8 bits;
} DiskAnnV2CreateMetaArgs;

typedef struct DiskAnnV2WriteStreamArgs {
    uint8 pageType;
    Size pageBytes;
    const char* data;
    Size total;
    DiskAnnV2Extent* extOut;
} DiskAnnV2WriteStreamArgs;

typedef struct DiskAnnV2HeapVecArgs {
    Relation heap;
    ItemPointer tid;
    AttrNumber attno;
    bool normalize;
    int dim;
    float* out;
} DiskAnnV2HeapVecArgs;

void DiskAnnV2InitPage(Page page, Size pageSize, uint8 pageType);
uint16 DiskAnnV2CodeSlotSizeFor(int dimOut, int bits);
void DiskAnnV2ComputeGeometry(DiskAnnV2Meta* meta);
void DiskAnnV2MetaFromPage(const DiskAnnV2MetaPageData* src, DiskAnnV2Meta* out);
void DiskAnnV2GetMetaSnapshot(Relation index, DiskAnnV2Meta* out);
void DiskAnnV2CreateMetaPage(Relation index, ForkNumber forkNum, const DiskAnnV2CreateMetaArgs* args);
Buffer DiskAnnV2AppendPage(Relation index, ForkNumber forkNum, uint8 pageType, BlockNumber expected);
void DiskAnnV2WriteStream(Relation index, ForkNumber forkNum, const DiskAnnV2WriteStreamArgs* args);
void DiskAnnV2LoadStream(Relation index, const DiskAnnV2Extent* ext, Size pageBytes, char* out, Size total);
void DiskAnnV2WriteTransform(Relation index, ForkNumber forkNum, const VectorTransform* vt, DiskAnnV2Extent* extOut);
VectorTransform* DiskAnnV2LoadTransform(Relation index, const DiskAnnV2Meta* meta);

/* node addressing: initial regions by dense index, tail nodes by 1024-node chunk */
void DiskAnnV2ResolveCodeSlot(const DiskAnnV2Meta* meta, uint32 nodeId, BlockNumber* blkno, uint16* slot);
void DiskAnnV2ResolveGraphSlot(const DiskAnnV2Meta* meta, uint32 nodeId, BlockNumber* blkno, uint16* slot);
void DiskAnnV2ReadCodeSlot(Relation index, const DiskAnnV2Meta* meta, uint32 nodeId, DiskAnnV2CodeSlot* out);
void DiskAnnV2ReadGraphSlot(Relation index, const DiskAnnV2Meta* meta, uint32 nodeId, DiskAnnV2GraphSlot* out);

/* runtime node allocation and tail growth (diskannv2utils.cpp) */
uint32 DiskAnnV2AllocateNodeId(Relation index, uint32* tailChunkCount);
uint32 DiskAnnV2PublishFirstNode(Relation index, uint32 nodeId);
void DiskAnnV2EnsureNodeCapacity(Relation index, uint64 requiredSlots);
uint32 DiskAnnV2GraphSlotsOnPage(const DiskAnnV2Meta* meta, uint32 nodeId);

bool DiskAnnV2HeapVector(const DiskAnnV2HeapVecArgs* args);

/* build (diskannv2build.cpp) */
IndexBuildResult* DiskAnnV2BuildIndex(Relation heap, Relation index, IndexInfo* indexInfo);
void DiskAnnV2BuildEmptyIndex(Relation index);

#endif /* DISKANNV2_H */
