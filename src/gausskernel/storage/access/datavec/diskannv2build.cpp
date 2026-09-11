/*
 * Copyright (c) 2026 Huawei Technologies Co.,Ltd.
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
 * diskannv2build.cpp
 *
 *        DiskANN RaBitQ format (version 2) build pipeline:
 *          Scan    one heap pass -> original (normalized) vectors into an
 *                  in-memory chunked array, heap TIDs per node (bitwise
 *                  duplicates merge into one node), running mean, reservoir
 *                  sample when PCA is requested
 *          Train   PcaTrain (vectortransformer): population mean, random
 *                  orthogonal rotation W, PCA projection P only when pca_dim
 *                  reduces the dimension; M = W * P
 *          Encode  VtTransform + ComputeVectorRBQCodeBits (rabitq) into the
 *                  code slots; entry = vector closest to the mean
 *          Write   transform / code / graph regions sequentially, full-page
 *                  WAL afterwards, meta page (version 2) last
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/datavec/diskannv2build.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include <cfloat>
#include <cmath>

#include "access/generic_xlog.h"
#include "access/heapam.h"
#include "access/tableam.h"
#include "catalog/index.h"
#include "catalog/pg_type.h"
#include "miscadmin.h"
#include "storage/buf/bufmgr.h"
#include "utils/memutils.h"
#include "access/datavec/utils.h"
#include "access/datavec/vector.h"
#include "access/datavec/diskannv2.h"

/* encode work chunk (CHECK_FOR_INTERRUPTS granularity) */
#define DISKANN_V2_ENCODE_CHUNK 256

/* in-memory vector array: fixed-size chunks so the heap scan can grow it
 * without knowing the row count and without a 2x copy on growth */
#define DISKANN_V2_VEC_CHUNK_SHIFT 16
#define DISKANN_V2_VEC_CHUNK ((uint32)1 << DISKANN_V2_VEC_CHUNK_SHIFT)
#define DISKANN_V2_VEC_CHUNK_MASK (DISKANN_V2_VEC_CHUNK - 1)

#define DISKANN_V2_NODES_INITIAL 65536
#define DISKANN_V2_NODES_INITIAL_MAX (16u * 1024 * 1024) /* pg_class estimates can be stale; grow beyond this */
#define DISKANN_V2_VEC_STORE_INIT_CHUNKS 64
#define DISKANN_V2_GROW_FACTOR 2
#define DISKANN_V2_HASH_SHIFT_EVEN 31
#define DISKANN_V2_HASH_SHIFT_ODD 29
#define DISKANN_V2_RANDOM_HI_SHIFT 31
#define DISKANN_V2_DUP_CAP_MIN 1024
#define DISKANN_V2_DUP_CAP_MAX 0x40000000u

/* ------------------------------------------------------------ vector store */

typedef struct DiskAnnV2VecStore {
    float** chunks;
    uint32 nchunks;
    uint32 capChunks;
    uint32 count;
    int dim;
    MemoryContext ctx;
} DiskAnnV2VecStore;

static inline const float* VecAt(const DiskAnnV2VecStore* vs, uint32 id)
{
    return vs->chunks[id >> DISKANN_V2_VEC_CHUNK_SHIFT] + (Size)(id & DISKANN_V2_VEC_CHUNK_MASK) * vs->dim;
}

static void VecStoreInit(DiskAnnV2VecStore* vs, int dim, MemoryContext ctx)
{
    vs->dim = dim;
    vs->ctx = ctx;
    vs->count = 0;
    vs->nchunks = 0;
    vs->capChunks = DISKANN_V2_VEC_STORE_INIT_CHUNKS;
    vs->chunks = (float**)MemoryContextAllocZero(ctx, sizeof(float*) * vs->capChunks);
}

static void VecStoreAppend(DiskAnnV2VecStore* vs, const float* x)
{
    uint32 id = vs->count;
    uint32 ci = id >> DISKANN_V2_VEC_CHUNK_SHIFT;
    if (ci == vs->nchunks) {
        if (vs->nchunks == vs->capChunks) {
            uint32 newCap = vs->capChunks * DISKANN_V2_GROW_FACTOR;
            float** grown = (float**)MemoryContextAllocZero(vs->ctx, sizeof(float*) * newCap);
            errno_t rc = memcpy_s(grown, sizeof(float*) * newCap, vs->chunks, sizeof(float*) * vs->nchunks);
            if (rc != EOK) {
                securec_check(rc, "\0", "\0");
            }
            pfree(vs->chunks);
            vs->chunks = grown;
            vs->capChunks = newCap;
        }
        Size chunkBytes = sizeof(float) * (Size)DISKANN_V2_VEC_CHUNK * vs->dim;
        float* chunk = (float*)palloc_huge(vs->ctx, chunkBytes);
        vs->chunks[vs->nchunks++] = chunk;
    }
    errno_t rc = memcpy_s(vs->chunks[ci] + (Size)(id & DISKANN_V2_VEC_CHUNK_MASK) * vs->dim,
                          sizeof(float) * (Size)vs->dim, x, sizeof(float) * (Size)vs->dim);
    if (rc != EOK) {
        securec_check(rc, "\0", "\0");
    }
    vs->count++;
}

static void VecStoreFree(DiskAnnV2VecStore* vs)
{
    if (vs->chunks == NULL) {
        return;
    }
    for (uint32 i = 0; i < vs->nchunks; i++) {
        pfree_ext(vs->chunks[i]);
    }
    pfree(vs->chunks);
    vs->chunks = NULL;
    vs->nchunks = 0;
    vs->count = 0;
}

/* ------------------------------------------------------------ duplicate table */

/* exact-duplicate table: 64-bit hash of the vector bytes -> node id */
typedef struct DiskAnnV2DupTable {
    uint64* hashes;
    uint32* ids; /* DISKANN_V2_INVALID_NODE = empty */
    uint32 mask;
    uint32 count;
} DiskAnnV2DupTable;

static uint64 HashVector(const float* x, int dim)
{
    const uint32* p = (const uint32*)x;
    uint64 h = 0x9E3779B97F4A7C15ull ^ (uint64)dim;
    int nw = dim / 2;
    for (int i = 0; i < nw; i++) {
        uint64 w = ((uint64)p[2 * i + 1] << 32) | p[2 * i];
        h ^= w;
        h *= 0xBF58476D1CE4E5B9ull;
        h ^= h >> DISKANN_V2_HASH_SHIFT_EVEN;
    }
    if (dim & 1) {
        h ^= p[dim - 1];
        h *= 0x94D049BB133111EBull;
        h ^= h >> DISKANN_V2_HASH_SHIFT_ODD;
    }
    return h;
}

static void DupInit(DiskAnnV2DupTable* t, uint32 cap)
{
    t->hashes = (uint64*)palloc_huge(CurrentMemoryContext, sizeof(uint64) * (Size)cap);
    t->ids = (uint32*)palloc_huge(CurrentMemoryContext, sizeof(uint32) * (Size)cap);
    errno_t rc = memset_s(t->ids, sizeof(uint32) * (Size)cap, 0xFF, sizeof(uint32) * (Size)cap);
    if (rc != EOK) {
        securec_check(rc, "\0", "\0");
    }
    t->mask = cap - 1;
    t->count = 0;
}

static void DupGrow(DiskAnnV2DupTable* t)
{
    uint32 oldCap = t->mask + 1;
    uint64* oldHashes = t->hashes;
    uint32* oldIds = t->ids;
    DupInit(t, oldCap * DISKANN_V2_GROW_FACTOR);
    for (uint32 i = 0; i < oldCap; i++) {
        if (oldIds[i] == DISKANN_V2_INVALID_NODE) {
            continue;
        }
        uint32 pos = (uint32)oldHashes[i] & t->mask;
        while (t->ids[pos] != DISKANN_V2_INVALID_NODE) {
            pos = (pos + 1) & t->mask;
        }
        t->hashes[pos] = oldHashes[i];
        t->ids[pos] = oldIds[i];
        t->count++;
    }
    pfree(oldHashes);
    pfree(oldIds);
}

/* ------------------------------------------------------------ build state */

typedef struct DiskAnnV2BuildState {
    Relation heap;
    Relation index;
    IndexInfo* indexInfo;

    int dimIn;
    int dimOut;
    int lsize; /* Vamana L = index_size */
    int funcType;
    bool usePca;
    uint8 bits;
    FmgrInfo* normprocinfo;
    bool normalize; /* cosine opclass: vectors are indexed normalized */

    /* scan */
    uint32 nnodes;
    double reltuples; /* indexed rows (merged duplicates included) */
    DiskAnnV2VecStore vecs;
    ItemPointerData* tids; /* nodeCap x DISKANN_HEAPTIDS */
    uint8* ntids;          /* nodeCap */
    uint32 nodeCap;
    double* meanAcc; /* dimIn */
    DiskAnnV2DupTable dup;
    MemoryContext rowCtx; /* per-row scratch (detoast / normalize) */

    /* reservoir sample (PCA only) */
    float* samples;
    int sampleCap;
    int sampleCount;
    uint64 seen;

    /* transform: PCA_ORTHOGONAL {mean, M = W * P}, trained once, read-only afterwards */
    VectorTransform* vt;

    /* in-memory code + graph arrays (instance memory) */
    char* codes;
    uint16 codeSlotSize;
    uint32* graph;  /* nnodes x DISKANN_V2_DEGREE */
    uint16* gcount; /* nnodes */
    uint32 frozen;

    float* normBuf; /* dimIn scratch: normalized vector of the row being scanned */

    MemoryContext buildCtx;
} DiskAnnV2BuildState;

/* cosine opclass: out = raw * inv */
static inline void NormalizeVector(const float* raw, float inv, int dim, float* out)
{
    for (int i = 0; i < dim; i++) {
        out[i] = raw[i] * inv;
    }
}

static inline DiskAnnV2CodeSlot* BuildCodeSlot(const DiskAnnV2BuildState* state, uint32 nodeId)
{
    return (DiskAnnV2CodeSlot*)(state->codes + (Size)nodeId * state->codeSlotSize);
}

/* ------------------------------------------------------------------ scan */

static void NodesEnsure(DiskAnnV2BuildState* state, uint32 need)
{
    if (need <= state->nodeCap) {
        return;
    }
    uint32 newCap = Max(need, Max(state->nodeCap * DISKANN_V2_GROW_FACTOR, (uint32)DISKANN_V2_NODES_INITIAL));
    Size tidBytes = sizeof(ItemPointerData) * (Size)newCap * DISKANN_HEAPTIDS;
    if (state->tids == NULL) {
        state->tids = (ItemPointerData*)palloc_huge(state->buildCtx, tidBytes);
        state->ntids = (uint8*)palloc_huge(state->buildCtx, (Size)newCap);
    } else {
        state->tids = (ItemPointerData*)repalloc_huge(state->tids, tidBytes);
        state->ntids = (uint8*)repalloc_huge(state->ntids, (Size)newCap);
    }
    state->nodeCap = newCap;
}

static void SampleVector(DiskAnnV2BuildState* state, const float* x)
{
    state->seen++;
    if (state->sampleCount < state->sampleCap) {
        errno_t rc = memcpy_s(state->samples + (Size)state->sampleCount * state->dimIn,
                              sizeof(float) * (Size)state->dimIn, x, sizeof(float) * (Size)state->dimIn);
        if (rc != EOK) {
            securec_check(rc, "\0", "\0");
        }
        state->sampleCount++;
        return;
    }
    uint64 j = ((((uint64)gs_random()) << DISKANN_V2_RANDOM_HI_SHIFT) | (uint64)gs_random()) % state->seen;
    if (j < (uint64)state->sampleCap) {
        errno_t rc = memcpy_s(state->samples + j * state->dimIn, sizeof(float) * (Size)state->dimIn, x,
                              sizeof(float) * (Size)state->dimIn);
        if (rc != EOK) {
            securec_check(rc, "\0", "\0");
        }
    }
}

/* one indexed row (x = indexed vector): merge into a bitwise-equal node or open a new one */
static void AddRow(DiskAnnV2BuildState* state, ItemPointer tid, const float* x)
{
    uint64 h = HashVector(x, state->dimIn);
    DiskAnnV2DupTable* t = &state->dup;
    uint32 pos = (uint32)h & t->mask;
    while (t->ids[pos] != DISKANN_V2_INVALID_NODE) {
        if (t->hashes[pos] == h &&
            memcmp(VecAt(&state->vecs, t->ids[pos]), x, sizeof(float) * (Size)state->dimIn) == 0) {
            uint32 id = t->ids[pos];
            if (state->ntids[id] < DISKANN_HEAPTIDS) {
                state->tids[(Size)id * DISKANN_HEAPTIDS + state->ntids[id]] = *tid;
                state->ntids[id]++;
                return;
            }
            break; /* node full: a new node takes over this hash slot */
        }
        pos = (pos + 1) & t->mask;
    }

    uint32 nodeId = state->nnodes;
    NodesEnsure(state, nodeId + 1);
    VecStoreAppend(&state->vecs, x);
    state->tids[(Size)nodeId * DISKANN_HEAPTIDS] = *tid;
    state->ntids[nodeId] = 1;
    state->nnodes++;

    bool fresh = (t->ids[pos] == DISKANN_V2_INVALID_NODE);
    t->hashes[pos] = h;
    t->ids[pos] = nodeId;
    if (fresh) {
        t->count++;
        if (t->count * DISKANN_V2_GROW_FACTOR > t->mask + 1) {
            DupGrow(t);
        }
    }
}

static void BuildCallback(Relation index, HeapTuple hup, Datum* values, const bool* isnull, bool tupleIsAlive,
                          void* stateArg)
{
    DiskAnnV2BuildState* state = (DiskAnnV2BuildState*)stateArg;
    ItemPointer tid = &hup->t_self;

    if (isnull[0]) {
        return;
    }

    MemoryContext oldCtx = MemoryContextSwitchTo(state->rowCtx);
    Datum value = PointerGetDatum(PG_DETOAST_DATUM(values[0]));
    Vector* vec = (Vector*)DatumGetPointer(value);
    if (vec->dim != state->dimIn) {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
                        errmsg("diskann: vector dimension %d does not match index dimension %d", vec->dim,
                               state->dimIn)));
    }
    MemoryContextSwitchTo(oldCtx);

    /* cosine opclass: skip zero vectors, index x * (1 / |x|) */
    const float* x = vec->x;
    if (state->normalize) {
        double sq = 0;
        for (int i = 0; i < state->dimIn; i++) {
            sq += (double)vec->x[i] * vec->x[i];
        }
        if (sq <= 0) {
            MemoryContextReset(state->rowCtx);
            return;
        }
        NormalizeVector(vec->x, (float)(1.0 / sqrt(sq)), state->dimIn, state->normBuf);
        x = state->normBuf;
    }

    for (int c = 0; c < state->dimIn; c++) {
        state->meanAcc[c] += x[c];
    }
    if (state->samples != NULL) {
        SampleVector(state, x);
    }
    AddRow(state, tid, x);
    state->reltuples += 1;

    MemoryContextReset(state->rowCtx);
    if (((uint64)state->reltuples & 0xFFFF) == 0) {
        CHECK_FOR_INTERRUPTS();
    }
}

/* ------------------------------------------------------------- training */

/*
 * PCA_ORTHOGONAL transform {mean, M = W * P} through the vectortransformer
 * module. The mean is the population mean of the scan (meanAcc / reltuples);
 * the reservoir sample only feeds the PCA components (dimOut < dimIn).
 */
static void TrainTransform(DiskAnnV2BuildState* state)
{
    int d = state->dimIn;
    float* mean = (float*)palloc0(sizeof(float) * (Size)d);
    if (state->reltuples > 0) {
        for (int c = 0; c < d; c++) {
            mean[c] = (float)(state->meanAcc[c] / state->reltuples);
        }
    }
    Assert(state->usePca || state->dimOut == state->dimIn);
    state->vt = (VectorTransform*)palloc0(sizeof(VectorTransform));
    state->vt->dimIn = d;
    state->vt->dimOut = state->dimOut;
    PcaTrain(state->vt, state->samples, state->sampleCount, mean);
    pfree(mean);
}

/* --------------------------------------------------------------- encode */

/* encode [from, to) and track the vector closest to the mean (entry point) */
typedef struct DiskAnnV2EncodeRange {
    const DiskAnnV2BuildState* state;
    uint32 from;
    uint32 to;
    float* y;
    uint32* bestId;
    float* bestDist;
} DiskAnnV2EncodeRange;

static void EncodeRange(const DiskAnnV2EncodeRange* args)
{
    const DiskAnnV2BuildState* state = args->state;
    for (uint32 i = args->from; i < args->to; i++) {
        const float* x = VecAt(&state->vecs, i);
        VtTransform(state->vt, x, args->y);
        RbqBitsArgs rbq;
        rbq.dim = state->dimOut;
        rbq.bits = state->bits;
        rbq.qb = 0;
        rbq.funcType = state->funcType;
        rbq.dimIn = state->dimIn;
        rbq.y = args->y;
        rbq.x = x;
        rbq.mean = state->vt->mean;
        float xcSqr = ComputeVectorRBQCodeBits(&rbq, BuildCodeSlot(state, i));
        if (xcSqr < *args->bestDist) {
            *args->bestDist = xcSqr;
            *args->bestId = i;
        }
    }
}

static void EncodeAll(DiskAnnV2BuildState* state)
{
    float* y = (float*)palloc(sizeof(float) * (Size)state->dimOut);
    uint32 bestId = 0;
    float bestDist = FLT_MAX;

    for (uint32 start = 0; start < state->nnodes; start += DISKANN_V2_ENCODE_CHUNK) {
        DiskAnnV2EncodeRange range;
        range.state = state;
        range.from = start;
        range.to = Min(start + DISKANN_V2_ENCODE_CHUNK, state->nnodes);
        range.y = y;
        range.bestId = &bestId;
        range.bestDist = &bestDist;
        EncodeRange(&range);
        CHECK_FOR_INTERRUPTS();
    }
    state->frozen = bestId;
    pfree(y);
}

/* ----------------------------------------------------------- region IO */

static void WriteCodeRegion(DiskAnnV2BuildState* state, uint32 slotsPerPage, DiskAnnV2Extent* ext)
{
    if (slotsPerPage == 0) {
        ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED), errmsg("diskann: code slotsPerPage is 0")));
    }
    uint32 n = state->nnodes;
    uint32 npages = (n + slotsPerPage - 1) / slotsPerPage;
    BlockNumber start = InvalidBlockNumber;

    for (uint32 pg = 0; pg < npages; pg++) {
        Buffer buf = DiskAnnV2AppendPage(state->index, MAIN_FORKNUM, DISKANN_V2_PAGE_CODE, InvalidBlockNumber);
        if (pg == 0) {
            start = BufferGetBlockNumber(buf);
        }
        Page page = BufferGetPage(buf);
        uint32 first = pg * slotsPerPage;
        uint32 cnt = Min(slotsPerPage, n - first);
        errno_t rc = memcpy_s((char*)page + DISKANN_V2_PAGE_DATA_OFFSET, DiskAnnV2PageUsable(),
                              state->codes + (Size)first * state->codeSlotSize, (Size)cnt * state->codeSlotSize);
        if (rc != EOK) {
            securec_check(rc, "\0", "\0");
        }
        ((PageHeader)page)->pd_lower = (uint16)(DISKANN_V2_PAGE_DATA_OFFSET + DiskAnnV2PageUsable());
        MarkBufferDirty(buf);
        UnlockReleaseBuffer(buf);
        if ((pg & 0xFFF) == 0) {
            CHECK_FOR_INTERRUPTS();
        }
    }

    ext->startBlk = (npages > 0) ? start : InvalidBlockNumber;
    ext->nblocks = npages;
}

static void WriteGraphRegion(DiskAnnV2BuildState* state, uint32 slotsPerPage, DiskAnnV2Extent* ext)
{
    if (slotsPerPage == 0) {
        ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED), errmsg("diskann: graph slotsPerPage is 0")));
    }
    uint32 n = state->nnodes;
    uint32 npages = (n + slotsPerPage - 1) / slotsPerPage;
    BlockNumber start = InvalidBlockNumber;
    uint32 node = 0;

    for (uint32 pg = 0; pg < npages; pg++) {
        Buffer buf = DiskAnnV2AppendPage(state->index, MAIN_FORKNUM, DISKANN_V2_PAGE_GRAPH, InvalidBlockNumber);
        if (pg == 0) {
            start = BufferGetBlockNumber(buf);
        }
        Page page = BufferGetPage(buf);
        char* dst = (char*)page + DISKANN_V2_PAGE_DATA_OFFSET;
        for (uint32 s = 0; s < slotsPerPage && node < n; s++, node++) {
            DiskAnnV2GraphSlot* slot = (DiskAnnV2GraphSlot*)(dst + (Size)s * DISKANN_V2_GRAPH_SLOT);
            slot->count = state->gcount[node];
            slot->tidCount = state->ntids[node];
            slot->flags = 0;
            errno_t rc = memcpy_s(slot->nexts, sizeof(slot->nexts), state->graph + (Size)node * DISKANN_V2_DEGREE,
                                  sizeof(uint32) * (Size)state->gcount[node]);
            if (rc != EOK) {
                securec_check(rc, "\0", "\0");
            }
            rc = memcpy_s(slot->heaptids, sizeof(slot->heaptids), state->tids + (Size)node * DISKANN_HEAPTIDS,
                          sizeof(ItemPointerData) * (Size)state->ntids[node]);
            if (rc != EOK) {
                securec_check(rc, "\0", "\0");
            }
        }
        ((PageHeader)page)->pd_lower = (uint16)(DISKANN_V2_PAGE_DATA_OFFSET + DiskAnnV2PageUsable());
        MarkBufferDirty(buf);
        UnlockReleaseBuffer(buf);
        if ((pg & 0xFFF) == 0) {
            CHECK_FOR_INTERRUPTS();
        }
    }

    ext->startBlk = (npages > 0) ? start : InvalidBlockNumber;
    ext->nblocks = npages;
}

/* publish the region directory and counters (build time: no WAL, the whole
 * relation is logged with LogNewpageRange afterwards) */
typedef struct DiskAnnV2FinalizeMeta {
    Relation index;
    ForkNumber forkNum;
    uint32 nnodes;
    uint32 frozen;
    const DiskAnnV2Extent* xformExt;
    const DiskAnnV2Extent* codeExt;
    const DiskAnnV2Extent* graphExt;
} DiskAnnV2FinalizeMeta;

static void FinalizeMeta(const DiskAnnV2FinalizeMeta* args)
{
    Buffer buf = ReadBufferExtended(args->index, args->forkNum, DISKANN_METAPAGE_BLKNO, RBM_NORMAL, NULL);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    Page page = BufferGetPage(buf);
    DiskAnnV2MetaPage metap = DiskAnnV2PageGetMeta(page);

    metap->nextNodeId = args->nnodes;
    metap->frozenNodeId = (args->nnodes > 0) ? args->frozen : DISKANN_V2_INVALID_NODE;
    metap->tailNodeStart = args->nnodes;
    metap->tailStart = RelationGetNumberOfBlocksInFork(args->index, args->forkNum);
    metap->tailChunkCount = 0;
    metap->xform = *args->xformExt;
    if (args->codeExt != NULL) {
        metap->code = *args->codeExt;
    }
    if (args->graphExt != NULL) {
        metap->graph = *args->graphExt;
    }

    MarkBufferDirty(buf);
    UnlockReleaseBuffer(buf);
}

/* ---------------------------------------------------------------- entry */

static void InitBuildState(DiskAnnV2BuildState* state, Relation heap, Relation index, IndexInfo* indexInfo)
{
    errno_t rc = memset_s(state, sizeof(DiskAnnV2BuildState), 0, sizeof(DiskAnnV2BuildState));
    if (rc != EOK) {
        securec_check(rc, "\0", "\0");
    }

    state->heap = heap;
    state->index = index;
    state->indexInfo = indexInfo;
    state->frozen = DISKANN_V2_INVALID_NODE;

    const DiskAnnTypeInfo* typeInfo = DiskAnnGetTypeInfo(index);
    if (TupleDescAttr(index->rd_att, 0)->atttypid == VARBITOID) {
        elog(ERROR, "type not supported for diskann index");
    }
    state->dimIn = TupleDescAttr(index->rd_att, 0)->atttypmod;
    if (state->dimIn < 0) {
        elog(ERROR, "column does not have dimensions");
    }
    if (state->dimIn > typeInfo->maxDimensions) {
        elog(ERROR, "column cannot have more than %d dimensions for diskann index", typeInfo->maxDimensions);
    }
    if (state->dimIn < 1) {
        elog(ERROR, "column does not have dimensions");
    }

    DiskAnnOptions* opts = (DiskAnnOptions*)index->rd_options;
    state->lsize = opts ? opts->indexSize : DISKANN_DEFAULT_INDEX_SIZE;

    FmgrInfo* procinfo = index_getprocinfo(index, 1, DISKANN_DISTANCE_PROC);
    state->normprocinfo = DiskAnnOptionalProcInfo(index, DISKANN_NORM_PROC);
    /*
     * Like the version 1 build, the graph is built over exact L2 for every opclass (no
     * normalization for IP); the metric only matters at scan time, where IP
     * uses the ipMu factor written into each code slot (EncodeRange).
     */
    state->funcType = GetFunctionType(procinfo, state->normprocinfo);
    state->normalize = (state->normprocinfo != NULL);

    int pcaDim = DiskAnnGetPcaDim(index);
    state->usePca = (pcaDim != 0);
    state->dimOut = state->usePca ? pcaDim : state->dimIn;
    state->bits = (uint8)DiskAnnGetRabitqBits(index);
    state->codeSlotSize = DiskAnnV2CodeSlotSizeFor(state->dimOut, state->bits);
}

/* instance-memory arrays are not covered by context cleanup: free them by hand */
static void FreeSharedArrays(DiskAnnV2BuildState* state)
{
    VecStoreFree(&state->vecs);
    pfree_ext(state->codes);
    pfree_ext(state->graph);
    pfree_ext(state->gcount);
}

static void PrepareScan(DiskAnnV2BuildState* state)
{
    MemoryContext instCtx = INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE);
    VecStoreInit(&state->vecs, state->dimIn, instCtx);
    if (state->normalize) {
        state->normBuf = (float*)palloc(sizeof(float) * (Size)state->dimIn);
    }
    double estRows = (state->heap != NULL) ? (double)state->heap->rd_rel->reltuples : 0;
    uint32 initialNodes =
        (uint32)Min(Max(estRows, (double)DISKANN_V2_NODES_INITIAL), (double)DISKANN_V2_NODES_INITIAL_MAX);
    NodesEnsure(state, initialNodes);
    uint32 dupCap = DISKANN_V2_DUP_CAP_MIN;
    while (dupCap < initialNodes * DISKANN_V2_GROW_FACTOR && dupCap < DISKANN_V2_DUP_CAP_MAX) {
        dupCap <<= 1;
    }
    DupInit(&state->dup, dupCap);
    state->meanAcc = (double*)palloc0(sizeof(double) * (Size)state->dimIn);
    if (state->usePca) {
        state->sampleCap = DISKANN_V2_SAMPLE_CAP;
        state->samples = (float*)palloc_huge(state->buildCtx, sizeof(float) * (Size)state->sampleCap * state->dimIn);
    }
}

static void EncodeAndWrite(DiskAnnV2BuildState* state, const DiskAnnV2Meta* meta, DiskAnnV2Extent* codeExt,
                           DiskAnnV2Extent* graphExt)
{
    MemoryContext instCtx = INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE);
    uint32 n = state->nnodes;
    Size codeBytes = (Size)n * state->codeSlotSize;
    Size graphBytes = sizeof(uint32) * (Size)n * DISKANN_V2_DEGREE;
    /* codes zeroed: slot padding takes part in the insert-time duplicate comparison */
    state->codes = (char*)palloc0_huge(instCtx, codeBytes);
    state->graph = (uint32*)palloc_huge(instCtx, graphBytes);
    state->gcount = (uint16*)palloc0_huge(instCtx, sizeof(uint16) * (Size)n);

    EncodeAll(state);
    VecStoreFree(&state->vecs);
    WriteCodeRegion(state, meta->codeSlotsPerPage, codeExt);
    WriteGraphRegion(state, meta->graphSlotsPerPage, graphExt);
    FreeSharedArrays(state);
}

/* scan -> train -> encode -> regions; everything after meta page creation */
static double BuildCore(DiskAnnV2BuildState* state, const DiskAnnV2Meta* meta, DiskAnnV2Extent* xformExt,
                        DiskAnnV2Extent* codeExt, DiskAnnV2Extent* graphExt)
{
    PrepareScan(state);

    double heapTuples = 0;
    if (state->heap != NULL) {
        heapTuples = tableam_index_build_scan(state->heap, state->index, state->indexInfo, true, BuildCallback,
                                              (void*)state, NULL);
    }

    TrainTransform(state);
    DiskAnnV2WriteTransform(state->index, MAIN_FORKNUM, state->vt, xformExt);

    if (state->nnodes == 0) {
        FreeSharedArrays(state); /* the vector array is instance memory */
        return heapTuples;
    }

    EncodeAndWrite(state, meta, codeExt, graphExt);
    return heapTuples;
}

static void OpenV2Meta(Relation index, ForkNumber forkNum, const DiskAnnV2BuildState* state)
{
    DiskAnnV2CreateMetaArgs metaArgs;
    metaArgs.dimIn = state->dimIn;
    metaArgs.dimOut = state->dimOut;
    metaArgs.distType = (uint8)state->funcType;
    metaArgs.indexSize = (uint32)state->lsize;
    metaArgs.bits = state->bits;
    DiskAnnV2CreateMetaPage(index, forkNum, &metaArgs);
}

static double GuardedBuildCore(DiskAnnV2BuildState* state, const DiskAnnV2Meta* meta, DiskAnnV2Extent* xformExt,
                               DiskAnnV2Extent* codeExt, DiskAnnV2Extent* graphExt)
{
    DiskAnnV2BuildState* volatile st = state;
    double heapTuples = 0;
    PG_TRY();
    {
        heapTuples = BuildCore(state, meta, xformExt, codeExt, graphExt);
    }
    PG_CATCH();
    {
        FreeSharedArrays(st);
        PG_RE_THROW();
    }
    PG_END_TRY();
    return heapTuples;
}

IndexBuildResult* DiskAnnV2BuildIndex(Relation heap, Relation index, IndexInfo* indexInfo)
{
    DiskAnnV2BuildState state;
    InitBuildState(&state, heap, index, indexInfo);

    state.buildCtx = AllocSetContextCreate(CurrentMemoryContext, "diskann v2 build context", ALLOCSET_DEFAULT_SIZES);
    state.rowCtx = AllocSetContextCreate(state.buildCtx, "diskann v2 build row context", ALLOCSET_DEFAULT_SIZES);
    MemoryContext oldCtx = MemoryContextSwitchTo(state.buildCtx);

    OpenV2Meta(index, MAIN_FORKNUM, &state);
    DiskAnnV2Meta meta;
    DiskAnnV2GetMetaSnapshot(index, &meta);

    DiskAnnV2Extent xformExt = {InvalidBlockNumber, 0};
    DiskAnnV2Extent codeExt = {InvalidBlockNumber, 0};
    DiskAnnV2Extent graphExt = {InvalidBlockNumber, 0};
    double heapTuples = GuardedBuildCore(&state, &meta, &xformExt, &codeExt, &graphExt);

    DiskAnnV2FinalizeMeta fin;
    fin.index = index;
    fin.forkNum = MAIN_FORKNUM;
    fin.nnodes = state.nnodes;
    fin.frozen = state.frozen;
    fin.xformExt = &xformExt;
    fin.codeExt = &codeExt;
    fin.graphExt = &graphExt;
    FinalizeMeta(&fin);

    if (RelationNeedsWAL(index)) {
        LogNewpageRange(index, MAIN_FORKNUM, 0, RelationGetNumberOfBlocksInFork(index, MAIN_FORKNUM), true);
    }

    MemoryContextSwitchTo(oldCtx);
    MemoryContextDelete(state.buildCtx);

    IndexBuildResult* result = (IndexBuildResult*)palloc(sizeof(IndexBuildResult));
    result->heap_tuples = heapTuples;
    result->index_tuples = (double)state.nnodes;
    return result;
}

/*
 * Init fork of an UNLOGGED index: an empty index (mean 0, M = W). A PCA
 * reduction cannot be trained without rows, so it is rejected here.
 */
void DiskAnnV2BuildEmptyIndex(Relation index)
{
    DiskAnnV2BuildState state;
    InitBuildState(&state, NULL, index, NULL);
    if (state.usePca) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
                        errmsg("pca_dim requires at least %d rows for training, 0 available",
                               DISKANN_PCA_MIN_TRAIN_ROWS),
                        errhint("Leave pca_dim at 0 (no reduction) for an empty unlogged table.")));
    }

    state.buildCtx = AllocSetContextCreate(CurrentMemoryContext, "diskann v2 buildempty context",
                                           ALLOCSET_DEFAULT_SIZES);
    MemoryContext oldCtx = MemoryContextSwitchTo(state.buildCtx);

    OpenV2Meta(index, INIT_FORKNUM, &state);
    state.meanAcc = (double*)palloc0(sizeof(double) * (Size)state.dimIn);
    TrainTransform(&state);

    DiskAnnV2Extent xformExt = {InvalidBlockNumber, 0};
    DiskAnnV2WriteTransform(index, INIT_FORKNUM, state.vt, &xformExt);
    DiskAnnV2FinalizeMeta fin;
    fin.index = index;
    fin.forkNum = INIT_FORKNUM;
    fin.nnodes = 0;
    fin.frozen = DISKANN_V2_INVALID_NODE;
    fin.xformExt = &xformExt;
    fin.codeExt = NULL;
    fin.graphExt = NULL;
    FinalizeMeta(&fin);

    MemoryContextSwitchTo(oldCtx);
    MemoryContextDelete(state.buildCtx);
}
