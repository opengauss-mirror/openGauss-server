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
 *          Graph   Vamana graph over EXACT original-space distances built by
 *                  the shared DiskAnnGraph::Link algorithm (diskannutils.cpp)
 *                  through DiskAnnV2MemGraphStore: two rounds over all nodes,
 *                  candidate list L = index_size, alpha = 1.2 RobustPrune over
 *                  the whole visited set to out-degree <= 64, reverse-edge
 *                  InterInsert
 *          Write   transform / code / graph regions sequentially, full-page
 *                  WAL afterwards, meta page (version 2) last
 *        Vector source (GUC diskann_build_in_memory):
 *          on   the heap scan copies every (normalized) vector into a chunked
 *               instance-memory array; encode and graph read the array
 *          off  nothing but the per-node TIDs, norms, codes and adjacency stay
 *               in memory; encode and graph re-read each vector from the heap
 *               through the buffer pool by the node's first TID
 *               (DiskAnnV2HeapVector: ReadBuffer + heap_getattr + detoast,
 *               pin released at once; the cosine opclass rescales the raw
 *               vector by the node's scan-time 1 / |x|, so no norm is
 *               recomputed and the floats equal the scan's). CREATE INDEX
 *               holds ShareLock, so TIDs do not move. The bitwise-duplicate
 *               check on a hash hit also re-reads the candidate node's vector.
 *               The graph search reads one vector per distance; a prune
 *               materializes its pool once (DiskAnnGraphStore::PrefetchPool
 *               -> per-store cache of <= INDEXINGMAXC + degree + 1 vectors)
 *               and pairs it from there.
 *        One DiskAnnGraph (scratch lists, candidate queue) and one store
 *        (generation-stamped visited array, pool cache) serve every Link.
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
#include "knl/knl_session.h"
#include "miscadmin.h"
#include "storage/buf/bufmgr.h"
#include "utils/memutils.h"
#include "access/datavec/utils.h"
#include "access/datavec/vector.h"
#include "access/datavec/diskannv2.h"

#define DISKANN_V2_GRAPH_ROUNDS 2

/* encode work chunk (CHECK_FOR_INTERRUPTS granularity) */
#define DISKANN_V2_ENCODE_CHUNK 256

/* buffer-pool source: per-store cache of the prune pool vectors (PrefetchPool):
 * a Link pool holds <= INDEXINGMAXC + 1 nodes, an InterInsert re-prune pool <= degree + 2 */
#define DISKANN_V2_POOL_CACHE_CAP (INDEXINGMAXC + DISKANN_V2_DEGREE + 1)
#define DISKANN_V2_POOL_CACHE_HASH 2048 /* power of two, >= 2 x CAP */
#define DISKANN_V2_LINK_INTERRUPT_MASK 0x3FF

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

    /* vector source: in-memory array (inMemory) or heap re-read by TID */
    bool inMemory;
    bool normalize;  /* cosine opclass: vectors are indexed normalized */
    AttrNumber attno; /* heap attribute of the indexed column (buffer-pool source) */
    float* cmpBuf;    /* dimIn scratch for the duplicate check (buffer-pool source) */

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

    /* graph construction through DiskAnnGraph::Link */
    double* norms; /* nnodes squared norms for ComputeL2DistanceFast (instance memory) */
    /*
     * cosine opclass: 1 / |raw vector| per node (instance memory, nodeCap), taken
     * during the scan. The scan normalizes with it and the buffer-pool source
     * re-normalizes a re-read raw vector with the very same factor
     * (NormalizeVector), so both sources yield the bitwise-equal vector
     * without recomputing the norm per read. NULL for L2 / IP.
     */
    float* invNorms;
    float* normBuf; /* dimIn scratch: normalized vector of the row being scanned */

    MemoryContext buildCtx;
} DiskAnnV2BuildState;

/* cosine opclass: out = raw * inv, the one arithmetic every vector source uses */
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

/* ------------------------------------------------------ vector source */

/*
 * Buffer-pool source: the vector of node `id` (first TID) into `out`, with the
 * same preprocessing as the heap scan: the raw vector is read back and, for
 * the cosine opclass, scaled by the node's scan-time 1 / |x| (no norm is
 * recomputed per read). Under the ShareLock of CREATE INDEX every TID the
 * scan handed out is still there, so a miss is an error.
 */
static void FetchNodeVector(const DiskAnnV2BuildState* state, uint32 id, float* out)
{
    const ItemPointer tid = &state->tids[(Size)id * DISKANN_HEAPTIDS];
    DiskAnnV2HeapVecArgs hv;
    hv.heap = state->heap;
    hv.tid = tid;
    hv.attno = state->attno;
    hv.normalize = false;
    hv.dim = state->dimIn;
    hv.out = out;
    if (!DiskAnnV2HeapVector(&hv)) {
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                        errmsg("diskann: heap row (%u,%u) of node %u vanished during the index build",
                               ItemPointerGetBlockNumber(tid), ItemPointerGetOffsetNumber(tid), id)));
    }
    if (state->normalize) {
        NormalizeVector(out, state->invNorms[id], state->dimIn, out);
    }
}

/* the vector of node `id`: the array element (in-memory) or a heap re-read into `buf` */
static inline const float* NodeVector(const DiskAnnV2BuildState* state, uint32 id, float* buf)
{
    if (state->inMemory) {
        return VecAt(&state->vecs, id);
    }
    FetchNodeVector(state, id, buf);
    return buf;
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
    MemoryContext instCtx = INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE);
    if (!state->inMemory) {
        /* buffer-pool source: norms are taken during the scan (no array to recompute them from later) */
        Size normBytes = sizeof(double) * (Size)newCap;
        state->norms = (state->norms == NULL) ? (double*)palloc_huge(instCtx, normBytes)
                                              : (double*)repalloc_huge(state->norms, normBytes);
    }
    if (state->normalize) {
        Size invBytes = sizeof(float) * (Size)newCap;
        state->invNorms = (state->invNorms == NULL) ? (float*)palloc_huge(instCtx, invBytes)
                                                    : (float*)repalloc_huge(state->invNorms, invBytes);
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

/* one indexed row (x = indexed vector, inv = 1 / |raw| for cosine):
 * merge into a bitwise-equal node or open a new one */
static void AddRow(DiskAnnV2BuildState* state, ItemPointer tid, const float* x, float inv)
{
    uint64 h = HashVector(x, state->dimIn);
    DiskAnnV2DupTable* t = &state->dup;
    uint32 pos = (uint32)h & t->mask;
    while (t->ids[pos] != DISKANN_V2_INVALID_NODE) {
        /* hash hit is almost always a true duplicate; buffer-pool source re-reads rarely */
        if (t->hashes[pos] == h &&
            memcmp(NodeVector(state, t->ids[pos], state->cmpBuf), x, sizeof(float) * (Size)state->dimIn) == 0) {
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
    if (state->inMemory) {
        VecStoreAppend(&state->vecs, x);
    } else {
        double acc = 0;
        for (int c = 0; c < state->dimIn; c++) {
            acc += (double)x[c] * x[c];
        }
        state->norms[nodeId] = acc;
    }
    if (state->normalize) {
        state->invNorms[nodeId] = inv;
    }
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

    /*
     * cosine opclass: skip zero vectors, index x * (1 / |x|). The factor is kept
     * per node so the buffer-pool source reproduces the same floats from the
     * raw row (FetchNodeVector) without recomputing the norm.
     */
    const float* x = vec->x;
    float inv = 0.0f;
    if (state->normalize) {
        double sq = 0;
        for (int i = 0; i < state->dimIn; i++) {
            sq += (double)vec->x[i] * vec->x[i];
        }
        if (sq <= 0) {
            MemoryContextReset(state->rowCtx);
            return;
        }
        inv = (float)(1.0 / sqrt(sq));
        NormalizeVector(vec->x, inv, state->dimIn, state->normBuf);
        x = state->normBuf;
    }

    for (int c = 0; c < state->dimIn; c++) {
        state->meanAcc[c] += x[c];
    }
    if (state->samples != NULL) {
        SampleVector(state, x);
    }
    AddRow(state, tid, x, inv);
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
    float* xbuf;
    uint32* bestId;
    float* bestDist;
} DiskAnnV2EncodeRange;

static void EncodeRange(const DiskAnnV2EncodeRange* args)
{
    const DiskAnnV2BuildState* state = args->state;
    for (uint32 i = args->from; i < args->to; i++) {
        const float* x = NodeVector(state, i, args->xbuf);
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
    float* xbuf = (float*)palloc(sizeof(float) * (Size)state->dimIn);
    uint32 bestId = 0;
    float bestDist = FLT_MAX;

    for (uint32 start = 0; start < state->nnodes; start += DISKANN_V2_ENCODE_CHUNK) {
        DiskAnnV2EncodeRange range;
        range.state = state;
        range.from = start;
        range.to = Min(start + DISKANN_V2_ENCODE_CHUNK, state->nnodes);
        range.y = y;
        range.xbuf = xbuf;
        range.bestId = &bestId;
        range.bestDist = &bestDist;
        EncodeRange(&range);
        CHECK_FOR_INTERRUPTS();
    }
    state->frozen = bestId;
    pfree(xbuf);
    pfree(y);
}

/* ---------------------------------------------------------------- graph */

static bool GraphContains(const uint32* nbrs, int cnt, uint32 id)
{
    for (int i = 0; i < cnt; i++) {
        if (nbrs[i] == id) {
            return true;
        }
    }
    return false;
}

/* ------------------------------------------------ DiskAnnGraph::Link reuse
 *
 * Storage adapter that lets the shared Vamana algorithm layer (DiskAnnGraph
 * in diskannutils.cpp) build the in-memory v2 graph. Vectors come from the
 * shared chunked array (in-memory source) or from the heap by TID (buffer-pool
 * source): the greedy search re-reads one vector per ComputeDistance, while a
 * prune (RobustPrune over <= INDEXINGMAXC candidates, or the reverse-edge
 * re-prune of InterInsert) first materializes the vectors of its pool once
 * through PrefetchPool into a per-store cache that GetDistance consults, so
 * the O(pool^2) pair distances cost no heap reads. Distances are exact L2 for
 * every opclass (like the version 1 build), adjacency lives in graph/gcount and is
 * snapshotted or replaced as a whole. Edge distances are not stored (v2 slots keep ids only;
 * DiskAnnGraph never reads them back). Bitwise duplicates were merged during
 * the heap scan, so a zero distance here is a distinct node and
 * MergeDuplicate declines.
 */
class DiskAnnV2MemGraphStore : public DiskAnnGraphStore {
public:
    explicit DiskAnnV2MemGraphStore(const DiskAnnV2BuildState* state) : m_state(state)
    {
        m_edgeSize = sizeof(DiskAnnEdgePageData);
        m_visitGen = (uint32*)palloc0(sizeof(uint32) * (Size)Max(state->nnodes, 1u));
        if (!state->inMemory) {
            m_bufA = (float*)palloc(sizeof(float) * (Size)state->dimIn);
            m_bufB = (float*)palloc(sizeof(float) * (Size)state->dimIn);
            m_cacheVecs = (float*)palloc(sizeof(float) * (Size)DISKANN_V2_POOL_CACHE_CAP * (Size)state->dimIn);
            m_cacheKeys = (uint32*)palloc(sizeof(uint32) * (Size)DISKANN_V2_POOL_CACHE_HASH);
            m_cacheSlots = (uint16*)palloc(sizeof(uint16) * (Size)DISKANN_V2_POOL_CACHE_HASH);
            m_cacheGen = (uint32*)palloc0(sizeof(uint32) * (Size)DISKANN_V2_POOL_CACHE_HASH);
        }
    }

    ~DiskAnnV2MemGraphStore() override
    {
        pfree_ext(m_visitGen);
        pfree_ext(m_bufA);
        pfree_ext(m_bufB);
        pfree_ext(m_cacheVecs);
        pfree_ext(m_cacheKeys);
        pfree_ext(m_cacheSlots);
        pfree_ext(m_cacheGen);
    }

    void GetVector(BlockNumber id, float* vec, double* sqrSum, ItemPointerData* hctid) const override
    {
        if (m_state->inMemory) {
            Size bytes = sizeof(float) * (Size)m_state->dimIn;
            errno_t rc = memcpy_s(vec, bytes, VecAt(&m_state->vecs, id), bytes);
            if (rc != EOK) {
                securec_check(rc, "\0", "\0");
            }
        } else {
            FetchNodeVector(m_state, id, vec);
        }
        *sqrSum = m_state->norms[id];
        ItemPointerSetInvalid(hctid);
    }

    float GetDistance(BlockNumber a, BlockNumber b) const override
    {
        if (m_state->inMemory) {
            return ComputeL2DistanceFast(VecAt(&m_state->vecs, a), m_state->norms[a], VecAt(&m_state->vecs, b),
                                         m_state->norms[b], (uint16_t)m_state->dimIn);
        }
        /* prune pairs hit the pool cache; any other pair falls back to a heap read */
        const float* va = CachedVector(a);
        if (va == NULL) {
            FetchNodeVector(m_state, a, m_bufA);
            va = m_bufA;
        }
        const float* vb = CachedVector(b);
        if (vb == NULL) {
            FetchNodeVector(m_state, b, m_bufB);
            vb = m_bufB;
        }
        return ComputeL2DistanceFast(va, m_state->norms[a], vb, m_state->norms[b], (uint16_t)m_state->dimIn);
    }

    /*
     * Buffer-pool source: make the vectors of `location` and every pool member
     * resident in the cache before the prune reads them pairwise. Entries left
     * by earlier prunes stay valid (vectors never change under the ShareLock),
     * so only the missing ones are read; the cache is emptied first when they
     * would not fit. A Link pool (<= INDEXINGMAXC + 1) always fits an empty
     * cache, the InterInsert pools (<= degree + 2) usually fit next to it.
     */
    void PrefetchPool(BlockNumber location, const VectorList<Neighbor>* pool) override
    {
        if (m_state->inMemory) {
            return;
        }
        uint32 missing = (CachedVector(location) == NULL) ? 1 : 0;
        for (size_t i = 0; i < pool->size(); i++) {
            if (CachedVector((*pool)[i].id) == NULL) {
                missing++;
            }
        }
        if (missing == 0) {
            return;
        }
        if (m_cacheCount + missing > DISKANN_V2_POOL_CACHE_CAP) {
            CacheReset();
        }
        CacheInsert(location);
        for (size_t i = 0; i < pool->size(); i++) {
            CacheInsert((*pool)[i].id);
        }
    }

    float ComputeDistance(BlockNumber a, float* vec, double sqrSum) const override
    {
        return ComputeL2DistanceFast(NodeVector(m_state, a, m_bufA), m_state->norms[a], vec, sqrSum,
                                     (uint16_t)m_state->dimIn);
    }

    void GetNeighbors(BlockNumber id, VectorList<Neighbor>* nbrs) override
    {
        uint32 snap[DISKANN_V2_DEGREE];
        int cnt = Snapshot(id, snap);
        nbrs->reset();
        nbrs->reserve(DISKANN_V2_DEGREE);
        for (int i = 0; i < cnt; i++) {
            nbrs->push_back(Neighbor(snap[i], 0.0f));
        }
    }

    void GetEdge(DiskAnnEdgePage edge, BlockNumber id) const override
    {
        edge->type = 0;
        int cnt = Snapshot(id, edge->nexts);
        for (int i = 0; i < cnt; i++) {
            edge->distance[i] = 0.0f;
        }
        edge->count = (uint16)cnt;
    }

    void FlushEdge(DiskAnnEdgePage edge, BlockNumber id, bool building) const override
    {
        int cnt = Min((int)edge->count, DISKANN_V2_DEGREE);
        uint32* nbrs = m_state->graph + (Size)id * DISKANN_V2_DEGREE;
        errno_t rc = memcpy_s(nbrs, sizeof(uint32) * (Size)DISKANN_V2_DEGREE, edge->nexts, sizeof(uint32) * (Size)cnt);
        if (rc != EOK) {
            securec_check(rc, "\0", "\0");
        }
        m_state->gcount[id] = (uint16)cnt;
    }

    bool ContainsNeighbors(BlockNumber src, BlockNumber blk) const override
    {
        const uint32* nbrs = m_state->graph + (Size)src * DISKANN_V2_DEGREE;
        bool found = GraphContains(nbrs, m_state->gcount[src], blk);
        return found;
    }

    bool MergeDuplicate(BlockNumber dst, BlockNumber blk, bool building) override
    {
        return false;
    }

    uint32 MaxDegree() const override
    {
        return DISKANN_V2_DEGREE;
    }

    int GetFuncType() const override
    {
        return DISKANN_DIS_L2;
    }

    /*
     * RobustPrune over the whole visited set (Vamana paper): with the fixed
     * out-degree of this format the expanded-nodes pool alone leaves the graph
     * too sparse, the visited pool reaches the target degree at L = index_size
     * without saturating the lists.
     */
    bool PruneOverVisited() const override
    {
        return true;
    }

    /*
     * Visited set: node ids are dense, so one generation stamp per node
     * stands in for the hash table of the page store; Reset only bumps the stamp.
     */
    void VisitedReset(long nelemHint) override
    {
        if (++m_visitCur == 0) {
            /* stamp wrapped: stale entries could look current, wipe them */
            errno_t rc = memset_s(m_visitGen, sizeof(uint32) * (Size)m_state->nnodes, 0,
                                  sizeof(uint32) * (Size)m_state->nnodes);
            if (rc != EOK) {
                securec_check(rc, "\0", "\0");
            }
            m_visitCur = 1;
        }
    }

    bool VisitedTestAndSet(BlockNumber id) override
    {
        if (m_visitGen[id] == m_visitCur) {
            return true;
        }
        m_visitGen[id] = m_visitCur;
        return false;
    }

    void VisitedRelease() override
    {}

private:
    int Snapshot(uint32 id, uint32* out) const
    {
        const uint32* nbrs = m_state->graph + (Size)id * DISKANN_V2_DEGREE;
        int cnt = m_state->gcount[id];
        errno_t rc = memcpy_s(out, sizeof(uint32) * (Size)DISKANN_V2_DEGREE, nbrs, sizeof(uint32) * (Size)cnt);
        if (rc != EOK) {
            securec_check(rc, "\0", "\0");
        }
        return cnt;
    }

    /*
     * Pool cache (buffer-pool source): open-addressing hash node id -> slot of
     * m_cacheVecs. Entries are valid while their generation stamp equals
     * m_cacheGenCur; CacheReset only bumps the stamp.
     */
    static inline uint32 CacheHash(uint32 id)
    {
        return (id * 2654435761u) & (DISKANN_V2_POOL_CACHE_HASH - 1);
    }

    const float* CachedVector(uint32 id) const
    {
        uint32 pos = CacheHash(id);
        while (m_cacheGen[pos] == m_cacheGenCur) {
            if (m_cacheKeys[pos] == id) {
                return m_cacheVecs + (Size)m_cacheSlots[pos] * m_state->dimIn;
            }
            pos = (pos + 1) & (DISKANN_V2_POOL_CACHE_HASH - 1);
        }
        return NULL;
    }

    void CacheReset()
    {
        m_cacheCount = 0;
        if (++m_cacheGenCur == 0) {
            /* stamp wrapped: stale entries could look current, wipe them */
            errno_t rc = memset_s(m_cacheGen, sizeof(uint32) * (Size)DISKANN_V2_POOL_CACHE_HASH, 0,
                                  sizeof(uint32) * (Size)DISKANN_V2_POOL_CACHE_HASH);
            if (rc != EOK) {
                securec_check(rc, "\0", "\0");
            }
            m_cacheGenCur = 1;
        }
    }

    void CacheInsert(uint32 id)
    {
        uint32 pos = CacheHash(id);
        while (m_cacheGen[pos] == m_cacheGenCur) {
            if (m_cacheKeys[pos] == id) {
                return; /* already resident */
            }
            pos = (pos + 1) & (DISKANN_V2_POOL_CACHE_HASH - 1);
        }
        if (m_cacheCount >= DISKANN_V2_POOL_CACHE_CAP) {
            /* PrefetchPool sized the reset so this does not happen; misses would only cost heap reads */
            CacheReset();
            pos = CacheHash(id);
        }
        uint16 slot = (uint16)m_cacheCount++;
        FetchNodeVector(m_state, id, m_cacheVecs + (Size)slot * m_state->dimIn);
        m_cacheGen[pos] = m_cacheGenCur;
        m_cacheKeys[pos] = id;
        m_cacheSlots[pos] = slot;
    }

    const DiskAnnV2BuildState* m_state;
    uint32* m_visitGen = NULL; /* nnodes visited stamps, valid while == m_visitCur */
    uint32 m_visitCur = 1;
    float* m_bufA = NULL; /* buffer-pool source: fetched operands of GetDistance / ComputeDistance */
    float* m_bufB = NULL;
    /* buffer-pool source: vectors of the current prune pool(s), see PrefetchPool */
    float* m_cacheVecs = NULL;   /* DISKANN_V2_POOL_CACHE_CAP x dimIn */
    uint32* m_cacheKeys = NULL;  /* DISKANN_V2_POOL_CACHE_HASH */
    uint16* m_cacheSlots = NULL; /* DISKANN_V2_POOL_CACHE_HASH */
    uint32* m_cacheGen = NULL;   /* DISKANN_V2_POOL_CACHE_HASH */
    uint32 m_cacheGenCur = 1;
    uint32 m_cacheCount = 0;
};

/* squared L2 norms for ComputeL2DistanceFast; buffer-pool source takes them during the scan */
static void ComputeNorms(DiskAnnV2BuildState* state)
{
    if (!state->inMemory) {
        return;
    }
    MemoryContext instCtx = INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE);
    uint32 n = state->nnodes;
    state->norms = (double*)palloc_huge(instCtx, sizeof(double) * (Size)n);
    for (uint32 i = 0; i < n; i++) {
        const float* x = VecAt(&state->vecs, i);
        double acc = 0;
        for (int c = 0; c < state->dimIn; c++) {
            acc += (double)x[c] * x[c];
        }
        state->norms[i] = acc;
        if ((i & 0xFFFF) == 0) {
            CHECK_FOR_INTERRUPTS();
        }
    }
}

/*
 * One Link per node of [from, to). Graph object and store outlive the range:
 * Link resets the visited set and reuses the scratch lists, so no per-node
 * allocation happens here.
 */
static void LinkRange(DiskAnnGraph* graph, const DiskAnnV2BuildState* state, uint32 from, uint32 to)
{
    for (uint32 i = from; i < to; i++) {
        if (i == state->frozen) {
            continue;
        }
        graph->Link(i, state->lsize, true);
        if ((i & DISKANN_V2_LINK_INTERRUPT_MASK) == 0) {
            CHECK_FOR_INTERRUPTS();
        }
    }
}

static void DiskAnnV2CheckPoolCacheLayout(void)
{
    StaticAssertStmt(DISKANN_V2_POOL_CACHE_HASH >= DISKANN_V2_GROW_FACTOR * DISKANN_V2_POOL_CACHE_CAP,
                     "pool cache hash too small");
    StaticAssertStmt(DISKANN_V2_POOL_CACHE_CAP <= PG_UINT16_MAX, "pool cache slots are uint16");
}

/*
 * Serial graph build: two rounds (DiskANN batch-build convention). During
 * round one every node links against a half-built graph; round two re-links
 * every node against the complete graph.
 */
static void GraphLoop(DiskAnnV2BuildState* state)
{
    DiskAnnV2CheckPoolCacheLayout();
    DiskAnnV2MemGraphStore store(state);
    DiskAnnGraph graph(NULL, (double)state->dimIn, state->frozen, &store);
    for (int round = 0; round < DISKANN_V2_GRAPH_ROUNDS; round++) {
        LinkRange(&graph, state, 0, state->nnodes);
    }
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

    /* vector source; the init fork of an unlogged index has no rows and needs neither */
    state->inMemory = u_sess->datavec_ctx.diskann_build_in_memory;
    state->attno = (indexInfo != NULL) ? indexInfo->ii_KeyAttrNumbers[0] : 0;
    if (!state->inMemory && indexInfo != NULL && (state->attno <= 0 || indexInfo->ii_Expressions != NIL)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("diskann buffer-pool build mode does not support expression indexes"),
                        errhint("SET diskann_build_in_memory = on.")));
    }

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
    pfree_ext(state->norms);
    pfree_ext(state->invNorms);
}

static void PrepareScan(DiskAnnV2BuildState* state)
{
    MemoryContext instCtx = INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE);

    /* scan: vectors into memory (in-memory source), TIDs + norms per node, running mean, optional sample */
    if (state->inMemory) {
        VecStoreInit(&state->vecs, state->dimIn, instCtx);
    } else {
        state->cmpBuf = (float*)palloc(sizeof(float) * (Size)state->dimIn);
    }
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

    ComputeNorms(state);
    EncodeAll(state);
    GraphLoop(state);

    uint64 sumOut = 0;
    for (uint32 i = 0; i < n; i++) {
        sumOut += state->gcount[i];
    }
    ereport(LOG, (errmsg("diskann: rabitq index \"%s\" graph built, %u nodes (%.0f rows), dim %d -> %d, %u bit, "
                         "avg out-degree %.1f, entry %u, L=%d, vectors %s",
                         RelationGetRelationName(state->index), n, state->reltuples, state->dimIn, state->dimOut,
                         (unsigned)state->bits, (double)sumOut / n, state->frozen, state->lsize,
                         state->inMemory ? "in memory" : "from the buffer pool")));

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
