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
 * diskannv2insert.cpp
 *
 *        DiskANN RaBitQ format (version 2) INSERT: preprocess -> transform +
 *        encode -> greedy search on code estimates (L = index_size) ->
 *        duplicate merge -> allocate nodeId (tail grows by 1024-node chunks)
 *        -> write code slot -> alpha-prune the pool -> write own graph slot
 *        -> backfill reverse edges one neighbor page at a time; every page
 *        change goes through GenericXLog. A node becomes reachable only when
 *        a neighbor publishes an edge to it, so a crash between the slot
 *        writes leaves an unreachable ghost, never a dangling edge.
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/datavec/diskannv2insert.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include <cmath>

#include "access/genam.h"
#include "access/generic_xlog.h"
#include "miscadmin.h"
#include "storage/buf/bufmgr.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "access/datavec/utils.h"
#include "access/datavec/vector.h"
#include "access/datavec/diskannv2.h"

/* prune states */
#define INS_CAND 0
#define INS_SELECTED 1
#define INS_PRUNED 2
/* alpha passes of the insert-time prune (1.0 then 1.2, as in the build) */
#define INS_PRUNE_PASSES 2

typedef struct DiskAnnV2InsertCtx {
    Relation index;
    Relation heap;
    DiskAnnV2Meta meta;
    bool normalize;
    AttrNumber heapAttno;
    Oid collation;
} DiskAnnV2InsertCtx;

/* per-insert working set handed between the phases of DiskAnnV2Insert */
typedef struct DiskAnnV2InsertWork {
    Vector* vec;                /* preprocessed input vector (dimIn) */
    float* yq;                  /* transformed vector (dimOut) */
    DiskAnnV2CodeSlot* newCode; /* its code slot */
    DiskAnnV2SearchCand* cands; /* greedy-search pool, estimated distance ascending */
    int csize;
    char* poolCodes; /* code slots of the pool */
    DiskAnnV2HitList hits;
    ItemPointer tid;
} DiskAnnV2InsertWork;

/* pool being alpha-pruned */
typedef struct InsPruneCtx {
    const DiskAnnV2Meta* meta;
    const DiskAnnV2SearchCand* pool;
    const char* poolCodes;
    int poolSize;
    uint8* stateArr;
} InsPruneCtx;

static inline DiskAnnV2CodeSlot* PoolCode(const DiskAnnV2Meta* meta, char* poolCodes, int i)
{
    return (DiskAnnV2CodeSlot*)(poolCodes + (Size)i * meta->codeSlotSize);
}

static inline const DiskAnnV2CodeSlot* PoolCodeConst(const DiskAnnV2Meta* meta, const char* poolCodes, int i)
{
    return (const DiskAnnV2CodeSlot*)(poolCodes + (Size)i * meta->codeSlotSize);
}

static inline DiskAnnV2GraphSlot* GraphSlotOnPage(Page page, const DiskAnnV2Meta* meta, uint16 slotNo)
{
    return (DiskAnnV2GraphSlot*)((char*)page + DiskAnnV2SlotOffset(meta->graphSlotSize, slotNo));
}

/* ------------------------------------------------------------ pruning */

/* mark every later candidate that the just-selected pool[i] occludes under alpha */
static void InsOccludeAfter(const InsPruneCtx* pc, int i, float alpha)
{
    const DiskAnnV2Meta* meta = pc->meta;
    const DiskAnnV2CodeSlot* ci = PoolCodeConst(meta, pc->poolCodes, i);
    for (int j = i + 1; j < pc->poolSize; j++) {
        if (pc->stateArr[j] != INS_CAND) {
            continue;
        }
        const DiskAnnV2CodeSlot* cj = PoolCodeConst(meta, pc->poolCodes, j);
        if (alpha * ComputeRbqCodeDistanceBits(meta->dimOut, meta->rabitqBits, ci, cj) <= pc->pool[j].dist) {
            pc->stateArr[j] = INS_PRUNED;
        }
    }
}

/*
 * Alpha-prune a pool sorted by estimated distance ascending into <= R diverse
 * neighbors. Two passes (alpha 1.0 then 1.2) as in the build; a pruned
 * candidate gets another chance under the larger alpha.
 */
static int InsPrune(const DiskAnnV2Meta* meta, const DiskAnnV2SearchCand* pool, int poolSize, const char* poolCodes,
                    uint32* out)
{
    const float alphas[INS_PRUNE_PASSES] = {1.0f, 1.2f};
    InsPruneCtx pc;
    pc.meta = meta;
    pc.pool = pool;
    pc.poolCodes = poolCodes;
    pc.poolSize = poolSize;
    pc.stateArr = (uint8*)palloc0((Size)Max(poolSize, 1));
    int selected = 0;

    for (int pass = 0; pass < INS_PRUNE_PASSES && selected < DISKANN_V2_DEGREE; pass++) {
        for (int i = 0; i < poolSize; i++) {
            if (pc.stateArr[i] == INS_PRUNED) {
                pc.stateArr[i] = INS_CAND;
            }
        }
        for (int i = 0; i < poolSize && selected < DISKANN_V2_DEGREE; i++) {
            if (pc.stateArr[i] != INS_CAND) {
                continue;
            }
            pc.stateArr[i] = INS_SELECTED;
            out[selected++] = pool[i].id;
            InsOccludeAfter(&pc, i, alphas[pass]);
        }
    }
    pfree(pc.stateArr);
    return selected;
}

/* sort (pool, codes) by dist ascending in place; tiny pool, insertion sort with a one-slot scratch */
static void InsSortPool(const DiskAnnV2Meta* meta, DiskAnnV2SearchCand* pool, char* codes, int n)
{
    Size slot = meta->codeSlotSize;
    char* held = (char*)palloc(slot);
    errno_t rc;

    for (int i = 1; i < n; i++) {
        DiskAnnV2SearchCand key = pool[i];
        rc = memcpy_s(held, slot, PoolCode(meta, codes, i), slot);
        if (rc != EOK) {
            securec_check(rc, "\0", "\0");
        }
        int j = i - 1;
        while (j >= 0 && pool[j].dist > key.dist) {
            pool[j + 1] = pool[j];
            rc = memcpy_s(PoolCode(meta, codes, j + 1), slot, PoolCode(meta, codes, j), slot);
            if (rc != EOK) {
                securec_check(rc, "\0", "\0");
            }
            j--;
        }
        pool[j + 1] = key;
        rc = memcpy_s(PoolCode(meta, codes, j + 1), slot, held, slot);
        if (rc != EOK) {
            securec_check(rc, "\0", "\0");
        }
    }
    pfree(held);
}

/* ------------------------------------------------------------ slot writes */

static void InsWriteCodeSlot(const DiskAnnV2InsertCtx* ctx, uint32 nodeId, const DiskAnnV2CodeSlot* code)
{
    const DiskAnnV2Meta* meta = &ctx->meta;
    BlockNumber blkno;
    uint16 slotNo;
    DiskAnnV2ResolveCodeSlot(meta, nodeId, &blkno, &slotNo);

    Buffer buf = ReadBuffer(ctx->index, blkno);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    GenericXLogState* state = GenericXLogStart(ctx->index);
    Page page = GenericXLogRegisterBuffer(state, buf, 0);
    errno_t rc = memcpy_s((char*)page + DiskAnnV2SlotOffset(meta->codeSlotSize, slotNo), meta->codeSlotSize, code,
                          meta->codeSlotSize);
    if (rc != EOK) {
        securec_check(rc, "\0", "\0");
    }
    GenericXLogFinish(state);
    UnlockReleaseBuffer(buf);
}

/*
 * Publish the new node's own slot: adjacency + its single heap TID. Concurrent
 * inserters may already have backfilled edges into this (still zeroed) slot;
 * those are kept behind the selected neighbors.
 */
static void InsWriteGraphSlot(const DiskAnnV2InsertCtx* ctx, uint32 nodeId, const uint32* ids, int count,
                              ItemPointer tid)
{
    const DiskAnnV2Meta* meta = &ctx->meta;
    BlockNumber blkno;
    uint16 slotNo;
    DiskAnnV2ResolveGraphSlot(meta, nodeId, &blkno, &slotNo);

    Buffer buf = ReadBuffer(ctx->index, blkno);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    GenericXLogState* state = GenericXLogStart(ctx->index);
    Page page = GenericXLogRegisterBuffer(state, buf, 0);
    DiskAnnV2GraphSlot* slot = GraphSlotOnPage(page, meta, slotNo);

    uint32 prevNexts[DISKANN_V2_DEGREE];
    int prev = Min((int)slot->count, DISKANN_V2_DEGREE);
    if (prev > 0) {
        errno_t rc = memcpy_s(prevNexts, sizeof(prevNexts), slot->nexts, sizeof(uint32) * (Size)prev);
        if (rc != EOK) {
            securec_check(rc, "\0", "\0");
        }
    }

    errno_t rc = memset_s(slot, sizeof(DiskAnnV2GraphSlot), 0, sizeof(DiskAnnV2GraphSlot));
    if (rc != EOK) {
        securec_check(rc, "\0", "\0");
    }
    int n = 0;
    for (int i = 0; i < count && n < DISKANN_V2_DEGREE; i++) {
        slot->nexts[n++] = ids[i];
    }
    for (int i = 0; i < prev && n < DISKANN_V2_DEGREE; i++) {
        bool dup = false;
        for (int k = 0; k < n && !dup; k++) {
            dup = (slot->nexts[k] == prevNexts[i]);
        }
        if (!dup) {
            slot->nexts[n++] = prevNexts[i];
        }
    }
    slot->count = (uint16)n;
    slot->heaptids[0] = *tid;
    slot->tidCount = 1;

    GenericXLogFinish(state);
    UnlockReleaseBuffer(buf);
}

/* append a TID to an existing node; false when the node filled up meanwhile */
static bool InsAppendTid(const DiskAnnV2InsertCtx* ctx, uint32 nodeId, ItemPointer tid)
{
    const DiskAnnV2Meta* meta = &ctx->meta;
    BlockNumber blkno;
    uint16 slotNo;
    DiskAnnV2ResolveGraphSlot(meta, nodeId, &blkno, &slotNo);

    Buffer buf = ReadBuffer(ctx->index, blkno);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    GenericXLogState* state = GenericXLogStart(ctx->index);
    Page page = GenericXLogRegisterBuffer(state, buf, 0);
    DiskAnnV2GraphSlot* slot = GraphSlotOnPage(page, meta, slotNo);

    if (slot->tidCount >= DISKANN_HEAPTIDS) {
        GenericXLogAbort(state);
        UnlockReleaseBuffer(buf);
        return false;
    }
    slot->heaptids[slot->tidCount] = *tid;
    slot->tidCount++;
    GenericXLogFinish(state);
    UnlockReleaseBuffer(buf);
    return true;
}

/*
 * Full neighbor slot: re-prune {existing nexts + newId} relative to the
 * neighbor nbId and rewrite the slot's adjacency (slot lies in a page the
 * caller registered with GenericXLog).
 */
static void InsRepruneFullSlot(const DiskAnnV2InsertCtx* ctx, DiskAnnV2GraphSlot* slot, uint32 nbId, uint32 newId,
                               const DiskAnnV2CodeSlot* newCode)
{
    const DiskAnnV2Meta* meta = &ctx->meta;
    int n = DISKANN_V2_DEGREE + 1;
    DiskAnnV2SearchCand* pool = (DiskAnnV2SearchCand*)palloc(sizeof(DiskAnnV2SearchCand) * (Size)n);
    char* codes = (char*)palloc((Size)n * meta->codeSlotSize);
    DiskAnnV2CodeSlot* nbCode = (DiskAnnV2CodeSlot*)palloc(meta->codeSlotSize);
    DiskAnnV2ReadCodeSlot(ctx->index, meta, nbId, nbCode);

    for (int i = 0; i < DISKANN_V2_DEGREE; i++) {
        DiskAnnV2CodeSlot* ci = PoolCode(meta, codes, i);
        DiskAnnV2ReadCodeSlot(ctx->index, meta, slot->nexts[i], ci);
        pool[i].id = slot->nexts[i];
        pool[i].dist = ComputeRbqCodeDistanceBits(meta->dimOut, meta->rabitqBits, nbCode, ci);
    }
    errno_t rc = memcpy_s(PoolCode(meta, codes, n - 1), meta->codeSlotSize, newCode, meta->codeSlotSize);
    if (rc != EOK) {
        securec_check(rc, "\0", "\0");
    }
    pool[n - 1].id = newId;
    pool[n - 1].dist = ComputeRbqCodeDistanceBits(meta->dimOut, meta->rabitqBits, nbCode, newCode);
    InsSortPool(meta, pool, codes, n);

    uint32 pruned[DISKANN_V2_DEGREE];
    int selected = InsPrune(meta, pool, n, codes, pruned);
    slot->count = (uint16)selected;
    rc = memcpy_s(slot->nexts, sizeof(slot->nexts), pruned, sizeof(uint32) * (Size)selected);
    if (rc != EOK) {
        securec_check(rc, "\0", "\0");
    }

    pfree(pool);
    pfree(codes);
    pfree(nbCode);
}

/*
 * Backfill one neighbor's adjacency with an edge to newId. Appends in place
 * when the slot has room; otherwise re-prunes {existing nexts + newId}
 * relative to the neighbor. Only this one graph page is locked exclusively;
 * code slots are read under share locks (code page writers never wait on
 * graph locks, so the ordering is acyclic).
 */
static void InsBackfillNeighbor(const DiskAnnV2InsertCtx* ctx, uint32 nbId, uint32 newId,
                                const DiskAnnV2CodeSlot* newCode)
{
    const DiskAnnV2Meta* meta = &ctx->meta;
    BlockNumber blkno;
    uint16 slotNo;
    DiskAnnV2ResolveGraphSlot(meta, nbId, &blkno, &slotNo);

    Buffer buf = ReadBuffer(ctx->index, blkno);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    GenericXLogState* state = GenericXLogStart(ctx->index);
    Page page = GenericXLogRegisterBuffer(state, buf, 0);
    DiskAnnV2GraphSlot* slot = GraphSlotOnPage(page, meta, slotNo);

    int cnt = Min((int)slot->count, DISKANN_V2_DEGREE);
    for (int i = 0; i < cnt; i++) {
        if (slot->nexts[i] == newId) {
            GenericXLogAbort(state);
            UnlockReleaseBuffer(buf);
            return;
        }
    }

    if (cnt < DISKANN_V2_DEGREE) {
        slot->nexts[cnt] = newId;
        slot->count = (uint16)(cnt + 1);
    } else {
        InsRepruneFullSlot(ctx, slot, nbId, newId, newCode);
    }
    GenericXLogFinish(state);
    UnlockReleaseBuffer(buf);
}

/* ------------------------------------------------------------ duplicate merge */

/*
 * Fetch the indexed vector behind one heap TID with the same preprocessing the
 * inserted value received, so the comparison is bitwise meaningful. Returns
 * false when no version exists or the value cannot be indexed.
 */
static bool InsHeapVectorPreprocessed(const DiskAnnV2InsertCtx* ctx, ItemPointer tid, Vector* scratch,
                                      const float** outVec)
{
    DiskAnnV2HeapVecArgs hv;
    hv.heap = ctx->heap;
    hv.tid = tid;
    hv.attno = ctx->heapAttno;
    hv.normalize = ctx->normalize;
    hv.dim = ctx->meta.dimIn;
    hv.out = scratch->x;
    if (!DiskAnnV2HeapVector(&hv, SnapshotSelf)) {
        return false;
    }
    *outVec = scratch->x;
    return true;
}

/* is the first readable heap vector behind hit bitwise equal to x? */
static bool InsHitMatchesVector(const DiskAnnV2InsertCtx* ctx, const DiskAnnV2NodeHit* hit, Vector* scratch,
                                const float* x)
{
    Size vecBytes = sizeof(float) * (Size)ctx->meta.dimIn;
    for (int t = 0; t < (int)hit->ntids; t++) {
        const float* hv = NULL;
        if (InsHeapVectorPreprocessed(ctx, const_cast<ItemPointer>(&hit->tids[t]), scratch, &hv)) {
            return memcmp(hv, x, vecBytes) == 0;
        }
    }
    return false;
}

/*
 * Duplicate merge. A live candidate whose code slot is bitwise identical to
 * the new code (a necessary condition for an identical vector: the transform
 * and encoder are deterministic) is merged only when a heap vector behind it
 * is bitwise equal to the preprocessed new vector and the node still has a
 * free TID slot. Tombstones cannot be verified against the heap and are left
 * alone. Returns true when the TID found a home.
 */
static bool InsTryMerge(const DiskAnnV2InsertCtx* ctx, const DiskAnnV2InsertWork* w)
{
    const DiskAnnV2Meta* meta = &ctx->meta;
    Size cmpBytes = DISKANN_V2_CODE_FACTOR_BYTES + DiskAnnV2CodeBytes(meta->dimOut, meta->rabitqBits);
    Vector* scratch = NULL;

    if (ctx->heap == NULL) {
        return false;
    }

    for (int i = 0; i < w->csize; i++) {
        if (w->cands[i].hit < 0) {
            continue;
        }
        if (memcmp(PoolCodeConst(meta, w->poolCodes, i), w->newCode, cmpBytes) != 0) {
            continue;
        }
        const DiskAnnV2NodeHit* hit = &w->hits.items[w->cands[i].hit];
        if (hit->ntids == 0 || hit->ntids >= DISKANN_HEAPTIDS) {
            continue;
        }
        if (scratch == NULL) {
            scratch = InitVector(meta->dimIn);
        }
        if (InsHitMatchesVector(ctx, hit, scratch, w->vec->x) && InsAppendTid(ctx, w->cands[i].id, w->tid)) {
            return true;
        }
    }
    return false;
}

/* ------------------------------------------------------------ entry */

/*
 * Preprocess the inserted value: cosine normalizes and skips zero vectors
 * (same as the build). Returns false when there is nothing to index.
 */
static bool InsPreprocess(const DiskAnnV2InsertCtx* ctx, Datum value, Vector** out)
{
    value = PointerGetDatum(PG_DETOAST_DATUM(value));
    if (ctx->normalize) {
        Vector* raw = (Vector*)DatumGetPointer(value);
        double sq = 0;
        for (int i = 0; i < raw->dim; i++) {
            sq += (double)raw->x[i] * raw->x[i];
        }
        if (sq <= 0) {
            return false;
        }
        value = DirectFunctionCall1Coll(l2_normalize, ctx->collation, value);
    }
    Vector* vec = (Vector*)DatumGetPointer(value);
    if (vec->dim != (int)ctx->meta.dimIn) {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
                        errmsg("diskann: vector dimension %d does not match index dimension %u", vec->dim,
                               (uint32)ctx->meta.dimIn)));
    }
    *out = vec;
    return true;
}

/* transform + encode with the frozen build-time transform (vectortransformer / rabitq modules) */
static void InsEncode(const DiskAnnV2InsertCtx* ctx, DiskAnnV2InsertWork* w)
{
    const DiskAnnV2Meta* meta = &ctx->meta;
    VectorTransform* vt = DiskAnnV2GetCachedTransform(ctx->index, meta);
    w->yq = (float*)palloc(sizeof(float) * (Size)meta->dimOut);
    VtTransform(vt, w->vec->x, w->yq);
    w->newCode = (DiskAnnV2CodeSlot*)palloc0(meta->codeSlotSize);

    RbqBitsArgs codeArgs;
    codeArgs.dim = meta->dimOut;
    codeArgs.bits = meta->rabitqBits;
    codeArgs.qb = 0;
    codeArgs.funcType = meta->distType;
    codeArgs.dimIn = meta->dimIn;
    codeArgs.y = w->yq;
    codeArgs.x = w->vec->x;
    codeArgs.mean = vt->mean;
    (void)ComputeVectorRBQCodeBits(&codeArgs, w->newCode);
}

/*
 * Greedy search for the candidate pool (L = current index_size reloption, so
 * ALTER INDEX takes effect immediately) and read the pool's code slots. The
 * graph is an L2 graph for every opclass: search with the L2 estimate.
 */
static void InsSearchPool(const DiskAnnV2InsertCtx* ctx, DiskAnnV2InsertWork* w)
{
    Relation index = ctx->index;
    const DiskAnnV2Meta* meta = &ctx->meta;
    DiskAnnOptions* opts = (DiskAnnOptions*)index->rd_options;
    int lsize = opts ? opts->indexSize : DISKANN_DEFAULT_INDEX_SIZE;
    lsize = Max(lsize, DISKANN_V2_DEGREE + 1);

    QueryRabitqVector* query = (QueryRabitqVector*)palloc0(rbqQuerySize(meta->dimOut, DISKANN_V2_INSERT_QUERY_BITS));
    RbqBitsArgs queryArgs;
    queryArgs.dim = meta->dimOut;
    queryArgs.bits = 0;
    queryArgs.qb = DISKANN_V2_INSERT_QUERY_BITS;
    queryArgs.funcType = DIS_L2;
    queryArgs.dimIn = 0;
    queryArgs.y = w->yq;
    queryArgs.x = NULL;
    queryArgs.mean = NULL;
    SetRBQQueryBits(&queryArgs, query);

    DiskAnnV2SearchEnv env;
    env.index = index;
    env.meta = meta;
    env.query = query;
    env.queryBits = DISKANN_V2_INSERT_QUERY_BITS;
    env.keyType = DIS_L2;

    w->cands = (DiskAnnV2SearchCand*)palloc(sizeof(DiskAnnV2SearchCand) * (Size)(lsize + 1));
    w->csize = DiskAnnV2GreedySearch(&env, w->cands, lsize, &w->hits);
    w->poolCodes = NULL;
    if (w->csize > 0) {
        w->poolCodes = (char*)palloc((Size)w->csize * meta->codeSlotSize);
        for (int i = 0; i < w->csize; i++) {
            DiskAnnV2ReadCodeSlot(index, meta, w->cands[i].id, PoolCode(meta, w->poolCodes, i));
        }
    }
}

/*
 * Allocate the node, write its code and graph slots and publish it: the
 * pruned pool becomes its adjacency, reverse edges are backfilled one
 * neighbor page at a time. An empty index publishes the node as the entry;
 * if another first inserter won after our snapshot the two are linked both ways.
 */
static void InsPlaceNode(DiskAnnV2InsertCtx* ctx, const DiskAnnV2InsertWork* w)
{
    Relation index = ctx->index;
    DiskAnnV2Meta* meta = &ctx->meta;

    /* grow the tail when the node lies beyond the published capacity */
    uint32 tailChunks = 0;
    uint32 nodeId = DiskAnnV2AllocateNodeId(index, &tailChunks);
    if ((uint64)nodeId >= (uint64)meta->tailNodeStart + (uint64)tailChunks * DISKANN_V2_CHUNK_NODES) {
        DiskAnnV2EnsureNodeCapacity(index, (uint64)nodeId + 1);
    }
    /* re-snapshot: the published chunk count may have changed since the search */
    DiskAnnV2GetMetaSnapshot(index, meta);

    InsWriteCodeSlot(ctx, nodeId, w->newCode);

    uint32 neighbors[DISKANN_V2_DEGREE];
    int nSelected = 0;
    if (w->csize > 0) {
        nSelected = InsPrune(meta, w->cands, w->csize, w->poolCodes, neighbors);
    } else if (meta->frozenNodeId != DISKANN_V2_INVALID_NODE && meta->frozenNodeId != nodeId) {
        /* searched an empty snapshot: hang off the entry so we stay reachable */
        neighbors[nSelected++] = meta->frozenNodeId;
    }
    InsWriteGraphSlot(ctx, nodeId, neighbors, nSelected, w->tid);

    uint32 entry = meta->frozenNodeId;
    if (entry == DISKANN_V2_INVALID_NODE) {
        entry = DiskAnnV2PublishFirstNode(index, nodeId);
    }
    if (entry != nodeId && nSelected == 0) {
        DiskAnnV2CodeSlot* entryCode = (DiskAnnV2CodeSlot*)palloc(meta->codeSlotSize);
        DiskAnnV2ReadCodeSlot(index, meta, entry, entryCode);
        InsBackfillNeighbor(ctx, nodeId, entry, entryCode);
        InsBackfillNeighbor(ctx, entry, nodeId, w->newCode);
        pfree(entryCode);
    }
    for (int i = 0; i < nSelected; i++) {
        InsBackfillNeighbor(ctx, neighbors[i], nodeId, w->newCode);
    }
}

/*
 * aminsert for the RaBitQ format. Returns false (no uniqueness support).
 */
bool DiskAnnV2Insert(Relation index, Datum* values, const bool* isnull, ItemPointer heapTid, Relation heap)
{
    if (isnull[0]) {
        return false;
    }

    DiskAnnV2InsertCtx ctx;
    ctx.index = index;
    ctx.heap = heap;
    DiskAnnV2GetMetaSnapshot(index, &ctx.meta);
    ctx.normalize = (DiskAnnOptionalProcInfo(index, DISKANN_NORM_PROC) != NULL);
    ctx.heapAttno = index->rd_index->indkey.values[0];
    ctx.collation = index->rd_indcollation[0];

    MemoryContext insCtx = AllocSetContextCreate(CurrentMemoryContext, "diskann v2 insert context",
                                                 ALLOCSET_DEFAULT_SIZES);
    MemoryContext oldCtx = MemoryContextSwitchTo(insCtx);

    DiskAnnV2InsertWork w = {0};
    w.tid = heapTid;
    if (InsPreprocess(&ctx, values[0], &w.vec)) {
        InsEncode(&ctx, &w);
        InsSearchPool(&ctx, &w);
        if (!InsTryMerge(&ctx, &w)) {
            InsPlaceNode(&ctx, &w);
        }
    }

    MemoryContextSwitchTo(oldCtx);
    MemoryContextDelete(insCtx);
    return false;
}
