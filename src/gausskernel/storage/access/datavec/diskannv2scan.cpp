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
 * diskannv2scan.cpp
 *
 *        DiskANN RaBitQ format (version 2) index scan: greedy traversal on
 *        code estimates, then exact heap rerank. Extra rows double L.
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/datavec/diskannv2scan.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include <cfloat>
#include <cmath>

#include "access/genam.h"
#include "access/relscan.h"
#include "knl/knl_session.h"
#include "miscadmin.h"
#include "storage/buf/bufmgr.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "access/datavec/utils.h"
#include "access/datavec/vector.h"
#include "access/datavec/diskannv2.h"

/* code pages pinned together while their slots are prefetched */
#define DISKANN_V2_CODE_READ_WINDOW 16

void DiskAnnV2VisitedReset(DiskAnnV2Visited* vt)
{
    uint32 cap = vt->mask + 1;
    if (cap > vt->baseCap * DISKANN_V2_VISITED_SHRINK_MULT) {
        pfree(vt->slots);
        DiskAnnV2VisitedAlloc(vt, vt->baseCap);
        return;
    }
    errno_t rc = memset_s(vt->slots, sizeof(uint32) * (Size)cap, 0xFF, sizeof(uint32) * (Size)cap);
    if (rc != EOK) {
        securec_check(rc, "\0", "\0");
    }
    vt->count = 0;
}

void DiskAnnV2VisitedGrow(DiskAnnV2Visited* vt)
{
    uint32 oldCap = vt->mask + 1;
    uint32* oldSlots = vt->slots;
    DiskAnnV2VisitedAlloc(vt, oldCap * DISKANN_V2_VISITED_GROW);
    for (uint32 i = 0; i < oldCap; i++) {
        if (oldSlots[i] == DISKANN_V2_INVALID_NODE) {
            continue;
        }
        uint32 pos = DiskAnnV2VisitedHash(oldSlots[i], vt->mask);
        while (vt->slots[pos] != DISKANN_V2_INVALID_NODE) {
            pos = (pos + 1) & vt->mask;
        }
        vt->slots[pos] = oldSlots[i];
        vt->count++;
    }
    pfree(oldSlots);
}

/* returns true when `id` was already present; inserts it otherwise */
bool DiskAnnV2VisitedTestAndSet(DiskAnnV2Visited* vt, uint32 id)
{
    uint32 pos = DiskAnnV2VisitedHash(id, vt->mask);
    while (vt->slots[pos] != DISKANN_V2_INVALID_NODE) {
        if (vt->slots[pos] == id) {
            return true;
        }
        pos = (pos + 1) & vt->mask;
    }
    if (vt->count >= vt->limit) {
        DiskAnnV2VisitedGrow(vt);
        return DiskAnnV2VisitedTestAndSet(vt, id);
    }
    vt->slots[pos] = id;
    vt->count++;
    return false;
}

/* ------------------------------------------------------------ scan state */

typedef struct DiskAnnV2ScanResult {
    ItemPointerData tid;
    float dist; /* exact distance from the opclass support function */
} DiskAnnV2ScanResult;

typedef struct DiskAnnV2ScanOpaqueData {
    uint32 formatVersion; /* DISKANN_VERSION_V2, must stay first (DiskAnnScanHeader) */
    MemoryContext ctx;
    Relation index;

    DiskAnnV2Meta meta;
    bool ready; /* the index holds nodes and an entry point */
    FmgrInfo* procinfo;     /* DISKANN_DISTANCE_PROC */
    FmgrInfo* normprocinfo; /* DISKANN_NORM_PROC, cosine only */
    Oid collation;
    AttrNumber heapAttno;

    /* per-search query state (scratch sized once from the meta snapshot) */
    Vector* qvec;   /* preprocessed query in the original space */
    float* yq;      /* dimOut: transformed query */
    float* heapVec; /* dimIn scratch for reranking */
    Vector* heapVecDatum;
    bool queryValid; /* false: NULL / zero-length query, nothing to return */

    /* version 1 scan protocol: doubled candidate list on every re-search */
    int lsize;
    int iter;
    bool exhausted; /* last pool did not fill up: the graph has nothing more to offer */
    bool searched;

    /* exact-ordered results; [0, curpos) have been returned */
    DiskAnnV2ScanResult* results;
    int nresults;
    int curpos;
} DiskAnnV2ScanOpaqueData;
typedef DiskAnnV2ScanOpaqueData* DiskAnnV2ScanOpaque;

/* PCA_ORTHOGONAL transform cached in rd_amcache (rd_indexcxt); reload on invalidation */
VectorTransform* DiskAnnV2GetCachedTransform(Relation index, const DiskAnnV2Meta* meta)
{
    VectorTransform* vt = (VectorTransform*)index->rd_amcache;

    if (vt == NULL || vt->type != PCA_ORTHOGONAL || vt->dimIn != meta->dimIn || vt->dimOut != meta->dimOut) {
        if (index->rd_amcache != NULL) {
            pfree(index->rd_amcache);
            index->rd_amcache = NULL;
        }
        MemoryContext oldCtx = MemoryContextSwitchTo(index->rd_indexcxt);
        vt = DiskAnnV2LoadTransform(index, meta);
        MemoryContextSwitchTo(oldCtx);
        index->rd_amcache = vt;
    }
    return vt;
}

/* ------------------------------------------------------------ query prep */

/* estimated ordering key of one code slot (ComputeRbqDistanceBits) */
static float EstimateKey(const DiskAnnV2SearchEnv* env, const DiskAnnV2CodeSlot* slot)
{
    RbqBitsArgs args;
    args.dim = env->meta->dimOut;
    args.bits = env->meta->rabitqBits;
    args.qb = env->queryBits;
    args.funcType = env->keyType;
    args.dimIn = 0;
    args.y = NULL;
    args.x = NULL;
    args.mean = NULL;
    return ComputeRbqDistanceBits(&args, slot, env->query);
}

/* fill so->qvec / so->yq; false when the query cannot order anything */
static bool PrepareQuery(IndexScanDesc scan, DiskAnnV2ScanOpaque so)
{
    const DiskAnnV2Meta* meta = &so->meta;

    if (scan->orderByData == NULL) {
        elog(ERROR, "cannot scan diskann index without order");
    }
    if (scan->orderByData->sk_flags & SK_ISNULL) {
        return false;
    }

    Datum value = PointerGetDatum(PG_DETOAST_DATUM(scan->orderByData->sk_argument));
    Vector* raw = (Vector*)DatumGetPointer(value);
    if (raw->dim != meta->dimIn) {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
                        errmsg("diskann: query dimension %d does not match index dimension %u", raw->dim,
                               (uint32)meta->dimIn)));
    }

    if (so->normprocinfo != NULL) {
        double sq = 0;
        for (int i = 0; i < raw->dim; i++) {
            sq += (double)raw->x[i] * raw->x[i];
        }
        if (sq <= 0) {
            return false; /* a zero vector has no cosine ordering */
        }
        Datum normalized = DirectFunctionCall1Coll(l2_normalize, so->collation, value);
        raw = (Vector*)DatumGetPointer(normalized);
    }

    errno_t rc = memcpy_s(so->qvec->x, sizeof(float) * (Size)meta->dimIn, raw->x, sizeof(float) * (Size)meta->dimIn);
    if (rc != EOK) {
        securec_check(rc, "\0", "\0");
    }

    VectorTransform* vt = DiskAnnV2GetCachedTransform(so->index, meta);
    VtTransform(vt, so->qvec->x, so->yq);
    return true;
}

/* ------------------------------------------------------------ greedy search */

static void SearchCandSortBatch(DiskAnnV2SearchCand* batch, int size)
{
    /* degree <= 64: stable insertion sort keeps adjacency order for ties */
    for (int i = 1; i < size; i++) {
        DiskAnnV2SearchCand key = batch[i];
        int j = i;
        while (j > 0 && key.dist < batch[j - 1].dist) {
            batch[j] = batch[j - 1];
            j--;
        }
        batch[j] = key;
    }
}

typedef struct DiskAnnV2MergeBatch {
    const DiskAnnV2SearchCand* current;
    int currentSize;
    DiskAnnV2SearchCand* batch;
    int batchSize;
    int lsize;
    DiskAnnV2SearchCand* out;
    int* firstUnexpanded;
} DiskAnnV2MergeBatch;

/* merge one expanded node's new neighbors into the sorted top-L pool; existing candidates win ties */
static int SearchCandMergeBatch(const DiskAnnV2MergeBatch* args)
{
    SearchCandSortBatch(args->batch, args->batchSize);

    int ci = 0;
    int bi = 0;
    int nout = 0;
    *args->firstUnexpanded = -1;
    while (nout < args->lsize && (ci < args->currentSize || bi < args->batchSize)) {
        if (bi < args->batchSize && (ci >= args->currentSize || args->batch[bi].dist < args->current[ci].dist)) {
            args->out[nout] = args->batch[bi++];
        } else {
            args->out[nout] = args->current[ci++];
        }
        if (*args->firstUnexpanded < 0 && !args->out[nout].expanded) {
            *args->firstUnexpanded = nout;
        }
        nout++;
    }
    return nout;
}

static int SearchCandFindUnexpanded(const DiskAnnV2SearchCand* cands, int size, int start)
{
    for (int i = start; i < size; i++) {
        if (!cands[i].expanded) {
            return i;
        }
    }
    return -1;
}

/*
 * Price one expanded node's newly discovered neighbors from code pages. A
 * small pin window lets the CPU fetch slot cache lines while the following
 * buffer descriptors are looked up; content locks are taken one page at a
 * time and duplicate blocks inside the window share one pin and lock.
 */
typedef struct DiskAnnV2CodeWindow {
    const DiskAnnV2SearchEnv* env;
    DiskAnnV2SearchCand* batch;
    const BlockNumber* blknos;
    const uint16* slots;
    int base;
    int window;
    Size hotBytes;
} DiskAnnV2CodeWindow;

static int PinCodeWindow(const DiskAnnV2CodeWindow* w, Buffer* buffers, BlockNumber* blocks, uint8* bufferIndex)
{
    int nbufs = 0;
    for (int i = 0; i < w->window; i++) {
        BlockNumber blkno = w->blknos[w->base + i];
        int bi = 0;
        while (bi < nbufs && blocks[bi] != blkno) {
            bi++;
        }
        if (bi == nbufs) {
            blocks[nbufs] = blkno;
            buffers[nbufs] = ReadBuffer(w->env->index, blkno);
            nbufs++;
        }
        bufferIndex[i] = (uint8)bi;
        Page page = BufferGetPage(buffers[bi]);
        const char* code = (const char*)page + DiskAnnV2SlotOffset(w->env->meta->codeSlotSize, w->slots[w->base + i]);
        for (Size off = 0; off < w->hotBytes; off += DISKANN_V2_CACHE_LINE) {
            __builtin_prefetch(code + off, 0, 1);
        }
    }
    return nbufs;
}

static void PriceCodeWindow(const DiskAnnV2CodeWindow* w, Buffer* buffers, const uint8* bufferIndex, int nbufs)
{
    const DiskAnnV2Meta* meta = w->env->meta;
    for (int bi = 0; bi < nbufs; bi++) {
        LockBuffer(buffers[bi], BUFFER_LOCK_SHARE);
        Page page = BufferGetPage(buffers[bi]);
        for (int i = 0; i < w->window; i++) {
            if (bufferIndex[i] != (uint8)bi) {
                continue;
            }
            const DiskAnnV2CodeSlot* code = (const DiskAnnV2CodeSlot*)((char*)page +
                DiskAnnV2SlotOffset(meta->codeSlotSize, w->slots[w->base + i]));
            w->batch[w->base + i].dist = EstimateKey(w->env, code);
        }
        UnlockReleaseBuffer(buffers[bi]);
    }
}

static void CodeDistBatch(const DiskAnnV2SearchEnv* env, DiskAnnV2SearchCand* batch, int count)
{
    const DiskAnnV2Meta* meta = env->meta;
    BlockNumber blknos[DISKANN_V2_DEGREE];
    uint16 slots[DISKANN_V2_DEGREE];

    Assert(count >= 0 && count <= DISKANN_V2_DEGREE);
    for (int i = 0; i < count; i++) {
        DiskAnnV2ResolveCodeSlot(meta, batch[i].id, &blknos[i], &slots[i]);
    }

    Size hotBytes = DISKANN_V2_CODE_FACTOR_BYTES + DiskAnnV2CodeBytes(meta->dimOut, meta->rabitqBits);
    for (int base = 0; base < count; base += DISKANN_V2_CODE_READ_WINDOW) {
        DiskAnnV2CodeWindow win;
        Buffer buffers[DISKANN_V2_CODE_READ_WINDOW];
        BlockNumber blocks[DISKANN_V2_CODE_READ_WINDOW];
        uint8 bufferIndex[DISKANN_V2_CODE_READ_WINDOW];
        win.env = env;
        win.batch = batch;
        win.blknos = blknos;
        win.slots = slots;
        win.base = base;
        win.window = Min(DISKANN_V2_CODE_READ_WINDOW, count - base);
        win.hotBytes = hotBytes;
        int nbufs = PinCodeWindow(&win, buffers, blocks, bufferIndex);
        PriceCodeWindow(&win, buffers, bufferIndex, nbufs);
    }
}

static int HitListAppend(DiskAnnV2HitList* hits, const DiskAnnV2GraphSlot* slot)
{
    if (hits->n == hits->cap) {
        int newCap = Max(hits->cap * DISKANN_V2_HITLIST_GROW, DISKANN_V2_HITLIST_MIN_CAP);
        if (hits->items == NULL) {
            hits->items = (DiskAnnV2NodeHit*)palloc(sizeof(DiskAnnV2NodeHit) * (Size)newCap);
        } else {
            hits->items = (DiskAnnV2NodeHit*)repalloc(hits->items, sizeof(DiskAnnV2NodeHit) * (Size)newCap);
        }
        hits->cap = newCap;
    }
    DiskAnnV2NodeHit* h = &hits->items[hits->n];
    h->ntids = slot->tidCount;
    for (int t = 0; t < (int)slot->tidCount; t++) {
        h->tids[t] = slot->heaptids[t];
    }
    return hits->n++;
}

/* greedy traversal from the frozen entry; nodes past the meta snapshot are skipped */
typedef struct DiskAnnV2GreedyWork {
    const DiskAnnV2SearchEnv* env;
    uint32 limit;
    DiskAnnV2Visited* visited;
    DiskAnnV2GraphSlot* gslot;
    DiskAnnV2SearchCand* batch;
    DiskAnnV2SearchCand* pool;
    DiskAnnV2SearchCand* mergeScratch;
    DiskAnnV2HitList* hits;
    int lsize;
    int csize;
} DiskAnnV2GreedyWork;

static int CollectNeighbors(DiskAnnV2GreedyWork* w)
{
    int discovered = 0;
    int cnt = (int)w->gslot->count;
    for (int k = 0; k < cnt; k++) {
        uint32 nb = w->gslot->nexts[k];
        if (nb >= w->limit) {
            continue; /* allocated after our meta snapshot */
        }
        if (DiskAnnV2VisitedTestAndSet(w->visited, nb)) {
            continue;
        }
        w->batch[discovered].id = nb;
        w->batch[discovered].expanded = false;
        w->batch[discovered].hit = -1;
        discovered++;
    }
    return discovered;
}

static int CompactBatch(const DiskAnnV2GreedyWork* w, int discovered)
{
    int batchSize = 0;
    for (int i = 0; i < discovered; i++) {
        if (w->csize == w->lsize && w->batch[i].dist >= w->pool[w->csize - 1].dist) {
            continue;
        }
        if (batchSize != i) {
            w->batch[batchSize] = w->batch[i];
        }
        batchSize++;
    }
    return batchSize;
}

static int ExpandOnce(DiskAnnV2GreedyWork* w, int next)
{
    w->pool[next].expanded = true;
    DiskAnnV2ReadGraphSlot(w->env->index, w->env->meta, w->pool[next].id, w->gslot);
    w->pool[next].hit = HitListAppend(w->hits, w->gslot);

    int discovered = CollectNeighbors(w);
    CodeDistBatch(w->env, w->batch, discovered);
    int batchSize = CompactBatch(w, discovered);
    if (batchSize == 0) {
        return SearchCandFindUnexpanded(w->pool, w->csize, next + 1);
    }

    int firstUnexpanded = -1;
    DiskAnnV2MergeBatch merge;
    merge.current = w->pool;
    merge.currentSize = w->csize;
    merge.batch = w->batch;
    merge.batchSize = batchSize;
    merge.lsize = w->lsize;
    merge.out = w->mergeScratch;
    merge.firstUnexpanded = &firstUnexpanded;
    w->csize = SearchCandMergeBatch(&merge);

    DiskAnnV2SearchCand* oldPool = w->pool;
    w->pool = w->mergeScratch;
    w->mergeScratch = oldPool;
    return firstUnexpanded;
}

static void SeedEntry(DiskAnnV2GreedyWork* w, DiskAnnV2CodeSlot* slot)
{
    uint32 entry = w->env->meta->frozenNodeId;
    (void)DiskAnnV2VisitedTestAndSet(w->visited, entry);
    DiskAnnV2ReadCodeSlot(w->env->index, w->env->meta, entry, slot);
    w->pool[0].id = entry;
    w->pool[0].dist = EstimateKey(w->env, slot);
    w->pool[0].expanded = false;
    w->pool[0].hit = -1;
    w->csize = 1;
}

static void CopyPoolIfMoved(const DiskAnnV2GreedyWork* w, DiskAnnV2SearchCand* cands, int lsize)
{
    if (w->pool == cands) {
        return;
    }
    errno_t rc = memcpy_s(cands, sizeof(DiskAnnV2SearchCand) * (Size)(lsize + 1), w->pool,
                          sizeof(DiskAnnV2SearchCand) * (Size)w->csize);
    if (rc != EOK) {
        securec_check(rc, "\0", "\0");
    }
}

int DiskAnnV2GreedySearch(const DiskAnnV2SearchEnv* env, DiskAnnV2SearchCand* cands, int lsize,
                          DiskAnnV2HitList* hits)
{
    const DiskAnnV2Meta* meta = env->meta;
    uint32 limit = (uint32)Min((uint64)meta->nextNodeId, DiskAnnV2NodeCapacity(meta));
    if (limit == 0 || meta->frozenNodeId == DISKANN_V2_INVALID_NODE || meta->frozenNodeId >= limit) {
        return 0;
    }

    DiskAnnV2Visited visited;
    DiskAnnV2VisitedInit(&visited, DiskAnnV2VisitedCapacityFor(lsize));
    DiskAnnV2CodeSlot* slot = (DiskAnnV2CodeSlot*)palloc(meta->codeSlotSize);
    DiskAnnV2GraphSlot* gslot = (DiskAnnV2GraphSlot*)palloc(sizeof(DiskAnnV2GraphSlot));
    DiskAnnV2SearchCand* mergeBuffer = (DiskAnnV2SearchCand*)palloc(sizeof(DiskAnnV2SearchCand) * (Size)lsize);
    DiskAnnV2SearchCand batch[DISKANN_V2_DEGREE];

    DiskAnnV2GreedyWork w;
    w.env = env;
    w.limit = limit;
    w.visited = &visited;
    w.gslot = gslot;
    w.batch = batch;
    w.pool = cands;
    w.mergeScratch = mergeBuffer;
    w.hits = hits;
    w.lsize = lsize;
    SeedEntry(&w, slot);

    int next = 0;
    while (next >= 0) {
        next = ExpandOnce(&w, next);
        CHECK_FOR_INTERRUPTS();
    }
    CopyPoolIfMoved(&w, cands, lsize);

    DiskAnnV2VisitedFree(&visited);
    pfree(slot);
    pfree(gslot);
    pfree(mergeBuffer);
    return w.csize;
}

/* ------------------------------------------------------------ exact rerank */

static int CmpScanResultDist(const void* a, const void* b)
{
    const DiskAnnV2ScanResult* ra = (const DiskAnnV2ScanResult*)a;
    const DiskAnnV2ScanResult* rb = (const DiskAnnV2ScanResult*)b;
    if (ra->dist < rb->dist) {
        return -1;
    }
    if (ra->dist > rb->dist) {
        return 1;
    }
    return 0;
}

static int CmpTid(const void* a, const void* b)
{
    return ItemPointerCompare((ItemPointer)a, (ItemPointer)b);
}

static QueryRabitqVector* MakeQueryBits(DiskAnnV2ScanOpaque so, int* queryBitsOut)
{
    const DiskAnnV2Meta* meta = &so->meta;
    int queryBits = Min(Max(u_sess->datavec_ctx.rbq_query_bits, DISKANN_V2_QUERY_BITS_MIN), DISKANN_V2_QUERY_BITS_MAX);
    QueryRabitqVector* query = (QueryRabitqVector*)palloc0(rbqQuerySize(meta->dimOut, queryBits));
    const VectorTransform* vt = DiskAnnV2GetCachedTransform(so->index, meta);
    RbqBitsArgs queryArgs;
    queryArgs.dim = meta->dimOut;
    queryArgs.bits = 0;
    queryArgs.qb = queryBits;
    queryArgs.funcType = meta->distType;
    queryArgs.dimIn = meta->dimIn;
    queryArgs.y = so->yq;
    queryArgs.x = so->qvec->x;
    queryArgs.mean = vt->mean;
    SetRBQQueryBits(&queryArgs, query);
    *queryBitsOut = queryBits;
    return query;
}

static ItemPointerData* CopySeenTids(const DiskAnnV2ScanOpaque so, int nseen)
{
    if (nseen <= 0) {
        return NULL;
    }
    ItemPointerData* seen = (ItemPointerData*)palloc(sizeof(ItemPointerData) * (Size)nseen);
    for (int i = 0; i < nseen; i++) {
        seen[i] = so->results[i].tid;
    }
    qsort(seen, (size_t)nseen, sizeof(ItemPointerData), CmpTid);
    return seen;
}

static bool AllTidsSeen(const DiskAnnV2NodeHit* hit, const ItemPointerData* seen, int nseen)
{
    if (nseen <= 0) {
        return false;
    }
    for (int t = 0; t < (int)hit->ntids; t++) {
        if (bsearch(&hit->tids[t], seen, (size_t)nseen, sizeof(ItemPointerData), CmpTid) == NULL) {
            return false;
        }
    }
    return true;
}

static bool FetchHitVector(IndexScanDesc scan, DiskAnnV2ScanOpaque so, const DiskAnnV2NodeHit* hit)
{
    DiskAnnV2HeapVecArgs hv;
    hv.heap = scan->heapRelation;
    hv.attno = so->heapAttno;
    hv.normalize = (so->normprocinfo != NULL);
    hv.dim = so->meta.dimIn;
    hv.out = so->heapVec;
    for (int t = 0; t < (int)hit->ntids; t++) {
        hv.tid = const_cast<ItemPointer>(&hit->tids[t]);
        if (DiskAnnV2HeapVector(&hv)) {
            return true;
        }
    }
    return false;
}

typedef struct DiskAnnV2AppendTids {
    DiskAnnV2ScanResult* fresh;
    int nfresh;
    const DiskAnnV2NodeHit* hit;
    float dist;
    const ItemPointerData* seen;
    int nseen;
} DiskAnnV2AppendTids;

static int AppendUnseenTids(DiskAnnV2AppendTids* args)
{
    for (int t = 0; t < (int)args->hit->ntids; t++) {
        if (args->nseen > 0 &&
            bsearch(&args->hit->tids[t], args->seen, (size_t)args->nseen, sizeof(ItemPointerData), CmpTid) != NULL) {
            continue;
        }
        args->fresh[args->nfresh].tid = args->hit->tids[t];
        args->fresh[args->nfresh].dist = args->dist;
        args->nfresh++;
    }
    return args->nfresh;
}

typedef struct DiskAnnV2RerankArgs {
    IndexScanDesc scan;
    DiskAnnV2ScanOpaque so;
    const DiskAnnV2SearchCand* cands;
    int csize;
    const DiskAnnV2HitList* hits;
    DiskAnnV2ScanResult* fresh;
    const ItemPointerData* seen;
    int nseen;
} DiskAnnV2RerankArgs;

static int RerankPool(const DiskAnnV2RerankArgs* args)
{
    int nfresh = 0;
    for (int i = 0; i < args->csize; i++) {
        if (args->cands[i].hit < 0) {
            continue;
        }
        const DiskAnnV2NodeHit* hit = &args->hits->items[args->cands[i].hit];
        if (hit->ntids == 0 || AllTidsSeen(hit, args->seen, args->nseen)) {
            continue;
        }
        if (!FetchHitVector(args->scan, args->so, hit)) {
            continue; /* every TID already pruned from the heap */
        }
        float dist = (float)DatumGetFloat8(FunctionCall2Coll(args->so->procinfo, args->so->collation,
                                                             PointerGetDatum(args->so->qvec),
                                                             PointerGetDatum(args->so->heapVecDatum)));
        DiskAnnV2AppendTids append;
        append.fresh = args->fresh;
        append.nfresh = nfresh;
        append.hit = hit;
        append.dist = dist;
        append.seen = args->seen;
        append.nseen = args->nseen;
        nfresh = AppendUnseenTids(&append);
    }
    return nfresh;
}

static void AppendFreshResults(DiskAnnV2ScanOpaque so, const DiskAnnV2ScanResult* fresh, int nfresh)
{
    if (nfresh <= 0) {
        return;
    }
    Size bytes = sizeof(DiskAnnV2ScanResult) * (Size)(so->nresults + nfresh);
    if (so->results == NULL) {
        so->results = (DiskAnnV2ScanResult*)palloc(bytes);
    } else {
        so->results = (DiskAnnV2ScanResult*)repalloc(so->results, bytes);
    }
    errno_t rc = memcpy_s(&so->results[so->nresults], sizeof(DiskAnnV2ScanResult) * (Size)nfresh, fresh,
                          sizeof(DiskAnnV2ScanResult) * (Size)nfresh);
    if (rc != EOK) {
        securec_check(rc, "\0", "\0");
    }
    so->nresults += nfresh;
}

/*
 * One search round: traversal with the current lsize, exact rerank of every
 * live candidate, dedup against results returned by earlier rounds, append
 * in exact order. Runs in so->ctx.
 */
static void SearchRound(IndexScanDesc scan, DiskAnnV2ScanOpaque so)
{
    int queryBits = 0;
    int lsize = so->lsize;

    if (scan->heapRelation == NULL) {
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                        errmsg("diskann: rabitq format index scan needs the heap relation for reranking")));
    }

    QueryRabitqVector* query = MakeQueryBits(so, &queryBits);
    DiskAnnV2SearchEnv env;
    env.index = so->index;
    env.meta = &so->meta;
    env.query = query;
    env.queryBits = queryBits;
    env.keyType = so->meta.distType;

    DiskAnnV2SearchCand* cands = (DiskAnnV2SearchCand*)palloc(sizeof(DiskAnnV2SearchCand) * (Size)(lsize + 1));
    DiskAnnV2HitList hits = {NULL, 0, 0};
    int csize = DiskAnnV2GreedySearch(&env, cands, lsize, &hits);
    so->exhausted = (csize < lsize);

    ItemPointerData* seen = CopySeenTids(so, so->nresults);
    DiskAnnV2ScanResult* fresh =
        (DiskAnnV2ScanResult*)palloc(sizeof(DiskAnnV2ScanResult) * (Size)Max(csize, 1) * DISKANN_HEAPTIDS);
    DiskAnnV2RerankArgs rerank;
    rerank.scan = scan;
    rerank.so = so;
    rerank.cands = cands;
    rerank.csize = csize;
    rerank.hits = &hits;
    rerank.fresh = fresh;
    rerank.seen = seen;
    rerank.nseen = so->nresults;
    int nfresh = RerankPool(&rerank);
    qsort(fresh, (size_t)nfresh, sizeof(DiskAnnV2ScanResult), CmpScanResultDist);
    AppendFreshResults(so, fresh, nfresh);
    pfree(fresh);
    pfree(cands);
    pfree(query);
    if (seen != NULL) {
        pfree(seen);
    }
    if (hits.items != NULL) {
        pfree(hits.items);
    }
}

/* drop every per-query state so the next gettuple searches afresh */
static void ResetSearchState(DiskAnnV2ScanOpaque so)
{
    so->searched = false;
    so->queryValid = false;
    so->exhausted = false;
    so->iter = 0;
    so->lsize = 0;
    so->nresults = 0;
    so->curpos = 0;
    if (so->results != NULL) {
        pfree(so->results);
        so->results = NULL;
    }
}

/* ------------------------------------------------------------ AM interface */

IndexScanDesc DiskAnnV2BeginScan(Relation index, int nkeys, int norderbys)
{
    IndexScanDesc scan = RelationGetIndexScan(index, nkeys, norderbys);

    DiskAnnV2ScanOpaque so = (DiskAnnV2ScanOpaque)palloc0(sizeof(DiskAnnV2ScanOpaqueData));
    so->formatVersion = DISKANN_VERSION_V2;
    so->ctx = AllocSetContextCreate(CurrentMemoryContext, "DiskANN v2 scan context", ALLOCSET_DEFAULT_SIZES);
    MemoryContext oldCtx = MemoryContextSwitchTo(so->ctx);

    so->index = index;
    DiskAnnV2GetMetaSnapshot(index, &so->meta);
    so->ready = (so->meta.nextNodeId > 0) && (so->meta.frozenNodeId != DISKANN_V2_INVALID_NODE);
    so->collation = index->rd_indcollation[0];
    so->heapAttno = index->rd_index->indkey.values[0];
    so->procinfo = index_getprocinfo(index, 1, DISKANN_DISTANCE_PROC);
    so->normprocinfo = DiskAnnOptionalProcInfo(index, DISKANN_NORM_PROC);

    so->qvec = InitVector(so->meta.dimIn);
    so->yq = (float*)palloc(sizeof(float) * (Size)so->meta.dimOut);
    so->heapVecDatum = InitVector(so->meta.dimIn);
    so->heapVec = so->heapVecDatum->x;

    if (so->ready) {
        /* warm the per-backend transform cache while we hold no other resources */
        (void)DiskAnnV2GetCachedTransform(index, &so->meta);
    }
    ResetSearchState(so);

    MemoryContextSwitchTo(oldCtx);
    scan->opaque = so;
    return scan;
}

void DiskAnnV2Rescan(IndexScanDesc scan, ScanKey keys, int nkeys, ScanKey orderbys, int norderbys)
{
    DiskAnnV2ScanOpaque so = (DiskAnnV2ScanOpaque)scan->opaque;

    ResetSearchState(so);

    if (keys != NULL && scan->numberOfKeys > 0) {
        errno_t rc = memmove_s(scan->keyData, scan->numberOfKeys * sizeof(ScanKeyData), keys,
                               scan->numberOfKeys * sizeof(ScanKeyData));
        if (rc != EOK) {
            securec_check(rc, "\0", "\0");
        }
    }
    if (orderbys != NULL && scan->numberOfOrderBys > 0) {
        errno_t rc = memmove_s(scan->orderByData, scan->numberOfOrderBys * sizeof(ScanKeyData), orderbys,
                               scan->numberOfOrderBys * sizeof(ScanKeyData));
        if (rc != EOK) {
            securec_check(rc, "\0", "\0");
        }
    }
}

bool DiskAnnV2GetTuple(IndexScanDesc scan, ScanDirection dir)
{
    DiskAnnV2ScanOpaque so = (DiskAnnV2ScanOpaque)scan->opaque;

    if (dir != ForwardScanDirection) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("diskann: only forward scan direction is supported")));
    }

    MemoryContext oldCtx = MemoryContextSwitchTo(so->ctx);

    if (!so->searched) {
        so->searched = true;
        so->queryValid = so->ready && PrepareQuery(scan, so);
        if (so->queryValid) {
            so->lsize = Max(u_sess->datavec_ctx.diskann_probes, 1);
            SearchRound(scan, so);
            so->iter = 1;
        }
    }

    /* version 1 scan protocol: the pool is drained but more rows are wanted -> double L and search again */
    while (so->curpos >= so->nresults && so->queryValid && !so->exhausted && so->iter < MAX_SEARCH_ITERATION) {
        so->lsize *= SEARCH_DOUBLE;
        SearchRound(scan, so);
        so->iter++;
    }

    MemoryContextSwitchTo(oldCtx);

    if (so->curpos >= so->nresults) {
        return false;
    }
    scan->xs_ctup.t_self = so->results[so->curpos].tid;
    scan->xs_recheck = false;
    so->curpos++;
    return true;
}

void DiskAnnV2EndScan(IndexScanDesc scan)
{
    DiskAnnV2ScanOpaque so = (DiskAnnV2ScanOpaque)scan->opaque;
    MemoryContextDelete(so->ctx);
    pfree(so);
    scan->opaque = NULL;
}
