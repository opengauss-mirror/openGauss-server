/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT license.
 *
 * Portions Copyright (c) 2025 Huawei Technologies Co.,Ltd.
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
 * diskannutils.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/datavec/diskannutils.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <dlfcn.h>
#include <stdlib.h>
#include "postgres.h"
#include "access/datavec/diskann.h"
#include "access/datavec/hnsw.h"
#include "access/generic_xlog.h"

#define INDEXING_ALPHA (1.2)
#define GRAPH_SLACK_FACTOR (1.365f)
#define INIT_INSERT_NODE_SIZE 16
#define DISTANCE_FACTOR 2

static inline uint64 murmurhash64(uint64 data)
{
    uint64 h = data;

    h ^= h >> 33;
    h *= 0xff51afd7ed558ccd;
    h ^= h >> 33;
    h *= 0xc4ceb9fe1a85ec53;
    h ^= h >> 33;

    return h;
}

/* BlockNumber hash table */
static uint32 hash_block(BlockNumber blk)
{
#if SIZEOF_VOID_P == 8
    return murmurhash64((uint64)blk);
#else
    return murmurhash32((uint32)blk);
#endif
}

#define SH_PREFIX blockhash
#define SH_ELEMENT_TYPE BlockNumberHashEntry
#define SH_KEY_TYPE BlockNumber
#define SH_KEY block
#define SH_HASH_KEY(tb, key) hash_block(key)
#define SH_EQUAL(tb, a, b) ((a) == (b))
#define SH_SCOPE extern
#define SH_DEFINE
#include "lib/simplehash.h"

/*
 * Get type info
 */
const DiskAnnTypeInfo* DiskAnnGetTypeInfo(Relation index)
{
    FmgrInfo* procinfo = DiskAnnOptionalProcInfo(index, DISKANN_TYPE_INFO_PROC);

    if (procinfo == NULL) {
        static const DiskAnnTypeInfo typeInfo = {.maxDimensions = DISKANN_MAX_DIM,
                                                 .supportPQ = true,
                                                 .itemSize = VectorItemSize,
                                                 .normalize = l2_normalize,
                                                 .checkValue = NULL};
        return (&typeInfo);
    } else {
        return (const DiskAnnTypeInfo*)DatumGetPointer(OidFunctionCall0Coll(procinfo->fn_oid, InvalidOid));
    }
}

FmgrInfo* DiskAnnOptionalProcInfo(Relation index, uint16 procnum)
{
    if (!OidIsValid(index_getprocid(index, 1, procnum))) {
        return NULL;
    }

    return index_getprocinfo(index, 1, procnum);
}

Datum DiskAnnNormValue(const DiskAnnTypeInfo* typeInfo, Oid collation, Datum value)
{
    return DirectFunctionCall1Coll(typeInfo->normalize, collation, value);
}

bool DiskAnnCheckNorm(FmgrInfo* procinfo, Oid collation, Datum value)
{
    return DatumGetFloat8(FunctionCall1Coll(procinfo, collation, value)) > 0;
}

void DiskAnnUpdateMetaPage(Relation index, BlockNumber blkno, ForkNumber forkNum, bool building)
{
    Buffer buf;
    Page page;
    GenericXLogState *state;
    buf = ReadBuffer(index, DISKANN_METAPAGE_BLKNO);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    if (building) {
        page = BufferGetPage(buf);
    } else {
        state = GenericXLogStart(index);
        page = GenericXLogRegisterBuffer(state, buf, 0);
    }

    DiskAnnMetaPage metaPage = DiskAnnPageGetMeta(page);
    metaPage->insertPage = blkno;
    if (building) {
        MarkBufferDirty(buf);
    } else {
        GenericXLogFinish(state);
    }
    UnlockReleaseBuffer(buf);
}

Buffer DiskAnnNewBuffer(Relation index, ForkNumber forkNum)
{
    Buffer buf = ReadBufferExtended(index, forkNum, P_NEW, RBM_NORMAL, NULL);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    return buf;
}

void DiskAnnInitPage(Page page, Size pagesize)
{
    PageInit(page, pagesize, sizeof(DiskAnnPageOpaqueData));
    DiskAnnPageGetOpaque(page)->nextblkno = InvalidBlockNumber;
    DiskAnnPageGetOpaque(page)->pageId = DISKANN_PAGE_ID;
}

Page DiskAnnInitRegisterPage(Relation index, Buffer buf)
{
    Page page = BufferGetPage(buf);
    DiskAnnInitPage(page, BufferGetPageSize(buf));
    return page;
}

/* magic / version sanity check shared by every meta page reader */
static void DiskAnnCheckMetaHeader(Relation index, uint32 magic, uint32 version)
{
    if (unlikely(magic != DISKANN_MAGIC_NUMBER)) {
        ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                        errmsg("\"%s\" is not a diskann index", RelationGetRelationName(index))));
    }
    if (unlikely(version != DISKANN_VERSION && version != DISKANN_VERSION_V2)) {
        ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                        errmsg("diskann index \"%s\" has unsupported format version %u, REINDEX it",
                               RelationGetRelationName(index), version)));
    }
}

void DiskANNGetMetaPageInfo(Relation index, DiskAnnMetaPage meta)
{
    Buffer buf;
    Page page;
    DiskAnnMetaPage metapage;

    buf = ReadBuffer(index, DISKANN_METAPAGE_BLKNO);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buf);
    Size itemsz = sizeof(DiskAnnMetaPageData);
    metapage = (DiskAnnMetaPage)DiskAnnPageGetMeta(page);
    errno_t rc = memcpy_s(meta, itemsz, metapage, itemsz);
    if (rc != EOK) {
        securec_check(rc, "\0", "\0");
    }
    UnlockReleaseBuffer(buf);
    DiskAnnCheckMetaHeader(index, meta->magicNumber, meta->version);
    return;
}

/*
 * Format version stored in the meta page (1 = page format, 2 = RaBitQ). Callbacks
 * dispatch on this value, never on reloptions: the two may disagree.
 */
uint32 DiskAnnGetFormatVersion(Relation index)
{
    Buffer buf = ReadBuffer(index, DISKANN_METAPAGE_BLKNO);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    Page page = BufferGetPage(buf);
    DiskAnnMetaPage metapage = (DiskAnnMetaPage)DiskAnnPageGetMeta(page);
    uint32 magic = metapage->magicNumber;
    uint32 version = metapage->version;
    UnlockReleaseBuffer(buf);
    DiskAnnCheckMetaHeader(index, magic, version);
    return version;
}

void DiskAnnGraphStore::AddNeighbor(DiskAnnEdgePage edge, BlockNumber id, float distance) const
{
    edge->nexts[edge->count] = id;
    edge->distance[edge->count] = distance;
    edge->count++;
}

bool DiskAnnGraphStore::NeighborExists(const DiskAnnEdgePage edge, BlockNumber id) const
{
    for (uint16_t i = 0; i < edge->count; ++i) {
        if (edge->nexts[i] == id) {
            return true;
        }
    }
    return false;
}

DiskAnnPageGraphStore::DiskAnnPageGraphStore(Relation relation)
{
    m_rel = relation;
    DiskAnnMetaPageData metapage;
    DiskANNGetMetaPageInfo(relation, &metapage);
    m_nodeSize = metapage.nodeSize;
    m_edgeSize = metapage.edgeSize;
    m_itemSize = metapage.itemSize;
    m_dimension = metapage.dimensions;
    m_funcType = GetFunctionType(index_getprocinfo(relation, 1, DISKANN_DISTANCE_PROC),
                                 DiskAnnOptionalProcInfo(relation, DISKANN_NORM_PROC));
}

DiskAnnPageGraphStore::~DiskAnnPageGraphStore()
{
    m_rel = NULL;
}

void DiskAnnPageGraphStore::GetVector(BlockNumber blkno, float* vec, double* sqrSum, ItemPointerData* hctid) const
{
    Buffer buf;
    Page page;
    IndexTuple ctup;
    bool isnull;
    Datum src;
    Datum dst;
    Vector* vector;
    errno_t rc;
    DiskAnnNodePage ntup;

    buf = ReadBuffer(m_rel, blkno);
    LockBuffer(buf, BUFFER_LOCK_SHARE);

    /* get index value from page */
    page = BufferGetPage(buf);
    Assert(PageIsValid(page));
    ctup = (IndexTuple)PageGetItem(page, PageGetItemId(page, FirstOffsetNumber));
    *hctid = ctup->t_tid;

    src = index_getattr(ctup, 1, RelationGetDescr(m_rel), &isnull);
    dst = PointerGetDatum(PG_DETOAST_DATUM(src));

    /* get vector data from index tup */
    vector = (Vector*)DatumGetPointer(dst);
    Assert(m_dimension == vector->dim);
    rc = memcpy_s(vec, (Size)(vector->dim * sizeof(float)), vector->x, (Size)(vector->dim * sizeof(float)));
    if (rc != EOK) {
        securec_check(rc, "\0", "\0");
    }

    /* get sqrSum from node tup */
    ntup = DiskAnnPageGetNode(ctup);
    *sqrSum = ntup->sqrSum;

    if (DatumGetPointer(dst) != DatumGetPointer(src)) {
        pfree(DatumGetPointer(dst));
    }

    UnlockReleaseBuffer(buf);
}

float DiskAnnPageGraphStore::GetDistance(BlockNumber blk1, BlockNumber blk2) const
{
    Buffer buf;
    Page page;
    IndexTuple ctup;
    Vector* vector;
    Datum src;
    Datum dst;
    bool isnull;
    float distance;

    /* Read and lock the second block */
    buf = ReadBuffer(m_rel, blk2);
    LockBuffer(buf, BUFFER_LOCK_SHARE);

    /* Get the vector from the second block */
    page = BufferGetPage(buf);
    ctup = (IndexTuple)PageGetItem(page, PageGetItemId(page, FirstOffsetNumber));

    src = index_getattr(ctup, 1, RelationGetDescr(m_rel), &isnull);
    dst = PointerGetDatum(PG_DETOAST_DATUM(src));
    vector = (Vector*)DatumGetPointer(dst);

    /* Calculate distance */
    distance = ComputeDistance(blk1, vector->x, DiskAnnPageGetNode(ctup)->sqrSum);

    /* Clean up */
    if (DatumGetPointer(dst) != DatumGetPointer(src)) {
        pfree(DatumGetPointer(dst));
    }

    UnlockReleaseBuffer(buf);
    return distance;
}

float DiskAnnPageGraphStore::ComputeDistance(BlockNumber blk1, float* vec2, double sqrSum2) const
{
    Buffer buf;
    Page page;
    IndexTuple ctup;
    Vector* vector1;
    Datum src;
    Datum dst;
    bool isnull;
    float distance;

    /* Read and lock block 1 */
    buf = ReadBuffer(m_rel, blk1);
    LockBuffer(buf, BUFFER_LOCK_SHARE);

    /* Get the vector from block 1 */
    page = BufferGetPage(buf);
    ctup = (IndexTuple)PageGetItem(page, PageGetItemId(page, FirstOffsetNumber));

    src = index_getattr(ctup, 1, RelationGetDescr(m_rel), &isnull);
    dst = PointerGetDatum(PG_DETOAST_DATUM(src));
    vector1 = (Vector*)DatumGetPointer(dst);

    /* Calculate L2 distance */
    distance = ComputeL2DistanceFast(vector1->x, DiskAnnPageGetNode(ctup)->sqrSum, vec2, sqrSum2, vector1->dim);

    /* Clean up */
    if (DatumGetPointer(dst) != DatumGetPointer(src)) {
        pfree(DatumGetPointer(dst));
    }
    UnlockReleaseBuffer(buf);
    return distance;
}

void DiskAnnPageGraphStore::GetNeighbors(BlockNumber blkno, VectorList<Neighbor>* nbrs)
{
    Buffer buf;
    Page page;
    IndexTuple ctup;
    DiskAnnNodePage ntup;
    DiskAnnEdgePage etup;

    /* Reserve space */
    nbrs->reset();
    nbrs->reserve((size_t)(ceil(GRAPH_SLACK_FACTOR * DISKANN_MAX_DEGREE)));

    buf = ReadBuffer(m_rel, blkno);
    LockBuffer(buf, BUFFER_LOCK_SHARE);

    page = BufferGetPage(buf);
    ctup = (IndexTuple)PageGetItem(page, PageGetItemId(page, FirstOffsetNumber));
    ntup = DiskAnnPageGetNode(ctup);
    etup = (DiskAnnEdgePage)((uint8_t*)ntup + m_nodeSize);

    /* Add edge page neighbors info into neighbor's list */
    for (uint16 i = 0; i < etup->count; i++) {
        nbrs->push_back(Neighbor(etup->nexts[i], etup->distance[i]));
    }
    UnlockReleaseBuffer(buf);
}

void DiskAnnPageGraphStore::GetEdge(DiskAnnEdgePage edge, BlockNumber blkno) const
{
    GetEdgeTuple(edge, blkno, m_rel, m_nodeSize, m_edgeSize);
}

void DiskAnnPageGraphStore::FlushEdge(DiskAnnEdgePage edgePage, BlockNumber blk, bool building) const
{
    Buffer buf;
    Page page;
    GenericXLogState *state;
    DiskAnnNodePage ntup;
    DiskAnnEdgePage etup;
    errno_t rc;

    buf = ReadBuffer(m_rel, blk);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

    if (building) {
        page = BufferGetPage(buf);
    } else {
        state = GenericXLogStart(m_rel);
        page = GenericXLogRegisterBuffer(state, buf, 0);
    }
    ntup = DiskAnnPageGetNode(DiskAnnPageGetIndexTuple(page));
    etup = (DiskAnnEdgePage)((uint8_t*)ntup + m_nodeSize);
    rc = memcpy_s(etup, m_edgeSize, edgePage, m_edgeSize);
    if (rc != EOK) {
        securec_check(rc, "\0", "\0");
    }

    if (building) {
        MarkBufferDirty(buf);
    } else {
        GenericXLogFinish(state);
    }
    UnlockReleaseBuffer(buf);
}

bool DiskAnnPageGraphStore::ContainsNeighbors(BlockNumber src, BlockNumber blk) const
{
    Buffer buf;
    Page page;
    IndexTuple ctup;
    DiskAnnNodePage ntup;
    DiskAnnEdgePage etup;
    bool found = false;

    buf = ReadBuffer(m_rel, src);
    LockBuffer(buf, BUFFER_LOCK_SHARE);

    page = BufferGetPage(buf);
    ctup = (IndexTuple)PageGetItem(page, PageGetItemId(page, FirstOffsetNumber));
    ntup = DiskAnnPageGetNode(ctup);
    etup = (DiskAnnEdgePage)((uint8_t*)ntup + m_nodeSize);

    for (uint16 i = 0; i < etup->count; i++) {
        if (etup->nexts[i] == blk) {
            found = true;
            break;
        }
    }

    UnlockReleaseBuffer(buf);
    return found;
}

/* blk holds a vector equal to dst's: record blk's heap TID on dst (blk's page stays an unlinked node) */
bool DiskAnnPageGraphStore::MergeDuplicate(BlockNumber dst, BlockNumber blk, bool building)
{
    Buffer buf = ReadBuffer(m_rel, blk);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    IndexTuple itup = DiskAnnPageGetIndexTuple(BufferGetPage(buf));
    AddDuplicateNeighbor(dst, itup->t_tid, building);
    UnlockReleaseBuffer(buf);
    return true;
}

void DiskAnnPageGraphStore::AddDuplicateNeighbor(BlockNumber src, ItemPointerData tid, bool building)
{
    Buffer buf;
    Page page;
    GenericXLogState *state;
    DiskAnnNodePage ntup;
    bool found = false;

    buf = ReadBuffer(m_rel, src);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

    if (building) {
        page = BufferGetPage(buf);
    } else {
        state = GenericXLogStart(m_rel);
        page = GenericXLogRegisterBuffer(state, buf, 0);
    }
    ntup = DiskAnnPageGetNode(DiskAnnPageGetIndexTuple(page));
    for (int i = 0; i < ntup->heaptidsLength; i++) {
        if (ItemPointerEquals(&ntup->heaptids[i], &tid)) {
            found = true;
            break;
        }
    }

    /* Only add if not a duplicate */
    if (!found) {
        /* Check if there's space */
        if (ntup->heaptidsLength >= DISKANN_HEAPTIDS) {
            elog(WARNING, "Cannot add more neighbors, heaptids array is full");
        } else {
            ntup->heaptids[ntup->heaptidsLength] = tid;
            ntup->heaptidsLength++;
            if (building) {
                MarkBufferDirty(buf);
            } else {
                GenericXLogFinish(state);
            }
        }
    }
    UnlockReleaseBuffer(buf);
}

/* ---- default visited set: one dynahash per Reset (the version 1 per-DiskAnnGraph hash table) */

void DiskAnnGraphStore::VisitedReset(long nelemHint)
{
    VisitedRelease();
    HASHCTL hashCtl;
    errno_t rc = memset_s(&hashCtl, sizeof(HASHCTL), 0, sizeof(HASHCTL));
    if (rc != EOK) {
        securec_check(rc, "\0", "\0");
    }
    hashCtl.keysize = sizeof(uint32_t);
    hashCtl.entrysize = sizeof(uint32_t);
    hashCtl.hcxt = CurrentMemoryContext;
    m_visited = hash_create("DiskANN inserted nodes hash", nelemHint, &hashCtl, HASH_ELEM | HASH_CONTEXT);
}

bool DiskAnnGraphStore::VisitedTestAndSet(BlockNumber id)
{
    bool found = false;
    hash_search(m_visited, &id, HASH_ENTER, &found);
    return found;
}

void DiskAnnGraphStore::VisitedRelease()
{
    if (m_visited != NULL) {
        hash_destroy(m_visited);
        m_visited = NULL;
    }
}

DiskAnnGraph::DiskAnnGraph(Relation rel, double dim, BlockNumber blkno, DiskAnnGraphStore* graphStore)
{
    this->graphStore = graphStore;
    functype = graphStore->GetFuncType();
    maxDegree = graphStore->MaxDegree();
    pruneVisited = graphStore->PruneOverVisited();
    scratch = (QueryScratch*)palloc0(sizeof(QueryScratch));
    scratch->alignedQuery = (float*)palloc(dim * sizeof(float));
    scratch->bestLNodes = (NeighborPriorityQueue*)palloc0(sizeof(NeighborPriorityQueue));
    scratch->bestLNodes->_data = VectorList<Neighbor>();
    scratch->edgePage = (DiskAnnEdgePage)palloc(graphStore->m_edgeSize);
    scratch->pool.initialize_vector();
    scratch->prunedList.initialize_vector();
    scratch->current.initialize_vector();
    scratch->neighbors.initialize_vector();
    scratch->occludeFactor.initialize_vector();
    scratch->desPool.initialize_vector();
    scratch->copyNeighbors.initialize_vector();
    scratch->dummyPool.initialize_vector();
    scratch->pruned.initialize_vector();

    graphStore->VisitedReset(INIT_INSERT_NODE_SIZE);
    visitedFresh = true;
    frozen = blkno;
}

DiskAnnGraph::~DiskAnnGraph()
{
    Clear();
}

void DiskAnnGraph::Clear()
{
    if (scratch != NULL) {
        pfree_ext(scratch->alignedQuery);
        scratch->bestLNodes->_data.clear();
        pfree_ext(scratch->bestLNodes);
        pfree_ext(scratch->edgePage);
        scratch->pool.clear();
        scratch->prunedList.clear();
        scratch->current.clear();
        scratch->neighbors.clear();
        scratch->occludeFactor.clear();
        scratch->desPool.clear();
        scratch->copyNeighbors.clear();
        scratch->dummyPool.clear();
        scratch->pruned.clear();
        graphStore->VisitedRelease();
        pfree_ext(scratch);
    }
    scratch = NULL;
}

void DiskAnnGraph::Link(BlockNumber blk, int index_size, bool building)
{
    VectorList<Neighbor>* pool = &scratch->pool;
    VectorList<Neighbor>* prunedList = &scratch->prunedList;
    ItemPointerData hctid;

    scratch->bestLNodes->clear();
    pool->reset();
    ItemPointerSetInvalid(&hctid);
    /* the visited set is empty right after construction; every later Link starts a new one */
    if (!visitedFresh) {
        graphStore->VisitedReset(INIT_INSERT_NODE_SIZE);
    }
    visitedFresh = false;

    graphStore->GetVector(blk, scratch->alignedQuery, &(scratch->sqrSum), &hctid);

    /* Find and add appropriate graph edges */
    IterateToFixedPoint(blk, index_size, frozen, pool, false);

    if (pruneVisited) {
        /* RobustPrune over V union current out-edges: keep edges the search did not revisit */
        VectorList<Neighbor>* current = &scratch->current;
        graphStore->GetNeighbors(blk, current);
        for (size_t i = 0; i < current->size(); i++) {
            BlockNumber nb = (*current)[i].id;
            if (nb != blk && !graphStore->VisitedTestAndSet(nb)) {
                float dis = graphStore->ComputeDistance(nb, scratch->alignedQuery, scratch->sqrSum);
                pool->push_back(Neighbor(nb, dis));
            }
        }
        current->reset();
    }

    if (FindDuplicateNeighbor(scratch->bestLNodes, blk, building)) {
        scratch->bestLNodes->clear();
        pool->reset();
        return;
    }

    PruneNeighbors(blk, pool, prunedList);
    Assert(prunedList->size() <= maxDegree);

    DiskAnnEdgePage edgePage = scratch->edgePage;
    graphStore->GetEdge(edgePage, blk);
    for (uint16 i = 0; i < edgePage->count; i++) {
        edgePage->nexts[i] = InvalidOffsetNumber;
    }
    edgePage->count = 0;

    for (uint32 i = 0; i < (uint32)prunedList->size(); i++) {
        graphStore->AddNeighbor(edgePage, (*prunedList)[i].id, (*prunedList)[i].distance);
    }
    graphStore->FlushEdge(edgePage, blk, building);

    /* Establish conns for blk and its neighbors */
    InterInsert(blk, prunedList, building);

    /* Clean up */
    scratch->bestLNodes->clear();
    prunedList->reset();
    pool->reset();
}

void DiskAnnGraph::IterateToFixedPoint(BlockNumber blk, const uint32 Lsize, BlockNumber frozen,
                                       VectorList<Neighbor>* pool, bool searchInvocation)
{
    NeighborPriorityQueue* bestLNodes = scratch->bestLNodes;
    VectorList<Neighbor>* neighbors = &scratch->neighbors;
    bestLNodes->reserve(Lsize);

    /* pool collects the expanded nodes, or (pruneVisited) every node whose distance was computed */
    bool collectVisited = !searchInvocation && pruneVisited;
    bool collectExpanded = !searchInvocation && !pruneVisited;

    if (!graphStore->VisitedTestAndSet(frozen)) {
        float dis = graphStore->ComputeDistance(frozen, scratch->alignedQuery, scratch->sqrSum);
        Neighbor nn = Neighbor(frozen, dis);
        bestLNodes->insert(nn);
        if (collectVisited) {
            pool->push_back(nn);
        }
    }

    while (bestLNodes->has_unexpanded_node()) {
        Neighbor nbr;
        BlockNumber blockNumber;

        nbr = bestLNodes->closest_unexpanded();
        blockNumber = nbr.id;

        if (collectExpanded) {
            pool->push_back(nbr);
        }

        neighbors->reset();
        graphStore->GetNeighbors(blockNumber, neighbors);
        for (size_t i = 0; i < neighbors->size(); i++) {
            BlockNumber blkno = (*neighbors)[i].id;
            /* TestAndSet enters blkno; the distance is only needed for unvisited nodes */
            if (graphStore->VisitedTestAndSet(blkno)) {
                continue;
            }
            float distance = graphStore->ComputeDistance(blkno, scratch->alignedQuery, scratch->sqrSum);
            Neighbor cand = Neighbor(blkno, distance);
            bestLNodes->insert(cand);
            if (collectVisited) {
                pool->push_back(cand);
            }
        }
        neighbors->reset();
    }
}

void DiskAnnGraph::PruneNeighbors(BlockNumber location, VectorList<Neighbor>* pool, VectorList<Neighbor>* prunedList)
{
    if (pool->size() == 0) {
        // if the pool is empty, behave like a noop (the list is scratch: empty it, keep its buffer)
        prunedList->reset();
        return;
    }
    qsort(&((*pool)[0]), pool->size(), sizeof(Neighbor), CmpNeighborInfo);
    prunedList->reset();
    prunedList->reserve(maxDegree);
    float alpha = INDEXING_ALPHA;
    OccludeList(location, pool, prunedList, alpha);
    Assert(prunedList->size() <= maxDegree);

    if (saturateGraph && alpha > 1) {
        for (size_t i = 0; i < pool->size(); i++) {
            const auto& node = (*pool)[i];
            if (prunedList->size() >= maxDegree)
                break;
            if (!prunedList->contains(node) && node.id != location) {
                prunedList->push_back(node);
            }
        }
    }
}

void DiskAnnGraph::OccludeList(BlockNumber location, VectorList<Neighbor>* pool, VectorList<Neighbor>* result,
                               const float alpha)
{
    if (pool->size() == 0)
        return;

    Assert(result->size() == 0);
    if (pool->size() > INDEXINGMAXC)
        pool->resize(INDEXINGMAXC);
    graphStore->PrefetchPool(location, pool);

    VectorList<float>& occlude_factor = scratch->occludeFactor;
    // occlude_list can be called with the same scratch more than once by
    // search_for_point_and_add_link through inter_insert.
    occlude_factor.reset();
    occlude_factor.resize(pool->size());
    errno_t rc = memset_s(&occlude_factor[0], sizeof(float) * occlude_factor.size(), 0.0f,
                          sizeof(float) * occlude_factor.size());
    securec_check(rc, "\0", "\0");

    float cur_alpha = 1;
    while (cur_alpha <= alpha && result->size() < maxDegree) {
        // used for MIPS, where we store a value of eps in cur_alpha to
        // denote pruned out entries which we can skip in later rounds.
        float eps = cur_alpha + 0.01f;
        unsigned int idx = 0;
        for (auto iter = pool->begin(); result->size() < maxDegree && iter != pool->end(); ++iter, ++idx) {
            size_t slot = (size_t)(iter - pool->begin());
            if (occlude_factor[slot] > cur_alpha) {
                continue;
            }
            // Set the entry to float::max so that is not considered again
            occlude_factor[slot] = FLT_MAX;
            // Add the entry to the result if its not been deleted, and doesn't
            // add a self loop
            if (iter->id != location) {
                result->push_back(*iter);
            }

            unsigned int pos = idx + 1;

            // Update occlude factor for points from iter+1 to pool.end()
            for (auto iter2 = iter + 1; iter2 != pool->end(); iter2++, pos++) {
                size_t t = (size_t)(iter2 - pool->begin());
                if (occlude_factor[t] > alpha)
                    continue;

                float djk = graphStore->GetDistance(iter2->id, iter->id);
                if (functype == DISKANN_DIS_L2 || functype == DISKANN_DIS_COSINE) {
                    occlude_factor[t] = (djk == 0) ? FLT_MAX : std::max(occlude_factor[t], iter2->distance / djk);
                } else if (functype == DISKANN_DIS_IP) {
                    // Improvization for flipping max and min dist for MIPS
                    float x = -iter2->distance;
                    float y = -djk;
                    if (y > cur_alpha * x) {
                        occlude_factor[t] = std::max(occlude_factor[t], eps);
                    }
                }
            }
        }
        cur_alpha *= 1.2f;
    }
    occlude_factor.reset();
}

/*
 * Reverse edges for blk. Called by Link right after the forward edges were
 * flushed: scratch->alignedQuery / sqrSum still hold blk's vector from the
 * GetVector at the start of Link, so it is not fetched again. The search's
 * visited set is not needed any more at this point; the re-prune reuses it
 * to de-duplicate the destination's neighbour list.
 */
static void CopyNeighborsWithExtra(VectorList<Neighbor>* dst, VectorList<Neighbor>* src, const Neighbor& extra)
{
    for (size_t j = 0; j < src->size(); j++) {
        dst->push_back((*src)[j]);
    }
    dst->push_back(extra);
}

bool DiskAnnGraph::AppendReverseEdge(BlockNumber desId, BlockNumber blk, float distance, bool building)
{
    DiskAnnEdgePage edgePage = scratch->edgePage;
    graphStore->GetEdge(edgePage, desId);
    if (graphStore->NeighborExists(edgePage, blk) || edgePage->count >= maxDegree) {
        return false;
    }
    graphStore->AddNeighbor(edgePage, blk, distance);
    graphStore->FlushEdge(edgePage, desId, building);
    return true;
}

void DiskAnnGraph::RepruneReverseEdge(BlockNumber desId, VectorList<Neighbor>* copyNeighbors, bool building)
{
    VectorList<Neighbor>* dummyPool = &scratch->dummyPool;
    VectorList<Neighbor>* pruned = &scratch->pruned;
    DiskAnnEdgePage edgePage = scratch->edgePage;
    size_t reserveSize = (size_t)(ceil(GRAPH_SLACK_FACTOR * maxDegree));

    dummyPool->reset();
    dummyPool->reserve(reserveSize);
    graphStore->VisitedReset((long)reserveSize);
    visitedFresh = false;

    graphStore->PrefetchPool(desId, copyNeighbors);
    for (size_t j = 0; j < copyNeighbors->size(); j++) {
        const Neighbor& curNbr = (*copyNeighbors)[j];
        if (graphStore->VisitedTestAndSet(curNbr.id) || curNbr.id == desId) {
            continue;
        }
        float dist = graphStore->GetDistance(desId, curNbr.id);
        dummyPool->push_back(Neighbor(curNbr.id, dist));
    }

    PruneNeighbors(desId, dummyPool, pruned);
    graphStore->GetEdge(edgePage, desId);
    for (uint16 k = 0; k < edgePage->count; k++) {
        edgePage->nexts[k] = InvalidBlockNumber;
    }
    edgePage->count = 0;
    for (uint32 k = 0; k < (uint32)pruned->size(); k++) {
        graphStore->AddNeighbor(edgePage, (*pruned)[k].id, (*pruned)[k].distance);
    }
    graphStore->FlushEdge(edgePage, desId, building);
    pruned->reset();
    dummyPool->reset();
}

void DiskAnnGraph::InterInsert(BlockNumber blk, VectorList<Neighbor>* prunedList, bool building)
{
    VectorList<Neighbor>* pool = prunedList;
    VectorList<Neighbor>* desPool = &scratch->desPool;
    VectorList<Neighbor>* copyNeighbors = &scratch->copyNeighbors;

    for (size_t i = 0; i < pool->size(); ++i) {
        auto des = (*pool)[i];
        if (graphStore->ContainsNeighbors(des.id, blk)) {
            continue;
        }

        float distance = graphStore->ComputeDistance(des.id, scratch->alignedQuery, scratch->sqrSum);
        Neighbor nn = Neighbor(blk, distance);
        copyNeighbors->reset();
        graphStore->GetNeighbors(des.id, desPool);
        if (desPool->contains(nn)) {
            desPool->reset();
            copyNeighbors->reset();
            continue;
        }
        if (desPool->size() < maxDegree) {
            bool appended = AppendReverseEdge(des.id, blk, distance, building);
            desPool->reset();
            copyNeighbors->reset();
            if (!appended) {
                break;
            }
            continue;
        }
        CopyNeighborsWithExtra(copyNeighbors, desPool, nn);
        RepruneReverseEdge(des.id, copyNeighbors, building);
        desPool->reset();
        copyNeighbors->reset();
    }
}

bool DiskAnnGraph::FindDuplicateNeighbor(NeighborPriorityQueue* bestLNodes, BlockNumber blk, bool building)
{
    for (size_t i = 0; i < bestLNodes->size(); i++) {
        Neighbor res = bestLNodes->_data[i];
        if (res.distance > DISKANN_DISTANCE_THRESHOLD) {
            break;
        }
        if (res.id != blk) {
            return graphStore->MergeDuplicate(res.id, blk, building);
        }
    }

    return false;
}

float ComputeL2DistanceFast(const float* u, const double su, const float* v, const double sv, uint16_t dim)
{
    return (float)(su + sv - DISTANCE_FACTOR * VectorInnerProduct((int)dim, (float*)u, (float*)v));
}

void GetEdgeTuple(DiskAnnEdgePage tup, BlockNumber blkno, Relation index, uint32 nodeSize, uint32 edgeSize)
{
    Buffer buf;
    Page page;
    DiskAnnNodePage ntup;
    DiskAnnEdgePage etup;
    errno_t rc;

    buf = ReadBuffer(index, blkno);
    LockBuffer(buf, BUFFER_LOCK_SHARE);

    page = BufferGetPage(buf);
    ntup = DiskAnnPageGetNode(DiskAnnPageGetIndexTuple(page));
    etup = (DiskAnnEdgePage)((uint8_t*)ntup + nodeSize);

    rc = memcpy_s(tup, edgeSize, etup, edgeSize);
    securec_check_c(rc, "\0", "\0");

    UnlockReleaseBuffer(buf);
    return;
}

int CmpNeighborInfo(const void* a, const void* b)
{
    const Neighbor* left = (const Neighbor*)a;
    const Neighbor* right = (const Neighbor*)b;
    if (left->distance < right->distance) {
        return -1;
    } else if (left->distance > right->distance) {
        return 1;
    }
    Assert(left->id != right->id);
    return left->id > right->id ? -1 : 1;
}

int CmpIdxScanData(const void *a, const void *b)
{
    const DiskAnnIdxScanData *left = (const DiskAnnIdxScanData *)a;
    const DiskAnnIdxScanData *right = (const DiskAnnIdxScanData *)b;
    if (left->distance < right->distance) {
        return -1;
    } else if (left->distance > right->distance) {
        return 1;
    }
    Assert(left->id != right->id);
    return left->id > right->id ? -1 : 1;
}

/*
 * Get whether to enable PQ
 */
bool DiskAnnEnablePQ(Relation index)
{
    DiskAnnOptions* opts = (DiskAnnOptions*)index->rd_options;
    return opts ? opts->enablePQ : GENERIC_DEFAULT_ENABLE_PQ;
}

/*
 * Get the number of subquantizer
 */
int DiskAnnGetPqM(Relation index)
{
    DiskAnnOptions* opts = (DiskAnnOptions*)index->rd_options;
    return opts ? opts->pqM : GENERIC_DEFAULT_PQ_M;
}

bool IsMarkDeleted(Relation index, BlockNumber master)
{
    bool masterIsVacuumDeleted = false;
    bool foundAlive = false;
    BlockNumber curVertex = master;
    Buffer cbuf = ReadBuffer(index, curVertex);
    LockBuffer(cbuf, BUFFER_LOCK_SHARE);
    Page cpage = BufferGetPage(cbuf);
    IndexTuple itup = DiskAnnPageGetIndexTuple(cpage);
    DiskAnnNodePage tup_src = DiskAnnPageGetNode(itup);
    ItemId iid = PageGetItemId((cpage), FirstOffsetNumber);
    bool isDeleted = ItemIdIsDead(iid);
    if (isDeleted && curVertex == master && (!DiskAnnNodeIsInserted(tup_src->tag))) {
        masterIsVacuumDeleted = true;
    }
    UnlockReleaseBuffer(cbuf);
    if (!isDeleted) {
        foundAlive = true;
    }
    return (!foundAlive);
}

VamanaVertexNbIterator::VamanaVertexNbIterator(Relation index, BlockNumber blk, uint32_t flag)
{
    infoFlag = flag;
    curNeighborId = DISKANN_MAX_DEGREE;
    curNeighborBuf = InvalidBuffer;
    curNbtup = NULL;
    curItup = NULL;
    Buffer nodeBuf = ReadBuffer(index, blk);
    LockBuffer(nodeBuf, BUFFER_LOCK_SHARE);
    Page page = BufferGetPage(nodeBuf);
    DiskAnnNodePage tup = DiskAnnPageGetNode(DiskAnnPageGetIndexTuple(page));
    if (DiskAnnNodeIsSlave(tup->tag)) {
        Assert(tup->master != InvalidBlockNumber);
        Buffer masterBuf = ReadBuffer(index, tup->master);
        LockBuffer(masterBuf, BUFFER_LOCK_SHARE);
        Page masterPage = BufferGetPage(masterBuf);
        tup = DiskAnnPageGetNode(DiskAnnPageGetIndexTuple(masterPage));
        UnlockReleaseBuffer(nodeBuf);
        nodeBuf = masterBuf;
        Assert(DiskAnnNodeIsMaster(tup->tag));
    }
    edges = (DiskAnnEdgePage)((uint8_t *)tup + nodeSize);
}

void VamanaVertexNbIterator::begin()
{
    conditionMove(BEGIN);
}

void VamanaVertexNbIterator::next()
{
    conditionMove(NEXT);
}

bool VamanaVertexNbIterator::isFinished()
{
    return curNeighborId >= edges->count;
}

void VamanaVertexNbIterator::conditionMove(MoveDirection direction)
{
    bool found = false;
    releaseCurNeighborBuffer();
    if (direction == BEGIN) {
        curNeighborId = 0;
    } else if (direction == NEXT) {
        curNeighborId++;
    }
    if (skipVisited) {
        for (; curNeighborId < edges->count; curNeighborId++) {
            blockhash_insert(blocks, edges->nexts[curNeighborId], &found);
            if (!found) {
                break;
            }
        }
    }
}

BlockNumber VamanaVertexNbIterator::getCurNeighborInfo(DiskAnnNeighborInfoT *info)
{
    if ((infoFlag & DISKANN_NEIGHBOR_VECTOR) == DISKANN_NEIGHBOR_VECTOR) {
        getCurNeighborBuffer();
    }

    DiskAnnNeighborInfoT nbinfo = {0};
    if ((infoFlag & DISKANN_NEIGHBOR_BLKNO) == DISKANN_NEIGHBOR_BLKNO) {
        nbinfo.blkno = edges->nexts[curNeighborId];
    }
    if ((infoFlag & DISKANN_NEIGHBOR_DISTANCE) == DISKANN_NEIGHBOR_DISTANCE) {
        nbinfo.distance = edges->distance[curNeighborId];
    }
    if ((infoFlag & DISKANN_NEIGHBOR_VECTOR) == DISKANN_NEIGHBOR_VECTOR) {
        Assert(curItup != NULL);
        Assert(curNbtup != NULL);
        TupleDesc tupdesc = RelationGetDescr(rel);
        bool isnull;
        Datum src = index_getattr(curItup, 1, tupdesc, &isnull);
        Datum dst = PointerGetDatum(PG_DETOAST_DATUM(src));
        nbinfo.vector = dst;
        nbinfo.sqrSum = curNbtup->sqrSum;
        if (DatumGetPointer(dst) != DatumGetPointer(src)) {
            /* Mark vector needs to be released by the user */
            nbinfo.freeVector = true;
        }
    }
    *info = nbinfo;
    return info->blkno;
}

void VamanaVertexNbIterator::getCurNeighborBuffer()
{
    curNeighborBuf = ReadBuffer(rel, edges->nexts[curNeighborId]);
    LockBuffer(curNeighborBuf, BUFFER_LOCK_SHARE);
    Page page = BufferGetPage(curNeighborBuf);
    curItup = DiskAnnPageGetIndexTuple(page);
    curNbtup = DiskAnnPageGetNode(curItup);
}

void VamanaVertexNbIterator::releaseCurNeighborBuffer()
{
    if (curNeighborBuf != InvalidBuffer) {
        UnlockReleaseBuffer(curNeighborBuf);
        curNeighborBuf = InvalidBuffer;
        curNbtup = NULL;
        curItup = NULL;
    }
}

DiskAnnAliveSlaveIterator::DiskAnnAliveSlaveIterator(BlockNumber blk)
{
    master = blk;
    curVertex = InvalidBlockNumber;
    nextVertex = InvalidBlockNumber;
}

DiskAnnIdxScanData DiskAnnAliveSlaveIterator::GetCurVer()
{
    curVertex = nextVertex;
    return data;
}

void DiskAnnAliveSlaveIterator::begin()
{
    curVertex = master;
    next();
}

void DiskAnnAliveSlaveIterator::next()
{
    while (curVertex != InvalidBlockNumber) {
        Buffer cbuf = ReadBuffer(index, curVertex);
        LockBuffer(cbuf, BUFFER_LOCK_SHARE);
        Page cpage = BufferGetPage(cbuf);
        IndexTuple itup = DiskAnnPageGetIndexTuple(cpage);
        DiskAnnNodePage tup = DiskAnnPageGetNode(itup);

        bool visible = true;
        bool needRecheck = false;
        ItemId iid = PageGetItemId((cpage), FirstOffsetNumber);
        bool isDead = ItemIdIsDead(iid);

        data.id = curVertex;
        data.heapCtid = itup->t_tid;
        data.needRecheck = needRecheck;
        data.itup = itup;

        UnlockReleaseBuffer(cbuf);
        if (visible && (!isDead)) {
            break;
        } else {
            curVertex = nextVertex;
        }
    }
}

bool DiskAnnAliveSlaveIterator::isFinished()
{
    return curVertex == InvalidBlockNumber;
}

// return PQ_ERROR if error occurs
#define PQ_RETURN_IFERR(ret)                            \
    do {                                                \
        int status = (ret);                           \
        if (SECUREC_UNLIKELY(status != PQ_SUCCESS)) { \
            return status;                            \
        }                                               \
    } while (0)

int diskann_pq_load_symbol(char *symbol, void **sym_lib_handle)
{
#ifndef WIN32
    const char *dlsym_err = NULL;
    *sym_lib_handle = dlsym(g_diskann_pq_func.handle, symbol);
    dlsym_err = dlerror();
    if (dlsym_err != NULL) {
        ereport(WARNING, (errcode(ERRCODE_INVALID_OPERATION),
            errmsg("incompatible library \"%s\", load %s failed, %s", DISKANN_PQ_SO_NAME, symbol, dlsym_err)));
        return PQ_ERROR;
    }
#endif // !WIN32
    return PQ_SUCCESS;
}

#define PQ_LOAD_SYMBOL_FUNC(func) diskann_pq_load_symbol(#func, (void **)&g_diskann_pq_func.func)

int diskann_pq_resolve_path(char* absolute_path, const char* raw_path, const char* filename)
{
    char* resolvedPath = realpath(raw_path, NULL);
    if (resolvedPath == NULL) {
        if (errno != ENOENT && errno != EACCES) {
            return PQ_ERROR;
        }
        int ret = snprintf_s(absolute_path, MAX_PATH_LEN, MAX_PATH_LEN - 1, "%s/%s", raw_path, filename);
        if (ret < 0) {
            return PQ_ERROR;
        }
        return PQ_SUCCESS;
    }

    int ret = snprintf_s(absolute_path, MAX_PATH_LEN, MAX_PATH_LEN - 1, "%s/%s", resolvedPath, filename);
    free(resolvedPath);

    if (ret < 0) {
        return PQ_ERROR;
    }
    return PQ_SUCCESS;
}

int diskann_pq_open_dl(void **lib_handle, char *symbol)
{
#ifndef WIN32
    *lib_handle = dlopen(symbol, RTLD_LAZY);
    if (*lib_handle == NULL) {
        ereport(WARNING, (errcode_for_file_access(), errmsg("could not load library %s, %s", DISKANN_PQ_SO_NAME, dlerror())));
        return PQ_ERROR;
    }
    return PQ_SUCCESS;
#else
    return PQ_ERROR;
#endif
}

void diskann_pq_close_dl(void *lib_handle)
{
#ifndef WIN32
    (void)dlclose(lib_handle);
#endif
}

int diskann_pq_load_symbols(char *lib_dl_path)
{
    PQ_RETURN_IFERR(diskann_pq_open_dl(&g_diskann_pq_func.handle, lib_dl_path));

    PQ_RETURN_IFERR(PQ_LOAD_SYMBOL_FUNC(ComputePQTable));
    PQ_RETURN_IFERR(PQ_LOAD_SYMBOL_FUNC(ComputeVectorPQCode));
    PQ_RETURN_IFERR(PQ_LOAD_SYMBOL_FUNC(GetPQDistanceTable));
    PQ_RETURN_IFERR(PQ_LOAD_SYMBOL_FUNC(GetPQDistance));

    return PQ_SUCCESS;
}

int diskann_pq_func_init()
{
    if (g_diskann_pq_func.inited) {
        return PQ_SUCCESS;
    }

    char lib_dl_path[MAX_PATH_LEN] = { 0 };
    char *raw_path = getenv(PQ_ENV_PATH);
    if (raw_path == nullptr) {
        ereport(WARNING, (errmsg("failed to get DATAVEC_PQ_LIB_PATH")));
        return PQ_ERROR;
    }

    int ret = diskann_pq_resolve_path(lib_dl_path, raw_path, DISKANN_PQ_SO_NAME);
    if (ret != PQ_SUCCESS) {
        ereport(WARNING, (errmsg("failed to resolve the path of libdisksearch_opgs.so, lib_dl_path %s, raw_path %s",
                                 lib_dl_path, raw_path)));
        return PQ_ERROR;
    }

    ret = diskann_pq_load_symbols(lib_dl_path);
    if (ret != PQ_SUCCESS) {
        ereport(WARNING,
                (errmsg("failed to load libdisksearch_opgs.so, lib_dl_path %s, raw_path %s", lib_dl_path, raw_path)));
        return PQ_ERROR;
    }

    g_diskann_pq_func.inited = true;
    return PQ_SUCCESS;
}

int DiskAnnPQInit()
{
#ifdef __x86_64__
    return PQ_ERROR;
#endif
    if (diskann_pq_func_init() != PQ_SUCCESS) {
        return PQ_ERROR;
    }
    g_instance.diskann_pq_inited = true;
    return PQ_SUCCESS;
}

void DiskAnnPQUinit()
{
    if (!g_instance.diskann_pq_inited) {
        return;
    }
    g_instance.diskann_pq_inited = false;
    ereport(LOG, (errmsg("datavec diskann PQ uninit")));
    if (g_diskann_pq_func.handle != NULL) {
        diskann_pq_close_dl(g_diskann_pq_func.handle);
        g_diskann_pq_func.handle = NULL;
        g_diskann_pq_func.inited = false;
    }
}

int ComputePQTable(VectorArray samples, DiskPQParams *params)
{
    return g_diskann_pq_func.ComputePQTable(samples, params);
}

int ComputeVectorPQCode(VectorArray baseData, const DiskPQParams *params, uint8_t *pqCode)
{
    return g_diskann_pq_func.ComputeVectorPQCode(baseData, params, pqCode);
}

int GetPQDistanceTable(char *vec, const DiskPQParams *params, float *pqDistanceTable)
{
    return g_diskann_pq_func.GetPQDistanceTable(vec, params, pqDistanceTable);
}

int GetPQDistance(const uint8_t *basecode, const DiskPQParams *params,
                  const float *pqDistanceTable, float &pqDistance)
{
    return g_diskann_pq_func.GetPQDistance(basecode, params, pqDistanceTable, pqDistance);
}

void DiskAnnCreatePQPages(DiskAnnBuildState *buildstate)
{
    Relation index = buildstate->index;
    Buffer buf;
    Page page;
    uint16 pqTableNblk;
    uint16 pqCentroidsblk;
    uint16 pqOffsetblk;

    DiskAnnGetPQInfoFromMetaPage(index, &pqTableNblk, NULL, &pqCentroidsblk, NULL, &pqOffsetblk, NULL);

    /* create pq table transposed page */
    for (uint16 i = 0; i < pqTableNblk; i++) {
        buf = DiskAnnNewBuffer(index, MAIN_FORKNUM);
        page = DiskAnnInitRegisterPage(index, buf);
        MarkBufferDirty(buf);
        UnlockReleaseBuffer(buf);
    }

    /* create pq centroids page */
    for (uint16 i = 0; i < pqCentroidsblk; i++) {
        buf = DiskAnnNewBuffer(index, MAIN_FORKNUM);
        page = DiskAnnInitRegisterPage(index, buf);
        MarkBufferDirty(buf);
        UnlockReleaseBuffer(buf);
    }

    /* create pq offset page */
    for (uint16 i = 0; i < pqOffsetblk; i++) {
        buf = DiskAnnNewBuffer(index, MAIN_FORKNUM);
        page = DiskAnnInitRegisterPage(index, buf);
        MarkBufferDirty(buf);
        UnlockReleaseBuffer(buf);
    }
}

/*
* Flush PQ table into page during index building
 */
void DiskAnnFlushPQInfo(DiskAnnBuildState *buildstate)
{
    Relation index = buildstate->index;
    char* pqTableTransposed = buildstate->params->tablesTransposed;
    char* pqCentroids = buildstate->params->centroids;
    uint32 *pqOffset = buildstate->params->offsets;
    uint16 pqTableNblk;
    uint32 pqTableSize;
    uint16 pqCentroidsblk;
    uint32 pqCentroidsSize;
    uint16 pqOffsetblk;
    uint32 pqOffsetSize;

    DiskAnnCreatePQPages(buildstate);

    DiskAnnGetPQInfoFromMetaPage(index, &pqTableNblk, &pqTableSize, &pqCentroidsblk, &pqCentroidsSize,
                                 &pqOffsetblk, &pqOffsetSize);

    /* Flush pq table transposed */
    DiskAnnFlushPQInfoInternal(index, pqTableTransposed, DISKANN_PQDATA_START_BLKNO, pqTableNblk, pqTableSize);

    /* Flush pqCentroids */
    DiskAnnFlushPQInfoInternal(index, pqCentroids, DISKANN_PQDATA_START_BLKNO + pqTableNblk,
                               pqCentroidsblk, pqCentroidsSize);

    /* Flush pqOffsets */
    DiskAnnFlushPQInfoInternal(index, pqOffset, DISKANN_PQDATA_START_BLKNO + pqTableNblk + pqCentroidsblk,
                               pqOffsetblk, pqOffsetSize);
}

/*
* Get the info related to pqTable in metapage
 */
void DiskAnnGetPQInfoFromMetaPage(Relation index, uint16 *pqTableNblk, uint32 *pqTableSize,
                                  uint16 *pqCentroidsblk, uint32 *pqCentroidsSize,
                                  uint16 *pqOffsetblk, uint32 *pqOffsetSize)
{
    Buffer buf;
    Page page;
    DiskAnnMetaPage metap;

    buf = ReadBuffer(index, DISKANN_METAPAGE_BLKNO);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buf);
    metap = DiskAnnPageGetMeta(page);
    if (unlikely(metap->magicNumber != DISKANN_MAGIC_NUMBER)) {
        UnlockReleaseBuffer(buf);
        elog(ERROR, "diskann index is not valid");
    }
    if (pqTableNblk != NULL) {
        *pqTableNblk = metap->pqTableNblk;
    }
    if (pqTableSize != NULL) {
        *pqTableSize = metap->pqTableSize;
    }
    if (pqCentroidsblk != NULL) {
        *pqCentroidsblk = metap->pqCentroidsblk;
    }
    if (pqCentroidsSize != NULL) {
        *pqCentroidsSize = metap->pqCentroidsSize;
    }
    if (pqOffsetblk != NULL) {
        *pqOffsetblk = metap->pqOffsetblk;
    }
    if (pqOffsetSize != NULL) {
        *pqOffsetSize = metap->pqOffsetSize;
    }

    UnlockReleaseBuffer(buf);
}

DiskPQParams* InitDiskPQParamsOnDisk(Relation index, FmgrInfo *procinfo, int dim, bool enablePQ, bool needOrigTable)
{
    if (!enablePQ) {
        return NULL;
    }

    DiskPQParams *params = (DiskPQParams*)palloc(sizeof(DiskPQParams));

    Buffer buf = ReadBuffer(index, DISKANN_METAPAGE_BLKNO);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    Page page = BufferGetPage(buf);
    DiskAnnMetaPage metap = DiskAnnPageGetMeta(page);
    params->pqChunks = metap->pqM;
    UnlockReleaseBuffer(buf);

    if (!g_instance.diskann_pq_inited) {
        ereport(ERROR, (errmsg("the SQL involves operations related to DiskPQ, "
                               "but this instance has not currently loaded the PQ dynamic library.")));
    }

    params->funcType = GetFunctionType(procinfo, DiskAnnOptionalProcInfo(index, DISKANN_NORM_PROC));
    params->dim = dim;
    
    uint16 pqTableNblk;
    uint32 pqTableSize;
    uint16 pqCentroidsblk;
    uint32 pqCentroidsSize;
    uint16 pqOffsetblk;
    uint32 pqOffsetSize;

    DiskAnnGetPQInfoFromMetaPage(index, &pqTableNblk, &pqTableSize, &pqCentroidsblk, &pqCentroidsSize,
                                 &pqOffsetblk, &pqOffsetSize);

    /* Only load data once per relation to prevent repeated loading for every search */
    bool needLoadIndex = false;
    MemoryContext oldcxt;
    if (index->diskPQTableTransposed == NULL || index->centroids == NULL || index->offsets == NULL) {
        needLoadIndex = true;
        oldcxt = MemoryContextSwitchTo(index->rd_indexcxt);
    }

    /* Load pq table transposed into relation's fields */
    if (index->diskPQTableTransposed == NULL) {
        LoadPQInfo(index, index->diskPQTableTransposed, DISKANN_PQDATA_START_BLKNO, pqTableNblk, pqTableSize);
    }

    /* Load pqCentroids */
    if (index->centroids == NULL) {
        LoadPQInfo(index, index->centroids, DISKANN_PQDATA_START_BLKNO + pqTableNblk, pqCentroidsblk, pqCentroidsSize);
    }

    /* Load pqOffsets */
    if (index->offsets == NULL) {
        LoadPQInfo(index, index->offsets, DISKANN_PQDATA_START_BLKNO + pqTableNblk + pqCentroidsblk,
                   pqOffsetblk, pqOffsetSize);
    }

    if (needOrigTable && index->pqTable == NULL) {
        // If the original pq table is needed (for example when update requires re-computing pqcode)
        index->pqTable = (char *)palloc0(pqTableSize);
        for (size_t i = 0; i < GENERIC_DEFAULT_PQ_KSUB; i++) {
            for (size_t j = 0; j < dim; j++) {
                ((float *)(index->pqTable))[i * dim + j] =
                    ((float *)(index->diskPQTableTransposed))[j * GENERIC_DEFAULT_PQ_KSUB + i];
            }
        }
    }

    if (needLoadIndex) { // once load pq is done, we switch to the original context
        (void)MemoryContextSwitchTo(oldcxt);
    }

    if (needOrigTable) {
        params->pqTable = index->pqTable;
    }

    params->tablesTransposed = index->diskPQTableTransposed;
    params->centroids = index->centroids;
    params->offsets = index->offsets;

    return params;
}

