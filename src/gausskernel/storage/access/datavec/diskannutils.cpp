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
#include "postgres.h"
#include "access/datavec/diskann.h"

#define INDEXING_ALPHA (1.2)
#define GRAPH_SLACK_FACTOR (1.365f)
#define INIT_INSERT_NODE_SIZE 16
#define DISTANCE_FACTOR 2

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
    if (!OidIsValid(index_getprocid(index, 1, procnum)))
        return NULL;

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

void DiskAnnUpdateMetaPage(Relation index, BlockNumber blkno, ForkNumber forkNum)
{
    Buffer buf;
    Page page;
    buf = ReadBuffer(index, DISKANN_METAPAGE_BLKNO);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    page = BufferGetPage(buf);

    DiskAnnMetaPage metaPage = DiskAnnPageGetMeta(page);
    metaPage->insertPage = blkno;
    MarkBufferDirty(buf);  // todo: add xlog for meta page
    UnlockReleaseBuffer(buf);
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
    securec_check(rc, "\0", "\0");
    UnlockReleaseBuffer(buf);
    return;
}

DiskAnnGraphStore::DiskAnnGraphStore(Relation relation)
{
    m_rel = relation;
    DiskAnnMetaPageData metapage;
    DiskANNGetMetaPageInfo(relation, &metapage);
    m_nodeSize = metapage.nodeSize;
    m_edgeSize = metapage.edgeSize;
    m_itemSize = metapage.itemSize;
    m_dimension = metapage.dimensions;
}

DiskAnnGraphStore::~DiskAnnGraphStore()
{
    m_rel = NULL;
}

void DiskAnnGraphStore::GetVector(BlockNumber blkno, float* vec, double* sqrSum, ItemPointerData* hctid) const
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
    securec_check_c(rc, "\0", "\0");

    /* get sqrSum from node tup */
    ntup = DiskAnnPageGetNode(ctup);
    *sqrSum = ntup->sqrSum;

    if (DatumGetPointer(dst) != DatumGetPointer(src)) {
        pfree(DatumGetPointer(dst));
    }

    UnlockReleaseBuffer(buf);
}

float DiskAnnGraphStore::GetDistance(BlockNumber blk1, BlockNumber blk2) const
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

float DiskAnnGraphStore::ComputeDistance(BlockNumber blk1, float* vec2, double sqrSum2) const
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

void DiskAnnGraphStore::GetNeighbors(BlockNumber blkno, VectorList<Neighbor>* nbrs)
{
    Buffer buf;
    Page page;
    IndexTuple ctup;
    DiskAnnNodePage ntup;
    DiskAnnEdgePage etup;

    /* Reserve space */
    nbrs->reset();
    nbrs->reserve((size_t)(ceil(GRAPH_SLACK_FACTOR * OUTDEGREE)));

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

void DiskAnnGraphStore::AddNeighbor(DiskAnnEdgePage edge, BlockNumber id, float distance) const
{
    edge->nexts[edge->count] = id;
    edge->distance[edge->count] = distance;
    edge->count++;
}

void DiskAnnGraphStore::FlushEdge(DiskAnnEdgePage edgePage, BlockNumber blk) const
{
    Buffer buf;
    Page page;
    DiskAnnNodePage ntup;
    DiskAnnEdgePage etup;
    errno_t rc;

    buf = ReadBuffer(m_rel, blk);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

    page = BufferGetPage(buf);
    ntup = DiskAnnPageGetNode(DiskAnnPageGetIndexTuple(page));
    etup = (DiskAnnEdgePage)((uint8_t*)ntup + m_nodeSize);
    rc = memcpy_s(etup, m_edgeSize, edgePage, m_edgeSize);
    securec_check_c(rc, "\0", "\0");

    MarkBufferDirty(buf);   // todo: add xlog for edge page
    UnlockReleaseBuffer(buf);
}

bool DiskAnnGraphStore::ContainsNeighbors(BlockNumber src, BlockNumber blk) const
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

void DiskAnnGraphStore::AddDuplicateNeighbor(BlockNumber src, ItemPointerData tid)
{
    Buffer buf;
    Page page;
    DiskAnnNodePage ntup;
    bool found = false;

    buf = ReadBuffer(m_rel, src);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

    page = BufferGetPage(buf);
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
            MarkBufferDirty(buf);   // todo: add xlog
        }
    }
    UnlockReleaseBuffer(buf);
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

DiskAnnGraph::DiskAnnGraph(Relation rel, double dim, BlockNumber blkno, DiskAnnGraphStore* graphStore)
{
    this->graphStore = graphStore;
    scratch = (QueryScratch*)palloc0(sizeof(QueryScratch));
    scratch->alignedQuery = (float*)palloc(dim * sizeof(float));
    scratch->bestLNodes = (NeighborPriorityQueue*)palloc0(sizeof(NeighborPriorityQueue));
    scratch->bestLNodes->_data = VectorList<Neighbor>();
    HASHCTL hash_ctl = {.keysize = sizeof(uint32_t), .entrysize = sizeof(uint32_t), .hcxt = CurrentMemoryContext};

    scratch->insertedNodeHash =
        hash_create("DiskANN inserted nodes hash", INIT_INSERT_NODE_SIZE, &hash_ctl, HASH_ELEM | HASH_CONTEXT);
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
        if (scratch->insertedNodeHash) {
            hash_destroy(scratch->insertedNodeHash);
        }
        pfree_ext(scratch);
    }
    scratch = NULL;
}

void DiskAnnGraph::Link(BlockNumber blk, int index_size)
{
    VectorList<Neighbor> pool;
    ItemPointerData hctid;
    VectorList<Neighbor> prunedList;
    DiskAnnEdgePage edgePage;

    scratch->bestLNodes->clear();
    ItemPointerSetInvalid(&hctid);

    graphStore->GetVector(blk, scratch->alignedQuery, &(scratch->sqrSum), &hctid);

    /* Find and add appropriate graph edges */
    IterateToFixedPoint(blk, index_size, frozen, &pool, false);

    if (FindDuplicateNeighbor(scratch->bestLNodes, blk)) {
        scratch->bestLNodes->clear();
        pool.clear();
        return;
    }

    prunedList = VectorList<Neighbor>();
    PruneNeighbors(blk, &pool, &prunedList);
    Assert(prunedList.size() <= OUTDEGREE);

    edgePage = (DiskAnnEdgePage)palloc(graphStore->m_edgeSize);
    GetEdgeTuple(edgePage, blk, graphStore->m_rel, graphStore->m_nodeSize, graphStore->m_edgeSize);
    for (uint16 i = 0; i < edgePage->count; i++) {
        edgePage->nexts[i] = InvalidOffsetNumber;
    }
    edgePage->count = 0;

    for (uint32 i = 0; i < (uint32)prunedList.size(); i++) {
        graphStore->AddNeighbor(edgePage, prunedList[i].id, prunedList[i].distance);
    }
    graphStore->FlushEdge(edgePage, blk);
    pfree(edgePage);

    /* Establish conns for blk and its neighbors */
    InterInsert(blk, &prunedList);

    /* Clean up */
    scratch->bestLNodes->clear();
    prunedList.clear();
    pool.clear();
}

static bool IsVisited(BlockNumber blk, HTAB* insertedNodeHash)
{
    bool found;
    hash_search(insertedNodeHash, &blk, HASH_ENTER, &found);
    return found;
}

void DiskAnnGraph::IterateToFixedPoint(BlockNumber blk, const uint32 Lsize, BlockNumber frozen,
                                       VectorList<Neighbor>* pool, bool searchInvocation)
{
    NeighborPriorityQueue* bestLNodes = scratch->bestLNodes;
    HTAB* insertedNodeHash = scratch->insertedNodeHash;
    bestLNodes->reserve(Lsize);

    if (!IsVisited(frozen, insertedNodeHash)) {
        hash_search(insertedNodeHash, &frozen, HASH_ENTER, NULL);
        float dis = graphStore->ComputeDistance(frozen, scratch->alignedQuery, scratch->sqrSum);
        Neighbor nn = Neighbor(frozen, dis);
        bestLNodes->insert(nn);
    }

    while (bestLNodes->has_unexpanded_node()) {
        VectorList<Neighbor> neighbors;
        Neighbor nbr;
        BlockNumber blockNumber;

        nbr = bestLNodes->closest_unexpanded();
        blockNumber = nbr.id;

        if (!searchInvocation) {
            pool->push_back(nbr);
        }

        neighbors = VectorList<Neighbor>();
        graphStore->GetNeighbors(blockNumber, &neighbors);
        if (!neighbors.empty()) {
            float distance;
            for (size_t i = 0; i < neighbors.size(); i++) {
                BlockNumber blkno = neighbors[i].id;
                distance = graphStore->ComputeDistance(blkno, scratch->alignedQuery, scratch->sqrSum);
                if (!IsVisited(blkno, insertedNodeHash)) {
                    hash_search(insertedNodeHash, &blkno, HASH_ENTER, NULL);
                    bestLNodes->insert(Neighbor(blkno, distance));
                }
            }
        }
        neighbors.clear();
    }
}

void DiskAnnGraph::PruneNeighbors(BlockNumber location, VectorList<Neighbor>* pool, VectorList<Neighbor>* prunedList)
{
    if (pool->size() == 0) {
        // if the pool is empty, behave like a noop
        prunedList->clear();
        return;
    }
    qsort(&((*pool)[0]), pool->size(), sizeof(Neighbor), CmpNeighborInfo);
    prunedList->reset();
    prunedList->reserve(OUTDEGREE);
    float alpha = INDEXING_ALPHA;
    OccludeList(location, pool, prunedList, alpha);
    Assert(prunedList->size() <= OUTDEGREE);

    if (saturateGraph && alpha > 1) {
        for (size_t i = 0; i < pool->size(); i++) {
            const auto& node = (*pool)[i];
            if (prunedList->size() >= OUTDEGREE)
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
    uint32_t degree = OUTDEGREE;
    uint32_t maxc = INDEXINGMAXC;

    if (pool->size() == 0)
        return;

    Assert(result->size() == 0);
    if (pool->size() > maxc)
        pool->resize(maxc);

    VectorList<float> occlude_factor = VectorList<float>();
    // occlude_list can be called with the same scratch more than once by
    // search_for_point_and_add_link through inter_insert.
    occlude_factor.resize(pool->size());
    errno_t rc = memset_s(&occlude_factor[0], sizeof(float) * occlude_factor.size(), 0.0f,
                          sizeof(float) * occlude_factor.size());
    securec_check(rc, "\0", "\0");

    float cur_alpha = 1;
    while (cur_alpha <= alpha && result->size() < degree) {
        // used for MIPS, where we store a value of eps in cur_alpha to
        // denote pruned out entries which we can skip in later rounds.
        float eps = cur_alpha + 0.01f;
        unsigned int idx = 0;
        for (auto iter = pool->begin(); result->size() < degree && iter != pool->end(); ++iter, ++idx) {
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
    occlude_factor.clear();
}

void DiskAnnGraph::InterInsert(BlockNumber blk, VectorList<Neighbor>* prunedList)
{
    VectorList<Neighbor>* pool = prunedList;
    ItemPointerData hctid;

    ItemPointerSetInvalid(&hctid);

    /* Save origin block node info into scratch */
    graphStore->GetVector(blk, scratch->alignedQuery, &(scratch->sqrSum), &hctid);

    for (size_t i = 0; i < pool->size(); ++i) {
        bool prune_needed = false;
        VectorList<Neighbor> copyNeighbors;
        VectorList<Neighbor> desPool;
        float distance;
        Neighbor nn;

        auto des = (*pool)[i];
        /* Skip existed neighbor node */
        if (graphStore->ContainsNeighbors(des.id, blk)) {
            continue;
        }

        /* Get destination block neighbors */
        desPool = VectorList<Neighbor>();
        graphStore->GetNeighbors(des.id, &desPool);

        /* Calculate distance between destination node and origin block node */
        distance = graphStore->ComputeDistance(des.id, scratch->alignedQuery, scratch->sqrSum);
        nn = Neighbor(blk, distance);
        if (!desPool.contains(nn)) {
            if (desPool.size() < OUTDEGREE) {
                DiskAnnEdgePage edgePage = (DiskAnnEdgePage)palloc(graphStore->m_edgeSize);
                GetEdgeTuple(edgePage, des.id, graphStore->m_rel, graphStore->m_nodeSize, graphStore->m_edgeSize);
                if (graphStore->NeighborExists(edgePage, blk) || edgePage->count >= OUTDEGREE) {
                    desPool.clear();
                    copyNeighbors.clear();
                    break;
                }
                graphStore->AddNeighbor(edgePage, blk, distance);
                graphStore->FlushEdge(edgePage, des.id);

                pfree(edgePage);
                prune_needed = false;
            } else {
                copyNeighbors = desPool;
                copyNeighbors.push_back(nn);
                prune_needed = true;
            }
        }

        if (prune_needed) {
            VectorList<Neighbor> dummyPool;
            VectorList<Neighbor> pruned;
            DiskAnnEdgePage edge;

            size_t reserveSize = (size_t)(ceil(GRAPH_SLACK_FACTOR * OUTDEGREE));
            dummyPool.reset();
            dummyPool.reserve(reserveSize);

            HASHCTL hctl;
            int ret = memset_s(&hctl, sizeof(HASHCTL), 0, sizeof(HASHCTL));
            securec_check(ret, "\0", "\0");
            hctl.keysize = sizeof(uint32_t);
            hctl.entrysize = sizeof(uint32_t);

            HTAB* dummyVisited = hash_create("Dummy visited hash table", reserveSize, &hctl, HASH_ELEM | HASH_CONTEXT);

            for (auto cur_nbr : copyNeighbors) {
                bool found = false;
                hash_search(dummyVisited, &cur_nbr.id, HASH_ENTER, &found);
                if (!found && cur_nbr.id != des.id) {
                    float dist = graphStore->GetDistance(des.id, cur_nbr.id);
                    Neighbor neighbor = Neighbor(cur_nbr.id, dist);
                    dummyPool.push_back(neighbor);
                }
            }
            hash_destroy(dummyVisited);

            PruneNeighbors(des.id, &dummyPool, &pruned);
            edge = (DiskAnnEdgePage)palloc(graphStore->m_edgeSize);
            GetEdgeTuple(edge, des.id, graphStore->m_rel, graphStore->m_nodeSize, graphStore->m_edgeSize);
            for (uint16 i = 0; i < edge->count; i++) {
                edge->nexts[i] = InvalidBlockNumber;
            }
            edge->count = 0;

            for (uint32 i = 0; i < (uint32)pruned.size(); i++) {
                graphStore->AddNeighbor(edge, pruned[i].id, pruned[i].distance);
            }
            graphStore->FlushEdge(edge, des.id);

            /* Clean up */
            pruned.clear();
            dummyPool.clear();
            pfree(edge);
        }

        /* Clean up */
        desPool.clear();
        copyNeighbors.clear();
    }
}

bool DiskAnnGraph::FindDuplicateNeighbor(NeighborPriorityQueue* bestLNodes, BlockNumber blk)
{
    for (size_t i = 0; i < bestLNodes->size(); i++) {
        Neighbor res = bestLNodes->_data[i];
        if (res.distance > DISKANN_DISTANCE_THRESHOLD) {
            break;
        }
        if (res.id != blk) {
            Buffer buf;
            Page page;

            buf = ReadBuffer(graphStore->m_rel, blk);
            LockBuffer(buf, BUFFER_LOCK_SHARE);

            page = BufferGetPage(buf);
            IndexTuple itup = DiskAnnPageGetIndexTuple(page);
            graphStore->AddDuplicateNeighbor(res.id, itup->t_tid);
            UnlockReleaseBuffer(buf);
            return true;
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

int DiskAnnComputePQTable(VectorArray samples, PQParams* params)
{
    // todo g_diskann_pq_func.DiskAnnComputePQTable(samples, params);
    return -1;
}

int DiskAnnComputeVectorPQCode(float* vector, const PQParams* params, uint8* pqCode)
{
    // todo g_diskann_pq_func.DiskAnnComputeVectorPQCode(vector, params, pqCode);
    return -1;
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

/*
 * Get the number of centroids for each subquantizer
 */
int DiskAnnGetPqKsub(Relation index)
{
    DiskAnnOptions* opts = (DiskAnnOptions*)index->rd_options;
    return opts ? opts->pqKsub : GENERIC_DEFAULT_PQ_KSUB;
}

/*
 * Flush PQ table into page during index building
 */
void DiskAnnFlushPQInfo(DiskAnnBuildState* buildstate)
{
    Relation index = buildstate->index;
    char* pqTable = buildstate->pqTable;
    uint16 pqTableNblk;
    uint16 pqDisTableNblk;
    uint32 pqTableSize;
    uint32 pqDisTableSize;

    DiskAnnGetPQInfoFromMetaPage(index, &pqTableNblk, &pqTableSize, &pqDisTableNblk, &pqDisTableSize);

    /* Flush pq table */
    DiskAnnFlushPQInfoInternal(index, pqTable, DISKANN_PQTABLE_START_BLKNO, pqTableNblk, pqTableSize);
}

/*
 * Get the info related to pqTable in metapage
 */
void DiskAnnGetPQInfoFromMetaPage(Relation index, uint16* pqTableNblk, uint32* pqTableSize, uint16* pqDisTableNblk,
                                  uint32* pqDisTableSize)
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
    if (pqDisTableNblk != NULL) {
        *pqDisTableNblk = metap->pqDisTableNblk;
    }
    if (pqDisTableSize != NULL) {
        *pqDisTableSize = metap->pqDisTableSize;
    }

    UnlockReleaseBuffer(buf);
}

void DiskAnnFlushPQInfoInternal(Relation index, char* table, BlockNumber startBlkno, uint32 nblks, uint64 totalSize)
{
    Buffer buf;
    Page page;
    PageHeader p;
    uint32 curFlushSize;

    for (uint32 i = 0; i < nblks; i++) {
        curFlushSize = (i == nblks - 1) ? (totalSize - i * PQTABLE_STORAGE_SIZE) : PQTABLE_STORAGE_SIZE;
        buf = ReadBufferExtended(index, MAIN_FORKNUM, P_NEW, RBM_NORMAL, NULL);
        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
        page = DiskAnnInitRegisterPage(index, buf);
        errno_t err =
            memcpy_s(PageGetContents(page), curFlushSize, table + i * PQTABLE_STORAGE_SIZE, curFlushSize);
        securec_check(err, "\0", "\0");
        p = (PageHeader)page;
        p->pd_lower += curFlushSize;
        MarkBufferDirty(buf);  // todo: add xlog for pq table
        UnlockReleaseBuffer(buf);
    }
}
