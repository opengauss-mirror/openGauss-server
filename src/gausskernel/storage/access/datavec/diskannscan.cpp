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
* diskannscan.cpp
*
* IDENTIFICATION
*        src/gausskernel/storage/access/datavec/diskannscan.cpp
*
* -------------------------------------------------------------------------
*/

#include <algorithm>
#include "access/datavec/diskann.h"
#include "utils/memutils.h"

static Datum GetScanValue(IndexScanDesc scan)
{
    DiskAnnScanOpaque so = (DiskAnnScanOpaque)scan->opaque;
    Datum value;

    if (scan->orderByData->sk_flags & SK_ISNULL) {
        value = PointerGetDatum(NULL);
    } else {
        value = scan->orderByData->sk_argument;

        /* Value should not be compressed or toasted */
        Assert(!VARATT_IS_COMPRESSED(DatumGetPointer(value)));
        Assert(!VARATT_IS_EXTENDED(DatumGetPointer(value)));

        /* Normalize if needed */
        if (so->normprocinfo != NULL) {
            value = DirectFunctionCall1Coll(l2_normalize, so->collation, value);
        }
    }

    return value;
}

void diskannrescan_internal(IndexScanDesc scan, ScanKey keys, int nkeys, ScanKey orderbys, int norderbys)
{
    errno_t rc = 0;
    if (keys && scan->numberOfKeys > 0) {
        size_t keySize = (uint32_t)scan->numberOfKeys * sizeof(ScanKeyData);
        rc = memmove_s(scan->keyData, keySize, keys, keySize);
        securec_check(rc, "\0", "\0");
    }
    if (orderbys && scan->numberOfOrderBys > 0) {
        size_t orderBySize = (uint32_t)scan->numberOfOrderBys * sizeof(ScanKeyData);
        rc = memmove_s(scan->orderByData, orderBySize, orderbys, orderBySize);
        securec_check(rc, "\0", "\0");
    }
    DiskAnnScanOpaque so = (DiskAnnScanOpaque)scan->opaque;
    so->curpos = 0;
}

IndexScanDesc diskannbeginscan_internal(Relation index, int nkeys, int norderbys)
{
    IndexScanDesc scan;
    DiskAnnScanOpaque so;
    DiskAnnMetaPageData metapage;

    DiskANNGetMetaPageInfo(index, &metapage);

    scan = RelationGetIndexScan(index, nkeys, norderbys);
    so = (DiskAnnScanOpaque)palloc(sizeof(DiskAnnScanOpaqueData));
    so->rel = index;
    so->tmpCtx = AllocSetContextCreate(CurrentMemoryContext, "DiskANN scan temporary context", ALLOCSET_DEFAULT_SIZES);
    so->nodeSize = metapage.nodeSize;
    so->nexpextedCandidates = 0;
    so->ncandidates = 0;
    so->curpos = 0;
    so->delSearch = false;
    so->curIterNum = 0;
    so->frozenBlks = VectorList<BlockNumber>();
    so->candidates = VectorList<DiskAnnCandidatesData>();
    so->enablePQ = false;

    so->procinfo = index_getprocinfo(index, 1, DISKANN_DISTANCE_PROC);
    so->normprocinfo = NULL;
    if (OidIsValid(index_getprocid(index, 1, DISKANN_NORM_PROC)))
        so->normprocinfo = index_getprocinfo(index, 1, DISKANN_NORM_PROC);
    so->collation = index->rd_indcollation[0];

    so->nfrozen = metapage.nfrozen;
    for (uint32_t i = 0; i < metapage.nfrozen; i++) {
        so->frozenBlks.push_back(metapage.frozenBlkno[i]);
    }

    MemoryContext oldCtx = MemoryContextSwitchTo(so->tmpCtx);
    so->blocks = blockhash_create(CurrentMemoryContext, VAMANA_CAP, NULL);
    so->queue = (NeighborPriorityQueue *)palloc(sizeof(NeighborPriorityQueue));
    so->queue->_data = VectorList<Neighbor>();
    MemoryContextSwitchTo(oldCtx);

    scan->opaque = so;

    return scan;
}

void diskannendscan_internal(IndexScanDesc scan)
{
    DiskAnnScanOpaque so = (DiskAnnScanOpaque)scan->opaque;
    so->candidates.clear();
    so->frozenBlks.clear();
    MemoryContextDelete(so->tmpCtx);
    pfree(so);
    scan->opaque = NULL;
}

/*
* Fetch the next tuple in the given scan
*/
bool diskanngettuple_internal(IndexScanDesc scan, ScanDirection dir)
{
    DiskAnnScanOpaque so = (DiskAnnScanOpaque)scan->opaque;
    MemoryContext oldCtx = MemoryContextSwitchTo(so->tmpCtx);

    Datum value;

    /* Safety check */
    if (scan->orderByData == NULL)
        elog(ERROR, "cannot scan diskann index without order");

    value = GetScanValue(scan);
    so->value = value;

    if ((so->curpos == 0 || so->curpos == so->ncandidates) && so->curIterNum < MAX_SEARCH_ITERATION) {
        if (so->curIterNum == 0) {
            so->nexpextedCandidates = (uint32_t)DEFAULT_CADIDATES_NUMBER;
        } else {
            so->nexpextedCandidates *= SEARCH_DOUBLE;
        }

        diskannsearch(so);
        so->ncandidates = (uint32_t)so->candidates.size();

        so->curIterNum++;
    }

    bool found = false;
    if (so->curpos < so->ncandidates) {
        scan->xs_ctup.t_self = so->candidates[so->curpos].heapTid;
        scan->xs_recheck = false;
        so->curpos++;
        found = true;
    }

    MemoryContextSwitchTo(oldCtx);
    PG_RETURN_BOOL(found);
}

void diskannsearch(DiskAnnScanOpaque so)
{
    NeighborPriorityQueue *queue = so->queue;
    VectorList<DiskAnnCandidatesData> *searchedResults = &(so->candidates);
    uint32_t expectedSize = so->nexpextedCandidates;
    float *query = DatumGetVector(so->value)->x;
    int16 dim = DatumGetVector(so->value)->dim;

    queue->clear();
    so->querySqrSum = VectorSquareNorm(query, dim);

    SearchFixedPoint(so);

    for (size_t i = 0; i < queue->size(); i++) {
        queue->_data[i].distance = GetDistance(so, queue->_data[i].id, NULL, NULL);
    }

    qsort(&(queue->_data[0]), queue->size(), sizeof(Neighbor), CmpNeighborInfo);

    uint32_t pos = 0;
    bool hasDuplicates = false;

    for (size_t i = 0; i < queue->size(); ++i) {
        int count = 0;
        if (so->delSearch && queue->_data[i].distance > DISKANN_DISTANCE_THRESHOLD) {
            continue;
        }

        for (size_t j = 0; j < queue->_data[i].heaptidsLength; j++) {
            DiskAnnCandidatesData candidate = {.id = queue->_data[i].id, .heapTid = queue->_data[i].heaptids[j]};
            if (!searchedResults->contains(candidate)) {
                searchedResults->push_back(candidate);
            }
        }
    }

    if (hasDuplicates) {
        size_t start = searchedResults->size() - pos;
        qsort(&((*searchedResults)[start]), (size_t)pos, sizeof(DiskAnnIdxScanData), CmpIdxScanData);
    }
}

void SearchFixedPoint(DiskAnnScanOpaque so)
{
    NeighborPriorityQueue *queue = so->queue;
    blockhash_hash *blocks = so->blocks;
    PQParams* params = &so->params;
    bool enablePQ = so->enablePQ;
    float* queryPQDistance = NULL;
    bool found = false;
    float *query = DatumGetVector(so->value)->x;

    queue->reserve(so->nexpextedCandidates);

    VectorList<BlockNumber> *frozenBlknos = &(so->frozenBlks);

    if (enablePQ) {
        size_t tmpSize = 0;
        queryPQDistance = (float*)palloc(tmpSize);
        DiskAnnGetPQDistanceTable(query, params, queryPQDistance);
    }

    // Add frozen nodes to the priority queue
    for (size_t i = 0; i < frozenBlknos->size(); i++) {
        BlockNumber master = (*frozenBlknos)[i];
        blockhash_insert(blocks, master, &found);
        if (found) {
            continue;
        }
        Neighbor nn;
        float distance = GetDistance(so, (*frozenBlknos)[i], nn.heaptids, &nn.heaptidsLength);
        nn.id = master;
        nn.distance = distance;
        nn.expanded = false;

        queue->insert(nn);
    }

    // Search the graph
    while (queue->has_unexpanded_node()) {
        size_t idx;
        Neighbor *n = GetNextNeighbor(queue, &idx);
        BlockNumber master = n->id;

        bool deleted = IsMarkDeleted(so->rel, n->id);
        if (deleted) {
            queue->remove(idx);
        }

        uint32_t infoFlag = DISKANN_NEIGHBOR_VECTOR | DISKANN_NEIGHBOR_BLKNO;
        VamanaVertexNbIterator *nbIter = CreateIterator(n->id, so, infoFlag);

        for (nbIter->begin(); !nbIter->isFinished(); nbIter->next()) {
            DiskAnnNeighborInfoT nbinfo = {0};
            BlockNumber nbVertex = nbIter->getCurNeighborInfo(&nbinfo);
            Vector *nVec = (Vector *)DatumGetPointer(nbinfo.vector);
            Assert(DatumGetVector(so->value)->dim == nVec->dim);
            float distance = 0;
            if (!enablePQ) {
                distance = DatumGetFloat8(FunctionCall2Coll(so->procinfo, so->collation, so->value, PointerGetDatum(nVec)));
            } else {
                uint8_t *curNeighborPQCode = GetCurNeighborPQCode();
                DiskAnnGetPQDistance(curNeighborPQCode, params, queryPQDistance, &distance);
            }
            if (nbinfo.freeVector) {
                pfree(DatumGetPointer(nbinfo.vector));
            }
            Neighbor nn = Neighbor(nbVertex, distance);
            nn.heaptidsLength = nbIter->curNbtup->heaptidsLength;
            for (int i = 0; i < nn.heaptidsLength; i++) {
                nn.heaptids[i] = nbIter->curNbtup->heaptids[i];
            }

            queue->insert(nn);
        }
        ReleaseIterator(nbIter);
    }
}

float GetDistance(DiskAnnScanOpaque so, BlockNumber blk, ItemPointer heaptids, uint8* heaptidsLength)
{
    Buffer buf = ReadBuffer(so->rel, blk);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    Page page = BufferGetPage(buf);
    IndexTuple itup = DiskAnnPageGetIndexTuple(page);
    TupleDesc tupdesc = RelationGetDescr(so->rel);
    bool isnull;
    Datum src = index_getattr(itup, 1, tupdesc, &isnull);
    Datum dst = PointerGetDatum(PG_DETOAST_DATUM(src));
    Vector* vec = (Vector*)DatumGetPointer(dst);
    int16 dim = DatumGetVector(so->value)->dim;
    Assert(dim == vec->dim);
    DiskAnnNodePage tup = DiskAnnPageGetNode(DiskAnnPageGetIndexTuple(page));
    float *query = DatumGetVector(so->value)->x;
    if (heaptids != NULL) {
        *heaptidsLength = tup->heaptidsLength;
        for (int i = 0; i < DISKANN_HEAPTIDS; i++) {
            if (!ItemPointerIsValid(&tup->heaptids[i])) {
                break;
            }
            heaptids[i] = tup->heaptids[i];
        }
    }

    float distance = DatumGetFloat8(FunctionCall2Coll(so->procinfo, so->collation, so->value, PointerGetDatum(vec)));
    if (DatumGetPointer(dst) != DatumGetPointer(src)) {
        pfree(DatumGetPointer(dst));
    }
    UnlockReleaseBuffer(buf);
    return distance;
}

Neighbor *GetNextNeighbor(NeighborPriorityQueue* queue, size_t *idx)
{
    queue->_data[queue->_cur].expanded = true;
    size_t pre = queue->_cur;
    while (queue->_cur < queue->_size && queue->_data[queue->_cur].expanded) {
        queue->_cur++;
    }
    *idx = pre;
    return &queue->_data[pre];
}

VamanaVertexNbIterator *CreateIterator(BlockNumber blk, DiskAnnScanOpaque so, uint32_t infoFlag)
{
    VamanaVertexNbIterator *nbIter = (VamanaVertexNbIterator *)palloc(sizeof(VamanaVertexNbIterator));
    nbIter->rel = so->rel;
    nbIter->nodeSize = so->nodeSize;
    nbIter->blocks = so->blocks;
    nbIter->infoFlag = infoFlag;
    nbIter->curNeighborId = DISKANN_MAX_DEGREE;
    nbIter->skipVisited = true;
    nbIter->curNeighborBuf = InvalidBuffer;
    nbIter->curItup = NULL;
    nbIter->curNbtup = NULL;
    nbIter->nodeBuf = ReadBuffer(so->rel, blk);
    LockBuffer(nbIter->nodeBuf, BUFFER_LOCK_SHARE);
    Page cpage = BufferGetPage(nbIter->nodeBuf);
    DiskAnnNodePage tup_src = DiskAnnPageGetNode(DiskAnnPageGetIndexTuple(cpage));
    if (DiskAnnNodeIsSlave(tup_src->tag)) {
        Assert(tup_src->master != InvalidBlockNumber);
        Buffer masterBuf = ReadBuffer(so->rel, tup_src->master);
        LockBuffer(masterBuf, BUFFER_LOCK_SHARE);
        Page masterPage = BufferGetPage(masterBuf);
        tup_src = DiskAnnPageGetNode(DiskAnnPageGetIndexTuple(masterPage));
        UnlockReleaseBuffer(masterBuf);
        Assert(DiskAnnNodeIsMaster(tup_src->tag));
    }
    nbIter->edges = (DiskAnnEdgePage)((uint8_t *)tup_src + nbIter->nodeSize);

    return nbIter;
}

void ReleaseIterator(VamanaVertexNbIterator *iter)
{
    if (iter->nodeBuf != InvalidBuffer) {
        UnlockReleaseBuffer(iter->nodeBuf);
    }
    pfree(iter);
}

DiskAnnAliveSlaveIterator *CreateSlaveIterator(Relation index, BlockNumber vertex, float *query, uint16_t dim, double sqrsum)
{
    DiskAnnAliveSlaveIterator *iter = (DiskAnnAliveSlaveIterator *)palloc(sizeof(DiskAnnAliveSlaveIterator));
    iter->index = index;
    iter->master = vertex;
    iter->dim = dim;
    iter->curVertex = InvalidBlockNumber;
    iter->nextVertex = InvalidBlockNumber;
    return iter;
}

uint8_t *GetCurNeighborPQCode()
{
}
