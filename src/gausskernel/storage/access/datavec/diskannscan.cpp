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
#include <cmath>
#include "access/datavec/diskann.h"
#include "access/datavec/utils.h"
#include "utils/guc.h"
#include "utils/memutils.h"

static void DiskAnnInvalidateOrderByDistance(IndexScanDesc scan)
{
    scan->xs_ann_orderby.valid = false;
    scan->xs_ann_orderby.isnull = true;
    scan->xs_ann_orderby.heap_recheck = false;
    scan->xs_ann_orderby.value = (Datum)0;
    scan->xs_ann_orderby.quality = ANN_ORDERBY_DISTANCE_UNAVAILABLE;
    scan->xs_ann_orderby.row_token = 0;
    ItemPointerSetInvalid(&scan->xs_ann_orderby.heap_tid);
}

/* Convert the already-computed full-vector rerank result to the SQL ORDER
 * BY distance scale.  No vector is loaded and no distance kernel is called
 * here. */
static bool DiskAnnConvertOrderByDistance(DiskAnnScanOpaque so, float rawDistance,
    DiskAnnDistanceSource source, double* outputDistance,
    AnnOrderByDistanceQuality* quality)
{
    IndexScanDesc scan = so->scan;

    if (scan == NULL || !scan->xs_ann_orderby.requested) {
        return false;
    }

    if (so->enablePQ || source != DISKANN_DISTANCE_FULL_INTERNAL) {
        ereport(ERROR,
            (errcode(ERRCODE_INTERNAL_ERROR),
             errmsg("DiskANN ORDER BY reuse received a distance with unsupported provenance")));
    }

    if (so->procinfo->fn_oid == L2_FUNC_OID && so->normprocinfo == NULL &&
        (scan->xs_ann_orderby.requested_mode & ANN_ORDERBY_REUSE_L2) != 0) {
        if (std::isnan(rawDistance) || rawDistance < 0.0f) {
            ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("DiskANN produced a negative squared L2 distance")));
        }
        *outputDistance = sqrt((double)rawDistance);
        *quality = ANN_ORDERBY_DISTANCE_EXACT_L2;
        scan->xs_ann_orderby.output_conversions++;
        return true;
    }

    if (so->procinfo->fn_oid == IP_FUNC_OID && so->normprocinfo != NULL &&
        (scan->xs_ann_orderby.requested_mode & ANN_ORDERBY_REUSE_COSINE) != 0) {
        if (!std::isfinite(rawDistance)) {
            ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("DiskANN produced a non-finite cosine distance")));
        }
        double distance = 1.0 + (double)rawDistance;
        *outputDistance = Min(Max(distance, 0.0), 2.0);
        *quality = ANN_ORDERBY_DISTANCE_APPROX_COSINE;
        scan->xs_ann_orderby.output_conversions++;
        return true;
    }

    return false;
}

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
    so->scanEpoch++;
    so->rowToken = 0;
    scan->xs_ann_orderby.scan_epoch = so->scanEpoch;
    DiskAnnInvalidateOrderByDistance(scan);
    /* Every rescan owns a new query and distance-publication epoch. Keep
     * vector storage reusable, but discard all old results and graph visits. */
    so->curpos = 0;
    so->candidates.resize(0);
    so->ncandidates = 0;
    so->curIterNum = 0;
    so->nexpextedCandidates = 0;
    so->value = (Datum)0;
    so->orderedBatch = false;
    so->queue->clear();
    blockhash_reset(so->blocks);
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
    so->scan = scan;
    so->tmpCtx = AllocSetContextCreate(CurrentMemoryContext, "DiskANN scan temporary context", ALLOCSET_DEFAULT_SIZES);
    so->nodeSize = metapage.nodeSize;
    so->nexpextedCandidates = 0;
    so->ncandidates = 0;
    so->curpos = 0;
    so->delSearch = false;
    so->orderedBatch = false;
    so->curIterNum = 0;
    so->frozenBlks = VectorList<BlockNumber>();
    so->candidates = VectorList<DiskAnnCandidatesData>();
    so->enablePQ = metapage.enablePQ;
    so->scanEpoch = 0;
    so->rowToken = 0;
    so->procinfo = index_getprocinfo(index, 1, DISKANN_DISTANCE_PROC);
    DiskPQParams *tmpDiskPQParams = InitDiskPQParamsOnDisk(index, so->procinfo, metapage.dimensions, so->enablePQ);
    if (tmpDiskPQParams != NULL) {
        so->params = *tmpDiskPQParams;
    }
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

    /* The AM owns ORDER BY output storage even for ordinary heap scans.
     * This is the same descriptor contract used by GiST, independent of
     * returning index tuples or supporting INCLUDE attributes. */
    if (norderbys > 0) {
        scan->xs_orderbyvals = (Datum*)palloc0(sizeof(Datum) * norderbys);
        scan->xs_orderbynulls = (bool*)palloc0(sizeof(bool) * norderbys);
    }
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
 * A heap tuple is one logical index result even if an old-format DiskANN
 * index happens to retain its TID in more than one zero-distance node.  This
 * can occur when a duplicate bucket reaches DISKANN_HEAPTIDS: the overflow
 * tuple must remain in a newly linked node, while a legacy frozen point may
 * already carry the same sampled TID.  Node identity therefore must not be
 * part of result de-duplication.
 */
static bool DiskAnnCandidatesContainHeapTid(VectorList<DiskAnnCandidatesData>* candidates,
                                            ItemPointerData* heapTid)
{
    for (size_t i = 0; i < candidates->size(); ++i) {
        if (ItemPointerEquals(&(*candidates)[i].heapTid, heapTid)) {
            return true;
        }
    }
    return false;
}

/* A batch ends at the AM boundary, not at LIMIT: the executor must see
 * every visible, qualifying member before choosing which rows to return.
 * Only the executor may request another batch, and only before publishing
 * the sorted result. This also prevents cross-batch order inversions. */
bool diskann_advance_ordered_batch(IndexScanDesc scan)
{
    DiskAnnScanOpaque so = (DiskAnnScanOpaque)scan->opaque;
    Assert(so->orderedBatch && so->curpos == so->ncandidates);
    if (so->curIterNum >= MAX_SEARCH_ITERATION || so->nfrozen == 0 ||
        (scan->orderByData[0].sk_flags & SK_ISNULL)) {
        return false;
    }
    MemoryContext oldCtx = MemoryContextSwitchTo(so->tmpCtx);
    Datum query = GetScanValue(scan);
    so->value = query;
    so->nexpextedCandidates = so->curIterNum == 0 ? (uint32)DEFAULT_CADIDATES_NUMBER :
        so->nexpextedCandidates * SEARCH_DOUBLE;
    diskannsearch(so);
    so->ncandidates = (uint32)so->candidates.size();
    so->curIterNum++;
    if (so->queue->size() < so->nexpextedCandidates) {
        /* The beam did not fill: all reachable vertices were visited. */
        so->curIterNum = MAX_SEARCH_ITERATION;
    }
    if (so->normprocinfo != NULL) {
        pfree(DatumGetPointer(query));
    }
    so->value = (Datum)0;
    MemoryContextSwitchTo(oldCtx);
    /* No new output at a narrow beam does not prove graph exhaustion:
     * dead/navigation-only vertices can fill the beam. The caller retries
     * with a wider beam until the iteration cap, or enough qualified rows. */
    return true;
}

/* Old indexes may already contain a TID recycled before VACUUM maintenance
 * was implemented. Even full TID equality is not a row-version proof.
 * Compare the immutable stored vector with the current visible row before
 * trusting its cached distance. This is a byte comparison, not another
 * distance evaluation. Normalized opclasses compare normalized vectors. */
bool diskann_orderby_vector_matches(IndexScanDesc scan, Datum value)
{
    DiskAnnScanOpaque so = (DiskAnnScanOpaque)scan->opaque;
    Assert(so->curpos > 0 && so->curpos <= so->ncandidates);
    Datum heapValue = PointerGetDatum(PG_DETOAST_DATUM(value));
    Datum normalized = so->normprocinfo != NULL ?
        DirectFunctionCall1Coll(l2_normalize, so->collation, heapValue) : heapValue;
    Vector* heapVector = DatumGetVector(normalized);
    Buffer buf = ReadBuffer(so->rel, so->candidates[so->curpos - 1].id);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    bool isnull = false;
    Datum key = index_getattr(DiskAnnPageGetIndexTuple(BufferGetPage(buf)), 1,
        RelationGetDescr(so->rel), &isnull);
    bool matches = false;
    if (!isnull) {
        Vector* stored = (Vector*)PG_DETOAST_DATUM(key);
        matches = heapVector->dim == stored->dim &&
            memcmp(heapVector->x, stored->x, stored->dim * sizeof(float)) == 0;
        if ((Pointer)stored != DatumGetPointer(key)) {
            pfree(stored);
        }
    }
    UnlockReleaseBuffer(buf);
    if (normalized != heapValue) {
        pfree(DatumGetPointer(normalized));
    }
    if (heapValue != value) {
        pfree(DatumGetPointer(heapValue));
    }
    return matches;
}

/*
* Fetch the next tuple in the given scan
*/
bool diskanngettuple_internal(IndexScanDesc scan, ScanDirection dir)
{
    DiskAnnScanOpaque so = (DiskAnnScanOpaque)scan->opaque;
    MemoryContext oldCtx = MemoryContextSwitchTo(so->tmpCtx);

    /* Never let a rejected, invisible or exhausted candidate inherit the
     * previous row's value. */
    DiskAnnInvalidateOrderByDistance(scan);

    Datum value;

    /* Safety check */
    if (scan->orderByData == NULL)
        elog(ERROR, "cannot scan diskann index without order");

    if (scan->orderByData[0].sk_flags & SK_ISNULL) {
        MemoryContextSwitchTo(oldCtx);
        return false;
    }
    /* Ordered batches already evaluated the query and materialized their
     * candidates. Do not normalize/allocate again for each returned TID. */
    value = so->orderedBatch ? (Datum)0 : GetScanValue(scan);
    so->value = value;

    if (so->orderedBatch) {
        /* Explicit batch advancement belongs to the executor. */
    } else if ((so->curpos == 0 || so->curpos == so->ncandidates) && so->curIterNum < MAX_SEARCH_ITERATION) {
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
        DiskAnnCandidatesData* cand = &so->candidates[so->curpos];
        scan->xs_ctup.t_self = cand->heapTid;
        scan->xs_recheck = false;
        /* [P1-7] exact distance for ORDER BY consumers (non-lossy rerank) */
        if (scan->numberOfOrderBys > 0) {
            if (cand->outputDistanceReady && cand->heapRecheck) {
                /* No exact value or lower bound is known for this merged
                 * TID until the visible heap row has been evaluated. */
                scan->xs_orderbyvals[0] = (Datum)0;
                scan->xs_orderbynulls[0] = true;
            } else if (cand->outputDistanceReady &&
                cand->outputQuality == ANN_ORDERBY_DISTANCE_EXACT_L2) {
                scan->xs_orderbyvals[0] = Float8GetDatum(cand->outputDistance);
                scan->xs_orderbynulls[0] = false;
            } else if (cand->outputDistanceReady &&
                       cand->outputQuality == ANN_ORDERBY_DISTANCE_APPROX_COSINE) {
                /* Approximate cosine is intentionally confined to the ANN
                 * channel; it is neither an exact SQL value nor a documented
                 * lower bound for generic IndexScan reordering. */
                scan->xs_orderbyvals[0] = (Datum)0;
                scan->xs_orderbynulls[0] = true;
            } else {
                /* Preserve the default-off behavior until the legacy
                 * DiskANN ORDER BY value contract is changed independently. */
                scan->xs_orderbyvals[0] = Float8GetDatum(cand->distance);
                scan->xs_orderbynulls[0] = false;
            }
            scan->xs_recheckorderby = false;
        }

        if (scan->xs_ann_orderby.requested && !cand->outputDistanceReady) {
            ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("DiskANN did not prepare the requested ORDER BY distance")));
        }

        if (scan->xs_ann_orderby.requested) {
            if (cand->distanceSource != DISKANN_DISTANCE_FULL_INTERNAL ||
                cand->outputEpoch != so->scanEpoch ||
                (cand->heapRecheck && cand->outputQuality != ANN_ORDERBY_DISTANCE_EXACT_L2) ||
                (cand->outputQuality == ANN_ORDERBY_DISTANCE_EXACT_L2 &&
                 (scan->xs_ann_orderby.requested_mode & ANN_ORDERBY_REUSE_L2) == 0) ||
                (cand->outputQuality == ANN_ORDERBY_DISTANCE_APPROX_COSINE &&
                 (scan->xs_ann_orderby.requested_mode & ANN_ORDERBY_REUSE_COSINE) == 0)) {
                ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("DiskANN ORDER BY candidate metadata is stale or incompatible")));
            }
            scan->xs_ann_orderby.value = Float8GetDatum(cand->outputDistance);
            scan->xs_ann_orderby.isnull = false;
            scan->xs_ann_orderby.valid = true;
            scan->xs_ann_orderby.quality = cand->outputQuality;
            scan->xs_ann_orderby.heap_recheck = cand->heapRecheck;
            scan->xs_ann_orderby.scan_epoch = so->scanEpoch;
            scan->xs_ann_orderby.row_token = ++so->rowToken;
            scan->xs_ann_orderby.heap_tid = cand->heapTid;
            scan->xs_ann_orderby.publications++;
        }

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
        /* Refresh identity together with the full distance. Search-time
         * TIDs can be stale, and distance-threshold deduplication does not
         * prove that every TID has the node's stored vector. */
        queue->_data[i].distance = GetDistance(so, queue->_data[i].id,
            queue->_data[i].heaptids, &queue->_data[i].heaptidsLength,
            &queue->_data[i].vectorTid);
        so->scan->xs_ann_orderby.full_distance_evaluations++;
    }

    if (queue->size() > 1) {
        qsort(&(queue->_data[0]), queue->size(), sizeof(Neighbor), CmpNeighborInfo);
    }

    uint32_t pos = 0;
    bool hasDuplicates = false;

    for (size_t i = 0; i < queue->size(); ++i) {
        int count = 0;
        if (so->delSearch && queue->_data[i].distance > DISKANN_DISTANCE_THRESHOLD) {
            continue;
        }

        bool hasNewTuple = false;
        for (size_t j = 0; j < queue->_data[i].heaptidsLength; j++) {
            if (!DiskAnnCandidatesContainHeapTid(searchedResults, &queue->_data[i].heaptids[j])) {
                hasNewTuple = true;
                break;
            }
        }
        if (!hasNewTuple) {
            continue;
        }

        double outputDistance = 0.0;
        AnnOrderByDistanceQuality outputQuality = ANN_ORDERBY_DISTANCE_UNAVAILABLE;
        DiskAnnDistanceSource distanceSource = DISKANN_DISTANCE_FULL_INTERNAL;
        bool outputDistanceReady = DiskAnnConvertOrderByDistance(
            so, queue->_data[i].distance, distanceSource,
            &outputDistance, &outputQuality);

        for (size_t j = 0; j < queue->_data[i].heaptidsLength; j++) {
            /* [P1-7] carry the exact distance for ORDER BY consumers */
            DiskAnnCandidatesData candidate = {.id = queue->_data[i].id,
                                               .heapTid = queue->_data[i].heaptids[j],
                                               .distance = (double)queue->_data[i].distance,
                                               .outputDistance = outputDistance,
                                               .outputDistanceReady = outputDistanceReady,
                                               .heapRecheck = outputDistanceReady &&
                                                   outputQuality == ANN_ORDERBY_DISTANCE_EXACT_L2 &&
                                                   !ItemPointerEquals(&queue->_data[i].vectorTid,
                                                                      &queue->_data[i].heaptids[j]),
                                               .outputQuality = outputQuality,
                                               .distanceSource = distanceSource,
                                               .outputEpoch = outputDistanceReady ? so->scanEpoch : 0};
            if (!DiskAnnCandidatesContainHeapTid(searchedResults, &candidate.heapTid)) {
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
    DiskPQParams* params = &so->params;
    bool enablePQ = so->enablePQ;
    float* queryPQDistance = NULL;
    bool found = false;
    float *query = DatumGetVector(so->value)->x;

    /* A wider beam restarts at the frozen entry points. Visits from the
     * preceding beam cannot suppress those entry points or their edges. */
    blockhash_reset(blocks);
    queue->reserve(so->nexpextedCandidates);

    VectorList<BlockNumber> *frozenBlknos = &(so->frozenBlks);

    if (enablePQ) {
        size_t tmpSize = sizeof(float) * params->pqChunks * GENERIC_DEFAULT_PQ_KSUB;
        queryPQDistance = (float*)palloc(tmpSize);
        int ret = GetPQDistanceTable((char *)query, params, queryPQDistance);
        if (ret != 0) {
            ereport(WARNING, (ret, errmsg("GetPQDistanceTable failed.")));
        }
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
                uint8_t *curNeighborPQCode = nbIter->curNbtup->pqcode;
                int ret = GetPQDistance(curNeighborPQCode, params, queryPQDistance, distance);
                if (ret != 0) {
                    ereport(WARNING, (ret, errmsg("GetPQDistanceTable failed.")));
                }
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

float GetDistance(DiskAnnScanOpaque so, BlockNumber blk, ItemPointer heaptids,
                  uint8* heaptidsLength, ItemPointer vectorTid)
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
        if (tup->heaptidsLength > DISKANN_HEAPTIDS) {
            ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                errmsg("DiskANN node has an invalid heap TID count")));
        }
        *heaptidsLength = tup->heaptidsLength;
        for (int i = 0; i < tup->heaptidsLength; i++) {
            heaptids[i] = tup->heaptids[i];
        }
    }
    if (vectorTid != NULL) {
        *vectorTid = itup->t_tid;
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
