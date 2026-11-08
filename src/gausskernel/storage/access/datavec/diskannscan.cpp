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

/* Query-local filtering uses one explicit beam search in phases 1/2.
 * Its navigation queue includes rejected nodes; its result queue does not.
 * All page data used by SQL callbacks is copied before releasing locks. */
struct DiskAnnQueryNodeCopy {
    BlockNumber id;
    ItemPointerData owner_tid;
    Datum key;
    float distance;
    uint8 members;
    ItemPointerData roots[DISKANN_HEAPTIDS];
    ItemPointerData visible[DISKANN_HEAPTIDS];
    bool dead;
    uint16 edges;
    BlockNumber nexts[DISKANN_MAX_DEGREE];
};

struct DiskAnnQueryScanState {
    const DiskAnnFilterSpec* spec; /* borrowed until ClearFilter */
    MemoryContext context;
    NeighborPriorityQueue navigation;
    blockhash_hash* visited;
    DiskAnnQueryNodeCopy* results;
    int result_count;
    int result_pos;
    int member_pos;
    bool searching;
    bool done;
    bool current_valid;
    DiskAnnQueryMember current;
};

static void DiskAnnQueryCheckMemory(DiskAnnScanOpaque so)
{
    DiskAnnQueryScanState* state = so->queryScan;
    Size owned = GetMemoryTotalSpace(so->tmpCtx, true) +
        sizeof(DiskAnnScanOpaqueData) +
        so->frozenBlks.myCapacity * sizeof(BlockNumber) +
        so->candidates.myCapacity * sizeof(DiskAnnCandidatesData);
    if (state->spec->owned_memory != NULL)
        owned += state->spec->owned_memory(state->spec->context);
    state->spec->counters->memory_peak = Max(state->spec->counters->memory_peak, owned);
    if (owned > state->spec->memory_limit)
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
            errmsg("DiskANN filtering exceeded diskann_filter_work_mem"),
            errdetail("Query-owned search and filter memory: %zu bytes; limit: %zu bytes.",
                owned, state->spec->memory_limit)));
}

/* Returned vectors, member identities and edges contain no page pointers. */
static DiskAnnQueryNodeCopy DiskAnnQueryCopyNode(DiskAnnScanOpaque so, BlockNumber id, bool edges,
    bool measure_distance = true)
{
    DiskAnnQueryNodeCopy node = {};
    node.id = id;
    Buffer buffer = ReadBuffer(so->rel, id);
    LockBuffer(buffer, BUFFER_LOCK_SHARE);
    Page page = BufferGetPage(buffer);
    if (DiskAnnPageGetOpaque(page)->pageId != DISKANN_PAGE_ID ||
        PageGetMaxOffsetNumber(page) < FirstOffsetNumber)
        ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
            errmsg("invalid DiskANN node page")));
    ItemId item = PageGetItemId(page, FirstOffsetNumber);
    IndexTuple tuple = DiskAnnPageGetIndexTuple(page);
    node.owner_tid = tuple->t_tid;
    DiskAnnNodePage disk_node = DiskAnnPageGetNode(tuple);
    if (disk_node->heaptidsLength > DISKANN_HEAPTIDS)
        ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
            errmsg("invalid DiskANN member count")));
    node.dead = ItemIdIsDead(item);
    node.members = node.dead ? 0 : disk_node->heaptidsLength;
    for (int i = 0; i < node.members; i++)
        node.roots[i] = disk_node->heaptids[i];
    bool isnull = false;
    Datum key = index_getattr(tuple, 1, RelationGetDescr(so->rel), &isnull);
    /* Expansion only consumes copied edges. Discovery and admission copy
     * the vector; avoid allocating and measuring it again for expansion. */
    if (!isnull && !edges)
        node.key = PointerGetDatum(PG_DETOAST_DATUM_COPY(key));
    if (isnull && node.members != 0)
        ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
            errmsg("DiskANN member has a NULL vector key")));
    BlockNumber master = DiskAnnNodeIsSlave(disk_node->tag) ? disk_node->master : InvalidBlockNumber;
    if (edges && master == InvalidBlockNumber) {
        DiskAnnEdgePage neighbours = (DiskAnnEdgePage)((char*)disk_node + so->nodeSize);
        if (neighbours->count > DISKANN_MAX_DEGREE)
            ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                errmsg("invalid DiskANN neighbour count")));
        node.edges = neighbours->count;
        for (int i = 0; i < node.edges; i++)
            node.nexts[i] = neighbours->nexts[i];
    }
    UnlockReleaseBuffer(buffer);
    if (edges && master != InvalidBlockNumber) {
        /* Copy a slave's navigation from its master under a fresh pin/lock. */
        buffer = ReadBuffer(so->rel, master);
        LockBuffer(buffer, BUFFER_LOCK_SHARE);
        page = BufferGetPage(buffer);
        DiskAnnNodePage master_node = DiskAnnPageGetNode(DiskAnnPageGetIndexTuple(page));
        if (!DiskAnnNodeIsMaster(master_node->tag))
            ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                errmsg("invalid DiskANN master identity")));
        DiskAnnEdgePage neighbours = (DiskAnnEdgePage)((char*)master_node + so->nodeSize);
        if (neighbours->count > DISKANN_MAX_DEGREE)
            ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                errmsg("invalid DiskANN neighbour count")));
        node.edges = neighbours->count;
        for (int i = 0; i < node.edges; i++)
            node.nexts[i] = neighbours->nexts[i];
        UnlockReleaseBuffer(buffer);
    }
    /* Distance kernels, interrupts and SQL all run after releasing locks. */
    CHECK_FOR_INTERRUPTS();
    if (node.key != (Datum)0) {
        Vector* vector = DatumGetVector(node.key);
        if (vector->dim != DatumGetVector(so->value)->dim)
            ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                errmsg("DiskANN vector dimension changed")));
        if (measure_distance)
            node.distance = DatumGetFloat8(FunctionCall2Coll(so->procinfo,
                so->collation, so->value, node.key));
    }
    return node;
}

static void DiskAnnQueryAdmit(DiskAnnScanOpaque so, DiskAnnQueryNodeCopy* node)
{
    DiskAnnQueryScanState* state = so->queryScan;
    int admitted = 0;
    for (int i = 0; i < node->members; i++) {
        CHECK_FOR_INTERRUPTS();
        state->spec->counters->candidate_members++;
        if (!ItemPointerIsValid(&node->roots[i]))
            ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                errmsg("DiskANN node contains an invalid member TID")));
        DiskAnnQueryMember member = {};
        member.node_block = node->id;
        member.owner_tid = node->owner_tid;
        member.root_tid = node->roots[i];
        member.key = node->key;
        member.normalized = so->normprocinfo != NULL;
        ItemPointerData visible;
        ItemPointerSetInvalid(&visible);
        if (state->spec->qualify(state->spec->context, &member, &visible)) {
            node->roots[admitted] = node->roots[i];
            node->visible[admitted++] = visible;
        }
        DiskAnnQueryCheckMemory(so);
    }
    node->members = admitted;
    int capacity = state->spec->search_nodes;
    /* POST admits every retained navigation group. It does not trim or
     * rank that set again: the ordinary SQL Sort ranks snapshot-visible
     * heap vectors. Fresh keys/identities are still copied and checked. */
    int pos = state->spec->mode == ANN_FILTER_POST ? state->result_count : 0;
    int end = state->result_count;
    while (pos < end) {
        int mid = pos + (end - pos) / 2;
        if (state->results[mid].distance < node->distance ||
            (state->results[mid].distance == node->distance && state->results[mid].id < node->id))
            pos = mid + 1;
        else
            end = mid;
    }
    if (admitted == 0 || pos >= capacity) {
        if (node->key != (Datum)0)
            pfree(DatumGetPointer(node->key));
        return;
    }
    if (state->result_count == capacity) {
        pfree(DatumGetPointer(state->results[capacity - 1].key));
        state->result_count--;
    }
    if (state->result_count > pos) {
        Size bytes = (state->result_count - pos) * sizeof(DiskAnnQueryNodeCopy);
        errno_t rc = memmove_s(&state->results[pos + 1], bytes, &state->results[pos], bytes);
        securec_check(rc, "", "");
    }
    state->results[pos] = *node;
    state->result_count++;
}

static void DiskAnnQueryDiscover(DiskAnnScanOpaque so, BlockNumber id)
{
    DiskAnnQueryScanState* state = so->queryScan;
    bool found = false;
    blockhash_insert(state->visited, id, &found);
    if (found)
        return;
    DiskAnnQueryNodeCopy node = DiskAnnQueryCopyNode(so, id, false);
    state->spec->counters->visited_nodes++;
    Neighbor neighbour(id, node.distance);
    neighbour.heaptidsLength = 0;
    state->navigation.insert(neighbour);
    if (state->spec->mode != ANN_FILTER_POST)
        DiskAnnQueryAdmit(so, &node); /* synchronously inside active search */
    else if (node.key != (Datum)0)
        pfree(DatumGetPointer(node.key));
    DiskAnnQueryCheckMemory(so);
}

static void DiskAnnQuerySearch(DiskAnnScanOpaque so)
{
    DiskAnnQueryScanState* state = so->queryScan;
    state->searching = true;
    state->spec->counters->search_rounds++;
    so->value = GetScanValue(so->scan); /* normalize once per scan epoch */
    if (so->normprocinfo == NULL)
        so->value = PointerGetDatum(PG_DETOAST_DATUM_COPY(so->value));
    for (size_t i = 0; i < so->frozenBlks.size(); i++)
        DiskAnnQueryDiscover(so, so->frozenBlks[i]);
    while (state->navigation.has_unexpanded_node()) {
        CHECK_FOR_INTERRUPTS();
        size_t pos;
        BlockNumber id = GetNextNeighbor(&state->navigation, &pos)->id;
        DiskAnnQueryNodeCopy node = DiskAnnQueryCopyNode(so, id, true);
        if (node.dead)
            state->navigation.remove(pos);
        if (node.key != (Datum)0)
            pfree(DatumGetPointer(node.key));
        for (int i = 0; i < node.edges; i++)
            DiskAnnQueryDiscover(so, node.nexts[i]);
    }
    state->searching = false;
    if (state->spec->mode == ANN_FILTER_POST) {
        for (size_t i = 0; i < state->navigation.size(); i++) {
            DiskAnnQueryNodeCopy node = DiskAnnQueryCopyNode(so, state->navigation[i].id, false, false);
            DiskAnnQueryAdmit(so, &node); /* visibility/key only, no SQL qualification */
        }
    }
    state->done = true;
    DiskAnnQueryCheckMemory(so);
}

void diskann_clear_query_filter(IndexScanDesc scan)
{
    DiskAnnScanOpaque so = (DiskAnnScanOpaque)scan->opaque;
    if (so->queryScan == NULL)
        return;
    MemoryContext context = so->queryScan->context;
    so->queryScan->spec = NULL;
    so->queryScan = NULL; /* release the borrowed executor state first */
    so->value = (Datum)0;
    MemoryContextDelete(context);
}

void diskann_set_query_filter(IndexScanDesc scan, const DiskAnnFilterSpec* filter)
{
    DiskAnnScanOpaque so = (DiskAnnScanOpaque)scan->opaque;
    if (filter == NULL || filter->qualify == NULL || filter->counters == NULL ||
        filter->epoch == 0 || filter->search_nodes < 1 || filter->memory_limit == 0 ||
        (filter->mode != ANN_FILTER_PRE && filter->mode != ANN_FILTER_IN &&
         filter->mode != ANN_FILTER_POST) || so->orderedBatch ||
        so->enablePQ || scan->numberOfOrderBys != 1 || scan->xs_ann_orderby.requested)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("invalid DiskANN query filtering protocol")));
    diskann_clear_query_filter(scan);
    MemoryContext context = AllocSetContextCreate(so->tmpCtx,
        "DiskANN query search", ALLOCSET_DEFAULT_SIZES);
    MemoryContext old = MemoryContextSwitchTo(context);
    DiskAnnQueryScanState* state = (DiskAnnQueryScanState*)palloc0(sizeof(DiskAnnQueryScanState));
    so->queryScan = state;
    state->context = context;
    state->spec = filter;
    if ((Size)filter->search_nodes > MaxAllocSize / sizeof(DiskAnnQueryNodeCopy))
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
            errmsg("DiskANN search capacity exceeds the allocation limit")));
    Size requested = (Size)filter->search_nodes *
        (sizeof(DiskAnnQueryNodeCopy) + sizeof(Neighbor));
    if (requested > filter->memory_limit)
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
            errmsg("DiskANN filtering exceeded diskann_filter_work_mem")));
    state->navigation._data = VectorList<Neighbor>();
    state->navigation.reserve(filter->search_nodes);
    state->visited = blockhash_create(context, 128, NULL);
    state->results = (DiskAnnQueryNodeCopy*)palloc0(
        (Size)filter->search_nodes * sizeof(DiskAnnQueryNodeCopy));
    DiskAnnQueryCheckMemory(so);
    MemoryContextSwitchTo(old);
}

bool diskann_current_query_member(IndexScanDesc scan, DiskAnnQueryMember* member)
{
    DiskAnnQueryScanState* state = ((DiskAnnScanOpaque)scan->opaque)->queryScan;
    if (state == NULL || !state->current_valid)
        return false;
    *member = state->current;
    return true;
}

bool diskann_query_search_active(IndexScanDesc scan)
{
    DiskAnnQueryScanState* state = ((DiskAnnScanOpaque)scan->opaque)->queryScan;
    return state != NULL && state->searching;
}

void diskann_query_check_memory(IndexScanDesc scan)
{
    DiskAnnScanOpaque so = (DiskAnnScanOpaque)scan->opaque;
    if (so->queryScan != NULL)
        DiskAnnQueryCheckMemory(so);
}

static bool DiskAnnQueryGetTuple(IndexScanDesc scan)
{
    DiskAnnScanOpaque so = (DiskAnnScanOpaque)scan->opaque;
    DiskAnnQueryScanState* state = so->queryScan;
    MemoryContext old = MemoryContextSwitchTo(state->context);
    state->current_valid = false;
    if (!state->done)
        DiskAnnQuerySearch(so);
    while (state->result_pos < state->result_count) {
        DiskAnnQueryNodeCopy* group = &state->results[state->result_pos];
        if (state->member_pos == group->members) {
            state->result_pos++;
            state->member_pos = 0;
            continue; /* an internal group boundary is not EOF */
        }
        int member = state->member_pos++;
        state->current.key = group->key;
        state->current.node_block = group->id;
        state->current.owner_tid = group->owner_tid;
        state->current.root_tid = group->roots[member];
        state->current.visible_tid = group->visible[member];
        state->current.normalized = so->normprocinfo != NULL;
        state->current_valid = true;
        scan->xs_ctup.t_self = group->roots[member];
        scan->xs_recheck = false;
        scan->xs_recheckorderby = false;
        scan->xs_orderbynulls[0] = true; /* actual SQL distance belongs to Sort */
        MemoryContextSwitchTo(old);
        return true;
    }
    MemoryContextSwitchTo(old);
    return false; /* one completed configured search, all members delivered */
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
    diskann_clear_query_filter(scan);
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
    so->queryScan = NULL;
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
    diskann_clear_query_filter(scan);
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
    if (so->queryScan != NULL)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("legacy batch expansion cannot control query filtering")));
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
    if (so->queryScan != NULL) {
        if (!ScanDirectionIsForward(dir))
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("DiskANN query filtering requires a forward scan")));
        DiskAnnInvalidateOrderByDistance(scan);
        return DiskAnnQueryGetTuple(scan);
    }
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
