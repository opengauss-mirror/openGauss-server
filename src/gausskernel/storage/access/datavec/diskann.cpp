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
 * diskann.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/datavec/diskann.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/reloptions.h"
#include "storage/freespace.h"
#include "commands/vacuum.h"
#include "miscadmin.h"
#include "utils/guc.h"
#include "utils/selfuncs.h"
#include "access/datavec/diskann.h"
#include "access/generic_xlog.h"

/*
 * Estimate the cost of an index scan
 */
static void diskanncostestimate_internal(PlannerInfo* root, IndexPath* path, double loopCount, Cost* indexStartupCost,
                                         Cost* indexTotalCost, Selectivity* indexSelectivity, double* indexCorrelation)
{
    GenericCosts costs;
    Relation index;

    Assert(indexTotalCost != nullptr);
    Assert(indexStartupCost != nullptr);
    Assert(indexSelectivity != nullptr);
    Assert(indexCorrelation != nullptr);

    /* Never use index without order */
    if (path->indexorderbys == NULL) {
        *indexStartupCost = DBL_MAX;
        *indexTotalCost = DBL_MAX;
        *indexSelectivity = 0;
        *indexCorrelation = 0;
        return;
    }

    errno_t rc = memset_s(&costs, sizeof(costs), 0, sizeof(costs));
    securec_check_c(rc, "\0", "\0");

    index = index_open(path->indexinfo->indexoid, NoLock);

    index_close(index, NoLock);

    costs.numIndexTuples = path->ann_filter_enabled ?
        path->ann_estimated_members : path->indexinfo->tuples;

    genericcostestimate(root, path, loopCount, costs.numIndexTuples, &costs.indexStartupCost, &costs.indexTotalCost,
                        &costs.indexSelectivity, &costs.indexCorrelation);

    if (path->ann_filter_enabled) {
        /* A fixed beam limits delivery, not graph exploration or Probe
         * calls. The planner separately estimates graph work; publish C/N
         * for cost_index's heap-fetch part. Unknown distinct uses the
         * physical member-slot upper bound, never K or K/selectivity. */
        double n = path->indexinfo->rel->tuples;
        if (!isfinite(n) || n < 0)
            n = Max(path->indexinfo->rel->rows, 1.0);
        n = Min(n, 1.0e100);
        double t = DISKANN_HEAPTIDS;
        VariableStatData vardata;
        bool isdefault = true;
        examine_variable(root, (Node*)linitial(((OpExpr*)path->ann_distance_expr)->args),
            path->ann_target_rti, &vardata);
        double raw_distinct = HeapTupleIsValid(vardata.statsTuple) ?
            ((Form_pg_statistic)GETSTRUCT(vardata.statsTuple))->stadistinct : 0.0;
        bool hasstats = isfinite(raw_distinct) && raw_distinct != 0.0;
        /* This is the full index's member density. Native Poisson filtering
         * is appropriate for qualified output distinct, not this input. */
        vardata.enablePossion = false;
        double distinct = get_variable_numdistinct(&vardata, &isdefault, false);
        ReleaseVariableStats(vardata);
        if (hasstats && !isdefault && isfinite(distinct) && distinct > 0)
            t = Min(Max(Max(n, path->indexinfo->tuples) / distinct, 1.0), (double)DISKANN_HEAPTIDS);
        double candidates = Min(path->ann_estimated_members,
            (double)path->ann_initial_search_nodes * t);
        costs.indexSelectivity = n > 0 ? Min(candidates / n, 1.0) : 0.0;
        ereport(DEBUG2, (errmsg("DiskANN fixed beam estimate: members=%.0f members_per_node=%.8f "
            "candidates=%.8f heap_selectivity=%.8f", path->ann_estimated_members, t,
            candidates, costs.indexSelectivity)));
    }

    /* Use total cost since most work happens before first tuple is returned */
    *indexStartupCost = costs.indexTotalCost;
    *indexTotalCost = costs.indexTotalCost;
    *indexSelectivity = costs.indexSelectivity;
    *indexCorrelation = costs.indexCorrelation;
}

/*
 * Parse and validate the reloptions
 */
static bytea* diskannoptions_internal(Datum reloptions, bool validate)
{
    static const relopt_parse_elt tab[] = {
        {"index_size", RELOPT_TYPE_INT, offsetof(DiskAnnOptions, indexSize)},
        {"enable_pq", RELOPT_TYPE_BOOL, offsetof(DiskAnnOptions, enablePQ)},
        {"pq_m", RELOPT_TYPE_INT, offsetof(DiskAnnOptions, pqM)}};

    relopt_value* options;
    int numoptions;
    DiskAnnOptions* rdopts;

    options = parseRelOptions(reloptions, validate, RELOPT_KIND_DISKANN, &numoptions);
    rdopts = (DiskAnnOptions*)allocateReloptStruct(sizeof(DiskAnnOptions), options, numoptions);
    fillRelOptions((void*)rdopts, sizeof(DiskAnnOptions), options, numoptions, validate, tab, lengthof(tab));

    return (bytea*)rdopts;
}

/*
 * Validate catalog entries for the specified operator class
 */
static bool diskannvalidate_internal(Oid opclassoid)
{
    return true;
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(diskannhandler);
Datum diskannhandler(PG_FUNCTION_ARGS)
{
    IndexAmRoutine* amroutine = makeNode(IndexAmRoutine);

    amroutine->amstrategies = 0;
    amroutine->amsupport = DISKANN_FUNC_NUM;
    amroutine->amcanorder = false;
    amroutine->amcanorderbyop = true;
    amroutine->amcanbackward = false; /* can change direction mid-scan */
    amroutine->amcanunique = false;
    amroutine->amcanmulticol = false;
    amroutine->amoptionalkey = true;
    amroutine->amsearcharray = false;
    amroutine->amsearchnulls = false;
    amroutine->amstorage = false;
    amroutine->amclusterable = false;
    amroutine->ampredlocks = false;
    amroutine->amcanparallel = false;
    amroutine->amcaninclude = false;
    amroutine->amkeytype = InvalidOid;

    /* Interface functions */
    errno_t rc = 0;
    rc = strcpy_s(amroutine->ambuildfuncname, NAMEDATALEN, "diskannbuild");
    securec_check(rc, "\0", "\0");
    rc = strcpy_s(amroutine->ambuildemptyfuncname, NAMEDATALEN, "diskannbuildempty");
    securec_check(rc, "\0", "\0");
    rc = strcpy_s(amroutine->aminsertfuncname, NAMEDATALEN, "diskanninsert");
    securec_check(rc, "\0", "\0");
    rc = strcpy_s(amroutine->ambulkdeletefuncname, NAMEDATALEN, "diskannbulkdelete");
    securec_check(rc, "\0", "\0");
    rc = strcpy_s(amroutine->amvacuumcleanupfuncname, NAMEDATALEN, "diskannvacuumcleanup");
    securec_check(rc, "\0", "\0");
    rc = strcpy_s(amroutine->amcostestimatefuncname, NAMEDATALEN, "diskanncostestimate");
    securec_check(rc, "\0", "\0");
    rc = strcpy_s(amroutine->amoptionsfuncname, NAMEDATALEN, "diskannoptions");
    securec_check(rc, "\0", "\0");
    rc = strcpy_s(amroutine->amvalidatefuncname, NAMEDATALEN, "diskannvalidate");
    securec_check(rc, "\0", "\0");
    rc = strcpy_s(amroutine->ambeginscanfuncname, NAMEDATALEN, "diskannbeginscan");
    securec_check(rc, "\0", "\0");
    rc = strcpy_s(amroutine->amrescanfuncname, NAMEDATALEN, "diskannrescan");
    securec_check(rc, "\0", "\0");
    rc = strcpy_s(amroutine->amgettuplefuncname, NAMEDATALEN, "diskanngettuple");
    securec_check(rc, "\0", "\0");
    rc = strcpy_s(amroutine->amendscanfuncname, NAMEDATALEN, "diskannendscan");
    securec_check(rc, "\0", "\0");

    PG_RETURN_POINTER(amroutine);
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(diskannbuild);
Datum diskannbuild(PG_FUNCTION_ARGS)
{
    if (IsExtremeRedo()) {
        elog(ERROR, "diskann index do not support extreme rto.");
    }
    Relation heap = (Relation)PG_GETARG_POINTER(0);
    Relation index = (Relation)PG_GETARG_POINTER(1);
    IndexInfo* indexinfo = (IndexInfo*)PG_GETARG_POINTER(2);
    IndexBuildResult* result = diskannbuild_internal(heap, index, indexinfo);

    PG_RETURN_POINTER(result);
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(diskannbuildempty);
Datum diskannbuildempty(PG_FUNCTION_ARGS)
{
    if (IsExtremeRedo()) {
        elog(ERROR, "diskann index do not support extreme rto.");
    }
    Relation index = (Relation)PG_GETARG_POINTER(0);
    diskannbuildempty_internal(index);

    PG_RETURN_VOID();
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(diskanninsert);
Datum diskanninsert(PG_FUNCTION_ARGS)
{
    if (IsExtremeRedo()) {
        elog(ERROR, "diskann index do not support extreme rto.");
    }
    Relation rel = (Relation)PG_GETARG_POINTER(0);
    Datum* values = (Datum*)PG_GETARG_POINTER(1);
    bool* isnull = reinterpret_cast<bool*>(PG_GETARG_POINTER(2));
    ItemPointer ht_ctid = (ItemPointer)PG_GETARG_POINTER(3);
    Relation heaprel = (Relation)PG_GETARG_POINTER(4);
    IndexUniqueCheck checkunique = (IndexUniqueCheck)PG_GETARG_INT32(5);
    bool result = diskanninsert_internal(rel, values, isnull, ht_ctid, heaprel, checkunique);

    PG_RETURN_BOOL(result);
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(diskannbulkdelete);
Datum diskannbulkdelete(PG_FUNCTION_ARGS)
{
    if (IsExtremeRedo()) {
        elog(ERROR, "diskann index do not support extreme rto.");
    }
    IndexVacuumInfo* info = (IndexVacuumInfo*)PG_GETARG_POINTER(0);
    IndexBulkDeleteResult* volatile stats = (IndexBulkDeleteResult*)PG_GETARG_POINTER(1);
    IndexBulkDeleteCallback callback = (IndexBulkDeleteCallback)PG_GETARG_POINTER(2);
    void* callbackState = static_cast<void*>(PG_GETARG_POINTER(3));
    stats = diskannbulkdelete_internal(info, stats, callback, callbackState);

    PG_RETURN_POINTER(stats);
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(diskannvacuumcleanup);
Datum diskannvacuumcleanup(PG_FUNCTION_ARGS)
{
    if (IsExtremeRedo()) {
        elog(ERROR, "diskann index do not support extreme rto.");
    }
    IndexVacuumInfo* info = (IndexVacuumInfo*)PG_GETARG_POINTER(0);
    IndexBulkDeleteResult* stats = (IndexBulkDeleteResult*)PG_GETARG_POINTER(1);
    stats = diskannvacuumcleanup_internal(info, stats);

    PG_RETURN_POINTER(stats);
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(diskanncostestimate);
Datum diskanncostestimate(PG_FUNCTION_ARGS)
{
    PlannerInfo* root = (PlannerInfo*)PG_GETARG_POINTER(0);
    IndexPath* path = (IndexPath*)PG_GETARG_POINTER(1);
    double loopcount = static_cast<double>(PG_GETARG_FLOAT8(2));
    Cost* startupcost = (Cost*)PG_GETARG_POINTER(3);
    Cost* totalcost = (Cost*)PG_GETARG_POINTER(4);
    Selectivity* selectivity = (Selectivity*)PG_GETARG_POINTER(5);
    double* correlation = reinterpret_cast<double*>(PG_GETARG_POINTER(6));
    diskanncostestimate_internal(root, path, loopcount, startupcost, totalcost, selectivity, correlation);

    PG_RETURN_VOID();
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(diskannoptions);
Datum diskannoptions(PG_FUNCTION_ARGS)
{
    Datum reloptions = PG_GETARG_DATUM(0);
    bool validate = PG_GETARG_BOOL(1);
    bytea* result = diskannoptions_internal(reloptions, validate);

    if (NULL != result) {
        PG_RETURN_BYTEA_P(result);
    }

    PG_RETURN_NULL();
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(diskannvalidate);
Datum diskannvalidate(PG_FUNCTION_ARGS)
{
    Oid opclassoid = PG_GETARG_OID(0);
    bool result = diskannvalidate_internal(opclassoid);

    PG_RETURN_BOOL(result);
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(diskannbeginscan);
Datum diskannbeginscan(PG_FUNCTION_ARGS)
{
    Relation rel = (Relation)PG_GETARG_POINTER(0);
    int nkeys = PG_GETARG_INT32(1);
    int norderbys = PG_GETARG_INT32(2);
    IndexScanDesc scan = diskannbeginscan_internal(rel, nkeys, norderbys);

    PG_RETURN_POINTER(scan);
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(diskannrescan);
Datum diskannrescan(PG_FUNCTION_ARGS)
{
    IndexScanDesc scan = (IndexScanDesc)PG_GETARG_POINTER(0);
    ScanKey scankey = (ScanKey)PG_GETARG_POINTER(1);
    int nkeys = PG_GETARG_INT32(2);
    ScanKey orderbys = (ScanKey)PG_GETARG_POINTER(3);
    int norderbys = PG_GETARG_INT32(4);
    diskannrescan_internal(scan, scankey, nkeys, orderbys, norderbys);

    PG_RETURN_VOID();
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(diskanngettuple);
Datum diskanngettuple(PG_FUNCTION_ARGS)
{
    IndexScanDesc scan = (IndexScanDesc)PG_GETARG_POINTER(0);
    ScanDirection direction = (ScanDirection)PG_GETARG_INT32(1);

    if (NULL == scan) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Invalid arguments for function diskanngettuple")));
    }

    bool result = diskanngettuple_internal(scan, direction);

    PG_RETURN_BOOL(result);
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(diskannendscan);
Datum diskannendscan(PG_FUNCTION_ARGS)
{
    IndexScanDesc scan = (IndexScanDesc)PG_GETARG_POINTER(0);
    diskannendscan_internal(scan);

    PG_RETURN_VOID();
}

bool diskanninsert_internal(Relation index, Datum* values, const bool* isnull, ItemPointer heap_tid, Relation heap,
                            IndexUniqueCheck checkUnique)
{
    Datum dst = PointerGetDatum(PG_DETOAST_DATUM(values[0]));
    Vector* value = (Vector*)DatumGetPointer(dst);

    DiskAnnMetaPageData metapage;
    DiskANNGetMetaPageInfo(index, &metapage);
    FmgrInfo* procinfo = index_getprocinfo(index, 1, DISKANN_DISTANCE_PROC);
    metapage.params = InitDiskPQParamsOnDisk(index, procinfo, metapage.dimensions, metapage.enablePQ, true);

    BlockNumber blkno = InsertTuple(index, values, heap_tid, &metapage, false);

    if (0 == metapage.nfrozen) {
        ItemPointerData hctid;
        ItemPointerSetInvalid(&hctid);

        BlockNumber frozen = InsertTuple(index, values, &hctid, &metapage, false);
        InsertFrozenPoint(index, frozen, false);
        DiskANNGetMetaPageInfo(index, &metapage);
    }

    DiskAnnGraphStore* graphStore = New(CurrentMemoryContext) DiskAnnGraphStore(index);
    DiskAnnGraph graph(index, metapage.dimensions, metapage.frozenBlkno[0], graphStore);
    graph.Link(blkno, metapage.indexSize, false);

    return false;
}
IndexBulkDeleteResult* diskannbulkdelete_internal(IndexVacuumInfo* info, IndexBulkDeleteResult* stats,
                                                  IndexBulkDeleteCallback callback, void* callbackState)
{
    DiskAnnMetaPageData meta;
    DiskANNGetMetaPageInfo(info->index, &meta);
    if (stats == NULL) {
        stats = (IndexBulkDeleteResult*)palloc0(sizeof(IndexBulkDeleteResult));
    }

    /* The v1 layout has no page-type discriminator. PQ pages precede all
     * vertex pages; the frozen vertex is NOT necessarily the first vertex.
     * Include unlinked duplicate pages and the frozen vertex in cleanup. */
    BlockNumber first = DISKANN_PQDATA_START_BLKNO + meta.pqTableNblk +
        meta.pqCentroidsblk + meta.pqOffsetblk;
    BlockNumber nblocks = RelationGetNumberOfBlocks(info->index);
    for (BlockNumber blk = first; blk < nblocks; ++blk) {
        vacuum_delay_point();
        Buffer buf = ReadBufferExtended(info->index, MAIN_FORKNUM, blk, RBM_NORMAL, info->strategy);
        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
        Page page = BufferGetPage(buf);
        if (PageIsNew(page) || PageGetMaxOffsetNumber(page) < FirstOffsetNumber ||
            !ItemIdHasStorage(PageGetItemId(page, FirstOffsetNumber))) {
            UnlockReleaseBuffer(buf);
            continue;
        }
        IndexTuple itup = DiskAnnPageGetIndexTuple(page);
        DiskAnnNodePage node = DiskAnnPageGetNode(itup);
        if (node->heaptidsLength > DISKANN_HEAPTIDS) {
            ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                errmsg("DiskANN node has an invalid heap TID count")));
        }

        ItemPointerData survivors[DISKANN_HEAPTIDS];
        uint8 count = 0;
        for (uint8 i = 0; i < node->heaptidsLength; ++i) {
            if (!callback(&node->heaptids[i], callbackState, InvalidOid, InvalidBktId)) {
                survivors[count++] = node->heaptids[i];
            }
        }
        /* t_tid identifies the stored vector, not just a member of the
         * bucket. Invalidate that ownership before the heap slot is reused,
         * even if an older deletion already removed it from heaptids[]. */
        bool deadOwner = ItemPointerIsValid(&itup->t_tid) &&
            callback(&itup->t_tid, callbackState, InvalidOid, InvalidBktId);
        if (count != node->heaptidsLength || deadOwner) {
            uint8 removed = node->heaptidsLength - count;
            GenericXLogState* state = GenericXLogStart(info->index);
            page = GenericXLogRegisterBuffer(state, buf, 0);
            itup = DiskAnnPageGetIndexTuple(page);
            node = DiskAnnPageGetNode(itup);
            for (uint8 i = 0; i < count; ++i) {
                node->heaptids[i] = survivors[i];
            }
            for (uint8 i = count; i < DISKANN_HEAPTIDS; ++i) {
                ItemPointerSetInvalid(&node->heaptids[i]);
            }
            node->heaptidsLength = count;
            if (deadOwner) {
                ItemPointerSetInvalid(&itup->t_tid);
            }
            GenericXLogFinish(state);
            stats->tuples_removed += removed;
        }
        /* Empty vertices remain navigation landmarks. Do not break edges,
         * recycle vertex pages, or erase vectors while searches can use them. */
        UnlockReleaseBuffer(buf);
    }
    stats->num_pages = nblocks;
    /* Physical buckets can contain the same TID in frozen/unlinked pages. */
    stats->num_index_tuples = info->num_heap_tuples;
    stats->estimated_count = true;
    return stats;
}

/*
 * Every DiskANN format must defer physical deletion to VACUUM. Generic WAL
 * is redo, not transaction undo: immediate deletion loses rows after an
 * abort and also destroys rows still needed by older snapshots.
 */
bool DiskAnnIndexUsesDeferredDelete(Relation index)
{
    return index->rd_rel->relam == DISKANN_AM_OID;
}

IndexBulkDeleteResult* diskannvacuumcleanup_internal(IndexVacuumInfo* info, IndexBulkDeleteResult* stats)
{
    if (!stats) {
        stats = (IndexBulkDeleteResult*)palloc0(sizeof(IndexBulkDeleteResult));
    }

    FreeSpaceMapVacuum(info->index);

    return stats;
}
