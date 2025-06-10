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
 * bm25.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/datavec/bm25.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "access/multi_redo_api.h"
#include "access/datavec/bm25.h"

/*
 * Estimate the cost of an index scan
 */
static void bm25costestimate_internal(PlannerInfo *root, IndexPath *path, double loop_count, Cost *indexStartupCost,
                                      Cost *indexTotalCost, Selectivity *indexSelectivity, double *indexCorrelation)
{
    /* Never use index without order */
    if (path->indexorderbys == NULL) {
        *indexStartupCost = DBL_MAX;
        *indexTotalCost = DBL_MAX;
        *indexSelectivity = 0;
        *indexCorrelation = 0;
        return;
    }

    *indexStartupCost = 0;
    *indexTotalCost = 0;
    return;
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(bm25build);
Datum bm25build(PG_FUNCTION_ARGS)
{
    if (IsExtremeRedo()) {
        elog(ERROR, "bm25 index do not support extreme rto.");
    }
    Relation heap = (Relation)PG_GETARG_POINTER(0);
    Relation index = (Relation)PG_GETARG_POINTER(1);
    IndexInfo *indexinfo = (IndexInfo *)PG_GETARG_POINTER(2);
    IndexBuildResult *result = bm25build_internal(heap, index, indexinfo);

    PG_RETURN_POINTER(result);
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(bm25buildempty);
Datum bm25buildempty(PG_FUNCTION_ARGS)
{
    if (IsExtremeRedo()) {
        elog(ERROR, "bm25 index do not support extreme rto.");
    }
    Relation index = (Relation)PG_GETARG_POINTER(0);
    bm25buildempty_internal(index);

    PG_RETURN_VOID();
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(bm25beginscan);
Datum bm25beginscan(PG_FUNCTION_ARGS)
{
    Relation rel = (Relation)PG_GETARG_POINTER(0);
    int nkeys = PG_GETARG_INT32(1);
    int norderbys = PG_GETARG_INT32(2);
    IndexScanDesc scan = bm25beginscan_internal(rel, nkeys, norderbys);

    PG_RETURN_POINTER(scan);
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(bm25rescan);
Datum bm25rescan(PG_FUNCTION_ARGS)
{
    IndexScanDesc scan = (IndexScanDesc)PG_GETARG_POINTER(0);
    ScanKey scankey = (ScanKey)PG_GETARG_POINTER(1);
    int nkeys = PG_GETARG_INT32(2);
    ScanKey orderbys = (ScanKey)PG_GETARG_POINTER(3);
    int norderbys = PG_GETARG_INT32(4);
    bm25rescan_internal(scan, scankey, nkeys, orderbys, norderbys);

    PG_RETURN_VOID();
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(bm25gettuple);
Datum bm25gettuple(PG_FUNCTION_ARGS)
{
    IndexScanDesc scan = (IndexScanDesc)PG_GETARG_POINTER(0);
    ScanDirection direction = (ScanDirection)PG_GETARG_INT32(1);

    if (NULL == scan)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Invalid arguments for function bm25gettuple")));

    bool result = bm25gettuple_internal(scan, direction);

    PG_RETURN_BOOL(result);
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(bm25endscan);
Datum bm25endscan(PG_FUNCTION_ARGS)
{
    IndexScanDesc scan = (IndexScanDesc)PG_GETARG_POINTER(0);
    bm25endscan_internal(scan);

    PG_RETURN_VOID();
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(bm25costestimate);
Datum bm25costestimate(PG_FUNCTION_ARGS)
{
    PlannerInfo *root = (PlannerInfo *)PG_GETARG_POINTER(0);
    IndexPath *path = (IndexPath *)PG_GETARG_POINTER(1);
    double loopcount = static_cast<double>(PG_GETARG_FLOAT8(2));
    Cost *startupcost = (Cost *)PG_GETARG_POINTER(3);
    Cost *totalcost = (Cost *)PG_GETARG_POINTER(4);
    Selectivity *selectivity = (Selectivity *)PG_GETARG_POINTER(5);
    double *correlation = reinterpret_cast<double *>(PG_GETARG_POINTER(6));
    bm25costestimate_internal(root, path, loopcount, startupcost, totalcost, selectivity, correlation);

    PG_RETURN_VOID();
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(bm25insert);
Datum bm25insert(PG_FUNCTION_ARGS)
{
    if (IsExtremeRedo()) {
        elog(ERROR, "bm25 index do not support extreme rto.");
    }
    Relation rel = (Relation)PG_GETARG_POINTER(0);
    Datum *values = (Datum *)PG_GETARG_POINTER(1);
    bool *isnull = reinterpret_cast<bool *>(PG_GETARG_POINTER(2));
    ItemPointer ht_ctid = (ItemPointer)PG_GETARG_POINTER(3);
    Relation heaprel = (Relation)PG_GETARG_POINTER(4);
    IndexUniqueCheck checkunique = (IndexUniqueCheck)PG_GETARG_INT32(5);
    if (isnull[0]) {
        PG_RETURN_BOOL(false);
    }

    bool result = bm25insert_internal(rel, values, ht_ctid);

    PG_RETURN_BOOL(result);
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(bm25options);
Datum bm25options(PG_FUNCTION_ARGS)
{
    /* No options for bm25 currently. */
    PG_RETURN_NULL();
}


PGDLLEXPORT PG_FUNCTION_INFO_V1(bm25bulkdelete);
Datum bm25bulkdelete(PG_FUNCTION_ARGS)
{
    if (IsExtremeRedo()) {
        elog(ERROR, "bm25 index do not support extreme rto.");
    }
    IndexVacuumInfo *info = (IndexVacuumInfo *)PG_GETARG_POINTER(0);
    IndexBulkDeleteResult *volatile stats = (IndexBulkDeleteResult *)PG_GETARG_POINTER(1);
    IndexBulkDeleteCallback callback = (IndexBulkDeleteCallback)PG_GETARG_POINTER(2);
    void *callbackState = static_cast<void *>(PG_GETARG_POINTER(3));
    stats = bm25bulkdelete_internal(info, stats, callback, callbackState);

    PG_RETURN_POINTER(stats);
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(bm25vacuumcleanup);
Datum bm25vacuumcleanup(PG_FUNCTION_ARGS)
{
    if (IsExtremeRedo()) {
        elog(ERROR, "bm25 index do not support extreme rto.");
    }
    IndexVacuumInfo *info = (IndexVacuumInfo *)PG_GETARG_POINTER(0);
    IndexBulkDeleteResult *stats = (IndexBulkDeleteResult *)PG_GETARG_POINTER(1);
    stats = bm25vacuumcleanup_internal(info, stats);
    PG_RETURN_POINTER(stats);
}