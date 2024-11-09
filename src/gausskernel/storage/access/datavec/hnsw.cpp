/*
 * Copyright (c) 2024 Huawei Technologies Co.,Ltd.
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
 * hnsw.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/datavec/hnsw.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include <cfloat>
#include <cmath>

#include "access/amapi.h"
#include "access/reloptions.h"
#include "commands/vacuum.h"
#include "access/datavec/hnsw.h"
#include "miscadmin.h"
#include "utils/guc.h"
#include "utils/selfuncs.h"

int hnsw_lock_tranche_id;

/*
 * Estimate the cost of an index scan
 */
static void hnswcostestimate_internal(PlannerInfo *root, IndexPath *path, double loop_count, Cost *indexStartupCost,
                                      Cost *indexTotalCost, Selectivity *indexSelectivity, double *indexCorrelation)
{
    GenericCosts costs;
    int m;
    int entryLevel;
    Relation index;

    /* Never use index without order */
    if (path->indexorderbys == NULL) {
        *indexStartupCost = DBL_MAX;
        *indexTotalCost = DBL_MAX;
        *indexSelectivity = 0;
        *indexCorrelation = 0;
        return;
    }

    MemSet(&costs, 0, sizeof(costs));

    index = index_open(path->indexinfo->indexoid, NoLock);
    HnswGetMetaPageInfo(index, &m, NULL);
    index_close(index, NoLock);

    /* Approximate entry level */
    entryLevel = (int)-log(1.0 / path->indexinfo->tuples) * HnswGetMl(m);

    /* TODO Improve estimate of visited tuples (currently underestimates) */
    /* Account for number of tuples (or entry level), m, and ef_search */
    costs.numIndexTuples = (entryLevel + 2) * m;

    genericcostestimate(root, path, loop_count, costs.numIndexTuples, &costs.indexStartupCost, &costs.indexTotalCost,
                        &costs.indexSelectivity, &costs.indexCorrelation);

    /* Use total cost since most work happens before first tuple is returned */
    *indexStartupCost = costs.indexTotalCost;
    *indexTotalCost = costs.indexTotalCost;
    *indexSelectivity = costs.indexSelectivity;
    *indexCorrelation = costs.indexCorrelation;
}

/*
 * Parse and validate the reloptions
 */
static bytea *hnswoptions_internal(Datum reloptions, bool validate)
{
    static const relopt_parse_elt tab[] = {
        {"m", RELOPT_TYPE_INT, offsetof(HnswOptions, m)},
        {"ef_construction", RELOPT_TYPE_INT, offsetof(HnswOptions, efConstruction)},
        {"enable_pq", RELOPT_TYPE_BOOL, offsetof(HnswOptions, enablePQ)},
        {"pq_m", RELOPT_TYPE_INT, offsetof(HnswOptions, pqM)},
        {"pq_ksub", RELOPT_TYPE_INT, offsetof(HnswOptions, pqKsub)},
        {"parallel_workers", RELOPT_TYPE_INT, offsetof(StdRdOptions, parallel_workers)},
        {"storage_type", RELOPT_TYPE_STRING, offsetof(HnswOptions, storage_type)}};

    relopt_value *options;
    int numoptions;
    HnswOptions *rdopts;

    options = parseRelOptions(reloptions, validate, RELOPT_KIND_HNSW, &numoptions);
    rdopts = (HnswOptions *)allocateReloptStruct(sizeof(HnswOptions), options, numoptions);
    fillRelOptions((void *)rdopts, sizeof(HnswOptions), options, numoptions, validate, tab, lengthof(tab));

    return (bytea *)rdopts;
}

/*
 * Validate catalog entries for the specified operator class
 */
static bool hnswvalidate_internal(Oid opclassoid)
{
    return true;
}

/*
 * Define index handler
 *
 * See https://www.postgresql.org/docs/current/index-api.html
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(hnswhandler);
Datum hnswhandler(PG_FUNCTION_ARGS)
{
    IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

    amroutine->amstrategies = 0;
    amroutine->amsupport = 3;
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
    rc = strcpy_s(amroutine->ambuildfuncname, NAMEDATALEN, "hnswbuild");
    securec_check(rc, "\0", "\0");
    rc = strcpy_s(amroutine->ambuildemptyfuncname, NAMEDATALEN, "hnswbuildempty");
    securec_check(rc, "\0", "\0");
    rc = strcpy_s(amroutine->aminsertfuncname, NAMEDATALEN, "hnswinsert");
    securec_check(rc, "\0", "\0");
    rc = strcpy_s(amroutine->ambulkdeletefuncname, NAMEDATALEN, "hnswbulkdelete");
    securec_check(rc, "\0", "\0");
    rc = strcpy_s(amroutine->amvacuumcleanupfuncname, NAMEDATALEN, "hnswvacuumcleanup");
    securec_check(rc, "\0", "\0");
    rc = strcpy_s(amroutine->amcostestimatefuncname, NAMEDATALEN, "hnswcostestimate");
    securec_check(rc, "\0", "\0");
    rc = strcpy_s(amroutine->amoptionsfuncname, NAMEDATALEN, "hnswoptions");
    securec_check(rc, "\0", "\0");
    rc = strcpy_s(amroutine->amvalidatefuncname, NAMEDATALEN, "hnswvalidate");
    securec_check(rc, "\0", "\0");
    rc = strcpy_s(amroutine->ambeginscanfuncname, NAMEDATALEN, "hnswbeginscan");
    securec_check(rc, "\0", "\0");
    rc = strcpy_s(amroutine->amrescanfuncname, NAMEDATALEN, "hnswrescan");
    securec_check(rc, "\0", "\0");
    rc = strcpy_s(amroutine->amgettuplefuncname, NAMEDATALEN, "hnswgettuple");
    securec_check(rc, "\0", "\0");
    rc = strcpy_s(amroutine->amendscanfuncname, NAMEDATALEN, "hnswendscan");
    securec_check(rc, "\0", "\0");
    rc = strcpy_s(amroutine->amdeletefuncname, NAMEDATALEN, "hnswdelete");
    securec_check(rc, "\0", "\0");

    PG_RETURN_POINTER(amroutine);
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(hnswbuild);
Datum hnswbuild(PG_FUNCTION_ARGS)
{
    Relation heap = (Relation)PG_GETARG_POINTER(0);
    Relation index = (Relation)PG_GETARG_POINTER(1);
    IndexInfo *indexinfo = (IndexInfo *)PG_GETARG_POINTER(2);
    IndexBuildResult *result = hnswbuild_internal(heap, index, indexinfo);

    PG_RETURN_POINTER(result);
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(hnswbuildempty);
Datum hnswbuildempty(PG_FUNCTION_ARGS)
{
    Relation index = (Relation)PG_GETARG_POINTER(0);
    hnswbuildempty_internal(index);

    PG_RETURN_VOID();
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(hnswinsert);
Datum hnswinsert(PG_FUNCTION_ARGS)
{
    Relation rel = (Relation)PG_GETARG_POINTER(0);
    Datum *values = (Datum *)PG_GETARG_POINTER(1);
    bool *isnull = reinterpret_cast<bool *>(PG_GETARG_POINTER(2));
    ItemPointer ht_ctid = (ItemPointer)PG_GETARG_POINTER(3);
    Relation heaprel = (Relation)PG_GETARG_POINTER(4);
    IndexUniqueCheck checkunique = (IndexUniqueCheck)PG_GETARG_INT32(5);
    bool result = hnswinsert_internal(rel, values, isnull, ht_ctid, heaprel, checkunique);

    PG_RETURN_BOOL(result);
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(hnswbulkdelete);
Datum hnswbulkdelete(PG_FUNCTION_ARGS)
{
    IndexVacuumInfo *info = (IndexVacuumInfo *)PG_GETARG_POINTER(0);
    IndexBulkDeleteResult *volatile stats = (IndexBulkDeleteResult *)PG_GETARG_POINTER(1);
    IndexBulkDeleteCallback callback = (IndexBulkDeleteCallback)PG_GETARG_POINTER(2);
    void *callbackState = static_cast<void *>(PG_GETARG_POINTER(3));
    stats = hnswbulkdelete_internal(info, stats, callback, callbackState);

    PG_RETURN_POINTER(stats);
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(hnswvacuumcleanup);
Datum hnswvacuumcleanup(PG_FUNCTION_ARGS)
{
    IndexVacuumInfo *info = (IndexVacuumInfo *)PG_GETARG_POINTER(0);
    IndexBulkDeleteResult *stats = (IndexBulkDeleteResult *)PG_GETARG_POINTER(1);
    stats = hnswvacuumcleanup_internal(info, stats);

    PG_RETURN_POINTER(stats);
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(hnswcostestimate);
Datum hnswcostestimate(PG_FUNCTION_ARGS)
{
    PlannerInfo *root = (PlannerInfo *)PG_GETARG_POINTER(0);
    IndexPath *path = (IndexPath *)PG_GETARG_POINTER(1);
    double loopcount = static_cast<double>(PG_GETARG_FLOAT8(2));
    Cost *startupcost = (Cost *)PG_GETARG_POINTER(3);
    Cost *totalcost = (Cost *)PG_GETARG_POINTER(4);
    Selectivity *selectivity = (Selectivity *)PG_GETARG_POINTER(5);
    double *correlation = reinterpret_cast<double *>(PG_GETARG_POINTER(6));
    hnswcostestimate_internal(root, path, loopcount, startupcost, totalcost, selectivity, correlation);

    PG_RETURN_VOID();
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(hnswoptions);
Datum hnswoptions(PG_FUNCTION_ARGS)
{
    Datum reloptions = PG_GETARG_DATUM(0);
    bool validate = PG_GETARG_BOOL(1);
    bytea *result = hnswoptions_internal(reloptions, validate);

    if (NULL != result)
        PG_RETURN_BYTEA_P(result);

    PG_RETURN_NULL();
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(hnswvalidate);
Datum hnswvalidate(PG_FUNCTION_ARGS)
{
    Oid opclassoid = PG_GETARG_OID(0);
    bool result = hnswvalidate_internal(opclassoid);

    PG_RETURN_BOOL(result);
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(hnswbeginscan);
Datum hnswbeginscan(PG_FUNCTION_ARGS)
{
    Relation rel = (Relation)PG_GETARG_POINTER(0);
    int nkeys = PG_GETARG_INT32(1);
    int norderbys = PG_GETARG_INT32(2);
    IndexScanDesc scan = hnswbeginscan_internal(rel, nkeys, norderbys);

    PG_RETURN_POINTER(scan);
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(hnswrescan);
Datum hnswrescan(PG_FUNCTION_ARGS)
{
    IndexScanDesc scan = (IndexScanDesc)PG_GETARG_POINTER(0);
    ScanKey scankey = (ScanKey)PG_GETARG_POINTER(1);
    int nkeys = PG_GETARG_INT32(2);
    ScanKey orderbys = (ScanKey)PG_GETARG_POINTER(3);
    int norderbys = PG_GETARG_INT32(4);
    hnswrescan_internal(scan, scankey, nkeys, orderbys, norderbys);

    PG_RETURN_VOID();
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(hnswgettuple);
Datum hnswgettuple(PG_FUNCTION_ARGS)
{
    IndexScanDesc scan = (IndexScanDesc)PG_GETARG_POINTER(0);
    ScanDirection direction = (ScanDirection)PG_GETARG_INT32(1);

    if (NULL == scan)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Invalid arguments for function hnswgettuple")));

    bool result = hnswgettuple_internal(scan, direction);

    PG_RETURN_BOOL(result);
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(hnswendscan);
Datum hnswendscan(PG_FUNCTION_ARGS)
{
    IndexScanDesc scan = (IndexScanDesc)PG_GETARG_POINTER(0);
    hnswendscan_internal(scan);

    PG_RETURN_VOID();
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(hnswdelete);
Datum hnswdelete(PG_FUNCTION_ARGS)
{
    Relation rel = (Relation)PG_GETARG_POINTER(0);
    Datum *values = (Datum *)PG_GETARG_POINTER(1);
    const bool *isnull = (const bool *)PG_GETARG_POINTER(2);
    ItemPointer heapTCtid = (ItemPointer)PG_GETARG_POINTER(3);
    bool isRollbackIndex = (bool)PG_GETARG_POINTER(4);

    bool result = hnswdelete_internal(rel, values, isnull, heapTCtid, isRollbackIndex);

    PG_RETURN_BOOL(result);
}