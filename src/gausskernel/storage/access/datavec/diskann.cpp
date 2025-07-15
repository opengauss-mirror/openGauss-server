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
#include "commands/vacuum.h"
#include "miscadmin.h"
#include "utils/guc.h"
#include "utils/selfuncs.h"
#include "access/datavec/diskann.h"

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

    costs.numIndexTuples = path->indexinfo->tuples;

    genericcostestimate(root, path, loopCount, costs.numIndexTuples, &costs.indexStartupCost, &costs.indexTotalCost,
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
static bytea* diskannoptions_internal(Datum reloptions, bool validate)
{
    static const relopt_parse_elt tab[] = {
        {"index_size", RELOPT_TYPE_INT, offsetof(DiskAnnOptions, indexSize)},
        {"enable_pq", RELOPT_TYPE_BOOL, offsetof(DiskAnnOptions, enablePQ)},
        {"pq_m", RELOPT_TYPE_INT, offsetof(DiskAnnOptions, pqM)},
        {"pq_ksub", RELOPT_TYPE_INT, offsetof(DiskAnnOptions, pqKsub)}};

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
    Relation heap = (Relation)PG_GETARG_POINTER(0);
    Relation index = (Relation)PG_GETARG_POINTER(1);
    IndexInfo* indexinfo = (IndexInfo*)PG_GETARG_POINTER(2);
    IndexBuildResult* result = diskannbuild_internal(heap, index, indexinfo);

    PG_RETURN_POINTER(result);
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(diskannbuildempty);
Datum diskannbuildempty(PG_FUNCTION_ARGS)
{
    Relation index = (Relation)PG_GETARG_POINTER(0);
    diskannbuildempty_internal(index);

    PG_RETURN_VOID();
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(diskanninsert);
Datum diskanninsert(PG_FUNCTION_ARGS)
{
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
    return false;
}
IndexBulkDeleteResult* diskannbulkdelete_internal(IndexVacuumInfo* info, IndexBulkDeleteResult* stats,
                                                  IndexBulkDeleteCallback callback, void* callbackState)
{
    return NULL;
}
IndexBulkDeleteResult* diskannvacuumcleanup_internal(IndexVacuumInfo* info, IndexBulkDeleteResult* stats)
{
    return NULL;
}

/*
 * Prepare for an index scan
 */
IndexScanDesc diskannbeginscan_internal(Relation index, int nkeys, int norderbys)
{
    return NULL;
}

/*
 * Start or restart an index scan
 */
void diskannrescan_internal(IndexScanDesc scan, ScanKey keys, int nkeys, ScanKey orderbys, int norderbys)
{}

/*
 * Fetch the next tuple in the given scan
 */
bool diskanngettuple_internal(IndexScanDesc scan, ScanDirection dir)
{
    return false;
}

/*
 * End a scan and release resources
 */
void diskannendscan_internal(IndexScanDesc scan)
{}