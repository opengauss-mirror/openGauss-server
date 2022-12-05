/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
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
 * gc_fdw_single.cpp
 *		  FDW option handling for gc_fdw
 *
 * IDENTIFICATION
 *		  contrib/gc_fdw/gc_fdw_single.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
 
#include "gc_fdw.h"
 
#include "access/htup.h"
#include "access/sysattr.h"
#include "access/transam.h"
#include "catalog/pg_class.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/vacuum.h"
#include "foreign/fdwapi.h"
#include "funcapi.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/cost.h"
#include "optimizer/clauses.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/var.h"
#include "optimizer/tlist.h"
#include "parser/parsetree.h"
#include "pgxc/pgxcnode.h"
#include "storage/buf/block.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/selfuncs.h"

 /*
 * SQL functions
 */
PG_FUNCTION_INFO_V1(gc_fdw_handler);
static void gcGetForeignRelSize(PlannerInfo* root, RelOptInfo* baserel, Oid foreigntableid)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return;
}
static void gcGetForeignPaths(PlannerInfo* root, RelOptInfo* baserel, Oid foreigntableid)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return;
}

static ForeignScan *gcGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid,
 ForeignPath *best_path, List *tlist, List *scan_clauses, Plan *outer_plan)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}
static void gcBeginForeignScan(ForeignScanState* node, int eflags)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return;
}

static TupleTableSlot* gcIterateForeignScan(ForeignScanState* node)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}

static VectorBatch* gcIterateVecForeignScan(VecForeignScanState* node)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}

static void gcReScanForeignScan(ForeignScanState* node)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return;
}

static void gcEndForeignScan(ForeignScanState* node)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return;
}

static void gcExplainForeignScan(ForeignScanState* node, ExplainState* es)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return;
}

static bool gcAnalyzeForeignTable(Relation relation, AcquireSampleRowsFunc* func, BlockNumber* totalpages,
                                            void* additionalData, bool estimate_table_rownum)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return false;
}
static void gcValidateTableDef(Node* Obj)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return;
}


/*
 * Foreign-data wrapper handler function: return a struct with pointers
 * to my callback routines.
 */
Datum gc_fdw_handler(PG_FUNCTION_ARGS)
{
    FdwRoutine* routine = makeNode(FdwRoutine);

    /* Functions for scanning foreign tables */
    routine->GetForeignRelSize = gcGetForeignRelSize;
    routine->GetForeignPaths = gcGetForeignPaths;
    routine->GetForeignPlan = gcGetForeignPlan;

    routine->BeginForeignScan = gcBeginForeignScan;
    routine->IterateForeignScan = gcIterateForeignScan;
    routine->VecIterateForeignScan = gcIterateVecForeignScan;
    routine->ReScanForeignScan = gcReScanForeignScan;
    routine->EndForeignScan = gcEndForeignScan;

    /* Support functions for EXPLAIN */
    routine->ExplainForeignScan = gcExplainForeignScan;

    /* Support functions for ANALYZE */
    routine->AnalyzeForeignTable = gcAnalyzeForeignTable;

    /* Check create/alter foreign table */
    routine->ValidateTableDef = gcValidateTableDef;

    PG_RETURN_POINTER(routine);
}


