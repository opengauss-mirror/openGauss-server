/* -------------------------------------------------------------------------
 *
 * nodeFunctionscan.cpp
 *	  Support routines for scanning RangeFunctions (functions in rangetable).
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/runtime/executor/nodeFunctionscan.cpp
 *
 * -------------------------------------------------------------------------
 *
 * INTERFACE ROUTINES
 *		ExecFunctionScan		scans a function.
 *		ExecFunctionNext		retrieve next tuple in sequential order.
 *		ExecInitFunctionScan	creates and initializes a functionscan node.
 *		ExecEndFunctionScan		releases any storage allocated.
 *		ExecReScanFunctionScan	rescans the function
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "executor/node/nodeFunctionscan.h"
#include "funcapi.h"
#include "nodes/nodeFuncs.h"

static TupleTableSlot* FunctionNext(FunctionScanState* node);

/* ----------------------------------------------------------------
 *						Scan Support
 * ----------------------------------------------------------------
 */
/* ----------------------------------------------------------------
 *		FunctionNext
 *
 *		This is a workhorse for ExecFunctionScan
 * ----------------------------------------------------------------
 */
static TupleTableSlot* FunctionNext(FunctionScanState* node)
{
    TupleTableSlot* slot = NULL;
    EState* estate = NULL;
    ScanDirection direction;
    Tuplestorestate* tuplestorestate = NULL;

    /*
     * get information from the estate and scan state
     */
    estate = node->ss.ps.state;
    direction = estate->es_direction;

    tuplestorestate = node->tuplestorestate;

    /*
     * If first time through, read all tuples from function and put them in a
     * tuplestore. Subsequent calls just fetch tuples from tuplestore.
     */
    if (tuplestorestate == NULL) {
        node->tuplestorestate = tuplestorestate = ExecMakeTableFunctionResult(
            node->funcexpr, node->ss.ps.ps_ExprContext, node->tupdesc, node->eflags & EXEC_FLAG_BACKWARD, node);
    }

    /*
     * Get the next tuple from tuplestore. Return NULL if no more tuples.
     */
    slot = node->ss.ss_ScanTupleSlot;
    (void)tuplestore_gettupleslot(tuplestorestate, ScanDirectionIsForward(direction), false, slot);
    return slot;
}

/*
 * FunctionRecheck -- access method routine to recheck a tuple in EvalPlanQual
 */
static bool FunctionRecheck(FunctionScanState* node, TupleTableSlot* slot)
{
    /* nothing to check */
    return true;
}

/* ----------------------------------------------------------------
 *		ExecFunctionScan(node)
 *
 *		Scans the function sequentially and returns the next qualifying
 *		tuple.
 *		We call the ExecScan() routine and pass it the appropriate
 *		access method functions.
 * ----------------------------------------------------------------
 */
TupleTableSlot* ExecFunctionScan(FunctionScanState* node)
{
    return ExecScan(&node->ss, (ExecScanAccessMtd)FunctionNext, (ExecScanRecheckMtd)FunctionRecheck);
}

/* ----------------------------------------------------------------
 *		ExecInitFunctionScan
 * ----------------------------------------------------------------
 */
FunctionScanState* ExecInitFunctionScan(FunctionScan* node, EState* estate, int eflags)
{
    FunctionScanState* scanstate = NULL;
    Oid funcrettype;
    TypeFuncClass functypclass;
    TupleDesc tupdesc = NULL;

    /* check for unsupported flags */
    Assert(!(eflags & EXEC_FLAG_MARK));

    /*
     * FunctionScan should not have any children.
     */
    Assert(outerPlan(node) == NULL);
    Assert(innerPlan(node) == NULL);

    /*
     * create new ScanState for node
     */
    scanstate = makeNode(FunctionScanState);
    scanstate->ss.ps.plan = (Plan*)node;
    scanstate->ss.ps.state = estate;
    scanstate->eflags = eflags;

    /*
     * Miscellaneous initialization
     *
     * create expression context for node
     */
    ExecAssignExprContext(estate, &scanstate->ss.ps);

    /*
     * tuple table initialization
     */
    ExecInitResultTupleSlot(estate, &scanstate->ss.ps);
    ExecInitScanTupleSlot(estate, &scanstate->ss);

    /*
     * initialize child expressions
     */
    scanstate->ss.ps.targetlist = (List*)ExecInitExpr((Expr*)node->scan.plan.targetlist, (PlanState*)scanstate);
    scanstate->ss.ps.qual = (List*)ExecInitExpr((Expr*)node->scan.plan.qual, (PlanState*)scanstate);

    /*
     * Now determine if the function returns a simple or composite type, and
     * build an appropriate tupdesc.
     */
    functypclass = get_expr_result_type(node->funcexpr, &funcrettype, &tupdesc);
    if (functypclass == TYPEFUNC_COMPOSITE) {
        /* Composite data type, e.g. a table's row type */
        Assert(tupdesc);
        /* Must copy it out of typcache for safety */
        tupdesc = CreateTupleDescCopy(tupdesc);
        if (tupdesc->tdTableAmType != TAM_HEAP) {
            /* For function scan, we need tupdesc type to be heap,
             * and invalidate attcacheoff, since other storage type
             * uses different offset values than heap.
             */
            int i;
            for (i = 0; i < tupdesc->natts; i++) {
                tupdesc->attrs[i]->attcacheoff = -1;
            }
            tupdesc->tdTableAmType = TAM_HEAP;
        }
    } else if (functypclass == TYPEFUNC_SCALAR) {
        /* Base data type, i.e. scalar */
        char* attname = strVal(linitial(node->funccolnames));

        tupdesc = CreateTemplateTupleDesc(1, false, TAM_HEAP);
        TupleDescInitEntry(tupdesc, (AttrNumber)1, attname, funcrettype, -1, 0);
        TupleDescInitEntryCollation(tupdesc, (AttrNumber)1, exprCollation(node->funcexpr));
    } else if (functypclass == TYPEFUNC_RECORD) {
        tupdesc =
            BuildDescFromLists(node->funccolnames, node->funccoltypes, node->funccoltypmods, node->funccolcollations);
    } else {
        /* crummy error message, but parser should have caught this */
        ereport(
            ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("function in FROM has unsupported return type")));
    }

    /*
     * For RECORD results, make sure a typmod has been assigned.  (The
     * function should do this for itself, but let's cover things in case it
     * doesn't.)
     */
    BlessTupleDesc(tupdesc);

    scanstate->tupdesc = tupdesc;
    ExecAssignScanType(&scanstate->ss, tupdesc);

    /*
     * Other node-specific setup
     */
    scanstate->tuplestorestate = NULL;
    scanstate->funcexpr = ExecInitExpr((Expr*)node->funcexpr, (PlanState*)scanstate);

    scanstate->ss.ps.ps_TupFromTlist = false;

    /*
     * Initialize result tuple type and projection info.
     */
    ExecAssignResultTypeFromTL(
            &scanstate->ss.ps,
            scanstate->ss.ss_ScanTupleSlot->tts_tupleDescriptor->tdTableAmType);

    ExecAssignScanProjectionInfo(&scanstate->ss);

    Assert(scanstate->ss.ps.ps_ResultTupleSlot->tts_tupleDescriptor->tdTableAmType != TAM_INVALID);

    return scanstate;
}

/* ----------------------------------------------------------------
 *		ExecEndFunctionScan
 *
 *		frees any storage allocated through C routines.
 * ----------------------------------------------------------------
 */
void ExecEndFunctionScan(FunctionScanState* node)
{
    /*
     * Free the exprcontext
     */
    ExecFreeExprContext(&node->ss.ps);

    /*
     * clean out the tuple table
     */
    (void)ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
    (void)ExecClearTuple(node->ss.ss_ScanTupleSlot);

    /*
     * Release tuplestore resources
     */
    if (node->tuplestorestate != NULL)
        tuplestore_end(node->tuplestorestate);
    node->tuplestorestate = NULL;
}

/* ----------------------------------------------------------------
 *		ExecReScanFunctionScan
 *
 *		Rescans the relation.
 * ----------------------------------------------------------------
 */
void ExecReScanFunctionScan(FunctionScanState* node)
{
    (void)ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);

    ExecScanReScan(&node->ss);

    /*
     * If we haven't materialized yet, just return.
     */
    if (!node->tuplestorestate)
        return;

    /*
     * Here we have a choice whether to drop the tuplestore (and recompute the
     * function outputs) or just rescan it.  We must recompute if the
     * expression contains parameters, else we rescan.	XXX maybe we should
     * recompute if the function is volatile?
     */
    if (node->ss.ps.chgParam != NULL) {
        tuplestore_end(node->tuplestorestate);
        node->tuplestorestate = NULL;
    } else
        tuplestore_rescan(node->tuplestorestate);
}
