/* -------------------------------------------------------------------------
 *
 * nodeValuesscan.cpp
 *	  Support routines for scanning Values lists
 *	  ("VALUES (...), (...), ..." in rangetable).
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/runtime/executor/nodeValuesscan.cpp
 *
 * -------------------------------------------------------------------------
 *
 * INTERFACE ROUTINES
 *		ExecValuesScan			scans a values list.
 *		ExecValuesNext			retrieve next tuple in sequential order.
 *		ExecInitValuesScan		creates and initializes a valuesscan node.
 *		ExecEndValuesScan		releases any storage allocated.
 *		ExecReScanValuesScan	rescans the values list
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "executor/executor.h"
#include "executor/node/nodeValuesscan.h"
#include "parser/parsetree.h"
#include "optimizer/clauses.h"

static TupleTableSlot* ExecValuesScan(PlanState* state);
static TupleTableSlot* ValuesNext(ValuesScanState* node);

/* ----------------------------------------------------------------
 *						Scan Support
 * ----------------------------------------------------------------
 */
/* ----------------------------------------------------------------
 *		ValuesNext
 *
 *		This is a workhorse for ExecValuesScan
 * ----------------------------------------------------------------
 */
static TupleTableSlot* ValuesNext(ValuesScanState* node)
{
    int curr_idx = 0;

    /*
     * get information from the estate and scan state
     */
    EState* estate = node->ss.ps.state;
    ScanDirection direction = estate->es_direction;
    TupleTableSlot* slot = node->ss.ss_ScanTupleSlot;
    ExprContext* econtext = node->rowcontext;

    /*
     * Get the next tuple. Return NULL if no more tuples.
     */
    if (ScanDirectionIsForward(direction)) {
        if (node->curr_idx < node->array_len)
            node->curr_idx++;
    } else {
        if (node->curr_idx >= 0)
            node->curr_idx--;
    }

    /*
     * Always clear the result slot; this is appropriate if we are at the end
     * of the data, and if we're not, we still need it as the first step of
     * the store-virtual-tuple protocol.  It seems wise to clear the slot
     * before we reset the context it might have pointers into.
     */
    (void)ExecClearTuple(slot);

    curr_idx = node->curr_idx;
    if (curr_idx >= 0 && curr_idx < node->array_len) {
        MemoryContext old_context;
        List* expr_state_list = node->exprstatelists[curr_idx];
        List* exprlist = node->exprlists[curr_idx];
        Datum* values = NULL;
        bool* is_null = NULL;
        ListCell* lc = NULL;
        int resind;

        /*
         * Get rid of any prior cycle's leftovers.  We use ReScanExprContext
         * not just ResetExprContext because we want any registered shutdown
         * callbacks to be called.
         */
        ReScanExprContext(econtext);

        /*
         * Build the expression eval state in the econtext's per-tuple memory.
         * This is a tad unusual, but we want to delete the eval state again
         * when we move to the next row, to avoid growth of memory
         * requirements over a long values list.
         * Do per-value-row work in the per-tuple context.
         */
        old_context = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

        /*
         * Pass NULL, not my plan node, because we don't want anything in this
         * transient state linking into permanent state.  The only possibility
         * is a SubPlan, and there shouldn't be any (any subselects in the
         * VALUES list should be InitPlans).
         */
        if (expr_state_list == NIL) {
            if (estate->es_is_flt_frame) {
                expr_state_list = ExecInitExprListByFlatten(exprlist, NULL);
            } else {
                expr_state_list = ExecInitExprListByRecursion(exprlist, NULL);
            }
        }

        /* parser should have checked all sublists are the same length */
        Assert(list_length(expr_state_list) == slot->tts_tupleDescriptor->natts);

        /*
         * Compute the expressions and build a virtual result tuple. We
         * already did ExecClearTuple(slot).
         */
        values = slot->tts_values;
        is_null = slot->tts_isnull;

        RightRefState* refState = econtext->rightRefState;
        int targetCount = list_length(expr_state_list);

        int colCnt = (IS_ENABLE_RIGHT_REF(refState) && refState->colCnt > 0) ? refState->colCnt : 1;
        bool hasExecs[colCnt];
        Datum rightRefValues[colCnt];
        bool rightRefIsNulls[colCnt];
        InitOutputValues(refState, rightRefValues, rightRefIsNulls, hasExecs);

        resind = 0;
        foreach (lc, expr_state_list) {
            ExprState* exprState = (ExprState*)lfirst(lc);

            values[resind] = ExecEvalExpr(exprState, econtext, &is_null[resind]);
            if (unlikely(IS_ENABLE_INSERT_RIGHT_REF(refState) && resind < refState->explicitAttrLen)) {
                int idx = refState->explicitAttrNos[resind] - 1;
                hasExecs[idx] = true;
                rightRefValues[idx] = values[resind];
                rightRefIsNulls[idx] = is_null[resind];
            }
            resind++;
        }

        if (unlikely(IS_ENABLE_RIGHT_REF(refState))) {
            refState->values = nullptr;
            refState->isNulls = nullptr;
            refState->hasExecs = nullptr;
        }

        MemoryContextSwitchTo(old_context);

        /*
         * And return the virtual tuple.
         */
        ExecStoreVirtualTuple(slot);
    }

    return slot;
}

/*
 * ValuesRecheck -- access method routine to recheck a tuple in EvalPlanQual
 */
static bool ValuesRecheck(ValuesScanState* node, TupleTableSlot* slot)
{
    /* nothing to check */
    return true;
}

/* ----------------------------------------------------------------
 *		ExecValuesScan(node)
 *
 *		Scans the values lists sequentially and returns the next qualifying
 *		tuple.
 *		We call the ExecScan() routine and pass it the appropriate
 *		access method functions.
 * ----------------------------------------------------------------
 */
static TupleTableSlot* ExecValuesScan(PlanState* state)
{
    ValuesScanState* node = castNode(ValuesScanState, state);
    return ExecScan(&node->ss, (ExecScanAccessMtd)ValuesNext, (ExecScanRecheckMtd)ValuesRecheck);
}

/* ----------------------------------------------------------------
 *		ExecInitValuesScan
 * ----------------------------------------------------------------
 */
ValuesScanState* ExecInitValuesScan(ValuesScan* node, EState* estate, int eflags)
{
    RangeTblEntry* rte = rt_fetch(node->scan.scanrelid, estate->es_range_table);
    TupleDesc tupdesc = NULL;
    ListCell* vtl = NULL;
    int i;

    /*
     * ValuesScan should not have any children.
     */
    Assert(outerPlan(node) == NULL);
    Assert(innerPlan(node) == NULL);

    /*
     * create new ScanState for node
     */
    ValuesScanState* scan_state = makeNode(ValuesScanState);

    scan_state->ss.ps.plan = (Plan*)node;
    scan_state->ss.ps.state = estate;
    scan_state->ss.ps.ExecProcNode =  ExecValuesScan;

    /*
     * Miscellaneous initialization
     */
    PlanState* plan_state = &scan_state->ss.ps;

    /*
     * Create expression contexts.	We need two, one for per-sublist
     * processing and one for execScan.c to use for quals and projections. We
     * cheat a little by using ExecAssignExprContext() to build both.
     */
    ExecAssignExprContext(estate, plan_state);
    scan_state->rowcontext = plan_state->ps_ExprContext;
    ATTACH_RIGHT_REF_STATE(plan_state);
    ExecAssignExprContext(estate, plan_state);

    /*
     * tuple table initialization
     */
    ExecInitResultTupleSlot(estate, &scan_state->ss.ps);
    ExecInitScanTupleSlot(estate, &scan_state->ss);

    /*
     * initialize child expressions
     */
    if (estate->es_is_flt_frame) {
        scan_state->ss.ps.qual = (List*)ExecInitQualByFlatten(node->scan.plan.qual, (PlanState*)scan_state);
    } else {
        scan_state->ss.ps.targetlist = (List*)ExecInitExprByRecursion((Expr*)node->scan.plan.targetlist, (PlanState*)scan_state);
        scan_state->ss.ps.qual = (List*)ExecInitExprByRecursion((Expr*)node->scan.plan.qual, (PlanState*)scan_state);
    }
    /*
     * get info about values list
     * value lists scan, no relation is  involved, default tableAm type is set to HEAP.
     */
    tupdesc = ExecTypeFromExprList((List*)linitial(node->values_lists), rte->eref->colnames);

    ExecAssignScanType(&scan_state->ss, tupdesc);

    /*
     * Other node-specific setup
     */
    scan_state->marked_idx = -1;
    scan_state->curr_idx = -1;
    scan_state->array_len = list_length(node->values_lists);

    /* Convert the list of expression sublists into an array for easier
	 * addressing at runtime.  Also, detect whether any sublists contain
	 * SubPlans; for just those sublists, go ahead and do expression
	 * initialization.  (This avoids problems with SubPlans wanting to connect
	 * themselves up to the outer plan tree.  Notably, EXPLAIN won't see the
	 * subplans otherwise; also we will have troubles with dangling pointers
	 * and/or leaked resources if we try to handle SubPlans the same as
	 * simpler expressions.)
     */
    scan_state->exprlists = (List**)palloc(scan_state->array_len * sizeof(List*));
    scan_state->exprstatelists = (List**)palloc0(scan_state->array_len * sizeof(List*));
    i = 0;
    foreach (vtl, node->values_lists) {
        List* exprs = castNode(List, lfirst(vtl));
        scan_state->exprlists[i] = exprs;

        /*
         * Avoid the cost of a contain_subplans() scan in the simple
         * case where there are no SubPlans anywhere.
         */
        if (estate->es_subplanstates && contain_subplans((Node*)exprs)) {
            /*
             * As these expressions are only used once. This is worthwhile
             * because it's common to insert significant amounts of data
             * via VALUES().  Note that's initialized separately;
             * this just affects the upper-level subexpressions.
             */
            scan_state->exprstatelists[i] = ExecInitExprList(exprs, &scan_state->ss.ps);
        }
        i++;
    }

    scan_state->ss.ps.ps_vec_TupFromTlist = false;

    /*
     * Initialize result tuple type and projection info.
     * value lists result tuple is set to default tableAm type HEAP.
     */
    ExecAssignResultTypeFromTL(&scan_state->ss.ps);

    ExecAssignScanProjectionInfo(&scan_state->ss);

    return scan_state;
}

/* ----------------------------------------------------------------
 *		ExecEndValuesScan
 *
 *		frees any storage allocated through C routines.
 * ----------------------------------------------------------------
 */
void ExecEndValuesScan(ValuesScanState* node)
{
    /*
     * Free both exprcontexts
     */
    ExecFreeExprContext(&node->ss.ps);
    node->ss.ps.ps_ExprContext = node->rowcontext;
    ExecFreeExprContext(&node->ss.ps);

    /*
     * clean out the tuple table
     */
    (void)ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
    (void)ExecClearTuple(node->ss.ss_ScanTupleSlot);
}

/* ----------------------------------------------------------------
 *		ExecValuesMarkPos
 *
 *		Marks scan position.
 * ----------------------------------------------------------------
 */
void ExecValuesMarkPos(ValuesScanState* node)
{
    node->marked_idx = node->curr_idx;
}

/* ----------------------------------------------------------------
 *		ExecValuesRestrPos
 *
 *		Restores scan position.
 * ----------------------------------------------------------------
 */
void ExecValuesRestrPos(ValuesScanState* node)
{
    node->curr_idx = node->marked_idx;
}

/* ----------------------------------------------------------------
 *		ExecReScanValuesScan
 *
 *		Rescans the relation.
 * ----------------------------------------------------------------
 */
void ExecReScanValuesScan(ValuesScanState* node)
{
    (void)ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);

    ExecScanReScan(&node->ss);

    node->curr_idx = -1;
}
