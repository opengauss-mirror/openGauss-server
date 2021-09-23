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
    List* expr_list = NIL;

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
        if (node->curr_idx < node->array_len)
            expr_list = node->exprlists[node->curr_idx];
        else
            expr_list = NIL;
    } else {
        if (node->curr_idx >= 0)
            node->curr_idx--;
        if (node->curr_idx >= 0)
            expr_list = node->exprlists[node->curr_idx];
        else
            expr_list = NIL;
    }

    /*
     * Always clear the result slot; this is appropriate if we are at the end
     * of the data, and if we're not, we still need it as the first step of
     * the store-virtual-tuple protocol.  It seems wise to clear the slot
     * before we reset the context it might have pointers into.
     */
    (void)ExecClearTuple(slot);

    if (expr_list != NULL) {
        MemoryContext old_context;
        List* expr_state_list = NIL;
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
         */
        old_context = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

        /*
         * Pass NULL, not my plan node, because we don't want anything in this
         * transient state linking into permanent state.  The only possibility
         * is a SubPlan, and there shouldn't be any (any subselects in the
         * VALUES list should be InitPlans).
         */
        expr_state_list = (List*)ExecInitExpr((Expr*)expr_list, NULL);

        /* parser should have checked all sublists are the same length */
        Assert(list_length(expr_state_list) == slot->tts_tupleDescriptor->natts);

        /*
         * Compute the expressions and build a virtual result tuple. We
         * already did ExecClearTuple(slot).
         */
        values = slot->tts_values;
        is_null = slot->tts_isnull;

        resind = 0;
        foreach (lc, expr_state_list) {
            ExprState* exprState = (ExprState*)lfirst(lc);

            values[resind] = ExecEvalExpr(exprState, econtext, &is_null[resind], NULL);
            resind++;
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
TupleTableSlot* ExecValuesScan(ValuesScanState* node)
{
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
    ExecAssignExprContext(estate, plan_state);

    /*
     * tuple table initialization
     */
    ExecInitResultTupleSlot(estate, &scan_state->ss.ps);
    ExecInitScanTupleSlot(estate, &scan_state->ss);

    /*
     * initialize child expressions
     */
    scan_state->ss.ps.targetlist = (List*)ExecInitExpr((Expr*)node->scan.plan.targetlist, (PlanState*)scan_state);
    scan_state->ss.ps.qual = (List*)ExecInitExpr((Expr*)node->scan.plan.qual, (PlanState*)scan_state);

    /*
     * get info about values list
     * value lists scan, no relation is  involved, default tableAm type is set to HEAP.
     */
    tupdesc = ExecTypeFromExprList((List*)linitial(node->values_lists), rte->eref->colnames, TAM_HEAP);

    ExecAssignScanType(&scan_state->ss, tupdesc);

    /*
     * Other node-specific setup
     */
    scan_state->marked_idx = -1;
    scan_state->curr_idx = -1;
    scan_state->array_len = list_length(node->values_lists);

    /* convert list of sublists into array of sublists for easy addressing */
    scan_state->exprlists = (List**)palloc(scan_state->array_len * sizeof(List*));
    i = 0;
    foreach (vtl, node->values_lists) {
        scan_state->exprlists[i++] = (List*)lfirst(vtl);
    }

    scan_state->ss.ps.ps_TupFromTlist = false;

    /*
     * Initialize result tuple type and projection info.
     * value lists result tuple is set to default tableAm type HEAP.
     */
    ExecAssignResultTypeFromTL(&scan_state->ss.ps, TAM_HEAP);

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
