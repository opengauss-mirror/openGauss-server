/* -------------------------------------------------------------------------
 *
 * nodeUnique.cpp
 *	  Routines to handle unique'ing of queries where appropriate
 *
 * Unique is a very simple node type that just filters out duplicate
 * tuples from a stream of sorted tuples from its subplan.	It's essentially
 * a dumbed-down form of Group: the duplicate-removal functionality is
 * identical.  However, Unique doesn't do projection nor qual checking,
 * so it's marginally more efficient for cases where neither is needed.
 * (It's debatable whether the savings justifies carrying two plan node
 * types, though.)
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/runtime/executor/nodeUnique.cpp
 *
 * -------------------------------------------------------------------------
 *
 * INTERFACE ROUTINES
 *		ExecUnique		- generate a unique'd temporary relation
 *		ExecInitUnique	- initialize node and subnodes
 *		ExecEndUnique	- shutdown node and subnodes
 *
 * NOTES
 *		Assumes tuples returned from subplan arrive in
 *		sorted order.
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "executor/executor.h"
#include "executor/node/nodeUnique.h"
#include "utils/memutils.h"

/* ----------------------------------------------------------------
 *		ExecUnique
 * ----------------------------------------------------------------
 */
TupleTableSlot* ExecUnique(UniqueState* node) /* return: a tuple or NULL */
{
    Unique* plan_node = (Unique*)node->ps.plan;
    TupleTableSlot* slot = NULL;

    /*
     * get information from the node
     */
    PlanState* outer_plan = outerPlanState(node);
    TupleTableSlot* result_tuple_slot = node->ps.ps_ResultTupleSlot;

    /*
     * now loop, returning only non-duplicate tuples. We assume that the
     * tuples arrive in sorted order so we can detect duplicates easily. The
     * first tuple of each group is returned.
     */
    for (;;) {
        /*
         * fetch a tuple from the outer subplan
         */
        slot = ExecProcNode(outer_plan);
        if (TupIsNull(slot)) {
            /* end of subplan, so we're done */
            (void)ExecClearTuple(result_tuple_slot);
            return NULL;
        }

        /*
         * Always return the first tuple from the subplan.
         */
        if (TupIsNull(result_tuple_slot))
            break;

        /*
         * Else test if the new tuple and the previously returned tuple match.
         * If so then we loop back and fetch another new tuple from the
         * subplan.
         */
        if (!execTuplesMatch(
                slot, result_tuple_slot, plan_node->numCols, plan_node->uniqColIdx, node->eqfunctions, node->tempContext))
            break;
    }

    /*
     * We have a new tuple different from the previous saved tuple (if any).
     * Save it and return it.  We must copy it because the source subplan
     * won't guarantee that this source tuple is still accessible after
     * fetching the next source tuple.
     */
    return ExecCopySlot(result_tuple_slot, slot);
}

/* ----------------------------------------------------------------
 *		ExecInitUnique
 *
 *		This initializes the unique node state structures and
 *		the node's subplan.
 * ----------------------------------------------------------------
 */
UniqueState* ExecInitUnique(Unique* node, EState* estate, int eflags)
{
    /* check for unsupported flags */
    Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

    /*
     * create state structure
     */
    UniqueState* unique_state = makeNode(UniqueState);

    unique_state->ps.plan = (Plan*)node;
    unique_state->ps.state = estate;

    /*
     * Miscellaneous initialization
     *
     * Unique nodes have no ExprContext initialization because they never call
     * ExecQual or ExecProject.  But they do need a per-tuple memory context
     * anyway for calling execTuplesMatch.
     */
    unique_state->tempContext = AllocSetContextCreate(
        CurrentMemoryContext, "Unique", ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);

    /*
     * Tuple table initialization
     */
    ExecInitResultTupleSlot(estate, &unique_state->ps);

    /*
     * then initialize outer plan
     */
    outerPlanState(unique_state) = ExecInitNode(outerPlan(node), estate, eflags);

    /*
     * unique nodes do no projections, so initialize projection info for this
     * node appropriately
     */
    ExecAssignResultTypeFromTL(
            &unique_state->ps,
            ExecGetResultType(outerPlanState(unique_state))->tdTableAmType);

    unique_state->ps.ps_ProjInfo = NULL;

    /*
     * Precompute fmgr lookup data for inner loop
     */
    unique_state->eqfunctions = execTuplesMatchPrepare(node->numCols, node->uniqOperators);

    return unique_state;
}

/* ----------------------------------------------------------------
 *		ExecEndUnique
 *
 *		This shuts down the subplan and frees resources allocated
 *		to this node.
 * ----------------------------------------------------------------
 */
void ExecEndUnique(UniqueState* node)
{
    /* clean up tuple table */
    (void)ExecClearTuple(node->ps.ps_ResultTupleSlot);

    MemoryContextDelete(node->tempContext);

    ExecEndNode(outerPlanState(node));
}

void ExecReScanUnique(UniqueState* node)
{
    /* must clear result tuple so first input tuple is returned */
    (void)ExecClearTuple(node->ps.ps_ResultTupleSlot);

    /*
     * if chgParam of subnode is not null then plan will be re-scanned by
     * first ExecProcNode.
     */
    if (node->ps.lefttree->chgParam == NULL)
        ExecReScan(node->ps.lefttree);
}
