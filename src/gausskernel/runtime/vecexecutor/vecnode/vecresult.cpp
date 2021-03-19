/* ---------------------------------------------------------------------------------------
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2000, PostgreSQL, Inc
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * vecresult.cpp
 *  support for constant nodes needing special code.
 *
 * IDENTIFICATION
 *        Code/src/gausskernel/runtime/vecexecutor/vecnode/vecresult.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "vecexecutor/vecexecutor.h"
#include "executor/executor.h"
#include "vecexecutor/vecnoderesult.h"
#include "utils/memutils.h"
#include "vecexecutor/vectorbatch.h"

/* ----------------------------------------------------------------
 *		ExecVecResult(node)
 *
 *		returns the tuples from the outer plan which satisfy the
 *		qualification clause.  Since result nodes with right
 *		subtrees are never planned, we ignore the right subtree
 *		entirely (for now).. -cim 10/7/89
 *
 *		The qualification containing only constant clauses are
 *		checked first before any processing is done. It always returns
 *		'nil' if the constant qualification is not satisfied.
 * ----------------------------------------------------------------
 */
VectorBatch* ExecVecResult(VecResultState* node)
{
    PlanState* outer_plan = NULL;
    ExprContext* expr_context = NULL;
    VectorBatch* batch = NULL;
    VectorBatch* res_batch = NULL;
    List* qual = node->ps.qual;

    expr_context = node->ps.ps_ExprContext;

    /*
     * check constant qualifications like (2 > 1), if not already done
     */
    if (node->rs_checkqual) {
        bool qual_result = ExecQual((List*)node->resconstantqual, expr_context, false);
        node->rs_checkqual = false;
        if (!qual_result) {
            node->rs_done = true;
            /*
             * Mark this constant qualification check failure for future corner cases. Currently only used in GDS
             * foreign scan
             */
            u_sess->exec_cxt.exec_result_checkqual_fail = true;
            return NULL;
        }
    }

    Assert(node->ps.ps_TupFromTlist == false);

    /*
     * Reset per-tuple memory context to free any expression evaluation
     * storage allocated in the previous tuple cycle.  Note this can't happen
     * until we're done projecting out tuples from a scan tuple.
     */
    ResetExprContext(expr_context);

    /*
     * if rs_done is true then it means that we were asked to return a
     * constant tuple and we already did the last time ExecResult() was
     * called, OR that we failed the constant qual check. Either way, now we
     * are through.
     */
    while (!node->rs_done) {
        outer_plan = outerPlanState(node);
        if (outer_plan != NULL) {
            /*
             * retrieve tuples from the outer plan until there are no more.
             */
            batch = VectorEngine(outer_plan);

            if (BatchIsNull(batch)) {
                return NULL;
            }

            /*
             * prepare to compute projection expressions, which will expect to
             * access the input tuples as varno OUTER.
             */
            expr_context->ecxt_outerbatch = batch;
            if (list_length(qual) != 0) {
                expr_context->ecxt_scanbatch = batch;

                ScalarVector* p_vector = ExecVecQual(qual, expr_context, false);
                if (unlikely(p_vector == NULL))
                    continue;

                batch->Pack(expr_context->ecxt_scanbatch->m_sel);
            }
        } else {
            /*
             * if we don't have an outer plan, then we are just generating the
             * results from a constant target list.  Do it only once.
             */
            node->rs_done = true;
        }

        /*
         * form the result tuple using ExecProject(), and return it --- unless
         * the projection produces an empty set, in which case we must loop
         * back to see if there are more outer_plan tuples.
         */
        res_batch = ExecVecProject(node->ps.ps_ProjInfo);

        if (batch != NULL) {
            res_batch->m_rows = Min(res_batch->m_rows, batch->m_rows);
        }

        res_batch->FixRowCount();
        return res_batch;
    }

    return NULL;
}

/* ----------------------------------------------------------------
 *		ExecInitResult
 *
 *		Creates the run-time state information for the result node
 *		produced by the planner and initializes outer relations
 *		(child nodes).
 * ----------------------------------------------------------------
 */
VecResultState* ExecInitVecResult(VecResult* node, EState* estate, int eflags)
{
    VecResultState* res_state = NULL;
    ScalarDesc desc;

    /* check for unsupported flags */
    Assert(!(eflags & (EXEC_FLAG_MARK | EXEC_FLAG_BACKWARD)) || outerPlan(node) != NULL);

    /*
     * create state structure
     */
    res_state = makeNode(VecResultState);
    res_state->ps.plan = (Plan*)node;
    res_state->ps.state = estate;
    res_state->ps.vectorized = true;
    res_state->ps.type = T_VecResultState;

    res_state->rs_done = false;
    res_state->rs_checkqual = (node->resconstantqual == NULL) ? false : true;

    /*
     * Miscellaneous initialization
     *
     * create expression context for node
     */
    ExecAssignExprContext(estate, &res_state->ps);

    res_state->ps.ps_TupFromTlist = false;

    /*
     * tuple table initialization
     */
    ExecInitResultTupleSlot(estate, &res_state->ps);

    /*
     * initialize child expressions
     */
    res_state->ps.targetlist = (List*)ExecInitVecExpr((Expr*)node->plan.targetlist, (PlanState*)res_state);
    res_state->ps.qual = (List*)ExecInitVecExpr((Expr*)node->plan.qual, (PlanState*)res_state);

    res_state->resconstantqual = ExecInitExpr((Expr*)node->resconstantqual, (PlanState*)res_state);

    /*
     * initialize child nodes
     */
    outerPlanState(res_state) = ExecInitNode(outerPlan(node), estate, eflags);

    /*
     * we don't use inner plan
     */
    Assert(innerPlan(node) == NULL);

    /*
     * initialize tuple type and projection info
     * no relations are involved in nodeResult, set the default
     * tableAm type to HEAP
     */
    ExecAssignResultTypeFromTL(&res_state->ps, TAM_HEAP);
    res_state->ps.ps_ProjInfo = ExecBuildVecProjectionInfo(
        res_state->ps.targetlist, node->plan.qual, res_state->ps.ps_ExprContext, res_state->ps.ps_ResultTupleSlot, NULL);

    ExecAssignVectorForExprEval(res_state->ps.ps_ExprContext);

    return res_state;
}

/* ----------------------------------------------------------------
 *		ExecEndResult
 *
 *		frees up storage allocated through C routines
 * ----------------------------------------------------------------
 */
void ExecEndVecResult(VecResultState* node)
{
    /*
     * Free the exprcontext
     */
    ExecFreeExprContext(&node->ps);

    (void)ExecClearTuple(node->ps.ps_ResultTupleSlot);
    /*
     * shut down subplans
     */
    ExecEndNode(outerPlanState(node));
}

void ExecReScanVecResult(VecResultState* node)
{
    node->rs_done = false;
    node->ps.ps_TupFromTlist = false;
    node->rs_checkqual = (node->resconstantqual == NULL) ? false : true;

    /*
     * If chgParam of subnode is not null then plan will be re-scanned by
     * first ExecProcNode.
     */
    if (node->ps.lefttree && node->ps.lefttree->chgParam == NULL) {
        VecExecReScan(node->ps.lefttree);
    }
}
