/* -------------------------------------------------------------------------
 *
 * nodeResult.cpp
 *	  support for constant nodes needing special code.
 *
 * DESCRIPTION
 *
 *		Result nodes are used in queries where no relations are scanned.
 *		Examples of such queries are:
 *
 *				select 1 * 2
 *
 *				insert into emp values ('mike', 15000)
 *
 *		(Remember that in an INSERT or UPDATE, we need a plan tree that
 *		generates the new rows.)
 *
 *		Result nodes are also used to optimise queries with constant
 *		qualifications (ie, quals that do not depend on the scanned data),
 *		such as:
 *
 *				select * from emp where 2 > 1
 *
 *		In this case, the plan generated is
 *
 *						Result	(with 2 > 1 qual)
 *						/
 *				   SeqScan (emp.*)
 *
 *		At runtime, the Result node evaluates the constant qual once,
 *		which is shown by EXPLAIN as a One-Time Filter.  If it's
 *		false, we can return an empty result set without running the
 *		controlled plan at all.  If it's true, we run the controlled
 *		plan normally and pass back the results.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/gausskernel/runtime/executor/nodeResult.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "executor/executor.h"
#include "executor/node/nodeResult.h"
#include "utils/memutils.h"

/* ----------------------------------------------------------------
 *		ExecResult(node)
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
static TupleTableSlot* ExecResult(PlanState* state)
{
    ResultState* node = castNode(ResultState, state);
    TupleTableSlot* outer_tuple_slot = NULL;
    TupleTableSlot* result_slot = NULL;
    PlanState* outer_plan = NULL;
    ExprDoneCond is_done;
    ExprContext* econtext = node->ps.ps_ExprContext;

    CHECK_FOR_INTERRUPTS();
    
    /*
     * check constant qualifications like (2 > 1), if not already done
     */
    if ((node->rs_checkqual || (u_sess->parser_cxt.has_set_uservar && DB_IS_CMPT(B_FORMAT))) &&
        !u_sess->exec_cxt.has_equal_uservar) {
        bool qualResult = ExecQual((List*)node->resconstantqual, econtext, false);

        node->rs_checkqual = false;
        if (!qualResult) {
            node->rs_done = true;
            /*
             * Mark this constant qualification check failure for future corner cases. Currently only used in GDS
             * foreign scan
             */
            u_sess->exec_cxt.exec_result_checkqual_fail = true;
            return NULL;
        }
    }

    /*
     * Reset per-tuple memory context to free any expression evaluation
     * storage allocated in the previous tuple cycle.  Note this can't happen
     * until we're done projecting out tuples from a scan tuple.
     */
    if (!econtext->hasSetResultStore) {
        /* return value one by one, just free early one */
        ResetExprContext(econtext);
    }

    /*
     * Check to see if we're still projecting out tuples from a previous scan
     * tuple (because there is a function-returning-set in the projection
     * expressions).  If so, try to project another one.
     */
    if (node->ps.ps_vec_TupFromTlist) {
        result_slot = ExecProject(node->ps.ps_ProjInfo, &is_done);
        if (is_done == ExprMultipleResult) {
            if (u_sess->exec_cxt.has_equal_uservar) {
                u_sess->exec_cxt.has_equal_uservar = false;
                bool qualResult = ExecQual((List*)node->resconstantqual, econtext, false);

                node->rs_checkqual = false;
                if (!qualResult) {
                    node->rs_done = true;
                    /*
                    * Mark this constant qualification check failure for future corner cases. Currently only used in GDS
                    * foreign scan
                    */
                    u_sess->exec_cxt.exec_result_checkqual_fail = true;
                    return NULL;
                }
            }
            return result_slot;
        }
        /* Done with that source tuple... */
        node->ps.ps_vec_TupFromTlist = false;
    }

    if (econtext->hasSetResultStore) {
        /* return values all store in ResultStore, could not free early one */
        ResetExprContext(econtext);
        econtext->hasSetResultStore = false;
    }

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
            outer_tuple_slot = ExecProcNode(outer_plan);
            if (TupIsNull(outer_tuple_slot))
                return NULL;

            /*
             * prepare to compute projection expressions, which will expect to
             * access the input tuples as varno OUTER.
             */
            econtext->ecxt_outertuple = outer_tuple_slot;

            if (node->ps.qual && !ExecQual(node->ps.qual, econtext, false))
                continue;
        } else {
            /*
             * if we don't have an outer plan, then we are just generating the
             * results from a constant target list.  Do it only once.
             */
            node->rs_done = true;
        }

        if (u_sess->exec_cxt.has_equal_uservar && node->resconstantqual != NULL) {
            bool qualResult = ExecQual((List*)node->resconstantqual, econtext, false);

            node->rs_checkqual = false;
            if (!qualResult) {
                node->rs_done = true;
                /*
                * Mark this constant qualification check failure for future corner cases. Currently only used in GDS
                * foreign scan
                */
                u_sess->exec_cxt.exec_result_checkqual_fail = true;
                return NULL;
            }
        }

        /*
         * form the result tuple using ExecProject(), and return it --- unless
         * the projection produces an empty set, in which case we must loop
         * back to see if there are more outer_plan tuples.
         */
        result_slot = ExecProject(node->ps.ps_ProjInfo, &is_done);

        if (u_sess->exec_cxt.has_equal_uservar && node->resconstantqual != NULL) {
            u_sess->exec_cxt.has_equal_uservar = false;
            bool qualResult = ExecQual((List*)node->resconstantqual, econtext, false);

            node->rs_checkqual = false;
            if (!qualResult) {
                node->rs_done = true;
                /*
                * Mark this constant qualification check failure for future corner cases. Currently only used in GDS
                * foreign scan
                */
                u_sess->exec_cxt.exec_result_checkqual_fail = true;
                return NULL;
            }
        }
        if (is_done != ExprEndResult) {
            node->ps.ps_vec_TupFromTlist = (is_done == ExprMultipleResult);
            return result_slot;
        }
    }

    return NULL;
}

/* ----------------------------------------------------------------
 *		ExecResultMarkPos
 * ----------------------------------------------------------------
 */
void ExecResultMarkPos(ResultState* node)
{
    PlanState* outer_plan = outerPlanState(node);

    if (outer_plan != NULL) {
        ExecMarkPos(outer_plan);
    } else {
        elog(DEBUG2, "Result nodes do not support mark/restore");
    }
}

/* ----------------------------------------------------------------
 *		ExecResultRestrPos
 * ----------------------------------------------------------------
 */
void ExecResultRestrPos(ResultState* node)
{
    PlanState* outer_plan = outerPlanState(node);

    if (outer_plan != NULL) {
        ExecRestrPos(outer_plan);
    } else {
        ereport(ERROR,
            (errmodule(MOD_EXECUTOR),
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Result nodes do not support mark/restore"))));
    }
}

/* ----------------------------------------------------------------
 *		ExecInitResult
 *
 *		Creates the run-time state information for the result node
 *		produced by the planner and initializes outer relations
 *		(child nodes).
 * ----------------------------------------------------------------
 */
ResultState* ExecInitResult(BaseResult* node, EState* estate, int eflags)
{
    /* check for unsupported flags */
    Assert(!(eflags & (EXEC_FLAG_MARK | EXEC_FLAG_BACKWARD)) || outerPlan(node) != NULL);

    /*
     * create state structure
     */
    ResultState* resstate = makeNode(ResultState);
    resstate->ps.plan = (Plan*)node;
    resstate->ps.state = estate;
    resstate->ps.ExecProcNode = ExecResult;

    resstate->rs_done = false;
    resstate->rs_checkqual = (node->resconstantqual == NULL) ? false : true;

    /*
     * Miscellaneous initialization
     *
     * create expression context for node
     */
    ExecAssignExprContext(estate, &resstate->ps);

    resstate->ps.ps_vec_TupFromTlist = false;

    /*
     * tuple table initialization
     */
    ExecInitResultTupleSlot(estate, &resstate->ps);

    /*
     * initialize child expressions
     */
    if (estate->es_is_flt_frame) {
        resstate->ps.qual = (List*)ExecInitQualByFlatten(node->plan.qual, (PlanState*)resstate);
        resstate->resconstantqual = ExecInitQualByFlatten((List*)node->resconstantqual, (PlanState*)resstate);
    } else {
        resstate->ps.targetlist = (List*)ExecInitExpr((Expr*)node->plan.targetlist, (PlanState*)resstate);
        resstate->ps.qual = (List*)ExecInitExprByRecursion((Expr*)node->plan.qual, (PlanState*)resstate);
        resstate->resconstantqual = ExecInitExprByRecursion((Expr*)node->resconstantqual, (PlanState*)resstate);
    }
    /*
     * initialize child nodes
     */
    outerPlanState(resstate) = ExecInitNode(outerPlan(node), estate, eflags);

    /*
     * we don't use inner plan
     */
    Assert(innerPlan(node) == NULL);

    /*
     * initialize tuple type and projection info
     * no relations are involved in nodeResult, set the default
     * tableAm type to HEAP
     */
    ExecAssignResultTypeFromTL(&resstate->ps);

    ExecAssignProjectionInfo(&resstate->ps, NULL);

    return resstate;
}

/* ----------------------------------------------------------------
 *		ExecEndResult
 *
 *		frees up storage allocated through C routines
 * ----------------------------------------------------------------
 */
void ExecEndResult(ResultState* node)
{
    /*
     * Free the exprcontext
     */
    ExecFreeExprContext(&node->ps);

    /*
     * clean out the tuple table
     */
    (void)ExecClearTuple(node->ps.ps_ResultTupleSlot);

    /*
     * shut down subplans
     */
    ExecEndNode(outerPlanState(node));
}

void ExecReScanResult(ResultState* node)
{
    node->rs_done = false;
    node->ps.ps_vec_TupFromTlist = false;
    node->rs_checkqual = (node->resconstantqual == NULL) ? false : true;

    /*
     * If chgParam of subnode is not null then plan will be re-scanned by
     * first ExecProcNode.
     */
    if (node->ps.lefttree && node->ps.lefttree->chgParam == NULL) {
        ExecReScan(node->ps.lefttree);
    }
}
