#include "postgres.h"
#include "knl/knl_variable.h"

#include "parser/parse_coerce.h"
#include "executor/exec/execdebug.h"
#include "executor/node/nodeProjectSet.h"
#include "executor/tuptable.h"
#include "utils/memutils.h"
#include "nodes/execnodes.h"
#include "nodes/plannodes.h"
#include "vecexecutor/vecnodes.h"

static TupleTableSlot *ExecProjectSet(PlanState *state);
static TupleTableSlot *ExecProjectSRF(ProjectSetState *node);

ProjectSetState *
ExecInitProjectSet(ProjectSet *node, EState *estate, int eflags)
{
    ProjectSetState *state;

    /* check for unsupported flags */
    Assert(!(eflags & (EXEC_FLAG_MARK | EXEC_FLAG_BACKWARD)));

    /*
     * create state structure
     */
    state = makeNode(ProjectSetState);
    state->ps.plan = (Plan *)node;
    state->ps.state = estate;
    state->ps.ExecProcNode = ExecProjectSet;

    state->pending_srf_tuples = false;

    /*
     * Miscellaneous initialization
     *
     * create expression context for node
     */
    ExecAssignExprContext(estate, &state->ps);

    /*
     * tuple table initialization
     */
    ExecInitResultTupleSlot(estate, &state->ps);

    /*
     * initialize child expressions
     */
    state->ps.targetlist = (List *)ExecInitExpr((Expr *)node->plan.targetlist, (PlanState *)state);
    Assert(node->plan.qual == NIL);

    /*
     * initialize child nodes
     */
    outerPlanState(state) = ExecInitNode(outerPlan(node), estate, eflags);

    /*
     * we don't use inner plan
     */
    Assert(innerPlan(node) == NULL);

    /*
     * initialize tuple type and projection info
     */
    ExecAssignResultTypeFromTL(&state->ps);

    /* Create workspace for per-SRF is-done state */
    state->nelems = list_length(node->plan.targetlist);
    state->elemdone = (ExprDoneCond *)palloc(sizeof(ExprDoneCond) * state->nelems);

    return state;
}

static TupleTableSlot *ExecProjectSet(PlanState *state)
{
    ProjectSetState *node = castNode(ProjectSetState, state);
    TupleTableSlot *outerTupleSlot;
    TupleTableSlot *resultSlot;
    PlanState *outerPlan;
    ExprContext *econtext = node->ps.ps_ExprContext;

    CHECK_FOR_INTERRUPTS();

    /*
     * Check to see if we're still projecting out tuples from a previous scan
     * tuple (because there is a function-returning-set in the projection
     * expressions).  If so, try to project another one.
     */
    if (node->pending_srf_tuples) {
        resultSlot = ExecProjectSRF(node);

        if (resultSlot != NULL)
            return resultSlot;
    }

    /*
     * Reset per-tuple memory context to free any expression evaluation
     * storage allocated in the previous tuple cycle.  Note this can't happen
     * until we're done projecting out tuples from a scan tuple.
     */
    ResetExprContext(econtext);

    /*
     * Get another input tuple and project SRFs from it.
     */
    for (;;) {
        /*
         * Retrieve tuples from the outer plan until there are no more.
         */
        outerPlan = outerPlanState(node);
        outerTupleSlot = ExecProcNode(outerPlan);

        if (TupIsNull(outerTupleSlot))
            return NULL;

        /*
         * Prepare to compute projection expressions, which will expect to
         * access the input tuples as varno OUTER.
         */
        econtext->ecxt_outertuple = outerTupleSlot;

        /* Evaluate the expressions */
        resultSlot = ExecProjectSRF(node);

        /*
         * Return the tuple unless the projection produced no rows (due to an
         * empty set), in which case we must loop back to see if there are
         * more outerPlan tuples.
         */
        if (resultSlot)
            return resultSlot;
    }

    return NULL;
}

static inline Datum 
execMakeExprResult(ExprState *arg, ExprContext *econtext, bool *isnull, 
                    ExprDoneCond *isdone, bool *hassrf)
{

    Datum result;
    if (IsA(arg, FuncExprState) && ((FuncExprState *)arg)->funcReturnsSet) {
        /*
         * Evaluate SRF - possibly continuing previously started output.
         */
        result = ExecMakeFunctionResultSet((FuncExprState *)arg, econtext, isnull, isdone);
        *hassrf = true;
    } else {
        /* Non-SRF tlist expression, just evaluate normally. */
        result = ExecEvalExpr(arg, econtext, isnull, NULL);
        *isdone = ExprSingleResult;
    }    
    return result;
}

/* ----------------------------------------------------------------
 *		ExecProjectSRF
 *
 *		Project a targetlist containing one or more set-returning functions.
 *
 *		Returns NULL if no output tuple has been produced.
 *
 * ----------------------------------------------------------------
 */
static TupleTableSlot *ExecProjectSRF(ProjectSetState *node)
{
    TupleTableSlot *resultSlot = node->ps.ps_ResultTupleSlot;
    ExprContext *econtext = node->ps.ps_ExprContext;
    bool hassrf = false;
    bool hasresult = false;
    bool haveDoneSets = false; /* any exhausted set exprs in tlist? */
    ExprDoneCond isDone = ExprSingleResult;
    int argno;
    ListCell *lc;

    ExecClearTuple(resultSlot);

    /*
     * Assume no further tuples are produced unless an ExprMultipleResult is
     * encountered from a set returning function.
     */
    node->pending_srf_tuples = false;

    argno = 0;
    foreach (lc, node->ps.targetlist) {
        GenericExprState *gstate = (GenericExprState *)lfirst(lc);
        TargetEntry *tle = (TargetEntry *)gstate->xprstate.expr;

        ExprDoneCond *itemIsDone = &node->elemdone[argno];
        Datum *result = &resultSlot->tts_values[argno];
        bool *isnull = &resultSlot->tts_isnull[argno];

        ELOG_FIELD_NAME_START(tle->resname);
        *result = execMakeExprResult(gstate->arg, econtext, isnull, itemIsDone, &hassrf);
        ELOG_FIELD_NAME_END;

        switch (*itemIsDone) {
            case ExprSingleResult:
                hasresult = true;
                break;
            case ExprMultipleResult:
                hasresult = true;
                /* we have undone sets in the tlist, set flag */
                isDone = ExprMultipleResult;
                node->pending_srf_tuples = true;
                break;
            case ExprEndResult:
                /* we have done sets in the tlist, set flag for that */
                haveDoneSets = true;
                break;
            default:
                Assert(false);
                break;
        }
        argno++;
    }

    /* ProjectSet should not be used if there's no SRFs */
    Assert(hassrf);

    if (haveDoneSets) {
        if (isDone == ExprSingleResult) {
            /*
             * all sets are done, so report that tlist expansion is complete.
             */
            return NULL;
        }

        /*
         * We have some done and some undone sets.	Restart the done ones
         * so that we can deliver a tuple (if possible).
         */
        argno = 0;
        foreach (lc, node->ps.targetlist) {
            GenericExprState *gstate = (GenericExprState *)lfirst(lc);
            TargetEntry *tle = (TargetEntry *)gstate->xprstate.expr;

            ExprDoneCond *itemIsDone = &node->elemdone[argno];
            Datum *result = &resultSlot->tts_values[argno];
            bool *isnull = &resultSlot->tts_isnull[argno];
            if (*itemIsDone != ExprEndResult) {
                argno++;
                continue;
            }

            /*restart the done ones*/
            ELOG_FIELD_NAME_START(tle->resname);
            *result = execMakeExprResult(gstate->arg, econtext, isnull, itemIsDone, &hassrf);
            ELOG_FIELD_NAME_END;

            Assert(hassrf);

            if (*itemIsDone != ExprEndResult) {
                hasresult = true;
            }

            if (*itemIsDone == ExprMultipleResult) {
                isDone = ExprMultipleResult;
                node->pending_srf_tuples = true;
            }
            if (*itemIsDone == ExprEndResult) {
                /*
                 * Oh dear, this item is returning an empty set. Guess
                 * we can't make a tuple after all.
                 */
                isDone = ExprEndResult;
                break;
            }
            argno++;
        }

        /*
         * If we cannot make a tuple because some sets are empty, we still
         * have to cycle the nonempty sets to completion, else resources
         * will not be released from subplans etc.
         *
         * XXX is that still necessary?
         */
        if (isDone == ExprEndResult) {
            hasresult = false;
            argno = 0;
            foreach (lc, node->ps.targetlist) {
                GenericExprState *gstate = (GenericExprState *)lfirst(lc);
                TargetEntry *tle = (TargetEntry *)gstate->xprstate.expr;

                ExprDoneCond *itemIsDone = &node->elemdone[argno];
                Datum *result = &resultSlot->tts_values[argno];
                bool *isnull = &resultSlot->tts_isnull[argno];

                while (*itemIsDone == ExprMultipleResult) {
                    ELOG_FIELD_NAME_START(tle->resname);
                    *result = execMakeExprResult(gstate->arg, econtext, isnull, itemIsDone, &hassrf);
                    ELOG_FIELD_NAME_END;
                    /* no need for MakeExpandedObjectReadOnly */
                }
                
                argno++;
            }
        }
    }

    if (hasresult) {
        ExecStoreVirtualTuple(resultSlot);
        return resultSlot;
    }
    return NULL; /* all sets are done */
}

void ExecEndProjectSet(ProjectSetState *node)
{
    /*
     * Free the exprcontext
     */
    ExecFreeExprContext(&node->ps);

    /*
     * clean out the tuple table
     */
    ExecClearTuple(node->ps.ps_ResultTupleSlot);

    /*
     * shut down subplans
     */
    ExecEndNode(outerPlanState(node));
}

void ExecReScanProjectSet(ProjectSetState *node)
{
    /* Forget any incompletely-evaluated SRFs */
    node->pending_srf_tuples = false;

    /*
        * If chgParam of subnode is not null then plan will be re-scanned by
        * first ExecProcNode.
        */
    if (node->ps.lefttree->chgParam == NULL)
        ExecReScan(node->ps.lefttree);
}