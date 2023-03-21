#include "postgres.h"
#include "knl/knl_variable.h"

#include "parser/parse_coerce.h"
#include "executor/exec/execdebug.h"
#include "executor/node/nodeProjectSet.h"
#include "executor/tuptable.h"
#include "utils/memutils.h"
#include "nodes/execnodes.h"
#include "nodes/plannodes.h"
#include "nodes/nodeFuncs.h"
#include "vecexecutor/vecnodes.h"
#include "executor/executor.h"

static TupleTableSlot *ExecProjectSet(PlanState *state);
static TupleTableSlot *ExecProjectSRF(ProjectSetState *node);

ProjectSetState *
ExecInitProjectSet(ProjectSet *node, EState *estate, int eflags)
{
    ProjectSetState *state;
    ListCell *lc;
    int off;

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
    if (estate->es_is_flt_frame) {

    } else {
        state->ps.targetlist = (List *)ExecInitExpr((Expr *)node->plan.targetlist, (PlanState *)state);
    }
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

    /* Create workspace for per-tlist-entry expr state & SRF-is-done state */
    state->nelems = list_length(node->plan.targetlist);
    state->elems = (Node **)palloc(sizeof(Node *) * state->nelems);
    state->elemdone = (ExprDoneCond *)palloc(sizeof(ExprDoneCond) * state->nelems);

    /*
     * Build expressions to evaluate targetlist.  We can't use
     * ExecBuildProjectionInfo here, since that doesn't deal with SRFs.
     * Instead compile each expression separately, using
     * ExecInitFunctionResultSet where applicable.
     */
    off = 0;
    foreach (lc, node->plan.targetlist) {
        TargetEntry *te = (TargetEntry *)lfirst(lc);
        Expr *expr = te->expr;

        if ((IsA(expr, FuncExpr) && ((FuncExpr *)expr)->funcretset) ||
            (IsA(expr, OpExpr) && ((OpExpr *)expr)->opretset)) {
            state->elems[off] = (Node *)ExecInitFunctionResultSet(expr, state->ps.ps_ExprContext, &state->ps);
        } else {
            Assert(!expression_returns_set((Node *)expr));
            state->elems[off] = (Node *)ExecInitExprByFlatten(expr, &state->ps);
        }

        off++;
    }

    /*
     * Create a memory context that ExecMakeFunctionResult can use to evaluate
     * function arguments in.  We can't use the per-tuple context for this
     * because it gets reset too often; but we don't want to leak evaluation
     * results into the query-lifespan context either.  We use one context for
     * the arguments of all tSRFs, as they have roughly equivalent lifetimes.
     */
    state->argcontext = AllocSetContextCreate(CurrentMemoryContext,
                                        "tSRF function arguments", ALLOCSET_DEFAULT_SIZES);

    return state;
}

static TupleTableSlot *ExecProjectSet(PlanState *state)
{
    ProjectSetState *node = castNode(ProjectSetState, state);
    TupleTableSlot *outerTupleSlot;
    TupleTableSlot *resultSlot;
    PlanState *outerPlan;
    ExprContext *econtext = node->ps.ps_ExprContext;

    /*
     * Reset per-tuple context to free expression-evaluation storage allocated
     * for a potentially previously returned tuple. Note that the SRF argument
     * context has a different lifetime and is reset below.
     */
    ResetExprContext(econtext);

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
     * Reset argument context to free any expression evaluation storage
     * allocated in the previous tuple cycle.  Note this can't happen until
     * we're done projecting out tuples from a scan tuple, as ValuePerCall
     * functions are allowed to reference the arguments for each returned
     * tuple.
     */
    MemoryContextReset(node->argcontext);

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
execMakeExprResult(Node *arg, ExprContext *econtext, MemoryContext argContext,
                   bool *isnull, ExprDoneCond *isdone, bool *hassrf)
{
    Datum result;
    if (IsA(arg, FuncExprState)) {
        /*
         * Evaluate SRF - possibly continuing previously started output.
         */
        result = ExecMakeFunctionResultSet((FuncExprState*)arg, econtext, argContext, isnull, isdone);
        *hassrf = true;
    } else {
        /* Non-SRF tlist expression, just evaluate normally. */
        result = ExecEvalExpr((ExprState *)arg, econtext, isnull);
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
    MemoryContext oldcontext;
    bool hassrf = false;
    bool hasresult = false;
    bool haveDoneSets = false; /* any exhausted set exprs in tlist? */
    ExprDoneCond isDone = ExprSingleResult;
    int argno;

    ExecClearTuple(resultSlot);

    /* Call SRFs, as well as plain expressions, in per-tuple context */
    oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

    /*
     * Assume no further tuples are produced unless an ExprMultipleResult is
     * encountered from a set returning function.
     */
    node->pending_srf_tuples = false;

    for (argno = 0; argno < node->nelems; argno++) {
        Node *elem = node->elems[argno];
        ExprDoneCond *itemIsDone = &node->elemdone[argno];
        Datum *result = &resultSlot->tts_values[argno];
        bool *isnull = &resultSlot->tts_isnull[argno];

        *result = execMakeExprResult(elem, econtext, node->argcontext,
                                     isnull, itemIsDone, &hassrf);

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
    }

    /* ProjectSet should not be used if there's no SRFs */
    Assert(hassrf);

    if (haveDoneSets) {
        if (isDone == ExprSingleResult) {
            /*
             * all sets are done, so report that tlist expansion is complete.
             */
            MemoryContextSwitchTo(oldcontext);
            return NULL;
        }

        /*
         * We have some done and some undone sets.	Restart the done ones
         * so that we can deliver a tuple (if possible).
         */
        for (argno = 0; argno < node->nelems; argno++) {
            Node *elem = node->elems[argno];
            ExprDoneCond *itemIsDone = &node->elemdone[argno];
            Datum *result = &resultSlot->tts_values[argno];
            bool *isnull = &resultSlot->tts_isnull[argno];

            if (*itemIsDone != ExprEndResult) {
                continue;
            }

            /*restart the done ones*/
            *result = ExecMakeFunctionResultSet((FuncExprState*)elem, econtext,
                                                node->argcontext, isnull, itemIsDone);

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

            for (argno = 0; argno < node->nelems; argno++) {
                Node *elem = node->elems[argno];
                ExprDoneCond *itemIsDone = &node->elemdone[argno];
                Datum *result = &resultSlot->tts_values[argno];
                bool *isnull = &resultSlot->tts_isnull[argno];

                while (*itemIsDone == ExprMultipleResult) {
                    *result = ExecMakeFunctionResultSet((FuncExprState*)elem, econtext,
                                                        node->argcontext, isnull, itemIsDone);
                    /* no need for MakeExpandedObjectReadOnly */
                }
            }
        }
    }

    MemoryContextSwitchTo(oldcontext);

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