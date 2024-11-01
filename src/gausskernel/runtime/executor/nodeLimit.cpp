/* -------------------------------------------------------------------------
 *
 * nodeLimit.cpp
 *	  Routines to handle limiting of query results where appropriate
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/runtime/executor/nodeLimit.cpp
 *
 * -------------------------------------------------------------------------
 *
 * INTERFACE ROUTINES
 *		ExecLimit		- extract a limited range of tuples
 *		ExecInitLimit	- initialize node and subnodes..
 *		ExecEndLimit	- shutdown node and subnodes
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "executor/executor.h"
#include "executor/node/nodeLimit.h"
#include "nodes/nodeFuncs.h"
#include "instruments/instr_statement.h"

#define REPORT_LIMIT_THRESHOLD 5000 /* report cause_type's threshold for limit */

static TupleTableSlot* ExecLimit(PlanState* state);
static void pass_down_bound(LimitState* node, PlanState* child_node);
static TupleTableSlot* exec_get_tuple(LimitState* node);

static constexpr double PERCENTAGE_BASE = 100;
/* ----------------------------------------------------------------
 *		ExecLimit
 *
 *		This is a very simple node which just performs LIMIT/OFFSET
 *		filtering on the stream of tuples returned by a subplan.
 * ----------------------------------------------------------------
 */
static TupleTableSlot* ExecLimit(PlanState* state) /* return: a tuple or NULL */
{
    LimitState* node = castNode(LimitState, state);
    Plan* plan = node->ps.plan;
    ScanDirection direction;
    TupleTableSlot* slot = NULL;
    PlanState* outer_plan = NULL;
    TupleTableSlot* result_tuple_slot = node->ps.ps_ResultTupleSlot;

    CHECK_FOR_INTERRUPTS();

    /*
     * get information from the node
     */
    direction = node->ps.state->es_direction;
    outer_plan = outerPlanState(node);

    /*
     * The main logic is a simple state machine.
     */
    switch (node->lstate) {
        case LIMIT_INITIAL:

            /*
             * First call for this node, so compute limit/offset. (We can't do
             * this any earlier, because parameters from upper nodes will not
             * be set during ExecInitLimit.)  This also sets position = 0 and
             * changes the state to LIMIT_RESCAN.
             */
            recompute_limits(node);

            /* fall through */
        case LIMIT_RESCAN:

            /*
             * If backwards scan, just return NULL without changing state.
             */
            if (!ScanDirectionIsForward(direction))
                return NULL;

            /*
             * Check for empty window; if so, treat like empty subplan.
             */
            if (!node->noCount && 
                ((node->count <= 0 && !node->isPercent) || 
                (node->isPercent && node->fraction <= 0))) {
                node->lstate = LIMIT_EMPTY;
                return NULL;
            }

            if (node->withTies) {
                if (node->outputSlot == NULL) {
                    node->outputSlot = MakeSingleTupleTableSlot(ExecGetResultType(outer_plan));
                } else {
                    (void)ExecClearTuple(node->outputSlot);
                }
            }

            /*
             * Fetch rows from subplan until we reach position > offset.
             */
            for (;;) {
                slot = ExecProcNode(outer_plan);
                if (TupIsNull(slot)) {
                    /*
                     * The subplan returns too few tuples for us to produce
                     * any output at all.
                     */
                    node->lstate = LIMIT_EMPTY;
                    return NULL;
                }
                node->subSlot = slot;
                
                if (++node->position > node->offset)
                    break;
            }

            if (node->isPercent) {
                int64 operator_mem = SET_NODEMEM(plan->operatorMemKB[0], plan->dop);
                int64 max_mem = (plan->operatorMaxMem > 0) ? SET_NODEMEM(plan->operatorMaxMem, plan->dop) : 0;
                size_t tuple_num = node->position;
                TupleTableSlot* result_tuple_slot = node->ps.ps_ResultTupleSlot;

                /*
                 * We have a new tuple different from the previous saved tuple (if any).
                 * We must copy it because the source subplan won't guarantee that 
                 * this source tuple is still accessible after fetching the next source tuple.
                 */
                ExecCopySlot(result_tuple_slot, slot);

                /* Initialize Tuple store */
                if (node->tuplestorestate) {
                    tuplestore_end(node->tuplestorestate);
                    node->tuplestorestate = NULL;
                }
                node->tuplestorestate = 
                            tuplestore_begin_heap(true, false, operator_mem, max_mem, 
                                plan->plan_node_id, SET_DOP(plan->dop), true);

                /* Fetching all the results from subplan */
                TupleTableSlot* next_slot = ExecProcNode(outer_plan);
                while (!TupIsNull(next_slot)) {
                    tuplestore_puttupleslot(node->tuplestorestate, next_slot);
                    next_slot = ExecProcNode(outer_plan);
                    tuple_num++;
                }

                node->count = std::ceil(node->fraction * tuple_num);
                if (node->count <= 0) {
                    node->lstate = LIMIT_EMPTY;
                    return NULL;
                }

                EARLY_FREE_LOG(elog(LOG,
                    "Early Free: Before completing the fetch percent "
                    "at node %d, memory used %d MB.",
                    plan_node->plan.plan_node_id,
                    getSessionMemoryUsageMB()));

                /* Finish scanning the subplan, it's safe to early free the memory of lefttree */
                ExecEarlyFree(outerPlanState(node));

                EARLY_FREE_LOG(elog(LOG,
                    "Early Free: After completing the fetch percent "
                    "at node %d, memory used %d MB.",
                    plan_node->plan.plan_node_id,
                    getSessionMemoryUsageMB()));
                
                /* restore next tuple from result_tuple_slot */
                slot = result_tuple_slot;
                node->subSlot = slot;
                Assert(!TupIsNull(slot));
            }

            /*
             * Okay, we have the first tuple of the window.
             */
            node->lstate = LIMIT_INWINDOW;
            break;

        case LIMIT_EMPTY:

            /*
             * The subplan is known to return no tuples (or not more than
             * OFFSET tuples, in general).	So we return no tuples.
             */
            return NULL;

        case LIMIT_INWINDOW:
            if (ScanDirectionIsForward(direction)) {
                /*
                 * Forwards scan, so check for stepping off end of window. If
                 * we are at the end of the window, return NULL without
                 * advancing the subplan or the position variable; but change
                 * the state machine state to record having done so.
                 */
                if (!node->noCount && node->position - node->offset >= node->count) {
                    if (node->withTies) {
                        Assert(node->outputSlot != NULL);
                        slot = exec_get_tuple(node);
                        if (TupIsNull(slot)) {
                            node->lstate = LIMIT_WINDOWEND;
                            return NULL;
                        }
                        if (execTuplesMatch(slot, node->outputSlot, node->numCols, node->sortColIdx,
                                            node->eqfunctions, node->tempContext, node->collations)) {
                            node->subSlot = slot;
                            node->position++;
                            return slot;
                        } else {
                            node->lstate = LIMIT_WINDOWEND;
                            return NULL;
                        }
                    }
                    node->lstate = LIMIT_WINDOWEND;
                    return NULL;
                }

                /*
                 * Get next tuple from subplan, if any.
                 */
                slot = exec_get_tuple(node);
                if (TupIsNull(slot)) {
                    node->lstate = LIMIT_SUBPLANEOF;
                    return NULL;
                }
                node->subSlot = slot;
                node->position++;
            } else {
                /*
                 * Backwards scan, so check for stepping off start of window.
                 * As above, change only state-machine status if so.
                 */
                if (node->position <= node->offset + 1) {
                    node->lstate = LIMIT_WINDOWSTART;
                    return NULL;
                }

                /*
                 * Get previous tuple from subplan; there should be one!
                 */
                slot = exec_get_tuple(node);
                if (TupIsNull(slot))
                    ereport(ERROR,
                        (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                            errmodule(MOD_EXECUTOR),
                            errmsg("LIMIT subplan failed to run backwards")));
                node->subSlot = slot;
                node->position--;
            }
            break;

        case LIMIT_SUBPLANEOF:
            if (ScanDirectionIsForward(direction))
                return NULL;

            /*
             * Backing up from subplan EOF, so re-fetch previous tuple; there
             * should be one!  Note previous tuple must be in window.
             */
            slot = exec_get_tuple(node);
            if (TupIsNull(slot))
                ereport(ERROR,
                    (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                        errmodule(MOD_EXECUTOR),
                        errmsg("LIMIT subplan failed to run backwards")));
            node->subSlot = slot;
            node->lstate = LIMIT_INWINDOW;
            /* position does not change 'cause we didn't advance it before */
            break;

        case LIMIT_WINDOWEND:
            if (ScanDirectionIsForward(direction))
                return NULL;

            /*
             * Backing up from window end: simply re-return the last tuple
             * fetched from the subplan.
             */
            slot = node->subSlot;
            node->lstate = LIMIT_INWINDOW;
            /* position does not change 'cause we didn't advance it before */
            break;

        case LIMIT_WINDOWSTART:
            if (!ScanDirectionIsForward(direction))
                return NULL;

            /*
             * Advancing after having backed off window start: simply
             * re-return the last tuple fetched from the subplan.
             */
            slot = node->subSlot;
            node->lstate = LIMIT_INWINDOW;
            /* position does not change 'cause we didn't change it before */
            break;

        default:
            ereport(ERROR,
                (errcode(ERRCODE_UNEXPECTED_NODE_STATE),
                    errmodule(MOD_EXECUTOR),
                    errmsg("impossible LIMIT state: %d", (int)node->lstate)));
            slot = NULL; /* keep compiler quiet */
            break;
    }

    /* Return the current tuple */
    Assert(!TupIsNull(slot));

    if (node->withTies && node->position - node->offset == node->count) {
        ExecCopySlot(node->outputSlot, slot);
    }

    return slot;
}

/*
 * Evaluate the limit/offset expressions --- done at startup or rescan.
 *
 * This is also a handy place to reset the current-position state info.
 */
void recompute_limits(LimitState* node)
{
    ExprContext* econtext = node->ps.ps_ExprContext;
    Datum val;
    bool is_null = false;

    if (node->limitOffset) {
        val = ExecEvalExprSwitchContext(node->limitOffset, econtext, &is_null, NULL);
        /* Interpret NULL offset as no offset */
        if (is_null) {
            node->offset = 0;
        } else {
            node->offset = DatumGetInt64(val);
            if (node->offset < 0)
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_ROW_COUNT_IN_RESULT_OFFSET_CLAUSE),
                        errmodule(MOD_EXECUTOR),
                        errmsg("OFFSET must not be negative")));
        }
    } else {
        /* No OFFSET supplied */
        node->offset = 0;
    }

    if (node->limitCount) {
        val = ExecEvalExprSwitchContext(node->limitCount, econtext, &is_null, NULL);
        /* Interpret NULL count as no count (LIMIT ALL) */
        if (is_null) {
            node->count = 0;
            node->noCount = true;
        } else if (!node->isPercent) {
            node->count = DatumGetInt64(val);
            node->fraction = 0;
            if (node->count < 0)
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_ROW_COUNT_IN_LIMIT_CLAUSE),
                        errmodule(MOD_EXECUTOR),
                        errmsg("LIMIT must not be negative")));
            node->noCount = false;
        } else if (node->isPercent) {
            node->count = 0;
            node->fraction = DatumGetFloat8(val) / PERCENTAGE_BASE;
            if (node->fraction < 0)
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_ROW_COUNT_IN_LIMIT_CLAUSE),
                        errmodule(MOD_EXECUTOR),
                        errmsg("LIMIT must not be negative")));
            node->noCount = false;
        }
    } else {
        /* No COUNT supplied */
        node->count = 0;
        node->noCount = true;
        node->fraction = 0;
    }

    /*
     * Check whether there are risks caused by limit to much rows.
     */
    if (!node->noCount && node->count >= REPORT_LIMIT_THRESHOLD)
        instr_stmt_report_cause_type(NUM_F_LIMIT);

    /* Reset position to start-of-scan */
    node->position = 0;
    node->subSlot = NULL;
    (void)ExecClearTuple(node->ps.ps_ResultTupleSlot);

    /* Set state-machine state */
    node->lstate = LIMIT_RESCAN;

    /* Notify child node about limit, if useful */
    if (!node->withTies && !node->isPercent)
        pass_down_bound(node, outerPlanState(node));
}

/*
 * If we have a COUNT, and our input is a Sort node, notify it that it can
 * use bounded sort.  Also, if our input is a MergeAppend, we can apply the
 * same bound to any Sorts that are direct children of the MergeAppend,
 * since the MergeAppend surely need read no more than that many tuples from
 * any one input.  We also have to be prepared to look through a Result,
 * since the planner might stick one atop MergeAppend for projection purposes.
 *
 * This is a bit of a kluge, but we don't have any more-abstract way of
 * communicating between the two nodes; and it doesn't seem worth trying
 * to invent one without some more examples of special communication needs.
 *
 * Note: it is the responsibility of nodeSort.c to react properly to
 * changes of these parameters.  If we ever do redesign this, it'd be a
 * good idea to integrate this signaling with the parameter-change mechanism.
 */
static void pass_down_bound(LimitState* node, PlanState* child_node)
{
    /* Note: if this overflows, we'll return a negative value, which is OK */
    int64 tuples_needed = node->noCount ? -1 : (node->count + node->offset); 
    if (IsA(child_node, SortState) || IsA(child_node, VecSortState)) {
        SortState* sortState = (SortState*)child_node;
        /* negative test checks for overflow in sum */
        if (tuples_needed < 0) {
            /* make sure flag gets reset if needed upon rescan */
            sortState->bounded = false;
        } else {
            sortState->bounded = true;
            sortState->bound = tuples_needed;
        }
    } else if (IsA(child_node, MergeAppendState)) {
        MergeAppendState* maState = (MergeAppendState*)child_node;
        int i;

        for (i = 0; i < maState->ms_nplans; i++)
            pass_down_bound(node, maState->mergeplans[i]);
    } else if (IsA(child_node, ResultState) || IsA(child_node, VecResultState)) {
        /*
         * An extra consideration here is that if the Result is projecting a
         * targetlist that contains any SRFs, we can't assume that every input
         * tuple generates an output tuple, so a Sort underneath might need to
         * return more than N tuples to satisfy LIMIT N. So we cannot use
         * bounded sort.
         *
         * If Result supported qual checking, we'd have to punt on seeing a
         * qual, too.  Note that having a resconstantqual is not a
         * showstopper: if that fails we're not getting any rows at all.
         */
        if (outerPlanState(child_node) && !expression_returns_set((Node*)child_node->plan->targetlist))
            pass_down_bound(node, outerPlanState(child_node));
    } else if (IsA(child_node, AggState)) {
        if (tuples_needed > 0) {
            child_node = outerPlanState((AggState *)child_node);
            if (IsA(child_node, SortGroupState)) {
                SortGroupState *sortGroup = (SortGroupState *)child_node;
                sortGroup->bound = tuples_needed;
            }
        }
    }
}

/**
 * @brief Retrieves a tuple from the LimitState node.
 *
 * This function is responsible for fetching a tuple from the outer plan of the LimitState node.
 * If the tuplestorestate is not NULL, it retrieves the tuple from the tuplestorestate.
 * Otherwise, it calls ExecProcNode to fetch the tuple from the outer plan.
 *
 * @param node The LimitState node.
 * @return The TupleTableSlot containing the fetched tuple.
 */
static TupleTableSlot* exec_get_tuple(LimitState* node)
{
    PlanState* outer_plan = outerPlanState(node);
    TupleTableSlot* slot = NULL;
    bool dir = ScanDirectionIsForward(node->ps.state->es_direction);

    if (node->tuplestorestate == NULL) {
        slot = ExecProcNode(outer_plan);
    } else {
        slot = node->ps.ps_ResultTupleSlot;
        if (!tuplestore_gettupleslot(node->tuplestorestate, dir, false, slot)) {
            return NULL;
        }
    }
    return slot;
}

/* ----------------------------------------------------------------
 *		ExecInitLimit
 *
 *		This initializes the limit node state structures and
 *		the node's subplan.
 * ----------------------------------------------------------------
 */
LimitState* ExecInitLimit(Limit* node, EState* estate, int eflags)
{
    LimitState* limit_state = NULL;
    Plan* outer_plan = NULL;

    /* check for unsupported flags */
    Assert(!(eflags & EXEC_FLAG_MARK));

    int64 operator_mem = SET_NODEMEM(((Plan*)node)->operatorMemKB[0], ((Plan*)node)->dop);
    AllocSetContext* set = (AllocSetContext*)(estate->es_query_cxt);
    set->maxSpaceSize = operator_mem * 1024L + SELF_GENRIC_MEMCTX_LIMITATION;

    /*
     * create state structure
     */
    limit_state = makeNode(LimitState);
    limit_state->ps.plan = (Plan*)node;
    limit_state->ps.state = estate;

    limit_state->lstate = LIMIT_INITIAL;
    limit_state->ps.ExecProcNode = ExecLimit;

    /*
     * Miscellaneous initialization
     *
     * Limit nodes never call ExecQual or ExecProject, but they need an
     * exprcontext anyway to evaluate the limit/offset parameters in.
     */
    ExecAssignExprContext(estate, &limit_state->ps);

    limit_state->tempContext = AllocSetContextCreate(
        CurrentMemoryContext, "limit", ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);

    /*
     * initialize child expressions
     */
    limit_state->limitOffset = ExecInitExpr((Expr*)node->limitOffset, (PlanState*)limit_state);
    limit_state->limitCount = ExecInitExpr((Expr*)node->limitCount, (PlanState*)limit_state);
    limit_state->isPercent = node->isPercent;
    limit_state->withTies = node->withTies;
    limit_state->numCols = node->numCols;
    limit_state->sortColIdx = node->sortColIdx;
    limit_state->collations = node->collations;

    /*
     * Tuple table initialization (XXX not actually used...)
     */
    ExecInitResultTupleSlot(estate, &limit_state->ps);
    /*
     * then initialize outer plan
     */
    outer_plan = outerPlan(node);
    outerPlanState(limit_state) = ExecInitNode(outer_plan, estate, eflags);

    /*
     * limit nodes do no projections, so initialize projection info for this
     * node appropriately
     */
    ExecAssignResultTypeFromTL(
            &limit_state->ps,
            ExecGetResultType(outerPlanState(limit_state))->td_tam_ops);

    limit_state->ps.ps_ProjInfo = NULL;

    /*
     * Precompute fmgr lookup data for inner loop
     */
    limit_state->eqfunctions = execTuplesMatchPrepare(node->numCols, node->equalOperators);

    return limit_state;
}

/* ----------------------------------------------------------------
 *		ExecEndLimit
 *
 *		This shuts down the subplan and frees resources allocated
 *		to this node.
 * ----------------------------------------------------------------
 */
void ExecEndLimit(LimitState* node)
{
    ExecFreeExprContext(&node->ps);

    /*
     * Release tuplestore resources
     */
    if (node->tuplestorestate != NULL)
        tuplestore_end(node->tuplestorestate);
    node->tuplestorestate = NULL;

    if (node->ps.ps_ResultTupleSlot)
        (void)ExecClearTuple(node->ps.ps_ResultTupleSlot);
    
    if (node->outputSlot)
        ExecDropSingleTupleTableSlot(node->outputSlot);
    
    MemoryContextDelete(node->tempContext);

    ExecEndNode(outerPlanState(node));
}

void ExecReScanLimit(LimitState* node)
{
    /*
     * Recompute limit/offset in case parameters changed, and reset the state
     * machine.  We must do this before rescanning our child node, in case
     * it's a Sort that we are passing the parameters down to.
     */
    recompute_limits(node);

    if (node->ps.ps_ResultTupleSlot)
        (void)ExecClearTuple(node->ps.ps_ResultTupleSlot);

    if (node->tuplestorestate) {
        tuplestore_end(node->tuplestorestate);
        node->tuplestorestate = NULL;
    }

    if (node->outputSlot)
        (void)ExecClearTuple(node->outputSlot);

    /*
     * if chgParam of subnode is not null then plan will be re-scanned by
     * first ExecProcNode.
     */
    if (node->ps.lefttree->chgParam == NULL)
        ExecReScan(node->ps.lefttree);
}
