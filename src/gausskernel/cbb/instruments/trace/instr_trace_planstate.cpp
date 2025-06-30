/*-------------------------------------------------------------------------
 * MIT License
 * Copyright (c) 2024, Datadog, Inc.
 *
 * Based on github's /DataDog/pg_tracing reconstruction
 * instr_trace_planstate.cpp
 *         instr trace planstate manipulation functions.
 *
 * IDENTIFICATION
 *      src/gausskernel/cbb/instruments/trace/instr_trace_planstate.cpp
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/datavec/pg_prng.h"
#include "nodes/nodeFuncs.h"
#include "instr_trace_inner.h"
#include "utils/timestamp.h"

#define US_IN_S INT64CONST(1000000)
#define INITIAL_NUM_PLANSTATE 20
#define INCREMENT_NUM_PLANSTATE 20

static ExecProcNodeMtd previous_ExecProcNode = NULL;
TimestampTz create_spans_from_plan_list(PlanState **planstates, int nplans,
    PlanstateTraceContext *planstateTraceContext, uint64 parent_id, uint64 query_id,
    TimestampTz parent_start, TimestampTz root_end, TimestampTz *latest_end);
TimestampTz create_spans_from_bitmap_nodes(PlanState **planstates,
    int nplans, PlanstateTraceContext *planstateTraceContext, uint64 parent_id,
    uint64 query_id, TimestampTz parent_start, TimestampTz root_end, TimestampTz *latest_end);

/*
 * Get the node type name from a plan node
 */
SpanType plan_to_span_type(const Plan *plan)
{
    switch (nodeTag(plan)) {
        case T_Result:
            return SPAN_NODE_RESULT;
        case T_ProjectSet:
            return SPAN_NODE_PROJECT_SET;
        case T_ModifyTable:
            switch (((ModifyTable *) plan)->operation) {
                case CMD_INSERT:
                    return SPAN_NODE_INSERT;
                case CMD_UPDATE:
                    return SPAN_NODE_UPDATE;
                case CMD_DELETE:
                    return SPAN_NODE_DELETE;
                case CMD_MERGE:
                    return SPAN_NODE_MERGE;
                default:
                    return SPAN_NODE_UNKNOWN;
            }
        case T_Append:
            return SPAN_NODE_APPEND;
        case T_MergeAppend:
            return SPAN_NODE_MERGE_APPEND;
        case T_RecursiveUnion:
            return SPAN_NODE_RECURSIVE_UNION;
        case T_BitmapAnd:
            return SPAN_NODE_BITMAP_AND;
        case T_BitmapOr:
            return SPAN_NODE_BITMAP_OR;
        case T_NestLoop:
            return SPAN_NODE_NESTLOOP;
        case T_MergeJoin:
            return SPAN_NODE_MERGE_JOIN;
        case T_HashJoin:
            return SPAN_NODE_HASH_JOIN;
        case T_SeqScan:
            return SPAN_NODE_SEQ_SCAN;
        case T_Gather:
            return SPAN_NODE_GATHER;
        case T_IndexScan:
            return SPAN_NODE_INDEX_SCAN;
        case T_IndexOnlyScan:
            return SPAN_NODE_INDEX_ONLY_SCAN;
        case T_BitmapIndexScan:
            return SPAN_NODE_BITMAP_INDEX_SCAN;
        case T_BitmapHeapScan:
            return SPAN_NODE_BITMAP_HEAP_SCAN;
        case T_TidScan:
            return SPAN_NODE_TID_SCAN;
        case T_TidRangeScan:
            return SPAN_NODE_TID_RANGE_SCAN;
        case T_SubqueryScan:
            return SPAN_NODE_SUBQUERY_SCAN;
        case T_FunctionScan:
            return SPAN_NODE_FUNCTION_SCAN;
        case T_ValuesScan:
            return SPAN_NODE_VALUES_SCAN;
        case T_CteScan:
            return SPAN_NODE_CTE_SCAN;
        case T_WorkTableScan:
            return SPAN_NODE_WORKTABLE_SCAN;
        case T_ForeignScan:
            switch (((ForeignScan *) plan)->operation) {
                case CMD_SELECT:
                    return SPAN_NODE_FOREIGN_SCAN;
                case CMD_INSERT:
                    return SPAN_NODE_FOREIGN_INSERT;
                case CMD_UPDATE:
                    return SPAN_NODE_FOREIGN_UPDATE;
                case CMD_DELETE:
                    return SPAN_NODE_FOREIGN_DELETE;
                default:
                    return SPAN_NODE_UNKNOWN;
            }
        case T_Material:
            return SPAN_NODE_MATERIALIZE;
        case T_Sort:
            return SPAN_NODE_SORT;
        case T_Group:
            return SPAN_NODE_GROUP;
        case T_Agg:
            {
                Agg *agg = (Agg *) plan;

                switch (agg->aggstrategy) {
                    case AGG_PLAIN:
                        return SPAN_NODE_AGGREGATE;
                    case AGG_SORTED:
                        return SPAN_NODE_GROUP_AGGREGATE;
                    case AGG_HASHED:
                        return SPAN_NODE_HASH_AGGREGATE;
                    default:
                        return SPAN_NODE_UNKNOWN;
                }
            }
        case T_WindowAgg:
            return SPAN_NODE_WINDOW_AGG;
        case T_Unique:
            return SPAN_NODE_UNIQUE;
        case T_SetOp:
            switch (((SetOp *) plan)->strategy) {
                case SETOP_SORTED:
                    return SPAN_NODE_SETOP;
                case SETOP_HASHED:
                    return SPAN_NODE_SETOP_HASHED;
                default:
                    return SPAN_NODE_UNKNOWN;
            }
        case T_LockRows:
            return SPAN_NODE_LOCK_ROWS;
        case T_Limit:
            return SPAN_NODE_LIMIT;
        case T_Hash:
            return SPAN_NODE_HASH;
        default:
            return SPAN_NODE_UNKNOWN;
    }
}

/*
 * Reset the u_sess->trace_cxt.traced_planstates array
 */
void cleanup_planstarts(void)
{
    u_sess->trace_cxt.traced_planstates = NULL;
    u_sess->trace_cxt.max_planstart = 0;
    u_sess->trace_cxt.index_planstart = 0;
}

/*
 * Fetch the node start of a planstate
 */
static TracedPlanstate *get_traced_planstate(PlanState *planstate)
{
    for (int i = 0; i < u_sess->trace_cxt.index_planstart; i++) {
        if (planstate == u_sess->trace_cxt.traced_planstates[i].planstate) {
            return &u_sess->trace_cxt.traced_planstates[i];
        }
    }
    return NULL;
}

/*
 * Get traced_planstate from index
 */
TracedPlanstate *get_traced_planstate_from_index(int index)
{
    Assert(index > -1);
    Assert(index < u_sess->trace_cxt.max_planstart);
    return &u_sess->trace_cxt.traced_planstates[index];
}

/*
 * Get index in u_sess->trace_cxt.traced_planstates array of a possible parent traced_planstate
 */
int get_parent_traced_planstate_index(int nested_level)
{
    TracedPlanstate *traced_planstate;

    if (u_sess->trace_cxt.index_planstart >= 2) {
        traced_planstate = &u_sess->trace_cxt.traced_planstates[u_sess->trace_cxt.index_planstart - 2];
        if (traced_planstate->nested_level == nested_level &&
            nodeTag(((PlanState*)traced_planstate->planstate)->plan) == T_ProjectSet) {
            return u_sess->trace_cxt.index_planstart - 2;
        }
    }
    if (u_sess->trace_cxt.index_planstart >= 1) {
        traced_planstate = &u_sess->trace_cxt.traced_planstates[u_sess->trace_cxt.index_planstart - 1];
        if (traced_planstate->nested_level == nested_level &&
            nodeTag(((PlanState*)traced_planstate->planstate)->plan) == T_Result) {
            return u_sess->trace_cxt.index_planstart - 1;
        }
    }
    return -1;
}

/*
 * Get end time for a span node from the provided planstate.
 */
TimestampTz get_span_end_from_planstate(PlanState *planstate, TimestampTz plan_start, TimestampTz root_end)
{
    TimestampTz span_end_time;
    if (!INSTR_TIME_IS_ZERO(planstate->instrument->starttime) && root_end > 0) {
        /* Node was ongoing but aborted due to an error, use root end as the end */
        span_end_time = root_end;
    } else if (planstate->instrument->total == 0) {
        span_end_time = GetCurrentTimestamp();
    } else {
        span_end_time = plan_start + planstate->instrument->total * US_IN_S;
        /*
         * Since we convert from double seconds to microseconds again, we can
         * have a span_end_time greater to the root due to the loss of
         * precision for long durations. Fallback to the root end in this
         * case.
         */
        if (span_end_time > root_end) {
            span_end_time = root_end;
        }
    }
    return span_end_time;
}

/*
 * Drop all u_sess->trace_cxt.traced_planstates after the provided nested level
 */
static void drop_traced_planstates(void)
{
    int i;
    int new_index_start = 0;

    for (i = u_sess->trace_cxt.index_planstart; i > 0; i--) {
        if (u_sess->trace_cxt.traced_planstates[i - 1].nested_level <= u_sess->trace_cxt.nested_level) {
            /*
             * Found a new planstate from a previous nested level, we can stop
             * here
             */
            u_sess->trace_cxt.index_planstart = i;
            return;
        } else {
            /* the traced_planstate should be dropped */
            new_index_start = i - 1;
        }
    }
    u_sess->trace_cxt.index_planstart = new_index_start;
}

/*
 * Create span node for the provided planstate
 */
static Span create_span_node(PlanState *planstate, const PlanstateTraceContext *planstateTraceContext,
    uint64 *span_id, uint64 parent_id, uint64 query_id, SpanType node_type,
    TimestampTz span_start, int nested_level, TimestampTz span_end)
{
    Span span;
    SpanType span_type = node_type;
    Plan const *plan = planstate->plan;

    /*
     * Make sure stats accumulation is done. Function is a no-op if if was
     * already done.
     */
    InstrEndLoop(planstate->instrument);

    /* We only create span node on node that were executed */
    Assert(planstate->instrument->total > 0);

    if (node_type == SPAN_NODE) {
        span_type = plan_to_span_type(plan);
    }
    begin_span(planstateTraceContext->trace_id, &span, span_type, span_id, parent_id,
               query_id, span_start);

    if (!planstate->state->es_finished) {
        span.sql_error_code = planstateTraceContext->sql_error_code;
    }
    span.nested_level = nested_level;
    end_span(&span, &span_end);

    return span;
}

/*
* Check if the subplan will run or not
*
* With parallel queries, if leader participation is disabled, the leader won't execute the subplans
*/
static bool is_subplan_executed(PlanState *planstate)
{
    return planstate != NULL;
}

/*
* Walk through the planstate tree generating a node span for each node.
*/
static TimestampTz create_spans_from_planstate(PlanState *planstate, PlanstateTraceContext * planstateTraceContext,
    uint64 parent_id, uint64 query_id, TimestampTz parent_start, TimestampTz root_end, TimestampTz *latest_end)
{
    ListCell *l;
    uint64 span_id;
    Span span_node;
    TracedPlanstate *traced_planstate = NULL;
    TimestampTz span_start;
    TimestampTz span_end;
    TimestampTz child_end = 0;
    bool subplan_executed = true;

    /* The node was never executed, skip it */
    if (planstate->instrument == NULL) {
        return parent_start;
    }

    if (!planstate->state->es_finished && !INSTR_TIME_IS_ZERO(planstate->instrument->starttime)) {
        /*
        * If the query is in an unfinished state, it means that we're in an
        * error handler. Stop the node instrumentation to get the latest
        * known state.
        */
        InstrStopNode(planstate->instrument, planstate->state->es_processed);
    }

    /*
    * Make sure stats accumulation is done. Function is a no-op if if was
    * already done.
    */
    InstrEndLoop(planstate->instrument);

    if (planstate->instrument->total == 0) {
        /* The node was never executed, ignore it */
        return parent_start;
    }

    switch (nodeTag(planstate->plan)) {
        case T_BitmapIndexScan:
        case T_BitmapAnd:
        case T_BitmapOr:
            /*
            * Those nodes won't go through ExecProcNode so we won't have
            * their start. Fallback to the parent start.
            */
            span_start = parent_start;
            span_id = pg_prng_uint64(&pg_global_prng_state);
            break;
        case T_Hash:
            /* For hash node, use the child's start */
            traced_planstate = get_traced_planstate(outerPlanState(planstate));
            Assert(traced_planstate != NULL);
            span_start = traced_planstate->node_start;

            /*
            * We still need to generate a dedicated span_id since
            * traced_planstate's span_id will be used by the child
            */
            span_id = pg_prng_uint64(&pg_global_prng_state);
            break;
        default:
            /*
            * We should have a traced_planstate, use it for span_start and
            * span_id
            */
            traced_planstate = get_traced_planstate(planstate);
            if (traced_planstate == NULL) {
                span_start = parent_start;
                span_id = pg_prng_uint64(&pg_global_prng_state);
            } else {
                span_start = traced_planstate->node_start;
                span_id = traced_planstate->span_id;
            }
            break;
    }
    Assert(span_start > 0);
    span_end = get_span_end_from_planstate(planstate, span_start, root_end);
    /* Keep track of the last child end */
    if (*latest_end < span_end) {
        *latest_end = span_end;
    }

    /* We only need to walk the subplans if they are executed */
    subplan_executed = is_subplan_executed(planstate);
    /* Walk the outerplan */
    if (outerPlanState(planstate) && subplan_executed) {
        create_spans_from_planstate(outerPlanState(planstate), planstateTraceContext,
            span_id, query_id, span_start, root_end, latest_end);
    }
    /* Walk the innerplan */
    if (innerPlanState(planstate) && subplan_executed) {
        create_spans_from_planstate(innerPlanState(planstate), planstateTraceContext,
            span_id, query_id, span_start, root_end, latest_end);
    }

    /* Handle init plans */
    foreach(l, planstate->initPlan) {
        SubPlanState *sstate = (SubPlanState *) lfirst(l);
        PlanState  *splan = sstate->planstate;
        Span        initplan_span;
        TracedPlanstate *initplan_traced_planstate;
        TimestampTz initplan_span_end;
        uint64        init_plan_span_id;

        InstrEndLoop(splan->instrument);
        if (splan->instrument->total == 0) {
            continue;
        }

        /*
        * There's no specific init_plan planstate so we need to generate a
        * dedicated span_id. We will still use the child's traced planstate
        * to get the span's end.
        */
        initplan_traced_planstate = get_traced_planstate(splan);
        if (initplan_traced_planstate == NULL) {
            continue;
        }
        init_plan_span_id = pg_prng_uint64(&pg_global_prng_state);
        initplan_span_end = get_span_end_from_planstate(splan, initplan_traced_planstate->node_start, root_end);

        initplan_span = create_span_node(splan, planstateTraceContext,
            &init_plan_span_id, span_id, query_id,
            SPAN_NODE_INIT_PLAN, initplan_traced_planstate->node_start,
            initplan_traced_planstate->nested_level, initplan_span_end);
        store_span(&initplan_span);
        /* Use the initplan span as a parent */
        create_spans_from_planstate(splan, planstateTraceContext, initplan_span.span_id,
            query_id, initplan_traced_planstate->node_start, root_end, latest_end);
    }

    /* Handle sub plans */
    foreach(l, planstate->subPlan) {
        SubPlanState *sstate = (SubPlanState *) lfirst(l);
        PlanState  *splan = sstate->planstate;
        Span        subplan_span;
        TracedPlanstate *subplan_traced_planstate;
        TimestampTz subplan_span_end;
        uint64        subplan_span_id;

        InstrEndLoop(splan->instrument);
        if (splan->instrument->total == 0) {
            continue;
        }

        /*
        * Same as initplan, we create a dedicated span node for subplan but
        * still use the tracedplan to get the end.
        */
        subplan_traced_planstate = get_traced_planstate(splan);
        if (subplan_traced_planstate == NULL) {
            continue;
        }
        subplan_span_id = pg_prng_uint64(&pg_global_prng_state);
        subplan_span_end = get_span_end_from_planstate(((PlanState*)subplan_traced_planstate->planstate),
            subplan_traced_planstate->node_start, root_end);

        subplan_span = create_span_node(splan, planstateTraceContext,
            &subplan_span_id, span_id, query_id,
            SPAN_NODE_SUBPLAN, subplan_traced_planstate->node_start,
            subplan_traced_planstate->nested_level, subplan_span_end);
        store_span(&subplan_span);
        child_end = create_spans_from_planstate(splan, planstateTraceContext, subplan_span.span_id, query_id,
            subplan_traced_planstate->node_start, root_end, latest_end);
    }

    /* Handle special nodes with children nodes */
    switch (nodeTag(planstate->plan)) {
        case T_Append:
            child_end = create_spans_from_plan_list(((AppendState *) planstate)->appendplans,
                ((AppendState *) planstate)->as_nplans, planstateTraceContext,
                span_id, query_id, span_start, root_end, latest_end);
            break;
        case T_MergeAppend:
            child_end = create_spans_from_plan_list(((MergeAppendState *) planstate)->mergeplans,
                ((MergeAppendState *) planstate)->ms_nplans, planstateTraceContext,
                span_id, query_id, span_start, root_end, latest_end);
            break;
        case T_BitmapAnd:
            child_end = create_spans_from_bitmap_nodes(((BitmapAndState *) planstate)->bitmapplans,
                ((BitmapAndState *) planstate)->nplans, planstateTraceContext,
                span_id, query_id, span_start, root_end, latest_end);
            break;
        case T_BitmapOr:
            child_end = create_spans_from_bitmap_nodes(((BitmapOrState *) planstate)->bitmapplans,
                ((BitmapOrState *) planstate)->nplans, planstateTraceContext,
                span_id, query_id, span_start, root_end, latest_end);
            break;
        case T_SubqueryScan:
            child_end = create_spans_from_planstate(((SubqueryScanState *) planstate)->subplan, planstateTraceContext,
                span_id, query_id, span_start, root_end, latest_end);
            break;
        default:
            break;
    }

    /* If node had no duration, use the latest end of its child */
    if (planstate->instrument->total == 0) {
        span_end = *latest_end;
    }
    /* For special node with children, use the last child's end */
    if (child_end > 0) {
        span_end = child_end;
    }

    int nested_level = (traced_planstate == NULL)? u_sess->trace_cxt.nested_level : traced_planstate->nested_level;
    span_node = create_span_node(planstate, planstateTraceContext, &span_id, parent_id,
        query_id, SPAN_NODE, span_start, nested_level, span_end);
    store_span(&span_node);
    return span_end;
}

/*
 * Iterate over a list of planstate to generate span node
 */
TimestampTz create_spans_from_plan_list(PlanState **planstates, int nplans,
    PlanstateTraceContext * planstateTraceContext, uint64 parent_id, uint64 query_id,
    TimestampTz parent_start, TimestampTz root_end, TimestampTz *latest_end)
{
    int j;
    TimestampTz last_end = 0;

    for (j = 0; j < nplans; j++) {
        last_end = create_spans_from_planstate(planstates[j], planstateTraceContext,
            parent_id, query_id, parent_start, root_end, latest_end);
    }
    return last_end;
}

/*
 * Iterate over children of BitmapOr and BitmapAnd
 */
TimestampTz create_spans_from_bitmap_nodes(PlanState **planstates,
    int nplans, PlanstateTraceContext * planstateTraceContext, uint64 parent_id,
    uint64 query_id, TimestampTz parent_start, TimestampTz root_end, TimestampTz *latest_end)
{
    int j;

    /* We keep track of the end of the last sibling end to use as start */
    TimestampTz sibling_end = parent_start;

    for (j = 0; j < nplans; j++) {
        sibling_end = create_spans_from_planstate(planstates[j], planstateTraceContext,
            parent_id, query_id, sibling_end, root_end, latest_end);
    }
    return sibling_end;
}

/*
 * Process planstate to generate spans of the executed plan
 */
void process_planstate(const Traceparent * traceparent, const QueryDesc *queryDesc,
    int sql_error_code, uint64 parent_id, uint64 query_id,
    TimestampTz parent_start, TimestampTz parent_end)
{
    PlanstateTraceContext planstateTraceContext;
    TimestampTz latest_end = 0;

    if (queryDesc->planstate == NULL || queryDesc->planstate->instrument == NULL) {
        return;
    }

    errno_t rc = memcpy_s(planstateTraceContext.trace_id,
        MAX_TRACE_ID_SIZE, traceparent->trace_id, MAX_TRACE_ID_SIZE);
    securec_check(rc, "", "");

    planstateTraceContext.sql_error_code = sql_error_code;
    /* Prepare the planstate context for deparsing */

    create_spans_from_planstate(queryDesc->planstate, &planstateTraceContext,
                                parent_id, query_id, parent_start,
                                parent_end, &latest_end);

    /* We can get rid of all the traced planstate for this level */
    drop_traced_planstates();
}

/*
* If spans from planstate is requested, we override planstate's ExecProcNode's pointer with this function.
* It will track the time of the first node call needed to place the planstate span.
*/
static TupleTableSlot *ExecProcNodeFirstTrace(PlanState *planstate)
{
    uint64 span_id;

    if (u_sess->trace_cxt.max_planstart == 0) {
        /*
        * Queries calling pg_tracing_spans will have aborted tracing and
        * everything cleaned.
        */
        goto exit;
    }

    if (u_sess->trace_cxt.index_planstart >= u_sess->trace_cxt.max_planstart) {
        /* We need to extend the u_sess->trace_cxt.traced_planstates array */
        int old_max_planstart = u_sess->trace_cxt.max_planstart;

        Assert(u_sess->trace_cxt.traced_planstates != NULL);
        u_sess->trace_cxt.max_planstart += INCREMENT_NUM_PLANSTATE;
        TracedPlanstate* traced_planstates = u_sess->trace_cxt.traced_planstates;
        knl_u_trace_context trace_cxt = u_sess->trace_cxt;
        traced_planstates = (TracedPlanstate*)repalloc0((void*)traced_planstates,
            old_max_planstart * sizeof(TracedPlanstate),
            u_sess->trace_cxt.max_planstart * sizeof(TracedPlanstate));
        u_sess->trace_cxt.traced_planstates = traced_planstates;
    }

    /* Normal node, generate a random span_id */
    span_id = pg_prng_uint64(&pg_global_prng_state);

    /* Register planstate start */
    u_sess->trace_cxt.traced_planstates[u_sess->trace_cxt.index_planstart].planstate = planstate;
    u_sess->trace_cxt.traced_planstates[u_sess->trace_cxt.index_planstart].node_start = GetCurrentTimestamp();
    u_sess->trace_cxt.traced_planstates[u_sess->trace_cxt.index_planstart].span_id = span_id;
    u_sess->trace_cxt.traced_planstates[u_sess->trace_cxt.index_planstart].nested_level =
        u_sess->trace_cxt.nested_level;
    u_sess->trace_cxt.index_planstart++;

    exit:
        /* Restore previous exec proc */
        planstate->ExecProcNode = previous_ExecProcNode;
        return planstate->ExecProcNode(planstate);
}

/*
* Walk the planstate and override all executor pointer
*/
static bool override_ExecProcNode(struct PlanState *planstate, void *context)
{
    if (planstate->instrument == NULL) {
        /* No instrumentation set, do nothing */
        return false;
    }

    planstate->ExecProcNode = ExecProcNodeFirstTrace;
    return planstate_tree_walker(planstate, override_ExecProcNode, context);
}

/*
* Override all ExecProcNode pointer of the planstate with ExecProcNodeFirstPgTracing to track first call of a node.
*/
void setup_ExecProcNode_override(MemoryContext context, QueryDesc *queryDesc)
{
    knl_u_trace_context trace_cxt = u_sess->trace_cxt;
    TracedPlanstate* planstate = u_sess->trace_cxt.traced_planstates;
    if (u_sess->trace_cxt.max_planstart == 0 || u_sess->trace_cxt.traced_planstates == NULL) {
        MemoryContext oldcxt = MemoryContextSwitchTo(trace_cxt.trace_mem_ctx);
        planstate = (TracedPlanstate*)palloc0(INITIAL_NUM_PLANSTATE * sizeof(TracedPlanstate));
        u_sess->trace_cxt.traced_planstates = planstate;
        MemoryContextSwitchTo(oldcxt);
        u_sess->trace_cxt.max_planstart = INITIAL_NUM_PLANSTATE;
    }
    Assert(queryDesc->planstate->instrument);
    /* Pointer should target ExecProcNodeFirst. Save it to restore it later. */
    previous_ExecProcNode = queryDesc->planstate->ExecProcNode;
    queryDesc->planstate->ExecProcNode = ExecProcNodeFirstTrace;
    planstate_tree_walker(queryDesc->planstate, override_ExecProcNode, NULL);
}

