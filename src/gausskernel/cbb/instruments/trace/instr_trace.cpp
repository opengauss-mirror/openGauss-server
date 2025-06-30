/*-------------------------------------------------------------------------
 * MIT License
 * Copyright (c) 2024, Datadog, Inc.
 *
 * Based on github's /DataDog/pg_tracing reconstruction
 * instr_trace.cpp
 *        Generate spans for trace from SQL query
 *
 * IDENTIFICATION
 *      src/gausskernel/cbb/instruments/trace/instr_trace.cpp
 *-------------------------------------------------------------------------
 */

#include "instr_trace_inner.h"
#include "parser/analyze.h"
#include "storage/proc.h"
#include "access/hash.h"
#include "access/xact.h"
#include "utils/builtins.h"
#include "executor/executor.h"
#include "optimizer/planner.h"
#include "instruments/instr_handle_mgr.h"


/* Number of spans initially allocated at the start of a trace. */
#define INITIAL_NUM_CURSPANS (50)
#define INITIAL_NUM_NESTED_LEVEL (10)
#define TRACE_HEAD_SIZE (1)     /* [VERSION] */
#define TRACE_SPAN_SIZE (sizeof(Span))
static int allocated_nested_level = INITIAL_NUM_NESTED_LEVEL;

bool instr_level_trace_enable()
{
    if (t_thrd.shemem_ptr_cxt.MyBEEntry == NULL ||
        CURRENT_STMT_METRIC_HANDLE == NULL ||
        BEENTRY_STMEMENET_CXT.stmt_stat_cxt == NULL) {
        return FALSE;
    }

    if (instr_stmt_level_fullsql_open()) {
        int fullsql_level = u_sess->statement_cxt.statement_level[0];
        /* record query plan when level >= L0 */
        if (fullsql_level >= STMT_TRACK_L1 && fullsql_level <= STMT_TRACK_L2) {
            return TRUE;
        }
    }

    if ((instr_stmt_level_slowsql_only_open())) {
        int slowsql_level = u_sess->statement_cxt.statement_level[1];
        /* record query plan when level >= L0 */
        if (slowsql_level >= STMT_TRACK_L1 && slowsql_level <= STMT_TRACK_L2) {
            return TRUE;
        }
    }

    return FALSE;
}

/*
* Returns true if TraceId is zero
*/
bool traceid_null(char* trace_id)
{
    return trace_id[0] == '\0';
}

/*
* Returns true if trace ids are equals
*/
bool traceid_equal(const char* trace_id_1, const char* trace_id_2)
{
    return (memcmp(trace_id_1, trace_id_2, MAX_TRACE_ID_SIZE) == 0);
}

/*
 * Given an arbitrarily long query string, produce a hash for the purposes of
 * identifying the query, without normalizing constants.  Used when hashing
 * utility statements.
 */
static uint32 hash_string(const char* str)
{
    return hash_any((const unsigned char*)str, strlen(str));
}

/*
 * Convert a node CmdType to the matching SpanType
 */
static SpanType cmd_type_to_span_type(CmdType cmd_type)
{
    switch (cmd_type) {
        case CMD_SELECT:
            return SPAN_TOP_SELECT;
        case CMD_INSERT:
            return SPAN_TOP_INSERT;
        case CMD_UPDATE:
            return SPAN_TOP_UPDATE;
        case CMD_DELETE:
            return SPAN_TOP_DELETE;
        case CMD_MERGE:
            return SPAN_TOP_MERGE;
        case CMD_UTILITY:
            return SPAN_TOP_UTILITY;
        case CMD_NOTHING:
            return SPAN_TOP_NOTHING;
        case CMD_UNKNOWN:
            return SPAN_TOP_UNKNOWN;
        default:
            return SPAN_TOP_UNKNOWN;
    }
    return SPAN_TOP_UNKNOWN;
}

static void update_latest_lxid()
{
    u_sess->trace_cxt.latest_lxid = t_thrd.proc->lxid;
}

static bool is_new_lxid()
{
    return t_thrd.proc->lxid != u_sess->trace_cxt.latest_lxid;
}

/*
 * Preallocate space in the shared current_trace_spans to have at least slots_needed slots
 */
static void preallocate_spans(int slots_needed)
{
    TraceSpans *current_trace_spans = u_sess->trace_cxt.current_trace_spans;
    int target_size = current_trace_spans->end + slots_needed;

    Assert(slots_needed > 0);
    if (target_size > current_trace_spans->max) {
            int old_spans_max = current_trace_spans->max;
            current_trace_spans->max = target_size;
            current_trace_spans = (TraceSpans*)repalloc(current_trace_spans,
                            sizeof(TraceSpans) + target_size * sizeof(Span));
        ereport(LOG, (errmodule(MOD_INSTR), errmsg("zff: repalloc curspan, num:%d, cur:%d, max:%d.\n",
            slots_needed, current_trace_spans->end, current_trace_spans->max)));
    }
}

static bool instr_extend_trace_node(StatementStatContext *ssctx, bool first)
{
    MemoryContext oldcontext = MemoryContextSwitchTo(BEENTRY_STMEMENET_CXT.stmt_stat_cxt);
    TraceNode *node = (TraceNode *)palloc0_noexcept(sizeof(TraceNode));
    (void)MemoryContextSwitchTo(oldcontext);

    if (node == NULL) {
        ereport(LOG, (errmodule(MOD_INSTR), errmsg("[Statement] trace is lost due to OOM.")));
        return false;
    }

    if (first) {
        ssctx->trace_info.head = node;
        ssctx->trace_info.tail = node;
        // update list info
        ssctx->trace_info.n_nodes = 1;
        ssctx->trace_info.cur_pos = 0;
    } else {
        ssctx->trace_info.tail->next = node;
        ssctx->trace_info.tail = node;
        // update list info
        ssctx->trace_info.n_nodes++;
        ssctx->trace_info.cur_pos = 0;
    }

    return true;
}

static bool check_trace_info_record(StatementStatContext *ssctx, const char *trace)
{
    if (u_sess->attr.attr_common.track_stmt_trace_size == 0 || ssctx->trace_info.oom) {
        ereport(LOG, (errmodule(MOD_INSTR), errmsg("[Statement] trace is lost due to OOM.")));
        return false;
    }

    if (ssctx->trace_info.n_nodes > 0 && u_sess->attr.attr_common.track_stmt_trace_size <
        (ssctx->trace_info.n_nodes - 1) * STATEMENT_TRACE_BUFSIZE + ssctx->trace_info.cur_pos) {
        ereport(LOG, (errmodule(MOD_INSTR), errmsg("[Statement] trace is lost due to OOM.")));
        return false;
    }

    if (trace == NULL) {
        ssctx->trace_info.oom = true;
        return false;
    }

    return true;
}

void instr_trace_store_str(char* str, int len)
{
    errno_t rc;
    if (!check_trace_info_record(CURRENT_STMT_METRIC_HANDLE, str)) {
        return;
    }

    Assert(len < STATEMENT_TRACE_BUFSIZE);
    /* caculate bytea total len */
    uint32 cur_pos = CURRENT_STMT_METRIC_HANDLE->trace_info.cur_pos;

    if (CURRENT_STMT_METRIC_HANDLE->trace_info.n_nodes <= 0) {
        if (instr_extend_trace_node(CURRENT_STMT_METRIC_HANDLE, TRUE)) {
            rc = memcpy_s(CURRENT_STMT_METRIC_HANDLE->trace_info.tail->buf + cur_pos,
                STATEMENT_TRACE_BUFSIZE - cur_pos, str, len);
            securec_check(rc, "", "");
            CURRENT_STMT_METRIC_HANDLE->trace_info.cur_pos += len;
        }
    } else if (CURRENT_STMT_METRIC_HANDLE->trace_info.cur_pos + len <= STATEMENT_TRACE_BUFSIZE) {
        rc = memcpy_s(CURRENT_STMT_METRIC_HANDLE->trace_info.tail->buf + cur_pos,
            STATEMENT_TRACE_BUFSIZE - cur_pos, str, len);
        securec_check(rc, "", "");
        CURRENT_STMT_METRIC_HANDLE->trace_info.cur_pos += len;
    } else {
        size_t last = STATEMENT_TRACE_BUFSIZE - CURRENT_STMT_METRIC_HANDLE->trace_info.cur_pos;
        if (last > 0) {
            rc = memcpy_s(CURRENT_STMT_METRIC_HANDLE->trace_info.tail->buf +
                CURRENT_STMT_METRIC_HANDLE->trace_info.cur_pos, last, str, last);
            securec_check(rc, "", "");
        }

        if (instr_extend_trace_node(CURRENT_STMT_METRIC_HANDLE, false)) {
            rc = memcpy_s(CURRENT_STMT_METRIC_HANDLE->trace_info.tail->buf,
                STATEMENT_TRACE_BUFSIZE, str + last, len - last);
            securec_check(rc, "", "");
            CURRENT_STMT_METRIC_HANDLE->trace_info.cur_pos += len - last;
        }
    }
}

/*
 * Reset traceparent fields
 */
void reset_traceparent(Traceparent * traceparent)
{
    errno_t rc = memset_s(traceparent, sizeof(Traceparent), 0, sizeof(Traceparent));
    securec_check(rc, "\0", "\0");
}

/*
 * Reset pg_tracing memory context and global state.
 */
void cleanup_tracing(void)
{
    if (u_sess->trace_cxt.current_trace_spans == NULL) {
        /* No need for cleaning */
        return;
    }
    MemoryContextReset(u_sess->trace_cxt.trace_mem_ctx);

    /*
     * Don't reset parse_traceparent here. With extended protocol +
     * transaction block, tracing of the previous statement may end while
     * parsing of the next statement was already done and stored in
     * parse_traceparent.
     */
    reset_traceparent(u_sess->trace_cxt.executor_traceparent);
    u_sess->trace_cxt.current_trace_spans = NULL;
    u_sess->trace_cxt.per_level_infos = NULL;
    u_sess->trace_cxt.max_planstart = 0;
    u_sess->trace_cxt.index_planstart = 0;
    u_sess->trace_cxt.traced_planstates = NULL;
    allocated_nested_level = INITIAL_NUM_NESTED_LEVEL;
    cleanup_active_spans();
}

/*
 * End the query tracing and dump all spans in the shared buffer if we are at
 * the root level. This may happen either when query is finished or on a caught error.
 */
void end_tracing(void)
{
    if (!instr_level_trace_enable()) {
        return;
    }

    TraceSpans * current_trace_spans = u_sess->trace_cxt.current_trace_spans;

    /* We're still a nested query, tracing is not finished */
    if (u_sess->trace_cxt.nested_level > 0) {
        return;
    }

    if (u_sess->trace_cxt.current_trace_spans == NULL ||
       u_sess->trace_cxt.current_trace_spans->end == 0) {
        return;
    }

    // total_len + version + spancount + span1 + span2 + ... + span...
    if (u_sess->attr.attr_common.track_stmt_trace_size == 0) {
        ereport(LOG, (errmodule(MOD_INSTR), errmsg("[Statement] trace is lost due to OOM.")));
        return;
    }
    int64 max_span_bytes = u_sess->attr.attr_common.track_stmt_trace_size - sizeof(TraceHeader);
    int max_span_cnt = max_span_bytes / TRACE_SPAN_SIZE;
    int total_span_cnt = (u_sess->trace_cxt.current_trace_spans->end <= max_span_cnt)?
	    u_sess->trace_cxt.current_trace_spans->end : max_span_cnt;
    int32 total_len = sizeof(TraceHeader) + total_span_cnt * TRACE_SPAN_SIZE;
    TraceHeader header = {total_len, STATEMENT_TRACE_VERSION_V1, total_span_cnt};

    instr_trace_store_str((char*)&header, sizeof(TraceHeader));

    /* We're at the end, add all stored spans to the shared memory */
    for (int i = 0; i < current_trace_spans->end; i++) {
        Span       *span = &current_trace_spans->spans[i];
        instr_trace_store_str((char*)span, TRACE_SPAN_SIZE);
    }

    /* We can cleanup the rest here */
    cleanup_tracing();
}

/*
 * Process a query descriptor: Gather all query instrumentation in the query
 * span counters
 */
static void process_query_desc(const Traceparent * traceparent, const QueryDesc *queryDesc,
    int sql_error_code, TimestampTz parent_end)
{
    if (!queryDesc->totaltime->running && queryDesc->totaltime->total == 0) {
        return;
    }

    /* Process total counters */
    if (queryDesc->totaltime) {
        InstrEndLoop(queryDesc->totaltime);
    }

    int nested_level = u_sess->trace_cxt.nested_level;
    PerLevelInfos * per_level_infos = u_sess->trace_cxt.per_level_infos;
    uint64        parent_id = per_level_infos[nested_level].executor_run_span_id;
    TimestampTz parent_start = per_level_infos[nested_level].executor_run_start;
    uint64        query_id = queryDesc->plannedstmt->queryId;

    process_planstate(traceparent, queryDesc, sql_error_code, parent_id, query_id,
                      parent_start, parent_end);
}

/*
 * When we catch an error (timeout, cancel query), we need to flag the ongoing
 * span with an error, send current spans in the shared buffer and clean our
 * memory context.
 *
 * We provide the ongoing span where the error was caught to attach the sql
 * error code to it.
 */
static void handle_trace_error(const Traceparent * traceparent,
    const QueryDesc *queryDesc, TimestampTz span_end_time)
{
    int sql_error_code;
    Span *span;
    TraceSpans *current_trace_spans = u_sess->trace_cxt.current_trace_spans;
    int max_trace_spans = current_trace_spans->max;

    sql_error_code = geterrcode();

    if (queryDesc != NULL) {
        /* On error, instrumentation may have been left running, stop it to avoid error thrown by InstrEndLoop */
        if (!INSTR_TIME_IS_ZERO(queryDesc->totaltime->starttime)) {
            InstrStopNode(queryDesc->totaltime, 0);
        }

        /* Within error, we need to avoid any possible allocation as this
         * could be an out of memory error. deparse plan relies on allocation
         * through lcons so we explicitely disable it. */
        process_query_desc(traceparent, queryDesc, sql_error_code, span_end_time);
    }
    span = peek_active_span();
    while (span != NULL && span->nested_level ==  u_sess->trace_cxt.nested_level) {
        /* Assign the error code to the latest top span */
        span->sql_error_code = sql_error_code;
        /* End and store the span */
        pop_and_store_active_span(span_end_time);
        /* Get the next span in the stack */
        span = peek_active_span();
    }
}

/*
 * If we enter a new nested level, initialize all necessary buffers.
 */
static void initialize_trace_level(knl_u_trace_context* trace_cxt)
{
    /* Number of allocated nested levels. */
    Assert(trace_cxt->nested_level >= 0);

    /* First time */
    if (trace_cxt->trace_mem_ctx->isReset) {
        MemoryContext oldcxt;
        Assert(trace_cxt->trace_mem_ctx->isReset);

        oldcxt = MemoryContextSwitchTo(trace_cxt->trace_mem_ctx);
        /* initial allocation */
        trace_cxt->parse_traceparent = (Traceparent*)palloc0(sizeof(Traceparent));
        trace_cxt->executor_traceparent = (Traceparent*)palloc0(sizeof(Traceparent));
        trace_cxt->tx_traceparent = (Traceparent*)palloc0(sizeof(Traceparent));
        trace_cxt->next_active_span = (Span*)palloc0(sizeof(Span));
        trace_cxt->commit_span = (Span*)palloc0(sizeof(Span));
        trace_cxt->tx_block_span = (Span*)palloc0(sizeof(Span));

        trace_cxt->current_trace_spans =
            (TraceSpans*)palloc0(sizeof(TraceSpans) + INITIAL_NUM_CURSPANS * TRACE_SPAN_SIZE);
        trace_cxt->current_trace_spans->max = INITIAL_NUM_CURSPANS;

        trace_cxt->per_level_infos = (PerLevelInfos*)palloc0(allocated_nested_level * sizeof(PerLevelInfos));
        MemoryContextSwitchTo(oldcxt);
    } else if (trace_cxt->nested_level >= allocated_nested_level) {
        /* New nested level, allocate more memory */
        int old_allocated_nested_level = allocated_nested_level;
        allocated_nested_level++;

        /* repalloc uses the pointer's memory context, no need to switch */
        u_sess->trace_cxt.per_level_infos = (PerLevelInfos*)repalloc0(u_sess->trace_cxt.per_level_infos,
            old_allocated_nested_level * sizeof(PerLevelInfos),
            allocated_nested_level * sizeof(PerLevelInfos));
    }
}

/*
 * End all spans for the current nested level
 */
static void end_nested_level(TimestampTz span_end_time)
{
    Span* span = peek_active_span();
    if (span == NULL || span->nested_level <  u_sess->trace_cxt.nested_level) {
        return;
    }

    while (span != NULL && span->nested_level ==  u_sess->trace_cxt.nested_level) {
        if (span->parent_planstate_index > -1) {
            /* We have a parent planstate, use it for the span end */
            TracedPlanstate *traced_planstate = get_traced_planstate_from_index(span->parent_planstate_index);

            /* Make sure to end instrumentation */
            InstrEndLoop(((PlanState*)(traced_planstate->planstate))->instrument);
            span_end_time = get_span_end_from_planstate((PlanState*)(traced_planstate->planstate),
                traced_planstate->node_start, span_end_time);
        }
        pop_and_store_active_span(span_end_time);
        span = peek_active_span();
    }
}

/*
 * Adjust the tx_traceparent if the latest captured traceparent is more
 * relevant, i.e. provided from upstream and not automatically generated.
 */
static void adjust_tx_traceparent(const Traceparent * traceparent)
{
    errno_t rc;
    TraceSpans * current_trace_spans = u_sess->trace_cxt.current_trace_spans;
    Span* tx_block_span = u_sess->trace_cxt.tx_block_span;
    if (traceid_equal(traceparent->trace_id, tx_block_span->trace_id)) {
        return;
    }

    /* We have a generated tx_traceparent and a provided traceparent, give
     * priority to the provided traceparent and amend the existing spans */
    rc = memcpy_s(tx_block_span->trace_id, MAX_TRACE_ID_SIZE, traceparent->trace_id, MAX_TRACE_ID_SIZE);
    securec_check(rc, "\0", "\0");
    rc = memcpy_s(u_sess->trace_cxt.tx_traceparent->trace_id, MAX_TRACE_ID_SIZE,
        traceparent->trace_id, MAX_TRACE_ID_SIZE);
    securec_check(rc, "\0", "\0");
    for (int i = 0; i < current_trace_spans->end; i++) {
        rc = memcpy_s(current_trace_spans->spans[i].trace_id, MAX_TRACE_ID_SIZE,
            traceparent->trace_id, MAX_TRACE_ID_SIZE);
        securec_check(rc, "\0", "\0");
    }
}

static void initialize_span_context(SpanContext * span_context, Traceparent * traceparent,
    const PlannedStmt *pstmt, Query *query, const char *query_text)
{
    Span* tx_block_span = u_sess->trace_cxt.tx_block_span;
    span_context->start_time = GetCurrentTimestamp();
    span_context->traceparent = traceparent;
    errno_t rc =
        memcpy_s(traceparent->trace_id, MAX_TRACE_ID_SIZE, u_sess->trace_cxt.trace_id, MAX_TRACE_ID_SIZE);
    securec_check(rc, "\0", "\0");

    if (!traceid_null(tx_block_span->trace_id) && u_sess->trace_cxt.nested_level == 0) {
        adjust_tx_traceparent(span_context->traceparent);
        /* We have an ongoing transaction block. Use it as parent for level 0 */
        span_context->traceparent->parent_id = tx_block_span->span_id;
    }

    span_context->pstmt = pstmt;
    span_context->query = query;
    span_context->query_text = query_text;

    if (span_context->pstmt) {
        span_context->query_id = pstmt->queryId;
    } else {
        if (query == NULL) {
            span_context->query_id = u_sess->trace_cxt.current_query_id;
        } else {
            span_context->query_id = query->queryId;
        }
    }
}

static bool should_start_tx_block(const Node *utilityStmt, HookType hook_type)
{
    TransactionStmt *stmt;
    if (hook_type == HOOK_PARSE && GetCurrentTransactionStartTimestamp() != GetCurrentStatementStartTimestamp()) {
        /* There's an ongoing tx block, we can create the matching span */
        return true;
    }
    if (hook_type == HOOK_UTILITY && utilityStmt != NULL && nodeTag(utilityStmt) == T_TransactionStmt) {
        stmt = (TransactionStmt *) utilityStmt;
        /* If we have an explicit BEGIN statement, start a tx block */
        return (stmt->kind == TRANS_STMT_BEGIN);
    }
    return false;
}

/*
 * Post-parse-analyze hook
 *
 * Tracing can be started here if:
 * - The query has a SQLCommenter with traceparent parameter with sampled flag
 *   on and passes the caller_sample_rate
 * - The query passes the sample_rate
 *
 * If query is sampled, start the top span
 */
void instr_trace_post_parse_analyze(ParseState *pstate, Query *query)
{
    if (!instr_level_trace_enable()) {
        if (t_thrd.statement_cxt.instr_trace_prev_post_parse_analyze_hook) {
            (*(post_parse_analyze_hook_type)t_thrd.statement_cxt.instr_trace_prev_post_parse_analyze_hook)\
                (pstate, query);
        }
        return;
    }

    initialize_trace_level(&(u_sess->trace_cxt));

    int nested_level = u_sess->trace_cxt.nested_level;
    bool        new_lxid = is_new_lxid();
    bool        is_root_level = nested_level == 0;
    Traceparent *traceparent = u_sess->trace_cxt.parse_traceparent;
    SpanContext span_context;

    if (new_lxid) {
        /* We have a new local transaction, reset the tx_start traceparent */
        reset_traceparent(u_sess->trace_cxt.tx_traceparent);
    }

    if (!is_root_level) {
        /* We're in a nested query, grab the ongoing executor traceparent */
        traceparent = u_sess->trace_cxt.executor_traceparent;
    } else {
        /* We're at root level, reset the parse traceparent */
        reset_traceparent(u_sess->trace_cxt.parse_traceparent);
    }

    initialize_span_context(&span_context, traceparent, NULL, query, pstate->p_sourcetext);

    push_active_span(u_sess->trace_cxt.trace_mem_ctx, &span_context,
        cmd_type_to_span_type(query->commandType), HOOK_PARSE);
    if (t_thrd.statement_cxt.instr_trace_prev_post_parse_analyze_hook) {
        (*(post_parse_analyze_hook_type)t_thrd.statement_cxt.instr_trace_prev_post_parse_analyze_hook)(pstate, query);
    }
}

/*
 * Planner hook.
 * Tracing can start here if the query skipped parsing (prepared statement) and
 * passes the random sample_rate. If query is sampled, create a plan span and
 * start the top span if not already started.
 */
PlannedStmt* instr_trace_planner(Query *query, int cursorOptions, ParamListInfo params)
{
    if (!instr_level_trace_enable()) {
        if (t_thrd.statement_cxt.instr_trace_prev_planner_hook) {
            return (*(planner_hook_type)t_thrd.statement_cxt.instr_trace_prev_planner_hook)\
                (query, cursorOptions, params);
        } else {
            return standard_planner(query, cursorOptions, params);
        }
    }

    initialize_trace_level(&(u_sess->trace_cxt));

    SpanContext span_context;
    PlannedStmt *result;
    TimestampTz span_end_time;
    Traceparent *traceparent = u_sess->trace_cxt.parse_traceparent;
    MemoryContext trace_mem_ctx = u_sess->trace_cxt.trace_mem_ctx;
    int nested_level = u_sess->trace_cxt.nested_level;

    if (nested_level > 0) {
        /* We're in a nested query, grab the ongoing traceparent */
        traceparent = u_sess->trace_cxt.executor_traceparent;
    }

    initialize_span_context(&span_context, traceparent, NULL, query, NULL);
    push_active_span(trace_mem_ctx, &span_context, cmd_type_to_span_type(query->commandType), HOOK_PLANNER);
    /* Create and start the planner span */
    push_child_active_span(trace_mem_ctx, &span_context, SPAN_PLANNER);

    u_sess->trace_cxt.nested_level++;
    PG_TRY();
    {
        if (t_thrd.statement_cxt.instr_trace_prev_planner_hook) {
            result = (*(planner_hook_type)t_thrd.statement_cxt.instr_trace_prev_planner_hook)\
                (query, cursorOptions, params);
        } else {
            result = standard_planner(query, cursorOptions, params);
        }
    }
    PG_CATCH();
    {
        u_sess->trace_cxt.nested_level--;
        span_end_time = GetCurrentTimestamp();
        handle_trace_error(traceparent, NULL, span_end_time);
        PG_RE_THROW();
    }
    PG_END_TRY();
    span_end_time = GetCurrentTimestamp();
    end_nested_level(span_end_time);
    u_sess->trace_cxt.nested_level--;

    /* End planner span */
    pop_and_store_active_span(span_end_time);

    return result;
}

/*
 * ExecutorStart hook: Activate query instrumentation if query is sampled.
 * Tracing can be started here if the query used a cached plan and passes the
 * random sample_rate.
 * If query is sampled, start the top span if it doesn't already exist.
 */
void instr_trace_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
    if (!instr_level_trace_enable()) {
        if (t_thrd.statement_cxt.instr_trace_prev_ExecutorStart) {
            (*(ExecutorStart_hook_type)t_thrd.statement_cxt.instr_trace_prev_ExecutorStart)(queryDesc, eflags);
        } else {
            standard_ExecutorStart(queryDesc, eflags);
        }
        return;
    }

    initialize_trace_level(&(u_sess->trace_cxt));

    SpanContext span_context;
    bool        is_lazy_function;
    Traceparent *traceparent = u_sess->trace_cxt.executor_traceparent;
    int nested_level = u_sess->trace_cxt.nested_level;

    if (nested_level == 0) {
        /* We're at the root level, copy parse traceparent */
        traceparent = (u_sess->trace_cxt.parse_traceparent);
        reset_traceparent(u_sess->trace_cxt.parse_traceparent);
    }

    /*
     * We can detect the presence of lazy function through the node tag and
     * the executor flags. Lazy function will go through an ExecutorRun with
     * every call, possibly generating thousands of spans (ex: lazy call of
     * generate_series) which is not manageable and not very useful. If lazy
     * functions are detected, we don't generate spans.
     */
    is_lazy_function = nodeTag(queryDesc->plannedstmt->planTree) == T_FunctionScan && eflags == EXEC_FLAG_SKIP_TRIGGERS;
    if (is_lazy_function) {
        /* No sampling, go through normal ExecutorStart */
        if (t_thrd.statement_cxt.instr_trace_prev_ExecutorStart) {
            (*(ExecutorStart_hook_type)t_thrd.statement_cxt.instr_trace_prev_ExecutorStart)(queryDesc, eflags);
        } else {
            standard_ExecutorStart(queryDesc, eflags);
        }
        return;
    }

    initialize_span_context(&span_context, traceparent, queryDesc->plannedstmt, NULL, queryDesc->sourceText);
    push_active_span(u_sess->trace_cxt.trace_mem_ctx, &span_context, cmd_type_to_span_type(queryDesc->operation),
                     HOOK_EXECUTOR);

    if (t_thrd.statement_cxt.instr_trace_prev_ExecutorStart) {
        (*(ExecutorStart_hook_type)t_thrd.statement_cxt.instr_trace_prev_ExecutorStart)(queryDesc, eflags);
    } else {
        standard_ExecutorStart(queryDesc, eflags);
    }

    /* Allocate totaltime instrumentation in the per-query context */
    if (queryDesc->totaltime == NULL) {
        MemoryContext oldcxt = MemoryContextSwitchTo(queryDesc->estate->es_query_cxt);
        queryDesc->totaltime = InstrAlloc(1, INSTRUMENT_TIMER);
        MemoryContextSwitchTo(oldcxt);
    }
}

/*
 * ExecutorRun hook: track nesting depth and create ExecutorRun span.
 * ExecutorRun can create nested queries so we need to create ExecutorRun span
 * as a top span.
 * If the plan needs to create parallel workers, push the trace context in the parallel shared buffer.
 */
void instr_trace_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction, long count)
{
    if (!instr_level_trace_enable()) {
        if (t_thrd.statement_cxt.instr_trace_prev_ExecutorRun) {
            (*(ExecutorRun_hook_type)t_thrd.statement_cxt.instr_trace_prev_ExecutorRun)(queryDesc, direction, count);
        } else {
            standard_ExecutorRun(queryDesc, direction, count);
        }
        return;
    }

    initialize_trace_level(&(u_sess->trace_cxt));

    SpanContext span_context;
    TimestampTz span_end_time;
    Span       *executor_run_span;
    Traceparent *traceparent = u_sess->trace_cxt.executor_traceparent;
    int            num_nodes;
    TraceSpans * current_trace_spans = u_sess->trace_cxt.current_trace_spans;
    PerLevelInfos * per_level_infos = u_sess->trace_cxt.per_level_infos;
    MemoryContext trace_mem_ctx = u_sess->trace_cxt.trace_mem_ctx;
    int nested_level = u_sess->trace_cxt.nested_level;

    /* ExecutorRun is sampled */
    initialize_span_context(&span_context, traceparent, queryDesc->plannedstmt, NULL, queryDesc->sourceText);
    push_active_span(trace_mem_ctx, &span_context, cmd_type_to_span_type(queryDesc->operation),
                     HOOK_EXECUTOR);
    /* Start ExecutorRun span as a new active span */
    executor_run_span = push_child_active_span(trace_mem_ctx, &span_context, SPAN_EXECUTOR_RUN);

    per_level_infos[nested_level].executor_run_span_id = executor_run_span->span_id;
    per_level_infos[nested_level].executor_run_start = executor_run_span->start;

    /*
     * Setup ExecProcNode override to capture node start if planstate spans
     * were requested. If there's no query instrumentation, we can skip
     * ExecProcNode override.
     */
    if (queryDesc->planstate->instrument) {
        setup_ExecProcNode_override(u_sess->trace_cxt.trace_mem_ctx, queryDesc);
    }

    /*
     * Preallocate enough space in the current_trace_spans to process the
     * queryDesc in the error handler without doing new allocations
     */
    u_sess->trace_cxt.nested_level++;
    PG_TRY();
    {
        if (t_thrd.statement_cxt.instr_trace_prev_ExecutorRun) {
            (*(ExecutorRun_hook_type)t_thrd.statement_cxt.instr_trace_prev_ExecutorRun)(queryDesc, direction, count);
        } else {
            standard_ExecutorRun(queryDesc, direction, count);
        }
    }
    PG_CATCH();
    {
        if (current_trace_spans != NULL) {
            span_end_time = GetCurrentTimestamp();
            end_nested_level(span_end_time);
            u_sess->trace_cxt.nested_level--;
            handle_trace_error(traceparent, queryDesc, span_end_time);
        } else {
            u_sess->trace_cxt.nested_level--;
        }
        PG_RE_THROW();
    }
    PG_END_TRY();

    /* Same as above, tracing could have been aborted, check for current_trace_spans */
    if (current_trace_spans == NULL) {
        u_sess->trace_cxt.nested_level--;
        return;
    }

    /* End nested level */
    span_end_time = GetCurrentTimestamp();
    end_nested_level(span_end_time);
    u_sess->trace_cxt.nested_level--;
    /* End ExecutorRun span and store it */
    pop_and_store_active_span(span_end_time);
    per_level_infos[nested_level].executor_run_end = span_end_time;
}

/*
 * ExecutorFinish hook: create executor finish span and track nesting depth.
 * ExecutorFinish can start nested queries through triggers so we need
 * to set the ExecutorFinish span as the top span.
 */
void instr_trace_ExecutorFinish(QueryDesc *queryDesc)
{
    if (!instr_level_trace_enable()) {
        if (t_thrd.statement_cxt.instr_trace_prev_ExecutorFinish) {
            (*(ExecutorFinish_hook_type)t_thrd.statement_cxt.instr_trace_prev_ExecutorFinish)(queryDesc);
        } else {
            standard_ExecutorFinish(queryDesc);
        }
        return;
    }

    initialize_trace_level(&(u_sess->trace_cxt));

    SpanContext span_context;
    TimestampTz span_end_time;
    int            num_stored_spans = 0;
    Traceparent *traceparent = u_sess->trace_cxt.executor_traceparent;
    TraceSpans * current_trace_spans = u_sess->trace_cxt.current_trace_spans;
    MemoryContext trace_mem_ctx = u_sess->trace_cxt.trace_mem_ctx;
    int nested_level = u_sess->trace_cxt.nested_level;

    /* Statement is sampled */
    initialize_span_context(&span_context, traceparent, queryDesc->plannedstmt, NULL, queryDesc->sourceText);
    push_active_span(trace_mem_ctx, &span_context, cmd_type_to_span_type(queryDesc->operation),
                     HOOK_EXECUTOR);
    /* Create ExecutorFinish as a new potential top span */
    push_child_active_span(trace_mem_ctx, &span_context, SPAN_EXECUTOR_FINISH);

    /* Save the initial number of spans for the current session. We will only
     * store ExecutorFinish span if we have created nested spans. */
    num_stored_spans = current_trace_spans->end;

    if (nested_level == 0) {
        /* Save the root query_id to be used by the xact commit hook */
        u_sess->trace_cxt.current_query_id = queryDesc->plannedstmt->queryId;
    }

    u_sess->trace_cxt.nested_level++;
    PG_TRY();
    {
        if (t_thrd.statement_cxt.instr_trace_prev_ExecutorFinish) {
            (*(ExecutorFinish_hook_type)t_thrd.statement_cxt.instr_trace_prev_ExecutorFinish)(queryDesc);
        } else {
            standard_ExecutorFinish(queryDesc);
        }
    }
    PG_CATCH();
    {
        /* current_trace_spans may be NULL if an after trigger call pg_tracing_consume_span. */
        if (current_trace_spans == NULL) {
            u_sess->trace_cxt.nested_level--;
        } else {
            span_end_time = GetCurrentTimestamp();
            end_nested_level(span_end_time);
            u_sess->trace_cxt.nested_level--;
            handle_trace_error(traceparent, queryDesc, span_end_time);
        }
        PG_RE_THROW();
    }
    PG_END_TRY();

    /* Tracing may have been aborted, check and bail out if it's the case */
    if (current_trace_spans == NULL) {
        u_sess->trace_cxt.nested_level--;
        return;
    }

    /* We only trace executorFinish when it has a nested query, check if we have a possible child */
    if (current_trace_spans->end > num_stored_spans) {
        span_end_time = GetCurrentTimestamp();
        end_nested_level(span_end_time);
        pop_and_store_active_span(span_end_time);
    } else {
        pop_active_span();
    }
    u_sess->trace_cxt.nested_level--;
}

/*
 * ExecutorEnd hook will:
 * - process queryDesc to extract informations from query instrumentation
 * - end top span for the current nested level
 * - end tracing if we're at the root level
 */
void instr_trace_ExecutorEnd(QueryDesc *queryDesc)
{
    if (!instr_level_trace_enable()) {
        if (t_thrd.statement_cxt.instr_trace_prev_ExecutorEnd) {
            (*(ExecutorEnd_hook_type)t_thrd.statement_cxt.instr_trace_prev_ExecutorEnd)(queryDesc);
        } else {
            standard_ExecutorEnd(queryDesc);
        }
        return;
    }

    initialize_trace_level(&(u_sess->trace_cxt));

    TimestampTz parent_end;
    TimestampTz span_end_time;
    Traceparent *traceparent = u_sess->trace_cxt.executor_traceparent;
    int nested_level = u_sess->trace_cxt.nested_level;

    /*
     * We're at the end of the Executor for this level, generate spans from
     * the planstate if needed
     */
    parent_end = u_sess->trace_cxt.per_level_infos[nested_level].executor_run_end;

    if (u_sess->trace_cxt.index_planstart > 0) {
        process_query_desc(traceparent, queryDesc, 0, parent_end);
    }

    if (t_thrd.statement_cxt.instr_trace_prev_ExecutorEnd) {
        (*(ExecutorEnd_hook_type)t_thrd.statement_cxt.instr_trace_prev_ExecutorEnd)(queryDesc);
    } else {
        standard_ExecutorEnd(queryDesc);
    }

    span_end_time = GetCurrentTimestamp();
    pop_and_store_active_span(span_end_time);
}

/*
 * ProcessUtility hook
 * Trace utility query if utility tracking is enabled and sampling was enabled
 * during parse step.
 * Process utility may create nested queries (for example function CALL) so we need
 * to set the ProcessUtility span as the top span before going through the standard
 * code path.
 */
void instr_trace_ProcessUtility(processutility_context* processutility_cxt,
    DestReceiver* dest,
#ifdef PGXC
    bool sent_to_remote,
#endif /* PGXC */
    char* completion_tag,
    ProcessUtilityContext context,
    bool isCTAS)
{
    if (!instr_level_trace_enable()) {
        if (t_thrd.statement_cxt.instr_trace_prev_ProcessUtility) {
            (*(ProcessUtility_hook_type)t_thrd.statement_cxt.instr_trace_prev_ProcessUtility)(processutility_cxt,
                dest,
#ifdef PGXC
                sent_to_remote,
#endif
                completion_tag,
                context,
                isCTAS);
        } else {
            standard_ProcessUtility(processutility_cxt,
                dest,
#ifdef PGXC
                sent_to_remote,
#endif
                completion_tag,
                context,
                isCTAS);
        }
        return;
    }

    initialize_trace_level(&(u_sess->trace_cxt));

    TimestampTz span_end_time;
    SpanContext span_context;
    bool in_aborted_transaction;
    Span* tx_block_span = u_sess->trace_cxt.tx_block_span;
    TraceSpans *current_trace_spans = u_sess->trace_cxt.current_trace_spans;
    MemoryContext trace_mem_ctx = u_sess->trace_cxt.trace_mem_ctx;
    Traceparent *traceparent = u_sess->trace_cxt.executor_traceparent;

    if (u_sess->trace_cxt.nested_level == 0) {
        /* We're at root level, copy the parse traceparent */
        traceparent = u_sess->trace_cxt.parse_traceparent;
        reset_traceparent(u_sess->trace_cxt.parse_traceparent);

        /* Update tracked query_id */
        u_sess->trace_cxt.current_query_id = hash_string(processutility_cxt->query_string);
    }
    traceparent = u_sess->trace_cxt.tx_traceparent;

    /* Save whether we're in an aborted transaction. A rollback will reset the state after standard_ProcessUtility */
    in_aborted_transaction = IsAbortedTransactionBlockState();

    initialize_span_context(&span_context, traceparent, NULL, NULL, processutility_cxt->query_string);

    push_active_span(trace_mem_ctx, &span_context, cmd_type_to_span_type(CMD_UTILITY), HOOK_EXECUTOR);
    push_child_active_span(trace_mem_ctx, &span_context, SPAN_PROCESS_UTILITY);

    u_sess->trace_cxt.nested_level++;
    PG_TRY();
    {
        if (t_thrd.statement_cxt.instr_trace_prev_ProcessUtility) {
            (*(ProcessUtility_hook_type)t_thrd.statement_cxt.instr_trace_prev_ProcessUtility)(processutility_cxt,
                dest,
#ifdef PGXC
                sent_to_remote,
#endif
                completion_tag,
                context,
                isCTAS);
        } else {
            standard_ProcessUtility(processutility_cxt,
                dest,
#ifdef PGXC
                sent_to_remote,
#endif
                completion_tag,
                context,
                isCTAS);
        }
    }
    PG_CATCH();
    {
        if (current_trace_spans != NULL) {
            span_end_time = GetCurrentTimestamp();
            end_nested_level(span_end_time);
            u_sess->trace_cxt.nested_level--;
            handle_trace_error(traceparent, NULL, span_end_time);
        } else {
            u_sess->trace_cxt.nested_level--;
        }
        PG_RE_THROW();
    }
    PG_END_TRY();
    /* Same as above, abort if tracing was disabled within process utility */
    if (current_trace_spans == NULL) {
        u_sess->trace_cxt.nested_level--;
        return;
    }
    span_end_time = GetCurrentTimestamp();
    end_nested_level(span_end_time);
    u_sess->trace_cxt.nested_level--;

    /* End ProcessUtility span and store it */
    pop_and_store_active_span(span_end_time);

    /* Also end and store parent active span */
    pop_and_store_active_span(span_end_time);

    /* If we're in an aborted transaction, xact callback won't be called so we need to end tracing here */
}

/*
 * Handle xact callback events
 * Create a commit span between pre-commit and commit event and end ongoing tracing
 */
void instr_trace_xact_callback(XactEvent event, void *arg)
{
    if (!instr_level_trace_enable()) {
        return;
    }

    Span* commit_span = u_sess->trace_cxt.commit_span;
    Span* tx_block_span = u_sess->trace_cxt.tx_block_span;
    TimestampTz current_ts;

    if (u_sess->trace_cxt.current_trace_spans == NULL) {
        return;
    }

    switch (event) {
        case XACT_EVENT_ABORT:
        case XACT_EVENT_COMMIT:
            current_ts = GetCurrentTimestamp();
            end_nested_level(current_ts);
            if (commit_span->span_id > 0) {
                end_span(commit_span, &current_ts);
                store_span(commit_span);
            }
            if (tx_block_span->span_id > 0) {
                /* We don't use GetCurrentTransactionStopTimestamp as that
                 * would make the commit span overlaps and ends after the TransactionBlock */
                end_span(tx_block_span, &current_ts);
                store_span(tx_block_span);
            }

            reset_span(commit_span);
            reset_span(tx_block_span);
            break;
        default:
            break;
    }
}

static void append_trace_info(TraceExtent *extent, TraceInfo *trace_info)
{
    Assert(extent);
    if (trace_info == NULL || trace_info->n_nodes == 0) {
        return;
    }

    errno_t rc;
    size_t node_size;
    TraceNode *node = NULL;
    for (node = trace_info->head; node != NULL; node = (TraceNode *)node->next) {
        node_size = (node == trace_info->tail) ? trace_info->cur_pos : STATEMENT_TRACE_BUFSIZE;
        rc = memcpy_s(extent->trace + extent->cur_offset,
            (extent->max_trace_len - extent->cur_offset), node->buf, node_size);
        securec_check(rc, "\0", "\0");
        extent->cur_offset += node_size;
    }
}

/*
 * bytea data format:
 *   Record format: TOTAL_LEN + VERSION + SPAN_1 + SPAN_2 + ... + SPAN_N
 */
bytea* get_statement_trace_info(TraceInfo *trace_info)
{
    /* caculate bytea total len */
    uint32 size = 0;
    text *result = NULL;

    size += (STATEMENT_TRACE_BUFSIZE * (trace_info->n_nodes - 1) + trace_info->cur_pos);

    /* apply memory */
    result = (text*)palloc0(size + VARHDRSZ);
    SET_VARSIZE(result, size + VARHDRSZ);

    char *trace_data = VARDATA(result);

    /* ----append each areas into detail record---- */
    TraceExtent extent = {0};
    extent.trace = trace_data;
    extent.max_trace_len = size;

    append_trace_info(&extent, trace_info);
    ereport(LOG, (errmodule(MOD_INSTR), errmsg("[Statement] zff flush -trace len: %u", size)));
    return result;
}

void instr_trace_register_hook()
{
    // register hooks
    t_thrd.statement_cxt.instr_trace_prev_post_parse_analyze_hook = (void *)post_parse_analyze_hook;
    post_parse_analyze_hook = instr_trace_post_parse_analyze;
    t_thrd.statement_cxt.instr_trace_prev_planner_hook = (void *)planner_hook;
    planner_hook = instr_trace_planner;
    t_thrd.statement_cxt.instr_trace_prev_ExecutorStart = (void *)ExecutorStart_hook;
    ExecutorStart_hook = instr_trace_ExecutorStart;
    t_thrd.statement_cxt.instr_trace_prev_ExecutorRun = (void *)ExecutorRun_hook;
    ExecutorRun_hook = instr_trace_ExecutorRun;
    t_thrd.statement_cxt.instr_trace_prev_ExecutorFinish = (void *)ExecutorFinish_hook;
    ExecutorFinish_hook = instr_trace_ExecutorFinish;
    t_thrd.statement_cxt.instr_trace_prev_ExecutorEnd = (void *)ExecutorEnd_hook;
    ExecutorEnd_hook = instr_trace_ExecutorEnd;
    t_thrd.statement_cxt.instr_trace_prev_ProcessUtility = (void *)ProcessUtility_hook;
    ProcessUtility_hook = instr_trace_ProcessUtility;

    RegisterXactCallback(instr_trace_xact_callback, NULL);
}

