/*-------------------------------------------------------------------------
 * MIT License
 * Copyright (c) 2024, Datadog, Inc.
 *
 * Based on github's /DataDog/pg_tracing reconstruction
 * intr_trace_span.cpp
 *         instr_trace span functions.
 *
 * IDENTIFICATION
 *      src/gausskernel/cbb/instruments/trace/instr_trace_span.cpp
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/datavec/pg_prng.h"
#include "storage/proc.h"
#include "instr_trace_inner.h"
#define INITIAL_NUM_ACTIVESPANS (15)
#define INCREMENT_NUM_ACTIVESPANS (20)
#define INCREMENT_NUM_CURSPANS (50)

/*
 * repalloc0
 * Adjust the size of a previously allocated chunk and zero out the added space.
 */
void *repalloc0(void *pointer, Size oldsize, Size size)
{
    void *ret;

    /* catch wrong argument order */
    if (unlikely(oldsize > size)) {
        elog(ERROR, "invalid repalloc0 call: oldsize %zu, new size %zu", oldsize, size);
    }

    ret = repalloc(pointer, size);
    errno_t rc = memset_s((char *) ret + oldsize, (size - oldsize), 0, (size - oldsize));
    securec_check(rc, "\0", "\0");
    return ret;
}

void cleanup_active_spans(void)
{
    u_sess->trace_cxt.active_spans = NULL;
}

/*
 * Push a new active span to the active_spans stack
 */
static Span *allocate_new_active_span(MemoryContext context)
{
    Span *span;
    TraceSpans* active_spans = u_sess->trace_cxt.active_spans;
    if (active_spans == NULL) {
        MemoryContext oldcxt = MemoryContextSwitchTo(u_sess->trace_cxt.trace_mem_ctx);
        active_spans = (TraceSpans*)palloc0(sizeof(TraceSpans) + INITIAL_NUM_ACTIVESPANS * TRACE_SPAN_SIZE);
        active_spans->max = INITIAL_NUM_ACTIVESPANS;
        u_sess->trace_cxt.active_spans = active_spans;
        MemoryContextSwitchTo(oldcxt);
    } else if (active_spans->end >= active_spans->max) {
        int old_spans_max = active_spans->max;
        active_spans->max += INCREMENT_NUM_ACTIVESPANS;
        active_spans = (TraceSpans*)repalloc0(active_spans,
            sizeof(TraceSpans) + old_spans_max * TRACE_SPAN_SIZE,
            sizeof(TraceSpans) + active_spans->max * TRACE_SPAN_SIZE);
        u_sess->trace_cxt.active_spans = active_spans;
    }

    span = &(active_spans->spans[active_spans->end++]);
    span->nested_level =  u_sess->trace_cxt.nested_level;
    span->span_id = 0;
    return span;
}

/*
 * Get the latest active span
 */
Span* peek_active_span(void)
{
    TraceSpans* active_spans = u_sess->trace_cxt.active_spans;
    if (active_spans == NULL || active_spans->end == 0) {
        return NULL;
    }
    return &active_spans->spans[active_spans->end - 1];
}

/*
 * Get the active span matching the current nested level.
 * Returns NULL if there's no span or it doesn't match the level.
 */
static Span* peek_active_span_current_level(void)
{
    Span* span = peek_active_span();

    if (span == NULL) {
        return NULL;
    }

    if (span->nested_level !=  u_sess->trace_cxt.nested_level) {
        return NULL;
    }

    return span;
}

/*
 * Store a span in the current_trace_spans buffer
 */
void store_span(const Span * span)
{
    TraceSpans* current_trace_spans = u_sess->trace_cxt.current_trace_spans;
    if (current_trace_spans->end >= u_sess->trace_cxt.current_trace_spans->max) {
        int old_spans_max = current_trace_spans->max;
        current_trace_spans->max += INCREMENT_NUM_CURSPANS;
        current_trace_spans = (TraceSpans*)repalloc0(current_trace_spans,
            sizeof(TraceSpans) + old_spans_max * TRACE_SPAN_SIZE,
            sizeof(TraceSpans) + current_trace_spans->max * TRACE_SPAN_SIZE);

        u_sess->trace_cxt.current_trace_spans = current_trace_spans;
    }
    current_trace_spans->spans[current_trace_spans->end++] = *span;
}

/*
 * Store and return the latest active span
 */
Span *pop_and_store_active_span(const TimestampTz end_time)
{
    Span *span = pop_active_span();

    if (span == NULL) {
        return NULL;
    }

    end_span(span, &end_time);
    store_span(span);
    return span;
}

/*
 * Pop the latest active span
 */
Span *pop_active_span(void)
{
    TraceSpans *active_spans = u_sess->trace_cxt.active_spans;

    if (active_spans == NULL || active_spans->end == 0) {
        return NULL;
    }

    Span* span = &active_spans->spans[--active_spans->end];
    return span;
}

/*
 * Start a new active span if we've entered a new nested level or if the previous
 * span at the same level ended.
 */
static void begin_active_span(const SpanContext *span_context, Span *span,
    SpanType span_type, Span *parent_span)
{
    uint64 parent_id = 0;
    int8 parent_planstate_index = -1;

    if (u_sess->trace_cxt.nested_level == 0) {
        /* Root active span, use the parent id from the trace context */
        parent_id = span_context->traceparent->parent_id;
    } else {
        TracedPlanstate *parent_traced_planstate = NULL;

        /*
         * We're in a nested level, check if we have a parent planstate and
         * use it as a parent span
         */
        parent_planstate_index = get_parent_traced_planstate_index(u_sess->trace_cxt.nested_level);
        if (parent_planstate_index > -1) {
            parent_traced_planstate = get_traced_planstate_from_index(parent_planstate_index);
        }

        /*
         * Both planstate and previous top span can be the parent for the new
         * top span, we use the most recent as a parent. planstate must be
         * currently active to be a parent candidate.
         */
        if (parent_traced_planstate != NULL && parent_traced_planstate->node_start >= parent_span->start
            && !INSTR_TIME_IS_ZERO(((PlanState*)(parent_traced_planstate->planstate))->instrument->starttime)) {
            parent_id = parent_traced_planstate->span_id;
        } else {
            parent_id = (parent_span != NULL)? parent_span->span_id : 0;
            parent_planstate_index = -1;
        }
    }

    /* Keep track of the parent planstate index */
    span->parent_planstate_index = parent_planstate_index;
    begin_span(span_context->traceparent->trace_id, span, span_type,
        NULL, parent_id, span_context->query_id, span_context->start_time);
}

/*
 * Push a new span that will be the child of the latest active span
 */
Span *push_child_active_span(MemoryContext context, const SpanContext *span_context, SpanType span_type)
{
    Span *parent_span = peek_active_span();
    Span *span = allocate_new_active_span(context);

    uint64 span_id = (parent_span == NULL)? 0 : parent_span->span_id;
    begin_span(span_context->traceparent->trace_id, span, span_type, NULL,
        span_id, span_context->query_id, span_context->start_time);
    return span;
}

/*
 * Initialise buffers if we are in a new nested level and start associated active span.
 * If the active span already exists for the current nested level, this has no effect.
 *
 * This needs to be called every time an active span could be started: post parse,
 * planner, executor start and process utility
 *
 * In case of extended protocol using transaction block, the parse of the next
 * statement happens while the previous span is still ongoing. To avoid conflict,
 * we keep the active span for the next statement in next_active_span.
 */
Span *push_active_span(MemoryContext context, const SpanContext *span_context, SpanType span_type,
    HookType hook_type)
{
    Span *span = peek_active_span_current_level();
    Span *parent_span = peek_active_span();
    Span* next_active_span = u_sess->trace_cxt.next_active_span;

    if (span == NULL) {
        /* No active span or it belongs to the previous level, allocate a new one */
        span = allocate_new_active_span(context);
        if (next_active_span->span_id > 0) {
            /* next_active_span is valid, use it and reset it */
            span = next_active_span;
            reset_span(next_active_span);
            return span;
        }
    } else {
        if (hook_type != HOOK_PARSE ||  u_sess->trace_cxt.nested_level > 0) {
            return span;
        }

        /*
         * We're at level 0, in a parse hook while we still have an active
         * span. This is the parse command for the next statement, save it in
         * next_active_span.
         */
        span = next_active_span;
    }

    begin_active_span(span_context, span, span_type, parent_span);
    return span;
}

/*
 * Initialize span fields
 */
void begin_span(const char* trace_id, Span *span, SpanType type,
    const uint64 *span_id, uint64 parent_id, uint64 query_id,
    TimestampTz start_span)
{
    errno_t rc = memcpy_s(span->trace_id, MAX_TRACE_ID_SIZE, trace_id, MAX_TRACE_ID_SIZE);
    securec_check(rc, "\0", "\0");
    span->start = start_span;
    span->type = type;

    /*
     * If parent id is unset, it means that there's no propagated trace
     * informations from the caller. In this case, this is the top span,
     * span_id == parent_id and we use 0 for the span
     * id.
     */

    span->parent_id = parent_id;
    if (span_id != NULL) {
        span->span_id = *span_id;
    } else {
        span->span_id = pg_prng_uint64(&pg_global_prng_state);
    }

    span->sql_error_code = 0;
    span->query_id = query_id;
}

/*
 * Set span duration and accumulated buffers.
 * end_span_input is optional, if NULL is passed, we use
 * the current time
 */
void end_span(Span* span, const TimestampTz *end_time_input)
{
    /* Set span duration with the end time before subtracting the start */
    if (end_time_input == NULL || *end_time_input == 0) {
        span->end = GetCurrentTimestamp();
    } else {
        span->end = *end_time_input;
    }

    Assert(span->end >= span->start);
}

/*
 * Reset span
 */
void reset_span(Span *span)
{
    span->span_id = 0;
}

