/*-------------------------------------------------------------------------
 * MIT License
 * Copyright (c) 2024, Datadog, Inc.
 *
 * Based on github's /DataDog/pg_tracing reconstruction
 * instr_trace_inner.h
 *     Header for instr_trace.
 *
 * IDENTIFICATION
 *     src/gausskernel/cbb/instruments/trace/instr_trace_inner.h
 *-------------------------------------------------------------------------
 */
#ifndef _INSTR_TRACE_INNER_H_
#define _INSTR_TRACE_INNER_H_

#include "instruments/instr_trace.h"
#include "nodes/plannodes.h"
#include "executor/exec/execdesc.h"
#include "nodes/execnodes.h"

/* Location of external text file */
#define NS_PER_S    INT64CONST(1000000000)
#define INT64_HEX_FORMAT "%o16"
#define INT64_MODIFIER "x"

/*
 * Hook currently called
 */
typedef enum HookType {
    HOOK_PARSE,
    HOOK_UTILITY,
    HOOK_PLANNER,
    HOOK_EXECUTOR,
} HookType;

/*
 * Error code when parsing traceparent field
 */
typedef enum ParseTraceparentErr {
    PARSE_OK = 0,
    PARSE_INCORRECT_SIZE,
    PARSE_NO_TRACEPARENT_FIELD,
    PARSE_INCORRECT_TRACEPARENT_SIZE,
    PARSE_INCORRECT_FORMAT,
} ParseTraceparentErr;

typedef struct SpanContext {
    TimestampTz start_time;
    Traceparent *traceparent;
    const PlannedStmt *pstmt;
    const Query *query;
    const char *query_text;
    uint64 query_id;
    int max_parameter_size;
} SpanContext;

typedef struct TraceHeader {
    int32 total_len;
    int version;
    int span_count;
} TraceHeader;

/* Context needed when generating spans from planstate */
typedef struct PlanstateTraceContext {
    char trace_id[MAX_TRACE_ID_SIZE];
    int  sql_error_code;
} PlanstateTraceContext;

extern ParseTraceparentErr parse_trace_context(Traceparent* traceparent,
    const char *trace_context_str, int trace_context_len);
extern char *parse_code_to_err(ParseTraceparentErr err);

/* pg_tracing_span.c */
extern void begin_span(const char* trace_id, Span* span, SpanType type,
    const uint64* span_id, uint64 parent_id, uint64 query_id,
    TimestampTz start_span);
extern void end_span(Span* span, const TimestampTz* end_time);
extern void reset_span(Span* span);
extern const char *get_operation_name(const Span* span);


/* pg_tracing_active_spans.c */
extern Span *pop_and_store_active_span(const TimestampTz end_time);
extern Span *pop_active_span(void);
extern Span *peek_active_span(void);
extern Span *push_active_span(MemoryContext context, const SpanContext* span_context,
    SpanType span_type, HookType hook_type);
extern Span *push_child_active_span(MemoryContext context, const SpanContext* span_context,
    SpanType span_type);

extern void end_span(Span* span, const TimestampTz *end_time_input);

extern void cleanup_active_spans(void);

extern void process_planstate(const Traceparent *traceparent, const QueryDesc *queryDesc,
    int sql_error_code, uint64 parent_id, uint64 query_id,
    TimestampTz parent_start, TimestampTz parent_end);

extern TracedPlanstate *get_traced_planstate_from_index(int index);

extern int get_parent_traced_planstate_index(int nested_level);

extern TimestampTz get_span_end_from_planstate(PlanState *planstate, TimestampTz plan_start, TimestampTz root_end);

extern void setup_ExecProcNode_override(MemoryContext context, QueryDesc *queryDesc);

extern int number_nodes_from_planstate(PlanState *planstate);

extern void *repalloc0(void *pointer, Size oldsize, Size size);

extern void store_span(const Span *span);

extern void reset_span(Span *span);

#endif
