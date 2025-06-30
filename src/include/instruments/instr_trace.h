/*
 * MIT License
 * Copyright (c) 2024, Datadog, Inc.
 *
 * Based on github's /DataDog/pg_tracing reconstruction
 * instr_trace.h
 *        definitions for manager used for trace
 *
 *
 * IDENTIFICATION
 *        src/include/instruments/instr_trace.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef _INSTR_TRACE_H_
#define _INSTR_TRACE_H_

#include "c.h"
#include "pgstat.h"
#include "nodes/parsenodes_common.h"
#include "nodes/plannodes.h"
#include "parser/parse_node.h"
#include "executor/exec/execdesc.h"
#include "nodes/execnodes.h"
#include "tcop/utility.h"
#include "instruments/instr_statement.h"

#define USESS_TRACE_CXT (u_sess->trace_cxt)
#define STATEMENT_TRACE_VERSION_V1 (1)
#define TRACE_SPAN_SIZE (sizeof(Span))

/*
 * SpanType: Type of the span
 */
typedef enum SpanType {
    SPAN_PLANNER,                       /* Wraps planner execution in planner hook */
    SPAN_FUNCTION,                      /* Wraps function in fmgr hook */
    SPAN_PROCESS_UTILITY,               /* Wraps ProcessUtility execution */

    SPAN_EXECUTOR_RUN,                 /* Wraps Executor run hook */
    SPAN_EXECUTOR_FINISH,               /* Wraps Executor finish hook */
    SPAN_TRANSACTION_COMMIT,            /* Wraps time between pre-commit and commit */
    SPAN_TRANSACTION_BLOCK,             /* Represents an explicit transaction block */

    /* Represents a node execution, generated from planstate */
    SPAN_NODE,
    SPAN_NODE_RESULT,
    SPAN_NODE_PROJECT_SET,

    /* Modify table */
    SPAN_NODE_INSERT,
    SPAN_NODE_UPDATE,
    SPAN_NODE_DELETE,
    SPAN_NODE_MERGE,

    SPAN_NODE_APPEND,
    SPAN_NODE_MERGE_APPEND,
    SPAN_NODE_RECURSIVE_UNION,
    SPAN_NODE_BITMAP_AND,
    SPAN_NODE_BITMAP_OR,
    SPAN_NODE_NESTLOOP,
    SPAN_NODE_MERGE_JOIN,
    SPAN_NODE_HASH_JOIN,
    SPAN_NODE_SEQ_SCAN,
    SPAN_NODE_SAMPLE_SCAN,
    SPAN_NODE_GATHER,
    SPAN_NODE_GATHER_MERGE,
    SPAN_NODE_INDEX_SCAN,
    SPAN_NODE_INDEX_ONLY_SCAN,
    SPAN_NODE_BITMAP_INDEX_SCAN,
    SPAN_NODE_BITMAP_HEAP_SCAN,
    SPAN_NODE_TID_SCAN,
    SPAN_NODE_TID_RANGE_SCAN,
    SPAN_NODE_SUBQUERY_SCAN,
    SPAN_NODE_FUNCTION_SCAN,
    SPAN_NODE_TABLEFUNC_SCAN,
    SPAN_NODE_VALUES_SCAN,
    SPAN_NODE_CTE_SCAN,
    SPAN_NODE_NAMED_TUPLE_STORE_SCAN,
    SPAN_NODE_WORKTABLE_SCAN,

    SPAN_NODE_FOREIGN_SCAN,
    SPAN_NODE_FOREIGN_INSERT,
    SPAN_NODE_FOREIGN_UPDATE,
    SPAN_NODE_FOREIGN_DELETE,

    SPAN_NODE_CUSTOM_SCAN,
    SPAN_NODE_MATERIALIZE,
    SPAN_NODE_MEMOIZE,
    SPAN_NODE_SORT,
    SPAN_NODE_INCREMENTAL_SORT,
    SPAN_NODE_GROUP,

    SPAN_NODE_AGGREGATE,
    SPAN_NODE_GROUP_AGGREGATE,
    SPAN_NODE_HASH_AGGREGATE,
    SPAN_NODE_MIXED_AGGREGATE,

    SPAN_NODE_WINDOW_AGG,
    SPAN_NODE_UNIQUE,

    SPAN_NODE_SETOP,
    SPAN_NODE_SETOP_HASHED,

    SPAN_NODE_LOCK_ROWS,
    SPAN_NODE_LIMIT,
    SPAN_NODE_HASH,

    SPAN_NODE_INIT_PLAN,
    SPAN_NODE_SUBPLAN,

    SPAN_NODE_UNKNOWN,

    /* Top Span types. They are created from the query cmdType */
    SPAN_TOP_SELECT,
    SPAN_TOP_INSERT,
    SPAN_TOP_UPDATE,
    SPAN_TOP_DELETE,
    SPAN_TOP_MERGE,
    SPAN_TOP_UTILITY,
    SPAN_TOP_NOTHING,
    SPAN_TOP_UNKNOWN,

    /* Must be last! */
    NUM_SPAN_TYPE,
} SpanType;

struct PlanState;
/*
 * Match a planstate to the first start of a node.
 * This is needed to set the start for spans generated from planstate.
 */
typedef struct TracedPlanstate {
    PlanState  *planstate;
    TimestampTz node_start;
    uint64        span_id;
    int           nested_level;
} TracedPlanstate;

/*
 * The Span data structure represents an operation with a start, a duration
 * and metadatas.
 */
typedef struct Span {
    char trace_id[MAX_TRACE_ID_SIZE];
    uint64        span_id;          /* Span Identifier generated from a random uint64 */
    uint64        parent_id;        /* Span's parent id. For the top span. */
                                    /* For other spans, we pass the parent's span */
    uint64        query_id;         /* QueryId of the trace query if available */
    TimestampTz start;              /* Start of the span */
    TimestampTz end;                /* End of the span */
    SpanType      type;             /* Type of the span. Used to generate the span's name */
    uint8         nested_level;     /* Nested level of this span this span. Internal usage only */
    int           parent_planstate_index; /* Index to the parent planstate of
                                            * this span. Internal usage only */

    int            sql_error_code;    /* query error code extracted from ErrorData, 0 if query was successful */
} Span;

/*
 * Traceparent values propagated by the caller
 */
typedef struct Traceparent {
    char trace_id[MAX_TRACE_ID_SIZE];
    uint64 parent_id;        /* Span id of the parent */
} Traceparent;

/*
 * Structure to store flexible array of spans
 */
typedef struct TraceSpans {
    int         end;            /* Index of last element */
    int         max;            /* Maximum number of element */
    Span        spans[FLEXIBLE_ARRAY_MEMBER];
} TraceSpans;

/*
 * Structure to store per exec level informations
 */
typedef struct PerLevelInfos {
    uint64        executor_run_span_id;    /* executor run span id for this level. Executor run is used as
                                         * parent for spans generated from planstate */
    TimestampTz executor_run_start;
    TimestampTz executor_run_end;
} PerLevelInfos;

/* statment history trace column */
typedef struct TraceExtent {
    char   *trace;
    uint32 max_trace_len;
    uint32 cur_offset;
    uint32 data_len;
} TraceExtent;

bytea* get_statement_trace_info(TraceInfo *trace_info);

void instr_trace_register_hook();

bool instr_level_trace_enable();

void end_tracing(void);
#endif
