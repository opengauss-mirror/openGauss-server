/*-------------------------------------------------------------------------
 * MIT License
 * Copyright (c) 2024, Datadog, Inc.
 *
 * Based on github's /DataDog/pg_tracing reconstruction
 * instr_trace_sql_func.cpp
 *         sql functions used by instr trace
 *
 * IDENTIFICATION
 *      src/gausskernel/cbb/instruments/trace/intr_trace_sql_func.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "funcapi.h"
#include "instr_trace_inner.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/timestamp.h"

static const char* const span_str[] = {
    "Planner",
    "Function",
    "ProcessUtility",
    "ExecutorRun",
    "ExecutorFinish",
    "TransactionCommit",
    "TransactionBlock",
    "Node",
    "Result",
    "ProjectSet",
    "Insert",
    "Update",
    "Delete",
    "Merge",
    "Append",
    "MergeAppend",
    "RecursiveUnion",
    "BitmapAnd",
    "BitmapOr",
    "NestedLoop",
    "MergeJoin",
    "HashJoin",
    "SeqScan",
    "SampleScan",
    "Gather",
    "GatherMerge",
    "IndexScan",
    "IndexOnlyScan",
    "BitmapIndexScan",
    "BitmapHeapScan",
    "TidScan",
    "TidRangeScan",
    "SubqueryScan",
    "FunctionScan",
    "TablefuncScan",
    "ValuesScan",
    "CTEScan",
    "NamedTupleStoreScan",
    "WorktableScan",
    "ForeignScan",
    "ForeignInsert",
    "ForeignUpdate",
    "ForeignDelete",
    "CustomScan",
    "Materialize",
    "Memoize",
    "Sort",
    "IncrementalSort",
    "Group",
    "Aggregate",
    "GroupAggregate",
    "HashAggregate",
    "MixedAggregate",
    "WindowAgg",
    "Unique",
    "Setop",
    "SetopHashed",
    "LockRows",
    "Limit",
    "Hash",
    "InitPlan",
    "SubPlan",
    "UnknownNode",
    "Select query",
    "Insert query",
    "Update query",
    "Delete query",
    "Merge query",
    "Utility query",
    "Nothing query",
    "Unknown query",
    "Unknown type"
};

static bool is_span_top(SpanType span_type)
{
    return span_type >= SPAN_TOP_SELECT && span_type <= SPAN_TOP_UNKNOWN;
}

static bool is_last_span(const Span *spans, uint32 total_count, uint32 cur_idx)
{
    if (cur_idx == total_count - 1) {
        return TRUE;
    }

    bool cur_level_last = TRUE;
    uint8 level = spans[cur_idx].nested_level;

    for (uint32 i = cur_idx + 1; i < total_count; i++) {
        if (is_span_top(spans[i].type)) {
            return TRUE;
        }
        if (spans[i].nested_level == level - 1) {
            return FALSE;
        }
    }

    return cur_level_last;
}

/*
 * Convert span_type to string
 */
const char *span_type_to_str(SpanType span_type)
{
    if (span_type <= NUM_SPAN_TYPE) {
        return span_str[span_type];
    } else {
        return "Unknown";
    }
}

/*
 * Get the operation of a span.
 * For node span, the name may be pulled from the stat file.
 */
void trace_get_operation_name(StringInfoData* operation_out, Span *spans, uint32 total_count, uint32 cur_idx)
{
    const char *operation_str;
    Span span = spans[cur_idx];
    operation_str = span_type_to_str(span.type);
    uint8 level = spans[cur_idx].nested_level;

    if (is_span_top(span.type)) {
        appendStringInfo(operation_out, "%s", "├─");
        appendStringInfo(operation_out, "%s", operation_str);
        return;
    }
    if (level == 0) {
        appendStringInfo(operation_out, "%s", "    ");
        appendStringInfo(operation_out, "%s", "├─");
    } else if (level == 1) {
        appendStringInfo(operation_out, "%s", "    ");
        if (!is_last_span(spans, total_count, cur_idx)) {
            appendStringInfo(operation_out, "%s", "|   ");
        } else {
            appendStringInfo(operation_out, "%s", "    ");
        }
        appendStringInfo(operation_out, "%s", "├─");
    } else if (level == 2) {
        appendStringInfo(operation_out, "%s", "    ");
        appendStringInfo(operation_out, "%s", "    ");
        if (!is_last_span(spans, total_count, cur_idx)) {
            appendStringInfo(operation_out, "%s", "|   ");
        } else {
            appendStringInfo(operation_out, "%s", "    ");
        }
        appendStringInfo(operation_out, "%s", "├─");
    } else {
        appendStringInfo(operation_out, "%s", "    ");
        appendStringInfo(operation_out, "%s", "    ");
        appendStringInfo(operation_out, "%s", "    ");
        if (!is_last_span(spans, total_count, cur_idx)) {
            appendStringInfo(operation_out, "%s", "|   ");
        } else {
            appendStringInfo(operation_out, "%s", "    ");
        }
        appendStringInfo(operation_out, "%s", "├─");
    }

    appendStringInfo(operation_out, "%s", operation_str);
    return;
}

void trace_decode_span(StringInfo resultBuf, Span* spans, uint32 total_span_count, uint32 index)
{
    StringInfoData operation_str;
    initStringInfo(&operation_str);
    Span span = spans[index];

    trace_get_operation_name(&operation_str, spans, total_span_count, index);
    appendStringInfo(resultBuf, "%-30s\t", operation_str.data);
    appendStringInfoChar(resultBuf, '\t');
    appendStringInfo(resultBuf, "%15lu (us)\t", span.end - span.start);
    appendStringInfo(resultBuf, "%s\t", timestamptz_to_str(span.start));
    appendStringInfo(resultBuf, "%s\n", timestamptz_to_str(span.end));
    pfree(operation_str.data);
}

static int instr_compare_trace(const void *a, const void *b)
{
    if (a == NULL || b == NULL) {
        return 0;
    }
    return (((Span*)a)->start < ((Span*)b)->start ? -1 : 1);
}

void trace_decode_helper(StringInfo resultBuf,
    char *trace, uint32 trace_len, bool *is_valid_record)
{
    /* trace get area type */
    uint32 offset = 0;
    uint32 total_span_count = 0;
    uint32 cur_span_count = 0;
    int rc = 0;

    rc = memcpy_s(&total_span_count, sizeof(uint32), trace + offset, sizeof(uint32));
    securec_check(rc, "\0", "\0");
    offset += sizeof(uint32);

    if (total_span_count == 0) {
        return;
    }
    Span* spans = (Span*)palloc0(sizeof(Span) * total_span_count);
    uint32 span_len = trace_len   - sizeof(uint32);

    while (offset < span_len) {
        rc = memcpy_s(&(spans[cur_span_count]), TRACE_SPAN_SIZE, trace + offset, TRACE_SPAN_SIZE);
        securec_check(rc, "\0", "\0");
        offset += TRACE_SPAN_SIZE;

        cur_span_count++;
        if (cur_span_count > total_span_count && offset < span_len) {
            pfree(spans);
            return;
        }
    }

    qsort(spans, total_span_count, sizeof(Span), &instr_compare_trace);
    for (uint32 i = 0; i < total_span_count; i++) {
        trace_decode_span(resultBuf, spans, total_span_count, i);
    }

    pfree(spans);
}

/* check the header and valid length of the trace binary data */
static bool intsr_trace_is_valid(uint32 bytea_data_len, const char *trace_str, uint32 *trace_str_len)
{
    /* VERSIZE(bytea) - VARHDRSZ should be > sizeof(uint32) + STATEMENT_DETAILS_HEAD_SIZE */
    if (bytea_data_len <= (sizeof(TraceHeader))) {
        return false;
    }

    int rc = memcpy_s(trace_str_len, sizeof(int32), trace_str, sizeof(int32));
    securec_check(rc, "\0", "\0");

    /* details binary length should be same with VERSIZE(bytea) - VARHDRSZ */
    if (bytea_data_len != *trace_str_len) {
        return false;
    }

    /* record version, should be [1, STATEMENT_DETAIL_VERSION] */
    int32 version = (int)trace_str[sizeof(uint32)];
    if (version != STATEMENT_TRACE_VERSION_V1) {
        return false;
    }
    return true;
}

static char* instr_trace_decode(bytea *trace)
{
    // total_len + version + spancount + span1 + span2 + ... + span...
    uint32 bytea_data_len = VARSIZE(trace) - VARHDRSZ;
    if (bytea_data_len == 0) {
        return NULL;
    }

    char *trace_str = (char *)VARDATA(trace);
    uint32 trace_str_len = 0;

    if (!intsr_trace_is_valid(bytea_data_len, trace_str, &trace_str_len)) {
        return pstrdup("invalid trace header");
    }

    bool is_valid_record = true;
    uint32 offset = sizeof(uint32);
    StringInfoData resultBuf;
    initStringInfo(&resultBuf);

    int trace_version = trace_str[sizeof(uint32)];
    offset += sizeof(int);
    if (trace_version == STATEMENT_TRACE_VERSION_V1) {
        trace_decode_helper(&resultBuf, trace_str + offset, trace_str_len - offset, &is_valid_record);
    }

    if (!is_valid_record) {
        pfree(resultBuf.data);
        return pstrdup("invalid trace data");
    }

    /* after is_valid_detail_record called, resultBuf.len never equal to 0 */
    resultBuf.data[resultBuf.len - 1] = ' ';
    return resultBuf.data;
}

Datum statement_trace_decode(PG_FUNCTION_ARGS)
{
    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();
    }
    bytea *traceText = DatumGetByteaP(PG_GETARG_DATUM(0));

    char *traceStr = instr_trace_decode(traceText);
    if (traceStr == NULL) {
        PG_RETURN_NULL();
    }

    text* result = cstring_to_text(traceStr);
    pfree(traceStr);
    PG_RETURN_TEXT_P(result);
}

