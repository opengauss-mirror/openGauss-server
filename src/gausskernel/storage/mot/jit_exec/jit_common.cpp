/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * jit_common.cpp
 *    Definitions used by LLVM jitted code.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/jit_exec/jit_common.cpp
 *
 * -------------------------------------------------------------------------
 */

/*
 * ATTENTION:
 * 1. Be sure to include gscodegen.h before anything else to avoid clash with PM definition in datetime.h.
 * 2. Be sure to include libintl.h before gscodegen.h to avoid problem with gettext.
 */
#include "libintl.h"
#include "codegen/gscodegen.h"
#include "postgres.h"
#include "catalog/pg_operator.h"
#include "utils/plpgsql.h"
#include "catalog/pg_language.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_proc.h"
#include "utils/builtins.h"
#include "utils/numeric.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "storage/mot/jit_exec.h"
#include "commands/proclang.h"

#include "global.h"
#include "jit_common.h"
#include "utilities.h"
#include "jit_plan.h"
#include "mm_global_api.h"
#include "catalog_column_types.h"
#include "mot_internal.h"

namespace JitExec {
DECLARE_LOGGER(JitCommon, JitExec)

const char* JitRuntimeFaultToString(int faultCode)
{
    switch (faultCode) {
        case JIT_FAULT_INTERNAL_ERROR:
            return "Internal Error";

        case JIT_FAULT_SUB_TX_NOT_CLOSED:
            return "Sub-transaction not closed";

        default:
            return "Unknown generic fault code";
    }
}

const char* CommandToString(JitCommandType commandType)
{
    switch (commandType) {
        case JIT_COMMAND_INSERT:
            return "Insert";
        case JIT_COMMAND_UPDATE:
            return "Point-Update";
        case JIT_COMMAND_SELECT:
            return "Point-Select";
        case JIT_COMMAND_DELETE:
            return "Point-Delete";
        case JIT_COMMAND_RANGE_UPDATE:
            return "Range-Update";
        case JIT_COMMAND_RANGE_SELECT:
            return "Range-Select";
        case JIT_COMMAND_RANGE_DELETE:
            return "Range-Delete";
        case JIT_COMMAND_FULL_SELECT:
            return "Full-Select";
        case JIT_COMMAND_AGGREGATE_RANGE_SELECT:
            return "Aggregate-Range-Select";
        case JIT_COMMAND_POINT_JOIN:
            return "Point-Join";
        case JIT_COMMAND_RANGE_JOIN:
            return "Range-Join";
        case JIT_COMMAND_AGGREGATE_JOIN:
            return "Aggregate-Range-Join";
        case JIT_COMMAND_COMPOUND_SELECT:
            return "Compound-Select";
        case JIT_COMMAND_FUNCTION:
            return "Function";
        case JIT_COMMAND_INVOKE:
            return "Invoke";

        case JIT_COMMAND_INVALID:
        default:
            return "Invalid command";
    }
}

bool IsRangeCommand(JitCommandType commandType)
{
    bool rangeCommand = false;

    switch (commandType) {
        case JIT_COMMAND_RANGE_UPDATE:
        case JIT_COMMAND_RANGE_SELECT:
        case JIT_COMMAND_RANGE_DELETE:
        case JIT_COMMAND_AGGREGATE_RANGE_SELECT:
        case JIT_COMMAND_POINT_JOIN:
        case JIT_COMMAND_RANGE_JOIN:
        case JIT_COMMAND_AGGREGATE_JOIN:
            rangeCommand = true;
            break;

        default:
            rangeCommand = false;
            break;
    }

    return rangeCommand;
}

bool IsJoinCommand(JitCommandType commandType)
{
    bool joinCommand = false;

    switch (commandType) {
        case JIT_COMMAND_POINT_JOIN:
        case JIT_COMMAND_RANGE_JOIN:
        case JIT_COMMAND_AGGREGATE_JOIN:
            joinCommand = true;
            break;

        default:
            joinCommand = false;
            break;
    }

    return joinCommand;
}

bool IsSelectCommand(JitCommandType commandType)
{
    bool selectCommand = false;

    switch (commandType) {
        case JIT_COMMAND_SELECT:
        case JIT_COMMAND_RANGE_SELECT:
        case JIT_COMMAND_FULL_SELECT:
        case JIT_COMMAND_AGGREGATE_RANGE_SELECT:
        case JIT_COMMAND_POINT_JOIN:
        case JIT_COMMAND_RANGE_JOIN:
        case JIT_COMMAND_AGGREGATE_JOIN:
        case JIT_COMMAND_COMPOUND_SELECT:
            selectCommand = true;
            break;

        default:
            selectCommand = false;
            break;
    }

    return selectCommand;
}

bool IsCommandUsingIndex(JitCommandType commandType)
{
    bool usingIndex = true;

    switch (commandType) {
        case JIT_COMMAND_INSERT:
        case JIT_COMMAND_FUNCTION:
        case JIT_COMMAND_INVOKE:
            usingIndex = false;
            break;

        default:
            break;
    }

    return usingIndex;
}

bool IsWriteCommand(JitCommandType commandType)
{
    bool writeCommand = false;

    switch (commandType) {
        case JIT_COMMAND_INSERT:
        case JIT_COMMAND_UPDATE:
        case JIT_COMMAND_DELETE:
        case JIT_COMMAND_RANGE_UPDATE:
        case JIT_COMMAND_RANGE_DELETE:
            writeCommand = true;
            break;

        default:
            break;
    }

    return writeCommand;
}

MOT::Table* GetTableFromQuery(const Query* query)
{
    // we reject queries that reference more than one table
    if (list_length(query->rtable) != 1) {
        MOT_LOG_TRACE("Cannot retrieve query table: query does not have exactly one rtable entry");
        return NULL;
    }

    RangeTblEntry* rte = (RangeTblEntry*)linitial(query->rtable);
    Oid relid = rte->relid;
    MOT_LOG_TRACE("Seeing relid=%u relname=%s", (unsigned)rte->relid, rte->relname);

    MOT::TxnManager* currTxn = GetSafeTxn(__FUNCTION__);
    MOT_ASSERT(currTxn != nullptr);
    MOT::Table* table = currTxn->GetTableByExternalId(relid);
    if (table != nullptr) {
        MOT_LOG_DEBUG("Retrieved table %p by external id %u: %s", table, relid, table->GetLongTableName().c_str());
    } else {
        MOT_LOG_TRACE("Failed to find table by external relation id: %u", relid);
    }
    return table;
}

int MapTableColumnToIndex(MOT::Table* table, const MOT::Index* index, int columnId)
{
    const int16_t* indexColumnIds = index->GetColumnKeyFields();
    int keyColumnCount = index->GetNumFields();
    for (int i = 0; i < keyColumnCount; ++i) {
        if (indexColumnIds[i] == columnId) {
            MOT_LOG_TRACE("Table %s column %d mapped to index %s column %d",
                table->GetTableName().c_str(),
                columnId,
                index->GetName().c_str(),
                i);
            return i;
        }
    }
    return -1;
}

int BuildColumnMap(MOT::Table* table, MOT::Index* index, int** columnMap)
{
    int tableColumnCount = (int)table->GetFieldCount();
    *columnMap = (int*)MOT::MemSessionAlloc(((uint64_t)tableColumnCount) * sizeof(int));
    if (*columnMap) {
        for (int i = 0; i < tableColumnCount; ++i) {
            int indexColumnId = MapTableColumnToIndex(table, index, i);
            (*columnMap)[i] = indexColumnId;
        }
    } else {
        MOT_REPORT_ERROR(
            MOT_ERROR_OOM, "JIT Compilation", "Failed to allocate column map of size %d", tableColumnCount);
        tableColumnCount = -1;
    }
    return tableColumnCount;
}

int BuildIndexColumnOffsets(MOT::Table* table, const MOT::Index* index, int** offsets)
{
    const uint16_t* keyLength = index->GetLengthKeyFields();
    int indexColumnCount = index->GetNumFields();
    *offsets = (int*)MOT::MemSessionAlloc(((uint64_t)indexColumnCount) * sizeof(int));
    if (*offsets) {
        int offset = 0;
        for (int i = 0; i < indexColumnCount; ++i) {
            MOT_LOG_TRACE("Setting index %s column %d offset to %d", index->GetName().c_str(), i, offset);
            (*offsets)[i] = offset;
            offset += keyLength[i];
        }
    } else {
        MOT_REPORT_ERROR(
            MOT_ERROR_OOM, "JIT Compilation", "Failed to allocate index offset array of size %d", indexColumnCount);
        indexColumnCount = -1;
    }
    return indexColumnCount;
}

bool IsTypeSupported(int resultType)
{
    switch (resultType) {
        case BOOLOID:
        case CHAROID:
        case INT1OID:
        case INT2OID:
        case INT4OID:
        case INT8OID:
        case TIMESTAMPOID:
        case DATEOID:
        case FLOAT4OID:
        case FLOAT8OID:
        case NUMERICOID:
        case VARCHAROID:
        case BPCHAROID:
        case TEXTOID:
        case BYTEAOID:
#ifdef MOT_JIT_ADVANCED_WHERE_OP
        case TIMESTAMPTZOID:
        case INTERVALOID:
        case TINTERVALOID:
        case TIMEOID:
#endif
            return true;
        default:
            return false;
    }
}

bool IsStringType(int type)
{
    switch (type) {
        case VARCHAROID:
        case BPCHAROID:
        case TEXTOID:
        case BYTEAOID:
            return true;
        default:
            return false;
    }
}

bool IsPrimitiveType(int type)
{
    switch (type) {
        case BOOLOID:
        case CHAROID:
        case INT1OID:
        case INT2OID:
        case INT4OID:
        case INT8OID:
        case TIMEOID:
        case TIMESTAMPOID:
        case DATEOID:
        case FLOAT4OID:
        case FLOAT8OID:
            return true;

        default:
            return false;
    }
}

static bool IsEqualsWhereOperator(int whereOp)
{
    bool result = false;

    if ((whereOp == BooleanEqualOperator) || (whereOp == CHAREQOID) || (whereOp == INT1EQOID) ||
        (whereOp == INT2EQOID) || (whereOp == INT4EQOID) || (whereOp == INT8EQOID) || (whereOp == FLOAT4EQOID) ||
        (whereOp == FLOAT8EQOID) || (whereOp == NUMERICEQOID) || (whereOp == TIMESTAMPEQOID) ||
        (whereOp == DATEEQOID) || (whereOp == BPCHAREQOID) || (whereOp == TEXTEQOID)

#ifdef MOT_JIT_ADVANCED_WHERE_OP
        || (whereOp == INT24EQOID) || (whereOp == INT42EQOID) || (whereOp == INT84EQOID) || (whereOp == INT48EQOID) ||
        (whereOp == INT82EQOID) || (whereOp == INT28EQOID) || (whereOp == FLOAT48EQOID) || (whereOp == FLOAT84EQOID) ||
        (whereOp == 1108 /* time_eq */) || (whereOp == TIMETZEQOID) || (whereOp == 2347 /* date_eq_timestamp */) ||
        (whereOp == 2373 /* timestamp_eq_date */) || (whereOp == 2360 /* date_eq_timestamptz */) ||
        (whereOp == 2386 /* timestamptz_eq_date */) || (whereOp == 2536 /* timestamp_eq_timestamptz */) ||
        (whereOp == 2542 /* timestamptz_eq_timestamp */) || (whereOp == INTERVALEQOID)
#endif
    ) {
        result = true;
    }

    return result;
}

static bool IsLessThanWhereOperator(int whereOp)
{
    bool result = false;

    if ((whereOp == 631) /* charlt */ || (whereOp == 5515) /* int1lt */
        || (whereOp == INT2LTOID) || (whereOp == INT4LTOID) || (whereOp == INT8LTOID) || (whereOp == FLOAT4LTOID) ||
        (whereOp == FLOAT8LTOID) || (whereOp == NUMERICLTOID) || (whereOp == TIMESTAMPLTOID) ||
        (whereOp == DATELTOID) || (whereOp == BPCHARLTOID) || (whereOp == TEXTLTOID)

#ifdef MOT_JIT_ADVANCED_WHERE_OP
        || (whereOp == INT24LTOID) || (whereOp == INT42LTOID) || (whereOp == INT84LTOID) || (whereOp == INT48LTOID) ||
        (whereOp == INT82LTOID) || (whereOp == INT28LTOID) || (whereOp == FLOAT48LTOID) || (whereOp == FLOAT84LTOID) ||
        (whereOp == 1108 /* time_lt */) || (whereOp == 1552 /* timetz_lt */) ||
        (whereOp == 2347 /* date_lt_timestamp */) || (whereOp == 2373 /* timestamp_lt_date */) ||
        (whereOp == 2360 /* date_lt_timestamptz */) || (whereOp == 2386 /* timestamptz_lt_date */) ||
        (whereOp == 2536 /* timestamp_lt_timestamptz */) || (whereOp == 2542 /* timestamptz_lt_timestamp */) ||
        (whereOp == 1332 /* interval_lt */)
#endif
    ) {
        result = true;
    }

    return result;
}

static bool IsGreaterThanWhereOperator(int whereOp)
{
    bool result = false;

    if ((whereOp == 633) /* chargt */ || (whereOp == 5517) /* int1lt */
        || (whereOp == INT2GTOID) || (whereOp == INT4GTOID) || (whereOp == INT8GTOID) || (whereOp == FLOAT4GTOID) ||
        (whereOp == FLOAT8GTOID) || (whereOp == NUMERICGTOID) || (whereOp == TIMESTAMPGTOID) ||
        (whereOp == DATEGTOID) || (whereOp == BPCHARGTOID) || (whereOp == TEXTGTOID)

#ifdef MOT_JIT_ADVANCED_WHERE_OP
        || (whereOp == INT24GTOID) || (whereOp == INT42GTOID) || (whereOp == INT84GTOID) || (whereOp == INT48GTOID) ||
        (whereOp == INT82GTOID) || (whereOp == INT28GTOID) || (whereOp == FLOAT48GTOID) || (whereOp == FLOAT84GTOID) ||
        (whereOp == 1112 /* time_gt */) || (whereOp == 1554 /* timetz_gt */) ||
        (whereOp == 2349 /* date_gt_timestamp */) || (whereOp == 2375 /* timestamp_gt_date */) ||
        (whereOp == 2362 /* date_gt_timestamptz */) || (whereOp == 2388 /* timestamptz_gt_date */) ||
        (whereOp == 2538 /* timestamp_gt_timestamptz */) || (whereOp == 2544 /* timestamptz_gt_timestamp */) ||
        (whereOp == 1334 /* interval_gt */)
#endif
    ) {
        result = true;
    }

    return result;
}

static bool IsLessEqualsWhereOperator(int whereOp)
{
    bool result = false;

    if ((whereOp == 632) /* charle */ || (whereOp == 5516) /* int1le */
        || (whereOp == INT2LEOID) || (whereOp == INT4LEOID) || (whereOp == INT8LEOID) || (whereOp == FLOAT4LEOID) ||
        (whereOp == FLOAT8LEOID) || (whereOp == NUMERICLEOID) || (whereOp == TIMESTAMPLEOID) ||
        (whereOp == DATELEOID) || (whereOp == 1059 /* bpcharle */) || (whereOp == 665 /* text_le */)

#ifdef MOT_JIT_ADVANCED_WHERE_OP
        || (whereOp == INT24LEOID) || (whereOp == INT42LEOID) || (whereOp == INT84LEOID) || (whereOp == INT48LEOID) ||
        (whereOp == INT82LEOID) || (whereOp == INT28LEOID) || (whereOp == FLOAT48LEOID) || (whereOp == FLOAT84LEOID) ||
        (whereOp == 1111 /* time_le */) || (whereOp == 1553 /* timetz_le */) ||
        (whereOp == 2346 /* date_le_timestamp */) || (whereOp == 2372 /* timestamp_le_date */) ||
        (whereOp == 2359 /* date_le_timestamptz */) || (whereOp == 2385 /* timestamptz_le_date */) ||
        (whereOp == 2535 /* timestamp_le_timestamptz */) || (whereOp == 2541 /* timestamptz_le_timestamp */) ||
        (whereOp == 1333 /* interval_le */)
#endif
    ) {
        result = true;
    }

    return result;
}

static bool IsGreaterEqualsWhereOperator(int whereOp)
{
    bool result = false;

    if ((whereOp == 634) /* charge */ || (whereOp == 5518) /* int1ge */
        || (whereOp == INT2GEOID) || (whereOp == INT4GEOID) || (whereOp == INT8GEOID) || (whereOp == FLOAT4GEOID) ||
        (whereOp == FLOAT8GEOID) || (whereOp == NUMERICGEOID) || (whereOp == TIMESTAMPGEOID) ||
        (whereOp == DATEGEOID) || (whereOp == 1061 /* bpcharge */) || (whereOp == 667 /* text_ge */)

#ifdef MOT_JIT_ADVANCED_WHERE_OP
        || (whereOp == INT24GEOID) || (whereOp == INT42GEOID) || (whereOp == INT84GEOID) || (whereOp == INT48GEOID) ||
        (whereOp == INT82GEOID) || (whereOp == INT28GEOID) || (whereOp == FLOAT48GEOID) || (whereOp == FLOAT84GEOID) ||
        (whereOp == 1113 /* time_ge */) || (whereOp == 1555 /* timetz_ge */) ||
        (whereOp == 2348 /* date_ge_timestamp */) || (whereOp == 2374 /* timestamp_ge_date */) ||
        (whereOp == 2361 /* date_ge_timestamptz */) || (whereOp == 2387 /* timestamptz_ge_date */) ||
        (whereOp == 2537 /* timestamp_ge_timestamptz */) || (whereOp == 2543 /* timestamptz_ge_timestamp */) ||
        (whereOp == 1335 /* interval_ge */)
#endif
    ) {
        result = true;
    }

    return result;
}

bool IsWhereOperatorSupported(int whereOp)
{
    return ClassifyWhereOperator(whereOp) != JIT_WOC_INVALID;
}

JitWhereOperatorClass ClassifyWhereOperator(int whereOp)
{
    JitWhereOperatorClass result = JIT_WOC_INVALID;

    if (IsEqualsWhereOperator(whereOp)) {
        result = JIT_WOC_EQUALS;
    } else if (IsLessThanWhereOperator(whereOp)) {
        result = JIT_WOC_LESS_THAN;
    } else if (IsGreaterThanWhereOperator(whereOp)) {
        result = JIT_WOC_GREATER_THAN;
    } else if (IsLessEqualsWhereOperator(whereOp)) {
        result = JIT_WOC_LESS_EQUALS;
    } else if (IsGreaterEqualsWhereOperator(whereOp)) {
        result = JIT_WOC_GREATER_EQUALS;
    }

    return result;
}

bool IsFuncIdSupported(int funcId)
{
    return true;
}

bool IsFullPrefixSearch(const int* columnArray, int columnCount, int* firstZeroColumn)
{
    *firstZeroColumn = -1;
    for (int i = 0; i < columnCount; ++i) {
        if (columnArray[i] == 0) {
            if (*firstZeroColumn == -1) {
                *firstZeroColumn = i;
            }
        } else if (*firstZeroColumn != -1) {
            MOT_LOG_TRACE(
                "Invalid where clause: non-full prefix encountered (intermediate zero column at %d)", *firstZeroColumn);
            return false;
        }
    }
    return true;
}

JitQuerySortOrder ClassifyOperatorSortOrder(Oid op)
{
    JitQuerySortOrder result = JIT_QUERY_SORT_INVALID;

    MOT_LOG_TRACE("Checking sort order by operator %u", op);

    bool ascending = ((op == INT48LTOID) || (op == 58) /* boolean less than */ || (op == INT2LTOID) ||
                      (op == INT4LTOID) || (op == INT8LTOID) || (op == INT84LTOID) || (op == INT24LTOID) ||
                      (op == INT42LTOID) || (op == INT28LTOID) || (op == INT82LTOID) || (op == 5515) /* int1lt */ ||
                      (op == FLOAT4LTOID) || (op == FLOAT8LTOID) || (op == FLOAT48LTOID) || (op == FLOAT84LTOID) ||
                      (op == NUMERICLTOID) || (op == 631) /* char less than */ || (op == TEXTLTOID) ||
                      (op == BPCHARLTOID) || (op == 1957) /* bytealt */ || (op == DATELTOID) ||
                      (op == 1110) /* time_lt */ || (op == 1552) /* timetz_lt */ || (op == 1322) /* timestamptz_lt */ ||
                      (op == 1332) /* interval_lt */ || (op == TIMESTAMPLTOID) || (op == 5552) /* smalldatetime_lt */);

    bool descending = ((op == INT48GTOID) || (op == 59) /* boolgt */ || (op == INT2GTOID) || (op == INT4GTOID) ||
                       (op == INT8GTOID) || (op == INT84GTOID) || (op == INT24GTOID) || (op == INT42GTOID) ||
                       (op == INT28GTOID) || (op == INT82GTOID) || (op == 5517) /* int1gt */ || (op == FLOAT4GTOID) ||
                       (op == FLOAT8GTOID) || (op == FLOAT48GTOID) || (op == FLOAT84GTOID) || (op == NUMERICGTOID) ||
                       (op == 633) /* chargt */ || (op == TEXTGTOID) || (op == BPCHARGTOID) ||
                       (op == 1959) /* byteagt */ || (op == DATEGTOID) || (op == 1112) /* time_gt */ ||
                       (op == 1554) /* timetz_gt */ || (op == 1324) /* timestamptz_gt */ ||
                       (op == 1334) /* interval_gt */ || (op == TIMESTAMPGTOID) || (op == 5554) /* smalldatetime_gt */);

    if (ascending) {
        MOT_LOG_TRACE("Found ascending sort order by sortop %u", op);
        result = JIT_QUERY_SORT_ASCENDING;
    } else if (descending) {
        MOT_LOG_TRACE("Found descending sort order by sortop %u", op);
        result = JIT_QUERY_SORT_DESCENDING;
    } else {
        MOT_LOG_TRACE("Could not classify sort order by operator %u", op);
    }
    return result;
}

JitQuerySortOrder GetQuerySortOrder(const Query* query)
{
    JitQuerySortOrder result = JIT_QUERY_SORT_INVALID;

    if (!query->sortClause) {
        MOT_LOG_TRACE("Query has no sort clause, setting it by default to ascending");
        result = JIT_QUERY_SORT_ASCENDING;
    } else {
        SortGroupClause* sgc = (SortGroupClause*)linitial(query->sortClause);
        result = ClassifyOperatorSortOrder(sgc->sortop);
    }

    return result;
}

int GetAggregateTupleColumnIdAndType(const Query* query, int& zeroType)
{
    TargetEntry* targetEntry = (TargetEntry*)linitial(query->targetList);
    int tupleColId = targetEntry->resno - 1;
    Aggref* aggRef = (Aggref*)targetEntry->expr;
    zeroType = (int)aggRef->aggtype;

    return tupleColId;
}

/** @brief Initializes a table info struct. */
bool InitTableInfo(TableInfo* tableInfo, MOT::Table* table, MOT::Index* index)
{
    tableInfo->m_table = table;
    tableInfo->m_index = index;

    tableInfo->m_tableColumnCount = BuildColumnMap(table, index, &tableInfo->m_columnMap);
    if (tableInfo->m_columnMap == nullptr) {
        MOT_REPORT_ERROR(
            MOT_ERROR_OOM, "JIT Compile", "Failed to allocate table column map for code-generation context");
        return false;
    }
    tableInfo->m_indexColumnCount = BuildIndexColumnOffsets(table, index, &tableInfo->m_indexColumnOffsets);
    if (tableInfo->m_indexColumnOffsets == nullptr) {
        MOT::MemSessionFree(tableInfo->m_columnMap);
        tableInfo->m_columnMap = nullptr;
        return false;
    }

    return true;
}

/** @brief Releases all resources associated with a table info struct. */
void DestroyTableInfo(TableInfo* tableInfo)
{
    if (tableInfo != nullptr) {
        if (tableInfo->m_columnMap != nullptr) {
            MOT::MemSessionFree(tableInfo->m_columnMap);
            tableInfo->m_columnMap = nullptr;
        }
        if (tableInfo->m_indexColumnOffsets != nullptr) {
            MOT::MemSessionFree(tableInfo->m_indexColumnOffsets);
            tableInfo->m_indexColumnOffsets = nullptr;
        }
    }
}

bool PrepareSubQueryData(JitQueryContext* jitContext, JitCompoundPlan* plan)
{
    bool result = true;

    // allocate sub-query data array
    int subQueryCount = plan->_sub_query_count;
    MOT_LOG_TRACE("Preparing %u sub-query data items", subQueryCount);
    uint32_t allocSize = sizeof(JitSubQueryContext) * subQueryCount;
    jitContext->m_subQueryContext = (JitSubQueryContext*)MOT::MemGlobalAllocAligned(allocSize, L1_CACHE_LINE);
    if (jitContext->m_subQueryContext == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Generate JIT code",
            "Failed to allocate %u bytes for %d sub-query data items in JIT context object",
            allocSize,
            subQueryCount);
        result = false;
    } else {
        // traverse all search expressions and for each sub-link expression prepare a sub-query data item
        jitContext->m_subQueryCount = subQueryCount;
        for (int i = 0; i < plan->_outer_query_plan->_query._search_exprs._count; ++i) {
            if (plan->_outer_query_plan->_query._search_exprs._exprs[i]._expr->_expr_type == JIT_EXPR_TYPE_SUBLINK) {
                // get sub-query index from sub-link expression
                JitSubLinkExpr* subLinkExpr =
                    (JitSubLinkExpr*)plan->_outer_query_plan->_query._search_exprs._exprs[i]._expr;
                int subQueryIndex = subLinkExpr->_sub_query_index;
                MOT_ASSERT(subQueryIndex < subQueryCount);
                MOT_LOG_TRACE("Preparing sub-query %u data", subQueryIndex);
                JitSubQueryContext* subQueryContext = &jitContext->m_subQueryContext[subQueryIndex];

                // prepare sub-query data items according to sub-plan type
                JitPlan* subPlan = plan->_sub_query_plans[subQueryIndex];
                JitPlanType subPlanType = subPlan->_plan_type;
                MOT_ASSERT((subPlanType == JIT_PLAN_POINT_QUERY) || (subPlanType == JIT_PLAN_RANGE_SCAN));

                if (subPlanType == JIT_PLAN_POINT_QUERY) {  // simple point-select
                    MOT_ASSERT(((JitPointQueryPlan*)subPlan)->_command_type == JIT_COMMAND_SELECT);
                    JitPointQueryPlan* subPointQueryPlan = (JitPointQueryPlan*)subPlan;
                    if (subPointQueryPlan->_command_type == JIT_COMMAND_SELECT) {
                        MOT_LOG_TRACE("Detected point-query select sub-plan");
                        subQueryContext->m_commandType = JIT_COMMAND_SELECT;
                        JitSelectPlan* selectPlan = (JitSelectPlan*)subPlan;
                        subQueryContext->m_table = selectPlan->_query._table;
                        subQueryContext->m_tableId = subQueryContext->m_table->GetTableExId();
                        MOT_LOG_TRACE("Installed sub-query %d table id: %" PRIu64, i, subQueryContext->m_tableId);
                        subQueryContext->m_index = subQueryContext->m_table->GetPrimaryIndex();
                        subQueryContext->m_indexId = subQueryContext->m_index->GetExtId();
                        MOT_LOG_TRACE("Installed sub-query %d index id: %" PRIu64, i, subQueryContext->m_indexId);
                    } else {
                        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                            "Generate JIT code",
                            "Invalid sub-point-query plan command type: %s",
                            CommandToString(subPointQueryPlan->_command_type));
                        result = false;
                        break;
                    }
                } else if (subPlanType == JIT_PLAN_RANGE_SCAN) {  // aggregate or "limit 1" range select
                    MOT_ASSERT(((JitRangeScanPlan*)subPlan)->_command_type == JIT_COMMAND_SELECT);
                    JitRangeScanPlan* subRangeScanPlan = (JitRangeScanPlan*)subPlan;
                    if (subRangeScanPlan->_command_type == JIT_COMMAND_SELECT) {
                        MOT_LOG_TRACE("Detected range-scan select sub-plan");
                        JitRangeSelectPlan* rangeSelectPlan = (JitRangeSelectPlan*)subPlan;
                        subQueryContext->m_table = rangeSelectPlan->_index_scan._table;
                        subQueryContext->m_index = rangeSelectPlan->_index_scan._index;
                        if (rangeSelectPlan->m_aggCount > 0) {  // aggregate range-scan
                            subQueryContext->m_commandType = JIT_COMMAND_AGGREGATE_RANGE_SELECT;
                        } else if (rangeSelectPlan->_limit_count == 1) {
                            subQueryContext->m_commandType = JIT_COMMAND_RANGE_SELECT;  // implies "limit 1" clause
                        } else {
                            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                                "Generate JIT code",
                                "Invalid sub-query range-scan plan: neither aggregate nor \"limit 1\" clause found");
                            result = false;
                            break;
                        }
                    } else {
                        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                            "Generate JIT code",
                            "Invalid sub-query range-scan plan command type: %s",
                            CommandToString(subRangeScanPlan->_command_type));
                        result = false;
                        break;
                    }
                } else {
                    MOT_REPORT_ERROR(
                        MOT_ERROR_INTERNAL, "Generate JIT code", "Invalid sub-query plan type: %d", (int)subPlanType);
                    result = false;
                    break;
                }
            }
        }
    }

    return result;
}

const char* ExtractFunctionName(Node* parseTree)
{
    const char* funcName = nullptr;

    // function invocation is in the form: "SELECT <function_name>(args)"
    if (parseTree->type != T_SelectStmt) {
        MOT_LOG_TRACE("Cannot extract function name from parse tree: not a SELECT statement");
    } else {
        SelectStmt* selectStmt = (SelectStmt*)parseTree;
        // now check target list
        if (list_length(selectStmt->targetList) != 1) {
            MOT_LOG_TRACE(
                "Cannot extract function name from parse tree: target list does not contain exactly one item");
        } else {
            ResTarget* resTarget = (ResTarget*)linitial(selectStmt->targetList);
            if (resTarget->indirection) {
                MOT_LOG_TRACE("Cannot extract function name from parse tree: result target contains indirection");
            } else {
                if (resTarget->val->type != T_FuncCall) {
                    MOT_LOG_TRACE("Cannot extract function name from parse tree: result target value type is not a "
                                  "function call");
                } else {
                    FuncCall* funcCall = (FuncCall*)resTarget->val;
                    Value* value = (Value*)linitial(funcCall->funcname);
                    if (value->type != T_String) {
                        MOT_LOG_TRACE("Cannot extract function name from parse tree: function call name of result "
                                      "target does not having a string value");
                    } else {
                        funcName = value->val.str;
                    }
                }
            }
        }
    }

    return funcName;
}

static bool CloneStringDatum(Datum source, Datum* target, JitContextUsage usage)
{
    bytea* value = DatumGetByteaP(source);
    size_t len = VARSIZE(value);  // includes header len VARHDRSZ
    char* src = VARDATA(value);

    // special case: empty string
    if (len == 0) {
        len = VARHDRSZ;
    }
    size_t strSize = len - VARHDRSZ;
    MOT_LOG_TRACE("CloneStringDatum(): len = %u, src = %*.*s", (unsigned)len, strSize, strSize, src);

    bytea* copy = (bytea*)JitMemAlloc(len, usage);
    if (copy == nullptr) {
        MOT_REPORT_ERROR(
            MOT_ERROR_OOM, "JIT Compile", "Failed to allocate %u bytes for datum string constant", (unsigned)len);
        return false;
    }

    if (strSize > 0) {
        errno_t erc = memcpy_s(VARDATA(copy), strSize, (uint8_t*)src, strSize);
        securec_check(erc, "\0", "\0");
    }

    SET_VARSIZE(copy, len);

    *target = PointerGetDatum(copy);
    return true;
}

static bool CloneTimeTzDatum(Datum source, Datum* target, JitContextUsage usage)
{
    MOT::TimetzSt* value = (MOT::TimetzSt*)DatumGetPointer(source);
    size_t allocSize = sizeof(MOT::TimetzSt);
    MOT::TimetzSt* copy = (MOT::TimetzSt*)JitMemAlloc(allocSize, usage);
    if (copy == nullptr) {
        MOT_REPORT_ERROR(
            MOT_ERROR_OOM, "JIT Compile", "Failed to allocate %u bytes for datum TimeTZ constant", (unsigned)allocSize);
        return false;
    }
    copy->m_time = value->m_time;
    copy->m_zone = value->m_zone;

    *target = PointerGetDatum(copy);
    return true;
}

static bool CloneIntervalDatum(Datum source, Datum* target, JitContextUsage usage)
{
    MOT::IntervalSt* value = (MOT::IntervalSt*)DatumGetPointer(source);
    size_t allocSize = sizeof(MOT::IntervalSt);
    MOT::IntervalSt* copy = (MOT::IntervalSt*)JitMemAlloc(allocSize, usage);
    if (copy == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "JIT Compile",
            "Failed to allocate %u bytes for datum Interval constant",
            (unsigned)allocSize);
        return false;
    }
    copy->m_day = value->m_day;
    copy->m_month = value->m_month;
    copy->m_time = value->m_time;

    *target = PointerGetDatum(copy);
    return true;
}

static bool CloneTIntervalDatum(Datum source, Datum* target, JitContextUsage usage)
{
    MOT::TintervalSt* value = (MOT::TintervalSt*)DatumGetPointer(source);
    size_t allocSize = sizeof(MOT::TintervalSt);
    MOT::TintervalSt* copy = (MOT::TintervalSt*)JitMemAlloc(allocSize, usage);
    if (copy == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "JIT Compile",
            "Failed to allocate %u bytes for datum TInterval constant",
            (unsigned)allocSize);
        return false;
    }
    copy->m_status = value->m_status;
    copy->m_data[0] = value->m_data[0];
    copy->m_data[1] = value->m_data[1];

    *target = PointerGetDatum(copy);
    return true;
}

static bool CloneNumericDatum(Datum source, Datum* target, JitContextUsage usage)
{
    varlena* var = (varlena*)DatumGetPointer(source);
    Size len = VARSIZE(var);
    struct varlena* result = (varlena*)JitMemAlloc(len, usage);
    if (result == nullptr) {
        MOT_REPORT_ERROR(
            MOT_ERROR_OOM, "JIT Compile", "Failed to allocate %u bytes for datum Numeric constant", (unsigned)len);
        return false;
    }

    errno_t rc = memcpy_s(result, len, var, len);
    securec_check(rc, "\0", "\0");

    *target = NumericGetDatum((Numeric)result);
    return true;
}

static bool CloneCStringDatum(Datum source, Datum* target, JitContextUsage usage)
{
    char* src = DatumGetCString(source);
    size_t len = strlen(src) + 1;  // includes terminating null

    char* copy = (char*)JitMemAlloc(len, usage);
    if (copy == nullptr) {
        MOT_REPORT_ERROR(
            MOT_ERROR_OOM, "JIT Compile", "Failed to allocate %u bytes for datum string constant", (unsigned)len);
        return false;
    }

    errno_t erc = memcpy_s(copy, len, src, len);
    securec_check(erc, "\0", "\0");

    *target = PointerGetDatum(copy);
    return true;
}

bool CloneDatum(Datum source, int type, Datum* target, JitContextUsage usage)
{
    bool result = true;
    if (IsStringType(type)) {
        result = CloneStringDatum(source, target, usage);
    } else {
        switch (type) {
            case TIMETZOID:
                result = CloneTimeTzDatum(source, target, usage);
                break;

            case INTERVALOID:
                result = CloneIntervalDatum(source, target, usage);
                break;

            case TINTERVALOID:
                result = CloneTIntervalDatum(source, target, usage);
                break;

            case NUMERICOID:
                result = CloneNumericDatum(source, target, usage);
                break;

            case UNKNOWNOID:
                result = CloneCStringDatum(source, target, usage);
                break;

            default:
                MOT_LOG_TRACE("Unsupported non-primitive constant type: %d", type);
                result = false;
                break;
        }
    }

    return result;
}

bool PrepareDatumArray(Const* constArray, uint32_t constCount, JitDatumArray* datumArray)
{
    size_t allocSize = sizeof(JitDatum) * constCount;
    JitDatum* datums = (JitDatum*)MOT::MemGlobalAlloc(allocSize);
    if (datums == nullptr) {
        MOT_REPORT_ERROR(
            MOT_ERROR_OOM, "JIT Compile", "Failed to allocate %u bytes for constant datum array", (unsigned)allocSize);
        return false;
    }

    for (uint32_t i = 0; i < constCount; ++i) {
        Const* constValue = &constArray[i];
        datums[i].m_isNull = constValue->constisnull;
        datums[i].m_type = constValue->consttype;
        if (!datums[i].m_isNull) {
            if (IsPrimitiveType(constValue->constvalue)) {
                datums[i].m_datum = constValue->constvalue;
            } else {
                if (!CloneDatum(
                        constValue->constvalue, constValue->consttype, &datums[i].m_datum, JIT_CONTEXT_GLOBAL)) {
                    MOT_LOG_TRACE("Failed to prepare datum value");
                    for (uint32_t j = 0; j < i; ++j) {
                        if (!datums[j].m_isNull && !IsPrimitiveType(datums[j].m_type)) {
                            MOT::MemGlobalFree(DatumGetPointer(datums[j].m_datum));
                        }
                    }
                    MOT::MemGlobalFree(datums);
                    return false;
                }
            }
        }
    }

    datumArray->m_datumCount = constCount;
    datumArray->m_datums = datums;
    return true;
}

static void PrintNumeric(MOT::LogLevel logLevel, Datum value)
{
    Datum result = DirectFunctionCall1(numeric_out, value);
    char* cstring = DatumGetCString(result);
    MOT_LOG_APPEND(MOT::LogLevel::LL_TRACE, "[numeric] %s", cstring);
}

static void PrintVarchar(MOT::LogLevel logLevel, Datum value)
{
    bytea* txt = DatumGetByteaP(value);
    size_t size = VARSIZE(txt);  // includes header len VARHDRSZ
    char* src = VARDATA(txt);
    size_t strSize = size - VARHDRSZ;
    MOT_LOG_APPEND(logLevel, "[text %u] %.*s", (unsigned)strSize, (int)strSize, src);
}

void PrintDatum(MOT::LogLevel logLevel, Oid ptype, Datum datum, bool isnull)
{
    if (isnull) {
        MOT_LOG_APPEND(logLevel, "[type %u] NULL", ptype);
    } else if (ptype == NUMERICOID) {
        PrintNumeric(logLevel, datum);
    } else if ((ptype == VARCHAROID) || (ptype == BPCHAROID) || (ptype == TEXTOID)) {
        PrintVarchar(logLevel, datum);
    } else {
        switch (ptype) {
            case BOOLOID:
                MOT_LOG_APPEND(logLevel, "[bool] %s", (unsigned)DatumGetBool(datum) ? "true" : "false");
                break;

            case CHAROID:
                MOT_LOG_APPEND(logLevel, "[char] %c", (char)DatumGetChar(datum));
                break;

            case INT1OID:
                MOT_LOG_APPEND(logLevel, "[int1] %u", (unsigned)DatumGetUInt8(datum));
                break;

            case INT2OID:
                MOT_LOG_APPEND(logLevel, "[int2] %d", (int)DatumGetInt16(datum));
                break;

            case INT4OID:
                MOT_LOG_APPEND(logLevel, "[int4] %d", (int)DatumGetInt32(datum));
                break;

            case INT8OID:
                MOT_LOG_APPEND(logLevel, "[int8] %" PRId64, (int64_t)DatumGetInt64(datum));
                break;

            case TIMESTAMPOID:
                MOT_LOG_APPEND(logLevel, "[timestamp] %" PRIu64, (uint64_t)DatumGetTimestamp(datum));
                break;

            case TIMESTAMPTZOID:
                MOT_LOG_APPEND(logLevel, "[timestamptz] %" PRId64, (int64_t)DatumGetTimestampTz(datum));
                break;

            case FLOAT4OID:
                MOT_LOG_APPEND(logLevel, "[float4] %f", (double)DatumGetFloat4(datum));
                break;

            case FLOAT8OID:
                MOT_LOG_APPEND(logLevel, "[float8] %f", (double)DatumGetFloat8(datum));
                break;

#ifdef MOT_JIT_ADVANCED_WHERE_OP
            case TIMETZOID: {
                MOT::TimetzSt* timetzSt = (MOT::TimetzSt*)DatumGetPointer(datum);
                MOT_LOG_APPEND(logLevel, "[timetz] time=%" PRIu64 ", zone=%u", timetzSt->m_time, timetzSt->m_zone);
                break;
            }

            case INTERVALOID: {
                MOT::IntervalSt* intervalSt = (MOT::IntervalSt*)DatumGetPointer(datum);
                MOT_LOG_APPEND(logLevel,
                    "[interval] time=%" PRIu64 ", days=%u, months=%u",
                    intervalSt->m_time,
                    intervalSt->m_day,
                    intervalSt->m_month);
                break;
            }

            case TINTERVALOID: {
                MOT::TintervalSt* tintervalSt = (MOT::TintervalSt*)DatumGetPointer(datum);
                MOT_LOG_APPEND(logLevel,
                    "[tinterval] status=%u, data[0]=%u, data[1]=%u",
                    tintervalSt->m_status,
                    tintervalSt->m_data[0],
                    tintervalSt->m_data[1]);
                break;
            }
#endif

            default:
                MOT_LOG_APPEND(logLevel, "[type %u] %" PRIu64, ptype, (uint64_t)datum);
                break;
        }
    }
}

double NumericToDouble(Datum numeric_value)
{
    return (double)DatumGetFloat8(DirectFunctionCall1(numeric_float8, numeric_value));
}

void PrepareExecState(PLpgSQL_execstate* estate, PLpgSQL_function* function)
{
    // plpgsql_estate_setup() does way too much, we just need the function pointer and the datum array
    estate->func = function;
    estate->ndatums = function->ndatums;
    estate->datums = function->datums;
}

bool GetExprQueryAttrs(PLpgSQL_expr* expr, PLpgSQL_function* function, ExprQueryAttrs* attrs)
{
    char* queryString = expr->query;
    MOT_LOG_TRACE("Retrieving query attributes: %s", queryString);

    expr->func = function;
    // Set pre-parse trigger to true. This is required for make_datum_param during non-jittable query cached plan
    // revalidation, otherwise we need to setup estate datums (or dump core)
    volatile bool preParseTrigger = expr->func->pre_parse_trig;
    expr->func->pre_parse_trig = true;

    // NOTE: every variable used after catch needs to be volatile (see longjmp() man page)
    u_sess->mot_cxt.jit_parse_error = 0;
    u_sess->mot_cxt.jit_codegen_error = 0;
    volatile bool result = false;
    volatile SPIPlanPtr spiPlan = nullptr;
    // in order to avoid life-cycle management, we use the session-level context
    volatile MemoryContext oldCtx = MemoryContextSwitchTo(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR));
    PG_TRY();
    {
        spiPlan = SPI_prepare_params(queryString, (ParserSetupHook)plpgsql_parser_setup, (void*)expr, 0);
        if (spiPlan == nullptr) {
            if (u_sess->mot_cxt.jit_parse_error == MOT_JIT_TABLE_NOT_FOUND) {
                MOT_LOG_TRACE("Failed to parse query, table not found: %s", queryString);
            } else if (u_sess->mot_cxt.jit_parse_error == MOT_JIT_GENERIC_PARSE_ERROR) {
                MOT_LOG_TRACE("Failed to parse query, unknown error: %s", queryString);
            } else {
                MOT_LOG_TRACE("Failed to prepare parameters for expression, SPI error: %s (%d)",
                    SPI_result_code_string(SPI_result),
                    (int)SPI_result);
            }
        } else {
            // get single plan source from plan
            List* planList = SPI_plan_get_plan_sources(spiPlan);
            if (list_length(planList) != 1) {
                MOT_LOG_TRACE("Unexpected plan list length %d, disqualifying function", list_length(planList));
                (void)SPI_freeplan(spiPlan);
                spiPlan = nullptr;
            } else {
                CachedPlanSource* planSource = (CachedPlanSource*)linitial(planList);

                // get single query from plan source
                if (list_length(planSource->query_list) != 1) {
                    (void)SPI_freeplan(spiPlan);
                    spiPlan = nullptr;
                    MOT_LOG_TRACE("Unexpected query list length, disqualifying function");
                } else {
                    Query* query = (Query*)linitial(planSource->query_list);

                    attrs->m_spiPlan = spiPlan;
                    attrs->m_planSource = planSource;
                    attrs->m_query = query;
                    if (expr->paramnos != nullptr) {
                        attrs->m_paramNos = bms_copy(expr->paramnos);
                    } else {
                        attrs->m_paramNos = nullptr;
                    }
                    result = true;
                }
            }
        }
    }
    PG_CATCH();
    {
        (void)MemoryContextSwitchTo(oldCtx);
        ErrorData* edata = CopyErrorData();
        MOT_LOG_WARN("Caught exception while retrieving query attributes: %s", edata->message);
        ereport(WARNING,
            (errmodule(MOD_MOT),
                errmsg("Caught exception while retrieving query attributes: %s", edata->message),
                errdetail("%s", edata->detail)));
        u_sess->mot_cxt.jit_codegen_error = edata->sqlerrcode;
        FlushErrorState();
        FreeErrorData(edata);
    }
    PG_END_TRY();
    (void)MemoryContextSwitchTo(oldCtx);

    if (!result) {
        if (spiPlan != nullptr) {
            (void)SPI_freeplan(spiPlan);
        }
    }
    expr->func->pre_parse_trig = preParseTrigger;
    return result;
}

void CleanupExprQueryAttrs(ExprQueryAttrs* attrs)
{
    if (attrs->m_spiPlan != nullptr) {
        (void)SPI_freeplan(attrs->m_spiPlan);
        attrs->m_spiPlan = nullptr;
    }
    if (attrs->m_paramNos != nullptr) {
        bms_free(attrs->m_paramNos);
        attrs->m_paramNos = nullptr;
    }
}

ParamListInfo CreateParamListInfo(int paramCount, bool isGlobalUsage)
{
    size_t allocSize = sizeof(ParamListInfoData) + sizeof(ParamExternData) * paramCount;
    ParamListInfo result = nullptr;
    if (isGlobalUsage) {
        result = (ParamListInfo)MOT::MemGlobalAlloc(allocSize);
    } else {
        result = (ParamListInfo)MOT::MemSessionAlloc(allocSize);
    }
    if (result != nullptr) {
        errno_t erc = memset_s(result, allocSize, 0, allocSize);
        securec_check(erc, "\0", "\0");
        result->numParams = paramCount;
    }
    return result;
}

static inline const char* FormatJitTime(char* buffer, size_t len)
{
    // get current time
    struct tm* tmInfo;
    struct timeval tv;
    struct tm localTime;
    (void)gettimeofday(&tv, NULL);

    long int millisec = lrint(tv.tv_usec / 1000.0);  // Round to nearest millisec
    if (millisec >= 1000) {                          // Allow for rounding up to nearest second
        millisec -= 1000;
        tv.tv_sec++;
    }

    tmInfo = localtime_r(&tv.tv_sec, &localTime);

    // format time
    size_t offset = strftime(buffer, len, "%Y-%m-%d %H:%M:%S", tmInfo);
    errno_t erc = snprintf_s(buffer + offset, len - offset, len - offset - 1, ".%03d", (int)millisec);
    securec_check_ss(erc, "\0", "\0");
    return buffer;
}

inline PLpgSQL_function* CompileFunction(Oid functionOid)
{
    // fill-in the minimum required for compiling
    FmgrInfo flinfo = {};
    errno_t errorno = memset_s(&flinfo, sizeof(flinfo), 0, sizeof(flinfo));
    securec_check(errorno, "", "");

    flinfo.fn_oid = functionOid;
    flinfo.fn_mcxt = CurrentMemoryContext;

    Datum args[1];
    FunctionCallInfoData fakeInfo = {};
    errorno = memset_s(&fakeInfo, sizeof(fakeInfo), 0, sizeof(fakeInfo));
    securec_check(errorno, "", "");

    fakeInfo.fncollation = DEFAULT_COLLATION_OID;
    fakeInfo.flinfo = &flinfo;
    fakeInfo.arg = args;
    fakeInfo.arg[0] = Int32GetDatum(functionOid);

    // must call PG initialization for current session first
    _PG_init();
    return plpgsql_compile(&fakeInfo, false);
}

static PLpgSQL_function* TriggerFunctionCompilation(const char* functionName, Oid functionOid)
{
    // NOTE: every variable used after catch needs to be volatile (see longjmp() man page)
    volatile PLpgSQL_function* result = nullptr;
    volatile HeapTuple procTuple = nullptr;
    volatile MemoryContext origCxt = CurrentMemoryContext;

    PG_TRY();
    {
        procTuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(functionOid));
        if (procTuple == nullptr) {
            MOT_LOG_TRACE("Cannot find function by id: %u", (unsigned)functionOid);
        } else {
            Form_pg_proc procStruct = (Form_pg_proc)GETSTRUCT(procTuple);
            MOT_LOG_TRACE("Function %u language id: %u", (unsigned)functionOid, (unsigned)procStruct->prolang);
            if (procStruct->prolang != get_language_oid("plpgsql", false)) {
                MOT_LOG_TRACE("Skipping trigger compilation for function %u: not PLpgSQL", (unsigned)functionOid);
            } else {
                MOT_LOG_TRACE("Triggering validation of function %s by id %u", functionName, (unsigned)functionOid);
                result = CompileFunction(functionOid);
            }
            ReleaseSysCache(procTuple);
            procTuple = nullptr;
        }
    }
    PG_CATCH();
    {
        // switch back to original context before issuing an error report
        (void)MemoryContextSwitchTo(origCxt);
        ErrorData* edata = CopyErrorData();
        MOT_LOG_WARN("Caught exception while triggering compilation for function %s with id %u: %s",
            functionName,
            functionOid,
            edata->message);
        ereport(WARNING,
            (errmsg("Failed to trigger compilation for function '%s' for MOT jitted execution.", functionName),
                errdetail("%s", edata->detail)));
        FlushErrorState();
        FreeErrorData(edata);

        // even though resource owner will take care of this, we want to clean up early
        if (procTuple != nullptr) {
            ReleaseSysCache(procTuple);
        }
    }
    PG_END_TRY();
    return (PLpgSQL_function*)result;
}

PLpgSQL_function* GetPGCompiledFunction(Oid functionOid, const char* funcName /*= nullptr */)
{
    // just trigger compilation for the function and collect the result.
    MOT_LOG_TRACE("Retrieving compiled function %u", functionOid);
    if (funcName == nullptr) {
        funcName = get_func_name(functionOid);
    }
    if (funcName == nullptr) {
        MOT_LOG_TRACE("GetPGCompiledFunction(): Cannot find function with id %u", (unsigned)functionOid);
        return nullptr;
    }
    PLpgSQL_function* func = TriggerFunctionCompilation(funcName, functionOid);
    if (func == nullptr) {
        MOT_LOG_TRACE("Failed to trigger function %u compilation", functionOid);
    } else {
        MOT_LOG_TRACE("Collected function %p by id %u with txn id %" PRIu64, func, functionOid, func->fn_xmin);
    }
    return func;
}

void SqlStateToCode(int sqlState, char* sqlStateCode)
{
    for (int i = 0; i < 5; ++i) {
        sqlStateCode[i] = PGUNSIXBIT(sqlState);
        sqlState = sqlState >> 6;
    }
    sqlStateCode[5] = 0;
}

void RaiseEreport(int sqlState, const char* errorMessage, const char* errorDetail, const char* errorHint)
{
    if (errorDetail && errorHint) {
        ereport(ERROR,
            (errmodule(MOD_MOT),
                errcode(sqlState),
                errmsg("%s", errorMessage),
                errdetail("%s", errorDetail),
                errhint("%s", errorHint)));
    } else if (errorDetail) {
        ereport(
            ERROR, (errmodule(MOD_MOT), errcode(sqlState), errmsg("%s", errorMessage), errdetail("%s", errorDetail)));
    } else {
        ereport(ERROR, (errmodule(MOD_MOT), errcode(sqlState), errmsg("%s", errorMessage)));
    }
}

char* DupString(const char* source, JitContextUsage usage)
{
    size_t len = strlen(source);
    char* result = (char*)JitMemAlloc(len + 1, usage);
    if (result != nullptr) {
        errno_t erc = strcpy_s(result, len + 1, source);
        securec_check(erc, "\0", "\0");
    }
    return result;
}

bool SPIAutoConnect::Connect(Oid functionOid /* = InvalidOid */)
{
    // NOTE: every variable used after catch needs to be volatile (see longjmp() man page)
    volatile MemoryContext origCtx = CurrentMemoryContext;
    PG_TRY();
    {
        if (!m_connected) {
            knl_u_SPI_context* spi_cxt = &u_sess->SPI_cxt;
            MOT_LOG_TRACE("Connecting to SPI: cur_id=%d, connected=%d", spi_cxt->_curid, spi_cxt->_connected);
            if (functionOid == InvalidOid) {
                m_rc = SPI_connect();
            } else {
                m_rc = SPI_connect_ext(DestSPI, nullptr, nullptr, SPI_OPT_NONATOMIC, functionOid);
            }
            if ((m_rc == SPI_OK_CONNECT) || (m_rc == SPI_ERROR_CONNECT)) {
                m_connected = true;
                if (m_rc == SPI_OK_CONNECT) {
                    m_shouldDisconnect = true;
                }
            }
        }
        m_connectId = SPI_connectid();
    }
    PG_CATCH();
    {
        (void)MemoryContextSwitchTo(origCtx);
        HandleError(ErrorOp::CONNECT);
    }
    PG_END_TRY();
    return m_connected;
}

void SPIAutoConnect::Disconnect()
{
    // NOTE: every variable used after catch needs to be volatile (see longjmp() man page)
    volatile MemoryContext origCtx = CurrentMemoryContext;
    PG_TRY();
    {
        if (m_connected && m_shouldDisconnect) {
            // it is possible that some error occurred and we are in intermediate state, so we first detect that, then
            // cleanup previous connections, then cleanup our connection
            knl_u_SPI_context* spi_cxt = &u_sess->SPI_cxt;
            MOT_LOG_TRACE("Disconnecting from SPI: cur_id=%d, connected=%d", spi_cxt->_curid, spi_cxt->_connected);
            if ((spi_cxt->_curid + 1) != spi_cxt->_connected) {  // already disconnected, or invalid state
                MOT_LOG_TRACE("SPI left in intermediate state");
                // first restore connection state
                if (m_connectId >= 0) {
                    // cleanup leftover connections
                    MOT_LOG_TRACE("Cleaning up leftover connections up to %d", m_connectId + 1);
                    SPI_disconnect(m_connectId + 1);
                    // restore connection state
                    MOT_LOG_TRACE("Restoring connection state");
                    SPI_restore_connection();
                    MOT_LOG_TRACE(
                        "SPI connection state: cur_id=%d, connected=%d", spi_cxt->_curid, spi_cxt->_connected);
                }
            }
            int rc = SPI_finish();
            if (rc == SPI_ERROR_UNCONNECTED) {
                MOT_LOG_TRACE("Failed to disconnect from SPI - already disconnected");
            } else if (rc != SPI_OK_FINISH) {
                MOT_REPORT_ERROR(MOT_ERROR_INVALID_STATE,
                    "JIT Compile",
                    "Failed to disconnect from SPI - %s (error code: %d)",
                    SPI_result_code_string(rc),
                    rc);
            }
        }
    }
    PG_CATCH();
    {
        (void)MemoryContextSwitchTo(origCtx);
        HandleError(ErrorOp::DISCONNECT);
    }
    PG_END_TRY();
    m_connected = false;
    m_shouldDisconnect = false;
}

void SPIAutoConnect::HandleError(ErrorOp errorOp)
{
    const char* operation = ErrorOpToString(errorOp);
    ErrorData* edata = CopyErrorData();
    MOT_LOG_WARN("Caught exception while %s: %s", operation, edata->message);
    ereport(WARNING,
        (errmodule(MOD_MOT),
            errmsg("Caught exception while %s: %s", operation, edata->message),
            errdetail("%s", edata->detail)));
    FlushErrorState();
    FreeErrorData(edata);
}

const char* SPIAutoConnect::ErrorOpToString(ErrorOp errorOp)
{
    switch (errorOp) {
        case ErrorOp::CONNECT:
            return "connecting to SPI";

        case ErrorOp::DISCONNECT:
            return "disconnecting from SPI";

        default:
            return "<Unknown SPI operation>";
    }
}

PGFunction GetPGFunctionInfo(Oid functionId, uint32_t* argCount, bool* isStrict /* = nullptr */)
{
    FmgrInfo flinfo;
    fmgr_info(functionId, &flinfo);
    MOT_LOG_TRACE("Retrieved function %u info: fn_addr=%p, fn_nargs=%d, fn_strict=%d, fn_retset=%d, fn_rettype=%u, "
                  "fn_rettypemod=%u, fnName=%s",
        functionId,
        flinfo.fn_addr,
        (int)flinfo.fn_nargs,
        (int)flinfo.fn_strict,
        (int)flinfo.fn_retset,
        flinfo.fn_rettype,
        flinfo.fn_rettypemod,
        flinfo.fnName);
    *argCount = flinfo.fn_nargs;
    if (isStrict != nullptr) {
        *isStrict = flinfo.fn_strict;
    }
    return flinfo.fn_addr;
}

PGFunction GetPGAggFunctionInfo(Oid functionId, uint32_t* argCount, bool* isStrict /* = nullptr */)
{
    MOT_LOG_TRACE("Searching for aggregation function %u", (unsigned)functionId);
    HeapTuple aggTuple = SearchSysCache1(AGGFNOID, ObjectIdGetDatum(functionId));
    if (!HeapTupleIsValid(aggTuple)) {
        MOT_LOG_TRACE("Failed to lookup aggregation function %u", (unsigned)functionId);
        return nullptr;
    }

    Form_pg_aggregate aggStruct = (Form_pg_aggregate)GETSTRUCT(aggTuple);
    MOT_LOG_TRACE("Redirected to aggregation function %u (transfn: %u, finalfn: %u, directargs: %d)",
        (unsigned)aggStruct->aggfnoid,
        (unsigned)aggStruct->aggtransfn,
        (unsigned)aggStruct->aggfinalfn,
        aggStruct->aggnumdirectargs);
    // we actually use the transition function for aggregation
    PGFunction funcPtr = GetPGFunctionInfo(aggStruct->aggtransfn, argCount, isStrict);
    ReleaseSysCache(aggTuple);
    return funcPtr;
}

bool GetPGAggTransFunctionInfo(Oid functionId, PGFunction* aggFunc, uint32_t* argCount, bool* isStrict /* = nullptr */)
{
    *aggFunc = nullptr;
    MOT_LOG_TRACE("Searching for transition of aggregation function %u", (unsigned)functionId);
    HeapTuple aggTuple = SearchSysCache1(AGGFNOID, ObjectIdGetDatum(functionId));
    if (!HeapTupleIsValid(aggTuple)) {
        MOT_LOG_TRACE("Failed to lookup aggregation function %u", (unsigned)functionId);
        return false;
    }

    Form_pg_aggregate aggStruct = (Form_pg_aggregate)GETSTRUCT(aggTuple);
    MOT_LOG_TRACE("Redirected to aggregation function %u (transfn: %u, finalfn: %u, directargs: %d)",
        (unsigned)aggStruct->aggfnoid,
        (unsigned)aggStruct->aggtransfn,
        (unsigned)aggStruct->aggfinalfn,
        aggStruct->aggnumdirectargs);
    if (aggStruct->aggfinalfn != 0) {
        *aggFunc = GetPGFunctionInfo(aggStruct->aggfinalfn, argCount, isStrict);
    }
    ReleaseSysCache(aggTuple);
    return true;
}

bool GetPGAggFinalFunctionInfo(Oid functionId, PGFunction* aggFunc, uint32_t* argCount, bool* isStrict /* = nullptr */)
{
    *aggFunc = nullptr;
    MOT_LOG_TRACE("Searching for finalization of aggregation function %u", (unsigned)functionId);
    HeapTuple aggTuple = SearchSysCache1(AGGFNOID, ObjectIdGetDatum(functionId));
    if (!HeapTupleIsValid(aggTuple)) {
        MOT_LOG_TRACE("Failed to lookup aggregation function %u", (unsigned)functionId);
        return false;
    }

    Form_pg_aggregate aggStruct = (Form_pg_aggregate)GETSTRUCT(aggTuple);
    MOT_LOG_TRACE("Redirected to aggregation function %u (transfn: %u, finalfn: %u, directargs: %d)",
        (unsigned)aggStruct->aggfnoid,
        (unsigned)aggStruct->aggtransfn,
        (unsigned)aggStruct->aggfinalfn,
        aggStruct->aggnumdirectargs);
    if (aggStruct->aggfinalfn != 0) {
        *aggFunc = GetPGFunctionInfo(aggStruct->aggfinalfn, argCount, isStrict);
    }
    ReleaseSysCache(aggTuple);
    return true;
}

bool ProcessCallSitePlan(JitCallSitePlan* callSitePlan, JitCallSite* callSite, PLpgSQL_function* function)
{
    // copy the context, it is valid and global
    if (callSitePlan->m_queryPlan != nullptr) {
        ExprQueryAttrs attrs;
        if (!GetExprQueryAttrs(callSitePlan->m_expr, function, &attrs)) {
            MOT_LOG_TRACE("Failed to get query attributes from expression: %s", callSitePlan->m_queryString);
            return false;
        }
        MOT_LOG_TRACE("Generating code for called query: %s", callSitePlan->m_queryString);
        if (MOT_CHECK_TRACE_LOG_LEVEL()) {
            char* queryStr = nodeToString(attrs.m_query);
            MOT_LOG_TRACE("Query tree: %s", queryStr);
            pfree(queryStr);
        }
        callSite->m_queryContext = JitCodegenQuery(
            attrs.m_query, callSitePlan->m_queryString, callSitePlan->m_queryPlan, JIT_CONTEXT_GLOBAL_SECONDARY);
        if (callSite->m_queryContext == nullptr) {
            MOT_LOG_TRACE("Failed to generate code for call-site plan: %s", callSite->m_queryString);
            CleanupExprQueryAttrs(&attrs);
            return false;
        }
        CleanupExprQueryAttrs(&attrs);
    } else {
        callSite->m_queryString = DupString(callSitePlan->m_queryString, JitContextUsage::JIT_CONTEXT_GLOBAL);
        if (callSite->m_queryString == nullptr) {
            MOT_LOG_TRACE("Failed to clone call site query string: %s", callSitePlan->m_queryString);
            return false;
        }
    }
    callSite->m_queryCmdType = callSitePlan->m_queryCmdType;

    // move the global tuple descriptor from the plan to the context
    callSite->m_tupDesc = callSitePlan->m_tupDesc;
    callSitePlan->m_tupDesc = nullptr;

    // clone parameter list from plan
    int paramCount = callSitePlan->m_callParamCount;
    if (paramCount > 0) {
        size_t allocSize = sizeof(JitCallParamInfo) * paramCount;
        JitCallParamInfo* result = (JitCallParamInfo*)MOT::MemGlobalAlloc(allocSize);
        if (result == nullptr) {
            MOT_LOG_TRACE(
                "Failed to allocate %u bytes for parameter info in JIT function context", (unsigned)allocSize);
            return false;
        }
        for (int j = 0; j < paramCount; ++j) {
            result[j] = callSitePlan->m_callParamInfo[j];
        }
        callSite->m_callParamInfo = result;
    } else {
        callSite->m_callParamInfo = nullptr;
    }
    callSite->m_callParamCount = paramCount;
    callSite->m_exprIndex = callSitePlan->m_exprIndex;
    callSite->m_isUnjittableInvoke = callSitePlan->m_isUnjittableInvoke;
    callSite->m_isModStmt = callSitePlan->m_isModStmt;
    callSite->m_isInto = callSitePlan->m_isInto;
    return true;
}

MotJitContext* ProcessInvokedPlan(JitFunctionPlan* functionPlan, JitContextUsage usage)
{
    SPIAutoConnect spiAutoConn;
    if (!spiAutoConn.IsConnected()) {
        int rc = spiAutoConn.GetErrorCode();
        MOT_LOG_TRACE("Failed to connect to SPI while generating code for function %s: %s (%u)",
            functionPlan->_function_name,
            SPI_result_code_string(rc),
            rc);
        return nullptr;
    }

    // NOTE: every variable used after catch needs to be volatile (see longjmp() man page)
    volatile HeapTuple procTuple = nullptr;
    volatile MotJitContext* jitContext = nullptr;
    volatile MemoryContext origCxt = CurrentMemoryContext;
    PG_TRY();
    {
        procTuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(functionPlan->_function_id));
        if (!HeapTupleIsValid(procTuple)) {
            MOT_LOG_TRACE("SP %s disqualified: Oid %u not found in pg_proc",
                functionPlan->_function_name,
                functionPlan->_function_id);
        } else {
            // now we try to generate JIT code for the function
            MOT_LOG_TRACE("Generating code for invoked stored procedure: %s", functionPlan->_function_name);
            jitContext = JitCodegenFunction(functionPlan->m_function,
                procTuple,
                functionPlan->_function_id,
                nullptr,
                (JitPlan*)functionPlan,
                usage);
        }
    }
    PG_CATCH();
    {
        (void)MemoryContextSwitchTo(origCxt);
        ErrorData* edata = CopyErrorData();
        MOT_LOG_WARN(
            "Caught exception while generating code for function %s: %s", functionPlan->_function_name, edata->message);
        ereport(WARNING,
            (errmodule(MOD_MOT),
                errmsg("Caught exception while generating code function %s: %s",
                    functionPlan->_function_name,
                    edata->message),
                errdetail("%s", edata->detail)));
        FlushErrorState();
        FreeErrorData(edata);
    }
    PG_END_TRY();

    if (procTuple != nullptr) {
        ReleaseSysCache(procTuple);
    }
    return (MotJitContext*)jitContext;
}

bool GetFuncTypeClass(JitFunctionPlan* functionPlan, TupleDesc* resultTupDesc, TypeFuncClass* typeClass)
{
    // NOTE: every variable used after catch needs to be volatile (see longjmp() man page)
    volatile bool result = false;
    volatile MemoryContext origCtx = CurrentMemoryContext;
    FmgrInfo finfo = {};
    PG_TRY();
    {
        fmgr_info(functionPlan->_function_id, &finfo);

        FunctionCallInfoData fcinfo;
        InitFunctionCallInfoData(fcinfo, &finfo, functionPlan->m_paramCount, InvalidOid, nullptr, nullptr);

        *typeClass = get_call_result_type(&fcinfo, nullptr, resultTupDesc);

        FreeFunctionCallInfoData(fcinfo);
        result = true;
    }
    PG_CATCH();
    {
        // cleanup
        (void)MemoryContextSwitchTo(origCtx);
        ErrorData* edata = CopyErrorData();
        MOT_LOG_WARN(
            "Caught exception while retrieving function %u type class: %s", functionPlan->_function_id, edata->message);
        FlushErrorState();
        FreeErrorData(edata);
    }
    PG_END_TRY();
    return result;
}

char* MakeInvokedQueryString(char* functionName, Oid functionId)
{
    char* result = nullptr;
    MOT::mot_string qualifiedName;
    if (!qualifiedName.format("%s.%u", functionName, functionId)) {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL, "Generate Code", "Failed to format qualified invoked query string");
    } else {
        result = DupString(qualifiedName.c_str(), JitContextUsage::JIT_CONTEXT_GLOBAL);
        if (result == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM,
                "Generate Code",
                "Failed to duplicate qualified invoked query string: %s",
                qualifiedName.c_str());
        }
    }
    return result;
}

MemoryContext SwitchToSPICallerContext()
{
    MemoryContext oldCtx = nullptr;
    if (u_sess->SPI_cxt._curid + 1 == u_sess->SPI_cxt._connected) {  // SPI connected
        if (u_sess->SPI_cxt._current != &(u_sess->SPI_cxt._stack[u_sess->SPI_cxt._curid + 1])) {
            ereport(ERROR,
                (errcode(ERRCODE_DATA_CORRUPTED),
                    errmodule(MOD_MOT),
                    errmsg("SPI stack corrupted when copy tuple, connected level: %d", u_sess->SPI_cxt._connected)));
        }

        MOT_LOG_TRACE("Switching to memory context of SPI caller (connect id = %d): %s --> %s",
            u_sess->SPI_cxt._connected,
            CurrentMemoryContext->name,
            u_sess->SPI_cxt._current->savedcxt ? u_sess->SPI_cxt._current->savedcxt->name : "none");
        oldCtx = MemoryContextSwitchTo(u_sess->SPI_cxt._current->savedcxt);
    } else {
        MOT_LOG_WARN("Cannot switch to memory context of SPI caller: SPI is not connected (connected=%d, curid=%d), "
                     "switching to top memory context of current transaction instead",
            u_sess->SPI_cxt._connected,
            u_sess->SPI_cxt._curid);
        oldCtx = MemoryContextSwitchTo(u_sess->top_transaction_mem_cxt);
    }
    return oldCtx;
}

Datum CopyDatum(Datum value, Oid type, bool isnull)
{
    if (isnull) {
        return (Datum)0;
    }

    if (JitExec::IsPrimitiveType(type)) {
        return value;
    }

    HeapTuple typeTuple = SearchSysCache1(TYPEOID, PointerGetDatum(type));
    Form_pg_type typeStruct = (Form_pg_type)GETSTRUCT(typeTuple);
    bool typeByVal = typeStruct->typbyval;
    int typeLen = typeStruct->typlen;
    ReleaseSysCache(typeTuple);
    Datum res = datumCopy(value, typeByVal, typeLen);
    return res;
}
}  // namespace JitExec
