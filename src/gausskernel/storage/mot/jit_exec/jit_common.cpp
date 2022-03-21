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
 *    Definitions used both by LLVM and TVM jitted code.
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
#include "global.h"
#include "jit_common.h"
#include "utilities.h"
#include "jit_plan.h"
#include "mm_global_api.h"

namespace JitExec {
DECLARE_LOGGER(JitCommon, JitExec)

extern JitCommandType ConvertCommandType(int pgCommandType, bool isPKey)
{
    JitCommandType result = JIT_COMMAND_INVALID;

    switch (pgCommandType) {
        case CMD_INSERT:
            result = JIT_COMMAND_INSERT;
            break;

        case CMD_UPDATE:
            result = JIT_COMMAND_UPDATE;
            break;

        case CMD_DELETE:
            result = JIT_COMMAND_DELETE;
            break;

        case CMD_SELECT:
            result = JIT_COMMAND_SELECT;
            break;

        default:
            break;
    }

    if ((result == JIT_COMMAND_UPDATE) && !isPKey) {
        result = JIT_COMMAND_RANGE_UPDATE;
    }

    return result;
}

extern const char* CommandToString(JitCommandType commandType)
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

        case JIT_COMMAND_INVALID:
        default:
            return "Invalid command";
    }
}

extern bool IsRangeCommand(JitCommandType commandType)
{
    bool rangeCommand = false;

    switch (commandType) {
        case JIT_COMMAND_RANGE_UPDATE:
        case JIT_COMMAND_RANGE_SELECT:
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

extern bool IsJoinCommand(JitCommandType commandType)
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

extern MOT::Table* GetTableFromQuery(const Query* query)
{
    // we reject queries that reference more than one table
    if (list_length(query->rtable) != 1) {
        MOT_LOG_TRACE("Cannot retrieve query table: query does not have exactly one rtable entry");
        return NULL;
    }

    RangeTblEntry* rte = (RangeTblEntry*)linitial(query->rtable);
    Oid relid = rte->relid;
    MOT_LOG_TRACE("Seeing relid=%u relname=%s", (unsigned)rte->relid, rte->relname);

    MOT::Table* table = MOT::GetTableManager()->GetTableByExternal(relid);
    if (table != nullptr) {
        MOT_LOG_DEBUG("Retrieved table %p by external id %u: %s", table, relid, table->GetLongTableName().c_str());
    } else {
        MOT_LOG_TRACE("Failed to find table by external relation id: %u", relid);
    }
    return table;
}

extern int MapTableColumnToIndex(MOT::Table* table, const MOT::Index* index, int columnId)
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

extern int BuildColumnMap(MOT::Table* table, MOT::Index* index, int** columnMap)
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

extern int BuildIndexColumnOffsets(MOT::Table* table, const MOT::Index* index, int** offsets)
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

extern bool IsTypeSupported(int resultType)
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
            return true;
        default:
            return false;
    }
}

extern bool IsStringType(int type)
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

extern bool IsPrimitiveType(int type)
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
        (whereOp == 1108 /* time_eq */) || (whereOp == TIMETZEQOID) || (whereOp == 5550 /* smalldatetime_eq */) ||
        (whereOp == 2347 /* date_eq_timestamp */) || (whereOp == 2373 /* timestamp_eq_date */) ||
        (whereOp == 2360 /* date_eq_timestamptz */) || (whereOp == 2386 /* timestamptz_eq_date */) ||
        (whereOp == 2536 /* timestamp_eq_timestamptz */) || (whereOp == 2542 /* timestamptz_eq_timestamp */) ||
        (whereOp == INTERVALEQOID)
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
        (whereOp == 5550 /* smalldatetime_lt */) || (whereOp == 2347 /* date_lt_timestamp */) ||
        (whereOp == 2373 /* timestamp_lt_date */) || (whereOp == 2360 /* date_lt_timestamptz */) ||
        (whereOp == 2386 /* timestamptz_lt_date */) || (whereOp == 2536 /* timestamp_lt_timestamptz */) ||
        (whereOp == 2542 /* timestamptz_lt_timestamp */) || (whereOp == 1332 /* interval_lt */)
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
        (whereOp == 5554 /* smalldatetime_gt */) || (whereOp == 2349 /* date_gt_timestamp */) ||
        (whereOp == 2375 /* timestamp_gt_date */) || (whereOp == 2362 /* date_gt_timestamptz */) ||
        (whereOp == 2388 /* timestamptz_gt_date */) || (whereOp == 2538 /* timestamp_gt_timestamptz */) ||
        (whereOp == 2544 /* timestamptz_gt_timestamp */) || (whereOp == 1334 /* interval_gt */)
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
        (whereOp == 5553 /* smalldatetime_le */) || (whereOp == 2346 /* date_le_timestamp */) ||
        (whereOp == 2372 /* timestamp_le_date */) || (whereOp == 2359 /* date_le_timestamptz */) ||
        (whereOp == 2385 /* timestamptz_le_date */) || (whereOp == 2535 /* timestamp_le_timestamptz */) ||
        (whereOp == 2541 /* timestamptz_le_timestamp */) || (whereOp == 1333 /* interval_le */)
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
        (whereOp == 5549 /* smalldatetime_ge */) || (whereOp == 2348 /* date_ge_timestamp */) ||
        (whereOp == 2374 /* timestamp_ge_date */) || (whereOp == 2361 /* date_ge_timestamptz */) ||
        (whereOp == 2387 /* timestamptz_ge_date */) || (whereOp == 2537 /* timestamp_ge_timestamptz */) ||
        (whereOp == 2543 /* timestamptz_ge_timestamp */) || (whereOp == 1335 /* interval_ge */)
#endif
    ) {
        result = true;
    }

    return result;
}

extern bool IsWhereOperatorSupported(int whereOp)
{
    return ClassifyWhereOperator(whereOp) != JIT_WOC_INVALID;
}

extern JitWhereOperatorClass ClassifyWhereOperator(int whereOp)
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

#define APPLY_UNARY_OPERATOR(funcId, name) \
    case funcId:                           \
        break;
#define APPLY_BINARY_OPERATOR(funcId, name) \
    case funcId:                            \
        break;
#define APPLY_TERNARY_OPERATOR(funcId, name) \
    case funcId:                             \
        break;
#define APPLY_UNARY_CAST_OPERATOR(funcId, name) \
    case funcId:                                \
        break;
#define APPLY_BINARY_CAST_OPERATOR(funcId, name) \
    case funcId:                                 \
        break;
#define APPLY_TERNARY_CAST_OPERATOR(funcId, name) \
    case funcId:                                  \
        break;

extern bool IsFuncIdSupported(int funcId)
{
    bool result = true;
    switch (funcId) {
        APPLY_OPERATORS()
        default:
            result = false;
    }
    return result;
}

#undef APPLY_UNARY_OPERATOR
#undef APPLY_BINARY_OPERATOR
#undef APPLY_TERNARY_OPERATOR
#undef APPLY_UNARY_CAST_OPERATOR
#undef APPLY_BINARY_CAST_OPERATOR
#undef APPLY_TERNARY_CAST_OPERATOR

extern bool IsFullPrefixSearch(const int* columnArray, int columnCount, int* firstZeroColumn)
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

extern JitQuerySortOrder GetQuerySortOrder(const Query* query)
{
    JitQuerySortOrder result = JIT_QUERY_SORT_INVALID;

    if (!query->sortClause) {
        MOT_LOG_TRACE("Query has no sort clause, setting it by default to ascending");
        result = JIT_QUERY_SORT_ASCENDING;
    } else {
        SortGroupClause* sgc = (SortGroupClause*)linitial(query->sortClause);
        int op = (int)sgc->sortop;
        MOT_LOG_TRACE("Checking sort order by operator %d", op);
        if ((op == INT48LTOID) || (op == 58) /* boolean less than */ || (op == INT2LTOID) || (op == INT4LTOID) ||
            (op == INT8LTOID) || (op == INT84LTOID) || (op == INT24LTOID) || (op == INT42LTOID) || (op == INT28LTOID) ||
            (op == INT82LTOID) || (op == 5515) /* int1lt */ || (op == FLOAT4LTOID) || (op == FLOAT8LTOID) ||
            (op == FLOAT48LTOID) || (op == FLOAT84LTOID) || (op == NUMERICLTOID) || (op == 631) /* char less than */ ||
            (op == TEXTLTOID) || (op == BPCHARLTOID) || (op == 1957) /* bytealt */ || (op == DATELTOID) ||
            (op == 1110) /* time_lt */ || (op == 1552) /* timetz_lt */ || (op == 1322) /* timestamptz_lt */ ||
            (op == 1332) /* interval_lt */ || (op == TIMESTAMPLTOID) || (op == 5552) /* smalldatetime_lt */) {
            MOT_LOG_TRACE("Found ascending sort order by sortop %d", op);
            result = JIT_QUERY_SORT_ASCENDING;
        } else if ((op == INT48GTOID) || (op == 59) /* boolgt */ || (op == INT2GTOID) || (op == INT4GTOID) ||
                   (op == INT8GTOID) || (op == INT84GTOID) || (op == INT24GTOID) || (op == INT42GTOID) ||
                   (op == INT28GTOID) || (op == INT82GTOID) || (op == 5517) /* int1gt */ || (op == FLOAT4GTOID) ||
                   (op == FLOAT8GTOID) || (op == FLOAT48GTOID) || (op == FLOAT84GTOID) || (op == NUMERICGTOID) ||
                   (op == 633) /* chargt */ || (op == TEXTGTOID) || (op == BPCHARGTOID) || (op == 1959) /* byteagt */ ||
                   (op == DATEGTOID) || (op == 1112) /* time_gt */ || (op == 1554) /* timetz_gt */ ||
                   (op == 1324) /* timestamptz_gt */ || (op == 1334) /* interval_gt */ || (op == TIMESTAMPGTOID) ||
                   (op == 5554) /* smalldatetime_gt */) {
            MOT_LOG_TRACE("Found descending sort order by sortop %d", op);
            result = JIT_QUERY_SORT_DESCENDING;
        }
    }

    return result;
}

extern int GetAggregateTupleColumnIdAndType(const Query* query, int& zeroType)
{
    TargetEntry* targetEntry = (TargetEntry*)linitial(query->targetList);
    int tupleColId = targetEntry->resno - 1;
    Aggref* aggRef = (Aggref*)targetEntry->expr;
    zeroType = (int)aggRef->aggtype;

    return tupleColId;
}

/** @brief Initializes a table info struct. */
extern bool InitTableInfo(TableInfo* tableInfo, MOT::Table* table, MOT::Index* index)
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
        return false;
    }

    return true;
}

/** @brief Releases all resources associated with a table info struct. */
extern void DestroyTableInfo(TableInfo* tableInfo)
{
    if (tableInfo != nullptr) {
        if (tableInfo->m_columnMap != nullptr) {
            MOT::MemSessionFree(tableInfo->m_columnMap);
        }
        if (tableInfo->m_indexColumnOffsets != nullptr) {
            MOT::MemSessionFree(tableInfo->m_indexColumnOffsets);
        }
    }
}

extern bool PrepareSubQueryData(JitContext* jitContext, JitCompoundPlan* plan)
{
    bool result = true;

    // allocate sub-query data array
    int subQueryCount = plan->_sub_query_count;
    MOT_LOG_TRACE("Preparing %u sub-query data items", subQueryCount);
    uint32_t allocSize = sizeof(JitContext::SubQueryData) * subQueryCount;
    jitContext->m_subQueryData = (JitContext::SubQueryData*)MOT::MemGlobalAllocAligned(allocSize, L1_CACHE_LINE);
    if (jitContext->m_subQueryData == nullptr) {
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
                JitContext::SubQueryData* subQueryData = &jitContext->m_subQueryData[subQueryIndex];

                // nullify unknown members
                subQueryData->m_tupleDesc = nullptr;
                subQueryData->m_slot = nullptr;
                subQueryData->m_searchKey = nullptr;
                subQueryData->m_endIteratorKey = nullptr;

                // prepare sub-query data items according to sub-plan type
                JitPlan* subPlan = plan->_sub_query_plans[subQueryIndex];
                JitPlanType subPlanType = subPlan->_plan_type;
                MOT_ASSERT((subPlanType == JIT_PLAN_POINT_QUERY) || (subPlanType == JIT_PLAN_RANGE_SCAN));

                if (subPlanType == JIT_PLAN_POINT_QUERY) {  // simple point-select
                    MOT_ASSERT(((JitPointQueryPlan*)subPlan)->_command_type == JIT_COMMAND_SELECT);
                    JitPointQueryPlan* subPointQueryPlan = (JitPointQueryPlan*)subPlan;
                    if (subPointQueryPlan->_command_type == JIT_COMMAND_SELECT) {
                        MOT_LOG_TRACE("Detected point-query select sub-plan");
                        subQueryData->m_commandType = JIT_COMMAND_SELECT;
                        JitSelectPlan* selectPlan = (JitSelectPlan*)subPlan;
                        subQueryData->m_table = selectPlan->_query._table;
                        subQueryData->m_index = subQueryData->m_table->GetPrimaryIndex();
                        subQueryData->m_indexId = subQueryData->m_index->GetExtId();
                        MOT_LOG_TRACE("Installed sub-query %d index id: %" PRIu64, i, subQueryData->m_indexId);
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
                        subQueryData->m_table = rangeSelectPlan->_index_scan._table;
                        subQueryData->m_index = subQueryData->m_table->GetIndex(rangeSelectPlan->_index_scan._index_id);
                        if (rangeSelectPlan->_aggregate._aggreaget_op != JIT_AGGREGATE_NONE) {  // aggregate range-scan
                            subQueryData->m_commandType = JIT_COMMAND_AGGREGATE_RANGE_SELECT;
                        } else if (rangeSelectPlan->_limit_count == 1) {
                            subQueryData->m_commandType = JIT_COMMAND_RANGE_SELECT;  // implies "limit 1" clause
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

    bytea* copy = nullptr;
    if (usage == JIT_CONTEXT_GLOBAL) {
        copy = (bytea*)MOT::MemGlobalAlloc(len);
    } else {
        copy = (bytea*)MOT::MemSessionAlloc(len);
    }
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
    MOT::TimetzSt* copy = nullptr;
    if (usage == JIT_CONTEXT_GLOBAL) {
        copy = (MOT::TimetzSt*)MOT::MemGlobalAlloc(allocSize);
    } else {
        copy = (MOT::TimetzSt*)MOT::MemSessionAlloc(allocSize);
    }
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
    MOT::IntervalSt* copy = nullptr;
    if (usage == JIT_CONTEXT_GLOBAL) {
        copy = (MOT::IntervalSt*)MOT::MemGlobalAlloc(allocSize);
    } else {
        copy = (MOT::IntervalSt*)MOT::MemSessionAlloc(allocSize);
    }
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
    MOT::TintervalSt* copy = nullptr;
    if (usage == JIT_CONTEXT_GLOBAL) {
        copy = (MOT::TintervalSt*)MOT::MemGlobalAlloc(allocSize);
    } else {
        copy = (MOT::TintervalSt*)MOT::MemSessionAlloc(allocSize);
    }
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
    struct varlena* result = nullptr;
    if (usage == JIT_CONTEXT_GLOBAL) {
        result = (varlena*)MOT::MemGlobalAlloc(len);
    } else {
        result = (varlena*)MOT::MemSessionAlloc(len);
    }
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

    char* copy = nullptr;
    if (usage == JIT_CONTEXT_GLOBAL) {
        copy = (char*)MOT::MemGlobalAlloc(len);
    } else {
        copy = (char*)MOT::MemSessionAlloc(len);
    }
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

extern bool CloneDatum(Datum source, int type, Datum* target, JitContextUsage usage)
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

extern bool PrepareDatumArray(Const* constArray, uint32_t constCount, JitDatumArray* datumArray)
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
}  // namespace JitExec
