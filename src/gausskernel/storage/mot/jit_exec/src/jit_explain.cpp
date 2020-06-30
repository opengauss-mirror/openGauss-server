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
 * jit_explain.cpp
 *    Implementation of JIT explain plan.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/jit_exec/src/jit_explain.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "global.h"
#include "jit_plan.h"
#include "jit_common.h"
#include "mot_engine.h"
#include "utilities.h"

namespace JitExec {
DECLARE_LOGGER(JitExplain, JitExec)

// Forward declarations
static void ExplainExpr(Query* query, Expr* expr);

static void ExplainConstExpr(Const* constExpr)
{
    switch (constExpr->consttype) {
        case BOOLOID:
            MOT_LOG_APPEND(MOT::LogLevel::LL_TRACE, "[bool] %u", (unsigned)DatumGetBool(constExpr->constvalue));
            break;

        case CHAROID:
            MOT_LOG_APPEND(MOT::LogLevel::LL_TRACE, "[char] %u", (unsigned)DatumGetChar(constExpr->constvalue));
            break;

        case INT1OID:
            MOT_LOG_APPEND(MOT::LogLevel::LL_TRACE, "[int1] %u", (unsigned)DatumGetUInt8(constExpr->constvalue));
            break;

        case INT2OID:
            MOT_LOG_APPEND(MOT::LogLevel::LL_TRACE, "[int2] %u", (unsigned)DatumGetUInt16(constExpr->constvalue));
            break;

        case INT4OID:
            MOT_LOG_APPEND(MOT::LogLevel::LL_TRACE, "[int4] %u", (unsigned)DatumGetUInt32(constExpr->constvalue));
            break;

        case INT8OID:
            MOT_LOG_APPEND(
                MOT::LogLevel::LL_TRACE, "[int8] %u" PRIu64, (uint64_t)DatumGetUInt64(constExpr->constvalue));
            break;

        case TIMESTAMPOID:
            MOT_LOG_APPEND(
                MOT::LogLevel::LL_TRACE, "[timestamp] %" PRIu64, (uint64_t)DatumGetTimestamp(constExpr->constvalue));
            break;

        case FLOAT4OID:
            MOT_LOG_APPEND(MOT::LogLevel::LL_TRACE, "[float4] %f", (double)DatumGetFloat4(constExpr->constvalue));
            break;

        case FLOAT8OID:
            MOT_LOG_APPEND(MOT::LogLevel::LL_TRACE, "[float8] %f", (double)DatumGetFloat8(constExpr->constvalue));
            break;

        case VARCHAROID: {
            VarChar* vc = DatumGetVarCharPP(constExpr->constvalue);
            int size = VARSIZE_ANY_EXHDR(vc);
            char* src = VARDATA_ANY(vc);
            MOT_LOG_APPEND(MOT::LogLevel::LL_TRACE, "[varchar] %.*s", size, src);
        } break;

        case NUMERICOID: {
            Datum result = DirectFunctionCall1(numeric_out, constExpr->constvalue);
            char* cstring = DatumGetCString(result);
            MOT_LOG_APPEND(MOT::LogLevel::LL_TRACE, "[numeric] %s", cstring);
        } break;

        default:
            MOT_LOG_APPEND(MOT::LogLevel::LL_TRACE,
                "[type %d] %" PRIu64,
                (int)constExpr->consttype,
                (uint64_t)constExpr->constvalue);
            break;
    }
}

static void ExplainRealColumnName(const Query* query, int tableRefId, int columnId)
{
    if (tableRefId <= list_length(query->rtable)) {  // varno index is 1-based
        RangeTblEntry* rte = (RangeTblEntry*)list_nth(query->rtable, tableRefId - 1);
        if (rte->rtekind == RTE_RELATION) {
            MOT::Table* table = MOT::GetTableManager()->GetTableByExternal(rte->relid);
            if (table != nullptr) {
                if (list_length(query->rtable) == 1) {
                    MOT_LOG_APPEND(MOT::LogLevel::LL_TRACE, "%s", table->GetFieldName(columnId));
                } else {
                    MOT_LOG_APPEND(
                        MOT::LogLevel::LL_TRACE, "%s.%s", table->GetTableName().c_str(), table->GetFieldName(columnId));
                }
                return;
            }
        } else if (rte->rtekind == RTE_JOIN) {
            Var* aliasVar = (Var*)list_nth(rte->joinaliasvars, columnId - 1);  // this is zero-based!
            tableRefId = aliasVar->varno;
            if (tableRefId <= list_length(query->rtable)) {  // tableRefId is 1-based
                rte = (RangeTblEntry*)list_nth(query->rtable, tableRefId - 1);
                MOT::Table* table = MOT::GetTableManager()->GetTableByExternal(rte->relid);
                if (table != nullptr) {
                    // take real column id and not column id from virtual join table
                    columnId = aliasVar->varattno;
                    MOT_LOG_APPEND(
                        MOT::LogLevel::LL_TRACE, "%s.%s", table->GetTableName().c_str(), table->GetFieldName(columnId));
                    return;
                }
            }
        }
    }
    MOT_LOG_APPEND(MOT::LogLevel::LL_TRACE, "UnknownColumn");
}

static void ExplainVarExpr(Query* query, const Var* varExpr)
{
    int columnId = varExpr->varattno;
    int tableRefId = varExpr->varno;
    ExplainRealColumnName(query, tableRefId, columnId);
}

static void ExplainParamExpr(const Param* paramExpr)
{
    MOT_LOG_APPEND(MOT::LogLevel::LL_TRACE, "$%d", paramExpr->paramid);
}

static void ExplainRelabelExpr(Query* query, RelabelType* relabelType)
{
    Expr* expr = (Expr*)relabelType->arg;
    if (expr->type == T_Param) {
        ExplainParamExpr((Param*)expr);
    } else if (expr->type == T_Var) {
        ExplainVarExpr(query, (Var*)expr);
    } else {
        MOT_LOG_APPEND(MOT::LogLevel::LL_TRACE, "RelabelExpr");
    }
}

#define APPLY_UNARY_OPERATOR(funcid, name)                  \
    case funcid:                                            \
        MOT_LOG_APPEND(MOT::LogLevel::LL_TRACE, #name "("); \
        ExplainExpr(query, args[0]);                        \
        MOT_LOG_APPEND(MOT::LogLevel::LL_TRACE, ")");       \
        break;

#define APPLY_BINARY_OPERATOR(funcid, name)                 \
    case funcid:                                            \
        MOT_LOG_APPEND(MOT::LogLevel::LL_TRACE, #name "("); \
        ExplainExpr(query, args[0]);                        \
        MOT_LOG_APPEND(MOT::LogLevel::LL_TRACE, ", ");      \
        ExplainExpr(query, args[1]);                        \
        MOT_LOG_APPEND(MOT::LogLevel::LL_TRACE, ")");       \
        break;

#define APPLY_TERNARY_OPERATOR(funcid, name)                \
    case funcid:                                            \
        MOT_LOG_APPEND(MOT::LogLevel::LL_TRACE, #name "("); \
        ExplainExpr(query, args[0]);                        \
        MOT_LOG_APPEND(MOT::LogLevel::LL_TRACE, ", ");      \
        ExplainExpr(query, args[1]);                        \
        MOT_LOG_APPEND(MOT::LogLevel::LL_TRACE, ", ");      \
        ExplainExpr(query, args[2]);                        \
        MOT_LOG_APPEND(MOT::LogLevel::LL_TRACE, ")");       \
        break;

#define APPLY_UNARY_CAST_OPERATOR(funcid, name) APPLY_UNARY_OPERATOR(funcid, name)
#define APPLY_BINARY_CAST_OPERATOR(funcid, name) APPLY_BINARY_OPERATOR(funcid, name)
#define APPLY_TERNARY_CAST_OPERATOR(funcid, name) APPLY_TERNARY_OPERATOR(funcid, name)

static void ExplainOpExpr(Query* query, const OpExpr* opExpr)
{
    Expr* args[3] = {nullptr, nullptr, nullptr};
    int argNum = 0;

    // process operator arguments (each one is an expression by itself)
    ListCell* lc = nullptr;

    foreach (lc, opExpr->args) {
        args[argNum] = (Expr*)lfirst(lc);
        if (++argNum == 3) {
            break;
        }
    }

    // explain the operator
    switch (opExpr->opfuncid) {
        APPLY_OPERATORS()

        default:
            MOT_LOG_APPEND(MOT::LogLevel::LL_TRACE, "[op_%u]", opExpr->opfuncid);
            break;
    }
}

static void ExplainFuncExpr(Query* query, const FuncExpr* funcExpr)
{
    Expr* args[3] = {nullptr, nullptr, nullptr};
    int argNum = 0;

    // process operator arguments (each one is an expression by itself)
    ListCell* lc = nullptr;

    foreach (lc, funcExpr->args) {
        args[argNum] = (Expr*)lfirst(lc);
        if (++argNum == 3) {
            break;
        }
    }

    // explain the function
    switch (funcExpr->funcid) {
        APPLY_OPERATORS()

        default:
            MOT_LOG_APPEND(MOT::LogLevel::LL_TRACE, "[func_%d]", funcExpr->funcid);
            break;
    }
}

static void ExplainExpr(Query* query, Expr* expr)
{
    switch (expr->type) {
        case T_Const:
            ExplainConstExpr((Const*)expr);
            break;

        case T_Var:
            ExplainVarExpr(query, (Var*)expr);
            break;

        case T_Param:
            ExplainParamExpr((Param*)expr);
            break;

        case T_OpExpr:
            ExplainOpExpr(query, (OpExpr*)expr);
            break;

        case T_FuncExpr:
            ExplainFuncExpr(query, (FuncExpr*)expr);
            break;

        case T_RelabelType:
            ExplainRelabelExpr(query, (RelabelType*)expr);
            break;

        default:
            MOT_LOG_APPEND(MOT::LogLevel::LL_TRACE, "[expr]");
    }
}

static const char* JitWhereOperatorClassToString(JitWhereOperatorClass opClass)
{
    switch (opClass) {
        case JIT_WOC_EQUALS:
            return "==";
        case JIT_WOC_LESS_THAN:
            return "<";
        case JIT_WOC_GREATER_THAN:
            return ">";
        case JIT_WOC_LESS_EQUALS:
            return "<=";
        case JIT_WOC_GREATER_EQUALS:
            return ">=";
        default:
            return "??";
    }
}

static void ExplainFilterArray(Query* query, int indent, JitFilterArray* filterArray)
{
    MOT_LOG_BEGIN(MOT::LogLevel::LL_TRACE, "%*sFILTER ON (", indent, "");
    for (int i = 0; i < filterArray->_filter_count; ++i) {
        ExplainExpr(query, filterArray->_scan_filters[i]._lhs_operand->_source_expr);
        JitWhereOperatorClass opClass = ClassifyWhereOperator(filterArray->_scan_filters[i]._filter_op);
        MOT_LOG_APPEND(MOT::LogLevel::LL_TRACE, " %s ", JitWhereOperatorClassToString(opClass));
        ExplainExpr(query, filterArray->_scan_filters[i]._rhs_operand->_source_expr);
        if (i < (filterArray->_filter_count - 1)) {
            MOT_LOG_APPEND(MOT::LogLevel::LL_TRACE, " AND ");
        }
    }
    MOT_LOG_APPEND(MOT::LogLevel::LL_TRACE, ")");
    MOT_LOG_END(MOT::LogLevel::LL_TRACE);
}

static void ExplainExprArray(Query* query, JitColumnExprArray* exprArray, const char* sep, bool forSelect = false)
{
    for (int i = 0; i < exprArray->_count; ++i) {
        if (!forSelect) {
            MOT_LOG_APPEND(MOT::LogLevel::LL_TRACE,
                "%s = ",
                exprArray->_exprs[i]._table->GetFieldName(exprArray->_exprs[i]._table_column_id));
        }
        ExplainExpr(query, exprArray->_exprs[i]._expr->_source_expr);
        if (i < (exprArray->_count - 1)) {
            MOT_LOG_APPEND(MOT::LogLevel::LL_TRACE, sep);
        }
    }
}

static void ExplainInsertExprArray(Query* query, int indent, JitColumnExprArray* exprArray)
{
    MOT_LOG_BEGIN(MOT::LogLevel::LL_TRACE, "%*sVALUES (", indent, "");
    ExplainExprArray(query, exprArray, ", ");
    MOT_LOG_APPEND(MOT::LogLevel::LL_TRACE, ")");
    MOT_LOG_END(MOT::LogLevel::LL_TRACE);
}

static void ExplainUpdateExprArray(Query* query, int indent, JitColumnExprArray* exprArray)
{
    MOT_LOG_BEGIN(MOT::LogLevel::LL_TRACE, "%*sSET (", indent, "");
    ExplainExprArray(query, exprArray, ", ");
    MOT_LOG_APPEND(MOT::LogLevel::LL_TRACE, ")");
    MOT_LOG_END(MOT::LogLevel::LL_TRACE);
}

static void ExplainSelectExprArray(Query* query, JitSelectExprArray* exprArray)
{
    MOT_LOG_APPEND(MOT::LogLevel::LL_TRACE, "(");
    for (int i = 0; i < exprArray->_count; ++i) {
        MOT_LOG_APPEND(MOT::LogLevel::LL_TRACE, "%%%d = ", exprArray->_exprs[i]._tuple_column_id);
        ExplainExpr(query, exprArray->_exprs[i]._column_expr->_source_expr);
        if (i < (exprArray->_count - 1)) {
            MOT_LOG_APPEND(MOT::LogLevel::LL_TRACE, ", ");
        }
    }
    MOT_LOG_APPEND(MOT::LogLevel::LL_TRACE, ")");
}

static void ExplainSearchExprArray(Query* query, int indent, JitColumnExprArray* exprArray)
{
    MOT_LOG_BEGIN(MOT::LogLevel::LL_TRACE, "%*sWHERE (", indent, "");
    ExplainExprArray(query, exprArray, " AND ");
    MOT_LOG_APPEND(MOT::LogLevel::LL_TRACE, ")");
    MOT_LOG_END(MOT::LogLevel::LL_TRACE);
}

static void ExplainInsertPlan(Query* query, JitInsertPlan* plan)
{
    MOT_LOG_TRACE("[Plan] INSERT into table %s", plan->_table->GetTableName().c_str());
    ExplainInsertExprArray(query, 2, &plan->_insert_exprs);
}

static void ExplainUpdateQueryPlan(Query* query, int indent, JitUpdatePlan* plan)
{
    MOT_LOG_TRACE("%*sUPDATE table %s:", indent, "", plan->_query._table->GetTableName().c_str());
    ExplainUpdateExprArray(query, indent + 2, &plan->_update_exprs);
}

static void ExplainDeleteQueryPlan(int indent, const JitDeletePlan* plan)
{
    MOT_LOG_TRACE("%*sDELETE from table %s:", indent, "", plan->_query._table->GetTableName().c_str());
}

static void ExplainSelectQueryPlan(Query* query, int indent, JitSelectPlan* plan)
{
    MOT_LOG_BEGIN(
        MOT::LogLevel::LL_TRACE, "%*sSELECT from table %s:", indent, "", plan->_query._table->GetTableName().c_str());
    ExplainSelectExprArray(query, &plan->_select_exprs);
    MOT_LOG_END(MOT::LogLevel::LL_TRACE);
}

static void ExplainPointQueryPlan(Query* query, JitPointQueryPlan* plan)
{
    MOT_LOG_TRACE("[Plan] Point Query", plan->_query._table->GetTableName().c_str());
    int indent = 0;
    if (plan->_query._filters._filter_count > 0) {
        ExplainFilterArray(query, indent, &plan->_query._filters);
        indent += 2;
    }
    switch (plan->_command_type) {
        case JIT_COMMAND_UPDATE:
            ExplainUpdateQueryPlan(query, indent + 2, (JitUpdatePlan*)plan);
            break;

        case JIT_COMMAND_DELETE:
            ExplainDeleteQueryPlan(indent + 2, (JitDeletePlan*)plan);
            break;

        case JIT_COMMAND_SELECT:
            ExplainSelectQueryPlan(query, indent + 2, (JitSelectPlan*)plan);
            break;

        default:
            MOT_LOG_TRACE("Invalid plan command type");
            return;
    }

    ExplainSearchExprArray(query, indent + 4, &plan->_query._search_exprs);
}

static void ExplainIndexExprArray(Query* query, JitIndexScan* indexScan, int count)
{
    for (int i = 0; i < count; ++i) {
        if (indexScan->_search_exprs._exprs[i]._join_expr) {
            MOT_LOG_APPEND(MOT::LogLevel::LL_TRACE, "JoinExpr(");
        }
        MOT_LOG_APPEND(MOT::LogLevel::LL_TRACE,
            "%s = ",
            indexScan->_table->GetFieldName(indexScan->_search_exprs._exprs[i]._table_column_id));
        ExplainExpr(query, indexScan->_search_exprs._exprs[i]._expr->_source_expr);
        if (indexScan->_search_exprs._exprs[i]._join_expr) {
            MOT_LOG_APPEND(MOT::LogLevel::LL_TRACE, ")");
        }
        if (i < (count - 1)) {
            MOT_LOG_APPEND(MOT::LogLevel::LL_TRACE, " AND ");
        }
    }
}

static void ExplainIndexOpenExpr(Query* query, JitIndexScan* indexScan, int index, JitWhereOperatorClass opClass)
{
    MOT_LOG_APPEND(MOT::LogLevel::LL_TRACE,
        "%s",
        indexScan->_table->GetFieldName(indexScan->_search_exprs._exprs[index]._table_column_id));
    MOT_LOG_APPEND(MOT::LogLevel::LL_TRACE, " %s ", JitWhereOperatorClassToString(opClass));
    ExplainExpr(query, indexScan->_search_exprs._exprs[index]._expr->_source_expr);
}

static void ExplainIndexExprArray(Query* query, JitIndexScan* indexScan)
{
    if ((indexScan->_scan_type == JIT_INDEX_SCAN_CLOSED) || (indexScan->_scan_type == JIT_INDEX_SCAN_POINT)) {
        ExplainIndexExprArray(query, indexScan, indexScan->_search_exprs._count);
    } else if (indexScan->_scan_type == JIT_INDEX_SCAN_SEMI_OPEN) {
        ExplainIndexExprArray(query, indexScan, indexScan->_search_exprs._count - 1);
        MOT_LOG_APPEND(MOT::LogLevel::LL_TRACE, " AND ");
        ExplainIndexOpenExpr(query, indexScan, indexScan->_search_exprs._count - 1, indexScan->_last_dim_op1);
    } else if (indexScan->_scan_type == JIT_INDEX_SCAN_OPEN) {
        ExplainIndexExprArray(query, indexScan, indexScan->_search_exprs._count - 2);
        MOT_LOG_APPEND(MOT::LogLevel::LL_TRACE, " AND ");
        ExplainIndexOpenExpr(query, indexScan, indexScan->_search_exprs._count - 2, indexScan->_last_dim_op1);
        MOT_LOG_APPEND(MOT::LogLevel::LL_TRACE, " AND ");
        ExplainIndexOpenExpr(query, indexScan, indexScan->_search_exprs._count - 1, indexScan->_last_dim_op2);
    }
}

static const char* JitQuerySortOrderToString(JitQuerySortOrder sortOrder)
{
    switch (sortOrder) {
        case JIT_QUERY_SORT_ASCENDING:
            return "Ascending";
        case JIT_QUERY_SORT_DESCENDING:
            return "Descending";
        case JIT_QUERY_SORT_INVALID:
        default:
            return "Invalid-Sort-Order";
    }
}

static const char* JitIndexScanTypeToString(JitIndexScanType scanType)
{
    switch (scanType) {
        case JIT_INDEX_SCAN_CLOSED:
            return "Closed-Scan";
        case JIT_INDEX_SCAN_OPEN:
            return "Open-Scan";
        case JIT_INDEX_SCAN_SEMI_OPEN:
            return "Semi-Open-Scan";
        case JIT_INDEX_SCAN_POINT:
            return "Point-Scan";
        default:
            return "Invalid-Scan-Type";
    }
}

static const char* JitIndexScanDirectionToString(JitIndexScanDirection scanDirection)
{
    switch (scanDirection) {
        case JIT_INDEX_SCAN_FORWARD:
            return "Forward-Scan";
        case JIT_INDEX_SCAN_BACKWARDS:
            return "Backwards-Scan";
        default:
            return "Invalid-Scan-Direction";
    }
}

static void ExplainIndexScan(Query* query, int indent, JitIndexScan* indexScan, const char* scanName = "")
{
    if (indexScan->_filters._filter_count > 0) {
        ExplainFilterArray(query, indent, &indexScan->_filters);
        indent += 2;
    }
    MOT::Index* index = indexScan->_table->GetIndex(indexScan->_index_id);
    MOT_LOG_BEGIN(MOT::LogLevel::LL_TRACE,
        "%*s%sSCAN %s index %s (%s, %s) ON (",
        indent,
        "",
        scanName,
        index->GetName().c_str(),
        JitQuerySortOrderToString(indexScan->_sort_order),
        JitIndexScanTypeToString(indexScan->_scan_type),
        JitIndexScanDirectionToString(indexScan->_scan_direction));

    ExplainIndexExprArray(query, indexScan);
    MOT_LOG_APPEND(MOT::LogLevel::LL_TRACE, ")");
    MOT_LOG_END(MOT::LogLevel::LL_TRACE);
}

static void ExplainRangeUpdatePlan(Query* query, JitRangeUpdatePlan* plan)
{
    MOT_LOG_TRACE("[Plan] Range UPDATE table %s:", plan->_index_scan._table->GetTableName().c_str());
    ExplainUpdateExprArray(query, 2, &plan->_update_exprs);
    ExplainIndexScan(query, 2, &plan->_index_scan);
}

static const char* JitAggregateOperatorToString(JitAggregateOperator aggregateOp)
{
    switch (aggregateOp) {
        case JIT_AGGREGATE_AVG:
            return "AVG";
        case JIT_AGGREGATE_SUM:
            return "SUM";
        case JIT_AGGREGATE_MAX:
            return "MAX";
        case JIT_AGGREGATE_MIN:
            return "MIN";
        case JIT_AGGREGATE_COUNT:
            return "COUNT";
        default:
            return "AGG-Unknown";
    }
}

static void ExplainAggregateOperator(int indent, const JitAggregate* aggregate)
{
    MOT_LOG_BEGIN(MOT::LogLevel::LL_TRACE,
        "%*s%s [op %d](",
        indent,
        "",
        JitAggregateOperatorToString(aggregate->_aggreaget_op),
        aggregate->_func_id);
    if (aggregate->_distinct) {
        MOT_LOG_APPEND(MOT::LogLevel::LL_TRACE, "DISTINCT(");
    }
    MOT_LOG_APPEND(MOT::LogLevel::LL_TRACE,
        "%s.%s)",
        aggregate->_table->GetTableName().c_str(),
        aggregate->_table->GetFieldName(aggregate->_table_column_id));
    if (aggregate->_distinct) {
        MOT_LOG_APPEND(MOT::LogLevel::LL_TRACE, ")");
    }
    MOT_LOG_END(MOT::LogLevel::LL_TRACE);
}

static void ExplainRangeSelectPlan(Query* query, JitRangeSelectPlan* plan)
{
    MOT_LOG_TRACE("[Plan] Range SELECT from table %s:", plan->_index_scan._table->GetTableName().c_str());
    int indent = 0;
    if (plan->_aggregate._aggreaget_op != JIT_AGGREGATE_NONE) {
        indent += 2;
        ExplainAggregateOperator(indent, &plan->_aggregate);
    }
    if (plan->_limit_count > 0) {
        indent += 2;
        MOT_LOG_TRACE("%*sLIMIT %d", indent, "", plan->_limit_count);
    }
    if (plan->_aggregate._aggreaget_op == JIT_AGGREGATE_NONE) {
        indent += 2;
        MOT_LOG_BEGIN(MOT::LogLevel::LL_TRACE, "%*sSELECT", indent, "");
        ExplainSelectExprArray(query, &plan->_select_exprs);
        MOT_LOG_END(MOT::LogLevel::LL_TRACE);
    }

    ExplainIndexScan(query, indent + 2, &plan->_index_scan);
}

static void ExplainRangeScanPlan(Query* query, JitRangeScanPlan* plan)
{
    if (plan->_command_type == JIT_COMMAND_UPDATE) {
        ExplainRangeUpdatePlan(query, (JitRangeUpdatePlan*)plan);
    } else if (plan->_command_type == JIT_COMMAND_SELECT) {
        ExplainRangeSelectPlan(query, (JitRangeSelectPlan*)plan);
    } else {
        MOT_LOG_TRACE("Invalid plan command type");
        return;
    }
}

static const char* JitJoinScanTypeToString(JitJoinScanType scanType)
{
    switch (scanType) {
        case JIT_JOIN_SCAN_POINT:
            return "Point-Join";
        case JIT_JOIN_SCAN_OUTER_POINT:
            return "Outer-Point-Join";
        case JIT_JOIN_SCAN_INNER_POINT:
            return "Inner-Point-Join";
        case JIT_JOIN_SCAN_RANGE:
            return "Range-Join";
        case JIT_JOIN_SCAN_INVALID:
        default:
            return "Invalid-Join-Scan";
    }
}

static void ExplainJoinPlan(Query* query, JitJoinPlan* plan)
{
    MOT_LOG_TRACE("[Plan] JOIN table %s on table %s (%s)",
        plan->_outer_scan._table->GetTableName().c_str(),
        plan->_inner_scan._table->GetTableName().c_str(),
        JitJoinScanTypeToString(plan->_scan_type));
    int indent = 0;
    if (plan->_aggregate._aggreaget_op != JIT_AGGREGATE_NONE) {
        indent += 2;
        ExplainAggregateOperator(indent, &plan->_aggregate);
    } else {
        indent += 2;
        MOT_LOG_BEGIN(MOT::LogLevel::LL_TRACE, "%*sSELECT", indent, "");
        ExplainSelectExprArray(query, &plan->_select_exprs);
        MOT_LOG_END(MOT::LogLevel::LL_TRACE);
    }
    if (plan->_limit_count > 0) {
        indent += 2;
        MOT_LOG_TRACE("%*sLIMIT %d", indent, "", plan->_limit_count);
    }
    ExplainIndexScan(query, indent + 2, &plan->_outer_scan, "OUTER ");
    ExplainIndexScan(query, indent + 2, &plan->_inner_scan, "INNER ");
}

extern void JitExplainPlan(Query* query, JitPlan* plan)
{
    if (plan != nullptr) {
        switch (plan->_plan_type) {
            case JIT_PLAN_INSERT_QUERY:
                ExplainInsertPlan(query, (JitInsertPlan*)plan);
                break;

            case JIT_PLAN_POINT_QUERY:
                ExplainPointQueryPlan(query, (JitPointQueryPlan*)plan);
                break;

            case JIT_PLAN_RANGE_SCAN:
                ExplainRangeScanPlan(query, (JitRangeScanPlan*)plan);
                break;

            case JIT_PLAN_JOIN:
                ExplainJoinPlan(query, (JitJoinPlan*)plan);
                break;

            case JIT_PLAN_INVALID:
            default:
                break;
        }
    }
}
}  // namespace JitExec
