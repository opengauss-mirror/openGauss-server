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
 * jit_plan.cpp
 *    JIT execution plan generation.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/jit_exec/jit_plan.cpp
 *
 * -------------------------------------------------------------------------
 */

/*
 * ATTENTION: Be sure to include global.h before postgres.h to avoid conflict between libintl.h (included in global.h)
 * and c.h (included in postgres.h).
 */
#include "global.h"
#include "jit_plan.h"
#include "jit_common.h"
#include "mm_session_api.h"
#include "utilities.h"
#include "nodes/pg_list.h"
#include "catalog/pg_aggregate.h"

#include <algorithm>

namespace JitExec {
DECLARE_LOGGER(JitPlan, JitExec)

// Count all leaves in parsed WHERE clause tree, that belong to the given table/index
// and have an EQUALS operator
static bool countWhereClauseEqualsLeaves(Query* query, MOT::Table* table, MOT::Index* index, int* count)
{
    if (MOT_CHECK_LOG_LEVEL(MOT::LogLevel::LL_TRACE)) {
        MOT_LOG_BEGIN(MOT::LogLevel::LL_TRACE,
            "Counting WHERE clause leaf nodes with EQUALS operator for table %s and index %s(",
            table->GetTableName().c_str(),
            index->GetName().c_str());
        for (int i = 0; i < index->GetNumFields(); ++i) {
            int columnId = index->GetColumnKeyFields()[i];
            if (columnId >= 0) {
                MOT_LOG_APPEND(MOT::LogLevel::LL_TRACE, "%s", table->GetFieldName(columnId));
            } else {
                MOT_LOG_APPEND(MOT::LogLevel::LL_TRACE, "N/A [column %d]", columnId);
            }
            if ((i + 1) < index->GetNumFields()) {
                MOT_LOG_APPEND(MOT::LogLevel::LL_TRACE, ", ");
            }
        }
        MOT_LOG_APPEND(MOT::LogLevel::LL_TRACE, ")");
        MOT_LOG_END(MOT::LogLevel::LL_TRACE);
    }

    ExpressionCounter expr_counter(count);
    Node* quals = query->jointree->quals;
    bool result = true;
    if (quals == nullptr) {  // no WHERE clause
        *count = 0;
    } else {
        result = visitSearchExpressions(query, table, index, (Expr*)&quals[0], true, &expr_counter, false);
    }
    if (!result) {
        MOT_LOG_TRACE("Failed to count WHERE clause EQUALS leaf nodes");
    } else {
        MOT_LOG_TRACE("Found %d EQUALS leaf nodes in WHERE clause", *count);
    }
    return result;
}

#define checkJittableAttribute(query, attr)                \
    if (query->attr) {                                     \
        MOT_LOG_TRACE("Query is not jittable: " #attr ""); \
        return false;                                      \
    }

#define checkJittableClause(query, clause)                                       \
    if (query->clause != nullptr) {                                              \
        MOT_LOG_TRACE("Query is not jittable: " #clause " clause is not empty"); \
        return false;                                                            \
    }

static bool CheckQueryAttributes(const Query* query, bool allowSorting, bool allowAggregate, bool allowSublink)
{
    checkJittableAttribute(query, hasWindowFuncs);
    checkJittableAttribute(query, hasDistinctOn);
    checkJittableAttribute(query, hasRecursive);
    checkJittableAttribute(query, hasModifyingCTE);

    checkJittableClause(query, returningList);
    checkJittableClause(query, groupClause);
    checkJittableClause(query, groupingSets);
    checkJittableClause(query, havingQual);
    checkJittableClause(query, windowClause);
    checkJittableClause(query, distinctClause);
    checkJittableClause(query, setOperations);
    checkJittableClause(query, constraintDeps);

    if (!allowSorting) {
        checkJittableClause(query, sortClause);
    }

    if (!allowAggregate) {
        checkJittableAttribute(query, hasAggs);
    }

    if (!allowSublink) {
        checkJittableAttribute(query, hasSubLinks);
    }

    return true;
}

static bool allocExprArray(JitColumnExprArray* expr_array, int expr_count)
{
    bool result = false;
    size_t alloc_size = expr_count * sizeof(JitColumnExpr);
    expr_array->_exprs = (JitColumnExpr*)MOT::MemSessionAlloc(alloc_size);
    if (!expr_array->_exprs) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Prepare JIT plan",
            "Failed to allocate %u bytes for %d expressions",
            (unsigned)alloc_size,
            expr_count);
    } else {
        errno_t erc = memset_s(expr_array->_exprs, alloc_size, 0, alloc_size);
        securec_check(erc, "\0", "\0");
        expr_array->_count = expr_count;
        result = true;
    }
    return result;
}

static void freeExprArray(JitColumnExprArray* expr_array)
{
    if (expr_array->_exprs != nullptr) {
        for (int i = 0; i < expr_array->_count; ++i) {
            if (expr_array->_exprs[i]._expr) {
                freeExpr(expr_array->_exprs[i]._expr);
            }
        }
        MOT::MemSessionFree(expr_array->_exprs);
        expr_array->_exprs = nullptr;
    }
}

static bool allocSelectExprArray(JitSelectExprArray* expr_array, int expr_count)
{
    bool result = false;
    size_t alloc_size = expr_count * sizeof(JitSelectExpr);
    expr_array->_exprs = (JitSelectExpr*)MOT::MemSessionAlloc(alloc_size);
    if (!expr_array->_exprs) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Prepare JIT plan",
            "Failed to allocate %u bytes for %d select expressions",
            (unsigned)alloc_size,
            expr_count);
    } else {
        errno_t erc = memset_s(expr_array->_exprs, alloc_size, 0, alloc_size);
        securec_check(erc, "\0", "\0");
        expr_array->_count = expr_count;
        result = true;
    }
    return result;
}

static void freeSelectExprArray(JitSelectExprArray* expr_array)
{
    if (expr_array->_exprs != nullptr) {
        for (int i = 0; i < expr_array->_count; ++i) {
            if (expr_array->_exprs[i]._column_expr) {
                freeExpr((JitExpr*)expr_array->_exprs[i]._column_expr);
            }
        }
        MOT::MemSessionFree(expr_array->_exprs);
        expr_array->_exprs = nullptr;
    }
}

static bool prepareSearchExpressions(Query* query, MOT::Table* table, MOT::Index* index, JitColumnExprArray* expr_array)
{
    MOT_LOG_TRACE("Preparing search expressions");
    bool result = false;
    int expr_count = index->GetNumFields();
    if (!allocExprArray(expr_array, expr_count)) {
        MOT_LOG_TRACE("Failed to allocate expression array with %d items", expr_count);
    } else {
        // retrieve search expressions
        expr_count = 0;
        if (!getSearchExpressions(query, table, index, true, expr_array, &expr_count, false)) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Prepare JIT plan", "Failed to collect search expressions");
            MOT::MemSessionFree(expr_array->_exprs);
            expr_array->_exprs = nullptr;
        } else {
            result = true;
        }
    }
    return result;
}

static int getNonJunkTargetEntryCount(const Query* query)
{
    int count = 0;
    ListCell* lc = nullptr;

    foreach (lc, query->targetList) {
        TargetEntry* target_entry = (TargetEntry*)lfirst(lc);
        if (!target_entry->resjunk) {
            ++count;
        }
    }
    return count;
}

static bool prepareTargetExpressions(Query* query, JitColumnExprArray* expr_array)
{
    MOT_LOG_TRACE("Preparing target expressions");
    bool result = false;
    int expr_count = getNonJunkTargetEntryCount(query);
    MOT_LOG_TRACE("Counted %d non-junk target expressions", expr_count);
    if (!allocExprArray(expr_array, expr_count)) {
        MOT_LOG_TRACE("Failed to allocate expression array with %d items", expr_count);
    } else {
        // retrieve search expressions
        if (!getTargetExpressions(query, expr_array)) {
            MOT_LOG_TRACE("Failed to collect target expressions");
            MOT::MemSessionFree(expr_array->_exprs);
            expr_array->_exprs = nullptr;
        } else {
            result = true;
        }
    }
    return result;
}

static bool prepareSelectExpressions(Query* query, JitSelectExprArray* expr_array)
{
    MOT_LOG_TRACE("Preparing target expressions");
    bool result = false;
    int expr_count = getNonJunkTargetEntryCount(query);
    MOT_LOG_TRACE("Counted %d non-junk target expressions", expr_count);
    if (!allocSelectExprArray(expr_array, expr_count)) {
        MOT_LOG_TRACE("Failed to allocate select expression array with %d items", expr_count);
    } else {
        // retrieve search expressions
        if (!getSelectExpressions(query, expr_array)) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Prepare JIT plan", "Failed to collect target expressions");
            MOT::MemSessionFree(expr_array->_exprs);
            expr_array->_exprs = nullptr;
        } else {
            result = true;
        }
    }
    return result;
}

static bool prepareRangeSearchExpressions(
    Query* query, MOT::Table* table, MOT::Index* index, JitIndexScan* index_scan, JoinClauseType join_clause_type)
{
    MOT_LOG_TRACE("Preparing range search expressions");
    bool result = false;
    int expr_count = index->GetNumFields() + 1;  // could be an open scan
    if (!allocExprArray(&index_scan->_search_exprs, expr_count)) {
        MOT_LOG_TRACE("Failed to allocate expression array with %d items", expr_count);
    } else {
        // retrieve search expressions
        result = getRangeSearchExpressions(query, table, index, index_scan, join_clause_type);
        if (!result) {
            MOT_LOG_TRACE("Failed to collect range search expressions");
            freeExprArray(&index_scan->_search_exprs);
        } else {
            if (index_scan->_scan_type == JIT_INDEX_SCAN_TYPE_INVALID) {
                MOT_LOG_TRACE("prepareRangeSearchExpressions(): Disqualifying query - invalid range scan type");
                result = false;
            }
        }
    }
    return result;
}

// Count all leaves in parsed WHERE clause tree, that belong to the given table
// but do not refer to the index columns
static bool countFilters(Query* query, MOT::Table* table, MOT::Index* index, int* count, JitColumnExprArray* pkey_exprs)
{
    MOT_LOG_TRACE("Counting filters for table %s, index %s", table->GetTableName().c_str(), index->GetName().c_str());
    FilterCounter filter_counter(count);
    Node* quals = query->jointree->quals;
    bool result = true;
    if (quals == nullptr) {
        *count = 0;
    } else {
        result =
            visitSearchExpressions(query, table, index, (Expr*)&quals[0], false, &filter_counter, false, pkey_exprs);
    }
    if (!result) {
        MOT_LOG_TRACE("Failed to count number of filters");
    } else {
        MOT_LOG_TRACE("Found %d filters", *count);
    }
    return result;
}

static bool allocFilterArray(JitFilterArray* filter_array, int filter_count)
{
    bool result = false;
    size_t alloc_size = filter_count * sizeof(JitFilter);
    filter_array->_scan_filters = (JitFilter*)MOT::MemSessionAlloc(alloc_size);
    if (filter_array->_scan_filters == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Prepare JIT plan",
            "Failed to allocate %u bytes for %d filters",
            (unsigned)alloc_size,
            filter_count);
    } else {
        errno_t erc = memset_s(filter_array->_scan_filters, alloc_size, 0, alloc_size);
        securec_check(erc, "\0", "\0");
        filter_array->_filter_count = filter_count;
        result = true;
    }
    return result;
}

static void freeFilterArray(JitFilterArray* filter_array)
{
    if (filter_array->_scan_filters != nullptr) {
        for (int i = 0; i < filter_array->_filter_count; ++i) {
            freeExpr(filter_array->_scan_filters[i]._lhs_operand);
            freeExpr(filter_array->_scan_filters[i]._rhs_operand);
        }
        MOT::MemSessionFree(filter_array->_scan_filters);
        filter_array->_scan_filters = nullptr;
    }
}

static bool getFilters(Query* query, MOT::Table* table, MOT::Index* index, JitFilterArray* filter_array, int* count,
    JitColumnExprArray* pkey_exprs)
{
    MOT_LOG_TRACE("Retrieving filters for table %s, index %s", table->GetTableName().c_str(), index->GetName().c_str());
    FilterCollector filter_collector(query, filter_array, count);
    Node* quals = query->jointree->quals;
    bool result = true;
    if (quals == nullptr) {
        *count = 0;
    } else {
        result =
            visitSearchExpressions(query, table, index, (Expr*)&quals[0], false, &filter_collector, false, pkey_exprs);
    }
    if (!result) {
        MOT_LOG_TRACE("Failed to retrieve filters");
    } else {
        MOT_LOG_TRACE("Collected %d filters", *count);
    }
    return result;
}

static bool prepareFilters(
    Query* query, MOT::Table* table, MOT::Index* index, JitFilterArray* filter_array, JitColumnExprArray* pkey_exprs)
{
    MOT_LOG_TRACE(
        "Preparing search filters on table %s, index %s", table->GetTableName().c_str(), index->GetName().c_str());
    bool result = false;

    // retrieve filters
    int filter_count = 0;
    if (!countFilters(query, table, index, &filter_count, pkey_exprs)) {
        MOT_LOG_TRACE("Failed to count filter expressions");
    } else if (filter_count == 0) {
        result = true;
    } else {
        // allocate filter array
        result = allocFilterArray(filter_array, filter_count);
        if (!result) {
            MOT_LOG_TRACE("Failed to allocate filter array with %d items", filter_count);
        } else {
            int collected_count = 0;
            result = getFilters(query, table, index, filter_array, &collected_count, pkey_exprs);
            if (!result) {
                MOT_LOG_TRACE("Failed to collect filters");
                freeFilterArray(filter_array);
            } else {
                MOT_LOG_TRACE("Collected %d filters", collected_count);
            }
        }
    }

    return result;
}

static TargetEntry* getRefTargetEntry(const List* targetList, int ref_index)
{
    TargetEntry* result = nullptr;
    ListCell* lc = nullptr;

    foreach (lc, targetList) {
        TargetEntry* te = (TargetEntry*)lfirst(lc);
        if (te->ressortgroupref == (unsigned)ref_index) {
            result = te;
            break;
        }
    }
    return result;
}

static bool getSortClauseColumns(
    Query* query, MOT::Table* table, MOT::Index* index, int* column_array, int key_column_count, int* column_count)
{
    *column_count = 0;
    ListCell* lc = nullptr;

    foreach (lc, query->sortClause) {
        SortGroupClause* sgc = (SortGroupClause*)lfirst(lc);
        if (sgc->groupSet) {
            MOT_LOG_TRACE("getSortClauseColumns(): Found groupSet flag in sort group clause");
            return false;
        }

        int te_index = sgc->tleSortGroupRef;
        TargetEntry* te = getRefTargetEntry(query->targetList, te_index);
        if (te == nullptr) {
            MOT_LOG_TRACE("getSortClauseColumns(): Cannot find TargetEntry by ref-index %d", te_index);
            return false;
        }

        if (te->expr->type != T_Var) {
            MOT_LOG_TRACE("getSortClauseColumns(): TargetEntry sub-expression is not Var expression");
            return false;
        }

        int table_column_id = ((Var*)te->expr)->varattno;
        int index_column_id = MapTableColumnToIndex(table, index, table_column_id);
        if (index_column_id >= key_column_count) {
            MOT_LOG_TRACE("getSortClauseColumns(): Disqualifying query - ORDER BY clause references invalid index "
                          "column %d from table column %d",
                index_column_id,
                table_column_id);
            return false;
        }
        MOT_LOG_TRACE(
            "getSortClauseColumns(): Found table column id %d, index column id %d", table_column_id, index_column_id);
        column_array[*column_count] = index_column_id;
        ++(*column_count);
    }

    return true;
}

static bool isJittableSortClause(Query* query, MOT::Table* table, MOT::Index* index, int prefix_column_count)
{
    // the ordered-by columns is an ordered list that must exhibit the following properties:
    // 1. columns must appear in order of index column
    // 2. it must contain a full prefix of the missing columns in the WHERE clause full prefix
    // This means that the ORDERED BY clause may contain some redundant column already found in the WHERE clause,
    // even with "holes", but they must be in the same order of the index columns.
    int key_column_count = index->GetNumFields();
    int* column_array = (int*)calloc(key_column_count, sizeof(int));
    if (column_array == nullptr) {
        MOT_LOG_TRACE("isJittableSortClause(): Disqualifying query - memory allocation failed");
        return false;
    }

    // traverse the sort clause of the query and compose
    int column_count = 0;
    if (!getSortClauseColumns(query, table, index, column_array, key_column_count, &column_count)) {
        free(column_array);
        return false;
    }

    // now check all columns are in order (according to index order)
    for (int i = 1; i < column_count; ++i) {
        if (column_array[i] < column_array[i - 1]) {
            MOT_LOG_TRACE("isJittableSortClause(): Disqualifying query - ORDER BY clause mismatches index %s order",
                index->GetName().c_str());
            free(column_array);
            return false;
        }
    }

    // now check that sort clause columns contains a full-prefix of columns not in WHERE clause (i.e. no "holes" in
    // excess columns)
    int column_id = 0;
    while ((column_id < column_count) && (column_array[column_id] < prefix_column_count)) {
        ++column_id;
    }
    int last_excess_column = prefix_column_count;
    while (column_id < column_count) {
        if (column_array[column_id] != last_excess_column) {
            MOT_LOG_TRACE("isJittableSortClause(): Disqualifying query - ORDER BY clause does not contain full-prefix "
                          "of missing columns in WHERE clause (hole found at column %d)",
                column_id);
            free(column_array);
            return false;
        }
        ++last_excess_column;
        ++column_id;
    }

    free(column_array);
    return true;
}

static bool isPlanSortOrderValid(Query* query, JitRangeSelectPlan* plan)
{
    bool result = false;

    if (query->sortClause == nullptr) {
        // not having a sort clause is fine
        result = true;
    } else {
        MOT::Index* index = plan->_index_scan._table->GetIndex(plan->_index_scan._index_id);
        result = isJittableSortClause(query, plan->_index_scan._table, index, plan->_index_scan._column_count);
        if (!result) {
            MOT_LOG_TRACE("isPlanSortOrderValid(): Disqualifying query - Sort clause is not jittable")
        }
    }

    return result;
}

static int evalConstExpr(Expr* expr)
{
    int result = -1;

    // we expect to see either a Const or a cast to int8 of a Const
    Const* const_expr = nullptr;
    if (expr->type == T_Const) {
        MOT_LOG_TRACE("evalConstExpr(): Found direct const expression");
        const_expr = (Const*)expr;
    } else if (expr->type == T_FuncExpr) {
        FuncExpr* func_expr = (FuncExpr*)expr;
        if (func_expr->funcid == 481) {  // cast to int8
            Expr* sub_expr = (Expr*)linitial(func_expr->args);
            if (sub_expr->type == T_Const) {
                MOT_LOG_TRACE("evalConstExpr(): Found const expression within cast to int8 function expression");
                const_expr = (Const*)sub_expr;
            }
        }
    }

    if (const_expr != nullptr) {
        // extract integer value
        result = const_expr->constvalue;
        MOT_LOG_TRACE("evalConstExpr(): Expression evaluated to constant value %d", result);
    } else {
        MOT_LOG_TRACE("evalConstExpr(): Could not infer const expression");
    }

    return result;
}

static bool getLimitCount(Query* query, int* limit_count)
{
    bool result = false;
    if (query->limitOffset) {
        MOT_LOG_TRACE("getLimitCount(): invalid limit clause - encountered limitOffset clause");
    } else if (query->limitCount) {
        *limit_count = evalConstExpr((Expr*)query->limitCount);
        result = true;
    } else {
        *limit_count = 0;  // no limit clause
        result = true;
    }
    return result;
}

static inline bool isValidAggregateResultType(int restype)
{
    bool result = false;
    if ((restype == INT1OID) || (restype == INT2OID) || (restype == INT4OID) || (restype == INT8OID) ||
        (restype == FLOAT4OID) || (restype == FLOAT8OID) || (restype == NUMERICOID)) {
        result = true;
    }
    return result;
}

static bool isAvgAggregateOperator(int funcid)
{
    bool result = false;
    if ((funcid == INT8AVGFUNCOID) /* int8_avg_accum */ || (funcid == INT4AVGFUNCOID) /* int4_avg_accum */ ||
        (funcid == INT2AVGFUNCOID) /* int2_avg_accum */ || (funcid == 5537) /* int1_avg_accum */ ||
        (funcid == 2104) /* float4_accum */ || (funcid == 2105) /* float8_accum */ ||
        (funcid == NUMERICAVGFUNCOID) /* numeric_avg_accum */) {
        result = true;
    }
    return result;
}

static bool isSumAggregateOperator(int funcid)
{
    bool result = false;
    if ((funcid == INT8SUMFUNCOID) /* int8_sum */ || (funcid == INT4SUMFUNCOID) /* int4_sum */ ||
        (funcid == INT2SUMFUNCOID) /* int2_sum */ || (funcid == 2110) /* float4pl */ ||
        (funcid == 2111) /* float8pl */ || (funcid == NUMERICSUMFUNCOID) /* numeric_sum */) {
        result = true;
    }
    return result;
}

static bool isMaxAggregateOperator(int funcid)
{
    bool result = false;
    if ((funcid == INT8LARGERFUNCOID) /* int8larger */ || (funcid == INT4LARGERFUNCOID) /* int4larger */ ||
        (funcid == INT2LARGERFUNCOID) /* int2larger */ || (funcid == 5538) /* int1larger */ ||
        (funcid == 2119) /* float4larger */ || (funcid == 2120) /* float8larger */ ||
        (funcid == NUMERICLARGERFUNCOID) /* numeric_larger */ || (funcid == 2126) /* timestamp_larger */ ||
        (funcid == 2122) /* date_larger */ || (funcid == 2244) /* bpchar_larger */ ||
        (funcid == 2129) /* text_larger */) {
        result = true;
    }
    return result;
}

static bool isMinAggregateOperator(int funcid)
{
    bool result = false;
    if ((funcid == INT8SMALLERFUNCOID) /* int8smaller */ || (funcid == INT4SMALLERFUNCOID) /* int4smaller */ ||
        (funcid == INT2SMALLERFUNCOID) /* int2smaller */ ||  // not int1 function was found for MIN operator
        (funcid == 2135) /* float4smaller */ || (funcid == 2136) /* float8smaller */ ||
        (funcid == NUMERICSMALLERFUNCOID) /* numeric_smaller */ || (funcid == 2142) /* timestamp_smaller */ ||
        (funcid == 2138) /* date_smaller */ || (funcid == 2245) /* bpchar_smaller */ ||
        (funcid == 2145) /* text_smaller */) {
        result = true;
    }
    return result;
}

static bool isCountAggregateOperator(int funcid)
{
    bool result = false;
    if ((funcid == 2147) /* int8inc_any */ || (funcid == 2803) /* int8inc */) {
        result = true;
    }
    return result;
}

static JitAggregateOperator classifyAggregateOperator(int funcid)
{
    JitAggregateOperator result = JIT_AGGREGATE_NONE;
    if (isAvgAggregateOperator(funcid)) {
        result = JIT_AGGREGATE_AVG;
    } else if (isSumAggregateOperator(funcid)) {
        result = JIT_AGGREGATE_SUM;
    } else if (isMaxAggregateOperator(funcid)) {
        result = JIT_AGGREGATE_MAX;
    } else if (isMinAggregateOperator(funcid)) {
        result = JIT_AGGREGATE_MIN;
    } else if (isCountAggregateOperator(funcid)) {
        result = JIT_AGGREGATE_COUNT;
    }
    return result;
}

static int classifyAggregateAvgType(int funcid, int* element_count)
{
    int element_type = -1;

    switch (funcid) {
        case INT8AVGFUNCOID:
        case NUMERICAVGFUNCOID:
            // the current_aggregate is a 2 numeric array
            element_type = NUMERICOID;
            *element_count = 2;
            break;

        case INT4AVGFUNCOID:
        case INT2AVGFUNCOID:
        case 5537:  // int1 avg
            // the current_aggregate is a 2 int8 array
            element_type = INT8OID;
            *element_count = 2;
            break;

        case 2104:  // float4
        case 2105:  // float8
            // the current_aggregate is a 3 float8 array
            element_type = FLOAT8OID;
            *element_count = 3;
            break;

        default:
            MOT_LOG_TRACE("Unsupported aggregate AVG() operator function type: %d", funcid);
            break;
    }

    return element_type;
}

static inline bool isValidAggregateFunction(int funcid)
{
    bool result = false;
    // check for aggregate_dummy leading to numeric_add and others (see src/include/catalog/pg_proc.h and
    // pg_aggregate.h)
    if (classifyAggregateOperator(funcid) != JIT_AGGREGATE_NONE) {
        result = true;
    }
    return result;
}

static inline bool isValidAggregateDistinctClause(const List* agg_distinct)
{
    bool result = false;

    if (agg_distinct == nullptr) {
        result = true;
    } else if (list_length(agg_distinct) != 1) {
        MOT_LOG_TRACE("Invalid DISTINCIT specifier with more than one sort clause");
    } else {
        SortGroupClause* sgc = (SortGroupClause*)linitial(agg_distinct);
        if (sgc->groupSet) {
            MOT_LOG_TRACE("Invalid DISTINCIT specifier with group-set flag");
        } else {
            result = true;
        }
    }

    return result;
}

static bool getTargetEntryAggregateOperator(Query* query, TargetEntry* target_entry, JitAggregate* aggregate)
{
    bool result = false;

    Aggref* agg_ref = (Aggref*)target_entry->expr;
    if (agg_ref->aggorder) {
        MOT_LOG_TRACE("getTargetEntryAggregateOperator(): Unsupported aggregate operator with ORDER BY specifiers");
    } else if (!isValidAggregateFunction(agg_ref->aggfnoid)) {
        MOT_LOG_TRACE("getTargetEntryAggregateOperator(): Unsupported aggregate operator %d", agg_ref->aggfnoid);
    } else if (!isValidAggregateResultType(agg_ref->aggtype)) {
        MOT_LOG_TRACE("getTargetEntryAggregateOperator(): Unsupported aggregate result type %d", agg_ref->aggtype);
    } else if (!isValidAggregateDistinctClause(agg_ref->aggdistinct)) {
        MOT_LOG_TRACE("getTargetEntryAggregateOperator(): Unsupported aggregate distinct clause");
    } else if (list_length(agg_ref->args) != 1) {
        MOT_LOG_TRACE(
            "getTargetEntryAggregateOperator(): Unsupported aggregate argument list with length unequal to 1");
    } else {
        TargetEntry* sub_te = (TargetEntry*)linitial(agg_ref->args);
        if (sub_te->expr->type != T_Var) {
            MOT_LOG_TRACE("getTargetEntryAggregateOperator(): Unsupported aggregate operator with non-column argument");
        } else {
            Var* var_expr = (Var*)sub_te->expr;
            int result_type = var_expr->vartype;
            if (!IsTypeSupported(result_type)) {
                MOT_LOG_TRACE("getTargetEntryAggregateOperator(): Unsupported aggregate operator with column type %d",
                    result_type);
            } else {
                MOT_LOG_TRACE("getTargetEntryAggregateOperator(): target entry for aggregate query is jittable");
                aggregate->_aggreaget_op = classifyAggregateOperator(agg_ref->aggfnoid);
                aggregate->_element_type = agg_ref->aggtype;
                if (aggregate->_aggreaget_op == JIT_AGGREGATE_AVG) {
                    aggregate->_avg_element_type =
                        classifyAggregateAvgType(agg_ref->aggfnoid, &aggregate->_avg_element_count);
                } else {
                    aggregate->_avg_element_type = -1;
                }
                aggregate->_func_id = agg_ref->aggfnoid;
                aggregate->_table = getRealTable(query, var_expr->varno, var_expr->varattno);
                aggregate->_table_column_id =
                    getRealColumnId(query, var_expr->varno, var_expr->varattno, aggregate->_table);
                aggregate->_distinct = (agg_ref->aggdistinct != nullptr) ? true : false;
                result = true;
            }
        }
    }

    return result;
}

static bool getAggregateOperator(Query* query, JitAggregate* aggregate)
{
    bool result = false;

    // if an aggregate operator is specified, then only one column can exist
    // so we check all target entries, and if one of them specifies an aggregate operator, then
    // it must be the only target entry in the query
    ListCell* lc = nullptr;

    bool aggregate_found = false;
    int entry_count = list_length(query->targetList);

    foreach (lc, query->targetList) {
        TargetEntry* target_entry = (TargetEntry*)lfirst(lc);
        if (target_entry->expr->type == T_Aggref) {
            // found an aggregate target entry
            aggregate_found = true;
            if (entry_count != 1) {
                MOT_LOG_TRACE(
                    "getAggregateOperator(): Disqualifying query - aggregate must specify only 1 target entry");
            }
            result = getTargetEntryAggregateOperator(query, target_entry, aggregate);
        }
    }

    if (!aggregate_found) {
        // it is fine not to have an aggregate clause
        result = true;
    }

    return result;
}

static double evaluatePlan(const JitRangeSelectPlan* plan)
{
    // currently the value of a range scan plan is how much it matches the used index
    MOT::Index* index = plan->_index_scan._table->GetIndex(plan->_index_scan._index_id);
    return ((double)plan->_index_scan._column_count) / ((double)index->GetNumFields());
}

static bool isJitPlanBetter(JitRangeSelectPlan* candidate_plan, JitRangeSelectPlan* current_plan)
{
    bool result = false;

    double candidate_plan_value = evaluatePlan(candidate_plan);
    double current_plan_value = evaluatePlan(current_plan);
    if (candidate_plan_value > current_plan_value) {
        result = true;
    }

    return result;
}

static JitPlan* JitPrepareInsertPlan(Query* query, MOT::Table* table)
{
    JitPlan* plan = nullptr;
    MOT_LOG_TRACE("Preparing INSERT plan for table %s", table->GetTableName().c_str());

    // Collect target expressions
    size_t alloc_size = sizeof(JitInsertPlan);
    JitInsertPlan* insert_plan = (JitInsertPlan*)MOT::MemSessionAlloc(alloc_size);
    if (insert_plan == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Prepare INSERT JIT plan",
            "Failed to allocate %u bytes for INSERT plan",
            (unsigned)alloc_size);
    } else {
        errno_t erc = memset_s(insert_plan, alloc_size, 0, alloc_size);
        securec_check(erc, "\0", "\0");
        insert_plan->_plan_type = JIT_PLAN_INSERT_QUERY;
        insert_plan->_table = table;

        // retrieve insert expressions
        if (!prepareTargetExpressions(query, &insert_plan->_insert_exprs)) {
            MOT_LOG_TRACE("Failed to collect INSERT expressions");
            JitDestroyPlan((JitPlan*)insert_plan);
        } else {
            plan = (JitPlan*)insert_plan;
        }
    }

    if (plan == nullptr) {
        MOT_LOG_TRACE("Failed to prepare INSERT plan");
    }
    return plan;
}

static JitPointQueryPlan* JitPreparePointQueryPlan(
    Query* query, MOT::Table* table, size_t alloc_size, JitCommandType command_type)
{
    MOT_LOG_TRACE("Preparing point query plan for table %s", table->GetTableName().c_str());
    JitPointQueryPlan* plan = (JitPointQueryPlan*)MOT::MemSessionAlloc(alloc_size);
    if (plan == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Prepare JIT plan",
            "Failed to allocate %u bytes for %s point query plan",
            (unsigned)alloc_size,
            CommandToString(command_type));
    } else {
        errno_t erc = memset_s(plan, alloc_size, 0, alloc_size);
        securec_check(erc, "\0", "\0");
        plan->_plan_type = JIT_PLAN_POINT_QUERY;
        plan->_command_type = command_type;
        plan->_query._table = table;

        // prepare search expressions and optional filters
        MOT::Index* index = table->GetPrimaryIndex();
        if (!prepareSearchExpressions(query, table, index, &plan->_query._search_exprs) ||
            !prepareFilters(query, table, index, &plan->_query._filters, &plan->_query._search_exprs)) {
            MOT_LOG_TRACE("Failed to prepare search expressions or filters");
            // cleanup
            JitDestroyPlan((JitPlan*)plan);
            plan = nullptr;
        }
    }

    if (plan == nullptr) {
        MOT_LOG_TRACE("Failed to prepare point query plan");
    }
    return plan;
}

static JitPlan* JitPrepareUpdatePlan(Query* query, MOT::Table* table)
{
    JitPlan* plan = nullptr;
    MOT_LOG_TRACE("Preparing UPDATE plan for table %s", table->GetTableName().c_str());

    // prepare basic point query plan
    size_t alloc_size = sizeof(JitUpdatePlan);
    JitUpdatePlan* update_plan = (JitUpdatePlan*)JitPreparePointQueryPlan(query, table, alloc_size, JIT_COMMAND_UPDATE);
    if (update_plan != nullptr) {
        // retrieve update expression (similar to insert expressions)
        if (!prepareTargetExpressions(query, &update_plan->_update_exprs)) {
            MOT_LOG_TRACE("Failed to prepare target expressions");
            JitDestroyPlan((JitPlan*)plan);
        } else {
            plan = (JitPlan*)update_plan;
        }
    }

    if (plan == nullptr) {
        MOT_LOG_TRACE("Failed to prepare UPDATE plan");
    }
    return plan;
}

static JitPlan* JitPrepareDeletePlan(Query* query, MOT::Table* table)
{
    MOT_LOG_TRACE("Preparing DELETE plan for table %s", table->GetTableName().c_str());

    // prepare basic point query plan
    size_t alloc_size = sizeof(JitDeletePlan);
    JitDeletePlan* plan = (JitDeletePlan*)JitPreparePointQueryPlan(query, table, alloc_size, JIT_COMMAND_DELETE);
    if (plan == nullptr) {
        MOT_LOG_TRACE("Failed to prepare DELETE plan");
    }
    return (JitPlan*)plan;
}

static JitPlan* JitPrepareSelectPlan(Query* query, MOT::Table* table)
{
    JitPlan* plan = nullptr;
    MOT_LOG_TRACE("Preparing SELECT plan for table %s", table->GetTableName().c_str());

    // prepare basic point query plan
    size_t alloc_size = sizeof(JitSelectPlan);
    JitSelectPlan* select_plan = (JitSelectPlan*)JitPreparePointQueryPlan(query, table, alloc_size, JIT_COMMAND_SELECT);
    if (select_plan != nullptr) {
        if (!prepareSelectExpressions(query, &select_plan->_select_exprs)) {
            MOT_LOG_TRACE("Failed to prepare target expressions");
            JitDestroyPlan((JitPlan*)plan);
        } else {
            plan = (JitPlan*)select_plan;
        }
    }

    if (plan == nullptr) {
        MOT_LOG_TRACE("Failed to prepare SELECT plan");
    }
    return plan;
}

static JitRangeScanPlan* JitPrepareRangeScanPlan(Query* query, MOT::Table* table, int index_id, size_t alloc_size,
    JitCommandType command_type, JoinClauseType join_clause_type)
{
    MOT::Index* index = table->GetIndex(index_id);
    MOT_LOG_TRACE("Preparing Range Scan plan for table %s, index %d (%s)",
        table->GetTableName().c_str(),
        index_id,
        index->GetName().c_str());
    JitRangeScanPlan* plan = (JitRangeScanPlan*)MOT::MemSessionAlloc(alloc_size);
    if (plan == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Prepare JIT plan",
            "Failed to allocate %u bytes for %s range scan plan",
            (unsigned)alloc_size,
            CommandToString(command_type));
    } else {
        errno_t erc = memset_s(plan, alloc_size, 0, alloc_size);
        securec_check(erc, "\0", "\0");
        plan->_plan_type = JIT_PLAN_RANGE_SCAN;
        plan->_command_type = command_type;
        plan->_index_scan._table = table;

        // prepare search expressions and optional filters
        plan->_index_scan._index_id = index_id;
        if (!prepareRangeSearchExpressions(query, table, index, &plan->_index_scan, join_clause_type) ||
            !prepareFilters(query, table, index, &plan->_index_scan._filters, &plan->_index_scan._search_exprs)) {
            MOT_LOG_TRACE("Failed to prepare search or filters expressions");
            // cleanup
            JitDestroyPlan((JitPlan*)plan);
            plan = nullptr;
        }
    }

    if (plan == nullptr) {
        MOT_LOG_TRACE("Failed to prepare Range scan plan");
    }
    return plan;
}

static JitPlan* JitPrepareRangeUpdatePlan(Query* query, MOT::Table* table)
{
    MOT_LOG_TRACE("Preparing range UPDATE plan for table %s", table->GetTableName().c_str());
    size_t alloc_size = sizeof(JitRangeUpdatePlan);
    int index_id = 0;  // primary index
    JitRangeUpdatePlan* plan = (JitRangeUpdatePlan*)JitPrepareRangeScanPlan(
        query, table, index_id, alloc_size, JIT_COMMAND_UPDATE, JoinClauseNone);
    if (plan == nullptr) {
        MOT_LOG_TRACE("Failed to prepare Range UPDATE plan");
    } else {
        if (!prepareTargetExpressions(query, &plan->_update_exprs)) {
            MOT_LOG_TRACE("Failed to prepare target expressions");
            // cleanup
            JitDestroyPlan((JitPlan*)plan);
            plan = nullptr;
        } else {
            plan->_index_scan._sort_order = JIT_QUERY_SORT_ASCENDING;
            plan->_index_scan._scan_direction = JIT_INDEX_SCAN_FORWARD;
        }
    }
    return (JitPlan*)plan;
}

static JitPlan* JitPrepareRangeSelectPlan(Query* query, MOT::Table* table, JoinClauseType join_clause_type)
{
    MOT_LOG_TRACE("Preparing range SELECT plan for table %s", table->GetTableName().c_str());

    JitRangeSelectPlan* plan = nullptr;
    bool clean_plan = false;

    // the limit count and aggregation can be inferred regardless of plan
    int limit_count = 0;
    JitAggregate aggregate = {JIT_AGGREGATE_NONE, 0, 0, nullptr, 0, 0, 0, false};
    if (!getLimitCount(query, &limit_count) || !getAggregateOperator(query, &aggregate)) {
        MOT_LOG_TRACE(
            "JitPrepareRangeSelectPlan(): Disqualifying query - unsupported scan limit count or aggregate operation");
        return nullptr;
    }

    // now we search for the best index/plan
    bool has_aggregate = (aggregate._aggreaget_op != JIT_AGGREGATE_NONE);
    size_t alloc_size = sizeof(JitRangeSelectPlan);

    for (int index_id = 0; index_id < (int)table->GetNumIndexes(); ++index_id) {
        MOT_LOG_TRACE("Attempting to prepare plan with index %d", index_id);
        JitRangeSelectPlan* next_plan = (JitRangeSelectPlan*)JitPrepareRangeScanPlan(
            query, table, index_id, alloc_size, JIT_COMMAND_SELECT, join_clause_type);

        if (next_plan == nullptr) {
            MOT_LOG_TRACE(
                "Failed to prepare range select plan with index %d: failed to prepare range scan plan", index_id);
            clean_plan = true;
            break;
        }

        if (!has_aggregate && !prepareSelectExpressions(query, &next_plan->_select_exprs)) {
            MOT_LOG_TRACE(
                "Failed to prepare range select plan with index %d: failed to prepare select expressions", index_id);
            JitDestroyPlan((JitPlan*)next_plan);
            clean_plan = true;
            break;
        }

        // verify sort order is valid (if one is specified)
        if (!isPlanSortOrderValid(query, next_plan)) {
            MOT_LOG_TRACE("Disqualifying plan - Query sort order is incompatible with index");
            JitDestroyPlan((JitPlan*)next_plan);
        } else {
            next_plan->_index_scan._sort_order = GetQuerySortOrder(query);
            next_plan->_index_scan._scan_direction = (next_plan->_index_scan._sort_order == JIT_QUERY_SORT_ASCENDING)
                                                         ? JIT_INDEX_SCAN_FORWARD
                                                         : JIT_INDEX_SCAN_BACKWARDS;
            next_plan->_limit_count = limit_count;
            next_plan->_aggregate = aggregate;
            MOT_LOG_TRACE("Found a candidate plan with value %0.2f:", evaluatePlan(next_plan));
            JitExplainPlan(query, (JitPlan*)next_plan);
            if (plan == nullptr) {
                MOT_LOG_TRACE(
                    "Using initial plan with index %d (%s)", index_id, table->GetIndex(index_id)->GetName().c_str());
                plan = next_plan;
            } else if (isJitPlanBetter(next_plan, plan)) {
                MOT_LOG_TRACE("Plan with index %d (%s) is better than previous plan",
                    index_id,
                    table->GetIndex(index_id)->GetName().c_str());
                JitDestroyPlan((JitPlan*)plan);
                plan = next_plan;
            } else {
                JitDestroyPlan((JitPlan*)next_plan);
            }
        }
    }

    if (clean_plan && (plan != nullptr)) {
        JitDestroyPlan((JitPlan*)plan);
        plan = nullptr;
    }

    if (plan == nullptr) {
        MOT_LOG_TRACE("Failed to prepare Range SELECT plan");
    }
    return (JitPlan*)plan;
}

static JitPlan* JitPrepareSimplePointQueryPlan(Query* query, MOT::Table* table)
{
    JitPlan* plan = nullptr;
    // point query does not expect sort clause or aggregate clause
    if (!CheckQueryAttributes(query, false, false, false)) {
        MOT_LOG_TRACE("JitPrepareSimplePlan(): Disqualifying point query - Invalid query attributes");
    } else if (query->commandType == CMD_UPDATE) {
        plan = JitPrepareUpdatePlan(query, table);
    } else if (query->commandType == CMD_DELETE) {
        plan = JitPrepareDeletePlan(query, table);
    } else if (query->commandType == CMD_SELECT) {
        plan = JitPrepareSelectPlan(query, table);
    } else {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
            "Prepare Simple Point-query JIT Plan",
            "Unexpected command type: %d",
            (int)query->commandType);
    }
    return plan;
}

static JitPlan* JitPrepareSimplePlan(Query* query)
{
    JitPlan* plan = nullptr;
    MOT_LOG_TRACE("Preparing a simple plan");

    MOT::Table* table = GetTableFromQuery(query);
    if (table == nullptr) {
        MOT_LOG_TRACE("JitPrepareSimplePlan(): Failed to retrieve table from query");
        return nullptr;
    }

    // if this is an insert command then generate an insert plan
    if (query->commandType == CMD_INSERT) {
        if (!CheckQueryAttributes(query, false, false, false)) {
            MOT_LOG_TRACE("JitPrepareSimplePlan(): Disqualifying INSERT query - Invalid query attributes");
        } else {
            plan = JitPrepareInsertPlan(query, table);
        }
    } else {
        MOT::Index* index = table->GetPrimaryIndex();
        int count = 0;
        if (!countWhereClauseEqualsLeaves(query,
                table,
                index,
                &count)) {  // count all equals operators that relate to the index (disregard other filters)
            MOT_LOG_TRACE("JitPrepareSimplePlan(): Failed to determine if this is a point query");
        } else if (count == index->GetNumFields()) {  // a point query
            plan = JitPrepareSimplePointQueryPlan(query, table);
        } else {
            if (query->commandType == CMD_UPDATE) {
                if (!CheckQueryAttributes(
                        query, false, false, false)) {  // range update does not expect sort clause or aggregate clause
                    MOT_LOG_TRACE(
                        "JitPrepareSimplePlan(): Disqualifying range update query - Invalid query attributes");
                } else {
                    plan = JitPrepareRangeUpdatePlan(query, table);
                }
            } else if (query->commandType == CMD_SELECT) {
                if (!CheckQueryAttributes(
                        query, true, true, false)) {  // range select can specify sort clause or aggregate clause
                    MOT_LOG_TRACE(
                        "JitPrepareSimplePlan(): Disqualifying range select query - Invalid query attributes");
                } else {
                    plan = JitPrepareRangeSelectPlan(query, table, JoinClauseNone);
                }
            } else {
                MOT_LOG_TRACE("JitPrepareSimplePlan(): Disqualifying unsupported range delete query");
            }
        }
    }

    if (plan == nullptr) {
        MOT_LOG_TRACE("Failed to prepare simple plan");
    }
    return plan;
}

static bool isValidJoinedRangeScan(Query* query, JitRangeSelectPlan* scan_plan2, const MOT::Table* table1)
{
    MOT::Table* table2 = scan_plan2->_index_scan._table;
    for (int i = 0; i < scan_plan2->_index_scan._search_exprs._count; ++i) {
        if (scan_plan2->_index_scan._search_exprs._exprs[i]._join_expr) {
            if (scan_plan2->_index_scan._search_exprs._exprs[i]._expr->_expr_type != JIT_EXPR_TYPE_VAR) {
                MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                    "Prepare JIT JOIN Plan",
                    "Unexpected non-column expression in JOIN search expression (index %d)",
                    i);
                return false;
            } else {
                MOT::Table* real_table = scan_plan2->_index_scan._search_exprs._exprs[i]._table;
                if ((real_table != table1) && (real_table != table2)) {
                    MOT_LOG_TRACE(
                        "Invalid search JOIN expression referring to unexpected table %s (expecting either %s or %s)",
                        real_table->GetTableName().c_str(),
                        table1->GetTableName().c_str(),
                        table2->GetTableName().c_str());
                    return false;
                }
            }
        }
    }

    return true;
}

static JitJoinScanType getJoinScanType(JitIndexScanType outer_scan_type, JitIndexScanType inner_scan_type)
{
    JitJoinScanType scan_type = JIT_JOIN_SCAN_INVALID;

    if ((outer_scan_type == JIT_INDEX_SCAN_POINT) && (inner_scan_type == JIT_INDEX_SCAN_POINT)) {
        scan_type = JIT_JOIN_SCAN_POINT;
    } else if ((outer_scan_type == JIT_INDEX_SCAN_POINT) && (inner_scan_type != JIT_INDEX_SCAN_POINT)) {
        scan_type = JIT_JOIN_SCAN_OUTER_POINT;
    } else if ((outer_scan_type != JIT_INDEX_SCAN_POINT) && (inner_scan_type == JIT_INDEX_SCAN_POINT)) {
        scan_type = JIT_JOIN_SCAN_INNER_POINT;
    } else {
        scan_type = JIT_JOIN_SCAN_RANGE;
    }

    return scan_type;
}

static JitPlan* JitPrepareExplicitJoinPlan(Query* query)
{
    JitPlan* plan = nullptr;
    MOT_LOG_TRACE("Preparing an explicit JOIN plan");

    // in this kind of join we have a JOIN <TableName> ON(qualifiers)
    // so we expect 3 tables: two originals and one virtual
    // we need to identify both tables, and divide all qualifiers into 3 groups:
    // 1. table 1 scan qualifiers and filters
    // 2. table 2 scan qualifiers and filters
    // 3. join qualifiers

    // two tables have rtekind=0 (RTE_RELATION), and the virtual join table has rtekind=2 (RTE_JOIN)
    RangeTblEntry* rte1 = (RangeTblEntry*)linitial(query->rtable);
    RangeTblEntry* rte2 = (RangeTblEntry*)lsecond(query->rtable);
    RangeTblEntry* rte3 = (RangeTblEntry*)lthird(query->rtable);

    // we expect to see tables in this order
    if ((rte1->rtekind == RTE_RELATION) && (rte2->rtekind == RTE_RELATION) && (rte3->rtekind == RTE_JOIN)) {
        Oid relid1 = rte1->relid;
        Oid relid2 = rte2->relid;

        // make sure that WHERE clause refers to full prefix in each table (even an empty one)
        MOT::Table* table1 = MOT::GetTableManager()->GetTableByExternal(relid1);
        MOT::Table* table2 = MOT::GetTableManager()->GetTableByExternal(relid2);

        MOT_LOG_TRACE("Preparing an explicit JOIN plan on tables: %s, %s",
            table1->GetTableName().c_str(),
            table2->GetTableName().c_str());

        // in essence we are trying to prepare two index scans
        JitRangeSelectPlan* scan_plan1 =
            (JitRangeSelectPlan*)JitPrepareRangeSelectPlan(query, table1, JoinClauseNone);  // do not use JOIN clause
        if (scan_plan1 == nullptr) {
            MOT_LOG_TRACE(
                "Failed to prepare a scan plan for JOIN query first table %s", table1->GetTableName().c_str());
            return nullptr;
        }
        JitRangeSelectPlan* scan_plan2 = (JitRangeSelectPlan*)JitPrepareRangeSelectPlan(
            query, table2, JoinClauseExplicit);  // use explicit JOIN clause
        if (scan_plan2 == nullptr) {
            MOT_LOG_TRACE(
                "Failed to prepare a scan plan for JOIN query second table %s", table2->GetTableName().c_str());
            JitDestroyPlan((JitPlan*)scan_plan1);
            return nullptr;
        }

        // ATTENTION: we should try to prepare the opposite plan and evaluate which is better
        // in the future the jitted code should be able to evaluate during execution in which index scan to begin
        MOT_LOG_TRACE("Found JOIN query range scan plan for first table %s", table1->GetTableName().c_str());
        JitExplainPlan(query, (JitPlan*)scan_plan1);
        MOT_LOG_TRACE("Found JOIN query range scan plan for second table %s", table2->GetTableName().c_str());
        JitExplainPlan(query, (JitPlan*)scan_plan2);

        // verify that the second scan plan has proper join expressions
        if (!isValidJoinedRangeScan(query, scan_plan2, table1)) {
            MOT_LOG_TRACE("Invalid joined range scan for second plan in JOIN plan");
            JitDestroyPlan((JitPlan*)scan_plan1);
            JitDestroyPlan((JitPlan*)scan_plan2);
            return nullptr;
        }

        // verify that point join has no aggregate
        if ((scan_plan1->_index_scan._scan_type == JIT_INDEX_SCAN_POINT) &&
            (scan_plan2->_index_scan._scan_type == JIT_INDEX_SCAN_POINT)) {
            if (scan_plan1->_aggregate._aggreaget_op != JIT_AGGREGATE_NONE) {
                MOT_LOG_TRACE("Invalid aggregate specifier for point JOIN plan");
                JitDestroyPlan((JitPlan*)scan_plan1);
                JitDestroyPlan((JitPlan*)scan_plan2);
                return nullptr;
            }
        }

        // now prepare the final join plan
        size_t alloc_size = sizeof(JitJoinPlan);
        JitJoinPlan* join_plan = (JitJoinPlan*)MOT::MemSessionAlloc(alloc_size);
        if (!join_plan) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM,
                "Prepare JIT JOIN plan",
                "Failed to allocate %u bytes for join plan",
                (unsigned)alloc_size);
            JitDestroyPlan((JitPlan*)scan_plan1);
            JitDestroyPlan((JitPlan*)scan_plan2);
        } else {
            errno_t erc = memset_s(join_plan, alloc_size, 0, alloc_size);
            securec_check(erc, "\0", "\0");
            join_plan->_plan_type = JIT_PLAN_JOIN;
            join_plan->_outer_scan = scan_plan1->_index_scan;
            join_plan->_inner_scan = scan_plan2->_index_scan;
            join_plan->_select_exprs = scan_plan1->_select_exprs;
            join_plan->_limit_count = scan_plan1->_limit_count;
            join_plan->_aggregate = scan_plan1->_aggregate;
            join_plan->_scan_type =
                getJoinScanType(join_plan->_outer_scan._scan_type, join_plan->_inner_scan._scan_type);

            // cleanup only second plan's select expressions
            freeSelectExprArray(&scan_plan2->_select_exprs);
            MOT::MemSessionFree(scan_plan1);
            MOT::MemSessionFree(scan_plan2);

            plan = (JitPlan*)join_plan;
        }
    }

    return plan;
}

static JitPlan* JitPrepareImplicitJoinPlan(Query* query)
{
    JitPlan* plan = nullptr;
    MOT_LOG_TRACE("Preparing an implicit JOIN plan");

    RangeTblEntry* rte1 = (RangeTblEntry*)linitial(query->rtable);
    RangeTblEntry* rte2 = (RangeTblEntry*)lsecond(query->rtable);

    MOT::Table* table1 = MOT::GetTableManager()->GetTableByExternal(rte1->relid);
    MOT::Table* table2 = MOT::GetTableManager()->GetTableByExternal(rte2->relid);

    MOT_LOG_TRACE("Preparing an implicit JOIN plan on tables: %s, %s",
        table1->GetTableName().c_str(),
        table2->GetTableName().c_str());

    // there is no explicit join expression
    JitRangeSelectPlan* scan_plan1 =
        (JitRangeSelectPlan*)JitPrepareRangeSelectPlan(query, table1, JoinClauseNone);  // do not use JOIN clause
    if (scan_plan1 == nullptr) {
        MOT_LOG_TRACE("Failed to prepare a scan plan for JOIN query first table %s", table1->GetTableName().c_str());
        return nullptr;
    }
    JitRangeSelectPlan* scan_plan2 =
        (JitRangeSelectPlan*)JitPrepareRangeSelectPlan(query, table2, JoinClauseImplicit);  // use JOIN clause
    if (scan_plan2 == nullptr) {
        MOT_LOG_TRACE("Failed to prepare a scan plan for JOIN query second table %s", table2->GetTableName().c_str());
        JitDestroyPlan((JitPlan*)scan_plan1);
        return nullptr;
    }

    if (scan_plan1 != nullptr && scan_plan2 != nullptr) {
        MOT_LOG_TRACE("Found JOIN query range scan plan for first table %s", table1->GetTableName().c_str());
        JitExplainPlan(query, (JitPlan*)scan_plan1);
        MOT_LOG_TRACE("Found JOIN query range scan plan for second table %s", table2->GetTableName().c_str());
        JitExplainPlan(query, (JitPlan*)scan_plan2);

        // verify that the second scan plan has proper join expressions
        if (!isValidJoinedRangeScan(query, scan_plan2, table1)) {
            MOT_LOG_TRACE("Invalid joineed range scan for second plan in JOIN plan");
            JitDestroyPlan((JitPlan*)scan_plan1);
            JitDestroyPlan((JitPlan*)scan_plan2);
            return nullptr;
        }

        // verify that point join has no aggregate
        if ((scan_plan1->_index_scan._scan_type == JIT_INDEX_SCAN_POINT) &&
            (scan_plan2->_index_scan._scan_type == JIT_INDEX_SCAN_POINT)) {
            if (scan_plan1->_aggregate._aggreaget_op != JIT_AGGREGATE_NONE) {
                MOT_LOG_TRACE("Invalid aggregate specifier for point JOIN plan");
                JitDestroyPlan((JitPlan*)scan_plan1);
                JitDestroyPlan((JitPlan*)scan_plan2);
                return nullptr;
            }
        }

        // now prepare the final join plan
        size_t alloc_size = sizeof(JitJoinPlan);
        JitJoinPlan* join_plan = (JitJoinPlan*)MOT::MemSessionAlloc(alloc_size);
        if (join_plan == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM,
                "Prepare JIT JOIN plan",
                "Failed to allocate %u bytes for join plan",
                (unsigned)alloc_size);
            JitDestroyPlan((JitPlan*)scan_plan1);
            JitDestroyPlan((JitPlan*)scan_plan2);
        } else {
            errno_t erc = memset_s(join_plan, alloc_size, 0, alloc_size);
            securec_check(erc, "\0", "\0");
            join_plan->_plan_type = JIT_PLAN_JOIN;
            join_plan->_outer_scan = scan_plan1->_index_scan;
            join_plan->_inner_scan = scan_plan2->_index_scan;
            join_plan->_select_exprs = scan_plan1->_select_exprs;
            join_plan->_limit_count = scan_plan1->_limit_count;
            join_plan->_aggregate = scan_plan1->_aggregate;
            join_plan->_scan_type =
                getJoinScanType(join_plan->_outer_scan._scan_type, join_plan->_inner_scan._scan_type);

            // cleanup only second plan's select expressions
            freeSelectExprArray(&scan_plan2->_select_exprs);
            MOT::MemSessionFree(scan_plan1);
            MOT::MemSessionFree(scan_plan2);

            plan = (JitPlan*)join_plan;
        }
    }

    return plan;
}

static JitPlan* JitPrepareJoinPlan(Query* query)
{
    JitPlan* plan = nullptr;
    MOT_LOG_TRACE("Preparing JOIN plan");

    if (!CheckQueryAttributes(
            query, false, true, false)) {  // we do not support join query with ORDER BY clause, but we can aggregate
        MOT_LOG_TRACE("JitPrepareJoinPlan(): Disqualifying join query - Invalid query attributes");
    } else {
        // we deal differently with explicit and implicit joins, since the parsed query looks much different
        int table_count = list_length(query->rtable);
        if (table_count == 3) {  // case 1: we have an explicit "JOIN <Table> ON" clause
            plan = JitPrepareExplicitJoinPlan(query);
        } else if (table_count == 2) {  // case 2: no explicit JOIN clause
            plan = JitPrepareImplicitJoinPlan(query);
        } else {
            MOT_LOG_TRACE("Query is not jittable - unsupported JOIN format (more than 3 tables involved)");
        }
    }

    return plan;
}

static SubLink* GetSingleSubLink(Query* query, MOT::Table* table)
{
    SubLink* result = nullptr;
    Node* quals = query->jointree->quals;
    if (quals != nullptr) {
        SubLinkFetcher subLinkFetcher;
        MOT::Index* index = table->GetPrimaryIndex();
        bool result = visitSearchExpressions(query, table, index, (Expr*)&quals[0], true, &subLinkFetcher, false);
        if (!result) {
            MOT_LOG_TRACE("Failed to fetch WHERE clause single sub-link node");
        }
        result = subLinkFetcher.GetSubLink();
    }
    return result;
}

#ifdef JIT_SUPPORT_FOR_POINT_SUB_QUERY
static bool IsPointQuery(Query* query)
{
    bool result = false;

    MOT::Table* table = GetTableFromQuery(query);
    if (table == nullptr) {
        MOT_LOG_TRACE("IsPointQuery(): Failed to retrieve table from query");
        return false;
    }

    // count all equals operators that relate to the index (disregard other filters)
    MOT::Index* index = table->GetPrimaryIndex();
    int count = 0;
    if (!countWhereClauseEqualsLeaves(query, table, index, &count)) {
        MOT_LOG_TRACE("IsPointQuery(): Failed to determine if this is a point query");
    } else if (count == index->GetNumFields()) {  // a point query
        // point query does not expect sort clause or aggregate clause
        if (!CheckQueryAttributes(query, false, false, false)) {
            MOT_LOG_TRACE("JitPrepareSimplePlan(): Disqualifying point query - Invalid query attributes");
        } else {
            result = true;
        }
    }

    return result;
}

static bool IsSingleColumnResult(Query* query)
{
    bool result = false;

    if (list_length(query->targetList) == 1) {
        result = true;
    }

    return result;
}
#endif

static bool IsAggregate(Query* query)
{
    bool result = false;

    // we expect to see single target entry whose expression is AggRef
    if (list_length(query->targetList) == 1) {
        TargetEntry* targetEntry = (TargetEntry*)linitial(query->targetList);
        if (targetEntry->expr->type == T_Aggref) {
            Aggref* aggref = (Aggref*)targetEntry->expr;
            if (aggref->aggfnoid == INT4LARGERFUNCOID) {  // only MAX of int4 is currently supported
                result = true;
            }
        }
    }

    return result;
}

static bool IsSingleResultPlan(JitPlan* plan)
{
    bool result = false;
    if (plan->_plan_type == JIT_PLAN_POINT_QUERY) {
        JitPointQueryPlan* pointQueryPlan = (JitPointQueryPlan*)plan;
        if (pointQueryPlan->_command_type == JIT_COMMAND_SELECT) {
            result = true;
        }
    } else if (plan->_plan_type == JIT_PLAN_RANGE_SCAN) {
        JitRangeScanPlan* rangeScanPlan = (JitRangeScanPlan*)plan;
        if (rangeScanPlan->_command_type == JIT_COMMAND_SELECT) {
            JitRangeSelectPlan* rangeSelectPlan = (JitRangeSelectPlan*)rangeScanPlan;
            if (rangeSelectPlan->_index_scan._scan_type == JIT_INDEX_SCAN_POINT) {
                result = true;
            } else if (rangeSelectPlan->_limit_count == 1) {
                result = true;
            } else if (rangeSelectPlan->_aggregate._aggreaget_op != JIT_AGGREGATE_NONE) {
                result = true;
            }
        }
    }
    return result;
}

static bool IsValidCompoundScan(JitSelectPlan* outerQueryPlan, JitPlan* subQueryPlan)
{
    int subLinkCount = 0;
    for (int i = 0; i < outerQueryPlan->_query._search_exprs._count; ++i) {
        if (outerQueryPlan->_query._search_exprs._exprs[i]._expr->_expr_type == JIT_EXPR_TYPE_SUBLINK) {
            if (++subLinkCount > 1) {
                MOT_LOG_TRACE("Invalid compound scan with more than one sub-link");
                return false;
            }
            if (outerQueryPlan->_query._search_exprs._exprs[i]._expr->_source_expr->type != T_SubLink) {
                MOT_LOG_TRACE("Invalid compound scan with inconsistent sub-link: source expression is not a sub-link");
                return false;
            }
            JitSubLinkExpr* subLinkExpr = (JitSubLinkExpr*)outerQueryPlan->_query._search_exprs._exprs[i]._expr;
            if (subLinkExpr->_sub_query_index != 0) {  // we support only a single sub-query currently
                MOT_LOG_TRACE("Invalid compound scan sub-link: sub-query plan index is invalid");
                return false;
            }
        }
    }
    return true;
}

static JitPlan* JitPrepareCompoundPlan(Query* query, Query* subQuery)
{
    JitPlan* plan = nullptr;
    MOT_LOG_TRACE("Preparing a compound plan");

    RangeTblEntry* rte = (RangeTblEntry*)linitial(query->rtable);
    RangeTblEntry* subQueryRte = (RangeTblEntry*)linitial(subQuery->rtable);

    MOT::Table* table = MOT::GetTableManager()->GetTableByExternal(rte->relid);
    MOT::Table* subQueryTable = MOT::GetTableManager()->GetTableByExternal(subQueryRte->relid);

    MOT_LOG_TRACE("Preparing a compound plan on tables: %s, %s",
        table->GetTableName().c_str(),
        subQueryTable->GetTableName().c_str());

    // prepare the sub-query plan (we know already it yields only a single value)
    JitPlan* sub_query_plan = JitPrepareSimplePlan(subQuery);
    if (sub_query_plan == nullptr) {
        MOT_LOG_TRACE("Failed to prepare a scan plan for sub-query on table %s", subQueryTable->GetTableName().c_str());
        return nullptr;
    }

    // verify plan yields a single result
    if (!IsSingleResultPlan(sub_query_plan)) {
        MOT_LOG_TRACE("Disqualifying sub-query plan: yielding more than one result");
        JitDestroyPlan(sub_query_plan);
        return nullptr;
    }

    // now prepare the outer query plan (knowing that one column points to sub-query)
    JitSelectPlan* outerQueryPlan = (JitSelectPlan*)JitPrepareSelectPlan(query, table);
    if (outerQueryPlan == nullptr) {
        MOT_LOG_TRACE("Failed to prepare a plan for outer query on table %s", table->GetTableName().c_str());
        JitDestroyPlan(sub_query_plan);
        return nullptr;
    }

    if (sub_query_plan != nullptr && outerQueryPlan != nullptr) {
        MOT_LOG_TRACE("Found Compound sub-query plan for table %s", subQueryTable->GetTableName().c_str());
        JitExplainPlan(subQuery, sub_query_plan);
        MOT_LOG_TRACE("Found Compound outer query plan for table %s", table->GetTableName().c_str());
        JitExplainPlan(query, (JitPlan*)outerQueryPlan);

        // verify that the outer query plan has proper reference to sub-query plan
        if (!IsValidCompoundScan(outerQueryPlan, sub_query_plan)) {
            MOT_LOG_TRACE("Invalid compound scan");
            JitDestroyPlan(sub_query_plan);
            JitDestroyPlan((JitPlan*)outerQueryPlan);
            return nullptr;
        }

        // now prepare the final compound plan
        size_t allocSize = sizeof(JitCompoundPlan);
        JitCompoundPlan* compoundPlan = (JitCompoundPlan*)MOT::MemSessionAlloc(allocSize);
        if (compoundPlan == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM,
                "Prepare JIT COMPOUND plan",
                "Failed to allocate %u bytes for compound plan",
                (unsigned)allocSize);
            JitDestroyPlan(sub_query_plan);
            JitDestroyPlan((JitPlan*)outerQueryPlan);
        } else {
            errno_t erc = memset_s(compoundPlan, allocSize, 0, allocSize);
            securec_check(erc, "\0", "\0");
            compoundPlan->_plan_type = JIT_PLAN_COMPOUND;
            compoundPlan->_command_type = JIT_COMMAND_SELECT;
            compoundPlan->_sub_query_count = 1;  // we support currently only one sub-query
            allocSize = sizeof(JitPlan*) * compoundPlan->_sub_query_count;
            compoundPlan->_sub_query_plans = (JitPlan**)MOT::MemSessionAlloc(allocSize);
            if (compoundPlan->_sub_query_plans == nullptr) {
                MOT_REPORT_ERROR(MOT_ERROR_OOM,
                    "Prepare JIT COMPOUND plan",
                    "Failed to allocate %u bytes for sub-query plans",
                    (unsigned)allocSize);
                JitDestroyPlan(sub_query_plan);
                JitDestroyPlan((JitPlan*)outerQueryPlan);
                MOT::MemSessionFree(compoundPlan);
            } else {
                compoundPlan->_sub_query_plans[0] = sub_query_plan;
                compoundPlan->_outer_query_plan = (JitPointQueryPlan*)outerQueryPlan;

                plan = (JitPlan*)compoundPlan;
            }
        }
    }

    return plan;
}

static JitPlan* JitPrepareCompoundPlan(Query* query)
{
    JitPlan* plan = nullptr;
    MOT_LOG_TRACE("Preparing COMPOUND query");

    // we do not support compound query with ORDER BY clause, but we can aggregate
    if (!CheckQueryAttributes(query, false, true, true)) {
        MOT_LOG_TRACE("JitPrepareCompoundPlan(): Disqualifying compound query - Invalid query attributes");
        return nullptr;
    }

    // we need to verify the outer query is a point query, and that only one dimension is depending on a sub-query
    // which by itself is a point query or an aggregate
    MOT::Table* table = GetTableFromQuery(query);
    if (table == nullptr) {
        MOT_LOG_TRACE("JitPrepareCompoundPlan(): Failed to retrieve table from query");
        return nullptr;
    }

    // count all equals operators that relate to the index (disregard other filters)
    MOT::Index* index = table->GetPrimaryIndex();
    int count = 0;
    if (!countWhereClauseEqualsLeaves(query, table, index, &count)) {
        MOT_LOG_TRACE("JitPrepareCompoundPlan(): Failed to determine if this is a point query");
    } else if (count == index->GetNumFields()) {  // a point query
        // now verify that only one dimension has a sub-link
        SubLink* subLink = GetSingleSubLink(query, table);
        if (subLink == nullptr) {
            MOT_LOG_TRACE("JitPrepareCompoundPlan(): Disqualifying query with unsupported sub-link specification");
        } else {
            Query* subQuery = (Query*)subLink->subselect;
            // sub-query can have aggregate, but no sub-query or ORDER BY clause
            if (!CheckQueryAttributes(subQuery, false, true, false)) {
                MOT_LOG_TRACE("JitPrepareCompoundPlan(): Disqualifying sub-query - Invalid query attributes");
                return nullptr;
            }
            // sub-query must be a SELECT command
            if (query->commandType != CMD_SELECT) {
                MOT_LOG_TRACE("JitPrepareCompoundPlan(): Disqualifying sub-query - Invalid command type %d",
                    (int)query->commandType);
                return nullptr;
            }
            // final test: verify that sub-link is a point query or aggregate (so it yields a single result)
#ifdef JIT_SUPPORT_FOR_POINT_SUB_QUERY
            if ((IsPointQuery(subQuery) && IsSingleColumnResult(subQuery)) || IsAggregate(subQuery)) {
#else
            if (IsAggregate(subQuery)) {
#endif
                plan = JitPrepareCompoundPlan(query, subQuery);
            } else {
                MOT_LOG_TRACE("JitPrepareCompoundPlan(): Disqualifying non-point and non-aggregate sub-link query");
            }
        }
    }

    return plan;
}

extern JitPlan* JitPreparePlan(Query* query, const char* query_string)
{
    JitPlan* plan = nullptr;
    MOT_LOG_TRACE("Preparing plan for query: %s", query_string);

    // we start by checking the number of tables involved
    if (list_length(query->rtable) == 1) {
        plan = JitPrepareSimplePlan(query);
        // special case: a sub-query that evaluates to point query or single value aggregate, which in turn
        // is used in a point-select outer query
        if ((plan == nullptr) && query->hasSubLinks && (query->commandType == CMD_SELECT)) {
            plan = JitPrepareCompoundPlan(query);
        }
    } else {
        plan = JitPrepareJoinPlan(query);
    }

    return plan;
}

extern bool JitPlanHasDistinct(JitPlan* plan)
{
    bool result = false;

    if (plan->_plan_type == JIT_PLAN_RANGE_SCAN) {
        if (((JitRangeScanPlan*)plan)->_command_type == JIT_COMMAND_SELECT) {
            JitRangeSelectPlan* select_plan = (JitRangeSelectPlan*)plan;
            if ((select_plan->_aggregate._aggreaget_op != JIT_AGGREGATE_NONE) && select_plan->_aggregate._distinct) {
                result = true;
            }
        }
    } else if (plan->_plan_type == JIT_PLAN_JOIN) {
        JitJoinPlan* join_plan = (JitJoinPlan*)plan;
        if ((join_plan->_aggregate._aggreaget_op != JIT_AGGREGATE_NONE) && join_plan->_aggregate._distinct) {
            result = true;
        }
    }

    return result;
}

extern bool JitPlanHasSort(JitPlan* plan)
{
    bool result = false;

    if (plan->_plan_type == JIT_PLAN_RANGE_SCAN) {
        if (((JitRangeScanPlan*)plan)->_command_type == JIT_COMMAND_SELECT) {
            JitRangeSelectPlan* select_plan = (JitRangeSelectPlan*)plan;
            if (select_plan->_index_scan._sort_order != JIT_QUERY_SORT_INVALID) {
                result = true;
            }
        }
    } else if (plan->_plan_type == JIT_PLAN_JOIN) {
        JitJoinPlan* join_plan = (JitJoinPlan*)plan;
        if (join_plan->_outer_scan._sort_order != JIT_QUERY_SORT_INVALID) {
            result = true;
        }
    }

    return result;
}

static void JitDestroyInsertPlan(JitInsertPlan* plan)
{
    freeExprArray(&plan->_insert_exprs);
    MOT::MemSessionFree(plan);
}

static void JitDestroyPointQueryPlan(JitPointQueryPlan* plan)
{
    freeExprArray(&plan->_query._search_exprs);
    freeFilterArray(&plan->_query._filters);

    if (plan->_command_type == JIT_COMMAND_UPDATE) {
        JitUpdatePlan* update_plan = (JitUpdatePlan*)plan;
        freeExprArray(&update_plan->_update_exprs);
    } else if (plan->_command_type == JIT_COMMAND_SELECT) {
        JitSelectPlan* select_plan = (JitSelectPlan*)plan;
        freeSelectExprArray(&select_plan->_select_exprs);
    }

    MOT::MemSessionFree(plan);
}

static void freeIndexScan(JitIndexScan* index_scan)
{
    freeExprArray(&index_scan->_search_exprs);
    freeFilterArray(&index_scan->_filters);
}

static void JitDestroyRangeUpdatePlan(JitRangeUpdatePlan* plan)
{
    freeExprArray(&plan->_update_exprs);
}

static void JitDestroyRangeSelectPlan(JitRangeSelectPlan* plan)
{
    freeSelectExprArray(&plan->_select_exprs);
}

static void JitDestroyRangeScanPlan(JitRangeScanPlan* plan)
{
    freeIndexScan(&plan->_index_scan);
    if (plan->_command_type == JIT_COMMAND_UPDATE) {
        JitDestroyRangeUpdatePlan((JitRangeUpdatePlan*)plan);
    } else if (plan->_command_type == JIT_COMMAND_SELECT) {
        JitDestroyRangeSelectPlan((JitRangeSelectPlan*)plan);
    }
    MOT::MemSessionFree(plan);
}

static void JitDestroyJoinPlan(JitJoinPlan* plan)
{
    freeIndexScan(&plan->_outer_scan);
    freeIndexScan(&plan->_inner_scan);
    freeSelectExprArray(&plan->_select_exprs);
    MOT::MemSessionFree(plan);
}

static void JitDestroyCompoundPlan(JitCompoundPlan* plan)
{
    for (uint32_t i = 0; i < plan->_sub_query_count; ++i) {
        JitDestroyPlan(plan->_sub_query_plans[i]);
    }
    MOT::MemSessionFree(plan->_sub_query_plans);

    JitDestroyPointQueryPlan(plan->_outer_query_plan);
    MOT::MemSessionFree(plan);
}

extern void JitDestroyPlan(JitPlan* plan)
{
    if (plan != nullptr) {
        switch (plan->_plan_type) {
            case JIT_PLAN_INSERT_QUERY:
                JitDestroyInsertPlan((JitInsertPlan*)plan);
                break;

            case JIT_PLAN_POINT_QUERY:
                JitDestroyPointQueryPlan((JitPointQueryPlan*)plan);
                break;

            case JIT_PLAN_RANGE_SCAN:
                JitDestroyRangeScanPlan((JitRangeScanPlan*)plan);
                break;

            case JIT_PLAN_JOIN:
                JitDestroyJoinPlan((JitJoinPlan*)plan);
                break;

            case JIT_PLAN_COMPOUND:
                JitDestroyCompoundPlan((JitCompoundPlan*)plan);
                break;

            case JIT_PLAN_INVALID:
            default:
                break;
        }
    }
}
}  // namespace JitExec
