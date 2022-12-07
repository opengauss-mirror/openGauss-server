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
 * jit_plan_expr.cpp
 *    JIT execution plan expression helpers.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/jit_exec/jit_plan_expr.cpp
 *
 * -------------------------------------------------------------------------
 */

/*
 * ATTENTION: Be sure to include global.h before postgres.h to avoid conflict between libintl.h (included in global.h)
 * and c.h (included in postgres.h).
 */
#include "global.h"
#include "jit_plan_expr.h"
#include "catalog/pg_aggregate.h"
#include "mot_internal.h"

namespace JitExec {
DECLARE_LOGGER(JitPlanExpr, JitExec)

static void FreeScalarArrayOpExpr(JitScalarArrayOpExpr* expr);
static bool ExprHasVarRef(Expr* expr, int depth);

bool ExprHasVarRef(Expr* expr)
{
    return ExprHasVarRef(expr, 0);
}

bool ExpressionCounter::OnExpression(
    Expr* expr, int columnType, int tableColumnId, MOT::Table* table, JitWhereOperatorClass opClass, bool joinExpr)
{
    if (opClass != JIT_WOC_EQUALS) {
        MOT_LOG_TRACE("ExpressionCounter::onExpression(): Skipping non-equals operator");
        return true;  // this is not an error condition
    }
    ++(*_count);
    return true;
}

bool ExpressionCollector::OnExpression(
    Expr* expr, int columnType, int tableColumnId, MOT::Table* table, JitWhereOperatorClass opClass, bool joinExpr)
{
    if (opClass != JIT_WOC_EQUALS) {
        MOT_LOG_TRACE("ExpressionCollector::onExpression(): Skipping non-equals operator");
        return true;  // this is not an error condition
    } else if (*_expr_count < _expr_array->_count) {
        JitExpr* jit_expr = parseExpr(_query, expr, 0);
        if (jit_expr == nullptr) {
            MOT_LOG_TRACE("ExpressionCollector::onExpression(): Failed to parse expression %d", *_expr_count);
            Cleanup();
            return false;
        }
        _expr_array->_exprs[*_expr_count]._table_column_id = tableColumnId;
        _expr_array->_exprs[*_expr_count]._table = table;
        _expr_array->_exprs[*_expr_count]._expr = jit_expr;
        _expr_array->_exprs[*_expr_count]._column_type = columnType;
        _expr_array->_exprs[*_expr_count]._join_expr = joinExpr;
        ++(*_expr_count);
        return true;
    } else {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL, "Prepare JIT Plan", "Exceeded expression count %d", _expr_array->_count);
        return false;
    }
}

void ExpressionCollector::Cleanup()
{
    for (int i = 0; i < *_expr_count; ++i) {
        freeExpr(_expr_array->_exprs[*_expr_count]._expr);
    }
}

bool RangeScanExpressionCollector::Init()
{
    bool result = false;
    _max_index_ops = _index->GetNumFields() + 1;
    size_t alloc_size = sizeof(IndexOpClass) * _max_index_ops;
    _index_ops = (IndexOpClass*)MOT::MemSessionAlloc(alloc_size);
    if (_index_ops == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Prepare JIT Range Scan Plan",
            "Failed to allocate %u bytes for %d index operations",
            (unsigned)alloc_size,
            _max_index_ops);
    } else {
        result = true;
    }
    return result;
}

bool RangeScanExpressionCollector::OnExpression(
    Expr* expr, int columnType, int tableColumnId, MOT::Table* table, JitWhereOperatorClass opClass, bool joinExpr)
{
    if (_index_op_count >= _index_scan->_search_exprs._count) {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
            "Prepare JIT Plan",
            "Exceeded expression count %d, while collecting range scan expressions",
            _index_scan->_search_exprs._count);
        return false;
    } else if (_index_op_count == _max_index_ops) {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
            "Prepare JIT Plan",
            "Exceeded index column count %d, while collecting range scan expressions",
            _max_index_ops);
        return false;
    } else {
        JitExpr* jit_expr = parseExpr(_query, expr, 0);
        if (jit_expr == nullptr) {
            MOT_LOG_TRACE(
                "RangeScanExpressionCollector::onExpression(): Failed to parse expression %d", _index_op_count);
            Cleanup();
            return false;
        }
        _index_scan->_search_exprs._exprs[_index_op_count]._table_column_id = tableColumnId;
        _index_scan->_search_exprs._exprs[_index_op_count]._table = table;
        _index_scan->_search_exprs._exprs[_index_op_count]._expr = jit_expr;
        _index_scan->_search_exprs._exprs[_index_op_count]._column_type = columnType;
        _index_scan->_search_exprs._exprs[_index_op_count]._join_expr = joinExpr;
        int indexColumnId = MapTableColumnToIndex(_table, _index, tableColumnId);
        _index_scan->_search_exprs._exprs[_index_op_count]._index_column_id = indexColumnId;
        _index_scan->_search_exprs._exprs[_index_op_count]._op_class = opClass;
        _index_ops[_index_op_count]._index_column_id = indexColumnId;
        _index_ops[_index_op_count]._op_class = opClass;
        MOT_LOG_TRACE("RangeScanExpressionCollector::onExpression(): collected index column %d at index %d",
            _index_ops[_index_op_count]._index_column_id,
            _index_op_count);
        ++_index_op_count;
        return true;
    }
}

void RangeScanExpressionCollector::EvaluateScanType()
{
    _index_scan->_scan_type = JIT_INDEX_SCAN_TYPE_INVALID;
    JitIndexScanType scanType = JIT_INDEX_SCAN_TYPE_INVALID;

    // if two expressions refer to the same column, we regard one of them as filter
    // if an expression is removed from index scan, it will automatically be collected as filter
    // (see pkey_exprs argument in @ref visitSearchOpExpression)
    if (!RemoveDuplicates()) {
        MOT_LOG_TRACE("RangeScanExpressionCollector(): Disqualifying query - failed to remove duplicates");
        return;
    }

    // if no expression was collected, this is an invalid scan (we do not support full scans yet)
    int columnCount = _index_op_count;
    if (_index_op_count == 0) {
#ifdef MOT_JIT_FULL_SCAN
        MOT_LOG_TRACE("RangeScanExpressionCollector(): no expression was collected, assuming full scan");
        _index_scan->_scan_type = JIT_INDEX_SCAN_FULL;
        _index_scan->_column_count = 0;
        _index_scan->_search_exprs._count = 0;
#else
        MOT_LOG_TRACE("RangeScanExpressionCollector(): Disqualifying query - full scan");
#endif
        return;
    }

    // first step: sort in-place all collected operators
    if (_index_op_count > 1) {
        for (int i = 0; i < _index_op_count; ++i) {
            MOT_LOG_TRACE("Unsorted index column %d: id=%d, op=%d",
                i,
                _index_ops[i]._index_column_id,
                (int)_index_ops[i]._op_class);
        }
        MOT_LOG_TRACE("Sorting index ops");
        std::stable_sort(&_index_ops[0], &_index_ops[_index_op_count], IndexOpCmp);
        std::stable_sort(
            &_index_scan->_search_exprs._exprs[0], &_index_scan->_search_exprs._exprs[_index_op_count], ExprCmp(*this));
        for (int i = 0; i < _index_op_count; ++i) {
            MOT_LOG_TRACE("Sorted index column %d: id=%d, op=%d",
                i,
                _index_ops[i]._index_column_id,
                (int)_index_ops[i]._op_class);
        }
    }

    // now verify all but last two are equals operator
    for (int i = 0; i < _index_op_count - 2; ++i) {
        if (_index_ops[i]._op_class != JIT_WOC_EQUALS) {
            MOT_LOG_TRACE("RangeScanExpressionCollector(): Disqualifying query - encountered non-equals operator "
                          "at premature index column %d",
                _index_ops[i]._index_column_id);
            return;
        }
    }

    // now carefully inspect last two operators to determine expected scan type
    if (!DetermineScanType(scanType, columnCount)) {
        MOT_LOG_TRACE("RangeScanExpressionCollector(): Disqualifying query - invalid open scan specifying last "
                      "operator as equals, while previous one is not");
        return;
    }

    // final step: verify we have no holes in the columns according to the expected scan type
    if (!ScanHasHoles(scanType)) {
        _index_scan->_scan_type = scanType;
        _index_scan->_column_count = columnCount;
        // clean up unused expressions before decreasing number of expressions
        for (int i = _index_op_count; i < _index_scan->_search_exprs._count; ++i) {
            freeExpr(_index_scan->_search_exprs._exprs[i]._expr);
            _index_scan->_search_exprs._exprs[i]._expr = nullptr;
        }
        _index_scan->_search_exprs._count = _index_op_count;  // update real number of participating expressions
    }
}

bool RangeScanExpressionCollector::DetermineScanType(JitIndexScanType& scanType, int& columnCount)
{
    // now carefully inspect last two operators to determine expected scan type
    bool result = true;
    if (_index_op_count >= 2) {
        if (_index_ops[_index_op_count - 2]._op_class == JIT_WOC_EQUALS) {
            if (_index_ops[_index_op_count - 1]._op_class == JIT_WOC_EQUALS) {
                if (_index->GetUnique() && (_index_op_count == _index->GetNumFields())) {
                    MOT_LOG_TRACE("DetermineScanType(): Found point query scan");
                    scanType = JIT_INDEX_SCAN_POINT;
                } else {
                    MOT_LOG_TRACE("DetermineScanType(): Found closed query scan");
                    scanType = JIT_INDEX_SCAN_CLOSED;
                }
            } else {
                MOT_LOG_TRACE("DetermineScanType(): Found semi-open query scan");
                scanType = JIT_INDEX_SCAN_SEMI_OPEN;
                _index_scan->_last_dim_op1 = _index_ops[_index_op_count - 1]._op_class;
            }
        } else if (_index_ops[_index_op_count - 1]._op_class == JIT_WOC_EQUALS) {
            // this can be treated as semi-open scan, having last expression regarded as filter
            MOT_LOG_TRACE("RangeScanExpressionCollector(): scan specifying last operator as equals, while previous one "
                          "is not - regarding scan as semi-open with a filter");
            scanType = JIT_INDEX_SCAN_SEMI_OPEN;
            _index_scan->_last_dim_op1 = _index_ops[_index_op_count - 2]._op_class;
            // remove last index op (clean up takes place later, both on success and failure)
            --_index_op_count;
            columnCount = _index_op_count;
        } else {
            MOT_LOG_TRACE("DetermineScanType(): Found open query scan");
            scanType = JIT_INDEX_SCAN_OPEN;
            columnCount = _index_op_count - 1;
            _index_scan->_last_dim_op1 = _index_ops[_index_op_count - 2]._op_class;
            _index_scan->_last_dim_op2 = _index_ops[_index_op_count - 1]._op_class;
        }
    } else if (_index_op_count == 1) {
        if (_index_ops[0]._op_class == JIT_WOC_EQUALS) {
            if (_index->GetUnique() && (_index_op_count == _index->GetNumFields())) {
                MOT_LOG_TRACE("DetermineScanType(): Found point query scan (one column)");
                scanType = JIT_INDEX_SCAN_POINT;
            } else {
                MOT_LOG_TRACE("DetermineScanType(): Found closed query scan (one column)");
                scanType = JIT_INDEX_SCAN_CLOSED;
            }
        } else {
            MOT_LOG_TRACE("DetermineScanType(): Found semi-open query scan (one column)");
            scanType = JIT_INDEX_SCAN_SEMI_OPEN;
            _index_scan->_last_dim_op1 = _index_ops[0]._op_class;
        }
    }
    return result;
}

void RangeScanExpressionCollector::Cleanup()
{
    for (int i = 0; i < _index_op_count; ++i) {
        if (_index_scan->_search_exprs._exprs[i]._expr != nullptr) {
            freeExpr(_index_scan->_search_exprs._exprs[i]._expr);
            _index_scan->_search_exprs._exprs[i]._expr = nullptr;
        }
    }
    _index_scan->_search_exprs._count = 0;
    _index_op_count = 0;
}

bool RangeScanExpressionCollector::RemoveDuplicates()
{
    int result = RemoveSingleDuplicate();
    while (result > 0) {
        result = RemoveSingleDuplicate();
    }
    return (result == 0);
}

bool RangeScanExpressionCollector::IsSameOp(JitWhereOperatorClass lhs, JitWhereOperatorClass rhs)
{
    // test same operation
    switch (lhs) {
        case JIT_WOC_GREATER_EQUALS:
        case JIT_WOC_GREATER_THAN: {
            switch (rhs) {
                case JIT_WOC_GREATER_EQUALS:
                case JIT_WOC_GREATER_THAN:
                    return true;
                default:
                    break;
            }
            break;
        }
        case JIT_WOC_LESS_EQUALS:
        case JIT_WOC_LESS_THAN: {
            switch (rhs) {
                case JIT_WOC_LESS_EQUALS:
                case JIT_WOC_LESS_THAN:
                    return true;
                default:
                    break;
            }
            break;
        }
        default:
            break;
    }

    return false;
}

int RangeScanExpressionCollector::RemoveSingleDuplicate()
{
    // scan and stop after first removal
    for (int i = 1; i < _index_op_count; ++i) {
        for (int j = 0; j < i; ++j) {
            if (_index_ops[i]._index_column_id == _index_ops[j]._index_column_id) {
                MOT_LOG_TRACE("RangeScanExpressionCollector(): Found duplicate column ref at %d and %d", i, j);
                if ((_index_ops[i]._op_class != JIT_WOC_EQUALS) && (_index_ops[j]._op_class != JIT_WOC_EQUALS)) {
                    // test same operation
                    if (IsSameOp(_index_ops[i]._op_class, _index_ops[j]._op_class)) {
                        MOT_LOG_TRACE("RangeScanExpressionCollector(): Found same operation - Disqualifying query");
                        return -1;
                    }
                    MOT_LOG_DEBUG("RangeScanExpressionCollector(): Skipping probable open scan operators while "
                                  "removing duplicates");
                    continue;
                }
                // now we need to decide which one to remove,
                // our consideration is to keep first JOIN expressions and then EQUALS expressions
                int victim = -1;
                if ((_index_ops[i]._op_class != JIT_WOC_EQUALS) || (_index_ops[j]._op_class != JIT_WOC_EQUALS)) {
                    // we keep the equals operator for index scan, and leave the other as a filter
                    MOT_LOG_TRACE("RangeScanExpressionCollector(): Found duplicate index column reference, one with "
                                  "EQUALS, one without - non-equals column will be considered as a filter");
                    if (_index_ops[i]._op_class != JIT_WOC_EQUALS) {
                        victim = i;
                    } else {
                        victim = j;
                    }
                    MOT_LOG_TRACE("Selected non-EQUALS victim at index %d", victim);
                } else if (_index_scan->_search_exprs._exprs[i]._join_expr &&
                           !_index_scan->_search_exprs._exprs[j]._join_expr) {
                    victim = j;
                    MOT_LOG_TRACE("Selected non-JOIN victim at index %d", victim);
                } else if (!_index_scan->_search_exprs._exprs[i]._join_expr &&
                           _index_scan->_search_exprs._exprs[j]._join_expr) {
                    victim = i;
                    MOT_LOG_TRACE("Selected non-JOIN victim at index %d", victim);
                } else if (_index_scan->_search_exprs._exprs[i]._join_expr &&
                           _index_scan->_search_exprs._exprs[j]._join_expr) {
                    // both are join expressions, this is unacceptable, so we abort
                    MOT_LOG_TRACE("RangeScanExpressionCollector(): Disqualifying query - duplicate JOIN expression "
                                  "on index %s in index column %d",
                        _index->GetName().c_str(),
                        _index_ops[i]._index_column_id);
                    return -1;  // signal error
                } else {
                    // both items are not join expressions, both refer to index columns, so we arbitrarily drop one
                    // of them
                    victim = j;
                    MOT_LOG_TRACE("Selected arbitrary duplicate EQUALS victim at index %d", victim);
                }
                // switch victim with last item
                MOT_LOG_TRACE(
                    "Removing victim index op at index %d and putting there index %d", victim, _index_op_count - 1);
                if (_index_scan->_search_exprs._exprs[victim]._expr != nullptr) {
                    freeExpr(_index_scan->_search_exprs._exprs[victim]._expr);
                }
                MOT_LOG_TRACE("Removing victim index op at index %d", victim);
                _index_scan->_search_exprs._exprs[victim] = _index_scan->_search_exprs._exprs[_index_op_count - 1];
                _index_scan->_search_exprs._exprs[_index_op_count - 1]._expr = nullptr;
                _index_ops[victim] = _index_ops[_index_op_count - 1];
                --_index_op_count;
                --_index_scan->_search_exprs._count;
                return 1;
            }
        }
    }

    // nothing changed
    return 0;
}

int RangeScanExpressionCollector::IntCmp(int lhs, int rhs)
{
    int result = 0;
    if (lhs < rhs) {
        result = -1;
    } else if (lhs > rhs) {
        result = 1;
    }
    return result;
}

bool RangeScanExpressionCollector::IndexOpCmp(const IndexOpClass& lhs, const IndexOpClass& rhs)
{
    int result = IntCmp(lhs._index_column_id, rhs._index_column_id);
    if (result == 0) {
        // make sure equals appears before other operators in case column id is equal
        if ((lhs._op_class == JIT_WOC_EQUALS) && (rhs._op_class != JIT_WOC_EQUALS)) {
            result = -1;
        } else if ((lhs._op_class != JIT_WOC_EQUALS) && (rhs._op_class == JIT_WOC_EQUALS)) {
            result = 1;
        }
        // otherwise we keep order intact to avoid misinterpreting open range scan as inverted
    }

    // we return true when strict ascending order is preserved
    return result < 0;
}

bool RangeScanExpressionCollector::ExprCmpImp(const JitColumnExpr& lhs, const JitColumnExpr& rhs)
{
    int lhsIndexColId = MapTableColumnToIndex(_table, _index, lhs._table_column_id);
    int rhsIndexColId = MapTableColumnToIndex(_table, _index, rhs._table_column_id);
    int result = IntCmp(lhsIndexColId, rhsIndexColId);
    if (result == 0) {
        // make sure equals appears before other operators in case column id is equal
        if ((lhs._op_class == JIT_WOC_EQUALS) && (rhs._op_class != JIT_WOC_EQUALS)) {
            result = -1;
        } else if ((lhs._op_class != JIT_WOC_EQUALS) && (rhs._op_class == JIT_WOC_EQUALS)) {
            result = 1;
        }
        // otherwise we keep order intact to avoid misinterpreting open range scan as inverted
    }

    // we return true when strict ascending order is preserved
    return result < 0;
}

bool RangeScanExpressionCollector::ScanHasHoles(JitIndexScanType scan_type) const
{
    MOT_ASSERT(_index_op_count >= 1);

    // closed and semi-open scans expect to see all columns in increasing order beginning from zero
    int column_count = _index_op_count - 1;
    if (scan_type != JIT_INDEX_SCAN_OPEN) {
        column_count = _index_op_count;
    }

    // full prefix must begin with index column zero
    if (_index_ops[0]._index_column_id != 0) {
        MOT_LOG_TRACE(
            "RangeScanExpressionCollector(): Disqualifying query - Index scan does not begin with index column 0");
        for (int i = 0; i < _index_op_count; ++i) {
            MOT_LOG_TRACE("Index column id %d: %d", i, _index_ops[i]._index_column_id);
        }
        return true;
    }

    // check each operation relates to the next index column
    for (int i = 1; i < column_count; ++i) {
        int prev_column = _index_ops[i - 1]._index_column_id;
        int next_column = _index_ops[i]._index_column_id;
        if (next_column != (prev_column + 1)) {
            MOT_LOG_TRACE("RangeScanExpressionCollector(): Disqualifying query - found hole in closed or semi-open "
                          "range scan from index column %d to %d",
                prev_column,
                next_column);
            return true;
        }
    }

    // in open scan we expect two last columns to be equal
    if (scan_type == JIT_INDEX_SCAN_OPEN) {
        MOT_ASSERT(_index_op_count >= 2);
        int prev_column = _index_ops[_index_op_count - 2]._index_column_id;
        int next_column = _index_ops[_index_op_count - 1]._index_column_id;
        if (next_column != prev_column) {
            MOT_LOG_TRACE("RangeScanExpressionCollector(): Disqualifying query - last two columns in open index "
                          "scan are not equals: %d, %d",
                prev_column,
                next_column);
            return true;
        }
    }

    return false;
}

bool FilterCollector::OnFilterExpr(Expr* expr)
{
    bool hasVarExpr = false;
    JitExpr* filterExpr = parseExpr(_query, expr, 0, &hasVarExpr);
    if (filterExpr == nullptr) {
        MOT_LOG_TRACE("FilterCollector::onFilterExpr(): Failed to parse filter expression %d", *_filter_count);
        Cleanup();
        return false;
    }
    bool isOneTimeFilter = !hasVarExpr;
    if (isOneTimeFilter) {
        if (*m_oneTimeFilterCount < m_oneTimeFilterArray->_filter_count) {
            m_oneTimeFilterArray->_scan_filters[*m_oneTimeFilterCount] = filterExpr;
            ++(*m_oneTimeFilterCount);
            MOT_LOG_TRACE("FilterCollector::onFilterExpr():Collected one-time filter");
            return true;
        } else {
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                "Prepare JIT Plan",
                "Exceeded one-time filter count %d",
                m_oneTimeFilterArray->_filter_count);
            return false;
        }
    } else {
        if (*_filter_count < _filter_array->_filter_count) {
            _filter_array->_scan_filters[*_filter_count] = filterExpr;
            ++(*_filter_count);
            return true;
        } else {
            MOT_REPORT_ERROR(
                MOT_ERROR_INTERNAL, "Prepare JIT Plan", "Exceeded filter count %d", _filter_array->_filter_count);
            return false;
        }
    }
}

void FilterCollector::Cleanup()
{
    for (int i = 0; i < *_filter_count; ++i) {
        freeExpr(_filter_array->_scan_filters[i]);
        _filter_array->_scan_filters[i] = nullptr;
    }
    for (int i = 0; i < *m_oneTimeFilterCount; ++i) {
        freeExpr(m_oneTimeFilterArray->_scan_filters[i]);
        m_oneTimeFilterArray->_scan_filters[i] = nullptr;
    }
}

bool SubLinkFetcher::OnExpression(
    Expr* expr, int columnType, int tableColumnId, MOT::Table* table, JitWhereOperatorClass opClass, bool joinExpr)
{
    if (opClass != JIT_WOC_EQUALS) {
        MOT_LOG_TRACE("SubLinkFetcher::onExpression(): Skipping non-equals operator");
        return true;  // this is not an error condition
    }
    if (expr->type == T_SubLink) {
        if (++_count > 1) {
            MOT_LOG_TRACE("SubLinkFetcher::onExpression(): encountered more than one sub-link");
            return false;  // already have a sub-link, we disqualify query
        }
        SubLink* subLink = (SubLink*)expr;
        if (subLink->subLinkType != EXPR_SUBLINK) {
            MOT_LOG_TRACE("SubLinkFetcher::onExpression(): unsupported sub-link type");
            return false;  // unsupported sub-link type, we disqualify query
        }
        if (subLink->testexpr != nullptr) {
            MOT_LOG_TRACE("SubLinkFetcher::onExpression(): unsupported sub-link outer test expression");
            return false;  // unsupported sub-link type, we disqualify query
        }
#ifdef JIT_SUPPORT_FOR_SUB_LINK
        MOT_ASSERT(_subLink == nullptr);
        _subLink = subLink;
#endif
    }
    return true;
}

MOT::Table* getRealTable(const Query* query, int table_ref_id, int column_id)
{
    MOT::Table* table = nullptr;
    MOT::TxnManager* currTxn = GetSafeTxn(__FUNCTION__);
    MOT_ASSERT(currTxn != nullptr);
    if (table_ref_id > list_length(query->rtable)) {  // varno index is 1-based
        MOT_LOG_TRACE("getRealTable(): Invalid table reference id %d", table_ref_id);
    } else {
        RangeTblEntry* rte = (RangeTblEntry*)list_nth(query->rtable, table_ref_id - 1);
        if (rte->rtekind == RTE_RELATION) {
            table = currTxn->GetTableByExternalId(rte->relid);
            if (table == nullptr) {
                MOT_LOG_TRACE("getRealTable(): Could not find table by external id %d", rte->relid);
            }
        } else if (rte->rtekind == RTE_JOIN) {
            Var* alias_var = (Var*)list_nth(rte->joinaliasvars, column_id - 1);  // this is zero-based!
            table_ref_id = alias_var->varno;
            if (table_ref_id > list_length(query->rtable)) {  // table_ref_id is 1-based
                MOT_LOG_TRACE("getRealTable(): Invalid indirect table ref index %d", table_ref_id);
            } else {
                rte = (RangeTblEntry*)list_nth(query->rtable, table_ref_id - 1);
                table = currTxn->GetTableByExternalId(rte->relid);
                if (table == nullptr) {
                    MOT_LOG_TRACE("getRealTable(): Could not find table by indirected external id %d", rte->relid);
                }
            }
        }
    }

    MOT_LOG_TRACE("getRealTable(): table_ref_id=%d, column_id=%d --> table=%p", table_ref_id, column_id, table);
    return table;
}

int getRealColumnId(const Query* query, int table_ref_id, int column_id, const MOT::Table* table)
{
    MOT_LOG_DEBUG("getRealColumnId(): table_ref_id = %d, column_id = %d", table_ref_id, column_id);
    if (table_ref_id > list_length(query->rtable)) {  // varno index is 1-based
        MOT_LOG_TRACE("getRealColumnId(): Invalid table reference id %d", table_ref_id);
        column_id = -1;  // signal error
    } else {
        RangeTblEntry* rte = (RangeTblEntry*)list_nth(query->rtable, table_ref_id - 1);
        if (rte->rtekind == RTE_RELATION) {
            if (rte->relid != table->GetTableExId()) {
                column_id = -2;  // signal irrelevant column
                MOT_LOG_TRACE("getRealColumnId(): Skipping var reference of another table %d", (int)rte->relid);
            }
        } else if (rte->rtekind == RTE_JOIN) {
            Var* alias_var = (Var*)list_nth(rte->joinaliasvars, column_id - 1);  // this is zero-based!
            table_ref_id = alias_var->varno;
            if (table_ref_id > list_length(query->rtable)) {  // table_ref_id is 1-based
                column_id = -1;                               // signal error
                MOT_LOG_TRACE("getRealColumnId(): Invalid indirect table ref index %d", table_ref_id);
            } else {
                rte = (RangeTblEntry*)list_nth(query->rtable, table_ref_id - 1);
                if (rte->relid != table->GetTableExId()) {
                    column_id = -2;  // signal irrelevant column
                    MOT_LOG_TRACE("getRealColumnId(): Skipping var reference of another table %d", (int)rte->relid);
                } else {
                    // take real column id and not column id from virtual join table
                    MOT_LOG_DEBUG("getRealColumnId(): Replacing join column id %d with real column id %d of table %s",
                        column_id,
                        (int)alias_var->varattno,
                        table->GetTableName().c_str());
                    column_id = alias_var->varattno;
                }
            }
        } else {
            column_id = -1;  // signal error
            MOT_LOG_TRACE("getRealColumnId(): Invalid relation kind %d", rte->rtekind);
        }
    }

    MOT_LOG_DEBUG("getRealColumnId(): RESULT - table_ref_id = %d, column_id = %d", table_ref_id, column_id);
    return column_id;
}

static JitExpr* parseConstExpr(const Const* const_expr)
{
    if (!IsTypeSupported(const_expr->consttype)) {
        MOT_LOG_TRACE("Disqualifying constant expression: constant type %d is unsupported", (int)const_expr->consttype);
        return nullptr;
    }

    size_t alloc_size = sizeof(JitConstExpr);
    JitConstExpr* result = (JitConstExpr*)MOT::MemSessionAlloc(alloc_size);
    if (result == nullptr) {
        MOT_REPORT_ERROR(
            MOT_ERROR_OOM, "Prepare JIT Plan", "Failed to allocate %u bytes for constant expression", alloc_size);
    } else {
        result->_expr_type = JIT_EXPR_TYPE_CONST;
        result->_const_type = const_expr->consttype;
        result->m_collationId = const_expr->constcollid;
        result->_value = const_expr->constvalue;
        result->_is_null = const_expr->constisnull;
    }
    return (JitExpr*)result;
}

static JitExpr* parseParamExpr(const Param* param_expr)
{
    if (!IsTypeSupported(param_expr->paramtype)) {
        MOT_LOG_TRACE(
            "Disqualifying parameter expression: parameter type %d is unsupported", (int)param_expr->paramtype);
        return nullptr;
    }

    size_t alloc_size = sizeof(JitParamExpr);
    JitParamExpr* result = (JitParamExpr*)MOT::MemSessionAlloc(alloc_size);
    if (result == nullptr) {
        MOT_REPORT_ERROR(
            MOT_ERROR_OOM, "Prepare JIT Plan", "Failed to allocate %u bytes for parameter expression", alloc_size);
    } else {
        result->_expr_type = JIT_EXPR_TYPE_PARAM;
        result->_param_type = param_expr->paramtype;
        result->m_collationId = param_expr->paramcollid;
        result->_param_id = param_expr->paramid - 1;  // move to zero-based index
    }
    return (JitExpr*)result;
}

static JitExpr* parseVarExpr(Query* query, const Var* var_expr)
{
    // make preliminary tests before memory allocation takes place
    if (!IsTypeSupported(var_expr->vartype)) {
        MOT_LOG_TRACE("Disqualifying var expression: var type %d is unsupported", (int)var_expr->vartype);
        return nullptr;
    }

    MOT::Table* table = getRealTable(query, var_expr->varno, var_expr->varattno);
    if (table == nullptr) {
        MOT_LOG_TRACE("parseVarExpr(): Failed to retrieve source table by table ref id %u and column id %d",
            var_expr->varno,
            var_expr->varattno);
        return nullptr;
    }
    int column_id = getRealColumnId(query, var_expr->varno, var_expr->varattno, table);
    if (column_id < 0) {
        MOT_LOG_TRACE("parseVarExpr(): Failed to retrieve column id by table ref id %d and column id %d (table %s)",
            var_expr->varno,
            var_expr->varattno,
            table->GetTableName().c_str());
        return nullptr;
    }

    size_t alloc_size = sizeof(JitVarExpr);
    JitVarExpr* result = (JitVarExpr*)MOT::MemSessionAlloc(alloc_size);
    if (result == nullptr) {
        MOT_REPORT_ERROR(
            MOT_ERROR_OOM, "Prepare JIT Plan", "Failed to allocate %u bytes for var expression", alloc_size);
    } else {
        result->_expr_type = JIT_EXPR_TYPE_VAR;
        result->_column_type = var_expr->vartype;
        result->m_collationId = var_expr->varcollid;
        result->_table = table;
        result->_column_id = column_id;
    }

    return (JitExpr*)result;
}

static JitExpr* parseRelabelExpr(Query* query, RelabelType* relabel_type, bool* hasVarExpr)
{
    JitExpr* result = nullptr;
    if (relabel_type->arg->type == T_Param) {
        result = parseParamExpr((Param*)relabel_type->arg);
        if (result != nullptr) {
            // replace result type with relabeled type
            ((JitParamExpr*)result)->_param_type = relabel_type->resulttype;
        }
    } else if (relabel_type->arg->type == T_Var) {
        if (hasVarExpr != nullptr) {
            *hasVarExpr = true;
        }
        result = parseVarExpr(query, (Var*)relabel_type->arg);
        if (result != nullptr) {
            // replace result type with relabeled type
            ((JitVarExpr*)result)->_column_type = relabel_type->resulttype;
        }
    } else {
        MOT_LOG_TRACE("parseRelabelExpr(): Unsupported relabel type %d", (int)relabel_type->arg->type);
        return nullptr;
    }

    return (JitExpr*)result;
}

static bool ValidateFuncCallExpr(Expr* expr)
{
    Oid resultType;
    Oid funcId;
    List* args;
    Oid oidValue;
    bool returnsSet = false;

    if (expr->type == T_OpExpr) {
        OpExpr* opExpr = (OpExpr*)expr;
        resultType = opExpr->opresulttype;
        funcId = opExpr->opfuncid;
        args = opExpr->args;
        oidValue = opExpr->opno;  // with operator expression we prefer printing the operator id for easier lookup
        returnsSet = opExpr->opretset;
    } else if (expr->type == T_FuncExpr) {
        FuncExpr* funcExpr = (FuncExpr*)expr;
        resultType = funcExpr->funcresulttype;
        funcId = funcExpr->funcid;
        args = funcExpr->args;
        oidValue = funcExpr->funcid;
        returnsSet = funcExpr->funcretset;
    } else {
        MOT_LOG_TRACE("ValidateFuncOpExpr(): Invalid expression type %u", expr->type);
        return false;
    }

    const char* exprName = (expr->type == T_OpExpr) ? "operator" : "function";
    if (!IsTypeSupported(resultType)) {
        MOT_LOG_TRACE("Disqualifying %s expression: result type %u is unsupported", exprName, resultType);
        return false;
    }

    if (!IsFuncIdSupported(funcId)) {
        MOT_LOG_TRACE("Disqualifying %s expression: operator function id %u is unsupported", exprName, funcId);
        return false;
    }

    if (list_length(args) > MOT_JIT_MAX_FUNC_EXPR_ARGS) {
        MOT_LOG_TRACE("Unsupported %s %u: too many arguments", exprName, oidValue);
        return false;
    }

    if (returnsSet) {
        MOT_LOG_TRACE("Unsupported %s %u: expression returns set", exprName, oidValue);
        return false;
    }

    return true;
}

static JitExpr* parseOpExpr(Query* query, const OpExpr* op_expr, int depth, bool* hasVarExpr)
{
    if (!ValidateFuncCallExpr((Expr*)op_expr)) {
        MOT_LOG_TRACE("Disqualifying invalid operator expression");
        return nullptr;
    }

    int argCount = list_length(op_expr->args);
    size_t allocSize = sizeof(JitExpr*) * argCount;
    JitExpr** args = (JitExpr**)MOT::MemSessionAlloc(allocSize);
    if (args == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Prepare JIT Plan",
            "Failed to allocate %u bytes for %d arguments in operator expression",
            (unsigned)allocSize,
            argCount);
        return nullptr;
    }
    int arg_num = 0;
    ListCell* lc = nullptr;
    foreach (lc, op_expr->args) {
        Expr* sub_expr = (Expr*)lfirst(lc);
        args[arg_num] = parseExpr(query, sub_expr, depth + 1, hasVarExpr);
        if (args[arg_num] == nullptr) {
            MOT_LOG_TRACE("Failed to process operator sub-expression %d", arg_num);
            for (int i = 0; i < arg_num; ++i) {
                freeExpr(args[i]);
            }
            MOT::MemSessionFree(args);
            return nullptr;
        }
        ++arg_num;
    }
    MOT_ASSERT(arg_num == argCount);

    allocSize = sizeof(JitOpExpr);
    JitOpExpr* result = (JitOpExpr*)MOT::MemSessionAlloc(allocSize);
    if (result == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Prepare JIT Plan",
            "Failed to allocate %u bytes for operator expression",
            (unsigned)allocSize);
        for (int i = 0; i < arg_num; ++i) {
            freeExpr(args[i]);
        }
        MOT::MemSessionFree(args);
        return nullptr;
    }

    result->_expr_type = JIT_EXPR_TYPE_OP;
    result->_op_no = op_expr->opno;
    result->_op_func_id = op_expr->opfuncid;
    result->m_collationId = op_expr->opcollid;
    result->m_opCollationId = op_expr->inputcollid;
    result->_result_type = op_expr->opresulttype;
    result->_arg_count = arg_num;
    result->_args = args;

    return (JitExpr*)result;
}

static JitExpr* parseFuncExpr(Query* query, const FuncExpr* func_expr, int depth, bool* hasVarExpr)
{
    if (!ValidateFuncCallExpr((Expr*)func_expr)) {
        MOT_LOG_TRACE("Disqualifying invalid function expression");
        return nullptr;
    }

    int argCount = list_length(func_expr->args);
    size_t allocSize = sizeof(JitExpr*) * argCount;
    JitExpr** args = (JitExpr**)MOT::MemSessionAlloc(allocSize);
    if (args == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Prepare JIT Plan",
            "Failed to allocate %u bytes for %d arguments in function expression",
            (unsigned)allocSize,
            argCount);
        return nullptr;
    }
    int arg_num = 0;
    ListCell* lc = nullptr;
    foreach (lc, func_expr->args) {
        Expr* sub_expr = (Expr*)lfirst(lc);
        args[arg_num] = parseExpr(query, sub_expr, depth + 1, hasVarExpr);
        if (args[arg_num] == nullptr) {
            MOT_LOG_TRACE("Failed to process function sub-expression %d", arg_num);
            for (int i = 0; i < arg_num; ++i) {
                freeExpr(args[i]);
            }
            MOT::MemSessionFree(args);
            return nullptr;
        }
        ++arg_num;
    }
    MOT_ASSERT(arg_num == argCount);

    allocSize = sizeof(JitFuncExpr);
    JitFuncExpr* result = (JitFuncExpr*)MOT::MemSessionAlloc(allocSize);
    if (result == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Prepare JIT Plan",
            "Failed to allocate %u bytes for function expression",
            (unsigned)allocSize);
        for (int i = 0; i < arg_num; ++i) {
            freeExpr(args[i]);
        }
        MOT::MemSessionFree(args);
        return nullptr;
    }

    result->_expr_type = JIT_EXPR_TYPE_FUNC;
    result->_func_id = func_expr->funcid;
    result->m_collationId = func_expr->funccollid;
    result->m_funcCollationId = func_expr->inputcollid;
    result->_result_type = func_expr->funcresulttype;
    result->_arg_count = arg_num;
    result->_args = args;

    return (JitExpr*)result;
}

static JitExpr* ParseSubLink(Query* query, const SubLink* subLink, int depth)
{
    Query* subQuery = (Query*)subLink->subselect;

    // get the result type
    int resultType = 0;
    TargetEntry* targetEntry = (TargetEntry*)linitial(subQuery->targetList);
    if (targetEntry->expr->type == T_Var) {
        resultType = ((Var*)targetEntry->expr)->vartype;
    } else if (targetEntry->expr->type == T_Aggref) {
        resultType = ((Aggref*)targetEntry->expr)->aggtype;
    } else {
        MOT_LOG_TRACE("Disqualifying sub-link expression: unexpected target entry expression type: %d",
            (int)targetEntry->expr->type);
        return nullptr;
    }

    if (!IsTypeSupported(resultType)) {
        MOT_LOG_TRACE("Disqualifying sub-link expression: result type %d is unsupported", resultType);
        return nullptr;
    }

    size_t alloc_size = sizeof(JitSubLinkExpr);
    JitSubLinkExpr* result = (JitSubLinkExpr*)MOT::MemSessionAlloc(alloc_size);
    if (result == nullptr) {
        MOT_REPORT_ERROR(
            MOT_ERROR_OOM, "Prepare JIT Plan", "Failed to allocate %u bytes for sub-link expression", alloc_size);
    } else {
        result->_expr_type = JIT_EXPR_TYPE_SUBLINK;
        result->_source_expr = (Expr*)subLink;
        result->_result_type = resultType;
        result->_sub_query_index = 0;  // currently the only valid value for a single sub-query
    }

    return (JitExpr*)result;
}

static JitExpr* ParseBoolExpr(Query* query, const BoolExpr* boolExpr, int depth, bool* hasVarExpr)
{
    if (list_length(boolExpr->args) > MOT_JIT_MAX_BOOL_EXPR_ARGS) {
        MOT_LOG_TRACE("Unsupported Boolean operator: too many arguments");
        return nullptr;
    }

    JitExpr* args[MOT_JIT_MAX_BOOL_EXPR_ARGS] = {nullptr, nullptr};
    int argNum = 0;

    ListCell* lc = nullptr;
    foreach (lc, boolExpr->args) {
        Expr* subExpr = (Expr*)lfirst(lc);
        args[argNum] = parseExpr(query, subExpr, depth + 1, hasVarExpr);
        if (args[argNum] == nullptr) {
            MOT_LOG_TRACE("Failed to process operator sub-expression %d", argNum);
            for (int i = 0; i < argNum; ++i) {
                freeExpr(args[i]);
            }
            return nullptr;
        }
        if (++argNum == MOT_JIT_MAX_BOOL_EXPR_ARGS) {
            break;
        }
    }

    size_t allocSize = sizeof(JitBoolExpr);
    JitBoolExpr* result = (JitBoolExpr*)MOT::MemSessionAlloc(allocSize);
    if (result == nullptr) {
        MOT_REPORT_ERROR(
            MOT_ERROR_OOM, "Prepare JIT Plan", "Failed to allocate %u bytes for Boolean expression", allocSize);
        for (int i = 0; i < argNum; ++i) {
            freeExpr(args[i]);
        }
        return nullptr;
    }

    result->_expr_type = JIT_EXPR_TYPE_BOOL;
    result->_source_expr = (Expr*)boolExpr;
    result->m_collationId = InvalidOid;
    result->_result_type = BOOLOID;
    result->_bool_expr_type = boolExpr->boolop;
    result->_arg_count = argNum;
    for (int i = 0; i < argNum; ++i) {
        result->_args[i] = args[i];
    }

    return (JitExpr*)result;
}

static JitExpr* ParseScalarArrayOpExpr(Query* query, const ScalarArrayOpExpr* expr, int depth, bool* hasVarExpr)
{
    if (list_length(expr->args) > MOT_JIT_MAX_BOOL_EXPR_ARGS) {
        MOT_LOG_TRACE("Unsupported Scalar Array operator: too many arguments");
        return nullptr;
    }

    JitExpr* scalarExpr = parseExpr(query, (Expr*)linitial(expr->args), depth + 1, hasVarExpr);
    if (scalarExpr == nullptr) {
        MOT_LOG_TRACE("Failed to parse scalar expression");
        return nullptr;
    }
    Expr* arrayExpr = (Expr*)lsecond(expr->args);
    if (arrayExpr->type != T_ArrayExpr) {
        MOT_LOG_TRACE("Unexpected expr type for array argument: %d", (int)arrayExpr->type);
        freeExpr(scalarExpr);
        return nullptr;
    }
    ArrayExpr* arr = (ArrayExpr*)arrayExpr;
    if (arr->multidims) {
        MOT_LOG_TRACE("Unsupported multi-dimensional array");
        freeExpr(scalarExpr);
        return nullptr;
    }
    if (!IsTypeSupported(arr->element_typeid)) {
        MOT_LOG_TRACE("Unsupported array element type %u in Scalar Array Op", (unsigned)arr->element_typeid);
        freeExpr(scalarExpr);
        return nullptr;
    }

    size_t allocSize = sizeof(JitScalarArrayOpExpr);
    JitScalarArrayOpExpr* result = (JitScalarArrayOpExpr*)MOT::MemSessionAlloc(allocSize);
    if (result == nullptr) {
        MOT_REPORT_ERROR(
            MOT_ERROR_OOM, "Prepare JIT Plan", "Failed to allocate %u bytes for Scalar array expression", allocSize);
        freeExpr(scalarExpr);
        return nullptr;
    }

    result->_expr_type = JIT_EXPR_TYPE_SCALAR_ARRAY_OP;
    result->_source_expr = (Expr*)expr;
    result->_result_type = BOOLOID;
    result->m_collationId = expr->inputcollid;
    result->m_funcCollationId = expr->inputcollid;
    result->_op_no = expr->opno;
    result->_op_func_id = expr->opfuncid;
    result->m_useOr = expr->useOr;
    result->m_scalar = scalarExpr;
    result->m_arraySize = list_length(arr->elements);
    if (result->m_arraySize <= 0) {
        result->m_arraySize = 0;
        result->m_arrayElements = nullptr;
    } else {
        allocSize = sizeof(JitExpr*) * result->m_arraySize;
        result->m_arrayElements = (JitExpr**)MOT::MemSessionAlloc(allocSize);
        if (result->m_arrayElements == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM,
                "Prepare JIT Plan",
                "Failed to allocate %u bytes for %u Scalar array elements",
                allocSize,
                result->m_arrayElements);
            FreeScalarArrayOpExpr(result);
            return nullptr;
        } else {
            errno_t erc = memset_s(result->m_arrayElements, allocSize, 0, allocSize);
            securec_check(erc, "\0", "\0");

            int elementIndex = 0;
            ListCell* arg = nullptr;
            foreach (arg, arr->elements) {
                MOT_ASSERT(elementIndex < result->m_arraySize);
                Expr* e = (Expr*)lfirst(arg);
                result->m_arrayElements[elementIndex] = parseExpr(query, e, depth, hasVarExpr);
                if (result->m_arrayElements[elementIndex] == nullptr) {
                    MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                        "Prepare JIT Plan",
                        "Failed to parse array element %d in Scalar array elements",
                        elementIndex);
                    FreeScalarArrayOpExpr(result);
                    return nullptr;
                }
                ++elementIndex;
            }
        }
    }

    return (JitExpr*)result;
}

static JitExpr* ParseCoerceViaIOExpr(Query* query, const CoerceViaIO* expr, int depth, bool* hasVarExpr)
{
    JitExpr* arg = parseExpr(query, expr->arg, depth + 1, hasVarExpr);
    if (arg == nullptr) {
        MOT_LOG_TRACE("Failed to parse coerce via IO input expression");
        return nullptr;
    }

    size_t allocSize = sizeof(JitCoerceViaIOExpr);
    JitCoerceViaIOExpr* result = (JitCoerceViaIOExpr*)MOT::MemSessionAlloc(allocSize);
    if (result == nullptr) {
        MOT_REPORT_ERROR(
            MOT_ERROR_OOM, "Prepare JIT Plan", "Failed to allocate %u bytes for Coerce via IO expression", allocSize);
        freeExpr(arg);
        return nullptr;
    }

    result->_expr_type = JIT_EXPR_TYPE_COERCE_VIA_IO;
    result->_source_expr = (Expr*)expr;
    result->_result_type = expr->resulttype;
    result->m_collationId = expr->resultcollid;
    result->m_arg = arg;

    return (JitExpr*)result;
}

static bool RelabelHasVarRef(RelabelType* relabel_type)
{
    if (relabel_type->arg->type == T_Var) {
        return true;
    }

    return false;
}

static bool OpExprHasVarRef(const OpExpr* op_expr, int depth)
{
    ListCell* lc = nullptr;
    foreach (lc, op_expr->args) {
        Expr* sub_expr = (Expr*)lfirst(lc);
        if (ExprHasVarRef(sub_expr, depth + 1)) {
            return true;
        }
    }

    return false;
}

static bool FuncExprHasVarRef(const FuncExpr* func_expr, int depth)
{
    ListCell* lc = nullptr;
    foreach (lc, func_expr->args) {
        Expr* sub_expr = (Expr*)lfirst(lc);
        if (ExprHasVarRef(sub_expr, depth + 1)) {
            return true;
        }
    }

    return false;
}

static bool BoolExprHasVarRef(const BoolExpr* boolExpr, int depth)
{
    if (list_length(boolExpr->args) > MOT_JIT_MAX_BOOL_EXPR_ARGS) {
        MOT_LOG_TRACE("Unsupported Boolean operator: too many arguments");
        return false;
    }

    ListCell* lc = nullptr;
    foreach (lc, boolExpr->args) {
        Expr* subExpr = (Expr*)lfirst(lc);
        if (ExprHasVarRef(subExpr, depth + 1)) {
            return true;
        }
    }

    return false;
}

static bool ScalarArrayOpExprHasVarRef(const ScalarArrayOpExpr* expr, int depth)
{
    if (list_length(expr->args) > MOT_JIT_MAX_BOOL_EXPR_ARGS) {
        MOT_LOG_TRACE("Unsupported Scalar Array operator: too many arguments");
        return false;
    }

    if (ExprHasVarRef((Expr*)linitial(expr->args), depth + 1)) {
        return true;
    }
    Expr* arrayExpr = (Expr*)lsecond(expr->args);
    if (arrayExpr->type != T_ArrayExpr) {
        MOT_LOG_TRACE("Unexpected expression type for array argument: %d", (int)arrayExpr->type);
        return false;
    }
    ArrayExpr* arr = (ArrayExpr*)arrayExpr;
    if (arr->multidims) {
        MOT_LOG_TRACE("Unsupported multi-dimensional array");
        return false;
    }

    ListCell* arg = nullptr;
    foreach (arg, arr->elements) {
        Expr* e = (Expr*)lfirst(arg);
        if (ExprHasVarRef(e, depth)) {
            return true;
        }
    }

    return false;
}

static bool ExprHasVarRef(Expr* expr, int depth)
{
    if (depth > MOT_JIT_MAX_EXPR_DEPTH) {
        MOT_LOG_TRACE(
            "Cannot check for var expression: Expression exceeds depth limit %d", (int)MOT_JIT_MAX_EXPR_DEPTH);
        return false;
    }

    bool result = false;
    if (expr->type == T_Var) {
        result = true;
    } else if (expr->type == T_RelabelType) {
        result = RelabelHasVarRef((RelabelType*)expr);
    } else if (expr->type == T_OpExpr) {
        result = OpExprHasVarRef((OpExpr*)expr, depth);
    } else if (expr->type == T_FuncExpr) {
        result = FuncExprHasVarRef((FuncExpr*)expr, depth);
    } else if (expr->type == T_BoolExpr) {
        result = BoolExprHasVarRef((BoolExpr*)expr, depth);
    } else if (expr->type == T_ScalarArrayOpExpr) {
        result = ScalarArrayOpExprHasVarRef((ScalarArrayOpExpr*)expr, depth);
    }

    return result;
}

JitExpr* parseExpr(Query* query, Expr* expr, int depth, bool* hasVarExpr /* = nullptr */)
{
    JitExpr* result = nullptr;

    if (depth > MOT_JIT_MAX_EXPR_DEPTH) {
        MOT_LOG_TRACE("Cannot parse expression: Expression exceeds depth limit %d", (int)MOT_JIT_MAX_EXPR_DEPTH);
        return nullptr;
    }

    if (expr->type == T_Const) {
        result = parseConstExpr((Const*)expr);
    } else if (expr->type == T_Param) {
        result = parseParamExpr((Param*)expr);
    } else if (expr->type == T_Var) {
        if (hasVarExpr != nullptr) {
            *hasVarExpr = true;
        }
        result = parseVarExpr(query, (Var*)expr);
    } else if (expr->type == T_RelabelType) {
        result = parseRelabelExpr(query, (RelabelType*)expr, hasVarExpr);
    } else if (expr->type == T_OpExpr) {
        result = parseOpExpr(query, (OpExpr*)expr, depth, hasVarExpr);
    } else if (expr->type == T_FuncExpr) {
        result = parseFuncExpr(query, (FuncExpr*)expr, depth, hasVarExpr);
    } else if (expr->type == T_SubLink) {
        result = ParseSubLink(query, (SubLink*)expr, depth);
    } else if (expr->type == T_BoolExpr) {
        result = ParseBoolExpr(query, (BoolExpr*)expr, depth, hasVarExpr);
    } else if (expr->type == T_ScalarArrayOpExpr) {
        result = ParseScalarArrayOpExpr(query, (ScalarArrayOpExpr*)expr, depth, hasVarExpr);
    } else if (expr->type == T_CoerceViaIO) {
        result = ParseCoerceViaIOExpr(query, (CoerceViaIO*)expr, depth, hasVarExpr);
    } else {
        MOT_LOG_TRACE("Disqualifying expression: unsupported target expression type %d", (int)expr->type);
    }

    if (result != nullptr) {
        result->_source_expr = expr;
    }
    return result;
}

static void freeOpExpr(JitOpExpr* op_expr)
{
    for (int i = 0; i < op_expr->_arg_count; ++i) {
        freeExpr(op_expr->_args[i]);
    }
    MOT::MemSessionFree(op_expr->_args);
    op_expr->_args = nullptr;
}

static void freeFuncExpr(JitFuncExpr* func_expr)
{
    for (int i = 0; i < func_expr->_arg_count; ++i) {
        freeExpr(func_expr->_args[i]);
    }
    MOT::MemSessionFree(func_expr->_args);
    func_expr->_args = nullptr;
}

static void FreeScalarArrayOpExpr(JitScalarArrayOpExpr* expr)
{
    if (expr != nullptr) {
        if (expr->m_scalar != nullptr) {
            freeExpr(expr->m_scalar);
        }
        if (expr->m_arrayElements != nullptr) {
            for (int i = 0; i < expr->m_arraySize; ++i) {
                if (expr->m_arrayElements[i] != nullptr) {
                    freeExpr(expr->m_arrayElements[i]);
                }
            }
            MOT::MemSessionFree(expr->m_arrayElements);
        }
    }
}

void freeExpr(JitExpr* expr)
{
    if (expr != nullptr) {
        switch (expr->_expr_type) {
            case JIT_EXPR_TYPE_OP:
                freeOpExpr((JitOpExpr*)expr);
                break;

            case JIT_EXPR_TYPE_FUNC:
                freeFuncExpr((JitFuncExpr*)expr);
                break;

            case JIT_EXPR_TYPE_SCALAR_ARRAY_OP:
                FreeScalarArrayOpExpr((JitScalarArrayOpExpr*)expr);
                break;

            default:
                break;
        }

        MOT::MemSessionFree(expr);
    }
}

static bool containsExpr(const JitColumnExprArray* pkey_exprs, const Expr* expr)
{
    for (int i = 0; i < pkey_exprs->_count; ++i) {
        if (pkey_exprs->_exprs[i]._expr->_source_expr == expr) {
            return true;
        }
    }
    return false;
}

enum TableExprClass {
    TableExprNeutral,  // does not refer any table
    TableExprPKey,     // refers the specified table and is a pkey column reference
    TableExprFilter,   // refers the specified table and is a filter column reference
    TableExprInvalid,  // refers another table
    TableExprError     // error occurred while processing expression
};

static TableExprClass classifyTableExpr(Query* query, MOT::Table* table, MOT::Index* index, Expr* expr);

static TableExprClass classifyTableVarExpr(Query* query, MOT::Table* table, MOT::Index* index, const Var* var_expr)
{
    MOT::Table* real_table = getRealTable(query, var_expr->varno, var_expr->varattno);
    if (real_table == nullptr) {
        MOT_LOG_TRACE(
            "Failed to infer table for table ref id %d and column id %d", var_expr->varno, var_expr->varattno);
        return TableExprError;
    } else if (real_table == table) {
        int column_id = getRealColumnId(query, var_expr->varno, var_expr->varattno, table);
        int index_column_id = MapTableColumnToIndex(table, index, column_id);
        if (index_column_id >= 0) {
            MOT_LOG_TRACE(
                "classifyTableVarExpr(): seeing target table/index %s/%s pkey (column_id=%d, index_column_id=%d)",
                table->GetTableName().c_str(),
                index->GetName().c_str(),
                column_id,
                index_column_id);
            return TableExprPKey;
        } else {
            MOT_LOG_TRACE("classifyTableVarExpr(): seeing target table/index %s/%s filter (column_id=%d)",
                table->GetTableName().c_str(),
                index->GetName().c_str(),
                column_id);
            return TableExprFilter;
        }
    } else {
        MOT_LOG_TRACE(
            "Var expression referring to table %s, while looking for table %s (table_ref_id=%d, column_id=%d)",
            real_table->GetTableName().c_str(),
            table->GetTableName().c_str(),
            var_expr->varno,
            var_expr->varattno);
        return TableExprInvalid;
    }
}

static TableExprClass combineTableExprClass(TableExprClass tec1, TableExprClass tec2)
{
    // if either is error then this is error
    if ((tec1 == TableExprError) || (tec2 == TableExprError)) {
        return TableExprError;
    }

    // if either is invalid then this is invalid
    if ((tec1 == TableExprInvalid) || (tec2 == TableExprInvalid)) {
        return TableExprInvalid;
    }

    // if either is pkey then this is a pkey
    if ((tec1 == TableExprPKey) || (tec2 == TableExprPKey)) {
        return TableExprPKey;
    }

    // neither is pkey, so if either is filter then this is a filter
    if ((tec1 == TableExprFilter) || (tec2 == TableExprFilter)) {
        return TableExprFilter;
    }

    // by definition, both must be neutral
    return TableExprNeutral;
}

static TableExprClass clasifyTableExprArgs(Query* query, MOT::Table* table, MOT::Index* index, const List* args)
{
    TableExprClass result = TableExprInvalid;
    int nargs = (int)list_length(args);
    if (nargs == 1) {
        Expr* arg = (Expr*)linitial(args);
        result = classifyTableExpr(query, table, index, arg);
        MOT_LOG_TRACE("clasifyTableExprArgs(): single arg %d", result);
    } else if (nargs == 2) {
        Expr* lhs = (Expr*)linitial(args);
        Expr* rhs = (Expr*)lsecond(args);

        TableExprClass lhs_tec = classifyTableExpr(query, table, index, lhs);
        TableExprClass rhs_tec = classifyTableExpr(query, table, index, rhs);
        MOT_LOG_TRACE("clasifyTableExprArgs(): lhs_tec %d", lhs_tec);
        MOT_LOG_TRACE("clasifyTableExprArgs(): rhs_tec %d", rhs_tec);
        result = combineTableExprClass(lhs_tec, rhs_tec);
    } else if (nargs == 3) {
        Expr* arg1 = (Expr*)linitial(args);
        Expr* arg2 = (Expr*)lsecond(args);
        Expr* arg3 = (Expr*)lthird(args);

        TableExprClass tec1 = classifyTableExpr(query, table, index, arg1);
        TableExprClass tec2 = classifyTableExpr(query, table, index, arg2);
        TableExprClass tec3 = classifyTableExpr(query, table, index, arg3);
        MOT_LOG_TRACE("clasifyTableExprArgs(): tec1 %d", tec1);
        MOT_LOG_TRACE("clasifyTableExprArgs(): tec2 %d", tec2);
        MOT_LOG_TRACE("clasifyTableExprArgs(): tec3 %d", tec3);
        result = combineTableExprClass(combineTableExprClass(tec1, tec2), tec3);
    } else {
        result = TableExprError;
    }
    MOT_LOG_TRACE("clasifyTableExprArgs(): result %d", result);
    return result;
}

static TableExprClass classifyTableOpExpr(Query* query, MOT::Table* table, MOT::Index* index, OpExpr* op_expr)
{
    if (!IsFuncIdSupported(op_expr->opfuncid)) {
        MOT_LOG_TRACE("classifyTableOpExpr(): Unsupported function id %u", op_expr->opfuncid);
        return TableExprError;
    }

    TableExprClass result = clasifyTableExprArgs(query, table, index, op_expr->args);
    if (result == TableExprError) {
        int nargs = (int)list_length(op_expr->args);
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
            "Prepare JIT Plan",
            "Unexpected argument count %d in operator expression type %d",
            nargs,
            (int)op_expr->xpr.type);
    }

    return result;
}

static TableExprClass classifyTableFuncExpr(Query* query, MOT::Table* table, MOT::Index* index, FuncExpr* func_expr)
{
    if (!IsFuncIdSupported(func_expr->funcid)) {
        MOT_LOG_TRACE("classifyTableFuncExpr(): Unsupported function id %d", (int)func_expr->funcid);
        return TableExprError;
    }

    TableExprClass result = clasifyTableExprArgs(query, table, index, func_expr->args);
    if (result == TableExprError) {
        int nargs = (int)list_length(func_expr->args);
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
            "Prepare JIT Plan",
            "Unexpected argument count %d in function expression type %d",
            nargs,
            (int)func_expr->xpr.type);
    }

    return result;
}

static TableExprClass classifyTableExpr(Query* query, MOT::Table* table, MOT::Index* index, Expr* expr)
{
    switch (expr->type) {
        case T_Const:
        case T_Param:
            MOT_LOG_TRACE("classifyTableExpr(): neutral const/param");
            return TableExprNeutral;

        case T_RelabelType:
            return classifyTableExpr(query, table, index, ((RelabelType*)expr)->arg);

        case T_Var:
            return classifyTableVarExpr(query, table, index, (Var*)expr);

        case T_OpExpr:
            return classifyTableOpExpr(query, table, index, (OpExpr*)expr);

        case T_FuncExpr:
            return classifyTableFuncExpr(query, table, index, (FuncExpr*)expr);

        case T_SubLink:
            MOT_LOG_TRACE("classifyTableExpr(): neutral sub-query");
            return TableExprNeutral;

        case T_ScalarArrayOpExpr:
            MOT_LOG_TRACE("classifyTableExpr(): scalar array operation treated as filter");
            return TableExprFilter;

        default:
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                "Prepare JIT Plan",
                "Unexpected expression type %d while trying to determine if an expression refers only to a specific "
                "table");
            return TableExprError;
    }
}

static bool VisitSearchOpExpressionFilters(Query* query, MOT::Table* table, MOT::Index* index, OpExpr* op_expr,
    ExpressionVisitor* visitor, bool include_join_exprs, JitColumnExprArray* pkey_exprs)
{
    // when collecting filters we need to visit only those expressions not visited during pkey collection, but still
    // refer only to this table in addition, the where operator class is not EQUALS, then this is definitely a filter
    // (and nothing else than that), but we still need to verify that it belongs to this table/index
    if (pkey_exprs != nullptr) {
        MOT_LOG_TRACE(
            "visitSearchOpExpression(): Checking if expression %p contains a previously collected pkey expression",
            op_expr);
        Expr* lhs = (Expr*)linitial(op_expr->args);
        Expr* rhs = (Expr*)lsecond(op_expr->args);
        if (containsExpr(pkey_exprs, lhs) || containsExpr(pkey_exprs, rhs)) {
            MOT_LOG_TRACE("visitSearchOpExpression(): expression %p contains a pkey expression (LHS %p or RHS %p)",
                op_expr,
                lhs,
                rhs);
        } else {
            MOT_LOG_TRACE(
                "visitSearchOpExpression(): expression does not contain a pkey expression, classifying for filter");
            TableExprClass lhs_tec = classifyTableExpr(query, table, index, lhs);
            TableExprClass rhs_tec = classifyTableExpr(query, table, index, rhs);
            MOT_LOG_TRACE("visitSearchOpExpression(): LHS table expression is %d", (int)lhs_tec);
            MOT_LOG_TRACE("visitSearchOpExpression(): RHS table expression is %d", (int)rhs_tec);
            TableExprClass tec = combineTableExprClass(lhs_tec, rhs_tec);
            if (tec == TableExprError) {
                MOT_LOG_TRACE(
                    "visitSearchOpExpression(): Encountered error while classifying table expression for filters");
                return false;
            } else if ((tec == TableExprInvalid) && !include_join_exprs) {
                MOT_LOG_TRACE("visitSearchOpExpression(): Skipping another table expression for filters (join "
                              "expressions excluded)");
            } else {
                if (tec == TableExprPKey) {
                    MOT_LOG_TRACE(
                        "visitSearchOpExpression(): Collecting duplicate primary key table expression for filters");
                }
                if (op_expr->opresulttype != BOOLOID) {
                    MOT_LOG_TRACE(
                        "visitSearchOpExpression(): Disqualifying query - filter result type %d is unsupported",
                        op_expr->opresulttype);
                } else {
                    MOT_LOG_TRACE("visitSearchOpExpression(): Collecting filter expression %p", op_expr);
                    if (!visitor->OnFilterExpr((Expr*)op_expr)) {
                        MOT_LOG_TRACE("visitSearchOpExpression(): Expression collection failed");
                        return false;
                    }
                }
            }
        }
    }

    return true;
}

static bool SkipKeyColumn(MOT::Table* table, MOT::Index* index, bool include_pkey, int colid, int index_colid)
{
    if (include_pkey && (index_colid < 0)) {  // ordered to include only pkey and column is non-pkey so skip
        MOT_LOG_TRACE("visitSearchOpExpression(): Skipping non-index key column %d %s (ordered to include "
                      "only index key columns)",
            colid,
            table->GetFieldName(colid));
        return true;  // not an error, but we need to stop (this is a filter expression)
    } else if (!include_pkey && (index_colid >= 0)) {  // ordered to include only non-pkey and column is pkey so skip
        MOT_LOG_TRACE("visitSearchOpExpression(): Skipping index key column %d, table column %d %s "
                      "(ordered to include only non-index key columns)",
            index_colid,
            colid,
            table->GetFieldName(colid));
        return true;  // not an error, but we need to stop (this is a primary key expression)
    }
    return false;
}

static bool visitSearchOpExpression(Query* query, MOT::Table* table, MOT::Index* index, OpExpr* op_expr,
    bool include_pkey, ExpressionVisitor* visitor, bool include_join_exprs, JitColumnExprArray* pkey_exprs)
{
    int arg_count = list_length(op_expr->args);
    if (arg_count != 2) {
        MOT_LOG_TRACE("visitSearchOpExpression(): Invalid OpExpr in WHERE clause having %d arguments", arg_count);
        return false;
    }

    if (!IsWhereOperatorSupported(op_expr->opno)) {
        if (pkey_exprs == nullptr) {  // first round, collecting pkey expressions
            MOT_LOG_TRACE("visitSearchOpExpression(): Unsupported operator %u. First validation.", op_expr->opno);
            // this is ok, we wait for second round to collect it as filter
            return true;
        } else {  // second round, try to collect expression as filter
            MOT_LOG_TRACE("visitSearchOpExpression(): Unsupported operator %u. Second validation. Validate filters "
                          "before failing",
                op_expr->opno);
            if (!VisitSearchOpExpressionFilters(
                    query, table, index, op_expr, visitor, include_join_exprs, pkey_exprs)) {
                MOT_LOG_TRACE("visitSearchOpExpression(): Encountered error while classifying table expressions for "
                              "filters while op is not supported");
                return false;
            }
            MOT_LOG_TRACE("visitSearchOpExpression(): Unsupported operator %u. Second validation. Filter validation "
                          "passed successfully",
                op_expr->opno);
            return true;
        }
    }

    if (!VisitSearchOpExpressionFilters(query, table, index, op_expr, visitor, include_join_exprs, pkey_exprs)) {
        MOT_LOG_TRACE("visitSearchOpExpression(): Encountered error while classifying table expressions for filters");
        return false;
    }

    ListCell* lc1 = nullptr;
    int colid = -1;
    int vartype = -1;
    int index_colid = -1;
    Expr* expr = nullptr;
    bool join_expr = false;

    foreach (lc1, op_expr->args) {
        Expr* arg_expr = (Expr*)lfirst(lc1);
        if (arg_expr->type ==
            T_RelabelType) {  // sometimes relabel expression hides the inner expression, so we peel it off
            arg_expr = ((RelabelType*)arg_expr)->arg;
        }
        if (arg_expr->type == T_Var) {
            Var* var = (Var*)arg_expr;
            if (!IsTypeSupported(var->vartype)) {  // error: column type unsupported
                MOT_LOG_TRACE("visitSearchOpExpression(): Unsupported type %d", (int)var->vartype);
                return false;
            }
            // get real column id and also filter by source table (in case of JOIN, this maybe the column id
            // in the virtual JOIN table, so we need to follow it into the real table and get the real column id)
            // beware to not override colid from previous round
            int tmp_colid = getRealColumnId(query, var->varno, var->varattno, table);
            if (tmp_colid == -1) {  // error occurred
                MOT_LOG_TRACE("visitSearchOpExpression(): aborting after error");
                return false;              // query is not jittable, or even internal error occurred
            } else if (tmp_colid == -2) {  // column does not belong to table, this happens in implicit JOIN queries, so
                                           // we regard it as the expression part
                if (include_join_exprs) {
                    MOT_LOG_TRACE("visitSearchOpExpression(): Regarding column of another table as expression in "
                                  "(probably) implicit JOIN query");
                    expr = arg_expr;
                    join_expr = true;  // ATTENTION: when all is done, we still need to verify this refers to the other
                                       // table in the JOIN
                } else {
                    MOT_LOG_TRACE("visitSearchOpExpression(): Skipping column of another table");
                    return true;  // not an error, but we need to stop (this is an expression of another table)
                }
            } else {  // column belongs to table, but...
                if (colid >= 0) {
                    // this is very unexpected, is the user trying to compare two columns of the same table? we do not
                    // allow it at the moment
                    MOT_LOG_TRACE("visitSearchOpExpression(): Rejecting query with comparison between two columns of "
                                  "the same table");
                    return false;
                }
                colid = tmp_colid;
                index_colid = MapTableColumnToIndex(table, index, colid);
                MOT_LOG_TRACE(
                    "visitSearchOpExpression(): Found table column id %d and index column id %d", colid, index_colid);
                if (SkipKeyColumn(table, index, include_pkey, colid, index_colid)) {
                    return true;  // not an error, but we need to stop (this is a filter or primary key expression)
                }
                vartype = var->vartype;
            }
            // no further processing
        } else {
            expr = arg_expr;
        }
    }

    if ((colid >= 0) && (expr != nullptr)) {
        MOT_LOG_TRACE(
            "visitSearchOpExpression(): Collecting expression %p for table column id %d, index column id %d (%s)",
            op_expr,
            colid,
            index_colid,
            table->GetFieldName(colid));
        return visitor->OnExpression(expr, vartype, colid, table, ClassifyWhereOperator(op_expr->opno), join_expr);
    }

    if (join_expr) {  // it is possible to see another table's column but not ours in implicit JOIN statements
        MOT_LOG_TRACE("visitSearchOpExpression(): Skipping expression %p of another table in implicit JOIN", op_expr);
        return true;
    }

    // last option: complex filter referring some table columns, so we need to analyze it is a valid filter expression
    // attention: we treat neutral expressions as filters
    TableExprClass exprClass = classifyTableExpr(query, table, index, (Expr*)op_expr);
    if ((exprClass == TableExprFilter) || (exprClass == TableExprNeutral)) {
        MOT_LOG_TRACE("visitSearchOpExpression(): Enabling complex filter expression %p", op_expr);
        return true;
    }

    MOT_LOG_TRACE("visitSearchOpExpression(): Invalid OpExpr");
    return false;  // query is not jittable
}

static bool visitSearchBoolExpression(Query* query, MOT::Table* table, MOT::Index* index, const BoolExpr* bool_expr,
    bool include_pkey, ExpressionVisitor* visitor, bool include_join_exprs, JitColumnExprArray* pkey_exprs)
{
    bool result = false;
    if (bool_expr->boolop == AND_EXPR) {
        // now traverse args to get param index to build search key
        ListCell* lc = nullptr;

        foreach (lc, bool_expr->args) {
            // each element is Expr
            Expr* expr = (Expr*)lfirst(lc);
            result = visitSearchExpressions(
                query, table, index, expr, include_pkey, visitor, include_join_exprs, pkey_exprs);
            if (!result) {
                MOT_LOG_TRACE("visitSearchBoolExpression(): Failed to process operand");
                break;
            }
        }
    } else {
        MOT_LOG_TRACE("visitSearchBoolExpression(): Unsupported boolean operator %d", (int)bool_expr->boolop);
    }
    return result;
}

bool visitSearchExpressions(Query* query, MOT::Table* table, MOT::Index* index, Expr* expr, bool include_pkey,
    ExpressionVisitor* visitor, bool include_join_exprs, JitColumnExprArray* pkey_exprs)
{
    bool result = false;
    if (expr->type == T_OpExpr) {
        result = visitSearchOpExpression(
            query, table, index, (OpExpr*)expr, include_pkey, visitor, include_join_exprs, pkey_exprs);
    } else if (expr->type == T_BoolExpr) {
        result = visitSearchBoolExpression(
            query, table, index, (BoolExpr*)expr, include_pkey, visitor, include_join_exprs, pkey_exprs);
    } else if (expr->type == T_ScalarArrayOpExpr) {
        result = visitor->OnFilterExpr(expr);
    } else {
        MOT_LOG_TRACE("Unsupported expression type %d while visiting search expressions", (int)expr->type);
    }
    return result;
}

bool getSearchExpressions(Query* query, MOT::Table* table, MOT::Index* index, bool include_pkey,
    JitColumnExprArray* search_exprs, int* count, bool use_join_clause)
{
    MOT_LOG_TRACE("Getting search expressions for table %s, index %s (include-pkey: %s, use-join-clause: %s)",
        table->GetTableName().c_str(),
        index->GetName().c_str(),
        include_pkey ? "yes" : "no",
        use_join_clause ? "yes" : "no");
    ExpressionCollector expr_collector(query, search_exprs, count);
    Node* quals = query->jointree->quals;
    bool result = true;
    if (quals == nullptr) {
        *count = 0;
    } else {
        result = visitSearchExpressions(
            query, table, index, (Expr*)&quals[0], include_pkey, &expr_collector, use_join_clause);
    }
    if (!result) {
        MOT_LOG_TRACE("Failed to get search expressions");
    } else {
        search_exprs->_count = *count;  // update actual number of expression used in search
        MOT_LOG_TRACE("Found %d expressions", *count);
    }
    return result;
}

Node* getJoinQualifiers(const Query* query)
{
    Node* quals = nullptr;

    Expr* from_expr = (Expr*)linitial(query->jointree->fromlist);
    if (from_expr->type == T_JoinExpr) {
        JoinExpr* join_expr = (JoinExpr*)from_expr;
        quals = join_expr->quals;
    }

    return quals;
}

static const char* joinClauseTypeToString(JoinClauseType join_clause_type)
{
    switch (join_clause_type) {
        case JoinClauseNone:
            return "none";
        case JoinClauseExplicit:
            return "explicit";
        case JoinClauseImplicit:
            return "implicit";
        default:
            return "N/A";
    }
}

bool getRangeSearchExpressions(
    Query* query, MOT::Table* table, MOT::Index* index, JitIndexScan* index_scan, JoinClauseType join_clause_type)
{
    MOT_LOG_TRACE("Getting range search expressions for table %s, index %s (join_clause_type: %s)",
        table->GetTableName().c_str(),
        index->GetName().c_str(),
        joinClauseTypeToString(join_clause_type));
    bool result = false;
    RangeScanExpressionCollector expr_collector(query, table, index, index_scan);
    if (!expr_collector.Init()) {
        MOT_LOG_TRACE("Failed to initialize range search expression collector");
    } else {
        Node* quals = query->jointree->quals;
        if (quals == nullptr) {
#ifdef MOT_JIT_FULL_SCAN
            MOT_LOG_TRACE("No range search expressions collected (empty WHERE clause) - using a full index scan");
            index_scan->_scan_type = JIT_INDEX_SCAN_FULL;
            return true;
#else
            MOT_LOG_TRACE("Query is not jittable: requires full scan");
            return false;
#endif
        }
        if (!visitSearchExpressions(
                query, table, index, (Expr*)&quals[0], true, &expr_collector, join_clause_type == JoinClauseImplicit)) {
            MOT_LOG_TRACE("Failed to collect range search expressions");
        } else {
            if (join_clause_type == JoinClauseExplicit) {
                quals = getJoinQualifiers(query);
                if (quals == nullptr) {
                    MOT_LOG_TRACE("Query is not jittable: JOIN clause has unexpectedly no qualifiers");
                    return result;
                } else {
                    MOT_LOG_TRACE("Adding JOIN clause qualifiers for WHERE clause classification");
                    if (!visitSearchExpressions(query, table, index, (Expr*)&quals[0], true, &expr_collector, true)) {
                        MOT_LOG_TRACE("Failed to collect range search expressions from JOIN qualifiers");
                        return result;
                    }
                }
            }
            expr_collector.EvaluateScanType();
            result = true;
        }
    }
    return result;
}

bool getTargetExpressions(Query* query, JitColumnExprArray* target_exprs)
{
    int i = 0;
    ListCell* lc = nullptr;

    MOT::TxnManager* currTxn = GetSafeTxn(__FUNCTION__);
    MOT_ASSERT(currTxn != nullptr);
    foreach (lc, query->targetList) {
        TargetEntry* target_entry = (TargetEntry*)lfirst(lc);
        if (target_entry->resjunk) {
            MOT_LOG_TRACE("getTargetExpressions(): Skipping resjunk target entry");
            continue;
        }
        if (i < target_exprs->_count) {
            target_exprs->_exprs[i]._expr = parseExpr(query, target_entry->expr, 0);
            if (target_exprs->_exprs[i]._expr == nullptr) {
                MOT_LOG_TRACE("getTargetExpressions(): Failed to parse target expression %d", i);
                return false;
            }
            target_exprs->_exprs[i]._table_column_id = target_entry->resno;  // update/insert
            if (target_entry->resorigtbl != 0) {                             // happens usually in INSERT
                target_exprs->_exprs[i]._table = currTxn->GetTableByExternalId(target_entry->resorigtbl);
                if (target_exprs->_exprs[i]._table == nullptr) {
                    MOT_LOG_TRACE("getTargetExpressions(): Failed to retrieve real table by id %d",
                        (int)target_entry->resorigtbl);
                    return false;
                }
            } else {
                // this is usually an update, and we retrieve the first table in rtable list
                RangeTblEntry* rte = (RangeTblEntry*)linitial(query->rtable);
                target_exprs->_exprs[i]._table = currTxn->GetTableByExternalId(rte->relid);
                if (target_exprs->_exprs[i]._table == nullptr) {
                    MOT_LOG_TRACE(
                        "getTargetExpressions(): Failed to retrieve real table by inferred id %d", (int)rte->relid);
                    return false;
                }
            }
            target_exprs->_exprs[i]._column_type = target_exprs->_exprs[i]._expr->_result_type;
            target_exprs->_exprs[i]._join_expr = false;
            if (query->commandType == CMD_UPDATE) {
                if (MOTAdaptor::IsColumnIndexed(target_entry->resno, target_exprs->_exprs[i]._table)) {
                    MOT_LOG_TRACE("getTargetExpressions(): Failed, column %d is used by index", target_entry->resno);
                    return false;
                }
            }
            ++i;
        } else {
            // this is unexpected and indicates internal error (we should have had enough items in the target expression
            // array)
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                "Prepare JIT Plan",
                "Exceeded number of target expressions %d",
                target_exprs->_count);
            return false;
        }
    }
    return true;
}

bool getSelectExpressions(Query* query, JitSelectExprArray* select_exprs)
{
    int i = 0;
    ListCell* lc = nullptr;

    foreach (lc, query->targetList) {
        TargetEntry* target_entry = (TargetEntry*)lfirst(lc);
        if (target_entry->resjunk) {
            MOT_LOG_TRACE("getSelectExpressions(): Skipping resjunk target entry");
            continue;
        }
        if (i < select_exprs->_count) {
            JitExpr* sub_expr = parseExpr(query, target_entry->expr, 0);
            if (sub_expr == nullptr) {
                MOT_LOG_TRACE("getSelectExpressions(): Failed to parse select expression %d", i);
                return false;
            }
            select_exprs->_exprs[i]._expr = sub_expr;
            select_exprs->_exprs[i]._tuple_column_id = target_entry->resno - 1;
            ++i;
        } else {
            // this is unexpected and indicates internal error (we should have had enough items in the target expression
            // array)
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                "Prepare JIT Plan",
                "Exceeded number of select expressions %d",
                select_exprs->_count);
            return false;
        }
    }
    return true;
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

bool getLimitCount(Query* query, int* limit_count)
{
    bool result = false;
    if (query->limitOffset) {
        MOT_LOG_TRACE("getLimitCount(): invalid limit clause - encountered limitOffset clause");
    } else if (query->limitCount) {
        *limit_count = evalConstExpr((Expr*)query->limitCount);
        if (*limit_count == 0) {
            MOT_LOG_TRACE("getLimitCount(): limit clause has limit zero - disqualifying");
        } else {
            result = true;
        }
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
    if ((funcid == INT8AVGFUNCOID) || (funcid == INT4AVGFUNCOID) || (funcid == INT2AVGFUNCOID) ||
        (funcid == INT1AVGFUNCOID) || (funcid == FLOAT4AVGFUNCOID) || (funcid == FLOAT8AVGFUNCOID) ||
        (funcid == NUMERICAVGFUNCOID)) {
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
        MOT_LOG_TRACE("Unsupported DISTINCIT specifier with more than one sort clause");
    } else {
        SortGroupClause* sgc = (SortGroupClause*)linitial(agg_distinct);
        if (sgc->groupSet) {
            MOT_LOG_TRACE("Unsupported DISTINCIT specifier with group-set flag");
        } else {
            result = true;
        }
    }

    return result;
}

static bool getTargetEntryAggCountStar(Aggref* agg_ref, JitAggregate* aggregate)
{
    JitAggregateOperator aggOp = classifyAggregateOperator(agg_ref->aggfnoid);
    bool isCountStar = (agg_ref->aggstar && (aggOp == JIT_AGGREGATE_COUNT) && (agg_ref->aggtype == INT8OID));
    if (isCountStar) {
        aggregate->_aggreaget_op = aggOp;
        aggregate->_element_type = agg_ref->aggtype;
        aggregate->_avg_element_type = -1;
        aggregate->_func_id = agg_ref->aggfnoid;
        aggregate->m_funcCollationId = agg_ref->inputcollid;
        aggregate->_table = nullptr;
        aggregate->_table_column_id = -1;
        aggregate->_distinct = (agg_ref->aggdistinct != nullptr) ? true : false;
        return true;
    } else {
        MOT_LOG_TRACE("getTargetEntryAggregateOperator(): Unsupported empty aggregate argument list");
        return false;
    }
}

static bool getTargetEntryAggCountConst(TargetEntry* sub_te, Aggref* agg_ref, JitAggregate* aggregate)
{
    Const* constValue = (Const*)sub_te->expr;
    JitAggregateOperator aggOp = classifyAggregateOperator(agg_ref->aggfnoid);
    bool isCountConst = ((aggOp == JIT_AGGREGATE_COUNT) && (agg_ref->aggtype == INT8OID) &&
                       (constValue->consttype == INT4OID) && (DatumGetInt32(constValue->constvalue) != 0));
    if (isCountConst) {
        aggregate->_aggreaget_op = aggOp;
        aggregate->_element_type = agg_ref->aggtype;
        aggregate->_avg_element_type = -1;
        aggregate->_func_id = agg_ref->aggfnoid;
        aggregate->_table = nullptr;
        aggregate->_table_column_id = -1;
        aggregate->_distinct = (agg_ref->aggdistinct != nullptr) ? true : false;
        return true;
    } else {
        MOT_LOG_TRACE(
            "getTargetEntryAggregateOperator(): Unsupported aggregate argument list with length unequal to 1");
        return false;
    }
}

bool getTargetEntryAggregateOperator(Query* query, TargetEntry* target_entry, JitAggregate* aggregate)
{
    bool result = false;

    Aggref* agg_ref = (Aggref*)target_entry->expr;
    int aggArgCount = list_length(agg_ref->args);
    if (agg_ref->aggorder) {
        MOT_LOG_TRACE("getTargetEntryAggregateOperator(): Unsupported aggregate operator with ORDER BY specifiers");
    } else if (!isValidAggregateFunction(agg_ref->aggfnoid)) {
        MOT_LOG_TRACE("getTargetEntryAggregateOperator(): Unsupported aggregate operator %d", agg_ref->aggfnoid);
    } else if (!isValidAggregateResultType(agg_ref->aggtype)) {
        MOT_LOG_TRACE("getTargetEntryAggregateOperator(): Unsupported aggregate result type %d", agg_ref->aggtype);
    } else if (!isValidAggregateDistinctClause(agg_ref->aggdistinct)) {
        MOT_LOG_TRACE("getTargetEntryAggregateOperator(): Unsupported aggregate distinct clause");
    } else if (aggArgCount == 0) {
        // special case: count(*)
        result = getTargetEntryAggCountStar(agg_ref, aggregate);
    } else if (aggArgCount == 1) {
        TargetEntry* sub_te = (TargetEntry*)linitial(agg_ref->args);
        if (sub_te->expr->type == T_Const) {
            // special case: count(1)
            result = getTargetEntryAggCountConst(sub_te, agg_ref, aggregate);
        } else if (sub_te->expr->type != T_Var) {
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
                aggregate->_element_type = result_type;
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
    } else {
        MOT_LOG_TRACE("getTargetEntryAggregateOperator(): Unsupported aggregate list length %d", aggArgCount);
    }

    return result;
}
}  // namespace JitExec
