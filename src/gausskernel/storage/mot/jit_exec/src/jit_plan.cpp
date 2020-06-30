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
 *    src/gausskernel/storage/mot/jit_exec/src/jit_plan.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <algorithm>

#include "global.h"
#include "jit_plan.h"
#include "jit_common.h"
#include "mm_session_api.h"
#include "utilities.h"
#include "nodes/pg_list.h"
#include "catalog/pg_aggregate.h"

namespace JitExec {
DECLARE_LOGGER(JitPlan, JitExec)

enum JoinClauseType { JoinClauseNone, JoinClauseExplicit, JoinClauseImplicit };

// Forward declarations
class ExpressionVisitor;
static bool visitSearchExpressions(Query* query, MOT::Table* table, MOT::Index* index, Expr* expr, bool include_pkey,
    ExpressionVisitor* visitor, bool include_join_exprs, JitColumnExprArray* pkey_exprs = nullptr);
static JitExpr* parseExpr(Query* query, Expr* expr, int arg_pos, int depth);
static void freeExpr(JitExpr* expr);
static MOT::Table* getRealTable(const Query* query, int table_ref_id, int column_id);
static int getRealColumnId(const Query* query, int table_ref_id, int column_id, const MOT::Table* table);

// Parent class for all expression visitors
class ExpressionVisitor {
public:
    ExpressionVisitor()
    {}

    virtual ~ExpressionVisitor()
    {}

    virtual bool onFilterExpr(int filter_op, int filter_op_funcid, Expr* lhs, Expr* rhs)
    {
        return true;
    }

    virtual bool onExpression(Expr* expr, int column_type, int table_column_id, MOT::Table* table,
        JitWhereOperatorClass op_class, bool join_expr)
    {
        return true;
    }
};

// Expression visitor that counts number of expressions
class ExpressionCounter : public ExpressionVisitor {
public:
    explicit ExpressionCounter(int* count) : _count(count)
    {}

    ~ExpressionCounter() final
    {
        _count = nullptr;
    }

    virtual bool onExpression(Expr* expr, int column_type, int table_column_id, MOT::Table* table,
        JitWhereOperatorClass op_class, bool join_expr)
    {
        if (op_class != JIT_WOC_EQUALS) {
            MOT_LOG_TRACE("ExpressionCounter::onExpression(): Skipping non-equals operator");
            return true;  // this is not an error condition
        }
        ++(*_count);
        return true;
    }

private:
    int* _count;
};

// Expression visitor that collects expressions with equals operator
class ExpressionCollector : public ExpressionVisitor {
public:
    ExpressionCollector(Query* query, JitColumnExprArray* expr_array, int* count)
        : _query(query), _expr_array(expr_array), _expr_count(count)
    {}

    ~ExpressionCollector() final
    {
        _query = nullptr;
        _expr_array = nullptr;
        _expr_count = nullptr;
    }

    virtual bool onExpression(Expr* expr, int column_type, int table_column_id, MOT::Table* table,
        JitWhereOperatorClass op_class, bool join_expr)
    {
        if (op_class != JIT_WOC_EQUALS) {
            MOT_LOG_TRACE("ExpressionCollector::onExpression(): Skipping non-equals operator");
            return true;  // this is not an error condition
        } else if (*_expr_count < _expr_array->_count) {
            JitExpr* jit_expr = parseExpr(_query, expr, 0, 0);
            if (jit_expr == nullptr) {
                MOT_LOG_TRACE("ExpressionCollector::onExpression(): Failed to parse expression %d", *_expr_count);
                cleanup();
                return false;
            }
            _expr_array->_exprs[*_expr_count]._table_column_id = table_column_id;
            _expr_array->_exprs[*_expr_count]._table = table;
            _expr_array->_exprs[*_expr_count]._expr = jit_expr;
            _expr_array->_exprs[*_expr_count]._column_type = column_type;
            _expr_array->_exprs[*_expr_count]._join_expr = join_expr;
            ++(*_expr_count);
            return true;
        } else {
            MOT_REPORT_ERROR(
                MOT_ERROR_INTERNAL, "Prepare JIT Plan", "Exceeded expression count %d", _expr_array->_count);
            return false;
        }
    }

private:
    Query* _query;
    JitColumnExprArray* _expr_array;
    int* _expr_count;

    void cleanup()
    {
        for (int i = 0; i < *_expr_count; ++i) {
            freeExpr(_expr_array->_exprs[*_expr_count]._expr);
        }
    }
};

// Expression visitor that collects expressions for possibly open range scans
// This requires special care: sort operators by index column id and evaluate the scan type
class RangeScanExpressionCollector : public ExpressionVisitor {
private:
    Query* _query;
    MOT::Table* _table;
    MOT::Index* _index;

    struct IndexOpClass {
        int _index_column_id;
        JitWhereOperatorClass _op_class;
    };
    IndexOpClass* _index_ops;
    int _max_index_ops;
    int _index_op_count;
    JitIndexScan* _index_scan;

public:
    RangeScanExpressionCollector(Query* query, MOT::Table* table, MOT::Index* index, JitIndexScan* index_scan)
        : _query(query),
          _table(table),
          _index(index),
          _index_ops(nullptr),
          _max_index_ops(0),
          _index_op_count(0),
          _index_scan(index_scan)
    {}

    ~RangeScanExpressionCollector() noexcept final
    {
        if (_index_ops != nullptr) {
            // NOTE: this is a good use case for txn-level allocation, right? (the transaction being PREPARE command)
            MOT::MemSessionFree(_index_ops);
        }
        _query = nullptr;
        _table = nullptr;
        _index = nullptr;
        _index_scan = nullptr;
    }

    inline bool init()
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

    virtual bool onExpression(Expr* expr, int column_type, int table_column_id, MOT::Table* table,
        JitWhereOperatorClass op_class, bool join_expr)
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
            JitExpr* jit_expr = parseExpr(_query, expr, 0, 0);
            if (jit_expr == nullptr) {
                MOT_LOG_TRACE(
                    "RangeScanExpressionCollector::onExpression(): Failed to parse expression %d", _index_op_count);
                cleanup();
                return false;
            }
            _index_scan->_search_exprs._exprs[_index_op_count]._table_column_id = table_column_id;
            _index_scan->_search_exprs._exprs[_index_op_count]._table = table;
            _index_scan->_search_exprs._exprs[_index_op_count]._expr = jit_expr;
            _index_scan->_search_exprs._exprs[_index_op_count]._column_type = column_type;
            _index_scan->_search_exprs._exprs[_index_op_count]._join_expr = join_expr;
            _index_scan->_search_exprs._count++;
            _index_ops[_index_op_count]._index_column_id = MapTableColumnToIndex(_table, _index, table_column_id);
            _index_ops[_index_op_count]._op_class = op_class;
            ++_index_op_count;
            return true;
        }
    }

    void evaluateScanType()
    {
        _index_scan->_scan_type = JIT_INDEX_SCAN_TYPE_INVALID;
        JitIndexScanType scan_type = JIT_INDEX_SCAN_TYPE_INVALID;

        // if two expressions refer to the same column, we regard one of them as filter
        // if an expression is removed from index scan, it will automatically be collected as filter
        // (see pkey_exprs argument in @ref visitSearchOpExpression)
        if (!removeDuplicates()) {
            MOT_LOG_TRACE("RangeScanExpressionCollector(): Disqualifying query - failed to remove duplicates");
            return;
        }

        // if no expression was collected, this is an invalid scan (we do not support full scans yet)
        int column_count = _index_op_count;
        if (_index_op_count == 0) {
            MOT_LOG_TRACE("RangeScanExpressionCollector(): Disqualifying query - no expression was collected "
                          "(unsupported for index scan)");
            return;
        }

        // first step: sort in-place all collected operators
        if (_index_op_count > 1) {
            std::sort(&_index_ops[0], &_index_ops[_index_op_count - 1], indexOpCmp);
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
        if (_index_op_count >= 2) {
            if (_index_ops[_index_op_count - 2]._op_class == JIT_WOC_EQUALS) {
                if (_index_ops[_index_op_count - 1]._op_class == JIT_WOC_EQUALS) {
                    if (_index->GetUnique() && (_index_op_count == _index->GetNumFields())) {
                        scan_type = JIT_INDEX_SCAN_POINT;
                    } else {
                        scan_type = JIT_INDEX_SCAN_CLOSED;
                    }
                } else {
                    scan_type = JIT_INDEX_SCAN_SEMI_OPEN;
                    _index_scan->_last_dim_op1 = _index_ops[_index_op_count - 1]._op_class;
                }
            } else if (_index_ops[_index_op_count - 1]._op_class == JIT_WOC_EQUALS) {
                MOT_LOG_TRACE("RangeScanExpressionCollector(): Disqualifying query - invalid open scan specifying last "
                              "operator as equals, while previous one is not");
                return;
            } else {
                scan_type = JIT_INDEX_SCAN_OPEN;
                column_count = _index_op_count - 1;
                _index_scan->_last_dim_op1 = _index_ops[_index_op_count - 2]._op_class;
                _index_scan->_last_dim_op2 = _index_ops[_index_op_count - 1]._op_class;
            }
        } else if (_index_op_count == 1) {
            if (_index_ops[0]._op_class == JIT_WOC_EQUALS) {
                if (_index->GetUnique() && (_index_op_count == _index->GetNumFields())) {
                    scan_type = JIT_INDEX_SCAN_POINT;
                } else {
                    scan_type = JIT_INDEX_SCAN_CLOSED;
                }
            } else {
                scan_type = JIT_INDEX_SCAN_SEMI_OPEN;
                _index_scan->_last_dim_op1 = _index_ops[0]._op_class;
            }
        }

        // final step: verify we have no holes in the columns according to the expected scan type
        if (!scanHasHoles(scan_type)) {
            _index_scan->_scan_type = scan_type;
            _index_scan->_column_count = column_count;
            _index_scan->_search_exprs._count = _index_op_count;  // update real number of participating expressions
        }
    }

private:
    void cleanup()
    {
        for (int i = 0; i < _index_op_count; ++i) {
            freeExpr(_index_scan->_search_exprs._exprs[i]._expr);
        }
        _index_scan->_search_exprs._count = 0;
        _index_op_count = 0;
    }

    bool removeDuplicates()
    {
        int result = removeSingleDuplicate();
        while (result > 0) {
            result = removeSingleDuplicate();
        }
        return (result == 0);
    }

    int removeSingleDuplicate()
    {
        // scan and stop after first removal
        for (int i = 1; i < _index_op_count; ++i) {
            for (int j = 0; j < i; ++j) {
                if (_index_ops[i]._index_column_id == _index_ops[j]._index_column_id) {
                    MOT_LOG_TRACE("RangeScanExpressionCollector(): Found duplicate column ref at %d and %d", i, j);
                    if ((_index_ops[i]._op_class != JIT_WOC_EQUALS) && (_index_ops[j]._op_class != JIT_WOC_EQUALS)) {
                        MOT_LOG_DEBUG("RangeScanExpressionCollector(): Skipping probable open scan operators while "
                                      "removing duplicates");
                        continue;
                    }
                    // now we need to decide which one to remove,
                    // our consideration is to keep equals operators and then join expressions
                    int victim = -1;
                    if ((_index_ops[i]._op_class == JIT_WOC_EQUALS) || (_index_ops[j]._op_class == JIT_WOC_EQUALS)) {
                        // we keep the equals operator for index scan, and leave the other as a filter
                        MOT_LOG_TRACE("RangeScanExpressionCollector(): Rejecting query due to duplicate index column "
                                      "reference, one with EQUALS, one without");
                        if (_index_ops[i]._op_class != JIT_WOC_EQUALS) {
                            victim = i;
                        } else {
                            victim = j;
                        }
                    } else if (_index_scan->_search_exprs._exprs[i]._join_expr &&
                               !_index_scan->_search_exprs._exprs[j]._join_expr) {
                        victim = j;
                    } else if (!_index_scan->_search_exprs._exprs[i]._join_expr &&
                               _index_scan->_search_exprs._exprs[j]._join_expr) {
                        victim = i;
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
                    }
                    // switch victim with last item
                    if (_index_scan->_search_exprs._exprs[victim]._expr != nullptr) {
                        freeExpr(_index_scan->_search_exprs._exprs[victim]._expr);
                    }
                    _index_scan->_search_exprs._exprs[victim] = _index_scan->_search_exprs._exprs[_index_op_count - 1];
                    --_index_op_count;
                    --_index_scan->_search_exprs._count;
                    return 1;
                }
            }
        }

        // nothing changed
        return 0;
    }

    static int intCmp(int lhs, int rhs)
    {
        int result = 0;
        if (lhs < rhs) {
            result = -1;
        } else if (lhs > rhs) {
            result = 1;
        }
        return result;
    }

    static bool indexOpCmp(const IndexOpClass& lhs, const IndexOpClass& rhs)
    {
        int result = intCmp(lhs._index_column_id, rhs._index_column_id);
        if (result == 0) {
            // make sure equals appears before other operators in case column id is equal
            result = intCmp(lhs._op_class, rhs._op_class);
        }
        return result < 0;
    }

    bool scanHasHoles(JitIndexScanType scan_type) const
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
};

// Expression visitor that counts number of filters
class FilterCounter : public ExpressionVisitor {
public:
    explicit FilterCounter(int* count) : _count(count)
    {}

    ~FilterCounter() final
    {
        _count = nullptr;
    }

    virtual bool onFilterExpr(int filter_op, int filter_op_funcid, Expr* lhs, Expr* rhs)
    {
        ++(*_count);
        return true;
    }

private:
    int* _count;
};

// Expression visitor that collects filters
class FilterCollector : public ExpressionVisitor {
private:
    Query* _query;
    JitFilterArray* _filter_array;
    int* _filter_count;

public:
    FilterCollector(Query* query, JitFilterArray* filter_array, int* count)
        : _query(query), _filter_array(filter_array), _filter_count(count)
    {}

    ~FilterCollector() final
    {
        _query = nullptr;
        _filter_array = nullptr;
        _filter_count = nullptr;
    }

    virtual bool onFilterExpr(int filter_op, int filter_op_funcid, Expr* lhs, Expr* rhs)
    {
        JitExpr* jit_lhs = parseExpr(_query, lhs, 0, 0);
        if (jit_lhs == nullptr) {
            MOT_LOG_TRACE("FilterCollector::onFilterExpr(): Failed to parse LHS expression in filter expression %d",
                *_filter_count);
            cleanup();
            return false;
        }
        JitExpr* jit_rhs = parseExpr(_query, rhs, 1, 0);
        if (jit_rhs == nullptr) {
            MOT_LOG_TRACE("FilterCollector::onFilterExpr(): Failed to parse RHS expression in filter expression %d",
                *_filter_count);
            freeExpr(jit_lhs);
            cleanup();
            return false;
        }
        if (*_filter_count < _filter_array->_filter_count) {
            _filter_array->_scan_filters[*_filter_count]._filter_op = filter_op;
            _filter_array->_scan_filters[*_filter_count]._filter_op_funcid = filter_op_funcid;
            _filter_array->_scan_filters[*_filter_count]._lhs_operand = jit_lhs;
            _filter_array->_scan_filters[*_filter_count]._rhs_operand = jit_rhs;
            ++(*_filter_count);
            return true;
        } else {
            MOT_REPORT_ERROR(
                MOT_ERROR_INTERNAL, "Prepare JIT Plan", "Exceeded filter count %d", _filter_array->_filter_count);
            return false;
        }
    }

private:
    void cleanup()
    {
        for (int i = 0; i < *_filter_count; ++i) {
            freeExpr(_filter_array->_scan_filters[*_filter_count]._lhs_operand);
            freeExpr(_filter_array->_scan_filters[*_filter_count]._rhs_operand);
        }
    }
};

static JitExpr* parseConstExpr(const Const* const_expr, int arg_pos)
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
        result->_value = const_expr->constvalue;
        result->_is_null = const_expr->constisnull;
        result->_arg_pos = arg_pos;
    }
    return (JitExpr*)result;
}

static JitExpr* parseParamExpr(const Param* param_expr, int arg_pos)
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
        result->_param_id = param_expr->paramid - 1;  // move to zero-based index
        result->_arg_pos = arg_pos;
    }
    return (JitExpr*)result;
}

static JitExpr* parseVarExpr(Query* query, const Var* var_expr, int arg_pos)
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
        result->_table = table;
        result->_column_id = column_id;
        result->_arg_pos = arg_pos;
    }

    return (JitExpr*)result;
}

static JitExpr* parseRelabelExpr(Query* query, RelabelType* relabel_type, int arg_pos)
{
    JitExpr* result = nullptr;
    if (relabel_type->arg->type == T_Param) {
        result = parseParamExpr((Param*)relabel_type->arg, arg_pos);
        if (result != nullptr) {
            // replace result type with relabeled type
            ((JitParamExpr*)result)->_param_type = relabel_type->resulttype;
        }
    } else if (relabel_type->arg->type == T_Var) {
        result = parseVarExpr(query, (Var*)relabel_type->arg, arg_pos);
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

static JitExpr* parseOpExpr(Query* query, const OpExpr* op_expr, int arg_pos, int depth)
{
    if (!IsTypeSupported(op_expr->opresulttype)) {
        MOT_LOG_TRACE("Disqualifying operator expression: result type %d is unsupported", (int)op_expr->opresulttype);
        return nullptr;
    }

    if (list_length(op_expr->args) > 3) {
        MOT_LOG_TRACE("Unsupported operator %d: too many arguments", op_expr->opno);
        return nullptr;
    }

    JitExpr* args[3] = {nullptr, nullptr, nullptr};
    int arg_num = 0;

    ListCell* lc = nullptr;
    foreach (lc, op_expr->args) {
        Expr* sub_expr = (Expr*)lfirst(lc);
        args[arg_num] = parseExpr(query, sub_expr, arg_pos + arg_num, depth + 1);
        if (args[arg_num] == nullptr) {
            MOT_LOG_TRACE("Failed to process operator sub-expression %d", arg_num);
            for (int i = 0; i < arg_num; ++i) {
                freeExpr(args[i]);
            }
            return nullptr;
        }
        if (++arg_num == 3) {
            break;
        }
    }

    size_t alloc_size = sizeof(JitOpExpr);
    JitOpExpr* result = (JitOpExpr*)MOT::MemSessionAlloc(alloc_size);
    if (result == nullptr) {
        MOT_REPORT_ERROR(
            MOT_ERROR_OOM, "Prepare JIT Plan", "Failed to allocate %u bytes for operator expression", alloc_size);
        for (int i = 0; i < arg_num; ++i) {
            freeExpr(args[i]);
        }
    } else {
        result->_expr_type = JIT_EXPR_TYPE_OP;
        result->_op_no = op_expr->opno;
        result->_op_func_id = op_expr->opfuncid;
        result->_result_type = op_expr->opresulttype;
        result->_arg_count = arg_num;
        for (int i = 0; i < arg_num; ++i) {
            result->_args[i] = args[i];
        }
        result->_arg_pos = arg_pos;
    }

    return (JitExpr*)result;
}

static JitExpr* parseFuncExpr(Query* query, const FuncExpr* func_expr, int arg_pos, int depth)
{
    if (!IsTypeSupported(func_expr->funcresulttype)) {
        MOT_LOG_TRACE(
            "Disqualifying function expression: result type %d is unsupported", (int)func_expr->funcresulttype);
        return nullptr;
    }

    if (list_length(func_expr->args) > 3) {
        MOT_LOG_TRACE("Unsupported function %d: too many arguments", func_expr->funcid);
        return nullptr;
    }

    JitExpr* args[3] = {nullptr, nullptr, nullptr};
    int arg_num = 0;

    ListCell* lc = nullptr;

    foreach (lc, func_expr->args) {
        Expr* sub_expr = (Expr*)lfirst(lc);
        args[arg_num] = parseExpr(query, sub_expr, arg_pos + arg_num, depth + 1);
        if (args[arg_num] == nullptr) {
            MOT_LOG_TRACE("Failed to process function sub-expression %d", arg_num);
            for (int i = 0; i < arg_num; ++i) {
                freeExpr(args[i]);
            }
            return nullptr;
        }
        if (++arg_num == 3) {
            break;
        }
    }

    size_t alloc_size = sizeof(JitFuncExpr);
    JitFuncExpr* result = (JitFuncExpr*)MOT::MemSessionAlloc(alloc_size);
    if (result == nullptr) {
        MOT_REPORT_ERROR(
            MOT_ERROR_OOM, "Prepare JIT Plan", "Failed to allocate %u bytes for operator expression", alloc_size);
        for (int i = 0; i < arg_num; ++i) {
            freeExpr(args[i]);
        }
    } else {
        result->_expr_type = JIT_EXPR_TYPE_FUNC;
        result->_func_id = func_expr->funcid;
        result->_result_type = func_expr->funcresulttype;
        result->_arg_count = arg_num;
        for (int i = 0; i < arg_num; ++i) {
            result->_args[i] = args[i];
        }
        result->_arg_pos = arg_pos;
    }

    return (JitExpr*)result;
}

static JitExpr* parseExpr(Query* query, Expr* expr, int arg_pos, int depth)
{
    JitExpr* result = nullptr;

    if (depth > MOT_JIT_MAX_EXPR_DEPTH) {
        MOT_LOG_TRACE("Cannot parse expression: Expression exceeds depth limit %d", (int)MOT_JIT_MAX_EXPR_DEPTH);
        return nullptr;
    }

    if (expr->type == T_Const) {
        result = parseConstExpr((Const*)expr, arg_pos);
    } else if (expr->type == T_Param) {
        result = parseParamExpr((Param*)expr, arg_pos);
    } else if (expr->type == T_Var) {
        result = parseVarExpr(query, (Var*)expr, arg_pos);
    } else if (expr->type == T_RelabelType) {
        result = parseRelabelExpr(query, (RelabelType*)expr, arg_pos);
    } else if (expr->type == T_OpExpr) {
        result = parseOpExpr(query, (OpExpr*)expr, arg_pos, depth);
    } else if (expr->type == T_FuncExpr) {
        result = parseFuncExpr(query, (FuncExpr*)expr, arg_pos, depth);
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
}

static void freeFuncExpr(JitFuncExpr* func_expr)
{
    for (int i = 0; i < func_expr->_arg_count; ++i) {
        freeExpr(func_expr->_args[i]);
    }
}

static void freeExpr(JitExpr* expr)
{
    switch (expr->_expr_type) {
        case JIT_EXPR_TYPE_OP:
            freeOpExpr((JitOpExpr*)expr);
            break;

        case JIT_EXPR_TYPE_FUNC:
            freeFuncExpr((JitFuncExpr*)expr);
            break;

        default:
            break;
    }

    MOT::MemSessionFree(expr);
}

static MOT::Table* getRealTable(const Query* query, int table_ref_id, int column_id)
{
    MOT::Table* table = nullptr;
    if (table_ref_id > list_length(query->rtable)) {  // varno index is 1-based
        MOT_LOG_TRACE("getRealTable(): Invalid table reference id %d", table_ref_id);
    } else {
        RangeTblEntry* rte = (RangeTblEntry*)list_nth(query->rtable, table_ref_id - 1);
        if (rte->rtekind == RTE_RELATION) {
            table = MOT::GetTableManager()->GetTableByExternal(rte->relid);
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
                table = MOT::GetTableManager()->GetTableByExternal(rte->relid);
                if (table == nullptr) {
                    MOT_LOG_TRACE("getRealTable(): Could not find table by indirected external id %d", rte->relid);
                }
            }
        }
    }

    MOT_LOG_TRACE("getRealTable(): table_ref_id=%d, column_id=%d --> table=%p", table_ref_id, column_id, table);
    return table;
}

static int getRealColumnId(const Query* query, int table_ref_id, int column_id, const MOT::Table* table)
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

        default:
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                "Prepare JIT Plan",
                "Unexpected expression type %d while trying to determine if an expression refers only to a specific "
                "table");
            return TableExprError;
    }
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
        MOT_LOG_TRACE("visitSearchOpExpression(): Unsupported operator %d", op_expr->opno);
        return false;
    }

    int nargs = (int)list_length(op_expr->args);
    if (nargs != 2) {
        MOT_LOG_TRACE(
            "visitSearchOpExpression(): Unexpected number of arguments %d in operator %d", nargs, op_expr->opno);
        return false;
    }

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
                    "visitSearchOpExpression(): Encountered error while classifying table expressions for filters");
                return false;
            } else if (tec == TableExprNeutral) {
                MOT_LOG_TRACE("visitSearchOpExpression(): Skipping neutral table expressions for filters");
            } else if (tec == TableExprInvalid) {
                MOT_LOG_TRACE("visitSearchOpExpression(): Skipping another table expressions for filters");
            } else if (tec == TableExprPKey) {
                MOT_LOG_TRACE("visitSearchOpExpression(): Skipping primary key table expressions for filters");
            } else {
                if (op_expr->opresulttype != BOOLOID) {
                    MOT_LOG_TRACE(
                        "visitSearchOpExpression(): Disqualifying query - filter result type %d is unsupported",
                        op_expr->opresulttype);
                } else {
                    MOT_LOG_TRACE("visitSearchOpExpression(): Collecting filter expression %p", op_expr);
                    if (!visitor->onFilterExpr(op_expr->opno, op_expr->opfuncid, lhs, rhs)) {
                        MOT_LOG_TRACE("visitSearchOpExpression(): Expression collection failed");
                        return false;
                    }
                }
            }
        }
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
                    // allow it at them moment
                    MOT_LOG_TRACE("visitSearchOpExpression(): Rejecting query with comparison between two columns of "
                                  "the same table");
                    return false;
                }
                colid = tmp_colid;
                index_colid = MapTableColumnToIndex(table, index, colid);
                MOT_LOG_TRACE(
                    "visitSearchOpExpression(): Found table column id %d and index column id %d", colid, index_colid);
                if (include_pkey && (index_colid < 0)) {  // ordered to include only pkey and column is non-pkey so skip
                    MOT_LOG_TRACE("visitSearchOpExpression(): Skipping non-index key column %d %s (ordered to include "
                                  "only index key columns)",
                        colid,
                        table->GetFieldName(colid));
                    return true;  // not an error, but we need to stop (this is a filter expression)
                } else if (!include_pkey &&
                           (index_colid >= 0)) {  // ordered to include only non-pkey and column is pkey so skip
                    MOT_LOG_TRACE("visitSearchOpExpression(): Skipping index key column %d, table column %d %s "
                                  "(ordered to include only non-index key columns)",
                        index_colid,
                        colid,
                        table->GetFieldName(colid));
                    return true;  // not an error, but we need to stop (this is a primary key expression)
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
        return visitor->onExpression(expr, vartype, colid, table, ClassifyWhereOperator(op_expr->opno), join_expr);
    }

    if (join_expr) {  // it is possible to see another table's column but not ours in implicit JOIN statements
        MOT_LOG_TRACE("visitSearchOpExpression(): Skipping expression %p of another table in implicit JOIN", op_expr);
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

static bool visitSearchExpressions(Query* query, MOT::Table* table, MOT::Index* index, Expr* expr, bool include_pkey,
    ExpressionVisitor* visitor, bool include_join_exprs, JitColumnExprArray* pkey_exprs)
{
    bool result = false;
    if (expr->type == T_OpExpr) {
        result = visitSearchOpExpression(
            query, table, index, (OpExpr*)expr, include_pkey, visitor, include_join_exprs, pkey_exprs);
    } else if (expr->type == T_BoolExpr) {
        result = visitSearchBoolExpression(
            query, table, index, (BoolExpr*)expr, include_pkey, visitor, include_join_exprs, pkey_exprs);
    } else {
        MOT_LOG_TRACE("Unsupported expression type %d while visiting search expressions", (int)expr->type);
    }
    return result;
}

static bool getSearchExpressions(Query* query, MOT::Table* table, MOT::Index* index, bool include_pkey,
    JitColumnExprArray* search_exprs, int* count, bool use_join_clause)
{
    MOT_LOG_TRACE("Getting search expressions for table %s, index %s (include-pkey: %s, use-join-clause: %s)",
        table->GetTableName().c_str(),
        index->GetName().c_str(),
        include_pkey ? "yes" : "no",
        use_join_clause ? "yes" : "no");
    ExpressionCollector expr_collector(query, search_exprs, count);
    Node* quals = query->jointree->quals;
    bool result =
        visitSearchExpressions(query, table, index, (Expr*)&quals[0], include_pkey, &expr_collector, use_join_clause);
    if (!result) {
        MOT_LOG_TRACE("Failed to get search expressions");
    } else {
        MOT_LOG_TRACE("Found %d expressions", *count);
    }
    return result;
}

static Node* getJoinQualifiers(const Query* query)
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

static bool getRangeSearchExpressions(
    Query* query, MOT::Table* table, MOT::Index* index, JitIndexScan* index_scan, JoinClauseType join_clause_type)
{
    MOT_LOG_TRACE("Getting range search expressions for table %s, index %s (join_clause_type: %s)",
        table->GetTableName().c_str(),
        index->GetName().c_str(),
        joinClauseTypeToString(join_clause_type));
    bool result = false;
    RangeScanExpressionCollector expr_collector(query, table, index, index_scan);
    if (!expr_collector.init()) {
        MOT_LOG_TRACE("Failed to initialize range search expression collector");
    } else {
        Node* quals = query->jointree->quals;
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
            expr_collector.evaluateScanType();
            result = true;
        }
    }
    return result;
}

static bool getTargetExpressions(Query* query, JitColumnExprArray* target_exprs)
{
    int i = 0;
    ListCell* lc = nullptr;

    foreach (lc, query->targetList) {
        TargetEntry* target_entry = (TargetEntry*)lfirst(lc);
        if (target_entry->resjunk) {
            MOT_LOG_TRACE("getTargetExpressions(): Skipping resjunk target entry");
            continue;
        }
        if (i < target_exprs->_count) {
            target_exprs->_exprs[i]._expr = parseExpr(query, target_entry->expr, 0, 0);
            if (target_exprs->_exprs[i]._expr == nullptr) {
                MOT_LOG_TRACE("getTargetExpressions(): Failed to parse target expression %d", i);
                return false;
            }
            target_exprs->_exprs[i]._table_column_id = target_entry->resno;  // update/insert
            if (target_entry->resorigtbl != 0) {                             // happens usually in INSERT
                target_exprs->_exprs[i]._table = MOT::GetTableManager()->GetTableByExternal(target_entry->resorigtbl);
                if (target_exprs->_exprs[i]._table == nullptr) {
                    MOT_LOG_TRACE("getTargetExpressions(): Failed to retrieve real table by id %d",
                        (int)target_entry->resorigtbl);
                    return false;
                }
            } else {
                // this is usually an update, and we retrieve the first table in rtable list
                RangeTblEntry* rte = (RangeTblEntry*)linitial(query->rtable);
                target_exprs->_exprs[i]._table = MOT::GetTableManager()->GetTableByExternal(rte->relid);
                if (target_exprs->_exprs[i]._table == nullptr) {
                    MOT_LOG_TRACE(
                        "getTargetExpressions(): Failed to retrieve real table by inferred id %d", (int)rte->relid);
                    return false;
                }
            }
            target_exprs->_exprs[i]._column_type = target_exprs->_exprs[i]._expr->_result_type;
            target_exprs->_exprs[i]._join_expr = false;
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

static bool getSelectExpressions(Query* query, JitSelectExprArray* select_exprs)
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
            JitExpr* sub_expr = parseExpr(query, target_entry->expr, 0, 0);
            if (sub_expr == nullptr) {
                MOT_LOG_TRACE("getSelectExpressions(): Failed to parse select expression %d", i);
                return false;
            }
            if (sub_expr->_expr_type != JIT_EXPR_TYPE_VAR) {
                MOT_LOG_TRACE("getSelectExpressions(): Unexpected non-var expression");
                return false;
            }
            select_exprs->_exprs[i]._column_expr = (JitVarExpr*)sub_expr;
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

// Count all leaves in parsed WHERE clause tree, that belong to the given table/index
// and have an EQUALS operator
static bool countWhereClauseEqualsLeaves(Query* query, MOT::Table* table, MOT::Index* index, int* count)
{
    MOT_LOG_TRACE("Counting WHERE clause leaf nodes with EQUALS operator for table %s and index %s",
        table->GetTableName().c_str(),
        index->GetName().c_str());
    ExpressionCounter expr_counter(count);
    Node* quals = query->jointree->quals;
    bool result = visitSearchExpressions(query, table, index, (Expr*)&quals[0], true, &expr_counter, false);
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

static bool checkQueryAttributes(const Query* query, bool allow_sorting, bool allow_aggregate)
{
    checkJittableAttribute(query, hasWindowFuncs);
    checkJittableAttribute(query, hasSubLinks);
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

    if (!allow_sorting) {
        checkJittableClause(query, sortClause);
    }

    if (!allow_aggregate) {
        checkJittableAttribute(query, hasAggs);
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
    bool result =
        visitSearchExpressions(query, table, index, (Expr*)&quals[0], false, &filter_counter, false, pkey_exprs);
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
    bool result =
        visitSearchExpressions(query, table, index, (Expr*)&quals[0], false, &filter_collector, false, pkey_exprs);
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
    JitAggregate aggregate;
    aggregate._aggreaget_op = JIT_AGGREGATE_NONE;
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
        if (!checkQueryAttributes(query, false, false)) {
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
            // point query does not expect sort clause or aggregate clause
            if (!checkQueryAttributes(query, false, false)) {
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
        } else {
            if (query->commandType == CMD_UPDATE) {
                if (!checkQueryAttributes(
                        query, false, false)) {  // range update does not expect sort clause or aggregate clause
                    MOT_LOG_TRACE(
                        "JitPrepareSimplePlan(): Disqualifying range update query - Invalid query attributes");
                } else {
                    plan = JitPrepareRangeUpdatePlan(query, table);
                }
            } else if (query->commandType == CMD_SELECT) {
                if (!checkQueryAttributes(
                        query, true, true)) {  // range select can specify sort clause or aggregate clause
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

    if (!checkQueryAttributes(
            query, false, true)) {  // we do not support join query with ORDER BY clause, but we can aggregate
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

extern JitPlan* JitPreparePlan(Query* query, const char* query_string)
{
    JitPlan* plan = nullptr;
    MOT_LOG_TRACE("Preparing plan for query: %s", query_string);

    // we start by checking the number of tables involved
    if (list_length(query->rtable) == 1) {
        plan = JitPrepareSimplePlan(query);
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
            if (select_plan->_aggregate._distinct) {
                result = true;
            }
        }
    } else if (plan->_plan_type == JIT_PLAN_JOIN) {
        JitJoinPlan* join_plan = (JitJoinPlan*)plan;
        if (join_plan->_aggregate._distinct) {
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

            case JIT_PLAN_INVALID:
            default:
                break;
        }
    }
}
}  // namespace JitExec
