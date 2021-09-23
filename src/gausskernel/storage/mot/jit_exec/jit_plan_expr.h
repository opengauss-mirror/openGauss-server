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
 * jit_plan_expr.h
 *    JIT execution plan expression helpers.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/jit_exec/jit_plan_expr.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef JIT_PLAN_EXPR_H
#define JIT_PLAN_EXPR_H

#include "postgres.h"
#include "nodes/primnodes.h"
#include "nodes/parsenodes.h"
#include "storage/mot/jit_def.h"
#include "jit_common.h"
#include "utilities.h"

#include <algorithm>

namespace JitExec {
/** @enum Index scan direction constants. */
enum JitIndexScanDirection {
    /** @var No scan direction. */
    JIT_INDEX_SCAN_NONE,

    /** @var Forward scan direction. */
    JIT_INDEX_SCAN_FORWARD,

    /** @var Backwards scan direction. */
    JIT_INDEX_SCAN_BACKWARDS
};

/** @enum Index scan types. */
enum JitIndexScanType {
    /** @var Invalid index scan. */
    JIT_INDEX_SCAN_TYPE_INVALID,

    /** @var Full scan. */
    JIT_INDEX_SCAN_FULL,

    /** @var Closed index scan (all columns are specified with equals operator. */
    JIT_INDEX_SCAN_CLOSED,

    /** @var Open index scan (last column is specified with less-than, greater than operators). */
    JIT_INDEX_SCAN_OPEN,

    /** @var Semi-open index scan (last column is specified with either less-than or greater than operator). */
    JIT_INDEX_SCAN_SEMI_OPEN,

    /** @var Point query index scan (all unique index columns are specified). */
    JIT_INDEX_SCAN_POINT
};

/** @enum Aggregate operator type. */
enum JitAggregateOperator {
    /** @var No aggregate operator. */
    JIT_AGGREGATE_NONE,

    /** @var AVG aggregate operator. */
    JIT_AGGREGATE_AVG,

    /** @var SUM aggregate operator. */
    JIT_AGGREGATE_SUM,

    /** @var MAX aggregate operator. */
    JIT_AGGREGATE_MAX,

    /** @var MAX aggregate operator. */
    JIT_AGGREGATE_MIN,

    /** @var COUNT aggregate operator. */
    JIT_AGGREGATE_COUNT
};

/** @enum Expression types. */
enum JitExprType {
    /** @var Invalid expression type. */
    JIT_EXPR_TYPE_INVALID,

    /** @var Constant expression type. */
    JIT_EXPR_TYPE_CONST,

    /** @var Parameter expression type. */
    JIT_EXPR_TYPE_PARAM,

    /** @var Column expression type. */
    JIT_EXPR_TYPE_VAR,

    /** @var Operator expression type. */
    JIT_EXPR_TYPE_OP,

    /** @var Function expression type. */
    JIT_EXPR_TYPE_FUNC,

    /** @var Sub-link expression type (for a sub-query). */
    JIT_EXPR_TYPE_SUBLINK,

    JIT_EXPR_TYPE_BOOL
};

/** @enum Join scan types. */
enum JitJoinScanType {
    /** @var Invalid join scan. */
    JIT_JOIN_SCAN_INVALID,

    /** @var Both outer and inner loops are actually point queries. */
    JIT_JOIN_SCAN_POINT,

    /** @var The outer loop is a point query, and the inner loop is a range scan. */
    JIT_JOIN_SCAN_OUTER_POINT,

    /** @var The outer loop is a range scan and the inner loop is a point query. */
    JIT_JOIN_SCAN_INNER_POINT,

    /** @var Both outer and inner loops are range scans. */
    JIT_JOIN_SCAN_RANGE
};

enum JoinClauseType { JoinClauseNone, JoinClauseExplicit, JoinClauseImplicit };

struct JitExpr {
    /** @var The expression type. */
    JitExprType _expr_type;

    /** @var The original expression in the parsed query (required for convenient filter collection). */
    Expr* _source_expr;

    /** @var The expression result type. */
    int _result_type;

    /** @var The position of the expression in the arg-is-null array. */
    int _arg_pos;
};

/** @struct A parsed constant expression. */
struct JitConstExpr {
    /** @var The expression type (always @ref JIT_EXPR_TYPE_CONST). */
    JitExprType _expr_type;

    /** @var The original expression in the parsed query (required for convenient filter collection). */
    Expr* _source_expr;

    /** @var The constant type. */
    int _const_type;

    /** @var The position of the expression in the arg-is-null array. */
    int _arg_pos;

    /** @var The constant value. */
    Datum _value;

    /** @var Specifies whether this is a null value. */
    bool _is_null;
};

/** @struct A parsed parameter expression. */
struct JitParamExpr {
    /** @var The expression type. */
    JitExprType _expr_type;

    /** @var The original expression in the parsed query (required for convenient filter collection). */
    Expr* _source_expr;

    /** @var The parameter type (always @ref JIT_EXPR_TYPE_PARAM). */
    int _param_type;

    /** @var The position of the expression in the arg-is-null array. */
    int _arg_pos;

    /** @var The zero-based index of the parameter in the parameter array. */
    int _param_id;
};

/** @struct A parsed column expression. */
struct JitVarExpr {
    /** @var The expression type (always @ref JIT_EXPR_TYPE_VAR). */
    JitExprType _expr_type;

    /** @var The original expression in the parsed query (required for convenient filter collection). */
    Expr* _source_expr;

    /** @var The column type. */
    int _column_type;

    /** @var The position of the expression in the arg-is-null array. */
    int _arg_pos;

    /** @var The column position. */
    int _column_id;

    /** @var The source table of the column. */
    MOT::Table* _table;
};

struct JitOpExpr {
    /** @var The expression type (always @ref JIT_EXPR_TYPE_OP). */
    JitExprType _expr_type;

    /** @var The original expression in the parsed query (required for convenient filter collection). */
    Expr* _source_expr;

    /** @var The expression result type. */
    int _result_type;

    /** @var The position of the expression in the arg-is-null array. */
    int _arg_pos;

    /** @var The operator number. */
    int _op_no;

    /** @var The correlating function id. */
    int _op_func_id;

    /** @var The operator arguments (3 at most). */
    JitExpr* _args[MOT_JIT_MAX_FUNC_EXPR_ARGS];

    /** @var The number of arguments used in the operator. */
    int _arg_count;
};

struct JitFuncExpr {
    /** @var The expression type (always @ref JIT_EXPR_TYPE_FUNC). */
    JitExprType _expr_type;

    /** @var The original expression in the parsed query (required for convenient filter collection). */
    Expr* _source_expr;

    /** @var The expression result type. */
    int _result_type;

    /** @var The position of the expression in the arg-is-null array. */
    int _arg_pos;

    /** @var The correlating function id. */
    int _func_id;

    /** @var The function arguments (3 at most). */
    JitExpr* _args[MOT_JIT_MAX_FUNC_EXPR_ARGS];

    /** @var The number of arguments used in the function. */
    int _arg_count;
};

struct JitSubLinkExpr {
    /** @var The expression type (always @ref JIT_EXPR_TYPE_SUBLINK). */
    JitExprType _expr_type;

    /** @var The original expression in the parsed query (required for extracting the sub-query). */
    Expr* _source_expr;

    /** @var The sub-query result type. */
    int _result_type;

    /** @var The position of the expression in the arg-is-null array. */
    int _arg_pos;

    /** @var The position of the sub-query plan in the sub-query plan array of the containing compound plan. */
    int _sub_query_index;
};

struct JitBoolExpr {
    /** @var The expression type (always @ref JIT_EXPR_TYPE_SUBLINK). */
    JitExprType _expr_type;

    /** @var The original expression in the parsed query (required for extracting the sub-query). */
    Expr* _source_expr;

    /** @var The expression result type (always BOOLOID). */
    int _result_type;

    /** @var The position of the expression in the arg-is-null array. */
    int _arg_pos;

    /** @var The correlating Boolean operator type. */
    BoolExprType _bool_expr_type;

    /** @var The Boolean operator arguments (2 at most). */
    JitExpr* _args[MOT_JIT_MAX_BOOL_EXPR_ARGS];

    /** @var The number of arguments used in the Boolean operator. */
    int _arg_count;
};

/** @struct An expression tied to a table column. */
struct JitColumnExpr {
    /** @var The expression. */
    JitExpr* _expr;

    /** @var The table column id. */
    int _table_column_id;

    /** @var The table to which the column belongs. */
    MOT::Table* _table;

    /** @var The type of the column. */
    int _column_type;

    /** @var Specifies that this is a join expression (and the @ref _expr member is a Var expression). */
    bool _join_expr;
};

/** @struct An array of column-tied expressions. */
struct JitColumnExprArray {
    /** @var The column-tied expressions. */
    JitColumnExpr* _exprs;

    /** @var The amount of items in the array. */
    int _count;
};

/** @struct A column select expression used in SELECT statements. */
struct JitSelectExpr {
    /** @var The expression. */
    JitVarExpr* _column_expr;

    /** @var The zero-based output tuple column id. */
    int _tuple_column_id;
};

/** @struct An array of select expressions. */
struct JitSelectExprArray {
    /** @var The expressions array . */
    JitSelectExpr* _exprs;

    /** @var The amount of items in the array. */
    int _count;
};

/** @struct Scan filter. */
struct JitFilter {
    /** @var The left-hand side operand. */
    JitExpr* _lhs_operand;

    /** @var The right-hand side operand. */
    JitExpr* _rhs_operand;

    /** @var The operator to evaluate (see catalog/pg_operator.h). */
    int _filter_op;

    /** @var The function identifier of the operator to evaluate (see catalog/pg_proc.h). */
    int _filter_op_funcid;
};

/** @struct Scan filter array. */
struct JitFilterArray {
    /** @var An array of scan filters used to filter rows in the scan. */
    JitFilter* _scan_filters;

    /** @var The number of filters used. */
    int _filter_count;
};

/** @struct The data required to plan a point query. */
struct JitPointQuery {
    /** @var The table being used (always using primary index). */
    MOT::Table* _table;

    /**
     * @var The array of expressions used to search a row. The amount of items in the array equals to
     * the number of columns in the primary key.
     */
    JitColumnExprArray _search_exprs;

    /** @var Any additional filters imposed on the scan. */
    JitFilterArray _filters;
};

/** @struct Index scan. */
struct JitIndexScan {
    /** @var The table being scanned. */
    MOT::Table* _table;

    /** @var The zero-based identifier of the index being used for scanning. */
    int _index_id;

    /** @var The number of columns participating in the scan. */
    int _column_count;

    /** @var The type of scan being used. */
    JitIndexScanType _scan_type;

    /**
     * @var Array of expressions to search for the tuple, matching index columns. In closed scans, the
     * number of expressions equals the column count. In open scans there is an addition expression,
     * and the last two expressions specify the lower and upper bound for the scan. In a semi-open
     * scan the expression count equals the column count, and the last expression specifies the lower
     * or upper bound.
     */
    JitColumnExprArray _search_exprs;

    /**
     * @var The operator class used for one bound in an open or semi-open scan. In a semi-open scan
     * this refers to the last expression in the search expression array. In an open scan, this refers
     * to the expression before the last-expression in the search expression array.
     */
    JitWhereOperatorClass _last_dim_op1;

    /**
     * @var The operator class used for the second bound in an open scan. This always refers to the
     * last expression in the search expression array.
     */
    JitWhereOperatorClass _last_dim_op2;

    /** @var The query sort order. */
    JitQuerySortOrder _sort_order;

    /** @var The scan direction. */
    JitIndexScanDirection _scan_direction;

    /** @var Any additional filters imposed on the scan. */
    JitFilterArray _filters;
};

/** @struct Specifies aggregation parameters. */
struct JitAggregate {
    /** @var An aggregate function (if one is specified then only one select expression should exist). */
    JitAggregateOperator _aggreaget_op;

    /** @var The aggregate function identifier. */
    int _func_id;

    /** @var The table column id to aggregate (we always aggregate into slot tuple column id 0). */
    int _table_column_id;

    /** @var The table to which the aggregated column belongs (required if this is in JOIN). */
    MOT::Table* _table;

    /** @var The type of the aggregated value. */
    int _element_type;

    /** @var The element type used for average calculation array. */
    int _avg_element_type;

    /** @var The element count used for average calculation array. */
    int _avg_element_count;

    /** @var Specifies whether this is a distinct aggregation. */
    bool _distinct;
};

/** @struct Specifies join of an outer column with an inner column. */
struct JitJoinExpr {
    /** @var The outer column identifier. */
    int _outer_column_id;

    /** @var The inner column identifier. */
    int _inner_column_id;
};

// Parent class for all expression visitors
class ExpressionVisitor {
public:
    ExpressionVisitor()
    {}

    virtual ~ExpressionVisitor()
    {}

    virtual bool OnFilterExpr(int filterOp, int filterOpFuncId, Expr* lhs, Expr* rhs)
    {
        return true;
    }

    virtual bool OnExpression(
        Expr* expr, int columnType, int tableColumnId, MOT::Table* table, JitWhereOperatorClass opClass, bool joinExpr)
    {
        return true;
    }

protected:
    DECLARE_CLASS_LOGGER()
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

    bool OnExpression(Expr* expr, int columnType, int tableColumnId, MOT::Table* table, JitWhereOperatorClass opClass,
        bool joinExpr) final;

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

    bool OnExpression(Expr* expr, int columnType, int tableColumnId, MOT::Table* table, JitWhereOperatorClass opClass,
        bool joinExpr) final;

private:
    Query* _query;
    JitColumnExprArray* _expr_array;
    int* _expr_count;

    void Cleanup();
};

// Expression visitor that collects expressions for possibly open range scans
// This requires special care: sort operators by index column id and evaluate the scan type
class RangeScanExpressionCollector : public ExpressionVisitor {
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

    bool Init();

    bool OnExpression(Expr* expr, int columnType, int tableColumnId, MOT::Table* table, JitWhereOperatorClass opClass,
        bool joinExpr) final;

    void EvaluateScanType();

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

    bool DetermineScanType(JitIndexScanType& scanType, int& columnCount);

    void Cleanup();

    bool RemoveDuplicates();

    int RemoveSingleDuplicate();

    static int IntCmp(int lhs, int rhs);

    static bool IndexOpCmp(const IndexOpClass& lhs, const IndexOpClass& rhs);

    bool ScanHasHoles(JitIndexScanType scan_type) const;
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

    bool OnFilterExpr(int filterOp, int filterOpFuncId, Expr* lhs, Expr* rhs) final
    {
        ++(*_count);
        return true;
    }

private:
    int* _count;
};

// Expression visitor that collects filters
class FilterCollector : public ExpressionVisitor {
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

    bool OnFilterExpr(int filterOp, int filterOpFuncId, Expr* lhs, Expr* rhs) final;

private:
    Query* _query;
    JitFilterArray* _filter_array;
    int* _filter_count;

    void Cleanup();
};

// Expression visitor that fetches a single sub-link
class SubLinkFetcher : public ExpressionVisitor {
public:
    explicit SubLinkFetcher() : _subLink(nullptr), _count(0)
    {}

    ~SubLinkFetcher() final
    {}

    inline SubLink* GetSubLink()
    {
        return _subLink;
    }

    bool OnExpression(Expr* expr, int columnType, int tableColumnId, MOT::Table* table, JitWhereOperatorClass opClass,
        bool joinExpr) final;

private:
    SubLink* _subLink;
    int _count;
};

MOT::Table* getRealTable(const Query* query, int table_ref_id, int column_id);
int getRealColumnId(const Query* query, int table_ref_id, int column_id, const MOT::Table* table);
void freeExpr(JitExpr* expr);
bool visitSearchExpressions(Query* query, MOT::Table* table, MOT::Index* index, Expr* expr, bool include_pkey,
    ExpressionVisitor* visitor, bool include_join_exprs, JitColumnExprArray* pkey_exprs = nullptr);
bool getSearchExpressions(Query* query, MOT::Table* table, MOT::Index* index, bool include_pkey,
    JitColumnExprArray* search_exprs, int* count, bool use_join_clause);
bool getRangeSearchExpressions(
    Query* query, MOT::Table* table, MOT::Index* index, JitIndexScan* index_scan, JoinClauseType join_clause_type);
bool getTargetExpressions(Query* query, JitColumnExprArray* target_exprs);
bool getSelectExpressions(Query* query, JitSelectExprArray* select_exprs);
}  // namespace JitExec

#endif /* JIT_PLAN_EXPR_H */
