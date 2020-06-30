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
 * jit_plan.h
 *    JIT execution plan generation.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/jit_exec/src/jit_plan.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef JIT_PLAN_H
#define JIT_PLAN_H

#include "postgres.h"
#include "nodes/primnodes.h"
#include "nodes/parsenodes.h"
#include "storage/mot/jit_def.h"
#include "jit_common.h"
#include "mot_engine.h"

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

/** @enum Plan types*/
enum JitPlanType {
    /** @var Invalid plan. */
    JIT_PLAN_INVALID,

    /** @var Plan for insert query. */
    JIT_PLAN_INSERT_QUERY,

    /** @var Plan for simple point queries. */
    JIT_PLAN_POINT_QUERY,

    /** @var Plan for a range scan. */
    JIT_PLAN_RANGE_SCAN,

    /** @var Plan for a nested loop join. */
    JIT_PLAN_JOIN
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
    JIT_EXPR_TYPE_FUNC
};

/** @enum Index scan types. */
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

struct JitExpr {
    /** @var The expression type. */
    JitExprType _expr_type;

    /** @var The original expression in the parsed query (required for convenient filter collection). */
    Expr* _source_expr;

    /** @var The column type. */
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

    /** @var The column type. */
    int _result_type;

    /** @var The position of the expression in the arg-is-null array. */
    int _arg_pos;

    /** @var The operator number. */
    int _op_no;

    /** @var The correlating function id. */
    int _op_func_id;

    /** @var The operator arguments (3 at most). */
    JitExpr* _args[3];

    /** @var The number of arguments used in the operator. */
    int _arg_count;
};

struct JitFuncExpr {
    /** @var The expression type (always @ref JIT_EXPR_TYPE_FUNC). */
    JitExprType _expr_type;

    /** @var The original expression in the parsed query (required for convenient filter collection). */
    Expr* _source_expr;

    /** @var The column type. */
    int _result_type;

    /** @var The position of the expression in the arg-is-null array. */
    int _arg_pos;

    /** @var The correlating function id. */
    int _func_id;

    /** @var The function arguments (3 at most). */
    JitExpr* _args[3];

    /** @var The number of arguments used in the function. */
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

/** @struct The parent struct for all plans. */
struct JitPlan {
    /** @var The type of plan being used. */
    JitPlanType _plan_type;
};

/** @struct Insert plan. */
struct JitInsertPlan {
    /** @var The type of plan being used (always @ref JIT_PLAN_INSERT_QUERY). */
    JitPlanType _plan_type;

    /** @var The table into which the tuple is to be inserted. */
    MOT::Table* _table;

    /** @var Array of expressions to insert into the table. */
    JitColumnExprArray _insert_exprs;
};

/** @strut Plan for point queries. */
struct JitPointQueryPlan {
    /** @var The type of plan being used (always @ref JIT_PLAN_POINT_QUERY). */
    JitPlanType _plan_type;

    /** @var The command type being used (select, update or delete). */
    JitCommandType _command_type;

    /** @var The search parameters. */
    JitPointQuery _query;
};

/** @strut Plan for DELETE point queries. */
struct JitDeletePlan {
    /** @var The type of plan being used (always @ref JIT_PLAN_POINT_QUERY). */
    JitPlanType _plan_type;

    /** @var The command type being used (always @ref JIT_COMMAND_DELETE). */
    JitCommandType _command_type;

    /** @var The search parameters. */
    JitPointQuery _query;
};

/** @strut Plan for UPDATE point queries. */
struct JitUpdatePlan {
    /** @var The type of plan being used (always @ref JIT_PLAN_POINT_QUERY). */
    JitPlanType _plan_type;

    /** @var The command type being used (always @ref JIT_COMMAND_UPDATE). */
    JitCommandType _command_type;

    /** @var The search parameters. */
    JitPointQuery _query;

    /** @var Array of expressions update in the row found. */
    JitColumnExprArray _update_exprs;
};

/** @strut Plan for SELECT point queries. */
struct JitSelectPlan {
    /** @var The type of plan being used (always @ref JIT_PLAN_POINT_QUERY). */
    JitPlanType _plan_type;

    /** @var The command type being used (always @ref JIT_COMMAND_SELECT). */
    JitCommandType _command_type;

    /** @var The search parameters. */
    JitPointQuery _query;

    /** @var Array of expressions to copy to the result tuple. */
    JitSelectExprArray _select_exprs;
};

/** @strut Plan for range queries. */
struct JitRangeScanPlan {
    /** @var The type of plan being used (always @ref JIT_PLAN_RANGE_SCAN). */
    JitPlanType _plan_type;

    /** @var The command type being used (either @ref JIT_COMMAND_UPDATE or @ref JIT_COMMAND_SELECT). */
    JitCommandType _command_type;

    /** @var Defines how to make the scan. */
    JitIndexScan _index_scan;
};

/** @strut Plan for UPDATE range queries. */
struct JitRangeUpdatePlan {
    /** @var The type of plan being used (always @ref JIT_PLAN_RANGE_SCAN). */
    JitPlanType _plan_type;

    /** @var The command type being used (always @ref JIT_COMMAND_RANGE_UPDATE). */
    JitCommandType _command_type;

    /** @var Defines how to make the scan. */
    JitIndexScan _index_scan;

    /** @var Array of expressions update in each row found. */
    JitColumnExprArray _update_exprs;
};

/** @strut Plan for SELECT range queries. */
struct JitRangeSelectPlan {
    /** @var The type of plan being used (always @ref JIT_PLAN_RANGE_SCAN). */
    JitPlanType _plan_type;

    /** @var The command type being used (always @ref JIT_COMMAND_RANGE_SELECT). */
    JitCommandType _command_type;

    /** @var Defines how to make the scan. */
    JitIndexScan _index_scan;

    /** @var Array of expressions to copy to the result tuple. */
    JitSelectExprArray _select_exprs;

    /** @var Limit on number of rows returned to the user (zero for none). */
    int _limit_count;

    /** @var An aggregate function (if one is specified then only one select expression should exist). */
    JitAggregate _aggregate;
};

/** @strut Plan for JOIN queries. */
struct JitJoinPlan {
    /** @var The type of plan being used (always @ref JIT_PLAN_JOIN). */
    JitPlanType _plan_type;  // always JIT_PLAN_JOIN

    /** @var The type of join scan. */
    JitJoinScanType _scan_type;

    /** @var Defines how to make the outer loop scan. */
    JitIndexScan _outer_scan;

    /** @var Defines how to make the inner loop scan. */
    JitIndexScan _inner_scan;

    /** @var Array of expressions to join from outer loop into inner loop search. */
    JitJoinExpr _join_exprs;

    /** @var Array of expressions to copy to the result tuple. */
    JitSelectExprArray _select_exprs;

    /** @var Limit on number of rows returned to the user (zero for none). */
    int _limit_count;

    /** @var An aggregate function (if one is specified then only one select expression should exist). */
    JitAggregate _aggregate;
};

/** @define A special constant denoting a plan is not needed since jitted query has already been generated. */
#define MOT_READY_JIT_PLAN ((JitPlan*)-1)

/**
 * @brief Prepares a lite execution plan for a query. This plan will be later used to generate jitted code.
 * @param query The parsed SQL query for which a jitted plan is to be generated.
 * @param queryString The query text.
 * @return The plan, or NULL of failed.
 */
extern JitPlan* JitPreparePlan(Query* query, const char* queryString);

/** @brief Queries whether a plan has a DISTINCT operator. */
extern bool JitPlanHasDistinct(JitPlan* plan);

/**
 * @brief Explains how a plan is to be executed.
 * @param query The parsed SQL query for which the jitted plan is to be explained.
 * @param plan The plan to be explained.
 */
extern void JitExplainPlan(Query* query, JitPlan* plan);

/**
 * @brief Releases all resources associated with a plan.
 * @param plan The plan to destroy.
 */
extern void JitDestroyPlan(JitPlan* plan);
}  // namespace JitExec

#endif /* JIT_PLAN_H */
