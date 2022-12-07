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
 *    src/gausskernel/storage/mot/jit_exec/jit_plan.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef JIT_PLAN_H
#define JIT_PLAN_H

#include "jit_plan_expr.h"

namespace JitExec {
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
    JIT_PLAN_JOIN,

    /** @var Plan for a query with sub-queries. */
    JIT_PLAN_COMPOUND,

    /** @var Plan for stored procedure execution. */
    JIT_PLAN_FUNCTION,

    /** @var Plan for invoking a stored procedure. */
    JIT_PLAN_INVOKE
};

/** @struct The parent struct for all plans. */
struct JitPlan {
    /** @var The type of plan being used. */
    JitPlanType _plan_type;

    /** @var The command type being used (select, update or delete). */
    JitCommandType _command_type;
};

/** @struct Insert plan. */
struct JitInsertPlan {
    /** @var The type of plan being used (always @ref JIT_PLAN_INSERT_QUERY). */
    JitPlanType _plan_type;

    /** @var The command type being used (select, update or delete). */
    JitCommandType _command_type;

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

    /**
     * @var The command type being used (@ref JIT_COMMAND_UPDATE, @ref JIT_COMMAND_SELECT, or
     * @ref JIT_COMMAND_DELETE).
     */
    JitCommandType _command_type;

    /** @var Defines how to make the scan. */
    JitIndexScan _index_scan;
};

/** @strut Plan for UPDATE range queries. */
struct JitRangeUpdatePlan {
    /** @var The type of plan being used (always @ref JIT_PLAN_RANGE_SCAN). */
    JitPlanType _plan_type;

    /** @var The command type being used (always @ref JIT_COMMAND_UPDATE). */
    JitCommandType _command_type;

    /** @var Defines how to make the scan. */
    JitIndexScan _index_scan;

    /** @var Array of expressions update in each row found. */
    JitColumnExprArray _update_exprs;
};

/** @struct Plan for SELECT range queries. */
struct JitRangeSelectPlan {
    /** @var The type of plan being used (always @ref JIT_PLAN_RANGE_SCAN). */
    JitPlanType _plan_type;

    /** @var The command type being used (always @ref JIT_COMMAND_SELECT). */
    JitCommandType _command_type;

    /** @var Defines how to make the scan. */
    JitIndexScan _index_scan;

    /** @var Array of expressions to copy to the result tuple. */
    JitSelectExprArray _select_exprs;

    /** @var Limit on number of rows returned to the user (zero for none). */
    int _limit_count;

    /** @var Aggregate count. */
    int m_aggCount;

    /** @var Aggregate functions (if one is specified then only one select expression should exist). */
    JitAggregate* m_aggregates;

    /** @var Required params for non native sort. */
    JitNonNativeSortParams* m_nonNativeSortParams;
};

/** @strut Plan for range DELETE queries. */
struct JitRangeDeletePlan {
    /** @var The type of plan being used (always @ref JIT_PLAN_RANGE_SCAN). */
    JitPlanType _plan_type;

    /** @var The command type being used (either @ref JIT_COMMAND_DELETE). */
    JitCommandType _command_type;

    /** @var Defines how to make the scan. */
    JitIndexScan _index_scan;

    /** @var Limit on number of rows deleted (zero for none). */
    int _limit_count;
};

/** @strut Plan for JOIN queries. */
struct JitJoinPlan {
    /** @var The type of plan being used (always @ref JIT_PLAN_JOIN). */
    JitPlanType _plan_type;  // always JIT_PLAN_JOIN

    /** @var The command type being used (select, update or delete). */
    JitCommandType _command_type;

    /** @var The type of join scan. */
    JitJoinScanType _scan_type;

    /** @var The kind of join being performed. */
    JitJoinType _join_type;

    /** @var Defines how to make the outer loop scan. */
    JitIndexScan _outer_scan;

    /** @var Defines how to make the inner loop scan. */
    JitIndexScan _inner_scan;

    /** @var Array of expressions to join from outer loop into inner loop search. */
    JitJoinExpr _join_exprs;

    /** @var Filters imposed on the query involving both tables. */
    JitFilterArray _filters;

    /** @var Array of expressions to copy to the result tuple. */
    JitSelectExprArray _select_exprs;

    /** @var Limit on number of rows returned to the user (zero for none). */
    int _limit_count;

    /** @var Aggregate count. */
    int m_aggCount;

    /** @var Aggregate functions (if one is specified then only one select expression should exist). */
    JitAggregate* m_aggregates;
};

/** @strut Plan for compound point query with a single sub-query. */
struct JitCompoundPlan {
    /** @var The type of plan being used (always @ref JIT_PLAN_COMPOUND). */
    JitPlanType _plan_type;  // always JIT_PLAN_COMPOUND

    /** @var The command type being used (currently always @ref JIT_COMMAND_SELECT). */
    JitCommandType _command_type;

    /** @var The outer point query plan (currently only one column refers to a sub-query). */
    JitPointQueryPlan* _outer_query_plan;

    /** @var The number of sub-queries (currently always limited to 1 by planning phase). */
    uint32_t _sub_query_count;

    /** @var The sub-query plans (each one must evaluate to a single value, whether by point, aggregate or limit 1). */
    JitPlan** _sub_query_plans;
};

/** @struct Plan for calling a query from within a jitted stored procedure. */
struct JitCallSitePlan {
    /** @ var The plan for jittable query, otherwise null. */
    JitPlan* m_queryPlan;

    /** @var The SPI plan for the query (must be valid - all JTI plan expressions depend on it). */
    SPIPlanPtr m_spiPlan;

    /** @var The PL/PG SQL expression from which the query comes. */
    PLpgSQL_expr* m_expr;

    /** @var The index of the expression according to visitation order in JIT planning. */
    int m_exprIndex;

    /** @var Holds the command type for non-jittable sub-queries. */
    CmdType m_queryCmdType;

    /** @var Holds the query string for non-jittable sub-queries. */
    char* m_queryString;

    /** @var The actual number of parameters used by the sub-query. */
    int m_callParamCount;

    /** @var Denotes whether this is an unjittable invoke query. */
    uint8_t m_isUnjittableInvoke;

    /** @var Denotes whether this is a modifying statement. */
    uint8_t m_isModStmt;

    /** @var Denotes whether the query result should be put into local variable. */
    uint8_t m_isInto;

    /** @var Align next member to 8 bytes. */
    uint8_t m_padding;

    /**  @var Information requires to form the parameter list used to invoke the query. */
    JitCallParamInfo* m_callParamInfo;

    /** @var The global tuple descriptor used to prepare tuple table slot object for the sub-query result. */
    TupleDesc m_tupDesc;
};

/** @strut Plan for invoke stored procedure. */
struct JitFunctionPlan {
    JitPlanType _plan_type;  // always JIT_PLAN_FUNCTION

    /** @var The command type being used (select, update or delete). */
    JitCommandType _command_type;

    /** @var The executed function identifier in the system catalog. */
    Oid _function_id;

    /** @var The executed function name. */
    char* _function_name;

    /** @var The compiled function. */
    PLpgSQL_function* m_function;

    /** @var The list of parameter types passed to the function. */
    Oid* m_paramTypes;

    /** @var The number of parameters passed to the function (this includes all datums). */
    uint32_t m_paramCount;

    /** @var The number of arguments passed to the function (this includes only input arguments). */
    uint32_t m_argCount;

    /** @var The context of the jitted queries for each query in the stored procedure. */
    JitCallSitePlan* m_callSitePlanList;

    /** @var The number of sub-queries in the executed stored procedure. */
    int _query_count;
};

/** @strut Plan for invoke stored procedure. */
struct JitInvokePlan {
    JitPlanType _plan_type;  // always JIT_PLAN_INVOKE

    /** @var The command type being used (select, update or delete). */
    JitCommandType _command_type;

    /** @var The executed function identifier in the system catalog. */
    Oid _function_id;

    /** @var The executed function name. */
    char* _function_name;

    /** @var The executed function xmin. */
    TransactionId m_functionTxnId;

    /** @var The context of the jitted function. */
    JitFunctionPlan* m_functionPlan;

    /** @var The arguments passed to the executed function. */
    JitExpr** _args;

    /** @var The number of arguments passed to the executed function. */
    int _arg_count;

    /** @var Default parameter expression array. */
    JitExpr** m_defaultParams;

    /** @var Default parameter count. */
    int m_defaultParamCount;
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

/**
 * @brief Prepares a lite execution plan for a stored procedure. This plan will be later used to generate jitted code.
 * @param function The parsed stored procedure.
 * @param functionSource The source code of the stored procedure.
 * @param functionName The name of the stored procedure.
 * @param functionOid The function identifier.
 * @param isStrict Specified whether function is defined as strict (i.e. does not allow null parameters).
 * @return The plan, or null of failed.
 */
extern JitPlan* JitPrepareFunctionPlan(
    PLpgSQL_function* function, const char* functionSource, const char* functionName, Oid functionOid, bool isStrict);

/** @brief Queries whether a plan has a DISTINCT operator. */
extern bool JitPlanHasDistinct(JitPlan* plan);

/** @brief Queries whether a plan has an ORDER BY specifier. */
extern bool JitPlanHasSort(JitPlan* plan);

/**
 * @brief Explains how a plan is to be executed.
 * @param query The parsed SQL query for which the jitted plan is to be explained.
 * @param plan The plan to be explained.
 */
extern void JitExplainPlan(Query* query, JitPlan* plan);

/**
 * @brief Explains how a plan is to be executed.
 * @param function The parsed stored procedure for which the jitted plan is to be explained.
 * @param plan The plan to be explained.
 */
extern void JitExplainPlan(PLpgSQL_function* function, JitPlan* plan);

/**
 * @brief Queries whether a query is acceptable for JIT.
 * @param query The query to check.
 * @param allowSorting Specifies whether sorting clause is allowed.
 * @param allowAggregate Specifies whether aggregate clause is allowed.
 * @param allowSublink Specifies whether sub-queries are allowed.
 * @return True if the query is acceptable for JIT, otherwise false.
 */
extern bool CheckQueryAttributes(const Query* query, bool allowSorting, bool allowAggregate, bool allowSublink);

/**
 * @brief Queries whether a plan involves function plan.
 * @param plan The plan to check.
 * @param[out] outPlan The function plan.
 * @return True if the given plan is an invoke or a function plan.
 */
extern bool JitPlanHasFunctionPlan(JitPlan* plan, JitFunctionPlan** outPlan);
}  // namespace JitExec

#endif /* JIT_PLAN_H */
