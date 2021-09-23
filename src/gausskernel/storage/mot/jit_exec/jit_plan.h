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
    JIT_PLAN_COMPOUND
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

    /** @var The command type being used (always @ref JIT_COMMAND_UPDATE). */
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

    /** @var The command type being used (always @ref JIT_COMMAND_SELECT). */
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

/** @brief Queries whether a plan has an ORDER BY specifier. */
extern bool JitPlanHasSort(JitPlan* plan);

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
