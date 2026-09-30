/* -------------------------------------------------------------------------
 *
 * unionall_faststart.cpp
 *	  UNION ALL Fast-Start reorder logic implementation; see unionall_faststart.h.
 *
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2026. All rights reserved.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/gausskernel/optimizer/plan/unionall_faststart.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "nodes/plannodes.h"
#include "nodes/relation.h"
#include "optimizer/unionall_faststart.h"

static const int UNION_ALL_FOUR_BRANCH_ORDERS[24][4] = {
    {0, 1, 2, 3}, {0, 1, 3, 2}, {0, 2, 1, 3}, {0, 2, 3, 1}, {0, 3, 1, 2}, {0, 3, 2, 1},
    {1, 0, 2, 3}, {1, 0, 3, 2}, {1, 2, 0, 3}, {1, 2, 3, 0}, {1, 3, 0, 2}, {1, 3, 2, 0},
    {2, 0, 1, 3}, {2, 0, 3, 1}, {2, 1, 0, 3}, {2, 1, 3, 0}, {2, 3, 0, 1}, {2, 3, 1, 0},
    {3, 0, 1, 2}, {3, 0, 2, 1}, {3, 1, 0, 2}, {3, 1, 2, 0}, {3, 2, 0, 1}, {3, 2, 1, 0}};

/*
 * Check only the branch root node; deep Limit is covered by the branch's own
 * startup cost. Only root-node special Limit (PERCENT/WITH TIES) skips reorder.
 * Setops leaf plans are wrapped in a one-level SubqueryScan by
 * recurse_set_operations; peel that wrapper so the leaf plan root is checked.
 */
static bool branch_root_has_special_limit(Plan* plan)
{
    if (plan == NULL) {
        return false;
    }
    if (IsA(plan, SubqueryScan)) {
        plan = ((SubqueryScan*)plan)->subplan;
    }
    if (plan == NULL) {
        return false;
    }
    return (IsA(plan, Limit) || IsA(plan, VecLimit)) &&
            (((Limit*)plan)->isPercent || ((Limit*)plan)->withTies);
}

/*
 * Parse the LIMIT constant of the current query block.
 * Return -1 if no LIMIT, not a constant, unsupported type, or NULL.
 */
static int64 eval_top_level_limit(Query* parse)
{
    if (parse->limitCount == NULL) {
        return -1;
    }
    if (!IsA(parse->limitCount, Const)) {
        return -1;
    }

    Const* c = (Const*)parse->limitCount;
    if (c->consttype != INT8OID && c->consttype != INT4OID) {
        return -1;
    }
    if (c->constisnull) {
        return -1;
    }

    return (c->consttype == INT8OID) ? DatumGetInt64(c->constvalue) : (int64)DatumGetInt32(c->constvalue);
}

/*
 * Parse the OFFSET constant of the current query block.
 * Return 0 if no OFFSET or NULL; return -1 if not parseable.
 */
static int64 eval_top_level_offset(Query* parse)
{
    if (parse->limitOffset == NULL) {
        return 0;
    }
    if (!IsA(parse->limitOffset, Const)) {
        return -1;
    }

    Const* c = (Const*)parse->limitOffset;
    if (c->consttype != INT8OID && c->consttype != INT4OID) {
        return -1;
    }
    if (c->constisnull) {
        return 0;
    }

    return (c->consttype == INT8OID) ? DatumGetInt64(c->constvalue) : (int64)DatumGetInt32(c->constvalue);
}

/*
 * Compute the minimum rows the lower level must output to satisfy
 * the current query block's LIMIT/OFFSET. Also checks LIMIT/OFFSET
 * are positive constants and their sum does not overflow.
 */
static bool eval_top_level_demand(Query* parse, int64* demand)
{
    int64 lim = 0;
    int64 offset = 0;

    if (parse == NULL || demand == NULL) {
        return false;
    }

    lim = eval_top_level_limit(parse);
    if (lim <= 0) {
        return false;
    }

    offset = eval_top_level_offset(parse);
    if (offset < 0 || offset > PG_INT64_MAX - lim) {
        return false;
    }

    *demand = lim + offset;
    return true;
}

/*
 * Check whether the current query block qualifies for UNION ALL fast-start
 * reorder, and return LIMIT + OFFSET. Outer PERCENT or ORDER BY blocks
 * early row production, so skip directly. WITH TIES without ORDER BY is
 * treated as a regular Limit by the plan.
 */
bool union_all_faststart_applicable_for_parse(Query* parse, int64* demand)
{
    if (parse == NULL) {
        return false;
    }

    /* Outer ORDER BY or PERCENT applies to the whole UNION result; cannot reorder input branches. */
    if (parse->sortClause != NIL || parse->limitIsPercent) {
        return false;
    }

    /*
     * Upper operations in the current query block change UNION input demand;
     * same operations inside sub-branches are not affected here, their cost
     * is already in the branch plan. Window functions consume the full UNION
     * input, so no fast-start gain. No rowMarks check is needed here: any
     * query containing UNION is rejected by parse analysis when FOR
     * UPDATE/SHARE is present, so rowMarks is always NIL on reachable paths.
     * Note: Query.hasTargetSRFs is only set by check_srf_call_placement when
     * enable_expr_fusion is on, so default off sessions always see false.
     * Must use expression_returns_set directly, consistent with the kernel's
     * query_check_srf (nodeFuncs.cpp).
     */
    if (parse->hasAggs || parse->groupClause != NIL || parse->groupingSets != NIL ||
        parse->havingQual != NULL || parse->distinctClause != NIL ||
        parse->hasWindowFuncs || parse->hasTargetSRFs ||
        expression_returns_set((Node*)parse->targetList)) {
        return false;
    }

    /* LIMIT is not a positive constant, OFFSET unparseable, or sum overflowed. */
    if (!eval_top_level_demand(parse, demand)) {
        return false;
    }

    return true;
}

/*
 * qsort comparator for Path startup cost ascending; equal costs are
 * equivalent, so return 0 and let qsort keep a stable order.
 * void* params are required by the fixed qsort comparator signature.
 */
static int CmpUnionAllStartupPath(const void* a, const void* b)
{
    Path* pa = (Path*)*(void* const*)a;
    Path* pb = (Path*)*(void* const*)b;
    Cost ca = pa->startup_cost;
    Cost cb = pb->startup_cost;
    if (ca < cb) {
        return -1;
    }
    if (ca > cb) {
        return 1;
    }
    return 0;
}

/*
 * qsort comparator for Plan startup cost ascending; equal costs are
 * equivalent, so return 0 and let qsort keep a stable order.
 * void* params are required by the fixed qsort comparator signature.
 * Path and Plan have startup_cost at different offsets, so two comparators.
 */
static int CmpUnionAllStartupPlan(const void* a, const void* b)
{
    Plan* pa = (Plan*)*(void* const*)a;
    Plan* pb = (Plan*)*(void* const*)b;
    Cost ca = pa->startup_cost;
    Cost cb = pb->startup_cost;
    if (ca < cb) {
        return -1;
    }
    if (ca > cb) {
        return 1;
    }
    return 0;
}

/* Estimate cost when a branch only needs to produce the first needrows rows. */
static Cost estimate_partial_branch_cost(Cost startup_cost, Cost total_cost, double rows, double needrows)
{
    /* rows <= 0 means no rows to take; needrows >= rows means full scan. */
    if (rows <= 0 || needrows >= rows) {
        return total_cost;
    }
    return startup_cost + (total_cost - startup_cost) * ((double)needrows / rows);
}

/* Estimate cost when a Path branch only needs to produce the first needrows rows. */
static Cost estimate_partial_path_cost(Path* path, double needrows)
{
    return estimate_partial_branch_cost(path->startup_cost, path->total_cost, path->rows, needrows);
}

/* Compute cost when Path branches run in first -> second order to satisfy needrows. */
static Cost estimate_pair_path_order_cost(Path* first, Path* second, double needrows)
{
    if (needrows <= first->rows) {
        return estimate_partial_path_cost(first, needrows);
    }

    return first->total_cost + estimate_partial_path_cost(second, needrows - first->rows);
}

/*
 * Compare two Path branch execution orders and write the lower-cost one back.
 */
static void reorder_two_path_orders(List* pathlist, double needrows)
{
    ListCell* first_cell = list_head(pathlist);
    ListCell* second_cell = first_cell->next;
    Path* first = (Path*)lfirst(first_cell);
    Path* second = (Path*)lfirst(second_cell);
    Cost cost12 = estimate_pair_path_order_cost(first, second, needrows);
    Cost cost21 = estimate_pair_path_order_cost(second, first, needrows);
    if (cost21 < cost12) {
        void* tmp = lfirst(first_cell);
        lfirst(first_cell) = lfirst(second_cell);
        lfirst(second_cell) = tmp;
    }
}

/* Compute cost when Path branches run in first -> second -> third order to satisfy needrows. */
static Cost estimate_three_path_order_cost(Path* first, Path* second, Path* third, double needrows)
{
    if (needrows <= first->rows) {
        return estimate_partial_path_cost(first, needrows);
    }
    if (needrows <= first->rows + second->rows) {
        return first->total_cost + estimate_partial_path_cost(second, needrows - first->rows);
    }
    return first->total_cost + second->total_cost +
        estimate_partial_path_cost(third, needrows - first->rows - second->rows);
}

/*
 * Compare candidate orders of three Path branches and write the lowest-cost one back.
 * When the first branch already satisfies needrows, skip equivalent suffix orders
 * and sort the rest by startup_cost as fallback for underestimated row counts.
 */
static void reorder_three_path_orders(List* pathlist, double needrows)
{
    static const int orders[6][3] = {{0, 1, 2}, {0, 2, 1}, {1, 0, 2},
        {1, 2, 0}, {2, 0, 1}, {2, 1, 0}};
    ListCell* cells[3] = {
        list_head(pathlist), list_head(pathlist)->next, list_head(pathlist)->next->next};
    Path* paths[3] = {(Path*)lfirst(cells[0]), (Path*)lfirst(cells[1]), (Path*)lfirst(cells[2])};
    Cost best_cost = estimate_three_path_order_cost(paths[0], paths[1], paths[2], needrows);
    int bestOrder = 0;

    /* 3 branches have 6 candidate orders. */
    for (int i = 1; i < 6; i++) {
        /*
         * When the first path already satisfies needrows, the two orders
         * with the same first path have the same partial cost.  Skip the
         * duplicate suffix calculation; the unused paths are sorted below
         * as a fallback for underestimated row counts.
         */
        if (orders[i][0] == orders[i - 1][0] &&
            needrows <= paths[orders[i][0]]->rows) {
            continue;
        }
        Cost cost = estimate_three_path_order_cost(paths[orders[i][0]], paths[orders[i][1]], paths[orders[i][2]],
            needrows);
        if (cost < best_cost) {
            best_cost = cost;
            bestOrder = i;
        }
    }

    Path* ordered[3] = {paths[orders[bestOrder][0]], paths[orders[bestOrder][1]],
        paths[orders[bestOrder][2]]};
    if (needrows <= ordered[0]->rows &&
        /* Compare suffix slots 1 and 2 to decide whether to swap. */
        CmpUnionAllStartupPath(&ordered[1], &ordered[2]) > 0) {
        /* Swap suffix slots 1 and 2 so the cheaper branch comes first. */
        Path* tmp = ordered[1];
        /* Move the branch in slot 2 into slot 1. */
        ordered[1] = ordered[2];
        /* Place the saved branch into slot 2. */
        ordered[2] = tmp;
    }

    /* Write the first of 3 branches back to list slot 0. */
    lfirst(cells[0]) = ordered[0];
    /* Write the second of 3 branches back to list slot 1. */
    lfirst(cells[1]) = ordered[1];
    /* Write the third of 3 branches back to list slot 2. */
    lfirst(cells[2]) = ordered[2];
}

/* Compute cost when four branches run in the specified order to satisfy needrows. */
static Cost estimate_four_order_cost(const double rows[4], const Cost startup_cost[4], const Cost total_cost[4],
    const int order[4], double needrows)
{
    int first = order[0];
    int second = order[1];
    int third = order[2];
    int fourth = order[3];

    if (needrows <= rows[first]) {
        return estimate_partial_branch_cost(startup_cost[first], total_cost[first], rows[first], needrows);
    }
    if (needrows <= rows[first] + rows[second]) {
        return total_cost[first] +
            estimate_partial_branch_cost(startup_cost[second], total_cost[second], rows[second],
                needrows - rows[first]);
    }
    if (needrows <= rows[first] + rows[second] + rows[third]) {
        return total_cost[first] + total_cost[second] +
            estimate_partial_branch_cost(startup_cost[third], total_cost[third], rows[third],
                needrows - rows[first] - rows[second]);
    }
    return total_cost[first] + total_cost[second] + total_cost[third] +
        estimate_partial_branch_cost(startup_cost[fourth], total_cost[fourth], rows[fourth],
            needrows - rows[first] - rows[second] - rows[third]);
}

/* Choose the best candidate order for four branches, skipping equivalent suffixes covered by prefix. */
static int choose_four_order(const double rows[4], const Cost startup_cost[4], const Cost total_cost[4],
    double needrows)
{
    Cost best_cost = estimate_four_order_cost(rows, startup_cost, total_cost, UNION_ALL_FOUR_BRANCH_ORDERS[0],
        needrows);
    int bestOrder = 0;

    /* 4 branches have 24 candidate orders. */
    for (int i = 1; i < 24; i++) {
        const int* order = UNION_ALL_FOUR_BRANCH_ORDERS[i];
        const int* previous = UNION_ALL_FOUR_BRANCH_ORDERS[i - 1];
        double firstRows = rows[order[0]];

        if (order[0] == previous[0] && needrows <= firstRows) {
            continue;
        }
        if (order[0] == previous[0] && order[1] == previous[1] &&
            needrows <= (firstRows + rows[order[1]])) {
            continue;
        }

        Cost cost = estimate_four_order_cost(rows, startup_cost, total_cost, order, needrows);
        if (cost < best_cost) {
            best_cost = cost;
            bestOrder = i;
        }
    }
    return bestOrder;
}

/* Return the suffix start index after the prefix of four branches satisfies needrows. */
static int FourOrderSuffixStart(const double rows[4], const int order[4], double needrows)
{
    double prefixRows = rows[order[0]];
    int suffixStart = 1;

    if (needrows > prefixRows) {
        prefixRows += rows[order[1]];
        /* First 2 branches still insufficient; suffix starts at branch 3. */
        suffixStart = 2;
    }
    if (needrows > prefixRows) {
        /* First 3 branches still insufficient; suffix starts at branch 4. */
        suffixStart = 3;
    }
    return suffixStart;
}

/*
 * Compare candidate orders of four Path branches and write the lowest-cost one back.
 * When the prefix already satisfies needrows, skip equivalent suffix orders
 * and sort the unconsumed suffix by startup_cost as fallback.
 */
static void reorder_four_path_orders(List* pathlist, double needrows)
{
    ListCell* cells[4] = {list_head(pathlist), list_head(pathlist)->next,
        list_head(pathlist)->next->next, list_head(pathlist)->next->next->next};
    Path* paths[4] = {(Path*)lfirst(cells[0]), (Path*)lfirst(cells[1]),
        (Path*)lfirst(cells[2]), (Path*)lfirst(cells[3])};
    double rows[4];
    Cost startup_cost[4];
    Cost total_cost[4];
    /* Fill row-count and cost arrays for the 4 branches. */
    for (int i = 0; i < 4; i++) {
        rows[i] = paths[i]->rows;
        startup_cost[i] = paths[i]->startup_cost;
        total_cost[i] = paths[i]->total_cost;
    }
    int bestOrder = choose_four_order(rows, startup_cost, total_cost, needrows);

    Path* ordered[4] = {paths[UNION_ALL_FOUR_BRANCH_ORDERS[bestOrder][0]],
        paths[UNION_ALL_FOUR_BRANCH_ORDERS[bestOrder][1]],
        paths[UNION_ALL_FOUR_BRANCH_ORDERS[bestOrder][2]],
        paths[UNION_ALL_FOUR_BRANCH_ORDERS[bestOrder][3]]};
    int suffixStart = FourOrderSuffixStart(rows, UNION_ALL_FOUR_BRANCH_ORDERS[bestOrder], needrows);
    /* Sort unconsumed suffix only when at least 2 branches remain after slot 3. */
    if (suffixStart < 3) {
        /* Sort count = total branch count 4 minus suffix start. */
        qsort(&ordered[suffixStart], 4 - suffixStart, sizeof(Path*), CmpUnionAllStartupPath);
    }

    /* Write the first of 4 branches back to list slot 0. */
    lfirst(cells[0]) = ordered[0];
    /* Write the second of 4 branches back to list slot 1. */
    lfirst(cells[1]) = ordered[1];
    /* Write the third of 4 branches back to list slot 2. */
    lfirst(cells[2]) = ordered[2];
    /* Write the fourth of 4 branches back to list slot 3. */
    lfirst(cells[3]) = ordered[3];
}

/* Estimate cost when a Plan branch only needs to produce the first needrows rows. */
static Cost estimate_partial_plan_cost(Plan* plan, double needrows)
{
    return estimate_partial_branch_cost(plan->startup_cost, plan->total_cost, plan->plan_rows, needrows);
}

/* Compute cost when Plan branches run in first -> second order to satisfy needrows. */
static Cost estimate_pair_plan_order_cost(Plan* first, Plan* second, double needrows)
{
    if (needrows <= first->plan_rows) {
        return estimate_partial_plan_cost(first, needrows);
    }

    return first->total_cost + estimate_partial_plan_cost(second, needrows - first->plan_rows);
}

/*
 * Compare two Plan branch execution orders and write the lower-cost one back.
 */
static void reorder_two_plan_orders(List* planlist, double needrows)
{
    ListCell* first_cell = list_head(planlist);
    ListCell* second_cell = first_cell->next;
    Plan* first = (Plan*)lfirst(first_cell);
    Plan* second = (Plan*)lfirst(second_cell);
    Cost cost12 = estimate_pair_plan_order_cost(first, second, needrows);
    Cost cost21 = estimate_pair_plan_order_cost(second, first, needrows);
    if (cost21 < cost12) {
        void* tmp = lfirst(first_cell);
        lfirst(first_cell) = lfirst(second_cell);
        lfirst(second_cell) = tmp;
    }
}

/* Compute cost when Plan branches run in first -> second -> third order to satisfy needrows. */
static Cost estimate_three_plan_order_cost(Plan* first, Plan* second, Plan* third, double needrows)
{
    if (needrows <= first->plan_rows) {
        return estimate_partial_plan_cost(first, needrows);
    }
    if (needrows <= first->plan_rows + second->plan_rows) {
        return first->total_cost + estimate_partial_plan_cost(second, needrows - first->plan_rows);
    }
    return first->total_cost + second->total_cost +
        estimate_partial_plan_cost(third, needrows - first->plan_rows - second->plan_rows);
}

/*
 * Compare candidate orders of three Plan branches and write the lowest-cost one back.
 * When the first branch already satisfies needrows, skip equivalent suffix orders
 * and sort the rest by startup_cost as fallback for underestimated row counts.
 */
static void reorder_three_plan_orders(List* planlist, double needrows)
{
    static const int orders[6][3] = {{0, 1, 2}, {0, 2, 1}, {1, 0, 2},
        {1, 2, 0}, {2, 0, 1}, {2, 1, 0}};
    ListCell* cells[3] = {
        list_head(planlist), list_head(planlist)->next, list_head(planlist)->next->next};
    Plan* plans[3] = {(Plan*)lfirst(cells[0]), (Plan*)lfirst(cells[1]), (Plan*)lfirst(cells[2])};
    Cost best_cost = estimate_three_plan_order_cost(plans[0], plans[1], plans[2], needrows);
    int bestOrder = 0;

    /* 3 branches have 6 candidate orders. */
    for (int i = 1; i < 6; i++) {
        /*
         * When the first plan already satisfies needrows, the two orders
         * with the same first plan have the same partial cost.  Skip the
         * duplicate suffix calculation; the unused plans are sorted below
         * as a fallback for underestimated row counts.
         */
        if (orders[i][0] == orders[i - 1][0] &&
            needrows <= plans[orders[i][0]]->plan_rows) {
            continue;
        }
        Cost cost = estimate_three_plan_order_cost(plans[orders[i][0]], plans[orders[i][1]], plans[orders[i][2]],
            needrows);
        if (cost < best_cost) {
            best_cost = cost;
            bestOrder = i;
        }
    }

    Plan* ordered[3] = {plans[orders[bestOrder][0]], plans[orders[bestOrder][1]],
        plans[orders[bestOrder][2]]};
    if (needrows <= ordered[0]->plan_rows &&
        /* Compare suffix slots 1 and 2 to decide whether to swap. */
        CmpUnionAllStartupPlan(&ordered[1], &ordered[2]) > 0) {
        /* Swap suffix slots 1 and 2 so the cheaper branch comes first. */
        Plan* tmp = ordered[1];
        /* Move the branch in slot 2 into slot 1. */
        ordered[1] = ordered[2];
        /* Place the saved branch into slot 2. */
        ordered[2] = tmp;
    }

    /* Write the first of 3 branches back to list slot 0. */
    lfirst(cells[0]) = ordered[0];
    /* Write the second of 3 branches back to list slot 1. */
    lfirst(cells[1]) = ordered[1];
    /* Write the third of 3 branches back to list slot 2. */
    lfirst(cells[2]) = ordered[2];
}

/*
 * Compare candidate orders of four Plan branches and write the lowest-cost one back.
 * When the prefix already satisfies needrows, skip equivalent suffix orders
 * and sort the unconsumed suffix by startup_cost as fallback.
 */
static void reorder_four_plan_orders(List* planlist, double needrows)
{
    ListCell* cells[4] = {list_head(planlist), list_head(planlist)->next,
        list_head(planlist)->next->next, list_head(planlist)->next->next->next};
    Plan* plans[4] = {(Plan*)lfirst(cells[0]), (Plan*)lfirst(cells[1]),
        (Plan*)lfirst(cells[2]), (Plan*)lfirst(cells[3])};
    double rows[4];
    Cost startup_cost[4];
    Cost total_cost[4];
    /* Fill row-count and cost arrays for the 4 branches. */
    for (int i = 0; i < 4; i++) {
        rows[i] = plans[i]->plan_rows;
        startup_cost[i] = plans[i]->startup_cost;
        total_cost[i] = plans[i]->total_cost;
    }
    int bestOrder = choose_four_order(rows, startup_cost, total_cost, needrows);

    Plan* ordered[4] = {plans[UNION_ALL_FOUR_BRANCH_ORDERS[bestOrder][0]],
        plans[UNION_ALL_FOUR_BRANCH_ORDERS[bestOrder][1]],
        plans[UNION_ALL_FOUR_BRANCH_ORDERS[bestOrder][2]],
        plans[UNION_ALL_FOUR_BRANCH_ORDERS[bestOrder][3]]};
    int suffixStart = FourOrderSuffixStart(rows, UNION_ALL_FOUR_BRANCH_ORDERS[bestOrder], needrows);
    /* Sort unconsumed suffix only when at least 2 branches remain after slot 3. */
    if (suffixStart < 3) {
        /* Sort count = total branch count 4 minus suffix start. */
        qsort(&ordered[suffixStart], 4 - suffixStart, sizeof(Plan*), CmpUnionAllStartupPlan);
    }

    /* Write the first of 4 branches back to list slot 0. */
    lfirst(cells[0]) = ordered[0];
    /* Write the second of 4 branches back to list slot 1. */
    lfirst(cells[1]) = ordered[1];
    /* Write the third of 4 branches back to list slot 2. */
    lfirst(cells[2]) = ordered[2];
    /* Write the fourth of 4 branches back to list slot 3. */
    lfirst(cells[3]) = ordered[3];
}

/*
 * Path branch reorder entry:
 *   - Skip reorder if any branch root is PERCENT or WITH TIES Limit.
 *   - Do not expand CTE or other wrapper nodes; deep special Limit may participate.
 *   - 2-4 branches with total estimated rows covered by needrows: sort by startup_cost.
 *   - 2 branches: compare both orders precisely.
 *   - 3 branches: compare candidate orders, skip equivalent suffixes after first satisfies needrows.
 *   - 4 branches: compare 24 candidate orders, skip equivalent suffixes after prefix satisfies needrows.
 *   - 5+ branches: sort by startup_cost only when OFFSET + LIMIT <= GUC threshold.
 */
void reorder_union_all_paths_by_limit_cost(List* pathlist, int64 needrows)
{
    int branchCount = list_length(pathlist);
    double totalRows = 0.0;

    if (branchCount <= 1) {
        return;
    }

    /* >4 branches with needrows exceeding threshold: no reorder. */
    if (branchCount > 4 &&
        needrows > u_sess->attr.attr_sql.union_all_faststart_limit_threshold) {
        return;
    }

    /* Check only branch root PERCENT/WITH TIES, avoid deep traversal. */
    ListCell* lc = NULL;
    foreach (lc, pathlist) {
        Path* path = (Path*)lfirst(lc);
        /* Subquery's subplan: check root node only, do not expand its subtree. */
        if (path->parent != NULL && path->parent->rtekind == RTE_SUBQUERY &&
            branch_root_has_special_limit(path->parent->subplan)) {
            return;
        }
        /* 2-4 branches: accumulate total estimated rows for full-demand check. */
        if (branchCount <= 4) {
            totalRows += path->rows;
        }
    }

    /* All branches needed (branch count <= 4): sort by startup cost for faster first rows. */
    if (branchCount <= 4 && (double)needrows >= totalRows) {
        make_sorted_list(pathlist, CmpUnionAllStartupPath);
        return;
    }

    /* 2 branches: compare both orders precisely. */
    if (branchCount == 2) {
        reorder_two_path_orders(pathlist, (double)needrows);
        return;
    }

    /* 3 branches: enumerate 6 candidate orders. */
    if (branchCount == 3) {
        reorder_three_path_orders(pathlist, (double)needrows);
        return;
    }

    /* 4 branches: enumerate 24 candidate orders. */
    if (branchCount == 4) {
        reorder_four_path_orders(pathlist, (double)needrows);
        return;
    }

    make_sorted_list(pathlist, CmpUnionAllStartupPath);
}

/*
 * Plan branch reorder entry:
 *   - Skip reorder if any branch root is PERCENT or WITH TIES Limit.
 *   - Do not expand CTE or other wrapper nodes; deep special Limit may participate.
 *   - 2-4 branches with total estimated rows covered by needrows: sort by startup_cost.
 *   - 2 branches: compare both orders precisely.
 *   - 3 branches: compare candidate orders, skip equivalent suffixes after first satisfies needrows.
 *   - 4 branches: compare 24 candidate orders, skip equivalent suffixes after prefix satisfies needrows.
 *   - 5+ branches: sort by startup_cost only when OFFSET + LIMIT <= GUC threshold.
 */
void reorder_union_all_plans_by_limit_cost(List* planlist, int64 needrows)
{
    int branchCount = list_length(planlist);
    double totalRows = 0.0;

    if (branchCount <= 1) {
        return;
    }

    /* >4 branches with needrows exceeding threshold: no reorder. */
    if (branchCount > 4 &&
        needrows > u_sess->attr.attr_sql.union_all_faststart_limit_threshold) {
        return;
    }

    /* Check only branch root PERCENT/WITH TIES, avoid deep traversal. */
    ListCell* lc = NULL;
    foreach (lc, planlist) {
        Plan* plan = (Plan*)lfirst(lc);
        if (branch_root_has_special_limit(plan)) {
            return;
        }
        /* 2-4 branches: accumulate total estimated rows for full-demand check. */
        if (branchCount <= 4) {
            totalRows += plan->plan_rows;
        }
    }

    /* All branches needed (branch count <= 4): sort by startup cost for faster first rows. */
    if (branchCount <= 4 && (double)needrows >= totalRows) {
        make_sorted_list(planlist, CmpUnionAllStartupPlan);
        return;
    }

    /* 2 branches: compare both orders precisely. */
    if (branchCount == 2) {
        reorder_two_plan_orders(planlist, (double)needrows);
        return;
    }

    /* 3 branches: enumerate 6 candidate orders. */
    if (branchCount == 3) {
        reorder_three_plan_orders(planlist, (double)needrows);
        return;
    }

    /* 4 branches: enumerate 24 candidate orders. */
    if (branchCount == 4) {
        reorder_four_plan_orders(planlist, (double)needrows);
        return;
    }

    make_sorted_list(planlist, CmpUnionAllStartupPlan);
}
