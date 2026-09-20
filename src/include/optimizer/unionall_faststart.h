/* -------------------------------------------------------------------------
 *
 * unionall_faststart.h
 *	  Helper routines for the UNION ALL Fast-Start Reorder rewrite_rule.
 *
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2026. All rights reserved.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/include/optimizer/unionall_faststart.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef UNIONALL_FASTSTART_H
#define UNIONALL_FASTSTART_H

#include "nodes/pg_list.h"
#include "nodes/parsenodes.h"
#include "nodes/relation.h"

/*
 * This module implements UNION ALL Fast-Start reorder:
 * When the GUC is on and the top-level query has a positive constant
 * LIMIT/OFFSET demand with no ORDER BY/PERCENT or cardinality-changing
 * upper operations, UNION ALL leaf branches may be reordered by estimated
 * cost. Branches whose root is a PERCENT or WITH TIES Limit are not
 * reordered; deep nodes are not checked.
 * Three interception points share this module:
 *   - allpaths.cpp set_append_rel_pathlist: simple UNION ALL flattened to appendrel;
 *   - allpaths.cpp set_subquery_pathlist: UNION ALL subquery planned as a whole
 *     (not flattened, e.g. branches need type coercion); reorders the Append
 *     inside the finished subplan using the outer query's LIMIT demand;
 *   - prepunion.cpp generate_union_plan: nested or mixed setops UNION ALL.
 */

/*
 * union_all_faststart_applicable_for_parse
 *   Decide whether the given top-level Query qualifies for fast-start
 *   reorder of its UNION ALL leaves.
 *
 *   Requirements (all must hold):
 *     1. The top-level LIMIT is positive and LIMIT + OFFSET is positive.
 *     2. The top-level has no ORDER BY (parse->sortClause == NIL).
 *     3. The top-level LIMIT is not PERCENT.
 *     4. The top-level has no cardinality-changing upper operation that
 *        invalidates direct LIMIT demand. WITH TIES without ORDER BY is
 *        treated as a regular LIMIT by the generated plan.
 *
 *   When return value is true, *demand receives LIMIT + OFFSET.
 */
extern bool union_all_faststart_applicable_for_parse(Query* parse, int64* demand);

/*
 * reorder_union_all_paths_by_limit_cost
 *   In-place reorder of a list of Path nodes.
 *   - No-op if any branch root is a PERCENT or WITH TIES Limit.
 *   - 2 to 4 branches with all estimated rows needed: sort by startup cost.
 *   - 2 branches: compare both orders precisely with LIMIT/OFFSET demand.
 *   - 3 branches: enumerate all 6 orders and compare estimated costs.
 *   - 4 branches: enumerate all 24 orders and compare estimated costs.
 *   - More than 4 branches: sort by startup cost only within the GUC threshold.
 *   No-op for lists of length <= 1.
 */
extern void reorder_union_all_paths_by_limit_cost(List* pathlist, int64 needrows);

/*
 * reorder_union_all_plans_by_limit_cost
 *   In-place reorder of a list of Plan nodes.
 *   - No-op if any branch root is a PERCENT or WITH TIES Limit.
 *   - 2 to 4 branches with all estimated rows needed: sort by startup cost.
 *   - 2 branches: compare both orders precisely with LIMIT/OFFSET demand.
 *   - 3 branches: enumerate all 6 orders and compare estimated costs.
 *   - 4 branches: enumerate all 24 orders and compare estimated costs.
 *   - More than 4 branches: sort by startup cost only within the GUC threshold.
 *   No-op for lists of length <= 1.
 */
extern void reorder_union_all_plans_by_limit_cost(List* planlist, int64 needrows);

#endif /* UNIONALL_FASTSTART_H */
