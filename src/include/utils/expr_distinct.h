/* -------------------------------------------------------------------------
 *
 * expr_distinct.h
 *    functions for get number of distinct of expressions.
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2021. All rights reserved.
 *
 * src/include/utils/expr_distinct.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef EXPR_DISTINCT_H
#define EXPR_DISTINCT_H

#include "postgres.h"

#include "nodes/relation.h"
#include "utils/selfuncs.h"
#include "utils/be_module.h"

extern double GetExprNumDistinctRouter(VariableStatData *varData, bool needAdjust, STATS_EST_TYPE eType,
    bool isJoinVar);

#endif /* EXPR_DISTINCT_H */
