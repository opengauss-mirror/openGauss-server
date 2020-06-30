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
 * ---------------------------------------------------------------------------------------
 * 
 * planner.h
 *     the head of tsdb/planner.cpp
 * 
 * 
 * IDENTIFICATION
 *        src/include/tsdb/planner.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef TIMESERIES_PLANNER_H
#define TIMESERIES_PLANNER_H

#include <optimizer/planner.h>
#include <nodes/primnodes.h>

typedef bool (*walker)();
typedef struct FillWalkerContext {
    FuncExpr* call;
    int fill_func_calls;
    int fill_last_func_calls;
    int column_calls;
} FillWalkerContext;

extern bool fill_function_call_walker(FuncExpr* node, FillWalkerContext* context);
extern Plan* tsdb_modifier(PlannerInfo* root, List* tlist, Plan* custom_plan);
extern List* tsdb_subtlist_modifier(List* tlist);
extern void tsdb_set_rel_pathlist_hook(PlannerInfo*, RelOptInfo*, Index, RangeTblEntry*);

#endif /* TIMESERIES_PLANNER_H */
