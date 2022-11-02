/* ---------------------------------------------------------------------------------------
 * 
 * stream_check.h
 *        prototypes for stream plan for checking.
 * 
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *        src/include/optimizer/stream_check.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef STREAM_CHECK_H
#define STREAM_CHECK_H

#include "catalog/pgxc_class.h"
#include "nodes/parsenodes.h"
#include "optimizer/pgxcplan.h"
#include "optimizer/pgxcship.h"
#include "optimizer/paths.h"
#include "optimizer/stream_cost.h"
#include "pgxc/nodemgr.h"
#include "pgxc/pgxc.h"

typedef struct underStreamContext {
    Index reslutIndex;
    bool underStream;
} underStreamContext;

extern Plan* mark_distribute_dml(
    PlannerInfo* root, Plan** sourceplan, ModifyTable* mt_plan, List** resultRelations, List* mergeActionList);
extern List* check_op_list_template(Plan* result_plan, List* (*check_eval)(Node*));
extern bool judge_lockrows_need_redistribute(
    PlannerInfo* root, Plan* subplan, Form_pgxc_class target_classForm, Index result_rte_idx);
extern bool judge_redistribute_setop_support(PlannerInfo* root, List* subplanlist, Bitmapset* redistributePlanSet);
extern void mark_query_canpush_flag(Node *query);
extern void mark_stream_unsupport();
extern List* check_random_expr(Plan* result_plan);
extern List* check_func_list(Plan* result_plan);
extern bool check_stream_support();
extern List* check_subplan_list(Plan* result_plan);
extern List* check_vartype_list(Plan* result_plan);
extern ExecNodes* get_plan_max_ExecNodes(Plan* lefttree, List* subplans);
extern List* get_max_nodeList(List** nodeList, Plan* result_plan);

#endif /* STREAM_CHECK_H */