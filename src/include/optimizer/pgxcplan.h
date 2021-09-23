/* -------------------------------------------------------------------------
 *
 * pgxcplan.h
 *		openGauss specific planner interfaces and structures.
 *
 *
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/optimizer/pgxcplan.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef PGXCPLANNER_H
#define PGXCPLANNER_H

#include "fmgr.h"
#include "lib/stringinfo.h"
#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "nodes/primnodes.h"
#include "pgxc/locator.h"
#include "tcop/dest.h"
#include "nodes/relation.h"
#include "optimizer/pgxc_plan_remote.h"

typedef struct VecRemoteQuery : public RemoteQuery {
} VecRemoteQuery;

extern PlannedStmt* pgxc_planner(Query* query, int cursorOptions, ParamListInfo boundParams);
extern List* AddRemoteQueryNode(List* stmts, const char* queryString, RemoteQueryExecType remoteExecType, bool is_temp);
extern bool pgxc_query_contains_temp_tables(List* queries);
extern bool pgxc_query_contains_utility(List* queries);
extern void pgxc_rqplan_adjust_tlist(PlannerInfo* root, RemoteQuery* rqplan, bool gensql = true);

extern Plan* pgxc_make_modifytable(PlannerInfo* root, Plan* topplan);
extern RangeTblEntry* make_dummy_remote_rte(char* relname, Alias* alias);
extern List* pgxc_get_dist_var(Index varno, RangeTblEntry* rte, List* tlist);
extern CombineType get_plan_combine_type(CmdType commandType, char baselocatortype);
extern ExecNodes* pgxc_is_join_shippable(ExecNodes* inner_en, ExecNodes* outer_en, bool inner_unshippable_tlist,
    bool outer_shippable_tlist, JoinType jointype, Node* join_quals);
extern bool contains_temp_tables(List* rtable);
extern List* process_agg_targetlist(PlannerInfo* root, List** local_tlist);
extern bool containing_ordinary_table(Node* node);
extern Param* pgxc_make_param(int param_num, Oid param_type);
extern Query* pgxc_build_shippable_query_recurse(
    PlannerInfo* root, RemoteQueryPath* rqpath, List** unshippable_quals, List** rep_tlist);

extern void preprocess_const_params(PlannerInfo* root, Node* jtnode);
extern bool contains_column_tables(List* rtable);

#endif /* PGXCPLANNER_H */

