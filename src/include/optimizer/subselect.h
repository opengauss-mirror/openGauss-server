/* -------------------------------------------------------------------------
 *
 * subselect.h
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/optimizer/subselect.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef SUBSELECT_H
#define SUBSELECT_H

#include "nodes/plannodes.h"
#include "nodes/relation.h"

#define SUBLINK_COUNTER 100

typedef struct
{
    List *aggFuncsList;
} fetch_agg_funcs_context;

extern void SS_process_ctes(PlannerInfo* root);
extern JoinExpr* convert_ANY_sublink_to_join(
    PlannerInfo* root, SubLink* sublink, bool under_not, Relids available_rels);
extern JoinExpr* convert_EXISTS_sublink_to_join(
    PlannerInfo* root, SubLink* sublink, bool under_not, Relids available_rels);
extern Node* convert_EXPR_sublink_to_join(PlannerInfo* root, Node** jtlink1, Node* inout_quals, SubLink* sublink,
    Relids *available_rels, Node* all_quals, const char *refname = NULL);

extern void convert_OREXISTS_to_join(
    PlannerInfo* root, BoolExpr* or_clause, SubLink* exists_sublink, Node** jtlink1, Relids available_rels1, bool isnull);
extern void convert_ORANY_to_join(
    PlannerInfo* root, BoolExpr* or_clause, SubLink* any_sublink, Node** jtlink1, Relids available_rels);

extern Node* convert_OREXPR_to_join(PlannerInfo* root, BoolExpr* or_clause, OpExpr* op_expr, SubLink* expr_sublink,
    Node** jtlink1, Relids *available_rels);
extern Node *generate_filter_on_opexpr_sublink(PlannerInfo *root, int rtIndex, Node *targetExpr, Query *subQuery);

extern void convert_ORCLAUSE_to_join(PlannerInfo* root, BoolExpr* or_clause, Node** jtlink1, Relids *available_rels1);
extern Node* SS_replace_correlation_vars(PlannerInfo* root, Node* expr);
extern Node* SS_process_sublinks(PlannerInfo* root, Node* expr, bool isQual);
extern void SS_finalize_plan(PlannerInfo* root, Plan* plan, bool attach_initplans);
extern Param* SS_make_initplan_from_plan(
    PlannerInfo* root, Plan* plan, Oid resulttype, int32 resulttypmod, Oid resultcollation);
extern Param* assign_nestloop_param_var(PlannerInfo* root, Var* var);
extern Param* assign_nestloop_param_placeholdervar(PlannerInfo* root, PlaceHolderVar* phv);
extern int assignPartIteratorParam(PlannerInfo* root);

extern int SS_assign_special_param(PlannerInfo* root);
extern bool check_var_nonnullable(Query* query, Node* node);

#ifdef STREAMPLAN
extern void convert_multi_count_distinct(PlannerInfo* root);
#endif
extern List* pull_sublink(Node* node, int flag, bool is_name, bool recurse = false);
extern List* pull_opExpr(Node* node);
extern List* pull_aggref(Node* node);

extern void push_down_one_query(PlannerInfo* root, Query** subquery);
extern void pull_up_sort_limit_clause(Query* query, Query* subquery, bool set_refs);
extern bool safe_pullup_op_expr_sublink(OpExpr* expr);
extern bool safe_apply_winmagic(PlannerInfo* root, Node* sublink_qual, SubLink* sublink, Relids* available_rels);
extern Node* convert_expr_sublink_with_winmagic(PlannerInfo* root, Node** jtlink1, Node* sublink_qual,
                                                SubLink* sublink, Relids* available_rels);
#endif /* SUBSELECT_H */
