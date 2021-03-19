/* -------------------------------------------------------------------------
 *
 * tlist.h
 *	  prototypes for tlist.c.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/optimizer/tlist.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef TLIST_H
#define TLIST_H

#include "optimizer/var.h"

extern TargetEntry* tlist_member(Node* node, List* targetlist);
extern TargetEntry* tlist_member_ignore_relabel(Node* node, List* targetlist);
#ifdef STREAMPLAN
extern TargetEntry* tlist_member_except_aggref(
    Node* node, List* targetlist, bool* nested_agg, bool* nested_relabeltype);
#endif

extern List* flatten_tlist(List* tlist, PVCAggregateBehavior aggbehavior, PVCPlaceHolderBehavior phbehavior);
extern List* add_to_flat_tlist(List* tlist, List* exprs);

extern List* get_tlist_exprs(List* tlist, bool includeJunk);
extern bool tlist_same_datatypes(List* tlist, List* colTypes, bool junkOK);
extern bool tlist_same_collations(List* tlist, List* colCollations, bool junkOK);

extern TargetEntry* get_sortgroupref_tle(Index sortref, List* targetList, bool report_error = true);
extern TargetEntry* get_sortgroupclause_tle(SortGroupClause* sgClause, List* targetList, bool report_error = true);
extern Node* get_sortgroupclause_expr(SortGroupClause* sgClause, List* targetList);
extern List* get_sortgrouplist_exprs(List* sgClauses, List* targetList);

extern SortGroupClause* get_sortgroupref_clause(Index sortref, List* clauses);
extern Oid* extract_grouping_ops(List* groupClause);
extern AttrNumber* extract_grouping_cols(List* groupClause, List* tlist);
extern bool grouping_is_sortable(List* groupClause);
extern bool grouping_is_hashable(List* groupClause);
extern bool grouping_is_distributable(List* groupClause, List* targetlist);
extern int get_grouping_column_index(Query* parse, TargetEntry* tle, List* groupClause);
extern List* make_agg_var_list(PlannerInfo* root, List* tlist, List** duplicate_tlist);
extern void get_tlist_group_vars_split(Query* parse, List* tlist, List** group_cols, List** non_group_cols);
extern List* get_dependency_var_list(Query* parse, List* group_cols, List* non_group_vars);
extern bool var_from_dependency_rel(Query* parse, Var* var, List* dep_oids);
extern bool var_from_sublink_pulluped(Query *parse, Var *var);
#endif /* TLIST_H */
