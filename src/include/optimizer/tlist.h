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
extern bool tlist_same_exprs(List *tlist1, List *tlist2);

extern TargetEntry* get_sortgroupref_tle(Index sortref, List* targetList, bool report_error = true);
extern TargetEntry* get_sortgroupclause_tle(SortGroupClause* sgClause, List* targetList, bool report_error = true);
extern Node* get_sortgroupclause_expr(SortGroupClause* sgClause, List* targetList);
extern List* get_sortgrouplist_exprs(List* sgClauses, List* targetList);

extern SortGroupClause* get_sortgroupref_clause(Index sortref, List* clauses);
extern SortGroupClause *get_sortgroupref_clause_noerr(Index sortref, List *clauses);

extern Oid* extract_grouping_ops(List* groupClause);
extern Oid* extract_grouping_collations(List* groupClause, List* tlist);
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
extern bool var_from_subquery_pulluped(Query *parse, Var *var);

extern void apply_tlist_labeling(List *dest_tlist, List *src_tlist);
extern PathTarget *make_pathtarget_from_tlist(List *tlist);
extern List *make_tlist_from_pathtarget(PathTarget *target);
extern PathTarget *create_empty_pathtarget(void);
extern void add_column_to_pathtarget(PathTarget *target, Expr *expr, Index sortgroupref);
extern void add_new_column_to_pathtarget(PathTarget *target, Expr *expr);
extern void add_new_columns_to_pathtarget(PathTarget *target, List *exprs);
extern bool split_pathtarget_at_srfs(PlannerInfo *root, PathTarget *target, PathTarget *input_target,
                                    List **targets, List **targets_contain_srfs);
                                    
/* Convenience macro to get a PathTarget with valid cost/width fields */
#define create_pathtarget(root, tlist) \
    set_pathtarget_cost_width(root, make_pathtarget_from_tlist(tlist))

#ifdef USE_SPQ
extern List* tlist_members(Node* node, List* targetlist);
extern void get_sortgroupclauses_tles(List *clauses, List *targetList, List **tles, List **sortops, List **eqops);
extern Index maxSortGroupRef(List *targetlist, bool include_orderedagg);
extern TargetEntry* get_sortgroupref_tle_spq(Index sortref, List* targetList, bool report_error = true);
#endif

#endif /* TLIST_H */
