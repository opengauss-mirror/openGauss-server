/* -------------------------------------------------------------------------
 *
 * pgxcship.h
 *		Functionalities for the evaluation of expression shippability
 *		to remote nodes
 *
 *
 * Portions Copyright (c) 1996-2012 PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/optimizer/pgxcship.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef PGXCSHIP_H
#define PGXCSHIP_H

#include "nodes/parsenodes.h"
#include "nodes/relation.h"
#include "pgxc/locator.h"
#include "utils/plpgsql.h"

typedef struct {
    bool is_randomfunc_shippable;
    bool is_nextval_shippable;
    bool allow_func_in_targetlist;
    bool is_ecfunc_shippable;
    bool disallow_volatile_func_shippable;
    List* query_list;
    uint8 query_count;
} shipping_context;

typedef struct {
    List* funcids;    /* List of func Oid */
    List* func_exprs; /* List of FuncExpr we have found */

    /* true means find all func specified by funcids, false means once find any func then return */
    bool find_all;
} contain_func_context;

/* Determine if query is shippable */
extern ExecNodes* pgxc_is_query_shippable(
    Query* query, int query_level, bool light_proxy, bool* contain_column_store = NULL, bool* use_star_up = NULL);
/* Determine if an expression is shippable */
extern bool pgxc_is_expr_shippable(Expr* node, bool* has_aggs);
extern bool pgxc_is_funcRTE_shippable(Expr* node);
/* Determine if given function is shippable */
extern bool pgxc_is_func_shippable(Oid funcid, shipping_context* context = NULL);
/* Determine if given function is shippable and the args conctains ANY type */
extern bool pgxc_is_shippable_func_contain_any(Oid funcid);
extern bool pgxc_is_internal_agg_final_func(Oid funcid);
/* Check equijoin conditions on given relations */
extern List* pgxc_find_dist_equijoin_qual(List* dist_vars1, List* dist_vars2, Node* quals);
/* Merge given execution nodes based on join shippability conditions */
extern ExecNodes* pgxc_merge_exec_nodes(ExecNodes* en1, ExecNodes* en2, int join_type = -1);
/* Check if given Query includes distribution column */
extern bool pgxc_query_has_distcolgrouping(Query* query, ExecNodes* exec_nodes);
/* Check the shippability of an index */
extern bool pgxc_check_index_shippability(RelationLocInfo* relLocInfo, bool is_primary, bool is_unique,
    bool is_exclusion, List* indexAttrs, List* indexExprs);
/* Check the shippability of a parent-child constraint */
extern bool pgxc_check_fk_shippability(
    RelationLocInfo* parentLocInfo, RelationLocInfo* childLocInfo, List* parentRefs, List* childRefs);
extern bool pgxc_check_triggers_shippability(Oid relid, int commandType, bool* hasTrigger);
extern bool pgxc_find_nonshippable_row_trig(
    Relation rel, int16 tgtype_event, int16 tgtype_timing, bool ignore_timing, bool* hasTrigger = NULL);
extern bool pgxc_query_contains_foreign_table(List* rtable);
extern bool contain_unsupport_expression(Node* expr, void* context);
extern bool pgxc_check_dynamic_param(List* dynamicExpr, ParamListInfo params);

/* For online expansion, we need some user defined function to be shippable to DN  */
extern bool redis_func_shippable(Oid funcid);
extern bool vector_search_func_shippable(Oid funcid);

/* For trigger shippable check */
extern bool pgxc_is_query_shippable_in_trigger(PLpgSQL_expr* expr);
extern bool pgxc_find_statement_trigger(Oid relid, int commandType);
#endif
