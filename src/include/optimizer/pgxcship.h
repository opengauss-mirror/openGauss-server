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
#include "nodes/nodeFuncs.h"
#include "pgxc/locator.h"
#include "utils/plpgsql.h"

/* Insert select shippability flags */
#define INSEL_SHIPPABLE_NCOL 0
#define INSEL_SHIPPABLE_DCOL 1
#define INSEL_UNSHIPPABLE_COL 2

/* Type casts that doesn't effect the hash value */
#define FLOAT4TOFLOAT8OID 311
#define FLOAT8TOFLOAT4OID 312
#define INT2TOINT4OID 313
#define INT4TOINT2OID 314
#define INT8TOINT4OID 480
#define INT4TOINT8OID 481
#define INT8TOINT2OID 714
#define INT2TOINT8OID 754
#define INT1TOINT2OID 5523
#define INT2TOINT1OID 5524
#define INT1TOINT4OID 5525
#define INT4TOINT1OID 5526
#define INT1TOINT8OID 5527
#define INT8TOINT1OID 5528

typedef struct {
    bool is_randomfunc_shippable;
    bool is_nextval_shippable;
    bool allow_func_in_targetlist;
    bool is_ecfunc_shippable;
    bool disallow_volatile_func_shippable;
    bool disable_dn_gather;
    List* query_list;
    uint8 query_count;
    bool  query_shippable;    /* backward compatible */
    bool  current_shippable;  /* current query can push */
    bool  global_shippable;   /* the whole query can push */
} shipping_context;

typedef struct {
    List* funcids;    /* List of func Oid */
    List* func_exprs; /* List of FuncExpr we have found */

    /* true means find all func specified by funcids, false means once find any func then return */
    bool find_all;
} contain_func_context;

typedef struct {
    List* subquery_exprs;
} contain_subquery_context;

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
extern bool is_avg_func(Oid funcid);
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
extern bool pgxc_check_dynamic_param(List* dynamicExpr, ParamListInfo params);

/* check junk tlist */
extern bool check_replicated_junktlist(Query* subquery);

/* For online expansion, we need some user defined function to be shippable to DN  */
extern bool redis_func_shippable(Oid funcid);
#ifdef ENABLE_MULTIPLE_NODES
extern bool vector_search_func_shippable(Oid funcid);
#endif
/* For trigger shippable check */
extern bool pgxc_is_query_shippable_in_trigger(PLpgSQL_expr* expr);
extern bool pgxc_find_statement_trigger(Oid relid, int commandType);

/* For params, subquery walker */
extern List* contains_subquery(Node* node, contain_subquery_context* context);
extern contain_subquery_context init_contain_subquery_context();

/* Get var from nodes */
extern Var* get_var_from_node(Node* node, bool (*func)(Oid));
extern bool func_oid_check_reject(Oid oid);
extern bool func_oid_check_restricted(Oid oid);
extern bool func_oid_check_pass(Oid oid);

extern ExecNodes* GetExecNodesByNodeName(char* node_name);
extern bool PgxcIsSingleShardQueryEnabled(Query* query);
#endif
