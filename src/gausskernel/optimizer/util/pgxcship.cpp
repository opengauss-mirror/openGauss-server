/* -------------------------------------------------------------------------
 *
 * pgxcship.cpp
 *		Routines to evaluate expression shippability to remote nodes
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012, Postgres-XC Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/optimizer/util/pgxcship.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"
#include "access/transam.h"
#include "catalog/pg_class.h"
#include "catalog/pg_inherits_fn.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_proc.h"
#ifdef PGXC
#include "catalog/pg_aggregate.h"
#include "catalog/pg_trigger.h"
#endif
#include "catalog/pg_type.h"
#include "catalog/pgxc_node.h"
#include "commands/proclang.h"
#include "commands/trigger.h"
#include "foreign/foreign.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/relation.h"
#include "optimizer/clauses.h"
#include "optimizer/nodegroups.h"
#include "optimizer/pathnode.h"
#include "optimizer/pgxcplan.h"
#include "optimizer/pgxcship.h"
#include "optimizer/planmain.h"
#include "optimizer/prep.h"
#include "optimizer/tlist.h"
#include "parser/analyze.h"
#include "parser/parsetree.h"
#include "parser/parse_coerce.h"
#include "parser/parse_type.h"
#include "pgxc/pgxcnode.h"
#include "storage/proc.h"
#include "tcop/tcopprot.h"
#include "utils/hotkey.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#ifdef STREAMPLAN
#include "optimizer/streamplan.h"
#include "pgxc/pgxc.h"
#endif

#define XOR(A, B) (((A) && !(B)) || (!(A) && (B)))

typedef bool (*CheckColocateHook)(Query* query, RangeTblEntry* rte, void* plpgsql_func);

/*
 * Shippability_context
 * This context structure is used by the Fast Query Shipping walker, to gather
 * information during analysing query for Fast Query Shipping.
 */
typedef struct {
    bool sc_for_expr;                                 /* if false, the we are checking shippability
                                                       * of the Query or a function RangeTableEntry
                                                       * , otherwise, we are checking
                                                       * shippability of a stand-alone expression.
                                                       */
    bool sc_for_trigger;                              /* if true, we are checking shippability of the
                                                       * Query inside a trigger body.
                                                       */
    Bitmapset* sc_shippability;                       /* The conditions for (un)shippability of the
                                                       * query.
                                                       */
    Query* sc_query;                                  /* the query being analysed for FQS */
    int sc_query_level;                               /* level of the query */
    int sc_max_varlevelsup;                           /* maximum upper level referred to by any
                                                       * variable reference in the query. If this
                                                       * value is greater than 0, the query is not
                                                       * shippable, if shipped alone.
                                                       */
    ExecNodes* sc_exec_nodes;                         /* nodes where the query should be executed */
    ExecNodes* sc_subquery_en;                        /* ExecNodes produced by merging the ExecNodes
                                                       * for individual subqueries. This gets
                                                       * ultimately merged with sc_exec_nodes.
                                                       */
    bool sc_groupby_has_distcol;                      /* GROUP BY clause has distribution column */
    bool sc_light_proxy;                              /* light proxy case, avg is not shippable */
    void* sc_plpgsql_func;                            /* Ptr to PLpgSQL_function */
    CheckColocateHook sc_plpgsql_check_colocate_hook; /* Hook to check colocate in trigger */
    bool sc_contain_column_store;                     /* column table case, some expr is not shippable */
    bool sc_use_star_upper_level;                     /* need to use * for targetlist of upper level query */

    bool sc_disallow_volatile_func_shippability; /* If true disallow volatie func shippable */
    
    bool sc_inselect;
} Shippability_context;

/*
 * ShippabilityStat
 * List of reasons why a query/expression is not shippable to remote nodes.
 */
typedef enum {
    SS_UNSHIPPABLE_EXPR = 0, /* it has unshippable expression */
    SS_NEED_SINGLENODE,      /* Has expressions which can be evaluated when
                              * there is only a single node involved.
                              * Athought aggregates too fit in this class, we
                              * have a separate status to report aggregates,
                              * see below.
                              */
    SS_NEEDS_COORD,          /* the query needs Coordinator */
    SS_NO_NODES,             /* no suitable nodes can be found to ship
                              * the query
                              */
    SS_UNSUPPORTED_EXPR,     /* it has expressions currently unsupported
                              * by FQS, but such expressions might be
                              * supported by FQS in future
                              */
    SS_HAS_AGG_EXPR,         /* it has aggregate expressions */
    SS_UNSHIPPABLE_TYPE,     /* the type of expression is unshippable */
    SS_UNSHIPPABLE_TRIGGER,  /* the type of trigger is unshippable */
    SS_UNSHIPPABLE_FUNCTION, /* the type of function is unshippable */
    SS_NEED_NO_CSTORE,       /* it is unshippable if contains cstore tables */
    SS_UNSHIPPABLE_UDF,      /* this type of function can support partial shipping */
} ShippabilityStat;

/* Manipulation of shippability reason */
static bool pgxc_test_shippability_reason(Shippability_context* context, ShippabilityStat reason);
static void pgxc_set_shippability_reason(Shippability_context* context, ShippabilityStat reason);
static void pgxc_reset_shippability_reason(Shippability_context* context, ShippabilityStat reason);

/* Evaluation of shippability */
static bool pgxc_shippability_walker(Node* node, Shippability_context* sc_context);
static void pgxc_set_exprtype_shippability(Oid exprtype, Shippability_context* sc_context);
static void pgxc_set_insert_shippability(Query* query, Shippability_context* sc_context);

/* Fast-query shipping (FQS) functions */
static ExecNodes* pgxc_FQS_get_relation_nodes(RangeTblEntry* rte, Index varno, Query* query);
static ExecNodes* pgxc_FQS_find_datanodes(Shippability_context* sc_context);
static bool pgxc_query_needs_coord(Query* query);
static bool pgxc_query_contains_only_pg_catalog(List* rtable);
static bool pgxc_distinct_has_distcol(Query* query, ExecNodes* exec_nodes);
static ExecNodes* pgxc_FQS_find_datanodes_recurse(Node* node, Shippability_context* sc_context);
static ExecNodes* pgxc_FQS_datanodes_for_rtr(Index varno, Shippability_context* sc_context);
static void pgxc_replace_dist_vars_subquery(Query* query, ExecNodes* exec_nodes, Index varno);
static bool pgxc_is_trigger_shippable(int trig_idx, Relation rel = NULL);
static bool check_has_sameAlias(List* targetList);
static bool pgxc_check_shippability_in_trigger(Query* query, Shippability_context* sc_context);
static void check_insert_subquery_shippability(Query* query, Query *subquery, Shippability_context* sc_context);
static bool PgxcIsDistInfoSame(ExecNodes *innerEn, ExecNodes *outerEn);

/* Other functions */
static void make_params_transparent(Query* query, int query_level);
static bool has_consistent_execnodes(ExecNodes* exec_nodes_rte, ExecNodes* exec_nodes_qry, int* dcol_cnt);
static Expr* find_distcol_expr(void* query, Index varno, AttrNumber attrNum, Node* quals);

/*
 * A set of functions for get_var_from_node() argument
 * reject: return false all time (please set to default)
 * pass: return true all time (in some condition)
 * restricted: switch between set of oids and return true if found.
 */
Var* get_var_from_node(Node* node, bool (*func)(Oid) = func_oid_check_reject);
bool func_oid_check_reject(Oid oid)
{
    return false;
}

bool func_oid_check_restricted(Oid oid)
{
    switch (oid) {
        case INT1TOINT2OID:
        case INT1TOINT4OID:
        case INT1TOINT8OID:
        case INT2TOINT4OID:
        case INT4TOINT2OID:
        case INT2TOINT8OID:
        case INT8TOINT2OID:
        case INT4TOINT8OID:
        case INT8TOINT4OID:
        case FLOAT4TOFLOAT8OID:
        case FLOAT8TOFLOAT4OID:
            return true;
            break;    /* a candy for compiler */
        default:
            return false;
    }
    /* shouldn't get here, but to keep compiler happy */
    return false;
}

bool func_oid_check_pass(Oid oid)
{
    return true;
}

/*
 * Set the given reason in Shippability_context indicating why the query can not be
 * shipped directly to remote nodes.
 */
static void pgxc_set_shippability_reason(Shippability_context* context, ShippabilityStat reason)
{
    context->sc_shippability = bms_add_member(context->sc_shippability, reason);
}

/*
 * pgxc_reset_shippability_reason
 * Reset reason why the query cannot be shipped to remote nodes
 */
static void pgxc_reset_shippability_reason(Shippability_context* context, ShippabilityStat reason)
{
    context->sc_shippability = bms_del_member(context->sc_shippability, reason);
    return;
}

/*
 * See if a given reason is why the query can not be shipped directly
 * to the remote nodes.
 */
static bool pgxc_test_shippability_reason(Shippability_context* context, ShippabilityStat reason)
{
    return bms_is_member(reason, context->sc_shippability);
}

/*
 * pgxc_set_exprtype_shippability
 * Set the expression type shippability. For now composite types
 * derived from view definitions are not shippable.
 */
static void pgxc_set_exprtype_shippability(Oid exprtype, Shippability_context* sc_context)
{
    char typerelkind = '\0';
    Oid relid;

    relid = typeidTypeRelid(exprtype);
    if (relid != InvalidOid)
        typerelkind = get_rel_relkind(relid);

    if (RELKIND_IS_SEQUENCE(typerelkind) ||
        typerelkind == RELKIND_VIEW || 
        typerelkind == RELKIND_CONTQUERY)
        pgxc_set_shippability_reason(sc_context, SS_UNSHIPPABLE_TYPE);
}

/*
 * pgxc_FQS_datanodes_for_rtr
 * For a given RangeTblRef find the datanodes where corresponding data is
 * located.
 */
static ExecNodes* pgxc_FQS_datanodes_for_rtr(Index varno, Shippability_context* sc_context)
{
    Query* query = sc_context->sc_query;
    RangeTblEntry* rte = rt_fetch(varno, query->rtable);
    switch (rte->rtekind) {
        case RTE_RELATION: {
            /* For anything, other than a table, we can't find the datanodes */
            bool flag = rte->relkind != RELKIND_RELATION && rte->relkind != RELKIND_FOREIGN_TABLE &&
                        rte->relkind != RELKIND_MATVIEW && rte->relkind != RELKIND_STREAM;
            if (flag)
                return NULL;
            /*
             * In case of inheritance, child tables can have completely different
             * Datanode distribution than parent. To handle inheritance we need
             * to merge the Datanodes of the children table as well. The inheritance
             * is resolved during planning, so we may not have the RTEs of the
             * children here. Also, the exact method of merging Datanodes of the
             * children is not known yet. So, when inheritance is requested, query
             * can not be shipped.
             * See prologue of has_subclass, we might miss on the optimization
             * because has_subclass can return true even if there aren't any
             * subclasses, but it's ok.
             */
            if (rte->inh && has_subclass(rte->relid))
                return NULL;

            return pgxc_FQS_get_relation_nodes(rte, varno, query);
        } break;

        case RTE_SUBQUERY: {
            ExecNodes* exec_nodes = NULL;
            bool contain_column_store = sc_context->sc_contain_column_store;
            bool use_star_targets = false;

            /*
             * Subquery in RangeTbleEntry is not scanned while scanning the
             * parent query, since we don't scan the parent query's rtable.
             */
            exec_nodes = (sc_context->sc_inselect) ? sc_context->sc_exec_nodes :
                pgxc_is_query_shippable(rte->subquery,
                    sc_context->sc_query_level + 1,
                    sc_context->sc_light_proxy,
                    &contain_column_store,
                    &use_star_targets);

            /* once contain cstore, always constain */
            if (contain_column_store)
                sc_context->sc_contain_column_store = contain_column_store;
            /* once need to use star, always use */
            if (use_star_targets)
                query->use_star_targets = use_star_targets;

            /*
             * If the query result is distributed by HASH or MODULO, we need to
             * map the Vars on which its distributed to the columns in the
             * result.
             */
            if (exec_nodes && IsExecNodesDistributedByValue(exec_nodes))
                pgxc_replace_dist_vars_subquery(rte->subquery, exec_nodes, varno);
            return exec_nodes;
        } break;
        case RTE_VALUES: {
            if (query->commandType != CMD_INSERT)
                return NULL;

            ExecNodes* exec_nodes = NULL;
            /*
             * In case of Values, we only optimize the INSERT case when the
             * result relation is located on a single node
             */
            exec_nodes = pgxc_FQS_datanodes_for_rtr((Index)query->resultRelation, sc_context);
            if (exec_nodes != NULL && exec_nodes->nodeList != NIL &&
                list_length(exec_nodes->nodeList) == 1)
                return exec_nodes;
            else
                return NULL;
        } break;
        /* For any other type of RTE, we return NULL for now */
        case RTE_JOIN:
        case RTE_CTE:
        case RTE_FUNCTION:
        default:
            return NULL;
    }
}

/*
 * match_equivclause_recurse
 * Iterating relations list recursively. Returns a list of matched Vars.
 * Pushing the remaining and the list of matched Vars to the next iteration. 
 * Keep iterating until no new matching Vars found in the list.
 * @param Var/RelabelType, Const/Param pair and iterating list
 * @return rewritten list
 */
static List* match_equivclause_recurse(Node* var, Node* con, List* quals_v)
{
    ListCell* qcell = NULL;
    List* ret_quals = NIL;
    List* tmp_quals = NIL;
    Node* tmp_var = NULL;

    /* 
     * The input list quals_v contains all Var-Var relations
     * (or RelabelType variant depends on the input node *var type)
     * For each relation, matching the input var with both args of the relation.
     * If matched, then pair the other half with the input con and
     * make up all implied relations base on that.
     * To visualize this:
     *      var: A con: 2 from relation 'A = 2'
     * In this iteration, we update the quals_v:
     *      A = B and C = A and B = C
     * With this:
     *      2 = B and C = 2 and B = C (and B = C)
     * Then push the rest to the next iteration.
     */
    foreach(qcell, quals_v) {
        Expr* qual_expr = (Expr*)lfirst(qcell);
        OpExpr* op = (OpExpr*)qual_expr;

        Node* lvar = (Node*)linitial(op->args);
        Node* rvar = (Node*)lsecond(op->args);
        Node* tvar = NULL;  /* temporary placeholder */

        /* Skip if type doesn't match */
        if ((nodeTag(lvar) != nodeTag(var)) && (nodeTag(rvar) != nodeTag(var))) {
            tmp_quals = lappend(tmp_quals, op);
            continue;
        }

        /* Check if there are matching Vars */
        if (equal(lvar, var)) {
            tvar = rvar;
        } else if (equal(rvar, var)) {
            tvar = lvar;
        } else {   /* Push down to next iteration if nothing found */
            tmp_quals = lappend(tmp_quals, op);
        }

        if (tvar != NULL) {
            OpExpr* ret_op = (OpExpr*)copyObject(op);
            ret_op->args = list_make2((Node*)copyObject(tvar), (Node*)copyObject(con));
            ret_quals = lappend(ret_quals, ret_op);

            /* Update the tmp list if more than one Var being pushed down */
            if (tmp_var == NULL) {  
                tmp_var = tvar;
                continue;
            }

            /* Loop proof, detect convergence & drop duplicates */
            if (!equal(tmp_var, tvar)) {
                OpExpr* tmp_op = (OpExpr*)copyObject(op);
                tmp_op->args = list_make2(tmp_var, tvar);
                tmp_quals = lappend(tmp_quals, tmp_op);
                tmp_var = tvar;
            }
        }
    }

    if (tmp_var == NULL) {
        return list_concat(ret_quals, (List*)copyObject(tmp_quals));
    }

    /* Matching Vars found, iterate */
    ret_quals = list_concat(ret_quals, match_equivclause_recurse(tmp_var, con, tmp_quals));

    list_free_ext(tmp_quals);   /* Free list after copy */
    return ret_quals;
}


/*
 * match_equivclause
 * Matching the Vars and its target Const by traversing the Var-Const relations list.
 * @param list of con-con relation and list of var-var relation
 * @return combined rewritten list
 */
static List* match_equivclause(List* quals_c, List* quals_v)
{
    ListCell* qcell = NULL;
    List* ret_quals = NIL;
    Node* var = NULL;
    Node* con = NULL;

    /* 
     * The input list quals_c contains all Var-Const relations
     * (or Var-Param or both, depends on the query tree quals)
     * Carefully seperate the Const with Var and pass them to the recurse 
     * as parameters. Note that the query can have multiple set of relations,
     * so we need to make sure nothing get removed by accident in this process.
     */
    foreach(qcell, quals_c) {
        /* For consistency */
        Expr* qual_expr = (Expr*)lfirst(qcell);
        OpExpr* op = (OpExpr*)qual_expr;

        /* Var-Const relation */
        if ((IsA((Node*)linitial(op->args), Const)) || (IsA((Node*)linitial(op->args), Param))) {
            var = (Node*)lsecond(op->args);
            con = (Node*)linitial(op->args);
        } else if ((IsA((Node*)lsecond(op->args), Const)) || (IsA((Node*)lsecond(op->args), Param))) {
            var = (Node*)linitial(op->args);
            con = (Node*)lsecond(op->args);
        } else {
            return NIL; /* Breaking condition */
        }

        /*
         * Update lists if more than one Const is given.
         * We only need those relations which has not been rewritten by recurse function.
         * Therefore, we do a intersection on Var list before get into the next Const.
         * Then filter out those extra relations in return quals until the last iteration.
         */
        if (ret_quals != NULL) {
            quals_v = list_intersection(ret_quals, quals_v);
            ret_quals = list_difference(ret_quals, quals_v);
        }

        /* Concat base on type */
        if (IsA(var, Var) || IsA(var, RelabelType)) {
            ret_quals = list_concat_unique(ret_quals, match_equivclause_recurse(var, con, quals_v));
        }
    }

    if (ret_quals != NIL) { /* If found something, else do nothing */
        ret_quals = list_concat_unique(ret_quals, (List*)copyObject(quals_c));
    }

    return ret_quals;
}

/*
 * rewrite_equivclause
 * Find matching Vars and complete the clauses with equivalence classes.
 * Rewrite the jointree quals to make it more straightforward.
 * Node that we do not want to handle any kind of type casts here since they
 * are not part of the equivalent class anymore.
 * @param quals from FromExpr of the query
 */
static Node* rewrite_equivclause(Node *quals)
{
    ListCell* qcell = NULL;
    List* quals_all = NIL;
    List* ret_quals = NIL;
    List* tmp_quals_c = NIL;
    List* tmp_quals_v = NIL;

    if (!IsA(quals, List)) {
        quals_all = make_ands_implicit((Expr*)quals);
    } else {
        quals_all = (List*)quals;
    }

    /* 
     * Shuffle through all nodes. Seperate Var-Const and Var-Var relations 
     * into two seperate lists. Filter out unwanted expressions in quals.
     * Append the unsupported/unwanted expression to the return quals.
     */
    foreach(qcell, quals_all) {
        Expr* qual_expr = (Expr*)lfirst(qcell);
        OpExpr* op = (OpExpr*)qual_expr;

        /* Filter out non-op expression and non-equal relations */
        if (!IsA(qual_expr, OpExpr) || !isEqualExpr((Node*)op)) {
            ret_quals = lappend(ret_quals, (Node*)copyObject(op));
            continue;
        }

        /* Var-Var relation with RelabelType support */
        if (nodeTag((Node*)linitial(op->args)) == nodeTag((Node*)lsecond(op->args))) { 
            if (nodeTag((Node*)linitial(op->args)) == T_Const) { /* (+) compatible */
                ret_quals = lappend(ret_quals, (Node*)copyObject(op));
                continue;
            }
            tmp_quals_v = lappend(tmp_quals_v, op);
        } else if ((XOR(IsA((Node*)linitial(op->args), Const), IsA((Node*)lsecond(op->args), Const))) ||
                  (XOR(IsA((Node*)linitial(op->args), Param), IsA((Node*)lsecond(op->args), Param)))) {
            tmp_quals_c = lappend(tmp_quals_c, op);
        } else {
            /* Other not handled cases, append */
            ret_quals = lappend(ret_quals, (Node*)copyObject(op));
        }
    }

    /* If there is no Var-Const or Var-Var relations */
    if (tmp_quals_c == NIL || tmp_quals_v == NIL) {
        /* Free lists before return */
        list_free_ext(tmp_quals_c);
        list_free_ext(tmp_quals_v);
        list_free_ext(quals_all);
        list_free_deep(ret_quals);
        return quals;
    }

    /* Concatenate the rewritten quals */
    ret_quals = list_concat(ret_quals, match_equivclause(tmp_quals_c, tmp_quals_v));
    /* Handle no return quals */
    if (ret_quals == NULL) {
        ret_quals = quals_all;
    } else {
        list_free_ext(quals_all);
    }

    Node* result = (Node*)make_andclause(ret_quals);
    list_free_ext(tmp_quals_c);
    list_free_ext(tmp_quals_v);
    return result;
}

/*
 * pgxc_FQS_find_datanodes_recurse
 * Recursively find whether the sub-tree of From Expr rooted under given node is
 * pushable and if yes where.
 */
static ExecNodes* pgxc_FQS_find_datanodes_recurse(Node* node, Shippability_context* sc_context)
{
    Query* query = sc_context->sc_query;

    if (node == NULL)
        return NULL;

    switch (nodeTag(node)) {
        case T_FromExpr: {
            FromExpr* from_expr = (FromExpr*)node;
            ListCell* lcell = NULL;
            bool first = false;
            ExecNodes* result_en = NULL;

            /*
             * For INSERT commands, we won't have any entries in the from list.
             * Get the datanodes using the resultRelation index.
             */
            if (query->commandType != CMD_SELECT && !from_expr->fromlist)
                return pgxc_FQS_datanodes_for_rtr(query->resultRelation, sc_context);

            /*
             * All the entries in the From list are considered to be INNER
             * joined with the quals as the JOIN condition. Get the datanodes
             * for the first entry in the From list. For every subsequent entry
             * determine whether the join between the relation in that entry and
             * the cumulative JOIN of previous entries can be pushed down to the
             * datanodes and the corresponding set of datanodes where the join
             * can be pushed down.
             */
            first = true;
            result_en = NULL;
            foreach (lcell, from_expr->fromlist) {
                Node* fromlist_entry = (Node*)lfirst(lcell);
                ExecNodes* tmp_en = NULL;
                ExecNodes* en = pgxc_FQS_find_datanodes_recurse(fromlist_entry, sc_context);
                /*
                 * If any entry in fromlist is not shippable, jointree is not
                 * shippable
                 */
                if (en == NULL) {
                    FreeExecNodes(&result_en);
                    return NULL;
                }

                /* FQS does't ship a DML with more than one relation involved */
                if (!first && query->commandType != CMD_SELECT) {
                    FreeExecNodes(&result_en);
                    return NULL;
                }

                if (first) {
                    first = false;
                    result_en = en;
                    continue;
                }

                tmp_en = result_en;
                /*
                 * Check whether the JOIN is pushable to the datanodes and
                 * find the datanodes where the JOIN can be pushed to. In FQS
                 * the query is shippable if only all the expressions are
                 * shippable. Hence assume that the targetlists of the joining
                 * relations are shippable.
                 */
                result_en = pgxc_is_join_shippable(result_en, en, false, false, JOIN_INNER, from_expr->quals);
                FreeExecNodes(&tmp_en);
            }

            return result_en;
        } break;

        case T_RangeTblRef: {
            RangeTblRef* rtr = (RangeTblRef*)node;
            return pgxc_FQS_datanodes_for_rtr(rtr->rtindex, sc_context);
        } break;

        case T_JoinExpr: {
            JoinExpr* join_expr = (JoinExpr*)node;
            ExecNodes* len = NULL;
            ExecNodes* ren = NULL;
            ExecNodes* result_en = NULL;

            /* FQS does't ship a DML with more than one relation involved */
            if (query->commandType != CMD_SELECT)
                return NULL;

            len = pgxc_FQS_find_datanodes_recurse(join_expr->larg, sc_context);
            ren = pgxc_FQS_find_datanodes_recurse(join_expr->rarg, sc_context);
            /* If either side of JOIN is unshippable, JOIN is unshippable */
            if (len == NULL || ren == NULL) {
                FreeExecNodes(&len);
                FreeExecNodes(&ren);
                return NULL;
            }
            /*
             * Check whether the JOIN is pushable or not, and find the datanodes
             * where the JOIN can be pushed to. In FQS the query is shippable if
             * only all the expressions are shippable. Hence assume that the
             * targetlists of the joining relations are shippable.
             */
            result_en = pgxc_is_join_shippable(ren, len, false, false, join_expr->jointype, join_expr->quals);
            FreeExecNodes(&len);
            FreeExecNodes(&ren);
            return result_en;
        } break;

        default:
            return NULL;
            break;
    }
    /* Keep compiler happy */
    return NULL;
}

/*
 * pgxc_FQS_find_datanodes
 * Find the list of nodes where to ship query.
 */
static ExecNodes* pgxc_FQS_find_datanodes(Shippability_context* sc_context)
{
    ExecNodes* exec_nodes = NULL;
    Query* query = sc_context->sc_query;

    /*
     * For SELECT, the datanodes required to execute the query is obtained from
     * the join tree of the query
     */
    exec_nodes = pgxc_FQS_find_datanodes_recurse((Node*)query->jointree, sc_context);
    /* If we found the datanodes to ship, use them */
    if (exec_nodes != NULL && exec_nodes->nodeList != NIL) {
        /*
         * If this is the highest level query in the query tree and
         * relations involved in the query are such that ultimate JOIN is
         * replicated JOIN, choose only one of them.
         * If we do this for lower level queries in query tree, we might loose
         * chance because common nodes are left out.
         */
        if (IsExecNodesReplicated(exec_nodes) &&
            ((exec_nodes->accesstype == RELATION_ACCESS_READ_FOR_UPDATE && exec_nodes->nodeList->length > 1) ||
            exec_nodes->accesstype == RELATION_ACCESS_READ) &&
            sc_context->sc_query_level == 0 && !sc_context->sc_inselect) {
            List* tmp_list = exec_nodes->nodeList;
            exec_nodes->nodeList = GetPreferredReplicationNode(exec_nodes->nodeList);
            list_free_ext(tmp_list);
        }
        return exec_nodes;
    } else if (exec_nodes != NULL && exec_nodes->en_expr != NIL) {
        /*
         * If we found the expression which can decide which can be used to decide
         * where to ship the query, use that
         */    
        return exec_nodes;
    }
    /* No way to figure out datanodes to ship the query to */
    return NULL;
}

static void free_distcol_info(Datum* distcol_value, bool* distcol_isnull, Oid* distcol_type, List* idx_dist)
{
    pfree_ext(distcol_value);
    pfree_ext(distcol_isnull);
    pfree_ext(distcol_type);
    list_free_ext(idx_dist);
}

static void free_and_reset_exec_nodes_info(ExecNodes* nodes, ExecNodes* rel_exec_nodes, RelationLocInfo* rel_loc_info)
{
    list_free_deep(rel_exec_nodes->en_expr);
    list_free_ext(rel_exec_nodes->primarynodelist);
    list_free_ext(rel_exec_nodes->nodeList);
    rel_exec_nodes->en_expr = NIL;
    rel_exec_nodes->primarynodelist = nodes->primarynodelist;
    rel_exec_nodes->nodeList = nodes->nodeList;
    rel_exec_nodes->en_relid = rel_loc_info->relid;
    pfree_ext(nodes);
}

/*
 * pgxc_FQS_find_insert_multiple_values_nodes
 * Identifies the insert multi-values statement and determines 
 * whether the query can be executed by a single DN. If yes, 
 * the DN information is returned.
 */
static ExecNodes* pgxc_FQS_find_insert_multiple_values_nodes(Query* query, RangeTblEntry* values_rte,
    RelationLocInfo* rel_loc_info, ExecNodes* rel_exec_nodes, List* shardingColNames, bool pseudoTsDistcol = false)
{
    /* Identifies the insert multi-values statement. */
    ListCell* cell = NULL;
    foreach (cell, rel_exec_nodes->en_expr)
    {
        Expr* distcol_expr = (Expr*)lfirst(cell);
        if (!IsA(distcol_expr, Var)) {
            continue;
        }
        RangeTblEntry* rte_distcol_expr = rt_fetch(((Var*)distcol_expr)->varno, query->rtable);
        if (rte_distcol_expr->rtekind != RTE_VALUES) {
            return NULL;
        }
    }
    /* determines whether the query can be executed by a single DN. */
    int len;
    if (pseudoTsDistcol) {
        len = list_length(shardingColNames);
    } else {
        len = list_length(rel_loc_info->partAttrNum);
    }
    Datum* distcol_value = (Datum*)palloc(len * sizeof(Datum));
    bool* distcol_isnull = (bool*)palloc(len * sizeof(bool));
    Oid* distcol_type = (Oid*)palloc(len * sizeof(Oid));
    List* idx_dist = NIL;
    int i;
    ListCell* cell1 = NULL;
    ListCell* cell2 = NULL;
    bool distcol_is_sharding = true;

    ExecNodes* nodes = NULL;
    foreach (cell, values_rte->values_lists) {
        List* row_list = (List*)lfirst(cell);
        i = 0;
        list_free_ext(idx_dist);
        forboth(cell1, rel_exec_nodes->en_expr, cell2, shardingColNames)
        {
            Expr* distcol_expr = (Expr*)lfirst(cell1);
            Node* discol_node = NULL;
            if (IsA(distcol_expr, Var)) {
                Var* distcol_var = (Var*)distcol_expr;
                discol_node = (Node*)list_nth(row_list, distcol_var->varattno - 1);
            } else {
                discol_node = (Node*)distcol_expr;
            }
            char* colName = (char*)lfirst(cell2);
            ELOG_FIELD_NAME_START(colName);
            distcol_expr = (Expr*)eval_const_expressions_params(NULL, (Node*)discol_node, query->boundParamsQ);
            ELOG_FIELD_NAME_END;
            if (distcol_expr != NULL && IsA(distcol_expr, Const)) {
                Const* const_expr = (Const*)distcol_expr;
                distcol_value[i] = const_expr->constvalue;
                distcol_isnull[i] = const_expr->constisnull;
                distcol_type[i] = const_expr->consttype;
                idx_dist = lappend_int(idx_dist, i);
                i++;
            } else {
                distcol_is_sharding = false;
                break;
            }
        }
        if (!distcol_is_sharding) {
            break;
        }
        ExecNodes* tmp_nodes = GetRelationNodes(
            rel_loc_info, distcol_value, distcol_isnull, distcol_type, idx_dist, rel_exec_nodes->accesstype);
        if (nodes == NULL) {
            nodes = tmp_nodes;
        } else {
            /* 
             * when it's list/range distributed table,
             * inserted values may match none of DNs and nodeList maybe null.
             * so we must check nodeList first.
             */
            if (nodes->nodeList == NULL || tmp_nodes->nodeList == NULL ||
                linitial_int(nodes->nodeList) != linitial_int(tmp_nodes->nodeList)) {
                distcol_is_sharding = false;
                break;
            }
        }
    }

    free_distcol_info(distcol_value, distcol_isnull, distcol_type, idx_dist);
    if (distcol_is_sharding && nodes != NULL && nodes->nodeList != NIL) {
        free_and_reset_exec_nodes_info(nodes, rel_exec_nodes, rel_loc_info);
        list_free_ext(shardingColNames);
        return rel_exec_nodes;
    } else {
        return NULL;
    }
}

/*
 * pgxc_FQS_find_insert_single_values_nodes
 * Determines whether the query can be executed by a single DN. If yes, 
 * the DN information is returned.
 */
static ExecNodes* pgxc_FQS_find_insert_single_values_nodes(
    Query* query, RelationLocInfo* rel_loc_info, ExecNodes* rel_exec_nodes, List* shardingColNames,
    bool pseudoTsDistcol = false)
{
    int len;
    if (pseudoTsDistcol) {
        len = list_length(shardingColNames);
    } else {
        len = list_length(rel_loc_info->partAttrNum);
    }
    Datum* distcol_value = (Datum*)palloc(len * sizeof(Datum));
    bool* distcol_isnull = (bool*)palloc(len * sizeof(bool));
    Oid* distcol_type = (Oid*)palloc(len * sizeof(Oid));
    List* idx_dist = NIL;
    int i = 0;
    ListCell* cell1 = NULL;
    ListCell* cell2 = NULL;
    forboth(cell1, rel_exec_nodes->en_expr, cell2, shardingColNames)
    {
        Expr* distcol_expr = (Expr*)lfirst(cell1); 
        char* colName = (char*)lfirst(cell2);

        ELOG_FIELD_NAME_START(colName);
        distcol_expr = (Expr*)eval_const_expressions_params(NULL, (Node*)distcol_expr, query->boundParamsQ);
        ELOG_FIELD_NAME_END;

        if (distcol_expr != NULL && IsA(distcol_expr, Const)) {
            Const* const_expr = (Const*)distcol_expr;
            distcol_value[i] = const_expr->constvalue;
            distcol_isnull[i] = const_expr->constisnull;
            distcol_type[i] = const_expr->consttype;
            idx_dist = lappend_int(idx_dist, i);
            i++;
        } else
            break;
    }

    if (cell1 == NULL) {
        ExecNodes* nodes = GetRelationNodes(
            rel_loc_info, distcol_value, distcol_isnull, distcol_type, idx_dist, rel_exec_nodes->accesstype);
        free_distcol_info(distcol_value, distcol_isnull, distcol_type, idx_dist);
        if (nodes != NULL && nodes->nodeList != NIL) {
            free_and_reset_exec_nodes_info(nodes, rel_exec_nodes, rel_loc_info);
            list_free_ext(shardingColNames);
            return rel_exec_nodes;
        } else {
            return NULL;
        }
    } else {
        free_distcol_info(distcol_value, distcol_isnull, distcol_type, idx_dist);
        return NULL;
    }
}

/* 
 * ts_get_taglist
 * Return a list of all tag columns in the timeserise table.
 * This function applies only to timeseries relation which use the pseudo distribution column.
 * So the hidden column is checked in the function.
 */
static List* ts_get_taglist(Oid relid)
{
    Relation rel = relation_open(relid, AccessShareLock);
    TupleDesc tup_desc = RelationGetDescr(rel);
    Form_pg_attribute* attr = tup_desc->attrs;
    List* tag_name_list = NIL;
    bool valid_ts_table = false;
    char* str = NULL;

    for (int i = 0; i < tup_desc->natts; i++) {
        if (attr[i]->attkvtype == ATT_KV_TAG && IsTypeDistributable(attr[i]->atttypid)) {
            str = pstrdup(NameStr(attr[i]->attname));
            if (str != NULL) {
                tag_name_list = lappend(tag_name_list, makeString(str));
            } else {
                ereport(ERROR, (errmodule(MOD_TIMESERIES),
                        errmsg("Timeseries relation %u cloud not find tag column %d", relid, i)));
            }
        }
        if (attr[i]->attkvtype == ATT_KV_HIDE) {
            valid_ts_table = true;
        }
    }
    if (!valid_ts_table) {
        relation_close(rel, AccessShareLock);
        list_free_ext(tag_name_list);
        return NULL;
    }

    Assert(tag_name_list != NULL);
    relation_close(rel, AccessShareLock);

    return tag_name_list;
}

static RangeTblEntry* get_insert_multiple_values_rte(Query* query)
{
    ListCell* cell = NULL;
    RangeTblEntry* rte_tmp = NULL;
    RangeTblEntry* values_rte = NULL;
    foreach (cell, query->rtable) {
        rte_tmp = (RangeTblEntry*)lfirst(cell);
        if (rte_tmp->rtekind == RTE_VALUES) {
            if (values_rte != NULL) {
                ereport(ERROR,
                    (errcode(ERRCODE_RESTRICT_VIOLATION),
                        errmsg("too many values RTEs in INSERT")));
            }
            values_rte = rte_tmp;
        }
    }
    return values_rte;
}

/*
 * pgxc_FQS_get_relation_nodes
 * Return ExecNodes structure so as to decide which node the query should
 * execute on. If it is possible to set the node list directly, set it.
 * Otherwise set the appropriate distribution column expression or relid in
 * ExecNodes structure.
 */
static ExecNodes* pgxc_FQS_get_relation_nodes(RangeTblEntry* rte, Index varno, Query* query)
{
    CmdType command_type = query->commandType;
    bool for_update = query->rowMarks ? true : false;
    ExecNodes* rel_exec_nodes = NULL;
    RelationAccessType rel_access = RELATION_ACCESS_READ;
    RelationLocInfo* rel_loc_info = NULL;

    AssertEreport(rte == rt_fetch(varno, (query->rtable)), MOD_OPT, "could not find range table entry.");

    switch (command_type) {
        case CMD_SELECT:
            if (for_update)
                rel_access = RELATION_ACCESS_READ_FOR_UPDATE;
            else
                rel_access = RELATION_ACCESS_READ;
            break;

        case CMD_UPDATE:
        case CMD_DELETE:
        case CMD_MERGE:
            rel_access = RELATION_ACCESS_UPDATE;
            break;

        case CMD_INSERT:
            rel_access = RELATION_ACCESS_INSERT;
            break;

        default:
            /* should not happen, but */
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("Unrecognised command type %d", command_type)));
            break;
    }

    rel_loc_info = GetRelationLocInfo(rte->relid);
    /* If we don't know about the distribution of relation, bail out */
    if (rel_loc_info == NULL)
        return NULL;

    if (list_length(rel_loc_info->nodeList) == 1) {
        rel_exec_nodes = GetRelationNodes(rel_loc_info, NULL, NULL, NULL, NULL, rel_access);
        return rel_exec_nodes;
    }

    /* Find out the datanodes to execute this query on. */
    rel_exec_nodes = GetRelationNodesByQuals(
        (void*)query, rte->relid, varno, query->jointree->quals, rel_access, query->boundParamsQ, true);
    /* If not find, get all datanodes related to the table as before */
    if (list_length(query->rtable) != 1 && rel_exec_nodes == NULL)
        rel_exec_nodes = GetRelationNodes(rel_loc_info, NULL, NULL, NULL, NULL, rel_access);

    if (rel_exec_nodes == NULL)
        return NULL;

    if (IsExecNodesDistributedByValue(rel_exec_nodes)) {
        List* dist_var = pgxc_get_dist_var(varno, rte, query->targetList);
        rel_exec_nodes->en_dist_vars = list_make1(dist_var);
    }

    if (rel_access == RELATION_ACCESS_INSERT && IsRelationDistributedByValue(rel_loc_info)) {
        ListCell* lc = NULL;
        TargetEntry* tle = NULL;
        /*
         * If the INSERT is happening on a table distributed by value of a
         * column, find out the
         * expression for distribution column in the targetlist, and stick in
         * in ExecNodes, and clear the nodelist. Execution will find
         * out where to insert the row.
         */
        /* It is a partitioned table, get value by looking in targetList */
        List* distributeCol = GetRelationDistribColumn(rel_loc_info);
        ListCell* cell = NULL;
        List* shardingColNames = NIL;
        bool pseudoTsDistcol = false;

        /* If the timeserise table is with a hidden column as distribution column,
         * replace the distribute list to the tag column list.
         */
        if (rte->orientation == REL_TIMESERIES_ORIENTED && list_length(distributeCol) == 1 &&
            strcmp(TS_PSEUDO_DIST_COLUMN, strVal(lfirst(list_head(distributeCol)))) == 0) {
            ereport(DEBUG1, (errmodule(MOD_TIMESERIES),
                        errmsg("In FQS path replace hidetag distribution.")));
            if (ts_get_taglist(rte->relid) != NULL) {
                distributeCol = ts_get_taglist(rte->relid);
                pseudoTsDistcol = true;
            }
        }

        foreach (cell, distributeCol) {
            foreach (lc, query->targetList) {
                tle = (TargetEntry*)lfirst(lc);
                if (tle->resjunk)
                    continue;
                if (strcmp(tle->resname, strVal(lfirst(cell))) == 0) {
                    tle = (TargetEntry*)eval_const_expressions(NULL, (Node*)tle);
                    rel_exec_nodes->en_expr = lappend(rel_exec_nodes->en_expr, tle->expr);
                    shardingColNames = lappend(shardingColNames, tle->resname);
                    break;
                }
            }
            /* Not found, bail out */
            if (lc == NULL) {
                list_free_deep(rel_exec_nodes->en_expr);
                list_free_ext(shardingColNames);
                return NULL;
            }
        }

        /* compute nodelist here for custom_plan with params and normal simple query */
        if (query->boundParamsQ || !u_sess->pcache_cxt.query_has_params) {
            RangeTblEntry* values_rte = get_insert_multiple_values_rte(query);
            if (pseudoTsDistcol) {
                if (values_rte != NULL) {
                    ExecNodes* nodes = pgxc_FQS_find_insert_multiple_values_nodes(
                        query, values_rte, rel_loc_info, rel_exec_nodes, shardingColNames, true);
                    if (nodes != NULL) {
                        return nodes;
                    }
                } else {
                    ExecNodes* nodes = pgxc_FQS_find_insert_single_values_nodes(
                        query, rel_loc_info, rel_exec_nodes, shardingColNames, true);
                    if (nodes != NULL) {
                        return nodes;
                    }
                }
            } else {
                if (values_rte != NULL) {
                    ExecNodes* nodes = pgxc_FQS_find_insert_multiple_values_nodes(
                        query, values_rte, rel_loc_info, rel_exec_nodes, shardingColNames);
                    if (nodes != NULL) {
                        return nodes;
                    }
                } else {
                    ExecNodes* nodes = pgxc_FQS_find_insert_single_values_nodes(
                        query, rel_loc_info, rel_exec_nodes, shardingColNames);
                    if (nodes != NULL) {
                        return nodes;
                    }
                }
            }
        }

        /* We found the TargetEntry for the partition column */
        list_free_ext(distributeCol);
        list_free_ext(rel_exec_nodes->primarynodelist);
        rel_exec_nodes->primarynodelist = NULL;
        list_free_ext(rel_exec_nodes->nodeList);
        rel_exec_nodes->nodeList = NULL;
        rel_exec_nodes->en_relid = rel_loc_info->relid;
        list_free_ext(shardingColNames);
    }
    return rel_exec_nodes;
}

static bool isIncludeAlldistcol(List* dist_vars, List* queryClause, List* targetList)
{
    ListCell* l_cell = NULL;
    ListCell* lcell = NULL;
    ListCell* cell = NULL;
    List* dis_list = NIL;
    Var* disVar = NULL;

    foreach (l_cell, dist_vars) {
        dis_list = (List*)lfirst(l_cell);
        foreach (cell, dis_list) {
            disVar = (Var*)lfirst(cell);
            foreach (lcell, queryClause) {
                SortGroupClause* sgc = (SortGroupClause*)lfirst(lcell);
                Node* sgc_expr = NULL;
                if (!IsA(sgc, SortGroupClause))
                    continue;
                sgc_expr = get_sortgroupclause_expr(sgc, targetList);
                if (unlikely(sgc_expr == NULL)) {
                    ereport(ERROR,
                        (errmodule(MOD_OPT),
                            errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                                errmsg("sgc_expr should not be NULL")));
                }
                if (IsA(sgc_expr, Var) && equal(sgc_expr, disVar)) {
                    break;
                }
            }
            if (NULL == lcell) {
                break;
            }
        }
        if (NULL == cell) {
            return true;
        }
    }
    return false;
}

bool pgxc_query_has_distcolgrouping(Query* query, ExecNodes* exec_nodes)
{
    if (exec_nodes == NULL || exec_nodes->en_dist_vars == NIL || !query->groupClause)
        return false;

    return isIncludeAlldistcol(exec_nodes->en_dist_vars, query->groupClause, query->targetList);
}

static bool pgxc_distinct_has_distcol(Query* query, ExecNodes* exec_nodes)
{
    if (exec_nodes == NULL || exec_nodes->en_dist_vars == NIL || !query->distinctClause)
        return false;

    return isIncludeAlldistcol(exec_nodes->en_dist_vars, query->distinctClause, query->targetList);
}

/*
 * pgxc_shippability_walker
 * walks the query/expression tree routed at the node passed in, gathering
 * information which will help decide whether the query to which this node
 * belongs is shippable to the Datanodes.
 *
 * The function should try to walk the entire tree analysing each subquery for
 * shippability. If a subquery is shippable but not the whole query, we would be
 * able to create a RemoteQuery node for that subquery, shipping it to the
 * Datanode.
 *
 * Return value of this function is governed by the same rules as
 * expression_tree_walker(), see prologue of that function for details.
 */
static bool pgxc_shippability_walker(Node* node, Shippability_context* sc_context)
{
    if (node == NULL)
        return false;

    /* Below is the list of nodes that can appear in a query, examine each
     * kind of node and find out under what conditions query with this node can
     * be shippable. For each node, update the context (add fields if
     * necessary) so that decision whether to FQS the query or not can be made.
     * Every node which has a result is checked to see if the result type of that
     * expression is shippable.
     */
    switch (nodeTag(node)) {
        /* Constants are always shippable */
        case T_Const:
            pgxc_set_exprtype_shippability(exprType(node), sc_context);
            break;

            /*
             * For placeholder nodes the shippability of the node, depends upon the
             * expression which they refer to. It will be checked separately, when
             * that expression is encountered.
             */
        case T_CaseTestExpr:
            pgxc_set_exprtype_shippability(exprType(node), sc_context);
            break;

            /*
             * record_in() function throws error, thus requesting a result in the
             * form of anonymous record from datanode gets into error. Hence, if the
             * top expression of a target entry is ROW(), it's not shippable.
             */
        case T_TargetEntry: {
            TargetEntry* tle = (TargetEntry*)node;
            if (tle->expr) {
                char typtype = get_typtype(exprType((Node*)tle->expr));
                if (!typtype || typtype == TYPTYPE_PSEUDO) {
                    if (sc_context->sc_contain_column_store)
                        /* if contain column table, not shippable */
                        pgxc_set_shippability_reason(sc_context, SS_UNSHIPPABLE_EXPR);
                    else {
                        pgxc_set_shippability_reason(sc_context, SS_NEED_NO_CSTORE);
                        pgxc_set_shippability_reason(sc_context, SS_NEED_SINGLENODE);
                    }
                }
            }
        } break;

        case T_SortGroupClause:
            if (sc_context->sc_for_expr)
                pgxc_set_shippability_reason(sc_context, SS_UNSUPPORTED_EXPR);
            break;

        case T_CoerceViaIO: {
            CoerceViaIO* cvio = (CoerceViaIO*)node;
            Oid input_type = exprType((Node*)cvio->arg);
            Oid output_type = cvio->resulttype;
            CoercionContext cc;

            cc = ((cvio->coerceformat == COERCE_IMPLICIT_CAST) ? COERCION_IMPLICIT : COERCION_EXPLICIT);
            /*
             * Internally we use IO coercion for types which do not have casting
             * defined for them e.g. cstring::date. If such casts are sent to
             * the datanode, those won't be accepted. Hence such casts are
             * unshippable. Since it will be shown as an explicit cast.
             *
             * If single node with no column table, it can be shipped
             * because cn and dn should do the same casting.
             */
            if (IsA(cvio->arg, Const))
                pgxc_set_shippability_reason(sc_context, SS_NEED_SINGLENODE);
            else if (!can_coerce_type(1, &input_type, &output_type, cc)) {
                if (sc_context->sc_contain_column_store)
                    pgxc_set_shippability_reason(sc_context, SS_UNSHIPPABLE_EXPR);
                else {
                    pgxc_set_shippability_reason(sc_context, SS_NEED_NO_CSTORE);
                    pgxc_set_shippability_reason(sc_context, SS_NEED_SINGLENODE);
                }
            }
            pgxc_set_exprtype_shippability(exprType(node), sc_context);
        } break;
        /*
         * Nodes, which are shippable if the tree rooted under these nodes is
         * shippable
         */
        case T_CoerceToDomainValue:
            /*
             * Mostly, CoerceToDomainValue node appears in DDLs,
             * do we handle DDLs here?
             */
        case T_FieldSelect:
        case T_NamedArgExpr:
        case T_RelabelType:
        case T_BoolExpr:
            /*
             * We might need to take into account the kind of boolean
             * operator we have in the quals and see if the corresponding
             * function is immutable.
             */
        case T_ArrayCoerceExpr:
        case T_ConvertRowtypeExpr:
        case T_CaseExpr:
        case T_ArrayExpr:
        case T_RowExpr:
        case T_CollateExpr:
        case T_CoalesceExpr:
        case T_XmlExpr:
        case T_NullTest:
        case T_BooleanTest:
        case T_CoerceToDomain:
        case T_HashFilter:
            pgxc_set_exprtype_shippability(exprType(node), sc_context);
            break;

        case T_List:
        case T_RangeTblRef:
        case T_UpsertExpr:
            break;

        case T_ArrayRef:
            /*
             * When multiple values of of an array are updated at once
             * FQS planner cannot yet handle SQL representation correctly.
             * So disable FQS in this case and let standard planner manage it.
             */
        case T_FieldStore:
            /*
             * openGauss deparsing logic does not handle the FieldStore
             * for more than one fields (see processIndirection()). So, let's
             * handle it through standard planner, where whole row will be
             * constructed.
             */
        case T_SetToDefault:
            /*
             * we should actually check whether the default value to
             * be substituted is shippable to the Datanode.
             */
            if (sc_context->sc_contain_column_store)
                pgxc_set_shippability_reason(sc_context, SS_UNSUPPORTED_EXPR);
            else {
                pgxc_set_shippability_reason(sc_context, SS_NEED_NO_CSTORE);
                pgxc_set_shippability_reason(sc_context, SS_NEED_SINGLENODE);
            }

            pgxc_set_exprtype_shippability(exprType(node), sc_context);
            break;

        case T_Var: {
            Var* var = (Var*)node;
            /*
             * if a subquery references an upper level variable, that query is
             * not shippable, if shipped alone.
             */
            if (var->varlevelsup > (unsigned int)sc_context->sc_max_varlevelsup)
                sc_context->sc_max_varlevelsup = var->varlevelsup;
            pgxc_set_exprtype_shippability(exprType(node), sc_context);
        } break;

        case T_Param: {
            Param* param = (Param*)node;
            /* We can ship PARAM_EXEC/PARAM_SUBLINK params if single node */
            if (param->paramkind != PARAM_EXTERN)
                pgxc_set_shippability_reason(sc_context, SS_NEED_SINGLENODE);

            pgxc_set_exprtype_shippability(exprType(node), sc_context);
        } break;

        case T_CurrentOfExpr: {
            /*
             * Ideally we should not see CurrentOf expression here, it
             * should have been replaced by the CTID = ? expression. But
             * still, no harm in shipping it as is.
             */
            pgxc_set_exprtype_shippability(exprType(node), sc_context);
        } break;

        case T_Aggref: {
            Aggref* aggref = (Aggref*)node;
            /*
             * An aggregate is completely shippable to the Datanode, if the
             * whole group resides on that Datanode. This will be clear when
             * we see the GROUP BY clause.
             * agglevelsup is minimum of variable's varlevelsup, so we will
             * set the sc_max_varlevelsup when we reach the appropriate
             * VARs in the tree.
             */
            pgxc_set_shippability_reason(sc_context, SS_HAS_AGG_EXPR);
            /*
             * If a stand-alone expression to be shipped, is an
             * 1. aggregate with ORDER BY, DISTINCT directives, it needs all
             * the qualifying rows
             * 2. aggregate without collection function
             * 3. aggregate with polymorphic transition type, the
             *    the transition type needs to be resolved to correctly interpret
             *    the transition results from Datanodes.
             * Hence, such an expression can not be shipped to the datanodes.
             */
            if (aggref->aggorder || aggref->aggdistinct || aggref->agglevelsup || !aggref->agghas_collectfn ||
                IsPolymorphicType(aggref->aggtrantype)) {
                pgxc_set_shippability_reason(sc_context, SS_NEED_SINGLENODE);

                /* when there is only a single node involved(Replication),
                 * we will append finalised agg func to the pushed down SQL query.
                 * But for array_agg/string_agg, the finalised func are not supported
                 * calling as SQL functions.because they are internal functions.
                 * So not push down them.
                 */
                if (!pgxc_is_internal_agg_final_func(aggref->aggfnoid))
                    pgxc_set_shippability_reason(sc_context, SS_UNSUPPORTED_EXPR);
            }

            /*
             * If the aggregate is on anyarray,
             * and coordinates will execute on the aggregate output,
             * such as doing ORBER BY or encoding mismatches between client and server,
             * such expression can not be shipped to the datanodes.
             * This is because in parse_agg() function,
             * datanode will replace agg->aggtype with agg->aggtrantype.
             * After fix that, remember to remove this.
             * For example:
             * postgres=# show server_encoding;
             * server_encoding
             * -----------------
             * UTF8
             * (1 row)
             * postgres=# set client_encoding=SQL_ASCII;
             * SET
             * postgres=# select max(f1) from arraggtest;
             * ERROR:  cache lookup failed for type 2779086336
             * postgres=# explain verbose select max(f1) from arraggtest;
             *                          QUERY PLAN
             * --------------------------------------------------------------
             * Data Node Scan  (cost=0.00..0.00 rows=0 width=0)
             *   Output: (max(arraggtest.f1))
             *   Node/s: datanode2
             *   Remote query: SELECT max(f1) AS max FROM public.arraggtest
             * (4 rows)
             * postgres=# select max(f1) from arraggtest;
             * ERROR:  cache lookup failed for type 2779086336
             */
            if (aggref->aggtrantype == ANYARRAYOID) {
                if (!sc_context->sc_light_proxy ||
                    (sc_context->sc_query != NULL && sc_context->sc_query->sortClause != NIL))
                    pgxc_set_shippability_reason(sc_context, SS_UNSUPPORTED_EXPR);
            }

            /* For light proxy mode, agg that has final function is not supported */
            if (sc_context->sc_light_proxy) {
                HeapTuple aggTuple;
                Form_pg_aggregate aggform;
                aggTuple = SearchSysCache(AGGFNOID, ObjectIdGetDatum(aggref->aggfnoid), 0, 0, 0);
                if (!HeapTupleIsValid(aggTuple))
                    ereport(ERROR,
                        (errmodule(MOD_OPT),
                            errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                            errmsg("cache lookup failed for aggregate %u", aggref->aggfnoid)));
                aggform = (Form_pg_aggregate)GETSTRUCT(aggTuple);
                if (OidIsValid(aggform->aggfinalfn))
                    pgxc_set_shippability_reason(sc_context, SS_UNSUPPORTED_EXPR);

                ReleaseSysCache(aggTuple);
            }

            pgxc_set_exprtype_shippability(exprType(node), sc_context);
        } break;

        case T_FuncExpr: {
            FuncExpr* funcexpr = (FuncExpr*)node;
            /*
             * NOTICE: it's too restrictive not to ship non-immutable
             * functions to the Datanode. We need a better way to see what
             * can be shipped to the Datanode and what can not be.
             */
            shipping_context context;
            errno_t rc = memset_s(&context, sizeof(context), 0, sizeof(context));
            securec_check(rc, "\0", "\0");

            context.is_randomfunc_shippable = u_sess->opt_cxt.is_randomfunc_shippable && IS_STREAM_PLAN;
            context.is_ecfunc_shippable = IS_STREAM_PLAN;
            context.disallow_volatile_func_shippable = sc_context->sc_disallow_volatile_func_shippability;

            if (!pgxc_is_func_shippable(funcexpr->funcid, &context)) {
                if (funcexpr->funcid == NEXTVALFUNCOID)
                    pgxc_set_shippability_reason(sc_context, SS_UNSUPPORTED_EXPR);
                else
                    pgxc_set_shippability_reason(sc_context, SS_UNSHIPPABLE_UDF);
            } else if (funcexpr->funcid == TESTSKEWNESSRETURNTYPE) {
                /* Function table_test_skewness can not FQS, because need send bucketmap to dn */
                pgxc_set_shippability_reason(sc_context, SS_UNSHIPPABLE_EXPR);
            }

            /* some function args type like concat() contains ANY, we disable FQS */
            if (pgxc_is_shippable_func_contain_any(funcexpr->funcid)) {
                /* check function's args */
                return expression_tree_walker(
                    (Node*)funcexpr->args, (bool (*)())pgxc_shippability_walker, (void*)sc_context);
            }

            /*
             * If this is a stand alone expression and the function returns a
             * set of rows, we need to handle it along with the final result of
             * other expressions. So, it can not be shippable.
             */
            if (funcexpr->funcretset && sc_context->sc_for_expr)
                pgxc_set_shippability_reason(sc_context, SS_UNSHIPPABLE_FUNCTION);

            pgxc_set_exprtype_shippability(exprType(node), sc_context);
        } break;

        case T_OpExpr:
        case T_DistinctExpr: /* struct-equivalent to OpExpr */
        case T_NullIfExpr:   /* struct-equivalent to OpExpr */
        {
            /*
             * All of these three are structurally equivalent to OpExpr, so
             * cast the node to OpExpr and check if the operator function is
             * immutable. See NOTICE item for FuncExpr.
             */
            OpExpr* op_expr = (OpExpr*)node;
            Oid opfuncid = OidIsValid(op_expr->opfuncid) ? op_expr->opfuncid : get_opcode(op_expr->opno);
            if (!OidIsValid(opfuncid) || !pgxc_is_func_shippable(opfuncid))
                pgxc_set_shippability_reason(sc_context, SS_UNSHIPPABLE_EXPR);

            pgxc_set_exprtype_shippability(exprType(node), sc_context);
        } break;

        case T_ScalarArrayOpExpr: {
            /*
             * Check if the operator function is shippable to the Datanode
             * NOTICE: see immutability note for FuncExpr above
             */
            ScalarArrayOpExpr* sao_expr = (ScalarArrayOpExpr*)node;
            Oid opfuncid = OidIsValid(sao_expr->opfuncid) ? sao_expr->opfuncid : get_opcode(sao_expr->opno);
            if (!OidIsValid(opfuncid) || !pgxc_is_func_shippable(opfuncid))
                pgxc_set_shippability_reason(sc_context, SS_UNSHIPPABLE_EXPR);
        } break;

        case T_RowCompareExpr:
        case T_MinMaxExpr: {
            /*
             * Should we be checking the comparision operator
             * functions as well, as we did for OpExpr OR that check is
             * unnecessary. Operator functions are always shippable?
             * Otherwise this node should be treated similar to other
             * "shell" nodes.
             */
            pgxc_set_exprtype_shippability(exprType(node), sc_context);
        } break;

        case T_Query: {
            Query* query = (Query*)node;
            bool hasTrigger = false;

            if (NULL == sc_context->sc_query) {
                ereport(ERROR,
                    (errmodule(MOD_OPT),
                        errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                        errmsg("Invalid query for shippable check.")));
            }

            /* the query with a returning list can be shippable, if single node */
            if (query->returningList)
                pgxc_set_shippability_reason(sc_context, SS_NEED_SINGLENODE);

            /* A stand-alone expression containing Query is not shippable */
            if (sc_context->sc_for_expr) {
                pgxc_set_shippability_reason(sc_context, SS_UNSHIPPABLE_EXPR);
                break;
            }

            /*
             * We are checking shippability of whole query, go ahead. The query
             * in the context should be same as the query being checked
             */
            AssertEreport(query == sc_context->sc_query, MOD_OPT, "fqs query is unexpected.");

            /* CREATE TABLE AS is not supported in FQS */
            if (query->commandType == CMD_UTILITY && IsA(query->utilityStmt, CreateTableAsStmt))
                pgxc_set_shippability_reason(sc_context, SS_UNSUPPORTED_EXPR);

            /* set sc_use_star_up for FQS */
            if (!sc_context->sc_light_proxy && query->commandType == CMD_SELECT && sc_context->sc_query_level > 0 &&
                check_has_sameAlias(query->targetList)) {
                sc_context->sc_use_star_upper_level = true;
            }

            /*
             * If it is a PBE-style SELECT query with multiple tables,
             * we only ship it if single node for now.
             */
            if (query->commandType == CMD_SELECT && list_length(query->rtable) > 1 &&
                u_sess->pcache_cxt.query_has_params) {
                pgxc_set_shippability_reason(sc_context, SS_NEED_SINGLENODE);
            }

            if (query->hasRecursive)
                pgxc_set_shippability_reason(sc_context, SS_UNSUPPORTED_EXPR);

            /*
             * If the query needs Coordinator for evaluation or the query can be
             * completed on Coordinator itself, we don't ship it to the Datanode
             */
            if (pgxc_query_needs_coord(query))
                pgxc_set_shippability_reason(sc_context, SS_NEEDS_COORD);

            /* It should be possible to look at the Query and find out
             * whether it can be completely evaluated on the Datanode just like SELECT
             * queries. But we need to be careful while finding out the Datanodes to
             * execute the query on, esp. for the result relations. If one happens to
             * remove/change this restriction, make sure you change
             * pgxc_FQS_get_relation_nodes appropriately.
             * For now UPDATE and DELETE DMLs with single rtable entry are candidates for FQS
             * INSERT DMLs will be checked later within pgxc_set_insert_shippability.
             */
            if (query->commandType != CMD_SELECT && query->commandType != CMD_INSERT &&
                list_length(query->rtable) > 1) {
                pgxc_set_shippability_reason(sc_context, SS_UNSUPPORTED_EXPR);
            }

            /* Ap function can not fast query shipping. Because it need do more then once
             * group by operator. Each group by maybe include all distribute also possible not
             * include all distribute therefore to ap function we need send plan to datanodes.
             *
             * However, we can ship the query if single node.
             */
            if (query->groupingSets) {
                pgxc_set_shippability_reason(sc_context, SS_NEED_SINGLENODE);
            }

            /*
             * In following conditions query is shippable when there is only one
             * Datanode involved
             * 1. the query has aggregagtes without grouping by distribution
             *    column
             * 2. the query has window functions
             * 3. the query has ORDER BY clause
             * 4. the query has Distinct clause without distribution column in
             *    distinct clause
             * 5. the query has limit and offset clause
             */
            if (query->hasWindowFuncs || query->sortClause || query->limitOffset || query->limitCount)
                pgxc_set_shippability_reason(sc_context, SS_NEED_SINGLENODE);

            /* Logic of check query in trigger : check update/delete query in trigger. */
            if (sc_context->sc_for_trigger && (CMD_UPDATE == query->commandType || CMD_DELETE == query->commandType)) {
                if (!pgxc_check_shippability_in_trigger(query, sc_context))
                    pgxc_set_shippability_reason(sc_context, SS_UNSHIPPABLE_EXPR);
            }

            /*
             * If the distribution column in insert query is a set, the query won't be shippable
             */
            bool temp_random_shippabe = u_sess->opt_cxt.is_randomfunc_shippable;
            if (CMD_INSERT == query->commandType) {
                if (sc_context->sc_for_trigger) {
                    /* Logic of check query in trigger : check insert query in trigger. */
                    if (!pgxc_check_shippability_in_trigger(query, sc_context))
                        pgxc_set_shippability_reason(sc_context, SS_UNSHIPPABLE_EXPR);
                } else {
                    pgxc_set_insert_shippability(query, sc_context);
                }
            }

            /* Disallow volatile function shippable when the target relation is replicated. */
            if (query->commandType != CMD_SELECT) {
                RangeTblEntry* rte = (RangeTblEntry*)list_nth(query->rtable, query->resultRelation - 1);
                RelationLocInfo* locator = GetRelationLocInfo(rte->relid);

                if (NULL != locator && IsLocatorReplicated(locator->locatorType)) {
                    sc_context->sc_disallow_volatile_func_shippability = true;
                }
            }

            /* view is not supported for light proxy */
            if (sc_context->sc_light_proxy) {
                ListCell* lc = NULL;
                foreach (lc, query->rtable) {
                    RangeTblEntry* rte = (RangeTblEntry*)lfirst(lc);
                    if (rte->rtekind == RTE_RELATION && (rte->relkind == RELKIND_VIEW || 
                        rte->relkind == RELKIND_CONTQUERY)) {
                        pgxc_set_shippability_reason(sc_context, SS_UNSHIPPABLE_EXPR);
                        break;
                    }
                }
            }

            /*
             * walk the entire query tree to analyse the query. We will walk the
             * range table, when examining the FROM clause. No need to do it
             * here. However, for light proxy, we will find view that doesn't
             * in FROM clause, so traverse it here
             */
            if (query_tree_walker(query, (bool (*)())pgxc_shippability_walker, sc_context, QTW_IGNORE_RANGE_TABLE)) {
                u_sess->opt_cxt.is_randomfunc_shippable = temp_random_shippabe;
                return true;
            }

            /* Reset thread local veriable 'u_sess->opt_cxt.is_randomfunc_shippable' for next query. */
            u_sess->opt_cxt.is_randomfunc_shippable = temp_random_shippabe;

            /*
             * NOTICE:
             * There is a subquery in this query, which references Vars in the upper
             * query. For now stop shipping such queries. We should get rid of this
             * condition.
             *
             * For single node, we can ship the query.
             */
            if (sc_context->sc_max_varlevelsup != 0)
                pgxc_set_shippability_reason(sc_context, SS_NEED_SINGLENODE);

            /*
             * Check shippability of triggers on this query. Don't consider
             * TRUNCATE triggers; it's a utility statement and triggers are
             * handled explicitly in ExecuteTruncate()
             */
            if (query->commandType == CMD_UPDATE || query->commandType == CMD_INSERT ||
                query->commandType == CMD_DELETE) {
                RangeTblEntry* rte = (RangeTblEntry*)list_nth(query->rtable, query->resultRelation - 1);

                if (!pgxc_check_triggers_shippability(rte->relid, query->commandType, &hasTrigger)) {
                    pgxc_set_shippability_reason(sc_context, SS_UNSHIPPABLE_TRIGGER);
                } else {
                    if (u_sess->attr.attr_sql.enable_trigger_shipping && hasTrigger && !sc_context->sc_for_trigger)
                        query->isRowTriggerShippable = true;
                    else
                        query->isRowTriggerShippable = false;
                }
            }

            /*
             * If the query contains equivalent classes, rewrite the quals to boost performance.
             * e.g.
             * Rewrite [... WHERE A = B and B = 3] into [... WHERE A = 3 and B = 3]
             * The original quals are recovered afterwards. All changes on quals will be discarded.
             */
            bool has_rewritten_quals = false; /* Rewrite flag */
            Node* copied_quals = NULL;  /* The rewritten quals */
            Node* original_quals = NULL; /* The original quals */
            if (query->jointree != NULL && IsA((Node*)query->jointree, FromExpr)) {
                FromExpr* from_expr = (FromExpr*)query->jointree;
                /* Rewrite equivclauses iff it is connected with AND */
                if (and_clause(from_expr->quals)) {
                    /* Create a temporary quals */
                    copied_quals = (Node*)copyObject(from_expr->quals);
                    original_quals = from_expr->quals;

                    /* Attach the copied quals to the query */
                    query->jointree->quals = copied_quals;
                    from_expr->quals = rewrite_equivclause(from_expr->quals);
                    has_rewritten_quals = true; /* Set flag */
                }
            }

            /*
             * Walk the join tree of the query and find the
             * Datanodes needed for evaluating this query
             */
            sc_context->sc_exec_nodes = pgxc_FQS_find_datanodes(sc_context);

            /* Retrieve the original jointree if rewritten */
            if (has_rewritten_quals) {
                query->jointree->quals = original_quals;
                pfree_ext(copied_quals);    /* Free temp quals */
            }

            /*
             * Presence of aggregates or having clause, implies grouping. In
             * such cases, the query won't be shippable unless 1. there is only
             * a single node involved 2. GROUP BY clause has distribution column
             * in it. In the later case aggregates for a given group are entirely
             * computable on a single datanode, because all the rows
             * participating in particular group reside on that datanode.
             * The distribution column can be of any relation
             * participating in the query. All the rows of that relation with
             * the same value of distribution column reside on same node.
             */
            if ((query->hasAggs || query->havingQual || query->groupClause) && sc_context->sc_exec_nodes &&
                !pgxc_query_has_distcolgrouping(query, sc_context->sc_exec_nodes))
                pgxc_set_shippability_reason(sc_context, SS_NEED_SINGLENODE);

            /*
             * If distribution column of any relation is present in the distinct
             * clause, values for that column across nodes will differ, thus two
             * nodes won't be able to produce same result row. Hence in such
             * case, we can execute the queries on many nodes managing to have
             * distinct result.
             */
            if (query->distinctClause && sc_context->sc_exec_nodes &&
                !pgxc_distinct_has_distcol(query, sc_context->sc_exec_nodes))
                pgxc_set_shippability_reason(sc_context, SS_NEED_SINGLENODE);
        } break;

        case T_FromExpr: {
            /* We don't expect FromExpr in a stand-alone expression */
            if (sc_context->sc_for_expr)
                pgxc_set_shippability_reason(sc_context, SS_UNSUPPORTED_EXPR);

            /*
             * We will examine the jointree of query separately to determine the
             * set of datanodes where to execute the query.
             * If this is an INSERT query with quals, resulting from say
             * conditional rule, we can not handle those in FQS, since there is
             * not SQL representation for such quals.
             */
            if (sc_context->sc_query != NULL && sc_context->sc_query->commandType == CMD_INSERT &&
                ((FromExpr*)node)->quals)
                pgxc_set_shippability_reason(sc_context, SS_UNSUPPORTED_EXPR);
        } break;

        case T_WindowFunc: {
            WindowFunc* winf = (WindowFunc*)node;
            /*
             * A window function can be evaluated on a Datanode if there is
             * only one Datanode involved.
             */
            pgxc_set_shippability_reason(sc_context, SS_NEED_SINGLENODE);

            /*
             * row_number() is not shippable for replication tables, since
             * the data may not have the same order on all datanodes.
             */
            if (winf->winfnoid == ROWNUMBERFUNCOID) {
                pgxc_set_shippability_reason(sc_context, SS_UNSHIPPABLE_EXPR);
            }

            /*
             * A window function is not shippable as part of a stand-alone
             * expression. If the window function is non-immutable, it can not
             * be shipped to the datanodes.
             */
            if (sc_context->sc_for_expr || !pgxc_is_func_shippable(winf->winfnoid)) {
                pgxc_set_shippability_reason(sc_context, SS_UNSHIPPABLE_EXPR);
            }

            pgxc_set_exprtype_shippability(exprType(node), sc_context);
        } break;

        case T_WindowClause: {
            /*
             * A window function can be evaluated on a Datanode if there is
             * only one Datanode involved.
             */
            pgxc_set_shippability_reason(sc_context, SS_NEED_SINGLENODE);

            /*
             * A window function is not shippable as part of a stand-alone
             * expression
             */
            if (sc_context->sc_for_expr)
                pgxc_set_shippability_reason(sc_context, SS_UNSHIPPABLE_EXPR);
        } break;

        case T_JoinExpr:
            /* We don't expect JoinExpr in a stand-alone expression */
            if (sc_context->sc_for_expr)
                pgxc_set_shippability_reason(sc_context, SS_UNSUPPORTED_EXPR);

            /*
             * The shippability of join will be deduced while
             * examining the jointree of the query. Nothing to do here
             */
            break;

        case T_SubLink: {
            /*
             * We need to walk the tree in sublink to check for its
             * shippability. We need to call pgxc_is_query_shippable() on Query
             * instead of this function so that every subquery gets a different
             * context for itself. We should avoid the default expression walker
             * getting called on the subquery. At the same time we don't want to
             * miss any other member (current or future) of this structure, from
             * being scanned. So, copy the SubLink structure with subselect
             * being NULL and call expression_tree_walker on the copied
             * structure.
             */
            SubLink sublink = *(SubLink*)node;
            ExecNodes* sublink_en = NULL;
            /*
             * Walk the query and find the nodes where the query should be
             * executed and node distribution. Merge this with the existing
             * node list obtained for other subqueries. If merging fails, we
             * can not ship the whole query.
             */
            if (IsA(sublink.subselect, Query)) {
                bool use_star_targets = false;
                bool contain_column_store = sc_context->sc_contain_column_store;
                sublink_en = pgxc_is_query_shippable((Query*)(sublink.subselect),
                    sc_context->sc_query_level + 1,
                    sc_context->sc_light_proxy,
                    &contain_column_store,
                    &use_star_targets);
                /* once contain cstore, always constain */
                if (contain_column_store)
                    sc_context->sc_contain_column_store = contain_column_store;
                /* once need to use star, always use */
                if (use_star_targets)
                    sc_context->sc_query->use_star_targets = use_star_targets;
            } else
                sublink_en = NULL;

            /* If we already know that this query does not have a set of nodes
             * to evaluate on, don't bother to merge again.
             */
            if (!pgxc_test_shippability_reason(sc_context, SS_NO_NODES)) {
                /*
                 * If this is the first time we are finding out the nodes for
                 * SubLink, we don't have anything to merge, just assign.
                 */
                if (sc_context->sc_subquery_en == NULL)
                    sc_context->sc_subquery_en = sublink_en;
                /*
                 * Merge the accumulated SubLink ExecNodes and the
                 * ExecNodes for this subquery.
                 */
                else if (sublink_en != NULL) {
                    sc_context->sc_subquery_en = pgxc_merge_exec_nodes(sublink_en, sc_context->sc_subquery_en);
                } else
                    sc_context->sc_subquery_en = NULL;

                /*
                 * If we didn't find a cumulative ExecNodes, set shippability
                 * reason, so that we don't bother merging future sublinks.
                 */
                if (sc_context->sc_subquery_en == NULL)
                    pgxc_set_shippability_reason(sc_context, SS_NO_NODES);
            } else
                AssertEreport(!sc_context->sc_subquery_en, MOD_OPT, "unexpected exec nodes in shippability_context.");

            /* Check if the type of sublink result is shippable */
            pgxc_set_exprtype_shippability(exprType(node), sc_context);

            /* Wipe out subselect as explained above and walk the copied tree */
            sublink.subselect = NULL;
            /* For sublink we send plan except for single node. */
            pgxc_set_shippability_reason(sc_context, SS_NEED_SINGLENODE);
            return expression_tree_walker((Node*)&sublink, (bool (*)())pgxc_shippability_walker, sc_context);
        } break;

        case T_GroupingFunc:
        case T_PlaceHolderVar: {
            if (sc_context->sc_contain_column_store)
                pgxc_set_shippability_reason(sc_context, SS_UNSUPPORTED_EXPR);
            else {
                pgxc_set_shippability_reason(sc_context, SS_NEED_NO_CSTORE);
                pgxc_set_shippability_reason(sc_context, SS_NEED_SINGLENODE);
            }
        } break;

        case T_MergeAction: {
            /* only support the UPSERT transformed MERGE.
             * the original MERGE are not supported for shippability.
             */
            if (sc_context->sc_query != NULL && sc_context->sc_query->commandType != CMD_INSERT) {
                pgxc_set_shippability_reason(sc_context, SS_UNSUPPORTED_EXPR);
                return false;
            }
        } break;
        case T_GroupingId:
        case T_SubPlan:
        case T_AlternativeSubPlan:
        case T_CommonTableExpr:
        case T_SetOperationStmt:
        case T_AppendRelInfo:
        case T_PlaceHolderInfo: {
            pgxc_set_shippability_reason(sc_context, SS_UNSUPPORTED_EXPR);
            /*
             * These expressions are not supported for shippability entirely, so
             * there is no need to walk trees underneath those. If we do so, we
             * might walk the trees with wrong context there.
             */
            return false;
        } break;

        default:
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("unrecognized node type: %d", (int)nodeTag(node))));
            break;
    }

    return expression_tree_walker(node, (bool (*)())pgxc_shippability_walker, (void*)sc_context);
}

/*
 * Returns whether or not the rtable (and its subqueries)
 * contain foreign table entries.
 */
bool pgxc_query_contains_foreign_table(List* rtable)
{
    ListCell* item = NULL;

    /* May be complicated. Before giving up, just check for pg_catalog usage */
    foreach (item, rtable) {
        RangeTblEntry* rte = (RangeTblEntry*)lfirst(item);

        if (rte->relkind == RELKIND_FOREIGN_TABLE || rte->relkind == RELKIND_STREAM) {
            return true;
        } else if (rte->rtekind == RTE_SUBQUERY && pgxc_query_contains_foreign_table(rte->subquery->rtable))
            return true;
    }
    return false;
}

/*
 * pgxc_query_needs_coord
 * Check if the query needs Coordinator for evaluation or it can be completely
 * evaluated on Coordinator. Return true if so, otherwise return false.
 */
static bool pgxc_query_needs_coord(Query* query)
{
    /*
     * If the query is an EXEC DIRECT on the same Coordinator where it's fired,
     * it should not be shipped
     */
    if (query->is_local)
        return true;
    /*
     * If the query involves just the catalog tables, and is not an EXEC DIRECT
     * statement, it can be evaluated completely on the Coordinator. No need to
     * involve Datanodes.
     */
    if (NIL != query->rtable && pgxc_query_contains_only_pg_catalog(query->rtable))
        return true;

    if (NIL != query->rtable && pgxc_query_contains_foreign_table(query->rtable))
        return true;
    return false;
}

/*
 * Returns whether or not the rtable (and its subqueries)
 * only contain pg_catalog entries.
 */
static bool pgxc_query_contains_only_pg_catalog(List* rtable)
{
    ListCell* item = NULL;

    /* May be complicated. Before giving up, just check for pg_catalog usage */
    foreach (item, rtable) {
        RangeTblEntry* rte = (RangeTblEntry*)lfirst(item);

        if (rte->rtekind == RTE_RELATION) {
            if (!is_sys_table(rte->relid))
                return false;
        } else if (rte->rtekind == RTE_SUBQUERY && !pgxc_query_contains_only_pg_catalog(rte->subquery->rtable))
            return false;
    }
    return true;
}

/*
 * contains_subquery_walker
 * Sublink walker, returns true iff found.
 */
static bool contains_subquery_walker(Node* node, void* context)
{
    if (node == NULL) {
        return false;
    }

    contain_subquery_context* ctxt = (contain_subquery_context*)context;
    if (IsA(node, Query)) {
        Query* qry = (Query*)node;
        ctxt->subquery_exprs = lappend(ctxt->subquery_exprs, qry);

        /* Recurse into subquery */
        bool result = query_tree_walker((Query*)node, (bool (*)())contains_subquery_walker, context, 0);
        return result;
    }
    return expression_tree_walker(node, (bool (*)())contains_subquery_walker, context);
}

/*
 * contains_specified_sublink
 * Walke through node, find all sublinks. 
 * We don't have any specifications now, add them to the context if needed.
 */
List* contains_subquery(Node* node, contain_subquery_context* context)
{
    (void)query_or_expression_tree_walker(node, (bool (*)())contains_subquery_walker, (void*)context, 0);
    return context->subquery_exprs;
}

/* Init the contains_subquery_context */
contain_subquery_context init_contain_subquery_context()
{
    contain_subquery_context context;
    context.subquery_exprs = NIL;

    return context;
}

/*
 * make_params_transparent
 * Make the boundParams transparent to all subqueries, if exists.
 * @param query and current query level
 * @return nothing.
 */
static void make_params_transparent(Query* query, int query_level)
{
    ListCell* lc = NULL;
    List* subquery_list = NULL;

    /* If not the first level query */
    if (query_level != 0) {
        return;
    }

    contain_subquery_context context = init_contain_subquery_context();
    subquery_list = contains_subquery((Node*)query, &context);
    foreach (lc, subquery_list) {
        Query* tmp_subquery = (Query*)lfirst(lc);
        tmp_subquery->boundParamsQ = query->boundParamsQ;
    }
}

/*
 * pgxc_is_query_shippable
 * This function calls the query walker to analyse the query to gather
 * information like  Constraints under which the query can be shippable, nodes
 * on which the query is going to be executed etc.
 * Based on the information gathered, it decides whether the query can be
 * executed on Datanodes directly without involving Coordinator.
 * If the query is shippable this routine also returns the nodes where the query
 * should be shipped. If the query is not shippable, it returns NULL.
 */
ExecNodes* pgxc_is_query_shippable(
    Query* query, int query_level, bool light_proxy, bool* contain_column_store, bool* use_star_up)
{
    Shippability_context sc_context;
    ExecNodes* exec_nodes = NULL;
    bool canShip = true;
    Bitmapset* shippability = NULL;

    /* Support remotequery streamed for foreign table */
    if (pgxc_query_contains_foreign_table(query->rtable)) {
        return NULL;
    }

    /* Only bind row level security policies to plan on coordinator node */
    if (query->hasRowSecurity) {
        return NULL;
    }

    errno_t errorno = EOK;
    errorno = memset_s(&sc_context, sizeof(sc_context), '\0', sizeof(sc_context));
    securec_check(errorno, "", "");

    /* Initialize */
    sc_context.sc_inselect = false;
    /* let's assume that by default query is shippable */
    sc_context.sc_query = query;
    sc_context.sc_query_level = query_level;
    sc_context.sc_for_expr = false;
    sc_context.sc_light_proxy = light_proxy;
    /* check if contains cstore tables in the whole query */
    sc_context.sc_contain_column_store = (contain_column_store != NULL && *contain_column_store) ?
        true : contains_column_tables(query->rtable);
    sc_context.sc_use_star_upper_level = false;

    /* Make the boundParams available for all subqueries if needed */
    if (query->boundParamsQ != NULL) {
        make_params_transparent(query, query_level);
    }

    /*
     * We might have already decided not to ship the query to the Datanodes, but
     * still walk it anyway to find out if there are any subqueries which can be
     * shipped.
     */
    pgxc_shippability_walker((Node*)query, &sc_context);

#ifdef STREAMPLAN
    /*
     * For nextval function, we disable FQS, but should allow stream plan for bulkload
     * So we differentiate the unship reason for nextval from other function
     */
    if (pgxc_test_shippability_reason(&sc_context, SS_UNSHIPPABLE_FUNCTION)) {
        set_stream_off();
    }
#endif

    exec_nodes = sc_context.sc_exec_nodes;
    /*
     * The shippability context contains two ExecNodes, one for the subLinks
     * involved in the Query and other for the relation involved in FromClause.
     * They are computed at different times while scanning the query. Merge both
     * of them in pgxc_merge_exec_nodes. If query doesn't have SubLinks, we
     * don't need to consider corresponding ExecNodes.
     */
    if (query->hasSubLinks && exec_nodes) {
        if (sc_context.sc_subquery_en != NULL) {
            exec_nodes = pgxc_merge_exec_nodes(exec_nodes, sc_context.sc_subquery_en);
        } else {
            FreeExecNodes(&exec_nodes);
        }
    }

    /*
     * Look at the information gathered by the walker in Shippability_context and that
     * in the Query structure to decide whether we should ship this query
     * directly to the Datanode or not
     */

    /*
     * If the planner was not able to find the Datanodes to the execute the
     * query, the query is not completely shippable. So, return NULL
     */
    if (exec_nodes == NULL) {
        return NULL;
    }

    /* Copy the shippability reasons. We modify the copy for easier handling.
     * The original can be saved away */
    shippability = bms_copy(sc_context.sc_shippability);
    /*
     * If the query has an expression which renders the shippability to single
     * node, and query needs to be shipped to more than one node, it can not be
     * shipped.
     */
    if (bms_is_member(SS_NEED_SINGLENODE, shippability)) {
        /*
         * if nodeList has no nodes, it ExecNodes will have en_expr to know
         * the nodes where to execute. Given the values of en_expr, it is true
         * the query is executed on some single node.
         * If query has result replicated across nodes, it's as good as having a
         * single node.
         * If query had returning list, it is only shippable with (real) single node.
         */
        canShip = (list_length(exec_nodes->nodeList) > 1 &&
            (!IsExecNodesReplicated(exec_nodes) || query->returningList != NIL)) ?
            false : canShip;

        /* We handled the reason here, reset it */
        shippability = bms_del_member(shippability, SS_NEED_SINGLENODE);
    }

    /*
     * If HAS_AGG_EXPR is set but NEED_SINGLENODE is not set, it means the
     * aggregates are entirely shippable, so don't worry about it.
     */
    shippability = bms_del_member(shippability, SS_HAS_AGG_EXPR);

    if (!sc_context.sc_contain_column_store) {
        shippability = bms_del_member(shippability, SS_NEED_NO_CSTORE);
    }

    /* Can not ship the query for some reason */
    if (!bms_is_empty(shippability)) {
        canShip = false;
    }

    /* Always keep this at the end before checking canShip and return */
    if (!canShip && exec_nodes != NULL) {
        FreeExecNodes(&exec_nodes);
    }

    /* If query is to be shipped, we should know where to execute the query */
    AssertEreport(!canShip || exec_nodes, MOD_OPT, "the query must be shippable and exec node is null.");

    bms_free_ext(shippability);
    shippability = NULL;

    if (contain_column_store != NULL) {
        *contain_column_store = sc_context.sc_contain_column_store;
    }

    if (use_star_up != NULL) {
        *use_star_up = sc_context.sc_use_star_upper_level;
    }

    return exec_nodes;
}

/*
 * pgxc_is_expr_shippable
 * Check whether the given expression can be shipped to datanodes.
 *
 * Note on has_aggs
 * The aggregate expressions are not shippable if they can not be completely
 * evaluated on a single datanode. But this function does not have enough
 * context to determine the set of datanodes where the expression will be
 * evaluated. Hence, the caller of this function can handle aggregate
 * expressions, it passes a non-NULL value for has_aggs. This function returns
 * whether the expression has any aggregates or not through this argument. If a
 * caller passes NULL value for has_aggs, this function assumes that the caller
 * can not handle the aggregates and deems the expression has unshippable.
 */
bool pgxc_is_expr_shippable(Expr* node, bool* has_aggs)
{
    Shippability_context sc_context;

    /* Create the FQS context */
    errno_t errorno = EOK;
    errorno = memset_s(&sc_context, sizeof(sc_context), '\0', sizeof(sc_context));
    securec_check(errorno, "", "");

    sc_context.sc_query = NULL;
    sc_context.sc_query_level = 0;
    sc_context.sc_for_expr = true;

    /* Walk the expression to check its shippability */
    pgxc_shippability_walker((Node*)node, &sc_context);

    /*
     * If caller is interested in knowing, whether the expression has aggregates
     * let the caller know about it. The caller is capable of handling such
     * expressions. Otherwise assume such an expression as not shippable.
     */
    if (has_aggs != NULL) {
        *has_aggs = pgxc_test_shippability_reason(&sc_context, SS_HAS_AGG_EXPR);
    } else if (pgxc_test_shippability_reason(&sc_context, SS_HAS_AGG_EXPR)) {
        return false;
    }
    /* Done with aggregate expression shippability. Delete the status */
    pgxc_reset_shippability_reason(&sc_context, SS_HAS_AGG_EXPR);

    /* If there are reasons why the expression is unshippable, return false */
    if (!bms_is_empty(sc_context.sc_shippability)) {
        return false;
    }

    /* If nothing wrong found, the expression is shippable */
    return true;
}

bool pgxc_is_funcRTE_shippable(Expr* node)
{
    Shippability_context sc_context;
    errno_t rc;

    AssertEreport(NULL != node, MOD_OPT, "Expr must not be null.");

    /* Create the FQS context */
    rc = memset_s(&sc_context, sizeof(sc_context), 0, sizeof(sc_context));
    securec_check(rc, "", "");

    sc_context.sc_query = NULL;
    sc_context.sc_query_level = 0;
    sc_context.sc_for_expr = false;

    /* Walk the expression to check its shippability */
    pgxc_shippability_walker((Node*)node, &sc_context);

    /* If nothing wrong found, the expression is shippable */
    if (bms_is_empty(sc_context.sc_shippability))
        return true;

    /* If there are reasons why the expression is unshippable, return false */
    return false;
}

/*
 * Internal functions.They can not be called as SQL functions.
 * string_agg_finalfn bytea_string_agg_finalfn array_agg_finalfn
 */
bool pgxc_is_internal_agg_final_func(Oid funcid)
{
    bool is_internal_func = true;
    switch (funcid) {
        case ARRAYAGGFUNCOID:                  // array_agg_finalfn
        case STRINGAGGFUNCOID:                 // string_agg_finalfn
        case BYTEASTRINGAGGFUNCOID:            // bytea_string_agg_finalfn
        case LISTAGGFUNCOID:                   // listagg_finalfn
        case LISTAGGNOARG2FUNCOID:             // nodelimiter_listagg_finalfn
        case INT2LISTAGGFUNCOID:               // int2_listagg_finalfn
        case INT2LISTAGGNOARG2FUNCOID:         // int2_listagg_noarg2_finalfn
        case INT4LISTAGGFUNCOID:               // int4_listagg_finalfn
        case INT4LISTAGGNOARG2FUNCOID:         // int4_listagg_noarg2_finalfn
        case INT8LISTAGGFUNCOID:               // int8_listagg_finalfn
        case INT8LISTAGGNOARG2FUNCOID:         // int8_listagg_noarg2_finalfn
        case FLOAT4LISTAGGFUNCOID:             // float4_listagg_finalfn
        case FLOAT4LISTAGGNOARG2FUNCOID:       // float4_listagg_noarg2_finalfn
        case FLOAT8LISTAGGFUNCOID:             // float8_listagg_finalfn
        case FLOAT8LISTAGGNOARG2FUNCOID:       // float8_listagg_noarg2_finalfn
        case NUMERICLISTAGGFUNCOID:            // numeric_listagg_finalfn
        case NUMERICLISTAGGNOARG2FUNCOID:      // numeric_listagg_noarg2_finalfn
        case DATELISTAGGFUNCOID:               // date_listagg_finalfn
        case DATELISTAGGNOARG2FUNCOID:         // date_listagg_noarg2_finalfn
        case TIMESTAMPLISTAGGFUNCOID:          // timestamp_listagg_finalfn
        case TIMESTAMPLISTAGGNOARG2FUNCOID:    // timestamp_listagg_noarg2_finalfn
        case TIMESTAMPTZLISTAGGFUNCOID:        // timestamptz_listagg_finalfn
        case TIMESTAMPTZLISTAGGNOARG2FUNCOID:  // timestamptz_listagg_noarg2_finalfn
        case INTERVALLISTAGGFUNCOID:           // interval_listagg_finalfn
        case INTERVALLISTAGGNOARG2FUNCOID:     // interval_listagg_noarg2_finalfn
        case JSONAGGFUNCOID:                   // json_agg_finalfn
        case JSONOBJECTAGGFUNCOID:             // json_object_agg_finalfn
            is_internal_func = false;
            break;
        default:
            break;
    }

    return is_internal_func;
}

bool is_avg_func(Oid funcid)
{
    switch (funcid) {
        case INT8AVGFUNCOID:
        case INT4AVGFUNCOID:
        case INT2AVGFUNCOID:
        case INT1AVGFUNCOID:
        case NUMERICAVGFUNCOID:
        case FLOAT4AVGFUNCOID:
        case FLOAT8AVGFUNCOID:
        case INTERVALAVGFUNCOID:
        case INTERVALAGGAVGFUNCOID:
            return true;
        default:
            break;
    }

    return false;
}

/*
 * Determine if the function arguments include ANY type. These function arguments
 * will be expanded to check if contains expression that can not be shipped.
 */
static Oid shippable_func_contain_ANY[] = {
    CONCATFUNCOID, CONCATWSFUNCOID, PGTYPEOFFUNCOID, TEXTANYCATFUNCOID, ANYTEXTCATFUNCOID, ANYTOTEXTFORMATFUNCOID
};

bool pgxc_is_shippable_func_contain_any(Oid funcid)
{
    for (uint i = 0; i < lengthof(shippable_func_contain_ANY); i++) {
        if (funcid == shippable_func_contain_ANY[i]) {
            return true;
        }
    }
    return false;
}

/*
 * pgxc_is_func_shippable
 * Determine if a function is shippable
 */
bool pgxc_is_func_shippable(Oid funcid, shipping_context* context)
{
    bool proshippable = false;
    char provolatile = func_volatile(funcid);

    proshippable = get_func_proshippable(funcid);

    /*
     * For the time being a function is thought as shippable
     * 1. it is immutable.
     * 2. it is in shippable_stablefunc
     * 3. some special volatile func.
     */

    if (PROVOLATILE_IMMUTABLE == provolatile) {
        return true;
    } else if (PROVOLATILE_STABLE == provolatile) {
        /*
         * For online expansion, we have to force some function shipple to DN e.g. get
         * start/end tupleid.
         */
        if (redis_func_shippable(funcid)) {
            return true;
        }
#ifdef ENABLE_MULTIPLE_NODES
        if (vector_search_func_shippable(funcid)) {
            return true;
        }
#endif
        if (proshippable) {
            return true;
        }
    } else if (PROVOLATILE_VOLATILE == provolatile && context != NULL && !context->disallow_volatile_func_shippable) {
        switch (funcid) {
            case GSENCRYPTAES128FUNCOID:
            case RANDOMFUNCOID:
                return context->is_randomfunc_shippable;
                break;

            case NEXTVALFUNCOID:
                return context->is_nextval_shippable;
                break;

            case ECEXTENSIONFUNCOID:
            case ECHADOOPFUNCOID:
                return context->is_ecfunc_shippable;
                break;

            case PGSYSTIMESTAMPFUNCOID:
            case TIMEZONETZFUNCOID:
                return true;
                break;

            default:
                break;
        }

        if (proshippable) {
            return true;
        }
    }

    return false;
}

/*
 * pgxc_find_dist_equijoin_qual
 * Check equijoin conditions on given relations
 */
List* pgxc_find_dist_equijoin_qual(List* dist_vars1, List* dist_vars2, Node* quals)
{
    List* lquals = NIL;
    List* dist_1 = NIL;
    List* dist_2 = NIL;
    ListCell* qcell = NULL;
    ListCell* cell_1 = NULL;
    ListCell* cell_2 = NULL;
    bool isFineAllDisKey = true;
    List* qual_expr_list = NIL;

    /* If no quals, no equijoin */
    if (quals == NULL)
        return NIL;

    if (!IsA(quals, List))
        lquals = make_ands_implicit((Expr*)quals);
    else
        lquals = (List*)quals;

    foreach (cell_1, dist_vars1) {
        dist_1 = (List*)lfirst(cell_1);
        foreach (cell_2, dist_vars2) {
            isFineAllDisKey = true;
            dist_2 = (List*)lfirst(cell_2);
            if (list_length(dist_1) != list_length(dist_2)) {
                continue;
            }

            ListCell *cell1 = NULL, *cell2 = NULL;
            forboth(cell1, dist_1, cell2, dist_2)
            {
                Var* lDistVar = (Var*)lfirst(cell1);
                Var* rDistVar = (Var*)lfirst(cell2);

                foreach (qcell, lquals) {
                    Expr* qual_expr = (Expr*)lfirst(qcell);
                    OpExpr* op = NULL;
                    Var* lvar = NULL;
                    Var* rvar = NULL;

                    if (!IsA(qual_expr, OpExpr))
                        continue;
                    op = (OpExpr*)qual_expr;
                    /* If not a binary operator, it can not be '='. */
                    if (list_length(op->args) != 2)
                        continue;
                    /*
                     * Check if both operands are Vars or RelabelType,if are RelabelType,we check the args.
                     * otherwise check next expression
                     */
                    lvar = locate_distribute_var((Expr*)linitial(op->args));
                    rvar = locate_distribute_var((Expr*)lsecond(op->args));
                    if (NULL == lvar || NULL == rvar)
                        continue;

                    /*
                     * If the data types of both the columns are not same, continue. Hash
                     * and Modulo of a the same bytes will be same if the data types are
                     * same. So, only when the data types of the columns are same, we can
                     * ship a distributed JOIN to the Datanodes
                     */
                    if (exprType((Node*)lvar) != exprType((Node*)rvar))
                        continue;

                    if (!((equal(lDistVar, lvar) && equal(rDistVar, rvar)) ||
                        ((equal(lDistVar, rvar) && equal(rDistVar, lvar)))))
                        continue;
                    /*
                     * If the operator is not an assignment operator, check next
                     * constraint. An operator is an assignment operator if it's
                     * mergejoinable or hashjoinable. Beware that not every assignment
                     * operator is mergejoinable or hashjoinable, so we might leave some
                     * oportunity. But then we have to rely on the opname which may not
                     * be something we know to be equality operator as well.
                     */
                    if (!op_mergejoinable(op->opno, exprType((Node*)lvar)) &&
                        !op_hashjoinable(op->opno, exprType((Node*)lvar)))
                        continue;
                    /* Found equi-join condition on distribution columns */
                    qual_expr_list = lappend(qual_expr_list, qual_expr);
                    break;
                }
                if (qcell == NULL) {
                    list_free_ext(qual_expr_list);
                    qual_expr_list = NULL;
                    isFineAllDisKey = false;
                    break;
                }
            }
            if (isFineAllDisKey) {
                return qual_expr_list;
            }
        }
    }
    return NULL;
}

/*
 * pgxc_merge_exec_nodes
 * The routine combines the two exec_nodes passed such that the resultant
 * exec_node corresponds to the JOIN of respective relations.
 * If both exec_nodes can not be merged, it returns NULL.
 */
ExecNodes* pgxc_merge_exec_nodes(ExecNodes* en1, ExecNodes* en2, int jointype)
{
    ExecNodes* merged_en = (ExecNodes*)makeNode(ExecNodes);
    ExecNodes* tmp_en = NULL;

    /* If either of exec_nodes are NULL, return the copy of other one */
    if (en1 == NULL) {
        tmp_en = (ExecNodes*)copyObject(en2);
        return tmp_en;
    }
    if (en2 == NULL) {
        tmp_en = (ExecNodes*)copyObject(en1);
        return tmp_en;
    }

    /*
     * Push join down when both en_expr are same, like nodelist.
     * For now, we can only deal with one value in en_expr.
     * Todo: when there are more values in en_expr, we need to compare the two lists
     * to make sure they will go to the same datanode/s.
     */
    bool single_expr_en1 =
        (en1->en_expr != NIL && IsExecNodesColumnDistributed(en1) && en1->accesstype == RELATION_ACCESS_READ);
    bool single_expr_en2 =
        (en2->en_expr != NIL && IsExecNodesColumnDistributed(en2) && en2->accesstype == RELATION_ACCESS_READ);
    bool single_node_en1 = (IsExecNodesColumnDistributed(en1) && list_length(en1->nodeList) == 1);
    bool single_node_en2 = (IsExecNodesColumnDistributed(en2) && list_length(en2->nodeList) == 1);
    bool has_list_range_node = IsExecNodesDistributedBySlice(en1) || IsExecNodesDistributedBySlice(en2);

    /* If any of node1 or node2 contains parameters, we check whether them can be merged.
     * For hash, we only check the node; for list/range, we should make sure they are from same distribution too. */
    if (!has_list_range_node || (has_list_range_node && PgxcIsDistInfoSame(en1, en2))) {
        if (single_expr_en1 && single_expr_en2) {
            /* must to be the same node group */
            if (ng_is_same_group(&en1->distribution, &en2->distribution)) {
                tmp_en = (ExecNodes*)copyObject(en1);
                /* If pram is different, we save it in dynamic_expr, and judge the value later */
                if (list_difference(en1->en_expr, en2->en_expr)) {
                    tmp_en->dynamic_en_expr = list_append_unique(tmp_en->dynamic_en_expr, linitial(en1->en_expr));
                    tmp_en->dynamic_en_expr = list_append_unique(tmp_en->dynamic_en_expr, linitial(en2->en_expr));
                }
            } else
                tmp_en = NULL;
            return tmp_en;
        } else if (jointype >= 0 &&
                   ((RHS_join(jointype) && single_expr_en1 && !single_node_en2) ||
                   (LHS_join(jointype) && !single_node_en1 && single_expr_en2)) &&
                   ng_is_same_group(&en1->distribution, &en2->distribution)) {
            tmp_en = single_expr_en1 ? (ExecNodes*)copyObject(en1) : (ExecNodes*)copyObject(en2);
            return tmp_en;
        }
    }

    /* Following cases are not handled in this routine */
    if (en1->primarynodelist || en2->primarynodelist || en1->en_expr || en2->en_expr || OidIsValid(en1->en_relid) ||
        OidIsValid(en2->en_relid) || en2->accesstype != RELATION_ACCESS_READ)
        return NULL;

    if (IsExecNodesReplicated(en1) && IsExecNodesReplicated(en2)) {
        /*
         * Replicated/replicated join case
         * Check that replicated relation is not disjoint
         * with initial relation which is also replicated.
         * If there is a common portion of the node list between
         * the two relations, other rtables have to be checked on
         * this restricted list.
         */
        if (en1->accesstype != RELATION_ACCESS_READ) {
            FreeExecNodes(&merged_en);
            return NULL;
        }

        merged_en->nodeList = list_intersection_int(en1->nodeList, en2->nodeList);

        Oid group_oid = InvalidOid;
        if (!list_difference_int(en1->nodeList, en2->nodeList) && !list_difference_int(en2->nodeList, en1->nodeList)) {
            group_oid =
                (InvalidOid != en1->distribution.group_oid) ? en1->distribution.group_oid : en2->distribution.group_oid;
        }
        Distribution* distribution = ng_convert_to_distribution(merged_en->nodeList);
        ng_set_distribution(&merged_en->distribution, distribution);
        merged_en->distribution.group_oid = group_oid;

        merged_en->baselocatortype = LOCATOR_TYPE_REPLICATED;
        if (!merged_en->nodeList)
            FreeExecNodes(&merged_en);
        return merged_en;
    }

    if (IsExecNodesReplicated(en1) && IsExecNodesColumnDistributed(en2)) {
        List* diff_nodelist = NULL;
        if (en1->accesstype != RELATION_ACCESS_READ) {
            FreeExecNodes(&merged_en);
            return NULL;
        }
        /*
         * Replicated/distributed join case.
         * Node list of distributed table has to be included
         * in node list of replicated table.
         */
        diff_nodelist = list_difference_int(en2->nodeList, en1->nodeList);
        /*
         * If the difference list is not empty, this means that node list of
         * distributed table is not completely mapped by node list of replicated
         * table, so go through standard planner.
         */
        if (diff_nodelist != NULL)
            FreeExecNodes(&merged_en);
        else {
            merged_en->nodeList = list_copy(en2->nodeList);
            ng_copy_distribution(&merged_en->distribution, &en2->distribution);
            merged_en->baselocatortype = en2->baselocatortype;
            merged_en->en_dist_vars = en2->en_dist_vars;
            if (IsLocatorDistributedBySlice(en2->baselocatortype)) {
                merged_en->rangelistOid = en2->rangelistOid;
            }
        }
        return merged_en;
    }

    if (IsExecNodesColumnDistributed(en1) && IsExecNodesReplicated(en2)) {
        List* diff_nodelist = NULL;
        /*
         * Distributed/replicated join case.
         * Node list of distributed table has to be included
         * in node list of replicated table.
         */
        diff_nodelist = list_difference_int(en1->nodeList, en2->nodeList);
        /*
         * If the difference list is not empty, this means that node list of
         * distributed table is not completely mapped by node list of replicated
         * table, so go through standard planner.
         */
        if (diff_nodelist != NULL)
            FreeExecNodes(&merged_en);
        else {
            merged_en->nodeList = list_copy(en1->nodeList);
            ng_copy_distribution(&merged_en->distribution, &en1->distribution);
            merged_en->baselocatortype = en1->baselocatortype;
            merged_en->en_dist_vars = en1->en_dist_vars;
            if (IsLocatorDistributedBySlice(en1->baselocatortype)) {
                merged_en->rangelistOid = en1->rangelistOid;
            }
        }
        return merged_en;
    }

    if (IsExecNodesColumnDistributed(en1) && IsExecNodesColumnDistributed(en2)) {
        /*
         * Distributed/distributed case
         * If the caller has suggested that this is an equi-join between two
         * distributed results, check that they have the same nodes in the distribution
         * node list. The caller is expected to fully decide whether to merge
         * the nodes or not.
         */
        bool merged_ok = false;

        /* if distribution type or slice clause or slice-dn map are not same, can't merge */
        if (IsLocatorDistributedBySlice(en1->baselocatortype) || IsLocatorDistributedBySlice(en2->baselocatortype)) {
            if (!PgxcIsDistInfoSame(en1, en2)) {
                FreeExecNodes(&merged_en);
                return merged_en;
            }

            /* pick any one, here we choose en1 */
            merged_en->rangelistOid = en1->rangelistOid;
        }

        if (!list_difference_int(en1->nodeList, en2->nodeList) && !list_difference_int(en2->nodeList, en1->nodeList) &&
            ng_is_same_group(&en1->distribution, &en2->distribution)) {
            merged_en->nodeList = list_copy(en1->nodeList);

            merged_ok = true;
        } else if (jointype >= 0 && ng_is_same_group(&en1->distribution, &en2->distribution) &&
                   ((RHS_join(jointype) && single_node_en1 && !single_node_en2) ||
                   (LHS_join(jointype) && !single_node_en1 && single_node_en2))) {
            /* Consider this:
             *
             * (1) bmsql_district distribute by (d_w_id)
             * (2) bmsql_order_line distribute by (ol_w_id)
             * (3) and we have a join condition on distribute key: ol_w_id = d_w_id
             * (4) adn we have a where clause d_w_id = 1
             *
             * the query looks like this:
             *
             * SELECT ol_i_id
             *   FROM bmsql_district
             *   JOIN bmsql_order_line ON ol_w_id = d_w_id
             *   WHERE d_w_id = 1;
             *
             * the join can be shipped to all datanodes using condition (1)(2)(3)
             * while condition (4) can force the join to ONE dn
             *
             */
            merged_en->nodeList = single_node_en1 ? list_copy(en1->nodeList) : list_copy(en2->nodeList);

            merged_ok = true;
        }

        if (merged_ok) {
            Distribution* distribution =
                (InvalidOid != en1->distribution.group_oid) ? &en1->distribution : &en2->distribution;
            ng_copy_distribution(&merged_en->distribution, distribution);
            if (en1->baselocatortype == en2->baselocatortype) {
                merged_en->baselocatortype = en1->baselocatortype;
                merged_en->en_dist_vars = list_concat(list_copy(en1->en_dist_vars), list_copy(en2->en_dist_vars));
            } else
                merged_en->baselocatortype = LOCATOR_TYPE_DISTRIBUTED;
        } else {
            FreeExecNodes(&merged_en);
        }

        return merged_en;
    }

    ereport(ERROR,
        (errmodule(MOD_OPT),
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("openGauss does not support this distribution type yet"),
                errdetail("The feature is not currently supported"))));

    /* Keep compiler happy */
    return NULL;
}

/*
 * pgxc_check_index_shippability
 * Check shippability of index described by given conditions. This generic
 * function can be called even if the index is not yet defined.
 */
bool pgxc_check_index_shippability(
    RelationLocInfo* relLocInfo, bool is_primary, bool is_unique, bool is_exclusion, List* indexAttrs, List* indexExprs)
{
    bool result = true;
    ListCell* lc = NULL;

    /*
     * Leave if no locator information, in this case shippability has no
     * meaning.
     */
    if (relLocInfo == NULL)
        return result;

    /*
     * Scan the expressions used in index and check the shippability of each
     * of them. If only one is not-shippable, the index is considered as non
     * shippable. It is important to check the shippability of the expressions
     * before refining scan on the index columns and distribution type of
     * parent relation.
     */
    foreach (lc, indexExprs) {
        if (!pgxc_is_expr_shippable((Expr*)lfirst(lc), NULL)) {
            /* One of the expressions is not shippable, so leave */
            result = false;
            goto finish;
        }
    }

    /*
     * Check if relation is distributed on a single node, in this case
     * the constraint can be shipped in all the cases.
     */
    if (list_length(relLocInfo->nodeList) == 1)
        return result;

    /*
     * Check the case of EXCLUSION index.
     * EXCLUSION constraints are shippable only for replicated relations as
     * such constraints need that one tuple is checked on all the others, and
     * if this tuple is correctly excluded of the others, the constraint is
     * verified.
     */
    if (is_exclusion) {
        if (!IsRelationReplicated(relLocInfo)) {
            result = false;
            goto finish;
        }
    }

    /*
     * Check the case of PRIMARY KEY INDEX and UNIQUE index.
     * Those constraints are shippable if the parent relation is replicated
     * or if the column
     */
    if (is_unique || is_primary) {
        /*
         * Perform different checks depending on distribution type of parent
         * relation.
         */
        switch (relLocInfo->locatorType) {
            case LOCATOR_TYPE_REPLICATED:
                /* In the replicated case this index is shippable */
                result = true;
                break;

            case LOCATOR_TYPE_RROBIN: {
                Relation rel = RelationIdGetRelation(relLocInfo->relid);
                if (!RelationIsValid(rel)) {
                    ereport(ERROR,
                        (errmodule(MOD_OPT),
                            errcode(ERRCODE_UNDEFINED_OBJECT),
                            errmsg("could not open relation with OID %u", relLocInfo->relid)));
                }
                /*
                 * Index on roundrobin parent table cannot be safely shipped
                 * because of the random behavior of data balancing.
                 * But, the hdfs foreign table support informational constraint.
                 */
                result = false;
                if (RELKIND_FOREIGN_TABLE == rel->rd_rel->relkind || RELKIND_STREAM == rel->rd_rel->relkind) {
                    ForeignTable* ftbl = NULL;
                    ForeignServer* fsvr = NULL;
                    Oid relationId = RelationGetRelid(rel);
                    ftbl = GetForeignTable(relationId);
                    if (NULL == ftbl)
                        ereport(ERROR,
                            (errmodule(MOD_OPT),
                                errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                                (errmsg("Failed to get foreign table."))));

                    fsvr = GetForeignServer(ftbl->serverid);
                    if (NULL == fsvr)
                        ereport(ERROR,
                            (errmodule(MOD_OPT),
                                errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                                (errmsg("Failed to get foreign server."))));

                    if (isObsOrHdfsTableFormSrvName(fsvr->servername) ||
                        IS_OBS_CSV_TXT_FOREIGN_TABLE(relLocInfo->relid)) {
                        result = true;
                    }
                }
                RelationClose(rel);
                break;
            }

            case LOCATOR_TYPE_RANGE:
            case LOCATOR_TYPE_LIST:
            case LOCATOR_TYPE_HASH:
            case LOCATOR_TYPE_MODULO: {
                /*
                 * Unique indexes on Hash and Modulo tables are shippable if the
                 * index expression contains all the distribution expressions of
                 * its parent relation.
                 *
                 * Here is a short example with concatenate that cannot be
                 * shipped:
                 * CREATE TABLE aa (a text, b text) DISTRIBUTE BY HASH(a);
                 * CREATE UNIQUE INDEX aap ON aa((a || b));
                 * INSERT INTO aa VALUES ('a', 'abb');
                 * INSERT INTO aa VALUES ('aab', b); -- no error ??!
                 * The output uniqueness is not guaranteed as both INSERT will
                 * go to different nodes. For such simple reasons unique
                 * indexes on distributed tables are not shippable.
                 * Shippability is not even ensured if all the expressions
                 * used as Var are only distributed columns as the hash output of
                 * their value combination does not ensure that query will
                 * be directed to the correct remote node. Uniqueness is not even
                 * protected if the index expression contains only the distribution
                 * column like for that with a cluster of 2 Datanodes:
                 * CREATE TABLE aa (a int) DISTRIBUTE BY HASH(a);
                 * CREATE UNIQUE INDEX aap ON (abs(a));
                 * INSERT INTO aa (2); -- to Datanode 1
                 * INSERT INTO aa (-2); -- to Datanode 2, breaks uniqueness
                 *
                 * For the time being distribution key can only be
                 * defined on a single column, so this will need to be changed
                 * onde a relation distribution will be able to be defined based
                 * on an expression of multiple columns.
                 */

                /* Index contains expressions, it cannot be shipped safely */
                if (indexExprs != NIL) {
                    result = false;
                    break;
                }
                /* Nothing to do if no attributes */
                if (indexAttrs == NIL)
                    break;

                /*
                 * Check that distribution column is included in the list of
                 * index columns.
                 */
                ListCell* cell = NULL;
                AttrNumber attnum;
                foreach (cell, relLocInfo->partAttrNum) {
                    attnum = lfirst_int(cell);
                    if (!list_member_int(indexAttrs, attnum)) {
                        /*
                         * Distribution column is not in index column list
                         * So index can be enforced remotely.
                         */
                        result = false;
                        break;
                    }
                }

                /*
                 * by being here we are now sure that the index can be enforced
                 * remotely as the distribution column is included in index.
                 */
                break;
            }
            /* Those types are not supported yet */
            case LOCATOR_TYPE_NONE:
            case LOCATOR_TYPE_DISTRIBUTED:
            case LOCATOR_TYPE_CUSTOM:
            default:
                /* Should not come here */
                Assert(0);
        }
    }

finish:
    return result;
}

/*
 * pgxc_check_fk_shippabilily
 * Check the shippability of a parent and a child relation based on the
 * distribution of each and the columns that are used to reference to
 * parent and child relation. This can be used for inheritance or foreign
 * key shippability evaluation.
 */
bool pgxc_check_fk_shippability(
    RelationLocInfo* parentLocInfo, RelationLocInfo* childLocInfo, List* parentRefs, List* childRefs)
{
    bool result = true;

    AssertEreport(list_length(parentRefs) == list_length(childRefs),
        MOD_OPT,
        "shippability of parent and child relation check failed.");

    /*
     * If either child or parent have no relation data, shippability makes
     * no sense.
     */
    if (parentLocInfo == NULL || childLocInfo == NULL)
        return result;

    /* In the case of a child referencing to itself, constraint is shippable */
    if (IsLocatorInfoEqual(parentLocInfo, childLocInfo))
        return result;

    /* Now begin the evaluation */
    switch (parentLocInfo->locatorType) {
        case LOCATOR_TYPE_REPLICATED:
            /*
             * If the parent relation is replicated, the child relation can
             * always refer to it on all the nodes.
             */
            result = true;
            break;

        case LOCATOR_TYPE_RROBIN:
            /*
             * If the parent relation is based on roundrobin, the child
             * relation cannot be enforced on remote nodes before of the
             * random behavior of data balancing.
             */
            result = false;
            break;

        case LOCATOR_TYPE_RANGE:
        case LOCATOR_TYPE_LIST:
        case LOCATOR_TYPE_HASH:
        case LOCATOR_TYPE_MODULO: {
            /*
             * If parent table is distributed, the child table can reference
             * to its parent safely if the following conditions are satisfied:
             * - parent and child are both hash-based, or both modulo-based
             * - parent reference columns contain the distribution column
             *   of the parent relation
             * - child reference columns contain the distribution column
             *   of the child relation
             * - both child and parent map the same nodes for data location
             */

            /* A replicated child cannot refer to a distributed parent */
            if (IsRelationReplicated(childLocInfo)) {
                result = false;
                break;
            }

            /*
             * Parent and child need to have the same distribution type:
             * hash or modulo.
             */
            if (parentLocInfo->locatorType != childLocInfo->locatorType) {
                result = false;
                break;
            }

            /*
             * Parent and child need to have their data located exactly
             * on the same list of nodes.
             */
            if (list_difference_int(childLocInfo->nodeList, parentLocInfo->nodeList) ||
                list_difference_int(parentLocInfo->nodeList, childLocInfo->nodeList)) {
                result = false;
                break;
            }

            /*
             * Check that child and parents are referenced using their
             * distribution column.
             */

            if (list_length(childLocInfo->partAttrNum) != list_length(parentLocInfo->partAttrNum)) {
                result = false;
                break;
            }

            ListCell* cell1 = NULL;
            ListCell* cell2 = NULL;
            AttrNumber attnum1, attnum2;

            forboth(cell1, childLocInfo->partAttrNum, cell2, parentLocInfo->partAttrNum)
            {
                attnum1 = lfirst_int(cell1);
                attnum2 = lfirst_int(cell2);
                if (!list_member_int(childRefs, attnum1) || !list_member_int(parentRefs, attnum2)) {
                    result = false;
                    break;
                }

                int i1 = 0, i2 = 0;
                ListCell* lc = NULL;

                foreach (lc, childRefs) {
                    if (lfirst_int(lc) == attnum1)
                        break;
                    i1++;
                }

                foreach (lc, parentRefs) {
                    if (lfirst_int(lc) == attnum2)
                        break;
                    i2++;
                }

                if (i1 != i2) {
                    result = false;
                    break;
                }
            }

            if (IsLocatorDistributedBySlice(parentLocInfo->locatorType)) {
                /*
                 * if parent and child slice definition and slice-dn map are the same,
                 * then parent-child constraint can be shipped.
                 */
                if (!IsSliceInfoEqualByOid(parentLocInfo->relid, childLocInfo->relid)) {
                    result = false;
                }
            }
            /* By being here, parent-child constraint can be shipped correctly */
            break;
        }
        case LOCATOR_TYPE_NONE:
        case LOCATOR_TYPE_DISTRIBUTED:
        case LOCATOR_TYPE_CUSTOM:
        default:
            /* Should not come here */
            Assert(0);
    }

    return result;
}

/*
 * pgxc_replace_dist_vars_subquery
 * The function looks up the members of ExecNodes::en_dist_var in the
 * query->targetList. If found, they are re-stamped with the given varno and
 * resno of the TargetEntry found and added to the new distribution var list
 * being created. This function is useful to re-stamp the distribution columns
 * of a subquery.
 */
static void pgxc_replace_dist_vars_subquery(Query* query, ExecNodes* exec_nodes, Index varno)
{
    ListCell* l_cell = NULL;
    ListCell* lcell = NULL;
    List* new_dist_vars = NIL;
    List* dis_list = NIL;
    List* tmp_l = NIL;

    foreach (l_cell, exec_nodes->en_dist_vars) {
        dis_list = (List*)lfirst(l_cell);
        foreach (lcell, dis_list) {
            Var* var = (Var*)lfirst(lcell);
            TargetEntry* tle = NULL;

            AssertEreport(IsA(var, Var), MOD_OPT, "targetlist type wrong.");

            tle = tlist_member((Node*)var, query->targetList);
            if (tle != NULL) {
                Var* new_dist_var = makeVar(varno,
                    tle->resno,
                    exprType((Node*)tle->expr),
                    exprTypmod((Node*)tle->expr),
                    exprCollation((Node*)tle->expr),
                    0);
                new_dist_vars = lappend(new_dist_vars, new_dist_var);
            } else {
                break;
            }
        }
        if (lcell == NULL) {
            if (new_dist_vars != NIL) {
                tmp_l = lappend(tmp_l, new_dist_vars);
            }
        } else {
            list_free_deep(new_dist_vars);
        }
        new_dist_vars = NIL;
    }
    list_free_deep(exec_nodes->en_dist_vars);
    exec_nodes->en_dist_vars = tmp_l;
}

/*
 * pgxc_get_dist_var
 * Given the varno, corresponding range table entry and targetlist, get the Var
 * node for distribution column, present in the targetlist as the root of
 * expression if there is one; otherwise return one created.
 *
 * If it's a replicated table or table local to the coordinator, or
 * any relation other than distributed table it returns NULL.
 *
 * varno: is the index of the range table entry in the query range table, to be
 *    		set as Var::varno
 * rte:   range table entry corresponding to varno. There is no way to verify
 * 			where the correspondence is true.
 * tlist: target list or just the list of expression where to find the Var
 * 			corresponding to the distribution column
 */
List* pgxc_get_dist_var(Index varno, RangeTblEntry* rte, List* tlist)
{
    RelationLocInfo* rel_loc_info = GetRelationLocInfo(rte->relid);
    ListCell* lcell = NULL;
    Var* dist_var = NULL;
    Oid dist_var_type;
    int32 dist_var_typmod;
    Oid dist_var_collid;
    List* varList = NULL;

    if (rel_loc_info == NULL || !IsRelationDistributedByValue(rel_loc_info))
        return NULL;

    /* find the TLE corresponding to the distribution column it. */
    ListCell* cell = NULL;
    foreach (cell, rel_loc_info->partAttrNum) {
        AttrNumber attnum = lfirst_int(cell);
        bool isMatch = false;

        foreach (lcell, tlist) {
            TargetEntry* tle = (TargetEntry*)lfirst(lcell);
            Var* var = NULL;
            if (tle && IsA(tle, TargetEntry))
                var = (Var*)tle->expr;
            else
                var = (Var*)tle;

            if (var && IsA(var, Var) && (var->varno == varno) && (var->varattno == attnum)) {
                isMatch = true;
                varList = lappend(varList, (Var*)copyObject(var));
                break;
            }
        }
        if (isMatch == false) {
            /*
             * Bare distribution column is not found in the targetlist, craft a Var for
             * it.
             */
            get_rte_attribute_type(rte, attnum, &dist_var_type, &dist_var_typmod, &dist_var_collid);

            dist_var = makeVar(varno, attnum, dist_var_type, dist_var_typmod, dist_var_collid, 0);

            varList = lappend(varList, dist_var);
        }

        isMatch = false;
    }

    return varList;
}

static bool PgxcIsDistInfoSame(ExecNodes *innerEn, ExecNodes *outerEn)
{
    if (innerEn->baselocatortype != outerEn->baselocatortype) {
        return false;
    }

    return IsSliceInfoEqualByOid(innerEn->rangelistOid, outerEn->rangelistOid);
}

/*
 * pgxc_is_join_shippable
 * The shippability of JOIN is decided in following steps
 * 1. Are the JOIN conditions shippable?
 * 	For INNER JOIN it's possible to apply some of the conditions at the
 * 	Datanodes and others at coordinator. But for other JOINs, JOIN conditions
 * 	decide which tuples on the OUTER side are appended with NULL columns from
 * 	INNER side, we need all the join conditions to be shippable for the join to
 * 	be shippable.
 * 2. Do the JOIN conditions have quals that will make it shippable?
 * 	When both sides of JOIN are replicated, irrespective of the quals the JOIN
 * 	is shippable.
 * 	INNER joins between replicated and distributed relation are shippable
 * 	irrespective of the quals. OUTER join between replicated and distributed
 * 	relation is shippable if distributed relation is the outer relation.
 * 	All joins between hash/modulo distributed relations are shippable if they
 * 	have equi-join on the distributed column, such that distribution columns
 * 	have same datatype and same distribution strategy.
 * 3. Are datanodes where the joining relations exist, compatible?
 * 	Joins between replicated relations are shippable if both relations share a
 * 	datanode. Joins between distributed relations are shippable if both
 * 	relations are distributed on same set of Datanodes. Join between replicated
 * 	and distributed relations is shippable is replicated relation is replicated
 * 	on all nodes where distributed relation is distributed.
 * 4. Are targetlists of both sides shippable?
 *  For OUTER Joins if there is at least one unshippable entry in the targetlist
 *  of the relation which contributes NULL columns in the join result, the join
 *  is not shippable. In such cases, the unshippable expression is projected at
 *  the coordinator, thus causing a non-NULL value to appear instead of NULL
 *  value in the result.
 *
 * The first step is to be applied by the caller of this function.
 */
ExecNodes* pgxc_is_join_shippable(ExecNodes* inner_en, ExecNodes* outer_en, bool inner_unshippable_tlist,
    bool outer_unshippable_tlist, JoinType jointype, Node* join_quals)
{
    bool merge_nodes = false;
    List* dist_vars = NIL;

    /*
     * If either of inner_en or outer_en is NULL, return NULL. We can't ship the
     * join when either of the sides do not have datanodes to ship to.
     */
    if (outer_en == NULL || inner_en == NULL)
        return NULL;
    /*
     * We only handle reduction of INNER, LEFT [OUTER], RIGHT [OUTER] and
     * FULL [OUTER] joins.
     */
    if (jointype != JOIN_INNER && jointype != JOIN_LEFT && jointype != JOIN_RIGHT && jointype != JOIN_FULL)
        return NULL;

    /*
     * For left outer join, if the inner relation (for which null columns are
     * added if there is a row unmatched from outer join), has unshippable
     * targetlist entry, we can not ship the join. This is because, the unshippable
     * targetlist entry needs to be calculated before it can be added to the
     * JOIN result, either as NULL or non-NULL.
     * Similarly for FULL OUTER Join, none of the sides should have unshippable
     * targetlist expression.
     */
    if (jointype == JOIN_LEFT && inner_unshippable_tlist)
        return NULL;
    if (jointype == JOIN_RIGHT && outer_unshippable_tlist)
        return NULL;
    if (jointype == JOIN_FULL && (inner_unshippable_tlist || outer_unshippable_tlist))
        return NULL;

    /* If both sides are replicated or have single node each, we ship any kind of JOIN */
    if ((IsExecNodesReplicated(inner_en) && IsExecNodesReplicated(outer_en)) ||
        (list_length(inner_en->nodeList) == 1 && list_length(outer_en->nodeList) == 1))
        merge_nodes = true;
    else if (inner_en->en_expr != NIL && outer_en->en_expr != NIL)
        merge_nodes = true;
    /* If both sides are distributed, ... */
    else if (IsExecNodesColumnDistributed(inner_en) && IsExecNodesColumnDistributed(outer_en)) {
        /*
         * If two sides are distributed in the same manner by a value, with an
         * equi-join on the distribution column and that condition
         * is shippable, ship the join if node lists from both sides can be
         * merged.
         */
        if (inner_en->baselocatortype == outer_en->baselocatortype && IsExecNodesDistributedByValue(inner_en) &&
            jointype != JOIN_FULL) {
            List* equi_join_expr =
                pgxc_find_dist_equijoin_qual(inner_en->en_dist_vars, outer_en->en_dist_vars, join_quals);

            ListCell* cell = NULL;
            if (equi_join_expr != NULL) {
                foreach (cell, equi_join_expr) {
                    Expr* join_expr = (Expr*)lfirst(cell);
                    if (!pgxc_is_expr_shippable(join_expr, NULL)) {
                        break;
                    }
                }

                if (cell == NULL) {
                    merge_nodes = true;

                    if (JOIN_LEFT == jointype) {
                        dist_vars = list_copy((List*)linitial(outer_en->en_dist_vars));
                    } else if (JOIN_RIGHT == jointype) {
                        dist_vars = list_copy((List*)linitial(inner_en->en_dist_vars));
                    }
                }
            }
        }
    } else if (IsExecNodesColumnDistributed(outer_en) && IsExecNodesReplicated(inner_en) &&
        (jointype == JOIN_INNER || jointype == JOIN_LEFT)) {
        /*
         * If outer side is distributed and inner side is replicated, we can ship
         * LEFT OUTER and INNER join.
         */
        merge_nodes = true;
    } else if (IsExecNodesReplicated(outer_en) && IsExecNodesColumnDistributed(inner_en) &&
        (jointype == JOIN_INNER || jointype == JOIN_RIGHT)) {
        /*
         * If outer side is replicated and inner side is distributed, we can ship
         * only for INNER join.
         */
        merge_nodes = true;
    }
    /*
     * If the ExecNodes of inner and outer nodes can be merged, the JOIN is
     * shippable
     */
    if (merge_nodes) {
        ExecNodes* merged_en = pgxc_merge_exec_nodes(inner_en, outer_en, (int)jointype);

        if (dist_vars != NIL && merged_en != NULL) {
            list_free_ext(merged_en->en_dist_vars);
            merged_en->en_dist_vars = list_make1(dist_vars);
        }

        return merged_en;
    } else {
        return NULL;
    }
}

/*
 * pgxc_check_triggers_shippability:
 * Return true if none of the triggers prevents the query from being FQSed.
 */
bool pgxc_check_triggers_shippability(Oid relid, int commandType, bool* hasTrigger)
{
    int16 trigevent = pgxc_get_trigevent((CmdType)commandType);
    Relation rel = relation_open(relid, AccessShareLock);
    bool found_nonshippable = false;

    /*
     * If we don't find a non-shippable row trigger, then the statement is
     * shippable as far as triggers are concerned. For FQSed query, statement
     * triggers are separately invoked on coordinator.
     */
    found_nonshippable = pgxc_find_nonshippable_row_trig(rel, trigevent, 0, true, hasTrigger);

    relation_close(rel, AccessShareLock);
    return !found_nonshippable;
}

/*
 * @Description : find IUD statement trigger on the relation.
 * @in relid : the relation oid need be find.
 * @in commandType : the command type on the relation.
 * @return : true if there is a statement trigger.
 */
bool pgxc_find_statement_trigger(Oid relid, int commandType)
{
    bool found_statement_trigger = false;
    Relation rel = NULL;
    int16 trigevent = 0;

    /* Check only IUD statement trigger here. */
    if (commandType != CMD_UPDATE && commandType != CMD_INSERT && commandType != CMD_DELETE)
        return false;

    if (!OidIsValid(relid))
        return false;

    trigevent = pgxc_get_trigevent((CmdType)commandType);
    rel = relation_open(relid, AccessShareLock);
    /* If any triggers on the table and match the event. */
    if (!(rel->trigdesc != NULL && pgxc_has_trigger_for_event(trigevent, rel->trigdesc))) {
        relation_close(rel, AccessShareLock);
        return false;
    }

    for (int i = 0; i < rel->trigdesc->numtriggers; i++) {
        uint16 tgtype = rel->trigdesc->triggers[i].tgtype;
        /* Check whether have statement level trigger. */
        if (!TRIGGER_FOR_ROW(tgtype)) {
            found_statement_trigger = true;
            break;
        }
    }

    relation_close(rel, AccessShareLock);
    return found_statement_trigger;
}

/* pgxc_find_nonshippable_row_trig:
 * Search for a non-shippable ROW trigger of a particular type.
 *
 * If ignore_timing is true, just the event_type is used to find a match, so
 * once the event matches, the search returns true regardless of whether it is a
 * before or after row trigger.
 *
 * If ignore_timing is false, return true if we find one or more non-shippable
 * row triggers that match the exact combination of event and timing.
 *
 * We have to do this way because the bitmask used for timing does
 * not have unique bit positions for different values. For e.g. for AFTER timing
 * type, the bit position 0x2 has value 0, and for BEFORE type the same
 * bit position has value 1, so it is impossible to use these bits to suggest
 * ignoring the timing. (ROW and STATEMENT values also share the same 0x1 bit
 * but we only want ROW triggers so it does not matter here). Hence an extra
 * flag ignore_timing to indicate that we want to ignore the timing
 * and only consider event type. The caller may just pass 0 for timing.
 * NOTE: To indicate that timing is to be ignored, we can device our own
 * "invalid" timing value in which all of the timing bits are set to 1
 * (i.e. the exact TRIGGER_TYPE_TIMING_MASK value), but that will make the
 * function calls unreadable.
 */
bool pgxc_find_nonshippable_row_trig(
    Relation rel, int16 tgtype_event, int16 tgtype_timing, bool ignore_timing, bool* hasTrigger)
{
    int i;

    /*
     * This function is used for finding matching row triggers only; should not
     * be called for TRUNCATE command.
     */
    AssertEreport(!TRIGGER_FOR_TRUNCATE((int32)tgtype_event), MOD_OPT, "should not be called for TRUNCATE command.");

    /* Assign initial value to true. */
    if (hasTrigger != NULL)
        *hasTrigger = true;

    /* Have triggers in the first place ? */
    if (rel->trigdesc == NULL) {
        if (hasTrigger != NULL)
            *hasTrigger = false;
        return false;
    }

    /*
     * Quick check by just scanning the trigger descriptor, before
     * actually peeking into each of the individual triggers.
     */
    if (!pgxc_has_trigger_for_event(tgtype_event, rel->trigdesc)) {
        if (hasTrigger != NULL)
            *hasTrigger = false;
        return false;
    }

    for (i = 0; i < rel->trigdesc->numtriggers; i++) {
        uint16 tgtype = rel->trigdesc->triggers[i].tgtype;

        /* We are looking for row triggers only */
        if (!TRIGGER_FOR_ROW(tgtype))
            continue;

        /*
         * If we are asked to find triggers of *any* level or timing, just match
         * the event type to determine whether we should ignore this trigger.
         */
        if (ignore_timing) {
            if ((TRIGGER_FOR_INSERT((int32)tgtype_event) && !TRIGGER_FOR_INSERT(tgtype)) ||
                (TRIGGER_FOR_UPDATE((int32)tgtype_event) && !TRIGGER_FOR_UPDATE(tgtype)) ||
                (TRIGGER_FOR_DELETE((int32)tgtype_event) && !TRIGGER_FOR_DELETE(tgtype)))
                continue;
        } else {
            /*
             * Otherwise, do an exact match with the given combination of event
             * and timing.
             */
            if (!TRIGGER_TYPE_MATCHES(tgtype, (int32)TRIGGER_TYPE_ROW, (int32)tgtype_timing, (int32)tgtype_event))
                continue;
        }

        /*
         * We now know that we cannot ignore this trigger, so check its
         * shippability.
         */
        if (!pgxc_is_trigger_shippable(i, rel))
            return true;
    }

    return false;
}

#define REL_GET_ITH_TRIG(rel, i) (rel->trigdesc->triggers[i])

/* Compile trigger function for body query and new old values. */
static inline bool trigger_need_compile(int trig_idx, Relation rel)
{
    return (IS_PGXC_COORDINATOR && u_sess->attr.attr_sql.enable_trigger_shipping && rel != NULL &&
        rel->rd_locator_info != NULL && !IsLocatorReplicated(rel->rd_locator_info->locatorType) &&
        get_func_lang(REL_GET_ITH_TRIG(rel, trig_idx).tgfoid) == get_language_oid("plpgsql", true) &&
        TRIGGER_FOR_ROW(REL_GET_ITH_TRIG(rel, trig_idx).tgtype));
}

/*
 * pgxc_is_trigger_shippable:
 * Check if trigger is shippable to a remote node. This function would be
 * called both on coordinator as well as datanode. We want this function
 * to be workable on datanode because we want to skip non-shippable triggers
 * on datanode.
 */
static bool pgxc_is_trigger_shippable(int trig_idx, Relation rel)
{
    /* For ordinary triggers, if enable_trigger_shipping is off, directly return false. 
     * Notice: We should consider if trigger is internal or not, since internal trigger 
     * must run on DN.
     */
    if (!u_sess->attr.attr_sql.enable_trigger_shipping) {
        if (!REL_GET_ITH_TRIG(rel, trig_idx).tgisinternal)
            return false;
        else
            return true;
    }

    bool res = true;

    /* We don't check the trigger when it's disabled. */
    if (REL_GET_ITH_TRIG(rel, trig_idx).tgenabled == TRIGGER_DISABLED)
        return true;

    /*
     * If trigger is based on a constraint or is internal, enforce its launch
     * whatever the node type where we are for the time being.
     * We need to remove this condition once constraints are better
     * implemented within openGauss as a constraint can be locally
     * evaluated on remote nodes depending on the distribution type of the table
     * on which it is defined or on its parent/child distribution types.
     */
    if (REL_GET_ITH_TRIG(rel, trig_idx).tgisinternal)
        return true;

    /*
     * INSTEAD OF triggers can only be defined on views, which are defined
     * only on Coordinators, so they cannot be shipped.
     */
    if (TRIGGER_FOR_INSTEAD(REL_GET_ITH_TRIG(rel, trig_idx).tgtype))
        return false;

    /* Compile trigger function for body query and new old values. */
    if (trigger_need_compile(trig_idx, rel)) {
        FunctionCallInfoData fake_fcinfo;
        FmgrInfo flinfo;
        TriggerData trigdata;
        PLpgSQL_function* function = NULL;
        errno_t rc = EOK;

        /* Set up a fake fcinfo with just enough info to satisfy function compile. */
        rc = memset_s(&fake_fcinfo, sizeof(fake_fcinfo), 0, sizeof(fake_fcinfo));
        securec_check(rc, "\0", "\0");

        rc = memset_s(&flinfo, sizeof(flinfo), 0, sizeof(flinfo));
        securec_check(rc, "\0", "\0");

        fake_fcinfo.flinfo = &flinfo;
        flinfo.fn_oid = REL_GET_ITH_TRIG(rel, trig_idx).tgfoid;
        flinfo.fn_mcxt = CurrentMemoryContext;

        rc = memset_s(&trigdata, sizeof(trigdata), 0, sizeof(trigdata));
        securec_check(rc, "\0", "\0");
        trigdata.type = T_TriggerData;
        fake_fcinfo.context = (Node*)&trigdata;

        /* Compile the trigger function to get info need by check shippable. */
        function = plpgsql_compile_nohashkey(&fake_fcinfo);

        /*
         * We need table attribute info to set NEW and OLD rec in the downstream,
         * and also need pre_parse_trig flag to indicate we are doing pre-parsing
         * for trigger body
         */
        function->tg_relation = rel;
        function->pre_parse_trig = true;

        /* Main function for check whehter the trigger function is shippalbe. */
        res = plpgsql_is_trigger_shippable(function);

        function->pre_parse_trig = false;
        function->tg_relation = NULL;

        /* Free subsidiary storage */
        plpgsql_free_function_memory(function);
    } else {
        /* Finally check if function called is shippable */
        shipping_context context;
        errno_t rc = memset_s(&context, sizeof(context), 0, sizeof(context));
        securec_check(rc, "\0", "\0");

        context.is_randomfunc_shippable = RANDOM_SHIPPABLE;
        context.is_ecfunc_shippable = IS_STREAM_PLAN;
        if (!pgxc_is_func_shippable(REL_GET_ITH_TRIG(rel, trig_idx).tgfoid, &context))
            res = false;
    }

    return res;
}

/*
 * get_var_from_node
 * Get a single Var reference base on node type
 * FuncExpr must have allowed (non hash breaking) type cast funcid to work.
 * @param The input node from expressions
 *         A function that defines the Oid range of FuncExpr, default is reject(false)
 * @return The Var related to the input node expression
 */
Var* get_var_from_node(Node* node, bool (*func)(Oid))
{
    Var* var = NULL;

    if (node == NULL)
        return NULL;

    switch (nodeTag(node)) {
        case T_Var: {
            var = (Var*)node;
            break;
        }

        case T_FuncExpr: {
            FuncExpr* fvar = (FuncExpr*)node;
            if (func(fvar->funcid) && fvar->args) {
                var = get_var_from_node((Node*)linitial(fvar->args), func);
            }
            break;
        }

        case T_RelabelType: {
            RelabelType* rtvar = (RelabelType*)node;
            if (rtvar->arg == NULL)
                return NULL;
            var = get_var_from_node((Node*)rtvar->arg);
            break;
        }

        default:
            return NULL;
    }

    return var;
}

/*
 * has_shippable_insert_rte:
 * Check rte feature for insert select. 
 * @param rte pending for evaluate
 * @return true if ready to ship as part of insertion
 */
static bool has_shippable_insert_rte(RangeTblEntry* rte)
{
    if (rte == NULL)
        return false;

    /* Partitoining is not supported */
    if (rte->isContainPartition || rte->partitionOid != 0)
        return false;

    /* Other featurs that is not currently supported by INSERT SELECT */
    if (rte->sublink_pull_up || rte->subquery_pull_up)
        return false;

    if (rte->isbucket || rte->relhasbucket || rte->lateral || rte->isexcluded)
        return false;

    if (rte->correlated_with_recursive_cte)
        return false;

    return true;
}

/*
 * get_rte_orientation_recurse:
 * Recursively find the source RTE and return its orientation.
 * @param the query, the var from upper-level query's targetEntry,
 *      the address of AttrNumber whos value get updated on recurse.
 * @return the orientation of the rte relation.
 */
static RelOrientation get_rte_orientation_recurse(Query* query, Var* var, AttrNumber* attrno)
{
    check_stack_depth();    /* Check depth */
    TargetEntry* tar = get_tle_by_resno(query->targetList, var->varattno);
    Var* tvar = NULL;
    if (tar) {
        tvar = get_var_from_node((Node*)tar->expr, func_oid_check_restricted);
    }

    if (tvar == NULL || attrno == NULL) {
        return REL_ORIENT_UNKNOWN;
    }

    *attrno = tvar->varattno;

    RangeTblEntry* rte = rt_fetch(tvar->varno, query->rtable);
    if (rte->rtekind == RTE_RELATION) {
        return rte->orientation;
    } else if (rte->rtekind == RTE_SUBQUERY) { /* Recurse on subquery */
        return get_rte_orientation_recurse(rte->subquery, tvar, attrno);
    } else {
        return REL_ORIENT_UNKNOWN;
    }
}

Expr* get_RelabelType_expr(Expr* expr)
{
    if (IsA(expr, RelabelType)) {
        return ((RelabelType*)expr)->arg;
    } else if (IsA(expr, FuncExpr) && list_length(((FuncExpr*)expr)->args) == 1 &&
        IsFunctionShippable(((FuncExpr*)expr)->funcid)) {
        return (Expr*)linitial(((FuncExpr*)expr)->args);
    } else {
        /* do nothing */
    }
    return expr;
}

bool equal_var(Expr* var, Index varno, AttrNumber attrNum)
{
    if (!IsA(var, Var)) {
        return false;
    }

    if (((Var*)var)->varno == varno && ((Var*)var)->varattno == attrNum) {
        return true;
    }
    return false;
}

/*
 * find_distcol_expr
 * This is the slightly twisted version of pgxc_find_distcol_expr
 * It returns the whole expression instead of the lower level expression.
 */
static Expr* find_distcol_expr(void* query_arg, Index varno, AttrNumber attrNum, Node* quals)
{
    List* lquals = NULL;
    ListCell* qual_cell = NULL;
    Query* query = (Query*)query_arg;

    if (quals == NULL)
        return NULL;

    if (!IsA(quals, List))
        lquals = make_ands_implicit((Expr*)quals);
    else
        lquals = (List*)quals;

    foreach (qual_cell, lquals) {
        Expr* qual_expr = (Expr*)lfirst(qual_cell);
        OpExpr* op = NULL;
        Expr* lexpr = NULL;
        Expr* rexpr = NULL;
        Var* var_expr = NULL;
        Expr* distcol_expr = NULL;

        if (IsA(qual_expr, NullTest)) {
            NullTest* nt = (NullTest*)qual_expr;
            if (nt->nulltesttype == IS_NULL && IsA(nt->arg, Var)) {
                var_expr = (Var*)(nt->arg);
                if (!equal_var((Expr*)var_expr, varno, attrNum))
                    continue;
                distcol_expr = (Expr*)makeNullConst(var_expr->vartype, var_expr->vartypmod, var_expr->varcollid);
                /* Found the distribution column expression return it */
                return distcol_expr;
            }
            continue;
        }

        if (!IsA(qual_expr, OpExpr))
            continue;
        op = (OpExpr*)qual_expr;
        /* If not a binary operator, it can not be '='. */
        if (list_length(op->args) != 2)
            continue;

        lexpr = (Expr*)linitial(op->args);
        rexpr = (Expr*)lsecond(op->args);

        lexpr = get_RelabelType_expr(lexpr);
        rexpr = get_RelabelType_expr(rexpr);

        if (equal_var(lexpr, varno, attrNum)) {
            var_expr = (Var*)lexpr;
            distcol_expr = rexpr;
        } else if (equal_var(rexpr, varno, attrNum)) {
            var_expr = (Var*)rexpr;
            distcol_expr = lexpr;
        } else {
            continue;
        }

        Var baserel_var = *var_expr;

        if (var_expr->varlevelsup == 0) {
            (void)get_real_rte_varno_attno(query, &(baserel_var.varno), &(baserel_var.varattno));

            var_expr = &baserel_var;
        }

        if (!equal_var((Expr*)var_expr, varno, attrNum))
            continue;

        if (!op_mergejoinable(op->opno, exprType((Node*)lexpr)) && !op_hashjoinable(op->opno, exprType((Node*)lexpr)))
            continue;
        return distcol_expr;
    }
    return NULL;
}

Expr* get_coerced_expr(Query* query, RelationLocInfo* loc_info, AttrNumber attnum, Expr* distcol_expr)
{
    Oid reloid = loc_info->relid;
    Oid disttype = get_atttype(reloid, attnum);
    int32 disttypmod = get_atttypmod(reloid, attnum);

    if (distcol_expr != NULL) {
        Oid exprtype = exprType((Node*)distcol_expr);
        if (disttype == NUMERICOID || disttype == BPCHAROID || disttype == VARCHAROID) {
            if (can_coerce_type(1, &exprtype, &disttype, COERCION_ASSIGNMENT)) {
                distcol_expr = (Expr*)coerce_type(NULL,
                    (Node*)distcol_expr,
                    exprtype,
                    disttype,
                    disttypmod,
                    COERCION_ASSIGNMENT,
                    COERCE_IMPLICIT_CAST,
                    -1);
            } else {
                distcol_expr = NULL;
            }
        } else {
            distcol_expr = (Expr*)coerce_to_target_type(NULL,
                (Node*)distcol_expr,
                exprtype,
                disttype,
                disttypmod,
                COERCION_ASSIGNMENT,
                COERCE_IMPLICIT_CAST,
                -1);
        }
    } else {
        return NULL;
    }
    return distcol_expr;
}

/*
 * find_distcol_expr_recurse:
 * This function can traverse all levels of the subquery and
 * calls the pgxc_find_distcol_expr() on each level.
 * @param the query, its jointree and the var from targetEntry, single node flag
 * @return the expr of given var
 */
static Expr* find_distcol_expr_recurse(Query* query, FromExpr* from_expr, Var* var, bool* no_quals, bool single_node)
{
    check_stack_depth();    /* Check depth */
    Expr* expr = NULL;
    Var* tvar = NULL;

    /* Current level check */
    RangeTblEntry* rte = rt_fetch(var->varno, query->rtable);
    if (from_expr == NULL) {
        return NULL;
    }
    if (no_quals) {
        *no_quals = (from_expr->quals == NULL);
    }
    expr = find_distcol_expr(query, var->varno, var->varattno, from_expr->quals);
    if (rte->rtekind != RTE_SUBQUERY || (expr && !IsA(expr, Var))) {
        return expr;
    }

    /* Try subquery */
    Query* subquery = rte->subquery;
    TargetEntry* tar = get_tle_by_resno(subquery->targetList, var->varattno);
    if (tar == NULL) {
        return NULL;
    }

    tvar = get_var_from_node((Node*)tar->expr, func_oid_check_restricted);
    if (tvar == NULL) {
        return single_node ? tar->expr : NULL;
    }

    return find_distcol_expr_recurse(subquery, subquery->jointree, tvar, no_quals, single_node);
}

/*
 * has_shippable_col_for_insert:
 * Find the source RTE and check if the column is ready to ship.
 * @param the SELECT query, SELECT target entries, target RTE from INSERT
 * @return insert select flags: unshippable or shippable distribution column/normal column
 */
static int has_shippable_col_for_insert(Query* query, TargetEntry* tar, RangeTblEntry* rte)
{
    Var* tvar = NULL;

    /* Fetch the var from target entry */
    tvar = get_var_from_node((Node*)tar->expr, func_oid_check_restricted);
    if (tvar == NULL)
        return INSEL_UNSHIPPABLE_COL;
    
    /* Make sure it is a valid RTE */
    RangeTblEntry* rte_qry = rt_fetch(tvar->varno, query->rtable);
    if (!has_shippable_insert_rte(rte_qry)) {
        return INSEL_UNSHIPPABLE_COL;
    }

    /* Handling complex situation */
    if (tar->resorigtbl == 0) {
        return INSEL_UNSHIPPABLE_COL;
    }

    Oid relid = tar->resorigtbl;

    /* Check the orientation of the source RTE */
    AttrNumber attrno = tvar->varattno;
    RelOrientation orien = REL_ORIENT_UNKNOWN;
    if (rte_qry->rtekind == RTE_SUBQUERY) { /* recurse on subqueries */
        orien = get_rte_orientation_recurse(rte_qry->subquery, tvar, &attrno);
    } else {
        orien = rte_qry->orientation;
    }

    /* Check orientation */
    if (rte->orientation != orien || orien == REL_ORIENT_UNKNOWN) {
        return INSEL_UNSHIPPABLE_COL;
    }

    if (attrno == 0) {  /* Special case handling */
        return INSEL_UNSHIPPABLE_COL;
    }

    /* 
     * Check if it is a distribution column
     * if not, return shippable normal column flag
     * if true, check if this column is defined in quals.
     */
    if (!IsDistribColumn(relid, attrno)) {
        return INSEL_SHIPPABLE_NCOL;
    }

    /* Recurse if we can't find anything at top level */
    bool no_quals = true;
    if (find_distcol_expr_recurse(query, query->jointree, tvar, &no_quals, false) || no_quals) {
        return INSEL_SHIPPABLE_DCOL;
    } else {
        return INSEL_UNSHIPPABLE_COL;
    }

    /* Default */
    return INSEL_UNSHIPPABLE_COL;
}

/*
 * has_consistent_execnodes:
 * Check exec nodes consistency for INSERT SELECT queries. 
 * @param exec nodes of target RTE and subqury from INSERT SELECT; dist column count (update with function);
 *      single_node flag: single DN execution ignores some of the limitations.
 * @return true if consistent, else false.
 */
static bool has_consistent_execnodes(ExecNodes* exec_nodes_rte, ExecNodes* exec_nodes_qry, int* dcol_cnt)
{
    ListCell* lc = NULL;
    List* dist_vars = NIL;
    Distribution distr_r;
    Distribution distr_q;

    if (exec_nodes_rte == NULL || exec_nodes_qry == NULL)   /* if not shippable */
        return false;

    /* Get node groups */
    distr_r = exec_nodes_rte->distribution;
    distr_q = exec_nodes_qry->distribution;

    /* If not in the same node group, return */
    if (distr_r.group_oid != distr_q.group_oid) {
        return false;
    }

    /* Make replication table an exception */
    if (IsExecNodesReplicated(exec_nodes_rte) && IsExecNodesReplicated(exec_nodes_qry)) {
        return true;
    }

    if (exec_nodes_qry->accesstype != RELATION_ACCESS_READ) {
        return false;
    }


    /* 
     * Agree on numbers
     * Shows that all RTEs have the same number of distribution columns.
     * It does not care about RTEs being subqueries or anything else.
     */
    int dcnt = 0;
    foreach(lc, exec_nodes_rte->en_dist_vars)
    {
        dist_vars = (List*)lfirst(lc);
        dcnt = list_length(dist_vars);
    }

    if (dcnt <= 0 || list_length(exec_nodes_rte->en_dist_vars) > 1)
        return false;

    if (dcol_cnt) {
        *dcol_cnt = dcnt;
    }

    foreach(lc, exec_nodes_qry->en_dist_vars)
    {
        dist_vars = (List*)lfirst(lc);
        int cnt = list_length(dist_vars);
        if (cnt != dcnt) {
            return false;
        }
    }

    return true;
}

/*
 * is_insert_subquery_deparsable
 * Check if this INSERT statement is able to parse back with get_var_from_node().
 * Node: We want to make sure it is able to parse back with get_var_from_node()
 *      so that the query can ship to datanodes with correct column order.
 * @param query and subquery of INSERT statement
 * @return true if all target entries are able to parse back with get_var_from_node()
 */
static bool is_insert_subquery_deparsable(Query* query, Query* subquery)
{
    ListCell* lc = NULL;

    /*
     * Before the targetList is modified by anywhere other than rewriteTargetListIU(),
     * we can assure that the difference between query's and subquery's targetList is 
     * default values. Then, we need to make sure all default values can be identified
     * by our deparser.
     *
     * The statement is unshippable(and not valid) if negative number of default values is given.
     */
    int default_value_cnt = list_length(query->targetList) - list_length(subquery->targetList);
    if (unlikely(default_value_cnt < 0)) {
        return false;
    }

    foreach(lc, query->targetList) {
        TargetEntry* tar = (TargetEntry*)lfirst(lc);

        /*
         * If we can get a Var from the targetEntry's expr, we can deparse it.
         */
        Var* var = get_var_from_node((Node*)tar->expr, func_oid_check_pass);
        if (var && (var->varattno <= list_length(subquery->targetList))) {
            continue;
        }
        /*
         * If we can't get a Var, it MUST be a default value.
         * We make this assumption base on the fact that all default value
         * should end up being a Const expression.
         * However, if we found more Const expressions than possible number
         * of default values, it means that we are unable to deparse all
         * non-default columns (since we need a Var to do that and one of them
         * doesn't have one).
         * (e.g. INSERT INTO t1(a, b) SELECT a, $1 FROM t2;
         * `b` here is referencing a Param, not a Var,
         *      and if `t1` has a column `c` with default value '1',
         *      we will end up here)
         */
        if (default_value_cnt == 0) {
            return false;
        }
        /*
         * We only handle the expressions that can be evaluated.
         */
        if (eval_const_expressions(NULL, (Node*)tar->expr)) {
            default_value_cnt--;
        } else {
            return false;   /* Unshippable if we can't handle it here */
        }
    }
    /* Make sure all predicted default values is checked */
    if (default_value_cnt != 0) {
        return false;
    }
    return true;
}

/*
 * get_relative_dist_pos
 * Get the relative position of a distribution column in ExecNodes.
 * varno is not required if the RTE is target table.
 * @param exec nodes of RTE, table indices(varno, varattno), is it a target table of a INSERT query?
 * @return position of the distribution column relative to other distribution columns in RTE
*/
int get_relative_dist_pos(ExecNodes* exec_nodes, Index varno, AttrNumber varattno, bool is_target_table)
{
    ListCell* lc = NULL;
    ListCell* dlc = NULL;
    List* dist_vars = NIL;

    foreach(lc, exec_nodes->en_dist_vars) {
        dist_vars = (List*)lfirst(lc);
        int pos = 0;    /* Reset */
        foreach(dlc, dist_vars)
        {
            Var* tvar = (Var*)lfirst(dlc);
            pos++;
            if ((is_target_table || varno == tvar->varno) && varattno == tvar->varattno) {
                return pos;
            }
        }
    }
    return 0;
}

/*
 * get_nodeList_from_dexprs
 * Get the execute datanode list from distribution expressions.
 * NOTE: This is a proprietary method, do not use this to get exec_nodes.
 * @param query, subquery, token list and access type
 * @return datenode list
 */
List* get_nodeList_from_dexprs(
    Query* query, Query* subquery, List* dexprList, List* idx_dist, List* distcols)
{
    int num_of_distcols = list_length(dexprList);

    if (list_length(idx_dist) != num_of_distcols) {
        list_free_ext(idx_dist);
        return NULL;
    }

    /* Allocate evaluate space */
    Datum* distcol_value = (Datum*)palloc(num_of_distcols * sizeof(Datum));
    bool* distcol_isnull = (bool*)palloc(num_of_distcols * sizeof(bool));
    Oid* distcol_type = (Oid*)palloc(num_of_distcols * sizeof(Oid));
    int i = 0;

    /* Get locator info from result relation */
    RelationLocInfo* loc_info = GetRelationLocInfo((rt_fetch(query->resultRelation, query->rtable))->relid);

    /* Loop through token list */
    ListCell* lc = NULL;
    ListCell* lc2 = NULL;
    forboth(lc, dexprList, lc2, distcols) {
        Node* node = (Node*)lfirst(lc);
        int distAttno = lfirst_int(lc2);
        Const* const_expr = NULL;

        /* 
         * We need to find the expression associated with the given Var.
         * And then try to transform the expression into a equivalent Const.
         */
        if (IsA(node, Var)) {
            Var* var_expr = (Var*)node;
            Expr* expr = find_distcol_expr_recurse(subquery, subquery->jointree, var_expr, NULL, true);
            expr = get_coerced_expr(query, loc_info, distAttno, expr);
            node = eval_const_expressions_params(NULL, (Node*)expr, query->boundParamsQ);
        }

        /* If it cannot be transformed, quit */
        if (node == NULL || !IsA(node, Const)) {
            free_distcol_info(distcol_value, distcol_isnull, distcol_type, idx_dist);   /* free */
            return NIL;
        }

        /* 
         * If it is a Const/can be transformed into Const, 
         * we simply put it in the eval space.
         */
        const_expr = (Const*)node;
        distcol_value[i] = const_expr->constvalue;
        distcol_isnull[i] = const_expr->constisnull;
        distcol_type[i] = const_expr->consttype;
        i++;
    }

    /* Get exec_nodes, hotkey handled here */
    ExecNodes* exec_nodes = GetRelationNodes(
        loc_info, distcol_value, distcol_isnull, distcol_type, idx_dist, RELATION_ACCESS_READ);

    /* Call free */
    free_distcol_info(distcol_value, distcol_isnull, distcol_type, idx_dist);
    return exec_nodes->nodeList;
}

/*
 * get_one_dexpr
 * Roughly get one distribute expression on targetlists.
 * @param query, subquery, target entry of the INSERT query
 * @return distribute expression
 */
Expr* get_one_dexpr(Query* query, Query* subquery, TargetEntry* tar_rte)
{
    Expr* dexpr = NULL;

    /* Append default values to tokenList, we do not expect any func except for type casts here */
    dexpr = (Expr*)get_var_from_node((Node*)tar_rte->expr, func_oid_check_restricted);
    if (dexpr == NULL) { /* If not found, it might be a default value here */
        dexpr = (Expr*)eval_const_expressions(NULL, (Node*)tar_rte->expr);
        if (dexpr == NULL) {
            return NULL;       /* Return if not found/not support */
        }
        return dexpr;
    }

    /* Try to get the 'true Var' from subquery's targetlist */
    TargetEntry* tar_qry = get_tle_by_resno(subquery->targetList, ((Var*)dexpr)->varattno);
    if (tar_qry == NULL) {
        return NULL;   /* return on not found */
    }

    /* We check the 'true var' with same method */
    dexpr = (Expr*)get_var_from_node((Node*)tar_qry->expr, func_oid_check_restricted);
    if (dexpr == NULL) {
        dexpr = (Expr*)eval_const_expressions_params(NULL, (Node*)tar_qry->expr, query->boundParamsQ);
        if (dexpr == NULL) {
            return NULL;       /* Return if not found/not support */
        }
    }
    return dexpr;   /* return a Var or Const */
}

/*
 * check_insert_subquery_on_singlenode
 * If subquery can be executed on a single node. All we need to do is to make sure
 * the INSERT can be executed on the same node.
 * @param query, subquery, exec_nodes of query and subquery
 * @return true if node list matches, else false
 */
bool check_insert_subquery_on_singlenode(
    Query* query, Query* subquery, ExecNodes* en_rte, ExecNodes* en_qry, Shippability_context* sc_context)
{
    List* dexprList = NIL;  /* List of distrbute column expressions */
    List* posList = NIL;    /* Position of distributed column, idx_dist */
    List* refList = NIL;    /* Refence List contains only distribution columns, if there is one */
    Expr* dexpr = NULL;

    /* Get relation oid of result relation */
    Oid relid_rte = (rt_fetch(query->resultRelation, query->rtable))->relid;

    /* We loop through targetlist and find the distribution column, or a value if lucky */
    ListCell* lc = NULL;
    foreach(lc, query->targetList) {
        TargetEntry* tar_rte = (TargetEntry*)lfirst(lc);
        if (!IsDistribColumn(relid_rte, tar_rte->resno)) {
            continue;
        }

        /* Position of the distrobute column, freed in get_nodeList_from_dexpr */
        posList = lappend_int(posList, get_relative_dist_pos(en_rte, 0, tar_rte->resno, true) - 1);

        /* Append one distcol expr into tokenList */
        dexpr = get_one_dexpr(query, subquery, tar_rte);
        if (dexpr == NULL) {
            list_free_ext(dexprList);
            list_free_ext(posList);
            list_free_ext(refList);
            return false;
        }
        dexprList = lappend(dexprList, dexpr);
        if (IsA(dexpr, Var)) {
            refList = lappend_int(refList, tar_rte->resno);
        } else {
            refList = lappend_int(refList, 0);
        }
    }

    /* Check if INSERT can be executed on the same datanode as SELECT */
    List* nodeList = get_nodeList_from_dexprs(query, subquery, dexprList, posList, refList);
    list_free_ext(dexprList);
    list_free_ext(refList);
    if (nodeList && list_is_subset_int(nodeList, en_qry->nodeList) && sc_context->sc_exec_nodes) {
        sc_context->sc_exec_nodes->nodeList = nodeList;
        return true;
    }
    return false;
}

bool check_replicated_junktlist(Query* subquery)
{
    ListCell *lc = NULL;

    foreach (lc, subquery->targetList) {
        TargetEntry *en = (TargetEntry *)lfirst(lc);

        if (!IsA(en->expr, Var)) {
            continue;
        }

        Var *v = (Var *)en->expr;
        if (v->varattno < 0) {
            return true;
        }
    }

    return false;
}

/*
 * check_insert_subquery_shippability:
 * Check if an INSERT SELECT query is shippable. 
 * Need to make sure the distribution of the subquery is matching with the distribution of the target table.
 * @param INSERT query, SELECT subquery, Shippability_context
 * @return void, but sc_context->inselect got updated
 */
static void check_insert_subquery_shippability(Query* query, Query* subquery, Shippability_context* sc_context)
{
    ExecNodes* exec_nodes_rte = NULL;
    ExecNodes* exec_nodes_qry = NULL;
    int dcnt_all = 0;   /* Counter of all distribtion columns */
    int dcnt_pst = 0;   /* Counter of presented distribtion columns */
    bool contain_column_store = sc_context->sc_contain_column_store;
    bool use_star_targets = false;

    /* Don't ship queries with FOR UPDATE/SHARE clause while dealing with INSERT */
    if (query->rowMarks || subquery->rowMarks) {
        return;
    }

    RangeTblEntry *rte = rt_fetch(query->resultRelation, query->rtable);
    if (!has_shippable_insert_rte(rte) || !is_insert_subquery_deparsable(query, subquery)) {
        return;
    }
    
    /* This is required only for target RTE */
    if (rte->rtekind != RTE_RELATION || rte->relid == 0) {
        return;
    }
    Oid relid_rte = rte->relid;

    /* Get execution nodes from target table and subquery, save it temporarily */
    exec_nodes_rte = pgxc_FQS_get_relation_nodes(rte, query->resultRelation, query);
    exec_nodes_qry = pgxc_is_query_shippable(subquery,
        sc_context->sc_query_level + 1,
        sc_context->sc_light_proxy,
        &contain_column_store,
        &use_star_targets);

    /* update necessary values */
    if (contain_column_store)
        sc_context->sc_contain_column_store = contain_column_store;
    if (use_star_targets)
        query->use_star_targets = use_star_targets;

    /* We donnot expect any value here in current level */
    if (unlikely(sc_context->sc_exec_nodes != NULL)) {
        return;
    }
    sc_context->sc_exec_nodes = exec_nodes_qry;

    /* RTE and query must have consistent exec_nodes features */
    if (!has_consistent_execnodes(exec_nodes_rte, exec_nodes_qry, &dcnt_all)) {
        return;
    }

    /*
     * If we know it is going to be executed on a single node, we can evaluate the shippability
     * base on the node being executed on, instead of the consistency of distribution.
     */
    if (((IsExecNodesReplicated(exec_nodes_qry) && exec_nodes_rte->baselocatortype == LOCATOR_TYPE_HASH) ||
        ExecOnSingleNode(exec_nodes_qry->nodeList)) &&
        check_insert_subquery_on_singlenode(query, subquery, exec_nodes_rte, exec_nodes_qry, sc_context)) {
        sc_context->sc_inselect = true;
        return;
    }

    /* If do not have the same distrbution for multi-node execution, return */
    if ((exec_nodes_rte->baselocatortype != exec_nodes_qry->baselocatortype)) {
        return;
    }

    if (IsExecNodesReplicated(exec_nodes_rte) &&
        IsExecNodesReplicated(exec_nodes_qry) &&
        check_replicated_junktlist(subquery)) {
        return;
    }

    /* Support HASH/REPLICATION/LIST/RANGE for now */
    bool isDistBySlice = IsLocatorDistributedBySlice(exec_nodes_rte->baselocatortype);
    if (exec_nodes_rte->baselocatortype != LOCATOR_TYPE_HASH && !isDistBySlice) {
        /* Handle REPLICATION statements, all target entries need to be parsable */
        sc_context->sc_inselect = IsExecNodesReplicated(exec_nodes_rte);
        return;
    }

    /* List/Range tables are shippable iff sliceinfo matches, keys and key order match, and NodeGroup matches. */
    if (isDistBySlice && !PgxcIsDistInfoSame(exec_nodes_rte, exec_nodes_qry)) {
        return;
    }

    /* 
     * To see if a INSERT SELECT query is shippable, we need to focus on the distribution
     * of both the target table and the output of SELECT subquery. And it is necessary to 
     * make sure the distribution of the table and targetlist of subquery has the same pattern.
     * e.g.
     * [table t1(a, b, c) hash(a, b)] and [table t2(b, c, d) hash(b, c)] has the same pattern
     * but
     * [table t1(a, b, c) hash(a, b)] and [table t2(b, c, d) hash(c, b)] does not.
     * Note that we are not interested in what happened to those non distribution columns,
     * but we do apply the same limitations on them for obvious reasons.
     */
    ListCell* lc = NULL;
    foreach (lc, query->targetList) {
        bool is_dcol_qry = false;
        Var* tvar_rte = NULL;
        Var* tvar_qry = NULL;
        Index src_varno = 0;    /* Source table varno in subquery */

        /* Get corrsponding target_lists, ignore the non distribute columns */
        TargetEntry* tar_rte = (TargetEntry*)lfirst(lc);
        if (!IsDistribColumn(relid_rte, tar_rte->resno)) {
            continue;
        }
        
        /* Get var from INSERT targetlist */
        tvar_rte = get_var_from_node((Node*)tar_rte->expr, func_oid_check_restricted);
        if (tvar_rte == NULL) {     /* If not found, it might be a default value here */
            return;   /* Do not handle defualt value on distriution column, can't ship */
        }

        /* Try to get the var from subquery's targetlist */
        TargetEntry* tar_qry = get_tle_by_resno(subquery->targetList, tvar_rte->varattno);
        if (tar_qry == NULL) {
            return;   /* return on not found */
        }
        tvar_qry = get_var_from_node((Node*)tar_qry->expr, func_oid_check_restricted);
        if (tvar_qry == NULL) {
            return;
        }

        /* Get distribution info of subquery column */
        int ship_flag = has_shippable_col_for_insert(subquery, tar_qry, rte);
        if (ship_flag == INSEL_SHIPPABLE_DCOL) {
            is_dcol_qry = true;
        } else {
            return;
        }

        /* Count number of distr column in target */
        Index new_src_varno = tvar_qry->varno;
        dcnt_pst++;
        if (src_varno == 0) {
            src_varno = new_src_varno;
        } else if (src_varno != new_src_varno) { /* If distribution columns are not from a single table */
            return;
        } else {
            /* Do nothing */
        }

        int pos_r = get_relative_dist_pos(exec_nodes_rte, query->resultRelation, tar_rte->resno, true);
        int pos_q = get_relative_dist_pos(exec_nodes_qry, tvar_qry->varno, tvar_qry->varattno, false);
        if (pos_r != pos_q) {   /* Distribution column relative position not aligned */
            return;
        }
        if (pos_r == 0) {   /* Cannot get distribution column position */
            return;
        }
    }
    if (dcnt_pst == dcnt_all && dcnt_pst != 0) { /* If not all distribution columns are present in query */
        sc_context->sc_inselect = true;
    }
    return;
}

static bool pgxc_check_insert_rte_shippability(Query* query, bool sc_light_proxy, Shippability_context* sc_context)
{
    RangeTblEntry* rte = NULL;
    RangeTblEntry* values_rte = NULL;
    RangeTblEntry* excluded_rte = NULL;
    RangeTblEntry* target_rte = NULL;
    ListCell* cell = NULL;
    List* rtables = query->rtable;

    foreach(cell, rtables) {
        rte = (RangeTblEntry*)lfirst(cell);
        if (rte->rtekind == RTE_VALUES) {
            if (values_rte != NULL) {
                return false;
            }
            values_rte = rte;
        } else if (rte->rtekind == RTE_RELATION) {
            if (rte->isexcluded) {
                if (excluded_rte != NULL) {
                    return false;
                }
                excluded_rte = rte;
            } else {
                if (target_rte != NULL) {
                    return false;
                }
                target_rte = rte;
            }
        } else if (rte->rtekind == RTE_SUBQUERY) {
            if (!sc_context->sc_inselect) {
                check_insert_subquery_shippability(query, rte->subquery, sc_context);
            }
            return sc_context->sc_inselect;
        } else {
            return false;
        }
    }

    return true;
}

/*
 * check whether expr of targestList can be shipped for distribution column
 */
static void pgxc_set_insert_shippability(Query* query, Shippability_context* sc_context)
{
    RangeTblEntry* rte = NULL;
    ListCell* cell = NULL;
    ListCell* attCell = NULL;

    Assert(query->commandType == CMD_INSERT);

    rte = (RangeTblEntry*)list_nth(query->rtable, query->resultRelation - 1);
    if (list_length(query->rtable) == 1 &&
        (RELKIND_RELATION != rte->relkind || rte->partAttrNum == NIL)) {
        return;
    }

    if (pgxc_check_insert_rte_shippability(query, sc_context->sc_light_proxy, sc_context)) {
        if (!sc_context->sc_inselect) {
            pgxc_set_shippability_reason(sc_context, SS_NEED_SINGLENODE);
        } else {
            return;
        }
    } else {
        pgxc_set_shippability_reason(sc_context, SS_UNSHIPPABLE_EXPR);
        return;
    }

    AttrNumber attnum;
    bool shippable_qual = false;
    foreach (attCell, rte->partAttrNum) {
        attnum = lfirst_int(attCell);
        foreach (cell, query->targetList) {
            TargetEntry* tle = (TargetEntry*)lfirst(cell);
            if (tle->resno == attnum) {
                /* if target column is distribution column, random function is not shippable */
                bool is_randomfunc_shippable_state = u_sess->opt_cxt.is_randomfunc_shippable;
                u_sess->opt_cxt.is_randomfunc_shippable = false;
                shippable_qual = pgxc_is_expr_shippable(tle->expr, NULL);
                u_sess->opt_cxt.is_randomfunc_shippable = is_randomfunc_shippable_state;
                if (!shippable_qual) {
                    pgxc_set_shippability_reason(sc_context, SS_UNSHIPPABLE_EXPR);
                    return;
                }

                /* Disable FQS when the distribution column in insert query contains volatile function */
                if (contain_volatile_functions((Node*)tle->expr)) {
                    pgxc_set_shippability_reason(sc_context, SS_UNSHIPPABLE_EXPR);
                    return;
                }
                break;
            }
        }
    }
}

/*
 * pgxc_check_dynamic_param:
 * If there's dynamicExpr that need to dynamically determine if the query can
 * be pushed to one datanode, use the value of params to judge it. If the value
 * of params are different, it can't be pushed down, and return false.
 */
bool pgxc_check_dynamic_param(List* dynamicExpr, ParamListInfo params)
{
    if (dynamicExpr == NIL)
        return true;

    /* If there's no parameter, we can't do make decision... */
    if (params == NULL)
        return false;

    /* If dynamic param is used, judge the value here */
    ListCell* lc = NULL;
    Node* node = (Node*)linitial(dynamicExpr);
    Datum value = 0;
    bool isnull = false;
    Oid nodeType = InvalidOid;

    /* We only handle Param node */
    if (IsA(node, Param)) {
        Param* par = (Param*)node;
        Assert(par->paramkind == PARAM_EXTERN);
        value = params->params[par->paramid - 1].value;
        isnull = params->params[par->paramid - 1].isnull;
        nodeType = par->paramtype;
    } else
        return false;

    /* If value is not fix-length, push down is not supported */
    Type typ = typeidType(nodeType);
    if (!OidIsValid(nodeType) || !typeByVal(typ)) {
        ReleaseSysCache(typ);
        return false;
    }

    ReleaseSysCache(typ);

    foreach (lc, dynamicExpr) {
        Datum value_local = 0;
        Datum isnull_local = false;
        if (lc == list_head(dynamicExpr))
            continue;
        node = (Node*)lfirst(lc);
        /* We only handle Param node */
        if (IsA(node, Param)) {
            Param* par = (Param*)node;
            Assert(par->paramkind == PARAM_EXTERN);
            value_local = params->params[par->paramid - 1].value;
            isnull_local = params->params[par->paramid - 1].isnull;
        } else
            return false;

        if (isnull != isnull_local)
            break;
        else if (!isnull && !datumIsEqual(value, value_local, true, -1))
            break;
    }
    if (lc != NULL)
        return false;

    return true;
}

/*
 * @Description : check the shippable of query which is in trigger function.
 * @in expr : the expr in trigger body.
 * @return : true if the query in trigger is shippable.
 */
bool pgxc_is_query_shippable_in_trigger(PLpgSQL_expr* expr)
{
    List* parsetree_list = NIL;
    ListCell* parsetree_item = NULL;
    MemoryContext oldcontext;
    MemoryContext trigship_context;
    bool can_ship = true;

    /* Switch to appropriate context for constructing parsetree. */
    oldcontext = MemoryContextSwitchTo(t_thrd.mem_cxt.msg_mem_cxt);

    /* Generate parse tree of query in trigger function. */
    parsetree_list = pg_parse_query(expr->query);

    /* Switch back to oldcontext to enter the loop. */
    MemoryContextSwitchTo(oldcontext);

    /* Apply for a new memory context for analyze and rewrite as well as pg_plan_queries. */
    trigship_context = AllocSetContextCreate(t_thrd.mem_cxt.msg_mem_cxt,
        "TrigshipContext",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);

    /* Run through the raw parsetree and process each one. */
    foreach (parsetree_item, parsetree_list) {
        List* querytree_list = NIL;
        ListCell* querytree_item = NULL;
        Node* parsetree = (Node*)lfirst(parsetree_item);
        bool snapshot_set = false;

        if (analyze_requires_snapshot(parsetree)) {
            PushActiveSnapshot(GetTransactionSnapshot());
            snapshot_set = true;
        }

        oldcontext = MemoryContextSwitchTo(trigship_context);

        /*
         * Generate the querytrees throuth querys in trigger body and the main
         * processing point is the get column info with no cur_estate, so we
         * filled the necessary info like copy_plpgsql_datum do.
         */
        querytree_list =
            pg_analyze_and_rewrite_params(parsetree, expr->query, (ParserSetupHook)plpgsql_parser_setup, (void*)expr);

        foreach (querytree_item, querytree_list) {
            Query* query = castNode(Query, lfirst(querytree_item));
            if (query->commandType == CMD_UTILITY) {
                can_ship = false;
                break;
            } else {
                Shippability_context sc_context;
                ExecNodes* exec_nodes = NULL;
                errno_t rc = EOK;

                rc = memset_s(&sc_context, sizeof(sc_context), '\0', sizeof(sc_context));
                securec_check(rc, "\0", "\0");

                sc_context.sc_query = query;
                sc_context.sc_query_level = 0;
                sc_context.sc_for_expr = false;
                sc_context.sc_for_trigger = true;
                sc_context.sc_plpgsql_func = (void*)expr->func;
                sc_context.sc_plpgsql_check_colocate_hook = plpgsql_check_colocate;

                /*
                 * We also use this walker to check the shippable of query in trigger as same
                 * as we do to the query outside the trigger, but there are some special logic
                 * only for the inner statement of the trigger.
                 */
                pgxc_shippability_walker((Node*)query, &sc_context);
                exec_nodes = sc_context.sc_exec_nodes;

                /*
                 * Exec_nodes is empty means not shippable.
                 * SELECT command is unshippable as it can not be checked colocate with IUD.
                 */
                if (exec_nodes == NULL || query->commandType == CMD_SELECT) {
                    bms_free_ext(sc_context.sc_shippability);
                    can_ship = false;
                    break;
                }

                /* Can not ship the query for some reason */
                if (!bms_is_empty(sc_context.sc_shippability)) {
                    FreeExecNodes(&exec_nodes);
                    bms_free_ext(sc_context.sc_shippability);
                    can_ship = false;
                    break;
                }

                FreeExecNodes(&exec_nodes);
            }
        }
        MemoryContextSwitchTo(oldcontext);
        MemoryContextDelete(trigship_context);

        if (snapshot_set)
            PopActiveSnapshot();

        if (!can_ship)
            return false;
    }
    return can_ship;
}

/*
 * check_has_sameAlias
 *
 * @Description : called by pgxc_shippability_walker for check
 * if the subquery/sublink's targets has same alias names.
 *
 * @in targetList : the targetList of subquery/sublink.
 * @return : true if has same alias names.
 */
static bool check_has_sameAlias(List* targetList)
{
    ListCell* cell = NULL;
    foreach (cell, targetList) {
        TargetEntry* target_1 = (TargetEntry*)lfirst(cell);
        ListCell* cell2 = cell;
        while ((cell2 = lnext(cell2)) != NULL) {
            TargetEntry* targest_2 = (TargetEntry*)lfirst(cell2);
            if (target_1->resname && targest_2->resname &&
                strncmp(target_1->resname, targest_2->resname, strlen(targest_2->resname) + 1) == 0) {
                return true;
            }
        }
    }
    return false;
}

/*
 * @Description : called by pgxc_shippability_walker for check the shippable of query in trigger
 * @in expr : the expr in trigger body.
 * @return : true if the query in trigger is shippable.
 */
static bool pgxc_check_shippability_in_trigger(Query* query, Shippability_context* sc_context)
{
    RangeTblEntry* rte = NULL;

    rte = (RangeTblEntry*)list_nth(query->rtable, query->resultRelation - 1);
    if (rte->relkind != RELKIND_RELATION || rte->partAttrNum == NIL)
        return true;

    if (sc_context->sc_plpgsql_check_colocate_hook != NULL) {
        return (*sc_context->sc_plpgsql_check_colocate_hook)(query, rte, sc_context->sc_plpgsql_func);
    }

    return false;
}
