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
 * -------------------------------------------------------------------------
 *
 * prepnonjointree.cpp
 *     Planner preprocessing for subqueries and non-join tree manipulation.
 *     The main module(s) are:
 *         (1) Lazy Agg
 *         (2) Reduce orderby
 *
 * IDENTIFICATION
 *     src/gausskernel/optimizer/prep/prepnonjointree.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "access/transam.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_proc.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/prep.h"
#include "optimizer/subselect.h"
#include "optimizer/tlist.h"
#include "optimizer/var.h"
#include "parser/parse_coerce.h"
#include "parser/parsetree.h"
#include "rewrite/rewriteHandler.h"
#include "rewrite/rewriteManip.h"
#include "utils/builtins.h"
#include "utils/syscache.h"

#if defined(ENABLE_UT) || defined(ENABLE_QUNIT)
#define static
#endif

/* ------------------------------------------------------------ */
/* Lazy Agg : begin                                           */
/* ------------------------------------------------------------ */
/*
 * --------------------
 * Data structure of Lazy Agg
 * --------------------
 */
enum LAZYAGG_AGGTYPE { LAZYAGG_SUM, LAZYAGG_MIN, LAZYAGG_MAX, LAZYAGG_COUNT, LAZYAGG_COUNTSTAR, LAZYAGG_OTHER };

struct lazyagg_query_context {
    Bitmapset* bms_sum = NULL;   /* bms of target list index(s) of SUM agg */
    Bitmapset* bms_min = NULL;   /* bms of target list index(s) of MIN agg */
    Bitmapset* bms_max = NULL;   /* bms of target list index(s) of MAX agg */
    Bitmapset* bms_count = NULL; /* bms of target list index(s) of COUNT agg in child-query, the potential rewritable
                                    SUM in parent-query */
    Bitmapset* bms_countStar = NULL; /* bms of target list index(s) of COUNT(*) agg in child-query, the potential
                                        rewritable SUM in parent-query */
    Bitmapset* bms_joinWhereGroupby =
        NULL;                  /* bms of target list index(s) of JOIN/WHERE/GROUP BY, only used by parent-query */
    Bitmapset* bms_all = NULL; /* bms of all attnum of child-query used in parent-query, only used by parent-query */
    bool isInnerSide;          /* is child-query in inner side of join, only used by parent-query */
    List* sumAggFuncs;         /* SUM Aggref list of parent-query, only used by parent-query */
    Expr** countParam = NULL;  /* param Expr of COUNT(c) in child-query, and best param Expr of child-query's
                                  corresponding column for parent-query */
    Oid* countParamBestType =
        NULL; /* best type Oid of countParam of all child-query's corresponding column, only used by parent-query */
    int lenOfCountParam; /* actually slot count of countParam, if this value is not init 0, it will be length+1 of
                            child-query's targetList */
};

/*
 * --------------------
 * Declaration of static functions
 * --------------------
 */
static void lazyagg_free_querycontext(lazyagg_query_context* context);
static void lazyagg_clear_childcontext(lazyagg_query_context* childContext);
static void lazyagg_init_parentquery_countparam(lazyagg_query_context* parentContext, lazyagg_query_context* from);
static void lazyagg_update_childquery_countparam(
    Query* childParse, Aggref* aggref, const AttrNumber attno, lazyagg_query_context* childContext);
static bool lazyagg_is_countparam_compatible(lazyagg_query_context* parentContext, lazyagg_query_context* childContext);
static Oid lazyagg_get_aggtrantype(Oid aggfnoid);
static Bitmapset* lazyagg_pull_vars_attno(Node* node, const Index rteIndex, Query* parse);
static void lazyagg_set_parentcontext_with_child(
    Query* parentParse, Index childQueryRTEIndex, lazyagg_query_context* parentContext);
static List* lazyagg_get_agg_list(Node* node, bool* has_AggrefsOuterquery = NULL);
static LAZYAGG_AGGTYPE lazyagg_get_agg_type(Aggref* aggref);
static bool lazyagg_check_delay_rewrite_compatibility(lazyagg_query_context* parentContext,
    lazyagg_query_context* childContext, Bitmapset* parentSumToCount, Bitmapset* parentSumToCountStar);
static bool lazyagg_check_childquery_feasibility(Query* parentParse, Query* childParse,
    lazyagg_query_context* parentContext, lazyagg_query_context* childContext, bool resetContextOnly = false);
static bool lazyagg_check_parentquery_feasibility(
    Query* parentParse, Index* targetRTEIndex, lazyagg_query_context* parentContext);
static void lazyagg_rewrite_setop_stmt(
    const AttrNumber varattno, lazyagg_query_context* parentContext, SetOperationStmt* setOpStmt);
static void lazyagg_rewrite_setop(
    Query* parentParse, const Index varno, const AttrNumber varattno, lazyagg_query_context* parentContext);
static void lazyagg_rewrite_join_aliasvars(Query* parentParse, Index varno, AttrNumber varattno, Oid vartype);
static bool lazyagg_rewrite_childquery(Query* childParse, lazyagg_query_context* parentContext, bool isCheck = false);
static bool lazyagg_rewrite_delayed_query(
    Query* parentParse, List* delayedChildQueryList, lazyagg_query_context* parentContext, bool isCheck = false);
static Query* lazyagg_analyze_single_childquery(
    Query* parentParse, Query* childParse, lazyagg_query_context* parentContext);
static List* lazyagg_search_setoperationstmt(
    Query* parentParse, Query* setOpParse, lazyagg_query_context* parentContext, Node* setOpStmtArg);
static List* lazyagg_analyze_setop_childquery(
    Query* parentParse, Query* setOpParse, lazyagg_query_context* parentContext, SetOperationStmt* setOpStmt);
static bool lazyagg_analyze_childquery(Query* parentParse, Query* childParse, lazyagg_query_context* parentContext);

/* ---------------------------------------------------------- */
/* Common module : begin										*/
/* ---------------------------------------------------------- */
/*
 * get_real_rte_varno_attno
 *     get the range table's real varno and varattno if there are JOIN typed RTE
 *
 * @param (in) parse:
 *     query tree
 * @param (in & out) varno:
 *     (in) varno of Var used
 *     (out) the real varno or the middle result
 * @param (in & out) varattno:
 *     (in) varattno of Var used
 *     (out) the real varattno or the middle result
 * @param (in) targetVarno:
 *     the target varno we need to find, search into CoalesceExpr only when target varno is set
 *
 * @return:
 *     return true if this <varno, varattno> touchs CoalesceExpr, which means this var is used in join clause
 */
bool get_real_rte_varno_attno(Query* parse, Index* varno, AttrNumber* varattno, const Index targetVarno)
{
    RangeTblEntry* rte = (RangeTblEntry*)list_nth(parse->rtable, *varno - 1);
    if (rte->rtekind == RTE_JOIN) {
        if (*varattno == 0) {
            return false;
        } else if (*varattno < 0) {
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                    (errmsg("Join range table do not have system column."))));
        }

        Node* node = (Node*)list_nth(rte->joinaliasvars, *varattno - 1);
        if (IsA(node, Var)) {
            Var* v = (Var*)node;
            *varno = v->varno;
            *varattno = v->varattno;
            return get_real_rte_varno_attno(parse, varno, varattno, targetVarno);
        } else if (IsA(node, CoalesceExpr)) {
            if (InvalidOid != targetVarno) {
                CoalesceExpr* c = (CoalesceExpr*)node;
                ListCell* lc = NULL;
                foreach (lc, c->args) {
                    Node* cnode = (Node*)lfirst(lc);
                    AssertEreport(IsA(cnode, Var),
                        MOD_OPT_REWRITE,
                        "CoalesceExpr's args should be Var in get_real_rte_varno_attno");
                    Var* v = (Var*)cnode;
                    Index cvarno = v->varno;
                    AttrNumber cvarattno = v->varattno;
                    (void)get_real_rte_varno_attno(parse, &cvarno, &cvarattno, targetVarno);
                    if (targetVarno == cvarno) {
                        *varno = cvarno;
                        *varattno = cvarattno;
                        break;
                    }
                }
            }
            return true;
        } else {
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                    (errmsg("Invalid join alias var"))));
        }
    }
    return false;
}

/*
 * get_real_rte_varno_attno_or_node
 *     Similar with get_real_rte_varno_attno, but if we can not find varno, we will return the node
 */
Node* get_real_rte_varno_attno_or_node(Query* parse, Index* varno, AttrNumber* varattno)
{
    RangeTblEntry* rte = (RangeTblEntry*)list_nth(parse->rtable, *varno - 1);
    if (rte->rtekind == RTE_JOIN) {
        if (*varattno == 0) {
            return NULL;
        } else if (*varattno < 0) {
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                    (errmsg("Join range table do not have system column."))));
        }

        Node* node = (Node*)list_nth(rte->joinaliasvars, *varattno - 1);
        if (IsA(node, Var)) {
            Var* v = (Var*)node;
            *varno = v->varno;
            *varattno = v->varattno;
            return get_real_rte_varno_attno_or_node(parse, varno, varattno);
        } else {
            return node;
        }
    }
    return NULL;
}

/*
 * is_join_inner_side
 *     get join side of a target RTE recursively
 *
 * @param (in) fromNode:
 *     a FROM node to be searched
 * @param (in) targetRTEIndex:
 *     target RTE's index
 * @param (in) isParentInnerSide
 *     fromNode's parent's join side is inner or not
 * @param (in & out) isFound:
 *     the target range table has benn found
 *
 * @return:
 *     whether the target range table (identified by targetRTEIndex) is in join inner side, true means in inner side
 */
bool is_join_inner_side(const Node* fromNode, const Index targetRTEIndex, const bool isParentInnerSide, bool* isFound)
{
    if ((*isFound)) {
        return true;
    }

    /*
     * if from node is a JoinExpr, recursive search in it's two side
     * if from node is a FromExpr, Traverse and recursive search it's fromList
     * if from node is a RangeTblRef, return true if target RTE is found in inner side
     */
    if (IsA(fromNode, JoinExpr)) {
        bool lres = false;
        bool rres = false;
        JoinExpr* je = (JoinExpr*)fromNode;
        switch (je->jointype) {
            case JOIN_INNER:
            case JOIN_SEMI:
            case JOIN_ANTI:
                lres = is_join_inner_side(je->larg, targetRTEIndex, isParentInnerSide, isFound);
                rres = is_join_inner_side(je->rarg, targetRTEIndex, isParentInnerSide, isFound);
                break;
            case JOIN_LEFT:
            case JOIN_LEFT_ANTI_FULL:
                lres = is_join_inner_side(je->larg, targetRTEIndex, isParentInnerSide, isFound);
                rres = is_join_inner_side(je->rarg, targetRTEIndex, true, isFound);
                break;
            case JOIN_RIGHT:
            case JOIN_RIGHT_ANTI_FULL:
                lres = is_join_inner_side(je->larg, targetRTEIndex, true, isFound);
                rres = is_join_inner_side(je->rarg, targetRTEIndex, isParentInnerSide, isFound);
                break;
            case JOIN_FULL:
                lres = is_join_inner_side(je->larg, targetRTEIndex, true, isFound);
                rres = is_join_inner_side(je->rarg, targetRTEIndex, true, isFound);
                break;
            /*
             * By now, we haven't cover some other join types:
             * (1) JOIN_UNIQUE_OUTER
             * (2) JOIN_UNIQUE_INNER
             * (3) JOIN_RIGHT_ANTI
             * (4) ...
             * These join types are included in default condition,
             * and may need to be explored
             */
            default:
                AssertEreport(false, MOD_OPT_REWRITE, "other join types should not be handled in is_join_inner_side");
                break;
        }
        return lres || rres;
    } else if (IsA(fromNode, FromExpr)) {
        FromExpr* fromExpr = (FromExpr*)fromNode;

        bool res = false;

        ListCell* lc = NULL;
        foreach (lc, fromExpr->fromlist) {
            Node* node = (Node*)lfirst(lc);
            res = res || is_join_inner_side(node, targetRTEIndex, isParentInnerSide, isFound);
        }

        return res;
    } else if (IsA(fromNode, RangeTblRef)) {
        RangeTblRef* rtblr = (RangeTblRef*)fromNode;
        AssertEreport(rtblr->rtindex > 0, MOD_OPT_REWRITE, "RangeTblRef node's index should > 0 in is_join_inner_side");
        if ((int)targetRTEIndex == rtblr->rtindex && isParentInnerSide == true) {
            /* the target child-query is in inner side */
            *isFound = true;
            return true;
        } else {
            return false;
        }
    } else {
        ereport(WARNING, (errmodule(MOD_OPT), (errmsg("Invalid fromNode type %d", fromNode->type))));
        AssertEreport(false, MOD_OPT_REWRITE, "Invalid fromNode type in is_join_inner_side");
        return false;
    }
}

/*
 * remove_useless_results_recurse
 *             Recursive guts of remove_useless_result_rtes.
 *
 * This recursively processes the jointree and returns a modified jointree.
 */
Node *remove_useless_results_recurse(PlannerInfo *root, Node *jtnode)
{
    Assert(jtnode != NULL);
    if (IsA(jtnode, RangeTblRef)) {
        /* Can't immediately do anything with a RangeTblRef */
    } else if (IsA(jtnode, FromExpr)) {
        FromExpr   *f = (FromExpr *) jtnode;
        Relids          result_relids = NULL;
        ListCell   *cell;
        ListCell   *prev;
        ListCell   *next;

        /*
         * We can drop RTE_RESULT rels from the fromlist so long as at least
         * one child remains, since joining to a one-row table changes
         * nothing.  The easiest way to mechanize this rule is to modify the
         * list in-place, using list_delete_cell.
         */
        prev = NULL;
        for (cell = list_head(f->fromlist); cell; cell = next) {
            Node       *child = (Node *) lfirst(cell);
            int                     varno;

            /* Recursively transform child ... */
            child = remove_useless_results_recurse(root, child);
            /* ... and stick it back into the tree */
            lfirst(cell) = child;
            next = lnext(cell);

            /*
             * If it's an RTE_RESULT with at least one sibling, we can drop
             * it.  We don't yet know what the inner join's final relid set
             * will be, so postpone cleanup of PHVs etc till after this loop.
             */
            if (list_length(f->fromlist) > 1 &&
                (varno = get_result_relid(root, child)) != 0) {
                f->fromlist = list_delete_cell(f->fromlist, cell, prev);
                result_relids = bms_add_member(result_relids, varno);
            } else {
                prev = cell;
            }
        }

        /*
         * Clean up if we dropped any RTE_RESULT RTEs.  This is a bit
         * inefficient if there's more than one, but it seems better to
         * optimize the support code for the single-relid case.
         */
        if (result_relids) {
            int                     varno = -1;

            while ((varno = bms_next_member(result_relids, varno)) >= 0)
                remove_result_refs(root, varno, (Node *) f);
        }

        /*
         * If we're not at the top of the jointree, it's valid to simplify a
         * degenerate FromExpr into its single child.  (At the top, we must
         * keep the FromExpr since Query.jointree is required to point to a
         * FromExpr.)
         */
        if (f != root->parse->jointree &&
            f->quals == NULL &&
            list_length(f->fromlist) == 1)
            return (Node *) linitial(f->fromlist);
    } else if (IsA(jtnode, JoinExpr)) {
        JoinExpr   *j = (JoinExpr *) jtnode;
        int                     varno;

        /* First, recurse */
        j->larg = remove_useless_results_recurse(root, j->larg);
        j->rarg = remove_useless_results_recurse(root, j->rarg);

        /* Apply join-type-specific optimization rules */
        switch (j->jointype) {
            case JOIN_INNER:

                /*
                 * An inner join is equivalent to a FromExpr, so if either
                 * side was simplified to an RTE_RESULT rel, we can replace
                 * the join with a FromExpr with just the other side; and if
                 * the qual is empty (JOIN ON TRUE) then we can omit the
                 * FromExpr as well.
                 */
                if ((varno = get_result_relid(root, j->larg)) != 0) {
                    remove_result_refs(root, varno, j->rarg);
                    if (j->quals)
                        jtnode = (Node *)
                                     makeFromExpr(list_make1(j->rarg), j->quals);
                    else
                        jtnode = j->rarg;
                } else if ((varno = get_result_relid(root, j->rarg)) != 0) {
                    remove_result_refs(root, varno, j->larg);
                    if (j->quals)
                        jtnode = (Node *)
                                     makeFromExpr(list_make1(j->larg), j->quals);
                    else
                        jtnode = j->larg;
                }
                break;
            case JOIN_LEFT:

                /*
                 * We can simplify this case if the RHS is an RTE_RESULT, with
                 * two different possibilities:
                 *
                 * If the qual is empty (JOIN ON TRUE), then the join can be
                 * strength-reduced to a plain inner join, since each LHS row
                 * necessarily has exactly one join partner.  So we can always
                 * discard the RHS, much as in the JOIN_INNER case above.
                 *
                 * Otherwise, it's still true that each LHS row should be
                 * returned exactly once, and since the RHS returns no columns
                 * (unless there are PHVs that have to be evaluated there), we
                 * don't much care if it's null-extended or not.  So in this
                 * case also, we can just ignore the qual and discard the left
                 * join.
                 */
                if ((varno = get_result_relid(root, j->rarg)) != 0 &&
                    (j->quals == NULL ||
                    !find_dependent_phvs((Node *) root->parse, varno))) {
                    remove_result_refs(root, varno, j->larg);
                    jtnode = j->larg;
                }
                break;
            case JOIN_RIGHT:
                /* Mirror-image of the JOIN_LEFT case */
                if ((varno = get_result_relid(root, j->larg)) != 0 &&
                    (j->quals == NULL ||
                    !find_dependent_phvs((Node *) root->parse, varno))) {
                    remove_result_refs(root, varno, j->rarg);
                    jtnode = j->rarg;
                }
                break;
            case JOIN_SEMI:

                /*
                 * We may simplify this case if the RHS is an RTE_RESULT; the
                 * join qual becomes effectively just a filter qual for the
                 * LHS, since we should either return the LHS row or not.  For
                 * simplicity we inject the filter qual into a new FromExpr.
                 *
                 * Unlike the LEFT/RIGHT cases, we just Assert that there are
                 * no PHVs that need to be evaluated at the semijoin's RHS,
                 * since the rest of the query couldn't reference any outputs
                 * of the semijoin's RHS.
                 */
                if ((varno = get_result_relid(root, j->rarg)) != 0) {
                    Assert(!find_dependent_phvs((Node *) root->parse, varno));
                    remove_result_refs(root, varno, j->larg);
                    if (j->quals)
                        jtnode = (Node *)
                                     makeFromExpr(list_make1(j->larg), j->quals);
                    else
                        jtnode = j->larg;
                }
                break;
            case JOIN_FULL:
            case JOIN_ANTI:
                /* We have no special smarts for these cases */
                break;
            default:
                elog(ERROR, "unrecognized join type: %d",
                     (int) j->jointype);
                break;
        }
    } else {
        elog(ERROR, "unrecognized node type: %d",
             (int) nodeTag(jtnode));
    }
    return jtnode;
}

/* ------------------------------------------------------------ */
/* Common module : end                                        */
/* ------------------------------------------------------------ */
/*
 * --------------------
 * Logic of Lazy Agg
 * --------------------
 */
/*
 * lazyagg_free_querycontext
 *     free a query context
 *
 * @param (in) context
 *     context to be free
 */
static void lazyagg_free_querycontext(lazyagg_query_context* context)
{
    if (context == NULL) {
        return;
    }

    bms_free_ext(context->bms_sum);
    bms_free_ext(context->bms_min);
    bms_free_ext(context->bms_max);
    bms_free_ext(context->bms_count);
    bms_free_ext(context->bms_countStar);
    bms_free_ext(context->bms_joinWhereGroupby);
    bms_free_ext(context->bms_all);
    if (context->sumAggFuncs != NIL) {
        list_free_ext(context->sumAggFuncs);
        context->sumAggFuncs = NIL;
    }
    if (context->countParam != NULL) {
        pfree_ext(context->countParam);
        context->countParam = NULL;
    }
    if (context->countParamBestType != NULL) {
        pfree_ext(context->countParamBestType);
        context->countParamBestType = NULL;
    }
    pfree_ext(context);
}

/*
 * lazyagg_clear_childcontext
 *     clear child-query's SUM/MIN/MAX/COUNT/COUNTSTAR bms after it is delayed rewriten with child-child-query
 *     and also clear countParam
 *     this is mainly for re-set of childContext
 *
 * @param (in) childContext:
 *     context information of child-query
 */
static void lazyagg_clear_childcontext(lazyagg_query_context* childContext)
{
    if (childContext == NULL) {
        return;
    }

    bms_free_ext(childContext->bms_sum);
    bms_free_ext(childContext->bms_min);
    bms_free_ext(childContext->bms_max);
    bms_free_ext(childContext->bms_count);
    bms_free_ext(childContext->bms_countStar);
    if (childContext->countParam != NULL) {
        pfree_ext(childContext->countParam);
    }
    childContext->bms_sum = NULL;
    childContext->bms_min = NULL;
    childContext->bms_max = NULL;
    childContext->bms_count = NULL;
    childContext->bms_countStar = NULL;
    childContext->countParam = NULL;
    childContext->lenOfCountParam = 0;
}

/*
 * lazyagg_init_parentquery_countparam
 *     init 'parentContext''s countParam and countParamOid from 'from' with a deep copy
 *
 * @param (out) parentContext:
 *     instence to be init, usually a parent-query context
 * @param (in) from:
 *     resource context, usually a child-query context, first child-query of the parent-query we meet
 */
static void lazyagg_init_parentquery_countparam(lazyagg_query_context* parentContext, lazyagg_query_context* from)
{
    /* remove old data */
    if (parentContext->countParam != NULL) {
        pfree_ext(parentContext->countParam);
        parentContext->countParam = NULL;
        pfree_ext(parentContext->countParamBestType);
        parentContext->countParamBestType = NULL;
    }

    parentContext->lenOfCountParam = from->lenOfCountParam;

    /*
     * if no COUNT found in child-query, lenOfCountParam will be 0
     * if COUNT was found in child-query, lenOfCountParam will be (list_length(child-query's targetList) + 1)
     */
    if (parentContext->lenOfCountParam == 0) {
        return;
    }

    parentContext->countParam = (Expr**)palloc0(sizeof(Expr*) * parentContext->lenOfCountParam);
    parentContext->countParamBestType = (Oid*)palloc0(sizeof(Oid) * parentContext->lenOfCountParam);

    /*
     * for each attrbute, copy countParam directly
     * if current attribute's countParam is not NULL, get it's type
     *     otherwise, mark it as InvalidOid
     */
    for (AttrNumber attno = 1; attno < parentContext->lenOfCountParam; ++attno) {
        parentContext->countParam[attno] = from->countParam[attno];
        if (parentContext->countParam[attno] != NULL) {
            parentContext->countParamBestType[attno] = exprType((Node*)(parentContext->countParam[attno]));
        } else {
            parentContext->countParamBestType[attno] = InvalidOid;
        }
    }
}

/*
 * lazyagg_update_childquery_countparam
 *     mainly used to update countParam of child-query
 *     countParamOid is not used by child-query
 *
 * @param (in) childParse:
 *     query tree of child-query
 * @param (in) aggref:
 *     a Aggref appeared in target list of child-query, which type is COUNT
 * @param (in) attno:
 *     the attribute index in target list, start with 1
 * @param (out) childContext:
 *     context of child-query
 */
static void lazyagg_update_childquery_countparam(
    Query* childParse, Aggref* aggref, const AttrNumber attno, lazyagg_query_context* childContext)
{
    AssertEreport(list_length(aggref->args) == 1,
        MOD_OPT_REWRITE,
        "Aggref should have 1 arg in lazyagg_update_childquery_countparam");
    AssertEreport(IsA(linitial(aggref->args), TargetEntry),
        MOD_OPT_REWRITE,
        "Aggref should have 1 TargetEntry arg in lazyagg_update_childquery_countparam");

    TargetEntry* te = (TargetEntry*)linitial(aggref->args);

    if (childContext->countParam == NULL) {
        childContext->lenOfCountParam = list_length(childParse->targetList) + 1;
        childContext->countParam = (Expr**)palloc0(sizeof(Expr*) * (childContext->lenOfCountParam));
    }

    childContext->countParam[attno] = te->expr;
}

/*
 * lazyagg_is_countparam_compatible
 *     for child-query(s) which are not the first one we meet,
 *     check it's compatibility with previous child-query(s)
 *
 * @param (in) parentContext:
 *     context of parent-query, where stored countParam and countParamOid information of previous child-query(s)
 * @param (in) childContext:
 *     context of child-query
 *
 * @return:
 *     whether current child-query is compatible with previous child-query(s), true means compatible
 */
static bool lazyagg_is_countparam_compatible(lazyagg_query_context* parentContext, lazyagg_query_context* childContext)
{
    /*
     * only two value lenOfCountParam could be:
     * (1) 0 means no COUNT,
     *     for parentContext means no COUNT in previous child-query,
     *     for childContext means no COUNT in current child-query
     * (2) (list_length(child-query's targetList) + 1) means COUNT is found
     */
    if (parentContext->lenOfCountParam != childContext->lenOfCountParam) {
        return false;
    }

    for (AttrNumber attno = 1; attno < parentContext->lenOfCountParam; ++attno) {
        Expr* expr1 = parentContext->countParam[attno];
        Expr* expr2 = childContext->countParam[attno];

        /* both of them are NULL means this column has no COUNT */
        if (NULL == expr1 && NULL == expr2) {
            continue;
        }

        /* one of them is NULL, but another is not, NOT compatible */
        if (NULL == expr1 || NULL == expr2) {
            return false;
        }

        /* get the best type Oid and better expr */
        Node* which_expr = NULL;
        Oid selectedOid = select_common_type(NULL, list_make2(expr1, expr2), NULL, &which_expr);
        if (InvalidOid == selectedOid) {
            return false;
        } else {
            parentContext->countParam[attno] = (Expr*)which_expr;
            parentContext->countParamBestType[attno] = selectedOid;
        }
    }

    return true;
}

/*
 * lazyAgg_get_aggTrantype
 *     get agg trantype oid when given a agg fn oid
 *     read it from system catalog table pg_aggregate
 *
 * @param (in) aggfnoid:
 *     agg function oid of Aggref, a.k.a., pg_proc Oid of the aggregate (Aggref->aggfnoid)
 *
 * @return:
 *     type Oid of transition results (Aggref->aggtrantype)
 */
static Oid lazyagg_get_aggtrantype(Oid aggfnoid)
{
    Oid aggTrantype = 0;

    HeapTuple tup = SearchSysCache1(AGGFNOID, ObjectIdGetDatum(aggfnoid));
    if (!HeapTupleIsValid(tup)) {
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for function %u", aggfnoid)));
    }
    Form_pg_aggregate aggr = (Form_pg_aggregate)GETSTRUCT(tup);
    aggTrantype = aggr->aggtranstype;
    ReleaseSysCache(tup);

    return aggTrantype;
}

/*
 * lazyagg_pull_vars_attno
 *     get all attno of var(s) which appear in node and belong to range table of rteIndex
 *
 * @param (in) node:
 *     node to be pull
 * @param (in) rteIndex:
 *     rte index of target's range table
 * @parse (in) parse:
 *     the query tree the node belong to
 *
 * @return:
 *     bitmap set of attno(s)
 */
static Bitmapset* lazyagg_pull_vars_attno(Node* node, const Index rteIndex, Query* parse)
{
    /* get all vars of the same level */
    List* allVars = pull_qual_vars(node, 0, QTW_IGNORE_JOINALIASES);

    /* bms to be return */
    Bitmapset* bms_varAttnum = NULL;

    /* find var(s) of target RTE */
    ListCell* lc = NULL;
    foreach (lc, allVars) {
        Var* v = (Var*)lfirst(lc);
        Index varnoReal = v->varno;
        AttrNumber varattnoReal = v->varattno;
        (void)get_real_rte_varno_attno(parse, &varnoReal, &varattnoReal, rteIndex);
        if (rteIndex == varnoReal) {
            bms_varAttnum = bms_add_member(bms_varAttnum, varattnoReal);
        }
    }

    return bms_varAttnum;
}

/*
 * lazyagg_set_parentcontext_with_child
 *     Extract JOIN, WHERE, GROUP BY columns in target RTE
 *
 * @param (in) parentParse:
 *     parent-query tree
 * @param (in) childQueryRTEIndex:
 *     target RTE
 * @param (in) parentContext:
 *     context information of parent-query
 */
static void lazyagg_set_parentcontext_with_child(
    Query* parentParse, Index childQueryRTEIndex, lazyagg_query_context* parentContext)
{
    /* var(s) attno of JOIN, WHERE */
    Bitmapset* bms_varAttNoInJointree =
        lazyagg_pull_vars_attno((Node*)(parentParse->jointree), childQueryRTEIndex, parentParse);

    /* var(s) attno of GROUP BY */
    List* groupByExprs = get_sortgrouplist_exprs(parentParse->groupClause, parentParse->targetList);
    Bitmapset* bms_varAttnoInGroupBy = lazyagg_pull_vars_attno((Node*)groupByExprs, childQueryRTEIndex, parentParse);
    if (NIL != groupByExprs) {
        list_free_ext(groupByExprs);
    }

    /* var(s) attno of JOIN, WHERE, GROUP BY */
    parentContext->bms_joinWhereGroupby = bms_union(bms_varAttNoInJointree, bms_varAttnoInGroupBy);

    /* bms of all attno of child-query used in parent-query */
    parentContext->bms_all = lazyagg_pull_vars_attno((Node*)parentParse, childQueryRTEIndex, parentParse);

    /* join side */
    bool isFound = false;
    parentContext->isInnerSide = is_join_inner_side((Node*)parentParse->jointree, childQueryRTEIndex, false, &isFound);
}

/*
 * lazyagg_get_agg_list
 *     Get all Aggref(s) and GroupingFunc(s) in a node
 *
 * @param (in) node:
 *     the target node
 * @param (in/out) has_AggrefsOuterquery:
 *     if the target node contains agg references outer query, set
 *     has_AggrefsOuterquery be true.
 *
 * @return:
 *     a list contain Aggref(s) and GroupingFunc(s)
 */
static List* lazyagg_get_agg_list(Node* node, bool* has_AggrefsOuterquery)
{
    if (node == NULL) {
        return NIL;
    }

    /*
     * Aggref in SubLink that reference outer query can not be rewrite when checking
     * childquery. Because delete the groupclause of outer query will lead result
     * incorret while rewrite the query.
     */
    if (has_AggrefsOuterquery != NULL) { /* should not be NULL when check childquery */
        ListCell* lc1 = NULL;
        List* sublinks = pull_sublink(node, 0, false, true);
        foreach (lc1, sublinks) {
            AssertEreport(IsA(lfirst(lc1), SubLink), MOD_OPT_REWRITE, "Sublink mismatch in lazyagg_get_agg_list");

            SubLink* sublink = (SubLink*)lfirst(lc1);
            if (contain_aggs_of_level_or_above(sublink->subselect, 1)) {
                /*
                 * Once found agg references outer query, the child query must
                 * not be rewrite, so set the flag and return immediately.
                 */
                *has_AggrefsOuterquery = true;
                return NULL;
            }
        }
    }

    List* aggList = NIL;
    List* l =
        pull_var_clause(node, PVC_INCLUDE_AGGREGATES, PVC_INCLUDE_PLACEHOLDERS, PVC_RECURSE_SPECIAL_EXPR, true, true);

    /* pick Aggref(s) from the Var(s) list */
    ListCell* lc2 = NULL;
    foreach (lc2, l) {
        Node* localNode = (Node*)lfirst(lc2);
        if ((IsA(localNode, Aggref) && 0 == ((Aggref*)localNode)->agglevelsup) ||
            (IsA(localNode, GroupingFunc) && 0 == ((GroupingFunc*)localNode)->agglevelsup)) {
            aggList = lappend(aggList, localNode);
        }
    }

    list_free_ext(l);

    return aggList;
}

/*
 * lazyagg_get_agg_type
 *     get agg function type enum by read agg name from system catalog pg_proc
 *
 * @param (in) aggref:
 *     target agg function, an Aggref
 *
 * @return:
 *     the agg type cluster, more detail in LAZYAGG_AGGTYPE
 */
static LAZYAGG_AGGTYPE lazyagg_get_agg_type(Aggref* aggref)
{
    Oid aggfnoid = aggref->aggfnoid;

    /* oid must be in range of system defined function */
    if (aggfnoid >= FirstNormalObjectId) {
        return LAZYAGG_OTHER;
    }

    HeapTuple tup = SearchSysCache1(PROCOID, ObjectIdGetDatum(aggfnoid));
    if (!HeapTupleIsValid(tup)) {
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for function %u", aggfnoid)));
    }

    Form_pg_proc proc = (Form_pg_proc)GETSTRUCT(tup);
    NameData name = proc->proname;
    int2 pronargs = proc->pronargs;
    ReleaseSysCache(tup);

    if (0 == strncmp(name.data, "sum", strlen("sum"))) {
        return LAZYAGG_SUM;
    } else if (0 == strncmp(name.data, "min", strlen("min"))) {
        return LAZYAGG_MIN;
    } else if (0 == strncmp(name.data, "max", strlen("max"))) {
        return LAZYAGG_MAX;
    } else if (0 == strncmp(name.data, "count", strlen("count"))) {
        if (1 == pronargs) {
            return LAZYAGG_COUNT;
        } else if (0 == pronargs) {
            return LAZYAGG_COUNTSTAR;
        }
    }

    return LAZYAGG_OTHER;
}

/*
 * lazyagg_check_delay_rewrite_compatibility
 *     Check wheather this child-query is compatible with other child-query with the shared parent-query.
 *     Use parentContext->bms_count, parentContext->bms_countStar,
 *         and parentContext->countParam to handle this compatibility check.
 *     parentContext->bms_count[InvalidOid] and parentContext->bms_countStar[InvalidOid] is used as a flag,
 *         set this bit when child-query(s) are not compatible, which will help futher check.
 *
 * @param (in) parentContext:
 *     context of parent-query
 * @param (in) childContext:
 *     context of child-query
 * @param (in) parentSumToCount:
 *     the bms to mark SUM(s) in parent-query have paired COUNT in current child-query
 * @param (in) parentSumToCountStar:
 *     the bms to mark SUM(s) in parent-query have paired COUNT(*) in current child-query
 *
 * @return:
 *     whether delay rewrite is compatible. true means it is compatible
 */
static bool lazyagg_check_delay_rewrite_compatibility(lazyagg_query_context* parentContext,
    lazyagg_query_context* childContext, Bitmapset* parentSumToCount, Bitmapset* parentSumToCountStar)
{
    bool canDelayRewrite = false;

    /*
     * If parentContext->bms_count and parentContext->bms_countStar are NULL,
     *     means it is the first child-query of the parent-query we meet.
     * If it's not the first one, check the compatibility with other previous child-query(s),
     *     previous information has been marked in parentContext->bms_count, parentContext->bms_countStar and
     * parentContext->countParam
     */
    if (parentContext->bms_count == NULL && parentContext->bms_countStar == NULL) {
        /* this is the first child-query we meet, which share the same parent-query */
        parentContext->bms_count = parentSumToCount;
        parentContext->bms_countStar = parentSumToCountStar;
        lazyagg_init_parentquery_countparam(parentContext, childContext);

        canDelayRewrite = true;
    } else if (!bms_equal(parentContext->bms_count, parentSumToCount) ||
               !bms_equal(parentContext->bms_countStar, parentSumToCountStar) ||
               !lazyagg_is_countparam_compatible(parentContext, childContext)) {
        /*
         * Not match the compatibility, this parent-query/child-querys pair should not be rewriten
         * set the InvalidOid slot to mark this bms is invalid
         */
        parentContext->bms_count = bms_add_member(parentContext->bms_count, InvalidOid);
        parentContext->bms_countStar = bms_add_member(parentContext->bms_countStar, InvalidOid);
        bms_free_ext(parentSumToCount);
        bms_free_ext(parentSumToCountStar);

        canDelayRewrite = false;
    } else {
        /* not the first one, but match with previous brother child-query(s), return it for further delay rewrite */
        bms_free_ext(parentSumToCount);
        bms_free_ext(parentSumToCountStar);

        canDelayRewrite = true;
    }

    return canDelayRewrite;
}

/*
 * lazyagg_check_childquery_feasibility
 *     (1) check wheather child-query is valid for Lazy Agg rule
 *     (2) check the feasibility of impact between parent-query and child-query
 *
 * @param (in) parentParse:
 *     parent-query tree
 * @param (in) childParse:
 *     child-query tree
 * @param (in) parentContext:
 *     context of parent-query
 * @param (in) childContext:
 *     context of child-query
 * @param (in) resetContextOnly:
 *     this call re-set context only
 *
 * @return:
 *     whether parent-query is valid for Lazy Agg rule, true means valid, false otherwise
 */
static bool lazyagg_check_childquery_feasibility(Query* parentParse, Query* childParse,
    lazyagg_query_context* parentContext, lazyagg_query_context* childContext, bool resetContextOnly)
{
    if (childParse->groupClause == NIL) {
        /*
         * ------ Rule child 1 ------
         * child-query need to have GROUP BY
         */
        return false;
    }

    /* Check other conditions */
    if (childParse->hasWindowFuncs || childParse->hasDistinctOn || childParse->hasRecursive ||
         childParse->groupingSets != NIL || childParse->havingQual != NULL || childParse->windowClause != NIL ||
         childParse->distinctClause != NIL || childParse->limitOffset != NULL || childParse->limitCount != NULL ) {
        /*
         * ------ Rule child 4 ------
         * No operators like window agg, distinct, recursive, AP function, having, offset, limit etc.
         */
        return false;
    }

    childContext->isInnerSide = false;
    childContext->sumAggFuncs = NIL;
    childContext->lenOfCountParam = 0;

    Index tleIndex = 1;
    ListCell* lc = NULL;
    foreach (lc, childParse->targetList) {
        if (!bms_is_member(tleIndex, parentContext->bms_all)) {
            ++tleIndex;
            continue;
        }

        TargetEntry* te = (TargetEntry*)lfirst(lc);
        bool has_AggrefsOuterquery = false;

        List* te_AggrefList = lazyagg_get_agg_list((Node*)te, &has_AggrefsOuterquery);

        if (has_AggrefsOuterquery) {
            list_free_ext(te_AggrefList);
            return false;
        }

        if (NIL != te_AggrefList) {
            if (!resetContextOnly) {
                /* there need to be only one Aggref and no other expr(s) around Aggref */
                if (!(1 == list_length(te_AggrefList) && IsA(te->expr, Aggref))) {
                    /*
                     * ------ Rule child 3 ------
                     * child-query's agg function is target output directly with out anything around it
                     */
                    list_free_ext(te_AggrefList);
                    return false;
                }
            } else {
                /*
                 * This is a reset process of child-query, these has been checked
                 * BUT==> If child-query and child-child-query is delayed rewriten,
                 *            a type converter may be added around Aggref of child-query
                 */
                AssertEreport(1 == list_length(te_AggrefList),
                    MOD_OPT_REWRITE,
                    "there should be only one Aggref in lazyagg_check_childquery_feasibility");
                AssertEreport(
                    IsA(te->expr, Aggref) ||
                        (IsA(te->expr, FuncExpr) && IsA((Node*)linitial(((FuncExpr*)te->expr)->args), Aggref)) ||
                        (IsA(te->expr, RelabelType) && IsA(((RelabelType*)te->expr)->arg, Aggref)),
                    MOD_OPT_REWRITE,
                    "Invalid type of TargetEntry node's expr in lazyagg_check_childquery_feasibility");
            }

            Aggref* aggref = (Aggref*)linitial(te_AggrefList);

            /* Get the agg function */
            LAZYAGG_AGGTYPE aggType = lazyagg_get_agg_type(aggref);
            if (LAZYAGG_OTHER == aggType) {
                /*
                 * ------ Rule child 2 ------
                 * agg function only support SUM/MIN/MAX/COUNT,
                 */
                list_free_ext(te_AggrefList);
                return false;
            }

            /* There need to be no DINSTINCT in agg function */
            if (aggref->aggdistinct != NIL) {
                /*
                 * ------ Rule child 4 ------
                 * No operators like window agg, distinct, recursive, AP function, having, offset, limit etc.
                 * (no DISTINCT here)
                 */
                list_free_ext(te_AggrefList);
                return false;
            }

            if (bms_is_member(tleIndex, parentContext->bms_joinWhereGroupby)) {
                /*
                 * ------ Rule joint 2 ------
                 * agg column in child-query needs to have no intersect with JOIN/WHERE/GROUP BY in parent-query
                 */
                list_free_ext(te_AggrefList);
                return false;
            }

            if ((LAZYAGG_COUNT == aggType || LAZYAGG_COUNTSTAR == aggType) &&
                (parentContext->isInnerSide || parentParse->groupClause == NIL)) {
                /*
                 * ------ Rule joint 3 ------
                 * COUNT typed child-query should not in inner side of (LEFT/RIGHT/FULL) JOIN
                 * ------ Rule joint 4 ------
                 * COUNT typed child-query's parent-query should have GROUP BY
                 */
                list_free_ext(te_AggrefList);
                return false;
            }

            /* mark agg functions */
            switch (aggType) {
                case LAZYAGG_SUM:
                    childContext->bms_sum = bms_add_member(childContext->bms_sum, tleIndex);
                    break;
                case LAZYAGG_MIN:
                    childContext->bms_min = bms_add_member(childContext->bms_min, tleIndex);
                    break;
                case LAZYAGG_MAX:
                    childContext->bms_max = bms_add_member(childContext->bms_max, tleIndex);
                    break;
                case LAZYAGG_COUNT:
                    childContext->bms_count = bms_add_member(childContext->bms_count, tleIndex);
                    lazyagg_update_childquery_countparam(childParse, aggref, tleIndex, childContext);
                    break;
                case LAZYAGG_COUNTSTAR:
                    childContext->bms_countStar = bms_add_member(childContext->bms_countStar, tleIndex);
                    break;
                default:
                    break;
            }
            list_free_ext(te_AggrefList);
        }
        ++tleIndex;
    }

    if ((!bms_is_subset(parentContext->bms_sum,
            bms_union(childContext->bms_sum, bms_union(childContext->bms_count, childContext->bms_countStar)))) ||
        (!bms_is_subset(parentContext->bms_min, childContext->bms_min)) ||
        (!bms_is_subset(parentContext->bms_max, childContext->bms_max))) {
        /*
         * ------ Rule joint 1 ------
         * agg function match each other and agg function param in parent-query is subset of agg function column in
         * child-query
         */
        return false;
    }

    return true;
}

/*
 * lazyagg_check_parentquery_feasibility
 *     check wheather parent-query is valid for Lazy Agg rule
 *
 * @param (in) parentParse:
 *     parent-query tree
 * @param (in) targetRTEIndex:
 *     0: parent-query does not have aggs in output columns
 *     other number: index(+1) of Query->rtable, starts from 1
 * @param (in) parentContext:
 *     part of context information of parent-query
 *
 * @return:
 *     whether parent-query is valid for lazy agg rule, true means valid, false otherwise
 */
static bool lazyagg_check_parentquery_feasibility(
    Query* parentParse, Index* targetRTEIndex, lazyagg_query_context* parentContext)
{
    *targetRTEIndex = 0;
    List *tlist_var_list = NIL;
    ListCell* lc = NULL;

    if (!(parentParse->hasAggs || parentParse->groupClause != NIL)) {
        /*
         * ------ Rule parent 1 ------
         * There need to be agg function(s) or GROUP BY in parent-query
         */
        return false;
    }

    tlist_var_list = pull_var_clause((Node *)parentParse->targetList,
                                  PVC_INCLUDE_AGGREGATES,
                                  PVC_INCLUDE_PLACEHOLDERS,
                                  PVC_RECURSE_SPECIAL_EXPR,
                                  true, true);

    /* We can not use lazyagg if any var in targetlist is from sublink pulled up */
    foreach(lc, tlist_var_list)	{
        Node *node = (Node *)lfirst(lc);
        if (IsA(node, Var) && (((Var *) node)->varlevelsup == 0) &&
            var_from_sublink_pulluped(parentParse, (Var *)node)) {
            return false;
        }
    }


    /* check volatile in JOIN and WHERE of parent-query */
    bool isJoinHasVolatile = contain_volatile_functions((Node*)parentParse->jointree);

    /* check volatile in GROUP BY of parent-query */
    List* groupByExprs = get_sortgrouplist_exprs(parentParse->groupClause, parentParse->targetList);
    bool isGroupByHasVolatile = contain_volatile_functions((Node*)groupByExprs);
    if (NIL != groupByExprs) {
        list_free_ext(groupByExprs);
    }

    /* volatile checking */
    if (isJoinHasVolatile || isGroupByHasVolatile) {
        /*
         * ------ Rule parent 5 -------
         * no volatile in JOIN and WHERE of parent-query
         * ------ Rule parent 6 -------
         * no volatile in GROUP BY of parent-query
         */
        return false;
    }

    /* Get all Aggref(s) in parent-query's target list */
    List* aggrefList = lazyagg_get_agg_list((Node*)(parentParse->targetList));

    /* Get all Aggref(s) in parent-query's having qual */
    List* aggrefList_having = lazyagg_get_agg_list((Node*)(parentParse->havingQual));
    aggrefList = list_concat(aggrefList, aggrefList_having);

    /* the return value */
    bool isOK = true;
    parentContext->isInnerSide = false;
    parentContext->sumAggFuncs = NIL;
    parentContext->lenOfCountParam = 0;

    /* If there are agg functions in target list of parent-query, traverse them */
    foreach (lc, aggrefList) {
        if (!IsA(lfirst(lc), Aggref)) {
            /*
             * ------ Rule parent 9 -------
             * parent-query should not contain 'grouping()' function
             */
            isOK = false;
            /* free memory and return */
            break;
        }

        Aggref* aggref = (Aggref*)lfirst(lc);

        /* Get the agg function of Aggref */
        LAZYAGG_AGGTYPE aggType = lazyagg_get_agg_type(aggref);
        /* Check the agg function */
        if (LAZYAGG_SUM != aggType && LAZYAGG_MIN != aggType && LAZYAGG_MAX != aggType) {
            /*
             * ------ Rule parent 2 -------
             * parent-query's agg function need to be sum/min/max
             */
            isOK = false;
            /* free memory and return */
            break;
        }

        /* Check is there a DISTINCT in agg function */
        if (aggref->aggdistinct != NIL) {
            /*
             * ------ Rule parent 3 ------
             * parent-query's agg function can only contain a Var param
             * (no distinct in agg function)
             */
            isOK = false;
            /* free memory and return */
            break;
        }

        /* The agg function need to contain a Var only */
        List* aggParam = aggref->args;
        if (!(1 == list_length(aggParam) && IsA(linitial(aggParam), TargetEntry) &&
                IsA(((TargetEntry*)linitial(aggParam))->expr, Var))) {
            /*
             * ------ Rule parent 3 ------
             * parent-query's agg function can only contain a Var param
             * (only a Var as param)
             */
            isOK = false;
            /* free memory and return */
            break;
        } else {
            /* get the Var paramter */
            Var* var = (Var*)(((TargetEntry*)linitial(aggParam))->expr);
            Index varno = var->varno;
            AttrNumber varattno = var->varattno;
            bool isInJoinClause = get_real_rte_varno_attno(parentParse, &varno, &varattno);
            if (isInJoinClause) {
                /*
                 * ------ Rule parent 8 -------
                 * agg column in child-query has no intersect with JOIN in parent-query (part of FULL JOIN detected
                 * here)
                 */
                isOK = false;
                /* free memory and return */
                break;
            }

            /* Check are there system column */
            if (varattno <= 0) {
                /*
                 * ------ Rule parent 7 -------
                 * Do not cover conditions of attnum <= 0
                 */
                isOK = false;
                /* free memory and return */
                break;
            }

            if (var->varlevelsup != 0) {
                /* only care about var(s) come from parent-query's level */
                continue;
            }

            if (*targetRTEIndex == 0) {
                /* the first Var paramter we meet in the Aggref list */
                *targetRTEIndex = varno;
            } else if (*targetRTEIndex != varno) {
                /*
                 * ------ Rule parent 4 -------
                 * parent-query's agg function parameter only come from a single table(child-query)
                 */
                isOK = false;
                /* free memory and return */
                break;
            }

            /* update parent-query context by agg function type */
            if (LAZYAGG_SUM == aggType) {
                parentContext->bms_sum = bms_add_member(parentContext->bms_sum, varattno);
                parentContext->sumAggFuncs = list_append_unique(parentContext->sumAggFuncs, aggref);
            } else if (LAZYAGG_MIN == aggType) {
                parentContext->bms_min = bms_add_member(parentContext->bms_min, varattno);
            } else if (LAZYAGG_MAX == aggType) {
                parentContext->bms_max = bms_add_member(parentContext->bms_max, varattno);
            }
        }
    }

    if (aggrefList != NIL) {
        list_free_ext(aggrefList);
    }

    return isOK;
}

/*
 * lazyagg_rewrite_setop_stmt
 *     recursivly modify SetOperationStmt
 *
 * @param (in) varattno:
 *     the target attno
 * @param (in) parentContext:
 *     context of parent-query
 * @param (in) setOpStmt:
 *     SetOperationStmt to recursivly modify
 */
static void lazyagg_rewrite_setop_stmt(
    const AttrNumber varattno, lazyagg_query_context* parentContext, SetOperationStmt* setOpStmt)
{
    /* rewrite parent node of setOpStmt */
    AttrNumber i = 1;
    ListCell* lc = NULL;
    foreach (lc, setOpStmt->colTypes) {
        if (varattno == i) {
            lc->data.oid_value = parentContext->countParamBestType[varattno];
            break;
        }
        ++i;
    }

    if (IsA(setOpStmt->larg, SetOperationStmt)) {
        lazyagg_rewrite_setop_stmt(varattno, parentContext, (SetOperationStmt*)(setOpStmt->larg));
    }
    if (IsA(setOpStmt->rarg, SetOperationStmt)) {
        lazyagg_rewrite_setop_stmt(varattno, parentContext, (SetOperationStmt*)(setOpStmt->rarg));
    }
}

/*
 * lazyagg_rewrite_setop
 *     in delay rewrite and child-query in setop, setOp-query need to be rewrited on it's targetList and setOperations
 *
 * @param (in) parentParse:
 *     query tree of parent-query
 * @param (in) varno:
 *     the RTE index of setOp-query in parent-query
 * @param (in) varattno:
 *     the attno of origin COUNT column in setOp-query
 * @param (in) parentContext:
 *     context of parent-query
 */
static void lazyagg_rewrite_setop(
    Query* parentParse, const Index varno, const AttrNumber varattno, lazyagg_query_context* parentContext)
{
    RangeTblEntry* rte = (RangeTblEntry*)list_nth(parentParse->rtable, varno - 1);
    AssertEreport(rte->rtekind == RTE_SUBQUERY,
        MOD_OPT_REWRITE,
        "RangeTblEntry node should be kind of SUBQUERY in lazyagg_rewrite_setop");
    Query* setOpParse = rte->subquery;

    if (setOpParse->setOperations == NULL) {
        return;
    }

    /* targetList */
    TargetEntry* te = (TargetEntry*)list_nth(setOpParse->targetList, varattno - 1);
    AssertEreport(
        IsA(te->expr, Var), MOD_OPT_REWRITE, "TargetEntry node's expr should be Var in lazyagg_rewrite_setop");
    Var* v = (Var*)(te->expr);
    v->vartype = parentContext->countParamBestType[varattno];

    /* setOperations */
    SetOperationStmt* setOpStmt = (SetOperationStmt*)(setOpParse->setOperations);
    lazyagg_rewrite_setop_stmt(varattno, parentContext, setOpStmt);
}

/*
 * lazyagg_rewrite_join_aliasvars
 *     change types for join range tables when there are tables join with SUM(COUNT) sub-query
 *
 * @param (in) parentParse:
 *     query tree of parent-query
 * @param (in) varno:
 *     the RTE index of join in parent-query
 * @param (in) varattno:
 *     the attno of origin SUM's param column in join
 * @param (in) vartype:
 *     origin type of COUNT's param, we will use it as column's type after we rewriten SUM(COUNT)
 */
static void lazyagg_rewrite_join_aliasvars(Query* parentParse, Index varno, AttrNumber varattno, Oid vartype)
{
    RangeTblEntry* rte = (RangeTblEntry*)list_nth(parentParse->rtable, varno - 1);
    if (rte->rtekind == RTE_JOIN) {
        if (varattno == 0) {
            return;
        } else if (varattno < 0) {
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                    (errmsg("Join range table do not have system column."))));
        }

        Node* node = (Node*)list_nth(rte->joinaliasvars, varattno - 1);
        if (IsA(node, Var)) {
            Var* v = (Var*)node;
            v->vartype = vartype;

            return lazyagg_rewrite_join_aliasvars(parentParse, v->varno, v->varattno, vartype);
        } else if (IsA(node, CoalesceExpr)) {
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                    (errmsg("Invalid agg param which used in a join clause"))));
        } else {
            ereport(ERROR,
                (errmodule(MOD_OPT), errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), (errmsg("Invalid join alias var"))));
        }
    }
}

/*
 * lazyagg_rewrite_childquery
 *     rewrite a child-query
 *
 * @param (in) childParse:
 *     child-query tree
 * @param (in) parentContext:
 *     context of parent-query
 * @param (in) isCheck:
 *     a flag to identify this call is a rewritability checking or not
 *
 * @return:
 *     whether this child-query is rewritable and rewrited, true means is rewritable and rewrited, false otherwise
 */
static bool lazyagg_rewrite_childquery(Query* childParse, lazyagg_query_context* parentContext, bool isCheck)
{
    /* Check wheather this child-query is rewritable */
    if (!isCheck) {
        bool isOK = lazyagg_rewrite_childquery(childParse, parentContext, true);
        if (!isOK) {
            return false;
        }
    }

    /* Traverse target list and find Aggref(s) */
    Index tleIndex = 1;
    ListCell* lc = NULL;
    foreach (lc, childParse->targetList) {
        TargetEntry* te = (TargetEntry*)lfirst(lc);
        Oid toType = exprType((Node*)te->expr);

        /* Used target */
        if (!bms_is_member(tleIndex, parentContext->bms_all)) {
            if (!isCheck) {
                te->expr = (Expr*)makeNullConst(toType, -1, InvalidOid);
            }
            ++tleIndex;
            continue;
        }

        /* Node pointer, used to find Aggref */
        Node* node = (Node*)te->expr;

        /* Just for middle layer of sum(sum(count)) which may have type convert around Aggref */
        if (IsA(node, FuncExpr)) {
            FuncExpr* funcExpr = (FuncExpr*)(node);
            /*
             * For normal case, skip non-type-convert FuncExpr.
             *
             * We can safely do this, because there are only two conditions we could meet FuncExpr here:
             * (1) type convert around Aggref in middle layer of sum(sum(count)) cases
             * (2) normal function in target list without Aggref
             */
            if (NULL == funcExpr->args) {
                ++tleIndex;
                continue;
            }
            node = (Node*)linitial(funcExpr->args);
        } else if (IsA(node, RelabelType)) {
            RelabelType* relabelType = (RelabelType*)(node);
            node = (Node*)(relabelType->arg);
        }

        /* It should be an Aggref now, or it is not an agg function column */
        if (IsA(node, Aggref)) {
            Aggref* aggref = (Aggref*)(node);

            /*
             * If there is no args in Aggref->args, means agg is COUNT(*)
             * If there is only one args in Aggref->args, means other agg functions
             */
            if (0 == list_length(aggref->args)) {
                if (!isCheck) {
                    /* Change COUNT(*) to a const node */
                    Const* cnst = makeConst(INT8OID, -1, InvalidOid, sizeof(int64), Int64GetDatum(1), false, true);
                    te->expr = (Expr*)cnst;
                }
            } else if (1 == list_length(aggref->args)) {
                /*
                 * Link te_inner->expr to te->expr, type convert needed
                 * Extract it here
                 */
                TargetEntry* te_inner = (TargetEntry*)linitial(aggref->args);
                Oid fromType = exprType((Node*)te_inner->expr);
                /* Convert data type if necessary */
                if (fromType == toType) {
                    if (!isCheck) {
                        te->expr = te_inner->expr;
                    }
                } else {
                    if (LAZYAGG_COUNT == lazyagg_get_agg_type(aggref)) {
                        toType = parentContext->countParamBestType[tleIndex];
                    }

                    /* data type convert */
                    if (isCheck) {
                        if (!can_coerce_type(1, &fromType, &toType, COERCION_IMPLICIT)) {
                            return false;
                        }
                    } else {
                        te->expr = (Expr*)coerce_to_target_type(NULL,
                            (Node*)te_inner->expr,
                            fromType,
                            toType,
                            -1,
                            COERCION_IMPLICIT,
                            COERCE_IMPLICIT_CAST,
                            aggref->location);
                    }
                }
            } else {
                AssertEreport(false,
                    MOD_OPT_REWRITE,
                    "It should be an Aggref or not an agg function column in lazyagg_rewrite_childquery");
            }
        }
        ++tleIndex;
    }

    if (!isCheck) {
        /* Modify groupClause and hasAggs of child-query */
        childParse->groupClause = NIL;
        childParse->hasAggs = false;
    }

    return true;
}

/*
 * lazyagg_rewrite_delayed_query
 *     rewrite delayed query, including delayed child-query(s) and their parent-query
 *
 * @param (in) parentParse:
 *     parent-query tree
 * @param (in) delayedChildQueryList:
 *     delayed child-query tree list
 * @param (in) parentContext:
 *     context information of parent-query
 * @param (in) isCheck:
 *     true means this function is used to check wheather delayed query is rewritable, mainly concern on type convert
 *
 * @return:
 *     whether this delay rewrite is rewritable and rewrited, true means is rewritable and rewrited, false otherwise
 */
static bool lazyagg_rewrite_delayed_query(
    Query* parentParse, List* delayedChildQueryList, lazyagg_query_context* parentContext, bool isCheck)
{
    AssertEreport(!bms_is_empty(bms_union(parentContext->bms_count, parentContext->bms_countStar)),
        MOD_OPT_REWRITE,
        "delayed child-query list should not be empty in lazyagg_rewrite_delayed_query");
    AssertEreport(!bms_is_member(0, parentContext->bms_count),
        MOD_OPT_REWRITE,
        "there should be columns marked as has COUNT in child-query in parent-query's context in "
        "lazyagg_rewrite_delayed_query");
    AssertEreport(!bms_is_member(0, parentContext->bms_countStar),
        MOD_OPT_REWRITE,
        "the parent-query's context COUNT flag should not be marked as invalid in lazyagg_rewrite_delayed_query");

    ListCell* lc = NULL;

    if (!isCheck) {
        /*
         * Part 1 in this 'if'
         * Check wheather child-query list and parent-query are rewritable
         */
        foreach (lc, delayedChildQueryList) {
            Query* childParse = (Query*)lfirst(lc);
            bool isChildOK = lazyagg_rewrite_childquery(childParse, parentContext, true);
            if (!isChildOK) {
                return false;
            }
        }
        bool isOK = lazyagg_rewrite_delayed_query(parentParse, delayedChildQueryList, parentContext, true);
        if (!isOK) {
            return false;
        }

        /*
         * Part 2 in this 'if'
         * rewrite child-query list
         */
        foreach (lc, delayedChildQueryList) {
            Query* childParse = (Query*)lfirst(lc);
            (void)lazyagg_rewrite_childquery(childParse, parentContext);
        }
    }

    /* parent-query's delay rewrited Aggref(s) need to be replaced */
    List* replaceSrcList = NIL;
    List* replaceDestList = NIL;

    /* rewrite parent-query, switch SUM to COUNT */
    foreach (lc, parentContext->sumAggFuncs) {
        Aggref* aggref = (Aggref*)lfirst(lc);

        List* aggParam = aggref->args;
        Oid aggOutType = aggref->aggtype;

        AssertEreport(1 == list_length(aggParam),
            MOD_OPT_REWRITE,
            "Aggref's args should be only one in lazyagg_rewrite_delayed_query");
        AssertEreport(IsA(linitial(aggParam), TargetEntry) && IsA(((TargetEntry*)linitial(aggParam))->expr, Var),
            MOD_OPT_REWRITE,
            "Aggref's args should be one TargetEntry with Var expr in lazyagg_rewrite_delayed_query");

        Var* var = (Var*)(((TargetEntry*)linitial(aggParam))->expr);
        Index varno = var->varno;
        AttrNumber varattno = var->varattno;
        bool isInJoinClause = get_real_rte_varno_attno(parentParse, &varno, &varattno);
        if (isInJoinClause) {
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                    (errmsg("Column should NOT be in JOIN clause."))));
        }

        /*
         * If current Aggref in SUM->COUNT flag,
         * change SUM to COUNT, and add a type converter around count
         */
        if (bms_is_member(varattno, parentContext->bms_count) ||
            bms_is_member(varattno, parentContext->bms_countStar)) {
            /* Output of COUNT is INT8OID, convert it to original SUM's output type */
            Oid fromType = INT8OID;
            Oid toType = aggOutType;

            if (isCheck) {
                if (fromType != toType && (!can_coerce_type(1, &fromType, &toType, COERCION_IMPLICIT))) {
                    return false;
                } else {
                    continue;
                }
            }

            /*
             * A new copy to replace old Aggref(s) is need,
             *     because old Aggref may have multiple references in targetList tree
             */
            Aggref* aggref_new = (Aggref*)copyObject(aggref);

            /* Switch SUM to COUNT */
            aggref_new->aggfnoid = ANYCOUNTOID;
            aggref_new->aggtype = INT8OID;
            aggref_new->aggtrantype = lazyagg_get_aggtrantype(aggref_new->aggfnoid);
            aggref_new->location = -1;

            if (bms_is_member(varattno, parentContext->bms_count)) {
                /* rewrite set op, for SUM(COUNT) only */
                lazyagg_rewrite_setop(parentParse, varno, varattno, parentContext);

                /* change Var's vartype in new COUNT in parent-query, for SUM(COUNT) only */
                Var* varOfSuper = (Var*)(((TargetEntry*)linitial(aggref_new->args))->expr);
                Oid vartype = parentContext->countParamBestType[varattno];
                varOfSuper->vartype = vartype;

                /* rewrite join alias var's var type of join rte */
                lazyagg_rewrite_join_aliasvars(parentParse, varOfSuper->varno, varOfSuper->varattno, vartype);
            }

            /* New node to be plugged in */
            Node* newNode = (Node*)aggref_new;

            /* Add a type converter if necessary */
            if (fromType != toType) {
                newNode = coerce_to_target_type(
                    NULL, newNode, fromType, toType, -1, COERCION_IMPLICIT, COERCE_IMPLICIT_CAST, -1);
            }

            /* generate replace lists */
            replaceSrcList = lappend(replaceSrcList, aggref);
            replaceDestList = lappend(replaceDestList, newNode);
        }
    }

    /* Search target list and having qual, and replace Aggref(s) with new node */
    if (replaceSrcList != NIL) {
        parentParse->targetList = (List*)replace_node_clause(
            (Node*)parentParse->targetList, (Node*)replaceSrcList, (Node*)replaceDestList, RNC_NONE);
        parentParse->havingQual =
            replace_node_clause(parentParse->havingQual, (Node*)replaceSrcList, (Node*)replaceDestList, RNC_NONE);
        list_free_ext(replaceSrcList);
        list_free_ext(replaceDestList);
    }

    return true;
}

/*
 * lazyagg_analyze_single_childquery
 *     Process a child-query (which is not a setop)
 *
 * @param (in) parentParse:
 *     parent-query tree
 * @param (in) childParse:
 *     child-query tree
 * @param (in) parentContext:
 *     context information of parent-query
 *
 * @return:
 *     If this child-query need to be delay rewriten, return itself
 */
static Query* lazyagg_analyze_single_childquery(
    Query* parentParse, Query* childParse, lazyagg_query_context* parentContext)
{
    /* context information of child-query */
    lazyagg_query_context* childContext = (lazyagg_query_context*)palloc0(sizeof(lazyagg_query_context));

    /*
     * (1) check the feasibility of child-query
     * (2) check the feasibility of impact between parent-query and child-query
     */
    bool isChildQueryOK = lazyagg_check_childquery_feasibility(parentParse, childParse, parentContext, childContext);
    if (!isChildQueryOK) {
        lazyagg_free_querycontext(childContext);
        return NULL;
    }

    /* SUM need to be changed in parent-query because of COUNT(column) in child-query */
    Bitmapset* parentSumToCount = bms_intersect(parentContext->bms_sum, childContext->bms_count);

    /* SUM need to be changed in parent-query because of COUNT(*) in child-query */
    Bitmapset* parentSumToCountStar = bms_intersect(parentContext->bms_sum, childContext->bms_countStar);

    /*
     * If there is no SUM(COUNT) pattern,
     * the child-query need to be applyed to another Lazy Agg rule process first as a parent-query,
     * before it is processed in current Lazy Agg rule process
     */
    if (bms_is_empty(parentSumToCount) && bms_is_empty(parentSumToCountStar)) {
        /* Search Lazy Agg between child-query and child-child-query recursively */
        Query* childParse_new = lazyagg_main(childParse);

        /*
         * Check is the child-query and child-child-query is delayed rewriten:
         * If lazy agg of child-query and child-child-query is delayed,
         *     means the child-query's agg func has been changed from sum to count.
         *     In this condition, childContext need to be updated.
         */
        if (NULL != childParse_new) {
            AssertEreport(childParse == childParse_new,
                MOD_OPT_REWRITE,
                "child-query mismatch in lazyagg_analyze_single_childquery");

            /* Update SUM/COUNT bms of childContext, clear and set */
            lazyagg_clear_childcontext(childContext);
            (void)lazyagg_check_childquery_feasibility(parentParse, childParse, parentContext, childContext, true);

            /* Update bms that identify delay rewrite property of parent-query layer */
            parentSumToCount = bms_intersect(parentContext->bms_sum, childContext->bms_count);
            parentSumToCountStar = bms_intersect(parentContext->bms_sum, childContext->bms_countStar);
        }

        /*
         * If Lazy Agg of child-query and child-child-query is not delayed,
         * or there is no SUM(COUNT) pattern (also may be an unused COUNT in child-query),
         *     rewrite the child-query as normal.
         */
        if (bms_is_empty(parentSumToCount) && bms_is_empty(parentSumToCountStar)) {
            /* Rewrite the child-query */
            (void)lazyagg_rewrite_childquery(childParse, parentContext);

            bms_free_ext(parentSumToCount);
            bms_free_ext(parentSumToCountStar);
            lazyagg_free_querycontext(childContext);
            return NULL;
        }
    }

    /* has pattern of SUM(COUNT) */
    AssertEreport(!bms_is_empty(bms_union(parentSumToCount, parentSumToCountStar)),
        MOD_OPT_REWRITE,
        "Should has pattern of SUM(COUNT) in lazyagg_analyze_single_childquery");

    /* check compatibility of delay rewrite */
    bool isDelayRewriteOK =
        lazyagg_check_delay_rewrite_compatibility(parentContext, childContext, parentSumToCount, parentSumToCountStar);

    lazyagg_free_querycontext(childContext);

    if (isDelayRewriteOK) {
        return childParse;
    } else {
        return NULL;
    }
}

/*
 * lazyagg_search_setoperationstmt
 *     Process arg of UNION ALL
 *     Two conditions:
 *	   (1) arg is a RangeTblRef,
 *	   (2) arg is also a SetOperationStmt, process it recursively with lazyAgg_analyzeSetOpChildQuery
 *
 * @param (in) parentParse:
 *     parent-query tree
 * @param (in) setOpParse:
 *     child-query tree (which is a setop query)
 * @param (in) parentContext:
 *     context information of parent-query
 * @param (in) setOpStmtArg:
 *     SetOperationStmt's arg of current layer (each setop operator has two arg, left and right, both use this function)
 *
 * @return:
 *     list of delayed child-query (which are all COUNT type)
 */
static List* lazyagg_search_setoperationstmt(
    Query* parentParse, Query* setOpParse, lazyagg_query_context* parentContext, Node* setOpStmtArg)
{
    List* delayedChildQueryList = NIL;

    if (IsA(setOpStmtArg, RangeTblRef)) {
        /* extract the child-query tree from setop query */
        int rtindex = ((RangeTblRef*)setOpStmtArg)->rtindex;
        RangeTblEntry* rte = (RangeTblEntry*)list_nth(setOpParse->rtable, rtindex - 1);
        AssertEreport(rte->rtekind == RTE_SUBQUERY,
            MOD_OPT_REWRITE,
            "RangeTblEntry of child-query tree should be kind of SUBQUERY in lazyagg_search_setoperationstmt");
        Query* childParse = rte->subquery;

        /* Process child-query */
        Query* delayedChildQuery = lazyagg_analyze_single_childquery(parentParse, childParse, parentContext);

        /* Save delayed child-query list */
        if (NULL != delayedChildQuery) {
            delayedChildQueryList = lappend(delayedChildQueryList, delayedChildQuery);
        }
    } else if (IsA(setOpStmtArg, SetOperationStmt)) {
        /* Process set operation, and save delayed child-query list */
        delayedChildQueryList =
            lazyagg_analyze_setop_childquery(parentParse, setOpParse, parentContext, (SetOperationStmt*)setOpStmtArg);
    } else {
        AssertEreport(false,
            MOD_OPT_REWRITE,
            "SetOperationStmt's arg should be a RangeTblRef or SetOperationStmt in lazyagg_search_setoperationstmt");
    }

    return delayedChildQueryList;
}

/*
 * lazyagg_analyze_setop_childquery
 *     process setop child-query recursively on SetOperationStmt
 *
 * @param (in) parentParse:
 *     parent-query tree
 * @param (in) setOpParse:
 *     child-query tree (which is a setop query)
 * @param (in) parentContext:
 *     context information of parent-query
 * @param (in) setOpStmt:
 *     SetOperationStmt of current layer (each setop operator has two child, left and right)
 *
 * @return:
 *     list of delayed child-query (which are all COUNT type)
 */
static List* lazyagg_analyze_setop_childquery(
    Query* parentParse, Query* setOpParse, lazyagg_query_context* parentContext, SetOperationStmt* setOpStmt)
{
    AssertEreport(setOpParse->setOperations != NULL,
        MOD_OPT_REWRITE,
        "set-operations of child-query tree should not be NULL in lazyagg_analyze_setop_childquery");

    /* recursively process UNION ALL, and return others */
    if (!(setOpStmt->op == SETOP_UNION && setOpStmt->all)) {
        return NIL;
    }

    List* delayedChildQueryList = NIL;

    List* delayedChildQueryList_left =
        lazyagg_search_setoperationstmt(parentParse, setOpParse, parentContext, setOpStmt->larg);
    List* delayedChildQueryList_right =
        lazyagg_search_setoperationstmt(parentParse, setOpParse, parentContext, setOpStmt->rarg);

    /*
     * If one arg returns no delayed child-query, means this arg is invalid to Lazy Agg rule, or it has been rewriten
     * without delay In this condition, the other child-query(s) of the same parent-query should not be delay rewriten,
     * skip it
     */
    if (NIL != delayedChildQueryList_left && NIL != delayedChildQueryList_right) {
        delayedChildQueryList = list_concat(delayedChildQueryList, delayedChildQueryList_left);
        delayedChildQueryList = list_concat(delayedChildQueryList, delayedChildQueryList_right);
        return delayedChildQueryList;
    } else {
        if (NIL != delayedChildQueryList_left) {
            list_free_ext(delayedChildQueryList_left);
        }
        if (NIL != delayedChildQueryList_right) {
            list_free_ext(delayedChildQueryList_right);
        }
        return NIL;
    }
}

/*
 * lazyagg_analyze_childquery
 *     main entry to process child-query
 *     (1) this is a router for child-query to setop or agg-query
 *     (2) by the end of processing, rewrite delayed query if there it is
 *
 * @param (in) parentParse:
 *     parent-query tree
 * @param (in) childParse:
 *     child-query tree
 * @param (in) parentContext:
 *     context information of parent-query
 *
 * @return:
 *     whether this child-query is successfully delay rewrited query
 */
static bool lazyagg_analyze_childquery(Query* parentParse, Query* childParse, lazyagg_query_context* parentContext)
{
    List* delayedChildQueryList = NIL;
    bool isDelayedQuery = false;

    /*
     * Two kinds of child-query are take into concern:
     * (1) set operation
     * (2) child-query with agg
     */
    if (childParse->setOperations != NULL) {
        /* Process set operation, and save delayed child-query list */
        delayedChildQueryList = lazyagg_analyze_setop_childquery(
            parentParse, childParse, parentContext, ((SetOperationStmt*)childParse->setOperations));
    } else if (childParse->groupClause != NIL) {
        /* Process agg child-query */
        Query* delayedChildQuery = lazyagg_analyze_single_childquery(parentParse, childParse, parentContext);

        /* Save delayed child-query list */
        if (delayedChildQuery != NULL) {
            delayedChildQueryList = list_make1(delayedChildQuery);
        }
    } else {
        return false;
    }

    /*
     * rewrite delayed query
     * (1) delayed child-query list is not empty
     * (2) there are columns marked as has COUNT in child-query in parent-query's context
     * (3) the parent-query's context COUNT flag is not marked as invalid
     */
    if (delayedChildQueryList != NIL &&
        (!bms_is_empty(bms_union(parentContext->bms_count, parentContext->bms_countStar))) &&
        (!bms_is_member(0, parentContext->bms_count)) && (!bms_is_member(0, parentContext->bms_countStar))) {
        /* rewrite delayed query */
        bool isSuccessfullyRewriten = lazyagg_rewrite_delayed_query(parentParse, delayedChildQueryList, parentContext);

        /* if rewriten is successful, mark parent-query as delayed */
        isDelayedQuery = isSuccessfullyRewriten;
    }

    if (delayedChildQueryList != NIL) {
        list_free_ext(delayedChildQueryList);
    }
    return isDelayedQuery;
}

/*
 * lazyagg_main
 *     main entry for Lazy Agg rewrite rule
 *
 * @param (in) parse:
 *     the query tree need to be rewriten
 *
 * @return:
 *     NULL: the query is not valid for Lazy Agg rule, or only child-query is rewriten
 *     Query: the child-query is rewriten and the parent-query is also rewriten
 *         (SUM in parent-query has been changed to COUNT)
 */
Query* lazyagg_main(Query* parse)
{
    /* There need to be only one child-query which can be applied to Lazy Agg, except for there is no agg func in
     * parent-query */
    Index targetRTEIndex = 0;
    bool isDelayedQuery = false;

    lazyagg_query_context* parentContext = (lazyagg_query_context*)palloc0(sizeof(lazyagg_query_context));

    /* Check the feasibility of parent-query, get target child-query's RTE index, and init part of parent-query's
     * context */
    bool isParentQueryOK = lazyagg_check_parentquery_feasibility(parse, &targetRTEIndex, parentContext);
    if (!isParentQueryOK) {
        lazyagg_free_querycontext(parentContext);
        return NULL;
    }

    /* Traverse RTE list of parent-query and find the target child-query */
    Index rteIndex = 1;
    ListCell* lc = NULL;
    foreach (lc, parse->rtable) {
        RangeTblEntry* rte = (RangeTblEntry*)lfirst(lc);
        if (RTE_SUBQUERY == rte->rtekind && !rte->subquery->unique_check) {
            Query* childParse = rte->subquery;

            /* child-query need to be a setop or has GROUP BY */
            if ((childParse->setOperations == NULL) && (childParse->groupClause == NIL)) {
                ++rteIndex;
                continue;
            }

            /*
             * targetRTEIndex is '0', means no agg function in target list of parent-query, all child-query need to be
             * rewriten. targetRTEIndex is not '0', means there is a single child-query in target list of parent-query,
             * find it and rewrite it.
             */
            if (targetRTEIndex == 0 || targetRTEIndex == rteIndex) {
                /* Init second part of parentContext */
                lazyagg_set_parentcontext_with_child(parse, rteIndex, parentContext);

                /*
                 * Rewrite child-query,
                 * if the parent-query has also been rewriten, mark it as delayed query
                 */
                isDelayedQuery = lazyagg_analyze_childquery(parse, childParse, parentContext);

                if (targetRTEIndex == 0) {
                    /*
                     * Revert second part of parent-query's context to rewrite other child-query(s)
                     * Do not need to revert bms_count and bms_countStar,
                     * because SUM(COUNT) is impossible when there are no agg function in parent-query
                     */
                    bms_free_ext(parentContext->bms_joinWhereGroupby);
                    bms_free_ext(parentContext->bms_all);
                    parentContext->bms_joinWhereGroupby = NULL;
                    parentContext->bms_all = NULL;
                    parentContext->isInnerSide = false;
                } else if (targetRTEIndex == rteIndex) {
                    /* No other child-query can be rewrite except the target child-query */
                    break;
                }
            }
        }
        ++rteIndex;
    }

    lazyagg_free_querycontext(parentContext);

    /*
     * If parent-query is delayed rewriten (say sum(count) pattern)
     * the query tree need to be returned for parent-query's parent-query
     */
    return (isDelayedQuery) ? parse : NULL;
}

/* ------------------------------------------------------------ */
/* Lazy Agg : end                                             */
/* ------------------------------------------------------------ */
/* ------------------------------------------------------------ */
/* Reduce orderby : begin                                     */
/* ------------------------------------------------------------ */
static void reduce_orderby_final(RangeTblEntry* rte, bool reduce);
static void reduce_orderby_recurse(Query* query, Node* jtnode, bool reduce);

/* Reduce orderby clause in subquery for join and setop */
/*
 * reduce_orderby_recurse
 *     recurse check join node and reduce order by final for RangeTblRef.
 *
 * @param (in) query: the query tree for reduce orderby
 * @param (in) jtnode: join node for RangeTblRef/FromExpr/JoinExpr/SetOperationStmt
 *
 * @return: void
 */
static void reduce_orderby_recurse(Query* query, Node* jtnode, bool reduce)
{
    if (jtnode == NULL)
        return;

    if (IsA(jtnode, RangeTblRef)) {
        int varno = ((RangeTblRef*)jtnode)->rtindex;
        RangeTblEntry* rte = rt_fetch(varno, query->rtable);

        /* Reduce orderby clause in subquery for join or from clause of more than one rte */
        reduce_orderby_final(rte, reduce);
    } else if (IsA(jtnode, FromExpr)) {
#ifndef ENABLE_MULTIPLE_NODES
        /* If there is ROWNUM, can not reduce orderby clause in subquery from fromlist.
         * For example, If there is a SQL {select * from table_name where rownum < n union
         * select * from (select * from table_name order by column_name desc) where rownum < n;},
         * can not reduce orderby clause here.
         */
        if (contain_rownum_walker(jtnode, NULL)) {
            return;
        }
#endif
        FromExpr* f = (FromExpr*)jtnode;
        ListCell* l = NULL;
        bool flag = false;

        if (1 == list_length(f->fromlist))
            flag = reduce;
        else
            flag = true;

        foreach (l, f->fromlist)
            reduce_orderby_recurse(query, (Node*)lfirst(l), flag);
    } else if (IsA(jtnode, JoinExpr)) {
        JoinExpr* j = (JoinExpr*)jtnode;

        if ((JOIN_INNER == j->jointype) || (JOIN_LEFT == j->jointype) || (JOIN_SEMI == j->jointype) ||
            (JOIN_ANTI == j->jointype) || (JOIN_FULL == j->jointype) || (JOIN_RIGHT == j->jointype) ||
            (JOIN_LEFT_ANTI_FULL == j->jointype) || (JOIN_RIGHT_ANTI_FULL == j->jointype)) {
            reduce_orderby_recurse(query, j->larg, true);
            reduce_orderby_recurse(query, j->rarg, true);
        } else {
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("unrecognized join type: %d", (int)j->jointype)));
        }
    } else if (IsA(jtnode, SetOperationStmt)) {
        SetOperationStmt* op = (SetOperationStmt*)jtnode;

        reduce_orderby_recurse(query, op->larg, true);
        reduce_orderby_recurse(query, op->rarg, true);
    }

    return;
}

/*
 * reduce_orderby_final
 *     Reduce order by clause for subquery for join and setop if the subquery have sortClause.
 *
 * @param (in) rte: the RTE of subquery for reduce orderby.
 * @param (in) reduce: the flag identify if reduce the orderby clause or not.
 *
 * @return: void
 */
static void reduce_orderby_final(RangeTblEntry* rte, bool reduce)
{
    /* Reduce orderby clause in subquery for join or from clause of more than one rte */
    if (rte->rtekind == RTE_SUBQUERY) {
        if (reduce && rte->subquery->sortClause && !rte->subquery->limitOffset && !rte->subquery->limitCount) {
            pfree_ext(rte->subquery->sortClause);
            rte->subquery->sortClause = NULL;
        }

        reduce_orderby(rte->subquery, reduce);
    }

    return;
}

/*
 * reduce_orderby
 *     The entry of reduce orderby which called by subquery_planner.
 *
 * @param (in) query: the query tree for reduce orderby.
 * @param (in) reduce: the flag identify if reduce the orderby clause or not.
 *
 * @return: void
 */
void reduce_orderby(Query* query, bool reduce)
{
    ListCell* l = NULL;
    RangeTblEntry* rte = NULL;

    if (query == NULL)
        return;

    /* If subquery in insert or update, it should find select query and deside whether reduce order by in subquery or
     * not.  */
    if (query->commandType != CMD_SELECT) {
        foreach (l, query->rtable) {
            rte = (RangeTblEntry*)lfirst(l);
            if (rte->rtekind == RTE_SUBQUERY)
                reduce_orderby(rte->subquery, reduce);
        }
        return;
    }

    /* Recurse find subquery in form,join or subquery, and deside whether reduce order by in subquery or not. */
    reduce_orderby_recurse(query, (Node*)query->jointree, reduce);

    /* If there has setop, it should optimize orderby clause. */
    if (query->setOperations) {
        reduce_orderby_recurse(query, ((SetOperationStmt*)query->setOperations)->larg, true);
        reduce_orderby_recurse(query, ((SetOperationStmt*)query->setOperations)->rarg, true);
    }
}

/* ------------------------------------------------------------ */
/* Reduce orderby : end                                       */
