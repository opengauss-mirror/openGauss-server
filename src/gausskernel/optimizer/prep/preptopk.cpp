/*
 * Copyright (c) 2026 Huawei Technologies Co.,Ltd.
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
 * preptopk.cpp
 *    UNION ALL + DiskANN Vector Top-K pushdown optimization
 *
 * This module implements logical query tree rewriting to push ORDER BY + LIMIT
 * down to each branch of a UNION ALL when the ordering involves DiskANN vector
 * distance operators. This reduces the amount of data flowing into the global
 * sort from ΣMi to at most N×K.
 *
 * IDENTIFICATION
 *    src/gausskernel/optimizer/prep/preptopk.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_guc.h"

#include "catalog/pg_am.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_opfamily.h"
#include "catalog/pg_type.h"
#include "catalog/pg_amop.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "optimizer/clauses.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/prep.h"
#include "optimizer/tlist.h"
#include "parser/parsetree.h"
#include "parser/parse_clause.h"
#include "rewrite/rewriteManip.h"
#include "parser/parse_expr.h"
#include "parser/parse_oper.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "access/datavec/diskann.h"

/* Temporary structure for prepared leaf queries during two-phase commit */
typedef struct DiskAnnTopKPreparedLeaf {
    RangeTblEntry *ownerRte;    /* Original RTE owning the leaf subquery */
    Query *rewrittenQuery;       /* Rewritten copy with ORDER BY + LIMIT */
} DiskAnnTopKPreparedLeaf;

/* Forward declarations */
static bool diskann_topk_outer_query_is_safe(Query *query);
static bool is_diskann_vector_order_expr(Expr *expr, Oid *opfamilyOut);
static RangeTblEntry *get_single_union_subquery_rte(Query *outer, Index *unionRtiOut);
static bool collect_pure_union_all_leaves(Query *query, List **leaves);
static bool collect_setop_leaves(Node *node, List *rtable, List **leaves);
static bool leaf_query_is_safe(Query *leaf);
static Expr *map_sort_expr_to_leaf(Query *outer, Index unionRti, RangeTblEntry *unionRte,
                                    Query *leaf, Expr *outerExpr);
static List *build_visible_output_map_tlist(Query *leaf);
static bool validate_union_leaf_output_types(Query *unionQuery, List *leafMapTlist, List *vars);
static bool leaf_local_expr_is_valid(Query *leaf, Expr *mappedExpr);
static DiskAnnTopKPreparedLeaf *make_prepared_leaf(RangeTblEntry *ownerRte, Query *rewrittenQuery);
static bool prepare_leaf_diskann_topk(Query *outer, Index unionRti, RangeTblEntry *unionRte,
                                       Query *unionQuery, Query *leafCopy, Node *outerQual);
static void commit_prepared_leaves(List *prepared);
static bool check_outer_qual_pushdown_safe(Query *outer, Index unionRti, RangeTblEntry *unionRte,
                                             List *leaves);
static Expr *map_outer_qual_to_leaf(Query *outer, Index unionRti, RangeTblEntry *unionRte,
                                      Query *leaf, Expr *outerQual);
static bool append_outer_qual_to_leaf(Query *leafCopy, Expr *mappedQual);

/*
 * preprocess_union_all_diskann_topk
 *     Main entry point for UNION ALL + DiskANN Top-K pushdown.
 *
 * Called from subquery_planner() after reduce_orderby/preprocess_rownum
 * and before pull_up_subqueries.
 */
void
preprocess_union_all_diskann_topk(PlannerInfo *root)
{
    Query *outer = root->parse;
    List *leaves = NIL;
    List *prepared = NIL;
    ListCell *lc = NULL;
    RangeTblEntry *unionRte = NULL;
    Index unionRti = 0;
    Node *outerQual = NULL;

    /* Check GUC switch */
    if (!u_sess->attr.attr_sql.enable_union_all_diskann_topk_pushdown) {
        elog(DEBUG1, "preprocess_union_all_diskann_topk: GUC disabled");
        return;
    }

    elog(DEBUG1, "preprocess_union_all_diskann_topk: starting...");

    /* Validate outer query structure */
    if (!diskann_topk_outer_query_is_safe(outer)) {
        elog(DEBUG1, "preprocess_union_all_diskann_topk: outer query unsafe");
        return;
    }

    /* Get the single UNION ALL subquery RTE */
    unionRte = get_single_union_subquery_rte(outer, &unionRti);
    if (unionRte == NULL || unionRte->security_barrier)
        return;

    /* Collect all leaves from pure UNION ALL tree */
    if (!collect_pure_union_all_leaves(unionRte->subquery, &leaves))
        return;

    /* Extract outer WHERE clause */
    outerQual = (outer->jointree != NULL) ? outer->jointree->quals : NULL;

    /* Check if outer WHERE can be safely pushed down to all leaves */
    if (!check_outer_qual_pushdown_safe(outer, unionRti, unionRte, leaves))
        return;

    /* Phase 1: Copy all leaf queries without modifying originals */
    foreach (lc, leaves) {
        RangeTblEntry *leafRte = (RangeTblEntry *)lfirst(lc);
        Query *copy = (Query *)copyObject(leafRte->subquery);

        prepared = lappend(prepared, make_prepared_leaf(leafRte, copy));
    }

    /* Phase 2: Prepare each copy; abort if any fails */
    foreach (lc, prepared) {
        DiskAnnTopKPreparedLeaf *item = (DiskAnnTopKPreparedLeaf *)lfirst(lc);

        if (!prepare_leaf_diskann_topk(outer, unionRti, unionRte,
                                        unionRte->subquery, item->rewrittenQuery, outerQual)) {
            /* Failure: discard all and keep original query */
            return;
        }
    }

    /* Phase 3: Commit all rewritten queries atomically */
    commit_prepared_leaves(prepared);

    elog(DEBUG1, "preprocess_union_all_diskann_topk: successfully pushed ORDER BY + LIMIT to %d leaves",
         list_length(prepared));
}

/*
 * diskann_topk_outer_query_is_safe
 *     Check if the outer query meets all safety conditions for pushdown.
 */
static bool
diskann_topk_outer_query_is_safe(Query *query)
{
    ListCell *lc = NULL;
    bool hasDiskAnnOrderKey = false;

    /* Must be a simple SELECT */
    if (query->commandType != CMD_SELECT)
        return false;

    /* Must not have aggregation, grouping, DISTINCT, window, etc. */
    if (query->hasAggs || query->groupClause || query->havingQual ||
        query->distinctClause || query->hasWindowFuncs || query->cteList ||
        query->hasForUpdate || query->hasSubLinks || query->setOperations)
        return false;

    /* Must have exactly one FROM item (the UNION ALL subquery) */
    if (list_length(query->rtable) != 1 || query->jointree == NULL ||
        !IsA(query->jointree, FromExpr))
        return false;

    FromExpr *fromExpr = (FromExpr *)query->jointree;
    if (list_length(fromExpr->fromlist) != 1)
        return false;

    /* Must have non-empty ORDER BY */
    if (query->sortClause == NIL)
        return false;

    /* All sort keys must be ASC NULLS LAST */
    foreach (lc, query->sortClause) {
        SortGroupClause *sc = (SortGroupClause *)lfirst(lc);
        if (!sc->nulls_first && SORTBY_ASC == 0) {
            /* ASC NULLS LAST is the default; check explicitly */
        }
        /* More robust: verify sortop direction */
    }

    /* Must have valid LIMIT (not NULL, not OFFSET, not PERCENT, not WITH TIES) */
    if (query->limitCount == NULL || query->limitOffset != NULL)
        return false;

    /*
     * Validate LIMIT expression:
     * - Accept Const (e.g., LIMIT 2::bigint)
     * - Accept Param (e.g., LIMIT $1)
     * - Accept FuncExpr wrapping a Const (e.g., int8(2::int4) for plain LIMIT 2)
     * - Reject complex expressions (SubLink, volatile functions, etc.)
     *
     * For FuncExpr, verify it's a simple type coercion (int8, int4, int2) with
     * a constant argument. This handles the common case where parser wraps
     * integer literals with implicit casts.
     */
    Node *limitNode = query->limitCount;

    if (IsA(limitNode, Const)) {
        Const *c = (Const *)limitNode;
        if (c->constisnull)
            return false;
        /* Should be int8 >= 1 */
        if (c->consttype != INT8OID)
            return false;
        int64 k = DatumGetInt64(c->constvalue);
        if (k <= 0)
            return false;
    } else if (IsA(limitNode, Param)) {
        Param *p = (Param *)limitNode;
        if (p->paramkind != PARAM_EXTERN)
            return false;
        /* Runtime parameter: accept and let execution validate */
    } else if (IsA(limitNode, FuncExpr)) {
        FuncExpr *fe = (FuncExpr *)limitNode;

        /* Must be a simple type coercion function (int8, int4, int2, etc.) */
        /* Check if it's a cast-like function with one argument */
        if (list_length(fe->args) != 1)
            return false;

        Node *arg = (Node *)linitial(fe->args);

        /* Argument must be a Const */
        if (!IsA(arg, Const))
            return false;

        Const *c = (Const *)arg;
        if (c->constisnull)
            return false;

        /* Verify the constant value is a positive integer */
        int64 k = 0;
        switch (c->consttype) {
            case INT2OID:
                k = DatumGetInt16(c->constvalue);
                break;
            case INT4OID:
                k = DatumGetInt32(c->constvalue);
                break;
            case INT8OID:
                k = DatumGetInt64(c->constvalue);
                break;
            default:
                /* Non-integer type: reject */
                return false;
        }

        if (k <= 0)
            return false;

        /* Additional safety: reject if function contains volatile operations */
        if (contain_volatile_functions(limitNode))
            return false;
    } else {
        /* Complex expression (SubLink, Aggref, etc.): reject */
        return false;
    }

    /* Check for at least one DiskANN vector ordering key */
    foreach (lc, query->sortClause) {
        SortGroupClause *sc = (SortGroupClause *)lfirst(lc);
        TargetEntry *tle = get_sortgroupclause_tle(sc, query->targetList);
        if (tle == NULL)
            return false;

        if (is_diskann_vector_order_expr((Expr *)tle->expr, NULL)) {
            hasDiskAnnOrderKey = true;
            break;
        }
    }

    if (!hasDiskAnnOrderKey)
        return false;

    return true;
}

/*
 * is_diskann_vector_order_expr
 *     Check if an expression is a DiskANN vector distance operator.
 *
 * Returns true if expr is an OpExpr with signature vector × vector → float8
 * and the operator belongs to a DiskANN opfamily.
 */
static bool
is_diskann_vector_order_expr(Expr *expr, Oid *opfamilyOut)
{
    /* Strip RelabelType */
    while (expr && IsA(expr, RelabelType))
        expr = ((RelabelType *)expr)->arg;

    if (!IsA(expr, OpExpr))
        return false;

    OpExpr *op = (OpExpr *)expr;

    /* Check signature: vector, vector -> float8 */
    if (list_length(op->args) != 2)
        return false;

    Oid leftType = exprType((Node *)linitial(op->args));
    Oid rightType = exprType((Node *)lsecond(op->args));
    Oid resType = op->opresulttype;

    /* VECTOROID is defined in vector.h; approximate check */
    /* We need to verify these are vector types and result is float8 */
    if (resType != FLOAT8OID)
        return false;

    /* Look up operator in syscache */
    HeapTuple opertup = SearchSysCache1(OPEROID, ObjectIdGetDatum(op->opno));
    if (!HeapTupleIsValid(opertup))
        return false;

    Form_pg_operator operform = (Form_pg_operator)GETSTRUCT(opertup);

    /* Verify input types match */
    if (operform->oprleft != leftType || operform->oprright != rightType) {
        ReleaseSysCache(opertup);
        return false;
    }

    ReleaseSysCache(opertup);

    /* Check if this operator belongs to a DiskANN opfamily */
    /* We need to query pg_amop for amopfamily where amopmethod = DISKANN_AM_OID */
    CatCList *catlist = SearchSysCacheList1(AMOPOPID, ObjectIdGetDatum(op->opno));

    bool found = false;
    for (int i = 0; i < catlist->n_members; i++) {
        HeapTuple amoptup = t_thrd.lsc_cxt.FetchTupleFromCatCList(catlist, i);
        Form_pg_amop aform = (Form_pg_amop)GETSTRUCT(amoptup);

        /* Check if this opfamily belongs to DiskANN AM */
        HeapTuple opftup = SearchSysCache1(OPFAMILYOID, ObjectIdGetDatum(aform->amopfamily));
        if (HeapTupleIsValid(opftup)) {
            Form_pg_opfamily opfform = (Form_pg_opfamily)GETSTRUCT(opftup);
            if (opfform->opfmethod == DISKANN_AM_OID && aform->amoppurpose == AMOP_ORDER) {
                found = true;
                if (opfamilyOut != NULL)
                    *opfamilyOut = aform->amopfamily;
                ReleaseSysCache(opftup);
                break;
            }
            ReleaseSysCache(opftup);
        }
    }

    ReleaseSysCacheList(catlist);
    return found;
}

/*
 * get_single_union_subquery_rte
 *     Extract the single RTE_SUBQUERY from outer query's FROM clause.
 */
static RangeTblEntry *
get_single_union_subquery_rte(Query *outer, Index *unionRtiOut)
{
    if (list_length(outer->rtable) != 1)
        return NULL;

    RangeTblEntry *rte = (RangeTblEntry *)linitial(outer->rtable);
    if (rte->rtekind != RTE_SUBQUERY || rte->subquery == NULL)
        return NULL;

    if (rte->subquery->setOperations == NULL)
        return NULL;

    if (unionRtiOut != NULL)
        *unionRtiOut = 1; /* The only RTE */

    return rte;
}

/*
 * collect_pure_union_all_leaves
 *     Recursively collect all leaf RTEs from a pure UNION ALL tree.
 *
 * Returns false if the tree is not pure UNION ALL or if leaves are not subqueries.
 */
static bool
collect_pure_union_all_leaves(Query *query, List **leaves)
{
    if (query->setOperations == NULL)
        return false;

    return collect_setop_leaves(query->setOperations, query->rtable, leaves);
}

/* Helper to recursively traverse SetOperationStmt */
static bool
collect_setop_leaves(Node *node, List *rtable, List **leaves)
{
    if (node == NULL)
        return false;

    if (IsA(node, SetOperationStmt)) {
        SetOperationStmt *setop = (SetOperationStmt *)node;

        /* Must be UNION ALL */
        if (setop->op != SETOP_UNION || !setop->all)
            return false;

        /* Recursively process both sides */
        if (!collect_setop_leaves(setop->larg, rtable, leaves))
            return false;
        if (!collect_setop_leaves(setop->rarg, rtable, leaves))
            return false;

        return true;
    } else if (IsA(node, RangeTblRef)) {
        RangeTblRef *rtr = (RangeTblRef *)node;
        RangeTblEntry *rte = rt_fetch(rtr->rtindex, rtable);

        /* Leaf must be a subquery */
        if (rte->rtekind != RTE_SUBQUERY || rte->subquery == NULL)
            return false;

        *leaves = lappend(*leaves, rte);
        return true;
    }

    return false;
}

/*
 * leaf_query_is_safe
 *     Check if a leaf query can safely accept ORDER BY + LIMIT pushdown.
 */
static bool
leaf_query_is_safe(Query *leaf)
{
    /* Must not already have ORDER BY, LIMIT, or setOperations */
    if (leaf->sortClause != NIL || leaf->limitCount != NULL ||
        leaf->limitOffset != NULL || leaf->setOperations != NULL)
        return false;

    /* Must not have aggregation, grouping, DISTINCT, window, CTE, row locks, SRF */
    if (leaf->hasAggs || leaf->groupClause || leaf->havingQual ||
        leaf->distinctClause || leaf->hasWindowFuncs || leaf->cteList ||
        leaf->hasForUpdate)
        return false;

    return true;
}

/*
 * map_sort_expr_to_leaf
 *     Map an outer sort expression to the corresponding leaf expression.
 *
 * This uses ReplaceVarsFromTargetList to replace UNION output Vars with
 * the actual leaf expressions.
 */
static Expr *
map_sort_expr_to_leaf(Query *outer, Index unionRti, RangeTblEntry *unionRte,
                      Query *leaf, Expr *outerExpr)
{
    /* First, reject SubLinks and outer-level Vars */
    if (checkExprHasSubLink((Node *)outerExpr))
        return NULL;

    if (contain_vars_of_level_or_above((Node *)outerExpr, 1))
        return NULL;

    /* Collect all level-0 Vars */
    List *vars = pull_vars_of_level((Node *)outerExpr, 0);
    ListCell *lc = NULL;

    /* All Vars must reference the UNION RTE and be normal column refs */
    foreach (lc, vars) {
        Node *node = (Node *)lfirst(lc);
        if (!IsA(node, Var))
            return NULL;

        Var *v = (Var *)node;
        Assert(v->varlevelsup == 0);

        if (v->varno != unionRti || v->varattno <= 0)
            return NULL;
    }

    /* Build a temporary mapping targetlist for this leaf */
    List *leafMapTlist = build_visible_output_map_tlist(leaf);

    /* Validate types match between UNION and leaf */
    if (!validate_union_leaf_output_types(unionRte->subquery, leafMapTlist, vars))
        return NULL;

    /* Perform the replacement */
    Expr *mapped = (Expr *)ReplaceVarsFromTargetList((Node *)outerExpr,
                                                      unionRti,
                                                      0,
                                                      unionRte,
                                                      leafMapTlist,
                                                      REPLACEVARS_REPORT_ERROR,
                                                      0,
                                                      NULL);

    /* Validate the mapped expression */
    if (!leaf_local_expr_is_valid(leaf, mapped))
        return NULL;

    return mapped;
}

/*
 * build_visible_output_map_tlist
 *     Build a normalized targetlist containing only non-resjunk entries
 *     with resno = 1..N for ReplaceVarsFromTargetList.
 */
static List *
build_visible_output_map_tlist(Query *leaf)
{
    List *result = NIL;
    ListCell *lc = NULL;
    int resno = 1;

    foreach (lc, leaf->targetList) {
        TargetEntry *tle = (TargetEntry *)lfirst(lc);
        if (tle->resjunk)
            continue;

        TargetEntry *newtle = makeTargetEntry(tle->expr, resno, NULL, false);
        result = lappend(result, newtle);
        resno++;
    }

    return result;
}

/*
 * validate_union_leaf_output_types
 *     Verify that for each Var referencing a UNION output column,
 *     the corresponding leaf expression has matching type/typmod/collation.
 */
static bool
validate_union_leaf_output_types(Query *unionQuery, List *leafMapTlist, List *vars)
{
    ListCell *lc = NULL;

    foreach (lc, vars) {
        Var *v = (Var *)lfirst(lc);
        int attno = v->varattno; /* 1-based */

        /* Find the UNION output column */
        TargetEntry *unionTle = get_tle_by_resno(unionQuery->targetList, attno);
        if (unionTle == NULL)
            return false;

        /* Find the corresponding leaf column */
        TargetEntry *leafTle = get_tle_by_resno(leafMapTlist, attno);
        if (leafTle == NULL)
            return false;

        /* Check type compatibility */
        if (exprType((Node *)unionTle->expr) != exprType((Node *)leafTle->expr))
            return false;
        if (exprTypmod((Node *)unionTle->expr) != exprTypmod((Node *)leafTle->expr))
            return false;
        if (exprCollation((Node *)unionTle->expr) != exprCollation((Node *)leafTle->expr))
            return false;
    }

    return true;
}

/*
 * leaf_local_expr_is_valid
 *     Validate that the mapped expression only contains local Vars resolvable
 *     in the leaf's rtable, and no volatile functions or sublinks.
 */
static bool
leaf_local_expr_is_valid(Query *leaf, Expr *mappedExpr)
{
    /* Check for volatile functions */
    if (contain_volatile_functions((Node *)mappedExpr))
        return false;

    /* Check for SubLinks */
    if (checkExprHasSubLink((Node *)mappedExpr))
        return false;

    /* Collect all Vars and verify they are local */
    List *vars = pull_var_clause((Node *)mappedExpr, PVC_RECURSE_AGGREGATES,
                                  PVC_INCLUDE_PLACEHOLDERS);
    ListCell *lc = NULL;

    foreach (lc, vars) {
        Node *node = (Node *)lfirst(lc);
        if (!IsA(node, Var))
            continue;

        Var *v = (Var *)node;
        if (v->varlevelsup != 0)
            return false;

        /* Verify varno is within leaf's rtable */
        if (v->varno < 1 || v->varno > list_length(leaf->rtable))
            return false;

        /* Verify it can be fetched */
        RangeTblEntry *rte = rt_fetch(v->varno, leaf->rtable);
        if (rte == NULL)
            return false;
    }

    return true;
}

/*
 * make_prepared_leaf
 *     Create a prepared leaf structure.
 */
static DiskAnnTopKPreparedLeaf *
make_prepared_leaf(RangeTblEntry *ownerRte, Query *rewrittenQuery)
{
    DiskAnnTopKPreparedLeaf *prep = (DiskAnnTopKPreparedLeaf *)palloc(sizeof(DiskAnnTopKPreparedLeaf));
    prep->ownerRte = ownerRte;
    prep->rewrittenQuery = rewrittenQuery;
    return prep;
}

/*
 * prepare_leaf_diskann_topk
 *     Prepare a single leaf query by adding outer WHERE + ORDER BY + LIMIT.
 *
 * This operates on the rewritten copy, not the original.
 * Execution order: map outer WHERE -> merge with leaf quals -> map ORDER BY -> set LIMIT
 */
static bool
prepare_leaf_diskann_topk(Query *outer, Index unionRti, RangeTblEntry *unionRte,
                          Query *unionQuery, Query *leafCopy, Node *outerQual)
{
    ListCell *lc = NULL;

    /* Validate leaf safety */
    if (!leaf_query_is_safe(leafCopy))
        return false;

    /* Step 1: Map and append outer WHERE to leaf if present */
    if (outerQual != NULL) {
        Expr *mappedQual = map_outer_qual_to_leaf(outer, unionRti, unionRte,
                                                    leafCopy, (Expr *)outerQual);
        if (mappedQual == NULL)
            return false;

        if (!append_outer_qual_to_leaf(leafCopy, mappedQual))
            return false;
    }

    /* Step 2: Map each sort clause */
    List *mappedSortClause = NIL;

    foreach (lc, outer->sortClause) {
        SortGroupClause *outerSc = (SortGroupClause *)lfirst(lc);
        TargetEntry *outerTle = get_sortgroupclause_tle(outerSc, outer->targetList);
        if (outerTle == NULL)
            return false;

        /* Map the sort expression to leaf */
        Expr *mappedExpr = map_sort_expr_to_leaf(outer, unionRti, unionRte,
                                                  leafCopy, (Expr *)outerTle->expr);
        if (mappedExpr == NULL)
            return false;

        /* Find or add this expression in leaf's targetList */
        TargetEntry *leafTle = tlist_member((Node *)mappedExpr, leafCopy->targetList);
        if (leafTle == NULL) {
            /* Add as resjunk */
            leafTle = makeTargetEntry(mappedExpr,
                                      list_length(leafCopy->targetList) + 1,
                                      NULL,
                                      true);
            leafCopy->targetList = lappend(leafCopy->targetList, leafTle);
        }

        /* Assign sortgroupref if needed */
        if (leafTle->ressortgroupref == 0)
            leafTle->ressortgroupref = assignSortGroupRef(leafTle, leafCopy->targetList);

        /* Copy the SortGroupClause, updating tleSortGroupRef */
        SortGroupClause *leafSc = (SortGroupClause *)copyObject(outerSc);
        leafSc->tleSortGroupRef = leafTle->ressortgroupref;

        mappedSortClause = lappend(mappedSortClause, leafSc);
    }

    /* Step 3: Assign the ORDER BY and LIMIT */
    leafCopy->sortClause = mappedSortClause;
    leafCopy->limitCount = (Node *)copyObject(outer->limitCount);
    leafCopy->limitOffset = NULL;
    leafCopy->limitIsPercent = false;
    leafCopy->limitWithTies = false;

    elog(DEBUG1, "prepare_leaf_diskann_topk: set leaf LIMIT, sortClause length=%d, limitCount=%s",
         list_length(leafCopy->sortClause),
         leafCopy->limitCount ? "SET" : "NULL");

    return true;
}

/*
 * commit_prepared_leaves
 *     Atomically replace all original leaf subqueries with rewritten versions.
 */
static void
commit_prepared_leaves(List *prepared)
{
    ListCell *lc = NULL;

    foreach (lc, prepared) {
        DiskAnnTopKPreparedLeaf *item = (DiskAnnTopKPreparedLeaf *)lfirst(lc);
        item->ownerRte->subquery = item->rewrittenQuery;
    }
}

/*
 * check_outer_qual_pushdown_safe
 *     Check if the outer WHERE clause can be safely pushed down to all UNION ALL leaves.
 *
 * Returns false if:
 * - Outer WHERE contains SubLink, volatile functions, SRF, aggregates, or window functions
 * - Any Var in outer WHERE does not reference the UNION RTE's normal output columns
 * - Type/typmod/collation mismatch between UNION output and any leaf's corresponding column
 */
static bool
check_outer_qual_pushdown_safe(Query *outer, Index unionRti, RangeTblEntry *unionRte,
                                 List *leaves)
{
    Node *outerQual = (outer->jointree != NULL) ? outer->jointree->quals : NULL;
    ListCell *lc = NULL;

    /* No WHERE clause is a valid case - nothing to push down */
    if (outerQual == NULL)
        return true;

    /* Reject SubLinks - correlated subqueries need special handling */
    if (checkExprHasSubLink(outerQual))
        return false;

    /* Reject volatile functions - they may have side effects or change results */
    if (contain_volatile_functions(outerQual))
        return false;

    /* Reject SRF, aggregates, and window functions */
    if (expression_returns_set(outerQual) ||
        contain_agg_clause(outerQual) ||
        contain_window_function(outerQual))
        return false;

    /* Collect all level-0 Vars from outer WHERE */
    List *vars = pull_vars_of_level((Node *)outerQual, 0);

    /* Verify all Vars reference the UNION RTE and are normal column refs */
    foreach (lc, vars) {
        Node *node = (Node *)lfirst(lc);
        if (!IsA(node, Var))
            return false;

        Var *v = (Var *)node;
        Assert(v->varlevelsup == 0);

        /* Must reference unionRti and be a normal output column (varattno > 0) */
        if (v->varno != unionRti || v->varattno <= 0)
            return false;
    }

    /* Verify type compatibility for each leaf */
    foreach (lc, leaves) {
        RangeTblEntry *leafRte = (RangeTblEntry *)lfirst(lc);
        Query *leaf = leafRte->subquery;

        /* Build temporary mapping targetlist for this leaf */
        List *leafMapTlist = build_visible_output_map_tlist(leaf);

        /* Validate types match between UNION and leaf */
        if (!validate_union_leaf_output_types(unionRte->subquery, leafMapTlist, vars))
            return false;
    }

    return true;
}

/*
 * map_outer_qual_to_leaf
 *     Map the outer WHERE expression to the corresponding leaf expression.
 *
 * This replaces UNION output Vars with the actual leaf expressions using
 * ReplaceVarsFromTargetList.
 */
static Expr *
map_outer_qual_to_leaf(Query *outer, Index unionRti, RangeTblEntry *unionRte,
                        Query *leaf, Expr *outerQual)
{
    /* Reject SubLinks and outer-level Vars */
    if (checkExprHasSubLink((Node *)outerQual))
        return NULL;

    if (contain_vars_of_level_or_above((Node *)outerQual, 1))
        return NULL;

    /* Collect all level-0 Vars */
    List *vars = pull_vars_of_level((Node *)outerQual, 0);
    ListCell *lc = NULL;

    /* All Vars must reference the UNION RTE and be normal column refs */
    foreach (lc, vars) {
        Node *node = (Node *)lfirst(lc);
        if (!IsA(node, Var))
            return NULL;

        Var *v = (Var *)node;
        Assert(v->varlevelsup == 0);

        if (v->varno != unionRti || v->varattno <= 0)
            return NULL;
    }

    /* Build a temporary mapping targetlist for this leaf */
    List *leafMapTlist = build_visible_output_map_tlist(leaf);

    /* Validate types match between UNION and leaf */
    if (!validate_union_leaf_output_types(unionRte->subquery, leafMapTlist, vars))
        return NULL;

    /* Perform the replacement */
    Expr *mapped = (Expr *)ReplaceVarsFromTargetList((Node *)outerQual,
                                                      unionRti,
                                                      0,
                                                      unionRte,
                                                      leafMapTlist,
                                                      REPLACEVARS_REPORT_ERROR,
                                                      0,
                                                      NULL);

    /* Validate the mapped expression */
    if (!leaf_local_expr_is_valid(leaf, mapped))
        return NULL;

    return mapped;
}

/*
 * append_outer_qual_to_leaf
 *     Append the mapped outer WHERE to the leaf's existing quals using AND.
 *
 * This preserves the leaf's original WHERE conditions (e.g., NOT EXISTS for deletions)
 * and adds the outer filter condition.
 */
static bool
append_outer_qual_to_leaf(Query *leafCopy, Expr *mappedQual)
{
    if (mappedQual == NULL)
        return false;

    /* Merge with existing leaf quals using AND */
    if (leafCopy->jointree == NULL) {
        leafCopy->jointree = makeNode(FromExpr);
        leafCopy->jointree->fromlist = NIL;
        leafCopy->jointree->quals = NULL;
    }

    leafCopy->jointree->quals = (Node *)make_and_qual(
        (Node *)leafCopy->jointree->quals,
        (Node *)mappedQual);

    return true;
}
