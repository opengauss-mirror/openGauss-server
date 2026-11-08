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

typedef struct RequiredColumnVarMapContext {
    Index unionRti;
    int oldNcols;
    AttrNumber *oldToNew;
    bool failed;
} RequiredColumnVarMapContext;

typedef struct UnionAllOrderKey {
    Index sortRef;
    List *leafExprs;
    Oid type;
    int32 typmod;
    Oid collation;
    AttrNumber newAttno;
} UnionAllOrderKey;

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
static bool required_columns_outer_query_is_safe(Query *query);
static int count_visible_tlist_entries(List *targetList);
static bool add_required_columns_from_expr(Node *expr, Index unionRti, int oldNcols,
                                           Bitmapset **required);
static bool collect_required_output_columns(Query *outer, Index unionRti, int oldNcols,
                                             Bitmapset **required);
static void preserve_semantically_required_outputs(List *leaves, int oldNcols, Bitmapset **required);
static bool validate_required_column_shape(Query *unionQuery, List *leaves, int oldNcols);
static bool prepare_outer_where_pushdown(Query *outer, Index unionRti, RangeTblEntry *unionRte,
                                         List *leaves);
static List *compact_name_list(List *names, AttrNumber *oldToNew, int oldNcols, bool *ok);
static List *compact_leaf_targetlist(List *targetList, AttrNumber *oldToNew, int oldNcols, bool *ok);
static List *compact_union_targetlist(List *targetList, AttrNumber *oldToNew, int oldNcols, bool *ok);
static bool compact_setop_metadata(Node *node, AttrNumber *oldToNew, int oldNcols);
static Node *required_column_var_mutator(Node *node, void *context);
static bool remap_outer_query_vars(Query *outer, Index unionRti, AttrNumber *oldToNew, int oldNcols);
static bool compact_union_all_query(Query *outer, Index unionRti, RangeTblEntry *unionRte,
                                    List *leaves, Bitmapset *required, int oldNcols);
static bool validate_order_projection_shape(Query *unionQuery, List *leaves, int oldNcols);
static UnionAllOrderKey *find_union_all_order_key(List *keys, Index sortRef);
static bool prepare_union_all_order_keys(Query *outer, Index unionRti, RangeTblEntry *unionRte,
                                         List *leaves, List **keys);
static bool collect_order_projection_raw_columns(Query *outer, Index unionRti, int oldNcols,
                                                 List *keys, Bitmapset **required);
static bool leaf_expr_is_order_key(Expr *expr, List *keys, int leafIndex);
static bool rewrite_leaf_order_projection(RangeTblEntry *leafRte, AttrNumber *oldToNew,
                                          int oldNcols, List *keys, int leafIndex);
static void append_setop_order_key_metadata(Node *node, List *keys);
static bool rewrite_outer_order_projection(Query *outer, Index unionRti,
                                           AttrNumber *oldToNew, int oldNcols, List *keys);
static bool rewrite_union_order_projection(Query *outer, Index unionRti,
                                           RangeTblEntry *unionRte, List *leaves,
                                           Bitmapset *rawRequired, List *keys, int oldNcols);

/*
 * preprocess_union_all_required_columns
 *     Push a safe mandatory outer WHERE into every pure UNION ALL leaf and
 *     physically compact the set-operation output to columns needed above it.
 *
 * The rewrite is performed on a private copy of the outer Query.  The copied
 * rtable/targetlist/jointree pointers are installed only after all positional
 * targetlist and type metadata checks succeed.
 * Return true only when this Query was rewritten; the inherited planner flag
 * must not authorize expression projection in an unchecked descendant Query.
 */
bool
preprocess_union_all_required_columns(PlannerInfo *root)
{
    Query *outer = root->parse;
    Query *work = NULL;
    RangeTblEntry *unionRte = NULL;
    Index unionRti = 0;
    List *leaves = NIL;
    Bitmapset *required = NULL;
    int oldNcols = 0;
    int newNcols = 0;
    bool wherePushed = false;

    if (!u_sess->attr.attr_sql.enable_union_all_required_columns_pushdown)
        return false;

    if (!required_columns_outer_query_is_safe(outer)) {
        elog(DEBUG1, "union_all_required_columns: skipped reason=outer_query_shape");
        return false;
    }

    /*
     * Work on a complete copy so that a late validation failure cannot leave
     * only some UNION ALL leaves rewritten.
     */
    work = (Query *)copyObject(outer);
    unionRte = get_single_union_subquery_rte(work, &unionRti);
    if (unionRte == NULL || unionRte->security_barrier || unionRte->lateral) {
        elog(DEBUG1, "union_all_required_columns: skipped reason=unsafe_union_rte");
        return false;
    }

    if (!collect_pure_union_all_leaves(unionRte->subquery, &leaves)) {
        elog(DEBUG1, "union_all_required_columns: skipped reason=non_union_all");
        return false;
    }

    oldNcols = count_visible_tlist_entries(unionRte->subquery->targetList);
    if (oldNcols <= 0 || !validate_required_column_shape(unionRte->subquery, leaves, oldNcols)) {
        elog(DEBUG1, "union_all_required_columns: skipped reason=inconsistent_output_shape");
        return false;
    }

    /*
     * Mapping must happen before column numbers are compacted.  If every leaf
     * accepts the exact predicate, remove the redundant outer recheck so that
     * columns used only by WHERE stop at the leaf.
     */
    wherePushed = prepare_outer_where_pushdown(work, unionRti, unionRte, leaves);

    if (!collect_required_output_columns(work, unionRti, oldNcols, &required)) {
        elog(DEBUG1, "union_all_required_columns: skipped reason=unsupported_outer_reference");
        return false;
    }

    preserve_semantically_required_outputs(leaves, oldNcols, &required);

    /*
     * An empty set-operation tuple is poorly supported by several legacy plan
     * paths.  Keep the first column as a harmless carrier for SELECT constants.
     */
    if (bms_is_empty(required))
        required = bms_add_member(required, 1);

    newNcols = bms_num_members(required);
    if (!compact_union_all_query(work, unionRti, unionRte, leaves, required, oldNcols)) {
        elog(DEBUG1, "union_all_required_columns: skipped reason=compact_validation_failed");
        return false;
    }

    /*
     * No planner structures referring to these fields have been built yet.
     * Installing the copied fields keeps root->parse and the caller's Query
     * object identity stable.
     */
    outer->targetList = work->targetList;
    outer->jointree = work->jointree;
    outer->rtable = work->rtable;
    root->union_all_required_columns_pruned = true;

    elog(DEBUG1,
         "union_all_required_columns: old=%d new=%d where_pushed=%s outer_recheck=%s",
         oldNcols,
         newNcols,
         wherePushed ? "true" : "false",
         outer->jointree != NULL && outer->jointree->quals != NULL ? "true" : "false");

    return true;
}

/*
 * Keep the first implementation deliberately narrow: it covers the target
 * SELECT-over-one-UNION-view shape, while declining constructs whose row or
 * evaluation semantics need more than positional output pruning.
 */
static bool
required_columns_outer_query_is_safe(Query *query)
{
    FromExpr *fromExpr = NULL;

    if (query == NULL || query->commandType != CMD_SELECT)
        return false;

    /* An absent predicate needs no pushdown; keep the same projection checks. */
    if (query->jointree == NULL)
        return false;

    if (query->setOperations != NULL || query->hasAggs || query->groupClause != NIL ||
        query->havingQual != NULL || query->distinctClause != NIL ||
        query->hasWindowFuncs || query->windowClause != NIL || query->cteList != NIL ||
        query->hasForUpdate || query->hasTargetSRFs || query->hasSubLinks ||
        query->hasRecursive || query->hasModifyingCTE || query->hasRowSecurity ||
        query->limitOffset != NULL || query->limitIsPercent || query->limitWithTies)
        return false;

    /* The early rewrite cannot rely on hasTargetSRFs being populated yet. */
    if (expression_returns_set((Node *)query->targetList) ||
        expression_contains_rownum((Node *)query->targetList) || ContainRownumQual(query))
        return false;

    if (list_length(query->rtable) != 1 || !IsA(query->jointree, FromExpr))
        return false;

    fromExpr = (FromExpr *)query->jointree;
    if (list_length(fromExpr->fromlist) != 1 ||
        !IsA(linitial(fromExpr->fromlist), RangeTblRef))
        return false;

    return ((RangeTblRef *)linitial(fromExpr->fromlist))->rtindex == 1;
}

static int
count_visible_tlist_entries(List *targetList)
{
    ListCell *lc = NULL;
    int count = 0;

    foreach (lc, targetList) {
        TargetEntry *tle = (TargetEntry *)lfirst(lc);
        if (!tle->resjunk)
            count++;
    }
    return count;
}

static bool
add_required_columns_from_expr(Node *expr, Index unionRti, int oldNcols,
                               Bitmapset **required)
{
    List *vars = NIL;
    ListCell *lc = NULL;

    if (expr == NULL)
        return true;

    vars = pull_vars_of_level(expr, 0);
    foreach (lc, vars) {
        Node *node = (Node *)lfirst(lc);
        Var *var = NULL;

        if (!IsA(node, Var)) {
            list_free_ext(vars);
            return false;
        }

        var = (Var *)node;
        if (var->varlevelsup != 0 || var->varno != unionRti ||
            var->varattno <= 0 || var->varattno > oldNcols) {
            list_free_ext(vars);
            return false;
        }
        *required = bms_add_member(*required, var->varattno);
    }

    list_free_ext(vars);
    return true;
}

static bool
collect_required_output_columns(Query *outer, Index unionRti, int oldNcols,
                                Bitmapset **required)
{
    ListCell *lc = NULL;

    foreach (lc, outer->targetList) {
        TargetEntry *tle = (TargetEntry *)lfirst(lc);
        if (!add_required_columns_from_expr((Node *)tle->expr, unionRti, oldNcols, required))
            return false;
    }

    if (outer->jointree != NULL &&
        !add_required_columns_from_expr((Node *)outer->jointree->quals,
                                        unionRti, oldNcols, required))
        return false;

    if (!add_required_columns_from_expr(outer->limitCount, unionRti, oldNcols, required) ||
        !add_required_columns_from_expr(outer->limitOffset, unionRti, oldNcols, required))
        return false;

    return true;
}

/*
 * Dropping an unused volatile/SRF output can alter observable evaluation or
 * row multiplicity.  Since UNION ALL columns are positional, preserve that
 * output position in every leaf when any leaf requires it.
 */
static void
preserve_semantically_required_outputs(List *leaves, int oldNcols, Bitmapset **required)
{
    ListCell *leafCell = NULL;

    foreach (leafCell, leaves) {
        RangeTblEntry *leafRte = (RangeTblEntry *)lfirst(leafCell);
        Query *leaf = leafRte->subquery;
        ListCell *lc = NULL;
        int pos = 0;

        foreach (lc, leaf->targetList) {
            TargetEntry *tle = (TargetEntry *)lfirst(lc);
            if (tle->resjunk)
                continue;

            pos++;
            if (pos > oldNcols)
                break;

            if (tle->ressortgroupref != 0 ||
                expression_returns_set((Node *)tle->expr) ||
                contain_volatile_functions((Node *)tle->expr) ||
                checkExprHasSubLink((Node *)tle->expr))
                *required = bms_add_member(*required, pos);
        }
    }
}

static bool
validate_required_column_shape(Query *unionQuery, List *leaves, int oldNcols)
{
    SetOperationStmt *topop = NULL;
    ListCell *lc = NULL;

    if (unionQuery == NULL || unionQuery->setOperations == NULL ||
        unionQuery->commandType != CMD_SELECT || unionQuery->sortClause != NIL ||
        unionQuery->limitCount != NULL || unionQuery->limitOffset != NULL ||
        unionQuery->hasAggs || unionQuery->groupClause != NIL ||
        unionQuery->havingQual != NULL || unionQuery->distinctClause != NIL ||
        unionQuery->hasWindowFuncs || unionQuery->windowClause != NIL ||
        unionQuery->cteList != NIL || unionQuery->hasForUpdate ||
        unionQuery->hasTargetSRFs || unionQuery->hasRecursive ||
        unionQuery->hasModifyingCTE || unionQuery->hasRowSecurity)
        return false;

    foreach (lc, unionQuery->targetList) {
        TargetEntry *tle = (TargetEntry *)lfirst(lc);
        if (tle->resjunk)
            return false;
    }

    topop = (SetOperationStmt *)unionQuery->setOperations;
    if (!IsA(topop, SetOperationStmt) ||
        list_length(topop->colTypes) != oldNcols ||
        list_length(topop->colTypmods) != oldNcols ||
        list_length(topop->colCollations) != oldNcols)
        return false;

    foreach (lc, leaves) {
        RangeTblEntry *leafRte = (RangeTblEntry *)lfirst(lc);
        Query *leaf = leafRte->subquery;

        if (leafRte->security_barrier || leafRte->lateral ||
            !leaf_query_is_safe(leaf) || leaf->hasTargetSRFs ||
            count_visible_tlist_entries(leaf->targetList) != oldNcols)
            return false;

        if (leafRte->eref == NULL ||
            list_length(leafRte->eref->colnames) != oldNcols)
            return false;
    }

    return true;
}

static bool
prepare_outer_where_pushdown(Query *outer, Index unionRti, RangeTblEntry *unionRte,
                             List *leaves)
{
    Node *outerQual = outer->jointree != NULL ? outer->jointree->quals : NULL;
    List *mappedQuals = NIL;
    ListCell *lc = NULL;
    ListCell *mq = NULL;

    if (outerQual == NULL ||
        !check_outer_qual_pushdown_safe(outer, unionRti, unionRte, leaves))
        return false;

    foreach (lc, leaves) {
        RangeTblEntry *leafRte = (RangeTblEntry *)lfirst(lc);
        Expr *mapped = map_outer_qual_to_leaf(outer, unionRti, unionRte,
                                             leafRte->subquery, (Expr *)outerQual);
        if (mapped == NULL)
            return false;
        mappedQuals = lappend(mappedQuals, mapped);
    }

    forboth(lc, leaves, mq, mappedQuals) {
        RangeTblEntry *leafRte = (RangeTblEntry *)lfirst(lc);
        if (!append_outer_qual_to_leaf(leafRte->subquery, (Expr *)lfirst(mq)))
            return false;
    }

    outer->jointree->quals = NULL;
    return true;
}

static List *
compact_name_list(List *names, AttrNumber *oldToNew, int oldNcols, bool *ok)
{
    List *result = NIL;
    ListCell *lc = NULL;
    int pos = 0;

    if (names == NIL || list_length(names) != oldNcols) {
        *ok = false;
        return NIL;
    }

    foreach (lc, names) {
        pos++;
        if (oldToNew[pos] > 0)
            result = lappend(result, copyObject(lfirst(lc)));
    }
    return result;
}

static List *
compact_leaf_targetlist(List *targetList, AttrNumber *oldToNew, int oldNcols, bool *ok)
{
    List *result = NIL;
    ListCell *lc = NULL;
    int oldPos = 0;
    int nextResno = 1;

    foreach (lc, targetList) {
        TargetEntry *tle = (TargetEntry *)lfirst(lc);
        TargetEntry *copy = NULL;

        if (tle->resjunk)
            continue;

        oldPos++;
        if (oldPos > oldNcols) {
            *ok = false;
            return NIL;
        }

        if (oldToNew[oldPos] == 0)
            continue;

        copy = (TargetEntry *)copyObject(tle);
        copy->resno = (AttrNumber)nextResno++;
        result = lappend(result, copy);
    }

    if (oldPos != oldNcols) {
        *ok = false;
        return NIL;
    }

    /* Resjunk items are internal to the leaf and remain after visible output. */
    foreach (lc, targetList) {
        TargetEntry *tle = (TargetEntry *)lfirst(lc);
        TargetEntry *copy = NULL;

        if (!tle->resjunk)
            continue;
        copy = (TargetEntry *)copyObject(tle);
        copy->resno = (AttrNumber)nextResno++;
        result = lappend(result, copy);
    }

    return result;
}

static List *
compact_union_targetlist(List *targetList, AttrNumber *oldToNew, int oldNcols, bool *ok)
{
    List *result = NIL;
    ListCell *lc = NULL;
    int oldPos = 0;

    foreach (lc, targetList) {
        TargetEntry *tle = (TargetEntry *)lfirst(lc);
        TargetEntry *copy = NULL;
        Var *var = NULL;

        if (tle->resjunk) {
            *ok = false;
            return NIL;
        }

        oldPos++;
        if (oldPos > oldNcols) {
            *ok = false;
            return NIL;
        }
        if (oldToNew[oldPos] == 0)
            continue;

        /*
         * A set-operation Query targetlist references the leftmost leaf
         * output.  Keep this restriction explicit so unexpected parse shapes
         * safely fall back instead of silently remapping the wrong Var.
         */
        if (!IsA(tle->expr, Var)) {
            *ok = false;
            return NIL;
        }
        var = (Var *)tle->expr;
        if (var->varlevelsup != 0 || var->varattno != oldPos) {
            *ok = false;
            return NIL;
        }

        copy = (TargetEntry *)copyObject(tle);
        copy->resno = oldToNew[oldPos];
        var = (Var *)copy->expr;
        var->varattno = oldToNew[oldPos];
        if (var->varoattno == oldPos)
            var->varoattno = oldToNew[oldPos];
        result = lappend(result, copy);
    }

    if (oldPos != oldNcols)
        *ok = false;
    return result;
}

static bool
compact_setop_metadata(Node *node, AttrNumber *oldToNew, int oldNcols)
{
    SetOperationStmt *op = NULL;
    List *types = NIL;
    List *typmods = NIL;
    List *collations = NIL;
    int pos = 0;

    if (node == NULL)
        return false;
    if (IsA(node, RangeTblRef))
        return true;
    if (!IsA(node, SetOperationStmt))
        return false;

    op = (SetOperationStmt *)node;
    if (op->op != SETOP_UNION || !op->all || op->groupClauses != NIL ||
        list_length(op->colTypes) != oldNcols ||
        list_length(op->colTypmods) != oldNcols ||
        list_length(op->colCollations) != oldNcols)
        return false;

    for (pos = 1; pos <= oldNcols; pos++) {
        if (oldToNew[pos] == 0)
            continue;
        types = lappend_oid(types, list_nth_oid(op->colTypes, pos - 1));
        typmods = lappend_int(typmods, list_nth_int(op->colTypmods, pos - 1));
        collations = lappend_oid(collations, list_nth_oid(op->colCollations, pos - 1));
    }

    if (!compact_setop_metadata(op->larg, oldToNew, oldNcols) ||
        !compact_setop_metadata(op->rarg, oldToNew, oldNcols))
        return false;

    op->colTypes = types;
    op->colTypmods = typmods;
    op->colCollations = collations;
    return true;
}

static Node *
required_column_var_mutator(Node *node, void *context)
{
    RequiredColumnVarMapContext *ctx = (RequiredColumnVarMapContext *)context;

    if (node == NULL)
        return NULL;

    if (IsA(node, Var)) {
        Var *oldVar = (Var *)node;
        Var *newVar = NULL;
        AttrNumber newAttno = 0;

        if (oldVar->varlevelsup != 0 || oldVar->varno != ctx->unionRti)
            return (Node *)copyObject(oldVar);

        if (oldVar->varattno <= 0 || oldVar->varattno > ctx->oldNcols) {
            ctx->failed = true;
            return (Node *)copyObject(oldVar);
        }

        newAttno = ctx->oldToNew[oldVar->varattno];
        if (newAttno <= 0) {
            ctx->failed = true;
            return (Node *)copyObject(oldVar);
        }

        newVar = (Var *)copyObject(oldVar);
        newVar->varattno = newAttno;
        if (newVar->varnoold == ctx->unionRti &&
            newVar->varoattno == oldVar->varattno)
            newVar->varoattno = newAttno;
        return (Node *)newVar;
    }

    return expression_tree_mutator(node, required_column_var_mutator, context);
}

static bool
remap_outer_query_vars(Query *outer, Index unionRti, AttrNumber *oldToNew, int oldNcols)
{
    RequiredColumnVarMapContext ctx;
    ListCell *lc = NULL;

    ctx.unionRti = unionRti;
    ctx.oldNcols = oldNcols;
    ctx.oldToNew = oldToNew;
    ctx.failed = false;

    foreach (lc, outer->targetList) {
        TargetEntry *tle = (TargetEntry *)lfirst(lc);
        tle->expr = (Expr *)required_column_var_mutator((Node *)tle->expr, &ctx);
    }

    if (outer->jointree != NULL)
        outer->jointree->quals = required_column_var_mutator(
            (Node *)outer->jointree->quals, &ctx);
    outer->limitCount = required_column_var_mutator(outer->limitCount, &ctx);
    outer->limitOffset = required_column_var_mutator(outer->limitOffset, &ctx);

    return !ctx.failed;
}

static bool
compact_union_all_query(Query *outer, Index unionRti, RangeTblEntry *unionRte,
                        List *leaves, Bitmapset *required, int oldNcols)
{
    Query *unionQuery = unionRte->subquery;
    AttrNumber *oldToNew = NULL;
    ListCell *lc = NULL;
    int pos = 0;
    int nextAttno = 1;
    bool ok = true;

    oldToNew = (AttrNumber *)palloc0((oldNcols + 1) * sizeof(AttrNumber));
    for (pos = 1; pos <= oldNcols; pos++) {
        if (bms_is_member(pos, required))
            oldToNew[pos] = (AttrNumber)nextAttno++;
    }

    if (!remap_outer_query_vars(outer, unionRti, oldToNew, oldNcols))
        return false;

    foreach (lc, leaves) {
        RangeTblEntry *leafRte = (RangeTblEntry *)lfirst(lc);
        leafRte->subquery->targetList = compact_leaf_targetlist(
            leafRte->subquery->targetList, oldToNew, oldNcols, &ok);
        if (!ok)
            return false;

        leafRte->eref->colnames = compact_name_list(
            leafRte->eref->colnames, oldToNew, oldNcols, &ok);
        if (!ok)
            return false;
    }

    unionQuery->targetList = compact_union_targetlist(
        unionQuery->targetList, oldToNew, oldNcols, &ok);
    if (!ok || !compact_setop_metadata(unionQuery->setOperations, oldToNew, oldNcols))
        return false;

    if (unionRte->eref == NULL)
        return false;
    unionRte->eref->colnames = compact_name_list(
        unionRte->eref->colnames, oldToNew, oldNcols, &ok);

    return ok;
}

/*
 * preprocess_union_all_order_key_projection
 *     Materialize non-Var ORDER BY expressions in each UNION ALL leaf.
 *
 * This runs after optional Top-K rewriting, because Top-K eligibility must be
 * checked while the outer expression is still the original DiskANN operator.
 * The outer query subsequently sorts a compact scalar key rather than carrying
 * the key's wide inputs (notably vector) through Append.
 */
void
preprocess_union_all_order_key_projection(PlannerInfo *root)
{
    Query *outer = root->parse;
    Query *work = NULL;
    RangeTblEntry *unionRte = NULL;
    Index unionRti = 0;
    List *leaves = NIL;
    List *keys = NIL;
    Bitmapset *rawRequired = NULL;
    int oldNcols = 0;

    if (!u_sess->attr.attr_sql.enable_union_all_required_columns_pushdown ||
        !root->union_all_required_columns_pruned || outer->sortClause == NIL)
        return;

    /*
     * A residual filter must run before evaluation of the outer sort key.
     * Even an immutable expression can throw (e.g. 1 / x for a filtered-out
     * zero).  Keep its original evaluation boundary unless WHERE was pushed
     * successfully into every leaf.
     */
    if (outer->jointree != NULL && outer->jointree->quals != NULL)
        return;

    work = (Query *)copyObject(outer);
    unionRte = get_single_union_subquery_rte(work, &unionRti);
    if (unionRte == NULL || unionRte->security_barrier || unionRte->lateral ||
        !collect_pure_union_all_leaves(unionRte->subquery, &leaves))
        return;

    oldNcols = count_visible_tlist_entries(unionRte->subquery->targetList);
    if (oldNcols <= 0 ||
        !validate_order_projection_shape(unionRte->subquery, leaves, oldNcols))
        return;

    if (!prepare_union_all_order_keys(work, unionRti, unionRte, leaves, &keys) ||
        keys == NIL)
        return;

    if (!collect_order_projection_raw_columns(work, unionRti, oldNcols,
                                              keys, &rawRequired))
        return;
    preserve_semantically_required_outputs(leaves, oldNcols, &rawRequired);

    if (!rewrite_union_order_projection(work, unionRti, unionRte, leaves,
                                        rawRequired, keys, oldNcols)) {
        elog(DEBUG1, "union_all_required_columns: order projection skipped reason=rewrite_failed");
        return;
    }

    outer->targetList = work->targetList;
    outer->jointree = work->jointree;
    outer->rtable = work->rtable;

    elog(DEBUG1,
         "union_all_required_columns: order_keys=%d raw_columns=%d materialized=true",
         list_length(keys),
         bms_num_members(rawRequired));
}

static UnionAllOrderKey *
find_union_all_order_key(List *keys, Index sortRef)
{
    ListCell *lc = NULL;

    if (sortRef == 0)
        return NULL;

    foreach (lc, keys) {
        UnionAllOrderKey *key = (UnionAllOrderKey *)lfirst(lc);
        if (key->sortRef == sortRef)
            return key;
    }
    return NULL;
}

/*
 * Direct Vars are already compact and are useful as tie-break keys, so only
 * derived expressions need a synthetic scalar output.
 */
static bool
prepare_union_all_order_keys(Query *outer, Index unionRti, RangeTblEntry *unionRte,
                             List *leaves, List **keys)
{
    ListCell *scCell = NULL;

    foreach (scCell, outer->sortClause) {
        SortGroupClause *sc = (SortGroupClause *)lfirst(scCell);
        TargetEntry *outerTle = get_sortgroupclause_tle(sc, outer->targetList);
        UnionAllOrderKey *key = NULL;
        List *vars = NIL;
        ListCell *lc = NULL;

        if (outerTle == NULL || outerTle->ressortgroupref == 0)
            return false;

        if (IsA(outerTle->expr, Var))
            continue;

        vars = pull_vars_of_level((Node *)outerTle->expr, 0);
        if (vars == NIL)
            continue; /* A constant ordering key needs no branch output. */

        foreach (lc, vars) {
            Node *node = (Node *)lfirst(lc);
            Var *var = NULL;

            if (!IsA(node, Var)) {
                list_free_ext(vars);
                return false;
            }
            var = (Var *)node;
            if (var->varlevelsup != 0 || var->varno != unionRti ||
                var->varattno <= 0) {
                list_free_ext(vars);
                return false;
            }
        }
        list_free_ext(vars);

        key = (UnionAllOrderKey *)palloc0(sizeof(UnionAllOrderKey));
        key->sortRef = outerTle->ressortgroupref;
        key->type = exprType((Node *)outerTle->expr);
        key->typmod = exprTypmod((Node *)outerTle->expr);
        key->collation = exprCollation((Node *)outerTle->expr);

        foreach (lc, leaves) {
            RangeTblEntry *leafRte = (RangeTblEntry *)lfirst(lc);
            Expr *mapped = map_sort_expr_to_leaf(outer, unionRti, unionRte,
                                                 leafRte->subquery,
                                                 (Expr *)outerTle->expr);
            if (mapped == NULL ||
                exprType((Node *)mapped) != key->type ||
                exprTypmod((Node *)mapped) != key->typmod ||
                exprCollation((Node *)mapped) != key->collation)
                return false;
            key->leafExprs = lappend(key->leafExprs, mapped);
        }

        *keys = lappend(*keys, key);
    }

    return true;
}

static bool
validate_order_projection_shape(Query *unionQuery, List *leaves, int oldNcols)
{
    SetOperationStmt *topop = NULL;
    ListCell *lc = NULL;

    if (unionQuery == NULL || unionQuery->setOperations == NULL ||
        count_visible_tlist_entries(unionQuery->targetList) != oldNcols)
        return false;

    foreach (lc, unionQuery->targetList) {
        TargetEntry *tle = (TargetEntry *)lfirst(lc);
        if (tle->resjunk)
            return false;
    }

    topop = (SetOperationStmt *)unionQuery->setOperations;
    if (!IsA(topop, SetOperationStmt) ||
        list_length(topop->colTypes) != oldNcols ||
        list_length(topop->colTypmods) != oldNcols ||
        list_length(topop->colCollations) != oldNcols)
        return false;

    foreach (lc, leaves) {
        RangeTblEntry *leafRte = (RangeTblEntry *)lfirst(lc);
        if (leafRte->security_barrier || leafRte->lateral ||
            leafRte->eref == NULL ||
            list_length(leafRte->eref->colnames) != oldNcols ||
            count_visible_tlist_entries(leafRte->subquery->targetList) != oldNcols)
            return false;
    }

    return true;
}

static bool
collect_order_projection_raw_columns(Query *outer, Index unionRti, int oldNcols,
                                     List *keys, Bitmapset **required)
{
    ListCell *lc = NULL;

    foreach (lc, outer->targetList) {
        TargetEntry *tle = (TargetEntry *)lfirst(lc);

        /*
         * The expression is replaced by its synthetic leaf output even when
         * it is a user-visible SELECT item, so its raw Vars are not needed.
         */
        if (find_union_all_order_key(keys, tle->ressortgroupref) != NULL)
            continue;

        if (!add_required_columns_from_expr((Node *)tle->expr,
                                            unionRti, oldNcols, required))
            return false;
    }

    if (outer->jointree != NULL &&
        !add_required_columns_from_expr((Node *)outer->jointree->quals,
                                        unionRti, oldNcols, required))
        return false;

    if (!add_required_columns_from_expr(outer->limitCount, unionRti, oldNcols, required) ||
        !add_required_columns_from_expr(outer->limitOffset, unionRti, oldNcols, required))
        return false;

    return true;
}

static bool
leaf_expr_is_order_key(Expr *expr, List *keys, int leafIndex)
{
    ListCell *lc = NULL;

    foreach (lc, keys) {
        UnionAllOrderKey *key = (UnionAllOrderKey *)lfirst(lc);
        Expr *keyExpr = (Expr *)list_nth(key->leafExprs, leafIndex);
        if (equal(expr, keyExpr))
            return true;
    }
    return false;
}

static bool
rewrite_leaf_order_projection(RangeTblEntry *leafRte, AttrNumber *oldToNew,
                              int oldNcols, List *keys, int leafIndex)
{
    Query *leaf = leafRte->subquery;
    List *oldTarget = leaf->targetList;
    List *newTarget = NIL;
    List *newNames = NIL;
    ListCell *lc = NULL;
    int oldPos = 0;
    int nextResno = 1;
    int keyNumber = 0;
    bool ok = true;

    newNames = compact_name_list(leafRte->eref->colnames,
                                 oldToNew, oldNcols, &ok);
    if (!ok)
        return false;

    foreach (lc, oldTarget) {
        TargetEntry *tle = (TargetEntry *)lfirst(lc);
        TargetEntry *copy = NULL;

        if (tle->resjunk)
            continue;
        oldPos++;
        if (oldPos > oldNcols)
            return false;
        if (oldToNew[oldPos] == 0)
            continue;

        copy = (TargetEntry *)copyObject(tle);
        copy->resno = (AttrNumber)nextResno++;
        newTarget = lappend(newTarget, copy);
    }
    if (oldPos != oldNcols)
        return false;

    foreach (lc, keys) {
        UnionAllOrderKey *key = (UnionAllOrderKey *)lfirst(lc);
        Expr *mapped = (Expr *)list_nth(key->leafExprs, leafIndex);
        TargetEntry *oldTle = tlist_member((Node *)mapped, oldTarget);
        TargetEntry *newTle = NULL;
        char name[NAMEDATALEN];
        errno_t rc;

        keyNumber++;
        rc = snprintf_s(name, NAMEDATALEN, NAMEDATALEN - 1,
                        "__ua_order_%d", keyNumber);
        securec_check_ss(rc, "\0", "\0");

        newTle = makeTargetEntry((Expr *)copyObject(mapped),
                                 (AttrNumber)nextResno++,
                                 pstrdup(name),
                                 false);
        if (oldTle != NULL)
            newTle->ressortgroupref = oldTle->ressortgroupref;
        newTarget = lappend(newTarget, newTle);
        newNames = lappend(newNames, makeString(pstrdup(name)));
    }

    /*
     * Keep unrelated leaf-internal junk entries, but remove the old resjunk
     * copy of a key that has just become a visible UNION output.
     */
    foreach (lc, oldTarget) {
        TargetEntry *tle = (TargetEntry *)lfirst(lc);
        TargetEntry *copy = NULL;

        if (!tle->resjunk || leaf_expr_is_order_key(tle->expr, keys, leafIndex))
            continue;
        copy = (TargetEntry *)copyObject(tle);
        copy->resno = (AttrNumber)nextResno++;
        newTarget = lappend(newTarget, copy);
    }

    leaf->targetList = newTarget;
    leafRte->eref->colnames = newNames;
    return true;
}

static void
append_setop_order_key_metadata(Node *node, List *keys)
{
    SetOperationStmt *op = NULL;
    ListCell *lc = NULL;

    if (node == NULL || IsA(node, RangeTblRef))
        return;

    Assert(IsA(node, SetOperationStmt));
    op = (SetOperationStmt *)node;
    foreach (lc, keys) {
        UnionAllOrderKey *key = (UnionAllOrderKey *)lfirst(lc);
        op->colTypes = lappend_oid(op->colTypes, key->type);
        op->colTypmods = lappend_int(op->colTypmods, key->typmod);
        op->colCollations = lappend_oid(op->colCollations, key->collation);
    }

    append_setop_order_key_metadata(op->larg, keys);
    append_setop_order_key_metadata(op->rarg, keys);
}

static bool
rewrite_outer_order_projection(Query *outer, Index unionRti,
                               AttrNumber *oldToNew, int oldNcols, List *keys)
{
    RequiredColumnVarMapContext ctx;
    ListCell *lc = NULL;

    ctx.unionRti = unionRti;
    ctx.oldNcols = oldNcols;
    ctx.oldToNew = oldToNew;
    ctx.failed = false;

    foreach (lc, outer->targetList) {
        TargetEntry *tle = (TargetEntry *)lfirst(lc);
        UnionAllOrderKey *key =
            find_union_all_order_key(keys, tle->ressortgroupref);

        if (key != NULL) {
            tle->expr = (Expr *)makeVar(unionRti,
                                       key->newAttno,
                                       key->type,
                                       key->typmod,
                                       key->collation,
                                       0);
        } else {
            tle->expr = (Expr *)required_column_var_mutator(
                (Node *)tle->expr, &ctx);
        }
    }

    if (outer->jointree != NULL)
        outer->jointree->quals = required_column_var_mutator(
            (Node *)outer->jointree->quals, &ctx);
    outer->limitCount = required_column_var_mutator(outer->limitCount, &ctx);
    outer->limitOffset = required_column_var_mutator(outer->limitOffset, &ctx);

    return !ctx.failed;
}

static bool
rewrite_union_order_projection(Query *outer, Index unionRti,
                               RangeTblEntry *unionRte, List *leaves,
                               Bitmapset *rawRequired, List *keys, int oldNcols)
{
    Query *unionQuery = unionRte->subquery;
    AttrNumber *oldToNew = NULL;
    List *unionTarget = NIL;
    List *unionNames = NIL;
    ListCell *lc = NULL;
    Var *leftmostVar = NULL;
    Index leftmostRti = 0;
    int pos = 0;
    int nextAttno = 1;
    int leafIndex = 0;
    int keyNumber = 0;
    bool ok = true;

    if (unionQuery->targetList == NIL ||
        !IsA(((TargetEntry *)linitial(unionQuery->targetList))->expr, Var))
        return false;
    leftmostVar = (Var *)((TargetEntry *)linitial(unionQuery->targetList))->expr;
    leftmostRti = leftmostVar->varno;

    oldToNew = (AttrNumber *)palloc0((oldNcols + 1) * sizeof(AttrNumber));
    for (pos = 1; pos <= oldNcols; pos++) {
        if (bms_is_member(pos, rawRequired))
            oldToNew[pos] = (AttrNumber)nextAttno++;
    }

    foreach (lc, keys) {
        UnionAllOrderKey *key = (UnionAllOrderKey *)lfirst(lc);
        key->newAttno = (AttrNumber)nextAttno++;
    }

    if (!rewrite_outer_order_projection(outer, unionRti,
                                        oldToNew, oldNcols, keys))
        return false;

    foreach (lc, leaves) {
        RangeTblEntry *leafRte = (RangeTblEntry *)lfirst(lc);
        if (!rewrite_leaf_order_projection(leafRte, oldToNew,
                                           oldNcols, keys, leafIndex++))
            return false;
    }

    unionTarget = compact_union_targetlist(unionQuery->targetList,
                                           oldToNew, oldNcols, &ok);
    if (!ok)
        return false;

    unionNames = compact_name_list(unionRte->eref->colnames,
                                   oldToNew, oldNcols, &ok);
    if (!ok)
        return false;

    foreach (lc, keys) {
        UnionAllOrderKey *key = (UnionAllOrderKey *)lfirst(lc);
        TargetEntry *tle = NULL;
        Var *var = NULL;
        char name[NAMEDATALEN];
        errno_t rc;

        keyNumber++;
        rc = snprintf_s(name, NAMEDATALEN, NAMEDATALEN - 1,
                        "__ua_order_%d", keyNumber);
        securec_check_ss(rc, "\0", "\0");

        var = makeVar(leftmostRti,
                      key->newAttno,
                      key->type,
                      key->typmod,
                      key->collation,
                      0);
        tle = makeTargetEntry((Expr *)var,
                              key->newAttno,
                              pstrdup(name),
                              false);
        unionTarget = lappend(unionTarget, tle);
        unionNames = lappend(unionNames, makeString(pstrdup(name)));
    }

    unionQuery->targetList = unionTarget;
    unionRte->eref->colnames = unionNames;

    if (!compact_setop_metadata(unionQuery->setOperations,
                                oldToNew, oldNcols))
        return false;
    append_setop_order_key_metadata(unionQuery->setOperations, keys);

    return true;
}


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
    if (unionRte == NULL || unionRte->security_barrier || unionRte->lateral)
        return;

    /* Collect all leaves from pure UNION ALL tree */
    if (!collect_pure_union_all_leaves(unionRte->subquery, &leaves))
        return;

    /*
     * The UNION container is a semantic boundary too.  An existing LIMIT or
     * OFFSET there selects rows before the outer predicate/order; pushing a
     * new local Top-K beneath it would change which rows can be selected.
     * Reuse the common container and leaf checks, including barrier RTEs.
     */
    int oldNcols = count_visible_tlist_entries(unionRte->subquery->targetList);
    if (oldNcols <= 0 || !validate_required_column_shape(unionRte->subquery, leaves, oldNcols))
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
        query->distinctClause || query->hasWindowFuncs || query->windowClause || query->cteList ||
        query->hasForUpdate || query->hasSubLinks || query->setOperations ||
        query->hasTargetSRFs || query->hasRecursive || query->hasModifyingCTE ||
        query->hasRowSecurity || query->limitIsPercent || query->limitWithTies)
        return false;

    /* A zero-row SRF invalidates input-row Top-K even if the flag is unset. */
    if (expression_returns_set((Node *)query->targetList) ||
        expression_contains_rownum((Node *)query->targetList) || ContainRownumQual(query))
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

    /* Each leaf receives the exact sort operators and NULL ordering below. */

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
    /* WHERE/order movement changes the numbering of a ROWNUM-producing leaf. */
    if (leaf == NULL || leaf->commandType != CMD_SELECT ||
        expression_returns_set((Node *)leaf->targetList) ||
        expression_contains_rownum((Node *)leaf->targetList) || ContainRownumQual(leaf))
        return false;

    /* Must not already have ORDER BY, LIMIT, or setOperations */
    if (leaf->sortClause != NIL || leaf->limitCount != NULL ||
        leaf->limitOffset != NULL || leaf->setOperations != NULL)
        return false;

    /* Must not have aggregation, grouping, DISTINCT, window, CTE, row locks, SRF */
    if (leaf->hasAggs || leaf->groupClause || leaf->havingQual ||
        leaf->distinctClause || leaf->hasWindowFuncs || leaf->cteList ||
        leaf->hasForUpdate || leaf->hasTargetSRFs || leaf->hasRecursive ||
        leaf->hasModifyingCTE || leaf->hasRowSecurity)
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
