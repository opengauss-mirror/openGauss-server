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
 * ---------------------------------------------------------------------------------------
 *
 * IDENTIFICATION
 *        contrib/gauss_connector/deparse.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "gc_fdw.h"

#include "access/heapam.h"
#include "access/htup.h"
#include "access/sysattr.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/plannodes.h"
#include "optimizer/clauses.h"
#include "optimizer/prep.h"
#include "optimizer/tlist.h"
#include "optimizer/var.h"
#include "parser/parsetree.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/typcache.h"

/*
 * Global context for gcforeign_expr_walker's search of an expression tree.
 */
typedef struct foreign_glob_cxt {
    PlannerInfo* root;      /* global planner state */
    RelOptInfo* foreignrel; /* the foreign relation we are planning for */
    Relids relids;          /* relids of base relations in the underlying
                             * scan */
} foreign_glob_cxt;

/*
 * Local (per-tree-level) context for gcforeign_expr_walker's search.
 * This is concerned with identifying collations used in the expression.
 */
typedef enum {
    FDW_COLLATE_NONE,  /* expression is of a noncollatable type, or
                        * it has default collation that is not
                        * traceable to a foreign Var */
    FDW_COLLATE_SAFE,  /* collation derives from a foreign Var */
    FDW_COLLATE_UNSAFE /* collation is non-default and derives from
                        * something other than a foreign Var */
} FDWCollateState;

typedef struct foreign_loc_cxt {
    Oid collation;         /* OID of current collation, if any */
    FDWCollateState state; /* state of current collation choice */
} foreign_loc_cxt;

/*
 * Context for gcDeparseExpr
 */
typedef struct deparse_expr_cxt {
    PlannerInfo* root;      /* global planner state */
    RelOptInfo* foreignrel; /* the foreign relation we are planning for */
    RelOptInfo* scanrel;    /* the underlying scan relation. Same as
                             * foreignrel, when that represents a join or
                             * a base relation. */
    StringInfo buf;         /* output buffer to append to */
    List** params_list;     /* exprs that will become remote Params */
    bool coorquery;         /* just for coor query */
    Plan* agg;              /* just for coor query */
    List* str_targetlist;   /* just for coor query */
    char* agg_arg1;
    char* agg_arg2;
    List** colmap;
    int map;
    bool local_schema;
} deparse_expr_cxt;

#define REL_ALIAS_PREFIX "r"
/* Handy macro to add relation name qualification */
#define ADD_REL_QUALIFIER(buf, varno) appendStringInfo((buf), "%s%d.", REL_ALIAS_PREFIX, (varno))
#define ADD_UNSIGNED_REL_QUALIFIER(buf, varno) appendStringInfo((buf), "%s%u.", REL_ALIAS_PREFIX, (varno))
#define SUBQUERY_REL_ALIAS_PREFIX "s"
#define SUBQUERY_COL_ALIAS_PREFIX "c"

/*
 * Functions to determine whether an expression can be evaluated safely on
 * remote server.
 */
static bool gcforeign_expr_walker(Node* node, foreign_glob_cxt* glob_cxt, foreign_loc_cxt* outer_cxt);
static char* deparse_type_name(Oid type_oid, int32 typemod);

/*
 * Functions to construct string representation of a node tree.
 */
static void gcdeparseTargetList(StringInfo buf, PlannerInfo* root, Index rtindex, Relation rel, bool is_returning,
    Bitmapset* attrs_used, bool qualify_col, List** retrieved_attrs);
static void gcDeparseExplicitTargetList(List* tlist, List** retrieved_attrs, deparse_expr_cxt* context);
static void gcDeparseSubqueryTargetList(deparse_expr_cxt* context);
static void gcDeparseColumnRef(StringInfo buf, int varno, int varattno, PlannerInfo* root, bool qualify_col);
static void gcDeparseRelation(StringInfo buf, Relation rel, bool schema = true);
static void gcDeparseExpr(Expr* expr, deparse_expr_cxt* context);
static void gcDeparseVar(Var* node, deparse_expr_cxt* context);
static void simpleDeparseVar(Var* node, deparse_expr_cxt* context);
static void gcDeparseConst(Const* node, deparse_expr_cxt* context, int showtype);
static void gcDeparseParam(Param* node, deparse_expr_cxt* context);
static void gcDeparseArrayRef(ArrayRef* node, deparse_expr_cxt* context);
static void gcDeparseFuncExpr(FuncExpr* node, deparse_expr_cxt* context);
static void gcDeparseOpExpr(OpExpr* node, deparse_expr_cxt* context);
static void gcDeparseOperatorName(StringInfo buf, Form_pg_operator opform);
static void gcDeparseDistinctExpr(DistinctExpr* node, deparse_expr_cxt* context);
static void gcDeparseScalarArrayOpExpr(ScalarArrayOpExpr* node, deparse_expr_cxt* context);
static void gcDeparseRelabelType(RelabelType* node, deparse_expr_cxt* context);
static void gcDeparseBoolExpr(BoolExpr* node, deparse_expr_cxt* context);
static void gcDeparseNullTest(NullTest* node, deparse_expr_cxt* context);
static void gcDeparseArrayExpr(ArrayExpr* node, deparse_expr_cxt* context);
static void printRemoteParam(int paramindex, Oid paramtype, int32 paramtypmod, deparse_expr_cxt* context);
static void printRemotePlaceholder(Oid paramtype, int32 paramtypmod, deparse_expr_cxt* context);
static void gcDeparseSelectSql(List* tlist, bool is_subquery, List** retrieved_attrs, deparse_expr_cxt* context);
static void gcDeparseLockingClause(deparse_expr_cxt* context);
static void gcAppendOrderByClause(List* pathkeys, deparse_expr_cxt* context);
static void appendConditions(List* exprs, deparse_expr_cxt* context);
static void gcDeparseFromExprForRel(StringInfo buf, PlannerInfo* root, RelOptInfo* joinrel, bool use_alias,
    List** params_list, bool local_schema = false);
static void gcDeparseFromExpr(List* quals, deparse_expr_cxt* context);
static void simpleDeparseAggref(Aggref* node, deparse_expr_cxt* context);
static void appendAggOrderBy(List* orderList, List* targetList, deparse_expr_cxt* context);
static void appendFunctionName(Oid funcid, deparse_expr_cxt* context);
static Node* deparseSortGroupClause(Index ref, List* tlist, bool force_colno, deparse_expr_cxt* context);
extern bool pgxc_is_expr_shippable(Expr* node, bool* has_aggs);

/*
 * Helper functions
 */
static bool is_subquery_var(Var* node, RelOptInfo* foreignrel, int* relno, int* colno);
static void get_relation_column_alias_ids(Var* node, RelOptInfo* foreignrel, int* relno, int* colno);

/*
 * Examine each qual clause in input_conds, and classify them into two groups,
 * which are returned as two lists:
 *	- remote_conds contains expressions that can be evaluated remotely
 *	- local_conds contains expressions that can't be evaluated remotely
 */
void classifyConditions(
    PlannerInfo* root, RelOptInfo* baserel, List* input_conds, List** remote_conds, List** local_conds)
{
    ListCell* lc = NULL;

    bool can_remote_filter = true;
    foreach (lc, input_conds) {
        RestrictInfo* ri = lfirst_node(RestrictInfo, lc);

        if (!is_foreign_expr(root, baserel, ri->clause)) {
            can_remote_filter = false;
            break;
        }
    }

    *remote_conds = NIL;
    *local_conds = NIL;

    foreach (lc, input_conds) {
        RestrictInfo* ri = lfirst_node(RestrictInfo, lc);

        if (can_remote_filter && is_foreign_expr(root, baserel, ri->clause))
            *remote_conds = lappend(*remote_conds, ri);
        else
            *local_conds = lappend(*local_conds, ri);
    }
}

/*
 * Returns true if given expr is safe to evaluate on the foreign server.
 */
bool is_foreign_expr(PlannerInfo* root, RelOptInfo* baserel, Expr* expr)
{
    foreign_glob_cxt glob_cxt;
    foreign_loc_cxt loc_cxt;

    /*
     * Check that the expression consists of nodes that are safe to execute
     * remotely.
     */
    glob_cxt.root = root;
    glob_cxt.foreignrel = baserel;

    glob_cxt.relids = baserel->relids;
    loc_cxt.collation = InvalidOid;
    loc_cxt.state = FDW_COLLATE_NONE;
    if (!gcforeign_expr_walker((Node*)expr, &glob_cxt, &loc_cxt))
        return false;

    /*
     * If the expression has a valid collation that does not arise from a
     * foreign var, the expression can not be sent over.
     */
    if (loc_cxt.state == FDW_COLLATE_UNSAFE)
        return false;

    /*
     * An expression which includes any mutable functions can't be sent over
     * because its result is not stable.  For example, sending now() remote
     * side could cause confusion from clock offsets.  Future versions might
     * be able to make this choice with more granularity.  (We check this last
     * because it requires a lot of expensive catalog lookups.)
     */
    if (contain_mutable_functions((Node*)expr))
        return false;

    /* OK to evaluate on the remote server */
    return true;
}

/*
 * Check if expression is safe to execute remotely, and return true if so.
 *
 * In addition, *outer_cxt is updated with collation information.
 *
 * We must check that the expression contains only node types we can deparse,
 * that all types/functions/operators are safe to send (they are "shippable"),
 * and that all collations used in the expression derive from Vars of the
 * foreign table.  Because of the latter, the logic is pretty close to
 * assign_collations_walker() in parse_collate.c, though we can assume here
 * that the given expression is valid.  Note function mutability is not
 * currently considered here.
 */
static bool gcforeign_expr_walker(Node* node, foreign_glob_cxt* glob_cxt, foreign_loc_cxt* outer_cxt)
{
    bool check_type = true;
    GcFdwRelationInfo* fpinfo = NULL;
    foreign_loc_cxt inner_cxt;
    Oid collation;
    FDWCollateState state;

    /* Need do nothing for empty subexpressions */
    if (node == NULL)
        return true;

    /* May need server info from baserel's fdw_private struct */
    fpinfo = (GcFdwRelationInfo*)(glob_cxt->foreignrel->fdw_private);

    /* Set up inner_cxt for possible recursion to child nodes */
    inner_cxt.collation = InvalidOid;
    inner_cxt.state = FDW_COLLATE_NONE;

    switch (nodeTag(node)) {
        case T_Var: {
            Var* var = (Var*)node;

            /*
             * If the Var is from the foreign table, we consider its
             * collation (if any) safe to use.  If it is from another
             * table, we treat its collation the same way as we would a
             * Param's collation, ie it's not safe for it to have a
             * non-default collation.
             */
            if (bms_is_member(var->varno, glob_cxt->relids) && var->varlevelsup == 0) {
                /* Var belongs to foreign table */

                /*
                 * System columns other than ctid and oid should not be
                 * sent to the remote, since we don't make any effort to
                 * ensure that local and remote values match (tableoid, in
                 * particular, almost certainly doesn't match).
                 */
                if (var->varattno < 0 && var->varattno != SelfItemPointerAttributeNumber &&
                    var->varattno != ObjectIdAttributeNumber)
                    return false;

                /* Else check the collation */
                collation = var->varcollid;
                state = OidIsValid(collation) ? FDW_COLLATE_SAFE : FDW_COLLATE_NONE;
            } else {
                /* Var belongs to some other table */
                collation = var->varcollid;
                if (collation == InvalidOid || collation == DEFAULT_COLLATION_OID) {
                    /*
                     * It's noncollatable, or it's safe to combine with a
                     * collatable foreign Var, so set state to NONE.
                     */
                    state = FDW_COLLATE_NONE;
                } else {
                    /*
                     * Do not fail right away, since the Var might appear
                     * in a collation-insensitive context.
                     */
                    state = FDW_COLLATE_UNSAFE;
                }
            }
        } break;
        case T_Const: {
            Const* c = (Const*)node;

            /*
             * If the constant has nondefault collation, either it's of a
             * non-builtin type, or it reflects folding of a CollateExpr.
             * It's unsafe to send to the remote unless it's used in a
             * non-collation-sensitive context.
             */
            collation = c->constcollid;
            if (collation == InvalidOid || collation == DEFAULT_COLLATION_OID)
                state = FDW_COLLATE_NONE;
            else
                state = FDW_COLLATE_UNSAFE;
        } break;
        case T_Param: {
            Param* p = (Param*)node;

            if (PARAM_EXTERN != p->paramkind)
                return false;

            /*
             * Collation rule is same as for Consts and non-foreign Vars.
             */
            collation = p->paramcollid;
            if (collation == InvalidOid || collation == DEFAULT_COLLATION_OID)
                state = FDW_COLLATE_NONE;
            else
                state = FDW_COLLATE_UNSAFE;
        } break;
        case T_ArrayRef: {
            ArrayRef* ar = (ArrayRef*)node;

            /* Assignment should not be in restrictions. */
            if (ar->refassgnexpr != NULL)
                return false;

            /*
             * Recurse to remaining subexpressions.  Since the array
             * subscripts must yield (noncollatable) integers, they won't
             * affect the inner_cxt state.
             */
            if (!gcforeign_expr_walker((Node*)ar->refupperindexpr, glob_cxt, &inner_cxt))
                return false;
            if (!gcforeign_expr_walker((Node*)ar->reflowerindexpr, glob_cxt, &inner_cxt))
                return false;
            if (!gcforeign_expr_walker((Node*)ar->refexpr, glob_cxt, &inner_cxt))
                return false;

            /*
             * Array subscripting should yield same collation as input,
             * but for safety use same logic as for function nodes.
             */
            collation = ar->refcollid;
            if (collation == InvalidOid)
                state = FDW_COLLATE_NONE;
            else if (inner_cxt.state == FDW_COLLATE_SAFE && collation == inner_cxt.collation)
                state = FDW_COLLATE_SAFE;
            else if (collation == DEFAULT_COLLATION_OID)
                state = FDW_COLLATE_NONE;
            else
                state = FDW_COLLATE_UNSAFE;
        } break;
        case T_FuncExpr: {
            FuncExpr* fe = (FuncExpr*)node;

            /*
             * If function used by the expression is not shippable, it
             * can't be sent to remote because it might have incompatible
             * semantics on remote side.
             */
            if (!is_shippable(fe->funcid, ProcedureRelationId, fpinfo))
                return false;

            /*
             * Recurse to input subexpressions.
             */
            if (!gcforeign_expr_walker((Node*)fe->args, glob_cxt, &inner_cxt))
                return false;

            /*
             * If function's input collation is not derived from a foreign
             * Var, it can't be sent to remote.
             */
            if (fe->inputcollid == InvalidOid)
                /* OK, inputs are all noncollatable */;
            else if (inner_cxt.state != FDW_COLLATE_SAFE || fe->inputcollid != inner_cxt.collation)
                return false;

            /*
             * Detect whether node is introducing a collation not derived
             * from a foreign Var.  (If so, we just mark it unsafe for now
             * rather than immediately returning false, since the parent
             * node might not care.)
             */
            collation = fe->funccollid;
            if (collation == InvalidOid)
                state = FDW_COLLATE_NONE;
            else if (inner_cxt.state == FDW_COLLATE_SAFE && collation == inner_cxt.collation)
                state = FDW_COLLATE_SAFE;
            else if (collation == DEFAULT_COLLATION_OID)
                state = FDW_COLLATE_NONE;
            else
                state = FDW_COLLATE_UNSAFE;
        } break;
        case T_OpExpr:
        case T_DistinctExpr: /* struct-equivalent to OpExpr */
        {
            OpExpr* oe = (OpExpr*)node;

            /*
             * Similarly, only shippable operators can be sent to remote.
             * (If the operator is shippable, we assume its underlying
             * function is too.)
             */
            if (!is_shippable(oe->opno, OperatorRelationId, fpinfo))
                return false;

            /*
             * Recurse to input subexpressions.
             */
            if (!gcforeign_expr_walker((Node*)oe->args, glob_cxt, &inner_cxt))
                return false;

            /*
             * If operator's input collation is not derived from a foreign
             * Var, it can't be sent to remote.
             */
            if (oe->inputcollid == InvalidOid)
                /* OK, inputs are all noncollatable */;
            else if (inner_cxt.state != FDW_COLLATE_SAFE || oe->inputcollid != inner_cxt.collation)
                return false;

            /* Result-collation handling is same as for functions */
            collation = oe->opcollid;
            if (collation == InvalidOid)
                state = FDW_COLLATE_NONE;
            else if (inner_cxt.state == FDW_COLLATE_SAFE && collation == inner_cxt.collation)
                state = FDW_COLLATE_SAFE;
            else if (collation == DEFAULT_COLLATION_OID)
                state = FDW_COLLATE_NONE;
            else
                state = FDW_COLLATE_UNSAFE;
        } break;
        case T_ScalarArrayOpExpr: {
            ScalarArrayOpExpr* oe = (ScalarArrayOpExpr*)node;

            /*
             * Again, only shippable operators can be sent to remote.
             */
            if (!is_shippable(oe->opno, OperatorRelationId, fpinfo))
                return false;

            /*
             * Recurse to input subexpressions.
             */
            if (!gcforeign_expr_walker((Node*)oe->args, glob_cxt, &inner_cxt))
                return false;

            /*
             * If operator's input collation is not derived from a foreign
             * Var, it can't be sent to remote.
             */
            if (oe->inputcollid == InvalidOid)
                /* OK, inputs are all noncollatable */;
            else if (inner_cxt.state != FDW_COLLATE_SAFE || oe->inputcollid != inner_cxt.collation)
                return false;

            /* Output is always boolean and so noncollatable. */
            collation = InvalidOid;
            state = FDW_COLLATE_NONE;
        } break;
        case T_RelabelType: {
            RelabelType* r = (RelabelType*)node;

            /*
             * Recurse to input subexpression.
             */
            if (!gcforeign_expr_walker((Node*)r->arg, glob_cxt, &inner_cxt))
                return false;

            /*
             * RelabelType must not introduce a collation not derived from
             * an input foreign Var (same logic as for a real function).
             */
            collation = r->resultcollid;
            if (collation == InvalidOid)
                state = FDW_COLLATE_NONE;
            else if (inner_cxt.state == FDW_COLLATE_SAFE && collation == inner_cxt.collation)
                state = FDW_COLLATE_SAFE;
            else if (collation == DEFAULT_COLLATION_OID)
                state = FDW_COLLATE_NONE;
            else
                state = FDW_COLLATE_UNSAFE;
        } break;
        case T_BoolExpr: {
            BoolExpr* b = (BoolExpr*)node;

            /*
             * Recurse to input subexpressions.
             */
            if (!gcforeign_expr_walker((Node*)b->args, glob_cxt, &inner_cxt))
                return false;

            /* Output is always boolean and so noncollatable. */
            collation = InvalidOid;
            state = FDW_COLLATE_NONE;
        } break;
        case T_NullTest: {
            NullTest* nt = (NullTest*)node;

            /*
             * Recurse to input subexpressions.
             */
            if (!gcforeign_expr_walker((Node*)nt->arg, glob_cxt, &inner_cxt))
                return false;

            /* Output is always boolean and so noncollatable. */
            collation = InvalidOid;
            state = FDW_COLLATE_NONE;
        } break;
        case T_ArrayExpr: {
            ArrayExpr* a = (ArrayExpr*)node;

            /*
             * Recurse to input subexpressions.
             */
            if (!gcforeign_expr_walker((Node*)a->elements, glob_cxt, &inner_cxt))
                return false;

            /*
             * ArrayExpr must not introduce a collation not derived from
             * an input foreign Var (same logic as for a function).
             */
            collation = a->array_collid;
            if (collation == InvalidOid)
                state = FDW_COLLATE_NONE;
            else if (inner_cxt.state == FDW_COLLATE_SAFE && collation == inner_cxt.collation)
                state = FDW_COLLATE_SAFE;
            else if (collation == DEFAULT_COLLATION_OID)
                state = FDW_COLLATE_NONE;
            else
                state = FDW_COLLATE_UNSAFE;
        } break;
        case T_List: {
            List* l = (List*)node;
            ListCell* lc = NULL;

            /*
             * Recurse to component subexpressions.
             */
            foreach (lc, l) {
                if (!gcforeign_expr_walker((Node*)lfirst(lc), glob_cxt, &inner_cxt))
                    return false;
            }

            /*
             * When processing a list, collation state just bubbles up
             * from the list elements.
             */
            collation = inner_cxt.collation;
            state = inner_cxt.state;

            /* Don't apply exprType() to the list. */
            check_type = false;
        } break;
        case T_Aggref: {
            Aggref* agg = (Aggref*)node;
            ListCell* lc = NULL;

            /* As usual, it must be shippable. */
            if (!is_shippable(agg->aggfnoid, ProcedureRelationId, fpinfo))
                return false;

            /*
             * Recurse to input args. aggdirectargs, aggorder and
             * aggdistinct are all present in args, so no need to check
             * their shippability explicitly.
             */
            foreach (lc, agg->args) {
                Node* n = (Node*)lfirst(lc);

                /* If TargetEntry, extract the expression from it */
                if (IsA(n, TargetEntry)) {
                    TargetEntry* tle = (TargetEntry*)n;

                    n = (Node*)tle->expr;
                }

                if (!gcforeign_expr_walker(n, glob_cxt, &inner_cxt))
                    return false;
            }

            /*
             * For aggorder elements, check whether the sort operator, if
             * specified, is shippable or not.
             */
            if (agg->aggorder) {
                ListCell* lc_order = NULL;

                foreach (lc_order, agg->aggorder) {
                    SortGroupClause* srt = (SortGroupClause*)lfirst(lc_order);
                    Oid sortcoltype;
                    TypeCacheEntry* typentry = NULL;
                    TargetEntry* tle = NULL;

                    tle = get_sortgroupref_tle(srt->tleSortGroupRef, agg->args);
                    sortcoltype = exprType((Node*)tle->expr);
                    typentry = lookup_type_cache(sortcoltype, TYPECACHE_LT_OPR | TYPECACHE_GT_OPR);
                    /* Check shippability of non-default sort operator. */
                    if (srt->sortop != typentry->lt_opr && srt->sortop != typentry->gt_opr &&
                        !is_shippable(srt->sortop, OperatorRelationId, fpinfo))
                        return false;
                }
            }

            /*
             * If aggregate's input collation is not derived from a
             * foreign Var, it can't be sent to remote.
             */
            if (agg->inputcollid == InvalidOid)
                /* OK, inputs are all noncollatable */;
            else if (inner_cxt.state != FDW_COLLATE_SAFE || agg->inputcollid != inner_cxt.collation)
                return false;

            /*
             * Detect whether node is introducing a collation not derived
             * from a foreign Var.  (If so, we just mark it unsafe for now
             * rather than immediately returning false, since the parent
             * node might not care.)
             */
            collation = agg->aggcollid;
            if (collation == InvalidOid)
                state = FDW_COLLATE_NONE;
            else if (inner_cxt.state == FDW_COLLATE_SAFE && collation == inner_cxt.collation)
                state = FDW_COLLATE_SAFE;
            else if (collation == DEFAULT_COLLATION_OID)
                state = FDW_COLLATE_NONE;
            else
                state = FDW_COLLATE_UNSAFE;
        } break;
        default:

            /*
             * If it's anything else, assume it's unsafe.  This list can be
             * expanded later, but don't forget to add deparse support below.
             */
            return false;
    }

    /*
     * If result type of given expression is not shippable, it can't be sent
     * to remote because it might have incompatible semantics on remote side.
     */
    if (check_type && !is_shippable(exprType(node), TypeRelationId, fpinfo))
        return false;

    /*
     * Now, merge my collation information into my parent's state.
     */
    if (state > outer_cxt->state) {
        /* Override previous parent state */
        outer_cxt->collation = collation;
        outer_cxt->state = state;
    } else if (state == outer_cxt->state) {
        /* Merge, or detect error if there's a collation conflict */
        switch (state) {
            case FDW_COLLATE_NONE:
                /* Nothing + nothing is still nothing */
                break;
            case FDW_COLLATE_SAFE:
                if (collation != outer_cxt->collation) {
                    /*
                     * Non-default collation always beats default.
                     */
                    if (outer_cxt->collation == DEFAULT_COLLATION_OID) {
                        /* Override previous parent state */
                        outer_cxt->collation = collation;
                    } else if (collation != DEFAULT_COLLATION_OID) {
                        /*
                         * Conflict; show state as indeterminate.  We don't
                         * want to "return false" right away, since parent
                         * node might not care about collation.
                         */
                        outer_cxt->state = FDW_COLLATE_UNSAFE;
                    }
                }
                break;
            case FDW_COLLATE_UNSAFE:
                /* We're still conflicted ... */
                break;
        }
    }

    /* It looks OK */
    return true;
}

/*
 * Convert type OID + typmod info into a type name we can ship to the remote
 * server.  Someplace else had better have verified that this type name is
 * expected to be known on the remote end.
 *
 * This is almost just format_type_with_typemod(), except that if left to its
 * own devices, that function will make schema-qualification decisions based
 * on the local search_path, which is wrong.  We must schema-qualify all
 * type names that are not in pg_catalog.  We assume here that built-in types
 * are all in pg_catalog and need not be qualified; otherwise, qualify.
 */
static char* deparse_type_name(Oid type_oid, int32 typemod)
{
    if (is_builtin(type_oid))
        return format_type_with_typemod(type_oid, typemod);
    else {
        elog(ERROR, "unsupported data type %u", type_oid);
        return NULL; /* keep compiler silence */
    }
}

/*
 * Build the targetlist for given relation to be deparsed as SELECT clause.
 *
 * The output targetlist contains the columns that need to be fetched from the
 * foreign server for the given relation.  If foreignrel is an upper relation,
 * then the output targetlist can also contain expressions to be evaluated on
 * foreign server.
 */
List* build_tlist_to_deparse(RelOptInfo* foreignrel)
{
    List* tlist = NIL;
    GcFdwRelationInfo* fpinfo = (GcFdwRelationInfo*)foreignrel->fdw_private;
    ListCell* lc = NULL;

    /*
     * We require columns specified in foreignrel->reltarget->exprs and those
     * required for evaluating the local conditions.
     */
    tlist = foreignrel->reltarget->exprs;

    foreach (lc, fpinfo->local_conds) {
        RestrictInfo* rinfo = lfirst_node(RestrictInfo, lc);

        tlist = add_to_flat_tlist(
            tlist, pull_var_clause((Node*)rinfo->clause, PVC_REJECT_AGGREGATES, PVC_RECURSE_PLACEHOLDERS));
    }

    return tlist;
}

/*
 * Deparse SELECT statement for given relation into buf.
 *
 * tlist contains the list of desired columns to be fetched from foreign server.
 * For a base relation fpinfo->attrs_used is used to construct SELECT clause,
 * hence the tlist is ignored for a base relation.
 *
 * remote_conds is the list of conditions to be deparsed into the WHERE clause
 * (or, in the case of upper relations, into the HAVING clause).
 *
 * If params_list is not NULL, it receives a list of Params and other-relation
 * Vars used in the clauses; these values must be transmitted to the remote
 * server as parameter values.
 *
 * If params_list is NULL, we're generating the query for EXPLAIN purposes,
 * so Params and other-relation Vars should be replaced by dummy values.
 *
 * pathkeys is the list of pathkeys to order the result by.
 *
 * is_subquery is the flag to indicate whether to deparse the specified
 * relation as a subquery.
 *
 * List of columns selected is returned in retrieved_attrs.
 */
extern void gcDeparseSelectStmtForRel(StringInfo buf, PlannerInfo* root, RelOptInfo* rel, List* tlist,
    List* remote_conds, List* pathkeys, bool is_subquery, List** retrieved_attrs, List** params_list)
{
    deparse_expr_cxt context;
    List* quals = NIL;

    /*
     * We handle relations for foreign tables, joins between those and upper
     * relations.
     */
    Assert(IS_JOIN_REL(rel) || IS_SIMPLE_REL(rel));

    /* Fill portions of context common to upper, join and base relation */
    context.buf = buf;
    context.root = root;
    context.foreignrel = rel;
    context.scanrel = rel;
    context.params_list = params_list;
    context.coorquery = false;
    context.local_schema = false;

    /* Construct SELECT clause */
    gcDeparseSelectSql(tlist, is_subquery, retrieved_attrs, &context);

    quals = remote_conds;

    /* Construct FROM and WHERE clauses */
    gcDeparseFromExpr(quals, &context);

    /* Add ORDER BY clause if we found any useful pathkeys */
    if (pathkeys != NULL)
        gcAppendOrderByClause(pathkeys, &context);

    /* Add any necessary FOR UPDATE/SHARE. */
    gcDeparseLockingClause(&context);
}

/*
 * Construct a simple SELECT statement that retrieves desired columns
 * of the specified foreign table, and append it to "buf".  The output
 * contains just "SELECT ... ".
 *
 * We also create an integer List of the columns being retrieved, which is
 * returned to *retrieved_attrs, unless we deparse the specified relation
 * as a subquery.
 *
 * tlist is the list of desired columns.  is_subquery is the flag to
 * indicate whether to deparse the specified relation as a subquery.
 * Read prologue of gcDeparseSelectStmtForRel() for details.
 */
static void gcDeparseSelectSql(List* tlist, bool is_subquery, List** retrieved_attrs, deparse_expr_cxt* context)
{
    StringInfo buf = context->buf;
    RelOptInfo* foreignrel = context->foreignrel;
    PlannerInfo* root = context->root;
    GcFdwRelationInfo* fpinfo = (GcFdwRelationInfo*)foreignrel->fdw_private;

    /*
     * Construct SELECT list
     */
    appendStringInfoString(buf, "SELECT ");

    if (is_subquery) {
        /*
         * For a relation that is deparsed as a subquery, emit expressions
         * specified in the relation's reltarget.  Note that since this is for
         * the subquery, no need to care about *retrieved_attrs.
         */
        gcDeparseSubqueryTargetList(context);
    } else if (IS_JOIN_REL(foreignrel)) {
        /*
         * For a join or upper relation the input tlist gives the list of
         * columns required to be fetched from the foreign server.
         */
        gcDeparseExplicitTargetList(tlist, retrieved_attrs, context);
    } else {
        /*
         * For a base relation fpinfo->attrs_used gives the list of columns
         * required to be fetched from the foreign server.
         */
        RangeTblEntry* rte = planner_rt_fetch(foreignrel->relid, root);

        /*
         * Core code already has some lock on each rel being planned, so we
         * can use NoLock here.
         */
        Relation rel = heap_open(rte->relid, NoLock);

        gcdeparseTargetList(buf, root, foreignrel->relid, rel, false, fpinfo->attrs_used, false, retrieved_attrs);
        heap_close(rel, NoLock);
    }
}

/*
 * Construct a FROM clause and, if needed, a WHERE clause, and append those to
 * "buf".
 *
 * quals is the list of clauses to be included in the WHERE clause.
 * (These may or may not include RestrictInfo decoration.)
 */
static void gcDeparseFromExpr(List* quals, deparse_expr_cxt* context)
{
    StringInfo buf = context->buf;
    RelOptInfo* scanrel = context->scanrel;

    /* For upper relations, scanrel must be either a joinrel or a baserel */
    Assert(IS_JOIN_REL(scanrel) || IS_SIMPLE_REL(scanrel));

    /* Construct FROM clause */
    appendStringInfoString(buf, " FROM ");
    gcDeparseFromExprForRel(buf,
        context->root,
        scanrel,
        (bms_num_members(scanrel->relids) > 1),
        context->params_list,
        context->local_schema);

    /* Construct WHERE clause */
    if (quals != NIL) {
        appendStringInfo(buf, " WHERE ");
        appendConditions(quals, context);
    }
}

/*
 * Emit a target list that retrieves the columns specified in attrs_used.
 * This is used for both SELECT and RETURNING targetlists; the is_returning
 * parameter is true only for a RETURNING targetlist.
 *
 * The tlist text is appended to buf, and we also create an integer List
 * of the columns being retrieved, which is returned to *retrieved_attrs.
 *
 * If qualify_col is true, add relation alias before the column name.
 */
static void gcdeparseTargetList(StringInfo buf, PlannerInfo* root, Index rtindex, Relation rel, bool is_returning,
    Bitmapset* attrs_used, bool qualify_col, List** retrieved_attrs)
{
    TupleDesc tupdesc = RelationGetDescr(rel);
    bool have_wholerow = false;
    bool first = true;
    int i;

    *retrieved_attrs = NIL;

    /* If there's a whole-row reference, we'll need all the columns. */
    have_wholerow = bms_is_member(0 - FirstLowInvalidHeapAttributeNumber, attrs_used);

    for (i = 1; i <= tupdesc->natts; i++) {
        Form_pg_attribute attr = &tupdesc->attrs[i - 1];

        /* Ignore dropped attributes. */
        if (attr->attisdropped)
            continue;

        if (have_wholerow || bms_is_member(i - FirstLowInvalidHeapAttributeNumber, attrs_used)) {
            if (!first)
                appendStringInfoString(buf, ", ");
            else if (is_returning)
                appendStringInfoString(buf, " RETURNING ");
            first = false;

            gcDeparseColumnRef(buf, rtindex, i, root, qualify_col);

            *retrieved_attrs = lappend_int(*retrieved_attrs, i);
        }
    }

    /*
     * Add ctid and oid if needed.  We currently don't support retrieving any
     * other system columns.
     */
    if (bms_is_member(SelfItemPointerAttributeNumber - FirstLowInvalidHeapAttributeNumber, attrs_used)) {
        if (!first)
            appendStringInfoString(buf, ", ");
        else if (is_returning)
            appendStringInfoString(buf, " RETURNING ");
        first = false;

        if (qualify_col)
            ADD_UNSIGNED_REL_QUALIFIER(buf, rtindex);
        appendStringInfoString(buf, "ctid");

        *retrieved_attrs = lappend_int(*retrieved_attrs, SelfItemPointerAttributeNumber);
    }
    if (bms_is_member(ObjectIdAttributeNumber - FirstLowInvalidHeapAttributeNumber, attrs_used)) {
        if (!first)
            appendStringInfoString(buf, ", ");
        else if (is_returning)
            appendStringInfoString(buf, " RETURNING ");
        first = false;

        if (qualify_col)
            ADD_UNSIGNED_REL_QUALIFIER(buf, rtindex);
        appendStringInfoString(buf, "oid");

        *retrieved_attrs = lappend_int(*retrieved_attrs, ObjectIdAttributeNumber);
    }

    /* Don't generate bad syntax if no undropped columns */
    if (first && !is_returning)
        appendStringInfoString(buf, "NULL");
}

/*
 * Deparse the appropriate locking clause (FOR UPDATE or FOR SHARE) for a
 * given relation (context->scanrel).
 */
static void gcDeparseLockingClause(deparse_expr_cxt* context)
{
    StringInfo buf = context->buf;
    PlannerInfo* root = context->root;
    RelOptInfo* rel = context->scanrel;
    GcFdwRelationInfo* fpinfo = (GcFdwRelationInfo*)rel->fdw_private;
    int relid = -1;

    while ((relid = bms_next_member(rel->relids, relid)) >= 0) {
        /*
         * Ignore relation if it appears in a lower subquery.  Locking clause
         * for such a relation is included in the subquery if necessary.
         */
        if (bms_is_member(relid, fpinfo->lower_subquery_rels))
            continue;

        /*
         * Add FOR UPDATE/SHARE if appropriate.  We apply locking during the
         * initial row fetch, rather than later on as is done for local
         * tables. The extra roundtrips involved in trying to duplicate the
         * local semantics exactly don't seem worthwhile (see also comments
         * for RowMarkType).
         *
         * Note: because we actually run the query as a cursor, this assumes
         * that DECLARE CURSOR ... FOR UPDATE is supported, which it isn't
         * before 8.3.
         */
        if (relid == linitial2_int(root->parse->resultRelations) &&
            (root->parse->commandType == CMD_UPDATE || root->parse->commandType == CMD_DELETE)) {
            /* Relation is UPDATE/DELETE target, so use FOR UPDATE */
            appendStringInfoString(buf, " FOR UPDATE");

            /* Add the relation alias if we are here for a join relation */
            if (IS_JOIN_REL(rel))
                appendStringInfo(buf, " OF %s%d", REL_ALIAS_PREFIX, relid);
        } else {
            PlanRowMark* rc = get_plan_rowmark(root->rowMarks, relid);

            if (rc != NULL) {
                /* Add the relation alias if we are here for a join relation */
                if (bms_num_members(rel->relids) > 1 /*&&
					rc->strength != LCS_NONE*/)
                    appendStringInfo(buf, " OF %s%d", REL_ALIAS_PREFIX, relid);
            }
        }
    }
}

/*
 * Deparse conditions from the provided list and append them to buf.
 *
 * The conditions in the list are assumed to be ANDed. This function is used to
 * deparse WHERE clauses, JOIN .. ON clauses and HAVING clauses.
 *
 * Depending on the caller, the list elements might be either RestrictInfos
 * or bare clauses.
 */
static void appendConditions(List* exprs, deparse_expr_cxt* context)
{
    int nestlevel;
    ListCell* lc = NULL;
    bool is_first = true;
    StringInfo buf = context->buf;

    /* Make sure any constants in the exprs are printed portably */
    nestlevel = set_transmission_modes();

    foreach (lc, exprs) {
        Expr* expr = (Expr*)lfirst(lc);

        /* Extract clause from RestrictInfo, if required */
        if (IsA(expr, RestrictInfo))
            expr = ((RestrictInfo*)expr)->clause;

        /* Connect expressions with "AND" and parenthesize each condition. */
        if (!is_first)
            appendStringInfoString(buf, " AND ");

        appendStringInfoChar(buf, '(');
        gcDeparseExpr(expr, context);
        appendStringInfoChar(buf, ')');

        is_first = false;
    }

    reset_transmission_modes(nestlevel);
}

/*
 * Deparse given targetlist and append it to context->buf.
 *
 * tlist is list of TargetEntry's which in turn contain Var nodes.
 *
 * retrieved_attrs is the list of continuously increasing integers starting
 * from 1. It has same number of entries as tlist.
 */
static void gcDeparseExplicitTargetList(List* tlist, List** retrieved_attrs, deparse_expr_cxt* context)
{
    ListCell* lc = NULL;
    StringInfo buf = context->buf;
    int i = 0;

    *retrieved_attrs = NIL;

    foreach (lc, tlist) {
        TargetEntry* tle = lfirst_node(TargetEntry, lc);

        if (i > 0)
            appendStringInfoString(buf, ", ");
        gcDeparseExpr((Expr*)tle->expr, context);

        *retrieved_attrs = lappend_int(*retrieved_attrs, i + 1);
        i++;
    }

    if (i == 0)
        appendStringInfoString(buf, "NULL");
}

/*
 * Emit expressions specified in the given relation's reltarget.
 *
 * This is used for deparsing the given relation as a subquery.
 */
static void gcDeparseSubqueryTargetList(deparse_expr_cxt* context)
{
    StringInfo buf = context->buf;
    RelOptInfo* foreignrel = context->foreignrel;
    bool first = true;
    ListCell* lc = NULL;

    /* Should only be called in these cases. */
    Assert(IS_SIMPLE_REL(foreignrel) || IS_JOIN_REL(foreignrel));

    foreach (lc, foreignrel->reltarget->exprs) {
        Node* node = (Node*)lfirst(lc);

        if (!first)
            appendStringInfoString(buf, ", ");
        first = false;

        gcDeparseExpr((Expr*)node, context);
    }

    /* Don't generate bad syntax if no expressions */
    if (first)
        appendStringInfoString(buf, "NULL");
}

/*
 * Construct FROM clause for given relation
 *
 * The function constructs ... JOIN ... ON ... for join relation. For a base
 * relation it just returns schema-qualified tablename, with the appropriate
 * alias if so requested.
 */
static void gcDeparseFromExprForRel(
    StringInfo buf, PlannerInfo* root, RelOptInfo* foreignrel, bool use_alias, List** params_list, bool local_schema)
{
    RangeTblEntry* rte = planner_rt_fetch(foreignrel->relid, root);

    /*
     * Core code already has some lock on each rel being planned, so we
     * can use NoLock here.
     */
    Relation rel = heap_open(rte->relid, NoLock);

    gcDeparseRelation(buf, rel, local_schema);

    /*
     * Add a unique alias to avoid any conflict in relation names due to
     * pulled up subqueries in the query being built for a pushed down
     * join.
     */
    if (use_alias)
        appendStringInfo(buf, " %s%u", REL_ALIAS_PREFIX, foreignrel->relid);

    heap_close(rel, NoLock);
}

/*
 * Construct name to use for given column, and emit it into buf.
 * If it has a column_name FDW option, use that instead of attribute name.
 *
 * If qualify_col is true, qualify column name with the alias of relation.
 */
static void gcDeparseColumnRef(StringInfo buf, int varno, int varattno, PlannerInfo* root, bool qualify_col)
{
    RangeTblEntry* rte = NULL;

    /* We support fetching the remote side's CTID and OID. */
    if (varattno == SelfItemPointerAttributeNumber) {
        if (qualify_col)
            ADD_REL_QUALIFIER(buf, varno);
        appendStringInfoString(buf, "ctid");
    } else if (varattno == ObjectIdAttributeNumber) {
        if (qualify_col)
            ADD_REL_QUALIFIER(buf, varno);
        appendStringInfoString(buf, "oid");
    } else if (varattno < 0) {
        /*
         * All other system attributes are fetched as 0, except for table OID,
         * which is fetched as the local table OID.  However, we must be
         * careful; the table could be beneath an outer join, in which case it
         * must go to NULL whenever the rest of the row does.
         */
        Oid fetchval = 0;

        if (varattno == TableOidAttributeNumber) {
            rte = planner_rt_fetch(varno, root);
            fetchval = rte->relid;
        }

        if (qualify_col) {
            appendStringInfoString(buf, "CASE WHEN (");
            ADD_REL_QUALIFIER(buf, varno);
            appendStringInfo(buf, "*)::text IS NOT NULL THEN %u END", fetchval);
        } else
            appendStringInfo(buf, "%u", fetchval);
    } else if (varattno == 0) {
        /* Whole row reference */
        Relation rel = NULL;
        Bitmapset* attrs_used = NULL;

        /* Required only to be passed down to gcdeparseTargetList(). */
        List* retrieved_attrs = NIL;

        /* Get RangeTblEntry from array in PlannerInfo. */
        rte = planner_rt_fetch(varno, root);

        /*
         * The lock on the relation will be held by upper callers, so it's
         * fine to open it with no lock here.
         */
        rel = heap_open(rte->relid, NoLock);

        /*
         * The local name of the foreign table can not be recognized by the
         * foreign server and the table it references on foreign server might
         * have different column ordering or different columns than those
         * declared locally. Hence we have to deparse whole-row reference as
         * ROW(columns referenced locally). Construct this by deparsing a
         * "whole row" attribute.
         */
        attrs_used = bms_add_member(NULL, 0 - FirstLowInvalidHeapAttributeNumber);

        /*
         * In case the whole-row reference is under an outer join then it has
         * to go NULL whenever the rest of the row goes NULL. Deparsing a join
         * query would always involve multiple relations, thus qualify_col
         * would be true.
         */
        if (qualify_col) {
            appendStringInfoString(buf, "CASE WHEN (");
            ADD_REL_QUALIFIER(buf, varno);
            appendStringInfo(buf, "*)::text IS NOT NULL THEN ");
        }

        appendStringInfoString(buf, "ROW(");
        gcdeparseTargetList(buf, root, varno, rel, false, attrs_used, qualify_col, &retrieved_attrs);
        appendStringInfoString(buf, ")");

        /* Complete the CASE WHEN statement started above. */
        if (qualify_col)
            appendStringInfo(buf, " END");

        heap_close(rel, NoLock);
        bms_free(attrs_used);
    } else {
        char* colname = NULL;
        List* options = NIL;
        ListCell* lc = NULL;

        /* varno must not be any of OUTER_VAR, INNER_VAR and INDEX_VAR. */
        Assert(!IS_SPECIAL_VARNO(varno));

        /* Get RangeTblEntry from array in PlannerInfo. */
        rte = planner_rt_fetch(varno, root);

        /*
         * If it's a column of a foreign table, and it has the column_name FDW
         * option, use that value.
         */
        options = GetForeignColumnOptions(rte->relid, varattno);
        foreach (lc, options) {
            DefElem* def = (DefElem*)lfirst(lc);

            if (strcmp(def->defname, "column_name") == 0) {
                colname = defGetString(def);
                break;
            }
        }

        /*
         * If it's a column of a regular table or it doesn't have column_name
         * FDW option, use attribute name.
         */
        if (colname == NULL)
            colname = get_relid_attribute_name(rte->relid, varattno);

        if (qualify_col)
            ADD_REL_QUALIFIER(buf, varno);

        appendStringInfoString(buf, quote_identifier(colname));
    }
}

/*
 * Append remote name of specified foreign table to buf.
 * Use value of table_name FDW option (if any) instead of relation's name.
 * Similarly, schema_name FDW option overrides schema name.
 */
static void gcDeparseRelation(StringInfo buf, Relation rel, bool local_schema)
{
    ForeignTable* table = NULL;
    const char* nspname = NULL;
    const char* relname = NULL;
    ListCell* lc = NULL;

    /* obtain additional catalog information. */
    table = GetForeignTable(RelationGetRelid(rel));

    /*
     * Use value of FDW options if any, instead of the name of object itself.
     */
    foreach (lc, table->options) {
        DefElem* def = (DefElem*)lfirst(lc);

        if (strcmp(def->defname, "schema_name") == 0)
            nspname = defGetString(def);
        else if (strcmp(def->defname, "table_name") == 0)
            relname = defGetString(def);
    }

    /*
     * Note: we could skip printing the schema name if it's pg_catalog, but
     * that doesn't seem worth the trouble.
     */
    if (nspname == NULL)
        nspname = get_namespace_name(RelationGetNamespace(rel));
    if (relname == NULL)
        relname = RelationGetRelationName(rel);

    if (true == local_schema) {
        relname = RelationGetRelationName(rel);
        nspname = get_namespace_name(RelationGetNamespace(rel));
    }

    appendStringInfo(buf, "%s.%s", quote_identifier(nspname), quote_identifier(relname));
}

/*
 * Append a SQL string literal representing "val" to buf.
 */
void gcDeparseStringLiteral(StringInfo buf, const char* val)
{
    const char* valptr = NULL;

    /*
     * Rather than making assumptions about the remote server's value of
     * u_sess->parser_cxt.standard_conforming_strings, always use E'foo' syntax if there are any
     * backslashes.  This will fail on remote servers before 8.1, but those
     * are long out of support.
     */
    if (strchr(val, '\\') != NULL)
        appendStringInfoChar(buf, ESCAPE_STRING_SYNTAX);
    appendStringInfoChar(buf, '\'');
    for (valptr = val; *valptr; valptr++) {
        char ch = *valptr;

        if (SQL_STR_DOUBLE(ch, true))
            appendStringInfoChar(buf, ch);
        appendStringInfoChar(buf, ch);
    }
    appendStringInfoChar(buf, '\'');
}

/*
 * Deparse given expression into context->buf.
 *
 * This function must support all the same node types that gcforeign_expr_walker
 * accepts.
 *
 * Note: unlike ruleutils.c, we just use a simple hard-wired parenthesization
 * scheme: anything more complex than a Var, Const, function call or cast
 * should be self-parenthesized.
 */
static void gcDeparseExpr(Expr* node, deparse_expr_cxt* context)
{
    if (node == NULL)
        return;

    switch (nodeTag(node)) {
        case T_Var:
            if (context->coorquery)
                simpleDeparseVar((Var*)node, context);
            else
                gcDeparseVar((Var*)node, context);
            break;
        case T_Const:
            gcDeparseConst((Const*)node, context, 0);
            break;
        case T_Param:
            gcDeparseParam((Param*)node, context);
            break;
        case T_ArrayRef:
            gcDeparseArrayRef((ArrayRef*)node, context);
            break;
        case T_FuncExpr:
            gcDeparseFuncExpr((FuncExpr*)node, context);
            break;
        case T_OpExpr:
            gcDeparseOpExpr((OpExpr*)node, context);
            break;
        case T_DistinctExpr:
            gcDeparseDistinctExpr((DistinctExpr*)node, context);
            break;
        case T_ScalarArrayOpExpr:
            gcDeparseScalarArrayOpExpr((ScalarArrayOpExpr*)node, context);
            break;
        case T_RelabelType:
            gcDeparseRelabelType((RelabelType*)node, context);
            break;
        case T_BoolExpr:
            gcDeparseBoolExpr((BoolExpr*)node, context);
            break;
        case T_NullTest:
            gcDeparseNullTest((NullTest*)node, context);
            break;
        case T_ArrayExpr:
            gcDeparseArrayExpr((ArrayExpr*)node, context);
            break;
        case T_Aggref:
            if (context->coorquery)
                simpleDeparseAggref((Aggref*)node, context);
            break;
        default:
            elog(ERROR, "unsupported expression type for deparse: %d", (int)nodeTag(node));
            break;
    }
}

/*
 * Deparse given Var node into context->buf.
 *
 * If the Var belongs to the foreign relation, just print its remote name.
 * Otherwise, it's effectively a Param (and will in fact be a Param at
 * run time).  Handle it the same way we handle plain Params --- see
 * gcDeparseParam for comments.
 */
static void gcDeparseVar(Var* node, deparse_expr_cxt* context)
{
    Relids relids = context->scanrel->relids;
    int relno;
    int colno;

    /* Qualify columns when multiple relations are involved. */
    bool qualify_col = (bms_num_members(relids) > 1);

    /*
     * If the Var belongs to the foreign relation that is deparsed as a
     * subquery, use the relation and column alias to the Var provided by the
     * subquery, instead of the remote name.
     */
    if (is_subquery_var(node, context->scanrel, &relno, &colno)) {
        appendStringInfo(context->buf, "%s%d.%s%d", SUBQUERY_REL_ALIAS_PREFIX, relno, SUBQUERY_COL_ALIAS_PREFIX, colno);
        return;
    }

    if (bms_is_member(node->varno, relids) && node->varlevelsup == 0)
        gcDeparseColumnRef(context->buf, node->varno, node->varattno, context->root, qualify_col);
    else {
        /* Treat like a Param */
        if (context->params_list != NULL) {
            int pindex = 0;
            ListCell* lc = NULL;

            /* find its index in params_list */
            foreach (lc, *context->params_list) {
                pindex++;
                if (equal(node, (Node*)lfirst(lc)))
                    break;
            }
            if (lc == NULL) {
                /* not in list, so add it */
                pindex++;
                *context->params_list = lappend(*context->params_list, node);
            }

            printRemoteParam(pindex, node->vartype, node->vartypmod, context);
        } else {
            printRemotePlaceholder(node->vartype, node->vartypmod, context);
        }
    }
}

/*
 * Deparse given constant value into context->buf.
 *
 * This function has to be kept in sync with ruleutils.c's get_const_expr.
 * As for that function, showtype can be -1 to never show "::typename" decoration,
 * or +1 to always show it, or 0 to show it only if the constant wouldn't be assumed
 * to be the right type by default.
 */
static void gcDeparseConst(Const* node, deparse_expr_cxt* context, int showtype)
{
    StringInfo buf = context->buf;
    Oid typoutput;
    bool typIsVarlena = false;
    char* extval = NULL;
    bool isfloat = false;
    bool needlabel = false;

    if (node->constisnull) {
        appendStringInfoString(buf, "NULL");
        if (showtype >= 0)
            appendStringInfo(buf, "::%s", deparse_type_name(node->consttype, node->consttypmod));
        return;
    }

    getTypeOutputInfo(node->consttype, &typoutput, &typIsVarlena);
    extval = OidOutputFunctionCall(typoutput, node->constvalue);

    switch (node->consttype) {
        case INT2OID:
        case INT4OID:
        case INT8OID:
        case OIDOID:
        case FLOAT4OID:
        case FLOAT8OID:
        case NUMERICOID: {
            /*
             * No need to quote unless it's a special value such as 'NaN'.
             * See comments in get_const_expr().
             */
            if (strspn(extval, "0123456789+-eE.") == strlen(extval)) {
                if (extval[0] == '+' || extval[0] == '-')
                    appendStringInfo(buf, "(%s)", extval);
                else
                    appendStringInfoString(buf, extval);
                if (strcspn(extval, "eE.") != strlen(extval))
                    isfloat = true; /* it looks like a float */
            } else
                appendStringInfo(buf, "'%s'", extval);
        } break;
        case BITOID:
        case VARBITOID:
            appendStringInfo(buf, "B'%s'", extval);
            break;
        case BOOLOID:
            if (strcmp(extval, "t") == 0)
                appendStringInfoString(buf, "true");
            else
                appendStringInfoString(buf, "false");
            break;
        default:
            gcDeparseStringLiteral(buf, extval);
            break;
    }

    pfree(extval);

    if (showtype < 0)
        return;

    /*
     * For showtype == 0, append ::typename unless the constant will be
     * implicitly typed as the right type when it is read in.
     *
     * XXX this code has to be kept in sync with the behavior of the parser,
     * especially make_const.
     */
    switch (node->consttype) {
        case BOOLOID:
        case INT4OID:
        case UNKNOWNOID:
            needlabel = false;
            break;
        case NUMERICOID:
            needlabel = !isfloat || (node->consttypmod >= 0);
            break;
        default:
            needlabel = true;
            break;
    }
    if (needlabel || showtype > 0)
        appendStringInfo(buf, "::%s", deparse_type_name(node->consttype, node->consttypmod));
}

/*
 * Deparse given Param node.
 *
 * If we're generating the query "for real", add the Param to
 * context->params_list if it's not already present, and then use its index
 * in that list as the remote parameter number.  During EXPLAIN, there's
 * no need to identify a parameter number.
 */
static void gcDeparseParam(Param* node, deparse_expr_cxt* context)
{
    if (context->params_list != NULL) {
        int pindex = 0;
        ListCell* lc = NULL;

        /* find its index in params_list */
        foreach (lc, *context->params_list) {
            pindex++;
            if (equal(node, (Node*)lfirst(lc)))
                break;
        }
        if (lc == NULL) {
            /* not in list, so add it */
            pindex++;
            *context->params_list = lappend(*context->params_list, node);
        }

        printRemoteParam(pindex, node->paramtype, node->paramtypmod, context);
    } else {
        printRemotePlaceholder(node->paramtype, node->paramtypmod, context);
    }
}

/*
 * Deparse an array subscript expression.
 */
static void gcDeparseArrayRef(ArrayRef* node, deparse_expr_cxt* context)
{
    StringInfo buf = context->buf;
    ListCell* lowlist_item = NULL;
    ListCell* uplist_item = NULL;

    /* Always parenthesize the expression. */
    appendStringInfoChar(buf, '(');

    /*
     * Deparse referenced array expression first.  If that expression includes
     * a cast, we have to parenthesize to prevent the array subscript from
     * being taken as typename decoration.  We can avoid that in the typical
     * case of subscripting a Var, but otherwise do it.
     */
    if (IsA(node->refexpr, Var))
        gcDeparseExpr(node->refexpr, context);
    else {
        appendStringInfoChar(buf, '(');
        gcDeparseExpr(node->refexpr, context);
        appendStringInfoChar(buf, ')');
    }

    /* Deparse subscript expressions. */
    lowlist_item = list_head(node->reflowerindexpr); /* could be NULL */
    foreach (uplist_item, node->refupperindexpr) {
        appendStringInfoChar(buf, '[');
        if (lowlist_item != NULL) {
            gcDeparseExpr((Expr*)lfirst(lowlist_item), context);
            appendStringInfoChar(buf, ':');
            lowlist_item = lnext(lowlist_item);
        }
        gcDeparseExpr((Expr*)lfirst(uplist_item), context);
        appendStringInfoChar(buf, ']');
    }

    appendStringInfoChar(buf, ')');
}

/*
 * Deparse a function call.
 */
static void gcDeparseFuncExpr(FuncExpr* node, deparse_expr_cxt* context)
{
    StringInfo buf = context->buf;
    bool use_variadic = false;
    bool first = true;
    ListCell* arg = NULL;

    /*
     * If the function call came from an implicit coercion, then just show the
     * first argument.
     */
    if (node->funcformat == COERCE_IMPLICIT_CAST && context->coorquery == false) {
        gcDeparseExpr((Expr*)linitial(node->args), context);
        return;
    }

    /*
     * If the function call came from a cast, then show the first argument
     * plus an explicit cast operation.
     */
    if (node->funcformat == COERCE_EXPLICIT_CAST ||
        (node->funcformat == COERCE_IMPLICIT_CAST && context->coorquery == true)) {
        Oid rettype = node->funcresulttype;
        int32 coercedTypmod = -1;

        /* Get the typmod if this is a length-coercion function */
        (void)exprIsLengthCoercion((Node*)node, &coercedTypmod);

        gcDeparseExpr((Expr*)linitial(node->args), context);
        appendStringInfo(buf, "::%s", deparse_type_name(rettype, coercedTypmod));
        return;
    }

    /* Check if need to print VARIADIC (cf. ruleutils.c) */
    use_variadic = node->funcvariadic;

    /*
     * Normal function: display as proname(args).
     */
    appendFunctionName(node->funcid, context);
    appendStringInfoChar(buf, '(');

    /* ... and all the arguments */
    foreach (arg, node->args) {
        if (!first)
            appendStringInfoString(buf, ", ");
        if (use_variadic && lnext(arg) == NULL)
            appendStringInfoString(buf, "VARIADIC ");
        gcDeparseExpr((Expr*)lfirst(arg), context);
        first = false;
    }
    appendStringInfoChar(buf, ')');
}

/*
 * Deparse given operator expression.   To avoid problems around
 * priority of operations, we always parenthesize the arguments.
 */
static void gcDeparseOpExpr(OpExpr* node, deparse_expr_cxt* context)
{
    StringInfo buf = context->buf;
    HeapTuple tuple = NULL;
    Form_pg_operator form = NULL;
    char oprkind;
    ListCell* arg = NULL;

    /* Retrieve information about the operator from system catalog. */
    tuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(node->opno));
    if (!HeapTupleIsValid(tuple))
        elog(ERROR, "cache lookup failed for operator %u", node->opno);
    form = (Form_pg_operator)GETSTRUCT(tuple);
    oprkind = form->oprkind;

    /* Sanity check. */
    Assert((oprkind == 'r' && list_length(node->args) == 1) || (oprkind == 'l' && list_length(node->args) == 1) ||
           (oprkind == 'b' && list_length(node->args) == 2));

    /* Always parenthesize the expression. */
    appendStringInfoChar(buf, '(');

    /* Deparse left operand. */
    if (oprkind == 'r' || oprkind == 'b') {
        arg = list_head(node->args);
        gcDeparseExpr((Expr*)lfirst(arg), context);
        appendStringInfoChar(buf, ' ');
    }

    /* Deparse operator name. */
    gcDeparseOperatorName(buf, form);

    /* Deparse right operand. */
    if (oprkind == 'l' || oprkind == 'b') {
        arg = list_tail(node->args);
        appendStringInfoChar(buf, ' ');
        gcDeparseExpr((Expr*)lfirst(arg), context);
    }

    appendStringInfoChar(buf, ')');

    ReleaseSysCache(tuple);
}

/*
 * Print the name of an operator.
 */
static void gcDeparseOperatorName(StringInfo buf, Form_pg_operator opform)
{
    char* opname = NULL;

    /* opname is not a SQL identifier, so we should not quote it. */
    opname = NameStr(opform->oprname);

    /* Print schema name only if it's not pg_catalog */
    if (opform->oprnamespace != PG_CATALOG_NAMESPACE) {
        const char* opnspname = NULL;

        opnspname = get_namespace_name(opform->oprnamespace);
        /* Print fully qualified operator name. */
        if (opnspname != NULL) {
            appendStringInfo(buf, "OPERATOR(%s.%s)", quote_identifier(opnspname), opname);
        } else {
            appendStringInfo(buf, "OPERATOR(\"Unknown\".%s)", opname);
        }
    } else {
        /* Just print operator name. */
        appendStringInfoString(buf, opname);
    }
}

/*
 * Deparse IS DISTINCT FROM.
 */
static void gcDeparseDistinctExpr(DistinctExpr* node, deparse_expr_cxt* context)
{
    StringInfo buf = context->buf;

    Assert(list_length(node->args) == 2);

    appendStringInfoChar(buf, '(');
    gcDeparseExpr((Expr*)linitial(node->args), context);
    appendStringInfoString(buf, " IS DISTINCT FROM ");
    gcDeparseExpr((Expr*)lsecond(node->args), context);
    appendStringInfoChar(buf, ')');
}

/*
 * Deparse given ScalarArrayOpExpr expression.  To avoid problems
 * around priority of operations, we always parenthesize the arguments.
 */
static void gcDeparseScalarArrayOpExpr(ScalarArrayOpExpr* node, deparse_expr_cxt* context)
{
    StringInfo buf = context->buf;
    HeapTuple tuple = NULL;
    Form_pg_operator form = NULL;
    Expr* arg1 = NULL;
    Expr* arg2 = NULL;

    /* Retrieve information about the operator from system catalog. */
    tuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(node->opno));
    if (!HeapTupleIsValid(tuple)) {
        elog(ERROR, "cache lookup failed for operator %u", node->opno);
    }
    form = (Form_pg_operator)GETSTRUCT(tuple);

    /* Sanity check. */
    Assert(list_length(node->args) == 2);

    /* Always parenthesize the expression. */
    appendStringInfoChar(buf, '(');

    /* Deparse left operand. */
    arg1 = (Expr*)linitial(node->args);
    gcDeparseExpr(arg1, context);
    appendStringInfoChar(buf, ' ');

    /* Deparse operator name plus decoration. */
    gcDeparseOperatorName(buf, form);
    appendStringInfo(buf, " %s (", node->useOr ? "ANY" : "ALL");

    /* Deparse right operand. */
    arg2 = (Expr*)lsecond(node->args);
    gcDeparseExpr(arg2, context);

    appendStringInfoChar(buf, ')');

    /* Always parenthesize the expression. */
    appendStringInfoChar(buf, ')');

    ReleaseSysCache(tuple);
}

/*
 * Deparse a RelabelType (binary-compatible cast) node.
 */
static void gcDeparseRelabelType(RelabelType* node, deparse_expr_cxt* context)
{
    gcDeparseExpr(node->arg, context);
    if (node->relabelformat != COERCE_IMPLICIT_CAST)
        appendStringInfo(context->buf, "::%s", deparse_type_name(node->resulttype, node->resulttypmod));
}

/*
 * Deparse a BoolExpr node.
 */
static void gcDeparseBoolExpr(BoolExpr* node, deparse_expr_cxt* context)
{
    StringInfo buf = context->buf;
    const char* op = NULL; /* keep compiler quiet */
    bool first = true;
    ListCell* lc = NULL;

    switch (node->boolop) {
        case AND_EXPR:
            op = "AND";
            break;
        case OR_EXPR:
            op = "OR";
            break;
        case NOT_EXPR:
            appendStringInfoString(buf, "(NOT ");
            gcDeparseExpr((Expr*)linitial(node->args), context);
            appendStringInfoChar(buf, ')');
            return;
    }

    appendStringInfoChar(buf, '(');
    foreach (lc, node->args) {
        if (!first)
            appendStringInfo(buf, " %s ", op);
        gcDeparseExpr((Expr*)lfirst(lc), context);
        first = false;
    }
    appendStringInfoChar(buf, ')');
}

/*
 * Deparse IS [NOT] NULL expression.
 */
static void gcDeparseNullTest(NullTest* node, deparse_expr_cxt* context)
{
    StringInfo buf = context->buf;

    appendStringInfoChar(buf, '(');
    gcDeparseExpr(node->arg, context);

    /*
     * For scalar inputs, we prefer to print as IS [NOT] NULL, which is
     * shorter and traditional.  If it's a rowtype input but we're applying a
     * scalar test, must print IS [NOT] DISTINCT FROM NULL to be semantically
     * correct.
     */
    if (node->argisrow || !type_is_rowtype(exprType((Node*)node->arg))) {
        if (node->nulltesttype == IS_NULL)
            appendStringInfoString(buf, " IS NULL)");
        else
            appendStringInfoString(buf, " IS NOT NULL)");
    } else {
        if (node->nulltesttype == IS_NULL)
            appendStringInfoString(buf, " IS NOT DISTINCT FROM NULL)");
        else
            appendStringInfoString(buf, " IS DISTINCT FROM NULL)");
    }
}

/*
 * Deparse ARRAY[...] construct.
 */
static void gcDeparseArrayExpr(ArrayExpr* node, deparse_expr_cxt* context)
{
    StringInfo buf = context->buf;
    bool first = true;
    ListCell* lc = NULL;

    appendStringInfoString(buf, "ARRAY[");
    foreach (lc, node->elements) {
        if (!first)
            appendStringInfoString(buf, ", ");
        gcDeparseExpr((Expr*)lfirst(lc), context);
        first = false;
    }
    appendStringInfoChar(buf, ']');

    /* If the array is empty, we need an explicit cast to the array type. */
    if (node->elements == NIL)
        appendStringInfo(buf, "::%s", deparse_type_name(node->array_typeid, -1));
}

/*
 * Append ORDER BY within aggregate function.
 */
static void appendAggOrderBy(List* orderList, List* targetList, deparse_expr_cxt* context)
{
    StringInfo buf = context->buf;
    ListCell* lc = NULL;
    bool first = true;

    foreach (lc, orderList) {
        SortGroupClause* srt = (SortGroupClause*)lfirst(lc);
        Node* sortexpr = NULL;
        Oid sortcoltype;
        TypeCacheEntry* typentry = NULL;

        if (!first)
            appendStringInfoString(buf, ", ");
        first = false;

        sortexpr = deparseSortGroupClause(srt->tleSortGroupRef, targetList, false, context);
        sortcoltype = exprType(sortexpr);
        /* See whether operator is default < or > for datatype */
        typentry = lookup_type_cache(sortcoltype, TYPECACHE_LT_OPR | TYPECACHE_GT_OPR);
        if (srt->sortop == typentry->lt_opr)
            appendStringInfoString(buf, " ASC");
        else if (srt->sortop == typentry->gt_opr)
            appendStringInfoString(buf, " DESC");
        else {
            HeapTuple opertup = NULL;
            Form_pg_operator operform = NULL;

            appendStringInfoString(buf, " USING ");

            /* Append operator name. */
            opertup = SearchSysCache1(OPEROID, ObjectIdGetDatum(srt->sortop));
            if (!HeapTupleIsValid(opertup)) {
                elog(ERROR, "cache lookup failed for operator %u", srt->sortop);
            }
            operform = (Form_pg_operator)GETSTRUCT(opertup);
            gcDeparseOperatorName(buf, operform);
            ReleaseSysCache(opertup);
        }

        if (srt->nulls_first)
            appendStringInfoString(buf, " NULLS FIRST");
        else
            appendStringInfoString(buf, " NULLS LAST");
    }
}

/*
 * Print the representation of a parameter to be sent to the remote side.
 *
 * Note: we always label the Param's type explicitly rather than relying on
 * transmitting a numeric type OID in PQexecParams().  This allows us to
 * avoid assuming that types have the same OIDs on the remote side as they
 * do locally --- they need only have the same names.
 */
static void printRemoteParam(int paramindex, Oid paramtype, int32 paramtypmod, deparse_expr_cxt* context)
{
    StringInfo buf = context->buf;
    char* ptypename = deparse_type_name(paramtype, paramtypmod);

    appendStringInfo(buf, "$%d::%s", paramindex, ptypename);
}

/*
 * Print the representation of a placeholder for a parameter that will be
 * sent to the remote side at execution time.
 *
 * This is used when we're just trying to EXPLAIN the remote query.
 * We don't have the actual value of the runtime parameter yet, and we don't
 * want the remote planner to generate a plan that depends on such a value
 * anyway.  Thus, we can't do something simple like "$1::paramtype".
 * Instead, we emit "((SELECT null::paramtype)::paramtype)".
 * In all extant versions of openGauss, the planner will see that as an unknown
 * constant value, which is what we want.  This might need adjustment if we
 * ever make the planner flatten scalar subqueries.  Note: the reason for the
 * apparently useless outer cast is to ensure that the representation as a
 * whole will be parsed as an a_expr and not a select_with_parens; the latter
 * would do the wrong thing in the context "x = ANY(...)".
 */
static void printRemotePlaceholder(Oid paramtype, int32 paramtypmod, deparse_expr_cxt* context)
{
    StringInfo buf = context->buf;
    char* ptypename = deparse_type_name(paramtype, paramtypmod);

    appendStringInfo(buf, "((SELECT null::%s)::%s)", ptypename, ptypename);
}

/*
 * Deparse ORDER BY clause according to the given pathkeys for given base
 * relation. From given pathkeys expressions belonging entirely to the given
 * base relation are obtained and deparsed.
 */
static void gcAppendOrderByClause(List* pathkeys, deparse_expr_cxt* context)
{
    ListCell* lcell = NULL;
    int nestlevel;
    char* delim = " ";
    RelOptInfo* baserel = context->scanrel;
    StringInfo buf = context->buf;

    /* Make sure any constants in the exprs are printed portably */
    nestlevel = set_transmission_modes();

    appendStringInfo(buf, " ORDER BY");
    foreach (lcell, pathkeys) {
        PathKey* pathkey = (PathKey*)lfirst(lcell);
        Expr* em_expr = NULL;

        em_expr = find_em_expr_for_rel(pathkey->pk_eclass, baserel);
        Assert(em_expr != NULL);

        appendStringInfoString(buf, delim);
        gcDeparseExpr(em_expr, context);
        if (pathkey->pk_strategy == BTLessStrategyNumber)
            appendStringInfoString(buf, " ASC");
        else
            appendStringInfoString(buf, " DESC");

        if (pathkey->pk_nulls_first)
            appendStringInfoString(buf, " NULLS FIRST");
        else
            appendStringInfoString(buf, " NULLS LAST");

        delim = ", ";
    }
    reset_transmission_modes(nestlevel);
}

/*
 * appendFunctionName
 *		Deparses function name from given function oid.
 */
static void appendFunctionName(Oid funcid, deparse_expr_cxt* context)
{
    StringInfo buf = context->buf;
    HeapTuple proctup = NULL;
    Form_pg_proc procform = NULL;
    const char* proname = NULL;

    proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
    if (!HeapTupleIsValid(proctup))
        elog(ERROR, "cache lookup failed for function %u", funcid);
    procform = (Form_pg_proc)GETSTRUCT(proctup);

    /* Print schema name only if it's not pg_catalog */
    if (procform->pronamespace != PG_CATALOG_NAMESPACE) {
        const char* schemaname = NULL;

        schemaname = get_namespace_name(procform->pronamespace);
        appendStringInfo(buf, "%s.", quote_identifier(schemaname));
    }

    /* Always print the function name */
    proname = NameStr(procform->proname);
    appendStringInfo(buf, "%s", quote_identifier(proname));

    ReleaseSysCache(proctup);
}

/*
 * Appends a sort or group clause.
 *
 * Like get_rule_sortgroupclause(), returns the expression tree, so caller
 * need not find it again.
 */
static Node* deparseSortGroupClause(Index ref, List* tlist, bool force_colno, deparse_expr_cxt* context)
{
    StringInfo buf = context->buf;
    TargetEntry* tle = NULL;
    Expr* expr = NULL;

    tle = get_sortgroupref_tle(ref, tlist);
    expr = tle->expr;

    if (force_colno) {
        /* Use column-number form when requested by caller. */
        Assert(!tle->resjunk);
        appendStringInfo(buf, "%d", tle->resno);
    } else if (expr && IsA(expr, Const)) {
        /*
         * Force a typecast here so that we don't emit something like "GROUP
         * BY 2", which will be misconstrued as a column position rather than
         * a constant.
         */
        gcDeparseConst((Const*)expr, context, 1);
    } else if ((expr == NULL) || IsA(expr, Var))
        gcDeparseExpr(expr, context);
    else {
        /* Always parenthesize the expression. */
        appendStringInfoString(buf, "(");
        gcDeparseExpr(expr, context);
        appendStringInfoString(buf, ")");
    }

    return (Node*)expr;
}

/*
 * Returns true if given Var is deparsed as a subquery output column, in
 * which case, *relno and *colno are set to the IDs for the relation and
 * column alias to the Var provided by the subquery.
 */
static bool is_subquery_var(Var* node, RelOptInfo* foreignrel, int* relno, int* colno)
{
    GcFdwRelationInfo* fpinfo = (GcFdwRelationInfo*)foreignrel->fdw_private;
    RelOptInfo* outerrel = fpinfo->outerrel;
    RelOptInfo* innerrel = fpinfo->innerrel;

    /* Should only be called in these cases. */
    Assert(IS_SIMPLE_REL(foreignrel) || IS_JOIN_REL(foreignrel));

    /*
     * If the given relation isn't a join relation, it doesn't have any lower
     * subqueries, so the Var isn't a subquery output column.
     */
    if (!IS_JOIN_REL(foreignrel))
        return false;

    /*
     * If the Var doesn't belong to any lower subqueries, it isn't a subquery
     * output column.
     */
    if (!bms_is_member(node->varno, fpinfo->lower_subquery_rels))
        return false;

    if (bms_is_member(node->varno, outerrel->relids)) {
        /*
         * If outer relation is deparsed as a subquery, the Var is an output
         * column of the subquery; get the IDs for the relation/column alias.
         */
        if (fpinfo->make_outerrel_subquery) {
            get_relation_column_alias_ids(node, outerrel, relno, colno);
            return true;
        }

        /* Otherwise, recurse into the outer relation. */
        return is_subquery_var(node, outerrel, relno, colno);
    } else {
        Assert(bms_is_member(node->varno, innerrel->relids));

        /*
         * If inner relation is deparsed as a subquery, the Var is an output
         * column of the subquery; get the IDs for the relation/column alias.
         */
        if (fpinfo->make_innerrel_subquery) {
            get_relation_column_alias_ids(node, innerrel, relno, colno);
            return true;
        }

        /* Otherwise, recurse into the inner relation. */
        return is_subquery_var(node, innerrel, relno, colno);
    }
}

/*
 * Get the IDs for the relation and column alias to given Var belonging to
 * given relation, which are returned into *relno and *colno.
 */
static void get_relation_column_alias_ids(Var* node, RelOptInfo* foreignrel, int* relno, int* colno)
{
    GcFdwRelationInfo* fpinfo = (GcFdwRelationInfo*)foreignrel->fdw_private;
    int i;
    ListCell* lc = NULL;

    /* Get the relation alias ID */
    *relno = fpinfo->relation_index;

    /* Get the column alias ID */
    i = 1;
    foreach (lc, foreignrel->reltarget->exprs) {
        if (equal(lfirst(lc), (Node*)node)) {
            *colno = i;
            return;
        }
        i++;
    }

    /* Shouldn't get here */
    elog(ERROR, "unexpected expression in subquery output");
}

/*
 * the string list of the targetlist of ForeignScan is used in explain command when agg
 * is deparsed to remote sql.
 */
List* get_str_targetlist(List* fdw_private)
{
    List* str_targetlist = (List*)list_nth(fdw_private, FdwScanPrivateStrTargetlist);

    List* rs = NIL;
    ListCell* lc = NULL;
    foreach (lc, str_targetlist) {
        Value* val = (Value*)lfirst(lc);
        rs = lappend(rs, val->val.str);
    }

    return rs;
}

/*
 * the transimition function is found in pg_aggregate.
 */
static char* getAggTransFn(Oid aggfnid)
{
    HeapTuple tuple = NULL;
    Form_pg_aggregate aggform = NULL;
    Form_pg_proc procform = NULL;

    Oid transfnid = InvalidOid;
    char* transfn = NULL;

    /* find transfn oid from pg_aggregate with aggfn oid */
    tuple = SearchSysCache(AGGFNOID, ObjectIdGetDatum(aggfnid), 0, 0, 0);
    if (!HeapTupleIsValid(tuple))
        elog(ERROR, "cache lookup failed for aggregate %u", aggfnid);

    aggform = (Form_pg_aggregate)GETSTRUCT(tuple);

    if (!OidIsValid(aggform->aggtransfn)) {
        ReleaseSysCache(tuple);
        return NULL;
    }
    transfnid = aggform->aggtransfn;

    ReleaseSysCache(tuple);

    /* find transfn name with transfn oid */
    tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(transfnid));
    if (!HeapTupleIsValid(tuple))
        elog(ERROR, "cache lookup failed for function %u", transfnid);

    procform = (Form_pg_proc)GETSTRUCT(tuple);

    transfn = (char*)pstrdup(NameStr(procform->proname));

    ReleaseSysCache(tuple);

    return transfn;
}

/*
 * get agg function name from pg_proc.
 */
static void deparseAggFunctionName(Oid funcid, deparse_expr_cxt* context)
{
    StringInfo buf = context->buf;
    HeapTuple proctup = NULL;
    Form_pg_proc procform = NULL;
    const char* proname = NULL;

    proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
    if (!HeapTupleIsValid(proctup)) {
        elog(ERROR, "cache lookup failed for function %u", funcid);
    }
    procform = (Form_pg_proc)GETSTRUCT(proctup);

    /* Print schema name only if it's not pg_catalog */
    if (procform->pronamespace != PG_CATALOG_NAMESPACE) {
        elog(ERROR, "can not support user-defined agg function: %u", funcid);
    }

    /* Always print the function name */
    proname = NameStr(procform->proname);
    appendStringInfo(buf, "%s", quote_identifier(proname));

    ReleaseSysCache(proctup);
}

/*
 * deparse Var node from targetlist from agg node.
 */
static void simpleDeparseVar(Var* node, deparse_expr_cxt* context)
{
    StringInfo buf = NULL;
    int varno;
    int varattno;

    PlannerInfo* root = context->root;

    if (OUTER_VAR == node->varno && 0 == node->varnoold) {
        List* fscan_targetlist = context->agg->lefttree->targetlist;
        if (node->varattno > list_length(fscan_targetlist)) {
            elog(ERROR, "varattno is out of range in ForeignScan node.");
        }

        TargetEntry* te = (TargetEntry*)list_nth(fscan_targetlist, node->varattno - 1);
        gcDeparseExpr(te->expr, context);
        return;
    }

    buf = context->buf;
    varno = node->varnoold;
    varattno = node->varoattno;

    if (varno <= 0 || varno >= root->simple_rel_array_size) {
        elog(ERROR, "invalid varno found.");
    }

    char* colname = NULL;

    /* Get RangeTblEntry from array in PlannerInfo. */
    RangeTblEntry* rte = planner_rt_fetch(varno, root);
    if (RTE_RELATION != rte->rtekind)
        elog(ERROR, "invalid relation type found.");

    colname = get_relid_attribute_name(rte->relid, varattno);

    appendStringInfoString(buf, quote_identifier(colname));
}

static deparse_expr_cxt* copyDeparseContext(deparse_expr_cxt* context)
{
    deparse_expr_cxt* new_context = (deparse_expr_cxt*)palloc0(sizeof(deparse_expr_cxt));

    new_context->root = context->root;
    new_context->foreignrel = context->foreignrel;
    new_context->scanrel = context->scanrel;
    new_context->buf = context->buf;
    new_context->params_list = context->params_list;
    new_context->coorquery = context->coorquery;
    new_context->agg = context->agg;
    new_context->str_targetlist = context->str_targetlist;
    new_context->agg_arg1 = context->agg_arg1;
    new_context->agg_arg2 = context->agg_arg2;
    new_context->local_schema = context->local_schema;

    return new_context;
}

static void deparseGroupByCol(Expr* expr, deparse_expr_cxt* context, bool addparenth)
{
    StringInfo buf = context->buf;

    if ((expr == NULL) || IsA(expr, Var))
        gcDeparseExpr(expr, context);
    else {
        /* Always parenthesize the expression. */
        if (addparenth)
            appendStringInfoString(buf, "(");
        gcDeparseExpr(expr, context);
        if (addparenth)
            appendStringInfoString(buf, ")");
    }
}

static void addGroupByColinAggTargetlist(deparse_expr_cxt* context)
{
    List* aggtlist = context->agg->targetlist;
    List* fstlist = context->agg->lefttree->targetlist;

    Agg* agg = (Agg*)context->agg;

    StringInfo buf = context->buf;

    for (int i = 0; i < list_length(aggtlist); i++) {
        /* check targetentry that is not in grpColIdx */
        TargetEntry* tle = (TargetEntry*)list_nth(aggtlist, i);
        Expr* expr = tle->expr;
        bool has_aggs = false;
        pgxc_is_expr_shippable((Expr*)tle, &has_aggs);

        if (has_aggs)
            continue;

        bool found = false;
        StringInfo expr1_str, expr2_str;
        for (int j = 0; j < agg->numCols; j++) {
            AttrNumber attr_idx = agg->grpColIdx[j];
            TargetEntry* fs_tle = (TargetEntry*)list_nth(fstlist, attr_idx - 1);
            Expr* fs_expr = fs_tle->expr;

            expr1_str = makeStringInfo();
            expr2_str = makeStringInfo();

            deparse_expr_cxt* cxt1 = NULL;
            deparse_expr_cxt* cxt2 = NULL;
            cxt1 = copyDeparseContext(context);
            cxt2 = copyDeparseContext(context);

            cxt1->buf = expr1_str;
            cxt2->buf = expr2_str;

            deparseGroupByCol(expr, cxt1, false);
            deparseGroupByCol(fs_expr, cxt2, false);

            if (!pg_strcasecmp(expr1_str->data, expr2_str->data)) {
                found = true;
                break;
            }
        }
        if (found)
            continue;

        /* add the targetentry to group by clause. */
        appendStringInfoString(buf, ", ");

        deparseGroupByCol(expr, context, true);
    }
}

/*
 * append "group by" to select statement.
 */
static void deparseGroupByClause(List* tlist, deparse_expr_cxt* context)
{
    StringInfo buf = context->buf;
    bool first = true;

    Agg* agg = (Agg*)context->agg;
    if (agg->numCols <= 0)
        return;

    appendStringInfo(buf, " GROUP BY ");

    List* fstargetlist = context->agg->lefttree->targetlist;
    for (int i = 0; i < agg->numCols; i++) {
        AttrNumber attr_idx = agg->grpColIdx[i];

        if (!first)
            appendStringInfoString(buf, ", ");
        first = false;

        if (attr_idx > list_length(fstargetlist))
            elog(ERROR, "invalid attr number in agg->grpColIdx");

        TargetEntry* tle = (TargetEntry*)list_nth(fstargetlist, attr_idx - 1);
        Expr* expr = tle->expr;

        deparseGroupByCol(expr, context, true);
    }

    addGroupByColinAggTargetlist(context);
}

/*
 * deparse agg express such as distinct, ...
 */
static void simpleDeparseAggExpr(Aggref* node, deparse_expr_cxt* context)
{
    StringInfo buf = context->buf;

    appendStringInfoChar(buf, '(');

    /* Add DISTINCT */
    appendStringInfo(buf, "%s", (node->aggdistinct != NIL) ? "DISTINCT " : "");

    /* aggstar can be set only in zero-argument aggregates */
    if (node->aggstar) {
        appendStringInfoChar(buf, '*');
    } else {
        ListCell* arg = NULL;
        bool first = true;
        int start;
        int i = 1;

        /* Add all the arguments */
        foreach (arg, node->args) {
            TargetEntry* tle = (TargetEntry*)lfirst(arg);
            Node* n = (Node*)tle->expr;

            if (tle->resjunk)
                continue;

            if (!first)
                appendStringInfoString(buf, ", ");
            first = false;

            start = buf->len;

            gcDeparseExpr((Expr*)n, context);

            if (1 == i)
                context->agg_arg1 = (char*)pstrdup(buf->data + start);
            if (2 == i)
                context->agg_arg2 = (char*)pstrdup(buf->data + start);
            ++i;
        }
    }

    /* Add ORDER BY */
    if (node->aggorder != NIL) {
        appendStringInfoString(buf, " ORDER BY ");

        appendAggOrderBy(node->aggorder, node->args, context);
    }

    appendStringInfoChar(buf, ')');
}

/*
 * deparse express for special agg function, such as regr_sxx, regr_syy, regr_sxy ...
 */
static char* deparseAggFor6ArrayResult(Oid aggfn, char* expr1, char* expr2)
{
    Assert(expr1);
    Assert(expr2);

    char* transfn = getAggTransFn(aggfn);
    if (NULL == transfn)
        elog(ERROR, "No function name found for agg func: %u", aggfn);

    StringInfo result = makeStringInfo();

    if (!pg_strcasecmp(transfn, "float8_regr_accum")) {
        appendStringInfo(result,
            "count((%s)+(%s)), sum((%s)::numeric), sum(((%s)::numeric)*((%s)::numeric)), ",
            expr1,
            expr2,
            expr1,
            expr1,
            expr1);
        appendStringInfo(result,
            "sum((%s)::numeric), sum(((%s)::numeric)*((%s)::numeric)), sum(((%s)::numeric)*((%s)::numeric))",
            expr2,
            expr2,
            expr2,
            expr1,
            expr2);
    } else {
        elog(ERROR, "unsupported transition function to deparse avg expr. funcname: %s", transfn);
    }

    pfree(transfn);

    return result->data;
}

/*
 * deparse express for special agg function, such as var_pop, variance, stddev...
 */
static char* deparseAggForTripleResult(Oid aggfn, char* expr)
{
    char* transfn = getAggTransFn(aggfn);
    if (transfn == NULL) {
        elog(ERROR, "No function name found for agg func: %u", aggfn);
    }

    StringInfo result = makeStringInfo();

    if (!pg_strcasecmp(transfn, "int8_accum") || !pg_strcasecmp(transfn, "int4_accum") ||
        !pg_strcasecmp(transfn, "int2_accum") || !pg_strcasecmp(transfn, "numeric_accum")) {
        appendStringInfo(
            result, "count(%s), sum((%s)::numeric), sum(((%s)::numeric)*((%s)::numeric))", expr, expr, expr, expr);
    } else if (!pg_strcasecmp(transfn, "float4_accum") || !pg_strcasecmp(transfn, "float8_accum")) {
        appendStringInfo(result, "count(%s), sum(%s), sum((%s)*(%s))", expr, expr, expr, expr);
    } else
        elog(ERROR, "unsupported transition function to deparse avg expr. funcname: %s", transfn);

    pfree(transfn);

    return result->data;
}

/*
 * deparse avg to real array[]
 */
static char* deparseAvg(Oid aggfn, char* expr, deparse_expr_cxt* context)
{
    char* transfn = getAggTransFn(aggfn);
    if (NULL == transfn)
        elog(ERROR, "No function name found for agg func: %u", aggfn);

    StringInfo result = makeStringInfo();

    if (!pg_strcasecmp(transfn, "int8_avg_accum") || !pg_strcasecmp(transfn, "int4_avg_accum") ||
        !pg_strcasecmp(transfn, "int2_avg_accum") || !pg_strcasecmp(transfn, "int1_avg_accum") ||
        !pg_strcasecmp(transfn, "numeric_avg_accum")) {
        appendStringInfo(result, "count(%s), sum((%s)::numeric)", expr, expr);
        context->map = 2;
    } else if (!pg_strcasecmp(transfn, "interval_accum")) {
        appendStringInfo(result, "count(%s), sum(%s)", expr, expr);
        context->map = 2;
    } else if (!pg_strcasecmp(transfn, "float4_accum") || !pg_strcasecmp(transfn, "float8_accum")) {
        appendStringInfo(result, "count(%s), sum(%s), sum((%s)*(%s))", expr, expr, expr, expr);
        context->map = 3;
    } else {
        elog(ERROR, "unsupported transition function to deparse avg expr. funcname: %s", transfn);
    }

    pfree(transfn);

    return result->data;
}

/*
 * entry to deparse aggref node
 */
static void simpleDeparseAggref(Aggref* node, deparse_expr_cxt* context)
{
    StringInfo buf = context->buf;

    context->agg_arg1 = NULL;
    context->agg_arg2 = NULL;

    int start = buf->len;

    /* get the string of expr in agg func */
    simpleDeparseAggExpr(node, context);

    char* expr = (char*)pstrdup(buf->data + start);

    buf->data[start] = '\0';
    buf->len = strlen(buf->data);

    /* Find aggregate name from aggfnoid which is a pg_proc entry */
    deparseAggFunctionName(node->aggfnoid, context);

    char* fname = (char*)pstrdup(buf->data + start);

    buf->data[start] = '\0';
    buf->len = strlen(buf->data);

    /* func name + (expr) */
    char* func_expr = NULL;
    if (!pg_strcasecmp("avg", fname)) {
        func_expr = deparseAvg(node->aggfnoid, expr, context);

        appendStringInfo(buf, "%s", func_expr);
    } else if (!pg_strcasecmp("var_pop", fname) || !pg_strcasecmp("var_samp", fname) ||
               !pg_strcasecmp("variance", fname) || !pg_strcasecmp("stddev_pop", fname) ||
               !pg_strcasecmp("stddev", fname) || !pg_strcasecmp("stddev_samp", fname)) {
        func_expr = deparseAggForTripleResult(node->aggfnoid, expr);
        context->map = 3;

        appendStringInfo(buf, "%s", func_expr);
    } else if (!pg_strcasecmp("regr_sxx", fname) || !pg_strcasecmp("regr_syy", fname) ||
               !pg_strcasecmp("regr_sxy", fname) || !pg_strcasecmp("regr_r2", fname) ||
               !pg_strcasecmp("regr_slope", fname) || !pg_strcasecmp("corr", fname) ||
               !pg_strcasecmp("covar_pop", fname) || !pg_strcasecmp("covar_samp", fname)) {
        func_expr = deparseAggFor6ArrayResult(node->aggfnoid, context->agg_arg1, context->agg_arg2);
        context->map = 6;

        appendStringInfo(buf, "%s", func_expr);
    } else if (!pg_strcasecmp("regr_avgx", fname) || !pg_strcasecmp("regr_avgy", fname) ||
               !pg_strcasecmp("regr_intercept", fname)) {
        func_expr = deparseAggFor6ArrayResult(node->aggfnoid, context->agg_arg2, context->agg_arg1);
        context->map = 6;

        appendStringInfo(buf, "%s", func_expr);
    } else {
        appendStringInfo(buf, "%s%s", fname, expr);
    }
}

/*
 * deparse the targetlist for agg node.
 */
static void deparseAggTargetList(List* tlist, deparse_expr_cxt* context)
{
    ListCell* lc = NULL;
    StringInfo buf = context->buf;
    int start;
    int i = 0;

    foreach (lc, tlist) {
        TargetEntry* tle = lfirst_node(TargetEntry, lc);

        context->map = 1;

        if (i > 0)
            appendStringInfoString(buf, ", ");
        i++;

        start = buf->len;

        gcDeparseExpr((Expr*)tle->expr, context);

        char* str_target = (char*)pstrdup(buf->data + start);

        Value* val = makeString(str_target);
        context->str_targetlist = lappend(context->str_targetlist, val);

        val = makeInteger(context->map);
        *context->colmap = lappend(*context->colmap, val);
    }
}

/*
 * the entry to deparse agg node to remote sql in foreignscan node.
 */
static void deparseSelectStmt(StringInfo buf, PlannerInfo* root, RelOptInfo* rel, List* remote_conds, List* paramlist,
    Plan* agg, List** str_targetlist, List** colmap, bool local_schema)
{
    List* quals = NIL;

    deparse_expr_cxt* context = (deparse_expr_cxt*)palloc0(sizeof(deparse_expr_cxt));

    /* Fill portions of context common to upper, join and base relation */
    context->buf = buf;
    context->root = root;
    context->foreignrel = rel;
    context->scanrel = rel;
    context->coorquery = true;
    context->agg = agg;
    context->agg_arg1 = NULL;
    context->agg_arg2 = NULL;
    context->params_list = &paramlist;
    context->colmap = colmap;
    context->local_schema = local_schema;

    /* Construct SELECT clause */
    appendStringInfoString(context->buf, "SELECT ");

    deparseAggTargetList(agg->targetlist, context);

    *str_targetlist = context->str_targetlist;

    /*
     * For upper relations, the WHERE clause is built from the remote
     * conditions of the underlying scan relation; otherwise, we can use the
     * supplied list of remote conditions directly.
     */
    quals = remote_conds;

    /* Construct FROM and WHERE clauses */
    gcDeparseFromExpr(quals, context);

    /* Append GROUP BY clause */
    deparseGroupByClause(agg->targetlist, context);
}

static bool test_remote_sql(const char* sql)
{
    if (NULL == sql)
        return false;

    List* query_string_locationlist = NIL;

    List* parsetree_list = pg_parse_query(sql, &query_string_locationlist);

    if (list_length(parsetree_list) != 0) {
        Node* parsetree = (Node*)list_nth(parsetree_list, 0);

        (void)pg_analyze_and_rewrite(parsetree, sql, NULL, 0);
    }

    return true;
}

/*
 * @Description: just for cooperation analysis on client cluster,
 *               try to deparse agg node to remote sql in ForeignScan node.
 *
 * @param[IN] agg :  current plan node
 * @param[IN] root :  PlannerInfo*
 * @return: Plan*: remote sql includes agg functions, or leave unchanged
 */
Plan* deparse_agg_node(Plan* agg, PlannerInfo* root)
{
    List* str_targetlist = NIL;
    List* colmap = NIL;

    ForeignScan* fscan = (ForeignScan*)agg->lefttree;

    if (fscan->scan.scanrelid == 0 || (int)fscan->scan.scanrelid >= root->simple_rel_array_size)
        return agg;

    RelOptInfo* scanrel = root->simple_rel_array[fscan->scan.scanrelid];
    if (NULL == scanrel->fdwroutine || NULL == scanrel->fdw_private)
        return agg;

    GcFdwRelationInfo* fpinfo = (GcFdwRelationInfo*)scanrel->fdw_private;
    if (fpinfo->reloid != fscan->scan_relid)
        return agg;

    List* remote_quals = (List*)list_nth(fscan->fdw_private, FdwScanPrivateRemoteQuals);
    List* param_list = (List*)list_nth(fscan->fdw_private, FdwScanPrivateParamList);

    /* deparse agg node to remote sql that includes agg functions. */
    StringInfo sql = makeStringInfo();
    deparseSelectStmt(sql, root, scanrel, remote_quals, param_list, agg, &str_targetlist, &colmap, false);

    Assert(list_length(agg->targetlist) == list_length(colmap));

    List* test_str_targetlist = NIL;
    List* test_colmap = NIL;
    StringInfo test_sql = makeStringInfo();
    deparseSelectStmt(test_sql, root, scanrel, remote_quals, param_list, agg, &test_str_targetlist, &test_colmap, true);

    if (NULL == param_list) {
        if (false == test_remote_sql(test_sql->data))
            return agg;
    }

    ereport(DEBUG1, (errmodule(MOD_COOP_ANALYZE), errmsg("remote agg sql: %s", sql->data)));

    /* get the real target list of ForeignScan node to match the output of the remote sql. */
    int i, j;
    List* aggResultTargetList = NIL;
    List* aggScanTargetList = NIL;
    ListCell* lc = NULL;

    for (i = 0, j = 0; i < list_length(agg->targetlist); i++) {
        TargetEntry* tle = (TargetEntry*)list_nth(agg->targetlist, i);

        /* the just type of Var is valid for the output of the remote sql, so varno set to 0 */
        Var* var = makeVarFromTargetEntry(0, tle);

        TargetEntry* newtle = makeTargetEntry((Expr*)var, i + 1, tle->resname, false);

        aggResultTargetList = lappend(aggResultTargetList, newtle);

        if (INT8ARRAYOID == var->vartype) {
            Var* itemvar = (Var*)copyObject(var);
            itemvar->vartype = INT8OID;
            itemvar->vartypmod = -1;

            TargetEntry* itemtle1 = makeTargetEntry((Expr*)itemvar, j++, tle->resname, false);
            TargetEntry* itemtle2 = makeTargetEntry((Expr*)itemvar, j++, tle->resname, false);

            aggScanTargetList = lappend(aggScanTargetList, itemtle1);
            aggScanTargetList = lappend(aggScanTargetList, itemtle2);
        } else if (FLOAT4ARRAYOID == var->vartype || FLOAT8ARRAYOID == var->vartype || NUMERICARRAY == var->vartype) {
            Var* itemvar = (Var*)copyObject(var);
            if (NUMERICARRAY == var->vartype)
                itemvar->vartype = NUMERICOID;
            else
                itemvar->vartype = FLOAT8OID;
            itemvar->vartypmod = -1;

            Value* val = (Value*)list_nth(colmap, i);
            long map = val->val.ival;
            for (long item = 0; item < map; item++) {
                TargetEntry* itemtle = makeTargetEntry((Expr*)itemvar, j++, tle->resname, false);
                aggScanTargetList = lappend(aggScanTargetList, itemtle);
            }
        } else if (ARRAYINTERVALOID == var->vartype)
            elog(ERROR, "unsupport data type in agg pushdown.");
        else {
            j++;
            aggScanTargetList = lappend(aggScanTargetList, newtle);
        }
    }

    /* reconstruct the fdw_private of ForeignScan node. */
    i = 0;
    List* newfdw_private = NIL;

    foreach (lc, fscan->fdw_private) {
        /* replace the remote sql with new one. */
        if (FdwScanPrivateSelectSql == i) {
            Value* val = makeString(sql->data);
            newfdw_private = lappend(newfdw_private, val);

            i++;
            continue;
        }

        /* add the string of targetlist of foreignscan for the output of explain cmd. */
        if (FdwScanPrivateStrTargetlist == i) {
            newfdw_private = lappend(newfdw_private, str_targetlist);
            i++;
            continue;
        }

        /* save the agg result targetlist in fdw_private */
        if (FdwScanPrivateAggResultTargetlist == i) {
            newfdw_private = lappend(newfdw_private, aggResultTargetList);
            i++;
            continue;
        }

        /* save the agg scan targetlist in fdw_private */
        if (FdwScanPrivateAggScanTargetlist == i) {
            newfdw_private = lappend(newfdw_private, aggScanTargetList);
            i++;
            continue;
        }

        /* save the agg scan targetlist in fdw_private */
        if (FdwScanPrivateAggColmap == i) {
            newfdw_private = lappend(newfdw_private, colmap);
            i++;
            continue;
        }

        newfdw_private = lappend(newfdw_private, lfirst(lc));
        i++;
    }

    fscan->fdw_private = newfdw_private;

    /* if success, set agg node to be dummy */
    Agg* aggplan = (Agg*)agg;
    aggplan->is_dummy = true;

    return agg;
}

// end of file
