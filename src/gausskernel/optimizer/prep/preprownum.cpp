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
 * preprownum.cpp
 *     Planner preprocessing for ROWNUM
 *     The main function is to rewrite ROWNUM to LIMIT in parse tree if possible.
 *     For example,
 *         {select * from table_name where rownum < 5;}
 *     can be rewrited to
 *         {select * from table_name limit 4;}
 *
 * IDENTIFICATION
 *     src/gausskernel/optimizer/prep/preprownum.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "optimizer/prep.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_relation.h"
#include "nodes/makefuncs.h"
#include "utils/int8.h"

#ifndef ENABLE_MULTIPLE_NODES
static Node* preprocess_rownum_opexpr(PlannerInfo* root, Query* parse, OpExpr* expr, bool isOrExpr);
static Node* process_rownum_boolexpr(PlannerInfo* root, Query* parse, BoolExpr* quals);
static Node* process_rownum_lt(Query *parse, OpExpr* qual, bool isOrExpr);
static Node* process_rownum_le(Query* parse, OpExpr* qual, bool isOrExpr);
static Node* process_rownum_eq(Query* parse, OpExpr* qual, bool isOrExpr);
static Node* process_rownum_gt(Query* parse, OpExpr* qual, bool isOrExpr);
static Node* process_rownum_ge(Query* parse, OpExpr* qual, bool isOrExpr);
static Node* process_rownum_ne(Query* parse, OpExpr* qual, bool isOrExpr);
static bool try_extract_rownum_limit(OpExpr* expr, int64* retValue);
static bool try_extract_numeric_rownum(Oid type, Datum value, int64* retValue);

/*
 * preprocess_rownum
 * rewrite ROWNUM to LIMIT in parse tree if possible
 */
void preprocess_rownum(PlannerInfo *root, Query *parse)
{
    Node* quals = parse->jointree->quals;
    if (quals == NULL) {
        return;
    }
    /* If it includes {aggregation function} or {order by} or {group by} or {offset}, can not be rewrited */
    if (parse->hasAggs || (parse->sortClause != NULL) || (parse->groupClause != NULL) || (parse->limitOffset != NULL)) {
        return;
    }
    if (parse->limitCount != NULL) {
        parse->limitCount = eval_const_expressions(root, parse->limitCount);
        if (!IsA(parse->limitCount, Const)) {
            /* can not estimate */
            return;
        }
    }

    quals = (Node*)canonicalize_qual((Expr*)quals, false);
    switch (nodeTag(quals)) {
        case T_OpExpr: {
            quals = preprocess_rownum_opexpr(root, parse, (OpExpr*)quals, false);
            break;
        }
        case T_BoolExpr: {
            quals = process_rownum_boolexpr(root, parse, (BoolExpr*)quals);
            break;
        }
        default: {
            break;
        }
    }

    parse->jointree->quals = quals;
}

/*
 * ROWNUM carry-through (Oracle-compatible): materialize ROWNUM at the scan
 * and carry it through the outer ORDER BY / window as a plain column.
 *
 * A bare `rownum` in a SELECT list binds to the node that projects the final
 * targetlist. When that node sits above a Sort/WindowAgg, `rownum` counts the
 * post-reorder (output) arrival order instead of the base-scan order. To get
 * the base-scan (Oracle-style) ordinal carried through, wrap the FROM + WHERE
 * into an inner subquery that exposes `rownum` as a column; the inner subquery
 * has no sort/window/agg, so its ROWNUM binds to the inner scan, and the outer
 * Sort/WindowAgg carry the materialized value upward as ordinary data.
 *
 *   select rownum, row_number() over (order by amount desc) from t order by ...
 * becomes
 *   select q.__rn as rownum, row_number() over (order by q.amount desc)
 *     from (select rownum as __rn, <needed cols> from t [where ...]) q
 *   order by ...
 *
 * is_simple_subquery() refuses to flatten a subquery whose targetList contains
 * ROWNUM, so the inner subquery survives pull-up. The gate below is narrow on
 * purpose (bare rownum + ORDER BY or window; no plain agg / GROUP BY / DISTINCT
 * / setops / sublinks) to avoid disturbing the grouped-rownum and
 * aggregate-arg-rownum paths that already have their own handling.
 */
typedef struct RownumCarryVarMap {
    Index varno;
    AttrNumber varattno;
    AttrNumber resno;
} RownumCarryVarMap;

typedef struct RownumCarryCtx {
    List* varmap;        /* RownumCarryVarMap* list: outer var -> inner resno */
    AttrNumber rn_resno; /* resno of the materialized rownum column in the inner subquery */
} RownumCarryCtx;

typedef struct RownumCollectCtx {
    List* vars;          /* collected level-0 Vars (may contain duplicates) */
    bool bad;            /* set true if an unsupported var (correlated / whole-row) is seen */
} RownumCollectCtx;

/* Look up the inner-tlist resno mapped to (varno, varattno); NULL if absent. */
static RownumCarryVarMap* rownum_carry_find_map(List* varmap, Index varno, AttrNumber varattno)
{
    ListCell* lc = NULL;
    
    foreach (lc, varmap) {
        RownumCarryVarMap* m = (RownumCarryVarMap*)lfirst(lc);
        
        if (m->varno == varno && m->varattno == varattno) {
            return m;
        }
    }

    return NULL;
}

/* Collect every same-query-level (varlevelsup==0) Var reachable from `node`. */
static bool rownum_collect_walker(Node* node, void* context)
{
    RownumCollectCtx* ctx = (RownumCollectCtx*)context;
    
    if (node == NULL) {
        return false;
    }

    if (IsA(node, Var)) {
        Var* v = (Var*)node;
        
        /* We only carry same-query Vars. Correlated refs (varlevelsup>0) and
         * whole-row refs (varattno==0) need handling we deliberately skip. */
        if (v->varlevelsup != 0 || v->varattno == 0) {
            ctx->bad = true;
            return true;
        }

        ctx->vars = lappend(ctx->vars, v);
        
        return false;
    }

    return expression_tree_walker(node, (bool (*)())rownum_collect_walker, context);
}

/* Mutator: replace each bare Rownum with a Var over the materialized inner
 * column, and remap each same-query Var to its exposed position in the inner
 * subquery. Recurse into everything else (Aggref/WindowFunc args, etc.).
 */
static Node* rownum_carry_mutator(Node* node, void* context)
{
    RownumCarryCtx* ctx = (RownumCarryCtx*)context;

    if (node == NULL) {
        return NULL;
    }

    if (IsA(node, Rownum)) {
        return (Node*)makeVar(1, ctx->rn_resno, exprType(node), -1, InvalidOid, 0);
    }

    if (IsA(node, Var)) {
        Var* v = (Var*)node;

        if (v->varlevelsup == 0) {
            ListCell* lc = NULL;

            foreach (lc, ctx->varmap) {
                RownumCarryVarMap* m = (RownumCarryVarMap*)lfirst(lc);
                
                if (m->varno == v->varno && m->varattno == v->varattno) {
                    return (Node*)makeVar(1, m->resno, v->vartype, v->vartypmod, v->varcollid, 0);
                }
            }

            /* Not collected: shouldn't happen; leave an untouched copy. */
            return (Node*)copyObject(v);
        }

        return (Node*)copyObject(v);
    }

    return expression_tree_mutator(node, rownum_carry_mutator, context);
}

/* Gate: SELECT only, and only when a bare ROWNUM in the targetlist would
 * otherwise bind above a Sort/WindowAgg
 */
static bool is_rownum_carry_wrap(const Query* parse)
{
    if (parse->commandType != CMD_SELECT) {
        return false;
    }

    if (parse->setOperations != NULL || parse->cteList != NULL || parse->hasSubLinks ||
        parse->groupClause != NULL || parse->groupingSets != NULL || parse->distinctClause != NULL ||
        parse->hasAggs || parse->rowMarks != NULL) {
        return false;
    }

    if (!expression_contains_rownum((Node*)parse->targetList)) {
        return false;
    }

    if (parse->sortClause == NULL && !parse->hasWindowFuncs) {
        return false;
    }

    return true;
}

/* Collect the level-0 Vars the outer query still needs (targetlist + having +
 * limit). Window/GROUP/ORDER/DISTINCT key columns are present as (junk)
 * targetlist entries at this point, so the targetlist collection covers them.
 * Returns false if an unsupported var (correlated / whole-row) is seen.
 */
static bool rownum_carry_collect_vars(Query* parse, RownumCollectCtx* collect)
{
    collect->vars = NIL;
    collect->bad = false;

    rownum_collect_walker((Node*)parse->targetList, collect);
    
    if (collect->bad) {
        return false;
    }

    if (parse->havingQual != NULL) {
        rownum_collect_walker(parse->havingQual, collect);
        
        if (collect->bad) {
            return false;
        }
    }

    if (parse->limitOffset != NULL) {
        rownum_collect_walker(parse->limitOffset, collect);
        
        if (collect->bad) {
            return false;
        }
    }
    
    if (parse->limitCount != NULL) {
        rownum_collect_walker(parse->limitCount, collect);
          
        if (collect->bad) {
            return false;
        }
    }

    return true;
}

/* Dedup collected Vars by (varno, varattno);
 * expose each once in the inner
 * tlist and remember its position;
 * append the materialized __rn column.
 * Returns the built targetlist; *varmap_out
 * and *rn_resno_out are filled.
 */
static List* rownum_carry_build_inner_tlist(List* vars,
                                            List** varmap_out,
                                            AttrNumber* rn_resno_out)
{
    List* inner_tlist = NIL;
    List* varmap = NIL;
    ListCell* lc = NULL;
    AttrNumber resno = 1;
    int  rc = 0;
    char namebuf[32];

    foreach (lc, vars) {
        Var* v = (Var*)lfirst(lc);
          
        if (rownum_carry_find_map(varmap, v->varno, v->varattno) != NULL) {
            continue;
        }

        RownumCarryVarMap* m = (RownumCarryVarMap*)palloc0(sizeof(RownumCarryVarMap));
        m->varno = v->varno;
        m->varattno = v->varattno;
        m->resno = resno;
        varmap = lappend(varmap, m);

        rc = snprintf_s(namebuf, sizeof(namebuf), sizeof(namebuf) - 1, "col%d", resno);
        securec_check_ss(rc, "\0", "\0");

        TargetEntry* tle = makeTargetEntry((Expr*)copyObject(v), resno, pstrdup(namebuf), false);
        inner_tlist = lappend(inner_tlist, tle);
        resno++;
    }

    /* The materialized ROWNUM column,
     * appended after the carried columns.
     */
    Rownum* rn = makeNode(Rownum);
    rn->rownumcollid = InvalidOid;
    rn->location = -1;
    inner_tlist = lappend(inner_tlist, makeTargetEntry((Expr*)rn, resno, pstrdup("__rn"), false));

    *varmap_out = varmap;
    *rn_resno_out = resno;
    
    return inner_tlist;
}

/* Build the inner subquery: same FROM + WHERE, no sort/window/agg/group/
 * distinct/limit (so its ROWNUM binds to the inner scan).
 */
static Query* rownum_carry_build_inner(const Query* parse, List* inner_tlist)
{
    Query* inner = makeNode(Query);
    inner->commandType = CMD_SELECT;
    inner->rtable = (List*)copyObject(parse->rtable);
    inner->jointree = (FromExpr*)copyObject(parse->jointree);
    inner->canSetTag = parse->canSetTag;
    inner->can_push = parse->can_push;

    if (parse->sql_statement != NULL) {
        inner->sql_statement = pstrdup(parse->sql_statement);
    }

    inner->targetList = inner_tlist;
    
    return inner;
}

/* Remap the outer targetlist / having / limit onto the new subquery RTE:
 * bare Rownum -> Var(__rn), base Vars -> Var(colN).
 */
static void rownum_carry_remap_outer(Query* parse, List* varmap, AttrNumber rn_resno)
{
    RownumCarryCtx ctx;
    ctx.varmap = varmap;
    ctx.rn_resno = rn_resno;

    parse->targetList = (List*)expression_tree_mutator((Node*)parse->targetList, rownum_carry_mutator, &ctx);
      
    if (parse->havingQual != NULL) {
        parse->havingQual = expression_tree_mutator(parse->havingQual, rownum_carry_mutator, &ctx);
    }
    
    if (parse->limitOffset != NULL) {
        parse->limitOffset = expression_tree_mutator(parse->limitOffset, rownum_carry_mutator, &ctx);
    }
    
    if (parse->limitCount != NULL) {
        parse->limitCount = expression_tree_mutator(parse->limitCount, rownum_carry_mutator, &ctx);
    }
}

/* FROM becomes the subquery; WHERE has been moved into the inner subquery. */
static void rownum_carry_install_wrapper(Query* parse, RangeTblEntry* rte)
{
    RangeTblRef* rtr = makeNode(RangeTblRef);
      
    rtr->rtindex = 1;
    parse->rtable = list_make1(rte);
    parse->jointree = makeFromExpr(list_make1(rtr), NULL);
    parse->hasSubLinks = false; /* we bailed above if hasSubLinks was set */
}

void preprocess_rownum_carrythrough(PlannerInfo* root, Query* parse)
{
    RownumCollectCtx collect;
    List *varmap = NIL;
    List *inner_tlist = NIL;
    RangeTblEntry *rte = NULL;
    AttrNumber rn_resno = 0;
    Query* inner = NULL;

    if (!is_rownum_carry_wrap(parse)) {
        return;
    }

    if (!rownum_carry_collect_vars(parse, &collect)) {
        return;
    }

    inner_tlist = rownum_carry_build_inner_tlist(collect.vars, &varmap, &rn_resno);
    inner       = rownum_carry_build_inner(parse, inner_tlist);
    rte         = addRangeTableEntryForSubquery(NULL, inner, makeAlias("rownum_carry_inner", NIL), false, true);

    rownum_carry_remap_outer(parse, varmap, rn_resno);
    rownum_carry_install_wrapper(parse, rte);
}

static Node* process_rownum_boolexpr(PlannerInfo* root, Query* parse, BoolExpr* quals)
{
    ListCell *lc = NULL;

    if (quals->boolop == AND_EXPR) {
        foreach(lc, quals->args)
        {
            Node* clause = (Node*)lfirst(lc);
            if (!IsA(clause, OpExpr)) {
                continue;
            }

            clause = preprocess_rownum_opexpr(root, parse, (OpExpr*)clause, false);
            if (IsA(clause, Const) && !DatumGetBool(((Const*)clause)->constvalue)) {
                /* if FALSE constant in AND expr, directly return FALSE qual  */
                return clause;
            }
            lfirst(lc) = clause;
        }
    } else if (quals->boolop == OR_EXPR) {
        foreach(lc, quals->args)
        {
            Node* clause = (Node*)lfirst(lc);
            if (!IsA(clause, OpExpr)) {
                continue;
            }

            clause = preprocess_rownum_opexpr(root, parse, (OpExpr*)clause, true);
            if (IsA(clause, Const) && DatumGetBool(((Const*)clause)->constvalue)) {
                /* if TRUE constant in OR expr, directly return TRUE qual  */
                return clause;
            }
            lfirst(lc) = clause;
        }
    }

    return (Node*)quals;
}


static void rewrite_rownum_to_limit(Query *parse, int64 num)
{
    Const* limitCount = (Const*)parse->limitCount;
    Assert(limitCount == NULL || IsA(limitCount, Const));

    if (limitCount == NULL || limitCount->constisnull) {
        /* limitCount->constisnull indicates LIMIT ALL, ie, no limit */
        parse->limitCount =
            (Node*)makeConst(INT8OID, -1, InvalidOid, sizeof(int64), Int64GetDatum(num), false, true);
        return;
    }

    if (DatumGetInt64(limitCount->constvalue) > num) {
        limitCount->constvalue = Int64GetDatum(num);
    }
}

static bool is_optimizable_rownum_opexpr(PlannerInfo* root, OpExpr* expr)
{
    Node* leftArg = (Node*)linitial(expr->args);
    if (!IsA(leftArg, Rownum)) {
        return false;
    }

    Node* rightArg = (Node*)llast(expr->args);
    rightArg = eval_const_expressions(root, rightArg);

    if (!IsA(rightArg, Const)) {
        return false;
    }

    /* now, only constant integer types are supported to rewrite */
    Oid consttype = ((Const*)rightArg)->consttype;
    if (consttype == INT8OID || consttype == INT4OID ||
        consttype == INT2OID || consttype == INT1OID ||
        consttype == NUMERICOID) {
        return true;
    }

    return false;
}

static Node* preprocess_rownum_opexpr(PlannerInfo* root, Query* parse, OpExpr* expr, bool isOrExpr)
{
    /* Currently, only {ROWNUM op Const} can be optimizable */
    if (!is_optimizable_rownum_opexpr(root, expr)) {
        return (Node*)expr;
    }

    switch (expr->opno) {
        case INT8LTOID:
        case INT84LTOID:
        case INT82LTOID:
        case NUMERICLTOID:
            /* operator '<' */
            return process_rownum_lt(parse, expr, isOrExpr);

        case INT8LEOID:
        case INT84LEOID:
        case INT82LEOID:
        case NUMERICLEOID:
            /* operator '<=' */
            return process_rownum_le(parse, expr, isOrExpr);

        case INT8EQOID:
        case INT84EQOID:
        case INT82EQOID:
            /* operator '=' */
            return process_rownum_eq(parse, expr, isOrExpr);

        case INT8GTOID:
        case INT84GTOID:
        case INT82GTOID:
        case NUMERICGTOID:
            /* operator '>' */
            return process_rownum_gt(parse, expr, isOrExpr);

        case INT8GEOID:
        case INT84GEOID:
        case INT82GEOID:
        case NUMERICGEOID:
            /* operator '>=' */
            return process_rownum_ge(parse, expr, isOrExpr);

        case INT8NEOID:
        case INT84NEOID:
        case INT82NEOID:
            /* operator '!=' */
            return process_rownum_ne(parse, expr, isOrExpr);

        default:
            return (Node*)expr;
    }
}

/*
 * adapt process rownum to limit with different ops when rownum is numeric
 * Note: if someone change the behavior of process_rownum_ops, please adapt
 * this function, too!
 */
static bool try_extract_numeric_rownum(Oid type, Datum value, int64* retValue)
{
    Numeric ceilValue = DatumGetNumeric(DirectFunctionCall1(numeric_ceil, value));
    Numeric floorValue = DatumGetNumeric(DirectFunctionCall1(numeric_floor, value));
    NumericVar x;

    /*
     * '<' takes ceilValue and '>' takes floorValue because we need to keep
     * consistent to the process_rownum_ops functions.
     */
    switch (type) {
        case NUMERICLTOID:
        case NUMERICGEOID:
            {
                uint16 numFlags = NUMERIC_NB_FLAGBITS(ceilValue);
                if (NUMERIC_FLAG_IS_NANORBI(numFlags) && !NUMERIC_FLAG_IS_BI(numFlags)) {
                    return false;
                }
                init_var_from_num(ceilValue, &x);
                break;
            }
        case NUMERICLEOID:
        case NUMERICGTOID:
            {
                uint16 numFlags = NUMERIC_NB_FLAGBITS(floorValue);
                if (NUMERIC_FLAG_IS_NANORBI(numFlags) && !NUMERIC_FLAG_IS_BI(numFlags)) {
                    return false;
                }
                init_var_from_num(floorValue, &x);
                break;
            }
        default:
            ereport(ERROR, ((errmodule(MOD_OPT),
                errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("unsupported data type %u for ROWNUM limit", NUMERICOID))));
    }

    return numericvar_to_int64(&x, retValue);
}

/* extract const value from OpExpr like {rownum op Const} */
static bool try_extract_rownum_limit(OpExpr *expr, int64* retValue)
{
    bool canExtract = true;

    Const* con = (Const *)llast(expr->args);
    Oid type = con->consttype;
    Datum value = con->constvalue;

    if (type == NUMERICOID) {
        /*
         * convert numeric to int64
         * if value is larger than the range of int64, rownum will not be converted to limit.
         */
        canExtract = try_extract_numeric_rownum(expr->opno, value, retValue);
    } else if (type == INT8OID) {
        *retValue = DatumGetInt64(value);
    } else if (type == INT4OID) {
        *retValue = (int64)DatumGetInt32(value);
    } else if (type == INT2OID) {
        *retValue = (int64)DatumGetInt16(value);
    } else if (type == INT1OID) {
        *retValue = (int64)DatumGetInt8(value);
    } else {
        ereport(ERROR,
            ((errmodule(MOD_OPT),
              errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
              errmsg("unsupported data type %u for ROWNUM limit", type))));
    }

    return canExtract;
}

/* process operator '<' in rownum expr like {rownum < 5}
 * if the OpExpr is rewritten, the original OpExpr can be
 * substituted by a bool constant expr. */
static Node* process_rownum_lt(Query *parse, OpExpr *qual, bool isOrExpr)
{
    int64 limitValue = -1;
    if (!try_extract_rownum_limit(qual, &limitValue))
        return (Node*)qual;

    /* ROWNUM OpExpr in OrExpr */
    if (isOrExpr) {
        if (limitValue <= 1) {
            return makeBoolConst(false, false);
        }
        return (Node*)qual;
    }

    /* ROWNUM OpExpr in AndExpr */
    if (limitValue <= 1) {
        rewrite_rownum_to_limit(parse, 0);
        return makeBoolConst(false, false);
    } else {
        rewrite_rownum_to_limit(parse, limitValue - 1);
        return makeBoolConst(true, false);
    }
}

/* process operator '<=' in rownum expr like {rownum <= 5}
 * if the OpExpr is rewritten, the original OpExpr can be
 * substituted by a bool constant expr. */
static Node* process_rownum_le(Query* parse, OpExpr* qual, bool isOrExpr)
{
    int64 limitValue = -1;
    if (!try_extract_rownum_limit(qual, &limitValue))
        return (Node*)qual;

    /* ROWNUM OpExpr in OrExpr */
    if (isOrExpr) {
        if (limitValue < 1)
            return makeBoolConst(false, false);
        return (Node*)qual;
    }

    /* ROWNUM OpExpr in AndExpr */
    if (limitValue < 1) {
        rewrite_rownum_to_limit(parse, 0);
        return makeBoolConst(false, false);
    } else {
        rewrite_rownum_to_limit(parse, limitValue);
        return makeBoolConst(true, false);
    }
}

/* process operator '=' in rownum expr like {rownum = 5} */
static Node* process_rownum_eq(Query* parse, OpExpr* qual, bool isOrExpr)
{
    int64 limitValue = -1;
    if (!try_extract_rownum_limit(qual, &limitValue))
        return (Node*)qual;

    /* ROWNUM OpExpr in OrExpr */
    if (isOrExpr) {
        if (limitValue < 1)
            return makeBoolConst(false, false);
        return (Node*)qual;
    }

    /* ROWNUM OpExpr in AndExpr */
    if (limitValue == 1) {
        rewrite_rownum_to_limit(parse, 1);
        return makeBoolConst(true, false);
    } else {
        rewrite_rownum_to_limit(parse, 0);
        return makeBoolConst(false, false);
    }
}

/* process operator '>' in rownum expr like {rcoerceownum > 5} */
static Node* process_rownum_gt(Query* parse, OpExpr* qual, bool isOrExpr)
{
    int64 limitValue = -1;
    if (!try_extract_rownum_limit(qual, &limitValue))
        return (Node*)qual;

    if (limitValue < 1)
        return makeBoolConst(true, false);

    /* ROWNUM OpExpr in OrExpr */
    if (isOrExpr)
        return (Node*)qual;

    /* ROWNUM OpExpr in AndExpr,
     * here limitValue >= 1, so ROWNUM > limitValue is always false. */
    rewrite_rownum_to_limit(parse, 0);
    return makeBoolConst(false, false);
}

/* process operator '>=' in rownum expr like {rownum >= 5}
 * if the OpExpr is rewritten, the original OpExpr can be
 * substituted by a bool constant expr. */
static Node* process_rownum_ge(Query* parse, OpExpr* qual, bool isOrExpr)
{
    int64 limitValue = -1;
    if (!try_extract_rownum_limit(qual, &limitValue))
        return (Node*)qual;

    if (limitValue <= 1)
        return makeBoolConst(true, false);

    /* ROWNUM OpExpr in OrExpr */
    if (isOrExpr)
        return (Node*)qual;

    /* ROWNUM OpExpr in AndExpr */
    rewrite_rownum_to_limit(parse, 0);
    return makeBoolConst(false, false);
}

/* process operator '!=' in rownum expr, e.g. rewrite {rownum != 5} to LIMIT 4
 * if it can be rewrited to LIMIT, return TRUE
 */
static Node* process_rownum_ne(Query* parse, OpExpr* qual, bool isOrExpr)
{
    int64 limitValue = -1;
    if (!try_extract_rownum_limit(qual, &limitValue))
        return (Node*)qual;

    if (limitValue < 1)
        return makeBoolConst(true, false);

    /* ROWNUM OpExpr in OrExpr */
    if (isOrExpr)
        return (Node*)qual;

    /* ROWNUM OpExpr in AndExpr */
    if (limitValue == 1) {
        rewrite_rownum_to_limit(parse, 0);
        return makeBoolConst(false, false);
    } else {  /* for limitValue > 1 */
        rewrite_rownum_to_limit(parse, limitValue - 1);
        return makeBoolConst(true, false);
    }
}
#endif
