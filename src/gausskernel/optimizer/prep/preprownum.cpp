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
#include "nodes/makefuncs.h"

#ifndef ENABLE_MULTIPLE_NODES
static Node* preprocess_rownum_opexpr(PlannerInfo* root, Query* parse, OpExpr* expr, bool isOrExpr);
static Node* process_rownum_boolexpr(PlannerInfo *root, Query* parse, BoolExpr* quals);
static Node* process_rownum_lt(Query *parse, OpExpr* qual, bool isOrExpr);
static Node* process_rownum_le(Query* parse, OpExpr* qual, bool isOrExpr);
static Node* process_rownum_eq(Query* parse, OpExpr* qual, bool isOrExpr);
static Node* process_rownum_gt(Query* parse, OpExpr* qual, bool isOrExpr);
static Node* process_rownum_ge(Query* parse, OpExpr* qual, bool isOrExpr);
static Node* process_rownum_ne(Query* parse, OpExpr* qual, bool isOrExpr);
static int64 extract_rownum_limit(OpExpr *expr);

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
    /* If it includes {order by} or {group by}, can not be rewrited */
    if ((parse->sortClause != NULL) || (parse->groupClause != NULL)) {
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
        consttype == INT2OID || consttype == INT1OID) {
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
            /* operator '<' */
            return process_rownum_lt(parse, expr, isOrExpr);

        case INT8LEOID:
        case INT84LEOID:
        case INT82LEOID:
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
            /* operator '>' */
            return process_rownum_gt(parse, expr, isOrExpr);

        case INT8GEOID:
        case INT84GEOID:
        case INT82GEOID:
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

/* extract const value from OpExpr like {rownum op Const} */
static int64 extract_rownum_limit(OpExpr *expr)
{
    Const* con = (Const *)llast(expr->args);
    Oid type = con->consttype;
    Datum value = con->constvalue;

    if (type == INT8OID) {
        return DatumGetInt64(value);
    } else if (type == INT4OID) {
        return (int64)DatumGetInt32(value);
    } else if (type == INT2OID) {
        return (int64)DatumGetInt16(value);
    } else if (type == INT1OID) {
        return (int64)DatumGetInt8(value);
    } else {
        ereport(ERROR,
            ((errmodule(MOD_OPT),
              errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
              errmsg("unsupported data type %u for ROWNUM limit", type))));
        return -1;
    }
}

/* process operator '<' in rownum expr like {rownum < 5}
 * if the OpExpr is rewritten, the original OpExpr can be
 * substituted by a bool constant expr. */
static Node* process_rownum_lt(Query *parse, OpExpr *rnxpr, bool isOrExpr)
{
    int64 limitValue = extract_rownum_limit(rnxpr);

    /* ROWNUM OpExpr in OrExpr */
    if (isOrExpr) {
        if (limitValue <= 1) {
            return makeBoolConst(false, false);
        }
        return (Node*)rnxpr;
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
    int64 limitValue = extract_rownum_limit(qual);

    /* ROWNUM OpExpr in OrExpr */
    if (isOrExpr) {
        if (limitValue < 1) {
            return makeBoolConst(false, false);
        }
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
    int64 limitValue = extract_rownum_limit(qual);

    /* ROWNUM OpExpr in OrExpr */
    if (isOrExpr) {
        if (limitValue < 1) {
            return makeBoolConst(false, false);
        }
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
    int64 limitValue = extract_rownum_limit(qual);

    if (limitValue < 1) {
        return makeBoolConst(true, false);
    }

    /* ROWNUM OpExpr in OrExpr */
    if (isOrExpr) {
        return (Node*)qual;
    }

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
    int64 limitValue = extract_rownum_limit(qual);

    if (limitValue <= 1) {
        return makeBoolConst(true, false);
    }

    /* ROWNUM OpExpr in OrExpr */
    if (isOrExpr) {
        return (Node*)qual;
    }

    /* ROWNUM OpExpr in AndExpr */
    rewrite_rownum_to_limit(parse, 0);
    return makeBoolConst(false, false);
}

/* process operator '!=' in rownum expr, e.g. rewrite {rownum != 5} to LIMIT 4
 * if it can be rewrited to LIMIT, return TRUE
 */
static Node* process_rownum_ne(Query* parse, OpExpr* qual, bool isOrExpr)
{
    int64 limitValue = extract_rownum_limit(qual);

    if (limitValue < 1) {
        return makeBoolConst(true, false);
    }

    /* ROWNUM OpExpr in OrExpr */
    if (isOrExpr) {
        return (Node*)qual;
    }

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
