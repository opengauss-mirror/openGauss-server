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
