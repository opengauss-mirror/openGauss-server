/* -------------------------------------------------------------------------
 *
 * expr_distinct.cpp
 *    utility functions for get number of distinct of expressions
 *
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2021. All rights reserved.
 *
 * IDENTIFICATION
 *    src/backend/utils/adt/expr_distinct.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include <ctype.h>
#include <math.h>

#include "access/transam.h"
#include "access/sysattr.h"
#include "catalog/pg_statistic.h"
#include "catalog/pg_type.h"
#include "catalog/pg_proc.h"
#include "nodes/makefuncs.h"
#include "nodes/nodes.h"
#include "optimizer/cost.h"
#include "optimizer/optimizerdebug.h"
#include "optimizer/pathnode.h"
#include "optimizer/planner.h"
#include "optimizer/prep.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/streamplan.h"
#include "optimizer/var.h"
#include "parser/analyze.h"
#include "parser/parse_clause.h"
#include "parser/parse_coerce.h"
#include "parser/parsetree.h"
#include "parser/parse_relation.h"
#include "storage/buf/bufmgr.h"
#include "storage/proc.h"
#include "utils/dynahash.h"
#include "utils/expr_distinct.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/selfuncs.h"
#include "utils/syscache.h"

/*
 * Context for estimate number of distinct of function
 */
typedef struct {
    double numDistinct[2]; /* numdistinct of the var */
    Oid attType;           /* attType of current values */
    int numMcv;            /* original number of MCV values in stats */
    int numHisBounds;      /* original number of histogram bounds in stats */
    /* following data may change during runtime */
    int numFMcv;       /* length of array mcvValues, may be changed at runtime */
    int numFHisBounds; /* length of array hisValues, simliar to mcvValues */
    Datum *fMcv;       /* "MCV" values or f(..(MCV)), except NULL */
    Datum *fHisBounds; /* "histogram bounds" or f(..g(histogram bounds)), except NULL */
    bool needAdjust;   /* whether var distinct need adjust */
} EstiFuncDistinctContext;

static char *GetFunctionNameWithDefault(Oid fnOid);
static void EstimateExprNumDistinct(PlannerInfo *root, VariableStatData *varData, Node *node, bool isJoinVar,
    bool *isDefault);
static void GetNumDistinctFuncExpr(PlannerInfo *root, VariableStatData *varData, FuncExpr *funcExpr, bool isJoinVar);
static void GetExprNumDistinctWalker(PlannerInfo *root, VariableStatData *varData, Node *node, bool isJoinVar);
static void TransferFunctionNumDistinct(PlannerInfo *root, VariableStatData *varData, FuncExpr *funcExpr,
    bool isJoinVar);
static bool CheckFuncArgsForTransferNumDistinct(FuncExpr *funcExpr, Node **varNode);

Oid g_typeCastFuncOids[] = {
    /* type cast from bool */
    BOOLTOINT1FUNCOID, BOOLTOINT2FUNCOID, BOOLTOINT4FUNCOID, BOOLTOINT8FUNCOID, BOOLTOTEXTFUNCOID,

    /* type cast from int1 */
    I1TOI2FUNCOID, I1TOI4FUNCOID, I1TOI8FUNCOID, I1TOF4FUNCOID, I1TOF8FUNCOID, INT1TOBPCHARFUNCOID,
    INT1TOVARCHARFUNCOID, INT1TONVARCHAR2FUNCOID, INT1TOTEXTFUNCOID, INT1TONUMERICFUNCOID, INT1TOINTERVALFUNCOID,

    /* type cast from int2 */
    I2TOI1FUNCOID, INT2TOINT4FUNCOID, INT2TOFLOAT4FUNCOID, INT2TOFLOAT8FUNCOID, INT2TOBPCHAR, INT2TOTEXTFUNCOID,
    INT2TOVARCHARFUNCOID, INT2TOINT8FUNCOID, INT2TONUMERICFUNCOID, INT2TOINTERVALFUNCOID,

    /* type cast from int4 */
    I4TOI1FUNCOID, INT4TOINT2FUNCOID, INT4TOINT8FUNCOID, INT4TOFLOAT8FUNCOID, INTEGER2CASHFUNCOID,
    INT4TONUMERICFUNCOID, INT4TOINTERVALFUNCOID, INT4TOHEXFUNCOID, INT4TOBPCHARFUNCOID, INT4TOTEXTFUNCOID,
    INT4TOVARCHARFUNCOID, INT4TOCHARFUNCOID, INT4TOCHRFUNCOID,

    /* type cast from int8 */
    I8TOI1FUNCOID, INT8TOINT2FUNCOID, INT8TOINT4FUNCOID, INT8TOBPCHARFUNCOID, INT8TOTEXTFUNCOID, INT8TOVARCHARFUNCOID,
    INT8TONUMERICFUNCOID, INT8TOHEXFUNCOID,

    /* type cast from float4/float8 */
    FLOAT4TOBPCHARFUNCOID, FLOAT4TOTEXTFUNCOID, FLOAT4TOVARCHARFUNCOID, FLOAT4TOFLOAT8FUNCOID,
    FLOAT4TONUMERICFUNCOID, FLOAT8TOBPCHARFUNCOID, FLOAT8TOINTERVALFUNCOID, FLOAT8TOTEXTFUNCOID,
    FLOAT8TOVARCHARFUNCOID, FLOAT8TONUMERICFUNCOID, FLOAT8TOTIMESTAMPFUNCOID,

    /* type cast from numeric */
    NUMERICTOBPCHARFUNCOID, NUMERICTOTEXTFUNCOID, NUMERICTOVARCHARFUNCOID,

    /* type cast from timestamp/date/time */
    DEFAULTFORMATTIMESTAMP2CHARFUNCOID, DEFAULTFORMATTIMESTAMPTZ2CHARFUNCOID, TIMESATMPTOTEXTFUNCOID,
    TIMESTAMPTOVARCHARFUNCOID, TIMESTAMP2TIMESTAMPTZFUNCOID, TIMESTAMPTZ2TIMESTAMPFUNCOID,
    DATETIMESTAMPTZFUNCOID, DATETOTIMESTAMPFUNCOID, DATETOBPCHARFUNCOID, DATETOVARCHARFUNCOID, DATETOTEXTFUNCOID,
    DATEANDTIMETOTIMESTAMPFUNCOID, DTAETIME2TIMESTAMPTZFUNCOID, TIMETOINTERVALFUNCOID, TIMESTAMPZONETOTEXTFUNCOID,
    TIME2TIMETZFUNCOID, RELTIMETOINTERVALFUNCOID,

    /* type cast from text */
    TODATEDEFAULTFUNCOID, TODATEFUNCOID, TOTIMESTAMPFUNCOID, TOTIMESTAMPDEFAULTFUNCOID,
    TEXTTOREGCLASSFUNCOID, TEXTTOINT1FUNCOID, TEXTTOINT2FUNCOID, TEXTTOINT4FUNCOID, TEXTTOINT8FUNCOID,
    TEXTTONUMERICFUNCOID, TEXTTOTIMESTAMP, TIMESTAMPTONEWTIMEZONEFUNCOID, TIMESTAMPTZTONEWTIMEZONEFUNCOID,
    HEXTORAWFUNCOID,

    /* type cast from char/varchar/bpchar */
    VARCHARTONUMERICFUNCOID, VARCHARTOINT4FUNCOID, VARCHARTOINT8FUNCOID, VARCHARTOTIMESTAMPFUNCOID,
    BPCHARTOINT4FUNCOID, BPCHARTOINT8FUNCOID, BPCHARTONUMERICFUNCOID, BPCHARTOTIMESTAMPFUNCOID,
    RTRIM1FUNCOID, BPCHARTEXTFUNCOID, CHARTOBPCHARFUNCOID, CHARTOTEXTFUNCOID
};

static char *GetFunctionNameWithDefault(Oid fnOid)
{
    char *funcName = get_func_name(fnOid);
    if (funcName == NULL) {
        funcName = "unknown-name";
    }
    return funcName;
}

/*
 * Entrance of the distinct estimation for expr whose stats is unavailable
 *
 * Handle expr-case in get_variable_numdistinct, aim to estimate variable distinct if no stats
 * (for the variable) are available. Also, GUC 'cost_model_version' controls the behaviour of
 * the router.
 *
 * Note:
 *   - function get_variable_numdistinct and get_variable_numdistinct_router should be called
 *     after examine_variable, otherwise, it may fail to get stats which actually exists.
 *   - This function may fail to estimate expr distinct, returns 0.0 (means unknown).
 *   - adjust ratio will be applied to the raw distinct if needAdjust is true, and
 *     varData->needAdjust is used to transfer the indicators.
 */
double GetExprNumDistinctRouter(VariableStatData *varData, bool needAdjust, STATS_EST_TYPE eType, bool isJoinVar)
{
    double distinct = 0.0;

    int idx = 0;
    bool saveNeedAdjust = varData->needAdjust;
    varData->needAdjust = needAdjust;

    ereport(DEBUG2, (errmodule(MOD_OPT), errmsg(
        "[Get Expr NumDistinct Router]: -------- <START>: adjust: %s --------", needAdjust ? "true" : "false")));
    if (IS_PGXC_COORDINATOR && (eType == STATS_TYPE_LOCAL)) {
        /* for local distinct */
        idx = 0;
    } else {
        /* for global distinct */
        idx = 1;
    }
    if (varData->numDistinct[idx] > 0.0) {
        distinct = clamp_row_est(varData->numDistinct[idx]);
    } else if (!varData->isEstimated) {
        GetExprNumDistinctWalker(varData->root, varData, varData->var, isJoinVar);
        varData->isEstimated = true;
        if (varData->numDistinct[idx] > 0.0) {
            distinct = clamp_row_est(varData->numDistinct[idx]);
        }
    }

    varData->needAdjust = saveNeedAdjust;
    ereport(DEBUG2, (errmodule(MOD_OPT),
        errmsg("[Get Expr NumDistinct Router]: -------- <END>: distinct: %.0lf --------", distinct)));

    return distinct;
}

/*
 * implementation of GetExprNumDistinctRouter with "cost_model_version = 1"
 *
 * NB: parameters node may be varData->var or other form or sub-node of varData->var
 */
static void GetExprNumDistinctWalker(PlannerInfo *root, VariableStatData *varData, Node *node, bool isJoinVar)
{
    char *exprName = NULL;

    errno_t rc =
        memset_s(varData->numDistinct, sizeof(varData->numDistinct[0]) * 2, 0, sizeof(varData->numDistinct[0]) * 2);
    securec_check(rc, "\0", "\0");

    switch (nodeTag(node)) {
        case T_FuncExpr:
            exprName = "FuncExpr";
            GetNumDistinctFuncExpr(root, varData, (FuncExpr *)node, isJoinVar);
            break;
        default:
            exprName = "Unknown-Expr";
            break;
    }

    ereport(DEBUG2, 
        (errmodule(MOD_OPT), 
            (errmsg("[%s Distinct Estimation]: local distinct is %.0lf, global distinct is %.0lf",
                exprName, varData->numDistinct[0], varData->numDistinct[1]))));
}

/*
 * Estimate the number of distinct of function expression.
 *
 * Parameters:
 * @in funcExpr: the function expression to calculate distinct value
 * @in isJoinVar: is the node used for join?
 *
 * @out: vardata: set the distinct value on numdistinct.
 */
static void GetNumDistinctFuncExpr(PlannerInfo *root, VariableStatData *varData, FuncExpr *funcExpr, bool isJoinVar)
{
    if (funcExpr->funcid >= FirstNormalObjectId) {
        /* user-defined function, do nothing */
        return;
    }

    /*
     * Let p denotes function can transfer distinct, f all other functions, w type-cast functions
     * which can also transfer distinct. In general, p contains w. Then all possible cases are:
     *   1. p(p(p(..p(var)))): p include w,
     *        transfer distinct of var to top level
     *   2. p(p(p(..p(f(var))))): p include w,
     *        transfer distinct of f to top level, distinct of f is estimated from var
     *   3. f(var):
     *        estimate distinct of f from var
     *   4. f(w(..w(var))
     *        estimate distinct of f from var
     *   5. ..f(..(p(..))): p except w
     *        can not estimate, return unknown
     *   6. ..f(..(f(..))):
     *        can not estimate, return unknown
     */

    /* bool-return function */
    if (get_func_rettype(funcExpr->funcid) == BOOLOID) {
        varData->numDistinct[0] = 2.0;
        varData->numDistinct[1] = 2.0;
        return;
    }

    if (funcExpr->args == NULL) {
        ereport(DEBUG2, 
            (errmodule(MOD_OPT),
                errmsg("[Func Distinct Estimation]:can not get number of distinct of function oid %u with no argument",
                    funcExpr->funcid)));
        return;
    }

    /* if failed to transfer, we can not estimate either */
    if (IsFunctionTransferNumDistinct(funcExpr)) {
        /* transfer the number of distinct */
        TransferFunctionNumDistinct(root, varData, funcExpr, isJoinVar);
    } else {
        /* estimate the number of distinct */
        return;
    }
}

/*
 * Estimate the number of distinct of the specifed node, work for get_num_distinct_coalesce,
 * get_num_distinct_casewhen and so on. The estimated numdistinct are always given, stored in
 * vardata->numdistinct[2].
 *
 * Parameters:
 * @in varData: the info we need to calculate distinct value, numdistinct is returned in varData
 * @in node: the node to calculate distinct value
 * @in isJoinVar: is the node used for join?
 * @out isDefault: is the numdistinct is a default value ?
 */
static void EstimateExprNumDistinct(PlannerInfo *root, VariableStatData *varData, Node *node, bool isJoinVar,
    bool *isDefault)
{
    VariableStatData tmpVarData;
    SpecialJoinInfo sjInfo;
    errno_t rc = EOK;

    varData->numDistinct[0] = 0.0;
    varData->numDistinct[1] = 0.0;

    /* return after count it if it's Const. */
    if (IsA(node, Const)) {
        varData->numDistinct[0] = 1.0;
        varData->numDistinct[1] = 1.0;
        *isDefault = false;
        return;
    }

    /* XXX should min_lefthand ? */
    rc = memset_s(&sjInfo, sizeof(sjInfo), 0, sizeof(sjInfo));
    securec_check(rc, "\0", "\0");
    sjInfo.min_lefthand = root->all_baserels;
    sjInfo.min_righthand = NULL;

    examine_variable(root, node, 0, &tmpVarData);
    tmpVarData.needAdjust = varData->needAdjust;
    double joinRatio = get_join_ratio(&tmpVarData, &sjInfo);
    varData->numDistinct[0] =
        get_variable_numdistinct(&tmpVarData, isDefault, tmpVarData.needAdjust, joinRatio, &sjInfo, 
            STATS_TYPE_LOCAL, isJoinVar);
    varData->numDistinct[1] =
        get_variable_numdistinct(&tmpVarData, isDefault, tmpVarData.needAdjust, joinRatio, &sjInfo, 
            STATS_TYPE_GLOBAL, isJoinVar);
    ReleaseVariableStats(tmpVarData);
}

/*
 * try to transfer number of distinct of given funcExpr, return unknown if fail
 */
static void TransferFunctionNumDistinct(PlannerInfo *root, VariableStatData *varData, FuncExpr *funcExpr,
    bool isJoinVar)
{
    bool isDefault = false;
    Node *varNode = NULL;

    /* check arguments and find the var-argument */
    if (!CheckFuncArgsForTransferNumDistinct(funcExpr, &varNode)) {
        return;
    }

    /* recursive to the arg, if there is no distinct for the arg, return unknown */
    EstimateExprNumDistinct(root, varData, varNode, isJoinVar, &isDefault);

    ereport(DEBUG2, 
        (errmodule(MOD_OPT),
            errmsg("[Func Distinct Estimation]: succeed to transfer number of distinct of function oid: %u",
                funcExpr->funcid)));
}

/*
 * check arguments of the func, return true and var-node if ok. Assume there is only one var or expr(var,..) in
 * its arguments. We allow f(var, const), f(const, var), f(expr(var), const) and f(expr(var1, var2), const)
 *
 * return true if ok, and varNode indicates the location of the Var
 */
static bool CheckFuncArgsForTransferNumDistinct(FuncExpr *funcExpr, Node **varNode)
{
    ListCell *listCell = NULL;
    int numVars = 0;
    Node *lastNode = NULL;

    /* find the first "var" */
    foreach (listCell, funcExpr->args) {
        Node *node = (Node *)lfirst(listCell);
        if (IsA(node, Var)) {
            numVars++;
            lastNode = node;
        } else if (IsA(node, Const)) {
            continue;
        } else {
            List *varList =
                pull_var_clause(node, PVC_RECURSE_AGGREGATES, PVC_RECURSE_PLACEHOLDERS, PVC_RECURSE_SPECIAL_EXPR);
            if (varList != NULL) {
                numVars++;
                lastNode = node;
            }
        }
    }

    if (numVars == 0) {
        *varNode = (Node *)linitial(funcExpr->args);
        return true;
    } else if (numVars == 1) {
        *varNode = lastNode;
        return true;
    } else {
        *varNode = NULL;
        ereport(DEBUG2, (errmodule(MOD_OPT),
            errmsg("[Check Args for Transfer]: more than one var-arguments for function oid: %u", 
                funcExpr->funcid)));
        return false;
    }
}

/*
 * check if the function can transfer number of distinct from one of its parameters
 */
bool IsFunctionTransferNumDistinct(FuncExpr *funcExpr)
{
    /*
     * We explicitly allow or disallow functions to transfer number of distinct.
     *
     * If a function called in form of COERCE_EXPLICIT_CAST/COERCE_IMPLICIT_CAST, we can not
     * conclude that this function can transfer number of distinct, e.g.
     *
     *    create table t1(price text);
     *    values: '20.01',
     *            '20.02',
     *            '20.12',
     *            '20.30',
     *            '20.99',
     *            '20.88'
     *    select price from t1 where int1(price) = 20;
     *    The filter transformed by Optimizer is:
     *        (((numeric_in(textout(price), 0::oid, (-1)))::tinyint)::bigint = 20)
     *
     * numeric::tinyint is the function numeric_int1 which is called as COERCE_EXPLICIT_CAST,
     * and it will make rounding to ensure an integer is assigned to int1. Hence a explicitly
     * cast function may not transfer number of distinct.
     */

    Oid funcId = funcExpr->funcid;
    char *funcName = GetFunctionNameWithDefault(funcExpr->funcid);

    /* special functions whose oid is not built-in, but we can transfer numdistinct for them */
    if (strcmp(funcName, "to_text") == 0 || strcmp(funcName, "to_varchar2") == 0 ||
        strcmp(funcName, "to_nvarchar2") == 0 || strcmp(funcName, "to_ts") == 0 ||
        (strcmp(funcName, "to_number") == 0 && (funcId != TONUMBERFUNCOID))) {
        pfree(funcName);
        return true;
    }

    for (int i = 0; i < (int)lengthof(g_typeCastFuncOids); i++) {
        if (g_typeCastFuncOids[i] == funcId) {
            pfree(funcName);
            return true;
        }
    }

    pfree(funcName);
    return false;
}