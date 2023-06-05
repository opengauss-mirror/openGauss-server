/* -------------------------------------------------------------------------
 * ndp_check.cpp
 *	  Routines to check whether to pushdown
 *
 * Portions Copyright (c) 2022 Huawei Technologies Co.,Ltd.
 *
 * IDENTIFICATION
 *	  contrib/ndpplugin/ndp_check.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "catalog/pg_operator.h"
#include "utils/builtins.h"
#include "ndp_check.h"

// operations not define in pg_operator.h
#define BPCHARLEOID 1059
#define BPCHARGEOID 1061
#define FLOAT48PLOID 1116
#define FLOAT48MIOID 1117
#define FLOAT48MULOID 1119
#define FLOAT48DIVOID 1118
#define FLOAT84PLOID 1126
#define FLOAT84MIOID 1127
#define FLOAT84MULOID 1129
#define FLOAT84DIVOID 1128
#define TEXTNEOID 531
#define TEXTLEOID 665
#define TEXTGEOID 667

// functions not define in pg_proc.h
#define INT4BOOLOID 2557
#define BOOLINT4OID 2558

const Oid g_ndp_support_data_type[] = {
    BOOLOID,
    INT1OID,
    INT2OID,
    INT4OID,
    INT8OID,
    FLOAT4OID,
    FLOAT8OID,
    VARCHAROID,
    TEXTOID,
    BPCHAROID,
    BPCHARARRAYOID,
    TIMESTAMPOID,
    NUMERICOID,
    INTERVALOID,
    ARRAYNUMERICOID,
    TEXTARRAYOID
};

inline static bool CheckNdpSupportDataType(Oid oid)
{
    for (int i = 0; i < (int)(sizeof(g_ndp_support_data_type) / sizeof(Oid)); ++i) {
        if (oid == g_ndp_support_data_type[i]) {
            return true;
        }
    }
    return false;
}

const Oid g_ndp_support_op_expr_type[] = {
    INT48EQOID,
    INT48NEOID,
    INT48LTOID,
    INT48GTOID,
    INT48LEOID,
    INT48GEOID,
    INT2EQOID,
    INT2LTOID,
    INT4EQOID,
    INT4LTOID,
    TEXTEQOID,
    INT8EQOID,
    INT8NEOID,
    INT8LTOID,
    INT8GTOID,
    INT8LEOID,
    INT8GEOID,
    INT84EQOID,
    INT84NEOID,
    INT84LTOID,
    INT84GTOID,
    INT84LEOID,
    INT84GEOID,
    INT4MULOID,
    INT4NEOID,
    INT2NEOID,
    INT2GTOID,
    INT4GTOID,
    INT2LEOID,
    INT4LEOID,
    INT2GEOID,
    INT4GEOID,
    INT2MULOID,
    INT2DIVOID,
    INT4DIVOID,
    INT24EQOID,
    INT42EQOID,
    INT24LTOID,
    INT42LTOID,
    INT24GTOID,
    INT42GTOID,
    INT24NEOID,
    INT42NEOID,
    INT24LEOID,
    INT42LEOID,
    INT24GEOID,
    INT42GEOID,
    INT24MULOID,
    INT42MULOID,
    INT24DIVOID,
    INT42DIVOID,
    INT2PLOID,
    INT4PLOID,
    INT24PLOID,
    INT42PLOID,
    INT2MIOID,
    INT4MIOID,
    INT24MIOID,
    INT42MIOID,
    FLOAT4EQOID,
    FLOAT4NEOID,
    FLOAT4LTOID,
    FLOAT4GTOID,
    FLOAT4LEOID,
    FLOAT4GEOID,
    TEXTLTOID,
    TEXTGTOID,
    FLOAT8EQOID,
    FLOAT8NEOID,
    FLOAT8LTOID,
    FLOAT8LEOID,
    FLOAT8GTOID,
    FLOAT8GEOID,
    INT8PLOID,
    INT8MIOID,
    INT8MULOID,
    INT8DIVOID,
    INT84PLOID,
    INT84MIOID,
    INT84MULOID,
    INT84DIVOID,
    INT48PLOID,
    INT48MIOID,
    INT48MULOID,
    INT48DIVOID,
    INT82PLOID,
    INT82MIOID,
    INT82MULOID,
    INT82DIVOID,
    INT28PLOID,
    INT28MIOID,
    INT28MULOID,
    INT28DIVOID,
    BPCHAREQOID,
    BPCHARNEOID,
    BPCHARLTOID,
    BPCHARGTOID,
    FLOAT48EQOID,
    FLOAT48NEOID,
    FLOAT48LTOID,
    FLOAT48GTOID,
    FLOAT48LEOID,
    FLOAT48GEOID,
    FLOAT84EQOID,
    FLOAT84NEOID,
    FLOAT84LTOID,
    FLOAT84GTOID,
    FLOAT84LEOID,
    FLOAT84GEOID,
    NUMERICEQOID,
    NUMERICNEOID,
    NUMERICLTOID,
    NUMERICLEOID,
    NUMERICGTOID,
    NUMERICGEOID,
    NUMERICADDOID,
    NUMERICSUBOID,
    NUMERICMULOID,
    NUMERICDIVOID,
    INT28EQOID,
    INT28NEOID,
    INT28LTOID,
    INT28GTOID,
    INT28LEOID,
    INT28GEOID,
    INT82EQOID,
    INT82NEOID,
    INT82LTOID,
    INT82GTOID,
    INT82LEOID,
    INT82GEOID,
    TIMESTAMPEQOID,
    TIMESTAMPNEOID,
    TIMESTAMPLTOID,
    TIMESTAMPLEOID,
    TIMESTAMPGTOID,
    TIMESTAMPGEOID,
    OID_TEXT_LIKE_OP,

    // operations not define in pg_operator.h
    BPCHARLEOID,
    BPCHARGEOID,
    FLOAT48PLOID,
    FLOAT48MIOID,
    FLOAT48MULOID,
    FLOAT48DIVOID,
    FLOAT84PLOID,
    FLOAT84MIOID,
    FLOAT84MULOID,
    FLOAT84DIVOID,
    TEXTNEOID,
    TEXTLEOID,
    TEXTGEOID,
};

inline static bool CheckNdpSupportOpExprType(Oid oid)
{
    for (int i = 0; i < (int)(sizeof(g_ndp_support_op_expr_type)/sizeof(Oid)); ++i) {
        if (oid == g_ndp_support_op_expr_type[i]) {
            return true;
        }
    }
    return false;
}

const Oid g_ndp_support_function_type[] = {
    INT4NUMERICFUNCOID,
    TIMESTAMPPARTFUNCOID,
    TEXTSUBSTRINGFUNCOID,
    FLOAT4TOFLOAT8FUNCOID,
    INT4TOFLOAT8FUNCOID,
    INT2TOFLOAT8FUNCOID,
    INT2TOFLOAT4FUNCOID,
    RTRIM1FUNCOID,

    // functions not define in pg_proc.h
    INT4BOOLOID,
    BOOLINT4OID,
};

inline static bool CheckNdpSupportFunctionType(Oid oid)
{
    for (int i = 0; i < (int)(sizeof(g_ndp_support_function_type)/sizeof(Oid)); ++i) {
        if (oid == g_ndp_support_function_type[i]) {
            return true;
        }
    }
    return false;
}

const Oid g_ndp_support_aggfunc_type[] = {
    2102, /* avg(int2) */
    2101, /* avg(int4) */
    2100, /* avg(int8) */
    2104, /* avg(float4) */
    2105, /* avg(float8) */
    2103, /* avg(numeric) */
    2106, /* avg(interval) */
    2109, /* sum(int2) */
    2108, /* sum(int4) */
    2107, /* sum(int8) */
    2110, /* sum(float4) */
    2111, /* sum(float8) */
    2114, /* sum(numeric) */
    2113, /* sum(interval) */
    2134, /* min(oid) */
    2118, /* max(oid) */
    2133, /* min(int2) */
    2117, /* max(int2) */
    2132, /* min(int4) */
    2116, /* max(int4) */
    2131, /* min(int8) */
    2115, /* max(int8) */
    2135, /* min(float4) */
    2136, /* min(float8) */
    2119, /* max(float4) */
    2120, /* max(float8) */
    2146, /* min(numeric) */
    2130, /* max(numeric) */
    2144, /* min(interval) */
    2128, /* max(interval) */
    2138, /* min(date) */
    2122, /* max(date) */
    2142, /* min(timestamp) */
    2126, /* max(timestamp) */
    2245, /* min(bpchar) */
    2244, /* max(bpchar) */
    2145, /* min(text) */
    2129, /* max(text) */
    2147, /* count(expr) */
    2803, /* count(*) */
};

inline static bool CheckNdpSupportAggFuncType(Oid oid)
{
    for (int i = 0; i < (int)(sizeof(g_ndp_support_aggfunc_type)/sizeof(Oid)); ++i) {
        if (oid == g_ndp_support_aggfunc_type[i]) {
            return true;
        }
    }
    return false;
}

static bool CheckNdpSupportVar(Var *var)
{
    if (!CheckNdpSupportDataType(var->vartype)) {
        return false;
    }

    if (var->vartype == NUMERICOID) {
        unsigned int typemod = (unsigned int)(var->vartypmod - VARHDRSZ);
        if (var->vartypmod != -1) {
            int precision = (typemod >> 16) & 0xffff;
            if (precision <= 0 || precision > 38) {
                return false;
            }
        }
    }
    return true;
}
static bool CheckNdpSupportParam(Param *param)
{
    if (param->paramkind != PARAM_EXTERN) {
        return false;
    }
    return CheckNdpSupportDataType(param->paramtype);
}

static bool CheckNdpSupportListType(const List* exprs);

static bool CheckNdpSupportNodeType(Node* node)
{
    if (!node) return true;

    switch (node->type) {
        case T_TargetEntry:
            return CheckNdpSupportNodeType(castNode(Node, (castNode(TargetEntry, node)->expr)));
        case T_Var:
            return CheckNdpSupportVar(castNode(Var, node));
        case T_Param:
            return CheckNdpSupportParam(castNode(Param, node));
        case T_OpExpr: {
            OpExpr* op = castNode(OpExpr, node);
            // check OpExpr::opfuncid in future
            if ((!CheckNdpSupportDataType(op->opresulttype)) || (!CheckNdpSupportOpExprType(op->opno))) {
                return false;
            }
            return CheckNdpSupportListType(op->args);
        } case T_Const:
            return CheckNdpSupportDataType(castNode(Const, node)->consttype);
        case T_RelabelType:
            return CheckNdpSupportDataType(castNode(RelabelType, node)->resulttype);
        case T_Aggref: {
            Aggref* agg = castNode(Aggref, node);
            if ((!CheckNdpSupportDataType(agg->aggtype)) || (!CheckNdpSupportAggFuncType(agg->aggfnoid))) {
                return false;
            }
            if ((!CheckNdpSupportListType(agg->aggorder)) || (!CheckNdpSupportListType(agg->aggdistinct))) {
                return false;
            }
            return CheckNdpSupportListType(agg->args);
        }
        case T_FuncExpr: {
            FuncExpr* func = castNode(FuncExpr, node);
            if ((!CheckNdpSupportDataType(func->funcresulttype)) || (!CheckNdpSupportFunctionType(func->funcid))) {
                return false;
            }
            return CheckNdpSupportListType(func->args);
        }
        case T_BoolExpr: {
            BoolExpr *boolExpr = castNode(BoolExpr, node);
            return CheckNdpSupportListType(boolExpr->args);
        }
        case T_CaseExpr: {
            CaseExpr *caseExpr = castNode(CaseExpr, node);
            if ((!CheckNdpSupportDataType(caseExpr->casetype)) ||
            (!CheckNdpSupportNodeType(castNode(Node, caseExpr->defresult)))) {
                return false;
            }
            return CheckNdpSupportListType(caseExpr->args);
        }
        case T_CaseWhen: {
            CaseWhen *caseWhen = castNode(CaseWhen, node);
            return CheckNdpSupportNodeType(castNode(Node, caseWhen->expr));
        }
        case T_CaseTestExpr: {
            CaseTestExpr *caseTestExpr = castNode(CaseTestExpr, node);
            return CheckNdpSupportDataType(caseTestExpr->typeId);
        }
        case T_ScalarArrayOpExpr: {
            ScalarArrayOpExpr *scalarArrayOpExpr = castNode(ScalarArrayOpExpr, node);
            if (!CheckNdpSupportOpExprType(scalarArrayOpExpr->opno)) {
                return false;
            }
            return CheckNdpSupportListType(scalarArrayOpExpr->args);
        }
        default:
            return false;
    }
}

static bool CheckNdpSupportListType(const List* exprs)
{
    if (!exprs) return true;

    foreach_cell (l, exprs) {
        Node* node = (Node*)lfirst(l);
        if (!CheckNdpSupportNodeType(node)) {
            return false;
        }
    }
    return true;
}

// check Plan, remove stmt if no use in future
Plan* CheckAndGetNdpPlan(PlannedStmt* stmt, SeqScan* scan, Plan* parent)
{
    Plan* node = nullptr;

    // 1. check scan, should check Scan::tableRows or Plan::plan_rows?
    if (scan->plan.exec_type != EXEC_ON_DATANODES
        || !CheckNdpSupportListType(scan->plan.targetlist)
        || !CheckNdpSupportListType(scan->plan.qual)
        || scan->isPartTbl) {
        return nullptr;
    }
    node = (Plan*)scan;

    // 2. check agg
    if (parent && IsA(parent, Agg)) {
        Agg* agg = (Agg*)parent;
        if (agg->aggstrategy != AGG_SORTED && // don't support distinct sort agg
            agg->groupingSets == nullptr && agg->chain == nullptr && // don't support grouping set
            CheckNdpSupportListType(parent->targetlist) &&
            CheckNdpSupportListType(parent->qual)) {
            node = parent;
        }
    }

    // 3. not push down if not agg and scan has no filter
    if (node == (Plan*)scan && scan->plan.qual == nullptr) {
        return nullptr;
    }

    // 4. if seqscan or agg is righttree of nestloop, do not push down
    if (parent && IsA(parent, NestLoop) && parent->righttree == node) {
        return nullptr;
    }

    // 5. if seqscan is under merge join, do not push down
    if (parent && IsA(parent, MergeJoin)) {
        return nullptr;
    }

    // 6. if seqscan is under limit, do not push down
    if (parent && IsA(parent, Limit)) {
        return nullptr;
    }

    return node;
}

static bool CheckNdpSupportHint(HintState* hint)
{
    return true;
}

static bool CheckNdpSupportXact(void)
{
    if (IsolationIsSerializable()) {
        return false;
    }

    if (!IsTransactionState() || IsSubTransaction()) {
        return false;
    }

    return true;
}

static bool CheckNdpPreloadSupport(const char* libraries)
{
    char* rawstring = NULL;
    List* elemlist = NULL;
    ListCell* l = NULL;

    if (libraries == NULL || libraries[0] == '\0') {
        return false; /* nothing to do */
    }

    /* Need a modifiable copy of string */
    rawstring = pstrdup(libraries);
    /* Parse string into list of identifiers */
    if (!SplitIdentifierString(rawstring, ',', &elemlist)) {
        /* syntax error in list */
        pfree(rawstring);
        list_free(elemlist);
        ereport(LOG, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("invalid syntax in parameter shared_preload_libraries")));
        return false;
    }

    foreach (l, elemlist) {
        char* tok = (char*)lfirst(l);
        char* filename = NULL;

        filename = pstrdup(tok);
        if (strcmp(filename, "ndpplugin") == 0) {
            pfree(filename);
            pfree(rawstring);
            list_free(elemlist);
            return true;
        } else {
            pfree(filename);
        }
    }

    pfree(rawstring);
    list_free(elemlist);
    ereport(LOG, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("ndpplugin is not preloaded")));
    return false;
}

// check Query/PlanedStmt/conf
bool CheckNdpSupport(Query* querytree, PlannedStmt *stmt)
{
    if (!u_sess->ndp_cxt.enable_ndp) {
        return false;
    }

    /* only plain relations are supported */
    if (!stmt || stmt->commandType != CMD_SELECT) {
        return false;
    }

    if (!CheckNdpSupportXact()) {
        return false;
    }

    if (!CheckNdpSupportHint(querytree->hintState)) {
        return false;
    }

    auto libraries = g_instance.attr.attr_common.shared_preload_libraries_string;
    if (stmt->num_streams > 0 && !CheckNdpPreloadSupport(libraries)) {
        return false;
    }
    return true;
}
