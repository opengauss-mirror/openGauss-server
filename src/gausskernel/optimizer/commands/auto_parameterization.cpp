/*
 * Copyright (c) 2020-2025 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * ---------------------------------------------------------------------------------------
 *
 * auto_parameterization.cpp
 * A module that turns a "simple query" into a parameterized query
 *
 * IDENTIFICATION
 * src/gausskernel/optimizer/commands/auto_parameterization.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"
#include "securec.h"
#include "nodes/nodes.h"
#include "nodes/nodeFuncs.h"
#include "nodes/execnodes.h"
#include "nodes/parsenodes_common.h"
#include "nodes/params.h"
#include "nodes/makefuncs.h"
#include "nodes/primnodes.h"
#include "parser/scanner.h"
#include "utils/int8.h"
#include "utils/elog.h"
#include "utils/plancache.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/portal.h"
#include "utils/snapmgr.h"
#include "utils/varbit.h"
#include "tcop/utility.h"
#include "tcop/dest.h"
#include "parser/analyze.h"
#include "parser/parse_expr.h"
#include "parser/parse_coerce.h"
#include "parser/parse_collate.h"
#include "parser/parse_node.h"
#include "rewrite/rewriteHandler.h"
#include "replication/libpqsw.h"
#include "executor/executor.h"
#include "opfusion/opfusion.h"
#include "opfusion/opfusion_util.h"
#include "optimizer/bucketpruning.h"
#include "access/printtup.h"
#include "commands/auto_parameterization.h"

typedef struct ParamCachedPlan {
    char parameterized_query[MAX_PARAM_QUERY_LEN];
    CachedPlanSource* psrc;
} ParamCachedPlan;

static inline int compLocation(const void* a, const void* b);
static void saveParamCachedPlan(CachedPlanSource* psrc);
static void insertIntoParameterizedHashTable(char* key, CachedPlanSource* psrc, bool* found);
static void initParameterizedQueryHashTable();
static void storeParamCachedPlan(char* key, CachedPlanSource* psrc);
static ParamCachedPlan* fetchCachedPlan(char* key);
static char* execParameterization(Node* parsetree, ParameterizationInfo* paramContext);
static void fillInConstantLengths(ParamState* pstate, const char* query);
static void fillParamState(ParamState* pstate, List* param_locs);
static bool parameterizeParsetree(Node* node, ParameterizationInfo* paramContext);
static bool canTurnParam(Node* node, Node* a_const, Node* param);
static char* generateNormalizedQuery(ParamState* pstate, const char* query, int* query_len_p, int encoding);
static ParamListInfo syncParams(Oid* paramTypes, int numParams, List* params, CachedPlanSource* psrc, const char* queryString);
static void dropAllParameterizedQueries(void);
static void dropFromQueryHashTable(const char* parameterizedQuery);
static void dropParamCachedPlan(CachedPlanSource* plansource);
static char* getStringOp(A_Expr* aExpr);
static bool isQualifiedOp(A_Expr* aExpr);
static bool isDoubleWhereNode(SelectStmt *stmt);


bool isQualifiedIuds(Node* parsetree)
{
    bool res = true;
    switch (nodeTag(parsetree)) {
        case T_InsertStmt: {
            InsertStmt* stmt = (InsertStmt*)parsetree;
            if (stmt->returningList != NIL || stmt->withClause != NULL ||
                stmt->upsertClause != NULL || stmt->hintState != NULL ||
                ((SelectStmt*)stmt->selectStmt)->valuesLists == NULL) {
                res = false;
            }
            break;
        }
        case T_DeleteStmt: {
            DeleteStmt* stmt = (DeleteStmt*)parsetree;
            if (stmt->whereClause == NULL || stmt->usingClause != NULL || stmt->returningList != NIL ||
                stmt->withClause != NULL || stmt->hintState != NULL ||
                stmt->sortClause != NULL || stmt->limitClause != NULL) {
                res = false;
            }
            break;
        }
        case T_UpdateStmt: {
            UpdateStmt* stmt = (UpdateStmt*)parsetree;
            if (stmt->returningList != NULL || stmt->withClause != NULL ||
                stmt->hintState != NULL || stmt->sortClause != NULL ||
                stmt->limitClause != NULL || stmt->hasIgnore) {
                res = false;
            }
            break;
        }
        case T_SelectStmt: {
            SelectStmt* stmt = (SelectStmt*)parsetree;
            if ((stmt->distinctClause == NULL && stmt->intoClause == NULL &&
            stmt->startWithClause == NULL && stmt->groupClause == NULL && stmt->havingClause == NULL &&
            stmt->windowClause == NULL && stmt->withClause == NULL && stmt->sortClause == NULL && stmt->lockingClause == NULL &&
            stmt->limitOffset == NULL && stmt->limitCount == NULL) && stmt->whereClause != NULL && isDoubleWhereNode(stmt)) {
                res = true;
            } else {
                res = false;
            }
            break;
        }
        default:
            res = false;
    }
    return res;
}

static bool isDoubleWhereNode(SelectStmt *stmt)
{
    A_Expr *whereClause = (A_Expr *)stmt->whereClause;
    if((whereClause->kind == AEXPR_OP && isQualifiedOp(whereClause))
    || (whereClause->kind == AEXPR_AND && whereClause->lexpr != NULL && isQualifiedOp((A_Expr*)whereClause->lexpr)
    && whereClause->lexpr != NULL && isQualifiedOp((A_Expr*)whereClause->lexpr))) {
        return true;
    }
    return false;
}

static bool isQualifiedOp(A_Expr* aExpr)
{
    char* op = getStringOp(aExpr);
    if(op == nullptr) {
        return false;
    }
    if(strcmp(op, "=") == 0 || strcmp(op, "<=") == 0 || strcmp(op, ">=") == 0 ||
    strcmp(op, "<") == 0 || strcmp(op, ">") == 0) {
        return true;
    }
    return false;
}

static char* getStringOp(A_Expr* aExpr)
{
    if(aExpr != nullptr && list_length(aExpr->name) == 1) {
        return ((Value*)linitial(aExpr->name))->val.str;
    }
    return nullptr;
}

bool isNodeSkipParam(Node* node)
{
    bool res = false;
    switch (nodeTag(node)) {
        case T_FuncCall:
            res = true;
            break;
        case T_SortGroupClause:
            res = true;
            break;
        case T_CollateClause:
            res = true;
            break;
        case T_CollateExpr:
            res = true;
            break;
        case T_TypeName:
            res = true;
            break;
        case T_TypeCast:
            res = true;
            break;
        case T_SortBy:
            res = true;
            break;
        case T_HintState:
        case T_OuterInnerRels:
        case T_JoinMethodHint:
        case T_LeadingHint:
        case T_RowsHint:
        case T_StreamHint:
        case T_BlockNameHint:
        case T_ScanMethodHint:
        case T_MultiNodeHint:
        case T_PredpushHint:
        case T_PredpushSameLevelHint:
        case T_SkewHint:
        case T_RewriteHint:
        case T_GatherHint:
        case T_SetHint:
        case T_PlanCacheHint:
        case T_NoExpandHint:
        case T_SqlIgnoreHint:
        case T_NoGPCHint:
            res = true;
            break;
        case T_NullTest:
            res = true;
            break;
        case T_SubLink:
            res = true;
            break;
        case T_CaseExpr:
            res = true;
            break;
        default:
            break;
    }
    return res;
}

/* A variant of function EvaluateParams() */
static ParamListInfo syncParams(Oid* paramTypes, int numParams, List* params, CachedPlanSource* psrc, const char* queryString)
{
    ListCell* lc = NULL;
    ParamListInfo paramListInfo;
    ParseState* pstate = NULL;
    int i;

    if (numParams == 0) {
        return NULL;
    }

    pstate = make_parsestate(NULL);
    pstate->p_sourcetext = queryString;

    i = 0;
    foreach(lc, params) {
        Node* expr = (Node*)lfirst(lc);
        Oid expectedTypeId = paramTypes[i];
        Oid givenTypeId;

        expr = transformExpr(pstate, expr, EXPR_KIND_EXECUTE_PARAMETER);

        /* Cannot contain subselects or aggregates */
        if (pstate->p_hasSubLinks)
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("can't use sublinks in parameterization")));
        if (pstate->p_hasAggs)
            ereport(ERROR, (errcode(ERRCODE_GROUPING_ERROR), errmsg("can't use agg function in parameterization")));
        if (pstate->p_hasWindowFuncs)
            ereport(ERROR, (errcode(ERRCODE_WINDOWING_ERROR), errmsg("can't use window function in parameterization")));

        givenTypeId = exprType(expr);
        expr = coerce_to_target_type(
            pstate, expr, givenTypeId, expectedTypeId, -1,
            COERCION_ASSIGNMENT, COERCE_IMPLICIT_CAST, -1);
        if (expr == NULL)
            ereport(ERROR,
                    (errcode(ERRCODE_DATATYPE_MISMATCH),
                            errmsg("parameter $%d of type %s cannot be coerced to the expected type %s",
                                   i + 1,
                                   format_type_be(givenTypeId),
                                   format_type_be(expectedTypeId)),
                            errhint("You will need to rewrite or cast the expression.")));

        /* Take care of collations in the finished expression. */
        assign_expr_collations(pstate, expr);

        lfirst(lc) = expr;
        i++;
    }
    if (pstate != NULL) {
        pfree(pstate);
    }
    /* Prepare the expressions for execution */
    paramListInfo = (ParamListInfo)palloc(offsetof(ParamListInfoData, params) + numParams * sizeof(ParamExternData));
    paramListInfo->paramFetch = NULL;
    paramListInfo->paramFetchArg = NULL;
    paramListInfo->parserSetup = NULL;
    paramListInfo->parserSetupArg = NULL;
    paramListInfo->params_need_process = false;
    paramListInfo->numParams = numParams;
    paramListInfo->uParamInfo = DEFUALT_INFO;
    paramListInfo->params_lazy_bind = false;

    i = 0;
    foreach (lc, params) {
        Const* e = (Const*)lfirst(lc);
        ParamExternData* prm = &paramListInfo->params[i];

        prm->ptype = paramTypes[i];
        prm->pflags = PARAM_FLAG_CONST;
        prm->value = e->constvalue;
        prm->isnull = e->constisnull;
        prm->tabInfo = NULL;
        i++;
    }
    return paramListInfo;
}

bool execQueryParameterization(Node* parsetree, const char* query_string, CommandDest cmdDest, char* completionTag)
{
    ParamListInfo paramListInfo = NULL;
    CachedPlanSource* psrc = NULL;
    Portal portal = NULL;
    Query* query =  NULL;
    CachedPlan* cplan = NULL;
    List* queryList = NIL;
    List* plan_list = NIL;
    int eflags = 0;
    long count = FETCH_ALL;
    Oid* argTypes = NULL;
    int nargs = 0;
    char* cachedPlanKey = NULL;
    bool fixed_result = true;
    DestReceiver* dest = NULL;

    /* As we can not guarantee that the whole process will succeed,
     * we need to assure that a raw parse tree
     * is still available for the upcoming process,
     * so we make a copy here to avoid modifying the original parse tree.
     */
    if (strlen(query_string) > MAX_PARAM_QUERY_LEN) {
        return false;
    }

    ParameterizationInfo* paramInfo = (ParameterizationInfo*)palloc0(sizeof(ParameterizationInfo));
    paramInfo->param_locs = NIL;
    paramInfo->param_types = NIL;
    paramInfo->params = NIL;
    paramInfo->parent_node = parsetree;
    paramInfo->query_string = query_string;
    paramInfo->is_skip = false;
    paramInfo->param_count = 0;

    char* parameterizedQuery = execParameterization(parsetree, paramInfo);
    nargs = paramInfo->param_count;

    if (parameterizedQuery == NULL) {
        return false;
    }

    if (nargs == 0) {
        return false;
    }

    int len = strlen(parameterizedQuery);
    cachedPlanKey = (char*)palloc(sizeof(char) * MAX_PARAM_QUERY_LEN);
    error_t rc = memcpy_s(cachedPlanKey, len + 1, parameterizedQuery, len);
    securec_check(rc, "\0", "\0");
    cachedPlanKey[len] = '\0';

    ParamCachedPlan* paramCachedPlan = fetchCachedPlan(cachedPlanKey);
    if (paramCachedPlan == NULL) {
        if (u_sess->param_cxt.param_cached_plan_count >= u_sess->attr.attr_sql.max_parameterized_query_stored) {
            dropAllParameterizedQueries();
            u_sess->param_cxt.param_cached_plan_count = 0;
        }

        int cnt = u_sess->param_cxt.param_cached_plan_count;

        /* if do not have saved ParamCachedPlan */
        psrc = CreateCachedPlan(parsetree,
                                query_string,
                                NULL,
                                CreateCommandTag(parsetree)
                                );

        nargs = paramInfo->param_count;
        if (nargs > 0) {
            argTypes = (Oid*)palloc(sizeof(Oid) * nargs);
            ListCell* lc;
            int i = 0;
            foreach(lc, paramInfo->param_types) {
                argTypes[i++] = lfirst_oid(lc);
            }
        }

        query = parse_analyze_varparams(parsetree, query_string, &argTypes, &nargs);

        /* check all parameter types were determined */
        for (int i = 0; i < nargs; i++) {
            Oid argType = argTypes[i];
            if (argType == InvalidOid || argType == UNKNOWNOID)
                ereport(ERROR, (errcode(ERRCODE_INDETERMINATE_DATATYPE), errmsg("could not determine data type of parameter $%d", i + 1)));
        }

        paramListInfo = syncParams(argTypes, nargs, paramInfo->params, psrc, query_string);

        queryList = QueryRewrite(query);

        CompleteCachedPlan(psrc,
                           queryList,
                           NULL,
                           argTypes,
                           NULL,
                           nargs,
                           NULL,
                           NULL,
                           0,
                           fixed_result,
                           "");

        storeParamCachedPlan(cachedPlanKey, psrc);
        u_sess->param_cxt.param_cached_plan_count++;
        goto SKIP;
    }

    psrc = paramCachedPlan->psrc;

    argTypes = psrc->param_types;
    nargs = list_length(paramInfo->params);
    for (int i = 0; i < nargs; i++) {
        Oid argType = argTypes[i];
        if (argType == InvalidOid || argType == UNKNOWNOID)
            ereport(ERROR, (errcode(ERRCODE_INDETERMINATE_DATATYPE), errmsg("could not determine data type of parameter $%d", i + 1)));
    }
    paramListInfo = syncParams(argTypes, nargs, paramInfo->params, psrc, query_string);

    /* Shouldn't find a non-fixed-result cached plan */
    if (!paramCachedPlan->psrc->fixed_result)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("EXECUTE does not support variable-result cached plans")));

    if (psrc->opFusionObj != NULL) {
        Assert(psrc->cplan == NULL);
        (void)RevalidateCachedQuery(psrc);
    }

    if (psrc->opFusionObj != NULL) {
        u_sess->param_cxt.use_parame = true;
        OpFusion *opFusionObj = (OpFusion *)(psrc->opFusionObj);
        opFusionObj->useOuterParameter(paramListInfo);
        opFusionObj->setCurrentOpFusionObj(opFusionObj);

        CachedPlanSource* cps = opFusionObj->m_global->m_psrc;
        bool needBucketId = cps != NULL && cps->gplan;
        if (needBucketId) {
            setCachedPlanBucketId(cps->gplan, paramListInfo);
        }

        if (OpFusion::process(FUSION_EXECUTE, NULL, completionTag, false, NULL)) {
            return true;
        }
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Bypass process Failed")));
    }

SKIP:
    u_sess->param_cxt.use_parame = true;
    if (ENABLE_CACHEDPLAN_MGR) {
        cplan = GetWiseCachedPlan(psrc, paramListInfo, false);
    } else {
        cplan = GetCachedPlan(psrc, paramListInfo, false);
    }
    plan_list = cplan->stmt_list;

    if (OpFusion::IsSqlBypass(psrc, plan_list)) {
        (void)MemoryContextSwitchTo(u_sess->cache_mem_cxt);
        dest = CreateDestReceiver(cmdDest);
        psrc->opFusionObj =
                OpFusion::FusionFactory(OpFusion::getFusionType(cplan, paramListInfo, NULL),
                                        u_sess->cache_mem_cxt, psrc, NULL, paramListInfo);
        psrc->is_checked_opfusion = true;
        if (psrc->opFusionObj != NULL) {
            ((OpFusion*)psrc->opFusionObj)->setPreparedDestReceiver(dest);
            ((OpFusion*)psrc->opFusionObj)->useOuterParameter(paramListInfo);
            ((OpFusion*)psrc->opFusionObj)->setCurrentOpFusionObj((OpFusion*)psrc->opFusionObj);

            if (OpFusion::process(FUSION_EXECUTE, NULL, completionTag, false, NULL)) {
                return true;
            }
            Assert(0);
        }
    }


    dest = CreateDestReceiver(cmdDest);
    portal = CreateNewPortal();
    portal->visible = false;
    if (cmdDest == DestRemote) {
        SetRemoteDestReceiverParams(dest, portal);
    }
    PortalDefineQuery(portal, NULL, query_string, psrc->commandTag, plan_list, cplan);


    portal->nextval_default_expr_type = psrc->nextval_default_expr_type;

    /*
    * Run the portal as appropriate.
    */
    PortalStart(portal, paramListInfo, eflags, GetActiveSnapshot());
    (void)PortalRun(portal, count, false, dest, dest, completionTag);

    PortalDrop(portal, false);
    if (paramListInfo != NULL) {
        pfree(paramListInfo);
    }
    return true;
}

void storeParamCachedPlan(char* key, CachedPlanSource* psrc)
{
    bool found = false;

    if (unlikely(!u_sess->param_cxt.parameterized_queries)) {
        initParameterizedQueryHashTable();
    }

    insertIntoParameterizedHashTable(key, psrc, &found);

    if (found) {
        ereport(ERROR, (errcode(ERRCODE_DUPLICATE_PSTATEMENT), errmsg("parameterized query \"%s\" already exists", key)));
    }

    saveParamCachedPlan(psrc);
    return;
}

/* A variant of SaveCachedPlan(), use param_cxt instead of pcache_cxt */
void saveParamCachedPlan(CachedPlanSource* psrc)
{
    /* Assert caller is doing things in a sane order */
    Assert(psrc != NULL && psrc->magic == CACHEDPLANSOURCE_MAGIC);
    Assert(psrc->is_complete);
    Assert(!psrc->is_saved);
    Assert(psrc->gpc.status.InShareTable() == false);
    /* This seems worth a real test, though */
    if (psrc->is_oneshot)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot save one-shot cached plan")));

    /*
     * In typical use, this function would be called before generating any
     * plans from the CachedPlanSource.  If there is a generic plan, moving it
     * into u_sess->cache_mem_cxt would be pretty risky since it's unclear
     * whether the caller has taken suitable care with making references
     * long-lived.	Best thing to do seems to be to discard the plan.
     */
    ReleaseGenericPlan(psrc);

    /*
     * Reparent the source memory context under u_sess->cache_mem_cxt so that it
     * will live indefinitely.	The query_context follows along since it's
     * already a child of the other one.
     */
    if (psrc->gpc.status.IsPrivatePlan()) {
        MemoryContextSetParent(psrc->context, u_sess->cache_mem_cxt);
    }

    START_CRIT_SECTION();
    ResourceOwnerForgetGMemContext(t_thrd.utils_cxt.TopTransactionResourceOwner, psrc->context);
    /*
     * Add the entry to the session's global list of cached plans.
     */
    psrc->next_saved = u_sess->param_cxt.first_saved_plan;
    u_sess->param_cxt.first_saved_plan = psrc;

    psrc->is_saved = true;
    END_CRIT_SECTION();

    return;
}

void insertIntoParameterizedHashTable(char* key, CachedPlanSource* psrc, bool* found)
{
    ParamCachedPlan* entry = NULL;
    entry = (ParamCachedPlan*)hash_search(u_sess->param_cxt.parameterized_queries, key, HASH_ENTER, found);
    if (!(*found)) {
        entry->psrc = psrc;
    }

    return;
}

void initParameterizedQueryHashTable()
{
    HASHCTL hash_ctl;
    errno_t rc = EOK;

    rc = memset_s(&hash_ctl, sizeof(hash_ctl), 0, sizeof(hash_ctl));
    securec_check(rc, "\0", "\0");

    hash_ctl.keysize = NAMEDATALEN;
    hash_ctl.entrysize = sizeof(ParamCachedPlan);
    hash_ctl.hcxt = u_sess->cache_mem_cxt;
    u_sess->param_cxt.parameterized_queries = hash_create("Parameterized Queries", 32,
                                                                &hash_ctl, HASH_ELEM | HASH_CONTEXT);
    Assert(u_sess->param_cxt.parameterized_queries);
    return;
}

char* execParameterization(Node* parsetree, ParameterizationInfo* paramInfo)
{
    paramInfo->parent_node = parsetree;
    paramInfo->param_count = 0;

    parameterizeParsetree(parsetree, paramInfo);
    if (paramInfo->param_count == 0 || paramInfo->is_skip) {
        return NULL;
    }

    int numOfLocations = list_length(paramInfo->param_locs);
    ParamState* pstate = (ParamState*)palloc(sizeof(ParamState));
    pstate->clocations_count = 0;
    pstate->clocations = (ParamLocationLen*)palloc(numOfLocations * sizeof(ParamLocationLen));

    Assert(pstate != NULL);
    Assert(paramInfo->param_locs != NIL);
    fillParamState(pstate, paramInfo->param_locs);

    const char* queryString = paramInfo->query_string;
    int encoding = GetDatabaseEncoding();
    int queryLen = strlen(queryString);

    char* parameterizedQuery = generateNormalizedQuery(pstate, queryString, &queryLen, encoding);
    char* res = (char*)palloc(sizeof(char) * (queryLen + 1));
    error_t rc = memcpy_s(res, queryLen + 1, parameterizedQuery, queryLen);
    securec_check(rc, "\0", "\0");
    res[queryLen] = '\0';

    return res;
}

ParamCachedPlan* fetchCachedPlan(char* key)
{
    if (u_sess->param_cxt.parameterized_queries == NULL) {
        return NULL;
    }

    ParamCachedPlan* entry = NULL;
    entry = (ParamCachedPlan*)hash_search(u_sess->param_cxt.parameterized_queries, key, HASH_FIND, NULL);
    if (entry == NULL) {
        ereport(LOG, (errcode(ERRCODE_UNDEFINED_PSTATEMENT), errmsg("ParamCachedPlan %s not found\n", key)));
    }
    return entry;
}

void fillParamState(ParamState* pstate, List* param_locs)
{
    ListCell* lc = NULL;
    int i = 0;
    foreach(lc, param_locs) {
        pstate->clocations[i++].location = lfirst_int(lc);
        pstate->clocations_count++;
    }
}

bool parameterizeParsetree(Node* node, ParameterizationInfo* paramInfo)
{
    if (node == NULL) {
        return false;
    }

    if (IsA(node, ParamRef)) {
        paramInfo->is_skip = true;
        return true;
    }

    Node* tempParentNode = paramInfo->parent_node;
    if (isNodeSkipParam(node)) {
        paramInfo->is_skip = true;
        return true;
    } else if (IsA(node, A_Const)) {
        A_Const* aconst = (A_Const*)node;
        int location = aconst->location;
        ParamRef* paramRef = makeNode(ParamRef);
        paramRef->location = location;
        if (canTurnParam(paramInfo->parent_node, node, (Node*)paramRef)) {
            Const* con = NULL;
            Oid typeOid = 0;
            con = make_const(NULL, &aconst->val, location);
            typeOid = con->consttype;
            paramRef->number = ++(paramInfo->param_count);
            paramInfo->param_locs = lappend_int(paramInfo->param_locs, paramRef->location);
            paramInfo->param_types = lappend_oid(paramInfo->param_types, typeOid);
            paramInfo->params = lappend(paramInfo->params, con);
        }
    } else {
        paramInfo->parent_node = node;
    }

    bool res = raw_expression_tree_walker(node, (bool (*)())parameterizeParsetree, paramInfo);
    paramInfo->parent_node = tempParentNode;
    return res;
}

static inline void tryTurnConstToParam(Node** node, Node* a_const, Node* param, bool* res)
{
    if (*node == a_const) {
        *node = param;
        *res = true;
    }
    return;
}

bool canTurnParam(Node* node, Node* a_const, Node* param)
{
    if (node == NULL) {
        return false;
    }
    bool res = false;
    switch(nodeTag(node)) {
        case T_Alias:
        case T_RangeVar:
        case T_Expr:
        case T_Var:
        case T_Const:
        case T_Param:
        case T_Aggref:
        case T_GroupingFunc:
        case T_WindowFunc:
        case T_ArrayRef:
        case T_FuncExpr:
        case T_NamedArgExpr:
        case T_OpExpr:
        case T_DistinctExpr:
        case T_NullIfExpr:
        case T_ScalarArrayOpExpr:
        case T_BoolExpr:
        case T_AlternativeSubPlan:
        case T_FieldSelect:
        case T_FieldStore:
        case T_RelabelType:
        case T_CoerceViaIO:
        case T_CaseTestExpr:
        case T_ArrayExpr:
        case T_RowExpr:
        case T_RowCompareExpr:
        case T_CoalesceExpr:
        case T_MinMaxExpr:
        case T_XmlExpr:
        case T_CoerceToDomainValue:
        case T_SetToDefault:
        case T_CurrentOfExpr:
        case T_ParamRef:
        case T_A_Star:
        case T_TypeName:
        case T_GroupingSet:
        case T_FuncWithArgs:
        case T_LockingClause:
        case T_WithClause:
        case T_StartWithInfo:
        case T_SqlLoadColPosInfo:
        case T_SqlLoadSequInfo:
        case T_SqlLoadFillerInfo:
        case T_SqlLoadConsInfo:
        case T_RenameCell:
        case T_SortBy:
        case T_CaseExpr:
            break;
        case T_A_Indices: {
            A_Indices *aIndices = (A_Indices *) node;
            tryTurnConstToParam(&aIndices->lidx, a_const, param, &res);
            tryTurnConstToParam(&aIndices->uidx, a_const, param, &res);
            break;
        }
        case T_ResTarget: {
            ResTarget *resTarget = (ResTarget *) node;
            tryTurnConstToParam(&resTarget->val, a_const, param, &res);
            break;
        }
        case T_A_Indirection: {
            A_Indirection *aIndirection = (A_Indirection *) node;
            tryTurnConstToParam(&aIndirection->arg, a_const, param, &res);
            break;
        }
        case T_WindowDef: {
            WindowDef *windowDef = (WindowDef *) node;
            tryTurnConstToParam(&windowDef->startOffset, a_const, param, &res);
            tryTurnConstToParam(&windowDef->endOffset, a_const, param, &res);
            break;
        }
        case T_RangeTableSample: {
            RangeTableSample *rangeTableSample = (RangeTableSample *) node;
            tryTurnConstToParam(&rangeTableSample->relation, a_const, param, &res);
            tryTurnConstToParam(&rangeTableSample->repeatable, a_const, param, &res);
            break;
        }
        case T_ColumnDef: {
            ColumnDef *columnDef = (ColumnDef *) node;
            tryTurnConstToParam(&columnDef->raw_default, a_const, param, &res);
            tryTurnConstToParam(&columnDef->cooked_default, a_const, param, &res);
            tryTurnConstToParam(&columnDef->update_default, a_const, param, &res);
            break;
        }
        case T_Constraint: {
            Constraint *constraint = (Constraint *) node;
            tryTurnConstToParam(&constraint->raw_expr, a_const, param, &res);
            tryTurnConstToParam(&constraint->where_clause, a_const, param, &res);
            tryTurnConstToParam(&constraint->update_expr, a_const, param, &res);
            break;
        }
        case T_DefElem: {
            DefElem *defElem = (DefElem *) node;
            tryTurnConstToParam(&defElem->arg, a_const, param, &res);
            break;
        }
        case T_TimeCapsuleClause: {
            TimeCapsuleClause *timeCapsuleClause = (TimeCapsuleClause *) node;
            tryTurnConstToParam(&timeCapsuleClause->tvver, a_const, param, &res);
            break;
        }
        case T_XmlSerialize: {
            XmlSerialize *xmlSerialize = (XmlSerialize *) node;
            tryTurnConstToParam(&xmlSerialize->expr, a_const, param, &res);
            break;
        }
        case T_SqlLoadScalarSpec: {
            SqlLoadScalarSpec *sqlLoadScalarSpec = (SqlLoadScalarSpec *) node;
            tryTurnConstToParam(&sqlLoadScalarSpec->position_info, a_const, param, &res);
            tryTurnConstToParam(&sqlLoadScalarSpec->sqlstr, a_const, param, &res);
            break;
        }
        case T_SqlLoadColExpr: {
            SqlLoadColExpr *sqlLoadColExpr = (SqlLoadColExpr *) node;
            tryTurnConstToParam(&sqlLoadColExpr->const_info, a_const, param, &res);
            tryTurnConstToParam(&sqlLoadColExpr->scalar_spec, a_const, param, &res);
            tryTurnConstToParam(&sqlLoadColExpr->sequence_info, a_const, param, &res);
            break;
        }
        case T_FunctionParameter: {
            FunctionParameter *functionParameter = (FunctionParameter *) node;
            tryTurnConstToParam(&functionParameter->defexpr, a_const, param, &res);
            break;
        }
        case T_SubLink: {
            SubLink *subLink = (SubLink *) node;
            tryTurnConstToParam(&subLink->subselect, a_const, param, &res);
            break;
        }
        case T_EstSPNode: {
            EstSPNode *estSpNode = (EstSPNode *) node;
            tryTurnConstToParam(&estSpNode->expr, a_const, param, &res);
        }
        break;
        case T_InsertStmt: {
            InsertStmt *insertStmt = (InsertStmt *) node;
            tryTurnConstToParam(&insertStmt->selectStmt, a_const, param, &res);
            break;
        }
        case T_A_Expr: {
            A_Expr *aExpr = (A_Expr *) node;
            tryTurnConstToParam(&aExpr->lexpr, a_const, param, &res);
            tryTurnConstToParam(&aExpr->rexpr, a_const, param, &res);
            break;
        }
        case T_List: {
            ListCell *lc = NULL;
            foreach(lc, (List*)node) {
                Node* tmp = (Node*)lfirst(lc);
                tryTurnConstToParam(&tmp, a_const, param, &res);
                lfirst(lc) = tmp;
            }
            break;
        }
        case T_SelectStmt: {
            SelectStmt *selectStmt = (SelectStmt *) node;
            tryTurnConstToParam(&selectStmt->limitOffset, a_const, param, &res);
            tryTurnConstToParam(&selectStmt->limitCount, a_const, param, &res);
            tryTurnConstToParam(&selectStmt->startWithClause, a_const, param, &res);
            tryTurnConstToParam(&selectStmt->whereClause, a_const, param, &res);
            tryTurnConstToParam(&selectStmt->havingClause, a_const, param, &res);
            break;
        }
        case T_DeleteStmt: {
            DeleteStmt *deleteStmt = (DeleteStmt *) node;
            tryTurnConstToParam(&deleteStmt->whereClause, a_const, param, &res);
            tryTurnConstToParam(&deleteStmt->limitClause, a_const, param, &res);
            break;
        }
        case T_UpdateStmt: {
            UpdateStmt *updateStmt = (UpdateStmt *) node;
            tryTurnConstToParam(&updateStmt->whereClause, a_const, param, &res);
            tryTurnConstToParam(&updateStmt->limitClause, a_const, param, &res);
            break;
        }
        default:
            ereport(ERROR, (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("Node type unrecognized: %d\n", nodeTag(node))));
    }
    return res;
}

char* generateNormalizedQuery(ParamState* pstate, const char* query, int* query_len_p, int encoding)
{
    char* norm_query = NULL;
    int query_len = *query_len_p;
    int max_output_len, i, rc;
    int len_to_wrt;       /* Length (in bytes) to write */
    int quer_loc = 0;     /* Source query byte location */
    int n_quer_loc = 0;   /* Normalized query byte location */
    int last_off = 0;     /* Offset from start for previous tok */
    int last_tok_len = 0; /* Length (in bytes) of that tok */

    /*
     * Get constants' lengths (core system only gives us locations).  Note
     * this also ensures the items are sorted by location.
     */
    fillInConstantLengths(pstate, query);

    /* Allocate result buffer, ensuring we limit result to allowed size */
    max_output_len = Min(query_len, g_instance.attr.attr_common.pgstat_track_activity_query_size - 1);
    norm_query = (char*)palloc0(max_output_len);

    for (i = 0; i < pstate->clocations_count; i++) {
        /*
         * off: Offset from start for cur tok
         * tok_len: Length (in bytes) of that tok
         */
        int off, tok_len;
        off = pstate->clocations[i].location;
        tok_len = pstate->clocations[i].length;

        if (tok_len < 0) {
            continue; /* ignore any duplicates */
        }
        /* Copy next chunk, or as much as will fit */
        len_to_wrt = off - last_off;
        len_to_wrt -= last_tok_len;
        len_to_wrt = Min(len_to_wrt, max_output_len - n_quer_loc);
        /* Should not happen, but for below SQL, Query struct and
         * query string can't be matched(location in Query is bigger
         * than query string)
         *  - delete from plan_table where statement_id='test statement_id',
         *    for sql 'delete plan_table', transformDeleteStmt method will
         *    modify Query member.
         */
        if (len_to_wrt <= 0) {
            break;
        }

        rc = memcpy_s(norm_query + n_quer_loc, max_output_len - n_quer_loc, query + quer_loc, len_to_wrt);
        securec_check(rc, "\0", "\0");
        n_quer_loc += len_to_wrt;

        if (n_quer_loc < max_output_len) {
            norm_query[n_quer_loc++] = '?';
        }

        quer_loc = off + tok_len;
        last_off = off;
        last_tok_len = tok_len;

        /* If we run out of space, might as well stop iterating */
        if (n_quer_loc >= max_output_len) {
            break;
        }
    }

    /*
     * We've copied up until the last ignorable constant.  Copy over the
     * remaining bytes of the original query string, or at least as much as
     * will fit.
     */
    len_to_wrt = query_len - quer_loc;
    len_to_wrt = Min(len_to_wrt, max_output_len - n_quer_loc);
    if (len_to_wrt > 0) {
        rc = memcpy_s(norm_query + n_quer_loc, max_output_len - n_quer_loc, query + quer_loc, len_to_wrt);
        securec_check(rc, "\0", "\0");
        n_quer_loc += len_to_wrt;
    }

    /*
     * If we ran out of space, we need to do an encoding-aware truncation,
     * just to make sure we don't have an incomplete character at the end.
     */
    if (n_quer_loc >= max_output_len) {
        query_len = pg_encoding_mbcliplen(
                encoding, norm_query, n_quer_loc,
                g_instance.attr.attr_common.pgstat_track_activity_query_size - 1);
    } else {
        query_len = n_quer_loc;
    }

    *query_len_p = query_len;
    return norm_query;
}

void fillInConstantLengths(ParamState* pstate, const char* query)
{
    ParamLocationLen* locs = NULL;
    core_yyscan_t yyscanner;
    core_yy_extra_type yyextra;
    core_YYSTYPE yylval;
    YYLTYPE yylloc;
    int last_loc = -1;
    int i;

    /*
     * Sort the records by location so that we can process them in order while
     * scanning the query text.
     */
    if (pstate->clocations_count > 1) {
        qsort(pstate->clocations, pstate->clocations_count, sizeof(ParamLocationLen), compLocation);
    }
    locs = pstate->clocations;

    /* initialize the flex scanner --- should match raw_parser() */
    yyscanner = scanner_init(query, &yyextra, &ScanKeywords, ScanKeywordTokens);

    void* coreYYlex = u_sess->hook_cxt.coreYYlexHook ? u_sess->hook_cxt.coreYYlexHook : (void*)core_yylex;
    /* Search for each constant, in sequence */
    for (i = 0; i < pstate->clocations_count; i++) {
        int loc = locs[i].location;
        int tok;

        Assert(loc >= 0);

        if (loc <= last_loc) {
            continue; /* Duplicate constant, ignore */
        }
        /* Lex tokens until we find the desired constant */
        for (;;) {
            tok = ((coreYYlexFunc)coreYYlex)(&yylval, &yylloc, yyscanner);
            /* We should not hit end-of-string, but if we do, behave sanely */
            if (tok == 0) {
                break; /* out of inner for-loop */
            }
            /*
             * We should find the token position exactly, but if we somehow
             * run past it, work with that.
             */
            if (yylloc >= loc) {
                if (query[loc] == '-') {
                    tok = ((coreYYlexFunc)coreYYlex)(&yylval, &yylloc, yyscanner);
                    if (tok == 0) {
                        break; /* out of inner for-loop */
                    }
                }

                /*
                 * We now rely on the assumption that flex has placed a zero
                 * byte after the text of the current token in scanbuf.
                 */
                locs[i].length = strlen(yyextra.scanbuf + loc);
                break; /* out of inner for-loop */
            }
        }

        /* If we hit end-of-string, give up, leaving remaining lengths -1 */
        if (tok == 0) {
            break;
        }

        last_loc = loc;
    }

    scanner_finish(yyscanner);
}

static inline int compLocation(const void* a, const void* b)
{
    int l = ((const ParamLocationLen*)a)->location;
    int r = ((const ParamLocationLen*)b)->location;

    if (l < r) {
        return -1;
    } else if (l > r) {
        return +1;
    } else {
        return 0;
    }
}

void dropAllParameterizedQueries(void)
{
    HASH_SEQ_STATUS seq;
    ParamCachedPlan *entry = NULL;
    ResourceOwner originalOwner = t_thrd.utils_cxt.CurrentResourceOwner;

    /* nothing cached */
    if (!u_sess->param_cxt.parameterized_queries) {
        return;
    }

#define ReleaseTempResourceOwner()                                                                               \
    do {                                                                                                         \
        ResourceOwnerRelease(t_thrd.utils_cxt.CurrentResourceOwner, RESOURCE_RELEASE_BEFORE_LOCKS, false, true); \
        ResourceOwnerRelease(t_thrd.utils_cxt.CurrentResourceOwner, RESOURCE_RELEASE_LOCKS, false, true);        \
        ResourceOwnerRelease(t_thrd.utils_cxt.CurrentResourceOwner, RESOURCE_RELEASE_AFTER_LOCKS, false, true);  \
        if (NULL == originalOwner && t_thrd.utils_cxt.CurrentResourceOwner) {                                    \
            ResourceOwner tempOwner = t_thrd.utils_cxt.CurrentResourceOwner;                                     \
            t_thrd.utils_cxt.CurrentResourceOwner = originalOwner;                                               \
            ResourceOwnerDelete(tempOwner);                                                                      \
        }                                                                                                        \
    } while (0);

    if (NULL == originalOwner) {
        /*
         * make sure ResourceOwner is not null, since it may acess catalog
         * when the pooler tries to create new connections
         */
        t_thrd.utils_cxt.CurrentResourceOwner = ResourceOwnerCreate(NULL, "dropAllParameterizedQueries",
                                                                    THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_OPTIMIZER));
    }

    bool failflag_dropcachedplan = false;
    ErrorData* edata = NULL;
    MemoryContext oldcontext = CurrentMemoryContext;
    bool isSharedPlan = false;

    /* walk over cache */
    hash_seq_init(&seq, u_sess->param_cxt.parameterized_queries);
    while ((entry = (ParamCachedPlan*)hash_seq_search(&seq)) != NULL) {
        PG_TRY();
        {
            /* Release the plancache entry */
            Assert (entry->psrc->magic == CACHEDPLANSOURCE_MAGIC);
            dropParamCachedPlan(entry->psrc);
        }
        PG_CATCH();
        {
            failflag_dropcachedplan = true;

            /* Must reset elog.c's state */
            MemoryContextSwitchTo(oldcontext);
            edata = CopyErrorData();
            FlushErrorState();
            ereport(LOG,
                    (errmodule(MOD_EXECUTOR),
                     errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("failed to drop cached plan when drop all prepared statements: %s", edata->message)));
            FreeErrorData(edata);
        }
        PG_END_TRY();

        /* Now we can remove the hash table entry */
        dropFromQueryHashTable(entry->parameterized_query);
    }
    ReleaseTempResourceOwner();

    if (failflag_dropcachedplan) {
        /* destory connections to other node to cleanup all cached statements */
        destroy_handles();
        ereport(ERROR, (errmodule(MOD_EXECUTOR), errcode(ERRCODE_INTERNAL_ERROR), errmsg("failed to drop cached plan")));
    }
}

static void dropFromQueryHashTable(const char* parameterizedQuery)
{
    hash_search(u_sess->param_cxt.parameterized_queries, parameterizedQuery, HASH_REMOVE, NULL);
}

/*
 * dropParamCachedPlan: destroy a cached plan.
 *
 * Actually this only destroys the CachedPlanSource: any referenced CachedPlan
 * is released, but not destroyed until its refcount goes to zero.	That
 * handles the situation where DropCachedPlan is called while the plan is
 * still in use.
 */
void dropParamCachedPlan(CachedPlanSource* plansource)
{
    Assert(plansource->magic == CACHEDPLANSOURCE_MAGIC);
    /* If it's been saved, remove it from the list */
    if (plansource->is_saved) {
        if (u_sess->param_cxt.first_saved_plan == plansource) {
            u_sess->param_cxt.first_saved_plan = plansource->next_saved;
        }
        else {
            CachedPlanSource* psrc = NULL;

            for (psrc = u_sess->param_cxt.first_saved_plan; psrc; psrc = psrc->next_saved) {
                if (psrc->next_saved == plansource) {
                    psrc->next_saved = plansource->next_saved;
                    break;
                }
            }
        }
        plansource->is_saved = false;
    }
    plansource->next_saved = NULL;
    DropCachedPlanInternal(plansource);

    /* Mark it no longer valid */
    plansource->magic = 0;
    /*
     * Remove the CachedPlanSource and all subsidiary data (including the
     * query_context if any).  But if it's a one-shot we can't free anything.
     */
    if (!plansource->is_oneshot)
        MemoryContextDelete(plansource->context);
}
