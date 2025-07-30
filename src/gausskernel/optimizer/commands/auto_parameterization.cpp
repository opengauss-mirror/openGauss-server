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

static void insertIntoParameterizedHashTable(ParamCachedKey* key, CachedPlanSource* psrc, bool* found);
static void storeParamCachedPlan(ParamCachedKey* key, CachedPlanSource* psrc);
static ParamCachedPlan* fetchCachedPlan(ParamCachedKey* key);
static char* execParameterization(Node* parsetree, ParameterizationInfo* paramContext);
static bool parameterizeParsetree(Node* node, ParameterizationInfo* paramContext);
static void fillInConstantLengths(ParamState* pstate, const char* query);
static bool isNodeSkipParam(Node* node);
static inline int compLocation(const void* a, const void* b);
static bool canTurnParam(Node* node, Node* a_const, ParameterizationInfo* param);
static char* generateNormalizedQuery(ParamState* pstate, const char* query, int* queryLenP, int encoding);
static ParamListInfo syncParams(Oid* paramTypes, int numParams, List* params, CachedPlanSource* psrc,
                                const char* queryString, EState* estate);
static void dropFromQueryHashTable(const ParamCachedKey* key);
static void dropParamCachedPlan(CachedPlanSource* plansource);
extern uint32 cachedPlanKeyHashFunc(const void* key, Size keysize);
extern int cachedPlanKeyHashMatch(const void* key1, const void* key2, Size keysize);
static bool composeParamInfo(ParameterizationInfo* paramInfo);
static void tryTurnConstToParam(Node** node, Node* a_const, ParameterizationInfo* paramInfo, bool* res,
                                ListCell* lc = NULL);
static void makeParamKey(ParamCachedKey* paramCachedKey, ParameterizationInfo* paramInfo, char* parameterizedQuery,
                         Oid relOid);
static bool executeParamQuery(CachedPlanSource* psrc, ParamListInfo paramListInfo, DestReceiver* dest,
                              char* completionTag, CommandDest cmdDest);
static bool tryBypass(CachedPlanSource* psrc, ParameterizationInfo* paramInfo, DestReceiver* dest,
                      ParamListInfo* paramListInfo, char* completionTag);
static CachedPlanSource* buildParamCachedPlan(Node* parsetree, const char* queryString, ParamCachedKey* paramCachedKey,
                                              ParameterizationInfo* paramInfo, ParamListInfo* paramListInfo,
                                              MemoryContext oldContext);
void saveParamCachedPlan(CachedPlanSource* plansource);
static void parsetreeRollBack(ParameterizationInfo* paramInfo);
static bool isTypeValid(Oid argType);
static bool validateType(ParamLocationLen clocations[], Oid* argTypes, int nargs);
static bool IsStringLengthValid(const char* queryString);
static bool IsQualifiedParsetree(Node* parsetree, RangeVar** rel);
static bool IsQualifiedTbl(RangeVar* rel, Oid* relOid);
static bool isDataValid(Oid paramType, Node* node);

char* query_type_text[FIXED_QUERY_TYPE_LEN] = {"OTHERS", "INSERT", "UPDATE", "DELETE"};

bool isQualifiedIuds(Node* parsetree, const char* queryString, Oid* relOid)
{
    *relOid = InvalidOid;
    if (!IsStringLengthValid(queryString)) {
        return false;
    }
    RangeVar* rel = NULL;
    bool res = IsQualifiedParsetree(parsetree, &rel);
    if (res == false || rel == NULL || rel->relname == NULL) {
        return false;
    }
    if (!IsQualifiedTbl(rel, relOid)) {
        return false;
    }
    return true;
}

static bool IsStringLengthValid(const char* queryString)
{
    return (strlen(queryString) <= MAX_PARAM_QUERY_LEN);
}

static bool IsQualifiedParsetree(Node* parsetree, RangeVar** rel)
{
    bool res = true;
    switch (nodeTag(parsetree)) {
        case T_InsertStmt: {
            InsertStmt* stmt = (InsertStmt*)parsetree;
            if (stmt->relation == NULL || stmt->returningList != NIL || stmt->withClause != NULL ||
                stmt->upsertClause != NULL || stmt->hintState != NULL || stmt->selectStmt == NULL || stmt->hasIgnore ||
                ((SelectStmt*)stmt->selectStmt)->valuesLists == NULL) {
                res = false;
            }
            if (stmt->relation != NULL) {
                *rel = (RangeVar*)stmt->relation;
            }
            break;
        }
        case T_DeleteStmt: {
            DeleteStmt* stmt = (DeleteStmt*)parsetree;
            if (stmt->whereClause == NULL || stmt->usingClause != NULL || stmt->returningList != NIL ||
                stmt->withClause != NULL || stmt->hintState != NULL || stmt->sortClause != NULL ||
                stmt->limitClause != NULL || stmt->relations == NIL || list_length(stmt->relations) != 1) {
                res = false;
            }
            if (list_length(stmt->relations) == 1) {
                *rel = (RangeVar*)linitial(stmt->relations);
            }
            break;
        }
        case T_UpdateStmt: {
            UpdateStmt* stmt = (UpdateStmt*)parsetree;
            if (stmt->returningList != NULL || stmt->withClause != NULL || stmt->hintState != NULL ||
                stmt->sortClause != NULL || stmt->limitClause != NULL || stmt->hasIgnore ||
                stmt->relationClause == NIL || list_length(stmt->relationClause) != 1) {
                res = false;
            }
            if (list_length(stmt->relationClause) == 1) {
                *rel = (RangeVar*)linitial(stmt->relationClause);
                if ((*rel)->partitionname != NULL) {
                    res = false;
                }
            }
            break;
        }

        default:
            res = false;
    }
    return res;
}

static bool IsQualifiedTbl(RangeVar* rel, Oid* relOid)
{
    *relOid = RelnameGetRelid(rel->relname);
    if (!OidIsValid(*relOid)) {
        return false;
    }
    Relation relation = heap_open(*relOid, NoLock);
    if (RelationIsPartitioned(relation)) {
        heap_close(relation, NoLock);
        return false;
    }
    heap_close(relation, NoLock);
    return true;
}

static bool isNodeSkipParam(Node* node)
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
        case T_NullTest:
        case T_SubLink:
        case T_CaseExpr:
        case T_PredictByFunction:
            res = true;
            break;
        default:
            break;
    }
    return res;
}

/* A variant of function EvaluateParams() */
static ParamListInfo syncParams(Oid* paramTypes, int numParams, List* params, CachedPlanSource* psrc,
                                const char* queryString, EState* estate)
{
    ParamListInfo paramListInfo;
    ParseState* pstate = NULL;
    Oid paramCollation;
    int paramCharset;
    int i;
    ListCell* lc = NULL;
    List* exprstates = NIL;

    if (numParams == 0) {
        return NULL;
    }

    pstate = make_parsestate(NULL);
    pstate->p_sourcetext = queryString;

    paramCollation = GetCollationConnection();
    paramCharset = GetCharsetConnection();
    i = 0;
    foreach (lc, params) {
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
        expr = coerce_to_target_type(pstate, expr, givenTypeId, expectedTypeId, -1, COERCION_ASSIGNMENT,
                                     COERCE_IMPLICIT_CAST, NULL, NULL, -1);
        if (expr == NULL)
            ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                            errmsg("parameter $%d of type %s cannot be coerced to the expected type %s", i + 1,
                                   format_type_be(givenTypeId), format_type_be(expectedTypeId)),
                            errhint("You will need to rewrite or cast the expression.")));

        /* Take care of collations in the finished expression. */
        assign_expr_collations(pstate, expr);

        /* Try convert expression to target parameter charset. */
        if (OidIsValid(paramCollation) && IsSupportCharsetType(expectedTypeId)) {
            /* convert charset only, expression will be evaluated below */
            expr = coerce_to_target_charset(expr, paramCharset, expectedTypeId, -1, paramCollation, false);
        }
        lfirst(lc) = expr;
        i++;
    }

    pfree_ext(pstate);
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
    bool isInsertConst = IsA(psrc->raw_parse_tree, InsertStmt);
    foreach (lc, params) {
        if (!IsA(lfirst(lc), Const)) {
            isInsertConst = false;
            break;
        }
    }
    i = 0;
    if (isInsertConst) {
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
    } else {
        exprstates = ExecPrepareExprList(params, estate);
        foreach (lc, exprstates) {
            ExprState* n = (ExprState*)lfirst(lc);
            ParamExternData* prm = &paramListInfo->params[i];

            prm->ptype = paramTypes[i];
            prm->pflags = PARAM_FLAG_CONST;
            prm->value = ExecEvalExprSwitchContext(n, GetPerTupleExprContext(estate), &prm->isnull);
            prm->tabInfo = NULL;

            i++;
        }
    }

    return paramListInfo;
}

bool execQueryParameterization(Node* parsetree, const char* queryString, CommandDest cmdDest, char* completionTag,
                               Oid relOid)
{
    ParameterizationInfo* paramInfo = NULL;
    ParamCachedKey paramCachedKey;
    MemoryContext oldContext = NULL;
    ParamListInfo paramListInfo = NULL;
    DestReceiver* dest = NULL;
    CachedPlanSource* psrc = NULL;
    EState* estate = NULL;
    ParamListInfo params = NULL;

    paramInfo = (ParameterizationInfo*)palloc0(sizeof(ParameterizationInfo));
    paramInfo->parent_node = parsetree;
    paramInfo->query_string = queryString;
    paramInfo->is_skip = false;
    char* parameterizedQuery = execParameterization(parsetree, paramInfo);
    if (parameterizedQuery == NULL || paramInfo->param_count == 0) {
        return false;
    }
    if (!composeParamInfo(paramInfo)) {
        return false;
    }
    makeParamKey(&paramCachedKey, paramInfo, parameterizedQuery, relOid);
    dest = CreateDestReceiver(cmdDest);
    ParamCachedPlan* paramCachedPlan = fetchCachedPlan(&paramCachedKey);
    if (paramCachedPlan != NULL) {
        psrc = paramCachedPlan->psrc;
        t_thrd.postgres_cxt.cur_command_tag = transform_node_tag(psrc->raw_parse_tree);

        if (!validateType(paramInfo->param_state.clocations, psrc->param_types, psrc->num_params)) {
            parsetreeRollBack(paramInfo);
            return false;
        }
        estate = CreateExecutorState();
        estate->es_param_list_info = params;
        paramListInfo = syncParams(psrc->param_types, psrc->num_params, paramInfo->params, psrc, queryString, estate);

        if (tryBypass(psrc, paramInfo, dest, &paramListInfo, completionTag)) {
            return true;
        }
        if (executeParamQuery(psrc, paramListInfo, dest, completionTag, cmdDest)) {
            return true;
        }
    } else {
        oldContext = MemoryContextSwitchTo(u_sess->param_cxt.query_param_cxt);
        psrc = buildParamCachedPlan(parsetree, queryString, &paramCachedKey, paramInfo, &paramListInfo, oldContext);
        (void)MemoryContextSwitchTo(oldContext);
        if (psrc == NULL) {
            parsetreeRollBack(paramInfo);
            return false;
        }
        if (executeParamQuery(psrc, paramListInfo, dest, completionTag, cmdDest)) {
            return true;
        }
    }
    parsetreeRollBack(paramInfo);
    pfree_ext(paramListInfo);
    pfree_ext(paramInfo);
    return false;
}

static void storeParamCachedPlan(ParamCachedKey* key, CachedPlanSource* psrc)
{
    bool found = false;

    if (unlikely(!u_sess->param_cxt.parameterized_queries)) {
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("hash table for parameterized query does not exist")));
    }

    insertIntoParameterizedHashTable(key, psrc, &found);

    if (found) {
        ereport(ERROR, (errcode(ERRCODE_DUPLICATE_PSTATEMENT),
                        errmsg("parameterized query \"%s\" already exists", key->parameterized_query)));
    }

    saveParamCachedPlan(psrc);
    return;
}

static void insertIntoParameterizedHashTable(ParamCachedKey* key, CachedPlanSource* psrc, bool* found)
{
    ParamCachedPlan* entry = NULL;
    entry = (ParamCachedPlan*)hash_search(u_sess->param_cxt.parameterized_queries, key, HASH_ENTER, found);
    if (!(*found)) {
        entry->psrc = psrc;
    }
    return;
}

static char* execParameterization(Node* parsetree, ParameterizationInfo* paramInfo)
{
    paramInfo->parent_node = parsetree;
    paramInfo->param_count = 0;

    parameterizeParsetree(parsetree, paramInfo);
    if (paramInfo->is_skip || paramInfo->param_count == 0) {
        return NULL;
    }

    paramInfo->param_state.clocations_count = paramInfo->param_count;
    const char* queryString = paramInfo->query_string;
    int encoding = GetDatabaseEncoding();
    int queryLen = strlen(queryString);

    char* parameterizedQuery = generateNormalizedQuery(&(paramInfo->param_state), queryString, &queryLen, encoding);
    parameterizedQuery[queryLen] = '\0';
    return parameterizedQuery;
}

static ParamCachedPlan* fetchCachedPlan(ParamCachedKey* key)
{
    if (u_sess->param_cxt.parameterized_queries == NULL) {
        return NULL;
    }

    ParamCachedPlan* entry = NULL;
    entry = (ParamCachedPlan*)hash_search(u_sess->param_cxt.parameterized_queries, key, HASH_FIND, NULL);
    if (entry == NULL) {
        ereport(LOG, (errcode(ERRCODE_UNDEFINED_PSTATEMENT),
                      errmsg("ParamCachedPlan %s not found\n", key->parameterized_query)));
    }
    return entry;
}

static bool parameterizeParsetree(Node* node, ParameterizationInfo* paramInfo)
{
    if (node == NULL) {
        return false;
    }
    if (paramInfo->is_skip) {
        return true;
    }

    if (IsA(node, ParamRef)) {
        paramInfo->is_skip = true;
        return true;
    }

    Node* saveParentNode = paramInfo->parent_node;
    if (isNodeSkipParam(node)) {
        paramInfo->is_skip = true;
        return true;
    } else if (IsA(node, A_Const)) {
        canTurnParam(paramInfo->parent_node, node, paramInfo);
    } else {
        paramInfo->parent_node = node;
    }

    bool res = raw_expression_tree_walker(node, (bool (*)())parameterizeParsetree, paramInfo);
    paramInfo->parent_node = saveParentNode;
    return res;
}

static void tryTurnConstToParam(Node** node, Node* a_const, ParameterizationInfo* paramInfo, bool* res,
                                ListCell* lc)
{
    if (*node == a_const) {
        if (paramInfo->param_count == MAX_PARAM_NODES) {
            paramInfo->is_skip = true;
            *res = false;
            return;
        }
        A_Const* tmp = (A_Const*)a_const;
        ParamLocationLen* curParam = &(paramInfo->param_state.clocations[paramInfo->param_count]);
        curParam->location = tmp->location;
        curParam->node = *node;
        curParam->node_addr = node;
        curParam->lc = lc;
        paramInfo->param_count++;
        *res = true;
    }
    return;
}

static bool canTurnParam(Node* node, Node* a_const, ParameterizationInfo* param)
{
    if (node == NULL) {
        return false;
    }
    bool res = false;
    switch (nodeTag(node)) {
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
        case T_FunctionPartitionInfo:
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
            break;
        }
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
                tryTurnConstToParam(&tmp, a_const, param, &res, lc);
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
            ereport(ERROR,
                    (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("Node type unrecognized: %d\n", nodeTag(node))));
    }
    return res;
}

static char* generateNormalizedQuery(ParamState* pstate, const char* query, int* queryLenP, int encoding)
{
    char* normQuery = NULL;
    int queryLen = *queryLenP;
    int maxOutputLen, i, rc;
    int lenToWrt;       /* Length (in bytes) to write */
    int querLoc = 0;     /* Source query byte location */
    int nQuerLoc = 0;   /* Normalized query byte location */
    int lastOff = 0;     /* Offset from start for previous tok */
    int lastTokLen = 0; /* Length (in bytes) of that tok */

    /*
     * Get constants' lengths (core system only gives us locations).  Note
     * this also ensures the items are sorted by location.
     */
    fillInConstantLengths(pstate, query);

    /* Allocate result buffer, ensuring we limit result to allowed size */
    maxOutputLen = Min(queryLen, g_instance.attr.attr_common.pgstat_track_activity_query_size - 1);
    normQuery = static_cast<char*>(palloc0(maxOutputLen + 1));
    for (i = 0; i < pstate->clocations_count; i++) {
        /*
         * off: Offset from start for cur tok
         * tokLen: Length (in bytes) of that tok
         */
        int off, tokLen;
        off = pstate->clocations[i].location;
        tokLen = pstate->clocations[i].length;

        if (tokLen < 0) {
            continue; /* ignore any duplicates */
        }
        /* Copy next chunk, or as much as will fit */
        lenToWrt = off - lastOff;
        lenToWrt -= lastTokLen;
        lenToWrt = Min(lenToWrt, maxOutputLen - nQuerLoc);
        /* Should not happen, but for below SQL, Query struct and
         * query string can't be matched(location in Query is bigger
         * than query string)
         *  - delete from plan_table where statement_id='test statement_id',
         *    for sql 'delete plan_table', transformDeleteStmt method will
         *    modify Query member.
         */
        if (lenToWrt <= 0) {
            break;
        }

        rc = memcpy_s(normQuery + nQuerLoc, maxOutputLen - nQuerLoc, query + querLoc, lenToWrt);
        securec_check(rc, "\0", "\0");
        nQuerLoc += lenToWrt;

        if (nQuerLoc < maxOutputLen) {
            normQuery[nQuerLoc++] = '?';
        }

        querLoc = off + tokLen;
        lastOff = off;
        lastTokLen = tokLen;

        /* If we run out of space, might as well stop iterating */
        if (nQuerLoc >= maxOutputLen) {
            break;
        }
    }

    /*
     * We've copied up until the last ignorable constant.  Copy over the
     * remaining bytes of the original query string, or at least as much as
     * will fit.
     */
    lenToWrt = queryLen - querLoc;
    lenToWrt = Min(lenToWrt, maxOutputLen - nQuerLoc);
    if (lenToWrt > 0) {
        rc = memcpy_s(normQuery + nQuerLoc, maxOutputLen - nQuerLoc, query + querLoc, lenToWrt);
        securec_check(rc, "\0", "\0");
        nQuerLoc += lenToWrt;
    }

    /*
     * If we ran out of space, we need to do an encoding-aware truncation,
     * just to make sure we don't have an incomplete character at the end.
     */
    if (nQuerLoc >= maxOutputLen) {
        queryLen = pg_encoding_mbcliplen(encoding, normQuery, nQuerLoc,
                                          g_instance.attr.attr_common.pgstat_track_activity_query_size - 1);
    } else {
        queryLen = nQuerLoc;
    }

    *queryLenP = queryLen;
    return normQuery;
}

static void fillInConstantLengths(ParamState* pstate, const char* query)
{
    ParamLocationLen* locs = NULL;
    core_yyscan_t yyscanner;
    core_yy_extra_type yyextra;
    core_YYSTYPE yylval;
    YYLTYPE yylloc;
    int lastLoc = -1;
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

        if (loc <= lastLoc) {
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

        lastLoc = loc;
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
    ParamCachedPlan* entry = NULL;
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

    bool failFlagDropCachedPlan = false;
    ErrorData* edata = NULL;
    MemoryContext oldContext = CurrentMemoryContext;
    bool isSharedPlan = false;

    /* walk over cache */
    hash_seq_init(&seq, u_sess->param_cxt.parameterized_queries);
    while ((entry = (ParamCachedPlan*)hash_seq_search(&seq)) != NULL) {
        PG_TRY();
        {
            /* Release the plancache entry */
            Assert(entry->psrc->magic == CACHEDPLANSOURCE_MAGIC);
            isSharedPlan = entry->psrc->gpc.status.InShareTable();
            if (!isSharedPlan) {
                CN_GPC_LOG("prepare remove private", entry->psrc, entry->paramCachedKey.parameterized_query);
                dropParamCachedPlan(entry->psrc);
                CN_GPC_LOG("prepare remove private succ", 0, entry->paramCachedKey.parameterized_query);
            }
        }
        PG_CATCH();
        {
            failFlagDropCachedPlan = true;

            /* Must reset elog.c's state */
            MemoryContextSwitchTo(oldContext);
            edata = CopyErrorData();
            FlushErrorState();
            ereport(LOG, (errmodule(MOD_EXECUTOR), errcode(ERRCODE_INTERNAL_ERROR),
                          errmsg("failed to drop cached plan when drop all prepared statements: %s", edata->message)));
            FreeErrorData(edata);
        }
        PG_END_TRY();
        if (isSharedPlan) {
            CN_GPC_LOG("prepare remove ", entry->psrc, entry->paramCachedKey.parameterized_query);
            /* sub refcount savely */
            entry->psrc->gpc.status.SubRefCount();
        }

        /* Now we can remove the hash table entry */
        dropFromQueryHashTable(&entry->paramCachedKey);
    }
    ReleaseTempResourceOwner();
    CN_GPC_LOG("remove prepare statment all", 0, 0);

    ReleaseTempResourceOwner();
    if (failFlagDropCachedPlan) {
        /* destory connections to other node to cleanup all cached statements */
        destroy_handles();
        ereport(ERROR,
                (errmodule(MOD_EXECUTOR), errcode(ERRCODE_INTERNAL_ERROR), errmsg("failed to drop cached plan")));
    }
}

static void dropFromQueryHashTable(const ParamCachedKey* key)
{
    hash_search(u_sess->param_cxt.parameterized_queries, key, HASH_REMOVE, NULL);
}

/*
 * dropParamCachedPlan: destroy a cached plan.
 *
 * Actually this only destroys the CachedPlanSource: any referenced CachedPlan
 * is released, but not destroyed until its refcount goes to zero.	That
 * handles the situation where DropCachedPlan is called while the plan is
 * still in use.
 */
static void dropParamCachedPlan(CachedPlanSource* plansource)
{
    Assert(plansource->magic == CACHEDPLANSOURCE_MAGIC);
    /* If it's been saved, remove it from the list */
    if (plansource->is_saved) {
        if (u_sess->param_cxt.first_saved_plan == plansource) {
            u_sess->param_cxt.first_saved_plan = plansource->next_saved;
        } else {
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

extern uint32 cachedPlanKeyHashFunc(const void* key, Size keysize)
{
    const ParamCachedKey* paramCachedKey = (const ParamCachedKey*)key;
    uint32 hashValue1 =
        DatumGetUInt32(hash_any((const unsigned char*)paramCachedKey->parameterized_query, paramCachedKey->query_len));
    uint32 hashValue2 = DatumGetUInt32(
        hash_any((const unsigned char*)paramCachedKey->param_types, paramCachedKey->num_param * sizeof(Oid)));
    hashValue1 ^= hashValue2;
    return hashValue1;
}

extern int cachedPlanKeyHashMatch(const void* key1, const void* key2, Size keysize)
{
    ParamCachedKey* leftKey = (ParamCachedKey*)key1;
    ParamCachedKey* rightKey = (ParamCachedKey*)key2;
    Assert(leftKey != NULL);
    Assert(rightKey != NULL);

    if (memcmp(key1, key2, keysize)) {
        return 1;
    }

    return 0;
}

static bool composeParamInfo(ParameterizationInfo* paramInfo)
{
    if (paramInfo->param_state.clocations == NULL) {
        return false;
    }

    A_Const* aconst;
    paramInfo->param_types = (Oid*)palloc(sizeof(Oid) * paramInfo->param_count);
    for (int i = 0; i < paramInfo->param_count; i++) {
        Node* tmpNode = paramInfo->param_state.clocations[i].node;
        aconst = (A_Const*)tmpNode;
        int location = aconst->location;
        Const* con = NULL;
        Oid typeOid = 0;
        con = make_const(NULL, &aconst->val, location);
        typeOid = con->consttype;
        paramInfo->param_types[i] = typeOid;
        paramInfo->params = lappend(paramInfo->params, paramInfo->param_state.clocations[i].node);
        ParamRef* paramRef = makeNode(ParamRef);
        paramRef->location = location;
        paramRef->number = i + 1;
        if (paramInfo->param_state.clocations[i].lc == NULL) {
            *(paramInfo->param_state.clocations[i].node_addr) = (Node*)paramRef;
        } else {
            lfirst(paramInfo->param_state.clocations[i].lc) = paramRef;
        }
    }
    return true;
}

static void makeParamKey(ParamCachedKey* paramCachedKey, ParameterizationInfo* paramInfo, char* parameterizedQuery,
                         Oid relOid)
{
    error_t rc = 0;
    memset_s(paramCachedKey, sizeof(ParamCachedKey), 0, sizeof(ParamCachedKey));
    int paramQueryLen = strlen(parameterizedQuery);
    rc = memcpy_s(paramCachedKey->parameterized_query, MAX_PARAM_QUERY_LEN, parameterizedQuery, paramQueryLen);
    securec_check(rc, "\0", "\0");
    paramCachedKey->parameterized_query[paramQueryLen] = '\0';
    rc = memcpy_s(paramCachedKey->param_types, sizeof(paramCachedKey->param_types), paramInfo->param_types,
                  paramInfo->param_count * sizeof(Oid));
    securec_check(rc, "\0", "\0");
    paramCachedKey->relOid = relOid;
    paramCachedKey->query_len = paramQueryLen;
    paramCachedKey->num_param = paramInfo->param_count;
    return;
}

static CachedPlanSource* buildParamCachedPlan(Node* parsetree, const char* queryString, ParamCachedKey* paramCachedKey,
                                              ParameterizationInfo* paramInfo, ParamListInfo* paramListInfo,
                                              MemoryContext oldContext)
{
    Query* query = NULL;
    CachedPlanSource* psrc = NULL;
    List* queryList = NIL;
    EState* estate = NULL;
    bool fixedResult = FORCE_VALIDATE_PLANCACHE_RESULT;
    int nargs = 0;
    ParamListInfo params = NULL;
    if (u_sess->param_cxt.param_cached_plan_count >= u_sess->attr.attr_sql.max_parameterized_query_stored) {
        dropAllParameterizedQueries();
        u_sess->param_cxt.param_cached_plan_count = 0;
    }

    psrc = CreateCachedPlan(parsetree,
                            paramCachedKey->parameterized_query,
                            NULL,
                            CreateCommandTag(parsetree)
                            );

    (void)MemoryContextSwitchTo(oldContext);
    query = parse_analyze_varparams(parsetree, queryString, &paramInfo->param_types, &nargs);
    if (paramInfo->param_count != nargs) {
        return NULL;
    }
    /* check all parameter types that were determined */
    if (!validateType(paramInfo->param_state.clocations, paramInfo->param_types, paramInfo->param_count)) {
        pfree_ext(psrc);
        return NULL;
    }
    queryList = QueryRewrite(query);

    estate = CreateExecutorState();
    estate->es_param_list_info = params;
    *paramListInfo =
        syncParams(paramInfo->param_types, paramInfo->param_count, paramInfo->params, psrc, queryString, estate);
    (void)MemoryContextSwitchTo(u_sess->param_cxt.query_param_cxt);
    CompleteCachedPlan(psrc,
                       queryList,
                       NULL,
                       paramInfo->param_types,
                       NULL,
                       paramInfo->param_count,
                       NULL,
                       NULL,
                       0,
                       fixedResult,
                       "");

    storeParamCachedPlan(paramCachedKey, psrc);
    u_sess->param_cxt.param_cached_plan_count++;
    return psrc;
}

static bool tryBypass(CachedPlanSource* psrc, ParameterizationInfo* paramInfo, DestReceiver* dest,
                      ParamListInfo* paramListInfo, char* completionTag)
{
    /* Shouldn't find a non-fixed-result cached plan */
    if (!psrc->fixed_result && FORCE_VALIDATE_PLANCACHE_RESULT)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("EXECUTE does not support variable-result cached plans")));

    OpFusion::clearForCplan((OpFusion*)psrc->opFusionObj, psrc);

    if (psrc->opFusionObj != NULL) {
        Assert(psrc->cplan == NULL);
        (void)RevalidateCachedQuery(psrc);
    }

    if (psrc->opFusionObj != NULL) {
        u_sess->param_cxt.use_parame = true;
        OpFusion* opFusionObj = (OpFusion*)(psrc->opFusionObj);
        opFusionObj->setPreparedDestReceiver(dest);
        opFusionObj->useOuterParameter(*paramListInfo);
        opFusionObj->setCurrentOpFusionObj(opFusionObj);
        opFusionObj->m_local.m_isFirst = true;
#ifdef ENABLE_MULTIPLE_NODES
        CachedPlanSource* cps = opFusionObj->m_global->m_psrc;
        bool needBucketId = cps != NULL && cps->gplan;
        if (needBucketId) {
            setCachedPlanBucketId(cps->gplan, *paramListInfo);
        }
#endif
        if (OpFusion::process(FUSION_EXECUTE, NULL, 0, completionTag, false, NULL)) {
            return true;
        }
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Bypass process Failed")));
    }
    return false;
}

static bool executeParamQuery(CachedPlanSource* psrc, ParamListInfo paramListInfo, DestReceiver* dest,
                              char* completionTag, CommandDest cmdDest)
{
    CachedPlan* cplan = NULL;
    List* plan_list = NIL;
    Portal portal = NULL;
    long count = FETCH_ALL;
    int eflags = 0;

    u_sess->param_cxt.use_parame = true;
    if (ENABLE_CACHEDPLAN_MGR) {
        cplan = GetWiseCachedPlan(psrc, paramListInfo, false);
    } else {
        cplan = GetCachedPlan(psrc, paramListInfo, false);
    }
    plan_list = cplan->stmt_list;

    if (OpFusion::IsSqlBypass(psrc, plan_list)) {
        psrc->opFusionObj = OpFusion::FusionFactory(OpFusion::getFusionType(cplan, paramListInfo, NULL),
                                                    u_sess->param_cxt.query_param_cxt, psrc, NULL, paramListInfo);
        psrc->is_checked_opfusion = true;
        if (psrc->opFusionObj != NULL) {
            ((OpFusion*)psrc->opFusionObj)->setPreparedDestReceiver(dest);
            ((OpFusion*)psrc->opFusionObj)->useOuterParameter(paramListInfo);
            ((OpFusion*)psrc->opFusionObj)->setCurrentOpFusionObj((OpFusion*)psrc->opFusionObj);

            if (OpFusion::process(FUSION_EXECUTE, NULL, 0, completionTag, false, NULL)) {
                return true;
            }
            Assert(0);
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Bypass process Failed")));
        }
    }

    portal = CreateNewPortal();
    portal->visible = false;
    if (cmdDest == DestRemote) {
        SetRemoteDestReceiverParams(dest, portal);
    }
    PortalDefineQuery(portal, NULL, psrc->query_string, psrc->commandTag, plan_list, cplan);
    portal->nextval_default_expr_type = psrc->nextval_default_expr_type;

    /*
     * Run the portal as appropriate.
     */
    PortalStart(portal, paramListInfo, eflags, GetActiveSnapshot());

    (void)MemoryContextSwitchTo(u_sess->top_transaction_mem_cxt);
    (void)PortalRun(portal, count, false, dest, dest, completionTag);

    PortalDrop(portal, false);
    pfree_ext(paramListInfo);

    return true;
}

/* A variant of SaveCachedPlan */
void saveParamCachedPlan(CachedPlanSource* plansource)
{
    /* Assert caller is doing things in a sane order */
    Assert(plansource->magic == CACHEDPLANSOURCE_MAGIC);
    Assert(plansource->is_complete);
    Assert(!plansource->is_saved);
    Assert(plansource->gpc.status.InShareTable() == false);
    /* This seems worth a real test, though */
    if (plansource->is_oneshot)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot save one-shot cached plan")));

    /*
     * In typical use, this function would be called before generating any
     * plans from the CachedPlanSource.  If there is a generic plan, moving it
     * into u_sess->cache_mem_cxt would be pretty risky since it's unclear
     * whether the caller has taken suitable care with making references
     * long-lived.	Best thing to do seems to be to discard the plan.
     */

    ReleaseGenericPlan(plansource);

    START_CRIT_SECTION();

    /*
     * Add the entry to the session's global list of cached plans.
     */
    plansource->next_saved = u_sess->param_cxt.first_saved_plan;
    u_sess->param_cxt.first_saved_plan = plansource;

    plansource->is_saved = true;
    END_CRIT_SECTION();
}

static void parsetreeRollBack(ParameterizationInfo* paramInfo)
{
    int paramCount = paramInfo->param_count;
    for (int i = 0; i < paramCount; i++) {
        Node* originNode = paramInfo->param_state.clocations[i].node;
        if (paramInfo->param_state.clocations[i].lc == NULL) {
            *(paramInfo->param_state.clocations[i].node_addr) = originNode;
        } else {
            lfirst(paramInfo->param_state.clocations[i].lc) = originNode;
        }
    }

    return;
}

static bool isDataValid(Oid paramType, Node* node)
{
    A_Const* aconst = (A_Const*)node;
    Value value = aconst->val;
    switch (paramType) {
        case INT1OID: {
            long num = value.val.ival;
            if (num < SCHAR_MIN || num > SCHAR_MAX) {
                return false;
            }
            break;
        }
        case INT2OID: {
            long num = value.val.ival;
            if (num < SHRT_MIN || num > SHRT_MAX) {
                return false;
            }
            break;
        }
        case INT4OID: {
            long num = value.val.ival;
            if (num < INT_MIN || num > INT_MAX) {
                return false;
            }
            break;
        }
        case INT8OID: {
            int64 num = 0L;
            if (nodeTag(&value) == T_Float) {
                if (!scanint8(strVal(&value), true, &num)) {
                    return false;
                }
                if (num < LONG_MIN || num > LONG_MAX) {
                    return false;
                }
            }
            break;
        }

        default:
            break;
    }
    return true;
}

static bool isTypeValid(Oid argType)
{
    /* INT16 is only available when enable_beta_features = on, and it will cause some problem,
    so we do not want handle it at least now */
    if (argType == InvalidOid || argType == UNKNOWNOID || argType == INT16OID) {
        return false;
    }
    return true;
}

static bool validateType(ParamLocationLen clocations[], Oid* argTypes, int nargs)
{
    Node* node = NULL;
    Oid argType = InvalidOid;
    for (int i = 0; i < nargs; i++) {
        node = clocations[i].node;
        argType = argTypes[i];
        if (!isTypeValid(argType) || !isDataValid(argType, node)) {
            return false;
        }
    }
    return true;
}