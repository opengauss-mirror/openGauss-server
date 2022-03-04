/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2021, openGauss Contributors
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
 * sqladvisor_online.cpp
 *		sqladvisor online model.
 * 
 *
 * IDENTIFICATION
 *      src/gausskernel/optimizer/commands/sqladvisor_online.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"
#include "nodes/pg_list.h"
#include "funcapi.h"
#include "optimizer/clauses.h"
#include "optimizer/planner.h"
#include "optimizer/randomplan.h"
#include "parser/parser.h"
#include "parser/parsetree.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/plpgsql.h"
#include "commands/sqladvisor.h"
#include "nodes/makefuncs.h"
#include "utils/typcache.h"

static bool checkGlobalAdvMemSize();
static void copyAdviseSearchPathFromSess(AdviseSearchPath* sp);
static PLpgSQL_execstate* copyPLpgEstate(PLpgSQL_execstate* srcEstate);
static PLpgSQL_expr* copyPLpgsqlExpr(PLpgSQL_expr* srcExpr);
static PLpgSQL_nsitem* copyPLpgNsitem(PLpgSQL_nsitem* srcNs);
static PLpgSQL_function* copyPLpgsqlFunc(PLpgSQL_function* srcFunc);
static bool equalParam(ParamListInfo bpA, ParamListInfo bpB);
static bool equalParamExternData(ParamExternData* prmA, ParamExternData* prmB);
static bool equalPLpgNsitem(PLpgSQL_nsitem* nsA, PLpgSQL_nsitem* nsB);
static bool equalPLpgsqlExpr(PLpgSQL_expr* exprA, PLpgSQL_expr* exprB);
static bool equalPLpgsqlFunc(PLpgSQL_function* funcA, PLpgSQL_function* funcB);
static bool equalStmtParam(SQLStatementParam* stmtParam, ParamListInfo boundParams, int cursorOptions);
static SQLStatementKey initSQLStatementKey(const char *src);
static SQLStatementParam* initSQLStatementParam(ParamListInfo paramLI, int cursorOptions);
static void statementStore(SQLStatementKey key, ParamListInfo boundParams, int cursorOptions);

void collectDynWithArgs(const char *src, ParamListInfo srcParamLI, int cursorOptions)
{
    checkGlobalAdvMemSize();
    MemoryContext oldcxt = MemoryContextSwitchTo(g_instance.adv_cxt.SQLAdvisorContext);
    ParamListInfo destParamLI = copyDynParam(srcParamLI);
    SQLStatementKey key = initSQLStatementKey(src);
    statementStore(key, destParamLI, cursorOptions);

    (void)MemoryContextSwitchTo(oldcxt);
}

static void statementStore(SQLStatementKey key, ParamListInfo boundParams, int cursorOptions)
{
    uint32 hashCode = SQLStmtHashFunc((const void *)&key, sizeof(key));
    uint32 bucketId = hashCode % GWC_NUM_OF_BUCKETS;
    Assert (bucketId >= 0 && bucketId < GPC_NUM_OF_BUCKETS);
    int lockId = g_instance.adv_cxt.GWCArray[bucketId].lockId;

    LWLockAcquire(GetMainLWLockByIndex(lockId), LW_EXCLUSIVE);
    MemoryContext oldcxt = MemoryContextSwitchTo(g_instance.adv_cxt.GWCArray[bucketId].context);

    bool found = true;
    SQLStatementEntry* entry = (SQLStatementEntry*)hash_search(
        g_instance.adv_cxt.GWCArray[bucketId].hashTbl, (const void*)&key, HASH_ENTER, &found);

    if (found) {
        ListCell* cell = NULL;
        foreach (cell, entry->paramList) {
            SQLStatementParam* stmtParam = (SQLStatementParam*)lfirst(cell);
            if (equalStmtParam(stmtParam, boundParams, cursorOptions)) {
                stmtParam->freqence += 1;
                break;
            }
        }
        if (cell == NULL) {
            SQLStatementParam* stmtParam = initSQLStatementParam(boundParams, cursorOptions);
            entry->paramList = lappend(entry->paramList, stmtParam);
        }
    } else {
        /* may occur maxsqlCount = -2 accoding to Concurrent scenarios. */
        (void)pg_atomic_fetch_sub_u32((volatile uint32*)&g_instance.adv_cxt.maxsqlCount, 1);
        entry->key = key;
        entry->paramList = NIL;
        SQLStatementParam* stmtParam = initSQLStatementParam(boundParams, cursorOptions);
        entry->paramList = lappend(entry->paramList, stmtParam);
    }

    (void)MemoryContextSwitchTo(oldcxt);
    LWLockRelease(GetMainLWLockByIndex(lockId));
}

static bool equalStmtParam(SQLStatementParam* stmtParam, ParamListInfo boundParams, int cursorOptions)
{
    if (stmtParam->cursorOptions == cursorOptions) {
        return equalParam(stmtParam->boundParams, boundParams);
    }
    
    return false;
}

static bool equalParam(ParamListInfo bpA, ParamListInfo bpB)
{   
    /* if bpA = bpB == NULL */
    if (bpA == bpB) {
        return true;
    } else if (bpA == NULL || bpB == NULL) {
        return false;
    }

    if (bpA->numParams != bpB->numParams ||
        bpA->parserSetup != bpB->parserSetup ||
        !equalPLpgsqlExpr((PLpgSQL_expr*)bpA->parserSetupArg, (PLpgSQL_expr*)bpB->parserSetupArg)) {
        return false;
    }

    for (int i = 0; i < bpA->numParams; i++) {
        if (!equalParamExternData(&bpA->params[i], &bpB->params[i])) {
            return false;
        }
    }

    return true;
}

ParamListInfo copyDynParam(ParamListInfo srcParamLI)
{
    if (srcParamLI == NULL) {
        return NULL;
    }
    ParamListInfo destParamLI;
    destParamLI = (ParamListInfo)palloc(offsetof(ParamListInfoData, params) +
                                        srcParamLI->numParams * sizeof(ParamExternData));
    destParamLI->paramFetch = NULL;
    destParamLI->paramFetchArg = NULL;
    destParamLI->params_need_process = false;
    destParamLI->numParams = srcParamLI->numParams;

    if (srcParamLI->parserSetup) {
        destParamLI->parserSetup = srcParamLI->parserSetup;
        destParamLI->parserSetupArg = (void*)copyPLpgsqlExpr((PLpgSQL_expr*)srcParamLI->parserSetupArg);
    } else {
        destParamLI->parserSetup = NULL;
        destParamLI->parserSetupArg = NULL;
    }

    for (int i = 0; i < srcParamLI->numParams; i++) {
        ParamExternData* srcPrm = &srcParamLI->params[i];
        ParamExternData* destPrm = &destParamLI->params[i];

        destPrm->isnull = srcPrm->isnull;
        destPrm->pflags = srcPrm->pflags;
        destPrm->ptype = srcPrm->ptype;

        /* need datumCopy in case it's a pass-by-reference datatype */
        if (destPrm->isnull || !OidIsValid(destPrm->ptype)) {
            destPrm->value = Datum(0);
        } else {
            int16 typLen;
            bool typByVal = false;
            get_typlenbyval(srcPrm->ptype, &typLen, &typByVal);
            destPrm->value = datumCopy(srcPrm->value, typByVal, typLen);
        }
        destPrm->tabInfo = NULL;
        if (srcPrm->tabInfo != NULL) {
            destPrm->tabInfo = (TableOfInfo*)palloc0(sizeof(TableOfInfo));
            destPrm->tabInfo->tableOfIndexType = srcPrm->tabInfo->tableOfIndexType;
            destPrm->tabInfo->tableOfIndex = copyTableOfIndex(srcPrm->tabInfo->tableOfIndex);
            destPrm->tabInfo->isnestedtable = srcPrm->tabInfo->isnestedtable;
            destPrm->tabInfo->tableOfLayers = srcPrm->tabInfo->tableOfLayers;
        }
        CopyCursorInfoData(&destPrm->cursor_data, &srcPrm->cursor_data);
    }

    return destParamLI;
}

static SQLStatementKey initSQLStatementKey(const char *src)
{
    SQLStatementKey key;
    key.queryString = pstrdup(src); 
    key.querylength = strlen(src);

    return key;
}

static SQLStatementParam* initSQLStatementParam(ParamListInfo paramLI, int cursorOptions)
{
    SQLStatementParam* stmtParam = (SQLStatementParam*)palloc(sizeof(SQLStatementParam));
    stmtParam->boundParams = paramLI;
    stmtParam->cursorOptions = cursorOptions;
    stmtParam->freqence = 1;
    stmtParam->searchPath = (AdviseSearchPath*)palloc(sizeof(AdviseSearchPath));
    copyAdviseSearchPathFromSess(stmtParam->searchPath);

    return stmtParam;
}

static void copyAdviseSearchPathFromSess(AdviseSearchPath* sp)
{
    sp->activeSearchPath = list_copy(u_sess->catalog_cxt.activeSearchPath);
    sp->baseSearchPath = list_copy(u_sess->catalog_cxt.baseSearchPath);
    sp->activeCreationNamespace = u_sess->catalog_cxt.activeCreationNamespace;
    sp->activeTempCreationPending = u_sess->catalog_cxt.activeTempCreationPending;
    sp->baseCreationNamespace = u_sess->catalog_cxt.baseCreationNamespace;
    sp->baseTempCreationPending = u_sess->catalog_cxt.baseTempCreationPending;
    sp->namespaceUser = u_sess->catalog_cxt.namespaceUser;
    sp->baseSearchPathValid = u_sess->catalog_cxt.baseSearchPathValid;
    sp->overrideStack = copyOverrideStack(u_sess->catalog_cxt.overrideStack);
    sp->overrideStackValid = u_sess->catalog_cxt.overrideStackValid;
}

List* copyOverrideStack(List* src)
{
    if (src == NULL) {
        return NIL;
    }

    List* dest = NIL;
    ListCell* cell = NULL;
    foreach (cell, src) {
        OverrideStackEntry* srcEntry = (OverrideStackEntry*)lfirst(cell);
        OverrideStackEntry* destEntry = (OverrideStackEntry*)palloc(sizeof(OverrideStackEntry));
        destEntry->searchPath = list_copy(srcEntry->searchPath);
        destEntry->creationNamespace = srcEntry->creationNamespace;
        destEntry->nestLevel = srcEntry->nestLevel;
        destEntry->inProcedure = srcEntry->inProcedure;
        dest = lappend(dest, destEntry);
    }

    return dest;
}

static bool equalPLpgsqlExpr(PLpgSQL_expr* exprA, PLpgSQL_expr* exprB)
{
    if (exprA == exprB) {
        return true;
    } else if (exprA == NULL || exprB == NULL) {
        return false;
    }

    if (exprA->dtype == exprB->dtype &&
        exprA->dno == exprB->dno &&
        strcmp(exprA->query, exprB->query) == 0 &&
        bms_equal(exprA->paramnos, exprB->paramnos) &&
        equalPLpgsqlFunc(exprA->func, exprB->func) &&
        equalPLpgNsitem(exprA->ns, exprB->ns) &&
        exprA->isouttype == exprB->isouttype) {
        return true;
    }

    return false;
}

static PLpgSQL_expr* copyPLpgsqlExpr(PLpgSQL_expr* srcExpr)
{
    if (srcExpr == NULL) {
        return NULL;
    }
    PLpgSQL_expr* destExpr = (PLpgSQL_expr*)palloc0(sizeof(PLpgSQL_expr));

    destExpr->dtype = srcExpr->dtype;
    destExpr->dno = srcExpr->dno;
    destExpr->ispkg = srcExpr->ispkg;
    destExpr->query = pstrdup(srcExpr->query);
    destExpr->plan = NULL;
    destExpr->func = copyPLpgsqlFunc(srcExpr->func);
    destExpr->paramnos = bms_copy(srcExpr->paramnos);
    destExpr->out_param_dno = srcExpr->out_param_dno;

    destExpr->ns = copyPLpgNsitem(srcExpr->ns);
    destExpr->expr_simple_expr = NULL;
    destExpr->expr_simple_generation = 0;
    destExpr->expr_simple_type = InvalidOid;
    destExpr->expr_simple_need_snapshot = false;
    destExpr->expr_simple_state = NULL;
    destExpr->expr_simple_in_use = false;
    destExpr->expr_simple_lxid = 0;
    destExpr->isouttype = srcExpr->isouttype;
    destExpr->is_have_tableof_index_var = srcExpr->is_have_tableof_index_var;
    destExpr->tableof_var_dno = srcExpr->tableof_var_dno;
    destExpr->is_have_tableof_index_func = srcExpr->is_have_tableof_index_func;
    destExpr->tableof_func_dno = srcExpr->tableof_func_dno;

    return destExpr;
}

static bool equalPLpgsqlFunc(PLpgSQL_function* funcA, PLpgSQL_function* funcB)
{
    if (funcA == funcB) {
        return true;
    } else if (funcA == NULL || funcB == NULL) {
        return false;
    }

    if (funcA->fn_nargs == funcB->fn_nargs &&
        funcA->ndatums == funcB->ndatums) {
        /* equalPLpgEstate(funcA->cur_estate, funcB->cur_estate) */
        for (int i = 0; i < funcA->fn_nargs; i++) {
            if (funcA->fn_argvarnos[i] != funcB->fn_argvarnos[i]) {
                break;
            }
        }
        return true;
    }

    return false;
}

static PLpgSQL_function* copyPLpgsqlFunc(PLpgSQL_function* srcFunc)
{
    if (srcFunc == NULL) {
        return NULL;
    }

    PLpgSQL_function* destFunc = (PLpgSQL_function*)palloc(sizeof(PLpgSQL_function));

    destFunc->fn_signature = NULL;
    destFunc->fn_oid = srcFunc->fn_oid;
    destFunc->pkg_oid = srcFunc->pkg_oid;
    destFunc->fn_searchpath = NULL;
    destFunc->fn_owner = srcFunc->fn_owner;
    destFunc->fn_xmin = 0;
    destFunc->fn_tid = srcFunc->fn_tid;
    destFunc->is_private = srcFunc->is_private;
    destFunc->fn_is_trigger = srcFunc->fn_is_trigger;
    destFunc->fn_input_collation = srcFunc->fn_input_collation;
    destFunc->fn_hashkey = NULL; /* back-link to hashtable key */
    destFunc->fn_cxt = g_instance.adv_cxt.SQLAdvisorContext;

    destFunc->fn_rettype = srcFunc->fn_rettype;
    destFunc->fn_rettyplen = srcFunc->fn_rettyplen;
    destFunc->fn_retbyval = srcFunc->fn_retbyval;
    destFunc->fn_retinput = srcFunc->fn_retinput;
    destFunc->fn_rettypioparam = srcFunc->fn_rettypioparam;
    destFunc->fn_retistuple = srcFunc->fn_retistuple;
    destFunc->fn_retset = srcFunc->fn_retset;
    destFunc->fn_readonly = srcFunc->fn_readonly;
    destFunc->fn_nargs = srcFunc->fn_nargs;
    for (int i = 0; i < srcFunc->fn_nargs; i++) {
        destFunc->fn_argvarnos[i] = srcFunc->fn_argvarnos[i];
    }
    destFunc->out_param_varno = srcFunc->out_param_varno;
    destFunc->found_varno = srcFunc->found_varno;
    destFunc->sql_cursor_found_varno = srcFunc->sql_cursor_found_varno;
    destFunc->sql_notfound_varno = srcFunc->sql_notfound_varno;
    destFunc->sql_isopen_varno = srcFunc->sql_isopen_varno;
    destFunc->sql_rowcount_varno = srcFunc->sql_rowcount_varno;
    destFunc->sqlcode_varno = srcFunc->sqlcode_varno;
    destFunc->new_varno = srcFunc->new_varno;
    destFunc->old_varno = srcFunc->old_varno;
    destFunc->tg_name_varno = srcFunc->tg_name_varno;
    destFunc->tg_when_varno = srcFunc->tg_when_varno;
    destFunc->tg_level_varno = srcFunc->tg_level_varno;
    destFunc->tg_op_varno = srcFunc->tg_op_varno;
    destFunc->tg_relid_varno = srcFunc->tg_relid_varno;
    destFunc->tg_relname_varno = srcFunc->tg_relname_varno;
    destFunc->tg_table_name_varno = srcFunc->tg_table_name_varno;
    destFunc->tg_table_schema_varno = srcFunc->tg_table_schema_varno;
    destFunc->tg_nargs_varno = srcFunc->tg_nargs_varno;
    destFunc->tg_argv_varno = srcFunc->tg_argv_varno;
    destFunc->resolve_option = srcFunc->resolve_option;
    destFunc->invalItems = (List*)copyObject(srcFunc->invalItems);
    destFunc->ndatums = srcFunc->ndatums;
    destFunc->datums =(PLpgSQL_datum**)palloc(sizeof(PLpgSQL_datum*) * srcFunc->ndatums);
    for (int i = 0; i < srcFunc->ndatums; i++) {
        destFunc->datums[i] = deepCopyPlpgsqlDatum(srcFunc->datums[i]);
    }
    destFunc->action = NULL;
    destFunc->goto_labels = NIL;

    destFunc->cur_estate = copyPLpgEstate(srcFunc->cur_estate);
    destFunc->use_count = srcFunc->use_count;
    destFunc->pre_parse_trig = srcFunc->pre_parse_trig;
    destFunc->tg_relation = NULL;
    destFunc->is_plpgsql_func_with_outparam = srcFunc->is_plpgsql_func_with_outparam;

    return destFunc;
}

static bool equalPLpgNsitem(PLpgSQL_nsitem* nsA, PLpgSQL_nsitem* nsB)
{
    if (nsA == nsB) {
        return true;
    } else if (nsA == NULL || nsB == NULL) {
        return false;
    }

    if (nsA->itemno == nsB->itemno &&
        nsA->itemtype == nsB->itemtype &&
        strcmp(nsA->name, nsB->name) == 0)  {
        return equalPLpgNsitem(nsA->prev, nsB->prev);
    }

    return  false;
}

static PLpgSQL_nsitem* copyPLpgNsitem(PLpgSQL_nsitem* srcNs)
{
    if (srcNs == NULL) {
        return NULL;
    }

    PLpgSQL_nsitem* destNs = (PLpgSQL_nsitem*)palloc(offsetof(PLpgSQL_nsitem, name) +
                                                     (strlen(srcNs->name) + 1) * sizeof(char));
    destNs->itemno = srcNs->itemno;
    destNs->itemtype = srcNs->itemtype;
    destNs->pkgname = pstrdup(srcNs->pkgname);
    destNs->schemaName = pstrdup(srcNs->schemaName);
    destNs->prev = copyPLpgNsitem(srcNs->prev);
    errno_t rc = strncpy_s(destNs->name, strlen(srcNs->name) + 1, srcNs->name, strlen(srcNs->name));
    securec_check_c(rc, "\0", "\0");

    return destNs;
}

static PLpgSQL_execstate* copyPLpgEstate(PLpgSQL_execstate* srcEstate)
{
    if (srcEstate == NULL) {
        return NULL;
    }

    PLpgSQL_execstate* destEstate = (PLpgSQL_execstate*)palloc(sizeof(PLpgSQL_execstate));

    destEstate->func = NULL;
    destEstate->retval = (Datum)0;
    destEstate->retisnull = srcEstate->retisnull;
    destEstate->rettype = srcEstate->rettype;
    destEstate->paramval = (Datum)0;
    destEstate->paramisnull = srcEstate->paramisnull;
    destEstate->paramtype = srcEstate->paramtype;
    destEstate->fn_rettype = srcEstate->fn_rettype;
    destEstate->retistuple = srcEstate->retistuple;
    destEstate->retisset = srcEstate->retisset;
    destEstate->readonly_func = srcEstate->readonly_func;
    destEstate->rettupdesc = NULL;
    destEstate->paramtupdesc = NULL;
    destEstate->exitlabel = NULL;
    destEstate->cur_error = NULL;
    destEstate->tuple_store = NULL;
    destEstate->tuple_store_cxt = NULL;
    destEstate->tuple_store_owner = NULL;
    destEstate->rsi = NULL;
    destEstate->found_varno = srcEstate->found_varno;
    destEstate->rowcount = srcEstate->rowcount;
    destEstate->sql_cursor_found_varno = srcEstate->sql_cursor_found_varno;
    destEstate->sql_notfound_varno = srcEstate->sql_notfound_varno;
    destEstate->sql_isopen_varno = srcEstate->sql_isopen_varno;
    destEstate->sql_rowcount_varno = srcEstate->sql_rowcount_varno;
    destEstate->sqlcode_varno = srcEstate->sqlcode_varno;
    destEstate->ndatums = srcEstate->ndatums;
    destEstate->datums =(PLpgSQL_datum**)palloc(sizeof(PLpgSQL_datum*) * srcEstate->ndatums);
    for (int i = 0; i < srcEstate->ndatums; i++) {
        destEstate->datums[i] = deepCopyPlpgsqlDatum(srcEstate->datums[i]);
    }

    destEstate->eval_tuptable = NULL;
    destEstate->eval_processed = srcEstate->eval_processed;
    destEstate->eval_lastoid = srcEstate->eval_lastoid;
    destEstate->eval_econtext = NULL;
    destEstate->cur_expr = NULL;
    destEstate->err_stmt = NULL;
    destEstate->err_text = NULL;
    destEstate->plugin_info = NULL;
    destEstate->goto_labels = NIL;
    destEstate->goto_target_label = NULL;
    destEstate->goto_target_stmt = NULL; 
    destEstate->block_level = srcEstate->block_level;
    destEstate->cursor_return_data = NULL;
    destEstate->stack_entry_start = srcEstate->stack_entry_start;
    destEstate->curr_nested_table_type = 0;
    destEstate->is_exception = false;

    return destEstate;
}

static bool equalParamExternData(ParamExternData* prmA, ParamExternData* prmB)
{
    if (prmA->isnull == prmB->isnull &&
        prmA->pflags == prmB->pflags &&
        prmA->ptype == prmB->ptype) {
        if (prmA->isnull || !OidIsValid(prmA->ptype)) {
            return true;
        } else {
            int16 typLen;
            bool typByVal = false;
            get_typlenbyval(prmA->ptype, &typLen, &typByVal);

            if (datumIsEqual(prmA->value, prmB->value, typByVal, typLen)) {
                return true;
            }
        }
    }

    return false;
}

bool checkSPIPlan(SPIPlanPtr plan)
{
    if (plan == NULL || plan->plancache_list == NULL) {
        return false;
    }

    ListCell *lc1 = NULL;
    foreach (lc1, plan->plancache_list) {
        CachedPlanSource *plansource = (CachedPlanSource *)lfirst(lc1);
        if (plansource->raw_parse_tree && !checkParsetreeTag(plansource->raw_parse_tree)) {
            return false;
        }

        if (plansource->cplan != NULL) {
            CachedPlan* cplan = plansource->cplan;
            if (!checkPlan(cplan->stmt_list)) {
                return false;
            }
        } else if (plansource->gplan != NULL) {
            CachedPlan* gplan = plansource->gplan;
            if (!checkPlan(gplan->stmt_list)) {
                return false;
            }
        } else {
            return false;
        }
    }

    return true;
}

/* check if has relation, temp table, system table, funtion. */
void checkRtable(List* rtable, TableConstraint* tableConstraint)
{
    if (rtable == NULL) {
        return ;
    }
        
    ListCell* lc = NULL;
    foreach (lc, rtable) {
        RangeTblEntry* rte = (RangeTblEntry*)lfirst(lc);
        if (rte->rtekind == RTE_RELATION) {
            tableConstraint->isHasTable = true;
            char relPersistence = get_rel_persistence(rte->relid);
            if (relPersistence == RELPERSISTENCE_TEMP ||
                relPersistence == RELPERSISTENCE_GLOBAL_TEMP ||
                rte->relid < FirstNormalObjectId) {
                tableConstraint->isHasTempTable = true;
            }
        } else if (rte->rtekind == RTE_SUBQUERY) {
            if (rte->subquery != NULL) {
                checkRtable(rte->subquery->rtable, tableConstraint);
            }
        } else if (rte->rtekind == RTE_FUNCTION) {
            tableConstraint->isHasFunction = true;
        } 

        if (tableConstraint->isHasFunction || tableConstraint->isHasTempTable) {
            return ;
        }
    }
}

bool checkPlan(List* stmtList)
{
    if (stmtList == NULL) {
        return false;
    }

    ListCell *lc1 = NULL;
    foreach (lc1, stmtList) {
        PlannedStmt* plannedstmt = (PlannedStmt*)lfirst(lc1);
        if (plannedstmt == NULL) {
            return false;
        }

        TableConstraint tableConstraint;
        initTableConstraint(&tableConstraint);
        checkRtable(plannedstmt->rtable, &tableConstraint);
        if (!tableConstraint.isHasTable || tableConstraint.isHasTempTable || tableConstraint.isHasFunction) {
            return false;
        }

        if (!IsA(plannedstmt, PlannedStmt)) {
            return false; /* Ignore utility statements */
        }
    }
    return true;
}

void initTableConstraint(TableConstraint* tableConstraint)
{
    tableConstraint->isHasTable = false;
    tableConstraint->isHasTempTable = false;
    tableConstraint->isHasFunction = false;
}

/* check whether query tree has relation, temp table, system table adn function */
void checkQuery(List* querytreeList, TableConstraint* tableConstraint)
{
    if (querytreeList == NULL) {
        return ;
    }

    ListCell* lc = NULL;
    foreach (lc, querytreeList) {
        Query* query = castNode(Query, lfirst(lc));
        if (query->cteList != NULL) {
            ListCell* cteCell = NULL;
            foreach (cteCell, query->cteList) {
                CommonTableExpr* cte = (CommonTableExpr*)lfirst(cteCell);
                List* quertCteList = list_make1(cte->ctequery);
                checkQuery(quertCteList, tableConstraint);
                list_free_ext(quertCteList);
                if (tableConstraint->isHasFunction || tableConstraint->isHasTempTable) {
                    return ;
                }
            }
        }
        
        checkRtable(query->rtable, tableConstraint);
        if (tableConstraint->isHasFunction || tableConstraint->isHasTempTable) {
            return ;
        }
    }
}

static bool checkGlobalAdvMemSize()
{
    if (g_instance.adv_cxt.maxMemory > 0 && g_instance.adv_cxt.SQLAdvisorContext) {
        int64 totalsize = ((AllocSet)g_instance.adv_cxt.SQLAdvisorContext)->totalSpace;
        if ((int64)g_instance.adv_cxt.maxMemory * 1024 * 1024 >= totalsize) {
            return true;
        } else {
            ereport(DEBUG1, (errmodule(MOD_ADVISOR),
                errmsg("SQL Advisor collect out of memory")));
        }
    } else {
        ereport(DEBUG1, (errmodule(MOD_ADVISOR),
            errmsg("SQL Advisor collect memory not init")));
    }

    return false;
}

bool checkAdivsorState()
{
    if (pg_atomic_read_u32(&g_instance.adv_cxt.isOnlineRunning) == 1 &&
        g_instance.adv_cxt.currentDB == u_sess->proc_cxt.MyDatabaseId) {
        if (pg_atomic_read_u32((volatile uint32*)&g_instance.adv_cxt.maxsqlCount) <= 0) {
            ereport(DEBUG1, (errmodule(MOD_ADVISOR),
                errmsg("SQL Advisor collect number is out of range, please call end_collect_workload.")));
            return false;
        } 
        return checkGlobalAdvMemSize();
    }
    return false;
}
