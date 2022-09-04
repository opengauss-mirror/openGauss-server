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
 * sqladvisor.cpp
 *		sqladvisor support.
 * 
 *
 * IDENTIFICATION
 *      src/gausskernel/optimizer/commands/sqladvisor.cpp
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
#include "commands/sqladvisor.h"
#include "nodes/makefuncs.h"
#include "utils/typcache.h"
#include "access/hash.h"
#include "executor/lightProxy.h"
#include "pgxc/route.h"

#define TEXT_LENGTH(dat) (VARSIZE(dat) - VARHDRSZ)

#define NON_EMPTY_TEXT(dat)                                    \
    do {                                                       \
        if (TEXT_LENGTH(dat) == 0) {                           \
            ereport(ERROR,                                     \
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),     \
                    errmsg("invalid parameter"),               \
                    errdetail("Empty string isn't allowed.")));\
        }                                                      \
    } while (0)

#define NOT_NULL_ARG(n)                                         \
    do {                                                        \
        if (PG_ARGISNULL(n)) {                                  \
            ereport(ERROR,                                      \
                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),       \
                    errmsg("null value not allowed"),           \
                    errhint("%dth argument is NULL.", n + 1))); \
        }                                                       \
    } while (0)

#ifdef HAVE_INT64_TIMESTAMP
static int64 min2t(const int minute)
{
    return minute * SECS_PER_MINUTE * USECS_PER_SEC;
}
#else
static double min2t(const int minute)
{
    return minute * SECS_PER_MINUTE;
}
#endif

#define WEIGHT_LOW 0
#define WEIGHT_HIGH 1000
#define JOIN_WERIGHT 1.0
#define GROUPBY_WEIGHT 0.1
#define QUAL_WEIGHT 0.05

static void cleanAdvisorResult();
static AdviseQuery* initAdviseQuery(char* queryString, int64 fequency, ParamListInfo boundParams, int cursorOptions,
                                    AdviseSearchPath* searchPath);
static void initGlobalWorkloadCache();
static List* getPlan(AdviseQuery* adviseQuery, bool* isCNLightProxy);
static AnalyzedResultview* getTableInfoView(Oid relid, int* num);
static DistributionResultview* getDistributionInfoView(int* num);
static int SQLStmtKeyMatch(const void *left, const void *right, uint32 keysize);

static void checkAdvisorInit()
{
    if (u_sess->adv_cxt.adviseMode == AM_NONE) {
        ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("sqladvisor dosen't init")));
    }
}

static void checkEnv()
{
#ifndef ENABLE_MULTIPLE_NODES
    ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
            errmsg("opengauss not support sqladvisor")));
#endif    
    if (IS_PGXC_DATANODE) {
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
            errmsg("this function must be called on CN")));
    }
}

static void checkRight()
{
    if (!superuser()) {
        ereport(ERROR,(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), (
            errmsg("must be system admin to use this function"))));
    }
}

Datum init(PG_FUNCTION_ARGS)
{
    checkEnv();
    NOT_NULL_ARG(0);  // ''can't filter
    char kind = PG_GETARG_CHAR(0);
    bool isUseCost = PG_GETARG_BOOL(1);
    bool isOnline = PG_GETARG_BOOL(2);
    bool isConstraintPrimaryKey = PG_GETARG_BOOL(3);
    int sqlCount = PG_GETARG_INT32(4);
    int maxMemory = PG_GETARG_INT32(5);

    if (u_sess->adv_cxt.adviseMode != AM_NONE) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("sqladvisor was initialized, please call clean() to reinitialize")));
    }

    if (kind != 'D') {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("we only support Distribution advise.")));
    }

    if (isOnline) {
        if (!superuser()) {
            ereport(ERROR,(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), (
                errmsg("must be system admin to use online model."))));
        }

        if (!OidIsValid(g_instance.adv_cxt.currentDB)) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("don't collect query.")));
        }

        if (g_instance.adv_cxt.currentDB != u_sess->proc_cxt.MyDatabaseId) {
            ereport(ERROR,(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), (
                errmsg("Online model must be under database %s",
                    get_and_check_db_name(g_instance.adv_cxt.currentDB, true)))));
        }

        if (isOnlineRunningOn()) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Global collect workload is running, should call end_collect_workload first.")));
        }
    } else if (!isOnline && g_instance.adv_cxt.currentUser == u_sess->misc_cxt.CurrentUserId) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("Current user run online collection, only support online model.")));
    }

    if (sqlCount <= 0 || sqlCount > 100000) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("sqlCount %d must 0 < sqlCount <= 100000.", sqlCount)));
    }

    if (maxMemory <= 0 || maxMemory > 10240) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("maxMemory %d must 0 < maxMemory <= 10240 MB.", maxMemory)));
    }

    if (isUseCost) {
        u_sess->adv_cxt.adviseMode = AM_COST;
        u_sess->adv_cxt.adviseCostMode = ACM_TOTALCOSTLOW;
        u_sess->adv_cxt.adviseCompressLevel = ACL_HIGH;
    } else {
        u_sess->adv_cxt.adviseMode = AM_HEURISITICS;
    }

    u_sess->adv_cxt.isOnline = isOnline;
    u_sess->adv_cxt.maxMemory = maxMemory;
    u_sess->adv_cxt.maxsqlCount = sqlCount;
    u_sess->adv_cxt.isConstraintPrimaryKey = isConstraintPrimaryKey;
    u_sess->adv_cxt.isCostModeRunning = false;
    u_sess->adv_cxt.currGroupIndex = 0;
    u_sess->adv_cxt.joinWeight = JOIN_WERIGHT;
    u_sess->adv_cxt.groupbyWeight = GROUPBY_WEIGHT;
    u_sess->adv_cxt.qualWeight = QUAL_WEIGHT;
    u_sess->adv_cxt.startTime = 0;
    u_sess->adv_cxt.maxTime = -1;
    u_sess->adv_cxt.endTime = 0;
    u_sess->adv_cxt.candicateQueries = NIL;
    u_sess->adv_cxt.candicateAdviseGroups = NIL;
    u_sess->adv_cxt.result = NIL;

    if (u_sess->adv_cxt.SQLAdvisorContext == NULL) {
        u_sess->adv_cxt.SQLAdvisorContext = AllocSetContextCreate(u_sess->top_mem_cxt,
            "SQLAdivsorContext",
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE);
    }

    initCandicateTables(u_sess->adv_cxt.SQLAdvisorContext, &u_sess->adv_cxt.candicateTables);

    PG_RETURN_BOOL(true);
}

Datum set_cost_params(PG_FUNCTION_ARGS)
{
    checkEnv();
    int maxTime = PG_GETARG_INT32(0);
    bool isLowestCost = PG_GETARG_BOOL(1);
    text* cl = PG_GETARG_TEXT_P(2);

    NON_EMPTY_TEXT(cl);
    char* compressLevel = text_to_cstring(cl);

    checkAdvisorInit();
    if (!u_sess->adv_cxt.adviseMode == AM_COST) {
        ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("sqladvisor dosen't use cost")));
    }

    if (isLowestCost) {
        u_sess->adv_cxt.adviseCostMode = ACM_TOTALCOSTLOW;
    } else {
        u_sess->adv_cxt.adviseCostMode = ACM_MEDCOSTLOW;
    }
    if (strcmp(compressLevel, "high") == 0) {
        u_sess->adv_cxt.adviseCompressLevel = ACL_HIGH;
    } else if (strcmp(compressLevel, "med") == 0) {
        u_sess->adv_cxt.adviseCompressLevel = ACL_MED;
    } else if (strcmp(compressLevel, "low") == 0) {
        u_sess->adv_cxt.adviseCompressLevel = ACL_LOW;
    } else {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("unrecognized advise compress Level: %s", compressLevel)));
    }

    if (maxTime > 0) {
        u_sess->adv_cxt.maxTime = min2t(maxTime);
    } else {
        u_sess->adv_cxt.maxTime = -1;
    }
    
    PG_RETURN_BOOL(true);
}

Datum set_weight_params(PG_FUNCTION_ARGS)
{
    checkEnv();
    checkAdvisorInit();
    float joinWeight = PG_GETARG_FLOAT4(0);
    float groupbyWeight = PG_GETARG_FLOAT4(1);
    float qualWeight = PG_GETARG_FLOAT4(2);
    
    if (joinWeight < WEIGHT_LOW || joinWeight > WEIGHT_HIGH) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("join weight %f must 0 <= weight <= 1000.", joinWeight)));
    }
    if (groupbyWeight < WEIGHT_LOW || groupbyWeight > WEIGHT_HIGH) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("groupby weight %f must 0 <= weight <= 1000.", groupbyWeight)));
    }
    if (qualWeight < WEIGHT_LOW || qualWeight > WEIGHT_HIGH) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("qual weight %f must 0 <= weight <= 1000.", qualWeight)));
    }

    u_sess->adv_cxt.joinWeight = joinWeight;
    u_sess->adv_cxt.groupbyWeight = groupbyWeight;
    u_sess->adv_cxt.qualWeight = qualWeight;
    PG_RETURN_BOOL(true);
}

/* this function is strict */
Datum assign_table_type(PG_FUNCTION_ARGS)
{
    checkEnv();
    checkAdvisorInit();
    text* tlein = PG_GETARG_TEXT_P(0);    

    if (u_sess->adv_cxt.candicateQueries != NIL) {
        ereport(ERROR, (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
            errmsg("assign table type must before analyze query.")));
    }

    List* nameList = textToQualifiedNameList(tlein);
    RangeVar* table = makeRangeVarFromNameList(nameList);
    Oid relid = RangeVarGetRelid(table, NoLock, false);
    char relPersistence = get_rel_persistence(relid);
    if (relPersistence == RELPERSISTENCE_TEMP || relPersistence == RELPERSISTENCE_GLOBAL_TEMP) {
        ereport(ERROR, (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
            errmsg("only support regular table.")));
    }
    /* filter system table */
    if (relid < FirstNormalObjectId) {
        ereport(ERROR, (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
            errmsg("don't support system table.")));
    }

    Relation targetTable = relation_open(relid, NoLock);
    bool found = true;
    MemoryContext oldcxt = MemoryContextSwitchTo(u_sess->adv_cxt.SQLAdvisorContext);
    AdviseTable* adviseTable = (AdviseTable*)hash_search(
        u_sess->adv_cxt.candicateTables, (void*)&(relid), HASH_ENTER, &found);

    /* If the same table is called twice, it will be reinitialized. */
    adviseTable->oid = relid;
    adviseTable->tableName = pstrdup(table->relname);
    adviseTable->totalCandidateAttrs = NIL;
    if(IS_PGXC_COORDINATOR && PointerIsValid(targetTable->rd_locator_info)) {
        adviseTable->originDistributionKey = (List*)copyObject(targetTable->rd_locator_info->partAttrNum);
    } else {
        adviseTable->originDistributionKey = NIL;
    }
    adviseTable->currDistributionKey = NULL;
    adviseTable->tableGroupOffset = 0;
    adviseTable->selecteCandidateAttrs = NIL;
    adviseTable->marked = false;
    adviseTable->isAssignRelication = true;
    adviseTable->enableAdvise = true;

    (void)MemoryContextSwitchTo(oldcxt);
    relation_close(targetTable, NoLock);

    PG_RETURN_BOOL(true);
}

Datum clean(PG_FUNCTION_ARGS)
{
    checkEnv();
    checkAdvisorInit();
    u_sess->adv_cxt.adviseMode = AM_NONE;
    u_sess->adv_cxt.adviseType = AT_NONE;
    u_sess->adv_cxt.adviseCostMode = ACM_NONE;
    u_sess->adv_cxt.adviseCompressLevel = ACL_NONE;
    u_sess->adv_cxt.isOnline = false;
    u_sess->adv_cxt.isCostModeRunning = false;
    u_sess->adv_cxt.currGroupIndex = 0;
    u_sess->adv_cxt.joinWeight = 0;
    u_sess->adv_cxt.groupbyWeight = 0;
    u_sess->adv_cxt.qualWeight = 0;
    u_sess->adv_cxt.maxTime = 0;
    u_sess->adv_cxt.startTime = 0;
    u_sess->adv_cxt.endTime = 0;
    u_sess->adv_cxt.maxMemory = 0;
    u_sess->adv_cxt.maxsqlCount = 0;

    u_sess->adv_cxt.candicateTables = NULL;
    u_sess->adv_cxt.candicateQueries = NIL;
    u_sess->adv_cxt.candicateAdviseGroups = NIL;
    u_sess->adv_cxt.result = NIL;

    MemoryContextDelete(u_sess->adv_cxt.SQLAdvisorContext);
    u_sess->adv_cxt.SQLAdvisorContext = NULL;
    PG_RETURN_BOOL(true);
}

Datum clean_workload(PG_FUNCTION_ARGS)
{
    checkEnv();
    checkRight();
    if (isOnlineRunningOn()) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("Global collect workload is running, can't clean workload.")));
    }

    if (isUsingGWCOn()) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("Global workload is using, can't clean workload.")));
    }
    (void)LWLockAcquire(SQLAdvisorLock, LW_EXCLUSIVE);
    g_instance.adv_cxt.isOnlineRunning = 0;
    g_instance.adv_cxt.isUsingGWC = 0;
    g_instance.adv_cxt.maxMemory = 0;
    g_instance.adv_cxt.maxsqlCount = 0;
    g_instance.adv_cxt.currentUser = InvalidOid;
    g_instance.adv_cxt.currentDB = InvalidOid;
    g_instance.adv_cxt.GWCArray = NULL;
    if (g_instance.adv_cxt.SQLAdvisorContext != NULL) {
        MemoryContextDelete(g_instance.adv_cxt.SQLAdvisorContext);
        g_instance.adv_cxt.SQLAdvisorContext = NULL;
    }
    LWLockRelease(SQLAdvisorLock);

    PG_RETURN_BOOL(true);
}

Datum analyze_query(PG_FUNCTION_ARGS)
{
    checkEnv();
    checkAdvisorInit();
    if (u_sess->adv_cxt.isOnline) {
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
            errmsg("online model don't support this function.")));
    }
    
    NOT_NULL_ARG(0);
    text* qs = PG_GETARG_TEXT_P(0);
    NON_EMPTY_TEXT(qs);
    char* queryString = text_to_cstring(qs);
    int fequency = PG_GETARG_INT32(1);
    if (list_length(u_sess->adv_cxt.candicateQueries) >= u_sess->adv_cxt.maxsqlCount) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Current sql count is out of max count.")));
    }

    AdviseQuery* adviseQuery = initAdviseQuery(queryString, fequency, NULL, 0, NULL);
    MemoryContext oldcxt = MemoryContextSwitchTo(u_sess->adv_cxt.SQLAdvisorContext);
    u_sess->adv_cxt.candicateQueries = lcons(adviseQuery, u_sess->adv_cxt.candicateQueries);
    (void)MemoryContextSwitchTo(oldcxt);

    Cost cost = 0.0;
    analyzeQuery(adviseQuery, true, &cost);
    pfree_ext(queryString);
    PG_RETURN_BOOL(true);
}

Datum get_analyzed_result(PG_FUNCTION_ARGS)
{
    checkEnv();
    checkAdvisorInit();

    FuncCallContext* funcctx = NULL;
    int num = 0;
    text* tableText = PG_GETARG_TEXT_P(0);
    List* nameList = textToQualifiedNameList(tableText);
    RangeVar* table = makeRangeVarFromNameList(nameList);

    Oid relid = RangeVarGetRelid(table, NoLock, false);

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext oldcontext;
        TupleDesc tupdesc;
        funcctx = SRF_FIRSTCALL_INIT();
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

#define SQLADVISOR_ANALYZED_TUPLES_ATTR_NUM 5

        tupdesc = CreateTemplateTupleDesc(SQLADVISOR_ANALYZED_TUPLES_ATTR_NUM, false);
        TupleDescInitEntry(tupdesc, (AttrNumber) 1, "schema_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 2, "table_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 3, "col_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 4, "operator", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 5, "count", INT4OID, -1, 0);
        funcctx->tuple_desc = BlessTupleDesc(tupdesc);
        funcctx->user_fctx = getTableInfoView(relid, &num);
        funcctx->max_calls = num;

        (void)MemoryContextSwitchTo(oldcontext);

        if (funcctx->user_fctx == NULL)
            SRF_RETURN_DONE(funcctx);
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();
    AnalyzedResultview *entry = (AnalyzedResultview *)funcctx->user_fctx;

    if (funcctx->call_cntr < funcctx->max_calls) {
    	/* do when there is more left to send */
        Datum values[SQLADVISOR_ANALYZED_TUPLES_ATTR_NUM];
        bool nulls[SQLADVISOR_ANALYZED_TUPLES_ATTR_NUM];
        HeapTuple tuple;

        errno_t rc = 0;
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        entry += funcctx->call_cntr;
        values[0] = CStringGetTextDatum(entry->schemaName);
        values[1] = CStringGetTextDatum(entry->tableName);
        values[2] = CStringGetTextDatum(entry->attrName);
        values[3] = CStringGetTextDatum(entry->operatorName);
        values[4] = Int32GetDatum(entry->count);
        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    } else {
        /* do when there is no more left */
        SRF_RETURN_DONE(funcctx);
    }
}

Datum get_distribution_key(PG_FUNCTION_ARGS)
{
    checkEnv();
    checkAdvisorInit();
    if (!u_sess->adv_cxt.result) {
        ereport(ERROR, (errmsg("Don't have result")));
    }

    FuncCallContext* funcctx = NULL;
    int num = 0;

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext oldcontext;
        TupleDesc tupdesc;
        funcctx = SRF_FIRSTCALL_INIT();
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

#define SQLADVISOR_DISTRIBUTION_TUPLES_ATTR_NUM 9

        tupdesc = CreateTemplateTupleDesc(SQLADVISOR_DISTRIBUTION_TUPLES_ATTR_NUM, false);
        TupleDescInitEntry(tupdesc, (AttrNumber) 1, "db_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 2, "schema_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 3, "table_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 4, "distribution_type", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 5, "distributionKey", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 6, "start_time", TIMESTAMPTZOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 7, "end_time", TIMESTAMPTZOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 8, "cost_improve", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 9, "comment", TEXTOID, -1, 0);
       
        funcctx->tuple_desc = BlessTupleDesc(tupdesc);
        funcctx->user_fctx = getDistributionInfoView(&num);
        funcctx->max_calls = num;

        (void)MemoryContextSwitchTo(oldcontext);

        if (funcctx->user_fctx == NULL)
            SRF_RETURN_DONE(funcctx);
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();
    DistributionResultview *entry = (DistributionResultview *)funcctx->user_fctx;

    if (funcctx->call_cntr < funcctx->max_calls) {
        /* do when there is more left to send */
        Datum values[SQLADVISOR_DISTRIBUTION_TUPLES_ATTR_NUM];
        bool nulls[SQLADVISOR_DISTRIBUTION_TUPLES_ATTR_NUM];
        HeapTuple tuple;

        errno_t rc = 0;
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        entry += funcctx->call_cntr;
        values[0] = CStringGetTextDatum(entry->dbName);
        if (entry->schemaName != NULL) {
            values[1] = CStringGetTextDatum(entry->schemaName);
        } else {
            nulls[1] = true;
        }
        if (entry->tableName != NULL) {
            values[2] = CStringGetTextDatum(entry->tableName);
        } else {
            nulls[2] = true;
        }
        values[3] = CStringGetTextDatum(entry->distributioType);
        if (entry->distributionKey != NULL) {
            values[4] = CStringGetTextDatum(entry->distributionKey);
        } else {
            nulls[4] = true;
        }
        values[5] = TimestampGetDatum(entry->startTime);
        values[6] = TimestampGetDatum(entry->endTime);
        if (entry->costImpove > 0.0) {
            errno_t rc;
            char* cp = (char*)palloc0(MAXDATELEN);
            rc = sprintf_s(cp, MAXDATELEN, "%.4lf", entry->costImpove);
            securec_check_ss(rc, "\0", "\0");
            values[7] = CStringGetTextDatum(cp);
        } else {
            nulls[7] = true;
        }
        if (entry->comment != NULL) {
            values[8] = CStringGetTextDatum(entry->comment);
        } else {
            nulls[8] = true;
        }
        
        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    } else {
        /* do when there is no more left */
        SRF_RETURN_DONE(funcctx);
    }
}

Datum run(PG_FUNCTION_ARGS)
{
    checkEnv();
    checkAdvisorInit();

    if (u_sess->adv_cxt.candicateQueries == NIL ||
        u_sess->adv_cxt.candicateTables == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("There is not query or table, call analyze_query() first")));
    }

    u_sess->adv_cxt.startTime = GetCurrentTimestamp();
    if (u_sess->adv_cxt.maxTime > 0) {
        u_sess->adv_cxt.endTime = u_sess->adv_cxt.startTime + u_sess->adv_cxt.maxTime;
    }
    
    cleanAdvisorResult();
    MemoryContext advisorRunContext = AllocSetContextCreate(u_sess->adv_cxt.SQLAdvisorContext,
        "advisorRunContext",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);
    MemoryContext oldcxt = MemoryContextSwitchTo(advisorRunContext);
    bool result = false;
    PG_TRY();
    {
        if (u_sess->adv_cxt.adviseMode == AM_HEURISITICS) {
            result = runWithHeuristicMethod();
        } else if (u_sess->adv_cxt.adviseMode == AM_COST) {
            result = runWithCostMethod();
        }

        (void)MemoryContextSwitchTo(oldcxt);
        MemoryContextDelete(advisorRunContext);
    }
    PG_CATCH();
    {
        (void)MemoryContextSwitchTo(oldcxt);
        MemoryContextDelete(advisorRunContext);
        /* maybe generated some results. */
        cleanAdvisorResult();
        u_sess->adv_cxt.candicateAdviseGroups = NIL;
        u_sess->adv_cxt.isCostModeRunning = false;
        PG_RE_THROW();
    }
    PG_END_TRY();
    PG_RETURN_BOOL(result);
}

static void cleanAdvisorResult()
{
    if (u_sess->adv_cxt.result == NULL) {
        return;
    }
    ListCell* cell = NULL;
    foreach (cell, u_sess->adv_cxt.result) {
        DistributionResultview* resultView = (DistributionResultview*)lfirst(cell);
        pfree_ext(resultView->distributionKey);
        pfree_ext(resultView->comment);
        pfree_ext(resultView->dbName);
        pfree_ext(resultView->tableName);
        pfree_ext(resultView->schemaName);
        pfree_ext(resultView->distributioType);
    }

    list_free_deep(u_sess->adv_cxt.result);
    u_sess->adv_cxt.result = NIL;
}

static DistributionResultview* getDistributionInfoView(int* num)
{
    *num = list_length(u_sess->adv_cxt.result);
    uint32 index = 0;
    ListCell* cell = NULL;

    DistributionResultview* viewArray = (DistributionResultview*)palloc0(*num * sizeof(DistributionResultview));

    foreach (cell, u_sess->adv_cxt.result) {
        DistributionResultview* resultView = (DistributionResultview*)lfirst(cell);
        viewArray[index].dbName = resultView->dbName;
        viewArray[index].schemaName = resultView->schemaName;
        viewArray[index].tableName = resultView->tableName;
        viewArray[index].distributioType = resultView->distributioType;
        viewArray[index].distributionKey = resultView->distributionKey;
        viewArray[index].startTime = resultView->startTime;
        viewArray[index].endTime = resultView->endTime;
        viewArray[index].costImpove = resultView->costImpove;
        viewArray[index].comment = resultView->comment;
        index += 1;
    }

    return viewArray;
}

static AnalyzedResultview* getTableInfoView(Oid relid, int* num)
{
    AdviseTable* adviseTable = (AdviseTable*)hash_search(
        u_sess->adv_cxt.candicateTables, (void*)&(relid), HASH_FIND, NULL);
    if (adviseTable == NULL) {
        ereport(WARNING, (errmsg("Table %s was not anaylzed", get_rel_name(relid))));
        return NULL;
    }
    
    if (adviseTable->totalCandidateAttrs == NIL) {
        ereport(WARNING, (errmsg("Table %s don't have candidate attrs", get_rel_name(relid))));
    }

    *num = list_length(adviseTable->totalCandidateAttrs) * 3;
    uint32 index = 0;

    AnalyzedResultview* viewArray = (AnalyzedResultview*)palloc0(*num * sizeof(AnalyzedResultview));

    VirtualAttrGroupInfo* attrInfo = NULL;
    ListCell* cell = NULL;

    foreach(cell, adviseTable->totalCandidateAttrs) {
        attrInfo = (VirtualAttrGroupInfo*)lfirst(cell);

        viewArray[index].schemaName = "";
        viewArray[index].tableName = "";
        viewArray[index].attrName = concatAttrNames(attrInfo->attrNames, attrInfo->natts);
        viewArray[index].operatorName = "join";
        viewArray[index].count = attrInfo->joinCount;
        index++;

        viewArray[index].schemaName = "";
        viewArray[index].tableName = "";
        viewArray[index].attrName = "";
        viewArray[index].operatorName = "group by";
        viewArray[index].count = attrInfo->groupbyCount;

        index++;
        viewArray[index].schemaName = "";
        viewArray[index].tableName = "";
        viewArray[index].attrName = "";
        viewArray[index].operatorName = "qual";
        viewArray[index].count = attrInfo->qualCount;

        index++;
    }
    /* schema name and table name only shown on the first line */
    viewArray[0].schemaName = get_namespace_name(get_rel_namespace(adviseTable->oid));
    viewArray[0].tableName = pstrdup(adviseTable->tableName);

    return viewArray;
}

Datum start_collect_workload(PG_FUNCTION_ARGS)
{
    checkEnv();
    checkRight();
    int sqlCount = PG_GETARG_INT32(0);
    int maxMemory = PG_GETARG_INT32(1);

    if (sqlCount <= 0 || sqlCount > 100000) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("sqlCount %d must 0 < sqlCount <= 100000.", sqlCount)));
    }

    if (maxMemory <= 0 || maxMemory > 10240) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("maxMemory %d must 0 < maxMemory <= 10240.", maxMemory)));
    }

    (void)LWLockAcquire(SQLAdvisorLock, LW_EXCLUSIVE);
    if (g_instance.adv_cxt.SQLAdvisorContext != NULL) {
        LWLockRelease(SQLAdvisorLock);
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
            errmsg("call clean_workload first.")));
    }

    if (pg_atomic_exchange_u32(&g_instance.adv_cxt.isOnlineRunning, 1) != 0) {
        LWLockRelease(SQLAdvisorLock);
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
            errmsg("Online collect workload is running.")));
    }

    g_instance.adv_cxt.maxMemory = maxMemory;
    g_instance.adv_cxt.maxsqlCount = sqlCount;
    g_instance.adv_cxt.currentUser = u_sess->misc_cxt.CurrentUserId;
    g_instance.adv_cxt.currentDB = u_sess->proc_cxt.MyDatabaseId;
    g_instance.adv_cxt.SQLAdvisorContext = AllocSetContextCreate(g_instance.instance_context,
                                                                "workload SQLAdivsorContext",
                                                                ALLOCSET_DEFAULT_MINSIZE,
                                                                ALLOCSET_DEFAULT_INITSIZE,
                                                                ALLOCSET_DEFAULT_MAXSIZE,
                                                                SHARED_CONTEXT,
                                                                DEFAULT_MEMORY_CONTEXT_MAX_SIZE,
                                                                false);

    initGlobalWorkloadCache();
    LWLockRelease(SQLAdvisorLock);
    PG_RETURN_BOOL(true);
}

static void initGlobalWorkloadCache()
{
    MemoryContext oldcxt = MemoryContextSwitchTo(g_instance.adv_cxt.SQLAdvisorContext);
    HASHCTL ctl;
    errno_t rc;
    rc = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
    securec_check(rc, "\0", "\0");
    ctl.keysize = sizeof(SQLStatementKey);
    ctl.entrysize = sizeof(SQLStatementEntry);
    ctl.hash = (HashValueFunc)SQLStmtHashFunc;
    ctl.match = (HashCompareFunc)SQLStmtKeyMatch;

    int flags = HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT | HASH_COMPARE | HASH_EXTERN_CONTEXT | HASH_NOEXCEPT;

    g_instance.adv_cxt.GWCArray = (GWCHashCtl *) MemoryContextAllocZero(g_instance.adv_cxt.SQLAdvisorContext,
                                                        sizeof(GWCHashCtl) * GWC_NUM_OF_BUCKETS); 
    for (int i = 0; i < GWC_NUM_OF_BUCKETS; i++) {
        g_instance.adv_cxt.GWCArray[i].lockId = FirstGWCMappingLock + i;
        g_instance.adv_cxt.GWCArray[i].context = AllocSetContextCreate(g_instance.adv_cxt.SQLAdvisorContext,
                                                                        "GWC_Bucket_Context",
                                                                        ALLOCSET_DEFAULT_MINSIZE,
                                                                        ALLOCSET_DEFAULT_INITSIZE,
                                                                        ALLOCSET_DEFAULT_MAXSIZE,
                                                                        SHARED_CONTEXT);
        ctl.hcxt = g_instance.adv_cxt.GWCArray[i].context;
        g_instance.adv_cxt.GWCArray[i].hashTbl = hash_create("Global_Workload_Cache", GWC_HTAB_SIZE, &ctl, flags);
    }
     
    (void)MemoryContextSwitchTo(oldcxt);
}

static int SQLStmtKeyMatch(const void *left, const void *right, uint32 keysize)
{
    SQLStatementKey* leftItem = (SQLStatementKey*)left;
    SQLStatementKey* rightItem = (SQLStatementKey*)right;
    Assert(NULL != leftItem);
    Assert(NULL != rightItem);

    /* we just care whether the result is 0 or not. */
    if (leftItem->querylength != rightItem->querylength) {
        return 1;
    }

    if(strncmp(leftItem->queryString, rightItem->queryString, leftItem->querylength)) {
        return 1;
    }

    return 0;
}

uint32 SQLStmtHashFunc(const void *key, Size keysize)
{
    const SQLStatementKey* item = (const SQLStatementKey*) key;
    uint32 val1 = DatumGetUInt32(hash_any((const unsigned char *)item->queryString, item->querylength));

    return val1;
}

Datum end_collect_workload(PG_FUNCTION_ARGS)
{
    checkEnv();
    checkRight();

    if (pg_atomic_exchange_u32(&g_instance.adv_cxt.isOnlineRunning, 0) != 1) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("Online collect workload is not running.")));
    }

    PG_RETURN_BOOL(true);
}

Datum analyze_workload(PG_FUNCTION_ARGS)
{
    checkEnv();
    checkRight();
    if(!u_sess->adv_cxt.isOnline) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("init parameter is not online.")));
    }

    if (pg_atomic_exchange_u32(&g_instance.adv_cxt.isUsingGWC, 1) != 0) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("Global workload is using, call this function after collect.")));
    }
    
    /* check right and init or not */
    u_sess->adv_cxt.candicateQueries = NIL;
    ListCell* cell = NULL;
    AdviseQuery* adviseQuery = NULL;
    List* queryList = NIL;
    MemoryContext oldcxt = MemoryContextSwitchTo(u_sess->adv_cxt.SQLAdvisorContext);

    PG_TRY();
    {
        (void)LWLockAcquire(SQLAdvisorLock, LW_SHARED);
        for (int i = 0; i < GWC_NUM_OF_BUCKETS; i++) {
            HASH_SEQ_STATUS hashSeq;
            SQLStatementEntry* entry = NULL;

            LWLockAcquire(GetMainLWLockByIndex(g_instance.adv_cxt.GWCArray[i].lockId), LW_SHARED);
            hash_seq_init(&hashSeq, g_instance.adv_cxt.GWCArray[i].hashTbl);
            while ((entry = (SQLStatementEntry*)hash_seq_search(&hashSeq)) != NULL) {
                foreach (cell, entry->paramList) {
                    SQLStatementParam* stmtParam = (SQLStatementParam*)lfirst(cell);
                    adviseQuery = initAdviseQuery(entry->key.queryString, stmtParam->freqence, stmtParam->boundParams,
                        stmtParam->cursorOptions, stmtParam->searchPath);
                    queryList = lappend(queryList, adviseQuery);
                }
            }

            LWLockRelease(GetMainLWLockByIndex(g_instance.adv_cxt.GWCArray[i].lockId));
        }
        LWLockRelease(SQLAdvisorLock);
    }
    PG_CATCH();
    {
        pg_atomic_write_u32(&g_instance.adv_cxt.isUsingGWC, 0);
        (void)MemoryContextSwitchTo(oldcxt);
        LWLockRelease(SQLAdvisorLock);
        PG_RE_THROW();
    }
    PG_END_TRY();
    pg_atomic_write_u32(&g_instance.adv_cxt.isUsingGWC, 0);

    if (queryList == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("There is not query to analyze.")));
    }

    PG_TRY();
    {
        foreach (cell, queryList) {
            CHECK_FOR_INTERRUPTS();
            adviseQuery = (AdviseQuery*)lfirst(cell);
            u_sess->adv_cxt.candicateQueries = lcons(adviseQuery, u_sess->adv_cxt.candicateQueries);
            Cost cost = 0.0;
            analyzeQuery(adviseQuery, true, &cost);
        }
        (void)MemoryContextSwitchTo(oldcxt);
        ereport(NOTICE, (errmsg("total collect %d queries, success %d queries", list_length(queryList),
            list_length(u_sess->adv_cxt.candicateQueries))));
        pfree_ext(queryList);
    }
    PG_CATCH();
    {
        (void)MemoryContextSwitchTo(oldcxt);
        pfree_ext(queryList);
        /* 
         * Whether analyze some query report error or receive cancel signal,
         * we both clean and assign the new context.
        */
        MemoryContextDelete(u_sess->adv_cxt.SQLAdvisorContext);
        u_sess->adv_cxt.candicateTables = NULL;
        u_sess->adv_cxt.candicateQueries = NIL;
        u_sess->adv_cxt.candicateAdviseGroups = NIL;
        u_sess->adv_cxt.result = NIL;
        u_sess->adv_cxt.SQLAdvisorContext = AllocSetContextCreate(u_sess->top_mem_cxt,
            "SQLAdivsorContext",
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE);
        initCandicateTables(u_sess->adv_cxt.SQLAdvisorContext, &u_sess->adv_cxt.candicateTables);
        PG_RE_THROW();
    }
    PG_END_TRY();
    PG_RETURN_BOOL(true);
}

static AdviseQuery* initAdviseQuery(char* queryString, int64 fequency, ParamListInfo boundParams, int cursorOptions,
                                    AdviseSearchPath* searchPath)
{
    AdviseQuery* adviseQuery = (AdviseQuery*)MemoryContextAlloc(
        u_sess->adv_cxt.SQLAdvisorContext, sizeof(AdviseQuery));
    MemoryContext oldcxt = MemoryContextSwitchTo(u_sess->adv_cxt.SQLAdvisorContext);

    adviseQuery->query = pstrdup(queryString);
    adviseQuery->boundParams = copyDynParam(boundParams);
    adviseQuery->cursorOptions = cursorOptions;
    adviseQuery->searchPath = copyAdviseSearchPath(searchPath);
    adviseQuery->frequence = fequency;
    adviseQuery->originCost = 0.0;
    adviseQuery->tableOids = NIL;
    adviseQuery->referencedTables = NULL;
    adviseQuery->virtualTableGroupInfos = NIL;
    adviseQuery->isUse = true;

    (void)MemoryContextSwitchTo(oldcxt);
    checkSessAdvMemSize();
    return adviseQuery;
}

AdviseSearchPath* copyAdviseSearchPath(AdviseSearchPath* src)
{
    if (src == NULL) {
        return NULL;
    }
    AdviseSearchPath* dest = (AdviseSearchPath*)palloc(sizeof(AdviseSearchPath));
    dest->activeSearchPath = list_copy(src->activeSearchPath);
    dest->activeCreationNamespace = src->activeCreationNamespace;
    dest->activeTempCreationPending = src->activeTempCreationPending;
    dest->baseSearchPath = list_copy(src->baseSearchPath);
    dest->baseCreationNamespace = src->baseCreationNamespace;
    dest->baseTempCreationPending = src->baseTempCreationPending;
    dest->namespaceUser = src->namespaceUser;
    dest->baseSearchPathValid = src->baseSearchPathValid;
    dest->overrideStack = copyOverrideStack(src->overrideStack);
    dest->overrideStackValid = src->overrideStackValid;

    return dest;
}

static void initAdviseSearchPath(AdviseSearchPath* sp)
{
    sp->activeSearchPath = NIL;
    sp->baseSearchPath = NIL;
    sp->activeCreationNamespace = InvalidOid;
    sp->activeTempCreationPending = false;
    sp->baseCreationNamespace = InvalidOid;
    sp->baseTempCreationPending = false;
    sp->namespaceUser = InvalidOid;
    sp->baseSearchPathValid = false;
    sp->overrideStack = NIL;
    sp->overrideStackValid = false;
}

static void saveSearchPath(AdviseSearchPath* oldSp)
{
    oldSp->activeSearchPath = u_sess->catalog_cxt.activeSearchPath;
    oldSp->baseSearchPath = u_sess->catalog_cxt.baseSearchPath;
    oldSp->activeCreationNamespace = u_sess->catalog_cxt.activeCreationNamespace;
    oldSp->activeTempCreationPending = u_sess->catalog_cxt.activeTempCreationPending;
    oldSp->baseCreationNamespace = u_sess->catalog_cxt.baseCreationNamespace;
    oldSp->baseTempCreationPending = u_sess->catalog_cxt.baseTempCreationPending;
    oldSp->namespaceUser = u_sess->catalog_cxt.namespaceUser;
    oldSp->baseSearchPathValid = u_sess->catalog_cxt.baseSearchPathValid;
    oldSp->overrideStack = u_sess->catalog_cxt.overrideStack;
    oldSp->overrideStackValid = u_sess->catalog_cxt.overrideStackValid;
}

static void searchPathSwitchTo(AdviseSearchPath* sp)
{
    u_sess->catalog_cxt.activeSearchPath = sp->activeSearchPath;
    u_sess->catalog_cxt.baseSearchPath = sp->baseSearchPath;
    u_sess->catalog_cxt.activeCreationNamespace = sp->activeCreationNamespace;
    u_sess->catalog_cxt.activeTempCreationPending = sp->activeTempCreationPending;
    u_sess->catalog_cxt.baseCreationNamespace = sp->baseCreationNamespace;
    u_sess->catalog_cxt.baseTempCreationPending = sp->baseTempCreationPending;
    u_sess->catalog_cxt.namespaceUser = sp->namespaceUser;
    u_sess->catalog_cxt.baseSearchPathValid = sp->baseSearchPathValid;
    u_sess->catalog_cxt.overrideStack = sp->overrideStack;
    u_sess->catalog_cxt.overrideStackValid = sp->overrideStackValid;
}

static void fillinAdviseQueryTables(AdviseQuery* adviseQuery, List* rtable)
{
    ListCell* cell = NULL;
    foreach(cell, rtable) {
        RangeTblEntry* rte = (RangeTblEntry*)lfirst(cell);
        if (rte->rtekind == RTE_RELATION) {
            adviseQuery->tableOids = lappend_oid(adviseQuery->tableOids, rte->relid);
        }
    }
}

static void fillinAdviseQuery(PlannedStmt* plan, Cost cost)
{
    MemoryContext oldcxt = MemoryContextSwitchTo(u_sess->adv_cxt.SQLAdvisorContext);
    /* default current query is the top. */
    AdviseQuery* adviseQuery = (AdviseQuery*)linitial(u_sess->adv_cxt.candicateQueries);
    adviseQuery->originCost = cost;
    fillinAdviseQueryTables(adviseQuery, plan->rtable);
    (void)MemoryContextSwitchTo(oldcxt);
}

/*
 * call this function has 2 situations:
 * 1. from analyze_query/analyze_workload: analyze query component, if error, throw the error
 * 2. from runCombineWithCost: if only get plan's cost, throw the error.
 */
void analyzeQuery(AdviseQuery* adviseQuery, bool isFistAnalyze, Cost* cost)
{
    bool oldEnableFqs = u_sess->attr.attr_sql.enable_fast_query_shipping;
    u_sess->attr.attr_sql.enable_fast_query_shipping = false;
    AdviseSearchPath oldSearchPath;
    initAdviseSearchPath(&oldSearchPath);
    if (u_sess->adv_cxt.isOnline) {
        Assert(adviseQuery->searchPath != NULL);
        u_sess->adv_cxt.isJumpRecompute = true;
        saveSearchPath(&oldSearchPath);
        searchPathSwitchTo(adviseQuery->searchPath);
    }
    /* query parse, rewrite, plan context */
    MemoryContext advisorPlanContext = AllocSetContextCreate(u_sess->adv_cxt.SQLAdvisorContext,
        "advisorPlanContext",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);
    MemoryContext oldcxt = MemoryContextSwitchTo(advisorPlanContext);
    PG_TRY();
    {
        bool isCNLightProxy = false;
        List* planList = getPlan(adviseQuery, &isCNLightProxy);
        ListCell* cell = NULL;
        PlannedStmt* plan = NULL;
        Cost tempCost = 0.0;
        foreach (cell, planList) {
            plan = (PlannedStmt*)lfirst(cell);
            if (isCNLightProxy) {
                tempCost = 0.0;
            } else {
                tempCost += plan->planTree->startup_cost + plan->planTree->total_cost;
            }
            
            if (isFistAnalyze) {
                fillinAdviseQuery(plan, tempCost);
                extractNode((Plan*)plan->planTree, NIL, plan->rtable, plan->subplans);
            }
            
            check_plan_mergeinto_replicate(plan, ERROR);
        }
        *cost = tempCost;
        list_free_deep(planList);
        (void)MemoryContextSwitchTo(oldcxt);
        MemoryContextDelete(advisorPlanContext);
    }
    PG_CATCH();
    {
        (void)MemoryContextSwitchTo(oldcxt);
        MemoryContextDelete(advisorPlanContext);
        ErrorData* edata = CopyErrorData();
        FlushErrorState();
        if (isFistAnalyze) {
            /* delete query record */
            AdviseQuery* head_query = (AdviseQuery*)linitial(u_sess->adv_cxt.candicateQueries);
            if (strcmp(adviseQuery->query, head_query->query) == 0) {
                u_sess->adv_cxt.candicateQueries = list_delete_first(u_sess->adv_cxt.candicateQueries);
            }
        }
        
        if (u_sess->adv_cxt.isOnline) {
            searchPathSwitchTo(&oldSearchPath);
            u_sess->adv_cxt.isJumpRecompute = false;
        }
        u_sess->attr.attr_sql.enable_fast_query_shipping = oldEnableFqs;
        ereport(ERROR, 
            (errmodule(MOD_ADVISOR), errcode(ERRCODE_SYNTAX_ERROR),
                errmsg("current sql: %s analyze error", adviseQuery->query),
                errdetail("%s", edata->message),
                errcause("analyze sql error or user cancel cammand"),
                erraction("check query component")));
    }
    PG_END_TRY();
    if (u_sess->adv_cxt.isOnline) {
        searchPathSwitchTo(&oldSearchPath);
        u_sess->adv_cxt.isJumpRecompute = false;
    }
    u_sess->attr.attr_sql.enable_fast_query_shipping = oldEnableFqs;
}

static bool sqladvisorCheckCNLightProxy(Node* parsetree, List* querytree)
{
    if ((list_length(querytree) == 1) && !IsA(parsetree, CreateTableAsStmt) &&
        !IsA(parsetree, RefreshMatViewStmt)) {
        ExecNodes* singleExecNode = NULL;
        Query* query = (Query*)linitial(querytree);

        u_sess->attr.attr_sql.enable_fast_query_shipping = true;
        if (ENABLE_ROUTER(query->commandType)) {
            singleExecNode = lightProxy::checkRouterQuery(query);
        } else {
            singleExecNode = lightProxy::checkLightQuery(query);
        }
        u_sess->attr.attr_sql.enable_fast_query_shipping = false;
        if (singleExecNode && list_length(singleExecNode->nodeList) +
            list_length(singleExecNode->primarynodelist) == 1) {
            FreeExecNodes(&singleExecNode);
            return true;
        }
        FreeExecNodes(&singleExecNode);
        return false;
    }
    
    return false;
}

static List* getPlan(AdviseQuery* adviseQuery, bool* isCNLightProxy)
{
    List* planList = NIL;
    ListCell* cell = NULL;
    List* parseTreeList = pg_parse_query(adviseQuery->query);
    if (parseTreeList == NULL || list_length(parseTreeList) <= 0) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
            errmsg("can not parse query: %s.", adviseQuery->query)));
    }

    size_t queryLen = strlen(adviseQuery->query);
    bool runLightProxyCheck = list_length(parseTreeList) == 1 && queryLen < SECUREC_MEM_MAX_LEN;
    foreach (cell, parseTreeList) {
        Node* parsetree = (Node*)lfirst(cell);
        List* stmtList = NIL;
        
        if (!checkParsetreeTag(parsetree)) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("only support DML or DQL query.")));
        }

        if (adviseQuery->boundParams == NULL) {
            stmtList = pg_analyze_and_rewrite(parsetree, adviseQuery->query, NULL, 0);
        } else {
            if (adviseQuery->boundParams->parserSetup != NULL) {
                stmtList = pg_analyze_and_rewrite_params(parsetree, adviseQuery->query,
                    adviseQuery->boundParams->parserSetup, adviseQuery->boundParams->parserSetupArg);
            } else {
                Oid* argtypes = (Oid*)palloc0((adviseQuery->boundParams->numParams) * sizeof(Oid));
                for (int i = 0; i < adviseQuery->boundParams->numParams; i++) {
                    argtypes[i] = adviseQuery->boundParams->params[i].ptype;
                }
                stmtList = pg_analyze_and_rewrite(parsetree, adviseQuery->query, argtypes,
                                                  adviseQuery->boundParams->numParams);
            }
        }

        *isCNLightProxy = runLightProxyCheck && sqladvisorCheckCNLightProxy(parsetree, stmtList);
        ListCell* queryCell = NULL;
        PlannedStmt* ps = NULL;
        TableConstraint tableConstraint;
        initTableConstraint(&tableConstraint);
        checkQuery(stmtList, &tableConstraint);
        if (!tableConstraint.isHasTable || tableConstraint.isHasTempTable || tableConstraint.isHasFunction) {
            ereport(ERROR, 
                (errmodule(MOD_ADVISOR), errcode(ERRCODE_SYNTAX_ERROR),
                    errmsg("sql advisor don't support none table, temp table, system table."),
                    errdetail("N/A"),
                    errcause("sql advisor don't support none table, temp table, system table."),
                    erraction("check query component")));
        }
        foreach (queryCell, stmtList) {
            Query* query = castNode(Query, lfirst(queryCell));
            if (query->commandType != CMD_UTILITY) {
                query->boundParamsQ = adviseQuery->boundParams;
                ps = pg_plan_query(query, adviseQuery->cursorOptions, adviseQuery->boundParams);
                planList = lappend(planList, ps);
            } else {
                /* Utility commands have no plans. */            
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),            
                    errmsg("sql advisor don't support DDL and DCL statement.")));
            }
        }
        list_free_ext(stmtList);
    }

    return planList;
}