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
 *   sql_limit_mgr.cpp
 *   functions for sql limit manager
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/workload/sql_limit_mgr.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "utils/atomic.h"
#include "knl/knl_variable.h"
#include "knl/knl_thread.h"
#include "miscadmin.h"
#include "funcapi.h"
#include "pgstat.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/palloc.h"
#include "utils/ps_status.h"
#include "utils/postinit.h"
#include "tcop/tcopprot.h"
#include "access/heapam.h"
#include "access/genam.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "catalog/indexing.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_database.h"
#include "catalog/gs_sql_limit.h"
#include "workload/sql_limit_process.h"
#include "instruments/gs_stack.h"

static inline bool IsSqlLimitTypeValid(char *sqlType)
{
    if (strcasecmp(sqlType, SQLID_TYPE) != 0 && strcasecmp(sqlType, SELECT_TYPE) != 0 &&
        strcasecmp(sqlType, UPDATE_TYPE) != 0 && strcasecmp(sqlType, INSERT_TYPE) != 0 &&
        strcasecmp(sqlType, DELETE_TYPE) != 0) {
        return false;
    }

    return true;
}

static void ValidateLimitParams(FunctionCallInfo fcinfo, bool isCreate)
{
    if (!superuser()) {
        aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_PROC, "must be system admin to execute");
    }

    if (!u_sess->attr.attr_common.enable_sql_limit) {
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), (errmsg("enable_sql_limit is off, please set to on"))));
    }

    if (isCreate) {
        if (PG_ARGISNULL(1)) {
            ereport(ERROR, (errmodule(MOD_WLM), errmsg("limit_type can not be null.")));
        }

        char *sqlType = text_to_cstring(PG_GETARG_TEXT_P(1));
        if (!IsSqlLimitTypeValid(sqlType)) {
            ereport(ERROR, (errmodule(MOD_WLM), errmsg("limit_type %s is invalid. "
                "expected: sqlid, select, update, insert, delete", sqlType)));
        }
        pfree_ext(sqlType);
    }

    const int WORK_NODE_ARG_INDEX = 2;
    const int MAX_CONCURRENCY_ARG_INDEX = 3;
    const int OPTION_VAL_ARG_INDEX = 6;
    if (PG_ARGISNULL(WORK_NODE_ARG_INDEX)) {
        ereport(ERROR, (errmodule(MOD_WLM), errmsg("work_node can not be null.")));
    }

    if (PG_ARGISNULL(MAX_CONCURRENCY_ARG_INDEX)) {
        ereport(ERROR, (errmodule(MOD_WLM), errmsg("max_cocurrency can not be null.")));
    }

    if (PG_ARGISNULL(OPTION_VAL_ARG_INDEX)) {
        ereport(ERROR, (errmodule(MOD_WLM), errmsg("limit_opt can not be null.")));
    }
}

static void FillLimitParams(Datum* values, bool* nulls, FunctionCallInfo fcinfo)
{
    static const struct {
        int attrNum;
        int argIndex;
    } fieldMap[] = {
        {Anum_gs_sql_limit_work_node, 2},
        {Anum_gs_sql_limit_max_concurrency, 3},
        {Anum_gs_sql_limit_start_time, 4},
        {Anum_gs_sql_limit_end_time, 5},
        {Anum_gs_sql_limit_limit_opt, 6},
        {Anum_gs_sql_limit_databases, 7},
        {Anum_gs_sql_limit_users, 8}
    };

    for (size_t i = 0; i < sizeof(fieldMap)/sizeof(fieldMap[0]); i++) {
        int fieldIndex = fieldMap[i].attrNum - 1;
        int argIndex = fieldMap[i].argIndex;

        if (PG_ARGISNULL(argIndex)) {
            nulls[fieldIndex] = true;
        } else {
            values[fieldIndex] = PG_GETARG_DATUM(argIndex);
            nulls[fieldIndex] = false;
        }
    }
}

static void UpdateLimitValidity(Datum* values, bool* nulls, bool* repl)
{
    TimestampTz startTime = DatumGetTimestampTz(values[Anum_gs_sql_limit_start_time - 1]);
    TimestampTz endTime = DatumGetTimestampTz(values[Anum_gs_sql_limit_end_time - 1]);
    TimestampTz now = GetCurrentTimestamp();
    int64 maxConcurrency = DatumGetInt64(values[Anum_gs_sql_limit_max_concurrency - 1]);

    bool isValid = !(maxConcurrency < 0 || (endTime != 0 && endTime < now) ||
        (startTime != 0 && endTime != 0 && startTime > endTime));
    values[Anum_gs_sql_limit_is_valid - 1] = BoolGetDatum(isValid);
    nulls[Anum_gs_sql_limit_is_valid - 1] = false;
    repl[Anum_gs_sql_limit_is_valid - 1] = true;
}

// gs_create_sql_limit(limit_name,limit_type,work_node,max_concurrency,start_time,end_time,limit_opt,databases,users)
Datum gs_create_sql_limit(PG_FUNCTION_ARGS)
{
    if (g_instance.sqlLimit_cxt.entryCount >= MAX_SQL_LIMIT_COUNT) {
        ereport(ERROR, (errmodule(MOD_WLM), errmsg("sql limit count(%d) is over the limit(%d). "
            "Please delete unused limits.", g_instance.sqlLimit_cxt.entryCount, MAX_SQL_LIMIT_COUNT)));
    }

    if (RecoveryInProgress()) {
        ereport(ERROR, (errmodule(MOD_WLM), errmsg("create sql limit is not allowed in standby.")));
    }

    ValidateLimitParams(fcinfo, true);

    char *sqlType = text_to_cstring(PG_GETARG_TEXT_P(1));
    if (!IsSqlLimitTypeValid(sqlType)) {
        ereport(ERROR, (errmodule(MOD_WLM), errmsg("limit_type %s is invalid. "
            "expected: sqlId, select, update, insert, delete", sqlType)));
    }

    uint64 uniqueSqlid = 0;
    Datum datum = PG_GETARG_DATUM(6);
    SqlType ruleTypeEnum = GetSqlLimitType(sqlType);
    if (ruleTypeEnum == SQL_TYPE_OTHER) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid rule type: %s", sqlType)));
        return false;
    }

    (void)ValidateAndExtractOption(ruleTypeEnum, &datum, &uniqueSqlid);

    Datum values[Natts_gs_sql_limit] = {};
    bool nulls[Natts_gs_sql_limit] = {};

    uint64 limitId = pg_atomic_fetch_add_u64(&g_instance.sqlLimit_cxt.entryIdSequence, 1);
    values[Anum_gs_sql_limit_limit_id - 1] = Int64GetDatum(limitId);

    if (PG_ARGISNULL(0)) {
        nulls[Anum_gs_sql_limit_limit_name - 1] = true;
    } else {
        values[Anum_gs_sql_limit_limit_name - 1] = PG_GETARG_DATUM(0); // limit_name
    }

    values[Anum_gs_sql_limit_limit_type - 1] = PG_GETARG_DATUM(1); // limit_type

    FillLimitParams(values, nulls, fcinfo);

    TimestampTz startTime = DatumGetTimestampTz(values[Anum_gs_sql_limit_start_time - 1]);
    TimestampTz endTime = DatumGetTimestampTz(values[Anum_gs_sql_limit_end_time - 1]);
    TimestampTz now = GetCurrentTimestamp();
    int64 maxConcurrency = DatumGetInt64(values[Anum_gs_sql_limit_max_concurrency - 1]);
    bool isValid = !(maxConcurrency < 0 || (startTime != 0 && startTime > now) ||
        (endTime != 0 && endTime < now));
    values[Anum_gs_sql_limit_is_valid - 1] = BoolGetDatum(isValid);
    nulls[Anum_gs_sql_limit_is_valid - 1] = false;

    CreateSqlLimit(values, nulls, &uniqueSqlid);

    Relation rel = heap_open(GsSqlLimitRelationId, RowExclusiveLock);
    TupleDesc tupedesc = RelationGetDescr(rel);
    HeapTuple tuple = heap_form_tuple(tupedesc, values, nulls);
    (void)simple_heap_insert(rel, tuple);
    CatalogUpdateIndexes(rel, tuple);
    heap_freetuple_ext(tuple);
    heap_close(rel, RowExclusiveLock);
    pfree_ext(sqlType);
    PG_RETURN_INT64(limitId);
}

static void SetReplacementFlags(bool* repl)
{
    repl[Anum_gs_sql_limit_limit_name - 1] = true;
    repl[Anum_gs_sql_limit_work_node - 1] = true;
    repl[Anum_gs_sql_limit_max_concurrency - 1] = true;
    repl[Anum_gs_sql_limit_start_time - 1] = true;
    repl[Anum_gs_sql_limit_end_time - 1] = true;
    repl[Anum_gs_sql_limit_limit_opt - 1] = true;
    repl[Anum_gs_sql_limit_databases - 1] = true;
    repl[Anum_gs_sql_limit_users - 1] = true;
}

// gs_update_sql_limit(limit_id,limit_name,work_node,max_concurrency,start_time,end_time,limit_opt,databases,users)
Datum gs_update_sql_limit(PG_FUNCTION_ARGS)
{
    if (RecoveryInProgress()) {
        ereport(ERROR, (errmodule(MOD_WLM), errmsg("update sql limit is not allowed in standby.")));
    }

    ValidateLimitParams(fcinfo, false);
    Datum limitId = PG_GETARG_DATUM(0);
    ScanKeyData entry;
    ScanKeyInit(&entry, Anum_gs_sql_limit_limit_id, BTEqualStrategyNumber, F_INT8EQ, limitId);
    Relation rel = heap_open(GsSqlLimitRelationId, RowExclusiveLock);
    TupleDesc tupledesc = RelationGetDescr(rel);
    SysScanDesc scan = systable_beginscan(rel, GsSqlLimitIdIndex, true, NULL, 1, &entry);
    HeapTuple tuple = systable_getnext(scan);
    if (!HeapTupleIsValid(tuple)) {
        systable_endscan(scan);
        heap_close(rel, RowExclusiveLock);
        ereport(ERROR, (errmodule(MOD_WLM), errmsg("limitId %lu is not exist.", DatumGetInt64(limitId))));
    }

    Datum values[Natts_gs_sql_limit] = {};
    bool nulls[Natts_gs_sql_limit] = {};
    bool repl[Natts_gs_sql_limit] = {};

    heap_deform_tuple(tuple, tupledesc, values, nulls);
    if (PG_ARGISNULL(1)) {
        nulls[Anum_gs_sql_limit_limit_name - 1] = true;
    } else {
        values[Anum_gs_sql_limit_limit_name - 1] = PG_GETARG_DATUM(1); // limit_name
        nulls[Anum_gs_sql_limit_limit_name - 1] = false;
    }

    uint64 uinqueSqlid = 0;
    Datum* datum = PG_ARGISNULL(6) ? NULL : &(PG_GETARG_DATUM(6));
    char* sqlType = text_to_cstring(DatumGetTextP(values[Anum_gs_sql_limit_limit_type - 1]));
    SqlType ruleTypeEnum = GetSqlLimitType(sqlType);
    if (ruleTypeEnum == SQL_TYPE_OTHER) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("invalid rule type: %s", sqlType)));
    }

    (void)ValidateAndExtractOption(ruleTypeEnum, datum, &uinqueSqlid);
    // reconstruction
    FillLimitParams(values, nulls, fcinfo);

    SetReplacementFlags(repl);

    UpdateLimitValidity(values, nulls, repl);

    HeapTuple newTuple = heap_modify_tuple(tuple, tupledesc, values, nulls, repl);
    simple_heap_update(rel, &newTuple->t_self, newTuple);
    CatalogUpdateIndexes(rel, newTuple);
    heap_freetuple_ext(newTuple);
    systable_endscan(scan);
    heap_close(rel, RowExclusiveLock);

    bool ret = UpdateSqlLimit(values, nulls, &uinqueSqlid);
    pfree_ext(sqlType);
    PG_RETURN_BOOL(ret);
}

static void AddToResultSet(SqlLimit* rule, TupleDesc tupdesc, Tuplestorestate* resultStore)
{
    const int SELECT_RETURN_ATTR_NUM = 6;
    Datum values[SELECT_RETURN_ATTR_NUM];
    bool nulls[SELECT_RETURN_ATTR_NUM] = {false};

    int j = 0;
    values[j++] = Int64GetDatum(rule->limitId);
    values[j++] = BoolGetDatum(rule->isValid);
    values[j++] = Int64GetDatum(rule->workNode);
    values[j++] = Int64GetDatum(rule->maxConcurrency);
    values[j++] = Int64GetDatum(rule->stats.hitCount);
    values[j++] = Int64GetDatum(rule->stats.rejectCount);

    tuplestore_putvalues(resultStore, tupdesc, values, nulls);
}

static TupleDesc InitializeResultSet(ReturnSetInfo* rsinfo)
{
    const int SELECT_RETURN_ATTR_NUM = 6;
    int i = 1;
    MemoryContext oldcontext = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);
    TupleDesc tupdesc = CreateTemplateTupleDesc(SELECT_RETURN_ATTR_NUM, false);
    TupleDescInitEntry(tupdesc, (AttrNumber)i++, "limit_id", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)i++, "is_valid", BOOLOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)i++, "work_node", INT1OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)i++, "max_concurrency", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)i++, "hit_count", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)i++, "reject_count", INT8OID, -1, 0);

    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tuplestore_begin_heap(true, false, u_sess->attr.attr_memory.work_mem);
    rsinfo->setDesc = BlessTupleDesc(tupdesc);

    MemoryContextSwitchTo(oldcontext);

    return tupdesc;
}

Datum gs_select_sql_limit(PG_FUNCTION_ARGS)
{
    if (!superuser()) {
        aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_PROC, "must be system admin to execute");
    }

    if (!u_sess->attr.attr_common.enable_sql_limit) {
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), (errmsg("enable_sql_limit is off, please set to on"))));
    }

    if (PG_ARGISNULL(0)) {
        ereport(ERROR, (errmodule(MOD_WLM), errmsg("limitId can not be null.")));
    }

    if (fcinfo->resultinfo == NULL || !IsA(fcinfo->resultinfo, ReturnSetInfo)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("set-valued function called in context that cannot accept a set")));
    }

    ReturnSetInfo* rsinfo = (ReturnSetInfo*)fcinfo->resultinfo;
    TupleDesc tupdesc = InitializeResultSet(rsinfo);

    int64 limitId = DatumGetInt64(PG_GETARG_DATUM(0));
    LWLockAcquire(SqlLimitLock, LW_SHARED);
    SqlLimit* rule = SearchSqlLimitCache(limitId);
    if (rule == NULL) {
        ereport(ERROR, (errmodule(MOD_WLM), errmsg("limitId %lu is not exist.", limitId)));
    }

    AddToResultSet(rule, tupdesc, rsinfo->setResult);
    LWLockRelease(SqlLimitLock);
    /* clean up and return the tuplestore */
    tuplestore_donestoring(rsinfo->setResult);
    return (Datum)0;
}

Datum gs_select_sql_limit_all(PG_FUNCTION_ARGS)
{
    if (!superuser()) {
        aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_PROC, "must be system admin to execute");
    }

    if (!u_sess->attr.attr_common.enable_sql_limit) {
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), (errmsg("enable_sql_limit is off, please set to on"))));
    }

    if (fcinfo->resultinfo == NULL || !IsA(fcinfo->resultinfo, ReturnSetInfo)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("set-valued function called in context that cannot accept a set")));
    }

    ReturnSetInfo* rsinfo = (ReturnSetInfo*)fcinfo->resultinfo;
    TupleDesc tupdesc = InitializeResultSet(rsinfo);

    LWLockAcquire(SqlLimitLock, LW_SHARED);
    HASH_SEQ_STATUS status;
    hash_seq_init(&status, g_instance.sqlLimit_cxt.uniqueSqlIdLimits);
    UniqueSqlIdHashEntry* entry = NULL;
    while ((entry = (UniqueSqlIdHashEntry*)hash_seq_search(&status)) != NULL) {
        AddToResultSet(entry->limit, tupdesc, rsinfo->setResult);
    }

    for (int i = 0; i < MAX_SQL_LIMIT_TYPE; i++) {
        dlist_iter iter;
        dlist_foreach(iter, &g_instance.sqlLimit_cxt.keywordsLimits[i]) {
            KeywordsLimitNode *currNode = (KeywordsLimitNode *)dlist_container(KeywordsLimitNode, node, iter.cur);
            if (currNode != NULL && currNode->limit != NULL) {
                AddToResultSet(currNode->limit, tupdesc, rsinfo->setResult);
            }
        }
    }
    LWLockRelease(SqlLimitLock);
    /* clean up and return the tuplestore */
    tuplestore_donestoring(rsinfo->setResult);
    return (Datum)0;
}

Datum gs_delete_sql_limit(PG_FUNCTION_ARGS)
{
    if (!superuser()) {
        aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_PROC, "must be system admin to execute");
    }

    if (!u_sess->attr.attr_common.enable_sql_limit) {
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), (errmsg("enable_sql_limit is off, please set to on"))));
    }

    if (RecoveryInProgress()) {
        ereport(ERROR, (errmodule(MOD_WLM), errmsg("delete sql limit is not allowed in standby.")));
    }

    Datum limitId = PG_GETARG_DATUM(0);
    ScanKeyData entry;
    ScanKeyInit(&entry, Anum_gs_sql_limit_limit_id, BTEqualStrategyNumber, F_INT8EQ, limitId);
    Relation rel = heap_open(GsSqlLimitRelationId, RowExclusiveLock);
    SysScanDesc scan = systable_beginscan(rel, GsSqlLimitIdIndex, true, NULL, 1, &entry);
    HeapTuple tuple = systable_getnext(scan);
    if (!HeapTupleIsValid(tuple)) {
        systable_endscan(scan);
        heap_close(rel, RowExclusiveLock);
        ereport(ERROR, (errmodule(MOD_WLM), errmsg("limitId %lu is not exist.", DatumGetInt64(limitId))));
    }

    simple_heap_delete(rel, &tuple->t_self);
    systable_endscan(scan);
    heap_close(rel, RowExclusiveLock);

    bool result = DeleteSqlLimitCache(DatumGetUInt64(limitId));

    PG_RETURN_BOOL(result);
}

static void SqlLimitSighupHandler(SIGNAL_ARGS)
{
    t_thrd.sql_limit_cxt.got_SIGHUP = true;
    if (t_thrd.proc) {
        SetLatch(&t_thrd.proc->procLatch);
    }
}

static void SqlLimitSigtermHandler(SIGNAL_ARGS)
{
    int saveErrno = errno;
    t_thrd.sql_limit_cxt.shutdown_requested = true;

    if (t_thrd.proc) {
        SetLatch(&t_thrd.proc->procLatch);
    }
    errno = saveErrno;
}

static void ProcessSignal()
{
    (void)gspqsignal(SIGURG, print_stack);
    (void)gspqsignal(SIGHUP, SqlLimitSighupHandler);
    (void)gspqsignal(SIGINT, SIG_IGN); /* Cancel signal */
    (void)gspqsignal(SIGTERM, SqlLimitSigtermHandler);
    (void)gspqsignal(SIGQUIT, quickdie);
    (void)gspqsignal(SIGALRM, SIG_IGN);
    (void)gspqsignal(SIGPIPE, SIG_IGN);
    (void)gspqsignal(SIGUSR1, procsignal_sigusr1_handler);
    (void)gspqsignal(SIGUSR2, SIG_IGN);
    (void)gspqsignal(SIGFPE,  FloatExceptionHandler);
    (void)gspqsignal(SIGCHLD, SIG_DFL);
    (void)gspqsignal(SIGTTIN, SIG_DFL);
    (void)gspqsignal(SIGTTOU, SIG_DFL);
    (void)gspqsignal(SIGCONT, SIG_DFL);
    (void)gspqsignal(SIGWINCH, SIG_DFL);

    if (IsUnderPostmaster) {
        /* We allow SIGQUIT (quickdie) at all times */
        (void)sigdelset(&t_thrd.libpq_cxt.BlockSig, SIGQUIT);
    }
    /*
     * Unblock signals (they were blocked when the postmaster forked us)
     */
    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();
}

void InitSqlLimit()
{
    for (;;) {
        if (pmState != PM_RUN && pmState != PM_HOT_STANDBY) {
            break;
        }

        if (t_thrd.sql_limit_cxt.got_SIGHUP) {
            t_thrd.sql_limit_cxt.got_SIGHUP = false;
            ProcessConfigFile(PGC_SIGHUP);
        }

        if (t_thrd.sql_limit_cxt.shutdown_requested) {
            t_thrd.sql_limit_cxt.shutdown_requested = false;
            CleanSqlLimitCache();
            proc_exit(0);
        }

        start_xact_command();
        if (IsRelationExists(GsSqlLimitRelationId)) {
            InitSqlLimitCache();
            finish_xact_command();
            break;
        }
        finish_xact_command();

        int ret = WaitLatch(&t_thrd.proc->procLatch,
            WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, TIME_INTERVAL_SEC * 1000);
        ResetLatch(&t_thrd.proc->procLatch);

        /* Process sinval catchup interrupts that happened while sleeping */
        ProcessCatchupInterrupt();

        /*
         * Emergency bailout if postmaster has died.  This is to avoid the
         * necessity for manual cleanup of all postmaster children.
         */
        if ((unsigned int)ret & WL_POSTMASTER_DEATH) {
            CleanSqlLimitCache();
            proc_exit(1);
        }
    }
}


static TimestampTz MaintainSqlLimitCache(TimestampTz lastRemoveTime)
{
    const int REMOVE_TIME_INTERVAL_MS = 300 * 1000;
    TimestampTz currentTime;

    UpdateSqlLimitCache();

    currentTime = GetCurrentTimestamp();
    if (TimestampDifferenceExceeds(lastRemoveTime, currentTime, REMOVE_TIME_INTERVAL_MS)) {
        RemoveInvalidSqlLimitCache();
        return currentTime;
    }

    return lastRemoveTime;
}

void SqlLimitMainLoop()
{
    TimestampTz lastRemoveTime = GetCurrentTimestamp();
    for (;;) {
        if (pmState != PM_RUN && pmState != PM_HOT_STANDBY) {
            break;
        }
        (void)MemoryContextSwitchTo(g_instance.sqlLimit_cxt.gSqlLimitCxt);

        ResetLatch(&t_thrd.proc->procLatch);

        if (t_thrd.sql_limit_cxt.got_SIGHUP) {
            t_thrd.sql_limit_cxt.got_SIGHUP = false;
            ProcessConfigFile(PGC_SIGHUP);
        }

        if (t_thrd.sql_limit_cxt.shutdown_requested) {
            t_thrd.sql_limit_cxt.shutdown_requested = false;
            CleanSqlLimitCache();
            proc_exit(0);
        }

        if (!u_sess->attr.attr_common.enable_sql_limit) {
            CleanSqlLimitCache();
            proc_exit(0);
        }

        start_xact_command();

        lastRemoveTime = MaintainSqlLimitCache(lastRemoveTime);

        finish_xact_command();

        int ret = WaitLatch(&t_thrd.proc->procLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
            TIME_INTERVAL_SEC * 1000);

        ResetLatch(&t_thrd.proc->procLatch);

        /* Process sinval catchup interrupts that happened while sleeping */
        ProcessCatchupInterrupt();

        /*
         * Emergency bailout if postmaster has died.  This is to avoid the
         * necessity for manual cleanup of all postmaster children.
         */
        if ((unsigned int)ret & WL_POSTMASTER_DEATH) {
            CleanSqlLimitCache();
            proc_exit(1);
        }
    }
}

NON_EXEC_STATIC void SqlLimitMain()
{
    char username[NAMEDATALEN] = {'\0'};
    sigjmp_buf localSigjmpBuf;

    /* we are a postmaster subprocess now */
    IsUnderPostmaster = true;
    t_thrd.role = SQL_LIMIT_THREAD;

    /* reset t_thrd.proc_cxt.MyProcPid */
    t_thrd.proc_cxt.MyProcPid = gs_thread_self();

    /* record Start Time for logging */
    t_thrd.proc_cxt.MyStartTime = time(NULL);

    t_thrd.proc_cxt.MyProgName = "SqlLimit";
    u_sess->attr.attr_common.application_name = pstrdup("SqlLimit");

    /* Identify myself via ps */
    init_ps_display("sql_limit_rule process", "", "", "");

    SetProcessingMode(InitProcessing);
    ProcessSignal();

    /* Early initialization */
    BaseInit();

#ifndef EXEC_BACKEND
    InitProcess();
#endif

    t_thrd.proc_cxt.PostInit->SetDatabaseAndUser((char*)pstrdup(DEFAULT_DATABASE), InvalidOid, username);
    t_thrd.proc_cxt.PostInit->InitWLM();

    SetProcessingMode(NormalProcessing);
    on_shmem_exit(PGXCNodeCleanAndRelease, 0);
    pgstat_report_appname("SqlLimit");
    pgstat_report_activity(STATE_IDLE, NULL);

    InitSqlLimit();
    (void)MemoryContextSwitchTo(g_instance.sqlLimit_cxt.gSqlLimitCxt);
    if (sigsetjmp(localSigjmpBuf, 1) != 0) {
        /* Since not using PG_TRY, must reset error stack by hand */
        t_thrd.log_cxt.error_context_stack = NULL;

        t_thrd.log_cxt.call_stack = NULL;

        /* Prevent interrupts while cleaning up */
        HOLD_INTERRUPTS();

        /* Forget any pending QueryCancel request */
        t_thrd.int_cxt.QueryCancelPending = false;
        (void)disable_sig_alarm(true);
        t_thrd.int_cxt.QueryCancelPending = false;

        /* Report the error to the server log */
        EmitErrorReport();

        /*
         * Abort the current transaction in order to recover.
         */
        AbortCurrentTransaction();

        /* release resource held by lsc */
        AtEOXact_SysDBCache(false);

        LWLockReleaseAll();

        /*
         * Now return to normal top-level context and clear ErrorContext for
         * next time.
         */
        (void)MemoryContextSwitchTo(g_instance.sqlLimit_cxt.gSqlLimitCxt);
        FlushErrorState();

        /* Now we can allow interrupts again */
        RESUME_INTERRUPTS();
    }

    /* We can now handle ereport(ERROR) */
    t_thrd.log_cxt.PG_exception_stack = &localSigjmpBuf;

    /*
     * Unblock signals in case they were blocked during long jump.
     */
    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();

    SqlLimitMainLoop();
    CleanSqlLimitCache();
    proc_exit(0);
}
