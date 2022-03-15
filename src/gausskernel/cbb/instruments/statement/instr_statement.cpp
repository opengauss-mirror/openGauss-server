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
 * instr_statement.cpp
 *   functions for full/slow SQL
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/instruments/statement/instr_statement.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "instruments/instr_statement.h"
#include "instruments/instr_handle_mgr.h"
#include "instruments/instr_unique_sql.h"
#include "instruments/instr_slow_query.h"
#include "postgres.h"
#include "pgxc/pgxc.h"
#include "pgstat.h"
#include "pgxc/poolutils.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "utils/memutils.h"
#include "storage/proc.h"
#include "storage/latch.h"
#include "storage/ipc.h"
#include "catalog/pg_database.h"
#include "gssignal/gs_signal.h"
#include "utils/guc.h"
#include "utils/ps_status.h"
#include "utils/elog.h"
#include "utils/memprot.h"
#include "tcop/dest.h"
#include "tcop/tcopprot.h"
#include "gs_thread.h"
#include "access/heapam.h"
#include "utils/rel.h"
#include "utils/postinit.h"
#include "utils/snapmgr.h"
#include "workload/gscgroup.h"
#include "libpq/pqsignal.h"
#include "pgxc/groupmgr.h"
#include "funcapi.h"
#include "libpq/ip.h"
#include "storage/lock/lock.h"
#include "nodes/makefuncs.h"
#include "catalog/indexing.h"
#include "commands/dbcommands.h"
#include "utils/builtins.h"
#include "commands/explain.h"
#include "utils/fmgroids.h"
#include "utils/relcache.h"
#include "commands/copy.h"
#include "instruments/instr_func_control.h"

#define MAX_SLOW_QUERY_RETENSION_DAYS 604800
#define MAX_FULL_SQL_RETENSION_SEC 86400
#define IS_NULL_STR(str) ((str) == NULL || (str)[0] == '\0')
#define STRING_MAX_LEN 256

#define STATEMENT_DETAILS_HEAD_SIZE (1 + 1)     /* [VERSION] + [TRUNCATED] */
#define INSTR_STMT_UNIX_DOMAIN_PORT (-1)
#define INSTR_STATEMENT_ATTRNUM 52

/* lock/lwlock's detail information */
typedef struct {
    StmtDetailType type;        /* statement details kind. */
    const LOCKTAG *locktag;     /* for Lock, this field is used. */
    uint16 lwlockId;            /* for LWLock, this field is used. */
    int lockmode;               /* lockmode for this request. */
} StmtLockDetail;

static bytea* get_statement_detail(StatementDetail *text, bool oom);
static bool is_valid_detail_record(uint32 bytea_data_len, const char *details, uint32 details_len);

static List* split_levels_into_list(const char* levels)
{
    List *result = NIL;
    char *str = pstrdup(levels);
    char *first_ch = str;
    int len = (int)strlen(str) + 1;

    for (int i = 0; i < len; i++) {
        if (str[i] == ',' || str[i] == '\0') {
            /* replace ',' with '\0'. */
            str[i] = '\0';

            /* copy this into result. */
            result = lappend(result, pstrdup(first_ch));

            /* move to the head of next string. */
            first_ch = str + i + 1;
        }
    }
    pfree(str);

    return result;
}

static StatLevel name2level(const char *name)
{
    if (pg_strcasecmp(name, "OFF") == 0) {
        return STMT_TRACK_OFF;
    } else if (pg_strcasecmp(name, "L0") == 0) {
        return STMT_TRACK_L0;
    } else if (pg_strcasecmp(name, "L1") == 0) {
        return STMT_TRACK_L1;
    } else if (pg_strcasecmp(name, "L2") == 0) {
        return STMT_TRACK_L2;
    } else {
        return LEVEL_INVALID;
    }
}

bool check_statement_stat_level(char** newval, void** extra, GucSource source)
{
    /* parse level */
    List *l = split_levels_into_list(*newval);

    if (list_length(l) != STATEMENT_SQL_KIND) {
        GUC_check_errdetail("attr num:%d is error,track_stmt_stat_level attr is 2", l->length);
        list_free_deep(l);
        return false;
    }

    int full_level = name2level((char*)linitial(l));
    int slow_level = name2level((char*)lsecond(l));

    list_free_deep(l);
    if (full_level == LEVEL_INVALID || slow_level == LEVEL_INVALID) {
        GUC_check_errdetail("invalid input syntax");
        return false;
    }

    *extra = MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_DFX), STATEMENT_SQL_KIND * sizeof(int));
    ((int*)(*extra))[0] = full_level;
    ((int*)(*extra))[1] = slow_level;

    return true;
}

void assign_statement_stat_level(const char* newval, void* extra)
{
    int *level = (int*) extra;

    u_sess->statement_cxt.statement_level[0] = level[0];
    u_sess->statement_cxt.statement_level[1] = level[1];
}


void JobStatementIAm(void)
{
    t_thrd.role = TRACK_STMT_WORKER;
}

bool IsStatementFlushProcess(void)
{
    return t_thrd.role == TRACK_STMT_WORKER;
}

/* SIGHUP handler for statement flush thread */
static void statement_sighup_handler(SIGNAL_ARGS)
{
    t_thrd.statement_cxt.got_SIGHUP = true;
}

static void statement_exit(SIGNAL_ARGS)
{
    t_thrd.statement_cxt.need_exit = true;
    die(postgres_signal_arg);
}

static void SetThrdCxt(void)
{
    t_thrd.mem_cxt.msg_mem_cxt = AllocSetContextCreate(TopMemoryContext,
        "MessageContext",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);

    /* Create the memory context we will use in the main loop. */
    t_thrd.mem_cxt.mask_password_mem_cxt = AllocSetContextCreate(TopMemoryContext,
        "MaskPasswordCtx",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);
}

static void ReloadInfo()
{
    /* Reload configuration if we got SIGHUP from the postmaster. */
    if (t_thrd.statement_cxt.got_SIGHUP) {
        t_thrd.statement_cxt.got_SIGHUP = false;
        ProcessConfigFile(PGC_SIGHUP);
    }

    if (IsGotPoolReload()) {
        processPoolerReload();
        ResetGotPoolReload(false);
    }
}

static void ProcessSignal(void)
{
    /*
     * Ignore all signals usually bound to some action in the postmaster,
     * except SIGHUP, SIGTERM and SIGQUIT.
     */
    (void)gspqsignal(SIGHUP, statement_sighup_handler);
    (void)gspqsignal(SIGINT, SIG_IGN);
    (void)gspqsignal(SIGTERM, statement_exit); /* cancel current query and exit */
    (void)gspqsignal(SIGQUIT, quickdie);
    (void)gspqsignal(SIGUSR1, procsignal_sigusr1_handler);
    (void)gspqsignal(SIGALRM, SIG_IGN);
    (void)gspqsignal(SIGPIPE, SIG_IGN);
    (void)gspqsignal(SIGUSR2, SIG_IGN);
    (void)gspqsignal(SIGCHLD, SIG_DFL);
    (void)gspqsignal(SIGTTIN, SIG_DFL);
    (void)gspqsignal(SIGTTOU, SIG_DFL);
    (void)gspqsignal(SIGCONT, SIG_DFL);
    (void)gspqsignal(SIGWINCH, SIG_DFL);

    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();
    if (u_sess->proc_cxt.MyProcPort->remote_host)
        pfree(u_sess->proc_cxt.MyProcPort->remote_host);
    u_sess->proc_cxt.MyProcPort->remote_host = pstrdup("localhost");

    t_thrd.wlm_cxt.thread_node_group = &g_instance.wlm_cxt->MyDefaultNodeGroup;  // initialize the default value
    t_thrd.wlm_cxt.thread_climgr = &t_thrd.wlm_cxt.thread_node_group->climgr;
    t_thrd.wlm_cxt.thread_srvmgr = &t_thrd.wlm_cxt.thread_node_group->srvmgr;
}

#define SET_NAME_VALUES(str, attr)                                             \
    do {                                                                       \
        if (str == NULL) {                                                     \
            nulls[attr] = true;                                                \
        } else {                                                               \
            values[attr] = DirectFunctionCall1(namein, CStringGetDatum(str));  \
        }                                                                      \
    } while (0)

#define SET_TEXT_VALUES(str, attr)                    \
    do {                                              \
        if (str == NULL) {                            \
            nulls[attr] = true;                       \
        } else {                                      \
            values[attr] = CStringGetTextDatum(str);  \
        }                                             \
    } while (0)

static void set_stmt_row_activity_cache_io(StatementStatContext* statementInfo, Datum values[], int* i)
{
    /* row activity */
    values[(*i)++] = Int64GetDatum(statementInfo->row_activity.returned_rows);
    values[(*i)++] = Int64GetDatum(statementInfo->row_activity.tuples_fetched);
    values[(*i)++] = Int64GetDatum(statementInfo->row_activity.tuples_returned);
    values[(*i)++] = Int64GetDatum(statementInfo->row_activity.tuples_inserted);
    values[(*i)++] = Int64GetDatum(statementInfo->row_activity.tuples_updated);
    values[(*i)++] = Int64GetDatum(statementInfo->row_activity.tuples_deleted);

    /* cache IO */
    values[(*i)++] = Int64GetDatum(statementInfo->cache_io.blocks_fetched);
    values[(*i)++] = Int64GetDatum(statementInfo->cache_io.blocks_hit);
}

static void set_stmt_basic_info(const knl_u_statement_context* statementCxt, StatementStatContext* statementInfo,
    Datum values[], bool nulls[], int* i)
{
    /* query basic info */
    values[(*i)++] = Int64GetDatum(statementInfo->unique_query_id);
    values[(*i)++] = Int64GetDatum(statementInfo->debug_query_id);
    SET_TEXT_VALUES(statementInfo->query, (*i)++);
    values[(*i)++] = TimestampTzGetDatum(statementInfo->start_time);
    values[(*i)++] = TimestampTzGetDatum(statementInfo->finish_time);
    values[(*i)++] = Int64GetDatum(statementInfo->slow_query_threshold);
    values[(*i)++] = Int64GetDatum(statementInfo->txn_id);
    values[(*i)++] = Int64GetDatum(statementInfo->tid);
    values[(*i)++] = Int64GetDatum(statementCxt->session_id);

    /* parse info */
    values[(*i)++] = Int64GetDatum(statementInfo->parse.soft_parse);
    values[(*i)++] = Int64GetDatum(statementInfo->parse.hard_parse);

    SET_TEXT_VALUES(statementInfo->query_plan, (*i)++);
}

static void set_stmt_lock_summary(const LockSummaryStat *lock_summary, Datum values[], int* i)
{
    values[(*i)++] = Int64GetDatum(lock_summary->lock_cnt);
    values[(*i)++] = Int64GetDatum(lock_summary->lock_time);
    values[(*i)++] = Int64GetDatum(lock_summary->lock_wait_cnt);
    values[(*i)++] = Int64GetDatum(lock_summary->lock_wait_time);
    values[(*i)++] = Int64GetDatum(lock_summary->lock_max_cnt);
    values[(*i)++] = Int64GetDatum(lock_summary->lwlock_cnt);
    values[(*i)++] = Int64GetDatum(lock_summary->lwlock_wait_cnt);
    values[(*i)++] = Int64GetDatum(lock_summary->lwlock_time);
    values[(*i)++] = Int64GetDatum(lock_summary->lwlock_wait_time);
}

static HeapTuple GetStatementTuple(Relation rel, StatementStatContext* statementInfo,
    const knl_u_statement_context* statementCxt)
{
    int i = 0;
    Datum values[INSTR_STATEMENT_ATTRNUM];
    bool nulls[INSTR_STATEMENT_ATTRNUM] = {false};
    errno_t rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
    securec_check(rc, "\0", "\0");

    /* get statment tuple */
    SET_NAME_VALUES(statementCxt->db_name, i++);
    SET_NAME_VALUES(statementInfo->schema_name, i++);
    values[i++] = UInt32GetDatum(statementInfo->unique_sql_cn_id);
    SET_NAME_VALUES(statementCxt->user_name, i++);

    /* client info */
    SET_TEXT_VALUES(statementInfo->application_name, i++);
    SET_TEXT_VALUES(statementCxt->client_addr, i++);

    if (statementCxt->client_port != INSTR_STMT_NULL_PORT)
        values[i++] = Int32GetDatum(statementCxt->client_port);
    else
        nulls[i++] = true;

    set_stmt_basic_info(statementCxt, statementInfo, values, nulls, &i);
    set_stmt_row_activity_cache_io(statementInfo, values, &i);

    /* time info */
    for (int num = 0; num < TOTAL_TIME_INFO_TYPES; num++) {
        if (num == NET_SEND_TIME)
            continue;
        values[i++] = Int64GetDatum(statementInfo->timeModel[num]);
    }

    /* net info */
    int idx = 0;
    while (idx < TOTAL_NET_INFO_TYPES) {
        char netInfo[STRING_MAX_LEN];
        uint64 time = (uint64)statementInfo->networkInfo[idx++];
        uint64 n_calls = (uint64)statementInfo->networkInfo[idx++];
        uint64 size = (uint64)statementInfo->networkInfo[idx++];
        rc = sprintf_s(netInfo, sizeof(netInfo), "{\"time\":%lu, \"n_calls\":%lu, \"size\":%lu}",
            time, n_calls, size);
        securec_check_ss(rc, "\0", "\0");
        values[i++] = CStringGetTextDatum(netInfo);
    }

    /* lock summary */
    set_stmt_lock_summary(&statementInfo->lock_summary, values, &i);

    /* lock detail */
    if (unlikely(statementInfo->details.oom)) {
        values[i++] = PointerGetDatum(get_statement_detail(&statementInfo->details, true));
    } else if (statementInfo->details.n_items == 0) {
        nulls[i++] = true;
    } else {
        values[i++] = PointerGetDatum(get_statement_detail(&statementInfo->details, false));
    }

    /* is slow sql */
    values[i++] = BoolGetDatum(
        (statementInfo->finish_time - statementInfo->start_time >= statementInfo->slow_query_threshold &&
        statementInfo->slow_query_threshold >= 0) ? true : false);
    SET_TEXT_VALUES(statementInfo->trace_id, i++);
    Assert(INSTR_STATEMENT_ATTRNUM == i);
    return heap_form_tuple(RelationGetDescr(rel), values, nulls);
}

static RangeVar* InitStatementRel()
{
    RangeVar* relrv = makeRangeVar(NULL, NULL, -1);
    relrv->relname = pstrdup("statement_history");
    relrv->schemaname = pstrdup("pg_catalog");
    relrv->catalogname = pstrdup("postgres");
    relrv->relpersistence = RELPERSISTENCE_UNLOGGED;
    return relrv;
}

bool check_statement_retention_time(char** newval, void** extra, GucSource source)
{
    /* Do str copy and remove space. */
    char* strs = TrimStr(*newval);
    if (IS_NULL_STR(strs))
        return false;
    const char* delim = ",";
    List *res = NULL;
    char* next_token = NULL;

    /* Get slow query retention days */
    char* token = strtok_s(strs, delim, &next_token);
    while (token != NULL) {
        res = lappend(res, TrimStr(token));
        token = strtok_s(NULL, delim, &next_token);
    }
    pfree(strs);
    if (res->length != STATEMENT_SQL_KIND) {
        GUC_check_errdetail("attr num:%d is error,track_stmt_retention_time attr is 2", res->length);
        return false;
    }

    int full_sql_retention_sec = 0;
    int slow_query_retention_days = 0;
    /* get full sql retention sec */
    if (!StrToInt32((char*)linitial(res), &full_sql_retention_sec) ||
        !StrToInt32((char*)lsecond(res), &slow_query_retention_days)) {
        GUC_check_errdetail("invalid input syntax");
        return false;
    }

    if (slow_query_retention_days < 0 || slow_query_retention_days > MAX_SLOW_QUERY_RETENSION_DAYS) {
        GUC_check_errdetail("slow_query_retention_days:%d is out of range [%d, %d].",
            slow_query_retention_days, 0, MAX_SLOW_QUERY_RETENSION_DAYS);
        return false;
    }
    if (full_sql_retention_sec < 0 || full_sql_retention_sec > MAX_FULL_SQL_RETENSION_SEC) {
        GUC_check_errdetail("full_sql_retention_sec:%d is out of range [%d, %d].",
            full_sql_retention_sec, 0, MAX_FULL_SQL_RETENSION_SEC);
        return false;
    }
    list_free_deep(res);

    *extra = MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_DFX), STATEMENT_SQL_KIND * sizeof(int));
    ((int*)(*extra))[0] = full_sql_retention_sec;
    ((int*)(*extra))[1] = slow_query_retention_days;
    return true;
}

void assign_statement_retention_time(const char* newval, void* extra)
{
    int *level = (int*) extra;
    t_thrd.statement_cxt.full_sql_retention_time = level[0];
    t_thrd.statement_cxt.slow_sql_retention_time = level[1];
}

static void CleanStatementByIdx(Relation rel, Oid statementTimeIndexId,
    int retentionTime, bool isSlowSQL)
{
    int index_num = 2;
    AttrNumber Anum_statement_history_indrelid = 11;
    AttrNumber Anum_statement_history_slow_sql_id = 51;
    ScanKeyData key[index_num];
    SysScanDesc indesc = NULL;
    HeapTuple tup = NULL;
    TimestampTz minTimestamp = (int64)GetCurrentTimestamp() - retentionTime * USECS_PER_SEC;
    ScanKeyInit(&key[0], Anum_statement_history_indrelid, BTLessStrategyNumber,
        F_TIMESTAMP_LE, TimestampGetDatum(minTimestamp));
    ScanKeyInit(&key[1], Anum_statement_history_slow_sql_id, BTEqualStrategyNumber,
        F_BOOLEQ, BoolGetDatum(isSlowSQL));
    indesc = systable_beginscan(rel, statementTimeIndexId, true, SnapshotSelf, index_num, key);
    while (HeapTupleIsValid(tup = systable_getnext(indesc))) {
        simple_heap_delete(rel, &tup->t_self);
    }
    systable_endscan(indesc);
}

static void StartCleanWorker(int* count)
{
    int maxCleanInterval = 600; // 600 * 100ms = 1min
    if (*count < maxCleanInterval || g_instance.stat_cxt.instr_stmt_is_cleaning) {
        return;
    }
    SendPostmasterSignal(PMSIGNAL_START_CLEAN_STATEMENT);
    g_instance.stat_cxt.instr_stmt_is_cleaning = true;
    *count = 0;
}
/*
 * The statement_history table should be cleaned up twice
 * In the first time, all records less than minStatementTimestamp are cleared,
 * and the last time is based on whether the records are slow query or full sql
 * If the retention time of slow SQL is longer than that of full SQL,
 * we only need to clean up the full SQL record on the last clean-up
 */
static void CleanStatementTable()
{
    List* statement_index = NULL;
    MemoryContext oldcxt = CurrentMemoryContext;
    PG_TRY();
    {
        StartTransactionCommand();
        PushActiveSnapshot(GetTransactionSnapshot());
        /* get statement_history relation and index oid  */
        RangeVar* relrv = InitStatementRel();
        Relation rel = heap_openrv(relrv, RowExclusiveLock);
        if (rel == NULL || (statement_index = RelationGetIndexList(rel)) == NULL) {
            heap_close(rel, RowExclusiveLock);
            ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("get statement_history relation failed")));
        }
        Oid statementTimeIndexId = linitial_oid(statement_index);
        /* clean up statement_history table */
        CleanStatementByIdx(rel, statementTimeIndexId, t_thrd.statement_cxt.slow_sql_retention_time, true);
        CleanStatementByIdx(rel, statementTimeIndexId, t_thrd.statement_cxt.full_sql_retention_time, false);
        heap_close(rel, RowExclusiveLock);
        PopActiveSnapshot();
        CommitTransactionCommand();
    }
    PG_CATCH();
    {
        (void)MemoryContextSwitchTo(oldcxt);
        ErrorData *edata = CopyErrorData();
        ereport(WARNING, (errcode(ERRCODE_WARNING),
            errmsg("[Statement] delete statement_history table failed, cause: %s", edata->message)));
        FlushErrorState();
        FreeErrorData(edata);
        PopActiveSnapshot();
        AbortCurrentTransaction();
    }
    PG_END_TRY();
}

static void FlushStatementToTable(StatementStatContext* suspendList, const knl_u_statement_context* statementCxt)
{
    Assert (suspendList != NULL);
    HeapTuple tuple = NULL;
    StatementStatContext *flushItem = suspendList;

    MemoryContext oldcxt = CurrentMemoryContext;
    uint32 saveInterruptHoldoffCount = t_thrd.int_cxt.InterruptHoldoffCount;
    PG_TRY();
    {
        StartTransactionCommand();
        PushActiveSnapshot(GetTransactionSnapshot());
        RangeVar* relrv = InitStatementRel();
        Relation rel = heap_openrv(relrv, RowExclusiveLock);
        while (flushItem != NULL) {
            tuple = GetStatementTuple(rel, flushItem, statementCxt);
            (void)simple_heap_insert(rel, tuple);
            CatalogUpdateIndexes(rel, tuple);
            heap_freetuple_ext(tuple);
            flushItem = (StatementStatContext *)flushItem->next;
        }
        heap_close(rel, RowExclusiveLock);
        PopActiveSnapshot();
        CommitTransactionCommand();
    }
    PG_CATCH();
    {
        t_thrd.int_cxt.InterruptHoldoffCount = saveInterruptHoldoffCount;
        (void)MemoryContextSwitchTo(oldcxt);
        ErrorData* edata = NULL;
        edata = CopyErrorData();
        FlushErrorState();

        ereport(WARNING, (errmodule(MOD_INSTR),
            errmsg("[Statement] flush suspend list to statement_history failed, reason: '%s'", edata->message)));
        FreeErrorData(edata);
        PopActiveSnapshot();
        AbortCurrentTransaction();
    }
    PG_END_TRY();
}

static void FlushAllStatement()
{
    PgBackendStatus* beentry = t_thrd.shemem_ptr_cxt.BackendStatusArray;
    for (int i = 1; i <= BackendStatusArray_size; i++) {
        StatementStatContext *suspendList = NULL;
        int suspendCnt = 0;

        /* Prevent the release of statementcxt memory when the session exits */
        (void)syscalllockAcquire(&beentry->statement_cxt_lock);

        knl_u_statement_context* statementCxt = (knl_u_statement_context*)beentry->statement_cxt;
        if (statementCxt != NULL) {
            /* read suspend statement list head to local */
            (void)syscalllockAcquire(&statementCxt->list_protect);
            suspendList = (StatementStatContext*)statementCxt->suspendStatementList;
            suspendCnt = statementCxt->suspend_count;
            statementCxt->suspendStatementList = NULL;
            statementCxt->suspend_count = 0;
            (void)syscalllockRelease(&statementCxt->list_protect);
        }

        if (suspendList != NULL) {
            /* flush statement list info to statement_history table */
            FlushStatementToTable(suspendList, statementCxt);

            /* append list to free list */
            (void)syscalllockAcquire(&statementCxt->list_protect);
            void *freeList = statementCxt->toFreeStatementList;
            statementCxt->toFreeStatementList = suspendList;
            while (suspendList->next != NULL) {
                suspendList = (StatementStatContext *)suspendList->next;
            }
            suspendList->next = freeList;
            statementCxt->free_count += suspendCnt;
            (void)syscalllockRelease(&statementCxt->list_protect);
        }

        (void)syscalllockRelease(&beentry->statement_cxt_lock);
        beentry++;
    }
}

static void StatementFlush()
{
    const int flush_usleep_interval = 100000;
    int count = 0;

    while (!t_thrd.statement_cxt.need_exit && ENABLE_STATEMENT_TRACK) {
        ReloadInfo();
        ereport(DEBUG1, (errmodule(MOD_INSTR), errmsg("[Statement] start to flush statemnts.")));
        StartCleanWorker(&count);

        HOLD_INTERRUPTS();
        /* flush all session's statement info to statement_history table */
        FlushAllStatement();
        RESUME_INTERRUPTS();

        count++;
        ereport(DEBUG1, (errmodule(MOD_INSTR), errmsg("[Statement] flush statemnts finished.")));
        /* report statement_history state to pgstat */
        if (OidIsValid(u_sess->proc_cxt.MyDatabaseId))
            pgstat_report_stat(true);
        pg_usleep(flush_usleep_interval);
    }
}

NON_EXEC_STATIC void StatementFlushMain()
{
    char username[NAMEDATALEN] = {'\0'};

    /* we are a postmaster subprocess now */
    IsUnderPostmaster = true;
    JobStatementIAm();
    /* reset MyProcPid */
    t_thrd.proc_cxt.MyProcPid = gs_thread_self();
    /* record Start Time for logging */
    t_thrd.proc_cxt.MyStartTime = time(NULL);

    t_thrd.proc_cxt.MyProgName = "Statement flush thread";
    u_sess->attr.attr_common.application_name = pstrdup("Statement flush thread");
    SetProcessingMode(InitProcessing);
    ProcessSignal();

    /* Early initialization */
    BaseInit();
#ifndef EXEC_BACKEND
    InitProcess();
#endif
    t_thrd.proc_cxt.PostInit->SetDatabaseAndUser((char*)pstrdup(DEFAULT_DATABASE), InvalidOid, username);
    t_thrd.proc_cxt.PostInit->InitStatementWorker();
    SetProcessingMode(NormalProcessing);
    on_shmem_exit(PGXCNodeCleanAndRelease, 0);

    /* Identify myself via ps */
    init_ps_display("statement flush process", "", "", "");
    SetThrdCxt();
    /* Create a resource owner to keep track of our resources. */
    t_thrd.utils_cxt.CurrentResourceOwner = ResourceOwnerCreate(NULL, "Statement Flush",
        THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_DFX));
    u_sess->proc_cxt.MyProcPort->SessionStartTime = GetCurrentTimestamp();
    Reset_Pseudo_CurrentUserId();

    /* initialize current pool handles, it's also only once */
    exec_init_poolhandles();
    pgstat_bestart();
    pgstat_report_appname("statement flush thread");
    ereport(LOG, (errmsg("statement flush thread start")));
    pgstat_report_activity(STATE_IDLE, NULL);

    /* flush statement into statement_history table */
    StatementFlush();
    gs_thread_exit(0);
}


static void SetupSignal(void)
{
    (void)gspqsignal(SIGHUP, SIG_IGN);
    (void)gspqsignal(SIGINT, SIG_IGN);
    (void)gspqsignal(SIGTERM, die); /* cancel current query and exit */
    (void)gspqsignal(SIGQUIT, quickdie);
    (void)gspqsignal(SIGUSR1, procsignal_sigusr1_handler);
    (void)gspqsignal(SIGALRM, SIG_IGN);
    (void)gspqsignal(SIGPIPE, SIG_IGN);
    (void)gspqsignal(SIGUSR2, SIG_IGN);
    (void)gspqsignal(SIGCHLD, SIG_DFL);
    (void)gspqsignal(SIGTTIN, SIG_DFL);
    (void)gspqsignal(SIGTTOU, SIG_DFL);
    (void)gspqsignal(SIGCONT, SIG_DFL);
    (void)gspqsignal(SIGWINCH, SIG_DFL);

    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();
    if (u_sess->proc_cxt.MyProcPort->remote_host)
        pfree(u_sess->proc_cxt.MyProcPort->remote_host);
    u_sess->proc_cxt.MyProcPort->remote_host = pstrdup("localhost");

    t_thrd.wlm_cxt.thread_node_group = &g_instance.wlm_cxt->MyDefaultNodeGroup;  // initialize the default value
    t_thrd.wlm_cxt.thread_climgr = &t_thrd.wlm_cxt.thread_node_group->climgr;
    t_thrd.wlm_cxt.thread_srvmgr = &t_thrd.wlm_cxt.thread_node_group->srvmgr;
}

NON_EXEC_STATIC void CleanStatementMain()
{
    char username[NAMEDATALEN] = {'\0'};

    /* we are a postmaster subprocess now */
    IsUnderPostmaster = true;
    t_thrd.role = TRACK_STMT_CLEANER;
    /* reset MyProcPid */
    t_thrd.proc_cxt.MyProcPid = gs_thread_self();
    /* record Start Time for logging */
    t_thrd.proc_cxt.MyStartTime = time(NULL);

    t_thrd.proc_cxt.MyProgName = "Clean Statement thread";
    u_sess->attr.attr_common.application_name = pstrdup("Clean Statement thread");
    SetProcessingMode(InitProcessing);
    SetupSignal();

    /* Early initialization */
    BaseInit();
#ifndef EXEC_BACKEND
    InitProcess();
#endif
    t_thrd.proc_cxt.PostInit->SetDatabaseAndUser((char*)pstrdup(DEFAULT_DATABASE), InvalidOid, username);
    t_thrd.proc_cxt.PostInit->InitStatementWorker();
    SetProcessingMode(NormalProcessing);
    on_shmem_exit(PGXCNodeCleanAndRelease, 0);

    /* Identify myself via ps */
    init_ps_display("clean statement process", "", "", "");
    SetThrdCxt();
    /* Create a resource owner to keep track of our resources. */
    t_thrd.utils_cxt.CurrentResourceOwner = ResourceOwnerCreate(NULL, "clean Statement",
        THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_DFX));
    u_sess->proc_cxt.MyProcPort->SessionStartTime = GetCurrentTimestamp();
    Reset_Pseudo_CurrentUserId();

    /* initialize current pool handles, it's also only once */
    exec_init_poolhandles();
    pgstat_bestart();
    pgstat_report_appname("clean statement thread");
    ereport(LOG, (errmsg("clean statement thread start")));
    pgstat_report_activity(STATE_IDLE, NULL);

    /* clean statement_history table */
    CleanStatementTable();
    g_instance.stat_cxt.instr_stmt_is_cleaning = false;
    proc_exit(0);
}


size_t get_statement_detail_size(StmtDetailType type)
{
    switch (type) {
        case LOCK_START:
            return LOCK_START_DETAIL_BUFSIZE;
        case LOCK_END:
            return LOCK_END_DETAIL_BUFSIZE;
        case LOCK_WAIT_START:
            return LOCK_WAIT_START_DETAIL_BUFSIZE;
        case LOCK_WAIT_END:
            return LOCK_WAIT_END_DETAIL_BUFSIZE;
        case LOCK_RELEASE:
            return LOCK_RELEASE_START_DETAIL_BUFSIZE;
        case LWLOCK_START:
            return LWLOCK_START_DETAIL_BUFSIZE;
        case LWLOCK_END:
            return LWLOCK_END_DETAIL_BUFSIZE;
        case LWLOCK_WAIT_START:
            return LWLOCK_WAIT_START_DETAIL_BUFSIZE;
        case LWLOCK_WAIT_END:
            return LWLOCK_WAIT_END_DETAIL_BUFSIZE;
        case LWLOCK_RELEASE:
            return LWLOCK_RELEASE_START_DETAIL_BUFSIZE;
        case TYPE_INVALID:
        default:
            return INVALID_DETAIL_BUFSIZE;
    }
}

static void* get_stmt_lock_detail(const StmtLockDetail *detail, TimestampTz curTime, size_t *size)
{
    void *info = NULL;

    switch (detail->type) {
        case LOCK_START:
        case LOCK_WAIT_START:
        case LOCK_RELEASE:
            info = (LockEventStartInfo *)palloc0_noexcept(sizeof(LockEventStartInfo));
            if (info == NULL) {
                break;
            }
            ((LockEventStartInfo *)info)->eventType = (char)detail->type;
            ((LockEventStartInfo *)info)->timestamp = curTime;
            ((LockEventStartInfo *)info)->tag = *detail->locktag;
            ((LockEventStartInfo *)info)->mode = (LOCKMODE)detail->lockmode;
            *size = sizeof(LockEventStartInfo);
            break;
        case LWLOCK_START:
        case LWLOCK_WAIT_START:
        case LWLOCK_RELEASE:
            info = (LWLockEventStartInfo *)palloc0_noexcept(sizeof(LWLockEventStartInfo));
            if (info == NULL) {
                break;
            }
            ((LWLockEventStartInfo *)info)->eventType = (char)detail->type;
            ((LWLockEventStartInfo *)info)->timestamp = curTime;
            ((LWLockEventStartInfo *)info)->id = detail->lwlockId;
            ((LWLockEventStartInfo *)info)->mode = (LWLockMode)detail->lockmode;
            *size = sizeof(LWLockEventStartInfo);
            break;
        case LOCK_END:
        case LOCK_WAIT_END:
        case LWLOCK_END:
        case LWLOCK_WAIT_END:
            info = (LockEventEndInfo *)palloc0_noexcept(sizeof(LockEventEndInfo));
            if (info == NULL) {
                break;
            }
            ((LockEventEndInfo *)info)->eventType = (char)detail->type;
            ((LockEventEndInfo *)info)->timestamp = curTime;
            *size = sizeof(LockEventEndInfo);
            break;
        case TYPE_INVALID:
        default:
            break;
    }

    return info;
}

static bool statement_extend_detail_item(StatementStatContext *ssctx, bool first)
{
    MemoryContext oldcontext = MemoryContextSwitchTo(u_sess->statement_cxt.stmt_stat_cxt);
    StatementDetailItem *item = (StatementDetailItem *)palloc0_noexcept(sizeof(StatementDetailItem));
    (void)MemoryContextSwitchTo(oldcontext);

    if (item == NULL) {
        ereport(LOG, (errmodule(MOD_INSTR), errmsg("[Statement] detail is lost due to OOM.")));
        ssctx->details.oom = true;
        return false;
    }

    if (first) {
        ssctx->details.head = item;
        ssctx->details.tail = item;
        // update list info
        ssctx->details.n_items = 1;
        ssctx->details.head->buf[0] = (char)STATEMENT_DETAIL_VERSION;
        ssctx->details.head->buf[1] = (char)STATEMENT_DETAIL_NOT_TRUNCATED;
        ssctx->details.cur_pos = STATEMENT_DETAILS_HEAD_SIZE;
    } else {
        ssctx->details.tail->next = item;
        ssctx->details.tail = item;
        // update list info
        ssctx->details.n_items++;
        ssctx->details.cur_pos = 0;
    }

    return true;
}

void *palloc_statement_detail(StmtDetailType type)
{
    void *info = NULL;
    switch (type) {
        case LOCK_START:
        case LOCK_WAIT_START:
        case LOCK_RELEASE:
            info = (LockEventStartInfo *)palloc0_noexcept(sizeof(LockEventStartInfo));
            break;
        case LWLOCK_START:
        case LWLOCK_WAIT_START:
        case LWLOCK_RELEASE:
            info = (LWLockEventStartInfo *)palloc0_noexcept(sizeof(LWLockEventStartInfo));
            break;
        case LOCK_END:
        case LOCK_WAIT_END:
        case LWLOCK_END:
        case LWLOCK_WAIT_END:
            info = (LockEventEndInfo *)palloc0_noexcept(sizeof(LockEventEndInfo));
            break;
        case TYPE_INVALID:
        default:
            break;
    }
    return info;
}

const char* get_statement_event_type(StmtDetailType type)
{
    switch (type) {
        case LOCK_START:
            return "LOCK_START";
        case LOCK_WAIT_START:
            return "LOCK_WAIT_START";
        case LOCK_RELEASE:
            return "LOCK_RELEASE";
        case LWLOCK_START:
            return "LWLOCK_START";
        case LWLOCK_WAIT_START:
            return "LWLOCK_WAIT_START";
        case LWLOCK_RELEASE:
            return "LWLOCK_RELEASE";
        case LOCK_END:
            return "LOCK_END";
        case LOCK_WAIT_END:
            return "LOCK_WAIT_END";
        case LWLOCK_END:
            return "LWLOCK_END";
        case LWLOCK_WAIT_END:
            return "LWLOCK_WAIT_END";
        case TYPE_INVALID:
        default:
            return "TYPE_INVALID";
    }
}

static bool check_statement_detail_info_record(StatementStatContext *ssctx, const char *detail)
{
    if (u_sess->attr.attr_common.track_stmt_details_size == 0 || ssctx->details.oom) {
        return false;
    }

    if (ssctx->details.n_items > 0 && u_sess->attr.attr_common.track_stmt_details_size <
        (ssctx->details.n_items - 1) * STATEMENT_DETAIL_BUFSIZE + ssctx->details.cur_pos) {
        ssctx->details.head->buf[1] = (char)STATEMENT_DETAIL_TRUNCATED;
        return false;
    }

    if (detail == NULL) {
        ssctx->details.oom = true;
        return false;
    }

    return true;
}

static void statement_detail_info_record(StatementStatContext *ssctx, const char *detail, size_t detailSize)
{
    errno_t rc;

    if (!check_statement_detail_info_record(ssctx, detail))
        return;

    Assert(detailSize < STATEMENT_DETAIL_BUFSIZE);

    if (ssctx->details.n_items <= 0) {
        if (statement_extend_detail_item(ssctx, true)) {
            rc = memcpy_s(ssctx->details.tail->buf + ssctx->details.cur_pos,
                STATEMENT_DETAIL_BUFSIZE - ssctx->details.cur_pos, detail, detailSize);
            securec_check(rc, "", "");
            ssctx->details.cur_pos += detailSize;
        }
    } else if (ssctx->details.cur_pos + detailSize <= STATEMENT_DETAIL_BUFSIZE) {
        rc = memcpy_s(ssctx->details.tail->buf + ssctx->details.cur_pos,
            STATEMENT_DETAIL_BUFSIZE - ssctx->details.cur_pos, detail, detailSize);
        securec_check(rc, "", "");
        ssctx->details.cur_pos += detailSize;
    } else {
        size_t last = STATEMENT_DETAIL_BUFSIZE - ssctx->details.cur_pos;

        if (last > 0) {
            rc = memcpy_s(ssctx->details.tail->buf + ssctx->details.cur_pos, last, detail, last);
            securec_check(rc, "", "");
        }

        if (statement_extend_detail_item(ssctx, false)) {
            rc = memcpy_s(ssctx->details.tail->buf,
                STATEMENT_DETAIL_BUFSIZE, detail + last, detailSize - last);
            securec_check(rc, "", "");
            ssctx->details.cur_pos += detailSize - last;
        }
    }
}

static void update_stmt_lock_cnt(StatementStatContext *ssctx, StmtDetailType type, int lockmode)
{
    switch (type) {
        case LOCK_START:
            ssctx->lock_summary.lock_cnt++;
            break;
        case LOCK_WAIT_START:
            ssctx->lock_summary.lock_wait_cnt++;
            break;
        case LWLOCK_START:
            ssctx->lock_summary.lwlock_cnt++;
            break;
        case LWLOCK_WAIT_START:
            ssctx->lock_summary.lwlock_wait_cnt++;
            break;
        case LOCK_RELEASE:
            ssctx->lock_summary.lock_hold_cnt--;
            break;
        case LOCK_END:
            if (lockmode != NoLock) {
                ssctx->lock_summary.lock_hold_cnt++;
                ssctx->lock_summary.lock_max_cnt =
                    Max(ssctx->lock_summary.lock_max_cnt, ssctx->lock_summary.lock_hold_cnt);
            }
            break;
        default:
            break;
    }
}

static void update_stmt_lock_time(StatementStatContext *ssctx, StmtDetailType type, TimestampTz curTime)
{
    switch (type) {
        case LOCK_START:
            ssctx->lock_summary.lock_start_time = curTime;
            break;
        case LOCK_END:
            ssctx->lock_summary.lock_time += curTime - ssctx->lock_summary.lock_start_time;
            break;
        case LOCK_WAIT_START:
            ssctx->lock_summary.lock_wait_start_time = curTime;
            break;
        case LOCK_WAIT_END:
            ssctx->lock_summary.lock_wait_time += curTime - ssctx->lock_summary.lock_wait_start_time;
            break;
        case LWLOCK_START:
            ssctx->lock_summary.lwlock_start_time = curTime;
            break;
        case LWLOCK_END:
            ssctx->lock_summary.lwlock_time += curTime - ssctx->lock_summary.lwlock_start_time;
            break;
        case LWLOCK_WAIT_START:
            ssctx->lock_summary.lwlock_wait_start_time = curTime;
            break;
        case LWLOCK_WAIT_END:
            ssctx->lock_summary.lwlock_wait_time += curTime - ssctx->lock_summary.lwlock_wait_start_time;
            break;
        default:
            break;
    }
}

void instr_stmt_report_lock(StmtDetailType type, int lockmode, const LOCKTAG *locktag, uint16 lwlockId)
{
    StatementStatContext *ssctx = (StatementStatContext *)u_sess->statement_cxt.curStatementMetrics;

    if (likely(ssctx != NULL)) {
        Assert(ssctx->level == STMT_TRACK_L0 ||
            ssctx->level == STMT_TRACK_L1 || ssctx->level == STMT_TRACK_L2);

        /* always capture the lock count */
        update_stmt_lock_cnt(ssctx, type, lockmode);

        if (ssctx->level != STMT_TRACK_L0) {
            TimestampTz curTime = GetCurrentTimestamp();

            update_stmt_lock_time(ssctx, type, curTime);
            if (ssctx->level == STMT_TRACK_L2) {
                size_t size = 0;
                char *bytes = NULL;
                StmtLockDetail detail = {type, locktag, lwlockId, lockmode};

                bytes = (char*)get_stmt_lock_detail(&detail, curTime, &size);
                statement_detail_info_record(ssctx, bytes, size);
                pfree_ext(bytes);
            }
        }
    }
}

char *get_lock_mode_name(LOCKMODE mode)
{
    switch (mode) {
        case AccessShareLock:
            return "AccessShareLock";
        case RowShareLock:
            return "RowShareLock";
        case RowExclusiveLock:
            return "RowExclusiveLock";
        case ShareUpdateExclusiveLock:
            return "ShareUpdateExclusiveLock";
        case ShareLock:
            return "ShareLock";
        case ShareRowExclusiveLock:
            return "ShareRowExclusiveLock";
        case ExclusiveLock:
            return "ExclusiveLock";
        case AccessExclusiveLock:
            return "AccessExclusiveLock";
        case NoLock:
        default:
            return "INVALID";
    }
}

void write_state_detail_lock_start(void *info, size_t *detailsCnt, StringInfo resultBuf, bool pretty)
{
    errno_t ret = 0;
    char formatStr[STATEMENT_DETAIL_BUF] = { 0 };
    if (pretty) {
        ret = strcpy_s(formatStr, STATEMENT_DETAIL_BUF, "'%lu'\t'%s'\t'%s'\t'%s'\t'%s'\n");
    } else {
        ret = strcpy_s(formatStr, STATEMENT_DETAIL_BUF, "'%lu'\t'%s'\t'%s'\t'%s'\t'%s',");
    }
    securec_check(ret, "\0", "\0");

    char *lock_tag = LocktagToString(((LockEventStartInfo *)info)->tag);
    appendStringInfo(resultBuf, formatStr, *detailsCnt,
        get_statement_event_type((StmtDetailType)((LockEventStartInfo *)info)->eventType),
        timestamptz_to_str(((LockEventStartInfo *)info)->timestamp),
        lock_tag,
        get_lock_mode_name(((LockEventStartInfo *)info)->mode));
    pfree(lock_tag);
    (*detailsCnt)++;
}

char *get_lwlock_mode_name(LWLockMode mode)
{
    switch (mode) {
        case LW_EXCLUSIVE:
            return "LW_EXCLUSIVE";
        case LW_SHARED:
            return "LW_SHARED";
        case LW_WAIT_UNTIL_FREE:
            return "LW_WAIT_UNTIL_FREE";
        default:
            return "INVALID";
    }
}

void write_state_detail_lwlock_start(void *info, size_t *detailsCnt, StringInfo resultBuf, bool pretty)
{
    errno_t ret = 0;
    char formatStr[STATEMENT_DETAIL_BUF] = { 0 };
    if (pretty) {
        ret = strcpy_s(formatStr, STATEMENT_DETAIL_BUF, "'%lu'\t'%s'\t'%s'\t'%s'\t'%s'\n");
    } else {
        ret = strcpy_s(formatStr, STATEMENT_DETAIL_BUF, "'%lu'\t'%s'\t'%s'\t'%s'\t'%s',");
    }
    securec_check(ret, "\0", "\0");
    appendStringInfo(resultBuf, formatStr, *detailsCnt,
        get_statement_event_type((StmtDetailType)((LWLockEventStartInfo *)info)->eventType),
        timestamptz_to_str(((LWLockEventStartInfo *)info)->timestamp),
        GetLWLockIdentifier(PG_WAIT_LWLOCK, ((LWLockEventStartInfo *)info)->id),
        get_lwlock_mode_name(((LWLockEventStartInfo *)info)->mode));
    (*detailsCnt)++;
}

void write_state_detail_end_event(void *info, size_t *detailsCnt, StringInfo resultBuf, bool pretty)
{
    errno_t ret = 0;
    char formatStr[STATEMENT_DETAIL_BUF] = { 0 };
    if (pretty) {
        ret = strcpy_s(formatStr, STATEMENT_DETAIL_BUF, "'%lu'\t'%s'\t'%s'\n");
    } else {
        ret = strcpy_s(formatStr, STATEMENT_DETAIL_BUF, "'%lu'\t'%s'\t'%s',");
    }
    securec_check(ret, "\0", "\0");
    appendStringInfo(resultBuf, formatStr, *detailsCnt,
        get_statement_event_type((StmtDetailType)((LockEventEndInfo *)info)->eventType),
        timestamptz_to_str(((LockEventEndInfo *)info)->timestamp));
    (*detailsCnt)++;
}

/* return true if run successfully */
bool write_statement_detail_text(void *info, StmtDetailType type, StringInfo resultBuf, size_t *detailsCnt, bool pretty)
{
    bool result = false;
    switch (type) {
        case LOCK_START:
        case LOCK_WAIT_START:
        case LOCK_RELEASE:
            write_state_detail_lock_start(info, detailsCnt, resultBuf, pretty);
            result = true;
            break;
        case LWLOCK_START:
        case LWLOCK_WAIT_START:
        case LWLOCK_RELEASE:
            write_state_detail_lwlock_start(info, detailsCnt, resultBuf, pretty);
            result = true;
            break;
        case LOCK_END:
        case LOCK_WAIT_END:
        case LWLOCK_END:
        case LWLOCK_WAIT_END:
            write_state_detail_end_event(info, detailsCnt, resultBuf, pretty);
            result = true;
            break;
        case TYPE_INVALID:
        default:
            break;
    }
    return result;
}

static bytea* get_statement_detail(StatementDetail *detail, bool oom)
{
    text *result = NULL;
    uint32 size;

    Assert (detail != NULL && (oom || detail->n_items > 0));

    if (oom) {
        size = sizeof(uint32) + STATEMENT_DETAILS_HEAD_SIZE;
    } else {
        size = sizeof(uint32) + (uint32)((detail->n_items - 1) * STATEMENT_DETAIL_BUFSIZE) + detail->cur_pos;
    }

    result = (text*)palloc(size + VARHDRSZ);
    SET_VARSIZE(result, size + VARHDRSZ);

    int rc = memcpy_s(VARDATA(result), size, &size, sizeof(uint32));
    securec_check(rc, "\0", "\0");

    char *details = VARDATA(result) + sizeof(uint32);
    if (oom) {
        details[0] = (char)STATEMENT_DETAIL_VERSION;
        details[1] = (char)STATEMENT_DETAIL_MISSING_OOM;
    } else {
        uint32 pos = 0;
        size_t itemSize;

        for (StatementDetailItem *item = detail->head; item != NULL; item = (StatementDetailItem *)(item->next)) {
            itemSize = (item == detail->tail) ? detail->cur_pos : STATEMENT_DETAIL_BUFSIZE;

            rc = memcpy_s(details + pos, (size - pos - sizeof(uint32)), item->buf, itemSize);
            securec_check(rc, "\0", "\0");
            pos += itemSize;
        }
    }

    return result;
}

static void handle_detail_flag(char flag, StringInfo result)
{
    if (flag == STATEMENT_DETAIL_TRUNCATED) {
        appendStringInfoString(result, "Truncated...");
    } else if (flag == STATEMENT_DETAIL_MISSING_OOM) {
        appendStringInfoString(result, "Missing(OOM)...");
    }
}

static char *decode_statement_detail_text(bytea *detail, const char *format, bool pretty)
{
    if (strcmp(format, STATEMENT_DETAIL_FORMAT_STRING) != 0) {
        ereport(WARNING, ((errmodule(MOD_INSTR), errmsg("decode format should be 'plaintext'!"))));
        return NULL;
    }

    uint32 bytea_data_len = VARSIZE(detail) - VARHDRSZ;
    if (bytea_data_len == 0) {
        return NULL;
    }

    char *details = (char *)VARDATA(detail);
    uint32 detailsLen = 0;
    int rc = memcpy_s(&detailsLen, sizeof(uint32), details, sizeof(uint32));
    securec_check(rc, "\0", "\0");

    if (!is_valid_detail_record(bytea_data_len, details, detailsLen)) {
        return pstrdup("invalid_detail_header_format");
    }

    StringInfoData resultBuf;
    initStringInfo(&resultBuf);
    char flag = details[sizeof(uint32) + 1];
    size_t pos = sizeof(uint32) + STATEMENT_DETAILS_HEAD_SIZE;
    bool is_valid_record = true;
    size_t detailsCnt = 1;

    while (pos < detailsLen && is_valid_record) {
        StmtDetailType itemType = (StmtDetailType)details[pos];
        size_t size = get_statement_detail_size(itemType);
        if (size == INVALID_DETAIL_BUFSIZE) {
            is_valid_record = false;
            break;
        }
        if (pos + size > detailsLen) {
            /* invalid buf */
            is_valid_record = false;
            break;
        }
        void *info = palloc_statement_detail(itemType);
        if (info == NULL) {
            break;
        }
        rc = memcpy_s(info, size, details + pos, size);
        securec_check(rc, "", "");
        pos += size;

        /* write to string */
        is_valid_record = write_statement_detail_text(info, itemType, &resultBuf, &detailsCnt, pretty);
        pfree(info);
    }
    if (!is_valid_record) {
        pfree(resultBuf.data);
        return pstrdup("invalid_detail_data_format");
    }

    /* after is_valid_detail_record called, resultBuf.len never equal to 0 */
    resultBuf.data[resultBuf.len - 1] = ' ';
    handle_detail_flag(flag, &resultBuf);
    return resultBuf.data;
}

Datum statement_detail_decode(PG_FUNCTION_ARGS)
{
    if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2)) {
        PG_RETURN_NULL();
    }
    bytea *detailText = DatumGetByteaP(PG_GETARG_DATUM(0));
    char *format = TextDatumGetCString(PG_GETARG_DATUM(1));
    bool pretty = PG_GETARG_BOOL(2);

    char *detailStr = decode_statement_detail_text(detailText, format, pretty);
    if (detailStr == NULL) {
        PG_RETURN_NULL();
    }

    text* result = cstring_to_text(detailStr);
    pfree(detailStr);
    PG_RETURN_TEXT_P(result);
}

static void instr_stmt_check_need_update(bool* to_update_db_name,
    bool* to_update_user_name, bool* to_update_client_addr)
{
    PgBackendStatus* beentry = t_thrd.shemem_ptr_cxt.MyBEEntry;

    if (u_sess->statement_cxt.db_name == NULL && OidIsValid(beentry->st_databaseid)) {
        *to_update_db_name = true;
    }

    if (u_sess->statement_cxt.user_name == NULL && OidIsValid(beentry->st_userid)) {
        *to_update_user_name = true;
    }

    if ((u_sess->statement_cxt.client_addr == NULL && u_sess->statement_cxt.client_port == INSTR_STMT_NULL_PORT) &&
        (beentry->st_clientaddr.addr.ss_family == AF_INET || beentry->st_clientaddr.addr.ss_family == AF_INET6 ||
        beentry->st_clientaddr.addr.ss_family == AF_UNIX)) {
        *to_update_client_addr = true;
    }
}

static void instr_stmt_update_client_info(const PgBackendStatus* beentry)
{
    SockAddr zero_clientaddr;
    errno_t rc = memset_s(&zero_clientaddr, sizeof(SockAddr), 0, sizeof(SockAddr));
    securec_check(rc, "\0", "\0");

    u_sess->statement_cxt.client_port = INSTR_STMT_NULL_PORT;
    if (memcmp(&(beentry->st_clientaddr), &zero_clientaddr, sizeof(SockAddr)) != 0) {
        if (beentry->st_clientaddr.addr.ss_family == AF_INET
#ifdef HAVE_IPV6
            || beentry->st_clientaddr.addr.ss_family == AF_INET6
#endif
        ) {
            char client_host[NI_MAXHOST] = {0};
            char client_port[NI_MAXSERV] = {0};

            int ret = pg_getnameinfo_all(&beentry->st_clientaddr.addr, beentry->st_clientaddr.salen,
                client_host, sizeof(client_host), client_port, sizeof(client_port),
                NI_NUMERICHOST | NI_NUMERICSERV);
            if (ret == 0) {
                clean_ipv6_addr(beentry->st_clientaddr.addr.ss_family, client_host);
                u_sess->statement_cxt.client_addr = pstrdup(client_host);
                u_sess->statement_cxt.client_port = atoi(client_port);
            }
        } else if (beentry->st_clientaddr.addr.ss_family == AF_UNIX) {
            u_sess->statement_cxt.client_port = INSTR_STMT_UNIX_DOMAIN_PORT;
        }
    }
}

/* Report basic information of statement, including:
 * - database name
 * - node name
 * - user name
 * - client connection
 * - session ID
 */
void instr_stmt_report_basic_info()
{
    if (t_thrd.shemem_ptr_cxt.MyBEEntry == NULL || u_sess->statement_cxt.stmt_stat_cxt == NULL) {
        return;
    }

    bool to_update_db_name = false;
    bool to_update_user_name = false;
    bool to_update_client_addr = false;
    instr_stmt_check_need_update(&to_update_db_name, &to_update_user_name, &to_update_client_addr);

    PgBackendStatus* beentry = t_thrd.shemem_ptr_cxt.MyBEEntry;
    if (u_sess->statement_cxt.session_id == 0) {
        u_sess->statement_cxt.session_id = ENABLE_THREAD_POOL ? beentry->st_sessionid : beentry->st_procpid;
    }
    if (to_update_db_name || to_update_user_name || to_update_client_addr) {
        ResourceOwner old_cur_owner = t_thrd.utils_cxt.CurrentResourceOwner;
        t_thrd.utils_cxt.CurrentResourceOwner = ResourceOwnerCreate(old_cur_owner, "Full/Slow SQL",
            THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_DFX));

        MemoryContext old_ctx = MemoryContextSwitchTo(u_sess->statement_cxt.stmt_stat_cxt);
        if (to_update_db_name) {
            u_sess->statement_cxt.db_name = get_database_name(beentry->st_databaseid);
        }
        if (to_update_user_name) {
            u_sess->statement_cxt.user_name = GetUserNameById(beentry->st_userid);
        }
        if (to_update_client_addr) {
            instr_stmt_update_client_info(beentry);
        }
        (void)MemoryContextSwitchTo(old_ctx);

        ResourceOwnerRelease(t_thrd.utils_cxt.CurrentResourceOwner, RESOURCE_RELEASE_BEFORE_LOCKS, true, true);
        ResourceOwnerRelease(t_thrd.utils_cxt.CurrentResourceOwner, RESOURCE_RELEASE_LOCKS, true, true);
        ResourceOwnerRelease(t_thrd.utils_cxt.CurrentResourceOwner, RESOURCE_RELEASE_AFTER_LOCKS, true, true);
        ResourceOwner tmpOwner = t_thrd.utils_cxt.CurrentResourceOwner;
        t_thrd.utils_cxt.CurrentResourceOwner = old_cur_owner;
        ResourceOwnerDelete(tmpOwner);
    }
}

void instr_stmt_report_start_time()
{
    CHECK_STMT_HANDLE();
    CURRENT_STMT_METRIC_HANDLE->start_time = GetCurrentTimestamp();
}

void instr_stmt_report_finish_time()
{
    CHECK_STMT_HANDLE();
    CURRENT_STMT_METRIC_HANDLE->finish_time = GetCurrentTimestamp();
}

void instr_stmt_report_debug_query_id(uint64 debug_query_id)
{
    CHECK_STMT_HANDLE();
    CURRENT_STMT_METRIC_HANDLE->debug_query_id = debug_query_id;
}

void instr_stmt_report_trace_id(char *trace_id)
{
    CHECK_STMT_HANDLE();
    errno_t rc =
        memcpy_s(CURRENT_STMT_METRIC_HANDLE->trace_id, MAX_TRACE_ID_SIZE, trace_id, strlen(trace_id) + 1);
    securec_check(rc, "\0", "\0");
}

inline void instr_stmt_track_param_query(const char *query)
{
    if (query == NULL) {
        return;
    }
    if (CURRENT_STMT_METRIC_HANDLE->params == NULL) {
        CURRENT_STMT_METRIC_HANDLE->query = pstrdup(query);
    } else {
        Size len = strlen(query) + strlen(CURRENT_STMT_METRIC_HANDLE->params) + 2;
        CURRENT_STMT_METRIC_HANDLE->query = (char *)palloc(len * sizeof(char));
        errno_t rc = sprintf_s(CURRENT_STMT_METRIC_HANDLE->query, len, "%s;%s",
                               query, CURRENT_STMT_METRIC_HANDLE->params);
        securec_check_ss_c(rc, "\0", "\0");
        pfree_ext(CURRENT_STMT_METRIC_HANDLE->params);
   }
}

/* using unique query */
void instr_stmt_report_query(uint64 unique_query_id)
{
    CHECK_STMT_HANDLE();
    CURRENT_STMT_METRIC_HANDLE->unique_query_id = unique_query_id;
    CURRENT_STMT_METRIC_HANDLE->unique_sql_cn_id = u_sess->unique_sql_cxt.unique_sql_cn_id;

    if (likely(!is_local_unique_sql() || CURRENT_STMT_METRIC_HANDLE->query)) {
        return;
    }

    MemoryContext old_ctx = MemoryContextSwitchTo(u_sess->statement_cxt.stmt_stat_cxt);

    if (!u_sess->attr.attr_common.track_stmt_parameter) {
        CURRENT_STMT_METRIC_HANDLE->query = FindCurrentUniqueSQL();
    } else {
        const char* query_string = NULL;
        char* mask_string = NULL;

        if (u_sess->unique_sql_cxt.curr_single_unique_sql != NULL) {
            query_string = u_sess->unique_sql_cxt.curr_single_unique_sql;
        } else {
            query_string = t_thrd.postgres_cxt.debug_query_string;
        }

        if (query_string == NULL) {
            return;
        }

        mask_string = maskPassword(query_string);
        if (mask_string == NULL) {
            mask_string = (char*)query_string;
        }

        instr_stmt_track_param_query(mask_string);

        if (mask_string != query_string) {
            pfree(mask_string);
        }
    }

    (void)MemoryContextSwitchTo(old_ctx);
}

void instr_stmt_report_txid(uint64 txid)
{
    CHECK_STMT_HANDLE();
    CURRENT_STMT_METRIC_HANDLE->txn_id = txid;
}

/*
 * Entry method to report stat at handle init stage
 * - txn_id/basic info/debug query id
 */
void instr_stmt_report_stat_at_handle_init()
{
    CHECK_STMT_HANDLE();
    /* report transaction id */
    if (CURRENT_STMT_METRIC_HANDLE->txn_id == InvalidTransactionId) {
        instr_stmt_report_txid(GetCurrentTransactionIdIfAny());
    }

    /* save current statement stat level */
    CURRENT_STMT_METRIC_HANDLE->level =
        (StatLevel)Max(u_sess->statement_cxt.statement_level[0], u_sess->statement_cxt.statement_level[1]);

    /* fill basic information */
    instr_stmt_report_basic_info();

    /* fill debug_query_id */
    instr_stmt_report_debug_query_id(u_sess->debug_query_id);

    /* when sql execute error on remote node, unique sql id will be reset,
     * update unique sql info at commit stage will be late
     */
    if (IsConnFromCoord() && CURRENT_STMT_METRIC_HANDLE->unique_query_id == 0 &&
        u_sess->unique_sql_cxt.unique_sql_id != 0) {
        instr_stmt_report_query(u_sess->unique_sql_cxt.unique_sql_id);
    }
}

/*
 * Entry method to report stat at handle commit stage
 * - schema name/application name
 */
void instr_stmt_report_stat_at_handle_commit()
{
    CHECK_STMT_HANDLE();

    MemoryContext old_ctx = MemoryContextSwitchTo(u_sess->statement_cxt.stmt_stat_cxt);
    CURRENT_STMT_METRIC_HANDLE->schema_name = pstrdup(u_sess->attr.attr_common.namespace_search_path);
    CURRENT_STMT_METRIC_HANDLE->application_name = pstrdup(u_sess->attr.attr_common.application_name);

    /* unit: microseconds */
    CURRENT_STMT_METRIC_HANDLE->slow_query_threshold =
        (int64)u_sess->attr.attr_storage.log_min_duration_statement * 1000;

    CURRENT_STMT_METRIC_HANDLE->tid = t_thrd.proc_cxt.MyProcPid;

    /* sql from remote node */
    if (IsConnFromCoord() && CURRENT_STMT_METRIC_HANDLE->unique_query_id == 0 &&
        u_sess->unique_sql_cxt.unique_sql_id != 0) {
        instr_stmt_report_query(u_sess->unique_sql_cxt.unique_sql_id);
    }

    if (CURRENT_STMT_METRIC_HANDLE->txn_id == InvalidTransactionId) {
        instr_stmt_report_txid(GetCurrentTransactionIdIfAny());
    }

    instr_stmt_report_finish_time();
    (void)MemoryContextSwitchTo(old_ctx);
}

void instr_stmt_report_soft_parse(uint64 soft_parse)
{
    CHECK_STMT_HANDLE();
    CURRENT_STMT_METRIC_HANDLE->parse.soft_parse += soft_parse;
}

void instr_stmt_report_hard_parse(uint64 hard_parse)
{
    CHECK_STMT_HANDLE();
    CURRENT_STMT_METRIC_HANDLE->parse.hard_parse += hard_parse;
}

void instr_stmt_report_returned_rows(uint64 returned_rows)
{
    CHECK_STMT_HANDLE();
    CURRENT_STMT_METRIC_HANDLE->row_activity.returned_rows += returned_rows;
}

void instr_stmt_report_unique_sql_info(const PgStat_TableCounts *agg_table_stat,
    const int64 timeInfo[], const uint64 *netInfo)
{
    if (u_sess->unique_sql_cxt.unique_sql_id == 0 || u_sess->statement_cxt.curStatementMetrics == NULL) {
        return;
    }

    StatementStatContext *ssctx = (StatementStatContext *)u_sess->statement_cxt.curStatementMetrics;
    if (agg_table_stat != NULL) {
        // row activity
        (void)pg_atomic_fetch_add_u64(&ssctx->row_activity.tuples_fetched, (uint64)agg_table_stat->t_tuples_fetched);
        (void)pg_atomic_fetch_add_u64(&ssctx->row_activity.tuples_returned, (uint64)agg_table_stat->t_tuples_returned);

        (void)pg_atomic_fetch_add_u64(&ssctx->row_activity.tuples_inserted, (uint64)agg_table_stat->t_tuples_inserted);
        (void)pg_atomic_fetch_add_u64(&ssctx->row_activity.tuples_updated, (uint64)agg_table_stat->t_tuples_updated);
        (void)pg_atomic_fetch_add_u64(&ssctx->row_activity.tuples_deleted, (uint64)agg_table_stat->t_tuples_deleted);

        // cache_io
        (void)pg_atomic_fetch_add_u64(&ssctx->cache_io.blocks_fetched, (uint64)agg_table_stat->t_blocks_fetched);
        (void)pg_atomic_fetch_add_u64(&ssctx->cache_io.blocks_hit, (uint64)agg_table_stat->t_blocks_hit);
    }

    if (timeInfo != NULL) {
        for (int idx = 0; idx < TOTAL_TIME_INFO_TYPES; idx++) {
            (void)gs_atomic_add_64(&ssctx->timeModel[idx], timeInfo[idx]);
        }
    }

    if (netInfo != NULL) {
        for (int i = 0; i < TOTAL_NET_INFO_TYPES; i++) {
            (void)pg_atomic_fetch_add_u64(&ssctx->networkInfo[i], netInfo[i]);
        }
    }
}

bool instr_stmt_need_track_plan()
{
    if (CURRENT_STMT_METRIC_HANDLE == NULL || CURRENT_STMT_METRIC_HANDLE->level <= STMT_TRACK_L0 ||
        CURRENT_STMT_METRIC_HANDLE->level > STMT_TRACK_L2)
        return false;

    return true;
}

void instr_stmt_report_query_plan(QueryDesc *queryDesc)
{
    StatementStatContext *ssctx = (StatementStatContext *)u_sess->statement_cxt.curStatementMetrics;
    if (queryDesc == NULL || ssctx == NULL || ssctx->level <= STMT_TRACK_L0
        || ssctx->level > STMT_TRACK_L2 || ssctx->plan_size != 0 || u_sess->statement_cxt.executer_run_level > 1) {
        return;
    }
    /* when getting plan directly from CN, the plan is partial, deparse plan will be failed,
     * it's reasonable, as CN has the full plan.
     */
    if (u_sess->exec_cxt.under_stream_runtime && IS_PGXC_DATANODE) {
        return;
    }

    ExplainState es;
    explain_querydesc(&es, queryDesc);

    MemoryContext oldcontext = MemoryContextSwitchTo(u_sess->statement_cxt.stmt_stat_cxt);
    ssctx->query_plan = (char*)palloc0((Size)es.str->len + 1);
    errno_t rc =
        memcpy_s(ssctx->query_plan, (size_t)es.str->len, es.str->data, (size_t)es.str->len);
    securec_check(rc, "\0", "\0");
    ssctx->query_plan[es.str->len] = '\0';
    ssctx->plan_size = (uint64)es.str->len + 1;
    (void)MemoryContextSwitchTo(oldcontext);

    ereport(DEBUG1,
        (errmodule(MOD_INSTR), errmsg("exec_auto_explain %s %s to %lu",
            ssctx->query_plan, queryDesc->sourceText, u_sess->unique_sql_cxt.unique_sql_id)));
    pfree(es.str->data);
}

/* check the header and valid length of the detail binary data */
static bool is_valid_detail_record(uint32 bytea_data_len, const char *details, uint32 details_len)
{
    /* VERSIZE(bytea) - VARHDRSZ should be > sizeof(uint32) + STATEMENT_DETAILS_HEAD_SIZE */
    if (bytea_data_len <= (sizeof(uint32) + STATEMENT_DETAILS_HEAD_SIZE)) {
        return false;
    }

    /* details binary length should be same with VERSIZE(bytea) - VARHDRSZ */
    if (bytea_data_len != details_len) {
        return false;
    }

    /* record version, should be [1, STATEMENT_DETAIL_VERSION] */
    int32 version = (int32)details[sizeof(uint32)];
    if (version == 0 || version > STATEMENT_DETAIL_VERSION) {
        return false;
    }

    /* record flag area, should be
     *   STATEMENT_DETAIL_NOT_TRUNCATED/STATEMENT_DETAIL_TRUNCATED/STATEMENT_DETAIL_MISSING_OOM
     */
    int32 flag = (int32)details[sizeof(uint32) + 1];
    if (flag != STATEMENT_DETAIL_NOT_TRUNCATED &&
        flag != STATEMENT_DETAIL_TRUNCATED &&
        flag != STATEMENT_DETAIL_MISSING_OOM) {
        return false;
    }

    return true;
}

void instr_stmt_dynamic_change_level()
{
    CHECK_STMT_HANDLE();

    /* if find user defined statement level, dynamic change the statement track level */
    StatLevel specified_level = instr_track_stmt_find_level();
    if (specified_level <= STMT_TRACK_OFF) {
        return;
    }

    /* at commit stage, for dynamic track utility, need a symbol to decide flush the handle or not */
    CURRENT_STMT_METRIC_HANDLE->level = (specified_level > CURRENT_STMT_METRIC_HANDLE->level) ?
        specified_level : CURRENT_STMT_METRIC_HANDLE->level;
    CURRENT_STMT_METRIC_HANDLE->dynamic_track_level = specified_level;
    ereport(DEBUG1, (errmodule(MOD_INSTR), errmsg("[Statement] change (%lu) track level to L%d",
        u_sess->unique_sql_cxt.unique_sql_id, CURRENT_STMT_METRIC_HANDLE->level - 1)));
}
