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
#include "instruments/instr_mfchain.h"
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
#include "storage/lmgr.h"
#include "instruments/instr_func_control.h"

#define MAX_SLOW_QUERY_RETENSION_DAYS 604800
#define MAX_FULL_SQL_RETENSION_SEC 86400
#define IS_NULL_STR(str) ((str) == NULL || (str)[0] == '\0')
#define STRING_MAX_LEN 256

#define STATEMENT_DETAILS_HEAD_SIZE (1)     /* [VERSION] */
#define INSTR_STMT_UNIX_DOMAIN_PORT (-1)
#define INSTR_STATEMENT_ATTRNUM (45 + TOTAL_TIME_INFO_TYPES)

/* support different areas in stmt detail column */
#define STATEMENT_DETAIL_TYPE_LEN (1)
#define STATEMENT_DETAIL_DATA_LEN (4)

#define STATEMENT_DETAIL_TYPE_EVENT 'A'
#define STATEMENT_DETAIL_TYPE_LOCK 'B'

#define STATEMENT_EVENT_TYPE_UNKNOWN     (0)
#define STATEMENT_EVENT_TYPE_IO          (1)
#define STATEMENT_EVENT_TYPE_LOCK        (2)
#define STATEMENT_EVENT_TYPE_LWLOCK      (3)
#define STATEMENT_EVENT_TYPE_STATUS      (4)
#define STATEMENT_EVENT_TYPE_DMS         (5)

/* stbyStmtHistSlow and stbyStmtHistFast always have the same life cycle. */
#define STBYSTMTHIST_IS_READY (g_instance.stat_cxt.stbyStmtHistSlow->state != MFCHAIN_STATE_NOT_READY)


/* lock/lwlock's detail information */
typedef struct {
    StmtDetailType type;        /* statement details kind. */
    const LOCKTAG *locktag;     /* for Lock, this field is used. */
    uint16 lwlockId;            /* for LWLock, this field is used. */
    int lockmode;               /* lockmode for this request. */
} StmtLockDetail;

typedef struct {
    char   *detail;
    uint32 max_detail_len;
    uint32 cur_offset;
    uint32 data_len;
} AreaInfo;

typedef struct {
    const char *event_type;
    const char *event_name;
    uint64 duration;
} StmtEventRecord;

static const int wait_event_io_event_max_index = IO_EVENT_NUM;
static const int wait_event_dms_event_max_index = wait_event_io_event_max_index + DMS_EVENT_NUM;
static const int wait_event_lock_event_max_index = wait_event_dms_event_max_index + LOCK_EVENT_NUM;
static const int wait_event_lwlock_event_max_index = wait_event_lock_event_max_index + LWLOCK_EVENT_NUM;
static const int wait_event_state_wait_max_index = wait_event_lwlock_event_max_index + STATE_WAIT_NUM;

static bytea* get_statement_detail(StatementDetail *text, StringInfo wait_events_info, bool oom);
static bool is_valid_detail_record(uint32 bytea_data_len, const char *details, uint32 *details_len);
static void get_wait_events_full_info(StatementStatContext *statement_stat, StringInfo wait_events_info);
static void decode_stmt_locks(
    StringInfo resultBuf, const char *details, uint32 detailsLen, bool *is_valid_record, bool pretty);
static void mark_session_bms(uint32 index, uint64 total_duration, bool is_init = true);
static bool instr_stmt_get_event_data(int32 event_idx, uint8 *event_type, int32 *event_real_id, uint64 *total_duration);

static uint32 calc_area_data_len(uint32 area_total_data_len)
{
    return area_total_data_len - STATEMENT_DETAIL_TYPE_LEN - STATEMENT_DETAIL_DATA_LEN;
}

static RangeVar* InitStatementRel();

/* using for standby statement history */
static void StartStbyStmtHistory();
static void ShutdownStbyStmtHistory();
static void AssignStbyStmtHistoryConf();
static void CleanStbyStmtHistory();

/* in standby, We record slow-sql and other queries separately, this is different from the primary. */
static void StartStbyStmtHistory()
{
    if (pmState != PM_HOT_STANDBY || STBYSTMTHIST_IS_READY) {
        return;
    }

    RangeVar* relrv = InitStatementRel();
    Relation rel = heap_openrv(relrv, AccessShareLock);

    MemFileChainCreateParam param;
    param.notInit = false;
    param.parent = g_instance.instance_context;
    param.rel = rel;
    param.dir = "dbe_perf_standby";
    param.needClean = t_thrd.proc->workingVersionNum < STANDBY_STMTHIST_VERSION_NUM ? true : false;

    param.name = "standby_statement_history_fast";
    param.maxBlockNumM = t_thrd.statement_cxt.fast_max_mblock;
    param.maxBlockNum = t_thrd.statement_cxt.fast_max_block;
    param.retentionTime = t_thrd.statement_cxt.full_sql_retention_time;
    param.lock  = GetMainLWLockByIndex(FirstStandbyStmtHistLock + 0);
    param.initTarget = g_instance.stat_cxt.stbyStmtHistFast;
    MemFileChainCreate(&param);

    param.name = "standby_statement_history_slow";
    param.maxBlockNumM = t_thrd.statement_cxt.slow_max_mblock;
    param.maxBlockNum = t_thrd.statement_cxt.slow_max_block;
    param.retentionTime = t_thrd.statement_cxt.slow_sql_retention_time;
    param.lock  = GetMainLWLockByIndex(FirstStandbyStmtHistLock + 1);
    param.initTarget = g_instance.stat_cxt.stbyStmtHistSlow;
    MemFileChainCreate(&param);

    heap_close(rel, AccessShareLock);
}

static void ShutdownStbyStmtHistory()
{
    if (!STBYSTMTHIST_IS_READY || u_sess->attr.attr_storage.DefaultXactReadOnly) {
        return;
    }

    MemFileChainDestory(g_instance.stat_cxt.stbyStmtHistFast);
    MemFileChainDestory(g_instance.stat_cxt.stbyStmtHistSlow);
}

static void AssignStbyStmtHistoryConf()
{
    if (pmState != PM_HOT_STANDBY || !STBYSTMTHIST_IS_READY) {
        return;
    }

    MemFileChainRegulate(g_instance.stat_cxt.stbyStmtHistSlow,
                         MFCHAIN_ASSIGN_NEW_SIZE,
                         t_thrd.statement_cxt.slow_max_mblock,
                         t_thrd.statement_cxt.slow_max_block,
                         t_thrd.statement_cxt.slow_sql_retention_time);
    MemFileChainRegulate(g_instance.stat_cxt.stbyStmtHistFast,
                         MFCHAIN_ASSIGN_NEW_SIZE,
                         t_thrd.statement_cxt.fast_max_mblock,
                         t_thrd.statement_cxt.fast_max_block,
                         t_thrd.statement_cxt.full_sql_retention_time);
}

static void CleanStbyStmtHistory()
{
    if (pmState != PM_HOT_STANDBY && STBYSTMTHIST_IS_READY) {
        ShutdownStbyStmtHistory();
        return;
    }

    if (!STBYSTMTHIST_IS_READY) {
        return;
    }

    MemFileChainRegulate(g_instance.stat_cxt.stbyStmtHistSlow, MFCHAIN_TRIM_OLDEST);
    MemFileChainRegulate(g_instance.stat_cxt.stbyStmtHistFast, MFCHAIN_TRIM_OLDEST);
}


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
        AssignStbyStmtHistoryConf();
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
    (void)gspqsignal(SIGURG, print_stack);
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

static void set_stmt_advise(StatementStatContext* statementInfo, Datum values[], bool nulls[], int* i)
{
    if (statementInfo->cause_type == 0)
        nulls[(*i)++] = true;
    else {
        errno_t rc;
        char causeInfo[STRING_MAX_LEN];
        rc = memset_s(causeInfo, sizeof(causeInfo), 0, sizeof(causeInfo));
        securec_check(rc, "\0", "\0");
        if (statementInfo->cause_type & NUM_F_TYPECASTING) {
            rc = strcat_s(causeInfo, sizeof(causeInfo), "Cast Function Cause Index Miss. ");
            securec_check(rc, "\0", "\0");
        }
        if (statementInfo->cause_type & NUM_F_LIMIT) {
            rc = strcat_s(causeInfo, sizeof(causeInfo), "Limit too much rows.");
            securec_check(rc, "\0", "\0");
        }
        if (statementInfo->cause_type & NUM_F_LEAKPROOF) {
            rc = strcat_s(causeInfo, sizeof(causeInfo), "Proleakproof of function is false.");
            securec_check(rc, "\0", "\0");
        }
        values[(*i)++] = CStringGetTextDatum(causeInfo);
    }
}

static HeapTuple GetStatementTuple(Relation rel, StatementStatContext* statementInfo,
    const knl_u_statement_context* statementCxt, bool* isSlow = NULL)
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
    for (int num = 0; num < TOTAL_TIME_INFO_TYPES_P1; num++) {
        if (num == NET_SEND_TIME) {
            continue;
        }
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

    /* lock + wait event detail */
    if (statementInfo->details.n_items == 0 && bms_num_members(statementInfo->wait_events_bitmap) == 0) {
        nulls[i++] = true;
    } else {
        /* generate stmt wait events */
        StringInfoData wait_events_info;
        initStringInfo(&wait_events_info);
        get_wait_events_full_info(statementInfo, &wait_events_info);
        values[i++] = PointerGetDatum(get_statement_detail(&statementInfo->details,
            &wait_events_info, statementInfo->details.oom));
        if (wait_events_info.data != NULL) {
            FreeStringInfo(&wait_events_info);
        }
    }

    /* is slow sql */
    values[i++] = BoolGetDatum(
        (statementInfo->finish_time - statementInfo->start_time >= statementInfo->slow_query_threshold &&
        statementInfo->slow_query_threshold >= 0) ? true : false);
    if (isSlow != NULL) {
        *isSlow = values[i - 1];
    }

    SET_TEXT_VALUES(statementInfo->trace_id, i++);
    set_stmt_advise(statementInfo, values, nulls, &i);
    /* time info addition */
    values[i++] = Int64GetDatum(statementInfo->timeModel[NET_SEND_TIME]);
    for (int num = TOTAL_TIME_INFO_TYPES_P1; num < TOTAL_TIME_INFO_TYPES; num++) {
        values[i++] = Int64GetDatum(statementInfo->timeModel[num]);
    }
    values[i++] = Int64GetDatum(statementInfo->parent_query_id);
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

/* parse value of GUC track_stmt_retention_time and track_stmt_standby_chain_size */
static List* SplitTrackStatementGUCParam(const char* newval)
{
    /* Do str copy and remove space. */
    char* strs = TrimStr(newval);
    if (IS_NULL_STR(strs))
        return NIL;
    const char* delim = ",";
    List *res = NULL;
    char* next_token = NULL;

    char* token = strtok_s(strs, delim, &next_token);
    while (token != NULL) {
        res = lappend(res, TrimStr(token));
        token = strtok_s(NULL, delim, &next_token);
    }
    pfree(strs);
    return res;
}

bool check_statement_retention_time(char** newval, void** extra, GucSource source)
{
    List* res = SplitTrackStatementGUCParam(*newval);
    if (res == NIL) {
        return false;
    }

    if (res->length != STATEMENT_SQL_KIND) {
        GUC_check_errdetail("attr num:%d is error,track_stmt_retention_time attr is 2", res->length);
        list_free_deep(res);
        return false;
    }

    int full_sql_retention_sec = 0;
    int slow_query_retention_days = 0;
    /* get full sql retention sec */
    if (!StrToInt32((char*)linitial(res), &full_sql_retention_sec) ||
        !StrToInt32((char*)lsecond(res), &slow_query_retention_days)) {
        GUC_check_errdetail("invalid input syntax");
        list_free_deep(res);
        return false;
    }

    if (slow_query_retention_days < 0 || slow_query_retention_days > MAX_SLOW_QUERY_RETENSION_DAYS) {
        GUC_check_errdetail("slow_query_retention_days:%d is out of range [%d, %d].",
            slow_query_retention_days, 0, MAX_SLOW_QUERY_RETENSION_DAYS);
        list_free_deep(res);
        return false;
    }
    if (full_sql_retention_sec < 0 || full_sql_retention_sec > MAX_FULL_SQL_RETENSION_SEC) {
        GUC_check_errdetail("full_sql_retention_sec:%d is out of range [%d, %d].",
            full_sql_retention_sec, 0, MAX_FULL_SQL_RETENSION_SEC);
        list_free_deep(res);
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

bool check_standby_statement_chain_size(char** newval, void** extra, GucSource source)
{
#define MFUNITS 16  // 16M each mfchain block
#define STANDBY_STMTHIST_NATTR 4
    static const char* attr_name[] = {
        "fast sql memory size", "fast sql disk size",
        "slow sql memory size", "slow sql disk size"};
    static const int attr_min[] = {
        MIN_MBLOCK_NUM * MFUNITS, MIN_FBLOCK_NUM * MFUNITS,
        MIN_MBLOCK_NUM * MFUNITS, MIN_FBLOCK_NUM * MFUNITS};
    static const int attr_max[] = {
        MAX_MBLOCK_NUM * MFUNITS, MAX_FBLOCK_NUM * MFUNITS,
        MAX_MBLOCK_NUM * MFUNITS, MAX_FBLOCK_NUM * MFUNITS};

    List* res = SplitTrackStatementGUCParam(*newval);
    if (res == NIL) {
        return false;
    } else if (res->length != STANDBY_STMTHIST_NATTR) {
        GUC_check_errdetail("attr num:%d is error, track_stmt_standby_chain_size attr is 4", res->length);
        return false;
    }

    int attr[STANDBY_STMTHIST_NATTR] = {0};
    ListCell* lc = NULL;
    int i = 0;
    foreach(lc, res) {
        if (!StrToInt32((char*)lfirst(lc), &attr[i])) {
            GUC_check_errdetail("invalid input syntax");
            return false;
        }
        if (attr[i] < attr_min[i] || attr[i] > attr_max[i]) {
            GUC_check_errdetail("%s:%d(MB) is out of range [%d, %d].",
            attr_name[i], attr[i], attr_min[i], attr_max[i]);
            return false;
        }

        i++;
    }

    if (attr[0] > attr[1] || attr[2] > attr[3]) {   // see attr_name
        GUC_check_errdetail("memory size can't not bigger than file size.");
        return false;
    }
    list_free_deep(res);

    *extra = MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_DFX), STANDBY_STMTHIST_NATTR * sizeof(int));
    ((int*)(*extra))[0] = attr[0] / MFUNITS;  // see attr_name
    ((int*)(*extra))[1] = attr[1] / MFUNITS;  // see attr_name
    ((int*)(*extra))[2] = attr[2] / MFUNITS;  // see attr_name
    ((int*)(*extra))[3] = attr[3] / MFUNITS;  // see attr_name
    return true;
}

void assign_standby_statement_chain_size(const char* newval, void* extra)
{
    int *size = (int*) extra;
    t_thrd.statement_cxt.fast_max_mblock = size[0];  // see attr_name in check function
    t_thrd.statement_cxt.fast_max_block  = size[1];  // see attr_name in check function
    t_thrd.statement_cxt.slow_max_mblock = size[2];  // see attr_name in check function
    t_thrd.statement_cxt.slow_max_block  = size[3];  // see attr_name in check function
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
    if (*count < maxCleanInterval) {
        return;
    }

    /* We do clean work by starting clean worker in primary mode, do it ourself in standby mode. */
    if (pmState == PM_RUN) { 
        if (g_instance.stat_cxt.instr_stmt_is_cleaning) {
            return;
        }
        SendPostmasterSignal(PMSIGNAL_START_CLEAN_STATEMENT);
        g_instance.stat_cxt.instr_stmt_is_cleaning = true;
    }

    /*
     * statement flush will not restart during switch over from standby to primary,
     * so clean stbyStmtHistory is necessary.
     */
    CleanStbyStmtHistory();

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

/* flush statement list info to statement_history table or mem-file chain */
static void FlushStatementToTableOrMFChain(StatementStatContext* suspendList, const knl_u_statement_context* statementCxt)
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
        bool isSlow = false;
        MemFileChain* target = NULL;
        while (flushItem != NULL) {
            tuple = GetStatementTuple(rel, flushItem, statementCxt, &isSlow);
            if (pmState == PM_HOT_STANDBY) {
                /*
                 * mefchain-insert action does not means this is a write transaction, it must be a read only trans,
                 * also it's result not controled by transaction, but we still set it in, to release some lock and mem
                 */
                target = isSlow ? g_instance.stat_cxt.stbyStmtHistSlow : g_instance.stat_cxt.stbyStmtHistFast;
                (void)MemFileChainInsert(target, tuple, rel);
            } else {
                /*
                 * in primary node. it is a common write transaction.
                 */
                (void)simple_heap_insert(rel, tuple);
                CatalogUpdateIndexes(rel, tuple);
            }
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
            /* flush statement list info to statement_history table or mem-file chain */
            FlushStatementToTableOrMFChain(suspendList, statementCxt);

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
    int count = 0;
    bool is_readonly_log_needed = false;

    while (!t_thrd.statement_cxt.need_exit && ENABLE_STATEMENT_TRACK) {
        ReloadInfo();
        if (u_sess->attr.attr_storage.DefaultXactReadOnly) {
            if (!is_readonly_log_needed) {
                is_readonly_log_needed = true;
                ereport(WARNING, (errmodule(MOD_INSTR),
                    errmsg("[Statement] cannot flush suspend list to statement_history in a read-only transaction")));
            }
            pg_usleep(FLUSH_USLEEP_INTERVAL);
            continue;
        }
        if (is_readonly_log_needed) {
            is_readonly_log_needed = false;
        }

        ereport(DEBUG4, (errmodule(MOD_INSTR), errmsg("[Statement] start to flush statements.")));
        StartCleanWorker(&count);

        HOLD_INTERRUPTS();
        /* flush all session's statement info to statement_history table or mem-file chain */
        FlushAllStatement();
        RESUME_INTERRUPTS();

        count++;
        ereport(DEBUG4, (errmodule(MOD_INSTR), errmsg("[Statement] flush statements finished.")));
        /* report statement_history state to pgstat */
        if (OidIsValid(u_sess->proc_cxt.MyDatabaseId))
            pgstat_report_stat(true);
        pg_usleep(FLUSH_USLEEP_INTERVAL);
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

    StartStbyStmtHistory();

    /* flush statement into statement_history table */
    StatementFlush();
    ShutdownStbyStmtHistory();
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
    (void)gspqsignal(SIGURG, print_stack);

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
        ssctx->details.head->buf[0] = (char)STATEMENT_DETAIL_LOCK_NOT_TRUNCATED;
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
        ssctx->details.head->buf[1] = (char)STATEMENT_DETAIL_LOCK_TRUNCATED;
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

static void copy_detail_area_type(AreaInfo *area_info, uint8 type)
{
    area_info->detail[area_info->cur_offset] = type;
    area_info->cur_offset += 1;
}

static void copy_detail_area_data_len(AreaInfo *area_info)
{
    uint32 data_len = area_info->data_len;
    errno_t rc = memcpy_s(area_info->detail + area_info->cur_offset,
        area_info->max_detail_len - area_info->cur_offset, &data_len,
        STATEMENT_DETAIL_DATA_LEN);
    securec_check(rc, "", "");
    area_info->cur_offset += STATEMENT_DETAIL_DATA_LEN;
}

static void copy_detail_area_data(AreaInfo *area_info, StringInfo wait_events_info)
{
    errno_t rc = 0;
    rc = memcpy_s(area_info->detail + area_info->cur_offset,
        area_info->max_detail_len - area_info->cur_offset,
        wait_events_info->data, wait_events_info->len);
    securec_check(rc, "", "");
    area_info->cur_offset += wait_events_info->len;
}

static bool should_ignore_append_area_events(StringInfo wait_events_info)
{
    return wait_events_info == NULL || wait_events_info->len == 0;
}

static uint32 get_wait_events_len_in_detail(StringInfo wait_events_info)
{
    return should_ignore_append_area_events(wait_events_info) ? 0 :
        STATEMENT_DETAIL_TYPE_LEN + STATEMENT_DETAIL_DATA_LEN + wait_events_info->len;
}

static void append_wait_events_into_detail(AreaInfo *area_info, StringInfo wait_events_info)
{
    Assert(area_info);
    if (should_ignore_append_area_events(wait_events_info)) {
        return;
    }

    copy_detail_area_type(area_info, STATEMENT_DETAIL_TYPE_EVENT);
    copy_detail_area_data_len(area_info);

    ereport(DEBUG2, (errmodule(MOD_INSTR), errmsg("[Statement] append wait events, binary len: %d",
        wait_events_info->len)));
    copy_detail_area_data(area_info, wait_events_info);
}

static bool should_ignore_append_area_lock(StatementDetail *detail)
{
    return detail == NULL || detail->n_items == 0;
}

static uint32 get_lock_len_in_detail(StatementDetail *detail, bool oom)
{
    if (should_ignore_append_area_lock(detail)) {
        return 0;
    }
    uint32 size = STATEMENT_DETAIL_TYPE_LEN + STATEMENT_DETAIL_DATA_LEN;

    if (oom) {
        size += STATEMENT_DETAIL_LOCK_STATUS_LEN;
    } else {
        size += (uint32)((detail->n_items - 1) * STATEMENT_DETAIL_BUFSIZE) + detail->cur_pos;
    }
    return size;
}

static void append_lock_into_detail(AreaInfo *area_info, StatementDetail *stmt_detail, bool oom)
{
    Assert(area_info);
    if (should_ignore_append_area_lock(stmt_detail)) {
        return;
    }
    errno_t rc;

    copy_detail_area_type(area_info, STATEMENT_DETAIL_TYPE_LOCK);
    copy_detail_area_data_len(area_info);

    if (oom) {
        area_info->detail[area_info->cur_offset] = (char)STATEMENT_DETAIL_LOCK_MISSING_OOM;
        area_info->cur_offset += 1;
    } else {
        size_t itemSize;
        for (StatementDetailItem *item = stmt_detail->head; item != NULL; item = (StatementDetailItem *)(item->next)) {
            itemSize = (item == stmt_detail->tail) ? stmt_detail->cur_pos : STATEMENT_DETAIL_BUFSIZE;

            rc = memcpy_s(area_info->detail + area_info->cur_offset,
                (area_info->max_detail_len - area_info->cur_offset), item->buf, itemSize);
            securec_check(rc, "\0", "\0");
            area_info->cur_offset += itemSize;
        }
    }
}

/* append total length and version number */
static void append_basic_into_details(AreaInfo *area_info, uint32 total_size)
{
    int rc = memcpy_s(area_info->detail + area_info->cur_offset, area_info->max_detail_len - area_info->cur_offset,
        &total_size, sizeof(uint32));
    securec_check(rc, "\0", "\0");
    area_info->cur_offset += sizeof(uint32);

    area_info->detail[area_info->cur_offset] = STATEMENT_DETAIL_VERSION;
    area_info->cur_offset += 1;
}

/*
 * bytea data format:
 *   Record format: TOTAL_LEN + VERSION + AREA_1 + AREA_2 + ... + AREA_N
 *   Area format:   TYPE + DATA_LEN + DATA
 */
static bytea* get_statement_detail(StatementDetail *detail, StringInfo wait_events_info, bool oom)
{
    /* caculate bytea total len */
    uint32 size = 0;
    text *result = NULL;

    size += sizeof(uint32);
    size += STATEMENT_DETAILS_HEAD_SIZE;

    uint32 wait_event_data_len = get_wait_events_len_in_detail(wait_events_info);
    size += wait_event_data_len;

    uint32 lock_data_len = get_lock_len_in_detail(detail, oom);
    size += lock_data_len;

    /* apply memory */
    result = (text*)palloc(size + VARHDRSZ);
    SET_VARSIZE(result, size + VARHDRSZ);

    char *detail_data = VARDATA(result);

    /* ----append each areas into detail record---- */
    AreaInfo area_info = {0};
    area_info.detail = detail_data;
    area_info.max_detail_len = size;

    append_basic_into_details(&area_info, size);

    /* wait event area */
    area_info.data_len = calc_area_data_len(wait_event_data_len);
    append_wait_events_into_detail(&area_info, wait_events_info);
    ereport(DEBUG3,
        (errmodule(MOD_INSTR), errmsg("[Statement] flush - wait event binary len: %u", wait_event_data_len)));

    /* lock area */
    area_info.data_len = calc_area_data_len(lock_data_len);
    append_lock_into_detail(&area_info, detail, oom);
    ereport(DEBUG3, (errmodule(MOD_INSTR), errmsg("[Statement] flush - lock binary len: %u", lock_data_len)));
    return result;
}

static void handle_detail_flag(char flag, StringInfo result)
{
    if (flag == STATEMENT_DETAIL_LOCK_TRUNCATED) {
        appendStringInfoString(result, "Truncated...");
    } else if (flag == STATEMENT_DETAIL_LOCK_MISSING_OOM) {
        appendStringInfoString(result, "Missing(OOM)...");
    }
}

static void check_stmt_detail_offset(uint32 total_len, uint32 offset, bool* is_valid_record)
{
    if (offset > total_len) {
        *is_valid_record = false;
    }
}

static const char *get_stmt_event_type_str(uint8 event_type)
{
    switch (event_type) {
        case STATEMENT_EVENT_TYPE_IO:
            return "IO_EVENT";
        case STATEMENT_EVENT_TYPE_DMS:
            return "DMS_EVENT";
        case STATEMENT_EVENT_TYPE_LOCK:
            return "LOCK_EVENT";
        case STATEMENT_EVENT_TYPE_LWLOCK:
            return "LWLOCK_EVENT";
        case STATEMENT_EVENT_TYPE_STATUS:
            return "STATUS";
        default:
            return "UNKNOWN_TYPE";
    }
}


static int stmt_compare_wait_events(const void *a, const void *b)
{
    if (a == NULL || b == NULL) {
        return 0;
    }return (((StmtEventRecord*)a)->duration > ((StmtEventRecord*)b)->duration ? -1 : 1);
}

/*
 * Binary format as:
 * uint32            +         2 bytes  + string     + 8         + EVENT
 * wait_events_count + event_1(name_len + event_name + duration) + event_2
 */
void decode_stmt_wait_events(StringInfo resultBuf, const char *details,
    uint32 total_len, bool *is_valid_record, bool pretty)
{
    if (total_len == 0) {
        *is_valid_record = false;
        return;
    }
    appendStringInfoString(resultBuf, "\t---------------Wait Events Area---------------\n");

    uint32 offset = 0;
    uint32 events_total_count = 0, events_current_count = 0;
    int rc = 0;

    check_stmt_detail_offset(total_len, offset + sizeof(uint32), is_valid_record);
    rc = memcpy_s(&events_total_count, sizeof(uint32), details + offset, sizeof(uint32));
    securec_check(rc, "\0", "\0");
    offset += sizeof(uint32);

    if (events_total_count == 0) {
        *is_valid_record = false;
        return;
    }
    StmtEventRecord *records = (StmtEventRecord*)palloc0(sizeof(StmtEventRecord) * events_total_count);
    uint16 event_name_len = 0;
    uint64 event_duration = 0;
    uint8  event_type = STATEMENT_EVENT_TYPE_UNKNOWN;
    while (offset < total_len && *is_valid_record) {
        check_stmt_detail_offset(total_len, offset + sizeof(uint8), is_valid_record);
        rc = memcpy_s(&event_type, sizeof(uint8), details + offset, sizeof(uint8));
        securec_check(rc, "\0", "\0");
        offset += sizeof(uint8);

        check_stmt_detail_offset(total_len, offset + sizeof(uint16), is_valid_record);
        rc = memcpy_s(&event_name_len, sizeof(uint16), details + offset, sizeof(uint16));
        securec_check(rc, "\0", "\0");
        offset += sizeof(uint16);

        check_stmt_detail_offset(total_len, offset + event_name_len, is_valid_record);
        records[events_current_count].event_name = details + offset;
        offset += event_name_len;

        check_stmt_detail_offset(total_len, offset + sizeof(uint64), is_valid_record);
        rc = memcpy_s(&event_duration, sizeof(uint64), details + offset, sizeof(uint64));
        securec_check(rc, "\0", "\0");
        offset += sizeof(uint64);

        records[events_current_count].event_type = get_stmt_event_type_str(event_type);
        records[events_current_count].duration = event_duration;

        events_current_count++;
        if (events_current_count > events_total_count) {
            *is_valid_record = false;
            pfree(records);
            return;
        }
    }

    qsort(records, events_total_count, sizeof(StmtEventRecord), &stmt_compare_wait_events);
    for (uint32 i = 0; i < events_total_count; i++) {
        appendStringInfo(resultBuf, "'%u'\t", i + 1);
        appendStringInfo(resultBuf, "%-13s\t", records[i].event_type);
        appendStringInfo(resultBuf, "%-45s", records[i].event_name);
        appendStringInfoChar(resultBuf, '\t');
        if (pretty) {
            appendStringInfo(resultBuf, "%10lu (us)\n", records[i].duration);
        } else {
            appendStringInfo(resultBuf, "%10lu (us)", records[i].duration);
        }
    }
    pfree(records);
}

void decode_statement_detail(StringInfo resultBuf, const char *details,
    uint32 total_len, bool *is_valid_record, bool pretty)
{
    if (total_len == 0) {
        *is_valid_record = false;
        return;
    }

    /* detail get area type */
    uint32 offset = 0;
    uint32 data_len = 0;
    int rc = 0;

    while (offset < total_len && is_valid_record) {
        char area_type = details[offset];
        check_stmt_detail_offset(total_len, offset + 1, is_valid_record);
        offset += 1;

        check_stmt_detail_offset(total_len, offset + sizeof(uint32), is_valid_record);
        rc = memcpy_s(&data_len, sizeof(uint32), (void*)(details + offset), sizeof(uint32));
        securec_check(rc, "\0", "\0");
        offset += sizeof(uint32);

        if (data_len > (total_len - STATEMENT_DETAIL_TYPE_LEN - STATEMENT_DETAIL_DATA_LEN)) {
            *is_valid_record = false;
            return;
        }

        switch (area_type) {
            case STATEMENT_DETAIL_TYPE_EVENT:
                decode_stmt_wait_events(resultBuf, details + offset, data_len, is_valid_record, pretty);
                break;
            case STATEMENT_DETAIL_TYPE_LOCK:
                decode_stmt_locks(resultBuf, details + offset, data_len, is_valid_record, pretty);
                break;
            default:
                *is_valid_record = false;
                return;
        }
        offset += data_len;
    }
}

static void decode_stmt_locks(StringInfo resultBuf, const char *area_data, uint32 detailsLen, bool *is_valid_record,
    bool pretty)
{
    /* record flag area, should be
     *   STATEMENT_DETAIL_LOCK_NOT_TRUNCATED/STATEMENT_DETAIL_LOCK_TRUNCATED/STATEMENT_DETAIL_LOCK_MISSING_OOM
     */
    uint32 offset = 0;
    char flag = area_data[0];
    if (flag != STATEMENT_DETAIL_LOCK_NOT_TRUNCATED &&
        flag != STATEMENT_DETAIL_LOCK_TRUNCATED &&
        flag != STATEMENT_DETAIL_LOCK_MISSING_OOM) {
        *is_valid_record = false;
        return;
    }
    offset += 1;

    appendStringInfo(resultBuf, "\t---------------LOCK/LWLOCK Area---------------\n");
    size_t detailsCnt = 1;
    while (offset < detailsLen && *is_valid_record) {
        StmtDetailType itemType = (StmtDetailType)area_data[offset];
        size_t size = get_statement_detail_size(itemType);
        if (size == INVALID_DETAIL_BUFSIZE) {
            *is_valid_record = false;
            return;
        }
        if (offset + size > detailsLen) {
            /* invalid buf */
            *is_valid_record = false;
            return;
        }
        void *info = palloc_statement_detail(itemType);
        if (info == NULL) {
            return;
        }
        int rc = memcpy_s(info, size, area_data + offset, size);
        securec_check(rc, "", "");
        offset  += size;

        /* write to string */
        *is_valid_record = write_statement_detail_text(info, itemType, resultBuf, &detailsCnt, pretty);
        pfree(info);
    }

    handle_detail_flag(flag, resultBuf);
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

    if (!is_valid_detail_record(bytea_data_len, details, &detailsLen)) {
        return pstrdup("invalid_detail_header_format");
    }

    bool is_valid_record = true;
    uint32 offset = sizeof(uint32);
    StringInfoData resultBuf;
    initStringInfo(&resultBuf);

    char detail_version = details[sizeof(uint32)];
    offset += 1;
    if (detail_version == STATEMENT_DETAIL_VERSION_v1) {
        decode_stmt_locks(&resultBuf, details + offset, detailsLen - offset, &is_valid_record, pretty);
    } else {
        decode_statement_detail(&resultBuf, details + offset, detailsLen - offset, &is_valid_record, pretty);
    }

    if (!is_valid_record) {
        pfree(resultBuf.data);
        return pstrdup("invalid_detail_data_format");
    }

    /* after is_valid_detail_record called, resultBuf.len never equal to 0 */
    resultBuf.data[resultBuf.len - 1] = ' ';
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
    CURRENT_STMT_METRIC_HANDLE->parent_query_id = u_sess->unique_sql_cxt.parent_unique_sql_id;

    if (likely(!is_local_unique_sql() ||
        (!u_sess->attr.attr_common.track_stmt_parameter && CURRENT_STMT_METRIC_HANDLE->query
                && !u_sess->unique_sql_cxt.need_record_in_dynexecplsql) ||
        (u_sess->attr.attr_common.track_stmt_parameter &&
         (u_sess->pbe_message == PARSE_MESSAGE_QUERY || u_sess->pbe_message == BIND_MESSAGE_QUERY)))) {
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

    /* unit: microseconds */
    CURRENT_STMT_METRIC_HANDLE->slow_query_threshold =
        (int64)u_sess->attr.attr_storage.log_min_duration_statement * 1000;

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
        ssctx->row_activity.tuples_fetched += (uint64)agg_table_stat->t_tuples_fetched;
        ssctx->row_activity.tuples_returned += (uint64)agg_table_stat->t_tuples_returned;

        ssctx->row_activity.tuples_inserted += (uint64)agg_table_stat->t_tuples_inserted;
        ssctx->row_activity.tuples_updated += (uint64)agg_table_stat->t_tuples_updated;
        ssctx->row_activity.tuples_deleted += (uint64)agg_table_stat->t_tuples_deleted;

        // cache_io
        ssctx->cache_io.blocks_fetched += (uint64)agg_table_stat->t_blocks_fetched;
        ssctx->cache_io.blocks_hit += (uint64)agg_table_stat->t_blocks_hit;
    }

    if (timeInfo != NULL) {
        for (int idx = 0; idx < TOTAL_TIME_INFO_TYPES; idx++) {
            ssctx->timeModel[idx] += timeInfo[idx];
        }
    }

    if (netInfo != NULL) {
        for (int i = 0; i < TOTAL_NET_INFO_TYPES; i++) {
            ssctx->networkInfo[i] += netInfo[i];
        }
    }
}

static inline bool instr_stmt_level_fullsql_open()
{
    int fullsql_level = u_sess->statement_cxt.statement_level[0];
    /* only record query plan when level >= L1 */
    return fullsql_level >= STMT_TRACK_L1 && fullsql_level <= STMT_TRACK_L2;
}

static inline bool instr_stmt_level_slowsql_only_open()
{
    if (CURRENT_STMT_METRIC_HANDLE == NULL || CURRENT_STMT_METRIC_HANDLE->slow_query_threshold < 0) {
        return false;
    }

    int slowsql_level = u_sess->statement_cxt.statement_level[1];
    /* only record query plan when level >= L1 */
    return slowsql_level >= STMT_TRACK_L1 && slowsql_level <= STMT_TRACK_L2;
}

static inline bool instr_stmt_is_slowsql()
{
    if (CURRENT_STMT_METRIC_HANDLE->slow_query_threshold == 0) {
        return true;
    }
    StatementStatContext *ssctx = (StatementStatContext *)u_sess->statement_cxt.curStatementMetrics;
    if (ssctx->query_plan != NULL) {
        return false;
    }
    TimestampTz cur_time = GetCurrentTimestamp();
    int64 start_time = (int64)ssctx->start_time;
    int64 elapse_time = (int64)(cur_time - start_time);

    return CURRENT_STMT_METRIC_HANDLE->slow_query_threshold <= elapse_time;
}

bool instr_stmt_need_track_plan()
{
    if (CURRENT_STMT_METRIC_HANDLE == NULL) {
        return false;
    }

    if (instr_stmt_level_fullsql_open() || (instr_stmt_level_slowsql_only_open() && instr_stmt_is_slowsql())) {
        return true;
    }

    return false;
}

void instr_stmt_report_query_plan(QueryDesc *queryDesc)
{
    StatementStatContext *ssctx = (StatementStatContext *)u_sess->statement_cxt.curStatementMetrics;
    if (queryDesc == NULL || ssctx == NULL || ssctx->level <= STMT_TRACK_L0
        || ssctx->level > STMT_TRACK_L2 || (ssctx->plan_size != 0 && !u_sess->unique_sql_cxt.is_open_cursor)
        || (u_sess->statement_cxt.executer_run_level > 1 && !IS_UNIQUE_SQL_TRACK_ALL)) {
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
static bool is_valid_detail_record(uint32 bytea_data_len, const char *details, uint32 *details_len)
{
    /* VERSIZE(bytea) - VARHDRSZ should be > sizeof(uint32) + STATEMENT_DETAILS_HEAD_SIZE */
    if (bytea_data_len <= (sizeof(uint32) + STATEMENT_DETAILS_HEAD_SIZE)) {
        return false;
    }

    int rc = memcpy_s(details_len, sizeof(uint32), details, sizeof(uint32));
    securec_check(rc, "\0", "\0");

    /* details binary length should be same with VERSIZE(bytea) - VARHDRSZ */
    if (bytea_data_len != *details_len) {
        return false;
    }

    /* record version, should be [1, STATEMENT_DETAIL_VERSION] */
    int32 version = (int32)details[sizeof(uint32)];
    if (version == 0 || version > STATEMENT_DETAIL_VERSION) {
        return false;
    }
    return true;
}

static void instr_stmt_set_wait_events_in_session_bms(int32 bms_event_idx)
{
    if (u_sess->statement_cxt.stmt_stat_cxt == NULL || !u_sess->statement_cxt.is_session_bms_active ||
        bms_event_idx == -1)
        return;

    mark_session_bms(bms_event_idx, 0, false);
}

static void instr_stmt_set_wait_events_in_handle_bms(int32 bms_event_idx)
{
    CHECK_STMT_HANDLE();
    if (!u_sess->statement_cxt.enable_wait_events_bitmap) {
        return;
    }

    if (bms_event_idx >= 0) {
        MemoryContext oldcontext = MemoryContextSwitchTo(u_sess->statement_cxt.stmt_stat_cxt);
        PG_TRY();
        {
            ereport(DEBUG4, (errmodule(MOD_INSTR), errmsg("[Statement] mark delta BMS in handle - ID: %d",
                bms_event_idx)));
            CURRENT_STMT_METRIC_HANDLE->wait_events_bitmap =
                bms_add_member(CURRENT_STMT_METRIC_HANDLE->wait_events_bitmap, bms_event_idx);


        }
        PG_CATCH();
        {
            (void)MemoryContextSwitchTo(oldcontext);
            FlushErrorState();
            ereport(LOG, (errmodule(MOD_INSTR),
                errmsg("[Statement] bms handle event failed - bms event idx: %d, msg: OOM", bms_event_idx)));
        }
        PG_END_TRY();
        (void)MemoryContextSwitchTo(oldcontext);
    }
}

static int32 get_wait_events_idx_in_bms(uint32 class_id, uint32 event_id)
{
    int event_idx = -1;
    switch (class_id) {
        case PG_WAIT_IO:
            ereport(DEBUG4, (errmodule(MOD_INSTR), errmsg("[Statement] tracked event - IO")));
            event_idx = event_id;
            break;
        case PG_WAIT_DMS:
            ereport(DEBUG4, (errmodule(MOD_INSTR), errmsg("[Statement] tracked event - DMS")));
            event_idx = event_id;
            break;
        case PG_WAIT_LOCK:
            ereport(DEBUG4, (errmodule(MOD_INSTR), errmsg("[Statement] tracked event - LOCK")));
            event_idx = event_id + wait_event_io_event_max_index;
            break;
        case PG_WAIT_LWLOCK:
            ereport(DEBUG4, (errmodule(MOD_INSTR), errmsg("[Statement] tracked event - LWLOCK")));
            event_idx = event_id + wait_event_lock_event_max_index;
            break;
        case PG_WAIT_STATE:
            ereport(DEBUG4, (errmodule(MOD_INSTR), errmsg("[Statement] tracked event - STATE")));
            event_idx = event_id + wait_event_lwlock_event_max_index;
            break;
        default:
            break;
    }
    ereport(DEBUG4,
        (errmodule(MOD_INSTR),
        errmsg("[Statement] tracked BMS event - index: %d, real index: %u", event_idx, event_id)));

    return event_idx;
}

void instr_stmt_set_wait_events_bitmap(uint32 class_id, uint32 event_id)
{
    if (u_sess->statement_cxt.stmt_stat_cxt == NULL)
        return;

    int32 bms_event_idx = -1;
    bms_event_idx = get_wait_events_idx_in_bms(class_id, event_id);
    if (bms_event_idx == -1) {
        return;
    }
    /* 1, after session BMS inited, we always mark events in BMS */
    if (u_sess->statement_cxt.is_session_bms_active) {
        instr_stmt_set_wait_events_in_session_bms(bms_event_idx);
    }

    /* 2, for statement in handle, mark delta events in handle BMS */
    instr_stmt_set_wait_events_in_handle_bms(bms_event_idx);
}

static void get_wait_events_full_info(StatementStatContext *statement_stat, StringInfo wait_events_info)
{
    if (statement_stat == NULL || statement_stat->wait_events == NULL) {
        return;
    }
    uint32 wait_events_count = bms_num_members(statement_stat->wait_events_bitmap);
    if (wait_events_count == 0) {
        return;
    }

    const char *event_str = NULL;
    uint16 event_str_len = 0;
    uint8 event_type = 0;
    int event_idx = -1;
    int32 virt_event_idx = -1;

    /*
     * Binary format as:
     * uint32            +         1 bytes + 2 bytes  + string     + 8         + EVENT
     * wait_events_count + event_1(type    + name_len + event_name + duration) + event_2
     */
    ereport(DEBUG2, (errmodule(MOD_INSTR), errmsg("[Statement] flush wait events - count: %u", wait_events_count)));
    appendBinaryStringInfo(wait_events_info, (const char *)(&wait_events_count), sizeof(uint32));
    uint32 i = 0;
    while ((virt_event_idx = bms_next_member(statement_stat->wait_events_bitmap, virt_event_idx)) >= 0) {
        if (virt_event_idx < wait_event_io_event_max_index) {
            event_idx = virt_event_idx;
            event_str = pgstat_get_wait_io(WaitEventIO(event_idx + PG_WAIT_IO));
            event_type = STATEMENT_EVENT_TYPE_IO;
        } else if (virt_event_idx < wait_event_dms_event_max_index) {
            event_idx = virt_event_idx - wait_event_io_event_max_index;
            event_str = pgstat_get_wait_dms(WaitEventDMS(event_idx + PG_WAIT_DMS));
            event_type = STATEMENT_EVENT_TYPE_DMS;
        } else if (virt_event_idx < wait_event_lock_event_max_index) {
            event_idx = virt_event_idx - wait_event_dms_event_max_index;
            event_str = GetLockNameFromTagType(event_idx);
            event_type = STATEMENT_EVENT_TYPE_LOCK;
        } else if (virt_event_idx < wait_event_lwlock_event_max_index) {
            event_idx = virt_event_idx - wait_event_lock_event_max_index;
            event_str = GetLWLockIdentifier(PG_WAIT_LWLOCK, event_idx);
            event_type = STATEMENT_EVENT_TYPE_LWLOCK;
        } else if (virt_event_idx < wait_event_state_wait_max_index) {
            event_idx = virt_event_idx - wait_event_lwlock_event_max_index;
            event_str = pgstat_get_waitstatusname(event_idx);
            event_type = STATEMENT_EVENT_TYPE_STATUS;
        } else {
            event_str = "UNKNOW_EVENT";
            event_type = STATEMENT_EVENT_TYPE_UNKNOWN;
        }
        ereport(DEBUG4,
            (errmodule(MOD_INSTR),
            errmsg("[Statement] flushing event - (%s - %s) virtual index: %d real index: %d",
            get_stmt_event_type_str(event_type), event_str, virt_event_idx, event_idx)));

        /* with '\0' terminated */
        event_str_len = strlen(event_str) + 1;
        appendBinaryStringInfo(wait_events_info, (const char *)(&event_type), sizeof(uint8));
        appendBinaryStringInfo(wait_events_info, (const char *)(&event_str_len), sizeof(uint16));
        appendBinaryStringInfo(wait_events_info, event_str, event_str_len);
        appendBinaryStringInfo(wait_events_info, (const char *)(&statement_stat->wait_events[i].total_duration),
            sizeof(uint64));
        i++;
    }
}

static void mark_session_bms(uint32 index, uint64 total_duration, bool is_init)
{
    if (is_init && (total_duration == 0)) {
        return;
    }

    MemoryContext oldcontext = MemoryContextSwitchTo(u_sess->statement_cxt.stmt_stat_cxt);
    PG_TRY();
    {
        u_sess->statement_cxt.wait_events_bms = bms_add_member(u_sess->statement_cxt.wait_events_bms, index);
    }
    PG_CATCH();
    {
        (void)MemoryContextSwitchTo(oldcontext);
        ErrorData* edata = NULL;
        edata = CopyErrorData();
        FlushErrorState();
        ereport(LOG, (errmodule(MOD_INSTR),
            errmsg("[Statement] mark session bms failed - bms event idx: %d, msg: %s", index,
            edata->message)));
        FreeErrorData(edata);
    }
    PG_END_TRY();
    (void)MemoryContextSwitchTo(oldcontext);
    ereport(DEBUG4, (errmodule(MOD_INSTR),
        errmsg("[Statement] %s base BMS - ID: %u", is_init ? "init" : "mark", index)));
}

/* full sql - wait events: copy wait events from backend entry to user session by using BMS */
void instr_stmt_copy_wait_events()
{
    CHECK_STMT_HANDLE();
    if (t_thrd.shemem_ptr_cxt.MyBEEntry == NULL) {
        return;
    }

    /* 1, if the first time enter the function for the session */
    /*   1.1 init bitmap by using the wait events info in backend entry */
    /*   1.2 copy wait events from backend entry to statement handle */
    if (!u_sess->statement_cxt.is_session_bms_active) {
        /* maybe no events changed */
        u_sess->statement_cxt.is_session_bms_active = true;
        ereport(DEBUG4, (errmodule(MOD_INSTR), errmsg("[Statement] begin to init base BMS")));

        uint32 start_idx = 0, end_idx = wait_event_io_event_max_index;
        WaitStatisticsInfo *wait_event_info = t_thrd.shemem_ptr_cxt.MyBEEntry->waitInfo.event_info.io_info;
        for (; start_idx < end_idx; start_idx++) {
            u_sess->statement_cxt.wait_events[start_idx].total_duration = wait_event_info[start_idx].total_duration;
            mark_session_bms(start_idx, u_sess->statement_cxt.wait_events[start_idx].total_duration);
        }

        wait_event_info = t_thrd.shemem_ptr_cxt.MyBEEntry->waitInfo.event_info.dms_info;
        start_idx = wait_event_io_event_max_index;
        end_idx = wait_event_dms_event_max_index;
        for (; start_idx < end_idx; start_idx++) {
            u_sess->statement_cxt.wait_events[start_idx].total_duration =
                wait_event_info[start_idx - wait_event_io_event_max_index].total_duration;
            mark_session_bms(start_idx, u_sess->statement_cxt.wait_events[start_idx].total_duration);
        }

        wait_event_info = t_thrd.shemem_ptr_cxt.MyBEEntry->waitInfo.event_info.lock_info;
        start_idx = wait_event_dms_event_max_index;
        end_idx = wait_event_lock_event_max_index;
        for (; start_idx < end_idx; start_idx++) {
            u_sess->statement_cxt.wait_events[start_idx].total_duration =
                wait_event_info[start_idx - wait_event_dms_event_max_index].total_duration;
            mark_session_bms(start_idx, u_sess->statement_cxt.wait_events[start_idx].total_duration);
        }

        wait_event_info = t_thrd.shemem_ptr_cxt.MyBEEntry->waitInfo.event_info.lwlock_info;
        start_idx = wait_event_lock_event_max_index;
        end_idx = wait_event_lwlock_event_max_index;
        for (; start_idx < end_idx; start_idx++) {
            u_sess->statement_cxt.wait_events[start_idx].total_duration =
                wait_event_info[start_idx - wait_event_lock_event_max_index].total_duration;
            mark_session_bms(start_idx, u_sess->statement_cxt.wait_events[start_idx].total_duration);
        }

        wait_event_info = t_thrd.shemem_ptr_cxt.MyBEEntry->waitInfo.status_info.statistics_info;
        start_idx = wait_event_lwlock_event_max_index;
        end_idx = wait_event_state_wait_max_index + 1;
        for (; start_idx < end_idx; start_idx++) {
            u_sess->statement_cxt.wait_events[start_idx].total_duration =
                wait_event_info[start_idx - wait_event_lwlock_event_max_index].total_duration;
            mark_session_bms(start_idx, u_sess->statement_cxt.wait_events[start_idx].total_duration);
        }
    } else {
        /* 2 if not the first time */
        /*   2.1 copy wait events from backend entry by using session's BMS */
        int event_idx = -1, event_real_id = 0;
        uint8 event_type = 0;
        uint64 total_duration = 0;

        ereport(DEBUG4, (errmodule(MOD_INSTR), errmsg("[Statement] begin to copy base event by using BMS")));
        while ((event_idx = bms_next_member(u_sess->statement_cxt.wait_events_bms, event_idx)) >= 0) {
            if (!instr_stmt_get_event_data(event_idx, &event_type, &event_real_id, &total_duration))
                continue;
            ereport(DEBUG4, (errmodule(MOD_INSTR), errmsg("[Statement] copy base - (%s) event index: %d, real index:%d",
                get_stmt_event_type_str(event_type), event_idx, event_real_id)));
            u_sess->statement_cxt.wait_events[event_idx].total_duration = total_duration;
        }
    }
}

void instr_stmt_diff_wait_events()
{
    CHECK_STMT_HANDLE();
    if (!u_sess->statement_cxt.enable_wait_events_bitmap) {
        return;
    }
    /*
     * 1, get the bitmap count,
     * 2, iterate bitmap to get event id,
     * 3, get the event delta duration by using 'event in backend entry' - 'event in user session'
     */
    int count = bms_num_members(CURRENT_STMT_METRIC_HANDLE->wait_events_bitmap);
    if (count == 0) {
        return;
    }

    ereport(DEBUG2, (errmodule(MOD_INSTR), errmsg("[Statement] diff BMS - changed events count - %d", count)));
    MemoryContext oldcontext = MemoryContextSwitchTo(u_sess->statement_cxt.stmt_stat_cxt);
    WaitEventEntry *wait_events = (WaitEventEntry*)palloc0_noexcept(count * sizeof(WaitEventEntry));
    (void)MemoryContextSwitchTo(oldcontext);
    if (wait_events != NULL) {
        int event_idx = -1, handle_idx = 0;
        while ((event_idx = bms_next_member(CURRENT_STMT_METRIC_HANDLE->wait_events_bitmap, event_idx)) >= 0) {
            Assert(handle_idx < count);

            int event_real_id = 0;
            uint8 event_type = 0;
            uint64 total_duration = 0;

            if (!instr_stmt_get_event_data(event_idx, &event_type, &event_real_id, &total_duration))
                continue;
            ereport(DEBUG3, (errmodule(MOD_INSTR), errmsg("[Statement] diff BMS - (%s) event index: %d, real index: %d",
                get_stmt_event_type_str(event_type), event_idx, event_real_id)));
            wait_events[handle_idx].total_duration =
                total_duration - u_sess->statement_cxt.wait_events[event_idx].total_duration;
            handle_idx++;
        }

        CURRENT_STMT_METRIC_HANDLE->wait_events = wait_events;
    }
}

/* get event type/real id/duration */
static bool instr_stmt_get_event_data(int32 event_idx, uint8 *event_type, int32 *event_real_id, uint64 *total_duration)
{
    WaitInfo *wait_info = &(t_thrd.shemem_ptr_cxt.MyBEEntry->waitInfo);
    if (event_idx < wait_event_io_event_max_index) {
        *event_real_id = event_idx;
        *total_duration = wait_info->event_info.io_info[*event_real_id].total_duration;
        *event_type = STATEMENT_EVENT_TYPE_IO;
    } else if (event_idx < wait_event_dms_event_max_index) {
        *event_real_id = event_idx - wait_event_io_event_max_index;
        *total_duration = wait_info->event_info.lock_info[*event_real_id].total_duration;
        *event_type = STATEMENT_EVENT_TYPE_DMS;
    } else if (event_idx < wait_event_lock_event_max_index) {
        *event_real_id = event_idx - wait_event_dms_event_max_index;
        *total_duration = wait_info->event_info.lock_info[*event_real_id].total_duration;
        *event_type = STATEMENT_EVENT_TYPE_LOCK;
    } else if (event_idx < wait_event_lwlock_event_max_index) {
        *event_real_id = event_idx - wait_event_lock_event_max_index;
        *total_duration = wait_info->event_info.lwlock_info[*event_real_id].total_duration;
        *event_type = STATEMENT_EVENT_TYPE_LWLOCK;
    } else if (event_idx < wait_event_state_wait_max_index) {
        *event_real_id = event_idx - wait_event_lwlock_event_max_index;
        *total_duration = wait_info->status_info.statistics_info[*event_real_id].total_duration;
        *event_type = STATEMENT_EVENT_TYPE_STATUS;
    } else {
        *event_type = STATEMENT_EVENT_TYPE_UNKNOWN;
        return false;
    }
    return true;
}

/* allocate memory for full sql wait events */
void init_full_sql_wait_events()
{
    if (u_sess->statement_cxt.wait_events == NULL && u_sess->statement_cxt.stmt_stat_cxt != NULL) {
        MemoryContext oldcontext = MemoryContextSwitchTo(u_sess->statement_cxt.stmt_stat_cxt);
        u_sess->statement_cxt.wait_events = (WaitEventEntry*)palloc0_noexcept(sizeof(WaitEventEntry) *
            (wait_event_state_wait_max_index + 1));
        (void)MemoryContextSwitchTo(oldcontext);
    }
}

void instr_stmt_dynamic_change_level()
{
    CHECK_STMT_HANDLE();

    /* if find user defined statement level, dynamic change the statement track level */
    StatLevel specified_level = instr_track_stmt_find_level();
    if (specified_level == STMT_TRACK_OFF) {
        return;
    }

    /* at commit stage, for dynamic track utility, need a symbol to decide flush the handle or not */
    CURRENT_STMT_METRIC_HANDLE->level = (specified_level > CURRENT_STMT_METRIC_HANDLE->level) ?
        specified_level : CURRENT_STMT_METRIC_HANDLE->level;
    CURRENT_STMT_METRIC_HANDLE->dynamic_track_level = specified_level;
    ereport(DEBUG1, (errmodule(MOD_INSTR), errmsg("[Statement] change (%lu) track level to L%d",
        u_sess->unique_sql_cxt.unique_sql_id, CURRENT_STMT_METRIC_HANDLE->level - 1)));
}

void instr_stmt_report_cause_type(uint32 type)
{
    CHECK_STMT_HANDLE();
 
    if (type & NUM_F_TYPECASTING)
        CURRENT_STMT_METRIC_HANDLE->cause_type |= NUM_F_TYPECASTING;
    if (type & NUM_F_LIMIT)
        CURRENT_STMT_METRIC_HANDLE->cause_type |= NUM_F_LIMIT;
    if (type & NUM_F_LEAKPROOF)
        CURRENT_STMT_METRIC_HANDLE->cause_type |= NUM_F_LEAKPROOF;
}
 
bool instr_stmt_plan_need_report_cause_type()
{
    if (CURRENT_STMT_METRIC_HANDLE == NULL || CURRENT_STMT_METRIC_HANDLE->cause_type == 0)
        return false;
 
    return true;
}
 
uint32 instr_stmt_plan_get_cause_type()
{
    return CURRENT_STMT_METRIC_HANDLE->cause_type;
}


/* **********************************************************************************************
 * STANDBY STATEMENG HISTORY FUNCTIONS
 * **********************************************************************************************
 * 
 * manage two scanner of mfchain. first is for full, second for slow.
 * We simply merge the results of the two scanners, which does not result in a significant
 * performance loss, but improves readability.
 *
 * Two candidates means two items from each scanner, and we know the who is the winner. Every time 
 * the old winner will be replaced by a new candidate from it's scanner, then we check and get a new
 * winner, and output it.
 */
typedef struct SStmtHistScanner {
    TupleDesc desc;
    int winner;
    HeapTuple candidate[STATEMENT_SQL_KIND];
    MemFileChainScanner* scanner[STATEMENT_SQL_KIND];
    TimestampTz range_s;
    TimestampTz range_e;
    MemoryContext memcxt;
} SStmtHistScanner;

/*
 * Only a HeapTuple struct without real data, every time it will bind real data from 
 * a MemFileItem to become a real candidate.
 * Because MemFileItem only store HeapTupleData, but some interface must using HeapTuple,
 * so we create a HeapTuple struction, every time we get a MemFileItem, bind it's HeapTupleData
 * into this HeapTuple, so we can access it. It seems silly, but it's necessary.
 */
static HeapTuple create_fake_candidate()
{
    HeapTuple candidate = (HeapTuple)heaptup_alloc(HEAPTUPLESIZE);
    candidate->t_len = 0;
    ItemPointerSetInvalid(&(candidate->t_self));
    candidate->t_tableOid = InvalidOid;
    candidate->t_bucketId = InvalidBktId;
    HeapTupleSetZeroBase(candidate);
    candidate->t_xc_node_id = 0;
    candidate->t_data = NULL;
    return candidate;
}

static TimestampTz get_candidate_finish_time(HeapTuple candidate, TupleDesc desc)
{
    if (candidate == NULL) {
        return DT_NOBEGIN;
    }

    Datum finish_time;
    bool isnull = false;
    finish_time = fastgetattr(candidate, Anum_statement_history_finish_time, desc, &isnull);
    if (unlikely(isnull)) {
        return DT_NOBEGIN;
    }
    return DatumGetTimestampTz(finish_time);
}

static bool get_next_candidate(SStmtHistScanner* scanner, int idx)
{
    MemFileItem* item = NULL;
    TimestampTz finish_time = DT_NOBEGIN;
    bool find = false;

    while (!find) {
        item = MemFileChainScanGetNext(scanner->scanner[idx]);
        if (item == NULL) {
            return false;
        }

        scanner->candidate[idx]->t_len = GetMemFileItemDataLen(item);
        scanner->candidate[idx]->t_data = (HeapTupleHeader)item->data;
        finish_time = get_candidate_finish_time(scanner->candidate[idx], scanner->desc);

        if (finish_time > scanner->range_e) {
            /* try next */
        } else if (finish_time >= scanner->range_s) {
            find = true;
        } else {
            return false;
        }
    }
    return true;
}

static SStmtHistScanner* sstmthist_scanner_start(MemoryContext memcxt, bool only_slow, TupleDesc desc, TimestampTz time1, TimestampTz time2)
{
    if (time1 > time2) {
        ereport(ERROR, (errmsg("Invalid time param.")));
    }
    if (time1 > GetCurrentTimestamp()) {
        return NULL;
    }

    MemoryContext oldcxt = MemoryContextSwitchTo(memcxt);
    SStmtHistScanner* scanner = (SStmtHistScanner*)palloc(sizeof(SStmtHistScanner));

    scanner->scanner[0] = NULL;
    scanner->candidate[0] = NULL;
    scanner->desc = desc;
    scanner->memcxt = memcxt;
    scanner->range_s = time1;
    scanner->range_e = time2;

    /* create mfscanner and fake candidate */
    if (!only_slow) {
        scanner->scanner[0] = MemFileChainScanStart(g_instance.stat_cxt.stbyStmtHistFast, time1, time2);
        scanner->candidate[0] = scanner->scanner[0] == NULL ? NULL : create_fake_candidate();
    }
    scanner->scanner[1] = MemFileChainScanStart(g_instance.stat_cxt.stbyStmtHistSlow, time1, time2);
    scanner->candidate[1] = scanner->scanner[1] == NULL ? NULL : create_fake_candidate();

    /*
     * Let's say 0 is the winner of two candidates, next time it will be replaced by a new candidate of 0.
     * But we have to make sure that candidate 1 is valid or NULL, it will battle with new 
     * candidate of 0 next time.
     */
    scanner->winner = 0;
    if (scanner->scanner[1] != NULL && !get_next_candidate(scanner, 1)) {
        pfree(scanner->candidate[1]);
        scanner->candidate[1] = NULL;
        MemFileChainScanEnd(scanner->scanner[1]);
        scanner->scanner[1] = NULL;
    }

    MemoryContextSwitchTo(oldcxt);
    return scanner;
}

static HeapTuple sstmthist_scanner_getnext(SStmtHistScanner* scanner)
{
    MemoryContext oldcxt = MemoryContextSwitchTo(scanner->memcxt);
    TimestampTz finish_time[2] = {DT_NOBEGIN, DT_NOBEGIN};

    if (scanner->candidate[scanner->winner] != NULL && !get_next_candidate(scanner, scanner->winner)) {
        pfree(scanner->candidate[scanner->winner]);
        scanner->candidate[scanner->winner] = NULL;
        MemFileChainScanEnd(scanner->scanner[scanner->winner]);
        scanner->scanner[scanner->winner] = NULL;
    }

    if (unlikely(scanner->candidate[0] == NULL && scanner->candidate[1] == NULL)) {
        MemoryContextSwitchTo(oldcxt);
        return NULL;
    }

    finish_time[0] = get_candidate_finish_time(scanner->candidate[0], scanner->desc);
    finish_time[1] = get_candidate_finish_time(scanner->candidate[1], scanner->desc);

    if (unlikely(finish_time[0] == DT_NOBEGIN && finish_time[1] == DT_NOBEGIN)) {
        MemoryContextSwitchTo(oldcxt);
        return NULL;
    }

    scanner->winner = finish_time[0] > finish_time[1] ? 0 : 1;
    MemoryContextSwitchTo(oldcxt);
    return scanner->candidate[scanner->winner];
}

static void sstmthist_scanner_end(SStmtHistScanner* scanner)
{
    pfree_ext(scanner->candidate[0]);
    pfree_ext(scanner->candidate[1]);
    MemFileChainScanEnd(scanner->scanner[0]);
    MemFileChainScanEnd(scanner->scanner[1]);
    // desc is a reference, so don't need free.
    pfree(scanner);
}

static void check_sstmthist_permissions()
{
    if (pmState != PM_HOT_STANDBY) {
        ereport(ERROR, 
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("Functions series of standby statement history only supported in standby mode.")));
    }

    if (!superuser() && !isMonitoradmin(GetUserId())) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            (errmsg("Only system/monitor admin can use this function."))));
    }
}

static TupleDesc create_sstmthist_tuple_entry(FunctionCallInfo fcinfo)
{
    RangeVar* relrv = InitStatementRel();
    Relation rel = heap_openrv(relrv, AccessShareLock);
    TupleDesc desc = RelationGetDescr(rel);
    TupleDesc expectedDesc = ((ReturnSetInfo*)fcinfo->resultinfo)->expectedDesc;

    /* Because we only support append cloumn to system table in upgrade, so we just compare natts here. */
    if (desc->natts != expectedDesc->natts) {
        heap_close(rel, AccessShareLock);
        ereport(ERROR, (errmsg("function standby_statement_history does not match relation statement_history.")));
    }

    TupleDesc tmpDesc = CreateTemplateTupleDesc(desc->natts, false, TableAmHeap);
    for (int i = 0; i < desc->natts; i++) {
        TupleDescInitEntry(tmpDesc, (AttrNumber)(i + 1), 
            NameStr(desc->attrs[i].attname), desc->attrs[i].atttypid, -1, 0);
    }

    heap_close(rel, AccessShareLock);
    return tmpDesc;
}

static void parse_sstmthist_args(PG_FUNCTION_ARGS, bool* only_slow, TimestampTz* time1, TimestampTz* time2)
{
    *only_slow = PG_ARGISNULL(0) ? false : PG_GETARG_BOOL(0);
    if (PG_NARGS() == 1) {
        *time1 = DT_NOBEGIN;
        *time2 = DT_NOEND;
        return;
    }
    
    const int max_time_args = 2;
    Assert(PG_NARGS() == max_time_args);
    ArrayType* arr = PG_GETARG_ARRAYTYPE_P(1);
    Oid element_type = ARR_ELEMTYPE(arr);
    int16 elmlen;
    bool elmbyval = false;
    char elmalign;
    int nitems = 0;
    Datum* elements = NULL;
    bool* nulls = NULL;

    get_typlenbyvalalign(element_type, &elmlen, &elmbyval, &elmalign);
    deconstruct_array(arr, element_type, elmlen, elmbyval, elmalign, &elements, &nulls, &nitems);

    if (nitems > max_time_args) {
        pfree_ext(elements);
        pfree_ext(nulls);
        ereport(ERROR, (errmsg("Too many time parameters,  no more than two.")));
    }

    if (nitems == 1) {
        *time1 = nulls[0] ? DT_NOBEGIN : DatumGetTimestampTz(elements[0]);
        *time2 = DT_NOEND;
    } else {
        *time1 = nulls[0] ? DT_NOBEGIN : DatumGetTimestampTz(elements[0]);
        *time2 = nulls[1] ? DT_NOEND : DatumGetTimestampTz(elements[1]);
    }

    pfree_ext(elements);
    pfree_ext(nulls);

    if (*time1 > *time2) {
        ereport(ERROR, (errmsg("Invalid time param, time1 cannot bigger than time2.")));
    }
}

Datum standby_statement_history(PG_FUNCTION_ARGS)
{
    check_sstmthist_permissions();

    FuncCallContext* funcctx = NULL;
    SStmtHistScanner* scanner = NULL;
    if (SRF_IS_FIRSTCALL()) {
        MemoryContext oldContext = NULL;
        TupleDesc tupdesc = NULL;
        TimestampTz time1 = 0;
        TimestampTz time2 = 0;
        bool only_slow = false;

        parse_sstmthist_args(fcinfo, &only_slow, &time1, &time2);

        funcctx = SRF_FIRSTCALL_INIT();
        oldContext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        tupdesc = create_sstmthist_tuple_entry(fcinfo);
        funcctx->tuple_desc = BlessTupleDesc(tupdesc);
        scanner = sstmthist_scanner_start(CurrentMemoryContext, only_slow, tupdesc, time1, time2);
        MemoryContextSwitchTo(oldContext);

        if (scanner == NULL) {
            SRF_RETURN_DONE(funcctx);
        } else {
            funcctx->user_fctx = (void*)scanner;
        }
    }

    funcctx = SRF_PERCALL_SETUP();
    scanner = (SStmtHistScanner*)funcctx->user_fctx;
    HeapTuple tuple = sstmthist_scanner_getnext(scanner);
    if (tuple == NULL) {
        sstmthist_scanner_end(scanner);
        SRF_RETURN_DONE(funcctx);
    } else {
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }
}

Datum standby_statement_history_1v(PG_FUNCTION_ARGS)
{
    return standby_statement_history(fcinfo);
}
