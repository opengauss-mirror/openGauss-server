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
 * -------------------------------------------------------------------------
 * snapshot.cpp
 *
 *    Automatically collect MPP snapshots in the background,
 * you can also manually collect snapshots
 *
 * IDENTIFICATION
 *	  src/gausskernel/cbb/instruments/wdr/snapshot.cpp
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "access/xact.h"
#include "postmaster/postmaster.h"
#include "pgxc/pgxc.h"
#include "pgxc/pgxcnode.h"
#include "pgxc/poolutils.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"
#include "miscadmin.h"
#include "storage/proc.h"
#include "storage/latch.h"
#include "storage/ipc.h"
#include "workload/workload.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_database.h"
#include "gssignal/gs_signal.h"
#include "utils/acl.h"
#include "utils/guc.h"
#include "utils/ps_status.h"
#include "utils/elog.h"
#include "utils/numeric.h"
#include "utils/memprot.h"
#include "utils/builtins.h"
#include "tcop/dest.h"
#include "tcop/tcopprot.h"
#include "gs_thread.h"
#include "access/heapam.h"
#include "optimizer/autoanalyzer.h"
#include "utils/rel.h"
#include "utils/postinit.h"
#include "pgstat.h"
#include "pgxc/execRemote.h"
#include "workload/gscgroup.h"
#include "commands/dbcommands.h"
#include "instruments/generate_report.h"
#include "instruments/snapshot.h"
#include "instruments/dblink_query.h"
#include "libpq/pqsignal.h"
#include "pgxc/groupmgr.h"

const int PGSTAT_RESTART_INTERVAL = 60;
#define COUNT_ARRAY_SIZE(array) (sizeof((array)) / sizeof(*(array)))

using namespace std;

/*
 * function
 */
NON_EXEC_STATIC void SnapshotMain();

namespace SnapshotNameSpace {
void CleanSnapshot(uint64 curr_min_snapid);
void take_snapshot(void);
void SubSnapshotMain(void);
void InsertOneTableData(const char** views, int numViews, uint64 snapid);
void InsertTablesData(uint64 snapid);
void DeleteTablesData(const char** views, int numViews, uint64 snapid);
void GetQueryData(const char* query, bool with_column_name, List** cstring_values);
bool ExecuteQuery(const char* query, int SPI_OK_STAT);
void init_curr_table_size(void);
void init_curr_snapid(void);
void CreateTable(const char** views, int numViews, bool ismultidbtable);
void InitTables(void);
void CreateSnapStatTables(void);
void CreateIndexes(const char* views);
void CreateSequence(void);
void UpdateSnapEndTime(uint64 curr_snapid);
void GetQueryStr(StringInfoData& query, const char* viewname, uint64 curr_snapid, const char* dbname);
void GetDatabaseList(List** databaselist);
void InsertOneDbTables(const char** views, int numViews, uint64 curr_snapid);
void InsertDatabaseData(const char* dbname, uint64 curr_snapid, const char** views, int numViews);
void SplitString(const char* str, const char* delim, List** res);
void SetSnapshotSize(uint32* max_snap_size);
void SleepCheckInterrupt(uint32 minutes);
void ResetPortDefaultKeepalivesValue(void);
void SetThrdCxt(void);
void AnalyzeTable(List* analyzeList);
void GetAnalyzeList(List** analyzeList);
char* GetTableColAttr(const char* viewname, bool onlyViewCol, bool addType);
}  // namespace SnapshotNameSpace

/*
 * select these views in a different database gives the same result,
 * We just need to snapshot these views under openGauss database
 */
static const char* sharedViews[] = {"global_os_runtime", "global_os_threads", "global_instance_time",
    "summary_workload_sql_count", "summary_workload_sql_elapse_time", "global_workload_transaction",
    "summary_workload_transaction", "global_thread_wait_status", "global_memory_node_detail",
    "global_shared_memory_detail",
#ifdef ENABLE_MULTIPLE_NODES
    "global_comm_delay", "global_comm_recv_stream", "global_comm_send_stream", "global_comm_status",
    "global_pooler_status",
#endif
    "global_stat_db_cu", "global_stat_database", "summary_stat_database",
    "global_stat_database_conflicts", "summary_stat_database_conflicts",
    "global_stat_bad_block", "summary_stat_bad_block", "global_file_redo_iostat", "summary_file_redo_iostat",
    "global_rel_iostat", "summary_rel_iostat", "global_file_iostat", "summary_file_iostat",
    "global_replication_slots", "global_bgwriter_stat", "global_replication_stat",
    "global_transactions_running_xacts", "summary_transactions_running_xacts",
    "global_transactions_prepared_xacts", "summary_transactions_prepared_xacts", "summary_statement",
    "global_statement_count", "summary_statement_count", "global_config_settings", "global_wait_events",
    "summary_user_login", "global_ckpt_status", "global_double_write_status",
    "global_pagewriter_status", "global_redo_status",
    "global_rto_status", "global_recovery_status", "global_threadpool_status",
    "statement_responsetime_percentile"};
/*
 * These views represent the state of the database in which they are located
 * select these views in a different database gives the different result,
 * We just need to snapshot these views under different database
 */
static const char* dbRelatedViews[] = {"global_statio_all_indexes", "summary_statio_all_indexes",
    "global_statio_all_sequences", "summary_statio_all_sequences", "global_statio_all_tables",
    "summary_statio_all_tables", "global_stat_all_indexes", "summary_stat_all_indexes",
    "summary_stat_user_functions", "global_stat_user_functions", "global_stat_all_tables", "summary_stat_all_tables"};

/* At each snapshot, these views must be placed behind them for the snapshot */
static const char* lastDbRelatedViews[] = {"class_vital_info"};
static const char* lastStatViews[] = {"global_record_reset_time"};
void instrSnapshotClean(void)
{
    dblinkCloseConn();
}

void instrSnapshotCancel(void)
{
    dblinkRequestCancel();
}

/* SIGQUIT signal handler for auditor process */
static void instr_snapshot_exit(SIGNAL_ARGS)
{
    t_thrd.perf_snap_cxt.need_exit = true;
    die(postgres_signal_arg);
}

/* SIGHUP handler for collector process */
static void instr_snapshot_sighup_handler(SIGNAL_ARGS)
{
    t_thrd.perf_snap_cxt.got_SIGHUP = true;
}

static void request_snapshot(SIGNAL_ARGS)
{
    t_thrd.perf_snap_cxt.request_snapshot = true;
}
static char* GetCurrentTimeStampString()
{
    return Datum_to_string(TimestampGetDatum(GetCurrentTimestamp()), TIMESTAMPTZOID, false);
}
/* check node group is exist */
static bool CheckNodeGroup()
{
#ifndef ENABLE_MULTIPLE_NODES
    return true;
#endif
    errno_t rc;
    SPI_STACK_LOG("connect", NULL, NULL);
    if ((rc = SPI_connect()) != SPI_OK_CONNECT) {
        ereport(ERROR,
            (errmodule(MOD_WDR_SNAPSHOT),
            errcode(ERRCODE_SPI_CONNECTION_FAILURE),
            errmsg("SPI_connect failed: %s", SPI_result_code_string(rc)),
            errdetail("SPI_connect failed in function CheckNodeGroup"),
            errcause("System error."),
            erraction("Analyze the error message before the error")));
    }
    Datum colval;
    bool isnull = false;
    const char* query =
        "select pg_catalog.count(*) from pgxc_group WHERE in_redistribution='n' and is_installation = TRUE;";
    colval = GetDatumValue(query, 0, 0, &isnull);
    SPI_STACK_LOG("finish", NULL, NULL);
    SPI_finish();
    return !isnull && (colval != 0);
}

/* 1. during redistribution, will set 'in_redistribution' field to 'y'
 *    in pgxc_group table, exit snapshot thread to avoid dead lock issue
 * 2. during upgrade, snapshot table can be modified,
 *    exit snapshot thread to avoid insert data failed
 */
static void check_snapshot_thd_exit()
{
    bool need_exit = false;

    start_xact_command();
    PushActiveSnapshot(GetTransactionSnapshot());
    char* redis_group = PgxcGroupGetInRedistributionGroup();
    if (redis_group != NULL || !CheckNodeGroup()) {
        need_exit = true;
        u_sess->attr.attr_common.ExitOnAnyError = true;
    }
    PopActiveSnapshot();
    finish_xact_command();

    if (need_exit || u_sess->attr.attr_common.upgrade_mode != 0) {
        /* to avoid postmaster to start snapshot thread again immediately,
         * we sleep several minute before exit.
         */
        const int SLEEP_GAP = 5;
        ereport(LOG, (errmsg("snapshot thread will exit during redistribution, upgrade or init group")));
        SnapshotNameSpace::SleepCheckInterrupt(SLEEP_GAP);
        ereport(LOG, (errmsg("snapshot thread is exited during redistribution, upgrade or init group")));

        gs_thread_exit(0);
    }
}

/*
 * If enable_memory_limit is off,
 * select session memory related views will report an error.
 * The snapshot thread successfully collects global session memory information.
 * Ensure that t_thrd.utils_cxt.gs_mp_inited of each node is true.
 * Before each snapshot thread collects information,
 * it will first determine whether the view related to session memory can be selected
 */
static bool CheckMemProtectInit()
{
    bool res = false;
    MemoryContext currentCtx = CurrentMemoryContext;
    PG_TRY();
    {
        errno_t rc;
        start_xact_command();
        PushActiveSnapshot(GetTransactionSnapshot());
        SPI_STACK_LOG("connect", NULL, NULL);
        if ((rc = SPI_connect()) != SPI_OK_CONNECT)
            ereport(ERROR,
                (errmodule(MOD_WDR_SNAPSHOT),
                errcode(ERRCODE_SPI_CONNECTION_FAILURE),
                errmsg("SPI_connect failed: %s", SPI_result_code_string(rc)),
                errdetail("SPI_connect failed in function CheckMemProtectInit"),
                errcause("System error."),
                erraction("Analyze the error message before the error")));
        const char *query = "select * from DBE_PERF.global_memory_node_detail";
        (void)SnapshotNameSpace::ExecuteQuery(query, SPI_OK_SELECT);
        SPI_STACK_LOG("finish", query, NULL);
        SPI_finish();
        PopActiveSnapshot();
        finish_xact_command();
        res = true;
    }
    PG_CATCH();
    {
        SPI_STACK_LOG("finish", NULL, NULL);
        SPI_finish();
        (void)MemoryContextSwitchTo(currentCtx);
        ErrorData *edata = CopyErrorData();
        ereport(LOG, (errmsg("Failed to snapshot global_memory_node_detail, cause: %s", edata->message)));
        FlushErrorState();
        FreeErrorData(edata);
        PopActiveSnapshot();
        AbortCurrentTransaction();
        t_thrd.postgres_cxt.xact_started = false;
    }
    PG_END_TRY();
    return res;
}

/* kill snapshot thread on all CN node */
void kill_snapshot_remote()
{
    ExecNodes* exec_nodes = NULL;
    StringInfoData buf;
    ParallelFunctionState* state = NULL;

    exec_nodes = (ExecNodes*)makeNode(ExecNodes);
    exec_nodes->baselocatortype = LOCATOR_TYPE_HASH;
    exec_nodes->accesstype = RELATION_ACCESS_READ;
    exec_nodes->primarynodelist = NIL;
    exec_nodes->en_expr = NULL;
    exec_nodes->en_relid = InvalidOid;
    exec_nodes->nodeList = NIL;

    initStringInfo(&buf);
    appendStringInfo(&buf, "SELECT pg_catalog.kill_snapshot();");
    state = RemoteFunctionResultHandler(buf.data, exec_nodes, NULL, true, EXEC_ON_COORDS, true);
    FreeParallelFunctionState(state);
    pfree_ext(buf.data);
}
static void CheckPermission()
{
    if (!superuser() && !has_rolreplication(GetUserId()) &&
        !is_member_of_role(GetUserId(), DEFAULT_ROLE_REPLICATION)) {
        ereport(ERROR, (errmodule(MOD_WDR_SNAPSHOT), errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            errmsg("permission denied for terminate snapshot thread"),
            errdetail("only system admin or replication role or a member of the gs_role_replication role"
                "can terminate snapshot thread"),
            errcause("The user does not have system admin privilege"), erraction("Grant system admin to user")));
    } 
}
/*
 * kill snapshot thread
 * There will be deadlocks between snapshot thread and redistribution,
 * so during redistribution, will stop snapshot thread.
 *
 * return true if killed snapshot thread successfully
 */
Datum kill_snapshot(PG_FUNCTION_ARGS)
{
    CheckPermission();
#ifdef ENABLE_MULTIPLE_NODES
    if (IS_PGXC_COORDINATOR) {
#endif
        /* CN need to kill snapshot thread */
        if (g_instance.pid_cxt.SnapshotPID != 0) {
            if (PgxcGroupGetInRedistributionGroup() != NULL) {
                ereport(LOG, (errmsg("snapshot thread - redistribution in progress")));
            }

            /* wait 100 seconds */
            const int MAX_RETRY_COUNT = 1000000;
            const int SLEEP_GAP = 100;
            int err = 0;
            uint32 old_snapshot_thread_counter = pg_atomic_read_u32(&g_instance.stat_cxt.snapshot_thread_counter);
            if ((err = gs_signal_send(g_instance.pid_cxt.SnapshotPID, SIGTERM)) != 0) {
                ereport(ERROR, (errmodule(MOD_WDR_SNAPSHOT), errcode(ERRCODE_OPERATE_FAILED),
                    errmsg("terminate snapshot thread failed"),
                    errdetail("N/A"), errcause("Execution failed due to: %s", gs_strerror(err)),
                    erraction("check if snapshot thread exists")));
            } else {
                int wait_times = 0;
                /* wait snapshot thread is exit.
                 * - exit case: g_instance.pid_cxt.SnapshotPID is zero
                 * - new start case: g_instance.stat_cxt.snapshot_thread_counter will be changed
                 *
                 * new started snapshot thread PID can be same with before,
                 * so use a status counter to indicate snapshot thread restart
                 */
                while (g_instance.pid_cxt.SnapshotPID != 0 &&
                       g_instance.stat_cxt.snapshot_thread_counter == old_snapshot_thread_counter) {
                    if (wait_times++ > MAX_RETRY_COUNT) {
                        ereport(ERROR, (errmodule(MOD_WDR_SNAPSHOT), errcode(ERRCODE_OPERATE_FAILED),
                            errmsg("terminate snapshot thread failed"),
                            errdetail("wait time(%d), exceeds MAX_RETRY_COUNT(%d)", wait_times, MAX_RETRY_COUNT),
                            errcause("restart wdr snapshot thread timeout"
                                "or The thread did not respond to the kill signal"),
                            erraction("Check the wdr snapshot thread is restarted")));
                    }
                    pg_usleep(SLEEP_GAP);
                }
                ereport(LOG, (errmsg("snapshot thread is killed")));
            }
        }

        if (!IsConnFromCoord()) {
            /* get kill snapshot request from GSQL/JDBC/SPI, etc, notity to other CNs */
            ereport(LOG, (errmsg("send 'kill snapshot thread' request to other CNs")));
#ifdef ENABLE_MULTIPLE_NODES
            kill_snapshot_remote();
#endif
        }
#ifdef ENABLE_MULTIPLE_NODES
    }
#endif
    PG_RETURN_VOID();
}
static void ReloadInfo()
{
    /* Reload configuration if we got SIGHUP from the postmaster. */
    if (t_thrd.perf_snap_cxt.got_SIGHUP) {
        t_thrd.perf_snap_cxt.got_SIGHUP = false;
        ProcessConfigFile(PGC_SIGHUP);
    }
    if (IsGotPoolReload()) {
        processPoolerReload();
        ResetGotPoolReload(false);
    }
}

/* to avoid deadlock with APP or gs_redis, set lockwait_timeout to 3s */
static void set_lock_timeout()
{
    int rc = 0;
    /* lock wait timeout to 3s */
    const char* timeout_sql = "set lockwait_timeout = 3000";
    if ((rc = SPI_execute(timeout_sql, false, 0)) != SPI_OK_UTILITY) {
        ereport(ERROR,
            (errmodule(MOD_WDR_SNAPSHOT),
            errcode(ERRCODE_INTERNAL_ERROR),
            errmsg("set lockwait_timeout failed"),
            errdetail("error code: %s", SPI_result_code_string(rc)),
            errcause("System error."),
            erraction("Contact engineer to support.")));
    }
}

/*
 * Manually performing a snapshot
 */
Datum create_wdr_snapshot(PG_FUNCTION_ARGS)
{
    const uint32 maxTryCount = 100;
    const uint32 waitTime = 100000;
    /* ensure limited access to create the snapshot */
    if (!superuser()) {
        ereport(ERROR,
            (errmodule(MOD_WDR_SNAPSHOT),
            errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            errmsg("permission denied for create WDR Snapshot"),
            errdetail("Superuser privilege is need to operate snapshot"),
            errcause("The user does not have system admin privilege"),
            erraction("Grant system admin to user")));
    }
    if (!u_sess->attr.attr_common.enable_wdr_snapshot) {
        ereport(WARNING, (errcode(ERRCODE_WARNING), errmsg("GUC parameter 'enable_wdr_snapshot' is off")));
        PG_RETURN_TEXT_P(cstring_to_text("WDR snapshot request can't be executed"));
        ;
    }
    if (!PgxcIsCentralCoordinator(g_instance.attr.attr_common.PGXCNodeName) && (IS_SINGLE_NODE != true)) {
        ereport(NOTICE, (errmsg("take snapshot must be on CCN node")));
        PG_RETURN_TEXT_P(cstring_to_text("WDR snapshot request can't be executed"));
    }
    int err = 0;
    for (uint32 ntries = 0;; ntries++) {
        if (g_instance.pid_cxt.SnapshotPID == 0) {
            if (ntries >= maxTryCount) {
                ereport(ERROR,
                    (errmodule(MOD_WDR_SNAPSHOT),
                    errcode(ERRCODE_OPERATE_FAILED),
                    errmsg("WDR snapshot request can not be accepted, please retry later"),
                    errdetail("N/A"),
                    errcause("wdr snapshot thread does not exist"),
                    erraction("Check if wdr snapshot thread exists")));
            }
        } else if ((err = gs_signal_send(g_instance.pid_cxt.SnapshotPID, SIGINT)) != 0) {
            if (ntries >= maxTryCount) {
                ereport(ERROR,
                    (errmodule(MOD_WDR_SNAPSHOT),
                    errcode(ERRCODE_OPERATE_FAILED),
                    errmsg("Cannot respond to WDR snapshot request"),
                    errdetail("N/A"),
                    errcause("Execution failed due to: %s", gs_strerror(err)),
                    erraction("Check if wdr snapshot thread exists")));
            }
        } else {
            break;
        }
        CHECK_FOR_INTERRUPTS();
        pg_usleep(waitTime); /* wait 0.1 sec, then retry */
    }
    PG_RETURN_TEXT_P(cstring_to_text("WDR snapshot request has been submitted"));
}
/*
 * Execute query
 * parameter:
 *    query     -- query text
 *    SPI_OK_STAT -- Query execution status
 */
bool SnapshotNameSpace::ExecuteQuery(const char* query, int SPI_OK_STAT)
{
    Assert(query != NULL);

    if (SPI_execute(query, false, 0) != SPI_OK_STAT) {
        ereport(WARNING, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("invalid query : %s", query)));
        return false;
    }
    return true;
}

/*
 * GetDatumValue
 * return the result of select sql
 * but the type of return value is Datum, You must
 * convert the value to the corresponding type
 * row : the row of value (begin from 0)
 * col : the col of value (begin from 0)
 */
Datum GetDatumValue(const char* query, uint32 row, uint32 col, bool* isnull)
{
    Datum colval;
    bool tmp_isnull = false;
    Assert(query != NULL);

    if (!SnapshotNameSpace::ExecuteQuery(query, SPI_OK_SELECT)) {
        ereport(ERROR, (errmodule(MOD_WDR_SNAPSHOT), errcode(ERRCODE_DATA_EXCEPTION),
            errmsg("query(%s) can not get datum values", query),
            errdetail("An error occurred in function GetDatumValue"),
            errcause("System error."),
            erraction("Check whether the query can be executed")));
    }

    /* if row and col out of the size of query values, we return the values is null */
    if (row >= SPI_processed || col >= (uint32)SPI_tuptable->tupdesc->natts) {
        ereport(WARNING,
            (errcode(ERRCODE_DATA_EXCEPTION),
                errmsg("row : %d, col : %d out of the size of query data in get datum value", row, col)));
        return (Datum)NULL;
    }
    colval = SPI_getbinval(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, (col + 1), isnull ? isnull : &tmp_isnull);
    return colval;
}
/*
 * Initialize the size of the current table Every time
 * you boot you need to check the size of the table
 * size of the snapshot to initialize the curr_table_size.
 */
void SnapshotNameSpace::init_curr_table_size(void)
{
    Datum colval;
    bool isnull = false;
    const char* sql = "select pg_catalog.count(*) from snapshot.snapshot";
    colval = GetDatumValue(sql, 0, 0, &isnull);
    if (isnull) {
        colval = 0;
    }
    t_thrd.perf_snap_cxt.curr_table_size = DatumGetInt32(colval);
}

void SnapshotNameSpace::CreateSequence(void)
{
    Datum colval;
    bool isnull = false;
    const char* query_seq_sql =
        "select pg_catalog.count(*) from pg_class c left join pg_namespace n"
        " on n.oid = c.relnamespace"
        " where n.nspname = 'snapshot' and c.relname = 'snap_seq'";
    colval = GetDatumValue(query_seq_sql, 0, 0, &isnull);
    if (isnull) {
        colval = 0;
    }
    if (!colval) {
        const char* create_seq_sql = "create sequence snapshot.snap_seq CYCLE";
        if (!SnapshotNameSpace::ExecuteQuery(create_seq_sql, SPI_OK_UTILITY)) {
            ereport(ERROR, (errmodule(MOD_WDR_SNAPSHOT), errcode(ERRCODE_DATA_EXCEPTION),
                errmsg("create sequence failed"),
                errdetail("query(%s) execute failed", create_seq_sql),
                errcause("System error."),
                erraction("Check if sequence can be created")));
        }
    }
}
void SnapshotNameSpace::init_curr_snapid(void)
{
    Datum colval;
    bool isnull = false;
    const char* sql = "select pg_catalog.nextval('snapshot.snap_seq')";
    colval = GetDatumValue(sql, 0, 0, &isnull);
    if (isnull) {
        colval = 0;
    }
    /* MAX value of the snapshot.snap_seq is MAX INT64 */
    t_thrd.perf_snap_cxt.curr_snapid = numeric_int16_internal(DatumGetNumeric(colval));
}
/*
 * UpdateSnapEndTime -- update snapshot end time stamp
 */
void SnapshotNameSpace::UpdateSnapEndTime(uint64 curr_snapid)
{
    if (curr_snapid == 0) {
        return;
    }
    StringInfoData query;
    initStringInfo(&query);
    appendStringInfo(&query,
        "update snapshot.snapshot set end_ts = '%s' where snapshot_id = %lu",
        GetCurrentTimeStampString(),
        curr_snapid);
    if (!SnapshotNameSpace::ExecuteQuery(query.data, SPI_OK_UPDATE)) {
        ereport(ERROR, (errmodule(MOD_WDR_SNAPSHOT), errcode(ERRCODE_DATA_EXCEPTION),
            errmsg("update snapshot end time stamp filled"),
            errdetail("query(%s) execute failed", query.data),
            errcause("System error."),
            erraction("Check whether the snapshot retry is successful")));
    }
    pfree_ext(query.data);
}

void SnapshotNameSpace::SetSnapshotSize(uint32* max_snap_size)
{
    const int HOUR_OF_DAY = 24;
    const int MINUTE_Of_HOUR = 60;
    /*
     * 1 <= wdr_snapshot_retention_days <= 8(d)
     * 10 <= wdr_snapshot_interval <= 60(min)
     */
    *max_snap_size = u_sess->attr.attr_common.wdr_snapshot_retention_days * HOUR_OF_DAY * MINUTE_Of_HOUR /
                     u_sess->attr.attr_common.wdr_snapshot_interval;
}

void SnapshotNameSpace::GetQueryData(const char* query, bool with_column_name, List** cstring_values)
{
    bool isnull = false;
    List* colname_cstring = NIL;
    if (cstring_values != NULL) {
        list_free_deep(*cstring_values);
    }
    /* Establish SPI connection and get query execution result */
    ereport(DEBUG1, (errmodule(MOD_WDR_SNAPSHOT), errmsg("[Instruments/Report] query: %s", query)));

    if (SPI_execute(query, false, 0) != SPI_OK_SELECT) {
        ereport(ERROR, (errmodule(MOD_WDR_SNAPSHOT), errcode(ERRCODE_DATA_EXCEPTION),
            errmsg("query can not get datum values"),
            errdetail("query(%s) execute failed", query),
            errcause("System error."),
            erraction("Check whether the query can be executed")));
    }
    /* get colname */
    if (with_column_name) {
        for (int32 i = 0; i < SPI_tuptable->tupdesc->natts; i++) {
            char* strName = pstrdup(SPI_tuptable->tupdesc->attrs[i].attname.data);
            colname_cstring = lappend(colname_cstring, strName);
        }
        *cstring_values = lappend(*cstring_values, colname_cstring);
    }
    /* Get the data in the table and convert it to string format */
    for (uint32 i = 0; i < SPI_processed; i++) {
        List* row_string = NIL;
        uint32 colNum = (uint32)SPI_tuptable->tupdesc->natts;
        for (uint32 j = 1; j <= colNum; j++) {
            Oid type = SPI_gettypeid(SPI_tuptable->tupdesc, j);
            Datum colval = SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, j, &isnull);
            char* tmpStr = Datum_to_string(colval, type, isnull);
            row_string = lappend(row_string, tmpStr);
        }
        *cstring_values = lappend(*cstring_values, row_string);
    }
}

#if ((defined(ENABLE_MULTIPLE_NODES)) || (defined(ENABLE_PRIVATEGAUSS)))
/*
 * SnapshotView
 * Since the snapshot table can be queried only when the parameter is on,
 * the view is created during snapshot initialization.
 * when the parameter is off, an empty view is provided.
 */
static void SnapshotView()
{
    const char* createSnapshotviewdrop = "drop view if exists SYS.ADM_HIST_SNAPSHOT";
    if (SPI_execute(createSnapshotviewdrop, false, 0) != SPI_OK_UTILITY) {
        ereport(ERROR, (errmodule(MOD_WDR_SNAPSHOT), errcode(ERRCODE_DATA_EXCEPTION),
            errmsg("drop view failed"),
            errdetail("query(%s) execute failed", createSnapshotviewdrop),
            errcause("System error."),
            erraction("Check whether the query can be executed")));
    }
    const char* createSnapshotview = "CREATE OR REPLACE VIEW SYS.ADM_HIST_SNAPSHOT AS "
        "SELECT s.snapshot_id AS SNAP_ID, s.start_ts AS BEGIN_INTERVAL_TIME FROM SNAPSHOT.SNAPSHOT s";
    if (SPI_execute(createSnapshotview, false, 0) != SPI_OK_UTILITY) {
        ereport(ERROR, (errmodule(MOD_WDR_SNAPSHOT), errcode(ERRCODE_DATA_EXCEPTION),
            errmsg("CREATE VIEW failed"),
            errdetail("query(%s) execute failed", createSnapshotview),
            errcause("System error."),
            erraction("Check whether the query can be executed")));
    }
}
#endif
/*
 * take_snapshot
 * All the snapshot processes are executed here,
 * we will collect snapshot info from views in dbe_perf schema,
 * then insert into snapshot tables in snapshot schema, In order to control
 * the amount of disk space occupied by the snapshot,
 * we use curr_tables_size to record the number of snapshots,
 * and use MAX_TABLE_SIZE to control the size of the snapshot disk space.
 */
void SnapshotNameSpace::take_snapshot()
{
    StringInfoData query;
    initStringInfo(&query);
    PG_TRY();
    {
        int rc = 0;
        uint32 max_snap_size = 0;
        SnapshotNameSpace::SetSnapshotSize(&max_snap_size);
        /* connect SPI to execute query */
        SPI_STACK_LOG("connect", NULL, NULL);
        if ((rc = SPI_connect()) != SPI_OK_CONNECT) {
            ereport(ERROR, (errmodule(MOD_WDR_SNAPSHOT), errcode(ERRCODE_INTERNAL_ERROR),
                errmsg("SPI_connect failed: %s", SPI_result_code_string(rc)),
                errdetail("N/A"), errcause("System error."),
                erraction("Check whether the snapshot retry is successful")));
        }
        set_lock_timeout();
        SnapshotNameSpace::init_curr_table_size();
        while (t_thrd.perf_snap_cxt.curr_table_size >= max_snap_size) {
            bool isnull = false;
            const char* sql = "select snapshot_id from snapshot.snapshot order by start_ts limit 1";
            Datum colval = GetDatumValue(sql, 0, 0, &isnull);
            if (isnull) {
                colval = 0;
            }
            SnapshotNameSpace::CleanSnapshot(DatumGetObjectId(colval));
            t_thrd.perf_snap_cxt.curr_table_size--;
        }
        SnapshotNameSpace::init_curr_snapid();
        appendStringInfo(&query,
            "INSERT INTO snapshot.snapshot(snapshot_id, start_ts) "
            "values (%lu, '%s')", t_thrd.perf_snap_cxt.curr_snapid,
            GetCurrentTimeStampString());
        if (SPI_execute(query.data, false, 0) == SPI_OK_INSERT) {
            SnapshotNameSpace::InsertTablesData(t_thrd.perf_snap_cxt.curr_snapid);
            t_thrd.perf_snap_cxt.curr_table_size++;
        } else {
            ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
                errmsg("query(%s) execute failed", query.data),
                errdetail("N/A"), errcause("System error."),
                erraction("Check whether the snapshot retry is successful")));
        }
        SnapshotNameSpace::UpdateSnapEndTime(t_thrd.perf_snap_cxt.curr_snapid);
        SPI_STACK_LOG("finish", query.data, NULL);
        pfree_ext(query.data);
        SPI_finish();
    }
    PG_CATCH();
    {
        SPI_STACK_LOG("finish", query.data, NULL);
        pfree_ext(query.data);
        SPI_finish();
        /* Carry on with error handling. */
        PG_RE_THROW();
    }
    PG_END_TRY();
}

static bool IsCgroupInit(const char* viewname)
{
    StringInfoData query;

    initStringInfo(&query);
    if (strncmp(viewname, "wlm_controlgroup_config", strlen("wlm_controlgroup_config")) == 0 ||
        strncmp(viewname, "wlm_controlgroup_ng_config", strlen("wlm_controlgroup_ng_config")) == 0 ||
        strncmp(viewname, "wlm_cgroup_config", strlen("wlm_cgroup_config")) == 0) {
        if (g_instance.wlm_cxt->gscgroup_init_done == 0) {
            return false;
        }
    }
    pfree_ext(query.data);
    return true;
}

void SnapshotNameSpace::DeleteTablesData(const char** views, int numViews, uint64 snapid)
{
    StringInfoData query;
    initStringInfo(&query);
    for (int i = 0; i < numViews; i++) {
        if (!IsCgroupInit(views[i])) {
            continue;
        }
        resetStringInfo(&query);
        appendStringInfo(&query, "delete from snapshot.snap_%s where snapshot_id = %lu", views[i], snapid);
        if (!SnapshotNameSpace::ExecuteQuery(query.data, SPI_OK_DELETE)) {
            ereport(ERROR, (errmodule(MOD_WDR_SNAPSHOT), errcode(ERRCODE_DATA_EXCEPTION),
                errmsg("clean table of snap_%s is failed", views[i]), 
                errdetail("N/A"),
                errcause("System error."),
                erraction("Check whether the snapshot retry is successful")));
        }
    }
    pfree_ext(query.data);
}

void SnapshotNameSpace::GetAnalyzeList(List** analyzeTableList)
{
    Datum colval;
    bool isnull = false;
    StringInfoData query;
    initStringInfo(&query);
    const char* tableName = "tables_snap_timestamp";
    appendStringInfo(&query,
        "select relid from dbe_perf.class_vital_info "
        "where schemaname = 'snapshot' and relname = '%s'",
        tableName);
    colval = GetDatumValue(query.data, 0, 0, &isnull);
    if (!colval) {
        ereport(ERROR, (errmodule(MOD_WDR_SNAPSHOT), errcode(ERRCODE_DATA_EXCEPTION),
            errmsg("analyze table failed"),
            errdetail("table(%s) not exist", tableName),
            errcause("System error."),
            erraction("Check whether the snapshot retry is successful")));
    }
    MemoryContext old_context = MemoryContextSwitchTo(t_thrd.perf_snap_cxt.PerfSnapMemCxt);
    Oid* relationObjectId = (Oid*)palloc(sizeof(Oid));
    *relationObjectId = DatumGetObjectId(colval);
    *analyzeTableList = lappend(*analyzeTableList, relationObjectId);
    (void)MemoryContextSwitchTo(old_context);
    pfree_ext(query.data);
}

void SnapshotNameSpace::AnalyzeTable(List* analyzeList)
{
    Relation relation = NULL;
    foreach_cell(cellAnalyze, analyzeList)
    {
        Oid* analyzeName = (Oid*)lfirst(cellAnalyze);
        relation = heap_open(*analyzeName, NoLock);
        bool is_analyzed = AutoAnaProcess::runAutoAnalyze(relation);
        if (!is_analyzed) {
            ereport(WARNING, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("snapshot analyze is failed")));
        }
        heap_close(relation, NoLock);
    }
}

/*
 * InsertOneTableData -- insert snapshot into snapshot.snap_xxx_xxx table
 *
 * insert into a table of snapshot schema from a view data from dbe_perf schema
 * and must get start time and end time when snapshot table insert
 */
void SnapshotNameSpace::InsertOneTableData(const char** views, int numViews, uint64 snapid)
{
    char* dbName = NULL;
    StringInfoData query;
    initStringInfo(&query);

    dbName = get_and_check_db_name(u_sess->proc_cxt.MyDatabaseId, true);
    for (int i = 0; i < numViews; i++) {
        if (!t_thrd.perf_snap_cxt.is_mem_protect &&
            (strcmp(views[i], "global_memory_node_detail") == 0))
            continue;
        resetStringInfo(&query);
        CHECK_FOR_INTERRUPTS();
        /* Record snapshot start time of a single table */
        appendStringInfo(&query,
            "INSERT INTO snapshot.tables_snap_timestamp(snapshot_id, db_name, tablename, start_ts) "
            "values(%lu, '%s', 'snap_%s', '%s')",
            snapid,
            dbName,
            views[i],
            GetCurrentTimeStampString());
        if (!SnapshotNameSpace::ExecuteQuery(query.data, SPI_OK_INSERT)) {
            ereport(ERROR, (errmodule(MOD_WDR_SNAPSHOT), errcode(ERRCODE_DATA_EXCEPTION),
                errmsg("insert into tables_snap_timestamp start time stamp is failed"),
                errdetail("query(%s) execute failed", query.data),
                errcause("System error."),
                erraction("Check whether the snapshot retry is successful")));
        }

        CHECK_FOR_INTERRUPTS();
        resetStringInfo(&query);
        char* snapColAttr = SnapshotNameSpace::GetTableColAttr(views[i], false, false);
        char* colAttr = SnapshotNameSpace::GetTableColAttr(views[i], true, false);
        appendStringInfo(&query,
            "INSERT INTO snapshot.snap_%s(snapshot_id, %s) select %lu, %s from dbe_perf.%s",
            views[i],
            snapColAttr,
            snapid,
            colAttr,
            views[i]);
        pfree(colAttr);
        pfree(snapColAttr);
        if (!SnapshotNameSpace::ExecuteQuery(query.data, SPI_OK_INSERT)) {
            ereport(ERROR, (errmodule(MOD_WDR_SNAPSHOT), errcode(ERRCODE_DATA_EXCEPTION),
                errmsg("insert data failed"),
                errdetail("query(%s) execute failed", query.data),
                errcause("System error."),
                erraction("Check whether the snapshot retry is successful "
                    "and check whether the query can be executed")));
        }
        CHECK_FOR_INTERRUPTS();

        resetStringInfo(&query);
        /* Record snapshot end time of a single table */
        appendStringInfo(&query,
            "update snapshot.tables_snap_timestamp set end_ts = '%s' "
            "where snapshot_id = %lu and db_name = '%s' and tablename = 'snap_%s'",
            GetCurrentTimeStampString(), snapid, dbName, views[i]);
        if (views == lastStatViews) {
            appendStringInfo(&query,
                " and start_ts = (select pg_catalog.max(start_ts) from snapshot.tables_snap_timestamp "
                "where snapshot_id = %lu and db_name = '%s' and tablename = 'snap_%s')",
                snapid, dbName, views[i]);
        }
        if (!SnapshotNameSpace::ExecuteQuery(query.data, SPI_OK_UPDATE)) {
            ereport(ERROR, (errmodule(MOD_WDR_SNAPSHOT), errcode(ERRCODE_DATA_EXCEPTION),
                errmsg("update tables_snap_timestamp end time stamp is failed"),
                errdetail("query(%s) execute failed", query.data),
                errcause("System error."),
                erraction("Check whether the snapshot retry is successful")));
        }
    }
    pfree_ext(dbName);
    pfree_ext(query.data);
}

void SnapshotNameSpace::InsertTablesData(uint64 snapid)
{
    int numViews = COUNT_ARRAY_SIZE(lastStatViews);
    SnapshotNameSpace::InsertOneTableData(lastStatViews, numViews, snapid);
    numViews = COUNT_ARRAY_SIZE(dbRelatedViews);
    SnapshotNameSpace::InsertOneDbTables(dbRelatedViews, numViews, snapid);
    numViews = COUNT_ARRAY_SIZE(sharedViews);
    SnapshotNameSpace::InsertOneTableData(sharedViews, numViews, snapid);
    numViews = COUNT_ARRAY_SIZE(lastStatViews);
    SnapshotNameSpace::InsertOneTableData(lastStatViews, numViews, snapid);
    numViews = COUNT_ARRAY_SIZE(lastDbRelatedViews);
    SnapshotNameSpace::InsertOneDbTables(lastDbRelatedViews, numViews, snapid);
}

static void DeleteStatTableDate(uint64 curr_min_snapid)
{
    StringInfoData query;
    initStringInfo(&query);

    appendStringInfo(&query, "delete from snapshot.snapshot where snapshot_id = %lu", curr_min_snapid);
    if (!SnapshotNameSpace::ExecuteQuery(query.data, SPI_OK_DELETE)) {
        ereport(ERROR, (errmodule(MOD_WDR_SNAPSHOT), errcode(ERRCODE_DATA_EXCEPTION),
            errmsg("clean snapshot id %lu is failed in snapshot table", curr_min_snapid),
            errdetail("query(%s) execute failed", query.data),
            errcause("System error."),
            erraction("Check whether the snapshot retry is successful"
                " and check whether the query can be executed")));
    }

    resetStringInfo(&query);
    appendStringInfo(&query, "delete from snapshot.tables_snap_timestamp where snapshot_id = %lu", curr_min_snapid);
    if (!SnapshotNameSpace::ExecuteQuery(query.data, SPI_OK_DELETE)) {
        ereport(ERROR, (errmodule(MOD_WDR_SNAPSHOT), errcode(ERRCODE_DATA_EXCEPTION),
            errmsg("clean snapshot failed"),
            errdetail("query(%s) execute failed", query.data),
            errcause("System error."),
            erraction("Check whether the snapshot retry is successful")));
    }
    pfree_ext(query.data);
}

/*
 * Called when the tablespace exceeds the set value
 * Delete the smallest case of snapid in the table
 */
void SnapshotNameSpace::CleanSnapshot(uint64 curr_min_snapid)
{
    DeleteStatTableDate(curr_min_snapid);
    int numViews = COUNT_ARRAY_SIZE(sharedViews);
    SnapshotNameSpace::DeleteTablesData(sharedViews, numViews, curr_min_snapid);
    numViews = COUNT_ARRAY_SIZE(dbRelatedViews);
    SnapshotNameSpace::DeleteTablesData(dbRelatedViews, numViews, curr_min_snapid);
    numViews = COUNT_ARRAY_SIZE(lastDbRelatedViews);
    SnapshotNameSpace::DeleteTablesData(lastDbRelatedViews, numViews, curr_min_snapid);
    numViews = COUNT_ARRAY_SIZE(lastStatViews);
    SnapshotNameSpace::DeleteTablesData(lastStatViews, numViews, curr_min_snapid);
    ereport(LOG, (errmsg("delete snapshot where snapshot_id = " UINT64_FORMAT, curr_min_snapid)));
}
/*
 * snapshot_start
 *
 *    Called from postmaster at startup or after an existing collector
 *    died.  Attempt to fire up a fresh statistics collector.
 *
 *    Returns PID of child process, or 0 if fail.
 *
 *    Note: if fail, we will be called again from the postmaster main loop.
 */
ThreadId snapshot_start(void)
{
    time_t curtime;

    /*
     * Do nothing if too soon since last collector start.  This is a safety
     * valve to protect against continuous respawn attempts if the collector
     * is dying immediately at launch.    Note that since we will be re-called
     * from the postmaster main loop, we will get another chance later.
     */
    curtime = time(NULL);
    if ((unsigned int)(curtime - t_thrd.perf_snap_cxt.last_snapshot_start_time) < (unsigned int)PGSTAT_RESTART_INTERVAL) {
        return 0;
    }
    t_thrd.perf_snap_cxt.last_snapshot_start_time = curtime;

    return initialize_util_thread(SNAPSHOT_WORKER);
}

void JobSnapshotIAm(void)
{
    t_thrd.role = SNAPSHOT_WORKER;
}

/*
 * called in  initpostgres() function
 */
bool IsJobSnapshotProcess(void)
{
    return t_thrd.role == SNAPSHOT_WORKER;
}

static void CreateStatTable(const char* query, const char* tablename)
{
    Datum colval;
    bool isnull = false;
    StringInfoData sql;
    initStringInfo(&sql);

    /* if the table is not created ,we will create it */
    appendStringInfo(
        &sql, "select pg_catalog.count(*) from pg_tables where tablename = '%s' and schemaname = 'snapshot'",
        tablename);
    colval = GetDatumValue(sql.data, 0, 0, &isnull);
    if (!DatumGetInt32(colval)) {
        if (!SnapshotNameSpace::ExecuteQuery(query, SPI_OK_UTILITY)) {
            ereport(ERROR, (errmodule(MOD_WDR_SNAPSHOT), errcode(ERRCODE_DATA_EXCEPTION),
                errmsg("can not create snapshot stat table"),
                errdetail("query(%s) execute failed", sql.data),
                errcause("System error."),
                erraction("Check whether the query can be executed")));
        }
    }
    pfree_ext(sql.data);
}

void SnapshotNameSpace::CreateSnapStatTables(void)
{
    StringInfoData createTs;
    StringInfoData createSnapshot;
    const char* tablename1 = "tables_snap_timestamp";
    const char* tablename2 = "snapshot";

    initStringInfo(&createTs);
    appendStringInfo(
        &createTs, "create table snapshot.tables_snap_timestamp(snapshot_id bigint not null, db_name text, "
        "tablename text, start_ts timestamp with time zone, end_ts timestamp with time zone)");
    
    initStringInfo(&createSnapshot);
    appendStringInfo(
        &createSnapshot, "create table snapshot.snapshot(snapshot_id bigint not null, "
        "start_ts timestamp with time zone, end_ts timestamp with time zone, primary key (snapshot_id))");

    // only allow create segment storage table when enable dss
    if (ENABLE_DSS) {
        appendStringInfo(&createTs, " with (segment = on)");
        appendStringInfo(&createSnapshot, " with (segment = on)");
    }

    CreateStatTable(createTs.data, tablename1);
    CreateStatTable(createSnapshot.data, tablename2);
}

static void DropIndexes(const char* indexName)
{
    StringInfoData query;
    initStringInfo(&query);
    appendStringInfo(&query, "drop index IF EXISTS snapshot.%s", indexName);
    if (!SnapshotNameSpace::ExecuteQuery(query.data, SPI_OK_UTILITY)) {
        pfree_ext(query.data);
        ereport(ERROR, (errmodule(MOD_WDR_SNAPSHOT), errcode(ERRCODE_DATA_EXCEPTION),
            errmsg("create index failed"), errdetail("drop index snapshot.%s execute error", indexName),
            errcause("System error."), erraction("Check whether the query can be executed")));
    }
    pfree_ext(query.data);
}

void SnapshotNameSpace::InitTables()
{
    SnapshotNameSpace::CreateSnapStatTables();
    SnapshotNameSpace::CreateSequence();
    int numViews = COUNT_ARRAY_SIZE(sharedViews);
    SnapshotNameSpace::CreateTable(sharedViews, numViews, true);
    numViews = COUNT_ARRAY_SIZE(dbRelatedViews);
    SnapshotNameSpace::CreateTable(dbRelatedViews, numViews, false);
    numViews = COUNT_ARRAY_SIZE(lastDbRelatedViews);
    SnapshotNameSpace::CreateTable(lastDbRelatedViews, numViews, false);
    numViews = COUNT_ARRAY_SIZE(lastStatViews);
    SnapshotNameSpace::CreateTable(lastStatViews, numViews, true);
    DropIndexes("snap_summary_statio_indexes_name");
    DropIndexes("snap_summary_statio_tables_name");
    DropIndexes("snap_summary_stat_indexes_name");
    DropIndexes("snap_class_info_name");
#if ((defined(ENABLE_MULTIPLE_NODES)) || (defined(ENABLE_PRIVATEGAUSS)))
    SnapshotView();
#endif
}

void SnapshotNameSpace::CreateTable(const char** views, int numViews, bool isSharedViews)
{
    bool isnull = false;
    StringInfoData query;
    initStringInfo(&query);
    for (int i = 0; i < numViews; i++) {
        resetStringInfo(&query);
        /* if the table is not created, we will create it */
        appendStringInfo(&query,
            "select pg_catalog.count(*) from pg_tables where tablename = 'snap_%s' "
            "and schemaname = 'snapshot'",
            views[i]);
        Datum colval = GetDatumValue(query.data, 0, 0, &isnull);
        if (!DatumGetInt32(colval)) {
            resetStringInfo(&query);
            /* Get the attributes of a table's columns */
            char* snapColAttrType = SnapshotNameSpace::GetTableColAttr(views[i], false, true);
            if (isSharedViews) {
                appendStringInfo(
                    &query, "create table snapshot.snap_%s(snapshot_id bigint, %s)", views[i], snapColAttrType);
            } else {
                appendStringInfo(&query,
                    "create table snapshot.snap_%s(snapshot_id bigint, db_name text, %s)",
                    views[i],
                    snapColAttrType);
            }
            /* only allow create segment storage table when enable dss */
            if (ENABLE_DSS) {
                appendStringInfo(&query, " with (segment = on)");
            }
            if (!SnapshotNameSpace::ExecuteQuery(query.data, SPI_OK_UTILITY)) {
                ereport(ERROR, (errmodule(MOD_WDR_SNAPSHOT), errcode(ERRCODE_DATA_EXCEPTION),
                    errmsg("create WDR snapshot data table failed"),
                    errdetail("query(%s) execute error", query.data),
                    errcause("System error."),
                    erraction("Check whether the query can be executed")));
            }
            pfree(snapColAttrType);
        }
        /* create index on snapshot table */
        SnapshotNameSpace::CreateIndexes(views[i]);
    }
    pfree_ext(query.data);
}
char* SnapshotNameSpace::GetTableColAttr(const char* viewName, bool onlyViewCol, bool addType)
{
    List* stringtables = NIL;
    StringInfoData sql;
    initStringInfo(&sql);
    StringInfoData colType;
    initStringInfo(&colType);
    /* Get the attributes of a view's columns under the dbe_perf schema */
    appendStringInfo(&sql,
        "SELECT a.attname AS field,t.typname AS type FROM "
        "pg_class c,pg_attribute a,pg_type t, pg_namespace n"
        " WHERE n.nspname = 'dbe_perf' and c.relnamespace = n.oid and c.relname = '%s' "
        "and a.attnum > 0 and a.attrelid = c.oid and a.atttypid = t.oid ORDER BY a.attnum;",
        viewName);
    SnapshotNameSpace::GetQueryData(sql.data, false, &stringtables);
    foreach_cell(cell, stringtables)
    {
        ListCell* row = ((List*)lfirst(cell))->head;
        if (onlyViewCol) {
            appendStringInfo(&colType, "\"%s\" ", (char*)lfirst(row));
        } else {
            appendStringInfo(&colType, "\"snap_%s\" ", (char*)lfirst(row));
        }
        if (addType) {
            appendStringInfo(&colType, "%s", (char*)lfirst(row->next));
        }
        if (cell->next != NULL)
            appendStringInfo(&colType, ", ");
    }
    pfree_ext(sql.data);
    DeepListFree(stringtables, true);
    return colType.data;
}
void SnapshotNameSpace::GetQueryStr(StringInfoData& query, const char* viewname, uint64 curr_snapid, const char* dbname)
{
    char* snapColAttr = SnapshotNameSpace::GetTableColAttr(viewname, false, false);
    char* snapColAttrType = SnapshotNameSpace::GetTableColAttr(viewname, false, true);
    appendStringInfo(&query,
        "insert into snapshot.snap_%s(snapshot_id, db_name, %s) select snapshot_id, dbname1, %s from"
        " pg_catalog.wdr_xdb_query('dbname=%s'::text, 'select %lu, ''%s'', t.* from dbe_perf.%s t'::text)"
        " as i(snapshot_id int8, dbname1 text, %s)",
        viewname,
        snapColAttr,
        snapColAttr,
        dbname,
        curr_snapid,
        dbname,
        viewname,
        snapColAttrType);
    pfree_ext(snapColAttr);
    pfree(snapColAttrType);
}
void SnapshotNameSpace::GetDatabaseList(List** databaselist)
{
    StringInfoData query;

    initStringInfo(&query);

    appendStringInfo(&query, "select datname from pg_database where datistemplate = 'f'");
    SnapshotNameSpace::GetQueryData(query.data, false, databaselist);
    pfree_ext(query.data);
}

/*
 * InsertOneDbTables
 *        for exemple pg_stat_xxx and pg_statio_xxx tables relation with pg_class
 */
void SnapshotNameSpace::InsertOneDbTables(const char** views, int numViews, uint64 curr_snapid)
{
    List* databaseList = NIL;
    SnapshotNameSpace::GetDatabaseList(&databaseList);
    foreach_cell(cell, databaseList)
    {
        List* row = (List*)lfirst(cell);
        SnapshotNameSpace::InsertDatabaseData((char*)linitial(row), curr_snapid, views, numViews);
    }
    DeepListFree(databaseList, true);
}

/*
 * SpiltString
 *     @str :string will be split
 *    @delim :Splitter
 *    @res :splited strings
 */
void SnapshotNameSpace::SplitString(const char* str, const char* delim, List** res)
{
    errno_t rc;
    char* next_token = NULL;
    char* token = NULL;

    if (str == NULL) {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("spilt str can not null")));
    }
    char* strs = (char*)palloc(strlen(str) + 1);
    rc = strncpy_s(strs, strlen(str) + 1, str, strlen(str));
    securec_check_c(rc, "\0", "\0");

    token = strtok_s(strs, delim, &next_token);
    while (token != NULL) {
        size_t strSize = strlen(token) + 1;
        char* subStr = (char*)palloc(strSize * sizeof(char));
        rc = strncpy_s(subStr, strSize, token, strSize - 1);
        securec_check_c(rc, "\0", "\0");
        *res = lappend(*res, subStr);
        token = strtok_s(NULL, delim, &next_token);
    }
    pfree(strs);
}

/*
 * GetDatabaseData
 * get other database data for snapshot
 */
void SnapshotNameSpace::InsertDatabaseData(const char* dbname, uint64 curr_snapid, const char** views, int numViews)
{
    StringInfoData query;
    initStringInfo(&query);
    StringInfoData sql;
    initStringInfo(&sql);
    for (int i = 0; i < numViews; i++) {
        SnapshotNameSpace::GetQueryStr(query, views[i], curr_snapid, dbname);
        resetStringInfo(&sql);
        CHECK_FOR_INTERRUPTS();
        appendStringInfo(&sql, "INSERT INTO  snapshot.tables_snap_timestamp"
            "(snapshot_id, db_name, tablename, start_ts) "
            "values(%lu, '%s', 'snap_%s', '%s')",
            curr_snapid, dbname, views[i], GetCurrentTimeStampString());
        if (!SnapshotNameSpace::ExecuteQuery(sql.data, SPI_OK_INSERT)) {
            ereport(ERROR, (errmodule(MOD_WDR_SNAPSHOT), errcode(ERRCODE_DATA_EXCEPTION),
                errmsg("insert into tables_snap_timestamp start time stamp failed"),
                errdetail("query(%s) execute failed", sql.data), errcause("System error."),
                erraction("Check whether the query can be executed")));
        }

        CHECK_FOR_INTERRUPTS();
        if (!SnapshotNameSpace::ExecuteQuery(query.data, SPI_OK_INSERT)) {
            ereport(ERROR, (errmodule(MOD_WDR_SNAPSHOT), errcode(ERRCODE_DATA_EXCEPTION),
                errmsg("insert into snap_%s is failed", views[i]),
                errdetail("query(%s) execute failed", query.data), errcause("System error."),
                erraction("Check whether the query can be executed")));
        }

        CHECK_FOR_INTERRUPTS();
        resetStringInfo(&sql);
        appendStringInfo(&sql,
            "update snapshot.tables_snap_timestamp set end_ts = '%s' "
            "where snapshot_id = %lu and db_name = '%s' and tablename = 'snap_%s'",
            GetCurrentTimeStampString(), curr_snapid, dbname, views[i]);
        if (!SnapshotNameSpace::ExecuteQuery(sql.data, SPI_OK_UPDATE)) {
            ereport(ERROR, (errmodule(MOD_WDR_SNAPSHOT), errcode(ERRCODE_DATA_EXCEPTION),
                errmsg("update tables_snap_timestamp end time stamp failed"),
                errdetail("query(%s) execute failed", sql.data),
                errcause("System error."),
                erraction("Check whether the query can be executed")));
        }

        resetStringInfo(&query);
    }
    pfree_ext(query.data);
    pfree_ext(sql.data);
}

/*
 In order to accelerate query for awr report, the index of some tables need to create
 The index is created immediately after whose table has existed at the start phase
*/
void SnapshotNameSpace::CreateIndexes(const char* views)
{
    bool isnull = false;
    StringInfoData query;
    initStringInfo(&query);
    appendStringInfo(&query,
        "select pg_catalog.count(*) from pg_indexes where schemaname = 'snapshot' and "
        "tablename = 'snap_%s' and indexname = 'snap_%s_idx'",
        views, views);
    Datum indexNum = GetDatumValue(query.data, 0, 0, &isnull);
    if (!DatumGetInt32(indexNum)) {
        resetStringInfo(&query);
        appendStringInfo(&query, "create index snapshot.snap_%s_idx on snapshot.snap_%s(snapshot_id)",
            views, views);
        if (!SnapshotNameSpace::ExecuteQuery(query.data, SPI_OK_UTILITY)) {
            pfree_ext(query.data);
            ereport(ERROR, (errmodule(MOD_WDR_SNAPSHOT), errcode(ERRCODE_DATA_EXCEPTION),
                errmsg("create WDR snapshot index failed"),
                errdetail("create index snapshot.snap_%s_idx execute error", views),
                errcause("System error."),
                erraction("Check whether the query can be executed")));
        }
    }
    pfree_ext(query.data);
}

void SnapshotNameSpace::SleepCheckInterrupt(uint32 minutes)
{
    const int ONE_SECOND = 1000000;
    const uint32 ONE_MINUTE = 60; /* 60s */

    uint32 waitCycle = ONE_MINUTE * minutes;
    for (uint32 i = 0; i < waitCycle; i++) {
        if (t_thrd.perf_snap_cxt.need_exit) {
            break;
        }
        pg_usleep(ONE_SECOND);
    }
}
static void SleepToNextTS(TimestampTz nextTimeStamp)
{
    const int ONE_SECOND = 1000000;

    /* exit,request,got_SIGHUP early exit from sleep */
    while (GetCurrentTimestamp() < nextTimeStamp) {
        if (t_thrd.perf_snap_cxt.need_exit ||
            t_thrd.perf_snap_cxt.request_snapshot ||
            t_thrd.perf_snap_cxt.got_SIGHUP) {
            break;
        }
        pg_usleep(ONE_SECOND);
    }
}

static TimestampTz GetNextSnapshotTS(TimestampTz lastSnapTS)
{
    TimestampTz nextSnapTS = lastSnapTS;

    /* a snapshot has token on nextSnapTS,we need get nextSnapTS */
    while (GetCurrentTimestamp() >= nextSnapTS) {
        nextSnapTS += u_sess->attr.attr_common.wdr_snapshot_interval * USECS_PER_MINUTE;
    }
    return nextSnapTS;
}

static void ProcessSignal(void)
{
    /*
     * Ignore all signals usually bound to some action in the postmaster,
     * except SIGHUP, SIGTERM and SIGQUIT.
     */
    (void)gspqsignal(SIGHUP, instr_snapshot_sighup_handler);
    (void)gspqsignal(SIGINT, request_snapshot);
    (void)gspqsignal(SIGTERM, instr_snapshot_exit); /* cancel current query and exit */
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
    if (u_sess->proc_cxt.MyProcPort->remote_host) {
        pfree(u_sess->proc_cxt.MyProcPort->remote_host);
    }
    u_sess->proc_cxt.MyProcPort->remote_host = pstrdup("localhost");

    t_thrd.wlm_cxt.thread_node_group = &g_instance.wlm_cxt->MyDefaultNodeGroup;  // initialize the default value
    t_thrd.wlm_cxt.thread_climgr = &t_thrd.wlm_cxt.thread_node_group->climgr;
    t_thrd.wlm_cxt.thread_srvmgr = &t_thrd.wlm_cxt.thread_node_group->srvmgr;
}

/*
 * The port info is initial status in snapshot thread when select
 * the config_setting view. The getsockopt is performed when the following
 * parameters are 0. The sock is -1 which make getsockopt failed and
 * unexpected log is printed. In order to avoid this, default values are set to -1.
 */
void SnapshotNameSpace::ResetPortDefaultKeepalivesValue(void)
{
    Port* port = u_sess->proc_cxt.MyProcPort;
    if (port->default_keepalives_idle == 0) {
        port->default_keepalives_idle = -1;
    }
    if (port->default_keepalives_interval == 0) {
        port->default_keepalives_interval = -1;
    }
    if (port->default_keepalives_count == 0) {
        port->default_keepalives_count = -1;
    }
}

void SnapshotNameSpace::SetThrdCxt(void)
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
    t_thrd.perf_snap_cxt.PerfSnapMemCxt = AllocSetContextCreate(TopMemoryContext,
        "SnapshotContext",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);
    /*
     * Create a resource owner to keep track of our resources (currently only
     * buffer pins).
     */
    t_thrd.utils_cxt.CurrentResourceOwner = ResourceOwnerCreate(NULL, "Snapshot",
        THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_DFX));
}
static void SetMyproc()
{
    /* reset MyProcPid */
    t_thrd.proc_cxt.MyProcPid = gs_thread_self();
    /* record Start Time for logging */
    t_thrd.proc_cxt.MyStartTime = time(NULL);

    t_thrd.proc_cxt.MyProgName = "WDRSnapshot";
    u_sess->attr.attr_common.application_name = pstrdup("WDRSnapshot");
}
/*
 * This is the main function of the background thread
 * A node set to ccn by cm will call the SubSnapshotMain
 * function. Loop collection of snapshot information
 */
NON_EXEC_STATIC void SnapshotMain()
{
    ereport(LOG, (errmsg("snapshot thread is started")));
    pg_atomic_add_fetch_u32(&g_instance.stat_cxt.snapshot_thread_counter, 1);

    const int INTERVAL = 20000;  // 20 seconds
    char username[NAMEDATALEN] = {'\0'};

    /* we are a postmaster subprocess now */
    IsUnderPostmaster = true;
    JobSnapshotIAm();
    SetMyproc();
    if (IS_PGXC_COORDINATOR && IsPostmasterEnvironment) {
        on_shmem_exit(PGXCNodeCleanAndRelease, 0);
    }
    ProcessSignal();
    /* Early initialization */
    BaseInit();
#ifndef EXEC_BACKEND
    InitProcess();
#endif

    t_thrd.proc_cxt.PostInit->SetDatabaseAndUser((char*)pstrdup(DEFAULT_DATABASE), InvalidOid, username);
    t_thrd.proc_cxt.PostInit->InitSnapshotWorker();

    /* initialize funtion such as max(), min() */
    SetProcessingMode(NormalProcessing);
    /* Identify myself via ps */
    init_ps_display("WDR snapshot process", "", "", "");

    SnapshotNameSpace::SetThrdCxt();
    u_sess->proc_cxt.MyProcPort->SessionStartTime = GetCurrentTimestamp();
    SnapshotNameSpace::ResetPortDefaultKeepalivesValue();
    Reset_Pseudo_CurrentUserId();
    /* initialize current pool handles, it's also only once */
    exec_init_poolhandles();
    /* initialize function of sql such as now() */
    InitVecFuncMap();
#ifndef ENABLE_MULTIPLE_NODES
    /* forbid smp in snapshot thread */
    AutoDopControl dopControl;
    dopControl.CloseSmp();
#endif
    pgstat_bestart();
    pgstat_report_appname("WDRSnapshot");
    pgstat_report_activity(STATE_IDLE, NULL);
    while (!t_thrd.perf_snap_cxt.need_exit) {
        ReloadInfo();
        /*
         * First sleep and then judge CCN, mainly considering that
         * the wlm thread has not completed initialization
         * when the snapshot thread starts at startup.
         */
        pg_usleep(INTERVAL);
        if ((PgxcIsCentralCoordinator(g_instance.attr.attr_common.PGXCNodeName) || IS_SINGLE_NODE) &&
            u_sess->attr.attr_common.enable_wdr_snapshot && !SS_IN_REFORM) {
            /* to avoid dead lock with redis, disable snapshot during redistribution */
            check_snapshot_thd_exit();
            SnapshotNameSpace::SubSnapshotMain();
        }
        pgstat_report_activity(STATE_IDLE, NULL);
    }
    gs_thread_exit(0);
}

/*
 * To avoid generate below message,
 * "Do analyze for them in order to generate optimized plan",
 * request analyze from snapshot thread.
 */
static void analyze_snap_table()
{
    List* analyzeTableList = NULL;
    int rc = 0;
    start_xact_command();
    PushActiveSnapshot(GetTransactionSnapshot());
    SPI_STACK_LOG("connect", NULL, NULL);
    if ((rc = SPI_connect()) != SPI_OK_CONNECT) {
        ereport(ERROR, (errmodule(MOD_WDR_SNAPSHOT), errcode(ERRCODE_INTERNAL_ERROR),
            errmsg("analyze table, connection failed: %s", SPI_result_code_string(rc)),
            errdetail("SPI_connect failed in function analyze_snap_table"),
            errcause("System error."),
            erraction("Check whether the snapshot retry is successful")));
    }
    SnapshotNameSpace::GetAnalyzeList(&(analyzeTableList));
    set_lock_timeout();
    SnapshotNameSpace::AnalyzeTable(analyzeTableList);
    SPI_STACK_LOG("finish", NULL, NULL);
    SPI_finish();
    PopActiveSnapshot();
    finish_xact_command();
}
void InitSnapshot()
{
    /* Check if global_memory_node_detail view can do snapshot */
    t_thrd.perf_snap_cxt.is_mem_protect = CheckMemProtectInit();
    /* All the tables list will be initialized once. when database is restarted or powered on */
    start_xact_command();
    PushActiveSnapshot(GetTransactionSnapshot());
    int rc = 0;
    /* connect SPI to execute query */
    SPI_STACK_LOG("connect", NULL, NULL);
    if ((rc = SPI_connect()) != SPI_OK_CONNECT) {
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
            errmsg("snapshot thread SPI_connect failed: %s", SPI_result_code_string(rc)),
            errdetail("SPI_connect failed in function analyze_snap_table"),
            errcause("System error."),
            erraction("Check whether the snapshot retry is successful")));
    }
    set_lock_timeout();
    SnapshotNameSpace::InitTables();
    SPI_STACK_LOG("finish", NULL, NULL);
    SPI_finish();
    PopActiveSnapshot();
    finish_xact_command();
    ereport(LOG, (errcode(ERRCODE_SUCCESSFUL_COMPLETION), errmsg("create snapshot tables succeed")));
    analyze_snap_table();
}
/*
 * Background thread for loop collection snapshot
 */
void SnapshotNameSpace::SubSnapshotMain(void)
{
    pgstat_report_activity(STATE_RUNNING, NULL);
    TimestampTz last_auto_time = GetCurrentTimestamp();
    TimestampTz next_auto_time = last_auto_time;
    const int SLEEP_GAP_AFTER_ERROR = 1;
    InitSnapshot();
    u_sess->attr.attr_common.ExitOnAnyError = false;

    while (!t_thrd.perf_snap_cxt.need_exit &&
           (PgxcIsCentralCoordinator(g_instance.attr.attr_common.PGXCNodeName) || IS_SINGLE_NODE) &&
           u_sess->attr.attr_common.enable_wdr_snapshot) {
        /* 1. to avoid dead lock with redis, disable snapshot during redistribution
           2. to avoid insert failed, disable snapshot during upgrade */
        check_snapshot_thd_exit();

        ReloadInfo();
        PG_TRY();
        {
            /* create snapshot if user_request or arriver at next_auto time,
             * be careful not mess the auto interval snapshot-creating schedule */
            if (t_thrd.perf_snap_cxt.request_snapshot || GetCurrentTimestamp() >= next_auto_time) {
                last_auto_time = t_thrd.perf_snap_cxt.request_snapshot ? last_auto_time : GetCurrentTimestamp();
                pgstat_report_activity(STATE_RUNNING, NULL);
                ereport(LOG, (errcode(ERRCODE_SUCCESSFUL_COMPLETION), errmsg("WDR snapshot start")));
                start_xact_command();
                PushActiveSnapshot(GetTransactionSnapshot());
                SnapshotNameSpace::take_snapshot();
                PopActiveSnapshot();
                finish_xact_command();
                if (OidIsValid(u_sess->proc_cxt.MyDatabaseId)) {
                    pgstat_report_stat(true);
                }
                ereport(LOG, (errcode(ERRCODE_SUCCESSFUL_COMPLETION), errmsg("WDR snapshot end")));
                t_thrd.perf_snap_cxt.request_snapshot = false;
                pgstat_report_activity(STATE_IDLE, NULL);
            }
            /* next time to create snapshot */
            next_auto_time = GetNextSnapshotTS(last_auto_time);
            /* Sleep until next time */
            SleepToNextTS(next_auto_time);
        }
        PG_CATCH();
        {
            EmitErrorReport();
            FlushErrorState();
            ereport(WARNING, (errcode(ERRCODE_WARNING), errmsg("WDR snapshot failed")));
            PopActiveSnapshot();
            AbortCurrentTransaction();
            t_thrd.postgres_cxt.xact_started = false;
            pgstat_report_activity(STATE_IDLE, NULL);
            t_thrd.perf_snap_cxt.request_snapshot = false;
            SnapshotNameSpace::SleepCheckInterrupt(SLEEP_GAP_AFTER_ERROR);
        }
        PG_END_TRY();
    }
}
