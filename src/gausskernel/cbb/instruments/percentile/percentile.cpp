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
 * percentile.cpp
 *
 *    Automatically collect MPP percentiles in the background,
 * you can also manually collect percentiles
 *
 * IDENTIFICATION
 *	  src/gausskernel/cbb/instruments/instrmention/utils/percentile.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "iostream"
#include "postgres.h"
#include "knl/knl_variable.h"
#include "access/xact.h"
#include "postmaster/postmaster.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"
#include "storage/proc.h"
#include "catalog/pg_database.h"
#include "gssignal/gs_signal.h"
#include "pgxc/pgxc.h"
#include "pgxc/pgxcnode.h"
#include "utils/ps_status.h"
#include "utils/builtins.h"
#include "pgxc/locator.h"
#include "libpq/pqformat.h"
#include "pgstat.h"
#include "libpq/pqsignal.h"
#include "funcapi.h"
#include "storage/ipc.h"
#include "pgxc/poolutils.h"
#include "instruments/percentile.h"
#include "utils/postinit.h"

extern void destroy_handles();
const int SLEEP_INTERVAL = 10;
namespace PercentileSpace {
bool pgstat_fetch_sql_rt_info(PGXCNodeAllHandles* pgxcHandle, char tag, int* Count, SqlRTInfo* sqlRT);
List* pgstat_send_command(PGXCNodeAllHandles* pgxc_handles, char tag, bool* isSendSuccess);
void SubPercentileMain(void);
bool SetTimer(TimestampTz start, TimestampTz stop);
bool ResetTimer(int interval);
int64 calculate_percentile(SqlRTInfo* sql_rt_info, int counter, int percentile);
void CalculatePercentile(SqlRTInfo* sqlRT, int counter);
void adjust(SqlRTInfo* sqlRT, int len, int index);
void heapSort(SqlRTInfo* sqlRT, int size);
void init_gspqsignal();
void init_MemCxt();
unsigned int process_remote_count_msg(const char* msg, int len);
void process_remote_record_msg(const char* msg, int len, int index, SqlRTInfo* sqlRT, int64 Count, bool* isWrongMsg);
bool CheckQueryPercentile(void);
void calculatePercentileOfSingleNode(void);
void calculatePercentileOfMultiNode(void);
bool is_enable_percentile_thread(void);
}  // namespace PercentileSpace
using namespace PercentileSpace;
using namespace std;

/* SIGQUIT signal handler for percentile process */
static void instr_percentile_exit(SIGNAL_ARGS)
{
    t_thrd.percentile_cxt.need_exit = true;
}

/* SIGHUP handler for percentile process */
static void instr_percentile_sighup_handler(SIGNAL_ARGS)
{
    t_thrd.percentile_cxt.got_SIGHUP = true;
}

static void pgstat_alarm_handler(SIGNAL_ARGS)
{
    g_instance.stat_cxt.force_process = true;
}

void JobPercentileIAm(void)
{
    t_thrd.role = PERCENTILE_WORKER;
}
/*
 * called in  initpostgres() function
 */
bool IsJobPercentileProcess(void)
{
    return t_thrd.role == PERCENTILE_WORKER;
}

void InitPercentile(void)
{
    /* init memory context */
    g_instance.stat_cxt.InstrPercentileContext = AllocSetContextCreate(g_instance.instance_context,
        "PercentileContext",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE,
        SHARED_CONTEXT);

    if (g_instance.stat_cxt.sql_rt_info_array == NULL) {
        g_instance.stat_cxt.sql_rt_info_array =
            (SqlRTInfoArray *)MemoryContextAllocZero(
            g_instance.stat_cxt.InstrPercentileContext, sizeof(SqlRTInfoArray));
    }
}

void PercentileSpace::init_gspqsignal()
{
    /*
     * Ignore all signals usually bound to some action in the postmaster,
     * except SIGHUP and SIGQUIT.
     */
    (void)gspqsignal(SIGHUP, instr_percentile_sighup_handler);
    (void)gspqsignal(SIGURG, print_stack);
    (void)gspqsignal(SIGINT, SIG_IGN);
    (void)gspqsignal(SIGTERM, instr_percentile_exit);
    (void)gspqsignal(SIGQUIT, quickdie);
    (void)gspqsignal(SIGALRM, pgstat_alarm_handler);
    (void)gspqsignal(SIGPIPE, SIG_IGN);
    (void)gspqsignal(SIGUSR1, procsignal_sigusr1_handler);
    (void)gspqsignal(SIGUSR2, SIG_IGN);
    (void)gspqsignal(SIGCHLD, SIG_DFL);
    (void)gspqsignal(SIGTTIN, SIG_DFL);
    (void)gspqsignal(SIGTTOU, SIG_DFL);
    (void)gspqsignal(SIGCONT, SIG_DFL);
    (void)gspqsignal(SIGWINCH, SIG_DFL);

    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();
}

void PercentileSpace::init_MemCxt()
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
/*
 * This is the main function of the background thread
 * A node set to ccn by cm will call the SubpercentileMain
 * function. Loop collection of percentile information
 */
NON_EXEC_STATIC void PercentileMain()
{
    char username[NAMEDATALEN];

    /* we are a postmaster subprocess now */
    IsUnderPostmaster = true;
    JobPercentileIAm();

    /* reset MyProcPid */
    t_thrd.proc_cxt.MyProcPid = gs_thread_self();
    /* record Start Time for logging */
    t_thrd.proc_cxt.MyStartTime = time(NULL);
    t_thrd.proc_cxt.MyProgName = "getpercentile";
    if (u_sess->proc_cxt.MyProcPort->remote_host) {
        pfree(u_sess->proc_cxt.MyProcPort->remote_host);
    }
    u_sess->proc_cxt.MyProcPort->remote_host = pstrdup("localhost");
    u_sess->attr.attr_common.application_name = pstrdup("PercentileJob");
    /* Identify myself via ps */
    init_ps_display("instrumention percentile process", "", "", "");
    SetProcessingMode(InitProcessing);
    PercentileSpace::init_gspqsignal();
    /* Early initialization */
    BaseInit();
#ifndef EXEC_BACKEND
    InitProcess();
#endif

    t_thrd.proc_cxt.PostInit->SetDatabaseAndUser((char*)pstrdup(DEFAULT_DATABASE), InvalidOid, username);
    t_thrd.proc_cxt.PostInit->InitPercentileWorker();

    SetProcessingMode(NormalProcessing);
    on_shmem_exit(PGXCNodeCleanAndRelease, 0);
    t_thrd.utils_cxt.CurrentResourceOwner = ResourceOwnerCreate(NULL, "Percentile",
        THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_DFX));
    /* initialize current pool handles, it's also only once */
    exec_init_poolhandles();

    PercentileSpace::init_MemCxt();
    elog(LOG, "instrumention percentile started");

    /* report this backend in the PgBackendStatus array */
    u_sess->proc_cxt.MyProcPort->SessionStartTime = GetCurrentTimestamp();
    pgstat_bestart();
    pgstat_report_appname("PercentileJob");
    pgstat_report_activity(STATE_IDLE, NULL);

    while (!t_thrd.percentile_cxt.need_exit) {
        if (IsGotPoolReload()) {
            processPoolerReload();
            ResetGotPoolReload(false);
        }
        if (t_thrd.percentile_cxt.got_SIGHUP) {
            t_thrd.percentile_cxt.got_SIGHUP = false;
            ProcessConfigFile(PGC_SIGHUP);
        }

        PercentileSpace::SubPercentileMain();
        pgstat_report_activity(STATE_IDLE, NULL);
        t_thrd.percentile_cxt.need_reset_timer = true;
        g_instance.stat_cxt.force_process = false;
        if (t_thrd.percentile_cxt.need_exit) break;
        sleep(SLEEP_INTERVAL);
    }
    elog(LOG, "instrumention percentile ended");
    gs_thread_exit(0);
}

bool PercentileSpace::SetTimer(TimestampTz start, TimestampTz stop)
{
    long secs;
    int usecs;
    struct itimerval timeval;

    TimestampDifference(start, stop, &secs, &usecs);

    /* If start time is equal to stop time, we set only 1 microsecond */
    if (secs == 0 && usecs == 0) {
        usecs = 1;
    }

    int ss_rc = memset_s(&timeval, sizeof(timeval), 0, sizeof(struct itimerval));
    securec_check(ss_rc, "\0", "\0");
    timeval.it_value.tv_sec = secs;
    timeval.it_value.tv_usec = usecs;

    if (gs_signal_settimer(&timeval)) {
        return false;
    }
    return true;
}

bool PercentileSpace::ResetTimer(int interval)
{
    if (!PgxcIsCentralCoordinator(g_instance.attr.attr_common.PGXCNodeName) &&
        !IS_SINGLE_NODE) {
        return false;
    }

    TimestampTz now = GetCurrentTimestamp();
    TimestampTz end = TimestampTzPlusMilliseconds(now,
        interval * 1000L);  // seconds

    /* reset the timer for the next time */
    return PercentileSpace::SetTimer(now, end);
}

bool PercentileSpace::is_enable_percentile_thread(void)
{
    if (t_thrd.percentile_cxt.need_exit) {
        return false;
    }

    if (IS_SINGLE_NODE) {
        return true;
    }

    if (PgxcIsCentralCoordinator(g_instance.attr.attr_common.PGXCNodeName)) {
        return true;
    }

    return false;
}

void PercentileSpace::calculatePercentileOfSingleNode(void)
{
    SqlRTInfo* sqlRT = NULL;

    if (!u_sess->attr.attr_common.enable_instr_rt_percentile)
        return;
    MemoryContext oldcxt = CurrentMemoryContext;
    PG_TRY();
    {
        int LocalCount = pgstat_fetch_sql_rt_info_counter();;
        sqlRT = (SqlRTInfo*)palloc0(LocalCount * sizeof(SqlRTInfo));
        pgstat_fetch_sql_rt_info_internal(sqlRT);
        PercentileSpace::heapSort(sqlRT, LocalCount);
        PercentileSpace::CalculatePercentile(sqlRT, LocalCount);
        pfree_ext(sqlRT);
    }
    PG_CATCH();
    {
        (void)MemoryContextSwitchTo(oldcxt);
        pfree_ext(sqlRT);
        LWLockReleaseAll();
        FlushErrorState();
        elog(WARNING, "Percentile job failed");
    }
    PG_END_TRY();
}

void PercentileSpace::calculatePercentileOfMultiNode(void)
{
    MemoryContext oldcxt = CurrentMemoryContext;
    PG_TRY();
    {
        processCalculatePercentile();
    }
    PG_CATCH();
    {
        (void)MemoryContextSwitchTo(oldcxt);
        /* free all handles */
        release_pgxc_handles(t_thrd.percentile_cxt.pgxc_all_handles);
        t_thrd.percentile_cxt.pgxc_all_handles = NULL;
        LWLockReleaseAll();
        FlushErrorState();
        elog(WARNING, "Percentile job failed");
    }
    PG_END_TRY();
}

/*
 * Background thread for loop collection percentile
 */
void PercentileSpace::SubPercentileMain(void)
{
    while (PercentileSpace::is_enable_percentile_thread()) {
        if (t_thrd.percentile_cxt.need_reset_timer) {
            if (!PercentileSpace::ResetTimer(u_sess->attr.attr_common.instr_rt_percentile_interval)) {
                elog(WARNING, "Percentile job reset timer failed");
            }
            pgstat_report_activity(STATE_RUNNING, NULL);
            t_thrd.percentile_cxt.need_reset_timer = false;
        }
        if (IsGotPoolReload()) {
            processPoolerReload();
            ResetGotPoolReload(false);
        }
        if (t_thrd.percentile_cxt.got_SIGHUP) {
            t_thrd.percentile_cxt.got_SIGHUP = false;
            ProcessConfigFile(PGC_SIGHUP);
        }
        if (g_instance.stat_cxt.force_process) {
            if (IS_SINGLE_NODE) {
                PercentileSpace::calculatePercentileOfSingleNode();
            } else {
                PercentileSpace::calculatePercentileOfMultiNode();
            }
            t_thrd.percentile_cxt.need_reset_timer = true;
            g_instance.stat_cxt.force_process = false;
        }
        pg_usleep(SLEEP_INTERVAL * 1000L);  // CCN check if need force process percentile
    }                               /* end of loop */
}

List* PercentileSpace::pgstat_send_command(PGXCNodeAllHandles* pgxc_handles, char tag, bool* isSendSuccess)
{
    int i;
    List* connlist = NULL;
    int cn_conn_count = pgxc_handles->co_conn_count;
    PGXCNodeHandle** pgxc_connections = pgxc_handles->coord_handles;
    for (i = 0; i < cn_conn_count; i++) {
        int stat = pgxc_node_pgstat_send(pgxc_connections[i], tag);
        if (stat == 0) {
            connlist = lappend_int(connlist, i);
        } else {
            *isSendSuccess = false;
        }
    }
    return connlist;
}

unsigned int PercentileSpace::process_remote_count_msg(const char* msg, int len)
{
    StringInfoData input_msg;
    unsigned int count;

    initStringInfo(&input_msg);
    appendBinaryStringInfo(&input_msg, msg, len);
    count = pq_getmsgint(&input_msg, sizeof(int));
    pq_getmsgend(&input_msg);
    pfree(input_msg.data);
    return count;
}

void PercentileSpace::process_remote_record_msg(const char* msg, int len, int index,
                                                SqlRTInfo* sqlRT, int64 Count, bool* isWrongMsg)
{
    if (sqlRT == NULL || index >= Count) {
        *isWrongMsg = true;
        return;
    }

    StringInfoData input_msg;
    initStringInfo(&input_msg);
    appendBinaryStringInfo(&input_msg, msg, len);
    int64 oid = (Oid)pq_getmsgint64(&input_msg);
    int64 rt = pq_getmsgint64(&input_msg);
    sqlRT[index].rt = rt;
    sqlRT[index].UniqueSQLId = oid;
    pq_getmsgend(&input_msg);
    pfree(input_msg.data);
}

bool PercentileSpace::pgstat_fetch_sql_rt_info(PGXCNodeAllHandles* pgxcHandle, char tag, int* Count, SqlRTInfo* sqlRT)
{
    struct timeval timeout = {120, 0};
    bool isSendSuccess = true;
    List* connlist = PercentileSpace::pgstat_send_command(pgxcHandle, tag, &isSendSuccess);
    int index = 0;
    bool isWrongMsg = false;
    bool isTimeOut = false;
    while (list_length(connlist) > 0) {
        PGXCNodeHandle* cn_handle = pgxcHandle->coord_handles[linitial_int(connlist)];
        connlist = list_delete_first(connlist);
        bool isFinished = false;
        for (;;) {
            if (pgxc_node_receive(1, &cn_handle, &timeout)) {
                ereport(LOG,
                    (errmsg("%s:%d recv fail", __FUNCTION__, __LINE__)));
                break;
            }

            char* msg = NULL;
            int len;

            char msg_type = get_message(cn_handle, &len, &msg);
            switch (msg_type) {
                case '\0': /* message is not completed */
                case 'A':  /* NotificationResponse */
                case 'S':  /* SetCommandComplete */
                    break;
                case 'E': /* message is error */
                    isWrongMsg = true;
                    break;
                case 'c': { /* sql rt info count */
                    unsigned int count = PercentileSpace::process_remote_count_msg(msg, len);
                    if (sqlRT == NULL && count <= MAX_SQL_RT_INFO_COUNT) {
                        *Count = *Count + count;
                        if ((*Count < 0) || (*Count > MAX_SQL_RT_INFO_COUNT_REMOTE)) {
                            isWrongMsg = true;
                        }
                    } else {
                        isWrongMsg = true;
                    }
                    break;
                }
                case 'r': { /* sql rt info */
                    PercentileSpace::process_remote_record_msg(msg, len, index, sqlRT, *Count, &isWrongMsg);
                    index++;
                    break;
                }
                case 'f': {
                    isFinished = true;
                    break;
                }
                default: {
                    isWrongMsg = true;
                    ereport(WARNING, (errmsg("[Percentile] unexpected message type: %c", msg_type)));
                    break;
                }
            }
            if (isFinished || isWrongMsg) {
                break;
            }
        }
        cn_handle->state = DN_CONNECTION_STATE_IDLE;
        if (!isFinished || isWrongMsg) {
            isTimeOut = true;
            break;
        }
    }
    if (!isSendSuccess) {
        isTimeOut = true;
    }
    return isTimeOut;
}

void processCalculatePercentile()
{
    if (!u_sess->attr.attr_common.enable_instr_rt_percentile) {
        return;
    }
    List* cnlist = NULL;
    int TotalCount = 0;
    int LocalCount;
    bool isTimeOut = false;
    /* get all data node index */
    cnlist = GetAllCoordNodes();
    MemoryContext oldcxt = CurrentMemoryContext;
    PG_TRY();
    {
        t_thrd.percentile_cxt.pgxc_all_handles = get_handles(NULL, cnlist, true);
    }
    PG_CATCH();
    {
        (void)MemoryContextSwitchTo(oldcxt);
        ereport(LOG, (errmodule(MOD_INSTR), errmsg("Percentile job - failed to get CN handler.")));
        release_pgxc_handles(t_thrd.percentile_cxt.pgxc_all_handles);
        t_thrd.percentile_cxt.pgxc_all_handles = NULL;
        list_free(cnlist);
        PG_RE_THROW();
    }
    PG_END_TRY();
    list_free(cnlist);

    if (t_thrd.percentile_cxt.pgxc_all_handles == NULL) {
        ereport(LOG, (errmodule(MOD_INSTR), errmsg("Percentile job - CN handler is null.")));
        return;
    }
    /* 1. fetch sql rt count from other cns */
    isTimeOut =
        PercentileSpace::pgstat_fetch_sql_rt_info(t_thrd.percentile_cxt.pgxc_all_handles, 'K', &TotalCount, NULL);

    if (!isTimeOut) {
        /* 2. fetch sql rt info count from local */
        LocalCount = pgstat_fetch_sql_rt_info_counter();
        SqlRTInfo* sqlRT = NULL;
        sqlRT = (SqlRTInfo*)palloc0((unsigned long)(TotalCount + LocalCount) * sizeof(SqlRTInfo));

        /* 3. fetch sql rt info from other cns */
        int sqlRTSize = TotalCount + LocalCount;
        isTimeOut =
            PercentileSpace::pgstat_fetch_sql_rt_info(t_thrd.percentile_cxt.pgxc_all_handles, 'k', &sqlRTSize, sqlRT);
        if (!isTimeOut) {
            /* 4. fetch sql rt info from local */
            pgstat_fetch_sql_rt_info_internal(sqlRT + TotalCount);

            /* 5 calculate percentile */
            PercentileSpace::heapSort(sqlRT, TotalCount + LocalCount);
            PercentileSpace::CalculatePercentile(sqlRT, TotalCount + LocalCount);
        }
        pfree(sqlRT);
    }

    /* 6. free all handles */
    release_pgxc_handles(t_thrd.percentile_cxt.pgxc_all_handles);
    t_thrd.percentile_cxt.pgxc_all_handles = NULL;
    if (isTimeOut) {
        /* if pgxc handles is time out, we should destroy handles, release occupied memory */
        ereport(LOG, (errmodule(MOD_INSTR), errmsg("Percentile job - get data timeout.")));
        destroy_handles();
    }
}

void PercentileSpace::adjust(SqlRTInfo* sqlRT, int len, int index)
{
    const int left = 2 * index + 1;   // heapSort get left node index
    const int right = 2 * index + 2;  // heapSort get right node index

    int maxIdx = index;
    if (left < len && sqlRT[left].rt > sqlRT[maxIdx].rt) {
        maxIdx = left;
    }
    if (right < len && sqlRT[right].rt > sqlRT[maxIdx].rt) {
        maxIdx = right;
    }

    if (maxIdx != index) {
        swap(sqlRT[maxIdx], sqlRT[index]);
        PercentileSpace::adjust(sqlRT, len, maxIdx);
    }
}

void PercentileSpace::heapSort(SqlRTInfo* sqlRT, int size)
{
    for (int i = size / 2 - 1; i >= 0; i--) {  // heapSort get last non leaf node index
        PercentileSpace::adjust(sqlRT, size, i);
    }

    for (int i = size - 1; i >= 1; i--) {
        swap(sqlRT[0], sqlRT[i]);
        PercentileSpace::adjust(sqlRT, i, 0);
    }
}

void PercentileSpace::CalculatePercentile(SqlRTInfo* sqlRT, int counter)
{
    char* percentile = NULL;
    List* percentilelist = NIL;
    ListCell* l = NULL;
    int i = 0;
    if (counter == 0) {
        /* there is no sql executed during last 10 seconds, so the percentile is 0 */
        for (int j = 0; j < NUM_PERCENTILE_COUNT; j++) {
            g_instance.stat_cxt.RTPERCENTILE[j] = 0;
        }
        return;
    }

    /* guc paramater percentile_values is reserved, only surport 80,95 now */
    percentile = pstrdup(u_sess->attr.attr_common.percentile_values);
    if (!SplitIdentifierInteger(percentile, ',', &percentilelist)) {
        /* syntax error in name list */
        /* this should not happen if GUC checked check_percentile */
        pfree_ext(percentile);
        list_free_ext(percentilelist);
        pfree_ext(sqlRT);
        ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NODE_STATE), errmsg("Invalid percentile syntax")));
    }

    if (list_length(percentilelist) > NUM_PERCENTILE_COUNT) {
        pfree_ext(percentile);
        list_free_ext(percentilelist);
        pfree_ext(sqlRT);
        ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NODE_STATE), errmsg("Too many percentile values")));
    }

    LWLockAcquire(PercentileLock, LW_EXCLUSIVE);
    foreach (l, percentilelist) {
        int pv = pg_atoi((char*)lfirst(l), sizeof(int), 0);
        g_instance.stat_cxt.RTPERCENTILE[i++] = PercentileSpace::calculate_percentile(sqlRT, counter, pv);
    }
    LWLockRelease(PercentileLock);
    pfree_ext(percentile);
    list_free_ext(percentilelist);
}

int64 PercentileSpace::calculate_percentile(SqlRTInfo* sqlRT, int counter, int percentile)
{
    if (counter == 1) {
        return sqlRT[0].rt;
    }
    float p = (float)(counter - 1) * percentile / 100;  // get percentages
    int i = (int)p;
    float j = p - (float)i;
    return (int64)((1 - j) * sqlRT[i].rt + j * sqlRT[i + 1].rt);
}

static int64* getPercentile(void)
{
    int64* p = NULL;
    p = (int64*)palloc(NUM_PERCENTILE_COUNT * sizeof(int64));
    errno_t rc = memset_s(p, NUM_PERCENTILE_COUNT * sizeof(int64), 0, NUM_PERCENTILE_COUNT * sizeof(int64));
    securec_check(rc, "\0", "\0");
    LWLockAcquire(PercentileLock, LW_SHARED);
    for (int i = 0; i < NUM_PERCENTILE_COUNT; ++i) {
        p[i] = g_instance.stat_cxt.RTPERCENTILE[i];
    }
    LWLockRelease(PercentileLock);
    return p;
}

bool PercentileSpace::CheckQueryPercentile(void)
{
    if ((IS_PGXC_COORDINATOR || IS_SINGLE_NODE) &&
        u_sess->attr.attr_common.enable_instr_rt_percentile) {
        return true;
    }

    if (!(IS_PGXC_COORDINATOR || IS_SINGLE_NODE)) {
        ereport(NOTICE, (errmsg("the view is only supported on cn node")));
    } else {
        ereport(WARNING, (errcode(ERRCODE_WARNING), (errmsg("GUC parameter 'enable_instr_rt_percentile' is off"))));
    }
    return false;
}

Datum get_instr_rt_percentile(PG_FUNCTION_ARGS)
{
    FuncCallContext* funcctx = NULL;

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext oldcontext;
        TupleDesc tupdesc;
        int i = 0;

        funcctx = SRF_FIRSTCALL_INIT();

        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        tupdesc = CreateTemplateTupleDesc(NUM_PERCENTILE_COUNT, false);
        if (!(PercentileSpace::CheckQueryPercentile())) {
            MemoryContextSwitchTo(oldcontext);
            SRF_RETURN_DONE(funcctx);
        }
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "P80", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)++i, "P95", INT8OID, -1, 0);
        funcctx->tuple_desc = BlessTupleDesc(tupdesc);
        funcctx->user_fctx = getPercentile();
        funcctx->max_calls = 1;
        MemoryContextSwitchTo(oldcontext);

        if (funcctx->user_fctx == NULL) {
            SRF_RETURN_DONE(funcctx);
        }
    }
    funcctx = SRF_PERCALL_SETUP();
    if (funcctx->user_fctx && funcctx->call_cntr < funcctx->max_calls) {
        Datum values[NUM_PERCENTILE_COUNT];
        bool nulls[NUM_PERCENTILE_COUNT] = {false};
        HeapTuple tuple;
        Datum result;
        int i = -1;

        int64* p = (int64*)funcctx->user_fctx + funcctx->call_cntr;

        errno_t rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");

        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");
        values[++i] = Int64GetDatum(*p);
        values[++i] = Int64GetDatum(*(p + 1));
        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        result = HeapTupleGetDatum(tuple);
        SRF_RETURN_NEXT(funcctx, result);
    }
    SRF_RETURN_DONE(funcctx);
}
