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
 * twophasecleaner.cpp
 *	 Automatically clean two-phase transaction
 *
 * This thread will call the tool of gs_clean to clean two-phase transaction
 * in the clusters every 5min(can be changed by the parameter of
 * gs_clean_timout).
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/process/postmaster/twophasecleaner.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <sys/stat.h>

#include "catalog/pg_database.h"
#include "gssignal/gs_signal.h"
#include "libpq/libpq-fe.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "pgxc/pgxc.h"
#include "port.h"
#include "postmaster/postmaster.h"
#include "storage/ipc.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"

#define PGXC_CLEAN_LOG_FILE "gs_clean.log"
#define PGXC_CLEAN "gs_clean"
#define CM_STATIC_CONFIG_FILE "cluster_static_config"
#define MAX_PATH_LEN 1024

bool bSyncXactsCallGsclean = false;
PGPROC* twoPhaseCleanerProc = NULL;

#ifndef ENABLE_MULTIPLE_NODES
#define MAX_ERRMSG_LENGTH 1024

typedef struct DatabaseNames {
    struct DatabaseNames* next;
    char* databaseName;
} DatabaseNames;

typedef struct TempSchemaInfo {
    struct TempSchemaInfo* next;
    char* tempSchemaName;
} TempSchemaInfo;

typedef struct ActiveBackendInfo {
    struct ActiveBackendInfo* next;
    int64 sessionID;
    uint32 tempID;
    uint32 timeLineID;
} ActiveBackendInfo;
#endif

/* Signal handlers */
static void TwoPCSigHupHandler(SIGNAL_ARGS);
static void TwoPCShutdownHandler(SIGNAL_ARGS);
#ifdef ENABLE_MULTIPLE_NODES
static int get_prog_path(const char* argv0);
#endif
#ifndef ENABLE_MULTIPLE_NODES
static bool DropTempNamespace();
#endif

#ifdef ENABLE_MULTIPLE_NODES
static const char* getGsCleanLogLevel()
{
    if (module_logging_is_on(MOD_GSCLEAN)) {
        return "DEBUG";
    } else {
        return "LOG";
    }
}
#endif

NON_EXEC_STATIC void TwoPhaseCleanerMain()
{
    sigjmp_buf local_sigjmp_buf;
    MemoryContext twopc_context;
    bool clean_successed = false;
    int rc;

    /* we are a postmaster subprocess now */
    IsUnderPostmaster = true;

    t_thrd.proc_cxt.MyProcPid = gs_thread_self();

    twoPhaseCleanerProc = t_thrd.proc;

    ereport(DEBUG5, (errmsg("twophasecleaner process is started: %lu", t_thrd.proc_cxt.MyProcPid)));

    (void)gspqsignal(SIGHUP, TwoPCSigHupHandler);    /* set flag to read config file */
    (void)gspqsignal(SIGINT, TwoPCShutdownHandler);  /* request shutdown */
    (void)gspqsignal(SIGTERM, TwoPCShutdownHandler); /* request shutdown */
    (void)gspqsignal(SIGQUIT, SIG_IGN);
    (void)gspqsignal(SIGALRM, SIG_IGN);
    (void)gspqsignal(SIGPIPE, SIG_IGN);
    (void)gspqsignal(SIGUSR1, SIG_IGN);
    (void)gspqsignal(SIGUSR2, SIG_IGN); /* not used */
    (void)gspqsignal(SIGURG, print_stack);
    /* Reset some signals that are accepted by postmaster but not here */
    (void)gspqsignal(SIGCHLD, SIG_DFL);
    (void)gspqsignal(SIGTTIN, SIG_DFL);
    (void)gspqsignal(SIGTTOU, SIG_DFL);
    (void)gspqsignal(SIGCONT, SIG_DFL);
    (void)gspqsignal(SIGWINCH, SIG_DFL);

    /* We allow SIGQUIT (quickdie) at all times */
    (void)sigdelset(&t_thrd.libpq_cxt.BlockSig, SIGQUIT);

    /*
     * Create a memory context that we will do all our work in.  We do this so
     * that we can reset the context during error recovery and thereby avoid
     * possible memory leaks.  Formerly this code just ran in
     * t_thrd.top_mem_cxt, but resetting that would be a really bad idea.
     */
    twopc_context = AllocSetContextCreate(t_thrd.top_mem_cxt,
        "TwoPhase Cleaner",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);
    (void)MemoryContextSwitchTo(twopc_context);

    /*
     * If an exception is encountered, processing resumes here.
     *
     * See notes in postgres.c about the design of this coding.
     */
    int curTryCounter;
    int* oldTryCounter = NULL;
    if (sigsetjmp(local_sigjmp_buf, 1) != 0) {
        gstrace_tryblock_exit(true, oldTryCounter);
        /* Prevents interrupts while cleaning up */
        HOLD_INTERRUPTS();

        /* Report the error to the server log */
        EmitErrorReport();

        /* release resource held by lsc */
        AtEOXact_SysDBCache(false);

        /*
         * Now return to normal top-level context and clear ErrorContext for
         * next time.
         */
        (void)MemoryContextSwitchTo(twopc_context);

        FlushErrorState();

        /* Flush any leaked data in the top-level context */
        MemoryContextResetAndDeleteChildren(twopc_context);

        /* Now we can allow interrupts again */
        RESUME_INTERRUPTS();
    }
    oldTryCounter = gstrace_tryblock_entry(&curTryCounter);

    /* We can now handle ereport(ERROR) */
    t_thrd.log_cxt.PG_exception_stack = &local_sigjmp_buf;

    /* Unblock signals (they were blocked when the postmaster forked us) */
    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();

    TimestampTz gs_clean_start_time = GetCurrentTimestamp();
    TimestampTz gs_clean_start_time_gtm_val = GetCurrentTimestamp();

    pgstat_report_appname("TwoPhase Cleaner");
    pgstat_report_activity(STATE_IDLE, NULL);

    for (;;) {
        TimestampTz gs_clean_current_time = GetCurrentTimestamp();

        /* Clear any already-pending wakeups */
        ResetLatch(&t_thrd.proc->procLatch);

        /* Process any requests or signals received recently. */
        if (t_thrd.tpcleaner_cxt.got_SIGHUP) {
            t_thrd.tpcleaner_cxt.got_SIGHUP = false;
            ProcessConfigFile(PGC_SIGHUP);
        }

        if (t_thrd.tpcleaner_cxt.shutdown_requested) {
            /* Normal exit from the twophasecleaner is here */
            ereport(LOG, (errmsg("TwoPhaseCleaner exits via SIGTERM")));
            proc_exit(0);
        }

        if (SS_PRIMARY_DEMOTING) {
            ereport(LOG, (errmsg("TwoPhaseCleaner exits via SS global var")));
            proc_exit(0);
        }

        /*
         * In distributed Gaussdb, we call gs_clean to clean the prepared
         * transaction every gs_clean_timeout second.
         * Remark: After the process is started, wait 60s to call
         * gs_clean firstly, if successed, then make the clean_successed
         * to true, if failed, wait 60s to recall gs_clean until it
         * success.
         *
         * In opengauss, instead of calling gs_clean, we use twophasecleaner
         * to clean up the temporary table every gs_clean_timeout second.
         */
        if (bSyncXactsCallGsclean ||
            (u_sess->attr.attr_storage.gs_clean_timeout &&
                ((!clean_successed &&
                     TimestampDifferenceExceeds(gs_clean_start_time, gs_clean_current_time, 60 * 1000 /* 60s */)) ||
                    TimestampDifferenceExceeds(gs_clean_start_time,
                        gs_clean_current_time,
                        u_sess->attr.attr_storage.gs_clean_timeout * 1000)))) {
#ifdef ENABLE_MULTIPLE_NODES
            int status = 0;
            char cmd[MAX_PATH_LEN];

            status = get_prog_path("gaussdb");
            if (status < 0) {
                ereport(DEBUG5, (errmsg("failed to invoke get_prog_path()")));
            } else {
                /* if we find explicit cn listen address, we use tcp connection instead of unix socket */
#ifdef USE_ASSERT_CHECKING
                rc = sprintf_s(cmd,
                    sizeof(cmd),
                    "gs_clean -a -p %d -h localhost -v -r -j %d -l %s >> %s 2>&1",
                    g_instance.attr.attr_network.PoolerPort,
                    u_sess->attr.attr_storage.twophase_clean_workers,
                    getGsCleanLogLevel(),
                    t_thrd.tpcleaner_cxt.pgxc_clean_log_path);
                securec_check_ss(rc, "\0", "\0");
#else
                rc = sprintf_s(cmd,
                    sizeof(cmd),
                    "gs_clean -a -p %d -h localhost -v -r -j %d -l %s > /dev/null 2>&1",
                    g_instance.attr.attr_network.PoolerPort,
                    u_sess->attr.attr_storage.twophase_clean_workers,
                    getGsCleanLogLevel());
                securec_check_ss(rc, "\0", "\0");
#endif
                socket_close_on_exec();

                pgstat_report_activity(STATE_RUNNING, NULL);
                status = system(cmd);

                if (status == 0) {
                    ereport(DEBUG5, (errmsg("clean up 2pc transactions succeed")));
                    clean_successed = true;
                    bSyncXactsCallGsclean = false;
                } else {
                    clean_successed = false;
                    ereport(WARNING, (errmsg("clean up 2pc transactions failed")));
                }
            }
#else
            if (DropTempNamespace()) {
                ereport(DEBUG5, (errmsg("clean up temp schemas succeed")));
                clean_successed = true;
                bSyncXactsCallGsclean = false;
            } else {
                clean_successed = false;
                ereport(WARNING, (errmsg("clean up temp schemas failed")));
            }
#endif
            gs_clean_start_time = GetCurrentTimestamp();
        }

        /*
         * Call CancelInvalidBeGTMConn every gtm_conn_check_interval seconds
         * to cancel those queries running in backends connected to demoted GTM.
         *
         * Remark: set gtm_conn_check_interval to ZERO to disable this function.
         */
        if (u_sess->attr.attr_storage.gtm_conn_check_interval &&
            TimestampDifferenceExceeds(gs_clean_start_time_gtm_val,
                gs_clean_current_time,
                u_sess->attr.attr_storage.gtm_conn_check_interval * 1000)) {
            pgstat_cancel_invalid_gtm_conn();
            gs_clean_start_time_gtm_val = GetCurrentTimestamp();
        }

        pgstat_report_activity(STATE_IDLE, NULL);
        rc = WaitLatch(&t_thrd.proc->procLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, (long)10000 /* 10s */);

        MemoryContextResetAndDeleteChildren(twopc_context);

        /*
         * Emergency bailout if postmaster has died.  This is to avoid the
         * necessity for manual cleanup of all postmaster children.
         */
        if (((unsigned int)rc) & WL_POSTMASTER_DEATH)
            gs_thread_exit(1);
    }
}

/* SIGHUP: set flag to re-read config file at next convenient time */
static void TwoPCSigHupHandler(SIGNAL_ARGS)
{
    int save_errno = errno;

    t_thrd.tpcleaner_cxt.got_SIGHUP = true;

    if (t_thrd.proc)
        SetLatch(&t_thrd.proc->procLatch);

    errno = save_errno;
}

/* SIGTERM: set flag to exit normally */
static void TwoPCShutdownHandler(SIGNAL_ARGS)
{
    int save_errno = errno;

    ereport(LOG, (errmsg("TwoPhaseCleaner received SIGTERM")));
    t_thrd.tpcleaner_cxt.shutdown_requested = true;

    if (t_thrd.proc)
        SetLatch(&t_thrd.proc->procLatch);

    errno = save_errno;
}

#ifdef ENABLE_MULTIPLE_NODES
static int get_prog_path(const char* argv0)
{
    char* exec_path = NULL;
    char* gausslog_dir = NULL;
    char log_dir[MAX_PATH_LEN] = {0};
    char env_gausshome_val[MAX_PATH_LEN] = {0};
    char env_gausslog_val[MAX_PATH_LEN] = {0};
    char gaussdb_bin_path[MAX_PATH_LEN] = {0};
    errno_t errorno = EOK;
    int rc;

    errorno = memset_s(t_thrd.tpcleaner_cxt.pgxc_clean_log_path, MAX_PATH_LEN, 0, MAX_PATH_LEN);
    securec_check_c(errorno, "\0", "\0");

    exec_path = gs_getenv_r("GAUSSHOME");

    if (NULL == exec_path) {
        if (find_my_exec(argv0, gaussdb_bin_path) < 0) {
            ereport(WARNING, (errmsg("%s: could not locate my own executable path", argv0)));
            return -1;
        }
        exec_path = gaussdb_bin_path;
        check_backend_env(exec_path);
        get_parent_directory(exec_path); /* remove my executable name */
    }

    Assert(NULL != exec_path);

    {
        char realExecPath[PATH_MAX + 1] = {'\0'};
        if (realpath(exec_path, realExecPath) == NULL) {
            ereport(WARNING,
                (errmodule(MOD_TRANS_XACT), errcode(ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION),
                errmsg("Failed to obtain environment value $GAUSSHOME!"),
                errdetail("N/A"),
                errcause("Incorrect environment value."),
                erraction("Please refer to backend log for more details.")));
            return -1;
        }
        exec_path = NULL;
        check_backend_env(realExecPath);
        rc = snprintf_s(env_gausshome_val, sizeof(env_gausshome_val), MAX_PATH_LEN - 1, "%s", realExecPath);
        securec_check_ss(rc, "\0", "\0");
        gausslog_dir = gs_getenv_r("GAUSSLOG");
        if ((NULL == gausslog_dir) || ('\0' == gausslog_dir[0])) {
            ereport(WARNING, (errmsg("environment variable $GAUSSLOG is not set")));
            rc = snprintf_s(t_thrd.tpcleaner_cxt.pgxc_clean_log_path,
                sizeof(t_thrd.tpcleaner_cxt.pgxc_clean_log_path),
                MAX_PATH_LEN - 1,
                "%s/%s.%s",
                env_gausshome_val,
                PGXC_CLEAN_LOG_FILE,
                g_instance.attr.attr_common.PGXCNodeName);
            securec_check_ss(rc, "\0", "\0");
        } else {
            char realGausslogDir[PATH_MAX + 1] = {'\0'};
            if (realpath(gausslog_dir, realGausslogDir) == NULL) {
                ereport(WARNING,
                    (errmodule(MOD_TRANS_XACT), errcode(ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION),
                    errmsg("Failed to obtain environment value $GAUSSLOG!"),
                    errdetail("N/A"),
                    errcause("Incorrect environment value."),
                    erraction("Please refer to backend log for more details.")));
                return -1;
            }
            gausslog_dir = NULL;
            check_backend_env(realGausslogDir);
            rc = snprintf_s(env_gausslog_val, sizeof(env_gausslog_val), MAX_PATH_LEN - 1, "%s", realGausslogDir);
            securec_check_ss(rc, "\0", "\0");
            rc = snprintf_s(log_dir, sizeof(log_dir), MAX_PATH_LEN - 1, "%s/bin/%s", env_gausslog_val, PGXC_CLEAN);
            securec_check_ss(rc, "\0", "\0");
            /* log_dir not exist, create log_dir path */
            if (0 != mkdir(log_dir, S_IRWXU)) {
                if (EEXIST != errno) {
                    ereport(WARNING, (errmsg("could not create directory %s: %s", log_dir, TRANSLATE_ERRNO)));
                    return -1;
                }
            }
            rc = snprintf_s(t_thrd.tpcleaner_cxt.pgxc_clean_log_path,
                sizeof(t_thrd.tpcleaner_cxt.pgxc_clean_log_path),
                MAX_PATH_LEN - 1,
                "%s/bin/%s/%s.%s",
                env_gausslog_val,
                PGXC_CLEAN,
                PGXC_CLEAN_LOG_FILE,
                g_instance.attr.attr_common.PGXCNodeName);
            securec_check_ss(rc, "\0", "\0");
        }
    }

    return 0;
}
#endif

#ifndef ENABLE_MULTIPLE_NODES
static PGconn* LoginDatabase(char* host, int port, char* user, char* password,
    char* dbname, const char* progname, char* encoding)
{
    PGconn* conn = NULL;
    char portValue[32];
#define PARAMS_ARRAY_SIZE 10
    const char* keywords[PARAMS_ARRAY_SIZE];
    const char* values[PARAMS_ARRAY_SIZE];
    int count = 0;
    int retryNum = 10;
    int rc;

    rc = sprintf_s(portValue, sizeof(portValue), "%d", port);
    securec_check_ss_c(rc, "\0", "\0");

    keywords[0] = "host";
    values[0] = host;
    keywords[1] = "port";
    values[1] = portValue;
    keywords[2] = "user";
    values[2] = user;
    keywords[3] = "password";
    values[3] = password;
    keywords[4] = "dbname";
    values[4] = dbname;
    keywords[5] = "fallback_application_name";
    values[5] = progname;
    keywords[6] = "client_encoding";
    values[6] = encoding;
    keywords[7] = "connect_timeout";
    values[7] = "5";
    keywords[8] = "options";
    /* this mode: remove timeout */
    values[8] = "-c xc_maintenance_mode=on";
    keywords[9] = NULL;
    values[9] = NULL;

retry:
    /* try to connect to database */
    conn = PQconnectdbParams(keywords, values, true);
    if (PQstatus(conn) != CONNECTION_OK) {
        if (++count < retryNum) {
            ereport(LOG, (errmsg("Could not connect to the %s, the connection info : %s",
                                 dbname, PQerrorMessage(conn))));
            PQfinish(conn);
            conn = NULL;

            /* sleep 0.1 s */
            pg_usleep(100000L);
            goto retry;
        }

        char connErrorMsg[MAX_ERRMSG_LENGTH] = {0};
        errno_t rc;
        rc = snprintf_s(connErrorMsg, MAX_ERRMSG_LENGTH, MAX_ERRMSG_LENGTH - 1,
                        "%s", PQerrorMessage(conn));
        securec_check_ss(rc, "\0", "\0");

        PQfinish(conn);
        conn = NULL;
        ereport(ERROR, (errcode(ERRCODE_CONNECTION_TIMED_OUT),
                       (errmsg("Could not connect to the %s, "
                               "we have tried %d times, the connection info: %s",
                               dbname, count, connErrorMsg))));
    }

    return (conn);
}

static void AddDatabaseInfo(DatabaseNames** dbList, char* dbName)
{
    DatabaseNames* tempDatabase = NULL;

    tempDatabase = (DatabaseNames*)palloc(sizeof(DatabaseNames));

    tempDatabase->next = NULL;
    tempDatabase->databaseName = pstrdup(dbName);

    if (*dbList == NULL) {
        *dbList = tempDatabase;
    } else {
        tempDatabase->next = (*dbList)->next;
        (*dbList)->next = tempDatabase;
    }
}

static void GetDatabaseList(PGconn* conn, DatabaseNames** dbList)
{
    int databaseCount;
    PGresult* res = NULL;
    char* dbName = NULL;

    /* SQL Statement */
    static const char* STMT_GET_DATABASE_LIST = "SELECT DATNAME FROM PG_DATABASE;";

    /* Get database list. */
    res = PQexec(conn, STMT_GET_DATABASE_LIST);
    if (res == NULL || PQresultStatus(res) != PGRES_TUPLES_OK) {
        char resErrorMsg[MAX_ERRMSG_LENGTH] = {0};
        errno_t rc;
        rc = snprintf_s(resErrorMsg, MAX_ERRMSG_LENGTH, MAX_ERRMSG_LENGTH - 1,
                        "%s", PQresultErrorMessage(res));
        securec_check_ss(rc, "\0", "\0");

        PQclear(res);
        PQfinish(conn);
        conn = NULL;
        ereport(ERROR, (errcode(ERRCODE_INVALID_STATUS),
                        errmsg("Could not obtain database list: %s",
                               resErrorMsg)));
        return;
    }

    databaseCount = PQntuples(res);
    for (int i = 0; i < databaseCount; i++) {
        dbName = PQgetvalue(res, i, 0);
        if (strcmp(dbName, "template0") == 0 || strcmp(dbName, "template1") == 0) {
            /* Skip template0 and template1 database */
            continue;
        }

        AddDatabaseInfo(dbList, dbName);
    }

    PQclear(res);
}

static void CleanDatabaseList(DatabaseNames** dbList)
{
    DatabaseNames* database = *dbList;

    while (database != NULL) {
        DatabaseNames* savedDatabase = database;
        database = database->next;
        pfree_ext(savedDatabase->databaseName);
        pfree_ext(savedDatabase);
    }

    *dbList = NULL;
}

static void AddTempSchemaInfo(TempSchemaInfo** tempSchemaList, char* tempSchemaName)
{
    TempSchemaInfo* tempSchema = NULL;

    tempSchema = (TempSchemaInfo*)palloc(sizeof(TempSchemaInfo));

    tempSchema->next = NULL;
    tempSchema->tempSchemaName = pstrdup(tempSchemaName);

    if (*tempSchemaList == NULL) {
        *tempSchemaList = tempSchema;
    } else {
        tempSchema->next = (*tempSchemaList)->next;
        (*tempSchemaList)->next = tempSchema;
    }
}

static void GetTempSchemaList(PGconn* conn, TempSchemaInfo** tempSchemaList)
{
    int tempSchemaCount;
    PGresult* res = NULL;
    char* nspname = NULL;
    int rc;

    /* SQL Statement */
    char STMT_GET_TEMP_SCHEMA_LIST[NAMEDATALEN + 128] = {0};

    rc = snprintf_s(STMT_GET_TEMP_SCHEMA_LIST,
        sizeof(STMT_GET_TEMP_SCHEMA_LIST),
        sizeof(STMT_GET_TEMP_SCHEMA_LIST) - 1,
        "SELECT NSPNAME FROM PG_NAMESPACE WHERE NSPNAME LIKE 'pg_temp_%%'");
    securec_check_ss_c(rc, "\0", "\0");

    /* Get temp schema list. */
    res = PQexec(conn, STMT_GET_TEMP_SCHEMA_LIST);
    if (res == NULL || PQresultStatus(res) != PGRES_TUPLES_OK) {
        char resErrorMsg[MAX_ERRMSG_LENGTH] = {0};
        errno_t rc;
        rc = snprintf_s(resErrorMsg, MAX_ERRMSG_LENGTH, MAX_ERRMSG_LENGTH - 1,
                        "%s", PQresultErrorMessage(res));
        securec_check_ss(rc, "\0", "\0");

        PQclear(res);
        PQfinish(conn);
        conn = NULL;
        ereport(ERROR, (errcode(ERRCODE_INVALID_STATUS),
                        errmsg("Could not obtain temp schema list: %s",
                               resErrorMsg)));
        return;
    }

    tempSchemaCount = PQntuples(res);
    for (int i = 0; i < tempSchemaCount; i++) {
        nspname = PQgetvalue(res, i, 0);
        if (strchr(&nspname[7], '_') == NULL) {
            char resErrorMsg[MAX_ERRMSG_LENGTH] = {0};
            errno_t rc;
            rc = snprintf_s(resErrorMsg, MAX_ERRMSG_LENGTH, MAX_ERRMSG_LENGTH - 1,
                            "%s", PQresultErrorMessage(res));
            securec_check_ss(rc, "\0", "\0");

            PQclear(res);
            PQfinish(conn);
            conn = NULL;
            ereport(ERROR, (errcode(ERRCODE_INVALID_STATUS),
                        errmsg("Error when parse schema name: %s",
                               resErrorMsg)));
            return;
        }

        AddTempSchemaInfo(tempSchemaList, nspname);
    }

    PQclear(res);
}

static void CleanTempSchemaList(TempSchemaInfo** tempSchemaList)
{
    TempSchemaInfo* tempSchema = *tempSchemaList;

    while (tempSchema != NULL) {
        TempSchemaInfo* savedTempSchema = tempSchema;
        tempSchema = tempSchema->next;
        pfree_ext(savedTempSchema->tempSchemaName);
        pfree_ext(savedTempSchema);
    }

    *tempSchemaList = NULL;
}

static void AddActiveBackendInfo(ActiveBackendInfo** activeBackednList,
    int64 sessionID, uint32 tempID, uint32 timeLineID)
{
    ActiveBackendInfo* tempActiveBackend = NULL;

    tempActiveBackend = (ActiveBackendInfo*)palloc(sizeof(ActiveBackendInfo));

    tempActiveBackend->next = NULL;
    tempActiveBackend->sessionID = sessionID;
    tempActiveBackend->tempID = tempID;
    tempActiveBackend->timeLineID = timeLineID;

    if (*activeBackednList == NULL) {
        *activeBackednList = tempActiveBackend;
    } else {
        tempActiveBackend->next = (*activeBackednList)->next;
        (*activeBackednList)->next = tempActiveBackend;
    }

}

static void GetActiveBackendList(PGconn* conn, ActiveBackendInfo** activeBackendList)
{
    int activeBackendCount;
    PGresult* res = NULL;
    int64 sessionID;
    uint32 tempID;
    uint32 timeLineID;
    int rc;

    /* SQL Statement */
    char STMT_ACTIVE_BACKEND_LIST[2 * NAMEDATALEN + 128] = {0};

    rc = sprintf_s(STMT_ACTIVE_BACKEND_LIST,
        sizeof(STMT_ACTIVE_BACKEND_LIST),
        "SELECT SESSIONID, TEMPID, TIMELINEID FROM PG_DATABASE D, "
        "PG_STAT_GET_ACTIVITY_FOR_TEMPTABLE() AS S WHERE "
        "S.DATID = D.OID AND D.DATNAME = '%s'",
        PQdb(conn));
    securec_check_ss_c(rc, "\0", "\0");

    /* Get active backend list. */
    res = PQexec(conn, STMT_ACTIVE_BACKEND_LIST);
    if (res == NULL || PQresultStatus(res) != PGRES_TUPLES_OK) {
        char resErrorMsg[MAX_ERRMSG_LENGTH] = {0};
        errno_t rc;
        rc = snprintf_s(resErrorMsg, MAX_ERRMSG_LENGTH, MAX_ERRMSG_LENGTH - 1,
                        "%s", PQresultErrorMessage(res));
        securec_check_ss(rc, "\0", "\0");

        PQclear(res);
        PQfinish(conn);
        conn = NULL;
        ereport(ERROR, (errcode(ERRCODE_INVALID_STATUS),
                        errmsg("Could not obtain active backend list: %s",
                               resErrorMsg)));
        return;
    }

    activeBackendCount = PQntuples(res);
    for (int i = 0; i < activeBackendCount; i++) {
        char* result = PQgetvalue(res, i, 0);
        sessionID = atoll(result);
        result = PQgetvalue(res, i, 1);
        tempID = atoi(result);
        result = PQgetvalue(res, i, 2);
        timeLineID = atoi(result);

        AddActiveBackendInfo(activeBackendList, sessionID, tempID, timeLineID);
    }

    PQclear(res);
}

static void CleanActiveBackendList(ActiveBackendInfo** activeBackendList)
{
    ActiveBackendInfo* activeBackend = *activeBackendList;
    while (activeBackend != NULL) {
        ActiveBackendInfo* savedActiveBackend = activeBackend;
        activeBackend = activeBackend->next;
        pfree_ext(savedActiveBackend);
    }

    *activeBackendList = NULL;
}

static void DropTempSchema(PGconn* conn, char* nspname)
{
    PGresult* res = NULL;
    int rc;

    char toastnspName[NAMEDATALEN] = {0};
    /* SQL Statement */
    char STMT_DROP_TEMP_SCHEMA[2 * NAMEDATALEN + 64] = {0};

    rc = snprintf_s(toastnspName, NAMEDATALEN, strlen(nspname) + 7, "pg_toast_temp_%s", nspname + 8);
    securec_check_ss_c(rc, "\0", "\0");

    rc = snprintf_s(STMT_DROP_TEMP_SCHEMA,
        sizeof(STMT_DROP_TEMP_SCHEMA),
        2 * strlen(nspname) + 30,
        "DROP SCHEMA %s, %s CASCADE;",
        nspname,
        toastnspName);
    securec_check_ss_c(rc, "\0", "\0");

    res = PQexec(conn, STMT_DROP_TEMP_SCHEMA);

    /* If exec is not success, give an log and go on. */
    if (res == NULL || PQresultStatus(res) != PGRES_COMMAND_OK) {
        char resErrorMsg[MAX_ERRMSG_LENGTH] = {0};
        errno_t rc;
        rc = snprintf_s(resErrorMsg, MAX_ERRMSG_LENGTH, MAX_ERRMSG_LENGTH - 1,
                        "%s", PQresultErrorMessage(res));
        securec_check_ss(rc, "\0", "\0");

        PQclear(res);
        PQfinish(conn);
        conn = NULL;
        ereport(ERROR, (errcode(ERRCODE_INVALID_STATUS),
                        errmsg("Could not drop temp schema %s, %s: %s",
                               nspname, toastnspName, resErrorMsg)));
    }

    PQclear(res);
}

static void DropTempSchemas(PGconn* conn)
{
    /* Store temp schema names selected from DN. */
    TempSchemaInfo* tempSchemaList = NULL;
    /* Store current active backend pid to decide which temp schema should be dropped. */
    ActiveBackendInfo* activeBackendList = NULL;

    GetTempSchemaList(conn, &tempSchemaList);
    GetActiveBackendList(conn, &activeBackendList);

    TempSchemaInfo* tempSchema = tempSchemaList;
    ActiveBackendInfo* activeBackend = NULL;
    char tempBuffer[NAMEDATALEN] = {0};
    char tempBuffer2[NAMEDATALEN] = {0};
    errno_t rc;
    if (tempSchemaList == NULL || activeBackendList == NULL)
        return;
    while (tempSchema != NULL) {
        /*
         * Get sessionID, tempID, timelineID from end to start.
         * Temp schema name is pg_temp_%s_%u_%u_%lu.
         * These items are DN'name, timelineID, tempID, sessionID.
         */
        const char* lastPos = strrchr(tempSchema->tempSchemaName, '_');
        if (lastPos == NULL) {
            PQfinish(conn);
            conn = NULL;
            elog(ERROR, "strrchr failed, can't find '%c' in '%s'\n", '_', tempSchema->tempSchemaName);
        }
        int64 sessionID = strtoll(lastPos + 1, NULL, 10);
        rc = strncpy_s(tempBuffer, sizeof(tempBuffer), tempSchema->tempSchemaName,
                       lastPos - tempSchema->tempSchemaName);
        securec_check_c(rc, "\0", "\0");
        const char* secondLastPos = strrchr(tempBuffer, '_');
        if (secondLastPos == NULL) {
            PQfinish(conn);
            conn = NULL;
            elog(ERROR, "strrchr failed, can't find '%c' in '%s'\n", '_', tempBuffer);
        }
        uint32 tempID = strtol(secondLastPos + 1, NULL, 10);
        rc = strncpy_s(tempBuffer2, sizeof(tempBuffer2), tempBuffer, secondLastPos - tempBuffer);
        securec_check_c(rc, "\0", "\0");
        const char* thirdLastPos = strrchr(tempBuffer2, '_');
        if (thirdLastPos == NULL) {
            PQfinish(conn);
            conn = NULL;
            elog(ERROR, "strrchr failed, can't find '%c' in '%s'\n", '_', tempBuffer2);
        }
        uint32 timeLineID = strtol(thirdLastPos + 1, NULL, 10);

        activeBackend = activeBackendList;
        /*
         * Thus, We use sessionID, timeLineID and tempID together
         * to judge which session a temp table belongs to, instead of sessionID only.
         */
        while (activeBackend != NULL) {
            if (sessionID == activeBackend->sessionID && tempID == activeBackend->tempID &&
                timeLineID == activeBackend->timeLineID)
                break;
            activeBackend = activeBackend->next;
        }
        if (activeBackend == NULL) {
            /*
             * Now it is not an active backend, furthermore, we need to drop it;
             */
            DropTempSchema(conn, tempSchema->tempSchemaName);
        }
        tempSchema = tempSchema->next;
    }

    CleanTempSchemaList(&tempSchemaList);
    CleanActiveBackendList(&activeBackendList);
}

/*
 * @Description: Drop inactive temp schemas of each database.
 * @in: void
 * @return: the result of cleaning temporary schemas
 */
static bool DropTempNamespace()
{
    DatabaseNames* dbList = NULL;
    DatabaseNames* curDatabase = NULL;

    PGconn* conn = NULL;
    conn = LoginDatabase("localhost", g_instance.attr.attr_network.PostPortNumber,
        NULL, NULL, DEFAULT_DATABASE, PGXC_CLEAN, "auto");

    /* Get database list. */
    GetDatabaseList(conn, &dbList);

    PQfinish(conn);
    conn = NULL;

    if (dbList != NULL) {
        for (curDatabase = dbList; curDatabase != NULL; curDatabase = curDatabase->next) {
            conn = LoginDatabase("localhost", g_instance.attr.attr_network.PostPortNumber,
                NULL, NULL, curDatabase->databaseName, PGXC_CLEAN, "auto");

            if (conn == NULL) {
                ereport(ERROR, (errcode(ERRCODE_INVALID_STATUS),
                               (errmsg("Could not connect to the database %s.",
                                       curDatabase->databaseName))));
                return false;
            }

            DropTempSchemas(conn);

            ereport(DEBUG5, (errmsg("Drop temp namespace for database \"%s\" finished.",
                                    curDatabase->databaseName)));
            PQfinish(conn);
            conn = NULL;
        }
    }

    CleanDatabaseList(&dbList);
    return true;
}
#endif
