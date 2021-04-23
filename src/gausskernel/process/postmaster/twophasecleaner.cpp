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

#include "gssignal/gs_signal.h"
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

/* Signal handlers */
static void TwoPCSigHupHandler(SIGNAL_ARGS);
static void TwoPCShutdownHandler(SIGNAL_ARGS);
static int get_prog_path(const char* argv0);

NON_EXEC_STATIC void TwoPhaseCleanerMain()
{
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
            proc_exit(0);
        }

        /*
         * Call gs_clean to clean the prepared transaction every
         * gs_clean_timeout second.
         * Remark: After the process is started, wait 60s to call
         * gs_clean firstly, if successed, then make the clean_successed
         * to true, if failed, wait 60s to recall gs_clean until it
         * success.
         */
        if (bSyncXactsCallGsclean ||
            (u_sess->attr.attr_storage.gs_clean_timeout &&
                ((!clean_successed &&
                     TimestampDifferenceExceeds(gs_clean_start_time, gs_clean_current_time, 60 * 1000 /* 60s */)) ||
                    TimestampDifferenceExceeds(gs_clean_start_time,
                        gs_clean_current_time,
                        u_sess->attr.attr_storage.gs_clean_timeout * 1000)))) {
            int status = 0;
            char cmd[MAX_PATH_LEN];

            status = get_prog_path("gaussdb");
            if (status < 0) {
                ereport(DEBUG5, (errmsg("failed to invoke get_prog_path()")));
            } else {
                /* if we find explicit cn listen address, we use tcp connection instead of unix socket */
#ifdef ENABLE_MULTIPLE_NODES
#ifdef USE_ASSERT_CHECKING
                rc = sprintf_s(cmd,
                    sizeof(cmd),
                    "gs_clean -a -p %d -h localhost -v -r -j %d >>%s 2>&1",
                    g_instance.attr.attr_network.PoolerPort,
                    u_sess->attr.attr_storage.twophase_clean_workers,
                    t_thrd.tpcleaner_cxt.pgxc_clean_log_path);
                securec_check_ss(rc, "\0", "\0");
#else
                rc = sprintf_s(cmd,
                    sizeof(cmd),
                    "gs_clean -a -p %d -h localhost -v -r -j %d > /dev/null 2>&1",
                    g_instance.attr.attr_network.PoolerPort,
                    u_sess->attr.attr_storage.twophase_clean_workers);
                securec_check_ss(rc, "\0", "\0");
#endif
#else
                rc = sprintf_s(cmd,
                    sizeof(cmd),
                    "gs_clean -a -p %d -h localhost -e -v -r -j %d > /dev/null 2>&1",
                    g_instance.attr.attr_network.PostPortNumber,
                    u_sess->attr.attr_storage.twophase_clean_workers);
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

    t_thrd.tpcleaner_cxt.shutdown_requested = true;

    if (t_thrd.proc)
        SetLatch(&t_thrd.proc->procLatch);

    errno = save_errno;
}

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
        check_backend_env(exec_path);
        rc = snprintf_s(env_gausshome_val, sizeof(env_gausshome_val), MAX_PATH_LEN - 1, "%s", exec_path);
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
            check_backend_env(gausslog_dir);
            rc = snprintf_s(env_gausslog_val, sizeof(env_gausslog_val), MAX_PATH_LEN - 1, "%s", gausslog_dir);
            securec_check_ss(rc, "\0", "\0");
            rc = snprintf_s(log_dir, sizeof(log_dir), MAX_PATH_LEN - 1, "%s/bin/%s", env_gausslog_val, PGXC_CLEAN);
            securec_check_ss(rc, "\0", "\0");
            /* log_dir not exist, create log_dir path */
            if (0 != mkdir(log_dir, S_IRWXU)) {
                if (EEXIST != errno) {
                    ereport(WARNING, (errmsg("could not create directory %s: %m", log_dir)));
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
