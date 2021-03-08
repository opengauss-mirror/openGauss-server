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
 * remoteservice.cpp
 *
 * IDENTIFICATION
 *	  src/gausskernel/process/postmaster/remoteservice.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "gssignal/gs_signal.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/remoteservice.h"
#include "service/rpc_server.h"
#include "storage/ipc.h"
#include "utils/memutils.h"
#include "utils/resowner.h"

/* Signal handlers */
static void RsSigHupHandler(SIGNAL_ARGS);
static void ReqShutdownHandler(SIGNAL_ARGS);
static void RsQuickDie(SIGNAL_ARGS);
static void RsSigusr1Handler(SIGNAL_ARGS);

/* MACROS which help to catch and print the exception. */
#define GRPC_TRY()                                         \
    bool saveStatus = t_thrd.int_cxt.ImmediateInterruptOK; \
    t_thrd.int_cxt.ImmediateInterruptOK = false;           \
    bool errOccur = false;                                 \
    int errNo = ERRCODE_SYSTEM_ERROR;                      \
    StringInfo errMsg = makeStringInfo();                  \
    try
#define GRPC_CATCH()                                                                        \
    catch (abi::__forced_unwind&)                                                           \
    {                                                                                       \
        throw;                                                                              \
    }                                                                                       \
    catch (std::exception & ex)                                                             \
    {                                                                                       \
        errOccur = true;                                                                    \
        try {                                                                               \
            appendStringInfo(errMsg, "%s", ex.what());                                      \
        } catch (abi::__forced_unwind&) {                                                   \
            throw;                                                                          \
        } catch (...) {                                                                     \
        }                                                                                   \
    }                                                                                       \
    catch (...)                                                                             \
    {                                                                                       \
        errOccur = true;                                                                    \
    }                                                                                       \
    t_thrd.int_cxt.ImmediateInterruptOK = saveStatus;                                       \
    saveStatus = InterruptPending;                                                          \
    InterruptPending = false;                                                               \
    if (errOccur && errMsg->len > 0) {                                                      \
        ereport(LOG, (errmodule(MOD_REMOTE), errmsg("catch exception: %s", errMsg->data))); \
    }                                                                                       \
    InterruptPending = saveStatus;

#define GRPC_REPORT_IF_EXCEPTION(level, msg)                                                \
    if (errOccur) {                                                                         \
        ereport(level, (errcode(errNo), errmodule(MOD_REMOTE), errmsg(msg, errMsg->data))); \
    }                                                                                       \
    pfree_ext(errMsg->data);                                                                \
    pfree_ext(errMsg);

#define GRPC_REPORT(level, msg)                               \
    if (!errOccur) {                                          \
        ereport(level, (errmodule(MOD_REMOTE), errmsg(msg))); \
    }

void RemoteServiceMain(void)
{
    sigjmp_buf local_sigjmp_buf;
    MemoryContext remote_service_context;
    long cur_timeout_ms = 0;

    ereport(LOG, (errmsg("remote service is starting...")));

    /*
     * Properly accept or ignore signals the postmaster might send us.
     *
     * remote service doesn't participate in ProcSignal signalling, but a SIGUSR1
     * handler is still needed for latch wakeups.
     */
    (void)gspqsignal(SIGHUP, RsSigHupHandler); /* set flag to read config file */
    (void)gspqsignal(SIGINT, SIG_IGN);
    (void)gspqsignal(SIGTERM, ReqShutdownHandler); /* shutdown */
    (void)gspqsignal(SIGQUIT, RsQuickDie);         /* hard crash time */
    (void)gspqsignal(SIGALRM, SIG_IGN);
    (void)gspqsignal(SIGPIPE, SIG_IGN);
    (void)gspqsignal(SIGUSR1, RsSigusr1Handler);
    (void)gspqsignal(SIGUSR2, SIG_IGN);

    /*
     * Reset some signals that are accepted by postmaster but not here
     */
    (void)gspqsignal(SIGCHLD, SIG_DFL);
    (void)gspqsignal(SIGTTIN, SIG_DFL);
    (void)gspqsignal(SIGTTOU, SIG_DFL);
    (void)gspqsignal(SIGCONT, SIG_DFL);
    (void)gspqsignal(SIGWINCH, SIG_DFL);

    /* We allow SIGQUIT (quickdie) at all times */
    sigdelset(&t_thrd.libpq_cxt.BlockSig, SIGQUIT);

    /*
     * Create a resource owner to keep track of our resources (currently only
     * buffer pins).
     */
    t_thrd.utils_cxt.CurrentResourceOwner = ResourceOwnerCreate(NULL, "Remote Service", MEMORY_CONTEXT_STORAGE);

    /*
     * Create a memory context that we will do all our work in.  We do this so
     * that we can reset the context during error recovery and thereby avoid
     * possible memory leaks.  Formerly this code just ran in
     * t_thrd.top_mem_cxt, but resetting that would be a really bad idea.
     */
    remote_service_context = AllocSetContextCreate(t_thrd.top_mem_cxt,
        "Remote Service",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);
    (void)MemoryContextSwitchTo(remote_service_context);

    int curTryCounter;
    int* oldTryCounter = NULL;
    if (sigsetjmp(local_sigjmp_buf, 1) != 0) {
        gstrace_tryblock_exit(true, oldTryCounter);

        /* Since not using PG_TRY, must reset error stack by hand */
        t_thrd.log_cxt.error_context_stack = NULL;

        /* Prevent interrupts while cleaning up */
        HOLD_INTERRUPTS();

        /* Report the error to the server log */
        EmitErrorReport();

        /* code here */

        /* Now we can allow interrupts again */
        RESUME_INTERRUPTS();

        /*
         * Sleep at least 1 second after any error.  A write error is likely
         * to be repeated, and we don't want to be filling the error logs as
         * fast as we can.
         */
        pg_usleep(1000000L);

        /*
         * Close all open files after any error.  This is helpful on Windows,
         * where holding deleted files open causes various strange errors.
         * It's not clear we need it elsewhere, but shouldn't hurt.
         */
        smgrcloseall();
    }
    oldTryCounter = gstrace_tryblock_entry(&curTryCounter);

    cur_timeout_ms = (long)1 * 60 * 1000L;

    /*
     * Unblock signals (blocked when postmaster forked us)
     */
    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();

    pgstat_report_appname("Remote Service");
    pgstat_report_activity(STATE_IDLE, NULL);

    char listen_address[MAXPGPATH];
    int rc = EOK;
    int i = 0;

    for (i = 1; i < MAX_REPLNODE_NUM; i++) {
        if (t_thrd.postmaster_cxt.ReplConnArray[i] && t_thrd.postmaster_cxt.ReplConnArray[i]->localservice != 0) {
            break;
        }
    }

    if (i == MAX_REPLNODE_NUM) {
        ereport(WARNING, (errmodule(MOD_REMOTE), errmsg("remote service port not available")));
        proc_exit(0);
    }
    rc = snprintf_s(listen_address,
        MAXPGPATH,
        (MAXPGPATH - 1),
        "%s:%d",
        t_thrd.postmaster_cxt.ReplConnArray[i]->localhost,
        t_thrd.postmaster_cxt.ReplConnArray[i]->localservice);
    securec_check_ss(rc, "", "");

    GRPC_TRY() {
        /* build and start rpc server */
        t_thrd.rs_cxt.server_context = BuildAndStartServer(listen_address);
    }
    GRPC_CATCH();
    GRPC_REPORT_IF_EXCEPTION(ERROR, "remote service start failed. exception: %s");

    /* loop to wait shutdown */
    for (;;) {
        /* Clear any already-pending wakeups */
        ResetLatch(&t_thrd.proc->procLatch);

        /* Process any requests or signals received recently. */
        if (t_thrd.rs_cxt.got_SIGHUP) {
            t_thrd.rs_cxt.got_SIGHUP = false;
            ProcessConfigFile(PGC_SIGHUP);
        }

        /* Exit before call RunServer() */
        if (t_thrd.rs_cxt.shutdown_requested) {
            ereport(LOG, (errmodule(MOD_REMOTE), errmsg("remote service is closing...")));
            GRPC_TRY() {
                ShutdownAndReleaseServer(t_thrd.rs_cxt.server_context);
                t_thrd.rs_cxt.server_context = NULL;
            }
            GRPC_CATCH();
            GRPC_REPORT_IF_EXCEPTION(LOG, "remote service shutdown failed. exception: %s");
            GRPC_REPORT(LOG, "remote service closed.");

            proc_exit(0);
        }
        pgstat_report_activity(STATE_IDLE, NULL);

        rc = WaitLatch(
            &t_thrd.proc->procLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, (long)cur_timeout_ms /* ms */);

        /*
         * Emergency bailout if postmaster has died.  This is to avoid the
         * necessity for manual cleanup of all postmaster children.
         */
        if ((unsigned int)rc & WL_POSTMASTER_DEATH) {
            gs_thread_exit(1);
        }
    }
}

/* SIGHUP: set flag to re-read config file at next convenient time */
static void RsSigHupHandler(SIGNAL_ARGS)
{
    int save_errno = errno;

    t_thrd.rs_cxt.got_SIGHUP = true;

    if (t_thrd.proc)
        SetLatch(&t_thrd.proc->procLatch);

    errno = save_errno;
}

/* SIGTERM: set flag to shutdown and exit */
static void ReqShutdownHandler(SIGNAL_ARGS)
{
    int save_errno = errno;

    t_thrd.rs_cxt.shutdown_requested = true;

    if (t_thrd.proc)
        SetLatch(&t_thrd.proc->procLatch);

    errno = save_errno;
}

/* SIGQUIT: */
static void RsQuickDie(SIGNAL_ARGS)
{
    gs_signal_setmask(&t_thrd.libpq_cxt.BlockSig, NULL);

    /* release t_thrd.rs_cxt.server_context */
    try {
        ForceReleaseServer(t_thrd.rs_cxt.server_context);
        t_thrd.rs_cxt.server_context = NULL;
    } catch (...) {
        /* quick die, do nothing */
    }

    /*
     * We DO NOT want to run proc_exit() callbacks -- we're here because
     * shared memory may be corrupted, so we don't want to try to clean up our
     * transaction.  Just nail the windows shut and get out of town.  Now that
     * there's an atexit callback to prevent third-party code from breaking
     * things by calling exit() directly, we have to reset the callbacks
     * explicitly to make this work as intended.
     */
    on_exit_reset();

    /*
     * Note we do exit(2) not exit(0).	This is to force the postmaster into a
     * system reset cycle if some idiot DBA sends a manual SIGQUIT to a random
     * backend.  This is necessary precisely because we don't clean up our
     * shared memory state.  (The "dead man switch" mechanism in pmsignal.c
     * should ensure the postmaster sees this as a crash, too, but no harm in
     * being doubly sure.)
     */
    exit(2);
}

/* SIGUSR1: used for latch wakeups */
static void RsSigusr1Handler(SIGNAL_ARGS)
{
    int save_errno = errno;

    latch_sigusr1_handler();

    errno = save_errno;
}
