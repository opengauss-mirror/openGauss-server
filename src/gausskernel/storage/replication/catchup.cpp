/* -------------------------------------------------------------------------
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2010, PostgreSQL Global Development Group
 *
 *
 * catchup.cpp
 *	 functions for data catch-up management
 *
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/replication/catchup.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/xact.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "storage/buf/bufmgr.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "storage/smgr/smgr.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/postinit.h"
#include "utils/ps_status.h"
#include "utils/resowner.h"
#include "gssignal/gs_signal.h"
#include "pgstat.h"

#include "postmaster/postmaster.h"
#include "replication/bcm.h"
#include "replication/dataqueue.h"
#include "replication/datasender.h"
#include "replication/walsender_private.h"
#include "replication/datasender_private.h"
#include "replication/catchup.h"

CatchupState catchupState = CATCHUP_NONE;

volatile bool catchup_online = false;
volatile bool catchupDone = false;

extern bool DataSndInProgress(int type);

NON_EXEC_STATIC void CatchupMain();

/* SIGTERM: set flag to shut down */
static void CatchupShutdownHandler(SIGNAL_ARGS)
{
    t_thrd.catchup_cxt.catchup_shutdown_requested = true;
}

static void CatchupShutdown(void)
{
    ereport(LOG, (errmsg("catchup process shutdown.")));
    catchup_online = false;
    proc_exit(0);
}

/* Destroy the catchup data structure for catchup process */
static void CatchupKill(int code, Datum arg)
{
    ReplaceOrFreeBcmFileListBuffer(NULL, 0);
    ereport(LOG, (errmsg("catchup process shutdown.")));
    catchupState = CATCHUP_NONE;
    catchup_online = false;
}

static bool WaitDummyStarts(void)
{
    bool fullCatchup = false;
    int dummyFirstStartupWaitTimes = 0;
    ereport(LOG, (errmsg("catchup process is waiting for dummy starts.")));
    /* Sleep 5 mins at most while waiting for dummy starts up firstly in default. */
    while ((!DataSndInProgress(SNDROLE_PRIMARY_DUMMYSTANDBY) || (DataSndInSearching())) &&
           dummyFirstStartupWaitTimes < 2 * u_sess->attr.attr_storage.wait_dummy_time) {
        if (!DataSndInProgress(SNDROLE_PRIMARY_STANDBY) || t_thrd.catchup_cxt.catchup_shutdown_requested) {
            ereport(LOG, (errmsg("catchup process is shutting down because of some quit signal.")));
            catchupDone = true;
            CatchupShutdown();
        }
        pg_usleep(500000L); /* sleep 0.5s every loop to release CPU resource */
        dummyFirstStartupWaitTimes++;
    }

    /* Dummy starts in a difficulty situation, we wouldn't wait and use full catchup. */
    if (dummyFirstStartupWaitTimes >= 2 * u_sess->attr.attr_storage.wait_dummy_time) {
        fullCatchup = true;
    }
    return fullCatchup;
}

static bool WaitBcmFileList(void)
{
    bool fullCatchup = false;
    int bcmListReceivedWaitTimes = 0;

    /* Now, the cluster is normal, we wait dummy sends bcm file list
     * if 5 mins passed, we would not wait and use full catchup.
     */
    while (catchupState != RECEIVED_OK) {
        if (!DataSndInProgress(SNDROLE_PRIMARY_DUMMYSTANDBY)) {
            fullCatchup = true;
            break;
        }
        pg_memory_barrier();
        /* In case sender2 restart in 0.5s(one loop) */
        if ((catchupState == CATCHUP_SEARCHING) && !DataSndInSearching()) {
            fullCatchup = true;
            break;
        }
        if (bcmListReceivedWaitTimes >= 2 * u_sess->attr.attr_storage.wait_dummy_time) {
            fullCatchup = true;
            break;
        }
        /* If no data need be caught up, we need exit or it will hang here */
        if ((catchupState == RECEIVED_NONE) || t_thrd.catchup_cxt.catchup_shutdown_requested) {
            ereport(LOG, (errmsg("catchup process will shutdown because there is no data to be caughtup.")));
            catchupState = CATCHUP_NONE;
            catchup_online = false;
            catchupDone = true;
            CatchupShutdown();
        }
        /* sleep 0.5s every loop to release CPU resource */
        pg_usleep(500000L);
        bcmListReceivedWaitTimes++;
    }
    return fullCatchup;
}

bool IsCatchupProcess(void)
{
    return t_thrd.role == CATCHUP;
}

/*
 * Catchup is designed for Standby Catchup Primary, when DataSend that transfer
 * data from primary to standby shutdown, Catchup should shutdown, otherwise,
 * data for Catchup will be sent to Secordary, this will cause data loss:
 *  1. Standby Catchup Primary;
 *	2. DataSender between Primary and Standby exit because of timeout;
 *	3. data(`table1') to Catcup will sent to Secondary;
 *	4. Standby connect Primary again and continue Catchup;
 *	5. finary, Standby Catchup all other data, at this time, we mistakenly believe
 *	   Standby Catchup Primary, and Secordary can delete all data;
 *	6. Then, Primary down, and Standby failover, the `table1' data will loss;
 *
 *	So, when DataSend between Primary and Standby shutdown because of timeout,
 *	Catchup should shutdown.
 */
void CatchupShutdownIfNoDataSender(void)
{
    if (t_thrd.role == CATCHUP) {
        if (!DataSndInProgress(SNDROLE_PRIMARY_STANDBY) || !u_sess->attr.attr_storage.enable_stream_replication)
            CatchupShutdown();
    }
}

/*
 * CatchupMain
 */
NON_EXEC_STATIC void CatchupMain()
{
    sigjmp_buf local_sigjmp_buf;
    MemoryContext catchupContext;

    /* we are a postmaster subprocess now */
    IsUnderPostmaster = true;

    t_thrd.role = CATCHUP;

    catchup_online = true;

    /* tell datasender we update the values. */
    pg_memory_barrier();

    /* reset t_thrd.proc_cxt.MyProcPid */
    t_thrd.proc_cxt.MyProcPid = gs_thread_self();

    /* record Start Time for logging */
    t_thrd.proc_cxt.MyStartTime = time(NULL);

    t_thrd.proc_cxt.MyProgName = "Catchup";

    /* Identify myself via ps */
    init_ps_display("catchup process", "", "", "");

    SetProcessingMode(InitProcessing);

    /*
     * Set up signal handlers.
     */
    on_shmem_exit(CatchupKill, 0);
    (void)gspqsignal(SIGINT, SIG_IGN);
    (void)gspqsignal(SIGUSR1, procsignal_sigusr1_handler);
    (void)gspqsignal(SIGPIPE, SIG_IGN);

    (void)gspqsignal(SIGTERM, CatchupShutdownHandler);
    (void)gspqsignal(SIGQUIT, quickdie);
    (void)gspqsignal(SIGALRM, handle_sig_alarm);
    (void)gspqsignal(SIGUSR2, SIG_IGN);
    (void)gspqsignal(SIGFPE, SIG_IGN);
    (void)gspqsignal(SIGCHLD, SIG_DFL);
    (void)gspqsignal(SIGHUP, SIG_IGN);
    (void)gspqsignal(SIGURG, print_stack);

    /* We allow SIGQUIT (quickdie) at all times */
    (void)sigdelset(&t_thrd.libpq_cxt.BlockSig, SIGQUIT);

    /* Early initialization */
    BaseInit();

    /*
     * Create a per-backend PGPROC struct in shared memory, except in the
     * EXEC_BACKEND case where this was done in SubPostmasterMain. We must do
     * this before we can use LWLocks (and in the EXEC_BACKEND case we already
     * had to do some stuff with LWLocks).
     */
#ifndef EXEC_BACKEND
    InitProcess();
#endif

    t_thrd.proc_cxt.PostInit->SetDatabaseAndUser(NULL, InvalidOid, NULL);
    t_thrd.proc_cxt.PostInit->InitCatchupWorker();

    SetProcessingMode(NormalProcessing);

    /*
     * Create a resource owner to keep track of our resources (currently only
     * buffer pins).
     */
    t_thrd.utils_cxt.CurrentResourceOwner = ResourceOwnerCreate(NULL, "Catchup",
        THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));

    /*
     * Create a memory context that we will do all our work in.  We do this so
     * that we can reset the context during error bcm read or send and thereby
     * avoid possible memory leaks.  Formerly this code just ran in
     * t_thrd.top_mem_cxt, but resetting that would be a really bad idea.
     */
    catchupContext = AllocSetContextCreate(t_thrd.top_mem_cxt, "Catchup", ALLOCSET_DEFAULT_MINSIZE,
                                           ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
    (void)MemoryContextSwitchTo(catchupContext);

    /* report this backend in the PgBackendStatus array */
    pgstat_bestart();
    pgstat_report_appname("Catchup");
    pgstat_report_activity(STATE_IDLE, NULL);

    /*
     * If an exception is encountered, processing resumes here.
     *
     * See notes in postgres.c about the design of this coding.
     */
    int curTryCounter = 0;
    int *oldTryCounter = NULL;
    if (sigsetjmp(local_sigjmp_buf, 1) != 0) {
        gstrace_tryblock_exit(true, oldTryCounter);

        /* Since not using PG_TRY, must reset error stack by hand */
        t_thrd.log_cxt.error_context_stack = NULL;

        /* Prevent interrupts while cleaning up */
        HOLD_INTERRUPTS();

        /* Forget any pending QueryCancel request */
        t_thrd.int_cxt.QueryCancelPending = false;
        (void)disable_sig_alarm(true);
        t_thrd.int_cxt.QueryCancelPending = false; /* again in case timeout occurred */

        /* Report the error to the server log */
        EmitErrorReport();

        /*
         * when clearing the BCM encounter ERROR, we should ResetBCMArray, or it
         * will enter ClearBCMArray infinite loop, then coredump.
         */
        ResetBCMArray();

        /* Abort the current transaction in order to recover */
        AbortCurrentTransaction();

        /* release resource held by lsc */
        AtEOXact_SysDBCache(false);

        /*
         * Now return to normal top-level context and clear ErrorContext for
         * next time.
         */
        (void)MemoryContextSwitchTo(catchupContext);
        FlushErrorState();

        /* Flush any leaked data in the top-level context */
        MemoryContextResetAndDeleteChildren(catchupContext);

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

        CatchupShutdownIfNoDataSender();
    }
    oldTryCounter = gstrace_tryblock_entry(&curTryCounter);

    catchupState = CATCHUP_NONE;
    /* We can now handle ereport(ERROR) */
    t_thrd.log_cxt.PG_exception_stack = &local_sigjmp_buf;

    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();

    /*
     * When primary receive switchover, it will SIGTERM all Backend thread,
     * include catchup thread, if getBcmFileList get ERROR, it will longjump
     * and enter CatchupShutdown, then datasnd will exit, primary will domote
     * to standby, but some unsync data in bcm file will lost. So we can not
     * respond the SIGTERM signal.
     */
    if (IS_DN_DUMMY_STANDYS_MODE()) {
        bool fullCatchup = false;

        /* If incremental catchup switch is off, we use full catchup. */
        if (!u_sess->attr.attr_storage.enable_incremental_catchup) {
            fullCatchup = true;
        } else {
            fullCatchup = WaitDummyStarts();
            /* Tell the sender who is connecting with dummy standby,
             * to scan local data and get BCM file lists.
             */
            if (!fullCatchup) {
                catchupState = CATCHUP_STARTING;
                fullCatchup = WaitBcmFileList();
            }
        }
        StartTransactionCommand();
        if (!fullCatchup) {
            GetIncrementalBcmFileList();
        } else {
            GetBcmFileList(false);
        }
        CommitTransactionCommand();
    }

    catchupDone = true;
    catchup_online = false;
    catchupState = CATCHUP_NONE;

    pg_memory_barrier();

    /* All done, go away */
    proc_exit(0);
}
