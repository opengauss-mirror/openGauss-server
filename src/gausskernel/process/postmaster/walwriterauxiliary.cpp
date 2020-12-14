/* -------------------------------------------------------------------------
 *
 * walwriterauxiliary.cpp
 *
 * The WAL writer auxiliary background process. It creates and zeros xlog segment
 * files in advance so that walwriterauxiliary thread can directly flush xlog record
 * into well-prepared files.
 * The walwriterauxiliary is started by the postmaster as soon as the startup subprocess
 * finishes.  It remains alive until the postmaster commands it to terminate.
 * Normal termination is by SIGTERM, which instructs the walwriterauxiliary to exit(0).
 * Emergency termination is by SIGQUIT; like any backend, the walwriterauxiliary will
 * simply abort and exit on SIGQUIT.
 *
 *
 * If the walwriterauxiliary exits unexpectedly, the postmaster treats that the same
 * as a backend crash: shared memory may be corrupted, so remaining backends
 * should be killed by SIGQUIT and then a recovery cycle started.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/postmaster/walwriterauxiliary.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <signal.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>
#include <utmpx.h>
#include <errno.h>

#ifdef __USE_NUMA
#include <numa.h>
#endif

#include "postgres.h"
#include "access/xlog.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/walwriterauxiliary.h"
#include "storage/bufmgr.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/smgr.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/resowner.h"

#include "gssignal/gs_signal.h"

/*
 * GUC parameters
 */
THR_LOCAL int WalWriterAuxiliaryDelay = 1000; /* (in ms) */
/*
 * Number of do-nothing loops before lengthening the delay time, and the
 * multiplier to apply to WalWriterDelay when we do decide to hibernate.
 * (Perhaps these need to be configurable?)
 */
#define LOOPS_UNTIL_HIBERNATE 50
#define HIBERNATE_FACTOR 25

/* Signal handlers */
static void walwriterauxiliary_quickdie(SIGNAL_ARGS);
static void WalwriterauxiliarySigHupHandler(SIGNAL_ARGS);
static void WalwriterauxiliaryShutdownHandler(SIGNAL_ARGS);
static void walwriterauxiliary_sigusr1_handler(SIGNAL_ARGS);

/*
 * Main entry point for walwriterauxiliary process
 *
 * This is invoked from AuxiliaryProcessMain, which has already created the
 * basic execution environment, but not enabled signals yet.
 */
void WalWriterAuxiliaryMain(void)
{
    sigjmp_buf local_sigjmp_buf;
    MemoryContext walwriterauxiliary_context;
    sigset_t old_sig_mask;
    t_thrd.role = WALWRITERAUXILIARY;
    ereport(LOG, (errmsg("walwriterauxiliary started")));
    /*
     * Properly accept or ignore signals the postmaster might send us
     *
     * We have no particular use for SIGINT at the moment, but seems
     * reasonable to treat like SIGTERM.
     *
     * Reset some signals that are accepted by postmaster but not here.
     */
    (void)gspqsignal(SIGHUP, WalwriterauxiliarySigHupHandler); /* set flag to read config file */
    (void)gspqsignal(SIGINT, WalwriterauxiliaryShutdownHandler); /* request shutdown */
    (void)gspqsignal(SIGTERM, WalwriterauxiliaryShutdownHandler); /* request shutdown */
    (void)gspqsignal(SIGQUIT, walwriterauxiliary_quickdie); /* hard crash time */
    (void)gspqsignal(SIGALRM, SIG_IGN);
    (void)gspqsignal(SIGPIPE, SIG_IGN);
    (void)gspqsignal(SIGUSR1, walwriterauxiliary_sigusr1_handler);
    (void)gspqsignal(SIGUSR2, SIG_IGN); /* not used */
    (void)gspqsignal(SIGCHLD, SIG_DFL);
    (void)gspqsignal(SIGTTIN, SIG_DFL);
    (void)gspqsignal(SIGTTOU, SIG_DFL);
    (void)gspqsignal(SIGCONT, SIG_DFL);
    (void)gspqsignal(SIGWINCH, SIG_DFL);

    /* We allow SIGQUIT (quickdie) at all times */
    sigdelset(&t_thrd.libpq_cxt.BlockSig, SIGQUIT);

    /*
     * Create a resource owner to keep track of our resources (not clear that
     * we need this, but may as well have one).
     */
    t_thrd.utils_cxt.CurrentResourceOwner = ResourceOwnerCreate(NULL, "Wal Writer Auxiliary");

    /*
     * Create a memory context that we will do all our work in.  We do this so
     * that we can reset the context during error recovery and thereby avoid
     * possible memory leaks.  Formerly this code just ran in
     * t_thrd.top_mem_cxt, but resetting that would be a really bad idea.
     */
    walwriterauxiliary_context = AllocSetContextCreate(t_thrd.top_mem_cxt,
        "Wal Writer Auxiliary",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);
    MemoryContextSwitchTo(walwriterauxiliary_context);

    /*
     * If an exception is encountered, processing resumes here.
     *
     * This code is heavily based on bgwriter.c, q.v.
     */
    if (sigsetjmp(local_sigjmp_buf, 1) != 0) {
        /* We need restore the signal mask of current thread */
        pthread_sigmask(SIG_SETMASK, &old_sig_mask, NULL);
        /* Since not using PG_TRY, must reset error stack by hand */
        t_thrd.log_cxt.error_context_stack = NULL;

        /* Prevent interrupts while cleaning up */
        HOLD_INTERRUPTS();

        /* Report the error to the server log */
        EmitErrorReport();

        /* abort async io, must before LWlock release */
        AbortAsyncListIO();

        /*
         * These operations are really just a minimal subset of
         * AbortTransaction().	We don't have very many resources to worry
         * about in walwriterauxiliary, but we do have LWLocks, and perhaps buffers?
         */
        LWLockReleaseAll();
        pgstat_report_waitevent(WAIT_EVENT_END);
        AbortBufferIO();
        UnlockBuffers();
        /* buffer pins are released here: */
        ResourceOwnerRelease(t_thrd.utils_cxt.CurrentResourceOwner, RESOURCE_RELEASE_BEFORE_LOCKS, false, true);
        /* we needn't bother with the other ResourceOwnerRelease phases */
        AtEOXact_Buffers(false);
        AtEOXact_SMgr();
        AtEOXact_Files();
        AtEOXact_HashTables(false);

        /*
         * Now return to normal top-level context and clear ErrorContext for
         * next time.
         */
        MemoryContextSwitchTo(walwriterauxiliary_context);
        FlushErrorState();

        /* Flush any leaked data in the top-level context */
        MemoryContextResetAndDeleteChildren(walwriterauxiliary_context);

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

    /* We can now handle ereport(ERROR) */
    t_thrd.log_cxt.PG_exception_stack = &local_sigjmp_buf;

    /*
     * Unblock signals (they were blocked when the postmaster forked us)
     */
    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();

    /*
     * Advertise our latch that backends can use to wake us up while we're
     * sleeping.
     */
    g_instance.proc_base->walwriterauxiliaryLatch = &t_thrd.proc->procLatch;

    pgstat_report_appname("Wal Writer Auxiliary");
    pgstat_report_activity(STATE_IDLE, NULL);

    /*
     * Loop forever
     */
    for (;;) {
        long cur_timeout = 0;
        int  rc = 0;

        if (g_instance.wal_cxt.isWalWriterUp) {
            PGSemaphoreLock(&g_instance.wal_cxt.walInitSegLock->l.sem, true);
        }

        /* Clear any already-pending wakeups */
        ResetLatch(&t_thrd.proc->procLatch);

        /*
         * Process any requests or signals received recently.
         */
        if (t_thrd.walwriterauxiliary_cxt.got_SIGHUP) {
            t_thrd.walwriterauxiliary_cxt.got_SIGHUP = false;
            ProcessConfigFile(PGC_SIGHUP);
        }

        if (t_thrd.walwriterauxiliary_cxt.shutdown_requested) {
            /* Normal exit from the walwriterauxiliary is here. */
            proc_exit(0); /* done */
        }

        XLogMultiFileInit(g_instance.attr.attr_storage.wal_file_init_num);

        if (cur_timeout > 0) {
            rc = WaitLatch(&t_thrd.proc->procLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, cur_timeout);
        }

        /*
         * Emergency bailout if postmaster has died.  This is to avoid the
         * necessity for manual cleanup of all postmaster children.
         */
        if (rc & WL_POSTMASTER_DEATH) {
            gs_thread_exit(1);
        }
    }
}

/* --------------------------------
 *		signal handler routines
 * --------------------------------
 */

/*
 * wal_quickdie() occurs when signalled SIGQUIT by the postmaster.
 *
 * Some backend has bought the farm,
 * so we need to stop what we're doing and exit.
 */
static void walwriterauxiliary_quickdie(SIGNAL_ARGS)
{
    gs_signal_setmask(&t_thrd.libpq_cxt.BlockSig, NULL);

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

/* SIGHUP: set flag to re-read config file at next convenient time */
static void WalwriterauxiliarySigHupHandler(SIGNAL_ARGS)
{
    int save_errno = errno;

    t_thrd.walwriterauxiliary_cxt.got_SIGHUP = true;

    if (t_thrd.proc)
        SetLatch(&t_thrd.proc->procLatch);

    errno = save_errno;
}

/* SIGTERM: set flag to exit normally */
static void WalwriterauxiliaryShutdownHandler(SIGNAL_ARGS)
{
    int save_errno = errno;

    t_thrd.walwriterauxiliary_cxt.shutdown_requested = true;

    if (t_thrd.proc)
        SetLatch(&t_thrd.proc->procLatch);

    errno = save_errno;
}

/* SIGUSR1: used for latch wakeups */
static void walwriterauxiliary_sigusr1_handler(SIGNAL_ARGS)
{
    int save_errno = errno;

    latch_sigusr1_handler();

    errno = save_errno;
}