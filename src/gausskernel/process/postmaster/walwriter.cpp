/* -------------------------------------------------------------------------
 *
 * walwriter.cpp
 *
 * The WAL writer background process is new as of Postgres 8.3.  It attempts
 * to keep regular backends from having to write out (and fsync) WAL pages.
 * Also, it guarantees that transaction commit records that weren't synced
 * to disk immediately upon commit (ie, were "asynchronously committed")
 * will reach disk within a knowable time --- which, as it happens, is at
 * most three times the wal_writer_delay cycle time.
 *
 * Note that as with the bgwriter for shared buffers, regular backends are
 * still empowered to issue WAL writes and fsyncs when the walwriter doesn't
 * keep up. This means that the WALWriter is not an essential process and
 * can shutdown quickly when requested.
 *
 * Because the walwriter's cycle is directly linked to the maximum delay
 * before async-commit transactions are guaranteed committed, it's probably
 * unwise to load additional functionality onto it.  For instance, if you've
 * got a yen to create xlog segments further in advance, that'd be better done
 * in bgwriter than in walwriter.
 *
 * The walwriter is started by the postmaster as soon as the startup subprocess
 * finishes.  It remains alive until the postmaster commands it to terminate.
 * Normal termination is by SIGTERM, which instructs the walwriter to exit(0).
 * Emergency termination is by SIGQUIT; like any backend, the walwriter will
 * simply abort and exit on SIGQUIT.
 *
 * If the walwriter exits unexpectedly, the postmaster treats that the same
 * as a backend crash: shared memory may be corrupted, so remaining backends
 * should be killed by SIGQUIT and then a recovery cycle started.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/gausskernel/process/postmaster/walwriter.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <signal.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#include "access/xlog.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/walwriter.h"
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
 * Number of do-nothing loops before lengthening the delay time, and the
 * multiplier to apply to WalWriterDelay when we do decide to hibernate.
 * (Perhaps these need to be configurable?)
 */
#define LOOPS_UNTIL_HIBERNATE 50
#define HIBERNATE_FACTOR 25

typedef struct WALCallbackItem {
    struct WALCallbackItem* next;
    WALCallback callback;
    void* arg;
} WALCallbackItem;

static WALCallbackItem* wal_callback = NULL;

/* Signal handlers */
static void wal_quickdie(SIGNAL_ARGS);
static void WalSigHupHandler(SIGNAL_ARGS);
static void WalShutdownHandler(SIGNAL_ARGS);
static void walwriter_sigusr1_handler(SIGNAL_ARGS);

/*
 * Main entry point for walwriter process
 *
 * This is invoked from AuxiliaryProcessMain, which has already created the
 * basic execution environment, but not enabled signals yet.
 */
void WalWriterMain(void)
{
    sigjmp_buf local_sigjmp_buf;
    MemoryContext walwriter_context;
    int left_till_hibernate;
    bool hibernating = false;
    sigset_t oldSigMask;

    ereport(LOG, (errmsg("walwriter started")));

    /*
     * Properly accept or ignore signals the postmaster might send us
     *
     * We have no particular use for SIGINT at the moment, but seems
     * reasonable to treat like SIGTERM.
     */
    /*
     * Reset some signals that are accepted by postmaster but not here
     */
    (void)gspqsignal(SIGHUP, WalSigHupHandler);    /* set flag to read config file */
    (void)gspqsignal(SIGINT, WalShutdownHandler);  /* request shutdown */
    (void)gspqsignal(SIGTERM, WalShutdownHandler); /* request shutdown */
    (void)gspqsignal(SIGQUIT, wal_quickdie);       /* hard crash time */
    (void)gspqsignal(SIGALRM, SIG_IGN);
    (void)gspqsignal(SIGPIPE, SIG_IGN);
    (void)gspqsignal(SIGUSR1, walwriter_sigusr1_handler);
    (void)gspqsignal(SIGUSR2, SIG_IGN); /* not used */

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
     * Create a resource owner to keep track of our resources (not clear that
     * we need this, but may as well have one).
     */
    t_thrd.utils_cxt.CurrentResourceOwner = ResourceOwnerCreate(NULL, "Wal Writer");

    /*
     * Create a memory context that we will do all our work in.  We do this so
     * that we can reset the context during error recovery and thereby avoid
     * possible memory leaks.  Formerly this code just ran in
     * t_thrd.top_mem_cxt, but resetting that would be a really bad idea.
     */
    walwriter_context = AllocSetContextCreate(t_thrd.top_mem_cxt,
        "Wal Writer",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);
    (void)MemoryContextSwitchTo(walwriter_context);

    /*
     * If an exception is encountered, processing resumes here.
     *
     * This code is heavily based on bgwriter.c, q.v.
     */
    int curTryCounter;
    int* oldTryCounter = NULL;
    if (sigsetjmp(local_sigjmp_buf, 1) != 0) {
        gstrace_tryblock_exit(true, oldTryCounter);

        // We need restore the signal mask of current thread
        //
        pthread_sigmask(SIG_SETMASK, &oldSigMask, NULL);

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
         * about in walwriter, but we do have LWLocks, and perhaps buffers?
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
        (void)MemoryContextSwitchTo(walwriter_context);
        FlushErrorState();

        /* Flush any leaked data in the top-level context */
        MemoryContextResetAndDeleteChildren(walwriter_context);

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

    /* We can now handle ereport(ERROR) */
    t_thrd.log_cxt.PG_exception_stack = &local_sigjmp_buf;

    /*
     * Unblock signals (they were blocked when the postmaster forked us)
     */
    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();

    /*
     * Use the recovery target timeline ID during recovery
     */
    if (RecoveryInProgress())
        t_thrd.xlog_cxt.ThisTimeLineID = GetRecoveryTargetTLI();

    /*
     * Reset hibernation state after any error.
     */
    left_till_hibernate = LOOPS_UNTIL_HIBERNATE;
    hibernating = false;
    SetWalWriterSleeping(false);

    /*
     * Advertise our latch that backends can use to wake us up while we're
     * sleeping.
     */
    g_instance.proc_base->walwriterLatch = &t_thrd.proc->procLatch;

    pgstat_report_appname("Wal Writer");
    pgstat_report_activity(STATE_IDLE, NULL);

    /*
     * Loop forever
     */
    for (;;) {
        long cur_timeout;
        int rc;

        /*
         * Advertise whether we might hibernate in this cycle.	We do this
         * before resetting the latch to ensure that any async commits will
         * see the flag set if they might possibly need to wake us up, and
         * that we won't miss any signal they send us.  (If we discover work
         * to do in the last cycle before we would hibernate, the global flag
         * will be set unnecessarily, but little harm is done.)  But avoid
         * touching the global flag if it doesn't need to change.
         */
        if (hibernating != (bool)(left_till_hibernate <= 1)) {
            hibernating = (left_till_hibernate <= 1);
            SetWalWriterSleeping(hibernating);
        }

        /* Clear any already-pending wakeups */
        ResetLatch(&t_thrd.proc->procLatch);

        pgstat_report_activity(STATE_RUNNING, NULL);

        /*
         * Process any requests or signals received recently.
         */
        if (t_thrd.walwriter_cxt.got_SIGHUP) {
            t_thrd.walwriter_cxt.got_SIGHUP = false;
            ProcessConfigFile(PGC_SIGHUP);
        }

        if (t_thrd.walwriter_cxt.shutdown_requested) {
            /* Normal exit from the walwriter is here */
            proc_exit(0); /* done */
        }

        /* execute callbacks (i.e. write data from MOT) */
        CallWALCallback();

        /*
         * Do what we're here for; then, if XLogBackgroundFlush() found useful
         * work to do, reset hibernation counter.
         */
        if (XLogBackgroundFlush()) {
            left_till_hibernate = LOOPS_UNTIL_HIBERNATE;
        } else if (left_till_hibernate > 0) {
            left_till_hibernate--;
        }

        /*
         * Sleep until we are signaled or WalWriterDelay has elapsed.  If we
         * haven't done anything useful for quite some time, lengthen the
         * sleep time so as to reduce the server's idle power consumption.
         */
        if (left_till_hibernate > 0) {
            cur_timeout = u_sess->attr.attr_storage.WalWriterDelay; /* in ms */
        } else {
            cur_timeout = u_sess->attr.attr_storage.WalWriterDelay * HIBERNATE_FACTOR;
        }

        pgstat_report_activity(STATE_IDLE, NULL);
        rc = WaitLatch(&t_thrd.proc->procLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, cur_timeout);

        /* r
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
static void wal_quickdie(SIGNAL_ARGS)
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
static void WalSigHupHandler(SIGNAL_ARGS)
{
    int save_errno = errno;

    t_thrd.walwriter_cxt.got_SIGHUP = true;

    if (t_thrd.proc)
        SetLatch(&t_thrd.proc->procLatch);

    errno = save_errno;
}

/* SIGTERM: set flag to exit normally */
static void WalShutdownHandler(SIGNAL_ARGS)
{
    int save_errno = errno;

    t_thrd.walwriter_cxt.shutdown_requested = true;

    if (t_thrd.proc)
        SetLatch(&t_thrd.proc->procLatch);

    errno = save_errno;
}

/* SIGUSR1: used for latch wakeups */
static void walwriter_sigusr1_handler(SIGNAL_ARGS)
{
    int save_errno = errno;

    latch_sigusr1_handler();

    errno = save_errno;
}

void RegisterWALCallback(WALCallback callback, void* arg)
{
    WALCallbackItem* item;

    item = (WALCallbackItem*)MemoryContextAlloc(g_instance.instance_context, sizeof(WALCallbackItem));
    item->callback = callback;
    item->arg = arg;
    item->next = wal_callback;
    wal_callback = item;
}

void CallWALCallback()
{
    WALCallbackItem* item;

    for (item = wal_callback; item; item = item->next) {
        (*item->callback) (item->arg);
    }
}
