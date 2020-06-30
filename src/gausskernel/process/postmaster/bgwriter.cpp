/*
 *
 * bgwriter.cpp
 *
 * The background writer (bgwriter) is new as of Postgres 8.0.	It attempts
 * to keep regular backends from having to write out dirty shared buffers
 * (which they would only do when needing to free a shared buffer to read in
 * another page).  In the best scenario all writes from shared buffers will
 * be issued by the background writer process.	However, regular backends are
 * still empowered to issue writes if the bgwriter fails to maintain enough
 * clean shared buffers.
 *
 * As of Postgres 9.2 the bgwriter no longer handles checkpoints.
 *
 * The bgwriter is started by the postmaster as soon as the startup subprocess
 * finishes, or as soon as recovery begins if we are doing archive recovery.
 * It remains alive until the postmaster commands it to terminate.
 * Normal termination is by SIGTERM, which instructs the bgwriter to exit(0).
 * Emergency termination is by SIGQUIT; like any backend, the bgwriter will
 * simply abort and exit on SIGQUIT.
 *
 * If the bgwriter exits unexpectedly, the postmaster treats that the same
 * as a backend crash: shared memory may be corrupted, so remaining backends
 * should be killed by SIGQUIT and then a recovery cycle started.
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/process/postmaster/bgwriter.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <signal.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#include "access/xlog_internal.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgwriter.h"
#include "storage/bufmgr.h"
#include "storage/buf_internals.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "storage/smgr.h"
#include "storage/spin.h"
#include "storage/standby.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/timestamp.h"
#include "gssignal/gs_signal.h"
#include "replication/slot.h"

/*
 * Multiplier to apply to BgWriterDelay when we decide to hibernate.
 * (Perhaps this needs to be configurable?)
 */
#define HIBERNATE_FACTOR 50

/*
 * Interval in which standby snapshots are logged into the WAL stream, in
 * milliseconds.
 */
#define LOG_SNAPSHOT_INTERVAL_MS 15000

/*
 * LSN and timestamp at which we last issued a LogStandbySnapshot(), to avoid
 * doing so too often or repeatedly if there has been no other write activity
 * in the system.
 */
static TimestampTz last_snapshot_ts;
static XLogRecPtr last_snapshot_lsn = InvalidXLogRecPtr;

/* Signal handlers */
static void bg_quickdie(SIGNAL_ARGS);
static void BgSigHupHandler(SIGNAL_ARGS);
static void ReqShutdownHandler(SIGNAL_ARGS);
static void bgwriter_sigusr1_handler(SIGNAL_ARGS);
extern void write_term_log(uint32 term);

/*
 * Main entry point for bgwriter process
 *
 * This is invoked from AuxiliaryProcessMain, which has already created the
 * basic execution environment, but not enabled signals yet.
 */
void BackgroundWriterMain(void)
{
    sigjmp_buf local_sigjmp_buf;
    MemoryContext bgwriter_context;
    bool prev_hibernate = false;
    WritebackContext wb_context;

    t_thrd.role = BGWRITER;

    ereport(LOG, (errmsg("bgwriter started")));

    /*
     * Properly accept or ignore signals the postmaster might send us.
     *
     * bgwriter doesn't participate in ProcSignal signalling, but a SIGUSR1
     * handler is still needed for latch wakeups.
     */
    (void)gspqsignal(SIGHUP, BgSigHupHandler); /* set flag to read config file */
    (void)gspqsignal(SIGINT, SIG_IGN);
    (void)gspqsignal(SIGTERM, ReqShutdownHandler); /* shutdown */
    (void)gspqsignal(SIGQUIT, bg_quickdie);        /* hard crash time */
    (void)gspqsignal(SIGALRM, SIG_IGN);
    (void)gspqsignal(SIGPIPE, SIG_IGN);
    (void)gspqsignal(SIGUSR1, bgwriter_sigusr1_handler);
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
     * We just started, assume there has been either a shutdown or
     * end-of-recovery snapshot.
     */
    last_snapshot_ts = GetCurrentTimestamp();

    /*
     * Create a resource owner to keep track of our resources (currently only
     * buffer pins).
     */
    t_thrd.utils_cxt.CurrentResourceOwner = ResourceOwnerCreate(NULL, "Background Writer");

    /*
     * Create a memory context that we will do all our work in.  We do this so
     * that we can reset the context during error recovery and thereby avoid
     * possible memory leaks.  Formerly this code just ran in
     * t_thrd.top_mem_cxt, but resetting that would be a really bad idea.
     */
    bgwriter_context = AllocSetContextCreate(t_thrd.top_mem_cxt,
        "Background Writer",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);
    MemoryContextSwitchTo(bgwriter_context);

    WritebackContextInit(&wb_context, &u_sess->attr.attr_storage.bgwriter_flush_after);

    /*
     * If an exception is encountered, processing resumes here.
     *
     * See notes in postgres.c about the design of this coding.
     */
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

        /* abort async io, must before LWlock release */
        AbortAsyncListIO();
        /*
         * These operations are really just a minimal subset of
         * AbortTransaction().	We don't have very many resources to worry
         * about in bgwriter, but we do have LWLocks, buffers, and temp files.
         */
        LWLockReleaseAll();
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
        MemoryContextSwitchTo(bgwriter_context);
        FlushErrorState();

        /* Flush any leaked data in the top-level context */
        MemoryContextResetAndDeleteChildren(bgwriter_context);

        /* re-initialize to avoid repeated errors causing problems */
        WritebackContextInit(&wb_context, &u_sess->attr.attr_storage.bgwriter_flush_after);

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
        /* Report wait end here, when there is no further possibility of wait */
        pgstat_report_waitevent(WAIT_EVENT_END);
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
    prev_hibernate = false;

    pgstat_report_appname("Background writer");
    pgstat_report_activity(STATE_IDLE, NULL);

    /*
     * Loop forever
     */
    for (;;) {
        bool can_hibernate = false;
        int rc;

        /* Clear any already-pending wakeups */
        ResetLatch(&t_thrd.proc->procLatch);

        pgstat_report_activity(STATE_RUNNING, NULL);

        if (t_thrd.bgwriter_cxt.got_SIGHUP) {
            t_thrd.bgwriter_cxt.got_SIGHUP = false;
            ProcessConfigFile(PGC_SIGHUP);
        }

        if (t_thrd.bgwriter_cxt.shutdown_requested) {
            /*
             * From here on, elog(ERROR) should end with exit(1), not send
             * control back to the sigsetjmp block above
             */
            u_sess->attr.attr_common.ExitOnAnyError = true;
            /* Normal exit from the bgwriter is here */
            proc_exit(0); /* done */
        }

        /*
         * Do one cycle of dirty-buffer writing.
         */
        can_hibernate = BgBufferSync(&wb_context);

        /*
         * Send off activity statistics to the stats collector
         */
        pgstat_send_bgwriter();

        if (FirstCallSinceLastCheckpoint()) {
            /*
             * After any checkpoint, close all smgr files.	This is so we
             * won't hang onto smgr references to deleted files indefinitely.
             */
            smgrcloseall();
        }
        /*
         * Log a new xl_running_xacts every now and then so replication can get
         * into a consistent state faster (think of suboverflowed snapshots)
         * and clean up resources (locks, KnownXids*) more frequently. The
         * costs of this are relatively low, so doing it 4 times
         * (LOG_SNAPSHOT_INTERVAL_MS) a minute seems fine.
         *
         * We assume the interval for writing xl_running_xacts is
         * significantly bigger than BgWriterDelay, so we don't complicate the
         * overall timeout handling but just assume we're going to get called
         * often enough even if hibernation mode is active. It's not that
         * important that log_snap_interval_ms is met strictly. To make sure
         * we're not waking the disk up unneccesarily on an idle system we
         * check whether there has been any WAL inserted since the last time
         * we've logged a running xacts.
         *
         * We do this logging in the bgwriter as its the only process thats
         * run regularly and returns to its mainloop all the
         * time. E.g. Checkpointer, when active, is barely ever in its
         * mainloop and thus makes it hard to log regularly.
         */
        if (XLogStandbyInfoActive() && !RecoveryInProgress()) {
            TimestampTz timeout = 0;
            TimestampTz now = GetCurrentTimestamp();
            timeout = TimestampTzPlusMilliseconds(last_snapshot_ts, LOG_SNAPSHOT_INTERVAL_MS);

            /*
             * only log if enough time has passed and some xlog record has been
             * inserted.
             */
            if (now >= timeout && !XLByteEQ(last_snapshot_lsn, GetXLogInsertRecPtr())) {
                last_snapshot_lsn = LogStandbySnapshot();
                last_snapshot_ts = now;
            }
            if (now >= timeout) {
                LogCheckSlot();
            }

            if (now >= timeout) {
                write_term_log(g_instance.comm_cxt.localinfo_cxt.term);
                g_instance.comm_cxt.localinfo_cxt.set_term = true;
            }
        }

        /*
         * Sleep until we are signaled or BgWriterDelay has elapsed.
         *
         * Note: the feedback control loop in BgBufferSync() expects that we
         * will call it every BgWriterDelay msec.  While it's not critical for
         * correctness that that be exact, the feedback loop might misbehave
         * if we stray too far from that.  Hence, avoid loading this process
         * down with latch events that are likely to happen frequently during
         * normal operation.
         */
        pgstat_report_activity(STATE_IDLE, NULL);
        rc = WaitLatch(&t_thrd.proc->procLatch,
            WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
            u_sess->attr.attr_storage.BgWriterDelay /* ms */);

        /*
         * If no latch event and BgBufferSync says nothing's happening, extend
         * the sleep in "hibernation" mode, where we sleep for much longer
         * than bgwriter_delay says.  Fewer wakeups save electricity.  When a
         * backend starts using buffers again, it will wake us up by setting
         * our latch.  Because the extra sleep will persist only as long as no
         * buffer allocations happen, this should not distort the behavior of
         * BgBufferSync's control loop too badly; essentially, it will think
         * that the system-wide idle interval didn't exist.
         *
         * There is a race condition here, in that a backend might allocate a
         * buffer between the time BgBufferSync saw the alloc count as zero
         * and the time we call StrategyNotifyBgWriter.  While it's not
         * critical that we not hibernate anyway, we try to reduce the odds of
         * that by only hibernating when BgBufferSync says nothing's happening
         * for two consecutive cycles.	Also, we mitigate any possible
         * consequences of a missed wakeup by not hibernating forever.
         */
        if (rc == WL_TIMEOUT && can_hibernate && prev_hibernate) {
            /* Ask for notification at next buffer allocation */
            StrategyNotifyBgWriter(t_thrd.proc->pgprocno);
            /* Sleep ... */
            rc = WaitLatch(&t_thrd.proc->procLatch,
                WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
                u_sess->attr.attr_storage.BgWriterDelay * HIBERNATE_FACTOR);
            /* Reset the notification request in case we timed out */
            StrategyNotifyBgWriter(-1);
        }

        /*
         * Emergency bailout if postmaster has died.  This is to avoid the
         * necessity for manual cleanup of all postmaster children.
         */
        if (rc & WL_POSTMASTER_DEATH)
            gs_thread_exit(1);

        prev_hibernate = can_hibernate;
    }
}

/* --------------------------------
 *		signal handler routines
 * --------------------------------
 */
/*
 * bg_quickdie() occurs when signalled SIGQUIT by the postmaster.
 *
 * Some backend has bought the farm,
 * so we need to stop what we're doing and exit.
 */
static void bg_quickdie(SIGNAL_ARGS)
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
static void BgSigHupHandler(SIGNAL_ARGS)
{
    int save_errno = errno;

    t_thrd.bgwriter_cxt.got_SIGHUP = true;

    if (t_thrd.proc)
        SetLatch(&t_thrd.proc->procLatch);

    errno = save_errno;
}

/* SIGTERM: set flag to shutdown and exit */
static void ReqShutdownHandler(SIGNAL_ARGS)
{
    int save_errno = errno;

    t_thrd.bgwriter_cxt.shutdown_requested = true;

    if (t_thrd.proc)
        SetLatch(&t_thrd.proc->procLatch);

    errno = save_errno;
}

/* SIGUSR1: used for latch wakeups */
static void bgwriter_sigusr1_handler(SIGNAL_ARGS)
{
    int save_errno = errno;

    latch_sigusr1_handler();

    errno = save_errno;
}

bool IsBgwriterProcess(void)
{
    return (t_thrd.role == BGWRITER);
}
