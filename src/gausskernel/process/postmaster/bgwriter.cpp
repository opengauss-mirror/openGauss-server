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

#include "access/xlog_internal.h"
#include "access/double_write.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgwriter.h"
#include "postmaster/pagewriter.h"
#include "storage/buf/bufmgr.h"
#include "storage/ipc.h"
#include "storage/lock/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "storage/smgr/smgr.h"
#include "storage/spin.h"
#include "storage/standby.h"
#include "utils/guc.h"
#include "utils/builtins.h"
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
 * LSN and timestamp at which we last issued a LogStandbySnapshot(), to avoid
 * doing so too often or repeatedly if there has been no other write activity
 * in the system.
 */
static TimestampTz last_snapshot_ts;
static XLogRecPtr last_snapshot_lsn = InvalidXLogRecPtr;

/* Signal handlers */
static void bgwriter_quickdie(SIGNAL_ARGS);
static void bgwriter_sighup_handler(SIGNAL_ARGS);
static void bgwriter_request_shutdown_handler(SIGNAL_ARGS);
static void bgwriter_sigusr1_handler(SIGNAL_ARGS);
extern void write_term_log(uint32 term);

/* incremental checkpoint bgwriter thread function */
const int MAX_THREAD_NAME_LEN = 128;
static void drop_rel_all_forks_buffers();
static void drop_rel_one_fork_buffers();

static void setup_bgwriter_signalhook(void)
{
    /*
     * Reset some signals that are accepted by postmaster but not here
     */
    (void)gspqsignal(SIGHUP, bgwriter_sighup_handler);      /* set flag to read config file */
    (void)gspqsignal(SIGINT, SIG_IGN);
    (void)gspqsignal(SIGTERM, bgwriter_request_shutdown_handler);       /* shutdown */
    (void)gspqsignal(SIGQUIT, bgwriter_quickdie);       /* hard crash time */
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
}

static void bgwriter_handle_exceptions(WritebackContext wb_context, MemoryContext bgwriter_cxt)
{
    /*
     * Close all open files after any error.  This is helpful on Windows,
     * where holding deleted files open causes various strange errors.
     * It's not clear we need it elsewhere, but shouldn't hurt.
     */

    /* Since not using PG_TRY, must reset error stack by hand */
    t_thrd.log_cxt.error_context_stack = NULL;

    t_thrd.log_cxt.call_stack = NULL;

    /* Prevent interrupts while cleaning up */
    HOLD_INTERRUPTS();

    /* Report the error to the server log */
    EmitErrorReport();

    /* abort async io, must before LWlock release */
    AbortAsyncListIO();

    /* release resource held by lsc */
    AtEOXact_SysDBCache(false);

    /*
     * These operations are really just a minimal subset of
     * AbortTransaction().  We don't have very many resources to worry
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
    (void)MemoryContextSwitchTo(bgwriter_cxt);
    FlushErrorState();

    /* Flush any leaked data in the top-level context */
    MemoryContextResetAndDeleteChildren(bgwriter_cxt);

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
    return;
}

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

    setup_bgwriter_signalhook();

    /*
     * We just started, assume there has been either a shutdown or
     * end-of-recovery snapshot.
     */
    last_snapshot_ts = GetCurrentTimestamp();

    /*
     * Create a resource owner to keep track of our resources (currently only
     * buffer pins).
     */
    t_thrd.utils_cxt.CurrentResourceOwner = ResourceOwnerCreate(NULL, "Background Writer",
        THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));
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
        bgwriter_handle_exceptions(wb_context, bgwriter_context);

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

        /*
         * when double write is disabled, pg_dw_meta will be created with dw_file_num = 0, so
         * here is for upgrading process. bgwriter will run when enable_incremetal_checkpoint = off.
         */
        if (pg_atomic_read_u32(&g_instance.dw_batch_cxt.dw_version) < DW_SUPPORT_REABLE_DOUBLE_WRITE
            && t_thrd.proc->workingVersionNum >= DW_SUPPORT_REABLE_DOUBLE_WRITE) {
            dw_upgrade_renable_double_write();
        }

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
static void bgwriter_quickdie(SIGNAL_ARGS)
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
static void bgwriter_sighup_handler(SIGNAL_ARGS)
{
    int save_errno = errno;

    t_thrd.bgwriter_cxt.got_SIGHUP = true;

    if (t_thrd.proc)
        SetLatch(&t_thrd.proc->procLatch);

    errno = save_errno;
}

/* SIGTERM: set flag to shutdown and exit */
static void bgwriter_request_shutdown_handler(SIGNAL_ARGS)
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

/* bgwriter view function */
Datum bgwriter_view_get_node_name()
{
    if (g_instance.attr.attr_common.PGXCNodeName == NULL || g_instance.attr.attr_common.PGXCNodeName[0] == '\0') {
        return CStringGetTextDatum("not define");
    } else {
        return CStringGetTextDatum(g_instance.attr.attr_common.PGXCNodeName);
    }
}

Datum bgwriter_view_get_actual_flush_num()
{
    return Int64GetDatum(0);
}


Datum bgwriter_view_get_last_flush_num()
{
    return Int32GetDatum(0);
}

Datum bgwriter_view_get_candidate_nums()
{
    int candidate_num = get_curr_candidate_nums(true) + get_curr_candidate_nums(false);
    return Int32GetDatum(candidate_num);
}

Datum bgwriter_view_get_num_candidate_list()
{
    return Int64GetDatum(g_instance.ckpt_cxt_ctl->get_buf_num_candidate_list);
}

Datum bgwriter_view_get_num_clock_sweep()
{
    return Int64GetDatum(g_instance.ckpt_cxt_ctl->get_buf_num_clock_sweep);
}

const incre_ckpt_view_col g_bgwriter_view_col[INCRE_CKPT_BGWRITER_VIEW_COL_NUM] = {
    {"node_name", TEXTOID, bgwriter_view_get_node_name},
    {"bgwr_actual_flush_total_num", INT8OID, bgwriter_view_get_actual_flush_num},
    {"bgwr_last_flush_num", INT4OID, bgwriter_view_get_last_flush_num},
    {"candidate_slots", INT4OID, bgwriter_view_get_candidate_nums},
    {"get_buffer_from_list", INT8OID, bgwriter_view_get_num_candidate_list},
    {"get_buf_clock_sweep", INT8OID, bgwriter_view_get_num_clock_sweep}};


const uint THREAD_SLEEP_TIME = 10 * 60 * 1000;
void invalid_buffer_bgwriter_main()
{
    sigjmp_buf	localSigjmpBuf;
    MemoryContext bgwriter_context;
    char name[MAX_THREAD_NAME_LEN] = {0};
    WritebackContext wb_context;
    t_thrd.role = SPBGWRITER;

    setup_bgwriter_signalhook();
    ereport(LOG, (errmsg("invalidate buffer bgwriter started")));

    errno_t err_rc = snprintf_s(name, MAX_THREAD_NAME_LEN, MAX_THREAD_NAME_LEN - 1, "%s", "spbgwriter");
    securec_check_ss(err_rc, "", "");

    /* Create a resource owner to keep track of our resources (currently only buffer pins). */
    t_thrd.utils_cxt.CurrentResourceOwner = ResourceOwnerCreate(NULL, name,
            THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));

    /*
     * Create a memory context that we will do all our work in.  We do this so
     * that we can reset the context during error recovery and thereby avoid
     * possible memory leaks.  Formerly this code just ran in
     * t_thrd.top_mem_cxt, but resetting that would be a really bad idea.
     */
    bgwriter_context = AllocSetContextCreate(t_thrd.top_mem_cxt, name, ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
    MemoryContextSwitchTo(bgwriter_context);

    WritebackContextInit(&wb_context, &u_sess->attr.attr_storage.bgwriter_flush_after);

    if (sigsetjmp(localSigjmpBuf, 1) != 0) {
        ereport(WARNING, (errmsg("invalidate buffer bgwriter exception occured.")));
        bgwriter_handle_exceptions(wb_context, bgwriter_context);
    }

    /* We can now handle ereport(ERROR) */
    t_thrd.log_cxt.PG_exception_stack = &localSigjmpBuf;

    /* Unblock signals (they were blocked when the postmaster forked us) */
    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();

    /* Use the recovery target timeline ID during recovery */
    if (RecoveryInProgress()) {
        t_thrd.xlog_cxt.ThisTimeLineID = GetRecoveryTargetTLI();
    }

    pgstat_report_appname("InvalidBufferBgWriter");
    pgstat_report_activity(STATE_IDLE, NULL);
    g_instance.bgwriter_cxt.invalid_buf_proc_latch = &t_thrd.proc->procLatch;
	/* Loop forever */
    for (;;) {
        int rc;

        if (t_thrd.bgwriter_cxt.got_SIGHUP) {
            t_thrd.bgwriter_cxt.got_SIGHUP = false;
            ProcessConfigFile(PGC_SIGHUP);
        }

        if (t_thrd.bgwriter_cxt.shutdown_requested) {
            ereport(LOG, (errmsg("invalidate buffer bgwriter thread shut down")));
            u_sess->attr.attr_common.ExitOnAnyError = true;
            proc_exit(0);
        }

        rc = WaitLatch(&t_thrd.proc->procLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, THREAD_SLEEP_TIME);
        if (rc & WL_POSTMASTER_DEATH) {
            gs_thread_exit(1);
        }

        /* Clear any already-pending wakeups */
        ResetLatch(&t_thrd.proc->procLatch);
        drop_rel_all_forks_buffers();
        drop_rel_one_fork_buffers();
    }
}

const int HASH_TABLE_ELEMENT_MIN_NUM = 512;
HTAB *relfilenode_hashtbl_create(const char *name, bool use_heap_mem)
{
    HASHCTL hashCtrl;
    HTAB *hashtbl = NULL;
    errno_t rc;

    rc = memset_s(&hashCtrl, sizeof(hashCtrl), 0, sizeof(hashCtrl));
    securec_check(rc, "", "");
    hashCtrl.hcxt = (MemoryContext)CurrentMemoryContext;
    hashCtrl.hash = tag_hash;
    hashCtrl.keysize = sizeof(RelFileNode);
    /* keep entrysize >= keysize, stupid limits */
    hashCtrl.entrysize = sizeof(DelFileTag);

    if (use_heap_mem) {
        hashtbl = HeapMemInitHash(name, HASH_TABLE_ELEMENT_MIN_NUM,
            Max(g_instance.attr.attr_common.max_files_per_process, t_thrd.storage_cxt.max_userdatafiles),  &hashCtrl,
            (HASH_FUNCTION | HASH_ELEM));
        if (hashtbl == NULL) {
            ereport(FATAL, (errmsg("could not initialize unlinik relation hash table")));
        }
    } else {
        hashtbl = hash_create(name, HASH_TABLE_ELEMENT_MIN_NUM, &hashCtrl, (HASH_CONTEXT | HASH_FUNCTION | HASH_ELEM));
    }
    return hashtbl;
}

HTAB *relfilenode_fork_hashtbl_create(const char* name, bool use_heap_mem)
{
    HASHCTL hashCtrl;
    HTAB *hashtbl = NULL;
    errno_t rc;

    rc = memset_s(&hashCtrl, sizeof(hashCtrl), 0, sizeof(hashCtrl));
    securec_check(rc, "", "");
    hashCtrl.hcxt = (MemoryContext)CurrentMemoryContext;
    hashCtrl.hash = tag_hash;
    hashCtrl.keysize = sizeof(ForkRelFileNode);
    /* keep  entrysize >= keysize, stupid limits */
    hashCtrl.entrysize = sizeof(DelForkFileTag);

    if (use_heap_mem) {
        hashtbl = HeapMemInitHash(name, HASH_TABLE_ELEMENT_MIN_NUM,
            Max(g_instance.attr.attr_common.max_files_per_process, t_thrd.storage_cxt.max_userdatafiles),
            &hashCtrl, (HASH_FUNCTION | HASH_ELEM));
        if (hashtbl == NULL) {
            ereport(FATAL, (errmsg("could not initialize unlinik relation hash table")));
        }
    } else {
        hashtbl = hash_create(name, HASH_TABLE_ELEMENT_MIN_NUM, &hashCtrl, (HASH_CONTEXT | HASH_FUNCTION | HASH_ELEM));
    }
    return hashtbl;
}

static void drop_rel_all_forks_buffers()
{
    HASH_SEQ_STATUS status;
    DelFileTag *entry = NULL;
    DelFileTag *temp_entry = NULL;
    bool found = false;
    uint rel_num = 0;
    HTAB *unlink_rel_hashtbl = g_instance.bgwriter_cxt.unlink_rel_hashtbl;
    HTAB *rel_bak = relfilenode_hashtbl_create("unlink_rel_bak", false);

    /* Obtains the entry in hashtable. */
    LWLockAcquire(g_instance.bgwriter_cxt.rel_hashtbl_lock, LW_SHARED);
    hash_seq_init(&status, unlink_rel_hashtbl);
    while ((temp_entry = (DelFileTag *)hash_seq_search(&status)) != NULL) {
        entry = (DelFileTag*)hash_search(rel_bak, (void *)&temp_entry->rnode, HASH_ENTER, &found);
        if (!found) {
            entry->rnode = temp_entry->rnode;
            entry->maxSegNo = temp_entry->maxSegNo;
            rel_num++;
        }
    }
    LWLockRelease(g_instance.bgwriter_cxt.rel_hashtbl_lock);

    if (rel_num > 0) {
        DropRelFileNodeAllBuffersUsingHash(rel_bak);

        hash_seq_init(&status, rel_bak);
        while ((temp_entry = (DelFileTag *)hash_seq_search(&status)) != NULL) {
            if (temp_entry->maxSegNo == -1) {
                ereport(DEBUG1, (errmodule(MOD_INCRE_BG),
                    errmsg("the max segno is -1, skip forget this rel %u/%u/%u, bucketNode is %d",
                        temp_entry->rnode.spcNode, temp_entry->rnode.dbNode, temp_entry->rnode.relNode,
                        temp_entry->rnode.bucketNode)));
                continue;
            }
            for (int32 i = 0; i < temp_entry->maxSegNo; i++) {
                for (int fork_num = 0; fork_num <= (int)MAX_FORKNUM; fork_num++) {
                    md_register_forget_request(temp_entry->rnode, fork_num, i);
                }
            }
            LWLockAcquire(g_instance.bgwriter_cxt.rel_hashtbl_lock, LW_EXCLUSIVE);
            if (hash_search(unlink_rel_hashtbl, (void *)&temp_entry->rnode, HASH_REMOVE, NULL) == NULL) {
                LWLockRelease(g_instance.bgwriter_cxt.rel_hashtbl_lock);
                hash_destroy(rel_bak);
                ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("unlink rel hash table corrupted")));
            } else {
                ereport(DEBUG1, (errmodule(MOD_INCRE_BG),
                    errmsg("invalidate buffer has been finished for rel %u/%u/%u, bucketNode is %d",
                        temp_entry->rnode.spcNode, temp_entry->rnode.dbNode, temp_entry->rnode.relNode,
                        temp_entry->rnode.bucketNode)));
            }
            LWLockRelease(g_instance.bgwriter_cxt.rel_hashtbl_lock);
        }
    }

    hash_destroy(rel_bak);
}

static void drop_rel_one_fork_buffers()
{
    HASH_SEQ_STATUS status;
    DelForkFileTag *entry = NULL;
    DelForkFileTag *temp_entry = NULL;
    bool found = false;
    uint rel_num = 0;
    HTAB *unlink_rel_fork_hashtbl = g_instance.bgwriter_cxt.unlink_rel_fork_hashtbl;
    HTAB *rel_bak = relfilenode_fork_hashtbl_create("unlink_rel_one_fork_bak", false);
    /* Obtains the entry in hashtable. */
    LWLockAcquire(g_instance.bgwriter_cxt.rel_one_fork_hashtbl_lock, LW_SHARED);
    hash_seq_init(&status, unlink_rel_fork_hashtbl);
    while ((temp_entry = (DelForkFileTag *)hash_seq_search(&status)) != NULL) {
        entry = (DelForkFileTag*)hash_search(rel_bak, temp_entry, HASH_ENTER, &found);
        if (!found) {
            entry->forkrnode.rnode.spcNode = temp_entry->forkrnode.rnode.spcNode;
            entry->forkrnode.rnode.dbNode = temp_entry->forkrnode.rnode.dbNode;
            entry->forkrnode.rnode.relNode = temp_entry->forkrnode.rnode.relNode;
            entry->forkrnode.rnode.bucketNode = temp_entry->forkrnode.rnode.bucketNode;
            entry->forkrnode.forkNum = temp_entry->forkrnode.forkNum;
            entry->maxSegNo = temp_entry->maxSegNo;
            rel_num++;
        }
    }
    LWLockRelease(g_instance.bgwriter_cxt.rel_one_fork_hashtbl_lock);

    if (rel_num > 0) {
        DropRelFileNodeOneForkAllBuffersUsingHash(rel_bak);
        hash_seq_init(&status, rel_bak);
        while ((temp_entry = (DelForkFileTag *)hash_seq_search(&status)) != NULL) {
            if (temp_entry->maxSegNo == -1) {
                ereport(DEBUG1, (errmodule(MOD_INCRE_BG),
                errmsg("the max segno is -1, skip forget this rel %u/%u/%u, bucketNode is %d",
                temp_entry->forkrnode.rnode.spcNode, temp_entry->forkrnode.rnode.dbNode,
                temp_entry->forkrnode.rnode.relNode, temp_entry->forkrnode.rnode.bucketNode)));
                continue;
            }
            for (int32 i = 0; i < temp_entry->maxSegNo; i++) {
                md_register_forget_request(temp_entry->forkrnode.rnode, temp_entry->forkrnode.forkNum, i);
            }
            LWLockAcquire(g_instance.bgwriter_cxt.rel_one_fork_hashtbl_lock, LW_EXCLUSIVE);
            if (hash_search(unlink_rel_fork_hashtbl, (void *)temp_entry, HASH_REMOVE, NULL) == NULL) {
                LWLockRelease(g_instance.bgwriter_cxt.rel_one_fork_hashtbl_lock);
                hash_destroy(rel_bak);
                ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("unlink rel one fork hash table corrupted")));
            } else {
                ereport(LOG, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("invalidate buffer has been finished for rel "
                    "%u/%u/%u, bucketNode is %d, forkNum is %d",
                    temp_entry->forkrnode.rnode.spcNode, temp_entry->forkrnode.rnode.dbNode,
                    temp_entry->forkrnode.rnode.relNode, temp_entry->forkrnode.rnode.bucketNode,
                    temp_entry->forkrnode.forkNum)));
            }
            LWLockRelease(g_instance.bgwriter_cxt.rel_one_fork_hashtbl_lock);
        }
    }
    hash_destroy(rel_bak);
}
