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
#include "storage/smgr.h"
#include "storage/spin.h"
#include "storage/standby.h"
#include "utils/guc.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/timestamp.h"
#include "gssignal/gs_signal.h"
#include "replication/slot.h"

#define MIN(A, B) ((B) < (A) ? (B) : (A))
#define MAX(A, B) ((B) > (A) ? (B) : (A))


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
static void bgwriter_quickdie(SIGNAL_ARGS);
static void bgwriter_sighup_handler(SIGNAL_ARGS);
static void bgwriter_request_shutdown_handler(SIGNAL_ARGS);
static void bgwriter_sigusr1_handler(SIGNAL_ARGS);
extern void write_term_log(uint32 term);

/* incremental checkpoint bgwriter thread function */
const int MAX_THREAD_NAME_LEN = 128;
const int MILLISECOND_TO_MICROSECOND = 1000;
#define FULL_CKPT g_instance.ckpt_cxt_ctl->flush_all_dirty_page

static void candidate_buf_push(int buf_id, int thread_id);
static int64 get_thread_candidate_nums(int thread_id);
static uint32 get_candidate_buf(bool *contain_hashbucket);
static uint32 get_buf_form_dirty_queue(bool *contain_hashbucket);

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

    /*
     * Close all open files after any error.  This is helpful on Windows,
     * where holding deleted files open causes various strange errors.
     * It's not clear we need it elsewhere, but shouldn't hurt.
     */
    smgrcloseall();
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
    t_thrd.utils_cxt.CurrentResourceOwner = ResourceOwnerCreate(NULL, "Background Writer", MEMORY_CONTEXT_STORAGE);

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
                uint32 term_cur = Max(g_instance.comm_cxt.localinfo_cxt.term_from_file,
				    g_instance.comm_cxt.localinfo_cxt.term_from_xlog);
				write_term_log(term_cur);
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
    return Int64GetDatum(g_instance.bgwriter_cxt.bgwriter_actual_total_flush);
}

Datum bgwriter_view_get_last_flush_num()
{
    int last_flush_num = 0;
    for (int i = 0; i < g_instance.bgwriter_cxt.bgwriter_num; i++) {
        if (g_instance.bgwriter_cxt.bgwriter_procs[i].proc != NULL) {
            last_flush_num += g_instance.bgwriter_cxt.bgwriter_procs[i].thread_last_flush;
        }
    }
    return Int32GetDatum(last_flush_num);
}

Datum bgwriter_view_get_candidate_nums()
{
    int candidate_num = get_curr_candidate_nums();
    return Int32GetDatum(candidate_num);
}

Datum bgwriter_view_get_num_candidate_list()
{
    return Int64GetDatum(g_instance.bgwriter_cxt.get_buf_num_candidate_list);
}

Datum bgwriter_view_get_num_clock_sweep()
{
    return Int64GetDatum(g_instance.bgwriter_cxt.get_buf_num_clock_sweep);
}

const incre_ckpt_view_col g_bgwriter_view_col[INCRE_CKPT_BGWRITER_VIEW_COL_NUM] = {
    {"node_name", TEXTOID, bgwriter_view_get_node_name},
    {"bgwr_actual_flush_total_num", INT8OID, bgwriter_view_get_actual_flush_num},
    {"bgwr_last_flush_num", INT4OID, bgwriter_view_get_last_flush_num},
    {"candidate_slots", INT4OID, bgwriter_view_get_candidate_nums},
    {"get_buffer_from_list", INT8OID, bgwriter_view_get_num_candidate_list},
    {"get_buf_clock_sweep", INT8OID, bgwriter_view_get_num_clock_sweep}};

uint32 incre_ckpt_bgwriter_flush_dirty_page(WritebackContext wb_context, int thread_id,
    const CkptSortItem *dirty_buf_list, int start, int batch_num)
{
    uint32 num_actual_flush = 0;
    uint32 candidates = 0;
    int buf_id_start = g_instance.bgwriter_cxt.bgwriter_procs[thread_id].buf_id_start;
    int buf_id_end = buf_id_start + g_instance.bgwriter_cxt.bgwriter_procs[thread_id].cand_list_size;

    for (int i = start; i < start + batch_num; i++) {
        uint32 buf_state;
        uint32 sync_state;
        BufferDesc *buf_desc = NULL;
        int buf_id = dirty_buf_list[i].buf_id;

        if (buf_id == DW_INVALID_BUFFER_ID) {
            continue;
        }
        buf_desc = GetBufferDescriptor(buf_id);
        buf_state = LockBufHdr(buf_desc);

        if ((buf_state & BM_CHECKPOINT_NEEDED) && (buf_state & BM_DIRTY)) {
            UnlockBufHdr(buf_desc, buf_state);

            sync_state = SyncOneBuffer(buf_id, false, &wb_context, true);
            if (!(sync_state & BUF_WRITTEN)) {
                clean_buf_need_flush_flag(buf_desc);
            } else {
                num_actual_flush++;
            }

            if (buf_id >= buf_id_start && buf_id < buf_id_end) {
                buf_state = pg_atomic_read_u32(&buf_desc->state);
                if (BUF_STATE_GET_REFCOUNT(buf_state) > 0) {
                    continue;
                }
                if (g_instance.bgwriter_cxt.candidate_free_map[buf_id] == false) {
                    buf_state = LockBufHdr(buf_desc);
                    if (g_instance.bgwriter_cxt.candidate_free_map[buf_id] == false) {
                        if (BUF_STATE_GET_REFCOUNT(buf_state) == 0 &&
                            !(buf_state & BM_DIRTY)) {
                            candidate_buf_push(buf_id, thread_id);
                            candidates++;
                            g_instance.bgwriter_cxt.candidate_free_map[buf_id] = true;
                        }
                    }
                    UnlockBufHdr(buf_desc, buf_state);
                }
            }
        } else {
            buf_state &= (~BM_CHECKPOINT_NEEDED);
            UnlockBufHdr(buf_desc, buf_state);
        }
    }
    return num_actual_flush;
}

void incre_ckpt_bgwriter_flush_page_batch(WritebackContext wb_context, uint32 need_flush_num,
    bool contain_hashbucket)
{
    int thread_id = t_thrd.bgwriter_cxt.thread_id;
    BgWriterProc *bgwriter = &g_instance.bgwriter_cxt.bgwriter_procs[thread_id];
    CkptSortItem *dirty_buf_list = bgwriter->dirty_buf_list;
    int dw_batch_page_max = GET_DW_DIRTY_PAGE_MAX(contain_hashbucket);
    int runs = (need_flush_num + dw_batch_page_max - 1) / dw_batch_page_max;
    int num_actual_flush = 0;

    qsort(dirty_buf_list, need_flush_num, sizeof(CkptSortItem), ckpt_buforder_comparator);
    ResourceOwnerEnlargeBuffers(t_thrd.utils_cxt.CurrentResourceOwner);

    /* Double write can only handle at most DW_DIRTY_PAGE_MAX at one time. */
    for (int i = 0; i < runs; i++) {
        /* Last batch, take the rest of the buffers */
        int offset = i * dw_batch_page_max;
        int batch_num = (i == runs - 1) ? (need_flush_num - offset) : dw_batch_page_max;
        uint32 flush_num;

        bgwriter->thrd_dw_cxt.contain_hashbucket = contain_hashbucket;
        bgwriter->thrd_dw_cxt.dw_page_idx = -1;
        dw_perform_batch_flush(batch_num, dirty_buf_list + offset, &bgwriter->thrd_dw_cxt);
        flush_num = incre_ckpt_bgwriter_flush_dirty_page(wb_context, thread_id, dirty_buf_list, offset, batch_num);
        bgwriter->thrd_dw_cxt.dw_page_idx = -1;
        num_actual_flush += flush_num;
    }
    bgwriter->thread_last_flush = num_actual_flush;
    (void)pg_atomic_fetch_add_u64(&g_instance.bgwriter_cxt.bgwriter_actual_total_flush, num_actual_flush);
    smgrcloseall();
}

void candidate_buf_init(void)
{
    bool found_candidate_buf = false;
    bool found_candidate_fm = false;
    int buffer_num = g_instance.attr.attr_storage.NBuffers;

    /* 
     * Each thread manages a part of the buffer. Several slots are reserved to 
     * prevent the thread first and last slots equals. 
     */
    g_instance.bgwriter_cxt.candidate_buffers = (Buffer *)
        ShmemInitStruct("CandidateBuffers", buffer_num * sizeof(Buffer), &found_candidate_buf);
    g_instance.bgwriter_cxt.candidate_free_map = (bool *)
        ShmemInitStruct("CandidateFreeMap", buffer_num * sizeof(bool), &found_candidate_fm);

    if (found_candidate_buf || found_candidate_fm) {
        Assert(found_candidate_buf && found_candidate_fm);
    } else {
        errno_t rc;
        rc = memset_s(g_instance.bgwriter_cxt.candidate_buffers, buffer_num * sizeof(Buffer),
            -1, buffer_num * sizeof(Buffer));
        rc = memset_s(g_instance.bgwriter_cxt.candidate_free_map, buffer_num * sizeof(bool),
            false, buffer_num * sizeof(bool));
        securec_check(rc, "", "");
        
        if (g_instance.bgwriter_cxt.bgwriter_procs != NULL) {
            int thread_num = g_instance.bgwriter_cxt.bgwriter_num;
            int avg_num = g_instance.attr.attr_storage.NBuffers / thread_num;
            for (int i = 0; i < thread_num; i++) {
                int start = avg_num * i;
                int end = start + avg_num;
                if (i == thread_num - 1) {
                    end += g_instance.attr.attr_storage.NBuffers % thread_num;
                }
                g_instance.bgwriter_cxt.bgwriter_procs[i].buf_id_start = start;
                g_instance.bgwriter_cxt.bgwriter_procs[i].cand_list_size = end - start;
                g_instance.bgwriter_cxt.bgwriter_procs[i].cand_buf_list =
                    &g_instance.bgwriter_cxt.candidate_buffers[start];
                g_instance.bgwriter_cxt.bgwriter_procs[i].head = 0;
                g_instance.bgwriter_cxt.bgwriter_procs[i].tail = 0;
            }
        }
    }
}

const int MAX_BGWRITER_FLUSH_NUM = 1000 * DW_DIRTY_PAGE_MAX_FOR_NOHBK; 
void incre_ckpt_bgwriter_cxt_init()
{
    MemoryContext oldcontext = MemoryContextSwitchTo(g_instance.increCheckPoint_context);
    int thread_num = g_instance.attr.attr_storage.bgwriter_thread_num;

    thread_num = thread_num > 0 ? thread_num : 1;
    g_instance.bgwriter_cxt.bgwriter_num = thread_num;
    g_instance.bgwriter_cxt.bgwriter_procs = (BgWriterProc *)palloc0(sizeof(BgWriterProc) * thread_num);

    uint32 dirty_list_size = MAX_BGWRITER_FLUSH_NUM / thread_num;
    int avg_num = g_instance.attr.attr_storage.NBuffers / thread_num;
    for (int i = 0; i < thread_num; i++) {
        int start = avg_num * i;
        int end = start + avg_num;
        if (i == thread_num - 1) {
            end += g_instance.attr.attr_storage.NBuffers % thread_num;
        }
        g_instance.bgwriter_cxt.bgwriter_procs[i].buf_id_start = start;
        g_instance.bgwriter_cxt.bgwriter_procs[i].cand_list_size = end - start;
        g_instance.bgwriter_cxt.bgwriter_procs[i].cand_buf_list = &g_instance.bgwriter_cxt.candidate_buffers[start];

        /* bgwriter thread dw cxt init */
        char *unaligned_buf = (char*)palloc0((DW_BUF_MAX_FOR_NOHBK + 1) * BLCKSZ);
        g_instance.bgwriter_cxt.bgwriter_procs[i].thrd_dw_cxt.dw_buf = (char*)TYPEALIGN(BLCKSZ, unaligned_buf);
        g_instance.bgwriter_cxt.bgwriter_procs[i].thrd_dw_cxt.dw_page_idx = -1;
        g_instance.bgwriter_cxt.bgwriter_procs[i].thrd_dw_cxt.contain_hashbucket = false;
        g_instance.bgwriter_cxt.bgwriter_procs[i].dirty_list_size = dirty_list_size;
        g_instance.bgwriter_cxt.bgwriter_procs[i].dirty_buf_list =
            (CkptSortItem *)palloc0(dirty_list_size * sizeof(CkptSortItem));
    }
    (void)MemoryContextSwitchTo(oldcontext);
}

static void incre_ckpt_bgwriter_kill(int code, Datum arg)
{
    int id = t_thrd.bgwriter_cxt.thread_id;
    Assert(id >= 0 && id < g_instance.bgwriter_cxt.bgwriter_num);

    /* Making sure that we mark our exit status */
    g_instance.bgwriter_cxt.bgwriter_procs[id].thrd_dw_cxt.dw_page_idx = -1;

    /* Decrements the current number of active bgwriter and reset it's PROC pointer. */
    (void)pg_atomic_fetch_sub_u32(&g_instance.bgwriter_cxt.curr_bgwriter_num, 1);
    g_instance.bgwriter_cxt.bgwriter_procs[id].proc = NULL;
    return;
}

static int64 get_bgwriter_sleep_time()
{
    uint64 now;
    int64 time_diff;
    int thread_id = t_thrd.bgwriter_cxt.thread_id;

    /* If primary instance do full checkpoint and not the first bgwriter thread, can scan the dirty
     * page queue, help the pagewriter thread finish the dirty page flush.
     */
    if (FULL_CKPT && !RecoveryInProgress() && thread_id > 0) {
        return 0;
    }

    now = get_time_ms();
    if (t_thrd.bgwriter_cxt.next_flush_time > now) {
        time_diff = t_thrd.bgwriter_cxt.next_flush_time - now;
    } else {
        time_diff = 0;
    }
    time_diff = MIN(time_diff, u_sess->attr.attr_storage.BgWriterDelay);
    return time_diff;
}

/**
 * @Description: incremental checkpoint buffer writer main function
 */
void incre_ckpt_background_writer_main(void)
{
    sigjmp_buf	localSigjmpBuf;
    MemoryContext bgwriter_context;
    char name[MAX_THREAD_NAME_LEN] = {0};
    WritebackContext wb_context;
    int thread_id = t_thrd.bgwriter_cxt.thread_id;
    BgWriterProc *bgwriter = &g_instance.bgwriter_cxt.bgwriter_procs[thread_id];
    uint64 now;

    t_thrd.role = BGWRITER;

    setup_bgwriter_signalhook();
    ereport(LOG, (errmodule(MOD_INCRE_BG), errmsg("bgwriter started, thread id is %d", thread_id)));

    Assert(thread_id >= 0);
    errno_t err_rc = snprintf_s(name, MAX_THREAD_NAME_LEN, MAX_THREAD_NAME_LEN - 1, "%s%d", "bgwriter", thread_id);
    securec_check_ss(err_rc, "", "");

    /*
     * Create a resource owner to keep track of our resources (currently only buffer pins).
     */
    t_thrd.utils_cxt.CurrentResourceOwner = ResourceOwnerCreate(NULL, name, MEMORY_CONTEXT_STORAGE);

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
    on_shmem_exit(incre_ckpt_bgwriter_kill, (Datum)0);

    if (sigsetjmp(localSigjmpBuf, 1) != 0) {
        ereport(WARNING, (errmodule(MOD_INCRE_BG), errmsg("bgwriter exception occured.")));
        bgwriter->thrd_dw_cxt.dw_page_idx = -1;
        bgwriter_handle_exceptions(wb_context, bgwriter_context);
    }

    /* We can now handle ereport(ERROR) */
    t_thrd.log_cxt.PG_exception_stack = &localSigjmpBuf;

    /*
     * Unblock signals (they were blocked when the postmaster forked us)
     */
    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();

    /*
    * Use the recovery target timeline ID during recovery
    */
    if (RecoveryInProgress()) {
        t_thrd.xlog_cxt.ThisTimeLineID = GetRecoveryTargetTLI();
    }

    now = get_time_ms();
    t_thrd.bgwriter_cxt.next_flush_time = now + u_sess->attr.attr_storage.BgWriterDelay;

    pgstat_report_appname("IncrBgWriter");
    pgstat_report_activity(STATE_IDLE, NULL);

	/* Loop forever */
    for (;;) {
        int rc;
        bool contain_hashbucket = false;
        uint32 need_flush_num = 0;
        int64 sleep_time = 0;

        if (t_thrd.bgwriter_cxt.got_SIGHUP) {
            t_thrd.bgwriter_cxt.got_SIGHUP = false;
            ProcessConfigFile(PGC_SIGHUP);
        }

        if (t_thrd.bgwriter_cxt.shutdown_requested) {
            /* the first thread should exit last */
            if (thread_id == 0) {
                while (pg_atomic_read_u32(&g_instance.bgwriter_cxt.curr_bgwriter_num) > 1) {
                    pg_usleep(MILLISECOND_TO_MICROSECOND);
                    continue;
                }
                /*
                 * From here on, elog(ERROR) should end with exit(1), not send
                 * control back to the sigsetjmp block above
                 */
                ereport(LOG, (errmodule(MOD_INCRE_BG), errmsg("bgwriter thread shut down, id is %d", thread_id)));
                u_sess->attr.attr_common.ExitOnAnyError = true;
                /* Normal exit from the bgwriter is here */
                proc_exit(0);       /* done */
            } else {
                ereport(LOG, (errmodule(MOD_INCRE_BG), errmsg("bgwriter thread shut down, id is %d", thread_id)));
                u_sess->attr.attr_common.ExitOnAnyError = true;
                proc_exit(0);
            }
        }

        pgstat_report_activity(STATE_IDLE, NULL);
        sleep_time = get_bgwriter_sleep_time();
        rc = WaitLatch(&t_thrd.proc->procLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, sleep_time);
        if (rc & WL_POSTMASTER_DEATH) {
            gs_thread_exit(1);
        }

        /* Clear any already-pending wakeups */
        ResetLatch(&t_thrd.proc->procLatch);

        pgstat_report_activity(STATE_RUNNING, NULL);

        now = get_time_ms();
        t_thrd.bgwriter_cxt.next_flush_time = now + u_sess->attr.attr_storage.BgWriterDelay;

        if (get_curr_candidate_nums() == (uint32)g_instance.attr.attr_storage.NBuffers) {
            continue;
        }
        /*
         * When the primary instance do full checkpoint, the first thread remain scan the
         * buffer pool to maintain the candidate buffer list, other threads scan the dirty
         * page queue and flush pages in sequence.
         * Standby redo the checkpoint xlog, which is similar to full checkpoint, so the
         * bgwirter thread only scan the buffer pool to maintain the candidate buffer list.
         */
        if (FULL_CKPT && !RecoveryInProgress() && thread_id > 0) {
            need_flush_num = get_buf_form_dirty_queue(&contain_hashbucket);
        } else {
            need_flush_num = get_candidate_buf(&contain_hashbucket);
        }

        if (need_flush_num == 0) {
            continue;
        }

        incre_ckpt_bgwriter_flush_page_batch(wb_context, need_flush_num, contain_hashbucket);
    }
}

int get_bgwriter_thread_id(void)
{
    if (t_thrd.bgwriter_cxt.thread_id != -1) {
        return t_thrd.bgwriter_cxt.thread_id;
    }

    /*
     * The first bgwriter thread start, will be placed in the writer_proc slot in order. Some
     * condition, some bgwriter thread exit, It must be placed in the corresponding slot.
     */
    int id = pg_atomic_fetch_add_u32(&g_instance.bgwriter_cxt.curr_bgwriter_num, 1);
    if (g_instance.bgwriter_cxt.bgwriter_procs[id].proc == NULL) {
        g_instance.bgwriter_cxt.bgwriter_procs[id].proc = t_thrd.proc;
        t_thrd.bgwriter_cxt.thread_id = id;
    } else {
        for (int i = 0; i < g_instance.bgwriter_cxt.bgwriter_num; i++) {
            void *expected = NULL;
            if (pg_atomic_compare_exchange_uintptr(
                (uintptr_t *)&g_instance.bgwriter_cxt.bgwriter_procs[i].proc,
                (uintptr_t *)&expected, (uintptr_t)t_thrd.proc)) {
                t_thrd.bgwriter_cxt.thread_id = i;
                break;
            }
        }
    }

    Assert(t_thrd.bgwriter_cxt.thread_id >= 0 && t_thrd.bgwriter_cxt.thread_id < g_instance.bgwriter_cxt.bgwriter_num);
    return t_thrd.bgwriter_cxt.thread_id;
}

const float GAP_PERCENT = 0.15;
static uint32 get_bgwriter_flush_num()
{
    int buffer_num = g_instance.attr.attr_storage.NBuffers;
    double percent_target = u_sess->attr.attr_storage.candidate_buf_percent_target;
    int thread_id = t_thrd.bgwriter_cxt.thread_id;
    uint32 dirty_list_size = g_instance.bgwriter_cxt.bgwriter_procs[thread_id].dirty_list_size;
    uint32 cur_candidate_num;
    uint32 total_target;
    uint32 high_water_mark;
    uint32 flush_num = 0;
    uint32 min_io = DW_DIRTY_PAGE_MAX_FOR_NOHBK;
    uint32 max_io = calculate_thread_max_flush_num(false);

    total_target = buffer_num * percent_target;
    high_water_mark = buffer_num * (percent_target + GAP_PERCENT);
    cur_candidate_num = get_curr_candidate_nums();

    /* If the slots are sufficient, the standby DN does not need to flush too many pages. */
    if (RecoveryInProgress() && cur_candidate_num >= total_target / 2) {
        max_io = max_io / 2;
    }

    /* max_io need greater than one batch flush num, and need less than the dirty list size */
    max_io = MAX(max_io / g_instance.bgwriter_cxt.bgwriter_num, DW_DIRTY_PAGE_MAX_FOR_NOHBK);
    max_io = MIN(max_io, dirty_list_size);

    if (cur_candidate_num >= high_water_mark) {
        flush_num = min_io; /* only flush one batch dirty page */
    } else if (cur_candidate_num >= total_target) {
        flush_num = min_io + (float)(high_water_mark - cur_candidate_num) /
            (float)(high_water_mark - total_target) * (max_io - min_io);
    } else {
        /* every time only flush max_io dirty pages */
        flush_num = max_io;
    }

    ereport(DEBUG1, (errmodule(MOD_INCRE_BG),
        errmsg("bgwriter flush_num num is %u, now candidate buf is %u", flush_num, cur_candidate_num)));
    return flush_num;
}

const int MAX_SCAN_BATCH_NUM = 131072 * 10; /* 10GB buffers */
/**
 * @Description: Scan n buffers in the BufferPool from start, put
 *    the unreferenced and not dirty page into the candidate list.
 * @in: bgwirter thread dirty buf list pointer
 * @in: bgwriter thread id
 * @out: Return the number of dirty buffers and dirty buffer list and this batch buffer whether hashbucket is included.
 */
static uint32 get_candidate_buf(bool *contain_hashbucket)
{
    uint32 need_flush_num = 0;
    uint32 candidates = 0;
    BufferDesc *buf_desc = NULL;
    uint32 local_buf_state;
    CkptSortItem* item = NULL;
    uint32 max_flush_num = get_bgwriter_flush_num();
    int thread_id = t_thrd.bgwriter_cxt.thread_id;
    BgWriterProc *bgwriter = &g_instance.bgwriter_cxt.bgwriter_procs[thread_id];
    CkptSortItem *dirty_buf_list = bgwriter->dirty_buf_list;
    int batch_scan_num = MIN(bgwriter->cand_list_size, MAX_SCAN_BATCH_NUM);
    int start = MAX(bgwriter->buf_id_start, bgwriter->next_scan_loc);
    int end = bgwriter->buf_id_start + bgwriter->cand_list_size;

    end = MIN(start + batch_scan_num, end);

    for (int buf_id = start; buf_id < end; buf_id++) {
        if (FULL_CKPT && !RecoveryInProgress() && thread_id > 0) {
            break;
        }

        buf_desc = GetBufferDescriptor(buf_id);
        local_buf_state = pg_atomic_read_u32(&buf_desc->state);

        /* Dirty read, pinned buffer, skip */
        if (BUF_STATE_GET_REFCOUNT(local_buf_state) > 0) {
            continue;
        }

        local_buf_state = LockBufHdr(buf_desc);
        if (BUF_STATE_GET_REFCOUNT(local_buf_state) > 0) {
            goto UNLOCK;
        }

        /* Not dirty, put directly into flushed candidates */
        if (!(local_buf_state & BM_DIRTY)) {
            if (g_instance.bgwriter_cxt.candidate_free_map[buf_id] == false) {
                candidate_buf_push(buf_id, thread_id);
                g_instance.bgwriter_cxt.candidate_free_map[buf_id] = true;
                candidates++;
            }
            goto UNLOCK;
        }

        if ((!RecoveryInProgress() && XLogNeedsFlush(BufferGetLSN(buf_desc))) ||
            need_flush_num >= max_flush_num) {
            goto UNLOCK;
        }

        if (!(local_buf_state & BM_CHECKPOINT_NEEDED)) {
            local_buf_state |= BM_CHECKPOINT_NEEDED;
            item = &dirty_buf_list[need_flush_num++];
            item->buf_id = buf_id;
            item->tsId = buf_desc->tag.rnode.spcNode;
            item->relNode = buf_desc->tag.rnode.relNode;
            item->bucketNode = buf_desc->tag.rnode.bucketNode;
            item->forkNum = buf_desc->tag.forkNum;
            item->blockNum = buf_desc->tag.blockNum;
            if (buf_desc->tag.rnode.bucketNode != InvalidBktId) {
                *contain_hashbucket = true;
            }
        }
UNLOCK:
        UnlockBufHdr(buf_desc, local_buf_state);
    }

    if (end >= bgwriter->buf_id_start + bgwriter->cand_list_size) {
        bgwriter->next_scan_loc = bgwriter->buf_id_start;
    } else {
        bgwriter->next_scan_loc = end;
    }

    ereport(DEBUG1, (errmodule(MOD_INCRE_BG),
        errmsg("get_candidate_buf %u buf, total candidate num is %u, thread num is %ld, sent %u to flush",
            candidates, get_curr_candidate_nums(), get_thread_candidate_nums(thread_id), need_flush_num)));
    return need_flush_num;
}

static uint32 get_buf_form_dirty_queue(bool *contain_hashbucket)
{
    uint32 need_flush_num = 0;
    BufferDesc *buf_desc = NULL;
    uint32 local_buf_state;
    CkptSortItem* item = NULL;
    int thread_id = t_thrd.bgwriter_cxt.thread_id;
    BgWriterProc *bgwriter = &g_instance.bgwriter_cxt.bgwriter_procs[thread_id];
    CkptSortItem *dirty_buf_list = bgwriter->dirty_buf_list;
    XLogRecPtr redo = g_instance.ckpt_cxt_ctl->full_ckpt_redo_ptr;
    uint64 dirty_queue_head = pg_atomic_read_u64(&g_instance.ckpt_cxt_ctl->dirty_page_queue_head);

    for (int i = 0; ;i++) {
        Buffer buffer;
        uint64 temp_loc = (dirty_queue_head + i) % g_instance.ckpt_cxt_ctl->dirty_page_queue_size;
        volatile DirtyPageQueueSlot* slot = &g_instance.ckpt_cxt_ctl->dirty_page_queue[temp_loc];

        /* slot location is pre-occupied, but the buffer not set finish, need break. */
        if (!(pg_atomic_read_u32(&slot->slot_state) & SLOT_VALID)) {
            break;
        }
        pg_read_barrier();
        buffer = slot->buffer;
        /* slot state is valid, buffer is invalid, the slot buffer set 0 when BufferAlloc or InvalidateBuffer */
        if (BufferIsInvalid(buffer)) {
            continue; /* this tempLoc maybe set 0 when remove dirty page */
        }
        buf_desc = GetBufferDescriptor(buffer - 1);
        local_buf_state = LockBufHdr(buf_desc);

        if (XLByteLT(redo, buf_desc->rec_lsn)) {
            UnlockBufHdr(buf_desc, local_buf_state);
            break;
        }

        if ((local_buf_state & BM_DIRTY) && !(local_buf_state & BM_CHECKPOINT_NEEDED)) {
            local_buf_state |= BM_CHECKPOINT_NEEDED;
            item = &dirty_buf_list[need_flush_num++];
            item->buf_id = buffer - 1;
            item->tsId = buf_desc->tag.rnode.spcNode;
            item->relNode = buf_desc->tag.rnode.relNode;
            item->bucketNode = buf_desc->tag.rnode.bucketNode;
            item->forkNum = buf_desc->tag.forkNum;
            item->blockNum = buf_desc->tag.blockNum;
            if (buf_desc->tag.rnode.bucketNode != InvalidBktId) {
                *contain_hashbucket = true;
            }
        }
        UnlockBufHdr(buf_desc, local_buf_state);
        if (need_flush_num >= GET_DW_DIRTY_PAGE_MAX(*contain_hashbucket)) {
            break;
        }
    }
    ereport(DEBUG1, (errmodule(MOD_INCRE_BG),
        errmsg("get_candidate_buf_full_ckpt, sent %u to flush", need_flush_num)));
    return need_flush_num;
}

/**
 * @Description: Push buffer bufId to thread threadId's candidate list.
 * @in: buf_id, buffer id which need push to the list
 * @in: thread_id, bgwriter thread id
 */
static void candidate_buf_push(int buf_id, int thread_id)
{
    BgWriterProc *bgwriter = &g_instance.bgwriter_cxt.bgwriter_procs[thread_id];
    uint32 list_size = bgwriter->cand_list_size;
    uint32 tail_loc;

    volatile uint64 head = pg_atomic_read_u64(&bgwriter->head);
    volatile uint64 tail = pg_atomic_read_u64(&bgwriter->tail);

    if (unlikely(tail - head >= list_size)) {
        Assert(0);
        return;
    }
    tail_loc = tail % list_size;
    bgwriter->cand_buf_list[tail_loc] = buf_id;
    pg_write_barrier();
    (void)pg_atomic_fetch_add_u64(&bgwriter->tail, 1);
}

/**
 * @Description: Pop a buffer from the head of thread threadId's candidate list and store the buffer in buf_id.
 * @in: buf_id, store the buffer id from the list.
 * @in: thread_id, bgwriter thread id
 */
bool candidate_buf_pop(int *buf_id, int thread_id)
{
    BgWriterProc *bgwriter = &g_instance.bgwriter_cxt.bgwriter_procs[thread_id];
    uint32 list_size = bgwriter->cand_list_size;
    uint32 head_loc;

    while (true) {
        uint64 head = pg_atomic_read_u64(&bgwriter->head);
        volatile uint64 tail = pg_atomic_read_u64(&bgwriter->tail);

        if (unlikely(head >= tail)) {
            return false;       /* candidate list is empty */
        }

        pg_write_barrier();
        head_loc = head % list_size;
        *buf_id = bgwriter->cand_buf_list[head_loc];
        if (pg_atomic_compare_exchange_u64(&bgwriter->head, &head, head + 1)) {
            return true;
        }
    }
}

static int64 get_thread_candidate_nums(int thread_id)
{
    BgWriterProc *bgwriter = &g_instance.bgwriter_cxt.bgwriter_procs[thread_id];
    volatile uint64 head = pg_atomic_read_u64(&bgwriter->head);
    volatile uint64 tail = pg_atomic_read_u64(&bgwriter->tail);
    int64 curr_cand_num = tail - head;
    Assert(curr_cand_num >= 0);
    return curr_cand_num;
}

/**
 * @Description: Return a rough estimate of the current number of buffers in the candidate list.
 */
uint32 get_curr_candidate_nums(void)
{
    uint32 currCandidates = 0;
    for (int i = 0; i < g_instance.bgwriter_cxt.bgwriter_num; i++) {
        BgWriterProc *curr_writer = &g_instance.bgwriter_cxt.bgwriter_procs[i];
        if (curr_writer->proc != NULL) {
            currCandidates += get_thread_candidate_nums(i);
        }
    }
    return currCandidates;
}

void ckpt_shutdown_bgwriter()
{
    if (!g_instance.attr.attr_storage.enableIncrementalCheckpoint) {
        return;
    }
    /* Wait for all buffer writer threads to exit. */
    while (pg_atomic_read_u32(&g_instance.bgwriter_cxt.curr_bgwriter_num) != 0) {
        pg_usleep(MILLISECOND_TO_MICROSECOND);
    }
}
