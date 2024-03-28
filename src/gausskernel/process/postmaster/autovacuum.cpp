/*
 *
 * autovacuum.cpp
 *
 * openGauss Integrated Autovacuum Daemon
 *
 * The autovacuum system is structured in two different kinds of processes: the
 * autovacuum launcher and the autovacuum worker.  The launcher is an
 * always-running process, started by the postmaster when the autovacuum GUC
 * parameter is set.  The launcher schedules autovacuum workers to be started
 * when appropriate.  The workers are the processes which execute the actual
 * vacuuming; they connect to a database as determined in the launcher, and
 * once connected they examine the catalogs to select the tables to vacuum.
 *
 * The autovacuum launcher cannot start the worker processes by itself,
 * because doing so would cause robustness issues (namely, failure to shut
 * them down on exceptional conditions, and also, since the launcher is
 * connected to shared memory and is thus subject to corruption there, it is
 * not as robust as the postmaster).  So it leaves that task to the postmaster.
 *
 * There is an autovacuum shared memory area, where the launcher stores
 * information about the database it wants vacuumed.  When it wants a new
 * worker to start, it sets a flag in shared memory and sends a signal to the
 * postmaster.	Then postmaster knows nothing more than it must start a worker;
 * so it forks a new child, which turns into a worker.	This new process
 * connects to shared memory, and there it can inspect the information that the
 * launcher has set up.
 *
 * If the fork() call fails in the postmaster, it sets a flag in the shared
 * memory area, and sends a signal to the launcher.  The launcher, upon
 * noticing the flag, can try starting the worker again by resending the
 * signal.	Note that the failure can only be transient (fork failure due to
 * high load, memory pressure, too many processes, etc); more permanent
 * problems, like failure to connect to a database, are detected later in the
 * worker and dealt with just by having the worker exit normally.  The launcher
 * will launch a new worker again later, per schedule.
 *
 * When the worker is done vacuuming it sends SIGUSR2 to the launcher.	The
 * launcher then wakes up and is able to launch another worker, if the schedule
 * is so tight that a new worker is needed immediately.  At this time the
 * launcher can also balance the settings for the various remaining workers'
 * cost-based vacuum delay feature.
 *
 * Note that there can be more than one worker in a database concurrently.
 * They will store the table they are currently vacuuming in shared memory, so
 * that other workers avoid being blocked waiting for the vacuum lock for that
 * table.  They will also reload the pgstats data just before vacuuming each
 * table, to avoid vacuuming a table that was just finished being vacuumed by
 * another worker and thus is no longer noted in shared memory.  However,
 * there is a window (caused by pgstat delay) on which a worker may choose a
 * table that was already vacuumed; this is a bug in the current design.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/process/postmaster/autovacuum.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <signal.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>
#ifdef HAVE_SYS_STAT_H
#include <sys/stat.h>
#endif

#include "access/hash.h"
#include "access/gtm.h"
#include "access/heapam.h"
#include "access/reloptions.h"
#include "access/tableam.h"
#include "access/twophase.h"
#include "access/transam.h"
#include "access/xact.h"
#include "access/multixact.h"
#include "catalog/dependency.h"
#include "catalog/namespace.h"
#include "catalog/pg_database.h"
#include "catalog/pgxc_class.h"
#include "commands/dbcommands.h"
#include "commands/vacuum.h"
#include "commands/matview.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "pgxc/execRemote.h"
#include "pgxc/pgxc.h"
#include "postmaster/autovacuum.h"
#include "postmaster/fork_process.h"
#include "postmaster/postmaster.h"
#include "storage/buf/bufmgr.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/procsignal.h"
#include "storage/sinvaladt.h"
#include "tcop/tcopprot.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/postinit.h"
#include "utils/ps_status.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"
#include "access/heapam.h"
#include "commands/user.h"
#include "gssignal/gs_signal.h"
#include "common/config/cm_config.h"
#include "catalog/pg_namespace.h"
#include "storage/lmgr.h"

/* struct to keep tuples stat that fetchs from DataNode */
typedef struct avw_info {
    PgStat_StatTabKey tabkey;
    int64 n_dead_tuples;
    int64 n_live_tuples;
    int64 changes_since_analyze;
} avw_info;

/* struct to keep track of databases in worker */
typedef struct avw_dbase {
    Oid adw_datid;
    char* adw_name;
    TransactionId adw_frozenxid;
    MultiXactId	adw_frozenmulti;
    PgStat_StatDBEntry* adw_entry;
} avw_dbase;

NON_EXEC_STATIC void AutoVacWorkerMain();
NON_EXEC_STATIC void AutoVacLauncherMain();

static Oid do_start_worker(void);
static void launcher_determine_sleep(bool canlaunch, bool recursing, struct timeval* nap);
static void launch_worker(TimestampTz now);
static List* get_database_list(void);
static void rebuild_database_list(Oid newdb);
static int db_comparator(const void* a, const void* b);
static void autovac_balance_cost(void);

static void do_autovacuum(void);
static void FreeWorkerInfo(int code, Datum arg);

/* add parameter toast_table_map by data partition. */
static autovac_table* table_recheck_autovac(
    vacuum_object* vacObj, HTAB* table_toast_map, HTAB* toast_table_map, TupleDesc pg_class_desc);
static void relation_needs_vacanalyze(Oid relid, AutoVacOpts* relopts, bytea* rawRelopts, Form_pg_class classForm,
    HeapTuple tuple, PgStat_StatTabEntry* tabentry, bool allowAnalyze, bool allowVacuum, bool is_recheck,
    bool* dovacuum, bool* doanalyze, bool* need_freeze);

static void autovacuum_do_vac_analyze(autovac_table* tab, BufferAccessStrategy bstrategy);
static void autovacuum_local_vac_analyze(autovac_table* tab, BufferAccessStrategy bstrategy);

/* add parameter statFlag by data partition. */
static PgStat_StatTabEntry* get_pgstat_tabentry_relid(
    Oid relid, bool isshared, uint32 statFlag, PgStat_StatDBEntry* shared, PgStat_StatDBEntry* dbentry);
static void autovac_report_activity(autovac_table* tab);
static void avl_sighup_handler(SIGNAL_ARGS);
static void avl_sigusr2_handler(SIGNAL_ARGS);
static void avl_sigterm_handler(SIGNAL_ARGS);
static void autovac_refresh_stats(void);

static void partition_needs_vacanalyze(Oid partid, AutoVacOpts* relopts, Form_pg_partition partForm,
    HeapTuple partTuple, at_partitioned_table* ap_entry, PgStat_StatTabEntry* tabentry, bool is_recheck, bool* dovacuum,
    bool* doanalyze, bool* need_freeze);
static autovac_table* partition_recheck_autovac(
    vacuum_object* vacObj, HTAB* table_relopt_map, HTAB* partitioned_tables_map, TupleDesc pg_class_desc);
extern void DoVacuumMppTable(VacuumStmt* stmt, const char* queryString, bool isTopLevel, bool sentToRemote);
/*
 * Called when the AutoVacuum is ending.
 */
static void autoVacQuitAndClean(int code, Datum arg)
{
    /* Close connection with GTM, if active */
    CloseGTM();

    /* Free remote xact state */
    free_RemoteXactState();

    /* Free gxip */
    UnsetGlobalSnapshotData();
}

/********************************************************************
 *					  AUTOVACUUM LAUNCHER CODE
 ********************************************************************/
#ifdef EXEC_BACKEND
/*
 * We need this set from the outside, before InitProcess is called
 */
void AutovacuumLauncherIAm(void)
{
    t_thrd.role = AUTOVACUUM_LAUNCHER;
}
#endif

/*
 * Main loop for the autovacuum launcher process.
 */
NON_EXEC_STATIC void AutoVacLauncherMain()
{
    sigjmp_buf local_sigjmp_buf;

    /* we are a postmaster subprocess now */
    IsUnderPostmaster = true;
    t_thrd.role = AUTOVACUUM_LAUNCHER;

    /* reset t_thrd.proc_cxt.MyProcPid */
    t_thrd.proc_cxt.MyProcPid = gs_thread_self();

    /* record Start Time for logging */
    t_thrd.proc_cxt.MyStartTime = time(NULL);

    t_thrd.proc_cxt.MyProgName = "AutoVacLauncher";
    u_sess->attr.attr_common.application_name = pstrdup("AutoVacLauncher");

    /* Identify myself via ps */
    init_ps_display("autovacuum launcher process", "", "", "");

    ereport(LOG, (errmsg("autovacuum launcher started")));

    if (u_sess->attr.attr_security.PostAuthDelay)
        pg_usleep(u_sess->attr.attr_security.PostAuthDelay * 1000000L);

    SetProcessingMode(InitProcessing);

    /*
     * Set up signal handlers.	We operate on databases much like a regular
     * backend, so we use the same signal handling.  See equivalent code in
     * tcop/postgres.c.
     */
    gspqsignal(SIGHUP, avl_sighup_handler);
    gspqsignal(SIGINT, StatementCancelHandler);
    gspqsignal(SIGTERM, avl_sigterm_handler);

    gspqsignal(SIGQUIT, quickdie);
    gspqsignal(SIGALRM, handle_sig_alarm);

    gspqsignal(SIGPIPE, SIG_IGN);
    gspqsignal(SIGUSR1, procsignal_sigusr1_handler);
    gspqsignal(SIGUSR2, avl_sigusr2_handler);
    gspqsignal(SIGFPE, FloatExceptionHandler);
    gspqsignal(SIGCHLD, SIG_DFL);
    gspqsignal(SIGURG, print_stack);
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
    t_thrd.proc_cxt.PostInit->InitAutoVacLauncher();

    SetProcessingMode(NormalProcessing);

    /* If we exit, first try and clean connections and memory */
    on_proc_exit(autoVacQuitAndClean, 0);

    /*
     * Create a memory context that we will do all our work in.  We do this so
     * that we can reset the context during error recovery and thereby avoid
     * possible memory leaks.
     */
    t_thrd.autovacuum_cxt.AutovacMemCxt = AllocSetContextCreate(t_thrd.top_mem_cxt, "Autovacuum Launcher",
        ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
    (void)MemoryContextSwitchTo(t_thrd.autovacuum_cxt.AutovacMemCxt);

    /*
     * If an exception is encountered, processing resumes here.
     *
     * This code is a stripped down version of PostgresMain error recovery.
     */
    int curTryCounter;
    int* oldTryCounter = NULL;
    if (sigsetjmp(local_sigjmp_buf, 1) != 0) {
        gstrace_tryblock_exit(true, oldTryCounter);

        /* since not using PG_TRY, must reset error stack by hand */
        t_thrd.log_cxt.error_context_stack = NULL;

        t_thrd.log_cxt.call_stack = NULL;

        /* Prevents interrupts while cleaning up */
        HOLD_INTERRUPTS();

        /* Forget any pending QueryCancel request */
        t_thrd.int_cxt.QueryCancelPending = false;
        disable_sig_alarm(true);
        t_thrd.int_cxt.QueryCancelPending = false; /* again in case timeout occurred */

        /* Report the error to the server log */
        EmitErrorReport();

        /* Abort the current transaction in order to recover */
        AbortCurrentTransaction();

        /* release resource held by lsc */
        AtEOXact_SysDBCache(false);

        /*
         * Now return to normal top-level context and clear ErrorContext for
         * next time.
         */
        (void)MemoryContextSwitchTo(t_thrd.autovacuum_cxt.AutovacMemCxt);
        FlushErrorState();

        /* Flush any leaked data in the top-level context */
        MemoryContextResetAndDeleteChildren(t_thrd.autovacuum_cxt.AutovacMemCxt);

        /* don't leave dangling pointers to freed memory */
        t_thrd.autovacuum_cxt.DatabaseListCxt = NULL;
        t_thrd.autovacuum_cxt.DatabaseList = NULL;

        /*
         * Make sure pgstat also considers our stat data as gone.  Note: we
         * mustn't use autovac_refresh_stats here.
         */
        pgstat_clear_snapshot();

        /* Now we can allow interrupts again */
        RESUME_INTERRUPTS();

        /* if in shutdown mode, no need for anything further; just go away */
        if (t_thrd.autovacuum_cxt.got_SIGTERM)
            goto shutdown;

        /*
         * Sleep at least 1 second after any error.  We don't want to be
         * filling the error logs as fast as we can.
         */
        pg_usleep(1000000L);
    }
    oldTryCounter = gstrace_tryblock_entry(&curTryCounter);

    /* We can now handle ereport(ERROR) */
    t_thrd.log_cxt.PG_exception_stack = &local_sigjmp_buf;

    /* must unblock signals before calling rebuild_database_list */
    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    gs_signal_unblock_sigusr2();

    /*
     * Force zero_damaged_pages OFF in the autovac process, even if it is set
     * in postgresql.conf.	We don't really want such a dangerous option being
     * applied non-interactively.
     */
    SetConfigOption("zero_damaged_pages", "false", PGC_SUSET, PGC_S_OVERRIDE);

    /*
     * Force statement_timeout to zero to avoid a timeout setting from
     * preventing regular maintenance from being executed.
     */
    SetConfigOption("statement_timeout", "0", PGC_SUSET, PGC_S_OVERRIDE);

    /*
     * Force default_transaction_isolation to READ COMMITTED.  We don't want
     * to pay the overhead of serializable mode, nor add any risk of causing
     * deadlocks or delaying other transactions.
     */
    SetConfigOption("default_transaction_isolation", "read committed", PGC_SUSET, PGC_S_OVERRIDE);

    /*
     * In emergency mode, just start a worker (unless shutdown was requested)
     * and go away.
     */
    if (!AutoVacuumingActive()) {
        if (!t_thrd.autovacuum_cxt.got_SIGTERM)
            do_start_worker();
        proc_exit(0); /* done */
    }

    t_thrd.autovacuum_cxt.AutoVacuumShmem->av_launcherpid = t_thrd.proc_cxt.MyProcPid;

    if (likely(t_thrd.shemem_ptr_cxt.mySessionMemoryEntry != NULL)) {
        t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->initMemInChunks = t_thrd.utils_cxt.trackedMemChunks;
        t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->queryMemInChunks = t_thrd.utils_cxt.trackedMemChunks;
        t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->peakChunksQuery = t_thrd.utils_cxt.trackedMemChunks;
    }
    t_thrd.utils_cxt.peakedBytesInQueryLifeCycle = 0;
    t_thrd.utils_cxt.basedBytesInQueryLifeCycle = 0;

    /*
     * Create the initial database list.  The invariant we want this list to
     * keep is that it's ordered by decreasing next_time.  As soon as an entry
     * is updated to a higher time, it will be moved to the front (which is
     * correct because the only operation is to add autovacuum_naptime to the
     * entry, and time always increases).
     */
    rebuild_database_list(InvalidOid);

    /* loop until shutdown request */
    while (!t_thrd.autovacuum_cxt.got_SIGTERM) {
        struct timeval nap;
        TimestampTz current_time = 0;
        bool can_launch = false;
        Dlelem* elem = NULL;
        int rc;

        /*
         * This loop is a bit different from the normal use of WaitLatch,
         * because we'd like to sleep before the first launch of a child
         * process.  So it's WaitLatch, then ResetLatch, then check for
         * wakening conditions.
         */
        launcher_determine_sleep((t_thrd.autovacuum_cxt.AutoVacuumShmem->av_freeWorkers != NULL), false, &nap);

        /*
         * Wait until naptime expires or we get some type of signal (all the
         * signal handlers will wake us by calling SetLatch).
         */
        rc = WaitLatch(&t_thrd.proc->procLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
            (nap.tv_sec * 1000L) + (nap.tv_usec / 1000L));

        ResetLatch(&t_thrd.proc->procLatch);

        /* Process sinval catchup interrupts that happened while sleeping */
        ProcessCatchupInterrupt();

        /*
         * Emergency bailout if postmaster has died.  This is to avoid the
         * necessity for manual cleanup of all postmaster children.
         */
        if (((unsigned int)rc) & WL_POSTMASTER_DEATH)
            proc_exit(1);

        /* the normal shutdown case */
        if (t_thrd.autovacuum_cxt.got_SIGTERM)
            break;

        if (t_thrd.autovacuum_cxt.got_SIGHUP) {
            t_thrd.autovacuum_cxt.got_SIGHUP = false;
            ProcessConfigFile(PGC_SIGHUP);

            /* shutdown requested in config file? */
            if (!AutoVacuumingActive())
                break;

            /* rebalance in case the default cost parameters changed */
            LWLockAcquire(AutovacuumLock, LW_EXCLUSIVE);
            autovac_balance_cost();
            LWLockRelease(AutovacuumLock);

            /* rebuild the list in case the naptime changed */
            rebuild_database_list(InvalidOid);
        }

        /*
         * a worker finished, or postmaster signalled failure to start a
         * worker
         */
        if (t_thrd.autovacuum_cxt.got_SIGUSR2) {
            t_thrd.autovacuum_cxt.got_SIGUSR2 = false;

            /* rebalance cost limits, if needed */
            if (t_thrd.autovacuum_cxt.AutoVacuumShmem->av_signal[AutoVacRebalance]) {
                LWLockAcquire(AutovacuumLock, LW_EXCLUSIVE);
                t_thrd.autovacuum_cxt.AutoVacuumShmem->av_signal[AutoVacRebalance] = false;
                autovac_balance_cost();
                LWLockRelease(AutovacuumLock);
            }

            if (t_thrd.autovacuum_cxt.AutoVacuumShmem->av_signal[AutoVacForkFailed]) {
                /*
                 * If the postmaster failed to start a new worker, we sleep
                 * for a little while and resend the signal.  The new worker's
                 * state is still in memory, so this is sufficient.  After
                 * that, we restart the main loop.
                 *
                 * XXX should we put a limit to the number of times we retry?
                 * I don't think it makes much sense, because a future start
                 * of a worker will continue to fail in the same way.
                 */
                t_thrd.autovacuum_cxt.AutoVacuumShmem->av_signal[AutoVacForkFailed] = false;
                pg_usleep(1000000L); /* 1s */
                SendPostmasterSignal(PMSIGNAL_START_AUTOVAC_WORKER);
                continue;
            }
        }

        /*
         * There are some conditions that we need to check before trying to
         * start a launcher.  First, we need to make sure that there is a
         * launcher slot available.  Second, we need to make sure that no
         * other worker failed while starting up.
         */
        current_time = GetCurrentTimestamp();
        LWLockAcquire(AutovacuumLock, LW_SHARED);

        can_launch = (t_thrd.autovacuum_cxt.AutoVacuumShmem->av_freeWorkers != NULL);

        if (t_thrd.autovacuum_cxt.AutoVacuumShmem->av_startingWorker != NULL) {
            int waittime;
            WorkerInfo worker = t_thrd.autovacuum_cxt.AutoVacuumShmem->av_startingWorker;

            /*
             * We can't launch another worker when another one is still
             * starting up (or failed while doing so), so just sleep for a bit
             * more; that worker will wake us up again as soon as it's ready.
             * We will only wait autovacuum_naptime seconds (up to a maximum
             * of 60 seconds) for this to happen however.  Note that failure
             * to connect to a particular database is not a problem here,
             * because the worker removes itself from the startingWorker
             * pointer before trying to connect.  Problems detected by the
             * postmaster (like fork() failure) are also reported and handled
             * differently.  The only problems that may cause this code to
             * fire are errors in the earlier sections of AutoVacWorkerMain,
             * before the worker removes the WorkerInfo from the
             * startingWorker pointer.
             */
            waittime = Min(u_sess->attr.attr_storage.autovacuum_naptime, 60) * 1000;
            if (TimestampDifferenceExceeds(worker->wi_launchtime, current_time, waittime)) {
                LWLockRelease(AutovacuumLock);
                LWLockAcquire(AutovacuumLock, LW_EXCLUSIVE);

                /*
                 * No other process can put a worker in starting mode, so if
                 * startingWorker is still INVALID after exchanging our lock,
                 * we assume it's the same one we saw above (so we don't
                 * recheck the launch time).
                 */
                if (t_thrd.autovacuum_cxt.AutoVacuumShmem->av_startingWorker != NULL) {
                    worker = t_thrd.autovacuum_cxt.AutoVacuumShmem->av_startingWorker;
                    worker->wi_dboid = InvalidOid;
                    worker->wi_tableoid = InvalidOid;
                    worker->wi_parentoid = InvalidOid;
                    worker->wi_sharedrel = false;
                    worker->wi_proc = NULL;
                    worker->wi_launchtime = 0;
                    worker->wi_links.next = (SHM_QUEUE*)t_thrd.autovacuum_cxt.AutoVacuumShmem->av_freeWorkers;
                    t_thrd.autovacuum_cxt.AutoVacuumShmem->av_freeWorkers = worker;
                    t_thrd.autovacuum_cxt.AutoVacuumShmem->av_startingWorker = NULL;
                    ereport(WARNING, (errmsg("worker took too long to start; canceled")));
                }
            } else
                can_launch = false;
        }
        LWLockRelease(AutovacuumLock); /* either shared or exclusive */

        /* if we can't do anything, just go back to sleep */
        if (!can_launch)
            continue;

        /* We're OK to start a new worker */
        elem = DLGetTail(t_thrd.autovacuum_cxt.DatabaseList);
        if (elem != NULL) {
            avl_dbase* avdb = (avl_dbase*)DLE_VAL(elem);

            /*
             * launch a worker if next_worker is right now or it is in the
             * past
             */
            if (TimestampDifferenceExceeds(avdb->adl_next_worker, current_time, 0))
                launch_worker(current_time);
        } else {
            /*
             * Special case when the list is empty: start a worker right away.
             * This covers the initial case, when no database is in pgstats
             * (thus the list is empty).  Note that the constraints in
             * launcher_determine_sleep keep us from starting workers too
             * quickly (at most once every autovacuum_naptime when the list is
             * empty).
             */
            launch_worker(current_time);
        }
    }

    /* Normal exit from the autovac launcher is here */
shutdown:
    ereport(LOG, (errmsg("autovacuum launcher shutting down")));
    t_thrd.autovacuum_cxt.AutoVacuumShmem->av_launcherpid = 0;

    proc_exit(0); /* done */
}

/*
 * Determine the time to sleep, based on the database list.
 *
 * The "canlaunch" parameter indicates whether we can start a worker right now,
 * for example due to the workers being all busy.  If this is false, we will
 * cause a long sleep, which will be interrupted when a worker exits.
 */
static void launcher_determine_sleep(bool canlaunch, bool recursing, struct timeval* nap)
{
    Dlelem* elem = NULL;

    /*
     * We sleep until the next scheduled vacuum.  We trust that when the
     * database list was built, care was taken so that no entries have times
     * in the past; if the first entry has too close a next_worker value, or a
     * time in the past, we will sleep a small nominal time.
     */
    if (!canlaunch) {
        nap->tv_sec = u_sess->attr.attr_storage.autovacuum_naptime;
        nap->tv_usec = 0;
    } else if ((elem = DLGetTail(t_thrd.autovacuum_cxt.DatabaseList)) != NULL) {
        avl_dbase* avdb = (avl_dbase*)DLE_VAL(elem);
        TimestampTz current_time = GetCurrentTimestamp();
        TimestampTz next_wakeup;
        long secs;
        int usecs;

        next_wakeup = avdb->adl_next_worker;
        TimestampDifference(current_time, next_wakeup, &secs, &usecs);

        nap->tv_sec = secs;
        nap->tv_usec = usecs;
    } else {
        /* list is empty, sleep for whole autovacuum_naptime seconds  */
        nap->tv_sec = u_sess->attr.attr_storage.autovacuum_naptime;
        nap->tv_usec = 0;
    }

    /*
     * If the result is exactly zero, it means a database had an entry with
     * time in the past.  Rebuild the list so that the databases are evenly
     * distributed again, and recalculate the time to sleep.  This can happen
     * if there are more tables needing vacuum than workers, and they all take
     * longer to vacuum than autovacuum_naptime.
     *
     * We only recurse once.  rebuild_database_list should always return times
     * in the future, but it seems best not to trust too much on that.
     */
    if (nap->tv_sec == 0 && nap->tv_usec == 0 && !recursing) {
        rebuild_database_list(InvalidOid);
        launcher_determine_sleep(canlaunch, true, nap);
        return;
    }

    /* The smallest time we'll allow the launcher to sleep. */
    if (nap->tv_sec <= 0 && nap->tv_usec <= MIN_AUTOVAC_SLEEPTIME * 1000) {
        nap->tv_sec = 0;
        nap->tv_usec = (__suseconds_t)(MIN_AUTOVAC_SLEEPTIME * 1000);
    }
}

/*
 * Build an updated t_thrd.autovacuum_cxt.DatabaseList.  It must only contain databases that appear
 * in pgstats, and must be sorted by next_worker from highest to lowest,
 * distributed regularly across the next autovacuum_naptime interval.
 *
 * Receives the Oid of the database that made this list be generated (we call
 * this the "new" database, because when the database was already present on
 * the list, we expect that this function is not called at all).  The
 * preexisting list, if any, will be used to preserve the order of the
 * databases in the autovacuum_naptime period.	The new database is put at the
 * end of the interval.  The actual values are not saved, which should not be
 * much of a problem.
 */
static void rebuild_database_list(Oid newdb)
{
    List* dblist = NIL;
    ListCell* cell = NULL;
    MemoryContext newcxt;
    MemoryContext oldcxt;
    MemoryContext tmpcxt;
    HASHCTL hctl;
    uint score;
    uint nelems;
    HTAB* dbhash = NULL;

    /* use fresh stats */
    autovac_refresh_stats();

    newcxt = AllocSetContextCreate(t_thrd.autovacuum_cxt.AutovacMemCxt, "AV dblist",
        ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
    tmpcxt = AllocSetContextCreate(
        newcxt, "tmp AV dblist", ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
    oldcxt = MemoryContextSwitchTo(tmpcxt);

    /*
     * Implementing this is not as simple as it sounds, because we need to put
     * the new database at the end of the list; next the databases that were
     * already on the list, and finally (at the tail of the list) all the
     * other databases that are not on the existing list.
     *
     * To do this, we build an empty hash table of scored databases.  We will
     * start with the lowest score (zero) for the new database, then
     * increasing scores for the databases in the existing list, in order, and
     * lastly increasing scores for all databases gotten via
     * get_database_list() that are not already on the hash.
     *
     * Then we will put all the hash elements into an array, sort the array by
     * score, and finally put the array elements into the new doubly linked
     * list.
     */
    hctl.keysize = sizeof(Oid);
    hctl.entrysize = sizeof(avl_dbase);
    hctl.hash = oid_hash;
    hctl.hcxt = tmpcxt;
    dbhash = hash_create("db hash", 20, &hctl, HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

    /* start by inserting the new database */
    score = 0;
    if (OidIsValid(newdb)) {
        avl_dbase* db = NULL;
        PgStat_StatDBEntry* entry = NULL;

        /* only consider this database if it has a pgstat entry */
        entry = pgstat_fetch_stat_dbentry(newdb);
        if (entry != NULL) {
            /* we assume it isn't found because the hash was just created */
            db = (avl_dbase*)hash_search(dbhash, &newdb, HASH_ENTER, NULL);

            /* hash_search already filled in the key */
            db->adl_score = score++;
            /* next_worker is filled in later */
        }
    }

    /* Now insert the databases from the existing list */
    if (t_thrd.autovacuum_cxt.DatabaseList != NULL) {
        Dlelem* elem = NULL;

        elem = DLGetHead(t_thrd.autovacuum_cxt.DatabaseList);
        while (elem != NULL) {
            avl_dbase* avdb = (avl_dbase*)DLE_VAL(elem);
            avl_dbase* db = NULL;
            bool found = false;
            PgStat_StatDBEntry* entry = NULL;

            elem = DLGetSucc(elem);

            /*
             * skip databases with no stat entries -- in particular, this gets
             * rid of dropped databases
             */
            entry = pgstat_fetch_stat_dbentry(avdb->adl_datid);
            if (entry == NULL)
                continue;

            db = (avl_dbase*)hash_search(dbhash, &(avdb->adl_datid), HASH_ENTER, &found);

            if (!found) {
                /* hash_search already filled in the key */
                db->adl_score = score++;
                /* next_worker is filled in later */
            }
        }
    }

    /* finally, insert all qualifying databases not previously inserted */
    dblist = get_database_list();
    foreach (cell, dblist) {
        avw_dbase* avdb = (avw_dbase*)lfirst(cell);
        avl_dbase* db = NULL;
        bool found = false;
        PgStat_StatDBEntry* entry = NULL;

        /* only consider databases with a pgstat entry */
        entry = pgstat_fetch_stat_dbentry(avdb->adw_datid);
        if (entry == NULL)
            continue;

        db = (avl_dbase*)hash_search(dbhash, &(avdb->adw_datid), HASH_ENTER, &found);
        /* only update the score if the database was not already on the hash */
        if (!found) {
            /* hash_search already filled in the key */
            db->adl_score = score++;
            /* next_worker is filled in later */
        }
    }
    nelems = score;

    /* from here on, the allocated memory belongs to the new list */
    (void)MemoryContextSwitchTo(newcxt);
    t_thrd.autovacuum_cxt.DatabaseList = DLNewList();

    if (nelems > 0) {
        TimestampTz current_time;
        int millis_increment;
        avl_dbase* dbary = NULL;
        avl_dbase* db = NULL;
        HASH_SEQ_STATUS seq;
        uint i;
        int rc = 0;

        if (unlikely(nelems > MaxAllocSize / sizeof(avl_dbase))) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Invalid database num:%u", nelems)));
        }

        /* put all the hash elements into an array */
        dbary = (avl_dbase*)palloc(nelems * sizeof(avl_dbase));

        i = 0;
        hash_seq_init(&seq, dbhash);
        while ((db = (avl_dbase*)hash_seq_search(&seq)) != NULL) {
            rc = memcpy_s(&(dbary[i++]), sizeof(avl_dbase), db, sizeof(avl_dbase));
            securec_check(rc, "\0", "\0");
        }

        /* sort the array */
        qsort(dbary, nelems, sizeof(avl_dbase), db_comparator);

        /*
         * Determine the time interval between databases in the schedule. If
         * we see that the configured naptime would take us to sleep times
         * lower than our min sleep time (which launcher_determine_sleep is
         * coded not to allow), silently use a larger naptime (but don't touch
         * the GUC variable).
         */
        millis_increment = (int)(1000.0 * u_sess->attr.attr_storage.autovacuum_naptime / nelems);
        if (millis_increment <= MIN_AUTOVAC_SLEEPTIME) {
            millis_increment = (int)(MIN_AUTOVAC_SLEEPTIME * 1.1);
        }

        current_time = GetCurrentTimestamp();

        /*
         * move the elements from the array into the dllist, setting the
         * next_worker while walking the array
         */
        for (i = 0; i < nelems; i++) {
            avl_dbase* db_tmp = (avl_dbase*)&(dbary[i]);
            Dlelem* elem = NULL;

            current_time = TimestampTzPlusMilliseconds(current_time, millis_increment);
            db_tmp->adl_next_worker = current_time;

            elem = DLNewElem(db_tmp);
            /* later elements should go closer to the head of the list */
            DLAddHead(t_thrd.autovacuum_cxt.DatabaseList, elem);
        }
    }

    /* all done, clean up memory */
    if (t_thrd.autovacuum_cxt.DatabaseListCxt != NULL)
        MemoryContextDelete(t_thrd.autovacuum_cxt.DatabaseListCxt);
    MemoryContextDelete(tmpcxt);
    t_thrd.autovacuum_cxt.DatabaseListCxt = newcxt;
    (void)MemoryContextSwitchTo(oldcxt);
}

/* qsort comparator for avl_dbase, using adl_score */
static int db_comparator(const void* a, const void* b)
{
    if (((const avl_dbase*)a)->adl_score == ((const avl_dbase*)b)->adl_score) {
        return 0;
    } else {
        return (((const avl_dbase*)a)->adl_score < ((const avl_dbase*)b)->adl_score) ? 1 : -1;
    }
}

/*
 * do_start_worker
 *
 * Bare-bones procedure for starting an autovacuum worker from the launcher.
 * It determines what database to work on, sets up shared memory stuff and
 * signals postmaster to start the worker.	It fails gracefully if invoked when
 * autovacuum_workers are already active.
 *
 * Return value is the OID of the database that the worker is going to process,
 * or InvalidOid if no worker was actually started.
 */
static Oid do_start_worker(void)
{
    List* dblist = NIL;
    ListCell* cell = NULL;
    TransactionId xidForceLimit;
#ifndef ENABLE_MULTIPLE_NODES
    MultiXactId multiForceLimit;
#endif
    bool for_xid_wrap = false;
    bool for_multi_wrap = false;
    avw_dbase* avdb = NULL;
    TimestampTz current_time;
    bool skipit = false;
    Oid retval = InvalidOid;
    MemoryContext tmpcxt, oldcxt;

    /* return quickly when there are no free workers */
    LWLockAcquire(AutovacuumLock, LW_SHARED);
    if (t_thrd.autovacuum_cxt.AutoVacuumShmem->av_freeWorkers == NULL) {
        LWLockRelease(AutovacuumLock);
        AUTOVAC_LOG(LOG, "no free autovaccm worker");

        return InvalidOid;
    }
    LWLockRelease(AutovacuumLock);

    /*
     * Create and switch to a temporary context to avoid leaking the memory
     * allocated for the database list.
     */
    tmpcxt = AllocSetContextCreate(CurrentMemoryContext, "Start worker tmp cxt",
        ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
    oldcxt = MemoryContextSwitchTo(tmpcxt);

    /* use fresh stats */
    autovac_refresh_stats();

    /* Get a list of databases */
    dblist = get_database_list();

    /*
     * Determine the oldest datfrozenxid64/relfrozenxid64 that we will allow to
     * pass without forcing a vacuum.  (This limit can be tightened for
     * particular tables, but not loosened.)
     */
    t_thrd.autovacuum_cxt.recentXid = ReadNewTransactionId();
    if (t_thrd.autovacuum_cxt.recentXid >
        FirstNormalTransactionId + (uint64)g_instance.attr.attr_storage.autovacuum_freeze_max_age)
        xidForceLimit = t_thrd.autovacuum_cxt.recentXid -
            (uint64)g_instance.attr.attr_storage.autovacuum_freeze_max_age;
    else
        xidForceLimit = FirstNormalTransactionId;

#ifndef ENABLE_MULTIPLE_NODES
    /* Also determine the oldest datminmxid we will consider. */
    t_thrd.autovacuum_cxt.recentMulti = ReadNextMultiXactId();
    if (t_thrd.autovacuum_cxt.recentMulti >
        FirstMultiXactId + (uint64)g_instance.attr.attr_storage.autovacuum_freeze_max_age)
        multiForceLimit = t_thrd.autovacuum_cxt.recentMulti -
            (uint64)g_instance.attr.attr_storage.autovacuum_freeze_max_age;
    else
        multiForceLimit = FirstMultiXactId;
#endif

    /*
     * Choose a database to connect to.  We pick the database that was least
     * recently auto-vacuumed, or one that needs vacuuming to recycle clog.
     *
     * Note that a database with no stats entry is not considered. The theory 
     * is that if no one has ever connected to it since the stats were last
     * initialized, it doesn't need vacuuming.
     *
     * XXX This could be improved if we had more info about whether it needs
     * vacuuming before connecting to it.  Perhaps look through the pgstats
     * data for the database's tables?  One idea is to keep track of the
     * number of new and dead tuples per database in pgstats.  However it
     * isn't clear how to construct a metric that measures that and not cause
     * starvation for less busy databases.
     */
    avdb = NULL;
    for_xid_wrap = false;
    for_multi_wrap = false;
    current_time = GetCurrentTimestamp();
    foreach (cell, dblist) {
        avw_dbase* tmp = (avw_dbase*)lfirst(cell);
        Dlelem* elem = NULL;

        /* Check to see if this one is need freeze */
        if (TransactionIdPrecedes(tmp->adw_frozenxid, xidForceLimit)) {
            if (avdb == NULL || TransactionIdPrecedes(tmp->adw_frozenxid, avdb->adw_frozenxid))
                avdb = tmp;
            for_xid_wrap = true;
            continue;
        } else if (for_xid_wrap)
            continue; /* ignore not-at-risk DBs */
#ifndef ENABLE_MULTIPLE_NODES
        else if (MultiXactIdPrecedes(tmp->adw_frozenmulti, multiForceLimit)) {
            if (avdb == NULL || MultiXactIdPrecedes(tmp->adw_frozenmulti, avdb->adw_frozenmulti))
                avdb = tmp;
            for_multi_wrap = true;
            continue;
        } else if (for_multi_wrap)
            continue; /* ignore not-at-risk DBs */
#endif

        /* Find pgstat entry if any */
        tmp->adw_entry = pgstat_fetch_stat_dbentry(tmp->adw_datid);

        /*
         * Skip a database with no pgstat entry; it means it hasn't seen any
         * activity.
         */
        if (NULL == tmp->adw_entry)
            continue;

        /*
         * Also, skip a database that appears on the database list as having
         * been processed recently (less than autovacuum_naptime seconds ago).
         * We do this so that we don't select a database which we just
         * selected, but that pgstat hasn't gotten around to updating the last
         * autovacuum time yet.
         */
        skipit = false;
        elem = t_thrd.autovacuum_cxt.DatabaseList ? DLGetTail(t_thrd.autovacuum_cxt.DatabaseList) : NULL;

        while (elem != NULL) {
            avl_dbase* dbp = (avl_dbase*)DLE_VAL(elem);

            if (dbp->adl_datid == tmp->adw_datid) {
                /*
                 * Skip this database if its next_worker value falls between
                 * the current time and the current time plus naptime.
                 */
                if (!TimestampDifferenceExceeds(dbp->adl_next_worker, current_time, 0) &&
                    !TimestampDifferenceExceeds(
                        current_time, dbp->adl_next_worker, u_sess->attr.attr_storage.autovacuum_naptime * 1000))
                    skipit = true;

                break;
            }
            elem = DLGetPred(elem);
        }
        if (skipit)
            continue;

        /*
         * Remember the db with oldest autovac time.  (If we are here, both
         * tmp->entry and db->entry must be non-null.)
         */
        if (avdb == NULL || tmp->adw_entry->last_autovac_time < avdb->adw_entry->last_autovac_time)
            avdb = tmp;
    }

    /* Found a database -- process it */
    if (avdb != NULL) {
        WorkerInfo worker = NULL;

        LWLockAcquire(AutovacuumLock, LW_EXCLUSIVE);

        /*
         * Get a worker entry from the freelist.  We checked above, so there
         * really should be a free slot -- complain very loudly if there
         * isn't.
         */
        worker = t_thrd.autovacuum_cxt.AutoVacuumShmem->av_freeWorkers;
        if (worker == NULL)
            ereport(FATAL, (errmsg("no free worker found")));

        t_thrd.autovacuum_cxt.AutoVacuumShmem->av_freeWorkers = (WorkerInfo)worker->wi_links.next;

        worker->wi_dboid = avdb->adw_datid;
        worker->wi_proc = NULL;
        worker->wi_launchtime = GetCurrentTimestamp();

        t_thrd.autovacuum_cxt.AutoVacuumShmem->av_startingWorker = worker;

        LWLockRelease(AutovacuumLock);

        SendPostmasterSignal(PMSIGNAL_START_AUTOVAC_WORKER);

        retval = avdb->adw_datid;
    } else if (skipit) {
        /*
         * If we skipped all databases on the list, rebuild it, because it
         * probably contains a dropped database.
         */
        rebuild_database_list(InvalidOid);
    }

    (void)MemoryContextSwitchTo(oldcxt);
    MemoryContextDelete(tmpcxt);

    return retval;
}

/*
 * launch_worker
 *
 * Wrapper for starting a worker from the launcher.  Besides actually starting
 * it, update the database list to reflect the next time that another one will
 * need to be started on the selected database.  The actual database choice is
 * left to do_start_worker.
 *
 * This routine is also expected to insert an entry into the database list if
 * the selected database was previously absent from the list.
 */
static void launch_worker(TimestampTz now)
{
    Oid dbid;
    Dlelem* elem = NULL;

    dbid = do_start_worker();
    if (OidIsValid(dbid)) {
        /*
         * Walk the database list and update the corresponding entry.  If the
         * database is not on the list, we'll recreate the list.
         */
        elem = (t_thrd.autovacuum_cxt.DatabaseList == NULL) ? NULL : DLGetHead(t_thrd.autovacuum_cxt.DatabaseList);
        while (elem != NULL) {
            avl_dbase* avdb = (avl_dbase*)DLE_VAL(elem);

            if (avdb->adl_datid == dbid) {
                /*
                 * add autovacuum_naptime seconds to the current time, and use
                 * that as the new "next_worker" field for this database.
                 */
                avdb->adl_next_worker =
                    TimestampTzPlusMilliseconds(now, u_sess->attr.attr_storage.autovacuum_naptime * 1000);

                DLMoveToFront(elem);
                break;
            }
            elem = DLGetSucc(elem);
        }

        /*
         * If the database was not present in the database list, we rebuild
         * the list.  It's possible that the database does not get into the
         * list anyway, for example if it's a database that doesn't have a
         * pgstat entry, but this is not a problem because we don't want to
         * schedule workers regularly into those in any case.
         */
        if (elem == NULL)
            rebuild_database_list(dbid);
    }
}

/*
 * Called from postmaster to signal a failure to fork a process to become
 * worker.	The postmaster should kill(SIGUSR2) the launcher shortly
 * after calling this function.
 */
void AutoVacWorkerFailed(void)
{
    t_thrd.autovacuum_cxt.AutoVacuumShmem->av_signal[AutoVacForkFailed] = true;
}

/* SIGHUP: set flag to re-read config file at next convenient time */
static void avl_sighup_handler(SIGNAL_ARGS)
{
    int save_errno = errno;

    t_thrd.autovacuum_cxt.got_SIGHUP = true;
    if (t_thrd.proc)
        SetLatch(&t_thrd.proc->procLatch);

    errno = save_errno;
}

/* SIGUSR2: a worker is up and running, or just finished, or failed to fork */
static void avl_sigusr2_handler(SIGNAL_ARGS)
{
    int save_errno = errno;

    t_thrd.autovacuum_cxt.got_SIGUSR2 = true;
    if (t_thrd.proc)
        SetLatch(&t_thrd.proc->procLatch);

    errno = save_errno;
}

/* SIGTERM: time to die */
static void avl_sigterm_handler(SIGNAL_ARGS)
{
    int save_errno = errno;

    t_thrd.autovacuum_cxt.got_SIGTERM = true;
    if (t_thrd.proc)
        SetLatch(&t_thrd.proc->procLatch);

    errno = save_errno;
}

/********************************************************************
 *					  AUTOVACUUM WORKER CODE
 ********************************************************************/
/*
 * AutoVacWorkerMain
 */
NON_EXEC_STATIC void AutoVacWorkerMain()
{
    sigjmp_buf local_sigjmp_buf;
    Oid dbid;
    char user[NAMEDATALEN];

    /* we are a postmaster subprocess now */
    IsUnderPostmaster = true;
    t_thrd.role = AUTOVACUUM_WORKER;

    /* reset t_thrd.proc_cxt.MyProcPid */
    t_thrd.proc_cxt.MyProcPid = gs_thread_self();

    /* record Start Time for logging */
    t_thrd.proc_cxt.MyStartTime = time(NULL);

    t_thrd.proc_cxt.MyProgName = "AutoVacWorker";

    /* Identify myself via ps */
    init_ps_display("autovacuum worker process", "", "", "");

    SetProcessingMode(InitProcessing);

    /*
     * Set up signal handlers.	We operate on databases much like a regular
     * backend, so we use the same signal handling.  See equivalent code in
     * tcop/postgres.c.
     *
     * Currently, we don't pay attention to postgresql.conf changes that
     * happen during a single daemon iteration, so we can ignore SIGHUP.
     */
    /*
     * SIGINT is used to signal canceling the current table's vacuum; SIGTERM
     * means abort and exit cleanly, and SIGQUIT means abandon ship.
     */
    (void)gspqsignal(SIGINT, StatementCancelHandler);
    (void)gspqsignal(SIGTERM, die);
    (void)gspqsignal(SIGQUIT, quickdie);
    (void)gspqsignal(SIGALRM, handle_sig_alarm);

    (void)gspqsignal(SIGPIPE, SIG_IGN);
    (void)gspqsignal(SIGUSR1, procsignal_sigusr1_handler);
    (void)gspqsignal(SIGUSR2, SIG_IGN);
    (void)gspqsignal(SIGFPE, FloatExceptionHandler);
    (void)gspqsignal(SIGCHLD, SIG_DFL);
    (void)gspqsignal(SIGHUP, SIG_IGN);
    (void)gspqsignal(SIGURG, print_stack);
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

    /* If we exit, first try and clean connections and memory */
    on_proc_exit(autoVacQuitAndClean, 0);

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
         * We can now go away.	Note that because we called InitProcess, a
         * callback was registered to do ProcKill, which will clean up
         * necessary state.
         */
        proc_exit(0);
    }
    oldTryCounter = gstrace_tryblock_entry(&curTryCounter);

    /* We can now handle ereport(ERROR) */
    t_thrd.log_cxt.PG_exception_stack = &local_sigjmp_buf;

    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();

    /*
     * Force zero_damaged_pages OFF in the autovac process, even if it is set
     * in postgresql.conf.	We don't really want such a dangerous option being
     * applied non-interactively.
     */
    SetConfigOption("zero_damaged_pages", "false", PGC_SUSET, PGC_S_OVERRIDE);

    /*
     * Force statement_timeout to zero to avoid a timeout setting from
     * preventing regular maintenance from being executed.
     */
    SetConfigOption("statement_timeout", "0", PGC_SUSET, PGC_S_OVERRIDE);

    /*
     * Force default_transaction_isolation to READ COMMITTED.  We don't want
     * to pay the overhead of serializable mode, nor add any risk of causing
     * deadlocks or delaying other transactions.
     */
    SetConfigOption("default_transaction_isolation", "read committed", PGC_SUSET, PGC_S_OVERRIDE);

    /*
     * Get the info about the database we're going to work on.
     */
    LWLockAcquire(AutovacuumLock, LW_EXCLUSIVE);

    /*
     * beware of startingWorker being INVALID; this should normally not
     * happen, but if a worker fails after forking and before this, the
     * launcher might have decided to remove it from the queue and start
     * again.
     */
    if (t_thrd.autovacuum_cxt.AutoVacuumShmem->av_startingWorker != NULL) {
        t_thrd.autovacuum_cxt.MyWorkerInfo = t_thrd.autovacuum_cxt.AutoVacuumShmem->av_startingWorker;
        dbid = t_thrd.autovacuum_cxt.MyWorkerInfo->wi_dboid;
        t_thrd.autovacuum_cxt.MyWorkerInfo->wi_proc = t_thrd.proc;

        /* insert into the running list */
        SHMQueueInsertBefore(
            &t_thrd.autovacuum_cxt.AutoVacuumShmem->av_runningWorkers, &t_thrd.autovacuum_cxt.MyWorkerInfo->wi_links);

        /*
         * remove from the "starting" pointer, so that the launcher can start
         * a new worker if required
         */
        t_thrd.autovacuum_cxt.AutoVacuumShmem->av_startingWorker = NULL;
        LWLockRelease(AutovacuumLock);

        on_shmem_exit(FreeWorkerInfo, 0);
        on_shmem_exit(PGXCNodeCleanAndRelease, 0);

        /* wake up the launcher */
        if (t_thrd.autovacuum_cxt.AutoVacuumShmem->av_launcherpid != 0)
            gs_signal_send(t_thrd.autovacuum_cxt.AutoVacuumShmem->av_launcherpid, SIGUSR2);
    } else {
        /* no worker entry for me, go away */
        ereport(WARNING, (errmsg("autovacuum worker started without a worker entry")));
        dbid = InvalidOid;
        LWLockRelease(AutovacuumLock);
    }

    if (OidIsValid(dbid)) {
        char dbname[NAMEDATALEN];
        MemoryContext oldcontext = NULL;

        /*
         * Report autovac startup to the stats collector.  We deliberately do
         * this before Init openGauss, so that the last_autovac_time will get
         * updated even if the connection attempt fails.  This is to prevent
         * autovac from getting "stuck" repeatedly selecting an unopenable
         * database, rather than making any progress on stuff it can connect
         * to.
         */
        pgstat_report_autovac(dbid);
        AUTOVAC_LOG(LOG, "report autovac startup on database %u to stats collector", dbid);

        /*
         * Connect to the selected database
         *
         * Note: if we have selected a just-deleted database (due to using
         * stale stats info), we'll fail and exit here.
         */
        t_thrd.proc_cxt.PostInit->SetDatabaseAndUser(NULL, dbid, NULL);
        t_thrd.proc_cxt.PostInit->InitAutoVacWorker();
        t_thrd.proc_cxt.PostInit->GetDatabaseName(dbname);
#ifndef ENABLE_MULTIPLE_NODES
        /* forbid smp in autovacuum thread */
        AutoDopControl dopControl;
        dopControl.CloseSmp();
#endif
        SetProcessingMode(NormalProcessing);
        pgstat_report_appname("AutoVacWorker");
        pgstat_report_activity(STATE_IDLE, NULL);

        set_ps_display(dbname, false);
        ereport(GetVacuumLogLevel(), (errmsg("start autovacuum on database \"%s\"", dbname)));

        if (u_sess->attr.attr_security.PostAuthDelay)
            pg_usleep(u_sess->attr.attr_security.PostAuthDelay * 1000000L);

        /*
         * Create the memory context we will use in the main loop.
         *
         * t_thrd.mem_cxt.msg_mem_cxt is reset once per iteration of the main loop, ie, upon
         * completion of processing of each command message from the client.
         */
        t_thrd.mem_cxt.msg_mem_cxt = AllocSetContextCreate(t_thrd.top_mem_cxt, "MessageContext",
            ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);

        t_thrd.mem_cxt.mask_password_mem_cxt = AllocSetContextCreate(t_thrd.top_mem_cxt, "MaskPasswordCtx",
            ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);

        /*
         * Create a resource owner to keep track of our resources (currently only
         * buffer pins).
         */
        t_thrd.utils_cxt.CurrentResourceOwner = ResourceOwnerCreate(NULL, "AutoVacuumWorker",
            THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));
#if (!defined(ENABLE_MULTIPLE_NODES)) && (!defined(ENABLE_PRIVATEGAUSS))
        LoadSqlPlugin();
#endif

        oldcontext = MemoryContextSwitchTo(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));
        if (u_sess->proc_cxt.MyProcPort->database_name)
            pfree_ext(u_sess->proc_cxt.MyProcPort->database_name);
        if (u_sess->proc_cxt.MyProcPort->user_name)
            pfree_ext(u_sess->proc_cxt.MyProcPort->user_name);
        u_sess->proc_cxt.MyProcPort->database_name = pstrdup(dbname);
        u_sess->proc_cxt.MyProcPort->user_name = (char*)GetSuperUserName((char*)user);
        (void)MemoryContextSwitchTo(oldcontext);

        /* Get classified list of node Oids for do analyze in coordinator. */
        exec_init_poolhandles();

        /* And do an appropriate amount of work */
        t_thrd.autovacuum_cxt.recentXid = ReadNewTransactionId();
        t_thrd.autovacuum_cxt.recentMulti = ReadNextMultiXactId();
        do_autovacuum();
    }

    /*
     * The launcher will be notified of my death in ProcKill, *if* we managed
     * to get a worker slot at all
     */
    /* All done, go away */
    proc_exit(0);
}

/*
 * Return a WorkerInfo to the free list
 */
static void FreeWorkerInfo(int code, Datum arg)
{
    if (t_thrd.autovacuum_cxt.MyWorkerInfo != NULL) {
        LWLockAcquire(AutovacuumLock, LW_EXCLUSIVE);
        /* double check */
        if (t_thrd.autovacuum_cxt.MyWorkerInfo == NULL) {
            LWLockRelease(AutovacuumLock);
            return;
        }
        /*
         * Wake the launcher up so that he can launch a new worker immediately
         * if required.  We only save the launcher's PID in local memory here;
         * the actual signal will be sent when the PGPROC is recycled.	Note
         * that we always do this, so that the launcher can rebalance the cost
         * limit setting of the remaining workers.
         *
         * We somewhat ignore the risk that the launcher changes its PID
         * between us reading it and the actual kill; we expect ProcKill to be
         * called shortly after us, and we assume that PIDs are not reused too
         * quickly after a process exits.
         */
        t_thrd.autovacuum_cxt.AutovacuumLauncherPid = t_thrd.autovacuum_cxt.AutoVacuumShmem->av_launcherpid;

        SHMQueueDelete(&t_thrd.autovacuum_cxt.MyWorkerInfo->wi_links);
        t_thrd.autovacuum_cxt.MyWorkerInfo->wi_links.next =
            (SHM_QUEUE*)t_thrd.autovacuum_cxt.AutoVacuumShmem->av_freeWorkers;
        t_thrd.autovacuum_cxt.MyWorkerInfo->wi_dboid = InvalidOid;
        t_thrd.autovacuum_cxt.MyWorkerInfo->wi_tableoid = InvalidOid;
        t_thrd.autovacuum_cxt.MyWorkerInfo->wi_parentoid = InvalidOid;
        t_thrd.autovacuum_cxt.MyWorkerInfo->wi_sharedrel = false;
        t_thrd.autovacuum_cxt.MyWorkerInfo->wi_proc = NULL;
        t_thrd.autovacuum_cxt.MyWorkerInfo->wi_launchtime = 0;
        t_thrd.autovacuum_cxt.MyWorkerInfo->wi_cost_delay = 0;
        t_thrd.autovacuum_cxt.MyWorkerInfo->wi_cost_limit = 0;
        t_thrd.autovacuum_cxt.MyWorkerInfo->wi_cost_limit_base = 0;
        t_thrd.autovacuum_cxt.AutoVacuumShmem->av_freeWorkers = t_thrd.autovacuum_cxt.MyWorkerInfo;
        /* not mine anymore */
        t_thrd.autovacuum_cxt.MyWorkerInfo = NULL;

        /*
         * now that we're inactive, cause a rebalancing of the surviving
         * workers
         */
        t_thrd.autovacuum_cxt.AutoVacuumShmem->av_signal[AutoVacRebalance] = true;
        LWLockRelease(AutovacuumLock);
    }
}

/*
 * Update the cost-based delay parameters, so that multiple workers consume
 * each a fraction of the total available I/O.
 */
void AutoVacuumUpdateDelay(void)
{
    if (t_thrd.autovacuum_cxt.MyWorkerInfo) {
        u_sess->attr.attr_storage.VacuumCostDelay = t_thrd.autovacuum_cxt.MyWorkerInfo->wi_cost_delay;
        u_sess->attr.attr_storage.VacuumCostLimit = t_thrd.autovacuum_cxt.MyWorkerInfo->wi_cost_limit;
    }
}

/*
 * autovac_balance_cost
 *		Recalculate the cost limit setting for each active worker.
 *
 * Caller must hold the AutovacuumLock in exclusive mode.
 */
static void autovac_balance_cost(void)
{
    /*
     * The idea here is that we ration out I/O equally.  The amount of I/O
     * that a worker can consume is determined by cost_limit/cost_delay, so we
     * try to equalize those ratios rather than the raw limit settings.
     *
     * note: in cost_limit, zero also means use value from elsewhere, because
     * zero is not a valid value.
     */
    int vac_cost_limit =
        (u_sess->attr.attr_storage.autovacuum_vac_cost_limit > 0 ? u_sess->attr.attr_storage.autovacuum_vac_cost_limit
                                                                 : u_sess->attr.attr_storage.VacuumCostLimit);
    int vac_cost_delay =
        (u_sess->attr.attr_storage.autovacuum_vac_cost_delay >= 0 ? u_sess->attr.attr_storage.autovacuum_vac_cost_delay
                                                                  : u_sess->attr.attr_storage.VacuumCostDelay);
    double cost_total;
    double cost_avail;
    WorkerInfo worker = NULL;

    /* not set? nothing to do */
    if (vac_cost_limit <= 0 || vac_cost_delay <= 0) {
        return;
    }

    /* caculate the total base cost limit of active workers */
    cost_total = 0.0;
    worker = (WorkerInfo)SHMQueueNext(&t_thrd.autovacuum_cxt.AutoVacuumShmem->av_runningWorkers,
        &t_thrd.autovacuum_cxt.AutoVacuumShmem->av_runningWorkers, offsetof(WorkerInfoData, wi_links));
    while (worker != NULL) {
        if (worker->wi_proc != NULL && worker->wi_cost_limit_base > 0 && worker->wi_cost_delay > 0)
            cost_total += (double)worker->wi_cost_limit_base / worker->wi_cost_delay;

        worker = (WorkerInfo)SHMQueueNext(&t_thrd.autovacuum_cxt.AutoVacuumShmem->av_runningWorkers, &worker->wi_links,
            offsetof(WorkerInfoData, wi_links));
    }
    /* there are no cost limits -- nothing to do */
    if (cost_total <= 0) {
        return;
    }

    /*
     * Adjust cost limit of each active worker to balance the total of cost
     * limit to autovacuum_vacuum_cost_limit.
     */
    cost_avail = (double)vac_cost_limit / vac_cost_delay;
    worker = (WorkerInfo)SHMQueueNext(&t_thrd.autovacuum_cxt.AutoVacuumShmem->av_runningWorkers,
        &t_thrd.autovacuum_cxt.AutoVacuumShmem->av_runningWorkers, offsetof(WorkerInfoData, wi_links));
    while (worker != NULL) {
        if (worker->wi_proc != NULL && worker->wi_cost_limit_base > 0 && worker->wi_cost_delay > 0) {
            int limit = (int)(cost_avail * worker->wi_cost_limit_base / cost_total);

            /*
             * We put a lower bound of 1 on the cost_limit, to avoid division-
             * by-zero in the vacuum code.	Also, in case of roundoff trouble
             * in these calculations, let's be sure we don't ever set
             * cost_limit to more than the base value.
             */
            worker->wi_cost_limit = Max(Min(limit, worker->wi_cost_limit_base), 1);

            AUTOVAC_LOG(LOG, "autovac_balance_cost(pid=%lu db=%u, rel=%u, parent=%u, cost_limit=%d, "
                "cost_limit_base=%d, cost_delay=%d)",
                worker->wi_proc->pid, worker->wi_dboid, worker->wi_tableoid, worker->wi_parentoid,
                worker->wi_cost_limit, worker->wi_cost_limit_base, worker->wi_cost_delay);
        }

        worker = (WorkerInfo)SHMQueueNext(&t_thrd.autovacuum_cxt.AutoVacuumShmem->av_runningWorkers,
            &worker->wi_links,
            offsetof(WorkerInfoData, wi_links));
    }
}

/*
 * get_database_list
 *		Return a list of all databases found in pg_database.
 *
 * The list and associated data is allocated in the caller's memory context,
 * which is in charge of ensuring that it's properly cleaned up afterwards.
 *
 * Note: this is the only function in which the autovacuum launcher uses a
 * transaction.  Although we aren't attached to any particular database and
 * therefore can't access most catalogs, we do have enough infrastructure
 * to do a seqscan on pg_database.
 */
static List* get_database_list(void)
{
    List* dblist = NIL;
    Relation rel;
    TableScanDesc scan;
    HeapTuple tup;
    MemoryContext resultcxt;

    /* This is the context that we will allocate our output data in */
    resultcxt = CurrentMemoryContext;

    /*
     * Start a transaction so we can access pg_database, and get a snapshot.
     * We don't have a use for the snapshot itself, but we're interested in
     * the secondary effect that it sets RecentGlobalXmin.	(This is critical
     * for anything that reads heap pages, because HOT may decide to prune
     * them even if the process doesn't attempt to modify any tuples.)
     */
    StartTransactionCommand();
    (void)GetTransactionSnapshot();

    rel = heap_open(DatabaseRelationId, AccessShareLock);
    scan = tableam_scan_begin(rel, SnapshotNow, 0, NULL);

    while (HeapTupleIsValid(tup = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection))) {
        Form_pg_database pgdatabase = (Form_pg_database)GETSTRUCT(tup);
        avw_dbase* avdb = NULL;
        MemoryContext oldcxt;

        /*
         * Allocate our results in the caller's context, not the
         * transaction's. We do this inside the loop, and restore the original
         * context at the end, so that leaky things like heap_getnext() are
         * not called in a potentially long-lived context.
         */
        oldcxt = MemoryContextSwitchTo(resultcxt);

        avdb = (avw_dbase*)palloc(sizeof(avw_dbase));

        avdb->adw_datid = HeapTupleGetOid(tup);
        avdb->adw_name = pstrdup(NameStr(pgdatabase->datname));

        bool isNull = false;
        TransactionId datfrozenxid;
        Datum xid64datum = heap_getattr(tup, Anum_pg_database_datfrozenxid64, RelationGetDescr(rel), &isNull);

        if (isNull) {
            datfrozenxid = pgdatabase->datfrozenxid;

            if (TransactionIdPrecedes(t_thrd.xact_cxt.ShmemVariableCache->nextXid, datfrozenxid))
                datfrozenxid = FirstNormalTransactionId;
        } else
            datfrozenxid = DatumGetTransactionId(xid64datum);
#ifndef ENABLE_MULTIPLE_NODES
        Datum mxidDatum = heap_getattr(tup, Anum_pg_database_datminmxid, RelationGetDescr(rel), &isNull);
        MultiXactId datminmxid = isNull ? FirstMultiXactId : DatumGetTransactionId(mxidDatum);
        avdb->adw_frozenmulti = datminmxid;
#endif
        avdb->adw_frozenxid = datfrozenxid;
        /* this gets set later: */
        avdb->adw_entry = NULL;

        dblist = lappend(dblist, avdb);
        (void)MemoryContextSwitchTo(oldcxt);
    }

    tableam_scan_end(scan);
    heap_close(rel, AccessShareLock);

    CommitTransactionCommand();

    return dblist;
}

/*
 * 1. just support global statistic since local statistic is gradually abandoned
 * 2. just support traditional-sample analyze since percent-sample analyze is too inefficient
 */
#define DO_ANALYZE                                                                 \
    ((AUTOVACUUM_DO_ANALYZE_VACUUM == u_sess->attr.attr_storage.autovacuum_mode || \
         AUTOVACUUM_DO_ANALYZE == u_sess->attr.attr_storage.autovacuum_mode) &&    \
        u_sess->attr.attr_sql.enable_global_stats && 0 < default_statistics_target)
#define DO_VACUUM                                                                 \
    (AUTOVACUUM_DO_ANALYZE_VACUUM == u_sess->attr.attr_storage.autovacuum_mode || \
        AUTOVACUUM_DO_VACUUM == u_sess->attr.attr_storage.autovacuum_mode)

/*
 * check if the relation can do auto-analyze or auto-vacuum
 */
void relation_support_autoavac(HeapTuple tuple, bool* enable_analyze, bool* enable_vacuum, bool* is_internal_relation)
{
    bytea* relopts = NULL;
    Form_pg_class classForm = (Form_pg_class)GETSTRUCT(tuple);

    Assert(PointerIsValid(enable_analyze));
    Assert(PointerIsValid(enable_vacuum));

    *enable_analyze = false;
    *enable_vacuum = false;
    *is_internal_relation = false;

    /* skip all autovac actions */
    if (IS_PGXC_COORDINATOR && !u_sess->attr.attr_storage.autovacuum_start_daemon)
        return;

    /*
     * 1. never analyze internal table since we never select them directly
     * 2. just analyze row/colume orientation table since analyze hdfs table is too inefficient
     */
    relopts = extractRelOptions(tuple, GetDefaultPgClassDesc(), InvalidOid);
    if (StdRelOptGetInternalMask(relopts)) {
        /* do nothing, but set is_internal_relation to be true */
        *is_internal_relation = true;

        /* if it is mlog or matmap then set it */
        if (StdRelOptIsRowStore(relopts) && (ISMATMAP(classForm->relname.data) || ISMLOG(classForm->relname.data))) {
            *enable_analyze = true;
            *enable_vacuum = true;
        }
    } else if (StdRelOptIsColStore(relopts)) {
        *enable_analyze = true;
    } else if (StdRelOptIsRowStore(relopts)) {
        *enable_analyze = true;
        *enable_vacuum = true;
    }

    if (StatisticRelationId == HeapTupleGetOid(tuple) || RELKIND_TOASTVALUE == classForm->relkind || !DO_ANALYZE) {
        *enable_analyze = false;
    }

    if (!DO_VACUUM) {
        *enable_vacuum = false;
    }

    if (relopts != NULL)
        pfree_ext(relopts);
        
    /*
     * 1. data in temp/unlogged is short-lived, so do nothing for temp/unlogged table
     * 2. foreign table dose not have stat info, so just support ordinary table
     */    
    if (RELPERSISTENCE_PERMANENT != classForm->relpersistence || RELKIND_RELATION != classForm->relkind) {
        *enable_analyze = false;
        *is_internal_relation = false;
    }
    /* It's useless to  vacuum toast directly at CN in distribute mode, ignore it */
    if (RELKIND_RELATION != classForm->relkind
#ifndef ENABLE_MULTIPLE_NODES
        && RELKIND_TOASTVALUE != classForm->relkind
#endif
        ) {
        *enable_vacuum = false;
    }
}

bool allow_autoanalyze(HeapTuple tuple)
{
    bool enable_analyze = false;
    bool enable_vacuum = false;
    bool is_internal_relation = false;
    relation_support_autoavac(tuple, &enable_analyze, &enable_vacuum, &is_internal_relation);

    return enable_analyze;
}

static void AddApplicationNameToPoolerParams()
{
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord() && IsAutoVacuumWorkerProcess()) {
        StringInfoData str;
        initStringInfo(&str);
        appendStringInfo(&str, "SET application_name = '%s';", AUTO_VACUUM_WORKER);
        (void)register_pooler_session_param("application_name", str.data, POOL_CMD_GLOBAL_SET);
        pfree_ext(str.data);
    }
}

static void DeleteApplicationNameFromPoolerParams()
{
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord() && IsAutoVacuumWorkerProcess()) {
        delete_pooler_session_params("application_name");
    }
}

static void fetch_global_autovac_info()
{
    PgStat_StatTabKey tablekey;
    bool connected = false;
    StringInfoData buf;

    /*
     * Dose not fetch global stat info from all datanodes if
     * 1. autovacuum = off
     * 2. autovacuum = on and u_sess->attr.attr_storage.autovacuum_mode = none
     */
    if (!u_sess->attr.attr_storage.autovacuum_start_daemon ||
        AUTOVACUUM_DO_NONE == u_sess->attr.attr_storage.autovacuum_mode)
        return;

    initStringInfo(&buf);
    if (DO_VACUUM) {
        appendStringInfo(&buf,
            "with f as (select nspname, relname, partname, pg_catalog.sum(n_dead_tuples) as n_dead_tuples, "
            "pg_catalog.sum(n_live_tuples) as n_live_tuples, pg_catalog.sum(changes_since_analyze) "
            "as changes_since_analyze, pg_catalog.count(1) as count from "
            "pg_catalog.pg_total_autovac_tuples(%s) group by nspname, relname, partname), "
            "t as(SELECT c.oid as relid,n.nspname AS nspname, c.relname AS relname, "
            "case when p.parttype = 'r' then null else p.oid end as partid, "
            "case when p.parttype = 'r' then null else p.relname end as partname, x.pclocatortype as locatortype "
            "FROM pg_class c INNER JOIN pg_namespace n ON n.oid = c.relnamespace INNER JOIN pgxc_class x "
            "on x.pcrelid = c.oid LEFT JOIN pg_partition p on c.oid = p.parentid "
            "WHERE c.relkind = 'r' and n.nspname not in ('pg_toast','cstore'))"
            "select t.relid, t.partid, (case when locatortype = 'R' then (f.n_dead_tuples/f.count) else "
            "f.n_dead_tuples end)::bigint as n_dead_tuples, "
            "(case when locatortype = 'R' then (f.n_live_tuples/f.count) else f.n_live_tuples end)::bigint as "
            "n_live_tuples, (case when locatortype = 'R' then "
            "(f.changes_since_analyze/f.count) else f.changes_since_analyze end)::bigint as changes_since_analyze "
            "from t inner join f on (t.nspname = f.nspname and t.relname = f.relname "
            "and (t.partname = f.partname or (t.partname is null and f.partname is null))) ", "false");
    } else {
        appendStringInfo(&buf, "with f as (select nspname, relname, pg_catalog.sum(n_dead_tuples) as n_dead_tuples, "
            "pg_catalog.sum(changes_since_analyze) as changes_since_analyze, pg_catalog.count(1) as count "
            "from pg_catalog.pg_total_autovac_tuples(%s) group by nspname, relname), t as(SELECT c.oid as relid, "
            "n.nspname AS nspname, c.relname AS relname, x.pclocatortype as locatortype FROM pg_class c "
            "INNER JOIN pg_namespace n ON n.oid = c.relnamespace INNER JOIN pgxc_class x on x.pcrelid = c.oid "
            "WHERE c.relkind = 'r' and c.relpersistence = 'p' and n.nspname not in ('pg_toast','cstore'))"
            "select t.relid, NULL AS partid, "
            "(case when locatortype = 'R' then (f.n_dead_tuples/f.count) else f.n_dead_tuples end)::bigint as "
            "n_dead_tuples, 0 AS n_live_tuples, "
            "(case when locatortype = 'R' then (f.changes_since_analyze/f.count) else f.changes_since_analyze "
            "end)::bigint as changes_since_analyze "
            "from t inner join f on (t.nspname = f.nspname and t.relname = f.relname) ", "true");
    }

    AUTOVAC_LOG(DEBUG2, "FETCH GLOABLE AUTOVAC INFO STRING: %s", buf.data);

    PushActiveSnapshot(GetTransactionSnapshot());
    AddApplicationNameToPoolerParams();
    PG_TRY();
    {
        DEBUG_MOD_START_TIMER(MOD_AUTOVAC);
        SPI_STACK_LOG("connect", NULL, NULL);
        if (SPI_OK_CONNECT != SPI_connect()) {
            ereport(ERROR, (errcode(ERRCODE_OPERATE_FAILED), errmsg("Unable to connect to execute internal query.")));
        }
        connected = true;
        DEBUG_MOD_STOP_TIMER(MOD_AUTOVAC, "AUTOVAC TIMER: build SPI connect");

        if (SPI_OK_SELECT != SPI_execute(buf.data, true, 0)) {
            ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("fail to execute query")));
        }
        DEBUG_MOD_STOP_TIMER(MOD_AUTOVAC, "AUTOVAC TIMER: execte SQL to fetch global autovac info");

        pfree_ext(buf.data);

        for (uint32 i = 0; i < SPI_processed; i++) {
            Oid relid;
            Oid partid;
            int64 n_dead_tuples = 0;
            int64 n_live_tuples = 0;
            int64 changes_since_analyze = 0;
            bool isnull = false;
            bool partid_isnull = true;
            avw_info* entry = NULL;

            relid = DatumGetObjectId(SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1, &isnull));
            partid = DatumGetObjectId(SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 2, &partid_isnull));
            n_dead_tuples = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 3, &isnull));
            n_live_tuples = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 4, &isnull));
            changes_since_analyze =
                DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 5, &isnull));

            if (partid_isnull) {
                tablekey.tableid = relid;
                tablekey.statFlag = InvalidOid;
            } else {
                tablekey.tableid = partid;
                tablekey.statFlag = relid;
            }

            entry =
                (avw_info*)hash_search(t_thrd.autovacuum_cxt.pgStatAutoVacInfo, (void*)(&tablekey), HASH_ENTER, NULL);
            entry->n_dead_tuples = n_dead_tuples;
            entry->n_live_tuples = n_live_tuples;
            entry->changes_since_analyze = changes_since_analyze;
        }
        DEBUG_MOD_STOP_TIMER(MOD_AUTOVAC, "AUTOVAC TIMER: process %u SPI tuples", SPI_processed);

        connected = false;
        SPI_STACK_LOG("finish", NULL, NULL);
        if (SPI_OK_FINISH != SPI_finish()) {
            ereport(ERROR, (errcode(ERRCODE_OPERATE_FAILED), errmsg("SPI_finish failed")));
        }
        PopActiveSnapshot();
        u_sess->debug_query_id = 0;
    }
    PG_CATCH(); /* Clean up in case of error. */
    {
        DeleteApplicationNameFromPoolerParams();
        if (connected) {
            SPI_STACK_LOG("finish", NULL, NULL);
            SPI_finish();
        }

        /* Carry on with error handling. */
        PopActiveSnapshot();
        PG_RE_THROW();
        u_sess->debug_query_id = 0;
    }
    PG_END_TRY();
    DeleteApplicationNameFromPoolerParams();
}

/*
 * Process a database table-by-table
 *
 * Note that CHECK_FOR_INTERRUPTS is supposed to be used in certain spots in
 * order not to ignore shutdown commands for too long.
 */
static void do_autovacuum(void)
{
    Relation classRel = NULL;
    HeapTuple tuple = NULL;
    TableScanDesc relScan = NULL;
    Form_pg_database dbForm = NULL;
    List* table_oids = NIL;
    HASHCTL partitioned_tables_ctl;
    HTAB* partitioned_tables_map = NULL;
    HASHCTL table_relopt_ctl;
    HTAB* table_relopt_map = NULL;
    HASHCTL toast_table_ctl;
    HTAB* toast_table_map = NULL;
    ListCell* volatile cell = NULL;
    PgStat_StatDBEntry* shared = NULL;
    PgStat_StatDBEntry* dbentry = NULL;
    BufferAccessStrategy bstrategy;
    HASHCTL avinfo_ctl;
    bool datallowconn = true;
    bool local_autovacuum = true;       /* just do autovacuum in current instance */
    bool freeze_autovacuum = false; /* just do vacuum since need freeze the old tuple */
    bool is_internal_relation = false;  /* whether current relation is an internal relation */
    ScanKeyData key[1];
    TableScanDesc partScan;
    TableScanDesc subpartScan;
    Relation partRel;
    HeapTuple partTuple;
    TupleDesc pg_class_desc;
    vacuum_object* vacObj = NULL;
    errno_t rc = EOK;

    /*
     * StartTransactionCommand and CommitTransactionCommand will automatically
     * switch to other contexts.  We need this one to keep the list of
     * relations to vacuum/analyze across transactions.
     */
    t_thrd.autovacuum_cxt.AutovacMemCxt = AllocSetContextCreate(
        t_thrd.top_mem_cxt, "AV worker", ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
    (void)MemoryContextSwitchTo(t_thrd.autovacuum_cxt.AutovacMemCxt);

    /*
     * may be NULL if we couldn't find an entry (only happens if we are
     * forcing a vacuum for anti-wrap purposes).
     */
    dbentry = pgstat_fetch_stat_dbentry(u_sess->proc_cxt.MyDatabaseId);

    /* Start a transaction so our commands have one to play into. */
    StartTransactionCommand();

    /*
     * Clean up any dead statistics collector entries for this DB. We always
     * want to do this exactly once per DB-processing cycle, even if we find
     * nothing worth vacuuming in the database.
     */
    DEBUG_MOD_START_TIMER(MOD_AUTOVAC);
    pgstat_vacuum_stat();
    DEBUG_MOD_STOP_TIMER(MOD_AUTOVAC, "AUTOVAC TIMER: Clean up dead statistics collector entries for current DB");

    /*
     * Find the pg_database entry and select the default freeze ages. We use
     * zero in template and nonconnectable databases, else the system-wide
     * default.
     */
    tuple = SearchSysCache1(DATABASEOID, ObjectIdGetDatum(u_sess->proc_cxt.MyDatabaseId));
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for database %u", u_sess->proc_cxt.MyDatabaseId)));
    dbForm = (Form_pg_database)GETSTRUCT(tuple);
    datallowconn = dbForm->datallowconn;
    if (dbForm->datistemplate || !dbForm->datallowconn) {
        t_thrd.autovacuum_cxt.default_freeze_min_age = 0;
        t_thrd.autovacuum_cxt.default_freeze_table_age = 0;
    } else {
        t_thrd.autovacuum_cxt.default_freeze_min_age = u_sess->attr.attr_storage.vacuum_freeze_min_age;
        t_thrd.autovacuum_cxt.default_freeze_table_age = u_sess->attr.attr_storage.vacuum_freeze_table_age;
    }

    ReleaseSysCache(tuple);

#ifdef PGXC
    /* skip autovacuum when it is doing inplaceupgrade */
    if (u_sess->attr.attr_common.upgrade_mode == 1) {
        CommitTransactionCommand();
        return;
    }

    if (false == datallowconn || IS_SINGLE_NODE) {
        /* for database that refuses to accpet connections, for single node mode, autovacuum remains the same as PG */
        local_autovacuum = true;
        freeze_autovacuum = false;
    } else if (IS_PGXC_COORDINATOR && PgxcIsCentralCoordinator(g_instance.attr.attr_common.PGXCNodeName)) {
        /* for database that can accept connections On CCN */
        local_autovacuum = false;
        freeze_autovacuum = false;
    } else {
        /* for database that can accept connections On non-CCN */
        local_autovacuum = true;
        freeze_autovacuum = true;
    }
#endif

    /* StartTransactionCommand changed elsewhere */
    (void)MemoryContextSwitchTo(t_thrd.autovacuum_cxt.AutovacMemCxt);

    t_thrd.autovacuum_cxt.pgStatAutoVacInfo = NULL;
    if (IS_PGXC_COORDINATOR && false == local_autovacuum) {
        rc = memset_s(&avinfo_ctl, sizeof(avinfo_ctl), 0, sizeof(avinfo_ctl));
        securec_check(rc, "", "");

        avinfo_ctl.keysize = sizeof(PgStat_StatTabKey);
        avinfo_ctl.entrysize = sizeof(avw_info);
        avinfo_ctl.hcxt = t_thrd.autovacuum_cxt.AutovacMemCxt;
        avinfo_ctl.hash = tag_hash;

        t_thrd.autovacuum_cxt.pgStatAutoVacInfo = hash_create(
            "autovac information of user-define table", 512, &avinfo_ctl, HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

        fetch_global_autovac_info();
    }

    /* The database hash where pgstat keeps shared relations */
    shared = pgstat_fetch_stat_dbentry(InvalidOid);

    classRel = heap_open(RelationRelationId, AccessShareLock);

    /* create a copy so we can use it after closing pg_class */
    pg_class_desc = CreateTupleDescCopy(RelationGetDescr(classRel));

    /* create hash table for partitoned relid <-> autovac info mapping */
    rc = memset_s(&partitioned_tables_ctl, sizeof(partitioned_tables_ctl), 0, sizeof(partitioned_tables_ctl));
    securec_check(rc, "", "");

    partitioned_tables_ctl.keysize = sizeof(Oid);
    partitioned_tables_ctl.entrysize = sizeof(at_partitioned_table);
    partitioned_tables_ctl.hash = oid_hash;

    partitioned_tables_map =
        hash_create("partitioned relid to autovac info map", 10, &partitioned_tables_ctl, HASH_ELEM | HASH_FUNCTION);

    /* create hash table for toastid <-> main relid mapping */
    rc = memset_s(&toast_table_ctl, sizeof(toast_table_ctl), 0, sizeof(toast_table_ctl));
    securec_check(rc, "", "");

    toast_table_ctl.keysize = sizeof(Oid);
    toast_table_ctl.entrysize = sizeof(av_toastid_mainid);
    toast_table_ctl.hash = oid_hash;

    toast_table_map = hash_create("TOAST to main relid map", 100, &toast_table_ctl, HASH_ELEM | HASH_FUNCTION);

    /* create hash table for reloptions <-> main relid mapping */
    rc = memset_s(&table_relopt_ctl, sizeof(table_relopt_ctl), 0, sizeof(table_relopt_ctl));
    securec_check(rc, "", "");

    table_relopt_ctl.keysize = sizeof(Oid);
    table_relopt_ctl.entrysize = sizeof(av_relation);
    table_relopt_ctl.hash = oid_hash;

    table_relopt_map = hash_create("main relid to rel options map", 100, &table_relopt_ctl, HASH_ELEM | HASH_FUNCTION);

    /*
     * Scan pg_class to determine which tables to vacuum.
     *
     * relations and materialized views, and on the second one we collect
     * TOAST tables. The reason for doing the second pass is that during it we
     * want to use the main relation's pg_class.reloptions entry if the TOAST
     * table does not have any, and we cannot obtain it unless we know
     * beforehand what's the main  table OID.
     *
     * We need to check TOAST tables separately because in cases with short,
     * wide tables there might be proportionally much more activity in the
     * TOAST table than in its parent.
     */
    relScan = tableam_scan_begin(classRel, SnapshotNow, 0, NULL);

    /*
     * On the first pass, we collect main tables to vacuum, and also the main
     * table relid to TOAST relid mapping.
     */
    while ((tuple = (HeapTuple) tableam_scan_getnexttuple(relScan, ForwardScanDirection)) != NULL) {
        Form_pg_class classForm = (Form_pg_class)GETSTRUCT(tuple);
        PgStat_StatTabEntry* tabentry = NULL;
        AutoVacOpts* relopts = NULL;
        Oid relid = HeapTupleGetOid(tuple);
        bool dovacuum = false;
        bool doanalyze = false;
        bool need_freeze = false;
        bool enable_analyze = false;
        bool enable_vacuum = false;

        if (classForm->relkind != RELKIND_RELATION &&
            classForm->relkind != RELKIND_MATVIEW)
            continue;

        /* We cannot safely process other backends' temp tables, so skip 'em. */
        if (classForm->relpersistence == RELPERSISTENCE_TEMP ||
            classForm->relpersistence == RELPERSISTENCE_GLOBAL_TEMP) {
            continue;
        }

        /* Here we skipped relation_support_autoavac() and relation_needs_vacanalyze() checks
         * for Ustore partitioned tables
         */
        bytea *rawRelopts = extractRelOptions(tuple, pg_class_desc, InvalidOid);
        if (rawRelopts != NULL && RelationIsTableAccessMethodUStoreType(rawRelopts) &&
            isPartitionedRelation(classForm)) {
            vacObj = (vacuum_object*)palloc(sizeof(vacuum_object));
            vacObj->tab_oid = relid;
            vacObj->parent_oid = InvalidOid;
            vacObj->dovacuum = true;
            vacObj->dovacuum_toast = false;
            vacObj->doanalyze = false;
            vacObj->need_freeze = false;
            vacObj->is_internal_relation = false;
            vacObj->flags = VACFLG_MAIN_PARTITION;
            table_oids = lappend(table_oids, vacObj);
            continue;
        }

        /* Fetch reloptions for this table */
        relopts = extract_autovac_opts(tuple, pg_class_desc);

        /* Fetch the pgstat entry for this table */
        tabentry = get_pgstat_tabentry_relid(relid, classForm->relisshared, InvalidOid, shared, dbentry);
        relation_support_autoavac(tuple, &enable_analyze, &enable_vacuum, &is_internal_relation);

        /* Check if it needs vacuum or analyze */
        relation_needs_vacanalyze(relid, relopts, rawRelopts, classForm, tuple, tabentry, enable_analyze, enable_vacuum,
            false, &dovacuum, &doanalyze, &need_freeze);

        if (freeze_autovacuum) {
            dovacuum = need_freeze;
            doanalyze = false;
        }

        /* Here we skipped relation_support_autoavac() and relation_needs_vacanalyze() checks
         * for Ustore partitioned tables
         */
        bool isUstorePartitionTable = (rawRelopts != NULL && RelationIsTableAccessMethodUStoreType(rawRelopts) &&
            isPartitionedRelation(classForm));

        /* relations that need work are added to table_oids */
        if (dovacuum || doanalyze || isUstorePartitionTable) {
            vacObj = (vacuum_object*)palloc(sizeof(vacuum_object));
            vacObj->tab_oid = relid;
            vacObj->parent_oid = InvalidOid;
            vacObj->dovacuum = isUstorePartitionTable ? true : dovacuum;
            vacObj->dovacuum_toast = false;
            vacObj->doanalyze = doanalyze;
            vacObj->need_freeze = isUstorePartitionTable ? false : need_freeze;
            vacObj->is_internal_relation = isUstorePartitionTable ? false : is_internal_relation;
            vacObj->gpi_vacuumed = false;
            vacObj->flags = (isPartitionedRelation(classForm) ? VACFLG_MAIN_PARTITION : VACFLG_SIMPLE_HEAP);
            table_oids = lappend(table_oids, vacObj);
        }

        /*
         * record partitioned table's autovac stat
         * 1. if we have to vaccum partitioned table since need freeze old tuple,
         *    we just skip vacuum its partition
         * 2. to avoid recompute allowvacuum falg
         */
        if (isPartitionedRelation(classForm)) {
            bool found = false;
            at_partitioned_table* ap_entry = NULL;

            ap_entry = (at_partitioned_table*)hash_search(partitioned_tables_map, &relid, HASH_ENTER, &found);
            if (!found) {
                ap_entry->at_allowvacuum = enable_vacuum;
                ap_entry->at_doanalyze = doanalyze;
                ap_entry->at_dovacuum = dovacuum;
                ap_entry->at_needfreeze = need_freeze;
                ap_entry->at_gpivacuumed = false;
            }
        }

        /*
         * Remember the association for the third pass.  Note: we must do
         * this even if the table is going to be vacuumed, because we
         * don't automatically vacuum toast tables along the parent table.
         *
         * AutoVacOpts are recorded in partitioned table's reloptions, so we read
         * and save AutoVacOpts so we can use them when we deal with parttition
         */
        if (OidIsValid(classForm->reltoastrelid) || isPartitionedRelation(classForm)) {
            av_relation* ar_entry = NULL;
            av_toastid_mainid* at_entry = NULL;
            bool found = false;

            /* Skip partitioned table's toasttable, since partitioned table
             * is a logic table and has no data in physical files corresponding
             * to its relfilenode
             */
            if (!isPartitionedRelation(classForm)) {
                at_entry =
                    (av_toastid_mainid*)hash_search(toast_table_map, &(classForm->reltoastrelid), HASH_ENTER, &found);
                if (!found) {
                    /* hash_search already filled in the key */
                    at_entry->at_relid = relid;
                    at_entry->at_parentid = InvalidOid;
                    at_entry->at_allowvacuum = enable_vacuum;
                    at_entry->at_doanalyze = doanalyze;
                    at_entry->at_dovacuum = dovacuum;
                    at_entry->at_needfreeze = need_freeze;
                    at_entry->at_internal = is_internal_relation;
                }

                Assert(OidIsValid(at_entry->at_relid));
            }

            /*
             * Because of the design of partition toast table,
             * we have to use to hash table to get reloptions for toast.
             * One is below, save relations of oid and reloptions;
             * The other is toastid-relid map, we get it from pg_class and pg_partition.
             * !!!Unlike PG, we pass relid instand of toastid to the hash table.
             */
            ar_entry = (av_relation*)hash_search(table_relopt_map, &relid, HASH_ENTER, &found);
            if (!found) {
                /* hash_search already filled in the key */
                ar_entry->ar_hasrelopts = false;
                if (relopts != NULL) {
                    ar_entry->ar_hasrelopts = true;
                    rc = memcpy_s(&ar_entry->ar_reloptions, sizeof(AutoVacOpts), relopts, sizeof(AutoVacOpts));
                    securec_check(rc, "", "");
                }
            }
        }

        if (relopts != NULL) {
            pfree_ext(relopts);
        }
    }
    tableam_scan_end(relScan);
    DEBUG_MOD_STOP_TIMER(MOD_AUTOVAC, "AUTOVAC TIMER: Scan pg_class to determine which tables to vacuum");

    /*
     * On the second pass, to collect all the partitions in the pg_partition,
     * and also the partitioned table relid to TOAST relid mapping.
     */
    ScanKeyInit(&key[0], Anum_pg_partition_parttype, BTEqualStrategyNumber, F_CHAREQ,
        CharGetDatum(PART_OBJ_TYPE_TABLE_PARTITION));

    partRel = heap_open(PartitionRelationId, AccessShareLock);
    partScan = tableam_scan_begin(partRel, SnapshotNow, 1, key);
    while (NULL != (partTuple = (HeapTuple) tableam_scan_getnexttuple(partScan, ForwardScanDirection))) {
        Form_pg_partition partForm = (Form_pg_partition)GETSTRUCT(partTuple);
        /* If relfilenode is invalid, means it's a partition of subpartition. We don't do vacuum on it, instead, we will
         * vacuum the subpartition later. */
        if (!OidIsValid(partForm->relfilenode)) {
            continue;
        }

        PgStat_StatTabEntry* tabentry = NULL;
        AutoVacOpts* relopts = NULL;
        bool dovacuum = false;
        bool doanalyze = false;
        bool need_freeze = false;
        Oid partOid;
        bool found = false;
        av_relation* ar_hentry = NULL;
        at_partitioned_table* ap_entry = NULL;

        /*
         * 'found = false' means partitioned table do autovac on other coordiantor
         * coordiantor that analyze table partition is consistent with the coordiantor
         * that analyze partition table.
         */
        ar_hentry = (av_relation*)hash_search(table_relopt_map, &(partForm->parentid), HASH_FIND, &found);
        if (!found)
            continue;

        if (ar_hentry->ar_hasrelopts)
            relopts = &ar_hentry->ar_reloptions;

        ap_entry = (at_partitioned_table*)hash_search(partitioned_tables_map, &partForm->parentid, HASH_FIND, &found);
        if (!found) {
            ereport(defence_errlevel(), (errcode(ERRCODE_DATA_CORRUPTED), errmsg("Oid: %u does not "
                "find in partitioned tables map.", partForm->parentid)));
        }

        /* Every partition table is local */
        partOid = HeapTupleGetOid(partTuple);
        tabentry = get_pgstat_tabentry_relid(partOid, false, partForm->parentid, shared, dbentry);

        /* Check if it needs vacuum or analyze */
        partition_needs_vacanalyze(
            partOid, relopts, partForm, partTuple, ap_entry, tabentry, false, &dovacuum, &doanalyze, &need_freeze);
        Assert(false == doanalyze);
        if (freeze_autovacuum) {
            dovacuum = need_freeze;
        }

        /* Partition that need work are added to table_oids */
        if (dovacuum) {
            vacObj = (vacuum_object*)palloc(sizeof(vacuum_object));
            vacObj->tab_oid = partOid;
            vacObj->parent_oid = partForm->parentid;
            vacObj->dovacuum = dovacuum;
            vacObj->dovacuum_toast = false;
            vacObj->doanalyze = doanalyze;
            vacObj->need_freeze = need_freeze;
            vacObj->is_internal_relation = false;
            vacObj->gpi_vacuumed = false;
            vacObj->flags = VACFLG_SUB_PARTITION;
            table_oids = lappend(table_oids, vacObj);
        }

        /* just save partitioned tableis oid as mainid for partition */
        if (OidIsValid(partForm->reltoastrelid)) {
            av_toastid_mainid* at_entry = NULL;

            at_entry = (av_toastid_mainid*)hash_search(toast_table_map, &(partForm->reltoastrelid), HASH_ENTER, &found);
            if (!found) {
                at_entry->at_relid = partOid;
                at_entry->at_parentid = partForm->parentid;
                at_entry->at_allowvacuum = ap_entry->at_allowvacuum;
                at_entry->at_doanalyze = doanalyze | ap_entry->at_doanalyze;
                at_entry->at_dovacuum = dovacuum | ap_entry->at_dovacuum;
                at_entry->at_needfreeze = need_freeze;
                at_entry->at_internal = false;
            }
            /* 
             * if we found map but parentid doesn't equal to partFrom->parentid
             * may be the reltoastrelid has been exchanged by some one,
             * (e.g. alter table exchange partition)
             * just skip it this time.
             */
            if (found && (at_entry->at_parentid != partForm->parentid)) {
                if (hash_search(toast_table_map, &(partForm->reltoastrelid), HASH_REMOVE, NULL) != NULL) {
                    ereport(LOG, (errmsg("reltoastrelid: %u toast table map "
                        "has been changed, skip it.", partForm->reltoastrelid)));
                } else {
                    ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("toast table map hash table corrupted.")));
                }
            } else {
                Assert(OidIsValid(at_entry->at_relid) && OidIsValid(at_entry->at_parentid));
            }
        }
    }
    /* Close the pg_partition */
    tableam_scan_end(partScan);
    heap_close(partRel, AccessShareLock);
    DEBUG_MOD_STOP_TIMER(MOD_AUTOVAC, "AUTOVAC TIMER: Scan pg_partition to determine which partitions to vacuum");

    /*
     * Meanwhile, to collect all the subpartitions in the pg_partition,
     * and also the partitioned table relid to TOAST relid mapping.
     */
    ScanKeyInit(&key[0], Anum_pg_partition_parttype, BTEqualStrategyNumber, F_CHAREQ,
        CharGetDatum(PART_OBJ_TYPE_TABLE_SUB_PARTITION));

    partRel = heap_open(PartitionRelationId, AccessShareLock);
    subpartScan = tableam_scan_begin(partRel, SnapshotNow, 1, key);
    while (NULL != (partTuple = (HeapTuple) tableam_scan_getnexttuple(subpartScan, ForwardScanDirection))) {
        Form_pg_partition partForm = (Form_pg_partition)GETSTRUCT(partTuple);
        PgStat_StatTabEntry* tabentry = NULL;
        AutoVacOpts* relopts = NULL;
        bool dovacuum = false;
        bool doanalyze = false;
        bool need_freeze = false;
        Oid partOid;
        Oid tableOid;
        bool found = false;
        av_relation* ar_hentry = NULL;
        at_partitioned_table* ap_entry = NULL;

        /* we get the subpartitioned table's oid first */
        tableOid = partid_get_parentid(partForm->parentid);
        if (!OidIsValid(tableOid)) {
            continue;
        }
        /*
         * 'found = false' means partitioned table do autovac on other coordiantor
         * coordiantor that analyze table partition is consistent with the coordiantor
         * that analyze partition table.
         */
        ar_hentry = (av_relation*)hash_search(table_relopt_map, &tableOid, HASH_FIND, &found);
        if (!found)
            continue;

        if (ar_hentry->ar_hasrelopts)
            relopts = &ar_hentry->ar_reloptions;

        ap_entry = (at_partitioned_table*)hash_search(partitioned_tables_map, &tableOid, HASH_FIND, &found);
        if (!found) {
            ereport(defence_errlevel(), (errcode(ERRCODE_DATA_CORRUPTED), errmsg("Oid: %u does not "
                "find in partitioned tables map.", tableOid)));
        }

        /* Every partition table is local */
        partOid = HeapTupleGetOid(partTuple);
        tabentry = get_pgstat_tabentry_relid(partOid, false, tableOid, shared, dbentry);

        /* Check if it needs vacuum or analyze */
        partition_needs_vacanalyze(
            partOid, relopts, partForm, partTuple, ap_entry, tabentry, false, &dovacuum, &doanalyze, &need_freeze);
        Assert(false == doanalyze);
        if (freeze_autovacuum) {
            dovacuum = need_freeze;
        }

        /* Partition that need work are added to table_oids */
        if (dovacuum) {
            vacObj = (vacuum_object*)palloc(sizeof(vacuum_object));
            vacObj->tab_oid = partOid;
            vacObj->parent_oid = partForm->parentid;
            vacObj->dovacuum = dovacuum;
            vacObj->dovacuum_toast = false;
            vacObj->doanalyze = doanalyze;
            vacObj->need_freeze = need_freeze;
            vacObj->is_internal_relation = false;
            vacObj->gpi_vacuumed = false;
            vacObj->flags = VACFLG_SUB_PARTITION;
            table_oids = lappend(table_oids, vacObj);
        }

        /* just save partitioned tableis oid as mainid for partition */
        if (OidIsValid(partForm->reltoastrelid)) {
            av_toastid_mainid* at_entry = NULL;

            at_entry = (av_toastid_mainid*)hash_search(toast_table_map, &(partForm->reltoastrelid), HASH_ENTER, &found);
            if (!found) {
                at_entry->at_relid = partOid;
                at_entry->at_parentid = partForm->parentid;
                at_entry->at_allowvacuum = ap_entry->at_allowvacuum;
                at_entry->at_doanalyze = doanalyze | ap_entry->at_doanalyze;
                at_entry->at_dovacuum = dovacuum | ap_entry->at_dovacuum;
                at_entry->at_needfreeze = need_freeze;
                at_entry->at_internal = false;
            }
            /* 
             * if we found map but parentid doesn't equal to partFrom->parentid
             * may be the reltoastrelid has been exchanged by some one,
             * (e.g. alter table exchange partition)
             * just skip it this time.
             */
            if (found && (at_entry->at_parentid != partForm->parentid)) {
                if (hash_search(toast_table_map, &(partForm->reltoastrelid), HASH_REMOVE, NULL) != NULL) {
                    ereport(LOG, (errmsg("reltoastrelid: %u toast table map "
                        "has been changed, skip it.", partForm->reltoastrelid)));
                } else {
                    ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("toast table map hash table corrupted.")));
                }
            } else {
                Assert(OidIsValid(at_entry->at_relid) && OidIsValid(at_entry->at_parentid));
            }
        }
    }
    /* Close the pg_partition */
    tableam_scan_end(subpartScan);
    heap_close(partRel, AccessShareLock);
    DEBUG_MOD_STOP_TIMER(MOD_AUTOVAC, "AUTOVAC TIMER: Scan pg_partition to determine which subpartitions to vacuum");

    /* On the third pass: check TOAST tables */
    ScanKeyInit(&key[0], Anum_pg_class_relkind, BTEqualStrategyNumber, F_CHAREQ, CharGetDatum(RELKIND_TOASTVALUE));
    relScan = tableam_scan_begin(classRel, SnapshotNow, 1, &key[0]);
    while ((tuple = (HeapTuple) tableam_scan_getnexttuple(relScan, ForwardScanDirection)) != NULL) {
        Form_pg_class classForm = (Form_pg_class)GETSTRUCT(tuple);
        Oid relid = HeapTupleGetOid(tuple);
        PgStat_StatTabEntry* tabentry = NULL;
        AutoVacOpts* relopts = NULL;
        bytea *rawRelopts = NULL;
        bool isReloptsReferenceOther = false;
        bool dovacuum = false;
        bool doanalyze = false;
        bool need_freeze = false;
        bool enable_analyze = false;
        bool enable_vacuum = false;
        bool found = false;
        av_toastid_mainid* at_entry = NULL;

        /* We cannot safely process other backends' temp tables, so skip 'em. */
        if (classForm->relpersistence == RELPERSISTENCE_TEMP ||
            classForm->relpersistence == RELPERSISTENCE_GLOBAL_TEMP)
            continue;

        at_entry = (av_toastid_mainid*)hash_search(toast_table_map, &(relid), HASH_FIND, &found);

        /*
         * For Global Autovaccum
         * 1. skip check if fail to find main table/partition
         * 2. skip check if main table/partition have been taken be vaccumed
         */
        if (!local_autovacuum && (!found || at_entry->at_dovacuum))
            continue;

        /*
         * fetch reloptions -- if this toast table does not have them, try the
         * main rel
         */
        relopts = extract_autovac_opts(tuple, pg_class_desc);
        rawRelopts = extractRelOptions(tuple, pg_class_desc, InvalidOid);
        /*
         * we must get main table id first, and then get the
         * reloptions according to the main table id
         */
        if (NULL == relopts && NULL != at_entry) {
            av_relation* ar_hentry = NULL;
            Oid MainId = at_entry->at_parentid > InvalidOid ? at_entry->at_parentid : at_entry->at_relid;

            ar_hentry = (av_relation*)hash_search(table_relopt_map, &MainId, HASH_FIND, &found);
            if (found && ar_hentry->ar_hasrelopts) {
                relopts = &ar_hentry->ar_reloptions;
                isReloptsReferenceOther = true;
            }
        }

        /* Fetch the pgstat entry for this table */
        tabentry = get_pgstat_tabentry_relid(relid, classForm->relisshared, InvalidOid, shared, dbentry);
        relation_support_autoavac(tuple, &enable_analyze, &enable_vacuum, &is_internal_relation);
        relation_needs_vacanalyze(relid, relopts, rawRelopts, classForm, tuple, tabentry, enable_analyze, enable_vacuum,
            true, &dovacuum, &doanalyze, &need_freeze);

        if (freeze_autovacuum) {
            if (ISMATMAP(classForm->relname.data) || ISMLOG(classForm->relname.data)) {
                dovacuum = true;
                doanalyze = false;
            } else {
                dovacuum = need_freeze;
                doanalyze = false;
            }
        }

        /* vacuum main table/partition instead if toast table */
        if (dovacuum) {
            vacObj = (vacuum_object*)palloc(sizeof(vacuum_object));
            if (local_autovacuum) {
                vacObj->tab_oid = relid;
                vacObj->parent_oid = InvalidOid;
                vacObj->flags = VACFLG_SIMPLE_HEAP;
                vacObj->dovacuum_toast = false;
            } else {
                vacObj->tab_oid = at_entry->at_relid;
                vacObj->parent_oid = at_entry->at_parentid;
                vacObj->flags = OidIsValid(at_entry->at_parentid) ? VACFLG_SUB_PARTITION : VACFLG_SIMPLE_HEAP;
                vacObj->dovacuum_toast = true;
                vacObj->is_internal_relation = at_entry->at_internal;
            }
            vacObj->dovacuum = dovacuum;
            vacObj->doanalyze = doanalyze;
            vacObj->need_freeze = need_freeze;

            table_oids = lappend(table_oids, vacObj);
        }
        if (relopts && !isReloptsReferenceOther) {
            pfree_ext(relopts);
        }
    }
    tableam_scan_end(relScan);
    heap_close(classRel, AccessShareLock);
    DEBUG_MOD_STOP_TIMER(MOD_AUTOVAC, "AUTOVAC TIMER: Scan pg_class to determine which toast tables to vacuum");

    /*
     * Create one buffer access strategy object per buffer pool for VACUUM to use.
     * We want to use the same one across all the vacuum operations we perform,
     * since the point is for VACUUM not to blow out the shared cache.
     */
    bstrategy = GetAccessStrategy(BAS_VACUUM);

    /*
     * create a memory context to act as fake t_thrd.mem_cxt.portal_mem_cxt, so that the
     * contexts created in the vacuum code are cleaned up for each table.
     */
    t_thrd.mem_cxt.portal_mem_cxt = AllocSetContextCreate(t_thrd.autovacuum_cxt.AutovacMemCxt, "Autovacuum Portal",
        ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_MAXSIZE);

    /*
     * Perform operations on collected tables.
     */
    foreach (cell, table_oids) {
        Oid relid;
        Oid parentid;
        autovac_table* tab = NULL;
        WorkerInfo worker = NULL;
        bool skipit = false;
        bool found = false;
        int stdVacuumCostDelay;
        int stdVacuumCostLimit;
        at_partitioned_table* ap_entry = NULL;

        vacObj = (vacuum_object*)lfirst(cell);
        relid = vacObj->tab_oid;
        parentid = vacObj->parent_oid;

        /* just skip all autovac actions quickly */
        if (!u_sess->attr.attr_storage.autovacuum_start_daemon && !vacObj->need_freeze)
            break;

        CHECK_FOR_INTERRUPTS();

        /*
         * hold schedule lock from here until we're sure that this table still
         * needs vacuuming.  We also need the AutovacuumLock to walk the
         * worker array, but we'll let go of that one quickly.
         */
        LWLockAcquire(AutovacuumScheduleLock, LW_EXCLUSIVE);
        LWLockAcquire(AutovacuumLock, LW_SHARED);

        /*
         * Check whether the table is being vacuumed concurrently by another
         * worker.
         */
        skipit = false;
        worker = (WorkerInfo)SHMQueueNext(&t_thrd.autovacuum_cxt.AutoVacuumShmem->av_runningWorkers,
            &t_thrd.autovacuum_cxt.AutoVacuumShmem->av_runningWorkers, offsetof(WorkerInfoData, wi_links));
        while (worker != NULL) {
            /* ignore myself */
            if (worker == t_thrd.autovacuum_cxt.MyWorkerInfo)
                goto next_worker;

            /* ignore workers in other databases (unless table is shared) */
            if (!worker->wi_sharedrel && worker->wi_dboid != u_sess->proc_cxt.MyDatabaseId)
                goto next_worker;

            /*
             * we can not identify it only by oid.
             * check the main table:
             * 1. other worker handle the main table, need check the worker's tableoid not equal the relid;
             * 2. other worker handle the part table, need check the worker's parentoid not equal the relid;
             * check the part table:
             * 1. other worker handle the main table, need check the worker's tableoid not equal the parentid;
             * 2. other worker handle the part table, need check the worker's parentoid not equal the parentid;
             */
            if (parentid == InvalidOid && (worker->wi_tableoid == relid || worker->wi_parentoid == relid)) {
                AUTOVAC_LOG(DEBUG1, "parentoid = %u, tableoid = %u is on autovac, just skip it", parentid, relid);
                skipit = true;
                break;
            }
            if (parentid != InvalidOid && (worker->wi_tableoid == parentid || worker->wi_parentoid == parentid)) {
                AUTOVAC_LOG(DEBUG1, "parentoid = %u, tableoid = %u is on autovac, just skip it", parentid, relid);
                skipit = true;
                break;
            }

        next_worker:
            worker = (WorkerInfo)SHMQueueNext(&t_thrd.autovacuum_cxt.AutoVacuumShmem->av_runningWorkers,
                &worker->wi_links, offsetof(WorkerInfoData, wi_links));
        }
        LWLockRelease(AutovacuumLock);
        if (skipit) {
            LWLockRelease(AutovacuumScheduleLock);
            continue;
        }

        /*
         * Check whether pgstat data still says we need to vacuum this table.
         * It could have changed if something else processed the table while
         * we weren't looking.
         *
         * Note: we have a special case in pgstat code to ensure that the
         * stats we read are as up-to-date as possible, to avoid the problem
         * that somebody just finished vacuuming this table.  The window to
         * the race condition is not closed but it is very small.
         */
        (void)MemoryContextSwitchTo(t_thrd.autovacuum_cxt.AutovacMemCxt);

        if ((vacObj->flags & VACFLG_SIMPLE_HEAP) || (vacObj->flags & VACFLG_MAIN_PARTITION)) {
            tab = table_recheck_autovac(vacObj, table_relopt_map, toast_table_map, pg_class_desc);
        } else {
            Assert(vacObj->flags & VACFLG_SUB_PARTITION);
            tab = partition_recheck_autovac(vacObj, table_relopt_map, partitioned_tables_map, pg_class_desc);
        }

        if (tab == NULL) {
            /* someone else vacuumed the table, or it went away */
            LWLockRelease(AutovacuumScheduleLock);
            continue;
        }

        tab->at_flags = vacObj->flags;

        /*
         * Ok, good to go.	Store the table in shared memory before releasing
         * the lock so that other workers don't vacuum it concurrently.
         */
        t_thrd.autovacuum_cxt.MyWorkerInfo->wi_tableoid = relid;
        t_thrd.autovacuum_cxt.MyWorkerInfo->wi_parentoid = parentid;
        t_thrd.autovacuum_cxt.MyWorkerInfo->wi_sharedrel = tab->at_sharedrel;
        t_thrd.autovacuum_cxt.MyWorkerInfo->wi_ispartition = vacuumPartition((uint32)(vacObj->flags));
        LWLockRelease(AutovacuumScheduleLock);

        /*
         * Remember the prevailing values of the vacuum cost GUCs.	We have to
         * restore these at the bottom of the loop, else we'll compute wrong
         * values in the next iteration of autovac_balance_cost().
         */
        stdVacuumCostDelay = u_sess->attr.attr_storage.VacuumCostDelay;
        stdVacuumCostLimit = u_sess->attr.attr_storage.VacuumCostLimit;

        /* Must hold AutovacuumLock while mucking with cost balance info */
        LWLockAcquire(AutovacuumLock, LW_EXCLUSIVE);

        /* advertise my cost delay parameters for the balancing algorithm */
        t_thrd.autovacuum_cxt.MyWorkerInfo->wi_cost_delay = tab->at_vacuum_cost_delay;
        t_thrd.autovacuum_cxt.MyWorkerInfo->wi_cost_limit = tab->at_vacuum_cost_limit;
        t_thrd.autovacuum_cxt.MyWorkerInfo->wi_cost_limit_base = tab->at_vacuum_cost_limit;

        /* do a balance */
        autovac_balance_cost();

        /* set the active cost parameters from the result of that */
        AutoVacuumUpdateDelay();

        /* done */
        LWLockRelease(AutovacuumLock);

        /* clean up memory before each iteration */
        MemoryContextResetAndDeleteChildren(t_thrd.mem_cxt.portal_mem_cxt);
        (void)MemoryContextSwitchTo(t_thrd.mem_cxt.portal_mem_cxt);

        /*
         * Save the relation name for a possible error message, to avoid a
         * catalog lookup in case of an error.	If any of these return NULL,
         * then the relation has been dropped since last we checked; skip it.
         * Note: they must live in a long-lived memory context because we call
         * vacuum and analyze in different transactions.
         */
        if (vacuumPartition((uint32)(vacObj->flags))) {
            Oid at_parentid = partid_get_parentid(tab->at_relid);
            Oid at_grandparentid = partid_get_parentid(at_parentid);
            if (OidIsValid(at_grandparentid)) {
                tab->at_subpartname = getPartitionName(tab->at_relid, false);
                tab->at_partname = NULL;
                tab->at_relname = get_rel_name(at_grandparentid);
                tab->at_nspname = get_namespace_name(get_rel_namespace(at_grandparentid));
            } else {
                tab->at_subpartname = NULL;
                tab->at_partname = getPartitionName(tab->at_relid, false);
                tab->at_relname = get_rel_name(at_parentid);
                tab->at_nspname = get_namespace_name(get_rel_namespace(at_parentid));
            }
        } else {
            tab->at_subpartname = NULL;
            tab->at_partname = NULL;
            tab->at_relname = get_rel_name(tab->at_relid);
            tab->at_nspname = get_namespace_name(get_rel_namespace(tab->at_relid));
        }

        tab->at_datname = get_database_name(u_sess->proc_cxt.MyDatabaseId);
        if ((NULL == tab->at_relname) || (NULL == tab->at_nspname) || (NULL == tab->at_datname))
            goto deleted;

        /*
         * We will abort vacuuming the current table if something errors out,
         * and continue with the next one in schedule; in particular, this
         * happens if we are interrupted with SIGINT.
         */
        PG_TRY();
        {
            /*
             * 1. Let pgstat know what we're doing
             * 2. in this function, statement_timestamp will be set to current time
             */
            autovac_report_activity(tab);

            if (ActiveSnapshotSet())
                PopActiveSnapshot();
            CommitTransactionCommand();

            StartTransactionCommand();
            /* vacuum must hold RowExclusiveLock on db for a new transaction */
            LockSharedObject(DatabaseRelationId, u_sess->proc_cxt.MyDatabaseId, 0, RowExclusiveLock);
            
            PushActiveSnapshot(GetTransactionSnapshot());

            (void)MemoryContextSwitchTo(t_thrd.mem_cxt.portal_mem_cxt);

            if ((IS_PGXC_COORDINATOR || IS_SINGLE_NODE) && u_sess->attr.attr_storage.autoanalyze_timeout > 0 &&
                tab->at_doanalyze && !tab->at_dovacuum) {
                t_thrd.storage_cxt.timeIsPausing = false;
                enable_sig_alarm(u_sess->attr.attr_storage.autoanalyze_timeout * 1000, true);
            }

            if (local_autovacuum || vacObj->is_internal_relation)
                autovacuum_local_vac_analyze(tab, bstrategy);
            else
                autovacuum_do_vac_analyze(tab, bstrategy);

            if (vacObj->flags & VACFLG_SUB_PARTITION) {
                // Get partitioned/subpartitioned table's oid
                Oid table_oid = parentid;
                Oid grandparentid = partid_get_parentid(parentid);
                if (OidIsValid(grandparentid)) {
                    table_oid = grandparentid;
                }
                // Update ap_entry->at_gpivacuumed
                ap_entry = (at_partitioned_table*)hash_search(partitioned_tables_map, &table_oid, HASH_FIND, &found);
                if (found && !ap_entry->at_gpivacuumed && tab->at_gpivacuumed) {
                    ap_entry->at_gpivacuumed = tab->at_gpivacuumed;
                }
            }

            /* Cancel any active statement timeout before committing */
            disable_sig_alarm(true);

            /*
             * Clear a possible query-cancel signal, to avoid a late reaction
             * to an automatically-sent signal because of vacuuming the
             * current table (we're done with it, so it would make no sense to
             * cancel at this point.)
             */
            t_thrd.int_cxt.QueryCancelPending = false;
        }
        PG_CATCH();
        {
            bool timeout_flag =
                (t_thrd.storage_cxt.cancel_from_timeout && u_sess->attr.attr_storage.autoanalyze_timeout);

            /*
             * Abort the transaction, start a new one, and proceed with the
             * next table in our list.
             */
            HOLD_INTERRUPTS();

            t_thrd.int_cxt.QueryCancelPending = false;
            (void)disable_sig_alarm(true);
            t_thrd.int_cxt.QueryCancelPending = false; /* again in case timeout occurred */

            /* Erase time counter pause info */
            t_thrd.storage_cxt.timeIsPausing = false;
            t_thrd.storage_cxt.restimems = -1;

            if (tab->at_dovacuum)
                errcontext("automatic vacuum of table \"%s.%s.%s\"", tab->at_datname, tab->at_nspname, tab->at_relname);
            else
                errcontext(
                    "automatic analyze of table \"%s.%s.%s\"", tab->at_datname, tab->at_nspname, tab->at_relname);

            EmitErrorReport();

            /* this resets the PGXACT flags too */
            AbortCurrentTransaction();
            FlushErrorState();
            MemoryContextResetAndDeleteChildren(t_thrd.mem_cxt.msg_mem_cxt);
            MemoryContextResetAndDeleteChildren(t_thrd.mem_cxt.portal_mem_cxt);

            /* for some cases, we could not response any signal here, so we need unblock signals */
            gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
            (void)gs_signal_unblock_sigusr2();

            /* restart our transaction for the following operations */
            StartTransactionCommand();
            if (timeout_flag) {
                Oid grandparent_oid = partid_get_parentid(vacObj->parent_oid);
                Oid statFlag = OidIsValid(grandparent_oid) ? grandparent_oid : vacObj->parent_oid;
                pgstat_report_autovac_timeout(vacObj->tab_oid, statFlag, tab->at_sharedrel);
            }

            RESUME_INTERRUPTS();
            /* vacuum must hold RowExclusiveLock on db for a new transaction */
            LockSharedObject(DatabaseRelationId, u_sess->proc_cxt.MyDatabaseId, 0, RowExclusiveLock);
        }
        PG_END_TRY();

        /* the PGXACT flags are reset at the next end of transaction */

    deleted:
        /*
         * Remove my info from shared memory.  We could, but intentionally
         * don't, clear wi_cost_limit and friends --- this is on the
         * assumption that we probably have more to do with similar cost
         * settings, so we don't want to give up our share of I/O for a very
         * short interval and thereby thrash the global balance.
         */
        LWLockAcquire(AutovacuumLock, LW_EXCLUSIVE);
        t_thrd.autovacuum_cxt.MyWorkerInfo->wi_tableoid = InvalidOid;
        t_thrd.autovacuum_cxt.MyWorkerInfo->wi_parentoid = InvalidOid;
        t_thrd.autovacuum_cxt.MyWorkerInfo->wi_sharedrel = false;
        t_thrd.autovacuum_cxt.MyWorkerInfo->wi_ispartition = false;
        LWLockRelease(AutovacuumLock);

        /* restore vacuum cost GUCs for the next iteration */
        u_sess->attr.attr_storage.VacuumCostDelay = stdVacuumCostDelay;
        u_sess->attr.attr_storage.VacuumCostLimit = stdVacuumCostLimit;

        MemoryContextResetAndDeleteChildren(t_thrd.mem_cxt.msg_mem_cxt);
        MemoryContextResetAndDeleteChildren(t_thrd.mem_cxt.portal_mem_cxt);
        /* reset t_thrd.vacuum_cxt.vac_context in case that invalid t_thrd.vacuum_cxt.vac_context would be used */
        t_thrd.vacuum_cxt.vac_context = NULL;
    }
    /*
     * We leak table_toast_map here (among other things), but since we're
     * going away soon, it's not a problem.
     */

    /*
     * Update pg_database.datfrozenxid, and truncate pg_clog if possible. We
     * only need to do this once, not after each table.
     */
    vac_update_datfrozenxid();

    /* Finally close out the last transaction. */
    if (ActiveSnapshotSet())
        PopActiveSnapshot();
    CommitTransactionCommand();
}

/*
 * extract_autovac_opts
 *
 * Given a relation's pg_class tuple, return the AutoVacOpts portion of
 * reloptions, if set; otherwise, return NULL.
 */
AutoVacOpts* extract_autovac_opts(HeapTuple tup, TupleDesc pg_class_desc)
{
    bytea* relopts = NULL;
    AutoVacOpts* av = NULL;
    int rc = 0;

    Assert(((Form_pg_class)GETSTRUCT(tup))->relkind == RELKIND_RELATION ||
           ((Form_pg_class) GETSTRUCT(tup))->relkind == RELKIND_MATVIEW ||
           ((Form_pg_class)GETSTRUCT(tup))->relkind == RELKIND_TOASTVALUE);

    relopts = extractRelOptions(tup, pg_class_desc, InvalidOid);
    if (relopts == NULL)
        return NULL;

    av = (AutoVacOpts*)palloc(sizeof(AutoVacOpts));
    rc = memcpy_s(av, sizeof(AutoVacOpts), &(((StdRdOptions*)relopts)->autovacuum), sizeof(AutoVacOpts));
    securec_check(rc, "\0", "\0");

    /* autovacuum for ustore unpartitioned table is disabled */
    if (RelationIsTableAccessMethodUStoreType(relopts)) {
        av->enabled = isPartitionedRelation((Form_pg_class)GETSTRUCT(tup));
    }

    pfree_ext(relopts);

    return av;
}

/*
 * get_pgstat_tabentry_relid
 *
 * Fetch the pgstat entry of a table, either local to a database or shared.
 */
static PgStat_StatTabEntry* get_pgstat_tabentry_relid(
    Oid relid, bool isshared, uint32 statFlag, PgStat_StatDBEntry* shared, PgStat_StatDBEntry* dbentry)
{
    PgStat_StatTabEntry* tabentry = NULL;
    PgStat_StatDBEntry* dnentry = NULL;

    if (isshared)
        dnentry = shared;
    else
        dnentry = dbentry;

    if (dnentry != NULL) {
        PgStat_StatTabKey tabkey;

        tabkey.statFlag = statFlag;
        tabkey.tableid = relid;
        tabentry = (PgStat_StatTabEntry*)hash_search(dnentry->tables, (void*)(&tabkey), HASH_FIND, NULL);
    }

    return tabentry;
}

/*
 * calculate_vacuum_cost_and_freezeages
 *
 * Calculate the vacuum cost parameters and the freeze ages.  If there
 * are options set in pg_class.reloptions, use them; in the case of a
 * toast table, try the main table too.  Otherwise use the GUC
 * defaults, autovacuum's own first and plain vacuum second.
 */
static autovac_table* calculate_vacuum_cost_and_freezeages(const AutoVacOpts* avopts, bool doanalyze, bool need_freeze)
{
    int64 freeze_min_age;
    int64 freeze_table_age;
    int vac_cost_limit;
    int vac_cost_delay;
    autovac_table* tab = NULL;

    /* -1 in autovac setting means use plain vacuum_cost_delay */
    vac_cost_delay = (avopts && avopts->vacuum_cost_delay >= 0)
                         ? avopts->vacuum_cost_delay
                         : (u_sess->attr.attr_storage.autovacuum_vac_cost_delay >= 0)
                               ? u_sess->attr.attr_storage.autovacuum_vac_cost_delay
                               : u_sess->attr.attr_storage.VacuumCostDelay;

    /* 0 or -1 in autovac setting means use plain vacuum_cost_limit */
    vac_cost_limit = (avopts && avopts->vacuum_cost_limit > 0)
                         ? avopts->vacuum_cost_limit
                         : (u_sess->attr.attr_storage.autovacuum_vac_cost_limit > 0)
                               ? u_sess->attr.attr_storage.autovacuum_vac_cost_limit
                               : u_sess->attr.attr_storage.VacuumCostLimit;

    /* these do not have autovacuum-specific settings */
    freeze_min_age =
        (avopts && avopts->freeze_min_age >= 0) ? avopts->freeze_min_age : t_thrd.autovacuum_cxt.default_freeze_min_age;

    freeze_table_age = (avopts && avopts->freeze_table_age >= 0) ? avopts->freeze_table_age
                                                                 : t_thrd.autovacuum_cxt.default_freeze_table_age;
    tab = (autovac_table*)palloc(sizeof(autovac_table));
    tab->at_doanalyze = doanalyze;
    tab->at_freeze_min_age = freeze_min_age;
    tab->at_freeze_table_age = freeze_table_age;
    tab->at_vacuum_cost_limit = vac_cost_limit;
    tab->at_vacuum_cost_delay = vac_cost_delay;
    tab->at_needfreeze = need_freeze;
    tab->at_relname = NULL;
    tab->at_nspname = NULL;
    tab->at_datname = NULL;
    return tab;
}

/*
 * table_recheck_autovac
 *
 * Recheck whether a table still needs vacuum or analyze.  Return value is a
 * valid autovac_table pointer if it does, NULL otherwise.
 *
 * Note that the returned autovac_table does not have the name fields set.
 */
static autovac_table* table_recheck_autovac(
    vacuum_object* vacObj, HTAB* table_relopt_map, HTAB* toast_table_map, TupleDesc pg_class_desc)
{
    Oid relid = vacObj->tab_oid;
    Form_pg_class classForm;
    HeapTuple classTup;
    bool dovacuum = false;
    bool dovacuum_toast = vacObj->dovacuum_toast;
    bool doanalyze = false;
    bool need_freeze = false;
    bool is_internal_relation = false;
    bool enable_analyze = false;
    bool enable_vacuum = false;
    autovac_table* tab = NULL;
    PgStat_StatDBEntry* shared = NULL;
    PgStat_StatDBEntry* dbentry = NULL;
    PgStat_StatTabEntry* tabentry = NULL;
    AutoVacOpts* avopts = NULL;
    bool isAvoptsRefereceOther = false;

    if (IS_SINGLE_NODE) {
        /* use fresh stats */
        autovac_refresh_stats();
    }

    shared = pgstat_fetch_stat_dbentry(InvalidOid);
    dbentry = pgstat_fetch_stat_dbentry(u_sess->proc_cxt.MyDatabaseId);

    /* fetch the relation's relcache entry */
    classTup = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(classTup))
        return NULL;
    classForm = (Form_pg_class)GETSTRUCT(classTup);
    bytea *rawRelopts = extractRelOptions(classTup, pg_class_desc, InvalidOid);
    bool isUstorePartitionTable = (rawRelopts != NULL && RelationIsTableAccessMethodUStoreType(rawRelopts) &&
        isPartitionedRelation(classForm));

    /*
     * Get the applicable reloptions.  If it is a TOAST table, try to get the
     * main table reloptions if the toast table itself doesn't have.
     */
    avopts = extract_autovac_opts(classTup, pg_class_desc);
    if (RELKIND_TOASTVALUE == classForm->relkind && (NULL == avopts) && (NULL != table_relopt_map) &&
        (NULL != toast_table_map)) {
        av_relation* hentry = NULL;
        av_toastid_mainid* tentry = NULL;
        bool found = false;
        Oid MainId;

        tentry = (av_toastid_mainid*)hash_search(toast_table_map, &relid, HASH_FIND, &found);
        if (found) {
            MainId = tentry->at_relid;
            hentry = (av_relation*)hash_search(table_relopt_map, &MainId, HASH_FIND, &found);
            if (found && hentry->ar_hasrelopts) {
                avopts = &hentry->ar_reloptions;
                isAvoptsRefereceOther = true;
            }
        }
    }

    /* fetch the pgstat table entry */
    tabentry = get_pgstat_tabentry_relid(relid, classForm->relisshared, InvalidOid, shared, dbentry);
    relation_support_autoavac(classTup, &enable_analyze, &enable_vacuum, &is_internal_relation);
    relation_needs_vacanalyze(relid, avopts, rawRelopts, classForm, classTup, tabentry, enable_analyze, enable_vacuum,
        true, &dovacuum, &doanalyze, &need_freeze);

    /* ignore ANALYZE for toast tables */
    if (classForm->relkind == RELKIND_TOASTVALUE)
        doanalyze = false;

    /* do vacuum with mlog and matmap anyway */
    if (ISMATMAP(classForm->relname.data) || ISMLOG(classForm->relname.data)) {
        dovacuum = true;
        doanalyze = true;
    }

    /* OK, it needs something done */
    if (doanalyze || dovacuum || dovacuum_toast || isUstorePartitionTable) {
        tab = calculate_vacuum_cost_and_freezeages(avopts, doanalyze, need_freeze);
        if (tab != NULL) {
            tab->at_relid = relid;
            tab->at_sharedrel = classForm->relisshared;
            tab->at_dovacuum = isUstorePartitionTable ? true : (dovacuum || dovacuum_toast);
            tab->at_gpivacuumed = vacObj->gpi_vacuumed;
        }
    }

    heap_freetuple(classTup);

    if (avopts && !isAvoptsRefereceOther) {
        pfree_ext(avopts);
    }

    return tab;
}
/*
 * determine_vacuum_params
 * Determine vacuum/analyze equation parameters.  We have two possible
 * sources: the passed reloptions (which could be a main table or a toast
 * table), or the autovacuum GUC variables.
 *
 */
static void determine_vacuum_params(float4& vac_scale_factor, int& vac_base_thresh, float4& anl_scale_factor,
    int& anl_base_thresh, int64& freeze_max_age, bool& av_enabled, TransactionId& xidForceLimit,
    MultiXactId& multiForceLimit, const AutoVacOpts* relopts)
{
    /* -1 in autovac setting means use plain vacuum_cost_delay */
    vac_scale_factor = (relopts && relopts->vacuum_scale_factor >= 0) ? relopts->vacuum_scale_factor
                                                                      : u_sess->attr.attr_storage.autovacuum_vac_scale;

    vac_base_thresh = (relopts && relopts->vacuum_threshold >= 0) ? relopts->vacuum_threshold
                                                                  : u_sess->attr.attr_storage.autovacuum_vac_thresh;

    anl_scale_factor = (relopts && relopts->analyze_scale_factor >= 0) ? relopts->analyze_scale_factor
                                                                       : u_sess->attr.attr_storage.autovacuum_anl_scale;

    anl_base_thresh = (relopts && relopts->analyze_threshold >= 0) ? relopts->analyze_threshold
                                                                   : u_sess->attr.attr_storage.autovacuum_anl_thresh;

    freeze_max_age = (relopts && relopts->freeze_max_age >= 0)
                         ? Min(relopts->freeze_max_age, g_instance.attr.attr_storage.autovacuum_freeze_max_age)
                         : g_instance.attr.attr_storage.autovacuum_freeze_max_age;

    av_enabled = (relopts ? relopts->enabled : true);

    /* Force vacuum if table need freeze the old tuple to recycle clog */
    if (t_thrd.autovacuum_cxt.recentXid > FirstNormalTransactionId + freeze_max_age)
        xidForceLimit = t_thrd.autovacuum_cxt.recentXid - freeze_max_age;
    else
        xidForceLimit = FirstNormalTransactionId;

#ifndef ENABLE_MULTIPLE_NODES
    if (t_thrd.autovacuum_cxt.recentMulti >
        FirstMultiXactId + (uint64)g_instance.attr.attr_storage.autovacuum_freeze_max_age)
        multiForceLimit = t_thrd.autovacuum_cxt.recentMulti -
            (uint64)g_instance.attr.attr_storage.autovacuum_freeze_max_age;
    else
        multiForceLimit = FirstMultiXactId;
#endif
}

/*
 * relation_needs_vacanalyze
 *
 * Check whether a relation needs to be vacuumed or analyzed; return each into
 * "dovacuum" and "doanalyze", respectively.  Also return whether the vacuum is
 * being forced because need freeze the old tuple for recycle clog.
 *
 * relopts is a pointer to the AutoVacOpts options (either for itself in the
 * case of a plain table, or for either itself or its parent table in the case
 * of a TOAST table), NULL if none; tabentry is the pgstats entry, which can be
 * NULL.
 *
 * A table needs to be vacuumed if the number of dead tuples exceeds a
 * threshold.  This threshold is calculated as
 *
 * threshold = vac_base_thresh + vac_scale_factor * reltuples
 *
 * For analyze, the analysis done is that the number of tuples inserted,
 * deleted and updated since the last analyze exceeds a threshold calculated
 * in the same fashion as above.  Note that the collector actually stores
 * the number of tuples (both live and dead) that there were as of the last
 * analyze.  This is asymmetric to the VACUUM case.
 *
 * We also force vacuum if the table's relfrozenxid64 is more than freeze_max_age
 * transactions back.
 *
 * A table whose autovacuum_enabled option is false is
 * automatically skipped (unless we have to vacuum it due to freeze_max_age).
 * Thus autovacuum can be disabled for specific tables. Also, when the stats
 * collector does not have data about a table, it will be skipped.
 *
 * A table whose vac_base_thresh value is < 0 takes the base value from the
 * autovacuum_vacuum_threshold GUC variable.  Similarly, a vac_scale_factor
 * value < 0 is substituted with the value of
 * autovacuum_vacuum_scale_factor GUC variable.  Ditto for analyze.
 */
static void relation_needs_vacanalyze(Oid relid, AutoVacOpts* relopts, bytea* rawRelopts, Form_pg_class classForm,
    HeapTuple tuple, PgStat_StatTabEntry* tabentry, bool allowAnalyze, bool allowVacuum, bool is_recheck,
    /* output params below */
    bool* dovacuum, bool* doanalyze, bool* need_freeze)
{
    PgStat_StatTabKey tablekey;
    avw_info* avwentry = NULL;
    bool found = false;
    bool force_vacuum = false;
    bool delta_vacuum = false;
    bool av_enabled = false;
    bool userEnabled = true;
    /* pg_class.reltuples */
    float4 reltuples;

    /* constants from reloptions or GUC variables */
    int vac_base_thresh = 0;
    int anl_base_thresh = 0;
    float4 vac_scale_factor = 0.0;
    float4 anl_scale_factor = 0.0;

    /* thresholds calculated from above constants */
    float4 vacthresh;
    float4 anlthresh;

    /* number of vacuum (resp. analyze) tuples at this time */
    int64 vactuples = 0;
    int64 anltuples = 0;

    /* freeze parameters */
    int64 freeze_max_age = 0;
    TransactionId xidForceLimit = InvalidTransactionId;
    MultiXactId	multiForceLimit = InvalidMultiXactId;

    AssertArg(classForm != NULL);
    AssertArg(OidIsValid(relid));

    determine_vacuum_params(vac_scale_factor, vac_base_thresh, anl_scale_factor, anl_base_thresh, freeze_max_age,
        av_enabled, xidForceLimit, multiForceLimit, relopts);

    bool isNull = false;
    TransactionId relfrozenxid = InvalidTransactionId;
    Relation rel = heap_open(RelationRelationId, AccessShareLock);
    Datum xid64datum = heap_getattr(tuple, Anum_pg_class_relfrozenxid64, RelationGetDescr(rel), &isNull);

    if (isNull) {
        relfrozenxid = classForm->relfrozenxid;

        if (TransactionIdPrecedes(t_thrd.xact_cxt.ShmemVariableCache->nextXid, relfrozenxid) ||
            !TransactionIdIsNormal(relfrozenxid)) {
            relfrozenxid = FirstNormalTransactionId;
        }
    } else {
        relfrozenxid = DatumGetTransactionId(xid64datum);
    }

    force_vacuum = (TransactionIdIsNormal(relfrozenxid) && TransactionIdPrecedes(relfrozenxid, xidForceLimit));
#ifndef ENABLE_MULTIPLE_NODES
    if (!force_vacuum) {
        Datum mxidDatum = heap_getattr(tuple, Anum_pg_class_relminmxid, RelationGetDescr(rel), &isNull);
        MultiXactId relminmxid = isNull ? FirstMultiXactId : DatumGetTransactionId(mxidDatum);
        force_vacuum = (MultiXactIdIsValid(relminmxid) && MultiXactIdPrecedes(relminmxid, multiForceLimit));
    }
#endif
    heap_close(rel, AccessShareLock);
    *need_freeze = force_vacuum;
    AUTOVAC_LOG(DEBUG2, "vac \"%s\": need freeze is %s", NameStr(classForm->relname), force_vacuum ? "true" : "false");

    /* Is time to move rows from delta to main cstore table by vacuum? */
    if (rawRelopts != NULL && StdRelOptIsColStore(rawRelopts) &&
        g_instance.attr.attr_storage.enable_delta_store && DO_VACUUM) {
        PgStat_StatDBEntry *dbentry = pgstat_fetch_stat_dbentry(u_sess->proc_cxt.MyDatabaseId);;
        PgStat_StatDBEntry *shared = pgstat_fetch_stat_dbentry(InvalidOid);

        /* delta table's relisshared is same to main cstore table */
        PgStat_StatTabEntry *deltaTabentry = get_pgstat_tabentry_relid(classForm->reldeltarelid,
            classForm->relisshared, InvalidOid, shared, dbentry);
        if (deltaTabentry != NULL) {
            delta_vacuum = (deltaTabentry->n_live_tuples >= ((StdRdOptions*)rawRelopts)->delta_rows_threshold);
        }
    }

    /* User disabled it in pg_class.reloptions?  (But ignore if at risk) */
    if (!force_vacuum && (!av_enabled || !u_sess->attr.attr_storage.autovacuum_start_daemon)) {
        userEnabled = false;
    }

    if (NULL != t_thrd.autovacuum_cxt.pgStatAutoVacInfo) {
        tablekey.statFlag = InvalidOid;
        tablekey.tableid = relid;
        avwentry =
            (avw_info*)hash_search(t_thrd.autovacuum_cxt.pgStatAutoVacInfo, (void*)(&tablekey), HASH_FIND, &found);
    }

    reltuples = classForm->reltuples;
    vacthresh = (float4)vac_base_thresh + vac_scale_factor * reltuples;
    anlthresh = (float4)anl_base_thresh + anl_scale_factor * reltuples;

    if ((avwentry == NULL) && (tabentry == NULL)) {
        *dovacuum = force_vacuum;
        *doanalyze = false;
    } else {
        if (tabentry && (tabentry->changes_since_analyze || tabentry->n_dead_tuples)) {
            anltuples = tabentry->changes_since_analyze;
            vactuples = tabentry->n_dead_tuples;
            AUTOVAC_LOG(DEBUG2, "fetch local stat info: vac \"%s\" changes_since_analyze = %ld  n_dead_tuples = %ld ",
                NameStr(classForm->relname), tabentry->changes_since_analyze, tabentry->n_dead_tuples);
        }

        if (avwentry && (avwentry->changes_since_analyze || avwentry->n_dead_tuples)) {
            anltuples = avwentry->changes_since_analyze;
            vactuples = avwentry->n_dead_tuples;
            AUTOVAC_LOG(DEBUG2, "fetch global stat info: vac \"%s\" changes_since_analyze = %ld  n_dead_tuples = %ld ",
                NameStr(classForm->relname), avwentry->changes_since_analyze, avwentry->n_dead_tuples);
        }

        /* Determine if this table needs vacuum. */
        *dovacuum = force_vacuum || delta_vacuum;
        *doanalyze = false;

        if (false == *dovacuum && allowVacuum)
            *dovacuum = ((float4)vactuples > vacthresh);

        /* Determine if this table needs analyze. */
        if (allowAnalyze)
            *doanalyze = ((float4)anltuples > anlthresh);
    }

    *dovacuum = *dovacuum && userEnabled;

    if (*dovacuum || *doanalyze) {
        AUTOVAC_LOG(DEBUG2, "vac \"%s\": recheck = %s need_freeze = %s dovacuum = %s (dead tuples %ld "
            "vacuum threshold %.0f) doanalyze = %s (changed tuples %ld analyze threshold %.0f)",
            NameStr(classForm->relname), is_recheck ? "true" : "false", *need_freeze ? "true" : "false",
            *dovacuum ? "true" : "false", vactuples, vacthresh, *doanalyze ? "true" : "false", anltuples, anlthresh);
    }

    DEBUG_VACUUM_LOG(relid, classForm->relnamespace, LOG, "vac \"%s\": recheck = %s need_freeze = %s dovacuum = %s "
        "(dead tuples %ld vacuum threshold %.0f) doanalyze = %s (changed tuples %ld analyze threshold %.0f) reltuples "
        "= %.0f", NameStr(classForm->relname), is_recheck ? "true" : "false", *need_freeze ? "true" : "false",
        *dovacuum ? "true" : "false", vactuples, vacthresh, *doanalyze ? "true" : "false",
        anltuples, anlthresh, reltuples);
}

/*
 * fill_in_vac_stmt
 *
 * fill in the vacuum statement.
 */
static void fill_in_vac_stmt(VacuumStmt& vacstmt, const autovac_table& tab, RangeVar* rangevar)
{
    vacstmt.type = T_VacuumStmt;
    if (!tab.at_needfreeze)
        vacstmt.options = VACOPT_NOWAIT;
    if (tab.at_dovacuum)
        vacstmt.options = (unsigned int)vacstmt.options | VACOPT_VACUUM;
    if (tab.at_doanalyze)
        vacstmt.options = (unsigned int)vacstmt.options | VACOPT_ANALYZE;
#ifdef ENABLE_MOT
    vacstmt.options |= VACOPT_AUTOVAC;
#endif
    vacstmt.flags = tab.at_flags;
    vacstmt.rely_oid = InvalidOid; /* we just simple set it invalid, maybe change */
    vacstmt.freeze_min_age = tab.at_freeze_min_age;
    vacstmt.freeze_table_age = tab.at_freeze_table_age;
    /* we pass the OID, but might need this anyway for an error message */
    vacstmt.relation = rangevar;
    vacstmt.va_cols = NIL;
    vacstmt.gpi_vacuumed = tab.at_gpivacuumed;
}

/*
 * autovacuum_do_vac_analyze
 *		Vacuum and/or analyze the specified table
 */
static void autovacuum_do_vac_analyze(autovac_table* tab, BufferAccessStrategy bstrategy)
{
    VacuumStmt vacstmt;
    RangeVar rangevar;
    const char* nspname = NULL;
    const char* relname = NULL;
    StringInfoData str;
    errno_t rc = EOK;

    /* Set up command parameters --- use local variables instead of palloc */
    rc = memset_s(&vacstmt, sizeof(vacstmt), 0, sizeof(vacstmt));
    securec_check(rc, "", "");

    rc = memset_s(&rangevar, sizeof(rangevar), 0, sizeof(rangevar));
    securec_check(rc, "", "");

    rangevar.schemaname = tab->at_nspname;
    rangevar.relname = tab->at_relname;
    if (NULL != tab->at_partname) {
        rangevar.ispartition = true;
        rangevar.partitionname = tab->at_partname;
    } else if (NULL != tab->at_subpartname) {
        rangevar.issubpartition = true;
        rangevar.subpartitionname = tab->at_subpartname;
    }
    rangevar.location = -1;

    nspname = quote_identifier(tab->at_nspname);
    relname = quote_identifier(tab->at_relname);

    fill_in_vac_stmt(vacstmt, *tab, &rangevar);
    initStringInfo(&str);
    if (tab->at_dovacuum)
        appendStringInfo(&str, "VACUUM ");
    if (tab->at_doanalyze)
        appendStringInfo(&str, "ANALYZE ");
    appendStringInfo(&str, "%s.%s", nspname, relname);
    if (NULL != tab->at_partname) {
        appendStringInfo(&str, " PARTITION (%s)", quote_identifier(tab->at_partname));
    } else if (NULL != tab->at_subpartname) {
        appendStringInfo(&str, " SUBPARTITION (%s)", quote_identifier(tab->at_subpartname));
    }

    WaitStatePhase oldPhase = pgstat_report_waitstatus_phase(PHASE_AUTOVACUUM);
    DEBUG_MOD_START_TIMER(MOD_AUTOVAC);
    DoVacuumMppTable(&vacstmt, str.data, true, false);
    DEBUG_MOD_STOP_TIMER(MOD_AUTOVAC, "AUTOVAC TIMER: %s", str.data);
    pgstat_report_waitstatus_phase(oldPhase);
}

/*
 * autovacuum_do_vac_analyze
 *		Vacuum and/or analyze the specified table
 */
static void autovacuum_local_vac_analyze(autovac_table* tab, BufferAccessStrategy bstrategy)
{
    VacuumStmt vacstmt;
    RangeVar rangevar;
    errno_t rc = EOK;

    /* Set up command parameters --- use local variables instead of palloc */
    rc = memset_s(&vacstmt, sizeof(vacstmt), 0, sizeof(vacstmt));
    securec_check(rc, "", "");

    rc = memset_s(&rangevar, sizeof(rangevar), 0, sizeof(rangevar));
    securec_check(rc, "", "");

    rangevar.schemaname = tab->at_nspname;
    rangevar.relname = tab->at_relname;
    rangevar.partitionname = tab->at_partname;
    rangevar.subpartitionname = tab->at_subpartname;
    rangevar.location = -1;

    fill_in_vac_stmt(vacstmt, *tab, &rangevar);
    /* Let pgstat know what we're doing */
    autovac_report_activity(tab);
    WaitStatePhase oldPhase = pgstat_report_waitstatus_phase(PHASE_AUTOVACUUM);
    DEBUG_MOD_START_TIMER(MOD_AUTOVAC);
    vacuum(&vacstmt, tab->at_relid, u_sess->attr.attr_storage.handle_toast_in_autovac, bstrategy, true);
    tab->at_gpivacuumed = vacstmt.gpi_vacuumed;
    DEBUG_MOD_STOP_TIMER(MOD_AUTOVAC, "AUTOVAC TIMER: %s", tab->at_relname);
    pgstat_report_waitstatus_phase(oldPhase);
}

/*
 * autovac_report_activity
 *		Report to pgstat what autovacuum is doing
 *
 * We send a SQL string corresponding to what the user would see if the
 * equivalent command was to be issued manually.
 *
 * Note we assume that we are going to report the next command as soon as we're
 * done with the current one, and exit right after the last one, so we don't
 * bother to report "<IDLE>" or some such.
 */
static void autovac_report_activity(autovac_table* tab)
{
#define MAX_AUTOVAC_ACTIV_LEN (NAMEDATALEN * 2 + 56)
    char activity[MAX_AUTOVAC_ACTIV_LEN];
    int len;
    int rc = 0;

    /* Report the command and possible options */
    if (tab->at_dovacuum)
        rc = snprintf_s(activity, MAX_AUTOVAC_ACTIV_LEN, MAX_AUTOVAC_ACTIV_LEN - 1, "autovacuum: VACUUM%s",
            tab->at_doanalyze ? " ANALYZE" : "");
    else
        rc = snprintf_s(activity, MAX_AUTOVAC_ACTIV_LEN, MAX_AUTOVAC_ACTIV_LEN - 1, "autovacuum: ANALYZE");
    securec_check_ss(rc, "\0", "\0");

    /*
     * Report the qualified name of the relation.
     */
    len = strlen(activity);

    rc = snprintf_s(activity + len, MAX_AUTOVAC_ACTIV_LEN - len, MAX_AUTOVAC_ACTIV_LEN - len - 1,
        " %s.%s%s", tab->at_nspname, tab->at_relname, tab->at_needfreeze ? " (freeze old tuple for recycle clog)" : "");
    securec_check_ss(rc, "\0", "\0");

    /* Set statement_timestamp() to current time for pg_stat_activity */
    SetCurrentStatementStartTimestamp();

    pgstat_report_activity(STATE_RUNNING, activity);
}

/*
 * AutoVacuumingActive
 *		Check GUC vars and report whether the autovacuum process should be
 *		running.
 */
bool AutoVacuumingActive(void)
{
    if (!u_sess->attr.attr_storage.autovacuum_start_daemon || !u_sess->attr.attr_common.pgstat_track_counts ||
        SSIsServerModeReadOnly())
        return false;
    return true;
}

/*
 * autovac_init
 *		This is called at postmaster initialization.
 *
 * All we do here is annoy the user if he got it wrong.
 */
void autovac_init(void)
{
    if (u_sess->attr.attr_storage.autovacuum_start_daemon && !u_sess->attr.attr_common.pgstat_track_counts)
        ereport(WARNING, (errmsg("autovacuum not started because of misconfiguration"),
                errhint("Enable the \"track_counts\" option.")));
}

/*
 * IsAutoVacuum functions
 *		Return whether this is either a launcher autovacuum process or a worker
 *		process.
 */
bool IsAutoVacuumLauncherProcess(void)
{
    return t_thrd.role == AUTOVACUUM_LAUNCHER;
}

bool IsAutoVacuumWorkerProcess(void)
{
    return t_thrd.role == AUTOVACUUM_WORKER;
}

const char* AUTO_VACUUM_WORKER = "AutoVacWorker";

bool IsFromAutoVacWoker(void)
{
    return (u_sess->attr.attr_common.application_name != NULL &&
        strcmp(u_sess->attr.attr_common.application_name, AUTO_VACUUM_WORKER) == 0);
}

/*
 * AutoVacuumShmemSize
 *		Compute space needed for autovacuum-related shared memory
 */
Size AutoVacuumShmemSize(void)
{
    Size size;

    /*
     * Need the fixed struct and the array of WorkerInfoData.
     */
    size = sizeof(AutoVacuumShmemStruct);
    size = MAXALIGN(size);
    size = add_size(size, mul_size(g_instance.attr.attr_storage.autovacuum_max_workers, sizeof(WorkerInfoData)));
    return size;
}

/*
 * AutoVacuumShmemInit
 *		Allocate and initialize autovacuum-related shared memory
 */
void AutoVacuumShmemInit(void)
{
    bool found = false;

    t_thrd.autovacuum_cxt.AutoVacuumShmem =
        (AutoVacuumShmemStruct*)ShmemInitStruct("AutoVacuum Data", AutoVacuumShmemSize(), &found);

    if (!IsUnderPostmaster) {
        WorkerInfo worker = NULL;
        int i = 0;

        if (unlikely(found)) {
            ereport(PANIC, (errmsg("AutoVacuum Data share mem is already init")));
        }

        t_thrd.autovacuum_cxt.AutoVacuumShmem->av_launcherpid = 0;
        t_thrd.autovacuum_cxt.AutoVacuumShmem->av_freeWorkers = NULL;
        SHMQueueInit(&t_thrd.autovacuum_cxt.AutoVacuumShmem->av_runningWorkers);
        t_thrd.autovacuum_cxt.AutoVacuumShmem->av_startingWorker = NULL;

        worker = (WorkerInfo)((char*)t_thrd.autovacuum_cxt.AutoVacuumShmem + MAXALIGN(sizeof(AutoVacuumShmemStruct)));

        /* initialize the WorkerInfo free list */
        for (i = 0; i < g_instance.attr.attr_storage.autovacuum_max_workers; i++) {
            worker[i].wi_links.next = (SHM_QUEUE*)t_thrd.autovacuum_cxt.AutoVacuumShmem->av_freeWorkers;
            t_thrd.autovacuum_cxt.AutoVacuumShmem->av_freeWorkers = &worker[i];
        }
    } else {
        if (unlikely(!found)) {
            ereport(PANIC, (errmsg("AutoVacuum Data share mem is not init")));
        }
    }
}

/*
 * autovac_refresh_stats
 *		Refresh pgstats data for an autovacuum process
 *
 * Cause the next pgstats read operation to obtain fresh data, but throttle
 * such refreshing in the autovacuum launcher.	This is mostly to avoid
 * rereading the pgstats files too many times in quick succession when there
 * are many databases.
 *
 * Note: we avoid throttling in the autovac worker, as it would be
 * counterproductive in the recheck logic.
 */
static void autovac_refresh_stats(void)
{
    if (IsAutoVacuumLauncherProcess()) {
        TimestampTz current_time;

        current_time = GetCurrentTimestamp();

        if (!TimestampDifferenceExceeds(t_thrd.autovacuum_cxt.last_read, current_time, STATS_READ_DELAY))
            return;

        t_thrd.autovacuum_cxt.last_read = current_time;
    }

    pgstat_clear_snapshot();
}

static void partition_needs_vacanalyze(Oid partid, AutoVacOpts* relopts, Form_pg_partition partForm,
    HeapTuple partTuple, at_partitioned_table* ap_entry, PgStat_StatTabEntry* tabentry, bool is_recheck, bool* dovacuum,
    bool* doanalyze, bool* need_freeze)
{
    PgStat_StatTabKey tablekey;
    avw_info* avwentry = NULL;
    bool found = false;
    bool av_enabled = false;
    bool force_vacuum = false;
    bool delta_vacuum = false;
    bytea* partoptions = NULL;
    /* pg_partition.reltuples */
    float4 reltuples;

    /* constants from reloptions or GUC variables */
    float4 vac_scale_factor = 0.0;
    float4 anl_scale_factor = 0.0;
    int vac_base_thresh = 0;
    int anl_base_thresh = 0;
    /* thresholds calculated from above constants */
    float4 vacthresh;
    float4 anlthresh;

    /* number of vacuum (resp. analyze) tuples at this time */
    int64 vactuples = 0;
    int64 anltuples = 0;
    /* freeze parameters */
    int64 freeze_max_age = 0;
    TransactionId xidForceLimit = InvalidTransactionId;
    MultiXactId multiForceLimit = InvalidMultiXactId;

    char* relname = NULL;
    Oid nameSpaceOid = InvalidOid;
    char* partname = NULL;

    AssertArg(partForm != NULL && OidIsValid(partid));
    determine_vacuum_params(vac_scale_factor, vac_base_thresh, anl_scale_factor, anl_base_thresh, freeze_max_age,
        av_enabled, xidForceLimit, multiForceLimit, relopts);
    /* Force vacuum if table need freeze the old tuple to recycle clog */
    if (t_thrd.autovacuum_cxt.recentXid > FirstNormalTransactionId + freeze_max_age)
        xidForceLimit = t_thrd.autovacuum_cxt.recentXid - freeze_max_age;
    else
        xidForceLimit = FirstNormalTransactionId;

    bool isNull = false;
    TransactionId relfrozenxid = InvalidTransactionId;
    Relation rel = heap_open(PartitionRelationId, AccessShareLock);
    Datum xid64datum = heap_getattr(partTuple, Anum_pg_partition_relfrozenxid64, RelationGetDescr(rel), &isNull);

    if (isNull) {
        relfrozenxid = partForm->relfrozenxid;

        if (TransactionIdPrecedes(t_thrd.xact_cxt.ShmemVariableCache->nextXid, relfrozenxid) ||
            !TransactionIdIsNormal(relfrozenxid)) {
            relfrozenxid = FirstNormalTransactionId;
        }
    } else {
        relfrozenxid = DatumGetTransactionId(xid64datum);
    }

#ifndef ENABLE_MULTIPLE_NODES
    if (!force_vacuum) {
        Datum mxidDatum = heap_getattr(partTuple, Anum_pg_partition_relminmxid, RelationGetDescr(rel), &isNull);
        MultiXactId relminmxid = isNull ? FirstMultiXactId : DatumGetTransactionId(mxidDatum);
        force_vacuum = (MultiXactIdIsValid(relminmxid) && MultiXactIdPrecedes(relminmxid, multiForceLimit));
    }
#endif
    Datum partoptsdatum = fastgetattr(partTuple, Anum_pg_partition_reloptions, RelationGetDescr(rel), &isNull);
    heap_close(rel, AccessShareLock);

    force_vacuum = (TransactionIdIsNormal(relfrozenxid) && TransactionIdPrecedes(relfrozenxid, xidForceLimit));
    *need_freeze = force_vacuum;

    /* Is time to move rows from delta to main cstore table by vacuum? */
    partoptions = heap_reloptions(RELKIND_RELATION, partoptsdatum, false);
    if (partoptions != NULL && StdRelOptIsColStore(partoptions) &&
        g_instance.attr.attr_storage.enable_delta_store && DO_VACUUM) {
        PgStat_StatDBEntry *dbentry = pgstat_fetch_stat_dbentry(u_sess->proc_cxt.MyDatabaseId);;
        PgStat_StatDBEntry *shared = pgstat_fetch_stat_dbentry(InvalidOid);

        /* Every partition table is local */
        PgStat_StatTabEntry *deltaTabentry = get_pgstat_tabentry_relid(partForm->reldeltarelid, false,
            InvalidOid, shared, dbentry);
        if (deltaTabentry != NULL) {
            delta_vacuum = (deltaTabentry->n_live_tuples >= ((StdRdOptions*)partoptions)->delta_rows_threshold);
        }
    }

    /* User disabled it in pg_class.reloptions?  (But ignore if at risk) */
    if (!force_vacuum && (!av_enabled || !u_sess->attr.attr_storage.autovacuum_start_daemon)) {
        *doanalyze = false;
        *dovacuum = false;
        return;
    }

#ifndef ENABLE_MULTIPLE_NODES
    if (!force_vacuum && !(ap_entry->at_allowvacuum && ap_entry->at_dovacuum)) { 
#else
    if (!force_vacuum && (!ap_entry->at_allowvacuum || ap_entry->at_dovacuum)) {
#endif
        *doanalyze = false;
        *dovacuum = delta_vacuum;
        return;
    }

    Oid relid = InvalidOid;
    if (partForm->parttype == PART_OBJ_TYPE_TABLE_SUB_PARTITION) {
        relid = partid_get_parentid(partForm->parentid);
    } else {
        relid = partForm->parentid;
    }

    relname = get_rel_name(relid);
    nameSpaceOid = get_rel_namespace(relid);
    partname = NameStr(partForm->relname);

    reltuples = partForm->reltuples;
    anlthresh = (float4)anl_base_thresh + anl_scale_factor * reltuples;
    vacthresh = (float4)vac_base_thresh + vac_scale_factor * reltuples;

    if (NULL != t_thrd.autovacuum_cxt.pgStatAutoVacInfo) {
        tablekey.statFlag = relid;
        tablekey.tableid = partid;
        avwentry =
            (avw_info*)hash_search(t_thrd.autovacuum_cxt.pgStatAutoVacInfo, (void*)(&tablekey), HASH_FIND, &found);
    }

    if ((avwentry == NULL) && (tabentry == NULL)) {
        *doanalyze = false;
        *dovacuum = force_vacuum || delta_vacuum;
    } else {
        if (tabentry && (tabentry->changes_since_analyze || tabentry->n_dead_tuples)) {
            vactuples = tabentry->n_dead_tuples;
            anltuples = tabentry->changes_since_analyze;
            AUTOVAC_LOG(DEBUG2, "fetch local stat info: vac \"%s\" partition(\"%s\") "
                "changes_since_analyze = %ld  n_dead_tuples = %ld ",
                relname, partname, tabentry->changes_since_analyze, tabentry->n_dead_tuples);
        }

        if (avwentry && (avwentry->changes_since_analyze || avwentry->n_dead_tuples)) {
            anltuples = avwentry->changes_since_analyze;
            vactuples = avwentry->n_dead_tuples;

            AUTOVAC_LOG(DEBUG2, "fetch local stat info: vac \"%s\" partition(\"%s\") changes_since_analyze = %ld "
                "n_dead_tuples = %ld ", relname, partname, avwentry->changes_since_analyze, avwentry->n_dead_tuples);

            /*
             * refresh partition's vacthresh/anlthresh with n_live_tuples because we
             * do not do analyze for partition, and so reltuples in pg_partition
             * is 0, so we just n_live_tuples instead of reltuples
             */
            anlthresh = (float4)anl_base_thresh + anl_scale_factor * avwentry->n_live_tuples;
            vacthresh = (float4)vac_base_thresh + vac_scale_factor * avwentry->n_live_tuples;
        }

        /* Determine if this partition needs vacuum. */
        *dovacuum = force_vacuum || delta_vacuum;
        if (false == *dovacuum)
            *dovacuum = (vactuples > vacthresh);

        /*
         * Determine if this table needs analyze.
         * we only do auto-analyze on partitioned table, never on partition
         * we keep the code just for we will support partition analyze one day
         */
        *doanalyze = (anltuples > anlthresh) && false;
    }

    if (!is_recheck && (*dovacuum || *doanalyze)) {
        AUTOVAC_LOG(DEBUG2, "vac table \"%s\" partition(\"%s\"): recheck = %s need_freeze = %s "
            "dovacuum = %s (dead tuples %ld vacuum threshold %.0f)",
            relname, partname, is_recheck ? "true" : "false", *need_freeze ? "true" : "false",
            *dovacuum ? "true" : "false", vactuples, vacthresh);
    }

    DEBUG_VACUUM_LOG(relid, nameSpaceOid, LOG, "vac table \"%s\" partition(\"%s\"): recheck = %s "
        "need_freeze = %s dovacuum = %s (dead tuples %ld vacuum threshold %.0f) reltuple = %.0f",
        relname, partname, is_recheck ? "true" : "false", *need_freeze ? "true" : "false",
        *dovacuum ? "true" : "false", vactuples, vacthresh, reltuples);
}

static autovac_table* partition_recheck_autovac(
    vacuum_object* vacObj, HTAB* table_relopt_map, HTAB* partitioned_tables_map, TupleDesc pg_class_desc)
{
    Oid partid = vacObj->tab_oid;
    bool dovacuum = false;
    bool doanalyze = false;
    bool dovacuum_toast = vacObj->dovacuum_toast;
    bool need_freeze = false;
    autovac_table* tab = NULL;
    PgStat_StatTabEntry* tabentry = NULL;
    PgStat_StatDBEntry* shared = NULL;
    PgStat_StatDBEntry* dbentry = NULL;
    at_partitioned_table* ap_entry = NULL;
    AutoVacOpts* avopts = NULL;
    av_relation* hentry = NULL;
    bool found = false;
    Form_pg_partition partForm;
    HeapTuple partTuple;
    Oid relid;

    if (IS_SINGLE_NODE) {
        /* use fresh stats */
        autovac_refresh_stats();
    }

    /* fetch the partition's syscache entry */
    partTuple = SearchSysCacheCopy1(PARTRELID, ObjectIdGetDatum(partid));
    if (!HeapTupleIsValid(partTuple))
        return NULL;

    partForm = (Form_pg_partition)GETSTRUCT(partTuple);
    if (partForm->parttype == PART_OBJ_TYPE_TABLE_SUB_PARTITION) {
        relid = partid_get_parentid(partForm->parentid);
    } else {
        relid = partForm->parentid;
    }
    shared = pgstat_fetch_stat_dbentry(InvalidOid);
    dbentry = pgstat_fetch_stat_dbentry(u_sess->proc_cxt.MyDatabaseId);

    /* fetch the relation's syscache entry */
    hentry = (av_relation*)hash_search(table_relopt_map, &relid, HASH_FIND, &found);
    if (!(found && hentry->ar_hasrelopts)) {
        ereport(defence_errlevel(), (errcode(ERRCODE_DATA_CORRUPTED), errmsg("Oid: %u does not "
            "find in rel options map.", relid)));
    }
    avopts = &(hentry->ar_reloptions);

    /* fetch the pgstat table entry */
    ap_entry = (at_partitioned_table*)hash_search(partitioned_tables_map, &relid, HASH_FIND, &found);
    if (!found) {
        ereport(defence_errlevel(), (errcode(ERRCODE_DATA_CORRUPTED), errmsg("Oid: %u does not "
            "find in partitioned tables map.", relid)));
    }
    tabentry = get_pgstat_tabentry_relid(partid, false, relid, shared, dbentry);
    partition_needs_vacanalyze(
        partid, avopts, partForm, partTuple, ap_entry, tabentry, true, &dovacuum, &doanalyze, &need_freeze);
    Assert(false == doanalyze);
    /* OK, it needs something done */
    if (dovacuum || dovacuum_toast) {
        tab = calculate_vacuum_cost_and_freezeages(avopts, doanalyze, need_freeze);
        if (tab != NULL) {
            tab->at_relid = partid;
            tab->at_sharedrel = false;
            tab->at_dovacuum = dovacuum || dovacuum_toast;
            tab->at_gpivacuumed = ap_entry->at_gpivacuumed;
        }
    }

    heap_freetuple(partTuple);

    return tab;
}

