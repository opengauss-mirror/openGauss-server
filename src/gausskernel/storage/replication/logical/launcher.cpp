/* -------------------------------------------------------------------------
 * launcher.c
 * 	   PostgreSQL logical replication worker launcher process
 *
 * Copyright (c) 2012-2016, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 * 	  src/backend/replication/logical/launcher.c
 *
 * NOTES
 * 	  This module contains the logical replication worker launcher which
 * 	  uses the background worker infrastructure to start the logical
 * 	  replication workers for every enabled subscription.
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "funcapi.h"
#include "miscadmin.h"
#include "pgstat.h"

#include "access/heapam.h"
#include "access/tableam.h"
#include "access/htup.h"
#include "access/xact.h"

#include "catalog/pg_subscription.h"
#include "catalog/pg_database.h"
#include "commands/user.h"

#include "libpq/pqsignal.h"

#include "postmaster/fork_process.h"
#include "postmaster/postmaster.h"

#include "replication/logicallauncher.h"
#include "replication/logicalworker.h"
#include "replication/slot.h"
#include "replication/worker_internal.h"
#include "replication/walreceiver.h"

#include "storage/ipc.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/procsignal.h"

#include "tcop/tcopprot.h"

#include "utils/memutils.h"
#include "utils/pg_lsn.h"
#include "utils/ps_status.h"
#include "utils/snapmgr.h"
#include "utils/postinit.h"

/* max sleep time between cycles (3min) */
static const int DEFAULT_NAPTIME_PER_CYCLE = 180000L;

static const int wal_retrieve_retry_interval = 5000;
static const int PG_STAT_GET_SUBSCRIPTION_COLS = 7;
static const int WAIT_SUB_WORKER_ATTACH_CYCLE = 50000L; /* 50ms */
static const int WAIT_SUB_WORKER_ATTACH_TIMEOUT = 1000000L; /* 1s */

static void ApplyLauncherWakeup(void);
static void logicalrep_launcher_onexit(int code, Datum arg);
static void logicalrep_worker_onexit(int code, Datum arg);
static void logicalrep_worker_detach(void);

Datum pg_stat_get_subscription(PG_FUNCTION_ARGS);


/*
 * Load the list of subscriptions.
 *
 * Only the fields interesting for worker start/stop functions are filled for
 * each subscription.
 */
static List *get_subscription_list(void)
{
    List *res = NIL;
    Relation rel;
    TableScanDesc scan;
    HeapTuple tup;
    MemoryContext resultcxt;

    /* This is the context that we will allocate our output data in */
    resultcxt = CurrentMemoryContext;

    /*
     * Start a transaction so we can access pg_database, and get a snapshot.
     * We don't have a use for the snapshot itself, but we're interested in
     * the secondary effect that it sets RecentGlobalXmin.  (This is critical
     * for anything that reads heap pages, because HOT may decide to prune
     * them even if the process doesn't attempt to modify any tuples.)
     */
    StartTransactionCommand();
    (void)GetTransactionSnapshot();

    rel = heap_open(SubscriptionRelationId, AccessShareLock);
    scan = tableam_scan_begin(rel, SnapshotNow, 0, NULL);

    while (HeapTupleIsValid(tup = heap_getnext(scan, ForwardScanDirection))) {
        Form_pg_subscription subform = (Form_pg_subscription)GETSTRUCT(tup);
        Subscription *sub;
        MemoryContext oldcxt;

        /*
         * Allocate our results in the caller's context, not the
         * transaction's. We do this inside the loop, and restore the original
         * context at the end, so that leaky things like heap_getnext() are
         * not called in a potentially long-lived context.
         */
        oldcxt = MemoryContextSwitchTo(resultcxt);

        sub = (Subscription *)palloc0(sizeof(Subscription));
        sub->oid = HeapTupleGetOid(tup);
        sub->dbid = subform->subdbid;
        sub->owner = subform->subowner;
        sub->enabled = subform->subenabled;
        sub->name = pstrdup(NameStr(subform->subname));
        /* We don't fill fields we are not interested in. */

        res = lappend(res, sub);
        MemoryContextSwitchTo(oldcxt);
    }

    heap_endscan(scan);
    heap_close(rel, AccessShareLock);

    CommitTransactionCommand();

    return res;
}

/*
 * Similar to logicalrep_worker_find(), but returns list of all workers for
 * the subscription, instead just one.
 */
List *logicalrep_workers_find(Oid subid, bool only_running)
{
    int i;
    List *res = NIL;

    Assert(LWLockHeldByMe(LogicalRepWorkerLock));
    /* Search for attached worker for a given subscription id. */
    for (i = 0; i < g_instance.attr.attr_storage.max_logical_replication_workers; i++) {
        LogicalRepWorker *w = &t_thrd.applylauncher_cxt.applyLauncherShm->workers[i];

        if (w->subid == subid && (!only_running || w->proc)) {
            res = lappend(res, w);
        }
    }

    return res;
}

/*
 * Walks the workers array and searches for one that matches given
 * subscription id.
 */
static LogicalRepWorker *logicalrep_worker_find(Oid subid)
{
    int i;
    LogicalRepWorker *res = NULL;

    Assert(LWLockHeldByMe(LogicalRepWorkerLock));
    /* Search for attached worker for a given subscription id. */
    for (i = 0; i < g_instance.attr.attr_storage.max_logical_replication_workers; i++) {
        LogicalRepWorker *w = &t_thrd.applylauncher_cxt.applyLauncherShm->workers[i];
        if (w->subid == subid && w->proc) {
            res = w;
            break;
        }
    }

    return res;
}

/*
 * We can't start another apply worker when another one is still
 * starting up (or failed while doing so), so just sleep for a bit
 * more; that worker will wake us up again as soon as it's ready.
 * We will only wait 1 seconds for this to happen however. Note that
 * failure to connect to a particular database is not a problem here,
 * because the worker removes itself from the startingWorker
 * pointer before trying to connect.  Problems detected by the
 * The problems that may cause this code to fire are errors
 * in the earlier sections of ApplyWorkerMain, before the worker
 * removes the LogicalRepWorker from the startingWorker pointer.
 */
static void WaitForReplicationWorkerAttach()
{
    int timeout = WAIT_SUB_WORKER_ATTACH_TIMEOUT;
    while (timeout > 0) {
        CHECK_FOR_INTERRUPTS();
        LWLockAcquire(LogicalRepWorkerLock, LW_SHARED);

        if (t_thrd.applylauncher_cxt.applyLauncherShm->startingWorker == NULL) {
            /* worker has started, we are done */
            LWLockRelease(LogicalRepWorkerLock);
            break;
        }

        LWLockRelease(LogicalRepWorkerLock);

        pg_usleep(WAIT_SUB_WORKER_ATTACH_CYCLE);
        timeout -= WAIT_SUB_WORKER_ATTACH_CYCLE;
    }

    if (timeout <= 0) {
        /* worker took too long time to start */
        LWLockAcquire(LogicalRepWorkerLock, LW_EXCLUSIVE);
        LogicalRepWorker *worker = t_thrd.applylauncher_cxt.applyLauncherShm->startingWorker;
        ereport(WARNING, (errmsg("Apply worker with sub id:%u took too long time to start, so canceled it",
            worker->subid)));
        worker->dbid = InvalidOid;
        worker->userid = InvalidOid;
        worker->subid = InvalidOid;
        worker->proc = NULL;
        worker->workerLaunchTime = 0;
        t_thrd.applylauncher_cxt.applyLauncherShm->startingWorker = NULL;
        LWLockRelease(LogicalRepWorkerLock);
    }
}


/*
 * Start new apply background worker.
 */
static void logicalrep_worker_launch(Oid dbid, Oid subid, const char *subname, Oid userid)
{
    int slot;
    int rc;
    LogicalRepWorker *worker = NULL;

    ereport(DEBUG1, (errmsg("starting logical replication worker for subscription \"%s\"", subname)));

    /* Report this after the initial starting message for consistency. */
    if (g_instance.attr.attr_storage.max_replication_slots == 0)
        ereport(ERROR, (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
            errmsg("cannot start logical replication workers when max_replication_slots = 0")));

    /*
     * We need to do the modification of the shared memory under lock so that
     * we have consistent view.
     */
    LWLockAcquire(LogicalRepWorkerLock, LW_EXCLUSIVE);

    /* Find unused worker slot. */
    for (slot = 0; slot < g_instance.attr.attr_storage.max_logical_replication_workers; slot++) {
        if (!t_thrd.applylauncher_cxt.applyLauncherShm->workers[slot].proc) {
            worker = &t_thrd.applylauncher_cxt.applyLauncherShm->workers[slot];
            break;
        }
    }

    /* Bail if not found */
    if (worker == NULL) {
        LWLockRelease(LogicalRepWorkerLock);
        ereport(WARNING,
            (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED), errmsg("out of logical replication worker slots"),
            errhint("You might need to increase max_logical_replication_workers.")));
        return;
    }

    /* Prepare the worker info. */
    rc = memset_s(worker, sizeof(LogicalRepWorker), 0, sizeof(LogicalRepWorker));
    securec_check(rc, "", "");
    worker->dbid = dbid;
    worker->userid = userid;
    worker->subid = subid;
    worker->workerLaunchTime = GetCurrentTimestamp();

    t_thrd.applylauncher_cxt.applyLauncherShm->startingWorker = worker;
    LWLockRelease(LogicalRepWorkerLock);

    SendPostmasterSignal(PMSIGNAL_START_APPLY_WORKER);
    WaitForReplicationWorkerAttach();
}

/*
 * Stop the logical replication worker for subid/relid, if any, and wait until
 * it detaches from the slot.
 */
void logicalrep_worker_stop(Oid subid)
{
    LogicalRepWorker *worker;

    LWLockAcquire(LogicalRepWorkerLock, LW_SHARED);

    worker = logicalrep_worker_find(subid);
    /* No worker, nothing to do. */
    if (!worker) {
        LWLockRelease(LogicalRepWorkerLock);
        return;
    }

    /*
     * If we found a worker but it does not have proc set then it is still
     * starting up; wait for it to finish starting and then kill it.
     */
    while (worker && !worker->proc) {
        int rc;

        LWLockRelease(LogicalRepWorkerLock);

        CHECK_FOR_INTERRUPTS();

        /* Wait a bit --- we don't expect to have to wait long. */
        rc = WaitLatch(&t_thrd.proc->procLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, 10L);
        /* emergency bailout if postmaster has died */
        if (rc & WL_POSTMASTER_DEATH)
            proc_exit(1);

        ResetLatch(&t_thrd.proc->procLatch);

        /* Recheck worker status. */
        LWLockAcquire(LogicalRepWorkerLock, LW_SHARED);

        /*
         * Worker is no longer associated with subscription.  It must have
         * exited, nothing more for us to do.
         */
        if (worker->subid == InvalidOid) {
            LWLockRelease(LogicalRepWorkerLock);
            return;
        }

        /* Worker has assigned proc, so it has started. */
        if (worker->proc)
            break;
    }

    /* Now terminate the worker ... */
    (void)gs_signal_send(worker->proc->pid, SIGTERM);

    /* ... and wait for it to die. */
    for (;;) {
        int rc;

        /* is it gone? */
        if (!worker->proc) {
            break;
        }
        LWLockRelease(LogicalRepWorkerLock);

        CHECK_FOR_INTERRUPTS();

        /* Wait a bit --- we don't expect to have to wait long. */
        rc = WaitLatch(&t_thrd.proc->procLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, 10L);
        /* emergency bailout if postmaster has died */
        if (rc & WL_POSTMASTER_DEATH)
            proc_exit(1);

        ResetLatch(&t_thrd.proc->procLatch);
        LWLockAcquire(LogicalRepWorkerLock, LW_SHARED);
    }
    LWLockRelease(LogicalRepWorkerLock);
}

/*
 * Attach to a slot.
 */
void logicalrep_worker_attach()
{
    /* Block concurrent access. */
    LWLockAcquire(LogicalRepWorkerLock, LW_EXCLUSIVE);
    if (t_thrd.applylauncher_cxt.applyLauncherShm->startingWorker == NULL) {
        LWLockRelease(LogicalRepWorkerLock);
        /* no worker entry for me, go away */
        ereport(WARNING, (errmsg("apply worker started wihtout worker entry")));
        proc_exit(0);
    }

    t_thrd.applyworker_cxt.curWorker = t_thrd.applylauncher_cxt.applyLauncherShm->startingWorker;

    if (t_thrd.applyworker_cxt.curWorker->proc) {
        LWLockRelease(LogicalRepWorkerLock);
        ereport(WARNING,
            (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("logical replication worker already used by "
            "another worker: %lu",
            t_thrd.applyworker_cxt.curWorker->proc->pid)));
        proc_exit(0);
    }

    t_thrd.applyworker_cxt.curWorker->proc = t_thrd.proc;
    /*
     * Remove from the "starting" pointer, so that the launcher can start
     * a new worker if required
     */
    t_thrd.applylauncher_cxt.applyLauncherShm->startingWorker = NULL;
    on_shmem_exit(logicalrep_worker_onexit, (Datum)0);

    LWLockRelease(LogicalRepWorkerLock);
}

/*
 * Detach the worker (cleans up the worker info).
 */
static void logicalrep_worker_detach(void)
{
    /* Block concurrent access. */
    LWLockAcquire(LogicalRepWorkerLock, LW_EXCLUSIVE);

    t_thrd.applyworker_cxt.curWorker->dbid = InvalidOid;
    t_thrd.applyworker_cxt.curWorker->userid = InvalidOid;
    t_thrd.applyworker_cxt.curWorker->subid = InvalidOid;
    t_thrd.applyworker_cxt.curWorker->proc = NULL;
    t_thrd.applyworker_cxt.curWorker->workerLaunchTime = 0;

    LWLockRelease(LogicalRepWorkerLock);
}

/*
 * Cleanup function for logical replication launcher.
 *
 * Called on logical replication launcher exit.
 */
static void logicalrep_launcher_onexit(int code, Datum arg)
{
    t_thrd.applylauncher_cxt.applyLauncherShm->applyLauncherPid = 0;
}

/*
 * Cleanup function.
 *
 * Called on logical replication worker exit.
 */
static void logicalrep_worker_onexit(int code, Datum arg)
{
    (WalReceiverFuncTable[GET_FUNC_IDX]).walrcv_disconnect();
    logicalrep_worker_detach();
    if (t_thrd.applylauncher_cxt.applyLauncherShm->applyLauncherPid != 0) {
        gs_signal_send(t_thrd.applylauncher_cxt.applyLauncherShm->applyLauncherPid, SIGUSR1);
    }
}

/* SIGHUP: set flag to reload configuration at next convenient time */
static void logicalrepLauncherSighub(SIGNAL_ARGS)
{
    int saveErrno = errno;
    t_thrd.applylauncher_cxt.got_SIGHUP = true;

    if (t_thrd.proc) {
        SetLatch(&t_thrd.proc->procLatch);
    }
    errno = saveErrno;
}

/*
 * SIGUSR2: request to start a new worker, for CREATE/ALTER subscription.
 * we will loop pg_subscription and launch worker immediately.
 */
static void logicalrep_launcher_sigusr2(SIGNAL_ARGS)
{
    int save_errno = errno;

    t_thrd.applylauncher_cxt.newWorkerRequest = true;
    if (t_thrd.proc) {
        SetLatch(&t_thrd.proc->procLatch);
    }

    errno = save_errno;
}

/*
 * SIGUSR1: a worker just finished, or failed to start.
 * set latch, but we may not try to launch worker immediately, cause we don't
 * want a abnormal worker restart too fast to cause too many error log.
 */
static void LogicalrepLauncherSigusr1(SIGNAL_ARGS)
{
    int save_errno = errno;
    if (t_thrd.proc) {
        SetLatch(&t_thrd.proc->procLatch);
    }
    errno = save_errno;
}

/*
 * ApplyLauncherShmemSize
 * 		Compute space needed for replication launcher shared memory
 */
Size ApplyLauncherShmemSize(void)
{
    /*
     * Need the fixed struct and the array of LogicalRepWorker.
     */
    Size size = offsetof(ApplyLauncherShmStruct, workers);
    size = add_size(size,
        mul_size((Size)(uint32)g_instance.attr.attr_storage.max_logical_replication_workers, sizeof(LogicalRepWorker)));
    return size;
}

/*
 * ApplyLauncherShmemInit
 * 		Allocate and initialize replication launcher shared memory
 */
void ApplyLauncherShmemInit(void)
{
    Size memSize = ApplyLauncherShmemSize();
    if (memSize == 0) {
        return;
    }
    bool found = false;
    int rc;

    t_thrd.applylauncher_cxt.applyLauncherShm =
        (ApplyLauncherShmStruct *)ShmemInitStruct("Logical Replication Launcher Data", memSize, &found);

    if (!found) {
        rc = memset_s(t_thrd.applylauncher_cxt.applyLauncherShm, memSize, 0, memSize);
        securec_check(rc, "", "");
    }
}

/*
 * Wakeup the launcher on commit if requested.
 */
void AtEOXact_ApplyLauncher(bool isCommit)
{
    if (isCommit && t_thrd.applylauncher_cxt.onCommitLauncherWakeup)
        ApplyLauncherWakeup();
    t_thrd.applylauncher_cxt.onCommitLauncherWakeup = false;
}

/*
 * Request wakeup of the launcher on commit of the transaction.
 *
 * This is used to send launcher signal to stop sleeping and process the
 * subscriptions when current transaction commits. Should be used when new
 * tuple was added to the pg_subscription catalog.
 */
void ApplyLauncherWakeupAtCommit(void)
{
    if (!t_thrd.applylauncher_cxt.onCommitLauncherWakeup)
        t_thrd.applylauncher_cxt.onCommitLauncherWakeup = true;
}

static void ApplyLauncherWakeup(void)
{
    if (t_thrd.applylauncher_cxt.applyLauncherShm->applyLauncherPid != 0)
        gs_signal_send(t_thrd.applylauncher_cxt.applyLauncherShm->applyLauncherPid, SIGUSR2);
}

/*
 * Main loop for the apply launcher process.
 */
void ApplyLauncherMain()
{
    Assert(t_thrd.applylauncher_cxt.applyLauncherShm->applyLauncherPid == 0);
    sigjmp_buf localSigjmpBuf;
    TimestampTz last_start_time = 0;
    char username[NAMEDATALEN];

    /* we are a postmaster subprocess now */
    IsUnderPostmaster = true;
    t_thrd.role = APPLY_LAUNCHER;

    /* reset t_thrd.proc_cxt.MyProcPid */
    t_thrd.proc_cxt.MyProcPid = gs_thread_self();
    t_thrd.applylauncher_cxt.applyLauncherShm->applyLauncherPid = t_thrd.proc_cxt.MyProcPid;

    /* record Start Time for logging */
    t_thrd.proc_cxt.MyStartTime = time(NULL);

    t_thrd.proc_cxt.MyProgName = "ApplyLauncher";
    u_sess->attr.attr_common.application_name = pstrdup("ApplyLauncher");

    /* Identify myself via ps */
    init_ps_display("apply launcher process", "", "", "");
    ereport(DEBUG1, (errmsg("logical replication launcher started")));

    SetProcessingMode(InitProcessing);

    on_shmem_exit(logicalrep_launcher_onexit, (Datum) 0);

    /*
     * Set up signal handlers.	We operate on databases much like a regular
     * backend, so we use the same signal handling.  See equivalent code in
     * tcop/postgres.c.
     */
    /*
     * Set up signal handlers.  We operate on databases much like a regular
     * backend, so we use the same signal handling.  See equivalent code in
     * tcop/postgres.c.
     */
    gspqsignal(SIGHUP, logicalrepLauncherSighub);
    gspqsignal(SIGINT, StatementCancelHandler);
    gspqsignal(SIGTERM, die);

    gspqsignal(SIGQUIT, quickdie);
    gspqsignal(SIGALRM, handle_sig_alarm);

    gspqsignal(SIGPIPE, SIG_IGN);
    gspqsignal(SIGUSR1, LogicalrepLauncherSigusr1);
    gspqsignal(SIGUSR2, logicalrep_launcher_sigusr2);
    gspqsignal(SIGFPE, FloatExceptionHandler);
    gspqsignal(SIGCHLD, SIG_DFL);

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

    t_thrd.proc_cxt.PostInit->SetDatabaseAndUser(DEFAULT_DATABASE, InvalidOid, username);
    t_thrd.proc_cxt.PostInit->InitApplyLauncher();

    SetProcessingMode(NormalProcessing);

    /* Unblock signals (they were blocked when the postmaster forked us) */
    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();

    /*
     * If an exception is encountered, processing resumes here.
     *
     * This code is a stripped down version of PostgresMain error recovery.
     */
    if (sigsetjmp(localSigjmpBuf, 1) != 0) {
        /* since not using PG_TRY, must reset error stack by hand */
        t_thrd.log_cxt.error_context_stack = NULL;

        /* Prevents interrupts while cleaning up */
        HOLD_INTERRUPTS();

        /* Report the error to the server log */
        EmitErrorReport();

        FlushErrorState();

        /* Now we can allow interrupts again */
        RESUME_INTERRUPTS();

        /*
         * Sleep at least 1 second after any error.  We don't want to be
         * filling the error logs as fast as we can.
         */
        pg_usleep(1000000L);
    }

    /* Enter main loop */
    while (true) {
        int rc;
        List *sublist;
        ListCell *lc;
        MemoryContext subctx;
        MemoryContext oldctx;
        TimestampTz now;
        long wait_time = DEFAULT_NAPTIME_PER_CYCLE;

        CHECK_FOR_INTERRUPTS();
        now = GetCurrentTimestamp();

        /*
         * Limit the start retry to once a wal_retrieve_retry_interval, but if it's a request from
         * CREATE/ALTER subscription, we will try to launch worker immediately.
         */
        if (t_thrd.applylauncher_cxt.newWorkerRequest ||
            TimestampDifferenceExceeds(last_start_time, now, wal_retrieve_retry_interval)) {
            if (t_thrd.applylauncher_cxt.newWorkerRequest) {
                t_thrd.applylauncher_cxt.newWorkerRequest = false;
            }
            /* Use temporary context for the database list and worker info. */
            subctx = AllocSetContextCreate(TopMemoryContext, "Logical Replication Launcher sublist",
                ALLOCSET_DEFAULT_SIZES);
            oldctx = MemoryContextSwitchTo(subctx);

            /* search for subscriptions to start or stop. */
            sublist = get_subscription_list();

            /* Start the missing workers for enabled subscriptions. */
            List *pendingSubList = NIL;
            foreach (lc, sublist) {
                Subscription *sub = (Subscription *)lfirst(lc);
                LogicalRepWorker *w;

                if (!sub->enabled) {
                    continue;
                }

                LWLockAcquire(LogicalRepWorkerLock, LW_SHARED);
                w = logicalrep_worker_find(sub->oid);
                LWLockRelease(LogicalRepWorkerLock);

                /* Add to pending list if the subscription has no work attached */
                if (w == NULL) {
                    pendingSubList = lappend(pendingSubList, sub);
                }
            }

            /*
             * Try to launch the subscription worker one by one, we will wait at the end of
             * logicalrep_worker_launch, to make sure in the next loop, the previous worker
             * has started or aborted definitely.
             */
            foreach(lc, pendingSubList) {
                Subscription *readyToLaunchSub = (Subscription*)lfirst(lc);
                logicalrep_worker_launch(readyToLaunchSub->dbid, readyToLaunchSub->oid,
                    readyToLaunchSub->name, readyToLaunchSub->owner);
                last_start_time = now;
                wait_time = wal_retrieve_retry_interval;
            }

            /* Switch back to original memory context. */
            MemoryContextSwitchTo(oldctx);
            /* Clean the temporary memory. */
            MemoryContextDelete(subctx);
        } else {
            /*
             * The wait in previous cycle was interrupted in less than
             * wal_retrieve_retry_interval since last worker was started,
             * this usually means crash of the worker, so we should retry
             * in wal_retrieve_retry_interval again.
             */
            wait_time = wal_retrieve_retry_interval;
        }

        /* Wait for more work. */
        rc = WaitLatch(&t_thrd.proc->procLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, wait_time);
        /* emergency bailout if postmaster has died */
        if (rc & WL_POSTMASTER_DEATH)
            proc_exit(1);

        if (t_thrd.applylauncher_cxt.got_SIGHUP) {
            t_thrd.applylauncher_cxt.got_SIGHUP = false;
            ProcessConfigFile(PGC_SIGHUP);
        }

        ResetLatch(&t_thrd.proc->procLatch);
    }

    t_thrd.applylauncher_cxt.applyLauncherShm->applyLauncherPid = 0;
    /* ... and if it returns, we're done */
    ereport(DEBUG1, (errmsg("logical replication launcher shutting down")));
}

/*
 * Is current process the logical replication launcher?
 */
bool IsLogicalLauncher(void)
{
    return t_thrd.applylauncher_cxt.applyLauncherShm != NULL &&
        t_thrd.applylauncher_cxt.applyLauncherShm->applyLauncherPid == t_thrd.proc_cxt.MyProcPid;
}

/*
 * Returns state of the subscriptions.
 */
Datum pg_stat_get_subscription(PG_FUNCTION_ARGS)
{
    Oid subid = PG_ARGISNULL(0) ? InvalidOid : PG_GETARG_OID(0);
    int i;
    ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
    TupleDesc tupdesc;
    Tuplestorestate *tupstore;
    MemoryContext per_query_ctx;
    MemoryContext oldcontext;

    /* check to see if caller supports us returning a tuplestore */
    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("set-valued function called in context that cannot accept a set")));
    if (!(rsinfo->allowedModes & SFRM_Materialize))
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("materialize mode required, but it is not "
            "allowed in this context")));

    /* Build a tuple descriptor for our result type */
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        elog(ERROR, "return type must be a row type");

    per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
    oldcontext = MemoryContextSwitchTo(per_query_ctx);

    tupstore = tuplestore_begin_heap(true, false, u_sess->attr.attr_memory.work_mem);
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;
    rsinfo->setDesc = tupdesc;

    MemoryContextSwitchTo(oldcontext);

    /* Make sure we get consistent view of the workers. */
    LWLockAcquire(LogicalRepWorkerLock, LW_SHARED);

    for (i = 0; i <= g_instance.attr.attr_storage.max_logical_replication_workers; i++) {
        /* for each row */
        Datum values[PG_STAT_GET_SUBSCRIPTION_COLS];
        bool nulls[PG_STAT_GET_SUBSCRIPTION_COLS];
        int worker_pid;
        LogicalRepWorker worker;
        int rc;
        int idx = 0;

        rc = memcpy_s(&worker, sizeof(LogicalRepWorker),
            &t_thrd.applylauncher_cxt.applyLauncherShm->workers[i], sizeof(LogicalRepWorker));
        securec_check(rc, "", "");
        if (!worker.proc || worker.proc->pid == 0)
            continue;

        if (OidIsValid(subid) && worker.subid != subid)
            continue;

        worker_pid = worker.proc->pid;

        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "", "");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "", "");

        values[idx++] = ObjectIdGetDatum(worker.subid);
        values[idx++] = Int32GetDatum(worker_pid);
        if (XLogRecPtrIsInvalid(worker.last_lsn))
            nulls[idx++] = true;
        else {
            char last_lsn_s[MAXFNAMELEN];
            int nRet = 0;
            nRet = snprintf_s(last_lsn_s, sizeof(last_lsn_s), sizeof(last_lsn_s) - 1, "%X/%X",
                                    (uint32)(worker.last_lsn >> BITS_PER_INT), (uint32)worker.last_lsn);
            securec_check_ss(nRet, "\0", "\0");
            values[idx++] = CStringGetTextDatum(last_lsn_s);
        }
        if (worker.last_send_time == 0)
            nulls[idx++] = true;
        else
            values[idx++] = TimestampTzGetDatum(worker.last_send_time);
        if (worker.last_recv_time == 0)
            nulls[idx++] = true;
        else
            values[idx++] = TimestampTzGetDatum(worker.last_recv_time);
        if (XLogRecPtrIsInvalid(worker.reply_lsn))
            nulls[idx++] = true;
        else {
            char reply_lsn_s[MAXFNAMELEN];
            int nRet = 0;
            nRet = snprintf_s(reply_lsn_s, sizeof(reply_lsn_s), sizeof(reply_lsn_s) - 1, "%X/%X",
                                    (uint32)(worker.reply_lsn >> BITS_PER_INT), (uint32)worker.reply_lsn);
            securec_check_ss(nRet, "\0", "\0");
            values[idx++] = CStringGetTextDatum(reply_lsn_s);
        }
        if (worker.reply_time == 0)
            nulls[idx++] = true;
        else
            values[idx++] = TimestampTzGetDatum(worker.reply_time);

        tuplestore_putvalues(tupstore, tupdesc, values, nulls);

        /* If only a single subscription was requested, and we found it, break. */
        if (OidIsValid(subid))
            break;
    }

    LWLockRelease(LogicalRepWorkerLock);

    /* clean up and return the tuplestore */
    tuplestore_donestoring(tupstore);

    return (Datum)0;
}
