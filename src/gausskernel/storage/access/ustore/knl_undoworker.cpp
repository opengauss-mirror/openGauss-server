
/* -------------------------------------------------------------------------
 *
 * knl_undoworker.cpp
 * access interfaces of the async undo worker for the ustore engine.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 * src/gausskernel/storage/access/ustore/knl_undoworker.cpp
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

#include "commands/dbcommands.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/fork_process.h"
#include "postmaster/postmaster.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/procsignal.h"
#include "storage/sinvaladt.h"
#include "utils/postinit.h"
#include "utils/ps_status.h"
#include "utils/timestamp.h"
#include "commands/user.h"
#include "gssignal/gs_signal.h"
#include "access/ustore/knl_undoworker.h"
#include "access/ustore/knl_undorequest.h"

#define InvalidPid ((ThreadId)(-1))

static void UndoworkerSighupHandler(SIGNAL_ARGS);
static void UndoworkerSigusr2Handler(SIGNAL_ARGS);
static void UndoworkerSigtermHandler(SIGNAL_ARGS);

static void UndoWorkerFreeInfo(int code, Datum arg);
static void UndoWorkerGetWork(UndoWorkInfo work);

/* SIGHUP: set flag to re-read config file at next convenient time */
static void UndoworkerSighupHandler(SIGNAL_ARGS)
{
    int saveErrno = errno;

    t_thrd.undoworker_cxt.got_SIGHUP = true;
    if (t_thrd.proc)
        SetLatch(&t_thrd.proc->procLatch);

    errno = saveErrno;
}

/* SIGUSR2: a worker is up and running, or just finished, or failed to fork */
static void UndoworkerSigusr2Handler(SIGNAL_ARGS)
{
    int saveErrno = errno;

    t_thrd.undoworker_cxt.got_SIGUSR2 = true;
    if (t_thrd.proc)
        SetLatch(&t_thrd.proc->procLatch);

    errno = saveErrno;
}

/* SIGTERM: time to die */
static void UndoworkerSigtermHandler(SIGNAL_ARGS)
{
    int saveErrno = errno;

    t_thrd.undoworker_cxt.got_SIGTERM = true;
    if (t_thrd.proc)
        SetLatch(&t_thrd.proc->procLatch);

    errno = saveErrno;
}

static void UndoWorkerFreeInfo(int code, Datum arg)
{
    int idx = -1;
    bool found = false;
    ThreadId pid = gs_thread_self();
    RollbackRequestsHashKey key;
    RollbackRequestsHashEntry *entry = NULL;

    int actualUndoWorkers = Min(g_instance.attr.attr_storage.max_undo_workers, MAX_UNDO_WORKERS);

    for (int i = 0; i < actualUndoWorkers; i++) {
        if (t_thrd.undolauncher_cxt.UndoWorkerShmem->undo_worker_status[i].pid == pid) {
            idx = i;
            key.xid = t_thrd.undolauncher_cxt.UndoWorkerShmem->undo_worker_status[i].xid;
            key.startUndoPtr = t_thrd.undolauncher_cxt.UndoWorkerShmem->undo_worker_status[i].startUndoPtr;
            t_thrd.undolauncher_cxt.UndoWorkerShmem->undo_worker_status[i].pid = InvalidPid;
            t_thrd.undolauncher_cxt.UndoWorkerShmem->undo_worker_status[i].xid = InvalidTransactionId;
            t_thrd.undolauncher_cxt.UndoWorkerShmem->undo_worker_status[i].startUndoPtr = INVALID_UNDO_REC_PTR;
            t_thrd.undolauncher_cxt.UndoWorkerShmem->undo_worker_status[i].rollbackStartTime = (TimestampTz)0;
            break;
        }
    }
    
    if (idx >= 0 && idx < actualUndoWorkers) {
        LWLockAcquire(RollbackReqHashLock, LW_EXCLUSIVE);
        entry = (RollbackRequestsHashEntry *)hash_search(t_thrd.rollback_requests_cxt.rollback_requests_hash, &key,
            HASH_FIND, &found);
        if (found) {
            entry->launched = false;
        }
        LWLockRelease(RollbackReqHashLock);
    }

    pg_atomic_sub_fetch_u32(&t_thrd.undolauncher_cxt.UndoWorkerShmem->active_undo_workers, 1);
    return;
}

static void UndoWorkerGetWork(UndoWorkInfo undowork)
{
    errno_t rc = memcpy_s(undowork, sizeof(UndoWorkInfoData), t_thrd.undolauncher_cxt.UndoWorkerShmem->rollback_request,
        sizeof(UndoWorkInfoData));
    securec_check(rc, "\0", "\0");
}

static void UndoPerformWork(UndoWorkInfo undowork)
{
    bool error = false;

    /* Should be connected to a database */
    Assert(u_sess->proc_cxt.MyDatabaseId != InvalidOid && undowork->dbid != InvalidOid);
    StartTransactionCommand();
    PG_TRY();
    {
        elog(LOG, "UndoWorker: Performing Rollback for xid:%ld, undo:(%ld -> %ld)", undowork->xid,
            undowork->startUndoPtr, undowork->endUndoPtr);
        ExecuteUndoActions(undowork->xid, undowork->startUndoPtr, /* last undorecord created in the txn */
            undowork->endUndoPtr,                                 /* first undorecord created in the txn */
            undowork->slotPtr, true);
    }
    PG_CATCH();
    {
        error = true;

        elog(WARNING,
            "[UndoPerformWork:] Error occured while executing undo actions "
            "TransactionId: %ld, latest urp: %ld, first urp: %lu, last urp %lu, dbid: %d",
            undowork->xid, undowork->startUndoPtr, undowork->endUndoPtr, undowork->startUndoPtr, undowork->dbid);

        ReportFailedRollbackRequest(undowork->xid, undowork->startUndoPtr, undowork->endUndoPtr, undowork->dbid);

        /* Prevent interrupts while cleaning up. */
        HOLD_INTERRUPTS();

        /* Send the error only to server log. */
        ErrOutToClient(false);
        EmitErrorReport();

        /*
         * Abort the transaction and continue processing pending undo
         * requests.
         */
        AbortOutOfAnyTransaction();
        FlushErrorState();

        RESUME_INTERRUPTS();
    }
    PG_END_TRY();

    if (!error) {
        CommitTransactionCommand();
        RemoveRollbackRequest(undowork->xid, undowork->startUndoPtr, gs_thread_self());
    }
}

bool IsUndoWorkerProcess(void)
{
    return t_thrd.role == UNDO_WORKER;
}

NON_EXEC_STATIC void UndoWorkerMain()
{
    UndoWorkInfoData undowork;
    bool databaseExists = false;
    int actualUndoWorkers = Min(g_instance.attr.attr_storage.max_undo_workers, MAX_UNDO_WORKERS);

    sigjmp_buf localSigjmpBuf;

    /* we are a postmaster subprocess now */
    IsUnderPostmaster = true;
    t_thrd.role = UNDO_WORKER;

    /* reset t_thrd.proc_cxt.MyProcPid */
    t_thrd.proc_cxt.MyProcPid = gs_thread_self();

    /* record Start Time for logging */
    t_thrd.proc_cxt.MyStartTime = time(NULL);

    t_thrd.proc_cxt.MyProgName = "UndoWorker";

    init_ps_display("undo worker process", "", "", "");
    ereport(LOG, (errmsg("UndoWorker: started")));

    SetProcessingMode(InitProcessing);

    /*
     * Set up signal handlers.  We operate on databases much like a regular
     * backend, so we use the same signal handling.  See equivalent code in
     * tcop/postgres.c.
     */
    gspqsignal(SIGHUP, UndoworkerSighupHandler);
    gspqsignal(SIGINT, StatementCancelHandler);
    gspqsignal(SIGTERM, UndoworkerSigtermHandler);

    gspqsignal(SIGQUIT, quickdie);
    gspqsignal(SIGALRM, handle_sig_alarm);

    gspqsignal(SIGPIPE, SIG_IGN);
    gspqsignal(SIGUSR1, procsignal_sigusr1_handler);
    gspqsignal(SIGUSR2, UndoworkerSigusr2Handler);
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

        /* release resource held by lsc */
        AtEOXact_SysDBCache(false);

        FlushErrorState();

        AbortOutOfAnyTransaction();

        /* Now we can allow interrupts again */
        RESUME_INTERRUPTS();

        /* if in shutdown mode, no need for anything further; just go away */
        if (t_thrd.undoworker_cxt.got_SIGTERM)
            goto shutdown;

        /*
         * Sleep at least 1 second after any error.  We don't want to be
         * filling the error logs as fast as we can.
         */
        pg_usleep(1000000L);
    }

    /* Let the UndoLauncher know we have picked up the job and that we're active. */
    pg_atomic_add_fetch_u32(&t_thrd.undolauncher_cxt.UndoWorkerShmem->active_undo_workers, 1);

    on_shmem_exit(UndoWorkerFreeInfo, 0);

    /* Get the work from the shared memory */
    UndoWorkerGetWork(&undowork);
    
    for (int i = 0; i < actualUndoWorkers; i++) {
        if (TransactionIdEquals(t_thrd.undolauncher_cxt.UndoWorkerShmem->undo_worker_status[i].xid, undowork.xid)) {
            t_thrd.undolauncher_cxt.UndoWorkerShmem->undo_worker_status[i].pid = gs_thread_self();
            t_thrd.undolauncher_cxt.UndoWorkerShmem->undo_worker_status[i].rollbackStartTime = GetCurrentTimestamp();
            break;
        }
    }

    Assert(undowork.dbid != InvalidOid);

    t_thrd.proc_cxt.PostInit->SetDatabaseAndUser(NULL, undowork.dbid, NULL);
    databaseExists = t_thrd.proc_cxt.PostInit->InitUndoWorker();

    SetProcessingMode(NormalProcessing);
    pgstat_report_appname("UndoWorker");
    pgstat_report_activity(STATE_IDLE, NULL);

    if (databaseExists) {
        /* Perform the actual rollback */
        UndoPerformWork(&undowork);
    } else {
        undo::UpdateRollbackFinish(undowork.slotPtr);
        ereport(WARNING, (errmodule(MOD_UNDO),
            errmsg(
                "db dose not exists but we need rollback txn on it, "
                "xid %lu, fromAddr %lu, toAddr %lu, dbid %u, slotptr %lu.",
                undowork.xid, undowork.startUndoPtr, undowork.endUndoPtr, undowork.dbid,
                undowork.slotPtr)));
        RemoveRollbackRequest(undowork.xid, undowork.startUndoPtr, gs_thread_self());
    }

shutdown:
    ereport(LOG, (errmsg("UndoWorker: shutting down")));
    proc_exit(0);
}
