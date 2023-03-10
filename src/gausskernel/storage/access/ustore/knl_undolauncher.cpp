/* -------------------------------------------------------------------------
 * knl_undolauncher.cpp
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 * src/gausskernel/storage/access/ustore/knl_undolauncher.cpp
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

static void UndolauncherSighupHandler(SIGNAL_ARGS);
static void UndolauncherSigusr2Handler(SIGNAL_ARGS);
static void UndolauncherSigtermHandler(SIGNAL_ARGS);

static bool UndoLauncherGetWork(UndoWorkInfo work, int *idx);
static bool CanLaunchUndoWorker();
static void StartUndoWorker(UndoWorkInfo work, int idx);

/* SIGHUP: set flag to re-read config file at next convenient time */
static void UndolauncherSighupHandler(SIGNAL_ARGS)
{
    int saveErrno = errno;

    t_thrd.undolauncher_cxt.got_SIGHUP = true;
    if (t_thrd.undolauncher_cxt.UndoWorkerShmem)
        SetLatch(&t_thrd.undolauncher_cxt.UndoWorkerShmem->latch);

    errno = saveErrno;
}

/* SIGUSR2: a worker is up and running, or just finished, or failed to fork */
static void UndolauncherSigusr2Handler(SIGNAL_ARGS)
{
    int saveErrno = errno;

    t_thrd.undolauncher_cxt.got_SIGUSR2 = true;
    if (t_thrd.undolauncher_cxt.UndoWorkerShmem)
        SetLatch(&t_thrd.undolauncher_cxt.UndoWorkerShmem->latch);

    errno = saveErrno;
}

/* SIGTERM: time to die */
static void UndolauncherSigtermHandler(SIGNAL_ARGS)
{
    int saveErrno = errno;

    t_thrd.undolauncher_cxt.got_SIGTERM = true;
    if (t_thrd.undolauncher_cxt.UndoWorkerShmem)
        SetLatch(&t_thrd.undolauncher_cxt.UndoWorkerShmem->latch);

    errno = saveErrno;
}

static bool UndoLauncherGetWork(UndoWorkInfo work, int *idx)
{
    RollbackRequestsHashEntry *entry = GetNextRollbackRequest();
    int actualUndoWorkers = Min(g_instance.attr.attr_storage.max_undo_workers, MAX_UNDO_WORKERS);

    if (entry == NULL) {
        return false;
    }

    for (int i = 0; i < actualUndoWorkers; i++) {
        if (*idx == -1 && !TransactionIdIsValid(t_thrd.undolauncher_cxt.UndoWorkerShmem->undo_worker_status[i].xid)) {
            *idx = i;
        }
        if (t_thrd.undolauncher_cxt.UndoWorkerShmem->undo_worker_status[i].xid == entry->xid) {
            return false;
        }
    }

    work->xid = entry->xid;
    work->startUndoPtr = entry->startUndoPtr;
    work->endUndoPtr = entry->endUndoPtr;
    work->dbid = entry->dbid;
    work->slotPtr = entry->slotPtr;
    return true;
}

static bool CanLaunchUndoWorker()
{
    /*
     * requestIsLaunched means the data that rollback_request
     * is pointing at has been picked up by an undo worker and that
     * we can override the value.
     */
    uint32 activeWorkers = pg_atomic_read_u32(&t_thrd.undolauncher_cxt.UndoWorkerShmem->active_undo_workers);

    return (activeWorkers < (uint32)g_instance.attr.attr_storage.max_undo_workers);
}


static void StartUndoWorker(UndoWorkInfo work, int idx)
{
    errno_t rc = memcpy_s(t_thrd.undolauncher_cxt.UndoWorkerShmem->rollback_request, sizeof(UndoWorkInfoData), work,
        sizeof(UndoWorkInfoData));
    securec_check(rc, "\0", "\0");

    int actualUndoWorkers = Min(g_instance.attr.attr_storage.max_undo_workers, MAX_UNDO_WORKERS);
    const TimestampTz waitTime = 10 * 1000;
    const int maxRetryTimes = 1000;
    int retryTimes = 0;
    if (idx < 0 || idx >= actualUndoWorkers) {
        ereport(PANIC, (errmsg("Can't find a slot in undo_worker_status, max_undo_workers %d, active_undo_workers %u",
            g_instance.attr.attr_storage.max_undo_workers,
            t_thrd.undolauncher_cxt.UndoWorkerShmem->active_undo_workers)));
    }

    t_thrd.undolauncher_cxt.UndoWorkerShmem->undo_worker_status[idx].xid = work->xid;
    t_thrd.undolauncher_cxt.UndoWorkerShmem->undo_worker_status[idx].startUndoPtr = work->startUndoPtr;

    do {
        bool hit10s = (retryTimes % maxRetryTimes == 0);
        if (hit10s) {
            SendPostmasterSignal(PMSIGNAL_START_UNDO_WORKER);
        }
        if (!TransactionIdIsValid(t_thrd.undolauncher_cxt.UndoWorkerShmem->undo_worker_status[idx].xid) ||
            t_thrd.undolauncher_cxt.UndoWorkerShmem->undo_worker_status[idx].pid != InvalidPid) {
            break;
        }
        pg_usleep(waitTime);
        retryTimes++;
    } while (true);
}

Size UndoWorkerShmemSize(void)
{
    Size size = MAXALIGN(sizeof(UndoWorkerShmemStruct));
    size = add_size(size, sizeof(UndoWorkInfoData));
    return size;
}

void UndoWorkerShmemInit(void)
{
    bool found = false;
    t_thrd.undolauncher_cxt.UndoWorkerShmem =
        (UndoWorkerShmemStruct *)ShmemInitStruct("Undo Worker", UndoWorkerShmemSize(), &found);

    if (!found) {
        t_thrd.undolauncher_cxt.UndoWorkerShmem->undo_launcher_pid = 0;
        t_thrd.undolauncher_cxt.UndoWorkerShmem->active_undo_workers = 0;

        InitSharedLatch(&t_thrd.undolauncher_cxt.UndoWorkerShmem->latch);

        t_thrd.undolauncher_cxt.UndoWorkerShmem->rollback_request =
            (UndoWorkInfo)((char *)t_thrd.undolauncher_cxt.UndoWorkerShmem + MAXALIGN(sizeof(UndoWorkerShmemStruct)));

        for (int i = 0; i < MAX_UNDO_WORKERS; i++) {
            t_thrd.undolauncher_cxt.UndoWorkerShmem->undo_worker_status[i].xid = InvalidTransactionId;
            t_thrd.undolauncher_cxt.UndoWorkerShmem->undo_worker_status[i].pid = InvalidPid;
            t_thrd.undolauncher_cxt.UndoWorkerShmem->undo_worker_status[i].startUndoPtr = INVALID_UNDO_REC_PTR;
            t_thrd.undolauncher_cxt.UndoWorkerShmem->undo_worker_status[i].rollbackStartTime = (TimestampTz)0;
        }
    }
}

NON_EXEC_STATIC void UndoLauncherMain()
{
    sigjmp_buf localSigjmpBuf;
    long int defaultSleepTime = 1000L; /* 1 s */
    long int currSleepTime = defaultSleepTime;

    /* we are a postmaster subprocess now */
    IsUnderPostmaster = true;
    t_thrd.role = UNDO_LAUNCHER;

    /* reset t_thrd.proc_cxt.MyProcPid */
    t_thrd.proc_cxt.MyProcPid = gs_thread_self();
    t_thrd.undolauncher_cxt.UndoWorkerShmem->undo_launcher_pid = t_thrd.proc_cxt.MyProcPid;

    /* record Start Time for logging */
    t_thrd.proc_cxt.MyStartTime = time(NULL);

    t_thrd.proc_cxt.MyProgName = "UndoLauncher";

    OwnLatch(&t_thrd.undolauncher_cxt.UndoWorkerShmem->latch);

    init_ps_display("undo launcher process", "", "", "");
    ereport(LOG, (errmsg("undo launcher started")));

    SetProcessingMode(InitProcessing);

    /*
     * Set up signal handlers.  We operate on databases much like a regular
     * backend, so we use the same signal handling.  See equivalent code in
     * tcop/postgres.c.
     */
    gspqsignal(SIGHUP, UndolauncherSighupHandler);
    gspqsignal(SIGINT, StatementCancelHandler);
    gspqsignal(SIGTERM, UndolauncherSigtermHandler);

    gspqsignal(SIGQUIT, quickdie);
    gspqsignal(SIGALRM, handle_sig_alarm);

    gspqsignal(SIGPIPE, SIG_IGN);
    gspqsignal(SIGUSR1, procsignal_sigusr1_handler);
    gspqsignal(SIGUSR2, UndolauncherSigusr2Handler);
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
    t_thrd.proc_cxt.PostInit->InitUndoLauncher();

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

        /* release resource held by lsc */
        AtEOXact_SysDBCache(false);

        FlushErrorState();

        /* Now we can allow interrupts again */
        RESUME_INTERRUPTS();

        /* if in shutdown mode, no need for anything further; just go away */
        if (t_thrd.undolauncher_cxt.got_SIGTERM)
            goto shutdown;

        /*
         * Sleep at least 1 second after any error.  We don't want to be
         * filling the error logs as fast as we can.
         */
        pg_usleep(1000000L);
    }

    while (!t_thrd.undolauncher_cxt.got_SIGTERM) {
        UndoWorkInfoData work;
        int idx = -1;

        if (CanLaunchUndoWorker() && UndoLauncherGetWork(&work, &idx)) {
            StartUndoWorker(&work, idx);
            currSleepTime = defaultSleepTime;
        } else {
            /* Wait until sleep time expires or we get some type of signal */
            WaitLatch(&t_thrd.undolauncher_cxt.UndoWorkerShmem->latch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
                currSleepTime);

            ResetLatch(&t_thrd.undolauncher_cxt.UndoWorkerShmem->latch);

            /* Keep doubling sleep time until 5 mins */
            currSleepTime = Min(defaultSleepTime * 300, 2 * currSleepTime);
        }
    }

shutdown:
    ereport(LOG, (errmsg("undo launcher shutting down")));
    t_thrd.undolauncher_cxt.UndoWorkerShmem->undo_launcher_pid = 0;
    DisownLatch(&t_thrd.undolauncher_cxt.UndoWorkerShmem->latch);
    proc_exit(0);
}
