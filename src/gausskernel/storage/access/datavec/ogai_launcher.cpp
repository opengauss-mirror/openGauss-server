/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-server is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
*
* ---------------------------------------------------------------------------------------
*
* ogai_launcher.cpp
*
* IDENTIFICATION
*        src/include/access/datavec/ogai_launcher.cpp
*
* ---------------------------------------------------------------------------------------
*/

#include <signal.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>
#ifdef HAVE_SYS_STAT_H
#include <sys/stat.h>
#endif

#include "postgres.h"
#include "knl/knl_variable.h"
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
#include "access/datavec/ogai_worker.h"
#include "commands/dbcommands.h"

#define INVALID_PID ((ThreadId)(-1))

static void OgailauncherSighupHandler(SIGNAL_ARGS);
static void OgailauncherSigusr2Handler(SIGNAL_ARGS);
static void OgailauncherSigtermHandler(SIGNAL_ARGS);

static bool OgaiLauncherGetWork(OgaiWorkInfo work, int *idx);
static bool CanLaunchOgaiWorker();
static void PrepareOgaiWorker(OgaiWorkInfo work, int idx);

/* SIGHUP: set flag to re-read config file at next convenient time */
static void OgailauncherSighupHandler(SIGNAL_ARGS)
{
    int saveErrno = errno;

    t_thrd.worker_sig_flags.got_SIGHUP = true;
    if (t_thrd.ogailauncher_cxt.ogaiWorkerShmem)
        SetLatch(&t_thrd.ogailauncher_cxt.ogaiWorkerShmem->latch);

    errno = saveErrno;
}

/* SIGUSR2: a worker is up and running, or just finished, or failed to fork */
static void OgailauncherSigusr2Handler(SIGNAL_ARGS)
{
    int saveErrno = errno;

    t_thrd.worker_sig_flags.got_SIGUSR2 = true;
    if (t_thrd.ogailauncher_cxt.ogaiWorkerShmem)
        SetLatch(&t_thrd.ogailauncher_cxt.ogaiWorkerShmem->latch);

    errno = saveErrno;
}

/* SIGTERM: time to die */
static void OgailauncherSigtermHandler(SIGNAL_ARGS)
{
    int saveErrno = errno;

    t_thrd.worker_sig_flags.got_SIGTERM = true;
    if (t_thrd.ogailauncher_cxt.ogaiWorkerShmem)
        SetLatch(&t_thrd.ogailauncher_cxt.ogaiWorkerShmem->latch);

    errno = saveErrno;
}

static bool OgaiLauncherGetWork(OgaiWorkInfo work, int *idx)
{
    int actualOgaiWorkers = MAX_OGAI_WORKERS;

    for (int i = 0; i < actualOgaiWorkers; i++) {
        if (!OidIsValid(t_thrd.ogailauncher_cxt.ogaiWorkerShmem->ogai_worker_status[i].dbid)) {
            *idx = i;
            break;
        }
    }

    if (*idx == -1) {
        return false;
    }

    for (int j = 0; j < actualOgaiWorkers; j++) {
        Oid candidate_dboid = t_thrd.ogailauncher_cxt.ogaiWorkerShmem->target_dbs[j];
        
        if (!OidIsValid(candidate_dboid)) {
            continue;
        }

        bool alreadyHasWorker = false;
        for (int i = 0; i < actualOgaiWorkers; i++) {
            if (t_thrd.ogailauncher_cxt.ogaiWorkerShmem->ogai_worker_status[i].dbid == candidate_dboid) {
                alreadyHasWorker = true;
                t_thrd.ogailauncher_cxt.ogaiWorkerShmem->target_dbs[j] = InvalidOid;
                break;
            }
        }

        if (!alreadyHasWorker) {
            work->dbid = candidate_dboid;
            return true;
        }
    }
    return false;
}

static bool CanLaunchOgaiWorker()
{
    /*
    * requestIsLaunched means the data that rollback_request
    * is pointing at has been picked up by an undo worker and that
    * we can override the value.
    */
    uint32 activeWorkers = pg_atomic_read_u32(&t_thrd.ogailauncher_cxt.ogaiWorkerShmem->active_ogai_workers);

    return (activeWorkers < MAX_OGAI_WORKERS);
}


static void PrepareOgaiWorker(OgaiWorkInfo work, int idx)
{
    errno_t rc = memcpy_s(t_thrd.ogailauncher_cxt.ogaiWorkerShmem->createdb_request, sizeof(OgaiWorkInfoData), work,
        sizeof(OgaiWorkInfoData));
    securec_check(rc, "\0", "\0");

    int actualOgaiWorkers = MAX_OGAI_WORKERS;
    const TimestampTz waitTime = 10 * 1000;
    const int maxRetryTimes = 1000;
    int retryTimes = 0;
    if (idx < 0 || idx >= actualOgaiWorkers) {
        ereport(PANIC, (errmsg("Can't find a slot in ogai_worker_status, max_ogai_workers %d, active_ogai_workers %u",
            actualOgaiWorkers,
            t_thrd.ogailauncher_cxt.ogaiWorkerShmem->active_ogai_workers)));
    }

    t_thrd.ogailauncher_cxt.ogaiWorkerShmem->ogai_worker_status[idx].dbid = work->dbid;

    while (!t_thrd.worker_sig_flags.got_SIGTERM) {
        bool hit10s = (retryTimes % maxRetryTimes == 0);
        if (hit10s) {
            SendPostmasterSignal(PMSIGNAL_START_OGAI_WORKER);
        }
        if (!OidIsValid(t_thrd.ogailauncher_cxt.ogaiWorkerShmem->ogai_worker_status[idx].dbid) ||
            t_thrd.ogailauncher_cxt.ogaiWorkerShmem->ogai_worker_status[idx].pid != INVALID_PID) {
            break;
        }
        pg_usleep(waitTime);
        retryTimes++;
    };
}

Size OgaiWorkerShmemSize(void)
{
    Size size = MAXALIGN(sizeof(OgaiWorkerShmemStruct));
    size = add_size(size, sizeof(OgaiWorkInfoData));
    return size;
}

void OgaiWorkerShmemInit(void)
{
    bool found = false;
    t_thrd.ogailauncher_cxt.ogaiWorkerShmem =
        (OgaiWorkerShmemStruct *)ShmemInitStruct("Ogai Worker", OgaiWorkerShmemSize(), &found);

    if (!found) {
        t_thrd.ogailauncher_cxt.ogaiWorkerShmem->ogai_launcher_pid = 0;
        t_thrd.ogailauncher_cxt.ogaiWorkerShmem->active_ogai_workers = 0;

        InitSharedLatch(&t_thrd.ogailauncher_cxt.ogaiWorkerShmem->latch);

        t_thrd.ogailauncher_cxt.ogaiWorkerShmem->createdb_request =
            (OgaiWorkInfo)((char *)t_thrd.ogailauncher_cxt.ogaiWorkerShmem + MAXALIGN(sizeof(OgaiWorkerShmemStruct)));

        for (int i = 0; i < MAX_OGAI_WORKERS; i++) {
            t_thrd.ogailauncher_cxt.ogaiWorkerShmem->target_dbs[i] = InvalidOid;
            t_thrd.ogailauncher_cxt.ogaiWorkerShmem->ogai_worker_status[i].pid = INVALID_PID;
            t_thrd.ogailauncher_cxt.ogaiWorkerShmem->ogai_worker_status[i].dbid = InvalidOid;
            t_thrd.ogailauncher_cxt.ogaiWorkerShmem->ogai_worker_status[i].createdbTime = (TimestampTz)0;
        }
    }
}

void OgaiLauncherQuitAndClean(int code, Datum arg)
{
    ereport(LOG, (errmsg("undo launcher shutting down")));
    t_thrd.ogailauncher_cxt.ogaiWorkerShmem->ogai_launcher_pid = 0;
    DisownLatch(&t_thrd.ogailauncher_cxt.ogaiWorkerShmem->latch);
}

NON_EXEC_STATIC void OgaiLauncherMain()
{
    sigjmp_buf localSigjmpBuf;
    long int defaultSleepTime = 1000L; /* 1 s */
    long int currSleepTime = defaultSleepTime;

    /* we are a postmaster subprocess now */
    IsUnderPostmaster = true;
    t_thrd.role = OGAI_LAUNCHER;

    /* reset t_thrd.proc_cxt.MyProcPid */
    t_thrd.proc_cxt.MyProcPid = gs_thread_self();
    t_thrd.ogailauncher_cxt.ogaiWorkerShmem->ogai_launcher_pid = t_thrd.proc_cxt.MyProcPid;

    /* record Start Time for logging */
    t_thrd.proc_cxt.MyStartTime = time(NULL);

    t_thrd.proc_cxt.MyProgName = "OgaiLauncher";

    OwnLatch(&t_thrd.ogailauncher_cxt.ogaiWorkerShmem->latch);

    init_ps_display("ogai launcher process", "", "", "");
    ereport(LOG, (errmsg("ogai launcher started")));

    SetProcessingMode(InitProcessing);

    /*
    * Set up signal handlers.  We operate on databases much like a regular
    * backend, so we use the same signal handling.  See equivalent code in
    * tcop/postgres.c.
    */
    gspqsignal(SIGHUP, OgailauncherSighupHandler);
    gspqsignal(SIGINT, StatementCancelHandler);
    gspqsignal(SIGTERM, OgailauncherSigtermHandler);

    gspqsignal(SIGQUIT, quickdie);
    gspqsignal(SIGALRM, handle_sig_alarm);

    gspqsignal(SIGPIPE, SIG_IGN);
    gspqsignal(SIGUSR1, procsignal_sigusr1_handler);
    gspqsignal(SIGUSR2, OgailauncherSigusr2Handler);
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
    t_thrd.proc_cxt.PostInit->InitOgaiLauncher();

    SetProcessingMode(NormalProcessing);

    on_proc_exit(OgaiLauncherQuitAndClean, 0);

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
        if (t_thrd.worker_sig_flags.got_SIGTERM) {
            goto shutdown;
        }

        /*
        * Sleep at least 1 second after any error.  We don't want to be
        * filling the error logs as fast as we can.
        */
        pg_usleep(1000000L);
    }
    while (!t_thrd.worker_sig_flags.got_SIGTERM) {
        OgaiWorkInfoData work;
        int idx = -1;

        if (CanLaunchOgaiWorker() && OgaiLauncherGetWork(&work, &idx)) {
            PrepareOgaiWorker(&work, idx);
            currSleepTime = defaultSleepTime;
        } else {
            /* Wait until sleep time expires or we get some type of signal */
            WaitLatch(&t_thrd.ogailauncher_cxt.ogaiWorkerShmem->latch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
                currSleepTime);

            ResetLatch(&t_thrd.ogailauncher_cxt.ogaiWorkerShmem->latch);

            /* Keep doubling sleep time until 5 mins */
            currSleepTime = Min(defaultSleepTime * 300, 2 * currSleepTime);
        }
    }

shutdown:
    proc_exit(0);
}
