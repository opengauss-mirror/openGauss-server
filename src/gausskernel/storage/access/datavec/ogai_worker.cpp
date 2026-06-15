/*
* Copyright (c) 2025 Huawei Technologies Co.,Ltd.
*
* openGauss is licensed under Mulan PSL v2.
* You can use this software according to the terms and conditions of the Mulan PSL v2.
* You may obtain a copy of Mulan PSL v2 at:
*
*          http://license.coscl.org.cn/MulanPSL2
*
* THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
* EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
* MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
* See the Mulan PSL v2 for more details.
* ---------------------------------------------------------------------------------------
*
* ogai_worker.cpp
*
* IDENTIFICATION
*        src/include/access/datavec/ogai_worker.cpp
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
#include "access/datavec/ogai_vector_processor.h"
#include "access/datavec/ogai_worker.h"

#define INVALID_PID ((ThreadId)(-1))

static void OgaiworkerSighupHandler(SIGNAL_ARGS);
static void OgaiworkerSigusr2Handler(SIGNAL_ARGS);
static void OgaiworkerSigtermHandler(SIGNAL_ARGS);

static void OgaiWorkerFreeInfo(int code, Datum arg);
static void OgaiWorkerGetWork(OgaiWorkInfo work);

/* SIGHUP: set flag to re-read config file at next convenient time */
static void OgaiworkerSighupHandler(SIGNAL_ARGS)
{
int saveErrno = errno;

t_thrd.worker_sig_flags.got_SIGHUP = true;
if (t_thrd.proc)
    SetLatch(&t_thrd.proc->procLatch);

errno = saveErrno;
}

/* SIGUSR2: a worker is up and running, or just finished, or failed to fork */
static void OgaiworkerSigusr2Handler(SIGNAL_ARGS)
{
    int saveErrno = errno;

    t_thrd.worker_sig_flags.got_SIGUSR2 = true;
    if (t_thrd.proc)
        SetLatch(&t_thrd.proc->procLatch);

    errno = saveErrno;
}

/* SIGTERM: time to die */
static void OgaiworkerSigtermHandler(SIGNAL_ARGS)
{
    int saveErrno = errno;

    t_thrd.worker_sig_flags.got_SIGTERM = true;
    t_thrd.int_cxt.ProcDiePending = true;
    if (t_thrd.proc)
        SetLatch(&t_thrd.proc->procLatch);

    errno = saveErrno;
}

static void OgaiWorkerFreeInfo(int code, Datum arg)
{
    int idx = -1;
    ThreadId pid = gs_thread_self();

    int actualOgaiWorkers = MAX_OGAI_WORKERS;

    for (int i = 0; i < actualOgaiWorkers; i++) {
        if (t_thrd.ogailauncher_cxt.ogaiWorkerShmem->ogai_worker_status[i].pid == pid) {
            idx = i;
            t_thrd.ogailauncher_cxt.ogaiWorkerShmem->ogai_worker_status[i].pid = INVALID_PID;
            t_thrd.ogailauncher_cxt.ogaiWorkerShmem->ogai_worker_status[i].dbid = InvalidOid;
            t_thrd.ogailauncher_cxt.ogaiWorkerShmem->ogai_worker_status[i].createdbTime = (TimestampTz)0;
            break;
        }
    }

    pg_atomic_sub_fetch_u32(&t_thrd.ogailauncher_cxt.ogaiWorkerShmem->active_ogai_workers, 1);
    return;
}

static void OgaiWorkerGetWork(OgaiWorkInfo ogaiwork)
{
    errno_t rc = memcpy_s(ogaiwork, sizeof(OgaiWorkInfoData), t_thrd.ogailauncher_cxt.ogaiWorkerShmem->createdb_request,
        sizeof(OgaiWorkInfoData));
    securec_check(rc, "\0", "\0");
}


bool IsOgaiWorkerProcess(void)
{
    return t_thrd.role == OGAI_WORKER;
}

NON_EXEC_STATIC void OgaiWorkerMain()
{
    OgaiWorkInfoData ogaiwork;
    bool databaseExists = false;
    int actualOgaiWorkers = MAX_OGAI_WORKERS;
    bool ogaiVectorProcessorInited = false;
    sigjmp_buf localSigjmpBuf;

    /* we are a postmaster subprocess now */
    IsUnderPostmaster = true;
    t_thrd.role = OGAI_WORKER;

    /* reset t_thrd.proc_cxt.MyProcPid */
    t_thrd.proc_cxt.MyProcPid = gs_thread_self();

    /* record Start Time for logging */
    t_thrd.proc_cxt.MyStartTime = time(NULL);

    t_thrd.proc_cxt.MyProgName = "OgaiWorker";

    init_ps_display("ogai worker process", "", "", "");
    ereport(LOG, (errmsg("OgaiWorker: started")));

    SetProcessingMode(InitProcessing);

    /*
    * Set up signal handlers.  We operate on databases much like a regular
    * backend, so we use the same signal handling.  See equivalent code in
    * tcop/postgres.c.
    */
    gspqsignal(SIGHUP, OgaiworkerSighupHandler);
    gspqsignal(SIGINT, StatementCancelHandler);
    gspqsignal(SIGTERM, OgaiworkerSigtermHandler);

    gspqsignal(SIGQUIT, quickdie);
    gspqsignal(SIGALRM, handle_sig_alarm);

    gspqsignal(SIGPIPE, SIG_IGN);
    gspqsignal(SIGUSR1, procsignal_sigusr1_handler);
    gspqsignal(SIGUSR2, OgaiworkerSigusr2Handler);
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

    t_thrd.mem_cxt.msg_mem_cxt = AllocSetContextCreate(t_thrd.top_mem_cxt,
        "MessageContext",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);

    t_thrd.mem_cxt.mask_password_mem_cxt = AllocSetContextCreate(t_thrd.top_mem_cxt,
        "MaskPasswordCtx",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);

    InitVecFuncMap();

    (void)MemoryContextSwitchTo(t_thrd.mem_cxt.msg_mem_cxt);

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
        if (t_thrd.worker_sig_flags.got_SIGTERM) {
            goto shutdown;
        }

        /*
        * Sleep at least 1 second after any error.  We don't want to be
        * filling the error logs as fast as we can.
        */
        pg_usleep(1000000L);
    }

    /* Let the UndoLauncher know we have picked up the job and that we're active. */
    pg_atomic_add_fetch_u32(&t_thrd.ogailauncher_cxt.ogaiWorkerShmem->active_ogai_workers, 1);

    on_shmem_exit(OgaiWorkerFreeInfo, 0);

    /* Get the work from the shared memory */
    OgaiWorkerGetWork(&ogaiwork);
    
    for (int i = 0; i < actualOgaiWorkers; i++) {
        if (t_thrd.ogailauncher_cxt.ogaiWorkerShmem->ogai_worker_status[i].dbid == ogaiwork.dbid) {
            t_thrd.ogailauncher_cxt.ogaiWorkerShmem->ogai_worker_status[i].pid = gs_thread_self();
            t_thrd.ogailauncher_cxt.ogaiWorkerShmem->ogai_worker_status[i].createdbTime = GetCurrentTimestamp();
            break;
        }
    }

    Assert(ogaiwork.dbid != InvalidOid);

    t_thrd.proc_cxt.PostInit->SetDatabaseAndUser(NULL, ogaiwork.dbid, NULL);
    databaseExists = t_thrd.proc_cxt.PostInit->InitOgaiWorker();

    SetProcessingMode(NormalProcessing);
    pgstat_report_appname("OgaiWorker");
    pgstat_report_activity(STATE_IDLE, NULL);

    ogaiVectorProcessorInited = OgaiVectorProcessorInit();
    if (!ogaiVectorProcessorInited) {
        ereport(ERROR, (errmsg("OgaiWorker: Vector processor initialization failed; worker thread exiting")));
        goto shutdown;
    }

    while (!t_thrd.worker_sig_flags.got_SIGTERM) {
        HeapTuple tuple = GetDatabaseTupleByOid(ogaiwork.dbid);
        if (!HeapTupleIsValid(tuple)) {
            databaseExists = false;
        } else {
            heap_freetuple(tuple);
            databaseExists = true;
        }
        if (!databaseExists) {
            goto shutdown;
        }

        OgaiParallelVectorize();

        pg_usleep(SCAN_INTERVAL);
    }

shutdown:
    ereport(LOG, (errmsg("OgaiWorker: shutting down")));
    proc_exit(0);
}
