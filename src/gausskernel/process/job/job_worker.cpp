/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
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
 * -------------------------------------------------------------------------
 *
 * job_worker.cpp
 *    Function for start JobWorker thread, and execute current job.
 *
 * IDENTIFICATION
 *    src/gausskernel/process/job/job_worker.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <sys/time.h>
#ifndef WIN32
#include <sys/prctl.h>
#endif
#include "lib/dllist.h"
#include "access/heapam.h"
#include "access/reloptions.h"
#include "access/transam.h"
#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/namespace.h"
#include "catalog/pg_database.h"
#include "commands/dbcommands.h"
#include "commands/vacuum.h"
#include "distributelayer/streamMain.h"
#include "gssignal/gs_signal.h"
#include "libpq/libpq.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "pgxc/pgxcnode.h"
#include "postmaster/autovacuum.h"
#include "postmaster/fork_process.h"
#include "postmaster/postmaster.h"
#include "storage/buf/bufmgr.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "storage/sinvaladt.h"
#include "tcop/tcopprot.h"
#include "utils/fmgroids.h"
#include "utils/globalplancore.h"
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
#include "utils/builtins.h"
#include "catalog/pg_job.h"
#include "catalog/pg_job_proc.h"
#include "job/job_shmem.h"
#include "job/job_worker.h"
#include "instruments/gs_stack.h"
#include "executor/executor.h"

/*****************************************************************************
 *					 PRIVATE FIELD DEFINE
 ****************************************************************************/
#define UNKNOW_PID ((ThreadId)(-1))

/*****************************************************************************
 *					 PRIVATE FUNCTION DEFINE
 ****************************************************************************/
static void SetupSignalHook(void);
static void FreeJobWorkerInfo(int code, Datum arg);

/*****************************************************************************
 *					 JOB WORKER IMPLEMENTS CODE : PRIVATE
 ****************************************************************************/
/*
 * Description: Return true if the thread is job worker.
 *
 * Returns: bool
 */
bool IsJobWorkerProcess(void)
{
    return t_thrd.role == JOB_WORKER;
}

/*
 * Description: Register signal process for job worker.
 *
 * Returns: void
 */
static void SetupSignalHook(void)
{
    (void)gspqsignal(SIGURG, print_stack);
    (void)gspqsignal(SIGHUP, SIG_IGN);
    (void)gspqsignal(SIGQUIT, quickdie);
    (void)gspqsignal(SIGTERM, die);
    (void)gspqsignal(SIGINT, StatementCancelHandler); /* cancel current query */
    (void)gspqsignal(SIGALRM, handle_sig_alarm);      /* timeout conditions */
    (void)gspqsignal(SIGPIPE, SIG_IGN);
    (void)gspqsignal(SIGUSR1, procsignal_sigusr1_handler);
    (void)gspqsignal(SIGUSR2, SIG_IGN);
    (void)gspqsignal(SIGFPE, FloatExceptionHandler);
    (void)gspqsignal(SIGCHLD, SIG_DFL);
}

/*
 * Description: Free job worker info when thread exit.
 *
 * Returns: void
 */
static void FreeJobWorkerInfo(int code, Datum arg)
{
    if (t_thrd.job_cxt.MyWorkerInfo != NULL) {
        (void)LWLockAcquire(JobShmemLock, LW_EXCLUSIVE);

        SHMQueueDelete(&t_thrd.job_cxt.MyWorkerInfo->job_links);
        t_thrd.job_cxt.MyWorkerInfo->job_links.next = (SHM_QUEUE*)t_thrd.job_cxt.JobScheduleShmem->jsch_freeWorkers;
        t_thrd.job_cxt.MyWorkerInfo->job_dboid = InvalidOid;
        t_thrd.job_cxt.MyWorkerInfo->job_id = InvalidOid;
        t_thrd.job_cxt.MyWorkerInfo->job_launchtime = 0;
        t_thrd.job_cxt.MyWorkerInfo->job_worker_pid = UNKNOW_PID;
        t_thrd.job_cxt.JobScheduleShmem->jsch_freeWorkers = t_thrd.job_cxt.MyWorkerInfo;
        t_thrd.job_cxt.MyWorkerInfo = NULL;

        LWLockRelease(JobShmemLock);
    }
}

/*
 * Description: Main loop for the job worker process.
 *
 * Parameters:
 *	@in argc: the number of args.
 *	@in argv: detail info for each args.
 * Returns: void
 */
void JobExecuteWorkerMain()
{
    sigjmp_buf local_sigjmp_buf;
    Oid dboid = InvalidOid;
    int4 job_id = -1;
    char* username = NULL;
    MemoryContext oldcontext = NULL;

    /* we are a postmaster subprocess now */
    IsUnderPostmaster = true;
    t_thrd.role = JOB_WORKER;

    /* reset t_thrd.proc_cxt.MyProcPid */
    t_thrd.proc_cxt.MyProcPid = gs_thread_self();

    t_thrd.proc_cxt.MyProgName = "JobExecuteWorker";

    /* record Start Time for logging */
    t_thrd.proc_cxt.MyStartTime = time(NULL);

    /* Identify myself via ps */
    init_ps_display("Job worker process", "", "", "");

    /* set processing mode */
    SetProcessingMode(InitProcessing);

    /* setup signal process hook */
    SetupSignalHook();

    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();

    /* Early initialization */
    BaseInit();

#ifndef EXEC_BACKEND
    InitProcess();
#endif

    /*
     * Create the memory context we will use in the main loop.
     *
     * t_thrd.mem_cxt.msg_mem_cxt is reset once per iteration of the main loop, ie, upon
     * completion of processing of each command message from the client.
     */
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

#ifndef ENABLE_MULTIPLE_NODES
    /* forbid smp in job worker thread */
    AutoDopControl dopControl;
    dopControl.CloseSmp();
#endif

    /* If an exception is encountered, processing resumes here. */
    int curTryCounter;
    int* oldTryCounter = NULL;
    if (sigsetjmp(local_sigjmp_buf, 1) != 0) {
        gstrace_tryblock_exit(true, oldTryCounter);

        /* Prevents interrupts while cleaning up */
        HOLD_INTERRUPTS();

        /* Report the error to the server log */
        EmitErrorReport();

        if (job_id > 0) {
            ereport(LOG, (errmsg("job worker with job id %d shutdown abnormaly", job_id)));
        }

        (void)MemoryContextSwitchTo(t_thrd.mem_cxt.msg_mem_cxt);
        FlushErrorState();

        /* Flush any leaked data in the top-level context */
        MemoryContextResetAndDeleteChildren(t_thrd.mem_cxt.msg_mem_cxt);

        /* release resource held by lsc */
        AtEOXact_SysDBCache(false);

        LWLockReleaseAll();
        if (t_thrd.utils_cxt.CurrentResourceOwner) {
            ResourceOwnerRelease(t_thrd.utils_cxt.CurrentResourceOwner, RESOURCE_RELEASE_BEFORE_LOCKS, false, true);
        }

        /*
         * process exit. Note that because we called InitProcess, a
         * callback was registered to do ProcKill, which will clean up
         * necessary state.
         */
        proc_exit(0);
    }
    oldTryCounter = gstrace_tryblock_entry(&curTryCounter);

    /* We can now handle ereport(ERROR) */
    t_thrd.log_cxt.PG_exception_stack = &local_sigjmp_buf;

    /* We need to allow SIGINT, etc during the initial transaction */
    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();

    /* Get job info from shared memory */
    LWLockAcquire(JobShmemLock, LW_EXCLUSIVE);
    if (t_thrd.job_cxt.JobScheduleShmem->jsch_startingWorker != NULL) {
        t_thrd.job_cxt.MyWorkerInfo = t_thrd.job_cxt.JobScheduleShmem->jsch_startingWorker;
        t_thrd.job_cxt.MyWorkerInfo->job_worker_pid = t_thrd.proc_cxt.MyProcPid;
        dboid = t_thrd.job_cxt.MyWorkerInfo->job_dboid;
        job_id = t_thrd.job_cxt.MyWorkerInfo->job_id;

        username = pstrdup(NameStr(t_thrd.job_cxt.MyWorkerInfo->username));

        SHMQueueInsertBefore(
            &t_thrd.job_cxt.JobScheduleShmem->jsch_runningWorkers, &t_thrd.job_cxt.MyWorkerInfo->job_links);

        /*
         * Remove from the "starting" pointer, so that the launcher can start
         * a new worker if required
         */
        t_thrd.job_cxt.JobScheduleShmem->jsch_startingWorker = NULL;

        LWLockRelease(JobShmemLock);

        /* setup shared memory hook */
        on_shmem_exit(FreeJobWorkerInfo, 0);
        on_shmem_exit(PGXCNodeCleanAndRelease, 0);
        ereport(LOG, (errmsg("job worker started with job id: %d", job_id)));
    } else {
        LWLockRelease(JobShmemLock);

        /* no worker entry for me, go away */
        ereport(WARNING, (errmsg("job worker started wihtout worker entry")));
        proc_exit(0);
    }

    /* user_name and database_name in u_sess->proc_cxt.MyProcPort is under t_thrd.top_mem_cxt */
    oldcontext = MemoryContextSwitchTo(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR));
    if (u_sess->proc_cxt.MyProcPort->database_name)
        pfree_ext(u_sess->proc_cxt.MyProcPort->database_name);
    if (u_sess->proc_cxt.MyProcPort->user_name)
        pfree_ext(u_sess->proc_cxt.MyProcPort->user_name);
    u_sess->proc_cxt.MyProcPort->database_name = (char*)palloc0(NAMEDATALEN);
    u_sess->proc_cxt.MyProcPort->user_name = pstrdup(username);
    (void)MemoryContextSwitchTo(oldcontext);

    u_sess->proc_cxt.MyProcPort->SessionStartTime = GetCurrentTimestamp();

    /* General initialization. */
    t_thrd.proc_cxt.PostInit->SetDatabaseAndUser(NULL, dboid, username);
    t_thrd.proc_cxt.PostInit->InitJobExecuteWorker();
    t_thrd.proc_cxt.PostInit->GetDatabaseName(u_sess->proc_cxt.MyProcPort->database_name);

#ifdef PGXC /* PGXC_COORD */
    /*
     * Initialize key pair to be used as object id while using advisory lock
     * for backup
     */
    t_thrd.postmaster_cxt.xc_lockForBackupKey1 = Int32GetDatum(XC_LOCK_FOR_BACKUP_KEY_1);
    t_thrd.postmaster_cxt.xc_lockForBackupKey2 = Int32GetDatum(XC_LOCK_FOR_BACKUP_KEY_2);
#endif

    /* report this backend in the PgBackendStatus array */
    pgstat_report_appname("JobWorker");
    pgstat_report_activity(STATE_IDLE, NULL);
    pgstat_report_jobid(job_id); /* Record job id into beentry */

    /* It should enter running state for ExecRemoteUtility. */
    pgstat_report_activity(STATE_RUNNING, NULL);

    /* Reset some flag related to stream. */
    ResetSessionEnv();

    t_thrd.role = JOB_WORKER;

    t_thrd.wlm_cxt.thread_node_group = &g_instance.wlm_cxt->MyDefaultNodeGroup;  // initialize the default value
    t_thrd.wlm_cxt.thread_climgr = &t_thrd.wlm_cxt.thread_node_group->climgr;
    t_thrd.wlm_cxt.thread_srvmgr = &t_thrd.wlm_cxt.thread_node_group->srvmgr;

    /*
     * Create a resource owner to keep track of our resources (currently only
     * buffer pins).
     */
    t_thrd.utils_cxt.CurrentResourceOwner = ResourceOwnerCreate(NULL, "Job Worker",
        THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR));

    /* Get classified list of node Oids for syschronise th job info. */
    exec_init_poolhandles();
    (void)MemoryContextSwitchTo(t_thrd.mem_cxt.msg_mem_cxt);

    SetProcessingMode(NormalProcessing);
#if (!defined(ENABLE_MULTIPLE_NODES)) && (!defined(ENABLE_PRIVATEGAUSS))
    LoadSqlPlugin();
#endif

    /* execute job procedure */
    elog(LOG, "Job is running, worker: %lu, job id: %d", t_thrd.proc_cxt.MyProcPid, job_id);
    execute_job(job_id);
    elog(LOG, "Job worker is shutdown normal.");

    MemoryContextResetAndDeleteChildren(t_thrd.mem_cxt.msg_mem_cxt);

    /* All done, go away */
    proc_exit(0);
}
