/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2021, openGauss Contributors
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
 * job_scheduler.cpp
 *     Function for start JobScheduler thread, scan the pg_job table periodically,
 *		and execute the job's procedure which has expired.
 *
 * IDENTIFICATION
 *    src/gausskernel/process/job/job_scheduler.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <sys/time.h>
#include "lib/dllist.h"
#include "access/heapam.h"
#include "access/reloptions.h"
#include "access/tableam.h"
#include "access/transam.h"
#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/namespace.h"
#include "catalog/pg_database.h"
#include "catalog/pg_authid.h"
#include "commands/dbcommands.h"
#include "commands/user.h"
#include "commands/vacuum.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "pgxc/pgxc.h"
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
#include "catalog/pg_job.h"
#include "job/job_shmem.h"
#include "job/job_scheduler.h"
#include "gssignal/gs_signal.h"

/* the minimum allowed time between two awakenings of the launcher */
#define MIN_JOB_SCHEDULE_SLEEPTIME 100  /* milliseconds */
#define MILLISECOND_PER_SECOND 1000000L /* sleep 1s when encounter with error */
#define MILLISECOND_JOB 1000
#define JOB_QUEUE_INTERVAL 1 /* the interval for check pg_job */
#define UNKNOW_PID ((ThreadId)(-1))

/*****************************************************************************
 *					 PRIVATE STRUCTURE DEFINE
 ****************************************************************************/
#define DLIsHead(list, elem) (DLGetHead(list) == (elem))
#define DLIsTail(list, elem) (DLGetTail(list) == (elem))
#define DLIsEmpty(list) ((list) == NULL || (DLGetHead(list) == NULL && DLGetTail(list) == NULL))

typedef struct Dlelem* DlelemPtr;
static void DLInsertByOrder(Dllist* l, Dlelem* e, int (*Comparator)(const void*, const void*));

/*****************************************************************************
 *					 PRIVATE FUNCTION DEFINE
 ****************************************************************************/
static void jobschd_sighup_handler(SIGNAL_ARGS);
static void jobschd_sigusr2_handler(SIGNAL_ARGS);
static void jobschd_sigterm_handler(SIGNAL_ARGS);
static void SchedulerDetermineSleep(bool canlaunch, struct timeval* nap);
static void ScanExpireJobs();
static int JobComparator(const void* a, const void* b);
static void ActivateWorker();
static void check_jobinfo();

/*****************************************************************************
 *					 JOB SCHEDULER IMPLEMENTS CODE : PRIVATE
 ****************************************************************************/
/*
 * Description: Main loop for the job scheduler process.
 *
 * Parameters:
 *	@in argc: the number of args.
 *	@in argv: detail info for each args.
 * Returns: void
 */
NON_EXEC_STATIC void JobScheduleMain()
{
    sigjmp_buf local_sigjmp_buf;
    char username[NAMEDATALEN];
    char* dbname = (char*)pstrdup(DEFAULT_DATABASE);

    /* we are a postmaster subprocess now */
    IsUnderPostmaster = true;
    t_thrd.role = JOB_SCHEDULER;

    /* reset t_thrd.proc_cxt.MyProcPid */
    t_thrd.proc_cxt.MyProcPid = gs_thread_self();

    t_thrd.proc_cxt.MyProgName = "JobScheduler";
    u_sess->attr.attr_common.application_name = pstrdup("JobScheduler");

    /* record Start Time for logging */
    t_thrd.proc_cxt.MyStartTime = time(NULL);

    /* Identify myself via ps */
    init_ps_display("job scheduler process", "", "", "");

    elog(LOG, "job scheduler started");

    SetProcessingMode(InitProcessing);

    bool isExit = IS_PGXC_COORDINATOR && IsPostmasterEnvironment;
    if (isExit) {
        /*
         * If we exit, first try and clean connections and send to
         * pooler thread does NOT exist any more, PoolerLock of LWlock is used instead.
         *
         * PoolManagerDisconnect() which is called by PGXCNodeCleanAndRelease()
         * is the last call to pooler in the openGauss thread, and PoolerLock is
         * used in PoolManagerDisconnect(), but it is called after ProcKill()
         * when openGauss thread exits.
         * ProcKill() releases any of its held LW locks. So Assert(!(proc == NULL ...))
         * will fail in LWLockAcquire() which is called by PoolManagerDisconnect().
         *
         * All exit functions in "on_shmem_exit_list" will be called before those functions
         * in "on_proc_exit_list", so move PGXCNodeCleanAndRelease() to "on_shmem_exit_list"
         * and registers it after ProcKill(), and PGXCNodeCleanAndRelease() will
         * be called before ProcKill().
         */
        on_shmem_exit(PGXCNodeCleanAndRelease, 0);
    }

    /*
     * Set up signal handlers.	We operate on databases much like a regular
     * backend, so we use the same signal handling.  See equivalent code in
     * tcop/postgres.c.
     */
    (void)gspqsignal(SIGHUP, jobschd_sighup_handler);
    (void)gspqsignal(SIGURG, print_stack);
    (void)gspqsignal(SIGINT, StatementCancelHandler);
    (void)gspqsignal(SIGTERM, jobschd_sigterm_handler);

    (void)gspqsignal(SIGQUIT, quickdie);
    (void)gspqsignal(SIGALRM, handle_sig_alarm);

    (void)gspqsignal(SIGPIPE, SIG_IGN);
    (void)gspqsignal(SIGUSR1, procsignal_sigusr1_handler);
    (void)gspqsignal(SIGUSR2, jobschd_sigusr2_handler);
    (void)gspqsignal(SIGFPE, FloatExceptionHandler);
    (void)gspqsignal(SIGCHLD, SIG_DFL);

    if (IsUnderPostmaster) {
        /* We allow SIGQUIT (quickdie) at all times */
        (void)sigdelset(&t_thrd.libpq_cxt.BlockSig, SIGQUIT);
    }

    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();

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

    /* Initialize openGauss with DEFAULT_DATABASE, since it cannot be dropped */
    t_thrd.proc_cxt.PostInit->SetDatabaseAndUser(dbname, InvalidOid, username);
    t_thrd.proc_cxt.PostInit->InitJobScheduler();

#ifdef PGXC /* PGXC_COORD */
    /*
     * Initialize key pair to be used as object id while using advisory lock
     * for backup
     */
    t_thrd.postmaster_cxt.xc_lockForBackupKey1 = Int32GetDatum(XC_LOCK_FOR_BACKUP_KEY_1);
    t_thrd.postmaster_cxt.xc_lockForBackupKey2 = Int32GetDatum(XC_LOCK_FOR_BACKUP_KEY_2);
#endif

    SetProcessingMode(NormalProcessing);

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

    /*
     * Create a memory context that we will do all our work in.  We do this so
     * that we can reset the context during error recovery and thereby avoid
     * possible memory leaks.
     */
    t_thrd.job_cxt.JobScheduleMemCxt = AllocSetContextCreate(t_thrd.top_mem_cxt,
        "Job Scheduler",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);

    t_thrd.job_cxt.ExpiredJobListCtx = AllocSetContextCreate(t_thrd.job_cxt.JobScheduleMemCxt,
        "Expired Job List",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);

    /*
     * If an exception is encountered, processing resumes here.
     *
     * This code is a stripped down version of PostgresMain error recovery.
     */
    int curTryCounter;
    int* oldTryCounter = NULL;
    if (sigsetjmp(local_sigjmp_buf, 1) != 0) {
        gstrace_tryblock_exit(true, oldTryCounter);

        /* Save error info */
        (void)MemoryContextSwitchTo(t_thrd.job_cxt.JobScheduleMemCxt);
        ErrorData* edata = CopyErrorData();

        /* since not using PG_TRY, must reset error stack by hand */
        t_thrd.log_cxt.error_context_stack = NULL;

        t_thrd.log_cxt.call_stack = NULL;

        /* Prevents interrupts while cleaning up */
        HOLD_INTERRUPTS();

        /* Forget any pending QueryCancel request */
        t_thrd.int_cxt.QueryCancelPending = false;
        (void)disable_sig_alarm(true);
        t_thrd.int_cxt.QueryCancelPending = false; /* again in case timeout occurred */

        /* Report the error to the server log */
        EmitErrorReport();

        /* Abort the current transaction in order to recover */
        AbortCurrentTransaction();

        /* release resource held by lsc */
        AtEOXact_SysDBCache(false);

        elog(LOG, "Job scheduler encounter abnormal, detail error msg: %s.", edata->message);

        /*
         * Now return to normal top-level context and clear ErrorContext for
         * next time.
         */
        (void)MemoryContextSwitchTo(t_thrd.job_cxt.JobScheduleMemCxt);
        FlushErrorState();

        /* Flush any leaked data in the top-level context */
        MemoryContextResetAndDeleteChildren(t_thrd.job_cxt.JobScheduleMemCxt);
        t_thrd.job_cxt.ExpiredJobList = NULL;
        t_thrd.job_cxt.ExpiredJobListCtx = NULL;

        t_thrd.job_cxt.ExpiredJobListCtx = AllocSetContextCreate(t_thrd.job_cxt.JobScheduleMemCxt,
            "Expired Job List",
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE);

        /* Now we can allow interrupts again */
        RESUME_INTERRUPTS();

        /*
         * Sleep at least 1 second after any error.  We don't want to be
         * filling the error logs as fast as we can.
         */
        pg_usleep(MILLISECOND_PER_SECOND);
    }
    oldTryCounter = gstrace_tryblock_entry(&curTryCounter);

    /* We can now handle ereport(ERROR) */
    t_thrd.log_cxt.PG_exception_stack = &local_sigjmp_buf;
    t_thrd.job_cxt.JobScheduleShmem->jsch_pid = t_thrd.proc_cxt.MyProcPid;

    /* report this backend in the PgBackendStatus array */
    u_sess->proc_cxt.MyProcPort->SessionStartTime = GetCurrentTimestamp();
    pgstat_bestart();
    pgstat_report_appname("JobScheduler");
    pgstat_report_activity(STATE_IDLE, NULL);

    if (t_thrd.job_cxt.got_SIGTERM) {
        /* Normal exit */
        ereport(LOG, (errmsg("job scheduler is shutting down")));

        t_thrd.job_cxt.JobScheduleShmem->jsch_pid = 0;

        proc_exit(0);
    }

    /*
     * Create a resource owner to keep track of our resources (currently only
     * buffer pins).
     */
    t_thrd.utils_cxt.CurrentResourceOwner = ResourceOwnerCreate(NULL, "Job Scheduler",
        THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR));

    /* Get classified list of node Oids for syschronise th job status info. */
    exec_init_poolhandles();

    (void)MemoryContextSwitchTo(t_thrd.job_cxt.JobScheduleMemCxt);

    /*
     * Update all the jobs's job_status from 'r' to 'f',
     * may be all nodes have reseted when these jobs is under running.
     */
    check_jobinfo();

    /* Main loop */
    for (;;) {
        /* close xlog file fd if any */
        CloseXlogFilesAtThreadExit();
        struct timeval nap;
        TimestampTz current_time = 0;
        bool can_launch = false;
        int ret;
        ResourceOwner save = t_thrd.utils_cxt.CurrentResourceOwner;
        int4 canceled_job_id = -1;

        /* calculate sleep time, we'd like to sleep before the first launch of a child process */
        can_launch =
            t_thrd.job_cxt.JobScheduleShmem->jsch_freeWorkers != NULL && !DLIsEmpty(t_thrd.job_cxt.ExpiredJobList);
        SchedulerDetermineSleep(can_launch, &nap);

        /*
         * Wait until naptime expires or we get some type of signal (all the
         * signal handlers will wake us by calling SetLatch).
         */
        ret = WaitLatch(&t_thrd.proc->procLatch,
            WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
            (nap.tv_sec * 1000L) + (nap.tv_usec / 1000L));

        ResetLatch(&t_thrd.proc->procLatch);

        /* Process sinval catchup interrupts that happened while sleeping */
        ProcessCatchupInterrupt();

        /*
         * Emergency bailout if postmaster has died.  This is to avoid the
         * necessity for manual cleanup of all postmaster children.
         */
        if ((unsigned int)ret & WL_POSTMASTER_DEATH) {
            elog(LOG, "Job scheduler shutting down with exit code 1");
            proc_exit(1);
        }

        /* the normal shutdown case */
        if (t_thrd.job_cxt.got_SIGTERM)
            break;

        pgstat_report_activity(STATE_RUNNING, NULL);
        if (t_thrd.job_cxt.got_SIGHUP) {
            t_thrd.job_cxt.got_SIGHUP = false;
            (void)MemoryContextSwitchTo(t_thrd.job_cxt.JobScheduleMemCxt);
            ProcessConfigFile(PGC_SIGHUP);
        }

        /* A job worker finished, or postmaster signalled failure to start a worker */
        if (t_thrd.job_cxt.got_SIGUSR2) {
            t_thrd.job_cxt.got_SIGUSR2 = false;

            /* if postmaster fork job_worker failed, we had better to try again */
            if (t_thrd.job_cxt.JobScheduleShmem->jsch_signal[ForkJobWorkerFailed]) {
                t_thrd.job_cxt.JobScheduleShmem->jsch_signal[ForkJobWorkerFailed] = false;
                pg_usleep(MILLISECOND_PER_SECOND); /* sleep 1s */
                SendPostmasterSignal(PMSIGNAL_START_JOB_WORKER);
                continue;
            }
        }
        if (u_sess->attr.attr_sql.enable_prevent_job_task_startup) {
            /* prevent to active job worker in config file ? */
            continue;
        }
        current_time = GetCurrentTimestamp();
        LWLockAcquire(JobShmemLock, LW_SHARED);

        can_launch = (t_thrd.job_cxt.JobScheduleShmem->jsch_freeWorkers != NULL);

        if (t_thrd.job_cxt.JobScheduleShmem->jsch_startingWorker != NULL) {
            int waittime;
            JobWorkerInfo worker = t_thrd.job_cxt.JobScheduleShmem->jsch_startingWorker;

            /*
             * We can't start another job worker when another one is still
             * starting up (or failed while doing so), so just sleep for a bit
             * more; that worker will wake us up again as soon as it's ready.
             * We will only wait job_queue_interval seconds (up to a maximum
             * of 60 seconds) for this to happen however.  Note that failure
             * to connect to a particular database is not a problem here,
             * because the worker removes itself from the startingWorker
             * pointer before trying to connect.  Problems detected by the
             * postmaster (like fork() failure) are also reported and handled
             * differently.  The only problems that may cause this code to
             * fire are errors in the earlier sections of JobExecuteWorkerMain,
             * before the worker removes the JobWorkerInfo from the
             * startingWorker pointer.
             */
            waittime = JOB_QUEUE_INTERVAL * MILLISECOND_JOB;
            if (TimestampDifferenceExceeds(worker->job_launchtime, current_time, waittime)) {
                LWLockRelease(JobShmemLock);
                LWLockAcquire(JobShmemLock, LW_EXCLUSIVE);

                /*
                 * No other process can put a worker in starting mode, so if
                 * startingWorker is still INVALID after exchanging our lock,
                 * we assume it's the same one we saw above (so we don't
                 * recheck the launch time).
                 */
                if (t_thrd.job_cxt.JobScheduleShmem->jsch_startingWorker != NULL) {
                    canceled_job_id = worker->job_id;
                    worker = t_thrd.job_cxt.JobScheduleShmem->jsch_startingWorker;
                    worker->job_dboid = InvalidOid;
                    worker->job_id = 0;
                    worker->job_launchtime = 0;
                    worker->job_links.next = (SHM_QUEUE*)(t_thrd.job_cxt.JobScheduleShmem->jsch_freeWorkers);
                    t_thrd.job_cxt.JobScheduleShmem->jsch_freeWorkers = worker;
                    t_thrd.job_cxt.JobScheduleShmem->jsch_startingWorker = NULL;
                }
            } else {
                can_launch = false;
            }
        }
        LWLockRelease(JobShmemLock); /* either shared or exclusive */

        if (canceled_job_id > 0) {
            ereport(WARNING,
                (errmsg("Job worker with job id:%d took too long "
                        "time to start, so canceled it",
                    canceled_job_id)));
        }
        /* If we can't do anything, just go back to sleep */
        if (!can_launch || u_sess->attr.attr_sql.enable_prevent_job_task_startup) {
            continue;
        }

        /* Get expired job */
        if (DLIsEmpty(t_thrd.job_cxt.ExpiredJobList)) {
            ScanExpireJobs();
        }

        t_thrd.utils_cxt.CurrentResourceOwner = save;
        /* To start a new worker thread for execute job. */
        ActivateWorker();
    }

    /* Normal exit */
    ereport(LOG, (errmsg("job scheduler is shutting down")));

    t_thrd.job_cxt.JobScheduleShmem->jsch_pid = 0;

    proc_exit(0);
}

/*
 * Description: Receive SIGHUP and set flag to re-read config file at next convenient time.
 *
 * Parameters:
 *	@in SIGNAL_ARGS: the args of signal.
 * Returns: void
 */
static void jobschd_sighup_handler(SIGNAL_ARGS)
{
    int save_errno = errno;

    t_thrd.job_cxt.got_SIGHUP = true;
    if (t_thrd.proc) {
        SetLatch(&t_thrd.proc->procLatch);
    }

    errno = save_errno;
}

/*
 * Description: Receive SIGUSR2, a worker is up and running, or just finished, or failed to fork.
 *
 * Parameters:
 *	@in SIGNAL_ARGS: the args of signal.
 * Returns: void
 */
static void jobschd_sigusr2_handler(SIGNAL_ARGS)
{
    int save_errno = errno;
    write_stderr("Job scheduler received sigusr2 when job worker startup failed.");

    t_thrd.job_cxt.got_SIGUSR2 = true;
    if (t_thrd.proc) {
        SetLatch(&t_thrd.proc->procLatch);
    }

    errno = save_errno;
}

/*
 * Description: Receive SIGTERM and time to die.
 *
 * Parameters:
 *	@in SIGNAL_ARGS: the args of signal.
 * Returns: void
 */
static void jobschd_sigterm_handler(SIGNAL_ARGS)
{
    t_thrd.job_cxt.got_SIGTERM = true;
    die(postgres_signal_arg);
}

/*
 * Description: Calculate sleep time, we'd like to sleep before the first launch of a child process.
 *
 * Parameters:
 *	@in canlaunch: there is free node in share memory for start a job worker.
 *	@in nap: time for sleep
 * Returns: void
 */
static void SchedulerDetermineSleep(bool canlaunch, struct timeval* nap)
{
    if (!canlaunch) {
        nap->tv_sec = JOB_QUEUE_INTERVAL;
        nap->tv_usec = 0;
    } else {
        /* Sleep time should ensure the job scheduler send signal to pm to start jobworker. */
        nap->tv_sec = 0;
        nap->tv_usec = MIN_JOB_SCHEDULE_SLEEPTIME * 1000; /* 0.1s */
    }
}

/*
 * Description: Insert a job element to queue order by last_start_date desc.
 *
 * Parameters:
 *	@in list: the queue of job worker
 *	@in newElem: job element for insert
 *	@in *Comparator: compare function
 * Returns: void
 */
void DLInsertByOrder(Dllist* list, Dlelem* newElem, int (*Comparator)(const void* a, const void* b))
{
    DlelemPtr elem = DLGetHead(list);

    if (NULL == elem) {
        DLAddHead(list, newElem);
        return;
    }

    while (elem != NULL) {
        if (DLIsHead(list, elem) && Comparator(elem->dle_val, newElem->dle_val) < 0) {
            DLAddHead(list, newElem);
            break;
        }

        if (DLIsTail(list, elem) && Comparator(elem->dle_val, newElem->dle_val) > 0) {
            DLAddTail(list, newElem);
            break;
        }

        /* Add new element next to current element. */
        if (Comparator(elem->dle_val, newElem->dle_val) > 0 &&
            Comparator(elem->dle_next->dle_val, newElem->dle_val) < 0) {
            newElem->dle_prev = elem;
            newElem->dle_next = elem->dle_next;
            elem->dle_next->dle_prev = newElem;
            elem->dle_next = newElem;
            break;
        }

        elem = DLGetSucc(elem);
    }
}

#define JOB_WORKER_RUNNING 2
#define JOB_WORKER_STARTING 1
#define JOB_WORKER_INACTIVE 0

static int GetJobStatus(int4 jobid)
{
    SHM_QUEUE* queue = NULL;
    SHM_QUEUE* nextPtr = NULL;

    LWLockAcquire(JobShmemLock, LW_SHARED);

    queue = &t_thrd.job_cxt.JobScheduleShmem->jsch_runningWorkers;
    nextPtr = queue;
    do {
        JobWorkerInfo worker = (JobWorkerInfo)nextPtr;

        if (worker->job_id == jobid) {
            /* job is executing */
            LWLockRelease(JobShmemLock);
            return JOB_WORKER_RUNNING;
        }
        nextPtr = nextPtr->next;
    } while (nextPtr != queue);

    if (t_thrd.job_cxt.JobScheduleShmem->jsch_startingWorker &&
        t_thrd.job_cxt.JobScheduleShmem->jsch_startingWorker->job_id == jobid) {
        /* job is ready to execute */
        LWLockRelease(JobShmemLock);
        return JOB_WORKER_STARTING;
    }

    LWLockRelease(JobShmemLock);
    return JOB_WORKER_INACTIVE;
}

static inline bool IsExecuteOnCurrentNode(const char* executeNodeName)
{
    if (strcmp(executeNodeName, g_instance.attr.attr_common.PGXCNodeName) == 0)
        return true;

    if (strcmp(executeNodeName, PGJOB_TYPE_ALL) == 0)
        return true;

    if (IS_PGXC_COORDINATOR) {
        if (strcmp(executeNodeName, PGJOB_TYPE_ALL_CN) == 0) {
            return true;
        } else if (strcmp(executeNodeName, PGJOB_TYPE_CCN) == 0) {
            return is_pgxc_central_nodename(g_instance.attr.attr_common.PGXCNodeName);
        } else {
            return false;
        }
    }

    if (IS_PGXC_DATANODE && strcmp(executeNodeName, PGJOB_TYPE_ALL_DN) == 0)
        return true;

    return false;
}

/*
 * @brief SkipSchedulerJob
 *  Skip process DBE_SCHEDULER jobs, contains various checks.
 *  1. whether the job is enabled
 *  2. whether the job is expired(end_date < current timestamp OR end_date is NULL)
 * @param values    pg_job attr values
 * @param nulls     pg_job attr nulls
 * @return true     skip current job
 * @return false    do not skip current job
 */
static bool SkipSchedulerJob(Datum *values, bool *nulls, Timestamp curtime)
{
    Assert(values != NULL);
    Assert(nulls != NULL);
    /* do not handle non-scheduler jobs */
    if (nulls[Anum_pg_job_job_name]) {
        return false;
    }

    /* expired job, need to drop even it is disabled */
    if (DatumGetBool(DirectFunctionCall2(timestamp_ge, curtime, values[Anum_pg_job_end_date - 1]))) {
        return false;
    }

    /* disabled jobs */
    if (!nulls[Anum_pg_job_enable - 1] && !DatumGetBool(values[Anum_pg_job_enable - 1])) {
        return true; /* skip here to avoid further overhead */
    }

    return false;
}

/*
 * Description: Find expire jobs and insert to job queue for execute.
 *
 * Returns: void
 */
static void ScanExpireJobs()
{
    Relation pg_job_tbl = NULL;
    TableScanDesc scan = NULL;
    HeapTuple tuple = NULL;
    MemoryContext oldCtx = NULL;
    int jobStatus = JOB_WORKER_INACTIVE;
    Datum curtime = TimestampGetDatum(GetCurrentTimestamp());

    StartTransactionCommand();

    pg_job_tbl = heap_open(PgJobRelationId, AccessShareLock);
    scan = tableam_scan_begin(pg_job_tbl, SnapshotNow, 0, NULL);

    MemoryContextReset(t_thrd.job_cxt.ExpiredJobListCtx);
    oldCtx = MemoryContextSwitchTo(t_thrd.job_cxt.ExpiredJobListCtx);
    /* Build a new job list if it is null. */
    t_thrd.job_cxt.ExpiredJobList = DLNewList();
    while (HeapTupleIsValid(tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection))) {
        Form_pg_job pg_job = (Form_pg_job)GETSTRUCT(tuple);
        Datum values[Natts_pg_job];
        bool nulls[Natts_pg_job];
        char status = pg_job->job_status;
        int64 jobID = pg_job->job_id;

        get_job_values(jobID, tuple, pg_job_tbl, values, nulls);

        /* dbms schedule creates a job but dont enable it */
        if (SkipSchedulerJob(values, nulls, curtime)) {
            continue;
        }

#ifdef ENABLE_MULTIPLE_NODES
        /* handle cases - ALL_NODE/ALL_CN/ALL_DN/CCN specific node */
        if (!IsExecuteOnCurrentNode(pg_job->node_name.data)) {
            continue;
        }
#endif

        Datum cur_job_start_time = DirectFunctionCall1(timestamp_timestamptz, values[Anum_pg_job_next_run_date - 1]);
        if (false == DatumGetBool(DirectFunctionCall2(timestamp_gt, curtime, cur_job_start_time))) {
            /* skip since it doesnot reach book time */
            continue;
        }

        jobStatus = GetJobStatus(jobID);
        if (PGJOB_RUN_STATUS == status) {
            if (JOB_WORKER_INACTIVE != jobStatus) {
                /* skip since job is active */
                continue;
            }

            /* ready to execute the job since it is not on executing */
        } else if (PGJOB_ABORT_STATUS == status) {
            /* skip since the job is broken */
            continue;
        } else {
            /* ready to execute the job */
            Assert(PGJOB_FAIL_STATUS == pg_job->job_status || PGJOB_SUCC_STATUS == pg_job->job_status);

            if (JOB_WORKER_RUNNING == jobStatus) {
                /*
                 * 1. skip long time job check since job will be finished soon
                 * 2. skip do the job since job is running
                 */
                continue;
            } else if (JOB_WORKER_STARTING == jobStatus) {
                ereport(WARNING, (errmsg("[job id %ld] worker is in risk of startup timeout", jobID)));
                /* skip since job woker is starting */
                continue;
            } else {
                Assert(JOB_WORKER_INACTIVE == jobStatus);
                /* ready to execute the job since it is not on executing */
            }
        }

        Oid dboid = get_database_oid(NameStr(pg_job->dbname), true);
        if (!OidIsValid(dboid)) {
            /* skip since the database of job does not exist */
            ereport(LOG, (errcode(ERRCODE_UNDEFINED_DATABASE),
                        errmsg("database \"%s\" of job %ld does not exist", NameStr(pg_job->dbname), jobID)));
            continue;
        }

        JobInfo jobInfo = (JobInfoData*)palloc0(sizeof(JobInfoData));
        jobInfo->job_id = jobID;
        jobInfo->job_oid = HeapTupleGetOid(tuple);
        jobInfo->job_dboid = dboid;
        jobInfo->log_user = pg_job->log_user;
        jobInfo->node_name = pg_job->node_name;
        jobInfo->last_start_date =
            (nulls[Anum_pg_job_last_start_date - 1] ? 0 : values[Anum_pg_job_last_start_date - 1]);
        DLInsertByOrder(t_thrd.job_cxt.ExpiredJobList, DLNewElem(jobInfo), JobComparator);
    }

    (void)MemoryContextSwitchTo(oldCtx);
    tableam_scan_end(scan);
    heap_close(pg_job_tbl, AccessShareLock);

    CommitTransactionCommand();
}

/*
 * Description: Compare with last_start_date and decide the smaller will insert previes.
 *
 * Parameters:
 *	@in baseOne: base job
 *	@in newOne: new job
 * Returns: int
 */
static int JobComparator(const void* baseOne, const void* newOne)
{
    if (((const JobInfo)newOne)->last_start_date <= ((const JobInfo)baseOne)->last_start_date) {
        return -1;
    } else {
        return 1;
    }
}

/*
 * Description: Send SIGUSR2 to postmaster and start a new job worker.
 *
 * Returns: void
 */
static void ActivateWorker()
{
    JobWorkerInfo worker = NULL;
    JobInfo jobInfo = NULL;
    DlelemPtr head_job = NULL;

    if (DLIsEmpty(t_thrd.job_cxt.ExpiredJobList)) {
        /* return immediately, after a period, main loop will fetch jobs again */
        return;
    }

    /* return quickly when there are no free job workers */
    LWLockAcquire(JobShmemLock, LW_SHARED);
    worker = t_thrd.job_cxt.JobScheduleShmem->jsch_freeWorkers;
    if (NULL == worker) {
        LWLockRelease(JobShmemLock);
        ereport(LOG, (errmsg("skip to activate job since no free job workers.")));
        return;
    }
    LWLockRelease(JobShmemLock);

    /* remove and return the head job from ExpiredJobList */
    head_job = DLRemHead(t_thrd.job_cxt.ExpiredJobList);
    if (NULL == head_job || NULL == head_job->dle_val) {
        /*
         * just throw an error if ExpiredJobList is invalid., and execution
         * environment of the scheduler will be reset in function
         * JobScheduleMain
         */
        ereport(ERROR,
            ((errcode(ERRCODE_INVALID_STATUS),
                errmsg("refuse to activate a job since illegal element in ExpiredJobList."))));
    }
    jobInfo = (JobInfo)(head_job->dle_val);

    LWLockAcquire(JobShmemLock, LW_EXCLUSIVE);

    /* Get a worker from freelist, and start it */
    worker = t_thrd.job_cxt.JobScheduleShmem->jsch_freeWorkers;
    if (NULL == worker) {
        LWLockRelease(JobShmemLock);
        /* log error, and proc exit */
        ereport(FATAL, (errmsg("no free slot when start job worker")));
        return;
    }

    t_thrd.job_cxt.JobScheduleShmem->jsch_freeWorkers = (JobWorkerInfo)worker->job_links.next;

    worker->job_dboid = jobInfo->job_dboid;
    worker->job_id = jobInfo->job_id;
    worker->job_oid = jobInfo->job_oid;
    worker->username = jobInfo->log_user;
    worker->job_launchtime = GetCurrentTimestamp();
    worker->job_worker_pid = UNKNOW_PID;

    t_thrd.job_cxt.JobScheduleShmem->jsch_startingWorker = worker;

    LWLockRelease(JobShmemLock);

    /* Tell postmaster start a new job worker. */
    SendPostmasterSignal(PMSIGNAL_START_JOB_WORKER);
    DLFreeElem(head_job);

    elog(LOG, "Job scheduler send signal to postmaster to start job worker, jobid=%d.", worker->job_id);
}

/*
 * Description: Check job's status is 'r' and update to 'f' when start job scheduler thread.
 *
 * Returns: void
 */
static void check_jobinfo()
{
    ResourceOwner save = t_thrd.utils_cxt.CurrentResourceOwner;

    StartTransactionCommand();
    (void)GetTransactionSnapshot();

    PG_TRY();
    {
        /* Check if have job which status is 'r', and update job_status as 'f'. */
        update_run_job_to_fail();
        CommitTransactionCommand();
    }
    PG_CATCH();
    {
        FlushErrorState();
        elog(LOG, "Check job info failed");
        AbortCurrentTransaction();
    }
    PG_END_TRY();

    t_thrd.utils_cxt.CurrentResourceOwner = save;
}

/*
 * Description: Shared memory size.
 *
 * Returns: Size
 */
Size JobInfoShmemSize(void)
{
    Size size;

    /* Need the fixed struct and the array of JobWorkerInfoData */
    size = sizeof(JobScheduleShmemStruct);
    size = MAXALIGN(size);
    size = add_size(size, mul_size(g_instance.attr.attr_sql.job_queue_processes, sizeof(JobWorkerInfoData)));
    return size;
}

/*
 * Description: Init shared memory.
 *
 * Returns: void
 */
void JobInfoShmemInit(void)
{
    bool found = false;
    t_thrd.job_cxt.JobScheduleShmem =
        (JobScheduleShmemStruct*)ShmemInitStruct("Job Scheduler Data", JobInfoShmemSize(), &found);

    if (!IsUnderPostmaster) {
        JobWorkerInfo worker;

        AssertEreport(!found, MOD_EXECUTOR, "");

        t_thrd.job_cxt.JobScheduleShmem->jsch_pid = 0;
        t_thrd.job_cxt.JobScheduleShmem->jsch_freeWorkers = NULL;
        SHMQueueInit(&t_thrd.job_cxt.JobScheduleShmem->jsch_runningWorkers);
        t_thrd.job_cxt.JobScheduleShmem->jsch_startingWorker = NULL;

        worker = (JobWorkerInfo)((char*)t_thrd.job_cxt.JobScheduleShmem + MAXALIGN(sizeof(JobScheduleShmemStruct)));

        /* Create new freeworker queue. */
        for (int i = 0; i < g_instance.attr.attr_sql.job_queue_processes; ++i) {
            worker[i].job_links.next = (SHM_QUEUE*)(t_thrd.job_cxt.JobScheduleShmem->jsch_freeWorkers);
            t_thrd.job_cxt.JobScheduleShmem->jsch_freeWorkers = &worker[i];
        }
    } else {
        AssertEreport(found, MOD_EXECUTOR, "");
    }
}

/*
 * Description: return true if the thread is job scheduler.
 *
 * Returns: bool
 */
bool IsJobSchedulerProcess(void)
{
    return t_thrd.role == JOB_SCHEDULER;
}

/*
 * RecordForkJobWorkerFailed: Called from postmaster when a worker could not be forked.
 *
 * Returns: void
 */
void RecordForkJobWorkerFailed(void)
{
    t_thrd.job_cxt.JobScheduleShmem->jsch_signal[ForkJobWorkerFailed] = true;
}
