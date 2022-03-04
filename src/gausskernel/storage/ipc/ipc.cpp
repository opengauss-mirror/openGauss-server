/* -------------------------------------------------------------------------
 *
 * ipc.cpp
 *	  openGauss inter-process communication definitions.
 *
 * This file is misnamed, as it no longer has much of anything directly
 * to do with IPC.	The functionality here is concerned with managing
 * exit-time cleanup for either a postmaster or a backend.
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/ipc/ipc.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include <signal.h>
#include <unistd.h>
#include <sys/stat.h>

#include "miscadmin.h"
#ifdef PROFILE_PID_DIR
    #include "postmaster/autovacuum.h"
#endif
#include "storage/ipc.h"
#include "tcop/tcopprot.h"
#include "libpq/libpq-be.h"
#include "storage/smgr/fd.h"
#include "storage/latch.h"
#include "storage/procarray.h"
#include "gssignal/gs_signal.h"
#include "storage/pmsignal.h"
#include "access/gtm.h"
#include "access/dfs/dfs_am.h"
#include "access/ustore/undo/knl_uundoapi.h"
#include "workload/workload.h"
#include "postmaster/syslogger.h"
#include "executor/exec/execStream.h"
#include "postmaster/bgworker.h"
#ifndef WIN32_ONLY_COMPILER
    #include "dynloader.h"
#else
    #include "port/dynloader/win32.h"
#endif
#include "utils/plog.h"
#include "threadpool/threadpool.h"
#include "instruments/instr_user.h"
#include "utils/postinit.h"
#ifdef ENABLE_MOT
#include "storage/mot/mot_fdw.h"
#endif

#ifdef ENABLE_MEMORY_CHECK
extern "C" {
    extern void __lsan_do_leak_check();
}

#endif
/* postmaster need wait some thread in immediate shutdown */
#define NUMWAITTHREADS 1
#define WAITTIME 15

volatile unsigned int alive_threads_waitted = NUMWAITTHREADS;

extern void pq_close(int code, Datum arg);
extern void AtProcExit_Files(int code, Datum arg);
extern void audit_processlogout(int code, Datum arg);
extern void CancelAutoAnalyze();
extern void DestoryAutonomousSession(bool force);
#ifdef ENABLE_MOT
static void MOTCleanupSession(int code, Datum arg)
{
    MOTOnSessionClose();
}
#endif

static const pg_on_exit_callback on_sess_exit_list[] = {
    ShutdownPostgres,
    PGXCNodeCleanAndRelease,
    PlDebugerCleanUp,
    cleanGPCPlanProcExit,
#ifdef ENABLE_MOT
    /*
     * 1. Must come after ShutdownPostgres(), in case there is abort/rollback callback.
     * 2. Must come after PGXCNodeCleanAndRelease(), due to prepared statement cleanup
     *    (which cleans also all session JIT context objects).
     */
    MOTCleanupSession,
#endif
    pq_close,
    AtProcExit_Files,
    audit_processlogout,
    log_disconnections
};

static const int on_sess_exit_size = lengthof(on_sess_exit_list);

extern void KillGraceThreads(void);
extern void gs_set_hs_shm_data(HaShmemData* ha_shm_data);
#ifndef ENABLE_LLT
    extern void clean_ec_conn();
    extern void delete_ec_ctrl();
#endif
extern void signal_sysloger_flush();

static void sess_exit_prepare(int code);
static void PreventInterrupt();
static bool IsCalledInSessExit(pg_on_exit_callback func);

void WaitGraceThreadsExit(void)
{
    unsigned int loop;

    for (loop = 0; loop < WAITTIME; loop++) {
        if (0 != alive_threads_waitted) {
            pg_usleep(100);
        }
    }
}

/* local functions */
void proc_exit_prepare(int code);

/* ----------------------------------------------------------------
 *						exit() handling stuff
 *
 * These functions are in generally the same spirit as atexit(),
 * but provide some additional features we need --- in particular,
 * we want to register callbacks to invoke when we are disconnecting
 * from a broken shared-memory context but not exiting the postmaster.
 *
 * Callback functions can take zero, one, or two args: the first passed
 * arg is the integer exitcode, the second is the Datum supplied when
 * the callback was registered.
 * ----------------------------------------------------------------
 */
/* ----------------------------------------------------------------
 *		proc_exit
 *
 *		this function calls all the callbacks registered
 *		for it (to free resources) and then calls exit.
 *
 *		This should be the only function to call exit().
 *		-cim 2/6/90
 *
 *		Unfortunately, we can't really guarantee that add-on code
 *		obeys the rule of not calling exit() directly.	So, while
 *		this is the preferred way out of the system, we also register
 *		an atexit callback that will make sure cleanup happens.
 * ----------------------------------------------------------------
 */
void proc_exit(int code)
{
    DynamicFileList* file_scanner = NULL;

    if (t_thrd.utils_cxt.backend_reserved) {
        ereport(DEBUG2, (errmodule(MOD_MEM),
            errmsg("[BackendReservedExit] current thread role is: %d, used memory is: %d MB\n",
            t_thrd.role, t_thrd.utils_cxt.trackedMemChunks)));
    }

    audit_processlogout_unified();

    (void)pgstat_report_waitstatus(STATE_WAIT_UNDEFINED);

    (void)gs_signal_block_sigusr2();

    /* release gpc_reset_lock for fataled scheduler thread */
    if (t_thrd.role == THREADPOOL_SCHEDULER && pmState == PM_RUN &&
        ENABLE_DN_GPC && g_instance.gpc_reset_lock.__data.__owner != 0) {
        pthread_mutex_unlock(&g_instance.gpc_reset_lock);
    }

    if (t_thrd.proc_cxt.MyProcPid == PostmasterPid) {
        if (t_thrd.postmaster_cxt.redirection_done)
            ereport(DEBUG3, (errmsg("Gaussdb will exit.")));

        if (code != 0) {
            pg_usleep(1000);
        }

        if (open_join_children != false && will_shutdown == false) {
            open_join_children = false;
            KillGraceThreads();
            WaitGraceThreadsExit();
        }
    }

    /* release active statements only once while proc exiting */
    if (ENABLE_WORKLOAD_CONTROL && IsUnderPostmaster && (u_sess->wlm_cxt->parctl_state_exit == 0)) {
        u_sess->wlm_cxt->parctl_state_exit = 1;

        if (u_sess->wlm_cxt->parctl_state_control) {
            WLMProcessExiting = true;

            if (g_instance.wlm_cxt->dynamic_workload_inited) {
                if (!COMP_ACC_CLUSTER)
                    dywlm_client_max_release(&t_thrd.wlm_cxt.parctl_state);

                if (t_thrd.wlm_cxt.parctl_state.rp_reserve) {
                    if (t_thrd.wlm_cxt.parctl_state.simple == 0) {
                        dywlm_client_release(&t_thrd.wlm_cxt.parctl_state);
                    }
                } else if (!WLMIsQueryFinished())
                    dywlm_client_clean(&t_thrd.wlm_cxt.parctl_state);

                WLMHandleDywlmSimpleExcept(true);  // handle simple except
                dywlm_client_proc_release();
            } else {
                WLMParctlRelease(&t_thrd.wlm_cxt.parctl_state);

                WLMProcReleaseActiveStatement();
            }

            WLMProcessExiting = false;
        }
    }

    /* INSTR: update instr user logout counter */
    if (IsUnderPostmaster && !IsBootstrapProcessingMode() && !dummyStandbyMode && !IS_THREAD_POOL_WORKER)
        InstrUpdateUserLogCounter(false);

    if (t_thrd.proc_cxt.MyProcPid == PostmasterPid && t_thrd.postmaster_cxt.redirection_done) {
        ereport(DEBUG3, (errmsg("Gaussdb before exit prepare.")));
    }

    if (t_thrd.proc_cxt.MyPMChildSlot > 0 && IsPostmasterChildSuspect(t_thrd.proc_cxt.MyPMChildSlot))
        MarkPostmasterChildInactive();

    CloseGTM();

    /* Reet to Local vfd if we have attach it to global vfdcache */
    ResetToLocalVfdCache();

#ifndef ENABLE_LLT
    clean_ec_conn();
    delete_ec_ctrl();
#endif

    if (IS_THREAD_POOL_WORKER) {
        if (t_thrd.threadpool_cxt.worker != NULL)
            t_thrd.threadpool_cxt.worker->CleanUpSessionWithLock();
        DecreaseUserCount(u_sess->proc_cxt.MyRoleId);
    }
    RemoveFromDnHashTable();

    /* Clean up Dfs Reader stuffs */
    CleanupDfsHandlers(true);

    BgworkerListSyncQuit();

    /* Clean up Allocated descs */
    FreeAllAllocatedDescs();

    /* Clean up everything that must be cleaned up */
    proc_exit_prepare(code);

    if (u_sess->SPI_cxt.autonomous_session) {
        DestoryAutonomousSession(true);
    }

    /*
     * Protect the node group incase the ShutPostgres Callback function
     * has not been registered
     */
    StreamNodeGroup::syncQuit(STREAM_ERROR);
    StreamNodeGroup::destroy(STREAM_ERROR);

#ifdef PROFILE_PID_DIR
    {
        /*
         * If we are profiling ourself then gprof's mcleanup() is about to
         * write out a profile to ./gmon.out.  Since mcleanup() always uses a
         * fixed file name, each backend will overwrite earlier profiles. To
         * fix that, we create a separate subdirectory for each backend
         * (./gprof/pid) and 'cd' to that subdirectory before we exit() - that
         * forces mcleanup() to write each profile into its own directory.	We
         * end up with something like: $PGDATA/gprof/8829/gmon.out
         * $PGDATA/gprof/8845/gmon.out ...
         *
         * To avoid undesirable disk space bloat, autovacuum workers are
         * discriminated against: all their gmon.out files go into the same
         * subdirectory.  Without this, an installation that is "just sitting
         * there" nonetheless eats megabytes of disk space every few seconds.
         *
         * Note that we do this here instead of in an on_proc_exit() callback
         * because we want to ensure that this code executes last - we don't
         * want to interfere with any other on_proc_exit() callback.  For the
         * same reason, we do not include it in proc_exit_prepare ... so if
         * you are exiting in the "wrong way" you won't drop your profile in a
         * nice place.
         */
        char gprofDirName[32];
        errno_t rc = EOK;
        if (IsAutoVacuumWorkerProcess())
            rc = snprintf_s(gprofDirName, sizeof(gprofDirName), sizeof(gprofDirName) - 1, "gprof/avworker");
        else
            rc =
                snprintf_s(gprofDirName, sizeof(gprofDirName), sizeof(gprofDirName) - 1, "gprof/%lu", gs_thread_self());

        securec_check_ss(rc, "\0", "\0");
        (void)mkdir("gprof", S_IRWXU | S_IRWXG | S_IRWXO);
        (void)mkdir(gprofDirName, S_IRWXU | S_IRWXG | S_IRWXO);
        (void)chdir(gprofDirName);
    }
#endif

    /*
     * Thread termination does not release any application visible process resources.
     * So we will take care of them explicitly.
     */
    if (!IsPostmasterEnvironment || t_thrd.proc_cxt.MyProcPid == PostmasterPid) {
        if (t_thrd.postmaster_cxt.redirection_done)
            ereport(LOG, (errmsg("Gaussdb exit(%d)", code)));

        while (file_list != NULL) {
            file_scanner = file_list;
            file_list = file_list->next;
#ifndef ENABLE_MEMORY_CHECK
            /* 
             * in the senario of ImmediateShutdown, it is not safe to close plugin 
             * as PM thread will not wait for all children threads exist(will send SIGQUIT signal) referring to pmdie
             */
            if (g_instance.status != ImmediateShutdown) {
                (void)pg_dlclose(file_scanner->handle);
            }
#endif
            pfree((char*)file_scanner);
            file_scanner = NULL;
        }
        file_list = file_tail = NULL;

        if (u_sess->attr.attr_resource.use_workload_manager)
            gscgroup_free();

#ifdef ENABLE_MEMORY_CHECK
        ereport(LOG, (errmsg("Gaussdb memory_leak checking")));
        __lsan_do_leak_check();
#endif

        /* flush log before exit */
        signal_sysloger_flush();

        // call _exit() safer than exit() in signal handler
        //
        _exit(code);
    }

    CloseClientSocket(u_sess, true);

    ClosePipesAtThreadExit();

    CloseXlogFilesAtThreadExit();

    /* write plog before closing syslogger */
    flush_plog();

    SysLoggerClose();
    SQMCloseLogFile();
    ASPCloseLogFile();

    if (IS_THREAD_POOL_WORKER && t_thrd.threadpool_cxt.worker) {
        CurrentMemoryContext = NULL;
        t_thrd.threadpool_cxt.worker->ShutDown();
    } else if (IS_THREAD_POOL_LISTENER && t_thrd.threadpool_cxt.listener) {
        t_thrd.threadpool_cxt.listener->ResetThreadId();
    } else if (IS_THREAD_POOL_SCHEDULER && t_thrd.threadpool_cxt.scheduler) {
        t_thrd.threadpool_cxt.scheduler->SetShutDown(true);
    } else if (IS_THREAD_POOL_STREAM && t_thrd.threadpool_cxt.stream) {
        t_thrd.threadpool_cxt.stream->ShutDown();
    }

    GlobalStatsCleanupFiles();

    gs_thread_exit(code);
}

/*
 * Code shared between proc_exit and the atexit handler.  Note that in
 * normal exit through proc_exit, this will actually be called twice ...
 * but the second call will have nothing to do.
 */
void proc_exit_prepare(int code)
{
    sigset_t old_sigset;

    PreventInterrupt();

    /*
     * Also clear the error context stack, to prevent error callbacks from
     * being invoked by any elog/ereport calls made during proc_exit. Whatever
     * context they might want to offer is probably not relevant, and in any
     * case they are likely to fail outright after we've done things like
     * aborting any open transaction.  (In normal exit scenarios the context
     * stack should be empty anyway, but it might not be in the case of
     * elog(FATAL) for example.)
     */
    t_thrd.log_cxt.error_context_stack = NULL;
    /* For the same reason, reset t_thrd.postgres_cxt.debug_query_string before it's clobbered */
    t_thrd.postgres_cxt.debug_query_string = NULL;

    old_sigset = gs_signal_block_sigusr2();

    /* do our shared memory exits first */
    shmem_exit(code);

    if (t_thrd.postmaster_cxt.redirection_done)
        ereport(DEBUG3, (errmsg("proc_exit(%d): %d callbacks to make", code, t_thrd.storage_cxt.on_proc_exit_index)));

    /*
     * call all the registered callbacks.
     *
     * Note that since we decrement on_proc_exit_index each time, if a
     * callback calls ereport(ERROR) or ereport(FATAL) then it won't be
     * invoked again when control comes back here (nor will the
     * previously-completed callbacks).  So, an infinite loop should not be
     * possible.
     */
    while (--t_thrd.storage_cxt.on_proc_exit_index >= 0) {
        pg_on_exit_callback func = t_thrd.storage_cxt.on_proc_exit_list[t_thrd.storage_cxt.on_proc_exit_index].function;
        if (!IsCalledInSessExit(func)) {
            (*func)(code, t_thrd.storage_cxt.on_proc_exit_list[t_thrd.storage_cxt.on_proc_exit_index].arg);
        }
    }

    /* release all refcount and lock */
    CloseLocalSysDBCache();

    t_thrd.storage_cxt.on_proc_exit_index = 0;

    gs_signal_recover_mask(old_sigset);

    if (t_thrd.log_cxt.thd_bt_symbol) {
        free(t_thrd.log_cxt.thd_bt_symbol);
        t_thrd.log_cxt.thd_bt_symbol = NULL;
    }
}

void sess_exit(int code)
{
    (void)pgstat_report_waitstatus(STATE_WAIT_UNDEFINED);

    (void)gs_signal_block_sigusr2();

    /* release active statements only once while proc exiting */
    WLMReleaseAtThreadExit();

    /* Reet to Local vfd if we have attach it to global vfdcache */
    ResetToLocalVfdCache();

    /* Clean up everything that must be cleaned up */
    sess_exit_prepare(code);

    /*
     * Protect the node group incase the ShutPostgres Callback function
     * has not been registered
     */
    StreamNodeGroup::syncQuit(STREAM_ERROR);
    StreamNodeGroup::destroy(STREAM_ERROR);
    BgworkerListSyncQuit();
    CloseClientSocket(u_sess, true);

    CancelAutoAnalyze();

    /* INSTR: update instr user logout counter */
    if (IsUnderPostmaster && !IsBootstrapProcessingMode() && !dummyStandbyMode)
        InstrUpdateUserLogCounter(false);

    t_thrd.proc_cxt.proc_exit_inprogress = false;
    t_thrd.proc_cxt.sess_exit_inprogress = false;
    (void)gs_signal_unblock_sigusr2();
}

void sess_exit_prepare(int code)
{
    sigset_t old_sigset;

    if (!EnableLocalSysCache()) {
        closeAllVfds();
    }

    PreventInterrupt();
    HOLD_INTERRUPTS();

    t_thrd.proc_cxt.sess_exit_inprogress = true;
    old_sigset = gs_signal_block_sigusr2();

    /* FDW exit callback, used to free connections to other server, check FDW code for detail. */
    for (int i = 0; i < MAX_TYPE_FDW; i++) {
        if (u_sess->ext_fdw_ctx[i].fdwExitFunc != NULL) {
            (u_sess->ext_fdw_ctx[i].fdwExitFunc)(code, UInt32GetDatum(NULL));
        }
    }

    for (; u_sess->on_sess_exit_index < on_sess_exit_size; u_sess->on_sess_exit_index++) {
        if (EnableLocalSysCache() && on_sess_exit_list[u_sess->on_sess_exit_index] == AtProcExit_Files) {
            // we close this only on proc exit
            continue;
        }
        (*on_sess_exit_list[u_sess->on_sess_exit_index])(code, UInt32GetDatum(NULL));
    }
    
    t_thrd.storage_cxt.on_proc_exit_index = 0;
    RESUME_INTERRUPTS();
    gs_signal_recover_mask(old_sigset);
}

/* ------------------
 * Run all of the on_shmem_exit routines --- but don't actually exit.
 * This is used by the postmaster to re-initialize shared memory and
 * semaphores after a backend dies horribly.
 * ------------------
 */
void shmem_exit(int code)
{
    if (!IsUnderPostmaster) {
        gs_set_hs_shm_data(NULL);
    }

    if (t_thrd.postmaster_cxt.redirection_done)
        ereport(DEBUG3, (errmsg("shmem_exit(%d): %d callbacks to make", code, t_thrd.storage_cxt.on_shmem_exit_index)));

    /*
     * call all the registered callbacks.
     *
     * As with proc_exit(), we remove each callback from the list before
     * calling it, to avoid infinite loop in case of error.
     */
    while (--t_thrd.storage_cxt.on_shmem_exit_index >= 0) {
        pg_on_exit_callback func =
            t_thrd.storage_cxt.on_shmem_exit_list[t_thrd.storage_cxt.on_shmem_exit_index].function;
        if (!IsCalledInSessExit(func)) {
            (*func)(code, t_thrd.storage_cxt.on_shmem_exit_list[t_thrd.storage_cxt.on_shmem_exit_index].arg);
        }
    }

    t_thrd.storage_cxt.on_shmem_exit_index = 0;
}

/* ----------------------------------------------------------------
 *		on_proc_exit
 *
 *		this function adds a callback function to the list of
 *		functions invoked by proc_exit().	-cim 2/6/90
 * ----------------------------------------------------------------
 */
void on_proc_exit(pg_on_exit_callback function, Datum arg)
{
    if (t_thrd.storage_cxt.on_proc_exit_index >= MAX_ON_EXITS)
        ereport(FATAL, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED), errmsg_internal("out of on_proc_exit slots")));
    
    /* reregister when cache miss on thread pool mode */
    if ((function == AtProcExit_Files || function == smgrshutdown) && EnableLocalSysCache()) {
        for (int i = 0; i < t_thrd.storage_cxt.on_proc_exit_index; i++) {
            if (t_thrd.storage_cxt.on_proc_exit_list[i].function == function) {
                Assert(t_thrd.storage_cxt.on_proc_exit_list[i].arg == arg);
                return;
            }
        }
    }

    t_thrd.storage_cxt.on_proc_exit_list[t_thrd.storage_cxt.on_proc_exit_index].function = function;
    t_thrd.storage_cxt.on_proc_exit_list[t_thrd.storage_cxt.on_proc_exit_index].arg = arg;

    ++t_thrd.storage_cxt.on_proc_exit_index;

    if (!t_thrd.storage_cxt.atexit_callback_setup) {
        t_thrd.storage_cxt.atexit_callback_setup = true;
    }
}

/* ----------------------------------------------------------------
 *		on_shmem_exit
 *
 *		this function adds a callback function to the list of
 *		functions invoked by shmem_exit().	-cim 2/6/90
 * ----------------------------------------------------------------
 */
void on_shmem_exit(pg_on_exit_callback function, Datum arg)
{
    if (t_thrd.storage_cxt.on_shmem_exit_index >= MAX_ON_EXITS)
        ereport(FATAL, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED), errmsg_internal("out of on_shmem_exit slots")));

    t_thrd.storage_cxt.on_shmem_exit_list[t_thrd.storage_cxt.on_shmem_exit_index].function = function;
    t_thrd.storage_cxt.on_shmem_exit_list[t_thrd.storage_cxt.on_shmem_exit_index].arg = arg;

    ++t_thrd.storage_cxt.on_shmem_exit_index;

    if (!t_thrd.storage_cxt.atexit_callback_setup) {
        t_thrd.storage_cxt.atexit_callback_setup = true;
    }
}

/* ----------------------------------------------------------------
 *		cancel_shmem_exit
 *
 *		this function removes an entry, if present, from the list of
 *		functions to be invoked by shmem_exit().  For simplicity,
 *		only the latest entry can be removed.  (We could work harder
 *		but there is no need for current uses.)
 * ----------------------------------------------------------------
 */
void cancel_shmem_exit(pg_on_exit_callback function, Datum arg)
{
    if (t_thrd.storage_cxt.on_shmem_exit_index > 0 &&
        t_thrd.storage_cxt.on_shmem_exit_list[t_thrd.storage_cxt.on_shmem_exit_index - 1].function == function &&
        t_thrd.storage_cxt.on_shmem_exit_list[t_thrd.storage_cxt.on_shmem_exit_index - 1].arg == arg)
        --t_thrd.storage_cxt.on_shmem_exit_index;
}

/* ----------------------------------------------------------------
 *		on_exit_reset
 *
 *		this function clears all on_proc_exit() and on_shmem_exit()
 *		registered functions.  This is used just after forking a backend,
 *		so that the backend doesn't believe it should call the postmaster's
 *		on-exit routines when it exits...
 * ----------------------------------------------------------------
 */
void on_exit_reset(void)
{
    t_thrd.storage_cxt.on_shmem_exit_index = 0;
    t_thrd.storage_cxt.on_proc_exit_index = 0;
}

/* cancel the shmem exit function */
void cancelShmemExit(pg_on_exit_callback function, Datum arg)
{
    int i = 0;
    int j = 0;
    i = t_thrd.storage_cxt.on_shmem_exit_index;

    while (i > 0) {
        if (t_thrd.storage_cxt.on_shmem_exit_list[i - 1].function == function) {
            for (j = i - 1; j != t_thrd.storage_cxt.on_shmem_exit_index - 1; ++j) {
                t_thrd.storage_cxt.on_shmem_exit_list[j].function =
                    t_thrd.storage_cxt.on_shmem_exit_list[j + 1].function;
                t_thrd.storage_cxt.on_shmem_exit_list[j].arg = t_thrd.storage_cxt.on_shmem_exit_list[j + 1].arg;
            }

            --t_thrd.storage_cxt.on_shmem_exit_index;
            break;
        }

        --i;
    }
}

void CloseClientSocket(knl_session_context* sess, bool closesock)
{
    if (sess == NULL) {
        return;
    }

    /*
     * Socket shall have been closed in pq_close () in normal code path. However, if
     * the thread exit before it registered exit call backs, like errors in handling
     * GUC configuration, it will leave socket open. So double check it here.
     */
    pgsocket tmpsock = -1;
    if (sess->proc_cxt.MyProcPort != NULL && closesock) {
        /* if gs_sock is NULL, just clean gs_poll hash table. */
        pfree_ext(sess->proc_cxt.MyProcPort->msgLog);
        gsocket* curGsock = &sess->proc_cxt.MyProcPort->gs_sock;
        gs_close_gsocket(curGsock);
    }
    if (t_thrd.postmaster_cxt.KeepSocketOpenForStream == false && (sess->proc_cxt.MyProcPort != NULL) &&
        sess->proc_cxt.MyProcPort->sock > 0) {
        tmpsock = sess->proc_cxt.MyProcPort->sock;
        if (closesock) {
            sess->proc_cxt.MyProcPort->sock = -1;
            closesocket(tmpsock);
        } else {
            shutdown(tmpsock, SHUT_RDWR);
        }
    }
}

void PreventInterrupt()
{
    /*
     * Once we set this flag, we are committed to exit.  Any ereport() will
     * NOT send control back to the main loop, but right back here.
     */
    t_thrd.proc_cxt.proc_exit_inprogress = true;

    /*
     * Forget any pending cancel or die requests; we're doing our best to
     * close up shop already.  Note that the signal handlers will not set
     * these flags again, now that proc_exit_inprogress is set.
     */
    InterruptPending = false;
    t_thrd.int_cxt.ProcDiePending = false;
    t_thrd.int_cxt.QueryCancelPending = false;
    t_thrd.int_cxt.PoolValidateCancelPending = false;
    /* And le's just make *sure* we'tre  not interrupted ... */
    t_thrd.int_cxt.ImmediateInterruptOK = false;
    t_thrd.int_cxt.CritSectionCount = 0;
}

bool IsCalledInSessExit(pg_on_exit_callback func)
{
    if (t_thrd.proc_cxt.sess_exit_inprogress) {
        for (int i = 0; i <= u_sess->on_sess_exit_index; i++) {
            if (func == on_sess_exit_list[i])
                return true;
        }
    }
    return false;
}

