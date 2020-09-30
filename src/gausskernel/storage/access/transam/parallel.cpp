/* -------------------------------------------------------------------------
 *
 * parallel.c
 * 	  Infrastructure for launching parallel workers
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 * 	  src/backend/access/transam/parallel.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/nbtree.h"
#include "access/parallel.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/pg_enum.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "commands/async.h"
#include "executor/execParallel.h"
#include "libpq/libpq.h"
#include "libpq/pqsignal.h"
#include "libpq/pqformat.h"
#include "libpq/pqmq.h"
#include "miscadmin.h"
#include "optimizer/planner.h"
#include "pgstat.h"
#include "storage/ipc.h"
#include "storage/predicate.h"
#include "storage/sinval.h"
#include "storage/spin.h"
#include "tcop/tcopprot.h"
#include "utils/combocid.h"
#include "utils/guc.h"
#include "utils/inval.h"
#include "utils/memutils.h"
#include "utils/relmapper.h"
#include "utils/snapmgr.h"
#include "utils/typcache.h"
#ifdef __USE_NUMA
#include <numa.h>
#endif

/*
 * We don't want to waste a lot of memory on an error queue which, most of
 * the time, will process only a handful of small messages.  However, it is
 * desirable to make it large enough that a typical ErrorResponse can be sent
 * without blocking.  That way, a worker that errors out can write the whole
 * message into the queue and terminate without waiting for the user backend.
 */
#define PARALLEL_ERROR_QUEUE_SIZE 16384

/*
 * List of internal parallel worker entry points.  We need this for
 * reasons explained in LookupParallelWorkerFunction(), below.
 */
static const struct {
    const char *fn_name;
    parallel_worker_main_type fn_addr;
}   InternalParallelWorkers[] = {
    {
        "ParallelQueryMain", ParallelQueryMain
    }
};

/* Private functions. */
static void HandleParallelMessage(ParallelContext *pcxt, int i, StringInfo msg);
static void WaitForParallelWorkersToExit(ParallelContext *pcxt);
static parallel_worker_main_type LookupParallelWorkerFunction(const char *libraryname, const char *funcname);
static void ParallelWorkerShutdown(int code, Datum arg);
#ifdef __USE_NUMA
static bool SaveCpuAffinity(cpu_set_t **cpuset);
static void GetCurrentNumaNode(ParallelInfoContext *pcxt);
#endif

/*
 * Establish a new parallel context.  This should be done after entering
 * parallel mode, and (unless there is an error) the context should be
 * destroyed before exiting the current subtransaction.
 */
ParallelContext *CreateParallelContext(const char *library_name, const char *function_name, int nworkers)
{
    /* It is unsafe to create a parallel context if not in parallel mode. */
    Assert(IsInParallelMode());

    /* Number of workers should be non-negative. */
    Assert(nworkers >= 0);

    /* We might be running in a short-lived memory context. */
    MemoryContext oldcontext = MemoryContextSwitchTo(u_sess->top_transaction_mem_cxt);

    /* Initialize a new ParallelContext. */
    ParallelContext *pcxt = (ParallelContext *)palloc0(sizeof(ParallelContext));
    pcxt->subid = GetCurrentSubTransactionId();
    pcxt->nworkers = nworkers;
    pcxt->library_name = pstrdup(library_name);
    pcxt->function_name = pstrdup(function_name);
    pcxt->error_context_stack = t_thrd.log_cxt.error_context_stack;
    dlist_push_head(&t_thrd.bgworker_cxt.pcxt_list, &pcxt->node);

    /* Restore previous memory context. */
    (void)MemoryContextSwitchTo(oldcontext);

    return pcxt;
}

/*
 * Establish the dynamic shared memory segment for a parallel context and
 * copy state and other bookkeeping information that will be needed by
 * parallel workers into it.
 */
void InitializeParallelDSM(ParallelContext *pcxt, const void *snap)
{
    int i;
    Snapshot active_snapshot = GetActiveSnapshot();

    /*
     * Create DSM and initialize with new table of contents.  But if the user
     * didn't request any workers, then don't bother creating a dynamic shared
     * memory segment; instead, just use backend-private memory.
     *
     * Also, if we can't create a dynamic shared memory segment because the
     * maximum number of segments have already been created, then fall back to
     * backend-private memory, and plan not to use any workers.  We hope this
     * won't happen very often, but it's better to abandon the use of
     * parallelism than to fail outright.
     */
    pcxt->seg = dsm_create();

    knl_u_parallel_context *cxt = (knl_u_parallel_context *)pcxt->seg;
    MemoryContext oldcontext = MemoryContextSwitchTo(cxt->memCtx);

    /* Initialize fixed-size state in shared memory. */
    cxt->pwCtx->database_id = u_sess->proc_cxt.MyDatabaseId;
    cxt->pwCtx->authenticated_user_id = GetAuthenticatedUserId();
    cxt->pwCtx->outer_user_id = GetCurrentRoleId();
    cxt->pwCtx->is_superuser = u_sess->attr.attr_common.session_auth_is_superuser;
    GetUserIdAndSecContext(&cxt->pwCtx->current_user_id, &cxt->pwCtx->sec_context);
    GetTempNamespaceState(&cxt->pwCtx->temp_namespace_id, &cxt->pwCtx->temp_toast_namespace_id);
    cxt->pwCtx->parallel_master_pgproc = t_thrd.proc;
    cxt->pwCtx->parallel_master_pid = t_thrd.proc_cxt.MyProcPid;
    cxt->pwCtx->parallel_master_backend_id = t_thrd.proc_cxt.MyBackendId;
    cxt->pwCtx->xact_ts = GetCurrentTransactionStartTimestamp();
    cxt->pwCtx->stmt_ts = GetCurrentStatementStartTimestamp();
    SpinLockInit(&cxt->pwCtx->mutex);
    cxt->pwCtx->last_xlog_end = 0;

    /* We can skip the rest of this if we're not budgeting for any workers. */
    if (pcxt->nworkers > 0) {
        /* Serialize combo CID state. */
        SerializeComboCIDState(cxt->pwCtx);

        /* Serialize active snapshot. */
        Size asnaplen = EstimateSnapshotSpace(active_snapshot);

        cxt->pwCtx->asnapspace = (char *)palloc0(asnaplen);
        cxt->pwCtx->asnapspace_len = asnaplen;
        SerializeSnapshot(active_snapshot, cxt->pwCtx->asnapspace, asnaplen);

        /* Save transaction snapshot */
        cxt->pwCtx->xmin = ((Snapshot)snap)->xmin;
        cxt->pwCtx->xmax = ((Snapshot)snap)->xmax;
        cxt->pwCtx->timeline = ((Snapshot)snap)->timeline;
        cxt->pwCtx->snapshotcsn  = ((Snapshot)snap)->snapshotcsn;
        cxt->pwCtx->curcid  = ((Snapshot)snap)->curcid;

        Size searchPathLen = strlen(u_sess->attr.attr_common.namespace_search_path);
        cxt->pwCtx->namespace_search_path = (char *)palloc(searchPathLen + 1);
        int rc = strcpy_s(cxt->pwCtx->namespace_search_path, searchPathLen + 1,
            u_sess->attr.attr_common.namespace_search_path);
        securec_check_c(rc, "", "");

        /* Serialize transaction state. */
        SerializeTransactionState(cxt->pwCtx);

        /* Serialize relmapper state. */
        cxt->pwCtx->active_shared_updates = u_sess->relmap_cxt.active_shared_updates;
        cxt->pwCtx->active_local_updates = u_sess->relmap_cxt.active_local_updates;

        /* Allocate space for worker information. */
        pcxt->worker = (ParallelWorkerInfo *)palloc0(sizeof(ParallelWorkerInfo) * pcxt->nworkers);

#ifdef __USE_NUMA
        GetCurrentNumaNode(cxt->pwCtx);
#endif
        /*
         * Establish error queues in dynamic shared memory.
         *
         * These queues should be used only for transmitting ErrorResponse,
         * NoticeResponse, and NotifyResponse protocol messages.  Tuple data
         * should be transmitted via separate (possibly larger?) queues.
         */
        cxt->pwCtx->errorQueue = (char *)palloc0(mul_size(pcxt->nworkers, PARALLEL_ERROR_QUEUE_SIZE));
        for (i = 0; i < pcxt->nworkers; ++i) {
            shm_mq *mq =
                shm_mq_create(cxt->pwCtx->errorQueue + i * PARALLEL_ERROR_QUEUE_SIZE, PARALLEL_ERROR_QUEUE_SIZE);
            shm_mq_set_receiver(mq, t_thrd.proc);
            pcxt->worker[i].error_mqh = shm_mq_attach(mq, pcxt->seg, NULL);
        }

        /*
         * Serialize entrypoint information.  It's unsafe to pass function
         * pointers across processes, as the function pointer may be different
         * in each process in EXEC_BACKEND builds, so we always pass library
         * and function name.  (We use library name "postgres" for functions
         * in the core backend.)
         */
        Size lnamelen = strlen(pcxt->library_name);
        cxt->pwCtx->library_name = (char *)palloc(lnamelen + 1);
        rc = strcpy_s(cxt->pwCtx->library_name, lnamelen + 1, pcxt->library_name);
        securec_check_c(rc, "", "");

        Size fnamelen = strlen(pcxt->function_name);
        cxt->pwCtx->function_name = (char *)palloc(fnamelen + 1);
        rc = strcpy_s(cxt->pwCtx->function_name, fnamelen + 1, pcxt->function_name);
        securec_check_c(rc, "", "");
    }

    /* Restore previous memory context. */
    (void)MemoryContextSwitchTo(oldcontext);
}

/*
 * Reinitialize the dynamic shared memory segment for a parallel context such
 * that we could launch workers for it again.
 */
void ReinitializeParallelDSM(ParallelContext *pcxt)
{
    /* Wait for any old workers to exit. */
    if (pcxt->nworkers_launched > 0) {
        WaitForParallelWorkersToFinish(pcxt);
        WaitForParallelWorkersToExit(pcxt);
        pcxt->nworkers_launched = 0;
        if (pcxt->known_attached_workers) {
            pfree(pcxt->known_attached_workers);
            pcxt->known_attached_workers = NULL;
            pcxt->nknown_attached_workers = 0;
        }
    }

    knl_u_parallel_context *cxt = (knl_u_parallel_context *)pcxt->seg;

    /* Reset a few bits of fixed parallel state to a clean state. */
    cxt->pwCtx->last_xlog_end = 0;

    /* Recreate error queues (if they exist). */
    if (pcxt->nworkers > 0) {
        for (int i = 0; i < pcxt->nworkers; ++i) {
            char *start = cxt->pwCtx->errorQueue + i * PARALLEL_ERROR_QUEUE_SIZE;
            shm_mq *mq = shm_mq_create(start, PARALLEL_ERROR_QUEUE_SIZE);
            shm_mq_set_receiver(mq, t_thrd.proc);
            pcxt->worker[i].error_mqh = shm_mq_attach(mq, pcxt->seg, NULL);
        }
    }
}

/*
 * Launch parallel workers.
 */
void LaunchParallelWorkers(ParallelContext *pcxt)
{
    BackgroundWorker worker;
    int i;
    bool any_registrations_failed = false;

    /* Skip this if we have no workers. */
    if (pcxt->nworkers == 0)
        return;

    /* If we do have workers, we'd better have a DSM segment. */
    Assert(pcxt->seg != NULL);

    /* We might be running in a short-lived memory context. */
    MemoryContext oldcontext = MemoryContextSwitchTo(u_sess->top_transaction_mem_cxt);

    /* Configure a worker. */
    int rc = memset_s(&worker, sizeof(worker), 0, sizeof(worker));
    securec_check(rc, "", "");
    rc = sprintf_s(worker.bgw_name, BGW_MAXLEN, "parallel worker for PID %lu", t_thrd.proc_cxt.MyProcPid);
    securec_check_ss(rc, "", "");
    rc = sprintf_s(worker.bgw_type, BGW_MAXLEN, "parallel worker");
    securec_check_ss(rc, "", "");
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION | BGWORKER_CLASS_PARALLEL;
    worker.bgw_start_time = BgWorkerStart_ConsistentState;
    worker.bgw_restart_time = BGW_NEVER_RESTART;
    rc = strcpy_s(worker.bgw_library_name, BGW_MAXLEN, "postgres");
    securec_check(rc, "", "");
    rc = strcpy_s(worker.bgw_function_name, BGW_MAXLEN, "ParallelWorkerMain");
    securec_check(rc, "", "");
    worker.bgw_main_arg = PointerGetDatum(pcxt->seg);
    worker.bgw_notify_pid = t_thrd.proc_cxt.MyProcPid;
    worker.bgw_parallel_context = pcxt->seg;

    /*
     * Start workers.
     *
     * The caller must be able to tolerate ending up with fewer workers than
     * expected, so there is no need to throw an error here if registration
     * fails.  It wouldn't help much anyway, because registering the worker in
     * no way guarantees that it will start up and initialize successfully.
     */
    for (i = 0; i < pcxt->nworkers; ++i) {
        rc = memcpy_s(worker.bgw_extra, BGW_EXTRALEN, &i, sizeof(int));
        securec_check(rc, "", "");
        if (!any_registrations_failed && RegisterDynamicBackgroundWorker(&worker, &pcxt->worker[i].bgwhandle)) {
            shm_mq_set_handle(pcxt->worker[i].error_mqh, pcxt->worker[i].bgwhandle);
            pcxt->nworkers_launched++;
        } else {
            /*
             * If we weren't able to register the worker, then we've bumped up
             * against the max_worker_processes limit, and future
             * registrations will probably fail too, so arrange to skip them.
             * But we still have to execute this code for the remaining slots
             * to make sure that we forget about the error queues we budgeted
             * for those workers.  Otherwise, we'll wait for them to start,
             * but they never will.
             */
            any_registrations_failed = true;
            pcxt->worker[i].bgwhandle = NULL;
            shm_mq_detach(pcxt->worker[i].error_mqh);
            pcxt->worker[i].error_mqh = NULL;
        }
    }

    /*
     * Now that nworkers_launched has taken its final value, we can initialize
     * known_attached_workers.
     */
    if (pcxt->nworkers_launched > 0) {
        pcxt->known_attached_workers = (bool *)palloc0(sizeof(bool) * pcxt->nworkers_launched);
        pcxt->nknown_attached_workers = 0;
    }

    /* Restore previous memory context. */
    (void)MemoryContextSwitchTo(oldcontext);
}

/*
 * Wait for all workers to attach to their error queues, and throw an error if
 * any worker fails to do this.
 *
 * Callers can assume that if this function returns successfully, then the
 * number of workers given by pcxt->nworkers_launched have initialized and
 * attached to their error queues.  Whether or not these workers are guaranteed
 * to still be running depends on what code the caller asked them to run;
 * this function does not guarantee that they have not exited.  However, it
 * does guarantee that any workers which exited must have done so cleanly and
 * after successfully performing the work with which they were tasked.
 *
 * If this function is not called, then some of the workers that were launched
 * may not have been started due to a fork() failure, or may have exited during
 * early startup prior to attaching to the error queue, so nworkers_launched
 * cannot be viewed as completely reliable.  It will never be less than the
 * number of workers which actually started, but it might be more.  Any workers
 * that failed to start will still be discovered by
 * WaitForParallelWorkersToFinish and an error will be thrown at that time,
 * provided that function is eventually reached.
 *
 * In general, the leader process should do as much work as possible before
 * calling this function.  fork() failures and other early-startup failures
 * are very uncommon, and having the leader sit idle when it could be doing
 * useful work is undesirable.  However, if the leader needs to wait for
 * all of its workers or for a specific worker, it may want to call this
 * function before doing so.  If not, it must make some other provision for
 * the failure-to-start case, lest it wait forever.  On the other hand, a
 * leader which never waits for a worker that might not be started yet, or
 * at least never does so prior to WaitForParallelWorkersToFinish(), need not
 * call this function at all.
 */
void WaitForParallelWorkersToAttach(ParallelContext *pcxt)
{
    int i;

    /* Skip this if we have no launched workers. */
    if (pcxt->nworkers_launched == 0)
        return;

    for (;;) {
        /*
         * This will process any parallel messages that are pending and it may
         * also throw an error propagated from a worker.
         */
        CHECK_FOR_INTERRUPTS();

        for (i = 0; i < pcxt->nworkers_launched; ++i) {
            shm_mq *mq = NULL;
            int rc;
            ThreadId pid;

            if (pcxt->known_attached_workers[i])
                continue;

            /*
             * If error_mqh is NULL, then the worker has already exited
             * cleanly.
             */
            if (pcxt->worker[i].error_mqh == NULL) {
                pcxt->known_attached_workers[i] = true;
                ++pcxt->nknown_attached_workers;
                continue;
            }

            BgwHandleStatus status = GetBackgroundWorkerPid(pcxt->worker[i].bgwhandle, &pid);
            if (status == BGWH_STARTED) {
                /* Has the worker attached to the error queue? */
                mq = shm_mq_get_queue(pcxt->worker[i].error_mqh);
                if (shm_mq_get_sender(mq) != NULL) {
                    /* Yes, so it is known to be attached. */
                    pcxt->known_attached_workers[i] = true;
                    ++pcxt->nknown_attached_workers;
                }
            } else if (status == BGWH_STOPPED) {
                /*
                 * If the worker stopped without attaching to the error queue,
                 * throw an error.
                 */
                mq = shm_mq_get_queue(pcxt->worker[i].error_mqh);
                if (shm_mq_get_sender(mq) == NULL)
                    ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("parallel worker failed to initialize"),
                        errhint("More details may be available in the server log.")));

                pcxt->known_attached_workers[i] = true;
                ++pcxt->nknown_attached_workers;
            } else {
                /*
                 * Worker not yet started, so we must wait.  The postmaster
                 * will notify us if the worker's state changes.  Our latch
                 * might also get set for some other reason, but if so we'll
                 * just end up waiting for the same worker again.
                 */
                rc = WaitLatch(&t_thrd.proc->procLatch, WL_LATCH_SET, -1);
                if (rc & WL_LATCH_SET) {
                    ResetLatch(&t_thrd.proc->procLatch);
                }
            }
        }

        /* If all workers are known to have started, we're done. */
        if (pcxt->nknown_attached_workers >= pcxt->nworkers_launched) {
            Assert(pcxt->nknown_attached_workers == pcxt->nworkers_launched);
            break;
        }
    }
}

/*
 * Wait for all workers to finish computing.
 *
 * Even if the parallel operation seems to have completed successfully, it's
 * important to call this function afterwards.  We must not miss any errors
 * the workers may have thrown during the parallel operation, or any that they
 * may yet throw while shutting down.
 *
 * Also, we want to update our notion of XactLastRecEnd based on worker
 * feedback.
 */
void WaitForParallelWorkersToFinish(ParallelContext *pcxt)
{
    for (;;) {
        bool anyone_alive = false;
        int nfinished = 0;
        int i;

        /*
         * This will process any parallel messages that are pending, which may
         * change the outcome of the loop that follows.  It may also throw an
         * error propagated from a worker.
         */
        CHECK_FOR_INTERRUPTS();

        for (i = 0; i < pcxt->nworkers_launched; ++i) {
            /*
             * If error_mqh is NULL, then the worker has already exited
             * cleanly.  If we have received a message through error_mqh from
             * the worker, we know it started up cleanly, and therefore we're
             * certain to be notified when it exits.
             */
            if (pcxt->worker[i].error_mqh == NULL)
                ++nfinished;
            else if (pcxt->known_attached_workers[i]) {
                anyone_alive = true;
                break;
            }
        }

        if (!anyone_alive) {
            /* If all workers are known to have finished, we're done. */
            if (nfinished >= pcxt->nworkers_launched) {
                Assert(nfinished == pcxt->nworkers_launched);
                break;
            }

            /*
             * We didn't detect any living workers, but not all workers are
             * known to have exited cleanly.  Either not all workers have
             * launched yet, or maybe some of them failed to start or
             * terminated abnormally.
             */
            for (i = 0; i < pcxt->nworkers_launched; ++i) {
                ThreadId pid;

                /*
                 * If the worker is BGWH_NOT_YET_STARTED or BGWH_STARTED, we
                 * should just keep waiting.  If it is BGWH_STOPPED, then
                 * further investigation is needed.
                 */
                if (pcxt->worker[i].error_mqh == NULL || pcxt->worker[i].bgwhandle == NULL ||
                    GetBackgroundWorkerPid(pcxt->worker[i].bgwhandle, &pid) != BGWH_STOPPED)
                    continue;

                /*
                 * Check whether the worker ended up stopped without ever
                 * attaching to the error queue.  If so, the postmaster was
                 * unable to fork the worker or it exited without initializing
                 * properly.  We must throw an error, since the caller may
                 * have been expecting the worker to do some work before
                 * exiting.
                 */
                shm_mq *mq = shm_mq_get_queue(pcxt->worker[i].error_mqh);
                if (shm_mq_get_sender(mq) == NULL)
                    ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("parallel worker failed to initialize"),
                        errhint("More details may be available in the server log.")));

                /*
                 * The worker is stopped, but is attached to the error queue.
                 * Unless there's a bug somewhere, this will only happen when
                 * the worker writes messages and terminates after the
                 * CHECK_FOR_INTERRUPTS() near the top of this function and
                 * before the call to GetBackgroundWorkerPid().  In that case,
                 * or latch should have been set as well and the right things
                 * will happen on the next pass through the loop.
                 */
            }
        }

        (void)WaitLatch(&t_thrd.proc->procLatch, WL_LATCH_SET, -1);
        ResetLatch(&t_thrd.proc->procLatch);
    }

    knl_u_parallel_context *cxt = (knl_u_parallel_context *)pcxt->seg;
    if (cxt->pwCtx->last_xlog_end > t_thrd.xlog_cxt.XactLastRecEnd)
        t_thrd.xlog_cxt.XactLastRecEnd = cxt->pwCtx->last_xlog_end;
}

/*
 * Wait for all workers to exit.
 *
 * This function ensures that workers have been completely shutdown.  The
 * difference between WaitForParallelWorkersToFinish and this function is
 * that former just ensures that last message sent by worker backend is
 * received by master backend whereas this ensures the complete shutdown.
 */
static void WaitForParallelWorkersToExit(ParallelContext *pcxt)
{
    /* Wait until the workers actually die. */
    for (int i = 0; i < pcxt->nworkers_launched; ++i) {
        if (pcxt->worker == NULL || pcxt->worker[i].bgwhandle == NULL) {
            continue;
        }

        BgwHandleStatus status = WaitForBackgroundWorkerShutdown(pcxt->worker[i].bgwhandle);
        /*
         * If the postmaster kicked the bucket, we have no chance of cleaning
         * up safely -- we won't be able to tell when our workers are actually
         * dead.  This doesn't necessitate a PANIC since they will all abort
         * eventually, but we can't safely continue this session.
         */
        if (status == BGWH_POSTMASTER_DIED)
            ereport(FATAL,
                (errcode(ERRCODE_ADMIN_SHUTDOWN), errmsg("postmaster exited during a parallel transaction")));

        /* Release memory. */
        pfree(pcxt->worker[i].bgwhandle);
        pcxt->worker[i].bgwhandle = NULL;
    }
}

/*
 * Destroy a parallel context.
 *
 * If expecting a clean exit, you should use WaitForParallelWorkersToFinish()
 * first, before calling this function.  When this function is invoked, any
 * remaining workers are forcibly killed; the dynamic shared memory segment
 * is unmapped; and we then wait (uninterruptibly) for the workers to exit.
 */
void DestroyParallelContext(ParallelContext *pcxt)
{
    int i;

    /*
     * Be careful about order of operations here!  We remove the parallel
     * context from the list before we do anything else; otherwise, if an
     * error occurs during a subsequent step, we might try to nuke it again
     * from AtEOXact_Parallel or AtEOSubXact_Parallel.
     */
    dlist_delete(&pcxt->node);

    /* Kill each worker in turn, and forget their error queues. */
    if (pcxt->worker != NULL) {
        for (i = 0; i < pcxt->nworkers_launched; ++i) {
            if (pcxt->worker[i].error_mqh != NULL) {
                TerminateBackgroundWorker(pcxt->worker[i].bgwhandle);

                shm_mq_detach(pcxt->worker[i].error_mqh);
                pcxt->worker[i].error_mqh = NULL;
            }
        }
    }

    /*
     * If this parallel context is actually in backend-private memory rather
     * than shared memory, free that memory instead.
     */
    if (pcxt->private_memory != NULL) {
        pfree(pcxt->private_memory);
        pcxt->private_memory = NULL;
    }

    /*
     * We can't finish transaction commit or abort until all of the workers
     * have exited.  This means, in particular, that we can't respond to
     * interrupts at this stage.
     */
    HOLD_INTERRUPTS();
    WaitForParallelWorkersToExit(pcxt);
    RESUME_INTERRUPTS();

    /* Free the worker array itself. */
    if (pcxt->worker != NULL) {
        pfree(pcxt->worker);
        pcxt->worker = NULL;
    }

    /*
     * If we have allocated a shared memory segment, detach it.  This will
     * implicitly detach the error queues, and any other shared memory queues,
     * stored there.
     */
    if (pcxt->seg != NULL) {
        dsm_detach(&(pcxt->seg));
        pcxt->seg = NULL;
    }

    /* Free memory. */
    pfree(pcxt->library_name);
    pfree(pcxt->function_name);
    pfree(pcxt);
}

/*
 * Are there any parallel contexts currently active?
 */
bool ParallelContextActive(void)
{
    return !dlist_is_empty(&t_thrd.bgworker_cxt.pcxt_list);
}

/*
 * Handle receipt of an interrupt indicating a parallel worker message.
 *
 * Note: this is called within a signal handler!  All we can do is set a flag
 * that will cause the next CHECK_FOR_INTERRUPTS() to invoke HandleParallelMessages().
 */
void HandleParallelMessageInterrupt(void)
{
    InterruptPending = true;
    t_thrd.bgworker_cxt.ParallelMessagePending = true;
    SetLatch(&t_thrd.proc->procLatch);
}

/*
 * Handle any queued protocol messages received from parallel workers.
 */
void HandleParallelMessages(void)
{
    dlist_iter iter;

    /*
     * This is invoked from ProcessInterrupts(), and since some of the
     * functions it calls contain CHECK_FOR_INTERRUPTS(), there is a potential
     * for recursive calls if more signals are received while this runs.  It's
     * unclear that recursive entry would be safe, and it doesn't seem useful
     * even if it is safe, so let's block interrupts until done.
     */
    HOLD_INTERRUPTS();

    /*
     * Moreover, CurrentMemoryContext might be pointing almost anywhere.  We
     * don't want to risk leaking data into long-lived contexts, so let's do
     * our work here in a private context that we can reset on each use.
     */
    if (t_thrd.bgworker_cxt.hpm_context == NULL) /* first time through? */
        t_thrd.bgworker_cxt.hpm_context =
            AllocSetContextCreate(TopMemoryContext, "HandleParallelMessages", ALLOCSET_DEFAULT_SIZES);
    else
        MemoryContextReset(t_thrd.bgworker_cxt.hpm_context);

    MemoryContext oldcontext = MemoryContextSwitchTo(t_thrd.bgworker_cxt.hpm_context);

    /* OK to process messages.  Reset the flag saying there are more to do. */
    t_thrd.bgworker_cxt.ParallelMessagePending = false;

    dlist_foreach(iter, &t_thrd.bgworker_cxt.pcxt_list)
    {
        ParallelContext *pcxt = dlist_container(ParallelContext, node, iter.cur);
        if (pcxt->worker == NULL)
            continue;

        for (int i = 0; i < pcxt->nworkers_launched; ++i) {
            /*
             * Read as many messages as we can from each worker, but stop when
             * either (1) the worker's error queue goes away, which can happen
             * if we receive a Terminate message from the worker; or (2) no
             * more messages can be read from the worker without blocking.
             */
            while (pcxt->worker[i].error_mqh != NULL) {
                Size nbytes;
                void *data = NULL;

                shm_mq_result res = shm_mq_receive(pcxt->worker[i].error_mqh, &nbytes, &data, true);
                if (res == SHM_MQ_WOULD_BLOCK) {
                    break;
                } else if (res == SHM_MQ_SUCCESS) {
                    StringInfoData msg;

                    initStringInfo(&msg);
                    appendBinaryStringInfo(&msg, (const char *)data, nbytes);
                    HandleParallelMessage(pcxt, i, &msg);
                    pfree(msg.data);
                } else {
                    ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("lost connection to parallel worker")));
                }
            }
        }
    }

    (void)MemoryContextSwitchTo(oldcontext);

    /* Might as well clear the context on our way out */
    MemoryContextReset(t_thrd.bgworker_cxt.hpm_context);

    RESUME_INTERRUPTS();
}

/*
 * Handle a single protocol message received from a single parallel worker.
 */
static void HandleParallelMessage(ParallelContext *pcxt, int i, StringInfo msg)
{
    if (pcxt->known_attached_workers != NULL && !pcxt->known_attached_workers[i]) {
        pcxt->known_attached_workers[i] = true;
        pcxt->nknown_attached_workers++;
    }

    char msgtype = (char)pq_getmsgbyte(msg);

    switch (msgtype) {
        case 'K': /* BackendKeyData */
        {
            ThreadId pid = pq_getmsgint64(msg);

            (void)pq_getmsgint64(msg); /* discard cancel key */
            pq_getmsgend(msg);
            pcxt->worker[i].pid = pid;
            break;
        }

        case 'E': /* ErrorResponse */
        case 'N': /* NoticeResponse */
        {
            ErrorData edata;

            /* Parse ErrorResponse or NoticeResponse. */
            pq_parse_errornotice(msg, &edata);

            /* Death of a worker isn't enough justification for suicide. */
            edata.elevel = Min(edata.elevel, ERROR);

            /*
             * If desired, add a context line to show that this is a
             * message propagated from a parallel worker.  Otherwise, it
             * can sometimes be confusing to understand what actually
             * happened.  (We don't do this in FORCE_PARALLEL_REGRESS mode
             * because it causes test-result instability depending on
             * whether a parallel worker is actually used or not.)
             */
            if (u_sess->attr.attr_sql.force_parallel_mode != FORCE_PARALLEL_REGRESS) {
                if (edata.context) {
                    /* 1 for '\0', 1 for '\n' */
                    Size len = strlen(edata.context) + strlen("parallel worker") + 2;
                    edata.context = (char *)palloc(len);
                    int rc = sprintf_s(edata.context, len, "%s\n%s", edata.context, "parallel worker");
                    securec_check_ss(rc, "", "");
                } else {
                    edata.context = pstrdup(_("parallel worker"));
                }
            }

            /*
             * Context beyond that should use the error context callbacks
             * that were in effect when the ParallelContext was created,
             * not the current ones.
             */
            ErrorContextCallback *save_error_context_stack = t_thrd.log_cxt.error_context_stack;
            t_thrd.log_cxt.error_context_stack = pcxt->error_context_stack;

            /* Rethrow error or print notice. */
            ThrowErrorData(&edata);

            /* Not an error, so restore previous context stack. */
            t_thrd.log_cxt.error_context_stack = save_error_context_stack;

            break;
        }

        case 'A': /* NotifyResponse */
        {
            /* Propagate NotifyResponse. */
            uint32 pid = pq_getmsgint(msg, 4);
            const char *channel = pq_getmsgrawstring(msg);
            const char *payload = pq_getmsgrawstring(msg);
            pq_endmessage(msg);

            NotifyMyFrontEnd(channel, payload, pid);

            break;
        }

        case 'X': /* Terminate, indicating clean exit */
        {
            shm_mq_detach(pcxt->worker[i].error_mqh);
            pcxt->worker[i].error_mqh = NULL;
            break;
        }

        default: {
            ereport(ERROR,
                (errmsg("unrecognized message type received from parallel worker: %c (message length %d bytes)",
                    msgtype, msg->len)));
        }
    }
}

/*
 * End-of-subtransaction cleanup for parallel contexts.
 *
 * Currently, it's forbidden to enter or leave a subtransaction while
 * parallel mode is in effect, so we could just blow away everything.  But
 * we may want to relax that restriction in the future, so this code
 * contemplates that there may be multiple subtransaction IDs in pcxt_list.
 */
void AtEOSubXact_Parallel(bool isCommit, SubTransactionId mySubId)
{
    while (!dlist_is_empty(&t_thrd.bgworker_cxt.pcxt_list)) {
        ParallelContext *pcxt = dlist_head_element(ParallelContext, node, &t_thrd.bgworker_cxt.pcxt_list);
        if (pcxt->subid != mySubId)
            break;
        if (isCommit)
            ereport(WARNING, (errmsg("leaked parallel context")));
        DestroyParallelContext(pcxt);
    }
}

/*
 * End-of-transaction cleanup for parallel contexts.
 */
void AtEOXact_Parallel(bool isCommit)
{
    while (!dlist_is_empty(&t_thrd.bgworker_cxt.pcxt_list)) {
        ParallelContext *pcxt = dlist_head_element(ParallelContext, node, &t_thrd.bgworker_cxt.pcxt_list);
        if (isCommit)
            ereport(WARNING, (errmsg("leaked parallel context")));
        DestroyParallelContext(pcxt);
    }
}

/*
 * Main entrypoint for parallel workers.
 */
void ParallelWorkerMain(Datum main_arg)
{
    StringInfoData msgbuf;

    knl_u_parallel_context *ctx = (knl_u_parallel_context *)DatumGetPointer(main_arg);

    /* Set flag to indicate that we're initializing a parallel worker. */
    t_thrd.bgworker_cxt.InitializingParallelWorker = true;

    /* Establish signal handlers. */
    gspqsignal(SIGTERM, die);
    BackgroundWorkerUnblockSignals();

    /* Determine and set our parallel worker number. */
    Assert(t_thrd.bgworker_cxt.ParallelWorkerNumber == -1);
    int rc = memcpy_s(&t_thrd.bgworker_cxt.ParallelWorkerNumber, sizeof(int),
        t_thrd.bgworker_cxt.my_bgworker_entry->bgw_extra, sizeof(int));
    securec_check(rc, "", "");

    char bgWorkerName[MAX_THREAD_NAME_LENGTH];
    rc = sprintf_s(bgWorkerName, MAX_THREAD_NAME_LENGTH, "BgWorker%d", t_thrd.bgworker_cxt.ParallelWorkerNumber);
    securec_check_ss(rc, "", "");
    knl_thread_set_name(bgWorkerName);

#ifdef __USE_NUMA
    if (ctx->pwCtx->numaNode != -1) {
        rc = numa_run_on_node(ctx->pwCtx->numaNode);
        if (rc != 0) {
            ereport(WARNING, (errmsg("numa_run_on_node failed, %m")));
        }
    }
#endif

    /* Set up a memory context to work in, just for cleanliness. */
    CurrentMemoryContext = AllocSetContextCreate(TopMemoryContext, "Parallel worker", ALLOCSET_DEFAULT_SIZES);

    /* Arrange to signal the leader if we exit. */
    on_shmem_exit(ParallelWorkerShutdown, (Datum)0);

    /*
     * Now we can find and attach to the error queue provided for us.  That's
     * good, because until we do that, any errors that happen here will not be
     * reported back to the process that requested that this worker be
     * launched.
     */
    char *error_queue_space = ctx->pwCtx->errorQueue;
    shm_mq *mq = (shm_mq *)(error_queue_space + t_thrd.bgworker_cxt.ParallelWorkerNumber * PARALLEL_ERROR_QUEUE_SIZE);
    shm_mq_set_sender(mq, t_thrd.proc);
    shm_mq_handle *mqh = shm_mq_attach(mq, ctx, NULL);
    pq_redirect_to_shm_mq(mqh);
    pq_set_parallel_master(ctx->pwCtx->parallel_master_pid, ctx->pwCtx->parallel_master_backend_id);

    /*
     * Send a BackendKeyData message to the process that initiated parallelism
     * so that it has access to our PID before it receives any other messages
     * from us.  Our cancel key is sent, too, since that's the way the
     * protocol message is defined, but it won't actually be used for anything
     * in this case.
     */
    pq_beginmessage(&msgbuf, 'K');
    pq_sendint64(&msgbuf, t_thrd.proc_cxt.MyProcPid);
    pq_sendint64(&msgbuf, t_thrd.proc_cxt.MyCancelKey);
    pq_endmessage(&msgbuf);

    /*
    * Hooray! Primary initialization is complete.  Now, we need to set up our
    * backend-local state to match the original backend.
    */
    /*
     * Restore transaction and statement start-time timestamps.  This must
     * happen before anything that would start a transaction, else asserts in
     * xact.c will fire.
     */
    SetParallelStartTimestamps(ctx->pwCtx->xact_ts, ctx->pwCtx->stmt_ts);

    /*
     * Identify the entry point to be called.  In theory this could result in
     * loading an additional library, though most likely the entry point is in
     * the core backend or in a library we just loaded.
     */
    parallel_worker_main_type entrypt =
        LookupParallelWorkerFunction(ctx->pwCtx->library_name, ctx->pwCtx->function_name);

    /* Restore database connection. */
    BackgroundWorkerInitializeConnectionByOid(ctx->pwCtx->database_id, ctx->pwCtx->authenticated_user_id, 0);

    /*
     * Set the client encoding to the database encoding, since that is what
     * the leader will expect.
     */
    (void)SetClientEncoding(GetDatabaseEncoding());

    /* Crank up a transaction state appropriate to a parallel worker. */
    StartParallelWorkerTransaction(ctx->pwCtx);

    /* Restore combo CID state. */
    RestoreComboCIDState(ctx->pwCtx);

    /* Restore namespace search path */
    u_sess->attr.attr_common.namespace_search_path = ctx->pwCtx->namespace_search_path;

    /* Restore transaction snapshot. */
    u_sess->utils_cxt.FirstSnapshotSet = true;
    u_sess->utils_cxt.CurrentSnapshot = u_sess->utils_cxt.CurrentSnapshotData;
    u_sess->utils_cxt.CurrentSnapshot->xmin = ctx->pwCtx->xmin;
    u_sess->utils_cxt.CurrentSnapshot->xmax = ctx->pwCtx->xmax;
    u_sess->utils_cxt.CurrentSnapshot->timeline = ctx->pwCtx->timeline;
    u_sess->utils_cxt.CurrentSnapshot->snapshotcsn = ctx->pwCtx->snapshotcsn;
    u_sess->utils_cxt.CurrentSnapshot->curcid = ctx->pwCtx->curcid;
    u_sess->utils_cxt.CurrentSnapshot->active_count = 0;
    u_sess->utils_cxt.CurrentSnapshot->regd_count = 0;
    u_sess->utils_cxt.CurrentSnapshot->copied = false;

    u_sess->utils_cxt.RecentGlobalXmin = ctx->pwCtx->RecentGlobalXmin;
    u_sess->utils_cxt.TransactionXmin = ctx->pwCtx->TransactionXmin;
    u_sess->utils_cxt.RecentXmin = ctx->pwCtx->RecentXmin;

    /* Restore active snapshot. */
    Snapshot active_snapshot = RestoreSnapshot(ctx->pwCtx->asnapspace, ctx->pwCtx->asnapspace_len);
    PushActiveSnapshot(active_snapshot);

    /*
     * We've changed which tuples we can see, and must therefore invalidate
     * system caches.
     */
    InvalidateSystemCaches();

    /*
     * Restore current role id.  Skip verifying whether session user is
     * allowed to become this role and blindly restore the leader's state for
     * current role.
     */
    SetCurrentRoleId(ctx->pwCtx->outer_user_id, ctx->pwCtx->is_superuser);

    /* Restore user ID and security context. */
    SetUserIdAndSecContext(ctx->pwCtx->current_user_id, ctx->pwCtx->sec_context);

    /* Restore temp-namespace state to ensure search path matches leader's. */
    SetTempNamespaceState(ctx->pwCtx->temp_namespace_id, ctx->pwCtx->temp_toast_namespace_id);

    /* Restore relmapper state. */
    u_sess->relmap_cxt.active_shared_updates = ctx->pwCtx->active_shared_updates;
    u_sess->relmap_cxt.active_local_updates = ctx->pwCtx->active_local_updates;

    /*
     * We've initialized all of our state now; nothing should change
     * hereafter.
     */
    t_thrd.bgworker_cxt.InitializingParallelWorker = false;
    EnterParallelMode();

    /*
     * Time to do the real work: invoke the caller-supplied code.
     */
    entrypt(ctx);

    /* Must exit parallel mode to pop active snapshot. */
    ExitParallelMode();

    /* Must pop active snapshot so snapmgr.c doesn't complain. */
    PopActiveSnapshot();

    /* Shut down the parallel-worker transaction. */
    EndParallelWorkerTransaction();

    /* Report success. */
    pq_putmessage('X', NULL, 0);
}

/*
 * Update shared memory with the ending location of the last WAL record we
 * wrote, if it's greater than the value already stored there.
 */
void ParallelWorkerReportLastRecEnd(XLogRecPtr last_xlog_end)
{
    knl_u_parallel_context *ctx = (knl_u_parallel_context *)t_thrd.bgworker_cxt.my_bgworker_entry->bgw_parallel_context;
    Assert(ctx->pwCtx != NULL);
    SpinLockAcquire(&ctx->pwCtx->mutex);
    if (ctx->pwCtx->last_xlog_end < last_xlog_end) {
        ctx->pwCtx->last_xlog_end = last_xlog_end;
    }
    SpinLockRelease(&ctx->pwCtx->mutex);
}

/*
 * Make sure the leader tries to read from our error queue one more time.
 * This guards against the case where we exit uncleanly without sending an
 * ErrorResponse to the leader, for example because some code calls proc_exit
 * directly.
 */
static void ParallelWorkerShutdown(int code, Datum arg)
{
    (void)SendProcSignal(t_thrd.msqueue_cxt.pq_mq_parallel_master_pid, PROCSIG_PARALLEL_MESSAGE,
        t_thrd.msqueue_cxt.pq_mq_parallel_master_backend_id);
}

#ifdef __USE_NUMA
static bool SaveCpuAffinity(cpu_set_t **cpuset)
{
    *cpuset = (cpu_set_t*)palloc(sizeof(cpu_set_t));
    int rc = pthread_getaffinity_np(t_thrd.proc->pid, sizeof(cpu_set_t), *cpuset);
    if (rc != 0) {
        pfree_ext(*cpuset);
        ereport(WARNING, (errmsg("pthread_getaffinity_np failed:%d", rc)));
        return false;
    }
    return true;
}

static void GetCurrentNumaNode(ParallelInfoContext *pcxt)
{
    pcxt->numaNode = -1;
    pcxt->cpuset = NULL;
    int cpu = sched_getcpu();
    if (cpu < 0) {
        ereport(WARNING, (errmsg("sched_getcpu failed, %m")));
        return;
    }

    int numaNode = numa_node_of_cpu(cpu);
    if (numaNode < 0) {
        ereport(WARNING, (errmsg("numa_node_of_cpu failed, %m")));
        return;
    }

    /* Save current CPU affinity, then we can restore it's value after the query is done */
    if (!SaveCpuAffinity(&pcxt->cpuset)) {
        return;
    }

    /*
     * Set thread to current numa node, then it won't schedule to other numa node
     * during query(in most cases). We should reset the affinity after the query is done.
     */
    int rc = numa_run_on_node(numaNode);
    if (rc < 0) {
        pfree_ext(pcxt->cpuset);
        ereport(WARNING, (errmsg("numa_run_on_node failed, %m")));
        return;
    }

    pcxt->numaNode = numaNode;
}
#endif


/*
 * Look up (and possibly load) a parallel worker entry point function.
 *
 * For functions contained in the core code, we use library name "postgres"
 * and consult the InternalParallelWorkers array.  External functions are
 * looked up, and loaded if necessary, using load_external_function().
 *
 * The point of this is to pass function names as strings across process
 * boundaries.  We can't pass actual function addresses because of the
 * possibility that the function has been loaded at a different address
 * in a different process.  This is obviously a hazard for functions in
 * loadable libraries, but it can happen even for functions in the core code
 * on platforms using EXEC_BACKEND (e.g., Windows).
 *
 * At some point it might be worthwhile to get rid of InternalParallelWorkers[]
 * in favor of applying load_external_function() for core functions too;
 * but that raises portability issues that are not worth addressing now.
 */
static parallel_worker_main_type LookupParallelWorkerFunction(const char *libraryname, const char *funcname)
{
    /*
     * If the function is to be loaded from postgres itself, search the
     * InternalParallelWorkers array.
     */
    if (strcmp(libraryname, "postgres") == 0) {
        for (size_t i = 0; i < lengthof(InternalParallelWorkers); i++) {
            if (strcmp(InternalParallelWorkers[i].fn_name, funcname) == 0)
                return InternalParallelWorkers[i].fn_addr;
        }

        ereport(ERROR, (errmsg("internal function \"%s\" not found", funcname)));
    }

    ereport(ERROR, (errmsg("library\"%s\" function \"%s\" not supported", libraryname, funcname)));
    return NULL;
}

