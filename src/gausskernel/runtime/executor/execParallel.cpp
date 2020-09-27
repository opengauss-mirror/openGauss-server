/* -------------------------------------------------------------------------
 *
 * execParallel.c
 * 	  Support routines for parallel execution.
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * This file contains routines that are intended to support setting up,
 * using, and tearing down a ParallelContext from within the PostgreSQL
 * executor.  The ParallelContext machinery will handle starting the
 * workers and ensuring that their state generally matches that of the
 * leader; see src/backend/access/transam/README.parallel for details.
 * However, we must save and restore relevant executor state, such as
 * any ParamListInfo associated with the query, buffer usage info, and
 * the actual plan to be passed down to the worker.
 *
 * IDENTIFICATION
 * 	  src/backend/executor/execParallel.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "executor/execParallel.h"
#include "executor/executor.h"
#include "executor/nodeSeqscan.h"
#include "executor/tqueue.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/planmain.h"
#include "optimizer/planner.h"
#include "storage/spin.h"
#include "tcop/tcopprot.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"

#define PARALLEL_TUPLE_QUEUE_SIZE 65536

/* DSM structure for accumulating per-PlanState instrumentation. */
struct SharedExecutorInstrumentation {
    int instrument_options;
    uint32 instrument_offset;                /* offset of first Instrumentation struct */
    int num_workers;                         /* # of workers */
    int num_plan_nodes;                      /* # of plan nodes */
    int plan_node_id[FLEXIBLE_ARRAY_MEMBER]; /* array of plan node IDs */
                                             /* array of num_plan_nodes * num_workers Instrumentation objects follows */
};
#define GetInstrumentationArray(sei) (AssertVariableIsOfTypeMacro(sei, SharedExecutorInstrumentation *), \
        (Instrumentation *)(((char *)sei) + sei->instrument_offset))


/* Context object for ExecParallelEstimate. */
typedef struct ExecParallelEstimateContext {
    ParallelContext *pcxt;
    int nnodes;
} ExecParallelEstimateContext;

/* Context object for ExecParallelEstimate. */
typedef struct ExecParallelInitializeDSMContext {
    ParallelContext *pcxt;
    SharedExecutorInstrumentation *instrumentation;
    int nnodes;
} ExecParallelInitializeDSMContext;

/* Helper functions that run in the parallel leader. */
static char *ExecSerializePlan(Plan *plan, EState *estate);
static bool ExecParallelEstimate(PlanState *node, ExecParallelEstimateContext *e);
static bool ExecParallelInitializeDSM(PlanState *node, ExecParallelInitializeDSMContext *d);
static shm_mq_handle **ExecParallelSetupTupleQueues(ParallelContext *pcxt, bool reinitialize);
static bool ExecParallelRetrieveInstrumentation(PlanState *planstate, SharedExecutorInstrumentation *instrumentation);

/* Helper functions that run in the parallel worker. */
static DestReceiver *ExecParallelGetReceiver(void *seg);

/*
 * Create a serialized representation of the plan to be sent to each worker.
 */
static char *ExecSerializePlan(Plan *plan, EState *estate)
{
    ListCell *tlist = NULL;

    /* We can't scribble on the original plan, so make a copy. */
    plan = (Plan *)copyObject(plan);

    /*
     * The worker will start its own copy of the executor, and that copy will
     * insert a junk filter if the toplevel node has any resjunk entries. We
     * don't want that to happen, because while resjunk columns shouldn't be
     * sent back to the user, here the tuples are coming back to another
     * backend which may very well need them.  So mutate the target list
     * accordingly.  This is sort of a hack; there might be better ways to do
     * this...
     */
    foreach (tlist, plan->targetlist) {
        TargetEntry *tle = (TargetEntry *)lfirst(tlist);

        tle->resjunk = false;
    }

    /*
     * Create a dummy PlannedStmt.  Most of the fields don't need to be valid
     * for our purposes, but the worker will need at least a minimal
     * PlannedStmt to start the executor.
     */
    PlannedStmt *pstmt = makeNode(PlannedStmt);
    pstmt->commandType = CMD_SELECT;
    pstmt->queryId = 0;
    pstmt->hasReturning = 0;
    pstmt->hasModifyingCTE = 0;
    pstmt->canSetTag = 1;
    pstmt->transientPlan = 0;
    pstmt->planTree = plan;
    pstmt->rtable = estate->es_range_table;
    pstmt->resultRelations = NIL;
    pstmt->utilityStmt = NULL;
    pstmt->subplans = NIL;
    pstmt->rewindPlanIDs = NULL;
    pstmt->rowMarks = NIL;
    pstmt->nParamExec = estate->es_plannedstmt->nParamExec;
    pstmt->relationOids = NIL;
    pstmt->invalItems = NIL; /* workers can't replan anyway... */
    pstmt->num_plannodes = estate->es_plannedstmt->num_plannodes;

    /* Return serialized copy of our dummy PlannedStmt. */
    return nodeToString(pstmt);
}

/*
 * Ordinary plan nodes won't do anything here, but parallel-aware plan nodes
 * may need some state which is shared across all parallel workers.  Before
 * we size the DSM, give them a chance to call shm_toc_estimate_chunk or
 * shm_toc_estimate_keys on &pcxt->estimator.
 *
 * While we're at it, count the number of PlanState nodes in the tree, so
 * we know how many SharedPlanStateInstrumentation structures we need.
 */
static bool ExecParallelEstimate(PlanState *planstate, ExecParallelEstimateContext *e)
{
    if (planstate == NULL)
        return false;

    /* Count this node. */
    e->nnodes++;

    /* Call estimators for parallel-aware nodes. */
    switch (nodeTag(planstate)) {
        case T_SeqScanState:
            ExecSeqScanEstimate((SeqScanState *)planstate, e->pcxt);
            break;
        default:
            break;
    }

    return planstate_tree_walker(planstate, (bool (*)())ExecParallelEstimate, e);
}

/*
 * Ordinary plan nodes won't do anything here, but parallel-aware plan nodes
 * may need to initialize shared state in the DSM before parallel workers
 * are available.  They can allocate the space they previous estimated using
 * shm_toc_allocate, and add the keys they previously estimated using
 * shm_toc_insert, in each case targeting pcxt->toc.
 */
static bool ExecParallelInitializeDSM(PlanState *planstate, ExecParallelInitializeDSMContext *d)
{
    if (planstate == NULL)
        return false;

    /* If instrumentation is enabled, initialize slot for this node. */
    if (d->instrumentation != NULL) {
        d->instrumentation->plan_node_id[d->nnodes] = planstate->plan->plan_node_id;
    }

    /* Count this node. */
    d->nnodes++;
    knl_u_parallel_context *cxt = (knl_u_parallel_context *)d->pcxt->seg;

    /* Call initializers for parallel-aware plan nodes. */
    switch (nodeTag(planstate)) {
        case T_SeqScanState:
            ExecSeqScanInitializeDSM((SeqScanState *)planstate, d->pcxt, cxt->pwCtx->pscan_num);
            cxt->pwCtx->pscan_num++;
            break;
        default:
            break;
    }

    return planstate_tree_walker(planstate, (bool (*)())ExecParallelInitializeDSM, d);
}

/*
 * It sets up the response queues for backend workers to return tuples
 * to the main backend and start the workers.
 */
static shm_mq_handle **ExecParallelSetupTupleQueues(ParallelContext *pcxt, bool reinitialize)
{
    /* Skip this if no workers. */
    if (pcxt->nworkers <= 0)
        return NULL;

    /* Allocate memory for shared memory queue handles. */
    shm_mq_handle **responseq = (shm_mq_handle **)palloc(pcxt->nworkers * sizeof(shm_mq_handle *));
    knl_u_parallel_context *cxt = (knl_u_parallel_context *)pcxt->seg;
    /*
     * If not reinitializing, allocate space from the DSM for the queues;
     * otherwise, find the already allocated space.
     */
    if (!reinitialize) {
        cxt->pwCtx->tupleQueue = (char *)palloc0(PARALLEL_TUPLE_QUEUE_SIZE * (Size)pcxt->nworkers);
    }
    Assert(cxt->pwCtx->tupleQueue != NULL);
    char *tqueuespace = cxt->pwCtx->tupleQueue;

    /* Create the queues, and become the receiver for each. */
    for (int i = 0; i < pcxt->nworkers; ++i) {
        shm_mq *mq = shm_mq_create(tqueuespace + i * PARALLEL_TUPLE_QUEUE_SIZE, (Size)PARALLEL_TUPLE_QUEUE_SIZE);
        shm_mq_set_receiver(mq, t_thrd.proc);
        responseq[i] = shm_mq_attach(mq, pcxt->seg, NULL);
    }

    /* Return array of handles. */
    return responseq;
}

/*
 * Re-initialize the parallel executor info such that it can be reused by
 * workers.
 */
void ExecParallelReinitialize(ParallelExecutorInfo *pei)
{
    ReinitializeParallelDSM(pei->pcxt);
    pei->tqueue = ExecParallelSetupTupleQueues(pei->pcxt, true);
    pei->finished = false;
}

/*
 * Sets up the required infrastructure for backend workers to perform
 * execution and return results to the main backend.
 */
ParallelExecutorInfo *ExecInitParallelPlan(PlanState *planstate, EState *estate, int nworkers)
{
    ExecParallelEstimateContext e;
    ExecParallelInitializeDSMContext d;
    uint32 instrumentation_len = 0;
    uint32 instrument_offset = 0;

    /* Allocate object for return value. */
    ParallelExecutorInfo *pei = (ParallelExecutorInfo *)palloc0(sizeof(ParallelExecutorInfo));
    pei->finished = false;
    pei->planstate = planstate;

    /* Create a parallel context. */
    ParallelContext *pcxt = CreateParallelContext("postgres", "ParallelQueryMain", nworkers);
    pei->pcxt = pcxt;

    /* Estimate space for serialized ParamListInfo. */
    Size param_len = EstimateParamListSpace(estate->es_param_list_info);

    /*
     * Give parallel-aware nodes a chance to add to the estimates, and get
     * a count of how many PlanState nodes there are.
     */
    e.pcxt = pcxt;
    e.nnodes = 0;
    (void)ExecParallelEstimate(planstate, &e);

    /* Estimate space for instrumentation, if required. */
    if (estate->es_instrument) {
        instrumentation_len = offsetof(SharedExecutorInstrumentation, plan_node_id) + sizeof(int) * e.nnodes;
        instrumentation_len = MAXALIGN(instrumentation_len);
        instrument_offset = instrumentation_len;
        instrumentation_len += sizeof(Instrumentation) * e.nnodes * nworkers;
    }

    /* Everyone's had a chance to ask for space, so now create the DSM. */
    InitializeParallelDSM(pcxt);
    knl_u_parallel_context *cxt = (knl_u_parallel_context *)pcxt->seg;

    /*
     * OK, now we have a dynamic shared memory segment, and it should be big
     * enough to store all of the data we estimated we would want to put into
     * it, plus whatever general stuff (not specifically executor-related) the
     * ParallelContext itself needs to store there.  None of the space we
     * asked for has been allocated or initialized yet, though, so do that.
     */
    MemoryContext oldcontext = MemoryContextSwitchTo(cxt->memCtx);

    /* Store serialized PlannedStmt. */
    cxt->pwCtx->pstmt_space = ExecSerializePlan(planstate->plan, estate);

    /* Store serialized ParamListInfo. */
    cxt->pwCtx->param_space = (char *)palloc0(param_len);
    cxt->pwCtx->param_len = param_len;
    SerializeParamList(estate->es_param_list_info, cxt->pwCtx->param_space, param_len);

    /* Allocate space for each worker's BufferUsage; no need to initialize. */
    cxt->pwCtx->bufUsage = (BufferUsage *)palloc0(sizeof(BufferUsage) * pcxt->nworkers);
    pei->buffer_usage = cxt->pwCtx->bufUsage;

    /* Set up tuple queues. */
    pei->tqueue = ExecParallelSetupTupleQueues(pcxt, false);

    /*
     * If instrumentation options were supplied, allocate space for the
     * data.  It only gets partially initialized here; the rest happens
     * during ExecParallelInitializeDSM.
     */
    if (estate->es_instrument) {
        cxt->pwCtx->instrumentation = (SharedExecutorInstrumentation *)palloc0(instrumentation_len);
        cxt->pwCtx->instrumentation->instrument_options = estate->es_instrument;
        cxt->pwCtx->instrumentation->instrument_offset = instrument_offset;
        cxt->pwCtx->instrumentation->num_workers = nworkers;
        cxt->pwCtx->instrumentation->num_plan_nodes = e.nnodes;
        Instrumentation *instrument = GetInstrumentationArray(cxt->pwCtx->instrumentation);
        for (int i = 0; i < nworkers * e.nnodes; ++i) {
            InstrInit(&instrument[i], estate->es_instrument);
        }
        pei->instrumentation = cxt->pwCtx->instrumentation;
    }

    cxt->pwCtx->pscan = (ParallelHeapScanDesc *)palloc0(sizeof(ParallelHeapScanDesc) * e.nnodes);

    /*
     * Give parallel-aware nodes a chance to initialize their shared data.
     * This also initializes the elements of instrumentation->ps_instrument,
     * if it exists.
     */
    d.pcxt = pcxt;
    d.instrumentation = cxt->pwCtx->instrumentation;
    d.nnodes = 0;

    /* Here we switch to old context, cause heap_beginscan_parallel need malloc memory */
    (void)MemoryContextSwitchTo(oldcontext);
    (void)ExecParallelInitializeDSM(planstate, &d);

    /*
     * Make sure that the world hasn't shifted under our feat.  This could
     * probably just be an Assert(), but let's be conservative for now.
     */
    if (e.nnodes != d.nnodes) {
        ereport(ERROR, (errmsg("inconsistent count of PlanState nodes")));
    }

    /* OK, we're ready to rock and roll. */
    return pei;
}

/*
 * Copy instrumentation information about this node and its descendents from
 * dynamic shared memory.
 */
static bool ExecParallelRetrieveInstrumentation(PlanState *planstate, SharedExecutorInstrumentation *instrumentation)
{
    int i;
    int plan_node_id = planstate->plan->plan_node_id;

    /* Find the instumentation for this node. */
    for (i = 0; i < instrumentation->num_plan_nodes; ++i) {
        if (instrumentation->plan_node_id[i] == plan_node_id) {
            break;
        }
    }
    if (i >= instrumentation->num_plan_nodes) {
        ereport(ERROR, (errmsg("plan node %d not found", plan_node_id)));
    }

    /* Accumulate the statistics from all workers. */
    Instrumentation *instrument = GetInstrumentationArray(instrumentation);
    instrument += i * instrumentation->num_workers;
    for (i = 0; i < instrumentation->num_workers; ++i) {
        InstrAggNode(planstate->instrument, &instrument[i]);
    }

    /* Also store the per-worker detail. */
    Size ibytes = instrumentation->num_workers * sizeof(Instrumentation);
    planstate->worker_instrument =
        (WorkerInstrumentation *)palloc(offsetof(WorkerInstrumentation, instrument) + ibytes);
    planstate->worker_instrument->num_workers = instrumentation->num_workers;
    int rc = memcpy_s(&planstate->worker_instrument->instrument, ibytes, instrument, ibytes);
    securec_check(rc, "", "");

    return planstate_tree_walker(planstate, (bool (*)())ExecParallelRetrieveInstrumentation, instrumentation);
}


/*
 * Finish parallel execution.  We wait for parallel workers to finish, and
 * accumulate their buffer usage and instrumentation.
 */
void ExecParallelFinish(ParallelExecutorInfo *pei)
{
    if (pei->finished)
        return;

    /* First, wait for the workers to finish. */
    WaitForParallelWorkersToFinish(pei->pcxt);

    /* Next, accumulate buffer usage. */
    for (int i = 0; i < pei->pcxt->nworkers; ++i)
        InstrAccumParallelQuery(&pei->buffer_usage[i]);

    /* Finally, accumulate instrumentation, if any. */
    if (pei->instrumentation) {
        (void)ExecParallelRetrieveInstrumentation(pei->planstate, pei->instrumentation);
    }

    pei->finished = true;
}

/*
 * Clean up whatever ParallelExecutreInfo resources still exist after
 * ExecParallelFinish.  We separate these routines because someone might
 * want to examine the contents of the DSM after ExecParallelFinish and
 * before calling this routine.
 */
void ExecParallelCleanup(ParallelExecutorInfo *pei)
{
    if (pei->pcxt != NULL) {
        DestroyParallelContext(pei->pcxt);
        pei->pcxt = NULL;
    }
    pfree(pei);
}

/*
 * Create a DestReceiver to write tuples we produce to the shm_mq designated
 * for that purpose.
 */
static DestReceiver *ExecParallelGetReceiver(void *seg)
{
    Assert(seg != NULL);
    knl_u_parallel_context *cxt = (knl_u_parallel_context *)seg;

    char *mqspace = cxt->pwCtx->tupleQueue;
    mqspace += t_thrd.bgworker_cxt.ParallelWorkerNumber * PARALLEL_TUPLE_QUEUE_SIZE;
    shm_mq *mq = (shm_mq *)mqspace;
    shm_mq_set_sender(mq, t_thrd.proc);
    return CreateTupleQueueDestReceiver(shm_mq_attach(mq, seg, NULL));
}

/*
 * Create a QueryDesc for the PlannedStmt we are to execute, and return it.
 */
static QueryDesc *ExecParallelGetQueryDesc(void *seg, DestReceiver *receiver, int instrument_options)
{
    knl_u_parallel_context *cxt = (knl_u_parallel_context *)seg;

    /* Reconstruct leader-supplied PlannedStmt. */
    PlannedStmt *pstmt = (PlannedStmt *)stringToNode(cxt->pwCtx->pstmt_space);

    /* Reconstruct ParamListInfo. */
    ParamListInfo paramLI = RestoreParamList(cxt->pwCtx->param_space, cxt->pwCtx->param_len);

    /*
     * Create a QueryDesc for the query.
     *
     * It's not obvious how to obtain the query string from here; and even if
     * we could copying it would take more cycles than not copying it. But
     * it's a bit unsatisfying to just use a dummy string here, so consider
     * revising this someday.
     */
    return CreateQueryDesc(pstmt, "<parallel query>", GetActiveSnapshot(), InvalidSnapshot, receiver, paramLI,
        instrument_options);
}

/*
 * Copy instrumentation information from this node and its descendents into
 * dynamic shared memory, so that the parallel leader can retrieve it.
 */
static bool ExecParallelReportInstrumentation(PlanState *planstate, SharedExecutorInstrumentation *instrumentation)
{
    int i;
    int plan_node_id = planstate->plan->plan_node_id;

    InstrEndLoop(planstate->instrument);

    /*
     * If we shuffled the plan_node_id values in ps_instrument into sorted
     * order, we could use binary search here.  This might matter someday
     * if we're pushing down sufficiently large plan trees.  For now, do it
     * the slow, dumb way.
     */
    for (i = 0; i < instrumentation->num_plan_nodes; ++i) {
        if (instrumentation->plan_node_id[i] == plan_node_id) {
            break;
        }
    }
    if (i >= instrumentation->num_plan_nodes) {
        ereport(ERROR, (errmsg("plan node %d not found", plan_node_id)));
    }

    /*
     * Add our statistics to the per-node, per-worker totals.  It's possible
     * that this could happen more than once if we relaunched workers.
     */
    Instrumentation *instrument = GetInstrumentationArray(instrumentation);
    instrument += i * instrumentation->num_workers;
    Assert(IsParallelWorker());
    Assert(t_thrd.bgworker_cxt.ParallelWorkerNumber < instrumentation->num_workers);
    InstrAggNode(&instrument[t_thrd.bgworker_cxt.ParallelWorkerNumber], planstate->instrument);

    return planstate_tree_walker(planstate, (bool (*)())ExecParallelReportInstrumentation, instrumentation);
}

/*
 * Initialize the PlanState and its descendents with the information
 * retrieved from shared memory.  This has to be done once the PlanState
 * is allocated and initialized by executor; that is, after ExecutorStart().
 */
static bool ExecParallelInitializeWorker(PlanState *planstate, void *context)
{
    if (planstate == NULL)
        return false;

    /* Call initializers for parallel-aware plan nodes. */
    if (planstate->plan->parallel_aware) {
        switch (nodeTag(planstate)) {
            case T_SeqScanState:
                ExecSeqScanInitializeWorker((SeqScanState *)planstate, context);
                break;
            default:
                break;
        }
    }

    return planstate_tree_walker(planstate, (bool (*)())ExecParallelInitializeWorker, context);
}

/*
 * Main entrypoint for parallel query worker processes.
 *
 * We reach this function from ParallelMain, so the setup necessary to create
 * a sensible parallel environment has already been done; ParallelMain worries
 * about stuff like the transaction state, combo CID mappings, and GUC values,
 * so we don't need to deal with any of that here.
 *
 * Our job is to deal with concerns specific to the executor.  The parallel
 * group leader will have stored a serialized PlannedStmt, and it's our job
 * to execute that plan and write the resulting tuples to the appropriate
 * tuple queue.  Various bits of supporting information that we need in order
 * to do this are also stored in the dsm_segment and can be accessed through
 * the shm_toc.
 */
void ParallelQueryMain(void *seg)
{
    int instrument_options = 0;

    /* Set up DestReceiver, SharedExecutorInstrumentation, and QueryDesc. */
    knl_u_parallel_context *cxt = (knl_u_parallel_context *)seg;
    DestReceiver *receiver = ExecParallelGetReceiver(seg);
    SharedExecutorInstrumentation *instrumentation = cxt->pwCtx->instrumentation;
    if (instrumentation != NULL)
        instrument_options = instrumentation->instrument_options;
    QueryDesc *queryDesc = ExecParallelGetQueryDesc(seg, receiver, instrument_options);

    /* Prepare to track buffer usage during query execution. */
    InstrStartParallelQuery();

    /* Start up the executor, have it run the plan, and then shut it down. */
    (void)ExecutorStart(queryDesc, 0);
    ExecParallelInitializeWorker(queryDesc->planstate, seg);
    ExecutorRun(queryDesc, ForwardScanDirection, 0L);
    ExecutorFinish(queryDesc);

    /* Report buffer usage during parallel execution. */
    BufferUsage *buffer_usage = cxt->pwCtx->bufUsage;
    InstrEndParallelQuery(&buffer_usage[t_thrd.bgworker_cxt.ParallelWorkerNumber]);

    /* Report instrumentation data if any instrumentation options are set. */
    if (instrumentation != NULL) {
        (void)ExecParallelReportInstrumentation(queryDesc->planstate, instrumentation);
    }

    /* Must do this after capturing instrumentation. */
    ExecutorEnd(queryDesc);

    /* Cleanup. */
    FreeQueryDesc(queryDesc);
    (*receiver->rDestroy)(receiver);
}

