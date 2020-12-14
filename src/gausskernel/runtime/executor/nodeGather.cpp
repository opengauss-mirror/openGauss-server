/* -------------------------------------------------------------------------
 *
 * nodeGather.c
 * 	  Support routines for scanning a plan via multiple workers.
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * A Gather executor launches parallel workers to run multiple copies of a
 * plan.  It can also run the plan itself, if the workers are not available
 * or have not started up yet.  It then merges all of the results it produces
 * and the results from the workers into a single output stream.  Therefore,
 * it will normally be used with a plan where running multiple copies of the
 * same plan does not produce duplicate output, such as parallel-aware
 * SeqScan.
 *
 * Alternatively, a Gather node can be configured to use just one worker
 * and the single-copy flag can be set.  In this case, the Gather node will
 * run the plan in one worker and will not execute the plan itself.  In
 * this case, it simply returns whatever tuples were returned by the worker.
 * If a worker cannot be obtained, then it will run the plan itself and
 * return the results.  Therefore, a plan used with a single-copy Gather
 * node need not be parallel-aware.
 *
 * IDENTIFICATION
 * 	  src/backend/executor/nodeGather.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/relscan.h"
#include "access/xact.h"
#include "executor/execdebug.h"
#include "executor/execParallel.h"
#include "executor/nodeGather.h"
#include "executor/nodeSubplan.h"
#include "executor/tqueue.h"
#include "miscadmin.h"
#include "utils/memutils.h"
#include "utils/rel.h"


static TupleTableSlot *gather_getnext(GatherState *gatherstate);
static HeapTuple gather_readnext(GatherState *gatherstate);
static void ExecShutdownGatherWorkers(GatherState *node);


/* ----------------------------------------------------------------
 * 		ExecInitGather
 * ----------------------------------------------------------------
 */
GatherState *ExecInitGather(Gather *node, EState *estate, int eflags)
{
    bool hasoid = false;

    /* Gather node doesn't have innerPlan node. */
    Assert(innerPlan(node) == NULL);

    /*
     * create state structure
     */
    GatherState *gatherstate = makeNode(GatherState);
    gatherstate->ps.plan = (Plan *)node;
    gatherstate->ps.state = estate;
    gatherstate->need_to_scan_locally = !node->single_copy &&
        u_sess->attr.attr_sql.parallel_leader_participation;
    gatherstate->tuples_needed = -1;

    /*
     * Miscellaneous initialization
     *
     * create expression context for node
     */
    ExecAssignExprContext(estate, &gatherstate->ps);

    /*
     * initialize child expressions
     */
    gatherstate->ps.targetlist = (List *)ExecInitExpr((Expr *)node->plan.targetlist, (PlanState *)gatherstate);
    gatherstate->ps.qual = (List *)ExecInitExpr((Expr *)node->plan.qual, (PlanState *)gatherstate);

    /*
     * tuple table initialization, doesn't need tuple_mcxt
     */
    gatherstate->funnel_slot = MakeTupleTableSlot(false);
    estate->es_tupleTable = lappend(estate->es_tupleTable, gatherstate->funnel_slot);
    ExecInitResultTupleSlot(estate, &gatherstate->ps);

    /*
     * now initialize outer plan
     */
    Plan *outerNode = outerPlan(node);
    outerPlanState(gatherstate) = ExecInitNode(outerNode, estate, eflags);

    gatherstate->ps.ps_TupFromTlist = false;

    /*
     * Initialize result tuple type and projection info.
     */
    ExecAssignResultTypeFromTL(&gatherstate->ps);
    if (tlist_matches_tupdesc(&gatherstate->ps, gatherstate->ps.plan->targetlist,
        OUTER_VAR, ExecGetResultType(outerPlanState(gatherstate)))) {
        gatherstate->ps.ps_ProjInfo = NULL;
    } else {
        ExecAssignProjectionInfo(&gatherstate->ps, NULL);
    }

    /*
     * Initialize funnel slot to same tuple descriptor as outer plan.
     */
    if (!ExecContextForcesOids(&gatherstate->ps, &hasoid))
        hasoid = false;
    TupleDesc tupDesc = ExecTypeFromTL(outerNode->targetlist, hasoid);
    ExecSetSlotDescriptor(gatherstate->funnel_slot, tupDesc);

    return gatherstate;
}

/* ----------------------------------------------------------------
 * 		ExecGather(node)
 *
 * 		Scans the relation via multiple workers and returns
 * 		the next qualifying tuple.
 * ----------------------------------------------------------------
 */
TupleTableSlot *ExecGather(GatherState *node)
{
    TupleTableSlot *fslot = node->funnel_slot;
    TupleTableSlot *slot = NULL;
    TupleTableSlot *resultSlot = NULL;
    ExprDoneCond isDone;

    CHECK_FOR_INTERRUPTS();

    /*
     * Initialize the parallel context and workers on first execution. We do
     * this on first execution rather than during node initialization, as it
     * needs to allocate large dynamic segement, so it is better to do if it
     * is really needed.
     */
    if (!node->initialized) {
        EState *estate = node->ps.state;
        Gather *gather = (Gather *)node->ps.plan;

        /*
         * Sometimes we might have to run without parallelism; but if
         * parallel mode is active then we can try to fire up some workers.
         */
        if (gather->num_workers > 0 && IsInParallelMode()) {
            /* Initialize the workers required to execute Gather node. */
            if (!node->pei) {
                node->pei = ExecInitParallelPlan(node->ps.lefttree, estate, gather->num_workers, node->tuples_needed);
            } else {
                ExecParallelReinitialize(node->ps.lefttree, node->pei);
            }
            /*
             * Register backend workers. We might not get as many as we
             * requested, or indeed any at all.
             */
            ParallelContext *pcxt = node->pei->pcxt;
            LaunchParallelWorkers(pcxt);

            /* Set up tuple queue readers to read the results. */
            if (pcxt->nworkers_launched > 0) {
                ExecParallelCreateReaders(node->pei, fslot->tts_tupleDescriptor);

                /* Make a working array showing the active readers */
                node->nreaders = pcxt->nworkers_launched;
                Size readerSize = node->nreaders * sizeof(TupleQueueReader *);
                node->reader = (TupleQueueReader **)palloc(readerSize);

                int rc = memcpy_s(node->reader, readerSize, node->pei->reader, readerSize);
                securec_check(rc, "", "");

                t_thrd.subrole = BACKGROUND_LEADER;
            } else {
                /* No workers?  Then never mind. */
                node->nreaders = 0;
                node->reader = NULL;
            }
            node->nextreader = 0;
        }

        /* Run plan locally if no workers or not single-copy. */
        node->need_to_scan_locally = (node->nreaders == 0) ||
            (!gather->single_copy && u_sess->attr.attr_sql.parallel_leader_participation);
        node->initialized = true;
    }

    /*
     * Check to see if we're still projecting out tuples from a previous scan
     * tuple (because there is a function-returning-set in the projection
     * expressions).  If so, try to project another one.
     */
    if (node->ps.ps_TupFromTlist) {
        resultSlot = ExecProject(node->ps.ps_ProjInfo, &isDone);
        if (isDone == ExprMultipleResult)
            return resultSlot;
        /* Done with that source tuple... */
        node->ps.ps_TupFromTlist = false;
    }

    /*
     * Reset per-tuple memory context to free any expression evaluation
     * storage allocated in the previous tuple cycle.  Note we can't do this
     * until we're done projecting.  This will also clear any previous tuple
     * returned by a TupleQueueReader; to make sure we don't leave a dangling
     * pointer around, clear the working slot first.
     */
    ExprContext *econtext = node->ps.ps_ExprContext;
    ResetExprContext(econtext);

    /* Get and return the next tuple, projecting if necessary. */
    for (;;) {
        /*
         * Get next tuple, either from one of our workers, or by running the
         * plan ourselves.
         */
        slot = gather_getnext(node);
        if (TupIsNull(slot)) {
            return NULL;
        }

        /* If no projection is required, we're done. */
        if (node->ps.ps_ProjInfo == NULL) {
            return slot;
        }

        /*
         * form the result tuple using ExecProject(), and return it --- unless
         * the projection produces an empty set, in which case we must loop
         * back around for another tuple
         */
        econtext->ecxt_outertuple = slot;
        resultSlot = ExecProject(node->ps.ps_ProjInfo, &isDone);

        if (isDone != ExprEndResult) {
            node->ps.ps_TupFromTlist = (isDone == ExprMultipleResult);
            return resultSlot;
        }
    }

    return slot;
}

/* ----------------------------------------------------------------
 * 		ExecEndGather
 *
 * 		frees any storage allocated through C routines.
 * ----------------------------------------------------------------
 */
void ExecEndGather(GatherState* node)
{
    /* let children clean up first */
    ExecEndNode(outerPlanState(node));
    ExecShutdownGather(node);
    ExecFreeExprContext(&node->ps);
    (void)ExecClearTuple(node->ps.ps_ResultTupleSlot);
}

/*
 * Read the next tuple.  We might fetch a tuple from one of the tuple queues
 * using gather_readnext, or if no tuple queue contains a tuple and the
 * single_copy flag is not set, we might generate one locally instead.
 */
static TupleTableSlot *gather_getnext(GatherState *gatherstate)
{
    PlanState *outerPlan = outerPlanState(gatherstate);
    TupleTableSlot *fslot = gatherstate->funnel_slot;

    while (gatherstate->nreaders > 0 || gatherstate->need_to_scan_locally) {
        CHECK_FOR_INTERRUPTS();

        if (gatherstate->nreaders > 0) {
            HeapTuple tup = gather_readnext(gatherstate);
            if (HeapTupleIsValid(tup)) {
                (void)ExecStoreTuple(tup,   /* tuple to store */
                    fslot,                  /* slot in which to store the tuple */
                    InvalidBuffer,          /* buffer associated with this tuple */
                    true);                  /* pfree this pointer if not from heap */
                return fslot;
            }
        }

        if (gatherstate->need_to_scan_locally) {
            TupleTableSlot *outerTupleSlot = ExecProcNode(outerPlan);

            if (!TupIsNull(outerTupleSlot))
                return outerTupleSlot;

            gatherstate->need_to_scan_locally = false;
        }
    }

    return ExecClearTuple(fslot);
}

/*
 * Attempt to read a tuple from one of our parallel workers.
 */
static HeapTuple gather_readnext(GatherState *gatherstate)
{
    int nvisited = 0;

    for (;;) {
        bool readerdone = false;

        /* Check for async events, particularly messages from workers. */
        CHECK_FOR_INTERRUPTS();

        /* Attempt to read a tuple, but don't block if none is available. */
        TupleQueueReader *reader = gatherstate->reader[gatherstate->nextreader];
        HeapTuple tup = TupleQueueReaderNext(reader, true, &readerdone);

        /*
         * If this reader is done, remove it from our working array of active
         * readers. If all readers are done, we're outta here.
         */
        if (readerdone) {
            Assert(!tup);
            --gatherstate->nreaders;
            if (gatherstate->nreaders == 0) {
                return NULL;
            }
            Size remainSize = sizeof(TupleQueueReader *) * (gatherstate->nreaders - gatherstate->nextreader);
            if (remainSize != 0) {
                int rc = memmove_s(&gatherstate->reader[gatherstate->nextreader], remainSize,
                    &gatherstate->reader[gatherstate->nextreader + 1], remainSize);
                securec_check(rc, "", "");
            }
            if (gatherstate->nextreader >= gatherstate->nreaders) {
                gatherstate->nextreader = 0;
            }
            continue;
        }

        /* If we got a tuple, return it. */
        if (tup)
            return tup;

        /*
         * Advance nextreader pointer in round-robin fashion.  Note that we
         * only reach this code if we weren't able to get a tuple from the
         * current worker.  We used to advance the nextreader pointer after
         * every tuple, but it turns out to be much more efficient to keep
         * reading from the same queue until that would require blocking.
         */
        gatherstate->nextreader++;
        if (gatherstate->nextreader >= gatherstate->nreaders)
            gatherstate->nextreader = 0;

        /* Have we visited every (surviving) TupleQueueReader? */
        nvisited++;
        if (nvisited >= gatherstate->nreaders) {
            /*
             * If (still) running plan locally, return NULL so caller can
             * generate another tuple from the local copy of the plan.
             */
            if (gatherstate->need_to_scan_locally)
                return NULL;

            /* Nothing to do except wait for developments. */
            (void)WaitLatch(&t_thrd.proc->procLatch, WL_LATCH_SET, 0);
            CHECK_FOR_INTERRUPTS();
            ResetLatch(&t_thrd.proc->procLatch);
            nvisited = 0;
        }
    }
}

/* ----------------------------------------------------------------
 * 		ExecShutdownGatherWorkers
 *
 * 		Stop all the parallel workers.
 * ----------------------------------------------------------------
 */
static void ExecShutdownGatherWorkers(GatherState *node)
{
    /* wait for the workers to finish first */
    if (node->pei != NULL)
        ExecParallelFinish(node->pei);

    /* Flush local copy of reader array */
    pfree_ext(node->reader);
}

/* ----------------------------------------------------------------
 * 		ExecShutdownGather
 *
 * 		Destroy the setup for parallel workers including parallel context.
 * 		Collect all the stats after workers are stopped, else some work
 * 		done by workers won't be accounted.
 * ----------------------------------------------------------------
 */
void ExecShutdownGather(GatherState *node)
{
    ExecShutdownGatherWorkers(node);

    /* Now destroy the parallel context. */
    if (node->pei != NULL) {
        ExecParallelCleanup(node->pei);
        node->pei = NULL;
    }
}

/* ----------------------------------------------------------------
 * 						Join Support
 * ----------------------------------------------------------------
 */
/* ----------------------------------------------------------------
 * 		ExecReScanGather
 *
 * 		Re-initialize the workers and rescans a relation via them.
 * ----------------------------------------------------------------
 */
void ExecReScanGather(GatherState* node)
{
    Gather* gather = (Gather*)node->ps.plan;
    PlanState* outerPlan = outerPlanState(node);

    /* Make sure any existing workers are gracefully shut down */
    ExecShutdownGatherWorkers(node);

    /* Mark node so that shared state will be rebuilt at next call */
    node->initialized = false;

    /*
     * Set child node's chgParam to tell it that the next scan might deliver a
     * different set of rows within the leader process.  (The overall rowset
     * shouldn't change, but the leader process's subset might; hence nodes
     * between here and the parallel table scan node mustn't optimize on the
     * assumption of an unchanging rowset.)
     */
    if (gather->rescan_param >= 0) {
        outerPlan->chgParam = bms_add_member(outerPlan->chgParam, gather->rescan_param);
    }

    /*
     * If chgParam of subnode is not null then plan will be re-scanned by
     * first ExecProcNode.  Note: because this does nothing if we have a
     * rescan_param, it's currently guaranteed that parallel-aware child nodes
     * will not see a ReScan call until after they get a ReInitializeDSM call.
     * That ordering might not be something to rely on, though.  A good rule
     * of thumb is that ReInitializeDSM should reset only shared state, ReScan
     * should reset only local state, and anything that depends on both of
     * those steps being finished must wait until the first ExecProcNode call.
     */
    if (outerPlan->chgParam == NULL) {
        ExecReScan(outerPlan);
    }
}
