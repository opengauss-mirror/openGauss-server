/* -------------------------------------------------------------------------
 *
 * nodeWorktablescan.cpp
 *    routines to handle WorkTableScan nodes.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *    src/gausskernel/runtime/executor/nodeWorktablescan.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "executor/exec/execdebug.h"
#include "executor/node/nodeRecursiveunion.h"
#include "executor/node/nodeWorktablescan.h"

static TupleTableSlot* ExecWorkTableScan(PlanState* node);
static TupleTableSlot* WorkTableScanNext(WorkTableScanState* node);

/* ----------------------------------------------------------------
 * WorkTableScanNext
 *
 * This is a workhorse for ExecWorkTableScan
 * ----------------------------------------------------------------
 */
static TupleTableSlot* WorkTableScanNext(WorkTableScanState* node)
{
    /*
     * get information from the estate and scan state
     *
     * Note: we intentionally do not support backward scan.  Although it would
     * take only a couple more lines here, it would force nodeRecursiveunion.c
     * to create the tuplestore with backward scan enabled, which has a
     * performance cost.  In practice backward scan is never useful for a
     * worktable plan node, since it cannot appear high enough in the plan
     * tree of a scrollable cursor to be exposed to a backward-scan
     * requirement.  So it's not worth expending effort to support it.
     *
     * Note: we are also assuming that this node is the only reader of the
     * worktable.  Therefore, we don't need a private read pointer for the
     * tuplestore, nor do we need to tell tuplestore_gettupleslot to copy.
     */
    Assert(ScanDirectionIsForward(node->ss.ps.state->es_direction));

    Tuplestorestate* tuple_store_state = node->rustate->working_table;

    /*
     * Get the next tuple from tuplestore. Return NULL if no more tuples.
     */
    TupleTableSlot* slot = node->ss.ss_ScanTupleSlot;

    if (node->rustate->rucontroller != NULL) {
        StreamNodeGroup* stream_nodegroup = u_sess->stream_cxt.global_obj;

        if (stream_nodegroup == NULL) {
            elog(ERROR,
                "MPP with-recursive, globalStreamNodeGroup is not found in Node:[%d]",
                GET_PLAN_NODEID(node->rustate->ps.plan));
        }

        pthread_mutex_t* recursive_mutex = stream_nodegroup->GetRecursiveMutex();
        AutoMutexLock recursiveLock(recursive_mutex);
        /*
         * In case of distribute recursive CTE and with Stream operator on inner side
         * of RecursiveUnion, we have to do worktable scan on Consumer's vfdcache, as
         * the tuplestore under WorkTableScan is created on Consumer thread, also we
         * need lock the vfdcache pointer to avoid being repallocated on Consumer thread
         * to avoid access trash pointer or fd
         */
        recursiveLock.lock();
        {
            /* If vfd already invalidated, stop scan work table here */
            if (stream_nodegroup->GetRecursiveVfdInvalid()) {
                recursiveLock.unLock();
                ereport(ERROR,
                    (errcode(ERRCODE_RU_STOP_QUERY),
                        errmsg("stop scan the work table due to transaction aborted and vfd invalidated")));
            }
            RecursiveVfd* recursive_cte_vfd = &node->rustate->rucontroller->recursive_vfd;
            /* Swith to consumer's VfdCache */
            SwitchToGlobalVfdCache(
                (void**)recursive_cte_vfd->recursive_VfdCache, recursive_cte_vfd->recursive_SizeVfdCache);

            PG_TRY();
            {
                (void)tuplestore_gettupleslot(tuple_store_state, true, false, slot);
            }
            PG_CATCH();
            {
                ResetToLocalVfdCache();

                recursiveLock.unLock();
                PG_RE_THROW();
            }
            PG_END_TRY();

            /* Set back to producer's VfdCache */
            ResetToLocalVfdCache();
        }
        recursiveLock.unLock();
    } else {
        (void)tuplestore_gettupleslot(tuple_store_state, true, false, slot);
    }

    return slot;
}

/*
 * WorkTableScanRecheck -- access method routine to recheck a tuple in EvalPlanQual
 */
static bool WorkTableScanRecheck(WorkTableScanState* node, TupleTableSlot* slot)
{
    /* nothing to check */
    return true;
}

/* ----------------------------------------------------------------
 * ExecWorkTableScan(node)
 *
 * Scans the worktable sequentially and returns the next qualifying tuple.
 * We call the ExecScan() routine and pass it the appropriate
 * access method functions.
 * ----------------------------------------------------------------
 */
static TupleTableSlot* ExecWorkTableScan(PlanState* state)
{
    WorkTableScanState* node = castNode(WorkTableScanState, state);
    /*
     * On the first call, find the ancestor RecursiveUnion's state via the
     * Param slot reserved for it.	(We can't do this during node init because
     * there are corner cases where we'll get the init call before the
     * RecursiveUnion does.)
     */
    if (node->rustate == NULL) {
        WorkTableScan* plan = (WorkTableScan*)node->ss.ps.plan;
        EState* estate = node->ss.ps.state;
        ParamExecData* param = NULL;

        param = &(estate->es_param_exec_vals[plan->wtParam]);
        Assert(param->execPlan == NULL);
        Assert(!param->isnull);
        node->rustate = (RecursiveUnionState*)DatumGetPointer(param->value);

        TupleDesc tuple_desc = NULL;

        /* distributed with-recursive support */
        if (IS_PGXC_DATANODE && node->rustate == NULL) {
            int ru_plan_nodeid = node->ss.ps.plan->recursive_union_plan_nodeid;
            StreamNodeGroup* stream_nodegroup = u_sess->stream_cxt.global_obj;

            Assert(stream_nodegroup != NULL);
            /* Find the corresponse controller and fetch recursive-union state object */
            SyncController* controller = stream_nodegroup->GetSyncController(ru_plan_nodeid);
            if (controller == NULL) {
                ereport(ERROR,
                    (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                        errmsg("MPP With-Recursive sync controller for RecursiveUnion[%d] is not found",
                            node->ss.ps.plan->plan_node_id)));
            }

            /*
             * the corresponding consumer may encounter a "short-circuit", in the normal
             * executor function, we just mark executorstop and return NULL, let the top
             * producer logic in ExecutePlan to handle ends-up steps
             */
            if (controller->executor_stop) {
                u_sess->exec_cxt.executorStopFlag = true;
                return NULL;
            }

            node->rustate = (RecursiveUnionState*)controller->controller_planstate;

            /*
             * In distributed recursive CTE, the tupdesc stored in rustate is from
             * another stream thread where it get allocated, we have to do tupdesc
             * copy in current thread memeory context to keep it safely without
             * being freed by another stream thread which may cause memory issues.
             */
            tuple_desc = CreateTupleDescCopyConstr(ExecGetResultType(&node->rustate->ps));
        } else {
            tuple_desc = ExecGetResultType(&node->rustate->ps);
        }

        Assert(node->rustate && IsA(node->rustate, RecursiveUnionState));

        /*
         * Assert tuple desc if ound either in distributed or none-distributed execution
         * cases.
         */
        Assert(tuple_desc != NULL);

        /*
         * The scan tuple type (ie, the rowtype we expect to find in the work
         * table) is the same as the result rowtype of the ancestor
         * RecursiveUnion node.  Note this depends on the assumption that
         * RecursiveUnion doesn't allow projection.
         */
        ExecAssignScanType(&node->ss, tuple_desc);

        /*
         * Now we can initialize the projection info.  This must be completed
         * before we can call ExecScan().
         */
        ExecAssignScanProjectionInfo(&node->ss);
    }

    return ExecScan(&node->ss, (ExecScanAccessMtd)WorkTableScanNext, (ExecScanRecheckMtd)WorkTableScanRecheck);
}

/* ----------------------------------------------------------------
 *		ExecInitWorkTableScan
 * ----------------------------------------------------------------
 */
WorkTableScanState* ExecInitWorkTableScan(WorkTableScan* node, EState* estate, int eflags)
{
    /* check for unsupported flags */
    Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

    /*
     * WorkTableScan should not have any children.
     */
    Assert(outerPlan(node) == NULL);
    Assert(innerPlan(node) == NULL);

    /*
     * create new WorkTableScanState for node
     */
    WorkTableScanState* scan_state = makeNode(WorkTableScanState);

    scan_state->ss.ps.plan = (Plan*)node;
    scan_state->ss.ps.state = estate;
    scan_state->rustate = NULL; /* we'll set this later */
    scan_state->ss.ps.ExecProcNode = ExecWorkTableScan;

    /*
     * Miscellaneous initialization
     *
     * create expression context for node
     */
    ExecAssignExprContext(estate, &scan_state->ss.ps);

    /*
     * initialize child expressions
     */
    if (estate->es_is_flt_frame) {
        scan_state->ss.ps.qual = (List*)ExecInitQualByFlatten(node->scan.plan.qual, (PlanState*)scan_state);
    } else {
        scan_state->ss.ps.targetlist = (List*)ExecInitExprByRecursion((Expr*)node->scan.plan.targetlist, (PlanState*)scan_state);
        scan_state->ss.ps.qual = (List*)ExecInitExprByRecursion((Expr*)node->scan.plan.qual, (PlanState*)scan_state);
    }

    /*
     * tuple table initialization
     */
    ExecInitResultTupleSlot(estate, &scan_state->ss.ps);
    ExecInitScanTupleSlot(estate, &scan_state->ss);

    /*
     * Initialize result tuple type, but not yet projection info.
     */
    ExecAssignResultTypeFromTL(&scan_state->ss.ps);

    scan_state->ss.ps.ps_vec_TupFromTlist = false;

    return scan_state;
}

/* ----------------------------------------------------------------
 * ExecEndWorkTableScan
 *
 * frees any storage allocated through C routines.
 * ----------------------------------------------------------------
 */
void ExecEndWorkTableScan(WorkTableScanState* node)
{
    /*
     * Free exprcontext
     */
    ExecFreeExprContext(&node->ss.ps);

    /*
     * clean out the tuple table
     */
    (void)ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
    (void)ExecClearTuple(node->ss.ss_ScanTupleSlot);
}

/* ----------------------------------------------------------------
 * ExecReScanWorkTableScan
 *
 * Rescans the relation.
 * ----------------------------------------------------------------
 */
void ExecReScanWorkTableScan(WorkTableScanState* node)
{
    (void)ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);

    ExecScanReScan(&node->ss);

    /* No need (or way) to rescan if ExecWorkTableScan not called yet */
    if (node->rustate)
        tuplestore_rescan(node->rustate->working_table);
}
