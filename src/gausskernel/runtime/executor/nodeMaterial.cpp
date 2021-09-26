/* -------------------------------------------------------------------------
 *
 * nodeMaterial.cpp
 *	  Routines to handle materialization nodes.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/runtime/executor/nodeMaterial.cpp
 *
 * -------------------------------------------------------------------------
 *
 * INTERFACE ROUTINES
 *		ExecMaterial			- materialize the result of a subplan
 *		ExecInitMaterial		- initialize node and subnodes
 *		ExecEndMaterial			- shutdown node and subnodes
 *
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "executor/executor.h"
#include "executor/node/nodeMaterial.h"
#include "miscadmin.h"
#include "optimizer/streamplan.h"
#include "pgstat.h"
#include "pgxc/pgxc.h"

/*
 * Material all the tuples first, and then return the tuple needed.
 */
static TupleTableSlot* ExecMaterialAll(MaterialState* node) /* result tuple from subplan */
{
    EState* estate = node->ss.ps.state;
    ScanDirection dir = estate->es_direction;
    bool forward = ScanDirectionIsForward(dir);
    Tuplestorestate* tuple_store_state = node->tuplestorestate;
    bool rescan = node->eof_underlying;
    Plan* plan = node->ss.ps.plan;

    int64 operator_mem = SET_NODEMEM(plan->operatorMemKB[0], plan->dop);
    int64 max_mem = (plan->operatorMaxMem > 0) ? SET_NODEMEM(plan->operatorMaxMem, plan->dop) : 0;

    /*
     * If first time through, and we need a tuplestore, initialize it.
     */
    if (tuple_store_state == NULL && node->eflags != 0) {
        tuple_store_state =
            tuplestore_begin_heap(true, false, operator_mem, max_mem, plan->plan_node_id, SET_DOP(plan->dop), true);
        tuplestore_set_eflags(tuple_store_state, node->eflags);
        if (node->eflags & EXEC_FLAG_MARK) {
            /*
             * Allocate a second read pointer to serve as the mark. We know it
             * must have index 1, so needn't store that.
             */
            int ptrno PG_USED_FOR_ASSERTS_ONLY;

            ptrno = tuplestore_alloc_read_pointer(tuple_store_state, node->eflags);
            Assert(ptrno == 1);
        }
        node->tuplestorestate = tuple_store_state;
    }

    if (!forward) {
        if (!node->eof_underlying) {
            /*
             * When reversing direction at tuplestore EOF, the first
             * gettupleslot call will fetch the last-added tuple; but we want
             * to return the one before that, if possible. So do an extra
             * fetch.
             */
            if (!tuplestore_advance(tuple_store_state, forward))
                return NULL; /* the tuplestore must be empty */
        }
    }

    /* fill slots into tuplestore, if any */
    if (!node->eof_underlying) {
        WaitState oldStatus = pgstat_report_waitstatus(STATE_EXEC_MATERIAL);
        for (;;) {
            PlanState* outerNode = outerPlanState(node);
            TupleTableSlot* outerslot = ExecProcNode(outerNode);

            /* subPlan may return NULL */
            if (TupIsNull(outerslot)) {
                node->eof_underlying = true;
                (void)pgstat_report_waitstatus(oldStatus);
                break;
            }

            /*
             * If we need a tuplestore, store slots in tuplestore, else return
             * slots directly.
             */
            if (tuple_store_state != NULL) {
                tuplestore_puttupleslot(tuple_store_state, outerslot);

                if (node->eof_underlying)
                    break;
            } else {
                /*
                 * If there is no tuplestore, just return the slots from sub
                 * plan.
                 */
                return outerslot;
            }
        }
    }

    if (HAS_INSTR(&node->ss, true) && node->tuplestorestate != NULL) {
        if (!rescan)
            node->ss.ps.instrument->width = (int)tuplestore_get_avgwidth(node->tuplestorestate);
        node->ss.ps.instrument->sysBusy = tuplestore_get_busy_status(node->tuplestorestate);
        node->ss.ps.instrument->spreadNum = tuplestore_get_spread_num(node->tuplestorestate);
    }
    /*
     * If we are done filling tuplestore, start fetching tuples from it.
     */
    TupleTableSlot* slot = node->ss.ps.ps_ResultTupleSlot;

    Assert(slot != NULL);

    if (node->eof_underlying && tuple_store_state) {
        if (!tuplestore_gettupleslot(tuple_store_state, forward, false, slot))
            return NULL;
    }

    return slot;
}

/*
 * Material the tuple one by one, the same as old ExecMaterial.
 */
static TupleTableSlot* ExecMaterialOne(MaterialState* node) /* result tuple from subplan */
{
    bool eof_tuple_store = false;
    TupleTableSlot* slot = NULL;
    Plan* plan = node->ss.ps.plan;

    /*
     * get state info from node
     */
    EState* estate = node->ss.ps.state;
    ScanDirection dir = estate->es_direction;
    bool forward = ScanDirectionIsForward(dir);
    Tuplestorestate* tuple_store_state = node->tuplestorestate;

    int64 operator_mem = SET_NODEMEM(plan->operatorMemKB[0], plan->dop);
    int64 max_mem = (plan->operatorMaxMem > 0) ? SET_NODEMEM(plan->operatorMaxMem, plan->dop) : 0;

    /*
     * If first time through, and we need a tuplestore, initialize it.
     */
    if (tuple_store_state == NULL && node->eflags != 0) {
        tuple_store_state =
            tuplestore_begin_heap(true, false, operator_mem, max_mem, plan->plan_node_id, SET_DOP(plan->dop), true);
        tuplestore_set_eflags(tuple_store_state, node->eflags);
        if (node->eflags & EXEC_FLAG_MARK) {
            /*
             * Allocate a second read pointer to serve as the mark. We know it
             * must have index 1, so needn't store that.
             */
            int ptrno PG_USED_FOR_ASSERTS_ONLY;

            ptrno = tuplestore_alloc_read_pointer(tuple_store_state, node->eflags);
            Assert(ptrno == 1);
        }
        node->tuplestorestate = tuple_store_state;
    }

    /*
     * If we are not at the end of the tuplestore, or are going backwards, try
     * to fetch a tuple from tuplestore.
     */
    eof_tuple_store = (tuple_store_state == NULL) || tuplestore_ateof(tuple_store_state);
    if (!forward && eof_tuple_store) {
        if (!node->eof_underlying) {
            /*
             * When reversing direction at tuplestore EOF, the first
             * gettupleslot call will fetch the last-added tuple; but we want
             * to return the one before that, if possible. So do an extra
             * fetch.
             */
            if (!tuplestore_advance(tuple_store_state, forward))
                return NULL; /* the tuplestore must be empty */
        }
        eof_tuple_store = false;
    }

    /*
     * If we can fetch another tuple from the tuplestore, return it.
     */
    slot = node->ss.ps.ps_ResultTupleSlot;
    if (!eof_tuple_store) {
        if (tuplestore_gettupleslot(tuple_store_state, forward, false, slot)) {
            return slot;
        }
        if (forward) {
            eof_tuple_store = true;
        }
    }

    /*
     * If necessary, try to fetch another row from the subplan.
     *
     * Note: the eof_underlying state variable exists to short-circuit further
     * subplan calls.  It's not optional, unfortunately, because some plan
     * node types are not robust about being called again when they've already
     * returned NULL.
     */
    if (eof_tuple_store && !node->eof_underlying) {
        PlanState* outerNode = NULL;
        TupleTableSlot* outerslot = NULL;

        /*
         * We can only get here with forward==true, so no need to worry about
         * which direction the subplan will go.
         */
        outerNode = outerPlanState(node);
        outerslot = ExecProcNode(outerNode);
        if (TupIsNull(outerslot)) {
            node->eof_underlying = true;
            if (HAS_INSTR(&node->ss, true) && node->tuplestorestate != NULL) {
                node->ss.ps.instrument->width = (int)tuplestore_get_avgwidth(node->tuplestorestate);
                node->ss.ps.instrument->sysBusy = tuplestore_get_busy_status(node->tuplestorestate);
                node->ss.ps.instrument->spreadNum = tuplestore_get_spread_num(node->tuplestorestate);
            }
            return NULL;
        }

        /*
         * Append a copy of the returned tuple to tuplestore.  NOTE: because
         * the tuplestore is certainly in EOF state, its read position will
         * move forward over the added tuple.  This is what we want.
         */
        if (tuple_store_state != NULL)
            tuplestore_puttupleslot(tuple_store_state, outerslot);

        /*
         * We can just return the subplan's returned tuple, without copying.
         */
        return outerslot;
    }

    /*
     * Nothing left ...
     */
    return ExecClearTuple(slot);
}

/* ----------------------------------------------------------------
 *		ExecMaterial
 *
 *		As long as we are at the end of the data collected in the tuplestore,
 *		we collect one new row from the subplan on each call, and stash it
 *		aside in the tuplestore before returning it.  The tuplestore is
 *		only read if we are asked to scan backwards, rescan, or mark/restore.
 *
 * ----------------------------------------------------------------
 */
TupleTableSlot* ExecMaterial(MaterialState* node) /* result tuple from subplan */
{
    if (node->materalAll)
        return ExecMaterialAll(node);
    else
        return ExecMaterialOne(node);
}

/* ----------------------------------------------------------------
 *		ExecInitMaterial
 * ----------------------------------------------------------------
 */
MaterialState* ExecInitMaterial(Material* node, EState* estate, int eflags)
{
    Plan* left_tree = NULL;

    /*
     * create state structure
     */
    MaterialState* mat_state = makeNode(MaterialState);
    mat_state->ss.ps.plan = (Plan*)node;
    mat_state->ss.ps.state = estate;

    int64 operator_mem = SET_NODEMEM(((Plan*)node)->operatorMemKB[0], ((Plan*)node)->dop);
    AllocSetContext* set = (AllocSetContext*)(estate->es_query_cxt);
    set->maxSpaceSize = operator_mem * 1024L + SELF_GENRIC_MEMCTX_LIMITATION;

    /*
     * We must have a tuplestore buffering the subplan output to do backward
     * scan or mark/restore.  We also prefer to materialize the subplan output
     * if we might be called on to rewind and replay it many times. However,
     * if none of these cases apply, we can skip storing the data.
     */
    mat_state->eflags = (eflags & (EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK));

    /*
     * Tuplestore's interpretation of the flag bits is subtly different from
     * the general executor meaning: it doesn't think BACKWARD necessarily
     * means "backwards all the way to start".	If told to support BACKWARD we
     * must include REWIND in the tuplestore eflags, else tuplestore_trim
     * might throw away too much.
     */
    if (eflags & EXEC_FLAG_BACKWARD)
        mat_state->eflags |= EXEC_FLAG_REWIND;

    /* Currently, we don't want to rescan stream node in subplans */
    if (node->materialize_all)
        mat_state->eflags |= EXEC_FLAG_REWIND;

    mat_state->eof_underlying = false;
    mat_state->tuplestorestate = NULL;

    if (node->plan.ispwj) {
        mat_state->ss.currentSlot = 0;
    }

    /*
     * Miscellaneous initialization
     *
     * Materialization nodes don't need ExprContexts because they never call
     * ExecQual or ExecProject.
     */
    /*
     * tuple table initialization
     *
     * material nodes only return tuples from their materialized relation.
     */
    ExecInitResultTupleSlot(estate, &mat_state->ss.ps);
    ExecInitScanTupleSlot(estate, &mat_state->ss);

    /*
     * initialize child nodes
     *
     * We shield the child node from the need to support REWIND, BACKWARD, or
     * MARK/RESTORE.
     */
    eflags &= ~(EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK);

    left_tree = outerPlan(node);
    outerPlanState(mat_state) = ExecInitNode(left_tree, estate, eflags);

    /*
     * initialize tuple type.  no need to initialize projection info because
     * this node doesn't do projections.
     */
    ExecAssignScanTypeFromOuterPlan(&mat_state->ss);

    ExecAssignResultTypeFromTL(
            &mat_state->ss.ps,
            mat_state->ss.ss_ScanTupleSlot->tts_tupleDescriptor->tdTableAmType);

    mat_state->ss.ps.ps_ProjInfo = NULL;

    Assert(mat_state->ss.ps.ps_ResultTupleSlot->tts_tupleDescriptor->tdTableAmType != TAM_INVALID);

    /*
     * Lastly, if this Material node is under subplan and used for materializing
     * Stream(BROADCAST) data, add it to estate->es_material_of_subplan
     * so that it can be run before main plan in ExecutePlan.
     */
    if (IS_PGXC_DATANODE && estate->es_under_subplan && IsA(left_tree, Stream) &&
        ((Stream*)left_tree)->type == STREAM_BROADCAST)
        estate->es_material_of_subplan = lappend(estate->es_material_of_subplan, (PlanState*)mat_state);

    return mat_state;
}

/* ----------------------------------------------------------------
 *		ExecEndMaterial
 * ----------------------------------------------------------------
 */
void ExecEndMaterial(MaterialState* node)
{
    /*
     * clean out the tuple table
     */
    (void)ExecClearTuple(node->ss.ss_ScanTupleSlot);

    /*
     * Release tuplestore resources
     */
    if (node->tuplestorestate != NULL)
        tuplestore_end(node->tuplestorestate);
    node->tuplestorestate = NULL;

    /*
     * shut down the subplan
     */
    ExecEndNode(outerPlanState(node));
}

/* ----------------------------------------------------------------
 *		ExecMaterialMarkPos
 *
 *		Calls tuplestore to save the current position in the stored file.
 * ----------------------------------------------------------------
 */
void ExecMaterialMarkPos(MaterialState* node)
{
    Assert(node->eflags & EXEC_FLAG_MARK);

    /*
     * if we haven't materialized yet, just return.
     */
    if (!node->tuplestorestate)
        return;

    /*
     * copy the active read pointer to the mark.
     */
    tuplestore_copy_read_pointer(node->tuplestorestate, 0, 1);

    /*
     * since we may have advanced the mark, try to truncate the tuplestore.
     */
    tuplestore_trim(node->tuplestorestate);
}

/* ----------------------------------------------------------------
 *		ExecMaterialRestrPos
 *
 *		Calls tuplestore to restore the last saved file position.
 * ----------------------------------------------------------------
 */
void ExecMaterialRestrPos(MaterialState* node)
{
    Assert(node->eflags & EXEC_FLAG_MARK);

    /*
     * if we haven't materialized yet, just return.
     */
    if (!node->tuplestorestate)
        return;

    /*
     * copy the mark to the active read pointer.
     */
    tuplestore_copy_read_pointer(node->tuplestorestate, 1, 0);
}

/* ----------------------------------------------------------------
 *		ExecReScanMaterial
 *
 *		Rescans the materialized relation.
 * ----------------------------------------------------------------
 */
void ExecReScanMaterial(MaterialState* node)
{
    int param_no = 0;
    int part_itr = 0;
    ParamExecData* param = NULL;
    bool need_switch_partition = false;

    /* Already reset, just rescan left_tree */
    if (node->ss.ps.recursive_reset && node->ss.ps.state->es_recursive_next_iteration) {
        if (node->ss.ps.lefttree->chgParam == NULL)
            ExecReScan(node->ss.ps.lefttree);

        node->ss.ps.recursive_reset = false;
        return;
    }

    (void)ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);

    if (node->ss.ps.plan->ispwj) {
        param_no = node->ss.ps.plan->paramno;
        param = &(node->ss.ps.state->es_param_exec_vals[param_no]);
        part_itr = (int)param->value;

        if (part_itr != node->ss.currentSlot) {
            need_switch_partition = true;
        }

        node->ss.currentSlot = part_itr;
    }

    if (node->eflags != 0) {
        /*
         * If we haven't materialized yet, just return. If outerplan's
         * chgParam is not NULL then it will be re-scanned by ExecProcNode,
         * else no reason to re-scan it at all.
         */
        if (!node->tuplestorestate) {
            if (node->ss.ps.plan->ispwj) {
                ExecReScan(node->ss.ps.lefttree);
            }

            return;
        }
        /*
         * If subnode is to be rescanned then we forget previous stored
         * results; we have to re-read the subplan and re-store.  Also, if we
         * told tuplestore it needn't support rescan, we lose and must
         * re-read.  (This last should not happen in common cases; else our
         * caller lied by not passing EXEC_FLAG_REWIND to us.)
         *
         * Otherwise we can just rewind and rescan the stored output. The
         * state of the subnode does not change.
         */
        if (node->ss.ps.lefttree->chgParam != NULL || (node->eflags & EXEC_FLAG_REWIND) == 0) {
            tuplestore_end(node->tuplestorestate);
            node->tuplestorestate = NULL;
            if (node->ss.ps.lefttree->chgParam == NULL)
                ExecReScan(node->ss.ps.lefttree);
            node->eof_underlying = false;
        } else {
            if (node->ss.ps.plan->ispwj && need_switch_partition) {
                tuplestore_end(node->tuplestorestate);
                node->tuplestorestate = NULL;

                ExecReScan(node->ss.ps.lefttree);
                node->eof_underlying = false;
            } else {
                tuplestore_rescan(node->tuplestorestate);
            }
        }
    } else {
        /* In this case we are just passing on the subquery's output */
        /*
         * if chgParam of subnode is not null then plan will be re-scanned by
         * first ExecProcNode.
         */
        if (node->ss.ps.lefttree->chgParam == NULL)
            ExecReScan(node->ss.ps.lefttree);
        node->eof_underlying = false;
    }
}

/*
 * @Description: Early free the memory for Material.
 *
 * @param[IN] node:  executor state for Material
 * @return: void
 */
void ExecEarlyFreeMaterial(MaterialState* node)
{
    PlanState* plan_state = &node->ss.ps;

    if (plan_state->earlyFreed)
        return;

    /*
     * clean out the tuple table
     */
    (void)ExecClearTuple(node->ss.ss_ScanTupleSlot);

    /*
     * Release tuplestore resources
     */
    if (node->tuplestorestate != NULL)
        tuplestore_end(node->tuplestorestate);
    node->tuplestorestate = NULL;

    EARLY_FREE_LOG(elog(LOG,
        "Early Free: After early freeing Material "
        "at node %d, memory used %d MB.",
        plan_state->plan->plan_node_id,
        getSessionMemoryUsageMB()));

    plan_state->earlyFreed = true;
    ExecEarlyFree(outerPlanState(node));
}

/*
 * @Function: ExecReSetMaterial()
 *
 * @Brief: Reset the material state structure in rescan case
 *	under recursive-stream new iteration condition.
 *
 * @Input node: node material planstate
 *
 * @Return: no return value
 */
void ExecReSetMaterial(MaterialState* node)
{
    if (node->tuplestorestate) {
        (void)ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
        tuplestore_end(node->tuplestorestate);
        node->tuplestorestate = NULL;
    }

    node->eof_underlying = false;
    node->ss.ps.recursive_reset = true;

    if (node->ss.ps.lefttree->chgParam == NULL)
        ExecReSetRecursivePlanTree(outerPlanState(node));

    return;
}
