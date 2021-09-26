/* -------------------------------------------------------------------------
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2010, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * vecmaterial.cpp
 *	  Routines to handle materialization nodes.
 *
 * IDENTIFICATION
 *	  Code/src/gausskernel/runtime/vecexecutor/vecnode/vecmaterial.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "executor/executor.h"
#include "miscadmin.h"
#include "optimizer/streamplan.h"
#include "pgxc/pgxc.h"
#include "utils/batchstore.h"
#include "vecexecutor/vecexecutor.h"
#include "vecexecutor/vecmaterial.h"

/*
 * Material all the tuples first, and then return the tuple needed.
 */
static VectorBatch* exec_vec_material_all(VecMaterialState* node) /* result tuple from subplan */
{
    EState* estate = NULL;
    ScanDirection dir;
    BatchStore* batch_store_stat = NULL;
    VectorBatch* batch = NULL;
    bool rescan = node->eof_underlying;
    Plan* plan = node->ss.ps.plan;

    int64 operator_mem = SET_NODEMEM(plan->operatorMemKB[0], plan->dop);
    int64 max_mem = (plan->operatorMaxMem > 0) ? SET_NODEMEM(plan->operatorMaxMem, plan->dop) : 0;

    /*
     * get state info from node
     */
    estate = node->ss.ps.state;
    dir = estate->es_direction;
    batch_store_stat = node->batchstorestate;
    node->m_pCurrentBatch->Reset();

    /*
     * If first time through, and we need a tuplestore, initialize it.
     */
    bool isInit = (batch_store_stat == NULL && node->eflags != 0);
    if (isInit) {
        PlanState* outer_node = NULL;
        TupleDesc tup_desc;
        outer_node = outerPlanState(node);
        tup_desc = ExecGetResultType(outer_node);
        batch_store_stat =
            batchstore_begin_heap(tup_desc, true, false, operator_mem, max_mem, plan->plan_node_id, SET_DOP(plan->dop));
        batchstore_set_eflags(batch_store_stat, node->eflags);
        if ((uint32)node->eflags & EXEC_FLAG_MARK) {
            /*
             * Allocate a second read pointer to serve as the mark. We know it
             * must have index 1, so needn't store that.
             */
            int ptrno PG_USED_FOR_ASSERTS_ONLY;

            ptrno = batchstore_alloc_read_pointer(batch_store_stat, node->eflags);
            Assert(ptrno == 1);
        }
        node->batchstorestate = batch_store_stat;
    }
    if (!node->eof_underlying) {
        PlanState* outer_node = NULL;
        VectorBatch* outer_batch = NULL;

        if (batch_store_stat != NULL && !batch_store_stat->m_colInfo) {
            batch_store_stat->InitColInfo(node->m_pCurrentBatch);
        }

        WaitState old_status = pgstat_report_waitstatus(STATE_EXEC_MATERIAL);
        for (;;) {
            outer_node = outerPlanState(node);
            outer_batch = VectorEngine(outer_node);

            /* subPlan may return NULL */
            if (BatchIsNull(outer_batch)) {
                node->eof_underlying = true;
                (void)pgstat_report_waitstatus(old_status);
                break;
            }

            if (batch_store_stat != NULL) {
                batchstore_putbatch(batch_store_stat, outer_batch);

                if (node->eof_underlying) {
                    break;
                }
            } else {
                /*
                 * If there is no tuplestore, just return the slots from sub
                 * plan.
                 */
                return outer_batch;
            }
        }
    }

    /*
     * If we are done filling tuplestore, start fetching tuples from it.
     */
    if (HAS_INSTR(&node->ss, true) && batch_store_stat != NULL) {
        if (batch_store_stat->m_status == BSS_INMEM && !rescan && batch_store_stat->m_storeColumns.m_memRowNum > 0) {
            batch_store_stat->m_colWidth /= batch_store_stat->m_storeColumns.m_memRowNum;
        }
        node->ss.ps.instrument->width = (int)node->batchstorestate->m_colWidth;
        node->ss.ps.instrument->sysBusy = node->batchstorestate->m_sysBusy;
        node->ss.ps.instrument->spreadNum = node->batchstorestate->m_spreadNum;
    }
    batch = node->m_pCurrentBatch;

    if (node->eof_underlying && batch_store_stat) {
        if (!batchstore_getbatch(batch_store_stat, ScanDirectionIsForward(dir), batch)) {
            return NULL;
        }
    }

    return batch;
}

/*
 * Material the tuple one by one, the same as old ExecMaterial.
 */
static VectorBatch* exec_vec_material_one(VecMaterialState* node) /* result tuple from subplan */
{
    EState* estate = NULL;
    ScanDirection dir;
    bool forward = false;
    bool eof_tuplestore = false;
    BatchStore* batch_store_stat = NULL;
    VectorBatch* batch = NULL;
    Plan* plan = node->ss.ps.plan;

    int64 operator_mem = SET_NODEMEM(plan->operatorMemKB[0], plan->dop);
    int64 max_mem = (plan->operatorMaxMem > 0) ? SET_NODEMEM(plan->operatorMaxMem, plan->dop) : 0;

    /*
     * get state info from node
     */
    estate = node->ss.ps.state;
    dir = estate->es_direction;
    forward = ScanDirectionIsForward(dir);
    batch_store_stat = node->batchstorestate;
    node->m_pCurrentBatch->Reset();
    WaitState old_status = pgstat_report_waitstatus(STATE_EXEC_MATERIAL);

    /*
     * If first time through, and we need a tuplestore, initialize it.
     */
    if (batch_store_stat == NULL && node->eflags != 0) {
        PlanState* outer_node = NULL;
        TupleDesc tup_desc;
        outer_node = outerPlanState(node);
        tup_desc = ExecGetResultType(outer_node);
        batch_store_stat =
            batchstore_begin_heap(tup_desc, true, false, operator_mem, max_mem, plan->plan_node_id, SET_DOP(plan->dop));
        batchstore_set_eflags(batch_store_stat, node->eflags);
        if ((uint32)node->eflags & EXEC_FLAG_MARK) {
            /*
             * Allocate a second read pointer to serve as the mark. We know it
             * must have index 1, so needn't store that.
             */
            int ptrno PG_USED_FOR_ASSERTS_ONLY;

            ptrno = batchstore_alloc_read_pointer(batch_store_stat, node->eflags);
            Assert(ptrno == 1);
        }
        node->batchstorestate = batch_store_stat;
    }

    /*
     * If we are not at the end of the tuplestore, or are going backwards, try
     * to fetch a tuple from tuplestore.
     */
    eof_tuplestore = (batch_store_stat == NULL) || batchstore_ateof(batch_store_stat);
    /*
     * If we can fetch another tuple from the tuplestore, return it.
     */
    batch = node->m_pCurrentBatch;
    if (!eof_tuplestore || node->eof_underlying) {
        if (batch_store_stat && batchstore_getbatch(batch_store_stat, forward, batch)) {
            node->from_memory = true;
            (void)pgstat_report_waitstatus(old_status);
            return batch;
        }
        if (forward) {
            eof_tuplestore = true;
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
    if (eof_tuplestore && !node->eof_underlying) {
        PlanState* outer_node = NULL;
        VectorBatch* outer_batch = NULL;

        if (batch_store_stat != NULL && !batch_store_stat->m_colInfo) {
            batch_store_stat->InitColInfo(node->m_pCurrentBatch);
        }

        /*
         * We can only get here with forward==true, so no need to worry about
         * which direction the subplan will go.
         */
        outer_node = outerPlanState(node);
        outer_batch = VectorEngine(outer_node);
        if (BatchIsNull(outer_batch)) {
            node->eof_underlying = true;
            if (HAS_INSTR(&node->ss, true) && batch_store_stat != NULL) {
                if (batch_store_stat->m_status == BSS_INMEM && batch_store_stat->m_storeColumns.m_memRowNum > 0)
                    batch_store_stat->m_colWidth /= batch_store_stat->m_storeColumns.m_memRowNum;
                node->ss.ps.instrument->width = (int)node->batchstorestate->m_colWidth;
                node->ss.ps.instrument->sysBusy = node->batchstorestate->m_sysBusy;
                node->ss.ps.instrument->spreadNum = node->batchstorestate->m_spreadNum;
            }
            (void)pgstat_report_waitstatus(old_status);
            return NULL;
        }

        node->from_memory = false;

        /*
         * Append a copy of the returned tuple to tuplestore.  NOTE: because
         * the tuplestore is certainly in EOF state, its read position will
         * move forward over the added tuple.  This is what we want.
         */
        if (batch_store_stat != NULL)
            batchstore_putbatch(batch_store_stat, outer_batch);

        /*
         * We can just return the subplan's returned tuple, without copying.
         */
        (void)pgstat_report_waitstatus(old_status);
        return outer_batch;
    }

    /*
     * Nothing left ...
     */
    (void)pgstat_report_waitstatus(old_status);
    return batch;
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
VectorBatch* ExecVecMaterial(VecMaterialState* node)
{
    if (node->materalAll) {
        return exec_vec_material_all(node);
    } else {
        return exec_vec_material_one(node);
    }
}

/* ----------------------------------------------------------------
 *		ExecInitMaterial
 * ----------------------------------------------------------------
 */
VecMaterialState* ExecInitVecMaterial(VecMaterial* node, EState* estate, int eflags)
{
    VecMaterialState* matstate = NULL;
    Plan* left_plan = NULL;

    /*
     * create state structure
     */
    matstate = makeNode(VecMaterialState);
    matstate->ss.ps.plan = (Plan*)node;
    matstate->ss.ps.state = estate;
    matstate->ss.ps.vectorized = true;
    matstate->from_memory = true;

    /*
     * We must have a tuplestore buffering the subplan output to do backward
     * scan or mark/restore.  We also prefer to materialize the subplan output
     * if we might be called on to rewind and replay it many times. However,
     * if none of these cases apply, we can skip storing the data.
     */
    matstate->eflags = ((uint32)eflags & (EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK));

    /*
     * Tuplestore's interpretation of the flag bits is subtly different from
     * the general executor meaning: it doesn't think BACKWARD necessarily
     * means "backwards all the way to start".	If told to support BACKWARD we
     * must include REWIND in the tuplestore eflags, else tuplestore_trim
     * might throw away too much.
     */
    if ((uint32)eflags & EXEC_FLAG_BACKWARD)
        matstate->eflags = (uint32)matstate->eflags | EXEC_FLAG_REWIND;

    /* Currently, we don't want to rescan stream node in subplans */
    if (node->materialize_all)
        matstate->eflags = (uint32)matstate->eflags | EXEC_FLAG_REWIND;

    matstate->eof_underlying = false;
    matstate->batchstorestate = NULL;

    if (node->plan.ispwj) {
        matstate->ss.currentSlot = 0;
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
    ExecInitResultTupleSlot(estate, &matstate->ss.ps);
    ExecInitScanTupleSlot(estate, &matstate->ss);

    /*
     * initialize child nodes
     *
     * We shield the child node from the need to support REWIND, BACKWARD, or
     * MARK/RESTORE.
     */
    eflags = (uint32)eflags & ~(EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK);

    left_plan = outerPlan(node);
    outerPlanState(matstate) = ExecInitNode(left_plan, estate, eflags);

    /*
     * initialize tuple type.  no need to initialize projection info because
     * this node doesn't do projections.
     */
    ExecAssignScanTypeFromOuterPlan(&matstate->ss);

    ExecAssignResultTypeFromTL(
            &matstate->ss.ps,
            matstate->ss.ss_ScanTupleSlot->tts_tupleDescriptor->tdTableAmType);

    matstate->ss.ps.ps_ProjInfo = NULL;

    /*
     * Vectorization specific initialization
     */
    TupleDesc res_desc = matstate->ss.ps.ps_ResultTupleSlot->tts_tupleDescriptor;
    MemoryContext context = CurrentMemoryContext;
    matstate->m_pCurrentBatch = New(context) VectorBatch(context, res_desc);

    /*
     * Lastly, if this Material node is under subplan and used for materializing
     * Stream(BROADCAST) data, add it to estate->es_material_of_subplan
     * so that it can be run before main plan in ExecuteVectorizedPlan.
     */
    if (IS_PGXC_DATANODE && estate->es_under_subplan && IsA(left_plan, VecStream) &&
        ((VecStream*)left_plan)->type == STREAM_BROADCAST)
        estate->es_material_of_subplan = lappend(estate->es_material_of_subplan, (PlanState*)matstate);

    return matstate;
}

/* ----------------------------------------------------------------
 *		ExecEndMaterial
 * ----------------------------------------------------------------
 */
void ExecEndVecMaterial(VecMaterialState* node)
{
    /*
     * clean out the tuple table
     */
    (void)ExecClearTuple(node->ss.ss_ScanTupleSlot);

    /*
     * Release tuplestore resources
     */
    if (node->batchstorestate != NULL)
        batchstore_end(node->batchstorestate);
    node->batchstorestate = NULL;

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
void ExecVecMaterialMarkPos(VecMaterialState* node)
{
    Assert(node->eflags & EXEC_FLAG_MARK);

    /*
     * if we haven't materialized yet, just return.
     */
    if (!node->batchstorestate) {
        return;
    }

    batchstore_markpos(node->batchstorestate, node->from_memory);
}

/* ----------------------------------------------------------------
 *		ExecMaterialRestrPos
 *
 *		Calls tuplestore to restore the last saved file position.
 * ----------------------------------------------------------------
 */
void ExecVecMaterialRestrPos(VecMaterialState* node)
{
    Assert(node->eflags & EXEC_FLAG_MARK);

    /*
     * if we haven't materialized yet, just return.
     */
    if (!node->batchstorestate) {
        return;
    }

    batchstore_restrpos(node->batchstorestate);
}

/* ----------------------------------------------------------------
 *		ExecReScanMaterial
 *
 *		Rescans the materialized relation.
 * ----------------------------------------------------------------
 */
void ExecVecReScanMaterial(VecMaterialState* node)
{
    int param_no = 0;
    int part_itr = 0;
    ParamExecData* param = NULL;
    bool need_switch_partition = false;

    (void)ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
    node->m_pCurrentBatch->Reset();

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
        if (!node->batchstorestate) {
            if (node->ss.ps.plan->ispwj) {
                VecExecReScan(node->ss.ps.lefttree);
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
        if (node->ss.ps.lefttree->chgParam != NULL || ((uint32)node->eflags & EXEC_FLAG_REWIND) == 0) {
            batchstore_end(node->batchstorestate);
            node->batchstorestate = NULL;
            if (node->ss.ps.lefttree->chgParam == NULL) {
                VecExecReScan(node->ss.ps.lefttree);
            }
            node->eof_underlying = false;
        } else {
            if (node->ss.ps.plan->ispwj && need_switch_partition) {
                batchstore_end(node->batchstorestate);
                node->batchstorestate = NULL;

                VecExecReScan(node->ss.ps.lefttree);
                node->eof_underlying = false;
            } else {
                batchstore_rescan(node->batchstorestate);
            }
        }
    } else {
        /* In this case we are just passing on the subquery's output */
        /*
         * if chgParam of subnode is not null then plan will be re-scanned by
         * first ExecProcNode.
         */
        if (node->ss.ps.lefttree->chgParam == NULL) {
            VecExecReScan(node->ss.ps.lefttree);
        }
        node->eof_underlying = false;
    }
}

/*
 * @Description: Early free the memory for VecMaterial.
 *
 * @param[IN] node:  vector executor state for Material
 * @return: void
 */
void ExecEarlyFreeVecMaterial(VecMaterialState* node)
{
    PlanState* plan_state = &node->ss.ps;

    if (plan_state->earlyFreed) {
        return;
    }

    /*
     * clean out the tuple table
     */
    (void)ExecClearTuple(node->ss.ss_ScanTupleSlot);

    /*
     * Release tuplestore resources
     */
    if (node->batchstorestate != NULL) {
        batchstore_end(node->batchstorestate);
    }
    node->batchstorestate = NULL;

    EARLY_FREE_LOG(elog(LOG,
        "Early Free: After early freeing Material "
        "at node %d, memory used %d MB.",
        plan_state->plan->plan_node_id,
        getSessionMemoryUsageMB()));

    plan_state->earlyFreed = true;
    ExecEarlyFree(outerPlanState(node));
}
