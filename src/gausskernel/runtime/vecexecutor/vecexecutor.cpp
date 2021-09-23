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
 * ---------------------------------------------------------------------------------------
 *
 * vecexecutor.cpp
 *      Vector execution runtime.
 *
 * IDENTIFICATION
 *        Code/src/gausskernel/runtime/vecexecutor/vecexecutor.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "executor/executor.h"
#include "executor/node/nodeAgg.h"
#include "executor/node/nodeHashjoin.h"
#include "executor/node/nodeMaterial.h"
#include "executor/node/nodeRecursiveunion.h"
#include "executor/node/nodeSetOp.h"
#include "executor/node/nodeSort.h"
#include "executor/node/nodeStub.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "nodes/plannodes.h"
#include "vecexecutor/vectorbatch.h"
#include "vecexecutor/vecnodes.h"
#include "vecexecutor/vecstream.h"
#include "pgxc/pgxc.h"
#include "vecexecutor/vecnodedfsindexscan.h"
#include "vecexecutor/vecnodecstorescan.h"
#include "vecexecutor/vecnodecstoreindexscan.h"
#include "vecexecutor/vecnodecstoreindexctidscan.h"
#include "vecexecutor/vecnodecstoreindexheapscan.h"
#include "vecexecutor/vecnodecstoreindexand.h"
#include "vecexecutor/vecnodecstoreindexor.h"
#include "vecexecutor/vecnoderowtovector.h"
#include "vecexecutor/vecnodeforeignscan.h"
#include "vecexecutor/vecremotequery.h"
#include "vecexecutor/vecnoderesult.h"
#include "vecexecutor/vecsubqueryscan.h"
#include "vecexecutor/vecnodesort.h"
#include "vecexecutor/vecmodifytable.h"
#include "vecexecutor/vechashjoin.h"
#include "vecexecutor/vechashagg.h"
#include "vecexecutor/vecpartiterator.h"
#include "vecexecutor/vecappend.h"
#include "vecexecutor/veclimit.h"
#include "vecexecutor/vecsetop.h"
#ifdef ENABLE_MULTIPLE_NODES
#include "vecexecutor/vectsstorescan.h"
#endif   /* ENABLE_MULTIPLE_NODES */
#include "vecexecutor/vecgroup.h"
#include "vecexecutor/vecunique.h"
#include "vecexecutor/vecnestloop.h"
#include "vecexecutor/vecmaterial.h"
#include "vecexecutor/vecmergejoin.h"
#include "vecexecutor/vecwindowagg.h"

extern char* nodeTagToString(NodeTag type);

typedef VectorBatch* (*VectorEngineFunc)(PlanState* node);

VectorBatch* UnSupportVectorRunner(PlanState* node)
{
    ereport(ERROR,
        (errmodule(MOD_VEC_EXECUTOR),
            errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("Unimplemented vector node %s", nodeTagToString(nodeTag(node)))));
    return NULL;
}

VectorEngineFunc VectorEngineRunner[] = {
    UnSupportVectorRunner,
    reinterpret_cast<VectorEngineFunc>(ExecRowToVec),
    reinterpret_cast<VectorEngineFunc>(ExecVecAggregation),
    reinterpret_cast<VectorEngineFunc>(ExecVecHashJoin),
    reinterpret_cast<VectorEngineFunc>(ExecVecStream),
    reinterpret_cast<VectorEngineFunc>(ExecVecSort),
    reinterpret_cast<VectorEngineFunc>(ExecVecForeignScan),
    reinterpret_cast<VectorEngineFunc>(ExecCStoreScan),
    reinterpret_cast<VectorEngineFunc>(ExecDfsScan),
#ifdef ENABLE_MULTIPLE_NODES
    reinterpret_cast<VectorEngineFunc>(ExecTsStoreScan),
#endif   /* ENABLE_MULTIPLE_NODES */
    reinterpret_cast<VectorEngineFunc>(ExecDfsIndexScan),
    reinterpret_cast<VectorEngineFunc>(ExecCstoreIndexScan),
    reinterpret_cast<VectorEngineFunc>(ExecCstoreIndexCtidScan),
    reinterpret_cast<VectorEngineFunc>(ExecCstoreIndexHeapScan),
    reinterpret_cast<VectorEngineFunc>(ExecCstoreIndexAnd),
    reinterpret_cast<VectorEngineFunc>(ExecCstoreIndexOr),
    reinterpret_cast<VectorEngineFunc>(ExecVecRemoteQuery),
    reinterpret_cast<VectorEngineFunc>(ExecVecResult),
    reinterpret_cast<VectorEngineFunc>(ExecVecSubqueryScan),
    reinterpret_cast<VectorEngineFunc>(ExecVecModifyTable),
    reinterpret_cast<VectorEngineFunc>(ExecVecPartIterator),
    reinterpret_cast<VectorEngineFunc>(ExecVecAppend),
    reinterpret_cast<VectorEngineFunc>(ExecVecLimit),
    reinterpret_cast<VectorEngineFunc>(ExecVecGroup),
    reinterpret_cast<VectorEngineFunc>(ExecVecUnique),
    reinterpret_cast<VectorEngineFunc>(ExecVecSetOp),
    reinterpret_cast<VectorEngineFunc>(ExecVecNestloop),
    reinterpret_cast<VectorEngineFunc>(ExecVecMaterial),
    reinterpret_cast<VectorEngineFunc>(ExecVecMergeJoin),
    reinterpret_cast<VectorEngineFunc>(ExecVecWindowAgg),
};

FORCE_INLINE
int GetRunnerIdx(int idx)
{
#ifdef ENABLE_MULTIPLE_NODES
    if (idx > T_VecStartState && idx < T_VecEndState)
#else
    if (idx > T_VecStartState && idx < T_VecEndState && idx != T_VecRemoteQueryState)
#endif
        return idx - T_VecStartState;
    else
        return 0;
}
#ifdef ENABLE_MULTIPLE_NODES
static bool NeedStub(const Plan* node)
{
    return (
        nodeTag(node) == T_Agg || nodeTag(node) == T_ModifyTable || nodeTag(node) == T_VecModifyTable ||
        nodeTag(node) == T_SeqScan || nodeTag(node) == T_CStoreScan || nodeTag(node) == T_CStoreIndexScan ||
        nodeTag(node) == T_CStoreIndexCtidScan || nodeTag(node) == T_CStoreIndexHeapScan || nodeTag(node) == T_Sort ||
        nodeTag(node) == T_Limit || nodeTag(node) == T_PartIterator || nodeTag(node) == T_VecPartIterator ||
        nodeTag(node) == T_Material || nodeTag(node) == T_MergeJoin || nodeTag(node) == T_HashJoin ||
        nodeTag(node) == T_SubqueryScan || nodeTag(node) == T_VecSubqueryScan || nodeTag(node) == T_TsStoreScan
    );
}
#endif   /* ENABLE_MULTIPLE_NODES */
VectorBatch* VectorEngine(PlanState* node)
{
    VectorBatch* result = NULL;
    MemoryContext old_context;

    CHECK_FOR_INTERRUPTS();

    /* Response to stop or cancel signal. */
    if (unlikely(executorEarlyStop()))
        return NULL;
#ifdef ENABLE_MULTIPLE_NODES
    if (IS_PGXC_DATANODE && !NeedExecute(node->plan) && NeedStub(node->plan)) {
        return NULL;
    }
#endif
    Assert(node->vectorized);

    old_context = MemoryContextSwitchTo(node->nodeContext);

    if (node->chgParam != NULL) /* something changed */
        VecExecReScan(node);    /* let ReScan handle this */

    if (node->instrument)
        InstrStartNode(node->instrument);

    t_thrd.pgxc_cxt.GlobalNetInstr = node->instrument;
    result = VectorEngineRunner[GetRunnerIdx(nodeTag(node))](node);
    t_thrd.pgxc_cxt.GlobalNetInstr = NULL;

    if (node->instrument) {
        switch (nodeTag(node)) {
            case T_VecModifyTableState:
                instr_time first_tuple;
                INSTR_TIME_SET_ZERO(first_tuple);
                INSTR_TIME_ACCUM_DIFF(
                    first_tuple, ((VecModifyTableState*)node)->first_tuple_modified, node->instrument->starttime);

                /*
                 * If the value of es_last_processed is zero means the value of es_processed
                 * just come from current operator.	If not means the value of es_processed
                 * come from current operator and other operator, es_processed minus
                 * es_last_processed is tuples processed of curent operator	when modify
                 * the hdfs table, which may include modify the main table and modify the
                 * detla table, in this case, the value of es_processed will be set twice,
                 * resulting in error row value for modify operator in explain command.
                 */
                if (node->state->es_last_processed == 0) {
                    InstrStopNode(node->instrument, node->state->es_processed);
                } else {
                    InstrStopNode(node->instrument, node->state->es_processed - node->state->es_last_processed);
                }

                node->state->es_last_processed = node->state->es_processed;
                node->instrument->firsttuple = INSTR_TIME_GET_DOUBLE(first_tuple);
                break;
            default:
                InstrStopNode(node->instrument, BatchIsNull(result) ? 0.0 : result->m_rows);
                break;
        }
        node->instrument->memoryinfo.operatorMemory = node->plan->operatorMemKB[0];

        if (BatchIsNull(result))
            node->instrument->status = true;
    }

    (void)MemoryContextSwitchTo(old_context);

    return result;
}

/*
 * ExecVecMarkPos
 * Marks the current scan position.
 */
void ExecVecMarkPos(PlanState* node)
{
    switch (nodeTag(node)) {
        case T_VecSortState:
            ExecVecSortMarkPos((VecSortState*)node);
            break;

        case T_VecMaterialState:
            ExecVecMaterialMarkPos((VecMaterialState*)node);
            break;

        case T_CStoreScan:
        case T_CStoreIndexScan:
        case T_CStoreIndexCtidScan:
        case T_CStoreIndexHeapScan:
#ifdef ENABLE_MULTIPLE_NODES
        case T_TsStoreScan:
#endif   /* ENABLE_MULTIPLE_NODES */
            ereport(ERROR,
                (errmodule(MOD_VEC_EXECUTOR),
                    errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("vector scan for VecMarkPos is not yet implemented ")));
            break; /* keep compiler silent */

        case T_VecResult:
            ereport(ERROR,
                (errmodule(MOD_VEC_EXECUTOR),
                    errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("VecResult for VecMarkPos is not yet implemented ")));
            break; /* keep compiler silent */

        default:
            ereport(ERROR,
                (errmodule(MOD_VEC_EXECUTOR),
                    errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("unrecognized node type: %s in function ExecVecMarkPos", nodeTagToString(nodeTag(node)))));
            break; /* keep compiler silent */
    }
}

/*
 * ExecVecRestrPos
 *
 * restores the scan position previously saved with ExecVecMarkPos()
 *
 * NOTE: the semantics of this are that the first VectorEngine following
 * the restore operation will yield the same tuple as the first one following
 * the mark operation.	It is unspecified what happens to the plan node's
 * result TupleTableSlot.  (In most cases the result slot is unchanged by
 * a restore, but the node may choose to clear it or to load it with the
 * restored-to tuple.)	Hence the caller should discard any previously
 * returned TupleTableSlot after doing a restore.
 */
void ExecVecRestrPos(PlanState* node)
{
    switch (nodeTag(node)) {
        case T_VecSortState:
            ExecVecSortRestrPos((VecSortState*)node);
            break;
        case T_VecMaterialState:
            ExecVecMaterialRestrPos((VecMaterialState*)node);
            break;
        case T_CStoreScan:
        case T_CStoreIndexScan:
        case T_CStoreIndexCtidScan:
        case T_CStoreIndexHeapScan:
#ifdef ENABLE_MULTIPLE_NODES
        case T_TsStoreScan:
#endif   /* ENABLE_MULTIPLE_NODES */
            ereport(ERROR,
                (errmodule(MOD_VEC_EXECUTOR),
                    errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("vector scan for VecRestrPos is not yet implemented ")));
            break;
        case T_VecResult:
            ereport(ERROR,
                (errmodule(MOD_VEC_EXECUTOR),
                    errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("VecResult for VecRestrPos is not yet implemented ")));
            break;
        default:
            ereport(ERROR,
                (errmodule(MOD_VEC_EXECUTOR),
                    errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("unrecognized node type: %s in ExecRestrPos", nodeTagToString(nodeTag(node)))));
            break;
    }
}


/*
 * @Description: Entry of early free.
 *
 * @param[IN] node:  PlanState tree paralleling the Plan tree
 * @return: void
 */
void ExecEarlyFree(PlanState* node)
{
    /* Exit if early free policy is not enabled */
    if (!u_sess->attr.attr_sql.enable_early_free || node->state->es_skip_early_free)
        return;

    /*
     * Skip early free if we are under recursive CTE, because the operators under
     * recursive branch, we still have to do ReScan at the end of current iteration,
     * there is some.
     *
     * Note: in the future, this logic could be improved with a more snart early-free
     * mechanism under iterative-execution, e.g. only early free the necessary parts
     * for current iteration but still keep sth for next round, for now we keep it
     * simple& stable for current.
     */
    if (EXEC_IN_RECURSIVE_MODE(node->plan)) {
        elog(DEBUG1,
            "MPP with-recursive skip EarlyFree under for recursive CTE[%d]",
            node->plan->recursive_union_plan_nodeid);

        return;
    }
    ExecEarlyFreeBody(node);
}


/*
 * @Description: Early free the memory for plan nodes.
 *
 * @param[IN] node:  PlanState tree paralleling the Plan tree
 * @return: void
 */
void ExecEarlyFreeBody(PlanState* node)
{
    switch (nodeTag(node)) {
        /* Memory intensive operators, need for early free */
        case T_VecSortState:
            ExecEarlyFreeVecSort((VecSortState*)node);
            break;

        case T_SortState:
            ExecEarlyFreeSort((SortState*)node);
            break;

        case T_VecMaterialState:
            ExecEarlyFreeVecMaterial((VecMaterialState*)node);
            break;

        case T_MaterialState:
            ExecEarlyFreeMaterial((MaterialState*)node);
            break;

        case T_VecAggState:
            ExecEarlyFreeVecAggregation((VecAggState*)node);
            break;

        case T_AggState:
            ExecEarlyFreeAggregation((AggState*)node);
            break;

        case T_VecHashJoinState:
            ExecEarlyFreeVecHashJoin((VecHashJoinState*)node);
            break;

        case T_HashJoinState:
            ExecEarlyFreeHashJoin((HashJoinState*)node);
            break;

        case T_VecSetOpState:
            ExecEarlyFreeVecHashedSetop((VecSetOpState*)node);
            break;

        case T_SetOpState:
            ExecEarlyFreeHashedSetop((SetOpState*)node);
            break;

        /* No need for early free */
        case T_MergeAppendState:
            if (!node->earlyFreed) {
                MergeAppendState* appendState = (MergeAppendState*)node;
                node->earlyFreed = true;
                for (int planNo = 0; planNo < appendState->ms_nplans; planNo++) {
                    ExecEarlyFree(appendState->mergeplans[planNo]);
                }
            }
            break;

        case T_VecAppendState:
        case T_AppendState:
            if (!node->earlyFreed) {
                AppendState* appendState = (AppendState*)node;
                node->earlyFreed = true;
                for (int planNo = 0; planNo < appendState->as_nplans; planNo++) {
                    ExecEarlyFree(appendState->appendplans[planNo]);
                }
            }
            break;

        case T_VecModifyTableState:
        case T_ModifyTableState:
        case T_DistInsertSelectState:
            if (!node->earlyFreed) {
                ModifyTableState* mt = (ModifyTableState*)node;
                node->earlyFreed = true;
                for (int planNo = 0; planNo < mt->mt_nplans; planNo++) {
                    ExecEarlyFree(mt->mt_plans[planNo]);
                }
            }
            break;

        case T_VecSubqueryScanState:
        case T_SubqueryScanState:
            if (!node->earlyFreed) {
                SubqueryScanState* ss = (SubqueryScanState*)node;
                node->earlyFreed = true;
                if (ss->subplan)
                    ExecEarlyFree(ss->subplan);
            }
            break;

        case T_CStoreIndexAndState:
        case T_BitmapAndState:
            if (!node->earlyFreed) {
                BitmapAndState* andState = (BitmapAndState*)node;
                node->earlyFreed = true;
                for (int planNo = 0; planNo < andState->nplans; planNo++) {
                    ExecEarlyFree(andState->bitmapplans[planNo]);
                }
            }
            break;

        case T_CStoreIndexOrState:
        case T_BitmapOrState:
            if (!node->earlyFreed) {
                BitmapOrState* orState = (BitmapOrState*)node;
                node->earlyFreed = true;
                for (int planNo = 0; planNo < orState->nplans; planNo++) {
                    ExecEarlyFree(orState->bitmapplans[planNo]);
                }
            }
            break;

        default:
            if (!node->earlyFreed) {
                node->earlyFreed = true;
                if (outerPlanState(node)) {
                    ExecEarlyFree(outerPlanState(node));
                }

                if (innerPlanState(node)) {
                    ExecEarlyFree(innerPlanState(node));
                }
            }
            break;
    }
}
