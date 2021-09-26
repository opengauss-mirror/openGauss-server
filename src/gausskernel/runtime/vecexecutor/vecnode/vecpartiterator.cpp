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
 * vecpartiterator.cpp
 *
 * IDENTIFICATION
 *        Code/src/gausskernel/runtime/vecexecutor/vecnode/vecpartiterator.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "executor/exec/execdebug.h"
#include "vecexecutor/vecpartiterator.h"
#include "executor/tuptable.h"
#include "utils/memutils.h"
#include "nodes/execnodes.h"
#include "nodes/plannodes.h"
#include "vecexecutor/vectorbatch.h"
#include "vecexecutor/vecexecutor.h"

VecPartIteratorState* ExecInitVecPartIterator(VecPartIterator* node, EState* estate, int eflags)
{
    VecPartIteratorState* state = NULL;

    state = makeNode(VecPartIteratorState);
    state->ps.plan = (Plan*)node;
    state->ps.state = estate;

    /* initiate sub node */
    state->ps.lefttree = ExecInitNode(node->plan.lefttree, estate, eflags);
    state->ps.qual = NULL;
    state->ps.righttree = NULL;
    state->ps.subPlan = NULL;
    state->currentItr = -1;
    state->ps.ps_TupFromTlist = false;
    state->ps.ps_ProjInfo = NULL;
    state->ps.vectorized = true;
    state->ps.ps_ResultTupleSlot = state->ps.lefttree->ps_ResultTupleSlot;

    return state;
}

static int GetVecscanPartitionNum(const PartIteratorState* node)
{
    VecPartIterator* pi_node = (VecPartIterator*)node->ps.plan;
    PlanState* noden = (PlanState*)node->ps.lefttree;
    int partitionScan;
    switch (nodeTag(noden)) {
        case T_CStoreScanState:
            partitionScan = ((CStoreScanState*)noden)->part_id;
            break;
#ifdef ENABLE_MULTIPLE_NODES
        case T_TsStoreScanState:
            partitionScan = ((TsStoreScanState*)noden)->part_id;
            break;
#endif
        case T_DfsIndexScanState:
            partitionScan = ((DfsIndexScanState*)noden)->part_id;
            break;
        case T_CStoreIndexScanState:
            partitionScan = ((CStoreIndexScanState*)noden)->part_id;
            break;
        case T_CStoreIndexCtidScanState:
            partitionScan = ((CStoreIndexCtidScanState*)noden)->part_id;
            break;
        case T_CStoreIndexHeapScanState:
            partitionScan = ((CStoreIndexHeapScanState*)noden)->part_id;
            break;
        default:
            partitionScan = pi_node->itrs;
            break;
    }
    return partitionScan;
}

static void InitVecscanPartition(VecPartIteratorState* node, int partitionScan)
{
    int paramno = 0;
    unsigned int itr_idx = 0;
    VecPartIterator* pi_node = (VecPartIterator*)node->ps.plan;
    ParamExecData* param = NULL;

    Assert(ForwardScanDirection == pi_node->direction || BackwardScanDirection == pi_node->direction);

    /* set iterator parameter */
    node->currentItr++;
    itr_idx = node->currentItr;
    if (BackwardScanDirection == pi_node->direction)
        itr_idx = partitionScan - itr_idx - 1;

    paramno = pi_node->param->paramno;
    param = &(node->ps.state->es_param_exec_vals[paramno]);
    param->isnull = false;
    param->value = (Datum)itr_idx;
    node->ps.lefttree->chgParam = bms_add_member(node->ps.lefttree->chgParam, paramno);

    /* reset the plan node so that next partition can be scanned */
    VecExecReScan(node->ps.lefttree);
}

VectorBatch* ExecVecPartIterator(VecPartIteratorState* node)
{
    VectorBatch* result = NULL;
    EState* state = node->ps.lefttree->state;
    bool orig_early_free = state->es_skip_early_free;

    int partitionScan = GetVecscanPartitionNum(node);
    if (partitionScan == 0) {
        return NULL;
    }

    /* init first scanned partition */
    if (-1 == node->currentItr)
        InitVecscanPartition(node, partitionScan);

    /* For partition wise join, can not early free left tree's caching memory */
    state->es_skip_early_free = true;
    result = VectorEngine(node->ps.lefttree);
    state->es_skip_early_free = orig_early_free;

    if (!BatchIsNull(result))
        return result;

    for (;;) {
        /* if there is no partition to scan, return null */
        if (node->currentItr >= partitionScan - 1)
            return NULL;

        InitVecscanPartition(node, partitionScan);

        /* For partition wise join, can not early free left tree's caching memory */
        orig_early_free = state->es_skip_early_free;
        state->es_skip_early_free = true;

        /* execute the given node to return a(nother) tuple from the special partition */
        result = VectorEngine(node->ps.lefttree);
        state->es_skip_early_free = orig_early_free;

        /* scan the next partition if no tuple returns */
        if (!BatchIsNull(result))
            return result;
    }
}

void ExecEndVecPartIterator(VecPartIteratorState* node)
{
    /* close down subplans */
    ExecEndNode(node->ps.lefttree);
}

void ExecReScanVecPartIterator(VecPartIteratorState* node)
{
    VecPartIterator* pi_node = NULL;
    int paramno = -1;
    ParamExecData* param = NULL;

    /* do nothing if there is no partition to scan */
    int partitionScan = GetVecscanPartitionNum(node);
    if (partitionScan == 0) {
        return;
    }

    node->currentItr = -1;

    pi_node = (VecPartIterator*)node->ps.plan;
    paramno = pi_node->param->paramno;
    param = &(node->ps.state->es_param_exec_vals[paramno]);
    param->isnull = false;
    param->value = (Datum)0;
    node->ps.lefttree->chgParam = bms_add_member(node->ps.lefttree->chgParam, paramno);

    /*
     * if the pruning result isnot null, Reset the subplan node so
     * that its output can be re-scanned.
     */
    VecExecReScan(node->ps.lefttree);
}
