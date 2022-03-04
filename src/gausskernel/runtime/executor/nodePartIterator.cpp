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
 * ---------------------------------------------------------------------------------------
 *
 *  nodePartIterator.cpp
 *        data partition: routines to support Partition Wise Join
 *
 * IDENTIFICATION
 *        src/gausskernel/runtime/executor/nodePartIterator.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "executor/exec/execdebug.h"
#include "executor/node/nodePartIterator.h"
#include "executor/tuptable.h"
#include "utils/memutils.h"
#include "nodes/execnodes.h"
#include "nodes/plannodes.h"
#include "vecexecutor/vecnodes.h"

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		: initialize PartIterator for partition iteration
 * Description	:
 * Notes		: it is used for partitioned-table only
 */
PartIteratorState* ExecInitPartIterator(PartIterator* node, EState* estate, int eflags)
{
    PartIteratorState* state = NULL;

    state = makeNode(PartIteratorState);
    state->ps.plan = (Plan*)node;
    state->ps.state = estate;

    /* initiate sub node */
    state->ps.lefttree = ExecInitNode(node->plan.lefttree, estate, eflags);
    state->ps.qual = NULL;
    state->ps.righttree = NULL;
    state->ps.subPlan = NULL;
    state->ps.ps_TupFromTlist = false;
    state->ps.ps_ProjInfo = NULL;
    state->currentItr = -1;
    state->subPartCurrentItr = -1;

    return state;
}

static int GetScanPartitionNum(PartIteratorState* node)
{
    PartIterator* pi_node = (PartIterator*)node->ps.plan;
    PlanState* noden = (PlanState*)node->ps.lefttree;
    int partitionScan;
    switch (nodeTag(noden)) {
        case T_SeqScanState:
        case T_IndexScanState:
        case T_IndexOnlyScanState:
        case T_BitmapHeapScanState:
            partitionScan =  ((ScanState*)noden)->part_id;
            break;
        case T_VecToRowState:
            partitionScan = ((VecToRowState*)noden)->part_id;
            break;
        default:
            partitionScan = pi_node->itrs;
            break;
    }
    return partitionScan;
}

void SetPartitionIteratorParamter(PartIteratorState* node, List* subPartLengthList)
{
    if (subPartLengthList != NIL) {
        if (node->currentItr == -1) {
            node->currentItr++;
        }

        int subPartLength = (int)list_nth_int(subPartLengthList, node->currentItr);
        if (node->subPartCurrentItr + 1 >= subPartLength) {
            node->currentItr++;
            node->subPartCurrentItr = -1;
        }
        node->subPartCurrentItr++;
        unsigned int subitr_idx = node->subPartCurrentItr;
        PartIterator* pi_node = (PartIterator*)node->ps.plan;
        int subPartParamno = pi_node->param->subPartParamno;
        ParamExecData* subPartParam = &(node->ps.state->es_param_exec_vals[subPartParamno]);
        subPartParam->isnull = false;
        subPartParam->value = (Datum)subitr_idx;
        node->ps.lefttree->chgParam = bms_add_member(node->ps.lefttree->chgParam, subPartParamno);
    } else {
        node->currentItr++;
    }
}

static void InitScanPartition(PartIteratorState* node, int partitionScan)
{
    int paramno = 0;
    unsigned int itr_idx = 0;
    PartIterator* pi_node = (PartIterator*)node->ps.plan;
    ParamExecData* param = NULL;
    PlanState* noden = (PlanState*)node->ps.lefttree;
    List *subPartLengthList = NIL;
    if (IsA(noden, VecToRowState)) {
        subPartLengthList = ((VecToRowState *)noden)->subPartLengthList;
    } else if (IsA(noden, ScanState) || IsA(noden, SeqScanState) || IsA(noden, IndexOnlyScanState) ||
               IsA(noden, IndexScanState) || IsA(noden, BitmapHeapScanState) || IsA(noden, TidScanState)) {
        subPartLengthList = ((ScanState *)noden)->subPartLengthList;
    }

    Assert(ForwardScanDirection == pi_node->direction || BackwardScanDirection == pi_node->direction);

    /* set iterator parameter */
    SetPartitionIteratorParamter(node, subPartLengthList);

    itr_idx = node->currentItr;
    if (BackwardScanDirection == pi_node->direction)
        itr_idx = partitionScan - itr_idx - 1;

    paramno = pi_node->param->paramno;
    param = &(node->ps.state->es_param_exec_vals[paramno]);
    param->isnull = false;
    param->value = (Datum)itr_idx;
    node->ps.lefttree->chgParam = bms_add_member(node->ps.lefttree->chgParam, paramno);

    /* reset the plan node so that next partition can be scanned */
    ExecReScan(node->ps.lefttree);
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		: Scans the partitioned table with partition iteration and returns
 *			: the next qualifying tuple in the direction specified
 * Description	: partition iteration is a frame of the Planstate for scan a partitioned
 *			: table. it is like a monitor. The real job is done by SeqScan .e.g
 * Notes		:
 */
TupleTableSlot* ExecPartIterator(PartIteratorState* node)
{
    TupleTableSlot* slot = NULL;
    EState* state = node->ps.lefttree->state;
    node->ps.lefttree->do_not_reset_rownum = true;
    bool orig_early_free = state->es_skip_early_free;

    int partitionScan = GetScanPartitionNum(node);
    if (partitionScan == 0) {
        /* return NULL if no partition is selected */
        return NULL;
    }

    /* init first scanned partition */
    if (node->currentItr == -1)
        InitScanPartition(node, partitionScan);

    /* For partition wise join, can not early free left tree's caching memory */
    state->es_skip_early_free = true;
    slot = ExecProcNode(node->ps.lefttree);
    state->es_skip_early_free = orig_early_free;

    if (!TupIsNull(slot))
        return slot;

    /* switch to next partition until we get a unempty tuple */
    for (;;) {
        /* minus wrong rownum */
        node->ps.lefttree->ps_rownum--;

        if (node->currentItr + 1 >= partitionScan) { /* have scanned all partitions */
            PlanState* noden = (PlanState*)node->ps.lefttree;
            List *subPartLengthList = NIL;
            if (IsA(noden, VecToRowState)) {
                subPartLengthList = ((VecToRowState *)noden)->subPartLengthList;
            } else if (IsA(noden, ScanState) || IsA(noden, SeqScanState) || IsA(noden, IndexOnlyScanState) ||
                       IsA(noden, IndexScanState) || IsA(noden, BitmapHeapScanState) || IsA(noden, TidScanState)) {
                subPartLengthList = ((ScanState *)noden)->subPartLengthList;
            }

            if (subPartLengthList != NIL) {
                int subPartLength = (int)list_nth_int(subPartLengthList, node->currentItr);
                if (node->subPartCurrentItr + 1 >= subPartLength) {
                    return NULL;
                }
            } else {
                return NULL;
            }
        }

        /* switch to next partiiton */
        InitScanPartition(node, partitionScan);

        /* For partition wise join, can not early free left tree's caching memory */
        orig_early_free = state->es_skip_early_free;
        state->es_skip_early_free = true;
        slot = ExecProcNode(node->ps.lefttree);
        state->es_skip_early_free = orig_early_free;

        if (!TupIsNull(slot))
            return slot;
    }
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		: clear out the partition iterator
 * Description	:
 * Notes		:
 */
void ExecEndPartIterator(PartIteratorState* node)
{
    /* close down subplans */
    ExecEndNode(node->ps.lefttree);
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		: Reset the partition iterator node so that its output
 *			: can be re-scanned.
 * Description	:
 * Notes		:
 */
void ExecReScanPartIterator(PartIteratorState* node)
{
    PartIterator* pi_node = NULL;
    int paramno = -1;
    ParamExecData* param = NULL;
    int subPartParamno = -1;
    ParamExecData* subPartParam = NULL;

    /* do nothing if there is no partition to scan */
    int partitionScan = GetScanPartitionNum(node);
    if (partitionScan == 0) {
        /* return NULL if no partition is selected */
        return;
    }

    node->currentItr = -1;

    pi_node = (PartIterator*)node->ps.plan;
    paramno = pi_node->param->paramno;
    param = &(node->ps.state->es_param_exec_vals[paramno]);
    param->isnull = false;
    param->value = (Datum)0;
    node->ps.lefttree->chgParam = bms_add_member(node->ps.lefttree->chgParam, paramno);

    node->subPartCurrentItr = -1;

    subPartParamno = pi_node->param->subPartParamno;
    subPartParam = &(node->ps.state->es_param_exec_vals[subPartParamno]);
    subPartParam->isnull = false;
    subPartParam->value = (Datum)0;
    node->ps.lefttree->chgParam = bms_add_member(node->ps.lefttree->chgParam, subPartParamno);

    /*
     * if the pruning result isnot null, Reset the subplan node so
     * that its output can be re-scanned.
     */
    ExecReScan(node->ps.lefttree);
}
