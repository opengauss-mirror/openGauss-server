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
 * vecunique.cpp
 *    Prototypes for vectorized unique
 *
 *
 * IDENTIFICATION
 *        Code/src/gausskernel/runtime/vecexecutor/vecnode/vecunique.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "executor/executor.h"
#include "executor/node/nodeUnique.h"
#include "vecexecutor/vecunique.h"
#include "vecexecutor/vecgrpuniq.h"

VectorBatch* ExecVecUnique(VecUniqueState* node)
{
    PlanState* outer_plan = outerPlanState(node);
    void** uniq_container = node->container;
    uint16 i = 0;
    uint16* idx = &(node->idx);
    Encap* cap = (Encap*)node->cap;
    cap->eqfunctions = node->eqfunctions;
    errno_t rc;

    if (node->uniqueDone) {
        return NULL;
    }

    node->bckBuf->Reset();
    node->scanBatch->Reset(true);

    for (;;) {
        // Get next batch, if it is null then break the loop.
        VectorBatch* batch = VectorEngine(outer_plan);
        if (unlikely(BatchIsNull(batch)))
            break;

        // Call the buildFunc to read batch into uniq_container using idx as the current position which is started from
        // 0.
        cap->batch = batch;
        FunctionCall2(node->buildFunc, PointerGetDatum(node), PointerGetDatum(cap));

        // When the uniq_container is full, call buildScanFunc to dump BatchMaxSize cells from uniq_container to scanBatch
        // which is used to return, then shift the idx to start from the remain cells and reset the ones in the tail.
        if (*idx >= BatchMaxSize) {
            cap->batch = node->scanBatch;
            for (i = 0; i < BatchMaxSize; i++) {
                FunctionCall2(node->buildScanFunc, PointerGetDatum(uniq_container[i]), PointerGetDatum(cap));
            }
            uint16 remain_uniq = *idx - BatchMaxSize + 1;
            for (i = 0; i < remain_uniq; i++) {
                uniq_container[i] = uniq_container[BatchMaxSize + i];
            }
            rc = memset_s(&uniq_container[remain_uniq],
                sizeof(GUCell*) * (2 * BatchMaxSize - remain_uniq),
                0,
                sizeof(GUCell*) * (2 * BatchMaxSize - remain_uniq));
            securec_check(rc, "\0", "\0");
            *idx = remain_uniq - 1;
            return node->scanBatch;
        }
    }

    // means no data.
    if (0 == *idx && NULL == uniq_container[0]) {
        return NULL;
    }

    cap->batch = node->scanBatch;
    for (i = 0; i <= *idx; i++) {
        FunctionCall2(node->buildScanFunc, PointerGetDatum(uniq_container[i]), PointerGetDatum(cap));
    }

    node->uniqueDone = true;
    return node->scanBatch;
}

VecUniqueState* ExecInitVecUnique(VecUnique* node, EState* estate, int eflags)
{
    VecUniqueState* uniquestate = NULL;
    int cols = node->numCols;

    // check for unsupported flags
    Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

    // create state structure
    uniquestate = makeNode(VecUniqueState);
    uniquestate->ps.plan = (Plan*)node;
    uniquestate->ps.state = estate;
    uniquestate->ps.vectorized = true;
    uniquestate->uniqueDone = false;

    // Tuple table initialization
    ExecInitResultTupleSlot(estate, &uniquestate->ps);

    // then initialize outer plan
    outerPlanState(uniquestate) = ExecInitNode(outerPlan(node), estate, eflags);

    // unique nodes do no projections, so initialize projection info for this
    // node appropriately
    ExecAssignResultTypeFromTL(
            &uniquestate->ps,
            ExecGetResultType(outerPlanState(uniquestate))->tdTableAmType);

    uniquestate->ps.ps_ProjInfo = NULL;

    // Precompute fmgr lookup data for inner loop
    uniquestate->eqfunctions = execTuplesMatchPrepare(node->numCols, node->uniqOperators);

    // Initialize the variables used in Vector execution process and bind the function in FmgrInfo.
    uniquestate->cap = (void*)palloc(sizeof(Encap));
    InitGrpUniq<VecUniqueState>(uniquestate, cols, node->uniqColIdx);

    return uniquestate;
}

void ExecEndVecUnique(VecUniqueState* node)
{
    ExecEndNode(outerPlanState(node));
}

void ExecReScanVecUnique(VecUniqueState* node)
{
    node->uniqueDone = false;
    ReScanGrpUniq<VecUniqueState>(node);

    // if chgParam of subnode is not null then plan will be re-scanned by
    // first ExecProcNode.
    if (node->ps.lefttree->chgParam == NULL)
        VecExecReScan(node->ps.lefttree);
}
