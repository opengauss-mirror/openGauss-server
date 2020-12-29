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
 * vecremotequery.cpp
 *
 *
 * IDENTIFICATION
 *        Code/src/gausskernel/runtime/vecexecutor/vecnode/vecremotequery.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "vecexecutor/vectorbatch.h"
#include "pgxc/execRemote.h"
#include "pgxc/pgxc.h"
#include "vecexecutor/vecnodes.h"
#include "vecexecutor/vecexecutor.h"
#include "utils/batchsort.h"

VectorBatch* ExecVecRemoteQuery(VecRemoteQueryState* node)
{
    PlanState* outer_node = NULL;

    RemoteQueryState* parent = (RemoteQueryState*)node;
    VectorBatch* result_batch = node->resultBatch;
    EState* estate = node->ss.ps.state;
    RemoteQuery* rq = (RemoteQuery*)parent->ss.ps.plan;

    // we assume that we do not do any qual calculation in the vector remotequery node.
    Assert(parent->ss.ps.qual == NULL);
    Assert(!parent->update_cursor);

    if (parent->query_Done == false) {
        if (PLAN_ROUTER == rq->position) {
            if (do_query_for_planrouter(node, true))
                return NULL;
        } else if (SCAN_GATHER == rq->position) {
            do_query_for_scangather(node, true);
        } else {
            do_query(node, true);
        }

        parent->query_Done = true;
    }

    result_batch->Reset();

    if (PLAN_ROUTER == rq->position && true == node->resource_error) {
        outer_node = outerPlanState(outerPlanState(node)); /* skip scangather node */
        result_batch = VectorEngine(outer_node);
    } else if (parent->batchsortstate) {
        batchsort_getbatch((Batchsortstate*)parent->batchsortstate, true, result_batch);
    } else {
        FetchBatch(parent, result_batch);
    }

    /* When finish remote query already, should better reset the flag. */
    if (BatchIsNull(result_batch))
        node->need_error_check = false;

    /* report error if any */
    pgxc_node_report_error(node);
    /*
     *We only handle stream plan && RemoteQuery as root && DML's tag here.
     * Other are handled by ExecModifyTable and ExecutePlan
     */
    if (rq->is_simple &&
        (estate->es_plannedstmt->commandType == CMD_INSERT || estate->es_plannedstmt->commandType == CMD_UPDATE ||
            estate->es_plannedstmt->commandType == CMD_DELETE || estate->es_plannedstmt->commandType == CMD_MERGE))
        estate->es_processed += node->rqs_processed;

    if (!BatchIsNull(result_batch))
        return result_batch;

    /* early free the connection to the compute pool */
    if (PLAN_ROUTER == rq->position) {
        release_conn_to_compute_pool();
    }

    return NULL;
}

VecRemoteQueryState* ExecInitVecRemoteQuery(VecRemoteQuery* node, EState* estate, int eflags)
{
    VecRemoteQueryState* state = NULL;
    RemoteQueryState* rstate = NULL;

    state = makeNode(VecRemoteQueryState);

    rstate = ExecInitRemoteQuery((RemoteQuery*)node, estate, eflags, false);

    state->resultBatch = New(CurrentMemoryContext)
        VectorBatch(CurrentMemoryContext, rstate->ss.ps.ps_ResultTupleSlot->tts_tupleDescriptor);

    *(RemoteQueryState*)state = *rstate;
    state->ss.ps.vectorized = true;
    state->ss.ps.type = T_VecRemoteQueryState;

    estate->es_remotequerystates = lcons(state, estate->es_remotequerystates);
    return state;
}

/*
 * End the remote query
 */
void ExecEndVecRemoteQuery(VecRemoteQueryState* node)
{
    ExecEndRemoteQuery((RemoteQueryState*)node);
}

/* ----------------------------------------------------------------
 *      ExecVecRemoteQueryReScan
 *
 *      Rescans the relation.
 * ----------------------------------------------------------------
 */
void ExecVecRemoteQueryReScan(VecRemoteQueryState* node, ExprContext* exprCtxt)
{
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("VecRemoteQuery ReScan is not yet implemented")));
}
