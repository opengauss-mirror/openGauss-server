/* -------------------------------------------------------------------------
 * Copyright (c) 2026 Huawei Technologies Co.,Ltd.
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
 * src/gausskernel/runtime/executor/nodeHybridRecall.cpp
 *
 * [P0-3] HybridRecall - two-branch recall fusion parent (hybrid recall
 * design doc §8). P0-3 delivers the empty lifecycle only: init wires both
 * branch PlanStates and the candidate context, Exec returns EOF (no
 * planner produces this node yet; the drain/dedup/RRF/Top-K body lands in
 * P0-4+ per the task gate). Rescan resets phase only - parameterized
 * rescans are rejected at plan time (design doc §8.8).
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "executor/executor.h"
#include "executor/node/nodeHybridRecall.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "utils/memutils.h"

static TupleTableSlot* ExecHybridRecall(PlanState* state);

/* ----------------------------------------------------------------
 *		ExecInitHybridRecall
 * ----------------------------------------------------------------
 */
HybridRecallState* ExecInitHybridRecall(HybridRecall* node, EState* estate, int eflags)
{
    /* Cursor mark/restore and backward scan are out of scope for V1
     * (design doc §1.3); reject them here rather than misbehave later. */
    Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

    HybridRecallState* state = makeNode(HybridRecallState);
    state->ps.plan = (Plan*)node;
    state->ps.state = estate;
    state->ps.type = T_HybridRecallState;
    state->ps.ExecProcNode = ExecHybridRecall;

    /* lefttree = text branch, righttree = vector branch (design doc §6.2) */
    state->text_state = ExecInitNode(node->plan.lefttree, estate, eflags);
    state->vector_state = ExecInitNode(node->plan.righttree, estate, eflags);

    /* Candidates live in a dedicated context so that End/Rescan can drop
     * dedup hash, payload copies and the output array in one shot. */
    state->candidate_cxt = AllocSetContextCreate(estate->es_query_cxt,
                                                 "HybridRecallCandidates",
                                                 ALLOCSET_DEFAULT_MINSIZE,
                                                 ALLOCSET_DEFAULT_INITSIZE,
                                                 ALLOCSET_DEFAULT_MAXSIZE);

    state->phase = HYBRID_PHASE_BUILD;
    state->by_doc_id = NULL;
    state->final_heap = NULL;
    state->output = NULL;
    state->output_count = 0;
    state->output_pos = 0;
    state->text_rows = 0;
    state->vector_rows = 0;
    state->dedup_rows = 0;
    state->final_rows = 0;
    state->actual_parallel = false;
    state->serial_fallback = false;

    return state;
}

/* ----------------------------------------------------------------
 *		ExecHybridRecall
 *
 * [P0-3] Empty body: returns EOF immediately. The BUILD phase (drain both
 * branches, dedup by doc key, RRF, final Top-K) is P0-4; until then no
 * planner-generated or handmade plan should rely on this node's output.
 * ----------------------------------------------------------------
 */
static TupleTableSlot* ExecHybridRecall(PlanState* state)
{
    HybridRecallState* node = (HybridRecallState*)state;

    node->phase = HYBRID_PHASE_DONE;
    return ExecClearTuple(node->ps.ps_ResultTupleSlot);
}

/* ----------------------------------------------------------------
 *		ExecEndHybridRecall
 * ----------------------------------------------------------------
 */
void ExecEndHybridRecall(HybridRecallState* node)
{
    /* End both branch subtrees; Stream-owned shared contexts are released
     * by the StreamNodeGroup lifecycle, not here (design doc §8.7). */
    ExecEndNode(node->text_state);
    ExecEndNode(node->vector_state);

    if (node->candidate_cxt != NULL) {
        MemoryContextDelete(node->candidate_cxt);
        node->candidate_cxt = NULL;
        node->by_doc_id = NULL;
        node->final_heap = NULL;
        node->output = NULL;
    }
}

/* ----------------------------------------------------------------
 *		ExecReScanHybridRecall
 *
 * [P0-3] Phase reset only. V1 rejects parameterized rescans at plan time;
 * the full generation-id protocol for late tuples lands with P0-7.
 * ----------------------------------------------------------------
 */
void ExecReScanHybridRecall(HybridRecallState* node)
{
    node->phase = HYBRID_PHASE_BUILD;
    node->output_pos = 0;
    node->text_rows = 0;
    node->vector_rows = 0;
    node->dedup_rows = 0;
    node->final_rows = 0;
}
