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
 * vecappend.cpp
 *    routines to handle vectorized append nodes.
 *
 * IDENTIFICATION
 *        Code/src/gausskernel/runtime/vecexecutor/vecnode/vecappend.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
/* INTERFACE ROUTINES
 *		ExecInitVecAppend	- initialize the append node
 *		ExecVecAppend		- retrieve the next batch from the node
 *		ExecEndVecAppend	- shut down the append node
 *		ExecReScanVecAppend - rescan the append node
 *
 *	 NOTES
 *		This implementation follows the same logic as row based append and
 *      we can even reuse some code. See notes in nodeAppend.cpp.
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "executor/execdebug.h"
#include "executor/nodeAppend.h"
#include "vecexecutor/vecappend.h"
#include "vecexecutor/vecexecutor.h"

/* ----------------------------------------------------------------
 *		ExecInitVecAppend
 *
 *		Begin all of the subscans of the append node.
 * ----------------------------------------------------------------
 */
VecAppendState* ExecInitVecAppend(VecAppend* node, EState* estate, int eflags)
{
    VecAppendState* appendstate = makeNode(VecAppendState);
    PlanState** appendplanstates;
    int nplans;
    int i;
    ListCell* lc = NULL;

    /* check for unsupported flags */
    Assert(!(eflags & EXEC_FLAG_MARK));

    /*
     * Set up empty vector of subplan states
     */
    nplans = list_length(node->appendplans);

    appendplanstates = (PlanState**)palloc0(nplans * sizeof(PlanState*));

    /*
     * create new AppendState for our append node
     */
    appendstate->ps.plan = (Plan*)node;
    appendstate->ps.state = estate;
    appendstate->appendplans = appendplanstates;
    appendstate->as_nplans = nplans;
    appendstate->ps.vectorized = true;

    /*
     * Miscellaneous initialization
     *
     * Append plans don't have expression contexts because they never call
     * ExecQual or ExecProject.
     */
    ExecInitResultTupleSlot(estate, &appendstate->ps);
    /*
     * call ExecInitNode on each of the plans to be executed and save the
     * results into the array "appendplans".
     */
    i = 0;
    foreach (lc, node->appendplans) {
        Plan* initNode = (Plan*)lfirst(lc);

        appendplanstates[i] = ExecInitNode(initNode, estate, eflags);
        i++;
    }

    ExecAssignResultTypeFromTL(&appendstate->ps);
    appendstate->ps.ps_ProjInfo = NULL;

    /*
     * Parallel-aware append plans must choose the first subplan to execute by
     * looking at shared memory, but non-parallel-aware append plans can
     * always start with the first subplan.
     */
    appendstate->as_whichplan = appendstate->ps.plan->parallel_aware ? INVALID_SUBPLAN_INDEX : 0;

    /* If parallel-aware, this will be overridden later. */
    appendstate->choose_next_subplan = choose_next_subplan_locally;

    return appendstate;
}

/* ----------------------------------------------------------------
 *	   ExecVecAppend
 *
 *		Handles iteration over multiple subplans.
 * ----------------------------------------------------------------
 */
VectorBatch* ExecVecAppend(VecAppendState* node)
{
    /* If no subplan has been chosen, we must choose one before proceeding. */
    if (node->as_whichplan == INVALID_SUBPLAN_INDEX && !node->choose_next_subplan(node))
        return NULL;

    for (;;) {
        PlanState* subnode = NULL;
        VectorBatch* result = NULL;

        /*
         * figure out which subplan we are currently processing
         */
        Assert(node->as_whichplan >= 0 && node->as_whichplan < node->as_nplans);
        subnode = node->appendplans[node->as_whichplan];

        /*
         * get a tuple from the subplan
         */
        result = VectorEngine(subnode);

        if (!BatchIsNull(result)) {
            /*
             * If the subplan gave us something then return it as-is. We do
             * NOT make use of the result slot that was set up in
             * ExecInitAppend; there's no need for it.
             */
            return result;
        }

        /* Early free each of subplans after finishing execution */
        ExecEarlyFree(subnode);

        /* choose new subplan; if none, we're done */
        if (!node->choose_next_subplan(node))
            return NULL;
    }
}

/* ----------------------------------------------------------------
 *		ExecEndVecAppend
 *
 *		Shuts down the subscans of the append node.
 *		Returns nothing of interest.
 * ----------------------------------------------------------------
 */
void ExecEndVecAppend(VecAppendState* node)
{
    // Nothing special to handle, so reuse append code
    //
    ExecEndAppend(node);
}

/* ----------------------------------------------------------------
 *		ExecReScanVecAppend
 *
 *		Rescan the append node.
 * ----------------------------------------------------------------
 */
void ExecReScanVecAppend(VecAppendState* node)
{
    int i;

    for (i = 0; i < node->as_nplans; i++) {
        PlanState* subnode = node->appendplans[i];

        /*
         * ExecReScan doesn't know about my subplans, so I have to do
         * changed-parameter signaling myself.
         */
        if (node->ps.chgParam != NULL)
            UpdateChangedParamSet(subnode, node->ps.chgParam);

        /*
         * If chgParam of subnode is not null then plan will be re-scanned by
         * first ExecProcNode.
         */
        if (subnode->chgParam == NULL)
            VecExecReScan(subnode);
    }
    node->as_whichplan = node->ps.plan->parallel_aware ? INVALID_SUBPLAN_INDEX : 0;
}
