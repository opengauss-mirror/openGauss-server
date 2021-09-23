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
 * vecsubqueryscan.cpp
 *    Support routines for vectorized implementation of scanning subqueries (subselects in rangetable).
 *
 *
 * IDENTIFICATION
 *    Code/src/gausskernel/runtime/vecexecutor/vecnode/vecsubqueryscan.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "executor/exec/execdebug.h"
#include "vecexecutor/vecsubqueryscan.h"
#include "vecexecutor/vecexecutor.h"
#include "executor/node/nodeSubqueryscan.h"

static VectorBatch* VecSubqueryNext(VecSubqueryScanState* node);

/* ----------------------------------------------------------------
 *                      Scan Support
 * ----------------------------------------------------------------
 */
/* ----------------------------------------------------------------
 *      SubqueryNext
 *
 *      This is a workhorse for ExecSubqueryScan
 * ----------------------------------------------------------------
 */
static VectorBatch* VecSubqueryNext(VecSubqueryScanState* node)
{
    VectorBatch* batch = NULL;

    /*
     * Get the next tuple from the sub-query.
     */
    batch = VectorEngine(node->subplan);

    /*
     * We just return the subplan's result slot, rather than expending extra
     * cycles for ExecCopySlot().  (Our own ScanTupleSlot is used only for
     * EvalPlanQual rechecks.)
     */
    return batch;
}

/*
 * SubqueryRecheck -- access method routine to recheck a tuple in EvalPlanQual
 */
static bool vec_sub_query_recheck(VecSubqueryScanState* node, VectorBatch* slot)
{
    /* nothing to check */
    return true;
}

/* ----------------------------------------------------------------
 *      ExecSubqueryScan(node)
 *
 *      Scans the subquery sequentially and returns the next qualifying
 *      tuple.
 *      We call the ExecScan() routine and pass it the appropriate
 *      access method functions.
 * ----------------------------------------------------------------
 */
VectorBatch* ExecVecSubqueryScan(VecSubqueryScanState* node)
{
    return ExecVecScan(&node->ss, (ExecVecScanAccessMtd)VecSubqueryNext, (ExecVecScanRecheckMtd)vec_sub_query_recheck);
}

/* ----------------------------------------------------------------
 *      ExecInitVecSubqueryScan
 * ----------------------------------------------------------------
 */
VecSubqueryScanState* ExecInitVecSubqueryScan(VecSubqueryScan* node, EState* estate, int eflags)
{
    VecSubqueryScanState* vecsubquerystate = NULL;

    /* check for unsupported flags */
    DBG_ASSERT(!(eflags & EXEC_FLAG_MARK));

    /*
     * SubqueryScan should not have any "normal" children.  Also, if planner
     * left anything in subrtable/subrowmark, it's fishy.
     */
    DBG_ASSERT(outerPlan(node) == NULL);
    DBG_ASSERT(innerPlan(node) == NULL);

    /*
     * create state structure
     */
    vecsubquerystate = makeNode(VecSubqueryScanState);
    vecsubquerystate->ss.ps.plan = (Plan*)node;
    vecsubquerystate->ss.ps.state = estate;
    vecsubquerystate->ss.ps.vectorized = true;

    /*
     * Miscellaneous initialization
     *
     * create expression context for node
     */
    ExecAssignExprContext(estate, &vecsubquerystate->ss.ps);

    /*
     * initialize child expressions
     */
    vecsubquerystate->ss.ps.targetlist =
        (List*)ExecInitVecExpr((Expr*)node->scan.plan.targetlist, (PlanState*)vecsubquerystate);
    vecsubquerystate->ss.ps.qual = (List*)ExecInitVecExpr((Expr*)node->scan.plan.qual, (PlanState*)vecsubquerystate);

    /*
     * tuple table initialization
     */
    ExecInitResultTupleSlot(estate, &vecsubquerystate->ss.ps);
    ExecInitScanTupleSlot(estate, &vecsubquerystate->ss);

    /*
     * initialize subquery
     */
    vecsubquerystate->subplan = ExecInitNode(node->subplan, estate, eflags);

    vecsubquerystate->ss.ps.ps_TupFromTlist = false;

    /*
     * Initialize scan tuple type (needed by ExecAssignScanProjectionInfo)
     */
    ExecAssignScanType(&vecsubquerystate->ss, ExecGetResultType(vecsubquerystate->subplan));

    /*
     * Initialize result tuple type and projection info.
     */
    ExecAssignResultTypeFromTL(
            &vecsubquerystate->ss.ps,
            vecsubquerystate->ss.ss_ScanTupleSlot->tts_tupleDescriptor->tdTableAmType);

    ExecAssignVecScanProjectionInfo(&vecsubquerystate->ss);

    // Allocate vector for qualification results
    //
    ExecAssignVectorForExprEval(vecsubquerystate->ss.ps.ps_ExprContext);

    return vecsubquerystate;
}

/* ----------------------------------------------------------------
 *      ExecEndVecSubqueryScan
 *
 *      frees any storage allocated through C routines.
 * ----------------------------------------------------------------
 */
void ExecEndVecSubqueryScan(VecSubqueryScanState* node)
{
    ExecEndSubqueryScan((SubqueryScanState*)node);
}

/* ----------------------------------------------------------------
 *		ExecReScanVecSubqueryScan
 *
 *		Rescans the relation.
 * ----------------------------------------------------------------
 */
void ExecReScanVecSubqueryScan(VecSubqueryScanState* node)
{
    ExecScanReScan(&node->ss);

    /*
     * ExecReScan doesn't know about my subplan, so I have to do
     * changed-parameter signaling myself.	This is just as well, because the
     * subplan has its own memory context in which its chgParam state lives.
     */
    if (node->ss.ps.chgParam != NULL)
        UpdateChangedParamSet(node->subplan, node->ss.ps.chgParam);

    /*
     * if chgParam of subnode is not null then plan will be re-scanned by
     * first ExecProcNode.
     */
    if (node->subplan->chgParam == NULL)
        VecExecReScan(node->subplan);
}
