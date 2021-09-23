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
 *   vecforeignscan.cpp
 *
 *
 * IDENTIFICATION
 *        Code/src/gausskernel/runtime/vecexecutor/vecnode/vecforeignscan.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "executor/executor.h"
#include "nodes/plannodes.h"
#include "nodes/nodes.h"
#include "nodes/execnodes.h"
#include "foreign/fdwapi.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/memutils.h"
#include "vecexecutor/vecnodes.h"
#include "vecexecutor/vecexecutor.h"
#include "executor/node/nodeForeignscan.h"

static VectorBatch* VecForeignNext(VecForeignScanState* node);
static bool VecForeignRecheck(ForeignScanState* node, VectorBatch* batch);

/* ----------------------------------------------------------------
 *		ExecVecForeignScan(node)
 *
 *		Fetches the next tuple from the FDW, checks local quals, and
 *		returns it.
 *		We call the ExecScan() routine and pass it the appropriate
 *		access method functions.
 * ----------------------------------------------------------------
 */
VectorBatch* ExecVecForeignScan(VecForeignScanState* node)
{
    return ExecVecScan(
        (ScanState*)node, (ExecVecScanAccessMtd)VecForeignNext, (ExecVecScanRecheckMtd)VecForeignRecheck);
}

/* ----------------------------------------------------------------
 *		ExecInitForeignScan
 * ----------------------------------------------------------------
 */
VecForeignScanState* ExecInitVecForeignScan(VecForeignScan* node, EState* estate, int eflags)
{
    VecForeignScanState* scan_state = NULL;
    ForeignScanState* foreign_state = NULL;
    PlanState* plan_state = NULL;
    /*
     * create state structure
     */
    scan_state = makeNode(VecForeignScanState);
    scan_state->ss.ps.plan = (Plan*)node;
    scan_state->ss.ps.state = estate;

    foreign_state = ExecInitForeignScan((ForeignScan*)node, estate, eflags);
    (*(ForeignScanState*)scan_state) = *foreign_state;
    scan_state->ss.ps.type = T_VecForeignScanState;
    scan_state->ss.ps.vectorized = true;
    scan_state->m_scanCxt = AllocSetContextCreate(CurrentMemoryContext,
        "Vec Foreign Scan",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);
    scan_state->m_pCurrentBatch = New(CurrentMemoryContext)
        VectorBatch(CurrentMemoryContext, scan_state->ss.ps.ps_ResultTupleSlot->tts_tupleDescriptor);
    scan_state->m_pScanBatch =
        New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, scan_state->ss.ss_currentRelation->rd_att);
    scan_state->m_values = (Datum*)palloc(sizeof(Datum) * scan_state->m_pScanBatch->m_cols);
    scan_state->m_nulls = (bool*)palloc0(sizeof(bool) * scan_state->m_pScanBatch->m_cols);

    scan_state->ss.ps.targetlist = (List*)ExecInitVecExpr((Expr*)node->scan.plan.targetlist, (PlanState*)scan_state);
    scan_state->ss.ps.qual = (List*)ExecInitVecExpr((Expr*)node->scan.plan.qual, (PlanState*)scan_state);

    plan_state = &(scan_state->ss.ps);
    plan_state->ps_ProjInfo = ExecBuildVecProjectionInfo(plan_state->targetlist,
        node->scan.plan.qual,
        plan_state->ps_ExprContext,
        plan_state->ps_ResultTupleSlot,
        scan_state->ss.ss_ScanTupleSlot->tts_tupleDescriptor);
    ExecAssignVectorForExprEval(plan_state->ps_ExprContext);
    return scan_state;
}

/* ----------------------------------------------------------------
 *		ExecEndVecForeignScan
 *
 *		frees any storage allocated through C routines.
 * ----------------------------------------------------------------
 */
void ExecEndVecForeignScan(VecForeignScanState* node)
{
    MemoryContextDelete(node->m_scanCxt);
    ExecEndForeignScan((ForeignScanState*)node);
}

/* ----------------------------------------------------------------
 *		ForeignNext
 *
 *		This is a workhorse for ExecForeignScan
 * ----------------------------------------------------------------
 */
static VectorBatch* VecForeignNext(VecForeignScanState* node)
{
    VectorBatch* batch = NULL;
    ExprContext* expr_context = node->ss.ps.ps_ExprContext;
    MemoryContext old_context;

    /* Call the Iterate function in short-lived context */
    old_context = MemoryContextSwitchTo(expr_context->ecxt_per_tuple_memory);
    batch = node->fdwroutine->VecIterateForeignScan(node);
    (void)MemoryContextSwitchTo(old_context);

    return batch;
}

static bool VecForeignRecheck(ForeignScanState* node, VectorBatch* batch)
{
    /* There are no access-method-specific conditions to recheck. */
    return true;
}

void ExecReScanVecForeignScan(VecForeignScanState* node)
{
    node->fdwroutine->ReScanForeignScan(node);
    ExecScanReScan(&node->ss);
}
