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
 * veccstoreindexheapscan.cpp
 *    Support routines for indexed-heap scans of column stores.
 *
 * IDENTIFICATION
 *        Code/src/gausskernel/runtime/vecexecutor/vecnode/veccstoreindexheapscan.cpp
 *
 * -------------------------------------------------------------------------
 */


/*
 * INTERFACE ROUTINES
 * 	ExecInitCstoreIndexHeapScan		creates and initializes a cstoreindexheapscan node.
 *	ExecCstoreIndexHeapScan			scans a column store with heap-indexed.
    ExecEndCstoreIndexHeapScan		release any storage allocated
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "access/relscan.h"
#include "access/cstore_am.h"
#include "executor/exec/execdebug.h"
#include "vecexecutor/vecexecutor.h"
#include "vecexecutor/vecnodecstoreindexscan.h"
#include "vecexecutor/vecnodecstoreindexheapscan.h"
#include "vecexecutor/vecnodecstorescan.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"

/* ----------------------------------------------------------------
 *      ExecInitCstoreIndexHeapScan
 *
 *      Initializes the index scan's state information, creates
 *      scan keys, and opens the base and index relations.
 *
 *      Note: it will get the ctid list returned by the child node
 *      and scan the base cstore table by the ctid list.
 * ----------------------------------------------------------------
 */
CStoreIndexHeapScanState* ExecInitCstoreIndexHeapScan(CStoreIndexHeapScan* node, EState* estate, int eflags)
{
    CStoreIndexHeapScanState* CIHSState = NULL;
    CStoreScan* cstorescan = NULL;
    CStoreScanState* cstorescanstate = NULL;
    errno_t rc;

    /*
     * Create state structure for column store scan. This column store
     * scan shares the target list, qualification etc with the index scan
     * plan but shall set selectionRatio and cstorequal correctly.
     */
    cstorescan = makeNode(CStoreScan);
    cstorescan->plan = node->scan.plan;
    cstorescan->plan.lefttree = NULL;
    cstorescan->scanrelid = node->scan.scanrelid;
    cstorescan->selectionRatio = 0.01;
    cstorescan->isPartTbl = node->scan.isPartTbl;
    cstorescan->itrs = node->scan.itrs;
    cstorescan->pruningInfo = node->scan.pruningInfo;
    cstorescan->partScanDirection = node->scan.partScanDirection;

    cstorescanstate = ExecInitCStoreScan(cstorescan, NULL, estate, eflags, false, true);

    /*
     * Create state structure for current plan using the column store
     * state we just created. The trick here is to still hang the original
     * plan node here for EXPLAIN purpose (execution does not need it).
     */
    CIHSState = makeNode(CStoreIndexHeapScanState);
    rc = memcpy_s(CIHSState, sizeof(CStoreScanState), cstorescanstate, sizeof(CStoreScanState));
    securec_check(rc, "", "");
    CIHSState->ps.plan = (Plan*)node;
    CIHSState->ps.state = estate;
    CIHSState->ps.vectorized = true;
    CIHSState->ps.type = T_CStoreIndexHeapScanState;
    CIHSState->m_deltaQual = (List*)ExecInitExpr((Expr*)node->bitmapqualorig, (PlanState*)&CIHSState->ps);

    outerPlanState(CIHSState) = ExecInitNode(outerPlan(node), estate, eflags);

    return CIHSState;
}

/* ----------------------------------------------------------------
 *	                 ExecCstoreIndexHeapScan
 * ----------------------------------------------------------------
 */
VectorBatch* ExecCstoreIndexHeapScan(CStoreIndexHeapScanState* state)
{
    VectorBatch* tids = NULL;
    VectorBatch* pScanBatch = NULL;
    VectorBatch* pOutBatch = NULL;

    pOutBatch = state->m_pCurrentBatch;
    pScanBatch = state->m_pScanBatch;

    pOutBatch->m_rows = 0;

    // update cstore scan timing flag
    state->m_CStore->SetTiming((CStoreScanState*)state);

    ExprDoneCond isDone = ExprSingleResult;
    /*
     * for function-returning-set.
     */
    if (state->ps.ps_TupFromTlist) {
        Assert(state->ps.ps_ProjInfo);
        pOutBatch = ExecVecProject(state->ps.ps_ProjInfo, true, &isDone);
        if (pOutBatch->m_rows > 0) {
            return pOutBatch;
        }

        state->ps.ps_TupFromTlist = false;
    }
    state->ps.ps_ProjInfo->pi_exprContext->current_row = 0;

restart:
    // First get target tids through index scan
    //
    pScanBatch->Reset(true);
    pOutBatch->Reset(true);

    tids = VectorEngine(outerPlanState(state));
    if (!BatchIsNull(tids)) {
        // Get actual column through tids scan
        //
        DBG_ASSERT(tids->m_cols >= 1);
        state->m_CStore->ScanByTids(state, tids, pScanBatch);
        state->m_CStore->ResetLateRead();

        pScanBatch->FixRowCount();

        // Apply quals and projections
        //
        pOutBatch = ApplyProjectionAndFilter(state, pScanBatch, &isDone);

        if (isDone != ExprEndResult) {
            state->ps.ps_TupFromTlist = (isDone == ExprMultipleResult);
        }

        if (BatchIsNull(pOutBatch))
            goto restart;
    } else {
        ScanDeltaStore((CStoreScanState*)state, pScanBatch, state->m_deltaQual);

        /* delta data is consumed */
        if (pScanBatch->m_rows == 0)
            return pOutBatch;

        pScanBatch->FixRowCount();

        pOutBatch = ApplyProjectionAndFilter(state, pScanBatch, &isDone);

        if (isDone != ExprEndResult) {
            state->ps.ps_TupFromTlist = (isDone == ExprMultipleResult);
        }

        if (BatchIsNull(pOutBatch))
            goto restart;
    }

    return pOutBatch;
}

/* ----------------------------------------------------------------
 *	                 ExecEndCstoreIndexHeapScan
 * ----------------------------------------------------------------
 */
void ExecEndCstoreIndexHeapScan(CStoreIndexHeapScanState* state)
{
    ExecEndNode(outerPlanState(state));
    ExecEndCStoreScan((CStoreScanState*)state, false);
}

/* ----------------------------------------------------------------
 *	                 ExecReScanCstoreIndexHeapScan
 * ----------------------------------------------------------------
 */
void ExecReScanCstoreIndexHeapScan(CStoreIndexHeapScanState* state)
{
    VecExecReScan(outerPlanState(state));
    ExecReScanCStoreScan((CStoreScanState*)state);
}
