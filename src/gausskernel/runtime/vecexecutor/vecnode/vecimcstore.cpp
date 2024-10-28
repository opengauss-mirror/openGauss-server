/*
* Copyright (c) 2024 Huawei Technologies Co.,Ltd.
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
 * vecimcstore.cpp
 *    This code provides support for generalized vectorized relation scans.
 *
 * IDENTIFICATION
 *        Code/src/gausskernel/runtime/vecexecutor/vecnode/vecimcstore.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "postgres.h"
#include "codegen/gscodegen.h"
#include "codegen/vecexprcodegen.h"
#include "knl/knl_variable.h"
#include "catalog/pg_partition_fn.h"
#include "access/heapam.h"

#include "executor/node/nodeSamplescan.h"
#include "access/htap/imcstore_am.h"
#include "access/tableam.h"
#include "vecexecutor/vecnodecstorescan.h"
#include "vecexecutor/vecnodeimcstorescan.h"
#include "executor/node/nodeModifyTable.h"

#ifdef ENABLE_HTAP

/* ----------------------------------------------------------------
 *      ExecInitIMCStoreScan
 * ----------------------------------------------------------------
 */
IMCStoreScanState* ExecInitIMCStoreScan(
    IMCStoreScan* node, Relation parent_heap_rel, EState* estate, int eflags, bool codegen_in_up_level)
{
    IMCStoreScanState* scan_stat = NULL;
    PlanState* plan_stat = NULL;
    ScalarDesc unknown_desc;

    // Column store can only be a leaf node
    //
    Assert(outerPlan(node) == NULL);
    Assert(innerPlan(node) == NULL);

    // There is no reverse scan with column store
    //
    Assert(!ScanDirectionIsBackward(estate->es_direction));

    // Create state structure
    //
    scan_stat = makeNode(IMCStoreScanState);

    scan_stat->ps.plan = (Plan*)node;
    scan_stat->ps.state = estate;
    scan_stat->ps.vectorized = true;
    scan_stat->isPartTbl = node->isPartTbl;
    plan_stat = &scan_stat->ps;
    scan_stat->partScanDirection = node->partScanDirection;
    scan_stat->m_isReplicaTable = node->is_replica_table;
    scan_stat->rangeScanInRedis = {false, 0, 0};
    scan_stat->m_isImcstore = true;
    if (!node->tablesample) {
        scan_stat->isSampleScan = false;
    } else {
        ereport(ERROR, (errmodule(MOD_VEC_EXECUTOR), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("Sample scan for IMCStore not support. ")));
    }

    /*
     * Miscellaneous initialization
     *
     * create expression context for node
     */
    ExecAssignExprContext(estate, &scan_stat->ps);

    // Allocate vector for qualification results
    //
    ExecAssignVectorForExprEval(scan_stat->ps.ps_ExprContext);

    // initialize child expressions
    //
    scan_stat->ps.targetlist = (List*)ExecInitVecExpr((Expr*)node->plan.targetlist, (PlanState*)scan_stat);

    /*
     * tuple table initialization
     */
    ExecInitResultTupleSlot(estate, &scan_stat->ps);
    ExecInitScanTupleSlot(estate, (ScanState*)scan_stat);

    /*
     * initialize scan relation
     */
    InitCStoreRelation(scan_stat, estate, false, parent_heap_rel);
    scan_stat->ps.ps_vec_TupFromTlist = false;

#ifdef ENABLE_LLVM_COMPILE
    /*
     * First, not only consider the LLVM native object, but also consider the cost of
     * the LLVM compilation time. We will not use LLVM optimization if there is
     * not enough number of row. Second, consider codegen after we get some information
     * about the scanned relation.
     */
    scan_stat->jitted_vecqual = NULL;
    llvm::Function* jitted_vecqual = NULL;
    dorado::GsCodeGen* llvm_code_gen = (dorado::GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    bool consider_codegen = false;

    /*
     * Check whether we should do codegen here and codegen is allowed for quallist expr.
     * In case of codegen_in_up_level is true, we do not even have to do codegen for target list.
     */
    if (!codegen_in_up_level) {
        consider_codegen = CodeGenThreadObjectReady() &&
            CodeGenPassThreshold(((Plan*)node)->plan_rows, estate->es_plannedstmt->num_nodes, ((Plan*)node)->dop);
        if (consider_codegen) {
            jitted_vecqual = dorado::VecExprCodeGen::QualCodeGen((List*)scan_stat->ps.qual, (PlanState*)scan_stat);
            if (jitted_vecqual != NULL)
                llvm_code_gen->addFunctionToMCJit(jitted_vecqual,
                    reinterpret_cast<void**>(&(scan_stat->jitted_vecqual)));
        }
    }
#endif

    /*
     * Initialize result tuple type and projection info.
     */
    ExecAssignResultTypeFromTL(&scan_stat->ps, scan_stat->ss_ScanTupleSlot->tts_tupleDescriptor->td_tam_ops);

    if (node->isPartTbl && scan_stat->ss_currentRelation == NULL) {
        // no data ,just return;
        return scan_stat;
    }

    /*
     * If we are just doing EXPLAIN (ie, aren't going to run the plan), stop
     * here.  This allows an index-advisor plugin to EXPLAIN a plan containing
     * references to nonexistent indexes.
     */
    if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
        return scan_stat;

    // Init result batch and work batch. Work batch has to contain all columns as qual is
    // running against it.
    // we shall avoid with this after we fix projection elimination.
    //
    scan_stat->m_pCurrentBatch = New(CurrentMemoryContext)
        VectorBatch(CurrentMemoryContext, scan_stat->ps.ps_ResultTupleSlot->tts_tupleDescriptor);
    scan_stat->m_pScanBatch =
        New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, scan_stat->ss_currentRelation->rd_att);
    for (int vecIndex = 0; vecIndex < scan_stat->m_pScanBatch->m_cols; vecIndex++) {
        FormData_pg_attribute* attr = NULL;
        attr = &scan_stat->ss_currentRelation->rd_att->attrs[vecIndex];

        // Hack!! move me out to update pg_attribute instead
        //
        if (attr->atttypid == TIDOID) {
            attr->attlen = sizeof(int64);
            attr->attbyval = true;
        }
    }

    plan_stat->ps_ProjInfo = ExecBuildVecProjectionInfo(plan_stat->targetlist,
        node->plan.qual,
        plan_stat->ps_ExprContext,
        plan_stat->ps_ResultTupleSlot,
        scan_stat->ss_ScanTupleSlot->tts_tupleDescriptor);

    /* Set min/max optimization info to ProjectionInfo's pi_maxOrmin. */
    plan_stat->ps_ProjInfo->pi_maxOrmin = node->minMaxInfo;

    /* If exist sysattrlist, not consider LLVM optimization, while the codegen process will be terminated in
     * VarJittable */
    if (plan_stat->ps_ProjInfo->pi_sysAttrList) {
        scan_stat->m_pScanBatch->CreateSysColContainer(CurrentMemoryContext, plan_stat->ps_ProjInfo->pi_sysAttrList);
    }

#ifdef ENABLE_LLVM_COMPILE
    /**
     * Since we separate the target list elements into simple var references and
     * generic expression, we only need to deal the generic expression with LLVM
     * optimization.
     */
    llvm::Function* jitted_vectarget = NULL;
    if (consider_codegen && plan_stat->ps_ProjInfo->pi_targetlist) {
        /*
         * check if codegen is allowed for generic targetlist expr.
         * Since targetlist is evaluated in projection, add this function to
         * ps_ProjInfo.
         */
        jitted_vectarget =
            dorado::VecExprCodeGen::TargetListCodeGen(plan_stat->ps_ProjInfo->pi_targetlist, (PlanState*)scan_stat);
        if (jitted_vectarget != NULL)
            llvm_code_gen->addFunctionToMCJit(
                jitted_vectarget, reinterpret_cast<void**>(&(plan_stat->ps_ProjInfo->jitted_vectarget)));
    }
#endif

    scan_stat->m_pScanRunTimeKeys = NULL;
    scan_stat->m_ScanRunTimeKeysNum = 0;
    scan_stat->m_ScanRunTimeKeysReady = false;
    scan_stat->csss_ScanKeys = NULL;
    scan_stat->csss_NumScanKeys = 0;

    ExecCStoreBuildScanKeys((CStoreScanState*)scan_stat,
        node->cstorequal,
        &scan_stat->csss_ScanKeys,
        &scan_stat->csss_NumScanKeys,
        &scan_stat->m_pScanRunTimeKeys,
        &scan_stat->m_ScanRunTimeKeysNum);
    scan_stat->m_CStore = New(CurrentMemoryContext) IMCStore();
    scan_stat->m_CStore->InitScan(scan_stat, GetActiveSnapshot());
    OptimizeProjectionAndFilter(scan_stat);

    return scan_stat;
}
#endif /* ENABLE_HTAP */