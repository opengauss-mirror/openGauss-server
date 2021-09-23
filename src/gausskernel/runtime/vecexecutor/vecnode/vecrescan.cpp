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
 * vecrescan.cpp
 *	  miscellaneous executor access method routines
 *
 * IDENTIFICATION
 *        Code/src/gausskernel/runtime/vecexecutor/vecnode/vecrescan.cpp
 *
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "vecexecutor/vecexecutor.h"
#include "executor/executor.h"
#include "executor/node/nodeSubplan.h"
#include "vecexecutor/vecnodecstorescan.h"
#include "vecexecutor/vecpartiterator.h"
#include "storage/cstore/cstore_compress.h"
#include "access/cstore_am.h"
#include "executor/node/nodeModifyTable.h"
#include "vecexecutor/vecnoderowtovector.h"
#include "vecexecutor/vecnestloop.h"
#include "vecexecutor/vecmaterial.h"
#include "vecexecutor/vecunique.h"
#include "vecexecutor/vecgroup.h"
#include "vecexecutor/veclimit.h"
#include "vecexecutor/vecremotequery.h"
#include "vecexecutor/vecnodesort.h"
#include "vecexecutor/vecsetop.h"
#include "vecexecutor/vecmergejoin.h"
#include "vecexecutor/vecnoderesult.h"
#include "vecexecutor/vecmodifytable.h"
#include "vecexecutor/vecsubqueryscan.h"
#include "vecexecutor/vecappend.h"
#include "vecexecutor/vechashjoin.h"
#include "vecexecutor/vechashagg.h"
#include "vecexecutor/vecnodeforeignscan.h"
#include "vecexecutor/vecnodecstoreindexheapscan.h"
#include "vecexecutor/vecnodecstoreindexctidscan.h"
#include "vecexecutor/vecnodecstoreindexand.h"
#include "vecexecutor/vecnodecstoreindexor.h"
#include "vecexecutor/vecnodevectorow.h"
#include "vecexecutor/vecnodedfsindexscan.h"
#ifdef ENABLE_MULTIPLE_NODES
#include "vecexecutor/vectsstorescan.h"
#endif   /* ENABLE_MULTIPLE_NODES */
#include "vecexecutor/vecwindowagg.h"

/*
 * VecExecReScan
 *		Reset a plan node so that its output can be re-scanned.
 *
 * Note that if the plan node has parameters that have changed value,
 * the output might be different from last time.
 */
void VecExecReScan(PlanState* node)
{
    /* If collecting timing stats, update them */
    if (node->instrument)
        InstrEndLoop(node->instrument);

    if (node->chgParam != NULL) {
        ListCell* l = NULL;

        foreach (l, node->initPlan) {
            SubPlanState* sstate = (SubPlanState*)lfirst(l);
            PlanState* splan = sstate->planstate;

            if (NULL == splan) {
                continue;
            }

            if (splan->plan->extParam != NULL) {/* don't care about child
                                                * local Params */
                UpdateChangedParamSet(splan, node->chgParam);
            }

            if (splan->chgParam != NULL) {
                ExecReScanSetParamPlan(sstate, node);
            }
        }
        foreach (l, node->subPlan) {
            SubPlanState* sstate = (SubPlanState*)lfirst(l);
            PlanState* splan = sstate->planstate;

            if (splan->plan->extParam != NULL) {
                UpdateChangedParamSet(splan, node->chgParam);
            }
        }
        /* Well. Now set chgParam for left/right trees. */
        if (node->lefttree != NULL) {
            UpdateChangedParamSet(node->lefttree, node->chgParam);
        }

        if (node->righttree != NULL) { 
            UpdateChangedParamSet(node->righttree, node->chgParam);
        }
    }

    /* Shut down any SRFs in the plan node's targetlist */
    if (node->ps_ExprContext)
        ReScanExprContext(node->ps_ExprContext);

    if (unlikely(planstate_need_stub(node))) {
        /*
         * skip real initialization if it is not in exec-nodes just as we do in
         * ExecReScan, since we doesnot do real operator initialization in
         * ExecInitNode when current datanode does not in the specail
         * operator's exec-nodes
         */
        goto VecEndExecRescan;
    }

    /* And do node-type-specific processing */
    switch (nodeTag(node)) {
        case T_VecToRowState:
            ExecReScanVecToRow((VecToRowState*)node);
            break;
        case T_CStoreScanState:
            ExecReScanCStoreScan((CStoreScanState*)node);
            break;
        case T_DfsScanState:
            ExecReScanDfsScan((DfsScanState*)node);
            break;
#ifdef ENABLE_MULTIPLE_NODES
        case T_TsStoreScanState:
            ExecReScanTsStoreScan((TsStoreScanState*)node);
            break;
#endif   /* ENABLE_MULTIPLE_NODES */
        /*
         * partition iterator node
         */
        case T_VecPartIteratorState:
            ExecReScanVecPartIterator((VecPartIteratorState*)node);
            break;
        case T_VecNestLoopState:
            ExecReScanVecNestLoop((VecNestLoopState*)node);
            break;
        case T_RowToVecState:
            ExecReScanRowToVec((RowToVecState*)node);
            break;
        case T_VecGroupState:
            ExecReScanVecGroup((VecGroupState*)node);
            break;
        case T_VecUniqueState:
            ExecReScanVecUnique((VecUniqueState*)node);
            break;
        case T_VecMaterialState:
            ExecVecReScanMaterial((VecMaterialState*)node);
            break;
        case T_VecLimitState:
            ExecReScanVecLimit((VecLimitState*)node);
            break;
        case T_VecRemoteQueryState:
            ExecVecRemoteQueryReScan((VecRemoteQueryState*)node, node->ps_ExprContext);
            break;
        case T_VecSortState:
            ExecReScanVecSort((VecSortState*)node);
            break;
        case T_DfsIndexScanState:
            ExecReScanDfsIndexScan((DfsIndexScanState*)node);
            break;
        case T_CStoreIndexScanState:
            ExecReScanCStoreIndexScan((CStoreIndexScanState*)node);
            break;
        case T_VecSetOpState:
            ExecReScanVecSetOp((VecSetOpState*)node);
            break;

        case T_VecMergeJoinState:
            ExecReScanVecMergeJoin((VecMergeJoinState*)node);
            break;
        case T_VecResultState:
            ExecReScanVecResult((VecResultState*)node);
            break;
        case T_VecModifyTableState:
            ExecReScanVecModifyTable((VecModifyTableState*)node);
            break;
        case T_VecSubqueryScanState:
            ExecReScanVecSubqueryScan((VecSubqueryScanState*)node);
            break;
        case T_VecAppendState:
            ExecReScanVecAppend((VecAppendState*)node);
            break;
        case T_VecHashJoinState:
            ExecReScanVecHashJoin((VecHashJoinState*)node);
            break;
        case T_VecAggState:
            ExecReScanVecAggregation((VecAggState*)node);
            break;
        case T_VecForeignScanState:
            ExecReScanVecForeignScan((VecForeignScanState*)node);
            break;
        case T_CStoreIndexHeapScanState:
            ExecReScanCstoreIndexHeapScan((CStoreIndexHeapScanState*)node);
            break;
        case T_CStoreIndexCtidScanState:
            ExecReScanCstoreIndexCtidScan((CStoreIndexCtidScanState*)node);
            break;
        case T_CStoreIndexAndState:
            ExecReScanCstoreIndexAnd((CStoreIndexAndState*)node);
            break;
        case T_CStoreIndexOrState:
            ExecReScanCstoreIndexOr((CStoreIndexOrState*)node);
            break;
        case T_VecWindowAggState:
            ExecReScanVecWindowAgg((VecWindowAggState*)node);
            break;
        default:
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmodule(MOD_VEC_EXECUTOR),
                    errmsg("unrecognized node type: %s", nodeTagToString(nodeTag(node)))));
            break;
    }

VecEndExecRescan:
    if (node->chgParam != NULL) {
        bms_free_ext(node->chgParam);
        node->chgParam = NULL;
    }
}
