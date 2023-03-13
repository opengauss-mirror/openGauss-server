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
 * veccstoreindexscan.cpp
 *    Routines to support indexed scans of column store relations
 *
 * IDENTIFICATION
 *        Code/src/gausskernel/runtime/vecexecutor/vecnode/veccstoreindexscan.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "codegen/gscodegen.h"
#include "codegen/vecexprcodegen.h"
#include "postgres.h"
#include "knl/knl_variable.h"

#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "access/genam.h"
#include "access/tableam.h"
#include "access/relscan.h"
#include "executor/exec/execdebug.h"
#include "executor/node/nodeIndexonlyscan.h"
#include "executor/node/nodeIndexscan.h"
#include "optimizer/clauses.h"
#include "utils/array.h"
#include "utils/batchsort.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"
#include "utils/snapmgr.h"
#include "vecexecutor/vecnodecstorescan.h"
#include "vecexecutor/vecnodecstoreindexscan.h"
/* remove it in future */
#include "nodes/makefuncs.h"
#include "executor/executor.h"

extern bool CodeGenThreadObjectReady();
extern bool CodeGenPassThreshold(double rows, int dn_num, int dop);
extern void ReScanDeltaRelation(CStoreScanState* node);

static List* FixIndexScanTargetList(CStoreIndexScan* node, CStoreIndexScanState* indexScanState, List* idxTargetList,
    Relation indexRel, Relation heapRel);

template <IndexType type>
VectorBatch* ExecCstoreIndexScanT(CStoreIndexScanState* state)
{
    /* never reach here */
    Assert(false);
    return NULL;
}

template <>
VectorBatch* ExecCstoreIndexScanT<BTREE_INDEX>(CStoreIndexScanState* state)
{
    VectorBatch* tids = NULL;
    IndexSortState* sort = NULL;
    IndexScanDesc scandesc = NULL;
    List* indexTList = NIL;

    CBTreeScanState* btreeIndexScanState = state->m_btreeIndexScan;
    Assert(btreeIndexScanState != NULL);

    /* fetch the sort state for btree index */
    sort = btreeIndexScanState->m_sort;
    scandesc = btreeIndexScanState->iss_ScanDesc;
    indexTList = btreeIndexScanState->m_indexScanTList;

    /* set runtime scan keys */
    if (btreeIndexScanState->iss_NumRuntimeKeys != 0 && !btreeIndexScanState->iss_RuntimeKeysReady) {
        if (btreeIndexScanState->ss.isPartTbl) {
            if (PointerIsValid(btreeIndexScanState->ss.partitions)) {
                btreeIndexScanState->ss.ss_ReScan = true;
                VecExecReScan((PlanState*)state);
            }
        } else {
            VecExecReScan((PlanState*)state);
        }
    }

    /* fetch ordered TIDS */
    Assert(sort != NULL);
    tids = sort->m_tids;

    if (!sort->m_tidEnd) {
        do {
            tids->Reset(true);
            FetchTids(scandesc, indexTList, &state->ps, sort, tids, false);
            PutBatchToSorter(sort, tids);
        } while (!sort->m_tidEnd);

        RunSorter(sort);
    }

    FetchBatchFromSorter(sort, tids);

    return tids;
}

/* for cbtree index only scan */
template <>
VectorBatch* ExecCstoreIndexScanT<BTREE_INDEX_ONLY>(CStoreIndexScanState* state)
{
    VectorBatch* tids = NULL;
    IndexSortState* sort = NULL;
    IndexScanDesc scandesc = NULL;
    List* indexTList = NIL;

    CBTreeOnlyScanState* btreeIndexOnlyScanState = state->m_btreeIndexOnlyScan;
    Assert(btreeIndexOnlyScanState != NULL);

    /* fetch the sort state for btree index */
    sort = btreeIndexOnlyScanState->m_sort;
    scandesc = btreeIndexOnlyScanState->ioss_ScanDesc;
    indexTList = btreeIndexOnlyScanState->m_indexScanTList;

    /* set runtime scan keys */
    if (btreeIndexOnlyScanState->ioss_NumRuntimeKeys != 0 && !btreeIndexOnlyScanState->ioss_RuntimeKeysReady) {
        if (btreeIndexOnlyScanState->ss.isPartTbl) {
            if (PointerIsValid(btreeIndexOnlyScanState->ss.partitions)) {
                btreeIndexOnlyScanState->ss.ss_ReScan = true;
                VecExecReScan((PlanState*)state);
            }
        } else {
            VecExecReScan((PlanState*)state);
        }
    }

    /* fetch TIDS without sort */
    Assert(sort != NULL);
    tids = sort->m_tids;
    tids->Reset(true);
    if (!sort->m_tidEnd) {
        FetchTids(scandesc, indexTList, &state->ps, sort, tids, true);
    }

    return tids;
}

/* for psort index scan */
template <>
VectorBatch* ExecCstoreIndexScanT<PSORT_INDEX>(CStoreIndexScanState* state)
{
    VectorBatch* tids = ExecCStoreScan(state->m_indexScan);
    if (state->m_indexScan->jitted_vecqual) {
        if (HAS_INSTR(state, false)) {
            state->ps.instrument->isLlvmOpt = true;
        }
    }

    return tids;
}

/* ----------------------------------------------------------------
 *      ExecCstoreIndexScan
 * ----------------------------------------------------------------
 */
VectorBatch* ExecCstoreIndexScan(CStoreIndexScanState* state)
{
    VectorBatch* tids = NULL;
    VectorBatch* pScanBatch = NULL;
    VectorBatch* pOutBatch = NULL;

    pOutBatch = state->m_pCurrentBatch;
    pScanBatch = state->m_pScanBatch;

    // update cstore scan timing flag
    state->m_CStore->SetTiming((CStoreScanState*)state);

    ExprDoneCond isDone = ExprSingleResult;
    /*
     * for function-returning-set.
     */
    if (state->ps.ps_vec_TupFromTlist) {
        Assert(state->ps.ps_ProjInfo);
        pOutBatch = ExecVecProject(state->ps.ps_ProjInfo, true, &isDone);
        if (pOutBatch->m_rows > 0) {
            return pOutBatch;
        }

        state->ps.ps_vec_TupFromTlist = false;
    }
    state->ps.ps_ProjInfo->pi_exprContext->current_row = 0;

restart:
    // First get target tids through index scan
    // and it may be LLVM optimized.
    pScanBatch->Reset(true);
    pOutBatch->Reset(true);

    // fetch tids from index
    tids = state->m_cstoreIndexScanFunc(state);
    if (!BatchIsNull(tids)) {
        // Get actual column through tids scan
        //
        DBG_ASSERT(tids->m_cols >= 1);
        state->m_CStore->ScanByTids(state, tids, pScanBatch);
        state->m_CStore->ResetLateRead();

        pScanBatch->FixRowCount();

        pOutBatch = ApplyProjectionAndFilter(state, pScanBatch, &isDone);
        if (isDone != ExprEndResult) {
            state->ps.ps_vec_TupFromTlist = (isDone == ExprMultipleResult);
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
            state->ps.ps_vec_TupFromTlist = (isDone == ExprMultipleResult);
        }

        if (BatchIsNull(pOutBatch))
            goto restart;
    }

    return pOutBatch;
}

/* ----------------------------------------------------------------
 *      ExecInitCstoreIndexScan
 *
 *      Initializes the index scan's state information, creates
 *      scan keys, and opens the base and index relations.
 *
 *      Note: index scans have 2 sets of state information because
 *            we have to keep track of the base relation and the
 *            index relation.
 * ----------------------------------------------------------------
 */
CStoreIndexScanState* ExecInitCstoreIndexScan(CStoreIndexScan* node, EState* estate, int eflags)
{
    CStoreIndexScanState* indexstate = NULL;
    CStoreScan* cstoreScan = NULL;
    CStoreScan* indexScan = NULL;
    CStoreScanState* scanstate = NULL;
    errno_t rc = EOK;

    // Sanity checks
    //
    if (node->indexorderby != NULL || node->indexorderbyorig != NULL) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("ORDER BY with column index is not supported")));
    }
    if (!ScanDirectionIsForward(estate->es_direction)) {
        ereport(
            ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("column index scan only supports forward scan")));
    }

    // Create state structure for column store scan. This column store
    // scan shares the target list, qualification etc with the index scan
    // plan but shall set selectionRatio and cstorequal correctly.
    //
    cstoreScan = makeNode(CStoreScan);
    cstoreScan->plan = node->scan.plan;
    cstoreScan->scanrelid = node->scan.scanrelid;
    cstoreScan->isPartTbl = node->scan.isPartTbl;
    cstoreScan->itrs = node->scan.itrs;
    cstoreScan->pruningInfo = node->scan.pruningInfo;
    cstoreScan->partScanDirection = node->scan.partScanDirection;
    cstoreScan->selectionRatio = 0.01;  // description: need optimizer to tell
    cstoreScan->cstorequal = node->baserelcstorequal;
    cstoreScan->partition_iterator_elimination = node->scan.partition_iterator_elimination;

    // we don't need preload cudesc when use tid scan
    // here we set 'codegenInUplevel' to true to disable codegen in ExecInitCStoreScan
    //
    scanstate = ExecInitCStoreScan(cstoreScan, NULL, estate, eflags, false, true);

    // Create state structure for current plan using the column store
    // state we just created. The trick here is to still hang the original
    // plan node here for EXPLAIN purpose (execution does not need it).
    //
    indexstate = makeNode(CStoreIndexScanState);
    rc = memcpy_s(indexstate, sizeof(CStoreScanState), scanstate, sizeof(CStoreScanState));
    securec_check(rc, "\0", "\0");

#ifdef ENABLE_LLVM_COMPILE
    /*
     * First, not only consider the LLVM native object, but also consider the cost of
     * the LLVM compilation time. We will not use LLVM optimization if there is
     * not enough number of row. Second, consider codegen after we get some information
     * about the scanned relation.
     * We do not codegen filter qual in ExecInitCStoreScan, while here is the right place to to consider this.
     */
    indexstate->jitted_vecqual = NULL;
    llvm::Function* jitted_vecqual = NULL;
    dorado::GsCodeGen* llvmCodeGen = (dorado::GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    bool consider_codegen = CodeGenThreadObjectReady() && 
                            CodeGenPassThreshold(((Plan*)cstoreScan)->plan_rows,
                                                  estate->es_plannedstmt->num_nodes,
                                                  ((Plan*)cstoreScan)->dop);
    if (consider_codegen) {
        /* check if codegen is allowed for quallist expr */
        jitted_vecqual = dorado::VecExprCodeGen::QualCodeGen(indexstate->ps.qual, (PlanState*)indexstate);
        if (jitted_vecqual != NULL)
            llvmCodeGen->addFunctionToMCJit(jitted_vecqual, reinterpret_cast<void**>(&(indexstate->jitted_vecqual)));
    }
#endif

    indexstate->ps.plan = (Plan*)node;
    indexstate->ps.state = estate;
    indexstate->ps.vectorized = true;
    indexstate->ps.type = T_CStoreIndexScanState;
    indexstate->index_only_scan = node->indexonly;
    if (estate->es_is_flt_frame) {
        indexstate->m_deltaQual = (List*)ExecInitQualByFlatten(node->indexqualorig, (PlanState*)&indexstate->ps);
    } else {
        indexstate->m_deltaQual = (List*)ExecInitExprByRecursion((Expr*)node->indexqualorig, (PlanState*)&indexstate->ps);
    }
    // If we are just doing EXPLAIN (ie, aren't going to run the plan), stop
    // here. This allows an index-advisor plugin to EXPLAIN a plan containing
    // references to nonexistent indexes.
    //
    if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
        return indexstate;

    /* If the parent table is one of the target relations of the query, then
     * InitPlan already opened and write-locked the index, so we can avoid
     * taking another lock here.  Otherwise we need a normal reader's lock.
     */
    bool relistarget = ExecRelationIsTargetRelation(estate, node->scan.scanrelid);
    Relation indexRel = index_open(node->indexid, relistarget ? NoLock : AccessShareLock);
    if (indexRel->rd_rel->relam == PSORT_AM_OID) {
        // psort index
        // Initialize column index scan. Create a state for column index scan. It
        // shall use index's Oid, indexqual and the target list shall include
        // only ctid as the target (taken care of by optimizer).
        indexScan = makeNode(CStoreScan);
        indexScan->scanrelid = node->indexid;

        // Fix targetList using attid from index relation.
        // Because optimizer put the attid from heap relation into node ->indexlist
        // So we need fix it
        indexScan->plan.targetlist =
            FixIndexScanTargetList(node, indexstate, node->indextlist, indexRel, scanstate->ss_currentRelation);

        // Since we do not support the "And" clause in Qual right now in CStoreIndexScan,
        // After 'IndexScan' in ExecCStoreIndexScan we need to recheck all the quals to
        // ensure the results is right.
        indexScan->plan.qual = node->indexqual;
        indexScan->isPartTbl = node->scan.isPartTbl;
        indexScan->itrs = node->scan.itrs;
        indexScan->pruningInfo = node->scan.pruningInfo;
        indexScan->partScanDirection = node->scan.partScanDirection;
        indexScan->selectionRatio = 0.01;  // description: need optimizer to tell
        indexScan->cstorequal = node->cstorequal;
        indexScan->partition_iterator_elimination = node->scan.partition_iterator_elimination;

        indexstate->m_indexScan = ExecInitCStoreScan(indexScan, indexstate->ss_partition_parent, estate, eflags, true);
        indexstate->part_id = indexstate->m_indexScan->part_id;
        indexstate->m_btreeIndexScan = NULL;
        indexstate->m_btreeIndexOnlyScan = NULL;
        indexstate->m_cstoreIndexScanFunc = &ExecCstoreIndexScanT<PSORT_INDEX>;
        index_close(indexRel, NoLock);
    } else {
        /* cbtree index */
        List* indexScanTList =
            FixIndexScanTargetList(node, indexstate, node->indextlist, indexRel, scanstate->ss_currentRelation);
        if (!indexstate->index_only_scan) {
            /* cbtree index scan */
            CBTreeScanState* btreeIndexScan = makeNode(CBTreeScanState);
            btreeIndexScan->m_indexScanTList = indexScanTList;
            BuildCBtreeIndexScan(btreeIndexScan,
                (ScanState*)scanstate,
                (Scan*)node,
                estate,
                indexRel,
                node->indexqual,
                node->indexorderby);
            indexstate->m_btreeIndexScan = btreeIndexScan;
            indexstate->part_id = indexstate->m_btreeIndexScan->ss.part_id;
            indexstate->m_btreeIndexOnlyScan = NULL;
            indexstate->m_cstoreIndexScanFunc = &ExecCstoreIndexScanT<BTREE_INDEX>;
        } else {
            /* cbtree index only scan */
            CBTreeOnlyScanState* btreeIndexOnlyScan = makeNode(CBTreeOnlyScanState);
            btreeIndexOnlyScan->m_indexScanTList = indexScanTList;
            BuildCBtreeIndexOnlyScan(btreeIndexOnlyScan,
                (ScanState*)scanstate,
                (Scan*)node,
                estate,
                indexRel,
                node->indexqual,
                node->indexorderby);
            indexstate->m_btreeIndexOnlyScan = btreeIndexOnlyScan;
            indexstate->part_id = indexstate->m_btreeIndexOnlyScan->ss.part_id;
            indexstate->m_btreeIndexScan = NULL;
            indexstate->m_cstoreIndexScanFunc = &ExecCstoreIndexScanT<BTREE_INDEX_ONLY>;
        }

        indexstate->m_indexScan = NULL;
        indexstate->is_scan_end = false;
    }

    // If partition pruning hits none partition, skip ResetLateRead()
    //
    if (indexstate->m_CStore != NULL)
        indexstate->m_CStore->ResetLateRead();

    return indexstate;
}

/* ----------------------------------------------------------------
 *      ExecEndCstoreIndexScan
 * ----------------------------------------------------------------
 */
void ExecEndCstoreIndexScan(CStoreIndexScanState* state)
{
    if (state->m_indexScan != NULL) {
        /* End psort index scan */
        ExecEndCStoreScan(state->m_indexScan, true);
    } else if (state->m_btreeIndexScan != NULL) {
        /* End cbtree index scan */
        if (state->m_btreeIndexScan->m_sort->m_sortState) {
            batchsort_end(state->m_btreeIndexScan->m_sort->m_sortState);
            state->m_btreeIndexScan->m_sort->m_sortState = NULL;
        }
        ExecEndIndexScan((IndexScanState*)state->m_btreeIndexScan);
    } else if (state->m_btreeIndexOnlyScan != NULL) {
        /* End cbtree index only scan */
        if (state->m_btreeIndexOnlyScan->m_sort->m_sortState) {
            batchsort_end(state->m_btreeIndexOnlyScan->m_sort->m_sortState);
            state->m_btreeIndexOnlyScan->m_sort->m_sortState = NULL;
        }
        ExecEndIndexOnlyScan((IndexOnlyScanState*)state->m_btreeIndexOnlyScan);
    }

    ExecEndCStoreScan(state, false);
}

/* ----------------------------------------------------------------
 *      ExecCstoreIndexMarkPos
 * ----------------------------------------------------------------
 */
void ExecCstoreIndexMarkPos(CStoreIndexScanState* state)
{}

/* ----------------------------------------------------------------
 *      ExecCstoreIndexRestrPos
 * ----------------------------------------------------------------
 */
void ExecCstoreIndexRestrPos(CStoreIndexScanState* state)
{}

void ExecInitNextPartitionForCStoreIndexScan(CStoreScanState* indexNodeState)
{
    Relation currentIdxRel;
    CStoreScan* plan = NULL;
    int paramno = -1;
    ParamExecData* param = NULL;

    plan = (CStoreScan*)indexNodeState->ps.plan;
    /*
     * release CStoreScan
     */
    if (indexNodeState->m_CStore) {
        DELETE_EX(indexNodeState->m_CStore);
    }

    /* get partition sequnce */
    paramno = plan->plan.paramno;
    param = &(indexNodeState->ps.state->es_param_exec_vals[paramno]);
    indexNodeState->currentSlot = (int)param->value;

    indexNodeState->m_pScanBatch->Reset();

    /* construct a dummy relation with the next table partition*/
    currentIdxRel = (Relation)list_nth(indexNodeState->partitions, indexNodeState->currentSlot);

    indexNodeState->ss_currentPartition = currentIdxRel;
    indexNodeState->ss_currentRelation = currentIdxRel;

    /* no heap scan here */
    indexNodeState->ss_currentScanDesc = NULL;
    indexNodeState->m_CStore = New(CurrentMemoryContext) CStore();
    indexNodeState->m_CStore->InitScan(indexNodeState, GetActiveSnapshot());
    indexNodeState->m_CStore->ResetLateRead();
}

/* ----------------------------------------------------------------
 *      ExecReScanIndexScan(node)
 * ----------------------------------------------------------------
 */
void ExecReScanCStoreIndexScan(CStoreIndexScanState* node)
{
    CStoreScanState* indexNodeState = node->m_indexScan;
    CBTreeScanState* btreeIndexScanState = node->m_btreeIndexScan;
    CBTreeOnlyScanState* btreeIndexOnlyScanState = node->m_btreeIndexOnlyScan;

    if (indexNodeState != NULL) {
        /* psort index rescan */
        indexNodeState->ps.plan->paramno = node->ps.plan->paramno;

        if (indexNodeState->isPartTbl && !(((Scan *)node->ps.plan)->partition_iterator_elimination)) {
            if (PointerIsValid(indexNodeState->partitions)) {
                /* finally init Scan for the next partition */
                ExecInitNextPartitionForCStoreIndexScan(indexNodeState);
            }
            ExecReScanCStoreScan((CStoreScanState*)node);
            if (indexNodeState->m_ScanRunTimeKeysNum != 0) {
                ExecReSetRuntimeKeys(indexNodeState);
            }
            indexNodeState->m_ScanRunTimeKeysReady = true;
        } else {
            ExecReScanCStoreScan(indexNodeState);
            ReScanDeltaRelation(node);
        }
    } else if (btreeIndexScanState != NULL) {
        /* cbtree index rescan */
        btreeIndexScanState->ss.ps.plan->paramno = node->ps.plan->paramno;
        if (list_length(btreeIndexScanState->m_indexScanTList) != 0) {
            /* Reset the sorter state */
            ResetSorter(btreeIndexScanState->m_sort);
            btreeIndexScanState->m_sort->m_tidEnd = false;

            ExecReScanIndexScan((IndexScanState*)btreeIndexScanState);
        }
        ExecReScanCStoreScan((CStoreScanState*)node);
    } else if (btreeIndexOnlyScanState != NULL) {
        /* cbtree index only rescan */
        btreeIndexOnlyScanState->ss.ps.plan->paramno = node->ps.plan->paramno;

        if (list_length(btreeIndexOnlyScanState->m_indexScanTList) != 0) {
            /* Reset the sorter state */
            ResetSorter(btreeIndexOnlyScanState->m_sort);
            btreeIndexOnlyScanState->m_sort->m_tidEnd = false;

            ExecReScanIndexOnlyScan((IndexOnlyScanState*)btreeIndexOnlyScanState);
        }
        ExecReScanCStoreScan((CStoreScanState*)node);
    }
}

// Fix targetList using attid from index relation.
// Because optimizer put the attid from heap relation into node->indexlist
// So we need fix it
//
static List* FixIndexScanTargetList(CStoreIndexScan* node, CStoreIndexScanState* indexScanState, List* idxTargetList,
    Relation indexRel, Relation heapRel)
{
    // No data to read because partition pruning hits none partition
    //
    if (node->scan.isPartTbl && indexScanState->ss_currentRelation == NULL)
        return NULL;

    FormData_pg_attribute* heapAttrs = heapRel->rd_att->attrs;
    FormData_pg_attribute* IdxRelAttrs = indexRel->rd_att->attrs;
    indexScanState->m_indexOutBaseTabAttr = (int*)palloc0(sizeof(int) * list_length(idxTargetList));
    int* outKeyId = indexScanState->m_indexOutBaseTabAttr;

    int idxAttNo = indexRel->rd_att->natts;
    int num = 0;
    List* newTargetList = NIL;
    ListCell* cell = NULL;

    /* for psort index(not indexonly) or btree index only scan, we need to fetch the column data except tids. */
    if (indexRel->rd_rel->relam == PSORT_AM_OID || node->indexonly) {
        // Step 1: Add normal columns into newTargetList
        // Conver the attid from heap relation to the attid from index relation
        //
        foreach (cell, idxTargetList) {
            TargetEntry* tle = (TargetEntry*)lfirst(cell);
            Assert(IsA(tle->expr, Var));
            int pos = ((Var*)tle->expr)->varattno - 1;
            for (int col = 0; col < idxAttNo; ++col) {
                if (strcmp(NameStr(IdxRelAttrs[col].attname), NameStr(heapAttrs[pos].attname)) == 0) {
                    Expr* idxVar = (Expr*)makeVar(((Var*)tle->expr)->varno,
                        col + 1,
                        IdxRelAttrs[col].atttypid,
                        IdxRelAttrs[col].atttypmod,
                        IdxRelAttrs[col].attcollation,
                        0);
                    newTargetList = lappend(newTargetList, makeTargetEntry(idxVar, num + 1, NULL, false));
                    outKeyId[num] = pos + 1;
                    ++num;
                }
            }
        }
        Assert(num == list_length(idxTargetList));
    }

    /* for btree index, the num here is zero */
    indexScanState->m_indexOutAttrNo = num;

    // Step 2: Add tid target column into newTargetList
    //
    Expr* idxVar = (Expr*)makeVar(0, idxAttNo + 1, TIDOID, -1, InvalidOid, 0);
    newTargetList = lappend(newTargetList, makeTargetEntry(idxVar, num + 1, NULL, false));

    return newTargetList;
}

/*
 * Build btree index scan state
 * @IN param btreeIndexScan: the target object to fill
 * @IN param scanstate: the basic ScanState
 * @IN param node: the basic scan node
 * @IN param estate: the executor state
 * @IN param indexRel: the index relation
 * @IN param indexqual: the qual on the index columns
 * @IN param indexorderby: the ordered qual on the index columns
 */
void BuildCBtreeIndexScan(CBTreeScanState* btreeIndexScan, ScanState* scanstate, Scan* node, EState* estate,
    Relation indexRel, List* indexqual, List* indexorderby)
{
    int sortMem = SET_NODEMEM(node->plan.operatorMemKB[0], node->plan.dop);
    int maxMem = (node->plan.operatorMaxMem > 0) ? (node->plan.operatorMaxMem / SET_DOP(node->plan.dop)) : 0;

    errno_t rc;
    /* Copy ScanState */
    rc = memcpy_s(btreeIndexScan, sizeof(ScanState), scanstate, sizeof(ScanState));
    securec_check(rc, "\0", "\0");

    btreeIndexScan->ss.ps.plan = (Plan*)node;
    btreeIndexScan->ss.isPartTbl = node->isPartTbl;
    btreeIndexScan->ss.ss_currentRelation = ExecOpenScanRelation(estate, node->scanrelid);

    /* Initialize the index sort state */
    btreeIndexScan->m_sort = (IndexSortState*)palloc0(sizeof(IndexSortState));
    btreeIndexScan->m_sort->m_sortTupleDesc = BuildTupleDescByTargetList(btreeIndexScan->m_indexScanTList);
    btreeIndexScan->m_sort->m_tids =
        New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, btreeIndexScan->m_sort->m_sortTupleDesc);
    btreeIndexScan->m_sort->m_sortState = InitTidSortState(
        btreeIndexScan->m_sort->m_sortTupleDesc, btreeIndexScan->m_sort->m_sortTupleDesc->natts, sortMem, maxMem);
    btreeIndexScan->m_sort->m_sortCount = 0;
    btreeIndexScan->m_sort->m_tidEnd = false;

    btreeIndexScan->iss_RelationDesc = indexRel;
    if (!IndexIsUsable(btreeIndexScan->iss_RelationDesc->rd_index)) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("can't initialize index scans using unusable index \"%s\"",
                    RelationGetRelationName(btreeIndexScan->iss_RelationDesc))));
    }

    /*
     * Initialize index-specific scan state
     */
    btreeIndexScan->iss_RuntimeKeysReady = false;
    btreeIndexScan->iss_RuntimeKeys = NULL;
    btreeIndexScan->iss_NumRuntimeKeys = 0;

    /* when we need not to scan anything, short circuit here. */
    if (list_length(btreeIndexScan->m_indexScanTList) == 0) {
        btreeIndexScan->m_sort->m_tidEnd = true;
        return;
    }

    /*
     * build the index scan keys from the index qualification
     */
    ExecIndexBuildScanKeys((PlanState*)btreeIndexScan,
        btreeIndexScan->iss_RelationDesc,
        indexqual,
        false,
        &btreeIndexScan->iss_ScanKeys,
        &btreeIndexScan->iss_NumScanKeys,
        &btreeIndexScan->iss_RuntimeKeys,
        &btreeIndexScan->iss_NumRuntimeKeys,
        NULL, /* no ArrayKeys */
        NULL);

    /*
     * any ORDER BY exprs have to be turned into scankeys in the same way
     */
    ExecIndexBuildScanKeys((PlanState*)btreeIndexScan,
        btreeIndexScan->iss_RelationDesc,
        indexorderby,
        true,
        &btreeIndexScan->iss_OrderByKeys,
        &btreeIndexScan->iss_NumOrderByKeys,
        &btreeIndexScan->iss_RuntimeKeys,
        &btreeIndexScan->iss_NumRuntimeKeys,
        NULL, /* no ArrayKeys */
        NULL);

    /*
     * If we have runtime keys, we need an ExprContext to evaluate them. The
     * node's standard context won't do because we want to reset that context
     * for every tuple.  So, build another context just like the other one...
     * -tgl 7/11/00
     */
    if (btreeIndexScan->iss_NumRuntimeKeys != 0) {
        ExprContext* stdecontext = btreeIndexScan->ss.ps.ps_ExprContext;

        ExecAssignExprContext(estate, &btreeIndexScan->ss.ps);
        btreeIndexScan->iss_RuntimeContext = btreeIndexScan->ss.ps.ps_ExprContext;
        btreeIndexScan->ss.ps.ps_ExprContext = stdecontext;
    } else {
        btreeIndexScan->iss_RuntimeContext = NULL;
    }

    /* deal with partition info */
    if (node->isPartTbl) {
        btreeIndexScan->iss_ScanDesc = NULL;

        if (0 < node->itrs) {
            Partition currentpartition = NULL;
            Partition currentindex = NULL;

            /* Initialize table partition list and index partition list for following scan*/
            ExecInitPartitionForIndexScan((IndexScanState*)btreeIndexScan, estate);

            /* construct a dummy relation with the first table partition for following scan */
            currentpartition = (Partition)list_nth(btreeIndexScan->ss.partitions, 0);
            btreeIndexScan->ss.ss_currentPartition =
                partitionGetRelation(btreeIndexScan->ss.ss_currentRelation, currentpartition);

            /* construct a dummy relation with the first table partition for following scan */
            currentindex = (Partition)list_nth(btreeIndexScan->iss_IndexPartitionList, 0);
            btreeIndexScan->iss_CurrentIndexPartition =
                partitionGetRelation(btreeIndexScan->iss_RelationDesc, currentindex);

            /* Initialize scan descriptor for partitioned table */
            btreeIndexScan->iss_ScanDesc = index_beginscan(btreeIndexScan->ss.ss_currentPartition,
                btreeIndexScan->iss_CurrentIndexPartition,
                estate->es_snapshot,
                btreeIndexScan->iss_NumScanKeys,
                btreeIndexScan->iss_NumOrderByKeys);
            Assert(PointerIsValid(btreeIndexScan->iss_ScanDesc));
        }
    } else {
        btreeIndexScan->iss_ScanDesc = index_beginscan(btreeIndexScan->ss.ss_currentPartition,
            btreeIndexScan->iss_RelationDesc,
            estate->es_snapshot,
            btreeIndexScan->iss_NumScanKeys,
            btreeIndexScan->iss_NumOrderByKeys);
    }

    GetIndexScanDesc(btreeIndexScan->iss_ScanDesc)->xs_want_itup = false;

    /*
     * If no run-time keys to calculate, go ahead and pass the scankeys to the
     * index AM.
     */
    if (btreeIndexScan->iss_NumRuntimeKeys == 0 && PointerIsValid(btreeIndexScan->iss_ScanDesc))
        scan_handler_idx_rescan(btreeIndexScan->iss_ScanDesc,
            btreeIndexScan->iss_ScanKeys,
            btreeIndexScan->iss_NumScanKeys,
            btreeIndexScan->iss_OrderByKeys,
            btreeIndexScan->iss_NumOrderByKeys);
}

/*
 * Build btree index only scan state
 * @IN param btreeIndexScan: the target object to fill
 * @IN param scanstate: the basic ScanState
 * @IN param node: the basic scan node
 * @IN param estate: the executor state
 * @IN param indexRel: the index relation
 * @IN param indexqual: the qual on the index columns
 * @IN param indexorderby: the ordered qual on the index columns
 */
void BuildCBtreeIndexOnlyScan(CBTreeOnlyScanState* btreeIndexOnlyScan, ScanState* scanstate, Scan* node, EState* estate,
    Relation indexRel, List* indexqual, List* indexorderby)
{
    int sortMem = u_sess->attr.attr_memory.work_mem;
    int maxMem = 0;
    if (node->plan.operatorMemKB[0] > 0) {
        sortMem = node->plan.operatorMemKB[0];
        maxMem = node->plan.operatorMaxMem;
    }
    errno_t rc;

    /* Copy ScanState */
    rc = memcpy_s(btreeIndexOnlyScan, sizeof(ScanState), scanstate, sizeof(ScanState));
    securec_check(rc, "\0", "\0");

    btreeIndexOnlyScan->ss.ps.plan = (Plan*)node;
    btreeIndexOnlyScan->ss.isPartTbl = node->isPartTbl;
    btreeIndexOnlyScan->ss.ss_currentRelation = ExecOpenScanRelation(estate, node->scanrelid);

    /* prepare the index sort state */
    btreeIndexOnlyScan->m_sort = (IndexSortState*)palloc0(sizeof(IndexSortState));
    btreeIndexOnlyScan->m_sort->m_sortTupleDesc = BuildTupleDescByTargetList(btreeIndexOnlyScan->m_indexScanTList);
    btreeIndexOnlyScan->m_sort->m_tids =
        New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, btreeIndexOnlyScan->m_sort->m_sortTupleDesc);
    btreeIndexOnlyScan->m_sort->m_sortState = InitTidSortState(btreeIndexOnlyScan->m_sort->m_sortTupleDesc,
        btreeIndexOnlyScan->m_sort->m_sortTupleDesc->natts,
        sortMem,
        maxMem);
    btreeIndexOnlyScan->m_sort->m_sortCount = 0;
    btreeIndexOnlyScan->m_sort->m_tidEnd = false;

    btreeIndexOnlyScan->ioss_RelationDesc = indexRel;
    if (!IndexIsUsable(btreeIndexOnlyScan->ioss_RelationDesc->rd_index)) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("can't initialize index scans using unusable index \"%s\"",
                    RelationGetRelationName(btreeIndexOnlyScan->ioss_RelationDesc))));
    }

    /*
     * Initialize index-specific scan state
     */
    btreeIndexOnlyScan->ioss_RuntimeKeysReady = false;
    btreeIndexOnlyScan->ioss_RuntimeKeys = NULL;
    btreeIndexOnlyScan->ioss_NumRuntimeKeys = 0;

    /* when we need not to scan anything, short circuit here. */
    if (list_length(btreeIndexOnlyScan->m_indexScanTList) == 0) {
        btreeIndexOnlyScan->m_sort->m_tidEnd = true;
        return;
    }

    /*
     * build the index scan keys from the index qualification
     */
    ExecIndexBuildScanKeys((PlanState*)btreeIndexOnlyScan,
        btreeIndexOnlyScan->ioss_RelationDesc,
        indexqual,
        false,
        &btreeIndexOnlyScan->ioss_ScanKeys,
        &btreeIndexOnlyScan->ioss_NumScanKeys,
        &btreeIndexOnlyScan->ioss_RuntimeKeys,
        &btreeIndexOnlyScan->ioss_NumRuntimeKeys,
        NULL, /* no ArrayKeys */
        NULL);

    /*
     * any ORDER BY exprs have to be turned into scankeys in the same way
     */
    ExecIndexBuildScanKeys((PlanState*)btreeIndexOnlyScan,
        btreeIndexOnlyScan->ioss_RelationDesc,
        indexorderby,
        true,
        &btreeIndexOnlyScan->ioss_OrderByKeys,
        &btreeIndexOnlyScan->ioss_NumOrderByKeys,
        &btreeIndexOnlyScan->ioss_RuntimeKeys,
        &btreeIndexOnlyScan->ioss_NumRuntimeKeys,
        NULL, /* no ArrayKeys */
        NULL);

    /*
     * If we have runtime keys, we need an ExprContext to evaluate them. The
     * node's standard context won't do because we want to reset that context
     * for every tuple.  So, build another context just like the other one...
     * -tgl 7/11/00
     */
    if (btreeIndexOnlyScan->ioss_NumRuntimeKeys != 0) {
        ExprContext* stdecontext = btreeIndexOnlyScan->ss.ps.ps_ExprContext;

        ExecAssignExprContext(estate, &btreeIndexOnlyScan->ss.ps);
        btreeIndexOnlyScan->ioss_RuntimeContext = btreeIndexOnlyScan->ss.ps.ps_ExprContext;
        btreeIndexOnlyScan->ss.ps.ps_ExprContext = stdecontext;
    } else {
        btreeIndexOnlyScan->ioss_RuntimeContext = NULL;
    }

    /* deal with partition info */
    if (node->isPartTbl) {
        btreeIndexOnlyScan->ioss_CurrentIndexPartition = NULL;
        btreeIndexOnlyScan->ss.ss_currentPartition = NULL;
        btreeIndexOnlyScan->ioss_ScanDesc = NULL;

        if (0 < node->itrs) {
            Partition currentpartition = NULL;
            Partition currentindex = NULL;

            /* Initialize table partition list and index partition list for following scan*/
            ExecInitPartitionForIndexOnlyScan((IndexOnlyScanState*)btreeIndexOnlyScan, estate);

            /* construct a dummy relation with the first table partition for following scan */
            currentpartition = (Partition)list_nth(btreeIndexOnlyScan->ss.partitions, 0);
            btreeIndexOnlyScan->ss.ss_currentPartition =
                partitionGetRelation(btreeIndexOnlyScan->ss.ss_currentRelation, currentpartition);

            /* construct a dummy relation with the first table partition for following scan */
            currentindex = (Partition)list_nth(btreeIndexOnlyScan->ioss_IndexPartitionList, 0);
            btreeIndexOnlyScan->ioss_CurrentIndexPartition =
                partitionGetRelation(btreeIndexOnlyScan->ioss_RelationDesc, currentindex);

            /* Initialize scan descriptor for partitioned table */
            btreeIndexOnlyScan->ioss_ScanDesc =
                index_beginscan(btreeIndexOnlyScan->ss.ss_currentPartition,
                    btreeIndexOnlyScan->ioss_CurrentIndexPartition,
                    estate->es_snapshot,
                    btreeIndexOnlyScan->ioss_NumScanKeys,
                    btreeIndexOnlyScan->ioss_NumOrderByKeys);
            Assert(PointerIsValid(btreeIndexOnlyScan->ioss_ScanDesc));
        }
    } else {
        btreeIndexOnlyScan->ioss_ScanDesc = index_beginscan(btreeIndexOnlyScan->ss.ss_currentPartition,
            btreeIndexOnlyScan->ioss_RelationDesc,
            estate->es_snapshot,
            btreeIndexOnlyScan->ioss_NumScanKeys,
            btreeIndexOnlyScan->ioss_NumOrderByKeys);
    }

    /*
     * If is Partition table, if ( 0 == node->scan.itrs), scan_desc is NULL.
     */
    if (PointerIsValid(btreeIndexOnlyScan->ioss_ScanDesc)) {
        /* Set it up for index-only scan */
        GetIndexScanDesc(btreeIndexOnlyScan->ioss_ScanDesc)->xs_want_itup = true;
        btreeIndexOnlyScan->ioss_VMBuffer = InvalidBuffer;

        /*
         * If no run-time keys to calculate, go ahead and pass the scankeys to the
         * index AM.
         */
        if (btreeIndexOnlyScan->ioss_NumRuntimeKeys == 0)
            scan_handler_idx_rescan(btreeIndexOnlyScan->ioss_ScanDesc,
                btreeIndexOnlyScan->ioss_ScanKeys,
                btreeIndexOnlyScan->ioss_NumScanKeys,
                btreeIndexOnlyScan->ioss_OrderByKeys,
                btreeIndexOnlyScan->ioss_NumOrderByKeys);
    }
}

Batchsortstate* InitTidSortState(TupleDesc sortTupDesc, int tidAttNo, int sortMem, int maxMem)
{
    Assert(sortTupDesc);
    Assert(tidAttNo <= sortTupDesc->natts);

    /* there is no data in index */
    if (tidAttNo == 0) {
        return NULL;
    }

    const int nkeys = 1;
    AttrNumber attNums[1];
    Oid sortCollations[1];
    bool nullsFirstFlags[1];
    Batchsortstate* sortState = NULL;

    TypeCacheEntry* typeEntry = lookup_type_cache(TIDOID, TYPECACHE_LT_OPR | TYPECACHE_GT_OPR);

    attNums[0] = tidAttNo;
    sortCollations[0] = sortTupDesc->attrs[tidAttNo - 1].attcollation;
    nullsFirstFlags[0] = false;

    sortState = batchsort_begin_heap(
        sortTupDesc, nkeys, attNums, &typeEntry->lt_opr, sortCollations, nullsFirstFlags, sortMem, false, maxMem);
    return sortState;
}

/*
 * @Description: Put the source batch into the sort buffer.
 * @IN sort: IndexSortState node.
 * @IN batch: the source vector batch
 * @See also:
 */
void PutBatchToSorter(IndexSortState* sort, VectorBatch* batch)
{
    Batchsortstate* sortState = sort->m_sortState;
    sortState->sort_putbatch(sortState, batch, 0, batch->m_rows);
    sort->m_sortCount += batch->m_rows;
}

/*
 * @Description: Check if the sort buffer is empty.
 * @IN sort: IndexSortState node.
 * @See also:
 */
bool IsSorterEmpty(IndexSortState* sort)
{
    return (0 == sort->m_sortCount);
}

/*
 * @Description: Reset the state of sort buffer.
 * @IN sort: IndexSortState node.
 * @See also:
 */
void ResetSorter(IndexSortState* sort)
{
    Batchsortstate* oldSortState = sort->m_sortState;
    TupleDesc desc = sort->m_sortTupleDesc;

    if (NULL != oldSortState) {
        /* Clean the old sort state and set the new sort state. */
        batchsort_end(oldSortState);
    }

    /* Build new sort state according to the old one. */
    sort->m_sortState = InitTidSortState(desc, desc->natts);
    sort->m_sortCount = 0;
    if (!BatchIsNull(sort->m_tids)) {
        sort->m_tids->Reset(false);
    }
}

/*
 * @Description: Sort the data in the buffer.
 * @IN sort: IndexSortState node.
 * @See also:
 */
void RunSorter(IndexSortState* sort)
{
    if (!IsSorterEmpty(sort)) {
        batchsort_performsort(sort->m_sortState);
    }
}

/*
 * @Description: Fetch the batch from the sort buffer.
 * @IN sort: IndexSortState node.
 * @See also:
 */
void FetchBatchFromSorter(IndexSortState* sort, VectorBatch* tids)
{
    tids->Reset(true);
    if (!IsSorterEmpty(sort)) {
        batchsort_getbatch(sort->m_sortState, true, tids);
        sort->m_sortCount -= tids->m_rows;
    }
}

void FetchTids(IndexScanDesc scandesc, List* indexScanTargetList, PlanState* ps, IndexSortState* sort,
    VectorBatch* tids, bool indexOnly)
{
    ScanDirection direction = ps->state->es_direction;
    int indexTargetNumber = list_length(indexScanTargetList);
    ScalarVector* vecs = tids->m_arr;
    ListCell* cell = NULL;
    ItemPointer tid;
    int offset = 0;

    while ((tid = scan_handler_idx_getnext_tid(scandesc, direction)) != NULL) {
        IndexScanDesc indexScan = GetIndexScanDesc(scandesc);
        if (indexOnly) {
            int colId = 0;
            foreach (cell, indexScanTargetList) {
                /* for non-tid columns */
                if (colId < indexTargetNumber - 1) {
                    TargetEntry* indexTle = (TargetEntry*)lfirst(cell);
                    int indexVarNo = ((Var*)indexTle->expr)->varattno;
                    bool isNull = false;
                    Datum value = index_getattr(indexScan->xs_itup, indexVarNo, indexScan->xs_itupdesc, &isNull);
                    ScalarVector* vec = &vecs[colId];
                    if (isNull) {
                        vec->SetNull(offset);
                    } else {
                        if (vec->m_desc.encoded) {
                            (void)vec->AddVar(value, offset);
                        } else {
                            vec->m_vals[offset] = value;
                        }
                    }
                } else {
                    /* for tid column */
                    ItemPointer itemP = (ItemPointer) & (vecs[colId].m_vals[offset]);
                    itemP->ip_blkid = tid->ip_blkid;
                    itemP->ip_posid = tid->ip_posid;
                }

                colId++;
            }
        } else {
            /* for tid column */
            ItemPointer itemP = (ItemPointer) & (vecs[0].m_vals[offset]);
            itemP->ip_blkid = tid->ip_blkid;
            itemP->ip_posid = tid->ip_posid;
        }
        offset++;
        if (offset == BatchMaxSize) {
            break;
        }
    }

    /* set rows of the vector batch */
    for (int i = 0; i < indexTargetNumber; i++) {
        vecs[i].m_rows = offset;
    }
    tids->m_rows = offset;

    if (tid == NULL) {
        sort->m_tidEnd = true;
    }
}
