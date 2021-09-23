/* -------------------------------------------------------------------------
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2010, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * veccstoreindexctidscan.cpp
 *    Support routines for index-ctids scans of column stores.
 *
 * IDENTIFICATION
 *        Code/src/gausskernel/runtime/vecexecutor/vecnode/veccstoreindexctidscan.cpp
 *
 * -------------------------------------------------------------------------
 */

/*
 * INTERFACE ROUTINES
 * 	ExecInitCStoreIndexCtidScan		creates and initializes a cstoreindexctidscan node.
 *	ExecCStoreIndexCtidScan			scans a column store with ctids.
    ExecEndCStoreIndexCtidScan		release any storage allocated
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "access/genam.h"
#include "access/relscan.h"
#include "access/gin_private.h"
#include "access/tableam.h"
#include "access/nbtree.h"
#include "executor/executor.h"
#include "executor/exec/execdebug.h"
#include "executor/node/nodeIndexscan.h"
#include "executor/node/nodeBitmapIndexscan.h"
#include "vecexecutor/vecnodecstoreindexctidscan.h"
#include "vecexecutor/vecnodecstorescan.h"
#include "vecexecutor/vecexecutor.h"
#include "vecexecutor/vectorbatch.h"
#include "vecexecutor/vecnodecstorescan.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/batchsort.h"
#include "nodes/makefuncs.h"
#include "catalog/pg_partition_fn.h"

static void EliminateDuplicateScalarArrayElem(BitmapIndexScanState *node);

/*
 * Fix targetList using attid from index relation.
 * Because optimizer put the attid from heap relation into node->indexlist.
 * So we need fix it.
 */
static List* FixIndexCtidScanTargetList(List* idxTargetList, Oid idxRelId, bool getNormalColumns)
{
    List* newTargetList = NIL;
    int idxAttNo = 0;
    int num = 0;

    /*
     * Step 1: Add normal columns into newTargetList
     * Conver the attid from heap relation to the attid from index relation
     */
    if (getNormalColumns) {
        ListCell* cell = NULL;
        Relation indexRel = index_open(idxRelId, AccessShareLock);
        Relation heapRel = relation_open(indexRel->rd_index->indrelid, AccessShareLock);
        Form_pg_attribute* heapRelAttrs = heapRel->rd_att->attrs;
        Form_pg_attribute* idxRelAttrs = indexRel->rd_att->attrs;
        idxAttNo = indexRel->rd_att->natts;

        foreach (cell, idxTargetList) {
            TargetEntry* tle = (TargetEntry*)lfirst(cell);
            Assert(IsA(tle->expr, Var));
            int pos = ((Var*)tle->expr)->varattno - 1;
            for (int col = 0; col < idxAttNo; ++col) {
                if (strcmp(NameStr(idxRelAttrs[col]->attname), NameStr(heapRelAttrs[pos]->attname)) == 0) {
                    Expr* idxVar = (Expr*)makeVar(((Var*)tle->expr)->varno,
                        col + 1,
                        idxRelAttrs[col]->atttypid,
                        idxRelAttrs[col]->atttypmod,
                        idxRelAttrs[col]->attcollation,
                        0);
                    newTargetList = lappend(newTargetList, makeTargetEntry(idxVar, num + 1, NULL, false));
                    ++num;
                }
            }
        }

        index_close(indexRel, NoLock);
        relation_close(heapRel, NoLock);
    }

    /* Step 2: Add tid target column into newTargetList */
    Expr* idxVar = (Expr*)makeVar(0, idxAttNo + 1, TIDOID, -1, InvalidOid, 0);
    newTargetList = lappend(newTargetList, makeTargetEntry(idxVar, num + 1, NULL, false));

    return newTargetList;
}

CstoreBitmapIndexScanState* ExecInitCstoreBitmapIndexScan(CStoreIndexCtidScan* node, EState* estate, int eflags)
{
    CstoreBitmapIndexScanState* indexstate = NULL;
    bool relistarget = false;
    int sortMem = SET_NODEMEM(node->scan.plan.operatorMemKB[0], node->scan.plan.dop);
    int maxMem = (node->scan.plan.operatorMaxMem > 0) ? (node->scan.plan.operatorMaxMem / SET_DOP(node->scan.plan.dop)) : 0;

    /* check for unsupported flags */
    Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

    /*
     * create state structure
     */
    indexstate = makeNode(CstoreBitmapIndexScanState);
    indexstate->ss.ps.plan = (Plan*)node;
    indexstate->ss.ps.state = estate;
    indexstate->ss.isPartTbl = node->scan.isPartTbl;
    indexstate->ss.currentSlot = 0;
    indexstate->ss.partScanDirection = node->scan.partScanDirection;
    indexstate->m_indexScanTList = FixIndexCtidScanTargetList(node->indextlist, node->indexid, false);
    /* prepare the index sort state */
    indexstate->m_sort = (IndexSortState*)palloc0(sizeof(IndexSortState));
    indexstate->m_sort->m_sortTupleDesc = BuildTupleDescByTargetList(indexstate->m_indexScanTList);
    indexstate->m_sort->m_tids =
        New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, indexstate->m_sort->m_sortTupleDesc);
    indexstate->m_sort->m_sortState = InitTidSortState(
        indexstate->m_sort->m_sortTupleDesc, indexstate->m_sort->m_sortTupleDesc->natts, sortMem, maxMem);
    indexstate->m_sort->m_sortCount = 0;
    indexstate->m_sort->m_tidEnd = false;
    /* normally we don't make the result bitmap till runtime */
    indexstate->biss_result = NULL;

    /*
     * Miscellaneous initialization
     *
     * We do not need a standard exprcontext for this node, though we may
     * decide below to create a runtime-key exprcontext
     *
     * initialize child expressions
     *
     * We don't need to initialize targetlist or qual since neither are used.
     *
     * Note: we don't initialize all of the indexqual expression, only the
     * sub-parts corresponding to runtime keys (see below).
     *
     * We do not open or lock the base relation here.  We assume that an
     * ancestor BitmapHeapScan node is holding AccessShareLock (or better) on
     * the heap relation throughout the execution of the plan tree.
     */
    indexstate->ss.ss_currentRelation = NULL;
    indexstate->ss.ss_currentScanDesc = NULL;

    /*
     * If we are just doing EXPLAIN (ie, aren't going to run the plan), stop
     * here.  This allows an index-advisor plugin to EXPLAIN a plan containing
     * references to nonexistent indexes.
     */
    if ((uint32)eflags & EXEC_FLAG_EXPLAIN_ONLY)
        return indexstate;

    /*
     * Open the index relation.
     *
     * If the parent table is one of the target relations of the query, then
     * InitPlan already opened and write-locked the index, so we can avoid
     * taking another lock here.  Otherwise we need a normal reader's lock.
     */
    relistarget = ExecRelationIsTargetRelation(estate, node->scan.scanrelid);
    indexstate->biss_RelationDesc = index_open(node->indexid, relistarget ? NoLock : AccessShareLock);
    if (!IndexIsUsable(indexstate->biss_RelationDesc->rd_index)) {
        ereport(ERROR,
            (errcode(ERRCODE_INDEX_CORRUPTED),
                errmsg("can't initialize bitmap index scans using unusable index \"%s\"",
                    RelationGetRelationName(indexstate->biss_RelationDesc))));
    }

    /*
     * Initialize index-specific scan state
     */
    indexstate->biss_RuntimeKeysReady = false;
    indexstate->biss_RuntimeKeys = NULL;
    indexstate->biss_NumRuntimeKeys = 0;

    /*
     * build the index scan keys from the index qualification
     */
    ExecIndexBuildScanKeys((PlanState*)indexstate,
        indexstate->biss_RelationDesc,
        node->indexqual,
        false,
        &indexstate->biss_ScanKeys,
        &indexstate->biss_NumScanKeys,
        &indexstate->biss_RuntimeKeys,
        &indexstate->biss_NumRuntimeKeys,
        &indexstate->biss_ArrayKeys,
        &indexstate->biss_NumArrayKeys);

    /*
     * If we have runtime keys or array keys, we need an ExprContext to
     * evaluate them. We could just create a "standard" plan node exprcontext,
     * but to keep the code looking similar to nodeIndexscan.c, it seems
     * better to stick with the approach of using a separate ExprContext.
     */
    if (indexstate->biss_NumRuntimeKeys != 0 || indexstate->biss_NumArrayKeys != 0) {
        ExprContext* stdecontext = indexstate->ss.ps.ps_ExprContext;

        ExecAssignExprContext(estate, &indexstate->ss.ps);
        indexstate->biss_RuntimeContext = indexstate->ss.ps.ps_ExprContext;
        indexstate->ss.ps.ps_ExprContext = stdecontext;
    } else {
        indexstate->biss_RuntimeContext = NULL;
    }

    /* get index partition list and table partition list */
    if (node->scan.isPartTbl) {
        indexstate->biss_ScanDesc = NULL;

        if (0 < node->scan.itrs) {
            Partition currentindex = NULL;
            Relation currentrel = NULL;

            currentrel = ExecOpenScanRelation(estate, node->scan.scanrelid);

            /* Initialize table partition and index partition */
            ExecInitPartitionForBitmapIndexScan(indexstate, estate, currentrel);

            ExecCloseScanRelation(currentrel);

            /* get the first index partition */
            if (indexstate->biss_IndexPartitionList != NIL) {
                currentindex = (Partition)list_nth(indexstate->biss_IndexPartitionList, 0);
                indexstate->biss_CurrentIndexPartition = 
                    partitionGetRelation(indexstate->biss_RelationDesc, currentindex);

                indexstate->biss_ScanDesc = scan_handler_idx_beginscan_bitmap(indexstate->biss_CurrentIndexPartition,
                    estate->es_snapshot,
                    indexstate->biss_NumScanKeys,
                    (ScanState*)indexstate);
            }
        }
    } else {
        /*
         * Initialize scan descriptor.
         */
        indexstate->biss_ScanDesc = scan_handler_idx_beginscan_bitmap(
            indexstate->biss_RelationDesc, estate->es_snapshot, indexstate->biss_NumScanKeys, (ScanState*)indexstate);
    }

    /*
     * If no run-time keys to calculate, go ahead and pass the scankeys to the
     * index AM.
     */
    if ((indexstate->biss_NumRuntimeKeys == 0 && indexstate->biss_NumArrayKeys == 0) &&
        PointerIsValid(indexstate->biss_ScanDesc))
        scan_handler_idx_rescan(indexstate->biss_ScanDesc, indexstate->biss_ScanKeys, indexstate->biss_NumScanKeys, NULL, 0);

    /*
     * all done.
     */
    return indexstate;
}

/* ----------------------------------------------------------------
 *                ExecInitCStoreIndexCtidScan
 * ----------------------------------------------------------------
 */
CStoreIndexCtidScanState* ExecInitCstoreIndexCtidScan(CStoreIndexCtidScan* node, EState* estate, int eflags)
{
    bool relistarget = ExecRelationIsTargetRelation(estate, node->scan.scanrelid);
    Relation indexRel = index_open(node->indexid, relistarget ? NoLock : AccessShareLock);
    if (indexRel->rd_rel->relam == PSORT_AM_OID) {
        Relation parentHeapRel = NULL;
        CStoreScan* cstorescan = makeNode(CStoreScan);

        cstorescan->cstorequal = node->cstorequal;
        cstorescan->plan = node->scan.plan;
        cstorescan->plan.qual = node->indexqual;
        cstorescan->scanrelid = node->indexid;
        cstorescan->selectionRatio = 0.01;
        cstorescan->isPartTbl = node->scan.isPartTbl;
        cstorescan->itrs = node->scan.itrs;
        cstorescan->pruningInfo = node->scan.pruningInfo;
        cstorescan->partScanDirection = node->scan.partScanDirection;

        cstorescan->plan.targetlist = FixIndexCtidScanTargetList(node->indextlist, node->indexid, true);

        if (cstorescan->isPartTbl)
            parentHeapRel = ExecOpenScanRelation(estate, node->scan.scanrelid);

        CStoreScanState* cstorescanstate = ExecInitCStoreScan(cstorescan, parentHeapRel, estate, eflags, true, true);

        if (NULL != parentHeapRel)
            heap_close(parentHeapRel, NoLock);

        // Need copy to avoid Heap-buffer-overflow when access other fields.
        // Create state structure for current plan using the column store
        // state we just created. The trick here is to still hang the original
        // plan node here for EXPLAIN purpose (execution does not need it).
        //
        CStoreIndexCtidScanState* cctidscanstate = makeNode(CStoreIndexCtidScanState);
        errno_t rc = memcpy_s(cctidscanstate, sizeof(CStoreScanState), cstorescanstate, sizeof(CStoreScanState));
        securec_check(rc, "", "");

        cctidscanstate->ps.plan = (Plan*)node;
        cctidscanstate->ps.state = estate;
        cctidscanstate->ps.vectorized = true;
        cctidscanstate->ps.type = T_CStoreIndexCtidScanState;

        cctidscanstate->m_cstoreBitmapIndexScan = NULL;
        cctidscanstate->m_btreeIndexScan = NULL;
        cctidscanstate->m_btreeIndexOnlyScan = NULL;

        index_close(indexRel, NoLock);
        return cctidscanstate;
    } else {
        CStoreIndexCtidScanState* cctidscanstate = NULL;

        cctidscanstate = makeNode(CStoreIndexCtidScanState);

        cctidscanstate->ps.plan = (Plan*)node;
        cctidscanstate->ps.state = estate;
        cctidscanstate->ps.vectorized = true;
        cctidscanstate->ps.type = T_CStoreIndexCtidScanState;

        cctidscanstate->m_cstoreBitmapIndexScan = ExecInitCstoreBitmapIndexScan(node, estate, eflags);
        cctidscanstate->part_id = cctidscanstate->m_cstoreBitmapIndexScan->ss.part_id;
        cctidscanstate->m_btreeIndexScan = NULL;
        cctidscanstate->m_btreeIndexOnlyScan = NULL;

        index_close(indexRel, NoLock);
        return cctidscanstate;
    }
}

/* ----------------------------------------------------------------
 *                   ExecCstoreIndexCtidScan
 * ----------------------------------------------------------------
 */
VectorBatch* ExecCstoreIndexCtidScan(CStoreIndexCtidScanState* state)
{
    VectorBatch* tids = NULL;
    IndexSortState* sort = NULL;
    IndexScanDesc scandesc = NULL;
    List* indexTList = NIL;
    int64 ntids = 0;
    bool doscan = false;

    if (state->m_cstoreBitmapIndexScan == NULL)
        return ExecCStoreScan((CStoreScanState*)state);
    else {
        CstoreBitmapIndexScanState* node = state->m_cstoreBitmapIndexScan;

        /* fetch the sort state for cgin index */
        sort = node->m_sort;
        scandesc = node->biss_ScanDesc;
        indexTList = node->m_indexScanTList;

        /* must provide our own instrumentation support */
        if (node->ss.ps.instrument)
            InstrStartNode(node->ss.ps.instrument);

        /*
         * extract necessary information from index scan node
         */
        scandesc = node->biss_ScanDesc;

        if (!node->biss_RuntimeKeysReady && (node->biss_NumRuntimeKeys != 0 || node->biss_NumArrayKeys != 0)) {
            if (node->ss.isPartTbl) {
                if (PointerIsValid(node->biss_IndexPartitionList)) {
                    node->ss.ss_ReScan = true;

                    VecExecReScan((PlanState*)state);
                    doscan = node->biss_RuntimeKeysReady;
                } else {
                    doscan = false;
                }
            } else {
                VecExecReScan((PlanState*)state);
                doscan = node->biss_RuntimeKeysReady;
            }
        } else {
            if (node->ss.isPartTbl && !PointerIsValid(node->biss_IndexPartitionList)) {
                doscan = false;
            } else {
                doscan = true;
            }
        }

        Assert(sort != NULL);
        tids = sort->m_tids;

        if (!sort->m_tidEnd) {
            /* check if need to switch hash bucket */
            if (unlikely(hbkt_idx_need_switch_bkt(scandesc, node->ss.ps.hbktScanSlot.currSlot))) {
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("hash bucket is not supported in column store.")));
            }
            while (doscan) {
                tids->Reset(true);
                if (likely(!RELATION_OWN_BUCKET(scandesc->indexRelation))) {
                    ntids = index_column_getbitmap(scandesc, (const void*)sort, tids);
                } else {
                    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("hash bucket is not supported in column store.")));
                }
                CHECK_FOR_INTERRUPTS();

                doscan = ExecIndexAdvanceArrayKeys(node->biss_ArrayKeys, node->biss_NumArrayKeys);
                if (doscan) /* reset index scan */
                    scan_handler_idx_rescan(node->biss_ScanDesc, node->biss_ScanKeys, node->biss_NumScanKeys, NULL, 0);
            }

            RunSorter(sort);

            /* must provide our own instrumentation support */
            if (node->ss.ps.instrument)
                InstrStopNode(node->ss.ps.instrument, ntids);
        }

        FetchBatchFromSorter(sort, tids);

        return tids;
    }
}

/* ----------------------------------------------------------------
 *                    ExecEndCstoreIndexCtidScan
 * ----------------------------------------------------------------
 */
void ExecEndCstoreIndexCtidScan(CStoreIndexCtidScanState* state)
{
    if (state->m_cstoreBitmapIndexScan == NULL) {
        ExecEndCStoreScan((CStoreScanState*)state, true);
    } else {
        if (state->m_cstoreBitmapIndexScan->m_sort->m_sortState) {
            batchsort_end(state->m_cstoreBitmapIndexScan->m_sort->m_sortState);
            state->m_cstoreBitmapIndexScan->m_sort->m_sortState = NULL;
        }
        ExecEndBitmapIndexScan((BitmapIndexScanState*)state->m_cstoreBitmapIndexScan);
    }
}

void ExecInitNextPartitionForIndexCtidScan(CStoreIndexCtidScanState* state)
{
    Relation currentIdxRel;
    CStoreScan* plan = NULL;
    int paramno = -1;
    ParamExecData* param = NULL;

    plan = (CStoreScan*)state->ps.plan;

    /* get partition sequnce */
    paramno = plan->plan.paramno;
    param = &(state->ps.state->es_param_exec_vals[paramno]);
    state->currentSlot = (int)param->value;

    currentIdxRel = (Relation)list_nth(state->partitions, state->currentSlot);

    state->ss_currentPartition = currentIdxRel;
    state->ss_currentRelation = currentIdxRel;

    state->m_CStore->InitPartReScan(state->ss_currentRelation);
}

/* ----------------------------------------------------------------
 *                    ExecReScanCstoreIndexCtidScan
 * ----------------------------------------------------------------
 */
void ExecReScanCstoreIndexCtidScan(CStoreIndexCtidScanState* state)
{
    if (state->m_cstoreBitmapIndexScan == NULL) {
        if (state->isPartTbl) {
            if (PointerIsValid(state->partitions)) {
                ExecInitNextPartitionForIndexCtidScan(state);
            }
        }

        if (state->m_ScanRunTimeKeysNum != 0) {
            ExecReSetRuntimeKeys(state);
        }
        state->m_ScanRunTimeKeysReady = true;

        state->m_CStore->InitReScan();
    } else {
        /* column bitmap index rescan */
        Assert(state->m_cstoreBitmapIndexScan != NULL);
        state->m_cstoreBitmapIndexScan->ss.ps.plan->paramno = state->ps.plan->paramno;
        if (list_length(state->m_cstoreBitmapIndexScan->m_indexScanTList) != 0) {
            /* Reset the sorter state */
            ResetSorter(state->m_cstoreBitmapIndexScan->m_sort);
            state->m_cstoreBitmapIndexScan->m_sort->m_tidEnd = false;

            ExecReScanBitmapIndexScan((BitmapIndexScanState*)state->m_cstoreBitmapIndexScan);
            EliminateDuplicateScalarArrayElem(state->m_cstoreBitmapIndexScan);
        }
    }
}

/*
 * handle with where clause is like that t1.a in [value1, values2, ..., valueN], as for cbtree index, 
 * in cstoreIndexCtidScan,we get each scalar element from [value1, values2, ..., valueN] clause, then scan cbtree.
 * if the [value1, values2, ..., valueN] clause have duplicate data, cstoreIndexCtidScan will scan duplicate result
 * set, so we must eliminate.
 */
static void EliminateDuplicateScalarArrayElem(BitmapIndexScanState* node)
{
    IndexScanDesc scan = (IndexScanDesc)node->biss_ScanDesc;
    int16* indoption = scan->indexRelation->rd_indoption;
    int i;

    /* Now process each array key. */
    for (i = 0; i < node->biss_NumArrayKeys; i++) {
        ScanKey cur;
        int num_elems;
        int num_nonnulls = 0;
        int j;
        IndexArrayKeyInfo* arrayKey = &(node->biss_ArrayKeys[i]);
        cur = &node->biss_ScanKeys[i];

        for (j = 0; j < arrayKey->num_elems; j++) {
            if (!arrayKey->elem_nulls[j]) {
                arrayKey->elem_nulls[num_nonnulls] = false;
                arrayKey->elem_values[num_nonnulls++] = arrayKey->elem_values[j];
            }
        }

        /* If there's no non-nulls, the scan qual is unsatisfiable. */
        if (num_nonnulls == 0) {
            node->biss_NumArrayKeys = -1;
            break;
        }

        /*
        * Sort the non-null elements and eliminate any duplicates. We must
        * sort in the same ordering used by the index column, so that the
        * successive primitive indexscans produce data in index order.
        */
        num_elems = _bt_sort_array_elements(scan,
                                            cur,
                                            (indoption[cur->sk_attno - 1] & INDOPTION_DESC) != 0,
                                            arrayKey->elem_values,
                                            num_nonnulls);
        arrayKey->num_elems = num_elems;
        /* because sort the arrayKey->elem_values var, we must init value of scan_key->sk_argument again. */
        arrayKey->scan_key->sk_argument = arrayKey->elem_values[0];
        /* the first element is not null. */
        arrayKey->scan_key->sk_flags &= ~SK_ISNULL;
    }

    /* scan->keyData will be used to scan cbtree, set the sorted condition. */
    if (node->biss_ScanKeys && scan->numberOfKeys > 0) {
        errno_t rc = memmove_s(scan->keyData, (size_t)scan->numberOfKeys * sizeof(ScanKeyData),
            node->biss_ScanKeys, (size_t)scan->numberOfKeys * sizeof(ScanKeyData));
        securec_check(rc, "\0", "\0");
    }
}

