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
 * vecdfsindexscan.cpp
 *    Routines to support indexed scans of dfs store relations
 *
 *    Notes :
 *    This module is higly assemble to the dfs scan code. We shall
 *    merge the common part.
 *
 * IDENTIFICATION
 *        Code/src/gausskernel/runtime/vecexecutor/vecnode/vecdfsindexscan.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "access/dfs/dfs_query.h"
#include "postgres.h"
#include "knl/knl_variable.h"

#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "access/genam.h"
#include "access/relscan.h"
#include "executor/exec/execdebug.h"
#include "executor/node/nodeIndexonlyscan.h"
#include "executor/node/nodeIndexscan.h"
#include "nodes/makefuncs.h"
#include "optimizer/clauses.h"
#include "utils/array.h"
#include "utils/batchsort.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"
#include "vecexecutor/vecnodecstorescan.h"
#include "vecexecutor/vecnodedfsindexscan.h"

inline static VectorBatch* ScanByTids(DfsIndexScanState* node);
static List* BuildDfsPreparedCols(List* scan_target_list, List* index_list);
static void DfsPrepareBtreeIndex(IndexScanDesc& scan_desc, List*& index_list, DfsIndexScanState* node);

template <IndexType type>
void ExecDfsIndexScanT(DfsIndexScanState* node)
{
    /* should never reach here */
}

template <>
void ExecDfsIndexScanT<PSORT_INDEX>(DfsIndexScanState* node)
{
    VectorBatch* tids = NULL;
    IndexSortState* sort = node->m_sort;

    /* step 1: sort all tids */
    if (!sort->m_tidEnd) {
        /* Fill the sorter until it is full or there is no more tids. */
        do {
            /* scan psort index */
            tids = ExecCStoreScan(node->m_indexScan);

            if (BatchIsNull(tids)) {
                sort->m_tidEnd = true;
            } else {
                PutBatchToSorter(sort, tids);
            }
        } while (!sort->m_tidEnd);

        /* Execute the sort process. */
        RunSorter(sort);
    }

    /* step 2: fetch ordered tids */
    FetchBatchFromSorter(sort, sort->m_tids);
}

template <>
void ExecDfsIndexScanT<PSORT_INDEX_ONLY>(DfsIndexScanState* node)
{
    VectorBatch* tids = NULL;
    IndexSortState* sort = node->m_sort;

    /* fetch tids by cstore scan. */
    if (!sort->m_tidEnd) {
        tids = ExecCStoreScan(node->m_indexScan);

        if (BatchIsNull(tids)) {
            sort->m_tidEnd = true;
        }
    }

    sort->m_tids = tids;
}

template <>
void ExecDfsIndexScanT<BTREE_INDEX>(DfsIndexScanState* node)
{
    VectorBatch* tids = NULL;
    IndexSortState* sort = node->m_sort;
    IndexScanDesc scan_desc = NULL;
    List* index_list = NIL;

    DfsPrepareBtreeIndex(scan_desc, index_list, node);

    /* step 1: sort all tids */
    if (!sort->m_tidEnd) {
        /* Fill the sorter until it is full or there is no more tids. */
        do {
            /* fetch all the tids and put them into sorter. */
            tids = sort->m_tids;
            tids->Reset(true);
            FetchTids(scan_desc, index_list, &node->ps, sort, tids, node->m_indexonly);
            PutBatchToSorter(sort, tids);
        } while (!sort->m_tidEnd);

        /* Execute the sort process. */
        RunSorter(sort);
    }

    /* step 2: fetch ordered tids from sorter */
    FetchBatchFromSorter(sort, sort->m_tids);
}

template <>
void ExecDfsIndexScanT<BTREE_INDEX_ONLY>(DfsIndexScanState* node)
{
    IndexSortState* sort = node->m_sort;
    IndexScanDesc scan_desc = NULL;
    List* index_list = NIL;

    DfsPrepareBtreeIndex(scan_desc, index_list, node);

    /* reset the tids vector batch */
    sort->m_tids->Reset(true);

    /* fetch a new tids vector batch */
    if (!sort->m_tidEnd) {
        FetchTids(scan_desc, index_list, &node->ps, sort, sort->m_tids, node->m_indexonly);
    }
}

/* ----------------------------------------------------------------
 *      ExecDfsIndexScan
 * ----------------------------------------------------------------
 */
VectorBatch* ExecDfsIndexScan(DfsIndexScanState* node)
{
    VectorBatch* out_batch = NULL;
    IndexSortState* sort = node->m_sort;

    for (;;) {
        /* Fetch a tid vector batch, which is stored in sort->m_tids. */
        node->m_dfsIndexScanFunc(node);

        /* When the tid is over, return NULL and stop the scan */
        if (BatchIsNull(sort->m_tids)) {
            return NULL;
        }

        /* Scan base relation by tids. */
        out_batch = ScanByTids(node);
        if (!BatchIsNull(out_batch)) {
            break;
        }
    }
    return out_batch;
}

/* ----------------------------------------------------------------
 *      ExecInitDfsIndexScan
 *
 *      Initializes the index scan's state information, creates
 *      scan keys, and opens the base and index relations.
 *
 *      Note: index scans have 2 sets of state information because
 *            we have to keep track of the base relation and the
 *            index relation.
 * ----------------------------------------------------------------
 */
DfsIndexScanState* ExecInitDfsIndexScan(DfsIndexScan* node, EState* estate, int eflags)
{
    DfsIndexScanState* index_state = NULL;
    CStoreScan* index_scan = NULL;
    DfsScanState* dfs_scan_state = NULL;
    errno_t rc = EOK;

    /* Sanity checks */
    if (node->indexorderby != NULL || node->indexorderbyorig != NULL) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("ORDER BY with column index is not supported")));
    }
    if (!ScanDirectionIsForward(estate->es_direction)) {
        ereport(
            ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("column index scan only supports forward scan")));
    }

    /*
     * Create state structure for current plan.
     */
    index_state = makeNode(DfsIndexScanState);

    /* Initialize the dfsscanstate for base relation. */
    node->dfsScan->plan.targetlist = node->scan.plan.targetlist;
    dfs_scan_state = ExecInitDfsScan(node->dfsScan, NULL, estate, eflags, false);
    ((Plan*)node)->qual = node->dfsScan->plan.qual;
    ((Plan*)node)->hasHashFilter = node->dfsScan->plan.hasHashFilter;
    rc = memcpy_s(index_state, sizeof(DfsScanState), dfs_scan_state, sizeof(DfsScanState));
    securec_check(rc, "\0", "\0");

    index_state->ps.plan = (Plan*)node;
    index_state->ps.state = estate;
    index_state->ps.vectorized = true;
    index_state->ps.type = T_DfsIndexScanState;
    index_state->m_indexonly = node->indexonly;

    /*
     * If we are just doing EXPLAIN (ie, aren't going to run the plan), stop
     * here. This allows an index-advisor plugin to EXPLAIN a plan containing
     * references to nonexistent indexes.
     */
    if ((uint32)eflags & EXEC_FLAG_EXPLAIN_ONLY)
        return index_state;

    bool rel_is_target = ExecRelationIsTargetRelation(estate, node->scan.scanrelid);
    int estimate_rows_count = (int)node->scan.plan.plan_rows;
    Relation index_rel = index_open(node->indexid, rel_is_target ? NoLock : AccessShareLock);
    if (index_rel->rd_rel->relam == PSORT_AM_OID) {
        /*
         * Initialize column index scan. Create a state for column index scan. It
         * shall use index's Oid, indexqual and the target list shall include the
         * final output columns together with the tid which can be found in index
         * relation(taken care of by optimizer).
         */
        index_scan = makeNode(CStoreScan);
        index_scan->scanrelid = node->indexid;
        index_scan->plan.targetlist = node->indexScantlist;

        /*
         * Since we do not support the "And" clause in Qual right now in CStoreIndexScan,
         * After 'index_scan' in ExecCStoreIndexScan we need to recheck all the quals to
         * ensure the results is right.
         */
        index_scan->plan.qual = node->indexqual;
        index_scan->cstorequal = node->cstorequal;

        /* Psort index relation is not a partition table for now. */
        index_scan->isPartTbl = node->scan.isPartTbl;
        index_scan->itrs = node->scan.itrs;
        index_scan->pruningInfo = node->scan.pruningInfo;
        index_scan->partScanDirection = node->scan.partScanDirection;
        index_scan->selectionRatio = 0.01;  // description: need optimizer to tell

        /* Initialize the cstorescanstate for index relation. */
        index_state->m_indexScan = ExecInitCStoreScan(index_scan, NULL, estate, eflags, true);
        index_state->part_id = index_state->m_indexScan->part_id;
        index_state->m_btreeIndexScan = NULL;
        index_state->m_btreeIndexOnlyScan = NULL;

        /* prepare the index sort state */
        index_state->m_sort = (IndexSortState*)palloc0(sizeof(IndexSortState));
        index_state->m_sort->m_sortTupleDesc = index_state->m_indexScan->ps.ps_ResultTupleSlot->tts_tupleDescriptor;
        index_state->m_sort->m_sortCount = 0;
        index_state->m_sort->m_tids =
            New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, index_state->m_sort->m_sortTupleDesc);
        index_state->m_sort->m_sortState =
            InitTidSortState(index_state->m_sort->m_sortTupleDesc, index_state->m_sort->m_sortTupleDesc->natts);
        index_state->m_sort->m_tidEnd = false;
        if (!index_state->m_indexonly) {
            index_state->m_dfsIndexScanFunc = ExecDfsIndexScanT<PSORT_INDEX>;
        } else {
            /* For a bit rows, sort is needed for index only scan to obtain better performance. */
            if (estimate_rows_count <= DefaultFullCUSize)
                index_state->m_dfsIndexScanFunc = ExecDfsIndexScanT<PSORT_INDEX>;
            else
                index_state->m_dfsIndexScanFunc = ExecDfsIndexScanT<PSORT_INDEX_ONLY>;
        }
        index_close(index_rel, NoLock);
    } else {
        /* cbtree index */
        if (!index_state->m_indexonly) {
            /* cbtree index scan */
            CBTreeScanState* btree_index_scan = makeNode(CBTreeScanState);
            btree_index_scan->m_indexScanTList = node->indexScantlist;
            BuildCBtreeIndexScan(btree_index_scan,
                (ScanState*)dfs_scan_state,
                (Scan*)node,
                estate,
                index_rel,
                node->indexqual,
                node->indexorderby);
            index_state->m_btreeIndexScan = btree_index_scan;
            index_state->part_id = index_state->m_btreeIndexScan->ss.part_id;
            index_state->m_btreeIndexOnlyScan = NULL;
            index_state->m_sort = btree_index_scan->m_sort;
            index_state->m_dfsIndexScanFunc = ExecDfsIndexScanT<BTREE_INDEX>;
        } else {
            /* cbtree index only scan */
            CBTreeOnlyScanState* btree_index_only_scan = makeNode(CBTreeOnlyScanState);
            btree_index_only_scan->m_indexScanTList = node->indexScantlist;
            BuildCBtreeIndexOnlyScan(btree_index_only_scan,
                (ScanState*)dfs_scan_state,
                (Scan*)node,
                estate,
                index_rel,
                node->indexqual,
                node->indexorderby);
            index_state->m_btreeIndexOnlyScan = btree_index_only_scan;
            index_state->part_id = index_state->m_btreeIndexOnlyScan->ss.part_id;
            index_state->m_btreeIndexScan = NULL;
            index_state->m_sort = btree_index_only_scan->m_sort;

            /* For a bit rows, sort is needed for index only scan to obtain better performance. */
            if (estimate_rows_count <= DefaultFullCUSize)
                index_state->m_dfsIndexScanFunc = ExecDfsIndexScanT<BTREE_INDEX>;
            else
                index_state->m_dfsIndexScanFunc = ExecDfsIndexScanT<BTREE_INDEX_ONLY>;
        }

        index_state->m_indexScan = NULL;
    }

    /* Build the list of columns which need not to be read by dfs scan. */
    index_state->m_dfsPreparedCols = BuildDfsPreparedCols(node->indexScantlist, node->indextlist);

    return index_state;
}

/* ----------------------------------------------------------------
 *      ExecEndDfsIndexScan
 * ----------------------------------------------------------------
 */
void ExecEndDfsIndexScan(DfsIndexScanState* node)
{
    /* Clean the sorter state */
    if (node->m_sort && node->m_sort->m_sortState) {
        batchsort_end(node->m_sort->m_sortState);
        node->m_sort->m_sortState = NULL;
    }

    /* End the psort index scan state(column store) */
    if (node->m_indexScan) {

        /* Set the llvm flag. */
        if (node->m_indexScan->jitted_vecqual) {
            if (HAS_INSTR(node, false)) {
                node->ps.instrument->isLlvmOpt = true;
            }
        }
        ExecEndCStoreScan(node->m_indexScan, true);
    } else if (node->m_btreeIndexScan != NULL) {
        /* End the btree index scan state */
        ExecEndIndexScan((IndexScanState*)node->m_btreeIndexScan);
    } else if (node->m_btreeIndexOnlyScan != NULL) {
        /* End the btree index only scan state */
        ExecEndIndexOnlyScan((IndexOnlyScanState*)node->m_btreeIndexOnlyScan);
    }

    /* End the dfs scan state(dfs store) */
    ExecEndDfsScan((DfsScanState*)node);
}

/* ----------------------------------------------------------------
 *      ExecReScanIndexScan(node)
 * ----------------------------------------------------------------
 */
void ExecReScanDfsIndexScan(DfsIndexScanState* node)
{
    CStoreScanState* index_node_state = node->m_indexScan;
    CBTreeScanState* btree_index_scan_state = node->m_btreeIndexScan;
    CBTreeOnlyScanState* btree_Index_only_scan_state = node->m_btreeIndexOnlyScan;
    DfsIndexScan* index_scan = (DfsIndexScan*)(node->ps.plan);
    bool need_short_cuit = (list_length(index_scan->indexScantlist) == 0) ? true : false;

    /* Reset the sorter state */
    if (node->m_sort && !need_short_cuit) {
        ResetSorter(node->m_sort);
        node->m_sort->m_tidEnd = false;
    }

    if (index_node_state != NULL) {
        index_node_state->ps.plan->paramno = ((Plan*)index_scan)->paramno;

        /* Rescan the index scan state(column store) */
        ExecReScanCStoreScan(index_node_state);
    } else if (btree_index_scan_state != NULL) {
        /* cbtree index rescan */
        btree_index_scan_state->ss.ps.plan->paramno = ((Plan*)index_scan)->paramno;
        if (!need_short_cuit)
            ExecReScanIndexScan((IndexScanState*)btree_index_scan_state);
    } else if (btree_Index_only_scan_state != NULL) {
        /* cbtree index only rescan */
        btree_Index_only_scan_state->ss.ps.plan->paramno = ((Plan*)index_scan)->paramno;
        if (!need_short_cuit)
            ExecReScanIndexOnlyScan((IndexOnlyScanState*)btree_Index_only_scan_state);
    }

    /* Rescan the dfs scan state(dfs store) */
    ExecReScanDfsScan((DfsScanState*)node);
}

bool ExecRecheckDfsIndex(DfsIndexScanState* node, VectorBatch* batch)
{
    /* nothing to check */
    return true;
}

/*
 * @Description: Fetch the next batch by tid.
 * @IN node: DfsIndexScanState node
 * @Return: the batch
 * @See also:
 */
static VectorBatch* dfs_index_tid_scan_next(DfsIndexScanState* node)
{
    DfsScanState* scan_state = (DfsScanState*)node;
    MemoryContext context = scan_state->m_scanCxt;
    MemoryContext old_context = NULL;
    VectorBatch* batch = scan_state->m_pScanBatch;

    if (scan_state->m_done || BatchIsNull(node->m_sort->m_tids)) {
        return NULL;
    }

    /* Build run time filter. */
    if (!scan_state->m_readerState->scanstate->runTimePredicatesReady) {
        BuildRunTimePredicates(scan_state->m_readerState);
    }

    /* Fetch the batch according to the tid batch. */
    MemoryContextReset(context);
    old_context = MemoryContextSwitchTo(context);
    if (node->m_indexonly) {
        scan_state->m_fileReader->nextBatchByTidsForIndexOnly(batch, node->m_sort->m_tids, node->m_dfsPreparedCols);
    } else {
        scan_state->m_fileReader->nextBatchByTids(batch, node->m_sort->m_tids);
    }
    node->m_sort->m_tids->Reset(false);

    /* Check soft constraints optimization. */
    if (((Scan*)(scan_state->ps.plan))->predicate_pushdown_optimized && !BatchIsNull(batch)) {
        batch->m_rows = 1;
        batch->FixRowCount();
        scan_state->is_scan_end = true;
    }

    (void)MemoryContextSwitchTo(old_context);

    return batch;
}

/*
 * @Description: intermedia function which calls ExecVecScan
 *		to scan the data.
 * @IN node: DfsIndexScanState node
 * @Return: the batch
 * @See also:
 */
inline static VectorBatch* ScanByTids(DfsIndexScanState* node)
{
    return ExecVecScan(node, (ExecVecScanAccessMtd)dfs_index_tid_scan_next, (ExecVecScanRecheckMtd)ExecRecheckDfsIndex);
}

/*
 * @Description: Build the prepared column list according to the index scan result.
 * @IN scan_target_list: the target list of index scan
 * @IN index_list: the list of index columns
 * @Return: the prepared column list
 * @See also:
 */
static List* BuildDfsPreparedCols(List* scan_target_list, List* index_list)
{
    ListCell* cell = NULL;
    int index_var_no = 0;
    List* prepared_cols = NIL;
    int offset = 0;
    int index_target_number = list_length(scan_target_list) - 1;

    foreach (cell, scan_target_list) {
        if (offset == index_target_number)
            break;

        TargetEntry* index_tle = (TargetEntry*)lfirst(cell);
        index_var_no = ((Var*)index_tle->expr)->varattno;
        TargetEntry* dfs_tle = (TargetEntry*)list_nth(index_list, index_var_no - 1);
        prepared_cols = lappend_int(prepared_cols, ((Var*)dfs_tle->expr)->varattno);
        offset++;
    }

    return prepared_cols;
}

static void DfsPrepareBtreeIndex(IndexScanDesc& scan_desc, List*& index_list, DfsIndexScanState* node)
{
    if (node->m_indexonly) {
        CBTreeOnlyScanState* btree_Index_only_scan_state = node->m_btreeIndexOnlyScan;
        Assert(btree_Index_only_scan_state != NULL);

        /* fetch the sort state for btree index only scan */
        scan_desc = btree_Index_only_scan_state->ioss_ScanDesc;
        index_list = btree_Index_only_scan_state->m_indexScanTList;

        /* set runtime scan keys */
        if (btree_Index_only_scan_state->ioss_NumRuntimeKeys != 0 && !btree_Index_only_scan_state->ioss_RuntimeKeysReady) {
            VecExecReScan((PlanState*)node);
        }
    } else {
        CBTreeScanState* btree_index_scan_state = node->m_btreeIndexScan;

        /* fetch the sort state for btree index */
        scan_desc = btree_index_scan_state->iss_ScanDesc;
        index_list = btree_index_scan_state->m_indexScanTList;

        /* set runtime scan keys */
        if (btree_index_scan_state->iss_NumRuntimeKeys != 0 && !btree_index_scan_state->iss_RuntimeKeysReady) {
            VecExecReScan((PlanState*)node);
        }
    }
}
