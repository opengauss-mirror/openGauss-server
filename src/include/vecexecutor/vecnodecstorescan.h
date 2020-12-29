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
 * vecnodecstorescan.h
 * 
 * 
 * IDENTIFICATION
 *        src/include/vecexecutor/vecnodecstorescan.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef VECNODECSTORESCAN_H
#define VECNODECSTORESCAN_H

#include "vecexecutor/vecnodes.h"
#include "nodes/plannodes.h"
#include "access/cstoreskey.h"
#include "vecexecutor/vecexecutor.h"
#include "executor/executor.h"
#include "utils/memutils.h"

typedef enum { BTREE_INDEX, BTREE_INDEX_ONLY, PSORT_INDEX, PSORT_INDEX_ONLY } IndexType;

extern CStoreScanState* ExecInitCStoreScan(CStoreScan* node, Relation parentHeapRel, EState* estate, int eflags,
    bool indexFlag = false, bool codegenInUplevel = false);
extern DfsScanState* ExecInitDfsScan(
    DfsScan* node, Relation parentHeapRel, EState* estate, int eflags, bool indexFlag = false);
extern VectorBatch* ExecCStoreScan(CStoreScanState* node);
extern VectorBatch* ExecDfsScan(DfsScanState* node);
extern void ExecReScanDfsScan(DfsScanState* node);
extern void ExecEndCStoreScan(CStoreScanState* node, bool indexFlag);
extern void ExecEndDfsScan(DfsScanState* node);
extern void ExecReSetRuntimeKeys(CStoreScanState* node);
extern void ExecReScanCStoreScan(CStoreScanState* node);
extern void ExecCStoreBuildScanKeys(CStoreScanState* state, List* quals, CStoreScanKey* scankeys, int* numScanKeys);
extern void ExecReScanCStoreIndexScan(CStoreIndexScanState* node);

extern void OptimizeProjectionAndFilter(CStoreScanState* node);

extern VectorBatch* ApplyProjectionAndFilter(
    CStoreScanState* node, VectorBatch* pScanBatch, ExprDoneCond* isDone = NULL);

extern TupleDesc BuildTupleDescByTargetList(List* tlist);
extern void BuildCBtreeIndexScan(CBTreeScanState* btreeIndexScan, ScanState* scanstate, Scan* node, EState* estate,
    Relation indexRel, List* indexqual, List* indexorderby);
extern void BuildCBtreeIndexOnlyScan(CBTreeOnlyScanState* btreeIndexOnlyScan, ScanState* scanstate, Scan* node,
    EState* estate, Relation indexRel, List* indexqual, List* indexorderby);
extern Batchsortstate* InitTidSortState(TupleDesc sortTupDesc, int tidAttNo, int sortMem = 0, int maxMem = 0);
extern void PutBatchToSorter(IndexSortState* sort, VectorBatch* batch);
extern bool IsSorterEmpty(IndexSortState* sort);
extern void ResetSorter(IndexSortState* sort);
extern void RunSorter(IndexSortState* sort);
extern void FetchBatchFromSorter(IndexSortState* sort, VectorBatch* tids);
extern void FetchTids(IndexScanDesc scandesc, List* indexScanTargetList, PlanState* ps, IndexSortState* sort,
    VectorBatch* tids, bool indexOnly);

#endif /* NODECSTORESCAN_H */

