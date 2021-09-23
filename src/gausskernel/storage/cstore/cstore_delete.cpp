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
 * cstore_delete.cpp
 *      routines to support ColStore
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/cstore/cstore_delete.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/cstore_delete.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/indexing.h"
#include "catalog/pg_type.h"
#include "executor/executor.h"
#include "storage/cucache_mgr.h"
#include "storage/custorage.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/fmgroids.h"
#include "utils/numeric.h"
#include "utils/numeric_gs.h"
#include "utils/relcache.h"
#include "utils/snapmgr.h"
#include "access/heapam.h"
#include "utils/typcache.h"

// tupledesc id
#define SORT_COL_PARTID 1
#define SORT_COL_TID 2

#define ARRAY_2_LEN 2

CStoreDelete::CStoreDelete(
    _in_ Relation rel, _in_ EState* estate, _in_ bool is_update_cu, _in_ Plan* plan, _in_ MemInfoArg* ArgmemInfo)
    : m_deltaRealtion(NULL),
      m_partDeltaOids(NIL),
      m_estate(estate),
      m_sortTupDesc(NULL),
      m_deleteSortState(NULL),
      m_partidIdx(0),
      m_ctidIdx(0),
      m_isTupDescCreateBySelf(false),
      m_sortBatch(NULL),
      m_rowOffset(NULL),
      m_curSortedNum(0),
      m_totalDeleteNum(0),
      m_isRptRepeatTupErrForUpdate(false)
{
    m_relation = rel;
    m_DelMemInfo = NULL;
    /* get partial sort num */
    m_maxSortNum = RelationGetPartialClusterRows(rel);

    /* temp buffer */
    m_rowOffset = (int*)palloc(sizeof(int) * RelMaxFullCuSize);

    InitDeleteMemArg(plan, ArgmemInfo);
    /* function routine */
    bool isPartition = RELATION_IS_PARTITIONED(rel);
    if (!isPartition) {
        m_InitDeleteSortStatePtr = &CStoreDelete::InitDeleteSortStateForTable;
        m_ExecDeletePtr = &CStoreDelete::ExecDeleteForTable;
        m_deltaRealtion = heap_open(m_relation->rd_rel->reldeltarelid, RowExclusiveLock);
    } else {
        m_InitDeleteSortStatePtr = &CStoreDelete::InitDeleteSortStateForPartition;
        m_ExecDeletePtr = &CStoreDelete::ExecDeleteForPartition;
        CollectPartDeltaOids();
    }

    if (RelationIsPAXFormat(rel)) {
        m_PutDeleteBatchPtr = &CStoreDelete::PutDeleteBatchForTable;
    } else {
        m_PutDeleteBatchPtr = &CStoreDelete::PutDeleteBatchForPartition;
    }

    /* set update flag */
    m_isUpdate = is_update_cu;
}

CStoreDelete::~CStoreDelete()
{
    m_deleteSortState = NULL;
    m_sortBatch = NULL;
    m_partDeltaOids = NULL;
    m_DelMemInfo = NULL;
    m_estate = NULL;
    m_sortTupDesc = NULL;
    m_rowOffset = NULL;
    m_deltaRealtion = NULL;
    m_relation = NULL;
}

void CStoreDelete::Destroy()
{
    if (m_sortTupDesc && m_isTupDescCreateBySelf) {
        FreeTupleDesc(m_sortTupDesc);
        m_sortTupDesc = NULL;
    }

    if (m_sortBatch) {
        pfree(m_sortBatch);
        m_sortBatch = NULL;
    }

    if (m_rowOffset) {
        pfree(m_rowOffset);
        m_rowOffset = NULL;
    }

    if (m_DelMemInfo) {
        pfree_ext(m_DelMemInfo);
    }

    if (m_deleteSortState) {
        batchsort_end(m_deleteSortState);
        m_deleteSortState = NULL;
    }

    if (m_deltaRealtion) {
        heap_close(m_deltaRealtion, NoLock);
        m_deltaRealtion = NULL;
    }

    if (list_length(m_partDeltaOids) > 0) {
        list_free(m_partDeltaOids);
        m_partDeltaOids = NIL;
    }
}

void CStoreDelete::CollectPartDeltaOids()
{
    Relation pgpartition = NULL;
    TableScanDesc scan = NULL;
    HeapTuple tuple = NULL;
    ScanKeyData keys[ARRAY_2_LEN];
    Form_pg_partition partitionFrom = NULL;

    /* Process all partitions of this partitiond table */
    ScanKeyInit(&keys[0],
                Anum_pg_partition_parttype,
                BTEqualStrategyNumber,
                F_CHAREQ,
                CharGetDatum(PART_OBJ_TYPE_TABLE_PARTITION));

    ScanKeyInit(&keys[1],
                Anum_pg_partition_parentid,
                BTEqualStrategyNumber,
                F_OIDEQ,
                ObjectIdGetDatum(RelationGetRelid(m_relation)));

    pgpartition = heap_open(PartitionRelationId, AccessShareLock);
    scan = tableam_scan_begin(pgpartition, SnapshotNow, ARRAY_2_LEN, keys);

    while ((tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection)) != NULL) {
        partitionFrom = (Form_pg_partition)GETSTRUCT(tuple);
        m_partDeltaOids = lappend_oid(m_partDeltaOids, partitionFrom->reldeltarelid);
    }
    tableam_scan_end(scan);
    heap_close(pgpartition, AccessShareLock);
}

/*
 * @Description: init delete memory info for cstore delete. There are three branches, plan is the optimizer
 *    estimation parameter passed to the storage layer for execution; ArgmemInfo is to execute the operator
 *	 from the upper layer to pass the parameter to the delete; others is uncontrolled memory.
 * @IN plan: If delete operator is directly used, the plan mem_info is given to execute.
 * @IN ArgmemInfo: ArgmemInfo is used to passing parameters of mem_info to execute, such as update.
 * @Return: void
 * @See also: InitInsertMemArg
 */
void CStoreDelete::InitDeleteMemArg(Plan* plan, MemInfoArg* ArgmemInfo)
{
    m_DelMemInfo = (MemInfoArg*)palloc0(sizeof(struct MemInfoArg));
    if (plan != NULL && plan->operatorMemKB[0] > 0) {
        m_DelMemInfo->canSpreadmaxMem = plan->operatorMaxMem;
        m_DelMemInfo->MemInsert = 0;
        m_DelMemInfo->MemSort =
            plan->operatorMemKB[0] ? plan->operatorMemKB[0] : u_sess->attr.attr_storage.psort_work_mem;
        m_DelMemInfo->spreadNum = 0;
        MEMCTL_LOG(DEBUG2,
                   "CStoreDelete(init plan):Insert workmem is : %dKB, sort workmem: %dKB,can spread maxMem is %dKB.",
                   m_DelMemInfo->MemInsert,
                   m_DelMemInfo->MemSort,
                   m_DelMemInfo->canSpreadmaxMem);
    } else if (ArgmemInfo != NULL) {
        m_DelMemInfo->canSpreadmaxMem = ArgmemInfo->canSpreadmaxMem;
        m_DelMemInfo->MemInsert = ArgmemInfo->MemInsert;
        m_DelMemInfo->MemSort = ArgmemInfo->MemSort;
        m_DelMemInfo->spreadNum = ArgmemInfo->spreadNum;
        MEMCTL_LOG(DEBUG2,
                   "CStoreDelete(init ArgmemInfo):Insert workmem is : %dKB, sort workmem: %dKB,can spread maxMem is %dKB.",
                   m_DelMemInfo->MemInsert,
                   m_DelMemInfo->MemSort,
                   m_DelMemInfo->canSpreadmaxMem);
    } else {
        m_DelMemInfo->canSpreadmaxMem = 0;
        m_DelMemInfo->MemInsert = 0;
        m_DelMemInfo->MemSort = u_sess->attr.attr_storage.psort_work_mem;
        m_DelMemInfo->spreadNum = 0;
    }
    m_DelMemInfo->partitionNum = 1;
}

void CStoreDelete::InitSortState()
{
    // init tupledesc, use partid and tid for sort
    m_sortTupDesc = CreateTemplateTupleDesc(ARRAY_2_LEN, false);
    m_isTupDescCreateBySelf = true;
    TupleDescInitEntry(m_sortTupDesc, SORT_COL_PARTID, "partid", OIDOID, -1, 0);
    TupleDescInitEntry(m_sortTupDesc, SORT_COL_TID, "tid", TIDOID, -1, 0);

    m_partidIdx = SORT_COL_PARTID;
    m_ctidIdx = SORT_COL_TID;

    // sort cache
    m_sortBatch = New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, m_sortTupDesc);

    // init sort state
    (this->*m_InitDeleteSortStatePtr)(m_sortTupDesc, SORT_COL_PARTID, SORT_COL_TID);
}

void CStoreDelete::InitSortState(TupleDesc sortTupDesc, int partidIdx, int ctidIdx)
{
    m_sortTupDesc = sortTupDesc;
    m_partidIdx = partidIdx;
    m_ctidIdx = ctidIdx;

    // sort cache
    m_sortBatch = New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, m_sortTupDesc);

    // init sort state
    (this->*m_InitDeleteSortStatePtr)(sortTupDesc, partidIdx, ctidIdx);

    // change m_PutDeleteBatchPtr
    m_PutDeleteBatchPtr = &CStoreDelete::PutDeleteBatchForUpdate;
}

void CStoreDelete::PutDeleteBatch(_in_ VectorBatch* batch, _in_ JunkFilter* junkfilter)
{
    (this->*m_PutDeleteBatchPtr)(batch, junkfilter);

    if (IsFull()) {
        // execute delete if parital sort is full
        PartialDelete();
    }

    return;
}

void CStoreDelete::PartialDelete()
{
    m_totalDeleteNum += (uint64)(this->*m_ExecDeletePtr)();

    // parital delete need commad id ++
    CommandCounterIncrement();

    // reset sort state
    ResetSortState();
}

uint64 CStoreDelete::ExecDelete()
{
    m_totalDeleteNum += (uint64)(this->*m_ExecDeletePtr)();

    // clean partition fake rel cache
    if (m_estate->esfRelations) {
        FakeRelationCacheDestroy(m_estate->esfRelations);
    }

    return m_totalDeleteNum;
}

void CStoreDelete::InitDeleteSortStateForTable(TupleDesc sortTupDesc, int /* partidAttNo */, int ctidAttNo)
{
    Assert(sortTupDesc);
    Assert(ctidAttNo <= sortTupDesc->natts);

    const int nkeys = 1;
    AttrNumber attNums[1];
    Oid sortCollations[1];
    bool nullsFirstFlags[1];
    int SortMem = u_sess->attr.attr_storage.psort_work_mem;
    int maxMem = 0;

    TypeCacheEntry* typeEntry = lookup_type_cache(TIDOID, TYPECACHE_LT_OPR | TYPECACHE_GT_OPR);

    attNums[0] = ctidAttNo;
    sortCollations[0] = sortTupDesc->attrs[ctidAttNo - 1]->attcollation;
    nullsFirstFlags[0] = false;

    SortMem = m_DelMemInfo->MemSort > 0 ? m_DelMemInfo->MemSort : u_sess->attr.attr_storage.psort_work_mem;
    maxMem = m_DelMemInfo->canSpreadmaxMem > 0 ? m_DelMemInfo->canSpreadmaxMem : 0;

    m_deleteSortState = batchsort_begin_heap(
                            sortTupDesc, nkeys, attNums, &typeEntry->lt_opr, sortCollations, nullsFirstFlags, SortMem, false, maxMem);
}

void CStoreDelete::InitDeleteSortStateForPartition(TupleDesc sortTupDesc, int partidAttNo, int ctidAttNo)
{
    Assert(sortTupDesc);
    Assert(partidAttNo <= sortTupDesc->natts);
    Assert(ctidAttNo <= sortTupDesc->natts);

    const int nkeys = ARRAY_2_LEN;
    AttrNumber attNums[ARRAY_2_LEN];
    Oid sortOperators[ARRAY_2_LEN];
    Oid sortCollations[ARRAY_2_LEN];
    bool nullsFirstFlags[ARRAY_2_LEN];
    int SortMem = u_sess->attr.attr_storage.psort_work_mem;
    int maxMem = 0;

    attNums[0] = partidAttNo;
    attNums[1] = ctidAttNo;

    TypeCacheEntry* typeEntry = lookup_type_cache(OIDOID, TYPECACHE_LT_OPR | TYPECACHE_GT_OPR);
    sortOperators[0] = typeEntry->lt_opr;
    typeEntry = lookup_type_cache(TIDOID, TYPECACHE_LT_OPR | TYPECACHE_GT_OPR);
    sortOperators[1] = typeEntry->lt_opr;

    sortCollations[0] = sortTupDesc->attrs[partidAttNo - 1]->attcollation;
    sortCollations[1] = sortTupDesc->attrs[ctidAttNo - 1]->attcollation;

    nullsFirstFlags[0] = false;
    nullsFirstFlags[1] = false;

    SortMem = m_DelMemInfo->MemSort > 0 ? m_DelMemInfo->MemSort : u_sess->attr.attr_storage.psort_work_mem;
    maxMem = m_DelMemInfo->canSpreadmaxMem > 0 ? m_DelMemInfo->canSpreadmaxMem : 0;

    m_deleteSortState = batchsort_begin_heap(
                            m_sortTupDesc, nkeys, attNums, sortOperators, sortCollations, nullsFirstFlags, SortMem, false, maxMem);
}

void CStoreDelete::PutDeleteBatchForTable(_in_ VectorBatch* batch, _in_ JunkFilter* junkfilter)
{
    Assert(batch && junkfilter && m_sortBatch && m_deleteSortState);

    int tididx = junkfilter->jf_junkAttNo;
    Assert(tididx - 1 < batch->m_cols);

    // shallow copy
    m_sortBatch->m_arr[m_ctidIdx - 1].copy(&batch->m_arr[tididx - 1]);

    m_sortBatch->m_rows = batch->m_rows;

    // put batch
    m_deleteSortState->sort_putbatch(m_deleteSortState, m_sortBatch, 0, m_sortBatch->m_rows);

    m_curSortedNum += batch->m_rows;
}

void CStoreDelete::PutDeleteBatchForPartition(_in_ VectorBatch* batch, _in_ JunkFilter* junkfilter)
{
    Assert(batch && junkfilter && m_sortBatch && m_deleteSortState);

    int tididx = junkfilter->jf_junkAttNo;
    int partidx = junkfilter->jf_xc_part_id;
    Assert(tididx - 1 < batch->m_cols);
    Assert(partidx - 1 < batch->m_cols);

    // shallow copy
    m_sortBatch->m_arr[m_partidIdx - 1].copy(&batch->m_arr[partidx - 1]);
    m_sortBatch->m_arr[m_ctidIdx - 1].copy(&batch->m_arr[tididx - 1]);

    m_sortBatch->m_rows = batch->m_rows;

    // put batch
    m_deleteSortState->sort_putbatch(m_deleteSortState, m_sortBatch, 0, m_sortBatch->m_rows);

    m_curSortedNum += batch->m_rows;
}

void CStoreDelete::PutDeleteBatchForUpdate(_in_ VectorBatch* batch, _in_ JunkFilter* /* junkfilter */)
{
    Assert(batch && m_sortBatch && m_deleteSortState);

    // shallow copy all batch data
    m_sortBatch->Copy<false, false>(batch);

    // put batch
    m_deleteSortState->sort_putbatch(m_deleteSortState, m_sortBatch, 0, m_sortBatch->m_rows);
    m_curSortedNum += batch->m_rows;
}

void CStoreDelete::PutDeleteBatchForUpdate(_in_ VectorBatch* batch, _in_ int startIdx, _in_ int endIdx) {
    Assert(batch && m_sortBatch && m_deleteSortState);

    /* copy batch data */
    m_sortBatch->Copy<false, false>(batch, startIdx, endIdx);

    /* put batch */
    m_deleteSortState->sort_putbatch(m_deleteSortState, m_sortBatch, 0, m_sortBatch->m_rows);
    m_curSortedNum += m_sortBatch->m_rows;
}

uint64 CStoreDelete::ExecDeleteForTable()
{
    Assert(m_relation && m_estate && m_sortBatch && m_deleteSortState && m_rowOffset);

    uint32 lastCUID = InValidCUID;
    uint32 lastOffset = 0;
    int delRowNum = 0;
    uint64 delTotalRowNum = 0;
    Oid deltaTableid = RelationGetRelid(m_deltaRealtion);

    (void)GetCurrentTransactionId();

    m_sortBatch->Reset(true);

    // run sort
    batchsort_performsort(m_deleteSortState);

    batchsort_getbatch(m_deleteSortState, true, m_sortBatch);

    while (!BatchIsNull(m_sortBatch)) {
        // get tid col
        ScalarValue* tableidValues = m_sortBatch->m_arr[m_partidIdx - 1].m_vals;
        ScalarValue* tidValues = m_sortBatch->m_arr[m_ctidIdx - 1].m_vals;
        ItemPointer lastRowTid = NULL;

        for (int i = 0; i < m_sortBatch->m_rows; i++) {
            ItemPointer tid = (ItemPointer)(tidValues + i);

            if (deltaTableid == tableidValues[i]) {
                /*
                 * for delete, allow replicated tid;
                 * for update, don't allow replicated tid;
                 */
                if (lastRowTid != NULL && ItemPointerEquals(lastRowTid, tid)) {
                    if (m_isRptRepeatTupErrForUpdate) {
                        ereport(ERROR, (errcode(ERRCODE_CARDINALITY_VIOLATION),
                                        errmsg("Non-deterministic UPDATE"),
                                        errdetail("multiple updates to a row by a single query for column store table.")));
                    }
                    continue;
                }

                simple_heap_delete(m_deltaRealtion, tid);
                lastRowTid = tid;
                delTotalRowNum++;
                continue;
            }

            uint32 curCUID = ItemPointerGetBlockNumber(tid);
            uint32 curOffset = ItemPointerGetOffsetNumber(tid) - 1;

            Assert(IsValidCUID(curCUID));

            if (!IsValidCUID(lastCUID)) {
                // first CU
                lastCUID = curCUID;
            } else if (lastCUID != curCUID) {
                // modify delete bitmap in same CU
                UpdateVCBitmap(m_relation, lastCUID, m_rowOffset, delRowNum, m_estate->es_snapshot);

                // switch CU
                lastCUID = curCUID;
                delTotalRowNum += delRowNum;
                delRowNum = 0;
            } else if (lastOffset == curOffset) {
                // repeat ctid
                Assert(lastCUID == curCUID);

                if (m_isRptRepeatTupErrForUpdate) {
                    ereport(ERROR, (errcode(ERRCODE_CARDINALITY_VIOLATION),
                                    errmsg("Non-deterministic UPDATE"),
                                    errdetail("multiple updates to a row by a single query for column store table.")));
                }

                continue;
            }

            // record offset
            m_rowOffset[delRowNum++] = curOffset;
            lastOffset = curOffset;
            Assert(delRowNum <= DefaultFullCUSize);
        }

        // get next sorted batch
        batchsort_getbatch(m_deleteSortState, true, m_sortBatch);
    }

    if (delRowNum > 0) {
        UpdateVCBitmap(m_relation, lastCUID, m_rowOffset, delRowNum, m_estate->es_snapshot);
        delTotalRowNum += delRowNum;
    }

    return delTotalRowNum;
}

uint64 CStoreDelete::ExecDeleteForPartition()
{
    Assert(m_relation && m_estate && m_sortBatch && m_deleteSortState && m_rowOffset);

    Relation partFakeRel = NULL;
    Partition partition = NULL;

    Oid lastPartID = InvalidOid;
    uint32 lastCUID = InValidCUID;
    uint32 lastOffset = 0;

    int delRowNum = 0;
    uint64 delTotalRowNum = 0;

    (void)GetCurrentTransactionId();

    m_sortBatch->Reset(true);

    // run sort
    batchsort_performsort(m_deleteSortState);

    batchsort_getbatch(m_deleteSortState, true, m_sortBatch);

    /*
     * ExecDeleteForPartition may be invoked by CStore update multi times.
     * We should close the m_deltaRelation opened last time.
     */
    if (m_deltaRealtion != NULL) {
        relation_close(m_deltaRealtion, NoLock);
        m_deltaRealtion = NULL;
    }

    while (!BatchIsNull(m_sortBatch)) {
        ScalarValue* partidValues = m_sortBatch->m_arr[m_partidIdx - 1].m_vals;
        ScalarValue* tidValues = m_sortBatch->m_arr[m_ctidIdx - 1].m_vals;
        ItemPointer lastRowTid = NULL;

        for (int i = 0; i < m_sortBatch->m_rows; i++) {
            Oid curPartID = DatumGetObjectId(*(partidValues + i));
            Assert(curPartID != InvalidOid);

            ItemPointer tid = (ItemPointer)(tidValues + i);

            /* for delta table */
            if (list_member_oid(m_partDeltaOids, curPartID)) {
                if (lastPartID == InvalidOid) {
                    // first Partition
                    lastPartID = curPartID;

                    /* open first delta partition */
                    m_deltaRealtion = relation_open(curPartID, RowExclusiveLock);
                    lastRowTid = NULL;
                } else if (lastPartID != curPartID) {
                    /* close the last delta relation */
                    if (m_deltaRealtion != NULL)
                        relation_close(m_deltaRealtion, NoLock);

                    /* switch Partition */
                    lastPartID = curPartID;

                    /* open the next delta partition */
                    m_deltaRealtion = relation_open(curPartID, RowExclusiveLock);
                    lastRowTid = NULL;
                }

                /*
                 * for delete, allow replicated tid;
                 * for update, don't allow replicated tid;
                 */
                if (lastRowTid != NULL && ItemPointerEquals(lastRowTid, tid)) {
                    if (m_isRptRepeatTupErrForUpdate) {
                        ereport(ERROR, (errcode(ERRCODE_CARDINALITY_VIOLATION),
                                        errmsg("Non-deterministic UPDATE"),
                                        errdetail("multiple updates to a row by a single query for column store table.")));
                    }
                    continue;
                }

                simple_heap_delete(m_deltaRealtion, tid);
                delTotalRowNum++;
                continue;
            }

            uint32 curCUID = ItemPointerGetBlockNumber(tid);
            uint32 curOffset = ItemPointerGetOffsetNumber(tid) - 1;
            Assert(IsValidCUID(curCUID));

            if (lastPartID == InvalidOid) {
                // first Partition and CU
                lastPartID = curPartID;
                lastCUID = curCUID;

                // get partition fake relation
                searchFakeReationForPartitionOid(m_estate->esfRelations,
                                                 m_estate->es_query_cxt,
                                                 m_relation,
                                                 curPartID,
                                                 partFakeRel,
                                                 partition,
                                                 RowExclusiveLock);
            } else if (lastPartID != curPartID) {
                /* Partiton changed
                 *
                 * modify delete bitmap
                 */
                UpdateVCBitmap(partFakeRel, lastCUID, m_rowOffset, delRowNum, m_estate->es_snapshot);

                // switch Partition and CU
                lastPartID = curPartID;
                lastCUID = curCUID;
                delTotalRowNum += delRowNum;
                delRowNum = 0;
                partFakeRel = NULL;
                partition = NULL;

                // get partition fake relation
                searchFakeReationForPartitionOid(m_estate->esfRelations,
                                                 m_estate->es_query_cxt,
                                                 m_relation,
                                                 curPartID,
                                                 partFakeRel,
                                                 partition,
                                                 RowExclusiveLock);
                Assert(partFakeRel);
            } else if (lastCUID != curCUID) {
                // CU changed in same Partition
                Assert(lastPartID == curPartID);

                // modify delete bitmap
                if (delRowNum > 0)
                    UpdateVCBitmap(partFakeRel, lastCUID, m_rowOffset, delRowNum, m_estate->es_snapshot);

                // switch CU
                lastCUID = curCUID;
                delTotalRowNum += delRowNum;
                delRowNum = 0;
            } else if (lastOffset == curOffset) {
                // repeat ctid
                Assert((lastPartID == curPartID) && (lastCUID == curCUID));

                if (m_isRptRepeatTupErrForUpdate) {
                    ereport(ERROR, (errcode(ERRCODE_CARDINALITY_VIOLATION),
                                    errmsg("Non-deterministic UPDATE"),
                                    errdetail("multiple updates to a row by a single query for column store table.")));
                }

                continue;
            }

            // record offset
            m_rowOffset[delRowNum++] = curOffset;
            lastOffset = curOffset;
            Assert(delRowNum <= DefaultFullCUSize);
        }

        // get next sorted batch
        batchsort_getbatch(m_deleteSortState, true, m_sortBatch);
    }

    if (delRowNum > 0) {
        UpdateVCBitmap(partFakeRel, lastCUID, m_rowOffset, delRowNum, m_estate->es_snapshot);
        delTotalRowNum += delRowNum;
    }

    return delTotalRowNum;
}

// ExecDelete
// Mark del_bitmap of CUDesc according to vecRowId
void CStoreDelete::ExecDelete(_in_ Relation rel, _in_ ScalarVector* vecRowId, _in_ Snapshot snapshot, _in_ Oid tableOid)
{
    uint32 lastCUID = InValidCUID;
    int delRowNum = 0;
    ScalarValue* values = vecRowId->m_vals;
    uint32 curCUID, curOffset;
    Oid deltaOid = RelationGetRelid(m_deltaRealtion);
    ItemPointer lastRowTid = NULL;
    GetCurrentTransactionId();

    for (int i = 0; i < vecRowId->m_rows; ++i) {
        ItemPointer tid = (ItemPointer)(values + i);

        /* for delta table */
        if (tableOid == deltaOid) {
            /*
             * for delete, allow replicated tid;
             * for update, don't allow replicated tid;
             */
            if (lastRowTid != NULL && ItemPointerEquals(lastRowTid, tid)) {
                if (m_isRptRepeatTupErrForUpdate) {
                    ereport(ERROR, (errcode(ERRCODE_CARDINALITY_VIOLATION),
                                    errmsg("Non-deterministic UPDATE"),
                                    errdetail("multiple updates to a row by a single query for column store table.")));
                }
                continue;
            }
            simple_heap_delete(m_deltaRealtion, tid);
            continue;
        }

        curCUID = ItemPointerGetBlockNumber(tid);
        curOffset = ItemPointerGetOffsetNumber(tid) - 1;

        Assert(IsValidCUID(curCUID));

        if (!IsValidCUID(lastCUID))
            lastCUID = curCUID;
        else if (lastCUID != curCUID) {
            UpdateVCBitmap(rel, lastCUID, m_rowOffset, delRowNum, snapshot);
            lastCUID = curCUID;
            delRowNum = 0;
        }

        m_rowOffset[delRowNum++] = curOffset;
    }

    if (delRowNum > 0) {
        UpdateVCBitmap(rel, lastCUID, m_rowOffset, delRowNum, snapshot);
    }
}

/*
 * Bitmask of virtual column represent which row are deleted
 * This function change the bitmap of corresponding CUs.
 * And update the bitmap of CUs of VC.
 */
void CStoreDelete::UpdateVCBitmap(
    _in_ Relation rel, _in_ uint32 cuid, _in_ const int* rowoffset, _in_ int delRowNum, _in_ Snapshot snapshot)
{
Retry:
    ScanKeyData key[2];
    HeapTuple tmpTup = NULL, oldTup = NULL, newTup = NULL;
    bool isnull = false;
    unsigned char* delMask = NULL;
    unsigned char* oldDelMask = NULL;
    errno_t rc = EOK;

    /*
     * Open the CUDesc relation and its index
     */
    Relation cudesc_rel = heap_open(rel->rd_rel->relcudescrelid, RowExclusiveLock);
    Relation idx_rel = index_open(cudesc_rel->rd_rel->relcudescidx, RowExclusiveLock);
    TupleDesc cudesc_tupdesc = cudesc_rel->rd_att;

    Assert(CUDescMaxAttrNum == cudesc_tupdesc->natts);

    ScanKeyInit(&key[0], (AttrNumber)CUDescColIDAttr, BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(VitrualDelColID));

    ScanKeyInit(&key[1], (AttrNumber)CUDescCUIDAttr, BTEqualStrategyNumber, F_OIDEQ, UInt32GetDatum(cuid));

    snapshot = SnapshotNow;
    SysScanDesc cudesc_scan = systable_beginscan_ordered(cudesc_rel, idx_rel, snapshot, 2, key);

    // Step 1: Get delMask and modify it
    // Because of snapshotNow, you might see different versions of tuple when scan for long.
    // just need get one to deal with, modify while->if 20150508
    // If get the old one, it might show 'delete or update row conflict'
    // If get the new one, it will continue to deal with the new one
    ItemPointerData oldTupCtid;
    if ((tmpTup = systable_getnext_ordered(cudesc_scan, ForwardScanDirection)) != NULL) {
        Assert(newTup == NULL);
        oldTup = tmpTup;
        uint32 rowCount = DatumGetUInt32(fastgetattr(oldTup, CUDescRowCountAttr, cudesc_tupdesc, &isnull));
        Assert(isnull == false);

        // Get delMask
        int delMaskBytes = (rowCount + 7) / 8;
        delMask = (unsigned char*)palloc0(delMaskBytes);

        char* cuPtr = DatumGetPointer(fastgetattr(oldTup, CUDescCUPointerAttr, cudesc_tupdesc, &isnull));

        // Delbitmap is not null
        // This CU has deleted rows before
        if (isnull == false) {
            char* detoastPtr = (char*)PG_DETOAST_DATUM(cuPtr);
            Assert((int)VARSIZE_ANY_EXHDR(detoastPtr) == delMaskBytes);
            rc = memcpy_s(delMask, delMaskBytes, VARDATA_ANY(detoastPtr), VARSIZE_ANY_EXHDR(detoastPtr));
            securec_check(rc, "", "");

            // if *detoastPtr* is a new space, we will free it
            // the first time when it's useless.
            if (detoastPtr != cuPtr) {
                pfree_ext(detoastPtr);
            }
        }

        oldDelMask = (unsigned char*)palloc(delMaskBytes);
        rc = memcpy_s(oldDelMask, delMaskBytes, delMask, delMaskBytes);
        securec_check(rc, "", "");

        // Modify delMask
        for (int i = 0; i < delRowNum; ++i) {
            uint32 row = (uint32)rowoffset[i];

            // This row have been deleted by other transaction
            if (oldDelMask[row >> 3] & (1 << (row % 8))) {
                ereport(
                    ERROR, (errcode(ERRCODE_CARDINALITY_VIOLATION), errmsg("These rows have been deleted or updated")));
            }

            delMask[row >> 3] |= (1 << (row % 8));
        }

        pfree(oldDelMask);

        // Form new tuple using new delMask
        newTup =
            CStore::FormVCCUDescTup(cudesc_tupdesc, (char*)delMask, cuid, rowCount, GetCurrentTransactionIdIfAny());
        oldTupCtid = oldTup->t_self;
    };

    systable_endscan_ordered(cudesc_scan);
    index_close(idx_rel, RowExclusiveLock);

    // Step 2: update del_bitmap
    TM_Result result = TM_Invisible;
    TM_FailureData tmfd;

    if (newTup) {
        result = tableam_tuple_update(cudesc_rel,
            NULL,
            &oldTupCtid,
            newTup,
            GetCurrentCommandId(true),
            InvalidSnapshot,
            InvalidSnapshot,
            true,
            NULL,
            &tmfd,
            NULL,   // we don't need update_indexes
            NULL,   // we don't meed modifiedIdxAttrs
            false);

        switch (result) {
            case TM_SelfUpdated:
            case TM_SelfModified: {
                // Now It is TM_SelfModified
                ereport(ERROR,
                        (errcode(ERRCODE_LOCK_NOT_AVAILABLE), (errmsg("delete or update failed because lock conflict"))));
                break;
            }
            case TM_Ok: {
                CatalogUpdateIndexes(cudesc_rel, newTup);
                break;
            }

            case TM_Updated:
            case TM_Deleted: {
                heap_freetuple(newTup);
                newTup = NULL;
                heap_close(cudesc_rel, NoLock);
                goto Retry;
            }

            default: {
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_OPERATION),
                         (errmsg("CStore: unrecognized heap_update status: %u", result))));
                break;
            }
        }
        heap_freetuple(newTup);
        newTup = NULL;
    } else {
        /*
         * because
         * 1. when BTREE index scan is used in forward direction,
         *	  maybe first new tuple and then older tuple is matched;
         * 2. for SnapshotNow, xid status may be changed from processing
         *	  to commited between two tuples.
         * so it's possible that no tuple is found druing this search with
         * SnapshotNow and index scan. once it happens, error reporting.
         */
        ereport(ERROR, (errcode(ERRCODE_CARDINALITY_VIOLATION), errmsg("delete or update row conflict")));
    }
    heap_close(cudesc_rel, NoLock);
    /*
     * if we do update, we don't count delete num,
     * dead_tuple will increase when commit, see
     * function AtEOXact_PgStat.
     */
    if (!m_isUpdate)
        pgstat_count_cu_delete(rel, delRowNum);
}

bool CStoreDelete::IsFull() const
{
    // m_maxSortNum <= 0 means  full sort
    return m_maxSortNum <= 0 ? false : m_curSortedNum >= m_maxSortNum;
}

void CStoreDelete::ResetSortState()
{
    // release last sort state
    if (m_deleteSortState) {
        batchsort_end(m_deleteSortState);
        m_deleteSortState = NULL;
    }

    m_curSortedNum = 0;

    // init new sort state
    return (this->*m_InitDeleteSortStatePtr)(m_sortTupDesc, m_partidIdx, m_ctidIdx);
}
