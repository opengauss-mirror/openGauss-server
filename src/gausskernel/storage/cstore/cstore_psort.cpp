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
 * cstore_psort.cpp
 *      routines to support ColStore
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/cstore/cstore_psort.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "access/cstore_psort.h"
#include "access/tableam.h"
#include "storage/cu.h"
#include "utils/datum.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/typcache.h"

CStorePSort::CStorePSort(Relation rel, AttrNumber* sortKeys, int keyNum, int type, MemInfoArg* m_memInfo)
    : m_tupleSortState(NULL),
      m_batchSortState(NULL),
      m_vecBatch(NULL),
      m_tupleSlot(NULL),
      m_type(type),
      m_rel(rel),
      m_sortKeys(sortKeys),
      m_keyNum(keyNum),
      m_curSortedRowNum(0),
      m_vecBatchCursor(InvalidBathCursor)
{
    m_tupDesc = m_rel->rd_att;
    FormData_pg_attribute* attr = m_tupDesc->attrs;
    m_psortMemInfo = NULL;

    m_fullCUSize = RelationGetMaxBatchRows(m_rel);
    m_partialClusterRowNum = RelationGetPartialClusterRows(m_rel);
#ifdef USE_ASSERT_CHECKING
    AssertCheck();
#endif

    // Note that these variables should be in parent memoryContex.
    // please free them in deconstructor method.
    //
    m_sortOperators = (Oid*)palloc(sizeof(Oid) * keyNum);
    m_sortCollations = (Oid*)palloc(sizeof(Oid) * keyNum);
    for (int i = 0; i < keyNum; ++i) {
        int colIdx = m_sortKeys[i] - 1;
        m_sortCollations[i] = attr[colIdx].attcollation;

        TypeCacheEntry* typeEntry = lookup_type_cache(attr[colIdx].atttypid, TYPECACHE_LT_OPR | TYPECACHE_GT_OPR);
        m_sortOperators[i] = typeEntry->lt_opr;
    }

    m_nullsFirst = (bool*)palloc(sizeof(bool) * m_keyNum);
    errno_t rc = memset_s(m_nullsFirst, m_keyNum * sizeof(bool), false, m_keyNum * sizeof(bool));
    securec_check(rc, "", "");

    m_val = (Datum*)palloc(sizeof(Datum) * m_tupDesc->natts);
    m_null = (bool*)palloc(sizeof(bool) * m_tupDesc->natts);

    InitPsortMemArg(m_memInfo);
    int sortMem = m_psortMemInfo->MemSort > 0 ? m_psortMemInfo->MemSort : u_sess->attr.attr_storage.psort_work_mem;
    int canSpreadmaxMem = m_psortMemInfo->canSpreadmaxMem;
    /*
     * for partition table psort , the max spread mem needs to be divided to n parts.
     * Here, we assume that not all partitions will exceed the available memory,
     * so for some partitions with large data volume, we can expand by 20% by default.
     * In extreme cases, it is possible to exceed 20% per zone for canSpreadmaxMem.
     */
    if (canSpreadmaxMem && m_psortMemInfo->partitionNum > 1) {
        canSpreadmaxMem = (int)((double)m_psortMemInfo->canSpreadmaxMem / (double)m_psortMemInfo->partitionNum *
                                PSORT_SPREAD_MAXMEM_RATIO);
    }

    m_psortMemContext = AllocSetContextCreate(CurrentMemoryContext,
                                              "PartialSort",
                                              ALLOCSET_DEFAULT_MINSIZE,
                                              ALLOCSET_DEFAULT_INITSIZE,
                                              ALLOCSET_DEFAULT_MAXSIZE);

    // WARNING: all the following variables should be regenerated
    // after m_psortMemContext is reset.
    //
    AutoContextSwitch memContextGuard(m_psortMemContext);

    if (m_type == TUPLE_SORT) {
        m_tupleSortState = tuplesort_begin_heap(m_tupDesc,
                                                m_keyNum,
                                                m_sortKeys,
                                                m_sortOperators,
                                                m_sortCollations,
                                                m_nullsFirst,
                                                sortMem,
                                                false,
                                                canSpreadmaxMem);

        m_funcGetBatchValue = &CStorePSort::GetBatchValueFromTupleSort;

        m_funcReset = &CStorePSort::ResetTupleSortState;

        m_tupleSlot = MakeSingleTupleTableSlot(m_tupDesc);
    } else {
        m_batchSortState = batchsort_begin_heap(m_tupDesc,
                                                m_keyNum,
                                                m_sortKeys,
                                                m_sortOperators,
                                                m_sortCollations,
                                                m_nullsFirst,
                                                sortMem,
                                                false,
                                                canSpreadmaxMem);

        m_funcGetBatchValue = &CStorePSort::GetBatchValueFromBatchSort;

        m_funcReset = &CStorePSort::ResetBatchSortState;

        m_vecBatch = New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, m_tupDesc);
    }
}

CStorePSort::~CStorePSort()
{
    m_vecBatch = NULL;
    m_nullsFirst = NULL;
    m_val = NULL;
    m_sortKeys = NULL;
    m_sortOperators = NULL;
    m_tupleSortState = NULL;
    m_tupleSlot = NULL;
    m_batchSortState = NULL;
    m_null = NULL;
    m_rel = NULL;
    m_psortMemInfo = NULL;
    m_tupDesc = NULL;
    m_sortCollations = NULL;
    m_psortMemContext = NULL;
}

void CStorePSort::Destroy()
{
    if (m_type == TUPLE_SORT && m_tupleSlot != NULL) {
        ExecDropSingleTupleTableSlot(m_tupleSlot);
        m_tupleSlot = NULL;
    }

    // free memory not alloc in m_psortMemContext
    pfree_ext(m_sortOperators);
    pfree_ext(m_sortCollations);
    pfree_ext(m_nullsFirst);
    pfree_ext(m_val);
    pfree_ext(m_null);
    if (m_psortMemInfo) {
        pfree_ext(m_psortMemInfo);
    }

    // free memory alloc in m_psortMemContext
    MemoryContextDelete(m_psortMemContext);
}

/*
 * @Description: init psort memory info for psort.
 * @IN ArgmemInfo: ArgmemInfo is used to passing parameters of mem_info to execute, such as update\insert.
 * @ArgmemInfo->MemSort is one partition sort mem for partition table. For cstore table, it is the table sort Mem.
 * @ArgmemInfo->canSpreadmaxMem is insert Mem and all partitions sort Mem. For psort, we should control the
 * @sort mem for cstore table and partition tables.
 * @Return: void
 */
void CStorePSort::InitPsortMemArg(MemInfoArg* ArgmemInfo)
{
    m_psortMemInfo = (MemInfoArg*)palloc0(sizeof(struct MemInfoArg));
    if (ArgmemInfo != NULL) {
        Assert(ArgmemInfo->partitionNum > 0);
        /*
         * m_psortMemInfo->canSpreadmaxMem is all partitions sort spread Mem for partition table.
         * m_psortMemInfo->MemSort is one partition sort Mem for partition table.
         */
        m_psortMemInfo->canSpreadmaxMem = (ArgmemInfo->canSpreadmaxMem - ArgmemInfo->MemInsert > 0) ?
                                        ArgmemInfo->canSpreadmaxMem - ArgmemInfo->MemInsert :
                                        0;
        m_psortMemInfo->MemInsert = ArgmemInfo->MemInsert;
        m_psortMemInfo->MemSort = ArgmemInfo->MemSort;
        m_psortMemInfo->spreadNum = ArgmemInfo->spreadNum;
        m_psortMemInfo->partitionNum = ArgmemInfo->partitionNum;
        MEMCTL_LOG(DEBUG2,
                   "CStorePSort(init ArgmemInfo):Insert workmem is : %dKB, sort workmem: %dKB,can spread maxMem is %dKB.",
                   m_psortMemInfo->MemInsert,
                   m_psortMemInfo->MemSort,
                   m_psortMemInfo->canSpreadmaxMem);
    } else {
        m_psortMemInfo->canSpreadmaxMem = 0;
        m_psortMemInfo->MemInsert = 0;
        m_psortMemInfo->MemSort = u_sess->attr.attr_storage.psort_work_mem;
        m_psortMemInfo->spreadNum = 0;
        m_psortMemInfo->partitionNum = 1;
    }
}

void CStorePSort::Reset(bool endFlag)
{
    return (this->*m_funcReset)(endFlag);
}

void CStorePSort::PutVecBatch(Relation rel, VectorBatch* pVecBatch)
{
    Assert(m_batchSortState && m_type == BATCH_SORT);
    m_batchSortState->sort_putbatch(m_batchSortState, pVecBatch, 0, pVecBatch->m_rows);
    m_curSortedRowNum += pVecBatch->m_rows;
}

void CStorePSort::PutTuple(Datum* values, bool* nulls)
{
    Assert(m_tupleSortState && m_type == TUPLE_SORT);

    AutoContextSwitch memContextGuard(m_psortMemContext);

    HeapTuple tuple = (HeapTuple)tableam_tops_form_tuple(m_tupDesc, values, nulls);

    TupleTableSlot* slot = MakeSingleTupleTableSlot(m_tupDesc);

    (void)ExecStoreTuple(tuple, slot, InvalidBuffer, false);

    tuplesort_puttupleslot(m_tupleSortState, slot);

    ExecDropSingleTupleTableSlot(slot);

    heap_freetuple(tuple);
    tuple = NULL;
}

void CStorePSort::PutSingleTuple(Datum* values, bool* nulls)
{
    PutTuple(values, nulls);
    m_curSortedRowNum++;
}

void CStorePSort::PutBatchValues(bulkload_rows* batchRowPtr)
{
    AutoContextSwitch memContextGuard(m_psortMemContext);

    bulkload_rows_iter iter;

    iter.begin(batchRowPtr);
    while (iter.not_end()) {
        /* fetch next tuple */
        iter.next(m_val, m_null);

        /* put tuple into sort processor */
        PutTuple(m_val, m_null);
    }
    iter.end();

    m_curSortedRowNum += batchRowPtr->m_rows_curnum;
}

void CStorePSort::RunSort()
{
    AutoContextSwitch memContextGuard(m_psortMemContext);

    if (m_type == TUPLE_SORT)
        tuplesort_performsort(m_tupleSortState);
    else
        batchsort_performsort(m_batchSortState);
}

void CStorePSort::GetBatchValue(bulkload_rows* batchRowsPtr)
{
    return (this->*m_funcGetBatchValue)(batchRowsPtr);
}

VectorBatch* CStorePSort::GetVectorBatch()
{
    Assert(m_batchSortState && m_vecBatch && m_type == BATCH_SORT);
    if (unlikely(!(m_tupleSortState && m_tupleSlot && m_type == TUPLE_SORT))) {
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("Invalid tupleSortState, tupleSlot, or sorting strategy while getting tuple in cstore psort.")));
    }
    m_vecBatch->Reset(true);
    batchsort_getbatch(m_batchSortState, true, m_vecBatch);
    return m_vecBatch;
}

TupleTableSlot* CStorePSort::GetTuple()
{
    Assert(m_tupleSortState && m_tupleSlot && m_type == TUPLE_SORT);
    if (unlikely(!(m_tupleSortState && m_tupleSlot && m_type == TUPLE_SORT))) {
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("Invalid tupleSortState, tupleSlot, or sorting strategy while getting tuple in cstore psort.")));
    }
    if (!TupIsNull(m_tupleSlot))
        (void)ExecClearTuple(m_tupleSlot);
    (void)tuplesort_gettupleslot(m_tupleSortState, true, m_tupleSlot, NULL);
    return m_tupleSlot;
}

bool CStorePSort::IsFull() const
{
    return m_curSortedRowNum >= m_partialClusterRowNum;
}

int CStorePSort::GetRowNum() const
{
    return m_curSortedRowNum;
}

#ifdef USE_ASSERT_CHECKING
inline void CStorePSort::AssertCheck()
{
    Assert((m_partialClusterRowNum >= m_fullCUSize) && ((m_partialClusterRowNum % m_fullCUSize) == 0));
}
#endif

void CStorePSort::ResetTupleSortState(bool endFlag)
{
    AutoContextSwitch memContextGuard(m_psortMemContext);
    int sortMem = u_sess->attr.attr_storage.psort_work_mem;
    int canSpreadMaxMem = 0;

    if (m_tupleSortState) {
        sortMem = m_psortMemInfo->MemSort > 0 ? m_psortMemInfo->MemSort : u_sess->attr.attr_storage.psort_work_mem;
        canSpreadMaxMem = m_psortMemInfo->canSpreadmaxMem > 0 ? m_psortMemInfo->canSpreadmaxMem : 0;

        if (canSpreadMaxMem && m_psortMemInfo->partitionNum > 1) {
            canSpreadMaxMem = (int)((double)m_psortMemInfo->canSpreadmaxMem / (double)m_psortMemInfo->partitionNum *
                                    PSORT_SPREAD_MAXMEM_RATIO);
        }
        tuplesort_end(m_tupleSortState);
        m_tupleSortState = NULL;
    }

    // clear m_tupleSlot
    if (m_tupleSlot != NULL) {
        ExecDropSingleTupleTableSlot(m_tupleSlot);
        m_tupleSlot = NULL;
    }

    // clear memory context
    MemoryContextReset(m_psortMemContext);

    if (!endFlag) {
        m_tupleSortState = tuplesort_begin_heap(m_tupDesc,
                                                m_keyNum,
                                                m_sortKeys,
                                                m_sortOperators,
                                                m_sortCollations,
                                                m_nullsFirst,
                                                sortMem,
                                                false,
                                                canSpreadMaxMem);

        m_tupleSlot = MakeSingleTupleTableSlot(m_tupDesc);
    }
    m_curSortedRowNum = 0;
}

void CStorePSort::ResetBatchSortState(bool endFlag)
{
    AutoContextSwitch memContextGuard(m_psortMemContext);
    int sortMem = u_sess->attr.attr_storage.psort_work_mem;
    int canSpreadMaxMem = 0;

    if (m_batchSortState) {
        sortMem = m_psortMemInfo->MemSort > 0 ? m_psortMemInfo->MemSort : u_sess->attr.attr_storage.psort_work_mem;
        canSpreadMaxMem = m_psortMemInfo->canSpreadmaxMem > 0 ? m_psortMemInfo->canSpreadmaxMem : 0;
        if (canSpreadMaxMem && m_psortMemInfo->partitionNum > 1) {
            canSpreadMaxMem = (int)((double)m_psortMemInfo->canSpreadmaxMem / (double)m_psortMemInfo->partitionNum *
                                    PSORT_SPREAD_MAXMEM_RATIO);
        }
        batchsort_end(m_batchSortState);
        m_batchSortState = NULL;
    }

    // clear memory context
    MemoryContextReset(m_psortMemContext);

    if (!endFlag) {
        m_batchSortState = batchsort_begin_heap(m_tupDesc,
                                                m_keyNum,
                                                m_sortKeys,
                                                m_sortOperators,
                                                m_sortCollations,
                                                m_nullsFirst,
                                                sortMem,
                                                false,
                                                canSpreadMaxMem);

        m_vecBatch = New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, m_tupDesc);
    }
    m_curSortedRowNum = 0;
    m_vecBatchCursor = InvalidBathCursor;
}

void CStorePSort::GetBatchValueFromTupleSort(bulkload_rows* batchRowsPtr)
{
    AutoContextSwitch memContextGuard(m_psortMemContext);
    TupleTableSlot* slot = MakeSingleTupleTableSlot(m_tupDesc);

    // here it isn't a dead loop.
    // when batchRowsPtr is full, break down the loops.
    while (true) {
        if (!tuplesort_gettupleslot(m_tupleSortState, true, slot, NULL)) {
            break;
        }

        heap_deform_tuple((HeapTuple)slot->tts_tuple, m_tupDesc, m_val, m_null);
        if (batchRowsPtr->append_one_tuple(m_val, m_null, m_tupDesc))
            break;
    }

    ExecDropSingleTupleTableSlot(slot);
}

void CStorePSort::GetBatchValueFromBatchSort(bulkload_rows* batchRowsPtr)
{
    AutoContextSwitch memContextGuard(m_psortMemContext);

    // precondition for the following WHILE loop
    //
    Assert(batchRowsPtr->m_rows_maxnum > 0);
    Assert(batchRowsPtr->m_rows_maxnum % BatchMaxSize == 0);
    bool is_start = true;

    if (BathCursorIsValid(m_vecBatchCursor)) {
        // first fetch tuples from last vector batch.
        if (batchRowsPtr->append_one_vector(m_tupDesc, m_vecBatch, &m_vecBatchCursor)) {
            // it's filled with part of VectBatch tuples.
            // m_vecBatchCursor has been updated, and we have to
            // care the special case. reset when all the m_vecBatch is handled.
            if (m_vecBatchCursor == m_vecBatch->m_rows) {
                m_vecBatchCursor = InvalidBathCursor;
            }
            return;
        }

        // it's fine to continue to hold more tuples.
        // first of all reset this cursor.
        m_vecBatchCursor = InvalidBathCursor;
        is_start = false;
    }

    // here it isn't a dead loop.
    // when batchRowsPtr is full, break down the loops.
    while (true) {
        Assert(m_vecBatchCursor == InvalidBathCursor);
        batchsort_getbatch(m_batchSortState, true, m_vecBatch);

        if (BatchIsNull(m_vecBatch)) {
            // break if there isn't any tuple
            break;
        }
        if (is_start) {
            Assert(batchRowsPtr->m_rows_curnum % BatchMaxSize == 0);
        }

        // we have to copy datum, because it belongs to
        // m_vecBatch which is reset by batchsort_getbatch().
        // free all the space by calling Reset(), so see its references.
        //
        m_vecBatchCursor = 0;
        if (batchRowsPtr->append_one_vector(m_tupDesc, m_vecBatch, &m_vecBatchCursor)) {
            // ok, it's filled with part of VectBatch tuples.
            // m_vecBatchCursor has been updated, and we have to
            // care the special case. reset when all the m_vecBatch is handled.
            if (m_vecBatchCursor == m_vecBatch->m_rows) {
                m_vecBatchCursor = InvalidBathCursor;
            }
            break;
        }
        m_vecBatchCursor = InvalidBathCursor;
    }
}
