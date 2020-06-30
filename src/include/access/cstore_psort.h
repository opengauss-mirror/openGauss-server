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
 * cstore_psort.h
 *        routines to support ColStore
 *
 *
 * IDENTIFICATION
 *        src/include/access/cstore_psort.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef CSTOREPSORT_H
#define CSTOREPSORT_H

#include "access/cstore_vector.h"
#include "vecexecutor/vectorbatch.h"
#include "utils/batchsort.h"
#include "utils/tuplesort.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "storage/cu.h"

enum PSORT_TYPE {
    TUPLE_SORT = 0,
    BATCH_SORT
};
extern THR_LOCAL int psort_work_mem;

#define InvalidBathCursor (-1)
#define BathCursorIsValid(_c) ((_c) > InvalidBathCursor)

class CStorePSort : public BaseObject {
public:
    CStorePSort(Relation rel, AttrNumber *sortKeys, int keyNum, int type, MemInfoArg *m_memInfo = NULL);

    virtual ~CStorePSort();

    virtual void Destroy();
    void InitPsortMemArg(MemInfoArg *ArgmemInfo);

    void PutVecBatch(Relation rel, VectorBatch *pVecBatch);

    void PutBatchValues(bulkload_rows *batchRowPtr);

    void PutTuple(_in_ Datum *values, _in_ bool *nulls);

    void PutSingleTuple(Datum *values, bool *nulls);

    void RunSort();

    void GetBatchValue(bulkload_rows *batchRowsPtr);

    VectorBatch *GetVectorBatch();

    TupleTableSlot *GetTuple();

    void Reset(bool endFlag);

    bool IsFull() const;

    int GetRowNum() const;

    void ResetTupleSortState(bool endFlag);

    void ResetBatchSortState(bool endFlag);

    void GetBatchValueFromTupleSort(bulkload_rows *batchRowsPtr);

    void GetBatchValueFromBatchSort(bulkload_rows *batchRowsPtr);

    Tuplesortstate *m_tupleSortState;
    Batchsortstate *m_batchSortState;
    VectorBatch *m_vecBatch;
    TupleTableSlot *m_tupleSlot;

private:
#ifdef USE_ASSERT_CHECKING
    void AssertCheck(void);
#endif
    int m_type;

    Relation m_rel;
    AttrNumber *m_sortKeys;
    int m_keyNum;

    Oid *m_sortOperators;
    Oid *m_sortCollations;
    bool *m_nullsFirst;

    TupleDesc m_tupDesc;

    int m_curSortedRowNum;
    int m_partialClusterRowNum;
    int m_fullCUSize;
    // current index of tuple to fetch.
    int m_vecBatchCursor;

    MemoryContext m_psortMemContext;

    Datum *m_val;

    bool *m_null;

    MemInfoArg *m_psortMemInfo;

    void (CStorePSort::*m_funcGetBatchValue)(bulkload_rows *batchRowsPtr);

    void (CStorePSort::*m_funcReset)(bool endFlag);
};

#endif
