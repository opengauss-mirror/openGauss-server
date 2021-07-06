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
 * cstore_delete.h
 *        routines to support ColStore
 *
 *
 * IDENTIFICATION
 *        src/include/access/cstore_delete.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef CSTORE_DELETE_H
#define CSTORE_DELETE_H

#include "access/cstore_am.h"
#include "utils/batchsort.h"
#include "access/cstore_vector.h"

class CStoreDelete : public BaseObject {
public:
    CStoreDelete(_in_ Relation rel, _in_ EState *estate, _in_ bool is_update_cu, _in_ Plan *plan,
                 _in_ MemInfoArg *ArgmemInfo);
    virtual ~CStoreDelete();
    virtual void Destroy();
    void InitDeleteMemArg(Plan *plan, MemInfoArg *ArgmemInfo);
    void ExecDelete(_in_ Relation rel, _in_ ScalarVector *vecRowId, _in_ Snapshot snapshot, _in_ Oid tableOid);
    void InitSortState();
    void InitSortState(_in_ TupleDesc sortTupDesc, _in_ int partidAttNo, _in_ int ctidAttNo);
    void PutDeleteBatch(_in_ VectorBatch *batch, _in_ JunkFilter *junkfilter);
    void PutDeleteBatchForUpdate(_in_ VectorBatch *batch, _in_ int startIdx=0, _in_ int endIdx=-1);
    void PartialDelete();
    uint64 ExecDelete();
    void setReportErrorForUpdate(bool isEnable)
    {
        m_isRptRepeatTupErrForUpdate = isEnable;
    };
    /* record memory auto spread info for delete sort. */
    MemInfoArg *m_DelMemInfo;

protected:
    void CollectPartDeltaOids();
    void UpdateVCBitmap(_in_ Relation rel, _in_ uint32 cuid, _in_ const int *rowoffset, _in_ int updateRowNum,
                        _in_ Snapshot snapshot);
    void InitDeleteSortStateForTable(_in_ TupleDesc sortTupDesc, _in_ int partidAttNo, _in_ int ctidAttNo);
    void InitDeleteSortStateForPartition(_in_ TupleDesc sortTupDesc, _in_ int partidAttNo, _in_ int ctidAttNo);
    void PutDeleteBatchForTable(_in_ VectorBatch *batch, _in_ JunkFilter *junkfilter);
    void PutDeleteBatchForPartition(_in_ VectorBatch *batch, _in_ JunkFilter *junkfilter);
    void PutDeleteBatchForUpdate(_in_ VectorBatch *batch, _in_ JunkFilter *junkfilter);
    uint64 ExecDeleteForTable();
    uint64 ExecDeleteForPartition();
    bool IsFull() const;
    void ResetSortState();

    Relation m_relation;
    Relation m_deltaRealtion;
    List *m_partDeltaOids;

    bool m_isUpdate;

    EState *m_estate;

    TupleDesc m_sortTupDesc;
    Batchsortstate *m_deleteSortState;
    int m_partidIdx;
    int m_ctidIdx;
    bool m_isTupDescCreateBySelf;

    VectorBatch *m_sortBatch;
    int *m_rowOffset;

    int m_maxSortNum;
    int m_curSortedNum;
    uint64 m_totalDeleteNum;

    // report repeat delete tuple error for update
    bool m_isRptRepeatTupErrForUpdate;

    void (CStoreDelete::*m_InitDeleteSortStatePtr)(_in_ TupleDesc sortTupDesc, _in_ int partidAttNo,
                                                   _in_ int ctidAttNo);
    void (CStoreDelete::*m_PutDeleteBatchPtr)(_in_ VectorBatch *batch, _in_ JunkFilter *junkfilter);
    uint64 (CStoreDelete::*m_ExecDeletePtr)();
};

#endif
