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
 * cstore_update.h
 *        routines to support ColStore
 *
 *
 * IDENTIFICATION
 *        src/include/access/cstore_update.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef CSTORE_UPDATE_H
#define CSTORE_UPDATE_H

#include "access/cstore_am.h"
#include "access/cstore_delete.h"
#include "access/cstore_insert.h"

class CStoreUpdate : public BaseObject {
public:
    CStoreUpdate(_in_ Relation rel, _in_ EState *estate, _in_ Plan *plan);
    virtual ~CStoreUpdate();
    virtual void Destroy();
    void InitSortState(TupleDesc sortTupDesc);
    uint64 ExecUpdate(_in_ VectorBatch *batch, _in_ int options);
    void EndUpdate(_in_ int options);
    void InitUpdateMemArg(Plan *plan);
    MemInfoArg *m_delMemInfo;
    MemInfoArg *m_insMemInfo;

private:
    void BatchDeleteAndInsert(VectorBatch *batch, int oriBatchCols, int options, JunkFilter *junkfilter);
    void PartitionBatchDeleteAndInsert(VectorBatch *batch, int oriBatchCols, int options, JunkFilter *junkfilter);

    bool CheckHasUniqueIdx();

    Relation m_relation;

    CStoreDelete *m_delete;
    CStoreInsert *m_insert;
    CStorePartitionInsert *m_partionInsert;

    ResultRelInfo *m_resultRelInfo;
    EState *m_estate;
    bool m_isPartition;
    bool m_hasUniqueIdx;
    static int BATCHROW_TIMES;

    InsertArg m_insert_args;
};

#endif
