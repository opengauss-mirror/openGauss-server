/*
 * Copyright (c) 2024 Huawei Technologies Co.,Ltd.
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
 * imcstore_insert.cpp
 *      routines to support IMColStore
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/htap/imcstore_insert.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "storage/cu.h"
#include "access/htap/imcucache_mgr.h"
#include "access/htap/imcs_ctlg.h"
#include "access/cstore_insert.h"
#include "access/htap/imcustorage.h"
#include "access/htap/imcstore_insert.h"


/*
 * @Description: Constructor used only by imcstore
 * @IN relation: relation
 * @IN imcsTupleDescWithCtid: imcstore tuple desc with ctid col
 * @IN imcsAttsNum: attribute number of imcstored cols
 * @See also:
 */
IMCStoreInsert::IMCStoreInsert(
    _in_ Relation relation, _in_ TupleDesc imcsTupleDescWithCtid, _in_ int2vector* imcsAttsNum)
{
    /* create imcstore rel, because imcstore contains ctid col, and supports conversion ofa few cols */
    Relation fakeImcsRel = CreateFakeRelcacheEntry(relation->rd_node);
    fakeImcsRel->pgstat_info = relation->pgstat_info;
    fakeImcsRel->rd_att = imcsTupleDescWithCtid;
    m_relation = fakeImcsRel;
    m_relOid = RelationGetRelid(relation);

    m_isImcstore = true;
    m_cuCmprsOptions = (compression_options*)palloc(sizeof(compression_options) * (imcsTupleDescWithCtid->natts));

    for (int i = 0; i < imcsTupleDescWithCtid->natts; ++i) {
        m_cuCmprsOptions[i].reset();
    }

    /* init cu batch ptr */
    m_tmpBatchRows = New(CurrentMemoryContext) bulkload_rows(imcsTupleDescWithCtid, MAX_IMCS_ROWS_ONE_CU(relation),
        true);
    m_tmpBatchRows->EnableImcs(imcsAttsNum, relation->rd_att->natts);

    /// set the compression level and extra modes.
    m_compress_modes = 0;
    InitFuncPtr();
}

IMCStoreInsert::~IMCStoreInsert()
{
    m_relation = NULL;
    m_setMinMaxFuncs = NULL;
    m_cuCmprsOptions = NULL;
    m_formCUFuncArray = NULL;
}

void IMCStoreInsert::Destroy()
{
    if (m_tmpBatchRows != NULL) {
        DELETE_EX(m_tmpBatchRows);
    }

    int attNo = m_relation->rd_att->natts;
    for (int col = 0; col < attNo; ++col) {
        if (m_cuDescPPtr[col]) {
            delete m_cuDescPPtr[col];
            m_cuDescPPtr[col] = NULL;
        }
    }
    m_cuDescPPtr = NULL;

    FreeFakeRelcacheEntry(m_relation);
    pfree_ext(m_setMinMaxFuncs);
    pfree_ext(m_cuCmprsOptions);
    pfree_ext(m_formCUFuncArray);
}

void IMCStoreInsert::BatchInsertCommon(uint32 cuid)
{
    if (unlikely(m_tmpBatchRows == NULL || m_tmpBatchRows->m_rows_curnum == 0))
        return;

    CUDesc* cuDescPtr = NULL;
    CU* cuPtr = NULL;
    RowGroup* rowGroup = NULL;
    MemoryContext oldcontext = NULL;
    CHECK_FOR_INTERRUPTS();
    IMCSDesc* imcsDesc = IMCU_CACHE->GetImcsDesc(m_relOid);
    if (unlikely(imcsDesc == NULL)) {
        ereport(ERROR, (errmodule(MOD_HTAP),
            (errmsg("Fail to enable imcs for rel(%d), imcstore desc not exists.", m_relOid))));
    }

    PG_TRY();
    {
        oldcontext = MemoryContextSwitchTo(imcsDesc->imcuDescContext);
        rowGroup = imcsDesc->rowGroups[cuid];

        for (int col = 0; col < m_relation->rd_att->natts; ++col) {
            /* form CU and CUDesc */
            cuDescPtr = New(CurrentMemoryContext) CUDesc();
            cuPtr = FormCU(col, m_tmpBatchRows, cuDescPtr);
            m_cuCmprsOptions[col].m_sampling_finished = true;
            cuDescPtr->cu_id = cuid;
            cuDescPtr->cu_pointer = (uint64)0;
            /* save cu and cudesc */
            IMCU_CACHE->SaveCU(imcsDesc, (RelFileNodeOld*)&m_relation->rd_node, col, cuPtr, cuDescPtr);
            rowGroup->m_cuDescs[col] = cuDescPtr;
            cuDescPtr = NULL;
            cuPtr = NULL;
        }
        rowGroup->m_actived = true;
        MemoryContextSwitchTo(oldcontext);
    }
    PG_CATCH();
    {
        if (cuPtr != NULL) {
            DELETE_EX(cuPtr);
        }
        if (cuDescPtr != NULL) {
            DELETE_EX(cuDescPtr);
        }
        MemoryContextSwitchTo(oldcontext);
        PG_RE_THROW();
    }
    PG_END_TRY();
}

void IMCStoreInsert::BatchReInsertCommon(IMCSDesc* imcsDesc, uint32 cuid, TransactionId xid)
{
    if (unlikely(m_tmpBatchRows == NULL))
        return;
    int col = 0;

    MemoryContext oldcontext = MemoryContextSwitchTo(imcsDesc->imcuDescContext);
    RowGroup* rowGroup = imcsDesc->GetRowGroup(cuid);

    if (m_tmpBatchRows->m_rows_curnum == 0) {
        rowGroup->Vacuum(m_relation, imcsDesc, nullptr, nullptr, xid);
        imcsDesc->UnReferenceRowGroup();
        return;
    }

    CUDesc* cuDescPtr = NULL;
    CU* cuPtr = NULL;

    CUDesc** tmpCUDescs = (CUDesc**)palloc0((m_relation->rd_att->natts) * sizeof(CUDesc*));
    CU** tmpCUs = (CU**)palloc0((m_relation->rd_att->natts) * sizeof(CU*));

    for (col = 0; col < m_relation->rd_att->natts; ++col) {
        /* form CU and CUDesc */
        cuDescPtr = New(CurrentMemoryContext)CUDesc();
        cuPtr = FormCU(col, m_tmpBatchRows, cuDescPtr);
        m_cuCmprsOptions[col].m_sampling_finished = true;
        cuDescPtr->cu_id = cuid;
        cuDescPtr->cu_pointer = (uint64)0;
        tmpCUDescs[col] = cuDescPtr;
        tmpCUs[col] = cuPtr;
    }

    rowGroup->Vacuum(m_relation, imcsDesc, tmpCUDescs, tmpCUs, xid);
    imcsDesc->UnReferenceRowGroup();

    MemoryContextSwitchTo(oldcontext);

    pfree(tmpCUDescs);
    pfree(tmpCUs);
}

void IMCStoreInsert::AppendOneTuple(Datum *values, const bool *isnull)
{
    (void)m_tmpBatchRows->append_one_tuple(values, isnull, m_relation->rd_att);
}

void IMCStoreInsert::ResetBatchRows(bool reuseBlocks)
{
    m_tmpBatchRows->reset(reuseBlocks);
}

CU* IMCStoreInsert::FormCU(int col, bulkload_rows* batchRowPtr, CUDesc* cuDescPtr)
{
    FormData_pg_attribute* attrs = m_relation->rd_att->attrs;
    int attlen = attrs[col].attlen;
    CU* cuPtr = New(CurrentMemoryContext) CU(attlen, attrs[col].atttypmod, attrs[col].atttypid);
    /* default in cache */
    cuPtr->m_inCUCache = true;
    cuDescPtr->Reset();

    int funIdx = batchRowPtr->m_vectors[col].m_values_nulls.m_has_null ? FORMCU_IDX_HAVE_NULL : FORMCU_IDX_NONE_NULL;
    (this->*(m_formCUFuncArray[col].colFormCU[funIdx]))(col, batchRowPtr, cuDescPtr, cuPtr);

    cuDescPtr->magic = GetCurrentTransactionIdIfAny();
    if (!cuDescPtr->IsNullCU()) {
        // a little tricky to reduce the recomputation of min/max value.
        // some data type is equal to int8/int16/int32/int32. for them it
        // is not necessary to recompute the min/max value.
        m_cuTempInfo.m_valid_minmax = !NeedToRecomputeMinMax(attrs[col].atttypid);
        if (m_cuTempInfo.m_valid_minmax) {
            m_cuTempInfo.m_min_value = ConvertToInt64Data(cuDescPtr->cu_min, attlen);
            m_cuTempInfo.m_max_value = ConvertToInt64Data(cuDescPtr->cu_max, attlen);
        }
        m_cuTempInfo.m_options = (m_cuCmprsOptions + col);
        cuPtr->m_tmpinfo = &m_cuTempInfo;

        // Magic number is for checking CU data
        cuPtr->SetMagic(cuDescPtr->magic);
        /* no compression by default for htap */
        cuPtr->Compress(batchRowPtr->m_rows_curnum, m_compress_modes, ALIGNOF_CUSIZE);
        cuPtr->UnCompress(batchRowPtr->m_rows_curnum, cuDescPtr->magic, ALIGNOF_CUSIZE);
        cuPtr->FreeCompressBuf();
        cuDescPtr->cu_size = cuPtr->GetCUSize();
    }
    cuDescPtr->row_count = batchRowPtr->m_rows_curnum;

    return cuPtr;
}
