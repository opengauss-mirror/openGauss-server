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
 * vecstore.cpp
 *
 * IDENTIFICATION
 *        Code/src/gausskernel/runtime/vecexecutor/vecnode/vecstore.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "vecexecutor/vectorbatch.h"
#include "vecexecutor/vecstore.h"

void MultiColumnsData::Init(int capacity)
{
    m_capacity = capacity;
    m_memRowNum = 0;
    m_memValues = (MultiColumns*)palloc0(capacity * sizeof(MultiColumns));
}

void VecStore::InitColInfo(VectorBatch* batch)
{
    m_colInfo = (ScalarVector*)palloc(batch->m_cols * sizeof(ScalarVector));

    for (int i = 0; i < batch->m_cols; ++i) {
        m_colInfo[i] = batch->m_arr[i];
    }
}

void VecStore::FreeMultiColumn(MultiColumns* pMultiColumn)
{
    FreeMem(GetMemoryChunkSpace(pMultiColumn->m_values));
    FreeMem(GetMemoryChunkSpace(pMultiColumn->m_nulls));

    for (int col = 0; col < m_colNum; ++col) {
        if (NeedDecode(col) && !IS_NULL(pMultiColumn->m_nulls[col])) {
            char* ptr = DatumGetPointer(pMultiColumn->m_values[col]);
            FreeMem(GetMemoryChunkSpace(ptr));
            pfree_ext(ptr);
        }
    }

    pfree_ext(pMultiColumn->m_values);
    pfree_ext(pMultiColumn->m_nulls);
}

/*
 * Note that the caller must free the result pointer
 */
char* VecStore::GetData(int colNum, MultiColumns* multiColumn)
{
    char* ptr = (char*)palloc(multiColumn->size);
    char* tmp_ptr = ptr;

    for (int i = 0; i < colNum; ++i) {
        // Fill NULL flag
        *(uint8*)tmp_ptr = multiColumn->m_nulls[i];
        tmp_ptr += sizeof(uint8);
        // Fill value if not null
        if (!IS_NULL(multiColumn->m_nulls[i])) {
            // variable attype
            if (NeedDecode(i)) {
                // It is datum string in muticolumns.
                Datum v = multiColumn->m_values[i];
                char* p = DatumGetPointer(v);
                errno_t rc;
                // Free space left in buffer
                int len = multiColumn->size - (tmp_ptr - ptr);
                rc = memcpy_s(tmp_ptr, len, p, VARSIZE_ANY(p));
                securec_check(rc, "\0", "\0");
                tmp_ptr += VARSIZE_ANY(p);
                // Fixed-length attype
            } else {
                *(ScalarValue*)tmp_ptr = multiColumn->m_values[i];
                tmp_ptr += sizeof(ScalarValue);
            }
        }
    }

    Assert(tmp_ptr == ptr + multiColumn->size);
    UseMem(GetMemoryChunkSpace(ptr));
    return ptr;
}

void VecStore::SetData(char* dataPtr, int dataSize, MultiColumns* multiColumn)
{
    multiColumn->m_values = (Datum*)palloc(m_colNum * sizeof(Datum));
    UseMem(GetMemoryChunkSpace(multiColumn->m_values));
    multiColumn->m_nulls = (uint8*)palloc(m_colNum * sizeof(uint8));
    UseMem(GetMemoryChunkSpace(multiColumn->m_nulls));
#ifdef USE_ASSERT_CHECKING
    char* head_ptr = dataPtr;
#endif
    multiColumn->size = dataSize;

    for (int i = 0; i < m_colNum; ++i) {
        Form_pg_attribute attr = &tupDesc->attrs[i];
        Datum v;

        int val_size = 0;

        // Hack deal with NULL value.
        //
        // Fill NULL flag
        multiColumn->m_nulls[i] = *(uint8*)dataPtr;
        dataPtr += sizeof(uint8);

        // Fill value if not null
        if (!IS_NULL(multiColumn->m_nulls[i])) {
            if (NeedDecode(i)) {
                v = PointerGetDatum(dataPtr);
                multiColumn->m_values[i] = datumCopy(v, attr->attbyval, VARSIZE_ANY(v));

                UseMem(GetMemoryChunkSpace(DatumGetPointer(multiColumn->m_values[i])));
                val_size = VARSIZE_ANY(dataPtr);
            } else {
                multiColumn->m_values[i] = *(ScalarValue*)dataPtr;
                val_size = sizeof(ScalarValue);
            }
            dataPtr += val_size;
        }
    }
    Assert(multiColumn->size == dataPtr - head_ptr);
}
void VecStore::AppendBatch(VectorBatch* batch, MultiColumns& multiColumn, int curRow, bool isfree)
{
    Datum* pValues = multiColumn.m_values;
    uint8* flag_arr = multiColumn.m_nulls;
    uint8 flag;

    for (int i = 0; i < batch->m_cols; ++i) {
        ScalarVector* vec = &batch->m_arr[i];
        flag = flag_arr[i];
        vec->m_flag[curRow] = flag;

        if (!IS_NULL(flag)) {
            if (NeedDecode(i)) {
                Assert(vec->m_desc.encoded);
                vec->m_vals[curRow] = vec->AddVarWithHeader(pValues[i]);
            } else {
                vec->m_vals[curRow] = pValues[i];
            }
        }
        vec->m_rows++;
    }
    batch->m_rows++;
    if (isfree)
        FreeMultiColumn(&multiColumn);
}
