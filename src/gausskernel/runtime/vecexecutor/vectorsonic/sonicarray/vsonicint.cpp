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
 * vsonicint.cpp
 *     Routines to handle data type whose attribute length is
 *     1, 2, 4, 8.
 *     The length of type can be retrieved from query below:
 *     select oid, typname from pg_type where typlen = len;
 *     There is one exception:
 *     TIDOID's length is 6, and use atomTypeSize 8.
 *
 * IDENTIFICATION
 *     src/backend/vectorsonic/sonicarray/vsonicint.cpp
 *
 * ---------------------------------------------------------------------
 */
#include "c.h"
#include "catalog/pg_type.h"
#include "storage/compress_kits.h"
#include "vectorsonic/vsonicint.h"
#include "utils/dynahash.h"
#include "vectorsonic/vsonicpartition.h"

/*
 * @Description: create IntTemplateDatumArray according to data size.
 * @in create_context: Created IntTemplateDatumArray under it.
 * @in internal_context: memory context used inside IntTemplateDatumArray.
 * @in capacity: atom size.
 * @in gen_null_flag: Whether use null flag.
 * @in desc: data description.
 */
SonicDatumArray* AllocateIntArray(
    MemoryContext create_context, MemoryContext internal_context, int capacity, bool gen_null_flag, DatumDesc* desc)
{
    SonicDatumArray* arr = NULL;
    switch (desc->typeSize) {
        case 1:
            arr = New(create_context) SonicIntTemplateDatumArray<uint8>(internal_context, capacity, gen_null_flag, desc);
            break;

        case 2:
            arr = New(create_context) SonicIntTemplateDatumArray<uint16>(internal_context, capacity, gen_null_flag, desc);
            break;

        case 4:
            arr = New(create_context) SonicIntTemplateDatumArray<uint32>(internal_context, capacity, gen_null_flag, desc);
            break;

        case 8:
            arr = New(create_context) SonicIntTemplateDatumArray<uint64>(internal_context, capacity, gen_null_flag, desc);
            break;

        default:
            Assert(false);
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid datum int array size %d", desc->typeSize)));
            break;
    }

    return arr;
}

/*
 * @Description: flush each data row in the atom into the corresponding partition file.
 * @in arr_idx - array index.
 * @in part_idx - partition indexes
 * @in file_partitions - file partitions to put the data.
 * @in col_idx - column index.
 * @in row_idx - which row to put.
 */
template <typename T>
size_t SonicIntTemplateDatumArray<T>::flushDatumFlagArrayByRow(
    int arr_idx, uint32* part_idx, void** file_partitions, uint32 col_idx, int row_idx)
{
    size_t written = 0;
    SonicHashFileSource* file = NULL;
    SonicHashFilePartition* file_partition = NULL;
    SonicHashPartition** partitions = (SonicHashPartition**)file_partitions;

    file_partition = (SonicHashFilePartition*)partitions[*part_idx];
    int file_idx = file_partition->m_filetype[col_idx];
    file = file_partition->m_files[file_idx];
    written = (file->write)((void*)m_curFlag, sizeof(uint8));

    written += (file->write)((void*)m_curData, sizeof(T));
    file_partition->m_fileRecords[file_idx] += 1;

    file_partition->m_size += written;
    m_curData += m_atomTypeSize;
    m_curFlag++;

    return written;
}

/*
 * @Description: flush nrows in the atom per row into the files of the partition,
 * 	each row belongs to.
 * @in arr_idx - array index.
 * @in part_idx - partition indexes
 * @in file_partitions - file partitions to put the data.
 * @in col_idx - column index.
 * @in nrows - rows to put.
 */
template <typename T>
size_t SonicIntTemplateDatumArray<T>::flushDatumFlagArray(
    int arr_idx, uint32* part_idx, void** file_partitions, uint32 col_idx, int nrows)
{
    Assert((uint32)nrows <= m_atomSize);
    size_t total_written = 0;
    size_t written = 0;
    char* cur_data = m_arr[arr_idx]->data;
    SonicHashFileSource* file = NULL;
    SonicHashFilePartition* file_partition = NULL;
    SonicHashPartition** partitions = (SonicHashPartition**)file_partitions;
    uint8* flag = (uint8*)(m_arr[arr_idx]->nullFlag);
    int atomIdx = 0;
    /* The first position in the first atom doesn't have data. */
    if (unlikely(arr_idx == 0)) {
        cur_data += m_atomTypeSize;
        ++flag;
        ++atomIdx;
        ++part_idx;
    }

    for (; atomIdx < nrows; ++atomIdx, ++part_idx) {
        file_partition = (SonicHashFilePartition*)partitions[*part_idx];
        file = file_partition->m_files[col_idx];

        written = (file->write)((void*)flag, sizeof(uint8));
        if (NOT_NULL(*flag)) {
            written += (file->write)((void*)cur_data, sizeof(T));
        }
        file_partition->m_fileRecords[col_idx] += 1;

        file_partition->m_size += written;
        total_written += written;
        cur_data += m_atomTypeSize;
        ++flag;
    }

    return total_written;
}

/*
 * @Description: load data from specified file to Datum Array.
 * @in file_source - file to load from.
 * @in nrows - rows to load.
 */
template <typename T>
void SonicIntTemplateDatumArray<T>::loadDatumFlagArray(void* file_source, int rows)
{
    size_t nread = 0;
    SonicHashFileSource* file = (SonicHashFileSource*)file_source;
    T* dst = (T*)(m_curAtom->data + m_atomTypeSize * m_atomIdx);
    uint8* flag_array = (uint8*)m_curAtom->nullFlag + m_atomIdx * sizeof(uint8);

    for (int i = 0; i < rows; ++i) {
        nread = (file->read)((void*)flag_array, sizeof(uint8));
        CheckReadIsValid(nread, sizeof(uint8));

        if (NOT_NULL(*flag_array)) {
            nread = (file->read)((void*)dst, m_atomTypeSize);
            CheckReadIsValid(nread, (size_t)m_atomTypeSize);
        }
        flag_array++;
        dst++;
    }
}

/*
 * Description: load data with specific row_idx from specified file to Datum Array .
 * @in file_source - file to load from.
 * @in leftrows - left rows to load, not used.
 */
template <typename T>
void SonicIntTemplateDatumArray<T>::loadDatumFlagArrayByRow(void* file_source, int leftrows)
{
    size_t nread = 0;
    SonicHashFileSource* file = (SonicHashFileSource*)file_source;
    T* dst = (T*)m_curData;

    nread = (file->read)((void*)m_curFlag, sizeof(uint8));
    CheckReadIsValid(nread, sizeof(uint8));

    nread = (file->read)((void*)dst, m_atomTypeSize);
    CheckReadIsValid(nread, (size_t)m_atomTypeSize);
    m_curFlag++;
    m_curData += m_atomTypeSize;
}

/*
 * @Description: load integer from specified file to Datum Array.
 * 	If no enough space, allocate a new DatumArray.
 * @in file - file to load from.
 * @in nrows - rows to load.
 */
template <typename T>
void SonicIntTemplateDatumArray<T>::loadIntValArray(void* file, int nrows)
{
    int loop_rows;

    bool need_expand = (uint32)nrows > (m_atomSize - m_atomIdx);
    loop_rows = need_expand ? (m_atomSize - m_atomIdx) : nrows;

    if (loop_rows > 0) {
        loadIntVal(file, loop_rows);
        m_atomIdx += loop_rows;
    }

    if (need_expand) {
        genNewArray(m_nullFlag);
        loadIntValArray(file, nrows - loop_rows);
    }
}

/*
 * @Description: load integer from specified file to Datum Array.
 * 	Ensure space is enough to load nrows before calling this function.
 * @in file - file to load from.
 * @in nrows - rows to load.
 */
template <typename T>
void SonicIntTemplateDatumArray<T>::loadIntVal(void* file_source, int rows)
{
    size_t nread = 0;
    SonicHashFileSource* file = (SonicHashFileSource*)file_source;
    T* dst = (T*)(m_curAtom->data + m_atomTypeSize * m_atomIdx);

    for (int i = 0; i < rows; ++i) {
        nread = (file->read)((void*)dst, m_atomTypeSize);
        CheckReadIsValid(nread, (size_t)m_atomTypeSize);
        dst++;
    }
}
