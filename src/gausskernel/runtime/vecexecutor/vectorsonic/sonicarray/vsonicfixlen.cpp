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
 * vsonicfixlen.cpp
 *     Routines to handle data type that attribute has fixed length.
 *     Currently support length of 12 and 16.
 *
 * IDENTIFICATION
 *     src/backend/vectorsonic/sonicarray/vsonicfixlen.cpp
 *
 * ------------------------------------------------------------------------------
 */
#include "c.h"
#include "catalog/pg_type.h"
#include "storage/compress_kits.h"
#include "utils/dynahash.h"
#include "vectorsonic/vsonicfixlen.h"
#include "vectorsonic/vsonicpartition.h"

/*
 * @Description: constructor.
 * 	Data attr length equals to 12 or 16, use this structure.
 * @in cxt - memory context.
 * @in capacity - atom size.
 * @in gen_bit_map - Whether use null flag.
 * @in desc - data description.
 */
SonicFixLenDatumArray::SonicFixLenDatumArray(MemoryContext cxt, int capacity, bool gen_bit_map, DatumDesc* desc)
    : SonicDatumArray(cxt, capacity)
{
    Assert(desc);
    AutoContextSwitch mem_guard(m_cxt);

    m_nullFlag = gen_bit_map;
    m_desc = *desc;
    m_atomTypeSize = m_desc.typeSize;

    m_resultBuf = NULL;
    m_resultBufIndx = 0;

    genNewArray(m_nullFlag);

    /*
     * The first position in the first atom doesn't have data.
     * And set the first flag to NULL.
     */
    if (m_nullFlag)
        setNthNullFlag(0, true);

    m_atomIdx = 1;
    /*
     * Only support attlen = 12 or 16
     * Both header length is 1 byte.
     */
    m_varheadlen = VARHDRSZ_SHORT;

    initResultBuf();
}

/*
 * @Description: Initialize buffer m_resultBuf.
 * 	The data will be retrieved from m_resultBuf.
 * 	Allocate space and prepare header info in advance.
 */
void SonicFixLenDatumArray::initResultBuf()
{
    AutoContextSwitch mem_guard(m_cxt);
    m_resultBuf = (char*)palloc0((m_atomTypeSize + m_varheadlen) * BatchMaxSize);
    m_resultBufIndx = 0;

    char* buf = m_resultBuf;
    /* prepare the header info for the fixed data type. */
    for (int i = 0; i < BatchMaxSize; i++) {
        SET_VARSIZE_SHORT(buf, m_atomTypeSize + m_varheadlen);
        buf += m_atomTypeSize + m_varheadlen;
    }
}

/*
 * @Description: put function.
 * 	Put values into atom from pointer.
 * 	Only store the data part, not header.
 * @in vals - data to put.
 * @in flag - flag checked when put data.
 * @in rows - the number of data to put.
 */
void SonicFixLenDatumArray::putDatumArrayWithNullCheck(ScalarValue* vals, uint8* flag, int rows)
{
    char* dst = m_curAtom->data + m_atomIdx * m_atomTypeSize;
    errno_t rc;
    uint32 pos = m_atomTypeSize * (m_atomSize - m_atomIdx);

    /* copy data to array base on the fixed data length. */
    for (int i = 0; i < rows; i++) {
        if (NOT_NULL(*flag)) {
            rc = memcpy_s(dst, pos, (char*)(DatumGetPointer(*vals) + m_varheadlen), m_atomTypeSize);
            securec_check(rc, "\0", "\0");
        }
        dst += m_atomTypeSize;
        pos -= m_atomTypeSize;
        vals++;
        flag++;
    }
}

/*
 * @Description: Restore val and flag via m_resultBuf.
 * @in arr_idx - array index.
 * @in atom_idx - index in atom.
 * @out val - output value stored in buffer m_resultBuf.
 * @out flag - set with getNthNullFlag.
 */
void SonicFixLenDatumArray::getDatumFlag(int arr_idx, int atom_idx, ScalarValue* val, uint8* flag)
{
    Assert(arr_idx >= 0 && arr_idx < m_arrSize);
    Assert(atom_idx >= 0 && (uint32)atom_idx < m_atomSize);

    errno_t rc;
    *flag = getNthNullFlag(arr_idx, atom_idx);
    if (NOT_NULL(*flag)) {
        char* src = m_arr[arr_idx]->data + m_atomTypeSize * atom_idx;
        char* dst = m_resultBuf + (m_atomTypeSize + m_varheadlen) * (m_resultBufIndx);
        m_resultBufIndx++;
        m_resultBufIndx %= BatchMaxSize;

        rc = memcpy_s(dst + m_varheadlen, m_atomTypeSize, src, m_atomTypeSize);
        securec_check(rc, "\0", "\0");
        *val = PointerGetDatum(dst);
    }
}

/*
 * @Description: Restore val via m_resultBuf.
 * 	Usually need to check flag at first.
 * 	So call getDatumFlag if you are not
 * 	sure whether is value is valid.
 * @in arr_idx - array index.
 * @in atom_idx - index in atom.
 * @return - the pointer points to the val.
 */
Datum SonicFixLenDatumArray::getDatum(int arr_idx, int atom_idx)
{
    Assert(arr_idx >= 0 && arr_idx < m_arrSize);
    Assert(atom_idx >= 0 && (uint32)atom_idx < m_atomSize);

    char* src = m_arr[arr_idx]->data + m_atomTypeSize * atom_idx;
    char* dst = m_resultBuf + (m_atomTypeSize + m_varheadlen) * (m_resultBufIndx);
    errno_t rc;

    m_resultBufIndx++;

    rc = memcpy_s(dst + m_varheadlen, m_atomTypeSize, src, m_atomTypeSize);
    securec_check(rc, "\0", "\0");
    return PointerGetDatum(dst);
}

/*
 * @escription: Get function. Get data and flag from atoms.
 * @in nrows - the number of data to get.
 * @in arr_idx - array index. The size should be nrows.
 * @in atom_idx - index in atom. The size should be nrows.
 * @out data - output values.
 * @out flag - output flags.
 * The space of data and flag should be allocated by caller.
 */
void SonicFixLenDatumArray::getDatumFlagArray(int nrows, ArrayIdx* array_idx, Datum* data, uint8* flag)
{
    Assert(nrows <= BatchMaxSize);
    int i;

    for (i = 0; i < nrows; i++) {
        getDatumFlag(array_idx->arrIdx, array_idx->atomIdx, (ScalarValue*)data, flag);
        array_idx++;
        data++;
        flag++;
    }
}

/*
 * @Description: Get function when does hash key matching. Get data and flag from atoms.
 * @in nrows - the number of data to get.
 * @in arr_idx - array index. The size should be nrows.
 * @in atom_idx - index in atom. The size should be nrows.
 * @out data - output values.
 * @out flag - output flags.
 * @in match_idx - matched array. If false, means current tuple pair doesn't match,
 * 	so no need to get datum and flag any more, skip it.
 * The space of data and flag should be allocated by caller.
 */
void SonicFixLenDatumArray::getDatumFlagArrayWithMatch(
    int nrows, ArrayIdx* array_idx, Datum* data, uint8* flag, const bool* match_idx)
{
    Assert(nrows <= BatchMaxSize);
    int i;

    for (i = 0; i < nrows; i++) {
        if (*match_idx)
            getDatumFlag(array_idx->arrIdx, array_idx->atomIdx, (ScalarValue*)data, flag);
        match_idx++;
        array_idx++;
        data++;
        flag++;
    }
}

/*
 * @Description: flush each data row in the atom into the corresponding partition file.
 * @in arr_idx - array index.
 * @in part_idx - partition indexes
 * @in file_partitions - file partitions to put the data.
 * @in col_idx - column index.
 * @in row_idx  - which row to put.
 */
size_t SonicFixLenDatumArray::flushDatumFlagArrayByRow(
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

    written += (file->write)((void*)m_curData, m_atomTypeSize);
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
size_t SonicFixLenDatumArray::flushDatumFlagArray(
    int arr_idx, uint32* part_idx, void** file_partitions, uint32 col_idx, int nrows)
{
    Assert((uint32)nrows <= m_atomSize);
    size_t total_written = 0;
    size_t written = 0;
    char* cur_data = m_arr[arr_idx]->data;
    SonicHashFileSource* file = NULL;
    SonicHashFilePartition* file_partition = NULL;
    SonicHashPartition** partitions = (SonicHashPartition**)file_partitions;
    uint8* flag = (uint8*)m_arr[arr_idx]->nullFlag;
    int atom_idx = 0;
    /* The first position in the first atom doesn't have data. */
    if (unlikely(arr_idx == 0)) {
        cur_data += m_atomTypeSize;
        ++flag;
        ++atom_idx;
        ++part_idx;
    }

    for (; atom_idx < nrows; ++atom_idx, ++part_idx) {
        file_partition = (SonicHashFilePartition*)partitions[*part_idx];
        file = file_partition->m_files[col_idx];

        written = (file->write)((void*)flag, sizeof(uint8));
        if (NOT_NULL(*flag)) {
            written += (file->write)((void*)cur_data, m_atomTypeSize);
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
 * Description: load data from specified file to Datum Array.
 * @in file_source - file to load from.
 * @in nrows - rows to load.
 */
void SonicFixLenDatumArray::loadDatumFlagArray(void* file_source, int rows)
{
    size_t nread = 0;
    SonicHashFileSource* file = (SonicHashFileSource*)file_source;
    char* dst = m_curAtom->data + m_atomIdx * m_atomTypeSize;
    uint8* flag_array = (uint8*)m_curAtom->nullFlag + m_atomIdx * sizeof(uint8);

    for (int i = 0; i < rows; ++i) {
        nread = (file->read)((void*)flag_array, sizeof(uint8));
        CheckReadIsValid(nread, sizeof(uint8));

        if (NOT_NULL(*flag_array)) {
            nread = (file->read)((void*)dst, m_atomTypeSize);
            CheckReadIsValid(nread, (size_t)m_atomTypeSize);
        }
        flag_array++;
        dst += m_atomTypeSize;
    }
}

/*
 * Description: load data with specific row_idx from specified file to Datum Array .
 * @in file_source - file to load from.
 * @in leftrows - left rows to load, not used.
 */
void SonicFixLenDatumArray::loadDatumFlagArrayByRow(void* file_source, int leftrows)
{
    size_t nread = 0;
    SonicHashFileSource* file = (SonicHashFileSource*)file_source;

    nread = (file->read)((void*)m_curFlag, sizeof(uint8));
    CheckReadIsValid(nread, sizeof(uint8));

    nread = (file->read)((void*)m_curData, m_atomTypeSize);
    CheckReadIsValid(nread, (size_t)m_atomTypeSize);

    m_curFlag++;
    m_curData += m_atomTypeSize;
}
