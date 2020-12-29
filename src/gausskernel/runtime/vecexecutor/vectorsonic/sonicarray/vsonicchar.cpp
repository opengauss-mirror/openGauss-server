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
 * vsonicchar.cpp
 *     Routines to handle Character data type like char, bpchar.
 *
 * IDENTIFICATION
 *     src/backend/vectorsonic/sonicarray/vsonicchar.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "c.h"
#include "catalog/pg_type.h"
#include "storage/compress_kits.h"
#include "utils/dynahash.h"
#include "vectorsonic/vsonicchar.h"
#include "vectorsonic/vsonicpartition.h"

/*
 * @Description: constructor.
 * 	Data type like bpchar(10), char(100) should use this
 * 	structure. There are two ways to store data:
 * 	dict and char. Use dict at first. When the distinct values
 * 	are greater than MAX_DIC_ENCODE_ITEMS, change to char.
 * @in cxt - memory context.
 * @in capacity - atom size.
 * @in gen_bit_map - Whether use null flag.
 * @in desc - data description.
 */
SonicCharDatumArray::SonicCharDatumArray(MemoryContext cxt, int capacity, bool gen_bit_map, DatumDesc* desc)
    : SonicDatumArray(cxt, capacity)
{
    AutoContextSwitch mem_guard(m_cxt);

    m_nullFlag = gen_bit_map;
    m_desc = *desc;

    m_dict = (void*)New(CurrentMemoryContext) DicCoder(m_desc.typeSize + VARHDRSZ, MAX_DIC_ENCODE_ITEMS);
    m_dictArrLen = INT_MAX;

    /* Allocate buffer when use char */
    m_resultBuf = NULL;
    m_resultBufIndx = 0;

    /* 256 is the max distinct count for dictionary encode */
    m_atomTypeSize = sizeof(uint8);

    genNewArray(m_nullFlag);
    /*
     * The first position in the first atom doesn't have data.
     * And set the first flag to NULL.
     */
    if (m_nullFlag)
        setNthNullFlag(0, true);

    m_atomIdx = 1;

    initFunc();
}

/*
 * @Description: Two ways to put or get data.
 * 	So initialize related functions here.
 */
void SonicCharDatumArray::initFunc()
{
    m_putFunc = &SonicCharDatumArray::putDicDatumArrayWithNullCheck;
    m_getDatumFunc[0] = &SonicCharDatumArray::getDicDatum;
    m_getDatumFunc[1] = &SonicCharDatumArray::getCharDatum;
    m_getDatumFlagFunc[0] = &SonicCharDatumArray::getDicDatumFlag;
    m_getDatumFlagFunc[1] = &SonicCharDatumArray::getCharDatumFlag;
}

/*
 * @Description: Initialize buffer m_resultBuf.
 * 	The data will be retrieved from m_resultBuf via getCharXXX().
 * 	Allocate space and prepare header info in advance.
 */
void SonicCharDatumArray::initResultBuf()
{
    AutoContextSwitch mem_guard(m_cxt);
    m_resultBuf = (char*)palloc0(m_desc.typeMod * BatchMaxSize);
    m_resultBufIndx = 0;

    char* buf = m_resultBuf;
    /* prepare the header info for the fixed data type. */
    for (int i = 0; i < BatchMaxSize; i++) {
        SET_VARSIZE(buf, m_desc.typeMod);
        buf += m_desc.typeMod;
    }
}

/*
 * @Description: put function.
 * 	m_putFunc decides which data structure the data should put to.
 */
void SonicCharDatumArray::putDatumArrayWithNullCheck(ScalarValue* vals, uint8* flag, int rows)
{
    InvokeFp(m_putFunc)(vals, flag, rows);
}

/*
 * @Description: Get function. Get both vals and flag.
 * 	when @in arr_idx is less than m_dictArrLen, get data from dict.
 * 	Otherwise get data from char.
 * @in arr_idx - array index.
 * @in atom_idx - index in atom.
 * @out val - output value.
 * @out flag - output flag.
 */
void SonicCharDatumArray::getDatumFlag(int arr_idx, int atom_idx, ScalarValue* vals, uint8* flag)
{
    Assert(arr_idx >= 0 && arr_idx < m_arrSize);
    Assert(atom_idx >= 0 && (uint32)atom_idx < m_atomSize);

    int funcid = (arr_idx < m_dictArrLen) ? 0 : 1;
    RuntimeBinding(m_getDatumFlagFunc, funcid)(arr_idx, atom_idx, vals, flag);
}

/*
 * @Description: get function. Get vals.
 * 	when @in arr_idx is less than m_dictArrLen, get data from dict.
 * 	Otherwise get data from char.
 * @in arr_idx - array index.
 * @in atom_idx - index in atom.
 * @return - Datum value.
 */
Datum SonicCharDatumArray::getDatum(int arr_idx, int atom_idx)
{
    Assert(arr_idx >= 0 && arr_idx < m_arrSize);
    Assert(atom_idx >= 0 && (uint32)atom_idx < m_atomSize);

    int funcid = (arr_idx < m_dictArrLen) ? 0 : 1;
    return RuntimeBinding(m_getDatumFunc, funcid)(arr_idx, atom_idx);
}

/*
 * Description: Put data into Dictionary structure DicCoder.
 *	When the number of distinct values is not greater
 *	than MAX_DIC_ENCODE_ITEMS (256), put them into DicCoder,
 *	and put index in DicCoder into atom.
 *	Otherwise put into charDatumArray.
 * @in vals: data to put.
 * @in flag: flag checked when put data.
 * @in rows: the number of data to put.
 */
void SonicCharDatumArray::putDicDatumArrayWithNullCheck(ScalarValue* vals, uint8* flag, int rows)
{
    DicCoder* dict = (DicCoder*)m_dict;
    DicCodeType result = 0;
    Datum* src = vals;
    uint8* dst = (uint8*)(m_curAtom->data + m_atomIdx * sizeof(uint8));
    uint8* tmp_flag = flag;

    for (int i = 0; i < rows; i++) {
        if (NOT_NULL(*tmp_flag)) {
            if (likely(dict->EncodeOneValue(*src, result))) {
                /* Store the index in dict to atom. */
                *dst = result & 0x00ff;
            } else {
                /*
                 * The distinct values is greater than 256,
                 * change the rest from DictXXX to CharXXX.
                 */
                m_atomTypeSize = m_desc.typeSize;
                /* rebuild this array from dict. */
                rebuildFromDict();
                putCharDatumArrayWithNullCheck(vals, flag, rows);
                initResultBuf();

                /* change put function. */
                m_putFunc = &SonicCharDatumArray::putCharDatumArrayWithNullCheck;
                m_dictArrLen = m_arrIdx;
                return;
            }
        }
        dst++;
        src++;
        tmp_flag++;
    }
}

/*
 * @Description: Put values into atom.
 * 	Only store the data part, not header.
 * 	The value length can be either 1 byte or 4 bytes.
 * 	So checking it before storing values.
 * @in vals - data to put.
 * @in flag - flag checked when put data.
 * @in rows - the number of data to put.
 */
void SonicCharDatumArray::putCharDatumArrayWithNullCheck(ScalarValue* vals, uint8* flag, int rows)
{
    errno_t rc;
    char* dst = m_curAtom->data + m_atomIdx * m_atomTypeSize;

    /* copy data to array base on the fixed data length. */
    for (int i = 0; i < rows; i++) {
        if (NOT_NULL(*flag)) {
            if (VARATT_IS_SHORT(*vals))
                rc = memcpy_s(dst, m_atomTypeSize, (char*)(DatumGetPointer(*vals) + VARHDRSZ_SHORT), m_atomTypeSize);
            else
                rc = memcpy_s(dst, m_atomTypeSize, (char*)(DatumGetPointer(*vals) + VARHDRSZ), m_atomTypeSize);
            securec_check(rc, "\0", "\0");
        }
        dst += m_atomTypeSize;
        vals++;
        flag++;
    }
}

/*
 * @Description: Get data from Dict.
 * @in arr_idx - array index.
 * @in atom_idx - index in atom.
 * @out val - output value stored in dict.
 * @out flag - set with getNthNullFlag.
 */
void SonicCharDatumArray::getDicDatumFlag(int arr_idx, int atom_idx, ScalarValue* val, uint8* flag)
{
    Assert(arr_idx >= 0 && arr_idx < m_arrSize);
    Assert(atom_idx >= 0 && (uint32)atom_idx < m_atomSize);

    *flag = getNthNullFlag(arr_idx, atom_idx);
    if (NOT_NULL(*flag)) {
        DicCoder* dict = (DicCoder*)m_dict;
        /* Get item index in dict. */
        DicCodeType item_indx = (DicCodeType)((uint8*)m_arr[arr_idx]->data)[atom_idx];

        /* Get value from dict via item index. */
        dict->DecodeOneValue(item_indx, val);
    }
}

/*
 * @Description: Get data from CharDatumArray.
 * @in arr_idx - array index.
 * @in atom_idx - index in atom.
 * @out val - output value stored in buffer m_resultBuf.
 * @out flag - set with getNthNullFlag.
 */
inline void SonicCharDatumArray::getCharDatumFlag(int arr_idx, int atom_idx, ScalarValue* val, uint8* flag)
{
    Assert(arr_idx >= 0 && arr_idx < m_arrSize);
    Assert(atom_idx >= 0 && (uint32)atom_idx < m_atomSize);

    errno_t rc;
    *flag = getNthNullFlag(arr_idx, atom_idx);
    if (NOT_NULL(*flag)) {
        char* src = m_arr[arr_idx]->data + m_atomTypeSize * atom_idx;
        char* dst = m_resultBuf + (m_atomTypeSize + VARHDRSZ) * (m_resultBufIndx);
        m_resultBufIndx++;
        m_resultBufIndx %= BatchMaxSize;

        /*
         * CharDatumArray only contains data parts.
         * Store data in m_resultBuf which already set header info.
         */
        rc = memcpy_s(dst + VARHDRSZ, m_atomTypeSize, src, m_atomTypeSize);
        securec_check(rc, "\0", "\0");
        *val = PointerGetDatum(dst);
    }
}

/*
 * @Description: Get data from Dict.
 * @in arr_idx - array index.
 * @in atom_idx - index in atom.
 * @return - Datum stored in dict.
 */
inline Datum SonicCharDatumArray::getDicDatum(int arr_idx, int atom_idx)
{
    Assert(arr_idx >= 0 && arr_idx < m_arrSize);
    Assert(atom_idx >= 0 && (uint32)atom_idx < m_atomSize);

    Datum result;
    DicCoder* dict = (DicCoder*)m_dict;
    DicCodeType item_indx = (DicCodeType)((uint8*)m_arr[arr_idx]->data)[atom_idx];

    dict->DecodeOneValue(item_indx, &result);
    return result;
}

/*
 * @Description: flush each data row in the atom into the corresponding partition file.
 * @in arr_idx - array index.
 * @in part_idx - partition indexes
 * @in file_partitions - file partitions to put the data.
 * @in col_idx - column index.
 * @in row_idx - which row to put.
 */
size_t SonicCharDatumArray::flushDatumFlagArrayByRow(
    int arr_idx, uint32* part_idx, void** file_partitions, uint32 col_idx, int row_idx)
{
    size_t written = 0;
    Datum datum;
    uint32 header_len = 0;
    SonicHashFileSource* file = NULL;
    SonicHashFilePartition* file_partition = NULL;
    SonicHashPartition** partitions = (SonicHashPartition**)file_partitions;

    file_partition = (SonicHashFilePartition*)partitions[*part_idx];
    int file_idx = file_partition->m_filetype[col_idx];
    file = file_partition->m_files[file_idx];

    if (arr_idx < m_dictArrLen) {
        written = (file->write)((void*)m_curFlag, sizeof(uint8));
        if (NOT_NULL(*m_curFlag)) {
            datum = getDicDatum(arr_idx, row_idx);
            header_len = VARATT_IS_SHORT(datum) ? VARHDRSZ_SHORT : VARHDRSZ;
            written += (file->write)((void*)(DatumGetPointer(datum) + header_len), m_desc.typeSize);
        }
    } else {
        written = (file->write)((void*)m_curFlag, sizeof(uint8));
        if (NOT_NULL(*m_curFlag)) {
            written += (file->write)((void*)m_curData, m_desc.typeSize);
        }
    }

    /* when enable_sonic_optspill, m_fileRows is qual to rows*m_cols */
    file_partition->m_fileRecords[file_idx] += 1;
    file_partition->m_size += written;
    m_curData += m_atomTypeSize;
    m_curFlag++;

    return written;
}

/*
 * @Description: flush nrows in the atom per row into the files of
 * 	the partition each row belongs to.
 * @in arr_idx - array index.
 * @in part_idx - partition indexes
 * @in file_partitions - file partitions to put the data.
 * @in col_idx - column index.
 * @in nrows - rows to put.
 */
size_t SonicCharDatumArray::flushDatumFlagArray(
    int arr_idx, uint32* part_idx, void** file_partitions, uint32 col_idx, int nrows)
{
    Assert((uint32)nrows <= m_atomSize);
    size_t total_written = 0;
    size_t written = 0;
    Datum datum;
    uint32 header_len = 0;
    char* cur_data = m_arr[arr_idx]->data;
    uint8* flag = (uint8*)(m_arr[arr_idx]->nullFlag);
    SonicHashFileSource* file = NULL;
    SonicHashFilePartition* file_partition = NULL;
    SonicHashPartition** partitions = (SonicHashPartition**)file_partitions;
    int atom_idx = 0;
    /* The first position in the first atom doesn't have data. */
    if (unlikely(arr_idx == 0)) {
        cur_data += m_atomTypeSize;
        ++flag;
        ++atom_idx;
        ++part_idx;
    }

    if (arr_idx < m_dictArrLen) {
        for (; atom_idx < nrows; ++atom_idx, ++part_idx) {
            file_partition = (SonicHashFilePartition*)partitions[*part_idx];
            file = file_partition->m_files[col_idx];

            written = (file->write)((void*)flag, sizeof(uint8));
            if (NOT_NULL(*flag)) {
                datum = getDicDatum(arr_idx, atom_idx);
                header_len = VARATT_IS_SHORT(datum) ? VARHDRSZ_SHORT : VARHDRSZ;
                written += (file->write)((void*)(DatumGetPointer(datum) + header_len), m_desc.typeSize);
            }

            file_partition->m_fileRecords[col_idx] += 1;
            file_partition->m_size += written;
            total_written += written;
            cur_data += m_atomTypeSize;
            ++flag;
        }
    } else {
        for (; atom_idx < nrows; ++atom_idx, ++part_idx) {
            file_partition = (SonicHashFilePartition*)partitions[*part_idx];
            file = file_partition->m_files[col_idx];

            written = (file->write)((void*)flag, sizeof(uint8));
            if (NOT_NULL(*flag)) {
                written += (file->write)((void*)cur_data, m_desc.typeSize);
            }

            file_partition->m_fileRecords[col_idx] += 1;
            file_partition->m_size += written;
            total_written += written;
            cur_data += m_atomTypeSize;
            ++flag;
        }
    }
    return total_written;
}

/*
 * @Description: load data from specified file to Datum Array.
 * @in file_source - file to load from.
 * @in nrows - rows to load.
 */
void SonicCharDatumArray::loadDatumFlagArray(void* file_source, int rows)
{
    /*
     * When load from disk, don't use Dict anymore.
     * Maybe optimize it in the future.
     */
    if (unlikely(m_arrIdx == 0)) {
        AutoContextSwitch mem_guard(m_cxt);
        m_atomTypeSize = m_desc.typeSize;
        m_curAtom->data = (char*)repalloc(m_curAtom->data, m_atomTypeSize * m_atomSize);
        initResultBuf();
        /* change put function. */
        m_putFunc = &SonicCharDatumArray::putCharDatumArrayWithNullCheck;
        m_dictArrLen = m_arrIdx;
    }

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
 * @in left_rows -left rows to load, not used.
 */
void SonicCharDatumArray::loadDatumFlagArrayByRow(void* file_source, int left_rows)
{
    /*
     * When load from disk, don't use Dict anymore.
     * Maybe optimize it in the future.
     */
    if (unlikely(m_arrIdx == 0 && m_atomIdx == 1)) {
        AutoContextSwitch mem_guard(m_cxt);
        m_atomTypeSize = m_desc.typeSize;
        m_curAtom->data = (char*)repalloc(m_curAtom->data, m_atomTypeSize * m_atomSize);
        initResultBuf();
        /* change put function. */
        m_putFunc = &SonicCharDatumArray::putCharDatumArrayWithNullCheck;
        m_dictArrLen = m_arrIdx;
        /* init m_curData */
        m_curData = m_curAtom->data + m_atomIdx * m_atomTypeSize;
    }

    size_t nread = 0;
    SonicHashFileSource* file = (SonicHashFileSource*)file_source;

    nread = (file->read)((void*)m_curFlag, sizeof(uint8));
    CheckReadIsValid(nread, sizeof(uint8));

    if (NOT_NULL(*m_curFlag)) {
        nread = (file->read)((void*)m_curData, m_atomTypeSize);
        CheckReadIsValid(nread, (size_t)m_atomTypeSize);
    }
    m_curFlag++;
    m_curData += m_atomTypeSize;
}

/*
 * @Description: Get data from CharDatumArray.
 * @in arr_idx - array index.
 * @in atom_idx - index in atom.
 * @return - pointer points to the value in buffer.
 */
inline Datum SonicCharDatumArray::getCharDatum(int arr_idx, int atom_idx)
{
    Assert(arr_idx >= 0 && arr_idx < m_arrSize);
    Assert(atom_idx >= 0 && (uint32)atom_idx < m_atomSize);

    char* src = m_arr[arr_idx]->data + m_atomTypeSize * atom_idx;
    char* dst = m_resultBuf + (m_atomTypeSize + VARHDRSZ) * (m_resultBufIndx);
    errno_t rc;
    m_resultBufIndx++;

    rc = memcpy_s(dst + VARHDRSZ, m_atomTypeSize, src, m_atomTypeSize);
    securec_check(rc, "\0", "\0");
    return PointerGetDatum(dst);
}

/*
 * @Description: Move data from Dict to CharDatumArray.
 * 	When distinct values is greater than 256,
 * 	call this functions. Only rebuild current atom.
 * 	The atoms before current atom should still use Dict.
 */
void SonicCharDatumArray::rebuildFromDict()
{
    AutoContextSwitch mem_guard(m_cxt);
    char* new_vals = (char*)palloc0(m_atomTypeSize * m_atomSize);
    char* dst = new_vals;
    Datum data;
    errno_t rc;
    /* The first position in the first atom doesn't have data. */
    int startIdx = (m_arrIdx == 0) ? 1 : 0;
    if (startIdx == 1)
        dst += m_atomTypeSize;

    for (int i = startIdx; i < m_atomIdx; i++) {
        if (NOT_NULL(getNthNullFlag(m_arrIdx, i))) {
            data = getDicDatum(m_arrIdx, i);
            /* same as putCharDatumArrayWithNullCheck() */
            if (VARATT_IS_SHORT(data))
                rc = memcpy_s(dst, m_atomTypeSize, (char*)(DatumGetPointer(data) + VARHDRSZ_SHORT), m_atomTypeSize);
            else
                rc = memcpy_s(dst, m_atomTypeSize, (char*)(DatumGetPointer(data) + VARHDRSZ), m_atomTypeSize);
            securec_check(rc, "\0", "\0");
        }
        dst += m_atomTypeSize;
    }

    pfree(m_curAtom->data);
    m_curAtom->data = new_vals;
}

/*
 * @Description: Get function. Get both vals and flags.
 * @in nrows - the number of data to get.
 * @in arr_idx - array index. The size should be nrows.
 * @in atom_idx - index in atom. The size should be nrows.
 * @out data -  output values.
 * @out flag - output flags.
 * The space of data and flag should be allocated by caller.
 */
void SonicCharDatumArray::getDatumFlagArray(int nrows, ArrayIdx* array_idx, Datum* data, uint8* flag)
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
void SonicCharDatumArray::getDatumFlagArrayWithMatch(
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
 * @Description: Change from Dict to char without
 * putting data until rebuildFromDict.
 * This function is used for ut test currently.
 */
void SonicCharDatumArray::initCharDatumArray()
{
    m_atomTypeSize = m_desc.typeSize;
    initResultBuf();
    /* change put function. */
    m_putFunc = &SonicCharDatumArray::putCharDatumArrayWithNullCheck;
    m_dictArrLen = m_arrIdx;
}
