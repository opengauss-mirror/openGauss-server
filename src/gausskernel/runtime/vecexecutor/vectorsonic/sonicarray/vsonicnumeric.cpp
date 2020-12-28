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
 * vsonicnumeric.cpp
 *     Routines to handle Numeric data type.
 *
 * IDENTIFICATION
 *     src/backend/vectorsonic/sonicarray/vsonicnumeric.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "c.h"
#include "catalog/pg_type.h"
#include "storage/compress_kits.h"
#include "vectorsonic/vsonicnumeric.h"
#include "utils/dynahash.h"
#include "vectorsonic/vsonicpartition.h"

/*
 * @Description: constructor.
 *      If data type is numeric, use this structure.
 * @in cxt - memory context.
 * @in capacity - atom size.
 * @in gen_bit_map - Whether use null flag.
 * @in desc - data description.
 */
SonicNumericDatumArray::SonicNumericDatumArray(MemoryContext cxt, int capacity, bool gen_bit_map, DatumDesc* desc)
    : SonicDatumArray(cxt, capacity)
{
    Assert(desc);
    AutoContextSwitch mem_guard(m_cxt);

    m_nullFlag = gen_bit_map;
    m_desc = *desc;
    m_atomTypeSize = 0;

    m_buf = New(m_cxt) VarBuf(m_cxt);

    m_offArrSize = m_arrSize;
    m_offsetArr = (uint32**)palloc0(sizeof(uint32*) * m_offArrSize);
    m_curOffset = NULL;

    m_tmpBuf = (char*)palloc0(NUMERIC_64SZ * BatchMaxSize);
    m_tmpBufIdx = 0;

    m_atomBuf = (char*)palloc0(10);

    m_internalLen = m_atomSize;
    m_curLen = 0;

    genNewArray(gen_bit_map);
    /*
     * The first position in the first atom doesn't have data.
     * And set the first flag to NULL.
     */
    if (m_nullFlag)
        setNthNullFlag(0, true);

    m_atomIdx = 1;
}

/*
 * @Description: generate auxiliary structures.
 */
void SonicNumericDatumArray::genAdditonArr()
{
    AutoContextSwitch mem_guard(m_cxt);
    if (unlikely((m_arrIdx) >= m_offArrSize)) {
        m_offArrSize = m_offArrSize * 2;
        m_offsetArr = (uint32**)repalloc(m_offsetArr, sizeof(uint32*) * m_offArrSize);
    }

    /* need to record size of each data via offset. */
    m_curOffset = (uint32*)palloc0(sizeof(uint32) * (m_atomSize + 1));
    m_curOffset[0] = 0;

    m_internalLen = m_atomSize;
    m_curLen = 0;
    m_offsetArr[m_arrIdx] = m_curOffset;
}

/*
 * @Description: Generate new atom if there is no space.
 * 	Generate m_curOffset also.
 */
void SonicNumericDatumArray::genNewArray(bool gen_bit_map)
{
    AutoContextSwitch mem_guard(m_cxt);

    if (unlikely((++m_arrIdx) >= m_arrSize)) {
        m_arrSize = m_arrSize * 2;
        m_arr = (atom**)repalloc(m_arr, sizeof(atom*) * m_arrSize);
    }

    atom* at = (atom*)palloc(sizeof(atom));
    at->data = (char*)palloc0(m_atomSize);
    if (gen_bit_map)
        at->nullFlag = (char*)palloc0(sizeof(bool) * m_atomSize);
    else
        at->nullFlag = NULL;

    genAdditonArr();

    m_curAtom = at;
    m_arr[m_arrIdx] = at;
    m_atomIdx = 0;

    /* init flag and curdata in current numeric atom */
    m_curFlag = (uint8*)m_curAtom->nullFlag;
}

/*
 * @Description: Put values into atom.
 * @in vals - data to put.
 * @in flag - flag checked when put data.
 * @in rows - the number of data to put.
 */
void SonicNumericDatumArray::putDatumArrayWithNullCheck(ScalarValue* vals, uint8* flag, int rows)
{
    char* dst = NULL;
    Datum* ddst = NULL;
    int start_idx = m_atomIdx + 1;
    int last_offset = m_curOffset[m_atomIdx];

    AutoContextSwitch mem_guard(m_cxt);

    for (int i = 0; i < rows; i++) {
        if (IS_NULL(*flag)) {
            /* NULL will Keep offset */
            m_curOffset[start_idx++] = last_offset;
            vals++;
            flag++;
            continue;
        }

        /*
         * If value is Numeric 64, store it into atom.
         * Need to check value size. If it is short, it cannot be Numeric64.
         */
        if (!VARATT_IS_SHORT(*vals) && NUMERIC_IS_BI64((Numeric)(*vals))) {
            uint64 numeric_val = (uint64)NUMERIC_64VALUE((Numeric)*vals);
            uint8 scale = NUMERIC_BI_SCALE((Numeric)*vals);
            if (numeric_val <= 0xFF) {
                /* 1 byte value + 1 byte scale */
                if (unlikely(m_curLen + 2 > m_internalLen)) {
                    /*
                     * The size may be not enough, because the element size
                     * is different in one atom.
                     * Try to allocate more space for rest data.
                     * The value 3 may be not good, can change.
                     */
                    m_internalLen += (rows - i) * 3;
                    m_curAtom->data = (char*)repalloc(m_curAtom->data, sizeof(char) * m_internalLen);
                }
                dst = &m_curAtom->data[m_curLen];
                ((n1pack*)dst)->scale = scale;
                ((n1pack*)dst)->val = (uint8)numeric_val;
                m_curLen += 2;
                last_offset += 2;
            } else if (numeric_val <= 0xFFFF) {
                /* 2 bytes value + 1 byte scale */
                if (unlikely(m_curLen + 3 > m_internalLen)) {
                    m_internalLen += (rows - i) * 3;
                    m_curAtom->data = (char*)repalloc(m_curAtom->data, sizeof(char) * m_internalLen);
                }
                dst = &m_curAtom->data[m_curLen];
                ((n2pack*)dst)->scale = scale;
                ((n2pack*)dst)->val = (uint16)numeric_val;
                m_curLen += 3;
                last_offset += 3;
            } else if (numeric_val <= 0xFFFFFFFF) {
                /* 4 bytes value + 1 byte scale */
                if (unlikely(m_curLen + 5 > m_internalLen)) {
                    /*
                     * The value 5 means the least allocated space should be 5
                     * to carry this 5 bytes value.
                     * If need more space, use (rows-i) * 3.
                     */
                    m_internalLen += Max((rows - i) * 3, 5);
                    m_curAtom->data = (char*)repalloc(m_curAtom->data, sizeof(char) * m_internalLen);
                }
                dst = &m_curAtom->data[m_curLen];
                ((n4pack*)dst)->scale = scale;
                ((n4pack*)dst)->val = (uint32)numeric_val;
                m_curLen += 5;
                last_offset += 5;
            } else {
                /* 4 bytes value + 1 byte scale */
                if (unlikely(m_curLen + 9 > m_internalLen)) {
                    m_internalLen += Max((rows - i) * 3, 9);
                    m_curAtom->data = (char*)repalloc(m_curAtom->data, sizeof(char) * m_internalLen);
                }
                dst = &m_curAtom->data[m_curLen];
                ((n8pack*)dst)->scale = scale;
                ((n8pack*)dst)->val = numeric_val;
                m_curLen += 9;
                last_offset += 9;
            }
        } else {
            /*
             * If valus is not Numeric 64, store the pointer into atom.
             * The data is stored in VarBuf m_buf.
             */
            if (m_curLen + 8 > m_internalLen) {
                m_internalLen += Max((rows - i) * 3, 8);
                m_curAtom->data = (char*)repalloc(m_curAtom->data, sizeof(char) * m_internalLen);
            }
            ddst = (Datum*)&m_curAtom->data[m_curLen];
            *ddst = PointerGetDatum(m_buf->Append(DatumGetPointer(*vals), VARSIZE_ANY(*vals)));
            m_curLen += 8;
            last_offset += 8;
        }

        /* offset in atom of next element */
        m_curOffset[start_idx++] = last_offset;

        vals++;
        flag++;
    }
}

/*
 * @Description: Get function. Get both val and flag.
 * @in arr_idx - array index.
 * @in atom_idx - index in atom.
 * @in data - output value.
 * @in flag - output flag.
 * The space of data and flag should be allocated by caller.
 */
void SonicNumericDatumArray::getDatumFlag(int arr_idx, int atom_idx, ScalarValue* val, uint8* flag)
{
    Assert(arr_idx >= 0 && arr_idx < m_arrSize);
    Assert(atom_idx >= 0 && (uint32)atom_idx < m_atomSize);

    m_curOffset = m_offsetArr[arr_idx];
    uint32 offset = m_curOffset[atom_idx + 1] - m_curOffset[atom_idx];
    switch (offset) {
        case 0:
            /* null flag, no data is stored. */
            SET_NULL(*flag);
            return;
        case 2: {
            /* Get 2 bytes data from atom. */
            n1pack* src = (n1pack*)(&m_arr[arr_idx]->data[m_curOffset[atom_idx]]);
            char* dst = m_tmpBuf + (NUMERIC_64SZ) * (m_tmpBufIdx);
            /* Make Numeric64 and store it into VarBuf m_tmpBuf. */
            MAKE_NUMERIC64(dst, src->val, src->scale);
            m_tmpBufIdx++;
            m_tmpBufIdx %= BatchMaxSize;
            *val = PointerGetDatum(dst);
        } break;
        case 3: {
            /* Get 3 bytes data from atom. */
            n2pack* src = (n2pack*)(&m_arr[arr_idx]->data[m_curOffset[atom_idx]]);
            char* dst = m_tmpBuf + (NUMERIC_64SZ) * (m_tmpBufIdx);
            MAKE_NUMERIC64(dst, src->val, src->scale);
            m_tmpBufIdx++;
            m_tmpBufIdx %= BatchMaxSize;
            *val = PointerGetDatum(dst);
        } break;
        case 5: {
            /* Get 5 bytes data from atom. */
            n4pack* src = (n4pack*)(&m_arr[arr_idx]->data[m_curOffset[atom_idx]]);
            char* dst = m_tmpBuf + (NUMERIC_64SZ) * (m_tmpBufIdx);
            MAKE_NUMERIC64(dst, src->val, src->scale);
            m_tmpBufIdx++;
            m_tmpBufIdx %= BatchMaxSize;
            *val = PointerGetDatum(dst);
        } break;
        case 8: {
            /* the pointer is already stored in atom. */
            *val = *(Datum*)(m_arr[arr_idx]->data + m_curOffset[atom_idx]);
        } break;
        case 9: {
            /* Get 9 bytes data from atom. */
            n8pack* src = (n8pack*)(&m_arr[arr_idx]->data[m_curOffset[atom_idx]]);
            char* dst = m_tmpBuf + (NUMERIC_64SZ) * (m_tmpBufIdx);
            MAKE_NUMERIC64(dst, src->val, src->scale);
            m_tmpBufIdx++;
            m_tmpBufIdx %= BatchMaxSize;
            *val = PointerGetDatum(dst);
        } break;
        default:
            break;
    }

    *flag = getNthNullFlag(arr_idx, atom_idx);
}

/*
 * @Description: get function. Get vals.
 * @in arr_idx - array index.
 * @in atom_idx - index in atom.
 * @return - Datum values.
 */
Datum SonicNumericDatumArray::getDatum(int arr_idx, int atom_idx)
{
    Assert(arr_idx >= 0 && arr_idx < m_arrSize);
    Assert(atom_idx >= 0 && (uint32)atom_idx < m_atomSize);

    return ((Datum*)m_arr[arr_idx]->data)[atom_idx];
}

/*
 * @Description: Get function. Get both vals and flags.
 * @in nrows - the number of data to get.
 * @in arr_idx - array index. The size should be nrows.
 * @in atom_idx - index in atom. The size should be nrows.
 * @out data - output values.
 * @out flag - output flags.
 * The space of data and flag should be allocated by caller.
 */
void SonicNumericDatumArray::getDatumFlagArray(int nrows, ArrayIdx* array_idx, Datum* data, uint8* flag)
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
void SonicNumericDatumArray::getDatumFlagArrayWithMatch(
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
 * @in row_idx - which row to put.
 */
size_t SonicNumericDatumArray::flushDatumFlagArrayByRow(
    int arr_idx, uint32* part_idx, void** file_partitions, uint32 col_idx, int row_idx)
{
    size_t total_written = 0;
    size_t written = 0;
    Datum datum;
    uint32 datum_size = 0;
    uint8 offset = 0;
    uint32* cur_offset = m_offsetArr[arr_idx];
    uint8 null_flag = V_NULL_MASK;
    uint8 not_null_flag = V_NOTNULL_MASK;

    SonicHashFileSource* file = NULL;
    SonicHashFilePartition* file_partition = NULL;
    SonicHashPartition** partitions = (SonicHashPartition**)file_partitions;

    file_partition = (SonicHashFilePartition*)partitions[*part_idx];
    int file_idx = file_partition->m_filetype[col_idx];
    file = file_partition->m_files[file_idx];
    offset = cur_offset[row_idx + 1] - cur_offset[row_idx];

    switch (offset) {
        case 0:
            written = (file->write)((void*)&null_flag, sizeof(uint8));
            break;
        case 2:
        case 3:
        case 5:
        case 9:
            written = (file->write)((void*)&not_null_flag, sizeof(uint8));
            written += (file->write)((void*)&offset, sizeof(uint8));
            written += (file->write)((void*)m_curData, offset);
            *(file->m_varSize) += offset;
            break;
        case 8:
            written = (file->write)((void*)&not_null_flag, sizeof(uint8));
            written += (file->write)((void*)&offset, sizeof(uint8));
            datum = PointerGetDatum(*(Datum*)m_curData);
            datum_size = VARSIZE_ANY(datum);
            written += (file->write)((void*)&datum_size, sizeof(uint32));
            written += (file->write)((void*)datum, datum_size);
            *(file->m_varSize) += (datum_size + 8);
            break;
        default:
            ereport(ERROR,
                (errmodule(MOD_VEC_EXECUTOR),
                    errcode(ERRCODE_WRONG_OBJECT_TYPE),
                    errmsg("[VecSonicHashJoin: write numeric atom occurs error.]")));
            break;
    }

    file_partition->m_fileRecords[file_idx] += 1;
    file_partition->m_size += written;
    total_written += written;
    m_curData += offset;

    return total_written;
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
size_t SonicNumericDatumArray::flushDatumFlagArray(
    int arr_idx, uint32* part_idx, void** file_partitions, uint32 col_idx, int nrows)
{
    Assert((uint32)nrows <= m_atomSize);
    size_t total_written = 0;
    size_t written = 0;
    Datum datum;
    uint32 datum_size = 0;
    int atom_idx = 0;
    uint8 offset = 0;
    uint32* cur_offset = m_offsetArr[arr_idx];
    char* cur_data = m_arr[arr_idx]->data;
    uint8 null_flag = V_NULL_MASK;
    uint8 not_null_flag = V_NOTNULL_MASK;

    SonicHashFileSource* file = NULL;
    SonicHashFilePartition* file_partition = NULL;
    SonicHashPartition** partitions = (SonicHashPartition**)file_partitions;

    /* The first position in the first atom doesn't have data. */
    if (unlikely(arr_idx == 0)) {
        ++atom_idx;
        ++part_idx;
    }

    for (; atom_idx < nrows; ++atom_idx, ++part_idx) {
        file_partition = (SonicHashFilePartition*)partitions[*part_idx];
        file = file_partition->m_files[col_idx];
        offset = cur_offset[atom_idx + 1] - cur_offset[atom_idx];

        switch (offset) {
            case 0:
                written = (file->write)((void*)&null_flag, sizeof(uint8));
                break;
            case 2:
            case 3:
            case 5:
            case 9:
                written = (file->write)((void*)&not_null_flag, sizeof(uint8));
                written += (file->write)((void*)&offset, sizeof(uint8));
                written += (file->write)((void*)cur_data, offset);
                *(file->m_varSize) += offset;
                break;
            case 8:
                written = (file->write)((void*)&not_null_flag, sizeof(uint8));
                written += (file->write)((void*)&offset, sizeof(uint8));
                datum = PointerGetDatum(*(Datum*)cur_data);
                datum_size = VARSIZE_ANY(datum);
                written += (file->write)((void*)&datum_size, sizeof(uint32));
                written += (file->write)((void*)datum, datum_size);
                *(file->m_varSize) += (datum_size + 8);
                break;
            default:
                ereport(ERROR,
                    (errmodule(MOD_VEC_EXECUTOR),
                        errcode(ERRCODE_WRONG_OBJECT_TYPE),
                        errmsg("[VecSonicHashJoin: write numeric atom occurs error.]")));
                break;
        }

        file_partition->m_fileRecords[col_idx] += 1;
        file_partition->m_size += written;
        total_written += written;
        cur_data += offset;
    }
    return total_written;
}

/*
 * Description: load data with specific row_idx from specified file to Datum Array .
 * @in file_source - file to load from.
 * @in left_rows - left rows to load in current atom.
 */
void SonicNumericDatumArray::loadDatumFlagArrayByRow(void* file_source, int left_rows)
{
    size_t nread = 0;
    SonicHashFileSource* file = (SonicHashFileSource*)file_source;
    char* dst = &m_curAtom->data[m_curLen];

    uint8 offset = 0;
    char* addr = NULL;
    uint32 datum_size = 0;
    int start_idx = m_atomIdx + 1;
    int last_offset = m_curOffset[m_atomIdx];

    AutoContextSwitch mem_guard(m_cxt);

    if (m_curFlag == NULL) {
        ereport(ERROR,
                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                 errmsg("m_curFlag should not be NULL")));
    }
    nread = (file->read)((void*)m_curFlag, sizeof(uint8));
    CheckReadIsValid(nread, sizeof(uint8));

    if (IS_NULL(*m_curFlag)) {
        /* NULL will Keep offset */
        m_curOffset[start_idx++] = last_offset;
    } else {
        nread = (file->read)((void*)&offset, sizeof(uint8));
        CheckReadIsValid(nread, sizeof(uint8));

        if (unlikely(m_curLen + offset > m_internalLen)) {
            /*
             * The size may be not enough, because the element size
             * is different in one atom.
             * Try to allocate more space for rest data.
             * The value 3 may be not good, can change.
             */
            m_internalLen += Max(left_rows * 3, offset);
            m_curAtom->data = (char*)repalloc(m_curAtom->data, sizeof(char) * m_internalLen);
            dst = &m_curAtom->data[m_curLen];
        }

        switch (offset) {
            case 2:
            case 3:
            case 5:
            case 9:
                nread = (file->read)((void*)dst, offset);
                CheckReadIsValid(nread, offset);
                break;
            case 8:
                nread = (file->read)((void*)&datum_size, sizeof(uint32));
                CheckReadIsValid(nread, sizeof(uint32));

                addr = m_buf->Allocate(datum_size);
                nread = (file->read)((void*)addr, datum_size);
                CheckReadIsValid(nread, (size_t)datum_size);

                (*(Datum*)dst) = PointerGetDatum(addr);
                break;
            default:
                ereport(ERROR,
                    (errmodule(MOD_VEC_EXECUTOR),
                        errcode(ERRCODE_WRONG_OBJECT_TYPE),
                        errmsg("[VecSonicHashJoin: load numeric atom occurs error.]")));
                break;
        }
        m_curLen += offset;
        last_offset += offset;
        /* offset in atom of next element */
        m_curOffset[start_idx++] = last_offset;
    }
    m_curFlag++;
}

/*
 * @Description: load data from specified file to Datum Array.
 * @in file_source - file to load from.
 * @in nrows - rows to load.
 */
void SonicNumericDatumArray::loadDatumFlagArray(void* file_source, int rows)
{
    size_t nread = 0;
    SonicHashFileSource* file = (SonicHashFileSource*)file_source;
    uint8* flag_array = (uint8*)m_curAtom->nullFlag + m_atomIdx * sizeof(uint8);
    char* dst = &m_curAtom->data[m_curLen];

    uint8 offset;
    char* addr = NULL;
    uint32 datum_size = 0;
    int start_idx = m_atomIdx + 1;
    int last_offset = m_curOffset[m_atomIdx];

    AutoContextSwitch mem_guard(m_cxt);

    for (int i = 0; i < rows; ++i) {
        nread = (file->read)((void*)flag_array, sizeof(uint8));
        CheckReadIsValid(nread, sizeof(uint8));

        if (IS_NULL(*flag_array)) {
            /* NULL will Keep offset */
            m_curOffset[start_idx++] = last_offset;
            flag_array++;
            continue;
        }

        nread = (file->read)((void*)&offset, sizeof(uint8));
        CheckReadIsValid(nread, sizeof(uint8));

        if (unlikely(m_curLen + offset > m_internalLen)) {
            /*
             * The size may be not enough, because the element size
             * is different in one atom.
             * Try to allocate more space for rest data.
             * The value 3 may be not good, can change.
             */
            m_internalLen += Max((rows - i) * 3, offset);
            m_curAtom->data = (char*)repalloc(m_curAtom->data, sizeof(char) * m_internalLen);
            dst = &m_curAtom->data[m_curLen];
        }

        switch (offset) {
            case 2:
            case 3:
            case 5:
            case 9:
                nread = (file->read)((void*)dst, offset);
                CheckReadIsValid(nread, offset);
                break;
            case 8:
                nread = (file->read)((void*)&datum_size, sizeof(uint32));
                CheckReadIsValid(nread, sizeof(uint32));

                addr = m_buf->Allocate(datum_size);
                nread = (file->read)((void*)addr, datum_size);
                CheckReadIsValid(nread, (size_t)datum_size);

                (*(Datum*)dst) = PointerGetDatum(addr);
                break;
            default:
                ereport(ERROR,
                    (errmodule(MOD_VEC_EXECUTOR),
                        errcode(ERRCODE_WRONG_OBJECT_TYPE),
                        errmsg("[VecSonicHashJoin: load numeric atom occurs error.]")));
                break;
        }
        dst += offset;
        m_curLen += offset;
        last_offset += offset;
        /* offset in atom of next element */
        m_curOffset[start_idx++] = last_offset;
        flag_array++;
    }
}
