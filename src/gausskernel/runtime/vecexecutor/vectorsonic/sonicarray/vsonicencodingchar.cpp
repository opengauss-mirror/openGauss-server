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
 * vsonicencodingchar.cpp
 *     Routines to handle variable length data type.
 *
 * IDENTIFICATION
 *     src/backend/vectorsonic/sonicarray/vsonicencodingchar.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "c.h"
#include "catalog/pg_type.h"
#include "storage/compress_kits.h"
#include "utils/dynahash.h"
#include "vectorsonic/vsonicencodingchar.h"
#include "vectorsonic/vsonicpartition.h"

/*
 * @Description: constructor.
 * 	Store pointer in atom. The pointer points to data stored in other places.
 * @in cxt - memory context.
 * @in capacity - atom size.
 * @in gen_bit_map - Whether use null flag.
 * @in desc - data description.
 */
SonicEncodingDatumArray::SonicEncodingDatumArray(MemoryContext cxt, int capacity, bool gen_bit_map, DatumDesc* desc)
    : SonicDatumArray(cxt, capacity)
{
    AutoContextSwitch mem_guard(m_cxt);

    m_nullFlag = gen_bit_map;
    m_desc = *desc;
    m_atomTypeSize = m_desc.typeSize;

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
 * @Description: Get function. Get both vals and flag.
 * @in arr_idx - array index.
 * @in atom_idx - index in atom.
 * @in val - output value.
 * @in flag - output flag.
 */
void SonicEncodingDatumArray::getDatumFlag(int arr_idx, int atom_idx, ScalarValue* val, uint8* flag)
{
    Assert(arr_idx >= 0 && arr_idx < m_arrSize);
    Assert(atom_idx >= 0 && (uint32)atom_idx < m_atomSize);

    *flag = getNthNullFlag(arr_idx, atom_idx);
    if (NOT_NULL(*flag))
        *val = ((Datum*)m_arr[arr_idx]->data)[atom_idx];
}

/*
 * @Description: get function. Get vals.
 * @in arr_idx: array index.
 * @in atom_idx: index in atom.
 * @return Datum values.
 */
Datum SonicEncodingDatumArray::getDatum(int arr_idx, int atom_idx)
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
 * @in data - output values.
 * @in flag - output flags.
 * The space of data and flag should be allocated by caller.
 */
void SonicEncodingDatumArray::getDatumFlagArray(int nrows, ArrayIdx* array_idx, Datum* data, uint8* flag)
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
 * @Description: Get function when doing hash key matching. Get data and flag from atoms.
 * @in nrows - the number of data to get.
 * @in arr_idx - array index. The size should be nrows.
 * @in atom_idx - index in atom. The size should be nrows.
 * @in data - output values.
 * @in flag - output flags.
 * @in match_idx - matched array. If false, means current tuple pair doesn't match,
 * 	so no need to get datum and flag any more, skip it.
 * The space of data and flag should be allocated by caller.
 */
void SonicEncodingDatumArray::getDatumFlagArrayWithMatch(
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
 * @Description: Put pointer into atom.
 * 	The data is stored in space palloced each time.
 * @in vals - data to put.
 * @in flag - flag checked when put data.
 * @in rows - the number of data to put.
 */
void SonicEncodingDatumArray::putDatumArrayWithNullCheck(ScalarValue* vals, uint8* flag, int rows)
{
    Datum* dst = (Datum*)(m_curAtom->data + m_atomIdx * sizeof(Datum));
    errno_t rc;
    AutoContextSwitch mem_guard(m_cxt);
    for (int i = 0; i < rows; i++) {
        if (NOT_NULL(*flag)) {
            int KeySize = VARSIZE_ANY(vals);
            char* addr = (char*)palloc(KeySize);

            rc = memcpy_s(addr, KeySize, vals, KeySize);
            securec_check(rc, "\0", "\0");
            *dst = PointerGetDatum(addr);
        }
        dst++;
        vals++;
        flag++;
    }
}

/*
 * @Description: flush each row in the atom into the files of the partition this row belongs to.
 * @in arr_idx - array index.
 * @in part_idx - partition indexes
 * @in file_partitions - file partitions to put the data.
 * @in col_idx - column index.
 * @in row_idx - which row to put.
 */
size_t SonicEncodingDatumArray::flushDatumFlagArrayByRow(
    int arr_idx, uint32* part_idx, void** file_partitions, uint32 col_idx, int row_idx)
{
    size_t written = 0;
    Datum datum;
    uint32 datum_size = 0;
    SonicHashFileSource* file = NULL;
    SonicHashFilePartition* file_partition = NULL;
    SonicHashPartition** partitions = (SonicHashPartition**)file_partitions;

    file_partition = (SonicHashFilePartition*)partitions[*part_idx];
    int file_idx = file_partition->m_filetype[col_idx];
    file = file_partition->m_files[file_idx];
    written = (file->write)((void*)m_curFlag, sizeof(uint8));
    if (NOT_NULL(*m_curFlag)) {
        datum = PointerGetDatum(*(Datum*)m_curData);
        datum_size = VARSIZE_ANY(datum);
        written += (file->write)((void*)&datum_size, sizeof(uint32));
        written += (file->write)((void*)datum, datum_size);
        *(file->m_varSize) += datum_size;
    }
    file_partition->m_fileRecords[file_idx] += 1;

    file_partition->m_size += written;
    m_curData += m_atomTypeSize;
    m_curFlag++;

    return written;
}

/*
 * @Description: flush each row in the atom into the files of the partition this row belongs to.
 * @in arr_idx - array index.
 * @in part_idx - partition indexes
 * @in file_partitions - file partitions to put the data.
 * @in col_idx - column index.
 * @in nrows - rows to put.
 */
size_t SonicEncodingDatumArray::flushDatumFlagArray(
    int arr_idx, uint32* part_idx, void** file_partitions, uint32 col_idx, int nrows)
{
    Assert((uint32)nrows <= m_atomSize);
    size_t total_written = 0;
    size_t written = 0;
    Datum datum;
    uint32 datum_size = 0;
    uint8* flag = (uint8*)m_arr[arr_idx]->nullFlag;
    char* cur_data = m_arr[arr_idx]->data;
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

    for (; atom_idx < nrows; ++atom_idx, ++part_idx) {
        file_partition = (SonicHashFilePartition*)partitions[*part_idx];
        file = file_partition->m_files[col_idx];

        written = (file->write)((void*)flag, sizeof(uint8));
        if (NOT_NULL(*flag)) {
            datum = PointerGetDatum(*(Datum*)cur_data);
            datum_size = VARSIZE_ANY(datum);
            written += (file->write)((void*)&datum_size, sizeof(uint32));
            written += (file->write)((void*)datum, datum_size);
            *(file->m_varSize) += datum_size;
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
void SonicEncodingDatumArray::loadDatumFlagArray(void* file_source, int rows)
{
    size_t nread = 0;
    uint32 datum_size = 0;
    SonicHashFileSource* file = (SonicHashFileSource*)file_source;
    Datum* dst = (Datum*)(m_curAtom->data + m_atomIdx * sizeof(Datum));
    uint8* flag_array = (uint8*)m_curAtom->nullFlag + m_atomIdx * sizeof(uint8);
    char* addr = NULL;

    AutoContextSwitch mem_guard(m_cxt);

    for (int i = 0; i < rows; ++i) {
        nread = (file->read)((void*)flag_array, sizeof(uint8));
        CheckReadIsValid(nread, sizeof(uint8));

        if (NOT_NULL(*flag_array)) {
            nread = (file->read)((void*)&datum_size, sizeof(uint32));
            CheckReadIsValid(nread, sizeof(uint32));

            addr = (char*)palloc(datum_size);
            nread = (file->read)((void*)addr, datum_size);
            CheckReadIsValid(nread, (size_t)datum_size);
            *dst = PointerGetDatum(addr);
        }
        flag_array++;
        dst++;
    }
}
/*
 * @Description: set data value in encoding m_arr.
 * @in val - value to be set
 * @in is_null - value's null flag
 * @in arr_idx - array index. The size should be nrows.
 * @in atom_idx - index in atom. The size should be nrows.
 */
void SonicEncodingDatumArray::setValue(Datum val, bool is_null, int arr_indx, int atom_indx)
{
    errno_t rc;

    if (!is_null) {
        AutoContextSwitch mem_guard(m_cxt);
        int key_size = VARSIZE_ANY(val);
        char* addr = (char*)palloc(key_size);
        rc = memcpy_s(addr, key_size, DatumGetPointer(val), key_size);
        securec_check(rc, "\0", "\0");
        /* store the address of palloced space */
        ((Datum*)m_arr[arr_indx]->data)[atom_indx] = PointerGetDatum(addr);
    }

    setNthNullFlag(arr_indx, atom_indx, is_null);
}

/*
 * @Description: constructor.
 * 	Store pointer in atom. The pointer points to data stored VarBuf.
 * 	SonicHashJoin use this structure.
 * @in cxt - memory context.
 * @in capacity - atom size.
 * @in gen_bit_map - Whether use null flag.
 * @in desc - data description.
 */
SonicStackEncodingDatumArray::SonicStackEncodingDatumArray(
    MemoryContext cxt, int capacity, bool gen_bit_map, DatumDesc* desc)
    : SonicEncodingDatumArray(cxt, capacity, gen_bit_map, desc)
{
    AutoContextSwitch mem_guard(m_cxt);
    m_buf = New(m_cxt) VarBuf(m_cxt);
}

/*
 * @Description: Put pointer into atom.
 * 	The data is stored in VarBuf.
 * @in val - data to put.
 * @in flag - flag checked when put data.
 */
void SonicStackEncodingDatumArray::putDatumFlag(ScalarValue* vals, uint8* flag)
{
    Datum* dst = (Datum*)(m_curAtom->data + m_atomIdx * sizeof(Datum));

    AutoContextSwitch mem_guard(m_cxt);
    if (NOT_NULL(*flag))
        *dst = PointerGetDatum(m_buf->Append(DatumGetPointer(*vals), VARSIZE_ANY(*vals)));
}

/*
 * @Description: Put pointer into atom.
 * 	The data is stored in VarBuf.
 * @in vals - data to put.
 * @in flag - flag checked when put data.
 * @in rows - the number of data to put.
 */
void SonicStackEncodingDatumArray::putDatumArrayWithNullCheck(ScalarValue* vals, uint8* flag, int rows)
{
    Datum* dst = (Datum*)(m_curAtom->data + m_atomIdx * sizeof(Datum));
    int dataLen = 0;

    AutoContextSwitch mem_guard(m_cxt);
    for (int i = 0; i < rows; ++i) {
        if (NOT_NULL(*flag)) {
            if (this->m_desc.typeId == NAMEOID) {
                dataLen = datumGetSize(*vals, false, -2);
            } else {
                dataLen = VARSIZE_ANY(*vals);
            }
            *dst = PointerGetDatum(m_buf->Append(DatumGetPointer(*vals), dataLen));
        }
        dst++;
        vals++;
        flag++;
    }
}

/*
 * @Description: load data from specified file to Datum Array.
 * @in file_source - file to load from.
 * @in nrows - rows to load.
 */
void SonicStackEncodingDatumArray::loadDatumFlagArray(void* file_source, int rows)
{
    size_t nread = 0;
    uint32 datum_size = 0;
    SonicHashFileSource* file = (SonicHashFileSource*)file_source;
    Datum* dst = (Datum*)(m_curAtom->data + m_atomIdx * sizeof(Datum));
    char* flag_array = &m_curAtom->nullFlag[m_atomIdx];
    char* addr = NULL;

    AutoContextSwitch mem_guard(m_cxt);

    for (int i = 0; i < rows; ++i) {
        nread = (file->read)((void*)flag_array, sizeof(uint8));
        CheckReadIsValid(nread, sizeof(uint8));

        if (NOT_NULL(*flag_array)) {
            nread = (file->read)((void*)&datum_size, sizeof(uint32));
            CheckReadIsValid(nread, sizeof(uint32));

            addr = m_buf->Allocate(datum_size);
            nread = (file->read)((void*)addr, datum_size);
            CheckReadIsValid(nread, (size_t)datum_size);
            *dst = PointerGetDatum(addr);
        }
        flag_array++;
        dst++;
    }
}

/*
 * Description: load data with specific row_idx from specified file to Datum Array .
 * @in file_source - file to load from.
 * @in left_rows - left rows to load, not used.
 */
void SonicStackEncodingDatumArray::loadDatumFlagArrayByRow(void* file_source, int left_rows)
{
    size_t nread = 0;
    uint32 datum_size = 0;
    SonicHashFileSource* file = (SonicHashFileSource*)file_source;
    Datum* dst = (Datum*)m_curData;
    char* flag_array = &m_curAtom->nullFlag[m_atomIdx];
    char* addr = NULL;

    AutoContextSwitch mem_guard(m_cxt);
    nread = (file->read)((void*)flag_array, sizeof(uint8));
    CheckReadIsValid(nread, sizeof(uint8));

    if (NOT_NULL(*flag_array)) {
        nread = (file->read)((void*)&datum_size, sizeof(uint32));
        CheckReadIsValid(nread, sizeof(uint32));

        addr = m_buf->Allocate(datum_size);
        nread = (file->read)((void*)addr, datum_size);
        CheckReadIsValid(nread, (size_t)datum_size);
        *dst = PointerGetDatum(addr);
    }
    m_curData += sizeof(Datum);
}
