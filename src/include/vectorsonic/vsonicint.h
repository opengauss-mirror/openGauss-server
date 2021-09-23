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
 * vsonicint.h
 *      Routines to handle data type whose attribute length is  
 *      1, 2, 4, 8.
 *      The length of type can be retrieved from query below:
 *      select oid, typname from pg_type where typlen = len;
 *      There is one exception:
 *      TIDOID's length is 6, and use atomTypeSize 8.
 * 
 * IDENTIFICATION
 *        src/include/vectorsonic/vsonicint.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_INCLUDE_VECTORSONIC_VSONICINT_H_
#define SRC_INCLUDE_VECTORSONIC_VSONICINT_H_

#include "vecexecutor/vechashjoin.h"
#include "vecexecutor/vechashtable.h"
#include "vecexecutor/vecexecutor.h"
#include "vecexecutor/vectorbatch.inl"
#include "postgres.h"
#include "knl/knl_variable.h"
#include "executor/executor.h"
#include "commands/explain.h"
#include "utils/anls_opt.h"
#include "utils/biginteger.h"
#include "utils/builtins.h"
#include "utils/numeric.h"
#include "miscadmin.h"
#include "vecexecutor/vecexpression.h"
#include "fmgr.h"
#include "foreign/fdwapi.h"
#include "tcop/utility.h"
#include "utils/bloom_filter.h"
#include "utils/lsyscache.h"

#include "vectorsonic/vsonicarray.h"

template <typename T>
class SonicIntTemplateDatumArray : public SonicDatumArray {
public:
    SonicIntTemplateDatumArray(MemoryContext cxt, int capacity, bool genBitMap, DatumDesc* desc);

    ~SonicIntTemplateDatumArray(){};

    void putDatumArrayWithNullCheck(ScalarValue* vals, uint8* flag, int rows);

    Datum getDatum(int arrIdx, int atomIdx);

    void getDatumFlag(int arrIdx, int atomIdx, ScalarValue* val, uint8* flag);

    void getDatumArray(int nrows, int* arrIdx, int* atomIdx, Datum* data);

    void getDatumFlagArray(int nrows, ArrayIdx* arrayIdx, Datum* data, uint8* flag);

    void getDatumFlagArrayWithMatch(int nrows, ArrayIdx* arrayIdx, Datum* data, uint8* flag, const bool* matchIdx);

    size_t flushDatumFlagArrayByRow(int arrIdx, uint32* partIdx, void** innerPartitions, uint32 colIdx, int rowIdx);

    size_t flushDatumFlagArray(int arrIdx, uint32* partIdx, void** innerPartitions, uint32 colIdx, int nrows);

    void loadDatumFlagArrayByRow(void* file, int leftrows);

    void loadDatumFlagArray(void* file, int rows);

    void loadIntValArray(void* file, int nrows);

    void loadIntVal(void* file, int rows);

    void setNthDatum(uint32 nth, ScalarValue* val);

    void putIntValArray(T* val, int nrows);

    void putIntVal(T* val, int nrows);
};

/*
 * @Description: constructor.
 * @in cxt - memory context.
 * @in capacity - atom size.
 * @in genBitMap - Whether use null flag.
 * @in desc - data description.
 */
template <typename T>
SonicIntTemplateDatumArray<T>::SonicIntTemplateDatumArray(
    MemoryContext cxt, int capacity, bool genBitMap, DatumDesc* desc)
    : SonicDatumArray(cxt, capacity)
{
    Assert(desc);
    AutoContextSwitch memGuard(m_cxt);

    m_nullFlag = genBitMap;
    m_desc = *desc;
    m_atomTypeSize = m_desc.typeSize;
    genNewArray(genBitMap);
    /*
     * The first position in the first atom doesn't have data.
     * And set the first flag to NULL.
     */
    if (m_nullFlag)
        setNthNullFlag(0, true);

    m_atomIdx = 1;
}

/*
 * @Description: put function.
 * 	keep parameter flag to keep the same interfaces as
 * 	as other data types. Do not check flag for better efficiency.
 * @in vals - data to put.
 * @in flag - flag, not used here.
 * @in rows - the number of data to put.
 */
template <typename T>
void SonicIntTemplateDatumArray<T>::putDatumArrayWithNullCheck(ScalarValue* vals, uint8* flag, int rows)
{
    T* dst = (T*)(m_curAtom->data + sizeof(T) * m_atomIdx);
    for (int i = 0; i < rows; i++) {
        *dst = (T)(*vals);
        dst++;
        vals++;
    }
}

/*
 * @Description: Get function. Get val.
 * @in arrIdx - array index.
 * @in atomIdx - index in atom.
 * @in val - output value.
 * @return - Datum values.
 */
template <typename T>
Datum SonicIntTemplateDatumArray<T>::getDatum(int arrIdx, int atomIdx)
{
    Assert(0 <= arrIdx && arrIdx < m_arrSize);
    Assert(0 <= atomIdx && (uint32)atomIdx < m_atomSize);

    return (Datum)((T*)m_arr[arrIdx]->data)[atomIdx];
}

/*
 * @Description: Get function. Get both val and flag.
 * @in arrIdx - array index.
 * @in atomIdx - index in atom.
 * @in val - output value.
 * @in flag - output flag.
 */
template <typename T>
void SonicIntTemplateDatumArray<T>::getDatumFlag(int arrIdx, int atomIdx, ScalarValue* val, uint8* flag)
{
    Assert(0 <= arrIdx && arrIdx < m_arrSize);
    Assert(0 <= atomIdx && (uint32)atomIdx < m_atomSize);

    *val = ((T*)m_arr[arrIdx]->data)[atomIdx];
    *flag = getNthNullFlag(arrIdx, atomIdx);
}

/*
 * @Description: Get function. Get vals.
 * @in nrows - the number of data to get.
 * @in arrIdx - array index. The size should be nrows.
 * @in atomIdx - index in atom. The size should be nrows.
 * @in data - output values.
 * The space of data and flag should be allocated by caller.
 */
template <typename T>
void SonicIntTemplateDatumArray<T>::getDatumArray(int nrows, int* arrIdx, int* atomIdx, Datum* data)
{
    Assert(nrows <= BatchMaxSize);
    int i;

    for (i = 0; i < nrows; i++) {
        *data = (Datum)((T*)m_arr[*arrIdx]->data)[*atomIdx];
        arrIdx++;
        atomIdx++;
        data++;
    }
}

/*
 * @Description: Get function. Get both vals and flags.
 * @in nrows - the number of data to get.
 * @in arrIdx - array index. The size should be nrows.
 * @in atomIdx - index in atom. The size should be nrows.
 * @in data - output values.
 * @in flag - output flags.
 * The space of data and flag should be allocated by caller.
 */
template <typename T>
void SonicIntTemplateDatumArray<T>::getDatumFlagArray(int nrows, ArrayIdx* arrayIdx, Datum* data, uint8* flag)
{
    Assert(nrows <= BatchMaxSize);
    int i;

    for (i = 0; i < nrows; i++) {
        *flag = getNthNullFlag(arrayIdx->arrIdx, arrayIdx->atomIdx);
        *data = (Datum)((T*)m_arr[arrayIdx->arrIdx]->data)[arrayIdx->atomIdx];
        arrayIdx++;
        data++;
        flag++;
    }
}

/*
 * @Description: Get function when does hash key matching. Get data and flag from atoms.
 * @in nrows - the number of data to get.
 * @in arrIdx - array index. The size should be nrows.
 * @in atomIdx - index in atom. The size should be nrows.
 * @in data - output values.
 * @in flag - output flags.
 * @in matchIdx - matched array. If false, means current tuple pair doesn't match,
 * 	so no need to get datum and flag any more, skip it.
 * The space of data and flag should be allocated by caller.
 */
template <typename T>
void SonicIntTemplateDatumArray<T>::getDatumFlagArrayWithMatch(
    int nrows, ArrayIdx* arrayIdx, Datum* data, uint8* flag, const bool* matchIdx)
{
    Assert(nrows <= BatchMaxSize);
    for (int i = 0; i < nrows; i++) {
        if (*matchIdx) {
            int arrIdx = arrayIdx->arrIdx;
            int atomIdx = arrayIdx->atomIdx;

            *flag = m_arr[arrIdx]->nullFlag[atomIdx];
            *data = (Datum)((T*)m_arr[arrIdx]->data)[atomIdx];
        }
        matchIdx++;
        arrayIdx++;
        data++;
        flag++;
    }
}

/*
 * @Description: set value into nth position in atom.
 *	The space in atom must be valid.
 * @in nth - value position.
 * @in val - value to put.
 */
template <typename T>
void SonicIntTemplateDatumArray<T>::setNthDatum(uint32 nth, ScalarValue* val)
{
    int arrIndx;
    int atomIndx;

    arrIndx = (int)getArrayIndx(nth, m_nbit);
    atomIndx = (int)getArrayLoc(nth, m_atomSize - 1);

    // Make sure the space is already allocated.
    Assert(&(((T*)m_arr[arrIndx]->data)[atomIndx]));
    ((T*)m_arr[arrIndx]->data)[atomIndx] = *((T*)val);
}

/*
 * @Description: put data into atom.
 *	Do not consider flag in this function.
 *	If more space is needed, allocate it.
 * @in val - data to put.
 * @in nrows - the number of data.
 */
template <typename T>
void SonicIntTemplateDatumArray<T>::putIntValArray(T* val, int nrows)
{
    int loopRows;

    bool needExpand = (uint32)nrows > (m_atomSize - m_atomIdx);
    loopRows = needExpand ? (m_atomSize - m_atomIdx) : nrows;

    if (loopRows > 0) {
        putIntVal(val, loopRows);
        m_atomIdx += loopRows;
    }

    if (needExpand) {
        genNewArray(m_nullFlag);
        putIntValArray(val + loopRows, nrows - loopRows);
    }
}

/*
 * @Description: put data into single atom.
 *	Do not consider flag in this function.
 *	The atom used must be valid.
 * @in val - data to put.
 * @in nrows - the number of data.
 */
template <typename T>
void SonicIntTemplateDatumArray<T>::putIntVal(T* val, int nrows)
{
    T* dst = (uint32*)(m_curAtom->data + sizeof(T) * m_atomIdx);
    for (int i = 0; i < nrows; i++) {
        *dst = *val;
        dst++;
        val++;
    }
}

extern SonicDatumArray* AllocateIntArray(
    MemoryContext createContext, MemoryContext internalContext, int capacity, bool genNullFlag, DatumDesc* desc);

#endif /* SRC_INCLUDE_VECTORSONIC_VSONICINT_H_ */
