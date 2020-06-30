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
 * vsonicchar.h
 *     Routines to handle Character data type like char, bpchar.
 * 
 * IDENTIFICATION
 *        src/include/vectorsonic/vsonicchar.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_INCLUDE_VECTORSONIC_VSONICCHAR_H_
#define SRC_INCLUDE_VECTORSONIC_VSONICCHAR_H_

#include "vectorsonic/vsonicarray.h"

class SonicCharDatumArray : public SonicDatumArray {
public:
    SonicCharDatumArray(MemoryContext cxt, int capacity, bool genBitMap, DatumDesc* desc);

    ~SonicCharDatumArray(){};

    size_t flushDatumFlagArrayByRow(int arrIdx, uint32* partIdx, void** filePartitions, uint32 colIdx, int rowIdx);

    size_t flushDatumFlagArray(int arrIdx, uint32* partIdx, void** innerPartitions, uint32 colIdx, int nrows);

    void loadDatumFlagArrayByRow(void* file, int leftrows);

    void loadDatumFlagArray(void* file, int rows);

    void putDatumArrayWithNullCheck(ScalarValue* vals, uint8* flag, int rows);

    void getDatumFlagArrayWithMatch(int nrows, ArrayIdx* arrayIdx, Datum* data, uint8* flag, const bool* matchIdx);

    Datum getDatum(int arrIdx, int atomIdx);

    void initCharDatumArray();

private:
    void initDict();

    void initResultBuf();

    void initFunc();

    void putDicDatumArrayWithNullCheck(ScalarValue* vals, uint8* flag, int rows);

    void putCharDatumArrayWithNullCheck(ScalarValue* vals, uint8* flag, int rows);

    void getDatumFlagArray(int nrows, ArrayIdx* arrayIdx, Datum* data, uint8* flag);

    void getDatumFlag(int arrIdx, int atomIdx, ScalarValue* val, uint8* flag);

    void getCharDatumFlag(int arrIdx, int atomIdx, ScalarValue* val, uint8* flag);

    void getDicDatumFlag(int arrIdx, int atomIdx, ScalarValue* val, uint8* flag);

    Datum getCharDatum(int arrIdx, int atomIdx);

    Datum getDicDatum(int arrIdx, int atomIdx);

    void rebuildFromDict();

    typedef void (SonicCharDatumArray::*putFunc)(ScalarValue* vals, uint8* flag, int rows);

    typedef void (SonicCharDatumArray::*putDatumFunc)(ScalarValue* vals, uint8* flag);

    typedef void (SonicCharDatumArray::*getDatumFlagFunc)(int arrIdx, int atomIdx, ScalarValue* val, uint8* flag);

    typedef Datum (SonicCharDatumArray::*getDatumFunc)(int arrIdx, int atomIdx);

    /* put and get functions */
    putFunc m_putFunc;
    getDatumFunc m_getDatumFunc[2];
    getDatumFlagFunc m_getDatumFlagFunc[2];

    /* store dict */
    void* m_dict;

    /* tag dict and char data,
     * for atom whose arrIdx < m_dictArrLen
     * stores dict data, otherwise stores char
     */
    int m_dictArrLen;

    /* buffer to store results */
    char* m_resultBuf;

    /* temp varible to get buffer data */
    int m_resultBufIndx;
};

#endif /* SRC_INCLUDE_VECTORSONIC_VSONICHAR_H_ */
