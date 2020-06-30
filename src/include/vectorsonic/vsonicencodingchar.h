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
 * vsonicencodingchar.h
 *     Routines to handle variable length data type.
 *
 * IDENTIFICATION
 *        src/include/vectorsonic/vsonicencodingchar.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_INCLUDE_VECTORSONIC_VSONICENCODINGCHAR_H_
#define SRC_INCLUDE_VECTORSONIC_VSONICENCODINGCHAR_H_

#include "vectorsonic/vsonicarray.h"

class SonicEncodingDatumArray : public SonicDatumArray {
public:
    SonicEncodingDatumArray(MemoryContext cxt, int capacity, bool genBitMap, DatumDesc* desc);

    ~SonicEncodingDatumArray(){};

    void getDatumFlagArray(int nrows, ArrayIdx* arrayIdx, Datum* data, uint8* flag);

    void getDatumFlagArrayWithMatch(int nrows, ArrayIdx* arrayIdx, Datum* data, uint8* flag, const bool* matchIdx);

    size_t flushDatumFlagArrayByRow(int arrIdx, uint32* partIdx, void** filePartitions, uint32 colIdx, int rowIdx);

    size_t flushDatumFlagArray(int arrIdx, uint32* partIdx, void** innerPartitions, uint32 colIdx, int nrows);

    void setValue(ScalarValue val, bool isNull, int arrIndx, int atomIndx);

    Datum getDatum(int arrIdx, int atomIdx);

private:
    void getDatumFlag(int arrIdx, int atomIdx, ScalarValue* val, uint8* flag);

    void putDatumArrayWithNullCheck(ScalarValue* vals, uint8* flag, int rows);

    void loadDatumFlagArray(void* file, int rows);
};

class SonicStackEncodingDatumArray : public SonicEncodingDatumArray {
public:
    SonicStackEncodingDatumArray(MemoryContext cxt, int capacity, bool genBitMap, DatumDesc* desc);

    ~SonicStackEncodingDatumArray(){};

private:
    void putDatumArrayWithNullCheck(ScalarValue* vals, uint8* flag, int rows);

    void putDatumFlag(ScalarValue* vals, uint8* flag);

    void loadDatumFlagArray(void* fileSource, int rows);

    void loadDatumFlagArrayByRow(void* file, int leftrows);

    VarBuf* m_buf;
};

#endif /* SRC_INCLUDE_VECTORSONIC_VSONICENCODINGCHAR_H_ */
