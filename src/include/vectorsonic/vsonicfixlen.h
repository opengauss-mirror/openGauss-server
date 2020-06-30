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
 * vsonicfixlen.h
 *     Routines to handle data type that attribute has fixed length.
 * 
 * IDENTIFICATION
 *        src/include/vectorsonic/vsonicfixlen.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_INCLUDE_VECTORSONIC_VSONICFIXLEN_H_
#define SRC_INCLUDE_VECTORSONIC_VSONICFIXLEN_H_

#include "vectorsonic/vsonicarray.h"
#include "vectorsonic/vsonicchar.h"

class SonicFixLenDatumArray : public SonicDatumArray {
public:
    SonicFixLenDatumArray(MemoryContext cxt, int capacity, bool genBitMap, DatumDesc* desc);

    ~SonicFixLenDatumArray(){};

    void putDatumArrayWithNullCheck(ScalarValue* vals, uint8* flag, int rows);

    void getDatumFlag(int arrIdx, int atomIdx, ScalarValue* val, uint8* flag);

    Datum getDatum(int arrIdx, int atomIdx);

    void getDatumFlagArray(int nrows, ArrayIdx* arrayIdx, Datum* data, uint8* flag);

    void getDatumFlagArrayWithMatch(int nrows, ArrayIdx* arrayIdx, Datum* data, uint8* flag, const bool* matchIdx);

    size_t flushDatumFlagArrayByRow(int arrIdx, uint32* partIdx, void** filePartitions, uint32 colIdx, int rowIdx);

    size_t flushDatumFlagArray(int arrIdx, uint32* partIdx, void** innerPartitions, uint32 colIdx, int nrows);

    void loadDatumFlagArray(void* fileSource, int rows);

    void loadDatumFlagArrayByRow(void* fileSource, int leftrows);

private:
    void initResultBuf();

    /* buffer to assemble results */
    char* m_resultBuf;

    /* index in the buffer */
    int m_resultBufIndx;

    /* header size*/
    int m_varheadlen;
};
#endif /* SRC_INCLUDE_VECTORSONIC_VSONICFIXLEN_H_ */
