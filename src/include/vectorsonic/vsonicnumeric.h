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
 * vsonicnumeric.h
 *     Routines to handle Numeric data type.
 * 
 * IDENTIFICATION
 *        src/include/vectorsonic/vsonicnumeric.h
 *
 * ---------------------------------------------------------------------------------------
 */


#ifndef SRC_INCLUDE_VECTORSONIC_VSONICNUMERIC_H_
#define SRC_INCLUDE_VECTORSONIC_VSONICNUMERIC_H_

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

class SonicNumericDatumArray : public SonicDatumArray {
public:
    SonicNumericDatumArray(MemoryContext cxt, int capacity, bool genBitMap, DatumDesc* desc);

    ~SonicNumericDatumArray(){};

    void genAdditonArr();

    void genNewArray(bool genBitMap);

    void setResultBuf(char* resultBuf, int* bufIndx);

    void putDatumArrayWithNullCheck(ScalarValue* vals, uint8* flag, int rows);

    void getDatumFlag(int arrIdx, int atomIdx, ScalarValue* val, uint8* flag);

    Datum getDatum(int arrIdx, int atomIdx);

    void getDatumFlagArray(int nrows, ArrayIdx* arrayIdx, Datum* data, uint8* flag);

    void getDatumFlagArrayWithMatch(int nrows, ArrayIdx* arrayIdx, Datum* data, uint8* flag, const bool* matchIdx);

    size_t flushDatumFlagArrayByRow(int arrIdx, uint32* partIdx, void** filePartitions, uint32 colIdx, int rowIdx);

    size_t flushDatumFlagArray(int arrIdx, uint32* partIdx, void** innerPartitions, uint32 colIdx, int nrows);

    void loadDatumFlagArrayByRow(void* fileSource, int leftrows);

    void loadDatumFlagArray(void* fileSource, int rows);

public:
    /* Used for non-numeric64. */
    VarBuf* m_buf;
    /* Manage multiple m_curOffset. */
    uint32** m_offsetArr;
    /* Record current offset. */
    uint32* m_curOffset;
    int m_offArrSize;
    uint32 m_internalLen;
    uint32 m_curLen;
    /* Used for numeric64 when get from atom. */
    char* m_tmpBuf;
    int m_tmpBufIdx;
    char* m_atomBuf;
};

#endif /* SRC_INCLUDE_VECTORSONIC_VSONICNUMERIC_H_ */
