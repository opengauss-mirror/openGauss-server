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
 * vsonicarray.h
 *     Routines to handle Sonic datum array.
 *     SonicDatumArray is the base class to store data and flag.
 *
 * IDENTIFICATION
 *        src/include/vectorsonic/vsonicarray.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_INCLUDE_VECTORSONIC_VSONICARRAY_H_
#define SRC_INCLUDE_VECTORSONIC_VSONICARRAY_H_

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

struct atom {
    char* data;
    char* nullFlag;
};

struct DatumDesc {
    Oid typeId;
    int typeMod;
    int typeSize;
    int dataType;

    DatumDesc()
    {
        typeId = InvalidOid;
        typeMod = -1;
        typeSize = 0;
        dataType = 0;
    };
};

#pragma pack(1)
struct n1pack {
    uint8 scale;
    uint8 val;
};

struct n2pack {
    uint8 scale;
    uint16 val;
};

struct n4pack {
    uint8 scale;
    uint32 val;
};

struct n8pack {
    uint8 scale;
    uint64 val;
};

#pragma pack()

struct ArrayIdx {
    int arrIdx;
    int16 atomIdx;
};

#define INIT_ARR_CONTAINER_SIZE 16
#define MAX_DIC_ENCODE_ITEMS 256
#define INIT_DATUM_ARRAY_SIZE (16 * 1024)

#define SONIC_INT_TYPE 0
#define SONIC_FIXLEN_TYPE 1
#define SONIC_CHAR_DIC_TYPE 2
#define SONIC_VAR_TYPE 3
#define SONIC_NUMERIC_COMPRESS_TYPE 4
#define SONIC_TOTAL_TYPES 5

#define getArrayIndx(nth, nbit) (nth >> (nbit))
#define getArrayLoc(nth, mask) (nth & (unsigned int)(mask))

class SonicDatumArray : public BaseObject {
public:
    SonicDatumArray(MemoryContext cxt, int capacity);

    virtual ~SonicDatumArray()
    {}

    /* descr area, static information */
    MemoryContext m_cxt;     /* context */
    DatumDesc m_desc;        /* datum descrption */
    bool m_nullFlag;         /* Whether atom has null flag array */
    int m_arrSize;           /* The number of allocated atoms. Init value is INIT_ARR_CONTAINER_SIZE 16 */
    const uint32 m_atomSize; /* The size of one atom. Def value is INIT_DATUM_ARRAY_SIZE 16k */
    int m_atomTypeSize;      /* Element type size in atom. */
    const uint32 m_nbit;     /* log2(m_atomSize) */

    /*data area*/
    atom** m_arr;    /* Record atoms address.. Each element points to one atom. */
    atom* m_curAtom; /* Current atom */

    /*index area*/
    int m_arrIdx;  /* m_arrIdx + 1 == the number of used atoms. Init value -1 */
    int m_atomIdx; /* Current position in atom */

    char* m_curData;  /* record the current data in atom */
    uint8* m_curFlag; /* record the current flag in atom */

public:
    virtual void genNewArray(bool genBitMap);

    /* put function. */
    void putArray(ScalarValue* vals, uint8* flag, int rows);

    void loadArrayByRow(void* file);

    void loadArray(void* file, int64 rows);

    /* get function */
    int64 getRows();

    FORCE_INLINE
    uint8 getNthNullFlag(int arrIdx, int atomIdx)
    {
        return m_arr[arrIdx]->nullFlag[atomIdx];
    }

    void getNthDatumFlag(uint32 nth, ScalarValue* val, uint8* toflag);

    Datum getNthDatum(uint32 nth);

    void getArrayAtomIdx(int nrows, uint32* locs, ArrayIdx* arrayIdx);

    /* set function. */
    ScalarValue replaceVariable(ScalarValue oldVal, ScalarValue val);

    FORCE_INLINE
    void setNthNullFlag(uint32 nth, bool isNull)
    {
        int arrIdx;
        int atomIdx;
        arrIdx = getArrayIndx(nth, m_nbit);
        atomIdx = getArrayLoc(nth, m_atomSize - 1);
        m_arr[arrIdx]->nullFlag[atomIdx] = isNull;
    }

    FORCE_INLINE
    void setNthNullFlag(int arrIdx, int atomIdx, bool isNull)
    {
        m_arr[arrIdx]->nullFlag[atomIdx] = isNull;
    }

    virtual void setNthDatum(uint32 nth, ScalarValue* val)
    {
        Assert(false);
    }

    virtual void setValue(ScalarValue val, bool isNull, int arrIndx, int atomIndx)
    {
        Assert(false);
    }

    virtual void getDatumArray(int nrows, int* arrayIdx, int* atomIdx, Datum* data)
    {
        Assert(false);
    }

    virtual void getDatumFlagArray(int nrows, ArrayIdx* arrayIdx, Datum* data, uint8* flag)
    {
        Assert(false);
    }

    virtual void getDatumFlagArrayWithMatch(
        int nrows, ArrayIdx* arrayIdx, Datum* data, uint8* flag, const bool* matchIdx)
    {
        Assert(false);
    }

    virtual size_t flushDatumFlagArrayByRow(
        int arrIdx, uint32* partIdx, void** innerPartitions, uint32 colIdx, int nrows)
    {
        return 0;
    }

    virtual size_t flushDatumFlagArray(int arrIdx, uint32* partIdx, void** innerPartitions, uint32 colIdx, int nrows)
    {
        return 0;
    }

    virtual void loadIntValArray(void* file, int nrows)
    {
        Assert(false);
    }

    virtual void putDatumArrayWithNullCheck(ScalarValue* vals, uint8* flag, int rows)
    {
        Assert(false);
    }

    virtual void getDatumFlag(int arrIdx, int atomIdx, ScalarValue* val, uint8* toflag)
    {
        Assert(false);
    }

private:
    void putNullFlagArray(uint8* flag, int rows);

    virtual void loadDatumFlagArray(void* file, int rows)
    {
        Assert(false);
    }

    virtual void loadDatumFlagArrayByRow(void* file, int leftrows)
    {
        Assert(false);
    }

    virtual Datum getDatum(int arrIdx, int atomIdx)
    {
        Assert(false);
        return 0;
    }
};

extern int getDataMinLen(Oid typeOid, int typeMod);
extern void getDataDesc(DatumDesc* desc, int typeSize, Form_pg_attribute attr, bool isHashKey);

#endif /* SRC_INCLUDE_VECTORSONIC_VSONICARRAY_H_ */
