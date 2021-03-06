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
 * vsonichash.h
 *     Routines to handle vector sonic hash nodes.
 *     SonicHash is base class of SonicHashJoin and SonicHashAgg.
 *     This file also contains many hash functions.
 * 
 * IDENTIFICATION
 *        src/include/vectorsonic/vsonichash.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_INCLUDE_VECTORSONIC_VSONICHASH_H_
#define SRC_INCLUDE_VECTORSONIC_VSONICHASH_H_

#include "vecexecutor/vectorbatch.h"
#include "vectorsonic/vsonicarray.h"
#include "vectorsonic/vsonicnumeric.h"
#include "vectorsonic/vsonicint.h"
#include "vectorsonic/vsonicchar.h"
#include "vectorsonic/vsonicencodingchar.h"
#include "vectorsonic/vsonicfixlen.h"

#define PROBE_FETCH 0
#define PROBE_PARTITION_FILE 1
#define PROBE_DATA 2
#define PROBE_FINAL 3
#define PROBE_PREPARE_PAIR 4
#define PROBE_PARTITION_MEM 5

#define USE_PRIME

#ifdef USE_PRIME
/*
 * The prime calculated via hashfindrprime() with rows
 * larger than SONIC_MAX_ROWS will be larger than UINT_MAX.
 */
#define SONIC_MAX_ROWS 4080218831
#else
#define SONIC_MAX_ROWS 4294950910
#endif

typedef enum { CALC_BASE = 0, CALC_SPILL, CALC_HASHTABLE } CalcBatchHashType;

struct hashStateLog {
    int lastProcessIdx;
    bool restore;
};

struct SonicHashMemoryControl {

    bool sysBusy;
    /* record spill info */
    bool spillToDisk; /* true when next batch should put into disk */
    int spillNum;
    /* record memory auto spread info */
    uint64 maxMem;
    int spreadNum;
    /* record context memory info */
    uint64 totalMem;
    uint64 availMem;
    uint64 allocatedMem;
    MemoryContext hashContext;
    MemoryContext tmpContext;

    SonicHashMemoryControl()
    {
        sysBusy = false;
        spillToDisk = false;
        spillNum = 0;
        maxMem = 0;
        spreadNum = 0;
        totalMem = 0;
        availMem = 0;
        allocatedMem = 0;
        hashContext = NULL;
        tmpContext = NULL;
    };
};

class SonicHashSource : public BaseObject {
public:
    virtual VectorBatch* getBatch()
    {
        Assert(false);
        return NULL;
    }

    virtual ~SonicHashSource()
    {}
};

class SonicHashOpSource : public SonicHashSource {
public:
    SonicHashOpSource(PlanState* op) : m_op(op)
    {}
    ~SonicHashOpSource(){};

    VectorBatch* getBatch()
    {
        return VectorEngine(m_op);
    }

private:
    /* data source operator */
    PlanState* m_op;
};

class SonicHash : public BaseObject {
public:
    SonicHash(int size);

    virtual ~SonicHash()
    {}

    void initHashFunc(TupleDesc desc, void* hashFun, uint16* m_keyIndx, bool isAtom = false);

    /* Hash Functions */
    /* Specialized for int8, int16, int32 */
    template <bool rehash, typename containerType, typename realType>
    void hashInteger(char* val, uint8* flag, int nval, uint32* res, FmgrInfo* hashFmgr = NULL);

    /* Specialized for int64 */
    template <bool rehash>
    void hashInteger8(char* val, uint8* flag, int nval, uint32* res, FmgrInfo* hashFmgr = NULL);

    template <bool rehash>
    void hashbpchar(char* val, uint8* flag, int nval, uint32* res, FmgrInfo* hashFmgr = NULL);

    template <bool rehash>
    void hashtext(char* val, uint8* flag, int nval, uint32* res, FmgrInfo* hashFmgr = NULL);

    template <bool rehash>
    void hashtext_compatible(char* val, uint8* flag, int nval, uint32* res, FmgrInfo* hashFmgr = NULL);

    template <bool rehash>
    void hashnumeric(char* val, uint8* flag, int nval, uint32* res, FmgrInfo* hashFmgr = NULL);

    /* General hash functions */
    template <bool rehash, typename containerType, typename realType>
    void hashGeneralFunc(char* val, uint8* flag, int nval, uint32* res, FmgrInfo* hashFmgr);

    void freeMemoryContext();

    int64 getRows();

    bool integerType(Oid typeId);

    bool isHashKey(Oid typeOid, int attidx, uint16* keyIdx, int keyNum);

    void replaceEqFunc();

    inline void hashBatchArray(VectorBatch* batch, void* hashFun, FmgrInfo* hashFmgr, uint16* keyIndx, uint32* hashRes)
    {
        int i;
        AutoContextSwitch memGuard(m_memControl.tmpContext);
        hashValFun* hashfun = (hashValFun*)hashFun;

        for (i = 0; i < m_buildOp.keyNum; i++)
            RuntimeBinding(hashfun, i)((char*)batch->m_arr[keyIndx[i]].m_vals,
                (uint8*)batch->m_arr[keyIndx[i]].m_flag,
                batch->m_rows,
                hashRes,
                (hashFmgr + i));

        MemoryContextReset(m_memControl.tmpContext);
    }

    inline void hashAtomArray(SonicDatumArray** array, int nrows, int arrIdx, void* hashFun, FmgrInfo* hashFmgr,
        uint16* keyIndx, uint32* hashRes)
    {
        int i;
        AutoContextSwitch memGuard(m_memControl.tmpContext);
        atom* datum_arr;
        hashValFun* hashfun = (hashValFun*)hashFun;

        for (i = 0; i < m_buildOp.keyNum; i++) {
            datum_arr = array[keyIndx[i]]->m_arr[arrIdx];
            RuntimeBinding(hashfun, i)(
                (char*)datum_arr->data, (uint8*)datum_arr->nullFlag, nrows, hashRes, (hashFmgr + i));
        }

        MemoryContextReset(m_memControl.tmpContext);
    }

    template <typename innerType, typename outerType, bool simpleType, bool nulleqnull>
    void matchCheckColT(ScalarVector* val, SonicDatumArray* array, int nrows, int keyNum)
    {
        bool* boolloc = m_match;
        uint16* loc1 = m_selectIndx;
        Datum* data = m_matchKeys;
        uint8* flag = m_nullFlag;

        bool notnullcheck = false;
        bool nullcheck = false;

        array->getArrayAtomIdx(nrows, m_loc, m_arrayIdx);
        array->getDatumFlagArrayWithMatch(nrows, m_arrayIdx, m_matchKeys, m_nullFlag, m_match);

        for (int i = 0; i < nrows; i++) {
            notnullcheck = BOTH_NOT_NULL(val->m_flag[*loc1], m_nullFlag[i]);
            nullcheck = nulleqnull & (uint8)BOTH_NULL((unsigned char)val->m_flag[*loc1], (unsigned char)m_nullFlag[i]);
            if (simpleType)
                *boolloc =
                    *boolloc && (nullcheck || (notnullcheck && ((outerType)val->m_vals[*loc1] == (innerType)(*data))));
            else {
                FunctionCallInfoData fcinfo;
                PGFunction cmpfun = m_eqfunctions[keyNum].fn_addr;
                Datum args[2];
                fcinfo.arg = &args[0];

                fcinfo.arg[0] = val->m_vals[*loc1];
                fcinfo.arg[1] = *data;
                fcinfo.flinfo = (m_eqfunctions + keyNum);
                *boolloc = *boolloc && (nullcheck || (notnullcheck && (bool)cmpfun(&fcinfo)));
            }

            boolloc++;
            loc1++;
            data++;
            flag++;
        }
    }

public:
    typedef void (SonicHash::*hashValFun)(char* val, uint8* flag, int nval, uint32* res, FmgrInfo* hashFmgr);

    typedef void (SonicHash::*matchFun)(ScalarVector* val, SonicDatumArray* array, int nrows, int keyNum);

    typedef void (SonicHash::*profileFun)();

    typedef struct SonicHashInputOpAttr {
        bool simple; /* without encoding */

        uint16* keyIndx;

        uint16* oKeyIndx;

        uint16 keyNum;

        hashValFun* hashFunc;

        hashValFun* hashAtomFunc;

        FmgrInfo* hashFmgr;

        VectorBatch* batch;

        uint16 cols;

        TupleDesc tupleDesc;

        SonicHashInputOpAttr()
        {
            simple = false;
            keyIndx = NULL;
            oKeyIndx = NULL;
            keyNum = 0;
            hashFunc = NULL;
            hashAtomFunc = NULL;
            hashFmgr = NULL;
            batch = NULL;
            cols = 0;
            tupleDesc = NULL;
        }
    } SonicHashInputOpAttr;

    /* the hash data and the desc */
    SonicDatumArray** m_data;
    SonicDatumArray* m_hash;
    char* m_bucket;
    uint8 m_bucketTypeSize;

    int m_atomSize; /* array size for atom structure */
    int64 m_hashSize;
    int64 m_rows;

    /* runtime status and binding function. */
    matchFun* m_matchKey; /* matching functions */
    hashStateLog m_stateLog;
    uint8 m_probeStatus;
    uint8 m_status;
    SonicHashInputOpAttr m_buildOp;
    FmgrInfo* m_eqfunctions; /* equal functions */

    /* memory control.*/
    SonicHashMemoryControl m_memControl;

    /* temporary space for vector processing */
    uint32 m_hashVal[INIT_DATUM_ARRAY_SIZE]; /* temp hash values */
    uint32 m_loc[BatchMaxSize];              /* record position from inner atom */
    uint32 m_partLoc[BatchMaxSize];          /* record partition number */
    uint16 m_selectIndx[BatchMaxSize];       /* record position from outer batch */
    uint16 m_selectRows;                     /* matched tuple number */
    bool m_match[BatchMaxSize];
    Datum m_matchKeys[BatchMaxSize];
    uint8 m_nullFlag[BatchMaxSize];
    ArrayIdx m_arrayIdx[BatchMaxSize]; /* record matched idx */
};

extern uint64 hashfindprime(uint64 n);
extern uint32 hashquickany(uint32 seed, register const unsigned char* data, register int len);
#endif /* SRC_INCLUDE_VECTORSONIC_VSONICHASH_H_ */
