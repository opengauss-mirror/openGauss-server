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
 * vectorbatch.h
 *     Core data structure definition for vector engine.
 * 
 * 
 * IDENTIFICATION
 *        src/include/vecexecutor/vectorbatch.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef VECTORBATCH_H_
#define VECTORBATCH_H_

#include "postgres.h"
#include "access/tupdesc.h"
#include "lib/stringinfo.h"
#include "catalog/pg_type.h"
// Scalar data type
//
typedef uintptr_t ScalarValue;

#define V_NULL_MASK 0b00000001
#define V_NOTNULL_MASK 0b00000000
// steal bit to identify variable value
#define MASK_VAR 0xC000000000000000ULL
#define MASK_VAR_POINTER 0x0000000000000000ULL
#define MASK_VAR_STORAGE 0x8000000000000000ULL
#define MASK_POINTER 0x0FFFFFFFFFFFFFFFULL

#define VAR_POINTER(val) ((val & MASK_VAR) == MASK_VAR_POINTER)
#define VAR_STORAGE(val) ((val & MASK_VAR) == MASK_VAR_STORAGE)

typedef enum BatchCompressType { BCT_NOCOMP, BCT_LZ4 } BatchCompressType;

inline bool COL_IS_ENCODE(int typeId)
{
    // we search the sys table to find [0,8] attlen data type
    // select typname,typlen from pg_type where typlen >= 0 and typlen <=8;
    switch (typeId) {
        case CHAROID:
        case BOOLOID:
        case INT2OID:
        case INT8OID:
        case INT1OID:
        case INT4OID:
        case FLOAT4OID:
        case FLOAT8OID:
        case CASHOID:
        case DATEOID:
        case TIMEOID:
        case TIMESTAMPOID:
        case TIMESTAMPTZOID:
        case SMALLDATETIMEOID:
        case OIDOID:
        case TIDOID:
        case CIDOID:
        case ABSTIMEOID:
        case RELTIMEOID:
        case ANYOID:
        case VOIDOID:
        case TRIGGEROID:
        case INTERNALOID:
        case OPAQUEOID:
        case ANYELEMENTOID:
        case ANYNONARRAYOID:
        case LANGUAGE_HANDLEROID:
        case REGPROCOID:
        case XIDOID:
        case REGPROCEDUREOID:
        case REGOPEROID:
        case REGOPERATOROID:
        case REGCLASSOID:
        case REGTYPEOID:
        case REGCONFIGOID:
        case REGDICTIONARYOID:
        case ANYENUMOID:
        case FDW_HANDLEROID:
        case HLL_HASHVAL_OID:
        case SMGROID:
            return false;
        default:
            return true;
    }
}

#define BOTH_NOT_NULL(flag1, flag2) (likely(NOT_NULL((flag1) | (flag2))))
#define IS_NULL(flag) (unlikely(((flag)&V_NULL_MASK) == V_NULL_MASK))
#define NOT_NULL(flag) ((((unsigned int)flag) & V_NULL_MASK) == V_NOTNULL_MASK)
#define SET_NULL(flag) ((flag) = (flag) | V_NULL_MASK)
#define BOTH_NULL(flag1, flag2) (IS_NULL((flag1) & (flag2)))

#define SET_NOTNULL(flag) ((flag) = (flag) & (~V_NULL_MASK))
#define BatchIsNull(pBatch) ((pBatch) == NULL || (pBatch)->m_rows == 0)
#define VAR_BUF_SIZE 16384

// Retrieve selection vector guarded by selection usage flag
//
#define SelectionVector(pBatch) ((pBatch)->m_checkSel ? (pBatch)->m_sel : NULL)

#define ShallowCopyVector(targetVector, sourceVector)  \
    ((targetVector).m_rows = (sourceVector).m_rows,    \
        (targetVector).m_vals = (sourceVector).m_vals, \
        (targetVector).m_flag = (sourceVector).m_flag, \
        (targetVector).m_buf = (sourceVector).m_buf)

struct ScalarDesc : public BaseObject {

    // Scalar Value type Oid.
    Oid typeId;

    // atttypmod records type-specific data supplied at table creation time
    //  The value will generally be -1 for types that do not need typmod.
    int4 typeMod;

    // this value means that the value in the scalarvector may be encoded, on storage/pointer to some
    // where else, may be inlined(in which case it is not encode)
    // it can be deduct from typeId,
    bool encoded : 1;

    ScalarDesc()
    {
        typeId = InvalidOid;
        typeMod = -1;
        encoded = false;
    };
};

struct varBuf : public BaseObject {
    char* buf;
    int len;
    int size;
    varBuf* next;
};

class VarBuf : public BaseObject {
public:
    // constructor .deconstructor
    VarBuf(MemoryContext context);

    ~VarBuf();

    // init
    void Init();

    void Init(int bufLen);

    void DeInit(bool needFree = true);

    // reset the buf.
    void Reset();

    // append a binary object
    char* Append(const char* data, int datalen);

    // allocate a space.
    char* Allocate(int datalen);
    // add a var

    FORCE_INLINE
    ScalarValue AddVar(ScalarValue value)
    {
        return PointerGetDatum(Append(DatumGetPointer(value), VARSIZE_ANY(value)));
    }

private:
    // create a buffer;
    varBuf* CreateBuf(int datalen);

    varBuf* m_head;

    varBuf* m_current;

    MemoryContext m_context;

    int m_bufNum;

    int m_bufInitLen;
};

// the core data structure for a column
class ScalarVector : public BaseObject {
    friend class VectorBatch;

public:
    // number of values.
    int m_rows;

    // type desciption information for this scalar value.
    ScalarDesc m_desc;

    // this value means that the value in the scalarvector is always the same
    bool m_const;

    // flags in the scalar value array.
    uint8* m_flag;

    // a company buffer for store the data if the data type is not plain.
    VarBuf* m_buf;

    // the value array.
    ScalarValue* m_vals;

public:
    // decode a variable length data.
    // null value judgement should be outside of this function.
    FORCE_INLINE
    static Datum Decode(ScalarValue val)
    {
        return val;
    }

    // convert a datum to scalar value
    static ScalarValue DatumToScalar(Datum datumVal, Oid datumType, bool isNull);

    template <Oid datumType>
    static ScalarValue DatumToScalarT(Datum datumVal, bool isNull);

public:
    // constructor/deconstructor.
    ScalarVector();
    ~ScalarVector();

    // init the ScalarVector.
    //
    void init(MemoryContext cxt, ScalarDesc desc);
    
    // used in tsdb. init with another ScalarVector object.
    //
    void init(MemoryContext cxt, ScalarVector* vec, const int batchSize);

    // serialize the Scalar vector
    //
    void Serialize(StringInfo buf);

    // serialize the Scalar vector of the particular index
    //
    void Serialize(StringInfo buf, int idx);

    // Deserialize the vector
    //
    char* Deserialize(char* msg, size_t len);

    // Add a variable length data
    // this var may be from
    // cstring, fixed length(> 8) data type, or pg traditional header-contain variable length
    Datum AddVar(Datum data, int index);

    // Add a header-contain variable
    Datum AddVarWithHeader(Datum data);

    // Add a variable without header on a special position. The original variable will be
    // transfered in together with the length of the content. And inside the funtion, the header
    // of the ScalarValue will be added before the actual content according to the data type.
    Datum AddBPCharWithoutHeader(const char* data, int maxLen, int len, int aindex);
    Datum AddVarCharWithoutHeader(const char* data, int len, int aindex);

    // Add a short decimal without header on a special position. The value of decimal
    // will be transfered in by int64 format together with the scale of it. And inside the function,
    // the header will be added and the value will be converted into PG format. Here we only support
    // short decimal which can be stored using int64.
    Datum AddShortNumericWithoutHeader(int64 value, uint8 scale, int aindex);
    Datum AddBigNumericWithoutHeader(int128 value, uint8 scale, int aindex);

    char* AddVars(const char* src, int length);

    // add a normal header-contain val
    Datum AddHeaderVar(Datum data, int index);

    // add a cstring type val
    Datum AddCStringVar(Datum data, int index);

    // add a fixed length val
    template <Size len>
    Datum AddFixLenVar(Datum data, int index);

    // copy a vector
    void copy(ScalarVector* vector, int start_idx, int endIdx);
    void copy(ScalarVector* vector);

    void copyDeep(ScalarVector* vector, int start_idx, int endIdx);
    void copyNth(ScalarVector* vector, int Nth);

    void copy(ScalarVector* vector, const bool* pSel);

    // convert a cstring to Scalar value.
    static Datum DatumCstringToScalar(Datum data, Size len);

    // convert a fixed len datatype to Scalar Value
    static Datum DatumFixLenToScalar(Datum data, Size len);

    FORCE_INLINE
    bool IsNull(int i)
    {
        Assert(i >= 0 && i < m_rows);
        return ((m_flag[i] & V_NULL_MASK) == V_NULL_MASK);
    }

    FORCE_INLINE
    void SetNull(int i)
    {
        Assert(i >= 0 && i < BatchMaxSize);
        m_flag[i] |= V_NULL_MASK;
    }

    FORCE_INLINE
    void SetAllNull()
    {
        for (int i = 0; i < m_rows; i++) {
            SetNull(i);
        }
    }

private:
    // init some function pointer.
    void BindingFp();

    Datum (ScalarVector::*m_addVar)(Datum data, int index);
};

struct SysColContainer : public BaseObject {
    int sysColumns;
    ScalarVector* m_ppColumns;
    uint8 sysColumpMap[9];
};

#define SelectionVector(pBatch) ((pBatch)->m_checkSel ? (pBatch)->m_sel : NULL)

// A batch of vectorize rows
//
class VectorBatch : public BaseObject {
public:
    // number of rows in the batch.
    //
    int m_rows;

    // number of columns in the batch.
    //
    int m_cols;

    // Shall we check the selection vector.
    //
    bool m_checkSel;

    // Selection vector;
    //
    bool* m_sel;

    // ScalarVector
    //
    ScalarVector* m_arr;

    // SysColumns
    //
    SysColContainer* m_sysColumns;

    // Compress buffer
    //
    StringInfo m_pCompressBuf;

public:
    // Many Constructors
    //
    VectorBatch(MemoryContext cxt, TupleDesc desc);

    VectorBatch(MemoryContext cxt, VectorBatch* batch);

    VectorBatch(MemoryContext cxt, ScalarDesc* desc, int ncols);

    // Deconstructor.
    //
    ~VectorBatch();

    // Serialize the particular data index of the batch into the buffer.
    //
    void Serialize(StringInfo buf, int idx);

    // Deserialze the per-row msg into the batch
    //
    void Deserialize(char* msg);

    // Serialize the batch into the buffer without compress.
    //
    void SerializeWithoutCompress(StringInfo buf);

    // Deserialze the msg into the batch without compress.
    //
    void DeserializeWithoutDecompress(char* msg, size_t msglen);

    // Serialize the batch into the buffer with lz4 compress.
    //
    void SerializeWithLZ4Compress(StringInfo buf);

    // Deserialze the compressed msg into the batch with lz4 compress.
    //
    void DeserializeWithLZ4Decompress(char* msg, size_t msglen);

    // Reset
    //
    void Reset(bool resetflag = false);

    void ResetSelection(bool value);

    // Test the batch is valid or not
    //
    bool IsValid();

    void FixRowCount();

    void FixRowCount(int rows);

    // Pack the batch
    //
    void Pack(const bool *sel);

    /* Optimzed Pack function */
    void OptimizePack(const bool* sel, List* CopyVars);

    /* Optimzed Pack function for later read. later read cols and ctid col*/
    void OptimizePackForLateRead(const bool* sel, List* lateVars, int ctidColIdx);

    // SysColumns
    //
    void CreateSysColContainer(MemoryContext cxt, List* sysVarList);
    ScalarVector* GetSysVector(int sysColIdx);
    int GetSysColumnNum();

    template <bool deep, bool add>
    void Copy(VectorBatch* batch, int start_idx = 0, int endIdx = -1);

    void CopyNth(VectorBatch* batchSrc, int Nth);

public:
    /* Pack template function. */
    template <bool copyMatch, bool hasSysCol>
    void PackT(const bool* sel);

    /* Optimize template function. */
    template <bool copyMatch, bool hasSysCol>
    void OptimizePackT(_in_ const bool* sel, _in_ List* CopyVars);

    /* Optimize template function for later read. */
    template <bool copyMatch, bool hasSysCol>
    void OptimizePackTForLateRead(_in_ const bool* sel, _in_ List* lateVars, int ctidColIdx);

private:
    // init the vectorbatch.
    void init(MemoryContext cxt, TupleDesc desc);

    void init(MemoryContext cxt, VectorBatch* batch);

    void init(MemoryContext cxt, ScalarDesc* desc, int ncols);
};

/*
 * @Description: copy batch with specific rows
 * @in batch - current batch to be copyed.
 * @in startIdx - start index at current batch
 * @in endIdx - end index at  current batch
 * @template deep - weather a deep copy
 * @template add - add rows or not
 */
template <bool deep, bool add>
void VectorBatch::Copy(VectorBatch* batch, int start_idx, int endIdx)
{
    int copy_end_idx;
    copy_end_idx = (endIdx == -1) ? batch->m_rows : endIdx;
    for (int i = 0; i < m_cols; i++) {
        if (false == add)
            m_arr[i].m_rows = 0;
        if (deep) {
            m_arr[i].copyDeep(&batch->m_arr[i], start_idx, copy_end_idx);
        } else {
            m_arr[i].copy(&batch->m_arr[i], start_idx, copy_end_idx);
        }
    }

    if (false == add)
        m_rows = 0;
    m_rows += copy_end_idx - start_idx;
}

template <Oid datumType>
inline ScalarValue ScalarVector::DatumToScalarT(Datum datumVal, bool isNull)
{
    ScalarValue val = 0;
    Size datumLen; /* length of the datum */

    DBG_ASSERT(datumType != InvalidOid);

    if (!isNull) {
        if (COL_IS_ENCODE(datumType)) {
            switch (datumType) {
                case MACADDROID:
                    val = DatumFixLenToScalar(datumVal, 6);
                    break;
                case TIMETZOID:
                case TINTERVALOID:
                    val = DatumFixLenToScalar(datumVal, 12);
                    break;
                case INTERVALOID:
                case UUIDOID:
                    val = DatumFixLenToScalar(datumVal, 16);
                    break;
                case NAMEOID:
                    val = DatumFixLenToScalar(datumVal, 64);
                    break;
                case UNKNOWNOID:
                case CSTRINGOID:
                    datumLen = strlen((char*)datumVal);
                    val = DatumCstringToScalar(datumVal, datumLen);
                    break;
                default:
                    val = datumVal;
                    break;
            }
        } else
            val = datumVal;
    }

    return val;
}

extern Datum ExtractAddrType(Datum* val);
extern Datum ExtractFixedType(Datum* val);
extern Datum ExtractVarType(Datum* val);
extern Datum ExtractCstringType(Datum* val);

/*
 * Convert the scalar value of vector batch to the datum of row tuple.
 * @_in_param val: The scalar value to be converted.
 * @return the converted datum to be returned.
 */
typedef Datum (*ScalarToDatum)(ScalarValue);
template <Oid typid>
Datum convertScalarToDatumT(ScalarValue val)
{
    Datum datum = 0;
    switch (typid) {
        case VARCHAROID: {
            datum = ScalarVector::Decode(val);
            break;
        }
        case TIMETZOID: {
            char* result = (char*)(ScalarVector::Decode(val)) + VARHDRSZ_SHORT;
            datum = PointerGetDatum(result);
            break;
        }
        case TIDOID: {
            datum = PointerGetDatum(val);
            break;
        }
        case UNKNOWNOID: {
            Datum tmp = ScalarVector::Decode(val);
            char* result = NULL;
            if (VARATT_IS_1B(tmp)) {
                result = (char*)tmp + VARHDRSZ_SHORT;
            } else {
                result = (char*)tmp + VARHDRSZ;
            }
            datum = PointerGetDatum(result);
            break;
        }
        default: {
            datum = (Datum)val;
            break;
        }
    }
    return datum;
}

#endif /* VECTORBATCH_H_ */
