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
 *
 * -------------------------------------------------------------------------
 * vsonichash.cpp
 *              Routines to handle vector sonic hash nodes.
 *              SonicHash is base class of SonicHashJoin and SonicHashAgg.
 *              This file also contains many hash functions.
 *
 * IDENTIFICATION
 *      Code/src/gausskernel/runtime/vecexecutor/vectorsonic/vsonichash.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "c.h"
#include "catalog/pg_type.h"
#include "storage/compress_kits.h"
#include "vectorsonic/vsonichash.h"
#include "utils/dynahash.h"
#ifdef __aarch64__
#include <arm_acle.h>
#else
#include <nmmintrin.h>
#endif

extern bool anls_opt_is_on(AnalysisOpt dfx_opt);

#define HASH_RANDOM_1 1.0412321
#define HASH_RANDOM_2 1.1131347
#define HASH_RANDOM_3 1.0132677

#ifdef __aarch64__
#define HASH_INT32_CRC(c, k) __crc32cw(c, k)
#else
#define HASH_INT32_CRC(c, k) _mm_crc32_u32(c, k)
#endif

#define HASH_CRC_SEED 0xFFFFFFFF

extern Datum hash_bi_key(Numeric key);

FORCE_INLINE
uint32 hashquickany(uint32 seed, register const unsigned char* data, register int len)
{
    const unsigned char* p = data;
    const unsigned char* pend = p + len;

    uint32 crc = seed;

#ifdef __aarch64__
    while (p + 8 <= pend) {
        crc = (uint32)__crc32d(crc, *((const uint64*)p));
        p += 8;
    }

    /* Process remaining full four bytes if any */
    if (p + 4 <= pend) {
        crc = __crc32cw(crc, *((const unsigned int*)p));
        p += 4;
    }

    /* Process any remaining bytes one at a time. */
    while (p < pend) {
        crc = __crc32cb(crc, *p);
        p++;
    }
#else
    while (p + 8 <= pend) {
        crc = (uint32)_mm_crc32_u64(crc, *((const uint64*)p));
        p += 8;
    }

    /* Process remaining full four bytes if any */
    if (p + 4 <= pend) {
        crc = _mm_crc32_u32(crc, *((const unsigned int*)p));
        p += 4;
    }

    /* Process any remaining bytes one at a time. */
    while (p < pend) {
        crc = _mm_crc32_u8(crc, *p);
        p++;
    }
#endif
    return crc;
}

/*
 * @Description: Looks for a prime number slightly greater than
 * 	input parameter n. The prime is chosen so that it is not
 * 	near any power of 2.
 * 	Note: If input n is near 2^64-1, the intermediate result
 * 	may overflow, the caller must avoid this condition.
 */
uint64 hashfindprime(uint64 n)
{
    uint64 pow2;
    uint64 i;

    n += 100;

    pow2 = 1;
    while (pow2 * 2 < n) {
        pow2 = 2 * pow2;
    }

    if ((double)n < 1.05 * (double)pow2) {
        n = (uint64)((double)n * HASH_RANDOM_1);
    }

    pow2 = 2 * pow2;

    if ((double)n > 0.95 * (double)pow2) {
        n = (uint64)((double)n * HASH_RANDOM_2);
    }

    if (n > pow2 - 20) {
        n += 30;
    }

    /*
     * Now we have n far enough from powers of 2. To make
     * n more random (especially, if it was not near
     * a power of 2), we then multiply it by a random number.
     */
    n = (uint64)((double)n * HASH_RANDOM_3);
    bool getPrime = false;
    while (true) {
        getPrime = true;
        for (i = 2; i * i <= n; i++) {
            if (n % i == 0) {
                /* not prime */
                getPrime = false;
                break;
            }
        }
        if (getPrime) {
            break;
        }
        n++;
    }

    return n;
}

/*
 * @Description: SonicHash constructor.
 * 	Create memory contexts and some general variables.
 */
SonicHash::SonicHash(int size) : m_hash(0), m_atomSize(size), m_rows(0), m_selectRows(0)
{
    m_memControl.tmpContext = AllocSetContextCreate(CurrentMemoryContext,
        "TmpHashContext",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE,
        STANDARD_CONTEXT);

    m_probeStatus = PROBE_FETCH;
    m_stateLog.lastProcessIdx = 0;
    m_stateLog.restore = false;
    m_eqfunctions = NULL;
    m_matchKey = NULL;
    m_data = NULL;
    m_bucket = NULL;
    m_hashSize = 0;
    m_bucketTypeSize = 0;
    m_status = 0;
}

/*
 * @Description: Initialize hash functions.
 * @in desc - relation description.
 * @in hashFun - hashFun needed to be assigned.
 * @in keyIndx - hash key indexes.
 * @in isAtom - if true, need to hash values from atom.
 * 	Otherwise, hash values from VectorBatch.
 */
void SonicHash::initHashFunc(TupleDesc desc, void* hash_fun_in, uint16* key_indx, bool is_atom)
{
    int i;
    Form_pg_attribute* attrs = desc->attrs;
    hashValFun* hashfun = (hashValFun*)hash_fun_in;

    for (i = 0; i < m_buildOp.keyNum; i++) {
        Oid type_oid = attrs[key_indx[i]]->atttypid;
        if (i == 0) {
            if (!integerType(type_oid)) {
                switch (type_oid) {
                    case BPCHAROID:
                        hashfun[i] = &SonicHash::hashbpchar<false>;
                        break;
                    case NUMERICOID:
                        hashfun[i] = &SonicHash::hashnumeric<false>;
                        break;
                    case NVARCHAR2OID:
                    case TEXTOID:
                    case VARCHAROID:
#ifdef PGXC
                        if (g_instance.attr.attr_sql.string_hash_compatible)
                            hashfun[i] = &SonicHash::hashtext_compatible<false>;
                        else
#endif
                            hashfun[i] = &SonicHash::hashtext<false>;
                        break;
                    default:
                        if (is_atom) {
                            switch (attrs[key_indx[i]]->attlen) {
                                case 1:
                                    hashfun[i] = &SonicHash::hashGeneralFunc<false, uint8, uint8>;
                                    break;
                                case 2:
                                    hashfun[i] = &SonicHash::hashGeneralFunc<false, uint16, int16>;
                                    break;
                                case 4:
                                    hashfun[i] = &SonicHash::hashGeneralFunc<false, uint32, int32>;
                                    break;
                                case 8:
                                    hashfun[i] = &SonicHash::hashGeneralFunc<false, uint64, int64>;
                                    break;
                                default:
                                    hashfun[i] = &SonicHash::hashGeneralFunc<false, ScalarValue, ScalarValue>;
                                    break;
                            }
                        } else
                            hashfun[i] = &SonicHash::hashGeneralFunc<false, ScalarValue, ScalarValue>;
                        break;
                }
            } else {
                switch (attrs[key_indx[i]]->attlen) {
                    case 1:
                        if (is_atom)
                            hashfun[i] = &SonicHash::hashInteger<false, uint8, uint8>;
                        else
                            hashfun[i] = &SonicHash::hashInteger<false, ScalarValue, uint8>;
                        break;
                    case 2:
                        if (is_atom)
                            hashfun[i] = &SonicHash::hashInteger<false, uint16, int16>;
                        else
                            hashfun[i] = &SonicHash::hashInteger<false, ScalarValue, int16>;
                        break;
                    case 4:
                        if (is_atom)
                            hashfun[i] = &SonicHash::hashInteger<false, uint32, int32>;
                        else
                            hashfun[i] = &SonicHash::hashInteger<false, ScalarValue, int32>;
                        break;
                    case 8:
                        hashfun[i] = &SonicHash::hashInteger8<false>;
                        break;
                    default:
                        Assert(0);
                        ereport(ERROR,
                            (errmodule(MOD_VEC_EXECUTOR),
                                errcode(ERRCODE_WRONG_OBJECT_TYPE),
                                errmsg("[SonicHash] Unrecognized datetype %u, attrlen %d when init hash functions.",
                                    type_oid,
                                    attrs[key_indx[i]]->attlen)));
                        break;
                }
            }
        } else {
            if (!integerType(attrs[key_indx[i]]->atttypid)) {
                switch (type_oid) {
                    case BPCHAROID:
                        hashfun[i] = &SonicHash::hashbpchar<true>;
                        break;
                    case NUMERICOID:
                        hashfun[i] = &SonicHash::hashnumeric<true>;
                        break;
                    case NVARCHAR2OID:
                    case TEXTOID:
                    case VARCHAROID:
#ifdef PGXC
                        if (g_instance.attr.attr_sql.string_hash_compatible)
                            hashfun[i] = &SonicHash::hashtext_compatible<true>;
                        else
#endif
                            hashfun[i] = &SonicHash::hashtext<true>;
                        break;
                    default:
                        if (is_atom) {
                            switch (attrs[key_indx[i]]->attlen) {
                                case 1:
                                    hashfun[i] = &SonicHash::hashGeneralFunc<true, uint8, uint8>;
                                    break;
                                case 2:
                                    hashfun[i] = &SonicHash::hashGeneralFunc<true, uint16, int16>;
                                    break;
                                case 4:
                                    hashfun[i] = &SonicHash::hashGeneralFunc<true, uint32, int32>;
                                    break;
                                case 8:
                                    hashfun[i] = &SonicHash::hashGeneralFunc<true, uint64, int64>;
                                    break;
                                default:
                                    hashfun[i] = &SonicHash::hashGeneralFunc<true, ScalarValue, ScalarValue>;
                                    break;
                            }
                        } else
                            hashfun[i] = &SonicHash::hashGeneralFunc<true, ScalarValue, ScalarValue>;
                        break;
                }
            } else {
                switch (attrs[key_indx[i]]->attlen) {
                    case 1:
                        if (is_atom)
                            hashfun[i] = &SonicHash::hashInteger<true, uint8, uint8>;
                        else
                            hashfun[i] = &SonicHash::hashInteger<true, ScalarValue, uint8>;
                        break;
                    case 2:
                        if (is_atom)
                            hashfun[i] = &SonicHash::hashInteger<true, uint16, int16>;
                        else
                            hashfun[i] = &SonicHash::hashInteger<true, ScalarValue, int16>;
                        break;
                    case 4:
                        if (is_atom)
                            hashfun[i] = &SonicHash::hashInteger<true, uint32, int32>;
                        else
                            hashfun[i] = &SonicHash::hashInteger<true, ScalarValue, int32>;
                        break;
                    case 8:
                        hashfun[i] = &SonicHash::hashInteger8<true>;
                        break;
                    default:
                        Assert(0);
                        ereport(ERROR,
                            (errmodule(MOD_VEC_EXECUTOR),
                                errcode(ERRCODE_WRONG_OBJECT_TYPE),
                                errmsg("[SonicHash] Unrecognized datetype %u, attrlen %d when init hash functions.",
                                    type_oid,
                                    attrs[key_indx[i]]->attlen)));
                        break;
                }
            }
        }
    }
}

/*
 * @Description: hash int1, int2, int4 values without function calls.
 * @in val - values to compute.
 * @in flag - check flag when compute hash.
 * @in res - store hash values to res.
 * @in func - hash function. For hashInteger, this paramter is not used.
 */
template <bool rehash, typename containerType, typename realType>
void SonicHash::hashInteger(char* val, uint8* flag, int nval, uint32* res, FmgrInfo* hashFmgr)
{
    containerType* arrval = (containerType*)val;
    uint32* res1 = res;

    for (int i = 0; i < nval; i++) {
        if (likely(NOT_NULL(*flag))) {
            if (rehash)
                *res1 = HASH_INT32_CRC(*res1, (uint32)(realType)(*arrval));
            else
                *res1 = HASH_INT32_CRC(HASH_CRC_SEED, (uint32)(realType)(*arrval));
        } else {
            if (!rehash)
                *res1 = 0;
        }
        res1++;
        arrval++;
        flag++;
    }
}

/*
 * @Description: hash int8 values without function calls.
 * @in val - values to compute.
 * @in flag - check flag when compute hash.
 * @in res - store hash values to res.
 * @in func - hash function. For hashInteger8, this paramter is not used.
 */
template <bool rehash>
void SonicHash::hashInteger8(char* val, uint8* flag, int nval, uint32* res, FmgrInfo* hashFmgr)
{
    int64* arrval = (int64*)val;
    uint32* res1 = res;

    for (int i = 0; i < nval; i++) {
        if (likely(NOT_NULL(*flag))) {
            uint32 lohalf = (uint32)(*arrval);
            uint32 hihalf = (uint32)((unsigned int64)(*arrval) >> 32);
            lohalf ^= (*arrval >= 0) ? hihalf : ~hihalf;
            if (rehash)
                *res1 = HASH_INT32_CRC(*res1, lohalf);
            else
                *res1 = HASH_INT32_CRC(HASH_CRC_SEED, lohalf);
        } else {
            if (!rehash)
                *res1 = 0;
        }
        res1++;
        arrval++;
        flag++;
    }
}

/*
 * @Description: hash values with function calls.
 * @in val - values to compute.
 * @in flag - check flag when compute hash.
 * @in res - store hash values to res.
 * @in func - hash function.
 */
template <bool rehash, typename containerType, typename realType>
void SonicHash::hashGeneralFunc(char* val, uint8* flag, int nval, uint32* res, FmgrInfo* hashFmgr)
{
    uint32 hash_val;
    containerType* arrval = (containerType*)val;
    uint32* res1 = res;
    FunctionCallInfoData fcinfo;
    Datum args[2];
    fcinfo.arg = &args[0];
    fcinfo.flinfo = hashFmgr;
    PGFunction func = hashFmgr->fn_addr;

    for (int i = 0; i < nval; i++) {
        if (likely(NOT_NULL(*flag))) {
            fcinfo.arg[0] = *arrval;
            if (rehash) {
                hash_val = *res1;
                hash_val = (hash_val << 1) | ((hash_val & 0x80000000) ? 1 : 0);
                *res1 = hash_val ^ func(&fcinfo);
            } else
                *res1 = func(&fcinfo);
        } else {
            if (!rehash)
                *res1 = 0;
        }
        res1++;
        arrval++;
        flag++;
    }

    /*
     * Rehash the hash value for avoiding the key and
     * distribute key using the same hash function.
     */
    for (int i = 0; i < nval; i++)
        res[i] = hash_uint32(DatumGetUInt32(res[i]));
}

/*
 * @Description: hash bpchar values without function calls.
 * @in val - values to compute.
 * @in flag - check flag when compute hash.
 * @in res - store hash values to res array.
 * @in func - hash function. For hashbpchar, this paramter is not used.
 */
template <bool rehash>
inline void SonicHash::hashbpchar(char* val, uint8* flag, int nval, uint32* res, FmgrInfo* hashFmgr)
{
    Datum* key = (Datum*)val;
    char* key_data = NULL;
    int key_len;
    uint32* res1 = res;

    for (int i = 0; i < nval; i++) {
        if (likely(NOT_NULL(*flag))) {
            key_data = VARDATA_ANY((BpChar*)*key);
            key_len = bcTruelen((BpChar*)*key);

            if (rehash)
                *res1 = hashquickany(*res1, (unsigned char*)key_data, key_len);
            else
                *res1 = hashquickany(HASH_CRC_SEED, (unsigned char*)key_data, key_len);
        } else {
            if (!rehash)
                *res1 = 0;
        }

        key++;
        res1++;
        flag++;
    }
}

/*
 * @Description: hash text, varchar, nvarchar values without function calls.
 * @in val - values to compute.
 * @in flag - check flag when compute hash.
 * @in res - store hash values to res array.
 * @in func - hash function. For hashtext, this paramter is not used.
 */
template <bool rehash>
inline void SonicHash::hashtext(char* val, uint8* flag, int nval, uint32* res, FmgrInfo* hashFmgr)
{
    Datum* key = (Datum*)val;
    char* key_data = NULL;
    int key_len;
    uint32* res1 = res;

    for (int i = 0; i < nval; i++) {
        if (likely(NOT_NULL(*flag))) {
            key_data = VARDATA_ANY((text*)*key);
            key_len = VARSIZE_ANY_EXHDR((text*)*key);

            if (rehash)
                *res1 = hashquickany(*res1, (unsigned char*)key_data, key_len);
            else
                *res1 = hashquickany(HASH_CRC_SEED, (unsigned char*)key_data, key_len);
        } else {
            if (!rehash)
                *res1 = 0;
        }

        key++;
        res1++;
        flag++;
    }
}

/*
 * @Description: hash text, varchar, nvarchar values without function calls.
 * 	The difference between hashtext_compatible and hashtext is the former
 *	one is used when GUC string_hash_compatible is on.
 * @in val - values to compute.
 * @in flag - check flag when compute hash.
 * @in res - store hash values to res array.
 * @in func - hash function. For hashtext, this paramter is not used.
 */
template <bool rehash>
inline void SonicHash::hashtext_compatible(char* val, uint8* flag, int nval, uint32* res, FmgrInfo* hashFmgr)
{
    Datum* key = (Datum*)val;
    char* key_data = NULL;
    int key_len;
    uint32* res1 = res;

    for (int i = 0; i < nval; i++) {
        if (likely(NOT_NULL(*flag))) {
            key_data = VARDATA_ANY((text*)*key);
            key_len = bcTruelen((text*)*key);

            if (rehash)
                *res1 = hashquickany(*res1, (unsigned char*)key_data, key_len);
            else
                *res1 = hashquickany(HASH_CRC_SEED, (unsigned char*)key_data, key_len);
        } else {
            if (!rehash)
                *res1 = 0;
        }

        key++;
        res1++;
        flag++;
    }
}
/*
 * @Description: hash numeric values without function calls.
 * @in val - values to compute.
 * @in flag - check flag when compute hash.
 * @out res - store hash values to res array.
 * @in func - hash function. For hashnumeric, this paramter is not used.
 */
template <bool rehash>
inline void SonicHash::hashnumeric(char* val, uint8* flag, int nval, uint32* res, FmgrInfo* hashFmgr)
{
    uint32 hash_val;
    Datum* key = (Datum*)val;
    uint32* res1 = res;
    for (int i = 0; i < nval; i++) {
        if (likely(NOT_NULL(*flag))) {
            if (rehash) {
                hash_val = *res1;
                hash_val = (hash_val << 1) | ((hash_val & 0x80000000) ? 1 : 0);
                *res1 = hash_val ^ hash_bi_key(DatumGetBINumeric(*key));
            } else
                *res1 = hash_bi_key(DatumGetBINumeric(*key));
        } else {
            if (!rehash)
                *res1 = 0;
        }

        key++;
        res1++;
        flag++;
    }
}

/*
 * @Description: Delete SonicHash memory context.
 */
void SonicHash::freeMemoryContext()
{
    if (m_memControl.hashContext != NULL) {
        /* Delete child context for m_hashContext */
        MemoryContextDelete(m_memControl.hashContext);
        m_memControl.hashContext = NULL;
    }
    if (m_memControl.tmpContext != NULL) {
        /* Delete child context for m_hashContext */
        MemoryContextDelete(m_memControl.tmpContext);
        m_memControl.tmpContext = NULL;
    }
}

/*
 * @Description: Get total nows.
 */
int64 SonicHash::getRows()
{
    return m_rows;
}

/*
 * @Description: Check whether a type is integer.
 * @in typeId - type oid.
 * @return - true is the type is integer.
 */
bool SonicHash::integerType(Oid type_id)
{
    switch (type_id) {
        case INT1OID:
        case INT2OID:
        case INT4OID:
        case INT8OID:
            return true;
            break;
        default:
            break;
    }

    return false;
}

/*
 * @Description: Check whether input hash column can be compressed.
 * 	When attribute oid is the oid listed in this function and
 * 	is hash key, the data can not be compressed,
 * 	use EncodingDatumArray to put data.
 * @in typeOid - column attr type oid need to check.
 * @in attidx - attr index need to check.
 * @in keyIdx - hash key indexes.
 * @in keyNum - total hash column number.
 * @return - true if the input hash column cannot be compressed.
 */
bool SonicHash::isHashKey(Oid type_oid, int attr_idx, uint16* key_idx, int key_num)
{
    switch (type_oid) {
        case BPCHAROID:
        case NUMERICOID:
        /* attlen = 12 */
        case ACLITEMOID:
        case TINTERVALOID:
        case TIMETZOID:
        /* attlen = 16 */
        case INTERVALOID:
        case POINTOID:
            for (int i = 0; i < key_num; i++)
                if (key_idx[i] == attr_idx)
                    return true;
            break;
        default:
            break;
    }
    return false;
}

/*
 * @Description: Replace some equal matching functions.
 */
void SonicHash::replaceEqFunc()
{
    Form_pg_attribute* attrs = m_buildOp.tupleDesc->attrs;
    for (int i = 0; i < m_buildOp.keyNum; i++) {
        switch (attrs[m_buildOp.keyIndx[i]]->atttypid) {
            case TIMETZOID:
                m_eqfunctions[i].fn_addr = timetz_eq_withhead;
                break;
            case TINTERVALOID:
                m_eqfunctions[i].fn_addr = tintervaleq_withhead;
                break;
            case INTERVALOID:
                m_eqfunctions[i].fn_addr = interval_eq_withhead;
                break;
            case NAMEOID:
                m_eqfunctions[i].fn_addr = nameeq_withhead;
                break;
            default:
                break;
        }
    }
}
