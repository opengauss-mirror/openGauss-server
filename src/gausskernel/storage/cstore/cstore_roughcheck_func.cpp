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
 * cstore_roughcheck_func.cpp
 *      routines to support ColStore
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/cstore/cstore_roughcheck_func.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "access/cstore_roughcheck_func.h"
#include "utils/date.h"
#include "utils/timestamp.h"
#include "catalog/pg_collation.h"

template <int strategy>
bool RoughCheckDateCU(CUDesc* cudesc, Datum arg);
template <int strategy>
bool RoughCheckTimeCU(CUDesc* cudesc, Datum arg);
template <int strategy>
bool RoughCheckTimestampCU(CUDesc* cudesc, Datum arg);
template <int strategy>
bool RoughCheckTimestampTzCU(CUDesc* cudesc, Datum arg);
template <int strategy>
bool RoughCheckCharCU(CUDesc* cudesc, Datum arg);
template <int strategy>
bool RoughCheckInt16CU(CUDesc* cudesc, Datum arg);
template <int strategy>
bool RoughCheckInt32CU(CUDesc* cudesc, Datum arg);
template <int strategy>
bool RoughCheckInt64CU(CUDesc* cudesc, Datum arg);
template <int strategy>
bool RoughCheckUInt32CU(CUDesc* cudesc, Datum arg);
template <int strategy>
bool RoughCheckStringCU(CUDesc* cudesc, Datum arg);
bool RoughCheckAllThrough(CUDesc* cudesc, Datum arg);

template <class T, int strategy>
bool RoughCheckIntCU(const T& min, const T& max, const T& arg);
template <class T, int strategy>
bool RoughCheckFloatCU(const T& min, const T& max, const T& arg);

RoughCheckFunc GetRoughCheckFunc(Oid typeOid, int strategy, Oid collation)
{
    switch (typeOid) {
        case CHAROID: {
            switch (strategy) {
                case CStoreLessStrategyNumber:
                    return RoughCheckCharCU<CStoreLessStrategyNumber>;
                case CStoreLessEqualStrategyNumber:
                    return RoughCheckCharCU<CStoreLessEqualStrategyNumber>;
                case CStoreEqualStrategyNumber:
                    return RoughCheckCharCU<CStoreEqualStrategyNumber>;
                case CStoreGreaterEqualStrategyNumber:
                    return RoughCheckCharCU<CStoreGreaterEqualStrategyNumber>;
                case CStoreGreaterStrategyNumber:
                    return RoughCheckCharCU<CStoreGreaterStrategyNumber>;
                default:
                    return RoughCheckAllThrough;
            }
            break;
        }
        case INT2OID: {
            switch (strategy) {
                case CStoreLessStrategyNumber:
                    return RoughCheckInt16CU<CStoreLessStrategyNumber>;
                case CStoreLessEqualStrategyNumber:
                    return RoughCheckInt16CU<CStoreLessEqualStrategyNumber>;
                case CStoreEqualStrategyNumber:
                    return RoughCheckInt16CU<CStoreEqualStrategyNumber>;
                case CStoreGreaterEqualStrategyNumber:
                    return RoughCheckInt16CU<CStoreGreaterEqualStrategyNumber>;
                case CStoreGreaterStrategyNumber:
                    return RoughCheckInt16CU<CStoreGreaterStrategyNumber>;
                default:
                    return RoughCheckAllThrough;
            }
            break;
        }
        case INT4OID: {
            switch (strategy) {
                case CStoreLessStrategyNumber:
                    return RoughCheckInt32CU<CStoreLessStrategyNumber>;
                case CStoreLessEqualStrategyNumber:
                    return RoughCheckInt32CU<CStoreLessEqualStrategyNumber>;
                case CStoreEqualStrategyNumber:
                    return RoughCheckInt32CU<CStoreEqualStrategyNumber>;
                case CStoreGreaterEqualStrategyNumber:
                    return RoughCheckInt32CU<CStoreGreaterEqualStrategyNumber>;
                case CStoreGreaterStrategyNumber:
                    return RoughCheckInt32CU<CStoreGreaterStrategyNumber>;
                default:
                    return RoughCheckAllThrough;
            }
            break;
        }
        case INT8OID: {
            switch (strategy) {
                case CStoreLessStrategyNumber:
                    return RoughCheckInt64CU<CStoreLessStrategyNumber>;
                case CStoreLessEqualStrategyNumber:
                    return RoughCheckInt64CU<CStoreLessEqualStrategyNumber>;
                case CStoreEqualStrategyNumber:
                    return RoughCheckInt64CU<CStoreEqualStrategyNumber>;
                case CStoreGreaterEqualStrategyNumber:
                    return RoughCheckInt64CU<CStoreGreaterEqualStrategyNumber>;
                case CStoreGreaterStrategyNumber:
                    return RoughCheckInt64CU<CStoreGreaterStrategyNumber>;
                default:
                    return RoughCheckAllThrough;
            }
            break;
        }
        case OIDOID: {
            switch (strategy) {
                case CStoreLessStrategyNumber:
                    return RoughCheckUInt32CU<CStoreLessStrategyNumber>;
                case CStoreLessEqualStrategyNumber:
                    return RoughCheckUInt32CU<CStoreLessEqualStrategyNumber>;
                case CStoreEqualStrategyNumber:
                    return RoughCheckUInt32CU<CStoreEqualStrategyNumber>;
                case CStoreGreaterEqualStrategyNumber:
                    return RoughCheckUInt32CU<CStoreGreaterEqualStrategyNumber>;
                case CStoreGreaterStrategyNumber:
                    return RoughCheckUInt32CU<CStoreGreaterStrategyNumber>;
                default:
                    return RoughCheckAllThrough;
            }
            break;
        }
        case DATEOID: {
            switch (strategy) {
                case CStoreLessStrategyNumber:
                    return RoughCheckDateCU<CStoreLessStrategyNumber>;
                case CStoreLessEqualStrategyNumber:
                    return RoughCheckDateCU<CStoreLessEqualStrategyNumber>;
                case CStoreEqualStrategyNumber:
                    return RoughCheckDateCU<CStoreEqualStrategyNumber>;
                case CStoreGreaterEqualStrategyNumber:
                    return RoughCheckDateCU<CStoreGreaterEqualStrategyNumber>;
                case CStoreGreaterStrategyNumber:
                    return RoughCheckDateCU<CStoreGreaterStrategyNumber>;
                default:
                    return RoughCheckAllThrough;
            }
            break;
        }
        case TIMEOID: {
            switch (strategy) {
                case CStoreLessStrategyNumber:
                    return RoughCheckTimeCU<CStoreLessStrategyNumber>;
                case CStoreLessEqualStrategyNumber:
                    return RoughCheckTimeCU<CStoreLessEqualStrategyNumber>;
                case CStoreEqualStrategyNumber:
                    return RoughCheckTimeCU<CStoreEqualStrategyNumber>;
                case CStoreGreaterEqualStrategyNumber:
                    return RoughCheckTimeCU<CStoreGreaterEqualStrategyNumber>;
                case CStoreGreaterStrategyNumber:
                    return RoughCheckTimeCU<CStoreGreaterStrategyNumber>;
                default:
                    return RoughCheckAllThrough;
            }
            break;
        }
        case TIMESTAMPOID: {
            switch (strategy) {
                case CStoreLessStrategyNumber:
                    return RoughCheckTimestampCU<CStoreLessStrategyNumber>;
                case CStoreLessEqualStrategyNumber:
                    return RoughCheckTimestampCU<CStoreLessEqualStrategyNumber>;
                case CStoreEqualStrategyNumber:
                    return RoughCheckTimestampCU<CStoreEqualStrategyNumber>;
                case CStoreGreaterEqualStrategyNumber:
                    return RoughCheckTimestampCU<CStoreGreaterEqualStrategyNumber>;
                case CStoreGreaterStrategyNumber:
                    return RoughCheckTimestampCU<CStoreGreaterStrategyNumber>;
                default:
                    return RoughCheckAllThrough;
            }
            break;
        }
        case TIMESTAMPTZOID: {
            switch (strategy) {
                case CStoreLessStrategyNumber:
                    return RoughCheckTimestampTzCU<CStoreLessStrategyNumber>;
                case CStoreLessEqualStrategyNumber:
                    return RoughCheckTimestampTzCU<CStoreLessEqualStrategyNumber>;
                case CStoreEqualStrategyNumber:
                    return RoughCheckTimestampTzCU<CStoreEqualStrategyNumber>;
                case CStoreGreaterEqualStrategyNumber:
                    return RoughCheckTimestampTzCU<CStoreGreaterEqualStrategyNumber>;
                case CStoreGreaterStrategyNumber:
                    return RoughCheckTimestampTzCU<CStoreGreaterStrategyNumber>;
                default:
                    return RoughCheckAllThrough;
            }
            break;
        }
        case BPCHAROID:
        case VARCHAROID:
        case TEXTOID: {
            if (collation != C_COLLATION_OID && strategy != CStoreEqualStrategyNumber)
                return RoughCheckAllThrough;

            switch (strategy) {
                case CStoreLessStrategyNumber:
                    return RoughCheckStringCU<CStoreLessStrategyNumber>;
                case CStoreLessEqualStrategyNumber:
                    return RoughCheckStringCU<CStoreLessEqualStrategyNumber>;
                case CStoreEqualStrategyNumber:
                    return RoughCheckStringCU<CStoreEqualStrategyNumber>;
                case CStoreGreaterEqualStrategyNumber:
                    return RoughCheckStringCU<CStoreGreaterEqualStrategyNumber>;
                case CStoreGreaterStrategyNumber:
                    return RoughCheckStringCU<CStoreGreaterStrategyNumber>;
                default:
                    return RoughCheckAllThrough;
            }
            break;
        }
        default: {
            return RoughCheckAllThrough;
        }
    }
}

template <int strategy>
bool RoughCheckCharCU(CUDesc* cudesc, Datum arg)
{
    char min = *(char*)cudesc->cu_min;
    char max = *(char*)cudesc->cu_max;

    return RoughCheckIntCU<char, strategy>(min, max, DatumGetChar(arg));
}

template <int strategy>
bool RoughCheckInt16CU(CUDesc* cudesc, Datum arg)
{
    int16 min = *(int16*)cudesc->cu_min;
    int16 max = *(int16*)cudesc->cu_max;

    return RoughCheckIntCU<int64, strategy>(min, max, DatumGetInt64(arg));
}

template <int strategy>
bool RoughCheckInt32CU(CUDesc* cudesc, Datum arg)
{
    int32 min = *(int32*)cudesc->cu_min;
    int32 max = *(int32*)cudesc->cu_max;

    return RoughCheckIntCU<int64, strategy>(min, max, DatumGetInt64(arg));
}

template <int strategy>
bool RoughCheckInt64CU(CUDesc* cudesc, Datum arg)
{
    int64 min = *(int64*)cudesc->cu_min;
    int64 max = *(int64*)cudesc->cu_max;

    return RoughCheckIntCU<int64, strategy>(min, max, DatumGetInt64(arg));
}

template <int strategy>
bool RoughCheckUInt32CU(CUDesc* cudesc, Datum arg)
{
    uint32 min = *(uint32*)cudesc->cu_min;
    uint32 max = *(uint32*)cudesc->cu_max;

    return RoughCheckIntCU<int64, strategy>(min, max, DatumGetInt64(arg));
}

template <int strategy>
bool RoughCheckDateCU(CUDesc* cudesc, Datum arg)
{
    DateADT min = *(DateADT*)cudesc->cu_min;
    DateADT max = *(DateADT*)cudesc->cu_max;

    return RoughCheckIntCU<DateADT, strategy>(min, max, DatumGetDateADT(arg));
}

template <int strategy>
bool RoughCheckTimeCU(CUDesc* cudesc, Datum arg)
{
    TimeADT min = *(TimeADT*)cudesc->cu_min;
    TimeADT max = *(TimeADT*)cudesc->cu_max;

#ifdef HAVE_INT64_TIMESTAMP
    return RoughCheckIntCU<TimeADT, strategy>(min, max, DatumGetTimeADT(arg));
#else
    // do not support for RoughCheck
    return true;
#endif
}

template <int strategy>
bool RoughCheckTimestampCU(CUDesc* cudesc, Datum arg)
{
    Timestamp min = *(Timestamp*)cudesc->cu_min;
    Timestamp max = *(Timestamp*)cudesc->cu_max;

#ifdef HAVE_INT64_TIMESTAMP
    return RoughCheckIntCU<Timestamp, strategy>(min, max, DatumGetTimestamp(arg));
#else
    // do not support for RoughCheck
    return true;
#endif
}

template <int strategy>
bool RoughCheckTimestampTzCU(CUDesc* cudesc, Datum arg)
{
    TimestampTz min = *(TimestampTz*)cudesc->cu_min;
    TimestampTz max = *(TimestampTz*)cudesc->cu_max;

#ifdef HAVE_INT64_TIMESTAMP
    return RoughCheckIntCU<TimestampTz, strategy>(min, max, DatumGetTimestampTz(arg));
#else
    // do not support for RoughCheck
    return true;
#endif
}

template <class T, int strategy>
bool RoughCheckIntCU(const T& min, const T& max, const T& arg)
{
    bool hitCU = true;

    if (strategy == CStoreLessStrategyNumber) {
        if (min >= arg) {
            hitCU = false;
        }
    } else if (strategy == CStoreLessEqualStrategyNumber) {
        if (min > arg) {
            hitCU = false;
        }
    } else if (strategy == CStoreEqualStrategyNumber) {
        if (arg < min || arg > max) {
            hitCU = false;
        }
    } else if (strategy == CStoreGreaterEqualStrategyNumber) {
        if (max < arg) {
            hitCU = false;
        }
    } else if (strategy == CStoreGreaterStrategyNumber) {
        if (max <= arg) {
            hitCU = false;
        }
    }

    return hitCU;
}

template <int strategy>
bool RoughCheckStringCU(CUDesc* cudesc, Datum arg)
{
    bool hitCU = true;

    char* min = cudesc->cu_min;
    char* max = cudesc->cu_max;

    char* argStr = VARDATA_ANY(DatumGetPointer(arg));
    int argLen = VARSIZE_ANY_EXHDR(arg);

    int minLen = *(char*)min;
    int maxLen = *(char*)max;

    char* minStr = min + 1;
    char* maxStr = max + 1;

    int cmpMinLen = minLen > argLen ? argLen : minLen;
    int cmpMaxLen = maxLen > argLen ? argLen : maxLen;

    switch (strategy) {
        case CStoreLessStrategyNumber:
        case CStoreLessEqualStrategyNumber: {
            if (memcmp(minStr, argStr, cmpMinLen) > 0)
                hitCU = false;
        } break;

        case CStoreEqualStrategyNumber: {
            if (memcmp(minStr, argStr, cmpMinLen) > 0 || memcmp(maxStr, argStr, cmpMaxLen) < 0)
                hitCU = false;
        } break;

        case CStoreGreaterEqualStrategyNumber:
        case CStoreGreaterStrategyNumber: {
            if (memcmp(maxStr, argStr, cmpMaxLen) < 0)
                hitCU = false;
        } break;
        default:
            break;
    }

    return hitCU;
}

/*
 *  In some unsupport type or invaild strategy number, make the CU through the rough check
 */
bool RoughCheckAllThrough(CUDesc* cudesc, Datum arg)
{
    return true;
}
