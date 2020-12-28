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
 * cstore_minmax_func.cpp
 *      routines to support ColStore
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/cstore/cstore_minmax_func.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "access/cstore_minmax_func.h"
#include "catalog/pg_type.h"
#include "utils/date.h"
#include "utils/timestamp.h"

/* just keep compiler silent */
#define UNUSED_ARG(_arg_) ((void)(_arg_))

/* Min/Max Option Function info */
struct FuncSetMinMaxInfo {
    /* data-type OID */
    Oid typeOid;

    /* Min/Max Set functions
     * Notice: we retain this member to avoid modifying many codes.
     */
    FuncSetMinMax set_minmax_func;

    /* substitute compare_datum_func for set_minmax_func !
     * compare datum and set min/max info.
     */
    CompareDatum compare_datum_func;
    FinishCompareDatum finish_compare_datum_func;
};

/* two offsets for var-length string
 * 1. offset of datum info (excluding var-header), 1B, its value is 1 or 4.
 * 2. offset of length info, 1B
 */
static int const varstr_dat_offset = sizeof(Datum) + 1;
static int const varstr_dat_length = sizeof(Datum) + 2;

static void SetMinMaxDate(Datum v, CUDesc* cuDescPtr, bool* flag);
static void SetMinMaxTime(Datum v, CUDesc* cuDescPtr, bool* flag);
static void SetMinMaxTimestamp(Datum v, CUDesc* cuDescPtr, bool* flag);
static void SetMinMaxTimestamptz(Datum v, CUDesc* cuDescPtr, bool* flag);
static void SetMinMaxInt64(Datum v, CUDesc* cuDescPtr, bool* flag);
static void SetMinMaxUint64(Datum v, CUDesc* cuDescPtr, bool* flag);
static void SetMinMaxInt32(Datum v, CUDesc* cuDescPtr, bool* flag);
static void SetMinMaxUint32(Datum v, CUDesc* cuDescPtr, bool* flag);
static void SetMinMaxInt16(Datum v, CUDesc* cuDescPtr, bool* flag);
static void SetMinMaxChar(Datum v, CUDesc* cuDescPtr, bool* flag);
static void SetMinMaxVarStrType(Datum v, CUDesc* cuDescPtr, bool* flag);

static void CompareDummy(char* minval, char* maxval, Datum v, bool* first, int* varstr_maxlen);
static void CompareDate(char* minval, char* maxval, Datum v, bool* first, int* varstr_maxlen);
static void CompareTime(char* minval, char* maxval, Datum v, bool* first, int* varstr_maxlen);
static void CompareTimestamp(char* minval, char* maxval, Datum v, bool* first, int* varstr_maxlen);
static void CompareTimestamptz(char* minval, char* maxval, Datum v, bool* first, int* varstr_maxlen);
static void CompareInt64(char* minval, char* maxval, Datum v, bool* first, int* varstr_maxlen);
static void CompareUint64(char* minval, char* maxval, Datum v, bool* first, int* varstr_maxlen);
static void CompareInt32(char* minval, char* maxval, Datum v, bool* first, int* varstr_maxlen);
static void CompareUint32(char* minval, char* maxval, Datum v, bool* first, int* varstr_maxlen);
static void CompareInt16(char* minval, char* maxval, Datum v, bool* first, int* varstr_maxlen);
static void CompareChar(char* minval, char* maxval, Datum v, bool* first, int* varstr_maxlen);
static void CompareVarStrType(char* minval, char* maxval, Datum v, bool* first, int* varstr_maxlen);

/*
 * @Description: dummy function for finishing comparison
 * @OUT cuDescPtr: CU Description
 * @IN maxval: max value info
 * @IN minval: min value info
 * @See also:
 */
static void FinishCompareDummy(const char* minval, const char* maxval, CUDesc* cuDescPtr)
{
    /* just keep compiler silent */
    UNUSED_ARG(minval);
    UNUSED_ARG(maxval);
    UNUSED_ARG(cuDescPtr);
    return;
}

/*
 * @Description: set CUDesc's min/max value for fixed length data-type
 * @OUT cuDescPtr: CU Description info.
 * @IN maxval: max value info
 * @IN minval: min value info
 * @See also:
 */
static void FinishCompareFixedLength(const char* minval, const char* maxval, CUDesc* cuDescPtr)
{
    errno_t rc = EOK;
    rc = memcpy_s(cuDescPtr->cu_min, MIN_MAX_LEN, minval, MIN_MAX_LEN);
    securec_check(rc, "\0", "\0");
    rc = memcpy_s(cuDescPtr->cu_max, MIN_MAX_LEN, maxval, MIN_MAX_LEN);
    securec_check(rc, "\0", "\0");
}

/*
 * @Description: set CUDesc's min/max value for var length data-type
 * @OUT cuDescPtr: CU Description info
 * @IN maxval: max value info
 * @IN minval: min value info
 * @See also:
 */
static void FinishCompareVarStrType(const char* minval, const char* maxval, CUDesc* cuDescPtr)
{
    char* dat = NULL;
    errno_t rc = EOK;

    /* if all values are NULLs, data point is 0. */
    if (*(Datum*)minval != 0) {
        Assert(minval[varstr_dat_length] < MIN_MAX_LEN);
        Assert(minval[varstr_dat_offset] == 1 || minval[varstr_dat_offset] == 4);

        /* remember the length of min value */
        cuDescPtr->cu_min[0] = minval[varstr_dat_length];
        dat = DatumGetPointer(*(Datum*)minval) + minval[varstr_dat_offset];
        /* remember the min value */
        rc = memcpy_s(cuDescPtr->cu_min + 1, (MIN_MAX_LEN - 1), dat, minval[varstr_dat_length]);
        securec_check(rc, "\0", "\0");

        Assert(maxval[varstr_dat_length] < MIN_MAX_LEN);
        Assert(maxval[varstr_dat_offset] == 1 || maxval[varstr_dat_offset] == 4);

        /* remember the length of max value */
        cuDescPtr->cu_max[0] = maxval[varstr_dat_length];
        dat = DatumGetPointer(*(Datum*)maxval) + maxval[varstr_dat_offset];
        /* remember the max value */
        rc = memcpy_s(cuDescPtr->cu_max + 1, (MIN_MAX_LEN - 1), dat, maxval[varstr_dat_length]);
        securec_check(rc, "\0", "\0");
    } else {
        /* cuDescPtr->cu_min/cu_max have been set to 0, so nothing to do */
    }
}

static FuncSetMinMaxInfo g_FuncTabSetMinMax[] = {
    {   BOOLOID, /* OID = 16 bool */
        NULL,
        CompareDummy,
        FinishCompareDummy
    },
    {   BYTEAOID, /* OID = 17 bytea */
        SetMinMaxVarStrType,
        CompareVarStrType,
        FinishCompareVarStrType
    },
    {   CHAROID, /* OID = 18 char */
        SetMinMaxChar,
        CompareChar,
        FinishCompareFixedLength
    },
    {   NAMEOID, /* OID = 19 name */
        NULL,
        CompareDummy,
        FinishCompareDummy
    },
    {   INT8OID, /* OID = 20 int8 */
        SetMinMaxInt64,
        CompareInt64,
        FinishCompareFixedLength
    },
    {   INT2OID, /* OID = 21 int2 */
        SetMinMaxInt16,
        CompareInt16,
        FinishCompareFixedLength
    },
    {   INT4OID, /* OID = 23 int4 */
        SetMinMaxInt32,
        CompareInt32,
        FinishCompareFixedLength
    },
    {   TEXTOID, /* OID = 25 text */
        SetMinMaxVarStrType,
        CompareVarStrType,
        FinishCompareVarStrType
    },
    {   OIDOID, /* OID = 26 oid */
        SetMinMaxUint32,
        CompareUint32,
        FinishCompareFixedLength
    },
    {   TIDOID, /* OID = 27 tid */
        SetMinMaxUint64,
        CompareUint64,
        FinishCompareFixedLength
    },
    {   BPCHAROID, /* OID = 1042 bpchar */
        SetMinMaxVarStrType,
        CompareVarStrType,
        FinishCompareVarStrType
    },
    {   VARCHAROID, /* OID = 1043 varchar */
        SetMinMaxVarStrType,
        CompareVarStrType,
        FinishCompareVarStrType
    },
    {   DATEOID, /* OID = 1082 date */
        SetMinMaxDate,
        CompareDate,
        FinishCompareFixedLength
    },
    {   TIMEOID, /* OID = 1083 time */
        SetMinMaxTime,
        CompareTime,
        FinishCompareFixedLength
    },
    {   TIMESTAMPOID, /* OID = 1114 timestamp */
        SetMinMaxTimestamp,
        CompareTimestamp,
        FinishCompareFixedLength
    },
    {   TIMESTAMPTZOID, /* OID = 1184 timestamptz */
        SetMinMaxTimestamptz,
        CompareTimestamptz,
        FinishCompareFixedLength
    },
    {   NUMERICOID, /* OID = 1700 numeric */
        NULL,
        CompareDummy,
        FinishCompareDummy
    },
    {   INT1OID, /* OID = 5545 int1 */
        SetMinMaxChar,
        CompareChar,
        FinishCompareFixedLength
    }
};

const int FuncSetMinMaxTabSize = sizeof(g_FuncTabSetMinMax) / sizeof(FuncSetMinMaxInfo);

/* Search and return the index of type OID given */
static int BinarySearch(Oid typeOid)
{
    int left = 0;
    int mid = -1;
    int right = FuncSetMinMaxTabSize - 1;

    while (left <= right) {
        mid = left + ((right - left) / 2);

        if (g_FuncTabSetMinMax[mid].typeOid > typeOid)
            right = mid - 1;
        else if (g_FuncTabSetMinMax[mid].typeOid < typeOid)
            left = mid + 1;
        else
            return mid;
    }

    return FuncSetMinMaxTabSize;
}

/*
 * @Description: find min/max setting function
 * @IN typeOid: data-type oid
 * @Return: function about min/max setting
 * @See also:
 */
FuncSetMinMax GetMinMaxFunc(Oid typeOid)
{
    int idx = BinarySearch(typeOid);
    return (idx < FuncSetMinMaxTabSize) ? g_FuncTabSetMinMax[idx].set_minmax_func : NULL;
}

/*
 * @Description: find datum comparing function
 * @IN typeOid: data-type oid
 * @Return: function about datum comparing
 * @See also:
 */
CompareDatum GetCompareDatumFunc(Oid typeOid)
{
    int idx = BinarySearch(typeOid);
    return (idx < FuncSetMinMaxTabSize) ? g_FuncTabSetMinMax[idx].compare_datum_func : CompareDummy;
}

/*
 * @Description: find datum comparing finishing function
 * @IN typeOid: data-type oid
 * @Return: finishing function about datum comparing
 * @See also:
 */
FinishCompareDatum GetFinishCompareDatum(Oid typeOid)
{
    int idx = BinarySearch(typeOid);
    return (idx < FuncSetMinMaxTabSize) ? g_FuncTabSetMinMax[idx].finish_compare_datum_func : FinishCompareDummy;
}

/*
 * @Description: check whether f is a dummy function
 * @IN f: datum comparing function
 * @Return: true if f is a dummy function; otherwise false.
 * @See also:
 */
bool IsCompareDatumDummyFunc(CompareDatum f)
{
    return (CompareDummy == f);
}

/*
 * @Description: dummy comparing function
 * @IN/OUT first: indicate the first value
 * @IN/OUT maxval: the newest max value
 * @IN/OUT minval: the newest min value
 * @IN v: the new value
 * @IN/OUT varstr_maxlen: max length about var-length string
 * @See also:
 */
static void CompareDummy(char* minval, char* maxval, Datum v, bool* first, int* varstr_maxlen)
{
    UNUSED_ARG(minval);
    UNUSED_ARG(maxval);
    UNUSED_ARG(v);
    UNUSED_ARG(first);
    UNUSED_ARG(varstr_maxlen);
    return;
}

/*
 * @Description: comparing function for data-type int8
 * @IN/OUT first: indicate the first value
 * @IN/OUT maxval: the newest max value
 * @IN/OUT minval: the newest min value
 * @IN v: the new int8 value
 * @IN/OUT varstr_maxlen: max length about var-length string
 * @See also:
 */
static void CompareChar(char* minval, char* maxval, Datum v, bool* first, int* varstr_maxlen)
{
    char chVal = DatumGetChar(v);

    if (!*first) {
        if (chVal < minval[0]) {
            minval[0] = chVal;
        } else if (chVal > maxval[0]) {
            maxval[0] = chVal;
        }
    } else {
        minval[0] = maxval[0] = chVal;
        *first = false;
    }
    UNUSED_ARG(varstr_maxlen);
}

/*
 * @Description: set min/max for data-type int8
 * @IN/OUT cuDescPtr: CU Descriptor
 * @IN/OUT first: the first flag
 * @IN v: the new int8 value
 * @See also:
 */
static void SetMinMaxChar(Datum v, CUDesc* cuDescPtr, bool* first)
{
    CompareChar(cuDescPtr->cu_min, cuDescPtr->cu_max, v, first, NULL);
}

/*
 * @Description: comparing function for data-type var-length string
 * @IN/OUT first: indicate the first value
 * @IN/OUT maxval: the newest max value
 * @IN/OUT minval: the newest min value
 * @IN v: the new var-length string value
 * @IN/OUT varstr_maxlen: max length about var-length string
 * @See also:
 *
 *  minval/maxval struct:
 *    8B --> Datum, pointer to min/max value
 *    1B --> Offset to Var-Data, or Var-Head size
 *    1B --> Compare Data Size
 */
static void CompareVarStrType(char* minval, char* maxval, Datum v, bool* first, int* varstr_maxlen)
{
    char* v_hdr = DatumGetPointer(v);
    char* v_dat = VARDATA_ANY(v_hdr);
    int v_total_len = VARSIZE_ANY(v_hdr);
    int v_exchdr_len = v_total_len - (v_dat - v_hdr);

    int copy_size = Min(v_exchdr_len, (MIN_MAX_LEN - 1));
    int cmp_size = 0;
    int ret = 0;

    Assert(v != (Datum)0);
    if (!*first) {
        /* Update Var-String max length info */
        if (v_total_len > *varstr_maxlen) {
            *varstr_maxlen = v_total_len;
        }

        Assert(minval[varstr_dat_length] < MIN_MAX_LEN);
        Assert(minval[varstr_dat_offset] == 1 || minval[varstr_dat_offset] == 4);

        char* curmin_dat = DatumGetPointer(*(Datum*)minval) + minval[varstr_dat_offset];
        cmp_size = Min(copy_size, minval[varstr_dat_length]);
        ret = memcmp(v_dat, curmin_dat, cmp_size);
        if (ret < 0 || (ret == 0 && copy_size < minval[varstr_dat_length])) {
            /* Update min value, and remember its pointer */
            *(Datum*)minval = v;
            /* Remember its data offset */
            minval[varstr_dat_offset] = (char)(v_dat - v_hdr);
            /* Remember its data length */
            minval[varstr_dat_length] = copy_size;
            return;
        }

        Assert(maxval[varstr_dat_length] < MIN_MAX_LEN);
        Assert(maxval[varstr_dat_offset] == 1 || maxval[varstr_dat_offset] == 4);

        char* curmax_dat = DatumGetPointer(*(Datum*)maxval) + maxval[varstr_dat_offset];
        cmp_size = Min(copy_size, maxval[varstr_dat_length]);
        ret = memcmp(v_dat, curmax_dat, cmp_size);
        if (ret > 0 || (ret == 0 && copy_size > maxval[varstr_dat_length])) {
            /* Update max value, and remember its pointer */
            *(Datum*)maxval = v;
            /* Remember its data offset */
            maxval[varstr_dat_offset] = (char)(v_dat - v_hdr);
            /* Remember its data length */
            maxval[varstr_dat_length] = copy_size;
        }
    } else {
        /* Init Var-String max length info */
        *varstr_maxlen = v_total_len;

        /* Remember its pointer */
        *(Datum*)minval = *(Datum*)maxval = v;
        /* Remember its data offset */
        minval[varstr_dat_offset] = maxval[varstr_dat_offset] = (char)(v_dat - v_hdr);
        /* Remember its data length */
        minval[varstr_dat_length] = maxval[varstr_dat_length] = copy_size;

        *first = false;
    }
}

/*
 * @Description: set min/max for data-type var-length string
 * @IN/OUT first: the first flag
 * @IN/OUT maxval: the newest max value
 * @IN/OUT minval: the newest min value
 * @IN v: the new var-length string value
 * @See also:
 */
static void SetMinMaxVarStrTypeInternal(char* minval, char* maxval, Datum v, bool* first)
{
    char* v_hdr = DatumGetPointer(v);
    char* v_dat = VARDATA_ANY(v_hdr);
    char* b = minval;
    int copySize = 0;
    int v_total_len = VARSIZE_ANY(v_hdr);
    int v_exchdr_len = v_total_len - (v_dat - v_hdr);
    errno_t rc = EOK;

    copySize = Min(v_exchdr_len, (MIN_MAX_LEN - 1));

    if (!*first) {
        Assert(minval[0] < MIN_MAX_LEN && maxval[0] < MIN_MAX_LEN);

        int cmpSize = Min(copySize, minval[0]);
        int ret = memcmp(v_dat, b + 1, cmpSize);
        if (ret < 0 || (ret == 0 && copySize < minval[0])) {
            minval[0] = copySize;
            rc = memcpy_s(minval + 1, (MIN_MAX_LEN - 1), v_dat, copySize);
            securec_check(rc, "\0", "\0");
            return;
        }

        b = maxval;

        cmpSize = Min(copySize, maxval[0]);
        ret = memcmp(v_dat, b + 1, cmpSize);
        if (ret > 0 || (ret == 0 && copySize > maxval[0])) {
            maxval[0] = copySize;
            rc = memcpy_s(maxval + 1, (MIN_MAX_LEN - 1), v_dat, copySize);
            securec_check(rc, "\0", "\0");
        }
    } else {
        maxval[0] = minval[0] = copySize;
        rc = memcpy_s(maxval + 1, (MIN_MAX_LEN - 1), v_dat, copySize);
        securec_check(rc, "\0", "\0");
        rc = memcpy_s(minval + 1, (MIN_MAX_LEN - 1), v_dat, copySize);
        securec_check(rc, "\0", "\0");
        *first = false;
    }
}

/*
 * @Description: set min/max for data-type var-length string
 * @IN/OUT cuDescPtr: CU Descriptor
 * @IN/OUT first: the first flag
 * @IN v: the new var-length string value
 * @See also:
 */
static void SetMinMaxVarStrType(Datum v, CUDesc* cuDescPtr, bool* first)
{
    SetMinMaxVarStrTypeInternal(cuDescPtr->cu_min, cuDescPtr->cu_max, v, first);
}

/*
 * @Description: comparing function for data-type date
 * @IN/OUT first: indicate the first value
 * @IN/OUT maxval: the newest max value
 * @IN/OUT minval: the newest min value
 * @IN v: the new date value
 * @IN/OUT varstr_maxlen: max length about var-length string
 * @See also:
 */
static void CompareDate(char* minval, char* maxval, Datum v, bool* first, int* varstr_maxlen)
{
    if (!*first) {
        DateADT a = DatumGetDateADT(v);
        if (a < *(DateADT*)(minval)) {
            *((DateADT*)(minval)) = a;
        } else if (a > *(DateADT*)(maxval)) {
            *((DateADT*)(maxval)) = a;
        }
    } else {
        *((DateADT*)(minval)) = *((DateADT*)(maxval)) = DatumGetDateADT(v);
        *first = false;
    }
    UNUSED_ARG(varstr_maxlen);
}

/*
 * @Description: set min/max for data-type date
 * @IN/OUT cuDescPtr: CU Descriptor
 * @IN/OUT first: the first flag
 * @IN v: the new date value
 * @See also:
 */
static void SetMinMaxDate(Datum v, CUDesc* cuDescPtr, bool* first)
{
    CompareDate(cuDescPtr->cu_min, cuDescPtr->cu_max, v, first, NULL);
}

/*
 * @Description: comparing function for data-type time
 * @IN/OUT first: indicate the first value
 * @IN/OUT maxval: the newest max value
 * @IN/OUT minval: the newest min value
 * @IN v: the new time value
 * @IN/OUT varstr_maxlen: max length about var-length string
 * @See also:
 */
static void CompareTime(char* minval, char* maxval, Datum v, bool* first, int* varstr_maxlen)
{
    if (!*first) {
        TimeADT tVal = DatumGetTimeADT(v);
        TimeADT curMin = *(TimeADT*)(minval);
        TimeADT curMax = *(TimeADT*)(maxval);

#ifdef HAVE_INT64_TIMESTAMP
        if (tVal < curMin) {
            *((TimeADT*)(minval)) = tVal;
        } else if (tVal > curMax) {
            *((TimeADT*)(maxval)) = tVal;
        }
#else
        /* the rules of comparing are
         * 1. NAN > non-NAN
         * 2. NAN == NAN
         * 3. the ordinary comparing rules
         */
        if (isnan(curMin) || (!isnan(tVal) && (tVal < curMin)))
            *((TimeADT*)(minval)) = tVal;

        /* Never use ELSE branch so update min/max value separately */
        if (isnan(tVal) || (!isnan(curMax) && (tVal > curMax)))
            *((TimeADT*)(maxval)) = tVal;
#endif
    } else {
        *((TimeADT*)(minval)) = *((TimeADT*)(maxval)) = DatumGetTimeADT(v);
        *first = false;
    }
    UNUSED_ARG(varstr_maxlen);
}

/*
 * @Description: set min/max for data-type time
 * @IN/OUT cuDescPtr: CU Descriptor
 * @IN/OUT first: the first flag
 * @IN v: the new time value
 * @See also:
 */
static void SetMinMaxTime(Datum v, CUDesc* cuDescPtr, bool* first)
{
    CompareTime(cuDescPtr->cu_min, cuDescPtr->cu_max, v, first, NULL);
}

/*
 * @Description: comparing function for data-type timestamp
 * @IN/OUT first: indicate the first value
 * @IN/OUT maxval: the newest max value
 * @IN/OUT minval: the newest min value
 * @IN v: the new timestamp value
 * @IN/OUT varstr_maxlen: max length about var-length string
 * @See also: timestamp_cmp_internal()
 */
static void CompareTimestamp(char* minval, char* maxval, Datum v, bool* first, int* varstr_maxlen)
{
    if (!*first) {
        Timestamp tVal = DatumGetTimestamp(v);
        Timestamp curMin = *(Timestamp*)(minval);
        Timestamp curMax = *(Timestamp*)(maxval);

#ifdef HAVE_INT64_TIMESTAMP
        if (tVal < curMin) {
            *((Timestamp*)(minval)) = tVal;
        } else if (tVal > curMax) {
            *((Timestamp*)(maxval)) = tVal;
        }
#else
        /* the rules of comparing are
         * 1. NAN > non-NAN
         * 2. NAN == NAN
         * 3. the ordinary comparing rules
         */
        if (isnan(curMin) || (!isnan(tVal) && (tVal < curMin)))
            *((Timestamp*)(minval)) = tVal;

        /* Never use ELSE branch so update min/max value separately */
        if (isnan(tVal) || (!isnan(curMax) && (tVal > curMax)))
            *((Timestamp*)(maxval)) = tVal;
#endif
    } else {
        *((Timestamp*)(minval)) = *((Timestamp*)(maxval)) = DatumGetTimestamp(v);
        *first = false;
    }
    UNUSED_ARG(varstr_maxlen);
}

/*
 * @Description: set min/max for data-type timestamp
 * @IN/OUT cuDescPtr: CU Descriptor
 * @IN/OUT first: the first flag
 * @IN v: the new timestamp value
 * @See also:
 */
static void SetMinMaxTimestamp(Datum v, CUDesc* cuDescPtr, bool* first)
{
    CompareTimestamp(cuDescPtr->cu_min, cuDescPtr->cu_max, v, first, NULL);
}

/*
 * @Description: comparing function for data-type timestamp with time zone
 * @IN/OUT first: indicate the first value
 * @IN/OUT maxval: the newest max value
 * @IN/OUT minval: the newest min value
 * @IN v: the new timestamp with time zone value
 * @IN/OUT varstr_maxlen: max length about var-length string
 * @See also: timestamp_cmp_internal()
 */
static void CompareTimestamptz(char* minval, char* maxval, Datum v, bool* first, int* varstr_maxlen)
{
    if (!*first) {
        TimestampTz tVal = DatumGetTimestampTz(v);
        TimestampTz curMin = *(TimestampTz*)(minval);
        TimestampTz curMax = *(TimestampTz*)(maxval);

#ifdef HAVE_INT64_TIMESTAMP
        if (tVal < curMin) {
            *((TimestampTz*)(minval)) = tVal;
        } else if (tVal > curMax) {
            *((TimestampTz*)(maxval)) = tVal;
        }
#else
        /* the rules of comparing are
         * 1. NAN > non-NAN
         * 2. NAN == NAN
         * 3. the ordinary comparing rules
         */
        if (isnan(curMin) || (!isnan(tVal) && (tVal < curMin)))
            *((TimestampTz*)(minval)) = tVal;

        /* Never use ELSE branch so update min/max value separately */
        if (isnan(tVal) || (!isnan(curMax) && (tVal > curMax)))
            *((TimestampTz*)(maxval)) = tVal;
#endif
    } else {
        *((TimestampTz*)(minval)) = *((TimestampTz*)(maxval)) = DatumGetTimestampTz(v);
        *first = false;
    }
    UNUSED_ARG(varstr_maxlen);
}

/*
 * @Description: set min/max for data-type timestamp with time zone
 * @IN/OUT cuDescPtr: CU Descriptor
 * @IN/OUT first: the first flag
 * @IN v: the new timestamp with time zone value
 * @See also:
 */
static void SetMinMaxTimestamptz(Datum v, CUDesc* cuDescPtr, bool* first)
{
    CompareTimestamptz(cuDescPtr->cu_min, cuDescPtr->cu_max, v, first, NULL);
}

static void CompareUint64(char* minval, char* maxval, Datum v, bool* first, int* varstr_maxlen)
{
    if (!*first) {
        uint64 intVal = (uint64)v;
        if (intVal < *(uint64*)(minval)) {
            *(uint64*)(minval) = intVal;
        } else if (intVal > *(uint64*)(maxval)) {
            *(uint64*)(maxval) = intVal;
        }
    } else {
        *(uint64*)(minval) = *(uint64*)(maxval) = (uint64)(v);
        *first = false;
    }
    UNUSED_ARG(varstr_maxlen);
}

/*
 * @Description: set min/max for data-type uint64
 * @IN/OUT cuDescPtr: CU Descriptor
 * @IN/OUT first: the first flag
 * @IN v: the new uint64 value
 * @See also:
 */
static void SetMinMaxUint64(Datum v, CUDesc* cuDescPtr, bool* flag)
{
    CompareUint64(cuDescPtr->cu_min, cuDescPtr->cu_max, v, flag, NULL);
}

/*
 * @Description: comparing function for data-type int64
 * @IN/OUT first: indicate the first value
 * @IN/OUT maxval: the newest max value
 * @IN/OUT minval: the newest min value
 * @IN v: the new int64 value
 * @IN/OUT varstr_maxlen: max length about var-length string
 * @See also:
 */
static void CompareInt64(char* minval, char* maxval, Datum v, bool* first, int* varstr_maxlen)
{
    if (!*first) {
        int64 intVal = DatumGetInt64(v);
        if (intVal < *(int64*)(minval)) {
            *(int64*)(minval) = intVal;
        } else if (intVal > *(int64*)(maxval)) {
            *(int64*)(maxval) = intVal;
        }
    } else {
        *(int64*)(minval) = *(int64*)(maxval) = DatumGetInt64(v);
        *first = false;
    }
    UNUSED_ARG(varstr_maxlen);
}

/*
 * @Description: set min/max for data-type int64
 * @IN/OUT cuDescPtr: CU Descriptor
 * @IN/OUT first: the first flag
 * @IN v: the new int64 value
 * @See also:
 */
static void SetMinMaxInt64(Datum v, CUDesc* cuDescPtr, bool* first)
{
    CompareInt64(cuDescPtr->cu_min, cuDescPtr->cu_max, v, first, NULL);
}

/*
 * @Description: comparing function for data-type int32
 * @IN/OUT first: indicate the first value
 * @IN/OUT maxval: the newest max value
 * @IN/OUT minval: the newest min value
 * @IN v: the new int32 value
 * @IN/OUT varstr_maxlen: max length about var-length string
 * @See also:
 */
static void CompareInt32(char* minval, char* maxval, Datum v, bool* first, int* varstr_maxlen)
{
    if (!*first) {
        int32 intVal = DatumGetInt32(v);
        if (intVal < *(int32*)(minval)) {
            *(int32*)(minval) = intVal;
        } else if (intVal > *(int32*)(maxval)) {
            *(int32*)(maxval) = intVal;
        }
    } else {
        *(int32*)(minval) = *(int32*)(maxval) = DatumGetInt32(v);
        *first = false;
    }
    UNUSED_ARG(varstr_maxlen);
}

/*
 * @Description: set min/max for data-type int32
 * @IN/OUT cuDescPtr: CU Descriptor
 * @IN/OUT first: the first flag
 * @IN v: the new int32 value
 * @See also:
 */
static void SetMinMaxInt32(Datum v, CUDesc* cuDescPtr, bool* first)
{
    CompareInt32(cuDescPtr->cu_min, cuDescPtr->cu_max, v, first, NULL);
}

/*
 * @Description: comparing function for data-type uint32
 * @IN/OUT first: indicate the first value
 * @IN/OUT maxval: the newest max value
 * @IN/OUT minval: the newest min value
 * @IN v: the new uint32 value
 * @IN/OUT varstr_maxlen: max length about var-length string
 * @See also:
 */
static void CompareUint32(char* minval, char* maxval, Datum v, bool* first, int* varstr_maxlen)
{
    if (!*first) {
        uint32 uintVal = DatumGetUInt32(v);
        if (uintVal < *(uint32*)(minval)) {
            *(uint32*)(minval) = uintVal;
        } else if (uintVal > *(uint32*)(maxval)) {
            *(uint32*)(maxval) = uintVal;
        }
    } else {
        *(uint32*)(minval) = *(uint32*)(maxval) = DatumGetUInt32(v);
        *first = false;
    }
    UNUSED_ARG(varstr_maxlen);
}

/*
 * @Description: set min/max for data-type uint32
 * @IN/OUT cuDescPtr: CU Descriptor
 * @IN/OUT first: the first flag
 * @IN v: the new uint32 value
 * @See also:
 */
void SetMinMaxUint32(Datum v, CUDesc* cuDescPtr, bool* first)
{
    CompareUint32(cuDescPtr->cu_min, cuDescPtr->cu_max, v, first, NULL);
}

/*
 * @Description: comparing function for data-type int16
 * @IN/OUT first: indicate the first value
 * @IN/OUT maxval: the newest max value
 * @IN/OUT minval: the newest min value
 * @IN v: the new int16 value
 * @IN/OUT varstr_maxlen: max length about var-length string
 * @See also:
 */
static void CompareInt16(char* minval, char* maxval, Datum v, bool* first, int* varstr_maxlen)
{
    if (!*first) {
        int16 intVal = DatumGetInt16(v);
        if (intVal < *(int16*)minval) {
            *(int16*)(minval) = intVal;
        } else if (intVal > *(int16*)maxval) {
            *(int16*)(maxval) = intVal;
        }
    } else {
        *(int16*)(minval) = *(int16*)(maxval) = DatumGetInt16(v);
        *first = false;
    }
    UNUSED_ARG(varstr_maxlen);
}

/*
 * @Description: set min/max for data-type int16
 * @IN/OUT cuDescPtr: CU Descriptor
 * @IN/OUT first: the first flag
 * @IN v: the new int16 value
 * @See also:
 */
static void SetMinMaxInt16(Datum v, CUDesc* cuDescPtr, bool* first)
{
    CompareInt16(cuDescPtr->cu_min, cuDescPtr->cu_max, v, first, NULL);
}
