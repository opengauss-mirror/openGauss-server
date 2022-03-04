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
 * dfs_wrapper.h
 *
 * IDENTIFICATION
 *    src/include/access/dfs/dfs_wrapper.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DFS_WRAPPER_H
#define DFS_WRAPPER_H
#ifndef ENABLE_LITE_MODE
#include "orc/Exceptions.hh"
#endif
#include "access/dfs/dfs_am.h"
#include "access/dfs/dfs_common.h"
#include "catalog/pg_collation.h"
#include "nodes/execnodes.h"
#include "nodes/pg_list.h"
#include "optimizer/clauses.h"
#include "optimizer/subselect.h"
#include "utils/biginteger.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/dfs_vector.h"
#include "utils/numeric.h"
#include "utils/numeric_gs.h"
#include "utils/lsyscache.h"

/*
 * The base template class for all the data type wrapper to  inherit.
 */
template <typename T>
class HdfsCheckWrapper : public BaseObject {
public:
    virtual ~HdfsCheckWrapper()
    {
    }
    /*
     * Abstract function to compare the argument and velue.
     * @_in param argument: The target to be compared with the member value.
     * @_in param collation: the collation of the var, it is used in function varstr_cmp.
     * @return Return 1=eq, -1=lt, 1=gt.
     */
    virtual int compareT(T argument, Oid collation) = 0;

    /*
     * Set the value in the class which is converted from datum.
     * @_in param datumValue: the datum value to be set.
     * @_in param datumType: the data type oid of the datum.
     * @_in param typeMod: the mod of the datum type.
     */
    virtual void SetValueFromDatum(Datum datumValue, Oid datumType, int32 typeMod) = 0;

    /*
     * A template function to check if the argument transfered in can be filtered. All the non-string
     * wrapper will inherit this function for call.
     *
     * @_in param argument: the value transfered in to be checked
     * @_in param strategy: the strategy used in the predicate.
     * @_in param collation: the collation of the var, it is used in function varstr_cmp.
     * @return True: match and not filtered; False: filtered and set isSelected to false.
     */
    inline bool CheckPredicate(T argument, Oid collation)
    {
        int cmpResult = compareT(argument, collation);
        return HdfsCheckCompareResult(cmpResult);
    }

    /*
     * Get the value of the wrapper.
     * @return Return the value in the class.
     */
    inline const T getValue()
    {
        return value;
    }

    /*
     * Get the datum of the wrapper.
     * @return the datum value.
     */
    inline Datum getDatum()
    {
        return dValue;
    }

private:
    /*
     * Check the compare result depending on the strategy of the predicate pushed down.
     *
     * @_in param cmpResult: The result of comparision, (o=eq, -1=lt, 1=gt)
     *
     * @return True: indicates the value of the row and column match the preidcate pushed down; False:
     *     indicates the value does not match the predicate and need to be filtered.
     */
    bool HdfsCheckCompareResult(int cmpResult);

protected:
    /* Store the real value of the wrapper with type T. */
    T value;
    Datum dValue;

    /*
     * The strategy of the predicate's operator, which must between HDFS_QUERY_EQ
     *     and HDFS_QUERY_NE2.
     */
    HdfsQueryOperator strategy;
    int32 typeMod;
    Oid datumType;
};

/*
 * Check the result of comparation according to the strategy.
 */
template <typename T>
inline bool HdfsCheckWrapper<T>::HdfsCheckCompareResult(int cmpResult)
{
    bool result = false;
    switch (strategy) {
        case HDFS_QUERY_LT: {
            if (0 > cmpResult)
                result = true;
            break;
        }
        case HDFS_QUERY_LTE: {
            if (0 >= cmpResult)
                result = true;
            break;
        }
        case HDFS_QUERY_EQ: {
            if (0 == cmpResult)
                result = true;
            break;
        }
        case HDFS_QUERY_GTE: {
            if (0 <= cmpResult)
                result = true;
            break;
        }
        case HDFS_QUERY_GT: {
            if (0 < cmpResult)
                result = true;
            break;
        }
        case HDFS_QUERY_NE1:
        case HDFS_QUERY_NE2: {
            if (0 != cmpResult)
                result = true;
            break;
        }
        case HDFS_QUERY_INVALID:
        default: {
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_DFS),
                            errmsg("Find unsupported strategy %d!", strategy)));
        }
    }

    return result;
}

/*
 * A wrapper class of int64 type, inherit HdfsCheckWrapper. Here we do not seperate
 * int16, int 32 and int64 wrapper just because what we read from orc file directly for int16,int32 and int64
 * is int64, then it will convert to int16,int32 or int64 datum. Since the predicate need to be checked before
 * the conversion from basic type to datum, so we will compare the int64 value directly and needn't
 * Int32Wrapper and Int16Wrapper.
 */
class Int64Wrapper : public HdfsCheckWrapper<int64> {
public:
    Int64Wrapper(HdfsQueryOperator _strategy)
    {
        value = 0;
        dValue = PointerGetDatum(NULL);
        strategy = _strategy;
        typeMod = 0;
        datumType = InvalidOid;
    }
    virtual ~Int64Wrapper()
    {
    }
    inline void SetValueFromDatum(Datum datumValue, Oid datumType, int32 typeMod)
    {
        dValue = datumValue;

        if (INT1OID == datumType) {
            value = DatumGetChar(datumValue);
        } else if (INT2OID == datumType) {
            value = DatumGetInt16(datumValue);
        } else if (INT4OID == datumType) {
            value = DatumGetInt32(datumValue);
        } else if (NUMERICOID == datumType) {
            int dscale = (unsigned int)(typeMod - VARHDRSZ) & 0xffff;
            Numeric n = DatumGetNumeric(datumValue);
            if (NUMERIC_IS_BI(n)) {
                Assert(dscale == NUMERIC_BI_SCALE(n));
                Assert(NUMERIC_IS_BI64(n));

                value = NUMERIC_64VALUE(n);
            } else
                value = convert_short_numeric_to_int64_byscale(n, dscale);
        } else {
            value = DatumGetInt64(datumValue);
        }
    }

private:
    inline int compareT(int64 argument, Oid collation)
    {
        int cmp = 0;
        INT_CMP_HDFS(argument, value, cmp);
        return cmp;
    }
};

/* just for decimal128, 18 < p <= 38 */
class Int128Wrapper : public HdfsCheckWrapper<int128> {
public:
    Int128Wrapper(HdfsQueryOperator _strategy)
    {
        value = 0;
        dValue = PointerGetDatum(NULL);
        strategy = _strategy;
        typeMod = 0;
        datumType = InvalidOid;
    }
    virtual ~Int128Wrapper()
    {
    }
    inline void SetValueFromDatum(Datum datumValue, Oid datumType, int32 typeMod)
    {
        int dscale = (unsigned int)(typeMod - VARHDRSZ) & 0xffff;

        dValue = datumValue;
        Numeric n = DatumGetNumeric(datumValue);
        if (NUMERIC_IS_BI(n)) {
            Assert(dscale == NUMERIC_BI_SCALE(n));

            if (NUMERIC_IS_BI128(n)) {
                errno_t rc = EOK;
                rc = memcpy_s(&value, sizeof(int128), (n)->choice.n_bi.n_data, sizeof(int128));
                securec_check(rc, "\0", "\0");
            } else
                value = NUMERIC_64VALUE(n);
        } else
            convert_short_numeric_to_int128_byscale(n, dscale, value);
    }

private:
    inline int compareT(int128 argument, Oid collation)
    {
        int cmp = 0;
        INT_CMP_HDFS(argument, value, cmp);
        return cmp;
    }
};

/*
 * A wrapper class of bool type, inherit HdfsCheckWrapper. The actual value stored is a char here.
 */
class BoolWrapper : public HdfsCheckWrapper<char> {
public:
    BoolWrapper(HdfsQueryOperator _strategy)
    {
        value = 0;
        dValue = PointerGetDatum(NULL);
        strategy = _strategy;
        typeMod = 0;
        datumType = InvalidOid;
    }
    virtual ~BoolWrapper()
    {
    }
    inline void SetValueFromDatum(Datum datumValue, Oid datumType, int32 typeMod)
    {
        dValue = datumValue;
        value = DatumGetBool(datumValue);
    }

private:
    inline int compareT(char argument, Oid collation)
    {
        int cmp = 0;
        INT_CMP_HDFS(argument, value, cmp);
        return cmp;
    }
};

/*
 * A wrapper class of float8 type, inherit HdfsCheckWrapper.
 */
class Float8Wrapper : public HdfsCheckWrapper<float8> {
public:
    Float8Wrapper(HdfsQueryOperator _strategy)
    {
        value = 0;
        dValue = PointerGetDatum(NULL);
        strategy = _strategy;
        typeMod = 0;
        datumType = InvalidOid;
    }
    virtual ~Float8Wrapper()
    {
    }
    inline void SetValueFromDatum(Datum datumValue, Oid _datumType, int32 typeMod)
    {
        datumType = _datumType;
        /*
         * For float4/8, there may be data convertion of the const value, so we
         * need to process seperately here.
         */
        if (FLOAT4OID == datumType) {
            value = (float8)DatumGetFloat4(datumValue);
            dValue = Float8GetDatum(value);
        } else  // float8
        {
            value = DatumGetFloat8(datumValue);
            dValue = datumValue;
        }
    }

private:
    inline int compareT(float8 argument, Oid collation)
    {
        int cmp = 0;
        FLOAT_CMP_HDFS(argument, value, cmp);
        return cmp;
    }
};

/*
 * A wrapper class of Timestamp type, inherit HdfsCheckWrapper. Since there is not actual Date type
 * in PG, we just need the wrapper for Timestamp here, and convert the hive date to timestamp before
 * the comparasion with predicate.
 */
class TimestampWrapper : public HdfsCheckWrapper<Timestamp> {
public:
    TimestampWrapper(HdfsQueryOperator _strategy)
    {
        value = 0;
        dValue = PointerGetDatum(NULL);
        strategy = _strategy;
        typeMod = 0;
        datumType = InvalidOid;
    }
    virtual ~TimestampWrapper()
    {
    }
    inline void SetValueFromDatum(Datum datumValue, Oid datumType, int32 typeMod)
    {
        dValue = datumValue;

        /*
         * For different time types, we use different ways to convert to timestamp.
         */
        if (DATEOID == datumType) {
            value = date2timestamp(DatumGetDateADT(datumValue));
        } else if (TIMESTAMPTZOID == datumType) {
            Datum out = DirectFunctionCall1(timestamptz_out, datumValue);
            Datum in = DirectFunctionCall3(timestamp_in, out, ObjectIdGetDatum(InvalidOid), Int32GetDatum(typeMod));
            value = DatumGetTimestamp(in);
        } else {
            value = DatumGetTimestamp(datumValue);
        }
    }

private:
    inline int compareT(Timestamp argument, Oid collation)
    {
        int cmp = 0;
#ifdef HAVE_INT64_TIMESTAMP
        INT_CMP_HDFS(argument, value, cmp);
#else
        FLOAT_CMP_HDFS(argument, value, cmp);
#endif
        return cmp;
    }
};

/*
 * Since we only care about equality or not-equality, we can avoid all the
 * expense of strcoll() here, and just do bitwise comparison.
 * The caller must ensure that both src1 and src2 are valid strings with '\0' at end.
 * return 0 if the two strings are equal.
 */
static int stringEQ(const char *src1, const char *src2)
{
    int result = 0;
    int len1 = strlen(src1);
    int len2 = strlen(src2);
    if (len1 != len2) {
        result = 1;
    } else {
        result = memcmp(src1, src2, len1);
    }
    return result;
}

/*
 * A wrapper class of string type includes varchar,bpchar,text because these types is actually the same for hive.
 */
class StringWrapper : public HdfsCheckWrapper<char *> {
public:
    StringWrapper(HdfsQueryOperator _strategy)
    {
        value = NULL;
        dValue = PointerGetDatum(NULL);
        strategy = _strategy;
        typeMod = 0;
        datumType = InvalidOid;
    }
    virtual ~StringWrapper()
    {
    }
    inline void SetValueFromDatum(Datum datumValue, Oid _datumType, int32 typeMod)
    {
        dValue = datumValue;
        datumType = _datumType;

        if (BPCHAROID == datumType) {
            int varLen = 0;
            char *str = TextDatumGetCString(datumValue);
            int strLen = strlen(str);

            /* variable length */
            if (typeMod < (int32)VARHDRSZ) {
                varLen = strLen;
            } else /* fixed length */
            {
                varLen = typeMod - VARHDRSZ;
            }

            /*
             * When the length of the var is larger than the const string's length, it needs to
             * add some blanks in the tail.
             */
            if (varLen >= strLen) {
                Datum bpchar = DirectFunctionCall3(bpcharin, CStringGetDatum(str), ObjectIdGetDatum(InvalidOid),
                                                   Int32GetDatum(typeMod));
                value = TextDatumGetCString(bpchar);
            } else {
                value = str;
            }
        } else {
            value = TextDatumGetCString(datumValue);
        }
    }

private:
    inline int compareT(char *argument, Oid collation)
    {
        int cmp = 0;
        if (HDFS_QUERY_EQ == strategy || HDFS_QUERY_NE1 == strategy || HDFS_QUERY_NE2 == strategy) {
            cmp = stringEQ(argument, value);
        } else {
            cmp = varstr_cmp(argument, strlen(argument), value, strlen(value), collation);
        }
        return cmp;
    }
};

/*
 * This wrapper is only used for HdfsPredicateCheckNull, and there is need to store any value here. We don't
 * remove it because Null check is processed specially and  keeping a special empty wrapper for null can
 * make it less confused.
 */
class NullWrapper {
};

#endif
