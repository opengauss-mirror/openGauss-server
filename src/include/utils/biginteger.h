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
 * biginteger.h
 *        Definitions for the big integer data type
 * 
 * Big integer(int64/int128) is one implementation of numeric data
 * type, the purpose is to increase speed of numeric operators.
 * 
 * IDENTIFICATION
 *        src/include/utils/biginteger.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef SRC_INCLUDE_UTILS_BIGINTERGER_H_
#define SRC_INCLUDE_UTILS_BIGINTERGER_H_

#include "postgres.h"
#include "knl/knl_variable.h"
#include "c.h"
#include "catalog/pg_type.h"
#include "fmgr.h"
#include "vecexecutor/vectorbatch.h"
#include "utils/array.h"
#include "utils/numeric.h"
#include "utils/numeric_gs.h"

/*
 * int32 integer:
 *    int32 value must in [-2147483648, 2147483647]
 *    abs(int32_value) < 10^11, so the max dscale of int32 can represent is 10.
 *
 * int64 integer:
 *    int64 value must in [-9223372036854775808, 9223372036854775807]
 *    abs(int64_value) < 10^20, so the max dscale of int64 can represent is 19.
 *
 * int128 integer:
 *    int128 value must in [-170141183460469231731687303715884105728,
 *                           170141183460469231731687303715884105727]
 *    abs(int128_value) < 10^40, so the max dscale of int128 can represent is
 *    39, but some numbers with 39 digits can't be stored by int128, so just
 *    choose 38 instead because one number with 38 digits must can be stored
 *    by int128.
 */
#define MAXINT32DIGIT 10
#define MAXINT64DIGIT 19
#define MAXINT128DIGIT 38

/* the flag of failed dscale encoding */
#define FAILED_DSCALE_ENCODING -1

/* the value of 10^18, it is used for print int128 data */
#define P10_INT64 1000000000000000000LL /* 18 zeroes */

/* this macro represents the first dimension index of BiAggFunMatrix */
#define BI_AGG_ADD 0
#define BI_AGG_SMALLER 1
#define BI_AGG_LARGER 2

/*
 * determine whether one num can convert to int64 or int128 by the
 * num's scale, this macro is used for both executor and storage modules.
 */
#define CAN_CONVERT_BI64(whole_scale) ((whole_scale) <= (MAXINT64DIGIT - 1))
#define CAN_CONVERT_BI128(whole_scale) ((whole_scale) <= MAXINT128DIGIT)

/* jduge whether int64 data can be transformed to int8 */
#define INT64_INT8_EQ(data) ((data) == (int64)((int8)(data)))

/* jduge whether int64 data can be transformed to int16 */
#define INT64_INT16_EQ(data) ((data) == (int64)((int16)(data)))

/* jduge whether int64 data can be transformed to int32 */
#define INT64_INT32_EQ(data) ((data) == (int64)((int32)(data)))

/* jduge whether int128 data can be transformed to int64 */
#define INT128_INT64_EQ(data) ((data) == (int128)((int64)(data)))

/*
 * align two int64 vals by dscale, multipler is factorial of 10.
 * set ScaleMultipler[i] = 10^i, i is between 0 and 18.
 * 10^19 is out of int64 bound, so set ScaleMultipler[19] to 0.
 */
extern const int64 ScaleMultipler[20];

/*
 * fast compare whether the result of arg * ScaleMultipler[i]
 * is out of int64 bound.
 * Int64MultiOutOfBound[i] = INT64_MIN / ScaleMultipler[i]
 */
extern const int64 Int64MultiOutOfBound[20];

/*
 * @Description: calculates the factorial of 10, align two int128
 *               vals by dscale, multipler is factorial of 10, set
 *               values[i] = 10^i, i is between 0 and 38.
 * @IN  scale: the scale'th power
 * @Return: the factorial of 10
 */
inline int128 getScaleMultiplier(int scale)
{
    Assert(scale >= 0 && scale <= MAXINT128DIGIT);
    static const int128 values[] = {static_cast<int128>(1LL),
        static_cast<int128>(10LL),
        static_cast<int128>(100LL),
        static_cast<int128>(1000LL),
        static_cast<int128>(10000LL),
        static_cast<int128>(100000LL),
        static_cast<int128>(1000000LL),
        static_cast<int128>(10000000LL),
        static_cast<int128>(100000000LL),
        static_cast<int128>(1000000000LL),
        static_cast<int128>(10000000000LL),
        static_cast<int128>(100000000000LL),
        static_cast<int128>(1000000000000LL),
        static_cast<int128>(10000000000000LL),
        static_cast<int128>(100000000000000LL),
        static_cast<int128>(1000000000000000LL),
        static_cast<int128>(10000000000000000LL),
        static_cast<int128>(100000000000000000LL),
        static_cast<int128>(1000000000000000000LL),
        static_cast<int128>(1000000000000000000LL) * 10LL,
        static_cast<int128>(1000000000000000000LL) * 100LL,
        static_cast<int128>(1000000000000000000LL) * 1000LL,
        static_cast<int128>(1000000000000000000LL) * 10000LL,
        static_cast<int128>(1000000000000000000LL) * 100000LL,
        static_cast<int128>(1000000000000000000LL) * 1000000LL,
        static_cast<int128>(1000000000000000000LL) * 10000000LL,
        static_cast<int128>(1000000000000000000LL) * 100000000LL,
        static_cast<int128>(1000000000000000000LL) * 1000000000LL,
        static_cast<int128>(1000000000000000000LL) * 10000000000LL,
        static_cast<int128>(1000000000000000000LL) * 100000000000LL,
        static_cast<int128>(1000000000000000000LL) * 1000000000000LL,
        static_cast<int128>(1000000000000000000LL) * 10000000000000LL,
        static_cast<int128>(1000000000000000000LL) * 100000000000000LL,
        static_cast<int128>(1000000000000000000LL) * 1000000000000000LL,
        static_cast<int128>(1000000000000000000LL) * 10000000000000000LL,
        static_cast<int128>(1000000000000000000LL) * 100000000000000000LL,
        static_cast<int128>(1000000000000000000LL) * 100000000000000000LL * 10LL,
        static_cast<int128>(1000000000000000000LL) * 100000000000000000LL * 100LL,
        static_cast<int128>(1000000000000000000LL) * 100000000000000000LL * 1000LL};
    return values[scale];
}

/*
 * @Description: fast compare whether the result of arg * getScaleMultiplier(i)
 *               is out of int128 bound. getScaleQuotient(i) equals to
 *               INT128_MIN / getScaleMultiplier(i)
 * @IN  scale: num between 0 and MAXINT128DIGIT
 * @Return: values[scale]
 */
inline int128 getScaleQuotient(int scale)
{
    Assert(scale >= 0 && scale <= MAXINT128DIGIT);
    static const int128 values[] = {INT128_MIN,
        INT128_MIN / getScaleMultiplier(1),
        INT128_MIN / getScaleMultiplier(2),
        INT128_MIN / getScaleMultiplier(3),
        INT128_MIN / getScaleMultiplier(4),
        INT128_MIN / getScaleMultiplier(5),
        INT128_MIN / getScaleMultiplier(6),
        INT128_MIN / getScaleMultiplier(7),
        INT128_MIN / getScaleMultiplier(8),
        INT128_MIN / getScaleMultiplier(9),
        INT128_MIN / getScaleMultiplier(10),
        INT128_MIN / getScaleMultiplier(11),
        INT128_MIN / getScaleMultiplier(12),
        INT128_MIN / getScaleMultiplier(13),
        INT128_MIN / getScaleMultiplier(14),
        INT128_MIN / getScaleMultiplier(15),
        INT128_MIN / getScaleMultiplier(16),
        INT128_MIN / getScaleMultiplier(17),
        INT128_MIN / getScaleMultiplier(18),
        INT128_MIN / getScaleMultiplier(19),
        INT128_MIN / getScaleMultiplier(20),
        INT128_MIN / getScaleMultiplier(21),
        INT128_MIN / getScaleMultiplier(22),
        INT128_MIN / getScaleMultiplier(23),
        INT128_MIN / getScaleMultiplier(24),
        INT128_MIN / getScaleMultiplier(25),
        INT128_MIN / getScaleMultiplier(26),
        INT128_MIN / getScaleMultiplier(27),
        INT128_MIN / getScaleMultiplier(28),
        INT128_MIN / getScaleMultiplier(29),
        INT128_MIN / getScaleMultiplier(30),
        INT128_MIN / getScaleMultiplier(31),
        INT128_MIN / getScaleMultiplier(32),
        INT128_MIN / getScaleMultiplier(33),
        INT128_MIN / getScaleMultiplier(34),
        INT128_MIN / getScaleMultiplier(35),
        INT128_MIN / getScaleMultiplier(36),
        INT128_MIN / getScaleMultiplier(37),
        INT128_MIN / getScaleMultiplier(38)};
    return values[scale];
}

/*
 * big integer common operators, we implement corresponding
 * functions of big integer for higher performance.
 */
enum biop {
    BIADD = 0,  // +
    BISUB,      // -
    BIMUL,      // *
    BIDIV,      // /
    BIEQ,       // =
    BINEQ,      // !=
    BILE,       // <=
    BILT,       // <
    BIGE,       // >=
    BIGT,       // >
    BICMP       // > || == || <
};

/*
 * when adjust scale for two big integer(int64 or int128) data,
 * determine the result whether out of bound.
 */
enum BiAdjustScale { BI_AJUST_TRUE = 0, BI_LEFT_OUT_OF_BOUND, BI_RIGHT_OUT_OF_BOUND };

/*
 * this struct is used for numeric agg function.
 * use vechashtable::CopyVarP to decrease memory application.
 */
struct bictl {
    Datum store_pos;
    MemoryContext context;
};

/*
 * Function to compute two big integer data(int64 or int128);
 * include four operations and comparison operations.
 */
typedef Datum (*biopfun)(Numeric larg, Numeric rarg, bictl* ctl);

/*
 * three dimensional arrays: BiFunMatrix
 * this array is used for fast locate the corresponding operation
 * functions by operator and type(int64 or int128)
 */
extern const biopfun BiFunMatrix[11][2][2];

/*
 * three dimensional arrays: BiAggFunMatrix
 * this array is used for fast locate the corresponding agg
 * functions by operator and type(int64 or int128)
 */
extern const biopfun BiAggFunMatrix[3][2][2];

/* convert big integer data to numeric */
extern Numeric bitonumeric(Datum arg);
/* convert int(int1/int2/int4/int8) to big integer type */
extern Datum int1_numeric_bi(PG_FUNCTION_ARGS);
extern Datum int2_numeric_bi(PG_FUNCTION_ARGS);
extern Datum int4_numeric_bi(PG_FUNCTION_ARGS);
extern Datum int8_numeric_bi(PG_FUNCTION_ARGS);
/* hash function for big integer(int64/int128/numeric) */
extern Datum hash_bi(PG_FUNCTION_ARGS);
/* replace numeric hash function and equal function for VecHashAgg/VecHashJoin Node */
extern void replace_numeric_hash_to_bi(int numCols, FmgrInfo* hashFunctions);
/* bi64 and bi128 output function */
Datum bi64_out(int64 data, int scale);
Datum bi128_out(int128 data, int scale);

Datum Simplebi64div64(Numeric larg, Numeric rarg);

void Int128GetVal(bool is_int128, int128* val, Numeric* arg);

#endif /* SRC_INCLUDE_UTILS_BIGINTERGER_H_ */
