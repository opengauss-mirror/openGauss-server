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
 * -------------------------------------------------------------------------
 * @file:  biginteger.cpp
 * @brief: An implementation of numeric data type which is more efficiently
 *
 * The type numeric can store numbers with a very large number of digits.
 * It is especially recommended for storing monetary amounts and other
 * quantities where exactness is required. However, calculations on numeric
 * values are very slow compared to the integer types. Calculations on
 * numeric have become bottleneck in many contexts.
 * To solve this problem, we use int64 and int128 data type of GCC to simulate
 * numeric operations. int64 can represent number within 19 digits, int128
 * can represent 39 digits. In most cases numeric data can be represented by
 * int64 or int128. So we implement the main operations of big integer,
 *  e.g. addition, subtraction, multiplication, division, comparsion, etc.
 *
 * IDENTIFICATION
 *	  src/gausskernel/cbb/utils/biginteger/biginteger.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "utils/biginteger.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "utils/builtins.h"
#include "utils/int8.h"

/*
 * max length of bi64
 * symbol(+/-) + digits + decimal point + '\0' + reserved for safe
 *      1      +   19   +      1        +  1   + 3 = 25
 */
#define MAXBI64LEN 25

/*
 * max length of bi128
 * symbol(+/-) + digits + decimal point + '\0' + reserved for safe
 *      1      +   39   +      1        +  1   + 3 = 45
 */
#define MAXBI128LEN 45

/* jduge whether a and b have the same sign */
#define SAMESIGN(a, b) (((a) < 0) == ((b) < 0))

/* INT64_MIN string */
static const char* int64_min_str = "-9223372036854775808";

/* INT128_MIN string */
static const char* int128_min_str = "-170141183460469231731687303715884105728";

/*
 * align two int64 vals by dscale, multipler is factorial of 10.
 * set ScaleMultipler[i] = 10^i, i is between 0 and 18.
 * 10^19 is out of int64 bound, so set ScaleMultipler[19] to 0.
 */
const int64 ScaleMultipler[20] = {1LL,
    10LL,
    100LL,
    1000LL,
    10000LL,
    100000LL,
    1000000LL,
    10000000LL,
    100000000LL,
    1000000000LL,
    10000000000LL,
    100000000000LL,
    1000000000000LL,
    10000000000000LL,
    100000000000000LL,
    1000000000000000LL,
    10000000000000000LL,
    100000000000000000LL,
    1000000000000000000LL,
    0LL};

/*
 * fast compare whether the result of arg * ScaleMultipler[i]
 * is out of int64 bound.
 * the value of "Int64MultiOutOfBound[i]" equals to "INT64_MIN / ScaleMultipler[i]"
 */
const int64 Int64MultiOutOfBound[20] = {INT64_MIN_VALUE,
    INT64_MIN_VALUE / ScaleMultipler[1],
    INT64_MIN_VALUE / ScaleMultipler[2],
    INT64_MIN_VALUE / ScaleMultipler[3],
    INT64_MIN_VALUE / ScaleMultipler[4],
    INT64_MIN_VALUE / ScaleMultipler[5],
    INT64_MIN_VALUE / ScaleMultipler[6],
    INT64_MIN_VALUE / ScaleMultipler[7],
    INT64_MIN_VALUE / ScaleMultipler[8],
    INT64_MIN_VALUE / ScaleMultipler[9],
    INT64_MIN_VALUE / ScaleMultipler[10],
    INT64_MIN_VALUE / ScaleMultipler[11],
    INT64_MIN_VALUE / ScaleMultipler[12],
    INT64_MIN_VALUE / ScaleMultipler[13],
    INT64_MIN_VALUE / ScaleMultipler[14],
    INT64_MIN_VALUE / ScaleMultipler[15],
    INT64_MIN_VALUE / ScaleMultipler[16],
    INT64_MIN_VALUE / ScaleMultipler[17],
    INT64_MIN_VALUE / ScaleMultipler[18],
    0LL};

/*
 * @Description: adjust two int64 number to same scale
 * @IN  x: the value of num1
 * @IN  x_scale: the scale of num1
 * @IN  y: the value of num2
 * @IN  y_scale: the scale of num2
 * @OUT result_scale: the adjusted scale of num1 and num2, the
 *                    maximum value of x_scale and y_scale
 * @OUT x_scaled: the value of num1 which scale is result_scale
 * @OUT y_scaled: the value of num2 which scale is result_scale
 * @Return: adjust succeed or not
 *
 * Example:
 * +-------------------+----------------------+-------------------------------+
 * | defination        | num1: numeric(10, 2) |  num2: numeric(12, 4)         |
 * +-------------------+----------------------+-------------------------------+
 * | user data         |      num1 = 1.0      |        num2 = 2.0             |
 * +-------------------+----------------------+-------------------------------+
 * | BI implementation | x = 100, x_scale = 2 | y = 20000, y_scale = 4        |
 * +-------------------+----------------------+-------------------------------+
 * | Adjusted result   | result_scale = 4, x_scaled = 10000, y_scaled = 20000 |
 * +-------------------+----------------------+-------------------------------+
 */
static inline BiAdjustScale adjustBi64ToSameScale(
    int64 x, int x_scale, int64 y, int y_scale, int* result_scale, int64* x_scaled, int64* y_scaled)
{
    Assert(x_scale >= 0);
    Assert(x_scale <= MAXINT64DIGIT);
    Assert(y_scale >= 0);
    Assert(y_scale <= MAXINT64DIGIT);

    int delta_scale = x_scale - y_scale;
    int64 tmpval = 0;

    if (delta_scale >= 0) {
        /* use negative number is to avoid INT64_MIN * -1
         * out of int64 bound.
         */
        tmpval = (y < 0) ? y : -y;
        *result_scale = x_scale;
        *x_scaled = x;
        /* the result of tmpval * ScaleMultipler[delta_scale]
         * doesn't out of int64 bound.
         */
        if (likely(tmpval >= Int64MultiOutOfBound[delta_scale])) {
            *y_scaled = y * ScaleMultipler[delta_scale];
            return BI_AJUST_TRUE;
        } else { /* y_scaled must be out of int64 bound */
            return BI_RIGHT_OUT_OF_BOUND;
        }
    } else {
        /* use negative number is to avoid INT64_MIN * -1
         * out of int64 bound.
         */
        tmpval = (x < 0) ? x : -x;
        *result_scale = y_scale;
        *y_scaled = y;
        /* the result of tmpval * ScaleMultipler[delta_scale]
         * doesn't out of int64 bound.
         */
        if (likely(tmpval >= Int64MultiOutOfBound[-delta_scale])) {
            *x_scaled = x * ScaleMultipler[-delta_scale];
            return BI_AJUST_TRUE;
        } else {
            return BI_LEFT_OUT_OF_BOUND;
        }
    }
}

/*
 * @Description: adjust two int128 number to same scale
 * @IN  x: the value of num1
 * @IN  x_scale: the scale of num1
 * @IN  y: the value of num2
 * @IN  y_scale: the scale of num2
 * @OUT result_scale: the adjusted scale of num1 and num2, the
 *                    maximum value of x_scale and y_scale
 * @OUT x_scaled: the value of num1 which scale is result_scale
 * @OUT y_scaled: the value of num2 which scale is result_scale
 * @Return: adjust succeed or not
 */
static inline BiAdjustScale adjustBi128ToSameScale(
    int128 x, int x_scale, int128 y, int y_scale, int* result_scale, int128* x_scaled, int128* y_scaled)
{
    Assert(x_scale >= 0);
    Assert(x_scale <= MAXINT128DIGIT);
    Assert(y_scale >= 0);
    Assert(y_scale <= MAXINT128DIGIT);

    int delta_scale = x_scale - y_scale;
    int128 tmpval = 0;

    if (delta_scale >= 0) {
        /* use negative number is to avoid INT128_MIN * -1
         * out of int128 bound.
         */
        tmpval = (y < 0) ? y : -y;
        *result_scale = x_scale;
        *x_scaled = x;
        /* the result of tmpval * ScaleMultipler[delta_scale]
         * doesn't out of int128 bound.
         */
        if (likely(tmpval >= getScaleQuotient(delta_scale))) {
            *y_scaled = y * getScaleMultiplier(delta_scale);
            return BI_AJUST_TRUE;
        } else { /* y_scaled must be out of int64 bound */
            return BI_RIGHT_OUT_OF_BOUND;
        }
    } else {
        /* use negative number is to avoid INT128_MIN * -1
         * out of int128 bound.
         */
        tmpval = (x < 0) ? x : -x;
        *result_scale = y_scale;
        *y_scaled = y;
        /* the result of tmpval * ScaleMultipler[delta_scale]
         * doesn't out of int128 bound.
         */
        if (likely(tmpval >= getScaleQuotient(-delta_scale))) {
            *x_scaled = x * getScaleMultiplier(-delta_scale);
            return BI_AJUST_TRUE;
        } else {
            return BI_LEFT_OUT_OF_BOUND;
        }
    }
}

/*
 * @Description: get the weight and firstDigit of bi64 or bi128, it is used to
 *               calculate bi div bi like numeric_div does.
 *
 * @IN val: the value of bi64/bi128 num.
 * @IN scale: the scale of bi64/bi128 num.
 * @OUT weight: the weight of num in Numeric mode.
 * @OUT firstDigit: the first digit of num in Numeric mode.
 * @return: NULL
 */
template <typename bitype>
inline void getBiWeightDigit(bitype val, int scale, int* weight, int* firstDigit)
{
    Assert(scale >= 0);
    Assert(scale <= MAXINT128DIGIT);

    /* calculate weight of val */
    int digits = 0;
    int result_scale = scale;
    /* temp value of calculation */
    bitype tmp_data = 0;

    /* step 1: split source int64 data into pre_data and post_data by decimal point */
    /* pre_data stores the data before decimal point. */
    bitype pre_data = val / getScaleMultiplier(result_scale);
    /* pre_data stores the data after decimal point. */
    bitype post_data = val % getScaleMultiplier(result_scale);

    /* step 2: calculate pre_data */
    /* pre_data == 0, skip this */
    if (pre_data != 0) {
        do {
            digits++;
            *firstDigit = pre_data % NBASE;
            pre_data = pre_data / NBASE;
        } while (pre_data);
        *weight = digits - 1;
        return;
    }

    /* step 3: calculate post_data */
    /* post_data == 0, skip this */
    if (post_data != 0) {
        while (post_data && result_scale >= DEC_DIGITS) {
            digits++;
            result_scale = result_scale - DEC_DIGITS;
            tmp_data = post_data / getScaleMultiplier(result_scale);
            if (tmp_data) {
                *firstDigit = tmp_data;
                *weight = -digits;
                return;
            }
            post_data = post_data % getScaleMultiplier(result_scale);
        }
        Assert(post_data != 0 && result_scale < DEC_DIGITS);
        *weight = -(digits + 1);
        *firstDigit = post_data * ScaleMultipler[DEC_DIGITS - result_scale];
        return;
    }
    *weight = 0;
    *firstDigit = 0;
    return;
}

/*
 * @Description: Calculate the result of bi64(big integer) add bi64.
 *
 * @IN larg: left-hand operand of addition(+).
 * @IN rarg: right-hand operand of addition(+).
 * @IN ctl: vechashtable info, it is used when use_ctl is true
 * @return: Datum - the datum data points to bi64 or bi128.
 */
template <bool use_ctl>
Datum bi64add64(Numeric larg, Numeric rarg, bictl* ctl)
{
    Assert(NUMERIC_IS_BI(larg));
    Assert(NUMERIC_IS_BI(rarg));

    uint8 lvalscale = NUMERIC_BI_SCALE(larg);
    uint8 rvalscale = NUMERIC_BI_SCALE(rarg);
    int64 leftval = NUMERIC_64VALUE(larg);
    int64 rightval = NUMERIC_64VALUE(rarg);
    int64 leftval_scaled = 0;
    int64 right_scaled = 0;
    int64 result = 0;
    int128 big_result = 0;
    int resScale = 0;
    ;

    /* Adjust leftval and rightval to the same scale */
    BiAdjustScale overflow =
        adjustBi64ToSameScale(leftval, lvalscale, rightval, rvalscale, &resScale, &leftval_scaled, &right_scaled);

    switch (overflow) {
        /* leftval_scaled and right_scaled both don't overflow, add directly */
        case BI_AJUST_TRUE:
            /* GNUC > 5, call __builtin_add_overflow() */
            if (likely(!__builtin_add_overflow(leftval_scaled, right_scaled, &result))) {
                /* For agg addiotion operation, ctl isn't NULL, assign value directly. */
                if (use_ctl) {
                    Numeric res = (Numeric)ctl->store_pos;
                    /* Set result scale */
                    res->choice.n_header = NUMERIC_64 + resScale;
                    /* Set result value */
                    *((int64*)(&res->choice.n_bi.n_data[0])) = result;
                    return (Datum)0;
                } else {
                    /* For normal addiotion operation, ctl is NULL, palloc memory for result. */
                    return makeNumeric64(result, resScale);
                }
            }
            /* result is out of bound, turn to int128 calculate */
            big_result = (int128)leftval_scaled + (int128)right_scaled;
            break;

        /* right_scaled is out of bound */
        case BI_RIGHT_OUT_OF_BOUND:
            big_result = (int128)leftval + (int128)rightval * getScaleMultiplier(resScale - rvalscale);
            break;

        /* leftval_scaled is out of bound */
        default:  // case BI_LEFT_OUT_OF_BOUND
            big_result = (int128)leftval * getScaleMultiplier(resScale - lvalscale) + (int128)rightval;
            break;
    }
    /* For agg addiotion operation, ctl isn't NULL, call vechashtable::CopyVarP(). */
    if (use_ctl) {
        ctl->store_pos = replaceVariable(ctl->context, ctl->store_pos, makeNumeric128(big_result, resScale));

        return (Datum)0;
    } else {
        /* For normal addiotion operation, ctl is NULL, palloc memory for result. */
        return makeNumeric128(big_result, resScale);
    }
}

/*
 * @Description: This function can calculate the result of bi64(big integer)
 *               add bi128(big integer), can calculate the result of bi128
 *               add bi64, also include bi128 add bi128.
 *
 * @IN larg: left-hand operand of addition(+), when template parameter larg_is_int128
 *           is true, larg is bi128 type, else larg is bi64 type.
 * @IN rarg: right-hand operand of addition(+), when template parameter rarg_is_int128
 *           is true, rarg is bi128 type, else rarg is bi64 type.
 * @IN ctl: vechashtable info, it is used when use_ctl is true.
 * @return: Datum - the datum data points to bi128 or numeric.
 */
template <bool larg_is_int128, bool rarg_is_int128, bool use_ctl>
Datum bi128add128(Numeric larg, Numeric rarg, bictl* ctl)
{
    Assert(NUMERIC_IS_BI(larg));
    Assert(NUMERIC_IS_BI(rarg));

    uint8 lvalscale = NUMERIC_BI_SCALE(larg);
    uint8 rvalscale = NUMERIC_BI_SCALE(rarg);
    int128 leftval = 0;
    int128 rightval = 0;
    int128 leftval_scaled = 0;
    int128 right_scaled = 0;
    int128 result = 0;
    int resScale = 0;

    /* get left val */
    Int128GetVal(larg_is_int128, &leftval, &larg);
    /* get right val */
    Int128GetVal(rarg_is_int128, &rightval, &rarg);

    /* Adjust leftval and rightval to the same scale */
    BiAdjustScale overflow =
        adjustBi128ToSameScale(leftval, lvalscale, rightval, rvalscale, &resScale, &leftval_scaled, &right_scaled);

    switch (overflow) {
        case BI_AJUST_TRUE:
            /* GNUC > 5, call __builtin_add_overflow() */
            if (likely(!__builtin_add_overflow(leftval_scaled, right_scaled, &result))) {
                /* For agg addiotion operation, ctl isn't NULL, assign value directly. */
                if (use_ctl) {
                    /* ctl->store_pos is bi128, it can store the int128 result */
                    if (larg_is_int128) {
                        Numeric res = (Numeric)ctl->store_pos;
                        /* Set result scale */
                        res->choice.n_header = NUMERIC_128 + resScale;
                        /* Set result value */
                        errno_t rc = memcpy_s((res)->choice.n_bi.n_data, sizeof(int128), &result, sizeof(int128));
                        securec_check(rc, "\0", "\0");
                        return (Datum)0;
                    } else {
                        /* ctl->store_pos is bi64, it can't store the int128 result */
                        ctl->store_pos =
                            replaceVariable(ctl->context, ctl->store_pos, makeNumeric128(result, resScale));

                        return (Datum)0;
                    }
                } else { /* For normal addiotion operation, ctl is NULL, palloc memory for result. */
                    return makeNumeric128(result, resScale);
                }
            }
        /* BI_LEFT_OUT_OF_BOUND OR BI_RIGHT_OUT_OF_BOUND OR add overflow */
        /* fall through */
        default:
            /* result is out of int128 bound, call numeric_add calculate */
            Datum args[2];
            FunctionCallInfoData finfo;
            finfo.arg = &args[0];
            args[0] = (Datum)makeNumericNormal(larg);
            args[1] = (Datum)makeNumericNormal(rarg);
            /* For agg addiotion operation, ctl isn't NULL, assign value directly. */
            if (use_ctl) {
                Datum res = numeric_add(&finfo);
                ctl->store_pos = replaceVariable(ctl->context, ctl->store_pos, res);
                return (Datum)0;
            } else { /* For normal addiotion operation, ctl is NULL, palloc memory for result. */
                return numeric_add(&finfo);
            }
    }
}

/*
 * @Description: Calculate the result of bi64 subtract bi64.
 *
 * @IN larg: left-hand operand of subtraction(-).
 * @IN rarg: right-hand operand of subtraction(-).
 * @IN ctl: ctl is always NULL.
 * @return: Datum - the datum data points to bi64 or bi128.
 */
Datum bi64sub64(Numeric larg, Numeric rarg, bictl* ctl)
{
    Assert(NUMERIC_IS_BI(larg));
    Assert(NUMERIC_IS_BI(rarg));

    uint8 lvalscale = NUMERIC_BI_SCALE(larg);
    uint8 rvalscale = NUMERIC_BI_SCALE(rarg);
    int64 leftval = NUMERIC_64VALUE(larg);
    int64 rightval = NUMERIC_64VALUE(rarg);
    int64 leftval_scaled = 0;
    int64 right_scaled = 0;
    int64 result = 0;
    int128 big_result = 0;
    int resScale = 0;

    /* Adjust leftvaland rightval to the same scale */
    BiAdjustScale overflow =
        adjustBi64ToSameScale(leftval, lvalscale, rightval, rvalscale, &resScale, &leftval_scaled, &right_scaled);

    switch (overflow) {
        /* leftval_scaled and right_scaled both don't overflow, add directly */
        case BI_AJUST_TRUE:
            /* GNUC > 5, call __builtin_add_overflow() */
            if (likely(!__builtin_sub_overflow(leftval_scaled, right_scaled, &result))) {
                return makeNumeric64(result, resScale);
            }
            /* result is out of bound, turn to int128 calculate */
            big_result = (int128)leftval_scaled - (int128)right_scaled;
            break;

        /* right_scaled is out of bound */
        case BI_RIGHT_OUT_OF_BOUND:
            big_result = (int128)leftval - (int128)rightval * getScaleMultiplier(resScale - rvalscale);
            break;

        /* leftval_scaled is out of bound */
        default:  // case BI_LEFT_OUT_OF_BOUND
            big_result = (int128)leftval * getScaleMultiplier(resScale - lvalscale) - (int128)rightval;
            break;
    }
    return makeNumeric128(big_result, resScale);
}

/*
 * @Description: This function can calculate the result of bi64(big integer)
 *               subtract bi128(big integer), can calculate the result of
 *               bi128 subtract bi64, also include bi128 subtract bi128.
 *
 * @IN larg: left-hand operand of subtraction(-), when template parameter larg_is_int128
 *           is true, larg is bi128 type, else larg is bi64 type.
 * @IN rarg: right-hand operand of subtraction(-), when template parameter rarg_is_int128
 *           is true, rarg is bi128 type, else rarg is bi64 type.
 * @IN ctl: ctl is always NULL.
 * @return: Datum - the datum data points to bi128 or numeric.
 */
template <bool larg_is_int128, bool rarg_is_int128>
Datum bi128sub128(Numeric larg, Numeric rarg, bictl* ctl)
{
    Assert(NUMERIC_IS_BI(larg));
    Assert(NUMERIC_IS_BI(rarg));

    uint8 lvalscale = NUMERIC_BI_SCALE(larg);
    uint8 rvalscale = NUMERIC_BI_SCALE(rarg);
    int128 leftval = 0;
    int128 rightval = 0;
    int128 leftval_scaled = 0;
    int128 right_scaled = 0;
    int128 result = 0;
    int resScale = 0;

    /* get left val */
    Int128GetVal(larg_is_int128, &leftval, &larg);
    /* get right val */
    Int128GetVal(rarg_is_int128, &rightval, &rarg);

    /* Adjust leftval and rightval to the same scale */
    BiAdjustScale overflow =
        adjustBi128ToSameScale(leftval, lvalscale, rightval, rvalscale, &resScale, &leftval_scaled, &right_scaled);

    switch (overflow) {
        case BI_AJUST_TRUE:
            /* GNUC > 5, call __builtin_add_overflow() */
            if (likely(!__builtin_sub_overflow(leftval_scaled, right_scaled, &result))) {
                return makeNumeric128(result, resScale);
            }
        /* BI_LEFT_OUT_OF_BOUND OR BI_RIGHT_OUT_OF_BOUND OR add overflow */
        /* fall through */
        default:
            /* result is out of int128 bound, call numeric_sub calculate */
            Datum args[2];
            FunctionCallInfoData finfo;
            finfo.arg = &args[0];
            args[0] = (Datum)makeNumericNormal(larg);
            args[1] = (Datum)makeNumericNormal(rarg);
            return numeric_sub(&finfo);
    }
}

/*
 * @Description: Calculate the result of bi64 multiply bi64.
 *
 * @IN larg: left-hand operand of multiplication(*).
 * @IN rarg: right-hand operand of multiplication(*).
 * @IN ctl: ctl is always NULL.
 * @return: Datum - the datum data points to bi64 or bi128.
 */
Datum bi64mul64(Numeric larg, Numeric rarg, bictl* ctl)
{
    Assert(NUMERIC_IS_BI(larg));
    Assert(NUMERIC_IS_BI(rarg));
    Assert(NUMERIC_BI_SCALE(larg) >= 0);
    Assert(NUMERIC_BI_SCALE(larg) <= MAXINT64DIGIT);
    Assert(NUMERIC_BI_SCALE(rarg) >= 0);
    Assert(NUMERIC_BI_SCALE(rarg) <= MAXINT64DIGIT);

    int64 leftval = NUMERIC_64VALUE(larg);
    int64 rightval = NUMERIC_64VALUE(rarg);
    int resScale = NUMERIC_BI_SCALE(larg) + NUMERIC_BI_SCALE(rarg);
    int64 result = 0;

    if (likely(resScale <= MAXINT64DIGIT && !__builtin_mul_overflow(leftval, rightval, &result))) {
        return makeNumeric64(result, resScale);
    } else {
        return makeNumeric128((int128)leftval * (int128)rightval, resScale);
    }
}

/*
 * @Description: This function can calculate the result of bi64(big integer)
 *               multiply bi128(big integer), can calculate the result of bi128
 *               multiply bi64, also include bi128 multiply bi128.
 *
 * @IN larg: left-hand operand of multiplication(*), when template parameter larg_is_int128
 *           is true, larg is bi128 type, else larg is bi64 type.
 * @IN rarg: right-hand operand of multiplication(*), when template parameter rarg_is_int128
 *           is true, rarg is bi128 type, else rarg is bi64 type.
 * @IN ctl: ctl is always NULL.
 * @return: Datum - the datum data points to bi128 or binumeric.
 */
template <bool larg_is_int128, bool rarg_is_int128>
Datum bi128mul128(Numeric larg, Numeric rarg, bictl* ctl)
{
    Assert(NUMERIC_IS_BI(larg));
    Assert(NUMERIC_IS_BI(rarg));
    Assert(NUMERIC_BI_SCALE(larg) >= 0);
    Assert(NUMERIC_BI_SCALE(larg) <= MAXINT128DIGIT);
    Assert(NUMERIC_BI_SCALE(rarg) >= 0);
    Assert(NUMERIC_BI_SCALE(rarg) <= MAXINT128DIGIT);

    int128 leftval = 0;
    int128 rightval = 0;
    int128 result = 0;
    int resScale = NUMERIC_BI_SCALE(larg) + NUMERIC_BI_SCALE(rarg);

    /* get left val */
    Int128GetVal(larg_is_int128, &leftval, &larg);
    /* get right val */
    Int128GetVal(rarg_is_int128, &rightval, &rarg);
    /* GNUC > 5, call __builtin_add_overflow() */
    if (likely(resScale <= MAXINT128DIGIT && !__builtin_mul_overflow(leftval, rightval, &result))) {
        return makeNumeric128(result, resScale);
    } else {
        /* result is out of int128 bound, call numeric_mul calculate */
        Datum args[2];
        FunctionCallInfoData finfo;
        finfo.arg = &args[0];
        args[0] = (Datum)makeNumericNormal(larg);
        args[1] = (Datum)makeNumericNormal(rarg);
        return numeric_mul(&finfo);
    }
}

static inline void CheckBi64Args(Numeric larg, Numeric rarg)
{
    Assert(NUMERIC_IS_BI(larg));
    Assert(NUMERIC_IS_BI(rarg));
    Assert(NUMERIC_BI_SCALE(larg) >= 0);
    Assert(NUMERIC_BI_SCALE(larg) <= MAXINT64DIGIT);
    Assert(NUMERIC_BI_SCALE(rarg) >= 0);
    Assert(NUMERIC_BI_SCALE(rarg) <= MAXINT64DIGIT);
}
/*
 * @Description: Calculate the result of bi64 divide bi64. The
 *               result is same with numeric_div.
 *
 * @IN larg: left-hand operand of division(/).
 * @IN rarg: right-hand operand of division(/).
 * @IN ctl: ctl is always NULL.
 * @return: Datum - the datum data points to bi128 or numeric.
 */
Datum bi64div64(Numeric larg, Numeric rarg, bictl* ctl)
{
    CheckBi64Args(larg, rarg);

    uint8 lvalscale = NUMERIC_BI_SCALE(larg);
    uint8 rvalscale = NUMERIC_BI_SCALE(rarg);
    int64 leftval = NUMERIC_64VALUE(larg);
    int64 rightval = NUMERIC_64VALUE(rarg);

    if (unlikely(rightval == 0)) {
        ereport(ERROR, (errcode(ERRCODE_DIVISION_BY_ZERO), errmsg("division by zero")));
    }

    int weight1 = 0;
    int weight2 = 0;
    int firstdigit1 = 0;
    int firstdigit2 = 0;
    /* Use the absolute value of leftval and rightval. */
    getBiWeightDigit<uint64>(((leftval >= 0) ? leftval : -leftval), lvalscale, &weight1, &firstdigit1);
    getBiWeightDigit<uint64>(((rightval >= 0) ? rightval : -rightval), rvalscale, &weight2, &firstdigit2);

    /*
     * Estimate weight of quotient.  If the two first digits are equal, we
     * can't be sure, but assume that var1 is less than var2.
     */
    int qweight = weight1 - weight2;
    if (firstdigit1 <= firstdigit2) {
        qweight--;
    }

    /* Select result scale */
    int rscale = NUMERIC_MIN_SIG_DIGITS - qweight * DEC_DIGITS;
    rscale = Max(rscale, lvalscale);
    rscale = Max(rscale, rvalscale);
    /* the reason of rscale++ is for calculate bound of last digit */
    rscale++;
    int adjustScale = rscale + rvalscale - lvalscale;
    int128 divnum = 0;
    int128 result = 0;
    bool round = false;

    if (likely(rscale <= MAXINT128DIGIT && adjustScale <= MAXINT128DIGIT &&
               !__builtin_mul_overflow(leftval, getScaleMultiplier(adjustScale), &divnum))) {
        result = divnum / rightval;
        if (result > 0) {
            round = ((result % 10) >= 5);
            result = round ? (result / 10 + 1) : (result / 10);
        } else if (result < 0) {
            round = ((result % 10) <= -5);
            result = round ? (result / 10 - 1) : (result / 10);
        }
        /* round over, rscale-- */
        rscale--;
        /* return the BI64 type result */
        if (rscale <= MAXINT64DIGIT && INT128_INT64_EQ(result)) {
            return makeNumeric64((int64)result, rscale);
        }
        /* return the BI128 type result */
        return makeNumeric128(result, rscale);
    }

    /* result is out of int128 bound, call numeric_div calculate */
    Datum args[2];
    FunctionCallInfoData finfo;
    finfo.arg = &args[0];
    args[0] = (Datum)makeNumericNormal(larg);
    args[1] = (Datum)makeNumericNormal(rarg);
    return numeric_div(&finfo);
}

/*
 * @Description: Calculate the result of bi64 divide bi64. Since
 * 				 ctl is always NULL, reduce this parameter and
 *				 make it simple for the useness of codegeneration
 *
 * @IN larg: left-hand operand of division(/).
 * @IN rarg: right-hand operand of division(/).
 * @return: Datum - the datum data points to bi128 or numeric.
 */
Datum Simplebi64div64(Numeric larg, Numeric rarg)
{
    return bi64div64(larg, rarg, NULL);
}

static inline void CheckBi128Args(Numeric larg, Numeric rarg)
{
    Assert(NUMERIC_IS_BI(larg));
    Assert(NUMERIC_IS_BI(rarg));
    Assert(NUMERIC_BI_SCALE(larg) >= 0);
    Assert(NUMERIC_BI_SCALE(larg) <= MAXINT128DIGIT);
    Assert(NUMERIC_BI_SCALE(rarg) >= 0);
    Assert(NUMERIC_BI_SCALE(rarg) <= MAXINT128DIGIT);
}

/*
 * @Description: This function can calculate the result of bi64(big integer)
 *               divide bi128(big integer), can calculate the result of bi128
 *               divide bi64, also include bi128 divide bi128.
 *
 * @IN larg: left-hand operand of division(/), when template parameter larg_is_int128
 *           is true, larg is bi128 type, else larg is bi64 type.
 * @IN rarg: right-hand operand of division(/), when template parameter rarg_is_int128
 *           is true, rarg is bi128 type, else rarg is bi64 type.
 * @IN ctl: ctl is always NULL.
 * @return: Datum - the datum data points to bi128 or binumeric.
 */
template <bool larg_is_int128, bool rarg_is_int128>
Datum bi128div128(Numeric larg, Numeric rarg, bictl* ctl)
{
    CheckBi128Args(larg, rarg);

    uint8 lvalscale = NUMERIC_BI_SCALE(larg);
    uint8 rvalscale = NUMERIC_BI_SCALE(rarg);
    int128 leftval = 0;
    int128 rightval = 0;

    /* get left val */
    Int128GetVal(larg_is_int128, &leftval, &larg);
    /* get right val */
    Int128GetVal(rarg_is_int128, &rightval, &rarg);

    if (unlikely(rightval == 0)) {
        ereport(ERROR, (errcode(ERRCODE_DIVISION_BY_ZERO), errmsg("division by zero")));
    }

    int weight1 = 0;
    int weight2 = 0;
    int firstdigit1 = 0;
    int firstdigit2 = 0;
    /* Use the absolute value of leftval and rightval. */
    getBiWeightDigit<uint128>(((leftval >= 0) ? leftval : -leftval), lvalscale, &weight1, &firstdigit1);
    getBiWeightDigit<uint128>(((rightval >= 0) ? rightval : -rightval), rvalscale, &weight2, &firstdigit2);

    /*
     * Estimate weight of quotient.  If the two first digits are equal, we
     * can't be sure, but assume that var1 is less than var2.
     */
    int qweight = weight1 - weight2;
    if (firstdigit1 <= firstdigit2) {
        qweight--;
    }

    /* Select result scale */
    int rscale = NUMERIC_MIN_SIG_DIGITS - qweight * DEC_DIGITS;
    rscale = Max(rscale, lvalscale);
    rscale = Max(rscale, rvalscale);
    /* the reason of rscale++ is for calculate bound of last digit */
    rscale++;
    int adjustScale = rscale + rvalscale - lvalscale;
    int128 divnum = 0;
    int128 result = 0;

    if (likely(rscale <= MAXINT128DIGIT && adjustScale <= MAXINT128DIGIT &&
               !__builtin_mul_overflow(leftval, getScaleMultiplier(adjustScale), &divnum))) {
        result = divnum / rightval;
        if (result > 0) {
            result = ((result % 10) >= 5) ? (result / 10 + 1) : (result / 10);
        } else if (result < 0) {
            result = ((result % 10) <= -5) ? (result / 10 - 1) : (result / 10);
        }
        /* round over, rscale-- */
        rscale--;
        /* return the BI64 type result */
        if (rscale <= MAXINT64DIGIT && INT128_INT64_EQ(result)) {
            return makeNumeric64((int64)result, rscale);
        }
        /* return the BI128 type result */
        return makeNumeric128(result, rscale);
    }

    /* result is out of int128 bound, call numeric_div calculate */
    Datum args[2];
    FunctionCallInfoData finfo;
    finfo.arg = &args[0];
    args[0] = (Datum)makeNumericNormal(larg);
    args[1] = (Datum)makeNumericNormal(rarg);
    return numeric_div(&finfo);
}

/*
 * @Description: Calculate the result of bi64 compares to bi64.
 *
 * @IN op: template parameter, it is used for distinguish compartion operators
 *         include =, !=, <=, <, >=, >.
 * @IN larg: left-hand operand of comparison.
 * @IN rarg: right-hand operand of comparison.
 * @IN ctl: ctl is always NULL.
 * @return: Datum - the boolean result.
 */
template <biop op>
Datum bi64cmp64(Numeric larg, Numeric rarg, bictl* ctl)
{
    Assert(NUMERIC_IS_BI(larg));
    Assert(NUMERIC_IS_BI(rarg));

    uint8 lvalscale = NUMERIC_BI_SCALE(larg);
    uint8 rvalscale = NUMERIC_BI_SCALE(rarg);
    int64 leftval = NUMERIC_64VALUE(larg);
    int64 rightval = NUMERIC_64VALUE(rarg);
    int64 leftval_scaled = 0;
    int64 right_scaled = 0;
    int resScale = 0;
    int result = 0;

    /* Adjust leftval and rightval to the same scale */
    BiAdjustScale overflow =
        adjustBi64ToSameScale(leftval, lvalscale, rightval, rvalscale, &resScale, &leftval_scaled, &right_scaled);

    /* leftval_scaled and right_scaled both don't overflow, compare directly */
    if (overflow == BI_AJUST_TRUE) {
        /* compare leftval with rightval directly, both are int64 type */
        switch (op) {
            case BIEQ:  // =
                result = (leftval_scaled == right_scaled);
                PG_RETURN_INT32(result);
            case BINEQ:  // !=
                result = (leftval_scaled != right_scaled);
                PG_RETURN_INT32(result);
            case BILE:  // <=
                result = (leftval_scaled <= right_scaled);
                PG_RETURN_INT32(result);
            case BILT:  // <
                result = (leftval_scaled < right_scaled);
                PG_RETURN_INT32(result);
            case BIGE:  // >=
                result = (leftval_scaled >= right_scaled);
                PG_RETURN_INT32(result);
            case BIGT:  // >
                result = (leftval_scaled > right_scaled);
                PG_RETURN_INT32(result);
            case BICMP:  // > || == || <
                result = ((leftval_scaled > right_scaled) ? 1 : ((leftval_scaled < right_scaled) ? -1 : 0));
                PG_RETURN_INT32(result);
            default:
                elog(LOG, "undefined big integer operator.");
                break;
        }
    } else if (overflow == BI_RIGHT_OUT_OF_BOUND) {
        /* right_scaled must be out of int64 bound, so leftval_scaled != right_scaled */
        switch (op) {
            case BIEQ:  // =
                result = false;
                PG_RETURN_INT32(result);
            case BINEQ:  // !=
                result = true;
                PG_RETURN_INT32(result);
            case BILE:  // <=
                result = ((int128)leftval <= (int128)rightval * getScaleMultiplier(resScale - rvalscale));
                PG_RETURN_INT32(result);
            case BILT:  // <
                result = ((int128)leftval < (int128)rightval * getScaleMultiplier(resScale - rvalscale));
                PG_RETURN_INT32(result);
            case BIGE:  // >=
                result = ((int128)leftval >= (int128)rightval * getScaleMultiplier(resScale - rvalscale));
                PG_RETURN_INT32(result);
            case BIGT:  // >
                result = ((int128)leftval > (int128)rightval * getScaleMultiplier(resScale - rvalscale));
                PG_RETURN_INT32(result);
            case BICMP: {
                int128 right_scaled_128 = rightval * getScaleMultiplier(resScale - rvalscale);
                result = ((leftval > right_scaled_128) ? 1 : ((leftval < right_scaled_128) ? -1 : 0));
                PG_RETURN_INT32(result);
            }
            default:
                elog(LOG, "undefined big integer operator.");
                break;
        }
    } else {
        /* leftval_scaled must be out of int64 bound, so leftval_scaled != right_scaled */
        // overflow equals to "BI_LEFT_OUT_OF_BOUND"
        switch (op) {
            case BIEQ:  // =
                result = false;
                PG_RETURN_INT32(result);
            case BINEQ:  // !=
                result = true;
                PG_RETURN_INT32(result);
            case BILE:  // <=
                result = ((int128)leftval * getScaleMultiplier(resScale - lvalscale) <= (int128)rightval);
                PG_RETURN_INT32(result);
            case BILT:  // <
                result = ((int128)leftval * getScaleMultiplier(resScale - lvalscale) < (int128)rightval);
                PG_RETURN_INT32(result);
            case BIGE:  // >=
                result = ((int128)leftval * getScaleMultiplier(resScale - lvalscale) >= (int128)rightval);
                PG_RETURN_INT32(result);
            case BIGT:  // >
                result = ((int128)leftval * getScaleMultiplier(resScale - lvalscale) > (int128)rightval);
                PG_RETURN_INT32(result);
            case BICMP: {
                int128 left_scaled_128 = leftval * getScaleMultiplier(resScale - lvalscale);
                result = ((left_scaled_128 > rightval) ? 1 : ((left_scaled_128 < rightval) ? -1 : 0));
                PG_RETURN_INT32(result);
            }
            default:
                elog(LOG, "undefined big integer operator.");
                break;
        }
    }
    return Datum(0);
}

/*
 * @Description: This function can calculate the result of bi64(big integer)
 *               compares with bi128(big integer), can calculate the result
 *               of bi128 compares with bi64, also include bi128 compares
 *               with bi128.
 *
 * @IN op: template parameter, it is used for distinguish compartion operators
 *         include =, !=, <=, <, >=, >.
 * @IN larg: left-hand operand of comparison operator, when template parameter
 *           larg_is_int128 is true, larg is bi128 type, else larg is bi64 type.
 * @IN rarg: right-hand operand of comparison operator, when template parameter
 *           rarg_is_int128 is true, rarg is bi128 type, else rarg is bi64 type.
 * @IN ctl: ctl is always NULL.
 * @return: Datum - the boolean data.
 */
template <biop op, bool larg_is_int128, bool rarg_is_int128>
Datum bi128cmp128(Numeric larg, Numeric rarg, bictl* ctl)
{
    Assert(NUMERIC_IS_BI(larg));
    Assert(NUMERIC_IS_BI(rarg));

    uint8 lvalscale = NUMERIC_BI_SCALE(larg);
    uint8 rvalscale = NUMERIC_BI_SCALE(rarg);
    int128 leftval = 0;
    int128 rightval = 0;
    int128 leftval_scaled = 0;
    int128 right_scaled = 0;
    int resScale = 0;
    int result = 0;

    /* get left val */
    Int128GetVal(larg_is_int128, &leftval, &larg);
    /* get right val */
    Int128GetVal(rarg_is_int128, &rightval, &rarg);

    /* Adjust leftval and rightval to the same scale */
    BiAdjustScale overflow =
        adjustBi128ToSameScale(leftval, lvalscale, rightval, rvalscale, &resScale, &leftval_scaled, &right_scaled);
    /* leftval_scaled and right_scaled both don't overflow, compare directly */
    if (overflow == BI_AJUST_TRUE) {
        /* compare leftval with rightval directly, both are int128 type */
        switch (op) {
            case BIEQ:  // =
                result = (leftval_scaled == right_scaled);
                PG_RETURN_INT32(result);
            case BINEQ:  // !=
                result = (leftval_scaled != right_scaled);
                PG_RETURN_INT32(result);
            case BILE:  // <=
                result = (leftval_scaled <= right_scaled);
                PG_RETURN_INT32(result);
            case BILT:  // <
                result = (leftval_scaled < right_scaled);
                PG_RETURN_INT32(result);
            case BIGE:  // >=
                result = (leftval_scaled >= right_scaled);
                PG_RETURN_INT32(result);
            case BIGT:  // >
                result = (leftval_scaled > right_scaled);
                PG_RETURN_INT32(result);
            case BICMP:
                result = ((leftval_scaled > right_scaled) ? 1 : ((leftval_scaled < right_scaled) ? -1 : 0));
                PG_RETURN_INT32(result);
            default:
                elog(LOG, "undefined big integer operator.");
                break;
        }
    } else {
        /*
         * BI_LEFT_OUT_OF_BOUND OR BI_RIGHT_OUT_OF_BOUND
         * leftval or rightval must be out of int128 bound, so leftval != rightval
         */
        if (op == BIEQ) {
            result = false;
            PG_RETURN_INT32(result);
        } else if (op == BINEQ) {
            result = true;
            PG_RETURN_INT32(result);
        } else {
            /* can't compare directly, call cmp_numerics */
            Numeric num1 = makeNumericNormal(larg);
            Numeric num2 = makeNumericNormal(rarg);
            switch (op) {
                case BILE:  // <=
                    result = cmp_numerics(num1, num2) <= 0;
                    PG_RETURN_INT32(result);
                case BILT:  // <
                    result = cmp_numerics(num1, num2) < 0;
                    PG_RETURN_INT32(result);
                case BIGE:  // >=
                    result = cmp_numerics(num1, num2) >= 0;
                    PG_RETURN_INT32(result);
                case BIGT:  // >
                    result = cmp_numerics(num1, num2) > 0;
                    PG_RETURN_INT32(result);
                case BICMP:
                    result = cmp_numerics(num1, num2);
                    PG_RETURN_INT32(result);
                default:
                    elog(LOG, "undefined big integer operator.");
                    break;
            }
        }
    }
    return (Datum)0;
}

/*
 * @Description: get the smaller one of two bi64 data.
 *
 * @IN larg: bi64 data, larg.
 * @IN rarg: bi64 data, rarg.
 * @IN ctl: vechashtable info, copy the smaller data to hash table.
 * @return: Datum - always 0.
 */
static Datum bi64cmp64_smaller(Numeric larg, Numeric rarg, bictl* ctl)
{
    Assert(NUMERIC_IS_BI(larg));
    Assert(NUMERIC_IS_BI(rarg));

    uint8 lvalscale = NUMERIC_BI_SCALE(larg);
    uint8 rvalscale = NUMERIC_BI_SCALE(rarg);
    int64 leftval = NUMERIC_64VALUE(larg);
    int64 rightval = NUMERIC_64VALUE(rarg);
    int64 leftval_scaled = 0;
    int64 right_scaled = 0;
    int resScale = 0;

    /* Adjust leftval and rightval to the same scale */
    BiAdjustScale overflow =
        adjustBi64ToSameScale(leftval, lvalscale, rightval, rvalscale, &resScale, &leftval_scaled, &right_scaled);

    switch (overflow) {
        /* leftval_scaled and right_scaled both don't overflow, compare directly */
        case BI_AJUST_TRUE:
            if (leftval_scaled <= right_scaled) {
                /* no need to update hash table value, return directly */
                return (Datum)0;
            }
            break;

        /* right_scaled must be out of int64 bound */
        case BI_RIGHT_OUT_OF_BOUND:
            if ((int128)leftval <= (int128)rightval * getScaleMultiplier(resScale - rvalscale)) {
                /* no need to update hash table value, return directly */
                return (Datum)0;
            }
            break;

        /* leftval_scaled must be out of int64 bound */
        default:  // BI_LEFT_OUT_OF_BOUND
            if ((int128)leftval * getScaleMultiplier(resScale - lvalscale) <= (int128)rightval) {
                /* no need to update hash table value, return directly */
                return (Datum)0;
            }
            break;
    }

    /* leftval is smaller, no need to update ctl->store_pos,
     * because ctl->store_pos stores the leftval, return directly.
     * else, copy the rightval to ctl->store_pos.
     */
    Numeric res = (Numeric)ctl->store_pos;
    /* Set result scale */
    res->choice.n_header = NUMERIC_64 + rvalscale;
    /* Set result value */
    *((int64*)(&res->choice.n_bi.n_data[0])) = rightval;
    return (Datum)0;
}

/*
 * @Description: get the larger one of two bi64 data.
 *
 * @IN larg: bi64 data, larg.
 * @IN rarg: bi64 data, rarg.
 * @IN ctl: vechashtable info, copy the larger data to hash table.
 * @return: Datum - always 0.
 */
static Datum bi64cmp64_larger(Numeric larg, Numeric rarg, bictl* ctl)
{
    Assert(NUMERIC_IS_BI(larg));
    Assert(NUMERIC_IS_BI(rarg));

    uint8 lvalscale = NUMERIC_BI_SCALE(larg);
    uint8 rvalscale = NUMERIC_BI_SCALE(rarg);
    int64 leftval = NUMERIC_64VALUE(larg);
    int64 rightval = NUMERIC_64VALUE(rarg);
    int64 leftval_scaled = 0;
    int64 right_scaled = 0;
    int resScale = 0;

    /* Adjust leftval and rightval to the same scale */
    BiAdjustScale overflow =
        adjustBi64ToSameScale(leftval, lvalscale, rightval, rvalscale, &resScale, &leftval_scaled, &right_scaled);

    switch (overflow) {
        /* leftval_scaled and right_scaled both don't overflow, compare directly */
        case BI_AJUST_TRUE:
            if (leftval_scaled >= right_scaled) {
                /* no need to update hash table value, return directly */
                return (Datum)0;
            }
            break;

        /* right_scaled must be out of int64 bound */
        case BI_RIGHT_OUT_OF_BOUND:
            if ((int128)leftval >= (int128)rightval * getScaleMultiplier(resScale - rvalscale)) {
                /* no need to update hash table value, return directly */
                return (Datum)0;
            }
            break;

        /* leftval_scaled must be out of int64 bound */
        default:  // BI_LEFT_OUT_OF_BOUND
            if ((int128)leftval * getScaleMultiplier(resScale - lvalscale) >= (int128)rightval) {
                /* no need to update hash table value, return directly */
                return (Datum)0;
            }
            break;
    }

    /* if leftval is larger, no need to update ctl->store_pos,
     * because ctl->store_pos stores the leftval, return directly.
     * else, copy the rightval to ctl->store_pos.
     */
    Numeric res = (Numeric)ctl->store_pos;
    /* Set result scale */
    res->choice.n_header = NUMERIC_64 + rvalscale;
    /* Set result value */
    *((int64*)(&res->choice.n_bi.n_data[0])) = rightval;
    return (Datum)0;
}

/*
 * @Description: get the smaller one of two big integer data(bi64 or bi128).
 *
 * @IN larg: larg(bi64 or bi128 data), when template parameter larg_is_int128
 *           is true, larg is bi128 type, else larg is bi64 type.
 * @IN rarg: rarg(bi64 or bi128 data), when template parameter rarg_is_int128
 *           is true, rarg is bi128 type, else rarg is bi64 type.
 * @IN ctl: vechashtable info, copy the smaller data to hash table.
 * @return: Datum - always 0.
 */
template <bool larg_is_int128, bool rarg_is_int128>
Datum bi128cmp128_smaller(Numeric larg, Numeric rarg, bictl* ctl)
{
    Assert(NUMERIC_IS_BI(larg));
    Assert(NUMERIC_IS_BI(rarg));

    uint8 lvalscale = NUMERIC_BI_SCALE(larg);
    uint8 rvalscale = NUMERIC_BI_SCALE(rarg);
    int128 leftval = 0;
    int128 rightval = 0;
    int128 leftval_scaled = 0;
    int128 right_scaled = 0;
    int resScale = 0;

    /* get left val */
    Int128GetVal(larg_is_int128, &leftval, &larg);
    /* get right val */
    Int128GetVal(rarg_is_int128, &rightval, &rarg);

    /* Adjust leftval and rightval to the same scale */
    BiAdjustScale overflow =
        adjustBi128ToSameScale(leftval, lvalscale, rightval, rvalscale, &resScale, &leftval_scaled, &right_scaled);

    switch (overflow) {
        /* leftval_scaled and right_scaled both don't overflow, compare directly */
        case BI_AJUST_TRUE:
            if (leftval_scaled <= right_scaled) {
                /* no need to update hash table value, return directly */
                return (Datum)0;
            }
            break;

        /* leftval_scaled or rightval_scaled must be out of int128 bound */
        default:  // BI_LEFT_OUT_OF_BOUND | BI_RIGHT_OUT_OF_BOUND
            /* can't compare directly, need function call */
            Datum args[2];
            FunctionCallInfoData finfo;
            finfo.arg = &args[0];
            args[0] = NumericGetDatum(larg);
            args[1] = NumericGetDatum(rarg);
            Datum res = numeric_smaller(&finfo);
            if (res != ctl->store_pos) {
                ctl->store_pos = replaceVariable(ctl->context, ctl->store_pos, res);
            }
            return (Datum)0;
    }

    /* leftval is smaller, no need to update ctl->store_pos,
     * because ctl->store_pos stores the leftval, return directly.
     * else, copy the rightval to ctl->store_pos.
     */
    if (larg_is_int128) {
        Numeric result = (Numeric)ctl->store_pos;
        /* Set result scale */
        result->choice.n_header = NUMERIC_128 + rvalscale;
        /* Set result value */
        errno_t rc = memcpy_s(result->choice.n_bi.n_data, sizeof(int128), &rightval, sizeof(int128));
        securec_check(rc, "\0", "\0");
        return (Datum)0;
    } else {
        /* larg is int 64, ctl->store_pos can't store int128 data. */
        ctl->store_pos = replaceVariable(ctl->context, ctl->store_pos, NumericGetDatum(rarg));
        return (Datum)0;
    }
}

/*
 * @Description: get the larger one of two big integer data(bi64 or bi128).
 *
 * @IN larg: larg(bi64 or bi128 data), when template parameter larg_is_int128
 *           is true, larg is bi128 type, else larg is bi64 type.
 * @IN rarg: rarg(bi64 or bi128 data), when template parameter rarg_is_int128
 *           is true, rarg is bi128 type, else rarg is bi64 type.
 * @IN ctl: vechashtable info, copy the larger data to hash table.
 * @return: Datum - always 0.
 */
template <bool larg_is_int128, bool rarg_is_int128>
Datum bi128cmp128_larger(Numeric larg, Numeric rarg, bictl* ctl)
{
    Assert(NUMERIC_IS_BI(larg));
    Assert(NUMERIC_IS_BI(rarg));

    uint8 lvalscale = NUMERIC_BI_SCALE(larg);
    uint8 rvalscale = NUMERIC_BI_SCALE(rarg);
    int128 leftval = 0;
    int128 rightval = 0;
    int128 leftval_scaled = 0;
    int128 right_scaled = 0;
    int resScale = 0;

    /* get left val */
    Int128GetVal(larg_is_int128, &leftval, &larg);
    /* get right val */
    Int128GetVal(rarg_is_int128, &rightval, &rarg);

    /* Adjust leftval and rightval to the same scale */
    BiAdjustScale overflow =
        adjustBi128ToSameScale(leftval, lvalscale, rightval, rvalscale, &resScale, &leftval_scaled, &right_scaled);

    switch (overflow) {
        /* leftval_scaled and right_scaled both don't overflow, compare directly */
        case BI_AJUST_TRUE:
            if (leftval_scaled >= right_scaled) {
                /* no need to update hash table value, return directly */
                return (Datum)0;
            }
            break;

        /* leftval_scaled or rightval_scaled must be out of int128 bound */
        default:  // BI_LEFT_OUT_OF_BOUND | BI_RIGHT_OUT_OF_BOUND
            /* can't compare directly, need function call */
            Datum args[2];
            FunctionCallInfoData finfo;
            finfo.arg = &args[0];
            args[0] = NumericGetDatum(larg);
            args[1] = NumericGetDatum(rarg);
            Datum res = numeric_larger(&finfo);
            if (res != ctl->store_pos) {
                /* only update hash table value when res and ctl->store_pos are not equal */
                ctl->store_pos = replaceVariable(ctl->context, ctl->store_pos, res);
            }
            return (Datum)0;
    }

    /* leftval is smaller, no need to update ctl->store_pos,
     * because ctl->store_pos stores the leftval, return directly.
     * else, copy the rightval to ctl->store_pos.
     */
    if (larg_is_int128) {
        Numeric result = (Numeric)ctl->store_pos;
        /* Set result scale */
        result->choice.n_header = NUMERIC_128 + rvalscale;
        /* Set result value */
        errno_t rc = memcpy_s(result->choice.n_bi.n_data, sizeof(int128), &rightval, sizeof(int128));
        securec_check(rc, "\0", "\0");
        return (Datum)0;
    } else {
        /* larg is int 64, ctl->store_pos can't store int128 data. */
        ctl->store_pos = replaceVariable(ctl->context, ctl->store_pos, NumericGetDatum(rarg));
        return (Datum)0;
    }
}

/*
 * three dimensional arrays: BiFunMatrix
 * this array is used for fast locate the corresponding operation
 * functions by operator and type(int64 or int128)
 */
const biopfun BiFunMatrix[11][2][2] = {
    /* addition operator functions for big integer */
    {{bi64add64<false>, bi128add128<false, true, false>},
        {bi128add128<true, false, false>, bi128add128<true, true, false>}},
    /* subtraction operator functions for big integer */
    {{bi64sub64, bi128sub128<false, true>}, {bi128sub128<true, false>, bi128sub128<true, true>}},
    /* multiplication operator functions for big integer */
    {{bi64mul64, bi128mul128<false, true>}, {bi128mul128<true, false>, bi128mul128<true, true>}},
    /* division operator functions for big integer */
    {{bi64div64, bi128div128<false, true>}, {bi128div128<true, false>, bi128div128<true, true>}},
    /* comparison operator(=) functions for big integer */
    {{bi64cmp64<BIEQ>, bi128cmp128<BIEQ, false, true>},
        {bi128cmp128<BIEQ, true, false>, bi128cmp128<BIEQ, true, true>}},
    /* comparison operator(!=) functions for big integer */
    {{bi64cmp64<BINEQ>, bi128cmp128<BINEQ, false, true>},
        {bi128cmp128<BINEQ, true, false>, bi128cmp128<BINEQ, true, true>}},
    /* comparison operator(<=) functions for big integer */
    {{bi64cmp64<BILE>, bi128cmp128<BILE, false, true>},
        {bi128cmp128<BILE, true, false>, bi128cmp128<BILE, true, true>}},
    /* comparison operator(<) functions for big integer */
    {{bi64cmp64<BILT>, bi128cmp128<BILT, false, true>},
        {bi128cmp128<BILT, true, false>, bi128cmp128<BILT, true, true>}},
    /* comparison operator(>=) functions for big integer */
    {{bi64cmp64<BIGE>, bi128cmp128<BIGE, false, true>},
        {bi128cmp128<BIGE, true, false>, bi128cmp128<BIGE, true, true>}},
    /* comparison operator(>) functions for big integer */
    {{bi64cmp64<BIGT>, bi128cmp128<BIGT, false, true>},
        {bi128cmp128<BIGT, true, false>, bi128cmp128<BIGT, true, true>}},
    /* comparison operator(> OR == OR <) functions for big integer */
    {{bi64cmp64<BICMP>, bi128cmp128<BICMP, false, true>},
        {bi128cmp128<BICMP, true, false>, bi128cmp128<BICMP, true, true>}}};

/*
 * three dimensional arrays: BiAggFunMatrix
 * this array is used for fast locate the corresponding agg
 * functions by operator and type(int64 or int128)
 */
const biopfun BiAggFunMatrix[3][2][2] = {
    /* agg add functions for vnumeric sum|avg */
    {{bi64add64<true>, bi128add128<false, true, true>},
        {bi128add128<true, false, true>, bi128add128<true, true, true>}},
    /* agg smaller functions for vnumeric_min */
    {{bi64cmp64_smaller, bi128cmp128_smaller<false, true>},
        {bi128cmp128_smaller<true, false>, bi128cmp128_smaller<true, true>}},
    /* agg larger functions for vnumeric_max */
    {{bi64cmp64_larger, bi128cmp128_larger<false, true>},
        {bi128cmp128_larger<true, false>, bi128cmp128_larger<true, true>}}};

/*
 * @Description: convert data(bi64, bi128 or numeric) to numeric type
 *
 * @IN  arg: numeric data
 * @Return:  the numeric result
 */
Numeric bitonumeric(Datum arg)
{
    Numeric val = DatumGetNumeric(arg);

    if (NUMERIC_IS_BI(val)) {
        return makeNumericNormal(val);
    }
    return val;
}

/*
 * @Description: convert int1 to big integer type
 *
 * @IN  arg: int type data
 * @Return:  the Datum point to bi64 type
 */
Datum int1_numeric_bi(PG_FUNCTION_ARGS)
{
    /* convert int1 to int8 */
    int64 val = PG_GETARG_UINT8(0);
    /* make bi64 data */
    return makeNumeric64(val, 0);
}

/*
 * @Description: convert int2 to big integer type
 *
 * @IN  arg: int type data
 * @Return:  the Datum point to bi64 type
 */
Datum int2_numeric_bi(PG_FUNCTION_ARGS)
{
    /* convert int2 to int8 */
    int64 val = PG_GETARG_INT16(0);
    /* make bi64 data */
    return makeNumeric64(val, 0);
}

/*
 * @Description: convert int4 to big integer type
 *
 * @IN  arg: int type data
 * @Return:  the Datum point to bi64 type
 */
Datum int4_numeric_bi(PG_FUNCTION_ARGS)
{
    /* convert int4 to int8 */
    int64 val = PG_GETARG_INT32(0);
    /* make bi64 data */
    return makeNumeric64(val, 0);
}

/*
 * @Description: convert int8 to big integer type
 *
 * @IN  arg: int type data
 * @Return:  the Datum point to bi64 type
 */
Datum int8_numeric_bi(PG_FUNCTION_ARGS)
{
    /* convert int8 to int8 */
    int64 val = PG_GETARG_INT64(0);
    /* make bi64 data */
    return makeNumeric64(val, 0);
}

/*
 * @Description: calculate the hash value of specified
 *               int64 and scale.
 *
 * @IN num: int64 value of bi64(big integer).
 * @IN scale: scale of bi64(big integer).
 * @Return:  the hash value
 */
static inline Datum int64_hash_bi(int64 num, int scale)
{
    /* handle num = 0 case */
    if (num == 0) {
        return hash_uint32(0) ^ 0;
    }

    /* Normalize num: omit any trailing zeros from the input to the hash */
    while ((num % 10) == 0 && scale > 0) {
        num /= 10;
        scale--;
    }

    uint32 lohalf = (uint32)num;
    uint32 hihalf = (uint32)((uint64)num >> 32);

    lohalf ^= (num >= 0) ? hihalf : ~hihalf;
    return hash_uint32(lohalf) ^ (uint32)scale;
}

/*
 * @Description: calculate the hash value of specified
 *               int128 and scale.
 *
 * @IN num: int128 value of bi128(big integer).
 * @IN scale: scale of bi128(big integer).
 * @Return:  the hash value
 */
static inline Datum int128_hash_bi(int128 num, int scale)
{
    /* handle num = 0 case */
    if (num == 0) {
        return hash_uint32(0) ^ 0;
    }

    /* Normalize num: omit any trailing zeros from the input to the hash */
    while ((num % 10) == 0 && scale > 0) {
        num /= 10;
        scale--;
    }
    /* call bi64_hash */
    if (INT128_INT64_EQ(num)) {
        return int64_hash_bi(num, scale);
    }

    uint64 lohalf64 = (uint64)num;
    uint64 hihalf64 = (uint64)((uint128)num >> 64);

    lohalf64 ^= (num >= 0) ? hihalf64 : ~hihalf64;

    uint32 lohalf32 = (uint32)lohalf64;
    uint32 hihalf32 = (uint32)(lohalf64 >> 32);

    lohalf32 ^= (num >= 0) ? hihalf32 : ~hihalf32;
    return hash_uint32(lohalf32) ^ (uint32)scale;
}

/*
 * @Description: inline function, call hash_numeric(PG_FUNCTION_ARGS)
 *
 * @IN num: numeric type data
 * @Return:  the hash value
 */
static inline Datum call_hash_numeric(Numeric num)
{
    /* call hash_numeric here */
    Datum args[1];
    FunctionCallInfoData finfo;
    finfo.arg = &args[0];
    args[0] = NumericGetDatum(num);
    return hash_numeric(&finfo);
}

/*
 * @Description: calculate the hash value of numeric data,
 *               make sure the same value with diferent types
 *               have the same hash value.
 *               e.g. int64   1.00->(100, 2)
 *                    int128  1.0000->(10000, 4)
 *                    numeric 1.0000000000000000
 *               they must have the same hash value
 *
 * @IN num: int128 value of bi128(big integer).
 * @Return:  the hash value
 */
static Datum numeric_hash_bi(Numeric num)
{
    /* If it's NaN, don't try to hash the rest of the fields */
    if (NUMERIC_IS_NAN(num)) {
        PG_RETURN_UINT32(0);
    }

    int ndigits = NUMERIC_NDIGITS(num);

    /*
     * If there are no non-zero digits, then the value of the number is zero,
     * regardless of any other fields.
     */
    if (ndigits == 0) {
        return hash_uint32(0) ^ 0;
    }

    NumericDigit* digits = NUMERIC_DIGITS(num);

    Assert(ndigits > 0);
    Assert(digits[0] != 0);
    Assert(digits[ndigits - 1] != 0);

    int weight = NUMERIC_WEIGHT(num);
    int dscale = NUMERIC_DSCALE(num);
    int num_sign = (NUMERIC_SIGN(num) == NUMERIC_POS) ? 1 : -1;
    int last_digit = 0;
    int last_base = 0;
    int i = 0;
    int128 data = 0;
    int128 mul_tmp_data = 0;
    int128 add_tmp_data = 0;

    /* numeric data can't convert to int128, call hash_numeric */
    if (weight >= 10) {
        return call_hash_numeric(num);
    }

    /* step1. numeric can convert to int128 */
    if ((weight >= 0 && CAN_CONVERT_BI128(weight * DEC_DIGITS + DEC_DIGITS + dscale)) ||
        (weight < 0 && CAN_CONVERT_BI128(dscale))) {
        convert_short_numeric_to_int128_byscale(num, dscale, data);
        return int128_hash_bi(data, dscale);
    }

    /* step2. numeric data have only pre data
     * e.g. numeric data->100.0000
     */
    if (weight >= 0 && (weight + 1) >= ndigits) {
        for (i = 0; i < ndigits; i++) {
            if (!__builtin_mul_overflow(
                    num_sign * digits[i], getScaleMultiplier((weight - i) * DEC_DIGITS), &mul_tmp_data)) {
                if (!__builtin_add_overflow(data, mul_tmp_data, &add_tmp_data)) {
                    data = add_tmp_data;
                } else {
                    return call_hash_numeric(num);
                }
            } else {
                return call_hash_numeric(num);
            }
        }
        return int128_hash_bi(data, 0);
    }

    /* step3. delete the trailing zeros of last digit
     * e.g. numeric(8,4) value is 2.1000, we should delete
     * the last 3 zeros, the last value is (value:21, scale:1)
     */
    /* update dscale(e.g. numeric(100,50) data=1.1000 dscale = 4) */
    dscale = (ndigits - weight - 1) * DEC_DIGITS;

    i = ndigits - 1;
    if (digits[i] % 1000 == 0) {
        last_digit = digits[i] / 1000;
        last_base = NBASE / 1000;
        dscale = dscale - 3;
    } else if (digits[i] % 100 == 0) {
        last_digit = digits[i] / 100;
        last_base = NBASE / 100;
        dscale = dscale - 2;
    } else if (digits[i] % 10 == 0) {
        last_digit = digits[i] / 10;
        last_base = NBASE / 10;
        dscale = dscale - 1;
    } else {
        last_digit = digits[i];
        last_base = NBASE;
    }

    /* step4. calculate the hash of type a.b(like: 123.1000 or 0.12301200).
     * if data is out of int128 bound, call hash_numeric directly.
     */
    for (i = 0; i < (ndigits - 1); i++) {
        if (!__builtin_mul_overflow(data, NBASE, &mul_tmp_data)) {
            if (!__builtin_add_overflow(digits[i] * num_sign, mul_tmp_data, &add_tmp_data)) {
                data = add_tmp_data;
            } else {
                return call_hash_numeric(num);
            }
        } else {
            return call_hash_numeric(num);
        }
    }
    /* calculate the last digit carefully to avoid out of int128 bound */
    if (!__builtin_mul_overflow(data, last_base, &mul_tmp_data)) {
        if (!__builtin_add_overflow(last_digit * num_sign, mul_tmp_data, &add_tmp_data)) {
            data = add_tmp_data;
        } else {
            return call_hash_numeric(num);
        }
    } else {
        return call_hash_numeric(num);
    }

    return int128_hash_bi(data, dscale);
}

/*
 * @Description: calculate the hash value of big integer
 *               (int64/int128/numeric type).
 *
 * @IN num: big integer.
 * @Return:  the hash value
 */
Datum hash_bi(PG_FUNCTION_ARGS)
{
    Numeric key = PG_GETARG_NUMERIC(0);
    uint16 keyFlag = NUMERIC_NB_FLAGBITS(key);
    uint8 keyScale = NUMERIC_BI_SCALE(key);

    /* Numeric is int64 */
    if (NUMERIC_FLAG_IS_BI64(keyFlag)) {
        /*
         * If the value of the number is zero, return -1 directly,
         * the same with hash_numeric.
         */
        int64 keyValue = NUMERIC_64VALUE(key);
        return int64_hash_bi(keyValue, keyScale);
    } else if (NUMERIC_FLAG_IS_BI128(keyFlag)) {
        /*
         * Numeric is int128
         * If the value of the number is zero, return -1 directly,
         * the same with hash_numeric.
         */
        int128 keyValue = 0;
        errno_t rc = memcpy_s(&keyValue, sizeof(int128), (key)->choice.n_bi.n_data, sizeof(int128));
        securec_check(rc, "\0", "\0");
        return int128_hash_bi(keyValue, keyScale);
    }
    /* Normal numeric */
    return numeric_hash_bi(key);
}

Datum hash_bi_key(Numeric key)
{
    uint16 keyFlag = NUMERIC_NB_FLAGBITS(key);
    uint8 keyScale = NUMERIC_BI_SCALE(key);

    /* Numeric is int64 */
    if (NUMERIC_FLAG_IS_BI64(keyFlag)) {
        /*
         * If the value of the number is zero, return -1 directly,
         * the same with hash_numeric.
         */
        int64 keyValue = NUMERIC_64VALUE(key);
        return int64_hash_bi(keyValue, keyScale);
    } else if (NUMERIC_FLAG_IS_BI128(keyFlag)) {
        /*
         * Numeric is int128
         * If the value of the number is zero, return -1 directly,
         * the same with hash_numeric.
         */
        int128 keyValue = 0;
        errno_t rc = memcpy_s(&keyValue, sizeof(int128), (key)->choice.n_bi.n_data, sizeof(int128));
        securec_check(rc, "\0", "\0");
        return int128_hash_bi(keyValue, keyScale);
    }
    /* Normal numeric */
    return numeric_hash_bi(key);
}

/*
 * @Description: replace numeric hash function to bihash function for
 *               vec hash agg and hash join.
 *
 * @IN numCols: num columns
 * @OUT hashFunctions: replace the corresponding hash function
 */
void replace_numeric_hash_to_bi(int numCols, FmgrInfo* hashFunctions)
{
    int i = 0;

    for (i = 0; i < numCols; i++) {
        /* replace hash_numeric to hash_bi */
        if (hashFunctions[i].fn_addr == hash_numeric) {
            hashFunctions[i].fn_addr = hash_bi;
        }
    }
}

/*
 * @Description: print bi64 data to string like numeric_out.
 *
 * @IN  data: int64 data
 * @IN  scale:  the scale of bi64 data
 * @Return: Output string for numeric data type
 */
Datum bi64_out(int64 data, int scale)
{
    Assert(scale >= 0);
    Assert(scale <= MAXINT64DIGIT);
    char buf[MAXBI64LEN];
    char* result = NULL;
    uint64 val_u64 = 0;

    int rc = -1;
    /* data equals to "INT64_MIN" */
    if (unlikely(data == (-INT64CONST(0x7FFFFFFFFFFFFFFF) - 1))) {
        /*
         * Avoid problems with the most negative integer not being representable
         * as a positive integer.
         */
        if (scale > 0) {
            int len = strlen(int64_min_str) - scale;
            rc = memcpy_s(buf, MAXBI64LEN, int64_min_str, len);
            /* check the return value of security function */
            securec_check(rc, "\0", "\0");
            buf[len] = '.';
            rc = memcpy_s(buf + len + 1, MAXBI64LEN - len - 1, int64_min_str + len, scale + 1);
            /* check the return value of security function */
            securec_check(rc, "\0", "\0");
        } else {
            rc = memcpy_s(buf, MAXBI64LEN, int64_min_str, strlen(int64_min_str) + 1);
            /* check the return value of security function */
            securec_check(rc, "\0", "\0");
        }
        result = pstrdup(buf);
        PG_RETURN_CSTRING(result);
    }

    int64 pre_val = 0;
    int64 post_val = 0;

    /* data is positive */
    if (data >= 0) {
        val_u64 = data;
        pre_val = val_u64 / (uint64)getScaleMultiplier(scale);
        post_val = val_u64 % (uint64)getScaleMultiplier(scale);
        if (likely(scale > 0)) {
            /* ignore preceding 0, eg: value is 0.1, output is '.1' */
            if (pre_val == 0 && post_val != 0 && DISPLAY_LEADING_ZERO == false) {
                rc = sprintf_s(buf, MAXBI64LEN, ".%0*ld", scale, post_val);
            } else {
                rc = sprintf_s(buf, MAXBI64LEN, "%ld.%0*ld", pre_val, scale, post_val);
            }
        } else {
            rc = sprintf_s(buf, MAXBI64LEN, "%ld", pre_val);
        }

        /* check the return value of security function */
        securec_check_ss(rc, "\0", "\0");
    } else {
        /* data is negative */
        val_u64 = -data;
        pre_val = val_u64 / (uint64)getScaleMultiplier(scale);
        post_val = val_u64 % (uint64)getScaleMultiplier(scale);
        if (likely(scale > 0)) {
            /* ignore preceding 0, eg: value is -0.1, output is '-.1' */
            if (pre_val == 0 && post_val != 0 && DISPLAY_LEADING_ZERO == false) {
                rc = sprintf_s(buf, MAXBI64LEN, "-.%0*ld", scale, post_val);
            } else {
                rc = sprintf_s(buf, MAXBI64LEN, "-%ld.%0*ld", pre_val, scale, post_val);
            }
        } else {
            rc = sprintf_s(buf, MAXBI64LEN, "-%ld", pre_val);
        }

        /* check the return value of security function */
        securec_check_ss(rc, "\0", "\0");
    }

    result = pstrdup(buf);
    PG_RETURN_CSTRING(result);
}

/*
 * @Description: print int128 data to string, because of current GCC doesn't provide
 *               solution to print int128 data, we provide this function.
 *
 * @IN  preceding_zero: mark whether print preceding zero or not
 * @IN  data: int128 data
 * @OUT str:  the output string buffer
 * @IN  len:  the length of string buffer
 * @IN  scale:when preceding_zero is true, scale is the standard output width size
 * @Return: print succeed or not
 */
template <bool preceding_zero>
static int int128_to_string(int128 data, char* str, int len, int scale)
{
    Assert(data >= 0);
    Assert(scale >= 0);
    Assert(scale <= MAXINT128DIGIT);

    int rc = -1;
    /* turn to int64 */
    if (INT128_INT64_EQ(data)) {
        if (preceding_zero) {
            rc = sprintf_s(str, len, "%0*ld", scale, (int64)data);
        } else {
            rc = sprintf_s(str, len, "%ld", (int64)data);
        }
        securec_check_ss(rc, "\0", "\0");
        return rc;
    }

    /* get the absolute value of data, it's useful for sprintf */
    int128 num = data;
    int64 leading = 0;
    int64 trailing = 0;
    trailing = num % P10_INT64;
    num = num / P10_INT64;
    /* two int64 num can represent the int128 data */
    if (INT128_INT64_EQ(num)) {
        leading = (int64)num;
        if (preceding_zero) {
            Assert(scale > 18);
            rc = sprintf_s(str, len, "%0*ld%018ld", scale - 18, leading, trailing);
        } else {
            rc = sprintf_s(str, len, "%ld%018ld", leading, trailing);
        }
        securec_check_ss(rc, "\0", "\0");
        return rc;
    }

    /* two int64 num can't represent int128data, use 3 int64 numbers */
    int64 middle = num % P10_INT64;
    num = num / P10_INT64;
    leading = (int64)num;
    /* both the middle and trailing digits have 18 digits */
    if (preceding_zero) {
        Assert(scale > 36);
        rc = sprintf_s(str, len, "%0*ld%018ld%018ld", scale - 36, leading, middle, trailing);
    } else {
        rc = sprintf_s(str, len, "%ld%018ld%018ld", leading, middle, trailing);
    }
    securec_check_ss(rc, "\0", "\0");
    return rc;
}

/*
 * @Description: print bi128 data to string like numeric_out.
 *
 * @IN  data: int128 data
 * @IN  scale:  the scale of bi128 data
 * @Return: Output string for numeric data type
 */
Datum bi128_out(int128 data, int scale)
{
    Assert(scale >= 0);
    Assert(scale <= MAXINT128DIGIT);
    char buf[MAXBI128LEN];
    char* result = NULL;

    /* data equals to "INT128_MIN" */
    if (unlikely(data == INT128_MIN)) {
        int rc = -1;
        /*
         * Avoid problems with the most negative integer not being representable
         * as a positive integer.
         */
        if (scale > 0) {
            int len = strlen(int128_min_str) - scale;
            rc = memcpy_s(buf, MAXBI128LEN, int128_min_str, len);
            /* check the return value of security function */
            securec_check(rc, "\0", "\0");
            buf[len] = '.';
            rc = memcpy_s(buf + len + 1, MAXBI128LEN - len - 1, int128_min_str + len, scale + 1);
            /* check the return value of security function */
            securec_check(rc, "\0", "\0");
        } else {
            rc = memcpy_s(buf, MAXBI128LEN, int128_min_str, strlen(int128_min_str) + 1);
            /* check the return value of security function */
            securec_check(rc, "\0", "\0");
        }
        result = pstrdup(buf);
        PG_RETURN_CSTRING(result);
    }

    int128 pre_val = 0;
    int128 post_val = 0;

    /* data is positive */
    if (data >= 0) {
        pre_val = data / getScaleMultiplier(scale);
        post_val = data % getScaleMultiplier(scale);
        if (likely(scale > 0)) {
            if (pre_val == 0 && post_val != 0 && DISPLAY_LEADING_ZERO == false) {
                buf[0] = '.';
                (void)int128_to_string<true>(post_val, buf + 1, MAXBI128LEN - 1, scale);
            } else {
                (void)int128_to_string<false>(pre_val, buf, MAXBI128LEN, 0);
                int len = strlen(buf);
                buf[len] = '.';
                (void)int128_to_string<true>(post_val, buf + len + 1, MAXBI128LEN - len - 1, scale);
            }
        } else {
            (void)int128_to_string<false>(pre_val, buf, MAXBI128LEN, 0);
        }
    } else { /* data is negative */
        data = -data;
        pre_val = data / getScaleMultiplier(scale);
        post_val = data % getScaleMultiplier(scale);
        buf[0] = '-';
        if (likely(scale > 0)) {
            if (pre_val == 0 && post_val != 0 && DISPLAY_LEADING_ZERO == false) {
                buf[1] = '.';
                int128_to_string<true>(post_val, buf + 2, MAXBI128LEN - 2, scale);
            } else {
                int128_to_string<false>(pre_val, buf + 1, MAXBI128LEN - 1, 0);
                int len = strlen(buf);
                buf[len] = '.';
                int128_to_string<true>(post_val, buf + len + 1, MAXBI128LEN - len - 1, scale);
            }
        } else {
            int128_to_string<false>(pre_val, buf + 1, MAXBI128LEN - 1, 0);
        }
    }
    result = pstrdup(buf);
    PG_RETURN_CSTRING(result);
}
inline void Int128GetVal(bool is_int128, int128* val, Numeric* arg)
{
    if (is_int128) {
        errno_t rc = memcpy_s(val, sizeof(int128), (*arg)->choice.n_bi.n_data, sizeof(int128));
        securec_check(rc, "\0", "\0");
    } else {
        *val = NUMERIC_64VALUE(*arg);
    }
}
