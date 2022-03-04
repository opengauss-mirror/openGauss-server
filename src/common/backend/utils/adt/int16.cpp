/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2021, openGauss Contributors
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
 *
 * int16.cpp
 *     Internal 128-bit integer operations.
 *
 * Portions Copyright (c) 2018, Huawei Tech. Co., Ltd.
 *
 * IDENTIFICATION
 *     src/common/backend/utils/adt/int16.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include <ctype.h>
#include <limits.h>
#include <math.h>

#include "common/int.h"
#include "funcapi.h"
#include "libpq/pqformat.h"
#include "utils/builtins.h"
#include "utils/int16.h"
#include "utils/int8.h"
#include "utils/numeric.h"

const int MAXINT16LEN = 45;

typedef struct {
    const char* str;
    unsigned char ptr;
    bool errorOK;
    int128 tmp;
} CheckContext;

static inline bool check_one_digit(const CheckContext* cxt, int128* result, bool* ret)
{
    /* require at least one digit */
    if (unlikely(!isdigit(cxt->ptr))) {
        if (cxt->errorOK) {
            *ret = false;
            return true;
        } else if (DB_IS_CMPT(A_FORMAT | PG_FORMAT)) {
            ereport(ERROR,
                (errmodule(MOD_FUNCTION), errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                    errmsg("invalid input syntax for type %s: \"%s\"", "int16", cxt->str),
                    errdetail("cannot convert input text to int16"),
                    errcause("invalid input."),
                    erraction("modify input text to be valid integer format.")));
        } else if (u_sess->attr.attr_sql.sql_compatibility == B_FORMAT) {
            *result = cxt->tmp;
            *ret = true;
            return true;
        }
    }
    return false;
}

static inline bool check_trailing_symbol(unsigned char ptr)
{
    return ptr != '\0' && isspace(ptr);
}

bool scanint16(const char* str, bool errorOK, int128* result)
{
    const char* ptr = str;
    int128 tmp = 0;
    bool neg = false;

    /*
     * Do our own scan, rather than relying on sscanf which might be broken
     * for long long.
     *
     * As INT64_MIN can't be stored as a positive 64 bit integer, accumulate
     * value as a negative number.
     */

    /* skip leading spaces */
    while (*ptr && isspace((unsigned char)*ptr)) {
        ptr++;
    }

    /* handle sign */
    if (*ptr == '-') {
        ptr++;
        neg = true;
    } else if (*ptr == '+') {
        ptr++;
    }
    bool ret = false;
    CheckContext cxt = {str, (unsigned char)*ptr, errorOK, tmp};
    if (check_one_digit(&cxt, result, &ret)) {
        return ret;
    }
    const int base = 10;

    /* process digits */
    while (*ptr && isdigit((unsigned char)*ptr)) {
        int8 digit = (*ptr++ - '0');
        if (unlikely(pg_mul_s128_overflow(tmp, base, &tmp)) || unlikely(pg_sub_s128_overflow(tmp, digit, &tmp))) {
            if (errorOK) {
                return false;
            } else {
                ereport(ERROR,
                    (errmodule(MOD_FUNCTION), errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                        errmsg("value \"%s\" is out of range for type %s", str, "int16"),
                        errdetail("text exceeds the length of int16"),
                        errcause("invalid input."),
                        erraction("use numeric for large integer value.")));
            }
        }
    }

    /* allow trailing whitespace, but not other trailing chars */
    while (check_trailing_symbol((unsigned char)*ptr)) {
        ptr++;
    }

    if (unlikely(*ptr != '\0')) {
        if (errorOK) {
            return false;
        } else {
            /* Empty string will be treated as NULL if sql_compatibility == A_FORMAT,
                Other wise whitespace will be convert to 0 */
            ereport(ERROR,
                (errmodule(MOD_FUNCTION), errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                    errmsg("invalid input syntax for type %s: \"%s\"", "int16", str),
                    errdetail("text contain invalid character"),
                    errcause("invalid input."),
                    erraction("check the validity of input.")));
        }
    }

    if (!neg) {
        /* could fail if input is most negative number */
        if (unlikely(tmp == PG_INT128_MIN)) {
            if (errorOK) {
                return false;
            } else {
                ereport(ERROR,
                    (errmodule(MOD_FUNCTION), errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                        errmsg("value \"%s\" is out of range for type %s", str, "int16"),
                        errdetail("text exceeds the length of int16"),
                        errcause("invalid input."),
                        erraction("use numeric for large integer value.")));
            }
        }
        tmp = -tmp;
    }

    *result = tmp;
    return true;
}

/* int16in()
 */
Datum int16in(PG_FUNCTION_ARGS)
{
    char* str = PG_GETARG_CSTRING(0);
    int128 result;

    (void)scanint16(str, false, &result);
    PG_RETURN_INT128(result);
}

/* int16out()
 */
Datum int16out(PG_FUNCTION_ARGS)
{
    int128 val = PG_GETARG_INT128(0);
    char buf[MAXINT16LEN + 1];
    char* result = NULL;

    pg_i128toa(val, buf, MAXINT16LEN + 1);
    result = pstrdup(buf);
    PG_RETURN_CSTRING(result);
}

/* int16recv()
 */
Datum int16recv(PG_FUNCTION_ARGS)
{
    StringInfo buf = (StringInfo)PG_GETARG_POINTER(0);

    PG_RETURN_INT128(pq_getmsgint128(buf));
}

/* int16send()
 */
Datum int16send(PG_FUNCTION_ARGS)
{
    int128 arg1 = PG_GETARG_INT128(0);
    StringInfoData buf;

    pq_begintypsend(&buf);
    pq_sendint128(&buf, arg1);
    PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

Datum int1_16(PG_FUNCTION_ARGS)
{
    uint1 arg = PG_GETARG_UINT8(0);
    PG_RETURN_INT128((int128)arg);
}

Datum int16_1(PG_FUNCTION_ARGS)
{
    int128 arg = PG_GETARG_INT128(0);
    uint8 result;

    result = (uint8)arg;

    /* Test for overflow by reverse-conversion. */
    if ((int128)result != arg) {
        ereport(ERROR,
            (errmodule(MOD_FUNCTION), errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                errmsg("tinyint out of range"),
                errdetail("cannot cast value too large for tinyint"),
                errcause("invalid cast."),
                erraction("cast overflow.")));
    }

    PG_RETURN_INT16(result);
}


Datum int2_16(PG_FUNCTION_ARGS)
{
    int16 arg = PG_GETARG_INT16(0);
    PG_RETURN_INT128((int128)arg);
}

Datum int16_2(PG_FUNCTION_ARGS)
{
    int128 arg = PG_GETARG_INT128(0);
    int16 result;

    result = (int16)arg;

    /* Test for overflow by reverse-conversion. */
    if ((int128)result != arg) {
        ereport(ERROR,
            (errmodule(MOD_FUNCTION), errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                errmsg("smallint out of range"),
                errdetail("cannot cast value too large for smallint"),
                errcause("invalid cast."),
                erraction("cast overflow.")));
    }

    PG_RETURN_INT16(result);
}

Datum int4_16(PG_FUNCTION_ARGS)
{
    int32 arg = PG_GETARG_INT32(0);
    PG_RETURN_INT128((int128)arg);
}

Datum int16_4(PG_FUNCTION_ARGS)
{
    int128 arg = PG_GETARG_INT128(0);
    int32 result;

    result = (int32)arg;

    /* Test for overflow by reverse-conversion. */
    if ((int128)result != arg) {
        ereport(ERROR,
            (errmodule(MOD_FUNCTION), errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                errmsg("integer out of range"),
                errdetail("cannot cast value too large for integer"),
                errcause("invalid cast."),
                erraction("cast overflow.")));
    }

    PG_RETURN_INT32(result);
}

Datum int8_16(PG_FUNCTION_ARGS)
{
    int64 arg = PG_GETARG_INT64(0);
    PG_RETURN_INT128((int128)arg);
}

Datum int16_8(PG_FUNCTION_ARGS)
{
    int128 arg = PG_GETARG_INT128(0);
    int64 result;

    result = (int64)arg;

    /* Test for overflow by reverse-conversion. */
    if ((int128)result != arg) {
        ereport(ERROR,
            (errmodule(MOD_FUNCTION), errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                errmsg("bigint out of range"),
                errdetail("cannot cast value too large for bigint"),
                errcause("invalid cast."),
                erraction("cast overflow.")));
    }

    PG_RETURN_INT64(result);
}

Datum i16tod(PG_FUNCTION_ARGS)
{
    int128 arg = PG_GETARG_INT128(0);
    float8 result;

    result = arg;

    PG_RETURN_FLOAT8(result);
}

Datum dtoi16(PG_FUNCTION_ARGS)
{
    float8 num = PG_GETARG_FLOAT8(0);

    /*
     * Get rid of any fractional part in the input.  This is so we don't fail
     * on just-out-of-range values that would round into range.  Note
     * assumption that rint() will pass through a NaN or Inf unchanged.
     */
    num = rint(num);
    /*
     * Range check.  We must be careful here that the boundary values are
     * expressed exactly in the float domain.  We expect PG_INT128_MIN  to be an
     * exact power of 2, so it will be represented exactly; but PG_INT128_MAX
     * isn't, and might get rounded off, so avoid using it.
     */
    if (num < (float8)PG_INT128_MIN || num >= -((float8)PG_INT128_MIN) || isnan(num)) {
        ereport(ERROR,
            (errmodule(MOD_FUNCTION), errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                errmsg("int16 out of range"),
                errdetail("cannot cast value too large for int16"),
                errcause("invalid cast."),
                erraction("cast overflow.")));
    }

    PG_RETURN_INT128((int128)num);
}

Datum i16tof(PG_FUNCTION_ARGS)
{
    int128 arg = PG_GETARG_INT128(0);
    float4 result;

    result = arg;

    PG_RETURN_FLOAT4(result);
}

Datum ftoi16(PG_FUNCTION_ARGS)
{
    float4 num = PG_GETARG_FLOAT4(0);

    /*
     * Get rid of any fractional part in the input.  This is so we don't fail
     * on just-out-of-range values that would round into range.  Note
     * assumption that rint() will pass through a NaN or Inf unchanged.
     */
    num = rint(num);
    /*
     * Range check.  We must be careful here that the boundary values are
     * expressed exactly in the float domain.  We expect PG_INT128_MIN  to be an
     * exact power of 2, so it will be represented exactly; but PG_INT128_MIN
     * isn't, and might get rounded off, so avoid using it.
     */
    if (num < (float4)PG_INT128_MIN || num >= -((float4)PG_INT128_MIN) || isnan(num)) {
        ereport(ERROR,
            (errmodule(MOD_FUNCTION), errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                errmsg("int16 out of range"),
                errdetail("cannot cast value too large for int16"),
                errcause("invalid cast."),
                erraction("cast overflow.")));
    }

    PG_RETURN_INT128((int128)num);
}

Datum i16tooid(PG_FUNCTION_ARGS)
{
    int128 arg = PG_GETARG_INT128(0);
    Oid result;

    result = (Oid)arg;

    /* Test for overflow by reverse-conversion. */
    if ((int128)result != arg) {
        ereport(ERROR,
            (errmodule(MOD_FUNCTION), errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                errmsg("OID out of range"),
                errdetail("cannot cast value too large for OID"),
                errcause("invalid cast."),
                erraction("cast overflow.")));
    }

    PG_RETURN_OID(result);
}

Datum oidtoi16(PG_FUNCTION_ARGS)
{
    Oid arg = PG_GETARG_OID(0);

    PG_RETURN_INT128((int128)arg);
}

Datum int16_bool(PG_FUNCTION_ARGS)
{
    if (PG_GETARG_INT128(0) == 0)
        PG_RETURN_BOOL(false);
    else
        PG_RETURN_BOOL(true);
}

Datum bool_int16(PG_FUNCTION_ARGS)
{
    if (PG_GETARG_BOOL(0) == false)
        PG_RETURN_INT128(0);
    else
        PG_RETURN_INT128(1);
}

Datum int16eq(PG_FUNCTION_ARGS)
{
    int128 val1 = PG_GETARG_INT128(0);
    int128 val2 = PG_GETARG_INT128(1);

    PG_RETURN_BOOL(val1 == val2);
}

Datum int16ne(PG_FUNCTION_ARGS)
{
    int128 val1 = PG_GETARG_INT128(0);
    int128 val2 = PG_GETARG_INT128(1);

    PG_RETURN_BOOL(val1 != val2);
}

Datum int16lt(PG_FUNCTION_ARGS)
{
    int128 val1 = PG_GETARG_INT128(0);
    int128 val2 = PG_GETARG_INT128(1);

    PG_RETURN_BOOL(val1 < val2);
}

Datum int16gt(PG_FUNCTION_ARGS)
{
    int128 val1 = PG_GETARG_INT128(0);
    int128 val2 = PG_GETARG_INT128(1);

    PG_RETURN_BOOL(val1 > val2);
}

Datum int16le(PG_FUNCTION_ARGS)
{
    int128 val1 = PG_GETARG_INT128(0);
    int128 val2 = PG_GETARG_INT128(1);

    PG_RETURN_BOOL(val1 <= val2);
}

Datum int16ge(PG_FUNCTION_ARGS)
{
    int128 val1 = PG_GETARG_INT128(0);
    int128 val2 = PG_GETARG_INT128(1);

    PG_RETURN_BOOL(val1 >= val2);
}

Datum int16pl(PG_FUNCTION_ARGS)
{
    int128 arg1 = PG_GETARG_INT128(0);
    int128 arg2 = PG_GETARG_INT128(1);
    int128 result;

    if (unlikely(pg_add_s128_overflow(arg1, arg2, &result))) {
        ereport(ERROR,
            (errmodule(MOD_FUNCTION), errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                errmsg("int16 out of range"),
                errdetail("result is too large for int16"),
                errcause("invalid expression."),
                erraction("result overflow.")));
    }
    PG_RETURN_INT128(result);
}

Datum int16mi(PG_FUNCTION_ARGS)
{
    int128 arg1 = PG_GETARG_INT128(0);
    int128 arg2 = PG_GETARG_INT128(1);
    int128 result;

    if (unlikely(pg_sub_s128_overflow(arg1, arg2, &result))) {
        ereport(ERROR,
            (errmodule(MOD_FUNCTION), errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                errmsg("int16 out of range"),
                errdetail("result is too large for int16"),
                errcause("invalid expression."),
                erraction("result overflow.")));
    }
    PG_RETURN_INT128(result);
}

Datum int16mul(PG_FUNCTION_ARGS)
{
    int128 arg1 = PG_GETARG_INT128(0);
    int128 arg2 = PG_GETARG_INT128(1);
    int128 result;

    if (unlikely(pg_mul_s128_overflow(arg1, arg2, &result))) {
        ereport(ERROR,
            (errmodule(MOD_FUNCTION), errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                errmsg("int16 out of range"),
                errdetail("result is too large for int16"),
                errcause("invalid expression."),
                erraction("result overflow.")));
    }
    PG_RETURN_INT128(result);
}

Datum int16div(PG_FUNCTION_ARGS)
{
    int128 arg1 = PG_GETARG_INT128(0);
    int128 arg2 = PG_GETARG_INT128(1);
    float8 result;

    if (arg2 == 0) {
        ereport(ERROR,
            (errmodule(MOD_FUNCTION), errcode(ERRCODE_DIVISION_BY_ZERO),
                errmsg("division by zero"),
                errdetail("N/A"),
                errcause("invalid expression."),
                erraction("division by zero.")));
    }

    if (arg1 == 0) {
        PG_RETURN_FLOAT8(0);
    }

    /* No overflow is possible */
    result = (arg1 * 1.0) / (arg2 * 1.0);

    PG_RETURN_FLOAT8(result);
}
