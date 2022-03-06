/* -------------------------------------------------------------------------
 *
 * int8.c
 *	  Internal 64-bit integer operations
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/int8.c
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
#include "utils/int8.h"
#include "utils/builtins.h"

#define MAXINT8LEN 25

typedef struct {
    int64 current;
    int64 finish;
    int64 step;
} generate_series_fctx;

/***********************************************************************
 **
 **		Routines for 64-bit integers.
 **
 ***********************************************************************/

/* ----------------------------------------------------------
 * Formatting and conversion routines.
 * --------------------------------------------------------- */

/*
 * scanint8 --- try to parse a string into an int8.
 *
 * If errorOK is false, ereport a useful error message if the string is bad.
 * If errorOK is true, just return "false" for bad input.
 */
bool scanint8(const char* str, bool errorOK, int64* result)
{
    const char* ptr = str;
    int64 tmp = 0;
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
    } else if (*ptr == '+')
        ptr++;

    /* require at least one digit */
    if (unlikely(!isdigit((unsigned char)*ptr))) {
        if (errorOK)
            return false;
        else if (DB_IS_CMPT(A_FORMAT | PG_FORMAT))
            ereport(ERROR,
                (errmodule(MOD_FUNCTION),
                    errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                    errmsg("invalid input syntax for type %s: \"%s\"", "bigint", str)));
        else if (u_sess->attr.attr_sql.sql_compatibility == B_FORMAT) {
            *result = tmp;
            return true;
        }
    }

    /* process digits */
    while (*ptr && isdigit((unsigned char)*ptr)) {
        int8 digit = (*ptr++ - '0');

        if (unlikely(pg_mul_s64_overflow(tmp, 10, &tmp)) || unlikely(pg_sub_s64_overflow(tmp, digit, &tmp))) {
            if (errorOK)
                return false;
            else
                ereport(ERROR,
                    (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                        errmsg("value \"%s\" is out of range for type %s", str, "bigint")));
        }
    }

    /* allow trailing whitespace, but not other trailing chars */
    while (*ptr != '\0' && isspace((unsigned char)*ptr)) {
        ptr++;
    }

    if (unlikely(*ptr != '\0')) {
        if (errorOK)
            return false;
        else
            /* Empty string will be treated as NULL if sql_compatibility == A_FORMAT,
                Other wise whitespace will be convert to 0 */
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                    errmsg("invalid input syntax for type %s: \"%s\"", "bigint", str)));
    }

    if (!neg) {
        /* could fail if input is most negative number */
        if (unlikely(tmp == PG_INT64_MIN)) {
            if (errorOK)
                return false;
            else
                ereport(ERROR,
                    (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                        errmsg("value \"%s\" is out of range for type %s", str, "bigint")));
        }
        tmp = -tmp;
    }

    *result = tmp;
    return true;
}

/* int8in()
 */
Datum int8in(PG_FUNCTION_ARGS)
{
    char* str = PG_GETARG_CSTRING(0);
    int64 result;

    (void)scanint8(str, false, &result);
    PG_RETURN_INT64(result);
}

/* int8out()
 */
Datum int8out(PG_FUNCTION_ARGS)
{
    int64 val = PG_GETARG_INT64(0);
    char buf[MAXINT8LEN + 1];
    char* result = NULL;

    pg_lltoa(val, buf);
    result = pstrdup(buf);
    PG_RETURN_CSTRING(result);
}

/*
 *		int8recv			- converts external binary format to int8
 */
Datum int8recv(PG_FUNCTION_ARGS)
{
    StringInfo buf = (StringInfo)PG_GETARG_POINTER(0);

    PG_RETURN_INT64(pq_getmsgint64(buf));
}

/*
 *		int8send			- converts int8 to binary format
 */
Datum int8send(PG_FUNCTION_ARGS)
{
    int64 arg1 = PG_GETARG_INT64(0);
    StringInfoData buf;

    pq_begintypsend(&buf);
    pq_sendint64(&buf, arg1);
    PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

/* ----------------------------------------------------------
 *	Relational operators for int8s, including cross-data-type comparisons.
 * --------------------------------------------------------- */

/* int8relop()
 * Is val1 relop val2?
 */
Datum int8eq(PG_FUNCTION_ARGS)
{
    int64 val1 = PG_GETARG_INT64(0);
    int64 val2 = PG_GETARG_INT64(1);

    PG_RETURN_BOOL(val1 == val2);
}

Datum int8ne(PG_FUNCTION_ARGS)
{
    int64 val1 = PG_GETARG_INT64(0);
    int64 val2 = PG_GETARG_INT64(1);

    PG_RETURN_BOOL(val1 != val2);
}

Datum int8lt(PG_FUNCTION_ARGS)
{
    int64 val1 = PG_GETARG_INT64(0);
    int64 val2 = PG_GETARG_INT64(1);

    PG_RETURN_BOOL(val1 < val2);
}

Datum int8gt(PG_FUNCTION_ARGS)
{
    int64 val1 = PG_GETARG_INT64(0);
    int64 val2 = PG_GETARG_INT64(1);

    PG_RETURN_BOOL(val1 > val2);
}

Datum int8le(PG_FUNCTION_ARGS)
{
    int64 val1 = PG_GETARG_INT64(0);
    int64 val2 = PG_GETARG_INT64(1);

    PG_RETURN_BOOL(val1 <= val2);
}

Datum int8ge(PG_FUNCTION_ARGS)
{
    int64 val1 = PG_GETARG_INT64(0);
    int64 val2 = PG_GETARG_INT64(1);

    PG_RETURN_BOOL(val1 >= val2);
}

/* int84relop()
 * Is 64-bit val1 relop 32-bit val2?
 */
Datum int84eq(PG_FUNCTION_ARGS)
{
    int64 val1 = PG_GETARG_INT64(0);
    int32 val2 = PG_GETARG_INT32(1);

    PG_RETURN_BOOL(val1 == val2);
}

Datum int84ne(PG_FUNCTION_ARGS)
{
    int64 val1 = PG_GETARG_INT64(0);
    int32 val2 = PG_GETARG_INT32(1);

    PG_RETURN_BOOL(val1 != val2);
}

Datum int84lt(PG_FUNCTION_ARGS)
{
    int64 val1 = PG_GETARG_INT64(0);
    int32 val2 = PG_GETARG_INT32(1);

    PG_RETURN_BOOL(val1 < val2);
}

Datum int84gt(PG_FUNCTION_ARGS)
{
    int64 val1 = PG_GETARG_INT64(0);
    int32 val2 = PG_GETARG_INT32(1);

    PG_RETURN_BOOL(val1 > val2);
}

Datum int84le(PG_FUNCTION_ARGS)
{
    int64 val1 = PG_GETARG_INT64(0);
    int32 val2 = PG_GETARG_INT32(1);

    PG_RETURN_BOOL(val1 <= val2);
}

Datum int84ge(PG_FUNCTION_ARGS)
{
    int64 val1 = PG_GETARG_INT64(0);
    int32 val2 = PG_GETARG_INT32(1);

    PG_RETURN_BOOL(val1 >= val2);
}

/* int48relop()
 * Is 32-bit val1 relop 64-bit val2?
 */
Datum int48eq(PG_FUNCTION_ARGS)
{
    int32 val1 = PG_GETARG_INT32(0);
    int64 val2 = PG_GETARG_INT64(1);

    PG_RETURN_BOOL(val1 == val2);
}

Datum int48ne(PG_FUNCTION_ARGS)
{
    int32 val1 = PG_GETARG_INT32(0);
    int64 val2 = PG_GETARG_INT64(1);

    PG_RETURN_BOOL(val1 != val2);
}

Datum int48lt(PG_FUNCTION_ARGS)
{
    int32 val1 = PG_GETARG_INT32(0);
    int64 val2 = PG_GETARG_INT64(1);

    PG_RETURN_BOOL(val1 < val2);
}

Datum int48gt(PG_FUNCTION_ARGS)
{
    int32 val1 = PG_GETARG_INT32(0);
    int64 val2 = PG_GETARG_INT64(1);

    PG_RETURN_BOOL(val1 > val2);
}

Datum int48le(PG_FUNCTION_ARGS)
{
    int32 val1 = PG_GETARG_INT32(0);
    int64 val2 = PG_GETARG_INT64(1);

    PG_RETURN_BOOL(val1 <= val2);
}

Datum int48ge(PG_FUNCTION_ARGS)
{
    int32 val1 = PG_GETARG_INT32(0);
    int64 val2 = PG_GETARG_INT64(1);

    PG_RETURN_BOOL(val1 >= val2);
}

/* int82relop()
 * Is 64-bit val1 relop 16-bit val2?
 */
Datum int82eq(PG_FUNCTION_ARGS)
{
    int64 val1 = PG_GETARG_INT64(0);
    int16 val2 = PG_GETARG_INT16(1);

    PG_RETURN_BOOL(val1 == val2);
}

Datum int82ne(PG_FUNCTION_ARGS)
{
    int64 val1 = PG_GETARG_INT64(0);
    int16 val2 = PG_GETARG_INT16(1);

    PG_RETURN_BOOL(val1 != val2);
}

Datum int82lt(PG_FUNCTION_ARGS)
{
    int64 val1 = PG_GETARG_INT64(0);
    int16 val2 = PG_GETARG_INT16(1);

    PG_RETURN_BOOL(val1 < val2);
}

Datum int82gt(PG_FUNCTION_ARGS)
{
    int64 val1 = PG_GETARG_INT64(0);
    int16 val2 = PG_GETARG_INT16(1);

    PG_RETURN_BOOL(val1 > val2);
}

Datum int82le(PG_FUNCTION_ARGS)
{
    int64 val1 = PG_GETARG_INT64(0);
    int16 val2 = PG_GETARG_INT16(1);

    PG_RETURN_BOOL(val1 <= val2);
}

Datum int82ge(PG_FUNCTION_ARGS)
{
    int64 val1 = PG_GETARG_INT64(0);
    int16 val2 = PG_GETARG_INT16(1);

    PG_RETURN_BOOL(val1 >= val2);
}

/* int28relop()
 * Is 16-bit val1 relop 64-bit val2?
 */
Datum int28eq(PG_FUNCTION_ARGS)
{
    int16 val1 = PG_GETARG_INT16(0);
    int64 val2 = PG_GETARG_INT64(1);

    PG_RETURN_BOOL(val1 == val2);
}

Datum int28ne(PG_FUNCTION_ARGS)
{
    int16 val1 = PG_GETARG_INT16(0);
    int64 val2 = PG_GETARG_INT64(1);

    PG_RETURN_BOOL(val1 != val2);
}

Datum int28lt(PG_FUNCTION_ARGS)
{
    int16 val1 = PG_GETARG_INT16(0);
    int64 val2 = PG_GETARG_INT64(1);

    PG_RETURN_BOOL(val1 < val2);
}

Datum int28gt(PG_FUNCTION_ARGS)
{
    int16 val1 = PG_GETARG_INT16(0);
    int64 val2 = PG_GETARG_INT64(1);

    PG_RETURN_BOOL(val1 > val2);
}

Datum int28le(PG_FUNCTION_ARGS)
{
    int16 val1 = PG_GETARG_INT16(0);
    int64 val2 = PG_GETARG_INT64(1);

    PG_RETURN_BOOL(val1 <= val2);
}

Datum int28ge(PG_FUNCTION_ARGS)
{
    int16 val1 = PG_GETARG_INT16(0);
    int64 val2 = PG_GETARG_INT64(1);

    PG_RETURN_BOOL(val1 >= val2);
}

/* ----------------------------------------------------------
 *	Arithmetic operators on 64-bit integers.
 * --------------------------------------------------------- */

Datum int8um(PG_FUNCTION_ARGS)
{
    int64 arg = PG_GETARG_INT64(0);
    int64 result;

    if (unlikely(arg == PG_INT64_MIN))
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("bigint out of range")));
    result = -arg;
    PG_RETURN_INT64(result);
}

Datum int8up(PG_FUNCTION_ARGS)
{
    int64 arg = PG_GETARG_INT64(0);

    PG_RETURN_INT64(arg);
}

Datum int8pl(PG_FUNCTION_ARGS)
{
    int64 arg1 = PG_GETARG_INT64(0);
    int64 arg2 = PG_GETARG_INT64(1);
    int64 result;

    if (unlikely(pg_add_s64_overflow(arg1, arg2, &result)))
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("bigint out of range")));
    PG_RETURN_INT64(result);
}

Datum int8mi(PG_FUNCTION_ARGS)
{
    int64 arg1 = PG_GETARG_INT64(0);
    int64 arg2 = PG_GETARG_INT64(1);
    int64 result;

    if (unlikely(pg_sub_s64_overflow(arg1, arg2, &result)))
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("bigint out of range")));
    PG_RETURN_INT64(result);
}

Datum int8mul(PG_FUNCTION_ARGS)
{
    int64 arg1 = PG_GETARG_INT64(0);
    int64 arg2 = PG_GETARG_INT64(1);
    int64 result;

    if (unlikely(pg_mul_s64_overflow(arg1, arg2, &result)))
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("bigint out of range")));
    PG_RETURN_INT64(result);
}

Datum int8div(PG_FUNCTION_ARGS)
{
    int64 arg1 = PG_GETARG_INT64(0);
    int64 arg2 = PG_GETARG_INT64(1);
    float8 result;

    if (arg2 == 0) {
        ereport(ERROR, (errcode(ERRCODE_DIVISION_BY_ZERO), errmsg("division by zero")));
        /* ensure compiler realizes we mustn't reach the division (gcc bug) */
        PG_RETURN_NULL();
    }

    if (arg1 == 0) {
        PG_RETURN_FLOAT8(0);
    }

    /*
     * INT64_MIN / -1 is problematic, since the result can't be represented on
     * a two's-complement machine.  Some machines produce INT64_MIN, some
     * produce zero, some throw an exception.  We produce an exception like
     * C db and D db do.
     */
    if (arg2 == -1) {
        int128 res = (int128)arg1 * (-1);
        /* overflow check (needed for INT64_MIN) */
        if (SUPPORT_BIND_DIVIDE && unlikely(res > PG_INT64_MAX))
            ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("bigint out of range")));
        PG_RETURN_FLOAT8(res);
    }

    /* No overflow is possible */
    result = (arg1 * 1.0) / (arg2 * 1.0);

    PG_RETURN_FLOAT8(result);
}

/* int8abs()
 * Absolute value
 */
Datum int8abs(PG_FUNCTION_ARGS)
{
    int64 arg1 = PG_GETARG_INT64(0);
    int64 result;

    if (unlikely(arg1 == PG_INT64_MIN))
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("bigint out of range")));
    result = (arg1 < 0) ? -arg1 : arg1;
    PG_RETURN_INT64(result);
}

/* int8mod()
 * Modulo operation.
 */
Datum int8mod(PG_FUNCTION_ARGS)
{
    int64 arg1 = PG_GETARG_INT64(0);
    int64 arg2 = PG_GETARG_INT64(1);

    if (unlikely(arg2 == 0)) {
        if (DB_IS_CMPT(PG_FORMAT)) {
            /* zero is not allowed to be divisor if compatible with PG */
            ereport(ERROR, (errcode(ERRCODE_DIVISION_BY_ZERO), errmsg("division by zero")));

            /* ensure compiler realizes we mustn't reach the division (gcc bug) */
            PG_RETURN_NULL();
        }
        // zero is allowed to be divisor
        PG_RETURN_INT64(arg1);
    }

    /*
     * Some machines throw a floating-point exception for INT64_MIN % -1,
     * which is a bit silly since the correct answer is perfectly
     * well-defined, namely zero.
     */
    if (arg2 == -1)
        PG_RETURN_INT64(0);

    /* No overflow is possible */

    PG_RETURN_INT64(arg1 % arg2);
}

Datum int8inc(PG_FUNCTION_ARGS)
{
    /*
     * When int8 is pass-by-reference, we provide this special case to avoid
     * palloc overhead for COUNT(): when called as an aggregate, we know that
     * the argument is modifiable local storage, so just update it in-place.
     * (If int8 is pass-by-value, then of course this is useless as well as
     * incorrect, so just ifdef it out.)
     */
#ifndef USE_FLOAT8_BYVAL /* controls int8 too */
    if (AggCheckCallContext(fcinfo, NULL)) {
        int64* arg = (int64*)PG_GETARG_POINTER(0);

        if (unlikely(pg_add_s64_overflow(*arg, 1, arg)))
            ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("bigint out of range")));

        PG_RETURN_POINTER(arg);
    } else
#endif
    {
        /* Not called as an aggregate, so just do it the dumb way */
        int64 arg = PG_GETARG_INT64(0);
        int64 result;

        if (unlikely(pg_add_s64_overflow(arg, 1, &result)))
            ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("bigint out of range")));

        PG_RETURN_INT64(result);
    }
}

Datum int8dec(PG_FUNCTION_ARGS)
{
    /*
     * When int8 is pass-by-reference, we provide this special case to avoid
     * palloc overhead for COUNT(): when called as an aggregate, we know that
     * the argument is modifiable local storage, so just update it in-place.
     * (If int8 is pass-by-value, then of course this is useless as well as
     * incorrect, so just ifdef it out.)
     */
#ifndef USE_FLOAT8_BYVAL /* controls int8 too */
    if (AggCheckCallContext(fcinfo, NULL)) {
        int64* arg = (int64*)PG_GETARG_POINTER(0);

        if (unlikely(pg_sub_s64_overflow(*arg, 1, arg)))
            ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("bigint out of range")));
        PG_RETURN_POINTER(arg);
    } else
#endif
    {
        /* Not called as an aggregate, so just do it the dumb way */
        int64 arg = PG_GETARG_INT64(0);
        int64 result;

        if (unlikely(pg_sub_s64_overflow(arg, 1, &result)))
            ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("bigint out of range")));

        PG_RETURN_INT64(result);
    }
}

/*
 * These functions are exactly like int8inc but are used for aggregates that
 * count only non-null values.	Since the functions are declared strict,
 * the null checks happen before we ever get here, and all we need do is
 * increment the state value.  We could actually make these pg_proc entries
 * point right at int8inc, but then the opr_sanity regression test would
 * complain about mismatched entries for a built-in function.
 */

Datum int8inc_any(PG_FUNCTION_ARGS)
{
    return int8inc(fcinfo);
}

Datum int8inc_float8_float8(PG_FUNCTION_ARGS)
{
    return int8inc(fcinfo);
}

Datum int8larger(PG_FUNCTION_ARGS)
{
    int64 arg1 = PG_GETARG_INT64(0);
    int64 arg2 = PG_GETARG_INT64(1);
    int64 result;

    result = ((arg1 > arg2) ? arg1 : arg2);

    PG_RETURN_INT64(result);
}

Datum int8smaller(PG_FUNCTION_ARGS)
{
    int64 arg1 = PG_GETARG_INT64(0);
    int64 arg2 = PG_GETARG_INT64(1);
    int64 result;

    result = ((arg1 < arg2) ? arg1 : arg2);

    PG_RETURN_INT64(result);
}

Datum int84pl(PG_FUNCTION_ARGS)
{
    int64 arg1 = PG_GETARG_INT64(0);
    int32 arg2 = PG_GETARG_INT32(1);
    int64 result;

    if (unlikely(pg_add_s64_overflow(arg1, (int64)arg2, &result)))
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("bigint out of range")));
    PG_RETURN_INT64(result);
}

Datum int84mi(PG_FUNCTION_ARGS)
{
    int64 arg1 = PG_GETARG_INT64(0);
    int32 arg2 = PG_GETARG_INT32(1);
    int64 result;

    if (unlikely(pg_sub_s64_overflow(arg1, (int64)arg2, &result)))
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("bigint out of range")));
    PG_RETURN_INT64(result);
}

Datum int84mul(PG_FUNCTION_ARGS)
{
    int64 arg1 = PG_GETARG_INT64(0);
    int32 arg2 = PG_GETARG_INT32(1);
    int64 result;

    if (unlikely(pg_mul_s64_overflow(arg1, (int64)arg2, &result)))
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("bigint out of range")));
    PG_RETURN_INT64(result);
}

Datum int84div(PG_FUNCTION_ARGS)
{
    int64 arg1 = PG_GETARG_INT64(0);
    int32 arg2 = PG_GETARG_INT32(1);
    float8 result;

    if (arg2 == 0) {
        ereport(ERROR, (errcode(ERRCODE_DIVISION_BY_ZERO), errmsg("division by zero")));
        /* ensure compiler realizes we mustn't reach the division (gcc bug) */
        PG_RETURN_NULL();
    }

    if (arg1 == 0) {
        PG_RETURN_FLOAT8(0);
    }

    /*
     * INT64_MIN / -1 is problematic, since the result can't be represented on
     * a two's-complement machine.  Some machines produce INT64_MIN, some
     * produce zero, some throw an exception.  We produce an exception like
     * C db and D db do.
     */
    if (arg2 == -1) {
        int128 res = (int128)arg1 * (-1);
        /* overflow check (needed for INT64_MIN) */
        if (SUPPORT_BIND_DIVIDE && unlikely(res > PG_INT64_MAX))
            ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("bigint out of range")));
        PG_RETURN_FLOAT8(res);
    }

    /* No overflow is possible */

    result = (arg1 * 1.0) / (arg2 * 1.0);

    PG_RETURN_FLOAT8(result);
}

Datum int48pl(PG_FUNCTION_ARGS)
{
    int32 arg1 = PG_GETARG_INT32(0);
    int64 arg2 = PG_GETARG_INT64(1);
    int64 result;

    if (unlikely(pg_add_s64_overflow((int64)arg1, arg2, &result)))
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("bigint out of range")));
    PG_RETURN_INT64(result);
}

Datum int48mi(PG_FUNCTION_ARGS)
{
    int32 arg1 = PG_GETARG_INT32(0);
    int64 arg2 = PG_GETARG_INT64(1);
    int64 result;

    result = arg1 - arg2;

    /*
     * Overflow check.	If the inputs are of the same sign then their
     * difference cannot overflow.	If they are of different signs then the
     * result should be of the same sign as the first input.
     */
    if (!SAMESIGN(arg1, arg2) && !SAMESIGN(result, arg1))
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("bigint out of range")));
    PG_RETURN_INT64(result);
}

Datum int48mul(PG_FUNCTION_ARGS)
{
    int32 arg1 = PG_GETARG_INT32(0);
    int64 arg2 = PG_GETARG_INT64(1);
    int64 result;

    result = arg1 * arg2;

    /*
     * Overflow check.	We basically check to see if result / arg2 gives arg1
     * again.  There is one case where this fails: arg2 = 0 (which cannot
     * overflow).
     *
     * Since the division is likely much more expensive than the actual
     * multiplication, we'd like to skip it where possible.  The best bang for
     * the buck seems to be to check whether both inputs are in the int32
     * range; if so, no overflow is possible.
     */
    if (arg2 != (int64)((int32)arg2) && result / arg2 != arg1)
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("bigint out of range")));
    PG_RETURN_INT64(result);
}

Datum int48div(PG_FUNCTION_ARGS)
{
    int32 arg1 = PG_GETARG_INT32(0);
    int64 arg2 = PG_GETARG_INT64(1);

    if (arg2 == 0) {
        ereport(ERROR, (errcode(ERRCODE_DIVISION_BY_ZERO), errmsg("division by zero")));
        /* ensure compiler realizes we mustn't reach the division (gcc bug) */
        PG_RETURN_NULL();
    }

    if (arg1 == 0) {
        PG_RETURN_FLOAT8(0);
    }

    /* No overflow is possible */
    PG_RETURN_FLOAT8((arg1 * 1.0) / (arg2 * 1.0));
}

Datum int82pl(PG_FUNCTION_ARGS)
{
    int64 arg1 = PG_GETARG_INT64(0);
    int16 arg2 = PG_GETARG_INT16(1);
    int64 result;

    result = arg1 + arg2;

    /*
     * Overflow check.	If the inputs are of different signs then their sum
     * cannot overflow.  If the inputs are of the same sign, their sum had
     * better be that sign too.
     */
    if (SAMESIGN(arg1, arg2) && !SAMESIGN(result, arg1))
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("bigint out of range")));
    PG_RETURN_INT64(result);
}

Datum int82mi(PG_FUNCTION_ARGS)
{
    int64 arg1 = PG_GETARG_INT64(0);
    int16 arg2 = PG_GETARG_INT16(1);
    int64 result;

    result = arg1 - arg2;

    /*
     * Overflow check.	If the inputs are of the same sign then their
     * difference cannot overflow.	If they are of different signs then the
     * result should be of the same sign as the first input.
     */
    if (!SAMESIGN(arg1, arg2) && !SAMESIGN(result, arg1))
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("bigint out of range")));
    PG_RETURN_INT64(result);
}

Datum int82mul(PG_FUNCTION_ARGS)
{
    int64 arg1 = PG_GETARG_INT64(0);
    int16 arg2 = PG_GETARG_INT16(1);
    int64 result;

    result = arg1 * arg2;

    /*
     * Overflow check.	We basically check to see if result / arg1 gives arg2
     * again.  There is one case where this fails: arg1 = 0 (which cannot
     * overflow).
     *
     * Since the division is likely much more expensive than the actual
     * multiplication, we'd like to skip it where possible.  The best bang for
     * the buck seems to be to check whether both inputs are in the int32
     * range; if so, no overflow is possible.
     */
    if (arg1 != (int64)((int32)arg1) && result / arg1 != arg2)
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("bigint out of range")));
    PG_RETURN_INT64(result);
}

Datum int82div(PG_FUNCTION_ARGS)
{
    int64 arg1 = PG_GETARG_INT64(0);
    int16 arg2 = PG_GETARG_INT16(1);
    float8 result;

    if (arg2 == 0) {
        ereport(ERROR, (errcode(ERRCODE_DIVISION_BY_ZERO), errmsg("division by zero")));
        /* ensure compiler realizes we mustn't reach the division (gcc bug) */
        PG_RETURN_NULL();
    }

    if (arg1 == 0) {
        PG_RETURN_FLOAT8(0);
    }

    /*
     * INT64_MIN / -1 is problematic, since the result can't be represented on
     * a two's-complement machine.  Some machines produce INT64_MIN, some
     * produce zero, some throw an exception.  We produce an exception like
     * C db and D db do.
     */
    if (arg2 == -1) {
        int128 res = (int128)arg1 * (-1);
        /* overflow check (needed for INT64_MIN) */
        if (SUPPORT_BIND_DIVIDE && unlikely(res > PG_INT64_MAX))
            ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("bigint out of range")));
        PG_RETURN_FLOAT8(res);
    }

    /* No overflow is possible */

    result = (arg1 * 1.0) / (arg2 * 1.0);

    PG_RETURN_FLOAT8(result);
}

Datum int28pl(PG_FUNCTION_ARGS)
{
    int16 arg1 = PG_GETARG_INT16(0);
    int64 arg2 = PG_GETARG_INT64(1);
    int64 result;

    result = arg1 + arg2;

    /*
     * Overflow check.	If the inputs are of different signs then their sum
     * cannot overflow.  If the inputs are of the same sign, their sum had
     * better be that sign too.
     */
    if (SAMESIGN(arg1, arg2) && !SAMESIGN(result, arg1))
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("bigint out of range")));
    PG_RETURN_INT64(result);
}

Datum int28mi(PG_FUNCTION_ARGS)
{
    int16 arg1 = PG_GETARG_INT16(0);
    int64 arg2 = PG_GETARG_INT64(1);
    int64 result;

    result = arg1 - arg2;

    /*
     * Overflow check.	If the inputs are of the same sign then their
     * difference cannot overflow.	If they are of different signs then the
     * result should be of the same sign as the first input.
     */
    if (!SAMESIGN(arg1, arg2) && !SAMESIGN(result, arg1))
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("bigint out of range")));
    PG_RETURN_INT64(result);
}

Datum int28mul(PG_FUNCTION_ARGS)
{
    int16 arg1 = PG_GETARG_INT16(0);
    int64 arg2 = PG_GETARG_INT64(1);
    int64 result;

    result = arg1 * arg2;

    /*
     * Overflow check.	We basically check to see if result / arg2 gives arg1
     * again.  There is one case where this fails: arg2 = 0 (which cannot
     * overflow).
     *
     * Since the division is likely much more expensive than the actual
     * multiplication, we'd like to skip it where possible.  The best bang for
     * the buck seems to be to check whether both inputs are in the int32
     * range; if so, no overflow is possible.
     */
    if (arg2 != (int64)((int32)arg2) && result / arg2 != arg1)
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("bigint out of range")));
    PG_RETURN_INT64(result);
}

Datum int28div(PG_FUNCTION_ARGS)
{
    int16 arg1 = PG_GETARG_INT16(0);
    int64 arg2 = PG_GETARG_INT64(1);

    if (arg2 == 0) {
        ereport(ERROR, (errcode(ERRCODE_DIVISION_BY_ZERO), errmsg("division by zero")));
        /* ensure compiler realizes we mustn't reach the division (gcc bug) */
        PG_RETURN_NULL();
    }

    if (arg1 == 0) {
        PG_RETURN_FLOAT8(0);
    }

    /* No overflow is possible */
    PG_RETURN_FLOAT8((arg1 * 1.0) / (arg2 * 1.0));
}

/* Binary arithmetics
 *
 *		int8and		- returns arg1 & arg2
 *		int8or		- returns arg1 | arg2
 *		int8xor		- returns arg1 # arg2
 *		int8not		- returns ~arg1
 *		int8shl		- returns arg1 << arg2
 *		int8shr		- returns arg1 >> arg2
 */

Datum int8and(PG_FUNCTION_ARGS)
{
    int64 arg1 = PG_GETARG_INT64(0);
    int64 arg2 = PG_GETARG_INT64(1);

    PG_RETURN_INT64(arg1 & arg2);
}

Datum int8or(PG_FUNCTION_ARGS)
{
    int64 arg1 = PG_GETARG_INT64(0);
    int64 arg2 = PG_GETARG_INT64(1);

    PG_RETURN_INT64(arg1 | arg2);
}

Datum int8xor(PG_FUNCTION_ARGS)
{
    int64 arg1 = PG_GETARG_INT64(0);
    int64 arg2 = PG_GETARG_INT64(1);

    PG_RETURN_INT64(arg1 ^ arg2);
}

Datum int8not(PG_FUNCTION_ARGS)
{
    int64 arg1 = PG_GETARG_INT64(0);

    PG_RETURN_INT64(~arg1);
}

Datum int8shl(PG_FUNCTION_ARGS)
{
    int64 arg1 = PG_GETARG_INT64(0);
    int32 arg2 = PG_GETARG_INT32(1);

    PG_RETURN_INT64(arg1 << arg2);
}

Datum int8shr(PG_FUNCTION_ARGS)
{
    int64 arg1 = PG_GETARG_INT64(0);
    int32 arg2 = PG_GETARG_INT32(1);

    PG_RETURN_INT64(arg1 >> arg2);
}

/* ----------------------------------------------------------
 *	Conversion operators.
 * --------------------------------------------------------- */

/* Cast int8 -> bool */
Datum int8_bool(PG_FUNCTION_ARGS)
{
    if (PG_GETARG_INT64(0) == 0)
        PG_RETURN_BOOL(false);
    else
        PG_RETURN_BOOL(true);
}

/* Cast bool -> int8 */
Datum bool_int8(PG_FUNCTION_ARGS)
{
    if (PG_GETARG_BOOL(0) == false)
        PG_RETURN_INT64(0);
    else
        PG_RETURN_INT64(1);
}

Datum int48(PG_FUNCTION_ARGS)
{
    int32 arg = PG_GETARG_INT32(0);

    PG_RETURN_INT64((int64)arg);
}

Datum int84(PG_FUNCTION_ARGS)
{
    int64 arg = PG_GETARG_INT64(0);
    int32 result;

    result = (int32)arg;

    /* Test for overflow by reverse-conversion. */
    if ((int64)result != arg)
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("integer out of range")));

    PG_RETURN_INT32(result);
}

Datum int28(PG_FUNCTION_ARGS)
{
    int16 arg = PG_GETARG_INT16(0);

    PG_RETURN_INT64((int64)arg);
}

Datum int82(PG_FUNCTION_ARGS)
{
    int64 arg = PG_GETARG_INT64(0);
    int16 result;

    result = (int16)arg;

    /* Test for overflow by reverse-conversion. */
    if ((int64)result != arg)
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("smallint out of range")));

    PG_RETURN_INT16(result);
}

Datum i8tod(PG_FUNCTION_ARGS)
{
    int64 arg = PG_GETARG_INT64(0);
    float8 result;

    result = arg;

    PG_RETURN_FLOAT8(result);
}

/* dtoi8()
 * Convert float8 to 8-byte integer.
 */
Datum dtoi8(PG_FUNCTION_ARGS)
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
     * expressed exactly in the float domain.  We expect PG_INT64_MIN  to be an
     * exact power of 2, so it will be represented exactly; but PG_INT64_MAX
     * isn't, and might get rounded off, so avoid using it.
     */
    if (num < (float8)PG_INT64_MIN || num >= -((float8)PG_INT64_MIN) || isnan(num))
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("bigint out of range")));

    PG_RETURN_INT64((int64)num);
}

Datum i8tof(PG_FUNCTION_ARGS)
{
    int64 arg = PG_GETARG_INT64(0);
    float4 result;

    result = arg;

    PG_RETURN_FLOAT4(result);
}

/* ftoi8()
 * Convert float4 to 8-byte integer.
 */
Datum ftoi8(PG_FUNCTION_ARGS)
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
     * expressed exactly in the float domain.  We expect PG_INT64_MIN  to be an
     * exact power of 2, so it will be represented exactly; but PG_INT64_MAX
     * isn't, and might get rounded off, so avoid using it.
     */
    if (num < (float4)PG_INT64_MIN || num >= -((float4)PG_INT64_MIN) || isnan(num))
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("bigint out of range")));

    PG_RETURN_INT64((int64)num);
}

Datum i8tooid(PG_FUNCTION_ARGS)
{
    int64 arg = PG_GETARG_INT64(0);
    Oid result;

    result = (Oid)arg;

    /* Test for overflow by reverse-conversion. */
    if ((int64)result != arg)
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("OID out of range")));

    PG_RETURN_OID(result);
}

Datum oidtoi8(PG_FUNCTION_ARGS)
{
    Oid arg = PG_GETARG_OID(0);

    PG_RETURN_INT64((int64)arg);
}

/*
 * non-persistent numeric series generator
 */
Datum generate_series_int8(PG_FUNCTION_ARGS)
{
    return generate_series_step_int8(fcinfo);
}

Datum generate_series_step_int8(PG_FUNCTION_ARGS)
{
    FuncCallContext* funcctx = NULL;
    generate_series_fctx* fctx = NULL;
    int64 result;
    MemoryContext oldcontext;

    /* stuff done only on the first call of the function */
    if (SRF_IS_FIRSTCALL()) {
        int64 start = PG_GETARG_INT64(0);
        int64 finish = PG_GETARG_INT64(1);
        int64 step = 1;

        /* see if we were given an explicit step size */
        if (PG_NARGS() == 3)
            step = PG_GETARG_INT64(2);
        if (step == 0)
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("step size cannot equal zero")));

        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        /*
         * switch to memory context appropriate for multiple function calls
         */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* allocate memory for user context */
        fctx = (generate_series_fctx*)palloc(sizeof(generate_series_fctx));

        /*
         * Use fctx to keep state from call to call. Seed current with the
         * original start value
         */
        fctx->current = start;
        fctx->finish = finish;
        fctx->step = step;

        funcctx->user_fctx = fctx;
        MemoryContextSwitchTo(oldcontext);
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();

    /*
     * get the saved state and use current as the result for this iteration
     */
    fctx = (generate_series_fctx*)funcctx->user_fctx;
    result = fctx->current;

    if ((fctx->step > 0 && fctx->current <= fctx->finish) || (fctx->step < 0 && fctx->current >= fctx->finish)) {
        /* increment current in preparation for next iteration */
        fctx->current += fctx->step;

        /* if next-value computation overflows, this is the final result */
        if (SAMESIGN(result, fctx->step) && !SAMESIGN(result, fctx->current))
            fctx->step = 0;

        /* do when there is more left to send */
        SRF_RETURN_NEXT(funcctx, Int64GetDatum(result));
    } else
        /* do when there is no more left */
        SRF_RETURN_DONE(funcctx);
}

// vector implementation
ScalarVector* vint4mul(PG_FUNCTION_ARGS)
{
    ScalarValue* parg1 = PG_GETARG_VECVAL(0);
    ScalarValue* parg2 = PG_GETARG_VECVAL(1);
    int32 nvalues = PG_GETARG_INT32(2);
    ScalarValue* presult = PG_GETARG_VECVAL(3);
    bool* pselection = PG_GETARG_SELECTION(4);
    uint8* pflags1 = (uint8*)(PG_GETARG_VECTOR(0)->m_flag);
    uint8* pflags2 = (uint8*)(PG_GETARG_VECTOR(1)->m_flag);
    uint8* pflagsRes = PG_GETARG_VECTOR(3)->m_flag;
    uint32 mask = 0;
    int i;
    int32 arg1, arg2, result;

    if (likely(pselection == NULL)) {
        for (i = 0; i < nvalues; i++) {
            if (BOTH_NOT_NULL(pflags1[i], pflags2[i])) {
                arg1 = (int32)parg1[i];
                arg2 = (int32)parg2[i];

                result = arg1 * arg2;
                mask |= (!(arg1 >= (int32)SHRT_MIN && arg1 <= (int32)SHRT_MAX && arg2 >= (int32)SHRT_MIN &&
                             arg2 <= (int32)SHRT_MAX) &&
                         arg2 != 0 && ((result / arg2 != arg1) || (arg2 == -1 && arg1 < 0 && result < 0)));
                presult[i] = result;
                SET_NOTNULL(pflagsRes[i]);
            } else
                SET_NULL(pflagsRes[i]);
        }
    } else {
        for (i = 0; i < nvalues; i++) {
            if (pselection[i]) {
                if (BOTH_NOT_NULL(pflags1[i], pflags2[i])) {
                    arg1 = (int32)parg1[i];
                    arg2 = (int32)parg2[i];

                    result = arg1 * arg2;
                    mask |= (!(arg1 >= (int32)SHRT_MIN && arg1 <= (int32)SHRT_MAX && arg2 >= (int32)SHRT_MIN &&
                                 arg2 <= (int32)SHRT_MAX) &&
                             arg2 != 0 && ((result / arg2 != arg1) || (arg2 == -1 && arg1 < 0 && result < 0)));
                    presult[i] = result;
                    SET_NOTNULL(pflagsRes[i]);
                } else
                    SET_NULL(pflagsRes[i]);
            }
        }
    }

    if (mask != 0)
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("integer out of range")));

    PG_GETARG_VECTOR(3)->m_rows = nvalues;
    PG_GETARG_VECTOR(3)->m_desc.typeId = INT4OID;
    return PG_GETARG_VECTOR(3);
}

ScalarVector* vint4mi(PG_FUNCTION_ARGS)
{
    ScalarValue* parg1 = PG_GETARG_VECVAL(0);
    ScalarValue* parg2 = PG_GETARG_VECVAL(1);
    int32 nvalues = PG_GETARG_INT32(2);
    ScalarValue* presult = PG_GETARG_VECVAL(3);
    bool* pselection = PG_GETARG_SELECTION(4);
    uint8* pflags1 = (uint8*)(PG_GETARG_VECTOR(0)->m_flag);
    uint8* pflags2 = (uint8*)(PG_GETARG_VECTOR(1)->m_flag);
    uint8* pflagsRes = (uint8*)(PG_GETARG_VECTOR(3)->m_flag);
    uint32 mask = 0;
    int i;
    int32 arg1, arg2, result;

    if (likely(pselection == NULL)) {
        for (i = 0; i < nvalues; i++) {
            if (BOTH_NOT_NULL(pflags1[i], pflags2[i])) {
                arg1 = (int32)parg1[i];
                arg2 = (int32)parg2[i];

                result = arg1 - arg2;
                mask |= !SAMESIGN(arg1, arg2) && !SAMESIGN(result, arg1);
                presult[i] = result;
                SET_NOTNULL(pflagsRes[i]);
            } else
                SET_NULL(pflagsRes[i]);
        }
    } else {
        for (i = 0; i < nvalues; i++) {
            if (pselection[i]) {
                if (BOTH_NOT_NULL(pflags1[i], pflags2[i])) {
                    arg1 = (int32)parg1[i];
                    arg2 = (int32)parg2[i];

                    result = arg1 - arg2;
                    mask |= !SAMESIGN(arg1, arg2) && !SAMESIGN(result, arg1);
                    presult[i] = result;
                    SET_NOTNULL(pflagsRes[i]);
                } else
                    SET_NULL(pflagsRes[i]);
            }
        }
    }

    if (mask != 0) {
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("integer out of range")));
    }

    PG_GETARG_VECTOR(3)->m_rows = nvalues;
    PG_GETARG_VECTOR(3)->m_desc.typeId = INT4OID;
    return PG_GETARG_VECTOR(3);
}

ScalarVector* vint4pl(PG_FUNCTION_ARGS)
{
    ScalarValue* parg1 = PG_GETARG_VECVAL(0);
    ScalarValue* parg2 = PG_GETARG_VECVAL(1);
    int32 nvalues = PG_GETARG_INT32(2);
    ScalarValue* presult = PG_GETARG_VECVAL(3);
    bool* pselection = PG_GETARG_SELECTION(4);
    uint8* pflags1 = (uint8*)(PG_GETARG_VECTOR(0)->m_flag);
    uint8* pflags2 = (uint8*)(PG_GETARG_VECTOR(1)->m_flag);
    uint8* pflagsRes = (uint8*)(PG_GETARG_VECTOR(3)->m_flag);
    uint32 mask = 0;
    int i;
    int32 arg1, arg2, result;

    if (likely(pselection == NULL)) {
        for (i = 0; i < nvalues; i++) {
            if (BOTH_NOT_NULL(pflags1[i], pflags2[i])) {
                arg1 = (int32)parg1[i];
                arg2 = (int32)parg2[i];

                result = arg1 + arg2;
                mask |= SAMESIGN(arg1, arg2) && !SAMESIGN(result, arg1);
                presult[i] = result;
                SET_NOTNULL(pflagsRes[i]);
            } else
                SET_NULL(pflagsRes[i]);
        }
    } else {
        for (i = 0; i < nvalues; i++) {
            if (pselection[i]) {
                if (BOTH_NOT_NULL(pflags1[i], pflags2[i])) {
                    arg1 = (int32)parg1[i];
                    arg2 = (int32)parg2[i];

                    result = arg1 + arg2;
                    mask |= SAMESIGN(arg1, arg2) && !SAMESIGN(result, arg1);
                    presult[i] = result;
                    SET_NOTNULL(pflagsRes[i]);
                } else
                    SET_NULL(pflagsRes[i]);
            }
        }
    }

    if (mask != 0) {
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("integer out of range")));
    }

    PG_GETARG_VECTOR(3)->m_rows = nvalues;
    PG_GETARG_VECTOR(3)->m_desc.typeId = INT4OID;
    return PG_GETARG_VECTOR(3);
}

Datum int8_text(PG_FUNCTION_ARGS)
{
    int64 arg1 = PG_GETARG_INT64(0);
    char* tmp = NULL;
    Datum result;

    tmp = DatumGetCString(DirectFunctionCall1(int8out, arg1));
    result = DirectFunctionCall1(textin, CStringGetDatum(tmp));
    pfree_ext(tmp);

    PG_RETURN_DATUM(result);
}

Datum varchar_int8(PG_FUNCTION_ARGS)
{
    Datum txt = PG_GETARG_DATUM(0);
    char* tmp = NULL;
    Datum result;
    tmp = DatumGetCString(DirectFunctionCall1(varcharout, txt));

    result = DirectFunctionCall1(int8in, CStringGetDatum(tmp));
    pfree_ext(tmp);

    PG_RETURN_DATUM(result);
}

Datum int8_varchar(PG_FUNCTION_ARGS)
{
    int64 val = PG_GETARG_INT64(0);
    char* tmp = NULL;
    Datum result;
    tmp = DatumGetCString(DirectFunctionCall1(int8out, val));

    result = DirectFunctionCall3(varcharin, CStringGetDatum(tmp), ObjectIdGetDatum(0), Int32GetDatum(-1));
    pfree_ext(tmp);

    PG_RETURN_DATUM(result);
}

/*
 * @Description: int8 convert to bpchar.
 * @in arg1 - bigint type numeric.
 * @return bpchar type string.
 */
Datum int8_bpchar(PG_FUNCTION_ARGS)
{
    int64 arg1 = PG_GETARG_INT64(0);
    char* tmp = NULL;
    Datum result;

    tmp = DatumGetCString(DirectFunctionCall1(int8out, arg1));

    result = DirectFunctionCall3(bpcharin, CStringGetDatum(tmp), ObjectIdGetDatum(0), Int32GetDatum(-1));
    pfree_ext(tmp);

    PG_RETURN_DATUM(result);
}

Datum text_int8(PG_FUNCTION_ARGS)
{
    Datum txt = PG_GETARG_DATUM(0);
    char* tmp = NULL;
    Datum result;
    tmp = DatumGetCString(DirectFunctionCall1(textout, txt));

    result = DirectFunctionCall1(int8in, CStringGetDatum(tmp));
    pfree_ext(tmp);

    PG_RETURN_DATUM(result);
}

Datum bpchar_int8(PG_FUNCTION_ARGS)
{
    Datum txt = PG_GETARG_DATUM(0);
    char* tmp = NULL;
    Datum result;
    tmp = DatumGetCString(DirectFunctionCall1(bpcharout, txt));

    result = DirectFunctionCall1(int8in, CStringGetDatum(tmp));
    pfree_ext(tmp);

    PG_RETURN_DATUM(result);
}
