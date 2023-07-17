/* -------------------------------------------------------------------------
 *
 * int.c
 *	  Functions for the built-in integer types (except int8).
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/int.c
 *
 * -------------------------------------------------------------------------
 */
/*
 * OLD COMMENTS
 *		I/O routines:
 *		 int2in, int2out, int2recv, int2send
 *		 int4in, int4out, int4recv, int4send
 *		 int2vectorin, int2vectorout, int2vectorrecv, int2vectorsend
 *		Boolean operators:
 *		 inteq, intne, intlt, intle, intgt, intge
 *		Arithmetic operators:
 *		 intpl, intmi, int4mul, intdiv
 *
 *		Arithmetic operators:
 *		 intmod
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <ctype.h>
#include <limits.h>

#include "catalog/pg_type.h"
#include "common/int.h"
#include "funcapi.h"
#include "libpq/pqformat.h"
#include "utils/array.h"
#include "utils/builtins.h"

#define SAMESIGN(a, b) (((a) < 0) == ((b) < 0))

#define Int2VectorSize(n) (offsetof(int2vector, values) + (n) * sizeof(int16))

typedef struct {
    int32 current;
    int32 finish;
    int32 step;
} generate_series_fctx;

/*****************************************************************************
 *	 USER I/O ROUTINES														 *
 *****************************************************************************/

/*
 *		int2in			- converts "num" to short
 */
Datum int2in(PG_FUNCTION_ARGS)
{
    char* num = PG_GETARG_CSTRING(0);

    PG_RETURN_INT16(pg_strtoint16(num, fcinfo->can_ignore));
}

/*
 *		int2out			- converts short to "num"
 */
Datum int2out(PG_FUNCTION_ARGS)
{
    int16 arg1 = PG_GETARG_INT16(0);
    char* result = (char*)palloc(7); /* sign, 5 digits, '\0' */

    pg_itoa(arg1, result);
    PG_RETURN_CSTRING(result);
}

/*
 *		int2recv			- converts external binary format to int2
 */
Datum int2recv(PG_FUNCTION_ARGS)
{
    StringInfo buf = (StringInfo)PG_GETARG_POINTER(0);

    PG_RETURN_INT16((int16)pq_getmsgint(buf, sizeof(int16)));
}

/*
 *		int2send			- converts int2 to binary format
 */
Datum int2send(PG_FUNCTION_ARGS)
{
    int16 arg1 = PG_GETARG_INT16(0);
    StringInfoData buf;

    pq_begintypsend(&buf);
    pq_sendint16(&buf, arg1);
    PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

/*
 * construct int2vector given a raw array of int2s
 *
 * If int2s is NULL then caller must fill values[] afterward
 */
int2vector* buildint2vector(const int2* int2s, int n)
{
    int2vector* result = NULL;

    result = (int2vector*)palloc0(Int2VectorSize(n));

    if (n > 0 && int2s) {
        errno_t rc = memcpy_s(result->values, n * sizeof(int16), int2s, n * sizeof(int2));
        securec_check(rc, "\0", "\0");
    }

    /*
     * Attach standard array header.  For historical reasons, we set the index
     * lower bound to 0 not 1.
     */
    SET_VARSIZE(result, Int2VectorSize(n));
    result->ndim = 1;
    result->dataoffset = 0; /* never any nulls */
    result->elemtype = INT2OID;
    result->dim1 = n;
    result->lbound1 = 0;

    return result;
}

/*Copy int2vector*/
int2vector* int2vectorCopy(int2vector* from)
{
    if (from == NULL || (from->dim1 == 1 && from->values[0] == 0)) {
        return NULL;
    }

    int2vector* result = NULL;
    int len = from->dim1;
    result = buildint2vector(NULL, len);
    for (int i = 0; i < len; i++) {
        result->values[i] = from->values[i];
    }
    return result;
}

/*
 *		int2vectorin			- converts "num num ..." to internal form
 */
Datum int2vectorin(PG_FUNCTION_ARGS)
{
    char* intString = PG_GETARG_CSTRING(0);
    int2vector* result = NULL;
    int n;

    result = (int2vector*)palloc0(Int2VectorSize(FUNC_MAX_ARGS));

    for (n = 0; *intString && n < FUNC_MAX_ARGS; n++) {
        while (*intString && isspace((unsigned char)*intString))
            intString++;
        if (*intString == '\0')
            break;
        result->values[n] = pg_atoi(intString, sizeof(int16), ' ');
        while (*intString && !isspace((unsigned char)*intString))
            intString++;
    }
    while (*intString && isspace((unsigned char)*intString)) {
        intString++;
    }
    if (*intString)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("int2vector has too many elements")));

    SET_VARSIZE(result, Int2VectorSize(n));
    result->ndim = 1;
    result->dataoffset = 0; /* never any nulls */
    result->elemtype = INT2OID;
    result->dim1 = n;
    result->lbound1 = 0;

    PG_RETURN_POINTER(result);
}

/*
 *		int2vectorout		- converts internal form to "num num ..."
 */
Datum int2vectorout(PG_FUNCTION_ARGS)
{
    int2vector* int2Array = (int2vector*)PG_GETARG_POINTER(0);
    int num, nnums = int2Array->dim1;
    char* rp = NULL;
    char* result = NULL;

    /* assumes sign, 5 digits, ' ' */
    rp = result = (char*)palloc(nnums * 7 + 1);
    for (num = 0; num < nnums; num++) {
        if (num != 0)
            *rp++ = ' ';
        pg_itoa(int2Array->values[num], rp);
        while (*++rp != '\0')
            ;
    }
    *rp = '\0';
    PG_RETURN_CSTRING(result);
}

/*
 *		int2vectorrecv			- converts external binary format to int2vector
 */
Datum int2vectorrecv(PG_FUNCTION_ARGS)
{
    StringInfo buf = (StringInfo)PG_GETARG_POINTER(0);
    FunctionCallInfoData locfcinfo;
    int2vector* result = NULL;

    /*
     * Normally one would call array_recv() using DirectFunctionCall3, but
     * that does not work since array_recv wants to cache some data using
     * fcinfo->flinfo->fn_extra.  So we need to pass it our own flinfo
     * parameter.
     */
    InitFunctionCallInfoData(locfcinfo, fcinfo->flinfo, 3, InvalidOid, NULL, NULL);

    locfcinfo.arg[0] = PointerGetDatum(buf);
    locfcinfo.arg[1] = ObjectIdGetDatum(INT2OID);
    locfcinfo.arg[2] = Int32GetDatum(-1);
    locfcinfo.argnull[0] = false;
    locfcinfo.argnull[1] = false;
    locfcinfo.argnull[2] = false;

    result = (int2vector*)DatumGetPointer(array_recv(&locfcinfo));

    Assert(!locfcinfo.isnull);

    /* sanity checks: int2vector must be 1-D, 0-based, no nulls */
    if (ARR_NDIM(result) != 1 || ARR_HASNULL(result) || ARR_ELEMTYPE(result) != INT2OID || ARR_LBOUND(result)[0] != 0)
        ereport(ERROR, (errcode(ERRCODE_INVALID_BINARY_REPRESENTATION), errmsg("invalid int2vector data")));

    /* check length for consistency with int2vectorin() */
    if (ARR_DIMS(result)[0] > FUNC_MAX_ARGS)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("oidvector has too many elements")));

    PG_RETURN_POINTER(result);
}

/*
 *		int2vectorsend			- converts int2vector to binary format
 */
Datum int2vectorsend(PG_FUNCTION_ARGS)
{
    return array_send(fcinfo);
}

Datum int2vectorin_extend(PG_FUNCTION_ARGS)
{
    return int2vectorin(fcinfo);
}

Datum int2vectorout_extend(PG_FUNCTION_ARGS)
{
    return int2vectorout(fcinfo);
}

Datum int2vectorrecv_extend(PG_FUNCTION_ARGS)
{
    return int2vectorrecv(fcinfo);
}

Datum int2vectorsend_extend(PG_FUNCTION_ARGS)
{
    return int2vectorsend(fcinfo);
}

/*
 * We don't have a complete set of int2vector support routines,
 * but we need int2vectoreq for catcache indexing.
 */
Datum int2vectoreq(PG_FUNCTION_ARGS)
{
    int2vector* a = (int2vector*)PG_GETARG_POINTER(0);
    int2vector* b = (int2vector*)PG_GETARG_POINTER(1);

    if (a->dim1 != b->dim1)
        PG_RETURN_BOOL(false);
    PG_RETURN_BOOL(memcmp(a->values, b->values, a->dim1 * sizeof(int2)) == 0);
}

/*****************************************************************************
 *	 PUBLIC ROUTINES														 *
 *****************************************************************************/

/*
 *		int4in			- converts "num" to int4
 */
Datum int4in(PG_FUNCTION_ARGS)
{
    char* num = PG_GETARG_CSTRING(0);

    PG_RETURN_INT32(pg_strtoint32(num, fcinfo->can_ignore));
}

/*
 *		int4out			- converts int4 to "num"
 */
Datum int4out(PG_FUNCTION_ARGS)
{
    int32 arg1 = PG_GETARG_INT32(0);
    char* result = (char*)palloc(12); /* sign, 10 digits, '\0' */

    pg_ltoa(arg1, result);
    PG_RETURN_CSTRING(result);
}

/*
 *		int4recv			- converts external binary format to int4
 */
Datum int4recv(PG_FUNCTION_ARGS)
{
    StringInfo buf = (StringInfo)PG_GETARG_POINTER(0);

    PG_RETURN_INT32((int32)pq_getmsgint(buf, sizeof(int32)));
}

/*
 *		int4send			- converts int4 to binary format
 */
Datum int4send(PG_FUNCTION_ARGS)
{
    int32 arg1 = PG_GETARG_INT32(0);
    StringInfoData buf;

    pq_begintypsend(&buf);
    pq_sendint32(&buf, arg1);
    PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

/*
 *		===================
 *		CONVERSION ROUTINES
 *		===================
 */

Datum i2toi4(PG_FUNCTION_ARGS)
{
    int16 arg1 = PG_GETARG_INT16(0);

    PG_RETURN_INT32((int32)arg1);
}

Datum i4toi2(PG_FUNCTION_ARGS)
{
    int32 arg1 = PG_GETARG_INT32(0);

    if (unlikely(arg1 < SHRT_MIN) || unlikely(arg1 > SHRT_MAX)) {
        if (fcinfo->can_ignore) {
            ereport(WARNING, (errmsg("smallint out of range")));
            PG_RETURN_INT16((int16)(arg1 < SHRT_MIN ? SHRT_MIN : SHRT_MAX));
        }
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("smallint out of range")));
    }

    PG_RETURN_INT16((int16)arg1);
}

/* Cast int4 -> bool */
Datum int4_bool(PG_FUNCTION_ARGS)
{
    if (PG_GETARG_INT32(0) == 0)
        PG_RETURN_BOOL(false);
    else
        PG_RETURN_BOOL(true);
}

/* Cast bool -> int4 */
Datum bool_int4(PG_FUNCTION_ARGS)
{
    if (PG_GETARG_BOOL(0) == false)
        PG_RETURN_INT32(0);
    else
        PG_RETURN_INT32(1);
}

/* Cast int2 -> bool */
Datum int2_bool(PG_FUNCTION_ARGS)
{
    if (PG_GETARG_INT16(0) == 0)
        PG_RETURN_BOOL(false);
    else
        PG_RETURN_BOOL(true);
}

/* Cast bool -> int2 */
Datum bool_int2(PG_FUNCTION_ARGS)
{
    if (PG_GETARG_BOOL(0) == false)
        PG_RETURN_INT16(0);
    else
        PG_RETURN_INT16(1);
}

/*
 *		============================
 *		COMPARISON OPERATOR ROUTINES
 *		============================
 */

/*
 *		inteq			- returns 1 iff arg1 == arg2
 *		intne			- returns 1 iff arg1 != arg2
 *		intlt			- returns 1 iff arg1 < arg2
 *		intle			- returns 1 iff arg1 <= arg2
 *		intgt			- returns 1 iff arg1 > arg2
 *		intge			- returns 1 iff arg1 >= arg2
 */

Datum int4eq(PG_FUNCTION_ARGS)
{
    int32 arg1 = PG_GETARG_INT32(0);
    int32 arg2 = PG_GETARG_INT32(1);

    PG_RETURN_BOOL(arg1 == arg2);
}

Datum int4ne(PG_FUNCTION_ARGS)
{
    int32 arg1 = PG_GETARG_INT32(0);
    int32 arg2 = PG_GETARG_INT32(1);

    PG_RETURN_BOOL(arg1 != arg2);
}

Datum int4lt(PG_FUNCTION_ARGS)
{
    int32 arg1 = PG_GETARG_INT32(0);
    int32 arg2 = PG_GETARG_INT32(1);

    PG_RETURN_BOOL(arg1 < arg2);
}

Datum int4le(PG_FUNCTION_ARGS)
{
    int32 arg1 = PG_GETARG_INT32(0);
    int32 arg2 = PG_GETARG_INT32(1);

    PG_RETURN_BOOL(arg1 <= arg2);
}

Datum int4gt(PG_FUNCTION_ARGS)
{
    int32 arg1 = PG_GETARG_INT32(0);
    int32 arg2 = PG_GETARG_INT32(1);

    PG_RETURN_BOOL(arg1 > arg2);
}

Datum int4ge(PG_FUNCTION_ARGS)
{
    int32 arg1 = PG_GETARG_INT32(0);
    int32 arg2 = PG_GETARG_INT32(1);

    PG_RETURN_BOOL(arg1 >= arg2);
}

Datum int2eq(PG_FUNCTION_ARGS)
{
    int16 arg1 = PG_GETARG_INT16(0);
    int16 arg2 = PG_GETARG_INT16(1);

    PG_RETURN_BOOL(arg1 == arg2);
}

Datum int2ne(PG_FUNCTION_ARGS)
{
    int16 arg1 = PG_GETARG_INT16(0);
    int16 arg2 = PG_GETARG_INT16(1);

    PG_RETURN_BOOL(arg1 != arg2);
}

Datum int2lt(PG_FUNCTION_ARGS)
{
    int16 arg1 = PG_GETARG_INT16(0);
    int16 arg2 = PG_GETARG_INT16(1);

    PG_RETURN_BOOL(arg1 < arg2);
}

Datum int2le(PG_FUNCTION_ARGS)
{
    int16 arg1 = PG_GETARG_INT16(0);
    int16 arg2 = PG_GETARG_INT16(1);

    PG_RETURN_BOOL(arg1 <= arg2);
}

Datum int2gt(PG_FUNCTION_ARGS)
{
    int16 arg1 = PG_GETARG_INT16(0);
    int16 arg2 = PG_GETARG_INT16(1);

    PG_RETURN_BOOL(arg1 > arg2);
}

Datum int2ge(PG_FUNCTION_ARGS)
{
    int16 arg1 = PG_GETARG_INT16(0);
    int16 arg2 = PG_GETARG_INT16(1);

    PG_RETURN_BOOL(arg1 >= arg2);
}

Datum int24eq(PG_FUNCTION_ARGS)
{
    int16 arg1 = PG_GETARG_INT16(0);
    int32 arg2 = PG_GETARG_INT32(1);

    PG_RETURN_BOOL(arg1 == arg2);
}

Datum int24ne(PG_FUNCTION_ARGS)
{
    int16 arg1 = PG_GETARG_INT16(0);
    int32 arg2 = PG_GETARG_INT32(1);

    PG_RETURN_BOOL(arg1 != arg2);
}

Datum int24lt(PG_FUNCTION_ARGS)
{
    int16 arg1 = PG_GETARG_INT16(0);
    int32 arg2 = PG_GETARG_INT32(1);

    PG_RETURN_BOOL(arg1 < arg2);
}

Datum int24le(PG_FUNCTION_ARGS)
{
    int16 arg1 = PG_GETARG_INT16(0);
    int32 arg2 = PG_GETARG_INT32(1);

    PG_RETURN_BOOL(arg1 <= arg2);
}

Datum int24gt(PG_FUNCTION_ARGS)
{
    int16 arg1 = PG_GETARG_INT16(0);
    int32 arg2 = PG_GETARG_INT32(1);

    PG_RETURN_BOOL(arg1 > arg2);
}

Datum int24ge(PG_FUNCTION_ARGS)
{
    int16 arg1 = PG_GETARG_INT16(0);
    int32 arg2 = PG_GETARG_INT32(1);

    PG_RETURN_BOOL(arg1 >= arg2);
}

Datum int42eq(PG_FUNCTION_ARGS)
{
    int32 arg1 = PG_GETARG_INT32(0);
    int16 arg2 = PG_GETARG_INT16(1);

    PG_RETURN_BOOL(arg1 == arg2);
}

Datum int42ne(PG_FUNCTION_ARGS)
{
    int32 arg1 = PG_GETARG_INT32(0);
    int16 arg2 = PG_GETARG_INT16(1);

    PG_RETURN_BOOL(arg1 != arg2);
}

Datum int42lt(PG_FUNCTION_ARGS)
{
    int32 arg1 = PG_GETARG_INT32(0);
    int16 arg2 = PG_GETARG_INT16(1);

    PG_RETURN_BOOL(arg1 < arg2);
}

Datum int42le(PG_FUNCTION_ARGS)
{
    int32 arg1 = PG_GETARG_INT32(0);
    int16 arg2 = PG_GETARG_INT16(1);

    PG_RETURN_BOOL(arg1 <= arg2);
}

Datum int42gt(PG_FUNCTION_ARGS)
{
    int32 arg1 = PG_GETARG_INT32(0);
    int16 arg2 = PG_GETARG_INT16(1);

    PG_RETURN_BOOL(arg1 > arg2);
}

Datum int42ge(PG_FUNCTION_ARGS)
{
    int32 arg1 = PG_GETARG_INT32(0);
    int16 arg2 = PG_GETARG_INT16(1);

    PG_RETURN_BOOL(arg1 >= arg2);
}

/*
 *		int[24]pl		- returns arg1 + arg2
 *		int[24]mi		- returns arg1 - arg2
 *		int[24]mul		- returns arg1 * arg2
 *		int[24]div		- returns arg1 / arg2
 */

Datum int4um(PG_FUNCTION_ARGS)
{
    int32 arg = PG_GETARG_INT32(0);

    if (unlikely(arg == PG_INT32_MIN))
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("integer out of range")));
    PG_RETURN_INT32(-arg);
}

Datum int4up(PG_FUNCTION_ARGS)
{
    int32 arg = PG_GETARG_INT32(0);

    PG_RETURN_INT32(arg);
}

Datum int4pl(PG_FUNCTION_ARGS)
{
    int32 arg1 = PG_GETARG_INT32(0);
    int32 arg2 = PG_GETARG_INT32(1);
    int32 result;

    if (unlikely(pg_add_s32_overflow(arg1, arg2, &result)))
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("integer out of range")));
    PG_RETURN_INT32(result);
}

Datum int4mi(PG_FUNCTION_ARGS)
{
    int32 arg1 = PG_GETARG_INT32(0);
    int32 arg2 = PG_GETARG_INT32(1);
    int32 result;

    if (unlikely(pg_sub_s32_overflow(arg1, arg2, &result)))
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("integer out of range")));
    PG_RETURN_INT32(result);
}

Datum int4mul(PG_FUNCTION_ARGS)
{
    int32 arg1 = PG_GETARG_INT32(0);
    int32 arg2 = PG_GETARG_INT32(1);
    int32 result;

    if (unlikely(pg_mul_s32_overflow(arg1, arg2, &result)))
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("integer out of range")));
    PG_RETURN_INT32(result);
}

Datum int4div(PG_FUNCTION_ARGS)
{
    int32 arg1 = PG_GETARG_INT32(0);
    int32 arg2 = PG_GETARG_INT32(1);
    // A db compatibility change the result to float.
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
     * INT_MIN / -1 is problematic, since the result can't be represented on a
     * two's-complement machine.  Some machines produce INT_MIN, some produce
     * zero, some throw an exception.  We can dodge the problem by recognizing
     * that division by -1 is the same as negation.
     */
    // A db compatibility change the result to float. This check is nessessary
    // to avoid overflow when INT_MIN / (-1)
    if (arg2 == -1) {
        int64 res = (int64)arg1 * (-1);
        /* overflow check (needed for INT_MIN) */
        if (SUPPORT_BIND_DIVIDE && unlikely(res > PG_INT32_MAX))
            ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("integer out of range")));
        PG_RETURN_FLOAT8(res);
    }

    /* No overflow is possible */

    result = (arg1 * 1.0) / (arg2 * 1.0);

    PG_RETURN_FLOAT8(result);
}

Datum int4inc(PG_FUNCTION_ARGS)
{
    int32 arg = PG_GETARG_INT32(0);
    int32 result;

    if (unlikely(pg_add_s32_overflow(arg, 1, &result)))
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("integer out of range")));

    PG_RETURN_INT32(result);
}

Datum int2um(PG_FUNCTION_ARGS)
{
    int16 arg = PG_GETARG_INT16(0);

    if (unlikely(arg == PG_INT16_MIN))
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("smallint out of range")));
    PG_RETURN_INT16(-arg);
}

Datum int2up(PG_FUNCTION_ARGS)
{
    int16 arg = PG_GETARG_INT16(0);

    PG_RETURN_INT16(arg);
}

Datum int2pl(PG_FUNCTION_ARGS)
{
    int16 arg1 = PG_GETARG_INT16(0);
    int16 arg2 = PG_GETARG_INT16(1);
    int16 result;

    if (unlikely(pg_add_s16_overflow(arg1, arg2, &result)))
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("smallint out of range")));
    PG_RETURN_INT16(result);
}

Datum int2mi(PG_FUNCTION_ARGS)
{
    int16 arg1 = PG_GETARG_INT16(0);
    int16 arg2 = PG_GETARG_INT16(1);
    int16 result;

    if (unlikely(pg_sub_s16_overflow(arg1, arg2, &result)))
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("smallint out of range")));
    PG_RETURN_INT16(result);
}

Datum int2mul(PG_FUNCTION_ARGS)
{
    int16 arg1 = PG_GETARG_INT16(0);
    int16 arg2 = PG_GETARG_INT16(1);
    int16 result;

    if (unlikely(pg_mul_s16_overflow(arg1, arg2, &result)))
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("smallint out of range")));

    PG_RETURN_INT16(result);
}

Datum int2div(PG_FUNCTION_ARGS)
{
    int16 arg1 = PG_GETARG_INT16(0);
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
     * SHRT_MIN / -1 is problematic, since the result can't be represented on
     * a two's-complement machine.  Some machines produce SHRT_MIN, some
     * produce zero, some throw an exception.  We produce an exception like
     * C db and D db do.
     */
    if (arg2 == -1) {
        int32 res = (int32)arg1 * (-1);
        /* overflow check (needed for SHRT_MIN) */
        if (SUPPORT_BIND_DIVIDE && unlikely(res > PG_INT16_MAX))
            ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("smallint out of range")));
        PG_RETURN_FLOAT8(res);
    }

    /* No overflow is possible */

    result = (arg1 * 1.0) / (arg2 * 1.0);

    PG_RETURN_FLOAT8(result);
}

Datum int24pl(PG_FUNCTION_ARGS)
{
    int16 arg1 = PG_GETARG_INT16(0);
    int32 arg2 = PG_GETARG_INT32(1);
    int32 result;

    if (unlikely(pg_add_s32_overflow((int32)arg1, arg2, &result)))
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("integer out of range")));
    PG_RETURN_INT32(result);
}

Datum int24mi(PG_FUNCTION_ARGS)
{
    int16 arg1 = PG_GETARG_INT16(0);
    int32 arg2 = PG_GETARG_INT32(1);
    int32 result;

    if (unlikely(pg_sub_s32_overflow((int32)arg1, arg2, &result)))
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("integer out of range")));
    PG_RETURN_INT32(result);
}

Datum int24mul(PG_FUNCTION_ARGS)
{
    int16 arg1 = PG_GETARG_INT16(0);
    int32 arg2 = PG_GETARG_INT32(1);
    int32 result;

    if (unlikely(pg_mul_s32_overflow((int32)arg1, arg2, &result)))
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("integer out of range")));
    PG_RETURN_INT32(result);
}

Datum int24div(PG_FUNCTION_ARGS)
{
    int16 arg1 = PG_GETARG_INT16(0);
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

    result = (arg1 * 1.0) / (arg2 * 1.0);
    PG_RETURN_FLOAT8(result);
}

Datum int42pl(PG_FUNCTION_ARGS)
{
    int32 arg1 = PG_GETARG_INT32(0);
    int16 arg2 = PG_GETARG_INT16(1);
    int32 result;

    if (unlikely(pg_add_s32_overflow(arg1, (int32)arg2, &result)))
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("integer out of range")));
    PG_RETURN_INT32(result);
}

Datum int42mi(PG_FUNCTION_ARGS)
{
    int32 arg1 = PG_GETARG_INT32(0);
    int16 arg2 = PG_GETARG_INT16(1);
    int32 result;

    if (unlikely(pg_sub_s32_overflow(arg1, (int32)arg2, &result)))
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("integer out of range")));
    PG_RETURN_INT32(result);
}

Datum int42mul(PG_FUNCTION_ARGS)
{
    int32 arg1 = PG_GETARG_INT32(0);
    int16 arg2 = PG_GETARG_INT16(1);
    int32 result;

    if (unlikely(pg_mul_s32_overflow(arg1, (int32)arg2, &result)))
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("integer out of range")));
    PG_RETURN_INT32(result);
}

Datum int42div(PG_FUNCTION_ARGS)
{
    int32 arg1 = PG_GETARG_INT32(0);
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
     * INT_MIN / -1 is problematic, since the result can't be represented on a
     * two's-complement machine.  Some machines produce INT_MIN, some produce
     * zero, some throw an exception.  We produce an exception like
     * C db and D db do.
     */
    if (arg2 == -1) {
        int64 res = (int64)arg1 * (-1);
        /* overflow check (needed for INT_MIN) */
        if (SUPPORT_BIND_DIVIDE && unlikely(res > PG_INT32_MAX))
            ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("integer out of range")));
        PG_RETURN_FLOAT8(res);
    }

    /* No overflow is possible */

    result = (arg1 * 1.0) / (arg2 * 1.0);
    PG_RETURN_FLOAT8(result);
}

Datum int4mod(PG_FUNCTION_ARGS)
{
    int32 arg1 = PG_GETARG_INT32(0);
    int32 arg2 = PG_GETARG_INT32(1);

    if (unlikely(arg2 == 0)) {
        if (DB_IS_CMPT(PG_FORMAT)) {
            /* zero is not allowed to be divisor if compatible with PG */
            ereport(ERROR, (errcode(ERRCODE_DIVISION_BY_ZERO), errmsg("division by zero")));

            /* ensure compiler realizes we mustn't reach the division (gcc bug) */
            PG_RETURN_NULL();
        }
        /* zero is allowed to be divisor */
        PG_RETURN_INT32(arg1);
    }

    /*
     * Some machines throw a floating-point exception for INT_MIN % -1, which
     * is a bit silly since the correct answer is perfectly well-defined,
     * namely zero.
     */
    if (arg2 == -1)
        PG_RETURN_INT32(0);

    /* No overflow is possible */

    PG_RETURN_INT32(arg1 % arg2);
}

Datum int2mod(PG_FUNCTION_ARGS)
{
    int16 arg1 = PG_GETARG_INT16(0);
    int16 arg2 = PG_GETARG_INT16(1);

    if (unlikely(arg2 == 0)) {
        if (DB_IS_CMPT(PG_FORMAT)) {
            /* zero is not allowed to be divisor if compatible with PG */
            ereport(ERROR, (errcode(ERRCODE_DIVISION_BY_ZERO), errmsg("division by zero")));

            /* ensure compiler realizes we mustn't reach the division (gcc bug) */
            PG_RETURN_NULL();
        }
        /* zero is allowed to be divisor */
        PG_RETURN_INT16(arg1);
    }

    /*
     * Some machines throw a floating-point exception for INT_MIN % -1, which
     * is a bit silly since the correct answer is perfectly well-defined,
     * namely zero.  (It's not clear this ever happens when dealing with
     * int16, but we might as well have the test for safety.)
     */
    if (arg2 == -1)
        PG_RETURN_INT16(0);

    /* No overflow is possible */

    PG_RETURN_INT16(arg1 % arg2);
}

/* int[24]abs()
 * Absolute value
 */
Datum int4abs(PG_FUNCTION_ARGS)
{
    int32 arg1 = PG_GETARG_INT32(0);
    int32 result;

    if (unlikely(arg1 == PG_INT32_MIN))
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("integer out of range")));
    result = (arg1 < 0) ? -arg1 : arg1;
    PG_RETURN_INT32(result);
}

Datum int2abs(PG_FUNCTION_ARGS)
{
    int16 arg1 = PG_GETARG_INT16(0);
    int16 result;

    if (unlikely(arg1 == PG_INT16_MIN))
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("smallint out of range")));
    result = (arg1 < 0) ? -arg1 : arg1;
    PG_RETURN_INT16(result);
}

Datum int2larger(PG_FUNCTION_ARGS)
{
    int16 arg1 = PG_GETARG_INT16(0);
    int16 arg2 = PG_GETARG_INT16(1);

    PG_RETURN_INT16((arg1 > arg2) ? arg1 : arg2);
}

Datum int2smaller(PG_FUNCTION_ARGS)
{
    int16 arg1 = PG_GETARG_INT16(0);
    int16 arg2 = PG_GETARG_INT16(1);

    PG_RETURN_INT16((arg1 < arg2) ? arg1 : arg2);
}

Datum int4larger(PG_FUNCTION_ARGS)
{
    int32 arg1 = PG_GETARG_INT32(0);
    int32 arg2 = PG_GETARG_INT32(1);

    PG_RETURN_INT32((arg1 > arg2) ? arg1 : arg2);
}

Datum int4smaller(PG_FUNCTION_ARGS)
{
    int32 arg1 = PG_GETARG_INT32(0);
    int32 arg2 = PG_GETARG_INT32(1);

    PG_RETURN_INT32((arg1 < arg2) ? arg1 : arg2);
}

/*
 * Bit-pushing operators
 *
 *		int[24]and		- returns arg1 & arg2
 *		int[24]or		- returns arg1 | arg2
 *		int[24]xor		- returns arg1 # arg2
 *		int[24]not		- returns ~arg1
 *		int[24]shl		- returns arg1 << arg2
 *		int[24]shr		- returns arg1 >> arg2
 */

Datum int4and(PG_FUNCTION_ARGS)
{
    int32 arg1 = PG_GETARG_INT32(0);
    int32 arg2 = PG_GETARG_INT32(1);

    PG_RETURN_INT32(arg1 & arg2);
}

Datum int4or(PG_FUNCTION_ARGS)
{
    int32 arg1 = PG_GETARG_INT32(0);
    int32 arg2 = PG_GETARG_INT32(1);

    PG_RETURN_INT32(arg1 | arg2);
}

Datum int4xor(PG_FUNCTION_ARGS)
{
    int32 arg1 = PG_GETARG_INT32(0);
    int32 arg2 = PG_GETARG_INT32(1);

    PG_RETURN_INT32(arg1 ^ arg2);
}

Datum int4shl(PG_FUNCTION_ARGS)
{
    int32 arg1 = PG_GETARG_INT32(0);
    int32 arg2 = PG_GETARG_INT32(1);

    PG_RETURN_INT32(arg1 << arg2);
}

Datum int4shr(PG_FUNCTION_ARGS)
{
    int32 arg1 = PG_GETARG_INT32(0);
    int32 arg2 = PG_GETARG_INT32(1);

    PG_RETURN_INT32(arg1 >> arg2);
}

Datum int4not(PG_FUNCTION_ARGS)
{
    int32 arg1 = PG_GETARG_INT32(0);

    PG_RETURN_INT32(~arg1);
}

Datum int2and(PG_FUNCTION_ARGS)
{
    int16 arg1 = PG_GETARG_INT16(0);
    int16 arg2 = PG_GETARG_INT16(1);

    PG_RETURN_INT16(arg1 & arg2);
}

Datum int2or(PG_FUNCTION_ARGS)
{
    int16 arg1 = PG_GETARG_INT16(0);
    int16 arg2 = PG_GETARG_INT16(1);

    PG_RETURN_INT16(arg1 | arg2);
}

Datum int2xor(PG_FUNCTION_ARGS)
{
    int16 arg1 = PG_GETARG_INT16(0);
    int16 arg2 = PG_GETARG_INT16(1);

    PG_RETURN_INT16(arg1 ^ arg2);
}

Datum int2not(PG_FUNCTION_ARGS)
{
    int16 arg1 = PG_GETARG_INT16(0);

    PG_RETURN_INT16(~arg1);
}

Datum int2shl(PG_FUNCTION_ARGS)
{
    int16 arg1 = PG_GETARG_INT16(0);
    int32 arg2 = PG_GETARG_INT32(1);

    PG_RETURN_INT16(arg1 << arg2);
}

Datum int2shr(PG_FUNCTION_ARGS)
{
    int16 arg1 = PG_GETARG_INT16(0);
    int32 arg2 = PG_GETARG_INT32(1);

    PG_RETURN_INT16(arg1 >> arg2);
}

/*
 * non-persistent numeric series generator
 */
Datum generate_series_int4(PG_FUNCTION_ARGS)
{
    return generate_series_step_int4(fcinfo);
}

Datum generate_series_step_int4(PG_FUNCTION_ARGS)
{
    FuncCallContext* funcctx = NULL;
    generate_series_fctx* fctx = NULL;
    int32 result;
    MemoryContext oldcontext;

    /* stuff done only on the first call of the function */
    if (SRF_IS_FIRSTCALL()) {
        int32 start = PG_GETARG_INT32(0);
        int32 finish = PG_GETARG_INT32(1);
        int32 step = 1;

        /* see if we were given an explicit step size */
        if (PG_NARGS() == 3)
            step = PG_GETARG_INT32(2);
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
        /*
         * Increment current in preparation for next iteration. If next-value
         * computation overflows, this is the final result.
         */
        if (pg_add_s32_overflow(fctx->current, fctx->step, &fctx->current))
            fctx->step = 0;

        /* do when there is more left to send */
        SRF_RETURN_NEXT(funcctx, Int32GetDatum(result));
    }
        /* do when there is no more left */
        SRF_RETURN_DONE(funcctx);
}

// int1in - converts "num" to uint8
Datum int1in(PG_FUNCTION_ARGS)
{
    char* num = PG_GETARG_CSTRING(0);

    PG_RETURN_UINT8((uint8)pg_atoi(num, sizeof(uint8), '\0', fcinfo->can_ignore));
}

// int1out - converts uint8 to "num"
Datum int1out(PG_FUNCTION_ARGS)
{
    uint8 arg1 = PG_GETARG_UINT8(0);
    char* result = (char*)palloc(5); /* sign, 3 digits, '\0' */

    pg_ctoa(arg1, result);
    PG_RETURN_CSTRING(result);
}

// int1recv - converts external binary format to uint8
Datum int1recv(PG_FUNCTION_ARGS)
{
    StringInfo buf = (StringInfo)PG_GETARG_POINTER(0);

    PG_RETURN_UINT8((uint8)pq_getmsgint(buf, sizeof(uint8)));
}

// int1send - converts uint8 to binary format
Datum int1send(PG_FUNCTION_ARGS)
{
    uint8 arg1 = PG_GETARG_UINT8(0);
    StringInfoData buf;

    pq_begintypsend(&buf);
    pq_sendint8(&buf, arg1);
    PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

Datum int1and(PG_FUNCTION_ARGS)
{
    uint8 arg1 = PG_GETARG_UINT8(0);
    uint8 arg2 = PG_GETARG_UINT8(1);

    PG_RETURN_UINT8(arg1 & arg2);
}

Datum int1or(PG_FUNCTION_ARGS)
{
    uint8 arg1 = PG_GETARG_UINT8(0);
    uint8 arg2 = PG_GETARG_UINT8(1);

    PG_RETURN_UINT8(arg1 | arg2);
}

Datum int1xor(PG_FUNCTION_ARGS)
{
    uint8 arg1 = PG_GETARG_UINT8(0);
    uint8 arg2 = PG_GETARG_UINT8(1);

    PG_RETURN_UINT8(arg1 ^ arg2);
}

Datum int1not(PG_FUNCTION_ARGS)
{
    uint8 arg1 = PG_GETARG_UINT8(0);

    PG_RETURN_UINT8((uint8)(~arg1));
}

Datum int1shl(PG_FUNCTION_ARGS)
{
    uint8 arg1 = PG_GETARG_UINT8(0);
    int32 arg2 = PG_GETARG_INT32(1);

    PG_RETURN_UINT8((uint8)(arg1 << arg2));
}

Datum int1shr(PG_FUNCTION_ARGS)
{
    uint8 arg1 = PG_GETARG_UINT8(0);
    int32 arg2 = PG_GETARG_INT32(1);

    PG_RETURN_UINT8(arg1 >> arg2);
}

/*
 * adapt Sybase, add a new data type int1
 *		int1eq			- returns 1 if arg1 == arg2
 *		int1ne			- returns 1 if arg1 != arg2
 *		int1lt			- returns 1 if arg1 < arg2
 *		int1le			- returns 1 if arg1 <= arg2
 *		int1gt			- returns 1 if arg1 > arg2
 *		int1ge			- returns 1 if arg1 >= arg2
 */
Datum int1eq(PG_FUNCTION_ARGS)
{
    uint8 arg1 = PG_GETARG_UINT8(0);
    uint8 arg2 = PG_GETARG_UINT8(1);

    PG_RETURN_BOOL(arg1 == arg2);
}

Datum int1ne(PG_FUNCTION_ARGS)
{
    uint8 arg1 = PG_GETARG_UINT8(0);
    uint8 arg2 = PG_GETARG_UINT8(1);

    PG_RETURN_BOOL(arg1 != arg2);
}

Datum int1lt(PG_FUNCTION_ARGS)
{
    uint8 arg1 = PG_GETARG_UINT8(0);
    uint8 arg2 = PG_GETARG_UINT8(1);

    PG_RETURN_BOOL(arg1 < arg2);
}

Datum int1le(PG_FUNCTION_ARGS)
{
    uint8 arg1 = PG_GETARG_UINT8(0);
    uint8 arg2 = PG_GETARG_UINT8(1);

    PG_RETURN_BOOL(arg1 <= arg2);
}

Datum int1gt(PG_FUNCTION_ARGS)
{
    uint8 arg1 = PG_GETARG_UINT8(0);
    uint8 arg2 = PG_GETARG_UINT8(1);

    PG_RETURN_BOOL(arg1 > arg2);
}

Datum int1ge(PG_FUNCTION_ARGS)
{
    uint8 arg1 = PG_GETARG_UINT8(0);
    uint8 arg2 = PG_GETARG_UINT8(1);

    PG_RETURN_BOOL(arg1 >= arg2);
}
Datum int1cmp(PG_FUNCTION_ARGS)
{
    uint8 arg1 = PG_GETARG_UINT8(0);
    uint8 arg2 = PG_GETARG_UINT8(1);

    if (arg1 > arg2)
        PG_RETURN_INT32(1);
    else if (arg1 == arg2)
        PG_RETURN_INT32(0);
    else
        PG_RETURN_INT32(-1);
}

/*
 *		===================
 *		CONVERSION ROUTINES
 *		===================
 */
Datum i1toi2(PG_FUNCTION_ARGS)
{
    uint8 arg1 = PG_GETARG_UINT8(0);

    PG_RETURN_INT16((int16)arg1);
}

Datum i2toi1(PG_FUNCTION_ARGS)
{
    int16 arg1 = PG_GETARG_INT16(0);

    if (arg1 < 0 || arg1 > UCHAR_MAX) {
        if (fcinfo->can_ignore) {
            ereport(WARNING, (errmsg("tinyint out of range")));
            PG_RETURN_UINT8((uint8)(arg1 < 0 ? 0 : UCHAR_MAX));
        }
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("tinyint out of range")));
    }

    PG_RETURN_UINT8((uint8)arg1);
}

Datum i1toi4(PG_FUNCTION_ARGS)
{
    uint8 arg1 = PG_GETARG_UINT8(0);

    PG_RETURN_INT32((int32)arg1);
}

Datum i4toi1(PG_FUNCTION_ARGS)
{
    int32 arg1 = PG_GETARG_INT32(0);

    if (arg1 < 0 || arg1 > UCHAR_MAX) {
        if (fcinfo->can_ignore) {
            ereport(WARNING, (errmsg("tinyint out of range")));
            PG_RETURN_UINT8((uint8)(arg1 < 0 ? 0 : UCHAR_MAX));
        }
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("tinyint out of range")));
    }

    PG_RETURN_UINT8((uint8)arg1);
}

Datum i1toi8(PG_FUNCTION_ARGS)
{
    uint8 arg1 = PG_GETARG_UINT8(0);

    PG_RETURN_INT64((int64)arg1);
}

Datum i8toi1(PG_FUNCTION_ARGS)
{
    int64 arg1 = PG_GETARG_INT64(0);

    if (arg1 < 0 || arg1 > UCHAR_MAX) {
        if (fcinfo->can_ignore) {
            ereport(WARNING, (errmsg("tinyint out of range")));
            PG_RETURN_UINT8((uint8)(arg1 < 0 ? 0 : UCHAR_MAX));
        }
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("tinyint out of range")));
    }

    PG_RETURN_UINT8((uint8)arg1);
}

Datum i1tof4(PG_FUNCTION_ARGS)
{
    uint8 arg1 = PG_GETARG_UINT8(0);

    PG_RETURN_FLOAT4((float4)arg1);
}

Datum i1tof8(PG_FUNCTION_ARGS)
{
    uint8 arg1 = PG_GETARG_UINT8(0);

    PG_RETURN_FLOAT8((float8)arg1);
}

Datum f4toi1(PG_FUNCTION_ARGS)
{
    float4 arg1 = PG_GETARG_FLOAT4(0);

    if (arg1 < 0 || arg1 > UCHAR_MAX) {
        if (fcinfo->can_ignore) {
            ereport(WARNING, (errmsg("tinyint out of range")));
            PG_RETURN_UINT8(arg1 < 0 ? 0 : UCHAR_MAX);
        }
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("tinyint out of range")));
    }

    PG_RETURN_UINT8((uint8)arg1);
}

Datum f8toi1(PG_FUNCTION_ARGS)
{
    float8 arg1 = PG_GETARG_FLOAT8(0);

    if (arg1 < 0 || arg1 > UCHAR_MAX) {
        if (fcinfo->can_ignore) {
            ereport(WARNING, (errmsg("tinyint out of range")));
            PG_RETURN_UINT8(arg1 < 0 ? 0 : UCHAR_MAX);
        }
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("tinyint out of range")));
    }

    PG_RETURN_UINT8((uint8)arg1);
}

/* Cast int1 -> bool */
Datum int1_bool(PG_FUNCTION_ARGS)
{
    if (PG_GETARG_UINT8(0) == 0)
        PG_RETURN_BOOL(false);
    else
        PG_RETURN_BOOL(true);
}

/* Cast bool -> int1 */
Datum bool_int1(PG_FUNCTION_ARGS)
{
    if (PG_GETARG_BOOL(0) == false)
        PG_RETURN_UINT8(0);
    else
        PG_RETURN_UINT8(1);
}

/* Cast int1 -> interval */
Datum int1_interval(PG_FUNCTION_ARGS)
{
    PG_RETURN_DATUM(DirectFunctionCall1(numeric_interval, DirectFunctionCall1(int1_numeric, PG_GETARG_DATUM(0))));
}

/* Cast int2 -> interval */
Datum int2_interval(PG_FUNCTION_ARGS)
{
    PG_RETURN_DATUM(DirectFunctionCall1(numeric_interval, DirectFunctionCall1(int2_numeric, PG_GETARG_DATUM(0))));
}

/* Cast int4 -> interval */
Datum int4_interval(PG_FUNCTION_ARGS)
{
    PG_RETURN_DATUM(DirectFunctionCall1(numeric_interval, DirectFunctionCall1(int4_numeric, PG_GETARG_DATUM(0))));
}

/* Cast int1 -> interval by typmod */
Datum int1_to_interval(PG_FUNCTION_ARGS)
{
    int32 typmod = PG_GETARG_INT32(1);
    PG_RETURN_DATUM(DirectFunctionCall2(numeric_to_interval,
                                        DirectFunctionCall1(int1_numeric, PG_GETARG_DATUM(0)),
                                        Int32GetDatum(typmod)));
}

/* Cast int2 -> interval by typmod */
Datum int2_to_interval(PG_FUNCTION_ARGS)
{
    int32 typmod = PG_GETARG_INT32(1);
    PG_RETURN_DATUM(DirectFunctionCall2(numeric_to_interval,
                                        DirectFunctionCall1(int2_numeric, PG_GETARG_DATUM(0)),
                                        Int32GetDatum(typmod)));
}

/* Cast int4 -> interval by typmod */
Datum int4_to_interval(PG_FUNCTION_ARGS)
{
    int32 typmod = PG_GETARG_INT32(1);
    PG_RETURN_DATUM(DirectFunctionCall2(numeric_to_interval,
                                        DirectFunctionCall1(int4_numeric, PG_GETARG_DATUM(0)),
                                        Int32GetDatum(typmod)));
}

/* OPERATE INT1 */
Datum int1um(PG_FUNCTION_ARGS)
{
    uint16 result = PG_GETARG_UINT8(0);

    PG_RETURN_INT16(0 - result);
}

Datum int1up(PG_FUNCTION_ARGS)
{
    uint8 arg = PG_GETARG_UINT8(0);

    PG_RETURN_UINT8(arg);
}

Datum int1pl(PG_FUNCTION_ARGS)
{
    uint8 arg1 = PG_GETARG_UINT8(0);
    uint8 arg2 = PG_GETARG_UINT8(1);
    uint16 result;

    result = arg1 + arg2;

    if (result > UCHAR_MAX) {
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("tinyint out of range")));
    }

    PG_RETURN_UINT8((uint8)result);
}

Datum int1mi(PG_FUNCTION_ARGS)
{
    uint8 arg1 = PG_GETARG_UINT8(0);
    uint8 arg2 = PG_GETARG_UINT8(1);
    uint8 result;

    if (arg1 < arg2) {
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("tinyint out of range")));
    }

    result = arg1 - arg2;

    PG_RETURN_UINT8(result);
}

Datum int1mul(PG_FUNCTION_ARGS)
{
    uint8 arg1 = PG_GETARG_UINT8(0);
    uint8 arg2 = PG_GETARG_UINT8(1);
    int16 result16;

    /*
     * The most practical way to detect overflow is to do the arithmetic in
     * int16 (so that the result can't overflow) and then do a range check.
     */
    result16 = (int16)arg1 * (int16)arg2;

    if ((result16 < 0) || (result16 > UCHAR_MAX)) {
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("tinyint out of range")));
    }

    PG_RETURN_UINT8((uint8)result16);
}

Datum int1div(PG_FUNCTION_ARGS)
{
    uint8 arg1 = PG_GETARG_UINT8(0);
    uint8 arg2 = PG_GETARG_UINT8(1);

    float8 result;

    if (arg2 == 0) {
        ereport(ERROR, (errcode(ERRCODE_DIVISION_BY_ZERO), errmsg("division by zero")));

        /* ensure compiler realizes we mustn't reach the division (gcc bug) */
        PG_RETURN_NULL();
    }

    result = (arg1 * 1.0) / (arg2 * 1.0);

    PG_RETURN_FLOAT8(result);
}

Datum int1abs(PG_FUNCTION_ARGS)
{
    uint8 arg1 = PG_GETARG_UINT8(0);

    PG_RETURN_UINT8(arg1);
}

Datum int1mod(PG_FUNCTION_ARGS)
{
    uint8 arg1 = PG_GETARG_UINT8(0);
    uint8 arg2 = PG_GETARG_UINT8(1);

    if (arg2 == 0) {
        if (DB_IS_CMPT(PG_FORMAT)) {
            /* zero is not allowed to be divisor if compatible with PG */
            ereport(ERROR, (errcode(ERRCODE_DIVISION_BY_ZERO), errmsg("division by zero")));

            /* ensure compiler realizes we mustn't reach the division (gcc bug) */
            PG_RETURN_NULL();
        }
        PG_RETURN_UINT8(arg1);
    }

    /* No overflow is possible */

    PG_RETURN_UINT8(arg1 % arg2);
}

Datum int1larger(PG_FUNCTION_ARGS)
{
    uint8 arg1 = PG_GETARG_UINT8(0);
    uint8 arg2 = PG_GETARG_UINT8(1);

    PG_RETURN_UINT8((arg1 > arg2) ? arg1 : arg2);
}

Datum int1smaller(PG_FUNCTION_ARGS)
{
    uint8 arg1 = PG_GETARG_UINT8(0);
    uint8 arg2 = PG_GETARG_UINT8(1);

    PG_RETURN_UINT8((arg1 < arg2) ? arg1 : arg2);
}

Datum int1inc(PG_FUNCTION_ARGS)
{
    uint8 arg = PG_GETARG_UINT8(0);
    int16 result;

    result = arg + 1;

    /* Overflow check */
    if (result > UCHAR_MAX)
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("tinyint out of range")));

    PG_RETURN_UINT8((uint8)result);
}

Datum int1_text(PG_FUNCTION_ARGS)
{
    uint8 arg1 = PG_GETARG_UINT8(0);
    char* tmp = NULL;
    Datum result;
    tmp = DatumGetCString(DirectFunctionCall1(int1out, arg1));

    result = DirectFunctionCall1(textin, CStringGetDatum(tmp));
    pfree_ext(tmp);

    PG_RETURN_DATUM(result);
}

Datum int2_text(PG_FUNCTION_ARGS)
{
    int16 arg1 = PG_GETARG_INT16(0);
    char* tmp = NULL;
    Datum result;
    tmp = DatumGetCString(DirectFunctionCall1(int2out, arg1));

    result = DirectFunctionCall1(textin, CStringGetDatum(tmp));
    pfree_ext(tmp);

    PG_RETURN_DATUM(result);
}

Datum int4_text(PG_FUNCTION_ARGS)
{
    int32 arg1 = PG_GETARG_INT32(0);
    char* tmp = NULL;
    Datum result;

    tmp = DatumGetCString(DirectFunctionCall1(int4out, arg1));
    result = DirectFunctionCall1(textin, CStringGetDatum(tmp));
    pfree_ext(tmp);

    PG_RETURN_DATUM(result);
}

Datum text_int1(PG_FUNCTION_ARGS)
{
    Datum txt = PG_GETARG_DATUM(0);
    char* tmp = NULL;
    Datum result;
    tmp = DatumGetCString(DirectFunctionCall1(textout, txt));

    result = DirectFunctionCall1(int1in, CStringGetDatum(tmp));
    pfree_ext(tmp);

    PG_RETURN_DATUM(result);
}

Datum text_int2(PG_FUNCTION_ARGS)
{
    Datum txt = PG_GETARG_DATUM(0);
    char* tmp = NULL;
    Datum result;
    tmp = DatumGetCString(DirectFunctionCall1(textout, txt));

    result = DirectFunctionCall1(int2in, CStringGetDatum(tmp));
    pfree_ext(tmp);

    PG_RETURN_DATUM(result);
}

Datum text_int4(PG_FUNCTION_ARGS)
{
    Datum txt = PG_GETARG_DATUM(0);
    char* tmp = NULL;
    Datum result;
    tmp = DatumGetCString(DirectFunctionCall1(textout, txt));

    result = DirectFunctionCall1(int4in, CStringGetDatum(tmp));
    pfree_ext(tmp);

    PG_RETURN_DATUM(result);
}
