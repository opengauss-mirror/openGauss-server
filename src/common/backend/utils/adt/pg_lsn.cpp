/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-server is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

/* -----------------------------------------------------------------------
 *
 * openGauss locale utilities
 *
 * Portions Copyright (c) 2002-2012, PostgreSQL Global Development Group
 *
 * src/backend/utils/adt/pg_lsn.c
 *
 * -----------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/hash.h"
#include "funcapi.h"
#include "libpq/pqformat.h"
#include "utils/builtins.h"
#include "utils/numeric.h"
#include "utils/pg_lsn.h"

extern Datum numeric_pg_lsn(PG_FUNCTION_ARGS);

#define MAXPG_LSNLEN            17
#define MAXPG_LSNCOMPONENT        8

/*----------------------------------------------------------
 * Formatting and conversion routines.
 *---------------------------------------------------------*/

#ifdef ENABLE_NEON
/* NEON version: pg_lsn_in with internal helper function */
XLogRecPtr pg_lsn_in_internal(const char *str, bool *have_error)
{
    int len1;
    int len2;
    uint32 id;
    uint32 off;
    XLogRecPtr result;

    Assert(have_error != NULL);
    *have_error = false;

    /* Sanity check input format. */
    len1 = strspn(str, "0123456789abcdefABCDEF");
    if (len1 < 1 || len1 > MAXPG_LSNCOMPONENT || str[len1] != '/') {
        *have_error = true;
        return InvalidXLogRecPtr;
    }
    len2 = strspn(str + len1 + 1, "0123456789abcdefABCDEF");
    if (len2 < 1 || len2 > MAXPG_LSNCOMPONENT || str[len1 + 1 + len2] != '\0') {
        *have_error = true;
        return InvalidXLogRecPtr;
    }

    /* Decode result. */
    id = (uint32) strtoul(str, NULL, 16);
    off = (uint32) strtoul(str + len1 + 1, NULL, 16);
    result = ((uint64) id << 32) | off;

    return result;
}

Datum pg_lsn_in(PG_FUNCTION_ARGS)
{
    char       *str = PG_GETARG_CSTRING(0);
    XLogRecPtr    result;
    bool        have_error = false;

    result = pg_lsn_in_internal(str, &have_error);
    if (have_error)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                 errmsg("invalid input syntax for type %s: \"%s\"",
                        "pg_lsn", str)));

    PG_RETURN_LSN(result);
}

#else
/* Non-NEON version: simple pg_lsn_in implementation */
Datum pg_lsn_in(PG_FUNCTION_ARGS)
{
    char* str = PG_GETARG_CSTRING(0);
    Size len1, len2;
    uint32 id, off;
    XLogRecPtr result;

    /* Sanity check input format. */
    len1 = strspn(str, "0123456789abcdefABCDEF");
    if (len1 < 1 || len1 > MAXPG_LSNCOMPONENT || str[len1] != '/')
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                errmsg("invalid input syntax for type %s: \"%s\"", "pg_lsn", str)));
    len2 = strspn(str + len1 + 1, "0123456789abcdefABCDEF");
    if (len2 < 1 || len2 > MAXPG_LSNCOMPONENT || str[len1 + 1 + len2] != '\0')
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                errmsg("invalid input syntax for type %s: \"%s\"", "pg_lsn", str)));

    /* Decode result. */
    id = (uint32)strtoul(str, NULL, 16);
    off = (uint32)strtoul(str + len1 + 1, NULL, 16);
    result = ((uint64)id << 32) | off;

    PG_RETURN_LSN(result);
}
#endif /* ENABLE_NEON */

/* Common functions for both NEON and non-NEON versions */
Datum pg_lsn_out(PG_FUNCTION_ARGS)
{
    XLogRecPtr    lsn = PG_GETARG_LSN(0);
    char        buf[MAXPG_LSNLEN + 1];
    char       *result;

    errno_t rc = snprintf_s(buf, sizeof(buf), sizeof(buf) - 1, "%X/%X", (uint32)((lsn) >> 32), (uint32)(lsn));
    securec_check_ss(rc, "", "");
    result = pstrdup(buf);
    PG_RETURN_CSTRING(result);
}

Datum pg_lsn_recv(PG_FUNCTION_ARGS)
{
    StringInfo    buf = (StringInfo) PG_GETARG_POINTER(0);
    XLogRecPtr    result;

    result = pq_getmsgint64(buf);
    PG_RETURN_LSN(result);
}

Datum pg_lsn_send(PG_FUNCTION_ARGS)
{
    XLogRecPtr    lsn = PG_GETARG_LSN(0);
    StringInfoData buf;

    pq_begintypsend(&buf);
    pq_sendint64(&buf, lsn);
    PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

/*----------------------------------------------------------
 *    Operators for PostgreSQL LSNs
 *---------------------------------------------------------*/

Datum pg_lsn_eq(PG_FUNCTION_ARGS)
{
    XLogRecPtr    lsn1 = PG_GETARG_LSN(0);
    XLogRecPtr    lsn2 = PG_GETARG_LSN(1);

    PG_RETURN_BOOL(lsn1 == lsn2);
}

Datum pg_lsn_ne(PG_FUNCTION_ARGS)
{
    XLogRecPtr    lsn1 = PG_GETARG_LSN(0);
    XLogRecPtr    lsn2 = PG_GETARG_LSN(1);

    PG_RETURN_BOOL(lsn1 != lsn2);
}

Datum pg_lsn_lt(PG_FUNCTION_ARGS)
{
    XLogRecPtr    lsn1 = PG_GETARG_LSN(0);
    XLogRecPtr    lsn2 = PG_GETARG_LSN(1);

    PG_RETURN_BOOL(lsn1 < lsn2);
}

Datum pg_lsn_gt(PG_FUNCTION_ARGS)
{
    XLogRecPtr    lsn1 = PG_GETARG_LSN(0);
    XLogRecPtr    lsn2 = PG_GETARG_LSN(1);

    PG_RETURN_BOOL(lsn1 > lsn2);
}

Datum pg_lsn_le(PG_FUNCTION_ARGS)
{
    XLogRecPtr    lsn1 = PG_GETARG_LSN(0);
    XLogRecPtr    lsn2 = PG_GETARG_LSN(1);

    PG_RETURN_BOOL(lsn1 <= lsn2);
}

Datum pg_lsn_ge(PG_FUNCTION_ARGS)
{
    XLogRecPtr    lsn1 = PG_GETARG_LSN(0);
    XLogRecPtr    lsn2 = PG_GETARG_LSN(1);

    PG_RETURN_BOOL(lsn1 >= lsn2);
}

Datum pg_lsn_larger(PG_FUNCTION_ARGS)
{
    XLogRecPtr    lsn1 = PG_GETARG_LSN(0);
    XLogRecPtr    lsn2 = PG_GETARG_LSN(1);

    PG_RETURN_LSN((lsn1 > lsn2) ? lsn1 : lsn2);
}

Datum pg_lsn_smaller(PG_FUNCTION_ARGS)
{
    XLogRecPtr    lsn1 = PG_GETARG_LSN(0);
    XLogRecPtr    lsn2 = PG_GETARG_LSN(1);

    PG_RETURN_LSN((lsn1 < lsn2) ? lsn1 : lsn2);
}

/* btree index opclass support */
Datum pg_lsn_cmp(PG_FUNCTION_ARGS)
{
    XLogRecPtr    a = PG_GETARG_LSN(0);
    XLogRecPtr    b = PG_GETARG_LSN(1);
    if (a > b) {
        PG_RETURN_INT32(1);
    } else if (a == b) {
        PG_RETURN_INT32(0);
    } else {
        PG_RETURN_INT32(-1);
    }
}

/* hash index opclass support */
Datum pg_lsn_hash(PG_FUNCTION_ARGS)
{
    /* We can use hashint8 directly */
    return hashint8(fcinfo);
}

Datum pg_lsn_hash_extended(PG_FUNCTION_ARGS)
{
    /* For now, fall back to regular hash if extended hash is not available */
    /* This is a simplified implementation - in PG14 this uses hashint8extended */
    return hashint8(fcinfo);
}


/*----------------------------------------------------------
 *    Arithmetic operators on PostgreSQL LSNs.
 *---------------------------------------------------------*/

Datum pg_lsn_mi(PG_FUNCTION_ARGS)
{
    XLogRecPtr    lsn1 = PG_GETARG_LSN(0);
    XLogRecPtr    lsn2 = PG_GETARG_LSN(1);
    char        buf[256];
    Datum        result;

    /* Output could be as large as plus or minus 2^63 - 1. */
    if (lsn1 < lsn2) {
        errno_t rc = snprintf_s(buf, sizeof(buf), sizeof(buf) - 1, "-" UINT64_FORMAT, lsn2 - lsn1);
        securec_check_ss(rc, "", "");
    } else {
        errno_t rc = snprintf_s(buf, sizeof(buf), sizeof(buf) - 1, UINT64_FORMAT, lsn1 - lsn2);
        securec_check_ss(rc, "", "");
    }

    /* Convert to numeric. */
    result = DirectFunctionCall3(numeric_in,
                                 CStringGetDatum(buf),
                                 ObjectIdGetDatum(0),
                                 Int32GetDatum(-1));

    return result;
}

/*
 * Add the number of bytes to pg_lsn, giving a new pg_lsn.
 * Must handle both positive and negative numbers of bytes.
 */
Datum pg_lsn_pli(PG_FUNCTION_ARGS)
{
    XLogRecPtr    lsn = PG_GETARG_LSN(0);
    Numeric        nbytes = PG_GETARG_NUMERIC(1);
    Datum        num;
    Datum        res;
    char        buf[32];

    if (numeric_is_nan(nbytes))
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("cannot add NaN to pg_lsn")));

    /* Convert to numeric */
    errno_t rc = snprintf_s(buf, sizeof(buf), sizeof(buf) - 1, UINT64_FORMAT, lsn);
    securec_check_ss(rc, "", "");
    num = DirectFunctionCall3(numeric_in,
                              CStringGetDatum(buf),
                              ObjectIdGetDatum(0),
                              Int32GetDatum(-1));

    /* Add two numerics */
    res = DirectFunctionCall2(numeric_add,
                              NumericGetDatum(num),
                              NumericGetDatum(nbytes));

    /* Convert to pg_lsn */
    return DirectFunctionCall1(numeric_pg_lsn, res);
}

/*
 * Subtract the number of bytes from pg_lsn, giving a new pg_lsn.
 * Must handle both positive and negative numbers of bytes.
 */
Datum pg_lsn_mii(PG_FUNCTION_ARGS)
{
    XLogRecPtr    lsn = PG_GETARG_LSN(0);
    Numeric        nbytes = PG_GETARG_NUMERIC(1);
    Datum        num;
    Datum        res;
    char        buf[32];

    if (numeric_is_nan(nbytes))
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("cannot subtract NaN from pg_lsn")));

    /* Convert to numeric */
    errno_t rc = snprintf_s(buf, sizeof(buf), sizeof(buf) - 1, UINT64_FORMAT, lsn);
    securec_check_ss(rc, "", "");
    num = DirectFunctionCall3(numeric_in,
                              CStringGetDatum(buf),
                              ObjectIdGetDatum(0),
                              Int32GetDatum(-1));

    /* Subtract two numerics */
    res = DirectFunctionCall2(numeric_sub,
                              NumericGetDatum(num),
                              NumericGetDatum(nbytes));

    /* Convert to pg_lsn */
    return DirectFunctionCall1(numeric_pg_lsn, res);
}
