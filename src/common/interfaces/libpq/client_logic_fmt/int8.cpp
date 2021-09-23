/* -------------------------------------------------------------------------
 *
 * int8.c
 * Internal 64-bit integer operations
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 * src/backend/utils/adt/int8.c
 *
 * -------------------------------------------------------------------------
 */
#define ereport(a, b)
#include "postgres_fe.h"
#include "libpq-int.h"
#include "nodes/pg_list.h"
#include <string>
#include <ctype.h>
#include <limits.h>
#include <math.h>

#include "common/int.h"
#include "utils/builtins.h"
#include "client_logic_cache/icached_column_manager.h"
#include "libpq-fe.h"

/* **********************************************************************
 * *
 * *        Routines for 64-bit integers.
 * *
 * ********************************************************************* */

/* ----------------------------------------------------------
 * Formatting and conversion routines.
 * --------------------------------------------------------- */

/*
 * scanint8 --- try to parse a string into an int8.
 *
 * If errorOK is false, ereport a useful error message if the string is bad.
 * If errorOK is true, just return "false" for bad input.
 */
bool scanint8(const PGconn* conn, const char *str, bool errorOK, int64 *result, char *err_msg)
{
    const char *ptr = str;
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
    } else if (*ptr == '+') {
        ptr++;
    }

    /* require at least one digit */
    if (unlikely(!isdigit((unsigned char)*ptr))) {
        RETURN_IF(errorOK, false);
        if (conn->client_logic->m_cached_column_manager->get_sql_compatibility() == ORA_FORMAT) {
            check_sprintf_s(
                sprintf_s(err_msg, MAX_ERRMSG_LENGTH, "invalid input syntax for type bigint: \"%s\"", str));
            return false;
        }
    }

    /* process digits */
    while (*ptr && isdigit((unsigned char)*ptr)) {
        int8 digit = (*ptr++ - '0');
        const int decimal_base = 10;
        if (unlikely(pg_mul_s64_overflow(tmp, decimal_base, &tmp)) || unlikely(pg_sub_s64_overflow(tmp, digit, &tmp))) {
            RETURN_IF(errorOK, false);
            check_sprintf_s(
                sprintf_s(err_msg, MAX_ERRMSG_LENGTH, "value %s is out of range for type bigint", str));
            return false;
        }
    }

    /* allow trailing whitespace, but not other trailing chars */
    while (*ptr != '\0' && isspace((unsigned char)*ptr)) {
        ptr++;
    }

    if (unlikely(*ptr != '\0')) {
        RETURN_IF(errorOK, false);
        /*
         * Empty string will be treated as NULL if sql_compatibility == ORA_FORMAT,
         * Other wise whitespace will be convert to 0
         */
        check_sprintf_s(
            sprintf_s(err_msg, MAX_ERRMSG_LENGTH, "invalid input syntax for type bigint: \"%s\"", str));
        return false;
    }

    if (!neg) {
        /* could fail if input is most negative number */
        if (unlikely(tmp == PG_INT64_MIN)) {
            RETURN_IF(errorOK, false);
            check_sprintf_s(
                sprintf_s(err_msg, MAX_ERRMSG_LENGTH, "value %s is out of range for type bigint", str));
            return false;
        }
        tmp = -tmp;
    }

    *result = tmp;
    return true;
}
