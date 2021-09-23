/* -------------------------------------------------------------------------
 *
 * numutils.c
 * utility functions for I/O of built-in numeric types.
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 * src/backend/utils/adt/numutils.c
 *
 * -------------------------------------------------------------------------
 */

#define ereport(a, b) return false;
#include "postgres_fe.h"
#include "nodes/pg_list.h"

#include <math.h>
#include <limits.h>
#include <ctype.h>

#include "common/int.h"
#include "utils/builtins.h"

#include "client_logic_fmt/numutils.h"
#include "client_logic_cache/icached_column_manager.h"
#include "libpq-int.h"

/*
 * fe_pg_atoi8: convert string to integer
 *
 * allows any number of leading or trailing whitespace characters.
 *
 * 'size' is the sizeof() the desired integral result (1, 2, or 4 bytes).
 *
 * c, if not 0, is a terminator character that may appear after the
 * integer (plus whitespace).  If 0, the string must end after the integer.
 *
 * Unlike plain atoi(), this will throw ereport() upon bad input format or
 * overflow.
 */
bool fe_pg_atoi8(const PGconn* conn, const char *s, int8 *res, char *err_msg)
{
    long l;
    char *badp = NULL;
    const int decimal_base = 10;

    /*
     * Some versions of strtol treat the empty string as an error, but some
     * seem not to.  Make an explicit test to be sure we catch it.
     */
    if (s == NULL) {
        check_sprintf_s(sprintf_s(err_msg, MAX_ERRMSG_LENGTH, " NULL pointer"));
        return false;
    }

    if ((*s == 0) && conn->client_logic->m_cached_column_manager->get_sql_compatibility() == ORA_FORMAT) {
        check_sprintf_s(sprintf_s(err_msg, MAX_ERRMSG_LENGTH, " invalid input syntax for integer: %s\n", s));
        return false;
    }

    errno = 0;
    l = strtol(s, &badp, decimal_base);

    /* We made no progress parsing the string, so bail out */
    if (s == badp) {
        if (conn->client_logic->m_cached_column_manager->get_sql_compatibility() == ORA_FORMAT) {
            check_sprintf_s(sprintf_s(err_msg, MAX_ERRMSG_LENGTH, " invalid input syntax for integer: %s\n", s));
            return false;
        }
    }

    if (errno == ERANGE || l < 0 || l > UCHAR_MAX) {
        check_sprintf_s(
            sprintf_s(err_msg, MAX_ERRMSG_LENGTH, " value %s is out of range for 8-bit integer\n", s));
        return false;
    }

    /*
     * Skip any trailing whitespace; if anything but whitespace remains before
     * the terminating character, bail out
     */
    while (*badp && *badp != '\0' && isspace((unsigned char)*badp)) {
        badp++;
    }

    if (*badp && *badp != '\0') {
        check_sprintf_s(sprintf_s(err_msg, MAX_ERRMSG_LENGTH, " invalid input syntax for integer: %s\n", s));
        return false;
    }

    *res = (int8)l;
    return true;
}

/*
 * Convert input string to a signed 16 bit integer.
 *
 * Allows any number of leading or trailing whitespace characters. Will throw
 * ereport() upon bad input format or overflow.
 *
 * NB: Accumulate input as a negative number, to deal with two's complement
 * representation of the most negative number, which can't be represented as a
 * positive number.
 */
bool fe_pg_strtoint16(const PGconn* conn, const char *s, int16 *res, char *err_msg)
{
    const char *ptr = s;
    int16 tmp = 0;
    bool neg = false;
    const int decimal_base = 10;

    /* skip leading spaces */
    while (likely(*ptr) && isspace((unsigned char)*ptr)) {
        ptr++;
    }

    /* handle sign */
    if (*ptr == '-') {
        ptr++;
        neg = true;
    } else if (*ptr == '+') {
        ptr++;
    }
    /* skip  spaces after sign */
    while (likely(*ptr) && isspace((unsigned char)*ptr)) {
        ptr++;
    }

    /* require at least one digit */
    if (unlikely(!isdigit((unsigned char)*ptr))) {
        if (conn->client_logic->m_cached_column_manager->get_sql_compatibility() == ORA_FORMAT) {
            goto invalid_syntax;
        }
    }

    /* process digits */
    while (*ptr && isdigit((unsigned char)*ptr)) {
        int8 digit = (*ptr++ - '0');

        if (unlikely(pg_mul_s16_overflow(tmp, decimal_base, &tmp)) || unlikely(pg_sub_s16_overflow(tmp, digit, &tmp))) {
            goto out_of_range;
        }
    }

    /* allow trailing whitespace, but not other trailing chars */
    while (*ptr != '\0' && isspace((unsigned char)*ptr)) {
        ptr++;
    }

    if (unlikely(*ptr != '\0')) {
        goto invalid_syntax;
    }

    if (!neg) {
        /* could fail if input is most negative number */
        if (unlikely(tmp == PG_INT16_MIN))
            goto out_of_range;
        tmp = -tmp;
    }

    *res = tmp;
    return true;

out_of_range : {
    check_sprintf_s(sprintf_s(err_msg, MAX_ERRMSG_LENGTH, " value %s is out of range for type smallint\n", s));
    return false;
}

invalid_syntax : {
    check_sprintf_s(sprintf_s(err_msg, MAX_ERRMSG_LENGTH, " invalid input syntax for integer %s\n", s));
    return false;
}
}

/*
 * Convert input string to a signed 32 bit integer.
 *
 * Allows any number of leading or trailing whitespace characters. Will throw
 * ereport() upon bad input format or overflow.
 *
 * NB: Accumulate input as a negative number, to deal with two's complement
 * representation of the most negative number, which can't be represented as a
 * positive number.
 */
bool fe_pg_strtoint32(const PGconn* conn, const char *s, int32 *res, char *err_msg)
{
    const char *ptr = s;
    int32 tmp = 0;
    bool neg = false;
    const int decimal_base = 10;

    /* skip leading spaces */
    while (likely(*ptr) && isspace((unsigned char)*ptr)) {
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
        if (conn->client_logic->m_cached_column_manager->get_sql_compatibility() == ORA_FORMAT) {
            goto invalid_syntax;
        }
    }

    /* process digits */
    while (*ptr && isdigit((unsigned char)*ptr)) {
        int8 digit = (*ptr++ - '0');

        if (unlikely(pg_mul_s32_overflow(tmp, decimal_base, &tmp)) || unlikely(pg_sub_s32_overflow(tmp, digit, &tmp))) {
            goto out_of_range;
        }
    }

    /* allow trailing whitespace, but not other trailing chars */
    while (*ptr != '\0' && isspace((unsigned char)*ptr)) {
        ptr++;
    }

    if (unlikely(*ptr != '\0')) {
        goto invalid_syntax;
    }

    if (!neg) {
        /* could fail if input is most negative number */
        if (unlikely(tmp == PG_INT32_MIN)) {
            goto out_of_range;
        }
        tmp = -tmp;
    }

    *res = tmp;
    return true;

out_of_range : {
    check_sprintf_s(sprintf_s(err_msg, MAX_ERRMSG_LENGTH, " value %s is out of range for type integer\n", s));
    return false;
}

invalid_syntax : {
    check_sprintf_s(sprintf_s(err_msg, MAX_ERRMSG_LENGTH, " invalid input syntax for integer %s integer\n", s));
    return false;
}
}

/*
 * fe_pg_ctoa: converts a unsigned 8-bit integer to its string representation
 *
 * Caller must ensure that 'a' points to enough memory to hold the result
 * (at least 5 bytes, counting a leading sign and trailing NUL).
 *
 * It doesn't seem worth implementing this separately.
 */
bool fe_pg_ctoa(uint8 i, char *a)
{
    return fe_pg_ltoa((int32)i, a);
}

/*
 * fe_pg_itoa: converts a signed 16-bit integer to its string representation
 *
 * Caller must ensure that 'a' points to enough memory to hold the result
 * (at least 7 bytes, counting a leading sign and trailing NUL).
 *
 * It doesn't seem worth implementing this separately.
 */
bool fe_pg_itoa(int16 i, char *a)
{
    return fe_pg_ltoa((int32)i, a);
}

/*
 * fe_pg_ltoa: converts a signed 32-bit integer to its string representation
 *
 * Caller must ensure that 'a' points to enough memory to hold the result
 * (at least 12 bytes, counting a leading sign and trailing NUL).
 */
bool fe_pg_ltoa(int32 value, char *a)
{
    char *start = a;
    bool neg = false;
    errno_t ss_rc = EOK;

    if (a == NULL) {
        return false;
    }

    /*
     * Avoid problems with the most negative integer not being representable
     * as a positive integer.
     */
    if (value == (-2147483647 - 1)) { /* -2147483647 is most negative integer is -2^32 - 1 */
        const int a_len = 12;
        ss_rc = memcpy_s(a, a_len, "-2147483648", a_len);
        securec_check_c(ss_rc, "\0", "\0");
        return true;
    } else if (value < 0) {
        value = -value;
        neg = true;
    }

    /* Compute the result string backwards. */
    do {
        int32 remainder;
        int32 oldval = value;

        const int decimal_base = 10;
        value /= decimal_base;
        remainder = oldval - value * decimal_base;
        *a++ = '0' + remainder;
    } while (value != 0);

    if (neg) {
        *a++ = '-';
    }

    /* Add trailing NUL byte, and back up 'a' to the last character. */
    *a-- = '\0';

    /* Reverse string. */
    while (start < a) {
        char swap = *start;

        *start++ = *a;
        *a-- = swap;
    }
    return true;
}

/*
 * fe_pg_lltoa: convert a signed 64-bit integer to its string representation
 *
 * Caller must ensure that 'a' points to enough memory to hold the result
 * (at least MAXINT8LEN+1 bytes, counting a leading sign and trailing NUL).
 */
bool fe_pg_lltoa(int64 value, char *a)
{
    char *start = a;
    bool neg = false;

    if (a == NULL) {
        return false;
    }

    /*
     * Avoid problems with the most negative integer not being representable
     * as a positive integer.
     */
    if (value == (-INT64CONST(0x7FFFFFFFFFFFFFFF) - 1)) {
        const int a_len = 21;
        errno_t ss_rc = EOK;
        ss_rc = memcpy_s(a, a_len, "-9223372036854775808", a_len);
        securec_check_c(ss_rc, "\0", "\0");
        return true;
    } else if (value < 0) {
        value = -value;
        neg = true;
    }

    /* Compute the result string backwards. */
    do {
        int64 remainder;
        int64 oldval = value;

        value /= 10;
        remainder = oldval - value * 10;
        *a++ = '0' + remainder;
    } while (value != 0);

    if (neg) {
        *a++ = '-';
    }

    /* Add trailing NUL byte, and back up 'a' to the last character. */
    *a-- = '\0';

    /* Reverse string. */
    while (start < a) {
        char swap = *start;

        *start++ = *a;
        *a-- = swap;
    }
    return true;
}
