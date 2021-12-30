/* -------------------------------------------------------------------------
 *
 * numutils.c
 *	  utility functions for I/O of built-in numeric types.
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/numutils.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <math.h>
#include <limits.h>
#include <ctype.h>

#include "common/int.h"
#include "utils/builtins.h"

/*
 * pg_atoi: convert string to integer
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
int32 pg_atoi(char* s, int size, int c)
{
    long l;
    char* badp = NULL;

    /*
     * Some versions of strtol treat the empty string as an error, but some
     * seem not to.  Make an explicit test to be sure we catch it.
     */
    if (s == NULL)
        ereport(ERROR, (errmodule(MOD_FUNCTION), errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("NULL pointer")));

    if ((*s == 0) && DB_IS_CMPT(A_FORMAT | PG_FORMAT))
        ereport(ERROR,
            (errmodule(MOD_FUNCTION),
                errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                errmsg("invalid input syntax for integer: \"%s\"", s)));

    /* In B compatibility, empty str is treated as 0 */
    if ((*s == 0) && (u_sess->attr.attr_sql.sql_compatibility == B_FORMAT)) {
        long l = 0;
        return (int32)l;
    }

    errno = 0;
    l = strtol(s, &badp, 10);

    /* We made no progress parsing the string, so bail out */
    if (s == badp) {
        if (DB_IS_CMPT(A_FORMAT | PG_FORMAT))
            ereport(ERROR,
                (errmodule(MOD_FUNCTION),
                    errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                    errmsg("invalid input syntax for integer: \"%s\"", s)));
        /* string is treated as 0 in B compatibility */
        if (u_sess->attr.attr_sql.sql_compatibility == B_FORMAT) {
            long l = 0;
            return (int32)l;
        }
    }

    switch (size) {
        case sizeof(int32):
            if (errno == ERANGE
#if defined(HAVE_LONG_INT_64)
                /* won't get ERANGE on these with 64-bit longs... */
                || l < INT_MIN || l > INT_MAX
#endif
            )
                ereport(ERROR,
                    (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                        errmsg("value \"%s\" is out of range for type integer", s)));
            break;
        case sizeof(int16):
            if (errno == ERANGE || l < SHRT_MIN || l > SHRT_MAX)
                ereport(ERROR,
                    (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                        errmsg("value \"%s\" is out of range for type smallint", s)));
            break;
        case sizeof(uint8):
            if (errno == ERANGE || l < 0 || l > UCHAR_MAX)
                ereport(ERROR,
                    (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                        errmsg("value \"%s\" is out of range for 8-bit integer", s)));
            break;
        default:
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("unsupported result size: %d", size)));
    }

    /*
     * Skip any trailing whitespace; if anything but whitespace remains before
     * the terminating character, bail out
     */
    while (*badp && *badp != c && isspace((unsigned char)*badp)) {
        badp++;
    }

    if (*badp && *badp != c && u_sess->attr.attr_sql.sql_compatibility != B_FORMAT)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION), errmsg("invalid input syntax for integer: \"%s\"", s)));

    return (int32)l;
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
int16 pg_strtoint16(const char* s)
{
    const char* ptr = s;
    int16 tmp = 0;
    bool neg = false;

    /* skip leading spaces */
    while (likely(*ptr) && isspace((unsigned char)*ptr)) {
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
        if (DB_IS_CMPT(A_FORMAT | PG_FORMAT))
            goto invalid_syntax;
        if (DB_IS_CMPT(B_FORMAT))
            return tmp;
    }

    /* process digits */
    while (*ptr && isdigit((unsigned char)*ptr)) {
        int8 digit = (*ptr++ - '0');

        if (unlikely(pg_mul_s16_overflow(tmp, 10, &tmp)) || unlikely(pg_sub_s16_overflow(tmp, digit, &tmp)))
            goto out_of_range;
    }

    /* allow trailing whitespace, but not other trailing chars */
    while (*ptr != '\0' && isspace((unsigned char)*ptr)) {
        ptr++;
    }

    if (unlikely(*ptr != '\0') && u_sess->attr.attr_sql.sql_compatibility != B_FORMAT)
        goto invalid_syntax;

    if (!neg) {
        /* could fail if input is most negative number */
        if (unlikely(tmp == PG_INT16_MIN))
            goto out_of_range;
        tmp = -tmp;
    }

    return tmp;

out_of_range:
    ereport(ERROR,
        (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
            errmsg("value \"%s\" is out of range for type %s", s, "smallint")));

invalid_syntax:
    ereport(ERROR,
        (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION), errmsg("invalid input syntax for %s: \"%s\"", "integer", s)));
    return 0;
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
int32 pg_strtoint32(const char* s)
{
    const char* ptr = s;
    int32 tmp = 0;
    bool neg = false;

    /* skip leading spaces */
    while (likely(*ptr) && isspace((unsigned char)*ptr)) {
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
        if (DB_IS_CMPT(A_FORMAT | PG_FORMAT))
            goto invalid_syntax;
        else if (DB_IS_CMPT(B_FORMAT))
            return tmp;
    }

    /* process digits */
    while (*ptr && isdigit((unsigned char)*ptr)) {
        int8 digit = (*ptr++ - '0');

        if (unlikely(pg_mul_s32_overflow(tmp, 10, &tmp)) || unlikely(pg_sub_s32_overflow(tmp, digit, &tmp)))
            goto out_of_range;
    }

    /* allow trailing whitespace, but not other trailing chars */
    while (*ptr != '\0' && isspace((unsigned char)*ptr)) {
        ptr++;
    }

    if (unlikely(*ptr != '\0') && u_sess->attr.attr_sql.sql_compatibility != B_FORMAT)
        goto invalid_syntax;

    if (!neg) {
        /* could fail if input is most negative number */
        if (unlikely(tmp == PG_INT32_MIN))
            goto out_of_range;
        tmp = -tmp;
    }

    return tmp;

out_of_range:
    ereport(ERROR,
        (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
            errmsg("value \"%s\" is out of range for type %s", s, "integer")));

invalid_syntax:
    ereport(ERROR,
        (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION), errmsg("invalid input syntax for %s: \"%s\"", "integer", s)));
    return 0;
}

// pg_ctoa: converts a unsigned 8-bit integer to its string representation
//
// Caller must ensure that 'a' points to enough memory to hold the result
// (at least 5 bytes, counting a leading sign and trailing NUL).
//
// It doesn't seem worth implementing this separately.
void pg_ctoa(uint8 i, char* a)
{
    pg_ltoa((int32)i, a);
}

/*
 * pg_itoa: converts a signed 16-bit integer to its string representation
 *
 * Caller must ensure that 'a' points to enough memory to hold the result
 * (at least 7 bytes, counting a leading sign and trailing NUL).
 *
 * It doesn't seem worth implementing this separately.
 */
void pg_itoa(int16 i, char* a)
{
    pg_ltoa((int32)i, a);
}

/*
 * pg_ltoa: converts a signed 32-bit integer to its string representation
 *
 * Caller must ensure that 'a' points to enough memory to hold the result
 * (at least 12 bytes, counting a leading sign and trailing NUL).
 */
void pg_ltoa(int32 value, char* a)
{
    char* start = a;
    bool neg = false;
    errno_t ss_rc;

    if (a == NULL)
        return;

    /*
     * Avoid problems with the most negative integer not being representable
     * as a positive integer.
     */
    if (value == (-2147483647 - 1)) {
        const int a_len = 12;
        ss_rc = memcpy_s(a, a_len, "-2147483648", a_len);
        securec_check(ss_rc, "\0", "\0");
        return;
    } else if (value < 0) {
        value = -value;
        neg = true;
    }

    /* Compute the result string backwards. */
    do {
        int32 remainder;
        int32 oldval = value;

        value /= 10;
        remainder = oldval - value * 10;
        *a++ = '0' + remainder;
    } while (value != 0);

    if (neg)
        *a++ = '-';

    /* Add trailing NUL byte, and back up 'a' to the last character. */
    *a-- = '\0';

    /* Reverse string. */
    while (start < a) {
        char swap = *start;

        *start++ = *a;
        *a-- = swap;
    }
}

/*
 * pg_lltoa: convert a signed 64-bit integer to its string representation
 *
 * Caller must ensure that 'a' points to enough memory to hold the result
 * (at least MAXINT8LEN+1 bytes, counting a leading sign and trailing NUL).
 */
void pg_lltoa(int64 value, char* a)
{
    char* start = a;
    bool neg = false;

    if (a == NULL) {
        return;
    }

    /*
     * Avoid problems with the most negative integer not being representable
     * as a positive integer.
     */
    if (value == (-INT64CONST(0x7FFFFFFFFFFFFFFF) - 1)) {
        const int a_len = 21;
        errno_t ss_rc = memcpy_s(a, a_len, "-9223372036854775808", a_len);
        securec_check(ss_rc, "\0", "\0");
        return;
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
}

/*
 * pg_lltoa: convert a signed 128-bit integer to its string representation
 *
 * Caller must ensure that 'a' points to enough memory to hold the result
 * (at least MAXINT16LEN+1 bytes, counting a leading sign and trailing NUL).
 */
void pg_i128toa(int128 value, char* a, int length)
{
    char* start = a;
    bool neg = false;

    if (a == NULL)
        return;

    /*
     * Avoid problems with the most negative integer not being representable
     * as a positive integer.
     */
    if (value == (PG_INT128_MIN)) {
        const int int128minStrLength = 41;
        const char* int128minStr = "-170141183460469231731687303715884105728";
        errno_t ss_rc = memcpy_s(a, length, int128minStr, int128minStrLength);
        securec_check(ss_rc, "\0", "\0");
        return;
    } else if (value < 0) {
        value = -value;
        neg = true;
    }

    /* Compute the result string backwards. */
    do {
        int128 remainder;
        int128 oldval = value;

        value /= 10;
        remainder = oldval - value * 10;
        *a++ = '0' + remainder;
    } while (value != 0);

    if (neg)
        *a++ = '-';

    /* Add trailing NUL byte, and back up 'a' to the last character. */
    *a-- = '\0';

    /* Reverse string. */
    while (start < a) {
        char swap = *start;

        *start++ = *a;
        *a-- = swap;
    }
}

/*
 * pg_strtouint64
 *		Converts 'str' into an unsigned 64-bit integer.
 *
 * This has the identical API to strtoul(3), except that it will handle
 * 64-bit ints even where "long" is narrower than that.
 *
 * For the moment it seems sufficient to assume that the platform has
 * such a function somewhere; let's not roll our own.
 */
uint64 pg_strtouint64(const char* str, char** endptr, int base)
{
#ifdef _MSC_VER /* MSVC only */
    return _strtoui64(str, endptr, base);
#elif defined(HAVE_STRTOULL) && SIZEOF_LONG < 8
    return strtoull(str, endptr, base);
#else
    return strtoul(str, endptr, base);
#endif
}
