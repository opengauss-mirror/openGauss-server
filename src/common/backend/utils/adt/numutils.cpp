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
#include "utils/numutils.h"

#include "postgres.h"
#include "knl/knl_variable.h"
#include "port/pg_bitutils.h"

#include <math.h>
#include <limits>
#include <cctype>

#include "common/int.h"
#include "utils/builtins.h"

static inline int
decimalLength32(const uint32 v)
{
    int t;
    static const uint32 PowersOfTen[] = {
        1, 10, 100,
        1000, 10000, 100000,
        1000000, 10000000, 100000000,
        1000000000
    };

    /*
     * Compute base-10 logarithm by dividing the base-2 logarithm by a
     * good-enough approximation of the base-2 logarithm of 10
     */
    t = (pg_leftmost_one_pos32(v) + 1) * 1233 / 4096;
    return t + (v >= PowersOfTen[t]);
}

static inline int
decimalLength64(const uint64 v)
{
	int			t;
	static const uint64 PowersOfTen[] = {
		UINT64CONST(1), UINT64CONST(10),
		UINT64CONST(100), UINT64CONST(1000),
		UINT64CONST(10000), UINT64CONST(100000),
		UINT64CONST(1000000), UINT64CONST(10000000),
		UINT64CONST(100000000), UINT64CONST(1000000000),
		UINT64CONST(10000000000), UINT64CONST(100000000000),
		UINT64CONST(1000000000000), UINT64CONST(10000000000000),
		UINT64CONST(100000000000000), UINT64CONST(1000000000000000),
		UINT64CONST(10000000000000000), UINT64CONST(100000000000000000),
		UINT64CONST(1000000000000000000), UINT64CONST(10000000000000000000)
	};

	/*
	 * Compute base-10 logarithm by dividing the base-2 logarithm by a
	 * good-enough approximation of the base-2 logarithm of 10
	 */
	t = (pg_leftmost_one_pos64(v) + 1) * 1233 / 4096;
	return t + (v >= PowersOfTen[t]);
}

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
 * can_ignore, if is true, means the input s will be truncated when its value
 * is invalid for integer.
 *
 * Unlike plain atoi(), this will throw ereport() upon bad input format or
 * overflow.
 */
int32 pg_atoi(char* s, int size, int c, bool can_ignore)
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
            ) {
                if (!can_ignore) {
                    ereport(ERROR,
                            (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                             errmsg("value \"%s\" is out of range for type integer", s)));
                }
                ereport(WARNING,
                        (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                         errmsg("value \"%s\" is out of range for type integer. truncated automatically", s)));
                l = l < INT_MIN ? INT_MIN : INT_MAX;
            }
            break;
        case sizeof(int16):
            if (errno == ERANGE || l < SHRT_MIN || l > SHRT_MAX) {
                if (!can_ignore) {
                    ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                                    errmsg("value \"%s\" is out of range for type smallint", s)));
                }
                ereport(WARNING, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                                  errmsg("value \"%s\" is out of range for type smallint. truncated automatically", s)));
                l = l < SHRT_MIN ? SHRT_MIN : SHRT_MAX;
            }
            break;
        case sizeof(uint8):
            if (errno == ERANGE || l < 0 || l > UCHAR_MAX) {
                if (!can_ignore) {
                    ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                                    errmsg("value \"%s\" is out of range for 8-bit integer", s)));
                }
                ereport(WARNING, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                                errmsg("value \"%s\" is out of range for 8-bit integer. truncated automatically", s)));
                l = l < 0 ? 0 : UCHAR_MAX;
            }
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
 */
int16 pg_strtoint16(const char* s, bool can_ignore)
{
    const char* ptr = s;
    uint16 tmp = 0;
    bool neg = false;

    /* skip leading spaces */
    while (std::isspace(static_cast<unsigned char>(*ptr))) {
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
        if (DB_IS_CMPT(A_FORMAT | PG_FORMAT)) {
            goto invalid_syntax;
        }
        if (DB_IS_CMPT(B_FORMAT)) {
            return 0;
        }
    }

    /* process digits */
    for (;;) {
        uint8 digit = *ptr - '0';

        if (digit >= 10) {
            break;
        }

        ptr++;

        if (unlikely(tmp > -(PG_INT16_MIN / 10))) {
            goto out_of_range;
        }

        tmp = tmp * 10 + digit;
    }

    /* allow trailing whitespace, but not other trailing chars */
    while (std::isspace(static_cast<unsigned char>(*ptr))) {
        ptr++;
    }

    if (unlikely(*ptr != '\0') && u_sess->attr.attr_sql.sql_compatibility != B_FORMAT) {
        goto invalid_syntax;
    }

    if (neg) {
        int16 result;
        if (unlikely(pg_neg_u16_overflow(tmp, &result))) {
            goto out_of_range;
        }
        return result;
    }

    if (unlikely(tmp > PG_INT16_MAX)) {
        goto out_of_range;
    }

    return (int16)tmp;

out_of_range:
    if (!can_ignore) {
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                        errmsg("value \"%s\" is out of range for type %s", s, "smallint")));
    }
    ereport(WARNING, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                    errmsg("value \"%s\" is out of range for type %s. truncated automatically", s, "smallint")));
    return neg ? PG_INT16_MIN : PG_INT16_MAX;

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
 */
int32 pg_strtoint32(const char* s, bool can_ignore)
{
    const char* ptr = s;
    uint32 tmp = 0;
    bool neg = false;

    /* skip leading spaces */
    while (isspace(static_cast<unsigned char>(*ptr))) {
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
        if (DB_IS_CMPT(A_FORMAT | PG_FORMAT)) {
            goto invalid_syntax;
        } else if (DB_IS_CMPT(B_FORMAT)) {
            return 0;
        }
    }

    /* process digits */
    for (;;) {
        uint8 digit = *ptr - '0';

        if (digit >= 10) {
            break;
        }

        ptr++;

        if (unlikely(tmp > -(PG_INT32_MIN / 10))) {
            goto out_of_range;
        }

        tmp = tmp * 10 + digit;
    }

    /* allow trailing whitespace, but not other trailing chars */
    while (isspace(static_cast<unsigned char>(*ptr))) {
        ptr++;
    }

    if (unlikely(*ptr != '\0') && !DB_IS_CMPT(B_FORMAT)) {
        goto invalid_syntax;
    }

    if (neg) {
        int32 result;
        if (unlikely(pg_neg_u32_overflow(tmp, &result))) {
            goto out_of_range;
        }
        return result;
    }

    if (unlikely(tmp > PG_INT32_MAX)) {
        goto out_of_range;
    }

    return (int32)tmp;

out_of_range:
    if (!can_ignore) {
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                        errmsg("value \"%s\" is out of range for type %s", s, "integer")));
    }
    ereport(WARNING, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                    errmsg("value \"%s\" is out of range for type %s. truncated automatically", s, "integer")));
    return neg ? PG_INT32_MIN : PG_INT32_MAX;


invalid_syntax:
    ereport(ERROR,
        (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION), errmsg("invalid input syntax for %s: \"%s\"", "integer", s)));
    return 0;
}

/*
 * Converts a unsigned 8-bit integer to its string representation
 *
 * Caller must ensure that 'a' points to enough memory to hold the result
 * (at least 5 bytes, counting a leading sign and trailing NUL).
 *
 * It doesn't seem worth implementing this separately.
 */
void pg_ctoa(uint8 c, char* a)
{
    pg_ltoa((int32)c, a);
}

/*
 * Converts a signed 16-bit integer to its string representation
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
 * Converts an unsigned 32-bit integer to its string representation,
 * not NUL-terminated, and returns the length of that string representation
 *
 * Caller must ensure that 'a' points to enough memory to hold the result (at
 * least 10 bytes)
 */
static inline int pg_ultoa_n(uint32 value, char* a)
{
    int i = 0;

    /* Degenerate case */
    if (value == 0) {
        *a = '0';
        return 1;
    }

    int olength = decimalLength32(value);

    /*
     * Compute the result string. Use memcpy instead of memcpy_s/memcpy_sp for 
     * better performance.
     */
    while (value >= 10000) {
        const uint32 c = value - 10000 * (value / 10000);
        const uint32 c0 = (c % 100) << 1;
        const uint32 c1 = (c / 100) << 1;

        char* pos = a + olength - i;

        value /= 10000;

        memcpy(pos - 2, DIGIT_TABLE + c0, 2);
        memcpy(pos - 4, DIGIT_TABLE + c1, 2);
        i += 4;
    }
    if (value >= 100) {
        const uint32 c = (value % 100) << 1;

        char* pos = a + olength - i;

        value /= 100;

        memcpy(pos - 2, DIGIT_TABLE + c, 2);
        i += 2;
    }
    if (value >= 10) {
        const uint32 c = value << 1;

        char* pos = a + olength - i;

        memcpy(pos - 2, DIGIT_TABLE + c, 2);
    } else {
        *a = (char)('0' + value);
    }

    return olength;
}

/*
 * Converts a signed 32-bit integer to its string representation,
 * not NUL-terminated, and returns the length of that string representation
 *
 * Caller must ensure that 'a' points to enough memory to hold the result (at
 * least 11 bytes)
 */
static inline int pg_ltoa_n(int32 value, char* a)
{
    uint32 uvalue = (uint32)value;
    int len = 0;

    if (value < 0) {
        uvalue = (uint32)0 - uvalue;
        a[len++] = '-';
    }

    len += pg_ultoa_n(uvalue, a + len);

    return len;
}

/*
 * Converts a signed 32-bit integer to its string representation
 *
 * @param a The buffer to output conversion result. Caller must ensure
 * that `a` points to enough memory to hold the result (at least 12
 * bytes, counting a leading sign and trailing NUL).
 */
void pg_ltoa(int32 value, char* a)
{
    int len = pg_ltoa_n(value, a);
    a[len] = '\0';
}

void pg_ltoa(int32 value, char* a, int* len)
{
    *len = pg_ltoa_n(value, a);
    a[*len] = '\0';
}

/*
 * Get the decimal representation, not NUL-terminated, and return the length of
 * same. Caller must ensure that a points to at least MAXINT8LEN bytes.
 */
static inline int pg_ulltoa_n(uint64 value, char* a)
{
    int i = 0;

    /* Degenerate case */
    if (value == 0) {
        *a = '0';
        return 1;
    }

    int olength = decimalLength64(value);

    /*
     * Compute the result string. Use memcpy instead of memcpy_s/memcpy_sp for 
     * better performance.
     */
    while (value >= 100000000) {
        const uint64 q = value / 100000000;
        uint32 value3 = (uint32)(value - 100000000 * q);

        const uint32 c = value3 % 10000;
        const uint32 d = value3 / 10000;
        const uint32 c0 = (c % 100) << 1;
        const uint32 c1 = (c / 100) << 1;
        const uint32 d0 = (d % 100) << 1;
        const uint32 d1 = (d / 100) << 1;

        char* pos = a + olength - i;

        value = q;

        memcpy(pos - 2, DIGIT_TABLE + c0, 2);
        memcpy(pos - 4, DIGIT_TABLE + c1, 2);
        memcpy(pos - 6, DIGIT_TABLE + d0, 2);
        memcpy(pos - 8, DIGIT_TABLE + d1, 2);
        i += 8;
    }

    /* Switch to 32-bit for speed */
    uint32 value2 = (uint32)value;

    if (value2 >= 10000) {
        const uint32 c = value2 - 10000 * (value2 / 10000);
        const uint32 c0 = (c % 100) << 1;
        const uint32 c1 = (c / 100) << 1;

        char* pos = a + olength - i;

        value2 /= 10000;

        memcpy(pos - 2, DIGIT_TABLE + c0, 2);
        memcpy(pos - 4, DIGIT_TABLE + c1, 2);
        i += 4;
    }
    if (value2 >= 100) {
        const uint32 c = (value2 % 100) << 1;
        char* pos = a + olength - i;

        value2 /= 100;

        memcpy(pos - 2, DIGIT_TABLE + c, 2);
        i += 2;
    }
    if (value2 >= 10) {
        const uint32 c = value2 << 1;
        char* pos = a + olength - i;

        memcpy(pos - 2, DIGIT_TABLE + c, 2);
    } else {
        *a = (char)('0' + value2);
    }

    return olength;
}

/*
 * Get the decimal representation, not NUL-terminated, and return the length of
 * same.  Caller must ensure that a points to at least MAXINT8LEN bytes.
 */
static inline int pg_lltoa_n(int64 value, char* a)
{
    uint64 uvalue = value;
    int len = 0;

    if (value < 0) {
        uvalue = (uint64)0 - uvalue;
        a[len++] = '-';
    }

    len += pg_ulltoa_n(uvalue, a + len);

    return len;
}

/*
 * Convert a signed 64-bit integer to its string representation
 *
 * Caller must ensure that 'a' points to enough memory to hold the result
 * (at least MAXINT8LEN+1 bytes, counting a leading sign and trailing NUL).
 */
void pg_lltoa(int64 value, char* a)
{
    int len = pg_lltoa_n(value, a);
    a[len] = '\0';
}

void pg_lltoa(int64 value, char* a, int* len)
{
    *len = pg_lltoa_n(value, a);
    a[*len] = '\0';
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
 * pg_ultostr
 *		Converts 'value' into a decimal string representation stored at 'str'.
 *
 * Returns the ending address of the string result (the last character written
 * plus 1).  Note that no NUL terminator is written.
 *
 * The intended use-case for this function is to build strings that contain
 * multiple individual numbers, for example:
 *
 *	str = pg_ultostr(str, a);
 *	*str++ = ' ';
 *	str = pg_ultostr(str, b);
 *	*str = '\0';
 *
 * Note: Caller must ensure that 'str' points to enough memory to hold the
 * result.
 */
char* pg_ultostr(char* str, uint32 value)
{
    int len = pg_ultoa_n(value, str);

    return str + len;
}

/*
 * Converts 'value' into a decimal string representation stored at 'str'.
 * 'min_width' specifies the minimum width of the result; any extra space
 * is filled up by prefixing the number with zeros.
 *
 * Returns the ending address of the string result (the last character written
 * plus 1).  Note that no NUL terminator is written.
 *
 * The intended use-case for this function is to build strings that contain
 * multiple individual numbers, for example:
 *
 * ```cpp
 * str = pg_ultostr_zeropad(str, hours, 2);
 * *str++ = ':';
 * str = pg_ultostr_zeropad(str, mins, 2);
 * *str++ = ':';
 * str = pg_ultostr_zeropad(str, secs, 2);
 * *str = '\0';
 * ```
 *
 * Note: Caller must ensure that 'str' points to enough memory to hold the
 * result
 */
char* pg_ultostr_zeropad(char* str, uint32 value, int min_width)
{
    int len;
    errno_t rc = EOK;

    Assert(min_width > 0);

    len = pg_ultoa_n(value, str);
    if (len >= min_width) {
        return str + len;
    }

    rc = memmove_s(str + min_width - len, len, str, len);
    securec_check(rc, "\0", "\0");
    rc = memset_s(str, min_width - len, '0', min_width - len);
    securec_check(rc, "\0", "\0");
    return str + min_width;
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
