/* -------------------------------------------------------------------------
 *
 * float.c
 *    Functions for the built-in floating-point types.
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *    src/backend/utils/adt/float.c
 *
 * -------------------------------------------------------------------------
 */

#define ereport(a, b) return false
#include "postgres_fe.h"
#include "nodes/pg_list.h"

#include "libpq-int.h"

#include <float.h>
#include <math.h>
#include <limits.h>

#include "common/int.h"
#include "utils/builtins.h"
#include "libpq-int.h"
#include <string>


#ifndef M_PI
/* from my RH5.2 gcc math.h file - thomas 2000-04-03 */
#define M_PI 3.14159265358979323846
#endif

/* Visual C++ etc lacks NAN, and won't accept 0.0/0.0.  NAN definition from
 * http://msdn.microsoft.com/library/default.asp?url=/library/en-us/vclang/html/vclrfNotNumberNANItems.asp
 */
#if defined(WIN32) && !defined(NAN)
static const uint32 nan[2] = {0xffffffff, 0x7fffffff};

#define NAN (*(const double *)nan)
#endif

/* not sure what the following should be, but better to make it over-sufficient */
#define MAXFLOATWIDTH 64
#define MAXDOUBLEWIDTH 128

/* check to see if a float4/8 val has underflowed or overflowed */
#define CHECKFLOATVAL(val, inf_is_valid, zero_is_valid)                                                             \
    do {                                                                                                            \
        if (isinf(val) && !(inf_is_valid))                                                                          \
            ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("value out of range: overflow")));  \
                                                                                                                    \
        if ((val) == 0.0 && !(zero_is_valid))                                                                       \
            ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("value out of range: underflow"))); \
    } while (0)

/* skip leading whitespace */
#define SKIP_LEADING_WHITESPACE(headptr)                                    \
    do {                                                                    \
        while (*(headptr) != '\0' && isspace((unsigned char)*(headptr))) { \
            (headptr)++;                                                    \
        }                                                                   \
    } while (0)


/* skip trailing whitespace */
#define SKIP_TRAILING_WHITESPACE(endptr)                                    \
    do {                                                                    \
        while (*(endptr) != '\0' && isspace((unsigned char)*(endptr))) {   \
            (endptr)++;                                                     \
        }                                                                   \
    } while (0)

#define IS_VAL_HUGE(val) ((val) == 0.0 || (val) >= HUGE_VAL || (val) <= -HUGE_VAL)

/* ========== USER I/O ROUTINES ========== */

/* Configurable GUC parameter */
THR_LOCAL int fe_extra_float_digits = 0; /* Added to DBL_DIG or FLT_DIG */


#ifndef HAVE_CBRT
/*
 * Some machines (in particular, some versions of AIX) have an extern
 * declaration for cbrt() in <math.h> but fail to provide the actual
 * function, which causes configure to not set HAVE_CBRT.  Furthermore,
 * their compilers spit up at the mismatch between extern declaration
 * and static definition.  We work around that here by the expedient
 * of a #define to make the actual name of the static function different.
 */
#define cbrt my_cbrt
static double cbrt(double x);
#endif /* HAVE_CBRT */


/*
 * Routines to provide reasonably platform-independent handling of
 * infinity and NaN.  We assume that isinf() and isnan() are available
 * and work per spec.  (On some platforms, we have to supply our own;
 * see src/port.)  However, generating an Infinity or NaN in the first
 * place is less well standardized; pre-C99 systems tend not to have C99's
 * INFINITY and NAN macros.  We centralize our workarounds for this here.
 */

double fe_get_float8_infinity(void)
{
#ifdef INFINITY
    /* C99 standard way */
    return (double)INFINITY;
#else

    /*
     * On some platforms, HUGE_VAL is an infinity, elsewhere it's just the
     * largest normal double.  We assume forcing an overflow will get us a
     * true infinity.
     */
    return (double)(HUGE_VAL * HUGE_VAL);
#endif
}

float fe_get_float4_infinity(void)
{
#ifdef INFINITY
    /* C99 standard way */
    return (float)INFINITY;
#else

    /*
     * On some platforms, HUGE_VAL is an infinity, elsewhere it's just the
     * largest normal double.  We assume forcing an overflow will get us a
     * true infinity.
     */
    return (float)(HUGE_VAL * HUGE_VAL);
#endif
}

double fe_get_float8_nan(void)
{
    /* (double) NAN doesn't work on some NetBSD/MIPS releases */
#if defined(NAN) && !(defined(__NetBSD__) && defined(__mips__))
    /* C99 standard way */
    return (double) NAN;
#else
    /* Assume we can get a NAN via zero divide */
    return (double)(0.0 / 0.0);
#endif
}

float fe_get_float4_nan(void)
{
#ifdef NAN
    /* C99 standard way */
    return (float) NAN;
#else
    /* Assume we can get a NAN via zero divide */
    return (float)(0.0 / 0.0);
#endif
}


/*
 * Returns -1 if 'val' represents negative infinity, 1 if 'val'
 * represents (positive) infinity, and 0 otherwise. On some platforms,
 * this is equivalent to the isinf() macro, but not everywhere: C99
 * does not specify that isinf() needs to distinguish between positive
 * and negative infinity.
 */
int fe_is_infinite(double val)
{
    int inf = isinf(val);
    if (inf == 0) {
        return 0;
    } else if (val > 0) {
        return 1;
    } else {
        return -1;
    }
}

/*
 *      scan_float4     - converts "num" to float
 *                        restricted syntax: {<sp>} [+|-] {digit} [.{digit}] [<exp>]
 *                        where <sp> is a space, digit is 0-9,
 *                        <exp> is "e" or "E" followed by an integer.
 */
bool scan_float4(const char *num, float4 *ret, char *err_msg)
{
    const char *orig_num = NULL;
    double val;
    const char *endptr = NULL;
    char *tmp = NULL;

    /*
     * endptr points to the first character _after_ the sequence we recognized
     * as a valid floating point number. orig_num points to the original input
     * string.
     */
    orig_num = num;

    /*
     * Check for an empty-string input to begin with, to avoid the vagaries of
     * strtod() on different platforms.
     */
    if (*num == '\0') {
        check_sprintf_s(
            sprintf_s(err_msg, MAX_ERRMSG_LENGTH, " invalid input syntax for type real:%s\n", orig_num));
        return false;
    }

    SKIP_LEADING_WHITESPACE(num);

    errno = 0;
    val = strtod(num, &tmp);
    endptr = tmp;

    /* change -0 to 0 */
    if (*num == '-' && val == 0.0) {
        val += 0.0;
    }

    /* did we not see anything that looks like a double? */
    if (endptr == num || errno != 0) {
        int save_errno = errno;

        /*
         * C99 requires that strtod() accept NaN and [-]Infinity, but not all
         * platforms support that yet (and some accept them but set ERANGE
         * anyway...)  Therefore, we check for these inputs ourselves.
         */
        if (pg_strncasecmp(num, "NaN", strlen("NaN")) == 0) {
            val = fe_get_float4_nan();
            endptr = num + strlen("NaN");
        } else if (pg_strncasecmp(num, "Infinity", strlen("Infinity")) == 0) {
            val = fe_get_float4_infinity();
            endptr = num + strlen("Infinity");
        } else if (pg_strncasecmp(num, "-Infinity", strlen("-Infinity")) == 0) {
            val = -fe_get_float4_infinity();
            endptr = num + strlen("-Infinity");
        } else if (save_errno == ERANGE) {
            /*
             * Some platforms return ERANGE for denormalized numbers (those
             * that are not zero, but are too close to zero to have full
             * precision).  We'd prefer not to throw error for that, so try to
             * detect whether it's a "real" out-of-range condition by checking
             * to see if the result is zero or huge.
             */
            if (IS_VAL_HUGE(val)) {
                check_sprintf_s(
                    sprintf_s(err_msg, MAX_ERRMSG_LENGTH, " %s is out of range for type real\n", orig_num));
                return false;
            }
        } else {
            check_sprintf_s(
                sprintf_s(err_msg, MAX_ERRMSG_LENGTH, " invalid input syntax for type real:%s\n", orig_num));
            return false;
        }
    }
#ifdef HAVE_BUGGY_SOLARIS_STRTOD
    else {
        /*
         * Many versions of Solaris have a bug wherein strtod sets endptr to
         * point one byte beyond the end of the string when given "inf" or
         * "infinity".
         */
        if (endptr != num && endptr[-1] == '\0') {
            endptr--;
        }
    }
#endif /* HAVE_BUGGY_SOLARIS_STRTOD */

#ifdef HAVE_BUGGY_IRIX_STRTOD

    /*
     * In some IRIX versions, strtod() recognizes only "inf", so if the input
     * is "infinity" we have to skip over "inity".  Also, it may return
     * positive infinity for "-inf".
     */
    if (isinf(val)) {
        if (pg_strncasecmp(num, "Infinity", strlen("Infinity")) == 0) {
            val = fe_get_float4_infinity();
            endptr = num + strlen("Infinity");
        } else if (pg_strncasecmp(num, "-Infinity", strlen("-Infinity")) == 0) {
            val = -fe_get_float4_infinity();
            endptr = num + strlen("-Infinity");
        } else if (pg_strncasecmp(num, "-inf", strlen("-inf")) == 0) {
            val = -fe_get_float4_infinity();
            endptr = num + strlen("-inf");
        }
    }
#endif /* HAVE_BUGGY_IRIX_STRTOD */

    SKIP_TRAILING_WHITESPACE(endptr);

    /* if there is any junk left at the end of the string, bail out */
    if (*endptr != '\0') {
        check_sprintf_s(sprintf_s(err_msg, MAX_ERRMSG_LENGTH,
            " invalid input syntax for type double precision: %s\n", orig_num));
        return false;
    }

    /*
     * if we get here, we have a legal double, still need to check to see if
     * it's a legal float4
     */
    check_sprintf_s(sprintf_s(err_msg, MAX_ERRMSG_LENGTH, " %s is out of range for type real\n", orig_num));
    CHECKFLOATVAL((float4)val, isinf(val), val == 0);
    *ret = ((float4)val);
    return true;
}

/*
 *      float4toa       - converts a float4 number to a ascii string
 *                        using a standard output format
 */
bool float4toa(float4 num, char *ascii)
{
    errno_t rc = EOK;

    if (isnan(num)) {
        rc = strcpy_s(ascii, MAXFLOATWIDTH + 1, "NaN");
        securec_check_c(rc, "\0", "\0");
        return true;
    }

    switch (fe_is_infinite(num)) {
        case 1:
            rc = strcpy_s(ascii, MAXFLOATWIDTH + 1, "Infinity");
            securec_check_c(rc, "\0", "\0");
            break;
        case -1:
            rc = strcpy_s(ascii, MAXFLOATWIDTH + 1, "-Infinity");
            securec_check_c(rc, "\0", "\0");
            break;
        default: {
            int ndig = FLT_DIG + fe_extra_float_digits;

            if (ndig < 1)
                ndig = 1;

            rc = snprintf_s(ascii, MAXFLOATWIDTH + 1, MAXFLOATWIDTH, "%.*g", ndig, num);
            securec_check_ss_c(rc, "\0", "\0");
        } break;
    }

    /*
     * Delete 0 before decimal.
     * For Example: convert 0.123 to .123, or -0.123 to -.123
     */
    if (num > 0 && num < 1 && ascii[0] == '0') {
        check_memmove_s(memmove_s(ascii, MAXFLOATWIDTH + 1, ascii + 1, MAXFLOATWIDTH));
    } else if (num > -1 && num < 0 && ascii[1] == '0') {
        check_memmove_s(memmove_s(ascii + 1, MAXFLOATWIDTH, ascii + 2, MAXFLOATWIDTH - 1));
    }
    return true;
}

/*
 *      scan_float8     - converts "num" to float8
 *                        restricted syntax: {<sp>} [+|-] {digit} [.{digit}] [<exp>]
 *                        where <sp> is a space, digit is 0-9,
 *                        <exp> is "e" or "E" followed by an integer.
 */
bool scan_float8(const char *num, float8 *ret, char *err_msg)
{
    const char *orig_num = NULL;
    double val;
    const char *endptr = NULL;
    char *tmp = NULL;

    /*
     * endptr points to the first character _after_ the sequence we recognized
     * as a valid floating point number. orig_num points to the original input
     * string.
     */
    orig_num = num;

    /*
     * Check for an empty-string input to begin with, to avoid the vagaries of
     * strtod() on different platforms.
     */
    if (*num == '\0') {
        check_sprintf_s(sprintf_s(err_msg, MAX_ERRMSG_LENGTH,
            " invalid input syntax for type double precision: %s\n", orig_num));
        return false;
    }

    SKIP_LEADING_WHITESPACE(num);

    errno = 0;
    val = strtod(num, &tmp);
    endptr = tmp;

    /* change -0 to 0 */
    if (*num == '-' && val == 0.0) {
        val += 0.0;
    }

    /* did we not see anything that looks like a double? */
    if (endptr == num || errno != 0) {
        int save_errno = errno;

        /*
         * C99 requires that strtod() accept NaN and [-]Infinity, but not all
         * platforms support that yet (and some accept them but set ERANGE
         * anyway...)  Therefore, we check for these inputs ourselves.
         */
        if (pg_strncasecmp(num, "NaN", strlen("NaN")) == 0) {
            val = fe_get_float8_nan();
            endptr = num + strlen("NaN");
        } else if (pg_strncasecmp(num, "Infinity", strlen("Infinity")) == 0) {
            val = fe_get_float8_infinity();
            endptr = num + strlen("Infinity");
        } else if (pg_strncasecmp(num, "-Infinity", strlen("-Infinity")) == 0) {
            val = -fe_get_float8_infinity();
            endptr = num + strlen("-Infinity");
        } else if (save_errno == ERANGE) {
            /*
             * Some platforms return ERANGE for denormalized numbers (those
             * that are not zero, but are too close to zero to have full
             * precision).  We'd prefer not to throw error for that, so try to
             * detect whether it's a "real" out-of-range condition by checking
             * to see if the result is zero or huge.
             */
            if (IS_VAL_HUGE(val)) {
                check_sprintf_s(sprintf_s(err_msg, MAX_ERRMSG_LENGTH,
                    " %s is out of range for type double precision\n", orig_num));
                return false;
            }
        } else {
            check_sprintf_s(sprintf_s(err_msg, MAX_ERRMSG_LENGTH,
                " invalid input syntax for type double precision:%s\n", orig_num));
            return false;
        }
    }
#ifdef HAVE_BUGGY_SOLARIS_STRTOD
    else {
        /*
         * Many versions of Solaris have a bug wherein strtod sets endptr to
         * point one byte beyond the end of the string when given "inf" or
         * "infinity".
         */
        if (endptr != num && endptr[-1] == '\0') {
            endptr--;
        }
    }
#endif /* HAVE_BUGGY_SOLARIS_STRTOD */

#ifdef HAVE_BUGGY_IRIX_STRTOD

    /*
     * In some IRIX versions, strtod() recognizes only "inf", so if the input
     * is "infinity" we have to skip over "inity".  Also, it may return
     * positive infinity for "-inf".
     */
    if (isinf(val)) {
        if (pg_strncasecmp(num, "Infinity", strlen("Infinity")) == 0) {
            val = fe_get_float8_infinity();
            endptr = num + strlen("Infinity");
        } else if (pg_strncasecmp(num, "-Infinity", strlen("-Infinity")) == 0) {
            val = -fe_get_float8_infinity();
            endptr = num + strlen("-Infinity");
        } else if (pg_strncasecmp(num, "-inf", strlen("-inf")) == 0) {
            val = -fe_get_float8_infinity();
            endptr = num + strlen("-inf");
        }
    }
#endif /* HAVE_BUGGY_IRIX_STRTOD */

    SKIP_TRAILING_WHITESPACE(endptr);

    /* if there is any junk left at the end of the string, bail out */
    if (*endptr != '\0') {
        check_sprintf_s(sprintf_s(err_msg, MAX_ERRMSG_LENGTH,
            " invalid input syntax for type double precision: %s\n", orig_num));
        return false;
    }

    check_sprintf_s(
        sprintf_s(err_msg, MAX_ERRMSG_LENGTH, " %s is out of range for type double precision\n", orig_num));
    CHECKFLOATVAL(val, true, true);
    *ret = val;
    return true;
}


/*
 *      float8toa       - converts float8 number to a ascii string
 *                        using a standard output format
 */
bool float8toa(float8 num, char *ascii)
{
    errno_t rc = EOK;

    if (isnan(num)) {
        rc = strcpy_s(ascii, MAXDOUBLEWIDTH + 1, "NaN");
        securec_check_c(rc, "\0", "\0");
        return true;
    }
    switch (fe_is_infinite(num)) {
        case 1:
            rc = strcpy_s(ascii, MAXDOUBLEWIDTH + 1, "Infinity");
            securec_check_c(rc, "\0", "\0");
            break;
        case -1:
            rc = strcpy_s(ascii, MAXDOUBLEWIDTH + 1, "-Infinity");
            securec_check_c(rc, "\0", "\0");
            break;
        default: {
            int ndig = DBL_DIG + fe_extra_float_digits;

            if (ndig < 1)
                ndig = 1;

            rc = snprintf_s(ascii, MAXDOUBLEWIDTH + 1, MAXDOUBLEWIDTH, "%.*g", ndig, num);
            securec_check_ss_c(rc, "\0", "\0");
        } break;
    }
    /*
     * Delete 0 before decimal.
     * For Example: convert 0.123 to .123, or -0.123 to -.123
     */
    if (num > 0 && num < 1) {
        check_memmove_s(memmove_s(ascii, MAXFLOATWIDTH + 1, ascii + 1, MAXFLOATWIDTH));
    } else if (num > -1 && num < 0) {
        check_memmove_s(memmove_s(ascii + 1, MAXFLOATWIDTH, ascii + 2, MAXFLOATWIDTH - 1));
    }
    return true;
}
