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
 *
 * numeric.cpp
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_fmt\numeric.cpp
 *
 * -------------------------------------------------------------------------
 */
 
#include "numeric.h"
#include "client_logic_cache/icached_column_manager.h"
#include "client_logic_common/client_logic_utils.h"
#include "libpq-int.h"

#define palloc(sz) calloc(sz, sizeof(unsigned char))
#define pfree free

#define ereport(a, b) return false;
#define NUMERIC_HDRSZ (VARHDRSZ + sizeof(uint16) + sizeof(int16))
#define NUMERIC_BI_MASK 0xF000
#define NUMERIC_IS_NAN(n) (NUMERIC_NB_FLAGBITS(n) == NUMERIC_NAN)
#define NUMERIC_HDRSZ_SHORT (VARHDRSZ + sizeof(uint16))
#define VEC_TO_CHOICE(res) ((NumericChoice *)(res))
#define NUMERIC_FLAGBITS(n) ((n)->choice.n_header & NUMERIC_SIGN_MASK)

#define NUMERIC_HEADER_IS_SHORT(n)	(((n)->choice.n_header & 0x8000) != 0)
#define NUMERIC_HEADER_SIZE(n) (VARHDRSZ + sizeof(uint16) + (NUMERIC_HEADER_IS_SHORT(n) ? 0 : sizeof(int16)))
#define NUMERIC_IS_SHORT(n) (NUMERIC_FLAGBITS(n) == NUMERIC_SHORT)
#define NUMERIC_SHORT_SIGN_MASK 0x2000
#define NUMERIC_DSCALE_MASK 0x3FFF
#define NUMERIC_SIGN(n)                                                                           \
    (NUMERIC_IS_SHORT(n)                                                                   \
         ? (((n)->choice.n_short.n_header & NUMERIC_SHORT_SIGN_MASK) ? NUMERIC_NEG : NUMERIC_POS) \
         : NUMERIC_FLAGBITS(n))
#define NUMERIC_DSCALE(n)                                                                           \
    (NUMERIC_HEADER_IS_SHORT((n))                                                                   \
         ? ((n)->choice.n_short.n_header & NUMERIC_SHORT_DSCALE_MASK) >> NUMERIC_SHORT_DSCALE_SHIFT \
         : ((n)->choice.n_long.n_sign_dscale & NUMERIC_DSCALE_MASK))

#define NUMERIC_WEIGHT(n)                                                                                      \
    (NUMERIC_HEADER_IS_SHORT((n))                                                                              \
         ? (((n)->choice.n_short.n_header & NUMERIC_SHORT_WEIGHT_SIGN_MASK ? ~NUMERIC_SHORT_WEIGHT_MASK : 0) | \
            ((n)->choice.n_short.n_header & NUMERIC_SHORT_WEIGHT_MASK))                                        \
         : ((n)->choice.n_long.n_weight))

#define NUMERIC_DIGITS(num) (NUMERIC_HEADER_IS_SHORT(num) ? (num)->choice.n_short.n_data : (num)->choice.n_long.n_data)
#define NUMERIC_64 0xD000
#define NUMERIC_64VALUE(n) (*((int64 *)((n)->choice.n_bi.n_data)))
#define NUMERIC_SHORT_DSCALE_MASK 0x1F80
#define NUMERIC_SHORT_DSCALE_SHIFT 7
#define NUMERIC_SHORT_WEIGHT_SIGN_MASK 0x0040
#define NUMERIC_SHORT_WEIGHT_MASK 0x003F
#define NUMERIC_SHORT_WEIGHT_MAX NUMERIC_SHORT_WEIGHT_MASK
#define NUMERIC_SHORT_WEIGHT_MIN (-(NUMERIC_SHORT_WEIGHT_MASK + 1))
#define NUMERIC_FLAG_IS_BI64(n) (n == NUMERIC_64)
#define NUMERIC_SHORT_DSCALE_MAX (NUMERIC_SHORT_DSCALE_MASK >> NUMERIC_SHORT_DSCALE_SHIFT)
#ifndef WORDS_LITTLEENDIAN
#define VARSIZE_4B(PTR) (((varattrib_4b_fe *)(PTR))->va_4byte.va_header & 0x3FFFFFFF)
#define SET_VARSIZE_4B(PTR, len) (((varattrib_4b_fe*)(PTR))->va_4byte.va_header = (len)&0x3FFFFFFF)
#else
#define VARSIZE_4B(PTR) ((((varattrib_4b_fe *)(PTR))->va_4byte.va_header >> 2) & 0x3FFFFFFF)
#define SET_VARSIZE_4B(PTR, len) (((varattrib_4b_fe*)(PTR))->va_4byte.va_header = (((uint32)(len)) << 2))
#endif
#define SET_VARSIZE(PTR, len) do { \
        SET_VARSIZE_4B(PTR, len);  \
        *binary_size = len;        \
    } while (0)

#define NUMERIC_NDIGITS(num) ((VARSIZE(num) - NUMERIC_HEADER_SIZE(num)) / sizeof(NumericDigit))

#define VARSIZE(PTR) VARSIZE_4B(PTR)
#define NUMERIC_NB_FLAGBITS(n) ((n)->choice.n_header & NUMERIC_BI_MASK)  // nan or biginteger
#define NUMERIC_IS_BI(n) (NUMERIC_NB_FLAGBITS(n) > NUMERIC_NAN)

#define FREE_POINTER(ptr) do {                        \
        if ((ptr) != NULL) {    \
            pfree((void *)ptr); \
            if (!is_const(ptr)) \
                ptr = NULL;     \
        }                       \
    } while (0)
#define pfree_ext(__p) FREE_POINTER(__p)
#define CNUMERIC_NB_FLAGBITS(n) ((n)->n_header & 0xF000)       // nan or biginteger
#define CNUMERIC_IS_NAN(n) (CNUMERIC_NB_FLAGBITS(n) == NUMERIC_NAN)
#define NUMERIC_128 0xE000
#define NUMERIC_FLAG_IS_BI128(n) (n == NUMERIC_128)
#define OPT_DISPLAY_LEADING_ZERO 1
#define DISPLAY_LEADING_ZERO (behavior_compat_flags & OPT_DISPLAY_LEADING_ZERO)

typedef union {
    struct {                /* Normal varlena (4-byte length) */
        uint32 va_header;
        char va_data[FLEXIBLE_ARRAY_MEMBER];
    } va_4byte;
    struct {                /* Compressed-in-line format */
        uint32 va_header;
        uint32 va_rawsize;  /* Original data size (excludes header) */
        char va_data[FLEXIBLE_ARRAY_MEMBER];    /* Compressed data */
    } va_compressed;
} varattrib_4b_fe;


#define quick_init_var(v)    \
    do {                     \
        (v)->buf = (v)->ndb; \
        (v)->digits = NULL;  \
    } while (0)

#define init_var(v)          \
    do {                     \
        quick_init_var((v)); \
        (v)->ndigits = 0;    \
        (v)->weight = 0;     \
        (v)->sign = 0;       \
        (v)->dscale = 0;     \
    } while (0)

#define digitbuf_alloc(ndigits) \
    ((NumericDigit*) palloc((ndigits) * sizeof(NumericDigit)))

#define digitbuf_free(v)            \
    do {                            \
        if ((v)->buf != (v)->ndb) { \
            pfree((v)->buf);        \
            (v)->buf = (v)->ndb;    \
        }                           \
    } while (0)

#define free_var(v) digitbuf_free((v));

/*
 * Init a var and allocate digit buffer of ndigits digits (plus a spare digit for rounding).
 * Called when first using a var.
 */
#define init_alloc_var(v, n)                    \
    do  {                                       \
        (v)->buf = (v)->ndb;                    \
        (v)->ndigits = (n);                     \
        if ((n) > NUMERIC_LOCAL_NMAX) {         \
            (v)->buf = digitbuf_alloc((n) + 1); \
        }                                       \
        (v)->buf[0] = 0;                        \
        (v)->digits = (v)->buf + 1;             \
    } while (0)


static void zero_var(NumericVar *var);
static void alloc_var(NumericVar *var, int ndigits);
static void strip_var(NumericVar *var);

static const char *set_var_from_str(const char *str, const char *cp, NumericVar *dest, char *err_msg);
static void init_var_from_num(NumericData* num, NumericVar *dest);
static bool get_str_from_var(const NumericVar *var, char *str, size_t max_size);
static NumericVar const_nan = { 0, 0, NUMERIC_NAN, 0, NULL, NULL };

#if DEC_DIGITS == 4
static const int round_powers[4] = {0, 1000, 100, 10};
#endif

#ifdef NUMERIC_DEBUG
static void dump_numeric(const char *str, NumericData* num);
#else
#define dump_numeric(s, n)
#endif

#define NUMERIC_CAN_BE_SHORT(scale, weight)                                         \
    ((scale) <= NUMERIC_SHORT_DSCALE_MAX && (weight) <= NUMERIC_SHORT_WEIGHT_MAX && \
        (weight) >= NUMERIC_SHORT_WEIGHT_MIN)


/*
 * alloc_var() -
 *
 * Allocate a digit buffer of ndigits digits (plus a spare digit for rounding)
 */
static void alloc_var(NumericVar *var, int ndigits)
{
    digitbuf_free(var);
    init_alloc_var(var, ndigits);
}


/*
 * zero_var() -
 *
 * Set a variable to ZERO.
 * Note: its dscale is not touched.
 */
static void zero_var(NumericVar *var)
{
    digitbuf_free(var);
    quick_init_var(var);
    var->ndigits = 0;
    var->weight = 0;         /* by convention; doesn't really matter */
    var->sign = NUMERIC_POS; /* anything but NAN... */
}
/*
 * strip_var
 *
 * Strip any leading and trailing zeroes from a numeric variable
 */
static void strip_var(NumericVar *var)
{
    NumericDigit *digits = var->digits;
    int ndigits = var->ndigits;

    /* Strip leading zeroes */
    while (ndigits > 0 && *digits == 0) {
        digits++;
        var->weight--;
        ndigits--;
    }

    /* Strip trailing zeroes */
    while (ndigits > 0 && digits[ndigits - 1] == 0) {
        ndigits--;
    }

    /* If it's zero, normalize the sign and weight */
    if (ndigits == 0) {
        var->sign = NUMERIC_POS;
        var->weight = 0;
    }

    var->digits = digits;
    var->ndigits = ndigits;
}

/*
 * round_var
 *
 * Round the value of a variable to no more than rscale decimal digits
 * after the decimal point.  NOTE: we allow rscale < 0 here, implying
 * rounding before the decimal point.
 */
static void round_var(NumericVar *var, int rscale)
{
    NumericDigit *digits = var->digits;
    int di;
    int ndigits;
    int carry;

    var->dscale = rscale;

    /* decimal digits wanted */
    di = (var->weight + 1) * DEC_DIGITS + rscale;

    /*
     * If di = 0, the value loses all digits, but could round up to 1 if its
     * first extra digit is >= 5.  If di < 0 the result must be 0.
     */
    if (di < 0) {
        var->ndigits = 0;
        var->weight = 0;
        var->sign = NUMERIC_POS;
    } else {
        /* NBASE digits wanted */
        ndigits = (di + DEC_DIGITS - 1) / DEC_DIGITS;

        /* 0, or number of decimal digits to keep in last NBASE digit */
        di %= DEC_DIGITS;

        if (ndigits < var->ndigits || (ndigits == var->ndigits && di > 0)) {
            var->ndigits = ndigits;

#if DEC_DIGITS == 1
            /* di must be zero */
            carry = (digits[ndigits] >= HALF_NBASE) ? 1 : 0;
#else
            if (di == 0) {
                carry = (digits[ndigits] >= HALF_NBASE) ? 1 : 0;
            } else {
                /* Must round within last NBASE digit */
                int extra, pow10;

#if DEC_DIGITS == 4
                pow10 = round_powers[di];
#elif DEC_DIGITS == 2
                pow10 = 10;
#else
#error unsupported NBASE
#endif
                extra = digits[--ndigits] % pow10;
                digits[ndigits] -= extra;
                carry = 0;
                if (extra >= pow10 / 2) {
                    pow10 += digits[ndigits];
                    if (pow10 >= NBASE) {
                        pow10 -= NBASE;
                        carry = 1;
                    }
                    digits[ndigits] = pow10;
                }
            }
#endif

            /* Propagate carry if needed */
            while (carry) {
                carry += digits[--ndigits];
                if (carry >= NBASE) {
                    digits[ndigits] = carry - NBASE;
                    carry = 1;
                } else {
                    digits[ndigits] = carry;
                    carry = 0;
                }
            }

            if (ndigits < 0) {
                Assert(ndigits == -1); /* better not have added > 1 digit */
                Assert(var->digits > var->buf);
                var->digits--;
                var->ndigits++;
                var->weight++;
            }
        }
    }
}

/*
 * scan_numeric() -
 * Input function for numeric data type
 */
unsigned char *scan_numeric(const PGconn* conn, const char *num, int typmod, size_t *binary_size, char *err_msg)
{
    const char *cp = NULL;

    /* Skip leading spaces */
    cp = num;
    while (*cp) {
        if (!isspace((unsigned char)*cp)) {
            break;
        }
        cp++;
    }

    /* the first parameter is null, we should convert to 0 if sql_compatibility == TD_FORMAT */
    if (conn->client_logic->m_cached_column_manager->get_sql_compatibility() == TD_FORMAT && '\0' == *cp) {
        NumericVar value;
        init_var(&value);
        zero_var(&value);
        return make_result(&value, binary_size, err_msg);
    }

    unsigned char *binary = NULL;
    /* Check for NaN */
    if (pg_strncasecmp(cp, "NaN", 3) == 0) {
        binary = make_result(&const_nan, binary_size, err_msg);
        if (!binary) {
            return NULL;
        }
        /* Should be nothing left but spaces */
        cp += 3;
        while (*cp) {
            if (!isspace((unsigned char)*cp)) {
                free(binary);
                binary = NULL;
                check_sprintf_s(
                    sprintf_s(err_msg, MAX_ERRMSG_LENGTH, "invalid input syntax for type numeric: \"%s\"\n", num));
                return NULL;
            }
            cp++;
        }
    } else {
        /* Use set_var_from_str() to parse a normal numeric value */
        NumericVar value;

        init_var(&value);

        cp = set_var_from_str(num, cp, &value, err_msg);
        if (!cp) {
            free_var(&value);
            return NULL;
        }
        /*
         * We duplicate a few lines of code here because we would like to
         * throw any trailing-junk syntax error before any semantic error
         * resulting from apply_typmod.  We can't easily fold the two cases
         * together because we mustn't apply apply_typmod to a NaN.
         */
        while (*cp) {
            if (!isspace((unsigned char)*cp)) {
                check_sprintf_s(
                    sprintf_s(err_msg, MAX_ERRMSG_LENGTH, "invalid input syntax for type numeric: \"%s\"\n", num));
                return NULL;
            }
            cp++;
        }

        if (!apply_typmod(&value, typmod, err_msg)) {
            free_var(&value);
            return NULL;
        }

        binary = make_result(&value, binary_size, err_msg);
        free_var(&value);
    }
    return binary;
}


bool numerictoa(NumericData* num, char *ascii, size_t max_size)
{
    NumericVar x;

    /*
     * Handle NaN
     */
    if (NUMERIC_IS_NAN(num)) {
        errno_t rc = EOK;
        rc = memcpy_s(ascii, max_size, "NaN", strlen("NaN"));
        securec_check_c(rc, "\0", "\0");
        return true;
    }

    /*
     * Get the number in the variable format
     */
    init_var_from_num(num, &x);
    return get_str_from_var(&x, ascii, max_size);
}
/*
 * apply_typmod() -
 *
 * Do bounds checking and rounding according to the attributes
 * typmod field.
 */
bool apply_typmod(NumericVar *var, int32 typmod, char *err_msg)
{
    int precision;
    int scale;
    int maxdigits;
    int ddigits;
    int i;

    /* Do nothing if we have a default typmod (-1) */
    if (typmod < (int32)(VARHDRSZ)) {
        return true;
    }

    typmod -= VARHDRSZ;
    precision = (typmod >> 16) & 0xffff;
    scale = typmod & 0xffff;
    maxdigits = precision - scale;

    /* Round to target scale (and set var->dscale) */
    round_var(var, scale);

    /*
     * Check for overflow - note we can't do this before rounding, because
     * rounding could raise the weight.  Also note that the var's weight could
     * be inflated by leading zeroes, which will be stripped before storage
     * but perhaps might not have been yet. In any case, we must recognize a
     * true zero, whose weight doesn't mean anything.
     */
    ddigits = (var->weight + 1) * DEC_DIGITS;
    if (ddigits > maxdigits) {
        /* Determine true weight; and check for all-zero result */
        for (i = 0; i < var->ndigits; i++) {
            NumericDigit dig = var->digits[i];

            if (dig) {
                /* Adjust for any high-order decimal zero digits */
#if DEC_DIGITS == 4
                if (dig < 10) {
                    ddigits -= 3;
                } else if (dig < 100) {
                    ddigits -= 2;
                } else if (dig < 1000) {
                    ddigits -= 1;
                }
#elif DEC_DIGITS == 2
                if (dig < 10) {
                    ddigits -= 1;
                }
#elif DEC_DIGITS == 1
                /* no adjustment */
#else
#error unsupported NBASE
#endif
                if (ddigits > maxdigits) {
                    check_sprintf_s(sprintf_s(err_msg, MAX_ERRMSG_LENGTH,
                        "numeric field overflow, A field with precision %d , scale %d must round to an absolute value "
                        "less than 10^%d\n",
                        precision, scale, (maxdigits > 0) ? (int)maxdigits : 1));
                    return false;
                }
            }
            ddigits -= DEC_DIGITS;
        }
    }
    return true;
}

/*
 * make_result() -
 *
 * Create the packed db numeric format in palloc()'d memory from
 * a variable.
 */
unsigned char *make_result(NumericVar *var, size_t *binary_size, char *err_msg)
{
    NumericData* result;
    NumericDigit *digits = var->digits;
    int weight = var->weight;
    int sign = var->sign;
    int n;
    Size len;

    if (sign == NUMERIC_NAN) {
        result = (NumericData*)palloc(NUMERIC_HDRSZ_SHORT);
        if (result == NULL) {
            return NULL;
        }

        SET_VARSIZE(result, NUMERIC_HDRSZ_SHORT);
        result->choice.n_header = NUMERIC_NAN;
        return (unsigned char*) result;
    }

    n = var->ndigits;

    /* truncate leading zeroes */
    while (n > 0 && *digits == 0) {
        digits++;
        weight--;
        n--;
    }
    /* truncate trailing zeroes */
    while (n > 0 && digits[n - 1] == 0) {
        n--;
    }

    /* If zero result, force to weight=0 and positive sign */
    if (n == 0) {
        weight = 0;
        sign = NUMERIC_POS;
    }

    /* Build the result */
    if (NUMERIC_CAN_BE_SHORT(var->dscale, weight)) {
        len = NUMERIC_HDRSZ_SHORT + n * sizeof(NumericDigit);
        result = (NumericData*)palloc(len);
        if (result == NULL) {
            return NULL;
        }
        SET_VARSIZE(result, len);
        result->choice.n_short.n_header =
            (sign == NUMERIC_NEG ? (NUMERIC_SHORT | NUMERIC_SHORT_SIGN_MASK) : NUMERIC_SHORT) |
            (var->dscale << NUMERIC_SHORT_DSCALE_SHIFT) | (weight < 0 ? NUMERIC_SHORT_WEIGHT_SIGN_MASK : 0) |
            (weight & NUMERIC_SHORT_WEIGHT_MASK);
    } else {
        len = NUMERIC_HDRSZ + n * sizeof(NumericDigit);
        result = (NumericData*)palloc(len);
        if (result == NULL) {
            return NULL;
        }
        SET_VARSIZE(result, len);
        result->choice.n_long.n_sign_dscale = sign | (var->dscale & NUMERIC_DSCALE_MASK);
        result->choice.n_long.n_weight = weight;
    }

    if (n != 0) {
        errno_t rc = memcpy_s(NUMERIC_DIGITS(result), n * sizeof(NumericDigit), digits, n * sizeof(NumericDigit));
        securec_check_c(rc, "\0", "\0");
    }

    /* Check for overflow of int16 fields */
    if (NUMERIC_WEIGHT(result) != weight || NUMERIC_DSCALE(result) != var->dscale) {
        libpq_free(result);
        check_sprintf_s(sprintf_s(err_msg, MAX_ERRMSG_LENGTH, "value overflows numeric format\n"));
        return NULL;
    }
    return (unsigned char*) result;
}
/*
 * set_var_from_str()
 *
 * Parse a string and put the number into a variable
 *
 * This function does not handle leading or trailing spaces, and it doesn't
 * accept "NaN" either.  It returns the end+1 position so that caller can
 * check for trailing spaces/garbage if deemed necessary.
 *
 * cp is the place to actually start parsing; str is what to use in error
 * reports.  (Typically cp would be the same except advanced over spaces.)
 */
static const char *set_var_from_str(const char *str, const char *cp, NumericVar *dest, char *err_msg)
{
    bool have_dp = FALSE;
    int i;
    unsigned char *decdigits = NULL;
    int sign = NUMERIC_POS;
    int dweight = -1;
    int ddigits;
    int dscale = 0;
    int weight;
    int ndigits;
    int offset;
    NumericDigit *digits = NULL;

    /*
     * We first parse the string to extract decimal digits and determine the
     * correct decimal weight.  Then convert to NBASE representation.
     */
    switch (*cp) {
        case '+':
            sign = NUMERIC_POS;
            cp++;
            break;

        case '-':
            sign = NUMERIC_NEG;
            cp++;
            break;
        default:
            break;
    }

    if (*cp == '.') {
        have_dp = TRUE;
        cp++;
    }

    if (!isdigit((unsigned char)*cp)) {
        check_sprintf_s(sprintf_s(err_msg, MAX_ERRMSG_LENGTH, "invalid input syntax for type numeric: \"%s\"\n", str));
        return NULL;
    }

    decdigits = (unsigned char *)palloc(strlen(cp) + DEC_DIGITS * 2);

    /* leading padding for digit alignment later */
    errno_t rc = EOK;
    rc = memset_s(decdigits, strlen(cp) + DEC_DIGITS * 2, 0, DEC_DIGITS);
    securec_check_c(rc, "\0", "\0");
    i = DEC_DIGITS;

    while (*cp) {
        if (isdigit((unsigned char)*cp)) {
            decdigits[i++] = *cp++ - '0';
            if (!have_dp) {
                dweight++;
            } else {
                dscale++;
            }
        } else if (*cp == '.') {
            if (have_dp) {
                pfree_ext(decdigits);
                check_sprintf_s(
                    sprintf_s(err_msg, MAX_ERRMSG_LENGTH, "invalid input syntax for type numeric: \"%s\"\n", str));
                return NULL;
            }

            have_dp = TRUE;
            cp++;
        } else {
            break;
        }
    }

    ddigits = i - DEC_DIGITS;
    /* trailing padding for digit alignment later */
    rc = memset_s(decdigits + i, DEC_DIGITS, 0, DEC_DIGITS - 1);
    securec_check_c(rc, "\0", "\0");

    /* Handle exponent, if any */
    if (tolower(*cp) == 'e') {
        long exponent;
        char *endptr = NULL;

        cp++;
        const int base = 10;
        exponent = strtol(cp, &endptr, base);
        if (endptr == cp) {
            pfree_ext(decdigits);
            check_sprintf_s(
                sprintf_s(err_msg, MAX_ERRMSG_LENGTH, "invalid input syntax for type numeric: \"%s\"\n", str));
            return NULL;
        }
        cp = endptr;
        bool exponent_error = exponent > NUMERIC_MAX_PRECISION || exponent < -NUMERIC_MAX_PRECISION;
        if (exponent_error) {
            pfree_ext(decdigits);
            check_sprintf_s(
                sprintf_s(err_msg, MAX_ERRMSG_LENGTH, "invalid input syntax for type numeric: \"%s\"\n", str));
            return NULL;
        }
        dweight += (int)exponent;
        dscale -= (int)exponent;
        if (dscale < 0) {
            dscale = 0;
        }
    }

    /*
     * Okay, convert pure-decimal representation to base NBASE.  First we need
     * to determine the converted weight and ndigits.  offset is the number of
     * decimal zeroes to insert before the first given digit to have a
     * correctly aligned first NBASE digit.
     */
    if (dweight >= 0) {
        weight = (dweight + 1 + DEC_DIGITS - 1) / DEC_DIGITS - 1;
    } else {
        weight = -((-dweight - 1) / DEC_DIGITS + 1);
    }
    offset = (weight + 1) * DEC_DIGITS - (dweight + 1);
    ndigits = (ddigits + offset + DEC_DIGITS - 1) / DEC_DIGITS;

    alloc_var(dest, ndigits);
    dest->sign = sign;
    dest->weight = weight;
    dest->dscale = dscale;

    i = DEC_DIGITS - offset;
    digits = dest->digits;

    while (ndigits-- > 0) {
#if DEC_DIGITS == 4
        *digits++ = ((decdigits[i] * 10 + decdigits[i + 1]) * 10 + decdigits[i + 2]) * 10 + decdigits[i + 3];
#elif DEC_DIGITS == 2
        *digits++ = decdigits[i] * 10 + decdigits[i + 1];
#elif DEC_DIGITS == 1
        *digits++ = decdigits[i];
#else
#error unsupported NBASE
#endif
        i += DEC_DIGITS;
    }

    pfree_ext(decdigits);

    /* Strip any leading/trailing zeroes, and normalize weight if zero */
    strip_var(dest);

    /* Return end+1 position for caller */
    return cp;
}

/*
 * init_var_from_num() -
 *
 *  Initialize a variable from packed db format. The digits array is not
 *  copied, which saves some cycles when the resulting var is not modified.
 *  Also, there's no need to call free_var(), as long as you don't assign any
 *  other value to it (with set_var_* functions, or by using the var as the
 *  destination of a function like add_var())
 *
 *  CAUTION: Do not modify the digits buffer of a var initialized with this
 *  function, e.g by calling round_var() or trunc_var(), as the changes will
 *  propagate to the original NumericData*! It's OK to use it as the destination
 *  argument of one of the calculational functions, though.
 */
static inline void init_var_from_num(NumericData* num, NumericVar* dest)
{
    Assert(!NUMERIC_IS_BI(num));
    dest->ndigits = NUMERIC_NDIGITS(num);
    dest->weight = NUMERIC_WEIGHT(num);
    dest->sign = NUMERIC_SIGN(num);
    dest->dscale = NUMERIC_DSCALE(num);
    dest->digits = NUMERIC_DIGITS(num);
    dest->buf = dest->ndb;
}

/*
 * get_str_from_var() -
 *
 * Convert a var to text representation (guts of numeric_out).
 * CAUTION: var's contents may be modified by rounding!
 * Returns a palloc'd string.
 */
static bool get_str_from_var(const NumericVar *var, char *str, size_t max_size)
{
    int dscale;
    char *cp = NULL;
    char *endcp = NULL;
    int i;
    int d;
    NumericDigit dig;

#if DEC_DIGITS > 1
    NumericDigit d1;
#endif

    dscale = var->dscale;

    /*
     * Allocate space for the result.
     *
     * i is set to the # of decimal digits before decimal point. dscale is the
     * # of decimal digits we will print after decimal point. We may generate
     * as many as DEC_DIGITS-1 excess digits at the end, and in addition we
     * need room for sign, decimal point, null terminator.
     */
    i = (var->weight + 1) * DEC_DIGITS;
    if (i <= 0) {
        i = 1;
    }

    if ((size_t)(i + dscale + DEC_DIGITS + 2) > max_size) {
        return false;
    }
    cp = str;

    /*
     * Output a dash for negative values
     */
    if (var->sign == NUMERIC_NEG) {
        *cp++ = '-';
    }

    /*
     * Output all digits before the decimal point
     */
    if (var->weight < 0) {
        d = var->weight + 1;
    } else {
        for (d = 0; d <= var->weight; d++) {
            dig = (d < var->ndigits) ? var->digits[d] : 0;
            /* In the first digit, suppress extra leading decimal zeroes */
#if DEC_DIGITS == 4
            bool putit = (d > 0);

            d1 = dig / 1000;
            dig -= d1 * 1000;
            putit |= (d1 > 0);
            if (putit) {
                *cp++ = d1 + '0';
            }
            d1 = dig / 100;
            dig -= d1 * 100;
            putit |= (d1 > 0);
            if (putit) {
                *cp++ = d1 + '0';
            }
            d1 = dig / 10;
            dig -= d1 * 10;
            putit |= (d1 > 0);
            if (putit) {
                *cp++ = d1 + '0';
            }
            *cp++ = dig + '0';
#elif DEC_DIGITS == 2
            d1 = dig / 10;
            dig -= d1 * 10;
            if (d1 > 0 || d > 0) {
                *cp++ = d1 + '0';
            }
            *cp++ = dig + '0';
#elif DEC_DIGITS == 1
            *cp++ = dig + '0';
#else
#error unsupported NBASE
#endif
        }
    }

    /*
     * If requested, output a decimal point and all the digits that follow it.
     * We initially put out a multiple of DEC_DIGITS digits, then truncate if
     * needed.
     */
    if (dscale > 0) {
        *cp++ = '.';
        endcp = cp + dscale;
        for (i = 0; i < dscale; d++, i += DEC_DIGITS) {
            dig = (d >= 0 && d < var->ndigits) ? var->digits[d] : 0;
#if DEC_DIGITS == 4
            d1 = dig / 1000;
            dig -= d1 * 1000;
            *cp++ = d1 + '0';
            d1 = dig / 100;
            dig -= d1 * 100;
            *cp++ = d1 + '0';
            d1 = dig / 10;
            dig -= d1 * 10;
            *cp++ = d1 + '0';
            *cp++ = dig + '0';
#elif DEC_DIGITS == 2
            d1 = dig / 10;
            dig -= d1 * 10;
            *cp++ = d1 + '0';
            *cp++ = dig + '0';
#elif DEC_DIGITS == 1
            *cp++ = dig + '0';
#else
#error unsupported NBASE
#endif
        }
        cp = endcp;
    }

    /*
     * terminate the string and return it
     */
    *cp = '\0';
    return true;
}
