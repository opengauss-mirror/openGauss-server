/* -------------------------------------------------------------------------
 *
 * numeric_gs.h
 *	  Definitions for the exact numeric data type of openGauss
 *
 * Original coding 1998, Jan Wieck.  Heavily revised 2003, Tom Lane.
 *
 * Copyright (c) 1998-2012, PostgreSQL Global Development Group
 *
 * src/include/utils/numeric_gs.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef _PG_NUMERIC_GS_H_
#define _PG_NUMERIC_GS_H_

#include "fmgr.h"
#include "vecexecutor/vectorbatch.h"

#define NUMERIC_HDRSZ (VARHDRSZ + sizeof(uint16) + sizeof(int16))
#define NUMERIC_HDRSZ_SHORT (VARHDRSZ + sizeof(uint16))

#define NUMERIC_FLAGBITS(n) ((n)->choice.n_header & NUMERIC_SIGN_MASK)
#define NUMERIC_NB_FLAGBITS(n) ((n)->choice.n_header & NUMERIC_BI_MASK)  // nan or biginteger

#define NUMERIC_IS_NAN(n) (NUMERIC_NB_FLAGBITS(n) == NUMERIC_NAN)
#define NUMERIC_IS_SHORT(n) (NUMERIC_FLAGBITS(n) == NUMERIC_SHORT)

/*
 * big integer macro
 * determine the type of numeric
 * verify whether a numeric data is NAN or BI by it's flag.
 */
#define NUMERIC_FLAG_IS_NAN(n) (n == NUMERIC_NAN)
#define NUMERIC_FLAG_IS_NANORBI(n) (n >= NUMERIC_NAN)
#define NUMERIC_FLAG_IS_BI(n) (n > NUMERIC_NAN)
#define NUMERIC_FLAG_IS_BI64(n) (n == NUMERIC_64)
#define NUMERIC_FLAG_IS_BI128(n) (n == NUMERIC_128)

/*
 * big integer macro
 * determine the type of numeric
 * verify whether a numeric data is NAN or BI by itself.
 */
#define NUMERIC_IS_NANORBI(n) (NUMERIC_NB_FLAGBITS(n) >= NUMERIC_NAN)
#define NUMERIC_IS_BI(n) (NUMERIC_NB_FLAGBITS(n) > NUMERIC_NAN)
#define NUMERIC_IS_BI64(n) (NUMERIC_NB_FLAGBITS(n) == NUMERIC_64)
#define NUMERIC_IS_BI128(n) (NUMERIC_NB_FLAGBITS(n) == NUMERIC_128)

/*
 * big integer macro
 * the size of bi64 or bi128 is fixed.
 * the size of bi64 is:
 *     4 bytes(vl_len_) + 2 bytes(n_header) + 8 bytes(int64) = 14 bytes
 * the size of bi128 is:
 *     4 bytes(vl_len_) + 2 bytes(n_header) + 16 bytes(int128) = 22 bytes
 */
#define NUMERIC_64SZ (NUMERIC_HDRSZ_SHORT + sizeof(int64))
#define NUMERIC_128SZ (NUMERIC_HDRSZ_SHORT + sizeof(int128))

/*
 * big integer macro
 * get the scale of bi64 or bi128.
 *     scale = n_header & NUMERIC_BI_SCALEMASK
 * get the value of bi64 or bi128
 *     convert NumericBi.n_data to int64 or int128 pointer, then
 *     assign value.
 */
#define NUMERIC_BI_SCALE(n) ((n)->choice.n_header & NUMERIC_BI_SCALEMASK)
#define NUMERIC_64VALUE(n) (*((int64*)((n)->choice.n_bi.n_data)))

/*
 * If the flag bits are NUMERIC_SHORT or NUMERIC_NAN, we want the short header;
 * otherwise, we want the long one.  Instead of testing against each value, we
 * can just look at the high bit, for a slight efficiency gain.
 */
#define NUMERIC_HEADER_SIZE(n) (VARHDRSZ + sizeof(uint16) + (((NUMERIC_FLAGBITS(n) & 0x8000) == 0) ? sizeof(int16) : 0))

/*
 * Short format definitions.
 */
#define NUMERIC_SHORT_SIGN_MASK 0x2000
#define NUMERIC_SHORT_DSCALE_MASK 0x1F80
#define NUMERIC_SHORT_DSCALE_SHIFT 7
#define NUMERIC_SHORT_DSCALE_MAX (NUMERIC_SHORT_DSCALE_MASK >> NUMERIC_SHORT_DSCALE_SHIFT)
#define NUMERIC_SHORT_WEIGHT_SIGN_MASK 0x0040
#define NUMERIC_SHORT_WEIGHT_MASK 0x003F
#define NUMERIC_SHORT_WEIGHT_MAX NUMERIC_SHORT_WEIGHT_MASK
#define NUMERIC_SHORT_WEIGHT_MIN (-(NUMERIC_SHORT_WEIGHT_MASK + 1))

/*
 * Extract sign, display scale, weight.
 */
#define NUMERIC_DSCALE_MASK 0x3FFF

#define NUMERIC_SIGN(n)                                                                                           \
    (NUMERIC_IS_SHORT(n) ? (((n)->choice.n_short.n_header & NUMERIC_SHORT_SIGN_MASK) ? NUMERIC_NEG : NUMERIC_POS) \
                         : NUMERIC_FLAGBITS(n))
#define NUMERIC_DSCALE(n)                                                                                             \
    (NUMERIC_IS_SHORT((n)) ? ((n)->choice.n_short.n_header & NUMERIC_SHORT_DSCALE_MASK) >> NUMERIC_SHORT_DSCALE_SHIFT \
                           : ((n)->choice.n_long.n_sign_dscale & NUMERIC_DSCALE_MASK))
#define NUMERIC_WEIGHT(n)                                                                                         \
    (NUMERIC_IS_SHORT((n))                                                                                        \
            ? (((n)->choice.n_short.n_header & NUMERIC_SHORT_WEIGHT_SIGN_MASK ? ~NUMERIC_SHORT_WEIGHT_MASK : 0) | \
                  ((n)->choice.n_short.n_header & NUMERIC_SHORT_WEIGHT_MASK))                                     \
            : ((n)->choice.n_long.n_weight))
#define NUMERIC_DIGITS(num) (NUMERIC_IS_SHORT(num) ? (num)->choice.n_short.n_data : (num)->choice.n_long.n_data)
#define NUMERIC_NDIGITS(num) ((VARSIZE(num) - NUMERIC_HEADER_SIZE(num)) / sizeof(NumericDigit))

/*
 * @Description: copy bi64 to ptr,  this operation no need to allocate memory
 * @IN  ptr: the numeric pointer
 * @IN  value: the value of bi64
 * @IN  scale: the scale of bi64
 */
#define MAKE_NUMERIC64(ptr, value, scale)                  \
    do {                                                   \
        Numeric result = (Numeric)(ptr);                   \
        SET_VARSIZE(result, NUMERIC_64SZ);                 \
        result->choice.n_header = NUMERIC_64 + (scale);    \
        *((int64*)(result->choice.n_bi.n_data)) = (value); \
    } while (0)

/*
 * @Description: copy bi128 to ptr, this operation no need to allocate memory
 * @IN  ptr: the numeric pointer
 * @IN  value: the value of bi128
 * @IN  scale: the scale of bi128
 */
#define MAKE_NUMERIC128(ptr, value, scale)                                                   \
    do {                                                                                     \
        Numeric result = (Numeric)(ptr);                                                     \
        SET_VARSIZE(result, NUMERIC_128SZ);                                                  \
        result->choice.n_header = NUMERIC_128 + (scale);                                     \
        errno_t rc = EOK;                                                                    \
        rc = memcpy_s(result->choice.n_bi.n_data, sizeof(int128), (&value), sizeof(int128)); \
        securec_check(rc, "\0", "\0");                                                       \
    } while (0)

/*
 * the header size of short numeric is 6 bytes.
 * the same to NUMERIC_HEADER_SIZE but for short numeric.
 */
#define SHORT_NUMERIC_HEADER_SIZE (VARHDRSZ + sizeof(uint16))

/*
 * the same to NUMERIC_NDIGITS but for short numeric.
 */
#define SHORT_NUMERIC_NDIGITS(num) \
    (AssertMacro(NUMERIC_IS_SHORT(num)), ((VARSIZE(num) - SHORT_NUMERIC_HEADER_SIZE) / sizeof(NumericDigit)))

/*
 * the same to NUMERIC_DIGITS but for short numeric.
 */
#define SHORT_NUMERIC_DIGITS(num) (AssertMacro(NUMERIC_IS_SHORT(num)), (num)->choice.n_short.n_data)

/*
 * @Description: allocate memory for bi64, and assign value to it
 * @IN  value: the value of bi64
 * @IN  scale: the scale of bi64
 */
inline Datum makeNumeric64(int64 value, uint8 scale, ScalarVector *arr = NULL)
{
    Numeric result = NULL;
    if (arr == NULL) {
        result = (Numeric)palloc(NUMERIC_64SZ);
    } else {
        result = (Numeric)arr->m_buf->Allocate(NUMERIC_64SZ);
    }
    SET_VARSIZE(result, NUMERIC_64SZ);
    result->choice.n_header = NUMERIC_64 + scale;
    *((int64*)(result->choice.n_bi.n_data)) = value;
    return (Datum)result;
}

/*
 * @Description: allocate memory for bi128, and assign value to it
 * @IN  value: the value of bi128
 * @IN  scale: the scale of bi128
 */
inline Datum makeNumeric128(int128 value, uint8 scale, ScalarVector *arr = NULL)
{
    Numeric result;
    if (arr == NULL) {
        result = (Numeric)palloc(NUMERIC_128SZ);
    } else {
        result = (Numeric)arr->m_buf->Allocate(NUMERIC_128SZ);
    }
    SET_VARSIZE(result, NUMERIC_128SZ);
    result->choice.n_header = NUMERIC_128 + scale;
    errno_t rc = EOK;
    rc = memcpy_s(result->choice.n_bi.n_data, sizeof(int128), &value, sizeof(int128));
    securec_check(rc, "\0", "\0") return (Datum)result;
}

/* Convert bi64 or bi128 to short numeric */
extern Numeric convert_int64_to_numeric(int64 data, uint8 scale);
extern Numeric convert_int128_to_numeric(int128 data, int scale);

/*
 * @Description: convert bi64 or bi128 to numeric type
 * @IN  val: the bi64 or bi128 data
 * @Return: the result of numeric type
 */
inline Numeric makeNumericNormal(Numeric val)
{
    Assert(NUMERIC_IS_BI(val));

    if (NUMERIC_IS_BI64(val)) {
        return convert_int64_to_numeric(NUMERIC_64VALUE(val), NUMERIC_BI_SCALE(val));
    }

    else if (NUMERIC_IS_BI128(val)) {
        int128 tmp_data = 0;
        errno_t rc = EOK;
        rc = memcpy_s(&tmp_data, sizeof(int128), val->choice.n_bi.n_data, sizeof(int128));
        securec_check(rc, "\0", "\0");
        return convert_int128_to_numeric(tmp_data, NUMERIC_BI_SCALE(val));
    }

    else {
        elog(ERROR, "unrecognized big integer numeric format");
    }
    return NULL;
}

/*
 * @Description: Detoast column numeric data. Column numeric is
 *               a short-header type.
 * @IN num: input numeric data
 * @return: Numeric - detoast numeric data
 */
inline Numeric DatumGetBINumericShort(Datum num)
{
    /*
     * unlikely this is a short-header varlena --- convert to 4-byte header format
     */
    struct varlena* new_attr = NULL;
    struct varlena* old_attr = (struct varlena*)num;
    Size data_size = VARSIZE_SHORT(old_attr) - VARHDRSZ_SHORT;
    Size new_size = data_size + VARHDRSZ;
    errno_t rc = EOK;

    new_attr = (struct varlena*)palloc(new_size);
    SET_VARSIZE(new_attr, new_size);
    rc = memcpy_s(VARDATA(new_attr), new_size, VARDATA_SHORT(old_attr), data_size);
    securec_check(rc, "", "");
    old_attr = new_attr;
    return (Numeric)old_attr;
}

/*
 * @Description: Detoast column numeric data. Column numeric is unlikely
 *               a short-header type, simplify macro DatumGetNumeric
 *
 * @IN num: input numeric data
 * @return: Numeric - detoast numeric data
 */
inline Numeric DatumGetBINumeric(Datum num)
{
    if (likely(!VARATT_IS_SHORT(num))) {
        return (Numeric)num;
    } else {
        return DatumGetBINumericShort(num);
    }
}

#endif /* _PG_NUMERIC_GS_H_ */
