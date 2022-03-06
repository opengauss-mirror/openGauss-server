/* -------------------------------------------------------------------------
 *
 * numeric.h
 *	  Definitions for the exact numeric data type of openGauss
 *
 * Original coding 1998, Jan Wieck.  Heavily revised 2003, Tom Lane.
 *
 * Copyright (c) 1998-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 * src/include/utils/numeric.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef _PG_NUMERIC_H_
#define _PG_NUMERIC_H_

#include "fmgr.h"

/* ----------
 * Uncomment the following to enable compilation of dump_numeric()
 * and dump_var() and to get a dump of any result produced by make_result().
 * ----------
#define NUMERIC_DEBUG
 */

/* ----------
 * Local data types
 *
 * Numeric values are represented in a base-NBASE floating point format.
 * Each "digit" ranges from 0 to NBASE-1.  The type NumericDigit is signed
 * and wide enough to store a digit.  We assume that NBASE*NBASE can fit in
 * an int.	Although the purely calculational routines could handle any even
 * NBASE that's less than sqrt(INT_MAX), in practice we are only interested
 * in NBASE a power of ten, so that I/O conversions and decimal rounding
 * are easy.  Also, it's actually more efficient if NBASE is rather less than
 * sqrt(INT_MAX), so that there is "headroom" for mul_var and div_var_fast to
 * postpone processing carries.
 *
 * Values of NBASE other than 10000 are considered of historical interest only
 * and are no longer supported in any sense; no mechanism exists for the client
 * to discover the base, so every client supporting binary mode expects the
 * base-10000 format.  If you plan to change this, also note the numeric
 * abbreviation code, which assumes NBASE=10000.
 * ----------
 */

#if 1
#define NBASE 10000
#define HALF_NBASE 5000
#define DEC_DIGITS 4 /* decimal digits per NBASE digit */
#define NUMERIC_SCALE_ADJUST(scale) ((int64)(scale + DEC_DIGITS - 1) / DEC_DIGITS)
#define MUL_GUARD_DIGITS 2 /* these are measured in NBASE digits */
#define DIV_GUARD_DIGITS 4

typedef int16 NumericDigit;
#endif

/*
 * The Numeric type as stored on disk.
 *
 * If the high bits of the first word of a NumericChoice (n_header, or
 * n_short.n_header, or n_long.n_sign_dscale) are NUMERIC_SHORT, then the
 * numeric follows the NumericShort format; if they are NUMERIC_POS or
 * NUMERIC_NEG, it follows the NumericLong format.	If they are NUMERIC_NAN,
 * it is a NaN.  We currently always store a NaN using just two bytes (i.e.
 * only n_header), but previous releases used only the NumericLong format,
 * so we might find 4-byte NaNs on disk if a database has been migrated using
 * pg_upgrade.	In either case, when the high bits indicate a NaN, the
 * remaining bits are never examined.  Currently, we always initialize these
 * to zero, but it might be possible to use them for some other purpose in
 * the future.
 *
 * In the NumericShort format, the remaining 14 bits of the header word
 * (n_short.n_header) are allocated as follows: 1 for sign (positive or
 * negative), 6 for dynamic scale, and 7 for weight.  In practice, most
 * commonly-encountered values can be represented this way.
 *
 * In the NumericLong format, the remaining 14 bits of the header word
 * (n_long.n_sign_dscale) represent the display scale; and the weight is
 * stored separately in n_weight.
 *
 * NOTE: by convention, values in the packed form have been stripped of
 * all leading and trailing zero digits (where a "digit" is of base NBASE).
 * In particular, if the value is zero, there will be no digits at all!
 * The weight is arbitrary in that case, but we normally set it to zero.
 */

struct NumericShort {
    uint16 n_header;        /* Sign + display scale + weight */
    NumericDigit n_data[1]; /* Digits */
};

struct NumericLong {
    uint16 n_sign_dscale;   /* Sign + display scale */
    int16 n_weight;         /* Weight of 1st digit	*/
    NumericDigit n_data[1]; /* Digits */
};

/*
 * NumericBi is used for big integer(bi64 or bi128)
 * n_header stores mark bits and scale of big integer, first 4 bits to
 * distinguish bi64 and bi128, next 4 bits are not used, the last 8 bits
 * store the scale of bit integer, scale value is between 0 and 38.
 * n_data store big integer value(int64 or int128)
 *
 */
struct NumericBi {
    uint16 n_header;
    uint8 n_data[1];
};

/*
 * Add NumericBi struct to NumericChoice
 */
union NumericChoice {
    uint16 n_header;             /* Header word */
    struct NumericLong n_long;   /* Long form (4-byte header) */
    struct NumericShort n_short; /* Short form (2-byte header) */
    struct NumericBi n_bi;       /* Short form (2-byte header) */
};

struct NumericData {
    int32 vl_len_;              /* varlena header (do not touch directly!) */
    union NumericChoice choice; /* choice of format */
};

/* The actual contents of Numeric are private to numeric.c */
struct NumericData;
typedef struct NumericData* Numeric;

/*
 * Interpretation of high bits.
 */

#define NUMERIC_SIGN_MASK 0xC000
#define NUMERIC_POS 0x0000
#define NUMERIC_NEG 0x4000
#define NUMERIC_SHORT 0x8000
#define NUMERIC_NAN 0xC000

/*
 * big integer macro
 * n_header is the mark bits of numeric struct, when numeric is NAN, n_header
 * marked 0xC000. To distinguish bi64, bi128 and numeric, we use 0xD000 to mark
 * bi64, 0xE000 marks bi128, others are numeric type.
 */
#define NUMERIC_64 0xD000
#define NUMERIC_128 0xE000
#define NUMERIC_BI_MASK 0xF000
#define NUMERIC_BI_SCALEMASK 0x00FF
/*
 * Hardcoded precision limit - arbitrary, but must be small enough that
 * dscale values will fit in 14 bits.
 */
#define NUMERIC_MAX_PRECISION 1000

/*
 * Internal limits on the scales chosen for calculation results
 */
#define NUMERIC_MAX_DISPLAY_SCALE NUMERIC_MAX_PRECISION
#define NUMERIC_MIN_DISPLAY_SCALE 0

#define NUMERIC_MAX_RESULT_SCALE (NUMERIC_MAX_PRECISION * 2)

/*
 * For inherently inexact calculations such as division and square root,
 * we try to get at least this many significant digits; the idea is to
 * deliver a result no worse than float8 would.
 */
#define NUMERIC_MIN_SIG_DIGITS 16

/*
 * numeric nan data length exclude header
 */
#define NUMERIC_NAN_DATALENGTH 0
#define NUMERIC_ZERO_DATALENGTH 2

/*
 * fmgr interface macros
 * DatumGetNumeric function rebuild
 */
#define DatumGetNumeric(X) ((Numeric)PG_DETOAST_DATUM(X))
#define DatumGetNumericCopy(X) ((Numeric)PG_DETOAST_DATUM_COPY(X))
#define NumericGetDatum(X) PointerGetDatum(X)
#define PG_GETARG_NUMERIC(n) DatumGetNumeric(PG_GETARG_DATUM(n))
#define PG_GETARG_NUMERIC_COPY(n) DatumGetNumericCopy(PG_GETARG_DATUM(n))
#define PG_RETURN_NUMERIC(x) return NumericGetDatum(x)

/*
 * Utility functions in numeric.c
 */
extern bool numeric_is_nan(Numeric num);
int32 numeric_maximum_size(int32 typmod);
extern char* numeric_out_sci(Numeric num, int scale);
extern Datum numtodsinterval(PG_FUNCTION_ARGS);
extern int cmp_numerics(Numeric num1, Numeric num2);
extern int128 numeric_int16_internal(Numeric num);

//
// Numeric Compression Codes Area
//
#define INT32_MIN_ASCALE (-2)
#define INT32_MAX_ASCALE (2)
#define INT64_MIN_ASCALE (-4)
#define INT64_MAX_ASCALE (4)

extern const int16 INT16_MIN_VALUE;
extern const int16 INT16_MAX_VALUE;
extern const int32 INT32_MIN_VALUE;
extern const int32 INT32_MAX_VALUE;
extern const int64 INT64_MIN_VALUE;
extern const int64 INT64_MAX_VALUE;

#ifdef ENABLE_UT
extern void test_dump_numeric_to_file(_out_ FILE* fd, _in_ const char* str, _in_ Numeric num);
extern bool test_numeric_dscale_equal(_in_ Numeric v1, _in_ Numeric v2);
extern void test_dump_compressed_numeric_to_file(_out_ FILE* fd, _in_ const char* str, _in_ int64 v, _in_ char ascale);
extern char number_of_tail_zeros[10000];
#endif

/*
 * convert functions between int32|int64|int128 and numerc
 *     convert numeric to int32, int64 or int128
 *     convert int32, int64, int128 to numeric
 */
extern bool convert_short_numeric_to_int32(_in_ int64 v, _in_ char ascale);
extern bool convert_short_numeric_to_int64(_in_ char* inBuf, _out_ int64* v, _out_ char* ascale);
extern int convert_int64_to_short_numeric(_out_ char* outBuf, _in_ int64 v, _in_ char ascale, _in_ int32 typmod);
extern int batch_convert_short_numeric_to_int64(_in_ Datum* batchValues, _in_ char* batchNulls, _in_ int batchRows,
    _in_ bool hasNull, _out_ int64* outInt, _out_ char* outAscales, _out_ bool* outSuccess, _out_ int* outNullCount);
extern int convert_int64_to_short_numeric_byscale(
    _out_ char* outBuf, _in_ __int128_t v, _in_ int32 typmod, _in_ int32 vscale);
extern int convert_int128_to_short_numeric_byscale(
    _out_ char* outBuf, _in_ int128 v, _in_ int32 typmod, _in_ int32 vscale);
extern Datum convert_short_numeric_to_int64(_in_ Numeric inNum, _out_ bool* outSuccess);
extern Datum convert_short_numeric_to_int128(_in_ Numeric inNum, _out_ bool* outSuccess);
extern Datum try_convert_numeric_normal_to_fast(Datum value, ScalarVector *arr = NULL);
extern int64 convert_short_numeric_to_int64_byscale(_in_ Numeric n, _in_ int scale);
extern void convert_short_numeric_to_int128_byscale(_in_ Numeric n, _in_ int scale, _out_ int128& result);
extern int32 get_ndigit_from_numeric(_in_ Numeric num);

/* ----------
 * NumericVar is the format we use for arithmetic.	The digit-array part
 * is the same as the NumericData storage format, but the header is more
 * complex.
 *
 * The value represented by a NumericVar is determined by the sign, weight,
 * ndigits, and digits[] array.
 * Note: the first digit of a NumericVar's value is assumed to be multiplied
 * by NBASE ** weight.	Another way to say it is that there are weight+1
 * digits before the decimal point.  It is possible to have weight < 0.
 *
 * buf points at the physical start of the palloc'd digit buffer for the
 * NumericVar.	digits points at the first digit in actual use (the one
 * with the specified weight).	We normally leave an unused digit or two
 * (preset to zeroes) between buf and digits, so that there is room to store
 * a carry out of the top digit without reallocating space.  We just need to
 * decrement digits (and increment weight) to make room for the carry digit.
 * (There is no such extra space in a numeric value stored in the database,
 * only in a NumericVar in memory.)
 *
 * If buf is NULL then the digit buffer isn't actually palloc'd and should
 * not be freed --- see the constants below for an example.
 *
 * dscale, or display scale, is the nominal precision expressed as number
 * of digits after the decimal point (it must always be >= 0 at present).
 * dscale may be more than the number of physically stored fractional digits,
 * implying that we have suppressed storage of significant trailing zeroes.
 * It should never be less than the number of stored digits, since that would
 * imply hiding digits that are present.  NOTE that dscale is always expressed
 * in *decimal* digits, and so it may correspond to a fractional number of
 * base-NBASE digits --- divide by DEC_DIGITS to convert to NBASE digits.
 *
 * rscale, or result scale, is the target precision for a computation.
 * Like dscale it is expressed as number of *decimal* digits after the decimal
 * point, and is always >= 0 at present.
 * Note that rscale is not stored in variables --- it's figured on-the-fly
 * from the dscales of the inputs.
 *
 * While we consistently use "weight" to refer to the base-NBASE weight of
 * a numeric value, it is convenient in some scale-related calculations to
 * make use of the base-10 weight (ie, the approximate log10 of the value).
 * To avoid confusion, such a decimal-units weight is called a "dweight".
 *
 * NB: All the variable-level functions are written in a style that makes it
 * possible to give one and the same variable as argument and destination.
 * This is feasible because the digit buffer is separate from the variable.
 * ----------
 */
typedef struct NumericVar {
    int ndigits;          /* # of digits in digits[] - can be 0! */
    int weight;           /* weight of first digit */
    int sign;             /* NUMERIC_POS, NUMERIC_NEG, or NUMERIC_NAN */
    int dscale;           /* display scale */
    NumericDigit* buf;    /* start of palloc'd space for digits[] */
    NumericDigit* digits; /* base-NBASE digits */
} NumericVar;

#define init_var(v)        MemSetAligned(v, 0, sizeof(NumericVar))
Numeric makeNumeric(NumericVar* var);
extern Numeric make_result(NumericVar *var);
extern void init_var_from_num(Numeric num, NumericVar* dest);
extern void free_var(NumericVar *var);
extern bool numericvar_to_int64(const NumericVar* var, int64* result);
extern void int64_to_numericvar(int64 val, NumericVar *var);
extern void add_var(NumericVar *var1, NumericVar *var2, NumericVar *result);
extern char *numeric_normalize(Numeric num);

#endif /* _PG_NUMERIC_H_ */
