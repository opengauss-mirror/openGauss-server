/* -------------------------------------------------------------------------
 *
 * numeric.c
 *	  An exact numeric data type for the openGauss database system
 *
 * Original coding 1998, Jan Wieck.  Heavily revised 2003, Tom Lane.
 *
 * Many of the algorithmic ideas are borrowed from David M. Smith's "FM"
 * multiple-precision math library, most recently published as Algorithm
 * 786: Multiple-Precision Complex Arithmetic and Functions, ACM
 * Transactions on Mathematical Software, Vol. 24, No. 4, December 1998,
 * pages 359-367.
 *
 * Copyright (c) 1998-2012, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/numeric.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include <float.h>
#include <limits.h>
#include <math.h>

#include "access/hash.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "common/int.h"
#include "lib/hyperloglog.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/biginteger.h"
#include "utils/gs_bitmap.h"
#include "utils/guc.h"
#include "utils/int8.h"
#include "utils/numeric.h"
#include "utils/sortsupport.h"
#include "vecexecutor/vechashtable.h"
#include "vecexecutor/vechashagg.h"
#include "vectorsonic/vsonichashagg.h"

/* ----------
 * Data for generate_series
 * ----------
 */
typedef struct {
    NumericVar current;
    NumericVar stop;
    NumericVar step;
} generate_series_numeric_fctx;

/* ----------
 * Fast sum accumulator.
 *
 * NumericSumAccum is used to implement SUM(), and other standard aggregates
 * that track the sum of input values.  It uses 32-bit integers to store the
 * digits, instead of the normal 16-bit integers (with NBASE=10000).  This
 * way, we can safely accumulate up to NBASE - 1 values without propagating
 * carry, before risking overflow of any of the digits.  'num_uncarried'
 * tracks how many values have been accumulated without propagating carry.
 *
 * Positive and negative values are accumulated separately, in 'pos_digits'
 * and 'neg_digits'.  This is simpler and faster than deciding whether to add
 * or subtract from the current value, for each new value (see sub_var() for
 * the logic we avoid by doing this).  Both buffers are of same size, and
 * have the same weight and scale.  In accum_sum_final(), the positive and
 * negative sums are added together to produce the final result.
 *
 * When a new value has a larger ndigits or weight than the accumulator
 * currently does, the accumulator is enlarged to accommodate the new value.
 * We normally have one zero digit reserved for carry propagation, and that
 * is indicated by the 'have_carry_space' flag.  When accum_sum_carry() uses
 * up the reserved digit, it clears the 'have_carry_space' flag.  The next
 * call to accum_sum_add() will enlarge the buffer, to make room for the
 * extra digit, and set the flag again.
 *
 * To initialize a new accumulator, simply reset all fields to zeros.
 *
 * The accumulator does not handle NaNs.
 * ----------
 */

/* ----------
 * Sort support.
 * ----------
 */
typedef struct {
    void* buf;         /* buffer for short varlenas */
    int64 input_count; /* number of non-null values seen */
    bool estimating;   /* true if estimating cardinality */

    hyperLogLogState abbr_card; /* cardinality estimator */
} NumericSortSupport;

typedef struct NumericSumAccum
{
    int     ndigits;
    int     weight;
    int     dscale;
    int     num_uncarried;
    bool    have_carry_space;
    int32   *pos_digits;
    int32   *neg_digits;
} NumericSumAccum;

typedef struct NumericAggState
{
    bool        calcSumX2;      /* if true, calculate sumX2 */
    bool        isNaN;          /* true if any processed number was NaN */
    MemoryContext agg_context;  /* context we're calculating in */
    int64       N;              /* count of processed numbers */
    NumericSumAccum  sumX;           /* sum of processed numbers */
    NumericSumAccum  sumX2;          /* sum of squares of processed numbers */
} NumericAggState;

#define NUMERIC_ABBREV_BITS (SIZEOF_DATUM * BITS_PER_BYTE)
#if SIZEOF_DATUM == 8
#define DatumGetNumericAbbrev(d) ((int64)d)
#define NUMERIC_ABBREV_NAN Int64GetDatum(PG_INT64_MIN)
#else
#define DatumGetNumericAbbrev(d) ((int32)(d))
#define NUMERIC_ABBREV_NAN Int32GetDatum(PG_INT32_MIN)
#endif

/* ----------
 * Some preinitialized constants
 * ----------
 */
static NumericDigit const_zero_data[1] = {0};
static NumericVar const_zero = {0, 0, NUMERIC_POS, 0, NULL, const_zero_data};

static NumericDigit const_one_data[1] = {1};
static NumericVar const_one = {1, 0, NUMERIC_POS, 0, NULL, const_one_data};

static NumericDigit const_two_data[1] = {2};
static NumericVar const_two = {1, 0, NUMERIC_POS, 0, NULL, const_two_data};

#if DEC_DIGITS == 4 || DEC_DIGITS == 2
static NumericDigit const_ten_data[1] = {10};
static NumericVar const_ten = {1, 0, NUMERIC_POS, 0, NULL, const_ten_data};
#elif DEC_DIGITS == 1
static NumericDigit const_ten_data[1] = {1};
static NumericVar const_ten = {1, 1, NUMERIC_POS, 0, NULL, const_ten_data};
#endif

#if DEC_DIGITS == 4
static NumericDigit const_zero_point_five_data[1] = {5000};
#elif DEC_DIGITS == 2
static NumericDigit const_zero_point_five_data[1] = {50};
#elif DEC_DIGITS == 1
static NumericDigit const_zero_point_five_data[1] = {5};
#endif
static NumericVar const_zero_point_five = {1, -1, NUMERIC_POS, 1, NULL, const_zero_point_five_data};

#if DEC_DIGITS == 4
static NumericDigit const_zero_point_nine_data[1] = {9000};
#elif DEC_DIGITS == 2
static NumericDigit const_zero_point_nine_data[1] = {90};
#elif DEC_DIGITS == 1
static NumericDigit const_zero_point_nine_data[1] = {9};
#endif
static NumericVar const_zero_point_nine = {1, -1, NUMERIC_POS, 1, NULL, const_zero_point_nine_data};

#if DEC_DIGITS == 4
static NumericDigit const_one_point_one_data[2] = {1, 1000};
#elif DEC_DIGITS == 2
static NumericDigit const_one_point_one_data[2] = {1, 10};
#elif DEC_DIGITS == 1
static NumericDigit const_one_point_one_data[2] = {1, 1};
#endif
static NumericVar const_one_point_one = {2, 0, NUMERIC_POS, 1, NULL, const_one_point_one_data};

static NumericVar const_nan = {0, 0, NUMERIC_NAN, 0, NULL, NULL};

#if DEC_DIGITS == 4
static const int round_powers[4] = {0, 1000, 100, 10};
#endif

/* ----------
 * Local functions
 * ----------
 */

#ifdef NUMERIC_DEBUG
static void dump_numeric(const char* str, Numeric num);
static void dump_var(const char* str, NumericVar* var);
#else
#define dump_numeric(s, n)
#define dump_var(s, v)
#endif

#define NUMERIC_CAN_BE_SHORT(scale, weight)                                         \
    ((scale) <= NUMERIC_SHORT_DSCALE_MAX && (weight) <= NUMERIC_SHORT_WEIGHT_MAX && \
        (weight) >= NUMERIC_SHORT_WEIGHT_MIN)

static void alloc_var(NumericVar* var, int ndigits);
static void zero_var(NumericVar* var);

static void init_ro_var_from_var(const NumericVar* value, NumericVar* dest);

static const char* set_var_from_str(const char* str, const char* cp, NumericVar* dest);
static void set_var_from_num(Numeric value, NumericVar* dest);
static void set_var_from_var(const NumericVar* value, NumericVar* dest);
static void init_var_from_var(const NumericVar *value, NumericVar *dest);
static char* get_str_from_var(NumericVar* var);
static char* output_get_str_from_var(NumericVar* var);
static char* get_str_from_var_sci(NumericVar* var, int rscale);

static void apply_typmod(NumericVar* var, int32 typmod);

static int32 numericvar_to_int32(const NumericVar* var, bool can_ignore = false);
static double numericvar_to_double_no_overflow(NumericVar* var);

static Datum numeric_abbrev_convert(Datum original_datum, SortSupport ssup);
static bool numeric_abbrev_abort(int memtupcount, SortSupport ssup);
static int numeric_fast_cmp(Datum x, Datum y, SortSupport ssup);
static int numeric_cmp_abbrev(Datum x, Datum y, SortSupport ssup);

static Datum numeric_abbrev_convert_var(NumericVar* var, NumericSortSupport* nss);

static int cmp_var(NumericVar* var1, NumericVar* var2);
static int cmp_var_common(const NumericDigit* var1digits, int var1ndigits, int var1weight, int var1sign,
    const NumericDigit* var2digits, int var2ndigits, int var2weight, int var2sign);
static void sub_var(NumericVar* var1, NumericVar* var2, NumericVar* result);
static void mul_var(NumericVar* var1, NumericVar* var2, NumericVar* result, int rscale);
static void div_var(NumericVar* var1, NumericVar* var2, NumericVar* result, int rscale, bool round);
static void div_var_fast(NumericVar* var1, NumericVar* var2, NumericVar* result, int rscale, bool round);
static void div_var_int(const NumericVar *var, int ival, int ival_weight, NumericVar *result, int rscale, bool round);
static int select_div_scale(NumericVar* var1, NumericVar* var2);
static void mod_var(NumericVar* var1, NumericVar* var2, NumericVar* result);
static void ceil_var(NumericVar* var, NumericVar* result);
static void floor_var(NumericVar* var, NumericVar* result);

static void sqrt_var(NumericVar* arg, NumericVar* result, int rscale);
static void exp_var(NumericVar* arg, NumericVar* result, int rscale);
static int estimate_ln_dweight(NumericVar* var);
static void ln_var(NumericVar* arg, NumericVar* result, int rscale);
static void log_var(NumericVar* base, NumericVar* num, NumericVar* result);
static void power_var(NumericVar* base, NumericVar* exp, NumericVar* result);
static void power_var_int(NumericVar* base, int exp, NumericVar* result, int rscale);

static int cmp_abs(NumericVar* var1, NumericVar* var2);
static int cmp_abs_common(const NumericDigit* var1digits, int var1ndigits, int var1weight,
    const NumericDigit* var2digits, int var2ndigits, int var2weight);
static void add_abs(NumericVar* var1, NumericVar* var2, NumericVar* result);
static void sub_abs(NumericVar* var1, NumericVar* var2, NumericVar* result);
static void round_var(NumericVar* var, int rscale);
static void trunc_var(NumericVar* var, int rscale);
static void strip_var(NumericVar* var);
static void compute_bucket(
    Numeric operand, Numeric bound1, Numeric bound2, NumericVar* count_var, NumericVar* result_var);
static void remove_tail_zero(char *ascii);

static void accum_sum_add(NumericSumAccum *accum, NumericVar *var1);
static void accum_sum_rescale(NumericSumAccum *accum, NumericVar *val);
static void accum_sum_carry(NumericSumAccum *accum);
static void accum_sum_final(NumericSumAccum *accum, NumericVar *result);

static void accum_sum_add(NumericSumAccum *accum, NumericVar *var1);
static void accum_sum_rescale(NumericSumAccum *accum, NumericVar *val);
static void accum_sum_carry(NumericSumAccum *accum);
static void accum_sum_final(NumericSumAccum *accum, NumericVar *result);

/*
 * @Description: call corresponding big integer operator functions.
 *
 * @IN op: template parameter, assign the operation name, e.g. add, sub, etc.
 * @IN larg: left-hand operand of operator.
 * @IN rarg: right-hand operand of operator.
 * @return: Datum - the datum data points to result of letfc op rightc.
 */
template <biop op>
inline Datum bipickfun(Numeric leftc, Numeric rightc)
{
    Assert(NUMERIC_IS_BI(leftc));
    Assert(NUMERIC_IS_BI(rightc));

    int left_type = NUMERIC_IS_BI128(leftc);
    int right_type = NUMERIC_IS_BI128(rightc);
    biopfun func = BiFunMatrix[op][left_type][right_type];

    Assert(func != NULL);
    /* call big integer fast calculate function */
    return func(leftc, rightc, NULL);
}

/* ----------------------------------------------------------------------
 *
 * Input-, output- and rounding-functions
 *
 * ----------------------------------------------------------------------
 */

/*
 * numeric_in() -
 *
 *	Input function for numeric data type
 */
Datum numeric_in(PG_FUNCTION_ARGS)
{
    char* str = PG_GETARG_CSTRING(0);

#ifdef NOT_USED
    Oid typelem = PG_GETARG_OID(1);
#endif
    int32 typmod = PG_GETARG_INT32(2);
    Numeric res;
    const char* cp = NULL;

    /* Skip leading spaces */
    cp = str;
    while (*cp) {
        if (!isspace((unsigned char)*cp))
            break;
        cp++;
    }

    /* the first parameter is null, we should convert to 0 if u_sess->attr.attr_sql.sql_compatibility == C_FORMAT */
    if (u_sess->attr.attr_sql.sql_compatibility == C_FORMAT && '\0' == *cp) {
        NumericVar value;
        init_var(&value);

        zero_var(&value);
        res = make_result(&value);
        PG_RETURN_NUMERIC(res);
    }

    /*
     * Check for NaN
     */
    int level = fcinfo->can_ignore ? WARNING : ERROR;
    if (pg_strncasecmp(cp, "NaN", 3) == 0) {
        res = make_result(&const_nan);

        /* Should be nothing left but spaces */
        cp += 3;
        while (*cp) {
            if (!isspace((unsigned char)*cp)) {
                ereport(level,
                    (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                        errmsg("invalid input syntax for type numeric: \"%s\"", str)));
                /* for this invalid input, if fcinfo->can_ignore == true, directly return 0 */
                PG_RETURN_NUMERIC(0);
            }
            cp++;
        }
    } else {
        /*
         * Use set_var_from_str() to parse a normal numeric value
         */
        NumericVar value;

        init_var(&value);

        cp = set_var_from_str(str, cp, &value);

        /*
         * We duplicate a few lines of code here because we would like to
         * throw any trailing-junk syntax error before any semantic error
         * resulting from apply_typmod.  We can't easily fold the two cases
         * together because we mustn't apply apply_typmod to a NaN.
         */
        while (*cp) {
            if (!isspace((unsigned char)*cp)) {
                ereport(level,
                    (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                        errmsg("invalid input syntax for type numeric: \"%s\"", str)));
                /* for this invalid input, if fcinfo->can_ignore == true, handle value only and
                 * discard rest of invalid characters
                 */
                break;
            }
            cp++;
        }

        apply_typmod(&value, typmod);

        res = make_result(&value);
        free_var(&value);
    }

    PG_RETURN_NUMERIC(res);
}

/*
 * numeric_out_with_zero -
 *
 *	Output function for numeric data type.
 *	include bi64 and bi128 type
 */
Datum numeric_out_with_zero(PG_FUNCTION_ARGS)
{
    Numeric num = PG_GETARG_NUMERIC(0);
    NumericVar x;
    char* str = NULL;
    int scale = 0;

    /*
     * Handle NaN
     */
    if (NUMERIC_IS_NAN(num))
        PG_RETURN_CSTRING(pstrdup("NaN"));

    /*
     * If numeric is big integer, call int64_out/int128_out
     */
    uint16 numFlags = NUMERIC_NB_FLAGBITS(num);
    if (NUMERIC_FLAG_IS_BI64(numFlags)) {
        int64 val64 = NUMERIC_64VALUE(num);
        scale = NUMERIC_BI_SCALE(num);
        return bi64_out(val64, scale);
    } else if (NUMERIC_FLAG_IS_BI128(numFlags)) {
        int128 val128 = 0;
        errno_t rc = memcpy_s(&val128, sizeof(int128), (num)->choice.n_bi.n_data, sizeof(int128));
        securec_check(rc, "\0", "\0");
        scale = NUMERIC_BI_SCALE(num);
        return bi128_out(val128, scale);
    }

    /*
     * Get the number in the variable format
     */
    init_var_from_num(num, &x);
    str = get_str_from_var(&x);

    /*
     * free memory if allocated by the toaster
     */
    PG_FREE_IF_COPY(num, 0);

    PG_RETURN_CSTRING(str);
}

/*
 * numeric_out() -
 *
 *  Output function for numeric data type.
 *  include bi64 and bi128 type
 *  Call function numeric_out_with_zero with pg origin logic, and then check if need trunc tail zero or not.
 */
Datum numeric_out(PG_FUNCTION_ARGS)
{
    char* ans = DatumGetCString(numeric_out_with_zero(fcinfo));

    if (TRUNC_NUMERIC_TAIL_ZERO) {
        remove_tail_zero(ans);
    }

    PG_RETURN_CSTRING(ans);
}

/*
 * output_numeric_out() -
 *
 *      Output function for numeric data type.
 *      include bi64 and bi128 type
 */
char* output_numeric_out(Numeric num)
{
    NumericVar x;
    char* str = NULL;
    int scale = 0;

    /*
     * Handle NaN
     */
    if (NUMERIC_IS_NAN(num))
        return pstrdup("NaN");

    /*
     * If numeric is big integer, call int64_out/int128_out
     */
    uint16 numFlags = NUMERIC_NB_FLAGBITS(num);
    if (NUMERIC_FLAG_IS_BI64(numFlags)) {
        int64 val64 = NUMERIC_64VALUE(num);
        scale = NUMERIC_BI_SCALE(num);
        Datum numeric_out_bi64 = bi64_out(val64, scale);
        return DatumGetCString(numeric_out_bi64);
    } else if (NUMERIC_FLAG_IS_BI128(numFlags)) {
        int128 val128 = 0;
        errno_t rc = memcpy_s(&val128, sizeof(int128), (num)->choice.n_bi.n_data, sizeof(int128));
        securec_check(rc, "\0", "\0");
        scale = NUMERIC_BI_SCALE(num);
        Datum numeric_out_bi128 =  bi128_out(val128, scale);
        return DatumGetCString(numeric_out_bi128);
    }
    /*
     * Get the number in the variable format
     */
    init_var_from_num(num, &x);
    str = output_get_str_from_var(&x);

    if (TRUNC_NUMERIC_TAIL_ZERO) {
        remove_tail_zero(str);
    }

    return str;
}

/*
 * numeric_is_nan() -
 *
 *	Is Numeric value a NaN?
 */
bool numeric_is_nan(Numeric num)
{
    return NUMERIC_IS_NAN(num);
}

/*
 * numeric_maximum_size() -
 *
 *	Maximum size of a numeric with given typmod, or -1 if unlimited/unknown.
 */
int32 numeric_maximum_size(int32 typmod)
{
    int precision;
    int numeric_digits;

    if (typmod < (int32)(VARHDRSZ))
        return -1;

    /* precision (ie, max # of digits) is in upper bits of typmod */
    precision = (int32)((((uint32)(typmod - VARHDRSZ) >> 16)) & 0xffff);

    /*
     * This formula computes the maximum number of NumericDigits we could need
     * in order to store the specified number of decimal digits. Because the
     * weight is stored as a number of NumericDigits rather than a number of
     * decimal digits, it's possible that the first NumericDigit will contain
     * only a single decimal digit.  Thus, the first two decimal digits can
     * require two NumericDigits to store, but it isn't until we reach
     * DEC_DIGITS + 2 decimal digits that we potentially need a third
     * NumericDigit.
     */
    numeric_digits = (precision + 2 * (DEC_DIGITS - 1)) / DEC_DIGITS;

    /*
     * In most cases, the size of a numeric will be smaller than the value
     * computed below, because the varlena header will typically get toasted
     * down to a single byte before being stored on disk, and it may also be
     * possible to use a short numeric header.	But our job here is to compute
     * the worst case.
     */
    return NUMERIC_HDRSZ + (numeric_digits * sizeof(NumericDigit));
}

/*
 * numeric_out_sci() -
 *
 *	Output function for numeric data type in scientific notation.
 */
char* numeric_out_sci(Numeric num, int scale)
{
    NumericVar x;
    char* str = NULL;

    if (NUMERIC_IS_NANORBI(num)) {
        /*
         * Handle Big Integer
         */
        if (NUMERIC_IS_BI(num))
            num = makeNumericNormal(num);
        /*
         * Handle NaN
         */
        else
            return pstrdup("NaN");
    }

    init_var_from_num(num, &x);

    str = get_str_from_var_sci(&x, scale);

    return str;
}

/*
 * numeric_normalize() -
 *
 * Output function for numeric data type without trailing zeroes.
 */
char *numeric_normalize(Numeric num)
{
    NumericVar  x;
    char       *str = NULL;
    int         orig, last;
    /*
     * Handle NaN
     */
    if (NUMERIC_IS_NAN(num)) {
        return pstrdup("NaN");
    }
    init_var_from_num(num, &x);
    str = get_str_from_var(&x);
    orig = last = strlen(str) - 1;

    for (;;) {
        if (last == 0 || str[last] != '0') {
            break;
        }
        last--;
    }
    if (last > 0 && last != orig) {
        str[last] = '\0';
    }
    return str;
}

/*
 *		numeric_recv			- converts external binary format to numeric
 *
 * External format is a sequence of int16's:
 * ndigits, weight, sign, dscale, NumericDigits.
 */
Datum numeric_recv(PG_FUNCTION_ARGS)
{
    StringInfo buf = (StringInfo)PG_GETARG_POINTER(0);

#ifdef NOT_USED
    Oid typelem = PG_GETARG_OID(1);
#endif
    int32 typmod = PG_GETARG_INT32(2);
    NumericVar value;
    Numeric res;
    int len, i;

    init_var(&value);

    len = (uint16)pq_getmsgint(buf, sizeof(uint16));
    if (len < 0 || len > NUMERIC_MAX_PRECISION + NUMERIC_MAX_RESULT_SCALE)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_BINARY_REPRESENTATION), errmsg("invalid length in external \"numeric\" value")));

    init_alloc_var(&value, len);

    value.weight = (int16)pq_getmsgint(buf, sizeof(int16));
    value.sign = (uint16)pq_getmsgint(buf, sizeof(uint16));
    if (!(value.sign == NUMERIC_POS || value.sign == NUMERIC_NEG || value.sign == NUMERIC_NAN))
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_BINARY_REPRESENTATION), errmsg("invalid sign in external \"numeric\" value")));

    value.dscale = (uint16)pq_getmsgint(buf, sizeof(uint16));
    for (i = 0; i < len; i++) {
        NumericDigit d = pq_getmsgint(buf, sizeof(NumericDigit));

        if (d < 0 || d >= NBASE)
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
                    errmsg("invalid digit in external \"numeric\" value")));
        value.digits[i] = d;
    }

    apply_typmod(&value, typmod);

    res = make_result(&value);
    free_var(&value);

    PG_RETURN_NUMERIC(res);
}

/*
 *		numeric_send			- converts numeric to binary format
 */
Datum numeric_send(PG_FUNCTION_ARGS)
{
    Numeric num = PG_GETARG_NUMERIC(0);
    NumericVar x;
    StringInfoData buf;
    int i;
    /*
     * Handle Big Integer
     */
    if (NUMERIC_IS_BI(num))
        num = makeNumericNormal(num);

    init_var_from_num(num, &x);

    pq_begintypsend(&buf);

    pq_sendint16(&buf, x.ndigits);
    pq_sendint16(&buf, x.weight);
    pq_sendint16(&buf, x.sign);
    pq_sendint16(&buf, x.dscale);
    for (i = 0; i < x.ndigits; i++)
        pq_sendint16(&buf, x.digits[i]);

    PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

/*
 * numeric_transform() -
 *
 * Flatten calls to numeric's length coercion function that solely represent
 * increases in allowable precision.  Scale changes mutate every datum, so
 * they are unoptimizable.	Some values, e.g. 1E-1001, can only fit into an
 * unconstrained numeric, so a change from an unconstrained numeric to any
 * constrained numeric is also unoptimizable.
 */
Datum numeric_transform(PG_FUNCTION_ARGS)
{
    FuncExpr* expr = (FuncExpr*)PG_GETARG_POINTER(0);
    Node* ret = NULL;
    Node* typmod = NULL;

    Assert(IsA(expr, FuncExpr));
    Assert(list_length(expr->args) >= 2);

    typmod = (Node*)lsecond(expr->args);

    if (IsA(typmod, Const) && !((Const*)typmod)->constisnull) {
        Node* source = (Node*)linitial(expr->args);
        int32 old_typmod = exprTypmod(source);
        int32 new_typmod = DatumGetInt32(((Const*)typmod)->constvalue);
        int32 old_scale = (int32)(((uint32)(old_typmod - VARHDRSZ)) & 0xffff);
        int32 new_scale = (int32)(((uint32)(new_typmod - VARHDRSZ)) & 0xffff);
        int32 old_precision = (int32)(((uint32)(old_typmod - VARHDRSZ)) >> 16 & 0xffff);
        int32 new_precision = (int32)(((uint32)(new_typmod - VARHDRSZ)) >> 16 & 0xffff);

        /*
         * If new_typmod < VARHDRSZ, the destination is unconstrained; that's
         * always OK.  If old_typmod >= VARHDRSZ, the source is constrained,
         * and we're OK if the scale is unchanged and the precision is not
         * decreasing.	See further notes in function header comment.
         */
        if (new_typmod < (int32)VARHDRSZ ||
            (old_typmod >= (int32)VARHDRSZ && new_scale == old_scale && new_precision >= old_precision))
            ret = relabel_to_typmod(source, new_typmod);
    }

    PG_RETURN_POINTER(ret);
}

/*
 * numeric() -
 *
 *	This is a special function called by the openGauss database system
 *	before a value is stored in a tuple's attribute. The precision and
 *	scale of the attribute have to be applied on the value.
 */
Datum numeric(PG_FUNCTION_ARGS)
{
    Numeric num = PG_GETARG_NUMERIC(0);
    int32 typmod = PG_GETARG_INT32(1);
    Numeric newm;
    int32 tmp_typmod;
    int precision;
    int scale;
    int ddigits;
    int maxdigits;
    NumericVar var;

    if (NUMERIC_IS_NANORBI(num)) {
        /*
         * Handle Big Integer
         */
        if (NUMERIC_IS_BI(num))
            num = makeNumericNormal(num);
        /*
         * Handle NaN
         */
        else
            PG_RETURN_NUMERIC(make_result(&const_nan));
    }

    /*
     * If the value isn't a valid type modifier, simply return a copy of the
     * input value
     */
    if (typmod < (int32)(VARHDRSZ)) {
        newm = (Numeric)palloc(VARSIZE(num));
        errno_t rc = memcpy_s(newm, VARSIZE(num), num, VARSIZE(num));
        securec_check(rc, "\0", "\0");
        PG_RETURN_NUMERIC(newm);
    }

    /*
     * Get the precision and scale out of the typmod value
     */
    tmp_typmod = typmod - VARHDRSZ;
    precision = (tmp_typmod >> 16) & 0xffff;
    scale = tmp_typmod & 0xffff;
    maxdigits = precision - scale;

    /*
     * If the number is certainly in bounds and due to the target scale no
     * rounding could be necessary, just make a copy of the input and modify
     * its scale fields, unless the larger scale forces us to abandon the
     * short representation.  (Note we assume the existing dscale is
     * honest...)
     */
    ddigits = (NUMERIC_WEIGHT(num) + 1) * DEC_DIGITS;
    if (ddigits <= maxdigits && scale >= NUMERIC_DSCALE(num) &&
        (NUMERIC_CAN_BE_SHORT(scale, NUMERIC_WEIGHT(num)) || !NUMERIC_IS_SHORT(num))) {
        newm = (Numeric)palloc(VARSIZE(num));
        errno_t rc = memcpy_s(newm, VARSIZE(num), num, VARSIZE(num));
        securec_check(rc, "\0", "\0");
        if (NUMERIC_IS_SHORT(num))
            newm->choice.n_short.n_header =
                (num->choice.n_short.n_header & ~NUMERIC_SHORT_DSCALE_MASK) | (scale << NUMERIC_SHORT_DSCALE_SHIFT);
        else
            newm->choice.n_long.n_sign_dscale = NUMERIC_SIGN(newm) | ((uint16)scale & NUMERIC_DSCALE_MASK);
        PG_RETURN_NUMERIC(newm);
    }

    /*
     * We really need to fiddle with things - unpack the number into a
     * variable and let apply_typmod() do it.
     */
    init_var(&var);

    set_var_from_num(num, &var);
    apply_typmod(&var, typmod);
    newm = make_result(&var);

    free_var(&var);

    PG_RETURN_NUMERIC(newm);
}

Datum numerictypmodin(PG_FUNCTION_ARGS)
{
    ArrayType* ta = PG_GETARG_ARRAYTYPE_P(0);
    int32* tl = NULL;
    int n;
    int32 typmod;

    tl = ArrayGetIntegerTypmods(ta, &n);

    if (n == 2) {
        if (tl[0] < 1 || tl[0] > NUMERIC_MAX_PRECISION)
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("NUMERIC precision %d must be between 1 and %d", tl[0], NUMERIC_MAX_PRECISION)));
        if (tl[1] < 0 || tl[1] > tl[0])
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("NUMERIC scale %d must be between 0 and precision %d", tl[1], tl[0])));
        typmod = (int32)(((uint32)(tl[0]) << 16) | (uint32)(tl[1])) + VARHDRSZ;
    } else if (n == 1) {
        if (tl[0] < 1 || tl[0] > NUMERIC_MAX_PRECISION)
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("NUMERIC precision %d must be between 1 and %d", tl[0], NUMERIC_MAX_PRECISION)));
        /* scale defaults to zero */
        typmod = (((uint32)tl[0]) << 16) + VARHDRSZ;
    } else {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid NUMERIC type modifier")));
        typmod = 0; /* keep compiler quiet */
    }

    PG_RETURN_INT32(typmod);
}

Datum numerictypmodout(PG_FUNCTION_ARGS)
{
    const size_t len = 64;
    int32 typmod = PG_GETARG_INT32(0);
    char* res = (char*)palloc(len + 1);

    if (typmod >= 0) {
        errno_t ret = snprintf_s(res,
            len + 1,
            len,
            "(%d,%d)",
            (int32)((((uint32)(typmod - VARHDRSZ)) >> 16) & 0xffff),
            (int32)(((uint32)(typmod - VARHDRSZ)) & 0xffff));
        securec_check_ss(ret, "", "");
    } else
        *res = '\0';

    PG_RETURN_CSTRING(res);
}

/* ----------------------------------------------------------------------
 *
 * Sign manipulation, rounding and the like
 *
 * ----------------------------------------------------------------------
 */

Datum numeric_abs(PG_FUNCTION_ARGS)
{
    Numeric num = PG_GETARG_NUMERIC(0);
    Numeric res;
    uint16 numFlags = NUMERIC_NB_FLAGBITS(num);

    if (NUMERIC_FLAG_IS_NANORBI(numFlags)) {
        /*
         * Handle Big Integer
         */
        if (NUMERIC_FLAG_IS_BI(numFlags))
            res = makeNumericNormal(num);
        /*
         * Handle NaN
         */
        else {
            PG_RETURN_NUMERIC(make_result(&const_nan));
        }
    } else {/* Handle original numeric type */
        /*
         * Do it the easy way directly on the packed format
         */
        res = (Numeric)palloc(VARSIZE(num));
        errno_t rc = memcpy_s(res, VARSIZE(num), num, VARSIZE(num));
        securec_check(rc, "\0", "\0");
    }
    if (NUMERIC_IS_SHORT(res)) {
        res->choice.n_short.n_header = res->choice.n_short.n_header & ~NUMERIC_SHORT_SIGN_MASK;
    } else {
        res->choice.n_long.n_sign_dscale = NUMERIC_POS | NUMERIC_DSCALE(res);
    }

    PG_RETURN_NUMERIC(res);
}

Datum numeric_uminus(PG_FUNCTION_ARGS)
{
    Numeric num = PG_GETARG_NUMERIC(0);
    Numeric res;
    uint16 numFlags = NUMERIC_NB_FLAGBITS(num);

    if (NUMERIC_FLAG_IS_NANORBI(numFlags)) {
        /*
         * Handle Big Integer
         */
        if (NUMERIC_FLAG_IS_BI(numFlags)) {
            res = makeNumericNormal(num);
        } else { /* Handle NaN */
            PG_RETURN_NUMERIC(make_result(&const_nan));
        }
    } else { /* Handle original numeric type */
        /*
         * Do it the easy way directly on the packed format
         */
        res = (Numeric)palloc(VARSIZE(num));
        errno_t rc = memcpy_s(res, VARSIZE(num), num, VARSIZE(num));
        securec_check(rc, "\0", "\0");
    }

    /*
     * The packed format is known to be totally zero digit trimmed always. So
     * we can identify a ZERO by the fact that there are no digits at all.	Do
     * nothing to a zero.
     */
    if (NUMERIC_NDIGITS(res) != 0) {
        /* Else, flip the sign */
        if (NUMERIC_IS_SHORT(res)) {
            res->choice.n_short.n_header = res->choice.n_short.n_header ^ NUMERIC_SHORT_SIGN_MASK;
        } else if (NUMERIC_SIGN(res) == NUMERIC_POS) {
            res->choice.n_long.n_sign_dscale = NUMERIC_NEG | NUMERIC_DSCALE(res);
        } else {
            res->choice.n_long.n_sign_dscale = NUMERIC_POS | NUMERIC_DSCALE(res);
        }
    }

    PG_RETURN_NUMERIC(res);
}

Datum numeric_uplus(PG_FUNCTION_ARGS)
{
    Numeric num = PG_GETARG_NUMERIC(0);
    Numeric res;

    res = (Numeric)palloc(VARSIZE(num));
    errno_t rc = memcpy_s(res, VARSIZE(num), num, VARSIZE(num));
    securec_check(rc, "\0", "\0");
    PG_RETURN_NUMERIC(res);
}

/*
 * numeric_sign() -
 *
 * returns -1 if the argument is less than 0, 0 if the argument is equal
 * to 0, and 1 if the argument is greater than zero.
 */
Datum numeric_sign(PG_FUNCTION_ARGS)
{
    Numeric num = PG_GETARG_NUMERIC(0);
    Numeric res;
    NumericVar result;

    if (NUMERIC_IS_NANORBI(num)) {
        /*
         * Handle Big Integer
         */
        if (NUMERIC_IS_BI(num))
            num = makeNumericNormal(num);
        /*
         * Handle NaN
         */
        else
            PG_RETURN_NUMERIC(make_result(&const_nan));
    }

    /*
     * The packed format is known to be totally zero digit trimmed always. So
     * we can identify a ZERO by the fact that there are no digits at all.
     */
    if (NUMERIC_NDIGITS(num) == 0)
        init_ro_var_from_var(&const_zero, &result);
    else {
        /*
         * And if there are some, we return a copy of ONE with the sign of our
         * argument
         */
        init_ro_var_from_var(&const_one, &result);
        result.sign = NUMERIC_SIGN(num);
    }

    res = make_result(&result);
    free_var(&result);

    PG_RETURN_NUMERIC(res);
}

/*
 * numeric_round() -
 *
 *	Round a value to have 'scale' digits after the decimal point.
 *	We allow negative 'scale', implying rounding before the decimal
 *	point --- A db interprets rounding that way.
 */
Datum numeric_round(PG_FUNCTION_ARGS)
{
    Numeric num = PG_GETARG_NUMERIC(0);
    int32 scale = PG_GETARG_INT32(1);
    Numeric res;
    NumericVar arg;

    if (NUMERIC_IS_NANORBI(num)) {
        /*
         * Handle Big Integer
         */
        if (NUMERIC_IS_BI(num))
            num = makeNumericNormal(num);
        /*
         * Handle NaN
         */
        else
            PG_RETURN_NUMERIC(make_result(&const_nan));
    }

    /*
     * Limit the scale value to avoid possible overflow in calculations
     */
    scale = Max(scale, -NUMERIC_MAX_RESULT_SCALE);
    scale = Min(scale, NUMERIC_MAX_RESULT_SCALE);

    /*
     * Unpack the argument and round it at the proper digit position
     */
    init_var(&arg);
    set_var_from_num(num, &arg);

    round_var(&arg, scale);

    /* We don't allow negative output dscale */
    if (scale < 0)
        arg.dscale = 0;

    /*
     * Return the rounded result
     */
    res = make_result(&arg);

    free_var(&arg);
    PG_RETURN_NUMERIC(res);
}

/*
 * numeric_trunc() -
 *
 *	Truncate a value to have 'scale' digits after the decimal point.
 *	We allow negative 'scale', implying a truncation before the decimal
 *	point --- A db interprets truncation that way.
 */
Datum numeric_trunc(PG_FUNCTION_ARGS)
{
    Numeric num = PG_GETARG_NUMERIC(0);
    int32 scale = PG_GETARG_INT32(1);
    Numeric res;
    NumericVar arg;

    if (NUMERIC_IS_NANORBI(num)) {
        /*
         * Handle Big Integer
         */
        if (NUMERIC_IS_BI(num))
            num = makeNumericNormal(num);
        /*
         * Handle NaN
         */
        else
            PG_RETURN_NUMERIC(make_result(&const_nan));
    }

    /*
     * Limit the scale value to avoid possible overflow in calculations
     */
    scale = Max(scale, -NUMERIC_MAX_RESULT_SCALE);
    scale = Min(scale, NUMERIC_MAX_RESULT_SCALE);

    /*
     * Unpack the argument and truncate it at the proper digit position
     */
    init_var(&arg);
    set_var_from_num(num, &arg);

    trunc_var(&arg, scale);

    /* We don't allow negative output dscale */
    if (scale < 0)
        arg.dscale = 0;

    /*
     * Return the truncated result
     */
    res = make_result(&arg);

    free_var(&arg);
    PG_RETURN_NUMERIC(res);
}

/*
 * numeric_ceil() -
 *
 *	Return the smallest integer greater than or equal to the argument
 */
Datum numeric_ceil(PG_FUNCTION_ARGS)
{
    Numeric num = PG_GETARG_NUMERIC(0);
    Numeric res;
    NumericVar result;

    if (NUMERIC_IS_NANORBI(num)) {
        /*
         * Handle Big Integer
         */
        if (NUMERIC_IS_BI(num))
            num = makeNumericNormal(num);
        /*
         * Handle NaN
         */
        else
            PG_RETURN_NUMERIC(make_result(&const_nan));
    }

    init_var_from_num(num, &result);
    ceil_var(&result, &result);

    res = make_result(&result);
    free_var(&result);

    PG_RETURN_NUMERIC(res);
}

/*
 * numeric_floor() -
 *
 *	Return the largest integer equal to or less than the argument
 */
Datum numeric_floor(PG_FUNCTION_ARGS)
{
    Numeric num = PG_GETARG_NUMERIC(0);
    Numeric res;
    NumericVar result;

    if (NUMERIC_IS_NANORBI(num)) {
        /*
         * Handle Big Integer
         */
        if (NUMERIC_IS_BI(num))
            num = makeNumericNormal(num);
        /*
         * Handle NaN
         */
        else
            PG_RETURN_NUMERIC(make_result(&const_nan));
    }

    init_var_from_num(num, &result);
    floor_var(&result, &result);

    res = make_result(&result);
    free_var(&result);

    PG_RETURN_NUMERIC(res);
}
/*
 * generate_series_numeric() -
 *
 *	Generate series of numeric.
 */

Datum generate_series_step_numeric(PG_FUNCTION_ARGS)
{
    generate_series_numeric_fctx* fctx = NULL;
    FuncCallContext* funcctx = NULL;
    MemoryContext oldcontext;

    if (SRF_IS_FIRSTCALL()) {
        Numeric start_num = PG_GETARG_NUMERIC(0);
        Numeric stop_num = PG_GETARG_NUMERIC(1);
        NumericVar steploc = const_one;

        /* handle NaN in start and stop values */
        if (NUMERIC_IS_NAN(start_num))
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("start value cannot be NaN")));

        if (NUMERIC_IS_NAN(stop_num))
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("stop value cannot be NaN")));

        /* see if we were given an explicit step size */
        if (PG_NARGS() == 3) {
            Numeric step_num = PG_GETARG_NUMERIC(2);

            if (NUMERIC_IS_NAN(step_num))
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("step size cannot be NaN")));

            init_var_from_num(step_num, &steploc);

            if (cmp_var(&steploc, &const_zero) == 0)
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("step size cannot equal zero")));
        }

        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        /*
         * Switch to memory context appropriate for multiple function calls.
         */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* allocate memory for user context */
        fctx = (generate_series_numeric_fctx*)palloc(sizeof(generate_series_numeric_fctx));

        /*
         * Use fctx to keep state from call to call. Seed current with the
         * original start value. We must copy the start_num and stop_num
         * values rather than pointing to them, since we may have detoasted
         * them in the per-call context.
         */
        init_var(&fctx->current);
        init_var(&fctx->stop);
        init_var(&fctx->step);

        set_var_from_num(start_num, &fctx->current);
        set_var_from_num(stop_num, &fctx->stop);
        set_var_from_var(&steploc, &fctx->step);

        funcctx->user_fctx = fctx;
        MemoryContextSwitchTo(oldcontext);
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();

    /*
     * Get the saved state and use current state as the result of this
     * iteration.
     */
    fctx = (generate_series_numeric_fctx*)funcctx->user_fctx;

    if ((fctx->step.sign == NUMERIC_POS && cmp_var(&fctx->current, &fctx->stop) <= 0) ||
        (fctx->step.sign == NUMERIC_NEG && cmp_var(&fctx->current, &fctx->stop) >= 0)) {
        Numeric result = make_result(&fctx->current);

        /* switch to memory context appropriate for iteration calculation */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* increment current in preparation for next iteration */
        add_var(&fctx->current, &fctx->step, &fctx->current);
        MemoryContextSwitchTo(oldcontext);

        /* do when there is more left to send */
        SRF_RETURN_NEXT(funcctx, NumericGetDatum(result));
    } else
        /* do when there is no more left */
        SRF_RETURN_DONE(funcctx);
}

Datum generate_series_numeric(PG_FUNCTION_ARGS)
{
    return generate_series_step_numeric(fcinfo);
}

/*
 * Implements the numeric version of the width_bucket() function
 * defined by SQL2003. See also width_bucket_float8().
 *
 * 'bound1' and 'bound2' are the lower and upper bounds of the
 * histogram's range, respectively. 'count' is the number of buckets
 * in the histogram. width_bucket() returns an integer indicating the
 * bucket number that 'operand' belongs to in an equiwidth histogram
 * with the specified characteristics. An operand smaller than the
 * lower bound is assigned to bucket 0. An operand greater than the
 * upper bound is assigned to an additional bucket (with number
 * count+1). We don't allow "NaN" for any of the numeric arguments.
 */
Datum width_bucket_numeric(PG_FUNCTION_ARGS)
{
    Numeric operand = PG_GETARG_NUMERIC(0);
    Numeric bound1 = PG_GETARG_NUMERIC(1);
    Numeric bound2 = PG_GETARG_NUMERIC(2);
    int32 count = PG_GETARG_INT32(3);
    NumericVar count_var;
    NumericVar result_var;
    int32 result;

    if (count <= 0)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_ARGUMENT_FOR_WIDTH_BUCKET_FUNCTION), errmsg("count must be greater than zero")));

    if (NUMERIC_IS_NAN(operand) || NUMERIC_IS_NAN(bound1) || NUMERIC_IS_NAN(bound2))
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_ARGUMENT_FOR_WIDTH_BUCKET_FUNCTION),
                errmsg("operand, lower bound, and upper bound cannot be NaN")));
    /*
     * Handle Big Integer
     */
    if (NUMERIC_IS_BI(operand)) {
        operand = makeNumericNormal(operand);
    }
    if (NUMERIC_IS_BI(bound1)) {
        bound1 = makeNumericNormal(bound1);
    }
    if (NUMERIC_IS_BI(bound2)) {
        bound2 = makeNumericNormal(bound2);
    }

    init_var(&result_var);
    init_var(&count_var);

    /* Convert 'count' to a numeric, for ease of use later */
    int64_to_numericvar((int64)count, &count_var);

    switch (cmp_numerics(bound1, bound2)) {
        case 0:
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_ARGUMENT_FOR_WIDTH_BUCKET_FUNCTION),
                    errmsg("lower bound cannot equal upper bound")));

            /* bound1 < bound2 */
        case -1:
            if (cmp_numerics(operand, bound1) < 0)
                set_var_from_var(&const_zero, &result_var);
            else if (cmp_numerics(operand, bound2) >= 0)
                add_var(&count_var, &const_one, &result_var);
            else
                compute_bucket(operand, bound1, bound2, &count_var, &result_var);
            break;

            /* bound1 > bound2 */
        case 1:
            if (cmp_numerics(operand, bound1) > 0)
                set_var_from_var(&const_zero, &result_var);
            else if (cmp_numerics(operand, bound2) <= 0)
                add_var(&count_var, &const_one, &result_var);
            else
                compute_bucket(operand, bound1, bound2, &count_var, &result_var);
            break;
        default:
            break;
    }

    /* if result exceeds the range of a legal int4, we ereport here */
    result = numericvar_to_int32(&result_var);

    free_var(&count_var);
    free_var(&result_var);

    PG_RETURN_INT32(result);
}

/*
 * If 'operand' is not outside the bucket range, determine the correct
 * bucket for it to go. The calculations performed by this function
 * are derived directly from the SQL2003 spec.
 */
static void compute_bucket(
    Numeric operand, Numeric bound1, Numeric bound2, NumericVar* count_var, NumericVar* result_var)
{
    NumericVar bound1_var;
    NumericVar bound2_var;
    NumericVar operand_var;

    init_var_from_num(bound1, &bound1_var);
    init_var_from_num(bound2, &bound2_var);
    init_var_from_num(operand, &operand_var);

    if (cmp_var(&bound1_var, &bound2_var) < 0) {
        sub_var(&operand_var, &bound1_var, &operand_var);
        sub_var(&bound2_var, &bound1_var, &bound2_var);
        div_var(&operand_var, &bound2_var, result_var, select_div_scale(&operand_var, &bound2_var), true);
    } else {
        sub_var(&bound1_var, &operand_var, &operand_var);
        sub_var(&bound1_var, &bound2_var, &bound1_var);
        div_var(&operand_var, &bound1_var, result_var, select_div_scale(&operand_var, &bound1_var), true);
    }

    mul_var(result_var, count_var, result_var, result_var->dscale + count_var->dscale);
    add_var(result_var, &const_one, result_var);
    floor_var(result_var, result_var);

    free_var(&bound1_var);
    free_var(&bound2_var);
    free_var(&operand_var);
}

/* ----------------------------------------------------------------------
 *
 * Comparison functions
 *
 * Note: btree indexes need these routines not to leak memory; therefore,
 * be careful to free working copies of toasted datums.  Most places don't
 * need to be so careful.
 *
 * Sort support:
 *
 * We implement the sortsupport strategy routine in order to get the benefit of
 * abbreviation. The ordinary numeric comparison can be quite slow as a result
 * of palloc/pfree cycles (due to detoasting packed values for alignment);
 * while this could be worked on itself, the abbreviation strategy gives more
 * speedup in many common cases.
 *
 * Two different representations are used for the abbreviated form, one in
 * int32 and one in int64, whichever fits into a by-value Datum.  In both cases
 * the representation is negated relative to the original value, because we use
 * the largest negative value for NaN, which sorts higher than other values. We
 * convert the absolute value of the numeric to a 31-bit or 63-bit positive
 * value, and then negate it if the original number was positive.
 *
 * We abort the abbreviation process if the abbreviation cardinality is below
 * 0.01% of the row count (1 per 10k non-null rows).  The actual break-even
 * point is somewhat below that, perhaps 1 per 30k (at 1 per 100k there's a
 * very small penalty), but we don't want to build up too many abbreviated
 * values before first testing for abort, so we take the slightly pessimistic
 * number.  We make no attempt to estimate the cardinality of the real values,
 * since it plays no part in the cost model here (if the abbreviation is equal,
 * the cost of comparing equal and unequal underlying values is comparable).
 * We discontinue even checking for abort (saving us the hashing overhead) if
 * the estimated cardinality gets to 100k; that would be enough to support many
 * billions of rows while doing no worse than breaking even.
 *
 * ----------------------------------------------------------------------
 */

/*
 * Sort support strategy routine.
 */
Datum numeric_sortsupport(PG_FUNCTION_ARGS)
{
    SortSupport ssup = (SortSupport)PG_GETARG_POINTER(0);

    ssup->comparator = numeric_fast_cmp;

    if (ssup->abbreviate) {
        NumericSortSupport* nss = NULL;
        MemoryContext oldcontext = MemoryContextSwitchTo(ssup->ssup_cxt);

        nss = (NumericSortSupport*)palloc(sizeof(NumericSortSupport));

        /*
         * palloc a buffer for handling unaligned packed values in addition to
         * the support struct
         */
        nss->buf = palloc(VARATT_SHORT_MAX + VARHDRSZ + 1);

        nss->input_count = 0;
        nss->estimating = true;
        initHyperLogLog(&nss->abbr_card, 10);

        ssup->ssup_extra = nss;

        ssup->abbrev_full_comparator = ssup->comparator;
        ssup->comparator = numeric_cmp_abbrev;
        ssup->abbrev_converter = numeric_abbrev_convert;
        ssup->abbrev_abort = numeric_abbrev_abort;

        MemoryContextSwitchTo(oldcontext);
    }

    PG_RETURN_VOID();
}

/*
 * Abbreviate a numeric datum, handling NaNs and detoasting
 * (must not leak memory!)
 */
static Datum numeric_abbrev_convert(Datum original_datum, SortSupport ssup)
{
    NumericSortSupport* nss = (NumericSortSupport*)ssup->ssup_extra;
    void* original_varatt = PG_DETOAST_DATUM_PACKED(original_datum);
    Numeric value;
    Datum result;
    errno_t rc = EOK;

    nss->input_count += 1;

    /*
     * This is to handle packed datums without needing a palloc/pfree cycle;
     * we keep and reuse a buffer large enough to handle any short datum.
     */
    if (!VARATT_IS_HUGE_TOAST_POINTER(original_varatt) && VARATT_IS_SHORT(original_varatt)) {
        void* buf = nss->buf;
        Size sz = VARSIZE_SHORT(original_varatt) - VARHDRSZ_SHORT;

        Assert(sz <= VARATT_SHORT_MAX - VARHDRSZ_SHORT);

        SET_VARSIZE(buf, VARHDRSZ + sz);
        rc = memcpy_s(VARDATA(buf), VARATT_SHORT_MAX - VARHDRSZ_SHORT, VARDATA_SHORT(original_varatt), sz);
        securec_check(rc, "\0", "\0");

        value = (Numeric)buf;
    } else
        value = (Numeric)original_varatt;

    if (NUMERIC_IS_NAN(value)) {
        result = NUMERIC_ABBREV_NAN;
    } else {
        NumericVar var;
        Numeric tmp_value = NULL;
        /*
         * convert bi64/bi128 to numeric, so we can get the
         * abbrev value of numeric
         */
        if (NUMERIC_IS_BI(value)) {
            tmp_value = makeNumericNormal(value);
            init_var_from_num(tmp_value, &var);

        } else {
            init_var_from_num(value, &var);
        }

        result = numeric_abbrev_convert_var(&var, nss);

        /* tmp_value should free for avoid heap-use-after-free */
        if (tmp_value != NULL) {
            pfree_ext(tmp_value);
            tmp_value = NULL;
        }
    }

    /* should happen only for external/compressed toasts */
    if ((Pointer)original_varatt != DatumGetPointer(original_datum))
        pfree_ext(original_varatt);

    return result;
}

/*
 * Consider whether to abort abbreviation.
 *
 * We pay no attention to the cardinality of the non-abbreviated data. There is
 * no reason to do so: unlike text, we have no fast check for equal values, so
 * we pay the full overhead whenever the abbreviations are equal regardless of
 * whether the underlying values are also equal.
 */
static bool numeric_abbrev_abort(int memtupcount, SortSupport ssup)
{
    NumericSortSupport* nss = (NumericSortSupport*)ssup->ssup_extra;
    double abbr_card;

    if (memtupcount < 10000 || nss->input_count < 10000 || !nss->estimating)
        return false;

    abbr_card = estimateHyperLogLog(&nss->abbr_card);

    /*
     * If we have >100k distinct values, then even if we were sorting many
     * billion rows we'd likely still break even, and the penalty of undoing
     * that many rows of abbrevs would probably not be worth it. Stop even
     * counting at that point.
     */
    if (abbr_card > 100000.0) {
#ifdef TRACE_SORT
        if (u_sess->attr.attr_common.trace_sort)
            elog(LOG,
                "numeric_abbrev: estimation ends at cardinality %f"
                " after " INT64_FORMAT " values (%d rows)",
                abbr_card,
                nss->input_count,
                memtupcount);
#endif
        nss->estimating = false;
        return false;
    }

    /*
     * Target minimum cardinality is 1 per ~10k of non-null inputs.  (The
     * break even point is somewhere between one per 100k rows, where
     * abbreviation has a very slight penalty, and 1 per 10k where it wins by
     * a measurable percentage.)  We use the relatively pessimistic 10k
     * threshold, and add a 0.5 row fudge factor, because it allows us to
     * abort earlier on genuinely pathological data where we've had exactly
     * one abbreviated value in the first 10k (non-null) rows.
     */
    if (abbr_card < nss->input_count / 10000.0 + 0.5) {
#ifdef TRACE_SORT
        if (u_sess->attr.attr_common.trace_sort)
            elog(LOG,
                "numeric_abbrev: aborting abbreviation at cardinality %f"
                " below threshold %f after " INT64_FORMAT " values (%d rows)",
                abbr_card,
                nss->input_count / 10000.0 + 0.5,
                nss->input_count,
                memtupcount);
#endif
        return true;
    }

#ifdef TRACE_SORT
    if (u_sess->attr.attr_common.trace_sort)
        elog(LOG,
            "numeric_abbrev: cardinality %f"
            " after " INT64_FORMAT " values (%d rows)",
            abbr_card,
            nss->input_count,
            memtupcount);
#endif

    return false;
}

/*
 * Non-fmgr interface to the comparison routine to allow sortsupport to elide
 * the fmgr call.  The saving here is small given how slow numeric comparisons
 * are, but it is a required part of the sort support API when abbreviations
 * are performed.
 *
 * Two palloc/pfree cycles could be saved here by using persistent buffers for
 * aligning short-varlena inputs, but this has not so far been considered to
 * be worth the effort.
 *
 * Optimize numeric_fast_cmp of pg9.5, numeric_fast_cmp is only invoked by sort,
 * add fast pre-check for equality and refuce function calls to increase speed.
 */
static int numeric_fast_cmp(Datum x, Datum y, SortSupport ssup)
{
    Numeric nx = DatumGetNumeric(x);
    Numeric ny = DatumGetNumeric(y);
    int result = 0;

    /*
     * We consider all NANs to be equal and larger than any non-NAN. This is
     * somewhat arbitrary; the important thing is to have a consistent sort
     * order.
     */
    if (NUMERIC_IS_NAN(nx)) {
        if (NUMERIC_IS_NAN(ny))
            result = 0; /* NAN = NAN */
        else
            result = 1; /* NAN > non-NAN */
    } else if (NUMERIC_IS_NAN(ny)) {
        result = -1; /* non-NAN < NAN */
    } else {
        /* Get the flags of num1/num2 */
        uint16 num1Flags = NUMERIC_NB_FLAGBITS(nx);
        uint16 num2Flags = NUMERIC_NB_FLAGBITS(ny);

        if (NUMERIC_FLAG_IS_BI(num1Flags) && NUMERIC_FLAG_IS_BI(num2Flags)) {
            /* call biginteger compare function */
            result = DatumGetInt32(bipickfun<BICMP>(nx, ny));
        } else {
            Numeric leftarg = NUMERIC_FLAG_IS_BI(num1Flags) ? makeNumericNormal(nx) : nx;
            Numeric rightarg = NUMERIC_FLAG_IS_BI(num2Flags) ? makeNumericNormal(ny) : ny;

            /* Compare the size between leftarg and rightarg. */
            result = cmp_var_common(NUMERIC_DIGITS(leftarg),
                NUMERIC_NDIGITS(leftarg),
                NUMERIC_WEIGHT(leftarg),
                NUMERIC_SIGN(leftarg),
                NUMERIC_DIGITS(rightarg),
                NUMERIC_NDIGITS(rightarg),
                NUMERIC_WEIGHT(rightarg),
                NUMERIC_SIGN(rightarg));

            /* Free the template malloc space */
            if (leftarg != nx)
                pfree_ext(leftarg);

            if (rightarg != ny)
                pfree_ext(rightarg);
        }
    }
    if (DatumGetPointer(nx) != DatumGetPointer(x))
        pfree_ext(nx);

    if (DatumGetPointer(ny) != DatumGetPointer(y))
        pfree_ext(ny);

    PG_RETURN_INT32(result);
}

/*
 * Compare abbreviations of values. (Abbreviations may be equal where the true
 * values differ, but if the abbreviations differ, they must reflect the
 * ordering of the true values.)
 */
static int numeric_cmp_abbrev(Datum x, Datum y, SortSupport ssup)
{
    /*
     * NOTE WELL: this is intentionally backwards, because the abbreviation is
     * negated relative to the original value, to handle NaN.
     */
    if (DatumGetNumericAbbrev(x) < DatumGetNumericAbbrev(y))
        return 1;
    if (DatumGetNumericAbbrev(x) > DatumGetNumericAbbrev(y))
        return -1;
    return 0;
}

/*
 * Abbreviate a NumericVar according to the available bit size.
 *
 * The 31-bit value is constructed as:
 *
 *	0 + 7bits digit weight + 24 bits digit value
 *
 * where the digit weight is in single decimal digits, not digit words, and
 * stored in excess-44 representation[1]. The 24-bit digit value is the 7 most
 * significant decimal digits of the value converted to binary. Values whose
 * weights would fall outside the representable range are rounded off to zero
 * (which is also used to represent actual zeros) or to 0x7FFFFFFF (which
 * otherwise cannot occur). Abbreviation therefore fails to gain any advantage
 * where values are outside the range 10^-44 to 10^83, which is not considered
 * to be a serious limitation, or when values are of the same magnitude and
 * equal in the first 7 decimal digits, which is considered to be an
 * unavoidable limitation given the available bits. (Stealing three more bits
 * to compare another digit would narrow the range of representable weights by
 * a factor of 8, which starts to look like a real limiting factor.)
 *
 * (The value 44 for the excess is essentially arbitrary)
 *
 * The 63-bit value is constructed as:
 *
 *	0 + 7bits weight + 4 x 14-bit packed digit words
 *
 * The weight in this case is again stored in excess-44, but this time it is
 * the original weight in digit words (i.e. powers of 10000). The first four
 * digit words of the value (if present; trailing zeros are assumed as needed)
 * are packed into 14 bits each to form the rest of the value. Again,
 * out-of-range values are rounded off to 0 or 0x7FFFFFFFFFFFFFFF. The
 * representable range in this case is 10^-176 to 10^332, which is considered
 * to be good enough for all practical purposes, and comparison of 4 words
 * means that at least 13 decimal digits are compared, which is considered to
 * be a reasonable compromise between effectiveness and efficiency in computing
 * the abbreviation.
 *
 * (The value 44 for the excess is even more arbitrary here, it was chosen just
 * to match the value used in the 31-bit case)
 *
 * [1] - Excess-k representation means that the value is offset by adding 'k'
 * and then treated as unsigned, so the smallest representable value is stored
 * with all bits zero. This allows simple comparisons to work on the composite
 * value.
 */

#if NUMERIC_ABBREV_BITS == 64

static Datum numeric_abbrev_convert_var(NumericVar* var, NumericSortSupport* nss)
{
    int ndigits = var->ndigits;
    int weight = var->weight;
    int64 result;

    if (ndigits == 0 || weight < -44) {
        result = 0;
    } else if (weight > 83) {
        result = PG_INT64_MAX;
    } else {
        result = (int64)((uint64)(weight + 44) << 56);

        switch (ndigits) {
            default:
                result = (int64)(((uint64)result) | ((uint64)var->digits[3]));
                /* fall through */
            case 3:
                result = (int64)((uint64)result | (((uint64)var->digits[2]) << 14));
                /* fall through */
            case 2:
                result = (int64)((uint64)result | (((uint64)var->digits[1]) << 28));
                /* fall through */
            case 1:
                result = (int64)((uint64)result | (((uint64)var->digits[0]) << 42));
                break;
        }
    }

    /* the abbrev is negated relative to the original */
    if (var->sign == NUMERIC_POS)
        result = -result;

    if (nss->estimating) {
        uint32 tmp = ((uint32)result ^ (uint32)((uint64)result >> 32));

        addHyperLogLog(&nss->abbr_card, DatumGetUInt32(hash_uint32(tmp)));
    }

    return Int64GetDatum(result);
}

#endif /* NUMERIC_ABBREV_BITS == 64 */

#if NUMERIC_ABBREV_BITS == 32

static Datum numeric_abbrev_convert_var(NumericVar* var, NumericSortSupport* nss)
{
    int ndigits = var->ndigits;
    int weight = var->weight;
    int32 result;

    if (ndigits == 0 || weight < -11) {
        result = 0;
    } else if (weight > 20) {
        result = PG_INT32_MAX;
    } else {
        NumericDigit nxt1 = (ndigits > 1) ? var->digits[1] : 0;

        weight = (weight + 11) * 4;

        result = var->digits[0];

        /*
         * "result" now has 1 to 4 nonzero decimal digits. We pack in more
         * digits to make 7 in total (largest we can fit in 24 bits)
         */

        if (result > 999) {
            /* already have 4 digits, add 3 more */
            result = (result * 1000) + (nxt1 / 10);
            weight += 3;
        } else if (result > 99) {
            /* already have 3 digits, add 4 more */
            result = (result * 10000) + nxt1;
            weight += 2;
        } else if (result > 9) {
            NumericDigit nxt2 = (ndigits > 2) ? var->digits[2] : 0;

            /* already have 2 digits, add 5 more */
            result = (result * 100000) + (nxt1 * 10) + (nxt2 / 1000);
            weight += 1;
        } else {
            NumericDigit nxt2 = (ndigits > 2) ? var->digits[2] : 0;

            /* already have 1 digit, add 6 more */
            result = (result * 1000000) + (nxt1 * 100) + (nxt2 / 100);
        }

        result = result | (weight << 24);
    }

    /* the abbrev is negated relative to the original */
    if (var->sign == NUMERIC_POS)
        result = -result;

    if (nss->estimating) {
        uint32 tmp = (uint32)result;

        addHyperLogLog(&nss->abbr_card, DatumGetUInt32(hash_uint32(tmp)));
    }

    return Int32GetDatum(result);
}

#endif /* NUMERIC_ABBREV_BITS == 32 */

/*
 * Ordinary (non-sortsupport) comparisons follow.
 * Numeric compare function, return -1/0/1.
 */
Datum numeric_cmp(PG_FUNCTION_ARGS)
{
    Numeric num1 = PG_GETARG_NUMERIC(0);
    Numeric num2 = PG_GETARG_NUMERIC(1);
    int result = 0;

    /*
     * We consider all NANs to be equal and larger than any non-NAN. This is
     * somewhat arbitrary; the important thing is to have a consistent sort
     * order.
     */
    if (NUMERIC_IS_NAN(num1)) {
        if (NUMERIC_IS_NAN(num2))
            result = 0; /* NAN = NAN */
        else
            result = 1; /* NAN > non-NAN */
    } else if (NUMERIC_IS_NAN(num2)) {
        result = -1; /* non-NAN < NAN */
    } else {
        /* Get the flags of num1/num2 */
        uint16 num1Flags = NUMERIC_NB_FLAGBITS(num1);
        uint16 num2Flags = NUMERIC_NB_FLAGBITS(num2);

        if (NUMERIC_FLAG_IS_BI(num1Flags) && NUMERIC_FLAG_IS_BI(num2Flags)) {
            /* call biginteger compare function */
            result = DatumGetInt32(bipickfun<BICMP>(num1, num2));
        } else {
            Numeric leftarg = NUMERIC_FLAG_IS_BI(num1Flags) ? makeNumericNormal(num1) : num1;
            Numeric rightarg = NUMERIC_FLAG_IS_BI(num2Flags) ? makeNumericNormal(num2) : num2;

            /* Compare the size between leftarg and rightarg. */
            result = cmp_var_common(NUMERIC_DIGITS(leftarg),
                NUMERIC_NDIGITS(leftarg),
                NUMERIC_WEIGHT(leftarg),
                NUMERIC_SIGN(leftarg),
                NUMERIC_DIGITS(rightarg),
                NUMERIC_NDIGITS(rightarg),
                NUMERIC_WEIGHT(rightarg),
                NUMERIC_SIGN(rightarg));

            /* Free the template malloc space */
            if (leftarg != num1)
                pfree_ext(leftarg);

            if (rightarg != num2)
                pfree_ext(rightarg);
        }
    }
    PG_FREE_IF_COPY(num1, 0);
    PG_FREE_IF_COPY(num2, 1);

    PG_RETURN_INT32(result);
}

/*
 * @Description: Numeric compare function, if num1 equals to num2
 *               then return true, else return false.
 *
 * @IN PG_FUNCTION_ARGS: Numeric data.
 * @return: Datum - the result of (num1 == num2).
 */
Datum numeric_eq(PG_FUNCTION_ARGS)
{
    Numeric num1 = PG_GETARG_NUMERIC(0);
    Numeric num2 = PG_GETARG_NUMERIC(1);
    bool result = false;

    if (NUMERIC_IS_BI(num1) && NUMERIC_IS_BI(num2)) {
        /*call biginteger function*/
        result = DatumGetInt32(bipickfun<BIEQ>(num1, num2));
    } else {
        /* handle NAN in cmp_numerics */
        result = cmp_numerics(num1, num2) == 0;
    }

    PG_FREE_IF_COPY(num1, 0);
    PG_FREE_IF_COPY(num2, 1);

    PG_RETURN_BOOL(result);
}

/*
 * @Description: Numeric compare function, if num1 not equals to num2
 *               then return true, else return false.
 *
 * @IN PG_FUNCTION_ARGS: Numeric data.
 * @return: Datum - the result of (num1 != num2).
 */
Datum numeric_ne(PG_FUNCTION_ARGS)
{
    Numeric num1 = PG_GETARG_NUMERIC(0);
    Numeric num2 = PG_GETARG_NUMERIC(1);
    bool result = false;

    if (NUMERIC_IS_BI(num1) && NUMERIC_IS_BI(num2)) {
        /*call biginteger function*/
        result = DatumGetInt32(bipickfun<BINEQ>(num1, num2));
    } else {
        /* handle NAN in cmp_numerics */
        result = cmp_numerics(num1, num2) != 0;
    }

    PG_FREE_IF_COPY(num1, 0);
    PG_FREE_IF_COPY(num2, 1);

    PG_RETURN_BOOL(result);
}

/*
 * @Description: Numeric compare function, if num1 great than num2
 *               then return true, else return false.
 *
 * @IN PG_FUNCTION_ARGS: Numeric data.
 * @return: Datum - the result of (num1 > num2).
 */
Datum numeric_gt(PG_FUNCTION_ARGS)
{
    Numeric num1 = PG_GETARG_NUMERIC(0);
    Numeric num2 = PG_GETARG_NUMERIC(1);
    bool result = false;

    if (NUMERIC_IS_BI(num1) && NUMERIC_IS_BI(num2)) {
        /*call biginteger function*/
        result = DatumGetInt32(bipickfun<BIGT>(num1, num2));
    } else {
        /* handle NAN in cmp_numerics */
        result = cmp_numerics(num1, num2) > 0;
    }

    PG_FREE_IF_COPY(num1, 0);
    PG_FREE_IF_COPY(num2, 1);

    PG_RETURN_BOOL(result);
}

/*
 * @Description: Numeric compare function, if num1 great than or equal to num2
 *               then return true, else return false.
 *
 * @IN PG_FUNCTION_ARGS: Numeric data.
 * @return: Datum - the result of (num1 >= num2).
 */
Datum numeric_ge(PG_FUNCTION_ARGS)
{
    Numeric num1 = PG_GETARG_NUMERIC(0);
    Numeric num2 = PG_GETARG_NUMERIC(1);
    bool result = false;

    if (NUMERIC_IS_BI(num1) && NUMERIC_IS_BI(num2)) {
        /*call biginteger function*/
        result = DatumGetInt32(bipickfun<BIGE>(num1, num2));
    } else {
        /* handle NAN in cmp_numerics */
        result = cmp_numerics(num1, num2) >= 0;
    }

    PG_FREE_IF_COPY(num1, 0);
    PG_FREE_IF_COPY(num2, 1);

    PG_RETURN_BOOL(result);
}

/*
 * @Description: Numeric compare function, if num1 less than num2
 *               then return true, else return false.
 *
 * @IN PG_FUNCTION_ARGS: Numeric data.
 * @return: Datum - the result of (num1 < num2).
 */
Datum numeric_lt(PG_FUNCTION_ARGS)
{
    Numeric num1 = PG_GETARG_NUMERIC(0);
    Numeric num2 = PG_GETARG_NUMERIC(1);
    bool result = false;

    if (NUMERIC_IS_BI(num1) && NUMERIC_IS_BI(num2)) {
        /*call biginteger function*/
        result = DatumGetInt32(bipickfun<BILT>(num1, num2));
    } else {
        /* handle NAN in cmp_numerics */
        result = cmp_numerics(num1, num2) < 0;
    }

    PG_FREE_IF_COPY(num1, 0);
    PG_FREE_IF_COPY(num2, 1);

    PG_RETURN_BOOL(result);
}

/*
 * @Description: Numeric compare function, if num1 less than or equal to num2
 *               then return true, else return false.
 *
 * @IN PG_FUNCTION_ARGS: Numeric data.
 * @return: Datum - the result of (num1 <= num2).
 */
Datum numeric_le(PG_FUNCTION_ARGS)
{
    Numeric num1 = PG_GETARG_NUMERIC(0);
    Numeric num2 = PG_GETARG_NUMERIC(1);
    bool result = false;

    if (NUMERIC_IS_BI(num1) && NUMERIC_IS_BI(num2)) {
        /*call biginteger function*/
        result = DatumGetInt32(bipickfun<BILE>(num1, num2));
    } else {
        /* handle NAN in cmp_numerics */
        result = cmp_numerics(num1, num2) <= 0;
    }

    PG_FREE_IF_COPY(num1, 0);
    PG_FREE_IF_COPY(num2, 1);

    PG_RETURN_BOOL(result);
}

int cmp_numerics(Numeric num1, Numeric num2)
{
    int result;

    /* compare numeric data, convert big integer to numeric */
    Numeric leftarg = NUMERIC_IS_BI(num1) ? makeNumericNormal(num1) : num1;
    Numeric rightarg = NUMERIC_IS_BI(num2) ? makeNumericNormal(num2) : num2;

    /*
     * We consider all NANs to be equal and larger than any non-NAN. This is
     * somewhat arbitrary; the important thing is to have a consistent sort
     * order.
     */
    if (NUMERIC_IS_NAN(leftarg)) {
        if (NUMERIC_IS_NAN(rightarg))
            result = 0; /* NAN = NAN */
        else
            result = 1; /* NAN > non-NAN */
    } else if (NUMERIC_IS_NAN(rightarg)) {
        result = -1; /* non-NAN < NAN */
    } else {
        result = cmp_var_common(NUMERIC_DIGITS(leftarg),
            NUMERIC_NDIGITS(leftarg),
            NUMERIC_WEIGHT(leftarg),
            NUMERIC_SIGN(leftarg),
            NUMERIC_DIGITS(rightarg),
            NUMERIC_NDIGITS(rightarg),
            NUMERIC_WEIGHT(rightarg),
            NUMERIC_SIGN(rightarg));
    }

    /* Free the template malloc space */
    if (leftarg != num1)
        pfree_ext(leftarg);

    if (rightarg != num2)
        pfree_ext(rightarg);

    return result;
}

Datum hash_numeric(PG_FUNCTION_ARGS)
{
    Numeric key = PG_GETARG_NUMERIC(0);
    Datum digit_hash;
    Datum result;
    int weight;
    int start_offset;
    int end_offset;
    int i;
    int hash_len;
    NumericDigit* digits = NULL;

    /* If it's NaN, don't try to hash the rest of the fields */
    if (NUMERIC_IS_NAN(key))
        PG_RETURN_UINT32(0);

    /*
     * Convert int64/128 to Numeric
     * create function hash_bi() for hash_agg and hash_join
     */
    if (NUMERIC_IS_BI(key))
        key = makeNumericNormal(key);

    weight = NUMERIC_WEIGHT(key);
    start_offset = 0;
    end_offset = 0;

    /*
     * Omit any leading or trailing zeros from the input to the hash. The
     * numeric implementation *should* guarantee that leading and trailing
     * zeros are suppressed, but we're paranoid. Note that we measure the
     * starting and ending offsets in units of NumericDigits, not bytes.
     */
    digits = NUMERIC_DIGITS(key);
    for (i = 0; (unsigned int)(i) < NUMERIC_NDIGITS(key); i++) {
        if (digits[i] != (NumericDigit)0)
            break;

        start_offset++;

        /*
         * The weight is effectively the # of digits before the decimal point,
         * so decrement it for each leading zero we skip.
         */
        weight--;
    }

    /*
     * If there are no non-zero digits, then the value of the number is zero,
     * regardless of any other fields.
     */
    if (NUMERIC_NDIGITS(key) == (unsigned int)(start_offset))
        PG_RETURN_UINT32(-1);

    for (i = NUMERIC_NDIGITS(key) - 1; i >= 0; i--) {
        if (digits[i] != (NumericDigit)0)
            break;

        end_offset++;
    }

    /* If we get here, there should be at least one non-zero digit */
    if ((unsigned int)(start_offset + end_offset) >= NUMERIC_NDIGITS(key)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_ATTRIBUTE), errmsg("there should be at least one non-zero digit.")));
    }

    /*
     * Note that we don't hash on the Numeric's scale, since two numerics can
     * compare equal but have different scales. We also don't hash on the
     * sign, although we could: since a sign difference implies inequality,
     * this shouldn't affect correctness.
     */
    hash_len = NUMERIC_NDIGITS(key) - start_offset - end_offset;
    digit_hash = hash_any((unsigned char*)(NUMERIC_DIGITS(key) + start_offset), hash_len * sizeof(NumericDigit));

    /* Mix in the weight, via XOR */
    result = digit_hash ^ (uint32)weight;

    /* free memory if allocated by the toaster */
    PG_FREE_IF_COPY(key, 0);

    PG_RETURN_DATUM(result);
}

/* ----------------------------------------------------------------------
 *
 * Basic arithmetic functions
 *
 * ----------------------------------------------------------------------
 */

/*
 * numeric_add() -
 *
 *	Add two numerics
 */
Datum numeric_add(PG_FUNCTION_ARGS)
{
    Numeric num1 = PG_GETARG_NUMERIC(0);
    Numeric num2 = PG_GETARG_NUMERIC(1);
    NumericVar arg1;
    NumericVar arg2;
    NumericVar result;
    Numeric res;

    uint16 num1Flags = NUMERIC_NB_FLAGBITS(num1);
    uint16 num2Flags = NUMERIC_NB_FLAGBITS(num2);

    if (NUMERIC_FLAG_IS_NANORBI(num1Flags) || NUMERIC_FLAG_IS_NANORBI(num2Flags)) {
        if (NUMERIC_FLAG_IS_BI(num1Flags) && NUMERIC_FLAG_IS_BI(num2Flags)) {
            // call biginteger function
            return bipickfun<BIADD>(num1, num2);
        } else if (NUMERIC_FLAG_IS_NAN(num1Flags) || NUMERIC_FLAG_IS_NAN(num2Flags)) {
            // handle NAN
            PG_RETURN_NUMERIC(make_result(&const_nan));
        } else if (NUMERIC_FLAG_IS_BI(num1Flags)) {
            // num1 is int64/128, num2 is numeric, turn num1 to numeric
            num1 = makeNumericNormal(num1);
        } else {
            // num1 is numeric, num2 is int64/128, turn num2 to numeric
            num2 = makeNumericNormal(num2);
        }
    }

    /*
     * Unpack the values, let add_var() compute the result and return it.
     */
    init_var_from_num(num1, &arg1);
    init_var_from_num(num2, &arg2);

    quick_init_var(&result);
    add_var(&arg1, &arg2, &result);

    res = make_result(&result);

    free_var(&result);

    PG_RETURN_NUMERIC(res);
}

/*
 * numeric_sub() -
 *
 *	Subtract one numeric from another
 */
Datum numeric_sub(PG_FUNCTION_ARGS)
{
    Numeric num1 = PG_GETARG_NUMERIC(0);
    Numeric num2 = PG_GETARG_NUMERIC(1);
    NumericVar arg1;
    NumericVar arg2;
    NumericVar result;
    Numeric res;

    uint16 num1Flags = NUMERIC_NB_FLAGBITS(num1);
    uint16 num2Flags = NUMERIC_NB_FLAGBITS(num2);

    if (NUMERIC_FLAG_IS_NANORBI(num1Flags) || NUMERIC_FLAG_IS_NANORBI(num2Flags)) {
        if (NUMERIC_FLAG_IS_BI(num1Flags) && NUMERIC_FLAG_IS_BI(num2Flags)) {
            // call biginteger function
            return bipickfun<BISUB>(num1, num2);
        } else if (NUMERIC_FLAG_IS_NAN(num1Flags) || NUMERIC_FLAG_IS_NAN(num2Flags)) {
            // handle NAN
            PG_RETURN_NUMERIC(make_result(&const_nan));
        } else if (NUMERIC_FLAG_IS_BI(num1Flags)) {
            // num1 is int64/128, num2 is numeric, turn num1 to numeric
            num1 = makeNumericNormal(num1);
        } else {
            // num1 is numeric, num2 is int64/128, turn num2 to numeric
            num2 = makeNumericNormal(num2);
        }
    }

    /*
     * Unpack the values, let sub_var() compute the result and return it.
     */
    init_var_from_num(num1, &arg1);
    init_var_from_num(num2, &arg2);

    quick_init_var(&result);
    sub_var(&arg1, &arg2, &result);

    res = make_result(&result);

    free_var(&result);

    PG_RETURN_NUMERIC(res);
}

/*
 * numeric_mul() -
 *
 *	Calculate the product of two numerics
 */
Datum numeric_mul(PG_FUNCTION_ARGS)
{
    Numeric num1 = PG_GETARG_NUMERIC(0);
    Numeric num2 = PG_GETARG_NUMERIC(1);
    NumericVar arg1;
    NumericVar arg2;
    NumericVar result;
    Numeric res;

    uint16 num1Flags = NUMERIC_NB_FLAGBITS(num1);
    uint16 num2Flags = NUMERIC_NB_FLAGBITS(num2);

    if (NUMERIC_FLAG_IS_NANORBI(num1Flags) || NUMERIC_FLAG_IS_NANORBI(num2Flags)) {
        if (NUMERIC_FLAG_IS_BI(num1Flags) && NUMERIC_FLAG_IS_BI(num2Flags)) {
            // call biginteger function
            return bipickfun<BIMUL>(num1, num2);
        } else if (NUMERIC_FLAG_IS_NAN(num1Flags) || NUMERIC_FLAG_IS_NAN(num2Flags)) {
            // handle NAN
            PG_RETURN_NUMERIC(make_result(&const_nan));
        } else if (NUMERIC_FLAG_IS_BI(num1Flags)) {
            // num1 is int64/128, num2 is numeric, turn num1 to numeric
            num1 = makeNumericNormal(num1);
        } else {
            // num1 is numeric, num2 is int64/128, turn num2 to numeric
            num2 = makeNumericNormal(num2);
        }
    }

    /*
     * Unpack the values, let mul_var() compute the result and return it.
     * Unlike add_var() and sub_var(), mul_var() will round its result. In the
     * case of numeric_mul(), which is invoked for the * operator on numerics,
     * we request exact representation for the product (rscale = sum(dscale of
     * arg1, dscale of arg2)).
     */
    init_var_from_num(num1, &arg1);
    init_var_from_num(num2, &arg2);

    init_var(&result);
    mul_var(&arg1, &arg2, &result, arg1.dscale + arg2.dscale);

    res = make_result(&result);

    free_var(&result);

    PG_RETURN_NUMERIC(res);
}

/*
 * numeric_div() -
 *
 *	Divide one numeric into another
 */
Datum numeric_div(PG_FUNCTION_ARGS)
{
    Numeric num1 = PG_GETARG_NUMERIC(0);
    Numeric num2 = PG_GETARG_NUMERIC(1);
    NumericVar arg1;
    NumericVar arg2;
    NumericVar result;
    Numeric res;
    int rscale;

    uint16 num1Flags = NUMERIC_NB_FLAGBITS(num1);
    uint16 num2Flags = NUMERIC_NB_FLAGBITS(num2);

    if (NUMERIC_FLAG_IS_NANORBI(num1Flags) || NUMERIC_FLAG_IS_NANORBI(num2Flags)) {
        if (NUMERIC_FLAG_IS_BI(num1Flags) && NUMERIC_FLAG_IS_BI(num2Flags)) {
            // call biginteger function
            return bipickfun<BIDIV>(num1, num2);
        } else if (NUMERIC_FLAG_IS_NAN(num1Flags) || NUMERIC_FLAG_IS_NAN(num2Flags)) {
            // handle NAN
            PG_RETURN_NUMERIC(make_result(&const_nan));
        } else if (NUMERIC_FLAG_IS_BI(num1Flags)) {
            // num1 is int64/128, num2 is numeric, turn num1 to numeric
            num1 = makeNumericNormal(num1);
        } else {
            // num1 is numeric, num2 is int64/128, turn num2 to numeric
            num2 = makeNumericNormal(num2);
        }
    }

    /*
     * Unpack the arguments
     */
    init_var_from_num(num1, &arg1);
    init_var_from_num(num2, &arg2);

    init_var(&result);

    /*
     * Select scale for division result
     */
    rscale = select_div_scale(&arg1, &arg2);

    /*
     * Do the divide and return the result
     */
    div_var(&arg1, &arg2, &result, rscale, true);

    res = make_result(&result);

    free_var(&result);

    PG_RETURN_NUMERIC(res);
}

/*
 * numeric_div_trunc() -
 *
 *	Divide one numeric into another, truncating the result to an integer
 */
Datum numeric_div_trunc(PG_FUNCTION_ARGS)
{
    Numeric num1 = PG_GETARG_NUMERIC(0);
    Numeric num2 = PG_GETARG_NUMERIC(1);
    NumericVar arg1;
    NumericVar arg2;
    NumericVar result;
    Numeric res;
    uint16 num1Flags = NUMERIC_NB_FLAGBITS(num1);
    uint16 num2Flags = NUMERIC_NB_FLAGBITS(num2);

    if (NUMERIC_FLAG_IS_NANORBI(num1Flags) || NUMERIC_FLAG_IS_NANORBI(num2Flags)) {
        /*
         * Handle NaN
         */
        if (NUMERIC_FLAG_IS_NAN(num1Flags) || NUMERIC_FLAG_IS_NAN(num2Flags)) {
            PG_RETURN_NUMERIC(make_result(&const_nan));
        }
        /*
         * If num1/num2 is int64/int128, turn it to Numeric
         */
        if (NUMERIC_FLAG_IS_BI(num1Flags)) {
            num1 = makeNumericNormal(num1);
        }
        if (NUMERIC_FLAG_IS_BI(num2Flags)) {
            num2 = makeNumericNormal(num2);
        }
    }

    /*
     * Unpack the arguments
     */
    init_var_from_num(num1, &arg1);
    init_var_from_num(num2, &arg2);

    init_var(&result);

    /*
     * Do the divide and return the result
     */
    div_var(&arg1, &arg2, &result, 0, false);

    res = make_result(&result);

    free_var(&result);

    PG_RETURN_NUMERIC(res);
}

/*
 * numeric_mod() -
 *
 *	Calculate the modulo of two numerics
 */
Datum numeric_mod(PG_FUNCTION_ARGS)
{
    Numeric num1 = PG_GETARG_NUMERIC(0);
    Numeric num2 = PG_GETARG_NUMERIC(1);
    Numeric res;
    NumericVar arg1;
    NumericVar arg2;
    NumericVar result;
    uint16 num1Flags = NUMERIC_NB_FLAGBITS(num1);
    uint16 num2Flags = NUMERIC_NB_FLAGBITS(num2);

    if (NUMERIC_FLAG_IS_NANORBI(num1Flags) || NUMERIC_FLAG_IS_NANORBI(num2Flags)) {
        /*
         * Handle NaN
         */
        if (NUMERIC_FLAG_IS_NAN(num1Flags) || NUMERIC_FLAG_IS_NAN(num2Flags)) {
            PG_RETURN_NUMERIC(make_result(&const_nan));
        }

        /*
         * If num1/num2 is int64/int128, turn it to Numeric
         */
        if (NUMERIC_FLAG_IS_BI(num1Flags)) {
            num1 = makeNumericNormal(num1);
        }
        if (NUMERIC_FLAG_IS_BI(num2Flags)) {
            num2 = makeNumericNormal(num2);
        }
    }

    init_var_from_num(num1, &arg1);
    init_var_from_num(num2, &arg2);

    init_var(&result);

    // zero is allowed to be divisor
    if (0 == cmp_var(&arg2, &const_zero)) {
        free_var(&result);
        free_var(&arg2);
        free_var(&arg1);

        if (DB_IS_CMPT(PG_FORMAT)) {
            /* zero is not allowed to be divisor if compatible with PG */
            ereport(ERROR, (errcode(ERRCODE_DIVISION_BY_ZERO), errmsg("division by zero")));

            /* ensure compiler realizes we mustn't reach the division (gcc bug) */
            PG_RETURN_NULL();
        }
        PG_RETURN_NUMERIC(num1);
    }
    mod_var(&arg1, &arg2, &result);

    res = make_result(&result);

    free_var(&result);

    PG_RETURN_NUMERIC(res);
}

/*
 * numeric_inc() -
 *
 *	Increment a number by one
 */
Datum numeric_inc(PG_FUNCTION_ARGS)
{
    Numeric num = PG_GETARG_NUMERIC(0);
    NumericVar arg;
    Numeric res;
    uint16 numFlags = NUMERIC_NB_FLAGBITS(num);

    if (NUMERIC_FLAG_IS_NANORBI(numFlags)) {
        /*
         * Handle Big Integer
         */
        if (NUMERIC_FLAG_IS_BI(numFlags)) {
            num = makeNumericNormal(num);
        } else {
            /*
             * Handle NaN
             */
            PG_RETURN_NUMERIC(make_result(&const_nan));
        }
    }

    /*
     * Compute the result and return it
     */
    init_var_from_num(num, &arg);

    add_var(&arg, &const_one, &arg);

    res = make_result(&arg);

    free_var(&arg);

    PG_RETURN_NUMERIC(res);
}

/*
 * numeric_smaller() -
 *
 *	Return the smaller of two numbers
 */
Datum numeric_smaller(PG_FUNCTION_ARGS)
{
    Numeric num1 = PG_GETARG_NUMERIC(0);
    Numeric num2 = PG_GETARG_NUMERIC(1);

    /*
     * If num1/num2 is int64/int128, turn it to Numeric
     */
    if (NUMERIC_IS_BI(num1))
        num1 = makeNumericNormal(num1);
    if (NUMERIC_IS_BI(num2))
        num2 = makeNumericNormal(num2);

    /*
     * Use cmp_numerics so that this will agree with the comparison operators,
     * particularly as regards comparisons involving NaN.
     */
    if (cmp_numerics(num1, num2) < 0)
        PG_RETURN_NUMERIC(num1);
    else
        PG_RETURN_NUMERIC(num2);
}

/*
 * numeric_larger() -
 *
 *	Return the larger of two numbers
 */
Datum numeric_larger(PG_FUNCTION_ARGS)
{
    Numeric num1 = PG_GETARG_NUMERIC(0);
    Numeric num2 = PG_GETARG_NUMERIC(1);

    /*
     * If num1/num2 is int64/int128, turn it to Numeric
     */
    if (NUMERIC_IS_BI(num1))
        num1 = makeNumericNormal(num1);
    if (NUMERIC_IS_BI(num2))
        num2 = makeNumericNormal(num2);

    /*
     * Use cmp_numerics so that this will agree with the comparison operators,
     * particularly as regards comparisons involving NaN.
     */
    if (cmp_numerics(num1, num2) > 0)
        PG_RETURN_NUMERIC(num1);
    else
        PG_RETURN_NUMERIC(num2);
}

/* ----------------------------------------------------------------------
 *
 * Advanced math functions
 *
 * ----------------------------------------------------------------------
 */

/*
 * numeric_fac()
 *
 * Compute factorial
 */
Datum numeric_fac(PG_FUNCTION_ARGS)
{
    int64 num = PG_GETARG_INT64(0);
    Numeric res;
    NumericVar fact;
    NumericVar result;

    if (num <= 1) {
        res = make_result(&const_one);
        PG_RETURN_NUMERIC(res);
    }
    /* Fail immediately if the result would overflow */
    if (num > 32177)
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("value overflows numeric format")));

    init_var(&fact);
    init_var(&result);

    int64_to_numericvar(num, &result);

    for (num = num - 1; num > 1; num--) {
        /* this loop can take awhile, so allow it to be interrupted */
        CHECK_FOR_INTERRUPTS();

        int64_to_numericvar(num, &fact);

        mul_var(&result, &fact, &result, 0);
    }

    res = make_result(&result);

    free_var(&fact);
    free_var(&result);

    PG_RETURN_NUMERIC(res);
}

/*
 * numeric_sqrt() -
 *
 *	Compute the square root of a numeric.
 */
Datum numeric_sqrt(PG_FUNCTION_ARGS)
{
    Numeric num = PG_GETARG_NUMERIC(0);
    Numeric res;
    NumericVar arg;
    NumericVar result;
    int sweight;
    int rscale;
    uint16 numFlags = NUMERIC_NB_FLAGBITS(num);

    if (NUMERIC_FLAG_IS_NANORBI(numFlags)) {
        /*
         * Handle Big Integer
         */
        if (NUMERIC_FLAG_IS_BI(numFlags)) {
            num = makeNumericNormal(num);
        } else {
            /*
             * Handle NaN
             */
            PG_RETURN_NUMERIC(make_result(&const_nan));
        }
    }

    /*
     * Unpack the argument and determine the result scale.	We choose a scale
     * to give at least NUMERIC_MIN_SIG_DIGITS significant digits; but in any
     * case not less than the input's dscale.
     */
    init_var_from_num(num, &arg);

    quick_init_var(&result);

    /* Assume the input was normalized, so arg.weight is accurate */
    sweight = (arg.weight + 1) * DEC_DIGITS / 2 - 1;

    rscale = NUMERIC_MIN_SIG_DIGITS - sweight;
    rscale = Max(rscale, arg.dscale);
    rscale = Max(rscale, NUMERIC_MIN_DISPLAY_SCALE);
    rscale = Min(rscale, NUMERIC_MAX_DISPLAY_SCALE);

    /*
     * Let sqrt_var() do the calculation and return the result.
     */
    sqrt_var(&arg, &result, rscale);

    res = make_result(&result);

    free_var(&result);

    PG_RETURN_NUMERIC(res);
}

/*
 * numeric_exp() -
 *
 *	Raise e to the power of x
 */
Datum numeric_exp(PG_FUNCTION_ARGS)
{
    Numeric num = PG_GETARG_NUMERIC(0);
    Numeric res;
    NumericVar arg;
    NumericVar result;
    int rscale;
    double val;
    uint16 numFlags = NUMERIC_NB_FLAGBITS(num);

    if (NUMERIC_FLAG_IS_NANORBI(numFlags)) {
        /*
         * Handle Big Integer
         */
        if (NUMERIC_FLAG_IS_BI(numFlags)) {
            num = makeNumericNormal(num);
        } else {
            /*
             * Handle NaN
             */
            PG_RETURN_NUMERIC(make_result(&const_nan));
        }
    }

    /*
     * Unpack the argument and determine the result scale.	We choose a scale
     * to give at least NUMERIC_MIN_SIG_DIGITS significant digits; but in any
     * case not less than the input's dscale.
     */
    init_var_from_num(num, &arg);

    init_var(&result);

    /* convert input to float8, ignoring overflow */
    val = numericvar_to_double_no_overflow(&arg);

    /*
     * log10(result) = num * log10(e), so this is approximately the decimal
     * weight of the result:
     */
    val *= 0.434294481903252;

    /* limit to something that won't cause integer overflow */
    val = Max(val, -NUMERIC_MAX_RESULT_SCALE);
    val = Min(val, NUMERIC_MAX_RESULT_SCALE);

    rscale = NUMERIC_MIN_SIG_DIGITS - (int)val;
    rscale = Max(rscale, arg.dscale);
    rscale = Max(rscale, NUMERIC_MIN_DISPLAY_SCALE);
    rscale = Min(rscale, NUMERIC_MAX_DISPLAY_SCALE);

    /*
     * Let exp_var() do the calculation and return the result.
     */
    exp_var(&arg, &result, rscale);

    res = make_result(&result);

    free_var(&result);

    PG_RETURN_NUMERIC(res);
}

/*
 * numeric_ln() -
 *
 *	Compute the natural logarithm of x
 */
Datum numeric_ln(PG_FUNCTION_ARGS)
{
    Numeric num = PG_GETARG_NUMERIC(0);
    Numeric res;
    NumericVar arg;
    NumericVar result;
    int ln_dweight;
    int rscale;
    uint16 numFlags = NUMERIC_NB_FLAGBITS(num);

    if (NUMERIC_FLAG_IS_NANORBI(numFlags)) {
        /*
         * Handle Big Integer
         */
        if (NUMERIC_FLAG_IS_BI(numFlags)) {
            num = makeNumericNormal(num);
        } else {
            /*
             * Handle NaN
             */
            PG_RETURN_NUMERIC(make_result(&const_nan));
        }
    }

    init_var_from_num(num, &arg);
    init_var(&result);

    /* Estimated dweight of logarithm */
    ln_dweight = estimate_ln_dweight(&arg);

    rscale = NUMERIC_MIN_SIG_DIGITS - ln_dweight;
    rscale = Max(rscale, arg.dscale);
    rscale = Max(rscale, NUMERIC_MIN_DISPLAY_SCALE);
    rscale = Min(rscale, NUMERIC_MAX_DISPLAY_SCALE);

    ln_var(&arg, &result, rscale);

    res = make_result(&result);

    free_var(&result);

    PG_RETURN_NUMERIC(res);
}

/*
 * numeric_log() -
 *
 *	Compute the logarithm of x in a given base
 */
Datum numeric_log(PG_FUNCTION_ARGS)
{
    Numeric num1 = PG_GETARG_NUMERIC(0);
    Numeric num2 = PG_GETARG_NUMERIC(1);
    Numeric res;
    NumericVar arg1;
    NumericVar arg2;
    NumericVar result;
    uint16 num1Flags = NUMERIC_NB_FLAGBITS(num1);
    uint16 num2Flags = NUMERIC_NB_FLAGBITS(num2);

    if (NUMERIC_FLAG_IS_NANORBI(num1Flags) || NUMERIC_FLAG_IS_NANORBI(num2Flags)) {
        /*
         * Handle NaN
         */
        if (NUMERIC_FLAG_IS_NAN(num1Flags) || NUMERIC_FLAG_IS_NAN(num2Flags)) {
            PG_RETURN_NUMERIC(make_result(&const_nan));
        }
        /*
         * If num1/num2 is int64/int128, turn it to Numeric
         */
        if (NUMERIC_FLAG_IS_BI(num1Flags)) {
            num1 = makeNumericNormal(num1);
        }
        if (NUMERIC_FLAG_IS_BI(num2Flags)) {
            num2 = makeNumericNormal(num2);
        }
    }

    /*
     * Initialize things
     */
    init_var_from_num(num1, &arg1);
    init_var_from_num(num2, &arg2);
    init_var(&result);

    /*
     * Call log_var() to compute and return the result; note it handles scale
     * selection itself.
     */
    log_var(&arg1, &arg2, &result);

    res = make_result(&result);

    free_var(&result);

    PG_RETURN_NUMERIC(res);
}

/*
 * numeric_power() -
 *
 *	Raise b to the power of x
 */
Datum numeric_power(PG_FUNCTION_ARGS)
{
    Numeric num1 = PG_GETARG_NUMERIC(0);
    Numeric num2 = PG_GETARG_NUMERIC(1);
    Numeric res;
    NumericVar arg1;
    NumericVar arg2;
    NumericVar arg2_trunc;
    NumericVar result;
    uint16 num1Flags = NUMERIC_NB_FLAGBITS(num1);
    uint16 num2Flags = NUMERIC_NB_FLAGBITS(num2);

    if (NUMERIC_FLAG_IS_NANORBI(num1Flags) || NUMERIC_FLAG_IS_NANORBI(num2Flags)) {
        /*
         * Handle NaN
         */
        if (NUMERIC_FLAG_IS_NAN(num1Flags) || NUMERIC_FLAG_IS_NAN(num2Flags)) {
            PG_RETURN_NUMERIC(make_result(&const_nan));
        }
        /*
         * If num1/num2 is int64/int128, turn it to Numeric
         */
        if (NUMERIC_FLAG_IS_BI(num1Flags)) {
            num1 = makeNumericNormal(num1);
        }
        if (NUMERIC_FLAG_IS_BI(num2Flags)) {
            num2 = makeNumericNormal(num2);
        }
    }

    /*
     * Initialize things
     */
    init_var(&arg2_trunc);
    init_var(&result);
    init_var_from_num(num1, &arg1);
    init_var_from_num(num2, &arg2);

    set_var_from_var(&arg2, &arg2_trunc);

    trunc_var(&arg2_trunc, 0);

    /*
     * The SQL spec requires that we emit a particular SQLSTATE error code for
     * certain error conditions.  Specifically, we don't return a
     * divide-by-zero error code for 0 ^ -1.
     */
    if (cmp_var(&arg1, &const_zero) == 0 && cmp_var(&arg2, &const_zero) < 0)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION),
                errmsg("zero raised to a negative power is undefined")));

    if (cmp_var(&arg1, &const_zero) < 0 && cmp_var(&arg2, &arg2_trunc) != 0)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION),
                errmsg("a negative number raised to a non-integer power yields a complex result")));

    /*
     * Call power_var() to compute and return the result; note it handles
     * scale selection itself.
     */
    power_var(&arg1, &arg2, &result);

    res = make_result(&result);

    free_var(&result);
    free_var(&arg2_trunc);

    PG_RETURN_NUMERIC(res);
}

/* ----------------------------------------------------------------------
 *
 * Type conversion functions
 *
 * ----------------------------------------------------------------------
 */

Datum int4_numeric(PG_FUNCTION_ARGS)
{
    int32 val = PG_GETARG_INT32(0);
    Numeric res;
    NumericVar result;

    init_var(&result);

    int64_to_numericvar((int64)val, &result);

    res = make_result(&result);

    free_var(&result);

    PG_RETURN_NUMERIC(res);
}

Datum numeric_int4(PG_FUNCTION_ARGS)
{
    Numeric num = PG_GETARG_NUMERIC(0);
    NumericVar x;
    int32 result;
    uint16 numFlags = NUMERIC_NB_FLAGBITS(num);

    if (NUMERIC_FLAG_IS_NANORBI(numFlags)) {
        /* Handle Big Integer */
        if (NUMERIC_FLAG_IS_BI(numFlags))
            num = makeNumericNormal(num);
        /* XXX would it be better to return NULL? */
        else
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot convert NaN to integer")));
    }

    /* Convert to variable format, then convert to int4 */
    init_var_from_num(num, &x);
    result = numericvar_to_int32(&x, fcinfo->can_ignore);
    PG_RETURN_INT32(result);
}

/*
 * Given a NumericVar, convert it to an int32. If the NumericVar
 * exceeds the range of an int32, raise the appropriate error via
 * ereport(). The input NumericVar is *not* free'd.
 */
static int32 numericvar_to_int32(const NumericVar* var, bool can_ignore)
{
    int32 result;
    int64 val;

    if (!numericvar_to_int64(var, &val, can_ignore))
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("integer out of range")));

    /* return INT32_MAX/INT32_MIN if SQL can ignore overflowing */
    if (can_ignore && (val > INT_MAX || val < INT_MIN)) {
        ereport(WARNING, (errmsg("integer out of range")));
        return val > INT_MAX ? INT_MAX : INT_MIN;
    }

    /* Down-convert to int4 */
    result = (int32)val;

    /* Test for overflow by reverse-conversion. */
    if ((int64)result != val)
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("integer out of range")));

    return result;
}

Datum int8_numeric(PG_FUNCTION_ARGS)
{
    int64 val = PG_GETARG_INT64(0);
    Numeric res;
    NumericVar result;

    init_var(&result);

    int64_to_numericvar(val, &result);

    res = make_result(&result);

    free_var(&result);

    PG_RETURN_NUMERIC(res);
}

Datum numeric_int8(PG_FUNCTION_ARGS)
{
    Numeric num = PG_GETARG_NUMERIC(0);
    NumericVar x;
    int64 result;
    uint16 numFlags = NUMERIC_NB_FLAGBITS(num);

    if (NUMERIC_FLAG_IS_NANORBI(numFlags)) {
        /* Handle Big Integer */
        if (NUMERIC_FLAG_IS_BI(numFlags))
            num = makeNumericNormal(num);
        /* XXX would it be better to return NULL? */
        else
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot convert NaN to bigint")));
    }

    /* Convert to variable format and thence to int8 */
    init_var_from_num(num, &x);

    if (!numericvar_to_int64(&x, &result, fcinfo->can_ignore))
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("bigint out of range")));

    PG_RETURN_INT64(result);
}

Datum int2_numeric(PG_FUNCTION_ARGS)
{
    int16 val = PG_GETARG_INT16(0);
    Numeric res;
    NumericVar result;

    init_var(&result);

    int64_to_numericvar((int64)val, &result);

    res = make_result(&result);

    free_var(&result);

    PG_RETURN_NUMERIC(res);
}

Datum numeric_int2(PG_FUNCTION_ARGS)
{
    Numeric num = PG_GETARG_NUMERIC(0);
    NumericVar x;
    int64 val;
    int16 result;
    uint16 numFlags = NUMERIC_NB_FLAGBITS(num);

    if (NUMERIC_FLAG_IS_NANORBI(numFlags)) {
        /* Handle Big Integer */
        if (NUMERIC_FLAG_IS_BI(numFlags))
            num = makeNumericNormal(num);
        /* XXX would it be better to return NULL? */
        else
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot convert NaN to smallint")));
    }

    /* Convert to variable format and thence to int8 */
    init_var_from_num(num, &x);

    if (!numericvar_to_int64(&x, &val, fcinfo->can_ignore))
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("smallint out of range")));

    /* return INT16_MAX/INT16_MIN if SQL can ignore overflowing */
    if (fcinfo->can_ignore && (val > SHRT_MAX || val < SHRT_MIN)) {
        ereport(WARNING, (errmsg("smallint out of range")));
        PG_RETURN_INT16(val > SHRT_MAX ? SHRT_MAX : SHRT_MIN);
    }

    /* Down-convert to int2 */
    result = (int16)val;

    /* Test for overflow by reverse-conversion. */
    if ((int64)result != val)
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("smallint out of range")));

    PG_RETURN_INT16(result);
}

// sql compatible : sybase data type tinyint
Datum int1_numeric(PG_FUNCTION_ARGS)
{
    uint8 val = PG_GETARG_UINT8(0);
    Numeric res;
    NumericVar result;

    init_var(&result);

    int64_to_numericvar((int64)val, &result);

    res = make_result(&result);

    free_var(&result);

    PG_RETURN_NUMERIC(res);
}

Datum numeric_int1(PG_FUNCTION_ARGS)
{
    Numeric num = PG_GETARG_NUMERIC(0);
    NumericVar x;
    int64 val;
    uint8 result;
    uint16 numFlags = NUMERIC_NB_FLAGBITS(num);

    if (NUMERIC_FLAG_IS_NANORBI(numFlags)) {
        /* Handle Big Integer */
        if (NUMERIC_FLAG_IS_BI(numFlags))
            num = makeNumericNormal(num);
        /* XXX would it be better to return NULL? */
        else
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot convert NaN to tinyint")));
    }

    /* Convert to variable format and thence to uint8 */
    init_var_from_num(num, &x);

    if (x.sign == NUMERIC_NEG && !fcinfo->can_ignore) {
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("tinyint out of range")));
    }

    if (!numericvar_to_int64(&x, &val, fcinfo->can_ignore))
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("tinyint out of range")));

    /* return UINT8_MAX/UINT8_MIN if SQL can ignore overflowing */
    if (fcinfo->can_ignore && (val > UCHAR_MAX || val < 0)) {
        ereport(WARNING, (errmsg("tinyint out of range")));
        PG_RETURN_UINT8(val > UCHAR_MAX ? UCHAR_MAX : 0);
    }

    /* Down-convert to int1 */
    result = (uint8)val;

    /* Test for overflow by reverse-conversion. */
    if ((int64)result != val)
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("tinyint out of range")));

    PG_RETURN_UINT8(result);
}

Datum float8_numeric(PG_FUNCTION_ARGS)
{
    float8 val = PG_GETARG_FLOAT8(0);
    Numeric res;
    NumericVar result;
    char buf[DBL_DIG + 100];
    errno_t rc;

    if (isnan(val))
        PG_RETURN_NUMERIC(make_result(&const_nan));

    if (isinf(val))
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot convert infinity to numeric")));

    rc = snprintf_s(buf, sizeof(buf), sizeof(buf) - 1, "%.*g", DBL_DIG, val);
    securec_check_ss(rc, "\0", "\0");

    init_var(&result);

    /* Assume we need not worry about leading/trailing spaces */
    (void)set_var_from_str(buf, buf, &result);

    res = make_result(&result);

    free_var(&result);

    PG_RETURN_NUMERIC(res);
}

Datum numeric_float8(PG_FUNCTION_ARGS)
{
    Numeric num = PG_GETARG_NUMERIC(0);
    char* tmp = NULL;
    Datum result;
    uint16 numFlags = NUMERIC_NB_FLAGBITS(num);

    if (NUMERIC_FLAG_IS_NANORBI(numFlags)) {
        /* If num is int64/int128, turn it to Numeric */
        if (NUMERIC_FLAG_IS_BI(numFlags)) {
            num = makeNumericNormal(num);
        } else {
            /* Handle NaN */
            PG_RETURN_FLOAT8(get_float8_nan());
        }
    }

    tmp = DatumGetCString(DirectFunctionCall1(numeric_out_with_zero, NumericGetDatum(num)));

    result = DirectFunctionCall1Coll(float8in, InvalidOid, CStringGetDatum(tmp), fcinfo->can_ignore);

    pfree_ext(tmp);

    PG_RETURN_DATUM(result);
}

/* Convert numeric to float8; if out of range, return +/- HUGE_VAL */
Datum numeric_float8_no_overflow(PG_FUNCTION_ARGS)
{
    Numeric num = PG_GETARG_NUMERIC(0);
    double val;
    uint16 numFlags = NUMERIC_NB_FLAGBITS(num);

    if (NUMERIC_FLAG_IS_NANORBI(numFlags)) {
        /* If num is int64/int128, turn it to Numeric */
        if (NUMERIC_FLAG_IS_BI(numFlags)) {
            num = makeNumericNormal(num);
        } else {
            /* Handle NaN */
            PG_RETURN_FLOAT8(get_float8_nan());
        }
    }

    val = numeric_to_double_no_overflow(num);

    PG_RETURN_FLOAT8(val);
}

Datum float4_numeric(PG_FUNCTION_ARGS)
{
    float4 val = PG_GETARG_FLOAT4(0);
    Numeric res;
    NumericVar result;
    char buf[FLT_DIG + 100];
    errno_t rc;

    if (isnan(val))
        PG_RETURN_NUMERIC(make_result(&const_nan));

    if (isinf(val))
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot convert infinity to numeric")));

    rc = snprintf_s(buf, sizeof(buf), sizeof(buf) - 1, "%.*g", FLT_DIG, val);
    securec_check_ss(rc, "\0", "\0");

    init_var(&result);

    /* Assume we need not worry about leading/trailing spaces */
    (void)set_var_from_str(buf, buf, &result);

    res = make_result(&result);

    free_var(&result);

    PG_RETURN_NUMERIC(res);
}

Datum numeric_float4(PG_FUNCTION_ARGS)
{
    Numeric num = PG_GETARG_NUMERIC(0);
    char* tmp = NULL;
    Datum result;
    uint16 numFlags = NUMERIC_NB_FLAGBITS(num);

    if (NUMERIC_FLAG_IS_NANORBI(numFlags)) {
        /* If num is int64/int128, turn it to Numeric */
        if (NUMERIC_FLAG_IS_BI(numFlags)) {
            num = makeNumericNormal(num);
        } else {
            /* Handle NaN */
            PG_RETURN_FLOAT4(get_float4_nan());
        }
    }

    tmp = DatumGetCString(DirectFunctionCall1(numeric_out_with_zero, NumericGetDatum(num)));

    if (fcinfo->can_ignore) {
        result = DirectFunctionCall1Coll(float4in, InvalidOid, CStringGetDatum(tmp), true);
    } else {
        result = DirectFunctionCall1(float4in, CStringGetDatum(tmp));
    }

    pfree_ext(tmp);

    PG_RETURN_DATUM(result);
}

/* ----------------------------------------------------------------------
 *
 * Aggregate functions
 *
 * The transition datatype for all these aggregates is a 3-element array
 * of Numeric, holding the values N, sum(X), sum(X*X) in that order.
 *
 * We represent N as a numeric mainly to avoid having to build a special
 * datatype; it's unlikely it'd overflow an int4, but ...
 *
 * ----------------------------------------------------------------------
 */

static NumericAggState *makeNumericAggState(FunctionCallInfo fcinfo, bool calcSumX2)
{
    NumericAggState *state;
    MemoryContext agg_context;
    MemoryContext old_context;

    if (!AggCheckCallContext(fcinfo, &agg_context)) {
        elog(ERROR, "aggregate function called in non-aggregate context");
    }

    old_context = MemoryContextSwitchTo(agg_context);

    state = (NumericAggState *) palloc0(sizeof(NumericAggState));
    state->calcSumX2 = calcSumX2;
    state->agg_context = agg_context;

    MemoryContextSwitchTo(old_context);

    return state;
}

static ArrayType* do_numeric_accum(ArrayType* transarray, Numeric newval)
{
    Datum* transdatums = NULL;
    int ndatums;
    Datum N, sumX, sumX2;
    ArrayType* result = NULL;

    /* We assume the input is array of numeric */
    deconstruct_array(transarray, NUMERICOID, -1, false, 'i', &transdatums, NULL, &ndatums);
    if (ndatums != 3)
        ereport(ERROR, (errcode(ERRCODE_ARRAY_ELEMENT_ERROR), errmsg("expected 3-element numeric array")));
    N = transdatums[0];
    sumX = transdatums[1];
    sumX2 = transdatums[2];

    N = DirectFunctionCall1(numeric_inc, N);
    sumX = DirectFunctionCall2(numeric_add, sumX, NumericGetDatum(newval));
    sumX2 = DirectFunctionCall2(
        numeric_add, sumX2, DirectFunctionCall2(numeric_mul, NumericGetDatum(newval), NumericGetDatum(newval)));

    transdatums[0] = N;
    transdatums[1] = sumX;
    transdatums[2] = sumX2;

    result = construct_array(transdatums, 3, NUMERICOID, -1, false, 'i');

    return result;
}

static void do_numeric_accum_numeric(NumericAggState *state, Numeric newval)
{
    NumericVar  X;
    NumericVar  X2;
    MemoryContext old_context;
    uint16 num1Flags = NUMERIC_NB_FLAGBITS(newval);
    /* result is NaN if any processed number is NaN */
    if (state->isNaN || NUMERIC_FLAG_IS_NAN(num1Flags)) {
        state->isNaN = true;
        return;
    }

    if (NUMERIC_FLAG_IS_BI(num1Flags)) {
        // num1 is int64/128, num2 is numeric, turn num1 to numeric
        newval = makeNumericNormal(newval);
    }

    /* load processed number in short-lived context */
    init_var_from_num(newval, &X);

    /* if we need X^2, calculate that in short-lived context */
    if (state->calcSumX2) {
        init_var(&X2);
        mul_var(&X, &X, &X2, X.dscale * 2);
    }

    /* The rest of this needs to work in the aggregate context */
    old_context = MemoryContextSwitchTo(state->agg_context);

    state->N++;

    /* Accumulate sums */
    accum_sum_add(&(state->sumX), &X);

    if (state->calcSumX2)
        accum_sum_add(&(state->sumX2), &X2);
    MemoryContextSwitchTo(old_context);
}

/*
 * Improve avg performance by not caclulating sum(X*X).
 */
static ArrayType* do_numeric_avg_accum(ArrayType* transarray, Numeric newval)
{
    Datum* transdatums = NULL;
    int ndatums;
    Datum N, sumX;
    ArrayType* result = NULL;

    /* We assume the input is array of numeric */
    deconstruct_array(transarray, NUMERICOID, -1, false, 'i', &transdatums, NULL, &ndatums);
    if (ndatums != 2)
        ereport(ERROR, (errcode(ERRCODE_ARRAY_ELEMENT_ERROR), errmsg("expected 2-element numeric array")));
    N = transdatums[0];
    sumX = transdatums[1];

    N = DirectFunctionCall1(numeric_inc, N);
    sumX = DirectFunctionCall2(numeric_add, sumX, NumericGetDatum(newval));

    transdatums[0] = N;
    transdatums[1] = sumX;

    result = construct_array(transdatums, 2, NUMERICOID, -1, false, 'i');

    return result;
}

Datum numeric_accum(PG_FUNCTION_ARGS)
{
    ArrayType* transarray = PG_GETARG_ARRAYTYPE_P(0);
    Numeric newval = PG_GETARG_NUMERIC(1);

    PG_RETURN_ARRAYTYPE_P(do_numeric_accum(transarray, newval));
}

Datum numeric_accum_numeric(PG_FUNCTION_ARGS)
{
    NumericAggState *state;

    state = PG_ARGISNULL(0) ? NULL : (NumericAggState *) PG_GETARG_POINTER(0);

    if (!PG_ARGISNULL(1)) {
        /* Create the state data when we see the first non-null input. */
        if (state == NULL)
            state = makeNumericAggState(fcinfo, true);

        do_numeric_accum_numeric(state, PG_GETARG_NUMERIC(1));
    }

    PG_RETURN_POINTER(state);
}

/*
 * Optimized case for average of numeric.
 */
Datum numeric_avg_accum(PG_FUNCTION_ARGS)
{
    ArrayType* transarray = PG_GETARG_ARRAYTYPE_P(0);
    Numeric newval = PG_GETARG_NUMERIC(1);

    PG_RETURN_ARRAYTYPE_P(do_numeric_avg_accum(transarray, newval));
}

Datum numeric_avg_accum_numeric(PG_FUNCTION_ARGS)
{
    NumericAggState *state;
    state = PG_ARGISNULL(0) ? NULL : (NumericAggState *) PG_GETARG_POINTER(0);
 
    if (!PG_ARGISNULL(1)) {
        /* Create the state data when we see the first non-null input. */
        if (state == NULL) {
            state = makeNumericAggState(fcinfo, false);
        }

        do_numeric_accum_numeric(state, PG_GETARG_NUMERIC(1));
    }

    PG_RETURN_POINTER(state);
}

/*
 * Integer data types all use Numeric accumulators to share code and
 * avoid risk of overflow.	For int2 and int4 inputs, Numeric accumulation
 * is overkill for the N and sum(X) values, but definitely not overkill
 * for the sum(X*X) value.	Hence, we use int2_accum and int4_accum only
 * for stddev/variance --- there are faster special-purpose accumulator
 * routines for SUM and AVG of these datatypes.
 */

Datum int2_accum(PG_FUNCTION_ARGS)
{
    ArrayType* transarray = PG_GETARG_ARRAYTYPE_P(0);
    Datum newval2 = PG_GETARG_DATUM(1);
    Numeric newval;

    newval = DatumGetNumeric(DirectFunctionCall1(int2_numeric, newval2));

    PG_RETURN_ARRAYTYPE_P(do_numeric_accum(transarray, newval));
}

Datum int4_accum(PG_FUNCTION_ARGS)
{
    ArrayType* transarray = PG_GETARG_ARRAYTYPE_P(0);
    Datum newval4 = PG_GETARG_DATUM(1);
    Numeric newval;

    newval = DatumGetNumeric(DirectFunctionCall1(int4_numeric, newval4));

    PG_RETURN_ARRAYTYPE_P(do_numeric_accum(transarray, newval));
}

Datum int8_accum(PG_FUNCTION_ARGS)
{
    ArrayType* transarray = PG_GETARG_ARRAYTYPE_P(0);
    Datum newval8 = PG_GETARG_DATUM(1);
    Numeric newval;

    newval = DatumGetNumeric(DirectFunctionCall1(int8_numeric, newval8));

    PG_RETURN_ARRAYTYPE_P(do_numeric_accum(transarray, newval));
}

/*
 * Optimized case for average of int8.
 */
Datum int8_avg_accum(PG_FUNCTION_ARGS)
{
    ArrayType* transarray = PG_GETARG_ARRAYTYPE_P(0);
    Datum newval8 = PG_GETARG_DATUM(1);
    Numeric newval;

    newval = DatumGetNumeric(DirectFunctionCall1(int8_numeric, newval8));

    PG_RETURN_ARRAYTYPE_P(do_numeric_avg_accum(transarray, newval));
}

Datum int8_avg_accum_numeric(PG_FUNCTION_ARGS)
{
    NumericAggState *state;

    state = PG_ARGISNULL(0) ? NULL : (NumericAggState *) PG_GETARG_POINTER(0);

    if (!PG_ARGISNULL(1)) {
        Numeric     newval;

        newval = DatumGetNumeric(DirectFunctionCall1(int8_numeric,
                                                     PG_GETARG_DATUM(1)));
        /* Create the state data when we see the first non-null input. */
        if (state == NULL) {
            state = makeNumericAggState(fcinfo, true);
        }
        do_numeric_accum_numeric(state, newval);
    }

    PG_RETURN_POINTER(state);

}

Datum numeric_avg(PG_FUNCTION_ARGS)
{
    ArrayType* transarray = PG_GETARG_ARRAYTYPE_P(0);
    Datum* transdatums = NULL;
    int ndatums;
    Numeric N, sumX;

    /* We assume the input is array of numeric */
    deconstruct_array(transarray, NUMERICOID, -1, false, 'i', &transdatums, NULL, &ndatums);
    if (ndatums != 2)
        ereport(ERROR, (errcode(ERRCODE_ARRAY_ELEMENT_ERROR), errmsg("expected 2-element numeric array")));
    N = DatumGetNumeric(transdatums[0]);
    sumX = DatumGetNumeric(transdatums[1]);

    /* SQL92 defines AVG of no values to be NULL */
    /* N is zero iff no digits (cf. numeric_uminus) */
    if (NUMERIC_NDIGITS(N) == 0)
        PG_RETURN_NULL();

    PG_RETURN_DATUM(DirectFunctionCall2(numeric_div, NumericGetDatum(sumX), NumericGetDatum(N)));
}

Datum numeric_avg_numeric(PG_FUNCTION_ARGS)
{
    NumericAggState *state;
    Datum       N_datum;
    Datum       sumX_datum;
    NumericVar	sumX_var;
    state = PG_ARGISNULL(0) ? NULL : (NumericAggState *) PG_GETARG_POINTER(0);
    if (state == NULL) {
        PG_RETURN_NULL();
    }
    if (state->isNaN) {
        PG_RETURN_NUMERIC(make_result(&const_nan));
    }

    N_datum = DirectFunctionCall1(int8_numeric, Int64GetDatum(state->N));
    init_var(&sumX_var);
    accum_sum_final(&state->sumX, &sumX_var);
    sumX_datum = NumericGetDatum(make_result(&sumX_var));
    free_var(&sumX_var);
    PG_RETURN_DATUM(DirectFunctionCall2(numeric_div, sumX_datum, N_datum));
}

Datum numeric_sum(PG_FUNCTION_ARGS)
{
    NumericAggState *state;
    NumericVar	sumX_var;
    Numeric result;
    state = PG_ARGISNULL(0) ? NULL : (NumericAggState *) PG_GETARG_POINTER(0);
    if (state == NULL) {
        PG_RETURN_NULL();
    }

    if (state->isNaN) {
        PG_RETURN_NUMERIC(make_result(&const_nan));
    }

    init_var(&sumX_var);
    accum_sum_final(&state->sumX, &sumX_var);
    result = make_result(&sumX_var);
    free_var(&sumX_var);

    PG_RETURN_NUMERIC(result);
}

static void int8_to_numericvar(int64 val, NumericVar *var)
{
    uint64      uval, newuval;
    NumericDigit *ptr;
    int         ndigits;

    /* int8 can require at most 19 decimal digits; add one for safety */
    alloc_var(var, 20 / DEC_DIGITS);
    if (val < 0) {
        var->sign = NUMERIC_NEG;
        uval = -val;
    } else {
        var->sign = NUMERIC_POS;
        uval = val;
    }
    var->dscale = 0;
    if (val == 0) {
        var->ndigits = 0;
        var->weight = 0;
        return;
    }
    ptr = var->digits + var->ndigits;
    ndigits = 0;
    do {
        ptr--;
        ndigits++;
        newuval = uval / NBASE;
        *ptr = uval - newuval * NBASE;
        uval = newuval;
    } while (uval);
    var->digits = ptr;
    var->ndigits = ndigits;
    var->weight = ndigits - 1;
}

/*
 * Workhorse routine for the standard deviance and variance
 * aggregates. 'transarray' is the aggregate's transition
 * array. 'variance' specifies whether we should calculate the
 * variance or the standard deviation. 'sample' indicates whether the
 * caller is interested in the sample or the population
 * variance/stddev.
 *
 * If appropriate variance statistic is undefined for the input,
 * *is_null is set to true and NULL is returned.
 */
static Numeric numeric_stddev_internal(ArrayType* transarray, bool variance, bool sample, bool* is_null)
{
    Datum* transdatums = NULL;
    int ndatums;
    Numeric N, sumX, sumX2, res;
    NumericVar vN, vsumX, vsumX2, vNminus1;
    NumericVar* comp = NULL;
    int rscale;

    *is_null = false;

    /* We assume the input is array of numeric */
    deconstruct_array(transarray, NUMERICOID, -1, false, 'i', &transdatums, NULL, &ndatums);
    if (ndatums != 3)
        ereport(ERROR, (errcode(ERRCODE_ARRAY_ELEMENT_ERROR), errmsg("expected 3-element numeric array")));
    N = DatumGetNumeric(transdatums[0]);
    sumX = DatumGetNumeric(transdatums[1]);
    sumX2 = DatumGetNumeric(transdatums[2]);

    if (NUMERIC_IS_NAN(N) || NUMERIC_IS_NAN(sumX) || NUMERIC_IS_NAN(sumX2)) {
        return make_result(&const_nan);
    }

    /*
     * Handle Big Integer
     */
    if (NUMERIC_IS_BI(N)) {
        N = makeNumericNormal(N);
    }
    init_var_from_num(N, &vN);

    /*
     * Sample stddev and variance are undefined when N <= 1; population stddev
     * is undefined when N == 0. Return NULL in either case.
     */
    if (sample)
        comp = &const_one;
    else
        comp = &const_zero;

    if (cmp_var(&vN, comp) <= 0) {
        *is_null = true;
        return NULL;
    }

    init_var(&vNminus1);
    sub_var(&vN, &const_one, &vNminus1);

    /*
     * Handle Big Integer
     */
    if (NUMERIC_IS_BI(sumX))
        sumX = makeNumericNormal(sumX);
    if (NUMERIC_IS_BI(sumX2))
        sumX2 = makeNumericNormal(sumX2);
    init_var_from_num(sumX, &vsumX);
    init_var_from_num(sumX2, &vsumX2);

    /* compute rscale for mul_var calls */
    rscale = vsumX.dscale * 2;

    mul_var(&vsumX, &vsumX, &vsumX, rscale); /* vsumX = sumX * sumX */
    mul_var(&vN, &vsumX2, &vsumX2, rscale);  /* vsumX2 = N * sumX2 */
    sub_var(&vsumX2, &vsumX, &vsumX2);       /* N * sumX2 - sumX * sumX */

    if (cmp_var(&vsumX2, &const_zero) <= 0) {
        /* Watch out for roundoff error producing a negative numerator */
        res = make_result(&const_zero);
    } else {
        if (sample)
            mul_var(&vN, &vNminus1, &vNminus1, 0); /* N * (N - 1) */
        else
            mul_var(&vN, &vN, &vNminus1, 0); /* N * N */
        rscale = select_div_scale(&vsumX2, &vNminus1);
        div_var(&vsumX2, &vNminus1, &vsumX, rscale, true); /* variance */
        if (!variance)
            sqrt_var(&vsumX, &vsumX, rscale); /* stddev */

        res = make_result(&vsumX);
    }

    free_var(&vNminus1);
    free_var(&vsumX);
    free_var(&vsumX2);

    return res;
}

static Numeric numeric_stddev_internal_numeric(NumericAggState* state, bool variance, bool sample, bool* is_null)
{
    Numeric     res;
    NumericVar vN, vsumX, vsumX2, vNminus1;
    NumericVar* comp = NULL;
    int rscale;

    /* Deal with empty input and NaN-input cases */
    if (state == NULL) {
        *is_null = true;
        return NULL;
    }

    *is_null = false;

    if (state->isNaN) {
        return make_result(&const_nan);
    }

    init_var(&vN);
    init_var(&vsumX);
    init_var(&vsumX2);
 
    int8_to_numericvar(state->N, &vN);
    /*
     * Sample stddev and variance are undefined when N <= 1; population stddev
     * is undefined when N == 0. Return NULL in either case.
     */
    if (sample) {
        comp = &const_one;
    } else {
        comp = &const_zero;
    }

    if (cmp_var(&vN, comp) <= 0) {
        *is_null = true;
        return NULL;
    }

    init_var(&vNminus1);
    sub_var(&vN, &const_one, &vNminus1);

    /*
     * Handle Big Integer
     */
    accum_sum_final(&(state->sumX), &vsumX);
    accum_sum_final(&(state->sumX2), &vsumX2);

    /* compute rscale for mul_var calls */
    rscale = vsumX.dscale * 2;

    mul_var(&vsumX, &vsumX, &vsumX, rscale); /* vsumX = sumX * sumX */
    mul_var(&vN, &vsumX2, &vsumX2, rscale);  /* vsumX2 = N * sumX2 */
    sub_var(&vsumX2, &vsumX, &vsumX2);       /* N * sumX2 - sumX * sumX */

    if (cmp_var(&vsumX2, &const_zero) <= 0) {
        /* Watch out for roundoff error producing a negative numerator */
        res = make_result(&const_zero);
    } else {
        if (sample) {
            mul_var(&vN, &vNminus1, &vNminus1, 0); /* N * (N - 1) */
        } else {
            mul_var(&vN, &vN, &vNminus1, 0); /* N * N */
        }
        rscale = select_div_scale(&vsumX2, &vNminus1);
        div_var(&vsumX2, &vNminus1, &vsumX, rscale, true); /* variance */
        if (!variance) {
            sqrt_var(&vsumX, &vsumX, rscale); /* stddev */
        }

        res = make_result(&vsumX);
    }

    free_var(&vNminus1);
    free_var(&vsumX);
    free_var(&vsumX2);

    return res;
}

Datum numeric_var_samp(PG_FUNCTION_ARGS)
{
    Numeric res;
    bool is_null = false;

    res = numeric_stddev_internal(PG_GETARG_ARRAYTYPE_P(0), true, true, &is_null);

    if (is_null)
        PG_RETURN_NULL();
    else
        PG_RETURN_NUMERIC(res);
}

Datum numeric_var_samp_numeric(PG_FUNCTION_ARGS)
{
    NumericAggState *state;
    Numeric res;
    bool is_null = false;

    state = PG_ARGISNULL(0) ? NULL : (NumericAggState *) PG_GETARG_POINTER(0);

    res = numeric_stddev_internal_numeric(state, true, true, &is_null);

    if (is_null) {
        PG_RETURN_NULL();
    } else {
        PG_RETURN_NUMERIC(res);
    }
}

Datum numeric_stddev_samp(PG_FUNCTION_ARGS)
{
    Numeric res;
    bool is_null = false;

    res = numeric_stddev_internal(PG_GETARG_ARRAYTYPE_P(0), false, true, &is_null);

    if (is_null)
        PG_RETURN_NULL();
    else
        PG_RETURN_NUMERIC(res);
}

void stddev_create_state_4_vector(PG_FUNCTION_ARGS)
{
    NumericAggState *state = makeNumericAggState(fcinfo, true);
    state->N = DatumGetInt64(DirectFunctionCall1(numeric_int8, PG_GETARG_DATUM(1)));
    NumericVar* sumX = (NumericVar*)palloc0(sizeof(NumericVar));
    NumericVar* sumX2 = (NumericVar*)palloc0(sizeof(NumericVar));
    init_var_from_num(DatumGetNumeric(PG_GETARG_DATUM(2)), sumX);
    init_var_from_num(DatumGetNumeric(PG_GETARG_DATUM(3)), sumX2);
    accum_sum_add(&(state->sumX), sumX);
    accum_sum_add(&(state->sumX2), sumX2);

    fcinfo->arg[0] = PointerGetDatum(state);
    return ;
}

Datum numeric_stddev_samp_numeric(PG_FUNCTION_ARGS)
{
    NumericAggState *state;
    Numeric res;
    bool is_null = false;

    state = (fcinfo->isnull && PG_ARGISNULL(0)) ? NULL : (NumericAggState *) PG_GETARG_POINTER(0);

    res = numeric_stddev_internal_numeric(state, false, true, &is_null);

    if (is_null) {
        PG_RETURN_NULL();
    } else {
        PG_RETURN_NUMERIC(res);
    }
}

Datum numeric_var_pop(PG_FUNCTION_ARGS)
{
    Numeric res;
    bool is_null = false;

    res = numeric_stddev_internal(PG_GETARG_ARRAYTYPE_P(0), true, false, &is_null);

    if (is_null)
        PG_RETURN_NULL();
    else
        PG_RETURN_NUMERIC(res);
}

Datum numeric_var_pop_numeric(PG_FUNCTION_ARGS)
{
    NumericAggState *state;
    Numeric res;
    bool is_null = false;

    state = PG_ARGISNULL(0) ? NULL : (NumericAggState *) PG_GETARG_POINTER(0);

    res = numeric_stddev_internal_numeric(state, true, false, &is_null);

    if (is_null) {
        PG_RETURN_NULL();
    } else {
        PG_RETURN_NUMERIC(res);
    }
}

Datum numeric_stddev_pop(PG_FUNCTION_ARGS)
{
    Numeric res;
    bool is_null = false;

    res = numeric_stddev_internal(PG_GETARG_ARRAYTYPE_P(0), false, false, &is_null);

    if (is_null)
        PG_RETURN_NULL();
    else
        PG_RETURN_NUMERIC(res);
}

Datum numeric_stddev_pop_numeric(PG_FUNCTION_ARGS)
{
    NumericAggState *state;
    Numeric res;
    bool is_null = false;

    state = PG_ARGISNULL(0) ? NULL : (NumericAggState *) PG_GETARG_POINTER(0);

    res = numeric_stddev_internal_numeric(state, false, false, &is_null);

    if (is_null) {
        PG_RETURN_NULL();
    } else {
        PG_RETURN_NUMERIC(res);
    }
}

/*
 * SUM transition functions for integer datatypes.
 *
 * To avoid overflow, we use accumulators wider than the input datatype.
 * A Numeric accumulator is needed for int8 input; for int4 and int2
 * inputs, we use int8 accumulators which should be sufficient for practical
 * purposes.  (The latter two therefore don't really belong in this file,
 * but we keep them here anyway.)
 *
 * Because SQL92 defines the SUM() of no values to be NULL, not zero,
 * the initial condition of the transition data value needs to be NULL. This
 * means we can't rely on ExecAgg to automatically insert the first non-null
 * data value into the transition data: it doesn't know how to do the type
 * conversion.	The upshot is that these routines have to be marked non-strict
 * and handle substitution of the first non-null input themselves.
 */

Datum int2_sum(PG_FUNCTION_ARGS)
{
    int64 newval;

    if (PG_ARGISNULL(0)) {
        /* No non-null input seen so far... */
        if (PG_ARGISNULL(1))
            PG_RETURN_NULL(); /* still no non-null */
        /* This is the first non-null input. */
        newval = (int64)PG_GETARG_INT16(1);
        PG_RETURN_INT64(newval);
    }

    /*
     * If we're invoked as an aggregate, we can cheat and modify our first
     * parameter in-place to avoid palloc overhead. If not, we need to return
     * the new value of the transition variable. (If int8 is pass-by-value,
     * then of course this is useless as well as incorrect, so just ifdef it
     * out.)
     */
#ifndef USE_FLOAT8_BYVAL /* controls int8 too */
    if (AggCheckCallContext(fcinfo, NULL)) {
        int64* oldsum = (int64*)PG_GETARG_POINTER(0);

        /* Leave the running sum unchanged in the new input is null */
        if (!PG_ARGISNULL(1))
            *oldsum = *oldsum + (int64)PG_GETARG_INT16(1);

        PG_RETURN_POINTER(oldsum);
    } else
#endif
    {
        int64 oldsum = PG_GETARG_INT64(0);

        /* Leave sum unchanged if new input is null. */
        if (PG_ARGISNULL(1)) {
            PG_RETURN_INT64(oldsum);
        }

        /* OK to do the addition. */
        newval = oldsum + (int64)PG_GETARG_INT16(1);

        PG_RETURN_INT64(newval);
    }
}

Datum int4_sum(PG_FUNCTION_ARGS)
{
    int64 newval;

    if (PG_ARGISNULL(0)) {
        /* No non-null input seen so far... */
        if (PG_ARGISNULL(1))
            PG_RETURN_NULL(); /* still no non-null */
        /* This is the first non-null input. */
        newval = (int64)PG_GETARG_INT32(1);
        PG_RETURN_INT64(newval);
    }

    /*
     * If we're invoked as an aggregate, we can cheat and modify our first
     * parameter in-place to avoid palloc overhead. If not, we need to return
     * the new value of the transition variable. (If int8 is pass-by-value,
     * then of course this is useless as well as incorrect, so just ifdef it
     * out.)
     */
#ifndef USE_FLOAT8_BYVAL /* controls int8 too */
    if (AggCheckCallContext(fcinfo, NULL)) {
        int64* oldsum = (int64*)PG_GETARG_POINTER(0);

        /* Leave the running sum unchanged in the new input is null */
        if (!PG_ARGISNULL(1))
            *oldsum = *oldsum + (int64)PG_GETARG_INT32(1);

        PG_RETURN_POINTER(oldsum);
    } else
#endif
    {
        int64 oldsum = PG_GETARG_INT64(0);

        /* Leave sum unchanged if new input is null. */
        if (PG_ARGISNULL(1)) {
            PG_RETURN_INT64(oldsum);
        }

        /* OK to do the addition. */
        newval = oldsum + (int64)PG_GETARG_INT32(1);

        PG_RETURN_INT64(newval);
    }
}

Datum int8_sum(PG_FUNCTION_ARGS)
{
    Numeric oldsum;
    Datum newval;

    if (PG_ARGISNULL(0)) {
        /* No non-null input seen so far... */
        if (PG_ARGISNULL(1))
            PG_RETURN_NULL(); /* still no non-null */
        /* This is the first non-null input. */
        newval = DirectFunctionCall1(int8_numeric, PG_GETARG_DATUM(1));
        PG_RETURN_DATUM(newval);
    }

    /*
     * Note that we cannot special-case the aggregate case here, as we do for
     * int2_sum and int4_sum: numeric is of variable size, so we cannot modify
     * our first parameter in-place.
     */

    oldsum = PG_GETARG_NUMERIC(0);

    /* Leave sum unchanged if new input is null. */
    if (PG_ARGISNULL(1))
        PG_RETURN_NUMERIC(oldsum);

    /* OK to do the addition. */
    newval = DirectFunctionCall1(int8_numeric, PG_GETARG_DATUM(1));

    PG_RETURN_DATUM(DirectFunctionCall2(numeric_add, NumericGetDatum(oldsum), newval));
}

#ifdef PGXC
/*
 * similar to int8_sum, except that the result is casted into int8
 */
Datum int8_sum_to_int8(PG_FUNCTION_ARGS)
{
    Datum result_num;
    Datum numeric_arg;

    /* if both arguments are null, the result is null */
    if (PG_ARGISNULL(0) && PG_ARGISNULL(1))
        PG_RETURN_NULL();

    /* if either of them is null, the other is the result */
    if (PG_ARGISNULL(0))
        PG_RETURN_DATUM(PG_GETARG_DATUM(1));

    if (PG_ARGISNULL(1))
        PG_RETURN_DATUM(PG_GETARG_DATUM(0));

    /*
     * convert the first argument to numeric (second one is converted into
     * numeric)
     * add both the arguments using int8_sum
     * convert the result into int8 using numeric_int8
     */
    numeric_arg = DirectFunctionCall1(int8_numeric, PG_GETARG_DATUM(0));
    result_num = DirectFunctionCall2(int8_sum, numeric_arg, PG_GETARG_DATUM(1));
    PG_RETURN_DATUM(DirectFunctionCall1(numeric_int8, result_num));
}
#endif

/*
 * Routines for avg(int2) and avg(int4).  The transition datatype
 * is a two-element int8 array, holding count and sum.
 */

typedef struct Int8TransTypeData {
    int64 count;
    int64 sum;
} Int8TransTypeData;

Datum int1_avg_accum(PG_FUNCTION_ARGS)
{
    ArrayType* transarray = NULL;
    uint8 newval = PG_GETARG_UINT8(1);
    Int8TransTypeData* transdata = NULL;

    /*
     * If we're invoked as an aggregate, we can cheat and modify our first
     * parameter in-place to reduce palloc overhead. Otherwise we need to make
     * a copy of it before scribbling on it.
     */
    if (AggCheckCallContext(fcinfo, NULL))
        transarray = PG_GETARG_ARRAYTYPE_P(0);
    else
        transarray = PG_GETARG_ARRAYTYPE_P_COPY(0);

    if (ARR_HASNULL(transarray) || ARR_SIZE(transarray) != ARR_OVERHEAD_NONULLS(1) + sizeof(Int8TransTypeData))
        ereport(ERROR, (errcode(ERRCODE_ARRAY_ELEMENT_ERROR), errmsg("expected 2-element int8 array")));

    transdata = (Int8TransTypeData*)ARR_DATA_PTR(transarray);
    transdata->count++;
    transdata->sum += newval;

    PG_RETURN_ARRAYTYPE_P(transarray);
}

Datum int2_avg_accum(PG_FUNCTION_ARGS)
{
    ArrayType* transarray = NULL;
    int16 newval = PG_GETARG_INT16(1);
    Int8TransTypeData* transdata = NULL;

    /*
     * If we're invoked as an aggregate, we can cheat and modify our first
     * parameter in-place to reduce palloc overhead. Otherwise we need to make
     * a copy of it before scribbling on it.
     */
    if (AggCheckCallContext(fcinfo, NULL))
        transarray = PG_GETARG_ARRAYTYPE_P(0);
    else
        transarray = PG_GETARG_ARRAYTYPE_P_COPY(0);

    if (ARR_HASNULL(transarray) || ARR_SIZE(transarray) != ARR_OVERHEAD_NONULLS(1) + sizeof(Int8TransTypeData))
        ereport(ERROR, (errcode(ERRCODE_ARRAY_ELEMENT_ERROR), errmsg("expected 2-element int8 array")));

    transdata = (Int8TransTypeData*)ARR_DATA_PTR(transarray);
    transdata->count++;
    transdata->sum += newval;

    PG_RETURN_ARRAYTYPE_P(transarray);
}

Datum int4_avg_accum(PG_FUNCTION_ARGS)
{
    ArrayType* transarray = NULL;
    int32 newval = PG_GETARG_INT32(1);
    Int8TransTypeData* transdata = NULL;

    /*
     * If we're invoked as an aggregate, we can cheat and modify our first
     * parameter in-place to reduce palloc overhead. Otherwise we need to make
     * a copy of it before scribbling on it.
     */
    if (AggCheckCallContext(fcinfo, NULL))
        transarray = PG_GETARG_ARRAYTYPE_P(0);
    else
        transarray = PG_GETARG_ARRAYTYPE_P_COPY(0);

    if (ARR_HASNULL(transarray) || ARR_SIZE(transarray) != ARR_OVERHEAD_NONULLS(1) + sizeof(Int8TransTypeData))
        ereport(ERROR, (errcode(ERRCODE_ARRAY_ELEMENT_ERROR), errmsg("expected 2-element int8 array")));

    transdata = (Int8TransTypeData*)ARR_DATA_PTR(transarray);
    transdata->count++;
    transdata->sum += newval;

    PG_RETURN_ARRAYTYPE_P(transarray);
}

Datum int8_avg(PG_FUNCTION_ARGS)
{
    ArrayType* transarray = PG_GETARG_ARRAYTYPE_P(0);
    Int8TransTypeData* transdata = NULL;
    Datum countd, sumd;

    if (ARR_HASNULL(transarray) || ARR_SIZE(transarray) != ARR_OVERHEAD_NONULLS(1) + sizeof(Int8TransTypeData))
        ereport(ERROR, (errcode(ERRCODE_ARRAY_ELEMENT_ERROR), errmsg("expected 2-element int8 array")));
    transdata = (Int8TransTypeData*)ARR_DATA_PTR(transarray);

    /* SQL92 defines AVG of no values to be NULL */
    if (transdata->count == 0)
        PG_RETURN_NULL();

    countd = DirectFunctionCall1(int8_numeric, Int64GetDatumFast(transdata->count));
    sumd = DirectFunctionCall1(int8_numeric, Int64GetDatumFast(transdata->sum));

    PG_RETURN_DATUM(DirectFunctionCall2(numeric_div, sumd, countd));
}

/* ----------------------------------------------------------------------
 *
 * Debug support
 *
 * ----------------------------------------------------------------------
 */

#ifdef NUMERIC_DEBUG

/*
 * dump_numeric() - Dump a value in the db storage format for debugging
 */
static void dump_numeric(const char* str, Numeric num)
{
    NumericDigit* digits = NUMERIC_DIGITS(num);
    int ndigits;
    int i;

    ndigits = NUMERIC_NDIGITS(num);

    printf("%s: NUMERIC w=%d d=%d ", str, NUMERIC_WEIGHT(num), NUMERIC_DSCALE(num));
    switch (NUMERIC_SIGN(num)) {
        case NUMERIC_POS:
            printf("POS");
            break;
        case NUMERIC_NEG:
            printf("NEG");
            break;
        case NUMERIC_NAN:
            printf("NaN");
            break;
        default:
            printf("SIGN=0x%x", NUMERIC_SIGN(num));
            break;
    }

    for (i = 0; i < ndigits; i++)
        printf(" %0*d", DEC_DIGITS, digits[i]);
    printf("\n");
}

/*
 * dump_var() - Dump a value in the variable format for debugging
 */
static void dump_var(const char* str, NumericVar* var)
{
    int i;

    printf("%s: VAR w=%d d=%d ", str, var->weight, var->dscale);
    switch (var->sign) {
        case NUMERIC_POS:
            printf("POS");
            break;
        case NUMERIC_NEG:
            printf("NEG");
            break;
        case NUMERIC_NAN:
            printf("NaN");
            break;
        default:
            printf("SIGN=0x%x", var->sign);
            break;
    }

    for (i = 0; i < var->ndigits; i++)
        printf(" %0*d", DEC_DIGITS, var->digits[i]);

    printf("\n");
}
#endif /* NUMERIC_DEBUG */

/* ----------------------------------------------------------------------
 *
 * Local functions follow
 *
 * In general, these do not support NaNs --- callers must eliminate
 * the possibility of NaN first.  (make_result() is an exception.)
 *
 * ----------------------------------------------------------------------
 */

/*
 * alloc_var() -
 *
 *	Allocate a digit buffer of ndigits digits (plus a spare digit for rounding)
 */
static void alloc_var(NumericVar* var, int ndigits)
{
    digitbuf_free(var);
    init_alloc_var(var, ndigits);
}

/*
 * zero_var() -
 *
 *	Set a variable to ZERO.
 *	Note: its dscale is not touched.
 */
static void zero_var(NumericVar* var)
{
    digitbuf_free(var);
    quick_init_var(var);
    var->ndigits = 0;
    var->weight = 0;         /* by convention; doesn't really matter */
    var->sign = NUMERIC_POS; /* anything but NAN... */
}

/*
 * set_var_from_str()
 *
 *	Parse a string and put the number into a variable
 *
 * This function does not handle leading or trailing spaces, and it doesn't
 * accept "NaN" either.  It returns the end+1 position so that caller can
 * check for trailing spaces/garbage if deemed necessary.
 *
 * cp is the place to actually start parsing; str is what to use in error
 * reports.  (Typically cp would be the same except advanced over spaces.)
 */
static const char* set_var_from_str(const char* str, const char* cp, NumericVar* dest)
{
    bool have_dp = FALSE;
    int i;
    unsigned char* decdigits = NULL;
    int sign = NUMERIC_POS;
    int dweight = -1;
    int ddigits;
    int dscale = 0;
    int weight;
    int ndigits;
    int offset;
    NumericDigit* digits = NULL;

    /*
     * We first parse the string to extract decimal digits and determine the
     * correct decimal weight.	Then convert to NBASE representation.
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

    if (!isdigit((unsigned char)*cp) && u_sess->attr.attr_sql.sql_compatibility == B_FORMAT) {
        char* cp = (char*)palloc0(sizeof(char));
        return cp;
    }
    if (!isdigit((unsigned char)*cp))
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                errmsg("invalid input syntax for type numeric: \"%s\"", str)));

    decdigits = (unsigned char*)palloc(strlen(cp) + DEC_DIGITS * 2);

    /* leading padding for digit alignment later */
    errno_t rc = memset_s(decdigits, strlen(cp) + DEC_DIGITS * 2, 0, DEC_DIGITS);
    securec_check_c(rc, "", "");

    i = DEC_DIGITS;

    while (*cp) {
        if (isdigit((unsigned char)*cp)) {
            decdigits[i++] = *cp++ - '0';
            if (!have_dp)
                dweight++;
            else
                dscale++;
        } else if (*cp == '.') {
            if (have_dp)
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                        errmsg("invalid input syntax for type numeric: \"%s\"", str)));
            have_dp = TRUE;
            cp++;
        } else
            break;
    }

    ddigits = i - DEC_DIGITS;
    /* trailing padding for digit alignment later */
    rc = memset_s(decdigits + i, DEC_DIGITS - 1, 0, DEC_DIGITS - 1);
    securec_check(rc, "\0", "\0");

    /* Handle exponent, if any */
    if (*cp == 'e' || *cp == 'E') {
        long exponent;
        char* endptr = NULL;

        cp++;
        exponent = strtol(cp, &endptr, 10);
        if (endptr == cp)
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                    errmsg("invalid input syntax for type numeric: \"%s\"", str)));
        cp = endptr;
        if (exponent > NUMERIC_MAX_PRECISION || exponent < -NUMERIC_MAX_PRECISION)
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                    errmsg("invalid input syntax for type numeric: \"%s\"", str)));
        dweight += (int)exponent;
        dscale -= (int)exponent;
        if (dscale < 0)
            dscale = 0;
    }

    /*
     * Okay, convert pure-decimal representation to base NBASE.  First we need
     * to determine the converted weight and ndigits.  offset is the number of
     * decimal zeroes to insert before the first given digit to have a
     * correctly aligned first NBASE digit.
     */
    if (dweight >= 0)
        weight = (dweight + 1 + DEC_DIGITS - 1) / DEC_DIGITS - 1;
    else
        weight = -((-dweight - 1) / DEC_DIGITS + 1);
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
 * set_var_from_num() -
 *
 *	Convert the packed db format into a variable
 */
static void set_var_from_num(Numeric num, NumericVar* dest)
{
    Assert(!NUMERIC_IS_BI(num));
    int ndigits = NUMERIC_NDIGITS(num);

    alloc_var(dest, ndigits);

    dest->weight = NUMERIC_WEIGHT(num);
    dest->sign = NUMERIC_SIGN(num);
    dest->dscale = NUMERIC_DSCALE(num);
    if (ndigits > 0) {
        errno_t rc =
            memcpy_s(dest->digits, ndigits * sizeof(NumericDigit), NUMERIC_DIGITS(num), ndigits * sizeof(NumericDigit));
        securec_check(rc, "\0", "\0");
    }
}

/*
 * init_var_from_num() -
 *
 *	Initialize a variable from packed db format. The digits array is not
 *	copied, which saves some cycles when the resulting var is not modified.
 *	Also, there's no need to call free_var(), as long as you don't assign any
 *	other value to it (with set_var_* functions, or by using the var as the
 *	destination of a function like add_var())
 *
 *	CAUTION: Do not modify the digits buffer of a var initialized with this
 *	function, e.g by calling round_var() or trunc_var(), as the changes will
 *	propagate to the original Numeric! It's OK to use it as the destination
 *	argument of one of the calculational functions, though.
 */
void init_var_from_num(Numeric num, NumericVar* dest)
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
 * init_ro_var_from_var() -
 *
 *	Initialize a variable from another variable
 */
static void init_ro_var_from_var(const NumericVar* value, NumericVar* dest)
{
    dest->ndigits = value->ndigits;
    dest->weight = value->weight;
    dest->sign = value->sign;
    dest->dscale = value->dscale;
    dest->digits = value->digits;
    dest->buf = dest->ndb;
}

/*
 * set_var_from_var() -
 *
 *	Copy one variable into another
 */
static void set_var_from_var(const NumericVar* value, NumericVar* dest)
{
    NumericDigit* newbuf = NULL;
    errno_t rc = 0;

    newbuf = digitbuf_alloc(value->ndigits + 1);
    newbuf[0] = 0; /* spare digit for rounding */
    if (value->ndigits > 0) {
        rc = memcpy_s(
            newbuf + 1, value->ndigits * sizeof(NumericDigit), value->digits, value->ndigits * sizeof(NumericDigit));
        securec_check(rc, "\0", "\0");
    }
    digitbuf_free(dest);

    rc = memmove_s(dest, sizeof(NumericVar), value, sizeof(NumericVar));
    securec_check(rc, "\0", "\0");
    dest->buf = newbuf;
    dest->digits = newbuf + 1;
}

/*
 * init_var_from_var() -
 *
 *	init one variable from another - they must NOT be the same variable
 */
static void
init_var_from_var(const NumericVar *value, NumericVar *dest)
{
    init_alloc_var(dest, value->ndigits);

    dest->weight = value->weight;
    dest->sign = value->sign;
    dest->dscale = value->dscale;

    if (value->ndigits > 0) {
        errno_t rc = memcpy_s(dest->digits,
                              value->ndigits * sizeof(NumericDigit),
                              value->digits,
                              value->ndigits * sizeof(NumericDigit));
        securec_check(rc, "\0", "\0");
    }
}

static void remove_tail_zero(char *ascii)
{
    if (ascii == NULL) {
        return;
    }
    int len = 0;
    bool is_decimal = false;
    while (ascii[len] != '\0') {
        if (ascii[len] == '.') {
            is_decimal = true;
        }
        len++;
    }
    if (!is_decimal) {
        return;
    }
    len--;
    while (ascii[len] == '0') {
        ascii[len] = '\0';
        len--;
    }
    if (ascii[len] == '.') {
        ascii[len] = '\0';
        len--;
    }
    if (len == -1) {
        len++;
        ascii[len] = '0';
        len++;
        ascii[len] = '\0';
    }
    return;
}

/*
 * get_str_from_var() -
 *
 *	Convert a var to text representation (guts of numeric_out).
 *	CAUTION: var's contents may be modified by rounding!
 *	Returns a palloc'd string.
 */
static char* get_str_from_var(NumericVar* var)
{
    int dscale;
    char* str = NULL;
    char* cp = NULL;
    char* endcp = NULL;
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
    if (i <= 0)
        i = 1;

    str = (char*)palloc(i + dscale + DEC_DIGITS + 2);
    cp = str;

    /*
     * Output a dash for negative values
     */
    if (var->sign == NUMERIC_NEG)
        *cp++ = '-';

    /*
     * Output all digits before the decimal point
     */
    if (var->weight < 0) {
        d = var->weight + 1;
        if (DISPLAY_LEADING_ZERO) {
            *cp++ = '0';
        }
    } else {
        for (d = 0; d <= var->weight; d++) {
            dig = (d < var->ndigits) ? var->digits[d] : 0;
            /* In the first digit, suppress extra leading decimal zeroes */
#if DEC_DIGITS == 4
            {
                bool putit = (d > 0);

                d1 = dig / 1000;
                dig -= d1 * 1000;
                putit |= (d1 > 0);
                if (putit)
                    *cp++ = d1 + '0';
                d1 = dig / 100;
                dig -= d1 * 100;
                putit |= (d1 > 0);
                if (putit)
                    *cp++ = d1 + '0';
                d1 = dig / 10;
                dig -= d1 * 10;
                putit |= (uint32)(d1 > 0);
                if (putit)
                    *cp++ = d1 + '0';
                *cp++ = dig + '0';
            }
#elif DEC_DIGITS == 2
            d1 = dig / 10;
            dig -= d1 * 10;
            if (d1 > 0 || d > 0)
                *cp++ = d1 + '0';
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
    if (HIDE_TAILING_ZERO) {
        remove_tail_zero(str);
    }
    return str;
}

/*
 * output_get_str_from_var() -
 *
 *      Convert a var to text representation (guts of numeric_out).
 *      CAUTION: var's contents may be modified by rounding!
 *      Returns a palloc'd string.
 */
static char* output_get_str_from_var(NumericVar* var)
{
    int dscale;
    char* str = NULL;
    char* cp = NULL;
    char* endcp = NULL;
    int i;
    int d;
    NumericDigit dig;
    int len;

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
    if (i <= 0)
        i = 1;

    len = i + dscale + DEC_DIGITS + 2;
    if (len >= 64) {
        str = (char*)palloc(len);
    } else {
        u_sess->utils_cxt.numericoutput_buffer[0] = '\0';
        str = u_sess->utils_cxt.numericoutput_buffer;
    }
    cp = str;

    /*
     * Output a dash for negative values
     */
    if (var->sign == NUMERIC_NEG)
        *cp++ = '-';

    /*
     * Output all digits before the decimal point
     */
    if (var->weight < 0) {
        d = var->weight + 1;
        if (DISPLAY_LEADING_ZERO) {
            *cp++ = '0';
        }
    } else {
        for (d = 0; d <= var->weight; d++) {
            dig = (d < var->ndigits) ? var->digits[d] : 0;
            /* In the first digit, suppress extra leading decimal zeroes */
#if DEC_DIGITS == 4
            {
                bool putit = (d > 0);

                d1 = dig / 1000;
                dig -= d1 * 1000;
                putit |= (d1 > 0);
                if (putit)
                    *cp++ = d1 + '0';
                d1 = dig / 100;
                dig -= d1 * 100;
                putit |= (d1 > 0);
                if (putit)
                    *cp++ = d1 + '0';
                d1 = dig / 10;
                dig -= d1 * 10;
                putit |= (uint32)(d1 > 0);
                if (putit)
                    *cp++ = d1 + '0';
                *cp++ = dig + '0';
            }
#elif DEC_DIGITS == 2
            d1 = dig / 10;
            dig -= d1 * 10;
            if (d1 > 0 || d > 0)
                *cp++ = d1 + '0';
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
    if (HIDE_TAILING_ZERO) {
        remove_tail_zero(str);
    }

    return str;
}


/*
 * get_str_from_var_sci() -
 *
 *	Convert a var to a normalised scientific notation text representation.
 *	This function does the heavy lifting for numeric_out_sci().
 *
 *	This notation has the general form a * 10^b, where a is known as the
 *	"significand" and b is known as the "exponent".
 *
 *	Because we can't do superscript in ASCII (and because we want to copy
 *	printf's behaviour) we display the exponent using E notation, with a
 *	minimum of two exponent digits.
 *
 *	For example, the value 1234 could be output as 1.2e+03.
 *
 *	We assume that the exponent can fit into an int32.
 *
 *	rscale is the number of decimal digits desired after the decimal point in
 *	the output, negative values will be treated as meaning zero.
 *
 *	CAUTION: var's contents may be modified by rounding!
 *
 *	Returns a palloc'd string.
 */
static char* get_str_from_var_sci(NumericVar* var, int rscale)
{
    int32 exponent;
    NumericVar denominator;
    NumericVar significand;
    int denom_scale;
    size_t len;
    char* str = NULL;
    char* sig_out = NULL;
    errno_t ret = EOK;

    if (rscale < 0)
        rscale = 0;

    /*
     * Determine the exponent of this number in normalised form.
     *
     * This is the exponent required to represent the number with only one
     * significant digit before the decimal place.
     */
    if (var->ndigits > 0) {
        exponent = (var->weight + 1) * DEC_DIGITS;

        /*
         * Compensate for leading decimal zeroes in the first numeric digit by
         * decrementing the exponent.
         */
        exponent -= DEC_DIGITS - (int)log10(var->digits[0]);
    } else {
        /*
         * If var has no digits, then it must be zero.
         *
         * Zero doesn't technically have a meaningful exponent in normalised
         * notation, but we just display the exponent as zero for consistency
         * of output.
         */
        exponent = 0;
    }

    /*
     * The denominator is set to 10 raised to the power of the exponent.
     *
     * We then divide var by the denominator to get the significand, rounding
     * to rscale decimal digits in the process.
     */
    if (exponent < 0)
        denom_scale = -exponent;
    else
        denom_scale = 0;

    init_var(&denominator);
    init_var(&significand);

    power_var_int(&const_ten, exponent, &denominator, denom_scale);
    div_var(var, &denominator, &significand, rscale, true);
    sig_out = get_str_from_var(&significand);

    free_var(&denominator);
    free_var(&significand);

    /*
     * Allocate space for the result.
     *
     * In addition to the significand, we need room for the exponent
     * decoration ("e"), the sign of the exponent, up to 10 digits for the
     * exponent itself, and of course the null terminator.
     */
    len = strlen(sig_out) + 13;
    const size_t slen = len + 1;
    str = (char*)palloc(slen);
    ret = snprintf_s(str, slen, len, "%se%+03d", sig_out, exponent);
    securec_check_ss(ret, "", "");

    pfree_ext(sig_out);

    return str;
}

/*
 * make_result() -
 *
 *	Create the packed db numeric format in palloc()'d memory from
 *	a variable.
 */
Numeric make_result(NumericVar* var)
{
    Numeric result;
    NumericDigit* digits = var->digits;
    int weight = var->weight;
    int sign = var->sign;
    int n;
    Size len;

    if (sign == NUMERIC_NAN) {
        result = (Numeric)palloc(NUMERIC_HDRSZ_SHORT);

        SET_VARSIZE(result, NUMERIC_HDRSZ_SHORT);
        result->choice.n_header = NUMERIC_NAN;
        /* the header word is all we need */

        dump_numeric("make_result()", result);
        return result;
    }

    n = var->ndigits;

    /* truncate leading zeroes */
    while (n > 0 && *digits == 0) {
        digits++;
        weight--;
        n--;
    }
    /* truncate trailing zeroes */
    while (n > 0 && digits[n - 1] == 0)
        n--;

    /* If zero result, force to weight=0 and positive sign */
    if (n == 0) {
        weight = 0;
        sign = NUMERIC_POS;
    }

    /* Build the result */
    if (NUMERIC_CAN_BE_SHORT(var->dscale, weight)) {
        len = NUMERIC_HDRSZ_SHORT + n * sizeof(NumericDigit);
        result = (Numeric)palloc(len);
        SET_VARSIZE(result, len);
        result->choice.n_short.n_header =
            (sign == NUMERIC_NEG ? (NUMERIC_SHORT | NUMERIC_SHORT_SIGN_MASK) : NUMERIC_SHORT) |
            (var->dscale << NUMERIC_SHORT_DSCALE_SHIFT) | (weight < 0 ? NUMERIC_SHORT_WEIGHT_SIGN_MASK : 0) |
            (weight & NUMERIC_SHORT_WEIGHT_MASK);
    } else {
        len = NUMERIC_HDRSZ + n * sizeof(NumericDigit);
        result = (Numeric)palloc(len);
        SET_VARSIZE(result, len);
        result->choice.n_long.n_sign_dscale = sign | (var->dscale & NUMERIC_DSCALE_MASK);
        result->choice.n_long.n_weight = weight;
    }
    if (n > 0) {
        errno_t rc = memcpy_s(NUMERIC_DIGITS(result), n * sizeof(NumericDigit), digits, n * sizeof(NumericDigit));
        securec_check(rc, "\0", "\0");
    }
    Assert(NUMERIC_NDIGITS(result) == (unsigned int)(n));

    /* Check for overflow of int16 fields */
    if (NUMERIC_WEIGHT(result) != weight || NUMERIC_DSCALE(result) != var->dscale)
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("value overflows numeric format")));

    dump_numeric("make_result()", result);
    return result;
}

Numeric makeNumeric(NumericVar* var)
{
    return make_result(var);
}


/*
 * apply_typmod() -
 *
 *	Do bounds checking and rounding according to the attributes
 *	typmod field.
 */
static void apply_typmod(NumericVar* var, int32 typmod)
{
    int precision;
    int scale;
    int maxdigits;
    int ddigits;
    int i;

    /* Do nothing if we have a default typmod (-1) */
    if (typmod < (int32)(VARHDRSZ))
        return;

    typmod -= VARHDRSZ;
    precision = (int32)(((uint32)(typmod) >> 16) & 0xffff);
    scale = (int32)(((uint32)typmod) & 0xffff);
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
                if (dig < 10)
                    ddigits -= 3;
                else if (dig < 100)
                    ddigits -= 2;
                else if (dig < 1000)
                    ddigits -= 1;
#elif DEC_DIGITS == 2
                if (dig < 10)
                    ddigits -= 1;
#elif DEC_DIGITS == 1
                /* no adjustment */
#else
#error unsupported NBASE
#endif
                if (ddigits > maxdigits)
                    ereport(ERROR,
                        (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                            errmsg("numeric field overflow"),
                            errdetail(
                                "A field with precision %d, scale %d must round to an absolute value less than %s%d.",
                                precision,
                                scale,
                                /* Display 10^0 as 1 */
                                maxdigits ? "10^" : "",
                                maxdigits ? maxdigits : 1)));
                break;
            }
            ddigits -= DEC_DIGITS;
        }
    }
}

/*
 * Convert numeric to int8, rounding if needed.
 *
 * Note: param can_ignore controls the function raising ERROR or WARNING. TRUE means overflowing will report WARNING
 * and set result to INT64_MAX/INT64_MIN instead. FALSE make it raise ERROR directly. FALSE DEFAULTED. It should be only
 * used for controls of keyword IGNORE.
 *
 * If overflow, return false (no error is raised).  Return true if okay.
 */
bool numericvar_to_int64(const NumericVar* var, int64* result, bool can_ignore)
{
    NumericDigit* digits = NULL;
    int ndigits;
    int weight;
    int i;
    int64 val;
    bool neg = false;
    NumericVar rounded;

    /* Round to nearest integer */
    init_var(&rounded);
    set_var_from_var(var, &rounded);
    round_var(&rounded, 0);

    /* Check for zero input */
    strip_var(&rounded);
    ndigits = rounded.ndigits;
    if (ndigits == 0) {
        *result = 0;
        free_var(&rounded);
        return true;
    }

    /*
     * For input like 10000000000, we must treat stripped digits as real. So
     * the loop assumes there are weight+1 digits before the decimal point.
     */
    weight = rounded.weight;
    Assert(weight >= 0 && ndigits <= weight + 1);

    /*
     * Construct the result. To avoid issues with converting a value
     * corresponding to INT64_MIN (which can't be represented as a positive 64
     * bit two's complement integer), accumulate value as a negative number.
     */
    digits = rounded.digits;
    neg = (rounded.sign == NUMERIC_NEG);
    val = -digits[0];
    for (i = 1; i <= weight; i++) {
        if (unlikely(pg_mul_s64_overflow(val, NBASE, &val))) {
            free_var(&rounded);
            if (can_ignore) {
                *result = neg ? LONG_MIN : LONG_MAX;
                ereport(WARNING, (errmsg("value out of range")));
                return true;
            }
            return false;
        }

        if (i < ndigits) {
            if (unlikely(pg_sub_s64_overflow(val, digits[i], &val))) {
                free_var(&rounded);
                if (can_ignore) {
                    *result = neg ? LONG_MIN : LONG_MAX;
                    ereport(WARNING, (errmsg("value out of range")));
                    return true;
                }
                return false;
            }
        }
    }

    free_var(&rounded);

    if (!neg) {
        if (unlikely(val == PG_INT64_MIN))
            return false;
        val = -val;
    }
    *result = val;

    return true;
}

/*
 * Convert int8 value to numeric.
 */
void int64_to_numericvar(int64 val, NumericVar* var)
{
    uint64 uval, newuval;
    NumericDigit* ptr = NULL;
    int ndigits;

    /* int64 can require at most 19 decimal digits; add one for safety */
    alloc_var(var, 20 / DEC_DIGITS);
    if (val < 0) {
        var->sign = NUMERIC_NEG;
        uval = -val;
    } else {
        var->sign = NUMERIC_POS;
        uval = val;
    }
    var->dscale = 0;
    if (val == 0) {
        var->ndigits = 0;
        var->weight = 0;
        return;
    }
    ptr = var->digits + var->ndigits;
    ndigits = 0;
    do {
        ptr--;
        ndigits++;
        newuval = uval / NBASE;
        *ptr = uval - newuval * NBASE;
        uval = newuval;
    } while (uval);
    var->digits = ptr;
    var->ndigits = ndigits;
    var->weight = ndigits - 1;
}

/*
 * Convert numeric to float8; if out of range, return +/- HUGE_VAL
 */
double numeric_to_double_no_overflow(Numeric num)
{
    char* tmp = NULL;
    double val;
    char* endptr = NULL;

    tmp = DatumGetCString(DirectFunctionCall1(numeric_out_with_zero, NumericGetDatum(num)));

    /* unlike float8in, we ignore ERANGE from strtod */
    val = strtod(tmp, &endptr);
    if (*endptr != '\0') {
        /* shouldn't happen ... */
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                errmsg("invalid input syntax for type double precision: \"%s\"", tmp)));
    }

    pfree_ext(tmp);

    return val;
}

/* As above, but work from a NumericVar */
static double numericvar_to_double_no_overflow(NumericVar* var)
{
    char* tmp = NULL;
    double val;
    char* endptr = NULL;

    tmp = get_str_from_var(var);

    /* unlike float8in, we ignore ERANGE from strtod */
    val = strtod(tmp, &endptr);
    if (*endptr != '\0') {
        /* shouldn't happen ... */
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                errmsg("invalid input syntax for type double precision: \"%s\"", tmp)));
    }

    pfree_ext(tmp);

    return val;
}

/*
 * cmp_var() -
 *
 *	Compare two values on variable level.  We assume zeroes have been
 *	truncated to no digits.
 */
static int cmp_var(NumericVar* var1, NumericVar* var2)
{
    return cmp_var_common(
        var1->digits, var1->ndigits, var1->weight, var1->sign, var2->digits, var2->ndigits, var2->weight, var2->sign);
}

/*
 * cmp_var_common() -
 *
 *	Main routine of cmp_var(). This function can be used by both
 *	NumericVar and Numeric.
 */
static int cmp_var_common(const NumericDigit* var1digits, int var1ndigits, int var1weight, int var1sign,
    const NumericDigit* var2digits, int var2ndigits, int var2weight, int var2sign)
{
    if (var1ndigits == 0) {
        if (var2ndigits == 0)
            return 0;
        if (var2sign == NUMERIC_NEG)
            return 1;
        return -1;
    }
    if (var2ndigits == 0) {
        if (var1sign == NUMERIC_POS)
            return 1;
        return -1;
    }

    if (var1sign == NUMERIC_POS) {
        if (var2sign == NUMERIC_NEG)
            return 1;
        return cmp_abs_common(var1digits, var1ndigits, var1weight, var2digits, var2ndigits, var2weight);
    }

    if (var2sign == NUMERIC_POS)
        return -1;

    return cmp_abs_common(var2digits, var2ndigits, var2weight, var1digits, var1ndigits, var1weight);
}

/*
 * add_var() -
 *
 *	Full version of add functionality on variable level (handling signs).
 *	result might point to one of the operands too without danger.
 */
void add_var(NumericVar* var1, NumericVar* var2, NumericVar* result)
{
    /*
     * Decide on the signs of the two variables what to do
     */
    if (var1->sign == NUMERIC_POS) {
        if (var2->sign == NUMERIC_POS) {
            /*
             * Both are positive result = +(ABS(var1) + ABS(var2))
             */
            add_abs(var1, var2, result);
            result->sign = NUMERIC_POS;
        } else {
            /*
             * var1 is positive, var2 is negative Must compare absolute values
             */
            switch (cmp_abs(var1, var2)) {
                case 0:
                    /* ----------
                     * ABS(var1) == ABS(var2)
                     * result = ZERO
                     * ----------
                     */
                    zero_var(result);
                    result->dscale = Max(var1->dscale, var2->dscale);
                    break;

                case 1:
                    /* ----------
                     * ABS(var1) > ABS(var2)
                     * result = +(ABS(var1) - ABS(var2))
                     * ----------
                     */
                    sub_abs(var1, var2, result);
                    result->sign = NUMERIC_POS;
                    break;

                case -1:
                    /* ----------
                     * ABS(var1) < ABS(var2)
                     * result = -(ABS(var2) - ABS(var1))
                     * ----------
                     */
                    sub_abs(var2, var1, result);
                    result->sign = NUMERIC_NEG;
                    break;
                default:
                    break;
            }
        }
    } else {
        if (var2->sign == NUMERIC_POS) {
            /* ----------
             * var1 is negative, var2 is positive
             * Must compare absolute values
             * ----------
             */
            switch (cmp_abs(var1, var2)) {
                case 0:
                    /* ----------
                     * ABS(var1) == ABS(var2)
                     * result = ZERO
                     * ----------
                     */
                    zero_var(result);
                    result->dscale = Max(var1->dscale, var2->dscale);
                    break;

                case 1:
                    /* ----------
                     * ABS(var1) > ABS(var2)
                     * result = -(ABS(var1) - ABS(var2))
                     * ----------
                     */
                    sub_abs(var1, var2, result);
                    result->sign = NUMERIC_NEG;
                    break;

                case -1:
                    /* ----------
                     * ABS(var1) < ABS(var2)
                     * result = +(ABS(var2) - ABS(var1))
                     * ----------
                     */
                    sub_abs(var2, var1, result);
                    result->sign = NUMERIC_POS;
                    break;
                default:
                    break;
            }
        } else {
            /* ----------
             * Both are negative
             * result = -(ABS(var1) + ABS(var2))
             * ----------
             */
            add_abs(var1, var2, result);
            result->sign = NUMERIC_NEG;
        }
    }
}

/*
 * sub_var() -
 *
 *	Full version of sub functionality on variable level (handling signs).
 *	result might point to one of the operands too without danger.
 */
static void sub_var(NumericVar* var1, NumericVar* var2, NumericVar* result)
{
    /*
     * Decide on the signs of the two variables what to do
     */
    if (var1->sign == NUMERIC_POS) {
        if (var2->sign == NUMERIC_NEG) {
            /* ----------
             * var1 is positive, var2 is negative
             * result = +(ABS(var1) + ABS(var2))
             * ----------
             */
            add_abs(var1, var2, result);
            result->sign = NUMERIC_POS;
        } else {
            /* ----------
             * Both are positive
             * Must compare absolute values
             * ----------
             */
            switch (cmp_abs(var1, var2)) {
                case 0:
                    /* ----------
                     * ABS(var1) == ABS(var2)
                     * result = ZERO
                     * ----------
                     */
                    zero_var(result);
                    result->dscale = Max(var1->dscale, var2->dscale);
                    break;

                case 1:
                    /* ----------
                     * ABS(var1) > ABS(var2)
                     * result = +(ABS(var1) - ABS(var2))
                     * ----------
                     */
                    sub_abs(var1, var2, result);
                    result->sign = NUMERIC_POS;
                    break;

                case -1:
                    /* ----------
                     * ABS(var1) < ABS(var2)
                     * result = -(ABS(var2) - ABS(var1))
                     * ----------
                     */
                    sub_abs(var2, var1, result);
                    result->sign = NUMERIC_NEG;
                    break;
                default:
                    break;
            }
        }
    } else {
        if (var2->sign == NUMERIC_NEG) {
            /* ----------
             * Both are negative
             * Must compare absolute values
             * ----------
             */
            switch (cmp_abs(var1, var2)) {
                case 0:
                    /* ----------
                     * ABS(var1) == ABS(var2)
                     * result = ZERO
                     * ----------
                     */
                    zero_var(result);
                    result->dscale = Max(var1->dscale, var2->dscale);
                    break;

                case 1:
                    /* ----------
                     * ABS(var1) > ABS(var2)
                     * result = -(ABS(var1) - ABS(var2))
                     * ----------
                     */
                    sub_abs(var1, var2, result);
                    result->sign = NUMERIC_NEG;
                    break;

                case -1:
                    /* ----------
                     * ABS(var1) < ABS(var2)
                     * result = +(ABS(var2) - ABS(var1))
                     * ----------
                     */
                    sub_abs(var2, var1, result);
                    result->sign = NUMERIC_POS;
                    break;
                default:
                    break;
            }
        } else {
            /* ----------
             * var1 is negative, var2 is positive
             * result = -(ABS(var1) + ABS(var2))
             * ----------
             */
            add_abs(var1, var2, result);
            result->sign = NUMERIC_NEG;
        }
    }
}

/*
 * mul_var() -
 *
 *	Multiplication on variable level. Product of var1 * var2 is stored
 *	in result.	Result is rounded to no more than rscale fractional digits.
 */
static void mul_var(NumericVar* var1, NumericVar* var2, NumericVar* result, int rscale)
{
    int res_ndigits;
    int res_sign;
    int res_weight;
    int maxdigits;
    int* dig = NULL;
    int carry;
    int maxdig;
    int newdig;
    int var1ndigits;
    int var2ndigits;
    NumericDigit* var1digits = NULL;
    NumericDigit* var2digits = NULL;
    NumericDigit* res_digits = NULL;
    int i, i1, i2;
    int tdig[NUMERIC_LOCAL_NDIG];

    /*
     * Arrange for var1 to be the shorter of the two numbers.  This improves
     * performance because the inner multiplication loop is much simpler than
     * the outer loop, so it's better to have a smaller number of iterations
     * of the outer loop.  This also reduces the number of times that the
     * accumulator array needs to be normalized.
     */
    if (var1->ndigits > var2->ndigits) {
        NumericVar* tmp = var1;

        var1 = var2;
        var2 = tmp;
    }

    /* copy these values into local vars for speed in inner loop */
    var1ndigits = var1->ndigits;
    var2ndigits = var2->ndigits;
    var1digits = var1->digits;
    var2digits = var2->digits;

    if (var1ndigits == 0 || var2ndigits == 0) {
        /* one or both inputs is zero; so is result */
        zero_var(result);
        result->dscale = rscale;
        return;
    }

    /* Determine result sign and (maximum possible) weight */
    if (var1->sign == var2->sign)
        res_sign = NUMERIC_POS;
    else
        res_sign = NUMERIC_NEG;
    res_weight = var1->weight + var2->weight + 2;

    /*
     * Determine the number of result digits to compute.  If the exact result
     * would have more than rscale fractional digits, truncate the computation
     * with MUL_GUARD_DIGITS guard digits, i.e., ignore input digits that
     * would only contribute to the right of that.  (This will give the exact
     * rounded-to-rscale answer unless carries out of the ignored positions
     * would have propagated through more than MUL_GUARD_DIGITS digits.)
     *
     * Note: an exact computation could not produce more than var1ndigits +
     * var2ndigits digits, but we allocate one extra output digit in case
     * rscale-driven rounding produces a carry out of the highest exact digit.
     */
    res_ndigits = var1ndigits + var2ndigits + 1;
    maxdigits = res_weight + 1 + (rscale + DEC_DIGITS - 1) / DEC_DIGITS + MUL_GUARD_DIGITS;
    res_ndigits = Min(res_ndigits, maxdigits);

    if (res_ndigits < 3) {
        /* All input digits will be ignored; so result is zero */
        zero_var(result);
        result->dscale = rscale;
        return;
    }

    /*
     * We do the arithmetic in an array "dig[]" of signed int's.  Since
     * INT_MAX is noticeably larger than NBASE*NBASE, this gives us headroom
     * to avoid normalizing carries immediately.
     *
     * maxdig tracks the maximum possible value of any dig[] entry; when this
     * threatens to exceed INT_MAX, we take the time to propagate carries.
     * Furthermore, we need to ensure that overflow doesn't occur during the
     * carry propagation passes either.  The carry values could be as much as
     * INT_MAX/NBASE, so really we must normalize when digits threaten to
     * exceed INT_MAX - INT_MAX/NBASE.
     *
     * To avoid overflow in maxdig itself, it actually represents the max
     * possible value divided by NBASE-1, ie, at the top of the loop it is
     * known that no dig[] entry exceeds maxdig * (NBASE-1).
     */
    uint32 dig_size = (uint32)(res_ndigits * sizeof(int));
    if (res_ndigits > NUMERIC_LOCAL_NMAX) {
        dig = (int *)palloc0(dig_size);
    } else {
        errno_t rc = memset_s(tdig, sizeof(tdig), 0, dig_size);
        securec_check(rc, "\0", "\0");
        dig = tdig;
    }
    maxdig = 0;

    /*
     * The least significant digits of var1 should be ignored if they don't
     * contribute directly to the first res_ndigits digits of the result that
     * we are computing.
     *
     * Digit i1 of var1 and digit i2 of var2 are multiplied and added to digit
     * i1+i2+2 of the accumulator array, so we need only consider digits of
     * var1 for which i1 <= res_ndigits - 3.
     */
    for (i1 = Min(var1ndigits - 1, res_ndigits - 3); i1 >= 0; i1--) {
        int var1digit = var1digits[i1];

        if (var1digit == 0)
            continue;

        /* Time to normalize? */
        maxdig += var1digit;
        if (maxdig > (INT_MAX - INT_MAX / NBASE) / (NBASE - 1)) {
            /* Yes, do it */
            carry = 0;
            for (i = res_ndigits - 1; i >= 0; i--) {
                newdig = dig[i] + carry;
                if (newdig >= NBASE) {
                    carry = newdig / NBASE;
                    newdig -= carry * NBASE;
                } else
                    carry = 0;
                dig[i] = newdig;
            }
            Assert(carry == 0);
            /* Reset maxdig to indicate new worst-case */
            maxdig = 1 + var1digit;
        }

        /*
         * Add the appropriate multiple of var2 into the accumulator.
         *
         * As above, digits of var2 can be ignored if they don't contribute,
         * so we only include digits for which i1+i2+2 < res_ndigits.
         *
         * This inner loop is the performance bottleneck for multiplication,
         * so we want to keep it simple enough so that it can be
         * auto-vectorized.  Accordingly, process the digits left-to-right
         * even though schoolbook multiplication would suggest right-to-left.
         * Since we aren't propagating carries in this loop, the order does
         * not matter.
         */
        {
            int i2limit = Min(var2ndigits, res_ndigits - i1 - 2);
            int *dig_i1_2 = &dig[i1 + 2];

            for (i2 = 0; i2 < i2limit; i2++)
                dig_i1_2[i2] += var1digit * var2digits[i2];
        }
    }

    /*
     * Now we do a final carry propagation pass to normalize the result, which
     * we combine with storing the result digits into the output. Note that
     * this is still done at full precision w/guard digits.
     */
    alloc_var(result, res_ndigits);
    res_digits = result->digits;
    carry = 0;
    for (i = res_ndigits - 1; i >= 0; i--) {
        newdig = dig[i] + carry;
        if (newdig >= NBASE) {
            carry = newdig / NBASE;
            newdig -= carry * NBASE;
        } else
            carry = 0;
        res_digits[i] = newdig;
    }
    Assert(carry == 0);

    if (dig != tdig) {
        pfree_ext(dig);
    }

    /*
     * Finally, round the result to the requested precision.
     */
    result->weight = res_weight;
    result->sign = res_sign;

    /* Round to target rscale (and set result->dscale) */
    round_var(result, rscale);

    /* Strip leading and trailing zeroes */
    strip_var(result);
}

/*
 * div_var() -
 *
 *	Division on variable level. Quotient of var1 / var2 is stored in result.
 *	The quotient is figured to exactly rscale fractional digits.
 *	If round is true, it is rounded at the rscale'th digit; if false, it
 *	is truncated (towards zero) at that digit.
 */
static void div_var(NumericVar* var1, NumericVar* var2, NumericVar* result, int rscale, bool round)
{
    int div_ndigits;
    int res_ndigits;
    int res_sign;
    int res_weight;
    int carry;
    int borrow;
    int divisor1;
    int divisor2;
    NumericDigit* dividend = NULL;
    NumericDigit* divisor = NULL;
    NumericDigit* res_digits = NULL;
    int i;
    int j;

    /* copy these values into local vars for speed in inner loop */
    int var1ndigits = var1->ndigits;
    int var2ndigits = var2->ndigits;

    /*
     * First of all division by zero check; we must not be handed an
     * unnormalized divisor.
     */
    if (var2ndigits == 0 || var2->digits[0] == 0)
        ereport(ERROR, (errcode(ERRCODE_DIVISION_BY_ZERO), errmsg("division by zero")));
    /*
     * If the divisor has just one or two digits, delegate to div_var_int(),
     * which uses fast short division.
     */
    if (var2ndigits <= 2) {
        int idivisor;
        int idivisor_weight;

        idivisor = var2->digits[0];
        idivisor_weight = var2->weight;
        if (var2ndigits == 2) {
            idivisor = idivisor * NBASE + var2->digits[1];
            idivisor_weight--;
        }
        if (var2->sign == NUMERIC_NEG) {
            idivisor = -idivisor;
        }

        div_var_int(var1, idivisor, idivisor_weight, result, rscale, round);
        return;
    }

    /*
     * Otherwise, perform full long division.
     */

    /* Result zero check */
    if (var1ndigits == 0) {
        zero_var(result);
        result->dscale = rscale;
        return;
    }

    /*
     * Determine the result sign, weight and number of digits to calculate.
     * The weight figured here is correct if the emitted quotient has no
     * leading zero digits; otherwise strip_var() will fix things up.
     */
    if (var1->sign == var2->sign)
        res_sign = NUMERIC_POS;
    else
        res_sign = NUMERIC_NEG;
    res_weight = var1->weight - var2->weight;
    /* The number of accurate result digits we need to produce: */
    res_ndigits = res_weight + 1 + (rscale + DEC_DIGITS - 1) / DEC_DIGITS;
    /* ... but always at least 1 */
    res_ndigits = Max(res_ndigits, 1);
    /* If rounding needed, figure one more digit to ensure correct result */
    if (round)
        res_ndigits++;

    /*
     * The working dividend normally requires res_ndigits + var2ndigits
     * digits, but make it at least var1ndigits so we can load all of var1
     * into it.  (There will be an additional digit dividend[0] in the
     * dividend space, but for consistency with Knuth's notation we don't
     * count that in div_ndigits.)
     */
    div_ndigits = res_ndigits + var2ndigits;
    div_ndigits = Max(div_ndigits, var1ndigits);

    /*
     * We need a workspace with room for the working dividend (div_ndigits+1
     * digits) plus room for the possibly-normalized divisor (var2ndigits
     * digits).  It is convenient also to have a zero at divisor[0] with the
     * actual divisor data in divisor[1 .. var2ndigits].  Transferring the
     * digits into the workspace also allows us to realloc the result (which
     * might be the same as either input var) before we begin the main loop.
     * Note that we use palloc0 to ensure that divisor[0], dividend[0], and
     * any additional dividend positions beyond var1ndigits, start out 0.
     */
    dividend = (NumericDigit*)palloc0((div_ndigits + var2ndigits + 2) * sizeof(NumericDigit));
    divisor = dividend + (div_ndigits + 1);
    errno_t rc =
        memcpy_s(dividend + 1, var1ndigits * sizeof(NumericDigit), var1->digits, var1ndigits * sizeof(NumericDigit));
    securec_check(rc, "\0", "\0");
    rc = memcpy_s(divisor + 1, var2ndigits * sizeof(NumericDigit), var2->digits, var2ndigits * sizeof(NumericDigit));
    securec_check(rc, "\0", "\0");
    /*
     * Now we can realloc the result to hold the generated quotient digits.
     */
    alloc_var(result, res_ndigits);
    res_digits = result->digits;

    /*
     * The full multiple-place algorithm is taken from Knuth volume 2,
     * Algorithm 4.3.1D.
     *
     * We need the first divisor digit to be >= NBASE/2.  If it isn't,
     * make it so by scaling up both the divisor and dividend by the
     * factor "d".	(The reason for allocating dividend[0] above is to
     * leave room for possible carry here.)
     */
    if (divisor[1] < HALF_NBASE) {
        int d = NBASE / (divisor[1] + 1);

        carry = 0;
        for (i = var2ndigits; i > 0; i--) {
            carry += divisor[i] * d;
            divisor[i] = carry % NBASE;
            carry = carry / NBASE;
        }
        Assert(carry == 0);
        carry = 0;
        /* at this point only var1ndigits of dividend can be nonzero */
        for (i = var1ndigits; i >= 0; i--) {
            carry += dividend[i] * d;
            dividend[i] = carry % NBASE;
            carry = carry / NBASE;
        }
        Assert(carry == 0);
        Assert(divisor[1] >= HALF_NBASE);
    }
    /* First 2 divisor digits are used repeatedly in main loop */
    divisor1 = divisor[1];
    divisor2 = divisor[2];

    /*
     * Begin the main loop.  Each iteration of this loop produces the j'th
     * quotient digit by dividing dividend[j .. j + var2ndigits] by the
     * divisor; this is essentially the same as the common manual
     * procedure for long division.
     */
    for (j = 0; j < res_ndigits; j++) {
        /* Estimate quotient digit from the first two dividend digits */
        int next2digits = dividend[j] * NBASE + dividend[j + 1];
        int qhat;

        /*
         * If next2digits are 0, then quotient digit must be 0 and there's
         * no need to adjust the working dividend.	It's worth testing
         * here to fall out ASAP when processing trailing zeroes in a
         * dividend.
         */
        if (next2digits == 0) {
            res_digits[j] = 0;
            continue;
        }

        if (dividend[j] == divisor1)
            qhat = NBASE - 1;
        else
            qhat = next2digits / divisor1;

        /*
         * Adjust quotient digit if it's too large.  Knuth proves that
         * after this step, the quotient digit will be either correct or
         * just one too large.	(Note: it's OK to use dividend[j+2] here
         * because we know the divisor length is at least 2.)
         */
        while (divisor2 * qhat > (next2digits - qhat * divisor1) * NBASE + dividend[j + 2])
            qhat--;

        /* As above, need do nothing more when quotient digit is 0 */
        if (qhat > 0) {
            NumericDigit *dividend_j = &dividend[j];

            /*
             * Multiply the divisor by qhat, and subtract that from the
             * working dividend.  The multiplication and subtraction are
             * folded together here, noting that qhat <= NBASE (since it might
             * be one too large), and so the intermediate result "tmp_result"
             * is in the range [-NBASE^2, NBASE - 1], and "borrow" is in the
             * range [0, NBASE].
            */
            borrow = 0;
            for (i = var2ndigits; i >= 0; i--) {
                int tmp_result;

                tmp_result = dividend_j[i] - borrow - divisor[i] * qhat;
                borrow = (NBASE - 1 - tmp_result) / NBASE;
                dividend_j[i] = tmp_result + borrow * NBASE;
            }

            /*
             * If we got a borrow out of the top dividend digit, then indeed
             * qhat was one too large.  Fix it, and add back the divisor to
             * correct the working dividend.  (Knuth proves that this will
             * occur only about 3/NBASE of the time; hence, it's a good idea
             * to test this code with small NBASE to be sure this section gets
             * exercised.)
             */
            if (borrow) {
                qhat--;
                carry = 0;
                for (i = var2ndigits; i >= 0; i--) {
                    carry += dividend_j[i] + divisor[i];
                    if (carry >= NBASE) {
                        dividend_j[i] = carry - NBASE;
                        carry = 1;
                    } else {
                        dividend_j[i] = carry;
                        carry = 0;
                    }
                }
                /* A carry should occur here to cancel the borrow above */
                Assert(carry == 1);
            }
        }

        /* And we're done with this quotient digit */
        res_digits[j] = qhat;
    }

    pfree_ext(dividend);

    /*
     * Finally, round or truncate the result to the requested precision.
     */
    result->weight = res_weight;
    result->sign = res_sign;

    /* Round or truncate to target rscale (and set result->dscale) */
    if (round)
        round_var(result, rscale);
    else
        trunc_var(result, rscale);

    /* Strip leading and trailing zeroes */
    strip_var(result);
}

/*
 * div_var_fast() -
 *
 *	This has the same API as div_var, but is implemented using the division
 *	algorithm from the "FM" library, rather than Knuth's schoolbook-division
 *	approach.  This is significantly faster but can produce inaccurate
 *	results, because it sometimes has to propagate rounding to the left,
 *	and so we can never be entirely sure that we know the requested digits
 *	exactly.  We compute DIV_GUARD_DIGITS extra digits, but there is
 *	no certainty that that's enough.  We use this only in the transcendental
 *	function calculation routines, where everything is approximate anyway.
 */
static void div_var_fast(NumericVar* var1, NumericVar* var2, NumericVar* result, int rscale, bool round)
{
    int div_ndigits;
    int res_sign;
    int res_weight;
    int* div = NULL;
    int qdigit;
    int carry;
    int maxdiv;
    int newdig;
    NumericDigit* res_digits = NULL;
    double fdividend, fdivisor, fdivisorinverse, fquotient;
    int qi;
    int i;

    /* copy these values into local vars for speed in inner loop */
    int var1ndigits = var1->ndigits;
    int var2ndigits = var2->ndigits;
    NumericDigit* var1digits = var1->digits;
    NumericDigit* var2digits = var2->digits;
    int tdiv[NUMERIC_LOCAL_NDIG];

    /*
     * First of all division by zero check; we must not be handed an
     * unnormalized divisor.
     */
    if (var2ndigits == 0 || var2digits[0] == 0)
        ereport(ERROR, (errcode(ERRCODE_DIVISION_BY_ZERO), errmsg("division by zero")));

    /*
     * If the divisor has just one or two digits, delegate to div_var_int(),
     * which uses fast short division.
     */
    if (var2ndigits <= 2) {
        int idivisor;
        int idivisor_weight;

        idivisor = var2->digits[0];
        idivisor_weight = var2->weight;
        if (var2ndigits == 2) {
            idivisor = idivisor * NBASE + var2->digits[1];
            idivisor_weight--;
        }
        if (var2->sign == NUMERIC_NEG) {
            idivisor = -idivisor;
        }

        div_var_int(var1, idivisor, idivisor_weight, result, rscale, round);
        return;
    }

    /*
     * Otherwise, perform full long division.
     */

    /*
     * Now result zero check
     */
    if (var1ndigits == 0) {
        zero_var(result);
        result->dscale = rscale;
        return;
    }

    /*
     * Determine the result sign, weight and number of digits to calculate
     */
    if (var1->sign == var2->sign)
        res_sign = NUMERIC_POS;
    else
        res_sign = NUMERIC_NEG;
    res_weight = var1->weight - var2->weight + 1;
    /* The number of accurate result digits we need to produce: */
    div_ndigits = res_weight + 1 + (rscale + DEC_DIGITS - 1) / DEC_DIGITS;
    /* Add guard digits for roundoff error */
    div_ndigits += DIV_GUARD_DIGITS;
    if (div_ndigits < DIV_GUARD_DIGITS)
        div_ndigits = DIV_GUARD_DIGITS;
    /* Must be at least var1ndigits, too, to simplify data-loading loop */
    if (div_ndigits < var1ndigits)
        div_ndigits = var1ndigits;

    /*
     * We do the arithmetic in an array "div[]" of signed int's.  Since
     * INT_MAX is noticeably larger than NBASE*NBASE, this gives us headroom
     * to avoid normalizing carries immediately.
     *
     * We start with div[] containing one zero digit followed by the
     * dividend's digits (plus appended zeroes to reach the desired precision
     * including guard digits).  Each step of the main loop computes an
     * (approximate) quotient digit and stores it into div[], removing one
     * position of dividend space.	A final pass of carry propagation takes
     * care of any mistaken quotient digits.
     */
    i = (div_ndigits + 1) * sizeof(int);
    if (div_ndigits > NUMERIC_LOCAL_NMAX) {
        div = (int *) palloc0(i);
    } else {
        errno_t rc = memset_s(tdiv, i, 0, i);
        securec_check(rc, "\0", "\0");
        div = tdiv;
    }
    for (i = 0; i < var1ndigits; i++)
        div[i + 1] = var1digits[i];

    /*
     * We estimate each quotient digit using floating-point arithmetic, taking
     * the first four digits of the (current) dividend and divisor. This must
     * be float to avoid overflow.
     */
    fdivisor = (double)var2digits[0];
    for (i = 1; i < 4; i++) {
        fdivisor *= NBASE;
        if (i < var2ndigits)
            fdivisor += (double)var2digits[i];
    }
    fdivisorinverse = 1.0 / fdivisor;

    /*
     * maxdiv tracks the maximum possible absolute value of any div[] entry;
     * when this threatens to exceed INT_MAX, we take the time to propagate
     * carries.  Furthermore, we need to ensure that overflow doesn't occur
     * during the carry propagation passes either.	The carry values may have
     * an absolute value as high as INT_MAX/NBASE + 1, so really we must
     * normalize when digits threaten to exceed INT_MAX - INT_MAX/NBASE - 1.
     *
     * To avoid overflow in maxdiv itself, it represents the max absolute
     * value divided by NBASE-1, ie, at the top of the loop it is known that
     * no div[] entry has an absolute value exceeding maxdiv * (NBASE-1).
     */
    maxdiv = 1;

    /*
     * Outer loop computes next quotient digit, which will go into div[qi]
     */
    for (qi = 0; qi < div_ndigits; qi++) {
        /* Approximate the current dividend value */
        fdividend = (double)div[qi];
        for (i = 1; i < 4; i++) {
            fdividend *= NBASE;
            if (qi + i <= div_ndigits)
                fdividend += (double)div[qi + i];
        }
        /* Compute the (approximate) quotient digit */
        fquotient = fdividend * fdivisorinverse;
        qdigit = (fquotient >= 0.0) ? ((int)fquotient) : (((int)fquotient) - 1); /* truncate towards -infinity */

        if (qdigit != 0) {
            /* Do we need to normalize now? */
            maxdiv += Abs(qdigit);
            if (maxdiv > (INT_MAX - INT_MAX / NBASE - 1) / (NBASE - 1)) {
                /* Yes, do it */
                carry = 0;
                for (i = div_ndigits; i > qi; i--) {
                    newdig = div[i] + carry;
                    if (newdig < 0) {
                        carry = -((-newdig - 1) / NBASE) - 1;
                        newdig -= carry * NBASE;
                    } else if (newdig >= NBASE) {
                        carry = newdig / NBASE;
                        newdig -= carry * NBASE;
                    } else
                        carry = 0;
                    div[i] = newdig;
                }
                newdig = div[qi] + carry;
                div[qi] = newdig;

                /*
                 * All the div[] digits except possibly div[qi] are now in the
                 * range 0..NBASE-1.
                 */
                maxdiv = Abs(newdig) / (NBASE - 1);
                maxdiv = Max(maxdiv, 1);

                /*
                 * Recompute the quotient digit since new info may have
                 * propagated into the top four dividend digits
                 */
                fdividend = (double)div[qi];
                for (i = 1; i < 4; i++) {
                    fdividend *= NBASE;
                    if (qi + i <= div_ndigits)
                        fdividend += (double)div[qi + i];
                }
                /* Compute the (approximate) quotient digit */
                fquotient = fdividend * fdivisorinverse;
                qdigit =
                    (fquotient >= 0.0) ? ((int)fquotient) : (((int)fquotient) - 1); /* truncate towards -infinity */
                maxdiv += Abs(qdigit);
            }

            /* Subtract off the appropriate multiple of the divisor */
            if (qdigit != 0) {
                int istop = Min(var2ndigits, div_ndigits - qi + 1);

                for (i = 0; i < istop; i++)
                    div[qi + i] -= qdigit * var2digits[i];
            }
        }

        /*
         * The dividend digit we are about to replace might still be nonzero.
         * Fold it into the next digit position.  We don't need to worry about
         * overflow here since this should nearly cancel with the subtraction
         * of the divisor.
         */
        div[qi + 1] += div[qi] * NBASE;

        div[qi] = qdigit;
    }

    /*
     * Approximate and store the last quotient digit (div[div_ndigits])
     */
    fdividend = (double)div[qi];
    for (i = 1; i < 4; i++)
        fdividend *= NBASE;
    fquotient = fdividend * fdivisorinverse;
    qdigit = (fquotient >= 0.0) ? ((int)fquotient) : (((int)fquotient) - 1); /* truncate towards -infinity */
    div[qi] = qdigit;

    /*
     * Now we do a final carry propagation pass to normalize the result, which
     * we combine with storing the result digits into the output. Note that
     * this is still done at full precision w/guard digits.
     */
    alloc_var(result, div_ndigits + 1);
    res_digits = result->digits;
    carry = 0;
    for (i = div_ndigits; i >= 0; i--) {
        newdig = div[i] + carry;
        if (newdig < 0) {
            carry = -((-newdig - 1) / NBASE) - 1;
            newdig -= carry * NBASE;
        } else if (newdig >= NBASE) {
            carry = newdig / NBASE;
            newdig -= carry * NBASE;
        } else
            carry = 0;
        res_digits[i] = newdig;
    }
    Assert(carry == 0);

    if (div != tdiv) {
        pfree_ext(div);
    }

    /*
     * Finally, round the result to the requested precision.
     */
    result->weight = res_weight;
    result->sign = res_sign;

    /* Round to target rscale (and set result->dscale) */
    if (round)
        round_var(result, rscale);
    else
        trunc_var(result, rscale);

    /* Strip leading and trailing zeroes */
    strip_var(result);
}

/*
 * div_var_int() -
 *
 *	Divide a numeric variable by a 32-bit integer with the specified weight.
 *	The quotient var / (ival * NBASE^ival_weight) is stored in result.
 */
static void
div_var_int(const NumericVar *var, int ival, int ival_weight, NumericVar *result, int rscale, bool round)
{
    NumericDigit *var_digits = var->digits;
    int var_ndigits = var->ndigits;
    int res_sign;
    int res_weight;
    int res_ndigits;
    NumericDigit *res_buf;
    NumericDigit *res_digits;
    uint32 divisor;
    int i;

    /* Guard against division by zero */
    if (ival == 0)
        ereport(ERROR, (errcode(ERRCODE_DIVISION_BY_ZERO), errmsg("division by zero")));

    /* Result zero check */
    if (var_ndigits == 0) {
        zero_var(result);
        result->dscale = rscale;
        return;
    }

    /*
     * Determine the result sign, weight and number of digits to calculate.
     * The weight figured here is correct if the emitted quotient has no
     * leading zero digits; otherwise strip_var() will fix things up.
     */
    if (var->sign == NUMERIC_POS)
        res_sign = ival > 0 ? NUMERIC_POS : NUMERIC_NEG;
    else
        res_sign = ival > 0 ? NUMERIC_NEG : NUMERIC_POS;
    res_weight = var->weight - ival_weight;
    /* The number of accurate result digits we need to produce: */
    res_ndigits = res_weight + 1 + (rscale + DEC_DIGITS - 1) / DEC_DIGITS;
    /* ... but always at least 1 */
    res_ndigits = Max(res_ndigits, 1);
    /* If rounding needed, figure one more digit to ensure correct result */
    if (round)
        res_ndigits++;

    res_buf = digitbuf_alloc(res_ndigits + 1);
    res_buf[0] = 0; /* spare digit for later rounding */
    res_digits = res_buf + 1;

    /*
     * Now compute the quotient digits.  This is the short division algorithm
     * described in Knuth volume 2, section 4.3.1 exercise 16, except that we
     * allow the divisor to exceed the internal base.
     *
     * In this algorithm, the carry from one digit to the next is at most
     * divisor - 1.  Therefore, while processing the next digit, carry may
     * become as large as divisor * NBASE - 1, and so it requires a 64-bit
     * integer if this exceeds UINT_MAX.
     */
    divisor = Abs(ival);
    if (divisor <= UINT_MAX / NBASE) {
        /* carry cannot overflow 32 bits */
        uint32		carry = 0;

        for (i = 0; i < res_ndigits; i++) {
            carry = carry * NBASE + (i < var_ndigits ? var_digits[i] : 0);
            res_digits[i] = (NumericDigit) (carry / divisor);
            carry = carry % divisor;
        }
    } else {
        /* carry may exceed 32 bits */
        uint64		carry = 0;

        for (i = 0; i < res_ndigits; i++) {
            carry = carry * NBASE + (i < var_ndigits ? var_digits[i] : 0);
            res_digits[i] = (NumericDigit) (carry / divisor);
            carry = carry % divisor;
        }
    }

    /* Store the quotient in result */
    digitbuf_free(result);
    result->ndigits = res_ndigits;
    result->buf = res_buf;
    result->digits = res_digits;
    result->weight = res_weight;
    result->sign = res_sign;

    /* Round or truncate to target rscale (and set result->dscale) */
    if (round)
        round_var(result, rscale);
    else
        trunc_var(result, rscale);

    /* Strip leading/trailing zeroes */
    strip_var(result);
}


/*
 * Default scale selection for division
 *
 * Returns the appropriate result scale for the division result.
 */
static int select_div_scale(NumericVar* var1, NumericVar* var2)
{
    int weight1, weight2, qweight, i;
    NumericDigit firstdigit1, firstdigit2;
    int rscale;

    /*
     * The result scale of a division isn't specified in any SQL standard. For
     * openGauss we select a result scale that will give at least
     * NUMERIC_MIN_SIG_DIGITS significant digits, so that numeric gives a
     * result no less accurate than float8; but use a scale not less than
     * either input's display scale.
     */

    /* Get the actual (normalized) weight and first digit of each input */

    weight1 = 0; /* values to use if var1 is zero */
    firstdigit1 = 0;
    for (i = 0; i < var1->ndigits; i++) {
        firstdigit1 = var1->digits[i];
        if (firstdigit1 != 0) {
            weight1 = var1->weight - i;
            break;
        }
    }

    weight2 = 0; /* values to use if var2 is zero */
    firstdigit2 = 0;
    for (i = 0; i < var2->ndigits; i++) {
        firstdigit2 = var2->digits[i];
        if (firstdigit2 != 0) {
            weight2 = var2->weight - i;
            break;
        }
    }

    /*
     * Estimate weight of quotient.  If the two first digits are equal, we
     * can't be sure, but assume that var1 is less than var2.
     */
    qweight = weight1 - weight2;
    if (firstdigit1 <= firstdigit2)
        qweight--;

    /* Select result scale */
    rscale = NUMERIC_MIN_SIG_DIGITS - qweight * DEC_DIGITS;
    rscale = Max(rscale, var1->dscale);
    rscale = Max(rscale, var2->dscale);
    rscale = Max(rscale, NUMERIC_MIN_DISPLAY_SCALE);
    rscale = Min(rscale, NUMERIC_MAX_DISPLAY_SCALE);

    return rscale;
}

/*
 * mod_var() -
 *
 *	Calculate the modulo of two numerics at variable level
 */
static void mod_var(NumericVar* var1, NumericVar* var2, NumericVar* result)
{
    NumericVar tmp;

    init_var(&tmp);

    /* ---------
     * We do this using the equation
     *		mod(x,y) = x - trunc(x/y)*y
     * div_var can be persuaded to give us trunc(x/y) directly.
     * ----------
     */
    div_var(var1, var2, &tmp, 0, false);

    mul_var(var2, &tmp, &tmp, var2->dscale);

    sub_var(var1, &tmp, result);

    free_var(&tmp);
}

/*
 * ceil_var() -
 *
 *	Return the smallest integer greater than or equal to the argument
 *	on variable level
 */
static void ceil_var(NumericVar* var, NumericVar* result)
{
    NumericVar tmp;

    init_var_from_var(var, &tmp);

    trunc_var(&tmp, 0);

    if (var->sign == NUMERIC_POS && cmp_var(var, &tmp) != 0)
        add_var(&tmp, &const_one, &tmp);

    set_var_from_var(&tmp, result);
    free_var(&tmp);
}

/*
 * floor_var() -
 *
 *	Return the largest integer equal to or less than the argument
 *	on variable level
 */
static void floor_var(NumericVar* var, NumericVar* result)
{
    NumericVar tmp;

    init_var_from_var(var, &tmp);

    trunc_var(&tmp, 0);

    if (var->sign == NUMERIC_NEG && cmp_var(var, &tmp) != 0)
        sub_var(&tmp, &const_one, &tmp);

    set_var_from_var(&tmp, result);
    free_var(&tmp);
}

/*
 * sqrt_var() -
 *
 *	Compute the square root of x using Newton's algorithm
 */
static void sqrt_var(NumericVar* arg, NumericVar* result, int rscale)
{
    NumericVar tmp_arg;
    NumericVar tmp_val;
    NumericVar last_val;
    int local_rscale;
    int stat;

    local_rscale = rscale + 8;

    stat = cmp_var(arg, &const_zero);
    if (stat == 0) {
        zero_var(result);
        result->dscale = rscale;
        return;
    }

    /*
     * SQL2003 defines sqrt() in terms of power, so we need to emit the right
     * SQLSTATE error code if the operand is negative.
     */
    if (stat < 0)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION),
                errmsg("cannot take square root of a negative number")));

    /* Copy arg in case it is the same var as result */
    init_var_from_var(arg, &tmp_arg);

    /*
     * Initialize the result to the first guess
     */
    alloc_var(result, 1);
    result->digits[0] = tmp_arg.digits[0] / 2;
    if (result->digits[0] == 0)
        result->digits[0] = 1;
    result->weight = tmp_arg.weight / 2;
    result->sign = NUMERIC_POS;

    init_var_from_var(result, &last_val);
    quick_init_var(&tmp_val);

    for (;;) {
        div_var_fast(&tmp_arg, result, &tmp_val, local_rscale, true);

        add_var(result, &tmp_val, result);
        mul_var(result, &const_zero_point_five, result, local_rscale);

        if (cmp_var(&last_val, result) == 0)
            break;
        set_var_from_var(result, &last_val);
    }

    free_var(&last_val);
    free_var(&tmp_val);
    free_var(&tmp_arg);

    /* Round to requested precision */
    round_var(result, rscale);
}

/*
 * exp_var() -
 *
 *	Raise e to the power of x, computed to rscale fractional digits
 */
static void exp_var(NumericVar* arg, NumericVar* result, int rscale)
{
    NumericVar x;
    NumericVar elem;
    int ni;
    double val;
    int dweight;
    int ndiv2;
    int sig_digits;
    int local_rscale;

    init_var(&x);
    init_var(&elem);

    set_var_from_var(arg, &x);

    /*
     * Estimate the dweight of the result using floating point arithmetic, so
     * that we can choose an appropriate local rscale for the calculation.
     */
    val = numericvar_to_double_no_overflow(&x);

    /* Guard against overflow */
    if (Abs(val) >= NUMERIC_MAX_RESULT_SCALE * 3)
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("value overflows numeric format")));

    /* decimal weight = log10(e^x) = x * log10(e) */
    dweight = (int)(val * 0.434294481903252);

    /*
     * Reduce x to the range -0.01 <= x <= 0.01 (approximately) by dividing by
     * 2^n, to improve the convergence rate of the Taylor series.
     */
    if (Abs(val) > 0.01) {
        NumericVar tmp;

        init_var(&tmp);
        set_var_from_var(&const_two, &tmp);

        ndiv2 = 1;
        val /= 2;

        while (Abs(val) > 0.01) {
            ndiv2++;
            val /= 2;
            add_var(&tmp, &tmp, &tmp);
        }

        local_rscale = x.dscale + ndiv2;
        div_var_int(&x, 1 << ndiv2, 0, &x, local_rscale, true);

        free_var(&tmp);
    } else
        ndiv2 = 0;

    /*
     * Set the scale for the Taylor series expansion.  The final result has
     * (dweight + rscale + 1) significant digits.  In addition, we have to
     * raise the Taylor series result to the power 2^ndiv2, which introduces
     * an error of up to around log10(2^ndiv2) digits, so work with this many
     * extra digits of precision (plus a few more for good measure).
     */
    sig_digits = 1 + dweight + rscale + (int)(ndiv2 * 0.301029995663981);
    sig_digits = Max(sig_digits, 0) + 8;

    local_rscale = sig_digits - 1;

    /*
     * Use the Taylor series
     *
     * exp(x) = 1 + x + x^2/2! + x^3/3! + ...
     *
     * Given the limited range of x, this should converge reasonably quickly.
     * We run the series until the terms fall below the local_rscale limit.
     */
    add_var(&const_one, &x, result);

    mul_var(&x, &x, &elem, local_rscale);
    ni = 2;
    div_var_int(&elem, ni, 0, &elem, local_rscale, true);

    while (elem.ndigits != 0) {
        add_var(result, &elem, result);

        mul_var(&elem, &x, &elem, local_rscale);
        ni++;
        div_var_int(&elem, ni, 0, &elem, local_rscale, true);
    }

    /*
     * Compensate for the argument range reduction.  Since the weight of the
     * result doubles with each multiplication, we can reduce the local rscale
     * as we proceed.
     */
    while (ndiv2-- > 0) {
        local_rscale = sig_digits - result->weight * 2 * DEC_DIGITS;
        local_rscale = Max(local_rscale, NUMERIC_MIN_DISPLAY_SCALE);
        mul_var(result, result, result, local_rscale);
    }

    /* Round to requested rscale */
    round_var(result, rscale);

    free_var(&x);
    free_var(&elem);
}

/*
 * Estimate the dweight of the most significant decimal digit of the natural
 * logarithm of a number.
 *
 * Essentially, we're approximating log10(abs(ln(var))).  This is used to
 * determine the appropriate rscale when computing natural logarithms.
 */
static int estimate_ln_dweight(NumericVar* var)
{
    int ln_dweight;

    if (cmp_var(var, &const_zero_point_nine) >= 0 && cmp_var(var, &const_one_point_one) <= 0) {
        /*
         * 0.9 <= var <= 1.1
         *
         * ln(var) has a negative weight (possibly very large).  To get a
         * reasonably accurate result, estimate it using ln(1+x) ~= x.
         */
        NumericVar x;

        init_var(&x);
        sub_var(var, &const_one, &x);

        if (x.ndigits > 0) {
            /* Use weight of most significant decimal digit of x */
            ln_dweight = x.weight * DEC_DIGITS + (int)log10(x.digits[0]);
        } else {
            /* x = 0.  Since ln(1) = 0 exactly, we don't need extra digits */
            ln_dweight = 0;
        }

        free_var(&x);
    } else {
        /*
         * Estimate the logarithm using the first couple of digits from the
         * input number.  This will give an accurate result whenever the input
         * is not too close to 1.
         */
        if (var->ndigits > 0) {
            int digits;
            int dweight;
            double ln_var;

            digits = var->digits[0];
            dweight = var->weight * DEC_DIGITS;

            if (var->ndigits > 1) {
                digits = digits * NBASE + var->digits[1];
                dweight -= DEC_DIGITS;
            }

            /* ----------
             * We have var ~= digits * 10^dweight
             * so ln(var) ~= ln(digits) + dweight * ln(10)
             * ----------
             */
            ln_var = log((double)digits) + dweight * 2.302585092994046;
            ln_dweight = (int)log10(Abs(ln_var));
        } else {
            /* Caller should fail on ln(0), but for the moment return zero */
            ln_dweight = 0;
        }
    }

    return ln_dweight;
}

/*
 * ln_var() -
 *
 *	Compute the natural log of x
 */
static void ln_var(NumericVar* arg, NumericVar* result, int rscale)
{
    NumericVar x;
    NumericVar xx;
    int ni;
    NumericVar elem;
    NumericVar fact;
    int local_rscale;
    int cmp;

    cmp = cmp_var(arg, &const_zero);
    if (cmp == 0)
        ereport(ERROR, (errcode(ERRCODE_INVALID_ARGUMENT_FOR_LOG), errmsg("cannot take logarithm of zero")));
    else if (cmp < 0)
        ereport(
            ERROR, (errcode(ERRCODE_INVALID_ARGUMENT_FOR_LOG), errmsg("cannot take logarithm of a negative number")));

    init_var(&x);
    init_var(&xx);
    init_var(&elem);

    init_var_from_var(arg, &x);
    init_ro_var_from_var(&const_two, &fact);

    /*
     * Reduce input into range 0.9 < x < 1.1 with repeated sqrt() operations.
     *
     * The final logarithm will have up to around rscale+6 significant digits.
     * Each sqrt() will roughly have the weight of x, so adjust the local
     * rscale as we work so that we keep this many significant digits at each
     * step (plus a few more for good measure).
     */
    while (cmp_var(&x, &const_zero_point_nine) <= 0) {
        local_rscale = rscale - x.weight * DEC_DIGITS / 2 + 8;
        local_rscale = Max(local_rscale, NUMERIC_MIN_DISPLAY_SCALE);
        sqrt_var(&x, &x, local_rscale);
        mul_var(&fact, &const_two, &fact, 0);
    }
    while (cmp_var(&x, &const_one_point_one) >= 0) {
        local_rscale = rscale - x.weight * DEC_DIGITS / 2 + 8;
        local_rscale = Max(local_rscale, NUMERIC_MIN_DISPLAY_SCALE);
        sqrt_var(&x, &x, local_rscale);
        mul_var(&fact, &const_two, &fact, 0);
    }

    /*
     * We use the Taylor series for 0.5 * ln((1+z)/(1-z)),
     *
     * z + z^3/3 + z^5/5 + ...
     *
     * where z = (x-1)/(x+1) is in the range (approximately) -0.053 .. 0.048
     * due to the above range-reduction of x.
     *
     * The convergence of this is not as fast as one would like, but is
     * tolerable given that z is small.
     */
    local_rscale = rscale + 8;

    sub_var(&x, &const_one, result);
    add_var(&x, &const_one, &elem);
    div_var_fast(result, &elem, result, local_rscale, true);
    set_var_from_var(result, &xx);
    mul_var(result, result, &x, local_rscale);

    ni = 1;

    for (;;) {
        ni += 2;
        mul_var(&xx, &x, &xx, local_rscale);
        div_var_int(&xx, ni, 0, &elem, local_rscale, true);

        if (elem.ndigits == 0)
            break;

        add_var(result, &elem, result);

        if (elem.weight < (result->weight - local_rscale * 2 / DEC_DIGITS))
            break;
    }

    /* Compensate for argument range reduction, round to requested rscale */
    mul_var(result, &fact, result, rscale);

    free_var(&x);
    free_var(&xx);
    free_var(&elem);
    free_var(&fact);
}

/*
 * log_var() -
 *
 *	Compute the logarithm of num in a given base.
 *
 *	Note: this routine chooses dscale of the result.
 */
static void log_var(NumericVar* base, NumericVar* num, NumericVar* result)
{
    NumericVar ln_base;
    NumericVar ln_num;
    int ln_base_dweight;
    int ln_num_dweight;
    int result_dweight;
    int rscale;
    int ln_base_rscale;
    int ln_num_rscale;

    init_var(&ln_base);
    init_var(&ln_num);

    /* Estimated dweights of ln(base), ln(num) and the final result */
    ln_base_dweight = estimate_ln_dweight(base);
    ln_num_dweight = estimate_ln_dweight(num);
    result_dweight = ln_num_dweight - ln_base_dweight;

    /*
     * Select the scale of the result so that it will have at least
     * NUMERIC_MIN_SIG_DIGITS significant digits and is not less than either
     * input's display scale.
     */
    rscale = NUMERIC_MIN_SIG_DIGITS - result_dweight;
    rscale = Max(rscale, base->dscale);
    rscale = Max(rscale, num->dscale);
    rscale = Max(rscale, NUMERIC_MIN_DISPLAY_SCALE);
    rscale = Min(rscale, NUMERIC_MAX_DISPLAY_SCALE);

    /*
     * Set the scales for ln(base) and ln(num) so that they each have more
     * significant digits than the final result.
     */
    ln_base_rscale = rscale + result_dweight - ln_base_dweight + 8;
    ln_base_rscale = Max(ln_base_rscale, NUMERIC_MIN_DISPLAY_SCALE);

    ln_num_rscale = rscale + result_dweight - ln_num_dweight + 8;
    ln_num_rscale = Max(ln_num_rscale, NUMERIC_MIN_DISPLAY_SCALE);

    /* Form natural logarithms */
    ln_var(base, &ln_base, ln_base_rscale);
    ln_var(num, &ln_num, ln_num_rscale);

    /* Divide and round to the required scale */
    div_var_fast(&ln_num, &ln_base, result, rscale, true);

    free_var(&ln_num);
    free_var(&ln_base);
}

/*
 * power_var() -
 *
 *	Raise base to the power of exp
 *
 *	Note: this routine chooses dscale of the result.
 */
static void power_var(NumericVar* base, NumericVar* exp, NumericVar* result)
{
    NumericVar ln_base;
    NumericVar ln_num;
    int ln_dweight;
    int rscale;
    int local_rscale;
    double val;

    /* If exp can be represented as an integer, use power_var_int */
    if (exp->ndigits == 0 || exp->ndigits <= exp->weight + 1) {
        /* exact integer, but does it fit in int? */
        int64 expval64;

        if (numericvar_to_int64(exp, &expval64)) {
            int expval = (int)expval64;

            /* Test for overflow by reverse-conversion. */
            if ((int64)expval == expval64) {
                /* Okay, select rscale */
                rscale = NUMERIC_MIN_SIG_DIGITS;
                rscale = Max(rscale, base->dscale);
                rscale = Max(rscale, NUMERIC_MIN_DISPLAY_SCALE);
                rscale = Min(rscale, NUMERIC_MAX_DISPLAY_SCALE);

                power_var_int(base, expval, result, rscale);
                return;
            }
        }
    }

    /*
     * This avoids log(0) for cases of 0 raised to a non-integer.  0 ^ 0 is
     * handled by power_var_int().
     */
    if (cmp_var(base, &const_zero) == 0) {
        set_var_from_var(&const_zero, result);
        result->dscale = NUMERIC_MIN_SIG_DIGITS; /* no need to round */
        return;
    }

    init_var(&ln_base);
    init_var(&ln_num);

    /* ----------
     * Decide on the scale for the ln() calculation.  For this we need an
     * estimate of the weight of the result, which we obtain by doing an
     * initial low-precision calculation of exp * ln(base).
     *
     * We want result = e ^ (exp * ln(base))
     * so result dweight = log10(result) = exp * ln(base) * log10(e)
     *
     * We also perform a crude overflow test here so that we can exit early if
     * the full-precision result is sure to overflow, and to guard against
     * integer overflow when determining the scale for the real calculation.
     * exp_var() supports inputs up to NUMERIC_MAX_RESULT_SCALE * 3, so the
     * result will overflow if exp * ln(base) >= NUMERIC_MAX_RESULT_SCALE * 3.
     * Since the values here are only approximations, we apply a small fuzz
     * factor to this overflow test and let exp_var() determine the exact
     * overflow threshold so that it is consistent for all inputs.
     * ----------
     */
    ln_dweight = estimate_ln_dweight(base);

    local_rscale = 8 - ln_dweight;
    local_rscale = Max(local_rscale, NUMERIC_MIN_DISPLAY_SCALE);
    local_rscale = Min(local_rscale, NUMERIC_MAX_DISPLAY_SCALE);

    ln_var(base, &ln_base, local_rscale);

    mul_var(&ln_base, exp, &ln_num, local_rscale);

    val = numericvar_to_double_no_overflow(&ln_num);

    /* initial overflow test with fuzz factor */
    if (Abs(val) > NUMERIC_MAX_RESULT_SCALE * 3.01)
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("value overflows numeric format")));

    val *= 0.434294481903252; /* approximate decimal result weight */

    /* choose the result scale */
    rscale = NUMERIC_MIN_SIG_DIGITS - (int)val;
    rscale = Max(rscale, base->dscale);
    rscale = Max(rscale, exp->dscale);
    rscale = Max(rscale, NUMERIC_MIN_DISPLAY_SCALE);
    rscale = Min(rscale, NUMERIC_MAX_DISPLAY_SCALE);

    /* set the scale for the real exp * ln(base) calculation */
    local_rscale = rscale + (int)val - ln_dweight + 8;
    local_rscale = Max(local_rscale, NUMERIC_MIN_DISPLAY_SCALE);

    /* and do the real calculation */

    ln_var(base, &ln_base, local_rscale);

    mul_var(&ln_base, exp, &ln_num, local_rscale);

    exp_var(&ln_num, result, rscale);

    free_var(&ln_num);
    free_var(&ln_base);
}

/*
 * power_var_int() -
 *
 *	Raise base to the power of exp, where exp is an integer.
 */
static void power_var_int(NumericVar* base, int exp, NumericVar* result, int rscale)
{
    double f;
    int p;
    int i;
    int sig_digits;
    unsigned int mask;
    bool neg = false;
    NumericVar base_prod;
    int local_rscale;

    /* Handle some common special cases, as well as corner cases */
    switch (exp) {
        case 0:

            /*
             * While 0 ^ 0 can be either 1 or indeterminate (error), we treat
             * it as 1 because most programming languages do this. SQL:2003
             * also requires a return value of 1.
             * http://en.wikipedia.org/wiki/Exponentiation#Zero_to_the_zero_pow
             * er
             */
            set_var_from_var(&const_one, result);
            result->dscale = rscale; /* no need to round */
            return;
        case 1:
            set_var_from_var(base, result);
            round_var(result, rscale);
            return;
        case -1:
            div_var(&const_one, base, result, rscale, true);
            return;
        case 2:
            mul_var(base, base, result, rscale);
            return;
        default:
            break;
    }

    /* Handle the special case where the base is zero */
    if (base->ndigits == 0) {
        if (exp < 0)
            ereport(ERROR, (errcode(ERRCODE_DIVISION_BY_ZERO), errmsg("division by zero")));
        zero_var(result);
        result->dscale = rscale;
        return;
    }

    /*
     * The general case repeatedly multiplies base according to the bit
     * pattern of exp.
     *
     * First we need to estimate the weight of the result so that we know how
     * many significant digits are needed.
     */
    f = base->digits[0];
    p = base->weight * DEC_DIGITS;

    for (i = 1; i < base->ndigits && i * DEC_DIGITS < 16; i++) {
        f = f * NBASE + base->digits[i];
        p -= DEC_DIGITS;
    }

    /* ----------
     * We have base ~= f * 10^p
     * so log10(result) = log10(base^exp) ~= exp * (log10(f) + p)
     * ----------
     */
    f = exp * (log10(f) + p);

    /*
     * Apply crude overflow/underflow tests so we can exit early if the result
     * certainly will overflow/underflow.
     */
    if (f > 3 * SHRT_MAX * DEC_DIGITS)
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("value overflows numeric format")));
    if (f + 1 < -rscale || f + 1 < -NUMERIC_MAX_DISPLAY_SCALE) {
        zero_var(result);
        result->dscale = rscale;
        return;
    }

    /*
     * Approximate number of significant digits in the result.  Note that the
     * underflow test above means that this is necessarily >= 0.
     */
    sig_digits = 1 + rscale + (int)f;

    /*
     * The multiplications to produce the result may introduce an error of up
     * to around log10(abs(exp)) digits, so work with this many extra digits
     * of precision (plus a few more for good measure).
     */
    sig_digits += (int)log(Abs(exp)) + 8;

    /*
     * Now we can proceed with the multiplications.
     */
    neg = (exp < 0);
    mask = Abs(exp);

    init_var(&base_prod);
    set_var_from_var(base, &base_prod);

    if (mask & 1) {
        set_var_from_var(base, result);
    } else {
        set_var_from_var(&const_one, result);
    }

    while ((mask >>= 1) > 0) {
        /*
         * Do the multiplications using rscales large enough to hold the
         * results to the required number of significant digits, but don't
         * waste time by exceeding the scales of the numbers themselves.
         */
        local_rscale = sig_digits - 2 * base_prod.weight * DEC_DIGITS;
        local_rscale = Min(local_rscale, 2 * base_prod.dscale);
        local_rscale = Max(local_rscale, NUMERIC_MIN_DISPLAY_SCALE);

        mul_var(&base_prod, &base_prod, &base_prod, local_rscale);

        if (mask & 1) {
            local_rscale = sig_digits - (base_prod.weight + result->weight) * DEC_DIGITS;
            local_rscale = Min(local_rscale, base_prod.dscale + result->dscale);
            local_rscale = Max(local_rscale, NUMERIC_MIN_DISPLAY_SCALE);

            mul_var(&base_prod, result, result, local_rscale);
        }

        /*
         * When abs(base) > 1, the number of digits to the left of the decimal
         * point in base_prod doubles at each iteration, so if exp is large we
         * could easily spend large amounts of time and memory space doing the
         * multiplications.  But once the weight exceeds what will fit in
         * int16, the final result is guaranteed to overflow (or underflow, if
         * exp < 0), so we can give up before wasting too many cycles.
         */
        if (base_prod.weight > SHRT_MAX || result->weight > SHRT_MAX) {
            /* overflow, unless neg, in which case result should be 0 */
            if (!neg)
                ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("value overflows numeric format")));
            zero_var(result);
            neg = false;
            break;
        }
    }

    free_var(&base_prod);

    /* Compensate for input sign, and round to requested rscale */
    if (neg) {
        div_var_fast(&const_one, result, result, rscale, true);
    } else {
        round_var(result, rscale);
    }
}

/* ----------------------------------------------------------------------
 *
 * Following are the lowest level functions that operate unsigned
 * on the variable level
 *
 * ----------------------------------------------------------------------
 */

/* ----------
 * cmp_abs() -
 *
 *	Compare the absolute values of var1 and var2
 *	Returns:	-1 for ABS(var1) < ABS(var2)
 *				0  for ABS(var1) == ABS(var2)
 *				1  for ABS(var1) > ABS(var2)
 * ----------
 */
static int cmp_abs(NumericVar* var1, NumericVar* var2)
{
    return cmp_abs_common(var1->digits, var1->ndigits, var1->weight, var2->digits, var2->ndigits, var2->weight);
}

/* ----------
 * cmp_abs_common() -
 *
 *	Main routine of cmp_abs(). This function can be used by both
 *	NumericVar and Numeric.
 * ----------
 */
static int cmp_abs_common(const NumericDigit* var1digits, int var1ndigits, int var1weight,
    const NumericDigit* var2digits, int var2ndigits, int var2weight)
{
    int i1 = 0;
    int i2 = 0;

    /* Check any digits before the first common digit */

    while (var1weight > var2weight && i1 < var1ndigits) {
        if (var1digits[i1++] != 0)
            return 1;
        var1weight--;
    }
    while (var2weight > var1weight && i2 < var2ndigits) {
        if (var2digits[i2++] != 0)
            return -1;
        var2weight--;
    }

    /* At this point, either w1 == w2 or we've run out of digits */

    if (var1weight == var2weight) {
        while (i1 < var1ndigits && i2 < var2ndigits) {
            int stat = var1digits[i1++] - var2digits[i2++];

            if (stat) {
                if (stat > 0) {
                    return 1;
                }
                return -1;
            }
        }
    }

    /*
     * At this point, we've run out of digits on one side or the other; so any
     * remaining nonzero digits imply that side is larger
     */
    while (i1 < var1ndigits) {
        if (var1digits[i1++] != 0)
            return 1;
    }
    while (i2 < var2ndigits) {
        if (var2digits[i2++] != 0)
            return -1;
    }

    return 0;
}

/*
 * add_abs() -
 *
 *	Add the absolute values of two variables into result.
 *	result might point to one of the operands without danger.
 */
static void add_abs(NumericVar* var1, NumericVar* var2, NumericVar* result)
{
    NumericDigit* res_buf = NULL;
    NumericDigit* res_digits = NULL;
    int res_ndigits;
    int res_weight;
    int res_rscale, rscale1, rscale2;
    int res_dscale;
    int i, i1, i2;
    int carry = 0;

    /* copy these values into local vars for speed in inner loop */
    int var1ndigits = var1->ndigits;
    int var2ndigits = var2->ndigits;
    NumericDigit* var1digits = var1->digits;
    NumericDigit* var2digits = var2->digits;
    NumericDigit tdig[NUMERIC_LOCAL_NDIG];

    res_weight = Max(var1->weight, var2->weight) + 1;

    res_dscale = Max(var1->dscale, var2->dscale);

    /* Note: here we are figuring rscale in base-NBASE digits */
    rscale1 = var1->ndigits - var1->weight - 1;
    rscale2 = var2->ndigits - var2->weight - 1;
    res_rscale = Max(rscale1, rscale2);

    res_ndigits = res_rscale + res_weight + 1;
    if (res_ndigits <= 0) {
        res_ndigits = 1;
    }

    res_buf = tdig;
    if (res_ndigits > NUMERIC_LOCAL_NMAX) {
        res_buf = digitbuf_alloc(res_ndigits + 1);
    }
    res_buf[0] = 0; /* spare digit for later rounding */
    res_digits = res_buf + 1;

    i1 = res_rscale + var1->weight + 1;
    i2 = res_rscale + var2->weight + 1;
    for (i = res_ndigits - 1; i >= 0; i--) {
        i1--;
        i2--;
        if (i1 >= 0 && i1 < var1ndigits)
            carry += var1digits[i1];
        if (i2 >= 0 && i2 < var2ndigits)
            carry += var2digits[i2];

        if (carry >= NBASE) {
            res_digits[i] = carry - NBASE;
            carry = 1;
        } else {
            res_digits[i] = carry;
            carry = 0;
        }
    }

    Assert(carry == 0); /* else we failed to allow for carry out */

    digitbuf_free(result);
    result->ndigits = res_ndigits;
    if (res_buf != tdig) {
        result->buf = res_buf;
        result->digits = res_digits;
    } else {
        result->buf = result->ndb;
        result->digits = result->buf;
        errno_t rc = memcpy_s(result->buf, sizeof(NumericDigit) * (res_ndigits + 1),
                              res_buf, sizeof(NumericDigit) * (res_ndigits + 1));
        securec_check(rc, "\0", "\0");
        result->digits ++;
    }
    result->weight = res_weight;
    result->dscale = res_dscale;

    /* Remove leading/trailing zeroes */
    strip_var(result);
}

/*
 * sub_abs()
 *
 *	Subtract the absolute value of var2 from the absolute value of var1
 *	and store in result. result might point to one of the operands
 *	without danger.
 *
 *	ABS(var1) MUST BE GREATER OR EQUAL ABS(var2) !!!
 */
static void sub_abs(NumericVar* var1, NumericVar* var2, NumericVar* result)
{
    NumericDigit* res_buf = NULL;
    NumericDigit* res_digits = NULL;
    int res_ndigits;
    int res_weight;
    int res_rscale, rscale1, rscale2;
    int res_dscale;
    int i, i1, i2;
    int borrow = 0;

    /* copy these values into local vars for speed in inner loop */
    int var1ndigits = var1->ndigits;
    int var2ndigits = var2->ndigits;
    NumericDigit* var1digits = var1->digits;
    NumericDigit* var2digits = var2->digits;
    NumericDigit tdig[NUMERIC_LOCAL_NDIG];

    res_weight = var1->weight;

    res_dscale = Max(var1->dscale, var2->dscale);

    /* Note: here we are figuring rscale in base-NBASE digits */
    rscale1 = var1->ndigits - var1->weight - 1;
    rscale2 = var2->ndigits - var2->weight - 1;
    res_rscale = Max(rscale1, rscale2);

    res_ndigits = res_rscale + res_weight + 1;
    if (res_ndigits <= 0) {
        res_ndigits = 1;
    }

    res_buf = tdig;
    if (res_ndigits > NUMERIC_LOCAL_NMAX) {
        res_buf = digitbuf_alloc(res_ndigits + 1);
    }
    res_buf[0] = 0; /* spare digit for later rounding */
    res_digits = res_buf + 1;

    i1 = res_rscale + var1->weight + 1;
    i2 = res_rscale + var2->weight + 1;
    for (i = res_ndigits - 1; i >= 0; i--) {
        i1--;
        i2--;
        if (i1 >= 0 && i1 < var1ndigits)
            borrow += var1digits[i1];
        if (i2 >= 0 && i2 < var2ndigits)
            borrow -= var2digits[i2];

        if (borrow < 0) {
            res_digits[i] = borrow + NBASE;
            borrow = -1;
        } else {
            res_digits[i] = borrow;
            borrow = 0;
        }
    }

    Assert(borrow == 0); /* else caller gave us var1 < var2 */

    digitbuf_free(result);
    result->ndigits = res_ndigits;
    if (res_buf != tdig)
    {
        result->buf = res_buf;
        result->digits = res_digits;
    } else {
        result->buf = result->ndb;
        result->digits = result->buf;
        errno_t rc = memcpy_s(result->buf, sizeof(NumericDigit) * (res_ndigits + 1),
                              res_buf, sizeof(NumericDigit) * (res_ndigits + 1));
        securec_check(rc, "\0", "\0");
        result->digits ++;
    }
    result->weight = res_weight;
    result->dscale = res_dscale;

    /* Remove leading/trailing zeroes */
    strip_var(result);
}

/*
 * round_var
 *
 * Round the value of a variable to no more than rscale decimal digits
 * after the decimal point.  NOTE: we allow rscale < 0 here, implying
 * rounding before the decimal point.
 */
static void round_var(NumericVar* var, int rscale)
{
    NumericDigit* digits = var->digits;
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
            }
            else {
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
 * trunc_var
 *
 * Truncate (towards zero) the value of a variable at rscale decimal digits
 * after the decimal point.  NOTE: we allow rscale < 0 here, implying
 * truncation before the decimal point.
 */
static void trunc_var(NumericVar* var, int rscale)
{
    int di;
    int ndigits;

    var->dscale = rscale;

    /* decimal digits wanted */
    di = (var->weight + 1) * DEC_DIGITS + rscale;

    /*
     * If di <= 0, the value loses all digits.
     */
    if (di <= 0) {
        var->ndigits = 0;
        var->weight = 0;
        var->sign = NUMERIC_POS;
    } else {
        /* NBASE digits wanted */
        ndigits = (di + DEC_DIGITS - 1) / DEC_DIGITS;

        if (ndigits <= var->ndigits) {
            var->ndigits = ndigits;

#if DEC_DIGITS == 1
            /* no within-digit stuff to worry about */
#else
            /* 0, or number of decimal digits to keep in last NBASE digit */
            di %= DEC_DIGITS;

            if (di > 0) {
                /* Must truncate within last NBASE digit */
                NumericDigit* digits = var->digits;
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
            }
#endif
        }
    }
}

/*
 * strip_var
 *
 * Strip any leading and trailing zeroes from a numeric variable
 */
static void strip_var(NumericVar* var)
{
    NumericDigit* digits = var->digits;
    int ndigits = var->ndigits;

    /* Strip leading zeroes */
    while (ndigits > 0 && *digits == 0) {
        digits++;
        var->weight--;
        ndigits--;
    }

    /* Strip trailing zeroes */
    while (ndigits > 0 && digits[ndigits - 1] == 0)
        ndigits--;

    /* If it's zero, normalize the sign and weight */
    if (ndigits == 0) {
        var->sign = NUMERIC_POS;
        var->weight = 0;
    }

    var->digits = digits;
    var->ndigits = ndigits;
}

#ifdef PGXC
Datum numeric_collect(PG_FUNCTION_ARGS)
{
    ArrayType* collectarray = PG_GETARG_ARRAYTYPE_P(0);
    ArrayType* transarray = PG_GETARG_ARRAYTYPE_P(1);
    Datum* collectdatums = NULL;
    Datum* transdatums = NULL;
    int ndatums;
    Datum N, sumX, sumX2;

    /* We assume the input is array of numeric */
    deconstruct_array(collectarray, NUMERICOID, -1, false, 'i', &collectdatums, NULL, &ndatums);
    if (ndatums != 3)
        ereport(ERROR, (errcode(ERRCODE_ARRAY_ELEMENT_ERROR), errmsg("expected 3-element numeric array")));
    N = collectdatums[0];
    sumX = collectdatums[1];
    sumX2 = collectdatums[2];

    /* We assume the input is array of numeric */
    deconstruct_array(transarray, NUMERICOID, -1, false, 'i', &transdatums, NULL, &ndatums);
    if (ndatums != 3)
        ereport(ERROR, (errcode(ERRCODE_ARRAY_ELEMENT_ERROR), errmsg("expected 3-element numeric array")));

    N = DirectFunctionCall2(numeric_add, N, transdatums[0]);
    sumX = DirectFunctionCall2(numeric_add, sumX, transdatums[1]);
    sumX2 = DirectFunctionCall2(numeric_add, sumX2, transdatums[2]);

    collectdatums[0] = N;
    collectdatums[1] = sumX;
    collectdatums[2] = sumX2;

    PG_RETURN_ARRAYTYPE_P(construct_array(collectdatums, 3, NUMERICOID, -1, false, 'i'));
}

Datum numeric_avg_collect(PG_FUNCTION_ARGS)
{
    ArrayType* collectarray = PG_GETARG_ARRAYTYPE_P(0);
    ArrayType* transarray = PG_GETARG_ARRAYTYPE_P(1);
    Datum* collectdatums = NULL;
    Datum* transdatums = NULL;
    int ndatums;
    Datum N, sumX;

    /* We assume the input is array of numeric */
    deconstruct_array(collectarray, NUMERICOID, -1, false, 'i', &collectdatums, NULL, &ndatums);
    if (ndatums != 2)
        ereport(ERROR, (errcode(ERRCODE_ARRAY_ELEMENT_ERROR), errmsg("expected 2-element numeric array")));
    N = collectdatums[0];
    sumX = collectdatums[1];

    /* We assume the input is array of numeric */
    deconstruct_array(transarray, NUMERICOID, -1, false, 'i', &transdatums, NULL, &ndatums);
    if (ndatums != 2)
        ereport(ERROR, (errcode(ERRCODE_ARRAY_ELEMENT_ERROR), errmsg("expected 2-element numeric array")));

    N = DirectFunctionCall2(numeric_add, N, transdatums[0]);
    sumX = DirectFunctionCall2(numeric_add, sumX, transdatums[1]);

    collectdatums[0] = N;
    collectdatums[1] = sumX;

    PG_RETURN_ARRAYTYPE_P(construct_array(collectdatums, 2, NUMERICOID, -1, false, 'i'));
}

Datum int8_avg_collect(PG_FUNCTION_ARGS)
{
    ArrayType* collectarray = NULL;
    ArrayType* transarray = PG_GETARG_ARRAYTYPE_P(1);
    Int8TransTypeData* collectdata = NULL;
    Int8TransTypeData* transdata = NULL;

    /*
     * If we're invoked by nodeAgg, we can cheat and modify our first
     * parameter in-place to reduce palloc overhead. Otherwise we need to make
     * a copy of it before scribbling on it.
     */
    if (fcinfo->context && (IsA(fcinfo->context, AggState) || IsA(fcinfo->context, WindowAggState)))
        collectarray = PG_GETARG_ARRAYTYPE_P(0);
    else
        collectarray = PG_GETARG_ARRAYTYPE_P_COPY(0);

    if (ARR_HASNULL(collectarray) || ARR_SIZE(collectarray) != ARR_OVERHEAD_NONULLS(1) + sizeof(Int8TransTypeData))
        ereport(ERROR, (errcode(ERRCODE_ARRAY_ELEMENT_ERROR), errmsg("expected 2-element int8 array")));
    collectdata = (Int8TransTypeData*)ARR_DATA_PTR(collectarray);

    if (ARR_HASNULL(transarray) || ARR_SIZE(transarray) != ARR_OVERHEAD_NONULLS(1) + sizeof(Int8TransTypeData))
        ereport(ERROR, (errcode(ERRCODE_ARRAY_ELEMENT_ERROR), errmsg("expected 2-element int8 array")));
    transdata = (Int8TransTypeData*)ARR_DATA_PTR(transarray);

    collectdata->count += transdata->count;
    collectdata->sum += transdata->sum;

    PG_RETURN_ARRAYTYPE_P(collectarray);
}
#endif

/* Convert numeric to interval with format*/
Datum numtodsinterval(PG_FUNCTION_ARGS)
{
    Datum num = PG_GETARG_DATUM(0);
    Datum fmt = PG_GETARG_DATUM(1);
    Oid collation = PG_GET_COLLATION();
    Datum result;
    errno_t errorno = 0;
    int str_len;
    char* buf = NULL;
    char* cp = NULL;

    CHECK_RETNULL_INIT();

    StringInfoData str;
    initStringInfo(&str);
    appendStringInfoString(&str, DatumGetCString(CHECK_RETNULL_CALL1(numeric_out_with_zero, collation, num)));
    appendStringInfoString(&str, " ");
    appendStringInfoString(&str, TextDatumGetCString(fmt));
    cp = str.data;

    if (*cp == '.' || (*cp == '-' && *(cp + 1) == '.')) {
        str_len = str.len + 2;
        buf = (char*)palloc0(str_len);
        if (*cp == '.') {
            errorno = snprintf_s(buf, str_len, str_len - 1, "0.%s", cp + 1);
        } else {
            errorno = snprintf_s(buf, str_len, str_len - 1, "-0.%s", cp + 2);
        }

        securec_check_ss(errorno, "\0", "\0");
        resetStringInfo(&str);
        appendStringInfoString(&str, buf);
        pfree_ext(buf);
    }

    result = CHECK_RETNULL_CALL3(
        interval_in, collation, CStringGetDatum(str.data), ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));
    pfree_ext(str.data);
    CHECK_RETNULL_RETURN_DATUM(result);
}

/* Convert numeric to interval */
Datum numeric_interval(PG_FUNCTION_ARGS)
{
    Datum num = PG_GETARG_DATUM(0);
    const char* fmt = "day";
    Oid collation = PG_GET_COLLATION();
    Datum result;
    StringInfoData str;
    errno_t errorno = 0;
    int str_len;
    char* buf = NULL;
    char* cp = NULL;

    CHECK_RETNULL_INIT();

    initStringInfo(&str);
    appendStringInfoString(&str, DatumGetCString(CHECK_RETNULL_CALL1(numeric_out_with_zero, collation, num)));
    appendStringInfoString(&str, " ");
    appendStringInfoString(&str, fmt);
    cp = str.data;

    if (*cp == '.' || (*cp == '-' && *(cp + 1) == '.')) {
        str_len = str.len + 2;
        buf = (char*)palloc0(str_len);
        if (*cp == '.') {
            errorno = snprintf_s(buf, str_len, str_len - 1, "0.%s", cp + 1);
        } else {
            errorno = snprintf_s(buf, str_len, str_len - 1, "-0.%s", cp + 2);
        }
        securec_check_ss(errorno, "\0", "\0");
        resetStringInfo(&str);
        appendStringInfoString(&str, buf);
        pfree_ext(buf);
    }

    result = CHECK_RETNULL_CALL3(
        interval_in, collation, CStringGetDatum(str.data), ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));
    pfree_ext(str.data);
    CHECK_RETNULL_RETURN_DATUM(result);
}

/* Convert numeric to interval by typmod */
Datum numeric_to_interval(PG_FUNCTION_ARGS)
{
    Datum num = PG_GETARG_DATUM(0);
    int32 typmod = PG_GETARG_INT32(1);
    Oid collation = PG_GET_COLLATION();
    Datum result;
    StringInfoData str;
    errno_t errorno = 0;
    int str_len;
    char* buf = NULL;
    char* cp = NULL;

    CHECK_RETNULL_INIT();

    initStringInfo(&str);
    appendStringInfoString(&str, DatumGetCString(CHECK_RETNULL_CALL1(numeric_out_with_zero, collation, num)));
    cp = str.data;

    if (*cp == '.' || (*cp == '-' && *(cp + 1) == '.')) {
        str_len = str.len + 2;
        buf = (char*)palloc0(str_len);
        if (*cp == '.') {
            errorno = snprintf_s(buf, str_len, str_len - 1, "0.%s", cp + 1);
        } else {
            errorno = snprintf_s(buf, str_len, str_len - 1, "-0.%s", cp + 2);
        }
        securec_check_ss(errorno, "\0", "\0");
        resetStringInfo(&str);
        appendStringInfoString(&str, buf);
        pfree_ext(buf);
    }

    result = CHECK_RETNULL_CALL3(
        interval_in, collation, CStringGetDatum(str.data), ObjectIdGetDatum(InvalidOid), Int32GetDatum(typmod));
    pfree_ext(str.data);
    CHECK_RETNULL_RETURN_DATUM(result);
}

ScalarVector* vnumeric_sum(PG_FUNCTION_ARGS)
{
    ScalarVector* pVector = (ScalarVector*)PG_GETARG_DATUM(0);
    int idx = PG_GETARG_DATUM(1);
    hashCell** loc = (hashCell**)PG_GETARG_DATUM(2);
    MemoryContext context = (MemoryContext)PG_GETARG_DATUM(3);

    hashCell* cell = NULL;
    ScalarValue* pVal = pVector->m_vals;
    uint8* flag = pVector->m_flag;
    int nrows = pVector->m_rows;
    Datum args[2];
    Datum result;
    FunctionCallInfoData finfo;
    bictl ctl;
    Numeric leftarg, rightarg;    // left-hand and right-hand operand of addition
    uint16 num1Flags, num2Flags;  // numeric flags of num1 and num2
    int arg1, arg2, i;

    finfo.arg = &args[0];
    ctl.context = context;

    for (i = 0; i < nrows; i++) {
        cell = loc[i];
        if (cell && IS_NULL(flag[i]) == false)  // only do when not null
        {
            if (NOT_NULL(cell->m_val[idx].flag)) {
                leftarg = (Numeric)(cell->m_val[idx].val);
                rightarg = DatumGetBINumeric(pVal[i]);
                num1Flags = NUMERIC_NB_FLAGBITS(leftarg);
                num2Flags = NUMERIC_NB_FLAGBITS(rightarg);

                if (likely(NUMERIC_FLAG_IS_BI(num1Flags) && NUMERIC_FLAG_IS_BI(num2Flags))) {
                    arg1 = NUMERIC_FLAG_IS_BI128(num1Flags);
                    arg2 = NUMERIC_FLAG_IS_BI128(num2Flags);
                    ctl.store_pos = cell->m_val[idx].val;
                    // call big integer fast add function
                    (BiAggFunMatrix[BI_AGG_ADD][arg1][arg2])(leftarg, rightarg, &ctl);
                    // ctl.store_pos may be pointed to new address.
                    cell->m_val[idx].val = ctl.store_pos;
                } else { // call numeric_add
                    args[0] = NumericGetDatum(leftarg);
                    args[1] = NumericGetDatum(rightarg);
                    result = numeric_add(&finfo);
                    cell->m_val[idx].val = replaceVariable(context, cell->m_val[idx].val, result);
                }
                SET_NOTNULL(cell->m_val[idx].flag);
            } else {
                /* make sure cell->m_val[idx].val is 4 bytes header */
                leftarg = DatumGetBINumeric(pVal[i]);
                cell->m_val[idx].val = addVariable(context, NumericGetDatum(leftarg));
                SET_NOTNULL(cell->m_val[idx].flag);
            }
        }
    }
    return NULL;
}

/*
 * @Description	: sum(numeric) function.
 *
 * @in m_loc		: location in hash table.
 * @in pVal		: vector to be calculated.
 * @out m_data[idx]	: value of sum(numeric) for each group.
 * @out m_data[idx + 1]	: number of sum(numeric) for each group.
 */
ScalarVector* vsnumeric_sum(PG_FUNCTION_ARGS)
{
    ScalarVector* pVector = (ScalarVector*)PG_GETARG_DATUM(0);
    int idx = (int)PG_GETARG_DATUM(1);
    uint32* loc = (uint32*)PG_GETARG_DATUM(2);
    SonicDatumArray** sdata = (SonicDatumArray**)PG_GETARG_DATUM(3);

    SonicEncodingDatumArray* data = (SonicEncodingDatumArray*)sdata[idx];
    ScalarValue* pVal = pVector->m_vals;
    uint8* flag = pVector->m_flag;
    int nrows = pVector->m_rows;
    Datum args[2];
    Datum result;
    FunctionCallInfoData finfo;

    Datum* leftdata = NULL;
    uint8 leftflag;
    bictl ctl;
    /* left-hand and right-hand operand of addition */
    Numeric leftarg, rightarg;
    /* numeric flags of num1 and num2 */
    uint16 num1Flags, num2Flags;
    int arg1, arg2, i;
    int arrIndx, atomIndx;

    finfo.arg = &args[0];
    ctl.context = data->m_cxt;

    for (i = 0; i < nrows; i++) {
        /* only consider not null numeric value */
        if ((loc[i] != 0) && NOT_NULL(flag[i])) {
            /* get atom location in *data* array */
            arrIndx = getArrayIndx(loc[i], data->m_nbit);
            atomIndx = getArrayLoc(loc[i], data->m_atomSize - 1);

            /* get previous sum result flag */
            leftflag = data->getNthNullFlag(arrIndx, atomIndx);

            if (NOT_NULL(leftflag)) {
                /* previous sum result(leftdata) */
                leftdata = &((Datum*)data->m_arr[arrIndx]->data)[atomIndx];

                /* updata previous sum result based on the given pVal[i] */
                leftarg = DatumGetBINumeric(leftdata[0]);

                rightarg = DatumGetBINumeric(pVal[i]);
                num1Flags = NUMERIC_NB_FLAGBITS(leftarg);
                num2Flags = NUMERIC_NB_FLAGBITS(rightarg);

                if (likely(NUMERIC_FLAG_IS_BI(num1Flags) && NUMERIC_FLAG_IS_BI(num2Flags))) {
                    arg1 = NUMERIC_FLAG_IS_BI128(num1Flags);
                    arg2 = NUMERIC_FLAG_IS_BI128(num2Flags);
                    ctl.store_pos = leftdata[0];
                    /* call big integer fast add function */
                    (BiAggFunMatrix[BI_AGG_ADD][arg1][arg2])(leftarg, rightarg, &ctl);
                    leftdata[0] = ctl.store_pos;
                } else {
                    /* call numeric_add */
                    args[0] = NumericGetDatum(leftarg);
                    args[1] = NumericGetDatum(rightarg);
                    result = numeric_add(&finfo);
                    /* update sum result */
                    leftdata[0] = data->replaceVariable(leftdata[0], result);
                }
            } else {
                leftarg = DatumGetBINumeric(pVal[i]);
                /* initialize sum result */
                data->setValue(NumericGetDatum(leftarg), false, arrIndx, atomIndx);
            }
        }
    }
    return NULL;
}

ScalarVector* vnumeric_abs(PG_FUNCTION_ARGS)
{
    ScalarValue* parg1 = PG_GETARG_VECVAL(0);
    ScalarVector* pvector1 = PG_GETARG_VECTOR(0);
    int32 nvalues = PG_GETARG_INT32(1);
    ScalarValue* presult = PG_GETARG_VECVAL(2);
    ScalarVector* presultVector = PG_GETARG_VECTOR(2);
    uint8* pflagsRes = (uint8*)(presultVector->m_flag);
    bool* pselection = PG_GETARG_SELECTION(3);
    Datum args;
    int i;

    FunctionCallInfoData finfo;

    finfo.arg = &args;

    if (pselection != NULL) {
        for (i = 0; i < nvalues; i++) {
            if (pvector1->IsNull(i)) {
                SET_NULL(pflagsRes[i]);
                continue;
            }

            if (pselection[i]) {
                args = ScalarVector::Decode(parg1[i]);
                presult[i] = numeric_abs(&finfo);
                SET_NOTNULL(pflagsRes[i]);
            }
        }
    } else {
        for (i = 0; i < nvalues; i++) {
            if (pvector1->IsNull(i)) {
                SET_NULL(pflagsRes[i]);
                continue;
            }

            args = ScalarVector::Decode(parg1[i]);
            presult[i] = numeric_abs(&finfo);
            SET_NOTNULL(pflagsRes[i]);
        }
    }

    PG_GETARG_VECTOR(2)->m_rows = nvalues;
    return PG_GETARG_VECTOR(2);
}

ScalarVector* vnumeric_fac(PG_FUNCTION_ARGS)
{
    ScalarVector* pvector1 = PG_GETARG_VECTOR(0);
    ScalarValue* pVal = pvector1->m_vals;
    int32 nvalues = PG_GETARG_INT32(1);
    ScalarValue* presult = PG_GETARG_VECVAL(2);
    ScalarVector* presultVector = PG_GETARG_VECTOR(2);
    uint8* pflagsRes = (uint8*)(presultVector->m_flag);
    bool* pselection = PG_GETARG_SELECTION(3);
    Datum args;
    int i;

    FunctionCallInfoData finfo;

    finfo.arg = &args;

    if (pselection != NULL) {
        for (i = 0; i < nvalues; i++) {
            if (pvector1->IsNull(i)) {
                SET_NULL(pflagsRes[i]);
                continue;
            }

            if (pselection[i]) {
                args = pVal[i];
                presult[i] = numeric_fac(&finfo);
                SET_NOTNULL(pflagsRes[i]);
            }
        }
    } else {
        for (i = 0; i < nvalues; i++) {
            if (pvector1->IsNull(i)) {
                SET_NULL(pflagsRes[i]);
                continue;
            }

            args = pVal[i];
            presult[i] = numeric_fac(&finfo);
            SET_NOTNULL(pflagsRes[i]);
        }
    }

    PG_GETARG_VECTOR(2)->m_rows = nvalues;
    return PG_GETARG_VECTOR(2);
}

/*
 * The internal realization of function vnumeric_ne.
 */
template <bool m_const1, bool m_const2>
static void vnumeric_ne_internal(ScalarVector* arg1, uint8* pflags1, ScalarVector* arg2, uint8* pflags2,
    ScalarVector* vresult, uint8* pflagRes, Numeric num, NumericDigit* vardigits, int varndigits, int varweight,
    int varsign, bool is_nan, int idx)
{
    int result;

    if (BOTH_NOT_NULL(pflags1[idx], pflags2[idx])) {
        Numeric num1 = m_const1 ? num : DatumGetNumeric(arg1->m_vals[idx]);
        Numeric num2 = m_const2 ? num : DatumGetNumeric(arg2->m_vals[idx]);
        bool is_nan1 = m_const1 ? is_nan : NUMERIC_IS_NAN(num1);
        bool is_nan2 = m_const2 ? is_nan : NUMERIC_IS_NAN(num2);
        if (is_nan1) {
            result = is_nan2 ? 0 : 1;
        } else if (is_nan2) {
            result = -1;
        } else {
            NumericDigit* vardigits1 = m_const1 ? vardigits : NUMERIC_DIGITS(num1);
            NumericDigit* vardigits2 = m_const2 ? vardigits : NUMERIC_DIGITS(num2);
            int varndigits1 = m_const1 ? varndigits : NUMERIC_NDIGITS(num1);
            int varndigits2 = m_const2 ? varndigits : NUMERIC_NDIGITS(num2);
            int varweight1 = m_const1 ? varweight : NUMERIC_WEIGHT(num1);
            int varweight2 = m_const2 ? varweight : NUMERIC_WEIGHT(num2);
            int varsign1 = m_const1 ? varsign : NUMERIC_SIGN(num1);
            int varsign2 = m_const2 ? varsign : NUMERIC_SIGN(num2);
            result = cmp_var_common(
                vardigits1, varndigits1, varweight1, varsign1, vardigits2, varndigits2, varweight2, varsign2);
        }
        vresult->m_vals[idx] = BoolGetDatum(result);
        SET_NOTNULL(pflagRes[idx]);
    } else
        SET_NULL(pflagRes[idx]);
}

ScalarVector* vnumeric_ne(PG_FUNCTION_ARGS)
{
    ScalarVector* arg1 = PG_GETARG_VECTOR(0);
    ScalarVector* arg2 = PG_GETARG_VECTOR(1);
    uint8* pflags1 = arg1->m_flag;
    uint8* pflags2 = arg2->m_flag;
    int32 nvalues = PG_GETARG_INT32(2);
    ScalarVector* vresult = PG_GETARG_VECTOR(3);
    uint8* pflagRes = (uint8*)(vresult->m_flag);
    bool* pselection = PG_GETARG_SELECTION(4);
    int k;
    Numeric num = NULL;
    NumericDigit* vardigits = NULL;
    int varndigits = 0;
    int varweight = 0;
    int varsign = 0;
    bool is_nan = 0;

    /*
     * Since if both arg1->m_const and arg2->m_const are true,
     * we could never enter here. In the following, we only
     * need to consider there cases. When one of the vector
     * is a const vector, some parameters that we need in
     * cmp_var_common could be predefined.
     */
    if (arg1->m_const && NOT_NULL(pflags1[0])) {
        num = DatumGetNumeric(arg1->m_vals[0]);
        vardigits = NUMERIC_DIGITS(num);
        varndigits = NUMERIC_NDIGITS(num);
        varweight = NUMERIC_WEIGHT(num);
        varsign = NUMERIC_SIGN(num);
        is_nan = NUMERIC_IS_NAN(num) ? true : false;
    } else if (arg2->m_const && NOT_NULL(pflags2[0])) {
        num = DatumGetNumeric(arg2->m_vals[0]);
        vardigits = NUMERIC_DIGITS(num);
        varndigits = NUMERIC_NDIGITS(num);
        varweight = NUMERIC_WEIGHT(num);
        varsign = NUMERIC_SIGN(num);
        is_nan = NUMERIC_IS_NAN(num) ? true : false;
    }

    if (pselection != NULL) {
        for (k = 0; k < nvalues; k++) {
            if (pselection[k]) {
                if (!arg1->m_const && arg2->m_const)
                    vnumeric_ne_internal<false, true>(arg1,
                        pflags1,
                        arg2,
                        pflags2,
                        vresult,
                        pflagRes,
                        num,
                        vardigits,
                        varndigits,
                        varweight,
                        varsign,
                        is_nan,
                        k);
                else if (arg1->m_const && !arg2->m_const)
                    vnumeric_ne_internal<true, false>(arg1,
                        pflags1,
                        arg2,
                        pflags2,
                        vresult,
                        pflagRes,
                        num,
                        vardigits,
                        varndigits,
                        varweight,
                        varsign,
                        is_nan,
                        k);
                else
                    vnumeric_ne_internal<false, false>(arg1,
                        pflags1,
                        arg2,
                        pflags2,
                        vresult,
                        pflagRes,
                        num,
                        vardigits,
                        varndigits,
                        varweight,
                        varsign,
                        is_nan,
                        k);
            }
        }
    } else {
        for (k = 0; k < nvalues; k++) {
            if (!arg1->m_const && arg2->m_const)
                vnumeric_ne_internal<false, true>(arg1,
                    pflags1,
                    arg2,
                    pflags2,
                    vresult,
                    pflagRes,
                    num,
                    vardigits,
                    varndigits,
                    varweight,
                    varsign,
                    is_nan,
                    k);
            else if (arg1->m_const && !arg2->m_const)
                vnumeric_ne_internal<true, false>(arg1,
                    pflags1,
                    arg2,
                    pflags2,
                    vresult,
                    pflagRes,
                    num,
                    vardigits,
                    varndigits,
                    varweight,
                    varsign,
                    is_nan,
                    k);
            else
                vnumeric_ne_internal<false, false>(arg1,
                    pflags1,
                    arg2,
                    pflags2,
                    vresult,
                    pflagRes,
                    num,
                    vardigits,
                    varndigits,
                    varweight,
                    varsign,
                    is_nan,
                    k);
        }
    }

    PG_GETARG_VECTOR(3)->m_rows = nvalues;

    return PG_GETARG_VECTOR(3);
}

ScalarVector* vinterval_sum(PG_FUNCTION_ARGS)
{
    ScalarVector* pVector = (ScalarVector*)PG_GETARG_DATUM(0);
    int idx = PG_GETARG_DATUM(1);
    hashCell** loc = (hashCell**)PG_GETARG_DATUM(2);

    MemoryContext context = (MemoryContext)PG_GETARG_DATUM(3);

    hashCell* cell = NULL;
    int i;
    ScalarValue* pVal = pVector->m_vals;
    uint8* flag = pVector->m_flag;
    int nrows = pVector->m_rows;
    Datum args[2];
    Datum result;
    FunctionCallInfoData finfo;

    finfo.arg = &args[0];

    for (i = 0; i < nrows; i++) {
        cell = loc[i];
        if (cell && IS_NULL(flag[i]) == false)  // only do when not null
        {
            if (IS_NULL(cell->m_val[idx].flag)) {
                cell->m_val[idx].val = addVariable(context, pVal[i]);
                SET_NOTNULL(cell->m_val[idx].flag);
            } else {
                args[0] = PointerGetDatum((char*)cell->m_val[idx].val + VARHDRSZ_SHORT);
                args[1] = PointerGetDatum((char*)pVal[i] + VARHDRSZ_SHORT);
                result = interval_pl(&finfo);
                cell->m_val[idx].val = replaceVariable(
                    context, cell->m_val[idx].val, ScalarVector::DatumToScalar(result, INTERVALOID, false));
            }
        }
    }

    return NULL;
}

ScalarVector* vcash_sum(PG_FUNCTION_ARGS)
{
    ScalarVector* pVector = (ScalarVector*)PG_GETARG_DATUM(0);
    int idx = PG_GETARG_DATUM(1);
    hashCell** loc = (hashCell**)PG_GETARG_DATUM(2);
    hashCell* cell = NULL;
    int i;
    ScalarValue* pVal = pVector->m_vals;
    uint8* flag = pVector->m_flag;
    int nrows = pVector->m_rows;
    Datum args[2];
    Datum result;

    for (i = 0; i < nrows; i++) {
        cell = loc[i];
        if (cell && IS_NULL(flag[i]) == false)  // only do when not null
        {
            if (IS_NULL(cell->m_val[idx].flag)) {
                cell->m_val[idx].val = pVal[i];

                SET_NOTNULL(cell->m_val[idx].flag);
            } else {
                args[0] = cell->m_val[idx].val;
                args[1] = pVal[i];
                result = args[0] + args[1];

                cell->m_val[idx].val = result;
            }
        }
    }
    return NULL;
}

ScalarVector* vintervalpl(PG_FUNCTION_ARGS)
{
    ScalarValue* parg1 = PG_GETARG_VECVAL(0);
    ScalarValue* parg2 = PG_GETARG_VECVAL(1);
    int32 nvalues = PG_GETARG_INT32(2);
    ScalarValue* presult = PG_GETARG_VECVAL(3);
    bool* pselection = PG_GETARG_SELECTION(4);
    uint8* pflags1 = (uint8*)(PG_GETARG_VECTOR(0)->m_flag);
    uint8* pflags2 = (uint8*)(PG_GETARG_VECTOR(1)->m_flag);
    uint8* pflagsRes = (uint8*)(PG_GETARG_VECTOR(3)->m_flag);
    int i;
    Datum args[2];
    FunctionCallInfoData finfo;
    Datum result;
    finfo.arg = &args[0];

    if (likely(pselection == NULL)) {
        for (i = 0; i < nvalues; i++) {
            if (BOTH_NOT_NULL(pflags1[i], pflags2[i])) {
                args[0] = PointerGetDatum((char*)parg1[i] + VARHDRSZ_SHORT);
                args[1] = PointerGetDatum((char*)parg2[i] + VARHDRSZ_SHORT);

                result = interval_pl(&finfo);

                presult[i] = ScalarVector::DatumToScalar(result, INTERVALOID, false);
                SET_NOTNULL(pflagsRes[i]);
            } else
                SET_NULL(pflagsRes[i]);
        }
    } else {
        for (i = 0; i < nvalues; i++) {
            if (pselection[i]) {
                if (BOTH_NOT_NULL(pflags1[i], pflags2[i])) {
                    args[0] = PointerGetDatum((char*)parg1[i] + VARHDRSZ_SHORT);
                    args[1] = PointerGetDatum((char*)parg2[i] + VARHDRSZ_SHORT);

                    result = interval_pl(&finfo);
                    presult[i] = ScalarVector::DatumToScalar(result, INTERVALOID, false);
                    SET_NOTNULL(pflagsRes[i]);
                } else
                    SET_NULL(pflagsRes[i]);
            }
        }
    }

    PG_GETARG_VECTOR(3)->m_rows = nvalues;
    PG_GETARG_VECTOR(3)->m_desc.typeId = INTERVALOID;
    return PG_GETARG_VECTOR(3);
}

Datum numeric_text(PG_FUNCTION_ARGS)
{
    Numeric num = PG_GETARG_NUMERIC(0);
    char* tmp = NULL;
    Datum result;

    /* Handle Big Integer */
    if (NUMERIC_IS_BI(num)) {
        num = makeNumericNormal(num);
    }
    tmp = DatumGetCString(DirectFunctionCall1(numeric_out, NumericGetDatum(num)));
    result = DirectFunctionCall1(textin, CStringGetDatum(tmp));
    pfree_ext(tmp);

    PG_RETURN_DATUM(result);
}

Datum bpchar_numeric(PG_FUNCTION_ARGS)
{
    Datum bpcharValue = PG_GETARG_DATUM(0);
    char* tmp = NULL;
    Datum result;
    tmp = DatumGetCString(DirectFunctionCall1(bpcharout, bpcharValue));

    result = DirectFunctionCall3(numeric_in, CStringGetDatum(tmp), ObjectIdGetDatum(0), Int32GetDatum(-1));
    pfree_ext(tmp);

    PG_RETURN_DATUM(result);
}

Datum varchar_numeric(PG_FUNCTION_ARGS)
{
    Datum txt = PG_GETARG_DATUM(0);
    char* tmp = NULL;
    Datum result;
    tmp = DatumGetCString(DirectFunctionCall1(varcharout, txt));

    result = DirectFunctionCall3(numeric_in, CStringGetDatum(tmp), ObjectIdGetDatum(0), Int32GetDatum(-1));
    pfree_ext(tmp);

    PG_RETURN_DATUM(result);
}

Datum numeric_varchar(PG_FUNCTION_ARGS)
{
    Numeric num = PG_GETARG_NUMERIC(0);
    char* tmp = NULL;
    Datum result;

    /* Handle Big Integer */
    if (NUMERIC_IS_BI(num)) {
        num = makeNumericNormal(num);
    }
    tmp = DatumGetCString(DirectFunctionCall1(numeric_out, NumericGetDatum(num)));

    result = DirectFunctionCall3(varcharin, CStringGetDatum(tmp), ObjectIdGetDatum(0), Int32GetDatum(-1));
    pfree_ext(tmp);

    PG_RETURN_DATUM(result);
}

/*
 * @Description: numeric convert to bpchar.
 * @in arg1 - numeric convert to bpchar.
 * @return bpchar type string.
 */
Datum numeric_bpchar(PG_FUNCTION_ARGS)
{
    Numeric arg1 = PG_GETARG_NUMERIC(0);
    char* tmp = NULL;
    Datum result;

    /* Handle Big Integer */
    if (NUMERIC_IS_BI(arg1)) {
        arg1 = makeNumericNormal(arg1);
    }

    tmp = DatumGetCString(DirectFunctionCall1(numeric_out, NumericGetDatum(arg1)));

    result = DirectFunctionCall3(bpcharin, CStringGetDatum(tmp), ObjectIdGetDatum(0), Int32GetDatum(-1));
    pfree_ext(tmp);

    PG_RETURN_DATUM(result);
}

Datum text_float4(PG_FUNCTION_ARGS)
{
    Datum txt = PG_GETARG_DATUM(0);
    char* tmp = NULL;
    Datum result;
    tmp = DatumGetCString(DirectFunctionCall1(textout, txt));

    result = DirectFunctionCall1(float4in, CStringGetDatum(tmp));
    pfree_ext(tmp);

    PG_RETURN_DATUM(result);
}

Datum text_float8(PG_FUNCTION_ARGS)
{
    Datum txt = PG_GETARG_DATUM(0);
    char* tmp = NULL;
    Datum result;
    tmp = DatumGetCString(DirectFunctionCall1(textout, txt));

    result = DirectFunctionCall1(float8in, CStringGetDatum(tmp));
    pfree_ext(tmp);

    PG_RETURN_DATUM(result);
}

Datum text_numeric(PG_FUNCTION_ARGS)
{
    Datum txt = PG_GETARG_DATUM(0);
    char* tmp = NULL;
    Datum result;
    tmp = DatumGetCString(DirectFunctionCall1(textout, txt));

    result = DirectFunctionCall3(numeric_in, CStringGetDatum(tmp), ObjectIdGetDatum(0), Int32GetDatum(-1));
    pfree_ext(tmp);

    PG_RETURN_DATUM(result);
}

Datum bpchar_float4(PG_FUNCTION_ARGS)
{
    Datum bpcharValue = PG_GETARG_DATUM(0);
    char* tmp = NULL;
    Datum result;
    tmp = DatumGetCString(DirectFunctionCall1(bpcharout, bpcharValue));

    result = DirectFunctionCall1(float4in, CStringGetDatum(tmp));
    pfree_ext(tmp);

    PG_RETURN_DATUM(result);
}

Datum bpchar_float8(PG_FUNCTION_ARGS)
{
    Datum bpcharValue = PG_GETARG_DATUM(0);
    char* tmp = NULL;
    Datum result;
    tmp = DatumGetCString(DirectFunctionCall1(bpcharout, bpcharValue));

    result = DirectFunctionCall1(float8in, CStringGetDatum(tmp));
    pfree_ext(tmp);

    PG_RETURN_DATUM(result);
}

Datum varchar_float4(PG_FUNCTION_ARGS)
{
    Datum varcharValue = PG_GETARG_DATUM(0);
    char* tmp = NULL;
    Datum result;
    tmp = DatumGetCString(DirectFunctionCall1(varcharout, varcharValue));

    result = DirectFunctionCall1(float4in, CStringGetDatum(tmp));
    pfree_ext(tmp);

    PG_RETURN_DATUM(result);
}

Datum varchar_float8(PG_FUNCTION_ARGS)
{
    Datum varcharValue = PG_GETARG_DATUM(0);
    char* tmp = NULL;
    Datum result;
    tmp = DatumGetCString(DirectFunctionCall1(varcharout, varcharValue));

    result = DirectFunctionCall1(float8in, CStringGetDatum(tmp));
    pfree_ext(tmp);

    PG_RETURN_DATUM(result);
}

//
// Numeric Compression Codes Area
//
// ascale: adjusted scale

/// we need to do unit testing for some *static* functions,
/// so we redefine *static* if *ENABLE_UT* is defined.
/// at the end of this file we will restore it.
#ifdef ENABLE_UT
#define static
#endif

#if DEC_DIGITS == 4

/// we know that it's failed to convert a numeric value to int64,
/// whose digits number is equal to or greater than 6.
#define NUMERIC_NDIGITS_UPLIMITED (6)
#define NUMERIC_NDIGITS_INT128_UPLIMITED (11)

/// infor about holding int64 min value.
/// 1. how many digits at least;
/// 2. its tailing data;
#define INT64_MIN_VAL_NDIGITS (5)
#define INT64_MIN_VALUE_LAST (5808)
#define INT64_MAX_VALUE_FIRST (922)

/// max buffer size for holding an valid int64 numeric.
#define MAX_NUMERIC_BUFFER_SIZE (NUMERIC_HDRSZ_SHORT + INT64_MIN_VAL_NDIGITS * sizeof(NumericDigit))

/// the same to MAX_NUMERIC_BUFFER_SIZE but for 1 varlena head
#define MAX_1HEAD_NUMERIC_BUFFER_SIZE (VARHDRSZ_SHORT + sizeof(uint16) + INT64_MIN_VAL_NDIGITS * sizeof(NumericDigit))

#endif

// how many tailing zeros within each of 0~9999
static char number_of_tail_zeros[10000] = {0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    3,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    3,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    3,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    3,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    3,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    3,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    3,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    3,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    3,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0};

const int16 INT16_MIN_VALUE = INT16_MIN;  // equal to 0x8000
const int16 INT16_MAX_VALUE = INT16_MAX;  // equal to 0x7fff
const int32 INT32_MIN_VALUE = INT32_MIN;  // equal to 0x80000000
const int32 INT32_MAX_VALUE = INT32_MAX;  // equal to 0x7fffffff
const int64 INT64_MIN_VALUE = INT64_MIN;  // equal to 8000000000000000
const int64 INT64_MAX_VALUE = INT64_MAX;  // equal to 7FFFFFFFFFFFFFFF

#ifdef ENABLE_UT

/// the same to dump_numeric()
/// - Dump a value in the db storage format
///   into a file for debugging.
void test_dump_numeric_to_file(_out_ FILE* fd, _in_ const char* str, _in_ Numeric num)
{
    fprintf(fd, "%s: NUMERIC weight=%d dscale=%d ", str, NUMERIC_WEIGHT(num), NUMERIC_DSCALE(num));

    int sign = NUMERIC_SIGN(num);
    if (NUMERIC_POS == sign)
        fprintf(fd, "POS");
    else if (NUMERIC_NEG == sign)
        fprintf(fd, "NEG");
    else if (NUMERIC_NAN == sign)
        fprintf(fd, "NaN");
    else
        fprintf(fd, "SIGN=0x%x", sign);

    NumericDigit* digits = NUMERIC_DIGITS(num);
    int ndigits = NUMERIC_NDIGITS(num);
    for (int k = 0; k < ndigits; k++)
        fprintf(fd, " %0*d", DEC_DIGITS, digits[k]);

    fprintf(fd, "\n");
}

// check the display scales are the same.
bool test_numeric_dscale_equal(_in_ Numeric v1, _in_ Numeric v2)
{
    return (NUMERIC_DSCALE(v1) == NUMERIC_DSCALE(v2));
}

void test_dump_compressed_numeric_to_file(_out_ FILE* fd, _in_ const char* str, _in_ int64 v, _in_ char ascale)
{
    fprintf(fd, "%s: ", str);
    if (convert_short_numeric_to_int32(v, ascale))
        fprintf(fd, "[INT32]%ld ", v);
    else
        fprintf(fd, "[INT64]%ld ", v);
    fprintf(fd, "[ascale]%d", ascale);
    fprintf(fd, "\n");
}

#endif

/// one member of make_numeric() family.
/// make numeric result for the min-int64 value.
/// and return the bytes size used.
static inline int make_short_numeric_of_int64_minval(_out_ Numeric result, _in_ int dscale, _in_ int weight)
{
    const int len = MAX_NUMERIC_BUFFER_SIZE;
    NumericDigit* digits = result->choice.n_short.n_data;

    SET_VARSIZE(result, len);

    result->choice.n_short.n_header = (NUMERIC_SHORT | NUMERIC_SHORT_SIGN_MASK)            // sign info
                                      | (dscale << NUMERIC_SHORT_DSCALE_SHIFT)             // display info
                                      | (weight < 0 ? NUMERIC_SHORT_WEIGHT_SIGN_MASK : 0)  // weight info
                                      | (weight & NUMERIC_SHORT_WEIGHT_MASK);

    /// see *INT64_MIN_VALUE* value.
    digits[0] = 922;
    digits[1] = 3372;
    digits[2] = 368;
    digits[3] = 5477;
    digits[4] = INT64_MIN_VALUE_LAST;

    return len;
}

/// one member of make_numeric() family.
/// this is for numeric 0.
static inline int make_short_numeric_of_zero(Numeric result, int typmod)
{
    /// set display scale if typmod is given, otherwise is 0 at default.
    int dscale = (typmod >= (int32)(VARHDRSZ)) ? ((typmod - VARHDRSZ) & 0xffff) : 0;

    SET_VARSIZE(result, NUMERIC_HDRSZ_SHORT);                                   // length info
    result->choice.n_short.n_header = NUMERIC_SHORT                             // sign is NUMERIC_POS
                                      | (dscale << NUMERIC_SHORT_DSCALE_SHIFT)  // dscale info
        // weight is 0
        ;
    return (int)NUMERIC_HDRSZ_SHORT;
}

/// the same to make_result() but only for short numeric.
/// and the bytes size will be returned also.
static inline int make_short_numeric(
    Numeric result, NumericDigit* digits, int ndigits, int sign, int weight, int dscale, bool is_int128 = false)
{
    Assert(sign != NUMERIC_NAN);
    Assert(NUMERIC_CAN_BE_SHORT(dscale, weight));

    // check the leading and tailing zeros have been stripped.
    Assert(ndigits > 0 && ndigits <= NUMERIC_NDIGITS_INT128_UPLIMITED);
    Assert(digits[0] != 0);

    const int totalSize = (NUMERIC_HDRSZ_SHORT + ndigits * sizeof(NumericDigit));

    // step 1: set length info
    SET_VARSIZE(result, totalSize);

    // step 2: set head info, including sign, weight and dscale.
    result->choice.n_short.n_header =
        ((sign == NUMERIC_NEG) ? (NUMERIC_SHORT | NUMERIC_SHORT_SIGN_MASK) : NUMERIC_SHORT) |
        (dscale << NUMERIC_SHORT_DSCALE_SHIFT) | (weight < 0 ? NUMERIC_SHORT_WEIGHT_SIGN_MASK : 0) |
        (((uint32)weight) & NUMERIC_SHORT_WEIGHT_MASK);

    // step 3: build the data info
    NumericDigit* src = digits;
    NumericDigit* dest = result->choice.n_short.n_data;
    switch (ndigits) {
        case 11:
            *dest++ = *src++;
            /* fall through */
        case 10:
            *dest++ = *src++;
            /* fall through */
        case 9:
            *dest++ = *src++;
            /* fall through */
        case 8:
            *dest++ = *src++;
            /* fall through */
        case 7:
            *dest++ = *src++;
            /* fall through */
        case 6:
            *dest++ = *src++;
            /* fall through */
        case 5:
            *dest++ = *src++;
            /* fall through */
        case 4:
            *dest++ = *src++;
            /* fall through */
        case 3:
            *dest++ = *src++;
            /* fall through */
        case 2:
            *dest++ = *src++;
            /* fall through */
        case 1:
            *dest++ = *src++;
            /* fall through */
        default:  // do nothing if 0
            break;
    }

    // Check for overflow of int16 fields
    Assert(NUMERIC_NDIGITS(result) == (unsigned int)(ndigits));
    Assert(weight == NUMERIC_WEIGHT(result));
    Assert(dscale == NUMERIC_DSCALE(result));
    return totalSize;
}

static inline int get_weight_from_ascale(int ndigits, int ascale)
{
    return (ndigits - ascale - 1);
}

static int get_dscale_from_typmod(int typmod, int ascale, int last_item)
{
    if (typmod >= (int32) (VARHDRSZ)) {
        return (int32) ((uint32) (typmod - VARHDRSZ) & 0xffff);
    }

    /*
     * If typmod is not given, we may restore the wrong dscale.
     * example: 1.000 its dscale is 3, but we cannot get it from {value=1, ascale=0}
     */
    if (ascale <= 0) {
        return 0;
    }

    Assert(4 == DEC_DIGITS);
    Assert(last_item > 0 && last_item < 10000);
    return ((uint32)((uint32)ascale << 2) - number_of_tail_zeros[last_item]);
}

/*
 * @Description:  get numeric whole scale
 * @IN numVar: numeric
 * @Return: numeric scale
 */
static inline int get_whole_scale(const NumericVar& numVar)
{
    int whole_scale = 0;
    if (numVar.weight >= 0 && numVar.ndigits > 0) {
        whole_scale = numVar.weight * DEC_DIGITS + numVar.dscale;
        int first_digit = numVar.digits[0];
        if (first_digit >= 1000) {
            whole_scale = whole_scale + 4;
        } else if (first_digit >= 100) {
            whole_scale = whole_scale + 3;
        } else if (first_digit >= 10) {
            whole_scale = whole_scale + 2;
        } else {
            whole_scale = whole_scale + 1;
        }
    } else {
        whole_scale = numVar.dscale;
    }

    return whole_scale;
}

/// make a copy with 4B varlena header.
static inline Numeric numeric_copy(Numeric shortNum, char* outBuf)
{
    Assert(VARATT_IS_SHORT(shortNum));

    const int diff = VARHDRSZ - VARHDRSZ_SHORT;
    int len = VARSIZE_SHORT(shortNum);

    // Notice: include two parts:
    // 1. numeric header, uint16;
    // 2. numeric digits, int16[];
    // so that we also can handle 0 numeric.
    int nShortDigtis = (len - VARHDRSZ_SHORT) / sizeof(uint16);
    Assert(nShortDigtis >= 1 && nShortDigtis <= NUMERIC_NDIGITS_UPLIMITED);

    char* buf = outBuf;
    uint16* dest = (uint16*)(buf + VARHDRSZ);
    uint16* src = (uint16*)((char*)shortNum + VARHDRSZ_SHORT);

    // set varlena header size.
    SET_VARSIZE(buf, len + diff);
    // copy all the data, including flags and digits.
    switch (nShortDigtis) {
        case 6:
            *dest++ = *src++;
            /* fall through */
        case 5:
            *dest++ = *src++;
            /* fall through */
        case 4:
            *dest++ = *src++;
            /* fall through */
        case 3:
            *dest++ = *src++;
            /* fall through */
        case 2:
            *dest++ = *src++;
            /* fall through */
        case 1:
        default:
            *dest++ = *src++;
            break;
    }

    return (Numeric)buf;
}

#define update_result(__result, __digits, __weight) \
    do {                                            \
        (__result) *= NBASE;                        \
        (__result) += (__digits)[(__weight)];       \
    } while (0)

#define encode_digits(__result, __digits, __ndigits)  \
    do {                                              \
        switch (__ndigits) {                          \
            case 6: {                                 \
                (__result) = (__digits)[0];           \
                update_result(__result, __digits, 1); \
                update_result(__result, __digits, 2); \
                update_result(__result, __digits, 3); \
                update_result(__result, __digits, 4); \
                update_result(__result, __digits, 5); \
                break;                                \
            }                                         \
            case 5: {                                 \
                (__result) = (__digits)[0];           \
                update_result(__result, __digits, 1); \
                update_result(__result, __digits, 2); \
                update_result(__result, __digits, 3); \
                update_result(__result, __digits, 4); \
                break;                                \
            }                                         \
            case 4: {                                 \
                (__result) = (__digits)[0];           \
                update_result(__result, __digits, 1); \
                update_result(__result, __digits, 2); \
                update_result(__result, __digits, 3); \
                break;                                \
            }                                         \
            case 3: {                                 \
                (__result) = (__digits)[0];           \
                update_result(__result, __digits, 1); \
                update_result(__result, __digits, 2); \
                break;                                \
            }                                         \
            case 2: {                                 \
                (__result) = (__digits)[0];           \
                update_result(__result, __digits, 1); \
                break;                                \
            }                                         \
            case 1: {                                 \
                (__result) = (__digits)[0];           \
                break;                                \
            }                                         \
            default: {                                \
                Assert(0 == (__ndigits));             \
                (__result) = 0;                       \
                break;                                \
            }                                         \
        }                                             \
    } while (0)

/*
 * @Description: encoding numeric to dscale int64
 * @IN  batchValues: array of numeric values
 * @IN  batchNulls: array of nulls values
 * @IN  batchRows: number of array
 * @OUT outInt: array of converted int64
 * @OUT outDscales: array of dscale
 * @OUT outSuccess: array of success convert
 * @Return: number of success convert
 */
template <bool hasNull>
int batch_convert_short_numeric_to_dscale_int64_T(_in_ Datum* batchValues, _in_ char* batchNulls, _in_ int batchRows,
    _out_ int64* outInt, _out_ char* outDscales, _out_ bool* outSuccess, _out_ int* outNullCount)
{
    Assert(4 == DEC_DIGITS);

    if (t_thrd.mem_cxt.batch_encode_numeric_mem_cxt == NULL) {
        t_thrd.mem_cxt.batch_encode_numeric_mem_cxt = AllocSetContextCreate(t_thrd.top_mem_cxt,
            "BATCH ENCODE NUMERIC CNXT",
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE);
    }

    MemoryContext oldMemCnxt = MemoryContextSwitchTo(t_thrd.mem_cxt.batch_encode_numeric_mem_cxt);

    NumericVar numVar;
    Numeric num = NULL;

    char* outBuffer = (char*)palloc(MAX_NUMERIC_BUFFER_SIZE * batchRows);
    char* tmpOutBuf = outBuffer;
    char* notNullDscales = outDscales;

    int batchCnt = 0;
    int dscale = 0;

    for (int i = 0; i < batchRows; ++i) {
        if (hasNull && bitmap_been_set(batchNulls, i)) {
            // treat NULL as failed case.
            *outSuccess++ = false;
            (*outNullCount)++;
            continue;
        }

        num = (Numeric)DatumGetPointer(batchValues[i]);

        // make sure that every Numeric in *batch* is with 4 byte var-header.
        // if it's with 1 byte var-header, we have to do a copy
        // which is with 4 bytes var-header.
        if (unlikely(VARATT_IS_SHORT(num))) {
            // this short numeric is out of range.
            if (unlikely(VARSIZE_SHORT(num) > MAX_1HEAD_NUMERIC_BUFFER_SIZE)) {
                *outSuccess++ = false;
                *notNullDscales++ = FAILED_DSCALE_ENCODING;
                continue;
            }

            num = numeric_copy(num, tmpOutBuf);
            tmpOutBuf += MAX_NUMERIC_BUFFER_SIZE;
        }

        if (NUMERIC_IS_BI64(num)) {
            // numeric with bigint64
            *outSuccess++ = true;
            *outInt++ = NUMERIC_64VALUE(num);
            dscale = NUMERIC_BI_SCALE(num);
            Assert(dscale < MAXINT64DIGIT && dscale >= 0);
            *notNullDscales++ = (char)dscale;
            ++batchCnt;
        } else if (NUMERIC_IS_BI128(num)) {
            // numeric with bigint128
            *outSuccess++ = false;
            *notNullDscales++ = FAILED_DSCALE_ENCODING;
        } else if (NUMERIC_IS_NAN(num)) {
            // NAN
            *outSuccess++ = false;
            *notNullDscales++ = FAILED_DSCALE_ENCODING;
        } else {
            // numeric
            init_var_from_num(num, &numVar);

            // whole scale
            int whole_scale = get_whole_scale(numVar);

            if (likely(CAN_CONVERT_BI64(whole_scale))) {
                // convert to BI64
                *outSuccess++ = true;
                *outInt++ = convert_short_numeric_to_int64_byscale(num, numVar.dscale);
                Assert(numVar.dscale < MAXINT64DIGIT && numVar.dscale >= 0);
                *notNullDscales++ = (char)numVar.dscale;
                ++batchCnt;
            } else {
                // can't not convert to BI64
                *outSuccess++ = false;
                *notNullDscales++ = FAILED_DSCALE_ENCODING;
            }
        }
    }

    (void)MemoryContextSwitchTo(oldMemCnxt);

    // reset this memory context after numeric encoding
    MemoryContextReset(t_thrd.mem_cxt.batch_encode_numeric_mem_cxt);

    return batchCnt;
}

int batch_convert_short_numeric_to_int64(_in_ Datum* batchValues, _in_ char* batchNulls, _in_ int batchRows,
    _in_ bool hasNull, _out_ int64* outInt, _out_ char* outAscales, _out_ bool* outSuccess, _out_ int* outNullCount)
{
    if (hasNull)
        return batch_convert_short_numeric_to_dscale_int64_T<true>(
            batchValues, batchNulls, batchRows, outInt, outAscales, outSuccess, outNullCount);
    else
        return batch_convert_short_numeric_to_dscale_int64_T<false>(
            batchValues, batchNulls, batchRows, outInt, outAscales, outSuccess, outNullCount);
}

bool convert_short_numeric_to_int64(char* inBuf, int64* v, char* ascale)
{
    Datum values[1] = {PointerGetDatum(inBuf)};
    char nulls[1] = {0};
    bool successful = false;
    int outNullCount = 0;
    int nSuccess =
        batch_convert_short_numeric_to_dscale_int64_T<false>(values, nulls, 1, v, ascale, &successful, &outNullCount);
    if (!(successful && (nSuccess == 1)) && !(!successful && (nSuccess == 0))) {
        Assert(0);
    }
    return successful;
}

/// this function is called after *convert_short_numeric_to_int64* works.
/// both value and its adjusted scale are checked.
bool convert_short_numeric_to_int32(_in_ int64 v, _in_ char ascale)
{
    return (
        (ascale >= INT32_MIN_ASCALE && ascale <= INT32_MAX_ASCALE) && (v >= INT32_MIN_VALUE && v <= INT32_MAX_VALUE));
}

static int16* get_digits_from_int64(_out_ int& sign, _in_ int16* digits_buf, _in_ int128 v)
{
    int128 multiple = 0;
    int16* digits_ptr = digits_buf + NUMERIC_NDIGITS_UPLIMITED;

    if (v < 0) {
        sign = NUMERIC_NEG;
        v = -v;
    }

    // step 1: compute digits_buf from int64 value.
    do {
        multiple = v / NBASE;
        *(--digits_ptr) = v - multiple * NBASE;
        v = multiple;
    } while (v);

    return digits_ptr;
}

int convert_int64_to_short_numeric(_out_ char* outBuf, _in_ int64 v, _in_ char ascale, _in_ int32 typmod)
{
    int sign = NUMERIC_POS;

    int16 digits_buf[NUMERIC_NDIGITS_UPLIMITED];
    Numeric n = (Numeric)outBuf;

    if (0 == v) {
        return make_short_numeric_of_zero(n, typmod);
    }

    if (unlikely(INT64_MIN_VALUE == v)) {
        // overflow happens when run "v = -v",
        // so we simply hard coding this situation.
        return make_short_numeric_of_int64_minval(n,
            get_dscale_from_typmod(typmod, ascale, INT64_MIN_VALUE_LAST),
            get_weight_from_ascale(INT64_MIN_VAL_NDIGITS, ascale));
    }

    int16* digits_ptr = get_digits_from_int64(sign, digits_buf, v);

    // step 2: restore Numeric storage format.
    int ndigits = digits_buf + NUMERIC_NDIGITS_UPLIMITED - digits_ptr;
    Assert(digits_ptr[ndigits - 1] != 0);
    return make_short_numeric(n,
        digits_ptr,
        ndigits,
        sign,
        // compute weight from int64 ndignit and ascale.
        get_weight_from_ascale(ndigits, ascale),
        // compute the display scale.
        get_dscale_from_typmod(typmod, ascale, digits_buf[NUMERIC_NDIGITS_UPLIMITED - 1]));
}

/*
 * convert functions between int32|int64|int128 and numeric
 *     convert numeric to int32, int64 or int128
 *     convert int32, int64, int128 to numeric
 */
int convert_int64_to_short_numeric_byscale(_out_ char* outBuf, _in_ int128 v, _in_ int32 typmod, _in_ int32 vscale)
{
    Numeric n = (Numeric)outBuf;

    if (0 == v) {
        return make_short_numeric_of_zero(n, typmod);
    }

    int sign = NUMERIC_POS;
    int16 digits_buf[NUMERIC_NDIGITS_UPLIMITED];
    int scale = (int32)((uint32)(typmod - VARHDRSZ) & 0xffff);
    int scaleDiff = NUMERIC_SCALE_ADJUST(vscale) * DEC_DIGITS - vscale;
    Assert(scaleDiff >= 0 && scaleDiff <= MAXINT64DIGIT);
    v = v * ScaleMultipler[scaleDiff];

    int16* digits_ptr = get_digits_from_int64(sign, digits_buf, v);
    int ndigits = digits_buf + NUMERIC_NDIGITS_UPLIMITED - digits_ptr;

    int16* p = digits_buf + NUMERIC_NDIGITS_UPLIMITED - 1;
    int real_ndigits = ndigits;
    while (0 == *p) {
        --real_ndigits;
        --p;
    }
    Assert(real_ndigits > 0 && real_ndigits <= NUMERIC_NDIGITS_UPLIMITED);
    return make_short_numeric(n,
        digits_ptr,
        real_ndigits,
        sign,
        // compute weight from int64 ndignit and ascale.
        get_weight_from_ascale(ndigits, NUMERIC_SCALE_ADJUST(vscale)),
        scale);
}

int64 convert_short_numeric_to_int64_byscale(_in_ Numeric n, _in_ int scale)
{
    int128 result = 0;
    int weight = NUMERIC_WEIGHT(n);
    int ndigits = SHORT_NUMERIC_NDIGITS(n);
    int ascale = (ndigits > 0) ? (ndigits - weight - 1) : 0;
    int scaleDiff = scale - ascale * DEC_DIGITS;
    NumericDigit* digits = SHORT_NUMERIC_DIGITS(n);

    encode_digits(result, digits, ndigits);

    /* adjust scale */
    result = (scaleDiff > 0) ? (result * ScaleMultipler[scaleDiff]) : (result / ScaleMultipler[-scaleDiff]);
    /* get the result by sign */
    result = (NUMERIC_POS == NUMERIC_SIGN(n)) ? result : -result;
    Assert(INT128_INT64_EQ(result));

    return (int64)result;
}

int64 convert_short_numeric_to_int64_byscale_fast(NumericVar *numVar)
{
    int128 result = 0;
    int ascale = (numVar->ndigits > 0) ? (numVar->ndigits - numVar->weight - 1) : 0;
    int scaleDiff = numVar->dscale - ascale * DEC_DIGITS;

    encode_digits(result, numVar->digits, numVar->ndigits);

    /* adjust scale */
    result = (scaleDiff > 0) ? (result * ScaleMultipler[scaleDiff]) : (result / ScaleMultipler[-scaleDiff]);
    /* get the result by sign */
    result = (NUMERIC_POS == numVar->sign) ? result : -result;
    Assert(INT128_INT64_EQ(result));

    return (int64)result;
}

/*
 * n: ndigits
 * w: weight
 * a: ascale  =  n - (w + 1)
 * d: dscale (display scale)
 * a <= 0 means no scale part in NumericShort;
 * w <  0 means just scale part in NumericShort;
 *
 * ... w=1   w=0    w=-1 ...
 * ... xxxx  xxxx . xxxx ...
 * ... a=-1  a=0    a=1  ...
 *
 * convert NumericShort to int128:
 * step 1: get all valid digitals to result; if d%4 != 0, just get valid
 *         digitals from the last item in digits[]. such as digits[-1] = 6800,
 *         d%4=2, so just 68 is needed.
 *
 * step 2: get diff_scale as four cases below:
 *   +--------------------------------------+---------------------------------+
 *   | a <= 0                               |                                 |
 *   +--------------------------------------+                                 |
 *   | a > 0 and (d%4) == 0                 |  diff_scale = d - a*4           |
 *   +--------------------------------------+                                 |
 *   | a > 0 and (d+4)/4 > a                |                                 |
 *   +--------------------------------------+---------------------------------+
 *   | a > 0 and (d%4) != 0 and (d+4)/4==a  |  diff_scale = d - (a-1)*4 - d%4 |
 *   +------------------------------------------------------------------------+
 * NOTE:
 *   case 4 : (a-1)*4+d%4 means the number of the valid digitals in scale part
 * example:
 *   case 1: 1.0
 *   case 2: dec(4,4) 1.1230, dec(4,8) 1.1230
 *   case 3: dec(6,9) 1.1230
 *   case 4: dec(6,3) 1.1230
 *
 * setp 3:
 *   result *= S_INT128_POWER[diff_scale]
 *
 *==============================================================================
 *
 * @Description: convert PG Numeric format to the data type of int128
 *
 * @IN  value: the source data with PG Numeric format
 * @IN  value: display scale
 * @OUT value: result, the result of the data type of int128.
 * @return: None
 */
void convert_short_numeric_to_int128_byscale(_in_ Numeric n, _in_ int dscale, _out_ int128& result)
{
    bool special_do = false;
    int ndigits = SHORT_NUMERIC_NDIGITS(n);

    /* ndigits is 0, result is 0, return directly */
    result = 0;
    if (0 == ndigits) {
        return;
    }

    int remainder = dscale % DEC_DIGITS;
    int weight = NUMERIC_WEIGHT(n);
    int ascale = ndigits - (weight + 1);
    int end_index = ndigits;
    int diff_scale = 0;
    NumericDigit* digits = SHORT_NUMERIC_DIGITS(n);
    Assert(ndigits >= 0);

    if (ascale > 0 && remainder != 0 && (dscale / DEC_DIGITS + 1) == ascale) {
        special_do = true;
        --end_index;
    }

    /* step1. get all valid digitals to result */
    for (int i = 0; i < end_index; i++)
        result += (digits[i] * getScaleMultiplier((end_index - 1 - i) * DEC_DIGITS));

    if (special_do) {
        result = (result * getScaleMultiplier(remainder)) +
                 (digits[ndigits - 1] / getScaleMultiplier(DEC_DIGITS - remainder));
        /* step2. get diff_scale by dscale and ascale */
        diff_scale = dscale - (ascale - 1) * 4 - dscale % 4;
    } else {
        /* step2. get diff_scale by dscale and ascale */
        diff_scale = dscale - ascale * 4;
    }

    /* step3. adjust result by diff_scale */
    result *= getScaleMultiplier(diff_scale);

    result = (NUMERIC_POS == NUMERIC_SIGN(n)) ? result : -result;
}

void convert_short_numeric_to_int128_byscale_fast(NumericVar *numVar, int128& result)
{
    bool special_do = false;

    /* ndigits is 0, result is 0, return directly */
    result = 0;
    if (0 == numVar->ndigits) {
        return;
    }

    int remainder = numVar->dscale % DEC_DIGITS;
    int ascale = numVar->ndigits - (numVar->weight + 1);
    int end_index = numVar->ndigits;
    int diff_scale = 0;
    Assert(numVar->ndigits >= 0);

    if (ascale > 0 && remainder != 0 && (numVar->dscale / DEC_DIGITS + 1) == ascale) {
        special_do = true;
        --end_index;
    }

    /* step1. get all valid digitals to result */
    for (int i = 0; i < end_index; i++)
        result += (numVar->digits[i] * getScaleMultiplier((end_index - 1 - i) * DEC_DIGITS));

    if (special_do) {
        result = (result * getScaleMultiplier(remainder)) +
                 (numVar->digits[numVar->ndigits - 1] / getScaleMultiplier(DEC_DIGITS - remainder));
        /* step2. get diff_scale by dscale and ascale */
        diff_scale = numVar->dscale - (ascale - 1) * 4 - numVar->dscale % 4;
    } else {
        /* step2. get diff_scale by dscale and ascale */
        diff_scale = numVar->dscale - ascale * 4;
    }

    /* step3. adjust result by diff_scale */
    result *= getScaleMultiplier(diff_scale);

    result = (NUMERIC_POS == numVar->sign) ? result : -result;
}

/*
 * vscale is from orc file, and dscale is from gaussdb
 */
int convert_int128_to_short_numeric_byscale(_out_ char* outBuf, _in_ int128 v, _in_ int32 typmod, _in_ int32 vscale)
{
    int16 digits_buf[NUMERIC_NDIGITS_INT128_UPLIMITED];
    int16* digits_ptr = NULL;
    int16 tmp;

    int sign;
    int remainder;
    int scale;
    int ndigits;
    int weight;

    int128 multiple;

    Assert(vscale >= 0);

    if (0 == v)
        return make_short_numeric_of_zero((Numeric)outBuf, typmod);

    sign = NUMERIC_POS;
    if (v < 0) {
        v = -v;
        sign = NUMERIC_NEG;
    }

    digits_ptr = digits_buf + NUMERIC_NDIGITS_INT128_UPLIMITED;
    remainder = vscale % DEC_DIGITS;
    if (remainder > 0) {
        tmp = (int16)(v % ScaleMultipler[remainder]);
        v /= ScaleMultipler[remainder];

        *(--digits_ptr) = tmp * ScaleMultipler[DEC_DIGITS - remainder];
    }

    multiple = 0;
    while (v) {
        multiple = v / NBASE;
        *(--digits_ptr) = v - multiple * NBASE;
        v = multiple;
    };

    scale = (int32)((uint32)(typmod - VARHDRSZ) & 0xffff);
    ndigits = digits_buf + NUMERIC_NDIGITS_INT128_UPLIMITED - digits_ptr;
    weight = get_weight_from_ascale(ndigits, NUMERIC_SCALE_ADJUST(vscale));

    int16* p = digits_buf + NUMERIC_NDIGITS_INT128_UPLIMITED - 1;
    int real_ndigits = ndigits;
    while (0 == *p) {
        --real_ndigits;
        --p;
    }

    return make_short_numeric((Numeric)outBuf, digits_ptr, real_ndigits, sign, weight, scale, true);
}

/*
 * @Description: This function try to convert numeric to big interger
 *               format. return the formated value or the original one.
 *
 * @IN value: input numeric value.
 * @return: Numeric - Datum points to fast numeric format
 */
Datum try_convert_numeric_normal_to_fast(Datum value, ScalarVector *arr)
{
    Numeric val = DatumGetNumeric(value);

    if (NUMERIC_IS_NANORBI(val) || u_sess->attr.attr_sql.enable_fast_numeric == false)
        return NumericGetDatum(val);

    NumericVar numVar;
    init_var_from_num(val, &numVar);

    int whole_scale = get_whole_scale(numVar);

    // should be ( whole_scale <= MAXINT64DIGIT)
    if (CAN_CONVERT_BI64(whole_scale)) {
        int64 result = convert_short_numeric_to_int64_byscale(val, numVar.dscale);
        return makeNumeric64(result, numVar.dscale, arr);
    } else if (CAN_CONVERT_BI128(whole_scale)) {
        int128 result = 0;
        convert_short_numeric_to_int128_byscale(val, numVar.dscale, result);
        return makeNumeric128(result, numVar.dscale, arr);
    } else
        return NumericGetDatum(val);
}

Datum try_direct_convert_numeric_normal_to_fast(Datum value, ScalarVector *arr)
{
    Numeric val = NULL;
    struct varlena* attr = (struct varlena*)value;
    union NumericChoice* choice;
    NumericVar numVar;

    if (u_sess->attr.attr_sql.enable_fast_numeric == false)
        return NumericGetDatum(DatumGetNumeric(value));

    if (VARATT_IS_EXTENDED(attr)) {
        if (VARATT_IS_SHORT(attr) && !VARATT_IS_HUGE_TOAST_POINTER(attr)) {
            choice = (union NumericChoice*)VARDATA_SHORT(attr);

            if (NUMERIC_IS_NANORBI_CHOICE(choice))
                return NumericGetDatum(DatumGetNumeric(value));

            numVar.ndigits = ((VARSIZE_SHORT(attr) - NUMERIC_HEADER_SIZE_CHOICE_1B(choice)) / sizeof(NumericDigit));
            numVar.weight = NUMERIC_WEIGHT_CHOICE(choice);
            numVar.sign = NUMERIC_SIGN_CHOICE(choice);
            numVar.dscale = NUMERIC_DSCALE_CHOICE(choice);
            numVar.digits = NUMERIC_DIGITS_CHOICE(choice);
        }
        else {
            val = DatumGetNumeric(value);

            if (NUMERIC_IS_NANORBI(val))
                return NumericGetDatum(val);

            init_var_from_num(val, &numVar);
        }
    }
    else {
        val = (Numeric)value;

        if (NUMERIC_IS_NANORBI(val))
            return NumericGetDatum(val);

        init_var_from_num(val, &numVar);
    }

    int whole_scale = get_whole_scale(numVar);

    // should be ( whole_scale <= MAXINT64DIGIT)
    if (CAN_CONVERT_BI64(whole_scale)) {
        int64 result = convert_short_numeric_to_int64_byscale_fast(&numVar);
        return makeNumeric64(result, numVar.dscale, arr);
    } else if (CAN_CONVERT_BI128(whole_scale)) {
        int128 result = 0;
        convert_short_numeric_to_int128_byscale_fast(&numVar, result);
        return makeNumeric128(result, numVar.dscale, arr);
    } else {
        if (val)
            return NumericGetDatum(val);
        else
            return NumericGetDatum(DatumGetNumeric(value));
    }   
}

/*
 * @Description: This function convert big integer64 to
 *               short numeric
 *
 * @IN data:  value of bi64
 * @IN scale: scale of bi64
 * @return: Numeric - the result of numeric type
 */
Numeric convert_int64_to_numeric(int64 data, uint8 scale)
{
    Assert(scale <= MAXINT64DIGIT);

    uint64 uval = 0;
    uint64 newuval = 0;
    Size len = 0;
    int tmp_loc = 0;
    NumericVar var;
    int rc;

    /* step 1: get the absolute value of int64 data */
    if (data < 0) {
        var.sign = NUMERIC_NEG;
        /* (-1 * data) maybe out of int64 bound, turn to uint64 */
        uval = -data;
    } else {
        var.sign = NUMERIC_POS;
        uval = data;
    }
    var.dscale = scale;
    var.ndigits = 0;
    var.weight = 0;
    /* data equals to 0, return here */
    if (uval == 0) {
        len = NUMERIC_HDRSZ_SHORT;
        Numeric result = (Numeric)palloc(len);
        SET_VARSIZE(result, len);
        result->choice.n_short.n_header =
            (uint16)((NUMERIC_SHORT) | (((uint32)var.dscale) << NUMERIC_SHORT_DSCALE_SHIFT));
        return result;
    }

    /* step 2: split source int64 data into pre_data and post_data by decimal point
     * pre_data stores the data before decimal point.
     */
    uint64 pre_data = uval / getScaleMultiplier(scale);
    /* pre_data stores the data after decimal point. */
    uint64 post_data = uval % getScaleMultiplier(scale);

    /* int64 can require at most 19 decimal digits;
     * add one for safety, buf1 stores pre_data,
     * buf2 stores post_data.
     */
    NumericDigit buf1[20 / DEC_DIGITS];
    NumericDigit buf2[20 / DEC_DIGITS];
    NumericDigit* ptr1 = buf1 + 5;
    NumericDigit* ptr2 = buf2;
    int pre_digits = 0;
    int post_digits = 0;

    /* step 3: calculate pre_data and store result in buf1
     * pre_data == 0, skip this
     */
    if (pre_data != 0) {
        do {
            ptr1--;
            pre_digits++;
            newuval = pre_data / NBASE;
            *ptr1 = pre_data - newuval * NBASE;
            pre_data = newuval;
        } while (pre_data);
        var.weight = pre_digits - 1;
    }
    /* step 4: calculate pre_data and store result in buf2
     * post_data == 0, skip this
     */
    if (post_data != 0) {
        int result_scale = (int)scale;
        while (post_data && result_scale >= DEC_DIGITS) {
            post_digits++;
            result_scale = result_scale - DEC_DIGITS;
            *ptr2 = post_data / ScaleMultipler[result_scale];
            post_data = post_data % ScaleMultipler[result_scale];
            ptr2++;
        }
        if (post_data) {
            Assert(result_scale < DEC_DIGITS);
            post_digits++;
            *ptr2 = post_data * ScaleMultipler[DEC_DIGITS - result_scale];
            ptr2++;
        }
    }

    /* step5: make numeric result */
    Numeric result = NULL;
    if (pre_digits) {
        /* pre_digits != 0 && post_digits != 0
         * Example: 900000000.0001
         */
        if (post_digits) {
            var.ndigits = pre_digits + post_digits;
            len = NUMERIC_HDRSZ_SHORT + var.ndigits * sizeof(NumericDigit);
            result = (Numeric)palloc(len);
            SET_VARSIZE(result, len);

            rc = memcpy_s(result->choice.n_short.n_data,
                pre_digits * sizeof(NumericDigit),
                ptr1,
                pre_digits * sizeof(NumericDigit));
            securec_check(rc, "\0", "\0");
            rc = memcpy_s(result->choice.n_short.n_data + pre_digits,
                post_digits * sizeof(NumericDigit),
                buf2,
                post_digits * sizeof(NumericDigit));
            securec_check(rc, "\0", "\0");
        } else {
            /* pre_digits != 0 && post_digits == 0
             * Example: 9000000.000
             */
            for (tmp_loc = 0; tmp_loc < pre_digits;) {
                if (buf1[4 - tmp_loc] == 0) {
                    tmp_loc++;
                } else {
                    break;
                }
            }
            pre_digits = pre_digits - tmp_loc;
            var.ndigits = pre_digits;
            len = NUMERIC_HDRSZ_SHORT + var.ndigits * sizeof(NumericDigit);
            result = (Numeric)palloc(len);
            SET_VARSIZE(result, len);
            rc = memcpy_s(result->choice.n_short.n_data,
                pre_digits * sizeof(NumericDigit),
                ptr1,
                pre_digits * sizeof(NumericDigit));
            securec_check(rc, "\0", "\0");
        }
    } else {
        /* pre_digits == 0 && post_digits != 0
         * Example: 0.0000001
         */
        Assert(post_digits <= 5);
        var.weight = 0;
        for (tmp_loc = 0; tmp_loc < post_digits; tmp_loc++) {
            var.weight--;
            if (buf2[tmp_loc] != 0)
                break;
        }
        post_digits = post_digits - tmp_loc;
        var.ndigits = post_digits;
        len = NUMERIC_HDRSZ_SHORT + var.ndigits * sizeof(NumericDigit);
        result = (Numeric)palloc(len);
        SET_VARSIZE(result, len);
        rc = memcpy_s(result->choice.n_short.n_data,
            post_digits * sizeof(NumericDigit),
            buf2 + tmp_loc,
            post_digits * sizeof(NumericDigit));
        securec_check(rc, "\0", "\0");
    }

    result->choice.n_short.n_header =
        (var.sign == NUMERIC_NEG ? (NUMERIC_SHORT | NUMERIC_SHORT_SIGN_MASK) : NUMERIC_SHORT) |
        ((uint32)(var.dscale) << NUMERIC_SHORT_DSCALE_SHIFT) | (var.weight < 0 ? NUMERIC_SHORT_WEIGHT_SIGN_MASK : 0) |
        ((uint32)(var.weight) & NUMERIC_SHORT_WEIGHT_MASK);

    /* Check for overflow of int64 fields */
    Assert(NUMERIC_NDIGITS(result) == (unsigned int)(pre_digits + post_digits));
    Assert(var.weight == NUMERIC_WEIGHT(result));
    Assert(var.dscale == NUMERIC_DSCALE(result));

    return result;
}

static inline uint16 GetNHeader(NumericVar var)
{
    return (var.sign == NUMERIC_NEG ? (NUMERIC_SHORT | NUMERIC_SHORT_SIGN_MASK) : NUMERIC_SHORT) |
        ((uint32)var.dscale << NUMERIC_SHORT_DSCALE_SHIFT) | (var.weight < 0 ? NUMERIC_SHORT_WEIGHT_SIGN_MASK : 0) |
        ((uint32)var.weight & NUMERIC_SHORT_WEIGHT_MASK);
}

/*
 * @Description: This function convert big integer128 to
 *               short numeric
 *
 * @IN data:  value of bi128
 * @IN scale: scale of bi128
 * @return: Numeric - the result of numeric type
 */
Numeric convert_int128_to_numeric(int128 data, int scale)
{
    Assert(scale <= MAXINT128DIGIT);

    uint128 uval = 0;
    uint128 newuval = 0;
    NumericVar var;
    Size len = 0;
    int tmp_loc = 0;
    int rc = 0;
    /* step 1: get the absolute value of int128 data */
    if (data < 0) {
        var.sign = NUMERIC_NEG;
        /* (-1 * data) maybe out of int128 bound, turn to uint128 */
        uval = -data;
    } else {
        var.sign = NUMERIC_POS;
        uval = data;
    }
    var.dscale = scale;
    var.ndigits = 0;
    var.weight = 0;
    /* data equals to 0, return here */
    if (uval == 0) {
        len = NUMERIC_HDRSZ_SHORT;
        Numeric result = (Numeric)palloc(len);
        SET_VARSIZE(result, len);
        result->choice.n_short.n_header =
            (uint16)((NUMERIC_SHORT) | (((uint32)(var.dscale)) << NUMERIC_SHORT_DSCALE_SHIFT));
        return result;
    }

    /* step 2: split source int128 data into pre_data and post_data by decimal point
     * pre_data stores the data before decimal point.
     */
    uint128 pre_data = uval / getScaleMultiplier(scale);
    /* pre_data stores the data after decimal point. */
    uint128 post_data = uval % getScaleMultiplier(scale);

    /* int128 can require at most 38 decimal digits;
     * add two for safety, buf1 stores pre_data,
     * buf2 stores post_data
     */
    NumericDigit buf1[40 / DEC_DIGITS];
    NumericDigit buf2[40 / DEC_DIGITS];
    NumericDigit* ptr1 = buf1 + 10;
    NumericDigit* ptr2 = buf2;
    int pre_digits = 0;
    int post_digits = 0;

    /* step 3: calculate pre_data and store result in buf1
     * pre_data == 0, skip this
     */
    if (pre_data != 0) {
        do {
            ptr1--;
            pre_digits++;
            newuval = pre_data / NBASE;
            *ptr1 = pre_data - newuval * NBASE;
            pre_data = newuval;
        } while (pre_data);
        var.weight = pre_digits - 1;
    }
    /* step 4: calculate pre_data and store result in buf2
     * post_data == 0, skip this
     */
    if (post_data != 0) {
        int result_scale = (int)scale;
        while (post_data && result_scale >= DEC_DIGITS) {
            post_digits++;
            *ptr2 = post_data / getScaleMultiplier(result_scale - DEC_DIGITS);
            post_data = post_data % getScaleMultiplier(result_scale - DEC_DIGITS);
            result_scale = result_scale - DEC_DIGITS;
            ptr2++;
        }
        if (post_data) {
            Assert(result_scale < DEC_DIGITS);
            post_digits++;
            *ptr2 = post_data * ScaleMultipler[DEC_DIGITS - result_scale];
            ptr2++;
        }
    }

    /* step5: make numeric result */
    Numeric result = NULL;
    if (pre_digits) {
        /* pre_digits != 0 && post_digits != 0
         * Example: 9000000.00001
         */
        if (post_digits) {
            var.ndigits = pre_digits + post_digits;
            len = NUMERIC_HDRSZ_SHORT + var.ndigits * sizeof(NumericDigit);
            result = (Numeric)palloc(len);
            SET_VARSIZE(result, len);

            rc = memcpy_s(result->choice.n_short.n_data,
                pre_digits * sizeof(NumericDigit),
                ptr1,
                pre_digits * sizeof(NumericDigit));
            securec_check(rc, "\0", "\0");
            rc = memcpy_s(result->choice.n_short.n_data + pre_digits,
                post_digits * sizeof(NumericDigit),
                buf2,
                post_digits * sizeof(NumericDigit));
            securec_check(rc, "\0", "\0");
        } else {
            /* pre_digits != 0 && post_digits == 0
             * Example: 9000000.000
             */
            for (tmp_loc = 0; tmp_loc < pre_digits;) {
                if (buf1[9 - tmp_loc] == 0) {
                    tmp_loc++;
                } else {
                    break;
                }
            }
            pre_digits = pre_digits - tmp_loc;
            var.ndigits = pre_digits;
            len = NUMERIC_HDRSZ_SHORT + var.ndigits * sizeof(NumericDigit);
            result = (Numeric)palloc(len);
            SET_VARSIZE(result, len);
            rc = memcpy_s(result->choice.n_short.n_data,
                pre_digits * sizeof(NumericDigit),
                ptr1,
                pre_digits * sizeof(NumericDigit));
            securec_check(rc, "\0", "\0");
        }
    } else {
        /* pre_digits == 0 && post_digits != 0
         * Example: 0.000001
         */
        Assert(post_digits <= 10);
        var.weight = 0;
        for (tmp_loc = 0; tmp_loc < post_digits; tmp_loc++) {
            var.weight--;
            if (buf2[tmp_loc] != 0)
                break;
        }
        post_digits = post_digits - tmp_loc;
        var.ndigits = post_digits;
        len = NUMERIC_HDRSZ_SHORT + var.ndigits * sizeof(NumericDigit);
        result = (Numeric)palloc(len);
        SET_VARSIZE(result, len);
        rc = memcpy_s(result->choice.n_short.n_data,
            post_digits * sizeof(NumericDigit),
            buf2 + tmp_loc,
            post_digits * sizeof(NumericDigit));
        securec_check(rc, "\0", "\0");
    }

    result->choice.n_short.n_header = GetNHeader(var);

    /* Check for overflow of int64 fields */
    Assert(NUMERIC_NDIGITS(result) == (unsigned int)(pre_digits + post_digits));
    Assert(var.weight == NUMERIC_WEIGHT(result));
    Assert(var.dscale == NUMERIC_DSCALE(result));

    return result;
}

/*
 * @Description: This function is used to calculate the byte size of the numeric type data,
 * but the result may not be the actual storage size. Due to business requirements,
 * management space and data compression are not considered,
 * so the calculation is defined as follows:
 * i.the sign bit is not considered
 * i.the leading zeros in the integer part and the trailing zeros in the fractional part are removed
 * i.each 4-digit decimal number occupies 2 bytes,and the integer part and the fractional part are calculated
 * respectively i.less than 4 decimal numbers occupy 2 bytes
 *
 * @IN  num: value of numeric
 * @return  result: the byte size of the numeric type data
 */
int32 get_ndigit_from_numeric(Numeric num)
{
    int32 result = 0;
    char* string_from_numeric = NULL;
    int32 string_length_from_numeric = 0;
    int32 i = 0;
    int32 width_of_integer = 0;
    int32 width_of_decimal = 0;
    bool integer_flag = false;
    bool decimal_flag = false;
    int32 point_position = -1;

    if (NUMERIC_IS_NAN(num)) {
        result = NUMERIC_NAN_DATALENGTH;
    } else {
        /* Handle Big Integer */
        if (NUMERIC_IS_BI(num)) {
            num = makeNumericNormal(num);
        }
        string_from_numeric = DatumGetCString(DirectFunctionCall1(numeric_out_with_zero, NumericGetDatum(num)));
        string_length_from_numeric = strlen(string_from_numeric);
        for (i = 0; i < string_length_from_numeric; i++) {
            if (string_from_numeric[i] == '.') {
                point_position = i;
                break;
            }
        }

        if (-1 == point_position) {
            for (i = 0; i < string_length_from_numeric; i++) {
                if (string_from_numeric[i] == '+' || string_from_numeric[i] == '-') {
                    continue;
                }
                if ('0' == string_from_numeric[i] && false == integer_flag) {
                    continue;
                } else {
                    integer_flag = true;
                    width_of_integer++;
                }
            }
        } else {
            for (i = 0; i < point_position; i++) {
                if (string_from_numeric[i] == '+' || string_from_numeric[i] == '-') {
                    continue;
                }
                if (string_from_numeric[i] == '0' && integer_flag == false) {
                    continue;
                } else {
                    integer_flag = true;
                    width_of_integer++;
                }
            }

            for (i = string_length_from_numeric - 1; i > point_position; i--) {
                if (string_from_numeric[i] == '0' && decimal_flag == false) {
                    continue;
                } else {
                    decimal_flag = true;
                    width_of_decimal++;
                }
            }
        }

        if (0 == width_of_integer && 0 == width_of_decimal) {
            result = NUMERIC_ZERO_DATALENGTH;
        } else {
            result = (width_of_integer + 3) / 4 * 2 + (width_of_decimal + 3) / 4 * 2;
        }

        pfree_ext(string_from_numeric);
    }

    return result;
}


/*
 * Convert int16 value to numeric.
 */
void int128_to_numericvar(int128 val, NumericVar* var)
{
    uint128 uval, newuval;
    NumericDigit* ptr = NULL;
    int ndigits;

    /* int128 can require at most 39 decimal digits; add one for safety */
    alloc_var(var, 40 / DEC_DIGITS);
    if (val < 0) {
        var->sign = NUMERIC_NEG;
        uval = -val;
    } else {
        var->sign = NUMERIC_POS;
        uval = val;
    }
    var->dscale = 0;
    if (val == 0) {
        var->ndigits = 0;
        var->weight = 0;
        return;
    }
    ptr = var->digits + var->ndigits;
    ndigits = 0;
    do {
        ptr--;
        ndigits++;
        newuval = uval / NBASE;
        *ptr = uval - newuval * NBASE;
        uval = newuval;
    } while (uval);
    var->digits = ptr;
    var->ndigits = ndigits;
    var->weight = ndigits - 1;
}

/*
 * Convert numeric to int16, rounding if needed.
 *
 * If overflow, return false (no error is raised).  Return true if okay.
 */
static bool numericvar_to_int128(const NumericVar* var, int128* result)
{
    NumericDigit* digits = NULL;
    int ndigits;
    int weight;
    int i;
    int128 val;
    bool neg = false;
    NumericVar rounded;

    /* Round to nearest integer */
    init_var(&rounded);
    set_var_from_var(var, &rounded);
    round_var(&rounded, 0);

    /* Check for zero input */
    strip_var(&rounded);
    ndigits = rounded.ndigits;
    if (ndigits == 0) {
        *result = 0;
        free_var(&rounded);
        return true;
    }

    /*
     * For input like 10000000000, we must treat stripped digits as real. So
     * the loop assumes there are weight+1 digits before the decimal point.
     */
    weight = rounded.weight;
    Assert(weight >= 0 && ndigits <= weight + 1);

    /*
     * Construct the result. To avoid issues with converting a value
     * corresponding to INT128_MIN (which can't be represented as a positive 64
     * bit two's complement integer), accumulate value as a negative number.
     */
    digits = rounded.digits;
    neg = (rounded.sign == NUMERIC_NEG);
    val = -digits[0];
    for (i = 1; i <= weight; i++) {
        if (unlikely(pg_mul_s128_overflow(val, NBASE, &val))) {
            free_var(&rounded);
            return false;
        }

        if (i < ndigits) {
            if (unlikely(pg_sub_s128_overflow(val, digits[i], &val))) {
                free_var(&rounded);
                return false;
            }
        }
    }

    free_var(&rounded);

    if (!neg) {
        if (unlikely(val == PG_INT128_MIN))
            return false;
        val = -val;
    }
    *result = val;

    return true;
}

Datum int16_numeric(PG_FUNCTION_ARGS)
{
    int128 val = PG_GETARG_INT128(0);
    Numeric res;
    NumericVar result;

    init_var(&result);

    int128_to_numericvar(val, &result);

    res = make_result(&result);

    free_var(&result);

    PG_RETURN_NUMERIC(res);
}

int128 numeric_int16_internal(Numeric num)
{
    int128 result = 0;
    NumericVar x;
    uint16 numFlags = NUMERIC_NB_FLAGBITS(num);

    if (NUMERIC_FLAG_IS_NANORBI(numFlags)) {
        /* Handle Big Integer */
        if (NUMERIC_FLAG_IS_BI(numFlags))
            num = makeNumericNormal(num);
        /* XXX would it be better to return NULL? */
        else
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot convert NaN to int128")));
    }

    /* Convert to variable format and thence to int8 */
    init_var_from_num(num, &x);

    if (!numericvar_to_int128(&x, &result))
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("int128 out of range")));

    return result;
}

Datum numeric_int16(PG_FUNCTION_ARGS)
{
    Numeric num = PG_GETARG_NUMERIC(0);
    int128 result = numeric_int16_internal(num);
    PG_RETURN_INT128(result);
}

Datum numeric_bool(PG_FUNCTION_ARGS)
{
    Numeric num = PG_GETARG_NUMERIC(0);
    bool result = false;
    char* tmp = DatumGetCString(DirectFunctionCall1(numeric_out, NumericGetDatum(num)));

    if (strcmp(tmp, "0") != 0) {
        result = true;
    }

    pfree_ext(tmp);

    PG_RETURN_BOOL(result);
}

Datum bool_numeric(PG_FUNCTION_ARGS)
{
    int val = 1;
    if (PG_GETARG_BOOL(0) == false) {
        val = 0;
    }

    Numeric res;
    NumericVar result;

    init_var(&result);

    int64_to_numericvar((int64)val, &result);

    res = make_result(&result);

    free_var(&result);

    PG_RETURN_NUMERIC(res);
}
/* ----------------------------------------------------------------------
 *
 * Fast sum accumulator functions
 *
 * ----------------------------------------------------------------------
 */

static void accum_sum_add(NumericSumAccum *accum, NumericVar *val)
{
    int32   *accum_digits;
    int i, val_i;
    int val_ndigits;
    NumericDigit *val_digits;

    /*
     * If we have accumulated too many values since the last carry
     * propagation, do it now, to avoid overflowing.  (We could allow more
     * than NBASE - 1, if we reserved two extra digits, rather than one, for
     * carry propagation.  But even with NBASE - 1, this needs to be done so
     * seldom, that the performance difference is negligible.)
     */
    if (accum->num_uncarried == NBASE - 1) {
        accum_sum_carry(accum);
    }

    /*
     * Adjust the weight or scale of the old value, so that it can accommodate
     * the new value.
     */
    accum_sum_rescale(accum, val);

    /* */
    if (val->sign == NUMERIC_POS) {
        accum_digits = accum->pos_digits;
    } else {
        accum_digits = accum->neg_digits;
    }
    /* copy these values into local vars for speed in loop */
    val_ndigits = val->ndigits;
    val_digits = val->digits;

    i = accum->weight - val->weight;
    for (val_i = 0; val_i < val_ndigits; val_i++) {
        accum_digits[i] += (int32) val_digits[val_i];
        i++;
    }

    accum->num_uncarried++;
}

static void accum_sum_carry(NumericSumAccum *accum)
{
    int          i;
    int          ndigits;
    int32       *dig;
    int32        carry;
    int32        newdig = 0;

    if (accum->num_uncarried == 0) {
        return;
    }

    Assert(accum->pos_digits[0] == 0 && accum->neg_digits[0] == 0);

    ndigits = accum->ndigits;

    dig = accum->pos_digits;
    carry = 0;
    for (i = ndigits - 1; i >= 0; i--) {
        newdig = dig[i] + carry;
        if (newdig >= NBASE) {
            carry = newdig / NBASE;
            newdig -= carry * NBASE;
        } else {
            carry = 0;
        }
        dig[i] = newdig;
    }

    if (newdig > 0) {
        accum->have_carry_space = false;
    }

    dig = accum->neg_digits;
    carry = 0;
    for (i = ndigits - 1; i >= 0; i--) {
        newdig = dig[i] + carry;
        if (newdig >= NBASE) {
            carry = newdig / NBASE;
            newdig -= carry * NBASE;
        } else {
            carry = 0;
        }
        dig[i] = newdig;
    }
    if (newdig > 0) {
        accum->have_carry_space = false;
    }

    accum->num_uncarried = 0;
}

static void accum_sum_rescale(NumericSumAccum *accum, NumericVar *val)
{
    int            old_weight = accum->weight;
    int            old_ndigits = accum->ndigits;
    int            accum_ndigits;
    int            accum_weight;
    int            accum_rscale;
    int            val_rscale;

    accum_weight = old_weight;
    accum_ndigits = old_ndigits;

    if (val->weight >= accum_weight) {
        accum_weight = val->weight + 1;
        accum_ndigits = accum_ndigits + (accum_weight - old_weight);
    } else if (!accum->have_carry_space) {
        accum_weight++;
        accum_ndigits++;
    }

    /* Is the new value wider on the right side? */
    accum_rscale = accum_ndigits - accum_weight - 1;
    val_rscale = val->ndigits - val->weight - 1;
    if (val_rscale > accum_rscale) {
        accum_ndigits = accum_ndigits + (val_rscale - accum_rscale);
    }

    if (accum_ndigits != old_ndigits ||
        accum_weight != old_weight) {
        int32       *new_pos_digits;
        int32       *new_neg_digits;
        int          weightdiff;
        size_t      size = (size_t)accum_ndigits * sizeof(int32);

        weightdiff = accum_weight - old_weight;

        new_pos_digits = (int32*)palloc0(size);
        new_neg_digits = (int32*)palloc0(size);

        if (accum->pos_digits) {
            errno_t rc = memcpy_s(&new_pos_digits[weightdiff], size, accum->pos_digits,
                (size_t)old_ndigits * sizeof(int32));
            securec_check(rc, "\0", "\0");
            pfree(accum->pos_digits);

            rc = memcpy_s(&new_neg_digits[weightdiff], size, accum->neg_digits,
                old_ndigits * sizeof(int32));
            securec_check(rc, "\0", "\0");
            pfree(accum->neg_digits);
        }

        accum->pos_digits = new_pos_digits;
        accum->neg_digits = new_neg_digits;

        accum->weight = accum_weight;
        accum->ndigits = accum_ndigits;

        Assert(accum->pos_digits[0] == 0 && accum->neg_digits[0] == 0);
        accum->have_carry_space = true;
    }

    if (val->dscale > accum->dscale)
        accum->dscale = val->dscale;
}

static void accum_sum_final(NumericSumAccum *accum, NumericVar *result)
{
    int            i;
    NumericVar    pos_var;
    NumericVar    neg_var;

    if (accum->ndigits == 0) {
        set_var_from_var(&const_zero, result);
        return;
    }

    /* Perform final carry */
    accum_sum_carry(accum);

    /* Create NumericVars representing the positive and negative sums */
    init_var(&pos_var);
    init_var(&neg_var);

    pos_var.ndigits = neg_var.ndigits = accum->ndigits;
    pos_var.weight = neg_var.weight = accum->weight;
    pos_var.dscale = neg_var.dscale = accum->dscale;
    pos_var.sign = NUMERIC_POS;
    neg_var.sign = NUMERIC_NEG;

    pos_var.buf = pos_var.digits = digitbuf_alloc(accum->ndigits);
    neg_var.buf = neg_var.digits = digitbuf_alloc(accum->ndigits);

    for (i = 0; i < accum->ndigits; i++) {
        Assert(accum->pos_digits[i] < NBASE);
        pos_var.digits[i] = (int16) accum->pos_digits[i];

        Assert(accum->neg_digits[i] < NBASE);
        neg_var.digits[i] = (int16) accum->neg_digits[i];
    }

    /* And add them together */
    add_var(&pos_var, &neg_var, result);

    /* Remove leading/trailing zeroes */
    strip_var(result);
}

bool numeric_agg_trans_initvalisnull(Oid transfn_oid, bool initvalisnull)
{
    if (transfn_oid == 5440 || transfn_oid == 5442 || transfn_oid == 5439) {
        return true;
    }

    return initvalisnull;
}

void numeric_transfn_info_change(Oid aggfn_oid, Oid *transfn_oid, Oid *transtype)
{
    Oid old_type = *transtype;
    *transtype = 2281;

    switch (aggfn_oid) {
        case 2100:
        case 2107:
            *transfn_oid = 5439;
            break;
        case 2103:
        case 2114:
            *transfn_oid = 5442;
            break;
        case 2723:
        case 2646:
        case 2153:
        case 2729:
        case 2717:
        case 2159:
            *transfn_oid = 5440;
            break;
        default:
            *transtype = old_type;
    }
}

void numeric_finalfn_info_change(Oid aggfn_oid, Oid *finalfn_oid)
{
    switch (aggfn_oid) {
        case 2100:
        case 2103:
            *finalfn_oid = 5441;
            break;
        case 2107:
        case 2114:
            *finalfn_oid = 5435;
            break;
        case 2723:
            *finalfn_oid = 5445;
            break;
        case 2646:
        case 2153:
            *finalfn_oid = 5446;
            break;
        case 2729:
            *finalfn_oid = 5443;
            break;
        case 2717:
        case 2159:
            *finalfn_oid = 5444;
            break;
        default:
            return;
    }
}

void numeric_aggfn_info_change(Oid aggfn_oid, Oid *transfn_oid, Oid *transtype, Oid *finalfn_oid)
{
    numeric_transfn_info_change(aggfn_oid, transfn_oid, transtype);
    numeric_finalfn_info_change(aggfn_oid, finalfn_oid);
}
