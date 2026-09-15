/*
 * numeric_pipeline.h
 *              Inline helpers for the numeric expression pipeline.
 *
 * The dedicated numeric interpreter opcodes (EEOP_NUMERIC_*) are compiled in
 * execExpr.cpp and evaluated by ExecInterpExpr.  A step runs once per row, so
 * these helpers are static inline here to let the interpreter fold them into
 * the opcode cases; the heavy arithmetic (add_var/mul_var/div_var/...) and the
 * big-integer kernels stay out of line.
 *
 * Only numeric.cpp and execExprInterp.cpp include this header.
 */
#ifndef NUMERIC_PIPELINE_H
#define NUMERIC_PIPELINE_H

#include "utils/biginteger.h"
#include "utils/numeric.h"

/* shared constants used by the register ops */
static NumericDigit const_zero_data[1] = {0};
static NumericVar const_zero = {0, 0, NUMERIC_POS, 0, NULL, const_zero_data};

static NumericDigit const_one_data[1] = {1};
static NumericVar const_one = {1, 0, NUMERIC_POS, 0, NULL, const_one_data};

/*
 * @Description: call corresponding big integer operator functions.
 *
 * @IN op: template parameter, assign the operation name, e.g. add, sub, etc.
 * @IN larg: left-hand operand of operator.
 * @IN rarg: right-hand operand of operator.
 * @return: Datum - the datum data points to result of letfc op rightc.
 */
template <biop op>
static inline Datum bipickfun(Numeric leftc, Numeric rightc)
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

static inline void alloc_var(NumericVar* var, int ndigits)
{
    digitbuf_free(var);
    init_alloc_var(var, ndigits);
}

static inline void zero_var(NumericVar* var)
{
    digitbuf_free(var);
    quick_init_var(var);
    var->ndigits = 0;
    var->weight = 0;         /* by convention; doesn't really matter */
    var->sign = NUMERIC_POS; /* anything but NAN... */
}

static inline void set_var_from_num(Numeric num, NumericVar* dest)
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

static inline int select_div_scale(NumericVar* var1, NumericVar* var2)
{
    int weight1;
    int weight2;
    int qweight;
    int i;
    NumericDigit firstdigit1;
    NumericDigit firstdigit2;
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
    if (firstdigit1 <= firstdigit2) {
        qweight--;
    }

    /* Select result scale */
    rscale = NUMERIC_MIN_SIG_DIGITS - qweight * DEC_DIGITS;
    rscale = Max(rscale, var1->dscale);
    rscale = Max(rscale, var2->dscale);
    rscale = Max(rscale, NUMERIC_MIN_DISPLAY_SCALE);
    rscale = Min(rscale, NUMERIC_MAX_DISPLAY_SCALE);

    return rscale;
}

/*
 * Helpers for the numeric register pipeline used by the expression
 * interpreter.  Registers hold either a packed big-integer numeric, which
 * keeps the int64/int128 fast path, or an unpacked NumericVar.
 */

static inline void numeric_var_set_nan(NumericVar *var)
{
    var->sign = NUMERIC_NAN;
    var->ndigits = 0;
    var->weight = 0;
}

static inline void numeric_round_var(NumericVar *var, int32 scale)
{
    scale = Max(scale, -NUMERIC_MAX_RESULT_SCALE);
    scale = Min(scale, NUMERIC_MAX_RESULT_SCALE);

    round_var(var, scale);

    /* We don't allow negative output dscale */
    if (scale < 0) {
        var->dscale = 0;
    }
}

static inline void numeric_trunc_var(NumericVar *var, int32 scale)
{
    scale = Max(scale, -NUMERIC_MAX_RESULT_SCALE);
    scale = Min(scale, NUMERIC_MAX_RESULT_SCALE);

    trunc_var(var, scale);

    /* We don't allow negative output dscale */
    if (scale < 0) {
        var->dscale = 0;
    }
}

static inline void copy_var_from_var(const NumericVar *value, NumericVar *dest)
{
    alloc_var(dest, value->ndigits + 1);
    dest->ndigits = value->ndigits;
    if (value->ndigits > 0) {
        errno_t rc = memcpy_s(dest->digits, (value->ndigits + 1) * sizeof(NumericDigit),
                              value->digits, value->ndigits * sizeof(NumericDigit));
        securec_check(rc, "\0", "\0");
    }
    dest->weight = value->weight;
    dest->sign = value->sign;
    dest->dscale = value->dscale;
}

/* Unpack a register into a NumericVar usable for read-only arithmetic */
static inline void numeric_reg_unpack(const NumericReg *arg, NumericVar *dest)
{
    if (arg->bi != NULL) {
        init_var_from_num(makeNumericNormal(arg->bi), dest);
    } else {
        *dest = arg->var;
    }
}

static inline void numeric_reg_extract(Datum d, NumericReg *res)
{
    struct varlena *attr = (struct varlena *)DatumGetPointer(d);

    /*
     * Fast path for 1-byte-varlena leaves: the payload is the NumericChoice
     * union, which the _CHOICE macros read directly, so no detoast copy is
     * needed.  The digits view points into the live tuple, which is valid
     * for the duration of the row; the register's own ndb buffer is only
     * used as the scratch for the buffered-detour path below.  A 1-byte
     * varlena payload may be big-integer, long-form or NaN encoded, so only
     * plain short/long-form numerics can be unpacked in place.
     */
    if (VARATT_IS_SHORT(attr)) {
        union NumericChoice *choice = (union NumericChoice *)VARDATA_SHORT(attr);

        if (likely(!NUMERIC_IS_NANORBI_CHOICE(choice))) {
            res->bi = NULL;
            res->var.ndigits = (VARSIZE_SHORT(attr) - NUMERIC_HEADER_SIZE_CHOICE_1B(choice)) / sizeof(NumericDigit);
            res->var.weight = NUMERIC_WEIGHT_CHOICE(choice);
            res->var.sign = NUMERIC_SIGN_CHOICE(choice);
            res->var.dscale = NUMERIC_DSCALE_CHOICE(choice);
            res->var.digits = NUMERIC_DIGITS_CHOICE(choice);
            res->var.buf = res->var.ndb; /* read-only view; digitbuf_free() is a no-op */
            return;
        }
    }

    /*
     * Buffered detoast.  This expands 1-byte-varlena datums back to the
     * 4-byte layout the Numeric macros assume.
     */
    Numeric num = DatumGetNumericBuffered(d, (char *)res->var.ndb, sizeof(res->var.ndb));
    if (NUMERIC_IS_BI(num)) {
        res->bi = num;
        return;
    }
    res->bi = NULL;
    init_var_from_num(num, &res->var);
}

/*
 * Pack var into the caller's buffer of bufsz bytes, mirroring
 * make_result_opt_error().  Returns NULL when the result does not fit, in
 * which case the caller falls back to make_result().
 */
static inline Numeric pack_var_into(NumericVar *var, Numeric buf, Size bufsz)
{
    NumericDigit *digits = var->digits;
    int weight = var->weight;
    int sign = var->sign;
    int n;
    Size len;

    if (sign == NUMERIC_NAN) {
        if (bufsz < NUMERIC_HDRSZ_SHORT) {
            return NULL;
        }
        SET_VARSIZE(buf, NUMERIC_HDRSZ_SHORT);
        buf->choice.n_header = NUMERIC_NAN;
        return buf;
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
        if (len > bufsz) {
            return NULL;
        }
        SET_VARSIZE(buf, len);
        buf->choice.n_short.n_header =
            (sign == NUMERIC_NEG ? (NUMERIC_SHORT | NUMERIC_SHORT_SIGN_MASK) : NUMERIC_SHORT) |
            (var->dscale << NUMERIC_SHORT_DSCALE_SHIFT) | (weight < 0 ? NUMERIC_SHORT_WEIGHT_SIGN_MASK : 0) |
            (weight & NUMERIC_SHORT_WEIGHT_MASK);
    } else {
        len = NUMERIC_HDRSZ + n * sizeof(NumericDigit);
        if (len > bufsz) {
            return NULL;
        }
        SET_VARSIZE(buf, len);
        buf->choice.n_long.n_sign_dscale = sign | (var->dscale & NUMERIC_DSCALE_MASK);
        buf->choice.n_long.n_weight = weight;
    }
    if (n > 0) {
        errno_t rc = memcpy_s(NUMERIC_DIGITS(buf), n * sizeof(NumericDigit), digits, n * sizeof(NumericDigit));
        securec_check(rc, "\0", "\0");
    }
    Assert(NUMERIC_NDIGITS(buf) == (unsigned int)(n));

    /* Check for overflow of int16 fields */
    if (NUMERIC_WEIGHT(buf) != weight || NUMERIC_DSCALE(buf) != var->dscale) {
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("value overflows numeric format")));
    }

    return buf;
}

static inline Datum numeric_reg_pack(NumericReg *res, Numeric packbuf)
{
    if (res->bi != NULL) {
        return NumericGetDatum(res->bi);
    }

    Numeric r = pack_var_into(&res->var, packbuf, NUMERIC_PACKBUF_SIZE);
    if (r == NULL) {
        return NumericGetDatum(make_result(&res->var));
    }
    return NumericGetDatum(r);
}

/*
 * Three-way comparison of two registers.  Big-integer operands use the
 * packed fast path; everything else is unpacked and compared with cmp_var().
 * NaN ordering matches cmp_numerics().
 */
static inline int numeric_reg_cmp3(const NumericReg *arg1, const NumericReg *arg2)
{
    if (arg1->bi != NULL && arg2->bi != NULL) {
        return DatumGetInt32(bipickfun<BICMP>(arg1->bi, arg2->bi));
    }

    NumericVar var1;
    NumericVar var2;
    int cmp;

    numeric_reg_unpack(arg1, &var1);
    numeric_reg_unpack(arg2, &var2);

    if (var1.sign == NUMERIC_NAN) {
        return (var2.sign == NUMERIC_NAN) ? 0 : 1;
    }
    if (var2.sign == NUMERIC_NAN) {
        return -1;
    }
    cmp = cmp_var(&var1, &var2);
    return (cmp < 0) ? -1 : ((cmp > 0) ? 1 : 0);
}

static inline void numeric_reg_eq(NumericReg *arg1, NumericReg *arg2, Datum *resvalue, bool *resnull)
{
    *resnull = arg1->isnull || arg2->isnull;
    if (*resnull) {
        return;
    }
    *resvalue = BoolGetDatum(numeric_reg_cmp3(arg1, arg2) == 0);
    *resnull = false;
}

static inline void numeric_reg_ne(NumericReg *arg1, NumericReg *arg2, Datum *resvalue, bool *resnull)
{
    *resnull = arg1->isnull || arg2->isnull;
    if (*resnull) {
        return;
    }
    *resvalue = BoolGetDatum(numeric_reg_cmp3(arg1, arg2) != 0);
    *resnull = false;
}

static inline void numeric_reg_le(NumericReg *arg1, NumericReg *arg2, Datum *resvalue, bool *resnull)
{
    *resnull = arg1->isnull || arg2->isnull;
    if (*resnull) {
        return;
    }
    *resvalue = BoolGetDatum(numeric_reg_cmp3(arg1, arg2) <= 0);
    *resnull = false;
}

static inline void numeric_reg_lt(NumericReg *arg1, NumericReg *arg2, Datum *resvalue, bool *resnull)
{
    *resnull = arg1->isnull || arg2->isnull;
    if (*resnull) {
        return;
    }
    *resvalue = BoolGetDatum(numeric_reg_cmp3(arg1, arg2) < 0);
    *resnull = false;
}

static inline void numeric_reg_ge(NumericReg *arg1, NumericReg *arg2, Datum *resvalue, bool *resnull)
{
    *resnull = arg1->isnull || arg2->isnull;
    if (*resnull) {
        return;
    }
    *resvalue = BoolGetDatum(numeric_reg_cmp3(arg1, arg2) >= 0);
    *resnull = false;
}

static inline void numeric_reg_gt(NumericReg *arg1, NumericReg *arg2, Datum *resvalue, bool *resnull)
{
    *resnull = arg1->isnull || arg2->isnull;
    if (*resnull) {
        return;
    }
    *resvalue = BoolGetDatum(numeric_reg_cmp3(arg1, arg2) > 0);
    *resnull = false;
}

/*
 * Store the result of a big-int fast-path operation into a register.  When
 * the value overflows the int64/int128 range the big-int code falls back to
 * the generic numeric functions and returns a regular (non-big-integer)
 * Numeric; keep that as an unpacked var so a later step never passes it to
 * bipickfun() again.
 */
static inline void numeric_reg_set_bi_result(Datum d, NumericReg *res)
{
    Numeric num = DatumGetNumeric(d);

    if (NUMERIC_IS_BI(num)) {
        res->bi = num;
        return;
    }
    res->bi = NULL;
    init_var_from_num(num, &res->var);
}

/*
 * Initialize the result register of a binary var-op and propagate NULL
 * inputs.  Returns true when the result is NULL and the caller should stop.
 */
static inline bool numeric_reg_prep_binary(const NumericReg *arg1, const NumericReg *arg2, NumericReg *res)
{
    res->bi = NULL;
    quick_init_var(&res->var);
    res->isnull = arg1->isnull || arg2->isnull;
    return res->isnull;
}

static inline void numeric_reg_add(NumericReg *arg1, NumericReg *arg2, NumericReg *res)
{
    if (numeric_reg_prep_binary(arg1, arg2, res)) {
        return;
    }
    if (arg1->bi != NULL && arg2->bi != NULL) {
        numeric_reg_set_bi_result(bipickfun<BIADD>(arg1->bi, arg2->bi), res);
        return;
    }
    NumericVar var1;
    NumericVar var2;
    numeric_reg_unpack(arg1, &var1);
    numeric_reg_unpack(arg2, &var2);
    if (var1.sign == NUMERIC_NAN || var2.sign == NUMERIC_NAN) {
        numeric_var_set_nan(&res->var);
        return;
    }
    add_var(&var1, &var2, &res->var);
}

static inline void numeric_reg_sub(NumericReg *arg1, NumericReg *arg2, NumericReg *res)
{
    if (numeric_reg_prep_binary(arg1, arg2, res)) {
        return;
    }
    if (arg1->bi != NULL && arg2->bi != NULL) {
        numeric_reg_set_bi_result(bipickfun<BISUB>(arg1->bi, arg2->bi), res);
        return;
    }
    NumericVar var1;
    NumericVar var2;
    numeric_reg_unpack(arg1, &var1);
    numeric_reg_unpack(arg2, &var2);
    if (var1.sign == NUMERIC_NAN || var2.sign == NUMERIC_NAN) {
        numeric_var_set_nan(&res->var);
        return;
    }
    sub_var(&var1, &var2, &res->var);
}

static inline void numeric_reg_mul(NumericReg *arg1, NumericReg *arg2, NumericReg *res)
{
    if (numeric_reg_prep_binary(arg1, arg2, res)) {
        return;
    }
    if (arg1->bi != NULL && arg2->bi != NULL) {
        numeric_reg_set_bi_result(bipickfun<BIMUL>(arg1->bi, arg2->bi), res);
        return;
    }
    NumericVar var1;
    NumericVar var2;
    numeric_reg_unpack(arg1, &var1);
    numeric_reg_unpack(arg2, &var2);
    if (var1.sign == NUMERIC_NAN || var2.sign == NUMERIC_NAN) {
        numeric_var_set_nan(&res->var);
        return;
    }
    mul_var(&var1, &var2, &res->var, var1.dscale + var2.dscale);
}

static inline void numeric_reg_div(NumericReg *arg1, NumericReg *arg2, NumericReg *res)
{
    if (numeric_reg_prep_binary(arg1, arg2, res)) {
        return;
    }
    if (arg1->bi != NULL && arg2->bi != NULL) {
        numeric_reg_set_bi_result(bipickfun<BIDIV>(arg1->bi, arg2->bi), res);
        return;
    }
    NumericVar var1;
    NumericVar var2;
    numeric_reg_unpack(arg1, &var1);
    numeric_reg_unpack(arg2, &var2);
    if (var1.sign == NUMERIC_NAN || var2.sign == NUMERIC_NAN) {
        numeric_var_set_nan(&res->var);
        return;
    }
    div_var(&var1, &var2, &res->var, select_div_scale(&var1, &var2), true);
}

static inline void numeric_reg_mod(NumericReg *arg1, NumericReg *arg2, NumericReg *res)
{
    if (numeric_reg_prep_binary(arg1, arg2, res)) {
        return;
    }
    NumericVar var1;
    NumericVar var2;
    numeric_reg_unpack(arg1, &var1);
    numeric_reg_unpack(arg2, &var2);
    if (var1.sign == NUMERIC_NAN || var2.sign == NUMERIC_NAN) {
        numeric_var_set_nan(&res->var);
        return;
    }
    if (cmp_var(&var2, &const_zero) == 0) {
        ereport(ERROR, (errcode(ERRCODE_DIVISION_BY_ZERO), errmsg("division by zero")));
    }
    mod_var(&var1, &var2, &res->var);
}

static inline void numeric_reg_div_trunc(NumericReg *arg1, NumericReg *arg2, NumericReg *res)
{
    if (numeric_reg_prep_binary(arg1, arg2, res)) {
        return;
    }
    NumericVar var1;
    NumericVar var2;
    numeric_reg_unpack(arg1, &var1);
    numeric_reg_unpack(arg2, &var2);
    if (var1.sign == NUMERIC_NAN || var2.sign == NUMERIC_NAN) {
        numeric_var_set_nan(&res->var);
        return;
    }
    div_var(&var1, &var2, &res->var, 0, false);
}

/*
 * Initialize the result register of a unary var-op and propagate a NULL
 * input.  Returns true when the result is NULL and the caller should stop.
 */
static inline bool numeric_reg_prep_unary(const NumericReg *arg1, NumericReg *res)
{
    res->bi = NULL;
    quick_init_var(&res->var);
    res->isnull = arg1->isnull;
    return res->isnull;
}

static inline void numeric_reg_uplus(NumericReg *arg1, NumericReg *res)
{
    if (numeric_reg_prep_unary(arg1, res)) {
        return;
    }
    if (arg1->bi != NULL) {
        res->bi = arg1->bi;
        return;
    }
    if (arg1->var.sign == NUMERIC_NAN) {
        numeric_var_set_nan(&res->var);
        return;
    }
    copy_var_from_var(&arg1->var, &res->var);
}

static inline void numeric_reg_abs(NumericReg *arg1, NumericReg *res)
{
    if (numeric_reg_prep_unary(arg1, res)) {
        return;
    }
    if (arg1->bi != NULL) {
        set_var_from_num(makeNumericNormal(arg1->bi), &res->var);
    } else {
        if (arg1->var.sign == NUMERIC_NAN) {
            numeric_var_set_nan(&res->var);
            return;
        }
        copy_var_from_var(&arg1->var, &res->var);
    }
    res->var.sign = NUMERIC_POS;
}

static inline void numeric_reg_uminus(NumericReg *arg1, NumericReg *res)
{
    if (numeric_reg_prep_unary(arg1, res)) {
        return;
    }
    if (arg1->bi != NULL) {
        set_var_from_num(makeNumericNormal(arg1->bi), &res->var);
    } else {
        if (arg1->var.sign == NUMERIC_NAN) {
            numeric_var_set_nan(&res->var);
            return;
        }
        copy_var_from_var(&arg1->var, &res->var);
    }
    if (res->var.ndigits != 0) {
        if (res->var.sign == NUMERIC_POS) {
            res->var.sign = NUMERIC_NEG;
        } else {
            res->var.sign = NUMERIC_POS;
        }
    }
}

static inline void numeric_reg_sign(NumericReg *arg1, NumericReg *res)
{
    if (numeric_reg_prep_unary(arg1, res)) {
        return;
    }
    NumericVar var;
    numeric_reg_unpack(arg1, &var);
    if (var.sign == NUMERIC_NAN) {
        numeric_var_set_nan(&res->var);
        return;
    }
    if (var.ndigits == 0) {
        zero_var(&res->var);
        res->var.dscale = 0;
    } else {
        copy_var_from_var(&const_one, &res->var);
        res->var.sign = var.sign;
    }
}

static inline void numeric_reg_inc(NumericReg *arg1, NumericReg *res)
{
    if (numeric_reg_prep_unary(arg1, res)) {
        return;
    }
    NumericVar var;
    numeric_reg_unpack(arg1, &var);
    if (var.sign == NUMERIC_NAN) {
        numeric_var_set_nan(&res->var);
        return;
    }
    add_var(&var, &const_one, &res->var);
}

static inline void numeric_reg_ceil(NumericReg *arg1, NumericReg *res)
{
    if (numeric_reg_prep_unary(arg1, res)) {
        return;
    }
    NumericVar var;
    numeric_reg_unpack(arg1, &var);
    if (var.sign == NUMERIC_NAN) {
        numeric_var_set_nan(&res->var);
        return;
    }
    ceil_var(&var, &res->var);
}

static inline void numeric_reg_floor(NumericReg *arg1, NumericReg *res)
{
    if (numeric_reg_prep_unary(arg1, res)) {
        return;
    }
    NumericVar var;
    numeric_reg_unpack(arg1, &var);
    if (var.sign == NUMERIC_NAN) {
        numeric_var_set_nan(&res->var);
        return;
    }
    floor_var(&var, &res->var);
}

static inline void numeric_reg_scale_var(NumericReg *arg1, int32 scale, NumericReg *res,
                                         void (*scale_op)(NumericVar *, int32))
{
    if (arg1 == res) {
        /* in-place scale op on an owned register: input and output share storage */
        if (arg1->isnull) {
            return; /* NULL propagates; the register may hold a stale var, don't touch it */
        }
        if (arg1->bi != NULL) {
            if (NUMERIC_BI_SCALE(arg1->bi) == 0 && scale == 0) {
                return; /* integer value is unchanged */
            }
            Numeric bi = arg1->bi;
            arg1->bi = NULL;
            set_var_from_num(makeNumericNormal(bi), &arg1->var);
            scale_op(&arg1->var, scale);
            return;
        }
        if (arg1->var.sign == NUMERIC_NAN) {
            return; /* NaN is unchanged */
        }
        scale_op(&arg1->var, scale);
        return;
    }

    res->bi = NULL;
    quick_init_var(&res->var);
    res->isnull = arg1->isnull;
    if (res->isnull) {
        return;
    }
    if (arg1->bi != NULL) {
        if (NUMERIC_BI_SCALE(arg1->bi) == 0 && scale == 0) {
            res->bi = arg1->bi;
            return;
        }
        set_var_from_num(makeNumericNormal(arg1->bi), &res->var);
        scale_op(&res->var, scale);
        return;
    }

    if (arg1->var.sign == NUMERIC_NAN) {
        numeric_var_set_nan(&res->var);
        return;
    }

    copy_var_from_var(&arg1->var, &res->var);
    scale_op(&res->var, scale);
}

static inline void numeric_reg_round(NumericReg *arg1, int32 scale, NumericReg *res)
{
    numeric_reg_scale_var(arg1, scale, res, numeric_round_var);
}

static inline void numeric_reg_trunc(NumericReg *arg1, int32 scale, NumericReg *res)
{
    numeric_reg_scale_var(arg1, scale, res, numeric_trunc_var);
}

#endif /* NUMERIC_PIPELINE_H */
