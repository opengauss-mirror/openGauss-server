/*
 * Copyright (c) 2024 Huawei Technologies Co.,Ltd.
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
 * vector.cpp
 *
 * IDENTIFICATION
 *        src/common/backend/utils/adt/vector.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include <cmath>

#ifdef __aarch64__
#include <arm_neon.h>
#else
#include <immintrin.h>
#endif

#include "access/datavec/bitvec.h"
#include "catalog/pg_type.h"
#include "fmgr.h"
#include "access/datavec/halfutils.h"
#include "access/datavec/halfvec.h"
#include "access/datavec/hnsw.h"
#include "access/datavec/ivfflat.h"
#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "port.h" /* for strtof() */
#include "access/datavec/shortest_dec.h"
#include "access/datavec/sparsevec.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/numeric.h"
#include "commands/extension.h"
#include "knl/knl_session.h"
#include "access/datavec/vector.h"

#define TYPALIGN_DOUBLE 'd'
#define TYPALIGN_INT 'i'

#define STATE_DIMS(x) (ARR_DIMS(x)[0] - 1)
#define CreateStateDatums(dim) palloc(sizeof(Datum) * ((dim) + 1))

#if defined(USE_TARGET_CLONES) && !defined(__FMA__)
#define VECTOR_TARGET_CLONES __attribute__((target_clones("default", "fma")))
#else
#define VECTOR_TARGET_CLONES
#endif

#define MarkGUCPrefixReserved(x) EmitWarningsOnPlaceholders(x)

/*
 * Ensure same dimensions
 */
static inline void CheckDims(Vector *a, Vector *b)
{
    if (a->dim != b->dim)
        ereport(ERROR,
                (errcode(ERRCODE_DATA_EXCEPTION), errmsg("different vector dimensions %d and %d", a->dim, b->dim)));
}

/*
 * Ensure expected dimensions
 */
static inline void CheckExpectedDim(int32 typmod, int dim)
{
    if (typmod != -1 && typmod != dim)
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("expected %d dimensions, not %d", typmod, dim)));
}

/*
 * Ensure valid dimensions
 */
static inline void CheckDim(int dim)
{
    if (dim < 1)
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("vector must have at least 1 dimension")));

    if (dim > VECTOR_MAX_DIM)
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                        errmsg("vector cannot have more than %d dimensions", VECTOR_MAX_DIM)));
}

/*
 * Ensure finite element
 */
static inline void CheckElement(float value)
{
    if (isnan(value))
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("NaN not allowed in vector")));

    if (isinf(value))
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("infinite value not allowed in vector")));
}

/*
 * Allocate and initialize a new vector
 */
Vector *InitVector(int dim)
{
    Vector *result;
    int size;

    size = VECTOR_SIZE(dim);
    result = (Vector *)palloc0(size);
    SET_VARSIZE(result, size);
    result->dim = dim;

    return result;
}

/*
 * Check for whitespace, since array_isspace() is static
 */
static inline bool VectorIsspace(char ch)
{
    if (ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' || ch == '\v' || ch == '\f') {
        return true;
    }
    return false;
}

/*
 * Check state array
 */
static float8 *CheckStateArray(ArrayType *statearray, const char *caller)
{
    if (ARR_NDIM(statearray) != 1 || ARR_DIMS(statearray)[0] < 1 || ARR_HASNULL(statearray))
        elog(ERROR, "%s: expected state array", caller);
    return (float8 *)ARR_DATA_PTR(statearray);
}

static pg_noinline void float_overflow_error(void)
{
    ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("value out of range: overflow")));
}

static pg_noinline void float_underflow_error(void)
{
    ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("value out of range: underflow")));
}

/*
 * Convert textual representation to internal representation
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(vector_in);
Datum vector_in(PG_FUNCTION_ARGS)
{
    char *lit = PG_GETARG_CSTRING(0);
    int32 typmod = PG_GETARG_INT32(2);
    float x[VECTOR_MAX_DIM];
    int dim = 0;
    char *pt = lit;
    Vector *result;

    while (VectorIsspace(*pt)) {
        pt++;
    }

    if (*pt != '[')
        ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                        errmsg("invalid input syntax for type vector: \"%s\"", lit),
                        errdetail("Vector contents must start with \"[\".")));

    pt++;

    while (VectorIsspace(*pt)) {
        pt++;
    }

    if (*pt == ']') {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("vector must have at least 1 dimension")));
    }

    for (;;) {
        float val;
        char *stringEnd;

        if (dim == VECTOR_MAX_DIM)
            ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                            errmsg("vector cannot have more than %d dimensions", VECTOR_MAX_DIM)));

        while (VectorIsspace(*pt)) {
            pt++;
        }

        /* Check for empty string like float4in */
        if (*pt == '\0')
            ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                            errmsg("invalid input syntax for type vector: \"%s\"", lit)));

        errno = 0;

        /* Use strtof like float4in to avoid a double-rounding problem */
        /* Postgres sets LC_NUMERIC to C on startup */
        val = strtof(pt, &stringEnd);

        if (stringEnd == pt) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                            errmsg("invalid input syntax for type vector: \"%s\"", lit)));
        }

        /* Check for range error like float4in */
        if (errno == ERANGE && isinf(val))
            ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                            errmsg("\"%s\" is out of range for type vector", pnstrdup(pt, stringEnd - pt))));

        CheckElement(val);
        x[dim++] = val;

        pt = stringEnd;

        while (VectorIsspace(*pt)) {
            pt++;
        }

        if (*pt == ',') {
            pt++;
        } else if (*pt == ']') {
            pt++;
            break;
        } else {
            ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                            errmsg("invalid input syntax for type vector: \"%s\"", lit)));
        }
    }

    /* Only whitespace is allowed after the closing brace */
    while (VectorIsspace(*pt)) {
        pt++;
    }

    if (*pt != '\0')
        ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                        errmsg("invalid input syntax for type vector: \"%s\"", lit),
                        errdetail("Junk after closing right brace.")));

    CheckDim(dim);
    CheckExpectedDim(typmod, dim);

    result = InitVector(dim);
    for (int i = 0; i < dim; i++) {
        result->x[i] = x[i];
    }

    PG_RETURN_POINTER(result);
}

#define AppendChar(ptr, c) (*(ptr)++ = (c))
#define AppendFloat(ptr, f) ((ptr) += FloatToShortestDecimalBufn((f), (ptr)))

/*
 * Convert internal representation to textual representation
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(vector_out);
Datum vector_out(PG_FUNCTION_ARGS)
{
    Vector *vector = PG_GETARG_VECTOR_P(0);
    int dim = vector->dim;
    char *buf;
    char *ptr;

    /*
     * Need:
     *
     * dim * (FLOAT_SHORTEST_DECIMAL_LEN - 1) bytes for
     * FloatToShortestDecimalBufn
     *
     * dim - 1 bytes for separator
     *
     * 3 bytes for [, ], and \0
     */
    buf = (char *)palloc(FLOAT_SHORTEST_DECIMAL_LEN * dim + 2);
    ptr = buf;

    AppendChar(ptr, '[');

    for (int i = 0; i < dim; i++) {
        if (i > 0) {
            AppendChar(ptr, ',');
        }

        AppendFloat(ptr, vector->x[i]);
    }

    AppendChar(ptr, ']');
    *ptr = '\0';

    PG_FREE_IF_COPY(vector, 0);
    PG_RETURN_CSTRING(buf);
}

/*
 * Print vector - useful for debugging
 */
void PrintVector(char *msg, Vector *vector)
{
    char *out = DatumGetPointer(DirectFunctionCall1(vector_out, PointerGetDatum(vector)));

    elog(INFO, "%s = %s", msg, out);
    pfree(out);
}

/*
 * Convert type modifier
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(vector_typmod_in);
Datum vector_typmod_in(PG_FUNCTION_ARGS)
{
    ArrayType *ta = PG_GETARG_ARRAYTYPE_P(0);
    int32 *tl;
    int n;

    tl = ArrayGetIntegerTypmods(ta, &n);

    if (n != 1) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid type modifier")));
    }

    if (*tl < 1) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("dimensions for type vector must be at least 1")));
    }

    if (*tl > VECTOR_MAX_DIM) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("dimensions for type vector cannot exceed %d", VECTOR_MAX_DIM)));
    }

    PG_RETURN_INT32(*tl);
}

/*
 * Convert external binary representation to internal representation
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(vector_recv);
Datum vector_recv(PG_FUNCTION_ARGS)
{
    StringInfo buf = (StringInfo)PG_GETARG_POINTER(0);
    int32 typmod = PG_GETARG_INT32(2);
    Vector *result;
    int16 dim;
    int16 unused;

    dim = pq_getmsgint(buf, sizeof(int16));
    unused = pq_getmsgint(buf, sizeof(int16));

    CheckDim(dim);
    CheckExpectedDim(typmod, dim);

    if (unused != 0) {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("expected unused to be 0, not %d", unused)));
    }

    result = InitVector(dim);
    for (int i = 0; i < dim; i++) {
        result->x[i] = pq_getmsgfloat4(buf);
        CheckElement(result->x[i]);
    }

    PG_RETURN_POINTER(result);
}

/*
 * Convert internal representation to the external binary representation
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(vector_send);
Datum vector_send(PG_FUNCTION_ARGS)
{
    Vector *vec = PG_GETARG_VECTOR_P(0);
    StringInfoData buf;

    pq_begintypsend(&buf);
    pq_sendint(&buf, vec->dim, sizeof(int16));
    pq_sendint(&buf, vec->unused, sizeof(int16));
    for (int i = 0; i < vec->dim; i++)
        pq_sendfloat4(&buf, vec->x[i]);

    PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

/*
 * Convert vector to vector
 * This is needed to check the type modifier
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(vector);
Datum vector(PG_FUNCTION_ARGS)
{
    Vector *vec = PG_GETARG_VECTOR_P(0);
    int32 typmod = PG_GETARG_INT32(1);

    CheckExpectedDim(typmod, vec->dim);

    PG_RETURN_POINTER(vec);
}

/*
 * Convert array to vector
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(array_to_vector);
Datum array_to_vector(PG_FUNCTION_ARGS)
{
    ArrayType *array = PG_GETARG_ARRAYTYPE_P(0);
    int32 typmod = PG_GETARG_INT32(1);
    Vector *result;
    int16 typlen;
    bool typbyval;
    char typalign;
    Datum *elemsp;
    int nelemsp;

    if (ARR_NDIM(array) > 1) {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("array must be 1-D")));
    }

    if (ARR_HASNULL(array) && array_contains_nulls(array)) {
        ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), errmsg("array must not contain nulls")));
    }

    get_typlenbyvalalign(ARR_ELEMTYPE(array), &typlen, &typbyval, &typalign);
    deconstruct_array(array, ARR_ELEMTYPE(array), typlen, typbyval, typalign, &elemsp, NULL, &nelemsp);

    CheckDim(nelemsp);
    CheckExpectedDim(typmod, nelemsp);

    result = InitVector(nelemsp);

    if (ARR_ELEMTYPE(array) == INT4OID) {
        for (int i = 0; i < nelemsp; i++)
            result->x[i] = DatumGetInt32(elemsp[i]);
    } else if (ARR_ELEMTYPE(array) == FLOAT8OID) {
        for (int i = 0; i < nelemsp; i++)
            result->x[i] = DatumGetFloat8(elemsp[i]);
    } else if (ARR_ELEMTYPE(array) == FLOAT4OID) {
        for (int i = 0; i < nelemsp; i++)
            result->x[i] = DatumGetFloat4(elemsp[i]);
    } else if (ARR_ELEMTYPE(array) == NUMERICOID) {
        for (int i = 0; i < nelemsp; i++)
            result->x[i] = DatumGetFloat4(DirectFunctionCall1(numeric_float4, elemsp[i]));
    } else {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("unsupported array type")));
    }

    /*
     * Free allocation from deconstruct_array. Do not free individual elements
     * when pass-by-reference since they point to original array.
     */
    pfree(elemsp);

    /* Check elements */
    for (int i = 0; i < result->dim; i++) {
        CheckElement(result->x[i]);
    }

    PG_RETURN_POINTER(result);
}

/*
 * Convert vector to float4[]
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(vector_to_float4);
Datum vector_to_float4(PG_FUNCTION_ARGS)
{
    Vector *vec = PG_GETARG_VECTOR_P(0);
    Datum *datums;
    ArrayType *result;

    datums = (Datum *)palloc(sizeof(Datum) * vec->dim);

    for (int i = 0; i < vec->dim; i++) {
        datums[i] = Float4GetDatum(vec->x[i]);
    }

    /* Use TYPALIGN_INT for float4 */
    result = construct_array(datums, vec->dim, FLOAT4OID, sizeof(float4), true, TYPALIGN_INT);

    pfree(datums);

    PG_RETURN_POINTER(result);
}

/*
 * Convert vector to int4[]
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(vector_to_int4);
Datum vector_to_int4(PG_FUNCTION_ARGS)
{
    Vector *vec = PG_GETARG_VECTOR_P(0);
    Datum *datums;
    ArrayType *result;

    datums = (Datum *)palloc(sizeof(Datum) * vec->dim);

    for (int i = 0; i < vec->dim; i++) {
        datums[i] = DirectFunctionCall1(ftoi4, Float4GetDatum(vec->x[i]));
    }

    /* Use TYPALIGN_INT for int4 */
    result = construct_array(datums, vec->dim, INT4OID, sizeof(int4), true, TYPALIGN_INT);

    pfree(datums);

    PG_RETURN_POINTER(result);
}

/*
 * Convert vector to float8[]
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(vector_to_float8);
Datum vector_to_float8(PG_FUNCTION_ARGS)
{
    Vector *vec = PG_GETARG_VECTOR_P(0);
    Datum *datums;
    ArrayType *result;

    datums = (Datum *)palloc(sizeof(Datum) * vec->dim);

    for (int i = 0; i < vec->dim; i++) {
        datums[i] = Float8GetDatum(vec->x[i]);
    }

    /* Use TYPALIGN_DOUBLE for float8 */
    result = construct_array(datums, vec->dim, FLOAT8OID, sizeof(float8), FLOAT8PASSBYVAL, TYPALIGN_DOUBLE);

    pfree(datums);

    PG_RETURN_POINTER(result);
}

/*
 * Convert vector to numeric[]
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(vector_to_numeric);
Datum vector_to_numeric(PG_FUNCTION_ARGS)
{
    Vector *vec = PG_GETARG_VECTOR_P(0);
    Datum *datums;
    ArrayType *result;

    datums = (Datum *)palloc(sizeof(Datum) * vec->dim);

    for (int i = 0; i < vec->dim; i++) {
        Datum numericVal;
        Numeric typmod_numericVal;
        numericVal = DirectFunctionCall1(float4_numeric, Float4GetDatum(vec->x[i]));
        datums[i] = NumericGetDatum(numericVal);
    }

    /* Use TYPALIGN_INT for numeric */
    result = construct_array(datums, vec->dim, NUMERICOID, -1, false, TYPALIGN_INT);

    pfree(datums);

    PG_RETURN_POINTER(result);
}

/*
 * Convert half vector to vector
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(halfvec_to_vector);
Datum halfvec_to_vector(PG_FUNCTION_ARGS)
{
    HalfVector *vec = PG_GETARG_HALFVEC_P(0);
    int32 typmod = PG_GETARG_INT32(1);
    Vector *result;

    CheckDim(vec->dim);
    CheckExpectedDim(typmod, vec->dim);

    result = InitVector(vec->dim);

    for (int i = 0; i < vec->dim; i++) {
        result->x[i] = HalfToFloat4(vec->x[i]);
    }

    PG_RETURN_POINTER(result);
}

inline void prefetch_L1(const void *address)
{
#if defined(__SSE2__)
    _mm_prefetch((const char*)address, _MM_HINT_T0);
#elif defined(__aarch64__)
    asm volatile("prfm PLDL1KEEP, [%0]" : : "r"(address));
#else
    __builtin_prefetch(address, 0, 3); // L3 cache
#endif
}

#ifdef __aarch64__
static float L2SquaredDistanceRef(int dim, float *ax, float *bx)
{
    float distance = 0.0f;

    for (int i = 0; i < dim; i++) {
        float diff = ax[i] - bx[i];
        distance += diff * diff;
    }

    return distance;
}

VECTOR_TARGET_CLONES static float
VectorL2SquaredDistance(int dim, float *ax, float *bx)
{
    // 128 bit register = float 32*4
    float32x4_t r1 = vdupq_n_f32(0.0);
    float32x4_t r2 = vdupq_n_f32(0.0);
    float32x4_t r3 = vdupq_n_f32(0.0);
    float32x4_t r4 = vdupq_n_f32(0.0);
    int i = 0;
    float* pta = ax;
    float* ptb = bx;
    int batch1 = 16;
    int batch2 = 4;
    int rest = batch2 - 1;
    for (; i + batch1 <= dim; i += batch1, pta += batch1, ptb += batch1) {
        float32x4x4_t packdata_a = vld1q_f32_x4(pta);
        float32x4x4_t packdata_b = vld1q_f32_x4(ptb);

        float32x4_t diff0 = vsubq_f32(packdata_a.val[0], packdata_b.val[0]);
        float32x4_t diff1 = vsubq_f32(packdata_a.val[1], packdata_b.val[1]);
        float32x4_t diff2 = vsubq_f32(packdata_a.val[2], packdata_b.val[2]);
        float32x4_t diff3 = vsubq_f32(packdata_a.val[3], packdata_b.val[3]);

        r1 = vfmaq_f32(r1, diff0, diff0);
        r2 = vfmaq_f32(r2, diff1, diff1);
        r3 = vfmaq_f32(r3, diff2, diff2);
        r4 = vfmaq_f32(r4, diff3, diff3);
    }

    for (; i + batch2 <= dim; i += batch2, pta += batch2, ptb += batch2) {
        float32x4_t data_a = vld1q_f32(pta);
        float32x4_t data_b = vld1q_f32(ptb);
        float32x4_t diff = vsubq_f32(data_a, data_b);
        r1 = vfmaq_f32(r1, diff, diff);
    }

    r1 = vpaddq_f32(r1, r2);
    r2 = vpaddq_f32(r3, r4);
    r1 = vpaddq_f32(r1, r2);

    float distance = vaddvq_f32(r1);
    if (dim & rest) {
        distance += L2SquaredDistanceRef(dim - i, ax + i, bx + i);
    }
    return distance;
}
#elif defined(__x86_64__)
static inline __m128 masked_read(int d, const float *x)
{
    __attribute__((__aligned__(16))) float buf[4];

    Assert(0 <= d && d < 4); /* reads 0 <= d < 4 floats as __m128 */
    memset((void*)buf, 0, sizeof(buf));
    switch (d) {
        case 3:
            buf[2] = x[2];
        case 2:
            buf[1] = x[1];
        case 1:
            buf[0] = x[0];
        default:
            break;
    }
    return _mm_load_ps(buf);
}
VECTOR_TARGET_CLONES static float
VectorL2SquaredDistance(int dim, float *ax, float *bx)
{
    float* x = (float*)ax;
    float* y = (float*)bx;
    size_t d = (size_t)dim;
    int batch_num1 = 8;
    int batch_num2 = 4;
    __m256 msum1 = _mm256_setzero_ps();

    while (d >= batch_num1) {
        __m256 mx = _mm256_loadu_ps(x);
        x += batch_num1;
        __m256 my = _mm256_loadu_ps(y);
        y += batch_num1;
        const __m256 a_m_b1 = mx - my;
        msum1 += a_m_b1 * a_m_b1;
        d -= batch_num1;
    }

    __m128 msum2 = _mm256_extractf128_ps(msum1, 1);
    msum2 += _mm256_extractf128_ps(msum1, 0);

    if (d >= batch_num2) {
        __m128 mx = _mm_loadu_ps(x);
        x += batch_num2;
        __m128 my = _mm_loadu_ps(y);
        y += batch_num2;
        const __m128 a_m_b1 = mx - my;
        msum2 += a_m_b1 * a_m_b1;
        d -= batch_num2;
    }

    if (d > 0) {
        __m128 mx = masked_read(d, x);
        __m128 my = masked_read(d, y);
        __m128 a_m_b1 = mx - my;
        msum2 += a_m_b1 * a_m_b1;
    }

    msum2 = _mm_hadd_ps(msum2, msum2);
    msum2 = _mm_hadd_ps(msum2, msum2);

    return _mm_cvtss_f32(msum2);
}
#else

VECTOR_TARGET_CLONES static float VectorL2SquaredDistance(int dim, float *ax, float *bx)
{
    float distance = 0.0;

    /* Auto-vectorized */
    for (int i = 0; i < dim; i++) {
        float diff = ax[i] - bx[i];

        distance += diff * diff;
    }

    return distance;
}

#endif

/*
 * Get the L2 distance between vectors
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(l2_distance);
Datum l2_distance(PG_FUNCTION_ARGS)
{
    Vector *a = PG_GETARG_VECTOR_P(0);
    Vector *b = PG_GETARG_VECTOR_P(1);

    CheckDims(a, b);

    PG_RETURN_FLOAT8(sqrt((double)VectorL2SquaredDistance(a->dim, a->x, b->x)));
}

/*
 * Get the L2 squared distance between vectors
 * This saves a sqrt calculation
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(vector_l2_squared_distance);
Datum vector_l2_squared_distance(PG_FUNCTION_ARGS)
{
    Vector *a = PG_GETARG_VECTOR_P(0);
    Vector *b = PG_GETARG_VECTOR_P(1);

    CheckDims(a, b);

    PG_RETURN_FLOAT8((double)VectorL2SquaredDistance(a->dim, a->x, b->x));
}

#ifdef __aarch64__
VECTOR_TARGET_CLONES static float
VectorInnerProduct(int dim, float *ax, float *bx)
{
    float dis = 0.0f;
    float32x4_t sum = vdupq_n_f32(0.0f);
    float *pta = ax;
    float *ptb = bx;

    int i = 0;
    int prefetch_len = 8;
    int batch_num = 4;
    for (; i + batch_num <= dim; i += batch_num) {
        prefetch_L1(pta + prefetch_len);
        prefetch_L1(ptb + prefetch_len);
        float32x4_t sub_a = vld1q_f32(pta);
        float32x4_t sub_b = vld1q_f32(ptb);
        sum = vmlaq_f32(sum, sub_a, sub_b);
        pta += batch_num;
        ptb += batch_num;
    }

    dis = vaddvq_f32(sum);
    for (; i < dim; ++i) {
        dis += ax[i] * bx[i];
    }
    return dis;
}
#else

VECTOR_TARGET_CLONES static float VectorInnerProduct(int dim, float *ax, float *bx)
{
    float distance = 0.0;

    /* Auto-vectorized */
    for (int i = 0; i < dim; i++) {
        distance += ax[i] * bx[i];
    }

    return distance;
}

#endif

/*
 * Get the inner product of two vectors
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(inner_product);
Datum inner_product(PG_FUNCTION_ARGS)
{
    Vector *a = PG_GETARG_VECTOR_P(0);
    Vector *b = PG_GETARG_VECTOR_P(1);

    CheckDims(a, b);

    PG_RETURN_FLOAT8((double)VectorInnerProduct(a->dim, a->x, b->x));
}

/*
 * Get the negative inner product of two vectors
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(vector_negative_inner_product);
Datum vector_negative_inner_product(PG_FUNCTION_ARGS)
{
    Vector *a = PG_GETARG_VECTOR_P(0);
    Vector *b = PG_GETARG_VECTOR_P(1);

    CheckDims(a, b);

    PG_RETURN_FLOAT8((double)-VectorInnerProduct(a->dim, a->x, b->x));
}

VECTOR_TARGET_CLONES static double VectorCosineSimilarity(int dim, float *ax, float *bx)
{
    float similarity = 0.0;
    float norma = 0.0;
    float normb = 0.0;

    /* Auto-vectorized */
    for (int i = 0; i < dim; i++) {
        similarity += ax[i] * bx[i];
        norma += ax[i] * ax[i];
        normb += bx[i] * bx[i];
    }

    /* Use sqrt(a * b) over sqrt(a) * sqrt(b) */
    return static_cast<double>(similarity) / sqrt(static_cast<double>(norma) * static_cast<double>(normb));
}

/*
 * Get the cosine distance between two vectors
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(cosine_distance);
Datum cosine_distance(PG_FUNCTION_ARGS)
{
    Vector *a = PG_GETARG_VECTOR_P(0);
    Vector *b = PG_GETARG_VECTOR_P(1);
    double similarity;

    CheckDims(a, b);

    similarity = VectorCosineSimilarity(a->dim, a->x, b->x);
#ifdef _MSC_VER
    /* /fp:fast may not propagate NaN */
    if (isnan(similarity)) {
        PG_RETURN_FLOAT8(NAN);
    }
#endif

    /* Keep in range */
    if (similarity > 1) {
        similarity = 1.0;
    } else if (similarity < -1) {
        similarity = -1.0;
    }

    PG_RETURN_FLOAT8(1.0 - similarity);
}

/*
 * Get the distance for spherical k-means
 * Currently uses angular distance since needs to satisfy triangle inequality
 * Assumes inputs are unit vectors (skips norm)
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(vector_spherical_distance);
Datum vector_spherical_distance(PG_FUNCTION_ARGS)
{
    Vector *a = PG_GETARG_VECTOR_P(0);
    Vector *b = PG_GETARG_VECTOR_P(1);
    double distance;

    CheckDims(a, b);

    distance = (double)VectorInnerProduct(a->dim, a->x, b->x);
    /* Prevent NaN with acos with loss of precision */
    if (distance > 1) {
        distance = 1;
    } else if (distance < -1) {
        distance = -1;
    }

    PG_RETURN_FLOAT8(acos(distance) / M_PI);
}

/* Does not require FMA, but keep logic simple */
VECTOR_TARGET_CLONES static float VectorL1Distance(int dim, float *ax, float *bx)
{
    float distance = 0.0;

    /* Auto-vectorized */
    for (int i = 0; i < dim; i++) {
        distance += fabsf(ax[i] - bx[i]);
    }

    return distance;
}

/*
 * Get the L1 distance between two vectors
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(l1_distance);
Datum l1_distance(PG_FUNCTION_ARGS)
{
    Vector *a = PG_GETARG_VECTOR_P(0);
    Vector *b = PG_GETARG_VECTOR_P(1);

    CheckDims(a, b);

    PG_RETURN_FLOAT8((double)VectorL1Distance(a->dim, a->x, b->x));
}

/*
 * Get the dimensions of a vector
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(vector_dims);
Datum vector_dims(PG_FUNCTION_ARGS)
{
    Vector *a = PG_GETARG_VECTOR_P(0);

    PG_RETURN_INT32(a->dim);
}

/*
 * Get the L2 norm of a vector
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(vector_norm);
Datum vector_norm(PG_FUNCTION_ARGS)
{
    Vector *a = PG_GETARG_VECTOR_P(0);
    float *ax = a->x;
    double norm = 0.0;

    /* Auto-vectorized */
    for (int i = 0; i < a->dim; i++) {
        norm += (double)ax[i] * (double)ax[i];
    }

    PG_RETURN_FLOAT8(sqrt(norm));
}

/*
 * Normalize a vector with the L2 norm
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(l2_normalize);
Datum l2_normalize(PG_FUNCTION_ARGS)
{
    Vector *a = PG_GETARG_VECTOR_P(0);
    float *ax = a->x;
    double norm = 0;
    Vector *result;
    float *rx;

    result = InitVector(a->dim);
    rx = result->x;

    /* Auto-vectorized */
    for (int i = 0; i < a->dim; i++) {
        norm += (double)ax[i] * (double)ax[i];
    }

    norm = sqrt(norm);
    /* Return zero vector for zero norm */
    if (norm > 0) {
        for (int i = 0; i < a->dim; i++) {
            rx[i] = ax[i] / norm;
        }

        /* Check for overflow */
        for (int i = 0; i < a->dim; i++) {
            if (isinf(rx[i])) {
                float_overflow_error();
            }
        }
    }

    PG_RETURN_POINTER(result);
}

/*
 * Add vectors
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(vector_add);
Datum vector_add(PG_FUNCTION_ARGS)
{
    Vector *a = PG_GETARG_VECTOR_P(0);
    Vector *b = PG_GETARG_VECTOR_P(1);
    float *ax = a->x;
    float *bx = b->x;
    Vector *result;
    float *rx;

    CheckDims(a, b);

    result = InitVector(a->dim);
    rx = result->x;

    /* Auto-vectorized */
    for (int i = 0, imax = a->dim; i < imax; i++) {
        rx[i] = ax[i] + bx[i];
    }

    /* Check for overflow */
    for (int i = 0, imax = a->dim; i < imax; i++) {
        if (isinf(rx[i])) {
            float_overflow_error();
        }
    }

    PG_RETURN_POINTER(result);
}

/*
 * Subtract vectors
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(vector_sub);
Datum vector_sub(PG_FUNCTION_ARGS)
{
    Vector *a = PG_GETARG_VECTOR_P(0);
    Vector *b = PG_GETARG_VECTOR_P(1);
    float *ax = a->x;
    float *bx = b->x;
    Vector *result;
    float *rx;

    CheckDims(a, b);

    result = InitVector(a->dim);
    rx = result->x;

    /* Auto-vectorized */
    for (int i = 0, imax = a->dim; i < imax; i++) {
        rx[i] = ax[i] - bx[i];
    }

    /* Check for overflow */
    for (int i = 0, imax = a->dim; i < imax; i++) {
        if (isinf(rx[i])) {
            float_overflow_error();
        }
    }

    PG_RETURN_POINTER(result);
}

/*
 * Multiply vectors
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(vector_mul);
Datum vector_mul(PG_FUNCTION_ARGS)
{
    Vector *a = PG_GETARG_VECTOR_P(0);
    Vector *b = PG_GETARG_VECTOR_P(1);
    float *ax = a->x;
    float *bx = b->x;
    Vector *result;
    float *rx;

    CheckDims(a, b);

    result = InitVector(a->dim);
    rx = result->x;

    /* Auto-vectorized */
    for (int i = 0, imax = a->dim; i < imax; i++) {
        rx[i] = ax[i] * bx[i];
    }

    /* Check for overflow and underflow */
    for (int i = 0, imax = a->dim; i < imax; i++) {
        if (isinf(rx[i])) {
            float_overflow_error();
        }

        if (rx[i] == 0 && !(ax[i] == 0 || bx[i] == 0)) {
            float_underflow_error();
        }
    }

    PG_RETURN_POINTER(result);
}

/*
 * Concatenate vectors
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(vector_concat);
Datum vector_concat(PG_FUNCTION_ARGS)
{
    Vector *a = PG_GETARG_VECTOR_P(0);
    Vector *b = PG_GETARG_VECTOR_P(1);
    Vector *result;
    int dim = a->dim + b->dim;

    CheckDim(dim);
    result = InitVector(dim);

    for (int i = 0; i < a->dim; i++) {
        result->x[i] = a->x[i];
    }

    for (int i = 0; i < b->dim; i++) {
        result->x[i + a->dim] = b->x[i];
    }

    PG_RETURN_POINTER(result);
}

/*
 * Quantize a vector
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(binary_quantize);
Datum binary_quantize(PG_FUNCTION_ARGS)
{
    Vector *a = PG_GETARG_VECTOR_P(0);
    float *ax = a->x;
    VarBit *result = InitBitVector(a->dim);
    unsigned char *rx = VARBITS(result);

    for (int i = 0; i < a->dim; i++) {
        rx[i / 8] |= (ax[i] > 0) << (7 - (i % 8));
    }

    PG_RETURN_VARBIT_P(result);
}

/*
 * Get a subvector
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(subvector);
Datum subvector(PG_FUNCTION_ARGS)
{
    Vector *a = PG_GETARG_VECTOR_P(0);
    int32 start = PG_GETARG_INT32(1);
    int32 count = PG_GETARG_INT32(2);
    int32 end;
    float *ax = a->x;
    Vector *result;
    int dim;

    if (count < 1) {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("vector must have at least 1 dimension")));
    }

    /* Indexing starts at 1, like substring */
    if (start < 1) {
        ereport(WARNING, (errmsg("when the start position is less than 1, it will begin with the first dimension")));
        start = 1;
    } else if (start > a->dim) {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("vector must have at least 1 dimension")));
    }

    /*
     * Check if (start + count > a->dim), avoiding integer overflow. a->dim
     * and count are both positive, so a->dim - count won't overflow.
     */
    if (start > a->dim - count) {
        end = a->dim + 1;
    } else {
        end = start + count;
    }

    dim = end - start;
    CheckDim(dim);
    result = InitVector(dim);

    for (int i = 0; i < dim; i++) {
        result->x[i] = ax[start - 1 + i];
    }

    PG_RETURN_POINTER(result);
}

/*
 * Internal helper to compare vectors
 */
int vector_cmp_internal(Vector *a, Vector *b)
{
    int dim = Min(a->dim, b->dim);

    /* Check values before dimensions to be consistent with Postgres arrays */
    for (int i = 0; i < dim; i++) {
        if (a->x[i] < b->x[i]) {
            return -1;
        }

        if (a->x[i] > b->x[i]) {
            return 1;
        }
    }

    if (a->dim < b->dim) {
        return -1;
    }

    if (a->dim > b->dim) {
        return 1;
    }

    return 0;
}

/*
 * Less than
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(vector_lt);
Datum vector_lt(PG_FUNCTION_ARGS)
{
    Vector *a = PG_GETARG_VECTOR_P(0);
    Vector *b = PG_GETARG_VECTOR_P(1);

    PG_RETURN_BOOL(vector_cmp_internal(a, b) < 0);
}

/*
 * Less than or equal
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(vector_le);
Datum vector_le(PG_FUNCTION_ARGS)
{
    Vector *a = PG_GETARG_VECTOR_P(0);
    Vector *b = PG_GETARG_VECTOR_P(1);

    PG_RETURN_BOOL(vector_cmp_internal(a, b) <= 0);
}

/*
 * Equal
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(vector_eq);
Datum vector_eq(PG_FUNCTION_ARGS)
{
    Vector *a = PG_GETARG_VECTOR_P(0);
    Vector *b = PG_GETARG_VECTOR_P(1);

    PG_RETURN_BOOL(vector_cmp_internal(a, b) == 0);
}

/*
 * Not equal
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(vector_ne);
Datum vector_ne(PG_FUNCTION_ARGS)
{
    Vector *a = PG_GETARG_VECTOR_P(0);
    Vector *b = PG_GETARG_VECTOR_P(1);

    PG_RETURN_BOOL(vector_cmp_internal(a, b) != 0);
}

/*
 * Greater than or equal
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(vector_ge);
Datum vector_ge(PG_FUNCTION_ARGS)
{
    Vector *a = PG_GETARG_VECTOR_P(0);
    Vector *b = PG_GETARG_VECTOR_P(1);

    PG_RETURN_BOOL(vector_cmp_internal(a, b) >= 0);
}

/*
 * Greater than
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(vector_gt);
Datum vector_gt(PG_FUNCTION_ARGS)
{
    Vector *a = PG_GETARG_VECTOR_P(0);
    Vector *b = PG_GETARG_VECTOR_P(1);

    PG_RETURN_BOOL(vector_cmp_internal(a, b) > 0);
}

/*
 * Compare vectors
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(vector_cmp);
Datum vector_cmp(PG_FUNCTION_ARGS)
{
    Vector *a = PG_GETARG_VECTOR_P(0);
    Vector *b = PG_GETARG_VECTOR_P(1);

    PG_RETURN_INT32(vector_cmp_internal(a, b));
}

/*
 * Accumulate vectors
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(vector_accum);
Datum vector_accum(PG_FUNCTION_ARGS)
{
    ArrayType *statearray = PG_GETARG_ARRAYTYPE_P(0);
    Vector *newval = PG_GETARG_VECTOR_P(1);
    float8 *statevalues;
    int16 dim;
    bool newarr;
    float8 n;
    Datum *statedatums;
    float *x = newval->x;
    ArrayType *result;

    /* Check array before using */
    statevalues = CheckStateArray(statearray, "vector_accum");
    dim = STATE_DIMS(statearray);
    newarr = dim == 0;

    if (newarr) {
        dim = newval->dim;
    } else {
        CheckExpectedDim(dim, newval->dim);
    }

    n = statevalues[0] + 1.0;

    statedatums = (Datum *)CreateStateDatums(dim);
    statedatums[0] = Float8GetDatum(n);

    if (newarr) {
        for (int i = 0; i < dim; i++) {
            statedatums[i + 1] = Float8GetDatum((double)x[i]);
        }
    } else {
        for (int i = 0; i < dim; i++) {
            double v = statevalues[i + 1] + x[i];

            /* Check for overflow */
            if (isinf(v)) {
                float_overflow_error();
            }

            statedatums[i + 1] = Float8GetDatum(v);
        }
    }

    /* Use float8 array like float4_accum */
    result = construct_array(statedatums, dim + 1, FLOAT8OID, sizeof(float8), FLOAT8PASSBYVAL, TYPALIGN_DOUBLE);

    pfree(statedatums);

    PG_RETURN_ARRAYTYPE_P(result);
}

/*
 * Combine vectors or half vectors (also used for halfvec_combine)
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(vector_combine);
Datum vector_combine(PG_FUNCTION_ARGS)
{
    /* Must also update parameters of halfvec_combine if modifying */
    ArrayType *statearray1 = PG_GETARG_ARRAYTYPE_P(0);
    ArrayType *statearray2 = PG_GETARG_ARRAYTYPE_P(1);
    float8 *statevalues1;
    float8 *statevalues2;
    float8 n;
    float8 n1;
    float8 n2;
    int16 dim;
    Datum *statedatums;
    ArrayType *result;

    /* Check arrays before using */
    statevalues1 = CheckStateArray(statearray1, "vector_combine");
    statevalues2 = CheckStateArray(statearray2, "vector_combine");

    n1 = statevalues1[0];
    n2 = statevalues2[0];

    if (n1 == 0.0) {
        n = n2;
        dim = STATE_DIMS(statearray2);
        statedatums = (Datum *)CreateStateDatums(dim);
        for (int i = 1; i <= dim; i++)
            statedatums[i] = Float8GetDatum(statevalues2[i]);
    } else if (n2 == 0.0) {
        n = n1;
        dim = STATE_DIMS(statearray1);
        statedatums = (Datum *)CreateStateDatums(dim);
        for (int i = 1; i <= dim; i++)
            statedatums[i] = Float8GetDatum(statevalues1[i]);
    } else {
        n = n1 + n2;
        dim = STATE_DIMS(statearray1);
        CheckExpectedDim(dim, STATE_DIMS(statearray2));
        statedatums = (Datum *)CreateStateDatums(dim);
        for (int i = 1; i <= dim; i++) {
            double v = statevalues1[i] + statevalues2[i];

            /* Check for overflow */
            if (isinf(v))
                float_overflow_error();

            statedatums[i] = Float8GetDatum(v);
        }
    }

    statedatums[0] = Float8GetDatum(n);

    result = construct_array(statedatums, dim + 1, FLOAT8OID, sizeof(float8), FLOAT8PASSBYVAL, TYPALIGN_DOUBLE);

    pfree(statedatums);

    PG_RETURN_ARRAYTYPE_P(result);
}

/*
 * Average vectors
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(vector_avg);
Datum vector_avg(PG_FUNCTION_ARGS)
{
    ArrayType *statearray = PG_GETARG_ARRAYTYPE_P(0);
    float8 *statevalues;
    float8 n;
    uint16 dim;
    Vector *result;

    /* Check array before using */
    statevalues = CheckStateArray(statearray, "vector_avg");
    n = statevalues[0];

    /* SQL defines AVG of no values to be NULL */
    if (n == 0.0) {
        PG_RETURN_NULL();
    }

    /* Create vector */
    dim = STATE_DIMS(statearray);
    CheckDim(dim);
    result = InitVector(dim);
    for (int i = 0; i < dim; i++) {
        result->x[i] = statevalues[i + 1] / n;
        CheckElement(result->x[i]);
    }

    PG_RETURN_POINTER(result);
}

/*
 * Convert sparse vector to dense vector
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(sparsevec_to_vector);
Datum sparsevec_to_vector(PG_FUNCTION_ARGS)
{
    SparseVector *svec = PG_GETARG_SPARSEVEC_P(0);
    int32 typmod = PG_GETARG_INT32(1);
    Vector *result;
    int dim = svec->dim;
    float *values = SPARSEVEC_VALUES(svec);

    CheckDim(dim);
    CheckExpectedDim(typmod, dim);

    result = InitVector(dim);
    for (int i = 0; i < svec->nnz; i++) {
        result->x[svec->indices[i]] = values[i];
    }

    PG_RETURN_POINTER(result);
}

/*
 * WAL-log a range of blocks in a relation.
 *
 * An image of all pages with block numbers 'startblk' <= X < 'endblk' is
 * written to the WAL. If the range is large, this is done in multiple WAL
 * records.
 *
 * If all page follows the standard page layout, with a PageHeader and unused
 * space between pd_lower and pd_upper, set 'page_std' to true. That allows
 * the unused space to be left out from the WAL records, making them smaller.
 *
 * NOTE: This function acquires exclusive-locks on the pages. Typically, this
 * is used on a newly-built relation, and the caller is holding a
 * AccessExclusiveLock on it, so no other backend can be accessing it at the
 * same time. If that's not the case, you must ensure that this does not
 * cause a deadlock through some other means.
 */
void LogNewpageRange(Relation rel, ForkNumber forknum, BlockNumber startblk, BlockNumber endblk, bool page_std)
{
    int flags;
    BlockNumber blkno;

    flags = REGBUF_FORCE_IMAGE;
    if (page_std) {
        flags |= REGBUF_STANDARD;
    }

    /*
     * Iterate over all the pages in the range. They are collected into
     * batches of XLR_MAX_BLOCK_ID pages, and a single WAL-record is written
     * for each batch.
     */
    XLogEnsureRecordSpace(XLR_MAX_BLOCK_ID - 1, 0);

    blkno = startblk;
    while (blkno < endblk) {
        Buffer bufpack[XLR_MAX_BLOCK_ID];
        XLogRecPtr recptr;
        int nbufs;
        int i;

        CHECK_FOR_INTERRUPTS();

        /* Collect a batch of blocks. */
        nbufs = 0;
        while (nbufs < XLR_MAX_BLOCK_ID && blkno < endblk) {
            Buffer buf = ReadBufferExtended(rel, forknum, blkno, RBM_NORMAL, NULL);

            LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

            /*
             * Completely empty pages are not WAL-logged. Writing a WAL record
             * would change the LSN, and we don't want that. We want the page
             * to stay empty.
             */
            if (!PageIsNew(BufferGetPage(buf))) {
                bufpack[nbufs++] = buf;
            } else {
                UnlockReleaseBuffer(buf);
            }
            blkno++;
        }

        /* Nothing more to do if all remaining blocks were empty. */
        if (nbufs == 0) {
            break;
        }

        /* Write WAL record for this batch. */
        XLogBeginInsert();

        START_CRIT_SECTION();
        for (i = 0; i < nbufs; i++) {
            MarkBufferDirty(bufpack[i]);
            XLogRegisterBuffer(i, bufpack[i], flags);
        }

        recptr = XLogInsert(RM_XLOG_ID, XLOG_FPI);

        for (i = 0; i < nbufs; i++) {
            PageSetLSN(BufferGetPage(bufpack[i]), recptr);
            UnlockReleaseBuffer(bufpack[i]);
        }
        END_CRIT_SECTION();
    }
}

int PlanCreateIndexWorkers(Relation heapRelation, IndexInfo *indexInfo)
{
    int parallelWorkers = RelationGetParallelWorkers(heapRelation, 0);
    int maxHashbucketIndexWorker = 32;

    if (parallelWorkers != 0) {
        parallelWorkers = Min(maxHashbucketIndexWorker, parallelWorkers);
    }

    if (indexInfo->ii_Concurrent && indexInfo->ii_ParallelWorkers > 0) {
        ereport(NOTICE, (errmsg("switch off parallel mode when concurrently flag is set")));
        parallelWorkers = 0;
    }

    if (heapRelation->rd_rel->relpersistence == RELPERSISTENCE_GLOBAL_TEMP && indexInfo->ii_ParallelWorkers > 0) {
        ereport(NOTICE, (errmsg("switch off parallel mode for global temp table")));
        parallelWorkers = 0;
    }

    /* disable parallel building index for system table */
    if (IsCatalogRelation(heapRelation)) {
        parallelWorkers = 0;
    }
    return parallelWorkers;
}
