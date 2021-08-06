/* *
Copyright (c) 2021 Huawei Technologies Co.,Ltd.

openGauss is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:

  http://license.coscl.org.cn/MulanPSL2

THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details.
---------------------------------------------------------------------------------------

distance_functions.cpp
       Current set of distance functions that can be used (for k-means for example)

IDENTIFICATION
    src/gausskernel/dbmind/db4ai/executor/distance_functions.cpp

---------------------------------------------------------------------------------------
* */

#include "postgres.h"

#include "db4ai/distance_functions.h"
#include "db4ai/fp_ops.h"
#include "db4ai/db4ai_cpu.h"

#include <cmath>

#if defined(__x86_64__) && defined(__SSE3__)
#include <pmmintrin.h>
#elif defined(__aarch64__) && defined(__ARM_NEON)
#include <arm_neon.h>
#endif


/*
 * L1 distance (Manhattan)
 * We sum using cascaded summation
 * This version is unvectorized and is used in case vectorized instructions
 * are not available or for the the case that the dimension is not a multiple
 * of the width of the registers
 */
static force_inline double l1_non_vectorized(double const * p, double const * q, uint32_t const dimension)
{
    double term = 0.;
    double term_correction = 0.;
    double distance = 0.;
    double distance_correction = 0.;

    twoDiff(q[0], p[0], &term, &term_correction);
    term += term_correction;
    // absolute value of the difference (hopefully done by clearing the sign bit)
    distance = std::abs(term);

    for (uint32_t d = 1; d < dimension; ++d) {
        twoDiff(q[d], p[d], &term, &term_correction);
        term += term_correction;
        term = std::abs(term);
        twoSum(distance, term, &distance, &term_correction);
        distance_correction += term_correction;
    }

    return distance + distance_correction;
}

#if (defined(__x86_64__) && defined(__SSE3__)) || (defined(__aarch64__) && defined(__ARM_NEON))
/*
 * L1 distance (Manhattan)
 * This version is vectorized using SSE or NEON and is used in case only 128-bit
 * vectorized instructions are available
 */
static double l1_128(double const * p, double const * q, uint32_t const dimension)
{
    if (unlikely(dimension == 0))
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("L1 distance (128-bit): dimension must be larger than 0")));

    double distance[2] = {0.};
    /* the result of dimension modulo 2 */
    uint32_t const dimension_remainder = dimension & 0x1U;
    uint32_t const offset = 2U;
    double distance_first_terms = 0.;
    double local_distance_correction = 0.;
    double global_distance_correction = 0.;

    /*
     * this will compute the very first terms of the distance
     * (the ones that cannot be fully computed using simd registers, and
     * thus have to be done with scalar computations)
     */
    if (dimension_remainder > 0) {
        /*
         * this is gonna compute the 1-dimensional distance (the very first
         * term in the whole distance computation)
         */
        distance_first_terms = l1_non_vectorized(p, q, dimension_remainder);
    }

    /*
     * if the dimension is < 2 then the term above is the whole distance
     * otherwise we have at least one simd register we can fill
     */
    if (unlikely(dimension < offset))
        return distance_first_terms;

#if defined(__x86_64__)
    __m128d const zero = _mm_setzero_pd();
    __m128d const sign_mask = _mm_set1_pd(-0.0f);
    __m128d sum = zero;
    __m128d absolute_value;
    __m128d sub;
    __m128d p128, q128;
#else // aarch64
    float64x2_t const zero = vdupq_n_f64(0);
    float64x2_t sum = zero;
    float64x2_t absolute_value;
    float64x2_t sub;
    float64x2_t p128, q128;
#endif

    Assert(((dimension - dimension_remainder) & 0x1U) == 0U);

    for (uint32_t d = dimension_remainder; d < dimension; d += offset) {
#if defined(__x86_64__)
        p128 = _mm_loadu_pd(p + d);
        q128 = _mm_loadu_pd(q + d);
        sub = _mm_sub_pd(p128, q128);
        /* this clears out the sign bit of sub (thus computing its absolute value) */
        absolute_value = _mm_andnot_pd(sign_mask, sub);
        sum = _mm_add_pd(sum, absolute_value);
#else // aarch64
        p128 = vld1q_f64(p + d);
        q128 = vld1q_f64(q + d);
        sub = vsubq_f64(p128, q128);
        /* this clears out the sign bit of sub - hopefully (thus computing its absolute value */
        absolute_value = vabsq_f64(sub);
        sum = vaddq_f64(sum, absolute_value);
#endif
    }
    /*
     * in here we end up having a register with two terms that need to be added up to produce
     * the final answer, we first perform an horizontal add to reduce two to one term
     */
#if defined(__x86_64__)
    sum = _mm_hadd_pd(sum, zero);
#else // aarch64
    sum = vpaddq_f64(sum, zero);
#endif

    /*
     * we extract the remaining term [x,0] to produce the final solution
     */
#if defined(__x86_64__)
    _mm_storeu_pd(distance, sum);
#else // aarch64
    vst1q_f64(distance, sum);
#endif

    if (dimension_remainder > 0) {
        /* d[0] = d[0] + distance_first_terms */
        twoSum(distance[0], distance_first_terms, distance, &local_distance_correction);
        global_distance_correction += local_distance_correction;
    }

    return distance[0] + global_distance_correction;
}

#endif


/*
 * Squared Euclidean (default)
 * We sum using cascaded summation
 * This version is unvectorized and is used in case vectorized instructions
 * are not available or for the the case that the dimension is not a multiple
 * of the width of the registers
 */
static force_inline double l2_squared_non_vectorized(double const * p, double const * q, uint32_t const dimension)
{
    double subtraction = 0.;
    double subtraction_correction = 0.;
    double term = 0.;
    double term_correction = 0.;
    double distance = 0.;
    double distance_correction = 0.;

    twoDiff(q[0], p[0], &subtraction, &subtraction_correction);
    subtraction += subtraction_correction;
    square(subtraction, &term, &term_correction);
    term += term_correction;
    distance = term;

    for (uint32_t d = 1; d < dimension; ++d) {
        twoDiff(q[d], p[d], &subtraction, &subtraction_correction);
        subtraction += subtraction_correction;
        square(subtraction, &term, &term_correction);
        term += term_correction;
        twoSum(distance, term, &distance, &term_correction);
        distance_correction += term_correction;
    }

    return distance + distance_correction;
}

#if (defined(__x86_64__) && defined(__SSE3__)) || (defined(__aarch64__) && defined(__ARM_NEON))
/*
 * Squared Euclidean (default)
 * This version is vectorized using SSE or NEON and is used in case only 128-bit
 * vectorized instructions are available
 */
static double l2_squared_128(double const * p, double const * q, uint32_t const dimension)
{
    if (unlikely(dimension == 0))
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("L2 squared distance (128-bit): dimension must be larger than 0")));

    double distance[2] = {0.};
    /* the result of dimension modulo 2 */
    uint32_t const dimension_remainder = dimension & 0x1U;
    uint32_t const offset = 2U;
    double distance_first_terms = 0.;
    double local_distance_correction = 0.;
    double global_distance_correction = 0.;

    /*
     * this will compute the very first terms of the distance
     * (the ones that cannot be fully computed using simd registers, and
     * thus have to be done with scalar computations)
     */
    if (dimension_remainder > 0) {
        /*
         * this is gonna compute the 1-dimensional distance (the very first
         * term in the whole distance computation)
         */
        distance_first_terms = l2_squared_non_vectorized(p, q, dimension_remainder);
    }

    /*
     * if the dimension is < 2 then the term above is the whole distance
     * otherwise we have at least one simd register we can fill
     */
    if (unlikely(dimension < offset))
        return distance_first_terms;

#if defined(__x86_64__)
    __m128d const zero = _mm_setzero_pd();
    __m128d sum = zero;
    __m128d square;
    __m128d sub;
    __m128d p128, q128;
#else // aarch64
    float64x2_t const zero = vdupq_n_f64(0);
    float64x2_t sum = zero;
    float64x2_t square;
    float64x2_t sub;
    float64x2_t p128, q128;
#endif

    Assert(((dimension - dimension_remainder) & 0x1U) == 0U);

    for (uint32_t d = dimension_remainder; d < dimension; d += offset) {
#if defined(__x86_64__)
        p128 = _mm_loadu_pd(p + d);
        q128 = _mm_loadu_pd(q + d);
        sub = _mm_sub_pd(p128, q128);
        square = _mm_mul_pd(sub, sub);
        sum = _mm_add_pd(sum, square);
#else // aarch64
        p128 = vld1q_f64(p + d);
        q128 = vld1q_f64(q + d);
        sub = vsubq_f64(p128, q128);
        square = vmulq_f64(sub, sub);
        sum = vaddq_f64(sum, square);
#endif
    }
    /*
     * in here we end up having a register with two terms that need to be added up to produce
     * the final answer, we first perform an horizontal add to reduce two to one term
     */
#if defined(__x86_64__)
    sum = _mm_hadd_pd(sum, zero);
#else // aarch64
    sum = vpaddq_f64(sum, zero);
#endif
    
    /*
     * we extract the remaining term [x,0] to produce the final solution
     */
#if defined(__x86_64__)
    _mm_storeu_pd(distance, sum);
#else // aarch64
    vst1q_f64(distance, sum);
#endif
    
    if (dimension_remainder > 0) {
        /* d[0] = d[0] + distance_first_terms */
        twoSum(distance[0], distance_first_terms, distance, &local_distance_correction);
        global_distance_correction += local_distance_correction;
    }

    return distance[0] + global_distance_correction;
}

#endif

/*
 * L infinity distance (Chebyshev)
 * This version is unvectorized and is used in case vectorized instructions
 * are not available or for the the case that the dimension is not a multiple
 * of the width of the registers
 */
static force_inline double linf_non_vectorized(double const * p, double const * q, uint32_t const dimension)
{
    double distance = 0.;

    double term = 0.;
    double term_correction = 0.;

    twoDiff(q[0], p[0], &term, &term_correction);
    term += term_correction;
    // absolute value of the difference (hopefully done by clearing the sign bit)
    distance = std::abs(term);

    for (uint32_t d = 1; d < dimension; ++d) {
        twoDiff(q[d], p[d], &term, &term_correction);
        term += term_correction;
        term = std::abs(term);
        distance = term > distance ? term : distance;
    }

    return distance;
}

#if (defined(__x86_64__) && defined(__SSE3__)) || (defined(__aarch64__) && defined(__ARM_NEON))
/*
 * L infinity distance (Chebyshev)
 * This version is vectorized using SSE or NEON and is used in case only 128-bit
 * vectorized instructions are available
 */
static double linf_128(double const * p, double const * q, uint32_t const dimension)
{
    if (unlikely(dimension == 0))
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("L infinity distance (128-bit): dimension must be larger than 0")));

    double distance[2] = {0.};
    /* the result of dimension modulo 2 */
    uint32_t const dimension_remainder = dimension & 0x1U;
    uint32_t const offset = 2U;
    double distance_first_terms = 0.;

    /*
     * this will compute the very first terms of the distance
     * (the ones that cannot be fully computed using simd registers)
     */
    if (dimension_remainder > 0)
        distance_first_terms = linf_non_vectorized(p, q, dimension_remainder);

    /*
     * if the dimension is < 4 then the term above is the whole distance
     * otherwise we have at least one simd register we can fill
     */
    if (unlikely(dimension < offset))
        return distance_first_terms;

#if defined(__x86_64__)
    __m128d const zero = _mm_setzero_pd();
    __m128d const sign_mask = _mm_set1_pd(-0.0f);
    __m128d max = zero;
    __m128d absolute_value;
    __m128d sub;
    __m128d p128, q128;
#else // aarch64
    float64x2_t const zero = vdupq_n_f64(0);
    float64x2_t max = zero;
    float64x2_t absolute_value;
    float64x2_t sub;
    float64x2_t p128, q128;
#endif

    Assert(((dimension - dimension_remainder) & 0x1U) == 0U);

    for (uint32_t d = dimension_remainder; d < dimension; d += offset) {
#if defined(__x86_64__)
        p128 = _mm_loadu_pd(p + d);
        q128 = _mm_loadu_pd(q + d);
        sub = _mm_sub_pd(p128, q128);
        /* this clears out the sign bit of sub (thus computing its absolute value */
        absolute_value = _mm_andnot_pd(sign_mask, sub);
        max = _mm_max_pd(max, absolute_value);
#else // aarch64
        p128 = vld1q_f64(p + d);
        q128 = vld1q_f64(q + d);
        sub = vsubq_f64(p128, q128);
        /* this clears out the sign bit of sub - hopefully (thus computing its absolute value */
        absolute_value = vabsq_f64(sub);
        max = vmaxq_f64(max, absolute_value);
#endif
    }
    /*
     * in here we end up having a register with two terms, from which we extract the max
     * to produce the final answer
     */
#if defined(__x86_64__)
    _mm_storeu_pd(distance, max);
#else // aarch64
    vst1q_f64(distance, max);
#endif
    
    double result = distance_first_terms;
    for (uint32_t m = 0; m < offset; ++m)
        result = distance[m] > result ? distance[m] : result;
    
    return result;
}

#endif

/*
 * L1 distance (Manhattan)
 * This is the main function. It will be automatically vectorized
 * if possible
 */
double l1(double const * p, double const * q, uint32_t const dimension)
{
    if (unlikely(dimension == 0))
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("L1 distance: dimension must be larger than 0")));
/*
 * depending on the feature of the underlying processor we vectorized one way
 * or another. in the worst case we do not vectorized at all
 */
#if (defined(__x86_64__) && defined(__SSE3__)) || (defined(__aarch64__) && defined(__ARM_NEON))
    return l1_128(p, q, dimension);
#else
    return l1_non_vectorized(p, q, dimension);
#endif
}

/*
 * Squared Euclidean (default)
 * This is the main function. It will be automatically vectorized
 * if possible
 */
double l2_squared(double const * p, double const * q, uint32_t const dimension)
{
    if (unlikely(dimension == 0))
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("L2 squared distance: dimension must be larger than 0")));
/*
 * depending on the feature of the underlying processor we vectorized one way
 * or another. in the worst case we do not vectorized at all
 */
#if (defined(__x86_64__) && defined(__SSE3__)) || (defined(__aarch64__) && defined(__ARM_NEON))
    return l2_squared_128(p, q, dimension);
#else
    return l2_squared_non_vectorized(p, q, dimension);
#endif
}

/*
 * L2 distance (Euclidean)
 * This is the main function. It will be automatically vectorized
 * if possible
 */
double l2(double const * p, double const * q, uint32_t const dimension)
{
    if (unlikely(dimension == 0))
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("L2 distance: dimension must be larger than 0")));

    /*
     * this one is vectorized automatically (or not)
     */
    double const l2_sq = l2_squared(p, q, dimension);

    // we can replace this with exact computation via mpfr (more costly, but the best alternative
    // for fixed precision)
    return std::sqrt(l2_sq);
}

/*
 * L infinity distance (Chebyshev)
 * This is the main function. It will be automatically vectorized
 * if possible
 */
double linf(double const * p, double const * q, uint32_t const dimension)
{
    if (unlikely(dimension == 0))
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("L infinity distance: dimension must be larger than 0")));
/*
 * depending on the feature of the underlying processor we vectorized one way
 * or another. in the worst case we do not vectorized at all
 */
#if (defined(__x86_64__) && defined(__SSE3__)) || (defined(__aarch64__) && defined(__ARM_NEON))
    return linf_128(p, q, dimension);
#else
    return linf_non_vectorized(p, q, dimension);
#endif
}
