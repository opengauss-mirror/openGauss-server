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
 * halfvec.h
 *
 * IDENTIFICATION
 *        src/include/access/datavec/halfvec.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef HALFVEC_H
#define HALFVEC_H

#define __STDC_WANT_IEC_60559_TYPES_EXT__

#include <float.h>
#include "fmgr.h"

/* We use two types of dispatching: intrinsics and target_clones */
/* TODO Move to better place */
#ifndef DISABLE_DISPATCH
/* Only enable for more recent compilers to keep build process simple */
#if defined(__x86_64__) && defined(__GNUC__) && __GNUC__ >= 11
#define USE_DISPATCH
#elif defined(__x86_64__) && defined(__clang_major__) && __clang_major__ >= 7
#define USE_DISPATCH
#elif defined(_M_AMD64) && defined(_MSC_VER) && _MSC_VER >= 1920
#define USE_DISPATCH
#endif
#endif

/* target_clones requires glibc */
#if defined(USE_DISPATCH) && defined(__gnu_linux__) && defined(__has_attribute)
/* Use separate line for portability */
#if __has_attribute(target_clones)
#define USE_TARGET_CLONES
#endif
#endif

/* Apple clang check needed for universal binaries on Mac */
#if defined(USE_DISPATCH) && (defined(HAVE__GET_CPUID) || defined(__apple_build_version__))
#define USE__GET_CPUID
#endif

#if defined(USE_DISPATCH)
#define HALFVEC_DISPATCH
#endif

/* F16C has better performance than _Float16 (on x86-64) */
#if defined(__F16C__)
#define F16C_SUPPORT
#endif

#define half uint16
#define HALF_MAX 65504

#define HALFVEC_MAX_DIM 16000

#define HALFVEC_SIZE(_dim) (offsetof(HalfVector, x) + sizeof(half) * (_dim))
#define DatumGetHalfVector(x) ((HalfVector *)PG_DETOAST_DATUM(x))
#define PG_GETARG_HALFVEC_P(x) DatumGetHalfVector(PG_GETARG_DATUM(x))
#define PG_RETURN_HALFVEC_P(x) PG_RETURN_POINTER(x)

typedef struct HalfVector {
    int32 vl_len_; /* varlena header (do not touch directly!) */
    int16 dim;     /* number of dimensions */
    int16 unused;  /* reserved for future use, always zero */
    half x[FLEXIBLE_ARRAY_MEMBER];
} HalfVector;

HalfVector *InitHalfVector(int dim);

Datum halfvec_in(PG_FUNCTION_ARGS);
Datum halfvec_out(PG_FUNCTION_ARGS);
Datum halfvec_typmod_in(PG_FUNCTION_ARGS);
Datum halfvec_recv(PG_FUNCTION_ARGS);
Datum halfvec_send(PG_FUNCTION_ARGS);
Datum halfvec_l2_distance(PG_FUNCTION_ARGS);
Datum halfvec_inner_product(PG_FUNCTION_ARGS);
Datum halfvec_cosine_distance(PG_FUNCTION_ARGS);
Datum halfvec_l1_distance(PG_FUNCTION_ARGS);
Datum halfvec_vector_dims(PG_FUNCTION_ARGS);
Datum halfvec_l2_norm(PG_FUNCTION_ARGS);
Datum halfvec_l2_normalize(PG_FUNCTION_ARGS);
Datum halfvec_binary_quantize(PG_FUNCTION_ARGS);
Datum halfvec_subvector(PG_FUNCTION_ARGS);
Datum halfvec_add(PG_FUNCTION_ARGS);
Datum halfvec_sub(PG_FUNCTION_ARGS);
Datum halfvec_mul(PG_FUNCTION_ARGS);
Datum halfvec_concat(PG_FUNCTION_ARGS);
Datum halfvec_lt(PG_FUNCTION_ARGS);
Datum halfvec_le(PG_FUNCTION_ARGS);
Datum halfvec_eq(PG_FUNCTION_ARGS);
Datum halfvec_ne(PG_FUNCTION_ARGS);
Datum halfvec_ge(PG_FUNCTION_ARGS);
Datum halfvec_gt(PG_FUNCTION_ARGS);
Datum halfvec_cmp(PG_FUNCTION_ARGS);
Datum halfvec_l2_squared_distance(PG_FUNCTION_ARGS);
Datum halfvec_negative_inner_product(PG_FUNCTION_ARGS);
Datum halfvec_spherical_distance(PG_FUNCTION_ARGS);
Datum halfvec_accum(PG_FUNCTION_ARGS);
Datum halfvec_avg(PG_FUNCTION_ARGS);
Datum halfvec_combine(PG_FUNCTION_ARGS);
Datum halfvec(PG_FUNCTION_ARGS);
Datum halfvec_to_vector(PG_FUNCTION_ARGS);
Datum vector_to_halfvec(PG_FUNCTION_ARGS);
Datum array_to_halfvec(PG_FUNCTION_ARGS);
Datum array_to_halfvec(PG_FUNCTION_ARGS);
Datum array_to_halfvec(PG_FUNCTION_ARGS);
Datum array_to_halfvec(PG_FUNCTION_ARGS);
Datum halfvec_to_float4(PG_FUNCTION_ARGS);

#endif
