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
 * sparsevec.h
 *
 * IDENTIFICATION
 *        src/include/access/datavec/sparsevec.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef SPARSEVEC_H
#define SPARSEVEC_H

#define SPARSEVEC_MAX_DIM 1000000000
#define SPARSEVEC_MAX_NNZ 16000

#define DatumGetSparseVector(x) ((SparseVector *)PG_DETOAST_DATUM(x))
#define PG_GETARG_SPARSEVEC_P(x) DatumGetSparseVector(PG_GETARG_DATUM(x))
#define PG_RETURN_SPARSEVEC_P(x) PG_RETURN_POINTER(x)

/*
 * Indices use 0-based numbering for the on-disk (and binary) format (consistent with C)
 * and are always sorted. Values come after indices.
 */

Datum sparsevec_in(PG_FUNCTION_ARGS);
Datum sparsevec_out(PG_FUNCTION_ARGS);
Datum sparsevec_typmod_in(PG_FUNCTION_ARGS);
Datum sparsevec_recv(PG_FUNCTION_ARGS);
Datum sparsevec_send(PG_FUNCTION_ARGS);
Datum sparsevec_l2_distance(PG_FUNCTION_ARGS);
Datum sparsevec_inner_product(PG_FUNCTION_ARGS);
Datum sparsevec_cosine_distance(PG_FUNCTION_ARGS);
Datum sparsevec_l1_distance(PG_FUNCTION_ARGS);
Datum sparsevec_l2_norm(PG_FUNCTION_ARGS);
Datum sparsevec_l2_normalize(PG_FUNCTION_ARGS);
Datum sparsevec_lt(PG_FUNCTION_ARGS);
Datum sparsevec_le(PG_FUNCTION_ARGS);
Datum sparsevec_eq(PG_FUNCTION_ARGS);
Datum sparsevec_ne(PG_FUNCTION_ARGS);
Datum sparsevec_ge(PG_FUNCTION_ARGS);
Datum sparsevec_gt(PG_FUNCTION_ARGS);
Datum sparsevec_cmp(PG_FUNCTION_ARGS);
Datum sparsevec_l2_squared_distance(PG_FUNCTION_ARGS);
Datum sparsevec_negative_inner_product(PG_FUNCTION_ARGS);
Datum sparsevec(PG_FUNCTION_ARGS);
Datum vector_to_sparsevec(PG_FUNCTION_ARGS);
Datum sparsevec_to_vector(PG_FUNCTION_ARGS);
Datum halfvec_to_sparsevec(PG_FUNCTION_ARGS);
Datum sparsevec_to_halfvec(PG_FUNCTION_ARGS);

typedef struct SparseVector {
    int32 vl_len_; /* varlena header (do not touch directly!) */
    int32 dim;     /* number of dimensions */
    int32 nnz;     /* number of non-zero elements */
    int32 unused;  /* reserved for future use, always zero */
    int32 indices[FLEXIBLE_ARRAY_MEMBER];
} SparseVector;

/* Use functions instead of macros to avoid double evaluation */

static inline Size SPARSEVEC_SIZE(int nnz)
{
    return offsetof(SparseVector, indices) + (nnz * sizeof(int32)) + (nnz * sizeof(float));
}

static inline float *SPARSEVEC_VALUES(SparseVector *x)
{
    return (float *)(((char *)x) + offsetof(SparseVector, indices) + (x->nnz * sizeof(int32)));
}

SparseVector *InitSparseVector(int dim, int nnz);

#endif
