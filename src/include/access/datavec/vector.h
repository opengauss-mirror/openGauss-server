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
 * vector.h
 *
 * IDENTIFICATION
 *        src/include/access/datavec/vector.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef VECTOR_H
#define VECTOR_H

#define VECTOR_MAX_DIM 16000
#define MEM_INFO_NUM (1024 * 1024)

#define VECTOR_SIZE(_dim) (offsetof(Vector, x) + sizeof(float) * (_dim))
#define DatumGetVector(x) ((Vector *)PG_DETOAST_DATUM(x))
#define PG_GETARG_VECTOR_P(x) DatumGetVector(PG_GETARG_DATUM(x))
#define PG_RETURN_VECTOR_P(x) PG_RETURN_POINTER(x)
#define UpdateProgress(index, val) ((void)(val))

typedef struct Vector {
    int32 vl_len_; /* varlena header (do not touch directly!) */
    int16 dim;     /* number of dimensions */
    int16 unused;  /* reserved for future use, always zero */
    float x[FLEXIBLE_ARRAY_MEMBER];
} Vector;

#if defined(USE_TARGET_CLONES) && !defined(__FMA__)
#define VECTOR_TARGET_CLONES __attribute__((target_clones("default", "fma")))
#else
#define VECTOR_TARGET_CLONES
#endif

VECTOR_TARGET_CLONES float VectorL2SquaredDistance(int dim, float *ax, float *bx);
VECTOR_TARGET_CLONES float VectorInnerProduct(int dim, float *ax, float *bx);
Vector *InitVector(int dim);
void PrintVector(char *msg, Vector *vector);
int vector_cmp_internal(Vector *a, Vector *b);
void VectorMadd(size_t n, const float *ax, float bf, const float *bx, float *cx);
void VectorL2SquaredDistanceNY(size_t d, size_t ny, float *x, char *pqTable, Size subSize, int offset, float *dis);
void VectorInnerProductNY(size_t d, size_t ny, float *x, char *pqTable, Size subSize, int offset, float *dis);
void LogNewpageRange(Relation rel, ForkNumber forknum, BlockNumber startblk, BlockNumber endblk, bool page_std);
int PlanCreateIndexWorkers(Relation heapRelation, IndexInfo *indexInfo);
void PrintOutVector(char *msg, Datum arg);

Datum vector_in(PG_FUNCTION_ARGS);
Datum vector_out(PG_FUNCTION_ARGS);
Datum vector_typmod_in(PG_FUNCTION_ARGS);
Datum vector_recv(PG_FUNCTION_ARGS);
Datum vector_send(PG_FUNCTION_ARGS);
Datum vector(PG_FUNCTION_ARGS);
Datum array_to_vector(PG_FUNCTION_ARGS);
Datum vector_to_float4(PG_FUNCTION_ARGS);
Datum vector_to_int4(PG_FUNCTION_ARGS);
Datum vector_to_float8(PG_FUNCTION_ARGS);
Datum vector_to_numeric(PG_FUNCTION_ARGS);
Datum vector_to_text(PG_FUNCTION_ARGS);
Datum vector_to_varchar(PG_FUNCTION_ARGS);
Datum l2_distance(PG_FUNCTION_ARGS);
Datum vector_l2_squared_distance(PG_FUNCTION_ARGS);
Datum inner_product(PG_FUNCTION_ARGS);
Datum vector_negative_inner_product(PG_FUNCTION_ARGS);
Datum cosine_distance(PG_FUNCTION_ARGS);
Datum vector_spherical_distance(PG_FUNCTION_ARGS);
Datum vector_dims(PG_FUNCTION_ARGS);
Datum vector_norm(PG_FUNCTION_ARGS);
Datum vector_add(PG_FUNCTION_ARGS);
Datum vector_sub(PG_FUNCTION_ARGS);
Datum vector_le(PG_FUNCTION_ARGS);
Datum vector_lt(PG_FUNCTION_ARGS);
Datum vector_eq(PG_FUNCTION_ARGS);
Datum vector_ne(PG_FUNCTION_ARGS);
Datum vector_ge(PG_FUNCTION_ARGS);
Datum vector_gt(PG_FUNCTION_ARGS);
Datum vector_cmp(PG_FUNCTION_ARGS);
Datum vector_accum(PG_FUNCTION_ARGS);
Datum vector_combine(PG_FUNCTION_ARGS);
Datum vector_avg(PG_FUNCTION_ARGS);
Datum l1_distance(PG_FUNCTION_ARGS);
Datum l2_normalize(PG_FUNCTION_ARGS);
Datum binary_quantize(PG_FUNCTION_ARGS);
Datum subvector(PG_FUNCTION_ARGS);
Datum vector_mul(PG_FUNCTION_ARGS);
Datum vector_concat(PG_FUNCTION_ARGS);

#endif
