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
 *---------------------------------------------------------------------------------------
 *
 *  matrix.h
 *
 * IDENTIFICATION
 *        src/include/dbmind/db4ai/executor/gd/matrix.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef MATRIX_H
#define MATRIX_H

#include "postgres.h"
#include "lib/stringinfo.h"
#include "db4ai/scores.h"

#include "math.h"
#include "float.h"

#define MATRIX_CACHE 16

typedef float4 gd_float;

typedef struct Matrix {
    int rows;
    int columns;
    bool transposed;
    int allocated;
    gd_float *data;
    gd_float cache[MATRIX_CACHE];
} Matrix;

int matrix_expected_size(int rows, int columns = 1);

// initializes a bidimensional matrix or a vector (#columns = 1) to zeros
void matrix_init(Matrix *matrix, int rows, int columns = 1);

// initializes with a copy of a matrix
void matrix_init_clone(Matrix *matrix, const Matrix *src);

// initializes with a transposes a matrix, it is a virtual operation
void matrix_init_transpose(Matrix *matrix, const Matrix *src);

// releases the memory of a matrix and makes it emmpty
void matrix_release(Matrix *matrix);

// fiils a matrix with zeroes
void matrix_zeroes(Matrix *matrix);

// changes the shape of a matrix
// only number of rows will be done later
void matrix_resize(Matrix *matrix, int rows, int columns);

// multiplies a matrix by a vector, row by row
void matrix_mult_vector(const Matrix *matrix, const Matrix *vector, Matrix *result);

// multiple two matrices by coefficients (a.k.a. Hadamard or Schur product)
void matrix_mult_entrywise(Matrix *m1, const Matrix *m2);

// multiplies a matrix by a scalar, coeeficient by coefficient
void matrix_mult_scalar(Matrix *matrix, gd_float factor);

// divides a matrix by a scalar, coeeficient by coefficient
void matrix_divide(Matrix *matrix, gd_float factor);

// adds a matrix by another matrix, coeeficient by coefficient
void matrix_add(Matrix *m1, const Matrix *m2);

// subtracts a matrix by another matrix, coeeficient by coefficient
void matrix_subtract(Matrix *m1, const Matrix *m2);

// squares all coefficients
void matrix_square(Matrix *matrix);

// obtains the square root of all coefficients
void matrix_square_root(Matrix *matrix);

// computes the sigmoid of all coefficients: 1.0 / (1.0 + exp(-c))
void matrix_sigmoid(Matrix *matrix);

// computes the natural logarithm of all coefficients
void matrix_log(Matrix *matrix);

// computes the natural logarithm log(1+c) of all coefficients
void matrix_log1p(Matrix *matrix);

// negates all coefficients (-c)
void matrix_negate(Matrix *matrix);

// complements all coefficients (1-c)
void matrix_complement(Matrix *matrix);

// make sure all coeeficients are c>=0
void matrix_positive(Matrix *matrix);

// return the sum of all coefficients
gd_float matrix_get_sum(Matrix *matrix);

// scales a matrix row by row, using two vectors ( N & D)
// where each coefficient is scale c'=(c-N)/D
// - normalization: N=min, D=(max-min)
// - standardization: N=avg, D=stdev
void matrix_scale(Matrix *matrix, const Matrix *m_n, const Matrix *m_d);

// computes the dot product of two vectors
gd_float matrix_dot(const Matrix *v1, const Matrix *v2);

// converts all coefficients to binary values w.r.t. a threshold
// low: v<threshold; high: v>=threshold
void matrix_binary(Matrix *matrix, gd_float threshold, gd_float low, gd_float high);

// compares two binary vectors
void matrix_relevance(const Matrix *v1, const Matrix *v2, Scores *scores, gd_float positive);

// prints to a buffer
void matrix_print(const Matrix *matrix, StringInfo buf, bool full);

// prints into the log
void elog_matrix(int elevel, const char *msg, const Matrix *matrix);

// ///////////////////////////////////////////////////////////////////////////
// inline

inline int matrix_expected_size(int rows, int columns)
{
    Assert(rows > 0);
    Assert(columns > 0);
    int cells = rows * columns;
    if (cells <= MATRIX_CACHE)
        cells = 0; // cached, no extra memory

    return sizeof(Matrix) + cells * sizeof(gd_float);
}

inline void matrix_init(Matrix *matrix, int rows, int columns)
{
    Assert(matrix != nullptr);
    Assert(rows > 0);
    Assert(columns > 0);
    matrix->transposed = false;
    matrix->rows = rows;
    matrix->columns = columns;
    matrix->allocated = rows * columns;
    if (matrix->allocated <= MATRIX_CACHE) {
        matrix->data = matrix->cache;
        errno_t rc = memset_s(matrix->data, MATRIX_CACHE * sizeof(gd_float), 0, matrix->allocated * sizeof(gd_float));
        securec_check(rc, "", "");
    } else
        matrix->data = (gd_float *)palloc0(matrix->allocated * sizeof(gd_float));
}

inline void matrix_init_clone(Matrix *matrix, const Matrix *src)
{
    Assert(matrix != nullptr);
    Assert(src != nullptr);
    Assert(!src->transposed);
    matrix_init(matrix, src->rows, src->columns);
    size_t bytes = src->rows * src->columns * sizeof(gd_float);
    errno_t rc = memcpy_s(matrix->data, bytes, src->data, bytes);
    securec_check(rc, "", "");
}

// fake transpose, only points to the data of the other matrix
inline void matrix_init_transpose(Matrix *matrix, const Matrix *src)
{
    Assert(matrix != nullptr);
    Assert(src != nullptr);
    Assert(!src->transposed);
    matrix->transposed = true;
    matrix->rows = src->columns;
    matrix->columns = src->rows;
    matrix->allocated = 0;
    matrix->data = src->data;
}

inline void matrix_release(Matrix *matrix)
{
    Assert(matrix != nullptr);
    if (matrix->allocated > 0) {
        Assert(matrix->data != nullptr);
        if (matrix->data != matrix->cache)
            pfree(matrix->data);

        matrix->allocated = 0;
    }
    matrix->data = nullptr;
    matrix->rows = 0;
    matrix->columns = 0;
}

inline void matrix_zeroes(Matrix *matrix)
{
    Assert(matrix != nullptr);
    Assert(!matrix->transposed);
    errno_t rc = memset_s(matrix->data, sizeof(gd_float) * matrix->allocated, 0,
        sizeof(gd_float) * matrix->rows * matrix->columns);
    securec_check(rc, "", "");
}

inline void matrix_resize(Matrix *matrix, int rows, int columns)
{
    Assert(matrix != nullptr);
    Assert(!matrix->transposed);
    Assert(rows > 0);
    Assert(columns > 0);
    if (columns != matrix->columns)
        ereport(ERROR,
            (errmodule(MOD_DB4AI), errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("resize column not yet supported")));

    if (rows != matrix->rows) {
        if (rows > matrix->rows) {
            int required = rows * matrix->columns;
            if (required > matrix->allocated)
                ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("matrix growth not yet supported")));
        }
        matrix->rows = rows;
    }
}

inline void matrix_mult_vector(const Matrix *matrix, const Matrix *vector, Matrix *result)
{
    Assert(matrix != nullptr);
    Assert(vector != nullptr);
    Assert(vector->columns == 1);
    Assert(!vector->transposed);
    Assert(result != nullptr);
    Assert(!result->transposed);
    Assert(result->columns == 1);
    Assert(matrix->rows == result->rows);
    Assert(matrix->columns == vector->rows);

    if (matrix->transposed) {
        gd_float *pd = result->data;
        // loop assumes that the data has not been physically transposed
        for (int r = 0; r < matrix->rows; r++) {
            const gd_float *pm = matrix->data + r;
            const gd_float *pv = vector->data;
            gd_float x = 0.0;
            for (int c = 0; c < matrix->columns; c++) {
                x += *pm * *pv++;
                pm += matrix->rows;
            }
            *pd++ = x;
        }
    } else {
        const gd_float *pm = matrix->data;
        gd_float *pd = result->data;
        for (int r = 0; r < matrix->rows; r++) {
            const gd_float *pv = vector->data;
            size_t count = matrix->columns;
            gd_float x = 0.0;
            while (count-- > 0)
                x += *pv++ * *pm++;
            *pd++ = x;
        }
    }
}

inline void matrix_mult_entrywise(Matrix *m1, const Matrix *m2)
{
    Assert(m1 != nullptr);
    Assert(!m1->transposed);
    Assert(m2 != nullptr);
    Assert(!m2->transposed);
    Assert(m1->rows == m2->rows);
    Assert(m1->columns == m2->columns);

    size_t count = m1->rows * m1->columns;
    gd_float *pd = m1->data;
    const gd_float *ps = m2->data;
    while (count-- > 0)
        *pd++ *= *ps++;
}

inline void matrix_mult_scalar(Matrix *matrix, gd_float factor)
{
    Assert(matrix != nullptr);
    size_t count = matrix->rows * matrix->columns;
    gd_float *pd = matrix->data;
    while (count-- > 0)
        *pd++ *= factor;
}

inline void matrix_divide(Matrix *matrix, gd_float factor)
{
    Assert(matrix != nullptr);
    Assert(factor != 0.0);
    size_t count = matrix->rows * matrix->columns;
    gd_float *pd = matrix->data;
    while (count-- > 0)
        *pd++ /= factor;
}

inline void matrix_add(Matrix *m1, const Matrix *m2)
{
    Assert(m1 != nullptr);
    Assert(!m1->transposed);
    Assert(m2 != nullptr);
    Assert(!m2->transposed);
    Assert(m1->rows == m2->rows);
    Assert(m1->columns == m2->columns);
    size_t count = m1->rows * m1->columns;
    gd_float *p1 = m1->data;
    const gd_float *p2 = m2->data;
    while (count-- > 0)
        *p1++ += *p2++;
}

inline void matrix_subtract(Matrix *m1, const Matrix *m2)
{
    Assert(m1 != nullptr);
    Assert(!m1->transposed);
    Assert(m2 != nullptr);
    Assert(!m2->transposed);
    Assert(m1->rows == m2->rows);
    Assert(m1->columns == m2->columns);
    size_t count = m1->rows * m1->columns;
    gd_float *p1 = m1->data;
    const gd_float *p2 = m2->data;
    while (count-- > 0)
        *p1++ -= *p2++;
}

inline gd_float matrix_dot(const Matrix *v1, const Matrix *v2)
{
    Assert(v1 != nullptr);
    Assert(!v1->transposed);
    Assert(v2 != nullptr);
    Assert(!v2->transposed);
    Assert(v1->rows == v2->rows);
    Assert(v1->columns == 1);
    Assert(v2->columns == 1);

    size_t count = v1->rows;
    const gd_float *p1 = v1->data;
    const gd_float *p2 = v2->data;
    gd_float result = 0;
    while (count-- > 0)
        result += *p1++ * *p2++;

    return result;
}

inline void matrix_square(Matrix *matrix)
{
    Assert(matrix != nullptr);
    size_t count = matrix->rows * matrix->columns;
    gd_float *pd = matrix->data;
    while (count-- > 0) {
        *pd *= *pd;
        pd++;
    }
}

inline void matrix_square_root(Matrix *matrix)
{
    Assert(matrix != nullptr);
    size_t count = matrix->rows * matrix->columns;
    gd_float *pd = matrix->data;
    while (count-- > 0) {
        *pd = sqrt(*pd);
        pd++;
    }
}

inline void matrix_sigmoid(Matrix *matrix)
{
    Assert(matrix != nullptr);
    size_t count = matrix->rows * matrix->columns;
    gd_float *pd = matrix->data;
    while (count-- > 0) {
        gd_float c = *pd;
        *pd++ = 1.0 / (1.0 + exp(-c));
    }
}

inline void matrix_log(Matrix *matrix)
{
    Assert(matrix != nullptr);
    size_t count = matrix->rows * matrix->columns;
    gd_float *pd = matrix->data;
    while (count-- > 0) {
        gd_float v = *pd;
        *pd++ = log(v);
    }
}

inline void matrix_log1p(Matrix *matrix)
{
    Assert(matrix != nullptr);
    size_t count = matrix->rows * matrix->columns;
    gd_float *pd = matrix->data;
    while (count-- > 0) {
        gd_float v = *pd + 1;
        *pd++ = log(v);
    }
}

inline void matrix_negate(Matrix *matrix)
{
    Assert(matrix != nullptr);
    size_t count = matrix->rows * matrix->columns;
    gd_float *pd = matrix->data;
    while (count-- > 0) {
        gd_float v = *pd;
        *pd++ = -v;
    }
}

inline void matrix_complement(Matrix *matrix)
{
    Assert(matrix != nullptr);
    size_t count = matrix->rows * matrix->columns;
    gd_float *pd = matrix->data;
    while (count-- > 0) {
        gd_float v = 1.0 - *pd;
        *pd++ = v;
    }
}

inline void matrix_positive(Matrix *matrix)
{
    Assert(matrix != nullptr);
    size_t count = matrix->rows * matrix->columns;
    gd_float *pd = matrix->data;
    while (count-- > 0) {
        gd_float v = *pd;
        if (v < 0.0)
            *pd = 0.0;
        pd++;
    }
}

inline gd_float matrix_get_sum(Matrix *matrix)
{
    Assert(matrix != nullptr);
    size_t count = matrix->rows * matrix->columns;
    gd_float *ps = matrix->data;
    gd_float s = 0.0;
    while (count-- > 0)
        s += *ps++;
    return s;
}

inline void matrix_scale(Matrix *matrix, const Matrix *m_n, const Matrix *m_d)
{
    Assert(matrix != nullptr);
    Assert(m_n != nullptr);
    Assert(m_d != nullptr);
    Assert(!matrix->transposed);
    Assert(!m_n->transposed);
    Assert(matrix->columns == m_n->rows);
    Assert(m_n->columns == 1);
    Assert(!m_d->transposed);
    Assert(matrix->columns == m_d->rows);
    Assert(m_d->columns == 1);
    gd_float *pd = matrix->data;
    for (int r = 0; r < matrix->rows; r++) {
        const gd_float *p1 = m_n->data;
        const gd_float *p2 = m_d->data;
        for (int c = 0; c < matrix->columns; c++) {
            *pd = (*pd - *p1++) / *p2++;
            pd++;
        }
    }
}

inline void matrix_binary(Matrix *matrix, gd_float threshold, gd_float low, gd_float high)
{
    Assert(matrix != nullptr);
    size_t count = matrix->rows * matrix->columns;
    gd_float *pd = matrix->data;
    while (count-- > 0) {
        gd_float v = *pd;
        *pd++ = (v < threshold ? low : high);
    }
}

inline void matrix_relevance(const Matrix *v1, const Matrix *v2, Scores *scores, gd_float positive)
{
    Assert(v1 != nullptr);
    Assert(!v1->transposed);
    Assert(v2 != nullptr);
    Assert(!v2->transposed);
    Assert(v1->rows == v2->rows);
    Assert(v1->columns == 1);
    Assert(v2->columns == 1);

    size_t count = v1->rows;
    const gd_float *p1 = v1->data;
    const gd_float *p2 = v2->data;
    while (count-- > 0) {
        gd_float x = *p1++;
        gd_float y = *p2++;
        if (x == positive) {
            // positive
            if (y == positive)
                scores->tp++;
            else
                scores->fp++;
        } else {
            // negative
            if (y != positive)
                scores->tn++;
            else
                scores->fn++;
        }
        scores->count++;
    }
}

#endif /* MATRIX_H */
