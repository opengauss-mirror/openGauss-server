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
 *        src/include/db4ai/matrix.h
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

typedef struct Matrix {
    int rows;
    int columns;
    bool transposed;
    int allocated;
    float8 *data;
    float8 cache[MATRIX_CACHE];
} Matrix;

int matrix_expected_size(int rows, int columns = 1);

// initializes a bidimensional matrix or a vector (#columns = 1) to zeros
void matrix_init(Matrix *matrix, int rows, int columns = 1);

// initializes with a copy of a matrix
void matrix_init_clone(Matrix *matrix, const Matrix *src);

// initializes with a transposes a matrix, it is a virtual operation
void matrix_init_transpose(Matrix *matrix, const Matrix *src);

// initializes a matrix with the MacLaurin coefficients from 0 to a certain degree
void matrix_init_maclaurin_coefs(Matrix *matrix, int degree, float8 coef0);

// initializes a vector with random values following a gaussian distribution
void matrix_init_random_gaussian(Matrix *matrix, int rows, int columns, float8 mu, float8 sigma, int seed);

// initializes a vector with random values following a uniform distribution
void matrix_init_random_uniform(Matrix *matrix, int rows, int columns, float8 min, float8 max, int seed);

// initializes a vector with random values following a bernoulli distribution (yes/no with some probability)
void matrix_init_random_bernoulli(Matrix *matrix, int rows, int columns, float8 p, float8 min, float8 max, int seed);

// initializes the matrices required for a Gaussian kernel mapping
void matrix_init_kernel_gaussian(int features, int components, float8 gamma, int seed, Matrix *weights, Matrix *offsets);

// transforms a vector to a linear space using a gaussien kernel
void matrix_transform_kernel_gaussian(const Matrix *input, const Matrix *weights, const Matrix *offsets, Matrix *output);

// initializes the matrices and components required for a polynomial kernel mapping
// returns a vector of components and the two transformation matrices
int *matrix_init_kernel_polynomial(int features, int components, int degree, float8 coef0, int seed, Matrix *weights, Matrix *coefs);

// transforms a vector to a linear space using a polynomial kernel
void matrix_transform_kernel_polynomial(const Matrix *input, int ncomponents, int *components,
                                        const Matrix *weights, const Matrix *coefficients, Matrix *output);

// releases the memory of a matrix and makes it emmpty
void matrix_release(Matrix *matrix);

// replaces temporarily the pointer to the matrix data
float8* matrix_swap_data(Matrix *matrix, float8 *new_data);

// copies the shape and data from another matrix
void matrix_copy(Matrix *matrix, const Matrix *src);

// fills a matrix with zeroes
void matrix_zeroes(Matrix *matrix);

// fills a matrix with ones
void matrix_ones(Matrix *matrix);

// changes the shape of a matrix
// only number of rows will be done later
void matrix_resize(Matrix *matrix, int rows, int columns = 1);

// adds a vector to each row of a matrix
void matrix_add_vector(Matrix *matrix, const Matrix *vector);

// multiplies a matrix by a vector, row by row
void matrix_mult_vector(const Matrix *matrix, const Matrix *vector, Matrix *result);

// multiple two matrices by coefficients (a.k.a. Hadamard or Schur product)
void matrix_mult_entrywise(Matrix *m1, const Matrix *m2);

// multiplies a matrix by a scalar, coeeficient by coefficient
void matrix_mult_scalar(Matrix *matrix, float8 factor);

// divides a matrix by a scalar, coeeficient by coefficient
void matrix_divide(Matrix *matrix, float8 factor);

// adds a matrix by another matrix, coeeficient by coefficient
void matrix_add(Matrix *m1, const Matrix *m2);

// matrix multiplication
void matrix_mult(const Matrix *matrix1, const Matrix *matrix2, Matrix *result);

// adds a matrix by the product of another matrix with a scalar, coefficient by coefficient
void matrix_mult_scalar_add(Matrix *m1, const Matrix *m2, const float8 factor);

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

// computes the cosinus of all coefficients
void matrix_cos(Matrix *matrix);

// negates all coefficients (-c)
void matrix_negate(Matrix *matrix);

// complements all coefficients (1-c)
void matrix_complement(Matrix *matrix);

// makes sure all coeeficients are c>=0
void matrix_positive(Matrix *matrix);

// returns the sum of all coefficients
float8 matrix_get_sum(const Matrix *matrix);

// returns the average of all coefficients
float8 matrix_get_average(Matrix *matrix);

// returns the standard deviation of all coefficients
float8 matrix_get_stdev(Matrix *matrix);

// scales a matrix row by row, using two vectors ( N & D)
// where each coefficient is scale c'=(c-N)/D
// - normalization: N=min, D=(max-min)
// - standardization: N=avg, D=stdev
void matrix_scale(Matrix *matrix, const Matrix *m_n, const Matrix *m_d);

// computes the dot product of two vectors
float8 matrix_dot(const Matrix *v1, const Matrix *v2);

// converts all coefficients to binary values w.r.t. a threshold
// low: v<threshold; high: v>=threshold
void matrix_binary(Matrix *matrix, float8 threshold, float8 low, float8 high);

// compares two binary vectors
void matrix_relevance(const Matrix *v1, const Matrix *v2, Scores *scores, float8 positive);

// prints to a buffer
void matrix_print(const Matrix *matrix, StringInfo buf, bool full);

// prints into the log
void elog_matrix(int elevel, const char *msg, const Matrix *matrix);

void matrix_gram_schmidt(Matrix *matrix, int32_t const num_vectors);

// ///////////////////////////////////////////////////////////////////////////
// inline

inline int matrix_expected_size(int rows, int columns)
{
    Assert(rows > 0);
    Assert(columns > 0);
    int cells = rows * columns;
    if (cells <= MATRIX_CACHE)
        cells = 0; // cached, no extra memory

    return sizeof(Matrix) + cells * sizeof(float8);
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
        errno_t rc = memset_s(matrix->data, MATRIX_CACHE * sizeof(float8), 0, matrix->allocated * sizeof(float8));
        securec_check(rc, "", "");
    } else
        matrix->data = (float8 *)palloc0(matrix->allocated * sizeof(float8));
}

inline void matrix_init_clone(Matrix *matrix, const Matrix *src)
{
    Assert(matrix != nullptr);
    Assert(src != nullptr);
    Assert(!src->transposed);
    matrix_init(matrix, src->rows, src->columns);
    size_t bytes = src->rows * src->columns * sizeof(float8);
    errno_t rc = memcpy_s(matrix->data, matrix->allocated * sizeof(float8), src->data, bytes);
    securec_check(rc, "", "");
}

// fake transpose, only points to the data of the other matrix
inline void matrix_init_transpose(Matrix *matrix, const Matrix *src)
{
    Assert(matrix != nullptr);
    Assert(src != nullptr);
    Assert(!src->transposed);
    // it is not necessary to mark vectors as transposed
    matrix->transposed = (src->columns > 1);
    matrix->rows = src->columns;
    matrix->columns = src->rows;
    matrix->allocated = 0;
    matrix->data = src->data;
}

static int nCr(int n, int r) {
    // ncr = n1 / (r! (n-r)!)
    Assert(n >= r);
    if (n == r)
        return 1;

    r = Min(r, n-r);

    int nume = 1;
    for (int i = n-r+1; i <= n; i++)
        nume *= i;

    int deno = 1;
    for (int i = 2; i <= r; i++)
        deno *= i;

    return nume / deno;
}

inline void matrix_init_maclaurin_coefs(Matrix *matrix, int degree, float8 coef0)
{
    matrix_init(matrix, degree + 1);
    float8 *pcoefs = matrix->data;
    for (int k = 0; k <= degree; k++) {
        float8 coef = 0;
        if (coef0 > 0)
            coef = nCr(degree, k) * pow(coef0, degree - k);

        *pcoefs++ = sqrt(coef * (1 << (k + 1)));
    }
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

inline float8* matrix_swap_data(Matrix *matrix, float8 *new_data) {
    Assert(matrix != nullptr);
    Assert(!matrix->transposed);
    Assert(new_data != nullptr);
    float8* current = matrix->data;
    matrix->data = new_data;
    return current;
}

inline void matrix_copy(Matrix *matrix, const Matrix *src)
{
    Assert(matrix != nullptr);
    Assert(src != nullptr);
    Assert(!src->transposed);

    int count = src->rows * src->columns;
    if (count > matrix->allocated) {
        // resize
        matrix->allocated = count;
        if (matrix->allocated > MATRIX_CACHE) {
            // realloc
            if (matrix->data != matrix->cache)
                pfree(matrix->data);

            matrix->data = (float8 *)palloc0(matrix->allocated * sizeof(float8));
        }
    }

    matrix->rows = src->rows;
    matrix->columns = src->columns;
    errno_t rc = memcpy_s(matrix->data, matrix->allocated * sizeof(float8),
                            src->data, sizeof(float8) * count);
    securec_check(rc, "", "");
}

inline void matrix_zeroes(Matrix *matrix)
{
    Assert(matrix != nullptr);
    Assert(!matrix->transposed);
    errno_t rc = memset_s(matrix->data, sizeof(float8) * matrix->allocated, 0,
        sizeof(float8) * matrix->rows * matrix->columns);
    securec_check(rc, "", "");
}

inline void matrix_ones(Matrix *matrix)
{
    Assert(matrix != nullptr);
    size_t count = matrix->rows * matrix->columns;
    float8 *pd = matrix->data;
    while (count-- > 0)
        *pd++ = 1.0;
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

inline void matrix_add_vector(Matrix *matrix, const Matrix *vector)
{
    Assert(matrix != nullptr);
    Assert(!matrix->transposed);
    Assert(vector != nullptr);
    Assert(!vector->transposed);
    Assert(vector->columns == 1);
    Assert(matrix->columns == vector->rows);

    float8 *pm = matrix->data;
    for (int r = 0; r < matrix->rows; r++) {
        const float8 *pv = vector->data;
        size_t count = matrix->columns;
        while (count-- > 0) {
            *pm += *pv++;
            pm++;
        }
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
        float8 *pd = result->data;
        // loop assumes that the data has not been physically transposed
        for (int r = 0; r < matrix->rows; r++) {
            const float8 *pm = matrix->data + r;
            const float8 *pv = vector->data;
            float8 x = 0.0;
            for (int c = 0; c < matrix->columns; c++) {
                x += *pm * *pv++;
                pm += matrix->rows;
            }
            *pd++ = x;
        }
    } else {
        const float8 *pm = matrix->data;
        float8 *pd = result->data;
        for (int r = 0; r < matrix->rows; r++) {
            const float8 *pv = vector->data;
            size_t count = matrix->columns;
            float8 x = 0.0;
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
    float8 *pd = m1->data;
    const float8 *ps = m2->data;
    while (count-- > 0)
        *pd++ *= *ps++;
}

inline void matrix_mult_scalar(Matrix *matrix, float8 factor)
{
    Assert(matrix != nullptr);
    size_t count = matrix->rows * matrix->columns;
    float8 *pd = matrix->data;
    while (count-- > 0)
        *pd++ *= factor;
}

inline void matrix_divide(Matrix *matrix, float8 factor)
{
    Assert(matrix != nullptr);
    Assert(factor != 0.0);
    size_t count = matrix->rows * matrix->columns;
    float8 *pd = matrix->data;
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
    float8 *p1 = m1->data;
    const float8 *p2 = m2->data;
    while (count-- > 0)
        *p1++ += *p2++;
}

inline void matrix_mult_scalar_add(Matrix *m1, const Matrix *m2, const float8 factor)
{
    Assert(m1 != nullptr);
    Assert(!m1->transposed);
    Assert(m2 != nullptr);
    Assert(!m2->transposed);
    Assert(m1->rows == m2->rows);
    Assert(m1->columns == m2->columns);
    size_t count = m1->rows * m1->columns;
    float8 *p1 = m1->data;
    const float8 *p2 = m2->data;
    while (count-- > 0)
        *p1++ += factor * *p2++;
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
    float8 *p1 = m1->data;
    const float8 *p2 = m2->data;
    while (count-- > 0)
        *p1++ -= *p2++;
}

inline float8 matrix_dot(const Matrix *v1, const Matrix *v2)
{
    Assert(v1 != nullptr);
    Assert(!v1->transposed);
    Assert(v2 != nullptr);
    Assert(!v2->transposed);
    Assert(v1->rows == v2->rows);
    Assert(v1->columns == 1);
    Assert(v2->columns == 1);

    size_t count = v1->rows;
    const float8 *p1 = v1->data;
    const float8 *p2 = v2->data;
    float8 result = 0;
    while (count-- > 0)
        result += *p1++ * *p2++;

    return result;
}

inline void matrix_square(Matrix *matrix)
{
    Assert(matrix != nullptr);
    size_t count = matrix->rows * matrix->columns;
    float8 *pd = matrix->data;
    while (count-- > 0) {
        *pd *= *pd;
        pd++;
    }
}

inline void matrix_square_root(Matrix *matrix)
{
    Assert(matrix != nullptr);
    size_t count = matrix->rows * matrix->columns;
    float8 *pd = matrix->data;
    while (count-- > 0) {
        *pd = sqrt(*pd);
        pd++;
    }
}

inline void matrix_cos(Matrix *matrix)
{
    Assert(matrix != nullptr);
    size_t count = matrix->rows * matrix->columns;
    float8 *pd = matrix->data;
    while (count-- > 0) {
        *pd = cos(*pd);
        pd++;
    }
}

inline void matrix_sigmoid(Matrix *matrix)
{
    Assert(matrix != nullptr);
    size_t count = matrix->rows * matrix->columns;
    float8 *pd = matrix->data;
    while (count-- > 0) {
        float8 c = *pd;
        *pd++ = 1.0 / (1.0 + exp(-c));
    }
}

inline void matrix_log(Matrix *matrix)
{
    Assert(matrix != nullptr);
    size_t count = matrix->rows * matrix->columns;
    float8 *pd = matrix->data;
    while (count-- > 0) {
        float8 v = *pd;
        *pd++ = log(v);
    }
}

inline void matrix_log1p(Matrix *matrix)
{
    Assert(matrix != nullptr);
    size_t count = matrix->rows * matrix->columns;
    float8 *pd = matrix->data;
    while (count-- > 0) {
        float8 v = *pd + 1;
        *pd++ = log(v);
    }
}

inline void matrix_negate(Matrix *matrix)
{
    Assert(matrix != nullptr);
    size_t count = matrix->rows * matrix->columns;
    float8 *pd = matrix->data;
    while (count-- > 0) {
        float8 v = *pd;
        *pd++ = -v;
    }
}

inline void matrix_complement(Matrix *matrix)
{
    Assert(matrix != nullptr);
    size_t count = matrix->rows * matrix->columns;
    float8 *pd = matrix->data;
    while (count-- > 0) {
        float8 v = 1.0 - *pd;
        *pd++ = v;
    }
}

inline void matrix_positive(Matrix *matrix)
{
    Assert(matrix != nullptr);
    size_t count = matrix->rows * matrix->columns;
    float8 *pd = matrix->data;
    while (count-- > 0) {
        float8 v = *pd;
        if (v < 0.0)
            *pd = 0.0;
        pd++;
    }
}

inline float8 matrix_get_sum(const Matrix *matrix)
{
    Assert(matrix != nullptr);
    size_t count = matrix->rows * matrix->columns;
    float8 *ps = matrix->data;
    float8 s = 0.0;
    while (count-- > 0)
        s += *ps++;
    return s;
}

inline float8 matrix_get_average(Matrix *matrix)
{
    Assert(matrix != nullptr);
    int total = matrix->rows * matrix->columns;
    int count = total;
    float8 *ps = matrix->data;
    float8 s = 0.0;
    while (count-- > 0)
        s += *ps++;
    return s / total;
}

inline float8 matrix_get_stdev(Matrix *matrix)
{
    Assert(matrix != nullptr);
    int total = matrix->rows * matrix->columns;
    int count = total;
    float8 *ps = matrix->data;
    float8 s = 0.0;
    float8 sq = 0.0;
    while (count-- > 0) {
        float8 v = *ps++;
        s += v;
        sq += v * v;
    }
    s /= total;
    return sqrt((sq / total) - (s * s));
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
    float8 *pd = matrix->data;
    for (int r = 0; r < matrix->rows; r++) {
        const float8 *p1 = m_n->data;
        const float8 *p2 = m_d->data;
        for (int c = 0; c < matrix->columns; c++) {
            *pd = (*pd - *p1++) / *p2++;
            pd++;
        }
    }
}

inline void matrix_binary(Matrix *matrix, float8 threshold, float8 low, float8 high)
{
    Assert(matrix != nullptr);
    size_t count = matrix->rows * matrix->columns;
    float8 *pd = matrix->data;
    while (count-- > 0) {
        float8 v = *pd;
        *pd++ = (v < threshold ? low : high);
    }
}

inline void matrix_relevance(const Matrix *v1, const Matrix *v2, Scores *scores, float8 positive)
{
    Assert(v1 != nullptr);
    Assert(!v1->transposed);
    Assert(v2 != nullptr);
    Assert(!v2->transposed);
    Assert(v1->rows == v2->rows);
    Assert(v1->columns == 1);
    Assert(v2->columns == 1);

    size_t count = v1->rows;
    scores->count += count;

    const float8 *p1 = v1->data;
    const float8 *p2 = v2->data;
    while (count-- > 0) {
        float8 x = *p1++;
        float8 y = *p2++;
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
    }
}

inline void matrix_gram_schmidt(Matrix *matrix, int32_t const num_vectors)
{
    int32_t const dimension = matrix->rows;
    Matrix done_vector;
    Matrix current_vector;
    done_vector.rows = current_vector.rows = dimension;
    done_vector.columns = current_vector.columns = 1;
    done_vector.allocated = current_vector.allocated = dimension;
    done_vector.transposed = current_vector.transposed = false;
    float8 projection = 0.;
    float8 squared_magnitude = 0.;
    float8 magnitude = 0.;
    int32_t first_non_zero = -1;

    /*
     * we first find the very first eigenvector with non-zero magnitude
     * and normalize it (the order of the test in the loop matters)
     */
    while ((magnitude == 0.) && (++first_non_zero < num_vectors)) {
        current_vector.data = matrix->data + (first_non_zero * dimension);
        squared_magnitude = matrix_dot(&current_vector, &current_vector);
        if (squared_magnitude == 0.)
            continue;
        magnitude = std::sqrt(squared_magnitude);
        matrix_mult_scalar(&current_vector, 1. / magnitude);
    }

    /*
     * no valid vector found :(
     */
    if (unlikely(first_non_zero == num_vectors))
        ereport(ERROR, (errmodule(MOD_DB4AI),
                errmsg("Gram-Schmidt: No vector of non-zero magnitude found")));

    /*
     * we can indeed orthonormalized at least one vector, let's go
     */
    for (int32_t cv = first_non_zero + 1; cv < num_vectors; ++cv) {
        current_vector.data = matrix->data + (cv * dimension);
        // we go thru previously orthonormalized vectors to produce the next one
        for (int32_t dv = first_non_zero; dv < cv; ++dv) {
            done_vector.data = matrix->data + (dv * dimension);

            projection = matrix_dot(&current_vector, &done_vector);

            /*
             * no shadow is cast, and thus the vectors are perpendicular
             * and we can continue with the next vector in the upper loop
             */
            if (projection == 0.) {
                dv = cv;
                continue;
            }

            matrix_mult_scalar(&done_vector, projection);
            matrix_subtract(&current_vector, &done_vector);
            /*
             * for the time being we are using no extra space and thus we have
             * to normalize again a previously-ready vector
             */
            squared_magnitude = matrix_dot(&done_vector, &done_vector);
            magnitude = std::sqrt(squared_magnitude);
            matrix_mult_scalar(&done_vector, 1. / magnitude);
        }
        squared_magnitude = matrix_dot(&current_vector, &current_vector);
        magnitude = std::sqrt(squared_magnitude);
        matrix_mult_scalar(&current_vector, 1. / magnitude);
    }
}

#endif /* MATRIX_H */
