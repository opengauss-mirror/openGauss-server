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
 *  matrix.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/dbmind/db4ai/executor/matrix.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "db4ai/matrix.h"

#define MATRIX_LIMITED_OUTPUT 30

// using Box-Muller implementation
void matrix_init_random_gaussian(Matrix *matrix, int rows, int columns, float8 mu, float8 sigma, int seed)
{
    matrix_init(matrix, rows, columns);

    const float8 two_pi = 2.0 * M_PI;
    struct drand48_data rnd;
    srand48_r(seed, &rnd);

    float8 u1, u2;
    float8 *pd = matrix->data;
    float8 *pdx = pd + rows * columns;
    while (pd < pdx) {
        do {
            drand48_r(&rnd, &u1);
        } while (u1 <= FLT_EPSILON);

        drand48_r(&rnd, &u2);
        u2 *= two_pi;

        float8 mag = sigma * sqrt(-2.0 * log(u1));
        *pd++ = mag * cos(u2) + mu;
        if (pd < pdx)
            *pd++ = mag * sin(u2) + mu;
    }
}

void matrix_init_kernel_gaussian(int features, int components, float8 gamma, int seed, Matrix *weights, Matrix *offsets)
{
    matrix_init_random_gaussian(weights, features, components, 0.0, sqrt(2.0 * gamma), seed);
    matrix_init_random_uniform(offsets, components, 1, 0.0, 2.0 * M_PI, seed+1);
}

void matrix_transform_kernel_gaussian(const Matrix *input, const Matrix *weights, const Matrix *offsets, Matrix *output)
{
    int components = weights->columns;

    Assert(input->rows == weights->rows);
    Assert(input->columns == 1);
    Assert(components == offsets->rows);
    Assert(offsets->columns == 1);
    Assert(output->rows == components);
    Assert(output->columns == 1);

    Matrix t_in, t_out;
    matrix_init_transpose(&t_in, input);
    matrix_init_transpose(&t_out, output);

    matrix_mult(&t_in, weights, &t_out);

    matrix_release(&t_in);
    matrix_release(&t_out);

    matrix_add(output, offsets);
    matrix_cos(output);
    matrix_mult_scalar(output, sqrt(2.0 / components));
}

void matrix_init_random_uniform(Matrix *matrix, int rows, int columns, float8 min, float8 max, int seed)
{
    Assert(min < max);

    matrix_init(matrix, rows, columns);

    float8 range = max - min;
    struct drand48_data rnd;
    srand48_r(seed, &rnd);

    float8 u;
    float8 *pd = matrix->data;
    float8 *pdx = pd + rows * columns;
    while (pd < pdx) {
        drand48_r(&rnd, &u);
        *pd++ = min + range * u;
    }
}

void matrix_init_random_bernoulli(Matrix *matrix, int rows, int columns, float8 p, float8 min, float8 max, int seed)
{
    matrix_init(matrix, rows, columns);

    struct drand48_data rnd;
    srand48_r(seed, &rnd);

    float8 r;
    float8 *pdata = matrix->data;
    int count = rows * columns;
    while (count--) {
        drand48_r(&rnd, &r);
        *pdata++ = (r < p ? min : max);
    }
}

int *matrix_init_kernel_polynomial(int features, int components, int degree, float8 coef0, int seed, Matrix *weights,
                                   Matrix *coefs)
{
    struct drand48_data rnd;
    srand48_r(seed, &rnd);

    Matrix mcoefs;
    matrix_init_maclaurin_coefs(&mcoefs, degree, coef0);

    int dims = 0;
    matrix_init(coefs, components);

    int *pcomponents = (int*)palloc(components * sizeof(int));
    for (int r = 0; r < components; r++) {
        int rep = 0;
        do {
            float8 rr = 0;
            drand48_r(&rnd, &rr);
            if (rr == 0) {
                continue;
            }
            rep = (int)log2(1.0 / rr);
        } while (rep == 0 || rep > degree);
        dims += rep;
        pcomponents[r] = rep;
        coefs->data[r] = mcoefs.data[rep];
    }
    matrix_release(&mcoefs);

    matrix_init_random_bernoulli(weights, dims, features, 0.5, -1, 1, seed);

    return pcomponents;
}

void matrix_transform_kernel_polynomial(const Matrix *input, int ncomponents, int *components, const Matrix *weights,
                                        const Matrix *coefficients, Matrix *output)
{
    Assert(output->rows == ncomponents);
    Assert(output->columns == 1);

    Matrix feat_w;
    matrix_init(&feat_w, weights->rows);
    matrix_mult(weights, input, &feat_w);

    float8 *pfold = output->data;
    const float8 *pfw = feat_w.data;
    for (int r = 0; r < ncomponents; r++) {
        int fold = (int)*components++;
        float8 v = 1;
        while (fold-- > 0)
            v *= *pfw++;
        *pfold++ = v;
    }
    matrix_release(&feat_w);

    matrix_mult_entrywise(output, coefficients);
    matrix_mult_scalar(output, sqrt(1.0 / output->rows));
}

void matrix_mult(const Matrix *matrix1, const Matrix *matrix2, Matrix *result)
{
    Assert(matrix1 != nullptr);
    Assert(!matrix1->transposed);
    Assert(matrix2 != nullptr);
    Assert(!matrix2->transposed);
    Assert(matrix1->columns == matrix2->rows);
    Assert(matrix1->rows == result->rows);
    Assert(matrix2->columns == result->columns);

    float8 *pd = result->data;
    float8 *ps = matrix1->data;
    for (int r = 0; r < matrix1->rows; r++) {
        for (int c = 0; c < matrix2->columns; c++) {
            float8 *ps1 = ps;
            float8 *ps2 = matrix2->data + c;
            float8 sum = 0.0;
            for (int cx = 0; cx < matrix1->columns; cx++) {
                sum += *ps1 * *ps2;
                ps1++;
                ps2 += matrix2->columns;
            }
            *pd++ = sum;
        }
        ps += matrix1->columns;
    }
}

void matrix_print(const Matrix *matrix, StringInfo buf, bool full)
{
    Assert(matrix != nullptr);
    Assert(!matrix->transposed);
    const float8 *pf = matrix->data;
    appendStringInfoChar(buf, '[');
    for (int r = 0; r < matrix->rows; r++) {
        if (!full && matrix->rows > MATRIX_LIMITED_OUTPUT && r > (MATRIX_LIMITED_OUTPUT / 2) &&
            r < matrix->rows - (MATRIX_LIMITED_OUTPUT / 2)) {
            if (matrix->columns > 1)
                appendStringInfoString(buf, ",\n...");
            else
                appendStringInfoString(buf, ", ...");

            r = matrix->rows - MATRIX_LIMITED_OUTPUT / 2;
            pf = matrix->data + r * matrix->columns;
            continue;
        }

        if (matrix->columns > 1) {
            if (r > 0)
                appendStringInfoString(buf, ",\n");

            appendStringInfoChar(buf, '[');
        } else {
            if (r > 0)
                appendStringInfoString(buf, ", ");
        }
        for (int c = 0; c < matrix->columns; c++) {
            if (c > 0)
                appendStringInfoString(buf, ", ");

            appendStringInfo(buf, "%.16g", *pf++);
        }
        if (matrix->columns > 1)
            appendStringInfoChar(buf, ']');
    }
    appendStringInfoChar(buf, ']');
}

void elog_matrix(int elevel, const char *msg, const Matrix *matrix)
{
    if (is_errmodule_enable(elevel, MOD_DB4AI)) {
        StringInfoData buf;
        initStringInfo(&buf);
        matrix_print(matrix, &buf, false);
        ereport(elevel, (errmodule(MOD_DB4AI), errmsg("%s(%d,%d) = %s", msg, matrix->rows, matrix->columns, buf.data)));
        pfree(buf.data);
    }
}
