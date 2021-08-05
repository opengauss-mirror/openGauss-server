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
 *  linreg.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/dbmind/db4ai/executor/gd/linreg.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "db4ai/gd.h"

static void linear_reg_gradients(const GradientDescent *gd_node, const Matrix *features, const Matrix *dep_var,
    Matrix *weights, Matrix *gradients)
{
    Assert(features->rows > 0);

    // xT * (x * w - y)
    Matrix loss;
    matrix_init(&loss, features->rows);
    matrix_mult_vector(features, weights, &loss);
    matrix_subtract(&loss, dep_var);

    Matrix x_t;
    matrix_init_transpose(&x_t, features);
    matrix_mult_vector(&x_t, &loss, gradients);

    matrix_release(&x_t);
    matrix_release(&loss);
}

static double linear_reg_test(const GradientDescent *gd_node, const Matrix *features, const Matrix *dep_var,
    const Matrix *weights, Scores *scores)
{
    Assert(features->rows > 0);

    // loss = sum((x * w - y)^2) / 2m
    Matrix errors;
    matrix_init(&errors, features->rows);
    matrix_mult_vector(features, weights, &errors);
    matrix_subtract(&errors, dep_var);
    matrix_square(&errors);
    gd_float tuple_loss = matrix_get_sum(&errors) / features->rows;
    scores->mse += tuple_loss;
    tuple_loss /= 2;

    matrix_release(&errors);

    return tuple_loss;
}

static gd_float linear_reg_predict(const Matrix *features, const Matrix *weights)
{
    // p = x * w
    return matrix_dot(features, weights);
}

GradientDescentAlgorithm gd_linear_regression = {
    "linear-regression",
    GD_DEPENDENT_VAR_CONTINUOUS,
    METRIC_MSE, // same as loss function
    0.0,
    0.0, // not necessary
    linear_reg_gradients,
    linear_reg_test,
    linear_reg_predict,
};
