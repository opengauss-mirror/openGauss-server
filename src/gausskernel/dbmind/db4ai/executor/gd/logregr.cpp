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
 *  logreg.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/dbmind/db4ai/executor/gd/logreg.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "db4ai/gd.h"

static void logreg_gradients(const GradientDescent *gd_node, const Matrix *features, const Matrix *dep_var,
    Matrix *weights, Matrix *gradients)
{
    Assert(features->rows > 0);

    // xT * ((1.0 / (1.0 + exp(-x*w))) - y)
    Matrix sigma;
    matrix_init(&sigma, features->rows);
    matrix_mult_vector(features, weights, &sigma);
    matrix_sigmoid(&sigma);
    matrix_subtract(&sigma, dep_var);

    Matrix x_t;
    matrix_init_transpose(&x_t, features);
    matrix_mult_vector(&x_t, &sigma, gradients);

    matrix_release(&x_t);
    matrix_release(&sigma);
}

static double logreg_test(const GradientDescent *gd_node, const Matrix *features, const Matrix *dep_var,
    const Matrix *weights, Scores *scores)
{
    Assert(features->rows > 0);

    Matrix predictions;
    Matrix cost1;
    Matrix cost2;
    Matrix classification;

    // p = 1.0 + exp(-x*w)
    matrix_init(&predictions, features->rows);
    matrix_mult_vector(features, weights, &predictions);
    matrix_sigmoid(&predictions);

    // compute relevance
    matrix_init_clone(&classification, &predictions);
    matrix_binary(&classification, 0.5, 0.0, 1.0);
    matrix_relevance(&classification, dep_var, scores, 1.0);
    matrix_release(&classification);

    // compute loss using cross-entropy
    // cost1 = y * log(p)
    matrix_init_clone(&cost1, &predictions);
    matrix_log(&cost1);
    matrix_mult_entrywise(&cost1, dep_var);

    // cost2 = (1-y) * log(1-p)
    matrix_complement(&predictions);
    matrix_log(&predictions);
    matrix_init_clone(&cost2, dep_var);
    matrix_complement(&cost2);
    matrix_mult_entrywise(&cost2, &predictions);

    // cost = sum(-cost1 - cost2) / N
    matrix_negate(&cost1);
    matrix_subtract(&cost1, &cost2);
    gd_float tuple_loss = matrix_get_sum(&cost1) / features->rows;
    matrix_release(&cost2);
    matrix_release(&cost1);

    matrix_release(&predictions);
    return tuple_loss;
}

static gd_float logreg_predict(const Matrix *features, const Matrix *weights)
{
    // p = 1.0 + exp(-x*w)
    gd_float r = matrix_dot(features, weights);
    r = 1.0 / (1.0 + exp(-r));
    return r < 0.5 ? 0.0 : 1.0;
}

GradientDescentAlgorithm gd_logistic_regression = {
    "logistic-regression",
    GD_DEPENDENT_VAR_BINARY,
    METRIC_ACCURACY | METRIC_F1 | METRIC_PRECISION | METRIC_RECALL | METRIC_LOSS,
    0.0,
    1.0,
    logreg_gradients,
    logreg_test,
    logreg_predict,
};
