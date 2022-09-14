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

static void linear_reg_gradients(GradientsConfig *cfg)
{
    Assert(cfg->features->rows > 0);

    GradientsConfigGD *cfg_gd = (GradientsConfigGD *)cfg;

    // xT * (x * w - y)
    Matrix loss;
    matrix_init(&loss, cfg->features->rows);
    matrix_mult_vector(cfg->features, cfg->weights, &loss);
    matrix_subtract(&loss, cfg_gd->dep_var);

    Matrix x_t;
    matrix_init_transpose(&x_t, cfg->features);
    matrix_mult_vector(&x_t, &loss, cfg->gradients);

    matrix_release(&x_t);
    matrix_release(&loss);
}

static double linear_reg_test(const GradientDescentState* gd_state, const Matrix *features, const Matrix *dep_var,
    const Matrix *weights, Scores *scores)
{
    Assert(features->rows > 0);

    // loss = sum((x * w - y)^2) / 2m
    Matrix errors;
    matrix_init(&errors, features->rows);
    matrix_mult_vector(features, weights, &errors);
    matrix_subtract(&errors, dep_var);
    matrix_square(&errors);
    float8 tuple_loss = matrix_get_sum(&errors) / features->rows;
    scores->mse += tuple_loss;
    tuple_loss /= 2;

    matrix_release(&errors);

    return tuple_loss;
}

static Datum linear_reg_predict(const Matrix *features, const Matrix *weights,
                                Oid return_type, void *extra_data, bool max_binary, bool* categorize)
{
    Assert(!max_binary);

    // p = x * w
    float8 prediction = matrix_dot(features, weights);
    *categorize = false;
    return float8_get_datum(return_type, prediction);
}

////////////////////////////////////////////////////////////////////////////////////////////////////

GradientDescent gd_linear_regression = {
    {
        LINEAR_REGRESSION,
        "linear_regression",
        ALGORITHM_ML_TARGET_CONTINUOUS | ALGORITHM_ML_RESCANS_DATA,
        gd_metrics_mse,
        gd_get_hyperparameters_regression,
        gd_make_hyperparameters,
        gd_update_hyperparameters,
        gd_create,
        gd_run,
        gd_end,
        gd_predict_prepare,
        gd_predict,
        gd_explain
    },
    true,
    0, // default return type
    0.0, // default feature
    0.0,
    0.0, // not necessary
    nullptr,
    gd_init_optimizer,
    nullptr,
    nullptr,
    nullptr,
    linear_reg_gradients,
    linear_reg_test,
    linear_reg_predict,
    nullptr,
};
