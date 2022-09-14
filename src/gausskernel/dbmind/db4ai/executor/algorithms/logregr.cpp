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

static void logreg_gradients(GradientsConfig *cfg)
{
    Assert(cfg->features->rows > 0);

    GradientsConfigGD *cfg_gd = (GradientsConfigGD *)cfg;

    // xT * ((1.0 / (1.0 + exp(-x*w))) - y)
    Matrix sigma;
    matrix_init(&sigma, cfg->features->rows);
    matrix_mult_vector(cfg->features, cfg->weights, &sigma);
    matrix_sigmoid(&sigma);
    matrix_subtract(&sigma, cfg_gd->dep_var);

    Matrix x_t;
    matrix_init_transpose(&x_t, cfg->features);
    matrix_mult_vector(&x_t, &sigma, cfg->gradients);

    matrix_release(&x_t);
    matrix_release(&sigma);
}

static double logreg_test(const GradientDescentState* gd_state, const Matrix *features, const Matrix *dep_var,
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
    float8 tuple_loss = matrix_get_sum(&cost1) / features->rows;
    matrix_release(&cost2);
    matrix_release(&cost1);

    matrix_release(&predictions);
    return tuple_loss;
}

static Datum logreg_predict(const Matrix *features, const Matrix *weights,
                            Oid return_type, void *extra_data, bool max_binary, bool* categorize)
{
    // p = 1.0 + exp(-x*w)
    float8 r = matrix_dot(features, weights);
    r = 1.0 / (1.0 + exp(-r));
    if (!max_binary)
        r = (r < 0.5 ? 0.0 : 1.0);
    *categorize = true;
    return Float4GetDatum(r);
}

/////////////////////////////////////////////////////////////////////

// Used by linear regression and logistic regression
static HyperparameterDefinition regression_hyperparameter_definitions[] = {
    GD_HYPERPARAMETERS_SUPERVISED
};

const HyperparameterDefinition* gd_get_hyperparameters_regression(AlgorithmAPI *self, int *definitions_size)
{
    Assert(definitions_size != nullptr);
    *definitions_size = sizeof(regression_hyperparameter_definitions) / sizeof(HyperparameterDefinition);
    return regression_hyperparameter_definitions;
}

GradientDescent gd_logistic_regression = {
    {
        LOGISTIC_REGRESSION,
        GD_LOGISTIC_REGRESSION_NAME,
        ALGORITHM_ML_DEFAULT | ALGORITHM_ML_RESCANS_DATA,
        gd_metrics_accuracy,
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
    1.0,
    nullptr,
    gd_init_optimizer,
    nullptr,
    nullptr,
    nullptr,
    logreg_gradients,
    logreg_test,
    logreg_predict,
    nullptr,
};
