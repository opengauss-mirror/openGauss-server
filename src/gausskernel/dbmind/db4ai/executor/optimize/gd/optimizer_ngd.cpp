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
 *  optimizer_ngd.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/dbmind/db4ai/executor/gd/optimizer_ngd.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "db4ai/gd.h"

// ////////////////////////////////////////////////////////////////////////
// ngd: normalized gradient descent optimizer
//
// An adaptation of NG algorithm from:
//   Ross, Stéphane, Paul Mineiro, and John Langford.
//   "Normalized online learning." arXiv preprint arXiv:1305.6646 (2013).


typedef struct OptimizerNormalize {
    OptimizerGD opt;
    double learning_rate;
    bool learn; // only first iteration
    double scale_rate;
    Matrix scale_gradients;
} OptimizerNormalize;

static void opt_ngd_end_iteration(OptimizerGD *optimizer)
{
    OptimizerNormalize *opt = (OptimizerNormalize *)optimizer;

    // decay the learning rate with decay^iterations
    opt->learning_rate *= optimizer->hyperp->decay;

    // be sure that learns how to normalize only in the first iteration
    opt->learn = false;
}

static void opt_ngd_update_batch(OptimizerGD *optimizer, const Matrix *features, const Matrix *dep_var)
{
    OptimizerNormalize *opt = (OptimizerNormalize *)optimizer;
    GradientDescent *gd_algo = (GradientDescent*)opt->opt.gd_state->tms.algorithm;

    if (opt->learn) {
        Assert(features->columns == opt->scale_gradients.rows);

        float8 *pf = features->data;
        for (int r = 0; r < features->rows; r++) {
            float8 *pw = optimizer->weights.data;
            float8 *ps = opt->scale_gradients.data;
            for (int c = 0; c < features->columns; c++) {
                float8 qx = *pf++;
                qx *= qx;
                if (qx > *ps) {
                    // update weights and scaling of gradients
                    *pw *= *ps / qx;
                    *ps = qx;
                }
                if (*ps > 0) {
                    // update scale rate
                    opt->scale_rate += qx / *ps;
                }
                ps++;
                pw++;
            }
        }
    }

    // clear gradients of the batch
    matrix_zeroes(&optimizer->gradients);

    GradientsConfigGD cfg;
    cfg.hdr.hyperp = optimizer->hyperp;
    cfg.hdr.features = features;
    cfg.hdr.weights = &optimizer->weights;
    cfg.hdr.gradients = &optimizer->gradients;
    cfg.dep_var = dep_var;
    gd_algo->compute_gradients(&cfg.hdr);

    elog_matrix(DEBUG1, "optimizer ngd: gradients", &optimizer->gradients);

    // normalize gradients
    float8 *pg = optimizer->gradients.data;
    float8 *ps = opt->scale_gradients.data;
    for (int r = 0; r < opt->scale_gradients.rows; r++) {
        float8 s = 0.0;
        if (*ps > 0)
            s = (1.0 / opt->scale_rate) / *ps;

        *pg *= s;

        ps++;
        pg++;
    }

    // add gradients to the model: weight -= alpha * scale_rate * gradients * scale_gradients
    // do not divide by the number of rows like in a simple minibatch
    matrix_mult_scalar(&optimizer->gradients, opt->learning_rate);
    matrix_subtract(&optimizer->weights, &optimizer->gradients);

    elog_matrix(DEBUG1, "optimizer ngd: weights", &optimizer->weights);
}

static void opt_ngd_release(OptimizerGD *optimizer)
{
    pfree(optimizer);
}

OptimizerGD *gd_init_optimizer_ngd(const GradientDescentState *gd_state, HyperparametersGD *hyperp)
{
    OptimizerNormalize *opt = (OptimizerNormalize *)palloc0(sizeof(OptimizerNormalize));
    opt->opt.hyperp = hyperp;
    opt->opt.start_iteration = nullptr;
    opt->opt.end_iteration = opt_ngd_end_iteration;
    opt->opt.update_batch = opt_ngd_update_batch;
    opt->opt.release = opt_ngd_release;
    opt->opt.finalize = nullptr;
    opt->opt.gd_state = gd_state;
    opt->learning_rate = hyperp->learning_rate;
    opt->learn = true;
    opt->scale_rate = 0.0;
    matrix_init(&opt->scale_gradients, gd_state->n_features);
    matrix_init(&opt->opt.weights, gd_state->n_features);
    matrix_init(&opt->opt.gradients, gd_state->n_features);
    return &opt->opt;
}
