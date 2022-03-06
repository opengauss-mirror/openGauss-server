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
 *  optimizer_gd.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/dbmind/db4ai/executor/gd/optimizer_gd.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "db4ai/gd.h"

// ////////////////////////////////////////////////////////////////////////
// gd: minibatch basic optimizer

typedef struct OptimizerMinibatch {
    OptimizerGD opt;
    double learning_rate;
} OptimizerMinibatch;

static void opt_gd_end_iteration(OptimizerGD *optimizer)
{
    OptimizerMinibatch *opt = (OptimizerMinibatch *)optimizer;

    // decay the learning rate with decay^iterations
    opt->learning_rate *= opt->opt.hyperp->decay;
}

static void opt_gd_update_batch(OptimizerGD *optimizer, const Matrix *features, const Matrix *dep_var)
{
    OptimizerMinibatch *opt = (OptimizerMinibatch *)optimizer;
    GradientDescent *gd_algo = (GradientDescent*)opt->opt.gd_state->tms.algorithm;

    // clear gradients of the batch
    matrix_zeroes(&optimizer->gradients);

    // update gradients
    GradientsConfigGD cfg;
    cfg.hdr.hyperp = optimizer->hyperp;
    cfg.hdr.features = features;
    cfg.hdr.weights = &optimizer->weights;
    cfg.hdr.gradients = &optimizer->gradients;
    cfg.dep_var = dep_var;
    gd_algo->compute_gradients(&cfg.hdr);

    elog_matrix(DEBUG1, "optimizer gd: gradients", &optimizer->gradients);

    // add gradients to the model: weight -= alpha * gradients * scale / N
    matrix_mult_scalar(&optimizer->gradients, opt->learning_rate / features->rows);
    matrix_subtract(&optimizer->weights, &optimizer->gradients);

    elog_matrix(DEBUG1, "optimizer gd: weights", &optimizer->weights);
}

static void opt_gd_release(OptimizerGD *optimizer)
{
    pfree(optimizer);
}

OptimizerGD *gd_init_optimizer_gd(const GradientDescentState *gd_state, HyperparametersGD *hyperp)
{
    OptimizerMinibatch *opt = (OptimizerMinibatch *)palloc0(sizeof(OptimizerMinibatch));
    opt->opt.hyperp = hyperp;
    opt->opt.start_iteration = nullptr;
    opt->opt.end_iteration = opt_gd_end_iteration;
    opt->opt.update_batch = opt_gd_update_batch;
    opt->opt.release = opt_gd_release;
    opt->opt.finalize = nullptr;
    opt->opt.gd_state = gd_state;
    opt->learning_rate = hyperp->learning_rate;
    matrix_init(&opt->opt.weights, gd_state->n_features);
    matrix_init(&opt->opt.gradients, gd_state->n_features);
    return &opt->opt;
}
