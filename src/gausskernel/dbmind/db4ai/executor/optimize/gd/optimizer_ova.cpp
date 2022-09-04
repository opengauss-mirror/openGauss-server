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
* optimizer_ova.cpp
*        Optimizer Implementation of One-to-Many Classification
*
* IDENTIFICATION
*        src/gausskernel/dbmind/db4ai/executor/gd/optimizer_ova.cpp
*
* ---------------------------------------------------------------------------------------
*/

#include "db4ai/gd.h"

// ova: one-vs-all multiclass optimizer
static void opt_gd_ova_start_iteration(OptimizerGD *optimizer)
{
    OptimizerOVA *opt = (OptimizerOVA *)optimizer;
    if (opt->parent->start_iteration != nullptr)
        opt->parent->start_iteration(opt->parent);
}

static void opt_gd_ova_end_iteration(OptimizerGD *optimizer)
{
    OptimizerOVA *opt = (OptimizerOVA *)optimizer;
    if (opt->parent->end_iteration != nullptr)
        opt->parent->end_iteration(opt->parent);
}

static void opt_gd_ova_update_batch(OptimizerGD *optimizer, const Matrix *features, const Matrix *dep_var)
{
    OptimizerOVA *opt = (OptimizerOVA *)optimizer;

    // one-vs-all computes one model for each different class, assuming all other values are false
    int n_classes = opt->opt.gd_state->num_classes;
    int n_rows = dep_var->rows;
    int n_features = optimizer->weights.columns;

    Matrix dep_var_ova;
    matrix_init(&dep_var_ova, n_rows);
    
    float8 *pw = optimizer->weights.data;
    for (int c = 0; c < n_classes; c++) {
        gd_multiclass_target(opt->algorithm, c, dep_var_ova.data, dep_var->data, n_rows);
        float8 *curr_w = matrix_swap_data(&opt->parent->weights, pw);
        opt->parent->update_batch(opt->parent, features, &dep_var_ova);
        matrix_swap_data(&opt->parent->weights, curr_w);
        pw += n_features;
    }
    matrix_release(&dep_var_ova);

    elog_matrix(DEBUG1, "optimizer ova: weights", &optimizer->weights);
}

static void opt_gd_ova_finalize(OptimizerGD *optimizer)
{
    OptimizerOVA *opt = (OptimizerOVA *)optimizer;
    if (opt->parent->finalize != nullptr)
        opt->parent->finalize(opt->parent);
}

static void opt_gd_ova_release(OptimizerGD *optimizer)
{
    OptimizerOVA *opt = (OptimizerOVA *)optimizer;
    pfree(opt->parent);
    pfree(optimizer);
}

OptimizerGD *gd_init_optimizer_ova(const GradientDescentState *gd_state, HyperparametersGD *hyperp)
{
    if (hyperp->classifier != SVM_CLASSIFICATION && hyperp->classifier != LOGISTIC_REGRESSION)
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_OPERATION),
                        errmsg("OVA is only supported for gradient descent binary classification")));

    // now we use the parent state to
    OptimizerOVA *opt = (OptimizerOVA *)palloc0(sizeof(OptimizerOVA));
    opt->opt.hyperp = hyperp;
    opt->opt.start_iteration = opt_gd_ova_start_iteration;
    opt->opt.end_iteration = opt_gd_ova_end_iteration;
    opt->opt.update_batch = opt_gd_ova_update_batch;
    opt->opt.release = opt_gd_ova_release;
    opt->opt.finalize = opt_gd_ova_finalize;
    opt->opt.gd_state = gd_state;
    opt->parent = gd_init_optimizer(gd_state, hyperp);
    opt->algorithm = (GradientDescent*) get_algorithm_api(hyperp->classifier);
    matrix_init(&opt->opt.weights, gd_state->num_classes, gd_state->n_features);
    matrix_init(&opt->opt.gradients, 1); // this is not used
    return &opt->opt;
}

