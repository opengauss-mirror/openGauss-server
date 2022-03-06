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
 *  svm.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/dbmind/db4ai/executor/gd/svm.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "db4ai/gd.h"
#include "db4ai/kernel.h"

static void svmc_gradients(GradientsConfig *cfg)
{
    Assert(cfg->features->rows > 0);

    GradientsConfigGD *cfg_gd = (GradientsConfigGD *)cfg;

    // distances = 1 - y * (x * w)
    Matrix distance;
    matrix_init(&distance, cfg->features->rows);
    matrix_mult_vector(cfg->features, cfg->weights, &distance);
    matrix_mult_entrywise(&distance, cfg_gd->dep_var);
    matrix_complement(&distance);

    Assert(distance.rows == cfg_gd->dep_var->rows);
    Assert(distance.columns == 1);

    const float8 *pf = cfg->features->data;
    const float8 *py = cfg_gd->dep_var->data;
    const float8 *pd = distance.data;
    float8 *pg = cfg->gradients->data;
    for (int r = 0; r < distance.rows; r++) {
        float8 y = *py++;
        float8 d = *pd++;
        if (d > 0) {
            for (int f = 0; f < cfg->features->columns; f++)
                pg[f] -= y * pf[f];
        }
        pf += cfg->features->columns;
    }
    matrix_release(&distance);

    matrix_mult_scalar(cfg->gradients, cfg->hyperp->lambda * 2.0);
}

static double svmc_test(const GradientDescentState* gd_state, const Matrix *features, const Matrix *dep_var,
    const Matrix *weights, Scores *scores)
{
    Assert(features->rows > 0);

    Matrix distances;
    Matrix predictions;
    matrix_init(&distances, features->rows);
    matrix_mult_vector(features, weights, &distances);

    matrix_init_clone(&predictions, &distances);
    matrix_binary(&predictions, FLT_MIN, -1.0, 1.0);
    matrix_relevance(&predictions, dep_var, scores, 1.0);
    matrix_release(&predictions);

    // cost = (1 - y * x * w)
    matrix_mult_entrywise(&distances, dep_var);
    matrix_complement(&distances);
    matrix_positive(&distances);

    float8 tuple_loss = matrix_get_sum(&distances) / features->rows;

    matrix_release(&distances);
    return tuple_loss;
}

static Datum svmc_predict(const Matrix *features, const Matrix *weights, Oid return_type, void *extra_data,
                          bool max_binary, bool *categorize)
{
    double r = matrix_dot(features, weights);
    if (!max_binary)
        r = (r <= 0 ? -1.0 : 1.0);
    *categorize = true;
    return Float4GetDatum(r);
}

///////////////////////////////////////////////////////////////////////////////////////////////

const char* svm_kernel_str[SVM_NUM_KERNELS] = {
    "linear",
    "gaussian",
    "polynomial"
};

const char *svm_kernel_getter(void *pkernel)
{
    KernelSVM kernel = *static_cast<KernelSVM *>(pkernel);
    if (kernel < SVM_NUM_KERNELS)
        return svm_kernel_str[kernel];

    ereport(ERROR,
            (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Invalid kernel %d", kernel)));
    return NULL;
}

void svm_kernel_setter(const char *str, void *kernel)
{
    for (int c = 0; c < SVM_NUM_KERNELS; c++) {
        if (strcmp(str, svm_kernel_str[c]) == 0) {
            *static_cast<KernelSVM *>(kernel) = (KernelSVM)c;
            return;
        }
    }

    ereport(ERROR,
            (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Invalid kernel '%s'", str)));
}

static HyperparameterDefinition svmc_hyperparameter_definitions[] = {
    GD_HYPERPARAMETERS_SVM_CLASSIFICATION
};

static const HyperparameterDefinition* gd_get_hyperparameters_svmc(AlgorithmAPI *self, int *definitions_size)
{
    Assert(definitions_size != nullptr);
    *definitions_size = sizeof(svmc_hyperparameter_definitions) / sizeof(HyperparameterDefinition);
    return svmc_hyperparameter_definitions;
}

KernelTransformer* svmc_init_kernel(int features, HyperparametersGD *hyperp)
{
    KernelTransformer *kernel = nullptr;

    if (hyperp->kernel != SVM_KERNEL_LINEAR) {
        int components = hyperp->components;
        if (components == 0)
            components = Max(128, 2 * features);

        if (hyperp->kernel == SVM_KERNEL_GAUSSIAN) {
            KernelGaussian *kernel_g = (KernelGaussian *) palloc0(sizeof(KernelGaussian));
            kernel_init_gaussian(kernel_g, features, components, hyperp->gamma, hyperp->seed);
            kernel = &kernel_g->km;
        } else {
            Assert(hyperp->kernel == SVM_KERNEL_POLYNOMIAL);
            KernelPolynomial *kernel_p = (KernelPolynomial *) palloc0(sizeof(KernelPolynomial));
            kernel_init_polynomial(kernel_p, features, components, hyperp->degree, hyperp->coef0, hyperp->seed);
            kernel = &kernel_p->km;
        }
    }

    return kernel;
}

GradientDescent gd_svm_classification = {
    {
        SVM_CLASSIFICATION,
        GD_SVM_CLASSIFICATION_NAME,
        ALGORITHM_ML_DEFAULT | ALGORITHM_ML_RESCANS_DATA,
        gd_metrics_accuracy,
        gd_get_hyperparameters_svmc,
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
    -1.0,
    1.0,
    nullptr,
    gd_init_optimizer,
    svmc_init_kernel,
    nullptr,
    nullptr,
    svmc_gradients,
    svmc_test,
    svmc_predict,
    nullptr,
};

