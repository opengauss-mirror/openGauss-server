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
 * multiclass.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/dbmind/db4ai/executor/gd/multiclass.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "db4ai/gd.h"


static bool multiclass_scan(const Matrix* features, const Matrix* dep_var)
{
    // do nothing, it is just to read all the different classes or target values
    return false; // scan all rows
}

static void multiclass_gradients(GradientsConfig *cfg)
{
    Assert(cfg->features->rows > 0);

    GradientDescent *algo = (GradientDescent *) get_algorithm_api(cfg->hyperp->classifier);
    algo->compute_gradients(cfg);
}

static double multiclass_test(const GradientDescentState* gd_state, const Matrix *features, const Matrix *dep_var,
    const Matrix *weights, Scores *scores)
{
    int n_classes = weights->rows;
    int n_features = weights->columns;
    int n_rows = dep_var->rows;

    Matrix weights_ova;
    matrix_init(&weights_ova, n_features);

    Matrix dep_var_ova;
    matrix_init(&dep_var_ova, n_rows);

    OptimizerOVA *opt = (OptimizerOVA *)gd_state->optimizer;

    Scores scores_tmp;
    double loss = 0;
    float8 *pw = weights->data;
    for (int c = 0; c < n_classes; c++) {
        scores_init(&scores_tmp);
        gd_multiclass_target(opt->algorithm, c, dep_var_ova.data, dep_var->data, n_rows);
        float8 *curr_w = matrix_swap_data(&weights_ova, pw);
        loss += opt->algorithm->test(gd_state, features, &dep_var_ova, &weights_ova, &scores_tmp);
        matrix_swap_data(&weights_ova, curr_w);
        pw += n_features;
    }

    matrix_release(&weights_ova);
    matrix_release(&dep_var_ova);
    return loss;
}

static Datum multiclass_predict(const Matrix *features, const Matrix *weights,
                                Oid return_type, void *extra_data, bool max_binary, bool* categorize)
{
    Assert(extra_data != nullptr);
    Assert(!max_binary);

    AlgorithmML algo = *(AlgorithmML*)extra_data;
    GradientDescent *palgo = (GradientDescent *) get_algorithm_api(algo);

    int n_classes = weights->rows;
    int n_features = weights->columns;

    Matrix weights_ova;
    matrix_init(&weights_ova, n_features);

    int cat = 0;
    double dmax = 0;
    float8 *pw = weights->data;
    for (int c = 0; c < n_classes; c++) {
        float8 *curr_w = matrix_swap_data(&weights_ova, pw);
        double r = DatumGetFloat4(palgo->predict(features, &weights_ova, return_type, nullptr, true, categorize));
        if (r > dmax) {
            dmax = r;
            cat = c;
        }
        matrix_swap_data(&weights_ova, curr_w);
        pw += n_features;
    }

    matrix_release(&weights_ova);

    *categorize = true;
    return Float4GetDatum(cat);
}

static void *multiclass_get_extra_data(GradientDescentState *gd_state, int *size)
{
    static AlgorithmML ml;
    *size = sizeof(AlgorithmML);
    return &ml;
}

#define GD_NUM_CLASSIFIERS   2

static const char* gd_classifiers_str[GD_NUM_CLASSIFIERS] = {
    GD_SVM_CLASSIFICATION_NAME,
    GD_LOGISTIC_REGRESSION_NAME
};

static AlgorithmML gd_classifiers[GD_NUM_CLASSIFIERS] {
    SVM_CLASSIFICATION,
    LOGISTIC_REGRESSION,
};

static const char *classifier_getter(void *classifier)
{
    AlgorithmML algo = *static_cast<AlgorithmML *>(classifier);
    for (int c = 0; c < GD_NUM_CLASSIFIERS; c++) {
        if (gd_classifiers[c] == algo)
            return gd_classifiers_str[c];
    }

    ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("Invalid classifier %d", algo)));
    return NULL;
}

static void classifier_setter(const char *str, void *algorithm_ml)
{
    for (int c = 0; c < GD_NUM_CLASSIFIERS; c++) {
        if (strcmp(str, gd_classifiers_str[c]) == 0) {
            *static_cast<AlgorithmML *>(algorithm_ml) = gd_classifiers[c];
            return;
        }
    }

    ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("Invalid classifier '%s'", str)));
}

static HyperparameterDefinition multiclass_hyperparameter_definitions[] = {
    GD_HYPERPARAMETERS_SVM_CLASSIFICATION
    HYPERPARAMETER_ENUM("classifier", GD_SVM_CLASSIFICATION_NAME,
                        gd_classifiers_str, GD_NUM_CLASSIFIERS,
                        classifier_getter, classifier_setter,
                        HyperparametersGD, classifier,
                        HP_AUTOML_ENUM()),
};

static const HyperparameterDefinition* gd_get_hyperparameters_multiclass(AlgorithmAPI *self, int *definitions_size)
{
    Assert(definitions_size != nullptr);
    *definitions_size = sizeof(multiclass_hyperparameter_definitions) / sizeof(HyperparameterDefinition);
    return multiclass_hyperparameter_definitions;
}

KernelTransformer* multiclass_init_kernel(int features, HyperparametersGD *hyperp)
{
    GradientDescent *algo = (GradientDescent *) get_algorithm_api(hyperp->classifier);
    if (algo->prepare_kernel != nullptr)
        return algo->prepare_kernel(features, hyperp);

    return nullptr;
}

GradientDescent gd_multiclass = {
    {
        MULTICLASS,
        "multiclass",
        ALGORITHM_ML_TARGET_MULTICLASS | ALGORITHM_ML_RESCANS_DATA,
        gd_metrics_accuracy,
        gd_get_hyperparameters_multiclass,
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
    0.0, // not needed
    0.0, // not needed
    multiclass_scan,
    gd_init_optimizer_ova,
    multiclass_init_kernel,
    nullptr,
    nullptr,
    multiclass_gradients,
    multiclass_test,
    multiclass_predict,
    multiclass_get_extra_data,
};
