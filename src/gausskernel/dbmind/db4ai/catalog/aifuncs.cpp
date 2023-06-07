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
 * aifuncs.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/dbmind/db4ai/catalog/aifuncs.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "db4ai/aifuncs.h"
#include "db4ai/db4ai_api.h"
#include "db4ai/gd.h"
#include "db4ai/kmeans.h"
#include "db4ai/xgboost.h"
#include "db4ai/bayesnet.h"
#include "db4ai/bayes.h"

// when adding new prediction types make sure of changing the corresponding enum correctly (positions must match)
const char *prediction_types_str[] = {[TYPE_BOOL] = "Boolean",
                                      [TYPE_BYTEA] = "Binary",
                                      [TYPE_INT32] = "Int32",
                                      [TYPE_INT64] = "Int64",
                                      [TYPE_FLOAT32] = "Float32",
                                      [TYPE_FLOAT64] = "Float64",
                                      [TYPE_FLOAT64ARRAY] = "Float64[]",
                                      [TYPE_NUMERIC] = "Numeric",
                                      [TYPE_TEXT] = "Text",
                                      [TYPE_VARCHAR] = "Varchar",
                                      [TYPE_INVALID_PREDICTION] = "Invalid"};

const int32_t prediction_type_str_size = ARRAY_LENGTH(prediction_types_str);
ASSERT_ELEMENTS_ENUM_TO_STR(prediction_types_str, TYPE_INVALID_PREDICTION);

const char* prediction_type_to_string(PredictionType x)
{
    return enum_to_string<PredictionType, prediction_types_str, prediction_type_str_size>(x);
}

const char* algorithm_ml_to_string(AlgorithmML x)
{
    return get_algorithm_api(x)->name;
}

AlgorithmML get_algorithm_ml(const char *str)
{
    for (int a = 0; a < INVALID_ALGORITHM_ML; a++) {
        AlgorithmML x = (AlgorithmML)a;
        AlgorithmAPI* api = get_algorithm_api(x);
        if (strcmp(str, api->name) == 0)
            return x;
    }

    ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Invalid ML algorithm '%s'", str)));
    return INVALID_ALGORITHM_ML;
}

// when adding new algorithms make sure of changing the corresponding enum correctly (positions must match)
const char* kmeans_distance_functions_str[] = {
    [KMEANS_L1] = "L1",
    [KMEANS_L2] = "L2",
    [KMEANS_L2_SQUARED] = "L2_Squared",
    [KMEANS_LINF] = "Linf"
};
const int32_t kmeans_distance_functions_str_size = ARRAY_LENGTH(kmeans_distance_functions_str);
ASSERT_ELEMENTS_ENUM_TO_STR(kmeans_distance_functions_str, KMEANS_LINF);

const char* kmeans_distance_to_string(DistanceFunction x)
{
    return enum_to_string<DistanceFunction, kmeans_distance_functions_str,
                ARRAY_LENGTH(kmeans_distance_functions_str)>(x);
}


DistanceFunction get_kmeans_distance(const char *str)
{
    return string_to_enum<DistanceFunction, kmeans_distance_functions_str,
                ARRAY_LENGTH(kmeans_distance_functions_str), true>(str, "No known distance function");
}


///////////////////////////////////////////////////////////////////////////////

// when adding new algorithms make sure of changing the corresponding enum correctly (positions must match)
const char* kmeans_seeding_str[] = {
    [KMEANS_RANDOM_SEED] = "Random++",
    [KMEANS_BB] = "KMeans||"
};
const int32_t kmeans_seeding_str_size = ARRAY_LENGTH(kmeans_seeding_str);
ASSERT_ELEMENTS_ENUM_TO_STR(kmeans_seeding_str, KMEANS_BB);


const char* kmean_seeding_to_string(SeedingFunction x)
{
    return enum_to_string<SeedingFunction, kmeans_seeding_str, ARRAY_LENGTH(kmeans_seeding_str)>(x);
}


SeedingFunction get_kmeans_seeding(const char *str)
{
    return string_to_enum<SeedingFunction, kmeans_seeding_str, ARRAY_LENGTH(kmeans_seeding_str), true>(
        str, "No known seeding function");
}


///////////////////////////////////////////////////////////////////////////////

// when adding new algorithms make sure of changing the corresponding enum correctly (positions must match)
const char *metric_ml_str[] = {[METRIC_ML_ACCURACY] = "accuracy",
                               [METRIC_ML_F1] = "f1",
                               [METRIC_ML_PRECISION] = "precision",
                               [METRIC_ML_RECALL] = "recall",
                               [METRIC_ML_LOSS] = "loss",
                               [METRIC_ML_MSE] = "mse",
                               [METRIC_ML_DISTANCE_L1] = "l1",
                               [METRIC_ML_DISTANCE_L2] = "l2",
                               [METRIC_ML_DISTANCE_L2_SQUARED] = "l2_squared",
                               [METRIC_ML_DISTANCE_L_INF] = "l_inf",
                               [METRIC_ML_AUC] = "auc",
                               [METRIC_ML_AUC_PR] = "aucpr",
                               [METRIC_ML_MAP] = "map",
                               [METRIC_ML_RMSE] = "rmse",
                               [METRIC_ML_RMSLE] = "rmsle",
                               [METRIC_ML_MAE] = "mae",
                               [METRIC_ML_INVALID] = "metric_invalid"};
const int32_t metric_ml_str_size = ARRAY_LENGTH(metric_ml_str);
ASSERT_ELEMENTS_ENUM_TO_STR(metric_ml_str, METRIC_ML_INVALID);

const char *metric_ml_to_string(MetricML x)
{
    return enum_to_string<MetricML, metric_ml_str, ARRAY_LENGTH(metric_ml_str)>(x);
}

MetricML get_metric_ml(const char *str)
{
    return string_to_enum<MetricML, metric_ml_str, ARRAY_LENGTH(metric_ml_str), false>(str, "No known metric");
}

///////////////////////////////////////////////////////////////////////////////
// when adding new algorithms make sure of changing the corresponding position
static AlgorithmAPI* algorithm_apis[] = {
    &gd_logistic_regression.algo,
    &gd_svm_classification.algo,
    &gd_linear_regression.algo,
    &gd_pca.algo,
    &kmeans.algo,
    &xg_reg_logistic.algo,// xgboost_regression_logistic
    &xg_bin_logistic.algo,// xgboost_binary_logistic
    &xg_reg_sqe.algo,     // xgboost_regression_squarederror
    &xg_reg_gamma.algo,   // xgboost_regression_gamma
    &gd_multiclass.algo,
    &bayes_net_internal.algo,
};

AlgorithmAPI *get_algorithm_api(AlgorithmML algorithm)
{
    if (algorithm >= INVALID_ALGORITHM_ML || algorithm_apis[algorithm] == nullptr)
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("invalid ML algorithm: %d", algorithm)));
    return algorithm_apis[algorithm];
}

bool is_supervised(AlgorithmML algorithm)
{
    return ((get_algorithm_api(algorithm)->flags & ALGORITHM_ML_UNSUPERVISED) == 0);
}

