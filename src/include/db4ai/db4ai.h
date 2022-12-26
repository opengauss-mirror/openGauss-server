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
 *  db4ai.h
 *
 * IDENTIFICATION
 *        src/include/db4ai/db4ai.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DB4AI_H
#define DB4AI_H

typedef enum MetricML{
    // classifier
    METRIC_ML_ACCURACY,
    METRIC_ML_F1,
    METRIC_ML_PRECISION,
    METRIC_ML_RECALL,
    // General purpouse
    METRIC_ML_LOSS,
    // regression
    METRIC_ML_MSE,
    // distance
    METRIC_ML_DISTANCE_L1,
    METRIC_ML_DISTANCE_L2,
    METRIC_ML_DISTANCE_L2_SQUARED,
    METRIC_ML_DISTANCE_L_INF,
    // xgboost
    METRIC_ML_AUC,    // area under curve
    METRIC_ML_AUC_PR, // area under pr curve
    METRIC_ML_MAP,    // mean avg. precision
    METRIC_ML_RMSE,   // root mean square err
    METRIC_ML_RMSLE,  // root mean square log err
    METRIC_ML_MAE,    // mean abs. value
    METRIC_ML_INVALID,
} MetricML;

typedef enum {
   TYPE_BOOL = 0,
   TYPE_BYTEA,
   TYPE_INT32,
   TYPE_INT64,
   TYPE_FLOAT32,
   TYPE_FLOAT64,
   TYPE_FLOAT64ARRAY,
   TYPE_NUMERIC,
   TYPE_TEXT,
   TYPE_VARCHAR,
   TYPE_INVALID_PREDICTION,
} PredictionType;

typedef enum {
    // algorithms implememted through the generic API
    LOGISTIC_REGRESSION = 0,
    SVM_CLASSIFICATION,
    LINEAR_REGRESSION,
    PCA,
    KMEANS,
    XG_REG_LOGISTIC,
    XG_BIN_LOGISTIC,
    XG_REG_SQE, // regression with squared error
    XG_REG_GAMMA,
    MULTICLASS,
    BAYES_NET_INTERNAL,
    // for internal use
    INVALID_ALGORITHM_ML,


} AlgorithmML;

#endif  // DB4AI_H


