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
 *  aifuncs.h
 *
 * IDENTIFICATION
 *        src/include/db4ai/aifuncs.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DB4AI_AIFUNCS_H
#define DB4AI_AIFUNCS_H

#include "db4ai/hyperparameter_validation.h"
#include "db4ai/kmeans.h"
#include "db4ai/xgboost.h"
#include "nodes/plannodes.h"

#define ARRAY_LENGTH(x) sizeof(x) / sizeof((x)[0])
#define ASSERT_ELEMENTS_ENUM_TO_STR(strings, max_enum) \
    static_assert((ARRAY_LENGTH(strings)-1) == max_enum, "Mismatch string to enum conversion. Check missing string values");


// Generic function to convert an enum to a string value
template <class Enum, const char *values[], int32_t values_size>
const char *enum_to_string(Enum x)
{
    if (static_cast<int32_t>(x) > values_size) return NULL;
    return values[static_cast<int32_t>(x)];
}

// Generic function to get the string value for an enumeration. Caller can define the
// error behavior when the string does not match. Setting error_on_mismatch stops the execution with
// an ERROR. Otherwise, the last value of the enum is returned
template <class Enum, const char *values[], int32_t values_size, bool include_last_on_error = true>
Enum string_to_enum(const char *str, const char *error_msg = NULL, bool error_on_mismatch = true)
{
    for (uint i = 0; i < values_size; i++) {
        if (0 == strcmp(values[i], str)) {
            return static_cast<Enum>(i);
        }
    }
    if (error_on_mismatch) {
        StringInfo s = makeStringInfo();
        int32_t printable_values = include_last_on_error ? values_size : values_size - 1;
        for (int32_t i = 0; i < printable_values; i++) {
            if (i != 0) appendStringInfoString(s, ", ");
            appendStringInfoString(s, enum_to_string<Enum, values, values_size>(static_cast<Enum>(i)));
        }
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("%s. Valid values are: %s", error_msg == NULL ? "Error" : error_msg, s->data)));
    }
    return static_cast<Enum>(values_size - 1);
}

///////////////////////////////////////////////////////////////////////////////

extern const char *algorithm_ml_str[];
extern const int32_t algorithm_ml_str_size;

const char *algorithm_ml_to_string(AlgorithmML x);
AlgorithmML get_algorithm_ml(const char *str);

///////////////////////////////////////////////////////////////////////////////

extern const char *prediction_type_str[];
extern const int32_t prediction_type_str_size;

const char* prediction_type_to_string(PredictionType x);

///////////////////////////////////////////////////////////////////////////////

extern const char *kmeans_distance_functions_str[];
extern const int32_t kmeans_distance_functions_str_size;

const char *kmeans_distance_to_string(DistanceFunction x);
DistanceFunction get_kmeans_distance(const char *str);

inline const char *kmeans_distance_function_getter(void *x)
{
    return kmeans_distance_to_string(*static_cast<DistanceFunction *>(x));
}


inline void kmeans_distance_function_setter(const char *str, void *x)
{
    *static_cast<DistanceFunction *>(x) = get_kmeans_distance(str);
}

///////////////////////////////////////////////////////////////////////////////

extern const char *kmeans_seeding_str[];
extern const int32_t kmeans_seeding_str_size;

const char *kmean_seeding_to_string(SeedingFunction x);
SeedingFunction get_kmeans_seeding(const char *str);

inline const char *kmeans_seeding_getter(void *x)
{
    return kmean_seeding_to_string(*static_cast<SeedingFunction *>(x));
}

inline void kmeans_seeding_setter(const char *str, void *x)
{
    *static_cast<SeedingFunction *>(x) = get_kmeans_seeding(str);
}

///////////////////////////////////////////////////////////////////////////////
extern const char *metric_ml_str[];
extern const int32_t metric_ml_str_size;

const char *metric_ml_to_string(MetricML x);
MetricML get_metric_ml(const char *str);


inline bool metric_ml_is_maximize(MetricML metric)
{
    switch (metric) {
        case METRIC_ML_ACCURACY:
        case METRIC_ML_F1:
        case METRIC_ML_PRECISION:
        case METRIC_ML_RECALL:
        case METRIC_ML_AUC:
        case METRIC_ML_AUC_PR:
        case METRIC_ML_MAP:
            return true;

        case METRIC_ML_LOSS:
        case METRIC_ML_MSE:
        case METRIC_ML_DISTANCE_L1:
        case METRIC_ML_DISTANCE_L2:
        case METRIC_ML_DISTANCE_L2_SQUARED:
        case METRIC_ML_DISTANCE_L_INF:
        case METRIC_ML_RMSE:
        case METRIC_ML_RMSLE:
        case METRIC_ML_MAE:
            return false;
        default:
            break;
    }
    Assert(false);
    return false;
}


inline double metric_ml_worst_score(MetricML metric)
{
    return metric_ml_is_maximize(metric) ? -DBL_MAX : DBL_MAX;
}


///////////////////////////////////////////////////////////////////////////////

bool is_supervised(AlgorithmML algorithm);

#endif  // DB4AI_AIFUNCS_H
