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
 *  command.h
 *
 * IDENTIFICATION
 *        src/gausskernel/catalog/model_warehouse.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DB4AI_MODEL_WAREHOUSE
#define DB4AI_MODEL_WAREHOUSE

#include "postgres.h"
#include "nodes/pg_list.h"
#include "nodes/plannodes.h"
#include "nodes/execnodes.h"

struct Hyperparameter{
    const char* name;
    Oid type;
    Datum value;
};

struct TrainingInfo{
    const char* name;
    Oid type;
    Datum value;
};

struct TrainingScore{
    const char* name;
    double value;
};

// Base class for models
struct Model{
    AlgorithmML algorithm;
    const char* model_name;
    const char* sql;
    double exec_time_secs;
    double pre_time_secs;
    int64_t processed_tuples;
    int64_t discarded_tuples;
    List* train_info;      // List of TrainingInfo
    List* hyperparameters; // List of Hyperparamters
    List* scores;          // List of TrainingScore
    Oid return_type;       // Return type of the model
    int32_t num_actual_iterations;
};

// Used by all GradientDescent variants
struct ModelGradientDescent{
    Model model;
    Datum weights;      // Float[]
    int ncategories;    // 0 for continuous
    Datum categories;   // only for categorical, an array of return_type[ncategories]
};

// Used by K-Means models
typedef struct WHCentroid {
    double objective_function = DBL_MAX;
    double avg_distance_to_centroid = DBL_MAX;
    double min_distance_to_centroid = DBL_MAX;
    double max_distance_to_centroid = DBL_MAX;
    double std_dev_distance_to_centroid = DBL_MAX;
    uint64_t cluster_size = 0ULL;
    double* coordinates = nullptr;
    uint32_t id = 0U;
} WHCentroid;

struct ModelKMeans {
    Model model;
    /*
     * the following fields are put here for convenience
     */
    uint32_t original_num_centroids = 0U;
    uint32_t actual_num_centroids = 0U;
    uint32_t dimension = 0U;
    uint32_t distance_function_id = 0U;
    uint64_t seed = 0ULL;
    WHCentroid* centroids = nullptr;
};

// Used by XGBoost
struct ModelBinary {
    Model model;
    uint64_t model_len;
    Datum model_data;    // varlena (void*)
};


// Store the model in the catalog tables
void store_model(const Model* model);

// Get the model from the catalog tables
Model* get_model(const char* model_name, bool only_model);

#endif
