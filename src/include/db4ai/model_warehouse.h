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
 *        src/include/db4ai/model_warehouse.h
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
    bool open_group;
    bool close_group;
};

struct TrainingScore{
    const char* name;
    double value;
};

// The model structure is mapped to the gs_model_wareshouse table that contains:
// - model_name             : unique model name provided by the user
// - model_owner            : user id of model owner (automatic)
// - create_time            : timestamp when model is stored (automatic)
// - processedtuples        : number of tuples processed
// - discardedtuples        : number of tuples discarded
// - pre_process_time       : preprocessing time in seconds
// - exec_time              : trainign time in seconds
// - iterations             : number of iterations
// - outputtype             : oid type of prediction
// - modeltype              : algorithm name
// - query                  : CREATE MODEL SQL statement
// - modeldata              : optional binary data of the trained model
// - weight                 : array of weights for gradient descent, DEPRECATED
// - hyperparametersnames   : hyperparameters
// - hyperparametersvalues
// - hyperparametersoids
// - coefnames              : extra training info, DEPRECATED
// - coefvalues             : DEPRECATED
// - coefoids               : DEPRECATED
// - trainingscorename      : scores, DEPRECATED
// - trainingscorevalue     : DEPRECATED
// - model_describe         : extra description, DEPRECATED

// Serialized model woth a content only known by the algorithm
typedef enum SerializedModelVersion {
    DB4AI_MODEL_UNDEFINED   = -1,
    DB4AI_MODEL_V00         = 0,
    DB4AI_MODEL_V01         = 1,
    DB4AI_MODEL_INVALID     = 2 // and above
} SerializedModelVersion;

typedef struct SerializedModel {
    SerializedModelVersion version;
    void *raw_data;
    Size size;
} SerializedModel;

// Base class for models
struct Model{
    // header, filled by the caller
    MemoryContext memory_context; // Memory context to allocate all Model fields
    AlgorithmML algorithm;
    const char* model_name;
    const char* sql;
    List* hyperparameters;      // List of Hyperparamters
    // model data filled by the algorithm
    int status;                 // ERRCODE_SUCCESSFUL_COMPLETION (0) or the error code
    Oid return_type;            // Return type of the model for prediction
    double pre_time_secs;       // preprocessing time
    double exec_time_secs;      // total training time
    int64_t processed_tuples;
    int64_t discarded_tuples;
    int32_t num_actual_iterations;
    List* scores;               // List of TrainingScore
    SerializedModel data;       // private model data

    // TODO_DB4AI_API: DEPRECATED
    List* train_info;      // List of TrainingInfo
    Datum weights;      // optional, float[]
    Datum model_data;   // optional, varlena (void*)
};

struct ModelGradientDescent{
    Model model;
    Datum weights;      // Float[]
    int ncategories;    // 0 for continuous
    Datum categories;   // only for categorical, an array of return_type[ncategories]
};

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

// Store the model in the catalog tables
void store_model(const Model* model);

// Get the model from the catalog tables
const Model* get_model(const char* model_name, bool only_model);

// Dump model to log
void elog_model(int level, const Model *model);

#endif
