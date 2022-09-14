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
 *  db4ai_api.h
 *
 * IDENTIFICATION
 *        src/include/db4ai/db4ai_api.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DB4AI_API_H
#define DB4AI_API_H

#include "nodes/execnodes.h"
#include "commands/explain.h"
#include "db4ai/hyperparameter_validation.h"
#include "db4ai/predict_by.h"

// generic flags, can be OR'ed
enum {
    ALGORITHM_ML_DEFAULT            = 0x00000000,   // default is supervised / binary
    ALGORITHM_ML_UNSUPERVISED       = 0x00000001,
    ALGORITHM_ML_RESCANS_DATA       = 0x00000002,   // set when algorithm reads more than once the subplan
    // for supervised
    ALGORITHM_ML_TARGET_MULTICLASS  = 0x00010000,   // categorical with more than two categories
    ALGORITHM_ML_TARGET_CONTINUOUS  = 0x00020000,   // not categorical (and not binary)
};

// this is just a base structure to be extended by the ML algorithms
typedef struct ModelHyperparameters {
} ModelHyperparameters;

/*
 * Generic ML API for training & prediction.
 *
 * Algorithms following this interface may be used in two different scenarios:
 *  - training embedded into query plan generated from a SQL statement, where
 *    the tuples are obtained transparently from the query subplan
 *  - direct training in C, where the tuples are provided by the user
 *
 * All callbacks receive a pointer to the AlgorithmAPI where they belong
 * in order to be able to call automatically to other callbacks or to extend
 * the api for specific purposes, such as multiple ML algorithms implemented
 * by a single API (e.g. gradient descent).
 */
typedef struct AlgorithmAPI {
    // algorithm description
    AlgorithmML algorithm;
    const char* name;
    int flags;              // OR'ed ALGORITHM_ML_xxx flags

    //////////////////////////////////////////////////////////////////////
    // callbaks used to populate model warehouse when database is created
    
    /*
     * Returns the metrics that can be computed by this algorithm
     */
    MetricML* (*get_metrics)(
        struct AlgorithmAPI *self,
        int *num_metrics // output, number of metrics returned
        );

    /* Returns the definition of the supported hyperparameters.
     *
     * The offsets of the fields must correspond to the structure
     * returned by the <make_hyperparameters> callback
     */
    const HyperparameterDefinition* (*get_hyperparameters_definitions)(
        struct AlgorithmAPI *self,
        int *num_hyperp // output, number of definitions returned
        );
    
    //////////////////////////////////////////////////////////////////////
    // callbacks used by CREATE MODEL interface
    
    /*
     * Creates an structure to store the hyperparameters
     */
    ModelHyperparameters* (*make_hyperparameters)(
        struct AlgorithmAPI *self
        );
    
    /*
     * Updates the input hyperparameters with extra computations or initializations (e.g. seed).
     *
     * Tiis callback it is optional, a NULL pointer disables it
     */
    void (*update_hyperparameters)(
        struct AlgorithmAPI *self,
        ModelHyperparameters *hyperparameters
        );

    //////////////////////////////////////////////////////////////////////
    // callbacks used into the query runtime operator
    
    /*
     * Create an initializes new training state.
     *
     * This training state has to be completely agnostic of query subplans
     * or executor nodes because it will be used also for direct training.
     */
    TrainModelState* (*create)(
        struct AlgorithmAPI *self,
        const TrainModel* configuration
        );
    
    /*
     * Trains all configurations at a time and return all trained models
     * in the same order as the hyperparameter configurations.
     *
     * Each model has a status field that denotes if it has terminated
     * successfully or not.
     *
     * Tuples are fetched using the callback provided into the state,
     * which loads the tuple data into the state itself. For supervised
     * algorithms, the target (label) column is always the first.
     *
     * Multiple iterations of the data are possible by requesting a
     * rescan to the tuple fetch callback. The 
     */
    void (*run)(
        struct AlgorithmAPI *self,
        TrainModelState *state,
        Model ** // output, returns all result models
        );

    /*
     * Ends the training. The state is freed by the caller.
     */
    void (*end)(
        struct AlgorithmAPI *self,
        TrainModelState *state
        );
	
    //////////////////////////////////////////////////////////////////////
    // used by PREDICT BY
    
    /*
     * Prepares a model predictor using a trained model
     */
    ModelPredictor (*prepare_predict)(
        struct AlgorithmAPI *self,
        const SerializedModel *model,
        Oid return_type
        );
    
    /*
     * Predicts using a trained model.
     *
     * The input tuple to use for prediction must have the same number
     * of attributes and in the same order as those used for training
     * except the target in the case of supervised.
     *
     * The predicted value is of the result type specified into the model.
     */
    Datum (*predict)(
        struct AlgorithmAPI *self,
        ModelPredictor predictor,
        Datum *values,
        bool *isnull,
        Oid *types,
        int num_fields
        );
    
    //////////////////////////////////////////////////////////////////////
    // serialization

    /*
     * Deserializes a model and returns a list of TrainingInfo
     */
    List* (*explain)(
        struct AlgorithmAPI *self,
        const SerializedModel *model,
        Oid return_type
        );
    
} AlgorithmAPI;

// return the API for a given algorithm
AlgorithmAPI *get_algorithm_api(AlgorithmML algorithm);

// direct ML interface, use only these methods and never direct calls to the API
Model* model_fit(const char* name,
                AlgorithmML algorithm,
                // optional hyperparameter values provided by the caller
                const Hyperparameter *hyperparameters,
                int num_hyperparameters,
                // tuple description
                Oid *typid,
                bool *typbyval,
                int16 *typlen,
                int num_columns,
                // callbacks to fetch data
                callback_ml_fetch fetch,
                callback_ml_rescan rescan,
                void* callback_data // optional, user defined structure for fetch/rescan
                );

// saves a trained model into the model warehouse
void model_store(const Model *model);

// returns a trained model stored into the model warehouse
const Model *model_load(const char *model_name);

// delete a trained model from the model warehouse
void model_drop(const char *model_name);

// prepares a predictor for a trained model
ModelPredictor model_prepare_predict(const Model* model);

// predicts using a trained model
Datum model_predict(ModelPredictor predictor,
                    Datum *values,
                    bool *isnull,
                    Oid *typid,
                    int num_columns
                    );

const char* model_explain(const Model *model, ExplainFormat format);

#endif  // DB4AI_H


