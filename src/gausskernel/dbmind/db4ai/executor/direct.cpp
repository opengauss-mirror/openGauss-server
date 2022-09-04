/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * ---------------------------------------------------------------------------------------
 *
 * direct.cpp
 *
 * IDENTIFICATION
 * src/gausskernel/dbmind/db4ai/executor/direct.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "db4ai/db4ai_api.h"
#include "catalog/gs_model.h"

Model *model_fit(const char *name, AlgorithmML algorithm, const Hyperparameter *hyperparameters, int nhyperp,
                 Oid *typid, bool *typbyval, int16 *typlen, int ncolumns, callback_ml_fetch fetch,
                 callback_ml_rescan rescan, void *callback_data)
{
    Assert(nhyperp == 0 || hyperparameters != nullptr);
    Assert(ncolumns > 0);
    Assert(typid != nullptr);
    Assert(typbyval != nullptr);
    Assert(typlen != nullptr);
    Assert(fetch != NULL);

    AlgorithmAPI *palgo = get_algorithm_api(algorithm);

    // prepare the hyperparameters
    int32_t definitions_size;
    const HyperparameterDefinition* definitions = palgo->get_hyperparameters_definitions(palgo, &definitions_size);
    ModelHyperparameters *hyperp = palgo->make_hyperparameters(palgo);
    init_hyperparameters_with_defaults(definitions, definitions_size, hyperp);
    if (nhyperp > 0)
        configure_hyperparameters(definitions, definitions_size, hyperparameters, nhyperp, hyperp);
    if (palgo->update_hyperparameters != nullptr)
        palgo->update_hyperparameters(palgo, hyperp);

    // create configuration
    TrainModel config;
    config.algorithm = algorithm;
    config.configurations = 1;
    config.hyperparameters = (const ModelHyperparameters **) &hyperp;
    config.cxt = CurrentMemoryContext;

    // create state structure
    Assert(palgo->create != nullptr);
    TrainModelState *pstate = palgo->create(palgo, &config);
    pstate->config = &config;
    pstate->algorithm = palgo;
    pstate->finished = 0;
    pstate->fetch = fetch;
    pstate->rescan = rescan;
    pstate->callback_data = callback_data;
    pstate->row_allocated = false;
    pstate->tuple.ncolumns = ncolumns;
    pstate->tuple.typid = typid;
    pstate->tuple.typbyval = typbyval;
    pstate->tuple.typlen = typlen;

    // fit, with a single configuration only a single Model is expected
    Model *model = (Model *)palloc0(sizeof(Model));
    model->memory_context = CurrentMemoryContext;
    model->algorithm = algorithm;
    model->model_name = name;
    model->sql = "DIRECT ML API";
    model->hyperparameters =
        prepare_model_hyperparameters(definitions, definitions_size, hyperp, model->memory_context);

    Assert(palgo->run != nullptr);
    palgo->run(palgo, pstate, &model);
    
    // done
    Assert(palgo->end != nullptr);
    palgo->end(palgo, pstate);
    pfree(pstate);
    
    return model;
}

struct DirectModelPredictor {
    AlgorithmAPI *palgo;
    ModelPredictor predictor;
};

ModelPredictor model_prepare_predict(const Model* model)
{
    DirectModelPredictor *pred = (DirectModelPredictor*) palloc(sizeof(DirectModelPredictor));
    pred->palgo = get_algorithm_api(model->algorithm);
    pred->predictor = pred->palgo->prepare_predict(pred->palgo, &model->data, model->return_type);
    return (ModelPredictor)pred;
}

Datum model_predict(ModelPredictor predictor, Datum *values, bool *isnull, Oid *typid, int num_columns)
{
    DirectModelPredictor *pred = (DirectModelPredictor*) predictor;
    return pred->palgo->predict(pred->palgo, pred->predictor, values, isnull, typid, num_columns);
}

void model_store(const Model *model) {
    store_model(model);
}

const Model *model_load(const char *model_name)
{
    return get_model(model_name, false);
}

void model_drop(const char *model_name)
{
    Oid mid = get_model_oid(model_name, true);
    if (OidIsValid(mid)) {
        remove_model_by_oid(mid);
        CommandCounterIncrement();
    }
}
