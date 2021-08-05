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
 *
 * IDENTIFICATION
 *        src/gausskernel/dbmind/db4ai/commands/predict_by.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "db4ai/predict_by.h"

#include "catalog/pg_type.h"
#include "db4ai/model_warehouse.h"
#include "nodes/parsenodes_common.h"
#include "parser/parse_expr.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "db4ai/gd.h"

#define DEBUG_MODEL_RETURN_TYPE INT4OID // Set manually the return type of model, until available from catalog

/*
 * functions relevant to k-means and defined in kmeans.cpp
 */
ModelPredictor kmeans_predict_prepare(Model const * model);
Datum kmeans_predict(ModelPredictor model, Datum *data, bool *nulls, Oid *types, int nargs);

struct PredictionByData {
    PredictorInterface *api;
    ModelPredictor model_predictor;
};

static PredictionByData *initialize_predict_by_data(Model *model)
{
    PredictionByData *result = (PredictionByData *)palloc0(sizeof(PredictionByData));

    // Initialize the API handlers
    switch (model->algorithm) {
        case LOGISTIC_REGRESSION:
        case SVM_CLASSIFICATION:
        case LINEAR_REGRESSION: {
            result->api = (PredictorInterface *)palloc0(sizeof(PredictorInterface));
            result->api->predict = gd_predict;
            result->api->prepare = gd_predict_prepare;
            break;
        }
        case KMEANS: {
            result->api = (PredictorInterface *)palloc0(sizeof(PredictorInterface));
            result->api->predict = kmeans_predict;
            result->api->prepare = kmeans_predict_prepare;
            break;
        }
        case INVALID_ALGORITHM_ML:
        default: {
            ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Model type %d is not supported for prediction", (int)model->algorithm)));
            break;
        }
    }

    // Prepare the in memory version of the model for efficient prediction
    result->model_predictor = result->api->prepare(model);

    return result;
}

Datum db4ai_predict_by(PG_FUNCTION_ARGS)
{
    // First argument is the model, the following ones are the inputs to the model predictor
    if (PG_NARGS() < 2) {
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("INVALID Number of parameters %d", PG_NARGS())));
    }

    int var_args_size = PG_NARGS() - 1;
    Datum *var_args = &fcinfo->arg[1]; // We skip the model name
    bool *nulls = &fcinfo->argnull[1];
    Oid *types = &fcinfo->argTypes[1];

    PredictionByData *prediction_by_data;
    if (fcinfo->flinfo->fn_extra != NULL) {
        prediction_by_data = (PredictionByData *)fcinfo->flinfo->fn_extra;
    } else {
        // Initialize the model, and save the object as state of the function.
        // The memory allocation is done in the function context. So, the model
        // will be deallocated automatically when the function finishes
        MemoryContext oldContext = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);
        text *model_name_text = PG_GETARG_TEXT_P(0);
        char *model_name = text_to_cstring(model_name_text);

        Model *model = get_model(model_name, false);
        prediction_by_data = initialize_predict_by_data(model);
        fcinfo->flinfo->fn_extra = prediction_by_data;
        pfree(model_name);

        MemoryContextSwitchTo(oldContext);
    }

    Datum result =
        prediction_by_data->api->predict(prediction_by_data->model_predictor, var_args, nulls, types, var_args_size);

    PG_RETURN_DATUM(result);
}

// These functions are only a wrapper for the generic function. We need this wrapper
// to be compliant with openGauss type system that specifies the return type
Datum db4ai_predict_by_bool(PG_FUNCTION_ARGS)
{
    return db4ai_predict_by(fcinfo);
}

Datum db4ai_predict_by_int32(PG_FUNCTION_ARGS)
{
    return db4ai_predict_by(fcinfo);
}

Datum db4ai_predict_by_int64(PG_FUNCTION_ARGS)
{
    return db4ai_predict_by(fcinfo);
}

Datum db4ai_predict_by_float4(PG_FUNCTION_ARGS)
{
    return db4ai_predict_by(fcinfo);
}

Datum db4ai_predict_by_float8(PG_FUNCTION_ARGS)
{
    return db4ai_predict_by(fcinfo);
}

Datum db4ai_predict_by_numeric(PG_FUNCTION_ARGS)
{
    return db4ai_predict_by(fcinfo);
}

Datum db4ai_predict_by_text(PG_FUNCTION_ARGS)
{
    return db4ai_predict_by(fcinfo);
}
