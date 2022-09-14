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
#include "utils/lsyscache.h"

#define DEBUG_MODEL_RETURN_TYPE INT4OID // Set manually the return type of model, until available from catalog

/*
 * functions relevant to xgboost and defined in xgboost.cpp
 */
ModelPredictor xgboost_predict_prepare(AlgorithmAPI *, Model const *model);
Datum xgboost_predict(AlgorithmAPI*, ModelPredictor model, Datum* values, bool* isnull, Oid* types, int ncolumns);


struct PredictionByData {
    PredictorInterface *api;
    ModelPredictor model_predictor;
    AlgorithmAPI *algorithm;
};
 
static void check_func_oid(Oid model_retype, Oid func)
{
    Oid mft = UNKNOWNOID;
    switch(model_retype){
        case INT1OID:
        case INT2OID:
        case INT4OID:
            mft = INT4OID;
            break;
        case VARCHAROID:
        case BPCHAROID:
        case CHAROID:
        case TEXTOID:
            mft = TEXTOID;
            break;
        case FLOAT4ARRAYOID:
        case FLOAT8ARRAYOID:
            mft = FLOAT8ARRAYOID;
        default:
            mft = model_retype;
    }
    if (mft != func)
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_DATATYPE_MISMATCH),
                    errmsg("The function return type is mismatched with model result type: %u.", model_retype)));
}

static PredictionByData *initialize_predict_by_data(const Model *model)
{
    PredictionByData *result = (PredictionByData *)palloc0(sizeof(PredictionByData));

    // Initialize the API handlers
    result->algorithm = get_algorithm_api(model->algorithm);
    result->api = (PredictorInterface*)palloc0(sizeof(PredictorInterface));
    Assert(result->algorithm->predict != nullptr);
    Assert(result->algorithm->prepare_predict != nullptr);
    result->api->predict = result->algorithm->predict;
    result->api->prepare = NULL;

    // Prepare the in memory version of the model for efficient prediction
    result->model_predictor = result->algorithm->prepare_predict(result->algorithm, &model->data, model->return_type);

    return result;
}

template <Oid ft>
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

        const Model *model = get_model(model_name, false);
        if (!model)
            ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("There is no model called \"%s\".", model_name)));
        prediction_by_data = initialize_predict_by_data(model);
        fcinfo->flinfo->fn_extra = prediction_by_data;
        pfree(model_name);
        check_func_oid(model->return_type, ft);
        MemoryContextSwitchTo(oldContext);
    }

    Datum result = prediction_by_data->api->predict(prediction_by_data->algorithm, prediction_by_data->model_predictor,
                                                    var_args, nulls, types, var_args_size);

    PG_RETURN_DATUM(result);
}

// These functions are only a wrapper for the generic function. We need this wrapper
// to be compliant with openGauss type system that specifies the return type
Datum db4ai_predict_by_bool(PG_FUNCTION_ARGS)
{
    return db4ai_predict_by<BOOLOID>(fcinfo);
}

Datum db4ai_predict_by_int32(PG_FUNCTION_ARGS)
{
    return db4ai_predict_by<INT4OID>(fcinfo);
}

Datum db4ai_predict_by_int64(PG_FUNCTION_ARGS)
{
    return db4ai_predict_by<INT8OID>(fcinfo);
}

Datum db4ai_predict_by_float4(PG_FUNCTION_ARGS)
{
    return db4ai_predict_by<FLOAT4OID>(fcinfo);
}

Datum db4ai_predict_by_float8(PG_FUNCTION_ARGS)
{
    return db4ai_predict_by<FLOAT8OID>(fcinfo);
}

Datum db4ai_predict_by_float8_array(PG_FUNCTION_ARGS)
{
    return db4ai_predict_by<FLOAT8ARRAYOID>(fcinfo);
}


Datum db4ai_predict_by_numeric(PG_FUNCTION_ARGS)
{
    return db4ai_predict_by<NUMERICOID>(fcinfo);
}

Datum db4ai_predict_by_text(PG_FUNCTION_ARGS)
{
    return db4ai_predict_by<TEXTOID>(fcinfo);
}
