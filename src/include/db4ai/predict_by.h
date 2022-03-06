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
 *        src/include/db4ai/predict_by.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DB4AI_PREDICT_BY_H
#define DB4AI_PREDICT_BY_H

#include "postgres.h"

#include "db4ai/model_warehouse.h"
#include "nodes/parsenodes_common.h"
#include "fmgr/fmgr_comp.h"
#include "parser/parse_node.h"



typedef void* ModelPredictor; // Deserialized version of the model that can compute efficiently predictions

struct AlgorithmAPI;
struct PredictorInterface {
    ModelPredictor (*prepare) (AlgorithmAPI*, const Model* model);
    Datum (*predict) (AlgorithmAPI*, ModelPredictor pred, Datum* values, bool* isnull, Oid* types, int values_size);
};

Datum db4ai_predict_by_bool(PG_FUNCTION_ARGS);
Datum db4ai_predict_by_bytea(PG_FUNCTION_ARGS);
Datum db4ai_predict_by_int32(PG_FUNCTION_ARGS);
Datum db4ai_predict_by_int64(PG_FUNCTION_ARGS);
Datum db4ai_predict_by_float4(PG_FUNCTION_ARGS);
Datum db4ai_predict_by_float8(PG_FUNCTION_ARGS);
Datum db4ai_predict_by_float8_array(PG_FUNCTION_ARGS);
Datum db4ai_predict_by_numeric(PG_FUNCTION_ARGS);
Datum db4ai_predict_by_text(PG_FUNCTION_ARGS);


#endif
