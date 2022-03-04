/**
Copyright (c) 2021 Huawei Technologies Co.,Ltd. 
openGauss is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:

  http://license.coscl.org.cn/MulanPSL2

THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details.
---------------------------------------------------------------------------------------
 *
 * gd_direct.cpp
 *        Implementation of GD-based algorithms using DB4AI's direct API
 *
 *
 * IDENTIFICATION
 *        src/test/ut/db4ai/direct_ml/gd_direct.cpp
 *
 * ---------------------------------------------------------------------------------------
**/

#include "direct_algos.h"

/*
 * implementation of logistic regression
 */

bool XGBoostDirectAPI::do_train(char const *model_name, Hyperparameters const *hp,
                                           Descriptor *td, Reader *reader)
{
    model = model_fit(model_name, XG_BIN_LOGISTIC, hp->hps, hp->count,
                      td->coltypes, td->colbyvals, td->coltyplen,
                      td->count, &XGBoostDirectAPI::fetch,
                      &XGBoostDirectAPI::rescan, reader);
    if (model->status != ERRCODE_SUCCESSFUL_COMPLETION)
        elog(ERROR, "could not train model, error %d\n", model->status);
    
    return true;
}

bool XGBoostDirectAPI::do_predict(Reader *reader)
{
    ModelPredictor pred = model_prepare_predict(model);
    const ModelTuple *rtuple;
    while ((rtuple = reader->fetch()) != nullptr) {
        Datum value = rtuple->values[0];
        bool isnull = false;
        Oid type = INT4OID;
        Datum dt = model_predict(pred, &value, &isnull, &type, 1);
        bool target = DatumGetInt32(rtuple->values[1]);
        bool predicted = (DatumGetFloat8(dt) < 0.2);
        printf(" - %d %d %d\n", DatumGetInt32(rtuple->values[0]), target, predicted);
    }

    return true;
}