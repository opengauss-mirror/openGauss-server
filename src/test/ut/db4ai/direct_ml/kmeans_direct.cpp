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
 * kmeans_direct.cpp
 *        Implementation of k-means using DB4AI's direct API
 *
 *
 * IDENTIFICATION
 *        src/test/ut/db4ai/direct_ml/kmeans_direct.cpp
 *
 * ---------------------------------------------------------------------------------------
**/

#include "direct_algos.h"

extern ArrayType *construct_empty_md_array(uint32_t const num_centroids, uint32_t const dimension);

bool KMeansDirectAPI::do_train(char const *model_name, Hyperparameters const *hp,
                               Descriptor *td, Reader *reader)
{
    model = model_fit(model_name, KMEANS, hp->hps, hp->count,
                      td->coltypes, td->colbyvals, td->coltyplen,
                      td->count, &KMeansDirectAPI::fetch,
                      &KMeansDirectAPI::rescan, reader);
    if (model->status != ERRCODE_SUCCESSFUL_COMPLETION)
        elog(ERROR, "could not train model, error %d\n", model->status);
    
    return true;
}

bool KMeansDirectAPI::do_predict(Reader *reader)
{
    ModelPredictor pred = model_prepare_predict(model);
    int r = 0;
    const ModelTuple *rtuple;
    while ((rtuple = reader->fetch()) != nullptr) {
        Datum value = rtuple->values[0];
        bool isnull = false;
        Oid type = FLOAT8ARRAYOID;
        Datum dt = model_predict(pred, &value, &isnull, &type, 1);
        printf(" - point %d: %d\n", ++r, DatumGetInt32(dt));
    }
    return true;
}