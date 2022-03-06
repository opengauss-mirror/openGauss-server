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

bool LogisticRegressionDirectAPI::do_train(char const *model_name, Hyperparameters const *hp,
                                           Descriptor *td, Reader *reader)
{
    model = model_fit(model_name, LOGISTIC_REGRESSION, hp->hps, hp->count,
                      td->coltypes, td->colbyvals, td->coltyplen,
                      td->count, &LogisticRegressionDirectAPI::fetch,
                      &LogisticRegressionDirectAPI::rescan, reader);
    if (model->status != ERRCODE_SUCCESSFUL_COMPLETION)
        elog(ERROR, "could not train model, error %d\n", model->status);
    
    return true;
}

bool LogisticRegressionDirectAPI::do_predict(Reader *reader)
{
    int row = 0;
    ModelPredictor pred = model_prepare_predict(model);
    const ModelTuple *rtuple;
    while ((rtuple = reader->fetch()) != nullptr) {
        bool isnull = rtuple->isnull[1];
        if (!isnull) {
            bool target = DatumGetBool(rtuple->values[0]);
            Datum dt = model_predict(pred, &rtuple->values[1], &rtuple->isnull[1], &rtuple->typid[1], 1);
            printf(" - %d %d %d\n", ++row, target, DatumGetBool(dt));
        }
    }

    return true;
}

/*
 * implementation of SVM classification
 */

bool SvmcDirectAPI::do_train(char const *model_name, Hyperparameters const *hp,
                                           Descriptor *td, Reader *reader)
{
    model = model_fit(model_name, SVM_CLASSIFICATION, hp->hps, hp->count,
                      td->coltypes, td->colbyvals, td->coltyplen,
                      td->count, &SvmcDirectAPI::fetch,
                      &SvmcDirectAPI::rescan, reader);
    if (model->status != ERRCODE_SUCCESSFUL_COMPLETION)
        elog(ERROR, "could not train model, error %d\n", model->status);
    
    double accuracy = lfirst_node(TrainingScore, list_head(model->scores))->value;
    printf(" - train accuracy=%.3f iterations=%d", accuracy, model->num_actual_iterations);

    return true;
}

bool SvmcDirectAPI::do_predict(Reader *reader)
{
    int row = 0;
    int hit = 0;
    ModelPredictor pred = model_prepare_predict(model);
    const ModelTuple *rtuple;
    while ((rtuple = reader->fetch()) != nullptr) {
        bool isnull = rtuple->isnull[1];
        if (!isnull) {
            bool target = DatumGetInt32(rtuple->values[0]);
            Datum dt = model_predict(pred, &rtuple->values[1], &rtuple->isnull[1], &rtuple->typid[1], 2);
            if (target == DatumGetInt32(dt))
                hit++;
            row++;
        }
    }
    printf(" - predict %d: accuracy=%.3f\n", row, (double)hit / row);
    return true;
}

/*
 * implementation of multiclass
 */

bool MulticlassDirectAPI::do_train(char const *model_name, Hyperparameters const *hp,
                                           Descriptor *td, Reader *reader)
{
    model = model_fit(model_name, MULTICLASS, hp->hps, hp->count,
                      td->coltypes, td->colbyvals, td->coltyplen,
                      td->count, &MulticlassDirectAPI::fetch,
                      &MulticlassDirectAPI::rescan, reader);
    if (model->status != ERRCODE_SUCCESSFUL_COMPLETION)
        elog(ERROR, "could not train model, error %d\n", model->status);
    
    return true;
}

bool MulticlassDirectAPI::do_predict(Reader *reader)
{
    int rows = 0;
    int hit = 0;
    ModelPredictor pred = model_prepare_predict(model);
    const ModelTuple *rtuple;
    while ((rtuple = reader->fetch()) != nullptr) {
        bool isnull = rtuple->isnull[1];
        if (!isnull) {
            Datum dt = model_predict(pred, &rtuple->values[1], &rtuple->isnull[1], &rtuple->typid[1], rtuple->ncolumns-1);

            char* target =  TextDatumGetCString(rtuple->values[0]);
            char* pred =  TextDatumGetCString(dt);
            if ((rows++ % 5) == 0)
                printf(" - %d <%s> <%s>\n", rows, target, pred);

            if (strcmp(target, pred) == 0)
                hit++;
        }
    }
    printf("accuracy = %.5f\n", hit / (double)rows);
    return true;
}

/*
 * implementation of principal components
 */

bool PrincipalComponentsDirectAPI::do_train(char const *model_name, Hyperparameters const *hp,
                                            Descriptor *td, Reader *reader)
{
    model = model_fit(model_name, PCA, hp->hps, hp->count,
                      td->coltypes, td->colbyvals, td->coltyplen,
                      td->count, &PrincipalComponentsDirectAPI::fetch,
                      &PrincipalComponentsDirectAPI::rescan, reader);
    if (model->status != ERRCODE_SUCCESSFUL_COMPLETION)
        elog(ERROR, "could not train model, error %d\n", model->status);
    
    return true;
}

bool PrincipalComponentsDirectAPI::do_predict(Reader *reader)
{
    ModelPredictor pred = model_prepare_predict(model);
    int r = 0;
    const ModelTuple *rtuple;
    while ((rtuple = reader->fetch()) != nullptr) {
        Datum value = rtuple->values[0];
        bool isnull = false;
        Oid type = FLOAT8ARRAYOID;
        Datum dt = model_predict(pred, &value, &isnull, &type, 1);
        printf(" - point %d: %s\n", ++r,
             DatumGetCString(OidFunctionCall2Coll(F_ARRAY_OUT, InvalidOid, dt, FLOAT8ARRAYOID)));
    }

    return true;
}