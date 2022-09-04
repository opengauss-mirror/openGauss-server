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
 *  predict.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/dbmind/db4ai/executor/gd/predict.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "postgres.h"
#include "utils/bytea.h"

#include "db4ai/gd.h"
#include "db4ai/db4ai_common.h"
#include "db4ai/model_warehouse.h"
#include "db4ai/predict_by.h"
#include "db4ai/kernel.h"

extern bool verify_pgarray(ArrayType const * pg_array, int32_t n);

ModelPredictor gd_predict_prepare(AlgorithmAPI *self, const SerializedModel *model, Oid return_type)
{
    SerializedModelGD *gdp = (SerializedModelGD *)palloc0(sizeof(SerializedModelGD));
    gd_deserialize((GradientDescent*)self, model, return_type, gdp);
    if (gdp->algorithm->prepare_kernel != nullptr) {
        gdp->kernel = gdp->algorithm->prepare_kernel(gdp->input, &gdp->hyperparameters);
        if (gdp->kernel != nullptr)
            matrix_init(&gdp->aux_input, gdp->input);
    }
    return (ModelPredictor *)gdp;
}

Datum gd_predict(AlgorithmAPI *self, ModelPredictor pred, Datum *values, bool *isnull, Oid *types, int ncolumns)
{
    // extract coefficients from model
    SerializedModelGD *gdp = (SerializedModelGD *)pred;
    bool const has_bias = gdp->algorithm->add_bias;
    
    // extract the features
    float8 *w;
    if (gdp->kernel == nullptr)
        w = gdp->features.data;
    else
        w = gdp->aux_input.data;

    if (ncolumns == 1 && (types[0] == FLOAT8ARRAYOID || types[0] == FLOAT4ARRAYOID)) {
        Assert(!isnull[0]);
        
        gd_copy_pg_array_data(w, values[0], gdp->input);
        w += gdp->input;
    } else {
        if (ncolumns != gdp->input)
            ereport(ERROR, (errmodule(MOD_DB4AI),
                    errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("Invalid number of features for prediction, provided %d, expected %d",
                           ncolumns, gdp->input)));
        
        for (int i = 0; i < ncolumns; i++) {
            double value;
            if (isnull[i])
                value = 0.0; // default value for feature, it is not the target for sure
            else
                value = datum_get_float8(types[i], values[i]);
            
            *w++ = value;
        }
    }

    if (gdp->kernel != nullptr) {
        // now apply the kernel transformation using a fake vector
        Matrix tmp;
        matrix_init(&tmp, gdp->kernel->coefficients);
        
        // make sure the transformation points to the destination vector
        float8 *p = matrix_swap_data(&tmp, gdp->features.data);
        gdp->kernel->transform(gdp->kernel, &gdp->aux_input, &tmp);
        matrix_swap_data(&tmp, p);
        matrix_release(&tmp);

        // update pointers of data
        w = gdp->features.data + gdp->kernel->coefficients;
    }

    if (has_bias)
        *w = 1.0;
    
    // and predict
    bool categorize;
    Datum result =
        gdp->algorithm->predict(&gdp->features, &gdp->weights, gdp->return_type, gdp->extra_data, false, &categorize);
    if (categorize) {
        Assert(!gd_dep_var_is_continuous(gdp->algorithm));
        float8 r = DatumGetFloat4(result);
        if (gd_dep_var_is_binary(gdp->algorithm)) {
            result = gdp->categories[0];
            if (r != gdp->algorithm->min_class) {
                Assert(r == gdp->algorithm->max_class);
                result = gdp->categories[1];
            }
        } else {
            Assert(r < gdp->ncategories);
            result = gdp->categories[(int)r];
        }
    }
    
    return result;
}
