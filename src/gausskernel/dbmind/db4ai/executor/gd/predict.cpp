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

#include "db4ai/gd.h"
#include "db4ai/model_warehouse.h"
#include "db4ai/predict_by.h"

typedef struct GradientDescentPredictor {
    GradientDescentAlgorithm *algorithm;
    int ncategories;
    Oid return_type;
    Matrix weights;
    Matrix features;
    Datum *categories;
} GradientDescentPredictor;

ModelPredictor gd_predict_prepare(const Model *model)
{
    GradientDescentPredictor *gdp = (GradientDescentPredictor *)palloc0(sizeof(GradientDescentPredictor));
    ModelGradientDescent *gdp_model = (ModelGradientDescent *)model;
    gdp->algorithm = gd_get_algorithm(gdp_model->model.algorithm);
    gdp->ncategories = gdp_model->ncategories;
    gdp->return_type = gdp_model->model.return_type;

    ArrayType *arr = (ArrayType *)pg_detoast_datum((struct varlena *)DatumGetPointer(gdp_model->weights));
    Assert(arr->elemtype == FLOAT4OID);

    int coefficients = ARR_DIMS(arr)[0];
    matrix_init(&gdp->weights, coefficients);
    matrix_init(&gdp->features, coefficients);

    Datum dt;
    bool isnull;
    gd_float *pf = gdp->weights.data;
    ArrayIterator it = array_create_iterator(arr, 0);
    while (array_iterate(it, &dt, &isnull)) {
        Assert(!isnull);
        *pf++ = DatumGetFloat4(dt);
    }
    array_free_iterator(it);
    if (arr != (ArrayType *)DatumGetPointer(gdp_model->weights))
        pfree(arr);

    if (gdp->ncategories > 0) {
        arr = (ArrayType *)pg_detoast_datum((struct varlena *)DatumGetPointer(gdp_model->categories));
        gdp->categories = (Datum *)palloc(ARR_DIMS(arr)[0] * sizeof(Datum));

        int cat = 0;
        it = array_create_iterator(arr, 0);
        while (array_iterate(it, &dt, &isnull)) {
            Assert(!isnull);
            gdp->categories[cat++] = dt;
        }
        array_free_iterator(it);
        if (arr != (ArrayType *)DatumGetPointer(gdp_model->categories))
            pfree(arr);
    } else
        gdp->categories = nullptr;

    return (ModelPredictor *)gdp;
}

Datum gd_predict(ModelPredictor pred, Datum *values, bool *isnull, Oid *types, int ncolumns)
{
    // extract coefficients from model
    GradientDescentPredictor *gdp = (GradientDescentPredictor *)pred;

    // extract the features
    if (ncolumns != (int)gdp->weights.rows - 1)
        elog(ERROR, "Invalid number of features for prediction, provided %d, expected %d", ncolumns,
            gdp->weights.rows - 1);

    gd_float *w = gdp->features.data;
    for (int i = 0; i < ncolumns; i++) {
        double value;
        if (isnull[i])
            value = 0.0; // default value for feature, it is not the target for sure
        else
            value = gd_datum_get_float(types[i], values[i]);

        *w++ = value;
    }
    *w = 1.0; // bias

    Datum result = 0;
    gd_float r = gdp->algorithm->predict_callback(&gdp->features, &gdp->weights);
    if (dep_var_is_binary(gdp->algorithm)) {
        if (gdp->ncategories == 0) {
            ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INTERNAL_ERROR),
                            errmsg("For classification algorithms: %s, the number of categories should not be 0.",
                                   gdp->algorithm->name)));
        }
        result = gdp->categories[0];
        if (r != gdp->algorithm->min_class) {
            if (gdp->ncategories == 2 && r == gdp->algorithm->max_class)
                result = gdp->categories[1];
        }
    } else {
        result = gd_float_get_datum(gdp->return_type, r);
    }

    return result;
}
