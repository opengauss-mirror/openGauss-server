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
 *  svm.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/dbmind/db4ai/executor/gd/svm.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "db4ai/gd.h"

static void svmc_gradients(const GradientDescent *gd_node, const Matrix *features, const Matrix *dep_var,
    Matrix *weights, Matrix *gradients)
{
    Assert(features->rows > 0);

    // distances = 1 - y * (x * w)
    Matrix distance;
    matrix_init(&distance, features->rows);
    matrix_mult_vector(features, weights, &distance);
    matrix_mult_entrywise(&distance, dep_var);
    matrix_complement(&distance);

    Assert(distance.rows == dep_var->rows);
    Assert(distance.columns == 1);

    const gd_float *pf = features->data;
    const gd_float *py = dep_var->data;
    const gd_float *pd = distance.data;
    gd_float *pg = gradients->data;
    for (int r = 0; r < distance.rows; r++) {
        gd_float y = *py++;
        gd_float d = *pd++;
        if (d > 0) {
            for (int f = 0; f < features->columns; f++)
                pg[f] -= y * pf[f];
        }
        pf += features->columns;
    }
    matrix_release(&distance);

    matrix_mult_scalar(gradients, gd_node->lambda * 2.0);
}

static double svmc_test(const GradientDescent *gd_node, const Matrix *features, const Matrix *dep_var,
    const Matrix *weights, Scores *scores)
{
    Assert(features->rows > 0);

    Matrix distances;
    Matrix predictions;
    matrix_init(&distances, features->rows);
    matrix_mult_vector(features, weights, &distances);

    matrix_init_clone(&predictions, &distances);
    matrix_positive(&predictions);
    matrix_binary(&predictions, FLT_MIN, -1.0, 1.0);
    matrix_relevance(&predictions, dep_var, scores, 1.0);
    matrix_release(&predictions);

    // cost = (1 - y * x * w)
    matrix_mult_entrywise(&distances, dep_var);
    matrix_complement(&distances);
    matrix_positive(&distances);

    gd_float tuple_loss = matrix_get_sum(&distances) / features->rows;

    matrix_release(&distances);
    return tuple_loss;
}

static gd_float svmc_predict(const Matrix *features, const Matrix *weights)
{
    double r = matrix_dot(features, weights);
    return r < 0 ? -1.0 : 1.0;
}

GradientDescentAlgorithm gd_svm_classification = {
    "svm-classification",
    GD_DEPENDENT_VAR_BINARY,
    METRIC_ACCURACY | METRIC_F1 | METRIC_PRECISION | METRIC_RECALL | METRIC_LOSS,
    -1.0,
    1.0,
    svmc_gradients,
    svmc_test,
    svmc_predict,
};
