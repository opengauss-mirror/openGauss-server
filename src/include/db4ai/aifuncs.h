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
 *  plannodes.h
 *
 * IDENTIFICATION
 *        src/include/dbmind/db4ai/nodes/aifuncs.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DB4AI_AIFUNCS_H
#define DB4AI_AIFUNCS_H

#include "nodes/plannodes.h"

inline const char* algorithm_ml_to_string(AlgorithmML x)
{
    switch(x) {
        case LOGISTIC_REGRESSION:   return "logistic_regression";
        case SVM_CLASSIFICATION:    return "svm_classification";
        case LINEAR_REGRESSION:     return "linear_regression";
        case KMEANS:                return "kmeans";
        case INVALID_ALGORITHM_ML:
        default:                    return "INVALID_ALGORITHM_ML";
    }
}

inline AlgorithmML get_algorithm_ml(const char *str)
{
    if (0 == strcmp(str, "logistic_regression")) {
        return LOGISTIC_REGRESSION;
    } else if (0 == strcmp(str, "svm_classification")) {
        return SVM_CLASSIFICATION;
    } else if (0 == strcmp(str, "linear_regression")) {
        return LINEAR_REGRESSION;
    } else if (0 == strcmp(str, "kmeans")) {
        return KMEANS;
    } else {
        return INVALID_ALGORITHM_ML;
    }
}

inline bool is_supervised(AlgorithmML algorithm)
{
    if (algorithm == KMEANS) {
        return false;
    } else {
        return true;
    }
}

#endif // DB4AI_AIFUNCS_H
