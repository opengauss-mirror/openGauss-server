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
 *  scores.h
 *
 * IDENTIFICATION
 *        src/include/db4ai/scores.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SCORES_H
#define SCORES_H

#include "postgres.h"

typedef struct Scores {
    int count;
    // for classification or binary regression
    int tp; // true positive
    int tn; // true negative
    int fp; // false positive
    int fn; // false negative
    // for continuous regression
    float mse; // mean squared error
} Scores;

inline void scores_init(Scores *scores)
{
    errno_t rc = memset_s(scores, sizeof(Scores), 0, sizeof(Scores));
    securec_check(rc, "", "");
}

// (tp + tn) / n
inline double get_accuracy(const Scores *scores, bool *has)
{
    if (scores->count == 0) {
        *has = false;
        return 0;
    }
    *has = true;
    return (scores->tp + scores->tn) / (double)scores->count;
}

// tp / (tp + fp)
inline double get_precision(const Scores *scores, bool *has)
{
    double d = scores->tp + scores->fp;
    if (d > 0) {
        *has = true;
        return scores->tp / d;
    }
    *has = false;
    return 0;
}

// tp / (tp + fn)
inline double get_recall(const Scores *scores, bool *has)
{
    double d = scores->tp + scores->fn;
    if (d > 0) {
        *has = true;
        return scores->tp / d;
    }
    *has = false;
    return 0;
}

#endif /* SCORES_H */
