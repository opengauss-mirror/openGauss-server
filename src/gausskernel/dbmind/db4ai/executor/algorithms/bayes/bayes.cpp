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
 * ---------------------------------------------------------------------------------------
 *
 *  bayes.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/dbmind/db4ai/executor/algorithms/bayes/bayes.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include <boost/math/distributions/normal.hpp>
#include <cmath>
#include "access/hash.h"
#include "db4ai/bayes.h"
const float epsilon = 0.000001;
double probabilityDensity(double x, double mean, double stdDiv)
{
    double prob = 0.0;
    if (stdDiv == 0.0) {
        if (abs(x - mean) < epsilon) {
            prob = 1.0;
        }
    } else {
        boost::math::normal_distribution<double> norm(mean, stdDiv);
        prob = boost::math::pdf(norm, x);
    }
    return prob;
}

bool checkTypeConsistent(Oid *tl, Oid *tr, int nattrs)
{
    for (int i = 0; i < nattrs; i++) {
        if (tl[i] != tr[i])
            return false;
    }
    return true;
}

ValueInTuple create_value(Datum data, Oid type, bool isnull)
{
    uint32_t hashvalue = 0;
    if (!isnull) {
        /* As before, used invalid collation. B format collation is not supported yet. */
        hashvalue = DatumGetUInt32(compute_hash(type, data, LOCATOR_TYPE_HASH, InvalidOid));
    }
    ValueInTuple val = {data, type, isnull, hashvalue};
    return val;
}

static int binarySearch(ValueInTuple input, bayesDatumList *features, int low, int high, bool enable_lastleftless)
{
    while (low <= high) {
        int difference = 0;
        int middle = low + (high - low) / 2;
        difference = cmp_value_bayesnet<ValueInTuple>((void *)(&input), (void *)(&(*features)[middle]));
        if (difference == 0)
            return middle;
        if (difference > 0)
            low = middle + 1;
        else
            high = middle - 1;
    }
    return enable_lastleftless ? high : -1;
}

int findDiscreteIndex(ValueInTuple input, bayesDatumList *features, int data_preprocess)
{
    int res = -1;
    uint32_t featurenum = features->size();
    int upper_offset_with_null = -2;
    int upper_offset_without_null = -1;
    if (data_preprocess == BAYES_BIN) {
        if ((*features)[featurenum - 1].isnull) {
            if (input.isnull) {
                res = featurenum - 1;
            } else {
                if (cmp_value_bayesnet<ValueInTuple>((void *)(&input),
                    (void *)(&(*features)[featurenum + upper_offset_with_null])) <= 0) {
                    res = binarySearch(input, features, 0, featurenum + upper_offset_with_null - 1, true);
                }
            }
        } else {
            if (!input.isnull) {
                if (cmp_value_bayesnet<ValueInTuple>((void *)(&input),
                    (void *)(&(*features)[featurenum + upper_offset_without_null])) <= 0) {
                    res = binarySearch(input, features, 0, featurenum + upper_offset_without_null - 1, true);
                }
            }
        }
    } else {
        res = binarySearch(input, features, 0, featurenum - 1, false);
    }
    return res;
}
