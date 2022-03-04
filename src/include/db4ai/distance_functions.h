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

distance_functions.h
       Current set of distance functions that can be used (for k-means for example)

IDENTIFICATION
    src/include/db4ai/distance_functions.h

---------------------------------------------------------------------------------------
**/

#ifndef DB4AI_DISTANCE_FUNCTIONS_H
#define DB4AI_DISTANCE_FUNCTIONS_H

#include <cinttypes>

/*
 * L1 distance (Manhattan)
 */
extern double l1(double const* p, double const* q, uint32_t dimension);

/*
 * Squared Euclidean (default)
 */
extern double l2_squared(double const* p, double const* q, uint32_t dimension);

/*
 * L2 distance (Euclidean)
 */
extern double l2(double const* p, double const* q, uint32_t dimension);

/*
 * L infinity distance (Chebyshev)
 */
extern double linf(double const* p, double const* q, uint32_t dimension);

#endif //DB4AI_DISTANCE_FUNCTIONS_H
