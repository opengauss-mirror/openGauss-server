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

kmeans.h
        Public k-means interface

IDENTIFICATION
    src/include/db4ai/kmeans.h

---------------------------------------------------------------------------------------
**/

#ifndef DB4AI_KMEANS_H
#define DB4AI_KMEANS_H

#include "db4ai/db4ai_api.h"
#include "db4ai/fp_ops.h"

/*
 * current available distance functions
 */
typedef enum : uint32_t {
    KMEANS_L1 = 0U,
    KMEANS_L2,
    KMEANS_L2_SQUARED,
    KMEANS_LINF
} DistanceFunction;

/*
 * current available seeding method
 */
typedef enum : uint32_t {
    KMEANS_RANDOM_SEED = 0U,
    KMEANS_BB
} SeedingFunction;

/*
 * Verbosity level
 */
typedef enum : uint32_t {
    NO_OUTPUT = 0U,
    FASTCHECK_OUTPUT,
    VERBOSE_OUTPUT
} Verbosity;

/*
 * the k-means node (re-packaged)
 */
typedef struct KMeans {
    AlgorithmAPI algo;
} KMeans;

/*
 * internal representation of a centroid
 */
typedef struct Centroid {
    IncrementalStatistics statistics;
    ArrayType* coordinates = nullptr;
    uint32_t id = 0U;
} Centroid;

/*
 * internal representation of a point (not a centroid)
 */
typedef struct GSPoint {
    ArrayType* pg_coordinates = nullptr;
    uint32_t weight = 1U;
    uint32_t id = 0ULL;
    double distance_to_closest_centroid = DBL_MAX;
    bool should_free = false;
} GSPoint;

#endif //DB4AI_KMEANS_H
