/*
 * Copyright (c) 2024 Huawei Technologies Co.,Ltd.
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
 * -------------------------------------------------------------------------
 *
 * ivfadaptor.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/datavec/ivfadaptor.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <dlfcn.h>
#include "access/datavec/ivfflat.h"
#include "access/datavec/utils.h"

int IvfComputePQTable(VectorArray samples, PQParams *params)
{
    return g_pq_func.ComputePQTable(samples, params);
}

int IvfComputeVectorPQCode(float *vector, const PQParams *params, uint8 *pqCode)
{
    return g_pq_func.ComputeVectorPQCode(vector, params, pqCode);
}

int IvfGetPQDistanceTableAdc(float *vector, const PQParams *params, float *pqDistanceTable)
{
    return g_pq_func.GetPQDistanceTableAdc(vector, params, pqDistanceTable);
}

int IvfGetPQDistance(const uint8 *basecode, const uint8 *querycode, const PQParams *params,
                     const float *pqDistanceTable, float *pqDistance)
{
    return g_pq_func.GetPQDistance(basecode, querycode, params, pqDistanceTable, pqDistance);
}