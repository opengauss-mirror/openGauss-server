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
 * -------------------------------------------------------------------------
 *
 * blcost.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/bloom/blcost.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "optimizer/cost.h"
#include "utils/selfuncs.h"

#include "access/bloom.h"

/*
 * Estimate cost of bloom index scan.
 */
void blcostestimate_internal(PlannerInfo *root, IndexPath *path, double loopCount,
    Cost *indexStartupCost, Cost *indexTotalCost, Selectivity *indexSelectivity, double *indexCorrelation)
{
    IndexOptInfo *index = path->indexinfo;
    List       *qinfos;
    GenericCosts costs;
    errno_t rc;

    /* Do preliminary analysis of indexquals */
    qinfos = deconstruct_indexquals(path);

    rc = memset_s(&costs, sizeof(costs), 0, sizeof(costs));
    securec_check(rc, "\0", "\0");

    /* We have to visit all index tuples anyway */
    costs.numIndexTuples = index->tuples;

    /* Use generic estimate */
    genericcostestimate(root, path, loopCount,
                        costs.numIndexTuples, &costs.indexStartupCost,
                        &costs.indexTotalCost, &costs.indexSelectivity,
                        &costs.indexCorrelation);
    *indexStartupCost = costs.indexStartupCost;
    *indexTotalCost = costs.indexTotalCost;
    *indexSelectivity = costs.indexSelectivity;
    *indexCorrelation = costs.indexCorrelation;
}
