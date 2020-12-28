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
 *  pruningboundary.cpp
 *        partition boundary
 *
 * IDENTIFICATION
 *        src/gausskernel/optimizer/util/pruningboundary.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "catalog/pg_type.h"
#include "catalog/index.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/relation.h"
#include "optimizer/clauses.h"
#include "optimizer/pruning.h"
#include "utils/array.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "catalog/pg_partition_fn.h"

void destroyPruningBoundary(PruningBoundary* boundary)
{
    int i = 0;
    Const* tempConst = NULL;

    if (!PointerIsValid(boundary)) {
        return;
    }

    for (; i < boundary->partitionKeyNum; i++) {
        tempConst = (Const*)DatumGetPointer(boundary->max[i]);
        if (tempConst != NULL) {
            pfree_ext(tempConst);
        }
        tempConst = (Const*)DatumGetPointer(boundary->min[i]);
        if (tempConst != NULL) {
            pfree_ext(tempConst);
        }
    }

    pfree_ext(boundary);
    return;
}

PruningBoundary* makePruningBoundary(int partKeyNum)
{
    PruningBoundary* boundary = NULL;

    AssertEreport(partKeyNum > 0, MOD_OPT, "Expected positive number of partion key, run into exception.");

    boundary = (PruningBoundary*)palloc0(sizeof(PruningBoundary));

    boundary->partitionKeyNum = partKeyNum;

    boundary->max = (Datum*)palloc0(partKeyNum * sizeof(Datum));

    boundary->maxClose = (bool*)palloc0(partKeyNum * sizeof(bool));

    boundary->min = (Datum*)palloc0(partKeyNum * sizeof(Datum));

    boundary->minClose = (bool*)palloc0(partKeyNum * sizeof(bool));

    boundary->state = PRUNING_RESULT_FULL;

    return boundary;
}

PruningBoundary* copyBoundary(PruningBoundary* boundary)
{
    PruningBoundary* result = NULL;
    int i = 0;
    Const* tempConst = NULL;

    if (!PointerIsValid(boundary)) {
        return NULL;
    }

    result = makePruningBoundary(boundary->partitionKeyNum);

    result->state = boundary->state;

    for (; i < result->partitionKeyNum; i++) {
        result->maxClose[i] = boundary->maxClose[i];
        result->minClose[i] = boundary->minClose[i];

        tempConst = (Const*)DatumGetPointer(boundary->max[i]);
        tempConst = (Const*)copyObject(tempConst);
        result->max[i] = PointerGetDatum(tempConst);

        tempConst = (Const*)DatumGetPointer(boundary->min[i]);
        tempConst = (Const*)copyObject(tempConst);
        result->min[i] = PointerGetDatum(tempConst);
    }

    return result;
}
