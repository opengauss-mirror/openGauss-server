/*
 * Copyright (c) 2025 Huawei Technologies Co.,Ltd.
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
 * diskannbuild.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/datavec/diskannbuild.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/datavec/diskann.h"
#include "access/tableam.h"

#define CALLBACK_ITEM_POINTER HeapTuple hup

/*
 * Initialize the build state
 */
static void InitBuildState(DiskAnnBuildState* buildstate, Relation heap, Relation index, IndexInfo* indexInfo)
{
    buildstate->heap = heap;
    buildstate->index = index;
    buildstate->indexInfo = indexInfo;
    buildstate->typeInfo = DiskAnnGetTypeInfo(index);

    DiskAnnOptions* opts = (DiskAnnOptions*)index->rd_options;

    buildstate->dimensions = TupleDescAttr(index->rd_att, 0)->atttypmod;

    /* Disallow varbit since require fixed dimensions */
    if (TupleDescAttr(index->rd_att, 0)->atttypid == VARBITOID) {
        elog(ERROR, "type not supported for diskann index");
    }

    /* Require column to have dimensions to be indexed */
    if (buildstate->dimensions < 0) {
        elog(ERROR, "column does not have dimensions");
    }

    if (buildstate->dimensions > buildstate->typeInfo->maxDimensions) {
        elog(ERROR, "column cannot have more than %d dimensions for diskann index",
             buildstate->typeInfo->maxDimensions);
    }

    if (opts) {
        buildstate->maxDegree = opts->maxDegree;
        buildstate->maxAlpha = opts->maxAlpha;
        buildstate->indexSize = opts->indexSize;
    } else {
        buildstate->maxDegree = DISKANN_DEFAULT_MAX_DEGREE;
        buildstate->maxAlpha = DISKANN_DEFAULT_MAX_ALPHA;
        buildstate->indexSize = DISKANN_DEFAULT_INDEX_SIZE;
    }

    buildstate->reltuples = 0;
    buildstate->indtuples = 0;

    /* Get support functions */
    buildstate->procinfo = index_getprocinfo(index, 1, DISKANN_DISTANCE_PROC);
    buildstate->normprocinfo = DiskAnnOptionalProcInfo(index, DISKANN_NORM_PROC);
    buildstate->kmeansnormprocinfo = DiskAnnOptionalProcInfo(index, DISKANN_KMEANS_NORMAL_PROC);
    buildstate->collation = index->rd_indcollation[0];

    /* Require more than one dimension for spherical k-means */
    if (buildstate->kmeansnormprocinfo != NULL && buildstate->dimensions == 1)
        elog(ERROR, "dimensions must be greater than one for this opclass");

    buildstate->tmpCtx =
        AllocSetContextCreate(CurrentMemoryContext, "diskann build temporary context", ALLOCSET_DEFAULT_SIZES);
}

/*
 * Free resources
 */
static void FreeBuildState(DiskAnnBuildState* buildstate)
{
    MemoryContextDelete(buildstate->tmpCtx);
}

/*
 * Build the index
 */
static void BuildIndex(Relation heap, Relation index, IndexInfo* indexInfo, DiskAnnBuildState* buildstate,
                       ForkNumber forkNum)
{
    InitBuildState(buildstate, heap, index, indexInfo);

    // todo: diskann build

    FreeBuildState(buildstate);
}

IndexBuildResult* diskannbuild_internal(Relation heap, Relation index, IndexInfo* indexInfo)
{
    IndexBuildResult* result;
    DiskAnnBuildState buildstate;

    BuildIndex(heap, index, indexInfo, &buildstate, MAIN_FORKNUM);

    result = (IndexBuildResult*)palloc(sizeof(IndexBuildResult));
    result->heap_tuples = buildstate.reltuples;
    result->index_tuples = buildstate.indtuples;

    return result;
}

void diskannbuildempty_internal(Relation index)
{
    IndexInfo* indexInfo = BuildIndexInfo(index);
    DiskAnnBuildState buildstate;

    BuildIndex(NULL, index, indexInfo, &buildstate, INIT_FORKNUM);
}
