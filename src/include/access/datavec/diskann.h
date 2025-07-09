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
 * diskann.h
 *
 * IDENTIFICATION
 *        src/include/access/datavec/diskann.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef DISKANN_H
#define DISKANN_H

#include "postgres.h"
#include "utils/rel.h"
#include "fmgr.h"
#include "nodes/execnodes.h"
#include "access/datavec/vector.h"
#include "access/datavec/utils.h"
#include "access/amapi.h"

#define DISKANN_FUNC_NUM  5

/* Support functions */
#define DISKANN_DISTANCE_PROC 1
#define DISKANN_NORM_PROC 2
#define DISKANN_TYPE_INFO_PROC 3
#define DISKANN_KMEANS_NORMAL_PROC 4

#define DISKANN_MAX_DIM 2000
#define DISKANN_DEFAULT_MAX_DEGREE 9600
#define DISKANN_DEFAULT_MAX_ALPHA 1
#define DISKANN_MIN_INDEX_SIZE 16
#define DISKANN_MAX_INDEX_SIZE 1000
#define DISKANN_DEFAULT_INDEX_SIZE 16

typedef struct DiskAnnTypeInfo {
    int maxDimensions;
    bool supportPQ;
    Size (*itemSize)(int dimensions);
    Datum (*normalize)(PG_FUNCTION_ARGS);
    void (*checkValue)(Pointer v);
} DiskAnnTypeInfo;

/* DiskAnn index options */
typedef struct DiskAnnOptions {
    StdRdOptions* rd_options;
    int dimensions;
    int maxDegree;
    int maxAlpha;
    int indexSize;
} DiskAnnOptions;

typedef struct DiskAnnBuildState {
    /* Info */
    Relation heap;
    Relation index;
    IndexInfo* indexInfo;
    const DiskAnnTypeInfo* typeInfo;

    /* Settings */
    uint16 dimensions;
    uint32 maxDegree;
    uint32 maxAlpha;
    uint32 indexSize;

    /* Statistics */
    double indtuples;
    double reltuples;

    /* Support functions */
    FmgrInfo* procinfo;
    FmgrInfo* normprocinfo;
    FmgrInfo* kmeansnormprocinfo;
    Oid collation;

    /* Memory */
    MemoryContext tmpCtx;
} DiskAnnBuildState;

Datum diskannhandler(PG_FUNCTION_ARGS);
Datum diskannbuild(PG_FUNCTION_ARGS);
Datum diskannbuildempty(PG_FUNCTION_ARGS);
Datum diskanninsert(PG_FUNCTION_ARGS);
Datum diskannbulkdelete(PG_FUNCTION_ARGS);
Datum diskannvacuumcleanup(PG_FUNCTION_ARGS);
Datum diskanncostestimate(PG_FUNCTION_ARGS);
Datum diskannoptions(PG_FUNCTION_ARGS);
Datum diskannvalidate(PG_FUNCTION_ARGS);
Datum diskannbeginscan(PG_FUNCTION_ARGS);
Datum diskannrescan(PG_FUNCTION_ARGS);
Datum diskanngettuple(PG_FUNCTION_ARGS);
Datum diskannendscan(PG_FUNCTION_ARGS);

const DiskAnnTypeInfo* DiskAnnGetTypeInfo(Relation index);
FmgrInfo* DiskAnnOptionalProcInfo(Relation index, uint16 procnum);
IndexBuildResult* diskannbuild_internal(Relation heap, Relation index, IndexInfo* indexInfo);
void diskannbuildempty_internal(Relation index);
bool diskanninsert_internal(Relation index, Datum* values, const bool* isnull, ItemPointer heap_tid, Relation heap,
                            IndexUniqueCheck checkUnique);
IndexBulkDeleteResult* diskannbulkdelete_internal(IndexVacuumInfo* info, IndexBulkDeleteResult* stats,
                                                  IndexBulkDeleteCallback callback, void* callbackState);
IndexBulkDeleteResult* diskannvacuumcleanup_internal(IndexVacuumInfo* info, IndexBulkDeleteResult* stats);
IndexScanDesc diskannbeginscan_internal(Relation index, int nkeys, int norderbys);
void diskannrescan_internal(IndexScanDesc scan, ScanKey keys, int nkeys, ScanKey orderbys, int norderbys);
bool diskanngettuple_internal(IndexScanDesc scan, ScanDirection dir);
void diskannendscan_internal(IndexScanDesc scan);
#endif
