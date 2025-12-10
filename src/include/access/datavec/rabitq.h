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
 * rabitq.h
 *
 * IDENTIFICATION
 *        src/include/access/datavec/rabitq.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef RABITQ_H
#define RABITQ_H

#include "postgres.h"

#include "access/genam.h"
#include "nodes/execnodes.h"
#include "access/datavec/vector.h"
#include "access/datavec/vectortransformer.h"
#include "access/datavec/scalarquantizer.h"
#include "access/datavec/utils.h"

typedef struct RabitQConfig {
    bool FHT;
    VectorTransform *vtrans;
    int rbqQueryBits;
    uint16 reOffset;
    RefineType reType;
    int64 kreorder;
    ScalarQuantizer *sq;
} RabitQConfig;

typedef struct FactorData {
    float orMinusCL2Sqr;
    float xbSum;
    float dpMultiplier;
    uint32 unused;
} FactorData;

typedef struct RabitqVector {
    FactorData fac;
    uint8 data[FLEXIBLE_ARRAY_MEMBER];
} RabitqVector;

typedef struct QueryFactorData {
    float cof1;
    float cof2;
    float cof34;
    
    float qrMinusCL2Sqr;
    float qrNormL2Sqr;
} QueryFactorData;

typedef struct QueryRabitqVector {
    QueryFactorData fac;
    uint8 data[FLEXIBLE_ARRAY_MEMBER];
} QueryRabitqVector;

typedef struct RabitqInsertOnDiskParams {
    Relation heap;
    FmgrInfo *normprocinfo;
    Oid collation;
    int funcType;
    HeapTuple heapTuple;
    IndexInfo *indexInfo;
    VectorTransform* vtrans;
} RabitqInsertOnDiskParams;

typedef struct RabitqQueryParams {
    int dim;
    int funcType;
    float *centroid;
    Relation heap;
    FmgrInfo *normprocinfo;
    Oid collation;
    Datum originQueryVec;
    RabitQConfig *rbqConfig;
    QueryRabitqVector* qrbqVec;
} RabitqQueryParams;

#define rbqCodeSize(d, sq8) MAXALIGN(sizeof(FactorData) + (d + 7) / 8 + (sq8 ? d : 0))
#define getRefineCode(ptr, offset) &(((RabitqVector *)ptr)->data[offset])
#define rbqQuerySize(d, qb) MAXALIGN(sizeof(QueryFactorData) + ((d + 7) / 8) * qb)
#define rbqDataSize(d, sq8) rbqCodeSize(d, sq8) - sizeof(FactorData)

#define RBQ_BUILD_NORMAL 1
#define RBQ_BUILD_DELAY 2
#define RBQ_BUILD_AFTER_DELAY 3

void ComputeVectorRBQCode(int dim, float *vec, RabitqVector *rbqVec, float *centroid, int funcType);
void SetRBQQuery(int dim, int qb, float *vec, QueryRabitqVector *qrbqVec, float *centroid, int funcType);
float ComputeRbqDistance(int dim, int qb, RabitqVector *eVec, QueryRabitqVector *qVec, int funcType);

#endif