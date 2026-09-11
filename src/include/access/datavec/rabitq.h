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

/*
 * ------------------------------------------------------------------------
 * 1 / 2-bit RaBitQ over a transformed space (DiskANN RaBitQ format).
 *
 * The code stands for y = M (x - mean) of a PCA_ORTHOGONAL transform (dim =
 * dimOut of the transform). The 1-bit API above is untouched; this variant
 * differs in the factor head and in the number of bit planes:
 *
 *   FactorDataBits (16 bytes)
 *     normSqr = |y|^2
 *     dpMul   = |y|^2 / <y, y_hat>
 *     ipMu    = <x, mean>            (IP metric, 0 otherwise)
 *     resSqr  = |x - mean|^2 - |y|^2 (energy dropped by the reduction, telemetry)
 *   data: `bits` planes of rbqPlaneBytes(dim) bytes each, bit i of a plane at
 *     data[i / 8] & (1 << (i % 8)) exactly like the 1-bit code; plane 0 is the
 *     sign plane s_hi = sign(y), plane 1 (2-bit only) the magnitude plane
 *     s_lo = sign(|y_i| - RBQ_2BIT_ALPHA * rms), rms = |y| / sqrt(D).
 *
 *   reconstruction (s in {-1, +1}^D)
 *     1-bit  y_hat = s_hi / sqrt(D)
 *     2-bit  y_hat = (RBQ_2BIT_ALPHA * s_hi + RBQ_2BIT_BETA * s_lo) / sqrt(D)
 *            (up to the rms scale, the four Lloyd-Max levels of N(0, 1):
 *             +-0.4528, +-1.5104; the scale is absorbed by dpMul)
 *   estimates
 *     <y_q, y> ~= dpMul * <y_q, y_hat>
 *     L2       d^2 ~= |y_q|^2 + normSqr - 2 * dpMul * <y_q, y_hat>
 *     IP       -<q, x> ~= -(dpMul * <y_q, y_hat> + ipMu + (<q, mean> - |mean|^2))
 *
 * The query side reuses SetRBQQuery's qb bit-plane quantization and its
 * QueryRabitqVector: <y_q, s> / sqrt(D) per plane costs one VectorRbqDpPopcnt
 * plus the plane popcount. For the IP metric the per-query constant
 * <q, mean> - |mean|^2 travels in QueryFactorData.qrNormL2Sqr (unused by the
 * L2 path of SetRBQQuery). funcType == DIS_IP selects the IP estimate; every
 * other metric (L2, COSINE on normalized vectors) gets the L2 estimate.
 */
#define RBQ_TWO_BIT 2
#define RBQ_2BIT_ALPHA 0.9816f
#define RBQ_2BIT_BETA 0.5288f

static inline Size rbqPlaneBytes(int d)
{
    return ((Size)d + BITS_PER_BYTE - 1) / BITS_PER_BYTE;
}

static inline Size rbqCodeBytesBits(int d, int bits)
{
    return (Size)bits * rbqPlaneBytes(d);
}

typedef struct FactorDataBits {
    float normSqr;
    float dpMul;
    float ipMu;
    float resSqr;
} FactorDataBits;

typedef struct RabitqVectorBits {
    FactorDataBits fac;
    uint8 data[FLEXIBLE_ARRAY_MEMBER];
} RabitqVectorBits;

/*
 * Shared arguments for the 1 / 2-bit APIs. Encode uses y / x / mean / bits;
 * the query side uses y as the transformed query, x as the input-space query,
 * and qb as the number of query planes. Distance uses dim / qb / bits / funcType.
 */
typedef struct RbqBitsArgs {
    int dim;
    int bits;
    int qb;
    int funcType;
    int dimIn;
    const float *y;
    const float *x;
    const float *mean;
} RbqBitsArgs;

/*
 * Encode args->y (already transformed) into rbqVec with args->bits planes.
 * args->x / mean / dimIn feed ipMu (funcType == DIS_IP) and resSqr. Returns
 * |x - mean|^2 (e.g. for picking the vector closest to the mean).
 */
float ComputeVectorRBQCodeBits(const RbqBitsArgs *args, RabitqVectorBits *rbqVec);
/*
 * Query side: SetRBQQuery on args->y (args->qb planes) plus the IP constant
 * from args->x / mean / dimIn (ignored unless funcType == DIS_IP).
 * qrbqVec must hold rbqQuerySize(args->dim, args->qb) bytes.
 */
void SetRBQQueryBits(const RbqBitsArgs *args, QueryRabitqVector *qrbqVec);
/* estimated distance of one code to the quantized query (see the table above) */
float ComputeRbqDistanceBits(const RbqBitsArgs *args, const RabitqVectorBits *eVec, const QueryRabitqVector *qVec);
/* estimated squared L2 between two codes in the transformed space (no query) */
float ComputeRbqCodeDistanceBits(int dim, int bits, const RabitqVectorBits *a, const RabitqVectorBits *b);

#endif