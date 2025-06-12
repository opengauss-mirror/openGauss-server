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
 * ivfkmeans.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/datavec/ivfkmeans.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include <cfloat>
#include <cmath>
#include <mutex>

#include "access/datavec/bitvec.h"
#include "access/datavec/halfutils.h"
#include "access/datavec/halfvec.h"
#include "access/datavec/ivfflat.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/memutils.h"
#include "access/datavec/vector.h"
#include "access/datavec/ivfnpuadaptor.h"

#define L2_FUNC_OID 8433
#define MAX_HBM 25.0

static int g_deviceNum = 6;

static void VectorsSquare(float *x, int dim, int xsize, float *res)
{
    for (int i = 0; i < xsize; i++) {
        res[i] = VectorSquareNorm(x + dim * i, dim);
    }
}

static void PreprocessMatrix(float *matrix, float *tmatrix, VectorArray samples, int maxNumSamples, int dim)
{
    for (int i = 0; i < samples->length; i++) {
        float *sampleData = (float *)PointerGetDatum(VectorArrayGet(samples, i));
        for (int j = 0; j < dim; j++) {
            matrix[(int64)i * dim + j] = sampleData[j + 2];
        }
    }

    if (dim == 0) ereport(ERROR, (errmsg("Please ensure dimension of vector type is not zero.")));
    for (int i = 0; i < (maxNumSamples * dim); i++) {
        tmatrix[(int64)(i % dim) * maxNumSamples + (i / dim)] = matrix[(int64)(i / dim) * dim + (i % dim)];
    }
}

static float GetDiffDistance(FmgrInfo *procinfo, float A_square, float B_square, float AB_mult)
{
    if (procinfo->fn_oid == L2_FUNC_OID) {
        return sqrt(A_square + B_square - 2 * AB_mult);
    } else {
        if (AB_mult > 1) {
            AB_mult = 1;
        } else if (AB_mult < -1) {
            AB_mult = -1;
        }
        return acos(AB_mult) / M_PI;
    }
}

 /*
 * compute all samples distance on npu
 */
static float *ComputeDistance(FmgrInfo *procinfo, VectorArray samplesA, VectorArray samplesB, int dim,
    uint8_t **tupleDevice, bool needCache, float *squareAmatrix, float *amatrix, float *tAmatrix)
{
    int numSamplesA = samplesA->length;
    int numSamplesB = samplesB->length;

    float *bmatrix = (float *)palloc(numSamplesB * sizeof(float) * dim);
    float *tBmatrix = (float *)palloc(numSamplesB * sizeof(float) * dim);

    float *squareBmatrix = nullptr;
    float *distanceInSamples = nullptr;
    errno_t rc = EOK;
    Size size = (Size)numSamplesA * sizeof(float) * dim;

    if (samplesA != samplesB) {
        PreprocessMatrix(bmatrix, tBmatrix, samplesB, numSamplesB, dim);
    } else {
        rc = memcpy_s(tBmatrix, size, tAmatrix, size);
        securec_check(rc, "\0", "\0");
    }

    /* (a^2 + a*b + b^2) */
    if ((samplesA != samplesB) && (procinfo->fn_oid == L2_FUNC_OID)) {
        squareBmatrix = new float[(int64)numSamplesB];
        VectorsSquare(bmatrix, dim, numSamplesB, squareBmatrix);
    }

    distanceInSamples = (float *)palloc_huge(CurrentMemoryContext, sizeof(float) * numSamplesA * numSamplesB);

    // batch computation
    size_t totalMem =
        ((double)((numSamplesA + numSamplesB) * dim + (int64)numSamplesA * (int64)numSamplesB) * sizeof(float)) /
        (1024 * 1024);  // MB
    int iterNum = std::max((int)(std::ceil((double)(totalMem) / (MAX_HBM * 1024))), 1);
    int batchSize = (int)std::ceil((double)numSamplesA / iterNum);
    int actualBatchSize = batchSize;

    int offset = 0;
    for (int i = 0; i < iterNum; i++) {
        actualBatchSize = std::min(batchSize, numSamplesA - offset);
        int ret = MatrixMulOnNPU(amatrix + (int64)offset * dim, tBmatrix,
            distanceInSamples + (int64)offset * numSamplesB, actualBatchSize, numSamplesB, dim,
            (uint8_t **)tupleDevice, g_deviceNum, needCache);
        if (ret != 0) {
            pfree(bmatrix);
            pfree(tBmatrix);

            delete [] squareBmatrix;
            squareBmatrix = nullptr;
            pfree(distanceInSamples);

            if (*tupleDevice != nullptr) {
                ReleaseNPUCache((uint8_t **)tupleDevice, g_deviceNum);
            }
            ereport(ERROR, (errmsg("MatrixMulOnNPU failed, errCode: %d.", ret)));
        }
        offset += actualBatchSize;
    }

    if ((samplesA == samplesB) && (procinfo->fn_oid == L2_FUNC_OID)) {
        int stride = numSamplesA + 1;
        for (int i = 0; i < numSamplesA; i++) {
            squareAmatrix[i] = distanceInSamples[(int64)i * stride];
        }
    }

    const bool isL2Distance = (procinfo->fn_oid == L2_FUNC_OID);
    const bool isSameSamples = (samplesA == samplesB);
    #pragma omp parallel for schedule(dynamic)
    for (int i = 0; i < numSamplesA; i++) {
        for (int j = 0; j < numSamplesB; j++) {
            int64 idx = (int64)i * numSamplesB + j;
            if (!isL2Distance) {
                distanceInSamples[idx] =GetDiffDistance(procinfo, 0, 0, distanceInSamples[idx]);
            } else {
                float matrixIndexA = squareAmatrix[i];
                float matrixIndexB = isSameSamples ? squareAmatrix[j] : squareBmatrix[j];
                distanceInSamples[idx] = GetDiffDistance(procinfo, matrixIndexA, matrixIndexB,
                    distanceInSamples[idx]);
            }
        }
    }

    pfree(bmatrix);
    pfree(tBmatrix);

    delete [] squareBmatrix;
    squareBmatrix = nullptr;

    return distanceInSamples;
}

/*
 * Initialize with kmeans++
 *
 * https://theory.stanford.edu/~sergei/papers/kMeansPP-soda.pdf
 */
static void InitCenters(Relation index, VectorArray samples, VectorArray centers, float *lowerBound,
    float *distanceInSamples, int dim)
{
    FmgrInfo *procinfo;
    Oid collation;
    int64 j;
    float *weight = (float *)palloc(samples->length * sizeof(float));
    int numCenters = centers->maxlen;
    int numSamples = samples->length;
    int *initCenterIndices = (int *)palloc(sizeof(int) * numCenters * dim);

    procinfo = index_getprocinfo(index, 1, IVFFLAT_KMEANS_DISTANCE_PROC);
    collation = index->rd_indcollation[0];

    /* Choose an initial center uniformly at random */
    initCenterIndices[0] = RandomInt() % samples->length;
    VectorArraySet(centers, 0, VectorArrayGet(samples, initCenterIndices[0]));
    centers->length++;

    for (j = 0; j < numSamples; j++)
        weight[j] = FLT_MAX;

    for (int i = 0; i < numCenters; i++) {
        double sum;
        double choice;

        CHECK_FOR_INTERRUPTS();

        sum = 0.0;

        for (j = 0; j < numSamples; j++) {
            Datum vec = PointerGetDatum(VectorArrayGet(samples, j));
            double distance;

            /* Only need to compute distance for new center */
            /* TODO Use triangle inequality to reduce distance calculations */
            if (u_sess->datavec_ctx.enable_npu) {
                distance = distanceInSamples[(int64)initCenterIndices[i] * numSamples + j];
            } else {
                distance = DatumGetFloat8(FunctionCall2Coll(procinfo, collation, vec,
                    PointerGetDatum(VectorArrayGet(centers, i))));
            }

            /* Set lower bound */
            lowerBound[j * numCenters + i] = distance;

            /* Use distance squared for weighted probability distribution */
            distance *= distance;

            if (distance < weight[j])
                weight[j] = distance;

            sum += weight[j];
        }

        /* Only compute lower bound on last iteration */
        if (i + 1 == numCenters) {
            break;
        }

        /* Choose new center using weighted probability distribution. */
        choice = sum * RandomDouble();
        for (j = 0; j < numSamples - 1; j++) {
            choice -= weight[j];
            if (choice <= 0)
                break;
        }

        VectorArraySet(centers, i + 1, VectorArrayGet(samples, j));
        initCenterIndices[i + 1] = j;
        centers->length++;
    }

    pfree(weight);
    pfree(initCenterIndices);
}

/*
 * Norm centers
 */
static void NormCenters(const IvfflatTypeInfo *typeInfo, Oid collation, VectorArray centers)
{
    MemoryContext normCtx =
        AllocSetContextCreate(CurrentMemoryContext, "Ivfflat norm temporary context", ALLOCSET_DEFAULT_SIZES);
    MemoryContext oldCtx = MemoryContextSwitchTo(normCtx);
    errno_t rc = EOK;

    for (int j = 0; j < centers->length; j++) {
        Datum center = PointerGetDatum(VectorArrayGet(centers, j));
        Datum newCenter = IvfflatNormValue(typeInfo, collation, center);
        Size size = VARSIZE_ANY(DatumGetPointer(newCenter));
        if (size > centers->itemsize)
            elog(ERROR, "safety check failed");

        rc = memcpy_s(DatumGetPointer(center), size, DatumGetPointer(newCenter), size);
        securec_check(rc, "\0", "\0");
        MemoryContextReset(normCtx);
    }

    MemoryContextSwitchTo(oldCtx);
    MemoryContextDelete(normCtx);
}

/*
 * Quick approach if we have no data
 */
static void RandomCenters(Relation index, VectorArray centers, const IvfflatTypeInfo *typeInfo)
{
    int dimensions = centers->dim;
    FmgrInfo *normprocinfo = IvfflatOptionalProcInfo(index, IVFFLAT_KMEANS_NORM_PROC);
    Oid collation = index->rd_indcollation[0];
    float *x = static_cast<float *>(palloc(sizeof(float) * dimensions));

    /* Fill with random data */
    while (centers->length < centers->maxlen) {
        Pointer center = VectorArrayGet(centers, centers->length);

        for (int i = 0; i < dimensions; i++) {
            x[i] = static_cast<float>(RandomDouble());
        }

        typeInfo->updateCenter(center, dimensions, x);

        centers->length++;
    }

    if (normprocinfo != NULL)
        NormCenters(typeInfo, collation, centers);
}

#ifdef IVFFLAT_MEMORY
/*
 * Show memory usage
 */
static void ShowMemoryUsage(MemoryContext context, Size estimatedSize)
{
    MemoryContextStats(context);
    elog(INFO, "estimated memory: %zu MB", estimatedSize / (1024 * 1024));
}
#endif

/*
 * Sum centers
 */
static void SumCenters(VectorArray samples, float *agg, int *closestCenters, const IvfflatTypeInfo *typeInfo)
{
    for (int j = 0; j < samples->length; j++) {
        float *x = agg + ((int64)closestCenters[j] * samples->dim);

        typeInfo->sumCenter(VectorArrayGet(samples, j), x);
    }
}

/*
 * Update centers
 */
static void UpdateCenters(float *agg, VectorArray centers, const IvfflatTypeInfo *typeInfo)
{
    for (int j = 0; j < centers->length; j++) {
        float *x = agg + ((int64)j * centers->dim);

        typeInfo->updateCenter(VectorArrayGet(centers, j), centers->dim, x);
    }
}

/*
 * Compute new centers
 */
static void ComputeNewCenters(VectorArray samples, float *agg, VectorArray newCenters, int *centerCounts,
                              int *closestCenters, FmgrInfo *normprocinfo, Oid collation,
                              const IvfflatTypeInfo *typeInfo)
{
    int dimensions = newCenters->dim;
    int numCenters = newCenters->length;
    int numSamples = samples->length;

    /* Reset sum and count */
    for (int j = 0; j < numCenters; j++) {
        float *x = agg + ((int64)j * dimensions);

        for (int k = 0; k < dimensions; k++) {
            x[k] = 0.0;
        }

        centerCounts[j] = 0;
    }

    /* Increment sum of closest center */
    SumCenters(samples, agg, closestCenters, typeInfo);

    /* Increment count of closest center */
    for (int j = 0; j < numSamples; j++) {
        centerCounts[closestCenters[j]] += 1;
    }

    /* Divide sum by count */
    for (int j = 0; j < numCenters; j++) {
        float *x = agg + ((int64)j * dimensions);

        if (centerCounts[j] > 0) {
            /* Double avoids overflow, but requires more memory */
            /* TODO Update bounds */
            for (int k = 0; k < dimensions; k++) {
                if (isinf(x[k])) {
                    x[k] = x[k] > 0 ? FLT_MAX : -FLT_MAX;
                }
            }

            for (int k = 0; k < dimensions; k++) {
                x[k] /= centerCounts[j];
            }
        } else {
            /* TODO Handle empty centers properly */
            for (int k = 0; k < dimensions; k++) {
                x[k] = RandomDouble();
            }
        }
    }

    /* Set new centers */
    UpdateCenters(agg, newCenters, typeInfo);

    /* Normalize if needed */
    if (normprocinfo != NULL)
        NormCenters(typeInfo, collation, newCenters);
}

/*
 * Use Elkan for performance. This requires distance function to satisfy triangle inequality.
 *
 * We use L2 distance for L2 (not L2 squared like index scan)
 * and angular distance for inner product and cosine distance
 *
 * https://www.aaai.org/Papers/ICML/2003/ICML03-022.pdf
 */
static void ElkanKmeans(Relation index, VectorArray samples, VectorArray centers, const IvfflatTypeInfo *typeInfo)
{
    FmgrInfo *procinfo;
    FmgrInfo *normprocinfo;
    Oid collation;
    int dimensions = centers->dim;
    int numCenters = centers->maxlen;
    int numSamples = samples->length;
    VectorArray newCenters;
    float *agg;
    int *centerCounts;
    int *closestCenters;
    float *lowerBound;
    float *upperBound;
    float *s;
    float *halfcdist;
    float *newcdist;

    /* Calculate allocation sizes */
    Size samplesSize = VECTOR_ARRAY_SIZE(samples->maxlen, samples->itemsize);
    Size centersSize = VECTOR_ARRAY_SIZE(centers->maxlen, centers->itemsize);
    Size newCentersSize = VECTOR_ARRAY_SIZE(numCenters, centers->itemsize);
    Size aggSize = sizeof(float) * (int64)numCenters * dimensions;
    Size centerCountsSize = sizeof(int) * numCenters;
    Size closestCentersSize = sizeof(int) * numSamples;
    Size lowerBoundSize = sizeof(float) * numSamples * numCenters;
    Size upperBoundSize = sizeof(float) * numSamples;
    Size sSize = sizeof(float) * numCenters;
    Size halfcdistSize = sizeof(float) * numCenters * numCenters;
    Size newcdistSize = sizeof(float) * numCenters;

    /* Calculate total size */
    Size totalSize = samplesSize + centersSize + newCentersSize + aggSize + centerCountsSize + closestCentersSize +
                     lowerBoundSize + upperBoundSize + sSize + halfcdistSize + newcdistSize;

    /* Check memory requirements */
    /* Add one to error message to ceil */
    if (totalSize > (Size)u_sess->attr.attr_memory.maintenance_work_mem * 1024L)
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                        errmsg("memory required is %zu MB, maintenance_work_mem is %d MB",
                               totalSize / (1024 * 1024) + 1, u_sess->attr.attr_memory.maintenance_work_mem / 1024)));

    /* Ensure indexing does not overflow */
    if (numCenters * numCenters > INT_MAX)
        elog(ERROR, "Indexing overflow detected. Please report a bug.");

    /* Set support functions */
    procinfo = index_getprocinfo(index, 1, IVFFLAT_KMEANS_DISTANCE_PROC);
    normprocinfo = IvfflatOptionalProcInfo(index, IVFFLAT_KMEANS_NORM_PROC);
    collation = index->rd_indcollation[0];

    /* Allocate space */
    /* Use float instead of double to save memory */
    agg = (float *)palloc(aggSize);
    centerCounts = (int *)palloc(centerCountsSize);
    closestCenters = (int *)palloc(closestCentersSize);
    lowerBound = (float *)MemoryContextAllocExtended(CurrentMemoryContext, lowerBoundSize, MCXT_ALLOC_HUGE);
    upperBound = (float *)palloc(upperBoundSize);
    s = (float *)palloc(sSize);
    halfcdist = (float *)MemoryContextAllocExtended(CurrentMemoryContext, halfcdistSize, MCXT_ALLOC_HUGE);
    newcdist = (float *)palloc(newcdistSize);

    /* Initialize new centers */
    newCenters = VectorArrayInit(numCenters, dimensions, centers->itemsize);
    newCenters->length = numCenters;

#ifdef IVFFLAT_MEMORY
    ShowMemoryUsage(MemoryContextGetParent(CurrentMemoryContext));
#endif

     /* Pick initial centers */
    InitCenters(index, samples, centers, lowerBound, NULL, dimensions);

    /* Assign each x to its closest initial center c(x) = argmin d(x,c) */
    for (int64 j = 0; j < numSamples; j++) {
        float minDistance = FLT_MAX;
        int closestCenter = 0;

        /* Find closest center */
        for (int64 k = 0; k < numCenters; k++) {
            /* TODO Use Lemma 1 in k-means++ initialization */
            float distance = lowerBound[j * numCenters + k];

            if (distance < minDistance) {
                minDistance = distance;
                closestCenter = k;
            }
        }

        upperBound[j] = minDistance;
        closestCenters[j] = closestCenter;
    }

    /* Give 500 iterations to converge */
    for (int iteration = 0; iteration < 500; iteration++) {
        int changes = 0;
        bool rjreset;

        /* Can take a while, so ensure we can interrupt */
        CHECK_FOR_INTERRUPTS();

        /* Step 1: For all centers, compute distance */
        for (int64 j = 0; j < numCenters; j++) {
            Datum vec = PointerGetDatum(VectorArrayGet(centers, j));

            for (int64 k = j + 1; k < numCenters; k++) {
                float distance = 0.5 * DatumGetFloat8(FunctionCall2Coll(procinfo, collation, vec,
                                                                        PointerGetDatum(VectorArrayGet(centers, k))));

                halfcdist[j * numCenters + k] = distance;
                halfcdist[k * numCenters + j] = distance;
            }
        }

        /* For all centers c, compute s(c) */
        for (int64 j = 0; j < numCenters; j++) {
            float minDistance = FLT_MAX;

            for (int64 k = 0; k < numCenters; k++) {
                float distance;

                if (j == k)
                    continue;

                distance = halfcdist[j * numCenters + k];
                if (distance < minDistance)
                    minDistance = distance;
            }

            s[j] = minDistance;
        }

        rjreset = iteration != 0;

        for (int64 j = 0; j < numSamples; j++) {
            bool rj;

            /* Step 2: Identify all points x such that u(x) <= s(c(x)) */
            if (upperBound[j] <= s[closestCenters[j]])
                continue;

            rj = rjreset;

            for (int64 k = 0; k < numCenters; k++) {
                Datum vec;
                float dxcx;

                /* Step 3: For all remaining points x and centers c */
                if (k == closestCenters[j])
                    continue;

                if (upperBound[j] <= lowerBound[j * numCenters + k])
                    continue;

                if (upperBound[j] <= halfcdist[closestCenters[j] * numCenters + k])
                    continue;

                vec = PointerGetDatum(VectorArrayGet(samples, j));

                /* Step 3a */
                if (rj) {
                    dxcx = DatumGetFloat8(FunctionCall2Coll(
                        procinfo, collation, vec, PointerGetDatum(VectorArrayGet(centers, closestCenters[j]))));

                    /* d(x,c(x)) computed, which is a form of d(x,c) */
                    lowerBound[j * numCenters + closestCenters[j]] = dxcx;
                    upperBound[j] = dxcx;

                    rj = false;
                } else
                    dxcx = upperBound[j];

                /* Step 3b */
                if (dxcx > lowerBound[j * numCenters + k] || dxcx > halfcdist[closestCenters[j] * numCenters + k]) {
                    float dxc = DatumGetFloat8(
                        FunctionCall2Coll(procinfo, collation, vec, PointerGetDatum(VectorArrayGet(centers, k))));

                    /* d(x,c) calculated */
                    lowerBound[j * numCenters + k] = dxc;

                    if (dxc < dxcx) {
                        closestCenters[j] = k;

                        /* c(x) changed */
                        upperBound[j] = dxc;

                        changes++;
                    }
                }
            }
        }

        /* Step 4: For each center c, let m(c) be mean of all points assigned */
        ComputeNewCenters(samples, agg, newCenters, centerCounts, closestCenters, normprocinfo, collation, typeInfo);

        /* Step 5 */
        for (int j = 0; j < numCenters; j++)
            newcdist[j] =
                DatumGetFloat8(FunctionCall2Coll(procinfo, collation, PointerGetDatum(VectorArrayGet(centers, j)),
                                                 PointerGetDatum(VectorArrayGet(newCenters, j))));

        for (int64 j = 0; j < numSamples; j++) {
            for (int64 k = 0; k < numCenters; k++) {
                float distance = lowerBound[j * numCenters + k] - newcdist[k];

                if (distance < 0) {
                    distance = 0;
                }

                lowerBound[j * numCenters + k] = distance;
            }
        }

        /* Step 6 */
        /* We reset r(x) before Step 3 in the next iteration */
        for (int j = 0; j < numSamples; j++) {
            upperBound[j] += newcdist[closestCenters[j]];
        }

        /* Step 7 */
        for (int j = 0; j < numCenters; j++) {
            VectorArraySet(centers, j, VectorArrayGet(newCenters, j));
        }

        if (changes == 0 && iteration != 0) {
            break;
        }
    }
}

static void ElkanKmeansOnNPU(Relation index, VectorArray samples, VectorArray centers, const IvfflatTypeInfo *typeInfo)
{
    FmgrInfo *procinfo;
    FmgrInfo *normprocinfo;
    Oid collation;
    int dimensions = centers->dim;
    int numCenters = centers->maxlen;
    int numSamples = samples->length;
    VectorArray newCenters;
    float *agg;
    int *centerCounts;
    int *closestCenters;
    float *lowerBound;

    /* Calculate allocation sizes */
    Size samplesSize = VECTOR_ARRAY_SIZE(samples->maxlen, samples->itemsize);
    Size centersSize = VECTOR_ARRAY_SIZE(centers->maxlen, centers->itemsize);
    Size newCentersSize = VECTOR_ARRAY_SIZE(numCenters, centers->itemsize);
    Size aggSize = sizeof(float) * (int64)numCenters * dimensions;
    Size centerCountsSize = sizeof(int) * numCenters;
    Size closestCentersSize = sizeof(int) * numSamples;
    Size lowerBoundSize = sizeof(float) * numSamples * numCenters;

    /* Calculate total size */
    Size totalSize = samplesSize + centersSize + newCentersSize + aggSize + centerCountsSize + closestCentersSize +
                     lowerBoundSize;

    /* Check memory requirements */
    /* Add one to error message to ceil */
    if (totalSize > (Size)u_sess->attr.attr_memory.maintenance_work_mem * 1024L)
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                        errmsg("memory required is %zu MB, maintenance_work_mem is %d MB",
                               totalSize / (1024 * 1024) + 1, u_sess->attr.attr_memory.maintenance_work_mem / 1024)));

    /* Ensure indexing does not overflow */
    if (numCenters * numCenters > INT_MAX)
        elog(ERROR, "Indexing overflow detected. Please report a bug.");

    /* Set support functions */
    procinfo = index_getprocinfo(index, 1, IVFFLAT_KMEANS_DISTANCE_PROC);
    normprocinfo = IvfflatOptionalProcInfo(index, IVFFLAT_KMEANS_NORM_PROC);
    collation = index->rd_indcollation[0];

    /* Allocate space */
    /* Use float instead of double to save memory */
    agg = (float *)palloc(aggSize);
    centerCounts = (int *)palloc(centerCountsSize);
    closestCenters = (int *)palloc(closestCentersSize);
    lowerBound = (float *)MemoryContextAllocExtended(CurrentMemoryContext, lowerBoundSize, MCXT_ALLOC_HUGE);

    /* Initialize new centers */
    newCenters = VectorArrayInit(numCenters, dimensions, centers->itemsize);
    newCenters->length = numCenters;

#ifdef IVFFLAT_MEMORY
    ShowMemoryUsage(MemoryContextGetParent(CurrentMemoryContext));
#endif

    uint8_t *tupleDevice = nullptr;
    uint8_t *nullTmp = nullptr;

    float *samplesMatrix = (float *)palloc(sizeof(float) * numSamples * dimensions);
    float *tsamplesMatrix = (float *)palloc(sizeof(float) * dimensions * numSamples);
    PreprocessMatrix(samplesMatrix, tsamplesMatrix, samples, numSamples, dimensions);

    float *samplesSquare = (float *)palloc(sizeof(float) * numSamples);
    float *disBetweenSamples = ComputeDistance(procinfo, samples, samples, dimensions, &nullTmp, false, samplesSquare,
        samplesMatrix, tsamplesMatrix);

    /* Pick initial centers */
    InitCenters(index, samples, centers, lowerBound, disBetweenSamples, dimensions);

    pfree(disBetweenSamples);

    /* Assign each x to its closest initial center c(x) = argmin d(x,c) */
    for (int64 j = 0; j < numSamples; j++) {
        float minDistance = FLT_MAX;
        int closestCenter = 0;

        /* Find closest center */
        for (int64 k = 0; k < numCenters; k++) {
            float distance = lowerBound[j * numCenters + k];

            if (distance < minDistance) {
                minDistance = distance;
                closestCenter = k;
            }
        }
        closestCenters[j] = closestCenter;
    }

    /* Give 500 iterations to converge */
    for (int iteration = 0; iteration < 500; iteration++) {
        int changes = 0;

        /* Can take a while, so ensure we can interrupt */
        CHECK_FOR_INTERRUPTS();

        float *distBetweenSamplesAndCenters = ComputeDistance(procinfo, samples, centers, dimensions, &tupleDevice,
            true, samplesSquare, samplesMatrix, tsamplesMatrix);

        /* For all centers c, compute s(c) */
        for (int64 j = 0; j < numSamples; j++) {
            float minDistance = FLT_MAX;
            int64 bestK = -1;

            for (int64 k = 0; k < numCenters; k++) {
                float dist = distBetweenSamplesAndCenters[(int64)j * numCenters + k];
                if (dist < minDistance) {
                    minDistance = dist;
                    bestK = k;
                }
            }

           // Check for updates
            if (bestK != closestCenters[j]) {
                closestCenters[j] = bestK;
                changes++;
            }
        }

        pfree(distBetweenSamplesAndCenters);

        /* Step 4: For each center c, let m(c) be mean of all points assigned */
        ComputeNewCenters(samples, agg, newCenters, centerCounts, closestCenters, normprocinfo, collation, typeInfo);

        /* Step 7 */
        for (int j = 0; j < numCenters; j++) {
            VectorArraySet(centers, j, VectorArrayGet(newCenters, j));
        }

        if (changes == 0 && iteration != 0) {
            ReleaseNPUCache(&tupleDevice, g_deviceNum);   // delete cache in NPU
            ereport(LOG, (errmsg("iteration : %d", iteration)));
            break;
        }
    }
    pfree(samplesSquare);
    pfree(samplesMatrix);
    pfree(tsamplesMatrix);
}

/*
 * Ensure no NaN or infinite values
 */
static void CheckElements(VectorArray centers, const IvfflatTypeInfo *typeInfo)
{
    float *scratch = (float *)palloc(sizeof(float) * centers->dim);

    for (int i = 0; i < centers->length; i++) {
        for (int j = 0; j < centers->dim; j++)
            scratch[j] = 0;

        /* /fp:fast may not propagate NaN with MSVC, but that's alright */
        typeInfo->sumCenter(VectorArrayGet(centers, i), scratch);

        for (int j = 0; j < centers->dim; j++) {
            if (isnan(scratch[j]))
                elog(ERROR, "NaN detected. Please report a bug.");

            if (isinf(scratch[j]))
                elog(ERROR, "Infinite value detected. Please report a bug.");
        }
    }
}

/*
 * Ensure no zero vectors for cosine distance
 */
static void CheckNorms(VectorArray centers, Relation index)
{
    /* Check NORM_PROC instead of KMEANS_NORM_PROC */
    FmgrInfo *normprocinfo = IvfflatOptionalProcInfo(index, IVFFLAT_NORM_PROC);
    Oid collation = index->rd_indcollation[0];

    if (normprocinfo == NULL) {
        return;
    }

    for (int i = 0; i < centers->length; i++) {
        double norm =
            DatumGetFloat8(FunctionCall1Coll(normprocinfo, collation, PointerGetDatum(VectorArrayGet(centers, i))));
        if (norm == 0) {
            elog(ERROR, "Zero norm detected. Please report a bug.");
        }
    }
}

/*
 * Detect issues with centers
 */
static void CheckCenters(Relation index, VectorArray centers, const IvfflatTypeInfo *typeInfo)
{
    if (centers->length != centers->maxlen)
        elog(ERROR, "Not enough centers. Please report a bug.");

    CheckElements(centers, typeInfo);
    CheckNorms(centers, index);
}

/*
 * Perform naive k-means centering
 * We use spherical k-means for inner product and cosine
 */
void IvfflatKmeans(Relation index, VectorArray samples, VectorArray centers, const IvfflatTypeInfo *typeInfo)
{
    MemoryContext kmeansCtx =
        AllocSetContextCreate(CurrentMemoryContext, "Ivfflat kmeans temporary context", ALLOCSET_DEFAULT_SIZES);
    MemoryContext oldCtx = MemoryContextSwitchTo(kmeansCtx);

    if (samples->length == 0) {
        RandomCenters(index, centers, typeInfo);
    } else {
        if (u_sess->datavec_ctx.enable_npu)
            ElkanKmeansOnNPU(index, samples, centers, typeInfo);
        else
            ElkanKmeans(index, samples, centers, typeInfo);
    }

    CheckCenters(index, centers, typeInfo);

    MemoryContextSwitchTo(oldCtx);
    MemoryContextDelete(kmeansCtx);
}
