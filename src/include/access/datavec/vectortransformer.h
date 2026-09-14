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
 * vectortransformer.h
 *
 * IDENTIFICATION
 *        src/include/access/datavec/vectortransformer.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef VECTORTRANSFORMER_H
#define VECTORTRANSFORMER_H

#include "postgres.h"

#include "nodes/execnodes.h"
#include "access/datavec/vector.h"

#include <random>

#define MAX_RETRIES 5
#define FHT_ROUND 4
#define RANSEED 12345
#define FHTBLOCK 64
#define FHT_POS_SIGN 1.0f
#define FHT_NEG_SIGN -1.0f

enum VectorTransformType {
    RANDOM_ORTHOGONAL, /* Random Orthogonal Matrix */
    FAST_HTRANSFORM,   /* Fast Walsh-Hadamard Transform Matrix */
    PCA_ORTHOGONAL     /* PCA projection merged with a random orthogonal rotation: y = M (x - mean) */
};

typedef struct {
    uint16_t i;
    uint16_t j;
} Swap;

typedef struct FastRotation {
    int outputDim;
    Swap** swaps;        /* swaps[rounds][n_swaps] */
    float** signs;       /* signs[rounds][output_dim] */
} FastRotation;

typedef struct VectorTransform VectorTransform;

struct VectorTransform {
    VectorTransformType type;
    int dim; /* ROM / FHT: input (= output) dimension */

    /* ROM: dim x dim orthogonal matrix. PCA_ORTHOGONAL: M = W * P, dimOut x dimIn, row-major */
    float *matrix;

    /* FHT */
    FastRotation *fastRotation;

    /*
     * PCA_ORTHOGONAL only (ROM / FHT users leave these untouched). mean and
     * matrix live in one allocation, mean first, so the pair serializes as
     * one byte stream (PcaSerialize) and frees as one block.
     */
    int dimIn;
    int dimOut;
    float *mean; /* dimIn */
};

struct RandomGenerator {
    std::mt19937 mt;

    // random positive integer
    int rand_int()
    {
        return mt() & 0x7fffffff;
    }

    double rand_double()
    {
        return mt() / double(mt.max());
    }

    uint8_t rand_uint8()
    {
        return static_cast<uint8_t>(rand_int() % 256);
    }

    explicit RandomGenerator(int64_t seed = 1234) : mt((unsigned int)seed) {}
};

void RomTrain(VectorTransform* vtrans);
void RomTransform(VectorTransform* vtrans, const float* vec, float *transvec);
void *RomGetMatrix(VectorTransform* vtrans);
int FhtOutputDim(int inputDim);
void FhtTrain(VectorTransform* vtrans);
void FhtTransform(VectorTransform* vtrans, const float* vec, float *transvec);
void *FhtGetMatrix(VectorTransform* vtrans);
size_t FhtSerializeSize(int outputDim);
void FreeTransformer(VectorTransform *vt);
FastRotation *FhtDeserialize(void *rbq);

/*
 * PCA_ORTHOGONAL: y = M (x - mean), M = W * P (dimOut x dimIn) where P holds
 * the top dimOut principal components of the samples and W is a dimOut x
 * dimOut random orthogonal rotation (the RANDOM_ORTHOGONAL construction).
 * dimOut == dimIn takes P = I: a pure rotation around the mean, no samples
 * needed. Caller sets vtrans->dimIn / dimOut first. mean is the population
 * mean the caller accumulated over the whole data set (NULL: the sample mean
 * is used; zero when there are no samples).
 */
void PcaTrain(VectorTransform *vtrans, const float *samples, int nSamples, const float *mean);
void PcaTransform(const VectorTransform *vtrans, const float *vec, float *transvec);
/* unified entry point: dispatches on vtrans->type to Rom / Fht / Pca */
void VtTransform(VectorTransform *vtrans, const float *vec, float *transvec);
/* byte stream = mean[dimIn] then M[dimOut][dimIn], raw floats, no header */
size_t PcaSerializeSize(int dimIn, int dimOut);
void PcaSerialize(const VectorTransform *vtrans, char *out, size_t outSize);
/* one allocation (struct + mean + M) in CurrentMemoryContext; release with FreeTransformer or pfree */
VectorTransform *PcaDeserialize(int dimIn, int dimOut, const char *bytes);


#endif