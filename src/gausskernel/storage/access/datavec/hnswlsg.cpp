/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-server is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 *
 * -------------------------------------------------------------------------
 *
 * hnswlsg.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/datavec/hnswlsg.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <float.h>
#include <math.h>

#include "access/datavec/hnswlsg.h"

#define FP16_SIGN_BIT_MASK 0x8000
#define FP16_EXPONENT_MASK 0x7C00
#define FP16_EXPONENT_MAX 0x1F
#define FP16_MANTISSA_MASK  0x03FF

#define FP32_EXPONENT_BIAS 127
#define FP16_EXPONENT_BIAS 15
#define EXPONENT_BIAS_ADJUSTMENT (FP32_EXPONENT_BIAS - FP16_EXPONENT_BIAS)

#define FP32_SIGN_BIT_MASK 0x80000000
#define FP32_EXPONENT_MASK 0x7F800000
#define FP32_MANTISSA_MASK 0x007FFFFF

#define MANTISSA_SHIFT 13
#define FP32_EXPONENT_SHIFT 23
#define FP16_EXPONENT_SHIFT 10
#define FP32_SIGN_SHIFT 16

float16 Float32ToFloat16(float f32)
{
    uint32_t bits = *reinterpret_cast<uint32_t*>(&f32);
    
    uint16_t sign = (bits >> FP32_SIGN_SHIFT) & FP16_SIGN_BIT_MASK;
    int32_t exponent = ((bits >> FP32_EXPONENT_SHIFT) & 0xFF) - EXPONENT_BIAS_ADJUSTMENT;
    uint16_t mantissa = (bits >> MANTISSA_SHIFT) & FP16_MANTISSA_MASK;

    if (exponent > FP16_EXPONENT_MAX) {
        exponent = FP16_EXPONENT_MAX;
    } else if (exponent < 0) {
        exponent = 0;
    }

    return sign | (exponent << FP16_EXPONENT_SHIFT) | mantissa;
}

float Float16ToFloat32(float16 f16)
{
    uint32_t sign = (f16 & FP16_SIGN_BIT_MASK) << FP32_SIGN_SHIFT;
    uint32_t exponent = ((f16 >> FP16_EXPONENT_SHIFT) & FP16_EXPONENT_MAX) + EXPONENT_BIAS_ADJUSTMENT;
    uint32_t mantissa = (f16 & FP16_MANTISSA_MASK) << MANTISSA_SHIFT;
    
    uint32_t bits = sign | (exponent << FP32_EXPONENT_SHIFT) | mantissa;
    return *reinterpret_cast<float*>(&bits);
}


float InnerProductDistance(float* a, float* b, int dim)
{
    float sum = 0.0f;
    for (int i = 0; i < dim; ++i) {
        sum += a[i] * b[i];
    }
    return 1.0 - sum;
}

float NegativeInnerProductDistance(float* a, float* b, int dim)
{
    float sum = 0.0f;
    for (int i = 0; i < dim; ++i) {
        sum += a[i] * b[i];
    }
    return sum;
}

float CosineDistance(float* a, float* b, int dim)
{
    float dotProduct = 0.0f;
    float normA = 0.0f;
    float normB = 0.0f;
    float productAB = 0.0f;

    for (int i = 0; i < dim; ++i) {
        dotProduct += a[i] * b[i];
        normA += a[i] * a[i];
        normB += b[i] * b[i];
    }

    normA = sqrt(normA);
    normB = sqrt(normB);
    productAB = normA * normB;
    if (productAB == 0) {
        return 1.0f;
    } else {
        float cosSim = dotProduct / productAB;
        return 1.0f - cosSim;
    }
}

float SquaredL2Distance(float* a, float* b, int dim)
{
    float sum = 0.0f;
    for (int i = 0; i < dim; i++) {
        float diff = a[i] - b[i];
        sum += diff * diff;
    }
    return sqrt(sum);
}

float FindKnnMu(float* a, LsgCalculator* params)
{
    float distances[params->sampleSize];
    if (params->degree > params->sampleSize) {
        params->degree = params->sampleSize;
    }

    for (int i = 0; i < params->sampleSize; i++) {
        switch (params->distType) {
            case LSG_L2_DIST:
                distances[i] = SquaredL2Distance(a, params->sampleVecs + i * params->dim, params->dim);
                break;
            case LSG_IP_DIST:
                distances[i] = InnerProductDistance(a, params->sampleVecs + i * params->dim, params->dim);
                break;
            case LSG_COS_DIST:
                distances[i] = CosineDistance(a, params->sampleVecs + i * params->dim, params->dim);
                break;
            default:
                ereport(ERROR, (errmsg("LSG disttype invalid.")));
                break;
        }
    }

    for (int i = 0; i < params->degree; i++) {
        int minIndex = i;
        for (int j = i + 1; j < params->sampleSize; j++) {
            if (distances[j] < distances[minIndex]) {
                minIndex = j;
            }
        }
        float tempDist = distances[i];
        distances[i] = distances[minIndex];
        distances[minIndex] = tempDist;
    }

    float avgDistance = 0.0f;
    for (int i = 0; i < params->degree; i++) {
        avgDistance += sqrtf(distances[i]);
    }
    avgDistance = avgDistance / params->degree;
    return avgDistance;
}

float16 CalcIsoVal(float* a, LsgCalculator* params)
{
    float mu = FindKnnMu(a, params);
    if (mu == 0) {
        ereport(ERROR, (errmsg("HNSW LsgCalculator isolation is zero")));
    }
    return Float32ToFloat16(pow(1/mu, params->alpha));
}

void InitScalingParam(LsgCalculator* params, int sampleSize, int dim, float* inputSampleVecs, LsgDistType type,
                      int degree, float alpha)
{
    if (params == NULL || sampleSize <= 0 || dim <= 0 || degree < 0 || alpha < 0 || inputSampleVecs == nullptr) {
        ereport(ERROR, (errmsg("HNSW LocScalingParam init failed, parameter invalid.")));
        return;
    }
    params->sampleSize = sampleSize;
    params->dim = dim;
    params->distType = type;
    params->degree = degree;
    params->alpha = alpha;

    params->sampleVecs = (float*)palloc0(sizeof(float) * sampleSize * dim);
    for (int i = 0; i < sampleSize; i++) {
        errno_t rc =
            memcpy_s(params->sampleVecs + i * dim, dim * sizeof(float), inputSampleVecs + i * dim, dim * sizeof(float));
        securec_check(rc, "\0", "\0");
    }
}