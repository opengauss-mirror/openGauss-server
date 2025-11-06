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
 * rabitq.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/datavec/rabitq.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <float.h>
#include <math.h>

#include "access/datavec/rabitq.h"

void ComputeVectorRBQCode(int dim, float *vec, RabitqVector *rbqVec, float *centroid, int funcType)
{
    FactorData *fac = &rbqVec->fac;
    uint8 *code = rbqVec->data;
    int alignedDim = (dim + 7) / 8;
    float invSqrtD = dim == 0 ? 1.0f : (1.0f / sqrt(dim));
    float normL2Sqr = 0;
    float orL2Sqr = 0;
    int xbSum = 0;
    float dpOo = 0;

    if (code != NULL) {
        errno_t rc = memset_s(code, alignedDim, 0, alignedDim);
        securec_check_c(rc, "\0", "\0");
    }

    for (int i = 0; i < dim; i++) {
        float orMinusC = vec[i] - (centroid == NULL ? 0 : centroid[i]);
        normL2Sqr += orMinusC * orMinusC;
        orL2Sqr += vec[i] * vec[i];
        int xb = orMinusC > 0 ? 1 : 0;
        xbSum += xb;
        dpOo += xb ? orMinusC : (-orMinusC);
        // compute rbq code
        if (xb) {
            code[i / 8] |= (1 << (i % 8));
        }
    }

    float invNormL2 = (fabsf(normL2Sqr) < FLT_EPSILON ? 1.0f : (1.0f / sqrtf(normL2Sqr)));
    dpOo = dpOo * invNormL2 * invSqrtD;
    float invDpOo = (fabsf(dpOo) < FLT_EPSILON ? 1.0f : (1.0f / sqrtf(dpOo)));

    fac->orMinusCL2Sqr = normL2Sqr;
    if (funcType != DIS_L2) {
        fac->orMinusCL2Sqr -= orL2Sqr;
    }
    fac->xbSum = xbSum;
    fac->dpMultiplier = sqrtf(normL2Sqr) * invDpOo;
}

void SetRBQQuery(int dim, int qb, float *vec, QueryRabitqVector *qrbqVec, float *centroid, int funcType)
{
    QueryFactorData *qfac = &qrbqVec->fac;
    uint8 *qData = qrbqVec->data;
    float invD = (dim == 0) ? 1.0f : (1.0f / sqrt(dim));
    float qrNormL2Sqr = 0;
    size_t sumQU = 0;
    float *qrMinusCVec;
    uint8 *quVec = (uint8 *)palloc(dim);

    if (centroid == NULL || funcType != DIS_L2) {
        for (int i = 0; i < dim; i++) {
            qrNormL2Sqr += vec[i] * vec[i];
        }
    }

    if (centroid == NULL) {
        qfac->qrMinusCL2Sqr = qrNormL2Sqr;
        qrMinusCVec = vec;
    } else {
        qfac->qrMinusCL2Sqr = VectorL2SquaredDistance(dim, vec, centroid);
        qrMinusCVec = (float *)palloc(sizeof(float) * dim);
        for (int i = 0; i < dim; i++) {
            qrMinusCVec[i] = vec[i] - centroid[i];
        }
    }

    float vMin = FLT_MAX;
    float vMax = -FLT_MAX;
    for (int i = 0; i < dim; i++) {
        float vq = qrMinusCVec[i];
        vMin = vMin < vq ? vMin : vq;
        vMax = vMax > vq ? vMax : vq;
    }

    float qbBits = (1 << qb) - 1;
    float width = (vMax - vMin) / qbBits;
    float invWidth = 1.0f / width;

    for (int i = 0; i < dim; i++) {
        float vq = qrMinusCVec[i];
        int vqu = (int)round((vq - vMin) * invWidth);
        quVec[i] = vqu < 0 ? 0 : (vqu > 255 ? 255 : vqu);
        sumQU += vqu;
    }

    /* SQ-encoded query vector */
    int offset = (dim + 7) / 8;
    int d8 = (dim / 8) * 8;
    for (int i = 0; i < qb; i++) {
        for (int idim = 0; idim < d8; idim += 8) {
            uint8 value = 0;
            for (int ldim = 0; ldim < 8; ldim++) {
                bool bit = ((quVec[idim + ldim] & (1 << i)) != 0);
                value |= bit ? (1 << ldim % 8) : 0;
            }
            qData[i * offset + idim / 8] = value;
        }
        for (int idim = d8; idim < dim; idim++) {
            bool bit = ((quVec[idim] & (1 << i)) != 0);
            qData[i * offset + idim / 8] |= bit ? (1 << idim % 8) : 0;
        }
    }

    qfac->cof1 = 2 * width * invD;
    qfac->cof2 = 2 * vMin * invD;
    qfac->cof34 = invD * (width * sumQU + dim * vMin);

    if (funcType != DIS_L2) {
        qfac->qrNormL2Sqr = qrNormL2Sqr;
    }
}

float ComputeRbqDistance(int dim, int qb, RabitqVector *eVec, QueryRabitqVector *qVec, int funcType)
{
    FactorData fac = eVec->fac;
    uint8 *edata = eVec->data;
    QueryFactorData qfac = qVec->fac;
    uint8 *qdata = qVec->data;
    
    float xbDotQu = VectorRbqDpPopcnt(dim, qb, qdata, edata);
    float finalDot = qfac.cof1 * xbDotQu + qfac.cof2 * fac.xbSum - qfac.cof34;

    /*
     * L2: distance = ||or-c||^2 + ||qr-c||^2 - 2*||or-c||*||qr-c||*<q,o>
     * IP: distance = ||or-c||^2 + ||qr-c||^2 - 2*||or-c||*||qr-c||*<q,o> - ||or||^2
     */
    float distance = fac.orMinusCL2Sqr + qfac.qrMinusCL2Sqr - 2 * fac.dpMultiplier * finalDot;

    if (funcType != DIS_L2) {
        /* -<or,q> = (||or-q||^2 - ||q||^2 - ||or||^2) / 2 */
        return (distance - qfac.qrNormL2Sqr) * 0.5f;
    } else {
        return distance;
    }
}