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
        securec_check_c(rc , "\0", "\0");
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
        for (int idim = d8; idim < dim; idim ++) {
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

void ComputeRbqDistanceBatch4(const RabitqQueryParams *params, RabitqVector **eVec, float *out)
{
    int dim = params->dim;
    int qb = params->rbqConfig->rbqQueryBits;
    QueryRabitqVector *qVec = params->qrbqVec;
    int funcType = params->funcType;
    uint8 *codes[VECTOR_RBQ_BATCH_SIZE] = {eVec[0]->data, eVec[1]->data, eVec[2]->data, eVec[3]->data};
    float dots[VECTOR_RBQ_BATCH_SIZE];
    VectorRbqDpPopcntBatch4(dim, qb, qVec->data, codes, dots);
    for (int j = 0; j < VECTOR_RBQ_BATCH_SIZE; j++) {
        FactorData fac = eVec[j]->fac;
        QueryFactorData qfac = qVec->fac;
        float xbDotQu = dots[j];
        float finalDot = qfac.cof1 * xbDotQu + qfac.cof2 * fac.xbSum - qfac.cof34;

        /*
         * L2: distance = ||or-c||^2 + ||qr-c||^2 - 2*||or-c||*||qr-c||*<q,o>
         * IP: distance = ||or-c||^2 + ||qr-c||^2 - 2*||or-c||*||qr-c||*<q,o> - ||or||^2
         */
        float distance = fac.orMinusCL2Sqr + qfac.qrMinusCL2Sqr - 2 * fac.dpMultiplier * finalDot;

        if (funcType != DIS_L2) {
            /* -<or,q> = (||or-q||^2 - ||q||^2 - ||or||^2) / 2 */
            out[j] = (distance - qfac.qrNormL2Sqr) * 0.5f;
        } else {
            out[j] = distance;
        }
    }
}

/* ------------------------------------------------------------------------
 * 1 / 2-bit codes over a transformed space (see rabitq.h)
 */

static inline void RbqSetPlaneBit(uint8 *plane, int i)
{
    plane[i / BITS_PER_BYTE] |= (uint8)(1u << (i % BITS_PER_BYTE));
}

/* popcount of one bit plane; whole qwords first, byte tail after (same walk as VectorRbqDpPopcnt) */
static uint32 RbqPlanePopcount(const uint8 *plane, int dim)
{
    int nbytes = rbqPlaneBytes(dim);
    int step = (int)sizeof(uint64);
    int nAligned = (nbytes / step) * step;
    uint32 count = 0;
    for (int off = 0; off < nAligned; off += step) {
        count += (uint32)__builtin_popcountll(*(const uint64 *)(plane + off));
    }
    for (int off = nAligned; off < nbytes; off++) {
        count += (uint32)__builtin_popcount(plane[off]);
    }
    return count;
}

static uint32 RbqPlaneHamming(const uint8 *a, const uint8 *b, int dim)
{
    int nbytes = rbqPlaneBytes(dim);
    int step = (int)sizeof(uint64);
    int nAligned = (nbytes / step) * step;
    uint32 count = 0;
    for (int off = 0; off < nAligned; off += step) {
        count += (uint32)__builtin_popcountll(*(const uint64 *)(a + off) ^ *(const uint64 *)(b + off));
    }
    for (int off = nAligned; off < nbytes; off++) {
        count += (uint32)__builtin_popcount((uint32)(a[off] ^ b[off]));
    }
    return count;
}

/*
 * <y_q, s> / sqrt(D) for one sign plane s in {-1, +1}^D from the qb query
 * planes: sum over the set bits of s of the quantized y_q, doubled, minus the
 * quantized sum of y_q (cof1 / cof2 / cof34 of SetRBQQuery carry 1 / sqrt(D)).
 */
static inline float RbqPlaneQueryDot(int dim, int qb, const QueryRabitqVector *qVec, const uint8 *plane)
{
    float xbDotQu = VectorRbqDpPopcnt(dim, qb, (uint8 *)qVec->data, (uint8 *)plane);
    float xbSum = (float)RbqPlanePopcount(plane, dim);
    return qVec->fac.cof1 * xbDotQu + qVec->fac.cof2 * xbSum - qVec->fac.cof34;
}

static float RbqFillInputFactors(const RbqBitsArgs *args, FactorDataBits *fac, double normSqr)
{
    float xcSqr = (float)normSqr;
    double ipMu = 0;
    if (args->mean != NULL) {
        double acc = 0;
        for (int c = 0; c < args->dimIn; c++) {
            float d = args->x[c] - args->mean[c];
            acc += (double)d * d;
        }
        xcSqr = (float)acc;
        if (args->funcType == DIS_IP) {
            for (int c = 0; c < args->dimIn; c++) {
                ipMu += (double)args->x[c] * args->mean[c];
            }
        }
    }
    fac->normSqr = (float)normSqr;
    fac->ipMu = (float)ipMu;
    fac->resSqr = (float)Max((double)xcSqr - normSqr, 0.0);
    return xcSqr;
}

static void RbqEncodeTwoBit(const RbqBitsArgs *args, RabitqVectorBits *rbqVec, double normSqr, double sqrtD)
{
    FactorDataBits *fac = &rbqVec->fac;
    int dim = args->dim;
    if (normSqr < 1e-24 || dim <= 1) {
        fac->dpMul = 0.0f;
        return;
    }
    double rms = sqrt(normSqr / (double)dim);
    if (rms == 0.0) {
        fac->dpMul = 0.0f;
        return;
    }
    uint8 *hi = rbqVec->data;
    uint8 *lo = rbqVec->data + rbqPlaneBytes(dim);
    double recDot = 0;
    for (int i = 0; i < dim; i++) {
        double z = args->y[i] / rms;
        bool positive = z > 0;
        bool large = fabs(z) > (double)RBQ_2BIT_ALPHA;
        bool loBit = positive == large;
        if (positive) {
            RbqSetPlaneBit(hi, i);
        }
        if (loBit) {
            RbqSetPlaneBit(lo, i);
        }
        double level = (positive ? 1.0 : -1.0) *
                       (large ? (RBQ_2BIT_ALPHA + RBQ_2BIT_BETA) : (RBQ_2BIT_ALPHA - RBQ_2BIT_BETA));
        recDot += (double)args->y[i] * level;
    }
    fac->dpMul = (recDot < 1e-12) ? 0.0f : (float)(normSqr * sqrtD / recDot);
}

static void RbqEncodeOneBit(const RbqBitsArgs *args, RabitqVectorBits *rbqVec, double normSqr, double sqrtD)
{
    FactorDataBits *fac = &rbqVec->fac;
    double sumAbs = 0;
    for (int i = 0; i < args->dim; i++) {
        double v = args->y[i];
        sumAbs += fabs(v);
        if (v > 0) {
            RbqSetPlaneBit(rbqVec->data, i);
        }
    }
    fac->dpMul = (sumAbs < 1e-12 || args->dim <= 1) ? 0.0f : (float)(normSqr * sqrtD / sumAbs);
}

float ComputeVectorRBQCodeBits(const RbqBitsArgs *args, RabitqVectorBits *rbqVec)
{
    FactorDataBits *fac = &rbqVec->fac;
    Size codeBytes = rbqCodeBytesBits(args->dim, args->bits);
    errno_t rc = memset_s(rbqVec->data, codeBytes, 0, codeBytes);
    if (rc != EOK) {
        securec_check(rc, "\0", "\0");
    }

    double normSqr = 0;
    for (int i = 0; i < args->dim; i++) {
        double v = args->y[i];
        normSqr += v * v;
    }

    float xcSqr = RbqFillInputFactors(args, fac, normSqr);
    double sqrtD = sqrt((double)args->dim);
    if (args->bits >= RBQ_TWO_BIT) {
        RbqEncodeTwoBit(args, rbqVec, normSqr, sqrtD);
    } else {
        RbqEncodeOneBit(args, rbqVec, normSqr, sqrtD);
    }
    return xcSqr;
}

void SetRBQQueryBits(const RbqBitsArgs *args, QueryRabitqVector *qrbqVec)
{
    Size planes = (Size)rbqPlaneBytes(args->dim) * (Size)args->qb;
    errno_t rc = memset_s(qrbqVec->data, planes, 0, planes);
    if (rc != EOK) {
        securec_check(rc, "\0", "\0");
    }
    SetRBQQuery(args->dim, args->qb, (float *)args->y, qrbqVec, NULL, DIS_L2);

    float ipConst = 0.0f;
    if (args->funcType == DIS_IP && args->mean != NULL) {
        double qMu = 0;
        double muMu = 0;
        for (int c = 0; c < args->dimIn; c++) {
            qMu += (double)args->x[c] * args->mean[c];
            muMu += (double)args->mean[c] * args->mean[c];
        }
        ipConst = (float)(qMu - muMu);
    }
    qrbqVec->fac.qrNormL2Sqr = ipConst;
}

float ComputeRbqDistanceBits(const RbqBitsArgs *args, const RabitqVectorBits *eVec, const QueryRabitqVector *qVec)
{
    const uint8 *hi = eVec->data;
    float dot = RbqPlaneQueryDot(args->dim, args->qb, qVec, hi);
    if (args->bits >= RBQ_TWO_BIT) {
        const uint8 *lo = hi + rbqPlaneBytes(args->dim);
        dot = RBQ_2BIT_ALPHA * dot + RBQ_2BIT_BETA * RbqPlaneQueryDot(args->dim, args->qb, qVec, lo);
    }
    float est = eVec->fac.dpMul * dot;

    if (args->funcType == DIS_IP) {
        return -(est + eVec->fac.ipMu + qVec->fac.qrNormL2Sqr);
    }
    return eVec->fac.normSqr + qVec->fac.qrMinusCL2Sqr - 2.0f * est;
}

float ComputeRbqCodeDistanceBits(int dim, int bits, const RabitqVectorBits *a, const RabitqVectorBits *b)
{
    float d = (float)dim;
    const uint8 *ahi = a->data;
    const uint8 *bhi = b->data;
    float hh = d - 2.0f * (float)RbqPlaneHamming(ahi, bhi, dim);
    float dot;

    if (bits >= RBQ_TWO_BIT) {
        const uint8 *alo = ahi + rbqPlaneBytes(dim);
        const uint8 *blo = bhi + rbqPlaneBytes(dim);
        float hl = d - 2.0f * (float)RbqPlaneHamming(ahi, blo, dim);
        float lh = d - 2.0f * (float)RbqPlaneHamming(alo, bhi, dim);
        float ll = d - 2.0f * (float)RbqPlaneHamming(alo, blo, dim);
        dot = (RBQ_2BIT_ALPHA * RBQ_2BIT_ALPHA * hh + RBQ_2BIT_ALPHA * RBQ_2BIT_BETA * (hl + lh) +
               RBQ_2BIT_BETA * RBQ_2BIT_BETA * ll) /
              d;
    } else {
        dot = hh / d;
    }
    return a->fac.normSqr + b->fac.normSqr - 2.0f * a->fac.dpMul * b->fac.dpMul * dot;
}