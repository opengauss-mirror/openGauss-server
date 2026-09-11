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
 * vectortransformer.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/datavec/vectortransformer.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "access/datavec/vectortransformer.h"

#include <lapacke.h>
#include <cblas.h>

void FloatRandom(float* x, size_t n)
{
    const size_t nblock = n < 1024 ? 1 : 1024;

    RandomGenerator rng0(RANSEED);
    int a0 = rng0.rand_int();
    int b0 = rng0.rand_int();

    for (int64_t j = 0; j < nblock; j++) {
        RandomGenerator rng(a0 + j * b0);

        double a = 0;
        double b = 0;
        double s = 0;
        int state = 0;

        const size_t istart = j * n / nblock;
        const size_t iend = (j + 1) * n / nblock;

        for (size_t i = istart; i < iend; i++) {
            if (state == 0) {
                do {
                    a = 2.0 * rng.rand_double() - 1;
                    b = 2.0 * rng.rand_double() - 1;
                    s = a * a + b * b;
                } while (s >= 1.0);
                x[i] = a * sqrt(-2.0 * log(s) / s);
            } else
                x[i] = b * sqrt(-2.0 * log(s) / s);
            state = 1 - state;
        }
    }
}

/*
 * QR decomposition
 */
bool MatrixQR(int dim, float* x)
{
    float *tau = (float *)palloc0(dim * sizeof(float));
    int sgeqrfRes = LAPACKE_sgeqrf(LAPACK_ROW_MAJOR, dim, dim, x, dim, tau);
    if (sgeqrfRes != 0) {
        ereport(LOG, (errmsg("Error in sgeqrf when MatrixQR.")));
        pfree(tau);
        return false;
    }

    int sorgqrRes = LAPACKE_sorgqr(LAPACK_ROW_MAJOR, dim, dim, dim, x, dim, tau);
    if (sorgqrRes != 0) {
        ereport(LOG, (errmsg("Error in sorgqr when MatrixQR.")));
        pfree(tau);
        return false;
    }

    pfree(tau);
    return true;
}

void RomTrain(VectorTransform* vtrans)
{
    int dim = vtrans->dim;
    vtrans->matrix = (float *)palloc(sizeof(float) * dim * dim);
    for (int i = 0; i < MAX_RETRIES; i++) {
        FloatRandom(vtrans->matrix, dim * dim);
        if (MatrixQR(dim, vtrans->matrix)) {
            return;
        }
    }
    ereport(ERROR, (errmsg("MatrixQR has failed after %d attempts.", MAX_RETRIES)));
}

void RomTransform(VectorTransform* vtrans, const float* vec, float *transvec)
{
    int dim = vtrans->dim;
    cblas_sgemv(CblasRowMajor, CblasNoTrans, dim, dim, 1.0f, vtrans->matrix,
                dim, vec, 1, 0.0f, transvec, 1);
    
}

void *RomGetMatrix(VectorTransform* vtrans)
{
    return (void *)vtrans->matrix;
}

void RandomSigns(float* signs, int dim, RandomGenerator rng0)
{
    int a0 = rng0.rand_int();
    for (int i = 0; i < dim; i++) {
        int rand = rng0.rand_int();
        signs[i] = (rand % 2 == 0) ? FHT_POS_SIGN : FHT_NEG_SIGN;
    }
}

int CompareSwaps(const void* a, const void* b)
{
    Swap* s1 = (Swap*)a;
    Swap* s2 = (Swap*)b;
    if (s1->i < s2->i) {
        return -1;
    }
    if (s1->i > s2->i) {
        return 1;
    }
    return 0;
}

void RandomSwaps(Swap* swaps, int n, RandomGenerator rng0)
{
    int outCount = n / 2;

    int* p = (int*)palloc(n * sizeof(int));
    for (int i = 0; i < n; i++) {
        p[i] = i;
    }
    for (int i = n - 1; i > 0; i--) {
        int j = rng0.rand_int() % (i + 1);
        int temp = p[i];
        p[i] = p[j];
        p[j] = temp;
    }

    for (int s = 0; s < outCount; s++) {
        uint16_t a = (uint16_t)p[2 * s];
        uint16_t b = (uint16_t)p[2 * s + 1];
        if (a < b) {
            swaps[s].i = a;
            swaps[s].j = b;
        } else {
            swaps[s].i = b;
            swaps[s].j = a;
        }
    }

    qsort(swaps, outCount, sizeof(Swap), CompareSwaps);
    pfree(p);
}

size_t FhtTotalSize(int outputDim)
{
    size_t totalSize = 0;

    totalSize += MAXALIGN(sizeof(FastRotation));

    // Swap* swaps[rounds]
    totalSize += FHT_ROUND * sizeof(Swap*);

    // float* signs[rounds]
    totalSize += FHT_ROUND * sizeof(float*);

    // Swap* swaps[rounds][n_swaps]
    totalSize += MAXALIGN(FHT_ROUND * (outputDim / 2) * sizeof(Swap));

    // float signs[rounds][outputDim]
    totalSize += MAXALIGN(FHT_ROUND * outputDim * sizeof(float));

    return totalSize;
}

size_t FhtSerializeSize(int outputDim)
{
    size_t totalSize = sizeof(int);
    totalSize += FHT_ROUND * (outputDim / 2) * sizeof(Swap);
    totalSize += FHT_ROUND * outputDim * sizeof(float);
    totalSize = MAXALIGN(totalSize);
    return totalSize;
}

int FhtOutputDim(int inputDim)
{
    int outputDim = FHTBLOCK;
    while (outputDim < inputDim) {
        outputDim += FHTBLOCK;
    }
    return outputDim;
}

void FhtTrain(VectorTransform* vtrans)
{
    int inputDim = vtrans->dim;
    int outputDim = FhtOutputDim(inputDim);
    int nSwaps = outputDim / 2;

    size_t totalSize = FhtTotalSize(outputDim);

    FastRotation* fr = (FastRotation*)palloc0(totalSize);
    fr->outputDim = outputDim;
    fr->swaps = (Swap**)((char*)fr + sizeof(FastRotation));
    fr->signs = (float**)((char*)fr->swaps + FHT_ROUND * sizeof(Swap*));

    char *swapsPtr = (char*)fr->signs + FHT_ROUND * sizeof(float*);
    for (int i = 0; i < FHT_ROUND; i++) {
        RandomGenerator rng0(RANSEED + i);
        fr->swaps[i] = (Swap*)(swapsPtr + i * nSwaps * sizeof(Swap));
        RandomSwaps(fr->swaps[i], outputDim, rng0);
    }

    char *signsPtr = swapsPtr + FHT_ROUND * nSwaps * sizeof(Swap);
    for (int i = 0; i < FHT_ROUND; i++) {
        RandomGenerator rng0(RANSEED + i);
        fr->signs[i] = (float*)(signsPtr + i * outputDim * sizeof(float));
        RandomSigns(fr->signs[i], outputDim, rng0);
    }
    vtrans->fastRotation = fr;
}

/* The 16-dimensional Walsh-Hadamard Transform is
 * implemented via 4 rounds od butterfly operations.
 */
void FastWalshHadamardTransform16(float* x, float normalize)
{
    float x0 = normalize * x[0];
    float x1 = normalize * x[1];
    float x2 = normalize * x[2];
    float x3 = normalize * x[3];
    float x4 = normalize * x[4];
    float x5 = normalize * x[5];
    float x6 = normalize * x[6];
    float x7 = normalize * x[7];
    float x8 = normalize * x[8];
    float x9 = normalize * x[9];
    float x10 = normalize * x[10];
    float x11 = normalize * x[11];
    float x12 = normalize * x[12];
    float x13 = normalize * x[13];
    float x14 = normalize * x[14];
    float x15 = normalize * x[15];

    // k = 1, k = 2
    float t0 = x0 + x1;
    x1 = x0 - x1;
    x0 = t0;
    float t1 = x2 + x3;
    x3 = x2 - x3;
    x2 = t1;
    t0 = x0 + x2;
    x2 = x0 - x2;
    x0 = t0;
    t1 = x1 + x3;
    x3 = x1 - x3;
    x1 = t1;

    t0 = x4 + x5;
    x5 = x4 - x5;
    x4 = t0;
    t1 = x6 + x7;
    x7 = x6 - x7;
    x6 = t1;
    t0 = x4 + x6;
    x6 = x4 - x6;
    x4 = t0;
    t1 = x5 + x7;
    x7 = x5 - x7;
    x5 = t1;

    // k = 3
    t0 = x0 + x4;
    x4 = x0 - x4;
    x0 = t0;
    t1 = x1 + x5;
    x5 = x1 - x5;
    x1 = t1;
    t0 = x2 + x6;
    x6 = x2 - x6;
    x2 = t0;
    t1 = x3 + x7;
    x7 = x3 - x7;
    x3 = t1;

    t0 = x8 + x9;
    x9 = x8 - x9;
    x8 = t0;
    t1 = x10 + x11;
    x11 = x10 - x11;
    x10 = t1;
    t0 = x8 + x10;
    x10 = x8 - x10;
    x8 = t0;
    t1 = x9 + x11;
    x11 = x9 - x11;
    x9 = t1;

    t0 = x12 + x13;
    x13 = x12 - x13;
    x12 = t0;
    t1 = x14 + x15;
    x15 = x14 - x15;
    x14 = t1;
    t0 = x12 + x14;
    x14 = x12 - x14;
    x12 = t0;
    t1 = x13 + x15;
    x15 = x13 - x15;
    x13 = t1;

    t0 = x8 + x12;
    x12 = x8 - x12;
    x8 = t0;
    t1 = x9 + x13;
    x13 = x9 - x13;
    x9 = t1;
    t0 = x10 + x14;
    x14 = x10 - x14;
    x10 = t0;
    t1 = x11 + x15;
    x15 = x11 - x15;
    x11 = t1;

    // k = 4
    t0 = x0 + x8;
    x8 = x0 - x8;
    x0 = t0;
    t1 = x1 + x9;
    x9 = x1 - x9;
    x1 = t1;
    t0 = x2 + x10;
    x10 = x2 - x10;
    x2 = t0;
    t1 = x3 + x11;
    x11 = x3 - x11;
    x3 = t1;
    t0 = x4 + x12;
    x12 = x4 - x12;
    x4 = t0;
    t1 = x5 + x13;
    x13 = x5 - x13;
    x5 = t1;
    t0 = x6 + x14;
    x14 = x6 - x14;
    x6 = t0;
    t1 = x7 + x15;
    x15 = x7 - x15;
    x7 = t1;

    x[0] = x0;
    x[1] = x1;
    x[2] = x2;
    x[3] = x3;
    x[4] = x4;
    x[5] = x5;
    x[6] = x6;
    x[7] = x7;
    x[8] = x8;
    x[9] = x9;
    x[10] = x10;
    x[11] = x11;
    x[12] = x12;
    x[13] = x13;
    x[14] = x14;
    x[15] = x15;
}


void FastWalshHadamardTransform64(float* x)
{
    const float normalize = 0.125f;
    const int batchNum1 = 16;
    const int batchNum2 = 32;

    FastWalshHadamardTransform16(&x[0], normalize);
    FastWalshHadamardTransform16(&x[16], normalize);

    for (int i = 0; i < batchNum1; i++) {
        float t = x[i] + x[batchNum1 + i];
        x[batchNum1 + i] = x[i] - x[batchNum1 + i];
        x[i] = t;
    }

    FastWalshHadamardTransform16(&x[32], normalize);
    FastWalshHadamardTransform16(&x[48], normalize);

    for (int i = 32; i < 48; i++) {
        float t = x[i] + x[batchNum1 + i];
        x[batchNum1 + i] = x[i] - x[batchNum1 + i];
        x[i] = t;
    }

    for (int i = 0; i < batchNum2; i++) {
        float t = x[i] + x[batchNum2 + i];
        x[batchNum2 + i] = x[i] - x[batchNum2 + i];
        x[i] = t;
    }
}

void Block64Fwht256(float* x)
{
    const float normalize = 0.0625f;
    const int batchNum1 = 16;
    const int batchNum2 = 32;
    FastWalshHadamardTransform16(&x[0], normalize);
    FastWalshHadamardTransform16(&x[16], normalize);

    for (int i = 0; i < batchNum1; i++) {
        float t = x[i] + x[batchNum1 + i];
        x[batchNum1 + i] = x[i] - x[batchNum1 + i];
        x[i] = t;
    }

    FastWalshHadamardTransform16(&x[32], normalize);
    FastWalshHadamardTransform16(&x[48], normalize);

    for (int i = 32; i < 48; i++) {
        float t = x[i] + x[batchNum1 + i];
        x[batchNum1 + i] = x[i] - x[batchNum1 + i];
        x[i] = t;
    }

    for (int i = 0; i < 32; i++) {
        float t = x[i] + x[batchNum2 + i];
        x[batchNum2 + i] = x[i] - x[batchNum2 + i];
        x[i] = t;
    }
}

void FastWalshHadamardTransform256(float* x)
{
    const int batchNum1 = 64;
    const int batchNum2 = 128;
    Block64Fwht256(&x[0]);
    Block64Fwht256(&x[64]);

    for (int i = 0; i < batchNum1; i++) {
        float t = x[i] + x[batchNum1 + i];
        x[batchNum1 + i] = x[i] - x[batchNum1 + i];
        x[i] = t;
    }

    Block64Fwht256(&x[128]);
    Block64Fwht256(&x[192]);

    for (int i = 128; i < 192; i++) {
        float t = x[i] + x[batchNum1 + i];
        x[batchNum1 + i] = x[i] - x[batchNum1 + i];
        x[i] = t;
    }

    for (int i = 0; i < batchNum2; i++) {
        float t = x[i] + x[batchNum2 + i];
        x[batchNum2 + i] = x[i] - x[batchNum2 + i];
        x[i] = t;
    }
}

void FhtTransform(VectorTransform* vtrans, const float* vec, float *transvec)
{
    FastRotation *fr = vtrans->fastRotation;
    size_t inputSize = vtrans->dim * sizeof(float);
    float* rx = (float*)palloc0(fr->outputDim * sizeof(float));
    errno_t rc = memcpy_s(rx, inputSize, vec, inputSize);
    securec_check_c(rc, "\0", "\0");
    int swapCount = fr->outputDim / 2;

    for (int round = 0; round < FHT_ROUND; round++) {
        for (int s = 0; s < swapCount; s++) {
            uint16_t i = fr->swaps[round][s].i;
            uint16_t j = fr->swaps[round][s].j;
            float temp = rx[i];
            rx[i] = fr->signs[round][i] * rx[j];
            rx[j] = fr->signs[round][j] * temp;
        }

        int pos = 0;
        while (pos < fr->outputDim) {
            if (fr->outputDim - pos >= 256) {
                FastWalshHadamardTransform256(&rx[pos]);
                pos += 256;
            } else {
                FastWalshHadamardTransform64(&rx[pos]);
                pos += 64;
            }
        }
    }
    rc = memcpy_s(transvec, inputSize, vec, inputSize);
    securec_check_c(rc, "\0", "\0");
    pfree(rx);
}

void *FhtGetMatrix(VectorTransform* vtrans)
{
    FastRotation *fr = vtrans->fastRotation;
    int outputDim = fr->outputDim;
    size_t swapSize = FHT_ROUND * (outputDim / 2) * sizeof(Swap);
    size_t signsSize = FHT_ROUND * outputDim * sizeof(float);

    void *matrix = (void *)palloc(FhtSerializeSize(outputDim));
    errno_t rc = memcpy_s(matrix, sizeof(int), &fr->outputDim, sizeof(int));
    securec_check_c(rc, "\0", "\0");
    rc = memcpy_s(matrix + sizeof(int), swapSize, (char*)fr->swaps[0], swapSize);
    securec_check_c(rc, "\0", "\0");
    rc = memcpy_s(matrix + sizeof(int) + swapSize, signsSize, (char*)fr->signs[0], signsSize);
    securec_check_c(rc, "\0", "\0");
    return matrix;
}

FastRotation *FhtDeserialize(void *rbq)
{
    int outputDim = *((int *)rbq);
    size_t totalSize = FhtTotalSize(outputDim);
    int nSwaps = outputDim / 2;
    size_t swapSize = FHT_ROUND * nSwaps * sizeof(Swap);
    size_t signsSize = FHT_ROUND * outputDim * sizeof(float);

    FastRotation* fr = (FastRotation*)palloc0(totalSize);
    fr->outputDim = outputDim;
    fr->swaps = (Swap**)((char*)fr + MAXALIGN(sizeof(FastRotation)));
    fr->signs = (float**)((char*)fr->swaps + FHT_ROUND * sizeof(Swap*));

    char *swapsPtr = (char*)fr->signs + FHT_ROUND * sizeof(float*);
    for (int i = 0; i < FHT_ROUND; i++) {
        fr->swaps[i] = (Swap*)(swapsPtr + i * nSwaps * sizeof(Swap));
    }

    char *signsPtr = swapsPtr + FHT_ROUND * nSwaps * sizeof(Swap);
    for (int i = 0; i < FHT_ROUND; i++) {
        fr->signs[i] = (float*)(signsPtr + i * outputDim * sizeof(float));
    }

    errno_t rc = memcpy_s(fr->swaps[0], swapSize, (char*)rbq + sizeof(int), swapSize);
    securec_check_c(rc, "\0", "\0");
    rc = memcpy_s(fr->signs[0], signsSize, (char*)rbq + sizeof(int) + swapSize, signsSize);
    securec_check_c(rc, "\0", "\0");

    return fr;
}

void FreeTransformer(VectorTransform *vt)
{
    if (vt == NULL) {
        return;
    }
    if (vt->type == PCA_ORTHOGONAL) {
        /* mean + matrix share one block; PcaDeserialize places it right behind the struct */
        if (vt->mean != NULL && vt->mean != (float *)(vt + 1)) {
            pfree(vt->mean);
        }
        pfree(vt);
        return;
    }
    if (vt->matrix != NULL) {
        pfree(vt->matrix);
    }
    if (vt->fastRotation != NULL) {
        pfree(vt->fastRotation);
    }
    pfree(vt);
}

/* ------------------------------------------------------------ PCA_ORTHOGONAL */

#define PCA_COV_CHUNK 2048 /* rows converted to f64 per dgemm call */
#define PCA_MIN_EXTRA_SAMPLES 8

static void PcaBindStorage(VectorTransform *vtrans, float *block, int dimIn, int dimOut)
{
    vtrans->type = PCA_ORTHOGONAL;
    vtrans->dim = dimIn;
    vtrans->dimIn = dimIn;
    vtrans->dimOut = dimOut;
    vtrans->fastRotation = NULL;
    vtrans->mean = block;
    vtrans->matrix = block + dimIn;
}

static void PcaFlipRowSign(float *projRow, int d)
{
    for (int c = 0; c < d; c++) {
        if (fabsf(projRow[c]) <= 1e-6f) {
            continue;
        }
        if (projRow[c] < 0) {
            for (int k = 0; k < d; k++) {
                projRow[k] = -projRow[k];
            }
        }
        break;
    }
}

/*
 * proj[dimOut][dimIn] = top principal components of the centered samples:
 * covariance accumulated in f64 chunk by chunk, dsyev, largest eigenvalues
 * first, each row sign-normalized (first non-negligible entry positive) so
 * the result is reproducible across LAPACK builds.
 */
static void PcaComponents(const float *samples, int nSamples, const VectorTransform *vtrans, float *proj)
{
    int d = vtrans->dimIn;
    int dimOut = vtrans->dimOut;
    const float *mean = vtrans->mean;
    double *cov = (double *)palloc0(sizeof(double) * (Size)d * d);
    double *chunk = (double *)palloc(sizeof(double) * (Size)PCA_COV_CHUNK * d);
    int pos = 0;
    while (pos < nSamples) {
        int m = Min(PCA_COV_CHUNK, nSamples - pos);
        for (int i = 0; i < m; i++) {
            const float *x = samples + (Size)(pos + i) * d;
            double *row = chunk + (Size)i * d;
            for (int c = 0; c < d; c++) {
                float xc = x[c] - mean[c];
                row[c] = (double)xc;
            }
        }
        cblas_dgemm(CblasRowMajor, CblasTrans, CblasNoTrans, d, d, m, 1.0, chunk, d, chunk, d, 1.0, cov, d);
        pos += m;
    }
    pfree(chunk);

    double *eigval = (double *)palloc(sizeof(double) * (Size)d);
    int info = LAPACKE_dsyev(LAPACK_ROW_MAJOR, 'V', 'U', d, cov, d, eigval);
    if (info != 0) {
        ereport(ERROR, (errmsg("PCA eigen-decomposition failed (dsyev info=%d)", info)));
    }

    for (int r = 0; r < dimOut; r++) {
        int col = d - 1 - r;
        float *projRow = proj + (Size)r * d;
        for (int c = 0; c < d; c++) {
            projRow[c] = (float)cov[(Size)c * d + col];
        }
        PcaFlipRowSign(projRow, d);
    }
    pfree(eigval);
    pfree(cov);
}

void PcaTrain(VectorTransform *vtrans, const float *samples, int nSamples, const float *mean)
{
    int dimIn = vtrans->dimIn;
    int dimOut = vtrans->dimOut;
    if (dimOut < 1 || dimOut > dimIn) {
        ereport(ERROR, (errmsg("PCA output dimension %d must lie in [1, %d]", dimOut, dimIn)));
    }
    if (dimOut < dimIn && nSamples < dimOut + PCA_MIN_EXTRA_SAMPLES) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
                        errmsg("%d sample rows are too few to train a %d-dimension PCA", nSamples, dimOut)));
    }

    float *block = (float *)palloc0(PcaSerializeSize(dimIn, dimOut));
    PcaBindStorage(vtrans, block, dimIn, dimOut);

    if (mean != NULL) {
        errno_t rc = memcpy_s(vtrans->mean, sizeof(float) * (Size)dimIn, mean, sizeof(float) * (Size)dimIn);
        if (rc != EOK) {
            securec_check(rc, "\0", "\0");
        }
    } else if (nSamples > 0) {
        double *acc = (double *)palloc0(sizeof(double) * (Size)dimIn);
        for (int i = 0; i < nSamples; i++) {
            const float *x = samples + (Size)i * dimIn;
            for (int c = 0; c < dimIn; c++) {
                acc[c] += x[c];
            }
        }
        for (int c = 0; c < dimIn; c++) {
            vtrans->mean[c] = (float)(acc[c] / nSamples);
        }
        pfree(acc);
    }

    VectorTransform rot;
    rot.type = RANDOM_ORTHOGONAL;
    rot.dim = dimOut;
    rot.matrix = NULL;
    rot.fastRotation = NULL;
    RomTrain(&rot);

    if (dimOut == dimIn) {
        errno_t rc = memcpy_s(vtrans->matrix, sizeof(float) * (Size)dimOut * dimIn, rot.matrix,
                              sizeof(float) * (Size)dimOut * dimIn);
        if (rc != EOK) {
            securec_check(rc, "\0", "\0");
        }
        pfree(rot.matrix);
        return;
    }

    float *proj = (float *)palloc(sizeof(float) * (Size)dimOut * dimIn);
    PcaComponents(samples, nSamples, vtrans, proj);
    cblas_sgemm(CblasRowMajor, CblasNoTrans, CblasNoTrans, dimOut, dimIn, dimOut, 1.0f, rot.matrix, dimOut, proj, dimIn,
                0.0f, vtrans->matrix, dimIn);
    pfree(rot.matrix);
    pfree(proj);
}

void PcaTransform(const VectorTransform *vtrans, const float *vec, float *transvec)
{
    int dimIn = vtrans->dimIn;
    float *xc = (float *)palloc(sizeof(float) * (Size)dimIn);
    for (int c = 0; c < dimIn; c++) {
        xc[c] = vec[c] - vtrans->mean[c];
    }
    cblas_sgemv(CblasRowMajor, CblasNoTrans, vtrans->dimOut, dimIn, 1.0f, vtrans->matrix, dimIn, xc, 1, 0.0f,
                transvec, 1);
    pfree(xc);
}

void VtTransform(VectorTransform *vtrans, const float *vec, float *transvec)
{
    switch (vtrans->type) {
        case RANDOM_ORTHOGONAL:
            RomTransform(vtrans, vec, transvec);
            break;
        case FAST_HTRANSFORM:
            FhtTransform(vtrans, vec, transvec);
            break;
        case PCA_ORTHOGONAL:
            PcaTransform(vtrans, vec, transvec);
            break;
        default:
            ereport(ERROR, (errmsg("unknown vector transform type %d", (int)vtrans->type)));
    }
}

size_t PcaSerializeSize(int dimIn, int dimOut)
{
    return sizeof(float) * ((size_t)dimIn + (size_t)dimOut * dimIn);
}

void PcaSerialize(const VectorTransform *vtrans, char *out, size_t outSize)
{
    size_t meanBytes = sizeof(float) * (size_t)vtrans->dimIn;
    size_t mBytes = sizeof(float) * (size_t)vtrans->dimOut * vtrans->dimIn;
    if (outSize < meanBytes + mBytes) {
        ereport(ERROR, (errmsg("PCA transform serialization buffer too small")));
    }
    errno_t rc = memcpy_s(out, outSize, vtrans->mean, meanBytes);
    if (rc != EOK) {
        securec_check(rc, "\0", "\0");
    }
    rc = memcpy_s(out + meanBytes, outSize - meanBytes, vtrans->matrix, mBytes);
    if (rc != EOK) {
        securec_check(rc, "\0", "\0");
    }
}

VectorTransform *PcaDeserialize(int dimIn, int dimOut, const char *bytes)
{
    size_t payload = PcaSerializeSize(dimIn, dimOut);
    VectorTransform *vt = (VectorTransform *)palloc(sizeof(VectorTransform) + payload);
    float *block = (float *)(vt + 1);
    PcaBindStorage(vt, block, dimIn, dimOut);
    errno_t rc = memcpy_s(block, payload, bytes, payload);
    if (rc != EOK) {
        securec_check(rc, "\0", "\0");
    }
    return vt;
}