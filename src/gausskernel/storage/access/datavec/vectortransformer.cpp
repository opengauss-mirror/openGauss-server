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

void FloatRandom(float* x, size_t n, int64_t seed)
{
    const size_t nblock = n < 1024 ? 1 : 1024;

    RandomGenerator rng0(seed);
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
        FloatRandom(vtrans->matrix, dim * dim, 12345);
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

inline int Log2Floor(int dim)
{
    int res = 0;
    while (dim > 1) {
        dim >>= 1;
        res++;
    }
    return res;
}

void Uint8Random(uint8 *x, size_t n, int64_t seed)
{
    RandomGenerator rng(seed);
    for (int i = 0; i < n; i++) {
        x[i] = rng.rand_uint8();
    }
}

void FhtInit(VectorTransform* vtrans)
{
    int dim = vtrans->dim;
    int alignedDim = (dim + 7) / 8;
    vtrans->power2Dim = 1 << Log2Floor(dim);
    vtrans->fac = 1.0 / sqrt(vtrans->power2Dim);
}

void FhtTrain(VectorTransform* vtrans)
{
    int dim = vtrans->dim;
    int alignedDim = (dim + 7) / 8;
    vtrans->matfht = (uint8 *)palloc(FHT_ROUND * alignedDim);
    Uint8Random(vtrans->matfht, FHT_ROUND * alignedDim, 12345);
    FhtInit(vtrans);
}

void FhtTransform(VectorTransform* vtrans, const float* vec, float *transvec)
{
    int dim = vtrans->dim;
    size_t dimSize = dim * sizeof(float);
    errno_t rc = memcpy_s(transvec, dimSize, vec, dimSize);
    securec_check_c(rc, "\0", "\0");
    uint8 *matfht = vtrans->matfht;
    int power2Dim = vtrans->power2Dim;
    int alignedDim = (dim + 7) / 8;

    if (dim == vtrans->power2Dim) {
        for (int i = 0; i < FHT_ROUND; i++) {
            FlipSign(matfht + i * alignedDim, transvec, dim);
            FHTRotate(transvec, power2Dim);
            VecRescale(transvec, power2Dim, vtrans->fac);
        }
    } else {
        int start = dim - power2Dim;
        for (int i = 0; i < FHT_ROUND; i += 2) {
            FlipSign(matfht + i * alignedDim, transvec, dim);
            FHTRotate(transvec, power2Dim);
            VecRescale(transvec, power2Dim, vtrans->fac);
            KacsWalk(transvec, dim);

            FlipSign(matfht + (i + 1) * alignedDim, transvec, dim);
            FHTRotate(transvec + start, power2Dim);
            VecRescale(transvec + start, power2Dim, vtrans->fac);
            KacsWalk(transvec, dim);
        }
    }
}

void *FhtGetMatrix(VectorTransform* vtrans)
{
    return (void *)vtrans->matfht;
}