/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
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
 *---------------------------------------------------------------------------------------
 *
 *  kernel.h
 *
 * IDENTIFICATION
 *        src/include/db4ai/kernel.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef _KERNEL_H_
#define _KERNEL_H_

#include "db4ai/matrix.h"

typedef struct KernelTransformer {
    int coefficients;
    void (*release)(struct KernelTransformer *kernel);
    void (*transform)(struct KernelTransformer *kernel, const Matrix *input, Matrix *output);
} KernelTransformer;

typedef struct KernelGaussian {
    KernelTransformer km;
    Matrix weights;
    Matrix offsets;
} KernelGaussian;

typedef struct KernelPolynomial {
    KernelTransformer km;
    int *components;
    Matrix weights;
    Matrix coefs;
} KernelPolynomial;

void kernel_init_gaussian(KernelGaussian *kernel, int features, int components, double gamma, int seed);
void kernel_init_polynomial(KernelPolynomial *kernel, int features, int components, int degree, double coef0, int seed);

#endif  // _KERNEL_H_


