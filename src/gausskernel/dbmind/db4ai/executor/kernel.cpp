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
* kernel.cpp
*        Kernel for transformations to a linear space
*
* IDENTIFICATION
*        src/gausskernel/dbmind/db4ai/executor/kernel.cpp
*
* ---------------------------------------------------------------------------------------
*/

#include "db4ai/kernel.h"

static void kernel_gaussian_release(struct KernelTransformer *kernel)
{
    KernelGaussian *kernel_g = (KernelGaussian *)kernel;
    matrix_release(&kernel_g->weights);
    matrix_release(&kernel_g->offsets);
}

static void kernel_gaussian_transform(struct KernelTransformer *kernel, const Matrix *input, Matrix *output)
{
    KernelGaussian *kernel_g = (KernelGaussian *)kernel;
    matrix_transform_kernel_gaussian(input, &kernel_g->weights, &kernel_g->offsets, output);
}

void kernel_init_gaussian(KernelGaussian *kernel, int features, int components, double gamma, int seed)
{
    kernel->km.coefficients = components;
    kernel->km.release = kernel_gaussian_release;
    kernel->km.transform = kernel_gaussian_transform;
    matrix_init_kernel_gaussian(features, components, gamma, seed, &kernel->weights, &kernel->offsets);
}

static void kernel_polynomial_release(struct KernelTransformer *kernel)
{
    KernelPolynomial *kernel_p = (KernelPolynomial *)kernel;
    matrix_release(&kernel_p->weights);
    matrix_release(&kernel_p->coefs);
    pfree(kernel_p->components);
}

static void kernel_polynomial_transform(struct KernelTransformer *kernel, const Matrix *input, Matrix *output)
{
    KernelPolynomial *kernel_p = (KernelPolynomial *)kernel;
    matrix_transform_kernel_polynomial(input, kernel_p->km.coefficients, kernel_p->components, &kernel_p->weights,
                                       &kernel_p->coefs, output);
}

void kernel_init_polynomial(KernelPolynomial *kernel, int features, int components, int degree, double coef0, int seed)
{
    kernel->km.coefficients = components;
    kernel->km.release = kernel_polynomial_release;
    kernel->km.transform = kernel_polynomial_transform;
    kernel->components =
        matrix_init_kernel_polynomial(features, components, degree, coef0, seed, &kernel->weights, &kernel->coefs);
}
