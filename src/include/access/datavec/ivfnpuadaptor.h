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
 * ivfnpuadaptor.h
 *
 * IDENTIFICATION
 *        src/include/access/datavec/ivfnpuadaptor.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef IVFNPUADAPTOR_H
#define IVFNPUADAPTOR_H

#include <cstdint>

#define NPU_ENV_PATH "DATAVEC_NPU_LIB_PATH"
#define NPU_SO_NAME "libnputurbo.so"

void NPUResourceInit();
void NPUResourceRelease();
int MatrixMulOnNPU(float *matrixA, float *matrixB, float *resMatrix, int paramM, int paramN, int paramK,
    uint8_t **matrixACacheAddr, int devIdx, bool cacheMatrixA);
void ReleaseNPUCache(uint8_t **matrixACacheAddr, int listId);

#endif
