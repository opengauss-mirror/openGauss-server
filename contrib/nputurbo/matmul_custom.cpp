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
 * This is a modification of code taken from https://gitee.com/ascend/samples.
 * The original copyright notice follows:
 * Copyright [2022-2023] [Huawei Technologies Co.,Ltd]

 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 *       http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.

 * IDENTIFICATION
 *        contrib/nputurbo/matmul_custom.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "kernel_operator.h"
#define ASCENDC_CUBE_ONLY
#include "lib/matmul_intf.h"

using namespace AscendC;
using namespace matmul;

constexpr MatmulConfig static CFG_MDL_OUTER_PRODUCT = GetMDLConfig(true);

__aicore__ inline uint64_t Ceiling(uint64_t a, uint64_t b)
{
    if (b == 0) {
        return 1;
    }
    return (a + b - 1) / b;
}

__aicore__ inline void CalcGMOffset(int blockIdx, int usedCoreNum, const TCubeTiling &param, uint64_t &offsetA,
    uint64_t &offsetB, uint64_t &offsetC, uint64_t &curSingleCoreM, uint64_t &curSingleCoreN)
{
    uint64_t mIterSize = Ceiling(param.M, param.singleCoreM);
    uint64_t mCoreIdx = blockIdx % mIterSize;
    uint64_t nCoreIdx = blockIdx / mIterSize;
    offsetA = mCoreIdx * param.Ka * param.singleCoreM;
    offsetB = nCoreIdx * param.singleCoreN;
    offsetC = mCoreIdx * param.N * param.singleCoreM + nCoreIdx * param.singleCoreN;

    uint64_t gmUseM = param.M - mCoreIdx * param.singleCoreM;
    curSingleCoreM = gmUseM < param.singleCoreM ? gmUseM : param.singleCoreM;

    uint64_t gmUseN = param.N - nCoreIdx * param.singleCoreN;
    curSingleCoreN = gmUseN < param.singleCoreN ? gmUseN : param.singleCoreN;
}

__aicore__ inline void CopyTiling(TCubeTiling *tiling, GM_ADDR tilingGM)
{
    uint32_t *ptr = reinterpret_cast<uint32_t *>(tiling);
    auto tiling32 = reinterpret_cast<__gm__ uint32_t *>(tilingGM);

    int tilingSize = sizeof(TCubeTiling) / sizeof(uint32_t);
    for (int idx = 0; idx < tilingSize; idx++, ptr++) {
        *ptr = *(tiling32 + idx);
    }
}

extern "C" __global__ __aicore__ void matmul_custom(GM_ADDR a, GM_ADDR b, GM_ADDR c, GM_ADDR tilingGM)
{
    TPipe que;
    TCubeTiling tiling;
    CopyTiling(&tiling, tilingGM);

    GlobalTensor<float> aGlobal;
    GlobalTensor<float> bGlobal;
    GlobalTensor<float> cGlobal;
    aGlobal.SetGlobalBuffer(reinterpret_cast<__gm__ float *>(a), (uint64_t)tiling.M * tiling.Ka);
    bGlobal.SetGlobalBuffer(reinterpret_cast<__gm__ float *>(b), (uint64_t)tiling.Ka * tiling.N);
    cGlobal.SetGlobalBuffer(reinterpret_cast<__gm__ float *>(c), (uint64_t)tiling.M * tiling.N);

    uint64_t offsetA = 0;
    uint64_t offsetB = 0;
    uint64_t offsetC = 0;
    uint64_t curSingleCoreM = 0;
    uint64_t curSingleCoreN = 0;

    CalcGMOffset(GetBlockIdx(), tiling.usedCoreNum, tiling, offsetA, offsetB, offsetC,
        curSingleCoreM, curSingleCoreN);
    auto gmA = aGlobal[offsetA];
    auto gmB = bGlobal[offsetB];
    auto gmC = cGlobal[offsetC];
    Matmul<MatmulType<TPosition::GM, CubeFormat::ND, float>,
        MatmulType<TPosition::GM, CubeFormat::ND, float>,
        MatmulType<TPosition::GM, CubeFormat::ND, float>,
        MatmulType<TPosition::GM, CubeFormat::ND, float>, CFG_MDL_OUTER_PRODUCT> mm;
    REGIST_MATMUL_OBJ(&que, GetSysWorkSpacePtr(), mm, &tiling);

    mm.SetTensorA(gmA);
    mm.SetTensorB(gmB);
    mm.SetTail(curSingleCoreM, curSingleCoreN);
    mm.IterateAll(gmC);
    mm.End();
}

void matmul_custom_do(uint32_t blockDim, void *stream, uint8_t *a, uint8_t *b, uint8_t *c, uint8_t *tiling)
{
    matmul_custom<<<blockDim, nullptr, stream>>>(a, b, c, tiling);
}
