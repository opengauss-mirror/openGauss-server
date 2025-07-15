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
 * matmulmgr.h
 *
 * IDENTIFICATION
 *        contrib/nputurbo/matmulmgr.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef MATMULMGR_H
#define MATMULMGR_H

#include <mutex>
#include <stack>
#include <condition_variable>
#include "acl/acl.h"
#include "tiling/tiling_api.h"

typedef struct DeviceRSC {
    uint16_t deviceId = 0;
    int aicNum = 0;
    aclrtContext context = nullptr;
    aclrtStream *streams = nullptr;
    std::stack<uint32_t> streamResource;
    mutable std::mutex mtx;
    std::condition_variable cv;
} DeviceRSC;

class MatmulMgr {
public:
    MatmulMgr() noexcept;
    virtual ~MatmulMgr();
    int Init(int *useNPUDevices, int devNum);
    void Release();
    uint8_t *GetTilingBuf(optiling::TCubeTiling *tilingData);
    uint8_t *GenerateTiling(aclrtContext *context, int &paramM, int &paramN, int &paramK, int &usedCoreNum,
        int &tilingCoreNum);
    int PerformMatmul(float *matrixA, float *matrixB, float *matrixC, int paramM, int paramN, int paramK,
        uint8_t **cacheMatrixAAddr, int devIdx, bool needCache);
    void ReleaseDeviceCache(uint8_t **tupleDevice, int devIdx);
    uint32_t AcquireStreamResource(uint16_t devIdx);
    void ReleaseResource(uint16_t devIdx, uint32_t streamIdx);

private:
    MatmulMgr(const MatmulMgr&) = delete;
    MatmulMgr& operator=(const MatmulMgr&) = delete;
    MatmulMgr(MatmulMgr&&) = delete;
    MatmulMgr& operator=(MatmulMgr &&) = delete;

    uint16_t deviceNum = 0;
    uint16_t streamNumPerDev = 0;
    DeviceRSC *devices = nullptr;
    bool initialized = false;
};

/* interfaces for npu */
#ifdef __cplusplus
extern "C" {
#endif

int InitNPU(int *useNPUDevices, int devNum);
int MatrixMulOnNPU(float *matrixA, float *matrixB, float *resMatrix, int paramM, int paramN, int paramK,
    uint8_t **tupleDevice, int devIdx, bool needCache);
void ReleaseNPU();
void ReleaseNPUCache(uint8_t **tupleDevice, int devIdx);

#ifdef __cplusplus
}
#endif

#endif /* MATMULMGR_H */
