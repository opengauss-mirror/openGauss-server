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
 * matmulmgr.cpp
 *
 * IDENTIFICATION
 *        contrib/nputurbo/matmulmgr.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "matmulmgr.h"
#include "tiling/platform/platform_ascendc.h"

#define unlikely(x) __builtin_expect(!!(x), 0)

extern void matmul_custom_do(uint32_t blockDim, void *stream, uint8_t *a, uint8_t *b, uint8_t *c, uint8_t *tiling);

using namespace matmul_tiling;

#define CHECK_ACL(x)                                                                           \
    do {                                                                                       \
        aclError __ret = (x);                                                                  \
        if (unlikely(__ret != ACL_ERROR_NONE)) {                                               \
            return __ret;                                                                      \
        }                                                                                      \
    } while (0);

#define CHECK_ACL_RELEASE(x, deviceId, streamId)                                           \
do {                                                                                       \
    aclError __ret = (x);                                                                  \
    if (unlikely(__ret != ACL_ERROR_NONE)) {                                               \
        ReleaseResource((deviceId), (streamId));                                           \
        return __ret;                                                                      \
    }                                                                                      \
} while (0);

const int STREAM_NUM_PER_DEV = 200;
static MatmulMgr g_matmulMgr;

MatmulMgr::MatmulMgr() noexcept {}

MatmulMgr::~MatmulMgr()
{
    Release();
}

int MatmulMgr::Init(int *useNPUDevices, int devNum)
{
    if (useNPUDevices == nullptr || devNum <= 0) {
        return -1;
    }
    const char *socVersion = NPU_SOC_VERSION;
    CHECK_ACL(aclInit(nullptr));
    this->deviceNum = devNum;
    this->streamNumPerDev = STREAM_NUM_PER_DEV;
    this->devices = new(std::nothrow) DeviceRSC[this->deviceNum];
    if (this->devices == nullptr) {
        return -1;
    }
    aclError ret = ACL_ERROR_NONE;
    for (uint16_t devIdx = 0; devIdx < this->deviceNum; devIdx++) {
        this->devices[devIdx].deviceId = useNPUDevices[devIdx];
        ret = aclrtCreateContext(&(this->devices[devIdx].context), useNPUDevices[devIdx]);
        if (ret != ACL_ERROR_NONE) {
            goto cleanup;
        }
        ret = aclrtSetCurrentContext(this->devices[devIdx].context);
        if (ret != ACL_ERROR_NONE) {
            goto cleanup;
        }
        auto ascendcPlatform = platform_ascendc::PlatformAscendCManager::GetInstance(socVersion);
        this->devices[devIdx].aicNum = ascendcPlatform->GetCoreNumAic();
        this->devices[devIdx].streams = new(std::nothrow) aclrtStream[streamNumPerDev];
        if (this->devices[devIdx].streams == nullptr) {
            goto cleanup;
        }
        for (uint16_t streamIdx = 0; streamIdx < streamNumPerDev; streamIdx++) {
            ret = aclrtCreateStreamWithConfig(&(this->devices[devIdx].streams[streamIdx]), 0, ACL_STREAM_FAST_LAUNCH);
            if (ret != ACL_ERROR_NONE) {
                goto cleanup;
            }
            this->devices[devIdx].streamResource.push(streamIdx);
        }
    }
    this->initialized = true;
    return 0;

cleanup:
    Release();
    return -1;
}

void MatmulMgr::Release()
{
    if (this->devices == nullptr) {
        return;
    }
    for (uint16_t devIdx = 0; devIdx < this->deviceNum; devIdx++) {
        if (this->devices[devIdx].context == nullptr) {
            continue;
        }
        aclrtSetCurrentContext(this->devices[devIdx].context);
        if (this->devices[devIdx].streams != nullptr) {
            for (uint16_t streamIdx = 0; streamIdx < streamNumPerDev; streamIdx++) {
                if (this->devices[devIdx].streams[streamIdx] == nullptr) {
                    continue;
                }
                aclrtDestroyStream(this->devices[devIdx].streams[streamIdx]);
                this->devices[devIdx].streams[streamIdx] = nullptr;
            }
        }
        aclrtDestroyContext(this->devices[devIdx].context);
        this->devices[devIdx].context = nullptr;
        aclrtResetDevice(this->devices[devIdx].deviceId);
    }
    delete [] (this->devices);

    aclFinalize();
    this->initialized = false;
    return;
}

uint8_t *MatmulMgr::GetTilingBuf(optiling::TCubeTiling *tilingData)
{
    uint32_t tilingSize = tilingData->GetDataSize();
    uint8_t *buf = (uint8_t *)malloc(tilingSize);
    tilingData->SaveToBuffer(buf, tilingSize);
    return buf;
}

uint8_t *MatmulMgr::GenerateTiling(aclrtContext *context, int &paramM, int &paramN, int &paramK, int &usedCoreNum,
    int &tilingCoreNum)
{
    optiling::TCubeTiling tilingData;
    MultiCoreMatmulTiling tilingApi;
    tilingApi.SetDim(usedCoreNum);
    tilingApi.SetAType(TPosition::GM, CubeFormat::ND, DataType::DT_FLOAT, false);
    tilingApi.SetBType(TPosition::GM, CubeFormat::ND, DataType::DT_FLOAT, false);
    tilingApi.SetCType(TPosition::GM, CubeFormat::ND, DataType::DT_FLOAT);

    tilingApi.SetOrgShape(paramM, paramN, paramK);
    tilingApi.SetShape(paramM, paramN, paramK);
    tilingApi.SetSingleShape(-1, -1, -1);
    tilingApi.EnableBias(false);
    tilingApi.SetBufferSpace(-1, -1, -1);

    tilingApi.GetTiling(tilingData);
    int32_t dim = 0;
    int32_t mDim = 0;
    int32_t nDim = 0;
    tilingApi.GetCoreNum(dim, mDim, nDim);
    tilingCoreNum = dim;
    return GetTilingBuf(&tilingData);
}

int MatmulMgr::PerformMatmul(float *matrixA, float *matrixB, float *matrixC, int paramM, int paramN, int paramK,
    uint8_t **cacheMatrixAAddr, int devIdx, bool needCache)
{
    if (unlikely(!(this->initialized))) {
        return -1;
    }
    if ((matrixA == nullptr && *cacheMatrixAAddr == nullptr) || matrixB == nullptr || matrixC == nullptr) {
        return -1;
    }
    int tilingCoreNum = 0;
    uint16_t deviceId = devIdx % this->deviceNum;
    uint16_t streamId = AcquireStreamResource(deviceId);
    CHECK_ACL(aclrtSetCurrentContext(this->devices[deviceId].context));

    // copy data to device
    uint8_t *aDevice = nullptr;
    size_t aSize = (size_t)paramM * paramK * sizeof(float);
    if (*cacheMatrixAAddr == nullptr || (!needCache)) {
        CHECK_ACL_RELEASE(aclrtMalloc((void **)&aDevice, aSize, ACL_MEM_MALLOC_HUGE_FIRST), deviceId, streamId);
        CHECK_ACL_RELEASE(aclrtMemcpy(aDevice, aSize, matrixA, aSize, ACL_MEMCPY_HOST_TO_DEVICE), deviceId, streamId);
        *cacheMatrixAAddr = aDevice;
    } else {
        aDevice = *cacheMatrixAAddr;
    }

    uint8_t *bDevice = nullptr;
    size_t bSize = (size_t)paramK * paramN * sizeof(float);
    CHECK_ACL_RELEASE(aclrtMalloc((void **)&bDevice, bSize, ACL_MEM_MALLOC_HUGE_FIRST), deviceId, streamId);
    CHECK_ACL_RELEASE(aclrtMemcpy(bDevice, bSize, matrixB, bSize, ACL_MEMCPY_HOST_TO_DEVICE), deviceId, streamId);

    uint8_t *tilingDevice = nullptr;
    size_t tilingSize = sizeof(TCubeTiling);
    uint8_t *tilingHost = nullptr;
    CHECK_ACL_RELEASE(aclrtMallocHost((void **)(&tilingHost), tilingSize), deviceId, streamId);
    CHECK_ACL_RELEASE(aclrtMalloc((void **)(&tilingDevice), tilingSize, ACL_MEM_MALLOC_HUGE_FIRST), deviceId, streamId);
    uint8_t *tilingBuf = GenerateTiling(&(this->devices[deviceId].context), paramM, paramN, paramK,
        this->devices[deviceId].aicNum, tilingCoreNum);
    CHECK_ACL_RELEASE(aclrtMemcpy(tilingHost, tilingSize, tilingBuf, tilingSize, ACL_MEMCPY_HOST_TO_HOST), deviceId,
        streamId);
    free(tilingBuf);
    CHECK_ACL_RELEASE(aclrtMemcpy(tilingDevice, tilingSize, tilingHost, tilingSize, ACL_MEMCPY_HOST_TO_DEVICE),
        deviceId, streamId);
    CHECK_ACL_RELEASE(aclrtFreeHost(tilingHost), deviceId, streamId);

    uint8_t *cDevice = nullptr;
    size_t cSize = (size_t)paramM * paramN * sizeof(float);
    CHECK_ACL_RELEASE(aclrtMalloc((void **)&cDevice, cSize, ACL_MEM_MALLOC_HUGE_FIRST), deviceId, streamId);

    matmul_custom_do(tilingCoreNum, this->devices[deviceId].streams[streamId],
        aDevice, bDevice, cDevice, tilingDevice);
    CHECK_ACL_RELEASE(aclrtSynchronizeStream(this->devices[deviceId].streams[streamId]), deviceId, streamId);

    CHECK_ACL_RELEASE(aclrtMemcpy(matrixC, cSize, cDevice, cSize, ACL_MEMCPY_DEVICE_TO_HOST), deviceId, streamId);

    CHECK_ACL_RELEASE(aclrtFree(bDevice), deviceId, streamId);
    CHECK_ACL_RELEASE(aclrtFree(cDevice), deviceId, streamId);
    CHECK_ACL_RELEASE(aclrtFree(tilingDevice), deviceId, streamId);
    if (!needCache) {
        CHECK_ACL_RELEASE(aclrtFree(aDevice), deviceId, streamId);
        *cacheMatrixAAddr = nullptr;
    }
    ReleaseResource(deviceId, streamId);
    return 0;
}

void MatmulMgr::ReleaseDeviceCache(uint8_t **tupleDevice, int devIdx)
{
    if (tupleDevice == nullptr || *tupleDevice == nullptr) {
        return;
    }
    aclrtSetCurrentContext(this->devices[devIdx % this->deviceNum].context);
    aclrtFree(*tupleDevice);
    *tupleDevice = nullptr;
    return;
}

uint32_t MatmulMgr::AcquireStreamResource(uint16_t devIdx)
{
    std::unique_lock<std::mutex> lock(this->devices[devIdx].mtx);
    this->devices[devIdx].cv.wait(lock, [this, devIdx] { return !(this->devices[devIdx].streamResource.empty()); });
    auto streamIdx = this->devices[devIdx].streamResource.top();
    this->devices[devIdx].streamResource.pop();
    return streamIdx;
}

void MatmulMgr::ReleaseResource(uint16_t devIdx, uint32_t streamIdx)
{
    std::lock_guard<std::mutex> lock(this->devices[devIdx].mtx);
    this->devices[devIdx].streamResource.push(streamIdx);
    this->devices[devIdx].cv.notify_one();
}

/* interfaces for npu */

extern "C" int InitNPU(int *useNPUDevices, int devNum)
{
    return g_matmulMgr.Init(useNPUDevices, devNum);
}

extern "C" int MatrixMulOnNPU(float *matrixA, float *matrixB, float *resMatrix, int paramM, int paramN, int paramK,
    uint8_t **tupleDevice, int devIdx, bool needCache)
{
    return g_matmulMgr.PerformMatmul(matrixA, matrixB, resMatrix, paramM, paramN, paramK, tupleDevice, devIdx,
        needCache);
}

extern "C" void ReleaseNPU()
{
    g_matmulMgr.Release();
}

extern "C" void ReleaseNPUCache(uint8_t **tupleDevice, int devIdx)
{
    g_matmulMgr.ReleaseDeviceCache(tupleDevice, devIdx);
}
