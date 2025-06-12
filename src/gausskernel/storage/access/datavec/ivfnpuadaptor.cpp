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
 * ivfnpuadaptor.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/datavec/ivfnpuadaptor.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <dlfcn.h>
#include "db4ai/bayesnet.h"
#include "utils/elog.h"
#include "access/datavec/utils.h"
#include "access/datavec/ivfnpuadaptor.h"

#define NPU_SUCCESS (0)
#define NPU_ERROR (-1)

#define IS_NULL_STR(str) ((str) == NULL || (str)[0] == '\0')

#define INVALID_ATTR_ERROR(detail) \
    ereport(FATAL, (errcode(ERRCODE_OPERATE_INVALID_PARAM), errmsg("Invalid attribute for NPU."), detail))

// return NPU_ERROR if error occurs
#define NPU_RETURN_IFERR(ret)                            \
    do {                                                 \
        int status = (ret);                              \
        if (SECUREC_UNLIKELY(status != NPU_SUCCESS)) {   \
            return status;                               \
        }                                                \
    } while (0)

static bool FindNPUIdExist(Vector<int> &useNPUDevices, int &npuId)
{
    for (size_t idx = 0; idx < useNPUDevices.size(); idx++) {
        if (useNPUDevices[idx] == npuId) {
            return true;
        }
    }
    return false;
}

static int *ParseNPUAttr(int &deviceNum)
{
    Vector<int> useNPUDevices;
    /* Do str copy and remove space. */
    char* attr = TrimStr(g_instance.attr.attr_storage.ivfflat_npubind_info);
    if (IS_NULL_STR(attr)) {
        return nullptr;
    }

    char* ptoken = NULL;
    char* psave = NULL;
    const char* pdelimiter = ",";

    ptoken = TrimStr(strtok_r(attr, pdelimiter, &psave));

    while (!IS_NULL_STR(ptoken)) {
        char* pt = NULL;
        char* ps = NULL;
        const char* pd = "-";
        int startid = -1;
        int endid = -1;

        pt = TrimStr(strtok_r(ptoken, pd, &ps));
        if (!IS_NULL_STR(pt))
            startid = pg_strtoint32(pt);
        if (!IS_NULL_STR(ps))
            endid = pg_strtoint32(ps);

        if (startid < 0 && endid < 0)
            INVALID_ATTR_ERROR(errdetail("Can not parse attribute %s", pt));
        if (endid == -1) {
            if (!FindNPUIdExist(useNPUDevices, startid))
                useNPUDevices.push_back(startid);
        } else {
            if (startid > endid) {
                int tmpid = startid;
                startid = endid;
                endid = tmpid;
            }

            for (int i = startid; i <= endid; i++) {
                if (!FindNPUIdExist(useNPUDevices, i))
                    useNPUDevices.push_back(i);
            }
        }

        /* Don't need to free when error ocurrs, errors here are FATAL level! */
        pfree_ext(pt);
        pfree_ext(ptoken);
        ptoken = TrimStr(strtok_r(NULL, pdelimiter, &psave));
    }
    deviceNum = useNPUDevices.size();
    int *useNPUDevicesArray = (int*)palloc0(sizeof(int) * deviceNum);
    for (size_t idx = 0; idx < deviceNum; idx++) {
        useNPUDevicesArray[idx] = useNPUDevices[idx];
    }
    return useNPUDevicesArray;
}

bool NPUResolvePath(char* absolutePath, const char* rawPath, const char* filename)
{
    char path[MAX_PATH_LEN] = { 0 };

    if (!realpath(rawPath, path)) {
        if (errno != ENOENT && errno != EACCES) {
            return false;
        }
    }

    int ret = snprintf_s(absolutePath, MAX_PATH_LEN, MAX_PATH_LEN - 1, "%s/%s", path, filename);
    if (ret < 0) {
        return false;
    }
    return true;
}

int NPUOpenDll(void **libHandle, char *symbol)
{
#ifndef WIN32
    *libHandle = dlopen(symbol, RTLD_LAZY);
    if (*libHandle == NULL) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not load library %s, %s", NPU_SO_NAME, dlerror())));
        return NPU_ERROR;
    }
    return NPU_SUCCESS;
#else
    return NPU_ERROR;
#endif
}

int NPULoadSymbol(char *symbol, void **symLibHandle)
{
#ifndef WIN32
    const char *dlsymErr = NULL;

    *symLibHandle = dlsym(g_npu_func.handle, symbol);
    dlsymErr = dlerror();
    if (dlsymErr != NULL) {
        ereport(FATAL, (errcode(ERRCODE_INVALID_OPERATION),
            errmsg("incompatible library \"%s\", load %s failed, %s", NPU_SO_NAME, symbol, dlsymErr)));
        return NPU_ERROR;
    }
#endif // !WIN32
    return NPU_SUCCESS;
}

#define NPULoadSymbol_FUNC(func) NPULoadSymbol(#func, (void **)&g_npu_func.func)

int NPULoadSymbols(char *libDllPath)
{
    NPU_RETURN_IFERR(NPUOpenDll(&g_npu_func.handle, libDllPath));

    NPU_RETURN_IFERR(NPULoadSymbol_FUNC(InitNPU));
    NPU_RETURN_IFERR(NPULoadSymbol_FUNC(MatrixMulOnNPU));
    NPU_RETURN_IFERR(NPULoadSymbol_FUNC(ReleaseNPU));
    NPU_RETURN_IFERR(NPULoadSymbol_FUNC(ReleaseNPUCache));

    return NPU_SUCCESS;
}

int NPUFuncInit()
{
    if (g_npu_func.inited) {
        return NPU_SUCCESS;
    }

    char libDllPath[MAX_PATH_LEN] = { 0 };
    char* rawPath = getenv(NPU_ENV_PATH);
    if (rawPath == nullptr) {
        ereport(ERROR, (errmsg("failed to get DATAVEC_NPU_LIB_PATH")));
        return NPU_ERROR;
    }

    if (!NPUResolvePath(libDllPath, rawPath, NPU_SO_NAME)) {
        ereport(ERROR, (errmsg(
            "failed to resolve the path of libnputurbo.so, libDllPath %s, rawPath %s",
            libDllPath, rawPath)));
        return NPU_ERROR;
    }

    int ret = NPULoadSymbols(libDllPath);
    if (ret != NPU_SUCCESS) {
        return NPU_ERROR;
    }

    g_npu_func.inited = true;
    return NPU_SUCCESS;
}

void NPUCloseDll(void *libHandle)
{
#ifndef WIN32
    (void)dlclose(libHandle);
#endif
}

void NPUResourceInit()
{
#ifdef __x86_64__
    ereport(FATAL, (errmsg("ivfflat-npu only support in arm.")));
#endif

    if (NPUFuncInit() != NPU_SUCCESS) {
        ereport(FATAL, (errmsg("NPUFunc init failed.")));
    }

    /* get params of npu */
    int deviceNum = 0;
    int *useNPUDevicesArray = ParseNPUAttr(deviceNum);
    if (useNPUDevicesArray == nullptr) {
        ereport(FATAL, (errmsg("parse NPU bind param failed.")));
    }

    if (g_npu_func.InitNPU(useNPUDevicesArray, deviceNum) != NPU_SUCCESS) {
        pfree(useNPUDevicesArray);
        ereport(FATAL, (errmsg("could not initialize NPU resource.")));
    }
    pfree(useNPUDevicesArray);
    return;
}

void NPUResourceRelease()
{
    if (!g_instance.attr.attr_storage.enable_ivfflat_npu || !g_npu_func.inited) {
        return;
    }
    ereport(LOG, (errmsg("datavec infflat_npu uninit")));
    if (g_npu_func.handle != NULL) {
        g_npu_func.ReleaseNPU();
        NPUCloseDll(g_npu_func.handle);
        g_npu_func.handle = NULL;
        g_npu_func.inited = false;
    }
    return;
}

int MatrixMulOnNPU(float *matrixA, float *matrixB, float *resMatrix, int paramM, int paramN, int paramK,
    uint8_t **matrixACacheAddr, int devIdx, bool cacheMatrixA)
{
    return g_npu_func.MatrixMulOnNPU(matrixA, matrixB, resMatrix, paramM, paramN, paramK, matrixACacheAddr, devIdx,
        cacheMatrixA);
}

void ReleaseNPUCache(uint8_t **matrixACacheAddr, int devIdx)
{
    g_npu_func.ReleaseNPUCache(matrixACacheAddr, devIdx);
}
