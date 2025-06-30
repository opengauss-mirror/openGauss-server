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
 * ---------------------------------------------------------------------------------------
 *
 * matrix_mem.cpp
 *        routines to support RackMemory
 *
 *
 * IDENTIFICATION
 *        src/include/storage/matrix_mem/matrix_mem.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include <fstream>
#include <cerrno>
#include <functional>
#include "postgres.h"
#include "dlfcn.h"
#include "securectype.h"
#include "knl/knl_instance.h"
#include "utils/memutils.h"
#include "utils/atomic.h"
#include "storage/matrix_mem.h"

MatrixMemFunc g_matrixMemFunc = {0};
constexpr auto MAX_RETRY_TIMES = 5;
static constexpr auto BASE_NID = "";
static char *g_nodeId = nullptr;
static char *g_hostName = nullptr;

int MaxtrixMemLoadSymbol(char *symbol, void **symLibHandle)
{
    const char *dlsymErr = nullptr;
    *symLibHandle = dlsym(g_matrixMemFunc.handle, symbol);
    dlsymErr = dlerror();
    if (dlsymErr != nullptr) {
#ifdef FRONTEND
        fprintf(stderr, _("matrix mem load symbol: %s, error: %s"), symbol, dlsymErr);
#else
        ereport(WARNING, (errmsg("matrix mem load symbol: %s, error: %s", symbol, dlsymErr)));
#endif
        return MATRIX_MEM_ERROR;
    }
    return MATRIX_MEM_SUCCESS;
}

int MaxtrixMemOpenDl(void **libHandle, char *symbol)
{
    *libHandle = dlopen(symbol, RTLD_LAZY);
    if (*libHandle == nullptr) {
#ifdef FRONTEND
        fprintf(stderr, _("load matrix mem dynamic lib: %s, error: %s"), symbol, dlerror());
#else
        ereport(WARNING, (errmsg("load matrix mem dynamic lib: %s, error: %s", symbol, dlerror())));
#endif
        return MATRIX_MEM_ERROR;
    }
    return MATRIX_MEM_SUCCESS;
}

void MatrixMemFuncInit(char* lmemfabricClientPath)
{
    SymbolInfo symbols[] = {
        {"RackMemMalloc", (void **)&g_matrixMemFunc.rackMemMalloc},
        {"RackMemMallocAsync", (void **)&g_matrixMemFunc.rackMemMallocAsync},
        {"RackMemFree", (void **)&g_matrixMemFunc.rackMemFree},
        {"RackMemFreeAsync", (void **)&g_matrixMemFunc.rackMemFreeAsync},
        {"RackMemShmLookupShareRegions", (void **)&g_matrixMemFunc.rackMemShmLookupShareRegions},
        {"RackMemShmLookupRegionInfo", (void **)&g_matrixMemFunc.rackMemShmLookupRegionInfo},
        {"RackMemShmCreate", (void **)&g_matrixMemFunc.rackMemShmCreate},
        {"RackMemShmMmap", (void **)&g_matrixMemFunc.rackMemShmMmap},
        {"RackMemShmCacheOpt", (void **)&g_matrixMemFunc.rackMemShmCacheOpt},
        {"RackMemShmUnmmap", (void **)&g_matrixMemFunc.rackMemShmUnmmap},
        {"RackMemShmDelete", (void **)&g_matrixMemFunc.rackMemShmDelete},
        {"RackMemLookupClusterStatistic",
            (void**)&g_matrixMemFunc.rackMemLookupClusterStatistic},
        {"ErrCodeToStr", (void**)&g_matrixMemFunc.errCodeToStr}};

    struct stat st;
    if (lstat((const char*)lmemfabricClientPath, &st) == -1) {
#ifdef FRONTEND
        fprintf(stderr, _("load matrix mem dynamic lib error: %s, lib not exists"), lmemfabricClientPath);
#else
        ereport(WARNING, (errmsg("load matrix mem dynamic lib error: %s, lib not exists", lmemfabricClientPath)));
#endif
        return;
    }

    if (SECUREC_UNLIKELY(MaxtrixMemOpenDl(&g_matrixMemFunc.handle, lmemfabricClientPath) != MATRIX_MEM_SUCCESS)) {
        return;
    }

    size_t numSymbols = sizeof(symbols) / sizeof(symbols[0]);
    for (size_t i = 0; i < numSymbols; i++) {
        if (SECUREC_UNLIKELY(MaxtrixMemLoadSymbol(symbols[i].symbolName, symbols[i].funcPtr) != MATRIX_MEM_SUCCESS)) {
            return ;
        }
    }

    /* succeeded to load */
    g_matrixMemFunc.matrix_mem_inited = true;
}

void MatrixMemFuncUnInit()
{
    if (g_matrixMemFunc.matrix_mem_inited) {
        (void)dlclose(g_matrixMemFunc.handle);
        g_matrixMemFunc.handle = NULL;
        g_matrixMemFunc.matrix_mem_inited = false;
    }
}

const char *ErrCodeToStr(int errCode)
{
    return g_matrixMemFunc.errCodeToStr(errCode);
}

static const ErrorInfo *GetErrorInfo(int errCode)
{
    for (size_t i = 0; i < sizeof(ERROR_INFOS) / sizeof(ERROR_INFOS[0]); i++) {
        if (ERROR_INFOS[i].errorCode == errCode) {
            return &ERROR_INFOS[i];
        }
    }
    return nullptr;
}

static void HandleError(const char *funcName, int lastErrno, int retry, bool isLastRetry)
{
    const char *errorMsg = ErrCodeToStr(lastErrno);
    const ErrorInfo *info = GetErrorInfo(lastErrno);
    const char *logHint = "Please check Rackmanager log";
    bool shouldRetry = false;

    if (info) {
        logHint = info->logHint;
        shouldRetry = info->shouldRetry;
    }

    if (isLastRetry || !shouldRetry) {
#ifdef FRONTEND
        fprintf(stderr, _("%s failed, code:[%d], error: %s, %s\n"), funcName, lastErrno, errorMsg, logHint);
#else
        ereport(WARNING, (errmsg("%s failed, code:[%d], error: %s, %s", funcName, lastErrno, errorMsg, logHint)));
#endif
    } else {
#ifdef FRONTEND
        fprintf(stdout, _("%s failed, code:[%d], error: %s, will retry after 1s. %s\n"), funcName, retry + 1,
            errorMsg, logHint);
#else
        ereport(WARNING, (errmsg("%s failed, code:[%d], error: %s, will retry after 1s. %s", funcName, retry + 1,
                                 errorMsg, logHint)));
#endif
        pg_usleep(1000000L);
    }
}

static bool Retry(std::function<int(void **)> func, const char *funcName, void **result)
{
    int retry = 0;
    int errorCode = MATRIX_MEM_ERROR;

    while (retry < MAX_RETRY_TIMES) {
        errorCode = func(result);
        if (errorCode == MATRIX_MEM_SUCCESS) {
            return true;
        }

        HandleError(funcName, errorCode, retry, (retry == MAX_RETRY_TIMES - 1));

        const ErrorInfo *info = GetErrorInfo(errorCode);
        bool shouldRetry = (info && info->shouldRetry);

        if (shouldRetry && retry < MAX_RETRY_TIMES - 1) {
            retry++;
            continue;
        }

        break;
    }

    return false;
}

void *RackMemMalloc(size_t size, PerfLevel perfLevel, intptr_t attr)
{
    void *result = nullptr;
    std::function<int(void **)> func = [size, perfLevel, attr](void **res) -> int {
        *res = g_matrixMemFunc.rackMemMalloc(size, perfLevel, attr);
        if (res != nullptr) {
            return MATRIX_MEM_SUCCESS;
        } else {
            return errno;
        };
    };
    if (Retry(func, "RackMemMalloc", &result)) {
        return result;
    }
    return nullptr;
}

int RackMemMallocAsync(size_t size, PerfLevel perfLevel, intptr_t attr, AsyncFreeCallBack func, intptr_t ctx)
{
    void *result = nullptr;
    std::function<int(void **)> funcin = [size, perfLevel, attr, func, ctx](void **res) -> int {
        res = nullptr;
        return g_matrixMemFunc.rackMemMallocAsync(size, perfLevel, attr, func, ctx);
    };
    if (Retry(funcin, "RackMemMallocAsync", &result)) {
        return MATRIX_MEM_SUCCESS;
    }
    return MATRIX_MEM_ERROR;
}

int RackMemFree(void *ptr)
{
    void *result = nullptr;
    std::function<int(void **)> funcin = [ptr](void **res) -> int {
        res = nullptr;
        g_matrixMemFunc.rackMemFree(ptr);
        return errno;
    };
    if (Retry(funcin, "RackMemFree", &result)) {
        return MATRIX_MEM_SUCCESS;
    }
    return MATRIX_MEM_ERROR;
}

int RackMemFreeAsync(void *ptr, AsyncFreeCallBack func, intptr_t ctx)
{
    void *result = nullptr;
    std::function<int(void **)> funcin = [ptr, func, ctx](void **res) -> int {
        res = nullptr;
        return g_matrixMemFunc.rackMemFreeAsync(ptr, func, ctx);
    };
    if (Retry(funcin, "RackMemFreeAsync", &result)) {
        return MATRIX_MEM_SUCCESS;
    }
    return MATRIX_MEM_ERROR;
}

int RackMemShmLookupShareRegions(const char *baseNid, ShmRegionType type, SHMRegions *regions)
{
    void *result = nullptr;
    std::function<int(void **)> funcin = [baseNid, type, regions](void **res) -> int {
        res = nullptr;
        return g_matrixMemFunc.rackMemShmLookupShareRegions(baseNid, type, regions);
    };
    if (Retry(funcin, "RackMemShmLookupShareRegions", &result)) {
        return MATRIX_MEM_SUCCESS;
    }
    return MATRIX_MEM_ERROR;
}

int RackMemShmLookupRegionInfo(SHMRegionDesc *region, SHMRegionInfo *info)
{
    void *result = nullptr;
    std::function<int(void **)> funcin = [region, info](void **res) -> int {
        res = nullptr;
        return g_matrixMemFunc.rackMemShmLookupRegionInfo(region, info);
    };
    if (Retry(funcin, "RackMemShmLookupRegionInfo", &result)) {
        return MATRIX_MEM_SUCCESS;
    }
    return MATRIX_MEM_ERROR;
}

int RackMemShmCreate(char *name, uint64_t size, const char *baseNid, SHMRegionDesc *shmRegion)
{
    void *result = nullptr;
    std::function<int(void **)> funcin = [name, size, baseNid, shmRegion](void **res) -> int {
        res = nullptr;
        return g_matrixMemFunc.rackMemShmCreate(name, size, baseNid, shmRegion);
    };
    if (Retry(funcin, "RackMemShmCreate", &result)) {
        return MATRIX_MEM_SUCCESS;
    }
    return MATRIX_MEM_ERROR;
}

void *RackMemShmMmap(void *start, size_t length, int prot, int flags, const char *name, off_t offset)
{
    void *result = nullptr;
    std::function<int(void **)> func = [start, length, prot, flags, name, offset](void **res) -> int {
        *res = g_matrixMemFunc.rackMemShmMmap(start, length, prot, flags, name, offset);
        if (res != nullptr) {
            return MATRIX_MEM_SUCCESS;
        } else {
            return errno;
        };
    };
    if (Retry(func, "RackMemShmMmap", &result)) {
        return result;
    }
    return nullptr;
}

int RackMemShmCacheOpt(void *start, size_t length, ShmCacheOpt type)
{
    void *result = nullptr;
    std::function<int(void **)> func = [start, length, type](void **res) -> int {
        res = nullptr;
        return g_matrixMemFunc.rackMemShmCacheOpt(start, length, type);
    };
    if (Retry(func, "RackMemShmCacheOpt", &result)) {
        return MATRIX_MEM_SUCCESS;
    }
    return MATRIX_MEM_ERROR;
}

int RackMemShmUnmmap(void *start, size_t length)
{
    void *result = nullptr;
    std::function<int(void **)> func = [start, length](void **res) -> int {
        res = nullptr;
        return g_matrixMemFunc.rackMemShmUnmmap(start, length);
    };
    if (Retry(func, "RackMemShmUnmmap", &result)) {
        return MATRIX_MEM_SUCCESS;
    }
    return MATRIX_MEM_ERROR;
}

int RackMemShmDelete(char *name)
{
    void *result = nullptr;
    std::function<int(void **)> func = [name](void **res) -> int {
        res = nullptr;
        return g_matrixMemFunc.rackMemShmDelete(name);
    };
    if (Retry(func, "RackMemShmDelete", &result)) {
        return MATRIX_MEM_SUCCESS;
    }
    return MATRIX_MEM_ERROR;
}

int RackMemLookupClusterStatistic(ClusterInfo *cluster)
{
    void *result = nullptr;
    std::function<int(void **)> func = [cluster](void **res) -> int {
        res = nullptr;
        return g_matrixMemFunc.rackMemLookupClusterStatistic(cluster);
    };
    if (Retry(func, "RackMemLookupClusterStatistic", &result)) {
        return MATRIX_MEM_SUCCESS;
    }
    return MATRIX_MEM_ERROR;
}

static void RackMemGetNodeInfo()
{
    int ret;
    SHMRegions regions = SHMRegions();
    ret = RackMemShmLookupShareRegions(BASE_NID, ShmRegionType::INCLUDE_ALL_TYPE, &regions);
    if (ret != 0 || regions.region[0].num <= 0) {
#ifdef FRONTEND
        fprintf(stderr, _("lookup rack share regions failed, code: [%d], node num: [%d]\n"), ret,
                regions.region[0].num);
#else
        ereport(WARNING,
                (errcode(ERRCODE_INVALID_OPERATION),
                 errmsg("lookup rack share regions failed, code: [%d], node num: [%d]\n", ret, regions.region[0].num)));
#endif
        return;
    }
    for (int i = 0; i < regions.num; i++) {
        for (int j = 0; j < regions.region[i].num; j++) {
            if (strcmp(g_hostName, regions.region[i].hostName[j]) == 0) {
#ifdef FRONTEND
                fprintf(stdout, _("The share regions [%d] host name: [%s], node id: [%s].\n"), i,
                        regions.region[i].hostName[j], regions.region[i].nodeId[j]);
#else
                ereport(DEBUG1, (errmsg("The share regions [%d] host name: [%s], node id: [%s].\n", i,
                                        regions.region[i].hostName[j], regions.region[i].nodeId[j])));
#endif
                g_nodeId = static_cast<char*>(malloc(strlen(regions.region[i].nodeId[j]) + 1));
                errno_t rc = strcpy_s(g_nodeId, strlen(regions.region[i].nodeId[j]) + 1, regions.region[i].nodeId[j]);
                securec_check(rc, "\0", "\0");
                break;
            }
        }
    }
    return;
}

static void GetHostName()
{
    if (g_hostName != nullptr) {
#ifdef FRONTEND
        fprintf(stdout, _("The RackManager host name [%s] has been initialized.\n"), g_hostName);
#else
        ereport(DEBUG1, (errmsg("The RackManager host name [%s] has been initialized.\n", g_hostName)));
#endif
        return;
    }

    const char *filePath = "/etc/hostname";
    std::ifstream file(filePath);
    if (!file.is_open()) {
#ifdef FRONTEND
        fprintf(stderr, _("Failed to open /etc/hostname , error: %s\n"), strerror(errno));
#else
        ereport(WARNING, (errmsg("Failed to open /etc/hostname , error: %s\n", strerror(errno))));
#endif
        return;
    }

    char content[MAX_HOSTNAME_LENGTH];
    if (file.getline(content, MAX_HOSTNAME_LENGTH)) {
        if (strlen(content) >= MAX_HOSTNAME_LENGTH) {
#ifdef FRONTEND
            fprintf(stderr, _("the hostname is too long."));
#else
            ereport(WARNING, (errmsg("the hostname is too long.")));
#endif
            file.close();
            return;
        }
        g_hostName = static_cast<char*>(malloc(strlen(content) + 1));
        errno_t rc = strcpy_s(g_hostName, strlen(content) + 1, content);
        securec_check(rc, "\0", "\0");
    } else {
#ifdef FRONTEND
        fprintf(stderr, _("Unable to read file /etc/hostname"));
#else
        ereport(WARNING, (errmsg("Unable to read file /etc/hostname")));
#endif
    }

    file.close();
#ifdef FRONTEND
    fprintf(stdout, _("The RackManager host name is: [%s].\n"), g_hostName);
#else
    ereport(DEBUG1, (errmsg("The RackManager host name is: [%s].\n", g_hostName)));
#endif
    return;
}

static void GetNodeId()
{
    if (g_nodeId != nullptr) {
#ifdef FRONTEND
        fprintf(stdout, _("The RackManager node id [%s] has been initialized.\n"), g_nodeId);
#else
        ereport(DEBUG1, (errmsg("The RackManager node id [%s] has been initialized.\n", g_nodeId)));
#endif
        return;
    }

    GetHostName();
    if (g_hostName == nullptr) {
#ifdef FRONTEND
        fprintf(stderr, _("Failed to get host name from /etc/hostname."));
#else
        ereport(WARNING, (errmsg("Failed to get host name from /etc/hostname.")));
#endif
        return;
    }
    RackMemGetNodeInfo();
    if (g_nodeId == nullptr) {
#ifdef FRONTEND
        fprintf(stderr, _("Failed to get Rack nodeId, hostname: [%s]."), g_hostName);
#else
        ereport(WARNING, (errmsg("Failed to get Rack nodeId, hostname: [%s].", g_hostName)));
#endif
        return;
    }
#ifdef FRONTEND
    fprintf(stdout, _("The RackManager node id is: [%s].\n"), g_nodeId);
#else
    ereport(DEBUG1, (errmsg("The RackManager node id is: [%s].\n", g_nodeId)));
#endif
}

int RackMemAvailable(int *availBorrowMemSize)
{
    ClusterInfo cluserInfo;
    SocketInfo socketInfo;
    int borrowMemSize = 0;

    GetNodeId();
    if (g_nodeId == nullptr) {
        return MATRIX_MEM_ERROR;
    }
    int ret = RackMemLookupClusterStatistic(&cluserInfo);
    if (ret != 0 || cluserInfo.num <= 1) {
#ifdef FRONTEND
        fprintf(stderr, _("lookup rack cluster statistic failed, code: [%d], node num: [%d]\n"), ret, cluserInfo.num);
#else
        ereport(WARNING,
                (errcode(ERRCODE_INVALID_OPERATION),
                 errmsg("lookup rack cluster statistic failed, code: [%d], node num: [%d]\n", ret, cluserInfo.num)));
#endif
        return MATRIX_MEM_ERROR;
    }

    for (int i = 0; i < cluserInfo.num; i++) {
        if (strcmp(cluserInfo.host[i].hostName, g_nodeId) != 0) {
            for (int j = 0; j < cluserInfo.host[i].num; j++) {
                socketInfo = cluserInfo.host[i].socket[j];
                borrowMemSize += socketInfo.memTotal * MAX_RACK_MEMORY_PERCENT - socketInfo.memExport;
#ifdef FRONTEND
                fprintf(stdout,
                        _("The RackManager node [%s] socket[%d] memory info: memTotal: %d, memUsed: %d, memExport: %d, "
                          "memImport: %d."),
                        cluserInfo.host[i].hostName, j, socketInfo.memTotal, socketInfo.memUsed, socketInfo.memExport,
                        socketInfo.memImport);
#else
                ereport(DEBUG1, (errmsg("The RackManager node [%s] socket[%d] memory info: "
                                        "memTotal: %d, memUsed: %d, memExport: %d, memImport: %d.",
                                        cluserInfo.host[i].hostName, j, socketInfo.memTotal, socketInfo.memUsed,
                                        socketInfo.memExport, socketInfo.memImport)));
#endif
            }
        }
    }
    if (borrowMemSize < 0) {
#ifdef FRONTEND
        fprintf(stdout,
                _("The RackManager node [%s] borrow memory size is less than 0, "
                  "please check the RackManager node memory info."),
                g_nodeId);
#else
        ereport(WARNING, (errmsg("The RackManager node [%s] borrow memory size is less than 0, "
                                 "please check the RackManager node memory info.",
                                 g_nodeId)));
#endif
        return MATRIX_MEM_ERROR;
    }
    *availBorrowMemSize = borrowMemSize;
    return MATRIX_MEM_SUCCESS;
}