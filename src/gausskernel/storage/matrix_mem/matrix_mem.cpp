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

#include "postgres.h"
#include "dlfcn.h"
#include "securectype.h"
#include "knl/knl_instance.h"
#include "ddes/dms/ss_common_attr.h"
#include "storage/matrix_mem.h"
#include <functional>
#include <fstream>

MatrixMemFunc g_matrixMemFunc = {0};
constexpr auto MAX_RETRY_TIMES = 5;
static constexpr auto BASE_NID = "";

int MaxtrixMemLoadSymbol(char *symbol, void **symLibHandle)
{
    const char *dlsymErr = NULL;
    *symLibHandle = dlsym(g_matrixMemFunc.handle, symbol);
    dlsymErr = dlerror();
    if (dlsymErr != NULL) {
        ereport(WARNING, (errmsg("matrix mem load symbol: %s, error: %s", symbol, dlsymErr)));
        return MATRIX_MEM_ERROR;
    }
    return MATRIX_MEM_SUCCESS;
}

int MaxtrixMemOpenDl(void **libHandle, char *symbol)
{
    *libHandle = dlopen(symbol, RTLD_LAZY);
    if (*libHandle == NULL) {
        int ret = ENABLE_UB ? ERROR : WARNING;
        ereport(ret, (errmsg("load matrix mem dynamic lib: %s, error: %s", symbol, dlerror())));
        return MATRIX_MEM_ERROR;
    }
    return MATRIX_MEM_SUCCESS;
}

void MatrixMemFuncInit(char* ubsMemPath)
{
    SymbolInfo symbols[] = {
        {"ubsmem_init_attributes", (void **)&g_matrixMemFunc.ubsmem_init_attributes},
        {"ubsmem_initialize", (void **)&g_matrixMemFunc.ubsmem_initialize},
        {"ubsmem_finalize", (void **)&g_matrixMemFunc.ubsmem_finalize},
        {"ubsmem_set_logger_level", (void **)&g_matrixMemFunc.ubsmem_set_logger_level},
        {"ubsmem_set_extern_logger", (void **)&g_matrixMemFunc.ubsmem_set_extern_logger},
        {"ubsmem_lookup_regions", (void **)&g_matrixMemFunc.ubsmem_lookup_regions},
        {"ubsmem_create_region", (void **)&g_matrixMemFunc.ubsmem_create_region},
        {"ubsmem_lookup_region", (void **)&g_matrixMemFunc.ubsmem_lookup_region},
        {"ubsmem_destroy_region", (void **)&g_matrixMemFunc.ubsmem_destroy_region},
        {"ubsmem_shmem_allocate", (void **)&g_matrixMemFunc.ubsmem_shmem_allocate},
        {"ubsmem_shmem_deallocate", (void **)&g_matrixMemFunc.ubsmem_shmem_deallocate},
        {"ubsmem_shmem_map", (void**)&g_matrixMemFunc.ubsmem_shmem_map},
        {"ubsmem_shmem_unmap", (void**)&g_matrixMemFunc.ubsmem_shmem_unmap},
        {"ubsmem_shmem_set_ownership", (void**)&g_matrixMemFunc.ubsmem_shmem_set_ownership},
        {"ubsmem_lease_malloc", (void**)&g_matrixMemFunc.ubsmem_lease_malloc},
        {"ubsmem_lease_free", (void**)&g_matrixMemFunc.ubsmem_lease_free},
        {"ubsmem_lookup_cluster_statistic", (void**)&g_matrixMemFunc.ubsmem_lookup_cluster_statistic}};

    struct stat st;
    if (lstat((const char*)ubsMemPath, &st) == -1) {
#ifdef FRONTEND
        fprintf(stderr, _("load matrix mem dynamic lib error: %s, lib not exists"), ubsMemPath);
#else
        int ret = ENABLE_UB ? ERROR : WARNING;
        ereport(ret, (errmsg("load matrix mem dynamic lib error: %s, lib not exists", ubsMemPath)));
#endif
        return;
    }

    if (SECUREC_UNLIKELY(MaxtrixMemOpenDl(&g_matrixMemFunc.handle, ubsMemPath) != MATRIX_MEM_SUCCESS)) {
        return;
    }

    size_t numSymbols = sizeof(symbols) / sizeof(symbols[0]);
    for (size_t i = 0; i < numSymbols; i++) {
        if (SECUREC_UNLIKELY(MaxtrixMemLoadSymbol(symbols[i].symbolName, symbols[i].funcPtr) != MATRIX_MEM_SUCCESS)) {
            return ;
        }
    }

    ubsmem_options_t ubsm_opts;
    int ret = ubsmem_init_attributes(&ubsm_opts);
    if (ret != 0) {
        return;
    }
    ret = ubsmem_initialize(&ubsm_opts);
    if (ret != 0) {
        ubsmem_finalize();
        return;
    }
    /* succeeded to load */
    g_matrixMemFunc.matrix_mem_inited = true;
}

void MatrixMemFuncUnInit()
{
    if (g_matrixMemFunc.matrix_mem_inited) {
        ubsmem_finalize();
        (void)dlclose(g_matrixMemFunc.handle);
        g_matrixMemFunc.handle = NULL;
        g_matrixMemFunc.matrix_mem_inited = false;
    }
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

static void PrintError(const char *funcName, int lastErrno, int retry, bool isLastRetry)
{
    const ErrorInfo *info = GetErrorInfo(lastErrno);
    bool shouldRetry = false;

    if (info) {
        shouldRetry = info->shouldRetry;
    }

    if (isLastRetry || !shouldRetry) {
#ifdef FRONTEND
        fprintf(stderr, _("%s failed, code:[%d]\n"), funcName, lastErrno);
#else
        ereport(WARNING, (errmsg("%s failed, code:[%d]", funcName, lastErrno)));
#endif
    } else {
#ifdef FRONTEND
        fprintf(stdout, _("%s failed for [%d] time, code:[%d], will retry after 1s.\n"), funcName,
                retry + 1, lastErrno);
#else
        ereport(WARNING, (errmsg("%s failed for [%d] time, code:[%d], will retry after 1s.", funcName,
                                 retry + 1, lastErrno)));
#endif
    }
}

template<typename Func>
static int Retry(Func func, const char *funcName)
{
    int retry = 0;
    int errorCode = MATRIX_MEM_ERROR;

    while (retry < MAX_RETRY_TIMES) {
        errorCode = func();
        if (errorCode == MATRIX_MEM_SUCCESS) {
            return MATRIX_MEM_SUCCESS;
        }
        if (errorCode != MATRIX_MEM_LOG_FREE) {
            PrintError(funcName, errorCode, retry, (retry == MAX_RETRY_TIMES - 1));
        }

        const ErrorInfo *info = GetErrorInfo(errorCode);
        bool shouldRetry = (info && info->shouldRetry);

        if (shouldRetry && retry < MAX_RETRY_TIMES - 1) {
            retry++;
            pg_usleep(1000000L);
            continue;
        }

        break;
    }

    return errorCode;
}

int ubsmem_init_attributes(ubsmem_options_t *ubsm_shmem_opts)
{
    std::function<int()> funcin = [&]() -> int {
        return g_matrixMemFunc.ubsmem_init_attributes(ubsm_shmem_opts);
    };
    return Retry(funcin, "ubsmem_init_attributes");
}

int ubsmem_initialize(const ubsmem_options_t *ubsm_shmem_opts)
{
    std::function<int()> funcin = [&]() -> int {
        return g_matrixMemFunc.ubsmem_initialize(ubsm_shmem_opts);
    };
    return Retry(funcin, "ubsmem_initialize");
}

int ubsmem_finalize(void)
{
    std::function<int()> funcin = [&]() -> int {
        return g_matrixMemFunc.ubsmem_finalize();
    };
    return Retry(funcin, "ubsmem_finalize");
}

int ubsmem_set_logger_level(int level)
{
    std::function<int()> funcin = [&]() -> int {
        return g_matrixMemFunc.ubsmem_set_logger_level(level);
    };
    return Retry(funcin, "ubsmem_set_logger_level");
}

int ubsmem_set_extern_logger(void (*func)(int level, const char *msg))
{
    std::function<int()> funcin = [&]() -> int {
        return g_matrixMemFunc.ubsmem_set_extern_logger(func);
    };
    return Retry(funcin, "ubsmem_set_extern_logger");
}

int ubsmem_lease_malloc(const char *region_name, size_t size, ubsmem_distance_t mem_distance, uint64_t flags,
                        void **local_ptr)
{
    std::function<int()> funcin = [&]() -> int {
        return g_matrixMemFunc.ubsmem_lease_malloc(region_name, size, mem_distance, flags, local_ptr);
    };
    return Retry(funcin, "ubsmem_lease_malloc");
}

int ubsmem_lease_free(void *local_ptr)
{
    std::function<int()> funcin = [&]() -> int {
        int ret = MATRIX_MEM_ERROR;
        ret = g_matrixMemFunc.ubsmem_lease_free(local_ptr);
        if (ret == UBSM_ERR_PARAM_INVALID) {
#ifdef FRONTEND
            fprintf(stdout, _("pointer [%p] has been freed.\n"), local_ptr);
#else
            ereport(LOG, (errmsg("pointer [%p] has been freed.\n", local_ptr)));
#endif
            return MATRIX_MEM_SUCCESS;
        }
        return ret;
    };
    return Retry(funcin, "ubsmem_lease_free");
}

int ubsmem_lookup_regions(ubsmem_regions_t* regions)
{
    std::function<int()> funcin = [&]() -> int {
        return g_matrixMemFunc.ubsmem_lookup_regions(regions);
    };
    return Retry(funcin, "ubsmem_lookup_regions");
}

int ubsmem_create_region(const char *region_name, size_t size, const ubsmem_region_attributes_t *reg_attr)
{
    std::function<int()> funcin = [&]() -> int {
        return g_matrixMemFunc.ubsmem_create_region(region_name, size, reg_attr);
    };
    return Retry(funcin, "ubsmem_create_region");
}

int ubsmem_lookup_region(const char *region_name, ubsmem_region_desc_t *region_desc)
{
    std::function<int()> funcin = [&]() -> int {
        return g_matrixMemFunc.ubsmem_lookup_region(region_name, region_desc);
    };
    return Retry(funcin, "ubsmem_lookup_region");
}

int ubsmem_destroy_region(const char *region_name)
{
    std::function<int()> funcin = [&]() -> int {
        return g_matrixMemFunc.ubsmem_destroy_region(region_name);
    };
    return Retry(funcin, "ubsmem_destroy_region");
}

int ubsmem_shmem_allocate(const char *region_name, const char *name, size_t size, mode_t mode, uint64_t flags)
{
    std::function<int()> funcin = [&]() -> int {
        return g_matrixMemFunc.ubsmem_shmem_allocate(region_name, name, size, mode, flags);
    };
    return Retry(funcin, "ubsmem_shmem_allocate");
}

int ubsmem_shmem_deallocate(const char *name)
{
    std::function<int()> funcin = [&]() -> int {
        return g_matrixMemFunc.ubsmem_shmem_deallocate(name);
    };
    return Retry(funcin, "ubsmem_shmem_deallocate");
}

int ubsmem_shmem_map(void *addr, size_t length, int prot, int flags, const char *name, off_t offset,
                     void **local_ptr)
{
    std::function<int()> funcin = [&]() -> int {
        return g_matrixMemFunc.ubsmem_shmem_map(addr, length, prot, flags, name, offset, local_ptr);
    };
    return Retry(funcin, "ubsmem_shmem_map");
}

int ubsmem_shmem_mapcheck(void *addr, size_t length, int prot, int flags, const char *name, off_t offset,
    void **local_ptr)
{
    std::function<int()> funcin = [&]() -> int {
        int ret = MATRIX_MEM_ERROR;
        ret = g_matrixMemFunc.ubsmem_shmem_map(addr, length, prot, flags, name, offset, local_ptr);
        if (ret == UBSM_ERR_NOT_FOUND) {
            return MATRIX_MEM_LOG_FREE;
        }
        return ret;
    };
    return Retry(funcin, "ubsmem_shmem_mapcheck");
}

int ubsmem_shmem_unmap(void *local_ptr, size_t length)
{
    std::function<int()> funcin = [&]() -> int {
        return g_matrixMemFunc.ubsmem_shmem_unmap(local_ptr, length);
    };
    return Retry(funcin, "ubsmem_shmem_unmap");
}

int ubsmem_shmem_set_ownership(const char *name, void *start, size_t length, int prot)
{
    std::function<int()> funcin = [&]() -> int {
        return g_matrixMemFunc.ubsmem_shmem_set_ownership(name, start, length, prot);
    };
    return Retry(funcin, "ubsmem_shmem_set_ownership");
}

int ubsmem_lookup_cluster_statistic(ubsmem_cluster_info_t* info)
{
    std::function<int()> funcin = [&]() -> int {
        return g_matrixMemFunc.ubsmem_lookup_cluster_statistic(info);
    };
    return Retry(funcin, "ubsmem_lookup_cluster_statistic");
}


static char* GetHostName()
{
    char* hostName = nullptr;
    const char *filePath = "/etc/hostname";
    std::ifstream file(filePath);
    if (!file.is_open()) {
#ifdef FRONTEND
        fprintf(stderr, _("Failed to open /etc/hostname , error: %s\n"), strerror(errno));
#else
        ereport(WARNING, (errmsg("Failed to open /etc/hostname , error: %s\n", strerror(errno))));
#endif
        return nullptr;
    }

    char content[MAX_HOST_NAME_DESC_LENGTH];
    if (file.getline(content, MAX_HOST_NAME_DESC_LENGTH)) {
        if (strlen(content) >= MAX_HOST_NAME_DESC_LENGTH) {
#ifdef FRONTEND
            fprintf(stderr, _("the hostname is too long."));
#else
            ereport(WARNING, (errmsg("the hostname is too long.")));
#endif
            file.close();
            return nullptr;
        }
        hostName = static_cast<char*>(malloc(strlen(content) + 1));
        errno_t rc = strcpy_s(hostName, strlen(content) + 1, content);
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
    fprintf(stdout, _("The host name is: [%s].\n"), hostName);
#else
    ereport(DEBUG1, (errmsg("The host name is: [%s].\n", hostName)));
#endif
    return hostName;
}

int RackMemAvailable(int *availBorrowMemSize)
{
    ubsmem_cluster_info_t cluster_info;
    ubsmem_numa_mem_t numa_mem;
    uint64_t borrow_mem_size = 0;
    char* host_name = GetHostName();
    if (!host_name) {
#ifdef FRONTEND
        fprintf(stderr, _("get host name failed\n"));
#else
        ereport(WARNING,
                (errcode(ERRCODE_INVALID_OPERATION),
                 errmsg("get host name failed\n")));
#endif
        return MATRIX_MEM_ERROR;
    }

    int ret = ubsmem_lookup_cluster_statistic(&cluster_info);
    if (ret != 0 || cluster_info.host_num <= 1) {
#ifdef FRONTEND
        fprintf(stderr, _("lookup rack cluster statistic failed, code: [%d], node num: [%d]\n"),
                ret, cluster_info.host_num);
#else
        ereport(WARNING,
                (errcode(ERRCODE_INVALID_OPERATION),
                 errmsg("lookup rack cluster statistic failed, code: [%d], node num: [%d]\n",
                        ret, cluster_info.host_num)));
#endif
        return MATRIX_MEM_ERROR;
    }

    for (int i = 0; i < cluster_info.host_num; ++i) {
        if (strcmp(cluster_info.host[i].host_name, host_name) != 0) {
            for (int j = 0; j < cluster_info.host[i].numa_num; ++j) {
                numa_mem = cluster_info.host[i].numa[j];
                borrow_mem_size += (numa_mem.mem_total * numa_mem.mem_lend_ratio) / 100 - numa_mem.mem_lend;
#ifdef FRONTEND
                fprintf(stdout,
                        _("The UBSE node[%s] socket[%d] memory info: mem_total: %d, mem_free: %d, mem_borrow: %d, "
                          "mem_lend: %d."),
                        cluster_info.host[i].host_name, numa_mem.socket_id, numa_mem.mem_total, numa_mem.mem_free,
                        numa_mem.mem_borrow, numa_mem.mem_lend);
#else
                ereport(DEBUG1, (errmsg("The UBSE node[%s] socket[%d] memory info: "
                                        "mem_total: %d, mem_free: %d, mem_borrow: %d, mem_lend: %d.",
                                        cluster_info.host[i].host_name, numa_mem.socket_id, numa_mem.mem_total,
                                        numa_mem.mem_free, numa_mem.mem_borrow, numa_mem.mem_lend)));
#endif
            }
        }
    }
    if (borrow_mem_size < 0) {
#ifdef FRONTEND
        fprintf(stdout,
                _("The UBSE node[%s] borrow memory size is less than 0, "
                  "please check the UBSE node memory info."),
                host_name);
#else
        ereport(WARNING, (errmsg("The UBSE node[%s] borrow memory size is less than 0, "
                                 "please check the UBSE node memory info.",
                                 host_name)));
#endif
        return MATRIX_MEM_ERROR;
    }

    *availBorrowMemSize = borrow_mem_size / (1024 * 1024);
#ifdef FRONTEND
        fprintf(stdout, _("The available memory size for UBSE node[%s] is %d MB."),
                host_name, *availBorrowMemSize);
#else
        ereport(DEBUG1, (errmsg("The available memory size for UBSE node[%s] is %d MB.",
                host_name, *availBorrowMemSize)));
#endif
    return MATRIX_MEM_SUCCESS;
}
