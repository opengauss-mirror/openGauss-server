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
 * matrix_mem.h
 *        routines to support RackMemory
 *
 *
 * IDENTIFICATION
 *        src/include/storage/matrix_mem.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef MATRIX_MEM_H
#define MATRIX_MEM_H

#include <string>
#include "storage/ubs_mem.h"

#ifdef __aarch64__
#define ENABLE_RACK_MEM (g_matrixMemFunc.matrix_mem_inited)
#else
#define ENABLE_RACK_MEM false
#endif

constexpr auto MATRIX_MEM_SUCCESS = 0;
constexpr auto MATRIX_MEM_ERROR = -1;
constexpr auto MATRIX_MEM_LOG_FREE = 1;

struct MmapParams {
    void *start = nullptr;
    size_t length = 0;
    int prot = 0;
    int flags = 0;
    const char *name = nullptr;
    off_t offset = 0;
};

typedef struct {
    int errorCode;
    bool shouldRetry;
} ErrorInfo;

static const ErrorInfo ERROR_INFOS[] = {
    {UBSM_ERR_PARAM_INVALID, false},
    {UBSM_ERR_NOPERM, false},
    {UBSM_ERR_MEMORY, false},
    {UBSM_ERR_UNIMPL, false},
    {UBSM_CHECK_RESOURCE_ERROR, false},
    {UBSM_ERR_NOT_FOUND, false},
    {UBSM_ERR_ALREADY_EXIST, false},
    {UBSM_ERR_MALLOC_FAIL, false},
    {UBSM_ERR_NET, false},
    {UBSM_ERR_UBSE, false},
    {UBSM_ERR_BUFF, false}
};

typedef struct SymbolInfo {
    char *symbolName;
    void **funcPtr;
} SymbolInfo;

typedef struct MatrixMemFunc {
    bool matrix_mem_inited;
    void *handle;
    int (*ubsmem_init_attributes)(ubsmem_options_t *ubsm_shmem_opts);
    int (*ubsmem_initialize)(const ubsmem_options_t *ubsm_shmem_opts);
    int (*ubsmem_finalize)(void);
    int (*ubsmem_set_logger_level)(int level);
    int (*ubsmem_set_extern_logger)(void (*func)(int level, const char *msg));
    int (*ubsmem_lookup_regions)(ubsmem_regions_t* regions);
    int (*ubsmem_create_region)(const char *region_name, size_t size, const ubsmem_region_attributes_t *reg_attr);
    int (*ubsmem_lookup_region)(const char *region_name, ubsmem_region_desc_t *region_desc);
    int (*ubsmem_destroy_region)(const char *region_name);
    int (*ubsmem_shmem_allocate)(const char *region_name, const char *name, size_t size, mode_t mode, uint64_t flags);
    int (*ubsmem_shmem_deallocate)(const char *name);
    int (*ubsmem_shmem_map)(void *addr, size_t length, int prot, int flags, const char *name, off_t offset,
                            void **local_ptr);
    int (*ubsmem_shmem_unmap)(void *local_ptr, size_t length);
    int (*ubsmem_shmem_set_ownership)(const char *name, void *start, size_t length, int prot);
    int (*ubsmem_lease_malloc)(const char *region_name, size_t size, ubsmem_distance_t mem_distance, uint64_t flags,
                               void **local_ptr);
    int (*ubsmem_lease_free)(void *local_ptr);
    int (*ubsmem_lookup_cluster_statistic)(ubsmem_cluster_info_t* info);
    int (*ubsmem_shmem_faults_register)(shmem_faults_func registerFunc);
} MatrixMemFunc;

extern MatrixMemFunc g_matrixMemFunc;

extern void MatrixMemFuncInit(char* ubsMemPath);

extern void MatrixMemFuncUnInit();

extern int RackMemAvailable(int *availBorrowMemSize);

extern int ubsmem_shmem_mapcheck(void* addr, size_t length, int prot, int flags, const char* name, off_t offset,
                                 void** local_ptr);
#endif // MATRIX_MEM_H
