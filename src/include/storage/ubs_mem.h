/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
 * ubs_mem.h
 *        routines to support UBSMemory
 *
 *
 * IDENTIFICATION
 *        src/include/storage/ubs_mem.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef __UBSM_SHMEM_H__
#define __UBSM_SHMEM_H__

#include "ubs_mem_def.h"

#ifdef __cplusplus
extern "C" {
#endif

SHMEM_API int ubsmem_init_attributes(ubsmem_options_t *ubsm_shmem_opts);

SHMEM_API int ubsmem_initialize(const ubsmem_options_t *ubsm_shmem_opts);

SHMEM_API int ubsmem_finalize(void);

SHMEM_API int ubsmem_set_logger_level(int level);

SHMEM_API int ubsmem_set_extern_logger(void (*func)(int level, const char *msg));

SHMEM_API int ubsmem_lookup_regions(ubsmem_regions_t* regions);

SHMEM_API int ubsmem_create_region(const char *region_name, size_t size, const ubsmem_region_attributes_t *reg_attr);

SHMEM_API int ubsmem_lookup_region(const char *region_name, ubsmem_region_desc_t *region_desc);

SHMEM_API int ubsmem_destroy_region(const char *region_name);

SHMEM_API int ubsmem_shmem_allocate(
    const char *region_name, const char *name, size_t size, mode_t mode, uint64_t flags);

SHMEM_API int ubsmem_shmem_deallocate(const char *name);

SHMEM_API int ubsmem_shmem_map(void *addr, size_t length, int prot, int flags, const char *name, off_t offset,
                               void **local_ptr);

SHMEM_API int ubsmem_shmem_unmap(void *local_ptr, size_t length);

SHMEM_API int ubsmem_shmem_set_ownership(const char *name, void *start, size_t length, int prot);

SHMEM_API int ubsmem_shmem_write_lock(const char *name);
SHMEM_API int ubsmem_shmem_read_lock(const char *name);
SHMEM_API int ubsmem_shmem_unlock(const char *name);

SHMEM_API int ubsmem_shmem_list_lookup(const char *prefix, ubsmem_shmem_desc_t *shm_list, uint32_t *shm_cnt);
SHMEM_API int ubsmem_shmem_lookup(const char *name, ubsmem_shmem_info_t *shm_info);
SHMEM_API int ubsmem_shmem_attach(const char *name);
SHMEM_API int ubsmem_shmem_detach(const char *name);

SHMEM_API int ubsmem_lease_malloc(const char *region_name, size_t size, ubsmem_distance_t mem_distance, uint64_t flags,
                                  void **local_ptr);

SHMEM_API int ubsmem_lease_free(void *local_ptr);

SHMEM_API int ubsmem_lookup_cluster_statistic(ubsmem_cluster_info_t* info);

SHMEM_API int ubsmem_shmem_faults_register(shmem_faults_func registerFunc);

SHMEM_API int ubsmem_local_nid_query(uint32_t* nid);

#ifdef __cplusplus
}
#endif

#endif  // __UBSM_SHMEM_H__