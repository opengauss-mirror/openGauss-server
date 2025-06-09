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
 * rack_mem_shm.h
 *        routines to support RackMemory
 *
 *
 * IDENTIFICATION
 *        src/include/storage/rack_mem_shm.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef RACK_MEM_SHM_H
#define RACK_MEM_SHM_H
#include <cstdint>
#include <sys/types.h>
#include "rack_mem_def.h"

#ifdef __cplusplus
extern "C" {
#endif

#ifndef MAX_REGIONS_NUM
#define MAX_REGIONS_NUM 6
#endif

typedef enum RackMemShmRegionType {
    ALL2ALL_SHARE = 0,
    ONE2ALL_SHARE,
    INCLUDE_ALL_TYPE,
} ShmRegionType;

typedef struct TagRackMemSHMRegionDesc {
    PerfLevel perfLevel;
    ShmRegionType type;
    int num;
    char nodeId[MEM_TOPOLOGY_MAX_HOSTS][MEM_MAX_ID_LENGTH];
} SHMRegionDesc;

typedef struct TagRackMemSHMRegions {
    int num;
    SHMRegionDesc region[MAX_REGIONS_NUM];
} SHMRegions;

typedef struct TagRackMemSHMRegionInfo {
    int num;
    int64_t info[0];
} SHMRegionInfo;

typedef enum RackMemShmCacheOpt { RACK_FLUSH = 0, RACK_INVALID } ShmCacheOpt;

int RackMemShmLookupShareRegions(const char *baseNid, ShmRegionType type, SHMRegions *regions);

int RackMemShmLookupRegionInfo(SHMRegionDesc *region, SHMRegionInfo *info);

int RackMemShmCreate(char *name, uint64_t size, const char *baseNid, SHMRegionDesc *shmRegion);

void *RackMemShmMmap(void *start, size_t length, int prot, int flags, const char *name, off_t offset);

int RackMemShmCacheOpt(void *start, size_t length, ShmCacheOpt type);

int RackMemShmUnmmap(void *start, size_t length);

int RackMemShmDelete(char *name);

#ifdef __cplusplus
}
#endif

#endif
