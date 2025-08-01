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
 * rack_mem.h
 *        routines to support RackMemory
 *
 *
 * IDENTIFICATION
 *        src/include/storage/rack_mem.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef RACK_MEM_H
#define RACK_MEM_H

#include <cstdint>
#include <sys/types.h>
#include "rack_mem_def.h"

constexpr size_t MIN_RACK_ALLOC_SIZE = (size_t) 1024 * 1024 * 128;
constexpr size_t MAX_RACK_ALLOC_SIZE = (size_t) 1024 * 1024 * 1024 * 4;
constexpr double MAX_RACK_MEMORY_PERCENT = 0.25;

#define MAX_HOSTNAME_LENGTH 48
#define MAX_SOCKET_NUM 2
#define MAX_HOST_NUM 16

#ifdef __cplusplus
extern "C" {
#endif

struct SocketInfo {
    int memTotal;
    int memUsed;
    int memExport;
    int memImport;
};

struct HostInfo {
    char hostName[MAX_HOSTNAME_LENGTH];
    int num;
    SocketInfo socket[MAX_SOCKET_NUM];
};

struct ClusterInfo {
    int num;
    HostInfo host[MAX_HOST_NUM];
};
typedef struct tagRackMemMallocResult {
    int code;
    void *ptr;
} RackMemMallocResult;

void *RackMemMalloc(size_t size, PerfLevel perfLevel, intptr_t attr);

typedef void (*AsyncFreeCallBack)(intptr_t ctx, int result);

typedef void (*AsyncMallocCallBack)(intptr_t ctx, RackMemMallocResult *result);

int RackMemMallocAsync(size_t size, PerfLevel perfLevel, intptr_t attr, AsyncMallocCallBack func, intptr_t ctx);

int RackMemFree(void *ptr);

int RackMemFreeAsync(void* ptr, AsyncFreeCallBack func, intptr_t ctx);

int RackMemLookupClusterStatistic(ClusterInfo *cluster);
#ifdef __cplusplus
}
#endif

#endif
