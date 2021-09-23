/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
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
 * sys_numa_api.h
 *    System NUMA APIs.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/memory/sys_numa_api.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef SYS_NUMA_API_H
#define SYS_NUMA_API_H

#include <stddef.h>

// Memory policy mode constants (adapted from /usr/include/linux/mempolicy.h)
#define MPOL_DEFAULT 0
#define MPOL_PREFERRED 1
#define MPOL_BIND 2
#define MPOL_INTERLEAVE 3
#define MPOL_LOCAL 4
#define MPOL_MAX 5

// Flags for sys_get_mempolicy
#define MPOL_F_NODE (1 << 0)
#define MPOL_F_ADDR (1 << 1)
#define MPOL_F_MEMS_ALLOWED (1 << 2)

namespace MOT {
/** @brief Initialize NUMA API. */
void MotSysNumaInit();

/** @brief Free all resources associates with NUMA API. */
void MotSysNumaDestroy();

/* Alloc memory page interleaved on all nodes. */
void* MotSysNumaAllocInterleaved(size_t size);

/* Alloc memory located on node */
void* MotSysNumaAllocOnNode(size_t size, int node);

/* Alloc memory on local node */
void* MotSysNumaAllocLocal(size_t size);

/* Alloc memory page interleaved on all nodes. */
void* MotSysNumaAllocAlignedInterleaved(size_t size, size_t align);

/* Alloc memory located on node */
void* MotSysNumaAllocAlignedOnNode(size_t size, size_t align, int node);

/* Alloc memory on local node */
void* MotSysNumaAllocAlignedLocal(size_t size, size_t align);

/* Free memory allocated by the functions above */
void MotSysNumaFree(void* mem, size_t size);

/* NUMA support available. If this returns a negative value all other function
   in this library are undefined. */
int MotSysNumaAvailable();

/* report the node of the specified cpu */
int MotSysNumaGetNode(int cpu);

int MotSysNumaConfiguredNodes();

/* When strict fail allocation when memory cannot be allocated in target node(s). */
void MotSysNumaSetBindPolicy(int strict);

/* Fail when existing memory has incompatible policy */
void MotSysNumaSetStrict(int flag);

/* Some node to preferably allocate memory from for task. */
void MotSysNumaSetPreferred(int node);

bool MotSysNumaCpuAllowed(int cpu);
}  // namespace MOT
#endif /* MM_NUMA_API_H */
