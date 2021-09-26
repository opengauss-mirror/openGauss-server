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
 * mm_virtual_huge_chunk.cpp
 *    A virtual huge chunk header.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/memory/mm_virtual_huge_chunk.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "mm_virtual_huge_chunk.h"
#include "mm_def.h"
#include "mm_cfg.h"
#include "mm_numa.h"
#include "mm_lock.h"
#include "mot_error_codes.h"
#include "mot_atomic_ops.h"
#include "utilities.h"

#include <string.h>

namespace MOT {
DECLARE_LOGGER(VirtualHugeChunkHeader, Memory)

// maintenance chunk (holds virtual huge chunk headers)
struct PACKED MemMaintenanceChunkHeader {
    MemChunkType m_chunkType;     // L1 offset [0-4]
    int16_t m_node;               // L1 offset [4-6]
    MemAllocType m_allocType;     // L1 offset [6-8]
    uint32_t m_headersAllocated;  // L1 offset [8-12]
    int8_t m_padding[4];  // L1 offset [12-16] (align struct size to 16 bytes so that following m_headers start in an
                          // aligned address)
    MemVirtualHugeChunkHeader m_headers[0];
};

#define VHUGE_HEADERS_PER_MAINT_CHUNK \
    ((PAGE_SIZE_BYTES - sizeof(MemMaintenanceChunkHeader)) / sizeof(MemVirtualHugeChunkHeader))

// Global management variables
static int g_vhcInit = 0;
static MemLock g_globalHeadersLock;
static MemLock* g_localHeadersLock[MEM_MAX_NUMA_NODES];

static MemVirtualHugeChunkHeader* g_globalFreeHeaders = nullptr;
static MemVirtualHugeChunkHeader* g_localFreeHeaders[MEM_MAX_NUMA_NODES];

// Helper functions
static void FreeHeaders(MemVirtualHugeChunkHeader* headerList);
static void FreeGlobalHeader(MemVirtualHugeChunkHeader* chunkHeader);
static void FreeLocalHeader(MemVirtualHugeChunkHeader* chunkHeader, int node);
static MemVirtualHugeChunkHeader* GetGlobalFreeHeader();
static MemVirtualHugeChunkHeader* GetLocalFreeHeader(int node);
static void AllocGlobalHeaders();
static void AllocLocalHeaders(int node);
static MemMaintenanceChunkHeader* GetMaintenanceChunk(MemVirtualHugeChunkHeader* chunkHeader);

extern int MemVirtualHugeChunkHeaderInit()
{
    int result = MemLockInitialize(&g_globalHeadersLock);
    if (result != 0) {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
            "Virtual Huge Chunk Initialization",
            "Failed to initialize virtual chunk header lock for interleaved huge chunks");
    } else {
        g_vhcInit = 1;
        errno_t erc = memset_s(g_localHeadersLock, sizeof(g_localHeadersLock), 0, sizeof(g_localHeadersLock));
        securec_check(erc, "\0", "\0");
        erc = memset_s(g_localFreeHeaders, sizeof(g_localFreeHeaders), 0, sizeof(g_localFreeHeaders));
        securec_check(erc, "\0", "\0");
        for (uint32_t i = 0; i < g_memGlobalCfg.m_nodeCount; ++i) {
            // although this is wasting a lot of memory (4K per lock), we currently have no good alternative
            // in the future: use simple raw pages for initialization allocations (just like session
            // allocator, but use one per node and one interleaved for initialization allocations).
            // currently this is not critical
            g_localHeadersLock[i] = (MemLock*)MemNumaAllocLocal(sizeof(MemLock), i);
            if (g_localHeadersLock[i] == nullptr) {
                // stop immediately
                result = MOT_ERROR_OOM;
                MOT_REPORT_ERROR(MOT_ERROR_OOM,
                    "Virtual Huge Chunk Initialization",
                    "Failed to allocate %u bytes for virtual huge chunk management lock on node %u",
                    sizeof(MemLock),
                    i);
                break;
            }
            result = MemLockInitialize(g_localHeadersLock[i]);
            if (result != 0) {
                MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                    "Virtual Huge Chunk Initialization",
                    "Failed to initialize virtual huge chunk management lock on node %u",
                    i);
                // make sure MemLockDestroy() is not called during destruction
                MemNumaFreeLocal(g_localHeadersLock[i], sizeof(MemLock), i);
                g_localHeadersLock[i] = nullptr;
                break;
            }
            g_localFreeHeaders[i] = nullptr;
        }
    }

    if (result != 0) {
        // safe cleanup
        MemVirtualHugeChunkHeaderDestroy();
    } else {
        MOT_LOG_TRACE("Virtual huge chunk header management initialized successfully");
    }

    return result;
}

extern void MemVirtualHugeChunkHeaderDestroy()
{
    if (g_vhcInit) {
        // free all headers and lock
        FreeHeaders(g_globalFreeHeaders);
        MemLockDestroy(&g_globalHeadersLock);

        for (uint32_t i = 0; i < g_memGlobalCfg.m_nodeCount; ++i) {
            // free all headers and locks
            FreeHeaders(g_localFreeHeaders[i]);
            if (g_localHeadersLock[i] != nullptr) {
                MemLockDestroy(g_localHeadersLock[i]);
                MemNumaFreeLocal(g_localHeadersLock[i], sizeof(MemLock), i);
                g_localHeadersLock[i] = nullptr;
            }
        }
        g_vhcInit = 0;
    }
}

extern MemVirtualHugeChunkHeader* MemVirtualHugeChunkHeaderAlloc(
    uint64_t size, uint64_t alignedSize, int16_t node, MemAllocType allocType)
{
    MemVirtualHugeChunkHeader* chunkHeader = nullptr;
    if (allocType == MEM_ALLOC_GLOBAL) {
        chunkHeader = GetGlobalFreeHeader();
    } else {
        chunkHeader = GetLocalFreeHeader(node);
    }
    if (chunkHeader != nullptr) {
        chunkHeader->m_chunkType = MEM_CHUNK_TYPE_HUGE_OBJECT;
        chunkHeader->m_node = node;
        chunkHeader->m_allocType = allocType;
        chunkHeader->m_chunkSizeBytes = alignedSize;
        chunkHeader->m_objectSizeBytes = size;

        MemMaintenanceChunkHeader* maintenanceChunk = GetMaintenanceChunk(chunkHeader);
        MOT_ATOMIC_INC(maintenanceChunk->m_headersAllocated);
    }
    return chunkHeader;
}

extern void MemVirtualHugeChunkHeaderFree(MemVirtualHugeChunkHeader* chunkHeader)
{
    if (chunkHeader->m_allocType == MEM_ALLOC_GLOBAL) {
        FreeGlobalHeader(chunkHeader);
    } else {
        FreeLocalHeader(chunkHeader, chunkHeader->m_node);
    }
    MemMaintenanceChunkHeader* maintenanceChunk = GetMaintenanceChunk(chunkHeader);
    MOT_ATOMIC_DEC(maintenanceChunk->m_headersAllocated);
}

static void FreeHeaders(MemVirtualHugeChunkHeader* headerList)
{
    MemVirtualHugeChunkHeader* itr = headerList;
    while (itr != nullptr) {
        MemVirtualHugeChunkHeader* next = itr->m_next;

        MemMaintenanceChunkHeader* maintenanceChunk = GetMaintenanceChunk(itr);
        if (--maintenanceChunk->m_headersAllocated == 0) {
            // can free maintenance chunk
            if (maintenanceChunk->m_allocType == MEM_ALLOC_GLOBAL) {
                MemNumaFreeGlobal(maintenanceChunk, PAGE_SIZE_BYTES);
            } else {
                MemNumaFreeLocal(maintenanceChunk, PAGE_SIZE_BYTES, maintenanceChunk->m_node);
            }
        }

        itr = next;
    }
}

static void FreeGlobalHeader(MemVirtualHugeChunkHeader* chunkHeader)
{
    MemLockAcquire(&g_globalHeadersLock);
    chunkHeader->m_next = g_globalFreeHeaders;
    chunkHeader->m_prev = nullptr;
    g_globalFreeHeaders = chunkHeader;
    MemLockRelease(&g_globalHeadersLock);
}

static void FreeLocalHeader(MemVirtualHugeChunkHeader* chunkHeader, int node)
{
    MemLockAcquire(g_localHeadersLock[node]);
    chunkHeader->m_next = g_localFreeHeaders[node];
    chunkHeader->m_prev = nullptr;
    g_localFreeHeaders[node] = chunkHeader;
    MemLockRelease(g_localHeadersLock[node]);
}

static MemVirtualHugeChunkHeader* GetGlobalFreeHeader()
{
    MemVirtualHugeChunkHeader* header = nullptr;

    MemLockAcquire(&g_globalHeadersLock);
    if (g_globalFreeHeaders == nullptr) {
        AllocGlobalHeaders();
    }
    if (g_globalFreeHeaders != nullptr) {
        header = g_globalFreeHeaders;
        g_globalFreeHeaders = g_globalFreeHeaders->m_next;
    }
    MemLockRelease(&g_globalHeadersLock);

    return header;
}

static MemVirtualHugeChunkHeader* GetLocalFreeHeader(int node)
{
    MemVirtualHugeChunkHeader* header = nullptr;

    MemLockAcquire(g_localHeadersLock[node]);
    if (!g_localFreeHeaders[node]) {
        AllocLocalHeaders(node);
    }
    if (g_localFreeHeaders[node]) {
        header = g_localFreeHeaders[node];
        g_localFreeHeaders[node] = g_localFreeHeaders[node]->m_next;
    }
    MemLockRelease(g_localHeadersLock[node]);

    return header;
}

static void InitMaintenanceChunk(MemMaintenanceChunkHeader* maintenanceHeader, int16_t node, MemAllocType allocType,
    MemVirtualHugeChunkHeader** freeList)
{
    maintenanceHeader->m_chunkType = MEM_CHUNK_TYPE_MAINTENANCE;
    maintenanceHeader->m_node = node;
    maintenanceHeader->m_allocType = allocType;
    maintenanceHeader->m_headersAllocated = 0;

    // initialize all huge buffer headers in the maintenance chunk
    uint8_t* base = (uint8_t*)maintenanceHeader->m_headers;
    for (uint32_t i = 0; i < VHUGE_HEADERS_PER_MAINT_CHUNK; ++i) {
        MemVirtualHugeChunkHeader* entry = (MemVirtualHugeChunkHeader*)(base + sizeof(MemVirtualHugeChunkHeader) * i);
        entry->m_chunkType = MEM_CHUNK_TYPE_HUGE_OBJECT;
        entry->m_node = node;
        entry->m_allocType = allocType;
        entry->m_index = i;
        entry->m_prev = nullptr;
        if (i < (VHUGE_HEADERS_PER_MAINT_CHUNK - 1)) {
            entry->m_next = (MemVirtualHugeChunkHeader*)(base + sizeof(MemVirtualHugeChunkHeader) * (i + 1));
        }
    }

    // chain headers to the global list of free headers
    MemVirtualHugeChunkHeader* entry =
        (MemVirtualHugeChunkHeader*)(base + sizeof(MemVirtualHugeChunkHeader) * (VHUGE_HEADERS_PER_MAINT_CHUNK - 1));
    entry->m_next = *freeList;
    *freeList = maintenanceHeader->m_headers;
}

static void AllocGlobalHeaders()
{
    // allocate a full page and initialize it
    MemMaintenanceChunkHeader* maintenanceHeader = (MemMaintenanceChunkHeader*)MemNumaAllocGlobal(PAGE_SIZE_BYTES);
    if (maintenanceHeader == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Virtual Huge Chunk Header Allocation",
            "Failed to allocate maintenance header in size of %u bytes from global memory",
            (unsigned)PAGE_SIZE_BYTES);
    } else {
        InitMaintenanceChunk(maintenanceHeader, 0 /* node-id */, MEM_ALLOC_GLOBAL, &g_globalFreeHeaders);
    }
}

static void AllocLocalHeaders(int node)
{
    // allocate a full page and initialize it
    MemMaintenanceChunkHeader* maintenanceHeader = (MemMaintenanceChunkHeader*)MemNumaAllocLocal(PAGE_SIZE_BYTES, node);
    if (maintenanceHeader == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Virtual Huge Chunk Header Allocation",
            "Failed to allocate maintenance header in size of %u bytes from node %d local memory",
            (unsigned)PAGE_SIZE_BYTES,
            node);
    } else {
        InitMaintenanceChunk(maintenanceHeader, node, MEM_ALLOC_LOCAL, &g_localFreeHeaders[node]);
    }
}

static MemMaintenanceChunkHeader* GetMaintenanceChunk(MemVirtualHugeChunkHeader* chunkHeader)
{
    uint8_t* headersStart = ((uint8_t*)chunkHeader) - sizeof(MemVirtualHugeChunkHeader) * chunkHeader->m_index;
    MemMaintenanceChunkHeader* maintenanceChunk =
        (MemMaintenanceChunkHeader*)(headersStart - sizeof(MemMaintenanceChunkHeader));
    return maintenanceChunk;
}
}  // namespace MOT
