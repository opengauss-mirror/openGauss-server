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
 * mm_global_api.cpp
 *    Global memory API provides objects that can be allocated by one thread,
 *    and be used and de-allocated by other threads.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/memory/mm_global_api.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <cstdint>
#include <pthread.h>
#include <cstring>

#include "mm_global_api.h"
#include "mm_cfg.h"
#include "mm_numa.h"
#include "mot_error.h"
#include "mm_raw_chunk_dir.h"
#include "memory_statistics.h"
#include "utilities.h"
#include "session_context.h"
#include "mm_session_allocator.h"
#include "mm_lock.h"
#include "mm_raw_chunk.h"
#include "memory_statistics.h"

namespace MOT {

DECLARE_LOGGER(GlobalApi, Memory)

struct MemGlobalAllocator {
    MemLock m_lock;
    MemSessionAllocator m_allocator;
};

static_assert(sizeof(MemGlobalAllocator) == L1_ALIGNED_SIZEOF(MemGlobalAllocator),
    "sizeof(MemGlobalAllocator) must be 64 byte (L1 cache line size) aligned");

// Global variables
// currently we use the session allocators that are bound to local chunk pools
static MemGlobalAllocator** globalAllocators = nullptr;

// Helpers
static int GlobalAllocatorsInit();
static void GlobalAllocatorsDestroy();
static void GlobalAllocatorsPrint(LogLevel logLevel);
static void GlobalAllocatorsToString(StringBuffer* stringBuffer);

extern int MemGlobalApiInit()
{
    MOT_LOG_TRACE("Initializing Global Memory API");
    int result = GlobalAllocatorsInit();
    if (result == 0) {
        MOT_LOG_TRACE("Global Memory API initialized successfully");
    } else {
        MOT_LOG_PANIC("Global Memory API initialization failed, see errors above");
    }

    return result;
}

extern void MemGlobalApiDestroy()
{
    MOT_LOG_TRACE("Destroying Global Memory API");
    GlobalAllocatorsPrint(LogLevel::LL_TRACE);
    GlobalAllocatorsDestroy();
    MOT_LOG_TRACE("Global Memory API destroyed");
}

inline void* MemGlobalAllocInline(uint64_t objectSizeBytes, uint32_t alignment, int node)
{
    void* object = nullptr;
    if (objectSizeBytes <= MEM_SESSION_MAX_ALLOC_SIZE) {
        MemLockAcquire(&globalAllocators[node]->m_lock);
        if (alignment > 0) {
            object = MemSessionAllocatorAllocAligned(&globalAllocators[node]->m_allocator, objectSizeBytes, alignment);
        } else {
            object = MemSessionAllocatorAlloc(&globalAllocators[node]->m_allocator, objectSizeBytes);
        }
        MemLockRelease(&globalAllocators[node]->m_lock);
    } else if ((alignment == 0) || ((PAGE_SIZE_BYTES % alignment) == 0)) {
        // page size is a multiple of required alignment
        // allocate as a large/huge allocation, since we have native page alignment
        MOT_LOG_DEBUG("Trying to allocate %" PRIu64 " global memory bytes as huge memory", objectSizeBytes);
        object = MemHugeAlloc(objectSizeBytes,
            node,
            MEM_ALLOC_GLOBAL,
            &(globalAllocators[node]->m_allocator.m_hugeChunkList),
            &globalAllocators[node]->m_lock);
    } else {
        MOT_REPORT_ERROR(MOT_ERROR_INVALID_MEMORY_SIZE,
            "Global Memory Allocation",
            "Unsupported alignment %u and size %" PRIu64 " for aligned allocations",
            alignment,
            objectSizeBytes);
    }

    return object;
}

extern void* MemGlobalAlloc(uint64_t objectSizeBytes)
{
    return MemGlobalAllocInline(objectSizeBytes, 0, MOTCurrentNumaNodeId);
}

extern void* MemGlobalAllocAligned(uint64_t objectSizeBytes, uint32_t alignment)
{
    return MemGlobalAllocInline(objectSizeBytes, alignment, MOTCurrentNumaNodeId);
}

extern void* MemGlobalAllocOnNode(uint64_t objectSizeBytes, int node)
{
    return MemGlobalAllocInline(objectSizeBytes, 0, node);
}

extern void* MemGlobalAllocAlignedOnNode(uint64_t objectSizeBytes, uint32_t alignment, int node)
{
    return MemGlobalAllocInline(objectSizeBytes, alignment, node);
}

static void* MemGlobalReallocSmall(void* object, uint64_t newSizeBytes, MemReallocFlags flags, int node)
{
    void* newObject = nullptr;
    errno_t erc;

    if (newSizeBytes <= MEM_SESSION_MAX_ALLOC_SIZE) {
        MOT_LOG_DEBUG("Reallocating %" PRIu64 " global memory bytes from global allocator %d", newSizeBytes, node);
        MemLockAcquire(&globalAllocators[node]->m_lock);
        newObject = MemSessionAllocatorRealloc(&globalAllocators[node]->m_allocator, object, newSizeBytes, flags);
        MemLockRelease(&globalAllocators[node]->m_lock);
    } else {
        MOT_LOG_DEBUG("Reallocating %" PRIu64 " global memory bytes from global allocator %d into huge buffer",
            newSizeBytes,
            node);
        newObject = MemGlobalAlloc(newSizeBytes);
        if (newObject) {
            MemSessionObjectHeader* objectHeader =
                (MemSessionObjectHeader*)((char*)object - MEM_SESSION_OBJECT_HEADER_LEN);
            uint64_t objectSizeBytes = (uint64_t)objectHeader->m_objectSize.m_realSize;
            if (flags == MEM_REALLOC_COPY) {
                // attention: new size may be smaller
                erc = memcpy_s(newObject, newSizeBytes, object, std::min(newSizeBytes, objectSizeBytes));
                securec_check(erc, "\0", "\0");
            } else if (flags == MEM_REALLOC_ZERO) {
                erc = memset_s(newObject, newSizeBytes, 0, newSizeBytes);
                securec_check(erc, "\0", "\0");
            } else if (flags == MEM_REALLOC_COPY_ZERO) {
                // attention: new size may be smaller
                erc = memcpy_s(newObject, newSizeBytes, object, std::min(newSizeBytes, objectSizeBytes));
                securec_check(erc, "\0", "\0");
                if (newSizeBytes > objectSizeBytes) {
                    erc = memset_s(((char*)newObject) + objectSizeBytes,
                        newSizeBytes - objectSizeBytes,
                        0,
                        newSizeBytes - objectSizeBytes);
                    securec_check(erc, "\0", "\0");
                }
            }
            MemSessionAllocatorFree(&globalAllocators[node]->m_allocator, object);
        }
    }

    return newObject;
}

static void* MemGlobalReallocHuge(
    void* object, uint64_t newSizeBytes, MemReallocFlags flags, MemRawChunkHeader* rawChunk, int node)
{
    void* newObject = nullptr;
    errno_t erc;

    if (newSizeBytes > MEM_SESSION_MAX_ALLOC_SIZE) {
        newObject = MemHugeRealloc((MemVirtualHugeChunkHeader*)rawChunk,
            object,
            newSizeBytes,
            flags,
            &(globalAllocators[node]->m_allocator.m_hugeChunkList),
            &globalAllocators[node]->m_lock);
    } else {
        MOT_LOG_TRACE("Reallocating %" PRIu64 " global memory bytes from huge to small buffer on global allocator %d",
            newSizeBytes,
            node);
        newObject = MemGlobalAlloc(newSizeBytes);
        if (newObject) {
            uint64_t objectSizeBytes = ((MemVirtualHugeChunkHeader*)rawChunk)->m_objectSizeBytes;
            if (flags == MEM_REALLOC_COPY) {
                // attention: new size may be smaller
                erc = memcpy_s(newObject, newSizeBytes, object, std::min(newSizeBytes, objectSizeBytes));
                securec_check(erc, "\0", "\0");
            } else if (flags == MEM_REALLOC_ZERO) {
                erc = memset_s(newObject, newSizeBytes, 0, newSizeBytes);
                securec_check(erc, "\0", "\0");
            } else if (flags == MEM_REALLOC_COPY_ZERO) {
                // attention: new size may be smaller
                erc = memcpy_s(newObject, newSizeBytes, object, std::min(newSizeBytes, objectSizeBytes));
                securec_check(erc, "\0", "\0");
                if (newSizeBytes > objectSizeBytes) {
                    erc = memset_s(((char*)newObject) + objectSizeBytes,
                        newSizeBytes - objectSizeBytes,
                        0,
                        newSizeBytes - objectSizeBytes);
                    securec_check(erc, "\0", "\0");
                }
            }
            MemHugeFree((MemVirtualHugeChunkHeader*)rawChunk,
                object,
                &(globalAllocators[node]->m_allocator.m_hugeChunkList),
                &globalAllocators[node]->m_lock);
        }
    }

    return newObject;
}

extern void* MemGlobalRealloc(void* object, uint64_t newSizeBytes, MemReallocFlags flags)
{
    void* result = nullptr;
    if (object == nullptr) {
        result = MemGlobalAlloc(newSizeBytes);
        if (result != nullptr) {
            if (flags == MEM_REALLOC_ZERO) {
                errno_t erc = memset_s(result, newSizeBytes, 0, newSizeBytes);
                securec_check(erc, "\0", "\0");
            }
        }
    } else {
        // we search the object's chunk so we can tell to which node it belongs
        MemRawChunkHeader* chunk = (MemRawChunkHeader*)MemRawChunkDirLookup(object);
        if (unlikely(chunk == nullptr)) {
            MOT_LOG_ERROR("Attempt to free corrupt global object %p: source chunk not found", object);
        } else {
            if (likely(chunk->m_chunkType == MEM_CHUNK_TYPE_SMALL_OBJECT)) {
                result = MemGlobalReallocSmall(object, newSizeBytes, flags, chunk->m_node);
            } else if (chunk->m_chunkType == MEM_CHUNK_TYPE_HUGE_OBJECT) {
                result = MemGlobalReallocHuge(object, newSizeBytes, flags, chunk, chunk->m_node);
            } else {
                MOT_LOG_ERROR(
                    "Attempt to free invalid global object %p: source chunk type %s is not a small object chunk",
                    object,
                    MemChunkTypeToString(chunk->m_chunkType));
            }
        }
    }
    return result;
}

extern void MemGlobalFree(void* object)
{
    // we search the object's chunk so we can tell to which node it belongs
    MemRawChunkHeader* chunk = (MemRawChunkHeader*)MemRawChunkDirLookup(object);
    if (unlikely(chunk == nullptr)) {
        MOT_LOG_ERROR("Attempt to free corrupt global object %p: source chunk not found", object);
    } else {
        int node = chunk->m_node;
        if (likely(chunk->m_chunkType == MEM_CHUNK_TYPE_SMALL_OBJECT)) {
            MemLockAcquire(&globalAllocators[node]->m_lock);
            MemSessionAllocatorFree(&globalAllocators[node]->m_allocator, object);
            MemLockRelease(&globalAllocators[node]->m_lock);
        } else if (chunk->m_chunkType == MEM_CHUNK_TYPE_HUGE_OBJECT) {
            MemHugeFree((MemVirtualHugeChunkHeader*)chunk,
                object,
                &(globalAllocators[node]->m_allocator.m_hugeChunkList),
                &globalAllocators[node]->m_lock);
        } else {
            MOT_LOG_ERROR("Attempt to free invalid global object %p: source chunk type %s is not a small object chunk",
                object,
                MemChunkTypeToString(chunk->m_chunkType));
        }
    }
}

extern void MemGlobalApiPrint(const char* name, LogLevel logLevel, MemReportMode reportMode /* = MEM_REPORT_SUMMARY */)
{
    if (MOT_CHECK_LOG_LEVEL(logLevel)) {
        StringBufferApply([name, logLevel, reportMode](StringBuffer* stringBuffer) {
            MemGlobalApiToString(0, name, stringBuffer, reportMode);
            MOT_LOG(logLevel, "%s", stringBuffer->m_buffer);
        });
    }
}

extern void MemGlobalApiToString(
    int indent, const char* name, StringBuffer* stringBuffer, MemReportMode reportMode /* = MEM_REPORT_SUMMARY */)
{
    if (reportMode == MEM_REPORT_SUMMARY) {
        MemGlobalAllocatorStats* globalStatsArray =
            (MemGlobalAllocatorStats*)calloc(g_memGlobalCfg.m_nodeCount, sizeof(MemGlobalAllocatorStats));
        if (globalStatsArray == nullptr) {
            return;
        }
        uint32_t entries = MemGlobalGetAllStats(globalStatsArray, g_memGlobalCfg.m_nodeCount);
        MemGlobalAllocatorStats aggStats;
        errno_t erc = memset_s(&aggStats, sizeof(MemGlobalAllocatorStats), 0, sizeof(MemGlobalAllocatorStats));
        securec_check(erc, "\0", "\0");
        for (uint32_t i = 0; i < entries; ++i) {
            aggStats.m_reservedSize += globalStatsArray[i].m_reservedSize;
            aggStats.m_usedSize += globalStatsArray[i].m_usedSize;
            aggStats.m_realUsedSize += globalStatsArray[i].m_realUsedSize;
        }
        StringBufferAppend(stringBuffer,
            "%*sGlobal Memory %s [Current]: Reserved = %" PRIu64 " MB, Requested = %" PRIu64 " MB, Allocated = %" PRIu64
            " MB",
            indent,
            "",
            name,
            aggStats.m_reservedSize / MEGA_BYTE,
            aggStats.m_usedSize / MEGA_BYTE,
            aggStats.m_realUsedSize / MEGA_BYTE);
        free(globalStatsArray);
    } else {
        StringBufferAppend(stringBuffer, "%*sGlobal Memory %s Report:\n", indent, "", name);
        GlobalAllocatorsToString(stringBuffer);
    }
}

extern void MemGlobalGetStats(int node, MemGlobalAllocatorStats* objectStats)
{
    MemLockAcquire(&globalAllocators[node]->m_lock);
    MemSessionAllocatorGetStats(&globalAllocators[node]->m_allocator, objectStats);
    MemLockRelease(&globalAllocators[node]->m_lock);
}

extern void MemGlobalPrintStats(const char* name, LogLevel logLevel, MemGlobalAllocatorStats* objectStats)
{
    if (MOT_CHECK_LOG_LEVEL(logLevel)) {
        char fullName[128];
        errno_t erc = snprintf_s(fullName, sizeof(fullName), sizeof(fullName) - 1, "%s (Global Memory API)", name);
        securec_check_ss(erc, "\0", "\0");
        MemSessionAllocatorPrintStats(fullName, logLevel, objectStats);
    }
}

extern uint32_t MemGlobalGetAllStats(MemGlobalAllocatorStats* globalStatsArray, uint32_t nodeCount)
{
    uint32_t entriesReported = 0;

    for (uint32_t node = 0; node < g_memGlobalCfg.m_nodeCount; ++node) {
        MemGlobalAllocator* objectAllocator = globalAllocators[node];
        if (objectAllocator) {
            MemGlobalAllocatorStats* stats = &globalStatsArray[entriesReported];
            if (MemLockTryAcquire(&objectAllocator->m_lock) == MOT_NO_ERROR) {
                MemSessionAllocatorGetStats(&objectAllocator->m_allocator, stats);
                MemLockRelease(&objectAllocator->m_lock);
                if (++entriesReported == nodeCount) {
                    break;
                }
            }
        }
    }

    return entriesReported;
}

extern void MemGlobalPrintAllStats(const char* name, LogLevel logLevel)
{
    if (MOT_CHECK_LOG_LEVEL(logLevel)) {
        MemGlobalAllocatorStats* globalStatsArray =
            (MemGlobalAllocatorStats*)calloc(g_memGlobalCfg.m_nodeCount, sizeof(MemGlobalAllocatorStats));
        if (globalStatsArray) {
            uint32_t statCount = MemGlobalGetAllStats(globalStatsArray, g_memGlobalCfg.m_nodeCount);

            MOT_LOG(logLevel, "%s Global Memory Allocator Report:", name);
            for (uint32_t i = 0; i < statCount; ++i) {
                MemGlobalAllocatorStats* stats = &globalStatsArray[i];
                MemGlobalPrintStats(name, logLevel, stats);
            }
            free(globalStatsArray);
        }
    }
}

extern void MemGlobalPrintSummary(const char* name, LogLevel logLevel, bool fullReport /* = false */)
{
    if (MOT_CHECK_LOG_LEVEL(logLevel)) {
        MemGlobalAllocatorStats* globalStatsArray =
            (MemGlobalAllocatorStats*)calloc(g_memGlobalCfg.m_nodeCount, sizeof(MemGlobalAllocatorStats));
        if (globalStatsArray) {
            MemGlobalAllocatorStats aggStats;
            errno_t erc = memset_s(&aggStats, sizeof(MemGlobalAllocatorStats), 0, sizeof(MemGlobalAllocatorStats));
            securec_check(erc, "\0", "\0");
            uint32_t statCount = MemGlobalGetAllStats(globalStatsArray, g_memGlobalCfg.m_nodeCount);
            for (uint32_t i = 0; i < statCount; ++i) {
                MemGlobalAllocatorStats* stats = &globalStatsArray[i];
                aggStats.m_reservedSize += stats->m_reservedSize;
                aggStats.m_usedSize += stats->m_usedSize;
                aggStats.m_realUsedSize += stats->m_realUsedSize;
                aggStats.m_maxUsedSize += stats->m_maxUsedSize;
                aggStats.m_maxRealUsedSize += stats->m_maxRealUsedSize;
            }
            free(globalStatsArray);

            MOT_LOG(logLevel,
                "Global Memory API %s [Current]: Reserved = %" PRIu64 " MB, Requested = %" PRIu64
                " MB, Allocated = %" PRIu64 " MB",
                name,
                aggStats.m_reservedSize / MEGA_BYTE,
                aggStats.m_usedSize / MEGA_BYTE,
                aggStats.m_realUsedSize / MEGA_BYTE);

            if (fullReport) {
                double memUtilInternal = (((double)aggStats.m_usedSize) / ((double)aggStats.m_realUsedSize)) * 100.0;
                double memUtilExternal = (((double)aggStats.m_usedSize) / aggStats.m_reservedSize) * 100.0;
                uint32_t minRequiredChunks =
                    (aggStats.m_usedSize + MEM_CHUNK_SIZE_MB * MEGA_BYTE - 1) / (MEM_CHUNK_SIZE_MB * MEGA_BYTE);
                double maxUtilExternal = (((double)aggStats.m_usedSize) /
                                             static_cast<uint64_t>(minRequiredChunks * MEM_CHUNK_SIZE_MB * MEGA_BYTE)) *
                                         100.0;

                MOT_LOG(logLevel,
                    "Global Memory API %s [Peak]:    Reserved = %" PRIu64 " MB, Requested = %" PRIu64
                    " MB, Allocated = %" PRIu64 " MB",
                    name,
                    aggStats.m_maxReservedSize / MEGA_BYTE,
                    aggStats.m_maxUsedSize / MEGA_BYTE,
                    aggStats.m_maxRealUsedSize / MEGA_BYTE);
                MOT_LOG(logLevel,
                    "Global Memory API %s [Util]: Int. %0.2f%%, Ext. %0.2f%%, Max Ext. %0.2f%%",
                    name,
                    memUtilInternal,
                    memUtilExternal,
                    maxUtilExternal);
            }
        }
    }
}

static int GlobalAllocatorsInit()
{
    int result = 0;
    errno_t erc;

    // allocate nullified buffer so in case of error we can safely destroy only those that were initialized
    globalAllocators = (MemGlobalAllocator**)calloc(g_memGlobalCfg.m_nodeCount, sizeof(MemGlobalAllocator*));
    if (globalAllocators == nullptr) {
        SetLastError(MOT_ERROR_OOM, MOT_SEVERITY_FATAL);
        MOT_REPORT_PANIC(MOT_ERROR_OOM,
            "Global Memory API Initialization",
            "Failed to allocate array for global allocations (element count: %u, element size: %u), aborting.",
            (unsigned)g_memGlobalCfg.m_nodeCount,
            (unsigned)sizeof(MemGlobalAllocator*));
        result = MOT_ERROR_OOM;
    } else {
        for (int i = 0; i < (int)g_memGlobalCfg.m_nodeCount; ++i) {
            // prepare allocators for all size classes on this node
            globalAllocators[i] = (MemGlobalAllocator*)MemNumaAllocLocal(sizeof(MemGlobalAllocator), i);
            if (globalAllocators[i] == nullptr) {
                MOT_REPORT_PANIC(MOT_ERROR_OOM,
                    "Global Memory API Initialization",
                    "Failed to allocate %u bytes for global allocator data on node %d, aborting",
                    (unsigned)sizeof(MemGlobalAllocator),
                    i);
                result = MOT_ERROR_OOM;
                break;
            }

            MOT_ASSERT((uintptr_t)(globalAllocators[i]) == L1_ALIGNED_PTR(globalAllocators[i]));

            // nullify all bytes so in case of error we can safely destroy only those that were initialized
            erc = memset_s(globalAllocators[i], sizeof(MemGlobalAllocator), 0, sizeof(MemGlobalAllocator));
            securec_check(erc, "\0", "\0");
            result = MemLockInitialize(&globalAllocators[i]->m_lock);
            if (result != 0) {
                MOT_REPORT_PANIC(MOT_ERROR_OOM,
                    "Global Memory API Initialization",
                    "Failed to initialize lock for global object allocator on node %d",
                    i);
                MemNumaFreeLocal(globalAllocators[i], sizeof(MemGlobalAllocator), i);
                globalAllocators[i] = nullptr;
                break;
            }
            MemSessionAllocatorInit(
                &globalAllocators[i]->m_allocator, 0, 0, nullptr, 0, MEM_ALLOC_GLOBAL);  // no special reservation
            globalAllocators[i]->m_allocator.m_nodeId = i;                               // fix node identifier
        }
    }

    if (result != 0) {
        // safe cleanup in case of error
        GlobalAllocatorsDestroy();
    }

    return result;
}

static void GlobalAllocatorsDestroy()
{
    if (globalAllocators != nullptr) {
        for (uint32_t i = 0; i < g_memGlobalCfg.m_nodeCount; ++i) {
            if (globalAllocators[i] != nullptr) {
                (void)MemLockDestroy(&globalAllocators[i]->m_lock);
                MemSessionAllocatorDestroy(&globalAllocators[i]->m_allocator);
                MemNumaFreeLocal(globalAllocators[i], sizeof(MemGlobalAllocator), i);
                globalAllocators[i] = nullptr;
            }
        }
        free(globalAllocators);
        globalAllocators = nullptr;
    }
}

static void GlobalAllocatorsPrint(LogLevel logLevel)
{
    errno_t erc;
    if (globalAllocators && MOT_CHECK_LOG_LEVEL(logLevel)) {
        for (uint32_t i = 0; i < g_memGlobalCfg.m_nodeCount; ++i) {
            if (globalAllocators[i]) {
                char name[64];
                erc = snprintf_s(name, sizeof(name), sizeof(name) - 1, "GlobalAllocator[%u]", i);
                securec_check_ss(erc, "\0", "\0");
                MemLockAcquire(&globalAllocators[i]->m_lock);
                MemSessionAllocatorPrintCurrentStats(name, logLevel, &globalAllocators[i]->m_allocator);
                MemLockRelease(&globalAllocators[i]->m_lock);
            }
        }
    }
}

static void GlobalAllocatorsToString(StringBuffer* stringBuffer)
{
    errno_t erc;
    if (globalAllocators) {
        for (uint32_t i = 0; i < g_memGlobalCfg.m_nodeCount; ++i) {
            if (globalAllocators[i]) {
                char name[64];
                erc = snprintf_s(name, sizeof(name), sizeof(name) - 1, "GlobalAllocator[%u]", i);
                securec_check_ss(erc, "\0", "\0");
                MemLockAcquire(&globalAllocators[i]->m_lock);
                MemSessionAllocatorToString(2, name, &globalAllocators[i]->m_allocator, stringBuffer);
                MemLockRelease(&globalAllocators[i]->m_lock);
                StringBufferAppend(stringBuffer, "\n");
            }
        }
    }
}

}  // namespace MOT

extern "C" void MemGlobalApiDump()
{
    MOT::StringBufferApply([](MOT::StringBuffer* stringBuffer) {
        MOT::MemGlobalApiToString(0, "Debug Dump", stringBuffer, MOT::MEM_REPORT_DETAILED);
        (void)fprintf(stderr, "%s", stringBuffer->m_buffer);
        (void)fflush(stderr);
    });
}

extern "C" int MemGlobalApiAnalyze(void* buffer)
{
    for (uint32_t i = 0; i < MOT::g_memGlobalCfg.m_nodeCount; ++i) {
        (void)fprintf(stderr, "Searching buffer %p in global allocator %u...\n", buffer, i);
        if (MOT::globalAllocators[i]) {
            if (MemSessionAllocatorAnalyze(MOT::globalAllocators[i], buffer)) {
                return 1;
            }
        }
    }
    return 0;
}
