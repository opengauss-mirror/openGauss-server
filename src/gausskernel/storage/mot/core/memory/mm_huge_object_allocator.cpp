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
 * mm_huge_object_allocator.cpp
 *    Memory huge object allocation infrastructure.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/memory/mm_huge_object_allocator.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <string.h>

#include "global.h"
#include "mm_huge_object_allocator.h"
#include "mm_numa.h"
#include "mm_raw_chunk_dir.h"
#include "mm_cfg.h"
#include "mot_atomic_ops.h"

namespace MOT {

DECLARE_LOGGER(HugeAlloc, Memory)

// Memory Usage Counters
static uint64_t memUsedBytes[MEM_MAX_NUMA_NODES];
static uint64_t memRequestedBytes[MEM_MAX_NUMA_NODES];
static uint64_t objectCount[MEM_MAX_NUMA_NODES];
static void MemSessionRecordHugeChunk(
    MemVirtualHugeChunkHeader* chunkHeader, MemVirtualHugeChunkHeader** chunkHeaderList);
static void MemSessioUnrecordHugeChunk(
    MemVirtualHugeChunkHeader* chunkHeader, MemVirtualHugeChunkHeader** chunkHeaderList);

extern void MemHugeInit()
{
    errno_t erc = memset_s(memUsedBytes, sizeof(memUsedBytes), 0, sizeof(memUsedBytes));
    securec_check(erc, "\0", "\0");
    erc = memset_s(memRequestedBytes, sizeof(memRequestedBytes), 0, sizeof(memRequestedBytes));
    securec_check(erc, "\0", "\0");
    erc = memset_s(objectCount, sizeof(objectCount), 0, sizeof(objectCount));
    securec_check(erc, "\0", "\0");
}

extern void MemHugeDestroy()
{
    MemHugePrint("Shutdown Report", LogLevel::LL_INFO);
}

extern void* MemHugeAlloc(uint64_t size, int node, MemAllocType allocType, MemVirtualHugeChunkHeader** chunkHeaderList,
    MemLock* lock /* = nullptr */)
{
    // compute aligned size
    uint64_t alignedSize = MEM_ALIGN(size, MEM_CHUNK_SIZE_MB * MEGA_BYTE);
    uint64_t alignment = MEM_CHUNK_SIZE_MB * MEGA_BYTE;

    // make the huge allocation
    void* chunk = NULL;
    if (allocType == MEM_ALLOC_GLOBAL) {
        chunk = MemNumaAllocAlignedGlobal(alignedSize, alignment);
    } else {
        chunk = MemNumaAllocAlignedLocal(alignedSize, alignment, node);
    }

    if (chunk != NULL) {
        // now prepare a virtual header (we have a special maintenance chunk for that)
        MemVirtualHugeChunkHeader* chunkHeader =
            MemVirtualHugeChunkHeaderAlloc(size, alignedSize, (int16_t)node, allocType);
        if (chunkHeader != NULL) {
            chunkHeader->m_chunk = chunk;
            MemRawChunkDirInsertEx(chunk, alignedSize / (MEM_CHUNK_SIZE_MB * MEGA_BYTE), chunkHeader);

            MOT_ATOMIC_ADD(memUsedBytes[node], alignedSize);
            MOT_ATOMIC_ADD(memRequestedBytes[node], size);
            MOT_ATOMIC_INC(objectCount[node]);

            if (lock != nullptr) {
                MemLockAcquire(lock);
            }
            MemSessionRecordHugeChunk(chunkHeader, chunkHeaderList);
            if (lock != nullptr) {
                MemLockRelease(lock);
            }
        } else {
            // cleanup to avoid memory leak
            MOT_LOG_TRACE("Failed to allocate virtual huge chunk header.");
            if (allocType == MEM_ALLOC_GLOBAL) {
                MemNumaFreeGlobal(chunk, alignedSize);
            } else {
                MemNumaFreeLocal(chunk, alignedSize, node);
            }

            chunk = NULL;
        }
    }

    return chunk;
}

extern void MemHugeFree(MemVirtualHugeChunkHeader* chunkHeader, void* object,
    MemVirtualHugeChunkHeader** chunkHeaderList, MemLock* lock /* = nullptr */)
{
    if (lock != nullptr) {
        MemLockAcquire(lock);
    }
    MemSessioUnrecordHugeChunk(chunkHeader, chunkHeaderList);
    if (lock != nullptr) {
        MemLockRelease(lock);
    }

    MOT_ATOMIC_SUB(memUsedBytes[chunkHeader->m_node], chunkHeader->m_chunkSizeBytes);
    MOT_ATOMIC_SUB(memRequestedBytes[chunkHeader->m_node], chunkHeader->m_objectSizeBytes);
    MOT_ATOMIC_DEC(objectCount[chunkHeader->m_node]);

    // clear the chunk directory
    MemRawChunkDirRemoveEx(object, chunkHeader->m_chunkSizeBytes / (MEM_CHUNK_SIZE_MB * MEGA_BYTE));

    // free the object
    if (chunkHeader->m_allocType == MEM_ALLOC_GLOBAL) {
        MemNumaFreeGlobal(object, chunkHeader->m_chunkSizeBytes);
    } else {
        MemNumaFreeLocal(object, chunkHeader->m_chunkSizeBytes, chunkHeader->m_node);
    }
    // recycle the header
    MemVirtualHugeChunkHeaderFree(chunkHeader);
}

extern void* MemHugeRealloc(MemVirtualHugeChunkHeader* chunkHeader, void* object, uint64_t newSizeBytes,
    MemReallocFlags flags, MemVirtualHugeChunkHeader** chunkHeaderList, MemLock* lock /* = nullptr */)
{
    errno_t erc;
    // locate the proper list to see if the size still fits
    void* newObject = NULL;
    if (chunkHeader->m_chunkSizeBytes >= newSizeBytes) {  // new object fits
        newObject = object;                               // size fits, we are done
        MOT_ATOMIC_SUB(memRequestedBytes[chunkHeader->m_node], chunkHeader->m_objectSizeBytes);
        MOT_ATOMIC_ADD(memRequestedBytes[chunkHeader->m_node], newSizeBytes);
        chunkHeader->m_objectSizeBytes = newSizeBytes;
    } else {
        // allocate new object, copy/zero data if required and free old object
        newObject = MemHugeAlloc(newSizeBytes, chunkHeader->m_node, chunkHeader->m_allocType, chunkHeaderList, lock);
        if (newObject != NULL) {
            uint64_t objectSizeBytes = chunkHeader->m_objectSizeBytes;
            if (flags == MEM_REALLOC_COPY) {
                // attention: new object size may be smaller
                erc = memcpy_s(newObject, newSizeBytes, object, std::min(newSizeBytes, objectSizeBytes));
                securec_check(erc, "\0", "\0");
            } else if (flags == MEM_REALLOC_ZERO) {
                erc = memset_s(newObject, newSizeBytes, 0, newSizeBytes);
                securec_check(erc, "\0", "\0");
            } else if (flags == MEM_REALLOC_COPY_ZERO) {
                // attention: new object size may be smaller
                erc = memcpy_s(newObject, newSizeBytes, object, std::min(newSizeBytes, objectSizeBytes));
                securec_check(erc, "\0", "\0");
                if (newSizeBytes > chunkHeader->m_objectSizeBytes) {
                    erc = memset_s(((char*)newObject) + objectSizeBytes,
                        newSizeBytes - objectSizeBytes,
                        0,
                        newSizeBytes - objectSizeBytes);
                    securec_check(erc, "\0", "\0");
                }
            }
            MemHugeFree(chunkHeader, object, chunkHeaderList, lock);
        }
    }
    return newObject;
}

static void MemSessionRecordHugeChunk(
    MemVirtualHugeChunkHeader* chunkHeader, MemVirtualHugeChunkHeader** chunkHeaderList)
{
    chunkHeader->m_next = *chunkHeaderList;
    chunkHeader->m_prev = NULL;
    if ((*chunkHeaderList) != NULL) {
        (*chunkHeaderList)->m_prev = chunkHeader;
    }
    *chunkHeaderList = chunkHeader;
}

static void MemSessioUnrecordHugeChunk(
    MemVirtualHugeChunkHeader* chunkHeader, MemVirtualHugeChunkHeader** chunkHeaderList)
{
    if (chunkHeader->m_prev == NULL) {
        *chunkHeaderList = chunkHeader->m_next;
        if (chunkHeader->m_next != NULL) {
            chunkHeader->m_next->m_prev = NULL;
        }
    } else if (chunkHeader->m_next == NULL) {
        if (chunkHeader->m_prev != NULL) {
            chunkHeader->m_prev->m_next = NULL;
        }
    } else {
        chunkHeader->m_prev->m_next = chunkHeader->m_next;
        chunkHeader->m_next->m_prev = chunkHeader->m_prev;
    }
}

extern void MemHugeGetStats(MemHugeAllocStats* stats)
{
    errno_t erc = memset_s(stats, sizeof(MemHugeAllocStats), 0, sizeof(MemHugeAllocStats));
    securec_check(erc, "\0", "\0");
    for (uint32_t i = 0; i < g_memGlobalCfg.m_nodeCount; ++i) {
        stats->m_memUsedBytes[i] = MOT_ATOMIC_LOAD(memUsedBytes[i]);
        stats->m_memRequestedBytes[i] = MOT_ATOMIC_LOAD(memRequestedBytes[i]);
        stats->m_objectCount[i] = MOT_ATOMIC_LOAD(objectCount[i]);
    }
}

extern void MemHugePrintStats(const char* name, LogLevel logLevel, MemHugeAllocStats* stats)
{
    MOT_LOG(logLevel, "%s Huge Memory Report:", name);
    for (uint32_t i = 0; i < g_memGlobalCfg.m_nodeCount; ++i) {
        if (stats->m_memUsedBytes[i] > 0) {
            MOT_LOG(
                logLevel, "Huge memory usage on node %d: %" PRIu64 " MB Used", i, stats->m_memUsedBytes[i] / MEGA_BYTE);
            MOT_LOG(logLevel,
                "Huge memory usage on node %d: %" PRIu64 " MB Requested",
                i,
                stats->m_memRequestedBytes[i] / MEGA_BYTE);
            MOT_LOG(logLevel, "Huge object count on node %d: %" PRIu64 "", i, stats->m_objectCount[i]);
        }
    }
}

extern void MemHugePrintCurrentStats(const char* name, LogLevel logLevel)
{
    if (MOT_CHECK_LOG_LEVEL(logLevel)) {
        MemHugeAllocStats stats;
        MemHugeGetStats(&stats);
        MemHugePrintStats(name, logLevel, &stats);
    }
}

extern void MemHugePrint(const char* name, LogLevel logLevel, MemReportMode reportMode /* = MEM_REPORT_SUMMARY */)
{
    if (MOT_CHECK_LOG_LEVEL(logLevel)) {
        StringBufferApply([name, logLevel, reportMode](StringBuffer* stringBuffer) {
            MemHugeToString(0, name, stringBuffer, reportMode);
            MOT_LOG(logLevel, "%s", stringBuffer->m_buffer);
        });
    }
}

extern void MemHugeToString(
    int indent, const char* name, StringBuffer* stringBuffer, MemReportMode reportMode /* = MEM_REPORT_SUMMARY */)
{
    MemHugeAllocStats stats;
    MemHugeGetStats(&stats);

    if (reportMode == MEM_REPORT_DETAILED) {
        for (uint32_t i = 0; i < g_memGlobalCfg.m_nodeCount; ++i) {
            if (stats.m_memUsedBytes[i] > 0) {
                StringBufferAppend(stringBuffer,
                    "\n%*sHuge memory usage on node %d: %" PRIu64 " MB Used",
                    indent,
                    "",
                    i,
                    stats.m_memUsedBytes[i] / MEGA_BYTE);
                StringBufferAppend(stringBuffer,
                    "\n%*sHuge memory usage on node %d: %" PRIu64 " MB Requested",
                    indent,
                    "",
                    i,
                    stats.m_memRequestedBytes[i] / MEGA_BYTE);
                StringBufferAppend(stringBuffer,
                    "\n%*sHuge object count on node %d: %" PRIu64 " MB",
                    indent,
                    "",
                    i,
                    stats.m_objectCount[i]);
            }
        }
    } else {
        uint64_t memUsedBytes = 0;
        uint64_t memRequestedBytes = 0;
        uint64_t objectCount = 0;
        for (uint32_t i = 0; i < g_memGlobalCfg.m_nodeCount; ++i) {
            memUsedBytes += stats.m_memUsedBytes[i];
            memRequestedBytes += stats.m_memRequestedBytes[i];
            objectCount += stats.m_objectCount[i];
        }
        StringBufferAppend(stringBuffer,
            "%*sHuge memory: %" PRIu64 " MB Used, %" PRIu64 " MB Requested, %" PRIu64 " objects\n",
            indent,
            "",
            memUsedBytes / MEGA_BYTE,
            memRequestedBytes / MEGA_BYTE,
            objectCount);
    }
}

}  // namespace MOT
