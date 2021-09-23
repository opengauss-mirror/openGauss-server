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
 * mm_raw_chunk_pool.cpp
 *    A raw size chunk pool without ability to grow.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/memory/mm_raw_chunk_pool.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "mm_raw_chunk_pool.h"
#include "mm_numa.h"
#include "mm_def.h"
#include "utilities.h"
#include "cycles.h"
#include "mot_error.h"
#include "thread_id.h"
#include "mm_raw_chunk_dir.h"
#include "mot_atomic_ops.h"
#include "memory_statistics.h"
#include "mm_cfg.h"
#include "mot_engine.h"

#include <stdlib.h>

namespace MOT {

DECLARE_LOGGER(FixedChunkPool, Memory)

// use super chunks
#define USING_SUPER_CHUNK
#define MM_SUPER_CHUNK_SIZE 1
#define MM_DEFAULT_RESERVE_WORKER_NUM 8

// Chunk-Interleaved chunk allocation policy
static uint64_t nextNode = 0;

inline int GteNextNode()
{
    return (MOT_ATOMIC_INC(nextNode) - 1) % g_memGlobalCfg.m_nodeCount;
}

inline void* NumaAllocInterleavedChunk(size_t size, size_t align)
{
    return MemNumaAllocAlignedLocal(size, align, GteNextNode());
}

inline void NumaFreeInterleavedChunk(void* buf, size_t size, int node)
{
    MemNumaFreeLocal(buf, size, node);
}

// Global emergency mode
static uint32_t globalEmergency = 0;

static void IncGlobalEmergency()
{
    if (MOT_ATOMIC_INC(globalEmergency) >= g_memGlobalCfg.m_nodeCount) {
        MOTEngine::GetInstance()->SetSoftMemoryLimitReached();
        MOT_LOG_WARN("Entered global memory emergency state: All global memory pools sank below red mark");
    }
}

static void DecGlobalEmergency()
{
    if (MOT_ATOMIC_DEC(globalEmergency) == (g_memGlobalCfg.m_nodeCount - 1)) {
        MOTEngine::GetInstance()->ResetSoftMemoryLimitReached();
        MOT_LOG_INFO(
            "Exited global memory emergency state (at least one global memory pool is above the low red mark)");
    }
}

// Helpers
static void ReserveChunksAsync(MemRawChunkPool* chunkPool, uint32_t reserveChunkCount, uint32_t reserveWorkerCount);
static int ReserveChunksWait(MemRawChunkPool* chunkPool, uint32_t reserveChunkCount);
static void ReserveChunksAbort(MemRawChunkPool* chunkPool);
static void FreeChunkLFStack(MemRawChunkPool* chunkPool, MemLFStack* chunkStack);
static void MemLFChunkStackToString(int indent, const char* name, MemLFStack* chunkList, StringBuffer* stringBuffer);
static void* ReserveWorker(void* param);
static MemRawChunkHeader* AllocateSuperChunk(MemRawChunkPool* chunkPool, uint32_t chunkCount, uint32_t* allocated);
static inline uint32_t PushSuperChunk(MemRawChunkPool* chunkPool, MemRawChunkHeader* head, uint32_t count);
static void FreeChunk(MemRawChunkPool* chunkPool, MemRawChunkHeader* chunkHeader);
static void FreeChunkStack(MemRawChunkPool* chunkPool, MemRawChunkHeader* chunkStack);

extern int MemRawChunkPoolInit(MemRawChunkPool* chunkPool, const char* poolName, int node, MemAllocType allocType,
    uint32_t reserveChunkCount, uint32_t reserveWorkerCount, uint32_t highRedMark, uint32_t highBlackMark,
    MemReserveMode reserveMode /* = MEM_RESERVE_PHYSICAL */, MemStorePolicy storePolicy /* = MEM_STORE_COMPACT */,
    MemRawChunkPool* subChunkPool /* = NULL */, int asyncReserve /* = 0 */)
{
    int result = 0;

    MOT_LOG_TRACE("Initializing chunk pool %s on node %d", poolName, node);
    errno_t erc =
        snprintf_s(chunkPool->m_poolName, MM_CHUNK_POOL_NAME_SIZE, MM_CHUNK_POOL_NAME_SIZE - 1, "%s", poolName);
    securec_check_ss(erc, "\0", "\0");
    chunkPool->m_node = node;
    chunkPool->m_allocType = allocType;
    chunkPool->m_reserveChunkCount = reserveChunkCount;
    chunkPool->m_highRedMark = highRedMark;
    chunkPool->m_highBlackMark = highBlackMark;
    chunkPool->m_emergencyMode = 0;
    chunkPool->m_totalChunkCount = 0;
    chunkPool->m_subChunkPool = subChunkPool;
    chunkPool->m_reserveMode = reserveMode;
    chunkPool->m_storePolicy = storePolicy;
    chunkPool->m_asyncReserveData = NULL;

    if (highRedMark > highBlackMark) {
        MOT_LOG_WARN("Invalid configuration for %s chunk pool: high red mark %u is greater than high black mark %u",
            chunkPool->m_poolName,
            highRedMark,
            highBlackMark);
    }

    // allocate the nodes for the lock-free stack
    if (node == MEM_INTERLEAVE_NODE) {
        chunkPool->m_nodeBuffer = (MemLFStackNode*)MemNumaAllocGlobal(sizeof(MemLFStackNode) * highBlackMark);
    } else {
        chunkPool->m_nodeBuffer = (MemLFStackNode*)MemNumaAllocLocal(sizeof(MemLFStackNode) * highBlackMark, node);
    }
    if (chunkPool->m_nodeBuffer == NULL) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Chunk Pool Initialization",
            "Failed to allocate %u nodes for lock-free stack",
            highBlackMark);
        return MOT_ERROR_OOM;
    }
    MemLFStackInit(&chunkPool->m_chunkAllocStack, chunkPool->m_nodeBuffer, highBlackMark);

    // pre-allocate all required memory (long blocking call)
    ReserveChunksAsync(chunkPool, reserveChunkCount, reserveWorkerCount);
    if (chunkPool->m_asyncReserveData == NULL) {
        result = GetLastError();
    } else if (!asyncReserve) {
        result = MemRawChunkPoolWaitReserve(chunkPool);
        if (result == 0) {
            MOT_LOG_TRACE("Chunk pool on node %d initialized successfully", node);
        } else {
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                "Chunk Pool Initialization",
                "Failed to initialize chunk pool %s on node %d (see errors above)",
                poolName,
                node);
        }
    }

    if (result != 0) {
        // safe cleanup
        MemRawChunkPoolDestroy(chunkPool);
    }

    return result;
}

extern int MemRawChunkPoolWaitReserve(MemRawChunkPool* chunkPool)
{
    return ReserveChunksWait(chunkPool, chunkPool->m_reserveChunkCount);
}

extern void MemRawChunkPoolAbortReserve(MemRawChunkPool* chunkPool)
{
    // repeated calls are safe
    MOT_LOG_WARN("Aborting asynchronous chunk reservation on chunk pool %s", chunkPool->m_poolName);
    ReserveChunksAbort(chunkPool);
}

extern void MemRawChunkPoolDestroy(MemRawChunkPool* chunkPool)
{
    MOT_LOG_TRACE("Destroying chunk pool %s on node %d", chunkPool->m_poolName, chunkPool->m_node);

    // free all chunks
    if (chunkPool->m_nodeBuffer != NULL) {
        MOT_LOG_TRACE(
            "Deallocating all remaining chunks for chunk pool %s on node %d", chunkPool->m_poolName, chunkPool->m_node);
        FreeChunkLFStack(chunkPool, &chunkPool->m_chunkAllocStack);

        // release resources
        MemLFStackDestroy(&chunkPool->m_chunkAllocStack);
        if (chunkPool->m_node == MEM_INTERLEAVE_NODE) {
            MemNumaFreeGlobal(chunkPool->m_nodeBuffer, sizeof(MemLFStackNode) * chunkPool->m_highBlackMark);
        } else {
            MemNumaFreeLocal(
                chunkPool->m_nodeBuffer, sizeof(MemLFStackNode) * chunkPool->m_highBlackMark, chunkPool->m_node);
        }

        chunkPool->m_nodeBuffer = NULL;
    }

    MOT_LOG_TRACE("Chunk pool %s on node %d destroyed", chunkPool->m_poolName, chunkPool->m_node);
}

static uint32_t MemRawChunkPoolSecureChunks(MemRawChunkPool* chunkPool, uint32_t chunksToSecure)
{
    uint32_t chunksCanSecure = 0;
    while (1) {
        chunksCanSecure = 0;  // reset before every round
        uint32_t totalChunkCount = MOT_ATOMIC_LOAD(chunkPool->m_totalChunkCount);
        if (totalChunkCount >= chunkPool->m_highBlackMark) {
            // limit reached, so stop trying
            break;
        } else {
            // can still grow, so secure as many as possible up to chunksToSecure
            chunksCanSecure = min(chunksToSecure, chunkPool->m_highBlackMark - totalChunkCount);
            if (MOT_ATOMIC_CAS(chunkPool->m_totalChunkCount, totalChunkCount, totalChunkCount + chunksCanSecure)) {
                // secure succeeded
                MOT_LOG_DEBUG("Secured %u chunks for allocation", chunksCanSecure);
                break;
            }
            // retry again
        }
    }
    return chunksCanSecure;
}

extern MemRawChunkHeader* MemRawChunkPoolAlloc(MemRawChunkPool* chunkPool)
{
    // allocate one header
    MemRawChunkHeader* chunkHeader = (MemRawChunkHeader*)MemLFStackPop(&chunkPool->m_chunkAllocStack);
    if (chunkHeader == NULL) {
        // first secure chunks by increasing atomically m_totalChunkCount, then allocate without race
        uint32_t chunksSecured = MemRawChunkPoolSecureChunks(chunkPool, MM_SUPER_CHUNK_SIZE);
        if (chunksSecured == 0) {
            MOT_LOG_TRACE("Failed to secure %u chunks: reached high black mark", (unsigned)MM_SUPER_CHUNK_SIZE);
        } else {
            uint32_t chunksAllocated = 0;
            chunkHeader = AllocateSuperChunk(chunkPool, chunksSecured, &chunksAllocated);
            if (chunkHeader) {
                // the actual amount allocated can be less than secured, so update the total count
                if (chunksAllocated != chunksSecured) {
                    MOT_ASSERT(chunksAllocated < chunksSecured);
                    MOT_ATOMIC_SUB(chunkPool->m_totalChunkCount, chunksSecured - chunksAllocated);
                    MOT_LOG_DEBUG(
                        "Secured %u chunks, but were able to allocate only %u, total count updated accordingly",
                        chunksSecured,
                        chunksAllocated);
                }
                // now push if required
                if (chunksAllocated > 1) {
                    MemRawChunkHeader* newHead = chunkHeader->m_nextChunk;
                    uint32_t chunksPushed = PushSuperChunk(chunkPool,
                        newHead,
                        chunksAllocated -
                            1);  // don't care if some chunks cannot be inserted because of high black mark
                    MOT_LOG_DEBUG("Pushed %u chunks from %u super-chunk while encountering empty chunk pool",
                        chunksPushed,
                        chunksAllocated - 1);
                    // the actual amount pushed can theoretically be less than allocated, so update the total count
                    if (chunksPushed != chunksAllocated - 1) {
                        MOT_ASSERT(chunksPushed < chunksAllocated - 1);
                        MOT_ATOMIC_SUB(chunkPool->m_totalChunkCount, chunksAllocated - chunksPushed - 1);
                        MOT_LOG_DEBUG(
                            "Allocated %u chunks, but were able to push only %u, total count updated accordingly",
                            chunksAllocated,
                            chunksPushed);
                    }
                }
            }
        }
    }

    // issue error if failed
    if (!chunkHeader) {
        // we set this error message level to trace, because it floods the log
        MOT_LOG_TRACE("Failed to allocate a chunk in %s chunk pool: Reached high black-mark limit %u",
            chunkPool->m_poolName,
            chunkPool->m_highBlackMark);
    } else if (chunkPool->m_subChunkPool == NULL) {
        // report statistics only for un-cascaded chunk pools
        if (chunkPool->m_allocType == MEM_ALLOC_GLOBAL) {
            MemoryStatisticsProvider::m_provider->AddGlobalChunksUsed(MEM_CHUNK_SIZE_MB * MEGA_BYTE);
            DetailedMemoryStatisticsProvider::m_provider->AddGlobalChunksUsed(
                chunkPool->m_node, MEM_CHUNK_SIZE_MB * MEGA_BYTE);
        } else {
            MemoryStatisticsProvider::m_provider->AddLocalChunksUsed(MEM_CHUNK_SIZE_MB * MEGA_BYTE);
            DetailedMemoryStatisticsProvider::m_provider->AddLocalChunksUsed(
                chunkPool->m_node, MEM_CHUNK_SIZE_MB * MEGA_BYTE);
        }
    }

    // update emergency mode (if no more chunks can be allocated and reserved chunk count is below red mark)
    uint32_t totalChunks = MOT_ATOMIC_LOAD(chunkPool->m_totalChunkCount);
    uint32_t freeChunks = (uint32_t)MemLFStackSize(&chunkPool->m_chunkAllocStack);
    uint32_t usedChunks = totalChunks - freeChunks;
    if (usedChunks >= chunkPool->m_highRedMark) {
        if (MOT_ATOMIC_CAS(chunkPool->m_emergencyMode, 0, 1)) {
            MOT_LOG_WARN("%s chunk pool exceeded high red mark %u, and is now defined to be in emergency state",
                chunkPool->m_poolName,
                chunkPool->m_highRedMark);
            if (chunkPool->m_allocType == MEM_ALLOC_GLOBAL) {
                IncGlobalEmergency();
            }
        }
    }

    return chunkHeader;
}

extern void MemRawChunkPoolFree(MemRawChunkPool* chunkPool, void* chunk)
{
    MemRawChunkHeader* chunkHeader = (MemRawChunkHeader*)chunk;
    chunkHeader->m_chunkType = MEM_CHUNK_TYPE_RAW;

    bool shouldPush = true;
    if (chunkPool->m_storePolicy == MEM_STORE_COMPACT) {
        uint32_t totalChunks = MOT_ATOMIC_LOAD(chunkPool->m_totalChunkCount);
        if (totalChunks > chunkPool->m_reserveChunkCount) {
            shouldPush = false;
        }
    }

    // atomically push
    if (shouldPush) {
        MOT_LOG_DEBUG("Deallocating chunk [%p] to chunk pool %s on node %d back to reservation stack",
            chunkHeader,
            chunkPool->m_poolName,
            chunkPool->m_node);
        int ret = MemLFStackPush(&chunkPool->m_chunkAllocStack, chunkHeader);
        MOT_ASSERT(ret == 0);
    } else {
        MOT_LOG_DEBUG("Deallocating chunk [%p] from chunk pool %s on node %d back to kernel",
            chunkHeader,
            chunkPool->m_poolName,
            chunkPool->m_node);
        FreeChunk(chunkPool, chunkHeader);
        MOT_ATOMIC_DEC(chunkPool->m_totalChunkCount);
    }

    // update emergency mode
    if (MOT_ATOMIC_LOAD(chunkPool->m_emergencyMode)) {
        uint32_t totalChunks = MOT_ATOMIC_LOAD(chunkPool->m_totalChunkCount);
        uint32_t freeChunks = (uint32_t)MemLFStackSize(&chunkPool->m_chunkAllocStack);
        uint32_t usedChunks = totalChunks - freeChunks;
        if (usedChunks < chunkPool->m_highRedMark) {
            if (MOT_ATOMIC_CAS(chunkPool->m_emergencyMode, 1, 0)) {
                MOT_LOG_INFO("%s chunk pool exited emergency mode", chunkPool->m_poolName);
                if (chunkPool->m_allocType == MEM_ALLOC_GLOBAL) {
                    DecGlobalEmergency();
                }
            }
        }
    }

    // report statistics
    if (chunkPool->m_subChunkPool == NULL) {
        // report statistics only for un-cascaded chunk pools
        if (chunkPool->m_allocType == MEM_ALLOC_GLOBAL) {
            MemoryStatisticsProvider::m_provider->AddGlobalChunksUsed(-((int64_t)MEM_CHUNK_SIZE_MB * MEGA_BYTE));
            DetailedMemoryStatisticsProvider::m_provider->AddGlobalChunksUsed(
                chunkPool->m_node, -((int64_t)MEM_CHUNK_SIZE_MB * MEGA_BYTE));
        } else {
            MemoryStatisticsProvider::m_provider->AddLocalChunksUsed(-((int64_t)MEM_CHUNK_SIZE_MB * MEGA_BYTE));
            DetailedMemoryStatisticsProvider::m_provider->AddLocalChunksUsed(
                chunkPool->m_node, -((int64_t)MEM_CHUNK_SIZE_MB * MEGA_BYTE));
        }
    }
}

extern void MemRawChunkPoolPrint(const char* name, LogLevel logLevel, MemRawChunkPool* chunkPool,
    MemReportMode reportMode /* = MEM_REPORT_SUMMARY */)
{
    if (MOT_CHECK_LOG_LEVEL(logLevel)) {
        StringBufferApply([name, logLevel, chunkPool, reportMode](StringBuffer* stringBuffer) {
            MemRawChunkPoolToString(0, name, chunkPool, stringBuffer, reportMode);
            MOT_LOG(logLevel, "%s", stringBuffer->m_buffer);
        });
    }
}

extern void MemRawChunkPoolToString(int indent, const char* name, MemRawChunkPool* chunkPool,
    StringBuffer* stringBuffer, MemReportMode reportMode /* = MEM_REPORT_SUMMARY */)
{
    uint32_t initialMb = chunkPool->m_reserveChunkCount * MEM_CHUNK_SIZE_MB;
    uint32_t reservedMb = MOT_ATOMIC_LOAD(chunkPool->m_totalChunkCount) * MEM_CHUNK_SIZE_MB;
    uint32_t freeMb = MemLFStackSize(&chunkPool->m_chunkAllocStack) * MEM_CHUNK_SIZE_MB;
    uint32_t usedMb = reservedMb - freeMb;
    if (reportMode == MEM_REPORT_SUMMARY) {
        StringBufferAppend(stringBuffer,
            "%*sChunk Pool %s [node=%d, initial=%u MB, reserved=%u MB, used=%u MB]",
            indent,
            "",
            chunkPool->m_poolName,
            chunkPool->m_node,
            initialMb,
            reservedMb,
            usedMb);
    } else {
        StringBufferAppend(stringBuffer,
            "%*s%s Chunk Pool %s [node=%d, initial=%u MB, reserved=%u MB, used=%u MB] = {\n",
            indent,
            "",
            name,
            chunkPool->m_poolName,
            chunkPool->m_node,
            initialMb,
            reservedMb,
            usedMb);

        MemLFChunkStackToString(indent + PRINT_REPORT_INDENT, "Free", &chunkPool->m_chunkAllocStack, stringBuffer);
        StringBufferAppend(stringBuffer, "\n");

        if (chunkPool->m_subChunkPool) {
            MemRawChunkPoolToString(indent + PRINT_REPORT_INDENT, "Sub", chunkPool->m_subChunkPool, stringBuffer);
            StringBufferAppend(stringBuffer, "\n");
        }
        StringBufferAppend(stringBuffer, "%*s}", indent, "");
    }
}

extern void MemRawChunkPoolGetStats(MemRawChunkPool* chunkPool, MemRawChunkPoolStats* stats)
{
    uint64_t reservedMb = MOT_ATOMIC_LOAD(chunkPool->m_totalChunkCount) * MEM_CHUNK_SIZE_MB;
    uint64_t freeMb = MemLFStackSize(&chunkPool->m_chunkAllocStack) * MEM_CHUNK_SIZE_MB;
    uint64_t usedMb = reservedMb - freeMb;
    stats->m_node = chunkPool->m_node;
    stats->m_allocType = chunkPool->m_allocType;
    stats->m_reservedBytes = reservedMb * MEGA_BYTE;
    stats->m_usedBytes = usedMb * MEGA_BYTE;
}

struct ReserveWorkerData {
    uint32_t m_workerId;
    MemRawChunkPool* m_chunkPool;
    uint32_t m_chunkCount;
};

struct AsyncReserveData {
    int m_globalAbort;
    uint32_t m_reserveWorkerCount;
    uint64_t m_startTime;
    ReserveWorkerData* m_reserveParam;
    pthread_t* m_reserveThreads;
};

inline void SetGlobalAbortFlag(AsyncReserveData* asyncReserveData)
{
    MOT_ATOMIC_STORE(asyncReserveData->m_globalAbort, 1);
}

inline int GetGlobalAbortFlag(AsyncReserveData* asyncReserveData)
{
    return MOT_ATOMIC_LOAD(asyncReserveData->m_globalAbort);
}

static void StopAsyncChunkReserve(AsyncReserveData* asyncReserveData, uint32_t taskCount)
{
    MOT_LOG_WARN("Ordering all pre-allocation threads to abort due to initialization error");
    SetGlobalAbortFlag(asyncReserveData);

    MemRawChunkPool* chunkPool = asyncReserveData->m_reserveParam[0].m_chunkPool;
    MOT_LOG_TRACE(
        "Waiting for %u chunk reservation workers to abort after initialization error (chunk pool %s on node %d)",
        taskCount,
        chunkPool->m_poolName,
        chunkPool->m_node);
    for (uint32_t i = 0; i < taskCount; ++i) {
        pthread_join(asyncReserveData->m_reserveThreads[i], NULL);
    }
    MOT_LOG_WARN("All pre-allocation threads aborted");
}

static void ReserveChunksAsync(MemRawChunkPool* chunkPool, uint32_t reserveChunkCount, uint32_t reserveWorkerCount)
{
    if (reserveWorkerCount == 0) {
        reserveWorkerCount = MM_DEFAULT_RESERVE_WORKER_NUM;
    }

    // distribute chunks between reserve workers
    AsyncReserveData* asyncReserveData = (AsyncReserveData*)malloc(sizeof(AsyncReserveData));
    if (asyncReserveData == NULL) {
        MOT_REPORT_PANIC(MOT_ERROR_OOM,
            "Fixed Chunk Pool Initialization",
            "Failed to allocate %u bytes for asynchronous chunk reservation, aborting",
            (unsigned)sizeof(AsyncReserveData));
    } else {
        asyncReserveData->m_reserveWorkerCount = reserveWorkerCount;
        asyncReserveData->m_startTime = GetSysClock();
        asyncReserveData->m_globalAbort = 0;

        uint32_t reserveChunkPerWorker = reserveChunkCount / reserveWorkerCount;
        uint32_t extraChunks = reserveChunkCount % reserveWorkerCount;
        MOT_LOG_TRACE("Reserving %u chunks with %u workers for chunk pool %s on node %d",
            reserveChunkCount,
            reserveWorkerCount,
            chunkPool->m_poolName,
            chunkPool->m_node);

        // scatter work
        uint32_t allocSize = sizeof(ReserveWorkerData) * reserveWorkerCount;
        asyncReserveData->m_reserveParam = (ReserveWorkerData*)malloc(allocSize);
        if (asyncReserveData->m_reserveParam == NULL) {
            MOT_REPORT_PANIC(MOT_ERROR_OOM,
                "Fixed Chunk Pool Initialization",
                "Failed to allocate %u bytes for asynchronous chunk reservation, aborting",
                allocSize);
            free(asyncReserveData);
            asyncReserveData = NULL;
        } else {
            allocSize = sizeof(pthread_t) * reserveWorkerCount;
            asyncReserveData->m_reserveThreads = (pthread_t*)malloc(allocSize);
            if (asyncReserveData->m_reserveThreads == NULL) {
                MOT_REPORT_PANIC(MOT_ERROR_OOM,
                    "Fixed Chunk Pool Initialization",
                    "Failed to allocate %u bytes for asynchronous chunk reservation, aborting",
                    allocSize);
                free(asyncReserveData->m_reserveParam);
                free(asyncReserveData);
                asyncReserveData = NULL;
            } else {
                // before we launch threads we must set up the m_asyncReserveData of the chunk pool,
                // otherwise checks for global abort flag will cause a core dump
                chunkPool->m_asyncReserveData = asyncReserveData;
                for (uint32_t i = 0; i < reserveWorkerCount; ++i) {
                    asyncReserveData->m_reserveParam[i].m_workerId = i;
                    asyncReserveData->m_reserveParam[i].m_chunkPool = chunkPool;
                    asyncReserveData->m_reserveParam[i].m_chunkCount = reserveChunkPerWorker;
                    if (extraChunks > 0) {
                        ++asyncReserveData->m_reserveParam[i].m_chunkCount;
                        --extraChunks;
                    }
                    int rc = pthread_create(&asyncReserveData->m_reserveThreads[i],
                        NULL,
                        ReserveWorker,
                        &asyncReserveData->m_reserveParam[i]);
                    if (rc != 0) {
                        MOT_REPORT_SYSTEM_PANIC_CODE(rc,
                            pthread_create,
                            "Fixed Chunk Pool Initialization",
                            "Failed to launch asynchronous chunk reservation thread %u",
                            i);
                        StopAsyncChunkReserve(asyncReserveData, i);
                        free(asyncReserveData->m_reserveThreads);
                        free(asyncReserveData->m_reserveParam);
                        free(asyncReserveData);
                        asyncReserveData = NULL;
                        chunkPool->m_asyncReserveData = NULL;
                        break;
                    } else {
                        MOT_LOG_TRACE(
                            "Launched chunk reservation worker %u with %u chunks for chunk pool %s on node %d",
                            i,
                            asyncReserveData->m_reserveParam[i].m_chunkCount,
                            chunkPool->m_poolName,
                            chunkPool->m_node);
                    }
                }
            }
        }
    }
}

static int ReserveChunksWait(MemRawChunkPool* chunkPool, uint32_t reserveChunkCount)
{
    int result = 0;

    AsyncReserveData* asyncReserveData = (AsyncReserveData*)chunkPool->m_asyncReserveData;

    // gather work
    MOT_LOG_TRACE("Waiting for all chunk reservation workers to finish on chunk pool %s on node %d",
        chunkPool->m_poolName,
        chunkPool->m_node);
    for (uint32_t i = 0; i < asyncReserveData->m_reserveWorkerCount; ++i) {
        void* retval = NULL;
        int rc = pthread_join(asyncReserveData->m_reserveThreads[i], &retval);
        if (rc != 0) {
            MOT_LOG_SYSTEM_ERROR_CODE(rc, pthread_join, "Failed to wait for pre-allocation worker %u", i);
            if (result == 0) {
                // push on error stack only first failure, but report to log all of them
                MOT_REPORT_SYSTEM_PANIC_CODE(rc,
                    pthread_join,
                    "Fixed Chunk Pool Initialization",
                    "Failed to wait for pre-allocation worker %u",
                    i);
                result = MOT_ERROR_SYSTEM_FAILURE;
                // continue as cleanup, but signal all threads to finish quickly
                SetGlobalAbortFlag(asyncReserveData);
            }
        } else if (retval != NULL) {
            int res = (int)(uint64_t)retval;
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                "Chunk Pool Initialization",
                "Chunk reservation worker %u for chunk pool %s on node %d failed with error: %s (error code %d)",
                i,
                chunkPool->m_poolName,
                chunkPool->m_node,
                ErrorCodeToString(res),
                res);
            if (result == 0) {
                // push on error stack only first failure, but report to log all of them
                MOT_REPORT_PANIC(res,
                    "Fixed Chunk Pool Initialization",
                    "Chunk reservation worker %u for chunk pool %s on node %d failed with error: %s (error code %d)",
                    i,
                    chunkPool->m_poolName,
                    chunkPool->m_node,
                    ErrorCodeToString(res),
                    res);
                result = res;
                // continue as cleanup, but signal all threads to finish quickly
                SetGlobalAbortFlag(asyncReserveData);
            }
        }
    }

    if (result == 0) {
        uint64_t endTime = GetSysClock();
        double workTimeSeconds = CpuCyclesLevelTime::CyclesToSeconds(endTime - asyncReserveData->m_startTime);
        double memGb = reserveChunkCount * MEM_CHUNK_SIZE_MB / 1024.0f;
        MOT_LOG_TRACE("Finished reserving %u chunks (%0.2f GB) for chunk pool %s on node %d within %0.4f seconds",
            reserveChunkCount,
            memGb,
            chunkPool->m_poolName,
            chunkPool->m_node,
            workTimeSeconds);
    } else {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Chunk Pool Initialization",
            "Reservation of %u chunks for chunk pool %s on node %d failed with error: %s (error code %d)",
            reserveChunkCount,
            chunkPool->m_poolName,
            chunkPool->m_node,
            ErrorCodeToString(result),
            result);
    }

    // cleanup
    free(asyncReserveData->m_reserveThreads);
    free(asyncReserveData->m_reserveParam);
    free(asyncReserveData);
    chunkPool->m_asyncReserveData = NULL;

    return result;
}

static void ReserveChunksAbort(MemRawChunkPool* chunkPool)
{
    AsyncReserveData* asyncReserveData = (AsyncReserveData*)chunkPool->m_asyncReserveData;
    if (asyncReserveData != NULL) {
        StopAsyncChunkReserve(asyncReserveData, asyncReserveData->m_reserveWorkerCount);
        free(asyncReserveData->m_reserveThreads);
        free(asyncReserveData->m_reserveParam);
        free(asyncReserveData);
        chunkPool->m_asyncReserveData = NULL;
    }
}

static void FreeChunk(MemRawChunkPool* chunkPool, MemRawChunkHeader* chunkHeader)
{
    if (chunkPool->m_subChunkPool != NULL) {
        MOT_LOG_DEBUG("Deallocating chunk [%p] from chunk pool %s on node %d into sub-pool %s",
            chunkHeader,
            chunkPool->m_poolName,
            chunkPool->m_node,
            chunkPool->m_subChunkPool->m_poolName);
        MemRawChunkPoolFree(chunkPool->m_subChunkPool, chunkHeader);
    } else {
        MOT_LOG_DEBUG("Deallocating chunk [%p] from chunk pool %s on node %d into kernel",
            chunkHeader,
            chunkPool->m_poolName,
            chunkPool->m_node);
        if (chunkHeader->m_chunkType == MEM_CHUNK_TYPE_DEAD) {
            MOT_REPORT_ERROR(MOT_ERROR_INVALID_ARG,
                "Chunk De-Allocation",
                "Attempted request for double-free of chunk %p denied",
                chunkHeader);
        } else {
            MemRawChunkDirRemove(chunkHeader);
            chunkHeader->m_chunkType = MEM_CHUNK_TYPE_DEAD;  // mark the chunk as dead, this might prevent crash
            if (chunkPool->m_allocType == MEM_ALLOC_GLOBAL) {
                if (g_memGlobalCfg.m_chunkAllocPolicy == MEM_ALLOC_POLICY_LOCAL) {
                    MemNumaFreeLocal(chunkHeader, MEM_CHUNK_SIZE_MB * MEGA_BYTE, chunkPool->m_node);
                } else if (g_memGlobalCfg.m_chunkAllocPolicy == MEM_ALLOC_POLICY_CHUNK_INTERLEAVED) {
                    NumaFreeInterleavedChunk(chunkHeader, MEM_CHUNK_SIZE_MB * MEGA_BYTE, chunkPool->m_node);
                } else if (g_memGlobalCfg.m_chunkAllocPolicy == MEM_ALLOC_POLICY_PAGE_INTERLEAVED) {
                    MemNumaFreeGlobal(chunkHeader, MEM_CHUNK_SIZE_MB * MEGA_BYTE);
                } else if (g_memGlobalCfg.m_chunkAllocPolicy == MEM_ALLOC_POLICY_NATIVE) {
                    free(chunkHeader);
                } else {
                    MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                        "Chunk De-Allocation",
                        "Invalid chunk allocation policy: %u",
                        (unsigned)g_memGlobalCfg.m_chunkAllocPolicy);
                    return;
                }
                MemoryStatisticsProvider::m_provider->AddGlobalChunksReserved(
                    -((int64_t)MEM_CHUNK_SIZE_MB * MEGA_BYTE));
                DetailedMemoryStatisticsProvider::m_provider->AddGlobalChunksReserved(
                    chunkPool->m_node, -((int64_t)MEM_CHUNK_SIZE_MB * MEGA_BYTE));
            } else {
                MemNumaFreeLocal(chunkHeader, MEM_CHUNK_SIZE_MB * MEGA_BYTE, chunkPool->m_node);
                MemoryStatisticsProvider::m_provider->AddLocalChunksReserved(-((int64_t)MEM_CHUNK_SIZE_MB * MEGA_BYTE));
                DetailedMemoryStatisticsProvider::m_provider->AddLocalChunksReserved(
                    chunkPool->m_node, -((int64_t)MEM_CHUNK_SIZE_MB * MEGA_BYTE));
            }
        }
    }
}

static void FreeChunkLFStack(MemRawChunkPool* chunkPool, MemLFStack* chunkStack)
{
    MemRawChunkHeader* itr = (MemRawChunkHeader*)MemLFStackPop(chunkStack);
    while (itr) {
        FreeChunk(chunkPool, itr);
        itr = (MemRawChunkHeader*)MemLFStackPop(chunkStack);
    }
}

static void FreeChunkStack(MemRawChunkPool* chunkPool, MemRawChunkHeader* chunkStack)
{
    if (chunkStack) {
        MemRawChunkHeader* itr = chunkStack;
        MemRawChunkHeader* next = NULL;
        while (itr) {
            next = itr->m_nextChunk;
            FreeChunk(chunkPool, itr);
            itr = next;
        }
    }
}

static void MakeChunkResident(MemRawChunkHeader* chunk)
{
    // just read every page in the chunk and force page-faults
    uint8_t* rawChunk = (uint8_t*)chunk;
    uint32_t pagesInChunk = MEM_CHUNK_SIZE_MB * KILO_BYTE / PAGE_SIZE_KB;
    for (uint32_t i = 0; i < pagesInChunk; ++i) {
        *((uint32_t*)(rawChunk + i * PAGE_SIZE_BYTES)) = 0;
    }
}

static MemRawChunkHeader* AllocateChunkFromKernel(MemRawChunkPool* chunkPool, size_t allocSize, size_t align)
{
    MemRawChunkHeader* chunk = nullptr;
    if (chunkPool->m_allocType == MEM_ALLOC_GLOBAL) {
        if (g_memGlobalCfg.m_chunkAllocPolicy == MEM_ALLOC_POLICY_LOCAL) {
            // allocate from specific local node
            chunk = (MemRawChunkHeader*)MemNumaAllocAlignedLocal(allocSize, align, chunkPool->m_node);
        } else if (g_memGlobalCfg.m_chunkAllocPolicy == MEM_ALLOC_POLICY_CHUNK_INTERLEAVED) {
            // allocate chunk from next node (round robin)
            chunk = (MemRawChunkHeader*)NumaAllocInterleavedChunk(allocSize, align);
        } else if (g_memGlobalCfg.m_chunkAllocPolicy == MEM_ALLOC_POLICY_PAGE_INTERLEAVED) {
            // allocate chunk from all nodes (interleaved on page boundary)
            chunk = (MemRawChunkHeader*)MemNumaAllocAlignedGlobal(allocSize, align);
        } else if (g_memGlobalCfg.m_chunkAllocPolicy == MEM_ALLOC_POLICY_NATIVE) {
            // allocate from kernel using malloc()
            int res = posix_memalign((void**)&chunk, align, allocSize);
            if (res != 0) {
                MOT_REPORT_SYSTEM_ERROR_CODE(
                    res, posix_memalign, "Chunk Allocation", "Failed to allocate aligned 2MB chunk");
                chunk = nullptr;
            }
        } else {
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                "Chunk Allocation",
                "Invalid chunk allocation policy: %u",
                (unsigned)g_memGlobalCfg.m_chunkAllocPolicy);
            return nullptr;
        }
        if (chunk) {
            MemoryStatisticsProvider::m_provider->AddGlobalChunksReserved(allocSize);
        }
    } else {
        if (g_memGlobalCfg.m_chunkAllocPolicy == MEM_ALLOC_POLICY_NATIVE) {
            int res = posix_memalign((void**)&chunk, align, allocSize);
            if (res != 0) {
                MOT_REPORT_SYSTEM_ERROR_CODE(
                    res, posix_memalign, "Chunk Allocation", "Failed to allocate aligned 2MB chunk");
                chunk = nullptr;
            }
        } else {
            chunk = (MemRawChunkHeader*)MemNumaAllocAlignedLocal(allocSize, align, chunkPool->m_node);
        }
        if (chunk) {
            MemoryStatisticsProvider::m_provider->AddLocalChunksReserved(allocSize);
            DetailedMemoryStatisticsProvider::m_provider->AddLocalChunksReserved(chunkPool->m_node, allocSize);
        }
    }
    return chunk;
}

static MemRawChunkHeader* AllocateChunk(MemRawChunkPool* chunkPool)
{
    if (chunkPool->m_asyncReserveData) {
        AsyncReserveData* asyncReserveData = (AsyncReserveData*)chunkPool->m_asyncReserveData;
        if (GetGlobalAbortFlag(asyncReserveData)) {
            // error during initial pre-allocation
            MOT_LOG_WARN("Noticed global stop flag, aborting pre-allocation");
            SetLastError(MOT_ERROR_INTERNAL, MOT_SEVERITY_FATAL);
            return NULL;
        }
    }

    MemRawChunkHeader* chunk = NULL;
    if (chunkPool->m_subChunkPool != NULL) {
        MOT_LOG_DEBUG("Allocating chunk for chunk pool %s on node %d from sub-pool %s",
            chunkPool->m_poolName,
            chunkPool->m_node,
            chunkPool->m_subChunkPool->m_poolName);
        chunk = (MemRawChunkHeader*)MemRawChunkPoolAlloc(chunkPool->m_subChunkPool);
    } else {
        MOT_LOG_DEBUG(
            "Allocating chunk for chunk pool %s on node %d from kernel", chunkPool->m_poolName, chunkPool->m_node);
        size_t allocSize = MEM_CHUNK_SIZE_MB * MEGA_BYTE;
        size_t align = allocSize;
        chunk = AllocateChunkFromKernel(chunkPool, allocSize, align);
        if (chunk) {
            if (chunkPool->m_reserveMode == MEM_RESERVE_PHYSICAL) {
                MakeChunkResident(chunk);
            }
            chunk->m_chunkType = MEM_CHUNK_TYPE_RAW;
            chunk->m_node = chunkPool->m_node;
            chunk->m_allocType = chunkPool->m_allocType;
            MemRawChunkDirInsert(chunk);
        } else {
            MOT_REPORT_ERROR(MOT_ERROR_OOM,
                "Chunk Allocation",
                "Failed to allocate one chunk of size %u bytes on node %u",
                (unsigned)allocSize,
                chunkPool->m_node);
        }
    }
    return chunk;
}

static int AllocateAndPushChunk(MemRawChunkPool* chunkPool)
{
    int result = 0;

    // check for stop flag
    AsyncReserveData* asyncReserveData = (AsyncReserveData*)chunkPool->m_asyncReserveData;
    if (asyncReserveData && GetGlobalAbortFlag(asyncReserveData)) {
        MOT_REPORT_ERROR(
            MOT_ERROR_INTERNAL, "Chunk Pool Initialization", "Aborting pre-allocation due to initialization error");
        return MOT_ERROR_INTERNAL;
    }

    // first secure the required amount of chunks (we expect and require full success, since this is pre-allocation
    // phase)
    uint32_t chunksSecured = MemRawChunkPoolSecureChunks(chunkPool, 1);
    if (chunksSecured != 1) {
        MOT_LOG_TRACE("Failed to secure a chunk: reached high black mark");
        result = MOT_ERROR_RESOURCE_LIMIT;
    } else {
        MemRawChunkHeader* chunkHeader = AllocateChunk(chunkPool);
        if (chunkHeader != NULL) {
            int ret = MemLFStackPush(&chunkPool->m_chunkAllocStack, chunkHeader);
            if (ret != 0) {
                MOT_LOG_TRACE(
                    "Reached high black-mark %u in %s chunk pool", chunkPool->m_highBlackMark, chunkPool->m_poolName);
                MOT_ATOMIC_DEC(chunkPool->m_totalChunkCount);  // keep counter accurate even if failed
                FreeChunk(chunkPool, chunkHeader);
                result = MOT_ERROR_RESOURCE_LIMIT;
            } else {
                MOT_LOG_DIAG1("Pushed chunk while pre-fetching");
            }
        } else {
            // no more memory, stop allocating silently
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                "Chunk Pool Initialization",
                "Failed to allocate a chunk for chunk pool %s on node %d",
                chunkPool->m_poolName,
                chunkPool->m_node);
            MOT_ATOMIC_DEC(chunkPool->m_totalChunkCount);  // keep counter accurate even if failed
            result = GetLastError();
        }
    }

    return result;
}

static int AllocateChunks(MemRawChunkPool* chunkPool, uint32_t chunkCount)
{
    int result = 0;

    for (uint32_t i = 0; i < chunkCount; ++i) {
        result = AllocateAndPushChunk(chunkPool);
        if (result != 0) {
            break;
        }
    }

    return result;
}

static MemRawChunkHeader* AllocateSuperChunk(MemRawChunkPool* chunkPool, uint32_t chunkCount, uint32_t* allocated)
{
    MemRawChunkHeader* superChunk = NULL;

    // NOTE: we cannot allocate one big contiguous memory chunk, because afterwards it is impossible
    // to deallocate only one chunk originating from a super chunk.

    // allocate chunks one by one and chain them
    if (allocated) {
        *allocated = 0;
    }
    for (uint32_t i = 0; i < chunkCount; ++i) {
        MemRawChunkHeader* chunk = AllocateChunk(chunkPool);
        if (chunk != NULL) {
            chunk->m_nextChunk = superChunk;
            superChunk = chunk;
            if (allocated) {
                ++(*allocated);
            }
        } else {
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                "Chunk Pool Initialization",
                "Failed to allocate chunk %u for super chunk of size %u",
                i,
                chunkCount);
            // caller should decide whether to cleanup
            break;
        }
    }

    return superChunk;
}

static inline uint32_t PushSuperChunk(MemRawChunkPool* chunkPool, MemRawChunkHeader* head, uint32_t count)
{
    // attention: caller has already update total chunks count when securing chunks
    uint32_t chunksPushed = 0;
    while (head != NULL) {
        // detach head
        MemRawChunkHeader* chunk = head;
        head = head->m_nextChunk;
        chunk->m_nextChunk = NULL;
        int ret = MemLFStackPush(&chunkPool->m_chunkAllocStack, chunk);
        if (ret == 0) {
            ++chunksPushed;
        } else {
            // this is unexpected and indicates a bug
            MOT_LOG_WARN("Reached high black-mark %u in %s chunk pool while pushing super-chunk part %u/%u",
                chunkPool->m_highBlackMark,
                chunkPool->m_poolName,
                chunksPushed,
                count);
            FreeChunk(chunkPool, chunk);
            break;
        }
    }

    // free all chunks that cannot be inserted
    if (head != NULL) {
        FreeChunkStack(chunkPool, head);
    }
    return chunksPushed;
}

static int AllocateAndPushSuperChunk(MemRawChunkPool* chunkPool, uint32_t chunkCount)
{
    int result = 0;

    // check for stop flag
    AsyncReserveData* asyncReserveData = (AsyncReserveData*)chunkPool->m_asyncReserveData;
    if (asyncReserveData && GetGlobalAbortFlag(asyncReserveData)) {
        MOT_REPORT_ERROR(
            MOT_ERROR_INTERNAL, "Chunk Pool Initialization", "Aborting pre-allocation due to initialization error");
        return MOT_ERROR_INTERNAL;
    }

    // first secure the required amount of chunks (we expect and require full success, since this is pre-allocation
    // phase)
    uint32_t chunksSecured = MemRawChunkPoolSecureChunks(chunkPool, chunkCount);
    if (chunksSecured != chunkCount) {
        MOT_LOG_TRACE("Failed to secure %u chunks: reached high black mark", chunkCount);
        result = MOT_ERROR_RESOURCE_LIMIT;
    } else {
        uint32_t allocated = 0;
        MemRawChunkHeader* superChunk = AllocateSuperChunk(chunkPool, chunkCount, &allocated);
        if (superChunk != NULL) {
            if (allocated != chunkCount) {  // we expect and require full success in pre-allocation phase
                // cleanup
                result = GetLastError();
                FreeChunkStack(chunkPool, superChunk);
            } else {
                uint32_t chunksPushed = PushSuperChunk(chunkPool, superChunk, allocated);
                if (chunksPushed < allocated) {
                    MOT_LOG_TRACE("Reached high black-mark %u in %s chunk pool",
                        chunkPool->m_highBlackMark,
                        chunkPool->m_poolName);
                    MOT_ATOMIC_SUB(chunkPool->m_totalChunkCount,
                        (chunksSecured - chunksPushed));  // keep counter accurate even if failed
                    result = MOT_ERROR_RESOURCE_LIMIT;
                } else {
                    MOT_LOG_DIAG1("Pushed %u chunks from %u super-chunk while pre-fetching", chunksPushed, allocated);
                }
            }
        } else {
            // no more memory, stop allocating silently
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                "Chunk Pool Initialization",
                "Failed to allocate a chunk for chunk pool %s on node %d",
                chunkPool->m_poolName,
                chunkPool->m_node);
            MOT_ATOMIC_SUB(chunkPool->m_totalChunkCount, chunksSecured);  // keep counter accurate even if failed
            result = GetLastError();
        }
    }

    return result;
}

static int AllocateSuperChunks(MemRawChunkPool* chunkPool, uint32_t chunkCount)
{
    int result = 0;

    // work in groups of super-chunks (more effective)
    uint32_t superChunkCount = chunkCount / MM_SUPER_CHUNK_SIZE;
    uint32_t extraChunkCount = chunkCount % MM_SUPER_CHUNK_SIZE;  // number of chunks for last extra iteration
    for (uint32_t i = 0; i < superChunkCount; ++i) {
        result = AllocateAndPushSuperChunk(chunkPool, MM_SUPER_CHUNK_SIZE);
        if (result != 0) {
            break;
        }
    }
    if ((extraChunkCount > 0) && (result == 0)) {
        result = AllocateAndPushSuperChunk(chunkPool, extraChunkCount);
    }

    return result;
}

static void* ReserveWorker(void* param)
{
    ReserveWorkerData* reserveParam = (ReserveWorkerData*)param;
    uint32_t workerId = reserveParam->m_workerId;
    MemRawChunkPool* chunkPool = reserveParam->m_chunkPool;

    knl_thread_mot_init();

    if (workerId > 0) {  // first worker is invoked in caller context and not spawned in a new thread
        AllocThreadId();
    }

    // we must be affined to the correct NUMA node for native allocation policy
    // but it is better anyway to be affined to the NUMA node of the pool
    if (GetGlobalConfiguration().m_enableNuma && !GetTaskAffinity().SetNodeAffinity(chunkPool->m_node)) {
        if (g_memGlobalCfg.m_chunkAllocPolicy == MEM_ALLOC_POLICY_NATIVE) {
            MOT_LOG_WARN("Failed to set chunk reservation worker affinity, chunk pre-allocation performance may be "
                         "affected, and table data distribution may be affected");
        } else {
            MOT_LOG_WARN("Failed to set chunk reservation worker affinity, chunk pre-allocation performance may be "
                         "affected");
        }
    }

    MOT_LOG_TRACE("Chunk reservation worker %u for chunk pool %s on node %d started with %u chunks job",
        workerId,
        chunkPool->m_poolName,
        chunkPool->m_node,
        reserveParam->m_chunkCount);
#ifdef USING_SUPER_CHUNK
    int result = AllocateSuperChunks(chunkPool, reserveParam->m_chunkCount);
#else
    int result = AllocateChunks(chunkPool, reserveParam->m_chunkCount);
#endif

    if (result == 0) {
        MOT_LOG_TRACE("Chunk reservation worker %u for chunk pool %s on node %d finished successfully",
            workerId,
            chunkPool->m_poolName,
            chunkPool->m_node);
    } else {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
            "Chunk Pool Initialization",
            "Chunk reservation worker %u for chunk pool %s on node %d finished with error: %s (error code %d)",
            workerId,
            chunkPool->m_poolName,
            chunkPool->m_node,
            ErrorCodeToString(result),
            result);
    }

    if (workerId > 0) {  // first worker is invoked in caller context and not spawned in a new thread
        FreeThreadId();
    }
    return (void*)(uint64_t)result;
}

static void MemLFChunkStackToString(int indent, const char* name, MemLFStack* chunkList, StringBuffer* stringBuffer)
{
    StringBufferAppend(stringBuffer, "%*s%s Chunk List = { ", indent, "", name);
    MemLFStackNode* itr = chunkList->m_head.m_node;
    while (itr != NULL) {
        StringBufferAppend(stringBuffer, "%p", itr->m_value);
        if (itr->m_next) {
            StringBufferAppend(stringBuffer, ", ");
        }
        itr = itr->m_next;
    }
    StringBufferAppend(stringBuffer, " }");
}

}  // namespace MOT

extern "C" void MemRawChunkPoolDump(void* arg)
{
    MOT::MemRawChunkPool* chunkPool = (MOT::MemRawChunkPool*)arg;

    MOT::StringBufferApply([chunkPool](MOT::StringBuffer* stringBuffer) {
        MOT::MemRawChunkPoolToString(0, "Debug Dump", chunkPool, stringBuffer, MOT::MEM_REPORT_DETAILED);
        fprintf(stderr, "%s", stringBuffer->m_buffer);
        fflush(stdout);
    });
}

extern "C" int MemRawChunkPoolAnalyze(void* pool, void* buffer)
{
    MOT::MemRawChunkPool* chunkPool = (MOT::MemRawChunkPool*)pool;
    MOT::MemRawChunkHeader* chunk = (MOT::MemRawChunkHeader*)MOT::MemRawChunkDirLookup(buffer);
    if (!chunk) {
        return 0;
    }

    // search the chunk in the allocated chunk stack
    MOT::MemLFStackNode* itr = chunkPool->m_chunkAllocStack.m_head.m_node;
    while (itr != NULL) {
        if ((void*)itr->m_value == chunk) {
            fprintf(stderr,
                "Buffer %p found in chunk %p found in chunk pool %s allocation stack\n",
                buffer,
                chunk,
                chunkPool->m_poolName);
            return 1;
        } else {
            itr = itr->m_next;
        }
    }

    // search in cascaded sub-pool
    if (chunkPool->m_subChunkPool) {
        return MemRawChunkPoolAnalyze(chunkPool->m_subChunkPool, buffer);
    }

    return 0;
}
