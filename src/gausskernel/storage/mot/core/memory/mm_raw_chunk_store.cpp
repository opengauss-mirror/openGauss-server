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
 * mm_raw_chunk_store.cpp
 *    The chunk store manages all the global chunk pools.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/memory/mm_raw_chunk_store.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "mm_raw_chunk_store.h"
#include "mm_cfg.h"
#include "mm_numa.h"
#include "mm_def.h"
#include "utilities.h"
#include "mot_error.h"

#include <stdlib.h>
#include <string.h>

namespace MOT {

DECLARE_LOGGER(RawChunkStore, Memory)

// global/local per-node chunk pools
static MemRawChunkPool** globalChunkPools = NULL;
static MemRawChunkPool** localChunkPools = NULL;

static int MemRawChunkStoreInitGlobal(MemReserveMode reserveMode, MemStorePolicy storePolicy);
static void MemRawChunkStoreDestroyGlobal();

static int MemRawChunkStoreInitLocal(MemReserveMode reserveMode, MemStorePolicy storePolicy);
static void MemRawChunkStoreDestroyLocal();

extern int MemRawChunkStoreInit(
    MemReserveMode reserveMode /* = MEM_RESERVE_PHYSICAL */, MemStorePolicy storePolicy /* = MEM_STORE_COMPACT */)
{
    MOT_LOG_TRACE("Initializing chunk store");
    int result = MemRawChunkStoreInitGlobal(reserveMode, storePolicy);
    if (result == 0) {
        result = MemRawChunkStoreInitLocal(reserveMode, storePolicy);
    }

    if (result == 0) {
        MOT_LOG_TRACE("Chunk store initialized successfully");
    }

    return result;
}

extern void MemRawChunkStoreDestroy()
{
    MOT_LOG_TRACE("Destroying chunk store");

    // destroy local pools
    MemRawChunkStoreDestroyLocal();

    // destroy global pools
    MemRawChunkStoreDestroyGlobal();

    MOT_LOG_TRACE("Chunk store destroyed");
}

extern void* MemRawChunkStoreAllocGlobal(int node)
{
    void* chunk = MemRawChunkPoolAlloc(globalChunkPools[node]);
    if (chunk == NULL) {
        // reached high black mark in this pool, continue attempt in round robin
        for (int i = (node + 1) % g_memGlobalCfg.m_nodeCount; i != node; i = (i + 1) % g_memGlobalCfg.m_nodeCount) {
            chunk = MemRawChunkPoolAlloc(globalChunkPools[i]);
            if (chunk != NULL) {
                break;
            }
        }
    }
    if (chunk == NULL) {
        // report once for current thread
        if (GetLastError() == MOT_NO_ERROR) {
            MOT_REPORT_ERROR(
                MOT_ERROR_OOM, "N/A", "Failed to allocate a global chunk: All global chunk pools are depleted");
        }
    }
    return chunk;
}

extern void* MemRawChunkStoreAllocLocal(int node)
{
    void* chunk = MemRawChunkPoolAlloc(localChunkPools[node]);
    if (chunk == NULL) {
        // reached high black mark in this pool, continue attempt in round robin
        for (int i = (node + 1) % g_memGlobalCfg.m_nodeCount; i != node; i = (i + 1) % g_memGlobalCfg.m_nodeCount) {
            chunk = MemRawChunkPoolAlloc(localChunkPools[i]);
            if (chunk != NULL) {
                break;
            }
        }
    }
    if (chunk == NULL) {
        // report once for current thread
        if (GetLastError() == MOT_NO_ERROR) {
            MOT_REPORT_ERROR(
                MOT_ERROR_OOM, "N/A", "Failed to allocate a local chunk: All local chunk pools are depleted");
        }
    }
    return chunk;
}

extern void MemRawChunkStoreFreeGlobal(void* chunk, int node)
{
    MemRawChunkPoolFree(globalChunkPools[node], chunk);
}

extern void MemRawChunkStoreFreeLocal(void* chunk, int node)
{
    MemRawChunkPoolFree(localChunkPools[node], chunk);
}

extern void MemRawChunkStorePrint(
    const char* name, LogLevel logLevel, MemReportMode reportMode /* = MEM_REPORT_SUMMARY */)
{
    if (MOT_CHECK_LOG_LEVEL(logLevel)) {
        StringBufferApply([name, logLevel, reportMode](StringBuffer* stringBuffer) {
            MemRawChunkStoreToString(0, name, stringBuffer, reportMode);
            MOT_LOG(logLevel, "%s", stringBuffer->m_buffer);
        });
    }
}

extern void MemRawChunkStoreToString(
    int indent, const char* name, StringBuffer* stringBuffer, MemReportMode reportMode /* = MEM_REPORT_SUMMARY */)
{
    StringBufferAppend(stringBuffer, "%*sRaw Chunk Store %s:\n", indent, "", name);
    for (uint32_t i = 0; i < g_memGlobalCfg.m_nodeCount; ++i) {
        MemRawChunkPoolToString(indent + PRINT_REPORT_INDENT, "", globalChunkPools[i], stringBuffer, reportMode);
        StringBufferAppend(stringBuffer, "\n");
    }
    for (uint32_t i = 0; i < g_memGlobalCfg.m_nodeCount; ++i) {
        MemRawChunkPoolToString(indent + PRINT_REPORT_INDENT, "", localChunkPools[i], stringBuffer, reportMode);
        StringBufferAppend(stringBuffer, "\n");
    }
}

extern MemRawChunkPool* MemRawChunkStoreGetGlobalPool(int node)
{
    MemRawChunkPool* result = NULL;
    if ((node >= 0) && (node < (int)g_memGlobalCfg.m_nodeCount)) {
        result = globalChunkPools[node];
    } else {
        MOT_REPORT_ERROR(MOT_ERROR_INVALID_ARG,
            "Get Global Chunk Pool",
            "Invalid NUMA node number %s passed to mm_raw_chunk_store_get_global_pool() (maximum allowed is %u)",
            node,
            g_memGlobalCfg.m_nodeCount - 1);
    }
    return result;
}

extern MemRawChunkPool* MemRawChunkStoreGetLocalPool(int node)
{
    MemRawChunkPool* result = NULL;
    if ((node >= 0) && (node < (int)g_memGlobalCfg.m_nodeCount)) {
        result = localChunkPools[node];
    } else {
        MOT_REPORT_ERROR(MOT_ERROR_INVALID_ARG,
            "Get Local Chunk Pool",
            "Invalid NUMA node number %s passed to mm_raw_chunk_store_get_local_pool() (maximum allowed is %u)",
            node,
            g_memGlobalCfg.m_nodeCount - 1);
    }
    return result;
}

static uint32_t MemRawChunkStoreGetStats(
    MemRawChunkPoolStats* chunkPoolStatsArray, uint32_t statsArraySize, bool isGlobal)
{
    // accumulate all pool statistics
    MemRawChunkPoolStats aggrPoolStats = {0};
    MemRawChunkPoolStats tmpStats;
    uint32_t entriesReported = 1;  // For the accumulated entry

    for (uint32_t node = 0; node < g_memGlobalCfg.m_nodeCount; ++node) {
        if (isGlobal) {
            MemRawChunkPoolGetStats(MemRawChunkStoreGetGlobalPool((int)node), &tmpStats);
        } else {
            MemRawChunkPoolGetStats(MemRawChunkStoreGetLocalPool((int)node), &tmpStats);
        }

        aggrPoolStats.m_reservedBytes += tmpStats.m_reservedBytes;
        aggrPoolStats.m_usedBytes += tmpStats.m_usedBytes;

        if (node + 1 < statsArraySize) {
            chunkPoolStatsArray[node + 1] = tmpStats;
            entriesReported++;
        }
    }

    // Get accumulated pool statistics. Aggregated statistics of all pools, so we set NUMA node as -1.
    aggrPoolStats.m_node = -1;
    chunkPoolStatsArray[0] = aggrPoolStats;
    return entriesReported;
}

extern uint32_t MemRawChunkStoreGetGlobalStats(MemRawChunkPoolStats* chunkPoolStatsArray, uint32_t statsArraySize)
{
    return MemRawChunkStoreGetStats(chunkPoolStatsArray, statsArraySize, true);
}

extern uint32_t MemRawChunkStoreGetLocalStats(MemRawChunkPoolStats* chunkPoolStatsArray, uint32_t statsArraySize)
{
    return MemRawChunkStoreGetStats(chunkPoolStatsArray, statsArraySize, false);
}

static int InitChunkPools(MemRawChunkPool** chunkPools, MemReserveMode reserveMode, MemStorePolicy storePolicy,
    uint32_t chunkReserveCount, uint32_t highBlackMark, uint32_t highRedMark, MemAllocType allocType)
{
    int result = 0;
    errno_t erc;
    const char* name = MemAllocTypeToString(allocType);
    const char* nameCapital = (allocType == MEM_ALLOC_GLOBAL) ? "Global" : "Local";

    MOT_LOG_TRACE("Reserving %s memory", name);
    for (uint32_t i = 0; i < g_memGlobalCfg.m_nodeCount; ++i) {
        chunkPools[i] = (MemRawChunkPool*)MemNumaAllocLocal(sizeof(MemRawChunkPool), (int)i);
        if (chunkPools[i] == NULL) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM,
                "Chunk Store Initialization",
                "Failed to allocate %u bytes for %s chunk pool on node %u, aborting.",
                (unsigned)sizeof(MemRawChunkPool),
                name,
                i);
            result = MOT_ERROR_OOM;
            break;
        }

        // initialize global pool
        MOT_LOG_TRACE("Initializing %s chunk pool for node %u with %u pre-alloc chunks (high red-mark: %u chunks, "
                      "reserve-mode: %s)",
            name,
            i,
            chunkReserveCount,
            highRedMark,
            MemReserveModeToString(reserveMode));
        MOT_LOG_TRACE("Reserving %0.2f GB of %s memory",
            ((double)chunkReserveCount) * MEM_CHUNK_SIZE_MB / KILO_BYTE,
            MemReserveModeToString(reserveMode));

        char name[32];
        erc = snprintf_s(name, sizeof(name), sizeof(name) - 1, "%s[%u]", nameCapital, i);
        securec_check_ss(erc, "\0", "\0");
        result = MemRawChunkPoolInit(chunkPools[i],
            name,
            i,
            allocType,
            chunkReserveCount,
            g_memGlobalCfg.m_chunkPreallocWorkerCount,
            highRedMark,
            highBlackMark,
            reserveMode,
            storePolicy,
            NULL,
            1 /* async */);
        if (result != 0) {
            // avoid destruction of this pool during cleanup
            MemNumaFreeLocal(chunkPools[i], sizeof(MemRawChunkPool), (int)i);
            chunkPools[i] = NULL;
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                "Chunk Store Initialization",
                "%s chunk pool initialization for node %u failed with error: %s (error code %d), aborting.",
                nameCapital,
                i,
                ErrorCodeToString(result),
                result);
            // order all other chunk pools to abort reservation
            for (uint32_t j = 0; j < i; ++j) {
                MemRawChunkPoolAbortReserve(chunkPools[j]);
            }
            break;
        }
    }

    // wait for all to finish
    if (result == 0) {
        MOT_LOG_TRACE("Waiting for %s memory pre-allocation to end", name);
        for (uint32_t i = 0; i < g_memGlobalCfg.m_nodeCount; ++i) {
            if (result == 0) {
                result = MemRawChunkPoolWaitReserve(chunkPools[i]);
                if (result != 0) {
                    MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                        "Chunk Store Initialization",
                        "%s chunk pool initialization for node %u failed during waiting for pre-allocation with error: "
                        "%s (error code %d), aborting.",
                        nameCapital,
                        i,
                        ErrorCodeToString(result),
                        result);
                    // continue as cleanup
                }
            } else {
                // in case of failure abort quickly rather than waiting
                MemRawChunkPoolAbortReserve(chunkPools[i]);
            }
        }
    }

    if (result == 0) {
        MOT_LOG_TRACE("Memory reservation finished");
    }

    return result;
}

static int MemRawChunkStoreInitGlobal(MemReserveMode reserveMode, MemStorePolicy storePolicy)
{
    int result = 0;
    MOT_LOG_TRACE("Initializing global chunk pools");
    uint64_t chunkReserveCount = g_memGlobalCfg.m_minGlobalMemoryMb / MEM_CHUNK_SIZE_MB / g_memGlobalCfg.m_nodeCount;
    uint64_t highBlackMark = g_memGlobalCfg.m_maxGlobalMemoryMb / MEM_CHUNK_SIZE_MB / g_memGlobalCfg.m_nodeCount;
    uint64_t highRedMark = highBlackMark * g_memGlobalCfg.m_highRedMarkPercent / 100;

    globalChunkPools = (MemRawChunkPool**)calloc(g_memGlobalCfg.m_nodeCount, sizeof(MemRawChunkPool*));
    if (globalChunkPools == NULL) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Chunk Store Initialization",
            "Failed to allocate %u bytes for global chunk pool array, aborting.",
            (unsigned)(g_memGlobalCfg.m_nodeCount * sizeof(MemRawChunkPool*)));
        result = MOT_ERROR_OOM;
    } else {
        result = InitChunkPools(globalChunkPools,
            reserveMode,
            storePolicy,
            chunkReserveCount,
            (uint32_t)highBlackMark,
            (uint32_t)highRedMark,
            MEM_ALLOC_GLOBAL);
    }

    if (result != 0) {
        MemRawChunkStoreDestroyGlobal();
    }

    return result;
}

static void MemRawChunkStoreDestroyGlobal()
{
    if (globalChunkPools != NULL) {
        for (uint32_t i = 0; i < g_memGlobalCfg.m_nodeCount; ++i) {
            if (globalChunkPools[i] != NULL) {
                MemRawChunkPoolDestroy(globalChunkPools[i]);
                MemNumaFreeLocal(globalChunkPools[i], sizeof(MemRawChunkPool), (int)i);
                globalChunkPools[i] = NULL;
            }
        }
        free(globalChunkPools);
        globalChunkPools = NULL;
    }
}

static int MemRawChunkStoreInitLocal(MemReserveMode reserveMode, MemStorePolicy storePolicy)
{
    int result = 0;
    MOT_LOG_TRACE("Initializing local chunk pools");
    uint64_t chunkReserveCount = g_memGlobalCfg.m_minLocalMemoryMb / MEM_CHUNK_SIZE_MB / g_memGlobalCfg.m_nodeCount;
    uint64_t highBlackMark = g_memGlobalCfg.m_maxLocalMemoryMb / MEM_CHUNK_SIZE_MB / g_memGlobalCfg.m_nodeCount;
    uint64_t highRedMark = highBlackMark;  // no emergency state for local pools

    localChunkPools = (MemRawChunkPool**)calloc(g_memGlobalCfg.m_nodeCount, sizeof(MemRawChunkPool*));
    if (localChunkPools == NULL) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Chunk Store Initialization",
            "Failed to allocate %u bytes for local chunk pool array, aborting.",
            (unsigned)(g_memGlobalCfg.m_nodeCount * sizeof(MemRawChunkPool*)));
        result = MOT_ERROR_OOM;
    } else {
        result = InitChunkPools(localChunkPools,
            reserveMode,
            storePolicy,
            chunkReserveCount,
            (uint32_t)highBlackMark,
            (uint32_t)highRedMark,
            MEM_ALLOC_LOCAL);
    }

    if (result != 0) {
        MemRawChunkStoreDestroyLocal();
    }

    return result;
}

static void MemRawChunkStoreDestroyLocal()
{
    if (localChunkPools != NULL) {
        for (uint32_t i = 0; i < g_memGlobalCfg.m_nodeCount; ++i) {
            if (localChunkPools[i] != NULL) {
                MOT_LOG_TRACE("Destroying Local[%u] chunk pool", i);
                MemRawChunkPoolPrint("Shutdown Report", LogLevel::LL_DEBUG, localChunkPools[i]);
                MemRawChunkPoolDestroy(localChunkPools[i]);
                MemNumaFreeLocal(localChunkPools[i], sizeof(MemRawChunkPool), (int)i);
                localChunkPools[i] = NULL;
            }
        }
        free(localChunkPools);
        localChunkPools = NULL;
    }
}

}  // namespace MOT

extern "C" void MemRawChunkStoreDump()
{
    MOT::StringBufferApply([](MOT::StringBuffer* stringBuffer) {
        MOT::MemRawChunkStoreToString(0, "Debug Dump", stringBuffer, MOT::MEM_REPORT_DETAILED);
        fprintf(stderr, "%s", stringBuffer->m_buffer);
    });
}

extern "C" void MemRawChunkStoreAnalyze(void* address)
{
    for (uint32_t i = 0; i < MOT::g_memGlobalCfg.m_nodeCount; ++i) {
        if (MemRawChunkPoolAnalyze(MOT::globalChunkPools[i], address)) {
            return;
        }
    }
    for (uint32_t i = 0; i < MOT::g_memGlobalCfg.m_nodeCount; ++i) {
        if (MemRawChunkPoolAnalyze(MOT::localChunkPools[i], address)) {
            break;
        }
    }
}
