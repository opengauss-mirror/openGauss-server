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
 * mm_cfg.cpp
 *    Memory configuration interfaces.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/memory/mm_cfg.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "mm_cfg.h"
#include "utilities.h"
#include "mot_error.h"
#include "mm_buffer_class.h"
#include "thread_id.h"
#include "mot_configuration.h"
#include "mm_buffer_chunk.h"
#include "mm_session_large_buffer_store.h"

#include <sys/sysinfo.h>
#include <sys/time.h>
#include <sys/resource.h>

namespace MOT {
DECLARE_LOGGER(MemCfg, Memory)

MemCfg g_memGlobalCfg;
static int memCfgInitOnce = 0;  // initialize only once

static uint64_t totalMemoryMb = 0;
static uint64_t availableMemoryMb = 0;
static int InitTotalAvailMemory();
static int ValidateCfg();

extern int MemCfgInit()
{
    if (memCfgInitOnce) {
        return 0;  // already initialized successfully
    }
    memCfgInitOnce = 1;
    MOT_LOG_TRACE("Loading MM configuration");

    int res = InitTotalAvailMemory();
    if (res != 0) {
        MOT_REPORT_PANIC(res, "MM Layer Configuration Load", "Failed to load MM Layer configuration");
        return res;
    }

    // load static configuration
    MOTConfiguration& motCfg = GetGlobalConfiguration();
    g_memGlobalCfg.m_nodeCount = motCfg.m_numaNodes;
    g_memGlobalCfg.m_cpuCount = motCfg.m_numaNodes * motCfg.m_coresPerCpu;
    g_memGlobalCfg.m_maxThreadCount = motCfg.m_maxThreads;
    g_memGlobalCfg.m_maxConnectionCount = motCfg.m_maxConnections;
    g_memGlobalCfg.m_maxThreadsPerNode = g_memGlobalCfg.m_maxThreadCount / g_memGlobalCfg.m_nodeCount;

    g_memGlobalCfg.m_lazyLoadChunkDirectory = motCfg.m_lazyLoadChunkDirectory;

    g_memGlobalCfg.m_maxGlobalMemoryMb = motCfg.m_globalMemoryMaxLimitMB;
    g_memGlobalCfg.m_minGlobalMemoryMb = motCfg.m_globalMemoryMinLimitMB;
    g_memGlobalCfg.m_maxLocalMemoryMb = motCfg.m_localMemoryMaxLimitMB;
    g_memGlobalCfg.m_minLocalMemoryMb = motCfg.m_localMemoryMinLimitMB;
    g_memGlobalCfg.m_maxSessionMemoryKb = motCfg.m_sessionMemoryMaxLimitKB;
    g_memGlobalCfg.m_minSessionMemoryKb = motCfg.m_sessionMemoryMinLimitKB;
    g_memGlobalCfg.m_reserveMemoryMode = motCfg.m_reserveMemoryMode;
    g_memGlobalCfg.m_storeMemoryPolicy = motCfg.m_storeMemoryPolicy;
    MOT_LOG_INFO("Global Memory Limit is configured to: %" PRIu64 " MB --> %" PRIu64 " MB",
        g_memGlobalCfg.m_minGlobalMemoryMb,
        g_memGlobalCfg.m_maxGlobalMemoryMb);
    MOT_LOG_INFO("Local Memory Limit is configured to: %" PRIu64 " MB --> %" PRIu64 " MB",
        g_memGlobalCfg.m_minLocalMemoryMb,
        g_memGlobalCfg.m_maxLocalMemoryMb);
    MOT_LOG_INFO("Session Memory Limit is configured to: %" PRIu64 " KB --> %" PRIu64 " KB (maximum %u sessions)",
        g_memGlobalCfg.m_minSessionMemoryKb,
        g_memGlobalCfg.m_maxSessionMemoryKb,
        g_memGlobalCfg.m_maxConnectionCount);

    g_memGlobalCfg.m_chunkAllocPolicy = motCfg.m_chunkAllocPolicy;
    g_memGlobalCfg.m_chunkPreallocWorkerCount = motCfg.m_chunkPreallocWorkerCount;
    g_memGlobalCfg.m_highRedMarkPercent = motCfg.m_highRedMarkPercent;

    g_memGlobalCfg.m_sessionLargeBufferStoreSizeMb = motCfg.m_sessionLargeBufferStoreSizeMB;
    g_memGlobalCfg.m_sessionLargeBufferStoreMaxObjectSizeMb = motCfg.m_sessionLargeBufferStoreMaxObjectSizeMB;
    g_memGlobalCfg.m_sessionMaxHugeObjectSizeMb = motCfg.m_sessionMaxHugeObjectSizeMB;

    res = ValidateCfg();
    if (res != 0) {
        MOT_REPORT_PANIC(res, "MM Layer Configuration Load", "Failed to validate MM Layer configuration");
        return res;
    }

    // configure automatic chunk allocation policy
    if (g_memGlobalCfg.m_chunkAllocPolicy == MEM_ALLOC_POLICY_AUTO) {
        if (g_memGlobalCfg.m_nodeCount > 1) {
            g_memGlobalCfg.m_chunkAllocPolicy = MEM_ALLOC_POLICY_CHUNK_INTERLEAVED;
            MOT_LOG_INFO("Configured automatic chunk allocation policy to 'CHUNK-INTERLEAVED' on %u nodes NUMA "
                         "machine in PRODUCTION mode",
                g_memGlobalCfg.m_nodeCount);
        } else {
            g_memGlobalCfg.m_chunkAllocPolicy = MEM_ALLOC_POLICY_LOCAL;
            MOT_LOG_INFO("Configured automatic chunk allocation policy to 'LOCAL' on single node machine");
        }
    }

    MemCfgPrint("Startup Report", LogLevel::LL_TRACE);

    MOT_LOG_TRACE("MM configuration loaded");

    return 0;
}

extern void MemCfgDestroy()
{
    // currently nothing else than reset initialized flag
    memCfgInitOnce = false;
}

extern void MemCfgPrint(const char* name, LogLevel logLevel)
{
    if (MOT_CHECK_LOG_LEVEL(logLevel)) {
        StringBufferApply([name, logLevel](StringBuffer* stringBuffer) {
            MemCfgToString(0, name, stringBuffer);
            MOT_LOG(logLevel, "%s", stringBuffer->m_buffer);
        });
    }
}

extern void MemCfgToString(int indent, const char* name, StringBuffer* stringBuffer)
{
    StringBufferAppend(stringBuffer, "%*sMemory Configuration %s = {\n", indent, "", name);
    StringBufferAppend(stringBuffer, "%*sNode Count: %u\n", indent, "", g_memGlobalCfg.m_nodeCount);
    StringBufferAppend(stringBuffer, "%*sCPU Count: %u\n", indent, "", g_memGlobalCfg.m_cpuCount);
    StringBufferAppend(stringBuffer, "%*sMax Thread Count: %u\n", indent, "", g_memGlobalCfg.m_maxThreadCount);
    StringBufferAppend(stringBuffer, "%*sMax Connection Count: %u\n", indent, "", g_memGlobalCfg.m_maxConnectionCount);
    StringBufferAppend(
        stringBuffer, "%*sMax Thread per NUMA node: %u\n", indent, "", g_memGlobalCfg.m_maxThreadsPerNode);
    StringBufferAppend(stringBuffer,
        "%*sLazy Load Chunk Directory: %s\n",
        indent,
        "",
        g_memGlobalCfg.m_lazyLoadChunkDirectory ? "Yes" : "No");
    StringBufferAppend(
        stringBuffer, "%*sMax Global Memory MB: %" PRIu64 "\n", indent, "", g_memGlobalCfg.m_maxGlobalMemoryMb);
    StringBufferAppend(
        stringBuffer, "%*sMin Global Memory MB: %" PRIu64 "\n", indent, "", g_memGlobalCfg.m_minGlobalMemoryMb);
    StringBufferAppend(
        stringBuffer, "%*sMax Local Memory MB: %" PRIu64 "\n", indent, "", g_memGlobalCfg.m_maxLocalMemoryMb);
    StringBufferAppend(
        stringBuffer, "%*sMin Local Memory MB: %" PRIu64 "\n", indent, "", g_memGlobalCfg.m_minLocalMemoryMb);
    StringBufferAppend(
        stringBuffer, "%*sMax Session Memory KB: %" PRIu64 "\n", indent, "", g_memGlobalCfg.m_maxSessionMemoryKb);
    StringBufferAppend(
        stringBuffer, "%*sMin Session Memory KB: %" PRIu64 "\n", indent, "", g_memGlobalCfg.m_minSessionMemoryKb);
    StringBufferAppend(stringBuffer,
        "%*sReserve Memory Mode: %s\n",
        indent,
        "",
        MemReserveModeToString(g_memGlobalCfg.m_reserveMemoryMode));
    StringBufferAppend(stringBuffer,
        "%*sStore Memory Policy: %s\n",
        indent,
        "",
        MemStorePolicyToString(g_memGlobalCfg.m_storeMemoryPolicy));
    StringBufferAppend(stringBuffer, "%*sChunk Size MB: %u\n", indent, "", MEM_CHUNK_SIZE_MB);
    StringBufferAppend(stringBuffer,
        "%*sChunk Allocation Policy: %s\n",
        indent,
        "",
        MemAllocPolicyToString(g_memGlobalCfg.m_chunkAllocPolicy));
    StringBufferAppend(stringBuffer,
        "%*sChunk pre-allocation Worker Count: %u\n",
        indent,
        "",
        g_memGlobalCfg.m_chunkPreallocWorkerCount);
    StringBufferAppend(stringBuffer, "%*sHigh Red Mark: %u%%\n", indent, "", g_memGlobalCfg.m_highRedMarkPercent);
    StringBufferAppend(stringBuffer,
        "%*sSession Large Buffer Store Size MB: %" PRIu64 "\n",
        indent,
        "",
        g_memGlobalCfg.m_sessionLargeBufferStoreSizeMb);
    StringBufferAppend(stringBuffer,
        "%*sSession Large Buffer Store Max Object Size MB: %" PRIu64 "\n",
        indent,
        "",
        g_memGlobalCfg.m_sessionLargeBufferStoreMaxObjectSizeMb);
    StringBufferAppend(stringBuffer,
        "%*sSession Max Huge Object Size MB: %" PRIu64 "\n",
        indent,
        "",
        g_memGlobalCfg.m_sessionMaxHugeObjectSizeMb);
}

extern uint64_t GetTotalSystemMemoryMb()
{
    uint64_t result = 0;
    int res = InitTotalAvailMemory();
    if (res != 0) {
        MOT_REPORT_PANIC(res, "MM Layer Configuration Load", "Failed to load MM Layer configuration");
    } else {
        result = totalMemoryMb;
    }
    return result;
}

extern uint64_t GetAvailableSystemMemoryMb()
{
    uint64_t result = (uint64_t)-1;
    int res = InitTotalAvailMemory();
    if (res != 0) {
        MOT_REPORT_PANIC(res, "MM Layer Configuration Load", "Failed to load MM Layer configuration");
    } else {
        result = availableMemoryMb;
    }
    return result;
}

static int InitTotalAvailMemory()
{
    int result = 0;
    if (totalMemoryMb == 0) {
        struct sysinfo si;
        if (sysinfo(&si) == 0) {
            totalMemoryMb = si.totalram * si.mem_unit / MEGA_BYTE;
            availableMemoryMb = si.freeram * si.mem_unit / MEGA_BYTE;
            MOT_LOG_TRACE("Total system memory: %" PRIu64 " MB", totalMemoryMb);
            MOT_LOG_TRACE("Available system memory: %" PRIu64 " MB", availableMemoryMb);
        } else {
            MOT_REPORT_SYSTEM_PANIC(
                sysinfo, "MM Layer Configuration Load", "Failed to retrieve total/available system memory");
            result = MOT_ERROR_SYSTEM_FAILURE;
        }
    }
    return result;
}

static int ValidateCfg()
{
    // check node count limit
    if (g_memGlobalCfg.m_nodeCount > MEM_MAX_NUMA_NODES) {
        MOT_REPORT_PANIC(MOT_ERROR_INVALID_CFG,
            "MM Layer Configuration Validation",
            "Invalid number of NUMA nodes %u: Maximum allowed is %u",
            g_memGlobalCfg.m_nodeCount,
            (unsigned)MEM_MAX_NUMA_NODES);
        return MOT_ERROR_INVALID_CFG;
    }

    // check thread count limit
    if (g_memGlobalCfg.m_maxThreadCount > MAX_THREAD_COUNT) {
        MOT_REPORT_PANIC(MOT_ERROR_INVALID_CFG,
            "MM Layer Configuration Validation",
            "Invalid maximum number of threads%u: Maximum allowed is %u",
            g_memGlobalCfg.m_maxThreadCount,
            (unsigned)MAX_THREAD_COUNT);
        return MOT_ERROR_INVALID_CFG;
    }

    // validate buffers per chunk limit (in case someone chooses to change constants)
    int bufferSizeKb = MemBufferClassToSizeKb(MEM_BUFFER_CLASS_SMALLEST);
    if (bufferSizeKb == MEM_BUFFER_SIZE_INVALID ||
        (MEM_CHUNK_SIZE_MB * KILO_BYTE) / bufferSizeKb > MAX_BUFFERS_PER_CHUNK) {
        MOT_REPORT_PANIC(MOT_ERROR_INVALID_CFG,
            "MM Layer Configuration Validation",
            "Invalid memory configuration: Ratio between chunk size %u MB and buffer size class %u KB exceeds %u",
            MEM_CHUNK_SIZE_MB,
            bufferSizeKb,
            (unsigned)MAX_BUFFERS_PER_CHUNK);
        return MOT_ERROR_INVALID_CFG;
    }

    // make sure minimum and maximum values make sense
    if (g_memGlobalCfg.m_minGlobalMemoryMb > g_memGlobalCfg.m_maxGlobalMemoryMb) {
        MOT_REPORT_PANIC(MOT_ERROR_INVALID_CFG,
            "MM Layer Configuration Validation",
            "Invalid memory configuration: Global memory pre-allocation %" PRIu64
            " MB exceeds the maximum limit %" PRIu64 " MB",
            g_memGlobalCfg.m_minGlobalMemoryMb,
            g_memGlobalCfg.m_maxGlobalMemoryMb);
        return MOT_ERROR_INVALID_CFG;
    }

    if (g_memGlobalCfg.m_minLocalMemoryMb > g_memGlobalCfg.m_maxLocalMemoryMb) {
        MOT_REPORT_PANIC(MOT_ERROR_INVALID_CFG,
            "MM Layer Configuration Validation",
            "Invalid memory configuration: Local memory pre-allocation %" PRIu64
            " MB exceeds the maximum limit %" PRIu64 " MB",
            g_memGlobalCfg.m_minLocalMemoryMb,
            g_memGlobalCfg.m_maxLocalMemoryMb);
        return MOT_ERROR_INVALID_CFG;
    }

    // validate memory limits do not exceed machine capabilities or process limits
    if (g_memGlobalCfg.m_maxGlobalMemoryMb > totalMemoryMb) {
        MOT_REPORT_PANIC(MOT_ERROR_INVALID_CFG,
            "MM Layer Configuration Validation",
            "Invalid memory configuration: Global maximum memory limit %" PRIu64
            " MB exceeds machine capability %" PRIu64 " MB",
            g_memGlobalCfg.m_maxGlobalMemoryMb,
            totalMemoryMb);
        return MOT_ERROR_INVALID_CFG;
    }

    if (g_memGlobalCfg.m_maxLocalMemoryMb > totalMemoryMb) {
        MOT_REPORT_PANIC(MOT_ERROR_INVALID_CFG,
            "MM Layer Configuration Validation",
            "Invalid memory configuration: Local maximum memory limit %u MB exceeds machine capability %u MB",
            g_memGlobalCfg.m_maxGlobalMemoryMb,
            totalMemoryMb);
        return MOT_ERROR_INVALID_CFG;
    }

    uint64_t reserveMemoryMb = g_memGlobalCfg.m_maxGlobalMemoryMb + g_memGlobalCfg.m_maxLocalMemoryMb +
                               g_memGlobalCfg.m_sessionLargeBufferStoreSizeMb;
    if (reserveMemoryMb > totalMemoryMb) {
        MOT_REPORT_PANIC(MOT_ERROR_INVALID_CFG,
            "MM Layer Configuration Validation",
            "Invalid memory configuration: Maximum memory reservation %u MB exceeds machine capability %u MB",
            reserveMemoryMb,
            totalMemoryMb);
        return MOT_ERROR_INVALID_CFG;
    }

    // validate maximum large object size (issue warning and rectify)
    if (g_memGlobalCfg.m_sessionLargeBufferStoreMaxObjectSizeMb >
        g_memGlobalCfg.m_sessionLargeBufferStoreSizeMb / MEM_SESSION_MAX_LARGE_OBJECT_FACTOR) {
        MOT_LOG_WARN("Invalid memory configuration: maximum object size %u MB in large buffer store is too big, value "
                     "will be truncated to %u MB.",
            g_memGlobalCfg.m_sessionLargeBufferStoreMaxObjectSizeMb,
            g_memGlobalCfg.m_sessionLargeBufferStoreSizeMb / MEM_SESSION_MAX_LARGE_OBJECT_FACTOR);
        g_memGlobalCfg.m_sessionLargeBufferStoreMaxObjectSizeMb =
            g_memGlobalCfg.m_sessionLargeBufferStoreSizeMb / MEM_SESSION_MAX_LARGE_OBJECT_FACTOR;
    }

    if ((g_memGlobalCfg.m_sessionLargeBufferStoreMaxObjectSizeMb == 0) &&
        (g_memGlobalCfg.m_sessionLargeBufferStoreSizeMb > 0)) {
        MOT_LOG_WARN("Invalid memory configuration: maximum object size in large buffer store is zero, value will be "
                     "rectified to %u MB.",
            g_memGlobalCfg.m_sessionLargeBufferStoreSizeMb / MEM_SESSION_MAX_LARGE_OBJECT_FACTOR);
        g_memGlobalCfg.m_sessionLargeBufferStoreMaxObjectSizeMb =
            g_memGlobalCfg.m_sessionLargeBufferStoreSizeMb / MEM_SESSION_MAX_LARGE_OBJECT_FACTOR;
    }

    return 0;
}
}  // namespace MOT

extern "C" void MemCfgDump()
{
    MOT::StringBufferApply([](MOT::StringBuffer* stringBuffer) {
        MOT::MemCfgToString(0, "Debug Dump", stringBuffer);
        fprintf(stderr, "%s", stringBuffer->m_buffer);
        fflush(stderr);
    });
}
