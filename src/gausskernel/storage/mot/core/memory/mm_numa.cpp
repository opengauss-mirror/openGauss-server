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
 * mm_numa.cpp
 *    This is the lowest layer of memory management.
 *    It enforces memory limits, as well as memory reservation (i.e. pre-allocation).
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/memory/mm_numa.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <string.h>

#include "mm_numa.h"
#include "mm_cfg.h"
#include "utilities.h"
#include "mot_error.h"
#include "mm_def.h"
#include "mot_atomic_ops.h"
#include "memory_statistics.h"
#include "mot_configuration.h"
#include "sys_numa_api.h"

namespace MOT {

DECLARE_LOGGER(MemNuma, Memory)

#define MEGABYTE (1024 * 1024)
#define RESET_WARNING_DELTA_BYTES (1024 * 1024 * 8)

// static configuration
static uint64_t maxGlobalMemoryBytes = 0;
static uint64_t maxLocalMemoryBytes = 0;

// Memory Usage Counters
static uint64_t globalMemUsedBytes = 0;
static uint64_t localMemUsedBytes[MEM_MAX_NUMA_NODES];

// Memory Usage Statistics
static uint64_t peakGlobalMemoryBytes = 0;
static uint64_t peakLocalMemoryBytes[MEM_MAX_NUMA_NODES];

static void UpdateLocalStats(uint64_t size, int node)
{
    uint64_t usedSize = MOT_ATOMIC_ADD(localMemUsedBytes[node], size);
    MOT_LOG_DIAG1("Increased local node %d memory usage to %" PRIu64 " bytes", node, usedSize);

    // update peak statistics
    uint64_t peak = peakLocalMemoryBytes[node];
    while (usedSize > peak) {  // check if current memory usage is above previous peak
        if (!MOT_ATOMIC_CAS(peakLocalMemoryBytes[node], peak, usedSize)) {
            peak = peakLocalMemoryBytes[node];  // retry
        }
    }
    MemoryStatisticsProvider::m_provider->AddNumaLocalAllocated(size);
    DetailedMemoryStatisticsProvider::m_provider->AddNumaLocalAllocated(node, size);
}

static void UpdateGlobalStats(uint64_t size)
{
    uint64_t usedSize = MOT_ATOMIC_ADD(globalMemUsedBytes, size);
    MOT_LOG_DIAG1("Increased global memory usage to %" PRIu64 " bytes", usedSize);

    // update peak statistics (consider putting this while loop in ifdef or restrict loop count)
    uint64_t peak = peakGlobalMemoryBytes;
    while (usedSize > peak) {  // check if current memory usage is above previous peak
        if (!MOT_ATOMIC_CAS(peakGlobalMemoryBytes, peak, usedSize)) {
            peak = peakGlobalMemoryBytes;  // retry
        }
    }
    MemoryStatisticsProvider::m_provider->AddNumaInterleavedAllocated(size);
}

extern void MemNumaInit()
{
    errno_t erc = memset_s(localMemUsedBytes, sizeof(localMemUsedBytes), 0, sizeof(localMemUsedBytes));
    securec_check(erc, "\0", "\0");
    erc = memset_s(peakLocalMemoryBytes, sizeof(peakLocalMemoryBytes), 0, sizeof(peakLocalMemoryBytes));
    securec_check(erc, "\0", "\0");

    maxGlobalMemoryBytes = g_memGlobalCfg.m_maxGlobalMemoryMb * MEGABYTE;
    maxLocalMemoryBytes = g_memGlobalCfg.m_maxLocalMemoryMb * MEGABYTE;
    MOT_LOG_TRACE("Initialized global limit to %" PRIu64 " MB (%" PRIu64 " bytes), and local limit to %" PRIu64
                  " MB (%" PRIu64 " bytes)",
        g_memGlobalCfg.m_maxGlobalMemoryMb,
        maxGlobalMemoryBytes,
        g_memGlobalCfg.m_maxLocalMemoryMb,
        maxLocalMemoryBytes);
}

extern void MemNumaDestroy()
{
    // print statistics
    MemNumaPrint("MM Layer Post-Shutdown Report", LogLevel::LL_INFO);
}

extern void* MemNumaAllocLocal(uint64_t size, int node)
{
    // do not impose hard limits!
    void* result = GetGlobalConfiguration().m_enableNuma ? MotSysNumaAllocOnNode(size, node) : malloc(size);
    if (result != NULL) {
        // update statistics if succeeded
        UpdateLocalStats(size, node);
    } else {
        // issue an error, this is a real problem
        // first report OOM as root cause before reporting system error, this way we make sure envelope can report error
        // correctly to user
        int errorCode = errno;  // save error code (it might be reset to zero due to call to printf()
        if (errorCode == ENOMEM) {
            MOT_REPORT_ERROR(
                MOT_ERROR_OOM, "Memory Allocation", "Failed to allocate %" PRIu64 " bytes from node %d", size, node);
        }
        MOT_REPORT_SYSTEM_ERROR_CODE(errorCode,
            MotSysNumaAllocOnNode,
            "Memory Allocation",
            "Failed to allocate %" PRIu64 " bytes from node %d",
            size,
            node);
    }

    return result;
}

extern void* MemNumaAllocGlobal(uint64_t size)
{
    // do not impose hard limits!
    void* result = GetGlobalConfiguration().m_enableNuma ? MotSysNumaAllocInterleaved(size) : malloc(size);
    if (result != NULL) {
        // update statistics if succeeded
        UpdateGlobalStats(size);
    } else {
        // issue an error, this is a real problem
        // first report OOM as root cause before reporting system error, this way we make sure envelope can report error
        // correctly to user
        int errorCode = errno;  // save error code (it might be reset to zero due to call to printf()
        if (errorCode == ENOMEM) {
            MOT_REPORT_ERROR(
                MOT_ERROR_OOM, "Memory Allocation", "Failed to allocate %" PRIu64 " interleaved bytes", size);
        }
        MOT_REPORT_SYSTEM_ERROR_CODE(
            errorCode, MotSysNumaAllocInterleaved, "N/A", "Failed to allocate %" PRIu64 " interleaved bytes", size);
    }

    return result;
}

static inline void* MallocAligned(size_t align, size_t size)
{
    void* result = nullptr;
    int rc = posix_memalign(&result, align, size);
    errno = rc;
    return result;
}

extern void* MemNumaAllocAlignedLocal(uint64_t size, uint64_t align, int node)
{
    // do not impose hard limits!
    void* result = GetGlobalConfiguration().m_enableNuma ? MotSysNumaAllocAlignedOnNode(size, align, node)
                                                         : MallocAligned(align, size);
    if (result != NULL) {
        // update statistics if succeeded
        UpdateLocalStats(size, node);
    } else {
        // issue an error, this is a real problem
        // first report OOM as root cause before reporting system error, this way we make sure envelope can report error
        // correctly to user
        int errorCode = errno;  // save error code (it might be reset to zero due to call to printf()
        if (errorCode == ENOMEM) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM,
                "Memory Allocation",
                "Failed to allocate %" PRIu64 " bytes from node %d aligned to %u bytes",
                size,
                node,
                (unsigned)align);
        }
        MOT_REPORT_SYSTEM_ERROR_CODE(errorCode,
            MotSysNumaAllocAlignedOnNode,
            "Memory Allocation",
            "Failed to allocate %" PRIu64 " bytes from node %d aligned to %u bytes",
            size,
            node,
            (unsigned)align);
    }

    return result;
}

extern void* MemNumaAllocAlignedGlobal(uint64_t size, uint64_t align)
{
    void* result = GetGlobalConfiguration().m_enableNuma ? MotSysNumaAllocAlignedInterleaved(size, align)
                                                         : MallocAligned(align, size);
    if (result != NULL) {
        // update statistics if succeeded
        UpdateGlobalStats(size);
    } else {
        // issue an error, this is a real problem
        // first report OOM as root cause before reporting system error, this way we make sure envelope can report error
        // correctly to user
        int errorCode = errno;  // save error code (it might be reset to zero due to call to printf()
        if (errorCode == ENOMEM) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM,
                "Memory Allocation",
                "Failed to allocate %" PRIu64 " interleaved bytes aligned to %u bytes",
                size,
                (unsigned)align);
        }
        MOT_REPORT_SYSTEM_ERROR_CODE(errorCode,
            MotSysNumaAllocAlignedInterleaved,
            "N/A",
            "Failed to allocate %" PRIu64 " interleaved bytes aligned to %u bytes",
            size,
            (unsigned)align);
    }

    return result;
}

extern void MemNumaFreeLocal(void* buf, uint64_t size, int node)
{
    if (GetGlobalConfiguration().m_enableNuma) {
        MotSysNumaFree(buf, size);
    } else {
        free(buf);
    }
    uint64_t memUsed = MOT_ATOMIC_SUB(localMemUsedBytes[node], size);
    MOT_LOG_DIAG1("Decreased local node %d memory usage to %" PRIu64 " bytes", node, memUsed);
    MemoryStatisticsProvider::m_provider->AddNumaLocalAllocated(-((int64_t)size));
    DetailedMemoryStatisticsProvider::m_provider->AddNumaLocalAllocated(node, -((int64_t)size));
}

extern void MemNumaFreeGlobal(void* buf, uint64_t size)
{
    if (GetGlobalConfiguration().m_enableNuma) {
        MotSysNumaFree(buf, size);
    } else {
        free(buf);
    }
    uint64_t memUsed = MOT_ATOMIC_SUB(globalMemUsedBytes, size);
    MOT_LOG_DIAG1("Decreased global memory usage to %" PRIu64 " bytes", memUsed);
    MemoryStatisticsProvider::m_provider->AddNumaInterleavedAllocated(-((int64_t)size));
}

extern void MemNumaGetStats(MemNumaStats* stats)
{
    errno_t erc = memset_s((void*)stats, sizeof(MemNumaStats), 0, sizeof(MemNumaStats));
    securec_check(erc, "\0", "\0");
    stats->m_globalMemUsedBytes = MOT_ATOMIC_LOAD(globalMemUsedBytes);
    stats->m_peakGlobalMemoryBytes = MOT_ATOMIC_LOAD(peakGlobalMemoryBytes);

    for (uint32_t i = 0; i < g_memGlobalCfg.m_nodeCount; ++i) {
        stats->m_localMemUsedBytes[i] = MOT_ATOMIC_LOAD(localMemUsedBytes[i]);
        stats->m_peakLocalMemoryBytes[i] = MOT_ATOMIC_LOAD(peakLocalMemoryBytes[i]);
    }
}

extern void MemNumaFormatStats(int indent, const char* name, StringBuffer* stringBuffer, MemNumaStats* stats,
    MemReportMode reportMode /* = MEM_REPORT_SUMMARY */)
{
    StringBufferAppend(stringBuffer, "%*sKernel allocation %s report:\n", indent, "", name);
    if (reportMode == MEM_REPORT_SUMMARY) {
        uint64_t localMemUsedBytes = 0;
        uint64_t localPeakMemUsedBytes = 0;
        for (uint32_t i = 0; i < g_memGlobalCfg.m_nodeCount; ++i) {
            localMemUsedBytes += stats->m_localMemUsedBytes[i];
            localPeakMemUsedBytes += stats->m_peakLocalMemoryBytes[i];
        }
        StringBufferAppend(stringBuffer,
            "%*sGlobal memory usage: Current = %" PRIu64 " MB, Peak = %" PRIu64 " MB\n",
            indent + PRINT_REPORT_INDENT,
            "",
            stats->m_globalMemUsedBytes / MEGA_BYTE,
            stats->m_peakGlobalMemoryBytes / MEGA_BYTE);
        StringBufferAppend(stringBuffer,
            "%*sLocal memory usage: Current = %" PRIu64 " MB, Peak = %" PRIu64 " MB\n",
            indent + PRINT_REPORT_INDENT,
            "",
            localMemUsedBytes / MEGA_BYTE,
            localPeakMemUsedBytes / MEGA_BYTE);
    } else {
        StringBufferAppend(stringBuffer,
            "%*sGlobal memory usage: Current = %" PRIu64 " MB, Peak = %" PRIu64 " MB\n",
            indent + PRINT_REPORT_INDENT,
            "",
            stats->m_globalMemUsedBytes / MEGA_BYTE,
            stats->m_peakGlobalMemoryBytes / MEGA_BYTE);

        for (uint32_t i = 0; i < g_memGlobalCfg.m_nodeCount; ++i) {
            if (stats->m_peakLocalMemoryBytes[i] > 0) {
                StringBufferAppend(stringBuffer,
                    "%*sLocal memory usage: Current = %" PRIu64 " MB, Peak = %" PRIu64 " MB\n",
                    indent + PRINT_REPORT_INDENT,
                    "",
                    stats->m_localMemUsedBytes[i] / MEGA_BYTE,
                    stats->m_peakLocalMemoryBytes[i] / MEGA_BYTE);
            }
        }
    }
}

extern void MemNumaPrintStats(
    const char* name, LogLevel logLevel, MemNumaStats* stats, MemReportMode reportMode /* = MEM_REPORT_SUMMARY */)
{
    StringBufferApply([name, logLevel, stats, reportMode](StringBuffer* stringBuffer) {
        MemNumaFormatStats(0, name, stringBuffer, stats, reportMode);
        MOT_LOG(logLevel, "%s", stringBuffer->m_buffer);
    });
}

extern void MemNumaPrintCurrentStats(
    const char* name, LogLevel logLevel, MemReportMode reportMode /* = MEM_REPORT_SUMMARY */)
{
    if (MOT_CHECK_LOG_LEVEL(logLevel)) {
        MemNumaStats stats = {0};
        MemNumaGetStats(&stats);
        MemNumaPrintStats(name, logLevel, &stats, reportMode);
    }
}

extern void MemNumaPrint(const char* name, LogLevel logLevel, MemReportMode reportMode /* = MEM_REPORT_SUMMARY */)
{
    if (MOT_CHECK_LOG_LEVEL(logLevel)) {
        StringBufferApply([name, logLevel, reportMode](StringBuffer* stringBuffer) {
            MemNumaToString(0, name, stringBuffer, reportMode);
            MOT_LOG(logLevel, "%s", stringBuffer->m_buffer);
        });
    }
}

extern void MemNumaToString(
    int indent, const char* name, StringBuffer* stringBuffer, MemReportMode reportMode /* = MEM_REPORT_SUMMARY */)
{
    MemNumaStats stats = {0};
    MemNumaGetStats(&stats);

    MemNumaFormatStats(indent, name, stringBuffer, &stats, reportMode);
}

}  // namespace MOT

extern "C" void MemNumaDump()
{
    MOT::StringBufferApply([](MOT::StringBuffer* stringBuffer) {
        MOT::MemNumaToString(0, "Debug Dump", stringBuffer, MOT::MEM_REPORT_DETAILED);
        fprintf(stderr, "%s", stringBuffer->m_buffer);
    });
}
