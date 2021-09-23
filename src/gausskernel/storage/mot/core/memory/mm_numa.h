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
 * mm_numa.h
 *    This is the lowest layer of memory management.
 *    It enforces memory limits, as well as memory reservation (i.e. pre-allocation).
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/memory/mm_numa.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef MM_NUMA_H
#define MM_NUMA_H

#include "mm_def.h"
#include "string_buffer.h"
#include "utilities.h"

#include <stddef.h>
#include <stdint.h>

namespace MOT {

/**
 * @typedef MemNumaStats Low-level memory allocation statistics data.
 */
struct MemNumaStats {
    /** @var Current global memory usage in bytes. */
    uint64_t m_globalMemUsedBytes;

    /** @var Current local (per-node) memory usage in bytes. */
    uint64_t m_localMemUsedBytes[MEM_MAX_NUMA_NODES];

    /** @var The all-time history peak global memory usage in bytes. */
    uint64_t m_peakGlobalMemoryBytes;

    /** @var The all-time history peak local (per-node) memory usage in bytes. */
    uint64_t m_peakLocalMemoryBytes[MEM_MAX_NUMA_NODES];
};

/** @brief Initializes lowest level NUMA-aware memory provider. */
extern void MemNumaInit();

/** @brief Cleans up lowest level NUMA-aware memory provider. */
extern void MemNumaDestroy();

/**
 * @brief Allocate NUMA-node local buffer.
 * @param size The allocation size in bytes.
 * @param node The NUMA node identifier.
 * @return A pointer to the allocated memory or NULL if failed (i.e. out of memory).
 * @note The total memory usage is limited by configuration.
 */
extern void* MemNumaAllocLocal(uint64_t size, int node);

/**
 * @brief Allocate NUMA-interleaved buffer.
 * @param size The allocation size in bytes.
 * @return A pointer to the allocated memory or NULL if failed (i.e. out of memory).
 * @note The total memory usage is limited by configuration.
 */
extern void* MemNumaAllocGlobal(uint64_t size);

/**
 * @brief Allocate NUMA-node local buffer.
 * @param size The allocation size in bytes.
 * @param align The allocation alignment. Must be a power of two.
 * @param node The NUMA node identifier.
 * @return A pointer to the allocated memory or NULL if failed (i.e. out of memory).
 * @note The total memory usage is limited by configuration.
 */
extern void* MemNumaAllocAlignedLocal(uint64_t size, uint64_t align, int node);

/**
 * @brief Allocate aligned NUMA-interleaved buffer.
 * @param size The allocation size in bytes.
 * @param align The allocation alignment. Must be a power of two.
 * @return A pointer to the allocated memory or NULL if failed (i.e. out of memory).
 * @note The total memory usage is limited by configuration.
 */
extern void* MemNumaAllocAlignedGlobal(uint64_t size, uint64_t align);

/**
 * @brief Reclaim memory previously allocated by a call to @fn mm_numa_alloc_local.
 * @param buf The buffer to reclaim.
 * @param size The size in bytes of the buffer to reclaim.
 * @param node The NUMA node identifier.
 */
extern void MemNumaFreeLocal(void* buf, uint64_t size, int node);

/**
 * @brief Reclaim memory previously allocated by a call to @fn mm_numa_alloc_global.
 * @param buf The buffer to reclaim.
 * @param size The size in bytes of the buffer to reclaim.
 */
extern void MemNumaFreeGlobal(void* buf, uint64_t size);

/**
 * @brief Retrieves up-to-date memory usage statistics.
 * @param [out] The statistics.
 */
extern void MemNumaGetStats(MemNumaStats* stats);

/**
 * @brief Formats statistics report into a string buffer.
 * @param indent The indentation level.
 * @param name The name of the report.
 * @param stringBuffer The string buffer.
 * @param stats The memory usage statistics.
 * @param[opt] reportMode Specifies the report mode.
 */
extern void MemNumaFormatStats(int indent, const char* name, StringBuffer* stringBuffer, MemNumaStats* stats,
    MemReportMode reportMode = MEM_REPORT_SUMMARY);

/**
 * @brief Print debug statistics.
 * @param name The report name to use in printing.
 * @param logLevel The log level to use in printing.
 * @param The statistics to print.
 * @param[opt] reportMode Specifies the report mode.
 */
extern void MemNumaPrintStats(
    const char* name, LogLevel logLevel, MemNumaStats* stats, MemReportMode reportMode = MEM_REPORT_SUMMARY);

/**
 * @brief Print current debug statistics.
 * @param name The report name to use in printing.
 * @param logLevel The log level to use in printing.
 * @param[opt] reportMode Specifies the report mode.
 */
extern void MemNumaPrintCurrentStats(
    const char* name, LogLevel logLevel, MemReportMode reportMode = MEM_REPORT_SUMMARY);

/**
 * @brief Prints global memory status into log.
 * @param name The name to prepend to the log message.
 * @param logLevel The log level to use in printing.
 * @param[opt] reportMode Specifies the report mode.
 */
extern void MemNumaPrint(const char* name, LogLevel logLevel, MemReportMode reportMode = MEM_REPORT_SUMMARY);

/**
 * @brief Dumps global memory status into string buffer.
 * @param indent The indentation level.
 * @param name The name to prepend to the log message.
 * @param stringBuffer The string buffer.
 * @param[opt] reportMode Specifies the report mode.
 */
extern void MemNumaToString(
    int indent, const char* name, StringBuffer* stringBuffer, MemReportMode reportMode = MEM_REPORT_SUMMARY);

}  // namespace MOT

/**
 * @brief Dumps global memory status into standard error stream.
 */
extern "C" void MemNumaDump();

#endif /* MM_NUMA_H */
