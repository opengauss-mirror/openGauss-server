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
 * mm_huge_object_allocator.h
 *    Memory huge object allocation infrastructure.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/memory/mm_huge_object_allocator.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef MM_HUGE_OBJECT_ALLOCATOR_H
#define MM_HUGE_OBJECT_ALLOCATOR_H

#include "mm_virtual_huge_chunk.h"
#include "mm_def.h"
#include "mm_lock.h"
#include "string_buffer.h"
#include "utilities.h"

#include <cstddef>

namespace MOT {

/**
 * @typedef MemHugeAllocStats Low-level memory allocation statistics data.
 */
struct MemHugeAllocStats {
    /** @var Current per-node memory usage in bytes (amount used by system). */
    uint64_t m_memUsedBytes[MEM_MAX_NUMA_NODES];

    /** @var Current per-node memory usage in bytes (amount request by caller). */
    uint64_t m_memRequestedBytes[MEM_MAX_NUMA_NODES];

    /** @var The number of huge objects allocated on each node. */
    uint64_t m_objectCount[MEM_MAX_NUMA_NODES];
};

/**
 * @brief Initialize huge allocation infrastructure.
 */
extern void MemHugeInit();

/**
 * @brief Destroy huge allocation infrastructure.
 */
extern void MemHugeDestroy();

/**
 * @brief Allocates a huge object.
 * @param size The object size in bytes.
 * @param node The NUMA node on which to allocate the object.
 * @param allocType The type of memory to allocate (global or local).
 * @param chunkHeaderList A huge allocation list into which the new huge allocation will be pushed.
 * @param[opt] Optional lock for serializing access to the huge allocation list.
 * @return The allocated object or NULL if failed.
 */
extern void* MemHugeAlloc(uint64_t size, int node, MemAllocType allocType, MemVirtualHugeChunkHeader** chunkHeaderList,
    MemLock* lock = nullptr);

/**
 * @brief De-allocates a huge object.
 * @param chunkHeader The virtual huge chunk header of the object
 * @param chunkHeaderList A huge allocation list from which the huge allocation will be removed.
 * @param[opt] Optional lock for serializing access to the huge allocation list.
 * @param object The object to de-allocate.
 */
extern void MemHugeFree(MemVirtualHugeChunkHeader* chunkHeader, void* object,
    MemVirtualHugeChunkHeader** chunkHeaderList, MemLock* lock = nullptr);

/**
 * @brief Reallocates a huge object.
 * @param chunkHeader The virtual huge chunk header of the object
 * @param object The huge object to reallocate.
 * @param newSizeBytes The new object size in bytes (expected to be larger than current object size).
 * @param flags Reallocation flags.
 * @param chunkHeaderList A huge allocation list that will updated with huge allocation details.
 * @param[opt] Optional lock for serializing access to the huge allocation list.
 * @return The reallocated object or NULL if failed (last error set appropriately). In case of
 * failure to allocate a new object, the old object is left intact.
 * @see MemReallocFlags
 */
extern void* MemHugeRealloc(MemVirtualHugeChunkHeader* chunkHeader, void* object, uint64_t newSizeBytes,
    MemReallocFlags flags, MemVirtualHugeChunkHeader** chunkHeaderList, MemLock* lock = nullptr);

/**
 * @brief Retrieves up-to-date memory usage statistics for huge allocations.
 * @param [out] The statistics.
 */
extern void MemHugeGetStats(MemHugeAllocStats* stats);

/**
 * @brief Print debug statistics.
 * @param name The report name to use in printing.
 * @param logLevel The log level to use in printing.
 * @param The statistics to print.
 */
extern void MemHugePrintStats(const char* name, LogLevel logLevel, MemHugeAllocStats* stats);

/**
 * @brief Print current debug statistics.
 * @param name The report name to use in printing.
 * @param logLevel The log level to use in printing.
 */
extern void MemHugePrintCurrentStats(const char* name, LogLevel logLevel);

/**
 * @brief Prints global memory status into log.
 * @param name The name to prepend to the log message.
 * @param logLevel The log level to use in printing.
 * @param[opt] reportMode Specifies the report mode.
 */
extern void MemHugePrint(const char* name, LogLevel logLevel, MemReportMode reportMode = MEM_REPORT_SUMMARY);

/**
 * @brief Dumps global memory status into string buffer.
 * @param indent The indentation level.
 * @param name The name to prepend to the log message.
 * @param stringBuffer The string buffer.
 * @param[opt] reportMode Specifies the report mode.
 */
extern void MemHugeToString(
    int indent, const char* name, StringBuffer* stringBuffer, MemReportMode reportMode = MEM_REPORT_SUMMARY);

}  // namespace MOT

#endif /* MM_HUGE_OBJECT_ALLOCATOR_H */
