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
 * mm_session_large_buffer_pool.h
 *    A pool of buffers of varying sizes for session large allocations.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/memory/mm_session_large_buffer_pool.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef MM_SESSION_LARGE_BUFFER_POOL_H
#define MM_SESSION_LARGE_BUFFER_POOL_H

#include "mm_session_large_buffer_list.h"

namespace MOT {
/** @struct MemSessionLargeBufferPool
 * @brief A pool of buffers of varying sizes for session large allocations.
 */
struct PACKED MemSessionLargeBufferPool {
    /** @var The node with which the pool is associated. */
    int16_t m_node;

    /** @var Align next member to 8 bytes offset. */
    uint8_t m_padding1[6];

    /** @var The buffer pool serving all buffer lists. */
    void* m_bufferPool;

    /** @var The size in bytes of the buffer pool. */
    uint64_t m_poolSizeBytes;

    /** @var The maximum object size in the pool in bytes. */
    uint64_t m_maxObjectSizeBytes;

    /** @var The number of buffer lists used. */
    uint32_t m_bufferListCount;

    /** @var Align next member to 8 bytes offset. */
    uint8_t m_padding2[4];

    /** @var The buffer lists. */
    MemSessionLargeBufferList** m_bufferLists;
};

/**
 * @brief Initializes the global pool used for large session buffer allocations.
 * @param bufferPool The buffer pool to initialize.
 * @param node The node to which the pool belongs.
 * @param poolSizeBytes The total pool size in bytes.
 * @param maxObjectSizeBytes The maximum allocation size in bytes.
 * @return Zero if succeeded, otherwise an error code.
 */
extern int MemSessionLargeBufferPoolInit(
    MemSessionLargeBufferPool* bufferPool, int16_t node, uint64_t poolSizeBytes, uint64_t maxObjectSizeBytes);

/**
 * @brief Destroys the global pool used for large session buffer allocations.
 * @param bufferPool The buffer pool to destroy.
 */
extern void MemSessionLargeBufferPoolDestroy(MemSessionLargeBufferPool* bufferPool);

/**
 * @brief Allocates a large session buffer from a buffer pool.
 * @param bufferPool The buffer pool.
 * @param sizeBytes The buffer size in bytes.
 * @return The allocated buffer or NULL if failed (last error is set appropriately).
 */
extern void* MemSessionLargeBufferPoolAlloc(
    MemSessionLargeBufferPool* bufferPool, uint64_t sizeBytes, MemSessionLargeBufferHeader** bufferHeaderList);

/**
 * @brief Deallocates a large session buffer into a buffer pool.
 * @param bufferPool The buffer pool.
 * @param buffer The buffer to deallocate.
 */
extern void MemSessionLargeBufferPoolFree(
    MemSessionLargeBufferPool* bufferPool, void* buffer, MemSessionLargeBufferHeader** bufferHeaderList);

/**
 * @brief Reallocates a large session buffer from a buffer pool.
 * @param bufferPool The buffer pool.
 * @param object The object to allocate
 * @param newSizeBytes The new object size in bytes (expected to be larger than current object size).
 * @param flags Reallocation flags.
 * @return The reallocated buffer or NULL if failed (last error set appropriately). In case of
 * failure to allocate a new buffer, the old buffer is left intact.
 * @see MemReallocFlags
 */
extern void* MemSessionLargeBufferPoolRealloc(MemSessionLargeBufferPool* bufferPool, void* object,
    uint64_t newSizeBytes, MemReallocFlags flags, MemSessionLargeBufferHeader** bufferHeaderList);

/**
 * @brief Retrieves the size in bytes of the given large buffer.
 */
extern uint64_t MemSessionLargeBufferPoolGetObjectSize(MemSessionLargeBufferPool* bufferPool, void* buffer);

/** @brief Retrieves session large buffer pool statistics. */
extern void MemSessionLargeBufferPoolGetStats(MemSessionLargeBufferPool* bufferPool, MemSessionLargeBufferStats* stats);

/**
 * @brief Prints a session large buffer pool into log.
 * @param name The name of the session large buffer pool to print.
 * @param logLevel The log level to use in printing.
 * @param bufferPool The session large buffer pool to print.
 * @param[opt] reportMode Specifies the report mode.
 */
extern void MemSessionLargeBufferPoolPrint(const char* name, LogLevel logLevel, MemSessionLargeBufferPool* bufferPool,
    MemReportMode reportMode = MEM_REPORT_SUMMARY);

/**
 * @brief Dumps a session large buffer pool into string buffer.
 * @param indent The indentation level.
 * @param name The name of the session large buffer pool to print.
 * @param bufferPool The session large buffer pool to print.
 * @param stringBuffer The string buffer.
 * @param[opt] reportMode Specifies the report mode.
 */
extern void MemSessionLargeBufferPoolToString(int indent, const char* name, MemSessionLargeBufferPool* bufferPool,
    StringBuffer* stringBuffer, MemReportMode reportMode = MEM_REPORT_SUMMARY);
}  // namespace MOT

/**
 * @brief Dumps all session large buffer pool status to standard error stream.
 */
extern "C" void MemSessionLargeBufferPoolDump(void* pool);

/**
 * @brief Analyzes the memory status of a given buffer address.
 * @param address The buffer to analyze.
 * @return Non-zero value if buffer was found.
 */
extern "C" int MemSessionLargeBufferPoolAnalyze(void* pool, void* buffer);

#endif /* MM_SESSION_LARGE_BUFFER_POOL_H */
