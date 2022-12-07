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
 * mm_buffer_allocator.h
 *    Memory buffer allocator implementation.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/memory/mm_buffer_allocator.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef MM_BUFFER_ALLOCATOR_H
#define MM_BUFFER_ALLOCATOR_H

#include "mm_raw_chunk_pool.h"
#include "mm_buffer_heap.h"
#include "string_buffer.h"
#include "mm_buffer_list.h"
#include "mm_buffer_chunk.h"

/**
 * @define The default size limit for the free list of the buffer allocator, above which list items are drained to the
 * heap.
 */
#define MEM_DEFAULT_FREE_LIST_SIZE 8

namespace MOT {
/**
 * @typedef MemBufferChunkSnapshot A buffer chunk snapshot with pointer to real chunk.
 */
struct PACKED MemBufferChunkSnapshot {
    /** @var The chunk header snapshot taken during cache refill. */
    MemBufferChunkHeader m_chunkHeaderSnapshot;

    /** @var The real chunk header. */
    MemBufferChunkHeader* m_realChunkHeader;

    /** @var The current bit-set index to the bit-set array in the header snapshot. */
    uint32_t m_bitsetIndex;

    /** @var Align next member to 8 bytes. */
    uint32_t m_padding;

    /** @var Specifies whether this is a new chunk snapshot. */
    uint32_t m_newChunk;

    /** @var Padding to round off the size to L1 cache line aligned size. */
    uint8_t m_padding1[44];
};

static_assert(sizeof(MemBufferChunkSnapshot) == L1_ALIGNED_SIZEOF(MemBufferChunkSnapshot),
    "sizeof(MemBufferChunkSnapshot) must be 64 byte (L1 cache line size) aligned");

/**
 * @brief Allocated a buffer from a partial chunk snapshot.
 * @param chunkSnapshot The chunk snapshot.
 * @return The allocated buffer or NULL if snapshot is empty.
 */
extern MemBufferHeader* MemBufferChunkSnapshotAllocBuffer(MemBufferChunkSnapshot* chunkSnapshot);

/**
 * @brief Allocated a buffer from a full chunk snapshot.
 * @param chunkSnapshot The chunk snapshot.
 * @return The allocated buffer or NULL if snapshot is empty.
 */
inline MemBufferHeader* MemBufferChunkSnapshotAllocBufferInline(MemBufferChunkSnapshot* chunkSnapshot)
{
    if (chunkSnapshot->m_newChunk) {
        MemBufferChunkHeader* chunkHeader = &chunkSnapshot->m_chunkHeaderSnapshot;
        MemBufferHeader* bufferHeader =
            MM_CHUNK_BUFFER_HEADER_AT(chunkSnapshot->m_realChunkHeader, chunkHeader->m_allocatedCount++);
        if (chunkHeader->m_allocatedCount == chunkHeader->m_bufferCount) {
            // discard chunk
            chunkSnapshot->m_realChunkHeader = NULL;
        }
        return bufferHeader;
    } else {
        return MemBufferChunkSnapshotAllocBuffer(chunkSnapshot);
    }
}

/**
 * @brief Allocates a buffer from a chunk snapshot.
 * @param chunkSnapshot The chunk snapshot.
 * @return The allocated buffer or NULL if snapshot is empty.
 */
inline void* MemChunkSnapshotAlloc(MemBufferChunkSnapshot* chunkSnapshot)
{
    void* buffer = nullptr;
    if (chunkSnapshot->m_realChunkHeader != NULL) {
        MemBufferHeader* bufferHeader = MemBufferChunkSnapshotAllocBufferInline(chunkSnapshot);
        MOT_ASSERT(bufferHeader != NULL);
        buffer = bufferHeader->m_buffer;
    }
    return buffer;
}

/**
 * @typedef MemBufferAllocator A buffer allocator for single buffer size class on a specific
 * node. The allocator management data is allocated in-place.
 */
struct PACKED MemBufferAllocator {
    /** @var The size in bytes of the memory allocated to manage this allocator. */
    uint64_t m_allocSize;

    /** @var The buffer heap. */
    MemBufferHeap* m_bufferHeap;

    /** @var Per-thread chunk snapshot acting as a fast cache for allocation intensive workloads. */
    MemBufferChunkSnapshot* m_chunkSnapshots;

    /** @var List of free buffers waiting to be put back in the heap. */
    MemBufferList* m_freeListArray;

    /** @var Size limit for the free list, above which free list items are drained to the heap. */
    uint64_t m_freeListDrainSize;
};

/**
 * @typedef MemBufferAllocatorStats Buffer allocator statistics.
 */
struct MemBufferAllocatorStats {
    /** @var The number of buffers available in the chunk snapshot cache. */
    uint64_t m_cachedBuffers;

    /** @var The number of free buffers found in the free list. */
    uint64_t m_freeBuffers;

    /** @var The number of chunks stored in the heap. */
    uint64_t m_heapChunks;

    /** @var The number of buffers available in the heap. */
    uint64_t m_heapFreeBuffers;

    /** @var The number of buffers allocated from the heap. */
    uint64_t m_heapUsedBuffers;
};

/**
 * @brief Helper function for printing error messages in inline functions.
 * @param errorCode The error code to report
 * @param context The error context.
 * @param format The error message to print.
 * @param ... Additional parameters required for the format message.
 */
extern void MemBufferAllocatorIssueError(int errorCode, const char* context, const char* format, ...);

/**
 * @brief Initializes a buffer allocator according to configuration.
 * @param bufferAllocator The buffer allocator to initialize.
 * @param node The node with which the allocator is associated.
 * @param bufferClass The size class of buffers managed by the allocator.
 * @param allocType The kind of allocations this allocator provides (global or local).
 * @param freeListDrainSize Size limit for the free list, above which free list items are drained to the heap.
 * @return Zero if succeeded, otherwise an error code.
 */
extern int MemBufferAllocatorInit(MemBufferAllocator* bufferAllocator, int node, MemBufferClass bufferClass,
    MemAllocType allocType, int freeListDrainSize = MEM_DEFAULT_FREE_LIST_SIZE);

/**
 * @brief Initializes a buffer allocator according to configuration.
 * @param bufferAllocator The buffer allocator to initialize.
 */
extern void MemBufferAllocatorDestroy(MemBufferAllocator* bufferAllocator);

/**
 * @brief Refills the chunk snapshot cache for the current thread.
 * @param bufferAllocator The buffer allocator.
 * @param chunkSnapshot The chunk snapshot.
 * @return A buffer from the refilled cache.
 */
extern void* MemBufferAllocatorRefillCache(MemBufferAllocator* bufferAllocator, MemBufferChunkSnapshot* chunkSnapshot);

/**
 * @brief Allocates a buffer from the buffer allocator.
 * @param bufferAllocator The buffer allocator.
 * @return The allocated buffer or NULL if failed.
 */
inline void* MemBufferAllocatorAlloc(MemBufferAllocator* bufferAllocator)
{
    void* buffer = nullptr;
    MOTThreadId threadId = MOTCurrThreadId;
    if (unlikely(threadId == INVALID_THREAD_ID)) {
        MemBufferAllocatorIssueError(MOT_ERROR_INTERNAL,
            "Allocate Memory",
            "Invalid attempt to allocate a buffer without current thread identifier");
    } else {
        MemBufferChunkSnapshot* chunkSnapshot = &bufferAllocator->m_chunkSnapshots[threadId];
        buffer = MemChunkSnapshotAlloc(chunkSnapshot);
        if (buffer == nullptr) {
            buffer = MemBufferAllocatorRefillCache(bufferAllocator, chunkSnapshot);
        }
    }

    return buffer;
}

/**
 * @brief Deallocates a buffer into the buffer allocator.
 * @param bufferAllocator The buffer allocator.
 * @param buffer The buffer to deallocate.
 */
extern void MemBufferAllocatorFree(MemBufferAllocator* bufferAllocator, void* buffer);

/**
 * @brief Reserve memory for current session. While in reserve-mode, released chunks are kept in the current session's
 * reserve, rather than being released to global memory.
 * @param bufferAllocator The buffer allocator.
 * @param chunkCount The number of chunks to reserve.
 * @return Zero on success, otherwise error code on failure.
 */
extern int MemBufferAllocatorReserve(MemBufferAllocator* bufferAllocator, uint32_t chunkCount);

/**
 * @brief Release all global memory reserved for current session for a specific buffer class.
 * @param bufferAllocator The buffer allocator.
 * @param bufferClass The buffer class for which an existing reservation is to be released.
 * @return Zero on success, otherwise error code on failure.
 */
extern int MemBufferAllocatorUnreserve(MemBufferAllocator* bufferAllocator);

/**
 * @brief Utility function for guarding against double free.
 * @param buffer The buffer to check.
 * @return The origin buffer header.
 * @note abort() is called if this is a double free.
 */
extern MemBufferHeader* MemBufferCheckDoubleFree(void* buffer);

/**
 * @brief Clears the thread cache for the current session on this allocator.
 * @param bufferAllocator The buffer allocator.
 */
extern void MemBufferAllocatorClearThreadCache(MemBufferAllocator* bufferAllocator);

/**
 * @brief Retrieves current allocator statistics.
 * @param bufferAllocator The buffer allocator.
 * @param stats The buffer allocator statistics.
 */
extern void MemBufferAllocatorGetStats(MemBufferAllocator* bufferAllocator, MemBufferAllocatorStats* stats);

/**
 * @brief Formats statistics report of a buffer allocator into a string buffer.
 * @param indent The indentation level.
 * @param name The name of the session allocator to print.
 * @param stringBuffer The string buffer.
 * @param bufferAllocator The buffer allocator.
 * @param stats The buffer allocator statistics.
 */
extern void MemBufferAllocatorFormatStats(int indent, const char* name, StringBuffer* stringBuffer,
    MemBufferAllocator* bufferAllocator, MemBufferAllocatorStats* stats);

/**
 * @brief Prints buffer allocator statistics to log.
 * @param name The name of the allocator to print.
 * @param logLevel The log level to use in printing.
 * @param bufferAllocator The buffer allocator.
 * @param stats The buffer allocator statistics.
 */
extern void MemBufferAllocatorPrintStats(
    const char* name, LogLevel logLevel, MemBufferAllocator* bufferAllocator, MemBufferAllocatorStats* stats);

/**
 * @brief Prints up-to-date buffer allocator statistics to log.
 * @param name The name of the allocator to print.
 * @param logLevel The log level to use in printing.
 * @param bufferAllocator The buffer allocator.
 */
extern void MemBufferAllocatorPrintCurrentStats(
    const char* name, LogLevel logLevel, MemBufferAllocator* bufferAllocator);

/**
 * @brief Prints a buffer allocator into log.
 * @param name The name of the buffer allocator to print.
 * @param logLevel The log level to use in printing.
 * @param bufferAllocator The buffer allocator to print.
 * @param[opt] reportMode Specifies the report mode.
 */
extern void MemBufferAllocatorPrint(const char* name, LogLevel logLevel, MemBufferAllocator* bufferAllocator,
    MemReportMode reportMode = MEM_REPORT_SUMMARY);

/**
 * @brief Dumps a buffer allocator into string buffer.
 * @param indent The indentation level.
 * @param name The name of the buffer allocator to print.
 * @param bufferAllocator The buffer allocator to print.
 * @param stringBuffer The string buffer.
 * @param[opt] reportMode Specifies the report mode.
 */
extern void MemBufferAllocatorToString(int indent, const char* name, MemBufferAllocator* bufferAllocator,
    StringBuffer* stringBuffer, MemReportMode reportMode = MEM_REPORT_SUMMARY);
}  // namespace MOT

/**
 * @brief Dumps a buffer allocator into standard error stream.
 * @param arg The buffer allocator to print.
 */
extern "C" void MemBufferAllocatorDump(void* arg);

/**
 * @brief Analyzes the buffer status in its allocator.
 * @param allocator The allocator.
 * @param buffer The buffer to analyze.
 * @return Non-zero value if the buffer was found.
 */
extern "C" int MemBufferAllocatorAnalyze(void* allocator, void* buffer);

#endif /* MM_BUFFER_ALLOCATOR_H */
