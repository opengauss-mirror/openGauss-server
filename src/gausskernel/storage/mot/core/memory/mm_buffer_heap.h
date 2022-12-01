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
 * mm_buffer_heap.h
 *    A heap for buffers of a specific size class.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/memory/mm_buffer_heap.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef MM_BUFFER_HEAP_H
#define MM_BUFFER_HEAP_H

#include "mm_def.h"
#include "mm_buffer_chunk.h"
#include "mm_lock.h"
#include "mm_raw_chunk_pool.h"
#include "string_buffer.h"

namespace MOT {
/**
 * @struct MemBufferHeap A heap for buffers of a specific size class.
 */
struct MemBufferHeap {
    /** @var The heap lock (noisy, in its own cache line). */
    MemLock m_lock;  // L1 offset [0-64]

    /** @var The NUMA node with which this heap is associated. */
    int16_t m_node;  // L1 offset [0-2]

    /** @var The kind of memory allocation this heap provides (global or local). */
    MemAllocType m_allocType;  // L1 offset [2-4]

    /** @var The size in kilobytes of each buffer in this heap.  */
    uint32_t m_bufferSizeKb;  // L1 offset [4-8]

    /** @var The size of class of buffers managed by this heap. */
    MemBufferClass m_bufferClass;  // L1 offset [8-9]

    /** @var Specifies whether the heap is in reserve mode. */
    uint8_t m_inReserveMode;  // L1 offset [9-10]

    /** @var Pad next member to 4 bytes alignment. */
    uint8_t m_padding2[2];  // L1 offset [10-12]

    /** @var The number of buffers in each chunk in this heap. */
    uint32_t m_maxBuffersInChunk;  // L1 offset [12-16]

    /**
     * @var The actual bit-set array size, calculated by the ratio between chunk size and buffer
     * size in this heap.
     */
    uint64_t m_fullnessBitsetCount;  // L1 offset [16-24]

    // constant members up to here
    /** @var Padding to ensure noisy bit-set array lies in a separate cache line. */
    uint8_t m_padding4[40];  // L1 offset [24-64]

    /**
     * @var A bit set denoting which chunk lists are active in descending fullness level. A value of
     * "1" means the slot contains a list of chunks, while a value of "0" means the slot does not
     * contains a list of chunks. The MSB (bit 63) in each bit-set word corresponds to slot 0, 64,
     * etc. in the corresponding 64 entries in the fullness directory, which contains all chunks with
     * only 1, 65, etc. free buffers. The LSB (bit 0) in each bit-set word corresponds to slot 63, 127,
     * etc. in the corresponding 64 entries in the fullness directory, which contains all chunks with
     * 64, 128, etc. free buffers. Full chunks without free buffers are kept in a separate list.
     *
     * Accommodate for MAX_BUFFERS_PER_CHUNK fullness levels.
     */
    uint64_t m_fullnessBitset[MAX_BITSET_COUNT];  // L1 offset [0-256]=[0-64]

    /**
     * @var An array of lists, each contains chunks with the same amount of allocated buffers. Index
     * 0 contains the fullest list (only one vacant buffer). The array accommodates for at most
     * @ref MAX_BUFFERS_PER_CHUNK fullness levels. In practice only the first @ref _max_buffers_in_chunk
     * slots are in use. Full chunks are kept in a separate list.
     */
    MemBufferChunkHeader* m_fullnessDirectory[MAX_BUFFERS_PER_CHUNK];  // L1 offset [0-16384]=[0-64]

    /** @var List of all full chunks. */
    MemBufferChunkHeader* m_fullChunkList;  // L1 offset [0-8]

    /** @var List of reserved chunks. */
    MemBufferChunkHeader* m_reserveChunkList;  // L1 offset [8-16]

    /** @var The number of chunks currently allocated by this heap. */
    uint64_t m_allocatedChunkCount;  // L1 offset [16-24]

    /** @var The number of buffers currently allocated by this heap. */
    uint64_t m_allocatedBufferCount;  // L1 offset [24-32]

    /** @var Padding to round off the size to L1 cache line aligned size. */
    uint8_t m_padding5[32];  // L1 offset [32-64]
};

static_assert(sizeof(MemBufferHeap) == L1_ALIGNED_SIZEOF(MemBufferHeap),
    "sizeof(MemBufferHeap) must be 64 byte (L1 cache line size) aligned");

/**
 * @brief Initializes a buffer heap.
 * @param bufferHeap The buffer heap to initialize.
 * @param node The node with which this buffer heap is associated. The pseudo-node @ref MM_INTERLEAVE_NODE can be used
 * here.
 * @param bufferClass The size class of buffers managed by this buffer heap.
 * @param allocType The kind of allocations this allocator provides (global or local).
 * @return Zero if succeeded, otherwise an error code.
 */
extern int MemBufferHeapInit(MemBufferHeap* bufferHeap, int node, MemBufferClass bufferClass, MemAllocType allocType);

/**
 * @brief Releases all resources associated with a buffer heap.
 * @param bufferHeap The buffer heap to destroy.
 */
extern void MemBufferHeapDestroy(MemBufferHeap* bufferHeap);

/**
 * @brief Allocates a single buffer from the heap.
 * @param bufferHeap The buffer heap.
 * @return The allocated buffer or NULL if failed. Call @fn mm_get_last_error() to find out the
 * reason for failure.
 */
extern MemBufferHeader* MemBufferHeapAlloc(MemBufferHeap* bufferHeap);

/**
 * @brief De-allocates a buffer previously allocated through this heap.
 * @param bufferHeap The buffer heap.
 * @param bufferHeader The header of the buffer to de-allocate.
 */
extern void MemBufferHeapFree(MemBufferHeap* bufferHeap, MemBufferHeader* bufferHeader);

/**
 * @brief Allocates multiple buffer from the heap.
 * @param bufferHeap The buffer heap.
 * @param itemCount The number of buffers to allocate.
 * @param [out] bufferList Receives the allocated buffers.
 * @return The number of allocated buffers or zero if failed.
 */
extern uint32_t MemBufferHeapAllocMultiple(MemBufferHeap* bufferHeap, uint32_t itemCount, MemBufferList* bufferList);

/**
 * @brief Extract the fullest chunk from the heap
 * @param bufferHeap The buffer heap.
 * @return The extracted chunk or NULL if failed.
 */
extern MemBufferChunkHeader* MemBufferHeapExtractChunk(MemBufferHeap* bufferHeap);

/**
 * @brief Re-links a chunk back into the heap (after allocating some buffers from the chunk).
 * @param bufferHeap The buffer heap.
 * @param chunkHeader The chunk to re-link.
 * @param buffersAllocated The number of buffers allocated from the chunk (for statistics).
 * @param newChunk Specifies whether this is a new chunk (for statistics).
 */
extern void MemBufferHeapRelinkChunk(
    MemBufferHeap* bufferHeap, MemBufferChunkHeader* chunkHeader, uint32_t buffersAllocated, int newChunk);

/**
 * @brief Takes a snapshot of the emptiest chunk (for cache) and marks it as fully used.
 * @param bufferHeap The buffer heap.
 * @param[out] chunkSnapshot Receives the chunk snapshot.
 * @return The real chunk or NULL if the heap is empty.
 */
extern MemBufferChunkHeader* MemBufferHeapSnapshotChunk(MemBufferHeap* bufferHeap, MemBufferChunkHeader* chunkSnapshot);

/**
 * @brief De-allocates a buffer previously allocated through this heap.
 * @param bufferHeap The buffer heap.
 * @param bufferList The list of the buffers to free.
 */
extern void MemBufferHeapFreeMultiple(MemBufferHeap* bufferHeap, MemBufferList* bufferList);

/**
 * @brief Reserve memory for current session. While in reserve-mode, released chunks are kept in the current session's
 * reserve, rather than being released to global memory.
 * @param bufferHeap The buffer heap.
 * @param bufferCount The number of buffers to reserve.
 * @return Zero on success, otherwise error code on failure.
 */
extern int MemBufferHeapReserve(MemBufferHeap* bufferHeap, uint32_t bufferCount);

/**
 * @brief Release all global memory reserved for current session for a specific buffer class.
 * @param bufferHeap The buffer heap.
 * @param bufferClass The buffer class for which an existing reservation is to be released.
 * @return Zero on success, otherwise error code on failure.
 */
extern int MemBufferHeapUnreserve(MemBufferHeap* bufferHeap);

/**
 * @brief Prints a buffer heap into log.
 * @param name The name of the buffer heap to print.
 * @param logLevel The log level to use in printing.
 * @param bufferHeap The buffer heap to print.
 */
extern void MemBufferHeapPrint(const char* name, LogLevel logLevel, MemBufferHeap* bufferHeap);

/**
 * @brief Dumps a buffer heap into string buffer.
 * @param indent The indentation level.
 * @param name The name of the buffer heap to print.
 * @param bufferHeap The buffer heap to print.
 * @param stringBuffer The string buffer.
 */
extern void MemBufferHeapToString(int indent, const char* name, MemBufferHeap* bufferHeap, StringBuffer* stringBuffer);

}  // namespace MOT
/**
 * @brief Dumps a buffer heap into standard error stream.
 * @param arg The buffer heap to print.
 */
extern "C" void MemBufferHeapDump(void* arg);

#endif /* MM_BUFFER_HEAP_H */
