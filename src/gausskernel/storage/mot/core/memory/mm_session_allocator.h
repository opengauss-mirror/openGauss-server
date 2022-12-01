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
 * mm_session_allocator.h
 *    Session-local memory allocator, which provides session-local objects that can be used only in session context.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/memory/mm_session_allocator.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef MM_SESSION_ALLOCATOR_H
#define MM_SESSION_ALLOCATOR_H

#include "mm_def.h"
#include "utilities.h"
#include "string_buffer.h"
#include "mm_chunk_type.h"
#include "session_context.h"
#include "mm_huge_object_allocator.h"
#include "mm_session_large_buffer_store.h"

#define MEM_SESSION_CHUNK_FREE_LIST_LEN 15  // from 2^6 bytes to 1022*1024 bytes
#define MEM_SESSION_MAX_ALLOC_SIZE 1046528  // 1022*1024
#define MEM_SESSION_MIN_ALLOC_SIZE 64       // 64 bytes
#define MEM_SESSION_CHUNK_BASE 6            // from 64 bytes which is 2^6

namespace MOT {

/** @typedef Chunk header of chunks used for session-local allocations. */
struct PACKED MemSessionChunkHeader {
    /** @var Chunk type (must be first member to comply with raw chunk). */
    MemChunkType m_chunkType;  // offset [0-4]

    /** @var The node with which the chunk is associated (must be second member to comply with raw chunk). */
    int16_t m_node;  // offset [4-6]

    /** @var A flag specifying whether the chunk data is node-local or global (must be third member to comply with raw
     * chunk). */
    MemAllocType m_allocType;  // offset [6-8]

    /** @var The allocator node identifier with which the session chunk is associated (required for free buffer). */
    int16_t m_allocatorNode;  // offset [8-10]

    /** @var Align next member to 8 byte. */
    int16_t m_padding1[3];  // offset [10-16]

    /** @var The start address of the free space in the chunk */
    char* m_freePtr;  // offset [16-24]

    /** @var The end address of the free space in the chunk. */
    char* m_endPtr;  // offset [24-32]

    /** @var The free size (number of bytes not allocated yet). */
    uint64_t m_freeSize;  // offset [32-40]

    /** @var The next session chunk. */
    MemSessionChunkHeader* m_next;  // offset [40-48]

    /** @var Align struct size to L1 cache line size. */
    int8_t m_padding2[16];  // offset [48-64]
};

/**
 * @brief Initializes a session chunk header.
 * @param chunk The chunk header to initialize.
 * @param chunkType The type of the chunk to use (can be session or global small object).
 * @param allocatorNode The NUMA node identifier with which the chunk is associated.
 * @param allocType The type of the allocation to use (can be session local or global object).
 */
extern void MemSessionChunkInit(
    MemSessionChunkHeader* chunk, MemChunkType chunkType, int allocatorNode, MemAllocType allocType);

/** @struct MemSessionObjectSize Object size helper struct. */
struct PACKED MemSessionObjectSize {
    // both sizes do NOT include the header size itself
    /** @var The size of the object (as requested by the user). */
    uint32_t m_size;

    /** @var The real size of the object (as given by the allocator). */
    uint32_t m_realSize;
};

/** @struct MemSessionObjectHeader Header of object allocated in a session allocator. */
struct PACKED MemSessionObjectHeader {
    /** @var The object size in bytes (as requested by user). Active when object is in use. */
    MemSessionObjectSize m_objectSize;

    /**
     * @var A pointer to the next object in the free object list when object is free or the next object in the
     * @ref MemSessionAllocator::m_sessionObjectList when object is allocated.
     */
    MemSessionObjectHeader* m_next;

    /** @var A pointer to the previous object in the _session_object_list when object is allocated. */
    MemSessionObjectHeader* m_prev;

    /** @var The session identifier. */
    SessionId m_sessionId;

    // although this member increases overhead per-object, it actually helps alignment so it is not totally
    // counter-productive
    /** @var a place-holder for designating the real header is in offset due to aligned allocation. */
    uint32_t m_alignedOffset;
};

/** @define Utility helper macro for real size of session object header. */
#define MEM_SESSION_OBJECT_HEADER_LEN sizeof(MOT::MemSessionObjectHeader)

struct PACKED MemSessionAllocator {
    /** @var the session id with which the allocator is associated. */
    SessionId m_sessionId;  // L1 offset [0-4]

    /** @var the connection id with which the allocator is associated. */
    ConnectionId m_connectionId;  // L1 offset [4-8]

    /** @var The thread id with which the allocator is associated. */
    MOTThreadId m_threadId;  // L1 offset [8-10]

    /** @var The NUMA node if with which the allocator is associated. */
    uint16_t m_nodeId;  // L1 offset [10-12]

    /** @var Specifies whether the session allocator uses the global chunk pool or local chunk pool. */
    MemAllocType m_allocType;  // L1 offset [12-14]

    /** @var Aligns next member to 8-byte. */
    uint8_t m_padding1[2];  // L1 offset [14-16]

    /** @var The list of chunks currently in use by this session allocator. */
    MemSessionChunkHeader* m_chunkList;  // L1 offset [16-24]

    /** @var the length of the _chunk_list. */
    uint32_t m_chunkCount;  // L1 offset [24-28]

    /** @var The maximum number of bytes reserved for the session. */
    uint32_t m_maxChunkCount;  // L1 offset [28-32]

    /** @var Number of bytes used by application (not necessarily same as @ref _real_used_size). */
    uint64_t m_usedSize;  // L1 offset [32-40]

    /** @var Number of bytes really given to the application (usually more than it asked for). */
    uint64_t m_realUsedSize;  // L1 offset [40-48]

    /** @var The maximum seen this far of @ref _used_size. */
    uint64_t m_maxUsedSize;  // L1 offset [48-56]

    /** @var The maximum seen this far of _real_used_size. */
    uint64_t m_maxRealUsedSize;  // L1 offset [56-64]

    /** @var Record all session objects being used. */
    MemSessionObjectHeader* m_sessionObjectList;  // L1 offset [0-8]

    /** @var Record all session large buffers being used. */
    MemSessionLargeBufferHeader* m_largeBufferList;  // L1 offset [8-16]

    /** @var Record all session huge chunks being used. */
    MemVirtualHugeChunkHeader* m_hugeChunkList;  // L1 offset [16-24]

    /** @var Array of free object lists in varying object sizes. */
    MemSessionObjectHeader* m_freeList[MEM_SESSION_CHUNK_FREE_LIST_LEN];  // L1 offset [24-144]=[24-16]

    /** @var Padding to round off the size to L1 cache line aligned size. */
    uint8_t m_padding2[48];  // L1 offset [16-64]
};

static_assert(sizeof(MemSessionAllocator) == L1_ALIGNED_SIZEOF(MemSessionAllocator),
    "sizeof(MemSessionAllocator) must be 64 byte (L1 cache line size) aligned");

/**
 * @typedef mm_session_allocator_stats Session allocator statistics.
 */
struct PACKED MemSessionAllocatorStats {
    /** @var The session identifier for which statistics are reported. */
    SessionId m_sessionId;  // L1 offset [0-4]

    /** @var The connection identifier for which statistics are reported. */
    ConnectionId m_connectionId;  // L1 offset [4-8]

    /** @var The thread id with which the allocator is associated. */
    MOTThreadId m_threadId;  // L1 offset [8-10]

    /** @var The NUMA node if with which the allocator is associated. */
    int16_t m_nodeId;  // L1 offset [10-12]

    /** @var Align next member to 8 byte offset. */
    uint8_t m_padding[4];  // L1 offset [12-16]

    /** @var The number of bytes a session can use in total. */
    uint64_t m_reservedSize;  // L1 offset [16-24]

    /** @var The number of bytes a session really used. */
    uint64_t m_usedSize;  // L1 offset [24-32]

    /** @var The number of bytes a session was actually given by the allocator (usually more than it requested). */
    uint64_t m_realUsedSize;  // L1 offset [32-40]

    /** @var The maximum number of bytes reserved for the session. */
    uint64_t m_maxReservedSize;  // L1 offset [40-48]

    /** @var The maximum number of bytes the session used. */
    uint64_t m_maxUsedSize;  // L1 offset [48-56]

    /** @var The maximum number of bytes the session was given by the allocator. */
    uint64_t m_maxRealUsedSize;  // L1 offset [56-64]
};

/**
 * @brief Initializes a session allocator.
 * @param sessionAllocator The session allocator to initialize.
 * @param sessionId The session identifier of the session.
 * @param connectionId The connection identifier of the session.
 * @param chunkList The initial list of chunks used for allocating objects.
 * @param chunkCount The number of chunks in the list.
 * @param allocType Specifies whether the session allocator uses the global chunk pool or local
 * chunk pool.
 */
extern void MemSessionAllocatorInit(MemSessionAllocator* sessionAllocator, SessionId sessionId,
    ConnectionId connectionId, MemSessionChunkHeader* chunkList, uint32_t chunkCount, MemAllocType allocType);

/**
 * @brief Destroys a session allocator.
 * @param sessionAllocator The session allocator to destroy.
 */
extern void MemSessionAllocatorDestroy(MemSessionAllocator* sessionAllocator);

/**
 * @brief Allocates an object from a session allocator. Pay attention that the session allocator
 * is intended for a single-threaded use.
 * @param sessionAllocator The session allocator.
 * @param size The size in bytes of the object to allocate. This size cannot exceed @ref MAX_ALLOC_SIZE.
 * @return The allocate object or NULL if failed. Call @ref mm_get_last_error() to find out the
 * failure reason.
 */
extern void* MemSessionAllocatorAlloc(MemSessionAllocator* sessionAllocator, uint32_t size);

/**
 * @brief Allocates an aligned object from a session allocator. Pay attention that the session
 * allocator is intended for a single-threaded use.
 * @param sessionAllocator The session allocator.
 * @param size The size in bytes of the object to allocate. This size cannot exceed @ref MAX_ALLOC_SIZE.
 * @param alignment The requested alignment in bytes.
 * @return The allocate object or NULL if failed. Call @ref mm_get_last_error() to find out the
 * failure reason.
 */
extern void* MemSessionAllocatorAllocAligned(MemSessionAllocator* sessionAllocator, uint32_t size, uint32_t alignment);

/**
 * @brief Deallocates an object into a session allocator. The object must have been previously
 * allocated by the current thread through a call to @ref mm_session_allocator_alloc().
 * @param sessionAllocator The session allocator.
 * @param object A pointer to the object to free.
 */
extern void MemSessionAllocatorFree(MemSessionAllocator* sessionAllocator, void* object);

/**
 * @brief Reallocates a memory region previously allocated by the current thread through a call to
 * @ref mm_session_allocator_alloc() or @ref mm_session_allocator_realloc().
 * @param sessionAllocator The session allocator.
 * @param object The existing object to reallocate.
 * @param newSize The new object size (can be smaller than original size).
 * @param flags One of the reallocation flags @ref MM_REALLOC_COPY or @ref MM_REALLOC_ZERO.
 * @return The reallocated object or NULL if failed. Call @ref mm_get_last_error() to find out the
 * failure reason.
 */
extern void* MemSessionAllocatorRealloc(
    MemSessionAllocator* sessionAllocator, void* object, uint32_t newSize, MemReallocFlags flags);

/**
 * @brief release all memory at the end of a transaction to avoid memory leaks.
 * @param sessionAllocator The session allocator.
 */
extern void MemSessionAllocatorCleanup(MemSessionAllocator* sessionAllocator);

/**
 * @brief Queries for the size of an object allocated by a session allocator.
 * @param object The object to query.
 * @param[opt,out] The size of the object as requested during allocation.
 * @return The real size of the object as used by the session allocator. The real size is at least
 * as large as the requested size.
 */
extern uint32_t MemSessionAllocatorGetObjectSize(void* object, uint32_t* requestedSize = NULL);

/**
 * @brief Retrieves current allocator statistics.
 * @param sessionAllocator The session allocator.
 * @param stats The session allocator statistics.
 */
extern void MemSessionAllocatorGetStats(MemSessionAllocator* sessionAllocator, MemSessionAllocatorStats* stats);

/**
 * @brief Formats statistics report of a session allocator into a string buffer.
 * @param indent The indentation level.
 * @param name The name of the session allocator to print.
 * @param stringBuffer The string buffer.
 * @param stats The session allocator statistics.
 */
extern void MemSessionAllocatorFormatStats(int indent, const char* name, StringBuffer* stringBuffer,
    MemSessionAllocatorStats* stats, MemReportMode reportMode = MEM_REPORT_SUMMARY);

/**
 * @brief Prints session allocator statistics to log.
 * @param name The name of the allocator to print.
 * @param logLevel The log level to use in printing.
 * @param stats The session allocator statistics.
 */
extern void MemSessionAllocatorPrintStats(const char* name, LogLevel logLevel, MemSessionAllocatorStats* stats,
    MemReportMode reportMode = MEM_REPORT_SUMMARY);

/**
 * @brief Prints up-to-date session allocator statistics to log.
 * @param name The name of the allocator to print.
 * @param logLevel The log level to use in printing.
 * @param sessionAllocator The session allocator.
 */
extern void MemSessionAllocatorPrintCurrentStats(const char* name, LogLevel logLevel,
    MemSessionAllocator* sessionAllocator, MemReportMode reportMode = MEM_REPORT_SUMMARY);

/**
 * @brief Prints a session allocator into log.
 * @param name The name of the session allocator to print.
 * @param logLevel The log level to use in printing.
 * @param sessionAllocator The session allocator to print.
 * @param[opt] reportMode Specifies the report mode.
 */
extern void MemSessionAllocatorPrint(const char* name, LogLevel logLevel, MemSessionAllocator* sessionAllocator,
    MemReportMode reportMode = MEM_REPORT_SUMMARY);

/**
 * @brief Dumps a session allocator into string buffer.
 * @param indent The indentation level.
 * @param name The name of the session allocator to print.
 * @param sessionAllocator The session allocator to print.
 * @param stringBuffer The string buffer.
 * @param[opt] reportMode Specifies the report mode.
 */
extern void MemSessionAllocatorToString(int indent, const char* name, MemSessionAllocator* sessionAllocator,
    StringBuffer* stringBuffer, MemReportMode reportMode = MEM_REPORT_SUMMARY);

}  // namespace MOT

/**
 * @brief Dumps a session allocator into standard error stream.
 * @param arg The session allocator to print.
 */
extern "C" void MemSessionAllocatorDump(void* arg);

/**
 * @brief Analyzes the buffer status in a session allocator.
 * @param allocator The allocator.
 * @param buffer The buffer to analyze.
 * @return Non-zero value if the buffer was found.
 */
extern "C" int MemSessionAllocatorAnalyze(void* allocator, void* buffer);

#endif /* MM_SESSION_ALLOCATOR_H */
