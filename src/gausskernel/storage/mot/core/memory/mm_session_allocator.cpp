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
 * mm_session_allocator.cpp
 *    Session-local memory allocator, which provides session-local objects that can be used only in session context.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/memory/mm_session_allocator.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <string.h>

#include "mm_session_allocator.h"
#include "session_context.h"
#include "mm_raw_chunk_store.h"
#include "mot_error.h"
#include "utilities.h"
#include "mm_api.h"
#include "mm_buffer_class.h"
#include "memory_statistics.h"
#include "mot_engine.h"

namespace MOT {

DECLARE_LOGGER(SessionAllocator, Memory)

#define MEM_SESSION_CHUNK_HEADER_LEN sizeof(MemSessionChunkHeader)

// helpers
inline void MemSessionObjectInit(
    SessionId sessionId, MemSessionObjectHeader* objectHeader, uint32_t size, uint32_t realSize);
static uint32_t RealAllocSize(uint32_t size);
static uint32_t GetFreeListIndex(uint32_t size);
static void* AllocFromFreeList(MemSessionAllocator* sessionAllocator, uint32_t size, uint32_t realSize);
static void* AllocFromExistingChunk(MemSessionAllocator* sessionAllocator, uint32_t size, uint32_t realSize);
static void* GetBlockFromChunk(MemSessionAllocator* sessionAllocator, MemSessionChunkHeader* chunk, uint32_t size,
    uint32_t realSize, bool printError = false);
static void* AllocFromNewChunk(MemSessionAllocator* sessionAllocator, uint32_t size, uint32_t realSize);
static void MemSessionAllocatorRecordSessionObject(
    MemSessionAllocator* sessionAllocator, MemSessionObjectHeader* objectHeader);
static void MemSessionAllocatorUnrecordSessionObject(
    MemSessionAllocator* sessionAllocator, MemSessionObjectHeader* objectHeader);

inline void MemSessionAllocatorAssertValid(MemSessionAllocator* sessionAllocator)
{
    MOT_ASSERT(sessionAllocator != NULL);
    if (sessionAllocator->m_allocType == MEM_ALLOC_LOCAL) {
        MOT_ASSERT(MOT_GET_CURRENT_SESSION_ID() != INVALID_SESSION_ID);
        MOT_ASSERT(MOT_GET_CURRENT_CONNECTION_ID() != INVALID_CONNECTION_ID);
        MOT_ASSERT(MOTCurrThreadId != INVALID_THREAD_ID);
        MOT_ASSERT(MOTCurrentNumaNodeId != MEM_INVALID_NODE);
        MOT_ASSERT(sessionAllocator->m_sessionId == MOT_GET_CURRENT_SESSION_ID());
        MOT_ASSERT(sessionAllocator->m_connectionId == MOT_GET_CURRENT_CONNECTION_ID());
    }
}

#ifdef MOT_DEBUG
#define ASSERT_SESSION_ALLOCATOR_VALID(sessionAllocator) MemSessionAllocatorAssertValid(sessionAllocator)
#else
#define ASSERT_SESSION_ALLOCATOR_VALID(sessionAllocator)
#endif

extern void MemSessionAllocatorInit(MemSessionAllocator* sessionAllocator, SessionId sessionId,
    ConnectionId connectionId, MemSessionChunkHeader* chunkList, uint32_t chunkCount, MemAllocType allocType)
{
    // better be safe than sorry, make sure that all members are initializes properly, rather than
    // relying on a previous successful cleanup during a call to MemSessionAllocatorDestroy()
    MOT_ASSERT((allocType == MEM_ALLOC_GLOBAL) || MOT_GET_CURRENT_SESSION_ID() != INVALID_SESSION_ID);
    MOT_ASSERT((allocType == MEM_ALLOC_GLOBAL) || MOTCurrThreadId != INVALID_THREAD_ID);
    MOT_ASSERT((allocType == MEM_ALLOC_GLOBAL) || MOTCurrentNumaNodeId != MEM_INVALID_NODE);

    errno_t erc = memset_s(sessionAllocator, sizeof(MemSessionAllocator), 0, sizeof(MemSessionAllocator));
    securec_check(erc, "\0", "\0");
    sessionAllocator->m_sessionId = sessionId;
    sessionAllocator->m_connectionId = connectionId;
    sessionAllocator->m_threadId = MOTCurrThreadId;
    sessionAllocator->m_nodeId = MOTCurrentNumaNodeId;
    sessionAllocator->m_chunkList = chunkList;
    sessionAllocator->m_chunkCount = chunkCount;
    sessionAllocator->m_allocType = allocType;
}

extern void MemSessionAllocatorDestroy(MemSessionAllocator* sessionAllocator)
{
    ASSERT_SESSION_ALLOCATOR_VALID(sessionAllocator);

    // 1. Return all chunks into the raw chunk pool of the local numa node.
    while (sessionAllocator->m_chunkList != NULL) {
        MemSessionChunkHeader* chunk = sessionAllocator->m_chunkList;
        sessionAllocator->m_chunkList = chunk->m_next;
        // chunk can come from a node different than the node of the session allocator, both in local and global
        // allocations, so we use the node specified in the chunk, and not the node specified in the allocator
        if (sessionAllocator->m_allocType == MEM_ALLOC_LOCAL) {
            MemRawChunkStoreFreeLocal((void*)chunk, chunk->m_node);
        } else {
            MemRawChunkStoreFreeGlobal((void*)chunk, chunk->m_node);
        }
    }
    sessionAllocator->m_chunkList = NULL;

    // 2. Return all large buffers into the reserved large buffer pool of the local numa node.
    if (sessionAllocator->m_largeBufferList != NULL) {
        MOT_LOG_WARN(
            "Session %u: There are still some large buffers need to be released.", sessionAllocator->m_sessionId);
        MemSessionLargeBufferHeader* bufferHeader = sessionAllocator->m_largeBufferList;
        while (bufferHeader != NULL) {
            void* buffer = bufferHeader->m_buffer;
            MOT_LOG_TRACE("Session %u: Free session large buffer %p in size of %u bytes",
                sessionAllocator->m_sessionId,
                buffer,
                bufferHeader->m_realObjectSize);
            bufferHeader = bufferHeader->m_next;
            MemSessionLargeBufferFree(buffer, &(sessionAllocator->m_largeBufferList));
        }
        sessionAllocator->m_largeBufferList = NULL;
    }

    // 3. Deallocate all huge chunks
    if (sessionAllocator->m_hugeChunkList != NULL) {
        MOT_LOG_WARN(
            "Session %u: There are still some huge chunks need to be released.", sessionAllocator->m_sessionId);
        MemVirtualHugeChunkHeader* chunkHeader = sessionAllocator->m_hugeChunkList;
        while (chunkHeader != NULL) {
            void* chunk = chunkHeader->m_chunk;
            MOT_LOG_TRACE("Session %u: Free session huge chunk %p in size of %u bytes",
                sessionAllocator->m_sessionId,
                chunk,
                chunkHeader->m_objectSizeBytes);
            MemVirtualHugeChunkHeader* oldChunkHeader = chunkHeader;
            chunkHeader = chunkHeader->m_next;
            MemHugeFree(oldChunkHeader, chunk, &(sessionAllocator->m_hugeChunkList));
        }
        sessionAllocator->m_hugeChunkList = NULL;
    }

    // 4. Reset the session allocator members
    errno_t erc = memset_s(sessionAllocator, sizeof(MemSessionAllocator), 0, sizeof(MemSessionAllocator));
    securec_check(erc, "\0", "\0");
    sessionAllocator->m_sessionId = INVALID_SESSION_ID;
}

extern void* MemSessionAllocatorAlloc(MemSessionAllocator* sessionAllocator, uint32_t size)
{
    ASSERT_SESSION_ALLOCATOR_VALID(sessionAllocator);

    void* result = NULL;

    if (size == 0 || size > MEM_SESSION_MAX_ALLOC_SIZE) {
        MOT_REPORT_ERROR(MOT_ERROR_INVALID_MEMORY_SIZE, "N/A", "Invalid requested size: %d byte(s).", size);
    } else {
        uint32_t realSize = RealAllocSize(size);
        MOT_LOG_DEBUG("Session %u thread %u: Request for %u bytes (real size is %u bytes)",
            sessionAllocator->m_sessionId,
            (unsigned)sessionAllocator->m_threadId,
            size,
            realSize);

        // 1. Search the free list to see if there is any memory block available.
        result = AllocFromFreeList(sessionAllocator, size, realSize);
        if (result == NULL) {
            // 2. Search the existed chunk to see if there is any memory block available.
            result = AllocFromExistingChunk(sessionAllocator, size, realSize);
            if (result == NULL) {
                // 3. Allocate a new chunk from the underlying raw chunk pool and get memory block from it.
                result = AllocFromNewChunk(sessionAllocator, size, realSize);
                if (result == NULL) {
                    MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                        "Session Memory Allocation",
                        "Failed to allocate memory from _session_allocators[%u].",
                        (unsigned)sessionAllocator->m_threadId);
                }
            }
        }

        // update statistics
        if (result != NULL) {
            MemSessionObjectHeader* objectHeader =
                (MemSessionObjectHeader*)(((char*)result - MEM_SESSION_OBJECT_HEADER_LEN));
            MemSessionAllocatorRecordSessionObject(sessionAllocator, objectHeader);

            sessionAllocator->m_usedSize += size;
            sessionAllocator->m_realUsedSize += realSize;
            if (sessionAllocator->m_usedSize > sessionAllocator->m_maxUsedSize) {
                sessionAllocator->m_maxUsedSize = sessionAllocator->m_usedSize;
            }
            if (sessionAllocator->m_realUsedSize > sessionAllocator->m_maxRealUsedSize) {
                sessionAllocator->m_maxRealUsedSize = sessionAllocator->m_realUsedSize;
            }
            if (sessionAllocator->m_allocType == MEM_ALLOC_GLOBAL) {
                MemoryStatisticsProvider::m_provider->AddGlobalBytesUsed(realSize);
                DetailedMemoryStatisticsProvider::m_provider->AddGlobalBytesRequested(size);
            } else {
                MemoryStatisticsProvider::m_provider->AddSessionBytesUsed(realSize);
                DetailedMemoryStatisticsProvider::m_provider->AddSessionBytesRequested(size);
            }
        }
    }

    return result;
}

extern void* MemSessionAllocatorAllocAligned(MemSessionAllocator* sessionAllocator, uint32_t size, uint32_t alignment)
{
    ASSERT_SESSION_ALLOCATOR_VALID(sessionAllocator);

    uint8_t* result = NULL;
    uint8_t* object = (uint8_t*)MemSessionAllocatorAlloc(sessionAllocator, size + alignment);
    if (object != NULL) {
        // fix pointer alignment and header offset
        result = (uint8_t*)((((uint64_t)object) & ~((uint64_t)(alignment - 1))) + alignment);
        MOT_LOG_DEBUG("Aligned [%u] allocation of % u bytes at %p from origin %p", alignment, size, result, object);
        if (result != object) {
            // object was not well-aligned, so we need to fix the header
            // in aligned allocations the first 4 bytes preceding the object tell where the real
            // object header is placed, as returned from MemSessionAllocatorAlloc()
            // the rest of the bogus header fields are not used.
            // see code in MemSessionAllocatorFree() for more details.
            MemSessionObjectHeader* objectHeader =
                (MemSessionObjectHeader*)((char*)result - MEM_SESSION_OBJECT_HEADER_LEN);
            objectHeader->m_alignedOffset = result - object;
            MemSessionObjectHeader* origObjectHeader =
                (MemSessionObjectHeader*)((char*)object - MEM_SESSION_OBJECT_HEADER_LEN);
            MOT_LOG_DEBUG("Aligned [%u] allocation of % u bytes at %p from origin %p: offset is %u (real size %u)",
                alignment,
                size,
                result,
                object,
                objectHeader->m_alignedOffset,
                origObjectHeader->m_objectSize.m_realSize);
        }
    }
    return result;
}

extern void MemSessionAllocatorFree(MemSessionAllocator* sessionAllocator, void* object)
{
    ASSERT_SESSION_ALLOCATOR_VALID(sessionAllocator);

    uint32_t objectSize = 0;
    MemSessionObjectHeader* objectHeader = (MemSessionObjectHeader*)((char*)object - MEM_SESSION_OBJECT_HEADER_LEN);

    // fix to real header of aligned object
    if (objectHeader->m_alignedOffset != 0) {
        MOT_LOG_DEBUG("Freeing aligned allocation %p by offset %u", object, objectHeader->m_alignedOffset);
        object = ((uint8_t*)object) - objectHeader->m_alignedOffset;
        objectHeader = (MemSessionObjectHeader*)(((uint8_t*)object) - MEM_SESSION_OBJECT_HEADER_LEN);
        MOT_LOG_DEBUG("Freeing fixed aligned allocation %p", object);
    }

    MOT_LOG_DEBUG(
        "Session %u thread %u free object %p: objectHeader->m_sessionId = %u, MOT_GET_CURRENT_SESSION_ID() = %u",
        sessionAllocator->m_sessionId,
        (unsigned)sessionAllocator->m_threadId,
        object,
        objectHeader->m_sessionId,
        MOT_GET_CURRENT_SESSION_ID());

    // Guard against double free of an object.
    if (objectHeader->m_sessionId == ((SessionId)INVALID_SESSION_ID)) {
        MOT_LOG_PANIC("Attempt to double-free session object %p at session %u, thread %u (current session id: %u, "
                      "current thread id: %u)",
            object,
            sessionAllocator->m_sessionId,
            (unsigned)sessionAllocator->m_threadId,
            MOT_GET_CURRENT_SESSION_ID(),
            (unsigned)MOTCurrThreadId);
        MOTAbort(object);
    } else if ((sessionAllocator->m_allocType == MEM_ALLOC_LOCAL) &&
               ((objectHeader->m_sessionId != sessionAllocator->m_sessionId) ||
                   ((objectHeader->m_sessionId !=
                       MOT_GET_CURRENT_SESSION_ID())))) {  // Check if the object belongs to this chunk or not.
        MOT_LOG_PANIC("Session %u thread %u free object %p: The object (objectHeader->m_sessionId = %u) going to be "
                      "freed does not belong to the sessionAllocator of current session id %u (current thread id: %u)",
            sessionAllocator->m_sessionId,
            (unsigned)sessionAllocator->m_threadId,
            object,
            objectHeader->m_sessionId,
            MOT_GET_CURRENT_SESSION_ID(),
            (unsigned)MOTCurrThreadId);
        MOTAbort(object);
    } else {
        // update statistics
        sessionAllocator->m_usedSize -= objectHeader->m_objectSize.m_size;
        sessionAllocator->m_realUsedSize -= objectHeader->m_objectSize.m_realSize;
        if (sessionAllocator->m_allocType == MEM_ALLOC_GLOBAL) {
            MemoryStatisticsProvider::m_provider->AddGlobalBytesUsed(-((int64_t)objectHeader->m_objectSize.m_size));
            DetailedMemoryStatisticsProvider::m_provider->AddGlobalBytesRequested(
                -((int64_t)objectHeader->m_objectSize.m_realSize));
        } else {
            MemoryStatisticsProvider::m_provider->AddSessionBytesUsed(-((int64_t)objectHeader->m_objectSize.m_size));
            DetailedMemoryStatisticsProvider::m_provider->AddSessionBytesRequested(
                -((int64_t)objectHeader->m_objectSize.m_realSize));
        }

        MemSessionAllocatorUnrecordSessionObject(sessionAllocator, objectHeader);
        // Push the object into the freeList.
        uint32_t index = GetFreeListIndex(objectHeader->m_objectSize.m_realSize);
        MOT_ASSERT(index < MEM_SESSION_CHUNK_FREE_LIST_LEN);
        MOT_LOG_DEBUG("Session %u thread %u free object %p: Push %u bytes memory block into the head of freeList[%u].",
            sessionAllocator->m_sessionId,
            (unsigned)sessionAllocator->m_threadId,
            object,
            objectHeader->m_objectSize.m_realSize,
            index);
        objectHeader->m_next = sessionAllocator->m_freeList[index];
        objectHeader->m_prev = NULL;
        objectHeader->m_sessionId = INVALID_SESSION_ID;
        MOT_LOG_DEBUG("Session %u thread %u free object %p: After free, objectHeader->m_sessionId = %u.",
            sessionAllocator->m_sessionId,
            (unsigned)sessionAllocator->m_threadId,
            object,
            objectHeader->m_sessionId);
        sessionAllocator->m_freeList[index] = objectHeader;
#ifdef MOT_DEBUG
        // set dead land pattern in object
        errno_t erc = memset_s(
            object, objectHeader->m_objectSize.m_realSize, MEM_DEAD_LAND, objectHeader->m_objectSize.m_realSize);
        securec_check(erc, "\0", "\0");
#endif
    }
}

extern void* MemSessionAllocatorRealloc(
    MemSessionAllocator* sessionAllocator, void* object, uint32_t newSize, MemReallocFlags flags)
{
    ASSERT_SESSION_ALLOCATOR_VALID(sessionAllocator);
    errno_t erc;
    void* result = NULL;
    MemSessionObjectHeader* objectHeader = (MemSessionObjectHeader*)((char*)object - MEM_SESSION_OBJECT_HEADER_LEN);
    if (objectHeader->m_alignedOffset != 0) {
        MOT_LOG_DEBUG("Reallocating aligned allocation %p by offset %u", object, objectHeader->m_alignedOffset);
        object = ((uint8_t*)object) - objectHeader->m_alignedOffset;
        objectHeader = (MemSessionObjectHeader*)(((uint8_t*)object) - MEM_SESSION_OBJECT_HEADER_LEN);
    }

    if (objectHeader->m_objectSize.m_realSize >= newSize) {
        result = object;
        MOT_LOG_DEBUG("The old object is big enough for requested new size.");

        // update statistics before update object size in object header
        // in case the new size is smaller than the current size, we update statistics in two steps (all variables are
        // unsigned)
        int64_t sizeDiff = ((int64_t)newSize) - ((int64_t)objectHeader->m_objectSize.m_size);
        if (sessionAllocator->m_allocType == MEM_ALLOC_GLOBAL) {
            MemoryStatisticsProvider::m_provider->AddGlobalBytesUsed(sizeDiff);
        } else {
            MemoryStatisticsProvider::m_provider->AddSessionBytesUsed(sizeDiff);
        }

        sessionAllocator->m_usedSize -= objectHeader->m_objectSize.m_size;
        sessionAllocator->m_usedSize += newSize;
        if (sessionAllocator->m_usedSize > sessionAllocator->m_maxUsedSize) {
            sessionAllocator->m_maxUsedSize = sessionAllocator->m_usedSize;
        }
        objectHeader->m_objectSize.m_size = newSize;
    } else {
        void* newObject = MemSessionAllocatorAlloc(sessionAllocator, newSize);
        if (newObject == NULL) {
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                "Session Memory Allocation",
                "Failed to allocate %u bytes from session allocator on thread %u",
                newSize,
                (unsigned)sessionAllocator->m_threadId);
        } else {
            uint32_t objectSizeBytes = objectHeader->m_objectSize.m_size;
            if (flags == MEM_REALLOC_COPY) {
                // attention: new size may be smaller
                erc = memcpy_s(newObject, newSize, object, min(objectSizeBytes, newSize));
                securec_check(erc, "\0", "\0");
            } else if (flags == MEM_REALLOC_ZERO) {
                erc = memset_s(newObject, newSize, 0, newSize);
                securec_check(erc, "\0", "\0");
            } else if (flags == MEM_REALLOC_COPY_ZERO) {
                // attention: new size may be smaller
                erc = memcpy_s(newObject, newSize, object, min(objectSizeBytes, newSize));
                securec_check(erc, "\0", "\0");
                if (newSize > objectSizeBytes) {
                    erc = memset_s(
                        ((char*)newObject) + objectSizeBytes, newSize - objectSizeBytes, 0, newSize - objectSizeBytes);
                    securec_check(erc, "\0", "\0");
                }
            }
            MemSessionAllocatorFree(sessionAllocator, object);
            result = newObject;
        }
    }
    return result;
}

extern uint32_t MemSessionAllocatorGetObjectSize(void* object, uint32_t* requestedSize /* = NULL */)
{
    uint32_t objectSize = 0;
    MemSessionObjectHeader* objectHeader = (MemSessionObjectHeader*)((char*)object - MEM_SESSION_OBJECT_HEADER_LEN);

    if (objectHeader->m_alignedOffset != 0) {
        MOT_LOG_DEBUG("Reallocating aligned allocation %p by offset %u", object, objectHeader->m_alignedOffset);
        object = ((uint8_t*)object) - objectHeader->m_alignedOffset;
        objectHeader = (MemSessionObjectHeader*)(((uint8_t*)object) - MEM_SESSION_OBJECT_HEADER_LEN);
    }
    if (requestedSize) {
        *requestedSize = objectHeader->m_objectSize.m_size;
    }
    return objectHeader->m_objectSize.m_realSize;
}

static void MemSessionAllocatorRecordSessionObject(
    MemSessionAllocator* sessionAllocator, MemSessionObjectHeader* objectHeader)
{
    ASSERT_SESSION_ALLOCATOR_VALID(sessionAllocator);
    objectHeader->m_next = sessionAllocator->m_sessionObjectList;
    objectHeader->m_prev = NULL;
    if (sessionAllocator->m_sessionObjectList != NULL) {
        sessionAllocator->m_sessionObjectList->m_prev = objectHeader;
    }
    sessionAllocator->m_sessionObjectList = objectHeader;
}

static void MemSessionAllocatorUnrecordSessionObject(
    MemSessionAllocator* sessionAllocator, MemSessionObjectHeader* objectHeader)
{
    ASSERT_SESSION_ALLOCATOR_VALID(sessionAllocator);
    if (objectHeader->m_prev == NULL) {
        sessionAllocator->m_sessionObjectList = objectHeader->m_next;
        if (objectHeader->m_next != NULL) {
            objectHeader->m_next->m_prev = NULL;
        }
    } else if (objectHeader->m_next == NULL) {
        if (objectHeader->m_prev != NULL) {
            objectHeader->m_prev->m_next = NULL;
        }
    } else {
        objectHeader->m_prev->m_next = objectHeader->m_next;
        objectHeader->m_next->m_prev = objectHeader->m_prev;
    }
}

extern void MemSessionAllocatorCleanup(MemSessionAllocator* sessionAllocator)
{
    ASSERT_SESSION_ALLOCATOR_VALID(sessionAllocator);
    if (sessionAllocator->m_sessionObjectList != NULL) {
        MOT_LOG_WARN("There are still some objects need to be released.");
        MemSessionObjectHeader* objectHeader = sessionAllocator->m_sessionObjectList;
        while (objectHeader != NULL) {
            void* object = (void*)((char*)objectHeader + MEM_SESSION_OBJECT_HEADER_LEN);
            MOT_LOG_TRACE("Free session object %p in size of %u bytes", object, objectHeader->m_objectSize.m_realSize);
            objectHeader = objectHeader->m_next;
            MemSessionAllocatorFree(sessionAllocator, object);
        }
        sessionAllocator->m_sessionObjectList = NULL;
    }
    if (sessionAllocator->m_largeBufferList != NULL) {
        MOT_LOG_WARN("There are still some large buffers need to be released.");
        MemSessionLargeBufferHeader* bufferHeader = sessionAllocator->m_largeBufferList;
        while (bufferHeader != NULL) {
            void* buffer = bufferHeader->m_buffer;
            MOT_LOG_TRACE("Free session large buffer %p in size of %u bytes", buffer, bufferHeader->m_realObjectSize);
            bufferHeader = bufferHeader->m_next;
            MemSessionLargeBufferFree(buffer, &(sessionAllocator->m_largeBufferList));
        }
        sessionAllocator->m_largeBufferList = NULL;
    }
    if (sessionAllocator->m_hugeChunkList != NULL) {
        MOT_LOG_WARN("There are still some huge chunks need to be released.");
        MemVirtualHugeChunkHeader* chunkHeader = sessionAllocator->m_hugeChunkList;
        while (chunkHeader != NULL) {
            void* chunk = chunkHeader->m_chunk;
            MOT_LOG_TRACE("Free session huge chunk %p in size of %u bytes", chunk, chunkHeader->m_objectSizeBytes);
            MemVirtualHugeChunkHeader* oldChunkHeader = chunkHeader;
            chunkHeader = chunkHeader->m_next;
            MemHugeFree(oldChunkHeader, chunk, &(sessionAllocator->m_hugeChunkList));
        }
        sessionAllocator->m_hugeChunkList = NULL;
    }
}

extern void MemSessionAllocatorGetStats(MemSessionAllocator* sessionAllocator, MemSessionAllocatorStats* stats)
{
    stats->m_sessionId = sessionAllocator->m_sessionId;
    stats->m_connectionId = sessionAllocator->m_connectionId;
    stats->m_threadId = sessionAllocator->m_threadId;
    stats->m_nodeId = sessionAllocator->m_nodeId;
    stats->m_reservedSize = MEM_CHUNK_SIZE_MB * MEGA_BYTE * sessionAllocator->m_chunkCount;
    stats->m_maxReservedSize = MEM_CHUNK_SIZE_MB * MEGA_BYTE * sessionAllocator->m_maxChunkCount;
    stats->m_usedSize = sessionAllocator->m_usedSize;
    stats->m_realUsedSize = sessionAllocator->m_realUsedSize;
    stats->m_maxUsedSize = sessionAllocator->m_maxUsedSize;
    stats->m_maxRealUsedSize = sessionAllocator->m_maxRealUsedSize;
}

extern void MemSessionAllocatorFormatStats(int indent, const char* name, StringBuffer* stringBuffer,
    MemSessionAllocatorStats* stats, MemReportMode reportMode /* = MEM_REPORT_SUMMARY */)
{
    StringBufferAppend(stringBuffer,
        "%*sSession allocator %s [node=%d, threadId=%u, sessionId=%u, connectionId=%u] statistics:\n",
        indent,
        "",
        name,
        stats->m_nodeId,
        (unsigned)stats->m_threadId,
        stats->m_sessionId,
        stats->m_connectionId);
    StringBufferAppend(stringBuffer,
        "%*sReserved      : %" PRIu64 " MB\n",
        indent + PRINT_REPORT_INDENT,
        "",
        stats->m_reservedSize / MEGA_BYTE);
    StringBufferAppend(stringBuffer,
        "%*sUsed          : %" PRIu64 " MB\n",
        indent + PRINT_REPORT_INDENT,
        "",
        stats->m_usedSize / MEGA_BYTE);
    StringBufferAppend(stringBuffer,
        "%*sAllocated     : %" PRIu64 " MB\n",
        indent + PRINT_REPORT_INDENT,
        "",
        stats->m_realUsedSize / MEGA_BYTE);

    if (reportMode == MEM_REPORT_DETAILED) {
        double memUtilInternal = ((double)stats->m_usedSize) / ((double)stats->m_realUsedSize) * 100.0;
        double memUtilExternal = ((double)stats->m_usedSize) / stats->m_reservedSize * 100.0;
        uint32_t minRequiredChunks =
            (stats->m_usedSize + MEM_CHUNK_SIZE_MB * MEGA_BYTE - 1) / (MEM_CHUNK_SIZE_MB * MEGA_BYTE);
        double maxUtilExternal =
            ((double)stats->m_usedSize) / (minRequiredChunks * MEM_CHUNK_SIZE_MB * MEGA_BYTE) * 100.0;

        StringBufferAppend(stringBuffer,
            "%*sPeak Reserved : %" PRIu64 "\n",
            indent + PRINT_REPORT_INDENT,
            "",
            stats->m_maxReservedSize);
        StringBufferAppend(
            stringBuffer, "%*sPeak Used     : %" PRIu64 "\n", indent + PRINT_REPORT_INDENT, "", stats->m_maxUsedSize);
        StringBufferAppend(stringBuffer,
            "%*sPeak Allocated: %" PRIu64 "\n",
            indent + PRINT_REPORT_INDENT,
            "",
            stats->m_maxRealUsedSize);
        StringBufferAppend(
            stringBuffer, "%*sInternal Util.: %0.2f%%\n", indent + PRINT_REPORT_INDENT, "", memUtilInternal);
        StringBufferAppend(stringBuffer,
            "%*sExternal Util.: %0.2f%% (Possible Maximum: %0.2f%%)\n",
            indent + PRINT_REPORT_INDENT,
            "",
            memUtilExternal,
            maxUtilExternal);
    }
}

extern void MemSessionAllocatorPrintStats(const char* name, LogLevel logLevel, MemSessionAllocatorStats* stats,
    MemReportMode reportMode /* = MEM_REPORT_SUMMARY */)
{
    if (MOT_CHECK_LOG_LEVEL(logLevel)) {
        StringBufferApply([name, logLevel, stats, reportMode](StringBuffer* stringBuffer) {
            MemSessionAllocatorFormatStats(0, name, stringBuffer, stats, reportMode);
            MOT_LOG(logLevel, "%s", stringBuffer->m_buffer);
        });
    }
}

extern void MemSessionAllocatorPrintCurrentStats(const char* name, LogLevel logLevel,
    MemSessionAllocator* sessionAllocator, MemReportMode reportMode /* = MEM_REPORT_SUMMARY */)
{
    if (MOT_CHECK_LOG_LEVEL(logLevel)) {
        MemSessionAllocatorStats stats = {0};
        MemSessionAllocatorGetStats(sessionAllocator, &stats);
        MemSessionAllocatorPrintStats(name, logLevel, &stats, reportMode);
    }
}

extern void MemSessionAllocatorPrint(const char* name, LogLevel logLevel, MemSessionAllocator* sessionAllocator,
    MemReportMode reportMode /* = MEM_REPORT_SUMMARY */)
{
    if (MOT_CHECK_LOG_LEVEL(logLevel)) {
        StringBufferApply([name, logLevel, sessionAllocator, reportMode](StringBuffer* stringBuffer) {
            MemSessionAllocatorToString(0, name, sessionAllocator, stringBuffer, reportMode);
            MOT_LOG(logLevel, "%s", stringBuffer->m_buffer);
        });
    }
}

extern void MemSessionAllocatorToString(int indent, const char* name, MemSessionAllocator* sessionAllocator,
    StringBuffer* stringBuffer, MemReportMode reportMode /* = MEM_REPORT_SUMMARY */)
{
    MemSessionAllocatorStats stats = {0};
    MemSessionAllocatorGetStats(sessionAllocator, &stats);

    MemSessionAllocatorFormatStats(0, name, stringBuffer, &stats);

    if (reportMode == MEM_REPORT_DETAILED) {
        // print each chunk status
        StringBufferAppend(stringBuffer, "%*sChunk List Status:\n", indent + PRINT_REPORT_INDENT, "");
        MemSessionChunkHeader* chunk = sessionAllocator->m_chunkList;
        for (uint32_t i = 0; i < sessionAllocator->m_chunkCount; ++i) {
            if (chunk == NULL) {
                StringBufferAppend(stringBuffer,
                    "%*sInvalid NULL Session Chunk at index %u",
                    indent + (2 * PRINT_REPORT_INDENT),
                    "",
                    i);
                break;
            } else if (chunk->m_chunkType != MEM_CHUNK_TYPE_SESSION) {
                StringBufferAppend(stringBuffer,
                    "%*sInvalid Session Chunk at index %u: chunk type %s does not match",
                    indent + (2 * PRINT_REPORT_INDENT),
                    "",
                    i,
                    MemChunkTypeToString(chunk->m_chunkType));
            } else {
                StringBufferAppend(stringBuffer,
                    "%*sSession Chunk at @p: FreePtr=%p, EndPtr=%p, FreeSize=%" PRIu64 "\n",
                    indent + (2 * PRINT_REPORT_INDENT),
                    chunk,
                    chunk->m_freePtr,
                    chunk->m_endPtr,
                    chunk->m_freeSize);
            }
            chunk = chunk->m_next;
        }

        // print free object lists
        uint32_t objectSize = 64;
        StringBufferAppend(stringBuffer, "%*sFree Object List Status:\n", indent + PRINT_REPORT_INDENT, "");
        for (uint32_t i = 0; i < MEM_SESSION_CHUNK_FREE_LIST_LEN; ++i) {
            MemSessionObjectHeader* objectList = sessionAllocator->m_freeList[i];
            if (objectList != NULL) {
                StringBufferAppend(
                    stringBuffer, "%*sObject[%u] = { ", indent + (2 * PRINT_REPORT_INDENT), "", objectSize);
                while (objectList != NULL) {
                    StringBufferAppend(stringBuffer, "%p (%u bytes)", objectList, objectList->m_objectSize.m_realSize);
                    if (objectList->m_next) {
                        StringBufferAppend(stringBuffer, ", ");
                    }
                    objectList = objectList->m_next;
                }
            }
            StringBufferAppend(stringBuffer, "}\n");
        }
    }
    StringBufferAppend(stringBuffer, "\n%*s}", indent, "");
}

extern void MemSessionChunkInit(
    MemSessionChunkHeader* chunk, MemChunkType chunkType, int nodeId, MemAllocType allocType)
{
    chunk->m_chunkType = chunkType;
    chunk->m_node = nodeId;
    chunk->m_allocType = allocType;
    chunk->m_allocatorNode = chunk->m_node;
    chunk->m_freePtr = (char*)chunk + MEM_SESSION_CHUNK_HEADER_LEN;
    chunk->m_endPtr = (char*)chunk + MEM_CHUNK_SIZE_MB * MEGA_BYTE;
    chunk->m_freeSize = MEM_CHUNK_SIZE_MB * MEGA_BYTE - MEM_SESSION_CHUNK_HEADER_LEN;
    chunk->m_next = NULL;
}

inline void MemSessionObjectInit(
    SessionId sessionId, MemSessionObjectHeader* objectHeader, uint32_t size, uint32_t realSize)
{
    // The size over here should be the size without object header length.
    // And it should always be power of two.
    objectHeader->m_objectSize.m_size = size;
    objectHeader->m_objectSize.m_realSize = realSize;
    objectHeader->m_sessionId = sessionId;
    objectHeader->m_alignedOffset = 0;
    objectHeader->m_prev = NULL;
    if (objectHeader->m_sessionId == INVALID_SESSION_ID) {
        MOTAbort();
    }
    MOT_LOG_DEBUG("Initialize session id to be %u.", sessionId);
}

static uint32_t RealAllocSize(uint32_t size)
{
    // If size is less than 64Kb
    if (size <= (KILO_BYTE * MemBufferClassToSizeKb(MEM_BUFFER_CLASS_KB_64))) {
        // Check if size is already power of two. If it is, just return.
        uint32_t count = __builtin_ctz(size) + __builtin_clz(size);
        if (count != 31) {
            size = size | (size >> 1);
            size = size | (size >> 2);
            size = size | (size >> 4);
            size = size | (size >> 8);
            size = size | (size >> 16);

            size += 1;
        }

        if (size < MEM_SESSION_MIN_ALLOC_SIZE) {
            size = MEM_SESSION_MIN_ALLOC_SIZE;
        }
    } else if (size <= (KILO_BYTE * MemBufferClassToSizeKb(MEM_BUFFER_CLASS_KB_127))) {
        // size is less than or equal to 127Kb
        size = KILO_BYTE * MemBufferClassToSizeKb(MEM_BUFFER_CLASS_KB_127);
    } else if (size <= (KILO_BYTE * MemBufferClassToSizeKb(MEM_BUFFER_CLASS_KB_255))) {
        // size is less than or equal to 255Kb
        size = KILO_BYTE * MemBufferClassToSizeKb(MEM_BUFFER_CLASS_KB_255);
    } else if (size <= (KILO_BYTE * MemBufferClassToSizeKb(MEM_BUFFER_CLASS_KB_511))) {
        // size is less than or equal to 511 Kb
        size = KILO_BYTE * MemBufferClassToSizeKb(MEM_BUFFER_CLASS_KB_511);
    } else {
        // size == 1022Kb
        size = KILO_BYTE * MemBufferClassToSizeKb(MEM_BUFFER_CLASS_KB_1022);
    }

    return size;
}

static uint32_t GetFreeListIndex(uint32_t size)
{
    uint32_t index = 0;

    if (size <= (KILO_BYTE * MemBufferClassToSizeKb(MEM_BUFFER_CLASS_KB_64))) {
        index = __builtin_ctz(size) - MEM_SESSION_CHUNK_BASE;
    } else if (size == (KILO_BYTE * MemBufferClassToSizeKb(MEM_BUFFER_CLASS_KB_127))) {
        index = 11;
    } else if (size == (KILO_BYTE * MemBufferClassToSizeKb(MEM_BUFFER_CLASS_KB_255))) {
        index = 12;
    } else if (size == (KILO_BYTE * MemBufferClassToSizeKb(MEM_BUFFER_CLASS_KB_511))) {
        index = 13;
    } else {
        index = 14;
    }

    return index;
}

static void* AllocFromFreeList(MemSessionAllocator* sessionAllocator, uint32_t size, uint32_t realSize)
{
    void* result = NULL;
    MemSessionObjectHeader* objectHeader = NULL;
    uint32_t index = GetFreeListIndex(realSize);

    objectHeader = sessionAllocator->m_freeList[index];

    if (objectHeader != NULL) {
        sessionAllocator->m_freeList[index] = objectHeader->m_next;
        MemSessionObjectInit(sessionAllocator->m_sessionId, objectHeader, size, realSize);
        result = (void*)((char*)objectHeader + MEM_SESSION_OBJECT_HEADER_LEN);
        MOT_LOG_DEBUG("Allocate %u bytes from freeList[%u] of _session_allocators[%u]",
            size,
            index,
            sessionAllocator->m_threadId);
    }

    return result;
}

static void* GetBlockFromChunk(MemSessionAllocator* sessionAllocator, MemSessionChunkHeader* chunk, uint32_t size,
    uint32_t realSize, bool printError /* = false */)
{
    // The input parameter size should always be power of two.
    void* result = NULL;
    MemSessionObjectHeader* objectHeader = (MemSessionObjectHeader*)chunk->m_freePtr;
    char* freePtr = ((char*)chunk->m_freePtr) + realSize + MEM_SESSION_OBJECT_HEADER_LEN;
    if (likely(freePtr <= chunk->m_endPtr)) {
        chunk->m_freePtr = freePtr;
        chunk->m_freeSize -= (realSize + MEM_SESSION_OBJECT_HEADER_LEN);
        MemSessionObjectInit(sessionAllocator->m_sessionId, objectHeader, size, realSize);
        result = (void*)((char*)objectHeader + MEM_SESSION_OBJECT_HEADER_LEN);
#ifdef MOT_DEBUG
        // set clean land pattern in object
        errno_t erc = memset_s(
            result, objectHeader->m_objectSize.m_realSize, MEM_CLEAN_LAND, objectHeader->m_objectSize.m_realSize);
        securec_check(erc, "\0", "\0");
#endif
    } else {
        if (printError) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM,
                "Session Memory Allocation",
                "Attempt to allocate %u bytes for session %u thread %u denied: Chunk has only %u bytes to give "
                "(aligned size is %u bytes)",
                size,
                sessionAllocator->m_sessionId,
                sessionAllocator->m_threadId,
                chunk->m_freeSize,
                realSize);
        } else {
            MOT_LOG_TRACE("Attempt to allocate %u bytes for session %u thread %u denied: Chunk has only %u bytes to "
                          "give (aligned size is %u bytes)",
                size,
                sessionAllocator->m_sessionId,
                sessionAllocator->m_threadId,
                chunk->m_freeSize,
                realSize);
        }
    }
    return result;
}

static void* AllocFromExistingChunk(MemSessionAllocator* sessionAllocator, uint32_t size, uint32_t realSize)
{
    void* result = NULL;

    MemSessionChunkHeader* chunk = sessionAllocator->m_chunkList;

    while (chunk != NULL) {
        if ((realSize + MEM_SESSION_OBJECT_HEADER_LEN) <= chunk->m_freeSize) {
            result = GetBlockFromChunk(sessionAllocator, chunk, size, realSize);
            if (result) {
                MOT_LOG_DEBUG("Session %u thread %u: Allocated %u bytes from existing chunk",
                    sessionAllocator->m_sessionId,
                    sessionAllocator->m_threadId,
                    size);
            }
            break;
        }
        chunk = chunk->m_next;
    }

    return result;
}

static void* AllocFromNewChunk(MemSessionAllocator* sessionAllocator, uint32_t size, uint32_t realSize)
{
    void* result = NULL;

    // make sure per-session small allocation restriction is imposed
    if ((sessionAllocator->m_allocType == MEM_ALLOC_LOCAL) && (g_memGlobalCfg.m_maxSessionMemoryKb > 0)) {
        if ((sessionAllocator->m_realUsedSize + realSize) >= g_memGlobalCfg.m_maxSessionMemoryKb * KILO_BYTE) {
            MOT_REPORT_ERROR(
                MOT_ERROR_OOM, "N/A", "Cannot allocate new chunk for session %u", sessionAllocator->m_sessionId);
            return nullptr;
        }
    }

    MemSessionChunkHeader* chunk = NULL;
    if (sessionAllocator->m_allocType == MEM_ALLOC_GLOBAL) {
        chunk = (MemSessionChunkHeader*)MemRawChunkStoreAllocGlobal(sessionAllocator->m_nodeId);
    } else {
        chunk = (MemSessionChunkHeader*)MemRawChunkStoreAllocLocal(sessionAllocator->m_nodeId);
    }

    if (chunk != NULL) {
        MemChunkType chunkType =
            (sessionAllocator->m_allocType == MEM_ALLOC_GLOBAL) ? MEM_CHUNK_TYPE_SMALL_OBJECT : MEM_CHUNK_TYPE_SESSION;
        MemSessionChunkInit(chunk, chunkType, sessionAllocator->m_nodeId, sessionAllocator->m_allocType);

        MOT_LOG_DEBUG("Push a new chunk into the head of the chunk list.");
        chunk->m_next = sessionAllocator->m_chunkList;
        sessionAllocator->m_chunkList = chunk;
        ++sessionAllocator->m_chunkCount;
        if (sessionAllocator->m_chunkCount > sessionAllocator->m_maxChunkCount) {
            sessionAllocator->m_maxChunkCount = sessionAllocator->m_chunkCount;
        }

        MOT_LOG_DEBUG("Allocate %d bytes from new chunk.", size);
        result = GetBlockFromChunk(sessionAllocator, chunk, size, realSize, true);
    } else {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Session Memory Allocation",
            "Request to allocate %u bytes for session %u denied: Could not load more chunks from the chunk pool of the "
            "associated NUMA node %u (aligned request size is %u bytes)",
            size,
            sessionAllocator->m_sessionId,
            sessionAllocator->m_nodeId,
            realSize);
    }

    return result;
}

}  // namespace MOT

extern "C" void MemSessionAllocatorDump(void* arg)
{
    MOT::MemSessionAllocator* sessionAllocator = (MOT::MemSessionAllocator*)arg;

    MOT::StringBufferApply([sessionAllocator](MOT::StringBuffer* stringBuffer) {
        MOT::MemSessionAllocatorToString(0, "Debug Dump", sessionAllocator, stringBuffer, MOT::MEM_REPORT_DETAILED);
        fprintf(stderr, "%s", stringBuffer->m_buffer);
        fflush(stderr);
    });
}

extern "C" int MemSessionAllocatorAnalyze(void* allocator, void* buffer)
{
    MOT::MemSessionAllocator* sessionAllocator = (MOT::MemSessionAllocator*)allocator;

    int found = 0;

    // First step make sure it belongs to any of the chunks
    MOT::MemSessionChunkHeader* chunk = sessionAllocator->m_chunkList;
    while (!found && (chunk != NULL)) {
        if ((((uint8_t*)buffer) >= ((uint8_t*)chunk)) && (((uint8_t*)buffer) < ((uint8_t*)chunk->m_endPtr))) {
            found = 1;
        } else {
            chunk = chunk->m_next;
        }
    }

    if (!found) {
        return 0;
    }

    fprintf(stderr,
        "Object %p found in session allocator for session %u thread %u\n",
        buffer,
        sessionAllocator->m_sessionId,
        (unsigned)sessionAllocator->m_threadId);

    // Step 2: print object details
    MOT::MemSessionObjectHeader* objectHeader =
        (MOT::MemSessionObjectHeader*)(((char*)buffer) - MEM_SESSION_OBJECT_HEADER_LEN);
    fprintf(stderr,
        "Object %p details: size = %u bytes, real size = %u bytes, session id = %u\n",
        buffer,
        objectHeader->m_objectSize.m_size,
        objectHeader->m_objectSize.m_size,
        objectHeader->m_sessionId);

    // Guard against double free of an object.
    if (objectHeader->m_sessionId == INVALID_SESSION_ID) {
        fprintf(stderr, "Object %p is an already freed object\n", buffer);
    } else if (objectHeader->m_sessionId !=
               sessionAllocator->m_sessionId) {  // Check if the object belongs to this chunk or not.
        fprintf(stderr,
            "Object %p is a live object marked as belonging to session %u, but is actually found in session allocator "
            "for session %u\n",
            buffer,
            objectHeader->m_sessionId,
            sessionAllocator->m_sessionId);
    } else {
        fprintf(stderr, "Object %p is a live object\n", buffer);
    }

    // Step 3: search buffer in all free lists (maybe this object was already freed)
    bool foundInList = false;
    for (uint32_t i = 0; i < MEM_SESSION_CHUNK_FREE_LIST_LEN && !foundInList; ++i) {
        MOT::MemSessionObjectHeader* freeList = sessionAllocator->m_freeList[i];
        while (freeList && !foundInList) {
            if (freeList == objectHeader) {
                fprintf(stderr, "Object %p found in free list %u\n", buffer, i);
                foundInList = true;
            } else {
                freeList = freeList->m_next;
            }
        }
    }

    return found;
}
