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
 * mm_session_api.cpp
 *    Session-local memory API, which provides session-local objects that can be used only in session context.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/memory/mm_session_api.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <string.h>

#include "mm_session_api.h"
#include "mm_cfg.h"
#include "mm_numa.h"
#include "mm_def.h"
#include "mot_error.h"
#include "mm_raw_chunk_store.h"
#include "thread_id.h"
#include "mm_buffer_class.h"
#include "mot_engine.h"
#include "mm_api.h"
#include "mm_session_large_buffer_store.h"
#include "mm_session_allocator.h"

namespace MOT {
DECLARE_LOGGER(SessionApi, Memory)

/** @var The actual maximum object size in bytes that can be allocated by a huge allocation. */
uint64_t memSessionHugeBufferMaxObjectSizeBytes = 0;

// comment to disable huge buffer usage
#define USING_LARGE_HUGE_SESSION_BUFFERS

struct MemSessionAttrSet {
    int m_node;
    MOTThreadId m_threadId;
    SessionId m_sessionId;
    ConnectionId m_connectionId;
};

static MemSessionAllocator** g_sessionAllocators = nullptr;
static void* g_sessionAllocatorBuf[MEM_MAX_NUMA_NODES];
static pthread_mutex_t g_sessionLock;  // required for safe statistics reporting

extern int MemSessionApiInit()
{
    int result = 0;
    MOT_LOG_TRACE("Initializing session API");
    memSessionHugeBufferMaxObjectSizeBytes = g_memGlobalCfg.m_sessionMaxHugeObjectSizeMb * MEGA_BYTE;

    int rc = pthread_mutex_init(&g_sessionLock, nullptr);
    if (rc != 0) {
        MOT_REPORT_SYSTEM_PANIC_CODE(
            rc, pthread_mutex_init, "Session API Initialization", "Failed to initialize session lock");
    } else {
        // 1. Allocate memory for global array: g_sessionAllocators, and nullify it.
        g_sessionAllocators =
            (MemSessionAllocator**)malloc(sizeof(MemSessionAllocator*) * g_memGlobalCfg.m_maxConnectionCount);
        if (g_sessionAllocators == nullptr) {
            MOT_REPORT_PANIC(MOT_ERROR_OOM,
                "Session API Initialization",
                "Failed to allocate memory for global array of g_sessionAllocators.");
            result = MOT_ERROR_OOM;
        } else {
            // make sure all session allocators are NULL
            errno_t erc = memset_s(g_sessionAllocators,
                sizeof(MemSessionAllocator*) * g_memGlobalCfg.m_maxConnectionCount,
                0,
                sizeof(MemSessionAllocator*) * g_memGlobalCfg.m_maxConnectionCount);
            securec_check(erc, "\0", "\0");
            // 2. Allocate memory for sessionAllocator for all threads, and nullify it.
            size_t size = sizeof(MemSessionAllocator) * g_memGlobalCfg.m_maxConnectionCount;
            for (uint32_t node = 0; node < g_memGlobalCfg.m_nodeCount; ++node) {
                g_sessionAllocatorBuf[node] = MemNumaAllocLocal(size, node);
                if (g_sessionAllocatorBuf[node] == nullptr) {
                    MOT_REPORT_PANIC(MOT_ERROR_OOM,
                        "Session API Initialization",
                        "Failed to allocate %u bytes for session allocators on NUMA node: %d",
                        size,
                        node);

                    // Exception: free all memory allocated successfully before to avoid memory leak;
                    for (uint32_t i = 0; i < node; i++) {
                        MemNumaFreeLocal(g_sessionAllocatorBuf[i], size, i);
                        g_sessionAllocatorBuf[i] = nullptr;
                    }
                    free(g_sessionAllocators);
                    g_sessionAllocators = nullptr;
                    pthread_mutex_destroy(&g_sessionLock);
                    result = GetLastError();
                    break;
                }
                MOT_ASSERT((uintptr_t)(g_sessionAllocatorBuf[node]) == L1_ALIGNED_PTR(g_sessionAllocatorBuf[node]));
                erc = memset_s(g_sessionAllocatorBuf[node], size, 0, size);
                securec_check(erc, "\0", "\0");
            }
        }
    }

    if (result == 0) {
        MOT_LOG_TRACE("Session API initialized successfully");
    }
    return result;
}

extern void MemSessionApiDestroy()
{
    MOT_LOG_TRACE("Destroying Session API");
    size_t size = sizeof(MemSessionAllocator) * g_memGlobalCfg.m_maxConnectionCount;

    // 1. Check if the session_allocators are still in use or not.
    for (uint32_t i = 0; i < g_memGlobalCfg.m_maxConnectionCount; ++i) {
        if (g_sessionAllocators[i] != nullptr) {
            MOT_LOG_WARN("Session allocator for thread %u is still in use", i);
        }
    }

    // 2. Free all g_sessionAllocatorBuf.
    for (uint32_t node = 0; node < g_memGlobalCfg.m_nodeCount; ++node) {
        MemNumaFreeLocal((void*)g_sessionAllocatorBuf[node], size, node);
        g_sessionAllocatorBuf[node] = nullptr;
    }

    // 3. Free g_sessionAllocators global array.
    free(g_sessionAllocators);
    g_sessionAllocators = nullptr;
    pthread_mutex_destroy(&g_sessionLock);
    MOT_LOG_TRACE("Session API destroyed");
}

static int MemSessionGetAttrSet(const char* phase, const char* operation, MemSessionAttrSet& attrSet)
{
    int result = 0;

    attrSet.m_node = MOTCurrentNumaNodeId;
    attrSet.m_threadId = MOTCurrThreadId;
    attrSet.m_sessionId = MOT_GET_CURRENT_SESSION_ID();
    attrSet.m_connectionId = MOT_GET_CURRENT_CONNECTION_ID();

    if ((attrSet.m_node == MEM_INVALID_NODE) || (attrSet.m_threadId == INVALID_THREAD_ID) ||
        (attrSet.m_sessionId == INVALID_SESSION_ID) || (attrSet.m_connectionId == INVALID_CONNECTION_ID)) {
        // invalid current session attributes, report and return error, MOT_ASSERT on debug mode
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
            phase,
            "Invalid attempt to %s without proper thread/connection/session/node identifier for the current thread "
            "(thread id: %u, connection id: %u, session id: %u, node id: %d)",
            operation,
            (unsigned)attrSet.m_threadId,
            attrSet.m_connectionId,
            attrSet.m_sessionId,
            attrSet.m_node);
        MOT_ASSERT(false);
        result = MOT_ERROR_INTERNAL;
    } else {
#ifdef MOT_DEBUG
        // validate session attributes (debug mode only)
        SessionContext* sessionContext = MOT_GET_CURRENT_SESSION_CONTEXT();
        if (sessionContext != nullptr) {
            MOT_ASSERT(attrSet.m_sessionId == sessionContext->GetSessionId());
            MOT_ASSERT(attrSet.m_connectionId == sessionContext->GetConnectionId());
        }

        MemSessionAllocator* sessionAllocator = g_sessionAllocators[attrSet.m_connectionId];
        if (sessionAllocator != nullptr) {
            MOT_ASSERT(sessionAllocator->m_sessionId == attrSet.m_sessionId);
            MOT_ASSERT(sessionAllocator->m_connectionId == attrSet.m_connectionId);
        }
#endif
    }

    return result;
}

extern int MemSessionReserve(uint32_t sizeBytes)
{
    MemSessionAttrSet attrSet = {0};
    int result =
        MemSessionGetAttrSet("Session Initialization", "reserve session memory and create session allocator", attrSet);
    if (result == 0) {
        int node = attrSet.m_node;
        ConnectionId connectionId = attrSet.m_connectionId;
        if (g_sessionAllocators[attrSet.m_connectionId] != nullptr) {
            // Check for double reservation for a sessionAllocator.
            MOT_LOG_WARN("Double reservation ignored for g_sessionAllocators[%u] on NUMA node %d.",
                attrSet.m_connectionId,
                node);
        } else {
            MOT_LOG_TRACE("Initializing g_sessionAllocators[%u]", attrSet.m_connectionId, node);
            // 1.  get the right start address of g_sessionAllocatorBuf.
            MemSessionAllocator* sessionAllocator =
                (MemSessionAllocator*)(((char*)g_sessionAllocatorBuf[node]) +
                                       sizeof(MemSessionAllocator) * connectionId);

            // 2. Allocate raw chunks from the NUMA node local chunk pool according to the requested size.
            uint32_t chunkCount = (sizeBytes + MEM_CHUNK_SIZE_MB * MEGA_BYTE - 1) / (MEM_CHUNK_SIZE_MB * MEGA_BYTE);

            MemSessionChunkHeader* chunkList = nullptr;

            for (uint32_t i = 0; i < chunkCount; ++i) {
                MemSessionChunkHeader* chunk = (MemSessionChunkHeader*)MemRawChunkStoreAllocLocal(node);
                if (chunk == nullptr) {
                    result = GetLastError();
                    MOT_REPORT_ERROR(MOT_ERROR_OOM,
                        "Session Initialization",
                        "Failed to allocate chunk for session allocator (during new session memory reservation)");
                    MOT_LOG_DEBUG("Exception: free all chunks successfully allocated before to avoid memory leak.");

                    // Exception: free all chunks allocated successfully before to avoid memory leak;
                    while (chunkList != nullptr) {
                        MemSessionChunkHeader* temp = chunkList;
                        chunkList = chunkList->m_next;
                        MemRawChunkStoreFreeLocal((void*)temp, node);
                    }

                    break;
                }
                MemSessionChunkInit(chunk, MEM_CHUNK_TYPE_SESSION, node, MEM_ALLOC_LOCAL);
                chunk->m_next = chunkList;
                chunkList = chunk;
            }

            // 3. Initialize the sessionAllocator.
            if (result == 0) {
                MemSessionAllocatorInit(
                    sessionAllocator, attrSet.m_sessionId, connectionId, chunkList, chunkCount, MEM_ALLOC_LOCAL);

                // save the new allocator in the allocator array (minimize time lock is held)
                pthread_mutex_lock(&g_sessionLock);
                g_sessionAllocators[connectionId] = sessionAllocator;
                pthread_mutex_unlock(&g_sessionLock);

                MOT_LOG_DEBUG("Session allocator initialized for thread id %u, connection id %u, session id: %u",
                    (unsigned)attrSet.m_threadId,
                    connectionId,
                    attrSet.m_sessionId);
            } else {
                g_sessionAllocators[attrSet.m_connectionId] = nullptr;
            }
        }
    }

    return result;
}

extern void MemSessionUnreserve()
{
    MemSessionAttrSet attrSet = {0};
    int result =
        MemSessionGetAttrSet("Session Termination", "unreserve session memory and destroy session allocator", attrSet);
    if (result != 0) {
        return;
    }

    MOT_LOG_TRACE("Destroying g_sessionAllocators[%u]", attrSet.m_connectionId);
    ConnectionId connectionId = attrSet.m_connectionId;
    MemSessionAllocator* sessionAllocator = g_sessionAllocators[connectionId];

    // Guard against double free.
    if (sessionAllocator == nullptr) {
        MOT_LOG_WARN("Ignoring double attempt to destroy session allocator[%u]", connectionId);
    } else {
        MemSessionAllocatorPrint("Pre-Terminate report", LogLevel::LL_TRACE, sessionAllocator);

        // minimize time lock is held (set NULL quickly before destroying later)
        pthread_mutex_lock(&g_sessionLock);
        g_sessionAllocators[connectionId] = nullptr;
        pthread_mutex_unlock(&g_sessionLock);

        MemSessionAllocatorDestroy(sessionAllocator);
    }
}

extern void* MemSessionAlloc(uint64_t sizeBytes)
{
    void* result = nullptr;

#ifndef MEM_SESSION_ACTIVE
    result = malloc(sizeBytes);
#else
    MemSessionAttrSet attrSet = {0};
    int res = MemSessionGetAttrSet("Session Memory Allocation", "allocate session-local memory", attrSet);
    if (res != 0) {
        return nullptr;
    }
    ConnectionId connectionId = attrSet.m_connectionId;
    SessionId sessionId = attrSet.m_sessionId;

#ifndef USING_LARGE_HUGE_SESSION_BUFFERS
    MOT_LOG_DEBUG("Allocating %" PRIu64 " bytes by session %u from session allocator %u",
        sizeBytes,
        sessionId,
        (unsigned)connectionId);
    result = MemSessionAllocatorAlloc(g_sessionAllocators[connectionId], sizeBytes);
#else
    if (sizeBytes <= MEM_SESSION_MAX_ALLOC_SIZE) {
        MOT_LOG_DEBUG(
            "Allocating %" PRIu64 " bytes by session %u from session allocator %u", sizeBytes, sessionId, connectionId);
        result = MemSessionAllocatorAlloc(g_sessionAllocators[connectionId], sizeBytes);
    } else {
        // try to allocate from large buffer store
        if (sizeBytes <= g_memSessionLargeBufferMaxObjectSizeBytes) {
            MOT_LOG_DEBUG("Trying to allocate %" PRIu64
                          " bytes by session %u connection %u from session large buffer store",
                sizeBytes,
                sessionId,
                connectionId);
            result = MemSessionLargeBufferAlloc(sizeBytes, &(g_sessionAllocators[connectionId]->m_largeBufferList));
        }
        // NOTE: large/huge buffers not counted in statistics
        // try to allocate from kernel (also when large allocation fails)
        if (result == nullptr) {
            if (sizeBytes <= memSessionHugeBufferMaxObjectSizeBytes) {
                // NOTE: check with g_instance.attr.attr_memory.max_process_memory is not done before we proceed
                MOT_LOG_DEBUG("Trying to allocate %" PRIu64 " bytes by session %u connection %u as huge memory",
                    sizeBytes,
                    sessionId,
                    connectionId);
                result = MemHugeAlloc(
                    sizeBytes, attrSet.m_node, MEM_ALLOC_LOCAL, &(g_sessionAllocators[connectionId]->m_hugeChunkList));
            } else {
                MOT_REPORT_ERROR(MOT_ERROR_INVALID_MEMORY_SIZE,
                    "N/A",
                    "Request for invalid allocation size %" PRIu64
                    " bytes for session %u, thread %u, connection %u denied: Size exceeds configured limit of %" PRIu64
                    " bytes",
                    sizeBytes,
                    sessionId,
                    (unsigned)attrSet.m_threadId,
                    connectionId,
                    memSessionHugeBufferMaxObjectSizeBytes);
            }
        }
    }
#endif
#endif
    return result;
}

extern void* MemSessionAllocAligned(uint64_t sizeBytes, uint32_t alignment)
{
    void* result = nullptr;
#ifndef MEM_SESSION_ACTIVE
    result = malloc(ALIGN_N(sizeBytes, alignment));
#else
    MemSessionAttrSet attrSet = {0};
    int res = MemSessionGetAttrSet("Session Memory Allocation", "allocate aligned session-local memory", attrSet);
    if (res != 0) {
        return nullptr;
    }
    ConnectionId connectionId = attrSet.m_connectionId;
    SessionId sessionId = attrSet.m_sessionId;

#ifndef USING_LARGE_HUGE_SESSION_BUFFERS
    MOT_LOG_DEBUG("Allocating %" PRIu64 " bytes aligned to %u bytes by session %u from session allocator %u",
        sizeBytes,
        alignment,
        sessionId,
        (unsigned)connectionId);
    result = MemSessionAllocatorAllocAligned(g_sessionAllocators[connectionId], sizeBytes, alignment);
#else
    if ((sizeBytes + alignment) <= MEM_SESSION_MAX_ALLOC_SIZE) {
        MOT_LOG_DEBUG("Allocating %" PRIu64 " bytes aligned to %u bytes by session %u from session allocator %u",
            sizeBytes,
            alignment,
            sessionId,
            connectionId);
        result = MemSessionAllocatorAllocAligned(g_sessionAllocators[connectionId], sizeBytes, alignment);
    } else if ((alignment % PAGE_SIZE_BYTES) == 0) {
        // allocate as a regular allocation, since we have native 4 KB alignment
        result = MemSessionAlloc(sizeBytes);
    } else {
        MOT_REPORT_ERROR(MOT_ERROR_INVALID_MEMORY_SIZE,
            "Session Memory Allocation",
            "Unsupported size %" PRIu64 " for aligned allocations",
            sizeBytes);
    }
#endif
#endif
    return result;
}

static void* MemSessionReallocSmall(
    void* object, uint64_t newSizeBytes, MemReallocFlags flags, ConnectionId connectionId, SessionId sessionId)
{
    void* newObject = nullptr;
    errno_t erc;
    if (newSizeBytes <= MEM_SESSION_MAX_ALLOC_SIZE) {
        MOT_LOG_DEBUG("Reallocating %" PRIu64 " bytes by session %u from session allocator %u",
            newSizeBytes,
            sessionId,
            connectionId);
        newObject = MemSessionAllocatorRealloc(g_sessionAllocators[connectionId], object, newSizeBytes, flags);
    } else {
        MOT_LOG_TRACE("Reallocating %" PRIu64 " bytes by session %u from session allocator %u to large/huge buffer",
            newSizeBytes,
            sessionId,
            connectionId);
        newObject = MemSessionAlloc(newSizeBytes);
        if (newObject != nullptr) {
            MemSessionObjectHeader* objectHeader =
                (MemSessionObjectHeader*)((char*)object - MEM_SESSION_OBJECT_HEADER_LEN);
            uint64_t objectSizeBytes = (uint64_t)objectHeader->m_objectSize.m_realSize;
            if (flags == MEM_REALLOC_COPY) {
                // attention: new size may be smaller
                erc = memcpy_s(newObject, newSizeBytes, object, std::min(newSizeBytes, objectSizeBytes));
                securec_check(erc, "\0", "\0");
            } else if (flags == MEM_REALLOC_ZERO) {
                erc = memset_s(newObject, newSizeBytes, 0, newSizeBytes);
                securec_check(erc, "\0", "\0");
            } else if (flags == MEM_REALLOC_COPY_ZERO) {
                // attention: new size may be smaller
                erc = memcpy_s(newObject, newSizeBytes, object, std::min(newSizeBytes, objectSizeBytes));
                securec_check(erc, "\0", "\0");
                if (newSizeBytes > objectSizeBytes) {
                    erc = memset_s(((char*)newObject) + objectSizeBytes,
                        newSizeBytes - objectSizeBytes,
                        0,
                        newSizeBytes - objectSizeBytes);
                    securec_check(erc, "\0", "\0");
                }
            }
            MemSessionAllocatorFree(g_sessionAllocators[connectionId], object);
        }
    }

    return newObject;
}

static void* MemSessionReallocLarge(
    void* object, uint64_t newSizeBytes, MemReallocFlags flags, ConnectionId connectionId, SessionId sessionId)
{
    void* newObject = nullptr;
    errno_t erc;
    if ((newSizeBytes > MEM_SESSION_MAX_ALLOC_SIZE) && (newSizeBytes <= g_memSessionLargeBufferMaxObjectSizeBytes)) {
        newObject = MemSessionLargeBufferRealloc(
            object, newSizeBytes, flags, &(g_sessionAllocators[connectionId]->m_largeBufferList));
    } else {
        MOT_LOG_TRACE("Reallocating %" PRIu64
                      " bytes by session %u, connection %u from large to small/huge session buffer",
            newSizeBytes,
            sessionId,
            connectionId);
        newObject = MemSessionAlloc(newSizeBytes);
        if (newObject != nullptr) {
            uint64_t objectSizeBytes = MemSessionLargeBufferGetObjectSize(object);
            if (flags == MEM_REALLOC_COPY) {
                // attention: new size may be smaller
                erc = memcpy_s(newObject, newSizeBytes, object, min(newSizeBytes, objectSizeBytes));
                securec_check(erc, "\0", "\0");
            } else if (flags == MEM_REALLOC_ZERO) {
                erc = memset_s(newObject, newSizeBytes, 0, newSizeBytes);
                securec_check(erc, "\0", "\0");
            } else if (flags == MEM_REALLOC_COPY_ZERO) {
                // attention: new size may be smaller
                erc = memcpy_s(newObject, newSizeBytes, object, min(newSizeBytes, objectSizeBytes));
                securec_check(erc, "\0", "\0");
                if (newSizeBytes > objectSizeBytes) {
                    erc = memset_s(((char*)newObject) + objectSizeBytes,
                        newSizeBytes - objectSizeBytes,
                        0,
                        newSizeBytes - objectSizeBytes);
                    securec_check(erc, "\0", "\0");
                }
            }
            MemSessionLargeBufferFree(object, &(g_sessionAllocators[connectionId]->m_largeBufferList));
        }
    }

    return newObject;
}

static void* MemSessionReallocHuge(void* object, uint64_t newSizeBytes, MemReallocFlags flags,
    MemRawChunkHeader* rawChunk, ConnectionId connectionId, SessionId sessionId)
{
    void* newObject = nullptr;
    errno_t erc;
    if ((newSizeBytes > g_memSessionLargeBufferMaxObjectSizeBytes) &&
        (newSizeBytes <= memSessionHugeBufferMaxObjectSizeBytes)) {
        newObject = MemHugeRealloc((MemVirtualHugeChunkHeader*)rawChunk,
            object,
            newSizeBytes,
            flags,
            &(g_sessionAllocators[connectionId]->m_hugeChunkList));
    } else {
        MOT_LOG_TRACE("Reallocating %" PRIu64
                      " bytes by session %u, connection %u from huge to small/large session buffer",
            newSizeBytes,
            sessionId,
            connectionId);
        newObject = MemSessionAlloc(newSizeBytes);
        if (newObject != nullptr) {
            uint64_t objectSizeBytes = ((MemVirtualHugeChunkHeader*)rawChunk)->m_objectSizeBytes;
            if (flags == MEM_REALLOC_COPY) {
                // attention: new size may be smaller
                erc = memcpy_s(newObject, newSizeBytes, object, min(newSizeBytes, objectSizeBytes));
                securec_check(erc, "\0", "\0");
            } else if (flags == MEM_REALLOC_ZERO) {
                erc = memset_s(newObject, newSizeBytes, 0, newSizeBytes);
                securec_check(erc, "\0", "\0");
            } else if (flags == MEM_REALLOC_COPY_ZERO) {
                // attention: new size may be smaller
                erc = memcpy_s(newObject, newSizeBytes, object, min(newSizeBytes, objectSizeBytes));
                securec_check(erc, "\0", "\0");
                if (newSizeBytes > objectSizeBytes) {
                    erc = memset_s(((char*)newObject) + objectSizeBytes,
                        newSizeBytes - objectSizeBytes,
                        0,
                        newSizeBytes - objectSizeBytes);
                    securec_check(erc, "\0", "\0");
                }
            }
            MemHugeFree(
                (MemVirtualHugeChunkHeader*)rawChunk, object, &(g_sessionAllocators[connectionId]->m_hugeChunkList));
        }
    }

    return newObject;
}

extern void* MemSessionRealloc(void* object, uint64_t newSizeBytes, MemReallocFlags flags)
{
    void* newObject = nullptr;

    MemSessionAttrSet attrSet = {0};
    int res = MemSessionGetAttrSet("Session Memory Allocation", "allocate aligned session-local memory", attrSet);
    if (res != 0) {
        return nullptr;
    }
    ConnectionId connectionId = attrSet.m_connectionId;
    SessionId sessionId = attrSet.m_sessionId;

    if (object == nullptr) {
        newObject = MemSessionAlloc(newSizeBytes);
    } else {
#ifndef USING_LARGE_HUGE_SESSION_BUFFERS
        MOT_LOG_DEBUG("Reallocating %" PRIu64 " bytes by session %u from session allocator %u",
            newSizeBytes,
            sessionId,
            connectionId);
        newObject = MemSessionAllocatorRealloc(g_sessionAllocators[connectionId], object, newSizeBytes, flags);
#else
        // find out source of object (session allocator/session large buffer store/huge chunk)
        // pay attention, this code is a bit tricky: the object could come from one source, but the new size fits to
        // another source
        MemRawChunkHeader* rawChunk = (MemRawChunkHeader*)MemRawChunkDirLookup(object);
        if (rawChunk->m_chunkType == MEM_CHUNK_TYPE_SESSION) {
            // re-allocate a session-local small object
            newObject = MemSessionReallocSmall(object, newSizeBytes, flags, connectionId, sessionId);
        } else if (rawChunk->m_chunkType == MEM_CHUNK_TYPE_SESSION_LARGE_BUFFER_POOL) {
            // re-allocate a session-local large object
            newObject = MemSessionReallocLarge(object, newSizeBytes, flags, connectionId, sessionId);
        } else if (rawChunk->m_chunkType == MEM_CHUNK_TYPE_HUGE_OBJECT) {
            // re-allocate a session-local huge object
            newObject = MemSessionReallocHuge(object, newSizeBytes, flags, rawChunk, connectionId, sessionId);
        } else {
            MOT_LOG_PANIC("Invalid object %p provided to MemSessionRealloc(): Unknown source chunk type %u (%s)",
                object,
                rawChunk->m_chunkType,
                MemChunkTypeToString(rawChunk->m_chunkType));
            MOTAbort(object);
        }
#endif
    }

    return newObject;
}

extern void MemSessionFree(void* object)
{
#ifndef MEM_SESSION_ACTIVE
    free(object);
#else
    MemSessionAttrSet attrSet = {0};
    int res = MemSessionGetAttrSet("Session Memory De-Allocation", "free session-local memory", attrSet);
    if (res != 0) {
        return;
    }
    ConnectionId connectionId = attrSet.m_connectionId;

    if (object != nullptr) {
        // find out source of object (session allocator/session large buffer store/huge chunk)
#ifdef USING_LARGE_HUGE_SESSION_BUFFERS
        MemRawChunkHeader* rawChunk = (MemRawChunkHeader*)MemRawChunkDirLookup(object);
        if (rawChunk->m_chunkType == MEM_CHUNK_TYPE_SESSION) {
#endif
            MemSessionAllocator* sessionAllocator = g_sessionAllocators[connectionId];

            if (sessionAllocator == nullptr) {
                MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                    "Session Memory De-Allocation",
                    "Cannot free session memory: session allocator of connection %u is NULL.",
                    connectionId);
            } else {
                MemSessionAllocatorFree(sessionAllocator, object);
            }
#ifdef USING_LARGE_HUGE_SESSION_BUFFERS
        } else if (rawChunk->m_chunkType == MEM_CHUNK_TYPE_SESSION_LARGE_BUFFER_POOL) {
            MemSessionLargeBufferFree(object, &(g_sessionAllocators[connectionId]->m_largeBufferList));
        } else if (rawChunk->m_chunkType == MEM_CHUNK_TYPE_HUGE_OBJECT) {
            MemHugeFree(
                (MemVirtualHugeChunkHeader*)rawChunk, object, &(g_sessionAllocators[connectionId]->m_hugeChunkList));
        } else {
            MOT_LOG_PANIC("Invalid object %p provided to MemSessionFree(): Unknown source chunk type %u (%s)",
                object,
                rawChunk->m_chunkType,
                MemChunkTypeToString(rawChunk->m_chunkType));
            MOTAbort(object);
        }
#endif
    }
#endif
}

extern void MemSessionCleanup()
{
    MemSessionAttrSet attrSet = {0};
    int res = MemSessionGetAttrSet("Session Destruction", "cleanup session-local memory", attrSet);
    if (res != 0) {
        return;
    }
    ConnectionId connectionId = attrSet.m_connectionId;

    MemSessionAllocator* sessionAllocator = g_sessionAllocators[connectionId];

    if (sessionAllocator == nullptr) {
        MOT_LOG_TRACE("g_sessionAllocators[%u] was already NULL. No need to cleanup.", connectionId);
    } else {
        MemSessionAllocatorCleanup(sessionAllocator);
    }
}

extern void MemSessionApiPrint(const char* name, LogLevel logLevel, MemReportMode reportMode /* = MEM_REPORT_SUMMARY */)
{
    if (MOT_CHECK_LOG_LEVEL(logLevel)) {
        StringBufferApply([name, logLevel, reportMode](StringBuffer* stringBuffer) {
            MemSessionApiToString(0, name, stringBuffer, reportMode);
            MOT_LOG(logLevel, "%s", stringBuffer->m_buffer);
        });
    }
}

extern void MemSessionApiToString(
    int indent, const char* name, StringBuffer* stringBuffer, MemReportMode reportMode /* = MEM_REPORT_SUMMARY */)
{
    StringBufferAppend(stringBuffer, "%*sSession Memory %s Report:\n", indent, "", name);
    MemSessionAllocatorStats* sessionStatsArray =
        (MemSessionAllocatorStats*)calloc(g_memGlobalCfg.m_maxConnectionCount, sizeof(MemSessionAllocatorStats));
    if (sessionStatsArray != nullptr) {
        uint32_t sessionCount = MemSessionGetAllStats(sessionStatsArray, g_memGlobalCfg.m_maxConnectionCount);
        if (reportMode == MEM_REPORT_SUMMARY) {
            MemSessionAllocatorStats aggStats;
            errno_t erc = memset_s(&aggStats, sizeof(MemSessionAllocatorStats), 0, sizeof(MemSessionAllocatorStats));
            securec_check(erc, "\0", "\0");
            for (uint32_t i = 0; i < sessionCount; ++i) {
                MemSessionAllocatorStats* stats = &sessionStatsArray[i];
                aggStats.m_reservedSize += stats->m_reservedSize;
                aggStats.m_usedSize += stats->m_usedSize;
                aggStats.m_realUsedSize += stats->m_realUsedSize;
                aggStats.m_maxUsedSize += stats->m_maxUsedSize;
                aggStats.m_maxRealUsedSize += stats->m_maxRealUsedSize;
            }
            MemSessionAllocatorFormatStats(indent + PRINT_REPORT_INDENT, name, stringBuffer, &aggStats, reportMode);
        } else {
            for (uint32_t i = 0; i < sessionCount; ++i) {
                MemSessionAllocatorStats* stats = &sessionStatsArray[i];
                MemSessionAllocatorFormatStats(indent + PRINT_REPORT_INDENT, name, stringBuffer, stats, reportMode);
            }
        }
        free(sessionStatsArray);
    }
}

extern int MemSessionGetStats(ConnectionId connectionId, MemSessionAllocatorStats* stats)
{
    // get the session allocator for the session id
    int result = 0;
    pthread_mutex_lock(&g_sessionLock);

    if (connectionId != INVALID_CONNECTION_ID) {
        MemSessionAllocator* sessionAllocator = g_sessionAllocators[connectionId];
        if (sessionAllocator == nullptr) {
            MOT_LOG_WARN(
                "Cannot retrieve session allocation statistics for connection %u: connection id invalid", connectionId);
        } else {
            MemSessionAllocatorGetStats(sessionAllocator, stats);
            result = 1;
        }
    }

    pthread_mutex_unlock(&g_sessionLock);
    return result;
}

extern void MemSessionPrintStats(ConnectionId connectionId, const char* name, LogLevel logLevel)
{
    MemSessionAllocatorStats stats;
    if (MemSessionGetStats(connectionId, &stats)) {
        MemSessionAllocatorPrintStats(name, logLevel, &stats);
    }
}

extern int MemSessionGetCurrentStats(MemSessionAllocatorStats* stats)
{
    return MemSessionGetStats(MOT_GET_CURRENT_CONNECTION_ID(), stats);
}

extern void MemSessionPrintCurrentStats(const char* name, LogLevel logLevel)
{
    MemSessionPrintStats(MOT_GET_CURRENT_CONNECTION_ID(), name, logLevel);
}

extern uint32_t MemSessionGetAllStats(MemSessionAllocatorStats* sessionStatsArray, uint32_t sessionCount)
{
    uint32_t entriesReported = 0;
    pthread_mutex_lock(&g_sessionLock);

    for (uint32_t connectionId = 0; connectionId < g_memGlobalCfg.m_maxConnectionCount; ++connectionId) {
        MemSessionAllocator* sessionAllocator = g_sessionAllocators[connectionId];
        if (sessionAllocator != nullptr) {
            MemSessionAllocatorStats* stats = &sessionStatsArray[entriesReported];
            MemSessionAllocatorGetStats(sessionAllocator, stats);
            if (++entriesReported == sessionCount) {
                break;
            }
        }
    }
    pthread_mutex_unlock(&g_sessionLock);
    return entriesReported;
}

extern void MemSessionPrintAllStats(const char* name, LogLevel logLevel)
{
    if (MOT_CHECK_LOG_LEVEL(logLevel)) {
        MemSessionAllocatorStats* sessionStatsArray =
            (MemSessionAllocatorStats*)calloc(g_memGlobalCfg.m_maxConnectionCount, sizeof(MemSessionAllocatorStats));
        if (sessionStatsArray != nullptr) {
            uint32_t sessionCount = MemSessionGetAllStats(sessionStatsArray, g_memGlobalCfg.m_maxConnectionCount);

            MOT_LOG(logLevel, "%s Session Allocator Report:", name);
            for (uint32_t i = 0; i < sessionCount; ++i) {
                MemSessionAllocatorStats* stats = &sessionStatsArray[i];
                MemSessionAllocatorPrintStats(name, logLevel, stats);
            }
            free(sessionStatsArray);
        }
    }
}

extern void MemSessionPrintSummary(const char* name, LogLevel logLevel, bool fullReport /* = false */)
{
    if (MOT_CHECK_LOG_LEVEL(logLevel)) {
        MemSessionAllocatorStats* sessionStatsArray =
            (MemSessionAllocatorStats*)calloc(g_memGlobalCfg.m_maxConnectionCount, sizeof(MemSessionAllocatorStats));
        if (sessionStatsArray != nullptr) {
            MemSessionAllocatorStats aggStats;
            errno_t erc = memset_s(&aggStats, sizeof(MemSessionAllocatorStats), 0, sizeof(MemSessionAllocatorStats));
            securec_check(erc, "\0", "\0");
            uint32_t sessionCount = MemSessionGetAllStats(sessionStatsArray, g_memGlobalCfg.m_maxConnectionCount);
            for (uint32_t i = 0; i < sessionCount; ++i) {
                MemSessionAllocatorStats* stats = &sessionStatsArray[i];
                aggStats.m_reservedSize += stats->m_reservedSize;
                aggStats.m_usedSize += stats->m_usedSize;
                aggStats.m_realUsedSize += stats->m_realUsedSize;
                aggStats.m_maxUsedSize += stats->m_maxUsedSize;
                aggStats.m_maxRealUsedSize += stats->m_maxRealUsedSize;
            }
            free(sessionStatsArray);

            MOT_LOG(logLevel,
                "Session %s [Current]: Reserved = %" PRIu64 " MB, Requested = %" PRIu64 " MB, Allocated = %" PRIu64
                " MB",
                name,
                aggStats.m_reservedSize / MEGA_BYTE,
                aggStats.m_usedSize / MEGA_BYTE,
                aggStats.m_realUsedSize / MEGA_BYTE);

            if (fullReport) {
                double memUtilInternal = ((double)aggStats.m_usedSize) / ((double)aggStats.m_realUsedSize) * 100.0;
                double memUtilExternal = ((double)aggStats.m_usedSize) / aggStats.m_reservedSize * 100.0;
                uint32_t minRequiredChunks =
                    (aggStats.m_usedSize + MEM_CHUNK_SIZE_MB * MEGA_BYTE - 1) / (MEM_CHUNK_SIZE_MB * MEGA_BYTE);
                double maxUtilExternal =
                    ((double)aggStats.m_usedSize) / (minRequiredChunks * MEM_CHUNK_SIZE_MB * MEGA_BYTE) * 100.0;

                MOT_LOG(logLevel,
                    "Session %s [Peak]:    Reserved = %" PRIu64 " MB, Requested = %" PRIu64 " MB, Allocated = %" PRIu64
                    " MB",
                    name,
                    aggStats.m_maxReservedSize / MEGA_BYTE,
                    aggStats.m_maxUsedSize / MEGA_BYTE,
                    aggStats.m_maxRealUsedSize / MEGA_BYTE);
                MOT_LOG(logLevel,
                    "Session %s [Util]: Int. %0.2f%%, Ext. %0.2f%%, Max Ext. %0.2f%%",
                    name,
                    memUtilInternal,
                    memUtilExternal,
                    maxUtilExternal);
            }
        }
    }
}
}  // namespace MOT

extern "C" void MemSessionApiDump()
{
    MOT::StringBufferApply([](MOT::StringBuffer* stringBuffer) {
        MOT::MemSessionApiToString(0, "Debug Dump", stringBuffer, MOT::MEM_REPORT_DETAILED);
        fprintf(stderr, "%s", stringBuffer->m_buffer);
        fflush(stderr);
    });
}

extern "C" int MemSessionApiAnalyze(void* buffer)
{
    for (uint32_t i = 0; i < MOT::g_memGlobalCfg.m_maxConnectionCount; ++i) {
        fprintf(stderr, "Searching buffer %p in session allocator %u...\n", buffer, i);
        if (MOT::g_sessionAllocators[i]) {
            if (MemSessionAllocatorAnalyze(MOT::g_sessionAllocators[i], buffer)) {
                return 1;
            }
        }
    }
    return 0;
}
