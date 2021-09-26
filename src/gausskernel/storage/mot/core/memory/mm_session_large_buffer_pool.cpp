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
 * mm_session_large_buffer_pool.cpp
 *    A pool of buffers of varying sizes for session large allocations.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/memory/mm_session_large_buffer_pool.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "mm_session_large_buffer_pool.h"
#include "utilities.h"
#include "mm_numa.h"
#include "mot_error.h"
#include "mm_virtual_huge_chunk.h"
#include "mm_raw_chunk_dir.h"

// 1. a pool is defined for each NUMA node. The size of each node pool is decided by dividing the
//    configured limit by the number of nodes.
// 2. each pool has a set of buffers in increasing sizes (2, 4, 8, 16, ...) up to configured limit.
// 3. The maximum buffer size is determined by the actual pool size as follows:
//    We divided half of the pool size by 4: this is the upper bound for buffer size.
//    If this upper bound is large enough then we divide the pool according to the largest configured size.
//    If this upper bound is not large enough then we update the actual upper bound and issue a warning.
// 4. Internal pool division is as follows:
//      - Half of the pool is divided for largest buffers
//      - Quarter of the pool is divided for half largest buffers
//      - 1/8 of the pool is divided for 1/4 largest buffers
//      and so on until we reach the smallest size 2MB which uses the rest of the space
// 5. Buffers are recorded through the virtual huge chunks mechanism
// 6. Each pool buffer list is managed by a header that contains the following data:
//      - buffer list lock
//      - buffer size
//      - max buffer count
//      - start address of pool buffer list
//      - allocated buffer count
//      - a bitset array to record which buffers are allocated
namespace MOT {
DECLARE_LOGGER(SessionLargeBufferPool, Memory)

// helper functions
static int InitPoolHeader(
    MemSessionLargeBufferPool* bufferPool, int node, uint64_t poolSizeBytes, uint64_t maxObjectSizeBytes);
static int RegisterPoolHeader(MemSessionLargeBufferPool* bufferPool);
static int InitPoolLists(MemSessionLargeBufferPool* bufferPool);
static uint32_t ComputeBufferListCount(uint64_t maxObjectSizeBytes);
static uint32_t ComputeBufferListIndex(uint64_t objectSizeBytes);
static void MemSessionRecordLargeBuffer(
    MemSessionLargeBufferHeader* bufferHeader, MemSessionLargeBufferHeader** bufferHeaderList);
static void MemSessionUnrecordLargeBuffer(
    MemSessionLargeBufferHeader* bufferHeader, MemSessionLargeBufferHeader** bufferHeaderList);

extern int MemSessionLargeBufferPoolInit(
    MemSessionLargeBufferPool* bufferPool, int16_t node, uint64_t poolSizeBytes, uint64_t maxObjectSizeBytes)
{
    int result = InitPoolHeader(bufferPool, node, poolSizeBytes, maxObjectSizeBytes);
    if (result == 0) {
        // register the pool as a virtual chunk with session pool type
        result = RegisterPoolHeader(bufferPool);
        if (result == 0) {
            result = InitPoolLists(bufferPool);
            if (result == 0) {
                MOT_LOG_TRACE(
                    "End of buffer pool init, buffer pool ptr is %p --> %p", bufferPool, bufferPool->m_bufferPool);
            }
        }
    }

    if (result != 0) {
        // safe cleanup in case of failure
        MemSessionLargeBufferPoolDestroy(bufferPool);
    }

    return result;
}

extern void MemSessionLargeBufferPoolDestroy(MemSessionLargeBufferPool* bufferPool)
{
    // release buffer lists
    MOT_LOG_TRACE("Destroying session large buffer pool for node %d: %p --> %p",
        bufferPool->m_node,
        bufferPool,
        bufferPool->m_bufferPool);
    if (bufferPool->m_bufferLists != nullptr) {
        for (uint32_t i = 0; i < bufferPool->m_bufferListCount; ++i) {
            if (bufferPool->m_bufferLists[i] != nullptr) {
                uint32_t allocSize = MemSessionLargeBufferListGetSize(
                    bufferPool->m_bufferLists[i]->m_bufferSize, bufferPool->m_bufferLists[i]->m_maxBufferCount);
                MemSessionLargeBufferListDestroy(bufferPool->m_bufferLists[i]);
                MemNumaFreeLocal(bufferPool->m_bufferLists[i], allocSize, bufferPool->m_node);
                bufferPool->m_bufferLists[i] = nullptr;
            }
        }
        MemNumaFreeLocal(bufferPool->m_bufferLists,
            sizeof(MemSessionLargeBufferList*) * bufferPool->m_bufferListCount,
            bufferPool->m_node);
        bufferPool->m_bufferLists = nullptr;
    }

    // release the entire pool
    // also get the virtual chunk header and clear the chunk directory
    if (bufferPool->m_bufferPool != nullptr) {
        MOT_LOG_TRACE("Searching for virtual chunk header for buffer pool %p", bufferPool->m_bufferPool);
        MemVirtualHugeChunkHeader* chunkHeader =
            (MemVirtualHugeChunkHeader*)MemRawChunkDirLookup(bufferPool->m_bufferPool);
        if (chunkHeader == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                "Session Large Buffer Pool Destruction",
                "Failed to find virtual chunk header for buffer pool %p",
                bufferPool->m_bufferPool);
        } else {
            MOT_LOG_TRACE("Found virtual chunk header at %p: chunk type is %s",
                chunkHeader,
                MemChunkTypeToString(chunkHeader->m_chunkType));
            uint32_t chunkCount = bufferPool->m_poolSizeBytes / (MEM_CHUNK_SIZE_MB * MEGA_BYTE);
            MOT_LOG_TRACE("Removing buffer pool %p with %u chunks from raw chunk dir: pointing to virtual header %p",
                bufferPool->m_bufferPool,
                chunkCount,
                chunkHeader);
            MemRawChunkDirRemoveEx(bufferPool->m_bufferPool, chunkCount);
            MemNumaFreeLocal(bufferPool->m_bufferPool, bufferPool->m_poolSizeBytes, bufferPool->m_node);
            MemVirtualHugeChunkHeaderFree(chunkHeader);
            bufferPool->m_bufferPool = nullptr;
        }
    }
}

extern void* MemSessionLargeBufferPoolAlloc(
    MemSessionLargeBufferPool* bufferPool, uint64_t sizeBytes, MemSessionLargeBufferHeader** bufferHeaderList)
{
    void* buffer = nullptr;
    uint32_t bufferListIndex = ComputeBufferListIndex(sizeBytes);
    if (bufferListIndex >= bufferPool->m_bufferListCount) {
        // silent warning, since huge allocation in next in line
        // we behave this way because the actual maximum object size can be smaller than configured
        MOT_LOG_TRACE("Cannot allocate buffer in size of %" PRIu64
                      " bytes in session buffer pool: size too large (maximum is %" PRIu64 ")",
            sizeBytes,
            bufferPool->m_maxObjectSizeBytes);
    } else {
        MOT_LOG_DEBUG("Allocating size %" PRIu64 " from list %u of size %" PRIu64,
            sizeBytes,
            bufferListIndex,
            bufferPool->m_bufferLists[bufferListIndex]->m_bufferSize);
        MemSessionLargeBufferHeader* bufferHeader =
            MemSessionLargeBufferListAlloc(bufferPool->m_bufferLists[bufferListIndex], sizeBytes);
        if (bufferHeader == nullptr) {
            MOT_LOG_TRACE("Failed to allocate large buffer: buffer list is depleted");
        } else {
            MemSessionRecordLargeBuffer(bufferHeader, bufferHeaderList);
            buffer = bufferHeader->m_buffer;
            MOT_LOG_DEBUG("Allocated large buffer %p of size %" PRIu64 " (real size %" PRIu64 ")",
                buffer,
                sizeBytes,
                bufferPool->m_bufferLists[bufferListIndex]->m_bufferSize);
        }
    }
    return buffer;
}

extern void MemSessionLargeBufferPoolFree(
    MemSessionLargeBufferPool* bufferPool, void* buffer, MemSessionLargeBufferHeader** bufferHeaderList)
{
    // locate the proper list
    MOT_LOG_DEBUG("Freeing large buffer %p", buffer);
    for (uint32_t i = 0; i < bufferPool->m_bufferListCount; ++i) {
        int bufferIndex = MemSessionLargeBufferListGetIndex(bufferPool->m_bufferLists[i], buffer);
        if (bufferIndex != -1) {
            MOT_LOG_DEBUG("Freeing large buffer %p into buffer list %u (real size %u)",
                buffer,
                i,
                bufferPool->m_bufferLists[i]->m_bufferSize);
            MemSessionLargeBufferHeader* bufferHeader =
                (MemSessionLargeBufferHeader*)(bufferPool->m_bufferLists[i]->m_bufferHeaderList + bufferIndex);
            MemSessionUnrecordLargeBuffer(bufferHeader, bufferHeaderList);
            MemSessionLargeBufferListFree(bufferPool->m_bufferLists[i], bufferHeader);
            break;
        }
    }
}

extern void* MemSessionLargeBufferPoolRealloc(MemSessionLargeBufferPool* bufferPool, void* buffer,
    uint64_t newSizeBytes, MemReallocFlags flags, MemSessionLargeBufferHeader** bufferHeaderList)
{
    errno_t erc;
    // locate the proper list to see if the size still fits
    void* newBuffer = nullptr;
    MOT_LOG_DEBUG("Reallocating large buffer %p: new size is %" PRIu64, buffer, newSizeBytes);
    for (uint32_t i = 0; i < bufferPool->m_bufferListCount; ++i) {
        int bufferIndex = MemSessionLargeBufferListGetIndex(bufferPool->m_bufferLists[i], buffer);
        if (bufferIndex != -1) {
            MOT_LOG_DEBUG("Found large buffer %p in buffer list %u", buffer, i);
            MemSessionLargeBufferHeader* bufferHeader =
                (MemSessionLargeBufferHeader*)(bufferPool->m_bufferLists[i]->m_bufferHeaderList +
                                               (uint32_t)bufferIndex);
            if (bufferPool->m_bufferLists[i]->m_bufferSize >= newSizeBytes) {
                // new size fits in existing buffer, so just update real size, and return current buffer
                bufferHeader->m_realObjectSize = newSizeBytes;
                newBuffer = buffer;
                MOT_LOG_DEBUG("Large buffer %p is big enough for reallocation", buffer);
            } else {
                // new size does not fit in existing buffer, so allocate new buffer, and copy/zero data if required and
                // free old buffer
                MOT_LOG_DEBUG("Large buffer %p is not big enough for reallocation", buffer);
                newBuffer = MemSessionLargeBufferPoolAlloc(bufferPool, newSizeBytes, bufferHeaderList);
                if (newBuffer != nullptr) {
                    MOT_LOG_DEBUG("Reallocated buffer %p into %p", buffer, newBuffer);
                    uint64_t objectSizeBytes = bufferHeader->m_realObjectSize;
                    if (flags == MEM_REALLOC_COPY) {
                        // attention: new size may be smaller than object size
                        erc = memcpy_s(newBuffer, newSizeBytes, buffer, std::min(newSizeBytes, objectSizeBytes));
                        securec_check(erc, "\0", "\0");
                    } else if (flags == MEM_REALLOC_ZERO) {
                        erc = memset_s(newBuffer, newSizeBytes, 0, newSizeBytes);
                        securec_check(erc, "\0", "\0");
                    } else if (flags == MEM_REALLOC_COPY_ZERO) {
                        // attention: new size may be smaller than object size
                        erc = memcpy_s(newBuffer, newSizeBytes, buffer, std::min(newSizeBytes, objectSizeBytes));
                        securec_check(erc, "\0", "\0");
                        if (newSizeBytes > objectSizeBytes) {
                            erc = memset_s(((char*)newBuffer) + objectSizeBytes,
                                newSizeBytes - objectSizeBytes,
                                0,
                                newSizeBytes - objectSizeBytes);
                            securec_check(erc, "\0", "\0");
                        }
                    }
                    MemSessionLargeBufferPoolFree(bufferPool, buffer, bufferHeaderList);
                } else {
                    MOT_LOG_TRACE("Failed to reallocate buffer %p to size %" PRIu64 ": buffer pool %u of size %" PRIu64
                                  " is depleted",
                        buffer,
                        newSizeBytes,
                        i,
                        bufferPool->m_bufferLists[i]->m_bufferSize);
                    // since we have a fallback to reallocation from huge buffers we do not report errors now
                }
            }
        }
    }
    return newBuffer;
}

static void MemSessionRecordLargeBuffer(
    MemSessionLargeBufferHeader* bufferHeader, MemSessionLargeBufferHeader** bufferHeaderList)
{
    bufferHeader->m_next = *bufferHeaderList;
    bufferHeader->m_prev = nullptr;
    if ((*bufferHeaderList) != nullptr) {
        (*bufferHeaderList)->m_prev = bufferHeader;
    }
    *bufferHeaderList = bufferHeader;
}

static void MemSessionUnrecordLargeBuffer(
    MemSessionLargeBufferHeader* bufferHeader, MemSessionLargeBufferHeader** bufferHeaderList)
{
    if (bufferHeader->m_prev == nullptr) {
        *bufferHeaderList = bufferHeader->m_next;
        if (bufferHeader->m_next != nullptr) {
            bufferHeader->m_next->m_prev = nullptr;
        }
    } else if (bufferHeader->m_next == nullptr) {
        if (bufferHeader->m_prev != nullptr) {
            bufferHeader->m_prev->m_next = nullptr;
        }
    } else {
        bufferHeader->m_prev->m_next = bufferHeader->m_next;
        bufferHeader->m_next->m_prev = bufferHeader->m_prev;
    }
}

extern uint64_t MemSessionLargeBufferPoolGetObjectSize(MemSessionLargeBufferPool* bufferPool, void* buffer)
{
    uint64_t result = 0;
    for (uint32_t i = 0; i < bufferPool->m_bufferListCount; ++i) {
        int bufferIndex = MemSessionLargeBufferListGetIndex(bufferPool->m_bufferLists[i], buffer);
        if (bufferIndex != -1) {
            result = MemSessionLargeBufferListGetRealObjectSize(bufferPool->m_bufferLists[i], buffer);
            break;
        }
    }
    return result;
}

extern void MemSessionLargeBufferPoolGetStats(MemSessionLargeBufferPool* bufferPool, MemSessionLargeBufferStats* stats)
{
    for (uint32_t i = 0; i < bufferPool->m_bufferListCount; ++i) {
        MemSessionLargeBufferListGetStats(bufferPool->m_bufferLists[i], stats);
    }
}

extern void MemSessionLargeBufferPoolPrint(const char* name, LogLevel logLevel, MemSessionLargeBufferPool* bufferPool,
    MemReportMode reportMode /* = MEM_REPORT_SUMMARY */)
{
    if (MOT_CHECK_LOG_LEVEL(logLevel)) {
        StringBufferApply([name, logLevel, bufferPool, reportMode](StringBuffer* stringBuffer) {
            MemSessionLargeBufferPoolToString(0, name, bufferPool, stringBuffer, reportMode);
            MOT_LOG(logLevel, "%s", stringBuffer->m_buffer);
        });
    }
}

extern void MemSessionLargeBufferPoolToString(int indent, const char* name, MemSessionLargeBufferPool* bufferPool,
    StringBuffer* stringBuffer, MemReportMode reportMode /* = MEM_REPORT_SUMMARY */)
{
    errno_t erc;
    StringBufferAppend(stringBuffer,
        "%*sSession Large Buffer Pool [node=%u, list-count=%u] %s:\n",
        indent,
        "",
        (unsigned)bufferPool->m_node,
        bufferPool->m_bufferListCount,
        name);
    if (reportMode == MEM_REPORT_DETAILED) {
        StringBufferAppend(stringBuffer,
            "%*sPool Size: %" PRIu64 " bytes\n",
            indent + PRINT_REPORT_INDENT,
            "",
            bufferPool->m_poolSizeBytes);
        StringBufferAppend(stringBuffer,
            "%*sMaximum Object Size: %" PRIu64 " bytes\n",
            indent + PRINT_REPORT_INDENT,
            "",
            bufferPool->m_maxObjectSizeBytes);
    }
    for (uint32_t i = 0; i < bufferPool->m_bufferListCount; ++i) {
        const uint32_t nameLen = 32;
        char listName[nameLen];
        erc = snprintf_s(listName, nameLen, nameLen - 1, "List[%u]", i);
        securec_check_ss(erc, "\0", "\0");
        MemSessionLargeBufferListToString(
            indent + PRINT_REPORT_INDENT, listName, bufferPool->m_bufferLists[i], stringBuffer, reportMode);
    }
}

static int InitPoolHeader(
    MemSessionLargeBufferPool* bufferPool, int node, uint64_t poolSizeBytes, uint64_t maxObjectSizeBytes)
{
    int result = 0;
    MOT_LOG_TRACE("Initializing session large buffer pool for node %d: pool-size=%" PRIu64 ", max-object-size=%" PRIu64,
        node,
        poolSizeBytes,
        maxObjectSizeBytes);
    errno_t erc = memset_s(bufferPool,
        sizeof(MemSessionLargeBufferPool),
        0,
        sizeof(MemSessionLargeBufferPool));  // for safe shutdown in case of init failure
    securec_check(erc, "\0", "\0");
    bufferPool->m_node = node;
    bufferPool->m_poolSizeBytes = poolSizeBytes;
    bufferPool->m_maxObjectSizeBytes = maxObjectSizeBytes;
    bufferPool->m_bufferPool = MemNumaAllocAlignedLocal(poolSizeBytes, MEM_CHUNK_SIZE_MB * MEGA_BYTE, node);
    if (unlikely(bufferPool->m_bufferPool == nullptr)) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Session Large Buffer Pool Allocation",
            "Failed to allocate %" PRIu64 " bytes on node %d for session large buffer pool",
            poolSizeBytes,
            node);
        result = MOT_ERROR_OOM;
    }
    return result;
}

static int RegisterPoolHeader(MemSessionLargeBufferPool* bufferPool)
{
    int result = 0;
    MemVirtualHugeChunkHeader* chunkHeader = MemVirtualHugeChunkHeaderAlloc(
        bufferPool->m_poolSizeBytes, bufferPool->m_poolSizeBytes, (int16_t)bufferPool->m_node, MEM_ALLOC_LOCAL);
    if (chunkHeader == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Session Large Buffer Pool Allocation",
            "Failed to allocate virtual huge chunk header for session large buffer pool on node %d",
            bufferPool->m_node);
        result = GetLastError();
    } else {
        chunkHeader->m_chunkType = MEM_CHUNK_TYPE_SESSION_LARGE_BUFFER_POOL;  // replace chunk type
        chunkHeader->m_chunk = bufferPool->m_bufferPool;
        uint32_t chunkCount = bufferPool->m_poolSizeBytes / (MEM_CHUNK_SIZE_MB * MEGA_BYTE);
        MemRawChunkDirInsertEx(bufferPool->m_bufferPool, chunkCount, chunkHeader);
        MOT_LOG_TRACE("Inserted buffer pool %p with %u chunks to raw chunk dir: pointing to virtual header %p",
            bufferPool->m_bufferPool,
            chunkCount,
            chunkHeader);
    }
    return result;
}

static int InitPoolLists(MemSessionLargeBufferPool* bufferPool)
{
    int result = 0;

    // allocate array of lists
    bufferPool->m_bufferListCount = ComputeBufferListCount(bufferPool->m_maxObjectSizeBytes);
    MOT_LOG_TRACE("Found %u buffer lists according to max object size %" PRIu64,
        bufferPool->m_bufferListCount,
        bufferPool->m_maxObjectSizeBytes);
    uint64_t allocSize = sizeof(MemSessionLargeBufferList*) * bufferPool->m_bufferListCount;
    bufferPool->m_bufferLists = (MemSessionLargeBufferList**)MemNumaAllocLocal(allocSize, bufferPool->m_node);
    if (unlikely(bufferPool->m_bufferLists == nullptr)) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Session Large Buffer Pool Initialization",
            "Failed to allocate %" PRIu64 " bytes for session large list array on node %d",
            allocSize,
            bufferPool->m_node);
        result = MOT_ERROR_OOM;
    } else {
        // now initialize each list
        uint64_t bufferSize = bufferPool->m_maxObjectSizeBytes;
        uint64_t totalSize = 0;
        uint64_t listSize = bufferPool->m_poolSizeBytes / 2;
        uint64_t bufferCount = listSize / bufferSize;
        uint8_t* listAddress = (uint8_t*)bufferPool->m_bufferPool;
        for (int i = (int)bufferPool->m_bufferListCount - 1; i >= 0; --i) {
            MOT_LOG_TRACE("Allocating buffer list %u: list-offset=%p, list-size=%u, buffer-size=%u, buffer-count=%u",
                i,
                listAddress,
                (unsigned)listSize,
                (unsigned)bufferSize,
                (unsigned)bufferCount);
            allocSize = MemSessionLargeBufferListGetSize(bufferSize, bufferCount);
            bufferPool->m_bufferLists[i] = (MemSessionLargeBufferList*)MemNumaAllocLocal(allocSize, bufferPool->m_node);
            if (unlikely(bufferPool->m_bufferLists[i] == nullptr)) {
                MOT_REPORT_ERROR(MOT_ERROR_OOM,
                    "Session Large Buffer Pool Initialization",
                    "Failed to allocate %" PRIu64 " bytes for session large list %d on node %d",
                    allocSize,
                    i,
                    bufferPool->m_node);
                result = MOT_ERROR_OOM;
                break;
            }
            result = MemSessionLargeBufferListInit(bufferPool->m_bufferLists[i], listAddress, bufferSize, bufferCount);
            if (result != 0) {
                MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                    "Session Large Buffer Pool Initialization",
                    "Failed to initialize session large buffer list %d on node %d",
                    i,
                    bufferPool->m_node);
                // make sure destroy for this list is not called
                MemNumaFreeLocal(bufferPool->m_bufferLists[i], allocSize, bufferPool->m_node);
                bufferPool->m_bufferLists[i] = nullptr;
                break;
            }

            // compute next list address and total size
            listAddress += listSize;
            totalSize += listSize;
            if (i > 1) {
                listSize /= 2;
            } else {
                // take what is left for the last pool of 2 MB buffers
                listSize = bufferPool->m_poolSizeBytes - totalSize;
            }
            // compute next buffer size and number of buffers in next list
            bufferSize /= 2;
            bufferCount = listSize / bufferSize;
        }
    }

    return result;
}

static uint32_t ComputeBufferListCount(uint64_t maxObjectSizeBytes)
{
    // the input parameter is expected to be a full power of 2 so we compute the buffer list count as follows:
    // 2 MB --> 1
    // 4 MB --> 2
    // 8 MB --> 3
    // so we shift right 20 digits (divide by 1 MB)
    // then we take a log in base 2 as follows: the number of trailing zeroes is the power of 2
    return __builtin_ctzll(maxObjectSizeBytes >> 20);
}

static inline bool isPow2(uint64_t objectSizeBytes)
{
    // the number has exactly one bit raised
    return ((__builtin_clzll(objectSizeBytes) + __builtin_ctzll(objectSizeBytes)) == 63);
}

static uint32_t ComputeBufferListIndex(uint64_t objectSizeBytes)
{
    // find the closest power of two: this is indicated by the number of leading zeros
    // the inverse is the number of active digits (including non-leading zeros)
    // 1 digit --> 2 (list index 0)
    // 2 digits --> 4 (list index 0)
    // 3 digits --> 8 (list index 0)
    // ...
    // 21 digits --> 2^21 (list index 0)
    // 22 digits --> 2^22 (list index 1)
    int result = 0;
    uint32_t digitCount = 64 - __builtin_clzll(objectSizeBytes);
    MOT_LOG_DEBUG("ComputeBufferListIndex: digitCount = %u", digitCount);
    if (digitCount > 21) {
        result = digitCount - 21;
    }
    MOT_LOG_DEBUG("Buffer list for size %" PRIu64 " is %u", objectSizeBytes, result);
    return result;
}
}  // namespace MOT

extern "C" void MemSessionLargeBufferPoolDump(void* arg)
{
    MOT::MemSessionLargeBufferPool* pool = (MOT::MemSessionLargeBufferPool*)arg;

    MOT::StringBufferApply([pool](MOT::StringBuffer* stringBuffer) {
        MOT::MemSessionLargeBufferPoolToString(0, "Debug Dump", pool, stringBuffer, MOT::MEM_REPORT_DETAILED);
        fprintf(stderr, "%s", stringBuffer->m_buffer);
        fflush(stderr);
    });
}

extern "C" int MemSessionLargeBufferPoolAnalyze(void* pool, void* buffer)
{
    int found = 0;
    MOT::MemSessionLargeBufferPool* bufferPool = (MOT::MemSessionLargeBufferPool*)pool;
    for (uint32_t i = 0; i < bufferPool->m_bufferListCount; ++i) {
        if (MemSessionLargeBufferListAnalyze(bufferPool->m_bufferLists[i], buffer)) {
            fprintf(
                stderr, "Buffer %p found in session large buffer list %u on node %d\n", buffer, i, bufferPool->m_node);
            found = 1;
            break;
        }
    }
    return found;
}
