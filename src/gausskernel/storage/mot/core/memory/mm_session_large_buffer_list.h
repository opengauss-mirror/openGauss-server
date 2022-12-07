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
 * mm_session_large_buffer_list.h
 *    A list of large buffers used for session allocations.
 *    A single list may be used by all sessions running on the same NUMA node.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/memory/mm_session_large_buffer_list.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef MM_SESSION_LARGE_BUFFER_LIST_H
#define MM_SESSION_LARGE_BUFFER_LIST_H

#include "mm_lock.h"
#include "mm_def.h"
#include "utilities.h"
#include "string_buffer.h"

#include <cstring>

namespace MOT {
/**
 * @struct MemSessionLargeBufferHeader
 * @brief A large buffer header that stores some information about the large buffer it points to.
 */
struct PACKED MemSessionLargeBufferHeader {
    /** @var The real block of memory which the buffer header points to. */
    void* m_buffer;  // L1 offset [0-8]

    /** @var When the buffer which this buffer points to has been allocated,
     * the _next pointer here points to the next buffer header in the _large_buffer_list of session allocator. */
    MemSessionLargeBufferHeader* m_next;  // L1 offset [8-16]

    /** @var When the buffer which this buffer points to has been allocated,
     * the _prev pointer here points to the previous buffer header in the _large_buffer_list of session allocator. */
    MemSessionLargeBufferHeader* m_prev;  // L1 offset [16-24]

    /** @var The real object size that the consumer requested for. */
    uint64_t m_realObjectSize;  // L1 offset [24-32]
};

/**
 * @struct MemSessionLargeBufferList
 * @brief A list of large buffers used for session allocations a single list may be used by all sessions running on the
 * same NUMA node.
 */
struct PACKED MemSessionLargeBufferList {
    /** @var The buffer list lock. */
    MemLock m_lock;  // L1 offset [0-64]

    /** @var The size in bytes of each buffer in the list. */
    uint64_t m_bufferSize;  // L1 offset [0-8]

    /** @var The total number of bytes requested by the user. */
    uint64_t m_requestedBytes;  // L1 offset [8-16]

    /** @var The maximum number of buffers in the list/ */
    uint32_t m_maxBufferCount;  // L1 offset [16-20]

    /** @var The current number of buffers allocated to the application. */
    uint32_t m_allocatedCount;  // L1 offset [20-24]

    /** @var The number of words in the free bit-set array. */
    uint32_t m_freeBitsetCount;  // L1 offset [24-28]

    /** @var Align next member offset to 8 bytes. */
    uint32_t m_padding;  // L1 offset [28-32]

    /** @var The starting address of the buffer containing the flat buffer list. */
    void* m_bufferList;  // L1 offset [32-40]

    /** @var An array of buffer header points to all buffers in the m_bufferList respectively. */
    MemSessionLargeBufferHeader* m_bufferHeaderList;  // L1 offset [40-48]

    /**
     * @var A bit-set array denoting which buffers are free in this buffer list. A value of "1" means
     * "free" and a value of "0" means "allocated".
     */
    uint64_t m_freeBitset[0];  // L1 offset [48-...]
};

/** @struct MemSessionLargeBufferStats */
struct PACKED MemSessionLargeBufferStats {
    /** @var The total number of bytes requested by the user. */
    uint64_t m_requestedBytes;

    /** @var The total number of bytes given to the user. */
    uint64_t m_allocatedBytes;
};

/**
 * @brief Computes the size required for a session large buffer list.
 * @param bufferSize The size in bytes of each buffer in the buffer list.
 * @param bufferCount The number of buffers in the buffer list.
 * @return The size in bytes required for the session large buffer list.
 */
inline uint32_t MemSessionLargeBufferListGetSize(uint64_t bufferSize, uint64_t bufferCount)
{
    uint32_t bitsetCount = (bufferCount + sizeof(uint64_t) - 1) / sizeof(uint64_t);
    uint32_t size = sizeof(MemSessionLargeBufferList) +                 // header
                    bitsetCount * sizeof(uint64_t) +                    // bit-set array
                    bufferCount * sizeof(MemSessionLargeBufferHeader);  // buffer header list
    return size;
}

/**
 * @brief Initializes a session large buffer list.
 * @param sessionBufferList The list to initialize. The list object must be large enough to
 * accommodate for the bit-set array.
 * @param bufferList The flat list of buffers.
 * @param bufferSize The size of each buffer in the buffer list.
 * @param bufferCount The number of buffers in the buffer list.
 * @return Zero if succeeded, otherwise an error code.
 */
extern int MemSessionLargeBufferListInit(
    MemSessionLargeBufferList* sessionBufferList, void* bufferList, uint64_t bufferSize, uint64_t bufferCount);

/**
 * @brief Releases all resources associated with a session large buffer list.
 * @param sessionBufferList The buffer list to destroy.
 */
inline void MemSessionLargeBufferListDestroy(MemSessionLargeBufferList* sessionBufferList)
{
    (void)MemLockDestroy(&sessionBufferList->m_lock);
}

/**
 * @brief Handles invalid or double buffer free. Dumps all relevant information and aborts.
 * @param sessionBufferList The faulting session large buffer list.
 * @param buffer The faulting buffer.
 * @param bufferIndex The faulting buffer index.
 * @param invalidType "INVALID" or "DOUBLE"
 */
extern void MemSessionLargeBufferListOnInvalidFree(
    MemSessionLargeBufferList* sessionBufferList, void* buffer, uint64_t bufferIndex, const char* invalidType);

/**
 * @brief Allocates a buffer from a session large buffer list.
 * @param sessionBufferList The session large buffer list.
 * @param realObjectSize The real object size (to be recorded for later query).
 * @return The allocated buffer.
 */
inline MemSessionLargeBufferHeader* MemSessionLargeBufferListAlloc(
    MemSessionLargeBufferList* sessionBufferList, uint64_t realObjectSize)
{
    MemSessionLargeBufferHeader* bufferHeader = nullptr;
    MemLockAcquire(&sessionBufferList->m_lock);
    if (sessionBufferList->m_allocatedCount < sessionBufferList->m_maxBufferCount) {
        for (uint32_t i = 0; i < sessionBufferList->m_freeBitsetCount; ++i) {
            if (sessionBufferList->m_freeBitset[i] != 0) {
                uint64_t bitIndex = __builtin_clzll(sessionBufferList->m_freeBitset[i]);
                uint64_t freeBufferIndex = ((uint64_t)i << 6) + bitIndex;
                if (freeBufferIndex <
                    sessionBufferList->m_maxBufferCount) {  // guard against extreme case: max_buffer_count is not a
                                                            // multiple of 64
                    sessionBufferList->m_freeBitset[i] &= ~(((uint64_t)1) << (63 - bitIndex));
                    ++sessionBufferList->m_allocatedCount;
                    sessionBufferList->m_requestedBytes += realObjectSize;

                    // buffer for MOT_ASSERT below
#ifdef MOT_DEBUG
                    void* buffer = ((uint8_t*)sessionBufferList->m_bufferList) +       // beginning offset
                                   sessionBufferList->m_bufferSize * freeBufferIndex;  // size * index
#endif

                    // update buffer header
                    bufferHeader =
                        (MemSessionLargeBufferHeader*)(sessionBufferList->m_bufferHeaderList + freeBufferIndex);
                    MOT_ASSERT(bufferHeader->m_buffer == buffer);
                    bufferHeader->m_next = nullptr;
                    bufferHeader->m_prev = nullptr;
                    bufferHeader->m_realObjectSize = realObjectSize;
                }
                break;
            }
        }
    }
    MemLockRelease(&sessionBufferList->m_lock);
    return bufferHeader;
}

/**
 * @brief Returns a buffer to the session large buffer list.
 * @param sessionBufferList The session large buffer list.
 * @param bufferHeader The buffer to free.
 */
inline void MemSessionLargeBufferListFree(
    MemSessionLargeBufferList* sessionBufferList, MemSessionLargeBufferHeader* bufferHeader)
{
    void* buffer = bufferHeader->m_buffer;
    uint64_t bufferOffset = (uint64_t)(((uint8_t*)buffer) - ((uint8_t*)sessionBufferList->m_bufferList));
    uint64_t bufferIndex = bufferOffset / sessionBufferList->m_bufferSize;
    if (bufferIndex >= sessionBufferList->m_maxBufferCount) {
        MemSessionLargeBufferListOnInvalidFree(sessionBufferList, buffer, bufferIndex, "INVALID");
    }
    uint64_t slot = bufferIndex / 64;
    uint32_t index = bufferIndex % 64;
    if (slot >= sessionBufferList->m_freeBitsetCount) {
        MemSessionLargeBufferListOnInvalidFree(sessionBufferList, buffer, bufferIndex, "INVALID");
    }
    MemLockAcquire(&sessionBufferList->m_lock);
    if (sessionBufferList->m_freeBitset[slot] & (((uint64_t)1) << (63 - index))) {
        MemSessionLargeBufferListOnInvalidFree(sessionBufferList, buffer, bufferIndex, "DOUBLE");
    }
    sessionBufferList->m_freeBitset[slot] |= (((uint64_t)1) << (63 - index));
    MOT_ASSERT(sessionBufferList->m_allocatedCount > 0);
    --sessionBufferList->m_allocatedCount;
    sessionBufferList->m_requestedBytes -= bufferHeader->m_realObjectSize;

    // update buffer header due to buffer free.
    MOT_ASSERT(
        buffer == (void*)((uint8_t*)sessionBufferList->m_bufferList + bufferIndex * (sessionBufferList->m_bufferSize)));
    bufferHeader->m_next = nullptr;
    bufferHeader->m_prev = nullptr;
    bufferHeader->m_realObjectSize = 0;
    MemLockRelease(&sessionBufferList->m_lock);
}

/**
 * @brief Queries whether a buffer belongs to a session large buffer list.
 * @param sessionBufferList The session large buffer list.
 * @param bufferHeader The buffer to check.
 * @return buffer index if the buffer belongs to the session large buffer list. Otherwise -1.
 */
inline int MemSessionLargeBufferListGetIndex(MemSessionLargeBufferList* sessionBufferList, void* buffer)
{
    int result = -1;
    if (((uint8_t*)buffer) >= ((uint8_t*)sessionBufferList->m_bufferList)) {
        uint64_t bufferOffset = (uint64_t)(((uint8_t*)buffer) - ((uint8_t*)sessionBufferList->m_bufferList));
        uint64_t bufferIndex = bufferOffset / sessionBufferList->m_bufferSize;
        if (bufferIndex < sessionBufferList->m_maxBufferCount) {
            result = (int)bufferIndex;
        }
    }
    return result;
}

/**
 * @brief Retrieves the real object size of a given allocated buffer in the large buffer list.
 * @param sessionBufferList The session large buffer list.
 * @param bufferHeader The buffer to check.
 * @return The real object size in bytes or zero if buffer is invalid.
 */
inline uint64_t MemSessionLargeBufferListGetRealObjectSize(MemSessionLargeBufferList* sessionBufferList, void* buffer)
{
    uint64_t result = 0;
    if (((uint8_t*)buffer) >= ((uint8_t*)sessionBufferList->m_bufferList)) {
        uint64_t bufferOffset = (uint64_t)(((uint8_t*)buffer) - ((uint8_t*)sessionBufferList->m_bufferList));
        uint64_t bufferIndex = bufferOffset / sessionBufferList->m_bufferSize;
        if (bufferIndex < sessionBufferList->m_maxBufferCount) {
            MemSessionLargeBufferHeader* bufferHeader = (MemSessionLargeBufferHeader*)(sessionBufferList + bufferIndex);
            result = bufferHeader->m_realObjectSize;
        }
    }
    return result;
}

/**
 * @brief Retrieves session large buffer list statistics (thread-safe).
 * @param sessionBufferList The session large buffer list.
 * @param stats The resulting statistics.
 * @note Statistics values are added rather than assigned in order to allow aggregation of multiple buffer list
 * statistics, so caller is responsible for initializing statistics structure members to zero before retrieving session
 * large buffer list statistics.
 */
inline void MemSessionLargeBufferListGetStats(
    MemSessionLargeBufferList* sessionBufferList, MemSessionLargeBufferStats* stats)
{
    MemLockAcquire(&sessionBufferList->m_lock);
    // allow aggregation by adding instead of assigning (caller must initialize statistics structure members)
    stats->m_allocatedBytes += sessionBufferList->m_allocatedCount * sessionBufferList->m_bufferSize;
    stats->m_requestedBytes += sessionBufferList->m_requestedBytes;
    MemLockRelease(&sessionBufferList->m_lock);
}

/**
 * @brief Prints a session large buffer list into log.
 * @param name The name of the session large buffer list to print.
 * @param logLevel The log level to use in printing.
 * @param buffer_pool The session large buffer list to print.
 * @param[opt] reportMode Specifies the report mode.
 */
extern void MemSessionLargeBufferListPrint(const char* name, LogLevel logLevel,
    MemSessionLargeBufferList* sessionBufferList, MemReportMode reportMode = MEM_REPORT_SUMMARY);

/**
 * @brief Dumps a session large buffer list into string buffer.
 * @param indent The indentation level.
 * @param name The name of the session large buffer list to print.
 * @param sessionBufferList The session large buffer list to print.
 * @param stringBuffer The string buffer.
 * @param[opt] reportMode Specifies the report mode.
 */
extern void MemSessionLargeBufferListToString(int indent, const char* name,
    MemSessionLargeBufferList* sessionBufferList, StringBuffer* stringBuffer,
    MemReportMode reportMode = MEM_REPORT_SUMMARY);
}  // namespace MOT

/**
 * @brief Dumps all session large buffer list status to standard error stream.
 */
extern "C" void MemSessionLargeBufferListDump(void* list);

/**
 * @brief Analyzes the memory status of a given buffer address.
 * @param address The buffer to analyze.
 * @return Non-zero value if buffer was found.
 */
extern "C" int MemSessionLargeBufferListAnalyze(void* list, void* buffer);

#endif /* MM_SESSION_LARGE_BUFFER_LIST_H */
