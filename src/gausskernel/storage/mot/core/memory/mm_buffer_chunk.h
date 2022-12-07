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
 * mm_buffer_chunk.h
 *    Memory buffer chunk allocation interfaces.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/memory/mm_buffer_chunk.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef MM_BUFFER_CHUNK_H
#define MM_BUFFER_CHUNK_H

#include "mm_def.h"
#include "mm_chunk_type.h"
#include "mm_buffer_class.h"
#include "mm_buffer_list.h"
#include "mm_raw_chunk_pool.h"
#include "mm_raw_chunk_dir.h"
#include "string_buffer.h"

#include <cstring>

// API for chunk allocation
// Chunk layout is as follows:
// Chunk Header
// Several Buffer Headers
// Padding to a 4KB page
// Buffers
/** @define Maximum number of buffers in a chunk (2048). */
#define MAX_BUFFERS_PER_CHUNK (MEM_CHUNK_SIZE_MB * KILO_BYTE / MEM_MIN_BUFFER_SIZE_KB)

/** @define The maximum number of 64-bit words required to manage the bit-set for free buffers (32). */
#define MAX_BITSET_COUNT ((MAX_BUFFERS_PER_CHUNK + 63) / 64)

namespace MOT {
/**
 * @struct MemBufferChunkHeader
 * @brief Holds all chunk management data. for a chunk of buffers. The header resides in the chunk
 * itself at offset zero. The chunk header is followed by buffer headers of all buffers owned by the
 * chunk. The chunk data itself begins at an offset of one system page. A chunk is in size of 16 MB,
 * and each chunk has an ordinal number. The usable x86 address space is 2^48 bytes and chunks are
 * 2^24, there are in total 2^24 chunks. A table with 16MB entries can serve as a global directory
 * of all chunks.
 */
struct PACKED MemBufferChunkHeader {
    /** @var Chunk type (must be first member to comply with raw chunk). */
    MemChunkType m_chunkType;  // offset [0-4]

    /** @var The node with which the chunk is associated (must be second member to comply with raw chunk). */
    int16_t m_node;  // offset [4-6]

    /** @var A flag specifying whether the chunk data is node-local or global (must be third member to comply with raw
     * chunk). */
    MemAllocType m_allocType;  // offset [6-8]

    /** @var The allocator node identifier with which the buffer is associated (required for free buffer). */
    int16_t m_allocatorNode;  // offset [8-10]

    /** @var The size of class of buffers managed by this heap. */
    MemBufferClass m_bufferClass;  // offset [10-11]

    /** @var Align next member to 4 byte. */
    int8_t m_padding1[1];  // offset [11-12]

    /**
     * @var The size in kilobytes of each buffer in the chunk. All buffers in a specific chunk have
     * the same size.
     */
    uint32_t m_bufferSizeKb;  // offset [12-16]

    /** @var The number of buffers in this chunk. */
    uint32_t m_bufferCount;  // offset [16-20]

    /** @var Align next member to 8 byte. */
    int8_t m_padding2[4];  // offset [20-24]

    /** @var Chunk pointer. We cache this value due to the complexity of its calculation and the use of cache snapshots.
     */
    void* m_chunk;  // offset [24-32]

    /** @var The next chunk with the same amount of allocated buffers. */
    MemBufferChunkHeader* m_nextChunk;  // offset [32-40]

    /** @var The previous chunk with the same amount of allocated buffers. */
    MemBufferChunkHeader* m_prevChunk;  // offset [40-48]

    /** @var The bit-set array size. */
    uint64_t m_freeBitsetCount;  // offset [48-56]

    /** @var Padding to ensure noisy bit-set array lies in a separate cache line. */
    uint8_t m_padding3[8];  // offset [56-64]

    /**
     * @var A bit-set array denoting which buffers are free in this chunk. A value of "1" means
     * "free" and a value of "0" means "allocated". Note that managing a LIFO list of hot buffers is
     * probably useless at this level, so we prefer a high-performing bit-set array instead.
     */
    uint64_t m_freeBitset[MAX_BITSET_COUNT];  // offset [64-320]

    /** @var The number of allocated buffers in this chunk. */
    uint32_t m_allocatedCount;  // offset [320-324]

    /** @var Padding to ensure previous noisy counter lies in a separate cache line. */
    uint8_t m_padding4[60];  // offset [324-384]
};

/**
 * @brief Calculates the maximum number of buffers that a chunk can hold. This calculation is not
 * so straightforward, so we provide a helper function for reuse.
 * @param bufferSizeKb The size of the buffer in kilo-bytes for which to make the calculation.
 * @return The number of buffers of the specified class that can be managed by a single chunk.
 */
extern uint32_t MemBufferChunksMaxBuffers(uint32_t bufferSizeKb);

/**
 * @brief Helper function for issuing errors in in-line functions.
 * @param errorCode The error code to report.
 * @param format The error message format
 * @param[opt] ... Additional arguments for the error message.
 */
extern void MemBufferChunkReportError(int errorCode, const char* format, ...);

/**
 * @brief Initializes a chunk header.
 * @param node The node with which the chunk is associated (source chunk pool).
 * @param allocType Specifies whether the chunk data is node-local or global.
 * @param allocatorNode The allocator node with which the chunk is associated (source allocator).
 * @param bufferClass The class of buffers managed by the chunk.
 */
extern void MemBufferChunkInit(
    MemBufferChunkHeader* chunkHeader, MemAllocType allocType, int16_t allocatorNode, MemBufferClass bufferClass);

/**
 * @brief Allocates a buffer from a chunk
 * @param chunkHeader The chunk header.
 * @return The allocated buffer.
 */
inline MemBufferHeader* MemBufferChunkAllocBuffer(MemBufferChunkHeader* chunkHeader);

/**
 * @brief Returns a buffer to the chunk's free list.
 * @param chunkHeader The chunk header.
 * @param bufferHeader The buffer header to free.
 */
inline void MemBufferChunkFreeBuffer(MemBufferChunkHeader* chunkHeader, MemBufferHeader* bufferHeader);

/**
 * @brief Allocates multiple buffers from a chunk
 * @param chunkHeader The chunk header.
 * @param bufferCount The number of buffers to allocate
 * @param [out] bufferList Received the allocated buffers.
 * @return The actual number of buffers allocated.
 */
inline uint32_t MemBufferChunkAllocMultipleBuffers(
    MemBufferChunkHeader* chunkHeader, uint32_t bufferCount, MemBufferList* bufferList);

/**
 * @brief Marks the chunk as fully in-use.
 * @param chunkHeader The chunk header.
 */
inline void MemBufferChunkMarkFull(MemBufferChunkHeader* chunkHeader);

/**
 * @brief Retrieves the buffer chunk header of a given buffer.
 * @param buffer The buffer to retrieve its buffer chunk header.
 * @return The buffer chunk header, or NULL if an error occurred.
 */
inline MemBufferChunkHeader* MemBufferChunkGetChunkHeader(void* buffer);

/**
 * @brief Retrieves the buffer header of a buffer in a chunk. This function can be called for any
 * address within the usable area of the chunk.
 * @param chunkHeader The chunk header in which to search the buffer header.
 * @param buffer The buffer to search.
 * @return The buffer header or NULL if not found.
 */
inline MemBufferHeader* MemBufferChunkGetBufferHeader(MemBufferChunkHeader* chunkHeader, void* buffer);

/**
 * @brief Queries whether a buffer is allocated.
 * @param chunkHeader The chunk header in which to check the buffer header.
 * @param buffer The buffer to check.
 * @return Non-zero value if the buffer is allocated, otherwise zero.
 */
inline bool MemBufferChunkIsAllocated(MemBufferChunkHeader* chunkHeader, MemBufferHeader* bufferHeader);

/**
 * @brief Queries whether a buffer is free.
 * @param chunkHeader The chunk header in which to check the buffer header.
 * @param buffer The buffer to check.
 * @return Non-zero value if the buffer is free, otherwise zero.
 */
inline bool MemBufferChunkIsFree(MemBufferChunkHeader* chunkHeader, MemBufferHeader* bufferHeader);

/**
 * @brief Handles double buffer free. Dumps all relevant information and aborts.
 * @param chunkHeader The faulting chunk header.
 * @param buffer The faulting buffer header.
 */
extern void MemBufferChunkOnDoubleFree(MemBufferChunkHeader* chunkHeader, MemBufferHeader* bufferHeader);

/**
 * @brief Prints a chunk header into log.
 * @param logLevel The log level to use in printing.
 * @param chunkHeader The chunk header to print.
 */
extern void MemBufferChunkHeaderPrint(LogLevel logLevel, MemBufferChunkHeader* chunkHeader);

/**
 * @brief Dumps a chunk header into string buffer.
 * @param chunkHeader The chunk header to print.
 * @param stringBuffer The string buffer.
 * @param fullReport Specifies whether to report also buffer headers.
 */
extern void MemBufferChunkHeaderToString(
    MemBufferChunkHeader* chunkHeader, StringBuffer* stringBuffer, MemReportMode reportMode = MEM_REPORT_SUMMARY);

/**
 * @brief Dumps a chunk list into string buffer.
 * @param indent The indentation level.
 * @param name The name of the chunk list to print.
 * @param chunk_list The chunk list head to print.
 * @param stringBuffer The string buffer.
 * @param [opt] raw Specifies whether to report raw chunks (just address pointer without chunk details).
 */
extern void MemBufferChunkListToString(
    int indent, const char* name, MemBufferChunkHeader* chunkList, StringBuffer* stringBuffer, int raw = 0);

}  // namespace MOT
#define MM_CHUNK_FULL_HEADER_SIZE(chunkHeader) \
    (sizeof(MemBufferChunkHeader) + (chunkHeader)->m_bufferCount * sizeof(MemBufferHeader))

/** @define Utility macro for getting buffer header pointer from chunk header. */
#define MM_CHUNK_BUFFER_HEADER_AT(chunkHeader, bufferIndex) (((MemBufferHeader*)((chunkHeader) + 1)) + (bufferIndex))

/** @define Utility macro for getting buffer pointer from chunk header. */
#define MM_CHUNK_BUFFER_AT(chunkHeader, bufferIndex) \
    ((void*)(((uint8_t*)(chunkHeader)->m_chunk) + K2B((chunkHeader)->m_bufferSizeKb) * (bufferIndex)))

/** @define Utility macro for getting chunk header pointer from a buffer header. */
#define MM_CHUNK_FROM_BUFFER_HEADER(bufferHeader) \
    (((MemBufferChunkHeader*)((bufferHeader) - (bufferHeader)->m_index)) - 1)

// inline implementation
namespace MOT {
inline MemBufferHeader* MemBufferChunkAllocBuffer(MemBufferChunkHeader* chunkHeader)
{
    MemBufferHeader* bufferHeader = nullptr;
    if (chunkHeader->m_allocatedCount < chunkHeader->m_bufferCount) {  // the number of buffers can be irregular
        for (uint32_t i = 0; i < chunkHeader->m_freeBitsetCount; ++i) {
            if (chunkHeader->m_freeBitset[i] != 0) {
                uint64_t bitIndex = __builtin_clzll(chunkHeader->m_freeBitset[i]);
                uint64_t bufferIndex = ((uint64_t)i << 6) + bitIndex;
                if (bufferIndex < chunkHeader->m_bufferCount) {
                    chunkHeader->m_freeBitset[i] &= ~(((uint64_t)1) << (63 - bitIndex));
                    ++chunkHeader->m_allocatedCount;
                    bufferHeader = MM_CHUNK_BUFFER_HEADER_AT(chunkHeader, bufferIndex);
                }
                break;
            }
        }
    }
    return bufferHeader;
}

inline void MemBufferChunkFreeBuffer(MemBufferChunkHeader* chunkHeader, MemBufferHeader* bufferHeader)
{
    uint32_t slot = bufferHeader->m_index / 64;
    uint32_t index = bufferHeader->m_index % 64;
    if (chunkHeader->m_freeBitset[slot] & (((uint64_t)1) << (63 - index))) {
        MemBufferChunkOnDoubleFree(chunkHeader, bufferHeader);
    }
    chunkHeader->m_freeBitset[slot] |= (((uint64_t)1) << (63 - index));
    MOT_ASSERT(chunkHeader->m_allocatedCount > 0);
    --chunkHeader->m_allocatedCount;
}

inline uint32_t MemBufferChunkAllocMultipleBuffers(
    MemBufferChunkHeader* chunkHeader, uint32_t bufferCount, MemBufferList* bufferList)
{
    uint32_t buffersAllocated = 0;
    while ((chunkHeader->m_allocatedCount < chunkHeader->m_bufferCount) && (buffersAllocated < bufferCount)) {
        MemBufferHeader* bufferHeader = MemBufferChunkAllocBuffer(chunkHeader);
        if (bufferHeader != nullptr) {
            MemBufferListPush(bufferList, bufferHeader);
            ++buffersAllocated;
        } else {
            break;
        }
    }
    return buffersAllocated;
}

inline void MemBufferChunkMarkFull(MemBufferChunkHeader* chunkHeader)
{
    chunkHeader->m_allocatedCount = chunkHeader->m_bufferCount;
    errno_t erc = memset_s(chunkHeader->m_freeBitset,
        sizeof(uint64_t) * chunkHeader->m_freeBitsetCount,
        0,
        sizeof(uint64_t) * chunkHeader->m_freeBitsetCount);
    securec_check(erc, "\0", "\0");
}

inline MemBufferChunkHeader* MemBufferChunkGetChunkHeader(void* buffer)
{
    MemBufferChunkHeader* result = nullptr;
    MemRawChunkHeader* rawChunkHeader = (MemRawChunkHeader*)MemRawChunkDirLookup(buffer);
    if (rawChunkHeader == nullptr) {
        MemBufferChunkReportError(
            MOT_ERROR_INTERNAL, "Attempt to access invalid buffer %p: Source chunk not found", buffer);
    } else if (rawChunkHeader->m_node < 0) {
        MemBufferChunkReportError(
            MOT_ERROR_INTERNAL, "N/A", "Attempt to access invalid buffer %p: Source chunk is corrupt", buffer);
    } else if (rawChunkHeader->m_chunkType != MEM_CHUNK_TYPE_BUFFER) {
        MemBufferChunkReportError(MOT_ERROR_INTERNAL,
            "N/A",
            "Attempt to free invalid global buffer %p: Source chunk type is unexpected (%s)",
            buffer,
            MemChunkTypeToString(rawChunkHeader->m_chunkType));
    } else {
        result = (MemBufferChunkHeader*)rawChunkHeader;
    }
    return result;
}

inline MemBufferHeader* MemBufferChunkGetBufferHeader(MemBufferChunkHeader* chunkHeader, void* buffer)
{
    MemBufferHeader* bufferHeader = nullptr;
    uint8_t* chunkData = (uint8_t*)(chunkHeader->m_chunk);
    uint64_t bufferOffset = ((uint64_t)buffer) - ((uint64_t)chunkData);
    uint64_t bufferSize = chunkHeader->m_bufferSizeKb * KILO_BYTE;
    if ((bufferOffset % bufferSize) != 0) {
        MemBufferChunkReportError(MOT_ERROR_INVALID_ARG, "Invalid buffer address: %p", buffer);
    } else {
        uint64_t bufferIndex = bufferOffset / bufferSize;
        if (bufferIndex < chunkHeader->m_bufferCount) {
            bufferHeader = MM_CHUNK_BUFFER_HEADER_AT(chunkHeader, bufferIndex);
        }
    }
    return bufferHeader;
}

inline bool MemBufferChunkIsAllocated(MemBufferChunkHeader* chunkHeader, MemBufferHeader* bufferHeader)
{
    return !MemBufferChunkIsFree(chunkHeader, bufferHeader);
}

inline bool MemBufferChunkIsFree(MemBufferChunkHeader* chunkHeader, MemBufferHeader* bufferHeader)
{
    bool result = false;
    uint32_t slot = bufferHeader->m_index / 64;
    uint32_t index = bufferHeader->m_index % 64;
    if (chunkHeader->m_freeBitset[slot] & (((uint64_t)1) << (63 - index))) {
        result = true;
    }
    return result;
}

}  // namespace MOT
/**
 * @brief Dumps a buffer header into standard error stream.
 * @param arg The buffer header to print.
 */
extern "C" void MemBufferHeaderDump(void* arg);

/**
 * @brief Dumps a chunk header into standard error stream.
 * @param arg The chunk header to print.
 */
extern "C" void MemBufferChunkHeaderDump(void* arg);

/**
 * @brief Dumps a chunk header into standard error stream.
 * @param arg The chunk header to print.
 */
extern "C" void MemBufferChunkListDump(void* arg);

/**
 * @brief Dumps all information related to the given memory address.
 * @param arg The memory address to analyze and print.
 */
extern "C" void MemBufferDump(void* arg);

#endif /* MM_BUFFER_CHUNK_H */
