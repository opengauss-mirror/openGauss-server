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
 * mm_raw_chunk_dir.cpp
 *    A raw size chunk directory.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/memory/mm_raw_chunk_dir.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <string.h>

#include "mm_raw_chunk_dir.h"
#include "mm_def.h"
#include "mm_numa.h"
#include "mm_raw_chunk.h"
#include "mot_error.h"
#include "mot_atomic_ops.h"
#include "mm_api.h"
#include "mm_virtual_huge_chunk.h"
#include "mot_atomic_ops.h"

namespace MOT {

DECLARE_LOGGER(RawChunkDir, Memory)

// Virtual Huge Chunks
// -------------------
// The MOT engine uses sometimes very lage contiguous memory chunks that expand much more then the regular 2 MB chunk.
// In this case we use a virtual chunk header stub in all the slots spanned by the large chunk. The virtual chunk header
// points to the start address of the large chunk.

// Chunk Directory Access:
// ----------------------
// Since we are using aligned chunks there is no race in directory access, but we employ full memory barriers since
// access is by many threads.

// Lazy Chunk Directory:
// ---------------------
// since the chunk directory is very large (128 MB entries, taking 1 GB), and since most allocations
// are grouped in adjacent entries, it is preferred to use a lazy load scheme:
// Divide the directory into several parts (1024), and whenever a part is first touched
// it is allocated. This way we keep a low footprint instead of allocating 1 GB in advance
// when most of it is unused. Pay attention that in production systems this is not an option (i.e.
// we cannot use lazy load).

#define CHUNK_ALIGN_BYTES (MEM_CHUNK_SIZE_MB * MEGA_BYTE)
#define IS_CHUNK_ALIGNED(chunk) ((((uint64_t)(chunk)) % CHUNK_ALIGN_BYTES) == 0)

static int lazyLoad = 0;

// we divide chunk directory to 1024 parts of 1 MB each
// ATTENTION: this constant **MUST** be a power of two due to calculation below
#define CHUNK_DIR_PART_COUNT 1024

// Lazy load globals and helpers
static void** chunkDirParts[CHUNK_DIR_PART_COUNT];
static uint64_t chunkDirPartSize = 0;
static void EnsureChunkDirPartExists(uint32_t part);

// Helper macros
#define CHUNK_PART_ID(chunkId) ((chunkId) / chunkDirPartSize)
#define CHUNK_OFFSET(chunkId) ((chunkId) % chunkDirPartSize)

// Full load parameters
static void** chunkDir = NULL;
static uint64_t chunkDirSize = 0;
static uint64_t chunkDirPow2 = 0;

// Helper function: compute the base 2 logarithm of some whole power of 2
static inline int Log2Pow2(uint64_t pow2)
{
    // the power of two of some number is the number of trailing zeros in its binary representation
    return __builtin_ctzll(pow2);
}

// Get real chunk address when using a virtual chunk header
static inline void* GetRealChunk(void* chunk)
{
    void* realChunk = chunk;
    if (chunk != NULL) {
        MemRawChunkHeader* rawChunkHeader = (MemRawChunkHeader*)chunk;
        if ((rawChunkHeader->m_chunkType == MEM_CHUNK_TYPE_HUGE_OBJECT) ||
            (rawChunkHeader->m_chunkType == MEM_CHUNK_TYPE_SESSION_LARGE_BUFFER_POOL)) {
            realChunk = ((MemVirtualHugeChunkHeader*)chunk)->m_chunk;
            MOT_LOG_DEBUG("Replacing virtual chunk at %p with real chunk at %p", chunk, realChunk);
        }
    }
    return realChunk;
}

extern int MemRawChunkDirInit(int lazy /* = 1 */)
{
    // our purpose is to compute the global chunk directory size
    // The usable memory size in the system is 2^48 bytes. Each chunk is 2^21 bytes
    // so there are at most 2^48 / 2^21 = 2^27 chunks in the system (128 MB)
    // each chunk entry requires 8 bytes (pointer to chunk header)
    // so the total size of the directory is 2^27 * 2^3 = 2^30 = 1 GB

    int rc = 0;

    lazyLoad = lazy;

    // get the power of 2 for left-shift (instead of dividing)
    chunkDirPow2 = Log2Pow2(MEM_CHUNK_SIZE_MB) + 20;  // add 20 to power for megabytes
    if (lazyLoad) {
        MOT_LOG_TRACE("Loading lazy chunk directory");
        uint32_t chunkPartPow2 = Log2Pow2(CHUNK_DIR_PART_COUNT);
        chunkDirPartSize = (1ull << 48) >> (chunkDirPow2 + chunkPartPow2);  // number of chunks divided
        MOT_LOG_TRACE("Chunk Dir part size is: %" PRIu64 ", total size: %" PRIu64,
            chunkDirPartSize,
            chunkDirPartSize * CHUNK_DIR_PART_COUNT);
        errno_t erc = memset_s(chunkDirParts, sizeof(chunkDirParts), 0, sizeof(chunkDirParts));
        securec_check(erc, "\0", "\0");
    } else {
        MOT_LOG_TRACE("Loading full chunk directory");
        chunkDirSize = (1ull << 48) >> chunkDirPow2;
        uint64_t allocSize = chunkDirSize * sizeof(void*);
        chunkDir = (void**)MemNumaAllocGlobal(allocSize);
        if (chunkDir == NULL) {
            MOT_REPORT_PANIC(MOT_ERROR_OOM,
                "MM Raw Chunk Directory Initialization",
                "Failed to allocate %" PRIu64 " bytes for the global chunk directory",
                chunkDirSize);
            rc = MOT_ERROR_OOM;
        } else {
            // we MOT_MEMSET() our table because we use unaligned chunks (see explanation below).
            // with 2MB chunks we incur a penalty of 2^27 entries of 8 bytes = 2^30 bytes
            // so we memset 1 GB of interleaved NUMA pages
            // this memset has 2 additional advantages:
            // 1. it ensures we have the physical memory reserved as resident memory (so we don't fail in runtime)
            // 2. we avoid the performance penalty in future page faults
            errno_t erc = memset_s(chunkDir, allocSize, 0, allocSize);
            securec_check(erc, "\0", "\0");
            MOT_LOG_TRACE("Reserved %" PRIu64 " MB for global chunk directory at %p", allocSize / MEGA_BYTE, chunkDir);
        }
    }
    return rc;
}

extern void MemRawChunkDirDestroy()
{
    if (lazyLoad) {
        for (uint32_t i = 0; i < CHUNK_DIR_PART_COUNT; ++i) {
            if (chunkDirParts[i] != NULL) {
                MemNumaFreeGlobal(chunkDirParts[i], chunkDirPartSize * sizeof(void*));
                MOT_LOG_TRACE("Removed chunk directory part %u", i);
            }
        }
    } else {
        MemNumaFreeGlobal(chunkDir, chunkDirSize * sizeof(void*));
    }
}

extern void MemRawChunkDirInsert(void* chunk)
{
    MOT_ASSERT(IS_CHUNK_ALIGNED(chunk));
    uint64_t chunkId = ((uint64_t)chunk) >> chunkDirPow2;  // more efficient than divide
    if (lazyLoad) {
        uint32_t part = CHUNK_PART_ID(chunkId);
        uint32_t offset = CHUNK_OFFSET(chunkId);
        EnsureChunkDirPartExists(part);
        MOT_ASSERT(chunkDirParts[part] != NULL);
        MOT_ASSERT(chunkDirParts[part][offset] == NULL);
        chunkDirParts[part][offset] = chunk;
        MOT_LOG_DEBUG(
            "Inserted chunk %p into slot %" PRIu64 " at partition %u, offset %u", chunk, chunkId, part, offset);
    } else {
        MOT_ASSERT(chunkDir[chunkId] == NULL);
        chunkDir[chunkId] = chunk;
        MOT_LOG_DEBUG("Inserted chunk %p into slot %" PRIu64, chunk, chunkId);
    }
    COMPILER_BARRIER;
}

extern void MemRawChunkDirInsertEx(void* chunk, uint32_t chunkCount, void* chunkData)
{
    MOT_ASSERT(IS_CHUNK_ALIGNED(chunk));
    uint64_t chunkId = ((uint64_t)chunk) >> chunkDirPow2;  // more efficient than divide
    if (lazyLoad) {
        for (uint32_t i = 0; i < chunkCount; ++i) {
            uint32_t part = CHUNK_PART_ID(chunkId + i);
            uint32_t offset = CHUNK_OFFSET(chunkId + i);
            EnsureChunkDirPartExists(part);
            MOT_ASSERT(chunkDirParts[part] != NULL);
            MOT_ASSERT(chunkDirParts[part][offset] == NULL);
            chunkDirParts[part][offset] = chunkData;
            MOT_LOG_DEBUG("Inserted extended chunk data %p for chunk %p into slot %" PRIu64
                          " at partition %u, offset %u",
                chunkData,
                chunk,
                chunkId + i,
                part,
                offset);
        }
    } else {
        for (uint32_t i = 0; i < chunkCount; ++i) {
            MOT_ASSERT(chunkDir[chunkId + i] == NULL);
            chunkDir[chunkId + i] = chunkData;
            MOT_LOG_DEBUG(
                "Inserted extended chunk data %p for chunk %p into slot %" PRIu64, chunkData, chunk, chunkId + i);
        }
    }
    COMPILER_BARRIER;
}

extern void MemRawChunkDirRemove(void* chunk)
{
    MOT_ASSERT(IS_CHUNK_ALIGNED(chunk));
    uint64_t chunkId = ((uint64_t)chunk) >> chunkDirPow2;  // more efficient than divide
    if (lazyLoad) {
        uint32_t part = CHUNK_PART_ID(chunkId);
        uint32_t offset = CHUNK_OFFSET(chunkId);
        MOT_ASSERT(chunkDirParts[part] != NULL);
        MOT_ASSERT(chunkDirParts[part][offset] == chunk);
        chunkDirParts[part][offset] = NULL;
        MOT_LOG_DEBUG(
            "Removed chunk %p from slot %" PRIu64 " at partition %u, offset %u", chunk, chunkId, part, offset);
    } else {
        MOT_ASSERT(chunkDir[chunkId] == chunk);
        chunkDir[chunkId] = NULL;
        MOT_LOG_DEBUG("Removed chunk %p from slot %" PRIu64, chunk, chunkId);
    }
    COMPILER_BARRIER;
}

extern void MemRawChunkDirRemoveEx(void* chunk, uint32_t chunkCount)
{
    MOT_ASSERT(IS_CHUNK_ALIGNED(chunk));
    uint64_t chunkId = ((uint64_t)chunk) >> chunkDirPow2;  // more efficient than divide
    if (lazyLoad) {
        void* chunkData = chunkDirParts[CHUNK_PART_ID(chunkId)][CHUNK_OFFSET(chunkId)];
        for (uint32_t i = 0; i < chunkCount; ++i) {
            uint32_t part = CHUNK_PART_ID(chunkId + i);
            uint32_t offset = CHUNK_OFFSET(chunkId + i);
            MOT_ASSERT(chunkDirParts[part] != NULL);
            MOT_ASSERT(chunkDirParts[part][offset] == chunkData);
            chunkDirParts[part][offset] = NULL;
            MOT_LOG_DEBUG("Removed extended chunk %p from slot %" PRIu64 " at partition %u, offset %u",
                chunk,
                chunkId + i,
                part,
                offset);
        }
    } else {
        void* chunkData = chunkDir[chunkId];
        for (uint32_t i = 0; i < chunkCount; ++i) {
            MOT_ASSERT(chunkDir[chunkId + i] == chunkData);
            chunkDir[chunkId + i] = NULL;
            MOT_LOG_DEBUG("Removed extended chunk %p from slot %" PRIu64, chunk, chunkId + i);
        }
    }
    COMPILER_BARRIER;
}

static void AssertChunkValid(void* address, void* chunk, void* realChunk)
{
    if (chunk && realChunk) {
        MemRawChunkHeader* rawChunkHeader = (MemRawChunkHeader*)chunk;
        if ((rawChunkHeader->m_chunkType == MEM_CHUNK_TYPE_HUGE_OBJECT) ||
            (rawChunkHeader->m_chunkType == MEM_CHUNK_TYPE_SESSION_LARGE_BUFFER_POOL)) {
            MemVirtualHugeChunkHeader* virtualChunkHeader = (MemVirtualHugeChunkHeader*)chunk;
            MOT_ASSERT(((uint64_t)address) >= ((uint64_t)realChunk));
            MOT_ASSERT((((uint64_t)address) - ((uint64_t)realChunk)) <= virtualChunkHeader->m_chunkSizeBytes);
        } else {
            MOT_ASSERT(chunk == realChunk);
            MOT_ASSERT(((uint64_t)address) >= ((uint64_t)chunk));
            MOT_ASSERT((((uint64_t)address) - ((uint64_t)chunk)) <= (MEM_CHUNK_SIZE_MB * MEGA_BYTE));
        }
    }
}

extern void* MemRawChunkDirLookupLazy(void* address, uint64_t chunkId)
{
    void* chunk = NULL;
    void* realChunk = NULL;
    uint32_t part = CHUNK_PART_ID(chunkId);
    uint32_t offset = CHUNK_OFFSET(chunkId);
    MOT_LOG_DEBUG("Search for address %p: chunk id=%" PRIu64 ", part=%u, offset=%u, chunk_dir_parts[part]=%p",
        address,
        chunkId,
        part,
        offset,
        chunkDirParts[part]);

    if (unlikely(!chunkDirParts[part])) {
        MOT_LOG_PANIC("Search for invalid address %p in chunk directory: chunk partition %u for slot %" PRIu64
                      " does not exist",
            address,
            part,
            chunkId);
    } else {
        chunk = chunkDirParts[part][offset];
        if (unlikely(chunk == NULL)) {
            MOT_LOG_PANIC("Search for invalid address %p in chunk directory: chunk partition %u at offset %u does not "
                          "contain chunk for slot %" PRIu64,
                address,
                part,
                offset,
                chunkId);
        } else {
            realChunk =
                GetRealChunk(chunk);  // we return the virtual chunk, but we check addresses against the real chunk
            if (unlikely(realChunk == NULL)) {
                MOT_LOG_PANIC("Search for invalid address %p in chunk directory: Could not resolve real chunk in "
                              "partition %u/%u slot %" PRIu64 "(corrupt virtual chunk header)",
                    address,
                    part,
                    offset,
                    chunkId);
                chunk = NULL;  // force core dump
            } else if (((uint64_t)realChunk) > ((uint64_t)address)) {
                MOT_LOG_PANIC("Search for invalid address %p in chunk directory: Found mismatching chunk %p (real "
                              "chunk %p) in slot %" PRIu64 "(corrupt virtual chunk header)",
                    address,
                    chunk,
                    realChunk,
                    chunkId);
                chunk = NULL;  // force core dump
            }
        }
    }

    // validation (wilfully crash before unplanned disaster)
    if (!chunk) {
        SetLastError(MOT_ERROR_INVALID_ARG, MOT_SEVERITY_FATAL);  // this is a serious error, just before crashing!
        MOTAbort(address);
    }

    // debug tests
#ifdef MOT_DEBUG
    AssertChunkValid(address, chunk, realChunk);
#endif

    return chunk;
}

extern void* MemRawChunkDirLookupDirect(void* address, uint64_t chunkId)
{
    void* chunk = chunkDir[chunkId];
    void* realChunk = NULL;
    if (unlikely(chunk == NULL)) {
        MOT_LOG_PANIC("Search for invalid address %p in chunk directory: chunk does not exist in slot %" PRIu64,
            address,
            chunkId);
    } else {
        realChunk = GetRealChunk(chunk);  // we return the virtual chunk, but we check addresses against the real chunk
        if (unlikely(realChunk == NULL)) {
            MOT_LOG_PANIC(
                "Search for invalid address %p in chunk directory: Could not resolve real chunk in slot " PRIu64
                "(corrupt virtual chunk header)",
                address,
                chunkId);
            chunk = NULL;  // force core dump
        } else if (((uint64_t)realChunk) > ((uint64_t)address)) {
            MOT_LOG_PANIC("Search for invalid address %p in chunk directory: Found mismatching chunk %p (real chunk "
                          "%p) in slot " PRIu64 "(corrupt virtual chunk header)",
                address,
                chunk,
                realChunk,
                chunkId);
            chunk = NULL;  // force core dump
        }
    }

    // validation (wilfully crash before unplanned disaster)
    if (!chunk) {
        SetLastError(MOT_ERROR_INVALID_ARG, MOT_SEVERITY_FATAL);  // this is a serious error, just before crashing!
        MOTAbort(address);
    }

    // debug tests
#ifdef MOT_DEBUG
    AssertChunkValid(address, chunk, realChunk);
#endif

    return chunk;
}

extern void* MemRawChunkDirLookup(void* address)
{
    // Pay attention since this is a global map with 1-1 mapping, there is no need for a lock.
    // we just need full barrier in writers side (insert/remove functions)
    void* chunk = NULL;
    uint64_t chunkId = ((uint64_t)address) >> chunkDirPow2;  // more efficient than divide

    if (lazyLoad) {
        chunk = MemRawChunkDirLookupLazy(address, chunkId);
    } else {
        chunk = MemRawChunkDirLookupDirect(address, chunkId);
    }

    return chunk;
}

extern void MemRawChunkDirPrint(const char* name, LogLevel logLevel)
{
    if (MOT_CHECK_LOG_LEVEL(logLevel)) {
        StringBufferApply([name, logLevel](StringBuffer* stringBuffer) {
            MemRawChunkDirToString(0, name, stringBuffer);
            MOT_LOG(logLevel, "%s", stringBuffer->m_buffer);
        });
    }
}

extern void MemRawChunkDirToString(int indent, const char* name, StringBuffer* stringBuffer)
{
    StringBufferAppend(stringBuffer, "%*sChunk Directory %s = {\n", indent, "", name);
    if (lazyLoad) {
        for (uint32_t i = 0; i < CHUNK_DIR_PART_COUNT; ++i) {
            if (chunkDirParts[i]) {
                for (uint32_t j = 0; j < chunkDirPartSize; ++j) {
                    if (chunkDirParts[i][j]) {
                        MemRawChunkHeader* rawChunk = (MemRawChunkHeader*)chunkDirParts[i][j];
                        StringBufferAppend(stringBuffer,
                            "%*s[%u:%u] = %p (type: %s, node: %d, usage-context: %s)\n",
                            indent + PRINT_REPORT_INDENT,
                            "",
                            i,
                            j,
                            rawChunk,
                            MemChunkTypeToString(rawChunk->m_chunkType),
                            (unsigned)rawChunk->m_node,
                            MemAllocTypeToString(rawChunk->m_allocType));
                    }
                }
            }
        }
    } else {
        for (uint64_t i = 0; i < chunkDirSize; ++i) {
            if (chunkDir[i] != NULL) {
                MemRawChunkHeader* rawChunk = (MemRawChunkHeader*)chunkDir[i];
                StringBufferAppend(stringBuffer,
                    "%*s[%" PRIu64 "] = %p (%s on node %d)\n",
                    indent + PRINT_REPORT_INDENT,
                    "",
                    i,
                    rawChunk,
                    MemChunkTypeToString(rawChunk->m_chunkType),
                    rawChunk->m_node);
            }
        }
    }
    StringBufferAppend(stringBuffer, "%*s}", indent, "");
}

static void EnsureChunkDirPartExists(uint32_t part)
{
    if (chunkDirParts[part] == NULL) {
        size_t allocSize = chunkDirPartSize * sizeof(void*);
        void** chunkDirPart = (void**)MemNumaAllocGlobal(allocSize);
        if (chunkDirPart == NULL) {
            // this is a serious error, just before crashing!
            MOT_REPORT_PANIC(MOT_ERROR_OOM, "N/A", "Failed to allocate chunk directory partition, aborting");
            MOTAbort();
        }

        // we MOT_MEMSET() the partition because we use unaligned chunks (see explanation above in
        // mm_raw_chunk_dir_init()).
        errno_t erc = memset_s(chunkDirPart, allocSize, 0, allocSize);
        securec_check(erc, "\0", "\0");
        void** nullArray = NULL;
        if (!MOT_ATOMIC_CAS(chunkDirParts[part], nullArray, chunkDirPart)) {
            // lost the race so just cleanup
            MemNumaFreeGlobal(chunkDirPart, allocSize);
        } else {
            MOT_LOG_TRACE("Created chunk directory partition %u with size %u", part, (unsigned)allocSize);
        }
    }
}

}  // namespace MOT

extern "C" void MemRawChunkDirDump()
{
    MOT::StringBufferApply([](MOT::StringBuffer* stringBuffer) {
        MOT::MemRawChunkDirToString(0, "Debug Dump", stringBuffer);
        fprintf(stderr, "%s", stringBuffer->m_buffer);
        fflush(stderr);
    });
}
