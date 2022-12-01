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
 * mm_raw_chunk_pool.h
 *    A raw size chunk pool without ability to grow.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/memory/mm_raw_chunk_pool.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef MM_RAW_CHUNK_POOL_H
#define MM_RAW_CHUNK_POOL_H

#include "mm_lock.h"
#include "mm_raw_chunk.h"
#include "mm_lf_stack.h"
#include "string_buffer.h"
#include "utilities.h"
#include "thread_id.h"

// A raw size chunk pool without ability to grow

namespace MOT {

/** @define Constant for the maximum number of bytes used in pool name. */
#define MM_CHUNK_POOL_NAME_SIZE 32

/**
 * @typedef MemRawChunkPool A raw-size pool of reserved chunks.
 */
struct MemRawChunkPool {
    /** @var The name of the chunk pool. */
    char m_poolName[MM_CHUNK_POOL_NAME_SIZE];

    /** @var The node with which the pool is associated. */
    int16_t m_node;

    /** @var The kind of memory allocation this pool provides (global or local). */
    MemAllocType m_allocType;

    /** @var The initial reservation count. */
    uint32_t m_reserveChunkCount;

    /**
     * @var The high red mark. If the total number of chunks given to the application exceeds this value, then the pool
     * is marked in emergency mode to allow the application to cope with this situation.
     */
    uint32_t m_highRedMark;

    /**
     * @var The maximum number of chunks that can be produced by this pool. This is an upper bound on the ability of the
     * pool to grow.
     */
    uint32_t m_highBlackMark;

    /**
     * @var A flag designating the chunk pool is in emergency mode. The chunk pool is in emergency mode whenever the
     * total amount of allocated chunks gets greater than the high red mark.
     */
    uint32_t m_emergencyMode;

    /** @var The total number of chunks produced by the pool, whether reserved or in use by the engine. */
    uint32_t m_totalChunkCount;

    /** @var The underlying cascaded chunk pool (optional). */
    MemRawChunkPool* m_subChunkPool;

    /** @var A buffer for lock-free stack nodes. */
    MemLFStackNode* m_nodeBuffer;

    /** @var The chunk stack (also holds the number of chunks in the pool). */
    MemLFStack m_chunkAllocStack;

    /** @var Chunk reservation mode (physical or virtual). */
    MemReserveMode m_reserveMode;

    /** @var Chunk reservation mode (compact or expanding). */
    MemStorePolicy m_storePolicy;

    /** @var Private data for managing asynchronous memory reservation. */
    void* m_asyncReserveData;

    /** @struct Per-session reservation data. */
    struct SessionReserve {
        /** @var Reservation mode flag. */
        uint32_t m_inReserveMode;

        /** @var Amount of reserved chunks. */
        uint32_t m_chunkCount;

        /** @var Reserved chunk list. */
        MemRawChunkHeader* m_reservedChunks;
    };
    /** @var Per-session reservation. */
    SessionReserve* m_sessionReservation;
};

/**
 * @typedef mm_raw_chunk_pool_stats_t Raw chunk pool statistics.
 */
struct PACKED MemRawChunkPoolStats {
    /** @var The NUMA node identifier with which the pool is associated. */
    int16_t m_node;  // L1 offset [0-2]

    /** @var The kind of memory allocation this pool provides (global or local). */
    MemAllocType m_allocType;  // L1 offset [2-4]

    /** @var Align next member to 8 byte offset. */
    uint8_t m_padding[4];  // L1 offset [4-8]

    /**
     * @var The <b>current</b> total number of reserved bytes (whether currently in pool or used by
     * the engine). Pay attention that this value may change over time, as the pool is expanding.
     */
    uint64_t m_reservedBytes;  // L1 offset [8-16]

    /** @var The current total number of bytes used by the application. */
    uint64_t m_usedBytes;  // L1 offset [16-24]
};

/**
 * @brief Initializes a chunk pool.
 * @param chunkPool The chunk pool to initialize.
 * @param poolName The name of the pool.
 * @param node The NUMA node with which the chunk poll is associated. The pseudo-node @ref MM_INTERLEAVE_NODE can be
 * used here.
 * @param allocType The kind of allocations this pool provides (global or local).
 * @param reserveChunkCount The number of chunks to reserve in the pool.
 * @param reserveWorkerCount The number of workers to employ in order to reserve chunks during chunk initialization.
 * @param highRedMark The maximum number of chunks that can be given to the application, above which the pool is
 * declared to be in emergency mode.
 * @param highBlackMark The maximum number of chunks that can be produced by the pool.
 * @param[opt] reserveMode Specifies whether to reserve physical or virtual memory.
 * @param[opt] storePolicy Specifies how to store deallocated chunks. In compact mode, if the chunk pool is still above
 * the minimum reservation, then the chunk is returned to kernel. In expanding mode, the chunk is never returned to
 * kernel, but always kept inside the chunk pool.
 * @param[opt] subChunkPool An optional underlying chunk pool. This allows defining cascaded chunk pools required for
 * implementing complex memory reservation schemes.
 * @param[opt] asyncReserve Specifies whether to reserve memory in the background. In this case this call is not
 * blocking, and a further call to @ref mm_raw_chunk_pool_wait_reserve must be made before the pool can be used.
 * @return Zero if initialization finished successfully, otherwise an error code.
 */
extern int MemRawChunkPoolInit(MemRawChunkPool* chunkPool, const char* poolName, int node, MemAllocType allocType,
    uint32_t reserveChunkCount, uint32_t reserveWorkerCount, uint32_t highRedMark, uint32_t highBlackMark,
    MemReserveMode reserveMode = MEM_RESERVE_PHYSICAL, MemStorePolicy storePolicy = MEM_STORE_COMPACT,
    MemRawChunkPool* subChunkPool = NULL, int asyncReserve = 0);

/**
 * @brief Waits for asynchronous chunk pool pre-loading ends.
 * @param chunkPool The chunk pool.
 * @return Zero if initialization finished successfully, otherwise an error code.
 */
extern int MemRawChunkPoolWaitReserve(MemRawChunkPool* chunkPool);

/**
 * @brief Aborts waiting for asynchronous pre-allocation to end.
 * @param chunkPool The chunk pool to abort its pre-allocation.
 */
extern void MemRawChunkPoolAbortReserve(MemRawChunkPool* chunkPool);

/**
 * @brief Destroys a chunk pool.
 * @param chunkPool The chunk pool to destroy.
 */
extern void MemRawChunkPoolDestroy(MemRawChunkPool* chunkPool);

/**
 * @brief Allocates a chunk from a chunk pool. If the pool is empty NULL is returned.
 * @param chunkPool The chunk pool.
 * @return The allocated chunk or NULL if failed. The returned chunk is not initialized.
 */
extern MemRawChunkHeader* MemRawChunkPoolAlloc(MemRawChunkPool* chunkPool);

/**
 * @brief Deallocates a chunk to the pool.
 * @param chunkPool The chunk pool.
 * @param chunkHeader The header of the chunk to be deallocated.
 */
extern void MemRawChunkPoolFree(MemRawChunkPool* chunkPool, void* chunkHeader);

/**
 * @brief Reserve global memory for current session. While in reserve-mode, released chunks  are kept in the current
 * session's reserve, rather than being released to global memory.
 * @param chunkPool The chunk pool.
 * @param chunkCount The number of chunks to reserve.
 * @return The number of chunks reserved.
 */
extern uint32_t MemRawChunkPoolReserveSession(MemRawChunkPool* chunkPool, uint32_t chunkCount);

/**
 * @brief Release all global memory reserved for current session.
 * @param chunkPool The chunk pool.
 */
extern uint32_t MemRawChunkPoolUnreserveSession(MemRawChunkPool* chunkPool, uint32_t chunkCount);

/**
 * @brief Prints a chunk pool into log.
 * @param name The name of the chunk pool to print.
 * @param logLevel The log level to use in printing.
 * @param chunkPool The chunk pool to print.
 * @param[opt] reportMode Specifies the report mode.
 */
extern void MemRawChunkPoolPrint(
    const char* name, LogLevel logLevel, MemRawChunkPool* chunkPool, MemReportMode reportMode = MEM_REPORT_SUMMARY);

/**
 * @brief Dumps a chunk pool into string buffer.
 * @param indent The indentation level.
 * @param name The name of the chunk pool to print.
 * @param chunkPool The chunk pool to print.
 * @param stringBuffer The string buffer.
 * @param[opt] reportMode Specifies the report mode.
 */
extern void MemRawChunkPoolToString(int indent, const char* name, MemRawChunkPool* chunkPool,
    StringBuffer* stringBuffer, MemReportMode reportMode = MEM_REPORT_SUMMARY);

/**
 * @brief Retrieves memory usage statistics for a chunk pool.
 * @param chunkPool The chunk pool.
 * @param[out] stats The resulting statistics.
 */
extern void MemRawChunkPoolGetStats(MemRawChunkPool* chunkPool, MemRawChunkPoolStats* stats);

}  // namespace MOT

/**
 * @brief Dumps a chunk pool into standard error stream.
 * @param arg The chunk pool to print.
 */
extern "C" void MemRawChunkPoolDump(void* arg);

/**
 * @brief Analyzes the buffer status in its chunk pool.
 * @param pool The chunk pool.
 * @param buffer The buffer to analyze.
 * @return Non-zero value if the buffer was found.
 */
extern "C" int MemRawChunkPoolAnalyze(void* pool, void* buffer);

#endif /* MM_RAW_CHUNK_POOL_H */
