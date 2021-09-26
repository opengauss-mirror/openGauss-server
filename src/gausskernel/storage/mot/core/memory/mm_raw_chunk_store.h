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
 * mm_raw_chunk_store.h
 *    The chunk store manages all the global chunk pools.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/memory/mm_raw_chunk_store.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef MM_RAW_CHUNK_STORE_H
#define MM_RAW_CHUNK_STORE_H

#include "mm_lock.h"
#include "mm_raw_chunk_pool.h"

// The chunk store manages all the global chunk pools.
// There is one global interleaved chunk pool in the systems, and a global chunk pool per NUMA node.
// Each global chunk pool manages a list of pre-allocated chunks, and high/low red marks triggering
// either chunk pre-fetching or issue a warning.

namespace MOT {

/**
 * @brief Initializes all the chunk pools of reserved memory in the chunk store.
 * @param[opt] reserveMode Specifies whether to reserve physical or virtual memory.
 * @param[opt] storePolicy Specifies how to store deallocated chunks. In compact mode, if the chunk pool is still above
 * the minimum reservation, then the chunk is returned to kernel. In expanding mode, the chunk is never returned to
 * kernel, but always kept inside the chunk pool.
 * @return Zero on success, otherwise an error code.
 */
extern int MemRawChunkStoreInit(
    MemReserveMode reserveMode = MEM_RESERVE_PHYSICAL, MemStorePolicy storePolicy = MEM_STORE_COMPACT);

/**
 * @brief Destroys all the chunk pools in the chunk store.
 */
extern void MemRawChunkStoreDestroy();

/**
 * @brief Allocates a chunk from the chunk store.
 * @detail The chunk is taken from a global pool of pre-allocated chunks. If the pool for the
 * specified node is empty then the caller is blocked until a chunk can be allocated from kernel.
 * If no chunk can be allocated from kernel due to high black mark restriction, then other global
 * pools of other NUMA nodes are attempted. If that fails as well then NULL is returned.
 * @param node The node from which to allocate a chunk.
 * @return The allocated chunk or NULL if failed. The returned chunks is not initialized.
 */
extern void* MemRawChunkStoreAllocGlobal(int node);

/**
 * @brief Allocates a chunk from the chunk store. The chunk is taken from a local pool of
 * pre-allocated chunks. If the pool for the specified node is empty then then the
 * caller is blocked until a chunk can be allocated from kernel. If no chunk can be allocated from
 * kernel due to high black mark restriction, then NULL is returned (no attempt is made to look at
 * other local pools in other nodes, as in the case of global chunk allocation).
 * @param node The node from which to allocate a chunk.
 * @return The allocated chunk or NULL if failed. The returned chunks is not initialized.
 */
extern void* MemRawChunkStoreAllocLocal(int node);

/**
 * @brief Deallocates a chunk to the store. The chunk is returned to a global chunk pool.
 * @param chunk The chunk to be deallocated.
 * @param node The node from which the chunk was allocated.
 */
extern void MemRawChunkStoreFreeGlobal(void* chunk, int node);

/**
 * @brief Deallocates a chunk to the store. The chunk is returned to a local chunk pool.
 * @param chunk The chunk to be deallocated.
 * @param node The node from which the chunk was allocated.
 */
extern void MemRawChunkStoreFreeLocal(void* chunk, int node);

/**
 * @brief Prints status of all chunk pools in chunk store into log.
 * @param name The name to prepend to the log message.
 * @param logLevel The log level to use in printing.
 * @param[opt] reportMode Specifies the report mode.
 */
extern void MemRawChunkStorePrint(const char* name, LogLevel logLevel, MemReportMode reportMode = MEM_REPORT_SUMMARY);

/**
 * @brief Dumps status of all chunk pools in chunk store into string buffer.
 * @param indent The indentation level.
 * @param name The name to prepend to the log message.
 * @param stringBuffer The string buffer.
 * @param[opt] reportMode Specifies the report mode.
 */
extern void MemRawChunkStoreToString(
    int indent, const char* name, StringBuffer* stringBuffer, MemReportMode reportMode = MEM_REPORT_SUMMARY);

/**
 * @brief Retrieves the global chunk pool for a specific node. This API is required for implementing
 * cascaded chunk pools (for global SLAB allocator provisioning). A global pool is is used for long-
 * term data and is used by many threads, even from different NUMA nodes. A global pool is
 * maintained per NUMA node.
 * @param node The NUMA node identifier.
 * @return The node-specific global chunk pool or NULL if the node number is invalid.
 */
extern MemRawChunkPool* MemRawChunkStoreGetGlobalPool(int node);

/**
 * @brief Retrieves the local chunk pool for the specified NUMA node. This API is required for
 * implementing cascaded chunk pools (for future session local pool/SLAB provisioning).
 * @param node The NUMA node identifier.
 * @return The node-specific local chunk pool or NULL if the node number is invalid.
 */
extern MemRawChunkPool* MemRawChunkStoreGetLocalPool(int node);

/**
 * @brief Retrieves global chunk pool statistics.
 * @param[out] chunkPoolStatsArray The resulting chunk pool statistics array.
 * @param statsArraySize The array size. If the array is not large enough then the report is truncated to as many pool
 * statistics as would fit in the array. The aggregated statistics of all global pools is put as the first entry.
 * @return The actual number of valid entries in the statistics array in which valid statistics were stored.
 */
extern uint32_t MemRawChunkStoreGetGlobalStats(MemRawChunkPoolStats* chunkPoolStatsArray, uint32_t statsArraySize);

/**
 * @brief Retrieves local chunk pool statistics.
 * @param[out] chunkPoolStatsArray The resulting chunk pool statistics array.
 * @param statsArraySize The array size. If the array is not large enough then the report is truncated to as many pool
 * statistics as would fit in the array. The aggregated statistics of all local pools is put as the first entry.
 * @return The actual number of valid entries in the statistics array in which valid statistics were stored.
 */
extern uint32_t MemRawChunkStoreGetLocalStats(MemRawChunkPoolStats* chunkPoolStatsArray, uint32_t statsArraySize);

}  // namespace MOT

/**
 * @brief Dumps status of all chunk pools in chunk store into standard error stream.
 */
extern "C" void MemRawChunkStoreDump();

/**
 * @brief Dumps analysis for a memory address.
 */
extern "C" void MemRawChunkStoreAnalyze(void* address);

#endif /* MM_CHUNK_STORE_H */
