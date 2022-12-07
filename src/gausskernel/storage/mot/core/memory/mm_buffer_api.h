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
 * mm_buffer_api.h
 *    Memory buffer API implementation.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/memory/mm_buffer_api.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef MM_BUFFER_API_H
#define MM_BUFFER_API_H

#include "mm_buffer_class.h"
#include "string_buffer.h"
#include "utilities.h"
#include "mm_def.h"
#include "mm_buffer_allocator.h"
#include "memory_statistics.h"
#include "session_context.h"
#include "mm_raw_chunk_dir.h"
#include "mm_cfg.h"
#include "mot_error.h"

#include <cstddef>

namespace MOT {
/** @var Array of global (long-term) buffer allocators per NUMA node. */
extern MemBufferAllocator** g_globalAllocators;

/** @var Array of local (short-term) buffer allocators per NUMA node. */
extern MemBufferAllocator** g_localAllocators;

/**
 * @brief Helper function to issue error.
 * @param func_name The faulting function name
 * @param node The invalid node identifier.
 */
extern void MemBufferIssueError(int errorCode, const char* format, ...);

/**
 * @brief Initialize the buffer API.
 * @return Zero if succeeded, otherwise an error code.
 * @note The thread id pool must be already initialized.
 * @see InitThreadIdPool.
 */
extern int MemBufferApiInit();

/**
 * @brief Destroys the buffer API.
 */
extern void MemBufferApiDestroy();

/**
 * @brief Allocates a buffer from the global buffer allocator of the current NUMA node.
 * @param bufferClass The class of buffer pool from which to allocate a buffer.
 * @return The buffer pointer or NULL if allocation failed (i.e. out of memory).
 * @note The global buffer allocator provides buffers from interleaved NUMA pages.
 */
inline void* MemBufferAllocGlobal(MemBufferClass bufferClass)
{
    void* buffer = nullptr;
    int node = MOTCurrentNumaNodeId;
    if (node < 0) {
        MemBufferIssueError(MOT_ERROR_INVALID_ARG,
            "Cannot allocate %s global buffer: Invalid NUMA node identifier %u",
            MemBufferClassToString(bufferClass),
            node);
    } else {
        buffer = MemBufferAllocatorAlloc(&g_globalAllocators[node][bufferClass]);
        if (buffer == nullptr) {
            // this is a rare out of memory scenario - all chunk pools are depleted, but other allocators might still
            // have memory
            for (uint32_t i = ((uint32_t)node + 1) % g_memGlobalCfg.m_nodeCount; i != (uint32_t)node;
                 i = (i + 1) % g_memGlobalCfg.m_nodeCount) {
                buffer = MemBufferAllocatorAlloc(&g_globalAllocators[i][bufferClass]);
                if (buffer != nullptr) {
                    break;
                }
            }
        }
        if (buffer != nullptr) {
            DetailedMemoryStatisticsProvider::GetInstance().AddGlobalBuffersUsed(node, bufferClass);
        } else {
            MemBufferIssueError(MOT_ERROR_OOM,
                "Failed to allocate %s global buffer: out of memory",
                MemBufferClassToString(bufferClass));
        }
    }
    return buffer;
}

/**
 * @brief Allocates a buffer from the local buffer allocator of the specified NUMA node.
 * @param bufferClass The class of buffer pool from which to allocate a buffer.
 * @param node The NUMA node identifier.
 * @return The buffer pointer or NULL if allocation failed (i.e. out of memory).
 * @note The local buffer allocator provides buffers from a specific NUMA node.
 */
inline void* MemBufferAllocOnNode(MemBufferClass bufferClass, int node)
{
    void* buffer = nullptr;
    if (node < 0) {
        MemBufferIssueError(MOT_ERROR_INVALID_ARG,
            "Cannot allocate %s local buffer: Invalid NUMA node identifier %u",
            MemBufferClassToString(bufferClass),
            node);
    } else {
        buffer = MemBufferAllocatorAlloc(&g_localAllocators[node][bufferClass]);
        if (buffer != nullptr) {
            DetailedMemoryStatisticsProvider::GetInstance().AddLocalBuffersUsed(node, bufferClass);
        } else {
            MemBufferIssueError(MOT_ERROR_OOM,
                "Failed to allocate %s local buffer on node %d: out of memory",
                MemBufferClassToString(bufferClass),
                node);
        }
    }
    return buffer;
}

/**
 * @brief Allocates a buffer from the local buffer allocator of the current NUMA node.
 * @param bufferClass The class of buffer pool from which to allocate a buffer.
 * @return The buffer pointer or NULL if allocation failed (i.e. out of memory).
 * @note The local buffer allocator provides buffers from the local NUMA node.
 */
inline void* MemBufferAllocLocal(MemBufferClass bufferClass)
{
    return MemBufferAllocOnNode(bufferClass, MOTCurrentNumaNodeId);
}

/**
 * @brief Reclaims memory previously allocated from the global buffer allocator.
 * @param buffer The buffer to reclaim.
 * @param bufferClass The class of the buffer.
 */
extern void MemBufferFreeGlobal(void* buffer, MemBufferClass bufferClass);

/**
 * @brief Reclaims memory previously allocated from the global per-node buffer allocator.
 * @param buffer The buffer to reclaim.
 * @param bufferClass The class of the buffer.
 * @param node The NUMA node identifier. This cannot be the pseudo-interleaved node.
 */
inline void MemBufferFreeOnNode(void* buffer, MemBufferClass bufferClass, int node)
{
    if (node < 0) {
        MemBufferIssueError(MOT_ERROR_INVALID_ARG, "Cannot free %s local buffer on node %d", node);
    } else {
        MemBufferAllocatorFree(&g_localAllocators[node][bufferClass], buffer);
        DetailedMemoryStatisticsProvider::GetInstance().AddLocalBuffersFreed(node, bufferClass);
    }
}

/**
 * @brief Reclaims memory previously allocated by a call to @fn mm_buffer_alloc_local.
 * @param buffer The buffer to reclaim.
 * @param bufferClass The class of the buffer.
 */
extern void MemBufferFreeLocal(void* buffer, MemBufferClass bufferClass);

/**
 * @brief Clears the current session caches on all global buffer allocators.
 */
extern void MemBufferClearSessionCache();

/**
 * @brief Reserve global memory for current session. While in reserve-mode, released chunks  are kept in the current
 * session's reserve, rather than being released to global memory.
 * @param chunkCount The number of chunks to reserve.
 * @return Zero on success, otherwise error code on failure.
 */
extern int MemBufferReserveGlobal(uint32_t chunkCount);

/**
 * @brief Release all global memory reserved for current session.
 * @param bufferClass The buffer class for which an existing reservation is to be released.
 * @return Zero on success, otherwise error code on failure.
 */
extern int MemBufferUnreserveGlobal(uint32_t chunkCount);

/**
 * @brief Reserve global memory for current session for a specific buffer class. While in reserve-mode, released chunks
 * are kept in the current session's reserve, rather than being released to global memory.
 * @param bufferClass The buffer class for which reservation is to be made.
 * @param chunkCount The number of chunks to reserve.
 * @return Zero on success, otherwise error code on failure.
 */
inline int MemBufferReserveGlobal(MemBufferClass bufferClass, uint32_t chunkCount)
{
    int result = 0;
    int node = MOTCurrentNumaNodeId;
    if (node < 0) {
        MemBufferIssueError(MOT_ERROR_INVALID_ARG,
            "Cannot reserve %u chunks for %s global buffers: Invalid NUMA node identifier %u",
            chunkCount,
            MemBufferClassToString(bufferClass),
            node);
        result = MOT_ERROR_INVALID_ARG;
    } else {
        result = MemBufferAllocatorReserve(&g_localAllocators[node][bufferClass], chunkCount);
    }
    return result;
}

/**
 * @brief Release all global memory reserved for current session for a specific buffer class.
 * @param bufferClass The buffer class for which an existing reservation is to be released.
 * @return Zero on success, otherwise error code on failure.
 */
inline int MemBufferUnreserveGlobal(MemBufferClass bufferClass)
{
    int result = 0;
    int node = MOTCurrentNumaNodeId;
    if (node < 0) {
        MemBufferIssueError(MOT_ERROR_INVALID_ARG,
            "Cannot unreserve global memory for %s buffers: Invalid NUMA node identifier %u",
            MemBufferClassToString(bufferClass),
            node);
        result = MOT_ERROR_INVALID_ARG;
    } else {
        result = MemBufferAllocatorUnreserve(&g_localAllocators[node][bufferClass]);
    }
    return result;
}

/**
 * @brief Prints all buffer API status into log.
 * @param name The name to prepend to the log message.
 * @param logLevel The log level to use in printing.
 * @param[opt] reportMode Specifies the report mode.
 */
extern void MemBufferApiPrint(const char* name, LogLevel logLevel, MemReportMode reportMode = MEM_REPORT_SUMMARY);

/**
 * @brief Dumps all buffer API status into string buffer.
 * @param indent The indentation level.
 * @param name The name to prepend to the log message.
 * @param stringBuffer The string buffer.
 * @param[opt] reportMode Specifies the report mode.
 */
extern void MemBufferApiToString(
    int indent, const char* name, StringBuffer* stringBuffer, MemReportMode reportMode = MEM_REPORT_SUMMARY);

}  // namespace MOT

/**
 * @brief Dumps all buffer API status to standard error stream.
 */
extern "C" void MemBufferApiDump();

/**
 * @brief Analyzes the memory status of a given buffer address.
 * @param address The buffer to analyze.
 * @return Non-zero value if buffer was found.
 */
extern "C" int MemBufferApiAnalyze(void* buffer);

#endif /* MM_BUFFER_API_H */
