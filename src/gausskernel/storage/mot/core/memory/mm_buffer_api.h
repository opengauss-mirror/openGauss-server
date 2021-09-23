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

#include <stddef.h>

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
            for (uint32_t i = (node + 1) % g_memGlobalCfg.m_nodeCount; i != (uint32_t)node;
                 i = (i + 1) % g_memGlobalCfg.m_nodeCount) {
                buffer = MemBufferAllocatorAlloc(&g_globalAllocators[i][bufferClass]);
                if (buffer != nullptr) {
                    break;
                }
            }
        }
        if (buffer != nullptr) {
            DetailedMemoryStatisticsProvider::m_provider->AddGlobalBuffersUsed(node, bufferClass);
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
            DetailedMemoryStatisticsProvider::m_provider->AddLocalBuffersUsed(node, bufferClass);
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
        DetailedMemoryStatisticsProvider::m_provider->AddLocalBuffersFreed(node, bufferClass);
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
