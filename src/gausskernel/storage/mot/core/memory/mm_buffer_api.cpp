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
 * mm_buffer_api.cpp
 *    Memory buffer API implementation.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/memory/mm_buffer_api.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "mm_buffer_api.h"
#include "mm_cfg.h"
#include "mm_numa.h"
#include "mm_buffer_class.h"
#include "mm_buffer_allocator.h"
#include "mm_buffer_list.h"
#include "mm_buffer_chunk.h"
#include "mm_raw_chunk_store.h"
#include "mot_error.h"
#include "mm_raw_chunk_dir.h"
#include "memory_statistics.h"
#include "affinity.h"
#include "thread_id.h"
#include "utilities.h"
#include "session_context.h"
#include "mm_api.h"

#include <cstdint>
#include <pthread.h>
#include <cstdarg>

namespace MOT {
DECLARE_LOGGER(BufferApi, Memory)

MemBufferAllocator** g_globalAllocators = NULL;
MemBufferAllocator** g_localAllocators = NULL;

// Helpers
static int AllocatorsInit();
static void AllocatorsDestroy();
static void AllocatorsPrint(LogLevel logLevel);
static void AllocatorsToString(int indent, StringBuffer* stringBuffer, MemReportMode reportMode);

extern void MemBufferIssueError(int errorCode, const char* format, ...)
{
    va_list args;
    va_start(args, format);
    MOT_REPORT_ERROR_V(errorCode, "N/A", format, args);
    va_end(args);
}

extern int MemBufferApiInit()
{
    MOT_LOG_TRACE("Initializing Buffer API");
    int result = AllocatorsInit();
    if (result == 0) {
        MOT_LOG_TRACE("Buffer API initialized successfully");
    } else {
        MOT_LOG_PANIC("Buffer API initialization failed, see errors above");
    }

    return result;
}

extern void MemBufferApiDestroy()
{
    // some destruction code can free a few buffers, so we should cleanup here as well
    MOT_LOG_TRACE("Destroying Buffer API");
    MemBufferClearSessionCache();
    MemBufferApiPrint("Shutdown Report", LogLevel::LL_TRACE, MEM_REPORT_SUMMARY);
    AllocatorsDestroy();
    MOT_LOG_TRACE("Buffer API destroyed");
}

extern void MemBufferFreeGlobal(void* buffer, MemBufferClass bufferClass)
{
    // in case of foreign allocation (i.e. buffer came from a chunk of another node), the buffer must return to the
    // allocator it came from (and not according to the node to which its chunk belongs). In such a case the source
    // chunk of the buffer shows that the allocator node differs from the origin node of the chunk. In this case, the
    // buffer eventually needs to return to the correct heap (according to allocator and not origin node id)
    // otherwise the source chunk of the buffer will be managed on two heaps (the heap from which it was allocated
    // and the heap into which it is erroneously returned to).
    MemBufferChunkHeader* bufferChunkHeader = MemBufferChunkGetChunkHeader(buffer);
    if (bufferChunkHeader != NULL) {
        // pay attention: when using compact storage policy, the call below to MemBufferAllocatorFree() could cause the
        // chunk containing the buffer to be deallocated, and so the chunk header cannot be accessed, so we save the
        // allocator node in a local variables to avoid core dump
        int allocatorNode = bufferChunkHeader->m_allocatorNode;
        MemBufferAllocatorFree(&g_globalAllocators[allocatorNode][bufferClass], buffer);
        DetailedMemoryStatisticsProvider::GetInstance().AddGlobalBuffersFreed(allocatorNode, bufferClass);
    } else {
        MOT_LOG_PANIC("Attempt to release invalid buffer at %p: source chunk not found", buffer);

        // this is a very extreme case, we force abort in the hope of a better root cause analysis
        MOTAbort(buffer);
    }
}

extern void MemBufferFreeLocal(void* buffer, MemBufferClass bufferClass)
{
    // buffer must return to origin allocator, but this might create many free lists that are never freed
    MemBufferChunkHeader* bufferChunkHeader = MemBufferChunkGetChunkHeader(buffer);
    if (bufferChunkHeader != NULL) {
        MemBufferFreeOnNode(buffer, bufferClass, bufferChunkHeader->m_node);
    }
}

extern void MemBufferClearSessionCache()
{
    // since cleanup code should be performed only for threads participating in allocation, we should
    // check first if that is the case. we do so by checking current thread/node id.
    int node = MOTCurrentNumaNodeId;
    if ((node != MEM_INVALID_NODE) && (MOTCurrThreadId != INVALID_THREAD_ID)) {
        for (MemBufferClass bufferClass = MEM_BUFFER_CLASS_SMALLEST; bufferClass < MEM_BUFFER_CLASS_COUNT;
             ++bufferClass) {
            MemBufferAllocatorClearThreadCache(&g_globalAllocators[node][bufferClass]);
            MemBufferAllocatorClearThreadCache(&g_localAllocators[node][bufferClass]);
        }
    } else {
        MOT_LOG_TRACE("Invalid current thread/node identifier encountered during session cache cleanup, skipping "
                      "(thread id: %u, node id: %d)",
            (unsigned)MOTCurrThreadId,
            node);
    }
}

extern int MemBufferReserveGlobal(uint32_t chunkCount)
{
    int result = 0;
    int node = MOTCurrentNumaNodeId;
    if (node < 0) {
        MemBufferIssueError(MOT_ERROR_INVALID_ARG,
            "Cannot reserve %u chunks from global memory: Invalid NUMA node identifier %u",
            chunkCount,
            node);
        result = MOT_ERROR_INVALID_ARG;
    } else {
        result = MemRawChunkStoreReserveGlobal(node, chunkCount);
    }
    return result;
}

extern int MemBufferUnreserveGlobal(uint32_t chunkCount)
{
    int result = 0;
    int node = MOTCurrentNumaNodeId;
    if (node < 0) {
        MemBufferIssueError(
            MOT_ERROR_INVALID_ARG, "Cannot unreserve global memory: Invalid NUMA node identifier %u", node);
        result = MOT_ERROR_INVALID_ARG;
    } else {
        MemRawChunkStoreUnreserveGlobal(node, chunkCount);
    }
    return result;
}

extern void MemBufferApiPrint(const char* name, LogLevel logLevel, MemReportMode reportMode /* = MEM_REPORT_SUMMARY */)
{
    if (MOT_CHECK_LOG_LEVEL(logLevel)) {
        StringBufferApply([name, logLevel, reportMode](StringBuffer* stringBuffer) {
            MemBufferApiToString(0, name, stringBuffer, reportMode);
            MOT_LOG(logLevel, "%s", stringBuffer->m_buffer);
        });
    }
}

extern void MemBufferApiToString(
    int indent, const char* name, StringBuffer* stringBuffer, MemReportMode reportMode /* = MEM_REPORT_SUMMARY */)
{
    StringBufferAppend(stringBuffer, "%*sBuffer Memory %s Report:\n", indent, "", name);
    AllocatorsToString(indent + PRINT_REPORT_INDENT, stringBuffer, reportMode);
}

static int AllocatorsInit()
{
    int result = 0;
    errno_t erc;

    // initialize global allocators
    g_globalAllocators = (MemBufferAllocator**)calloc(g_memGlobalCfg.m_nodeCount, sizeof(MemBufferAllocator*));
    for (int node = 0; node < (int)g_memGlobalCfg.m_nodeCount; ++node) {
        g_globalAllocators[node] =
            (MemBufferAllocator*)MemNumaAllocLocal(sizeof(MemBufferAllocator) * MEM_BUFFER_CLASS_COUNT, node);
        // nullify all bytes so in case of error we can safely destroy only those that were initialized
        erc = memset_s(g_globalAllocators[node],
            sizeof(MemBufferAllocator) * MEM_BUFFER_CLASS_COUNT,
            0,
            sizeof(MemBufferAllocator) * MEM_BUFFER_CLASS_COUNT);
        securec_check(erc, "\0", "\0");
        for (MemBufferClass bufferClass = MEM_BUFFER_CLASS_SMALLEST; bufferClass < MEM_BUFFER_CLASS_COUNT;
             ++bufferClass) {
            result =
                MemBufferAllocatorInit(&g_globalAllocators[node][bufferClass], node, bufferClass, MEM_ALLOC_GLOBAL);
            if (result != 0) {
                break;
            }
        }
        if (result != 0) {
            break;
        }
    }

    // initialize local allocators
    if (result == 0) {
        // allocate nullified buffer so in case of error we can safely destroy only those that were initialized
        g_localAllocators = (MemBufferAllocator**)calloc(g_memGlobalCfg.m_nodeCount, sizeof(MemBufferAllocator*));
        for (int node = 0; node < (int)g_memGlobalCfg.m_nodeCount; ++node) {
            // prepare allocators for all size classes on this node
            g_localAllocators[node] =
                (MemBufferAllocator*)MemNumaAllocLocal(sizeof(MemBufferAllocator) * MEM_BUFFER_CLASS_COUNT, node);
            // nullify all bytes so in case of error we can safely destroy only those that were initialized
            erc = memset_s(g_localAllocators[node],
                sizeof(MemBufferAllocator) * MEM_BUFFER_CLASS_COUNT,
                0,
                sizeof(MemBufferAllocator) * MEM_BUFFER_CLASS_COUNT);
            securec_check(erc, "\0", "\0");
            for (MemBufferClass bufferClass = MEM_BUFFER_CLASS_SMALLEST; bufferClass < MEM_BUFFER_CLASS_COUNT;
                 ++bufferClass) {
                result =
                    MemBufferAllocatorInit(&g_localAllocators[node][bufferClass], node, bufferClass, MEM_ALLOC_LOCAL);
                if (result != 0) {
                    break;
                }
            }
            if (result != 0) {
                break;
            }
        }
    }

    return result;
}

static void AllocatorsDestroy()
{
    // destroy local allocators
    for (int node = 0; node < (int)g_memGlobalCfg.m_nodeCount; ++node) {
        if (g_localAllocators[node]) {
            for (MemBufferClass bufferClass = MEM_BUFFER_CLASS_SMALLEST; bufferClass < MEM_BUFFER_CLASS_COUNT;
                 ++bufferClass) {
                if (g_localAllocators[node][bufferClass].m_bufferHeap != NULL) {
                    MemBufferAllocatorDestroy(&g_localAllocators[node][bufferClass]);
                }
            }
            MemNumaFreeLocal(g_localAllocators[node], sizeof(MemBufferAllocator) * MEM_BUFFER_CLASS_COUNT, node);
        }
    }
    free(g_localAllocators);

    // destroy global allocators
    for (int node = 0; node < (int)g_memGlobalCfg.m_nodeCount; ++node) {
        if (g_globalAllocators[node]) {
            for (MemBufferClass bufferClass = MEM_BUFFER_CLASS_SMALLEST; bufferClass < MEM_BUFFER_CLASS_COUNT;
                 ++bufferClass) {
                if (g_globalAllocators[node][bufferClass].m_bufferHeap != NULL) {
                    MemBufferAllocatorDestroy(&g_globalAllocators[node][bufferClass]);
                }
            }
            MemNumaFreeLocal(g_globalAllocators[node], sizeof(MemBufferAllocator) * MEM_BUFFER_CLASS_COUNT, node);
        }
    }
    free(g_globalAllocators);
}

static void AllocatorsPrint(LogLevel logLevel)
{
    if (MOT_CHECK_LOG_LEVEL(logLevel)) {
        StringBufferApply([logLevel](StringBuffer* stringBuffer) {
            AllocatorsToString(0, stringBuffer, MEM_REPORT_SUMMARY);
            MOT_LOG(logLevel, "%s", stringBuffer->m_buffer);
        });
    }
}

static void AllocatorsToString(int indent, StringBuffer* stringBuffer, MemReportMode reportMode)
{
    uint64_t cachedKb = 0;
    uint64_t freeKb = 0;
    uint64_t heapFreeKb = 0;
    uint64_t heapUsedKb = 0;
    MemBufferAllocatorStats stats;

    for (int node = 0; node < (int)g_memGlobalCfg.m_nodeCount; ++node) {
        for (MemBufferClass bufferClass = MEM_BUFFER_CLASS_SMALLEST; bufferClass < MEM_BUFFER_CLASS_COUNT;
             ++bufferClass) {
            if (g_globalAllocators[node][bufferClass].m_bufferHeap->m_allocatedChunkCount > 0) {
                if (reportMode == MEM_REPORT_SUMMARY) {
                    MemBufferAllocatorGetStats(&g_globalAllocators[node][bufferClass], &stats);
                    uint32_t bufferSizeKb = MemBufferClassToSizeKb(bufferClass);
                    cachedKb += stats.m_cachedBuffers * bufferSizeKb;
                    freeKb += stats.m_freeBuffers * bufferSizeKb;
                    heapFreeKb += stats.m_heapFreeBuffers * bufferSizeKb;
                    heapUsedKb += stats.m_heapUsedBuffers * bufferSizeKb;
                } else {
                    MemBufferAllocatorToString(
                        indent + PRINT_REPORT_INDENT, "Global", &g_globalAllocators[node][bufferClass], stringBuffer);
                    StringBufferAppend(stringBuffer, "\n");
                }
            }
        }
    }

    if (reportMode == MEM_REPORT_SUMMARY) {
        StringBufferAppend(stringBuffer,
            "%*sGlobal Allocators: cached=%" PRIu64 " MB, free-list=%" PRIu64 " MB, heap-free=%" PRIu64
            " MB, heap-used=%" PRIu64 " MB\n",
            indent,
            "",
            cachedKb / 1024,
            freeKb / 1024,
            heapFreeKb / 1024,
            heapUsedKb / 1024);
        cachedKb = 0;
        freeKb = 0;
        heapFreeKb = 0;
        heapUsedKb = 0;
    }

    for (int node = 0; node < (int)g_memGlobalCfg.m_nodeCount; ++node) {
        for (MemBufferClass bufferClass = MEM_BUFFER_CLASS_SMALLEST; bufferClass < MEM_BUFFER_CLASS_COUNT;
             ++bufferClass) {
            if (g_localAllocators[node][bufferClass].m_bufferHeap->m_allocatedChunkCount > 0) {
                if (reportMode == MEM_REPORT_SUMMARY) {
                    MemBufferAllocatorGetStats(&g_globalAllocators[node][bufferClass], &stats);
                    uint32_t bufferSizeKb = MemBufferClassToSizeKb(bufferClass);
                    cachedKb += stats.m_cachedBuffers * bufferSizeKb;
                    freeKb += stats.m_freeBuffers * bufferSizeKb;
                    heapFreeKb += stats.m_heapFreeBuffers * bufferSizeKb;
                    heapUsedKb += stats.m_heapUsedBuffers * bufferSizeKb;
                } else {
                    MemBufferAllocatorToString(
                        indent + PRINT_REPORT_INDENT, "Local", &g_localAllocators[node][bufferClass], stringBuffer);
                    StringBufferAppend(stringBuffer, "\n");
                }
            }
        }
    }

    if (reportMode == MEM_REPORT_SUMMARY) {
        StringBufferAppend(stringBuffer,
            "%*sLocal Allocators: cached=%" PRIu64 " MB, free-list=%" PRIu64 " MB, heap-free=%" PRIu64
            " MB, heap-used=%" PRIu64 " MB\n",
            indent,
            "",
            cachedKb / 1024,
            freeKb / 1024,
            heapFreeKb / 1024,
            heapUsedKb / 1024);
    }
}

}  // namespace MOT

extern "C" void MemBufferApiDump()
{
    MOT::StringBufferApply([](MOT::StringBuffer* stringBuffer) {
        MOT::MemBufferApiToString(0, "Debug Dump", stringBuffer, MOT::MEM_REPORT_DETAILED);
        (void)fprintf(stderr, "%s", stringBuffer->m_buffer);
        (void)fflush(stderr);
    });
}

extern "C" int MemBufferApiAnalyze(void* buffer)
{
    for (uint32_t i = 0; i < MOT::g_memGlobalCfg.m_nodeCount; ++i) {
        for (MOT::MemBufferClass bc = MOT::MEM_BUFFER_CLASS_KB_1; bc < MOT::MEM_BUFFER_CLASS_LARGEST; ++bc) {
            (void)fprintf(stderr,
                "Searching buffer %p in %s global-%u buffer allocator...\n",
                buffer,
                MOT::MemBufferClassToString(bc),
                i);
            if (MemBufferAllocatorAnalyze(&MOT::g_globalAllocators[i][bc], buffer)) {
                return 1;
            }
        }
    }
    for (uint32_t i = 0; i < MOT::g_memGlobalCfg.m_nodeCount; ++i) {
        for (MOT::MemBufferClass bc = MOT::MEM_BUFFER_CLASS_KB_1; bc < MOT::MEM_BUFFER_CLASS_LARGEST; ++bc) {
            (void)fprintf(stderr,
                "Searching buffer %p in %s local-%u buffer allocator...\n",
                buffer,
                MOT::MemBufferClassToString(bc),
                i);
            if (MemBufferAllocatorAnalyze(&MOT::g_localAllocators[i][bc], buffer)) {
                return 1;
            }
        }
    }
    return 0;
}
