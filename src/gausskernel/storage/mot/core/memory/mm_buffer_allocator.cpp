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
 * mm_buffer_allocator.cpp
 *    Memory buffer allocator implementation.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/memory/mm_buffer_allocator.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "mm_buffer_allocator.h"
#include "mm_numa.h"
#include "mm_cfg.h"
#include "utilities.h"
#include "mot_error.h"
#include "thread_id.h"
#include "mm_raw_chunk_dir.h"
#include "mm_raw_chunk_store.h"
#include "session_context.h"
#include "mm_api.h"
#include "knl/knl_instance.h"
#include <sched.h>

namespace MOT {
DECLARE_LOGGER(BufferAllocator, Memory)

static void MemChunkSnapshotClearThreadCache(MemBufferAllocator* bufferAllocator, MOTThreadId threadId);

extern void MemBufferAllocatorIssueError(int errorCode, const char* context, const char* format, ...)
{
    va_list args;
    va_start(args, format);
    MOT_REPORT_ERROR_V(errorCode, context, format, args);
    va_end(args);
}

extern int MemBufferAllocatorInit(MemBufferAllocator* bufferAllocator, int node, MemBufferClass bufferClass,
    MemAllocType allocType, int freeListDrainSize /* = MEM_DEFAULT_FREE_LIST_SIZE */)
{
    int result = 0;
    uint64_t chunkSnapshotSize = sizeof(MemBufferChunkSnapshot) *
                                 g_memGlobalCfg.m_maxThreadCount;  // this is a bit too much, but code is simpler
    uint64_t freeListArraySize = sizeof(MemBufferList) *
                                 g_memGlobalCfg.m_maxThreadCount;  // this is too much, but code is simpler

    freeListArraySize = L1_ALIGN(freeListArraySize);  // L1 Cache line aligned size

    // allocate whole buffer
    bufferAllocator->m_allocSize = sizeof(MemBufferHeap);
    bufferAllocator->m_allocSize += chunkSnapshotSize;
    bufferAllocator->m_allocSize += freeListArraySize;

    void* inplaceBuffer = MemNumaAllocLocal(bufferAllocator->m_allocSize, node);

    // check allocation result
    if (inplaceBuffer == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Buffer Allocator Initialization",
            "Failed to allocate %u bytes for buffer allocator on node %d for buffer class %s",
            bufferAllocator->m_allocSize,
            node,
            MemBufferClassToString(bufferClass));
        result = MOT_ERROR_OOM;
    } else {
        // initialize all pointers
        bufferAllocator->m_bufferHeap = (MemBufferHeap*)inplaceBuffer;
        bufferAllocator->m_chunkSnapshots = (MemBufferChunkSnapshot*)(bufferAllocator->m_bufferHeap + 1);
        MOT_ASSERT((uintptr_t)(bufferAllocator->m_chunkSnapshots) == L1_ALIGNED_PTR(bufferAllocator->m_chunkSnapshots));
        bufferAllocator->m_freeListArray =
            (MemBufferList*)(bufferAllocator->m_chunkSnapshots + g_memGlobalCfg.m_maxThreadCount);
        MOT_ASSERT((uintptr_t)(bufferAllocator->m_freeListArray) == L1_ALIGNED_PTR(bufferAllocator->m_freeListArray));
        bufferAllocator->m_freeListDrainSize = freeListDrainSize;

        // initialize heap and cache
        result = MemBufferHeapInit(bufferAllocator->m_bufferHeap, node, bufferClass, allocType);
        if (result != 0) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM,
                "Buffer Allocator Initialization",
                "Failed to initialize buffer heap for buffer allocator on node %d for buffer class %s",
                node,
                MemBufferClassToString(bufferClass));
            MemNumaFreeLocal(inplaceBuffer, bufferAllocator->m_allocSize, node);
        } else {
            errno_t erc = memset_s(bufferAllocator->m_chunkSnapshots,
                sizeof(MemBufferChunkSnapshot) * g_memGlobalCfg.m_maxThreadCount,
                0,
                sizeof(MemBufferChunkSnapshot) * g_memGlobalCfg.m_maxThreadCount);
            securec_check(erc, "\0", "\0");
            for (uint32_t i = 0; i < g_memGlobalCfg.m_maxThreadCount; ++i) {
                MemBufferListInit(&bufferAllocator->m_freeListArray[i]);
            }
        }
    }

    return result;
}

static void FreeToBufferHeap(void* object, void* userData)
{
    MemBufferHeap* bufferHeap = (MemBufferHeap*)userData;
    MemBufferChunkHeader* chunkHeader = (MemBufferChunkHeader*)MemRawChunkDirLookup(object);
    if (chunkHeader) {
        MemBufferHeader* bufferHeader = MemBufferChunkGetBufferHeader(chunkHeader, object);
        if (bufferHeader != NULL) {
            MemBufferHeapFree(bufferHeap, bufferHeader);
        }
    }
}

extern void MemBufferAllocatorDestroy(MemBufferAllocator* bufferAllocator)
{
    // we expect all caches to be free by now, if thread_pool is not enabled.
    if (!g_instance.attr.attr_common.enable_thread_pool) {
        for (uint32_t i = 0; i < g_memGlobalCfg.m_maxThreadCount; ++i) {
            if (bufferAllocator->m_chunkSnapshots[i].m_chunkHeaderSnapshot.m_allocatedCount <
                bufferAllocator->m_chunkSnapshots[i].m_chunkHeaderSnapshot.m_bufferCount) {
                MOT_LOG_WARN("Found non-empty cache for thread %u during buffer allocator destruction [%s-%s]",
                    i,
                    MemBufferClassToString(bufferAllocator->m_bufferHeap->m_bufferClass),
                    MemAllocTypeToString(bufferAllocator->m_bufferHeap->m_allocType));
            }
            if (bufferAllocator->m_freeListArray[i].m_count > 0) {
                MOT_LOG_WARN("Found non-empty free list for thread %u during buffer allocator destruction [%s-%s]",
                    i,
                    MemBufferClassToString(bufferAllocator->m_bufferHeap->m_bufferClass),
                    MemAllocTypeToString(bufferAllocator->m_bufferHeap->m_allocType));
            }
        }
    }

    // destroy buffer heap
    void* inplaceBuffer = (void*)bufferAllocator->m_bufferHeap;
    int node = bufferAllocator->m_bufferHeap->m_node;
    MemBufferHeapDestroy(bufferAllocator->m_bufferHeap);

    // free whole buffer
    uint32_t allocSize = bufferAllocator->m_allocSize;
    MemNumaFreeLocal(inplaceBuffer, allocSize, node);
}

inline void MemBufferChunkSnapshotInit(
    MemBufferChunkSnapshot* chunkSnapshot, MemBufferChunkHeader* chunkHeader, int newChunk)
{
    chunkSnapshot->m_realChunkHeader = chunkHeader;
    if (newChunk || (chunkSnapshot->m_chunkHeaderSnapshot.m_allocatedCount == 0)) {
        chunkSnapshot->m_newChunk = 1;
    } else {
        chunkSnapshot->m_newChunk = 0;
        chunkSnapshot->m_bitsetIndex = 0;
    }
}

extern MemBufferHeader* MemBufferChunkSnapshotAllocBuffer(MemBufferChunkSnapshot* chunkSnapshot)
{
    MemBufferHeader* bufferHeader = NULL;

    // do everything on snapshot, but the actual buffer is retrieved from the real chunk
    MemBufferChunkHeader* chunkHeader = &chunkSnapshot->m_chunkHeaderSnapshot;  // do bit-ops on snapshot
    MOT_ASSERT(chunkHeader->m_allocatedCount < chunkHeader->m_bufferCount);
    if (chunkSnapshot->m_newChunk) {  // optimize when free buffer is not called so often
        bufferHeader = MM_CHUNK_BUFFER_HEADER_AT(chunkSnapshot->m_realChunkHeader, chunkHeader->m_allocatedCount++);
    } else {
        for (uint32_t i = chunkSnapshot->m_bitsetIndex; i < chunkHeader->m_freeBitsetCount; ++i) {
            if (chunkHeader->m_freeBitset[i] != 0) {
                uint64_t freeBufferIndex = __builtin_clzll(chunkHeader->m_freeBitset[i]);
                chunkHeader->m_freeBitset[i] &= ~(((uint64_t)1) << (63 - freeBufferIndex));
                ++chunkHeader->m_allocatedCount;
                bufferHeader = MM_CHUNK_BUFFER_HEADER_AT(chunkSnapshot->m_realChunkHeader, (i << 6) + freeBufferIndex);
                break;
            }
            ++chunkSnapshot->m_bitsetIndex;
        }
    }
    if (chunkHeader->m_allocatedCount == chunkHeader->m_bufferCount) {
        // discard chunk
        chunkSnapshot->m_realChunkHeader = NULL;
    }

    MOT_ASSERT(bufferHeader);
    return bufferHeader;
}

static MemBufferChunkHeader* RefillGetChunkHeader(
    MemBufferAllocator* bufferAllocator, MemBufferChunkSnapshot* chunkSnapshot, int* newChunk)
{
    errno_t erc;
    *newChunk = 0;
    MemBufferChunkHeader* chunkHeader =
        MemBufferHeapSnapshotChunk(bufferAllocator->m_bufferHeap, &chunkSnapshot->m_chunkHeaderSnapshot);
    if (chunkHeader == NULL) {
        MOT_LOG_DIAG1("heap is empty");
        // get chunk header form round-robin global chunk pools
        if (bufferAllocator->m_bufferHeap->m_allocType == MEM_ALLOC_GLOBAL) {
            chunkHeader = (MemBufferChunkHeader*)MemRawChunkStoreAllocGlobal(bufferAllocator->m_bufferHeap->m_node);
        } else {
            chunkHeader = (MemBufferChunkHeader*)MemRawChunkStoreAllocLocal(bufferAllocator->m_bufferHeap->m_node);
        }
        if (chunkHeader != NULL) {
            MOT_LOG_DEBUG("allocated chunk from pool");
            MemBufferChunkInit(chunkHeader,
                chunkHeader->m_node,
                bufferAllocator->m_bufferHeap->m_allocType,
                bufferAllocator->m_bufferHeap->m_node,
                bufferAllocator->m_bufferHeap->m_bufferClass);
            erc = memcpy_s(&chunkSnapshot->m_chunkHeaderSnapshot,
                sizeof(MemBufferChunkHeader),
                chunkHeader,
                sizeof(MemBufferChunkHeader));
            securec_check(erc, "\0", "\0");
            MemBufferChunkMarkFull(chunkHeader);
            MOT_LOG_DIAG1("relinking new chunk");
            MemBufferHeapRelinkChunk(bufferAllocator->m_bufferHeap, chunkHeader, chunkHeader->m_bufferCount, 1);
            *newChunk = 1;
        }
    }
    MOT_ASSERT(!chunkHeader || chunkSnapshot->m_chunkHeaderSnapshot.m_allocatedCount <
                                   chunkSnapshot->m_chunkHeaderSnapshot.m_bufferCount);
    return chunkHeader;
}

extern void* MemBufferAllocatorRefillCache(MemBufferAllocator* bufferAllocator, MemBufferChunkSnapshot* chunkSnapshot)
{
    void* buffer = nullptr;

    int newChunk = 0;
    MemBufferChunkHeader* chunkHeader = RefillGetChunkHeader(bufferAllocator, chunkSnapshot, &newChunk);
    if (chunkHeader != NULL) {
        MemBufferChunkSnapshotInit(chunkSnapshot, chunkHeader, newChunk);
        MemBufferHeader* bufferHeader = MemBufferChunkSnapshotAllocBuffer(chunkSnapshot);
        if (bufferHeader) {
            buffer = bufferHeader->m_buffer;
        }
    }

    return buffer;
}

static inline void* MemChunkSnapshotAlloc(MemBufferAllocator* bufferAllocator)
{
    void* buffer = nullptr;
    MOTThreadId threadId = MOTCurrThreadId;
    if (unlikely(threadId == INVALID_THREAD_ID)) {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
            "Allocate Chunk Snapshot",
            "Invalid attempt to allocate a chunk snapshot without a current thread identifier");
    } else {
        MemBufferChunkSnapshot* chunkSnapshot = &bufferAllocator->m_chunkSnapshots[threadId];
        if (chunkSnapshot->m_realChunkHeader != NULL) {
            MemBufferHeader* bufferHeader = MemBufferChunkSnapshotAllocBuffer(chunkSnapshot);
            MOT_ASSERT(bufferHeader != NULL);
            if (bufferHeader != NULL) {
                buffer = bufferHeader->m_buffer;
                MOT_LOG_DIAG1(
                    "Buffer allocator [%s]: Allocated from chunk snapshot buffer %p at index %u from chunk %p (%u/%u)",
                    MemBufferClassToString(bufferAllocator->m_bufferHeap->m_bufferClass),
                    buffer,
                    bufferHeader->m_index,
                    chunkSnapshot->m_realChunkHeader,
                    chunkSnapshot->m_chunkHeaderSnapshot.m_allocatedCount,
                    chunkSnapshot->m_chunkHeaderSnapshot.m_bufferCount);
            }
        }
    }
    return buffer;
}

extern MemBufferHeader* MemBufferCheckDoubleFree(void* buffer)
{
    MemBufferHeader* bufferHeader = NULL;

    MemBufferChunkHeader* chunkHeader = (MemBufferChunkHeader*)MemRawChunkDirLookup(buffer);
    if (unlikely(chunkHeader == NULL)) {
        MOT_LOG_PANIC("Invalid buffer %p passed to MemBufferAllocatorFree(): source chunk not found", buffer);
        MOTAbort(buffer);  // force dump, this is a serious condition, crash is expected at any moment
    } else {
        if (unlikely(chunkHeader->m_chunkType != MEM_CHUNK_TYPE_BUFFER)) {
            MOT_LOG_PANIC("Invalid buffer %p passed to MemBufferAllocatorFree(): Invalid source chunk type %s",
                buffer,
                MemChunkTypeToString(chunkHeader->m_chunkType));
            MOTAbort(buffer);  // force dump, this is a serious condition, crash is expected at any moment
        } else {
            bufferHeader = MemBufferChunkGetBufferHeader(chunkHeader, buffer);
            if (unlikely(bufferHeader == NULL)) {
                MOT_LOG_PANIC(
                    "Invalid buffer %p passed to MemBufferAllocatorFree(): Buffer not found in its source chunk",
                    buffer);
                MOTAbort(buffer);  // force dump, this is a serious condition, crash is expected at any moment
            } else {
                if (MemBufferChunkIsFree(chunkHeader, bufferHeader)) {
                    MemBufferChunkOnDoubleFree(chunkHeader, bufferHeader);
                    bufferHeader = NULL;  // currently this is dead code, but if anything changes, we are still safe
                }
            }
        }
    }
    return bufferHeader;
}

extern void MemBufferAllocatorFree(MemBufferAllocator* bufferAllocator, void* buffer)
{
    MemBufferHeader* bufferHeader = MemBufferCheckDoubleFree(buffer);

    if (likely(bufferHeader)) {
        MemBufferChunkHeader* bufferChunkHeader = MemBufferChunkGetChunkHeader(buffer);
        if (MOTCurrThreadId == INVALID_THREAD_ID) {
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                "Free Buffer",
                "Invalid attempt to free a buffer without a current thread identifier");
        } else if (MOTCurrentNumaNodeId == MEM_INVALID_NODE) {
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                "Free Buffer",
                "Invalid attempt to free a buffer without a current NUMA node identifier");
        } else if (bufferChunkHeader->m_allocatorNode != bufferAllocator->m_bufferHeap->m_node) {
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                "Free Buffer",
                "Invalid attempt to free buffer %p from allocator of node %d on buffer allocator of node %d",
                buffer,
                bufferChunkHeader->m_allocatorNode,
                bufferAllocator->m_bufferHeap->m_node);
        } else {
            MemBufferList* bufferList = &bufferAllocator->m_freeListArray[MOTCurrThreadId];
            MemBufferListPush(bufferList, bufferHeader);
            if (bufferList->m_count >= bufferAllocator->m_freeListDrainSize) {
                MemBufferHeapFreeMultiple(bufferAllocator->m_bufferHeap, bufferList);
            }
        }
    }
}

static void MemChunkSnapshotClearThreadCache(MemBufferAllocator* bufferAllocator, MOTThreadId threadId)
{
    MOT_LOG_TRACE("Clearing chunk snapshot and free list for thread %u at buffer allocator [%s-%s]",
        (unsigned)threadId,
        MemBufferClassToString(bufferAllocator->m_bufferHeap->m_bufferClass),
        MemAllocTypeToString(bufferAllocator->m_bufferHeap->m_allocType));
    MemBufferChunkSnapshot* chunkSnapshot = &bufferAllocator->m_chunkSnapshots[threadId];
    MemBufferList* bufferList = &bufferAllocator->m_freeListArray[threadId];

    // allocate all available buffers from the snapshot, put them in the free buffer list for the thread,
    // and free them all together
    if (chunkSnapshot->m_realChunkHeader != NULL) {
        uint32_t itemsAvailable =
            chunkSnapshot->m_chunkHeaderSnapshot.m_bufferCount - chunkSnapshot->m_chunkHeaderSnapshot.m_allocatedCount;
        for (uint32_t i = 0; i < itemsAvailable; ++i) {
            MemBufferHeader* bufferHeader = MemBufferChunkSnapshotAllocBuffer(chunkSnapshot);
            MOT_ASSERT(bufferHeader != NULL);
            MemBufferListPush(bufferList, bufferHeader);
        }
        // discard snapshot
        chunkSnapshot->m_realChunkHeader = NULL;
    }
    MemBufferHeapFreeMultiple(bufferAllocator->m_bufferHeap, bufferList);
    MOT_LOG_TRACE("Thread id %u Buffer list %p size is: %u", (unsigned)threadId, bufferList, bufferList->m_count);
}

extern void MemBufferAllocatorClearThreadCache(MemBufferAllocator* bufferAllocator)
{
    // ATTENTION: If current thread id does not exist, then we do NOT want to allocate it on demand
    // since this is cleanup code (this causes severe errors when standby node becomes the master
    // and restarts, since it has a thread id that will be given also to another thread)
    if (MOTCurrThreadId == INVALID_THREAD_ID) {
        MOT_LOG_TRACE(
            "Silently ignoring invalid attempt to clear cache for buffer allocator without current thread identifier");
    } else {
        MOT_LOG_TRACE("Clearing cache for thread %u during buffer allocator destruction [%s-%s]",
            MOTCurrThreadId,
            MemBufferClassToString(bufferAllocator->m_bufferHeap->m_bufferClass),
            MemAllocTypeToString(bufferAllocator->m_bufferHeap->m_allocType));
        MemChunkSnapshotClearThreadCache(bufferAllocator, MOTCurrThreadId);
    }
}

extern void MemBufferAllocatorGetStats(MemBufferAllocator* bufferAllocator, MemBufferAllocatorStats* stats)
{
    errno_t erc = memset_s(stats, sizeof(MemBufferAllocatorStats), 0, sizeof(MemBufferAllocatorStats));
    securec_check(erc, "\0", "\0");
    for (int i = 0; i < GetMaxThreadCount(); ++i) {
        MemBufferChunkHeader* header = &bufferAllocator->m_chunkSnapshots[i].m_chunkHeaderSnapshot;
        stats->m_cachedBuffers += header->m_bufferCount - header->m_allocatedCount;
        stats->m_freeBuffers += bufferAllocator->m_freeListArray[i].m_count;
    }
    uint64_t buffersInChunk = bufferAllocator->m_bufferHeap->m_maxBuffersInChunk;
    stats->m_heapChunks = bufferAllocator->m_bufferHeap->m_allocatedChunkCount;
    stats->m_heapUsedBuffers = bufferAllocator->m_bufferHeap->m_allocatedBufferCount;
    stats->m_heapFreeBuffers = (buffersInChunk * stats->m_heapChunks - stats->m_heapUsedBuffers);
}

extern void MemBufferAllocatorFormatStats(int indent, const char* name, StringBuffer* stringBuffer,
    MemBufferAllocator* bufferAllocator, MemBufferAllocatorStats* stats)
{
    StringBufferAppend(stringBuffer,
        "%*sBuffer allocator %s [node=%d, buffer-size=%s] statistics:\n",
        indent,
        "",
        name,
        bufferAllocator->m_bufferHeap->m_node,
        MemBufferClassToString(bufferAllocator->m_bufferHeap->m_bufferClass));
    StringBufferAppend(stringBuffer,
        "%*sCached buffer count:   %" PRIu64 "\n",
        indent + PRINT_REPORT_INDENT,
        "",
        stats->m_cachedBuffers);
    StringBufferAppend(stringBuffer,
        "%*sFree buffer count:     %" PRIu64 "\n",
        indent + PRINT_REPORT_INDENT,
        "",
        stats->m_freeBuffers);
    StringBufferAppend(
        stringBuffer, "%*sHeap chunk count:      %" PRIu64 "\n", indent + PRINT_REPORT_INDENT, "", stats->m_heapChunks);
    StringBufferAppend(stringBuffer,
        "%*sHeap used chunk count: %" PRIu64 "\n",
        indent + PRINT_REPORT_INDENT,
        "",
        stats->m_heapUsedBuffers);
    StringBufferAppend(stringBuffer,
        "%*sHeap free chunk count: %" PRIu64 "\n",
        indent + PRINT_REPORT_INDENT,
        "",
        stats->m_heapFreeBuffers);
}

extern void MemBufferAllocatorPrintStats(
    const char* name, LogLevel logLevel, MemBufferAllocator* bufferAllocator, MemBufferAllocatorStats* stats)
{
    if (MOT_CHECK_LOG_LEVEL(logLevel)) {
        StringBufferApply([name, logLevel, bufferAllocator, stats](StringBuffer* stringBuffer) {
            MemBufferAllocatorFormatStats(0, name, stringBuffer, bufferAllocator, stats);
            MOT_LOG(logLevel, "%s", stringBuffer->m_buffer);
        });
    }
}

extern void MemBufferAllocatorPrintCurrentStats(
    const char* name, LogLevel logLevel, MemBufferAllocator* bufferAllocator)
{
    if (MOT_CHECK_LOG_LEVEL(logLevel)) {
        MemBufferAllocatorStats stats;
        MemBufferAllocatorGetStats(bufferAllocator, &stats);
        MemBufferAllocatorPrintStats(name, logLevel, bufferAllocator, &stats);
    }
}

extern void MemBufferAllocatorPrint(const char* name, LogLevel logLevel, MemBufferAllocator* bufferAllocator,
    MemReportMode reportMode /* = MEM_REPORT_SUMMARY */)
{
    if (MOT_CHECK_LOG_LEVEL(logLevel)) {
        StringBufferApply([name, logLevel, bufferAllocator, reportMode](StringBuffer* stringBuffer) {
            MemBufferAllocatorToString(0, name, bufferAllocator, stringBuffer, reportMode);
            MOT_LOG(logLevel, "%s", stringBuffer->m_buffer);
        });
    }
}

extern void MemBufferAllocatorToString(int indent, const char* name, MemBufferAllocator* bufferAllocator,
    StringBuffer* stringBuffer, MemReportMode reportMode /* = MEM_REPORT_SUMMARY */)
{
    errno_t erc;
    StringBufferAppend(stringBuffer,
        "%*s%s Buffer Allocator [node=%d, buffer-size=%s] = {\n",
        indent,
        "",
        name,
        bufferAllocator->m_bufferHeap->m_node,
        MemBufferClassToString(bufferAllocator->m_bufferHeap->m_bufferClass));

    MemBufferAllocatorStats stats = {};
    MemBufferAllocatorGetStats(bufferAllocator, &stats);
    MemBufferAllocatorFormatStats(0, name, stringBuffer, bufferAllocator, &stats);

    if (reportMode == MEM_REPORT_DETAILED) {
        // print chunk snapshots
        if (bufferAllocator->m_chunkSnapshots) {
            for (uint32_t i = 0; i < g_memGlobalCfg.m_maxThreadCount; ++i) {
                MemBufferChunkSnapshot* chunkSnapshot = &bufferAllocator->m_chunkSnapshots[i];
                if (chunkSnapshot->m_realChunkHeader) {
                    StringBufferAppend(stringBuffer, "%*sChunk Snapshot %u: ", indent + PRINT_REPORT_INDENT, "", i);
                    MemBufferChunkHeaderToString(&chunkSnapshot->m_chunkHeaderSnapshot, stringBuffer, reportMode);
                    StringBufferAppend(stringBuffer, "\n");
                }
            }
        }

        // print free lists
        if (bufferAllocator->m_freeListArray) {
            for (uint32_t i = 0; i < g_memGlobalCfg.m_maxThreadCount; ++i) {
                MemBufferList* freeList = &bufferAllocator->m_freeListArray[i];
                if (freeList->m_count > 0) {
                    const uint32_t nameLen = 32;
                    char name[nameLen];
                    erc = snprintf_s(name, nameLen, nameLen - 1, "TID %u Free", i);
                    securec_check_ss(erc, "\0", "\0");
                    MemBufferListToString(indent + PRINT_REPORT_INDENT, name, freeList, stringBuffer);
                    StringBufferAppend(stringBuffer, "\n");
                }
            }
        }

        MemBufferHeapToString(indent + PRINT_REPORT_INDENT, "", bufferAllocator->m_bufferHeap, stringBuffer);
        StringBufferAppend(stringBuffer, "\n%*s}", indent, "");
    }
}

}  // namespace MOT

extern "C" void MemBufferAllocatorDump(void* arg)
{
    MOT::MemBufferAllocator* bufferAllocator = (MOT::MemBufferAllocator*)arg;

    MOT::StringBufferApply([bufferAllocator](MOT::StringBuffer* stringBuffer) {
        MOT::MemBufferAllocatorToString(0, "Debug Dump", bufferAllocator, stringBuffer, MOT::MEM_REPORT_DETAILED);
        fprintf(stderr, "%s", stringBuffer->m_buffer);
        fflush(stderr);
    });
}

extern "C" int MemBufferAllocatorAnalyze(void* allocator, void* buffer)
{
    MOT::MemBufferAllocator* bufferAllocator = (MOT::MemBufferAllocator*)allocator;

    // search buffer in caches
    int found = 0;
    for (uint32_t i = 0; i < MOT::g_memGlobalCfg.m_maxThreadCount; ++i) {
        MOT::MemBufferChunkHeader* chunkHeader = &bufferAllocator->m_chunkSnapshots[i].m_chunkHeaderSnapshot;
        if (chunkHeader->m_chunkType == MOT::MEM_CHUNK_TYPE_BUFFER) {
            MOT::MemBufferHeader* bufferHeader = MOT::MemBufferChunkGetBufferHeader(chunkHeader, buffer);
            if (bufferHeader) {
                bool isAllocated = MOT::MemBufferChunkIsAllocated(chunkHeader, bufferHeader);
                fprintf(stderr,
                    "Found %s buffer %p in position %" PRIu64
                    " at chunk snapshot for thread %u of buffer allocator [node=%d, buffer-size=%s]\n",
                    isAllocated ? "allocated" : "free",
                    buffer,
                    bufferHeader->m_index,
                    i,
                    bufferAllocator->m_bufferHeap->m_node,
                    MOT::MemBufferClassToString(bufferAllocator->m_bufferHeap->m_bufferClass));
                found = 1;
                break;
            }
        }
    }

    if (!found) {
        // search buffer in free list
        for (uint32_t i = 0; i < MOT::g_memGlobalCfg.m_maxThreadCount; ++i) {
            MOT::MemBufferHeader* itr = bufferAllocator->m_freeListArray[i].m_head;
            while (itr && !found) {
                if (itr->m_buffer == buffer) {
                    fprintf(stderr,
                        "Found buffer %p in free list for thread %u of buffer allocator [node=%d, buffer-size=%s]\n",
                        buffer,
                        i,
                        bufferAllocator->m_bufferHeap->m_node,
                        MOT::MemBufferClassToString(bufferAllocator->m_bufferHeap->m_bufferClass));
                    found = 1;
                    break;
                }
                itr = itr->m_next;
            }
            if (found) {
                break;
            }
        }
    }

    if (!found) {
        MOT::MemRawChunkPool* chunkPool = nullptr;
        int node = bufferAllocator->m_bufferHeap->m_node;
        if (bufferAllocator->m_bufferHeap->m_allocType == MOT::MEM_ALLOC_GLOBAL) {
            chunkPool = MOT::MemRawChunkStoreGetGlobalPool(node);
        } else {
            chunkPool = MOT::MemRawChunkStoreGetLocalPool(node);
        }
        found = MemRawChunkPoolAnalyze(chunkPool, buffer);
    }

    return found;
}
