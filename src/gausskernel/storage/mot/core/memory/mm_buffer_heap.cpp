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
 * mm_buffer_heap.cpp
 *    A heap for buffers of a specific size class.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/memory/mm_buffer_heap.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "mm_buffer_heap.h"
#include "mm_cfg.h"
#include "utilities.h"
#include "mm_raw_chunk_store.h"
#include "knl/knl_instance.h"
#include <string.h>
#include <set>

namespace MOT {
DECLARE_LOGGER(BufferHeap, Memory)

// fullness bit-set utility macros
#define CHUNK_FULLNESS_INDEX(chunkHeader) (((chunkHeader)->m_bufferCount - (chunkHeader)->m_allocatedCount) - 1)
#define FULLNESS_BIT(fullnessOffset) (((uint64_t)1) << (63 - (fullnessOffset)))
#define RAISE_FULLNESS_BIT(bufferHeap, fullnessIndex, fullnessOffset) \
    ((bufferHeap)->m_fullnessBitset[fullnessIndex] |= FULLNESS_BIT(fullnessOffset))
#define RESET_FULLNESS_BIT(bufferHeap, fullnessIndex, fullnessOffset) \
    ((bufferHeap)->m_fullnessBitset[fullnessIndex] &= ~FULLNESS_BIT(fullnessOffset))

// helpers
static inline int GetNonEmptyFullnessIndex(MemBufferHeap* bufferHeap);
static inline int GetNonEmptyEmptiestIndex(MemBufferHeap* bufferHeap);
static inline int EnsureChunkExists(MemBufferHeap* bufferHeap);
static inline MemBufferChunkHeader* MemBufferHeapPopChunk(
    MemBufferHeap* bufferHeap, int fullnessIndex, int fullnessOffset);
static inline int MemBufferHeapPushChunk(
    MemBufferHeap* bufferHeap, MemBufferChunkHeader* chunkHeader, int allowFreeChunk);
static inline void MemBufferHeapUnlinkChunk(MemBufferHeap* bufferHeap, MemBufferChunkHeader* chunkHeader);

static inline void MemChunkListPush(MemBufferChunkHeader** chunkList, MemBufferChunkHeader* chunkHeader);
static inline MemBufferChunkHeader* MemChunkListPop(MemBufferChunkHeader** chunkList);
static inline void MemChunkListUnlink(MemBufferChunkHeader** chunkList, MemBufferChunkHeader* chunkHeader);
static inline void MemChunkListFree(MemBufferHeap* bufferHeap, MemBufferChunkHeader* chunkList);
static void AssertHeapValid(MemBufferHeap* bufferHeap);
static void AssertChunkRemoved(MemBufferHeap* bufferHeap, MemBufferChunkHeader* chunkHeader);
static int AllocAndPushChunk(MemBufferHeap* bufferHeap);

#ifdef MOT_DEBUG
#define ASSERT_HEAP_VALID AssertHeapValid
#define ASSERT_CHUNK_REMOVED AssertChunkRemoved
#else
#define ASSERT_HEAP_VALID(X)
#define ASSERT_CHUNK_REMOVED(X, Y)
#endif

extern int MemBufferHeapInit(MemBufferHeap* bufferHeap, int node, MemBufferClass bufferClass, MemAllocType allocType)
{
    int result = MemLockInitialize(&bufferHeap->m_lock);
    if (result != 0) {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
            "Buffer Heap Initialization",
            "Failed to initialize %s buffer heap lock on node %d",
            MemBufferClassToString(bufferClass),
            node);
    } else {
        bufferHeap->m_node = node;
        bufferHeap->m_allocType = allocType;
        bufferHeap->m_bufferClass = bufferClass;
        bufferHeap->m_bufferSizeKb = MemBufferClassToSizeKb(bufferClass);
        bufferHeap->m_maxBuffersInChunk = MemBufferChunksMaxBuffers(bufferHeap->m_bufferSizeKb);
        bufferHeap->m_fullnessBitsetCount = (bufferHeap->m_maxBuffersInChunk + 63) / 64;
        for (uint32_t i = 0; i < bufferHeap->m_fullnessBitsetCount; ++i) {
            bufferHeap->m_fullnessBitset[i] = 0;
        }
        errno_t erc = memset_s(bufferHeap->m_fullnessDirectory,
            sizeof(bufferHeap->m_fullnessDirectory),
            0,
            sizeof(bufferHeap->m_fullnessDirectory));
        securec_check(erc, "\0", "\0");
        bufferHeap->m_fullChunkList = NULL;
        bufferHeap->m_allocatedChunkCount = 0;
        bufferHeap->m_allocatedBufferCount = 0;
    }
    return result;
}

extern void MemBufferHeapDestroy(MemBufferHeap* bufferHeap)
{
    if (!g_instance.attr.attr_common.enable_thread_pool) {
        if ((bufferHeap->m_allocatedChunkCount > 0) || (bufferHeap->m_allocatedBufferCount > 0)) {
            MOT_LOG_WARN("Destroying a non-empty buffer heap (node: %d, buffer class: %s, chunks: %u, buffers: %u)",
                bufferHeap->m_node,
                MemBufferClassToString(bufferHeap->m_bufferClass),
                bufferHeap->m_allocatedChunkCount,
                bufferHeap->m_allocatedBufferCount);
        }
    }

    // free all full chunks
    if (bufferHeap->m_fullChunkList != nullptr) {
        // we put this at trace level to avoid log flooding
        MOT_LOG_TRACE("WARNING: Encountered non-empty full chunk list in buffer heap");
        MemChunkListFree(bufferHeap, bufferHeap->m_fullChunkList);
    }

    // now free all partially full chunks
    for (uint32_t i = 0; i < bufferHeap->m_maxBuffersInChunk; ++i) {
        if (bufferHeap->m_fullnessDirectory[i] != NULL) {
            // we put this at trace level to avoid log flooding
            MOT_LOG_TRACE("WARNING: Encountered non empty chunk list at fullness index %u in buffer heap", i);
            MemChunkListFree(bufferHeap, bufferHeap->m_fullnessDirectory[i]);
        }
    }
    MOT_LOG_TRACE("Heap status before destruction: chunks = %u, buffers = %u",
        bufferHeap->m_allocatedChunkCount,
        bufferHeap->m_allocatedBufferCount);

    // error ignored, it is already reported, and we have nothing to do about it
    (void)MemLockDestroy(&bufferHeap->m_lock);
}

extern MemBufferHeader* MemBufferHeapAlloc(MemBufferHeap* bufferHeap)
{
    MemBufferHeader* bufferHeader = NULL;
    MemLockAcquire(&bufferHeap->m_lock);

    // allocate chunk if heap is empty
    int fullnessIndex = EnsureChunkExists(bufferHeap);
    if (fullnessIndex != -1) {  // need to check, could be out of memory
        // unlink first first chunk in fullest chunk list
        int fullnessOffset = __builtin_clzll(bufferHeap->m_fullnessBitset[fullnessIndex]);
        MemBufferChunkHeader* chunkHeader = MemBufferHeapPopChunk(bufferHeap, fullnessIndex, fullnessOffset);

        // get buffer from chunk
        bufferHeader = MemBufferChunkAllocBuffer(chunkHeader);
        MOT_LOG_DEBUG("Allocated buffer %p from chunk %p (fullness index=%u, offset=%u\n",
            bufferHeader,
            chunkHeader,
            (unsigned)fullnessIndex,
            (unsigned)fullnessOffset);
        ++bufferHeap->m_allocatedBufferCount;

        // link modified chunk head to its new fullness list
        (void)MemBufferHeapPushChunk(bufferHeap, chunkHeader, 1);
    }

    MemLockRelease(&bufferHeap->m_lock);

    return bufferHeader;
}

extern void MemBufferHeapFree(MemBufferHeap* bufferHeap, MemBufferHeader* bufferHeader)
{
    // get chunk header
    MemBufferChunkHeader* chunkHeader = MM_CHUNK_FROM_BUFFER_HEADER(bufferHeader);

    // serialize access
    MemLockAcquire(&bufferHeap->m_lock);

    // unlink chunk from its list and update fullness bit-set if list became empty
    MemBufferHeapUnlinkChunk(bufferHeap, chunkHeader);

    // return buffer to chunk
    MemBufferChunkFreeBuffer(chunkHeader, bufferHeader);
    MOT_ASSERT(bufferHeap->m_allocatedBufferCount > 0);
    --bufferHeap->m_allocatedBufferCount;

    // push chunk on its fullness list head
    int allowFreeChunk = bufferHeap->m_inReserveMode ? 0 : 1;
    (void)MemBufferHeapPushChunk(bufferHeap, chunkHeader, allowFreeChunk);

    MemLockRelease(&bufferHeap->m_lock);
}

extern uint32_t MemBufferHeapAllocMultiple(MemBufferHeap* bufferHeap, uint32_t itemCount, MemBufferList* bufferList)
{
    uint32_t buffersAllocated = 0;
    MemLockAcquire(&bufferHeap->m_lock);

    while (buffersAllocated < itemCount) {
        // allocate chunk if heap is empty
        int fullnessIndex = EnsureChunkExists(bufferHeap);
        if (fullnessIndex != -1) {
            // unlink first first chunk in fullest chunk list
            int fullnessOffset = __builtin_clzll(bufferHeap->m_fullnessBitset[fullnessIndex]);
            MemBufferChunkHeader* chunkHeader = MemBufferHeapPopChunk(bufferHeap, fullnessIndex, fullnessOffset);

            // get buffer from chunk
            uint32_t buffersAllocatedCurr =
                MemBufferChunkAllocMultipleBuffers(chunkHeader, itemCount - buffersAllocated, bufferList);
            if (buffersAllocatedCurr == 0) {  // error
                break;
            }

            buffersAllocated += buffersAllocatedCurr;
            bufferHeap->m_allocatedBufferCount += buffersAllocatedCurr;

            // link modified chunk head to its new fullness list
            (void)MemBufferHeapPushChunk(bufferHeap, chunkHeader, 1);
        } else {
            break;  // out of memory
        }
    }

    MemLockRelease(&bufferHeap->m_lock);

    return buffersAllocated;
}

extern MemBufferChunkHeader* MemBufferHeapExtractChunk(MemBufferHeap* bufferHeap)
{
    MemBufferChunkHeader* chunkHeader = NULL;
    MemLockAcquire(&bufferHeap->m_lock);
    ASSERT_HEAP_VALID(bufferHeap);

    // get fullest fit if any
    int nonEmptyIndex = GetNonEmptyFullnessIndex(bufferHeap);
    if (nonEmptyIndex != -1) {
        int fullnessOffset = __builtin_clzll(bufferHeap->m_fullnessBitset[nonEmptyIndex]);
        chunkHeader = MemBufferHeapPopChunk(bufferHeap, nonEmptyIndex, fullnessOffset);
        MOT_ASSERT(chunkHeader);
        MOT_ASSERT(chunkHeader->m_allocatedCount < chunkHeader->m_bufferCount);
    }

    ASSERT_HEAP_VALID(bufferHeap);
    ASSERT_CHUNK_REMOVED(bufferHeap, chunkHeader);
    MemLockRelease(&bufferHeap->m_lock);

    return chunkHeader;
}

extern void MemBufferHeapRelinkChunk(
    MemBufferHeap* bufferHeap, MemBufferChunkHeader* chunkHeader, uint32_t buffersAllocated, int newChunk)
{
    // serialize access
    MOT_ASSERT(chunkHeader->m_allocatedCount == chunkHeader->m_bufferCount);
    MemLockAcquire(&bufferHeap->m_lock);
    ASSERT_HEAP_VALID(bufferHeap);

    // push chunk on its fullness list head
    int allowFreeChunk = bufferHeap->m_inReserveMode ? 0 : 1;
    (void)MemBufferHeapPushChunk(bufferHeap, chunkHeader, allowFreeChunk);
    bufferHeap->m_allocatedBufferCount += buffersAllocated;
    if (newChunk) {
        ++bufferHeap->m_allocatedChunkCount;
    }

    ASSERT_HEAP_VALID(bufferHeap);
    MemLockRelease(&bufferHeap->m_lock);
}

extern MemBufferChunkHeader* MemBufferHeapSnapshotChunk(MemBufferHeap* bufferHeap, MemBufferChunkHeader* chunkSnapshot)
{
    MemBufferChunkHeader* chunkHeader = NULL;
    MemLockAcquire(&bufferHeap->m_lock);
    ASSERT_HEAP_VALID(bufferHeap);

    // get emptiest chunk to optimize cache (this way snapshot has more buffers to give)
    int nonEmptyIndex = GetNonEmptyEmptiestIndex(bufferHeap);
    if (nonEmptyIndex != -1) {
        // attention: we call ctz and **NOT** clz (trailing zeroes, not leading zeroes) since we search in
        // reverse order for the emptiest non-empty chunk list
        int fullnessOffset = __builtin_ctzll(bufferHeap->m_fullnessBitset[nonEmptyIndex]);
        chunkHeader = MemBufferHeapPopChunk(bufferHeap, nonEmptyIndex, 63 - fullnessOffset);
        MOT_ASSERT(chunkHeader);
        MOT_ASSERT(chunkHeader->m_allocatedCount < chunkHeader->m_bufferCount);
        ASSERT_CHUNK_REMOVED(bufferHeap, chunkHeader);

        // take snapshot, mark as full and put back
        errno_t erc = memcpy_s(chunkSnapshot, sizeof(MemBufferChunkHeader), chunkHeader, sizeof(MemBufferChunkHeader));
        securec_check(erc, "\0", "\0");
        MemBufferChunkMarkFull(chunkHeader);
        (void)MemBufferHeapPushChunk(bufferHeap, chunkHeader, 0);

        // update statistics
        bufferHeap->m_allocatedBufferCount += chunkSnapshot->m_bufferCount - chunkSnapshot->m_allocatedCount;
    }

    ASSERT_HEAP_VALID(bufferHeap);
    MemLockRelease(&bufferHeap->m_lock);

    return chunkHeader;
}

extern void MemBufferHeapFreeMultiple(MemBufferHeap* bufferHeap, MemBufferList* bufferList)
{
    // except for taking a lock once instead of multiple times, there is not much change here
    MemLockAcquire(&bufferHeap->m_lock);
    ASSERT_HEAP_VALID(bufferHeap);

    while (bufferList->m_count != 0) {
        MemBufferHeader* bufferHeader = MemBufferListPop(bufferList);

        // get chunk header
        MemBufferChunkHeader* chunkHeader = MM_CHUNK_FROM_BUFFER_HEADER(bufferHeader);

        // unlink chunk from its list and update fullness bit-set if list became empty
        MemBufferHeapUnlinkChunk(bufferHeap, chunkHeader);
        ASSERT_CHUNK_REMOVED(bufferHeap, chunkHeader);

        // return buffer to chunk
        MemBufferChunkFreeBuffer(chunkHeader, bufferHeader);
        MOT_ASSERT(bufferHeap->m_allocatedBufferCount > 0);
        --bufferHeap->m_allocatedBufferCount;

        // push chunk on its fullness list head
        int allowFreeChunk = bufferHeap->m_inReserveMode ? 0 : 1;
        (void)MemBufferHeapPushChunk(bufferHeap, chunkHeader, allowFreeChunk);
    }

    ASSERT_HEAP_VALID(bufferHeap);
    MemLockRelease(&bufferHeap->m_lock);
}

extern int MemBufferHeapReserve(MemBufferHeap* bufferHeap, uint32_t bufferCount)
{
    int result = 0;
    MemLockAcquire(&bufferHeap->m_lock);

    // raise reserve mode flag
    bufferHeap->m_inReserveMode = 1;

    // count number of available buffers
    uint32_t availableBuffers = 0;
    for (uint32_t i = 0; i < bufferHeap->m_maxBuffersInChunk; ++i) {
        MemBufferChunkHeader* chunkHeader = bufferHeap->m_fullnessDirectory[i];
        while (chunkHeader != nullptr) {
            availableBuffers += (chunkHeader->m_bufferCount - chunkHeader->m_allocatedCount);
            chunkHeader = chunkHeader->m_nextChunk;
        }
    }

    if (availableBuffers < bufferCount) {
        // compute number of required chunks (round up)
        uint32_t requiredBuffers = bufferCount - availableBuffers;
        uint32_t requiredBytes =
            (requiredBuffers + bufferHeap->m_maxBuffersInChunk - 1) * bufferHeap->m_bufferSizeKb * KILO_BYTE;
        uint32_t requiredChunks = requiredBytes / (MEM_CHUNK_SIZE_MB * MEGA_BYTE);
        for (uint32_t i = 0; i < requiredChunks; ++i) {
            result = AllocAndPushChunk(bufferHeap);
            if (result == -1) {
                result = MOT_ERROR_OOM;
                bufferHeap->m_inReserveMode = 0;
                break;
            }
        }
    }

    MemLockRelease(&bufferHeap->m_lock);
    return result;
}

extern int MemBufferHeapUnreserve(MemBufferHeap* bufferHeap)
{
    // just reset reserve flag and let things happen naturally
    MemLockAcquire(&bufferHeap->m_lock);
    bufferHeap->m_inReserveMode = 0;

    // free all empty chunks in compact mode
    if (g_memGlobalCfg.m_storeMemoryPolicy == MEM_STORE_COMPACT) {
        while (bufferHeap->m_fullnessDirectory[bufferHeap->m_maxBuffersInChunk - 1] != nullptr) {
            MemBufferChunkHeader* chunkHeader = bufferHeap->m_fullnessDirectory[bufferHeap->m_maxBuffersInChunk - 1];
            MemBufferHeapUnlinkChunk(bufferHeap, chunkHeader);
            (void)MemBufferHeapPushChunk(bufferHeap, chunkHeader, 1);
        }
    }
    MemLockRelease(&bufferHeap->m_lock);
    return 0;
}

extern void MemBufferHeapPrint(const char* name, LogLevel logLevel, MemBufferHeap* bufferHeap)
{
    if (MOT_CHECK_LOG_LEVEL(logLevel)) {
        StringBufferApply([name, logLevel, bufferHeap](StringBuffer* stringBuffer) {
            MemBufferHeapToString(0, name, bufferHeap, stringBuffer);
            MOT_LOG(logLevel, "%s", stringBuffer->m_buffer);
        });
    }
}

extern void MemBufferHeapToString(int indent, const char* name, MemBufferHeap* bufferHeap, StringBuffer* stringBuffer)
{
    errno_t erc;
    StringBufferAppend(stringBuffer,
        "%*s%s Buffer Heap = {\n%*sBit-Set Array = [",
        indent,
        "",
        name,
        indent + PRINT_REPORT_INDENT,
        "");
    for (uint32_t i = 0; i < bufferHeap->m_fullnessBitsetCount; ++i) {
        StringBufferAppend(stringBuffer, "%p", (void*)bufferHeap->m_fullnessBitset[i]);
        if (i + 1 < bufferHeap->m_fullnessBitsetCount) {
            StringBufferAppend(stringBuffer, ", ");
        } else {
            StringBufferAppend(stringBuffer, "]");
        }
    }

    StringBufferAppend(stringBuffer, "\n%*sFullness Chunk Lists =\n", indent + PRINT_REPORT_INDENT, "");
    for (uint32_t i = 0; i < bufferHeap->m_maxBuffersInChunk; ++i) {
        if (bufferHeap->m_fullnessDirectory[i] != NULL) {
            char tmpName[20];
            erc = snprintf_s(tmpName, sizeof(tmpName), sizeof(tmpName) - 1, "Fullness[%u]", i);
            securec_check_ss(erc, "\0", "\0");
            MemBufferChunkListToString(
                indent + (2 * PRINT_REPORT_INDENT), tmpName, bufferHeap->m_fullnessDirectory[i], stringBuffer);
            StringBufferAppend(stringBuffer, "\n");
        }
    }

    MemBufferChunkListToString(indent + PRINT_REPORT_INDENT, "Full", bufferHeap->m_fullChunkList, stringBuffer, 1);
    StringBufferAppend(stringBuffer, "\n%*s}", indent, "");
}

static inline int GetNonEmptyFullnessIndex(MemBufferHeap* bufferHeap)
{
    int nonEmptyIndex = -1;
    for (uint32_t i = 0; i < bufferHeap->m_fullnessBitsetCount; ++i) {
        if (bufferHeap->m_fullnessBitset[i] != 0) {
            nonEmptyIndex = (int)i;
            break;
        }
    }
    return nonEmptyIndex;
}

static inline int GetNonEmptyEmptiestIndex(MemBufferHeap* bufferHeap)
{
    int nonEmptyIndex = -1;
    for (uint32_t i = 0; i < bufferHeap->m_fullnessBitsetCount; ++i) {
        if (bufferHeap->m_fullnessBitset[i] != 0) {
            nonEmptyIndex = (int)i;
            // continue, find the highest number, i.e. the emptiest non-empty chunk
        }
    }
    return nonEmptyIndex;
}

static inline int EnsureChunkExists(MemBufferHeap* bufferHeap)
{
    int nonEmptyIndex = GetNonEmptyFullnessIndex(bufferHeap);
    if (nonEmptyIndex == -1) {
        nonEmptyIndex = AllocAndPushChunk(bufferHeap);
    }
    return nonEmptyIndex;
}

static int AllocAndPushChunk(MemBufferHeap* bufferHeap)
{
    // allocate one chunk and update bit set (very costly unless preallocated in background)
    int nonEmptyIndex = -1;
    MemBufferChunkHeader* chunkHeader = NULL;
    if (bufferHeap->m_allocType == MEM_ALLOC_GLOBAL) {
        chunkHeader = (MemBufferChunkHeader*)MemRawChunkStoreAllocGlobal(bufferHeap->m_node);
    } else {
        chunkHeader = (MemBufferChunkHeader*)MemRawChunkStoreAllocLocal(bufferHeap->m_node);
    }
    if (chunkHeader != NULL) {
        MemBufferChunkInit(chunkHeader, bufferHeap->m_allocType, bufferHeap->m_node, bufferHeap->m_bufferClass);
        ++bufferHeap->m_allocatedChunkCount;
        nonEmptyIndex = MemBufferHeapPushChunk(bufferHeap, chunkHeader, 0);
    }
    return nonEmptyIndex;
}

// pop chunk from head of its fullness list (specified by fullness index)
static inline MemBufferChunkHeader* MemBufferHeapPopChunk(
    MemBufferHeap* bufferHeap, int fullnessIndex, int fullnessOffset)
{
    // pop chunk from list
    uint32_t chunkListIndex = (static_cast<unsigned int>(fullnessIndex) << 6) + fullnessOffset;
    MemBufferChunkHeader* chunkHeader = MemChunkListPop(&bufferHeap->m_fullnessDirectory[chunkListIndex]);
    MOT_ASSERT(chunkHeader);

    // reset bit if list became empty
    if (bufferHeap->m_fullnessDirectory[chunkListIndex] == NULL) {  // turned into empty, so reset bit
        RESET_FULLNESS_BIT(bufferHeap, fullnessIndex, fullnessOffset);
    }

    return chunkHeader;
}

// push chunk on head of its fullness list
static inline int MemBufferHeapPushChunk(
    MemBufferHeap* bufferHeap, MemBufferChunkHeader* chunkHeader, int allowFreeChunk)
{
    int fullnessIndex = -1;
    if (chunkHeader->m_allocatedCount == chunkHeader->m_bufferCount) {
        MemChunkListPush(&bufferHeap->m_fullChunkList, chunkHeader);
    } else if (allowFreeChunk && (chunkHeader->m_allocatedCount == 0)) {
        MOT_LOG_DEBUG("Deallocating chunk %p", chunkHeader);
        if (bufferHeap->m_allocType == MEM_ALLOC_GLOBAL) {
            MemRawChunkStoreFreeGlobal(chunkHeader, chunkHeader->m_node);
        } else {
            MemRawChunkStoreFreeLocal(chunkHeader, chunkHeader->m_node);
        }
        --bufferHeap->m_allocatedChunkCount;
    } else {
        int chunkListIndex = CHUNK_FULLNESS_INDEX(chunkHeader);
        if (bufferHeap->m_fullnessDirectory[chunkListIndex] == nullptr) {
            // turning to not-empty, so raise bit
            fullnessIndex = chunkListIndex / 64;
            int fullnessOffset = chunkListIndex % 64;
            RAISE_FULLNESS_BIT(bufferHeap, fullnessIndex, fullnessOffset);
        }
        MemChunkListPush(&bufferHeap->m_fullnessDirectory[chunkListIndex], chunkHeader);
    }
    return fullnessIndex;
}

// remove chunk from its fullness list (could be in middle of list)
static inline void MemBufferHeapUnlinkChunk(MemBufferHeap* bufferHeap, MemBufferChunkHeader* chunkHeader)
{
    if (chunkHeader->m_allocatedCount == chunkHeader->m_bufferCount) {
        MemChunkListUnlink(&bufferHeap->m_fullChunkList, chunkHeader);
    } else {
        uint32_t chunkListIndex = CHUNK_FULLNESS_INDEX(chunkHeader);
        MemChunkListUnlink(&bufferHeap->m_fullnessDirectory[chunkListIndex], chunkHeader);
        if (bufferHeap->m_fullnessDirectory[chunkListIndex] == NULL) {  // turned into empty, so reset bit
            int fullnessIndex = chunkListIndex / 64;
            int fullnessOffset = chunkListIndex % 64;
            RESET_FULLNESS_BIT(bufferHeap, fullnessIndex, fullnessOffset);
        }
    }
}

// push chunk on head of chunk list
static inline void MemChunkListPush(MemBufferChunkHeader** chunkList, MemBufferChunkHeader* chunkHeader)
{
    chunkHeader->m_nextChunk = *chunkList;
    if (*chunkList != NULL) {
        (*chunkList)->m_prevChunk = chunkHeader;
    }
    (*chunkList) = chunkHeader;
}

// pop chunk for head of a chunk list
static inline MemBufferChunkHeader* MemChunkListPop(MemBufferChunkHeader** chunkList)
{
    MemBufferChunkHeader* chunkHeader = (*chunkList);
    MOT_ASSERT(chunkHeader);
    (*chunkList) = (*chunkList)->m_nextChunk;  // can nullify list
    if (*chunkList) {
        (*chunkList)->m_prevChunk = NULL;
    }
    return chunkHeader;
}

// unlink chunk from a chunk list (can be any position)
static inline void MemChunkListUnlink(MemBufferChunkHeader** chunkList, MemBufferChunkHeader* chunkHeader)
{
    // separate cases
    if (*chunkList == chunkHeader) {  // unlink head
        (void)MemChunkListPop(chunkList);
    } else {
        if (chunkHeader->m_prevChunk != nullptr) {
            chunkHeader->m_prevChunk->m_nextChunk = chunkHeader->m_nextChunk;
        }
        if (chunkHeader->m_nextChunk != nullptr) {
            chunkHeader->m_nextChunk->m_prevChunk = chunkHeader->m_prevChunk;
        }
    }
}

// free all chunks in list back to chunk pool
static inline void MemChunkListFree(MemBufferHeap* bufferHeap, MemBufferChunkHeader* chunkList)
{
    MemBufferChunkHeader* itr = chunkList;
    while (itr != nullptr) {
        MemBufferChunkHeader* next = itr->m_nextChunk;
        --bufferHeap->m_allocatedChunkCount;
        bufferHeap->m_allocatedBufferCount -= itr->m_allocatedCount;
        if (bufferHeap->m_allocType == MEM_ALLOC_GLOBAL) {
            MemRawChunkStoreFreeGlobal(itr, itr->m_node);
        } else {
            MemRawChunkStoreFreeLocal(itr, itr->m_node);
        }
        itr = next;
    }
}

static void AssertHeapValid(MemBufferHeap* bufferHeap)
{
    for (uint32_t i = 0; i < bufferHeap->m_fullnessBitsetCount; ++i) {
        MemBufferChunkHeader* itr = bufferHeap->m_fullnessDirectory[i];
        std::set<MemBufferChunkHeader*> list_set;
        while (itr != NULL) {
            MOT_ASSERT(list_set.insert(itr).second == true);
            MOT_ASSERT((itr->m_bufferCount - itr->m_allocatedCount - 1) == i);
            itr = itr->m_nextChunk;
        }
    }
    MemBufferChunkHeader* itr = bufferHeap->m_fullChunkList;
    std::set<MemBufferChunkHeader*> list_set;
    while (itr != NULL) {
        MOT_ASSERT(list_set.insert(itr).second == true);
        MOT_ASSERT(itr->m_bufferCount == itr->m_allocatedCount);
        itr = itr->m_nextChunk;
    }
}

static void AssertChunkRemoved(MemBufferHeap* bufferHeap, MemBufferChunkHeader* chunkHeader)
{
    for (uint32_t i = 0; i < bufferHeap->m_fullnessBitsetCount; ++i) {
        MemBufferChunkHeader* itr = bufferHeap->m_fullnessDirectory[i];
        while (itr != NULL) {
            MOT_ASSERT(itr != chunkHeader);
            itr = itr->m_nextChunk;
        }
    }
    MemBufferChunkHeader* itr = bufferHeap->m_fullChunkList;
    while (itr != NULL) {
        MOT_ASSERT(itr != chunkHeader);
        itr = itr->m_nextChunk;
    }
}
}  // namespace MOT

extern "C" void MemBufferHeapDump(void* arg)
{
    MOT::MemBufferHeap* bufferHeap = (MOT::MemBufferHeap*)arg;

    MOT::StringBufferApply([bufferHeap](MOT::StringBuffer* stringBuffer) {
        MOT::MemBufferHeapToString(0, "Debug Dump", bufferHeap, stringBuffer);
        (void)fprintf(stderr, "%s", stringBuffer->m_buffer);
        (void)fflush(stderr);
    });
}
