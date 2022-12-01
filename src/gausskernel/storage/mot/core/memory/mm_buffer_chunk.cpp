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
 * mm_buffer_chunk.cpp
 *    Memory buffer chunk allocation interfaces.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/memory/mm_buffer_chunk.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "mm_buffer_chunk.h"
#include "utilities.h"
#include "mm_raw_chunk_dir.h"
#include "mot_error.h"
#include "mm_raw_chunk_store.h"
#include "mm_cfg.h"
#include "mm_numa.h"
#include "mm_def.h"
#include "mm_api.h"

#include <cstring>

namespace MOT {
DECLARE_LOGGER(BufferChunk, Memory)

static uint32_t CalcMaxBuffers(uint32_t bufferSizeKb, uint64_t* paddedSizeOut)
{
    // buffer count must accommodate for headers too, so the formula is as follows:
    // BufferCount = (ChunkSize - ChunkHeaderSize) / (BufferHeaderSize + BufferSize)
    uint32_t bufferCount =
        (MEM_CHUNK_SIZE_MB * MEGA_BYTE -
            sizeof(MemBufferChunkHeader)) /                    // all usable area in raw chunk after chunk header
        (sizeof(MemBufferHeader) + bufferSizeKb * KILO_BYTE);  // size required for each buffer and its header
    MOT_LOG_DEBUG("Initial buffer count for buffer size %u KB: %u", bufferSizeKb, bufferCount);

    // now we calculate the padded size required for all headers (rounded up to the next page boundary)
    uint32_t fullHeaderSize = sizeof(MemBufferChunkHeader) + bufferCount * sizeof(MemBufferHeader);
    uint64_t paddedSize = ((fullHeaderSize + PAGE_SIZE_BYTES - 1) / PAGE_SIZE_BYTES) * PAGE_SIZE_BYTES;
    if (paddedSizeOut != nullptr) {
        *paddedSizeOut = paddedSize;
    }
    MOT_LOG_DEBUG("Full header size: %u, padded size: %u", fullHeaderSize, paddedSize);

    // ATTENTION: due to padding we may not be able to fit all buffers in the chunk data area
    // so we must calculate buffer count again, this time in a different approach:
    // check how many raw buffers can fit in the raw chunk area (without reserved padded area for headers).
    // as a result of this required action, the padded area for all headers might contain more area
    // than actually needed (there is nothing we can do about it).
    uint32_t bufferCount2 =
        (MEM_CHUNK_SIZE_MB * MEGA_BYTE -
            paddedSize) /            // all usable area in raw chunk after padded area for chunk and buffer headers
        (bufferSizeKb * KILO_BYTE);  // size required for each buffer (without its header)
    MOT_LOG_DEBUG("Fixed buffer count: %u", bufferCount2);

    MOT_ASSERT(bufferCount2 <= bufferCount);
    return bufferCount2;
}

extern uint32_t MemBufferChunksMaxBuffers(uint32_t bufferSizeKb)
{
    return CalcMaxBuffers(bufferSizeKb, nullptr);
}

extern void MemBufferChunkReportError(int errorCode, const char* format, ...)
{
    va_list args;
    va_start(args, format);

    MOT_REPORT_ERROR_V(errorCode, "N/A", format, args);

    va_end(args);
}

extern void MemBufferChunkInit(
    MemBufferChunkHeader* chunkHeader, MemAllocType allocType, int16_t allocatorNode, MemBufferClass bufferClass)
{
    chunkHeader->m_chunkType = MEM_CHUNK_TYPE_BUFFER;
    chunkHeader->m_allocType = allocType;
    chunkHeader->m_allocatorNode = allocatorNode;
    chunkHeader->m_bufferClass = bufferClass;
    chunkHeader->m_bufferSizeKb = MemBufferClassToSizeKb(bufferClass);

    // buffer count must accommodate for headers too, so the formula is as follows:
    // BufferCount = (ChunkSize - ChunkHeaderSize) / (BufferHeaderSize + BufferSize)
    uint64_t paddedSize = 0;
    chunkHeader->m_bufferCount = CalcMaxBuffers(chunkHeader->m_bufferSizeKb, &paddedSize);
    chunkHeader->m_chunk = ((uint8_t*)chunkHeader) + paddedSize;

    chunkHeader->m_freeBitsetCount = (chunkHeader->m_bufferCount + 64 - 1) / 64;
    for (uint32_t i = 0; i < chunkHeader->m_freeBitsetCount; ++i) {
        chunkHeader->m_freeBitset[i] = 0xFFFFFFFFFFFFFFFF;
    }
    chunkHeader->m_allocatedCount = 0;
    chunkHeader->m_nextChunk = nullptr;
    chunkHeader->m_prevChunk = nullptr;

    // initialize buffer headers
    for (uint32_t bufferIndex = 0; bufferIndex < chunkHeader->m_bufferCount; ++bufferIndex) {
        MemBufferHeader* bufferHeader = MM_CHUNK_BUFFER_HEADER_AT(chunkHeader, bufferIndex);
        bufferHeader->m_index = bufferIndex;
        bufferHeader->m_buffer = MM_CHUNK_BUFFER_AT(chunkHeader, bufferIndex);
        bufferHeader->m_prev = nullptr;  // MM_CHUNK_BUFFER_HEADER_AT(chunkHeader, bufferIndex - 1);
        bufferHeader->m_prev = nullptr;  // MM_CHUNK_BUFFER_HEADER_AT(chunkHeader, bufferIndex + 1);
    }
}

extern void MemBufferChunkOnDoubleFree(MemBufferChunkHeader* chunkHeader, MemBufferHeader* bufferHeader)
{
    MOT_LOG_PANIC(
        "Double free of buffer header %p [@%u -->%p] in chunk %p (node: %d, buffer-size=%u KB, allocated=%u/%u)",
        bufferHeader,
        bufferHeader->m_index,
        bufferHeader->m_buffer,
        chunkHeader,
        chunkHeader->m_node,
        chunkHeader->m_bufferSizeKb,
        chunkHeader->m_allocatedCount,
        chunkHeader->m_bufferCount);

    // this is a very extreme case, we force abort in the hope of a better root cause analysis
    MOTAbort(bufferHeader);
}

extern void MemBufferChunkHeaderPrint(LogLevel logLevel, MemBufferChunkHeader* chunkHeader)
{
    if (MOT_CHECK_LOG_LEVEL(logLevel)) {
        StringBufferApply([logLevel, chunkHeader](StringBuffer* stringBuffer) {
            MemBufferChunkHeaderToString(chunkHeader, stringBuffer);
            MOT_LOG(logLevel, "%s", stringBuffer->m_buffer);
        });
    }
}

extern void MemBufferChunkHeaderToString(
    MemBufferChunkHeader* chunkHeader, StringBuffer* stringBuffer, MemReportMode reportMode /* = MEM_REPORT_SUMMARY */)
{
    StringBufferAppend(stringBuffer,
        "Buffer Chunk [%u/%u] @ %p --> %p = { bit-set: ",
        chunkHeader->m_allocatedCount,
        chunkHeader->m_bufferCount,
        (void*)chunkHeader,
        (void*)chunkHeader->m_chunk);
    if (reportMode == MEM_REPORT_DETAILED) {
        for (uint32_t i = 0; i < chunkHeader->m_bufferCount; ++i) {
            MemBufferHeader* bufferHeader = MM_CHUNK_BUFFER_HEADER_AT(chunkHeader, i);
            StringBufferAppend(stringBuffer,
                " Buffer Header %u @ %p --> %p",
                bufferHeader->m_index,
                bufferHeader,
                bufferHeader->m_buffer);
            if (i + 1 < chunkHeader->m_bufferCount) {
                StringBufferAppend(stringBuffer, ",");
            }
        }
    }
    StringBufferAppend(stringBuffer, " }");
}

extern void MemBufferChunkListToString(
    int indent, const char* name, MemBufferChunkHeader* chunkList, StringBuffer* stringBuffer, int raw /* = 0 */)
{
    StringBufferAppend(stringBuffer, "%*s%s Buffer Chunk List = { ", indent, "", name);
    MemBufferChunkHeader* itr = chunkList;
    uint32_t chunk_count = 0;
    while (itr != nullptr) {
        ++chunk_count;
        itr = itr->m_nextChunk;
    }
    StringBufferAppend(stringBuffer, "%u }", chunk_count);
}

}  // namespace MOT

extern "C" void MemBufferHeaderDump(void* arg)
{
    MOT::MemBufferHeader* bufferHeader = (MOT::MemBufferHeader*)arg;
    MOT::MemBufferChunkHeader* chunkHeader = (MOT::MemBufferChunkHeader*)MOT::MemRawChunkDirLookup(bufferHeader);
    (void)fprintf(stderr,
        "Buffer Header %u @ %p --> %p (source chunk: %p)\n",
        (unsigned)bufferHeader->m_index,
        bufferHeader,
        bufferHeader->m_buffer,
        chunkHeader);
}

extern "C" void MemBufferChunkHeaderDump(void* arg)
{
    MOT::MemBufferChunkHeader* chunkHeader = (MOT::MemBufferChunkHeader*)arg;
    MOT::StringBufferApply([chunkHeader](MOT::StringBuffer* stringBuffer) {
        MOT::MemBufferChunkHeaderToString(chunkHeader, stringBuffer);
        (void)fprintf(stderr, "%s", stringBuffer->m_buffer);
        (void)fflush(stderr);
    });
}

extern "C" void MemBufferChunkListDump(void* arg)
{
    MOT::MemBufferChunkHeader* chunkList = (MOT::MemBufferChunkHeader*)arg;

    MOT::StringBufferApply([chunkList](MOT::StringBuffer* stringBuffer) {
        MOT::MemBufferChunkListToString(0, "Debug Dump", chunkList, stringBuffer);
        (void)fprintf(stderr, "%s", stringBuffer->m_buffer);
        (void)fflush(stderr);
    });
}

extern "C" void MemBufferDump(void* arg)
{
    MOT::StringBufferApply([arg](MOT::StringBuffer* stringBuffer) {
        StringBufferAppend(stringBuffer, "Analysis report for memory buffer %p:\n", arg);
        MOT::MemBufferChunkHeader* chunkHeader = (MOT::MemBufferChunkHeader*)MOT::MemRawChunkDirLookup(arg);
        if (chunkHeader != nullptr) {
            StringBufferAppend(stringBuffer, "  Found buffer chunk header at %p\n    ", chunkHeader);
            MOT::MemBufferChunkHeaderToString(chunkHeader, stringBuffer);
            MOT::MemBufferHeader* bufferHeader = MOT::MemBufferChunkGetBufferHeader(chunkHeader, arg);
            if (bufferHeader != nullptr) {
                StringBufferAppend(stringBuffer,
                    "  Found buffer header at %p (pointing to %p)\n",
                    bufferHeader,
                    bufferHeader->m_buffer);
            }
        } else {
            StringBufferAppend(stringBuffer, " buffer not found");
        }
        (void)fprintf(stderr, "%s", stringBuffer->m_buffer);
        (void)fflush(stderr);
    });
}
