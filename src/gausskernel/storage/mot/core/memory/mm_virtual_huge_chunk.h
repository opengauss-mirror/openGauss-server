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
 * mm_virtual_huge_chunk.h
 *    A virtual huge chunk header.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/memory/mm_virtual_huge_chunk.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef MM_VIRTUAL_HUGE_CHUNK_H
#define MM_VIRTUAL_HUGE_CHUNK_H

#include "mm_chunk_type.h"
#include "mm_def.h"
#include <cstddef>
#include <cstdint>

namespace MOT {
struct PACKED MemVirtualHugeChunkHeader {
    /** @var Chunk type (must be first member to comply with raw chunk). */
    MemChunkType m_chunkType;  // offset [0-4]

    /** @var The node with which the chunk is associated (must be second member to comply with raw chunk). */
    int16_t m_node;  // offset [4-6]

    /** @var A flag specifying whether the chunk data is global or local (must be third member to comply with raw
     * chunk).
     */
    MemAllocType m_allocType;  // offset [6-8]

    /** The index of the header within its small maintenance chunk. */
    uint32_t m_index;  // offset [8-12]

    /** @var Align next member offset to 8 bytes. */
    uint8_t m_padding1[4];  // offset [12-16]

    /** @var The rounded size of the chunk. */
    uint64_t m_chunkSizeBytes;  // offset [16-24]

    /** @var The actual allocation size requested by the user. */
    uint64_t m_objectSizeBytes;  // offset [24-32]

    /** @var A pointer to the huge chunk. */
    void* m_chunk;  // offset [32-40]

    /** @var Pointer to next free header when this header is free or points to next chunk header in the
     * _large_chunk_list of the session allocator when it is allocated. */
    MemVirtualHugeChunkHeader* m_next;  // offset [40-48]

    /** @var Pointer to next chunk header in the _large_chunk_list of the session allocator when it is allocated. */
    MemVirtualHugeChunkHeader* m_prev;  // offset [48-56]

    /** @var Align struct size to cache line size. */
    uint8_t m_padding2[8];  // offset [56-64]
};

/**
 * @brief Initializes all maintenance data required for huge allocations.
 * @return Zero if successful otherwise an error code.
 */
extern int MemVirtualHugeChunkHeaderInit();

/**
 * @brief Deallocates all resources required for huge allocations.
 */
extern void MemVirtualHugeChunkHeaderDestroy();

/**
 * @brief Allocates a virtual huge chunk header
 * @param size The original allocation size.
 * @param alignedSize The aligned allocation size.
 * @param node The NUMA node.
 * @param allocType Specifies whether the chunk data is to be used for global (long-term) data
 * or local (short-term) data.
 * @return The allocated header.
 */
extern MemVirtualHugeChunkHeader* MemVirtualHugeChunkHeaderAlloc(
    uint64_t size, uint64_t alignedSize, int16_t node, MemAllocType allocType);

/**
 * @brief De-allocates a virtual huge chunk header
 * @param chunkHeader The chunk header.
 */
extern void MemVirtualHugeChunkHeaderFree(MemVirtualHugeChunkHeader* chunkHeader);
}  // namespace MOT

#endif /* MM_VIRTUAL_HUGE_CHUNK_H */
