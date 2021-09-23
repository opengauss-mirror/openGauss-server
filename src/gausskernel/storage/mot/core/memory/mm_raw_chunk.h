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
 * mm_raw_chunk.h
 *    A chunk header for raw chunks.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/memory/mm_raw_chunk.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef MM_RAW_CHUNK_H
#define MM_RAW_CHUNK_H

#include "mm_def.h"
#include "mm_chunk_type.h"

namespace MOT {

/**
 * @typedef mm_raw_chunk_header_t A chunk header for raw chunks.
 */
struct PACKED MemRawChunkHeader {
    /** @var The chunk type. */
    MemChunkType m_chunkType;  // offset [0-4]

    /** @var The node with which the chunk is associated. */
    int16_t m_node;  // offset [4-6]

    /** @var A flag specifying whether the chunk data is global or local. */
    MemAllocType m_allocType;  // offset [6-8]

    /** @var The next chunk in a raw chunk list. */
    MemRawChunkHeader* m_nextChunk;  // offset [8-16]

    /** @var The previous chunk in a raw chunk list. */
    MemRawChunkHeader* m_prevChunk;  // offset [16-24]
};

}  // namespace MOT

#endif /* MM_RAW_CHUNK_H */
