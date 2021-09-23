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
 * mm_chunk_type.h
 *    Constants denoting various chunk types.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/memory/mm_chunk_type.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef MM_CHUNK_TYPE_H
#define MM_CHUNK_TYPE_H

#include <stdint.h>

namespace MOT {

/**
 * @enum Constants denoting various chunk types.
 */
enum MemChunkType : uint32_t {
    /** @var Constant designating raw chunk type. */
    MEM_CHUNK_TYPE_RAW,

    /** @var Constant designating buffer chunk type. */
    MEM_CHUNK_TYPE_BUFFER,

    /** @var Constant designating small object chunk type. */
    MEM_CHUNK_TYPE_SMALL_OBJECT,

    /** @var Constant designating huge object chunk type. */
    MEM_CHUNK_TYPE_HUGE_OBJECT,

    /** @var Constant designating session chunk type. */
    MEM_CHUNK_TYPE_SESSION,

    /** @var Constant designating session large buffer pool chunk type. */
    MEM_CHUNK_TYPE_SESSION_LARGE_BUFFER_POOL,

    /** @var Constant designating maintenance chunk type. */
    MEM_CHUNK_TYPE_MAINTENANCE,

    /** @var Constant designating a dead chunk type (already de-allocated back to kernel). */
    MEM_CHUNK_TYPE_DEAD
};

/**
 * @brief COnverts chunk type constant to string form.
 * @param chunkType The chunk type.
 * @return The chunk type string.
 */
inline const char* MemChunkTypeToString(MemChunkType chunkType)
{
    switch (chunkType) {
        case MEM_CHUNK_TYPE_RAW:
            return "Raw Chunk";

        case MEM_CHUNK_TYPE_BUFFER:
            return "Buffer Chunk";

        case MEM_CHUNK_TYPE_SMALL_OBJECT:
            return "Small Object Chunk";

        case MEM_CHUNK_TYPE_HUGE_OBJECT:
            return "Huge Object Chunk";

        case MEM_CHUNK_TYPE_SESSION:
            return "Session Chunk";

        case MEM_CHUNK_TYPE_SESSION_LARGE_BUFFER_POOL:
            return "Session Large Buffer Pool Chunk";

        case MEM_CHUNK_TYPE_MAINTENANCE:
            return "Maintenance Chunk";

        case MEM_CHUNK_TYPE_DEAD:
            return "Dead Chunk";

        default:
            return "Unknown Chunk Type";
    }
}

}  // namespace MOT

#endif /* MM_CHUNK_TYPE_H */
