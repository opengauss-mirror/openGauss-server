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
 * mm_buffer_header.h
 *    Memory buffer header implementation. Holds all buffer management data.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/memory/mm_buffer_header.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef MM_BUFFER_HEADER_H
#define MM_BUFFER_HEADER_H

#include "mm_def.h"

namespace MOT {
/**
 * @struct MemBufferHeader
 * @brief Holds all buffer management data. The header resides in its owning chunk after the chunk
 * header. The buffer headers are separated from the buffers to avoid unnecessary page commits.
 */
struct PACKED MemBufferHeader {
    /** @var The ordinal index of the buffer in its owning chunk. */
    uint64_t m_index;

    /** @var The next item in a buffer list (can point to buffer in another page). */
    MemBufferHeader* m_next;

    /** @var The previous item in a buffer list (can point to buffer in another page). */
    MemBufferHeader* m_prev;

    /** @var Pointer to the actual buffer. */
    void* m_buffer;
};
}  // namespace MOT

#endif /* MM_BUFFER_HEADER_H */
