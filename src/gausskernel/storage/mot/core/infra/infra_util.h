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
 * infra_util.h
 *    Basic infrastructure utilities.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/infra_util.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef INFRA_UTIL_H
#define INFRA_UTIL_H

#include "mot_string.h"
#include "mot_list.h"
#include "mot_vector.h"
#include "mot_set.h"
#include "mot_map.h"
#include "mm_session_api.h"
#include "mm_global_api.h"

namespace MOT {
/**
 * @class mot_session_allocator
 * @brief An allocator that uses session-local memory, via @ref MOT::MemSessionAlloc(), @ref MOT::MemSessionFree() and
 * @ref MOT::MemSessionRealloc().
 * @note In order to use this allocator, all mot string and container usage must be within a session context, otherwise
 * all memory management operations may result in undefined behavior.
 */
class mot_session_allocator {
public:
    /**
     * @brief Allocates session-local memory.
     * @param sizeBytes The number of bytes to allocate.
     * @return The allocated memory, or null if failed.
     */
    static void* allocate(uint32_t sizeBytes)
    {
        return MemSessionAlloc(sizeBytes);
    }

    /**
     * @brief Deallocates session-local memory.
     * @param buf The memory to deallocate.
     */
    static void free(void* buf)
    {
        MemSessionFree(buf);
    }

    /**
     * @brief Reallocates existing memory.
     * @param buf The existing memory
     * @param currentSizeBytes The current buffer size.
     * @param newSizeBytes The new allocated size.
     * @return The reallocated memory, or NULL if failed (in which case the existing memory is unaffected).
     */
    static void* realloc(void* buf, uint32_t currentSizeBytes, uint32_t newSizeBytes)
    {
        // although session-memory reallocation allows using various flags, we preserve traditional realloc() semantics
        // and pass @ref MM_REALLOC_COPY, to enable copying old memory contents, in case a new memory buffer was
        // allocated.
        (void)currentSizeBytes;  // unused
        return MemSessionRealloc(buf, newSizeBytes, MEM_REALLOC_COPY);
    }
};

/**
 * @class mot_global_allocator
 * @brief An allocator that uses global memory, via @ref MOT::MemGlobalAlloc(), @ref MOT::MemGlobalFree() and
 * @ref MOT::MemGlobalRealloc().
 * @note In order to use this allocator, all mot string and container usage must be within a session context, otherwise
 * all memory management operations may result in undefined behavior.
 */
class mot_global_allocator {
public:
    /**
     * @brief Allocates session-local memory.
     * @param sizeBytes The number of bytes to allocate.
     * @return The allocated memory, or null if failed.
     */
    static void* allocate(uint32_t sizeBytes)
    {
        return MemGlobalAlloc(sizeBytes);
    }

    /**
     * @brief Deallocates session-local memory.
     * @param buf The memory to deallocate.
     */
    static void free(void* buf)
    {
        MemGlobalFree(buf);
    }

    /**
     * @brief Reallocates existing memory.
     * @param buf The existing memory
     * @param currentSizeBytes The current buffer size.
     * @param newSizeBytes The new allocated size.
     * @return The reallocated memory, or NULL if failed (in which case the existing memory is unaffected).
     */
    static void* realloc(void* buf, uint32_t currentSizeBytes, uint32_t newSizeBytes)
    {
        // although session-memory reallocation allows using various flags, we preserve traditional realloc() semantics
        // and pass @ref MM_REALLOC_COPY, to enable copying old memory contents, in case a new memory buffer was
        // allocated.
        (void)currentSizeBytes;  // unused
        return MemGlobalRealloc(buf, newSizeBytes, MEM_REALLOC_COPY);
    }
};

// type define commonly used containers
/** @typedef mot_string_list A list of string objects. */
typedef mot_list<mot_string> mot_string_list;

/** @typedef mot_string_map A map of string to stirng objects. */
typedef mot_map<mot_string, mot_string> mot_string_map;
}  // namespace MOT

#endif /* INFRA_UTIL_H */
