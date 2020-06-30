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
 * redo_log_buffer_pool.h
 *    Manages a simple thread-safe pool of RedoLogBuffer objects with memory buffers managed externally.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/src/system/transaction_logger/asynchronous_redo_log/redo_log_buffer_pool.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef REDO_LOG_BUFFER_POOL_H
#define REDO_LOG_BUFFER_POOL_H

#include <atomic>
#include <cstdint>
#include <mutex>

#include "object_pool.h"
#include "redo_log_buffer.h"

namespace MOT {
/** @define By default add 8 buffers each time pool is depleted. */
#define REDO_DEFAULT_GROW_SIZE 8

/**
 * @brief Manages a simple thread-safe pool of RedoLogBuffer objects with memory buffers managed externally (i.e. not
 * allocated and destroyed by parent Buffer class).
 */
class RedoLogBufferPool {
public:
    /**
     * @brief Constructor.
     * @param[opt] bufferSize The size of each memory buffer within each RedoLogBuffer object.
     * @param[opt] growSize The amount of RedoLogBuffer objects to add to the pool each time the pool is depleted.
     */
    explicit RedoLogBufferPool(
        uint32_t bufferSize = REDO_DEFAULT_BUFFER_SIZE, uint32_t growSize = REDO_DEFAULT_GROW_SIZE);

    /** @brief Destructor. */
    virtual ~RedoLogBufferPool();

    /** @brief Initializes the buffer pool. */
    bool Init();

    /** @brief Allocates a RedoLogBuffer object from the pool. */
    RedoLogBuffer* Alloc();

    /** @brief Returns a RedoLogBuffer object into the pool. */
    void Free(RedoLogBuffer* buffer);

private:
    /** @var The size of each memory buffer within each RedoLogBuffer object. */
    uint32_t m_bufferSize;

    /** @var The amount of RedoLogBuffer objects to add to the pool each time the pool is depleted. */
    uint32_t m_growSize;

    /** @var Synchronizes access to the free list. */
    std::mutex m_lock;

    /** @var The list of ready to use RedoLogBuffer objects. */
    RedoLogBuffer* m_freeList;

    /** @var Global pool for RedoLogBuffer objects (inside memory buffers are allocated externally). */
    ObjAllocInterface* m_objectPool;

    /** @brief Refills the free list. */
    void RefillFreeList();

    /** @brief Clears the free list and deallocates all associated objects and buffers. */
    void ClearFreeList();
};
}  // namespace MOT

#endif /* REDO_LOG_BUFFER_POOL_H */
