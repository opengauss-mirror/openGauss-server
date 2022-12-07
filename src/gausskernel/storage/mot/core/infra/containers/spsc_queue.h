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
 * spsc_queue.h
 *      A single producer/single consumer queue
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/mot/core/infra/containers/spsc_queue.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef SPSCQUEUE_H
#define SPSCQUEUE_H

#include <atomic>
#include <assert.h>
#include <stdlib.h>
#include "mot_atomic_ops.h"
#include "utilities.h"

namespace MOT {
#define COUNT(head, tail, mask) ((uint32_t)(((head) - (tail)) & (mask)))
#define SPACE(head, tail, mask) ((uint32_t)(((tail) - ((head) + 1)) & (mask)))

const uint32_t MAX_REDO_QUE_TAKE_DELAY = 200; /* 100 us */
const uint32_t MAX_REDO_QUE_IDEL_TAKE_DELAY = 1000;

const uint32_t QUEUE_CAPACITY_MIN_LIMIT = 2;
const uint32_t QUEUE_PUT_WAIT_LIMIT = 3;
const uint32_t QUEUE_TAKE_WAIT_LIMIT = 3;

template <typename T>
class SPSCQueue {
public:
    SPSCQueue(uint32_t capacity)
        : m_initialized(false),
          m_writeHead(0),
          m_readTail(0),
          m_capacity(capacity),
          m_mask(capacity - 1),
          m_maxUsage(0),
          m_totalCnt(0),
          m_buffer(nullptr)
    {
        /*
         * We require the capacity to be a power of 2, so index wrap can be
         * handled by a bit-wise and.  The actual capacity is one less than
         * the specified, so the minimum capacity is 2.
         */
        assert(capacity >= QUEUE_CAPACITY_MIN_LIMIT && POWER_OF_TWO(capacity));
    }

    SPSCQueue(const SPSCQueue& orig) = delete;

    virtual ~SPSCQueue()
    {
        if (m_buffer != nullptr) {
            free(m_buffer);
            m_buffer = nullptr;
        }
        m_initialized = false;
    }

    bool Init()
    {
        size_t allocSize = sizeof(void*) * m_capacity;
        m_buffer = (void**)malloc(allocSize);
        if (m_buffer != nullptr) {
            m_initialized = true;
        }
        return m_initialized;
    }

    bool Put(T* data)
    {
        uint32_t head;
        uint32_t tail;
        uint64_t cnt = 0;
        uint32_t tmpCnt = 0;
        head = m_writeHead.load();
        do {
            if (cnt > QUEUE_PUT_WAIT_LIMIT) {
                return false;
            }
            tail = m_readTail.load();
            cnt++;
        } while (SPACE(head, tail, m_mask) == 0);

        /*
         * Make sure the following write to the buffer happens after the read
         * of the tail.  Combining this with the corresponding barrier in Take()
         * which guarantees that the tail is updated after reading the buffer,
         * we can be sure that we cannot update a slot's value before it has
         * been read.
         */
        MEMORY_BARRIER();

        tmpCnt = COUNT(head, tail, m_mask);
        if (tmpCnt > m_maxUsage) {
            m_maxUsage.store(tmpCnt);
        }

        m_buffer[head] = static_cast<void*>(data);

        /* Make sure the index is updated after the buffer has been written. */
        WRITE_BARRIER();

        m_writeHead.store((head + 1) & m_mask);
        return true;
    }

    T* Take()
    {
        uint32_t head = m_writeHead.load();
        uint32_t tail = m_readTail.load();
        if (COUNT(head, tail, m_mask) == 0) {
            return nullptr;
        }

        /* Make sure the buffer is read after the index. */
        READ_BARRIER();

        T* elem = static_cast<T*>(m_buffer[tail]);

        /* Make sure the read of the buffer finishes before updating the tail. */
        MEMORY_BARRIER();

        m_readTail.store((tail + 1) & m_mask);
        return elem;
    }

    bool IsEmpty()
    {
        return Count() == 0;
    }

    T* Top()
    {
        uint32_t head = m_writeHead.load();
        uint32_t tail = m_readTail.load();
        if (COUNT(head, tail, m_mask) == 0) {
            return nullptr;
        }

        READ_BARRIER();
        T* elem = static_cast<T*>(m_buffer[tail]);
        return elem;
    }

    void Pop()
    {
        uint32_t head = m_writeHead.load();
        uint32_t tail = m_readTail.load();
        uint64_t totalCnt = m_totalCnt.load();
        if (COUNT(head, tail, m_mask) == 0) {
            return;
        }

        /* Make sure the read of the buffer finishes before updating the tail. */
        MEMORY_BARRIER();
        m_totalCnt.store(totalCnt + 1);
        m_readTail.store((tail + 1) & m_mask);
    }

    uint32_t Count()
    {
        uint32_t head = m_writeHead.load();
        uint32_t tail = m_readTail.load();
        return (COUNT(head, tail, m_mask));
    }

private:
    bool m_initialized;
    std::atomic<uint32_t> m_writeHead; /* Array index for the next write. */
    std::atomic<uint32_t> m_readTail;  /* Array index for the next read. */
    uint32_t m_capacity;               /* Queue capacity, must be power of 2. */
    uint32_t m_mask;                   /* Bit mask for computing index. */
    std::atomic<uint32_t> m_maxUsage;
    std::atomic<uint64_t> m_totalCnt;
    void** m_buffer; /* Queue buffer, the actual size is capacity. */
};
}  // namespace MOT

#endif /* SPSCQUEUE_H */
