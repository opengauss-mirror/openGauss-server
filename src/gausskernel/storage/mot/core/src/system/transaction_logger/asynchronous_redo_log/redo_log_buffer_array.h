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
 * redo_log_buffer_array.h
 *    Implements an array of redo log buffers.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/src/system/transaction_logger/asynchronous_redo_log/redo_log_buffer_array.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef REDO_LOG_BUFFER_ARRAY_H
#define REDO_LOG_BUFFER_ARRAY_H

#include <atomic>
#include "utilities.h"
#include "global.h"
#include "redo_log_buffer.h"

#define MAX_BUFFERS 1000

namespace MOT {
class RedoLogBufferArray {
public:
    RedoLogBufferArray() : m_nextFree(0)
    {
        for (uint32_t i = 0; i < MAX_BUFFERS; i++) {
            m_array[i] = nullptr;
        }
    }
    RedoLogBufferArray(const RedoLogBufferArray& orig) = delete;
    virtual ~RedoLogBufferArray(){};

    uint32_t Size() const
    {
        return m_nextFree;
    }

    bool Empty() const
    {
        return (m_nextFree == 0);
    }

    void Reset()
    {
        m_nextFree = 0;
    }

    RedoLogBuffer* Front()
    {
        return m_array[0];
    }

    RedoLogBuffer* Back()
    {
        return m_array[m_nextFree - 1];
    }

    RedoLogBuffer* operator[](size_t idx)
    {
        return m_array[idx];
    }

    int PushBack(RedoLogBuffer* val)
    {
        uint32_t index = m_nextFree.fetch_add(1);
        if (index >= MAX_BUFFERS) {
            m_nextFree.fetch_sub(1);
            return -1;
        } else {
            m_array[index] = std::move(val);
            return (int)index;
        }
    }

    RedoLogBuffer** GetEntries()
    {
        return m_array;
    }

private:
    std::atomic<uint32_t> m_nextFree;
    RedoLogBuffer* m_array[MAX_BUFFERS];
};
}  // namespace MOT

#endif /* REDO_LOG_BUFFER_ARRAY_H */
