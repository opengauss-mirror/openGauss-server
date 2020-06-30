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
 * redo_log_buffer_pool.cpp
 *    Manages a simple thread-safe pool of RedoLogBuffer objects with memory buffers managed externally.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/src/system/transaction_logger/asynchronous_redo_log/redo_log_buffer_pool.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "redo_log_buffer_pool.h"
#include "utilities.h"

namespace MOT {
DECLARE_LOGGER(RedoLogBufferPool, RedoLog);

RedoLogBufferPool::RedoLogBufferPool(
    uint32_t bufferSize /* = REDO_DEFAULT_BUFFER_SIZE */, uint32_t growSize /* = REDO_DEFAULT_GROW_SIZE */)
    : m_bufferSize(bufferSize), m_growSize(growSize), m_freeList(nullptr), m_objectPool(nullptr)
{}

RedoLogBufferPool::~RedoLogBufferPool()
{
    ClearFreeList();
    ObjAllocInterface::FreeObjPool(&m_objectPool);
}

bool RedoLogBufferPool::Init()
{
    // we create a global object pool even though pool access is guarded by lock,
    // because we do not want to rely on session-local memory for two main reasons:
    // 1. At this point of pool creation there might not be a session (but we can still create one if we want)
    // 2. The pool is accessed by many threads, and therefore whenever pool needs to be refilled, each allocation
    //    will be made by a different session (and this will lead to core dump during de-allocation, if
    //    de-allocation takes place at the wrong session).
    bool result = true;
    m_objectPool = ObjAllocInterface::GetObjPool(sizeof(RedoLogBuffer), false);
    if (m_objectPool == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Load Redo-Log Handler", "Failed to allocate object pool");
        result = false;
    }
    return result;
}

RedoLogBuffer* RedoLogBufferPool::Alloc()
{
    RedoLogBuffer* redoLogBuffer = nullptr;
    std::unique_lock<std::mutex> lock(m_lock);
    if (m_freeList == nullptr) {
        RefillFreeList();
    }

    redoLogBuffer = m_freeList;
    if (m_freeList != nullptr) {
        m_freeList = m_freeList->GetNext();
    }
    return redoLogBuffer;
}

void RedoLogBufferPool::Free(RedoLogBuffer* buffer)
{
    std::unique_lock<std::mutex> lock(m_lock);
    buffer->SetNext(m_freeList);
    m_freeList = buffer;
}

void RedoLogBufferPool::RefillFreeList()
{
    for (uint32_t i = 0; i < m_growSize; ++i) {
        uint8_t* buffer = new (std::nothrow) uint8_t[m_bufferSize];
        if (buffer == nullptr) {
            break;
        }
        RedoLogBuffer* redoLogBuffer = m_objectPool->Alloc<RedoLogBuffer>(buffer, m_bufferSize);
        if (redoLogBuffer != nullptr) {
            redoLogBuffer->SetNext(m_freeList);
            m_freeList = redoLogBuffer;
        } else {
            delete[] buffer;
            break;
        }
    }
}

void RedoLogBufferPool::ClearFreeList()
{
    while (m_freeList != nullptr) {
        // remove next redo-log buffer from the free list
        RedoLogBuffer* redoLogBuffer = m_freeList;
        m_freeList = m_freeList->GetNext();

        // get memory buffer and detach it (so it will not be deallocated in Buffer destructor)
        uint8_t* buffer = redoLogBuffer->Detach();

        // get rid of the object
        m_objectPool->Release(redoLogBuffer);

        // and get rid of the memory buffer
        delete[] buffer;
    }
}
}  // namespace MOT
