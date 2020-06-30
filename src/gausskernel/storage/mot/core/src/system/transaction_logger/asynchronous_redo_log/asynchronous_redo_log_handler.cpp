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
 * asynchronous_redo_log_handler.cpp
 *    Implements an asynchronous redo log.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/src/system/transaction_logger/
 *        asynchronous_redo_log/asynchronous_redo_log_handler.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "asynchronous_redo_log_handler.h"

#include "txn.h"
#include "global.h"
#include "utilities.h"
#include "mot_atomic_ops.h"

namespace MOT {
DECLARE_LOGGER(AsyncRedoLogHandler, redolog)

AsyncRedoLogHandler::AsyncRedoLogHandler() : m_bufferPool(), m_activeBuffer(0)
{}

AsyncRedoLogHandler::~AsyncRedoLogHandler()
{}

bool AsyncRedoLogHandler::Init()
{
    return m_bufferPool.Init();
}

RedoLogBuffer* AsyncRedoLogHandler::CreateBuffer()
{
    return m_bufferPool.Alloc();
}

void AsyncRedoLogHandler::DestroyBuffer(RedoLogBuffer* buffer)
{
    m_bufferPool.Free(buffer);
}

RedoLogBuffer* AsyncRedoLogHandler::WriteToLog(RedoLogBuffer* buffer)
{
    int position = 0;
    do {
        uint64_t activeBuffer = MOT_ATOMIC_LOAD(m_activeBuffer) % WRITE_LOG_BUFFER_COUNT;
        position = m_tripleBuffer[activeBuffer].PushBack(buffer);
        if (position == MAX_BUFFERS / 2) {
            WakeupWalWriter();
        } else if (position == -1) {
            // async redo log ternary buffer is full, waiting for write thread
            // to flush the buffer
            usleep(WRITE_LOG_WAIT_INTERVAL);
        }
    } while (position == -1);
    return CreateBuffer();
}

void AsyncRedoLogHandler::Write()
{
    // buffer list switch logic:
    // while writers write to list 0, we increment index to 1 and then write list 2 to log
    // while writers write to list 1, we increment index to 2 and then write list 0 to log
    // while writers write to list 2, we increment index to 0 and then write list 1 to log
    uint64_t currentIndex = MOT_ATOMIC_LOAD(m_activeBuffer);
    uint64_t nextIndex = (currentIndex + 1) % WRITE_LOG_BUFFER_COUNT;
    uint64_t prevIndex = (currentIndex + 2) % WRITE_LOG_BUFFER_COUNT;

    // allow writers to start writing to next buffer list
    MOT_ATOMIC_STORE(m_activeBuffer, nextIndex);

    // in the meantime we write and flush previous list to log
    // writers might still be writing to current list, but we do not touch it in this cycle
    RedoLogBufferArray& prevBufferArray = m_tripleBuffer[prevIndex];

    // invoke logger only if something happened, otherwise we just juggle with empty buffer arrays
    if (!prevBufferArray.Empty()) {
        m_logger->AddToLog(prevBufferArray);
        m_logger->FlushLog();
        FreeBuffers(prevBufferArray);
        prevBufferArray.Reset();
    }
}

void AsyncRedoLogHandler::FreeBuffers(RedoLogBufferArray& bufferArray)
{
    for (uint32_t i = 0; i < bufferArray.Size(); i++) {
        m_bufferPool.Free(bufferArray[i]);
    }
}
}  // namespace MOT
