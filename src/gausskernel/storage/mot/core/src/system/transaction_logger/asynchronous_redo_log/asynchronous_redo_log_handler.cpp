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
#include "mot_configuration.h"

namespace MOT {
DECLARE_LOGGER(AsyncRedoLogHandler, RedoLog)

AsyncRedoLogHandler::AsyncRedoLogHandler()
    : m_bufferPool(),
      m_redoLogBufferArrayCount(GetGlobalConfiguration().m_asyncRedoLogBufferArrayCount),
      m_activeBuffer(0),
      m_initialized(false)
{}

AsyncRedoLogHandler::~AsyncRedoLogHandler()
{
    // wait for all redo log buffers to be written
    while (!m_writeQueue.empty()) {
        usleep(WRITE_LOG_WAIT_INTERVAL);
    }
    if (m_initialized) {
        pthread_mutex_destroy(&m_writeLock);
    }
    m_initialized = false;
}

bool AsyncRedoLogHandler::Init()
{
    bool result = false;
    int rc = pthread_mutex_init(&m_writeLock, nullptr);
    result = (rc == 0);
    if (result != true) {
        MOT_LOG_ERROR("Error initializing async redolog handler lock");
        return result;
    }
    result = m_bufferPool.Init();
    m_initialized = true;
    return result;
}

RedoLogBuffer* AsyncRedoLogHandler::CreateBuffer()
{
    return m_bufferPool.Alloc();
}

void AsyncRedoLogHandler::DestroyBuffer(RedoLogBuffer* buffer)
{
    buffer->Reset();
    m_bufferPool.Free(buffer);
}

RedoLogBuffer* AsyncRedoLogHandler::WriteToLog(RedoLogBuffer* buffer)
{
    int position = -1;
    do {
        m_switchLock.RdLock();
        int activeBuffer = m_activeBuffer;
        position = m_redoLogBufferArrayArray[m_activeBuffer].PushBack(buffer);
        m_switchLock.RdUnlock();
        if (position == -1) {
            usleep(1000);
        } else if (position == (int)(MAX_BUFFERS - 1)) {
            if (TrySwitchBuffers(activeBuffer)) {
                WriteSingleBuffer();
            }
        }
    } while (position == -1);
    return CreateBuffer();
}

void AsyncRedoLogHandler::Write()
{
    m_switchLock.RdLock();
    int activeBuffer = m_activeBuffer;
    m_switchLock.RdUnlock();
    if (TrySwitchBuffers(activeBuffer)) {
        WriteSingleBuffer();
    }
}

bool AsyncRedoLogHandler::TrySwitchBuffers(int index)
{
    bool result = false;
    int nextIndex = (index + 1) % m_redoLogBufferArrayCount;
    m_switchLock.WrLock();
    while (index == m_activeBuffer && !m_redoLogBufferArrayArray[index].Empty()) {
        if (!m_redoLogBufferArrayArray[nextIndex].Empty()) {
            // the next buffer was not yet written to the log
            // wait until write complete
            m_switchLock.WrUnlock();
            usleep(WRITE_LOG_WAIT_INTERVAL);
            m_switchLock.WrLock();
            continue;
        }
        m_writeQueue.push(m_activeBuffer);
        m_activeBuffer = nextIndex;
        result = true;
    }
    m_switchLock.WrUnlock();
    return result;
}

void AsyncRedoLogHandler::FreeBuffers(RedoLogBufferArray& bufferArray)
{
    for (uint32_t i = 0; i < bufferArray.Size(); i++) {
        bufferArray[i]->Reset();
        m_bufferPool.Free(bufferArray[i]);
    }
}

void AsyncRedoLogHandler::WriteSingleBuffer()
{
    uint32_t writeBufferIndex;
    pthread_mutex_lock(&m_writeLock);
    if (!m_writeQueue.empty()) {
        writeBufferIndex = m_writeQueue.front();
        m_writeQueue.pop();
        RedoLogBufferArray& bufferArray = m_redoLogBufferArrayArray[writeBufferIndex];

        // invoke logger only if something happened, otherwise we just juggle with empty buffer arrays
        if (!bufferArray.Empty()) {
            m_logger->AddToLog(bufferArray);
            m_logger->FlushLog();
            FreeBuffers(bufferArray);
            bufferArray.Reset();
        }
    }
    pthread_mutex_unlock(&m_writeLock);
}

void AsyncRedoLogHandler::WriteAllBuffers()
{
    uint32_t writeBufferIndex;
    pthread_mutex_lock(&m_writeLock);
    while (!m_writeQueue.empty()) {
        writeBufferIndex = m_writeQueue.front();
        m_writeQueue.pop();
        RedoLogBufferArray& bufferArray = m_redoLogBufferArrayArray[writeBufferIndex];

        // invoke logger only if something happened, otherwise we just juggle with empty buffer arrays
        if (!bufferArray.Empty()) {
            m_logger->AddToLog(bufferArray);
            m_logger->FlushLog();
            FreeBuffers(bufferArray);
            bufferArray.Reset();
        }
    }
    pthread_mutex_unlock(&m_writeLock);
}

void AsyncRedoLogHandler::Flush()
{
    m_switchLock.RdLock();
    int activeBuffer = m_activeBuffer;
    m_switchLock.RdUnlock();
    TrySwitchBuffers(activeBuffer);
    WriteAllBuffers();
}
}  // namespace MOT
