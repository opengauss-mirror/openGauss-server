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
 * asynchronous_redo_log_handler.h
 *    Implements an asynchronous redo log.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/src/system/transaction_logger/
 *        asynchronous_redo_log/asynchronous_redo_log_handler.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef ASYNCHRONOUS_REDO_LOG_HANDLER_H
#define ASYNCHRONOUS_REDO_LOG_HANDLER_H

#include <queue>
#include "redo_log_handler.h"
#include "redo_log_buffer_pool.h"
#include "rw_lock.h"
#include "mot_configuration.h"

namespace MOT {
class TxnManager;

/**
 * @class AsyncRedoLogHandler
 * @brief implements an asynchronous redo log
 */
class AsyncRedoLogHandler : public RedoLogHandler {
public:
    AsyncRedoLogHandler();

    /** @brief Initializes the redo-log handler. */
    bool Init();

    /**
     * @brief creates a new Buffer object
     * @return a Buffer
     */
    RedoLogBuffer* CreateBuffer();

    /**
     * @brief destroys a Buffer object
     * @param buffer pointer to be destroyed and de-allocated
     */
    void DestroyBuffer(RedoLogBuffer* buffer);

    /**
     * @brief Inserts the data to the buffer.
     * @param buffer The buffer to write to log.
     * @return The next buffer to write to, or null in case of failure.
     */
    RedoLogBuffer* WriteToLog(RedoLogBuffer* buffer);

    void Flush();

    /**
     * @brief switches the buffers and flushes the log
     */
    void Write();

    AsyncRedoLogHandler(const AsyncRedoLogHandler& orig) = delete;
    AsyncRedoLogHandler& operator=(const AsyncRedoLogHandler& orig) = delete;
    ~AsyncRedoLogHandler();

private:
    static constexpr unsigned int WRITE_LOG_WAIT_INTERVAL = 1000;  // micro second

    /**
     * @brief free all the RedoLogBuffers in the array and return them to the pool
     */
    void FreeBuffers(RedoLogBufferArray& bufferArray);
    bool TrySwitchBuffers(int index);
    void WriteSingleBuffer();
    void WriteAllBuffers();

    RedoLogBufferPool m_bufferPool;
    // array of RedoLogBufferArray for switching in cyclic manner.
    RedoLogBufferArray m_redoLogBufferArrayArray[MOTConfiguration::MAX_ASYNC_REDO_LOG_BUFFER_ARRAY_COUNT];
    uint32_t m_redoLogBufferArrayCount;
    volatile int m_activeBuffer;
    bool m_initialized;
    RwLock m_switchLock;
    pthread_mutex_t m_writeLock;
    std::queue<uint32_t> m_writeQueue;
};
}  // namespace MOT

#endif /* ASYNCHRONOUS_REDO_LOG_HANDLER_H */