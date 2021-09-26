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
 * redo_log_handler.h
 *    Abstract interface of a redo logger.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/transaction_logger/redo_log_handler.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef REDO_LOG_HANDLER_H
#define REDO_LOG_HANDLER_H

#include "ilogger.h"
#include "txn.h"
#include "redo_log_buffer.h"
#include "rw_lock.h"

namespace MOT {
/** @typedef Callback for waking up WAL writer in the envelope. */
typedef void (*WalWakeupFunc)();

/**
 * @class RedoLogHandler
 * @brief abstract interface of a redo logger
 */
class RedoLogHandler {
public:
    RedoLogHandler();
    RedoLogHandler(const RedoLogHandler& orig) = delete;
    virtual ~RedoLogHandler();

    virtual bool Init()
    {
        return true;
    }

    /**
     * @brief creates a new Buffer object
     * @return a Buffer
     */
    virtual RedoLogBuffer* CreateBuffer() = 0;

    /**
     * @brief destroys a Buffer object
     * @param buffer pointer to be destroyed and de-allocated
     */
    virtual void DestroyBuffer(RedoLogBuffer* buffer) = 0;

    /**
     * @brief adds data to the log
     * @param buffer The buffer to write to the log.
     * @return The next buffer to write to, or null in case of failure.
     */
    virtual RedoLogBuffer* WriteToLog(RedoLogBuffer* buffer) = 0;

    /**
     * @brief flush all buffers (if exist) to log
     */
    virtual void Flush() = 0;

    /**
     * @brief flushes the the log
     */
    virtual void Write()
    {}

    inline ILogger* GetLogger()
    {
        return m_logger;
    }

    virtual void SetLogger(ILogger* logger)
    {
        this->m_logger = logger;
    }

    void SetWalWakeupFunc(WalWakeupFunc wakeupFunc)
    {
        m_wakeupFunc = wakeupFunc;
    }

    inline void RdLock()
    {
        m_redo_lock.RdLock();
    }

    inline void RdUnlock()
    {
        m_redo_lock.RdUnlock();
    }

    inline void WrLock()
    {
        m_redo_lock.WrLock();
    }

    inline void WrUnlock()
    {
        m_redo_lock.WrUnlock();
    }

protected:
    ILogger* m_logger;
    RwLock m_redo_lock;

    inline void WakeupWalWriter()
    {
        if (m_wakeupFunc != nullptr) {
            m_wakeupFunc();
        }
    }

private:
    WalWakeupFunc m_wakeupFunc;
};

/**
 * @class RedoLogHandlerFactory
 * @brief a factory class that is used to create the
 * configured redo logger
 */
class RedoLogHandlerFactory {
public:
    /**
     * @brief creates a redo log handler
     * @return a RedoLogHandler
     */
    static RedoLogHandler* CreateRedoLogHandler();

private:
    /** @brief Constructor */
    RedoLogHandlerFactory()
    {}

    /** @brief Destructor */
    ~RedoLogHandlerFactory()
    {}
};
}  // namespace MOT

#endif /* REDO_LOG_HANDLER_H */
