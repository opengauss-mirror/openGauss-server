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
 * synchronous_redo_log_handler.h
 *    Implements a synchronous redo log.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/transaction_logger/
 *        synchronous_redo_log/synchronous_redo_log_handler.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef SYNCHRONOUS_REDO_LOG_HANDLER_H
#define SYNCHRONOUS_REDO_LOG_HANDLER_H

#include "redo_log_handler.h"

namespace MOT {
class TxnManager;

/**
 * @class SynchronousRedoLogHandler
 * @brief implements a synchronous redo log
 */
class SynchronousRedoLogHandler : public RedoLogHandler {
public:
    SynchronousRedoLogHandler();

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
     * @brief synchronously flushes the transactions data to the log
     * @param buffer The buffer to write to the log.
     * @return The next buffer to write to, or null in case of failure.
     */
    RedoLogBuffer* WriteToLog(RedoLogBuffer* buffer);
    void Flush();
    SynchronousRedoLogHandler(const SynchronousRedoLogHandler& orig) = delete;
    SynchronousRedoLogHandler& operator=(const SynchronousRedoLogHandler& orig) = delete;
    ~SynchronousRedoLogHandler();
};
}  // namespace MOT

#endif /* SYNCHRONOUS_REDO_LOG_HANDLER_H */
