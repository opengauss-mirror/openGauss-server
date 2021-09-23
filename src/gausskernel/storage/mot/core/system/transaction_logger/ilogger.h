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
 * ilogger.h
 *    The base interface for redo log implementations.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/transaction_logger/ilogger.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef ILOGGER_H
#define ILOGGER_H

#include "redo_log_writer.h"

namespace MOT {
/**
 * @class ILogger
 * @brief The base interface for redo log implementations.
 */
class ILogger {
protected:
    /** Constructor. */
    ILogger()
    {}

public:
    /** Destructor. */
    virtual ~ILogger()
    {}

    /**
     * Serializes a RedoLogBuffer into the logger.
     * @param redoLogBuffer The RedoLogBuffer to write to the logger.
     * @return The amount of bytes written.
     */
    virtual uint64_t AddToLog(RedoLogBuffer* buffer)
    {
        uint32_t size;
        uint8_t* data = buffer->Serialize(&size);
        return AddToLog(data, size);
    }

    /**
     * Serializes a RedoLogBuffer array into the logger.
     * @param redoLogBufferArray The RedoLogBuffers to write to the logger.
     * @param size The number of entries in the redoLogBufferArray.
     * @return The amount of bytes written.
     */
    virtual uint64_t AddToLog(RedoLogBuffer** redoLogBufferArray, uint32_t size)
    {
        uint64_t written = 0;
        for (uint32_t i = 0; i < size; i++) {
            uint32_t length;
            uint8_t* data = redoLogBufferArray[i]->Serialize(&length);
            written += AddToLog(data, length);
        }
        return written;
    }

    /**
     * Serializes a log into the logger.
     * @param data The redo data to write to the logger.
     * @param size The redo data size to be written to the logger.
     * @return The amount of bytes written.
     */
    virtual uint64_t AddToLog(uint8_t* data, uint32_t size) = 0;

    /**
     * Flushes the log. This involves physical I/O operation (as opposed to
     * AddToLog(), which most of the time does not involve physical I/O operation).
     */
    virtual void FlushLog() = 0;

    /** Close the logger and the underlying I/O object. */
    virtual void CloseLog() = 0;

    /** For testing purposes. */
    virtual void ClearLog() = 0;
};
}  // namespace MOT

#endif /* ILOGGER_H */
