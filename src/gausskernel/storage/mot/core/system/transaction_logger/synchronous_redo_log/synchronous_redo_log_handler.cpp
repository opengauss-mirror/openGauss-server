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
 * synchronous_redo_log_handler.cpp
 *    Implements a synchronous redo log.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/transaction_logger/
 *        synchronous_redo_log/synchronous_redo_log_handler.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "synchronous_redo_log_handler.h"
#include "txn.h"
#include "global.h"
#include "utilities.h"

namespace MOT {
SynchronousRedoLogHandler::SynchronousRedoLogHandler()
{}

SynchronousRedoLogHandler::~SynchronousRedoLogHandler()
{}

RedoLogBuffer* SynchronousRedoLogHandler::CreateBuffer()
{
    RedoLogBuffer* buffer = new (std::nothrow) RedoLogBuffer();
    if (buffer != nullptr) {
        if (!buffer->Initialize()) {
            delete buffer;
            buffer = nullptr;
        }
    }
    return buffer;
}

void SynchronousRedoLogHandler::DestroyBuffer(RedoLogBuffer* buffer)
{
    if (buffer != nullptr) {
        delete buffer;
    }
}

RedoLogBuffer* SynchronousRedoLogHandler::WriteToLog(RedoLogBuffer* buffer)
{
    m_logger->AddToLog(buffer);
    m_logger->FlushLog();
    return buffer;
}

void SynchronousRedoLogHandler::Flush()
{}
}  // namespace MOT
