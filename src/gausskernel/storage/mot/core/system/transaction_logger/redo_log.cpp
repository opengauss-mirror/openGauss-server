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
 * redo_log.cpp
 *    Provides a redo logger interface.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/transaction_logger/redo_log.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "mot_engine.h"
#include "redo_log_handler.h"
#include "redo_log_writer.h"
#include "redo_log.h"

namespace MOT {
DECLARE_LOGGER(RedoLog, RedoLog);

RedoLog::RedoLog(TxnManager* txn) : BaseTxnLogger(), m_redoLogHandler(nullptr), m_flushed(false), m_forceWrite(false)
{
    SetTxn(txn);
}

RedoLog::~RedoLog()
{
    if (m_redoBuffer != nullptr) {
        m_redoLogHandler->DestroyBuffer(m_redoBuffer);
        m_redoBuffer = nullptr;
    }

    m_redoLogHandler = nullptr;
}

bool RedoLog::Init()
{
    if (m_configuration.m_enableRedoLog) {
        m_redoLogHandler = MOTEngine::GetInstance()->GetRedoLogHandler();
        if (m_redoLogHandler != nullptr) {
            m_redoBuffer = m_redoLogHandler->CreateBuffer();
        }
        if (m_redoBuffer == nullptr) {
            return false;
        }
        ResetBuffer();
    }
    return true;
}

void RedoLog::Reset()
{
    ResetBuffer();
    m_flushed = false;
}

RC RedoLog::Commit()
{
    RC status = RC_OK;
    // write only on primary therefore we have the isRecovering condition
    if (m_configuration.m_enableRedoLog && !MOTEngine::GetInstance()->IsRecovering()) {
        // write commit op to transaction wal buffer
        status = SerializeTransaction();
        if (status == RC_OK && (m_flushed || !m_redoBuffer->Empty() || m_forceWrite)) {
            RedoLogWriter::AppendCommit(*m_redoBuffer, m_txn);
            WriteToLog();
        }
    }
    return status;
}

void RedoLog::Rollback()
{
    // if no partial done, no need to write abort transaction to the log
    if (m_configuration.m_enableRedoLog && (m_flushed || m_forceWrite)) {
        if (!m_redoBuffer->Empty()) {
            m_redoBuffer->Reset();  // no need to write transaction ops only abort
        }
        RedoLogWriter::AppendRollback(*m_redoBuffer, m_txn);
        WriteToLog();
    }
}

RC RedoLog::Prepare()
{
    RC status = RC_OK;
    // write only on primary therefore we have the isRecovering condition
    if (m_configuration.m_enableRedoLog && !MOTEngine::GetInstance()->IsRecovering()) {
        status = SerializeTransaction();
        if (status == RC_OK && (m_flushed || !m_redoBuffer->Empty())) {
            // write commit op to transaction wal buffer
            RedoLogWriter::AppendPrepare(*m_redoBuffer, m_txn);
            WriteToLog();
        }
    }
    return status;
}

void RedoLog::RollbackPrepared()
{
    if (m_configuration.m_enableRedoLog && m_flushed) {
        RedoLogWriter::AppendRollbackPrepared(*m_redoBuffer, m_txn);
        WriteToLog();
    }
}

void RedoLog::CommitPrepared()
{
    if (m_configuration.m_enableRedoLog && (m_flushed || !m_redoBuffer->Empty()) &&
        !MOTEngine::GetInstance()->IsRecovering()) {
        // write commit op to transaction wal buffer
        RedoLogWriter::AppendCommitPrepared(*m_redoBuffer, m_txn);
        WriteToLog();
    }
}

void RedoLog::WriteToLog()
{
    if (!m_redoBuffer->Empty() || m_forceWrite) {
        m_redoLogHandler->RdLock();
        m_redoBuffer = m_redoLogHandler->WriteToLog(m_redoBuffer);
        m_redoLogHandler->RdUnlock();
        ResetBuffer();
        m_flushed = true;
    }
}

void RedoLog::ResetBuffer()
{
    if (m_configuration.m_enableRedoLog && m_redoLogHandler) {
        if (m_redoBuffer == nullptr) {
            m_redoBuffer = m_redoLogHandler->CreateBuffer();
        }
        if (likely(m_redoBuffer != nullptr)) {
            m_redoBuffer->Reset();
        }
    }
}

}  // namespace MOT
