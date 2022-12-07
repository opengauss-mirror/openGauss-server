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
 * pending_txn_logger.cpp
 *    Provides a redo logger interface for pending recovery transaction.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/transaction_logger/pending_txn_logger.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "mot_engine.h"
#include "pending_txn_logger.h"
#include "checkpoint_utils.h"

namespace MOT {
DECLARE_LOGGER(RedoLog, PendingTxnLogger);

PendingTxnLogger::PendingTxnLogger() : BaseTxnLogger(), m_fd(0)
{}

PendingTxnLogger::~PendingTxnLogger()
{
    if (m_redoBuffer != nullptr) {
        delete m_redoBuffer;
        m_redoBuffer = nullptr;
    }
}

bool PendingTxnLogger::Init()
{
    RedoLogBuffer* buffer = new (std::nothrow) RedoLogBuffer();
    if (buffer == nullptr) {
        return false;
    }

    if (!buffer->Initialize()) {
        delete buffer;
        return false;
    }

    m_redoBuffer = buffer;
    return true;
}

RC PendingTxnLogger::SerializePendingTransaction(TxnManager* txn, int fd)
{
    m_fd = fd;
    RC status = RC_OK;
    SetTxn(txn);
    status = SerializeTransaction();
    if (status == RC_OK && !m_redoBuffer->Empty()) {
        WritePartial();
    }
    return status;
}

void PendingTxnLogger::WriteToLog()
{
    if (!m_redoBuffer->Empty()) {
        PendingTxnLogger::Header header;
        uint32_t size = 0;
        char* data = (char*)m_redoBuffer->Serialize(&size);
        header.m_len = size;
        header.m_replayLsn = m_txn->GetReplayLsn();
        (void)CheckpointUtils::WriteFile(m_fd, (const char*)&header, sizeof(PendingTxnLogger::Header));
        (void)CheckpointUtils::WriteFile(m_fd, (const char*)data, size);
        ResetBuffer();
    }
}

void PendingTxnLogger::ResetBuffer()
{
    if (likely(m_redoBuffer != nullptr)) {
        m_redoBuffer->Reset();
    }
}

}  // namespace MOT