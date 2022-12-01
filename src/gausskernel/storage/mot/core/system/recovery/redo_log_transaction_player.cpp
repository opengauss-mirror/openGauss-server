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
 * redo_log_transaction_player.cpp
 *    Implements a transaction replay mechanism.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/recovery/redo_log_transaction_player.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "redo_log_transaction_player.h"
#include "mot_engine.h"

namespace MOT {
DECLARE_LOGGER(RedologRecoveryPlayer, Recovery);

RedoLogTransactionPlayer::RedoLogTransactionPlayer(IRecoveryManager* recoveryManager)
    : m_inPool(false),
      m_queueId(0),
      m_transactionId(INVALID_TRANSACTION_ID),
      m_externalId(INVALID_TRANSACTION_ID),
      m_csn(INVALID_CSN),
      m_replayLsn(0),
      m_prevId(INVALID_TRANSACTION_ID),
      m_numSegs(0),
      m_processed(false),
      m_firstSegment(false),
      m_initialized(false),
      m_retried(false),
      m_recoveryManager(recoveryManager),
      m_surrogateState(nullptr),
      m_txn(nullptr)
{}

RedoLogTransactionPlayer::~RedoLogTransactionPlayer()
{
    if (m_txn != nullptr) {
        m_txn->Rollback();
        m_txn->~TxnManager();
        free(m_txn);
        m_txn = nullptr;
    }
    m_recoveryManager = nullptr;
    m_surrogateState = nullptr;
    m_initialized = false;
}

void RedoLogTransactionPlayer::Init(TxnManager* txn)
{
    m_txn = txn;
    m_initialized = true;
}

void RedoLogTransactionPlayer::InitRedoTransactionData(
    uint64_t transactionId, uint64_t externalId, uint64_t csn, uint64_t replayLsn)
{
    m_transactionId = transactionId;
    m_externalId = externalId;
    m_csn = csn;
    m_replayLsn = replayLsn;
    m_retried = false;
    m_txn->SetTransactionId(externalId);
    m_txn->SetInternalTransactionId(transactionId);
    m_txn->SetReplayLsn(replayLsn);
    m_txn->SetCommitSequenceNumber(m_csn);
    m_txn->SetVisibleCSN(static_cast<uint64_t>(-1));
    m_firstSegment = false;
    m_numSegs = 0;
}

RC RedoLogTransactionPlayer::BeginTransaction()
{
    if (RecoveryOps::BeginTransaction(this, m_replayLsn) != RC_OK) {
        MOT_REPORT_ERROR(MOT_ERROR_RESOURCE_LIMIT, "Recover Redo Segment", "Cannot start a new transaction");
        return RC_ERROR;
    }
    return RC_OK;
}

RC RedoLogTransactionPlayer::RedoSegment(LogSegment* segment)
{
    RC status = RC_OK;
    uint8_t* endPosition = (uint8_t*)(segment->m_data + segment->m_len);
    uint8_t* operationData = (uint8_t*)(segment->m_data);
    while (operationData < endPosition) {
        // redo log recovery - single threaded
        if (IsRecoveryMemoryLimitReached(m_recoveryManager->GetNumRecoveryThreads())) {
            status = RC_ERROR;
            MOT_LOG_ERROR("Memory hard limit reached. Cannot recover datanode");
            break;
        }

        operationData += RecoveryOps::RecoverLogOperation(
            this, operationData, m_transactionId, MOTCurrThreadId, *m_surrogateState, status);
        // check operation result status
        if (status != RC_OK) {
            MOT_REPORT_ERROR(MOT_ERROR_RESOURCE_LIMIT, "Recover Redo Segment", "Failed to recover redo segment");
            break;
        }
    }
    IncNumSegs();
    return status;
}

bool RedoLogTransactionPlayer::IsRecoveryMemoryLimitReached(uint32_t numThreads) const
{
    uint64_t memoryRequiredBytes = numThreads * MEM_CHUNK_SIZE_MB * MEGA_BYTE;
    if (MOTEngine::GetInstance()->GetCurrentMemoryConsumptionBytes() + memoryRequiredBytes >=
        MOTEngine::GetInstance()->GetHardMemoryLimitBytes()) {
        MOT_LOG_WARN("IsRecoveryMemoryLimitReached: recovery memory limit reached "
                     "current memory: %lu, required memory: %lu, hard limit memory: %lu",
            MOTEngine::GetInstance()->GetCurrentMemoryConsumptionBytes(),
            memoryRequiredBytes,
            MOTEngine::GetInstance()->GetHardMemoryLimitBytes());
        return true;
    }

    return false;
}

RC RedoLogTransactionPlayer::CommitTransaction()
{
    MOT_ASSERT(m_transactionId == m_txn->GetInternalTransactionId());
    m_txn->SetCommitSequenceNumber(m_csn);
    m_txn->SetReplayLsn(m_replayLsn);

    RC status = m_txn->Commit();
    if (status != RC_OK) {
        MOT_LOG_ERROR("Failed to commit recovery transaction: %s (error code: %u)", RcToString(status), status);
        return status;
    } else {
        MOT_LOG_DEBUG("Committing transaction CSN = %lu", m_txn->GetCommitSequenceNumber());
    }

    MOTEngine::GetInstance()->GetCSNManager().SetCSN(m_csn);
    m_txn->EndTransaction();
    return RC_OK;
}

void RedoLogTransactionPlayer::CleanupTransaction()
{
    m_externalId = INVALID_TRANSACTION_ID;
    m_csn = INVALID_CSN;
    m_replayLsn = 0;
    m_processed.store(false);
    m_retried = false;
}
}  // namespace MOT
