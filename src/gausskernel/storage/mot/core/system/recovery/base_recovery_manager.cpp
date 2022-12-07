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
 * base_recovery_manager.cpp
 *    Implements the shared functionality for all recovery managers
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/recovery/base_recovery_manager.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "mot_engine.h"
#include "checkpoint_utils.h"
#include "checkpoint_manager.h"
#include "base_recovery_manager.h"

namespace MOT {
DECLARE_LOGGER(BaseRecoveryManager, Recovery);
IMPLEMENT_CLASS_LOGGER(RecoveryStats, Recovery);

bool BaseRecoveryManager::Initialize()
{
    if (CheckpointControlFile::GetCtrlFile() == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Recovery Manager Initialization", "Failed to allocate ctrlfile object");
        return false;
    }

    if (!m_surrogateState.Init()) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Recovery Manager Initialization", "Failed to allocate surrogate state object");
        return false;
    }

    if (m_enableLogStats) {
        m_recoveryTableStats = new (std::nothrow) RecoveryStats();
        if (m_recoveryTableStats == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Recovery Manager Initialization", "Failed to allocate statistics object");
            return false;
        }
    }

    m_initialized = true;
    return m_initialized;
}

bool BaseRecoveryManager::RecoverDbStart()
{
    if (m_recoverFromCkptDone) {
        /*
         * This is switchover case.
         * In case of CASCADE switchover (CASCADE node switchover with STANDBY node), we need to
         * set the correct m_lsn to make sure that we skip any redo which are already replayed.
         * After switchover, envelope will start sync from the previous ckpt position.
         * In MOT engine, we should skip any redo before m_lastReplayLsn.
         */
        if (m_lsn < m_lastReplayLsn.load()) {
            m_lsn = m_lastReplayLsn.load();
        }
        return true;
    }

    if (!m_checkpointRecovery.Recover()) {
        return false;
    }

    /*
     * Set both m_lsn and m_lastReplayLsn. m_lastReplayLsn is needed as it will also be stored in control file
     * during the checkpoint in standby node.
     */
    SetLsn(m_checkpointRecovery.GetLsn());
    SetLastReplayLsn(m_checkpointRecovery.GetLsn());
    SetMaxCsn(m_checkpointRecovery.GetMaxCsn());
    if (m_maxCsn) {
        GetCSNManager().SetCSN(m_maxCsn);
    }
    SetMaxTransactionId(m_checkpointRecovery.GetMaxTransactionId());
    m_recoverFromCkptDone = true;
    return true;
}

bool BaseRecoveryManager::RecoverDbEnd()
{
    ApplySurrogate();
    if (m_maxTransactionId) {
        GetTxnIdManager().SetId(m_maxTransactionId);
    }

    if (m_recoveryTableStats != nullptr) {
        m_recoveryTableStats->Print();
        m_recoveryTableStats->Clear();
    }

    if (IsErrorSet()) {
        MOT_LOG_ERROR("RecoverDbEnd: Recovery failed!");
        return false;
    }

    MOT_LOG_INFO("RecoverDbEnd: Recovery finished successfully (CSN: %lu, TxnId: %lu)", m_maxCsn, m_maxTransactionId);
    return true;
}

bool BaseRecoveryManager::ApplyRedoLog(uint64_t redoLsn, char* data, size_t len)
{
    if (redoLsn <= m_lsn) {
        // ignore old redo records which are prior to our checkpoint LSN
        MOT_LOG_INFO("ApplyRedoLog - ignoring old redo record. Checkpoint LSN: %lu, redo LSN: %lu", m_lsn, redoLsn);
        return true;
    }
    return ApplyLogSegmentData(data, len, redoLsn);
}

bool BaseRecoveryManager::Is1VCCLogSegment(LogSegment* segment)
{
    MOT_ASSERT(segment);
    if (segment != nullptr) {
        return (segment->m_controlBlock.m_opCode == OperationCode::COMMIT_TX_1VCC ||
                segment->m_controlBlock.m_opCode == OperationCode::PARTIAL_REDO_TX_1VCC);
    }
    return false;
}

bool BaseRecoveryManager::IsMotOnlyTransaction(LogSegment* segment)
{
    MOT_ASSERT(segment);
    if (segment != nullptr) {
        return (segment->m_controlBlock.m_opCode == OperationCode::COMMIT_TX ||
                segment->m_controlBlock.m_opCode == OperationCode::PARTIAL_REDO_TX ||
                segment->m_controlBlock.m_externalTransactionId == INVALID_TRANSACTION_ID);  // 1VCC log segment
    }
    return false;
}

bool BaseRecoveryManager::IsDDLTransaction(LogSegment* segment)
{
    MOT_ASSERT(segment);
    if (segment != nullptr) {
        return (
            segment->m_controlBlock.m_opCode == OperationCode::COMMIT_DDL_TX ||
            segment->m_controlBlock.m_opCode == OperationCode::PARTIAL_REDO_DDL_TX ||
            (Is1VCCLogSegment(segment) && segment->m_controlBlock.m_externalTransactionId != INVALID_TRANSACTION_ID));
    }
    return false;
}

bool BaseRecoveryManager::HasUpdateIndexColumn(LogSegment* segment) const
{
    MOT_ASSERT(segment);
    if (segment != nullptr) {
        return static_cast<bool>(segment->m_controlBlock.m_flags & EndSegmentBlock::MOT_UPDATE_INDEX_COLUMN_FLAG);
    }
    return false;
}

bool BaseRecoveryManager::IsTransactionIdCommitted(uint64_t xid)
{
    MOT_ASSERT(m_clogCallback);
    if (m_clogCallback != nullptr) {
        return ((*m_clogCallback)(xid) == TXN_COMMITED);
    }
    return false;
}

void BaseRecoveryManager::AddSurrogateArrayToList(SurrogateState& surrogate)
{
    std::lock_guard<std::mutex> lock(m_surrogateListLock);
    if (!surrogate.IsEmpty()) {
        uint64_t* newArray = (uint64_t*)malloc(surrogate.GetMaxConnections() * sizeof(uint64_t));
        if (newArray != nullptr) {
            errno_t erc = memcpy_s(newArray,
                surrogate.GetMaxConnections() * sizeof(uint64_t),
                surrogate.GetArray(),
                surrogate.GetMaxConnections() * sizeof(uint64_t));
            securec_check(erc, "\0", "\0");
            m_surrogateList.push_back(newArray);
        }
    }
}

void BaseRecoveryManager::ApplySurrogate()
{
    // merge and apply all SurrogateState maps
    SurrogateState::Merge(m_surrogateList, m_surrogateState);
    m_surrogateList.clear();

    if (m_surrogateState.IsEmpty()) {
        return;
    }

    const uint64_t* array = m_surrogateState.GetArray();
    for (int i = 0; i < m_maxConnections; i++) {
        GetSurrogateKeyManager()->SetSurrogateSlot(i, array[i]);
    }
}
}  // namespace MOT
