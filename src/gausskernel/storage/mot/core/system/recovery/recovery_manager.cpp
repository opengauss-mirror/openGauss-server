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
 * recovery_manager.cpp
 *    Handles all recovery tasks, including recovery from a checkpoint, xlog and 2PC operations.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/recovery/recovery_manager.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <list>
#include <atomic>
#include <thread>
#include "mot_engine.h"
#include "recovery_manager.h"
#include "checkpoint_utils.h"
#include "checkpoint_manager.h"
#include "spin_lock.h"
#include "redo_log_transaction_iterator.h"
#include "mot_engine.h"

namespace MOT {
DECLARE_LOGGER(RecoveryManager, Recovery);

bool RecoveryManager::Initialize()
{
    // in a thread-pooled envelope the affinity could be disabled, so we use task affinity here
    if (GetGlobalConfiguration().m_enableNuma) {
        Affinity& affinity = GetTaskAffinity();
        affinity.SetAffinity(m_threadId);
    }

    if (m_enableLogStats) {
        m_logStats = new (std::nothrow) LogStats();
        if (m_logStats == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Recovery Manager Initialization", "Failed to allocate statistics object");
            return false;
        }
    }

    if (CheckpointControlFile::GetCtrlFile() == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Recovery Manager Initialization", "Failed to allocate ctrlfile object");
        return false;
    }

    if (m_surrogateState.IsValid() == false || m_sState.IsValid() == false) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Recovery Manager Initialization", "Failed to allocate surrogate state object");
        return false;
    }

    m_initialized = true;
    return m_initialized;
}

bool RecoveryManager::RecoverDbStart()
{
    MOT_LOG_INFO("Starting MOT recovery");

    if (m_recoverFromCkptDone) {
        /*
         * This is switchover case.
         * In case of CASCADE switchover (CASCADE node switchover with STANDBY node), we need to
         * set the correct m_lsn to make sure that we skip any redo which are already replayed.
         * After switchover, envelope will start sync from the previous ckpt position.
         * In MOT engine, we should skip any redo before m_lastReplayLsn.
         */
        if (m_lsn < m_lastReplayLsn) {
            m_lsn = m_lastReplayLsn;
        }
        return true;
    }

    if (!m_checkpointRecovery.Recover()) {
        return false;
    }

    m_lsn = m_checkpointRecovery.GetLsn();
    m_recoverFromCkptDone = true;
    return true;
}

bool RecoveryManager::RecoverDbEnd()
{
    if (MOTEngine::GetInstance()->GetInProcessTransactions().GetNumTxns() != 0) {
        MOT_LOG_ERROR("MOT recovery: There are uncommitted or incomplete transactions, "
                      "ignoring and clearing those log segments.");
        MOTEngine::GetInstance()->GetInProcessTransactions().Clear();
    }

    if (m_sState.IsEmpty() == false) {
        AddSurrogateArrayToList(m_sState);
    }

    // set global commit sequence number
    GetCSNManager().SetCSN(m_maxRecoveredCsn);

    // merge and apply all SurrogateState maps
    SurrogateState::Merge(m_surrogateList, m_surrogateState);
    m_surrogateList.clear();
    ApplySurrogate();

    if (m_enableLogStats && m_logStats != nullptr) {
        m_logStats->Print();
    }

    bool success = !m_errorSet;
    MOT_LOG_INFO("MOT recovery %s", success ? "completed" : "failed");
    return success;
}

void RecoveryManager::CleanUp()
{
    if (!m_initialized) {
        return;
    }

    if (m_logStats != nullptr) {
        delete m_logStats;
        m_logStats = nullptr;
    }

    m_initialized = false;
}

bool RecoveryManager::ApplyRedoLog(uint64_t redoLsn, char* data, size_t len)
{
    if (redoLsn <= m_lsn) {
        // ignore old redo records which are prior to our checkpoint LSN
        MOT_LOG_DEBUG("ApplyRedoLog - ignoring old redo record. Checkpoint LSN: %lu, redo LSN: %lu", m_lsn, redoLsn);
        return true;
    }
    return ApplyLogSegmentFromData(data, len, redoLsn);
}

bool RecoveryManager::ApplyLogSegmentFromData(char* data, size_t len, uint64_t replayLsn /* = 0 */)
{
    bool result = false;
    char* curData = data;

    while (data + len > curData) {
        // obtain LogSegment from buffer
        RedoLogTransactionIterator iterator(curData, len);
        LogSegment* segment = iterator.AllocRedoSegment(replayLsn);
        if (segment == nullptr) {
            MOT_LOG_ERROR("ApplyLogSegmentFromData - failed to allocate segment");
            return false;
        }

        // check LogSegment Op validity
        OperationCode opCode = segment->m_controlBlock.m_opCode;
        if (opCode >= OperationCode::INVALID_OPERATION_CODE) {
            MOT_LOG_ERROR("ApplyLogSegmentFromData - encountered a bad opCode %u", opCode);
            delete segment;
            return false;
        }

        // build operation params
        uint64_t inId = segment->m_controlBlock.m_internalTransactionId;
        uint64_t exId = segment->m_controlBlock.m_externalTransactionId;
        RecoveryOps::RecoveryOpState recoveryState =
            IsCommitOp(opCode) ? RecoveryOps::RecoveryOpState::COMMIT : RecoveryOps::RecoveryOpState::ABORT;
        MOT_LOG_DEBUG("ApplyLogSegmentFromData: opCode %u, externalTransactionId %lu, internalTransactionId %lu",
            opCode,
            exId,
            inId);

        // insert the segment (if not abort)
        if (!IsAbortOp(opCode) && !MOTEngine::GetInstance()->GetInProcessTransactions().InsertLogSegment(segment)) {
            MOT_LOG_ERROR("ApplyLogSegmentFromData - insert log segment failed");
            delete segment;
            return false;
        }

        // operate on the transaction if:
        // 1. abort
        // 2. mot transaction (exid = 0)
        // 3. regular transaction that's committed in the clog
        if (IsAbortOp(opCode) ||
            (IsCommitOp(opCode) && (IsMotTransactionId(segment) || IsTransactionIdCommitted(exId)))) {
            result = OperateOnRecoveredTransaction(inId, exId, recoveryState);
            if (IsAbortOp(opCode)) {
                delete segment;
            }
            if (!result) {
                MOT_LOG_ERROR("ApplyLogSegmentFromData - operateOnRecoveredTransaction failed (abort)");
                return false;
            }
        } else {
            MOT_LOG_DEBUG("ApplyLogSegmentFromData: Added to map, opCode %u, externalTransactionId %lu, "
                          "internalTransactionId %lu",
                opCode,
                exId,
                inId);
        }
        curData += iterator.GetRedoTransactionLength();
    }
    return true;
}

bool RecoveryManager::CommitRecoveredTransaction(uint64_t externalTransactionId)
{
    uint64_t internalId = 0;
    if (MOTEngine::GetInstance()->GetInProcessTransactions().FindTransactionId(externalTransactionId, internalId)) {
        return OperateOnRecoveredTransaction(internalId, externalTransactionId, RecoveryOps::RecoveryOpState::COMMIT);
    }
    return true;
}

bool RecoveryManager::OperateOnRecoveredTransaction(
    uint64_t internalTransactionId, uint64_t externalTransactionId, RecoveryOps::RecoveryOpState rState)
{
    RC status = RC_OK;
    if (rState != RecoveryOps::RecoveryOpState::ABORT) {
        auto operateLambda = [this](RedoLogTransactionSegments* segments, uint64_t id) -> RC {
            RC redoStatus = RC_OK;
            LogSegment* segment = segments->GetSegment(segments->GetCount() - 1);
            uint64_t csn = segment->m_controlBlock.m_csn;
            for (uint32_t i = 0; i < segments->GetCount(); i++) {
                segment = segments->GetSegment(i);
                redoStatus = RedoSegment(segment, csn, id, RecoveryOps::RecoveryOpState::COMMIT);
                if (redoStatus != RC_OK) {
                    MOT_LOG_ERROR("OperateOnRecoveredTransaction failed with rc %d", redoStatus);
                    return redoStatus;
                }
            }
            return redoStatus;
        };

        status = MOTEngine::GetInstance()->GetInProcessTransactions().ForUniqueTransaction(
            internalTransactionId, externalTransactionId, operateLambda);
    }
    if (status != RC_OK) {
        MOT_LOG_ERROR("OperateOnRecoveredTransaction: wal recovery failed");
        return false;
    }
    return true;
}

RC RecoveryManager::RedoSegment(
    LogSegment* segment, uint64_t csn, uint64_t transactionId, RecoveryOps::RecoveryOpState rState)
{
    RC status = RC_OK;
    uint8_t* endPosition = (uint8_t*)(segment->m_data + segment->m_len);
    uint8_t* operationData = (uint8_t*)(segment->m_data);
    bool txnStarted = false;
    bool wasCommit = false;

    while (operationData < endPosition) {
        // redo log recovery - single threaded
        if (IsRecoveryMemoryLimitReached(NUM_REDO_RECOVERY_THREADS)) {
            status = RC_ERROR;
            MOT_LOG_ERROR("Memory hard limit reached. Cannot recover datanode");
            break;
        }

        // begin transaction on-demand
        if (!txnStarted) {
            if (RecoveryOps::BeginTransaction(MOTCurrTxn, segment->m_replayLsn) != RC_OK) {
                status = RC_ERROR;
                MOT_REPORT_ERROR(MOT_ERROR_RESOURCE_LIMIT, "Recover Redo Segment", "Cannot start a new transaction");
                break;
            }

            txnStarted = true;
        }

        operationData += RecoveryOps::RecoverLogOperation(
            MOTCurrTxn, operationData, csn, transactionId, MOTCurrThreadId, m_sState, status, wasCommit);

        // check operation result status
        if (status != RC_OK) {
            MOT_REPORT_ERROR(MOT_ERROR_RESOURCE_LIMIT, "Recover Redo Segment", "Failed to recover redo segment");
            break;
        }

        // update transactional state
        if (wasCommit) {
            txnStarted = false;
        }
    }

    // single threaded, no need for locking or CAS
    if (csn > m_maxRecoveredCsn) {
        m_maxRecoveredCsn = csn;
    }
    if (status != RC_OK) {
        MOT_LOG_ERROR("RecoveryManager::redoSegment: got error %u on tid %lu", status, transactionId);
    }
    return status;
}

bool RecoveryManager::LogStats::FindIdx(uint64_t tableId, uint64_t& id)
{
    id = m_numEntries;
    std::map<uint64_t, uint64_t>::iterator it;
    std::lock_guard<spin_lock> lock(m_slock);
    it = m_idToIdx.find(tableId);
    if (it == m_idToIdx.end()) {
        Entry* newEntry = new (std::nothrow) Entry(tableId);
        if (newEntry == nullptr) {
            return false;
        }
        m_tableStats.push_back(newEntry);
        m_idToIdx.insert(std::pair<uint64_t, uint64_t>(tableId, m_numEntries));
        m_numEntries++;
    } else {
        id = it->second;
    }
    return true;
}

void RecoveryManager::LogStats::Print()
{
    MOT_LOG_ERROR(">> log recovery stats >>");
    for (uint64_t i = 0; i < m_numEntries; i++) {
        MOT_LOG_ERROR("TableId %lu, Inserts: %lu, Updates: %lu, Deletes: %lu",
            m_tableStats[i]->m_id,
            m_tableStats[i]->m_inserts.load(),
            m_tableStats[i]->m_updates.load(),
            m_tableStats[i]->m_deletes.load());
    }
    MOT_LOG_ERROR("Overall tcls: %lu", m_commits.load());
}

void RecoveryManager::SetCsn(uint64_t csn)
{
    uint64_t currentCsn = m_maxRecoveredCsn;
    while (currentCsn < csn) {
        m_maxRecoveredCsn.compare_exchange_weak(currentCsn, csn);
        currentCsn = m_maxRecoveredCsn;
    }
}

void RecoveryManager::AddSurrogateArrayToList(SurrogateState& surrogate)
{
    m_surrogateListLock.lock();
    if (surrogate.IsEmpty() == false) {
        uint64_t* newArray = new (std::nothrow) uint64_t[surrogate.GetMaxConnections()];
        if (newArray != nullptr) {
            errno_t erc = memcpy_s(newArray,
                surrogate.GetMaxConnections() * sizeof(uint64_t),
                surrogate.GetArray(),
                surrogate.GetMaxConnections() * sizeof(uint64_t));
            securec_check(erc, "\0", "\0");
            m_surrogateList.push_back(newArray);
        }
    }
    m_surrogateListLock.unlock();
}

void RecoveryManager::ApplySurrogate()
{
    if (m_surrogateState.IsEmpty()) {
        return;
    }

    const uint64_t* array = m_surrogateState.GetArray();
    for (int i = 0; i < m_maxConnections; i++) {
        GetSurrogateKeyManager()->SetSurrogateSlot(i, array[i]);
    }
}

bool RecoveryManager::IsMotTransactionId(LogSegment* segment)
{
    MOT_ASSERT(segment);
    if (segment != nullptr) {
        return segment->m_controlBlock.m_externalTransactionId == INVALID_TRANSACTION_ID;
    }
    return false;
}

bool RecoveryManager::IsTransactionIdCommitted(uint64_t xid)
{
    MOT_ASSERT(m_clogCallback);
    if (m_clogCallback != nullptr) {
        return (*m_clogCallback)(xid) == TXN_COMMITED;
    }
    return false;
}

bool RecoveryManager::IsRecoveryMemoryLimitReached(uint32_t numThreads)
{
    uint64_t memoryRequiredBytes = (uint64_t)numThreads * MEM_CHUNK_SIZE_MB * MEGA_BYTE;
    if (MOTEngine::GetInstance()->GetCurrentMemoryConsumptionBytes() + memoryRequiredBytes >=
        MOTEngine::GetInstance()->GetHardMemoryLimitBytes()) {
        MOT_LOG_WARN("IsRecoveryMemoryLimitReached: recovery memory limit reached "
                     "current memory: %lu, required memory: %lu, hard limit memory: %lu",
            MOTEngine::GetInstance()->GetCurrentMemoryConsumptionBytes(),
            memoryRequiredBytes,
            MOTEngine::GetInstance()->GetHardMemoryLimitBytes());
        return true;
    } else {
        return false;
    }
}
}  // namespace MOT
