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
 * base_recovery_manager.h
 *    Implements the shared functionality for all recovery managers
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/recovery/base_recovery_manager.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef BASE_RECOVERY_MANAGER_H
#define BASE_RECOVERY_MANAGER_H

#include <atomic>
#include "checkpoint_ctrlfile.h"
#include "mot_configuration.h"
#include "irecovery_manager.h"
#include "recovery_stats.h"
#include "surrogate_state.h"
#include "checkpoint_recovery.h"
#include "recovery_ops.h"

namespace MOT {

/**
 * @class BaseRecoveryManager
 * @brief Shared recovery functionality
 */
class BaseRecoveryManager : public IRecoveryManager {
public:
    BaseRecoveryManager()
        : m_initialized(false),
          m_lsn(0),
          m_lastReplayLsn(0),
          m_errorSet(false),
          m_maxCsn(INVALID_CSN),
          m_maxTransactionId(INVALID_TRANSACTION_ID),
          m_clogCallback(nullptr),
          m_recoverFromCkptDone(false),
          m_maxConnections(GetGlobalConfiguration().m_maxConnections),
          m_enableLogStats(GetGlobalConfiguration().m_enableLogRecoveryStats),
          m_recoveryTableStats(nullptr)
    {}

    ~BaseRecoveryManager() override
    {
        if (m_recoveryTableStats != nullptr) {
            delete m_recoveryTableStats;
            m_recoveryTableStats = nullptr;
        }
        m_clogCallback = nullptr;
    }

    /**
     * @brief Performs the necessary tasks to initialize the object.
     * @return Boolean value denoting success or failure.
     */
    bool Initialize() override;

    void SetCommitLogCallback(CommitLogStatusCallback clogCallback) override
    {
        m_clogCallback = clogCallback;
    }

    /**
     * @brief Starts the recovery process.
     * @return Boolean value denoting success or failure.
     */
    bool RecoverDbStart() override;

    /**
     * @brief Performs post recovery operations.
     * @return Boolean value denoting success or failure.
     */
    bool RecoverDbEnd() override;

    /**
     * @brief attempts to insert a data chunk into the in-process
     * transactions map and operate on it. Checks that the redo lsn is after the
     * checkpoint snapshot lsn taken. Redo records that are prior snapshot are
     * ignored.
     * @return Boolean value denoting success or failure.
     */
    bool ApplyRedoLog(uint64_t redoLsn, char* data, size_t len) override;

    /**
     * @brief Checks if the given log segment is an 1VCC one (deprecated).
     * @param segment Log segment to check.
     * @return Boolean value that is true if the log segment is an 1VCC one.
     */
    static bool Is1VCCLogSegment(LogSegment* segment);

    /**
     * @brief checks if the transaction is an mot only one.
     * @param segment the log segment to check.
     * @return Boolean value that is true if the txn is an mot only one.
     */
    static bool IsMotOnlyTransaction(LogSegment* segment);

    /**
     * @brief Checks if the transaction has DDL operations.
     * @param segment Log segment to check.
     * @return Boolean value that is true if the txn has DDL operations.
     */
    static bool IsDDLTransaction(LogSegment* segment);

    /**
     * @brief Checks if the transaction has update index column operations.
     * @param segment Log segment to check.
     * @return Boolean value that is true if the txn has these operations.
     */
    bool HasUpdateIndexColumn(LogSegment* segment) const;

    /**
     * @brief checks the clog if a transaction id is in commit state.
     * @param xid the transaction id.
     * @return Boolean value that is true if the transaction is committed.
     */
    virtual bool IsTransactionIdCommitted(uint64_t xid);

    void AddSurrogateArrayToList(MOT::SurrogateState& surrogate) override;

    void ApplySurrogate();

    /**
     * @brief Sets the flag to indicate an error in recovery.
     */
    void SetError() override
    {
        m_errorSet.store(true);
    }

    bool IsErrorSet() const override
    {
        return m_errorSet.load();
    }

    void SetLsn(uint64_t lsn)
    {
        m_lsn = lsn;
    }

    void SetLastReplayLsn(uint64_t replayLsn) override
    {
        if (m_lastReplayLsn.load() < replayLsn) {
            m_lastReplayLsn.store(replayLsn);
        }
    }

    uint64_t GetLastReplayLsn() const override
    {
        return m_lastReplayLsn.load();
    }

    void SetMaxCsn(uint64_t csn) override
    {
        if (m_maxCsn < csn) {
            m_maxCsn = csn;
        }
    }

    inline uint64_t GetMaxTransactionId() const
    {
        return m_maxTransactionId;
    }

    inline void SetMaxTransactionId(uint64_t id)
    {
        if (m_maxTransactionId < id) {
            m_maxTransactionId = id;
        }
    }

    void LogInsert(uint64_t tableId) override
    {
        if (m_recoveryTableStats != nullptr) {
            m_recoveryTableStats->IncInsert(tableId);
        }
    }

    void LogUpdate(uint64_t tableId) override
    {
        if (m_recoveryTableStats != nullptr) {
            m_recoveryTableStats->IncUpdate(tableId);
        }
    }

    void LogDelete(uint64_t tableId) override
    {
        if (m_recoveryTableStats != nullptr) {
            m_recoveryTableStats->IncDelete(tableId);
        }
    }

    void LogCommit() override
    {
        if (m_recoveryTableStats != nullptr) {
            m_recoveryTableStats->IncCommit();
        }
    }

protected:
    bool m_initialized;
    uint64_t m_lsn;
    std::atomic<uint64_t> m_lastReplayLsn;
    std::mutex m_surrogateListLock;
    std::list<uint64_t*> m_surrogateList;
    SurrogateState m_surrogateState;
    std::atomic<bool> m_errorSet;
    uint64_t m_maxCsn;
    uint64_t m_maxTransactionId;
    CheckpointRecovery m_checkpointRecovery;

private:
    /**
     * @brief attempts to insert a data chunk into the in-process
     * transactions map and operate on it
     * @return Boolean value denoting success or failure.
     */
    virtual bool ApplyLogSegmentData(char* data, size_t len, uint64_t replayLsn) = 0;

    CommitLogStatusCallback m_clogCallback;
    bool m_recoverFromCkptDone;
    uint16_t m_maxConnections;
    bool m_enableLogStats;
    RecoveryStats* m_recoveryTableStats;
};

}  // namespace MOT

#endif  // BASE_RECOVERY_MANAGER_H
