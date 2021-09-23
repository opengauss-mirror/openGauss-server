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
 * recovery_manager.h
 *    Handles all recovery tasks, including recovery from a checkpoint, xlog and 2PC operations.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/recovery/recovery_manager.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef RECOVERY_MANAGER_H
#define RECOVERY_MANAGER_H

#include <vector>
#include "checkpoint_ctrlfile.h"
#include "redo_log_global.h"
#include "redo_log_transaction_iterator.h"
#include "txn.h"
#include "global.h"
#include "mot_configuration.h"
#include "irecovery_manager.h"
#include "surrogate_state.h"
#include "checkpoint_recovery.h"
#include "recovery_ops.h"

namespace MOT {
/**
 * @class RecoveryManager
 * @brief handles all recovery tasks, including recovery from
 * a checkpoint, xlog and 2 pc operations
 */
class RecoveryManager : public IRecoveryManager {
public:
    RecoveryManager()
        : m_logStats(nullptr),
          m_initialized(false),
          m_recoverFromCkptDone(false),
          m_lsn(0),
          m_lastReplayLsn(0),
          m_tid(0),
          m_maxRecoveredCsn(0),
          m_enableLogStats(GetGlobalConfiguration().m_enableLogRecoveryStats),
          m_errorSet(false),
          m_clogCallback(nullptr),
          m_threadId(AllocThreadId()),
          m_maxConnections(GetGlobalConfiguration().m_maxConnections)
    {}

    ~RecoveryManager() override
    {}

    /**
     * @brief Performs the necessary tasks to initialize the object.
     * @return Boolean value denoting success or failure.
     */
    bool Initialize() override;

    /**
     * @brief Cleans up the recovery object.
     */
    void CleanUp() override;

    inline void SetCommitLogCallback(CommitLogStatusCallback clogCallback) override
    {
        this->m_clogCallback = clogCallback;
    }

    /**
     * @brief Starts the recovery process which currently consists of
     * checkpoint recovery.
     * @return Boolean value denoting success or failure.
     */
    bool RecoverDbStart() override;

    /**
     * @brief Performs the post recovery operations: apply the in-process
     * transactions and surrogate array, set the max csn and prints the stats
     * @return Boolean value denoting success or failure.
     */
    bool RecoverDbEnd() override;

    /**
     * @brief attempts to insert a data chunk into the in-process
     * transactions map and operate on it
     * @return Boolean value denoting success or failure.
     */
    bool ApplyLogSegmentFromData(char* data, size_t len, uint64_t replayLsn = 0) override;

    /**
     * @brief attempts to insert a data chunk into the in-process
     * transactions map and operate on it. Checks that the redo lsn is after the
     * checkpoint snapshot lsn taken. Redo records that are prior snapshot are
     * ignored.
     * @return Boolean value denoting success or failure.
     */
    bool ApplyRedoLog(uint64_t redoLsn, char* data, size_t len) override;
    /**
     * @brief performs a commit on an in-process transaction,
     * @return Boolean value denoting success or failure to commit.
     */
    bool CommitRecoveredTransaction(uint64_t externalTransactionId) override;

    void SetCsn(uint64_t csn) override;

    /**
     * @brief adds the surrogate array inside surrogate to the surrogate list
     */
    void AddSurrogateArrayToList(SurrogateState& surrogate) override;

    bool IsErrorSet() const override
    {
        return m_errorSet;
    }

    inline void SetLastReplayLsn(uint64_t replayLsn) override
    {
        if (m_lastReplayLsn < replayLsn) {
            m_lastReplayLsn = replayLsn;
        }
    }

    inline uint64_t GetLastReplayLsn() const override
    {
        return m_lastReplayLsn;
    }

    /**
     * @class LogStats
     * @brief A per-table recovery stats collector
     */
    class LogStats {
    public:
        struct Entry {
            explicit Entry(uint64_t tableId) : m_inserts(0), m_updates(0), m_deletes(0), m_id(tableId)
            {}

            void IncInsert()
            {
                ++m_inserts;
            }

            void IncUpdate()
            {
                ++m_updates;
            }

            void IncDelete()
            {
                ++m_deletes;
            }

            std::atomic<uint64_t> m_inserts;

            std::atomic<uint64_t> m_updates;

            std::atomic<uint64_t> m_deletes;

            uint64_t m_id;
        };

        LogStats() : m_commits(0), m_numEntries(0)
        {}

        ~LogStats()
        {
            for (std::vector<Entry*>::iterator it = m_tableStats.begin(); it != m_tableStats.end(); ++it) {
                if (*it) {
                    delete *it;
                }
            }
        }

        void IncInsert(uint64_t id)
        {
            uint64_t idx = 0;
            if (FindIdx(id, idx)) {
                m_tableStats[idx]->IncInsert();
            }
        }

        void IncUpdate(uint64_t id)
        {
            uint64_t idx = 0;
            if (FindIdx(id, idx)) {
                m_tableStats[idx]->IncUpdate();
            }
        }

        void IncDelete(uint64_t id)
        {
            uint64_t idx = 0;
            if (FindIdx(id, idx)) {
                m_tableStats[idx]->IncDelete();
            }
        }

        inline void IncCommit()
        {
            ++m_commits;
        }

        /**
         * @brief Prints the stats data to the log
         */
        void Print();

    private:
        /**
         * @brief Returns a table id array index. it will create
         * a new table entry if necessary.
         * @param tableId The id of the table.
         * @param id The returned array index.
         * @return Boolean value denoting success or failure.
         */
        bool FindIdx(uint64_t tableId, uint64_t& id);

        std::map<uint64_t, uint64_t> m_idToIdx;

        std::vector<Entry*> m_tableStats;

        std::atomic<uint64_t> m_commits;

        spin_lock m_slock;

        uint64_t m_numEntries;
    };

    LogStats* m_logStats;

    std::map<uint64_t, RecoveryOps::TableInfo*> m_preCommitedTables;

private:
    static constexpr uint32_t NUM_REDO_RECOVERY_THREADS = 1;

    /**
     * @brief performs a redo on a segment, which is either a recovery op
     * or a segment that belongs to a 2pc recovered transaction.
     * @param segment the segment to redo.
     * @param csn the segment's csn
     * @param transactionId the transaction id of the segment
     * @param rState the operation to perform on the segment.
     * @return RC value denoting the operation's status
     */
    RC RedoSegment(LogSegment* segment, uint64_t csn, uint64_t transactionId, RecoveryOps::RecoveryOpState rState);

    /**
     * @brief inserts a segment in to the in-process transactions map
     * @param segment the segment to redo.
     * @return Boolean value denoting success or failure.
     */
    bool InsertLogSegment(LogSegment* segment);

    /**
     * @brief performs an operation on a recovered transaction.
     * @param internalTransactionId the internal transaction id to operate on.
     * @param externalTransactionId the external transaction id to operate on.
     * @param rState the transaction id of the segment
     * @return Boolean value denoting success or failure.
     */
    bool OperateOnRecoveredTransaction(
        uint64_t internalTransactionId, uint64_t externalTransactionId, RecoveryOps::RecoveryOpState rState);

    /**
     * @brief checks if a transaction id specified in the log segment is an mot
     * only one.
     * @param segment the log segment to check.
     * @return Boolean value that is true if the id is an mot one.
     */
    static bool IsMotTransactionId(LogSegment* segment);

    /**
     * @brief checks the clog if a transaction id is in commit state.
     * @param xid the transaction id.
     * @return Boolean value that is true if the transaction is committed.
     */
    bool IsTransactionIdCommitted(uint64_t xid);

    /**
     * @brief returns if a checkpoint is valid by its id.
     * @param id the checkpoint's id.
     * @return Boolean value that is true if the transaction is committed.
     */
    bool IsCheckpointValid(uint64_t id);

    /**
     * @brief returns if a recovery memory limit reached.
     * @return Boolean value that is true if there is not enough memory for
     * recovery.
     */
    bool IsRecoveryMemoryLimitReached(uint32_t numThreads);

    /**
     * @brief restores the surrogate counters to their last good known state
     */
    void ApplySurrogate();

    bool m_initialized;

    bool m_recoverFromCkptDone;

    uint64_t m_lsn;

    uint64_t m_lastReplayLsn;

    std::atomic<uint32_t> m_tid;

    std::atomic<uint64_t> m_maxRecoveredCsn;

    bool m_enableLogStats;

    SurrogateState m_surrogateState;

    std::mutex m_surrogateListLock;

    std::list<uint64_t*> m_surrogateList;

    bool m_errorSet;

    CommitLogStatusCallback m_clogCallback;

    int m_threadId;

    SurrogateState m_sState;

    uint16_t m_maxConnections;

    CheckpointRecovery m_checkpointRecovery;
};
}  // namespace MOT

#endif /* RECOVERY_MANAGER_H */
