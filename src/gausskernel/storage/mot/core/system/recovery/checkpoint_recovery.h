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
 * checkpoint_recovery.h
 *    Handles recovery from checkpoint.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/recovery/checkpoint_recovery.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef CHECKPOINT_RECOVERY_H
#define CHECKPOINT_RECOVERY_H

#include <set>
#include <list>
#include <mutex>
#include "global.h"
#include "spin_lock.h"
#include "table.h"
#include "surrogate_state.h"

namespace MOT {
class CheckpointRecovery {
public:
    CheckpointRecovery()
        : m_checkpointId(0),
          m_lsn(0),
          m_lastReplayLsn(0),
          m_numWorkers(GetGlobalConfiguration().m_checkpointRecoveryWorkers),
          m_stopWorkers(false),
          m_errorSet(false),
          m_errorCode(RC_OK)
    {}

    ~CheckpointRecovery()
    {}

    /**
     * @brief Recovers the database state from the last valid
     * checkpoint
     * @return Boolean value denoting success or failure.
     */
    bool Recover();

    /**
     * @brief error callback.
     */
    void OnError(RC errCode, const char* errMsg, const char* optionalMsg = nullptr);

    bool ShouldStopWorkers() const
    {
        return m_stopWorkers;
    }

    /**
     * @struct Task
     * @brief Describes a checkpoint recovery task by its table id and
     * segment file number.
     */
    struct Task {
        explicit Task(uint32_t tableId = 0, uint32_t segId = 0) : m_tableId(tableId), m_segId(segId)
        {}

        uint32_t m_tableId;
        uint32_t m_segId;
    };

    /**
     * @brief Pops a task from the tasks queue.
     * @return The task that was retrieved from the queue.
     */
    CheckpointRecovery::Task* GetTask();

    /**
     * @brief Reads and inserts rows from a checkpoint file
     * @param task The task (tableid / segment) to recover from.
     * @param keyData A key buffer.
     * @param entryData A row buffer..
     * @param maxCsn The returned maxCsn encountered during the recovery.
     * @param sState Surrogate key state structure that will be filled.
     * during the recovery.
     * @param status RC returned from the Insert function.
     * @return Boolean value denoting success or failure.
     */
    bool RecoverTableRows(
        Task* task, char* keyData, char* entryData, uint64_t& maxCsn, SurrogateState& sState, RC& status);

    uint64_t GetLsn() const
    {
        return m_lsn;
    }

    /**
     * @brief Implements the a checkpoint recovery worker
     * @param checkpointRecovery The caller checkpoint recovery class
     */
    static void CheckpointRecoveryWorker(CheckpointRecovery* checkpointRecovery);

private:
    /**
     * @brief Reads and creates a table's definition from a checkpoint
     * metadata file
     * @param tableId The table id to recover.
     * @return Boolean value denoting success or failure.
     */
    bool RecoverTableMetadata(uint32_t tableId);

    /**
     * @brief Reads the checkpoint map file and fills the tasks queue
     * with the relevant information.
     * @return Int value where 0 indicates no tasks (empty checkpoint),
     * -1 denotes an error has occurred and 1 means a success.
     */
    int FillTasksFromMapFile();

    /**
     * @brief Checks if there are any more tasks left in the queue
     * @return Int value where 0 means failure and 1 success
     */
    uint32_t HaveTasks();

    bool PerformRecovery();

    /**
     * @brief Recovers the in process two phase commit related transactions
     * from the checkpoint data file.
     * @return Boolean value denoting success or failure.
     */
    bool RecoverInProcessTxns();

    /**
     * @brief Deserializes the in process two phase commit data from the
     * checkpoint data file. called by RecoverTpc.
     * @return Boolean value denoting success or failure.
     */
    bool DeserializeInProcessTxns(int fd, uint64_t numEntries);

    /**
     * @brief Inserts a row into the database in a non transactional manner.
     * @param table the table's object pointer.
     * @param keyData key's data buffer.
     * @param keyLen key's data buffer len.
     * @param rowData row's data buffer.
     * @param rowLen row's data buffer len.
     * @param csn the operation's csn.
     * @param tid the thread id of the recovering thread.
     * @param sState the returned surrogate state.
     * @param status the returned status of the operation
     * @param rowId the row's internal id
     */
    void InsertRow(Table* table, char* keyData, uint16_t keyLen, char* rowData, uint64_t rowLen, uint64_t csn,
        uint32_t tid, SurrogateState& sState, RC& status, uint64_t rowId);

    /**
     * @brief performs table creation.
     * @param data the table's data
     * @return Boolean value that represents that status of the operation.
     */
    bool CreateTable(char* data);

    /**
     * @brief returns if a checkpoint is valid by its id.
     * @param id the checkpoint's id.
     * @return Boolean value that is true if the transaction is committed.
     */
    bool IsCheckpointValid(uint64_t id);

    /**
     * @brief checks if we have enough space for a segment recovery.
     * @param numThreads number of workers.
     * @param neededBytes the segment size in bytes.
     * @return Boolean value that is true if there is not enough memory for
     * recovery.
     */
    bool IsMemoryLimitReached(uint32_t numThreads, uint32_t neededMBs);

    uint64_t m_checkpointId;

    uint64_t m_lsn;

    uint64_t m_lastReplayLsn;

    uint32_t m_numWorkers;

    std::string m_workingDir;

    std::string m_errorMessage;

    bool m_stopWorkers;

    bool m_errorSet;

    RC m_errorCode;

    spin_lock m_errorLock;

    std::mutex m_tasksLock;

    std::set<uint32_t> m_tableIds;

    std::list<Task*> m_tasksList;
};
}  // namespace MOT

#endif /* CHECKPOINT_RECOVERY_H */
