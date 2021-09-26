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
 * checkpoint_manager.h
 *    Interface for all checkpoint related tasks.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/checkpoint/checkpoint_manager.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef CHECKPOINT_MANAGER_H
#define CHECKPOINT_MANAGER_H

#include <atomic>
#include <iostream>
#include <pthread.h>
#include "rw_lock.h"
#include "global.h"
#include "txn.h"
#include "txn_access.h"
#include <queue>
#include "checkpoint_worker.h"
#include "checkpoint_ctrlfile.h"
#include "spin_lock.h"

namespace MOT {
/**
 * @class CheckpointManager
 * @brief this class is responsible and is the main interface
 * for all checkpoint related tasks
 */
class CheckpointManager : public CheckpointManagerCallbacks {
public:
    CheckpointManager();

    virtual ~CheckpointManager();

    bool Initialize();

    /**
     * @brief Starts an MOT checkpoint snapshot operation.
     * @return Boolean value denoting success or failure.
     */
    bool CreateSnapShot();

    /**
     * @brief Notify that a checkpoint snapshot is ready.
     * @param lsn checkpoint capture start point
     * @return Boolean value denoting success or failure.
     */
    bool SnapshotReady(uint64_t lsn);

    /**
     * @brief Begins an MOT checkpoint capture operation.
     * @return Boolean value denoting success or failure.
     */
    bool BeginCheckpoint();

    /**
     * @brief Aborts an ongoing MOT checkpoint.
     * @return Boolean value denoting success or failure.
     */
    bool Abort();

    /**
     * @brief Sets transaction commit begin checkpoint state.
     * @param txn Transaction's TxnManger pointer.
     */
    void BeginCommit(TxnManager* txn);

    /**
     * @brief Sets transaction commit completed checkpoint state.
     * @param txn Transaction's TxnManger pointer.
     */
    void EndCommit(TxnManager* txn);

    /**
     * @brief Pre-allocates stable row according to the checkpoint state.
     *        This is done at the end of occ validation phase to prevent
     *        failures during actual commit.
     * @param txn Transaction's TxnManger pointer.
     * @return Boolean value denoting success or failure.
     */
    bool PreAllocStableRow(TxnManager* txnMan, Row* origRow, AccessType type);

    /**
     * @brief Cleans up and frees all the pre-allocated stable rows.
     * @param txn Transaction's TxnManger pointer.
     */
    void FreePreAllocStableRows(TxnManager* txn);

    /**
     * @brief Sets stable row according to the checkpoint state.
     * @param txn Transaction's TxnManger pointer.
     */
    void ApplyWrite(TxnManager* txnMan, Row* origRow, AccessType type);

    /**
     * @brief Checkpoint task completion callback
     * @param checkpointId The checkpoint's id.
     * @param table The table's pointer.
     * @param numSegs number of segments written.
     * @param success Indicates a success or a failure.
     */
    virtual void TaskDone(Table* table, uint32_t numSegs, bool success);

    virtual bool ShouldStop() const
    {
        return m_stopFlag;
    }

    /**
     * @brief Checkpoint task error callback
     * @param errCode The error's code.
     * @param errMsg The error's message.
     * @param optionalMsg An optional message to display.
     */
    virtual void OnError(int errCode, const char* errMsg, const char* optionalMsg = nullptr);

    /**
     * @brief Deletes 'old' checkpoint directories
     * @param the current checkpoint id which should not be deleted
     */
    void RemoveOldCheckpoints(uint64_t curCheckcpointId);

    int GetErrorCode() const
    {
        return m_checkpointError;
    }

    const char* GetErrorString() const
    {
        return m_errorMessage.c_str();
    }

    /**
     * @brief Creates a new and unique checkpoint id
     * @param id The new checkpoint id
     * @return Boolean value denoting success or failure.
     */
    static bool CreateCheckpointId(uint64_t& id);

    /**
     * @brief Creates a directory for the checkpoint
     * @param dir The directory path to create
     * @return Boolean value denoting success or failure.
     */
    static bool CreateCheckpointDir(std::string& dir);

    uint64_t GetId()
    {
        return m_id;
    }

    void SetId(uint64_t id)
    {
        m_id = id;
    }

    uint64_t GetLastReplayLsn()
    {
        return m_lastReplayLsn;
    }

    void FetchRdLock()
    {
        (void)pthread_rwlock_rdlock(&m_fetchLock);
    }

    void FetchRdUnlock()
    {
        (void)pthread_rwlock_unlock(&m_fetchLock);
    }

    bool GetCheckpointDirName(std::string& dirName);

    bool GetCheckpointWorkingDir(std::string& workingDir);

    CheckpointManager(const CheckpointManager& orig) = delete;

    CheckpointManager& operator=(const CheckpointManager&) = delete;

    struct MapFileEntry {
        uint32_t m_tableId;
        uint32_t m_maxSegId;
    };

private:
    RwLock m_lock;

    RedoLogHandler* m_redoLogHandler;

    volatile bool m_cntBit;

    volatile CheckpointPhase m_phase;

    // NA 'bit' handling
    std::atomic_bool m_availableBit;

    // Counts the number of table ids that we are checkpointing.
    // When this reaches 0, the checkpoint is complete;
    std::atomic<uint32_t> m_numCpTasks;

    // Holds tables to checkpoint to be passed to the checkpoint threads
    std::list<Table*> m_tasksList;

    // Holds finished (checkpointed) tables that can be released by the main thread
    std::list<Table*> m_finishedTasks;

    // the checkpoint workers pool
    CheckpointWorkerPool* m_checkpointers = nullptr;

    // Number of threads to run
    int m_numThreads;

    // mutex for safeguarding mapfile and tasks queues access
    std::mutex m_tasksMutex;

    std::list<MapFileEntry*> m_mapfileInfo;

    // Checkpoint segments size threshold
    uint32_t m_cpSegThreshold;

    // Signal working threads to exit
    volatile bool m_stopFlag;

    // Indicates checkpoint has ended
    volatile bool m_checkpointEnded;

    // Checkpoint error code
    int m_checkpointError;

    // The error message
    std::string m_errorMessage;

    // Spinlock for error reporting
    spin_lock m_errorReportLock;

    bool m_errorSet;

    // Zigzag counter for counting transactions started in prev phase and current phase
    // use the phase number to zigzag between them
    std::atomic<uint32_t> m_counters[2];

    // Envelope's checkpoint lsn
    uint64_t m_lsn;

    // Last Valid (completed) Checkpoint ID
    uint64_t m_id;

    // Current (in-progress) Checkpoint ID
    uint64_t m_inProgressId;

    // last seen recovery lsn
    uint64_t m_lastReplayLsn;

    // The most recent segment's lsn that was inserted to the in-process map
    uint64_t m_inProcessTxnsLsn;

    // Current number of serialized in-process entries
    uint64_t m_numSerializedEntries;

    // this lock guards gs_ctl checkpoint fetching
    pthread_rwlock_t m_fetchLock;

    CheckpointPhase GetPhase() const
    {
        return m_phase;
    }

    void SetPhase(CheckpointPhase p)
    {
        m_phase = p;
    }

    void SetLsn(uint64_t lsn)
    {
        m_lsn = lsn;
    }

    uint64_t GetLsn()
    {
        return m_lsn;
    }

    void SetLastReplayLsn(uint64_t lsn)
    {
        m_lastReplayLsn = lsn;
    }

    static const char* PhaseToString(CheckpointPhase phase);

    inline void SwapAvailableAndNotAvailable()
    {
        m_availableBit = !m_availableBit;
    }

    /**
     * @brief Creates the checkpoint directory.
     * @return Boolean value denoting success or failure.
     */
    bool CreateCheckpointDir();

    /**
     * @brief Performs checkpoint completion tasks:
     * updates control file, creates the map file
     * and 2pc recovery file
     */
    void CompleteCheckpoint();

    /**
     * @brief Performs the checkpoint's Capture phase
     */
    void Capture();

    /**
     * @brief Creates the tasks queue: a list of all the tables
     * that should be included in the checkpoint
     */
    void FillTasksQueue();

    /**
     * @brief Unlocks tables and clear the tables' list
     * @param tables Tables list to clear
     */
    void UnlockAndClearTables(std::list<Table*>& tables);

    /**
     * @brief Destroys all the checkpoint threads
     */
    void DestroyCheckpointers();

    /**
     * @brief Creates the checkpoint threads
     */
    void CreateCheckpointers();

    /**
     * @brief Ensures that before moving to a new state, all transactions that
     * started committing on previous phase will complete. This is needed
     * in order to ensure that previous counter is zeroed before moving
     * to a new checkpoint phase. This method should only be called by the
     * checkpoint main thread (and not by transaction threads) and only for
     * transactions which are not auto-committed
     */
    void WaitPrevPhaseCommittedTxnComplete();

    void MoveToNextPhase();

    inline bool IsAutoCompletePhase() const
    {
        if (m_phase == PREPARE)
            return true;
        else
            return false;
    }

    /**
     * @brief Creates the checkpoint's map file - where all
     * the metadata is stored in
     * @return Boolean value denoting success or failure.
     */
    bool CreateCheckpointMap();

    /**
     * @brief Saves the in-process transaction data for 2pc recovery
     * purposes during the checkpoint.
     * @return Boolean value denoting success or failure.
     */
    bool CreateTpcRecoveryFile();

    /**
     * @brief Creates a file that indicates checkpoint completion.
     * @return Boolean value denoting success or failure.
     */
    bool CreateEndFile();

    /**
     * @brief Serializes inProcess transactions to disk
     * @return RC value denoting the status of the operation.
     */
    RC SerializeInProcessTxns(int fd);

    void ResetFlags();

    /**
     * @brief Deletes a checkpoint directory
     * @param checkpointId The checkpoint id to be deleted.
     */
    void RemoveCheckpointDir(uint64_t checkpointId);
};
}  // namespace MOT

#endif /* CHECKPOINT_MANAGER_H */
