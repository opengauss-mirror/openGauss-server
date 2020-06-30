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
 *    src/gausskernel/storage/mot/core/src/system/checkpoint/checkpoint_manager.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef CHECKPOINT_MANAGER_H
#define CHECKPOINT_MANAGER_H

#include <atomic>
#include <iostream>
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

    void SetValidation(bool val)
    {
        m_checkpointValidation = val;
    }

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
     * @brief Sets transaction begin checkpoint state.
     * @param txn Transaction's TxnManger pointer.
     */
    void BeginTransaction(TxnManager* txn);

    /**
     * @brief Sets transaction abort checkpoint state.
     * @param txn Transaction's TxnManger pointer.
     */
    void AbortTransaction(TxnManager* txn);

    /**
     * @brief sets transaction commit checkpoint state.
     * @param txn Transaction's TxnManger pointer.
     */
    void CommitTransaction(TxnManager* txn, int writeSetSize);

    /**
     * @brief Sets transaction completed checkpoint state.
     * @param txn Transaction's TxnManger pointer.
     */
    void TransactionCompleted(TxnManager* txn);

    /**
     * @brief Sets stable row according to the checkpoint state.
     * @param txn Transaction's TxnManger pointer.
     * @return Boolean value denoting success or failure.
     */
    bool ApplyWrite(TxnManager* txnMan, Row* origRow, AccessType type);

    /**
     * @brief Checkpoint task completion callback
     * @param checkpointId The checkpoint's id.
     * @param tableId The table's id.
     * @param numSegs number of segments written.
     * @param success Indicates a success or a failure.
     */
    virtual void TaskDone(uint32_t tableId, uint32_t numSegs, bool success);

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

    void SetLastReplayLsn(uint64_t lsn)
    {
        m_lastReplayLsn = lsn;
    }

    uint64_t GetLastReplayLsn()
    {
        return m_lastReplayLsn;
    }

    void FetchRdLock()
    {
        m_fetchLock.RdLock();
    }

    void FetchRdUnlock()
    {
        m_fetchLock.RdUnlock();
    }

    bool GetCheckpointDirName(std::string& dirName);

    bool GetCheckpointWorkingDir(std::string& workingDir);

    CheckpointManager(const CheckpointManager& orig) = delete;

    CheckpointManager& operator=(const CheckpointManager&) = delete;

    struct MapFileEntry {
        uint32_t m_id;
        uint32_t m_numSegs;
    };

private:
    RwLock m_lock;

    RedoLogHandler* m_redoLogHandler;

    volatile bool m_cntBit;

    volatile CheckpointPhase m_phase;

    // NA 'bit' handling
    volatile std::atomic_bool m_availableBit;

    // Counts the number of table ids that we are checkpointing.
    // When this reaches 0, the checkpoint is complete;
    std::atomic<uint32_t> m_numCpTasks;

    // Holds table IDs to checkpoint
    std::list<uint32_t> m_tasksList;

    // the checkpoint workers pool
    CheckpointWorkerPool* m_checkpointers = nullptr;

    // Number of threads to run
    int m_numThreads;

    // Enable checkpoint validation - checkbits
    bool m_checkpointValidation;

    // Checkpoint map file information
    std::mutex m_mapfileMutex;

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

    // Current Checkpoint's ID
    uint64_t m_id;

    // last seen recovery lsn
    uint64_t m_lastReplayLsn;

    bool m_emptyCheckpoint;

    // this lock guards gs_ctl checkpoint fetching
    RwLock m_fetchLock;

    void SetId(uint64_t id)
    {
        m_id = id;
    }

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

    static const char* PhaseToString(CheckpointPhase phase);

    void SwapAvailableAndNotAvailable()
    {
        m_availableBit = !m_availableBit;
    }

    /**
     * @brief Creates an 'empty' checkpoint Synchronously
     * @return Boolean value denoting success or failure.
     */
    bool CreateEmptyCheckpoint();

    /**
     * @brief Performs a checkpoint completion tasks:
     * updates control file, creates the map file
     * and 2pc recovery file
     * @param checkpointId The checkpoint's id.
     */
    void CompleteCheckpoint(uint64_t checkpointId);

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
        if (m_phase == PREPARE || m_phase == RESOLVE)
            return true;
        else
            return false;
    }

    /**
     * @brief A utility function to validate the checkpoint.
     * should not be enabled by default since it can
     * have an impact on checkpoint's completion time
     */
    void Checkbits();

    /**
     * @brief Creates the checkpoint's map file - where all
     * the metadata is stored in
     * @param id The new checkpoint id
     * @return Boolean value denoting success or failure.
     */
    bool CreateCheckpointMap(uint64_t checkpointId);

    /**
     * @brief Saves the in-process transaction data for 2pc recovery
     * purposes during the checkpoint.
     * @param id The new checkpoint id
     * @return Boolean value denoting success or failure.
     */
    bool CreateTpcRecoveryFile(uint64_t checkpointId);

    /**
     * @brief Creates a file that indicates checkpoint completion.
     * @param id The checkoint id
     * @return Boolean value denoting success or failure.
     */
    bool CreateEndFile(uint64_t checkpointId);

    void ResetFlags();

    /**
     * @brief Deletes a checkpoint directory
     * @param checkpointId The checkpoint id to be deleted.
     */
    void RemoveCheckpointDir(uint64_t checkpointId);
};
}  // namespace MOT

#endif /* CHECKPOINT_MANAGER_H */
