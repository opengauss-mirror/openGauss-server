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
 * irecovery_manager.h
 *    Recovery manager interfaces.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/recovery/irecovery_manager.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef IRECOVERY_MANAGER_H
#define IRECOVERY_MANAGER_H

#include "global.h"
#include "surrogate_state.h"

namespace MOT {

typedef TxnCommitStatus (*CommitLogStatusCallback)(uint64_t);
class RedoLogTransactionPlayer;

class IRecoveryManager {
public:
    // destructor
    virtual ~IRecoveryManager()
    {}

    virtual bool Initialize() = 0;

    virtual void SetCommitLogCallback(CommitLogStatusCallback clogCallback) = 0;

    /**
     * @brief Starts the recovery process which currently consists of
     * checkpoint recovery.
     * @return Boolean value denoting success or failure.
     */
    virtual bool RecoverDbStart() = 0;

    /**
     * @brief Performs the post recovery operations: apply the in-process
     * transactions and surrogate array, set the max csn and prints the stats
     * @return Boolean value denoting success or failure.
     */
    virtual bool RecoverDbEnd() = 0;

    /**
     * @brief attempts to insert a data chunk into the in-process
     * transactions map and operate on it. Checks that the redo lsn is after the
     * checkpoint snapshot lsn taken. Redo records that are prior snapshot are
     * ignored.
     * @return Boolean value denoting success or failure.
     */
    virtual bool ApplyRedoLog(uint64_t redoLsn, char* data, size_t len) = 0;

    /**
     * @brief performs a commit on an in-process transaction.
     * @param extTxnId the transaction id.
     * @return Boolean value denoting success or failure to commit.
     */
    virtual bool CommitTransaction(uint64_t extTxnId) = 0;

    /**
     * @brief Sets the last replayed LSN value.
     * @param uint64_t the lsn value.
     */
    virtual void SetLastReplayLsn(uint64_t lastReplayLsn) = 0;

    /**
     * @brief Returns the last replayed LSN value.
     * @return uint64 the lsn value.
     */
    virtual uint64_t GetLastReplayLsn() const = 0;

    /**
     * @brief Sets the flag to indicate an error in recovery.
     */
    virtual void SetError() = 0;

    /**
     * @brief Checks for an error in recovery.
     * @return Boolean value denoting true in case of an error.
     */
    virtual bool IsErrorSet() const = 0;

    /**
     * @brief Adds a surrogate array into the surrogate global list.
     * @param SurrogateState the surrogate state object from which to obtain the array.
     */
    virtual void AddSurrogateArrayToList(SurrogateState& surrogate) = 0;

    /**
     * @brief Sets the max recovery csn.
     * @param uint64 the csn value to set (if greater than the current).
     */
    virtual void SetMaxCsn(uint64_t csn) = 0;

    /**
     * @brief Returns the number of currently running recovery threads.
     * @return uint32 the number of threads.
     */
    virtual uint32_t GetNumRecoveryThreads() const = 0;

    /**
     * @brief Instructs the manager to flush its thread queues (if any) in case a checkpoint needs
     * to be performed.
     */
    virtual void Flush() = 0;

    /**
     * @brief Returns whether an operation should be retried in case of failure.
     * @param RedoLogTransactionPlayer* the player currently need to be retried.
     * @return bool whether to retry the operation or not.
     */
    virtual bool ShouldRetryOp(RedoLogTransactionPlayer* player) = 0;

    /**
     * @brief Releases the player after the transaction is committed.
     * @param RedoLogTransactionPlayer* the player to be released.
     */
    virtual void ReleasePlayer(RedoLogTransactionPlayer* player) = 0;

    /**
     * @brief Will be called when a checkpoint begins. used to serialize pending transactions, etc.
     * @param fd the file descriptor to use.
     * @return uint64_t the num of transactions that were processed.
     */
    virtual uint64_t SerializePendingRecoveryData(int fd) = 0;

    /**
     * @brief Will be called during checkpoint recovery to restore the state of the pending transactions.
     * @param fd the file descriptor to use.
     * @return Boolean value denoting success or failure.
     */
    virtual bool DeserializePendingRecoveryData(int fd) = 0;

    /**
     * @brief locks the recovery manager in case of transaction serialization needs to be performed.
     *
     */
    virtual void Lock() = 0;

    /**
     * @brief unlocks the recovery manager when serialization is done.
     *
     */
    virtual void Unlock() = 0;

    /* Stats Collector API */
    virtual void LogInsert(uint64_t) = 0;
    virtual void LogUpdate(uint64_t) = 0;
    virtual void LogDelete(uint64_t) = 0;
    virtual void LogCommit() = 0;

protected:
    // constructor
    IRecoveryManager()
    {}
};
}  // namespace MOT

#endif /* IRECOVERY_MANAGER_H */
