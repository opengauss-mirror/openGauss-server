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

class IRecoveryManager {
public:
    // destructor
    virtual ~IRecoveryManager()
    {}

    virtual bool Initialize() = 0;

    virtual void SetCommitLogCallback(CommitLogStatusCallback clogCallback) = 0;

    /**
     * @brief Cleans up the recovery object.
     */
    virtual void CleanUp() = 0;

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
     * @brief attempts to insert a data chunk into the in-process
     * transactions map and operate on it
     * @return Boolean value denoting success or failure.
     */
    virtual bool ApplyLogSegmentFromData(char* data, size_t len, uint64_t replayLsn = 0) = 0;

    /**
     * @brief performs a commit on an in-process transaction,
     * @return Boolean value denoting success or failure to commit.
     */
    virtual bool CommitRecoveredTransaction(uint64_t externalTransactionId) = 0;

    virtual void SetLastReplayLsn(uint64_t lastReplayLsn) = 0;
    virtual uint64_t GetLastReplayLsn() const = 0;

    virtual bool IsErrorSet() const = 0;
    virtual void AddSurrogateArrayToList(SurrogateState& surrogate) = 0;
    virtual void SetCsn(uint64_t csn) = 0;

protected:
    // constructor
    IRecoveryManager()
    {}
};
}  // namespace MOT

#endif /* IRECOVERY_MANAGER_H */
