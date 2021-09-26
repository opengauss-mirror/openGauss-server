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
 * commit_group.h
 *    Implements a group object that takes part in group commit.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/transaction_logger/group_synchronous_redo_log/commit_group.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef COMMIT_GROUP_H
#define COMMIT_GROUP_H

#include <atomic>
#include <condition_variable>
#include <chrono>
#include "txn.h"
#include "rw_lock.h"

const int MAX_GROUP_SIZE = 1000;  // For static allocation of group array (not taken from configuration)

namespace MOT {
class GroupSyncRedoLogHandler;

/**
 * @class CommitGroup
 * @brief Describes a group object that takes part in group commit
 */
class CommitGroup {
public:
    CommitGroup(RedoLogBuffer* buffer, GroupSyncRedoLogHandler* handler, const uint8_t id);
    CommitGroup(const CommitGroup& orig) = delete;
    CommitGroup& operator=(const CommitGroup& orig) = delete;
    virtual ~CommitGroup();

    /**
     * @brief Attempts to add this transaction to the group
     * @param buffer The buffer to write
     * @return Int value that equals to the position in the group
     * or -1 in case the group could not be joined
     */
    int AddToGroup(RedoLogBuffer* buffer);
    inline uint32_t GetGroupSize()
    {
        return m_groupSize;
    }

    /**
     * @brief Flushes the group to the logger
     */
    void LogGroup();

    /**
     * @brief On a leader, waits for the right condition to commit
     * as a member waits for the group to be comitted
     * @param isLeader is the current caller a leader
     * @param groupRef a reference to the group
     */
    void Commit(bool isLeader, std::shared_ptr<CommitGroup> groupRef);

    inline bool IsCommitted()
    {
        return m_commited;
    }

private:
    const uint8_t m_handlerId;
    GroupSyncRedoLogHandler* m_handler;
    uint64_t m_maxGroupCommitSize;
    std::chrono::microseconds m_groupTimeout;
    volatile std::atomic<uint32_t> m_groupSize;
    volatile std::atomic<uint32_t> m_numWaiters;
    volatile bool m_commited;
    volatile bool m_closed;
    RedoLogBuffer* m_groupData[MAX_GROUP_SIZE];
    std::condition_variable m_groupCommitedCV;
    std::mutex m_commitMutex;
    std::condition_variable m_fullGroupCV;
    std::mutex m_fullGroupMutex;
    RwLock m_rwlock;

    /**
     * @brief Leader wait: on timeoot or max group size has reached.
     * @param groupRef A reference to the group
     */
    void WaitLeader(std::shared_ptr<CommitGroup> groupRef);

    /**
     * @brief Member wait: when the leader committed.
     */
    void WaitMember();

    /**
     * @brief Performs the actual logging on the group.
     */
    void CommitInternal();
};
} /* namespace MOT */

#endif /* COMMIT_GROUP_H */
