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
 * commit_group.cpp
 *    Implements a group object that takes part in group commit.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/transaction_logger/group_synchronous_redo_log/commit_group.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <chrono>

#include "commit_group.h"
#include "utilities.h"
#include "group_sync_redo_log_handler.h"

namespace MOT {
DECLARE_LOGGER(CommitGroup, RedoLog);

CommitGroup::CommitGroup(RedoLogBuffer* buffer, GroupSyncRedoLogHandler* handler, const uint8_t id)
    : m_handlerId(id),
      m_handler(handler),
      m_maxGroupCommitSize(handler->GetGroupCommitSize()),
      m_groupTimeout(handler->GetGroupTimeout()),
      m_groupSize(1),  // The creator of the group is automatically its leader and thus inserts itself into it.
      m_numWaiters(0),
      m_commited(false),
      m_closed(false),
      m_groupCommitedCV(),
      m_commitMutex(),
      m_fullGroupCV(),
      m_fullGroupMutex(),
      m_rwlock()
{
    m_groupData[0] = buffer;
}

CommitGroup::~CommitGroup()
{}

int CommitGroup::AddToGroup(RedoLogBuffer* buffer)
{
    int val;
    m_rwlock.RdLock();
    if (m_closed) {
        val = -1;
    } else {
        val = (m_groupSize.fetch_add(1));
        if (val >= (int)m_maxGroupCommitSize) {
            // group is full... undo our add
            (void)m_groupSize.fetch_sub(1);
            m_rwlock.RdUnlock();
            return -1;
        }
        m_groupData[val] = buffer;
        val++;  // return my position in the group (need to inc by 1)
    }
    m_rwlock.RdUnlock();
    return val;
}

void CommitGroup::LogGroup()
{
    ILogger* logger = m_handler->GetLogger();
    (void)logger->AddToLog(m_groupData, m_groupSize);
    logger->FlushLog();
    m_commited = true;
    MOT_LOG_DEBUG("group committed. num entries: %d, handler id: %d", m_groupSize.load(), m_handlerId);
}

void CommitGroup::Commit(bool isLeader, std::shared_ptr<CommitGroup> groupRef)
{
    if (isLeader) {
        WaitLeader(groupRef);
        CommitInternal();
    } else {
        WaitMember();
    }
}

void CommitGroup::WaitLeader(std::shared_ptr<CommitGroup> groupRef)
{
    (void)m_numWaiters.fetch_add(1);
    std::unique_lock<std::mutex> lock(m_fullGroupMutex);
    (void)m_fullGroupCV.wait_for(lock, m_groupTimeout, [this] { return m_groupSize >= m_maxGroupCommitSize; });
    m_rwlock.WrLock();
    m_closed = true;
    m_handler->CloseGroup(groupRef);
    m_rwlock.WrUnlock();
}

void CommitGroup::WaitMember()
{
    uint32_t val = m_numWaiters.fetch_add(1);
    std::unique_lock<std::mutex> lock(m_commitMutex);
    if (val == m_maxGroupCommitSize - 1) {  // i was the last one, all group txns are waiting
        MOT_LOG_DEBUG("all in, releasing group to commit. group size: %d, "
                      "handler id: %d",
            m_groupSize.load(),
            m_handlerId);
        std::lock_guard<std::mutex> groupLock(m_fullGroupMutex);
        m_fullGroupCV.notify_all();
    }
    m_groupCommitedCV.wait(lock, [this] { return m_commited; });
}

void CommitGroup::CommitInternal()
{
    std::unique_lock<std::mutex> lock(m_commitMutex);
    LogGroup();
    lock.unlock();
    m_groupCommitedCV.notify_all();
}
}  // namespace MOT
