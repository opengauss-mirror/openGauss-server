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
 * group_synchronous_redo_log_handler.cpp
 *    Implements a group commit redo log.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/transaction_logger/
 *        group_synchronous_redo_log/group_synchronous_redo_log_handler.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "group_synchronous_redo_log_handler.h"
#include "utilities.h"
#include "mot_configuration.h"

namespace MOT {
DECLARE_LOGGER(GroupSyncRedoLogHandler, RedoLog)

GroupSyncRedoLogHandler::GroupSyncRedoLogHandler(const uint8_t socketId) : m_id(socketId), m_currentGroup(nullptr)
{
    m_groupCommitSize = GetGlobalConfiguration().m_groupCommitSize;
    m_groupTimeout = std::chrono::microseconds(GetGlobalConfiguration().m_groupCommitTimeoutUSec);
    MOT_LOG_INFO("Group commit initialized with group size %u and timeout %u micro-seconds",
        (unsigned)m_groupCommitSize,
        (unsigned)GetGlobalConfiguration().m_groupCommitTimeoutUSec);
}

GroupSyncRedoLogHandler::~GroupSyncRedoLogHandler()
{}

RedoLogBuffer* GroupSyncRedoLogHandler::CreateBuffer()
{
    RedoLogBuffer* buffer = new (std::nothrow) RedoLogBuffer();
    if (buffer != nullptr) {
        if (!buffer->Initialize()) {
            delete buffer;
            buffer = nullptr;
        }
    }
    return buffer;
}

void GroupSyncRedoLogHandler::DestroyBuffer(RedoLogBuffer* buffer)
{
    if (buffer != nullptr) {
        delete buffer;
    }
}

void GroupSyncRedoLogHandler::CloseGroup(std::shared_ptr<CommitGroup> group)
{
    std::shared_ptr<CommitGroup> nullGroup(nullptr);
    std::atomic_compare_exchange_weak(&m_currentGroup, &group, nullGroup);
    uint64_t curGroupCommitSize = GetGlobalConfiguration().m_groupCommitSize;
    std::chrono::microseconds curGroupCommitTimeoutUSec =
        std::chrono::microseconds(GetGlobalConfiguration().m_groupCommitTimeoutUSec);
    if (curGroupCommitSize != m_groupCommitSize) {
        m_groupCommitSize = curGroupCommitSize;
        MOT_LOG_DEBUG("closeGroup: group commit size changed to %lu", m_groupCommitSize);
    }
    if (curGroupCommitTimeoutUSec != m_groupTimeout) {
        m_groupTimeout = curGroupCommitTimeoutUSec;
        MOT_LOG_DEBUG(
            "closeGroup: group commit timeout changed to %lu", GetGlobalConfiguration().m_groupCommitTimeoutUSec);
    }
}

RedoLogBuffer* GroupSyncRedoLogHandler::WriteToLog(RedoLogBuffer* buffer)
{
    // CAS requires and actual shared_ptr
    std::shared_ptr<CommitGroup> nullGroup(nullptr);
    std::shared_ptr<CommitGroup> joinedGroup(nullptr);
    std::shared_ptr<CommitGroup> myGroup = make_shared<CommitGroup>(buffer, this, m_id);
    bool joined = false;
    bool leader = false;
    bool tail = false;
    bool retryLeadership = false;
    int groupIndex = 0;
    const int maxNullLoops = 10000;
    int curNullLoops = 0;
    while (!joined) {
        // need to either join or create a new group
        if (std::atomic_compare_exchange_weak(&m_currentGroup, &nullGroup, myGroup)) {
            // success means that I am the leader
            leader = true;
            joinedGroup = myGroup;
        } else if (m_groupCommitSize == 1) {
            // no point in trying to join a group of size 1 that already has a leader
            cpu_relax();
            continue;
        } else {
            // loop until currentGroup is set
            while (m_currentGroup == nullptr) {
                MOT_LOG_DEBUG("currentGroup is still null. looping");
                cpu_relax();
                if (++curNullLoops > maxNullLoops) {
                    curNullLoops = 0;
                    retryLeadership = true;
                    break;
                }
            }

            if (retryLeadership) {
                retryLeadership = false;
                continue;
            }

            // try to join the group
            joinedGroup = m_currentGroup;
            if (joinedGroup == nullptr) {
                MOT_LOG_DEBUG("JoinedGroup was changed to null, retrying");
                cpu_relax();
                continue;
            }
        }

        groupIndex = leader ? 1 : joinedGroup->AddToGroup(buffer);  // leader always has the 1st index
        if (groupIndex == -1) {
            // missed the train, the group is already commiting. Need to try again
            // remove the group from the current group if it is still there
            // The group will be closed anyhow by the group leader but for
            // optimization, maybe we can close the group earlier (HERE)
            MOT_LOG_DEBUG("I missed the commit. cannot join now. retry");
            CloseGroup(joinedGroup);
            joinedGroup = nullptr;
            cpu_relax();
            continue;
        }

        joined = true;
        if (groupIndex == (int)m_groupCommitSize) {
            // I am the last to join the group
            // The group will be closed anyhow by the group leader but for
            // optimization, maybe we can close the group earlier (HERE)
            MOT_LOG_DEBUG("I am the last on the group. closing group");
            tail = true;
            CloseGroup(joinedGroup);
        }
    }

    joinedGroup->Commit(leader, joinedGroup);
    return buffer;
}

void GroupSyncRedoLogHandler::Flush()
{
    std::shared_ptr<CommitGroup> flushGroup = m_currentGroup;
    if (flushGroup == nullptr) {
        return;
    }

    while (!flushGroup->IsCommitted()) {
        // spin until group is flushed
        cpu_relax();
    }
}
}  // namespace MOT
