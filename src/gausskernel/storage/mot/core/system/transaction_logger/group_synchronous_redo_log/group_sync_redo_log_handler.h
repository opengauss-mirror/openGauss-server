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
 * group_sync_redo_log_handler.h
 *    Implements a group commit redo log.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/transaction_logger/
 *        group_synchronous_redo_log/group_sync_redo_log_handler.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef GROUP_SYNC_REDO_LOG_HANDLER_H
#define GROUP_SYNC_REDO_LOG_HANDLER_H

#include <atomic>
#include <chrono>
#include <memory>
#include "redo_log_handler.h"
#include "commit_group.h"

namespace MOT {
/**
 * @class GroupSyncRedoLogHandler
 * @brief implements a group commit redo log
 */
class GroupSyncRedoLogHandler : public RedoLogHandler {
public:
    explicit GroupSyncRedoLogHandler(const uint8_t socketId = 0);
    GroupSyncRedoLogHandler(const GroupSyncRedoLogHandler& orig) = delete;
    GroupSyncRedoLogHandler& operator=(const GroupSyncRedoLogHandler& orig) = delete;
    virtual ~GroupSyncRedoLogHandler();

    /**
     * @brief creates a new Buffer object
     * @return a Buffer
     */
    RedoLogBuffer* CreateBuffer();

    /**
     * @brief destroys a Buffer object
     * @param buffer pointer to be destroyed and de-allocated
     */
    void DestroyBuffer(RedoLogBuffer* buffer);

    /**
     * @brief tries to join a group and commit
     * @param buffer The buffer to write to the log.
     * @return The next buffer to write to, or null in case of failure.
     */
    virtual RedoLogBuffer* WriteToLog(RedoLogBuffer* buffer);

    void Flush();

    /**
     * @brief initializes group params
     * @param group the group to work on
     */
    void CloseGroup(std::shared_ptr<CommitGroup> group);

    inline uint64_t GetGroupCommitSize() const
    {
        return m_groupCommitSize;
    }
    inline std::chrono::microseconds& GetGroupTimeout()
    {
        return m_groupTimeout;
    }
    inline void SetId(uint8_t handlerId)
    {
        m_id = handlerId;
    }

private:
    uint8_t m_id;  // in segmented group handler, represents the socket id, otherwise 0
    std::shared_ptr<CommitGroup> m_currentGroup;
    uint64_t m_groupCommitSize;
    std::chrono::microseconds m_groupTimeout;
};
} /* namespace MOT */

#endif /* GROUP_SYNC_REDO_LOG_HANDLER_H */
