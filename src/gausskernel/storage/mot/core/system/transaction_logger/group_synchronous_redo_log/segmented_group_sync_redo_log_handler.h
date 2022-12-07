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
 * segmented_group_sync_redo_log_handler.h
 *    Implements a per-numa group commit redo log.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/transaction_logger/
 *        group_synchronous_redo_log/segmented_group_sync_redo_log_handler.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef SEGMENTED_GROUP_SYNC_REDO_LOG_HANDLER_H
#define SEGMENTED_GROUP_SYNC_REDO_LOG_HANDLER_H

#include "redo_log_handler.h"
#include "group_sync_redo_log_handler.h"

namespace MOT {
/**
 * @class SegmentedGroupSyncRedoLogHandler
 * @brief implements a per-numa group commit redo log
 */
class SegmentedGroupSyncRedoLogHandler : public RedoLogHandler {
public:
    SegmentedGroupSyncRedoLogHandler();
    SegmentedGroupSyncRedoLogHandler(const SegmentedGroupSyncRedoLogHandler& orig) = delete;
    SegmentedGroupSyncRedoLogHandler& operator=(const SegmentedGroupSyncRedoLogHandler& orig) = delete;
    virtual ~SegmentedGroupSyncRedoLogHandler();

    bool Init();

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
     * @brief Forwards and commits the transaction in the appropriate NUMA node.
     * @param buffer The buffer to write to the log.
     * @return The next buffer to write to, or null in case of failure.
     */
    virtual RedoLogBuffer* WriteToLog(RedoLogBuffer* buffer);
    virtual void Flush();
    virtual void SetLogger(ILogger* logger);

private:
    /** @var Number of NUMA nodes. */
    const unsigned int m_numaNodes;
    GroupSyncRedoLogHandler* m_redoLogHandlerArray;
};
}  // namespace MOT

#endif /* SEGMENTED_GROUP_SYNC_REDO_LOG_HANDLER_H */
