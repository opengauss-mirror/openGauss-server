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
 * segmented_group_sync_redo_log_handler.cpp
 *    Implements a per-numa group commit redo log.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/transaction_logger/
 *        group_synchronous_redo_log/segmented_group_sync_redo_log_handler.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "segmented_group_sync_redo_log_handler.h"
#include "utilities.h"
#include "mot_configuration.h"
#include "session_context.h"

namespace MOT {
DECLARE_LOGGER(SegmentedGroupSyncRedoLogHandler, RedoLog);

SegmentedGroupSyncRedoLogHandler::SegmentedGroupSyncRedoLogHandler()
    : m_numaNodes(GetGlobalConfiguration().m_numaNodes), m_redoLogHandlerArray(nullptr)
{}

bool SegmentedGroupSyncRedoLogHandler::Init()
{
    m_redoLogHandlerArray = new (std::nothrow) GroupSyncRedoLogHandler[m_numaNodes];
    if (m_redoLogHandlerArray == nullptr) {
        MOT_LOG_ERROR("Error allocating redoLogHandler array");
        return false;
    } else {
        for (unsigned int i = 0; i < m_numaNodes; i++) {
            m_redoLogHandlerArray[i].SetId(i);
        }
        return true;
    }
}

SegmentedGroupSyncRedoLogHandler::~SegmentedGroupSyncRedoLogHandler()
{
    delete[] m_redoLogHandlerArray;
}

RedoLogBuffer* SegmentedGroupSyncRedoLogHandler::CreateBuffer()
{
    return m_redoLogHandlerArray[MOTCurrentNumaNodeId].CreateBuffer();
}

void SegmentedGroupSyncRedoLogHandler::DestroyBuffer(RedoLogBuffer* buffer)
{
    m_redoLogHandlerArray[MOTCurrentNumaNodeId].DestroyBuffer(buffer);
}

RedoLogBuffer* SegmentedGroupSyncRedoLogHandler::WriteToLog(RedoLogBuffer* buffer)
{
    return m_redoLogHandlerArray[MOTCurrentNumaNodeId].WriteToLog(buffer);
}

void SegmentedGroupSyncRedoLogHandler::SetLogger(ILogger* logger)
{
    RedoLogHandler::SetLogger(logger);
    for (unsigned int i = 0; i < m_numaNodes; i++)
        m_redoLogHandlerArray[i].SetLogger(logger);
}

void SegmentedGroupSyncRedoLogHandler::Flush()
{
    for (unsigned int i = 0; i < m_numaNodes; i++) {
        m_redoLogHandlerArray[i].Flush();
    }
}
}  // namespace MOT
