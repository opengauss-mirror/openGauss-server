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
 * redo_log_handler.cpp
 *    Abstract interface of a redo logger.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/transaction_logger/redo_log_handler.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "mot_configuration.h"
#include "global.h"
#include "utilities.h"
#include "redo_log_handler_type.h"
#include "redo_log_handler.h"
#include "logger_factory.h"
#include "logger_type.h"
#include "synchronous_redo_log_handler.h"
#include "segmented_group_synchronous_redo_log_handler.h"
#include "mot_error.h"

namespace MOT {
DECLARE_LOGGER(RedoLogHandler, RedoLog);

RedoLogHandler* RedoLogHandlerFactory::CreateRedoLogHandler()
{
    MOTConfiguration& cfg = GetGlobalConfiguration();
    if (!cfg.m_enableRedoLog) {
        MOT_LOG_INFO("Redo log disabled. No redo log handle.");
        return nullptr;
    }

    MOT_LOG_INFO("Using %s redo log handler", RedoLogHandlerTypeToString(cfg.m_redoLogHandlerType));

    RedoLogHandler* handler = nullptr;
    switch (cfg.m_redoLogHandlerType) {
        case RedoLogHandlerType::NONE:
            handler = nullptr;
            break;
        case RedoLogHandlerType::SYNC_REDO_LOG_HANDLER:
            handler = new (std::nothrow) SynchronousRedoLogHandler();
            break;
        case RedoLogHandlerType::SEGMENTED_GROUP_SYNC_REDO_LOG_HANDLER:
            handler = new (std::nothrow) SegmentedGroupSyncRedoLogHandler();
            break;
        default:
            MOT_REPORT_PANIC(MOT_ERROR_INTERNAL,
                "Redo Log Handler Initialization",
                "Invalid redo log handler type %d, aborting",
                (int)cfg.m_redoLogHandlerType);
            handler = nullptr;
            return handler;
    }

    if ((cfg.m_redoLogHandlerType != RedoLogHandlerType::NONE) && (handler == nullptr)) {
        MOT_REPORT_PANIC(MOT_ERROR_OOM,
            "Redo Log Handler Initialization",
            "Failed to allocate memory for redo log handler, aborting");
    }

    return handler;
}

RedoLogHandler::RedoLogHandler() : m_logger(LoggerFactory::CreateLogger()), m_redo_lock(), m_wakeupFunc(nullptr)
{}

RedoLogHandler::~RedoLogHandler()
{
    if (m_logger != nullptr) {
        m_logger->FlushLog();
        m_logger->CloseLog();
        if (GetGlobalConfiguration().m_loggerType != LoggerType::EXTERNAL_LOGGER)
            delete m_logger;
    }
}
}  // namespace MOT
