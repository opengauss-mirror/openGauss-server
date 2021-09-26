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
 * logger_factory.cpp
 *    The factory used to generate a logger based on configuration.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/transaction_logger/logger_factory.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "logger_factory.h"
#include "mot_configuration.h"
#include "utilities.h"

namespace MOT {
DECLARE_LOGGER(LoggerFactory, Logger)

ILogger* LoggerFactory::CreateLogger()
{
    MOTConfiguration& cfg = GetGlobalConfiguration();
    if (!cfg.m_enableRedoLog) {
        MOT_LOG_INFO("Logging is disabled)");
        return nullptr;
    } else {
        MOT_LOG_INFO("Using %s logger", LoggerTypeToString(cfg.m_loggerType));
        switch (cfg.m_loggerType) {
            case LoggerType::EXTERNAL_LOGGER:
                return nullptr;

            default:
                return nullptr;
        }
    }
}
}  // namespace MOT
