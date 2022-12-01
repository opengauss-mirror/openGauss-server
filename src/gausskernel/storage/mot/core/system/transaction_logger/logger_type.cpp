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
 * logger_type.cpp
 *    Constants for configuration and logger factory.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/transaction_logger/logger_type.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <cstring>
#include "logger_type.h"
#include "utilities.h"

namespace MOT {
DECLARE_LOGGER(LoggerType, Logger)

static const char* EXTERNAL_LOGGER_STR = "external";
static const char* INVALID_LOGGER_STR = "INVALID";

static const char* loggerNames[] = {EXTERNAL_LOGGER_STR, INVALID_LOGGER_STR};

extern LoggerType LoggerTypeFromString(const char* loggerTypeName)
{
    LoggerType loggerType = LoggerType::INVALID_LOGGER;

    if (strcmp(loggerTypeName, EXTERNAL_LOGGER_STR) == 0) {
        loggerType = LoggerType::EXTERNAL_LOGGER;
    } else {
        MOT_LOG_ERROR("Invalid logger type: %s", loggerTypeName);
    }

    return loggerType;
}

extern const char* LoggerTypeToString(const LoggerType& loggerType)
{
    if (loggerType < LoggerType::INVALID_LOGGER) {
        return loggerNames[(uint32_t)loggerType];
    } else {
        MOT_LOG_ERROR("Invalid logger type: %d", (int)loggerType);
        return INVALID_LOGGER_STR;
    }
}
}  // namespace MOT
