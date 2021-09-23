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
 * log_level.cpp
 *    Log printing level constants.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/utils/log_level.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "log_level.h"
#include "logger.h"
#include "mot_log.h"

#include <string.h>

namespace MOT {
DECLARE_LOGGER(LogLevel, Utilities)

extern LogLevel LogLevelFromString(const char* logLevelStr)
{
    LogLevel logLevel = LogLevel::LL_INFO;  // default value if parsing fails

    MOT_LOG_DEBUG("Parsing log level: %s", logLevelStr);
    if (strcmp(logLevelStr, "PANIC") == 0) {
        logLevel = LogLevel::LL_PANIC;
    } else if (strcmp(logLevelStr, "ERROR") == 0) {
        logLevel = LogLevel::LL_ERROR;
    } else if (strcmp(logLevelStr, "WARNING") == 0) {
        logLevel = LogLevel::LL_WARN;
    } else if (strcmp(logLevelStr, "INFO") == 0) {
        logLevel = LogLevel::LL_INFO;
    } else if (strcmp(logLevelStr, "TRACE") == 0) {
        logLevel = LogLevel::LL_TRACE;
    } else if (strcmp(logLevelStr, "DEBUG") == 0) {
        logLevel = LogLevel::LL_DEBUG;
    } else if (strcmp(logLevelStr, "DIAG1") == 0) {
        logLevel = LogLevel::LL_DIAG1;
    } else if (strcmp(logLevelStr, "DIAG2") == 0) {
        logLevel = LogLevel::LL_DIAG2;
    }

    MOT_LOG_DEBUG("Log level value is: %d", (int)logLevel);
    return logLevel;
}

extern const char* LogLevelToString(LogLevel logLevel)
{
    switch (logLevel) {
        case LogLevel::LL_PANIC:
            return "PANIC";

        case LogLevel::LL_ERROR:
            return "ERROR";

        case LogLevel::LL_WARN:
            return "WARNING";

        case LogLevel::LL_INFO:
            return "INFO";

        case LogLevel::LL_TRACE:
            return "TRACE";

        case LogLevel::LL_DEBUG:
            return "DEBUG";

        case LogLevel::LL_DIAG1:
            return "DIAG1";

        case LogLevel::LL_DIAG2:
            return "DIAG2";

        default:
            return "INVALID";
    }
}
}  // namespace MOT
