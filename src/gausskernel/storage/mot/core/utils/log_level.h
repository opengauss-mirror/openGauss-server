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
 * log_level.h
 *    Log printing level constants.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/utils/log_level.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef LOG_LEVEL_H
#define LOG_LEVEL_H

#include <cinttypes>
#include <cstdint>

namespace MOT {
/**
 * @brief Log printing level constants.
 */
enum class LogLevel : uint8_t {
    /** @var Panic log level. System is expected to crash or run into undefined behavior. */
    LL_PANIC,

    /** @var Error occurred. System can continue operating. */
    LL_ERROR,

    /** @var Warning log level. Something unexpected happened, but this is not an error. */
    LL_WARN,

    /** @var Information log level. Usually component startup/shutdown notices.   */
    LL_INFO,

    /** @var Trace log level. Used for debugging. Not so noisy. */
    LL_TRACE,

    /** @var Debug log level. Used for debugging. Could be quite noisy. */
    LL_DEBUG,

    /** @var Diagnostic log level. Used for debugging. Very noisy. */
    LL_DIAG1,

    /** @var Diagnostic log level. Used for debugging. Extremely noisy. */
    LL_DIAG2,

    /** @var Invalid log level. */
    LL_INVALID
};

/** @brief Converts string to log level constant. */
extern LogLevel LogLevelFromString(const char* logLevelStr);

/** @brief Converts log level constant to string. */
extern const char* LogLevelToString(LogLevel logLevel);

/** @brief Validates the log level string. */
extern bool ValidateLogLevel(const char* logLevelStr);
}  // namespace MOT

#endif /* LOG_LEVEL_H */
