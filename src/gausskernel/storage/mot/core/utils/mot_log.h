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
 * mot_log.h
 *    Implements a log sink.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/utils/mot_log.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef MOT_LOG_H
#define MOT_LOG_H

#include "log_level.h"

#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>

/**
 * @define LOGGER_INDENT_SIZE The number bytes required to maintain indentation in multi-line log
 * messages (calculated according to the prefix format "%s [MOT] <TID:%.5d> %-10s %-20s").
 */
#define LOGGER_INDENT_SIZE 98

namespace MOT {

/** @enum Constants for log output sink. */
enum MotLogSinkType {
    /** @var Denotes logging is disabled. */
    MOT_LOG_SINK_NONE,

    /** @var Denotes logging is redirected to standard error stream. */
    MOT_LOG_SINK_STDERR,

    /** @var Denotes logging is redirected to standard output stream (the default). */
    MOT_LOG_SINK_STDOUT,

    /** @var Denotes logging is redirected to user-provided stream. */
    MOT_LOG_SINK_EXTERNAL
};

/** @typedef Callback for writing log text into log sink. */
typedef void (*MOTWriteToLogSinkFunc)(const char* line, size_t size, void* userData);

/** @typedef Callback for formatting test into log sink. */
typedef void (*MOTFormatToLogSinkFunc)(const char* format, va_list args, void* userData);

/** @typedef Callback for flushing log sink. */
typedef void (*MOTFlushLogSinkFunc)(void* userData);

/* @struct A log sink. */
struct MOTLogSink {
    /** @var The log sink write function. */
    MOTWriteToLogSinkFunc m_writeFunc;

    /** @var The log sink format function. */
    MOTFormatToLogSinkFunc m_formatFunc;

    /** @var The log sink flush function. */
    MOTFlushLogSinkFunc m_flushFunc;

    /** @var Any additional user data required to carry out log sink functions. */
    void* m_userData;
};

/** @brief Retrieves the currently configured log sink. */
extern MotLogSinkType GetLogSinkType();

/** @brief Configures the log sink. */
extern void SetLogSink(MotLogSinkType logSinkType, MOTLogSink* logSink = nullptr);

/** @brief Configures the log sink to use a file stream. */
extern void SetFileLogSink(FILE* fileLogSink);

/** @var The global log level. */
extern LogLevel g_globalLogLevel;

/**
 * @brief Configures the global log level.
 * @param logLevel The log level to set.
 * @return The previous global log level.
 */
extern LogLevel SetGlobalLogLevel(LogLevel logLevel);

/** @brief Retrieves the current global log level. */
extern LogLevel GetGlobalLogLevel();

/**
 * @brief Queries whether a log level qualifies for printing.
 * @details A log level qualifies for printing if it passes three qualification tests, the global log level, the log
 * level of the current thread , and the log level of the reporting component.
 * @param logLevel The log level to check.
 * @param componentLogLevel The reporting component.
 * @return True if the log level qualifies.
 */
extern bool CheckLogLevel(LogLevel logLevel, LogLevel componentLogLevel);

/**
 * @brief Queries whether a log level of a logger is sufficient for printing.
 * @param logLevel The log level limit.
 * @param loggerLogLevel The log level of the logger to check.
 * @return True if the log level of the logger is sufficient for printing.
 */
inline bool CheckLogLevelInline(LogLevel logLevel, LogLevel loggerLogLevel)
{
    return ((logLevel <= g_globalLogLevel) || (logLevel <= loggerLogLevel));
}

/**
 * @brief Print message to log
 * @param logLevel The log level.
 * @param loggerName The logging entity name.
 * @param format The formatted messages.
 * @param ... Additional arguments required by the formatted message.
 */
extern void MOTLog(LogLevel logLevel, const char* loggerName, const char* format, ...);

/**
 * @brief Print message to log
 * @param logLevel The log level.
 * @param loggerName The logging entity name.
 * @param format The formatted messages.
 * @param args Additional arguments required by the formatted message.
 */
extern void MOTLogV(LogLevel logLevel, const char* loggerName, const char* format, va_list args);

/**
 * @brief Begins a continued printing sequence. A full prefix and initial message formatting is
 * emitted to log, but without setting a trailing newline.
 * @param logLevel The report log level.
 * @param loggerName The name of the logger.
 * @param format The format message.
 * @param ... Any additional format message parameters.
 */
extern void MOTLogBegin(LogLevel logLevel, const char* loggerName, const char* format, ...);

/**
 * @brief Appends a message to a continued printing sequence.
 **/
extern void MOTLogAppend(const char* format, ...);

/**
 * @brief Terminates a continued printing sequence.
 */
extern void MOTLogEnd();

/**
 * @brief Utility function for printing last system error encountered by the current
 * thread.
 * The final message format is as follows:
 * `<FORMATTED_USER_MESSAGE>: <SYSTEM_MESSAGE> (error code: <LAST_ERROR_CODE>)`
 * @param logLevel The error message log level.
 * @param syscall The system call that failed.
 * @param loggerName The name of the logger.
 * @param format A user message to print.
 * @param ... Any additional extra arguments to the format message.
 */
extern void MOTLogSystemError(LogLevel logLevel, const char* syscall, const char* loggerName, const char* format, ...);

/**
 * @brief Utility function for printing last system error encountered by the current
 * thread.
 * The final message format is as follows:
 * `<FORMATTED_USER_MESSAGE>: <SYSTEM_MESSAGE> (error code: <PROVIDED_ERROR_CODE>)`
 * @param logLevel The error message log level.
 * @param rc The error code to print.
 * @param syscall The system call that failed.
 * @param loggerName The name of the logger.
 * @param format A user message to print.
 * @param ... Any additional extra arguments to the format message.
 */
extern void MOTLogSystemErrorCode(
    LogLevel logLevel, int rc, const char* syscall, const char* loggerName, const char* format, ...);

/**
 * @brief Utility function for printing call stack of the current thread.
 * @param logLevel The message log level.
 * @param loggerName The name of the logger.
 * @param opts Call stack printing options (see debug_utils.h for definitions).
 * @param format A user message to print.
 * @param ... Any additional extra arguments to the format message.
 */
extern void MOTPrintCallStack(LogLevel logLevel, const char* loggerName, int opts, const char* format, ...);
}  // namespace MOT

/** @define Utility macro for checking whether the log level of the current logger is sufficient for printing. */
#define MOT_CHECK_LOG_LEVEL(logLevel) MOT::CheckLogLevelInline(logLevel, MOT_LOGGER_LEVEL)

/** @define Begins a log printing in continuation. */
#define MOT_LOG_BEGIN(logLevel, format, ...)                                     \
    if (MOT_CHECK_LOG_LEVEL(logLevel)) {                                         \
        MOT::MOTLogBegin(logLevel, MOT_LOGGER_FULL_NAME, format, ##__VA_ARGS__); \
    }

/** @define Adds a log message to log printing in continuation. */
#define MOT_LOG_APPEND(logLevel, format, ...)     \
    if (MOT_CHECK_LOG_LEVEL(logLevel)) {          \
        MOT::MOTLogAppend(format, ##__VA_ARGS__); \
    }

/** @define Ends a log printing in continuation. */
#define MOT_LOG_END(logLevel)            \
    if (MOT_CHECK_LOG_LEVEL(logLevel)) { \
        MOT::MOTLogEnd();                \
    }

/** @define Utility log printing macro. */
#define MOT_LOG(logLevel, format, ...)                                      \
    if (MOT_CHECK_LOG_LEVEL(logLevel)) {                                    \
        MOT::MOTLog(logLevel, MOT_LOGGER_FULL_NAME, format, ##__VA_ARGS__); \
    }

/** @define Utility PANIC-level log printing macro. */
#define MOT_LOG_PANIC(format, ...) MOT_LOG(MOT::LogLevel::LL_PANIC, format, ##__VA_ARGS__)

/** @define Utility ERROR-level log printing macro. */
#define MOT_LOG_ERROR(format, ...) MOT_LOG(MOT::LogLevel::LL_ERROR, format, ##__VA_ARGS__)

/** @define Utility WARN-level log printing macro. */
#define MOT_LOG_WARN(format, ...) MOT_LOG(MOT::LogLevel::LL_WARN, format, ##__VA_ARGS__)

/** @define Utility INFO-level log printing macro. */
#define MOT_LOG_INFO(format, ...) MOT_LOG(MOT::LogLevel::LL_INFO, format, ##__VA_ARGS__)

/** @define Utility TRACE-level log printing macro. */
#define MOT_LOG_TRACE(format, ...) MOT_LOG(MOT::LogLevel::LL_TRACE, format, ##__VA_ARGS__)

/** @define Utility DEBUG-level log printing macro. */
#define MOT_LOG_DEBUG(format, ...) MOT_LOG(MOT::LogLevel::LL_DEBUG, format, ##__VA_ARGS__)

/** @define Utility DIAG1-level log printing macro. */
#define MOT_LOG_DIAG1(format, ...) MOT_LOG(MOT::LogLevel::LL_DIAG1, format, ##__VA_ARGS__)

/** @define Utility DIAG2-level log printing macro. */
#define MOT_LOG_DIAG2(format, ...) MOT_LOG(MOT::LogLevel::LL_DIAG2, format, ##__VA_ARGS__)

/** @define Utility log printing macro with variadic va_list argument. */
#define MOT_LOG_V(logLevel, format, args)                           \
    if (MOT_CHECK_LOG_LEVEL(logLevel)) {                            \
        MOT::MOTLogV(logLevel, MOT_LOGGER_FULL_NAME, format, args); \
    }

/** @define Utility PANIC-level log printing macro with variadic va_list argument. */
#define MOT_LOG_PANIC_V(format, args) MOT_LOG_V(MOT::LogLevel::LL_PANIC, format, args)

/** @define Utility ERROR-level log printing macro with variadic va_list argument. */
#define MOT_LOG_ERROR_V(format, args) MOT_LOG_V(MOT::LogLevel::LL_ERROR, format, args)

/** @define Utility WANR-level log printing macro with variadic va_list argument. */
#define MOT_LOG_WARN_V(format, args) MOT_LOG_V(MOT::LogLevel::LL_WARN, format, args)

/** @define Utility INFO-level log printing macro with variadic va_list argument. */
#define MOT_LOG_INFO_V(format, args) MOT_LOG_V(MOT::LogLevel::LL_INFO, format, args)

/** @define Utility TRACE-level log printing macro with variadic va_list argument. */
#define MOT_LOG_TRACE_V(format, args) MOT_LOG_V(MOT::LogLevel::LL_TRACE, format, args)

/** @define Utility DEBUG-level log printing macro with variadic va_list argument. */
#define MOT_LOG_DEBUG_V(format, args) MOT_LOG_V(MOT::LogLevel::LL_DEBUG, format, args)

/** @define Utility DIAG1-level log printing macro with variadic va_list argument. */
#define MOT_LOG_DIAG1_V(format, args) MOT_LOG_V(MOT::LogLevel::LL_DIAG1, format, args)

/** @define Utility DIAG2-level log printing macro with variadic va_list argument. */
#define MOT_LOG_DIAG2_V(format, args) MOT_LOG_V(MOT::LogLevel::LL_DIAG2, format, args)

/** @define Utility log printing macro for failed system call. */
#define MOT_LOG_SYSTEM(logLevel, syscall, format, ...) \
    MOT::MOTLogSystemError(logLevel, #syscall, MOT_LOGGER_FULL_NAME, format, ##__VA_ARGS__)

/** @define Utility log printing macro for failed system call with error code. */
#define MOT_LOG_SYSTEM_CODE(logLevel, rc, syscall, format, ...) \
    MOT::MOTLogSystemErrorCode(logLevel, rc, #syscall, MOT_LOGGER_FULL_NAME, format, ##__VA_ARGS__)

/** @define Utility PANIC-level log printing macro for failed system call. */
#define MOT_LOG_SYSTEM_PANIC(syscall, format, ...) \
    MOT_LOG_SYSTEM(MOT::LogLevel::LL_PANIC, syscall, format, ##__VA_ARGS__)

/** @define Utility PANIC-level log printing macro for failed system call with error code. */
#define MOT_LOG_SYSTEM_PANIC_CODE(rc, syscall, format, ...) \
    MOT_LOG_SYSTEM_CODE(MOT::LogLevel::LL_PANIC, rc, syscall, format, ##__VA_ARGS__)

/** @define Utility ERROR-level log printing macro for failed system call. */
#define MOT_LOG_SYSTEM_ERROR(syscall, format, ...) \
    MOT_LOG_SYSTEM(MOT::LogLevel::LL_ERROR, syscall, format, ##__VA_ARGS__)

/** @define Utility ERROR-level log printing macro for failed system call with error code. */
#define MOT_LOG_SYSTEM_ERROR_CODE(rc, syscall, format, ...) \
    MOT_LOG_SYSTEM_CODE(MOT::LogLevel::LL_ERROR, rc, syscall, format, ##__VA_ARGS__)

/** @define Utility WARN-level log printing macro for failed system call. */
#define MOT_LOG_SYSTEM_WARN(syscall, format, ...) MOT_LOG_SYSTEM(MOT::LogLevel::LL_WARN, syscall, format, ##__VA_ARGS__)

/** @define Utility WARN-level log printing macro for failed system call with error code. */
#define MOT_LOG_SYSTEM_WARN_CODE(rc, syscall, format, ...) \
    MOT_LOG_SYSTEM_CODE(MOT::LogLevel::LL_WARN, rc, syscall, format, ##__VA_ARGS__)

/** @define Utility macro for printing call stack of current thread. */
#define MOT_PRINT_CALL_STACK(logLevel, opts, format, ...)                                    \
    if (MOT_CHECK_LOG_LEVEL(logLevel)) {                                                     \
        MOT::MOTPrintCallStack(logLevel, MOT_LOGGER_FULL_NAME, opts, format, ##__VA_ARGS__); \
    }

#endif /* MOT_LOG_H */
