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
 * mot_error.h
 *    Utilities for error handling.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/mot_error.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef MM_ERROR_H
#define MM_ERROR_H

#include "mot_error_codes.h"
#include "global.h"
#include "mot_log.h"

#include <stdarg.h>

namespace MOT {
/**
 * @brief Sets the last error code for the current thread.
 * @param errorCode The error code to set.
 * @param severity The error severity.
 */
extern void SetLastError(int errorCode, int severity);

/**
 * @brief Retrieves the last error reported by the current thread.
 * @return The last error code reported by this thread.
 */
extern int GetLastError();

/**
 * @brief Retrieves the severity of the last error reported by the current thread.
 * @return The last error code severity reported by this thread.
 */
extern int GetLastErrorSeverity();

/**
 * @brief Retrieves the root error reported by the current thread. The root error is the first one
 * reported since the last call to @ref mm_clear_error_stack().
 * @return The root error code reported by this thread.
 */
extern int GetRootError();

/**
 * @brief Retrieves the root error severity reported by the current thread. The root error severity
 * is the first one reported since the last call to @ref mm_clear_error_stack().
 * @return The root error code reported by this thread.
 */
extern int GetRootErrorSeverity();

/**
 * @brief Converts error code to string.
 * @param errorCode The error code to translate.
 * @return The textual representation of the error code.
 */
extern const char* ErrorCodeToString(int errorCode);

/**
 * @brief Converts error severity to string.
 * @param severity The error severity to translate.
 * @return The textual representation of the error code.
 */
extern const char* SeverityToString(int severity);

/**
 * @brief Push error report on the error stack. Do not call this method directly, but rather use the
 * utility macro @ref MOT_PUSH_ERROR().
 * @param errorCode The error code to report.
 * @param severity The error severity.
 * @param file The reporting file.
 * @param line The reporting line.
 * @param function The reporting function.
 * @param entity The reporting entity.
 * @param context The report context.
 * @param format The error message format.
 * @param ... Additional arguments for the error message.
 */
extern void PushError(int errorCode, int severity, const char* file, int line, const char* function, const char* entity,
    const char* context, const char* format, ...);

/**
 * @brief Push error report on the error stack. Do not call this method directly, but rather use the
 * utility macro @ref MOT_PUSH_ERROR().
 * @param errorCode The error code to report.
 * @param severity The error severity.
 * @param file The reporting file.
 * @param line The reporting line.
 * @param function The reporting function.
 * @param entity The reporting entity.
 * @param context The report context.
 * @param format The error message format.
 * @param args Additional arguments for the error message.
 */
extern void PushErrorV(int errorCode, int severity, const char* file, int line, const char* function,
    const char* entity, const char* context, const char* format, va_list args);

/**
 * @brief Push error report on the error stack. Do not call this method directly, but rather use the
 * utility macro @ref MOT_PUSH_ERROR().
 * @param errorCode The error code to report.
 * @param severity The error severity.
 * @param file The reporting file.
 * @param line The reporting line.
 * @param function The reporting function.
 * @param entity The reporting entity.
 * @param context The report context.
 * @param systemCall The failing system call.
 */
extern void PushSystemError(int errorCode, int severity, const char* file, int line, const char* function,
    const char* entity, const char* context, const char* systemCall);

/**
 * @brief Prints the error stack.
 * @see mm_push_error().
 */
extern void PrintErrorStack();

/**
 * @brief Clears the error stack.
 * @see mm_push_error().
 */
extern void ClearErrorStack();

/**
 * @brief Maps an error code to a result code.
 * @param errorCode The error code to map.
 * @return The resulting result code.
 */
extern RC ErrorToRC(int errorCode);
}  // namespace MOT

/** @define Utility macro for retrieving the last error translated as a result code. */
#define MOT_GET_LAST_ERROR_RC() MOT::ErrorToRC(MOT::GetLastError())

/** @define Utility macro for retrieving the root error translated as a result code. */
#define MOT_GET_ROOT_ERROR_RC() MOT::ErrorToRC(MOT::GetRootError())

/** @define Utility macro for clearing the last error code for the current thread. */
#define MOT_CLEAR_LAST_ERROR() MOT::SetLastError(MOT_NO_ERROR, MOT_SEVERITY_NORMAL)

/** @define Utility macro for checking the last error code. */
#define MOT_IS_ERROR(errorCode) (MOT::GetLastError() == errorCode)

/** @define Utility macro for checking if the last error code was "out of memory". */
#define MOT_IS_OOM() (MOT::GetRootError() == MOT_ERROR_OOM)

/** @define Utility macro for checking if the current error state represents a severe error. */
#define MOT_IS_SEVERE() (MOT::GetRootErrorSeverity() >= MOT_SEVERITY_ERROR)

/** @define Utility macro for pushing error report on the error stack. */
#define MOT_PUSH_ERROR(errorCode, severity, context, format, ...) \
    MOT::PushError(                                               \
        errorCode, severity, __FILE__, __LINE__, __func__, _logger.m_qualifiedName, context, format, ##__VA_ARGS__)

/** @define Utility macro for pushing error report on the error stack. */
#define MOT_PUSH_ERROR_V(errorCode, severity, context, format, args) \
    MOT::PushErrorV(errorCode, severity, __FILE__, __LINE__, __func__, _logger.m_qualifiedName, context, format, args)

/** @define Utility macro for pushing system error report on the error stack. */
#define MOT_PUSH_SYSTEM_ERROR(systemErrorCode, severity, context, systemCall) \
    MOT::PushSystemError(                                                     \
        systemErrorCode, severity, __FILE__, __LINE__, __func__, _logger.m_qualifiedName, context, #systemCall)

/** @define Utility helper for printing the error stack (preceded by a formatted message). */
#define MOT_LOG_ERROR_STACK(format, ...)  \
    MOT_LOG_ERROR(format, ##__VA_ARGS__); \
    MOT::PrintErrorStack();

/** @define Utility helper for printing the error stack (preceded by a formatted message). */
#define MOT_LOG_PANIC_STACK(format, ...)  \
    MOT_LOG_PANIC(format, ##__VA_ARGS__); \
    MOT::PrintErrorStack();

/** @define Utility macro for pushing error report on the error stack. */
#define MOT_REPORT_ERROR(errorCode, context, format, ...) \
    MOT_LOG_ERROR(format, ##__VA_ARGS__);                 \
    MOT_PUSH_ERROR(errorCode, MOT_SEVERITY_ERROR, context, format, ##__VA_ARGS__)

/** @define Utility macro for pushing error report on the error stack. */
#define MOT_REPORT_PANIC(errorCode, context, format, ...) \
    MOT_LOG_PANIC(format, ##__VA_ARGS__);                 \
    MOT_PUSH_ERROR(errorCode, MOT_SEVERITY_FATAL, context, format, ##__VA_ARGS__)

/** @define Utility macro for pushing error report on the error stack. */
#define MOT_REPORT_ERROR_V(errorCode, context, format, args) \
    MOT_LOG_ERROR_V(format, args);                           \
    MOT_PUSH_ERROR_V(errorCode, MOT_SEVERITY_ERROR, context, format, args)

/** @define Utility macro for pushing error report on the error stack. */
#define MOT_REPORT_PANIC_V(errorCode, context, format, args) \
    MOT_LOG_PANIC_V(format, args);                           \
    MOT_PUSH_ERROR_V(errorCode, MOT_SEVERITY_FATAL, context, format, args)

/** @define Utility helper for reporting system call failure (with error code). */
#define MOT_REPORT_SYSTEM_ERROR_CODE(systemErrorCode, systemCall, context, format, ...) \
    MOT_LOG_SYSTEM_ERROR_CODE(systemErrorCode, systemCall, format, ##__VA_ARGS__);      \
    MOT_PUSH_SYSTEM_ERROR(systemErrorCode, MOT_SEVERITY_ERROR, context, systemCall);    \
    MOT_PUSH_ERROR(MOT_ERROR_SYSTEM_FAILURE, MOT_SEVERITY_ERROR, context, format, ##__VA_ARGS__);

/** @define Utility helper for reporting system call failure (with error code). */
#define MOT_REPORT_SYSTEM_PANIC_CODE(systemErrorCode, systemCall, context, format, ...) \
    MOT_LOG_SYSTEM_PANIC_CODE(systemErrorCode, systemCall, format, ##__VA_ARGS__);      \
    MOT_PUSH_SYSTEM_ERROR(systemErrorCode, MOT_SEVERITY_FATAL, context, systemCall);    \
    MOT_PUSH_ERROR(MOT_ERROR_SYSTEM_FAILURE, MOT_SEVERITY_FATAL, context, format, ##__VA_ARGS__);

/** @define Utility helper for reporting system call failure (with error code). */
#define MOT_REPORT_SYSTEM_ERROR(systemCall, context, format, ...) \
    MOT_REPORT_SYSTEM_ERROR_CODE(errno, systemCall, context, format, ##__VA_ARGS__)

/** @define Utility helper for reporting system call failure (with error code). */
#define MOT_REPORT_SYSTEM_PANIC(systemCall, context, format, ...) \
    MOT_REPORT_SYSTEM_PANIC_CODE(errno, systemCall, context, format, ##__VA_ARGS__)

#endif /* MM_ERROR_H */
