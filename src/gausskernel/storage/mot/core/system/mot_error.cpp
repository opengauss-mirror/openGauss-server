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
 * mot_error.cpp
 *    Utilities for error handling.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/mot_error.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "mot_error.h"
#include "utilities.h"

#include "postgres.h"
#include "knl/knl_thread.h"

#include <cstdarg>
#include <cstring>

namespace MOT {
DECLARE_LOGGER(Error, System)

// Attention: all definitions are found in knl_thread.h
#define lastErrorCode t_thrd.mot_cxt.last_error_code
#define lastErrorSeverity t_thrd.mot_cxt.last_error_severity
#define errorStack t_thrd.mot_cxt.error_stack
#define errorFrameCount t_thrd.mot_cxt.error_frame_count
typedef mot_error_frame ErrorFrame;

extern void SetLastError(int errorCode, int severity)
{
    lastErrorCode = errorCode;
    lastErrorSeverity = severity;
}

extern int GetLastError()
{
    int result = lastErrorCode;
    if (errorFrameCount > 0) {
        result = errorStack[errorFrameCount - 1].m_errorCode;
    }
    return result;
}

extern int SetLastErrorSeverity()
{
    int result = lastErrorSeverity;
    if (errorFrameCount > 0) {
        result = errorStack[errorFrameCount - 1].m_severity;
    }
    return result;
}

extern int GetRootError()
{
    int result = MOT_NO_ERROR;
    if (errorFrameCount > 0) {
        result = errorStack[0].m_errorCode;
    }
    return result;
}

extern int GetRootErrorSeverity()
{
    int result = MOT_SEVERITY_NORMAL;
    if (errorFrameCount > 0) {
        result = errorStack[0].m_severity;
    }
    return result;
}

extern const char* ErrorCodeToString(int errorCode)
{
    switch (errorCode) {
        case MOT_ERROR_OOM:
            return "Out of memory";
        case MOT_ERROR_INVALID_CFG:
            return "Invalid configuration";
        case MOT_ERROR_INVALID_ARG:
            return "Invalid argument passed to function";
        case MOT_ERROR_SYSTEM_FAILURE:
            return "System call failed";
        case MOT_ERROR_RESOURCE_LIMIT:
            return "Resource limit reached";
        case MOT_ERROR_INTERNAL:
            return "Internal logic error";
        case MOT_ERROR_RESOURCE_UNAVAILABLE:
            return "Resource unavailable";
        case MOT_ERROR_UNIQUE_VIOLATION:
            return "Unique violation";
        case MOT_ERROR_INVALID_MEMORY_SIZE:
            return "Invalid memory allocation size";
        case MOT_ERROR_INDEX_OUT_OF_RANGE:
            return "Index out of range";
        case MOT_ERROR_INVALID_STATE:
            return "Invalid state";
        case MOT_ERROR_CONCURRENT_MODIFICATION:
            return "Concurrent modification";
        case MOT_ERROR_STATEMENT_CANCELED:
            return "Statement canceled due to user request";
        default:
            return "Error code unknown";
    }
}

extern const char* SeverityToString(int severity)
{
    switch (severity) {
        case MOT_SEVERITY_NORMAL:
            return "Normal";
        case MOT_SEVERITY_WARN:
            return "Warning";
        case MOT_SEVERITY_ERROR:
            return "Error";
        case MOT_SEVERITY_FATAL:
            return "Fatal";

        default:
            return "Error severity unknown";
    }
}

extern void PushError(int errorCode, int severity, const char* file, int line, const char* function, const char* entity,
    const char* context, const char* format, ...)
{
    va_list args;
    va_start(args, format);

    PushErrorV(errorCode, severity, file, line, function, entity, context, format, args);

    va_end(args);
}

extern void PushErrorV(int errorCode, int severity, const char* file, int line, const char* function,
    const char* entity, const char* context, const char* format, va_list args)
{
    if (errorFrameCount < MOT_MAX_ERROR_FRAMES) {
        ErrorFrame* errorFrame = &errorStack[errorFrameCount];
        errorFrame->m_errorCode = errorCode;
        errorFrame->m_severity = severity;
        errorFrame->m_file = file;
        errorFrame->m_line = line;
        errorFrame->m_function = function;
        errorFrame->m_entity = entity;
        errorFrame->m_context = context;

        va_list args2;
        va_copy(args2, args);
        errno_t erc = vsnprintf_truncated_s(errorFrame->m_errorMessage, MOT_MAX_ERROR_MESSAGE, format, args2);
        securec_check_ss(erc, "\0", "\0");
        errorFrame->m_errorMessage[MOT_MAX_ERROR_MESSAGE - 1] = 0;
        ++errorFrameCount;
    }
}

extern void PushSystemError(int errorCode, int severity, const char* file, int line, const char* function,
    const char* entity, const char* context, const char* systemCall)
{
    errno_t erc;
    const int bufSize = 256;
    char errbuf[bufSize];
    if (errorFrameCount < MOT_MAX_ERROR_FRAMES) {
        ErrorFrame* errorFrame = &errorStack[errorFrameCount];
        errorFrame->m_errorCode = MOT_ERROR_SYSTEM_FAILURE;
        errorFrame->m_severity = severity;
        errorFrame->m_file = file;
        errorFrame->m_line = line;
        errorFrame->m_function = function;
        errorFrame->m_entity = entity;
        errorFrame->m_context = context;

#if (_POSIX_C_SOURCE >= 200112L || _XOPEN_SOURCE >= 600) && !_GNU_SOURCE
        strerror_r(errorCode, errbuf, bufSize);
        erc = snprintf_truncated_s(errorFrame->m_errorMessage,
            MOT_MAX_ERROR_MESSAGE,
            "System call %s() failed: %s (error code: %d)",
            systemCall,
            errbuf,
            errorCode);
#else
        erc = snprintf_truncated_s(errorFrame->m_errorMessage,
            MOT_MAX_ERROR_MESSAGE,
            "System call %s() failed: %s (error code: %d)",
            systemCall,
            strerror_r(errorCode, errbuf, bufSize),
            errorCode);
        securec_check_ss(erc, "\0", "\0");
#endif
        errorFrame->m_errorMessage[MOT_MAX_ERROR_MESSAGE - 1] = 0;
        ++errorFrameCount;
    }
}

static void PrintErrorFrame(ErrorFrame* errorFrame)
{
    (void)fprintf(stderr,
        "\nat %s() (%s:%d)\n"
        "\tEntity    : %s\n"
        "\tContext   : %s\n"
        "\tError     : %s\n"
        "\tError Code: %d (%s)\n"
        "\tSeverity  : %d (%s)\n",
        errorFrame->m_function,
        errorFrame->m_file,
        errorFrame->m_line,
        errorFrame->m_entity,
        errorFrame->m_context,
        errorFrame->m_errorMessage,
        errorFrame->m_errorCode,
        ErrorCodeToString(errorFrame->m_errorCode),
        errorFrame->m_severity,
        SeverityToString(errorFrame->m_severity));
}

extern void PrintErrorStack()
{
    for (int i = errorFrameCount - 1; i >= 0; --i) {
        PrintErrorFrame(&errorStack[i]);
    }
    (void)fprintf(stderr, "\n");
}

extern void ClearErrorStack()
{
    errorFrameCount = 0;
    lastErrorCode = MOT_NO_ERROR;
    lastErrorSeverity = MOT_SEVERITY_NORMAL;
}

extern RC ErrorToRC(int errorCode)
{
    switch (errorCode) {
        case MOT_NO_ERROR:
            return RC_OK;
        case MOT_ERROR_OOM:
            return RC_MEMORY_ALLOCATION_ERROR;
        case MOT_ERROR_UNIQUE_VIOLATION:
            return RC_UNIQUE_VIOLATION;
        case MOT_ERROR_CONCURRENT_MODIFICATION:
            return RC_CONCURRENT_MODIFICATION;
        case MOT_ERROR_STATEMENT_CANCELED:
            return RC_STATEMENT_CANCELED;
        case MOT_ERROR_INVALID_CFG:
        case MOT_ERROR_INVALID_ARG:
        case MOT_ERROR_SYSTEM_FAILURE:
        case MOT_ERROR_RESOURCE_LIMIT:
        case MOT_ERROR_INTERNAL:
        case MOT_ERROR_RESOURCE_UNAVAILABLE:
        case MOT_ERROR_INVALID_MEMORY_SIZE:
        case MOT_ERROR_INVALID_STATE:
        default:
            return RC_ERROR;
    }
}
}  // namespace MOT
