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
 * mot_log.cpp
 *    Implements a log sink.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/utils/mot_log.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "global.h"
#include "mot_log.h"
#include "utilities.h"
#include "session_context.h"
#include "mm_api.h"

#include <cstdio>
#include <sys/time.h>
#include <cmath>
#include <cstring>

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <unistd.h>
#include <sys/syscall.h>

#include "knl/knl_thread.h"

// we provide an 1K buffer for each thread (total of 4 MB when using 4K threads) - beyond that the line is truncated
// in order to allow longer line length, define LONG_LOG_LINE_MODE to be LONG_LOG_LINE_STRING_BUFFER
// Attention: the following two definitions were moved to knl_thread level, but still make sure they are properly
// defined
#ifndef MOT_LOG_BUF_SIZE_KB
#define MOT_LOG_BUF_SIZE_KB 1
#endif

#ifndef MOT_MAX_LOG_LINE_LENGTH
#define MOT_MAX_LOG_LINE_LENGTH (MOT_LOG_BUF_SIZE_KB * 1024 - 1)
#endif

// uncomment this define to enable very long log lines
#define ENABLE_LONG_LOG_LINE

// string buffer (slower but ensures lines from different thread do not intermix)
#define LONG_LOG_LINE_STRING_BUFFER 1

// printf (as fast as regular logging, but lines from different threads may intermix)
#define LONG_LOG_LINE_PRINTF 2

// select one mode for long log lines:
#define LONG_LOG_LINE_MODE LONG_LOG_LINE_PRINTF

#if defined(ENABLE_LONG_LOG_LINE) && (LONG_LOG_LINE_MODE == LONG_LOG_LINE_STRING_BUFFER)
#include "string_buffer.h"
#endif

namespace MOT {
/** @var Globally cached log level - exposed for performance reasons. */
LogLevel g_globalLogLevel = LogLevel::LL_INFO;

static void MOTWriteToNone(const char* line, size_t size, void* userData)
{}

static void MOTFormatToNone(const char* format, va_list args, void* userData)
{}

static void MOTFlushNone(void* userData)
{}

static void MOTWriteToFileSink(const char* line, size_t size, void* userData);
static void MOTFormatToFileSink(const char* format, va_list args, void* userData);
static void MOTFlushFileSink(void* userData);

static void MOTWriteToStdoutSink(const char* line, size_t size, void* userData)
{
    MOTWriteToFileSink(line, size, stdout);
}

static void MOTFormatToStdoutSink(const char* format, va_list args, void* userData)
{
    MOTFormatToFileSink(format, args, stdout);
}

static void MOTFlushStdoutSink(void* userData)
{
    MOTFlushFileSink(stdout);
}

static void MOTWriteToStderrSink(const char* line, size_t size, void* userData)
{
    MOTWriteToFileSink(line, size, stderr);
}

static void MOTFormatToStderrSink(const char* format, va_list args, void* userData)
{
    MOTFormatToFileSink(format, args, stderr);
}

static void MOTFlushStderrSink(void* userData)
{
    MOTFlushFileSink(stderr);
}

// we cannot put stdout and stderr in static initializer because they are not initialized yet
// so we use specialized functions instead
static MOTLogSink noneLogSink = {MOTWriteToNone, MOTFormatToNone, MOTFlushNone, nullptr};
static MOTLogSink stdoutLogSink = {MOTWriteToStdoutSink, MOTFormatToStdoutSink, MOTFlushStdoutSink, nullptr};
static MOTLogSink stderrLogSink = {MOTWriteToStderrSink, MOTFormatToStderrSink, MOTFlushStderrSink, nullptr};
static MOTLogSink fileLogSink = {MOTWriteToFileSink, MOTFormatToFileSink, MOTFlushFileSink, nullptr};

static MotLogSinkType globalLogSinkType = MOT_LOG_SINK_STDOUT;
static MOTLogSink* globalLogSink = &stdoutLogSink;

// use thread-local to avoid stack expansion
#define LOG_LINE t_thrd.mot_cxt.log_line
#define LOG_LINE_POS t_thrd.mot_cxt.log_line_pos
#define LOG_LINE_OVERFLOW t_thrd.mot_cxt.log_line_overflow
#define LOG_LINE_BUF t_thrd.mot_cxt.log_line_buf

// log level names
static const char* const strLogLevels[] = {
    "[PANIC]", "[ERROR]", "[WARNING]", "[INFO]", "[TRACE]", "[DEBUG]", "[DIAG1]", "[DIAG2]"};

static void MOTWriteToFileSink(const char* line, size_t size, void* userData)
{
    FILE* fileSink = (FILE*)userData;
    if (fileSink != nullptr) {
        (void)fwrite(line, 1, size, fileSink);
    }
}

static void MOTFormatToFileSink(const char* format, va_list args, void* userData)
{
    FILE* fileSink = (FILE*)userData;
    if (fileSink != nullptr) {
        (void)vfprintf(fileSink, format, args);
    }
}

static void MOTFlushFileSink(void* userData)
{
    FILE* fileSink = (FILE*)userData;
    if (fileSink != nullptr) {
        (void)fflush(fileSink);
    }
}

extern MotLogSinkType GetLogSinkType()
{
    return globalLogSinkType;
}

extern void SetLogSink(MotLogSinkType logSinkType, MOTLogSink* logSink /* = nullptr */)
{
    globalLogSinkType = logSinkType;
    switch (logSinkType) {
        case MOT_LOG_SINK_NONE:
            globalLogSink = &noneLogSink;
            break;

        case MOT_LOG_SINK_STDOUT:
            globalLogSink = &stdoutLogSink;
            break;

        case MOT_LOG_SINK_STDERR:
            globalLogSink = &stderrLogSink;
            break;

        case MOT_LOG_SINK_EXTERNAL:
            globalLogSink = logSink;
            break;

        default:
            (void)fprintf(stderr,
                "[MOT Logger Error] Request to configure log sink denied: Invalid logger sink type %d",
                (int)logSinkType);
            break;
    }
}

extern void SetFileLogSink(FILE* file)
{
    fileLogSink.m_userData = file;
    SetLogSink(MOT_LOG_SINK_EXTERNAL, &fileLogSink);
}

extern bool CheckLogLevel(LogLevel logLevel, LogLevel loggerLogLevel)
{
    return CheckLogLevelInline(logLevel, loggerLogLevel);
}

extern LogLevel SetGlobalLogLevel(LogLevel logLevel)
{
    LogLevel prevLogLevel = g_globalLogLevel;
    g_globalLogLevel = logLevel;
    return prevLogLevel;
}

extern LogLevel GetGlobalLogLevel()
{
    return g_globalLogLevel;
}

static inline void MOTWriteToLogSink(const char* line, size_t size = 0)
{
    if (size == 0) {
        size = strlen(line);
    }
    globalLogSink->m_writeFunc(line, size, globalLogSink->m_userData);
}

static inline void MOTFormatToLogSinkV(const char* format, va_list args)
{
    globalLogSink->m_formatFunc(format, args, globalLogSink->m_userData);
}

static inline void MOTFormatToLogSink(const char* format, ...)
{
    va_list args;
    va_start(args, format);

    globalLogSink->m_formatFunc(format, args, globalLogSink->m_userData);

    va_end(args);
}

static inline void MOTFlushLogSink()
{
    globalLogSink->m_flushFunc(globalLogSink->m_userData);
}

static inline void MOTBeginLogLine()
{
    LOG_LINE_OVERFLOW = false;
    LOG_LINE_POS = 0;
}

static inline void MOTAppendLogLineV(const char* format, va_list args)
{
    if (unlikely(LOG_LINE_OVERFLOW)) {
        // we have already previously overflowed
#if defined(ENABLE_LONG_LOG_LINE) && (LONG_LOG_LINE_MODE == LONG_LOG_LINE_PRINTF)
        MOTFormatToLogSinkV(format, args);
#elif defined(ENABLE_LONG_LOG_LINE) && (LONG_LOG_LINE_MODE == LONG_LOG_LINE_STRING_BUFFER)
        if (LOG_LINE_BUF != nullptr) {
            StringBufferAppendV(LOG_LINE_BUF, format, args);
        }
#endif
    } else {
        // we haven't overflowed yet, so try to append to log line
        int sizeCanWrite = MOT_MAX_LOG_LINE_LENGTH - LOG_LINE_POS;

        // must work on a copy in case buffer is too small
        va_list argsCopy;
        va_copy(argsCopy, args);

        // check possible overflow in advance
        int sizeToWrite = ::vsnprintf(nullptr, 0, format, argsCopy) + 1;
        va_end(argsCopy);
        if (likely(sizeToWrite < sizeCanWrite)) {  // no overflow expected
            va_copy(argsCopy, args);               // revalidate after call to vsnprintf()
            int sizeWritten = vsnprintf_s(LOG_LINE + LOG_LINE_POS, sizeCanWrite, sizeToWrite, format, argsCopy);
            securec_check_ss(sizeWritten, "\0", "\0");
            va_end(argsCopy);
            LOG_LINE_POS += sizeWritten;
        } else {
            LOG_LINE_OVERFLOW = true;
#if defined(ENABLE_LONG_LOG_LINE) && (LONG_LOG_LINE_MODE == LONG_LOG_LINE_PRINTF)
            MOTFormatToLogSink("%.*s", LOG_LINE_POS, LOG_LINE);
            MOTFormatToLogSinkV(format, args);
#elif defined(ENABLE_LONG_LOG_LINE) && (LONG_LOG_LINE_MODE == LONG_LOG_LINE_STRING_BUFFER)
            // log data is too long for log line, so we create an emergency buffer
            LOG_LINE_BUF = (StringBuffer*)malloc(sizeof(StringBuffer));
            if (LOG_LINE_BUF != nullptr) {
                StringBufferInit(
                    LOG_LINE_BUF, MOT_MAX_LOG_LINE_LENGTH, StringBuffer::GROWTH_FACTOR, StringBuffer::MULTIPLY);

                // copy what was formatted up until this call, and format on string buffer
                StringBufferAppendN(LOG_LINE_BUF, LOG_LINE, LOG_LINE_POS);
                StringBufferAppendV(LOG_LINE_BUF, format, args);
            }
#else
            // output truncated and line is full, so we put ellipsis at end of line
            errno_t erc = strcpy_s(LOG_LINE + MOT_MAX_LOG_LINE_LENGTH - 5, MOT_MAX_LOG_LINE_LENGTH, "...\n");
            securec_check(erc, "\0", "\0");
            LOG_LINE_POS = MOT_MAX_LOG_LINE_LENGTH;
#endif
        }
    }
}

static inline void MOTAppendLogLine(const char* format, ...)
{
    va_list args;
    va_start(args, format);

    MOTAppendLogLineV(format, args);

    va_end(args);
}

static inline void MOTEndLogLine()
{
    // append newline to formatted log line
    if (unlikely(LOG_LINE_OVERFLOW)) {
#if defined(ENABLE_LONG_LOG_LINE) && (LONG_LOG_LINE_MODE == LONG_LOG_LINE_PRINTF)
        MOTWriteToLogSink("\n");
#elif defined(ENABLE_LONG_LOG_LINE) && (LONG_LOG_LINE_MODE == LONG_LOG_LINE_STRING_BUFFER)
        // if we overflowed then we print the string buffer and cleanup
        if (LOG_LINE_BUF != nullptr) {
            StringBufferAppend(LOG_LINE_BUF, "\n");
            MOTWriteToLogSink(LOG_LINE_BUF->m_buffer, LOG_LINE_BUF->m_pos);
            StringBufferDestroy(LOG_LINE_BUF);
            free(LOG_LINE_BUF);
            LOG_LINE_BUF = nullptr;
        }
#else
        MOTWriteToLogSink(LOG_LINE, LOG_LINE_POS);
#endif
    } else {
        // otherwise, we print the simple log line
        if (likely(LOG_LINE_POS < MOT_MAX_LOG_LINE_LENGTH - 1)) {
            MOTAppendLogLine("\n");  // always have room for newline
        } else {                     // in case vsnprintf() overflowed
            LOG_LINE[MOT_MAX_LOG_LINE_LENGTH - 1] = '\n';
            LOG_LINE[MOT_MAX_LOG_LINE_LENGTH] = 0;
        }
        MOTWriteToLogSink(LOG_LINE, LOG_LINE_POS);
    }
    MOTFlushLogSink();
}

static inline const char* MOTFormatLogTime(char* buffer, size_t len)
{
    // get current time
    struct tm* tmInfo;
    struct timeval tv;
    struct tm localTime;
    (void)gettimeofday(&tv, nullptr);

    long int millisec = lrint(tv.tv_usec / 1000.0);  // Round to nearest millisec
    if (millisec >= 1000) {                          // Allow for rounding up to nearest second
        millisec -= 1000;
        tv.tv_sec++;
    }

    tmInfo = localtime_r(&tv.tv_sec, &localTime);

    // format time
    size_t offset = strftime(buffer, len, "%Y-%m-%d %H:%M:%S", tmInfo);
    errno_t erc = snprintf_s(buffer + offset, len - offset, (len - offset) - 1, ".%03d", (int)millisec);
    securec_check_ss(erc, "\0", "\0");
    return buffer;
}

static inline void MOTFormatLogPrefix(LogLevel logLevel, const char* loggerName)
{
    char timeBuffer[64];
    pid_t tid = syscall(__NR_gettid);
    if (tid == -1) {
        tid = (pid_t)pthread_self();
    }

    if (MOTCurrThreadId == INVALID_THREAD_ID) {
        MOTAppendLogLine("%s [MOT] <TID:%.5d/-----> <SID:-----/-----> %-10s %-20s ",
            MOTFormatLogTime(timeBuffer, sizeof(timeBuffer)),
            (int)tid,
            strLogLevels[static_cast<uint32_t>(logLevel)],
            loggerName);
    } else {
        SessionId sessionId = MOT_GET_CURRENT_SESSION_ID();
        if (sessionId != INVALID_SESSION_ID) {
            MOTAppendLogLine("%s [MOT] <TID:%.5d/%.5d> <SID:%.5u/%.5u> %-10s %-20s ",
                MOTFormatLogTime(timeBuffer, sizeof(timeBuffer)),
                (int)tid,
                (int)MOTCurrThreadId,
                (unsigned)sessionId,
                (unsigned)MOT_GET_CURRENT_CONNECTION_ID(),
                strLogLevels[static_cast<uint32_t>(logLevel)],
                loggerName);
        } else {
            MOTAppendLogLine("%s [MOT] <TID:%.5d/%.5d> <SID:-----/-----> %-10s %-20s ",
                MOTFormatLogTime(timeBuffer, sizeof(timeBuffer)),
                (int)tid,
                (int)MOTCurrThreadId,
                strLogLevels[static_cast<uint32_t>(logLevel)],
                loggerName);
        }
    }
}

extern void MOTLog(LogLevel logLevel, const char* loggerName, const char* format, ...)
{
    va_list args;
    va_start(args, format);

    MOTBeginLogLine();
    MOTFormatLogPrefix(logLevel, loggerName);
    MOTAppendLogLineV(format, args);
    MOTEndLogLine();

    va_end(args);
}

extern void MOTLogV(LogLevel logLevel, const char* loggerName, const char* format, va_list args)
{
    va_list argsCopy;
    va_copy(argsCopy, args);  // do not modify parameter - work on copy

    MOTBeginLogLine();
    MOTFormatLogPrefix(logLevel, loggerName);
    MOTAppendLogLineV(format, argsCopy);
    MOTEndLogLine();

    va_end(argsCopy);
}

extern void MOTLogBegin(LogLevel logLevel, const char* loggerName, const char* format, ...)
{
    va_list args;
    va_start(args, format);

    MOTBeginLogLine();
    MOTFormatLogPrefix(logLevel, loggerName);
    MOTAppendLogLineV(format, args);

    va_end(args);
}

extern void MOTLogAppend(const char* format, ...)
{
    va_list args;
    va_start(args, format);

    MOTAppendLogLineV(format, args);

    va_end(args);
}

extern void MOTLogAppendV(const char* format, va_list args)
{
    MOTAppendLogLineV(format, args);
}

extern void MOTLogEnd()
{
    MOTEndLogLine();
}

static void MOTPrintSystemErrorCodeV(
    LogLevel logLevel, int rc, const char* syscall, const char* loggerName, const char* format, va_list args)
{
    va_list argsCopy;
    va_copy(argsCopy, args);  // do not modify parameter - work on copy
    char errbuf[256];

    // print formatted user message
    MOTBeginLogLine();
    MOTFormatLogPrefix(logLevel, loggerName);
    if (format != nullptr) {
        MOTAppendLogLineV(format, argsCopy);

        // print system error message
#if (_POSIX_C_SOURCE >= 200112L || _XOPEN_SOURCE >= 600) && !_GNU_SOURCE
        strerror_r(err, buf, 256);
        MOTAppendLogLine(": %s (system call: %s(), error code: %d)", errbuf, syscall, rc);
#else
        MOTAppendLogLine(": %s (system call: %s(), error code: %d)", strerror_r(rc, errbuf, 256), syscall, rc);
#endif
    } else {
#if (_POSIX_C_SOURCE >= 200112L || _XOPEN_SOURCE >= 600) && !_GNU_SOURCE
        strerror_r(err, buf, 256);
        MOTAppendLogLine("System call %s() failed: %s (error code: %d)", syscall, errbuf, rc);
#else
        MOTAppendLogLine("System call %s() failed: %s (error code: %d)", syscall, strerror_r(rc, errbuf, 256), rc);
#endif
    }
    MOTEndLogLine();

    va_end(argsCopy);
}

extern void MOTLogSystemError(LogLevel logLevel, const char* syscall, const char* loggerName, const char* format, ...)
{
    int err = errno;

    // print formatted user message
    va_list args;
    va_start(args, format);
    MOTPrintSystemErrorCodeV(logLevel, err, syscall, loggerName, format, args);
    va_end(args);
}

extern void MOTLogSystemErrorCode(
    LogLevel logLevel, int rc, const char* syscall, const char* loggerName, const char* format, ...)
{
    va_list args;
    va_start(args, format);
    MOTPrintSystemErrorCodeV(logLevel, rc, syscall, loggerName, format, args);
    va_end(args);
}

extern void MOTPrintCallStack(LogLevel logLevel, const char* loggerName, int opts, const char* format, ...)
{
    va_list args;
    va_start(args, format);

    MOTPrintCallStackImplV(logLevel, loggerName, opts, format, args);

    va_end(args);
}
}  // namespace MOT
