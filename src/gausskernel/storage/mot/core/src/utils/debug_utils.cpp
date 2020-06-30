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
 * debug_utils.cpp
 *    Debug utilities implementation.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/src/utils/debug_utils.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "utilities.h"
#include "string_buffer.h"
#include "mm_api.h"
#include "debug_utils.h"

#include <pthread.h>
#include <execinfo.h>
#include <unistd.h>

namespace MOT {
DECLARE_LOGGER(DebugUtils, Utilities)

// global abort lock - only one thread can dump analysis and then call abort()
static pthread_mutex_t abortLock;
static bool abortFlag = false;
static bool initialized = false;

extern bool InitializeDebugUtils()
{
    pthread_mutexattr_t attr;
    int rc = pthread_mutexattr_init(&attr);
    if (rc != 0) {
        MOT_LOG_SYSTEM_ERROR_CODE(
            rc, pthread_mutexattr_init, "Failed to initialize abort lock attributes object, aborting.");
        return false;
    }

    rc = pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
    if (rc != 0) {
        MOT_LOG_SYSTEM_ERROR_CODE(
            rc, pthread_mutexattr_init, "Failed to set abort lock attributes type to recursive, aborting.");
        return false;
    }

    rc = pthread_mutex_init(&abortLock, &attr);
    if (rc != 0) {
        MOT_LOG_SYSTEM_ERROR_CODE(rc, pthread_mutexattr_init, "Failed to create abort lock, aborting.");
        return false;
    }

    initialized = true;
    return true;
}

extern void DestroyDebugUtils()
{
    if (initialized) {
        pthread_mutex_destroy(&abortLock);
        initialized = false;
    }
}

extern void MOTAbort(void* faultAddress /* = NULL */)
{
    pthread_mutex_lock(&abortLock);
    if (!abortFlag) {
        abortFlag = true;

        MOTDumpCallStack();

        // print whatever the application wanted to print
        fflush(stderr);
        fflush(stdout);

        // analyze fault address
        if (faultAddress != nullptr) {
            MemAnalyze(faultAddress);
        }

        // dump to standard output stream all memory status
        MemDump();

        // now call abort
        ::abort();
    }
    pthread_mutex_unlock(&abortLock);  // just for proper order
}

extern void MOTDumpCallStack()
{
    void* buffer[100];
    int frames = backtrace(buffer, 100);
    backtrace_symbols_fd(buffer, frames, STDERR_FILENO);
}

extern void MOTPrintCallStackImpl(LogLevel logLevel, const char* logger, const char* format, ...)
{
    va_list args;
    va_start(args, format);

    MOTPrintCallStackImplV(logLevel, logger, format, args);

    va_end(args);
}

extern void MOTPrintCallStackImplV(LogLevel logLevel, const char* logger, const char* format, va_list args)
{
    va_list argsCopy;
    va_copy(argsCopy, args);

    // get stack trace
    void* trace[100];
    int frames = backtrace(trace, 100);
    char** frameStrArray = backtrace_symbols(trace, 100);

    // format message
    StringBuffer sb = {0};
    StringBufferInit(&sb, 1024, 2, MOT::StringBuffer::MULTIPLY);
    StringBufferAppendV(&sb, format, argsCopy);
    StringBufferAppend(&sb, "\n");
    for (int i = 0; i < frames; ++i) {
        StringBufferAppend(&sb, "#%d %s\n", i, frameStrArray[i]);

        // find first occurrence of '(' or ' ' in frameStrArray[i] and assume everything before that is
        // the file name. (Don't go beyond 0 though (string terminator)
        int p = 0;
        while (frameStrArray[i][p] != '(' && frameStrArray[i][p] != ' ' && frameStrArray[i][p] != 0) {
            ++p;
        }

        char sysCom[256];
        errno_t erc = snprintf_s(
            sysCom, sizeof(sysCom), sizeof(sysCom) - 1, "addr2line %p -e %.*s", trace[i], p, frameStrArray[i]);
        securec_check_ss(erc, "\0", "\0");
        std::string fileLine = ExecOsCommand(sysCom);
        StringBufferAppend(&sb, "%s\n", fileLine.c_str());
    }

    // print
    MOTLog(logLevel, logger, "%s", sb.m_buffer);

    // cleanup
    StringBufferDestroy(&sb);
    free(frameStrArray);

    va_end(argsCopy);
}
}  // namespace MOT
