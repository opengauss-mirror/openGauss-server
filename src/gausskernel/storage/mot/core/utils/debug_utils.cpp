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
 *    src/gausskernel/storage/mot/core/utils/debug_utils.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "utilities.h"
#include "string_buffer.h"
#include "mm_api.h"
#include "debug_utils.h"
#include "mot_string.h"

#include <pthread.h>
#include <execinfo.h>
#include <unistd.h>
#include <algorithm>

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

static void SetCallStackName(char* name, const char* sourceName, size_t size)
{
    errno_t erc = strncpy_s(name, MOT_CALL_STACK_MAX_NAME - 1, sourceName, size);
    securec_check(erc, "\0", "\0");
    name[std::min(MOT_CALL_STACK_MAX_NAME - 1, size)] = 0;

    // trim any trailing whitespace (including newline)
    char ws[] = " \t\r\n";
    int pos = (int)strlen(name);
    if (pos > 0) {
        --pos;
        while ((pos >= 0) && (strchr(ws, name[pos]) != nullptr)) {
            name[pos] = 0;
            --pos;
        }
    }
}

static void ParseCallStackFrame(void* frame, char* frameStr, CallStackFrame* resolvedFrame, int opts)
{
    // expected format is MODULE(FRAME+OFFSET) [ADDRESS]
    // 1. assign raw frame
    size_t size = strlen(frameStr);
    SetCallStackName(resolvedFrame->m_rawFrame, frameStr, size);

    // 2. parse module and frame
    char* openParenPos = strchr(frameStr, '(');
    if (openParenPos != nullptr) {
        size = openParenPos - frameStr;
        SetCallStackName(resolvedFrame->m_module, frameStr, size);

        // parse frame and offset
        char* closeParenPos = strchr(frameStr, ')');
        if (closeParenPos != nullptr) {
            size = closeParenPos - openParenPos - 1;
            SetCallStackName(resolvedFrame->m_frame, openParenPos + 1, size);

            // now break it again into frame and offset
            char* plusPos = strchr(resolvedFrame->m_frame, '+');
            if (plusPos != nullptr) {
                resolvedFrame->m_frameOffset = strtoull(plusPos + 1, nullptr, 16);
                *plusPos = 0;
            }
        }
    }

    // 2. parse address
    char* openBracketPos = strchr(frameStr, '[');
    if (openBracketPos != nullptr) {
        resolvedFrame->m_address = strtoull(openBracketPos + 1, nullptr, 16);
    }

    // 3. get demangled name if required
    if (opts & MOT_CALL_STACK_DEMANGLE) {
        if (resolvedFrame->m_frame[0] != 0) {
            mot_string sysCom;
            sysCom.format("echo '%s' | c++filt", resolvedFrame->m_frame);
            std::string sysRes = ExecOsCommand(sysCom.c_str());
            SetCallStackName(resolvedFrame->m_demangledFrame, sysRes.c_str(), sysRes.length());
        }
    }

    // 4. get file and line if required
    if (opts & MOT_CALL_STACK_FILE_LINE) {
        mot_string sysCom;
        sysCom.format("addr2line %p -e %s", frame, resolvedFrame->m_module);
        std::string sysRes = ExecOsCommand(sysCom.c_str());
        SetCallStackName(resolvedFrame->m_file, sysRes.c_str(), sysRes.length());
        char* colonPos = strchr(resolvedFrame->m_file, ':');
        if (colonPos != nullptr) {
            resolvedFrame->m_line = strtoull(colonPos + 1, nullptr, 0);
            *colonPos = 0;
        }
    }
}

extern int MOTGetCallStack(CallStackFrame* resolvedFrames, uint32_t frameCount, int opts)
{
    uint32_t actualFrameCount = std::min(MOT_CALL_STACK_MAX_FRAMES, frameCount);
    void* rawFrames[MOT_CALL_STACK_MAX_FRAMES];

    // get back-trace with symbols
    int validFrameCount = backtrace(rawFrames, actualFrameCount);
    char** frameStrArray = backtrace_symbols(rawFrames, validFrameCount);

    // parse frames
    for (int i = 0; i < validFrameCount; ++i) {
        ParseCallStackFrame(rawFrames[i], frameStrArray[i], &resolvedFrames[i], opts);
    }

    free(frameStrArray);
    return validFrameCount;
}

extern void MOTDumpCallStack()
{
    void* buffer[100];
    int frames = backtrace(buffer, 100);
    backtrace_symbols_fd(buffer, frames, STDERR_FILENO);
}

extern void MOTPrintCallStackImpl(LogLevel logLevel, const char* logger, int opts, const char* format, ...)
{
    va_list args;
    va_start(args, format);

    MOTPrintCallStackImplV(logLevel, logger, opts, format, args);

    va_end(args);
}

extern void MOTPrintCallStackImplV(LogLevel logLevel, const char* logger, int opts, const char* format, va_list args)
{
    va_list argsCopy;
    va_copy(argsCopy, args);

    // get stack trace
    CallStackFrame resolvedFrames[MOT_CALL_STACK_MAX_FRAMES];
    int frameCount = MOTGetCallStack(resolvedFrames, MOT_CALL_STACK_MAX_FRAMES, opts);

    // format message
    StringBuffer sb = {0};
    StringBufferInit(&sb, 1024, 2, MOT::StringBuffer::MULTIPLY);
    StringBufferAppendV(&sb, format, argsCopy);
    StringBufferAppend(&sb, "\n");
    for (int i = 0; i < frameCount; ++i) {
        CallStackFrame* frame = &resolvedFrames[i];
        if (opts == 0) {
            // raw printing
            StringBufferAppend(&sb, "#%d %s\n", frame->m_rawFrame);
        } else {
            // formatted printing
            StringBufferAppend(&sb, "#%d ", i);
            if (opts & MOT_CALL_STACK_DEMANGLE) {
                StringBufferAppend(&sb, "%s (+0x%x)\n", frame->m_demangledFrame, frame->m_frameOffset);
            } else {
                StringBufferAppend(&sb, "%s (+0x%x)\n", frame->m_frame, frame->m_frameOffset);
            }
            if (opts & MOT_CALL_STACK_FILE_LINE) {
                StringBufferAppend(&sb, "\tat %s:%" PRIu64 "\n", frame->m_file, frame->m_line);
            }
            if (opts & MOT_CALL_STACK_MODULE) {
                StringBufferAppend(&sb, "\tat %s [0x%" PRIx64 "]\n", frame->m_module, frame->m_address);
            }
        }
    }

    // print
    MOTLog(logLevel, logger, "%s", sb.m_buffer);

    // cleanup
    StringBufferDestroy(&sb);

    va_end(argsCopy);
}
}  // namespace MOT
