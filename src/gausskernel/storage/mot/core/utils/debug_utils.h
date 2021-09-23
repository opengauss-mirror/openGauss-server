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
 * debug_utils.h
 *    Debug utilities implementation.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/utils/debug_utils.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef DEBUG_UTILS_H
#define DEBUG_UTILS_H

#include <assert.h>
#include <stddef.h>
#include <stdarg.h>

#include "log_level.h"

// due to varying and conflicting definitions, we make sure debug/release macros as well as assert are well defined
// we do not allow DEBUG and NDEBUG to be defined together
#if defined(DEBUG) && defined(NDEBUG)
#error Conflicting compiler definitions: both DEBUG and NDEBUG are defined
#endif

// we require explicit DEBUG to be defined for debug mode to be in effect (unlike assert() which requires explicit
// NDEBUG to be defined for release mode to be in effect, otherwise it is a debug build)
#if defined(DEBUG)
#define MOT_DEBUG
#endif

// unlike assert (which is defined if NDEBUG is not defined), we define MOT_ASSERT on explicit debug mode
#ifdef MOT_DEBUG
#define MOT_ASSERT assert
#else
#define MOT_ASSERT(invariant)
#endif

/** @define The maximum number of frames allowed in a call stack. */
#define MOT_CALL_STACK_MAX_FRAMES ((uint32_t)100)

/**
 * @define Specifies that mangled frame names should be demangled (triggers execution of c++filt sub-process for each
 * frame in the stack trace).
 */
#define MOT_CALL_STACK_DEMANGLE 0x01

/**
 * @define Specifies that frame addresses should be converted to file and line number (triggers execution of addr2line
 * sub-process for each frame in the stack trace).
 */
#define MOT_CALL_STACK_FILE_LINE 0x02

/** @define Specifies that full module path of each frame should be printed. */
#define MOT_CALL_STACK_MODULE 0x04

/** @define Specifies that frame addresses should be printed. */
#define MOT_CALL_STACK_ADDRESS 0x08

/** #define Slim call stack options (just module and address for quick printing). */
#define MOT_CALL_STACK_SLIM (MOT_CALL_STACK_MODULE | MOT_CALL_STACK_ADDRESS)

/** #define Normal call stack options (includes name demangling). */
#define MOT_CALL_STACK_NORMAL (MOT_CALL_STACK_SLIM | MOT_CALL_STACK_DEMANGLE)

/** #define Full call stack options (very slow). */
#define MOT_CALL_STACK_FULL 0x0F

/** @define Maximum number of character in a call stack frame string member. */
#define MOT_CALL_STACK_MAX_NAME ((size_t)256)

namespace MOT {
/** @struct A single call stack frame. */
struct CallStackFrame {
    /** @var The raw frame as retrieved from back-trace call. */
    char m_rawFrame[MOT_CALL_STACK_MAX_NAME];

    /** @var The module name. */
    char m_module[MOT_CALL_STACK_MAX_NAME];

    /** @var The raw frame name. */
    char m_frame[MOT_CALL_STACK_MAX_NAME];

    /** @var The demangled frame name. */
    char m_demangledFrame[MOT_CALL_STACK_MAX_NAME];

    /** @var The file name of the frame. */
    char m_file[MOT_CALL_STACK_MAX_NAME];

    /** @var The offset of the frame instruction within the frame function. */
    uint64_t m_frameOffset;

    /** @var The frame address. */
    uint64_t m_address;

    /** @var The line number (within the file) of the frame. */
    uint64_t m_line;

    /** @brief Default constructor. */
    CallStackFrame()
        : m_rawFrame{}, m_module{}, m_frame{}, m_demangledFrame{}, m_file{}, m_frameOffset(0), m_address(0), m_line(0)
    {}
};

/** @brief Initializes debug utilities. */
extern bool InitializeDebugUtils();

/** @brief Releases all resources associated with debug utilities. */
extern void DestroyDebugUtils();

/**
 * @brief A global abort function for all fatal errors. This function is supposed to analyze the faulting address
 * (if any) and dump an analysis report, as well as a full map report.
 * @param faultAddress The faulting address
 */
extern void MOTAbort(void* faultAddress = nullptr);

/**
 * @brief Debug helper for printing current stack trace (can be used later with addr2line). This is the most primitive
 * way to dump call stack to log, without any formatting.
 */
extern void MOTDumpCallStack();

/**
 * @brief Retrieves the current call stack. No more than @ref MOT_CALL_STACK_MAX_FRAMES will be retrieved.
 * @param[out] resolvedFrames The array of call stack frames to be resolved.
 * @param frameCount The number of frames in the frame array.
 * @param opts Frame parsing options.
 * @return The number of resolved frames.
 */
extern int MOTGetCallStack(CallStackFrame* resolvedFrames, uint32_t frameCount, int opts);

/**
 * @brief Prints a message along with a call stack.
 * @param logLevel The message log level.
 * @param logger The logger name.
 * @param opts Call stack printing options.
 * @param format The message format to print.
 * @param ... Any additional parameters required for the format message.
 */
extern void MOTPrintCallStackImpl(LogLevel logLevel, const char* logger, int opts, const char* format, ...);

/**
 * @brief Prints a message along with a call stack.
 * @param logLevel The message log level.
 * @param logger The logger name.
 * @param opts Call stack printing options.
 * @param format The message format to print.
 * @param args Any additional parameters required for the format message.
 */
extern void MOTPrintCallStackImplV(LogLevel logLevel, const char* logger, int opts, const char* format, va_list args);
}  // namespace MOT

#endif /* DEBUG_UTILS_H */
