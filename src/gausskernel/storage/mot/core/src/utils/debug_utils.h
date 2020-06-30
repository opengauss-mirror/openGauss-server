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
 *    src/gausskernel/storage/mot/core/src/utils/debug_utils.h
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

namespace MOT {
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

/** @brief Debug helper for printing current stack trace (can be used later with addr2line). */
extern void MOTDumpCallStack();

/**
 * @brief Prints a message along with a call stack.
 * @param logLevel The message log level.
 * @param logger The logger name.
 * @param format The message format to print.
 * @param ... Any additional parameters required for the format message.
 */
extern void MOTPrintCallStackImpl(LogLevel logLevel, const char* logger, const char* format, ...);

/**
 * @brief Prints a message along with a call stack.
 * @param logLevel The message log level.
 * @param logger The logger name.
 * @param format The message format to print.
 * @param args Any additional parameters required for the format message.
 */
extern void MOTPrintCallStackImplV(LogLevel logLevel, const char* logger, const char* format, va_list args);
}  // namespace MOT

#endif /* DEBUG_UTILS_H */
