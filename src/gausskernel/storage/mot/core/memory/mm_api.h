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
 * mm_api.h
 *    Memory management API implementation.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/memory/mm_api.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef MM_API_H
#define MM_API_H

#include "utilities.h"
#include "string_buffer.h"
#include "mm_def.h"

namespace MOT {
/**
 * @brief Initialize the memory management API.
 * @detail During startup memory is reserved. Usually the reserved memory is virtual, and cannot be ensured to be
 * available as physical when actually required. Setting the @ref reserveMode parameter to @ref MEM_RESERVE_VIRTUAL
 * accelerates startup time, but cannot ensure physical memory will be available when required. In addition, page faults
 * are expected during runtime, which can slow down execution. On the other hand, setting the @ref reserveMode
 * parameter to @ref MEM_RESERVE_PHYSICAL slows down startup time, but ensures all reserved memory will be available,
 * and in addition avoids page faults during runtime.
 * @param[opt] reserveMode Specifies whether to reserve physical or virtual memory.
 * @return Zero if succeeded, otherwise an error code.
 * @note The thread id pool must be already initialized.
 * @see InitThreadIdPool.
 */
extern int MemInit(MemReserveMode reserveMode = MEM_RESERVE_PHYSICAL);

/**
 * @brief Destroys the memory management API.
 */
extern void MemDestroy();

/**
 * @brief Clears all the thread-local caches of the current session.
 * @note Usually this function is called when a session terminates, so that all its buffer caches be cleared and
 * returned to the appropriate buffer heap, but on Thread-pooled environments the cache is cleared only when a worker
 * thread terminates. On thread-per-session model, end-of-session and end-of-thread are almost synonyms, so in both
 * cases (thread-pool or thread-per-session) we call this function when a thread ends, be it a worker thread or a
 * session thread.
 * @see @ref MMEngine::OnCurrentThreadEnding.
 */
extern void MemClearSessionThreadCaches();

/**
 * @brief Calculates the total memory consumption on all global chunk pools.
 * @return The total memory consumption in bytes or zero if failed.
 */
extern uint64_t MemGetCurrentGlobalMemoryBytes();

/**
 * @brief Prints all memory management API status into log.
 * @param name The name to prepend to the log message.
 * @param logLevel The log level to use in printing.
 * @param[opt] reportMode Specifies the report mode.
 */
extern void MemPrint(const char* name, LogLevel logLevel, MemReportMode reportMode = MEM_REPORT_SUMMARY);

/**
 * @brief Dumps all memory management API status into string buffer.
 * @param name The name to prepend to the log message.
 * @param stringBuffer The string buffer.
 * @param[opt] reportMode Specifies the report mode.
 */
extern void MemToString(const char* name, StringBuffer* stringBuffer, MemReportMode reportMode = MEM_REPORT_SUMMARY);
};  // namespace MOT

/**
 * @brief Dumps all memory management API status to standard error stream.
 */
extern "C" void MemDump();

/**
 * @brief Dumps analysis for a memory address.
 */
extern "C" void MemAnalyze(void* address);

#endif /* MM_API_H */
