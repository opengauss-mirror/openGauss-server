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
 * mm_session_large_buffer_store.h
 *    Global store used for large session buffer allocations..
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/memory/mm_session_large_buffer_store.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef MM_SESSION_LARGE_BUFFER_STORE_H
#define MM_SESSION_LARGE_BUFFER_STORE_H

#include "mm_def.h"
#include "utilities.h"
#include "string_buffer.h"
#include "mm_session_large_buffer_pool.h"

#include <cstdint>

namespace MOT {
/**
 * @define A constant denoting the maximum size of the largest object in the session large buffer store, relative to
 * the size of the entire large buffer store. A factor of X means that the largest object can have a size of at most
 * 1/X of the entire store. This value should not be modified, as it is derived from the way the store is designed.
 * For further details see ComputeRealMaxObjectSize().
 */
#define MEM_SESSION_MAX_LARGE_OBJECT_FACTOR 8

/**
 * @brief Initializes the global store used for large session buffer allocations.
 * @return Zero if succeeded, otherwise an error code.
 */
extern int MemSessionLargeBufferStoreInit();

/** @brief Destroys the global store used for large session buffer allocations. */
extern void MemSessionLargeBufferStoreDestroy();

/** @var The actual maximum object size in bytes that can be allocated by the session large buffer store. */
extern uint64_t g_memSessionLargeBufferMaxObjectSizeBytes;

/**
 * @brief Allocates a large session buffer.
 * @param sizeBytes The buffer size in bytes.
 * @return The allocated buffer or NULL if failed (last error is set appropriately).
 */
extern void* MemSessionLargeBufferAlloc(uint64_t sizeBytes, MemSessionLargeBufferHeader** bufferHeaderList);

/**
 * @brief Deallocates a large session buffer.
 * @param buffer The buffer to deallocate.
 */
extern void MemSessionLargeBufferFree(void* buffer, MemSessionLargeBufferHeader** bufferHeaderList);

/**
 * @brief Reallocates a large session buffer.
 * @param buffer The buffer to reallocate.
 * @param newSizeBytes The new object size in bytes (expected to be larger than current object size).
 * @param flags Reallocation flags.
 * @return The reallocated buffer or NULL if failed (last error set appropriately). In case of
 * failure to allocate a new buffer, the old buffer is left intact.
 * @see MemReallocFlags
 */
extern void* MemSessionLargeBufferRealloc(
    void* buffer, uint64_t newSizeBytes, MemReallocFlags flags, MemSessionLargeBufferHeader** bufferHeaderList);

/**
 * @brief Retrieves the size in bytes of the given large buffer.
 */
extern uint64_t MemSessionLargeBufferGetObjectSize(void* buffer);

/**
 * @brief Prints status of all session large buffer pools into log.
 * @param name The name to prepend to the log message.
 * @param logLevel The log level to use in printing.
 * @param[opt] reportMode Specifies the report mode.
 */
extern void MemSessionLargeBufferPrint(
    const char* name, LogLevel logLevel, MemReportMode reportMode = MEM_REPORT_SUMMARY);

/**
 * @brief Dumps status of all session large buffer pools into string buffer.
 * @param indent The indentation level.
 * @param name The name to prepend to the log message.
 * @param stringBuffer The string buffer.
 * @param[opt] reportMode Specifies the report mode.
 */
extern void MemSessionLargeBufferToString(
    int indent, const char* name, StringBuffer* stringBuffer, MemReportMode reportMode = MEM_REPORT_SUMMARY);
}  // namespace MOT

/**
 * @brief Dumps all session large buffer store status to standard error stream.
 */
extern "C" void MemSessionLargeBufferStoreDump();

/**
 * @brief Analyzes the memory status of a given buffer address.
 * @param address The buffer to analyze.
 * @return Non-zero value if buffer was found.
 */
extern "C" int MemSessionLargeBufferStoreAnalyze(void* buffer);

#endif /* MM_SESSION_LARGE_BUFFER_STORE_H */
