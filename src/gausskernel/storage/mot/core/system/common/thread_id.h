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
 * thread_id.h
 *    Encapsulates the logic of a reusable thread id.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/common/thread_id.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef THREAD_ID_H
#define THREAD_ID_H

#include <cstdint>
#include "libintl.h"
#include "postgres.h"
#include "knl/knl_thread.h"

namespace MOT {
/** @var Maximum number of threads supported by the engine. */
#define MAX_THREAD_COUNT 4096

/** @typedef Thread identifier type. */
typedef uint16_t MOTThreadId;

/** @define Invalid thread identifier. */
#define INVALID_THREAD_ID ((MOT::MOTThreadId)-1)

#define MOTCurrThreadId t_thrd.mot_cxt.currentThreadId

/**
 * @brief Initializes the pool of reusable thread identifiers.
 * @param maxThreads The maximum number of threads in the system.
 * @return Zero if initialization succeeded, otherwise an error code.
 */
extern int InitThreadIdPool(uint16_t maxThreads);

/**
 * @brief Releases all resources associated with the pool of reusable thread identifiers.
 */
extern void DestroyThreadIdPool();

/**
 * @brief Allocates a reusable thread identifier for the current thread. When a thread begins, it
 * must make a call to @fn alloc_thread_id before it can use the global thread-local variable @ref
 * _current_thread_id.
 * @return A valid thread id, or @ref INVALID_THREAD_ID if failed.
 */
extern MOTThreadId AllocThreadId();

/**
 * @brief Allocates the highest possible thread identifier. It may be affined to any NUMA node,
 * according to the affinity map.
 * @return A valid thread id, or @ref INVALID_THREAD_ID if failed.
 */
extern MOTThreadId AllocThreadIdHighest();

/**
 * @brief Allocates the highest possible thread identifier on the given NUMA node.
 * @param nodeId The identifier of the NUMA node to which the thread is to be affined.
 * @return A valid thread id, or @ref INVALID_THREAD_ID if failed.
 */
extern MOTThreadId AllocThreadIdNumaHighest(int nodeId);

/**
 * @brief Allocates the highest possible thread identifier on the current NUMA node.
 * @return A valid thread id, or @ref INVALID_THREAD_ID if failed.
 */
extern MOTThreadId AllocThreadIdNumaCurrentHighest();

/**
 * @brief Reserves a reusable thread identifier for a future thread. When the thread begins, it
 * must make a call to @fn apply_reserved_thread_id before it begins actual execution, otherwise
 * it might allocate for itself a different thread identifier than the one reserved for the thread.
 * @return A valid thread id, or @ref INVALID_THREAD_ID if failed.
 */
extern MOTThreadId ReserveThreadId();

/**
 * @brief Applies a previously reserved thread identifier (i.e. sets it as the current thread identifier).
 * @param threadId The thread identifier to set.
 */
extern void ApplyReservedThreadId(MOTThreadId threadId);

/**
 * @brief Frees a reusable thread identifier for the current thread. This identifier can be reused
 * by another thread. When a thread finishes its execution it must call this function, so its thread
 * identifier can be returned to the pool of reusable thread identifiers.
 */
extern void FreeThreadId();

/**
 * @brief Get the ordinal number of the thread with the NUMA node to which it is affined.
 * @return The ordinal number of the thread within its affined socket.
 */
extern MOTThreadId GetNumaOrdinalThreadId(MOTThreadId threadId);

/**
 * @brief Retrieves the maximum number of threads allowed in the system.
 */
extern uint16_t GetMaxThreadCount();

/**
 * @brief Retrieves the current number of threads allocated in the system.
 */
extern uint16_t GetCurrentThreadCount();

// Testing API - This API exists only for testing and should not be used by the engine
extern bool IsThreadIdFree(MOTThreadId threadId);
extern void DumpThreadIds(const char* reportName);
}  // namespace MOT

#endif /* THREAD_ID_H */
