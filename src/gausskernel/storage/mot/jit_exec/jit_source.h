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
 * jit_source.h
 *    A stencil used for caching a compiled function and cloning other context objects with identical query.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/jit_exec/jit_source.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef JIT_SOURCE_H
#define JIT_SOURCE_H

#include <pthread.h>

#include "jit_context.h"

namespace JitExec {
/**
 * @enum Constant values denoting return status for @ref WaitJitContextReady(). */
enum JitContextStatus : uint32_t {
    /** @var Source context is ready for use. */
    JIT_CONTEXT_READY,

    /** @var Source context is unavailable yet (it is being generated at the moment). */
    JIT_CONTEXT_UNAVAILABLE,

    /** @var Source context generation failed. This query will never be JIT-ted again. */
    JIT_CONTEXT_ERROR,

    /**
     * @var The source context expired since a DDL caused this query to be re-evaluated. Caller is required to
     * generate a new context. Only one session sees this status.
     */
    JIT_CONTEXT_EXPIRED
};

/**
 * @struct JitSource A stencil used for caching a compiled function and cloning other context
 * objects with identical query.
 */
struct __attribute__((packed)) JitSource {
    /** @var Lock used for concurrent compilation. */
    pthread_mutex_t _lock;  // L1 offset 0 (40 bytes)

    /** @var Keep noisy lock in its own cache line. */
    uint8_t _padding1[24];  // L1 offset 40

    /** @var Condition variable used to notify other threads waiting for compiler thread to finish. */
    pthread_cond_t _cond;  // L1 offset 0 (48 bytes)

    /** @var Keep noisy condition variable in its own cache line. */
    uint8_t _padding2[16];  // L1 offset 48

    /** @var The compiled function. */
    JitContext* _source_jit_context;  // L1 offset 0

    /** @var Tells whether the object was initialized successfully. */
    uint32_t _initialized;  // L1 offset 8

    /** @var The status of the context. */
    JitContextStatus _status;  // L1 offset 12

    /** @var Manage a list of cached context source objects for each session. */
    JitSource* _next;  // L1 offset 16

    /** @var The source query string. */
    char* _query_string;  // L1 offset 24

    /** @var Jit context list for cleanup during relation modification. */
    JitContext* m_contextList;  // L1 offset 32
};

/**
 * @brief Initializes a JIT source object.
 * @param jitSource The JIT source to initialize.
 * @param queryString The query string of the JIT source.
 * @return True if initialization succeeded, otherwise false. Consult @ref mm_get_root_error() for
 * further details.
 */
extern bool InitJitSource(JitSource* jitSource, const char* queryString);

/**
 * @brief Releases all resources associated with a JIT source.
 * @param jitSource The JIT source to destroy.
 */
extern void DestroyJitSource(JitSource* jitSource);

/**
 * @brief Re-initializes an already initialized JIT source object.
 * @detail Since JIT source objects are pooled, a quick re-init function is provided for reusing
 * reclaimed JIT source objects. Only partial initialization is required in this case.
 * @param jitSource The JIT source to re-initialize.
 * @param queryString The query string of the JIT source.
 */
extern void ReInitJitSource(JitSource* jitSource, const char* queryString);

/**
 * @brief Waits until the compiled function is ready for use (blocking call).
 * @param jitSource The JIT source to wait upon.
 * @param[out] readySourceJitContext The resulting ready JIT context (already cloned).
 * @return The resulting context status.
 */
extern JitContextStatus WaitJitContextReady(JitSource* jitSource, JitContext** readySourceJitContext);

/**
 * @brief Sets the cached context status to error.
 * jitSource The JIT source to set its error status.
 * @param errorCode The error code of the root cause for the failure.
 */
extern void SetJitSourceError(JitSource* jitSource, int errorCode);

/**
 * @brief Sets the cached context status to error.
 * @param relationId The external identifier of the relation that caused this JIT source to expire.
 */
extern void SetJitSourceExpired(JitSource* jitSource, uint64_t relationId);

/**
 * @brief Installs a ready source JIT context.
 * jitSource The JIT source to set its ready status.
 * @param readySourceJitContext The ready source JIT context to install.
 * @return True if operation succeeded, or false in case the JIT source is already ready with a valid JIT context.
 */
extern bool SetJitSourceReady(JitSource* jitSource, JitContext* readySourceJitContext);

/**
 * @brief Registers JIT context for cleanup when its JIT source gets purged due to relation modification.
 * @param jitSource The originating JIT source.
 * @param jitContext The JIT context to register.
 */
extern void AddJitSourceContext(JitSource* jitSource, JitContext* jitContext);

/**
 * @brief Un-registers JIT context from cleanup when its JIT source gets purged due to relation modification.
 * @param jitSource The originating JIT source.
 * @param jitContext The JIT context to un-register.
 */
extern void RemoveJitSourceContext(JitSource* jitSource, JitContext* cleanupContext);

/** @brief Queries whether a JIT source refers to a given relation. */
extern bool JitSourceRefersRelation(JitSource* jitSource, uint64_t relationId);

/** @brief Purges the JIT source from all key/index references to the given relation. */
extern void PurgeJitSource(JitSource* jitSource, uint64_t relationId);
}  // namespace JitExec

#endif
