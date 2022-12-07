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
 * jit_context_pool.cpp
 *    A pool of JIT context objects.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/jit_exec/jit_context_pool.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "global.h"
#include "postgres.h"
#include "utils/memutils.h"

#include "jit_context_pool.h"
#include "utilities.h"
#include "mm_global_api.h"

namespace JitExec {
DECLARE_LOGGER(JitContextPool, JitExec)

/** @define The maximum size of a JIT context object. */
union JitUnifiedContext {
    JitQueryContext m_queryContext;
    JitFunctionContext m_functionContext;
};

static uint32_t nextPoolId = 0;

#define JitContextSize (((sizeof(JitUnifiedContext) + 1) / 8) * 8)

#define TRACE_JIT_CONTEXT_SUB_POOL(op, usage)                                  \
    MOT_LOG_DEBUG("%s JIT context pool head %p free-count " op " context: %u", \
        JitContextUsageToString(usage),                                        \
        contextSubPool->m_freeContextList,                                     \
        contextSubPool->m_freeContextCount)

#define TRACE_JIT_CONTEXT_POOL(op)                                             \
    MOT_LOG_DEBUG("%s JIT context pool head %p free-count " op " context: %u", \
        JitContextUsageToString(contextPool->m_usage),                         \
        contextPool->m_freeContextList,                                        \
        contextPool->m_freeContextCount)

inline bool LockJitContextPool(JitContextPool* contextPool)
{
    if (contextPool->m_usage == JIT_CONTEXT_GLOBAL) {
        int res = pthread_spin_lock(&contextPool->m_lock);
        if (res != 0) {
            MOT_REPORT_SYSTEM_ERROR_CODE(
                res, pthread_spin_lock, "Global JIT Context Allocation", "Failed to acquire spin lock for global pool");
            return false;
        }
    }
    return true;
}

inline void UnlockJitContextPool(JitContextPool* contextPool)
{
    if (contextPool->m_usage == JIT_CONTEXT_GLOBAL) {
        int res = pthread_spin_unlock(&contextPool->m_lock);
        if (res != 0) {
            MOT_REPORT_SYSTEM_ERROR_CODE(res,
                pthread_spin_unlock,
                "Global JIT Context Allocation",
                "Failed to release spin lock for global pool");
            // system is in undefined state, we expect to crash any time soon, but we continue anyway
        }
    }
}

static bool InitJitContextSubPool(
    JitContextSubPool* contextSubPool, JitContextUsage usage, uint32_t poolId, uint16_t subPoolId, uint32_t poolSize)
{
    MOT_ASSERT(contextSubPool->m_contextPool == nullptr);
    contextSubPool->m_contextPool = nullptr;
    contextSubPool->m_freeContextList = nullptr;
    contextSubPool->m_freeContextCount = 0;
    contextSubPool->m_subPoolId = subPoolId;

    // allocate pool
    size_t allocSize = JitContextSize * poolSize;
    if (usage == JIT_CONTEXT_GLOBAL) {
        contextSubPool->m_contextPool = (MotJitContext*)MOT::MemGlobalAllocAligned(allocSize, L1_CACHE_LINE);
    } else {
        contextSubPool->m_contextPool = (MotJitContext*)MOT::MemSessionAllocAligned(allocSize, L1_CACHE_LINE);
    }
    if (contextSubPool->m_contextPool == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "JIT Context Pool Initialization",
            "Failed to allocate %u JIT context objects (64-byte aligned %u bytes) for %s JIT context sub-pool",
            poolSize,
            allocSize,
            JitContextUsageToString(usage));
        return false;
    }
    errno_t erc = memset_s(contextSubPool->m_contextPool, allocSize, 0, allocSize);
    securec_check(erc, "\0", "\0");

    // fill the free list
    for (uint32_t i = 0; i < poolSize; ++i) {
        MotJitContext* jitContext = (MotJitContext*)(((char*)contextSubPool->m_contextPool) + JitContextSize * i);
        jitContext->m_next = contextSubPool->m_freeContextList;
        jitContext->m_poolId = poolId;
        jitContext->m_subPoolId = subPoolId;
        jitContext->m_contextId = i;
        jitContext->m_usage = usage;
        contextSubPool->m_freeContextList = jitContext;
    }
    contextSubPool->m_freeContextCount = poolSize;

#ifdef MOT_JIT_DEBUG
    // manage free ids for error catching
    contextSubPool->m_freeContextIds = new (std::nothrow) int[poolSize];
    if (contextSubPool->m_freeContextIds != nullptr) {
        for (uint32_t i = 0; i < poolSize; ++i) {
            contextSubPool->m_freeContextIds[i] = 1;
        }
    }
#endif

    return true;
}

static void DestroyJitContextSubPool(JitContextSubPool* contextSubPool, JitContextUsage usage)
{
    // guard against repeated calls
    if (contextSubPool->m_contextPool == nullptr) {
        return;
    }

    MOT_LOG_TRACE("Destroy %s JIT sub-context pool %" PRIu16 " head %p free-count %u",
        JitContextUsageToString(usage),
        contextSubPool->m_subPoolId,
        contextSubPool->m_freeContextList,
        contextSubPool->m_freeContextCount);

    MOT_ASSERT(contextSubPool->m_freeContextCount == JIT_SUB_POOL_SIZE);

#ifdef MOT_JIT_DEBUG
    // cleanup debug ids
    if (contextSubPool->m_freeContextIds != nullptr) {
        delete[] contextSubPool->m_freeContextIds;
        contextSubPool->m_freeContextIds = nullptr;
    }
#endif

    // free context pool
    if (usage == JIT_CONTEXT_GLOBAL) {
        MOT::MemGlobalFree(contextSubPool->m_contextPool);
    } else {
        MOT::MemSessionFree(contextSubPool->m_contextPool);
    }
    contextSubPool->m_contextPool = nullptr;

    // reset all pool attributes
    contextSubPool->m_freeContextList = nullptr;
    contextSubPool->m_freeContextCount = 0;
}

static MotJitContext* AllocSubPooledJitContext(
    JitContextSubPool* contextSubPool, JitContextUsage usage, uint32_t poolId, uint16_t subPoolId)
{
    // allocate one context
    TRACE_JIT_CONTEXT_SUB_POOL("before alloc", usage);
    MotJitContext* result = contextSubPool->m_freeContextList;
    if (contextSubPool->m_freeContextList != nullptr) {
        contextSubPool->m_freeContextList = contextSubPool->m_freeContextList->m_next;
        --contextSubPool->m_freeContextCount;
#ifdef MOT_JIT_DEBUG
        if (result != nullptr) {
            if (contextSubPool->m_freeContextIds != nullptr) {
                contextSubPool->m_freeContextIds[result->m_contextId] = 0;
            }
        }
#endif
    }
    TRACE_JIT_CONTEXT_SUB_POOL("after alloc", usage);

    if (result == nullptr) {
        MOT_LOG_DEBUG("Sub-pool is empty");
    } else {
        MOT_ASSERT((result->m_usage == usage) ||
                   ((result->m_usage == JIT_CONTEXT_GLOBAL_SECONDARY) && (usage == JIT_CONTEXT_GLOBAL)));
        MOT_ASSERT(result->m_poolId == poolId);

        // prepare context for initial usage
        uint32_t contextId = result->m_contextId;  // save context id before memset
        errno_t erc = memset_s(result, sizeof(JitUnifiedContext), 0, sizeof(JitUnifiedContext));
        securec_check(erc, "\0", "\0");
        result->m_usage = usage;
        result->m_poolId = poolId;
        result->m_subPoolId = subPoolId;
        result->m_contextId = contextId;
        result->m_next = nullptr;
        MOT_LOG_TRACE("Allocated %s JIT context at %p", JitContextUsageToString(usage), result);
    }

    return result;
}

static void FreeSubPooledJitContext(JitContextSubPool* contextSubPool, MotJitContext* jitContext, JitContextUsage usage,
    uint32_t poolId, uint16_t subPoolId, uint32_t poolSize)
{
    if (contextSubPool == nullptr) {
        MOT_ASSERT(false);  // this should not happen
        return;
    }

    MOT_ASSERT(contextSubPool->m_freeContextCount < poolSize);
#ifdef MOT_JIT_DEBUG
    MOT_ASSERT((contextSubPool->m_freeContextIds == nullptr) ||
               (contextSubPool->m_freeContextIds[jitContext->m_contextId] == 0));
#endif

    // free the context into the pool
    TRACE_JIT_CONTEXT_SUB_POOL("before free", usage);
    jitContext->m_next = contextSubPool->m_freeContextList;
    contextSubPool->m_freeContextList = jitContext;
    ++contextSubPool->m_freeContextCount;
#ifdef MOT_JIT_DEBUG
    if (contextSubPool->m_freeContextIds != nullptr) {
        contextSubPool->m_freeContextIds[jitContext->m_contextId] = 1;
    }
#endif
    TRACE_JIT_CONTEXT_SUB_POOL("after free", usage);
}

extern bool InitJitContextPool(JitContextPool* contextPool, JitContextUsage usage, uint32_t poolSize)
{
    // we align pool size to a multiple of sub-pool size
    poolSize = ((poolSize + JIT_SUB_POOL_SIZE - 1) / JIT_SUB_POOL_SIZE) * JIT_SUB_POOL_SIZE;
    contextPool->m_usage = usage;
    contextPool->m_subPools = nullptr;
    contextPool->m_poolSize = poolSize;
    contextPool->m_subPoolCount = poolSize / JIT_SUB_POOL_SIZE;
    contextPool->m_poolId = MOT_ATOMIC_INC(nextPoolId);

    // allocate lock for global pool
    if (usage == JIT_CONTEXT_GLOBAL) {
        int res = pthread_spin_init(&contextPool->m_lock, 0);
        if (res != 0) {
            MOT_REPORT_SYSTEM_ERROR_CODE(res,
                pthread_spin_init,
                "JIT Context Pool Initialization",
                "Failed to initialize spin lock for global pool");
            return false;
        }
    }

    // allocate pool
    size_t allocSize = sizeof(JitContextSubPool*) * poolSize;
    if (usage == JIT_CONTEXT_GLOBAL) {
        contextPool->m_subPools = (JitContextSubPool**)MOT::MemGlobalAllocAligned(allocSize, L1_CACHE_LINE);
    } else {
        contextPool->m_subPools = (JitContextSubPool**)MOT::MemSessionAllocAligned(allocSize, L1_CACHE_LINE);
    }
    if (contextPool->m_subPools == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "JIT Context Pool Initialization",
            "Failed to allocate %u bytes for JIT context sub-pool array with %u pools",
            allocSize,
            poolSize,
            JitContextUsageToString(usage));
        if (usage == JIT_CONTEXT_GLOBAL) {
            (void)pthread_spin_destroy(&contextPool->m_lock);
        }
        return false;
    }
    errno_t erc = memset_s(contextPool->m_subPools, allocSize, 0, allocSize);
    securec_check(erc, "\0", "\0");

    return true;
}

extern void DestroyJitContextPool(JitContextPool* contextPool)
{
    // guard against repeated calls
    if (contextPool->m_subPools == nullptr) {
        return;
    }

    MOT_LOG_DEBUG("Destroy %s JIT context pool", JitContextUsageToString(contextPool->m_usage));

    // free sub-pools
    for (uint16_t i = 0; i < contextPool->m_subPoolCount; ++i) {
        if (contextPool->m_subPools[i] != nullptr) {
            DestroyJitContextSubPool(contextPool->m_subPools[i], contextPool->m_usage);
            if (contextPool->m_usage == JIT_CONTEXT_GLOBAL) {
                MOT::MemGlobalFree(contextPool->m_subPools[i]);
            } else {
                MOT::MemSessionFree(contextPool->m_subPools[i]);
            }
            contextPool->m_subPools[i] = nullptr;
        }
    }
    if (contextPool->m_usage == JIT_CONTEXT_GLOBAL) {
        MOT::MemGlobalFree(contextPool->m_subPools);
    } else {
        MOT::MemSessionFree(contextPool->m_subPools);
    }
    contextPool->m_subPools = nullptr;

    // free global pool lock
    if (contextPool->m_usage == JIT_CONTEXT_GLOBAL) {
        int res = pthread_spin_destroy(&contextPool->m_lock);
        if (res != 0) {
            MOT_REPORT_SYSTEM_ERROR_CODE(res,
                pthread_spin_destroy,
                "JIT Context Pool Destruction",
                "Failed to destroy spin lock for global pool");
        }
    }

    // reset all pool attributes
    contextPool->m_poolSize = 0;
}

extern MotJitContext* AllocPooledJitContext(JitContextPool* contextPool)
{
    // lock global pool if needed
    if (!LockJitContextPool(contextPool)) {
        return nullptr;
    }

    // allocate one context from any pool
    MotJitContext* result = nullptr;
    for (uint16_t i = 0; i < contextPool->m_subPoolCount; ++i) {
        if (contextPool->m_subPools[i] != nullptr) {
            result =
                AllocSubPooledJitContext(contextPool->m_subPools[i], contextPool->m_usage, contextPool->m_poolId, i);
            if (result != nullptr) {
                break;
            }
        }
    }

    // see if new sub-pool allocation is required
    if (result == nullptr) {
        MOT_LOG_TRACE("All Sub-pools are empty");
        for (uint16_t i = 0; i < contextPool->m_subPoolCount; ++i) {
            if (contextPool->m_subPools[i] == nullptr) {
                size_t allocSize = sizeof(JitContextSubPool);
                if (contextPool->m_usage == JIT_CONTEXT_GLOBAL) {
                    contextPool->m_subPools[i] =
                        (JitContextSubPool*)MOT::MemGlobalAllocAligned(allocSize, L1_CACHE_LINE);
                } else {
                    contextPool->m_subPools[i] =
                        (JitContextSubPool*)MOT::MemSessionAllocAligned(allocSize, L1_CACHE_LINE);
                }
                if (contextPool->m_subPools[i] == nullptr) {
                    MOT_REPORT_ERROR(MOT_ERROR_OOM,
                        "JIT Context Pool Initialization",
                        "Failed to allocate %u bytes for JIT context sub-pool",
                        allocSize);
                    break;
                }
                errno_t erc = memset_s(contextPool->m_subPools[i], allocSize, 0, allocSize);
                securec_check(erc, "\0", "\0");

                if (!InitJitContextSubPool(contextPool->m_subPools[i],
                        contextPool->m_usage,
                        contextPool->m_poolId,
                        i,
                        JIT_SUB_POOL_SIZE)) {
                    MOT_REPORT_ERROR(MOT_ERROR_OOM,
                        "JIT Context Pool Initialization",
                        "Failed to initialize JIT Context pool %u, sub-pool %u",
                        contextPool->m_poolId,
                        i);
                    break;
                }
                result = AllocSubPooledJitContext(
                    contextPool->m_subPools[i], contextPool->m_usage, contextPool->m_poolId, i);
                if (result == nullptr) {
                    MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                        "JIT Context Pool Allocation",
                        "Failed to allocate JIT context from JIT Context pool %u, new sub-pool %u",
                        contextPool->m_poolId,
                        i);
                }
                break;
            }
        }
    }

    // unlock global pool if needed
    UnlockJitContextPool(contextPool);

    if (result == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_RESOURCE_LIMIT,
            "JIT Context Allocation",
            "Failed to allocate %s JIT context: pool with %u context objects depleted",
            JitContextUsageToString(contextPool->m_usage),
            contextPool->m_poolSize);
    }

    return result;
}

extern void FreePooledJitContext(JitContextPool* contextPool, MotJitContext* jitContext)
{
    if (contextPool == nullptr) {
        MOT_ASSERT(false);  // this should not happen
        return;
    }

    MOT_ASSERT(jitContext->m_poolId == contextPool->m_poolId);
    MOT_ASSERT((jitContext->m_usage == contextPool->m_usage) ||
               ((jitContext->m_usage == JIT_CONTEXT_GLOBAL_SECONDARY) && (contextPool->m_usage == JIT_CONTEXT_GLOBAL)));

    MOT_LOG_TRACE("Freeing %s JIT %s context at %p",
        JitContextUsageToString(contextPool->m_usage),
        JitContextTypeToString(jitContext->m_contextType),
        jitContext);

    // lock global pool if needed
    if (!LockJitContextPool(contextPool)) {
        return;
    }

    uint16_t subPoolId = jitContext->m_subPoolId;
    JitContextSubPool* contextSubPool = contextPool->m_subPools[subPoolId];
    FreeSubPooledJitContext(
        contextSubPool, jitContext, contextPool->m_usage, contextPool->m_poolId, subPoolId, JIT_SUB_POOL_SIZE);

    // release sub-pool if it became empty
    if (contextSubPool->m_freeContextCount == JIT_SUB_POOL_SIZE) {
        DestroyJitContextSubPool(contextSubPool, contextPool->m_usage);
        if (contextPool->m_usage == JIT_CONTEXT_GLOBAL) {
            MOT::MemGlobalFree(contextSubPool);
        } else {
            MOT::MemSessionFree(contextSubPool);
        }
        contextPool->m_subPools[subPoolId] = nullptr;
    }

    // unlock global pool if needed
    UnlockJitContextPool(contextPool);
}

extern uint32_t GetJitContextSize()
{
    return (uint32_t)JitContextSize;
}
}  // namespace JitExec
