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

extern bool InitJitContextPool(JitContextPool* contextPool, JitContextUsage usage, uint32_t poolSize)
{
    MOT_ASSERT(contextPool->m_contextPool == nullptr);
    contextPool->m_usage = usage;
    contextPool->m_contextPool = nullptr;
    contextPool->m_poolSize = 0;
    contextPool->m_freeContextList = nullptr;
    contextPool->m_freeContextCount = 0;

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

    size_t allocSize = sizeof(JitContext) * poolSize;
    contextPool->m_contextPool = (JitContext*)MOT::MemGlobalAllocAligned(allocSize, L1_CACHE_LINE);
    if (contextPool->m_contextPool == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "JIT Context Pool Initialization",
            "Failed to allocate %u JIT context objects (64-byte aligned %u bytes) for %s JIT context pool",
            poolSize,
            allocSize,
            usage == JIT_CONTEXT_GLOBAL ? "global" : "session-local");
        if (usage == JIT_CONTEXT_GLOBAL) {
            pthread_spin_destroy(&contextPool->m_lock);
        }
        return false;
    }
    errno_t erc = memset_s(contextPool->m_contextPool, allocSize, 0, allocSize);
    securec_check(erc, "\0", "\0");

    // fill the free list
    contextPool->m_poolSize = poolSize;
    for (uint32_t i = 0; i < poolSize; ++i) {
        JitContext* jitContext = &contextPool->m_contextPool[i];
        jitContext->m_next = contextPool->m_freeContextList;
        contextPool->m_freeContextList = jitContext;
    }
    contextPool->m_freeContextCount = contextPool->m_poolSize;

    return true;
}

extern void DestroyJitContextPool(JitContextPool* contextPool)
{
    if (contextPool->m_contextPool == nullptr) {
        return;
    }

    MOT::MemGlobalFree(contextPool->m_contextPool);

    if (contextPool->m_usage == JIT_CONTEXT_GLOBAL) {
        int res = pthread_spin_destroy(&contextPool->m_lock);
        if (res != 0) {
            MOT_REPORT_SYSTEM_ERROR_CODE(res,
                pthread_spin_destroy,
                "JIT Context Pool Destruction",
                "Failed to destroy spin lock for global pool");
        }
    }

    contextPool->m_contextPool = nullptr;
    contextPool->m_poolSize = 0;
    contextPool->m_freeContextList = nullptr;
    contextPool->m_freeContextCount = 0;
}

extern JitContext* AllocPooledJitContext(JitContextPool* contextPool)
{
    if (contextPool->m_usage == JIT_CONTEXT_GLOBAL) {
        int res = pthread_spin_lock(&contextPool->m_lock);
        if (res != 0) {
            MOT_REPORT_SYSTEM_ERROR_CODE(
                res, pthread_spin_lock, "Global JIT Context Allocation", "Failed to acquire spin lock for global pool");
            return NULL;
        }
    }

    MOT_LOG_TRACE("%s JIT context pool free-count before alloc context: %u",
        contextPool->m_usage == JIT_CONTEXT_GLOBAL ? "global" : "session-local",
        contextPool->m_freeContextCount);
    JitContext* result = contextPool->m_freeContextList;
    if (contextPool->m_freeContextList != nullptr) {
        contextPool->m_freeContextList = contextPool->m_freeContextList->m_next;
        --contextPool->m_freeContextCount;
    }
    MOT_LOG_DEBUG("%s JIT context pool free-count after alloc context: %u",
        contextPool->m_usage == JIT_CONTEXT_GLOBAL ? "global" : "session-local",
        contextPool->m_freeContextCount);

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

    if (result == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_RESOURCE_LIMIT,
            "JIT Context Allocation",
            "Failed to allocate %s JIT context: pool with %u context objects depleted",
            contextPool->m_usage == JIT_CONTEXT_GLOBAL ? "global" : "session-local",
            contextPool->m_poolSize);
    } else {
        errno_t erc = memset_s(result, sizeof(JitContext), 0, sizeof(JitContext));
        securec_check(erc, "\0", "\0");
        result->m_usage = contextPool->m_usage;
    }

    return result;
}

extern void FreePooledJitContext(JitContextPool* contextPool, JitContext* jitContext)
{
    if (contextPool == nullptr)
        return;
    if (contextPool->m_usage == JIT_CONTEXT_GLOBAL) {
        int res = pthread_spin_lock(&contextPool->m_lock);
        if (res != 0) {
            MOT_REPORT_SYSTEM_ERROR_CODE(res,
                pthread_spin_lock,
                "Global JIT Context De-allocation",
                "Failed to acquire spin lock for global pool");
            return;
        }
    }

    jitContext->m_next = contextPool->m_freeContextList;
    contextPool->m_freeContextList = jitContext;
    ++contextPool->m_freeContextCount;
    MOT_LOG_TRACE("%s JIT context pool free-count after free context: %u",
        contextPool->m_usage == JIT_CONTEXT_GLOBAL ? "global" : "session-local",
        contextPool->m_freeContextCount);

    if (contextPool->m_usage == JIT_CONTEXT_GLOBAL) {
        int res = pthread_spin_unlock(&contextPool->m_lock);
        if (res != 0) {
            MOT_REPORT_SYSTEM_ERROR_CODE(res,
                pthread_spin_unlock,
                "Global JIT Context De-allocation",
                "Failed to release spin lock for global pool");
            // system is in undefined state, we expect to crash any time soon, but we continue anyway
        }
    }
}
}  // namespace JitExec
