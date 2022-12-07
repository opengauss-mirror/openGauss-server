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
 * jit_source_pool.cpp
 *    Global pool of JIT sources.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/jit_exec/jit_source_pool.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "global.h"
#include "postgres.h"
#include "utils/memutils.h"
#include "storage/mot/jit_exec.h"

#include "jit_source_pool.h"
#include "utilities.h"
#include "mm_global_api.h"

namespace JitExec {
DECLARE_LOGGER(JitSourcePool, JitExec)

/** @struct JitSourcePool */
struct __attribute__((packed)) JitSourcePool {
    /** @var Synchronize global pool access. */
    pthread_spinlock_t m_lock;  // 4 bytes

    /** @var Insert padding to keep noisy lock isolated in its own cache line. */
    uint8_t m_padding1[60];

    /** @var The pool of all JIT sources. */
    JitSource* m_sourcePool;

    /** @var The list of free JIT sources. */
    JitSource* m_freeSourceList;

    /** @var The total pool size. */
    uint32_t m_poolSize;

    /** @var The amount of free JIT sources. */
    uint32_t m_freeSourceCount;

    /** @var Keep the total struct size as 2 cache lines. */
    uint64_t m_padding2[5];
};

// Global variables
static JitSourcePool g_jitSourcePool __attribute__((aligned(64))) = {0};

static bool g_isFirstBreach = true;

// forward declarations
static void FreeJitSourceArray();

extern bool InitJitSourcePool(uint32_t poolSize)
{
    MOT_LOG_TRACE("Initializing global JIT source pool with size: %u", poolSize);
    MOT_ASSERT(g_jitSourcePool.m_sourcePool == nullptr);
    errno_t erc = memset_s((void*)&g_jitSourcePool, sizeof(JitSourcePool), 0, sizeof(JitSourcePool));
    securec_check(erc, "\0", "\0");
    int res = pthread_spin_init(&g_jitSourcePool.m_lock, 0);
    if (res != 0) {
        MOT_REPORT_SYSTEM_ERROR_CODE(res,
            pthread_spin_init,
            "JIT Source Pool Initialization",
            "Failed to initialize spin lock for global JIT Source pool");
        return false;
    }

    size_t allocSize = sizeof(JitSource) * poolSize;
    g_jitSourcePool.m_sourcePool = (JitSource*)MOT::MemGlobalAllocAligned(allocSize, L1_CACHE_LINE);
    if (g_jitSourcePool.m_sourcePool == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "JIT Source Pool Initialization",
            "Failed to allocate %u JIT source objects (64-byte aligned %u bytes) for global JIT source pool",
            poolSize,
            allocSize);
        (void)pthread_spin_destroy(&g_jitSourcePool.m_lock);
        return false;
    }
    erc = memset_s(g_jitSourcePool.m_sourcePool, allocSize, 0, allocSize);
    securec_check(erc, "\0", "\0");

    // fill the free list
    g_jitSourcePool.m_poolSize = poolSize;
    for (uint32_t i = 0; i < g_jitSourcePool.m_poolSize; ++i) {
        JitSource* jitSource = &g_jitSourcePool.m_sourcePool[i];
        jitSource->m_sourceId = i;
        jitSource->m_next = g_jitSourcePool.m_freeSourceList;
        g_jitSourcePool.m_freeSourceList = jitSource;
    }
    g_jitSourcePool.m_freeSourceCount = g_jitSourcePool.m_poolSize;

    return true;
}

extern void DestroyJitSourcePool()
{
    MOT_LOG_TRACE("Destroying global JIT source pool");
    if (g_jitSourcePool.m_sourcePool == nullptr) {
        return;
    }

    FreeJitSourceArray();

    int res = pthread_spin_destroy(&g_jitSourcePool.m_lock);
    if (res != 0) {
        MOT_REPORT_SYSTEM_ERROR_CODE(res,
            pthread_spin_destroy,
            "JIT Source Pool Destruction",
            "Failed to destroy spin lock for global JIT source pool");
    }

    g_jitSourcePool.m_sourcePool = nullptr;
    g_jitSourcePool.m_poolSize = 0;
    g_jitSourcePool.m_freeSourceList = nullptr;
    g_jitSourcePool.m_freeSourceCount = 0;
}

/** @brief Returns a JIT source to the pool. */
static void FreePooledJitSource(JitSource* jitSource)
{
    MOT_LOG_TRACE("Freeing JIT source %p", jitSource);
    int res = pthread_spin_lock(&g_jitSourcePool.m_lock);
    if (res != 0) {
        MOT_REPORT_SYSTEM_ERROR_CODE(res,
            pthread_spin_lock,
            "Global JIT Source De-allocation",
            "Failed to acquire spin lock for global JIT source pool");
        return;
    }

    jitSource->m_next = g_jitSourcePool.m_freeSourceList;
    g_jitSourcePool.m_freeSourceList = jitSource;
    ++g_jitSourcePool.m_freeSourceCount;
    g_isFirstBreach = true;

    res = pthread_spin_unlock(&g_jitSourcePool.m_lock);
    if (res != 0) {
        MOT_REPORT_SYSTEM_ERROR_CODE(res,
            pthread_spin_unlock,
            "Global JIT Source De-allocation",
            "Failed to release spin lock for global JIT source pool");
        // system is in undefined state, we expect to crash any time soon, but we continue anyway
    }
}

/** @brief Allocates a JIT source from the pool. */
static JitSource* AllocPooledJitSource(const char* queryString, JitContextUsage usage)
{
    bool issueWarningOnFailure = false;
    MOT_LOG_TRACE("Allocating JIT source for query: %s", queryString);
    int res = pthread_spin_lock(&g_jitSourcePool.m_lock);
    if (res != 0) {
        MOT_REPORT_SYSTEM_ERROR_CODE(res,
            pthread_spin_lock,
            "Global JIT Source Allocation",
            "Failed to acquire spin lock for global JIT source pool");
        return NULL;
    }

    JitSource* result = g_jitSourcePool.m_freeSourceList;
    if (g_jitSourcePool.m_freeSourceList != nullptr) {
        g_jitSourcePool.m_freeSourceList = g_jitSourcePool.m_freeSourceList->m_next;
        --g_jitSourcePool.m_freeSourceCount;
    }
    if ((result == nullptr) && g_isFirstBreach) {
        issueWarningOnFailure = true;
        g_isFirstBreach = false;
    }

    res = pthread_spin_unlock(&g_jitSourcePool.m_lock);
    if (res != 0) {
        MOT_REPORT_SYSTEM_ERROR_CODE(res,
            pthread_spin_unlock,
            "Global JIT Source Allocation",
            "Failed to release spin lock for global JIT source pool");
        // system is in undefined state, we expect to crash any time soon, but we continue anyway
    }

    if (result == nullptr) {
        if (issueWarningOnFailure) {
            MOT_LOG_WARN("Cannot allocate JIT source, reached configured limit: %u. Consider increasing the value "
                         "of 'mot_codegen_limit' in mot.conf.",
                GetMotCodegenLimit());
        }
    } else {
        if (!InitJitSource(result, queryString, usage)) {
            MOT_LOG_WARN("Failed to initialize JIT source");
            FreePooledJitSource(result);
            result = nullptr;
        }
    }

    return result;
}

extern JitSource* AllocJitSource(const char* queryString, JitContextUsage usage)
{
    if (usage == JIT_CONTEXT_GLOBAL) {
        return AllocPooledJitSource(queryString, usage);
    }

    size_t allocSize = sizeof(JitSource);
    JitSource* jitSource = (JitSource*)MOT::MemSessionAlloc(allocSize);
    if (jitSource == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Local JIT Source Allocation",
            "Failed to allocate %" PRIu64 " bytes for local JIT source",
            allocSize);
        return nullptr;
    }

    if (!InitJitSource(jitSource, queryString, usage)) {
        MOT_LOG_WARN("Failed to initialize JIT source");
        MOT::MemSessionFree(jitSource);
        jitSource = nullptr;
    }

    return jitSource;
}

extern void FreeJitSource(JitSource* jitSource)
{
    MOT_ASSERT(!jitSource->m_initialized);
    if (jitSource->m_usage == JIT_CONTEXT_GLOBAL) {
        FreePooledJitSource(jitSource);
    } else {
        MOT::MemSessionFree(jitSource);
    }
}

static void FreeJitSourceArray()
{
    MOT_LOG_TRACE("Freeing JIT source array");
    // destroy whatever is left
    for (uint32_t i = 0; i < g_jitSourcePool.m_poolSize; ++i) {
        JitSource* jitSource = &g_jitSourcePool.m_sourcePool[i];
        if (jitSource->m_initialized) {
            MOT_LOG_WARN("Found JIT source %p id %u not destroyed yet: %s", jitSource, i, jitSource->m_queryString);
            (void)CleanUpDeprecateJitSourceContexts(jitSource);
            DestroyJitSource(jitSource);
        }
    }
    MOT::MemGlobalFree(g_jitSourcePool.m_sourcePool);
    g_jitSourcePool.m_sourcePool = nullptr;
}
}  // namespace JitExec
