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
 *    src/gausskernel/storage/mot/jit_exec/src/jit_source_pool.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "global.h"
#include "postgres.h"
#include "utils/memutils.h"

#include "jit_source_pool.h"
#include "utilities.h"

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

// Globals
static JitSourcePool g_jitSourcePool __attribute__((aligned(64)));

// forward declarations
static void FreeJitSourceArray(uint32_t count);

extern bool InitJitSourcePool(uint32_t poolSize)
{
    bool result = false;

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
    res = posix_memalign((void**)&g_jitSourcePool.m_sourcePool, 64, allocSize);
    if (res != 0) {
        MOT_REPORT_SYSTEM_ERROR_CODE(res,
            posix_memalign,
            "JIT Source Pool Initialization",
            "Failed to allocate 64-byte aligned %u bytes for global JIT Source pool",
            allocSize);
        pthread_spin_destroy(&g_jitSourcePool.m_lock);
        return false;
    }
    erc = memset_s(g_jitSourcePool.m_sourcePool, allocSize, 0, allocSize);
    securec_check(erc, "\0", "\0");

    if (g_jitSourcePool.m_sourcePool == NULL) {
        pthread_spin_destroy(&g_jitSourcePool.m_lock);
        MOT_REPORT_ERROR(
            MOT_ERROR_OOM, "JIT Source Pool Initialization", "Failed to allocate %u JIT source objects", poolSize);
    } else {
        // we need now to construct each object
        for (uint32_t i = 0; i < poolSize; ++i) {
            JitSource* jitSource = &g_jitSourcePool.m_sourcePool[i];
            if (!InitJitSource(jitSource, "")) {
                MOT_REPORT_ERROR(
                    MOT_ERROR_INTERNAL, "JIT Source Pool Initialization", "Failed to initialize JIT source %u", i);
                // cleanup
                FreeJitSourceArray(i);
                pthread_spin_destroy(&g_jitSourcePool.m_lock);
                return false;
            }
        }

        // fill the free list
        result = true;
        g_jitSourcePool.m_poolSize = poolSize;
        for (uint32_t i = 0; i < g_jitSourcePool.m_poolSize; ++i) {
            JitSource* jitSource = &g_jitSourcePool.m_sourcePool[i];
            jitSource->_next = g_jitSourcePool.m_freeSourceList;
            g_jitSourcePool.m_freeSourceList = jitSource;
        }
        g_jitSourcePool.m_freeSourceCount = g_jitSourcePool.m_poolSize;
    }
    return result;
}

extern void DestroyJitSourcePool()
{
    if (g_jitSourcePool.m_sourcePool == NULL)
        return;

    FreeJitSourceArray(0);

    int res = pthread_spin_destroy(&g_jitSourcePool.m_lock);
    if (res != 0) {
        MOT_REPORT_SYSTEM_ERROR_CODE(res,
            pthread_spin_destroy,
            "JIT Source Pool Destruction",
            "Failed to destroy spin lock for global JIT source pool");
    }

    g_jitSourcePool.m_sourcePool = NULL;
    g_jitSourcePool.m_poolSize = 0;
    g_jitSourcePool.m_freeSourceList = NULL;
    g_jitSourcePool.m_freeSourceCount = 0;
}

extern JitSource* AllocPooledJitSource(const char* queryString)
{
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
        g_jitSourcePool.m_freeSourceList = g_jitSourcePool.m_freeSourceList->_next;
        --g_jitSourcePool.m_freeSourceCount;
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
        MOT_REPORT_ERROR(MOT_ERROR_RESOURCE_LIMIT,
            "Global JIT Source Allocation",
            "Failed to allocate JIT source, reached configured limit");
    } else {
        ReInitJitSource(result, queryString);
    }

    return result;
}

extern void FreePooledJitSource(JitSource* jitSource)
{
    MOT_LOG_TRACE("Freeing JIT source %p with query: %s", jitSource, jitSource->_query_string);
    int res = pthread_spin_lock(&g_jitSourcePool.m_lock);
    if (res != 0) {
        MOT_REPORT_SYSTEM_ERROR_CODE(res,
            pthread_spin_lock,
            "Global JIT Source De-allocation",
            "Failed to acquire spin lock for global JIT source pool");
        return;
    }

    jitSource->_next = g_jitSourcePool.m_freeSourceList;
    g_jitSourcePool.m_freeSourceList = jitSource;
    ++g_jitSourcePool.m_freeSourceCount;

    res = pthread_spin_unlock(&g_jitSourcePool.m_lock);
    if (res != 0) {
        MOT_REPORT_SYSTEM_ERROR_CODE(res,
            pthread_spin_unlock,
            "Global JIT Source De-allocation",
            "Failed to release spin lock for global JIT source pool");
        // system is in undefined state, we expect to crash any time soon, but we continue anyway
    }
}

static void FreeJitSourceArray(uint32_t count)
{
    if (count == 0) {
        count = g_jitSourcePool.m_poolSize;
    }
    for (uint32_t i = 0; i < count; ++i) {
        DestroyJitSource(&g_jitSourcePool.m_sourcePool[i]);
    }
    free(g_jitSourcePool.m_sourcePool);
    g_jitSourcePool.m_sourcePool = NULL;
}
}  // namespace JitExec
