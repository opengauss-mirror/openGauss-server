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
 * jit_context_pool.h
 *    A pool of JIT context objects.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/jit_exec/jit_context_pool.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef JIT_CONTEXT_POOL_H
#define JIT_CONTEXT_POOL_H

#include <pthread.h>

#include "jit_context.h"

namespace JitExec {
/** @struct A sub-pool of JIT context objects. */
struct __attribute__((packed)) JitContextSubPool {
    /** @var The array of pooled JIT context objects. */
    MotJitContext* m_contextPool;  // L1 offset 0

    /** @var The list of free JIT context objects. */
    MotJitContext* m_freeContextList;  // L1 offset 8

    /** @var The number of free JIT context objects. */
    uint32_t m_freeContextCount;  // L1 offset 16

    /** @var The unique pool identifier. */
    uint16_t m_subPoolId;  // L1 offset 20

    /** @var Align struct size to cache line. */
    uint8_t m_padding3[42];  // L1 offset 22

#ifdef MOT_JIT_DEBUG
    int* m_freeContextIds;
#endif
};

/** @define The size of each sub-pool is fixed at 256 items, so each sub-pool takes about 66 KB.  */
#define JIT_SUB_POOL_SIZE 256

/** @struct A pool of JIT context objects. */
struct __attribute__((packed)) JitContextPool {
    /** @var A lock to synchronize pool access. */
    pthread_spinlock_t m_lock;  // 4 bytes

    /** @var Keep noisy lock in its own cache line. */
    uint8_t m_padding1[60];

    /** @var The array of pooled JIT context objects. */
    JitContextSubPool** m_subPools;  // L1 offset 0

    /** @var The maximum size of the pool (number of objects). */
    uint32_t m_poolSize;  // L1 offset 8

    /** @var The maximum number of sub-pools. */
    uint16_t m_subPoolCount;  // L1 offset 12

    /** @var The usage of this context pool (global or session-local). */
    JitContextUsage m_usage;  // L1 offset 14 (1 byte)

    /** @var Align next pointer to 8 bytes. */
    uint8_t m_padding2[1];  // L1 offset 15

    /** @var The unique pool identifier. */
    uint32_t m_poolId;  // L1 offset 16

    /** @var Align struct size to cache line. */
    uint8_t m_padding3[44];  // L1 offset 20
};

/**
 * @brief Initializes a JIT context pool.
 * @param contextPool The JIT context pool to initialize.
 * @param usage The usage of the context objects in the pool (global or session-local).
 * @param poolSize The size of the pool.
 * @return True if initialization succeeded, otherwise false.
 */
extern bool InitJitContextPool(JitContextPool* contextPool, JitContextUsage usage, uint32_t poolSize);

/**
 * @brief Destroys a JIT context pool.
 * @param contextPool The JIT context pool to destroy.
 */
extern void DestroyJitContextPool(JitContextPool* contextPool);

/**
 * @brief Allocates a JIT context from a pool.
 * @param contextPool The JIT context pool from which to allocate a context object.
 * @return The JIT context if allocation succeeded, otherwise NULL. Consult @ref
 * mm_get_root_error() for futher details.
 */
MotJitContext* AllocPooledJitContext(JitContextPool* contextPool);

/**
 * @brief Returns a JIT context to a pool.
 * @param contextPool The JIT context pool into which the context object is to be returned.
 * @param jitContext The JIT context to return.
 */
extern void FreePooledJitContext(JitContextPool* contextPool, MotJitContext* jitContext);

/** @brief Retrieves the actual size in bytes that an allocated JIT context takes. */
extern uint32_t GetJitContextSize();
}  // namespace JitExec

#endif
