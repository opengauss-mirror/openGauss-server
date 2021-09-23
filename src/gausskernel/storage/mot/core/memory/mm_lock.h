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
 * mm_lock.h
 *    Memory management buffer lock implementation.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/memory/mm_lock.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef MM_LOCK_H
#define MM_LOCK_H

#include "mm_def.h"
#include "mot_error.h"

#include <pthread.h>
#include <errno.h>

namespace MOT {
/** @struct MemLock L1-cache line isolated lock. */
struct PACKED MemLock {
    union {
        /** @var The aligned and cache-line-isolated lock. */
        pthread_spinlock_t m_lock;

        /** @var Padding to ensure cache-line isolation of the lock. */
        uint8_t m_pad[L1_CACHE_LINE];
    };
};

/** @typedef MemLockOp Lock operation type for error reporting. */
enum MemLockOp {
    /** @var Lock initialization. */
    MEM_LOCK_OP_INIT,

    /** @var Lock destruction. */
    MEM_LOCK_OP_DESTROY,

    /** @var Try to acquire lock operation. */
    MEM_LOCK_OP_TRY_ACQUIRE
};

/**
 * @brief Helper function for reporting lock errors.
 * @param rc The return code of the failed system call.
 * @param lock_op The lock operation type.
 * @return A translation of the system error code to MM Engine internal error code.
 */
extern int MemLockReportError(int rc, MemLockOp lockPp);

/**
 * @brief Initializes the lock.
 * @param lock The lock to initialize.
 * @return Zero if succeeded, otherwise an error code.
 */
inline int MemLockInitialize(MemLock* lock)
{
    int result = 0;
    int rc = pthread_spin_init(&lock->m_lock, 0);
    if (rc != 0) {
        result = MemLockReportError(rc, MEM_LOCK_OP_INIT);
    }
    return result;
}

/**
 * @brief Destroys the buffer lock.
 * @param lock The buffer lock to destroy.
 * @return Zero if succeeded, otherwise an error code.
 */
inline int MemLockDestroy(MemLock* lock)
{
    int result = 0;
    int rc = pthread_spin_destroy(&lock->m_lock);
    if (rc != 0) {
        result = MemLockReportError(rc, MEM_LOCK_OP_DESTROY);
    }
    return result;
}

/**
 * @brief Locks the buffer lock.
 * @param lock The buffer lock to lock.
 */
inline void MemLockAcquire(MemLock* lock)
{
    pthread_spin_lock(&lock->m_lock);
}

/**
 * @brief Attempts to lock the buffer lock.
 * @param lock The buffer lock to lock.
 * @return Zero if attempt was successful otherwise error code denoting reason to failure.
 */
inline int MemLockTryAcquire(MemLock* lock)
{
    int result = 0;
    int rc = pthread_spin_trylock(&lock->m_lock);
    if (rc != 0) {
        if (rc == EBUSY) {
            result = MOT_ERROR_RESOURCE_UNAVAILABLE;
        } else {
            result = MemLockReportError(rc, MEM_LOCK_OP_TRY_ACQUIRE);
        }
    }
    return result;
}

/**
 * @brief Unlocks the buffer lock.
 * @param lock The buffer lock to unlock.
 * @return Zero if succeeded, otherwise an error code.
 */
inline void MemLockRelease(MemLock* lock)
{
    pthread_spin_unlock(&lock->m_lock);
}
}  // namespace MOT

#endif /* MM_LOCK_H */
