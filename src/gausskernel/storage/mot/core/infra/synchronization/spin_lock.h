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
 * spin_lock.h
 *    SpinLock implementation.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/synchronization/spin_lock.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef SPIN_LOCK_H
#define SPIN_LOCK_H

#include <atomic>
#include <mutex>
#include "mot_atomic_ops.h"

#define MOT_SPINLOCK_INLINE 1

#ifdef MOT_SPINLOCK_INLINE
#define SPINLOCK_INLINE inline __attribute__((always_inline))
#else
#define SPINLOCK_INLINE
#endif

namespace MOT {
/**
 * @class spin_lock
 *
 * @brief This class implements locks that never block the thread: if the lock
 * isn't available during a lock operation, the thread spins until the
 * lock becomes available.
 *
 * @details
 * common use of the class:
 *     spin_lock mutex;
 *     void a(){
 *       std::lock_guard<spin_lock> lock(mutex);
 *     }
 *
 * SpinLocks are intended for situations where locks are not held
 * for long periods of time, such as locks used for mutual exclusion.
 * These locks are not recursive: if a thread attempts
 * to lock a SpinLock while holding it, the thread will deadlock.
 *
 * This class implements the Boost "Lockable" concept, so SpinLocks can be
 * used with the Boost locking facilities.
 *
 */
class spin_lock {
public:
    explicit SPINLOCK_INLINE spin_lock() noexcept;

    /**
     * @brief Acquire the SpinLock; blocks the thread (by continuously polling
     * the lock) until the lock has been acquired.
     */
    SPINLOCK_INLINE void lock() noexcept;

    /**
     * @brief Try to acquire the SpinLock; does not block the thread and returns
     * immediately.
     *
     * @return True if the lock was successfully acquired, false if it was
     * already owned by some other thread.
     */
    SPINLOCK_INLINE bool try_lock() noexcept;

    /**
     * @brief Release the SpinLock.  The caller must previously have acquired
     * the lock with a call to #lock or #try_lock.
     */
    SPINLOCK_INLINE void unlock() noexcept;

    // class non-copy-able, non-assignable, non-movable
    /** @cond EXCLUDE_DOC */
    spin_lock(const spin_lock& orig) = delete;
    spin_lock(const spin_lock&& orig) = delete;
    spin_lock& operator=(const spin_lock& orig) = delete;
    spin_lock& operator=(const spin_lock&& orig) = delete;
    /** @endcond */

private:
    /** @var Implements the lock: False means free, True means locked. */
    volatile std::atomic_flag mutex;
};
}  // namespace MOT

#endif /* SPIN_LOCK_H */

#if defined(MOT_SPINLOCK_INLINE)
#include "spin_lock.cpp"
#endif
