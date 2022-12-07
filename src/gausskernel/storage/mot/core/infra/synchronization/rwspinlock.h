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
 * rwspinlock.h
 *    Implements a reader/writer spin lock.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/synchronization/rwspinlock.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef RWSPINLOCK_H
#define RWSPINLOCK_H

#include <pthread.h>

/** @define Helper macro for atomic increment of integer value. */
#define atomic_inc(P) __atomic_fetch_add_4((P), 1, __ATOMIC_ACQ_REL)

/** @define Helper macro for atomic decrement of integer value. */
#define atomic_dec(P) __atomic_fetch_add_4((P), -1, __ATOMIC_ACQ_REL)

#ifndef cpu_relax
/** @define Helper macro for CPU NOOP without memory bus overloading. */
/* Pause instruction to prevent excess processor bus usage */
#if defined(__x86_64__) || defined(__x86__)
#define cpu_relax() asm volatile("pause\n" : : : "memory")
#else                                                // some web says yield:::memory
#define cpu_relax() asm volatile("" : : : "memory")  // equivalent to "rep; nop"
#endif
#endif

/** @define Value denoting busy lock state (i.e. locked). */
#define MEBUSY 1

/** @define Helper macro defining inline functions. */
#define __LOCK_INLINE inline __attribute__((always_inline))

namespace MOT {
// NOTE: it is possible to bring in exponential back-off spin lock here
/** @typedef Spin lock type (simple unsigned integer). */
typedef unsigned spinlock_t; /* not volatile? */

/**
 * @brief Atomically exchange the contents of a memory location with another value.
 * @param ptr The memory location.
 * @param x The new value.
 * @return The value previously stored in the memory location.
 */
static __LOCK_INLINE unsigned xchg_32_asm(void* ptr, unsigned x)
{
    __asm__ __volatile__("xchgl %0,%1" : "=r"((unsigned)x) : "m"(*(volatile unsigned*)ptr), "0"(x) : "memory");

    return x;
}

/** @define Helper macro for atomic value exchange while imposing full CPU barrier. */
#define xchg_32(ptr, x) __atomic_exchange_4(ptr, x, __ATOMIC_ACQ_REL)

/**
 * @brief Initializes a spin-lock.
 * @param lock The spin lock to initialize.
 */
static __LOCK_INLINE void spinlock_init(spinlock_t* lock)
{
    *lock = 0;
}

/**
 * @brief Locks a spin lock. The call imposes a full barrier.
 * @param lock The spin lock to lock.
 */
static __LOCK_INLINE void spinlock_lock(spinlock_t* lock)
{
    for (;;) {
        // try to lock - put MEBUSY in lock, if previous value was zero then we are the owners
        if (!xchg_32(lock, MEBUSY))
            return;

        // busy wait until lock is released
        // attention: previous instruction imposes a barrier - so no out of order execution is expected
        // also we don't use atomic load to avoid CPU performance degradation.
        while (*lock)
            // no-op without memory bus overloading
            cpu_relax();
    }
}

/**
 * @brief Unlocks a spin lock. <b>No barrier is imposed.</b>
 * @param lock The spin lock to unlock.
 */
static __LOCK_INLINE void spinlock_unlock(spinlock_t* lock)
{
    *lock = 0;
}

/**
 * @brief Attempts to lock a spin lock. The call imposes a full barrier.
 * @param lock The spin-lock to lock
 * @return Zero if lock was acquired, otherwise EBUSY.
 */
static __LOCK_INLINE int spinlock_trylock(spinlock_t* lock)
{
    // conform to pthread_mutex_trylock semantics
    // try to lock - put MEBUSY in lock, if previous value was zero then we are the owners
    if (!xchg_32(lock, MEBUSY))
        return 0;

    // lock held by someone else
    return EBUSY;
}

/**
 * @brief Queries whether a spin lock is locked. <b>No barrier is imposed.</b>
 * @param lock The spin-lock to query its state.
 * @return Non-zero value if the spin-lock is locked, otherwise zero.
 */
static __LOCK_INLINE bool spinlock_is_locked(spinlock_t* lock)
{
    return (*lock == MEBUSY);
}

/** @struct Read-write spin lock. */
struct rwspinlock_t {
    /** @var Lock for synchronization. */
    spinlock_t lock;

    /** @var Number of readers inside the lock. */
    int readers;
};

/**
 * @brief Initializes a read-write spin lock.
 * @param rwspin The read-write spin lock to initialize.
 */
static __LOCK_INLINE void rwspin_init(rwspinlock_t* rwspin)
{
    rwspin->lock = 0;
    rwspin->readers = 0;
}

/**
 * @brief Locks for reading a read-write spin lock.
 * @details Readers do not interfere with each others.
 * @param rwspin The read-write spin lock to lock for reading.
 */
static __LOCK_INLINE void rwspin_rdlock(rwspinlock_t* rwspin)
{
    for (;;) {
        // Speculatively take read lock
        atomic_inc(&rwspin->readers);

        // check if no writer is present
        if (!spinlock_is_locked(&rwspin->lock))
            return;

        // failed locking - back off
        atomic_dec(&rwspin->readers);

        // wait until we can try again
        while (rwspin->lock)
            cpu_relax();
    }
}

/**
 * @brief Releases a reader lock from the read-write spin lock.
 * @param rwspin The read-write spin lock to release.
 */
static __LOCK_INLINE void rwspin_rdunlock(rwspinlock_t* rwspin)
{
    atomic_dec(&rwspin->readers);
}

/**
 * @brief Locks for writing a read-write spin lock.
 * @details After this call is made all subsequent readers and writers wait for this writer to
 * finish.
 * @param rwspin The read-write spin lock to lock for reading.
 */
static __LOCK_INLINE void rwspin_wrlock(rwspinlock_t* rwspin)
{
    // Get write lock (call imposes a barrier)
    spinlock_lock(&rwspin->lock);

    // Wait for current readers to finish
    while (rwspin->readers)
        cpu_relax();
}

/**
 * @brief Releases a writer lock from the read-write spin lock. <b>No barrier is imposed.</b>
 * @param rwspin The read-write spin lock to release.
 */
static __LOCK_INLINE void rwspin_wrunlock(rwspinlock_t* rwspin)
{
    spinlock_unlock(&rwspin->lock);
}

/**
 * @brief Attempts to lock for reading a read-write spin lock.
 * @param rwspin The read-write spin lock to lock for reading.
 * @return Zero if lock was acquired, otherwise EBUSY.
 */
static __LOCK_INLINE int rwspin_try_rdlock(rwspinlock_t* rwspin)
{
    // Speculatively take read lock (generates barrier)
    atomic_inc(&rwspin->readers);

    // check if succeeded
    if (!rwspin->lock)
        return 0;

    // failed locking - back off
    atomic_dec(&rwspin->readers);

    // conform to pthread conventions
    return EBUSY;
}

/**
 * @brief Attempts to lock for writing a read-write spin lock.
 * @param rwspin The read-write spin lock to lock for reading.
 * @return Zero if lock was acquired, otherwise EBUSY.
 */
static __LOCK_INLINE int rwspin_try_wrlock(rwspinlock_t* rwspin)
{
    // fail immediately if readers are present
    if (rwspin->readers)
        return EBUSY;

    // Try to get write lock
    if (spinlock_trylock(&rwspin->lock) == EBUSY)
        return EBUSY;

    // check again if readers slipped in somehow
    if (rwspin->readers) {
        // Oops, a reader started, so back off
        spinlock_unlock(&rwspin->lock);
        return EBUSY;
    }

    // Success
    return 0;
}

/**
 * @brief Promotes a read-write spin lock from read lock to write lock.
 * @param rwspin The read-write spin lock to promote.
 */
static __LOCK_INLINE void rwspin_promote(rwspinlock_t* rwspin)
{
    // Convert into a write lock
    spinlock_lock(&rwspin->lock);

    // I am no longer a reader
    atomic_dec(&rwspin->readers);

    // Wait for all other readers to finish
    while (rwspin->readers)
        cpu_relax();
}

/**
 * @brief Attempts to promote a read-write spin lock from a read lock to a write lock.
 * @details The attempt contends with other promote attempts, but still waits for existing readers
 * to finish.
 * @param rwspin The read-write spin lock to promote.
 * @return Zero if lock was acquired, otherwise EBUSY.
 */
static __LOCK_INLINE int rwspin_try_promote(rwspinlock_t* rwspin)
{
    // Try to convert into a write lock
    if (spinlock_trylock(&rwspin->lock))
        return EBUSY;

    // I am no longer a reader
    atomic_dec(&rwspin->readers);

    // Wait for all other readers to finish
    while (rwspin->readers)
        cpu_relax();

    // success
    return 0;
}

/**
 * @brief Demotes a read-write spin lock from write lock to read lock.
 * @param rwspin The read-write spin lock to demote.
 */
static __LOCK_INLINE void rwspin_demote(rwspinlock_t* rwspin)
{
    // be the first reader
    // barge-in even before existing writers waiting for the write lock!
    atomic_inc(&rwspin->readers);

    // no longer a writer
    spinlock_unlock(&rwspin->lock);

    // other writers will continue waiting until I finish reading
}
}  // namespace MOT

#endif /* RWSPINLOCK_H */
