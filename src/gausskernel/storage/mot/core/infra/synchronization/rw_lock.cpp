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
 * rw_lock.cpp
 *    Implements a reader/writer lock using spinlock.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/synchronization/rw_lock.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "rw_lock.h"

namespace MOT {
RwLock::RwLock() : m_lock(0), m_readers(0)
{}

#define ATOMIC_XADD(P, V) __sync_fetch_and_add((P), (V))
#define CMPXCHG(P, O, N) __sync_val_compare_and_swap((P), (O), (N))
#define ATOMIC_INC(P) __sync_add_and_fetch((P), 1)
#define ATOMIC_DEC(P) __sync_add_and_fetch((P), -1)
#define ATOMIC_ADD(P, V) __sync_add_and_fetch((P), (V))
#define ATOMIC_SET_BIT(P, V) __sync_or_and_fetch((P), 1 << (V))
#define ATOMIC_CLEAR_BIT(P, V) __sync_and_and_fetch((P), ~(1 << (V)))

/* Pause instruction to prevent excess processor bus usage */
#if defined(__x86_64__) || defined(__x86__)
#define CPU_RELAX() asm volatile("pause\n" : : : "memory")
#else                                                // SG taken from compiler.hh;; some web says yield:::memory
#define CPU_RELAX() asm volatile("" : : : "memory")  // equivalent to "rep; nop"
#endif

/* Compile read-write barrier */
#define BARRIER() asm volatile("" : : : "memory")

const int ME_BUSY = 1;

unsigned RwLock::LockXchg32(void* ptr, unsigned x)
{
#if defined(__x86_64__) || defined(__x86__)
    __asm__ __volatile__("xchgl %0,%1" : "=r"((unsigned)x) : "m"(*(volatile unsigned*)ptr), "0"(x) : "memory");
#else  // SG
    x = __atomic_exchange_n((unsigned*)ptr, x, __ATOMIC_SEQ_CST);
#endif
    return x;
}

void RwLock::SpinLock()
{
    while (1) {
        if (!LockXchg32(&m_lock, ME_BUSY)) {
            return;
        }

        while (m_lock) {
            CPU_RELAX();
        }
    }
}

void RwLock::SpinUnlock()
{
    BARRIER();
    m_lock = 0;
}

int RwLock::SpinTryLock()
{
    return LockXchg32(&m_lock, ME_BUSY);
}

void RwLock::WrLock()
{
    /* Get write lock */
    SpinLock();

    /* Wait for readers to finish */
    while (m_readers) {
        CPU_RELAX();
    }
}

void RwLock::WrUnlock()
{
    SpinUnlock();
}

int RwLock::WrTryLock()
{
    /* Want no readers */
    if (m_readers) {
        return ME_BUSY;
    }

    /* Try to get write lock */
    if (SpinTryLock()) {
        return ME_BUSY;
    }

    if (m_readers) {
        /* Oops, a reader started */
        SpinUnlock();
        return ME_BUSY;
    }

    /* Success! */
    return 0;
}

void RwLock::RdLock()
{
    while (1) {
        /* Speculatively take read lock */
        ATOMIC_INC(&m_readers);

        /* Success? */
        if (!m_lock) {
            return;
        }

        /* Failure - undo, and wait until we can try again */
        ATOMIC_DEC(&m_readers);
        while (m_lock) {
            CPU_RELAX();
        }
    }
}

void RwLock::RdUnlock()
{
    ATOMIC_DEC(&m_readers);
}

int RwLock::RdTryLock()
{
    /* Speculatively take read lock */
    ATOMIC_INC(&m_readers);

    /* Success? */
    if (!m_lock) {
        return 0;
    }

    /* Failure - undo */
    ATOMIC_DEC(&m_readers);

    return ME_BUSY;
}

int RwLock::RdUpgradeLock()
{
    /* Try to convert into a write lock */
    if (SpinTryLock()) {
        return ME_BUSY;
    }

    /* I'm no longer a reader */
    ATOMIC_DEC(&m_readers);

    /* Wait for all other readers to finish */
    while (m_readers) {
        CPU_RELAX();
    }

    return 0;
}
}  // namespace MOT
