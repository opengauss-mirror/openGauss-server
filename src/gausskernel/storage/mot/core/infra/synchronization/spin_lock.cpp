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
 * spin_lock.cpp
 *    SpinLock implementation.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/synchronization/spin_lock.cpp
 *
 * -------------------------------------------------------------------------
 */

#ifndef MOT_SPINLOCK_IMPLEMENTATION
#define MOT_SPINLOCK_IMPLEMENTATION

#include "spin_lock.h"

namespace MOT {
SPINLOCK_INLINE spin_lock::spin_lock() noexcept : mutex(0)
{}

SPINLOCK_INLINE void spin_lock::lock() noexcept
{
    while (mutex.test_and_set(std::memory_order_acquire)) {
        do {
            PAUSE
        } while (mutex._M_i != 0);
    }
}

SPINLOCK_INLINE bool spin_lock::try_lock() noexcept
{
    // test_and_set sets the flag to true and returns the previous value;
    // if it's True, someone else is owning the lock.
    return !mutex.test_and_set(std::memory_order_acquire);
}

SPINLOCK_INLINE void spin_lock::unlock() noexcept
{
    mutex.clear(std::memory_order_release);
}
}  // namespace MOT

#endif /* MOT_SPINLOCK_IMPLEMENTATION */
