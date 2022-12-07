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
 * gc_shared_info.h
 *    GC concurrent reclaim structure.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/storage/gc_shared_info.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef GC_SHARED_INFO_H
#define GC_SHARED_INFO_H

#include "cycles.h"
#include "mot_atomic_ops.h"
#include "debug_utils.h"
#include "mm_def.h"
#include <atomic>

namespace MOT {

enum GcInfoFlags : uint64_t {
    GC_INFO_LOCK_BIT = 1UL << 63,
    GC_INFO_CSN_SIZE = 63,
    GC_INFO_CSN_MASK = (1UL << GC_INFO_CSN_SIZE) - 1,
    GC_INFO_COUNTER_SIZE = 31,
    GC_INFO_COUNTER_LOCK_BIT = 1UL << 31,
    GC_INFO_COUNTER_MASK = ((1UL << GC_INFO_COUNTER_SIZE) - 1),
};

/**
 * @class GcSharedInfo
 * @brief GC descriptor for primary sentinel
 */

class GcSharedInfo {
public:
    inline uint64_t GetMinCSN() const
    {
        return m_minCsn & GC_INFO_CSN_MASK;
    }

    void SetMinCSN(uint64_t min_csn)
    {
        m_minCsn = (m_minCsn & GC_INFO_LOCK_BIT) | min_csn;
    }

    inline uint32_t GetCounter() const
    {
        return (m_refCount.load(std::memory_order_relaxed));
    }

    void Lock()
    {
        uint64_t v = m_minCsn;
        while ((v & GC_INFO_LOCK_BIT) || !__sync_bool_compare_and_swap(&m_minCsn, v, v | GC_INFO_LOCK_BIT)) {
            PAUSE
            v = m_minCsn;
        }
    }

    inline void Release()
    {
        MOT_ASSERT(m_minCsn & GC_INFO_LOCK_BIT);

#if defined(__GNUC__) && (defined(__x86_64__) || defined(__i386__))
        m_minCsn = m_minCsn & (~GC_INFO_LOCK_BIT);
#else

        uint64_t v = m_minCsn;
        while (!__sync_bool_compare_and_swap(&m_minCsn, v, (v & ~GC_INFO_LOCK_BIT))) {
            PAUSE
            v = m_minCsn;
        }
#endif
    }

    bool TryLock()
    {
        uint64_t v = m_minCsn;
        if (v & GC_INFO_LOCK_BIT) {  // already locked
            return false;
        }
        return __sync_bool_compare_and_swap(&m_minCsn, v, (v | GC_INFO_LOCK_BIT));
    }

    uint32_t RefCountUpdate(AccessType type)
    {
        if (type == INC) {
            return m_refCount.fetch_add(1, std::memory_order_relaxed);
        } else {
            MOT_ASSERT(GetCounter() != 0);
            return m_refCount.fetch_sub(1, std::memory_order_relaxed);
        }
    }

private:
    /** @var m_minCsn The minimal CSN where this version was reclaimed   */
    uint64_t m_minCsn = 0;

    /** @var m_refCount Reference count for the current reclaimed version   */
    std::atomic<uint32_t> m_refCount{0};
};

}  // namespace MOT

#endif  // GC_SHARED_INFO_H
