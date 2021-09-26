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
 * sentinel.h
 *    Primary/Secondary index sentinel.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/storage/sentinel.h
 *
 * -------------------------------------------------------------------------
 */

#pragma once

#ifndef MOT_SENTINEL_H
#define MOT_SENTINEL_H

#include <cstdint>
#include <iosfwd>
#include <ostream>

#include "key.h"
#include "index_defs.h"
#include "utilities.h"
#include "mot_atomic_ops.h"
#include "debug_utils.h"

namespace MOT {
// forward declarations
class Row;
class Index;

/**
 * @class Sentinel
 * @brief Primary/Secondary index sentinel.
 */
class Sentinel {
public:
    enum SentinelFlags : uint64_t {
        S_DIRTY_BIT = 1UL << 63,  // dirty bit
        S_LOCK_BIT = 1UL << 62,   // lock bit
        S_COUNTER_LOCK_BIT = 1UL << 31,
        S_PRIMARY_INDEX_BIT = 1UL << 61,
        S_STATUS_BITS = (S_DIRTY_BIT | S_LOCK_BIT | S_PRIMARY_INDEX_BIT),
        S_LOCK_OWNER_SIZE = 12,
        S_COUNTER_SIZE = 31,
        S_OBJ_ADDRESS_SIZE = 48,
        S_OBJ_ADDRESS_MASK = (1UL << S_OBJ_ADDRESS_SIZE) - 1,
        S_LOCK_OWNER_MASK = ((1UL << S_LOCK_OWNER_SIZE) - 1) << S_OBJ_ADDRESS_SIZE,
        S_LOCK_OWNER_RESIZE = (1UL << S_LOCK_OWNER_SIZE) - 1,
        S_COUNTER_MASK = ((1UL << S_COUNTER_SIZE) - 1),
        S_STATUS_MASK = ~S_STATUS_BITS
    };

    enum StableRowFlags : uint64_t { STABLE_BIT = 1UL << 63, PRE_ALLOC_BIT = 1UL << 62 };

    inline bool IsCommited() const
    {
        return (m_status & S_DIRTY_BIT) == 0;
    }

    inline bool IsDirty() const
    {
        return (m_status & S_DIRTY_BIT) == S_DIRTY_BIT;
    }

    inline void UnSetDirty()
    {
        m_status &= ~S_DIRTY_BIT;
    }

    inline void SetPrimaryIndex()
    {
        m_status |= S_PRIMARY_INDEX_BIT;
    }

    inline bool IsPrimaryIndex() const
    {
        return (m_status & S_PRIMARY_INDEX_BIT) == S_PRIMARY_INDEX_BIT;
    }

    inline bool IsCommitedORrPrimaryIndex() const
    {
        return (IsCommited() or IsPrimaryIndex());
    }

    inline bool IsCommitedRAndPrimaryIndex() const
    {
        return (IsCommited() and IsPrimaryIndex());
    }

    inline uint32_t GetCounter() const
    {
        return (m_refCount & S_COUNTER_MASK);
    }

    inline uint64_t GetLockOwner() const
    {
        return (m_status & S_LOCK_OWNER_SIZE) >> S_OBJ_ADDRESS_SIZE;
    }

    void SetUpgradeCounter()
    {
        LockRefCount();
        DecCounter();
        ReleaseRefCount();
        MOT_ASSERT(IsLocked() == true);
    }

    inline void SetIndex(Index* index)
    {
        m_index = index;
    }

    inline Index* GetIndex()
    {
        return m_index;
    }

    inline void SetDirty()
    {
        m_status |= S_DIRTY_BIT;
    }

    // Set dirty and init counter to 1
    inline void InitStatus()
    {
        m_status = S_DIRTY_BIT;
    }

    inline void InitCounter()
    {
        m_refCount |= 1;
    }

    inline void Release()
    {
        MOT_ASSERT(m_status & S_LOCK_BIT);
#ifdef MOT_DEBUG
        SetLockOwner(-1);
#endif
#if defined(__GNUC__) && (defined(__x86_64__) || defined(__i386__))
        m_status = m_status & (~S_LOCK_BIT);
#else

        uint64_t v = m_status;
        while (!__sync_bool_compare_and_swap(&m_status, v, (v & ~S_LOCK_BIT))) {
            PAUSE
            v = m_status;
        }
#endif
    }

    inline bool IsLocked() const
    {
        return (m_status & S_LOCK_BIT) == S_LOCK_BIT;
    }

    void Lock(uint64_t tid)
    {
        uint64_t v = m_status;
        while ((v & S_LOCK_BIT) || !__sync_bool_compare_and_swap(&m_status, v, v | S_LOCK_BIT)) {
            PAUSE
            v = m_status;
        }
#ifdef MOT_DEBUG
        SetLockOwner(tid);
#endif
    }

    bool TryLock(uint64_t tid)
    {
        uint64_t v = m_status;
        if (v & S_LOCK_BIT) {  // already locked
            return false;
        }
        bool res = __sync_bool_compare_and_swap(&m_status, v, (v | S_LOCK_BIT));
#ifdef MOT_DEBUG
        if (res) {
            SetLockOwner(tid);
        }
#endif
        return res;
    }

    void LockRefCount()
    {
        uint32_t v = m_refCount;
        while ((v & S_COUNTER_LOCK_BIT) || !__sync_bool_compare_and_swap(&m_refCount, v, v | S_COUNTER_LOCK_BIT)) {
            PAUSE
            v = m_refCount;
        }
    }

    inline void ReleaseRefCount()
    {
        MOT_ASSERT(m_refCount & S_COUNTER_LOCK_BIT);

#if defined(__GNUC__) && (defined(__x86_64__) || defined(__i386__))
        m_refCount = m_refCount & (~S_COUNTER_LOCK_BIT);
#else

        uint64_t v = m_refCount;
        while (!__sync_bool_compare_and_swap(&m_refCount, v, (v & ~S_COUNTER_LOCK_BIT))) {
            PAUSE
            v = m_refCount;
        }
#endif
    }

    RC RefCountUpdate(AccessType type, uint64_t tid);

    /**
     * @brief Sets the next versioned data pointer.
     * @param ptr The next data pointer to set.
     */
    void Init(Index* index, Row* row)
    {
        m_index = index;
        InitStatus();
        InitCounter();
        SetNextPtr(row);
        m_stable = 0;
    }
    /**
     * @brief Set the object pointer for the sentinel
     *        PS - points to a row
     *        SS - points to a secondary sentinel
     * @param ptr - the object pointer
     */
    void SetNextPtr(void* const& ptr)
    {
        m_status = (m_status & ~S_OBJ_ADDRESS_MASK) | ((uint64_t)(ptr)&S_OBJ_ADDRESS_MASK);
    }

    /**
     * @brief Retrieves the data associated with the sentinel index entry.
     * @return The sentinel data.
     */
    Row* GetData(void) const
    {
        if (IsPrimaryIndex()) {
            return reinterpret_cast<Row*>((uint64_t)(m_status & S_OBJ_ADDRESS_MASK));
        } else {
            Sentinel* s = reinterpret_cast<Sentinel*>((uint64_t)(m_status & S_OBJ_ADDRESS_MASK));
            if (s != nullptr) {
                return s->GetData();
            } else {
                return nullptr;
            }
        }
    }

    /**
     * @brief Retrieves the key associated with the sentinel index
     *    	entry.
     * @return The primary sentinel address.
     */
    void* GetPrimarySentinel() const
    {
        if (IsPrimaryIndex()) {
            return reinterpret_cast<void*>(const_cast<Sentinel*>(this));
        } else {
            return reinterpret_cast<void*>((uint64_t)(m_status & S_OBJ_ADDRESS_MASK));
        }
    }

    inline bool GetStableStatus() const
    {
        return (m_stable & STABLE_BIT) == STABLE_BIT;
    }

    inline void SetStableStatus(bool val)
    {
        if (val == true) {
            m_stable |= STABLE_BIT;
        } else {
            m_stable &= ~STABLE_BIT;
        }
    }

    /**
     * @brief Sets the current row as stable for CALC
     */
    void SetStable(Row* const& row)
    {
        m_stable = (m_stable & ~S_OBJ_ADDRESS_MASK) | ((uint64_t)(row)&S_OBJ_ADDRESS_MASK);
    }

    /**
     * @brief Retrieves the stable row
     * @return The row object.
     */
    Row* GetStable()
    {
        return reinterpret_cast<Row*>((uint64_t)(m_stable & S_OBJ_ADDRESS_MASK));
    };

    inline bool GetStablePreAllocStatus() const
    {
        return (m_stable & PRE_ALLOC_BIT) == PRE_ALLOC_BIT;
    }

    inline void SetStablePreAllocStatus(bool val)
    {
        if (val == true) {
            m_stable |= PRE_ALLOC_BIT;
        } else {
            m_stable &= ~PRE_ALLOC_BIT;
        }
    }

    void SetLockOwner(uint64_t tid)
    {
        MOT_ASSERT(m_status & S_LOCK_BIT);
        uint64_t owner = tid & S_LOCK_OWNER_RESIZE;
        m_status = (m_status & ~S_LOCK_OWNER_MASK) | (owner << S_OBJ_ADDRESS_SIZE);
    }

private:
    /** @var m_status Contains, the pointer and the status bit   */
    volatile uint64_t m_status = 0;

    /** @var m_index The index the sentinel belongs to   */
    Index* m_index = nullptr;

    /** @var m_stable A Container for the row and its status bit */
    volatile uint64_t m_stable = 0;

    /** @var m_refCount A counter of concurrent inserters of the same key  */
    volatile uint32_t m_refCount = 0;

    inline void DecCounter()
    {
        MOT_ASSERT(GetCounter() > 0);
        uint32_t counter = GetCounter() - 1;
        m_refCount = (m_refCount & ~S_COUNTER_MASK) | (counter & S_COUNTER_MASK);
    }

    inline void IncCounter()
    {
        uint32_t counter = GetCounter() + 1;
        m_refCount = (m_refCount & ~S_COUNTER_MASK) | (counter & S_COUNTER_MASK);
    }
};
}  // namespace MOT

#endif  // MOT_SENTINEL_H
