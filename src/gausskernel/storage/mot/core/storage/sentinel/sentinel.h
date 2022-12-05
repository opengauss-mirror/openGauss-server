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
 *    Base Index Sentinel
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
#include <chrono>
#include <thread>
#include <ostream>
#include "cycles.h"
#include "index_defs.h"
#include "utilities.h"
#include "mot_atomic_ops.h"
#include "debug_utils.h"
#include "mm_def.h"
#include "gc_shared_info.h"
#include "primary_sentinel_node.h"

namespace MOT {
// forward declarations
class Row;
class Index;
class PrimarySentinel;

const char* const enIndexOrder[] = {
    stringify(INDEX_ORDER_PRIMARY), stringify(INDEX_ORDER_SECONDARY), stringify(INDEX_ORDER_SECONDARY_UNIQUE)};

enum SentinelFlags : uint64_t {
    S_DIRTY_BIT = 1UL << 63,  // Dirty bit
    S_LOCK_BIT = 1UL << 62,   // Lock bit
    S_WRITE_BIT = 1UL << 61,  // Write bit
    S_LOCK_WRITE_BITS = (S_LOCK_BIT | S_WRITE_BIT),
    S_COUNTER_LOCK_BIT = 1UL << 31,
    S_STATUS_BITS = (S_DIRTY_BIT | S_LOCK_BIT),
    S_LOCK_OWNER_SIZE = 12,
    S_COUNTER_SIZE = 31,
    S_OBJ_ADDRESS_SIZE = 48,
    S_OBJ_ADDRESS_MASK = (1UL << S_OBJ_ADDRESS_SIZE) - 1,
    S_LOCK_OWNER_MASK = ((1UL << S_LOCK_OWNER_SIZE) - 1) << S_OBJ_ADDRESS_SIZE,
    S_LOCK_OWNER_RESIZE = (1UL << S_LOCK_OWNER_SIZE) - 1,
    S_COUNTER_MASK = ((1UL << S_COUNTER_SIZE) - 1),
    S_STATUS_MASK = ~S_STATUS_BITS
};

/**
 * @class Sentinel
 * @brief Base index sentinel.
 */
class Sentinel {
public:
    virtual ~Sentinel()
    {
        m_index = nullptr;
    }

    enum StableRowFlags : uint64_t { STABLE_BIT = 1UL << 63, PRE_ALLOC_BIT = 1UL << 62 };

    inline bool IsCommited() const
    {
        return (GetStatus() & S_DIRTY_BIT) == 0;
    }

    inline bool IsDirty() const
    {
        return (GetStatus() & S_DIRTY_BIT) == S_DIRTY_BIT;
    }

    inline void UnSetDirty()
    {
        m_status &= ~S_DIRTY_BIT;
    }

    inline bool IsPrimarySentinel() const
    {
        return (m_indexOrder == IndexOrder::INDEX_ORDER_PRIMARY);
    }

    inline void SetPrimaryIndex()
    {
        m_indexOrder = IndexOrder::INDEX_ORDER_PRIMARY;
    }

    inline void SetIndexOrder(IndexOrder order)
    {
        m_indexOrder = order;
    }

    inline IndexOrder GetIndexOrder() const
    {
        return m_indexOrder;
    }

    inline bool IsPrimaryIndex() const
    {
        return (m_indexOrder == IndexOrder::INDEX_ORDER_PRIMARY);
    }

    inline bool IsCommitedORrPrimaryIndex() const
    {
        return (IsCommited() or IsPrimaryIndex());
    }

    inline bool IsCommitedRAndSecondaryIndex() const
    {
        return (IsCommited() and !IsPrimaryIndex());
    }

    inline uint32_t GetCounter() const
    {
        return (GetRefCount() & S_COUNTER_MASK);
    }

    inline uint32_t GetRefCount() const
    {
        return MOT_ATOMIC_LOAD(m_refCount);
    }

    inline bool IsCounterReachedSoftLimit() const
    {
        return GetCounter() > 16;
    }

    void SetUpgradeCounter()
    {
        LockRefCount();
        DecCounter();
        UnlockRefCount();
        MOT_ASSERT(IsLocked());
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

    inline void InitStatus()
    {
        m_status = S_DIRTY_BIT;
    }

    inline void InitCounter()
    {
        m_refCount |= 1;
    }

    inline void Unlock()
    {
        MOT_ASSERT(m_status & S_LOCK_BIT);
#ifdef MOT_DEBUG
        SetLockOwner(-1);
#endif
#if defined(__GNUC__) && (defined(__x86_64__) || defined(__i386__))
        m_status = m_status & (~S_LOCK_WRITE_BITS);
#else

        uint64_t v = m_status;
        while (!__sync_bool_compare_and_swap(&m_status, v, (v & ~S_LOCK_WRITE_BITS))) {
            PAUSE
            v = m_status;
        }
#endif
    }

    inline bool IsLocked() const
    {
        return (GetStatus() & S_LOCK_BIT) == S_LOCK_BIT;
    }

    inline bool IsLockedAndDirty() const
    {
        return (GetStatus() & S_STATUS_BITS) == S_STATUS_BITS;
    }

    void Lock(uint64_t tid)
    {
        uint64_t v = GetStatus();
        while ((v & S_LOCK_BIT) || !__sync_bool_compare_and_swap(&m_status, v, v | S_LOCK_BIT)) {
            PAUSE
            v = GetStatus();
        }
#ifdef MOT_DEBUG
        SetLockOwner(tid);
#endif
    }

    void SetWriteBit()
    {
        MOT_ASSERT(IsLocked());
        m_status |= S_WRITE_BIT;
    }

    inline bool IsWriteBitOn() const
    {
        return (GetStatus() & S_WRITE_BIT) == S_WRITE_BIT;
    }

    bool TryLock(uint64_t tid)
    {
        uint64_t v = GetStatus();
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
        uint32_t v = GetRefCount();
        while ((v & S_COUNTER_LOCK_BIT) || !__sync_bool_compare_and_swap(&m_refCount, v, v | S_COUNTER_LOCK_BIT)) {
            PAUSE
            v = GetRefCount();
        }
    }

    inline void UnlockRefCount()
    {
        MOT_ASSERT(m_refCount & S_COUNTER_LOCK_BIT);

#if defined(__GNUC__) && (defined(__x86_64__) || defined(__i386__))
        m_refCount = m_refCount & (~S_COUNTER_LOCK_BIT);
#else

        uint32_t v = GetRefCount();
        while (!__sync_bool_compare_and_swap(&m_refCount, v, (v & ~S_COUNTER_LOCK_BIT))) {
            PAUSE
            v = GetRefCount();
        }
#endif
    }

    RC RefCountUpdate(AccessType type);
    RC RefCountUpdateNoLock(AccessType type);

    /**
     * @brief Rollback upgrade insert
     * @param RC Indicate if this sentinel can be deleted
     */
    RC RollBackUnique();

    /**
     * @brief Sets the next versioned data pointer.
     * @param ptr The next data pointer to set.
     */
    void Init(Index* index, Row* row)
    {
        m_index = index;
        m_startCSN = 0;
        InitStatus();
        InitCounter();
        SetNextPtr(row);
    }

    /**
     * @brief Set the object pointer for the sentinel
     *        PS - points to a row
     *        SS - points to a secondary sentinel
     *        SSU - points to a secondary
     *        sentinel unique node
     * @param ptr - the object pointer
     */
    void SetNextPtr(void* const& ptr)
    {
        m_status = (m_status & ~S_OBJ_ADDRESS_MASK) | ((uint64_t)(ptr) & S_OBJ_ADDRESS_MASK);
    }

    /**
     * @brief Retrieves the data associated with the sentinel index entry.
     * @return The sentinel data.
     */
    virtual Row* GetData(void) const
    {
        MOT_ASSERT(false);
        return nullptr;
    }

    /**
     * @brief Retrieves the row for the current CSN
     * @param csn the current snapshot
     * @return The sentinel data.
     */
    virtual Row* GetVisibleRowVersion(uint64_t csn) = 0;

    /**
     * @brief Retrieves the primarySentinel for the current CSN
     * @param csn the current snapshot
     * @return The primary sentinel.
     */
    virtual PrimarySentinel* GetVisiblePrimaryHeader(uint64_t csn) = 0;

    /**
     * @brief Retrieves the primary sentinel associated with the current sentinel
     * @return void pointer to the primary sentinel.
     */
    virtual void* GetPrimarySentinel() const
    {
        MOT_ASSERT(false);
        return nullptr;
    }

    void SetLockOwner(uint64_t tid)
    {
        MOT_ASSERT(m_status & S_LOCK_BIT);
        uint64_t owner = tid & S_LOCK_OWNER_RESIZE;
        m_status = (m_status & ~S_LOCK_OWNER_MASK) | (owner << S_OBJ_ADDRESS_SIZE);
    }

    virtual bool GetStableStatus() const
    {
        return false;
    }

    virtual void SetStableStatus(bool val)
    {}

    /**
     * @brief Sets the current row as stable for CALC
     */
    virtual void SetStable(Row* const& row)
    {}

    /**
     * @brief Retrieves the stable row
     * @return The row object.
     */
    virtual Row* GetStable() const
    {
        return nullptr;
    }

    uint64_t GetStartCSN() const
    {
        return m_startCSN;
    }

    uint64_t GetStatus() const
    {
        return MOT_ATOMIC_LOAD(m_status);
    }

    void SetStartCSN(uint64_t startCSN)
    {
        m_startCSN = startCSN;
    }

    virtual void SetEndCSN(uint64_t endCSN)
    {
        MOT_ASSERT(false);
    }

    virtual void Print() = 0;

    static constexpr uint64_t SENTINEL_INIT_CSN = static_cast<uint64_t>(-1);

    static constexpr uint64_t SENTINEL_INVISIBLE_CSN = 0x1FFFFFFFFFFFFFFFUL;

protected:
    /** @var m_startCSN The start TS the sentinel was created   */
    uint64_t m_startCSN = SENTINEL_INIT_CSN;

private:
    /** @var m_status Contains, the pointer and the status bit   */
    volatile uint64_t m_status = 0;

    /** @var m_index The index the sentinel belongs to   */
    Index* m_index = nullptr;

    /** @var m_refCount A counter of concurrent inserters of the same key  */
    uint32_t m_refCount = 0;

    /** @var The order of the index (primary or secondary). */
    IndexOrder m_indexOrder;

    inline void DecCounter()
    {
        MOT_ASSERT(GetCounter() > 0);
        uint32_t counter = GetCounter() - 1;
        m_refCount = (m_refCount & ~S_COUNTER_MASK) | counter;
    }

    inline void IncCounter()
    {
        uint32_t counter = GetCounter() + 1;
        m_refCount = (m_refCount & ~S_COUNTER_MASK) | counter;
    }

    DECLARE_CLASS_LOGGER()
};

}  // namespace MOT

#endif  // MOT_SENTINEL_H
