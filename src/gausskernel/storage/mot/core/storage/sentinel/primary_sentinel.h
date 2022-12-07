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
 * primary_sentinel.h
 *    Primary Index Sentinel
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/storage/primary_sentinel.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef PRIMARY_SENTINEL_H
#define PRIMARY_SENTINEL_H

#include "sentinel.h"

namespace MOT {
class PrimarySentinel : public Sentinel {
public:
    ~PrimarySentinel() override
    {}

    static constexpr uint16_t SPIN_WAIT_COUNT = 16;

    bool GetStableStatus() const override;

    void SetStableStatus(bool val) override;

    /**
     * @brief Sets the current row as stable for CALC
     */
    void SetStable(Row* const& row) override;

    /**
     * @brief Retrieves the stable row
     * @return The row object.
     */
    Row* GetStable() const override;

    Row* GetData(void) const override
    {
        return static_cast<Row*>((void*)(GetStatus() & S_OBJ_ADDRESS_MASK));
    }

    bool IsSentinelRemovable() const
    {
        return ((GetCounter() == 0) and (m_gcInfo.GetCounter() == 0));
    }

    /**
     * @brief Retrieves the row for the current CSN
     * @param csn the current snapshot
     * @return The sentinel data.
     */
    Row* GetVisibleRowVersion(uint64_t csn) override;

    /**
     * @brief Retrieves the primary sentinel for the current CSN
     * @param csn the current snapshot
     * @return The primary sentinel.
     */
    PrimarySentinel* GetVisiblePrimaryHeader(uint64_t csn) override;

    /**
     * @brief Retrieves the primary sentinel associated with the current sentinel
     * @return void pointer to the primary sentinel.
     */
    void* GetPrimarySentinel() const override
    {
        return static_cast<void*>(const_cast<PrimarySentinel*>(this));
    }

    inline bool GetStablePreAllocStatus() const
    {
        return (m_stable & PRE_ALLOC_BIT) == PRE_ALLOC_BIT;
    }

    inline void SetStablePreAllocStatus(bool val)
    {
        if (val) {
            m_stable |= PRE_ALLOC_BIT;
        } else {
            m_stable &= ~PRE_ALLOC_BIT;
        }
    }

    /**
     * @brief Retrieves the latest row which CSN is >= csn
     * @param csn the current snapshot
     * @return The sentinel data.
     */
    Row* GetVisibleRowForUpdate(uint64_t csn, ISOLATION_LEVEL isolationLevel);

    Row* GetRowFromChain(uint64_t csn, Row* row);

    void Print() override;

    void ReclaimSentinel(Sentinel* s);

    GcSharedInfo& GetGcInfo()
    {
        return m_gcInfo;
    }

    inline void SetTransactionId(uint64_t tid)
    {
        m_transactionId = tid;
    }

    uint64_t GetTransactionId() const
    {
        return m_transactionId;
    }

private:
    /** @var m_stable A Container for the row and its status bit. */
    volatile uint64_t m_stable = 0;

    /** @var m_transactionId The latest tid committed on this tuple (recovery). */
    volatile uint64_t m_transactionId = 0;

    /** @var m_gcInfo Shared synchronization container for garbage collection. */
    GcSharedInfo m_gcInfo;

    DECLARE_CLASS_LOGGER()
};

}  // namespace MOT

#endif  // PRIMARY_SENTINEL_H
