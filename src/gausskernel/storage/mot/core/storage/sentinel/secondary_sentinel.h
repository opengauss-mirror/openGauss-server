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
 * secondary_sentinel.h
 *    Secondary Index Sentinel
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/storage/secondary_sentinel.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef SECONDARY_SENTINEL_H
#define SECONDARY_SENTINEL_H

#include "sentinel.h"
#include "primary_sentinel.h"

namespace MOT {
class SecondarySentinel : public Sentinel {
public:
    ~SecondarySentinel() override
    {}

    uint64_t GetEndCSN() const
    {
        return m_endCSN;
    }

    std::string GetEndCSNStr() const
    {
        if (m_endCSN == SENTINEL_INIT_CSN) {
            return std::string("INF");
        } else {
            return std::to_string(m_endCSN);
        }
    }

    void SetEndCSN(uint64_t endCSN) override
    {
        m_endCSN = endCSN;
    }

    /**
     * @brief Retrieves the data associated with the sentinel index entry.
     * @return The sentinel data.
     */
    Row* GetData(void) const override
    {
        PrimarySentinel* ps = reinterpret_cast<PrimarySentinel*>((uint64_t)(GetStatus() & S_OBJ_ADDRESS_MASK));
        if (ps != nullptr) {
            return ps->GetData();
        } else {
            return nullptr;
        }
    }

    /**
     * @brief Retrieves the row for the current CSN
     * @param csn the current snapshot
     * @return The sentinel data.
     */
    Row* GetVisibleRowVersion(uint64_t csn) override
    {
        while (IsLocked()) {
            CpuCyclesLevelTime::Sleep(1);
        }
        if (IsCommited()) {
            // Valid range (start_csn,end_csn]
            if (csn <= m_endCSN and csn > m_startCSN) {
                PrimarySentinel* ps = reinterpret_cast<PrimarySentinel*>(GetPrimarySentinel());
                MOT_ASSERT(ps != nullptr);
                return ps->GetVisibleRowVersion(csn);
            }
        }
        return nullptr;
    }

    /**
     * @brief Retrieves the Correct primary-sentinel for the current CSN
     * @param csn the current snapshot
     * @return The primary-sentinel.
     */
    PrimarySentinel* GetVisiblePrimaryHeader(uint64_t csn) override
    {
        while (!IsCommited() and IsLocked()) {
            CpuCyclesLevelTime::Sleep(1);
        }
        if (IsCommited()) {
            if (csn <= m_endCSN and csn > m_startCSN) {
                PrimarySentinel* ps = reinterpret_cast<PrimarySentinel*>(GetPrimarySentinel());
                MOT_ASSERT(ps != nullptr);
                return ps->GetVisiblePrimaryHeader(csn);
            }
        }
        return nullptr;
    }

    /**
     * @brief Retrieves the primary sentinel associated with the current sentinel
     * @return void pointer to the primary sentinel.
     */
    void* GetPrimarySentinel() const override
    {
        return reinterpret_cast<void*>((uint64_t)(GetStatus() & S_OBJ_ADDRESS_MASK));
    }

    void Print() override;

private:
    /** @var m_endCSN indicate the timestamp the sentinel was deleted   */
    uint64_t m_endCSN = SENTINEL_INIT_CSN;

    DECLARE_CLASS_LOGGER()
};
}  // namespace MOT

#endif  // SECONDARY_SENTINEL_H
