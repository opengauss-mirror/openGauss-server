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
 * csn_manager.h
 *    Encapsulates the logic of a global unique auto incremental number.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/common/csn_manager.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef COMMIT_SEQUENCE_NUMBER_H
#define COMMIT_SEQUENCE_NUMBER_H

#include <atomic>
#include "global.h"
#include "icsn_manager.h"

namespace MOT {

/**
 * @class CSNManager
 * @brief Encapsulates the logic of a global unique auto incremental number. The CSN is used to identify the order of
 * commit records where required (for example, during recovery).
 */
class CSNManager final : public ICSNManager {
public:
    /** @brief Constructor. */
    CSNManager();

    /** @brief Destructor. */
    ~CSNManager() override
    {}

    /** @brief Get the next csn. This method also increment the current csn. */
    uint64_t GetNextCSN() override
    {
        uint64_t current = INVALID_CSN;
        uint64_t next = INVALID_CSN;
        do {
            current = m_csn;
            next = current + 1;
        } while (!m_csn.compare_exchange_strong(current, next, std::memory_order_acq_rel));
        return current;
    }

    /** @brief Get the current csn. This method does not change the value of the current csn. */
    uint64_t GetCurrentCSN() override
    {
        return m_csn;
    }

    /** @brief Get the current csn. This method does not change the value of the current csn. */
    uint64_t GetGcEpoch() override
    {
        return m_csn;
    }

    /**
     * @brief Used to enforce a csn value. It is used only during recovery by the committer (single thread) or at the
     * end of the recovery.
     */
    void SetCSN(uint64_t value) override;

private:
    /** @brief atomic uint64_t holds the current csn value */
    std::atomic<uint64_t> m_csn;
};
}  // namespace MOT

#endif /* COMMIT_SEQUENCE_NUMBER_H */
