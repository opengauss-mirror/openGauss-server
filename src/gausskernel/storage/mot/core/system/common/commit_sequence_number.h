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
 * commit_sequence_number.h
 *    Encapsulates the logic of a global unique auto incremental number.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/common/commit_sequence_number.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef COMMIT_SEQUENCE_NUMBER_H
#define COMMIT_SEQUENCE_NUMBER_H

#include <atomic>
#include "global.h"

namespace MOT {
/**
 * @class CSNManager
 * @brief Encapsulates the logic of a global unique auto incremental number. The CSN is used to identify the order of
 * commit records where required (for example, during recovery).
 */
class CSNManager {
public:
    /** @brief Constructor. */
    CSNManager();
    CSNManager(const CSNManager& orig) = delete;
    CSNManager& operator=(const CSNManager& orig) = delete;

    /** @brief Destructor. */
    virtual ~CSNManager();

    /** @brief Used to enforce a csn value (used after recovery). */
    RC SetCSN(uint64_t value);

    /** @brief Get the next csn. This method also increment the current csn. */
    uint64_t GetNextCSN();

    /** @brief Get the current csn. This method does not change the value of the current csn. */
    inline uint64_t GetCurrentCSN() const
    {
        return m_csn;
    }

    static constexpr uint64_t INVALID_CSN = 0;  // Equal to InvalidCommitSeqNo in the envelope.
    static constexpr uint64_t INITIAL_CSN = 3;  // Equal to COMMITSEQNO_FIRST_NORMAL in the envelope.

private:
    /** @brief atomic uint64_t holds the current csn value */
    std::atomic<uint64_t> m_csn;
};
}  // namespace MOT

#endif /* COMMIT_SEQUENCE_NUMBER_H */
