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
 * inprocess_transactions.h
 *    Implements a map that holds transactions which are pending commit or abort.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/recovery/inprocess_transactions.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef INPROCESS_TRANSACTIONS_H
#define INPROCESS_TRANSACTIONS_H

#include <cstdint>
#include <map>
#include <mutex>
#include "redo_log_transaction_segments.h"

namespace MOT {
class InProcessTransactions {
public:
    InProcessTransactions() : m_numEntries(0), m_replayLsn(0)
    {}

    ~InProcessTransactions();

    bool InsertLogSegment(LogSegment* segment);

    bool FindTransactionId(uint64_t externalId, uint64_t& internalId);

    template <typename T>
    RC ForUniqueTransaction(uint64_t internalId, uint64_t externalId, const T& func)
    {
        const std::lock_guard<std::mutex> lock(m_lock);
        auto it = m_map.find(internalId);
        if (it != m_map.end()) {
            RedoLogTransactionSegments* segments = it->second;
            RC status = func(segments, it->first);
            m_map.erase(it);
            m_extToInt.erase(externalId);
            m_numEntries--;
            delete segments;
            return status;
        }
        return RC_ERROR;
    }

    /* Attention: Caller's should acquire the lock by calling Lock() method, before calling this method. */
    template <typename T>
    RC ForEachTransactionNoLock(const T& func)
    {
        auto it = m_map.begin();
        while (it != m_map.end()) {
            RedoLogTransactionSegments* segments = it->second;
            RC status = func(segments, it->first);
            if (status != RC_OK) {
                return status;
            }
            ++it;
        }
        return RC_OK;
    }

    void Lock()
    {
        m_lock.lock();
    }

    void Unlock()
    {
        m_lock.unlock();
    }

    uint64_t GetNumTxns() const
    {
        return m_numEntries;
    }

    uint64_t GetReplayLsn() const
    {
        return m_replayLsn;
    }

    void Clear()
    {
        const std::lock_guard<std::mutex> lock(m_lock);
        if (m_numEntries > 0) {
            auto it = m_map.begin();
            while (it != m_map.end()) {
                RedoLogTransactionSegments* segments = it->second;
                delete segments;
                ++it;
            }
            m_map.clear();
            m_extToInt.clear();
            m_numEntries = 0;
        }
    }

private:
    std::mutex m_lock;

    std::map<uint64_t, RedoLogTransactionSegments*> m_map;

    std::map<uint64_t, uint64_t> m_extToInt;

    volatile uint64_t m_numEntries;

    uint64_t m_replayLsn;
};
}  // namespace MOT

#endif /* INPROCESS_TRANSACTIONS_H */
