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
 *    src/gausskernel/storage/mot/core/src/system/recovery/inprocess_transactions.h
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
    InProcessTransactions() : m_numEntries(0)
    {}

    ~InProcessTransactions();

    bool InsertLogSegment(LogSegment* segment);

    bool FindTransactionId(uint64_t externalId, uint64_t& internalId, bool pop = true);

    template <typename T>
    RC ForUniqueTransaction(uint64_t id, const T& func)
    {
        return OperateOnTransactionsMap(id, func, true, true);
    }

    template <typename T>
    RC ForEachTransaction(const T& func, bool pop)
    {
        return OperateOnTransactionsMap(0, func, pop, false);
    }

    bool IsInProcessTx(uint64_t id)
    {
        return (m_extToInt.find(id) != m_extToInt.end());
    }

    void Lock()
    {
        m_lock.lock();
    }

    void Unlock()
    {
        m_lock.unlock();
    }

    void UpdateTxIdMap(uint64_t intTx, uint64_t extTx)
    {
        m_extToInt[extTx] = intTx;
    }

    size_t GetNumTxns() const
    {
        return m_map.size();
    }

private:
    template <typename T>
    RC OperateOnTransactionsMap(uint64_t id, const T& func, bool pop, bool lock)
    {
        RC status = RC_OK;
        if (lock) {
            m_lock.lock();
        }
        auto it = id ? m_map.find(id) : m_map.begin();
        while (it != m_map.end()) {
            RedoLogTransactionSegments* segments = it->second;
            if (pop) {
                m_map.erase(it);
                m_numEntries--;
            }
            if (lock) {
                m_lock.unlock();
            }
            status = func(segments, it->first);
            if (pop) {
                delete segments;
            }
            if (id || status != RC_OK) {
                return status;
            } else {
                ++it;
            }
            if (lock) {
                m_lock.lock();
            }
        }
        if (lock) {
            m_lock.unlock();
        }
        return RC_OK;
    }
    std::mutex m_lock;

    std::map<uint64_t, RedoLogTransactionSegments*> m_map;

    std::map<uint64_t, uint64_t> m_extToInt;

    uint64_t m_numEntries;
};
}  // namespace MOT

#endif /* INPROCESS_TRANSACTIONS_H */
