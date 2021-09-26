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
 * inprocess_transactions.cpp
 *    Implements a map that holds transactions which are pending commit or abort.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/recovery/inprocess_transactions.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "inprocess_transactions.h"

namespace MOT {
DECLARE_LOGGER(RecoveryManager, InProcessTransactions);

InProcessTransactions::~InProcessTransactions()
{
    Clear();
}

bool InProcessTransactions::InsertLogSegment(LogSegment* segment)
{
    uint64_t transactionId = segment->m_controlBlock.m_internalTransactionId;
    RedoLogTransactionSegments* transactionLogEntries = nullptr;

    const std::lock_guard<std::mutex> lock(m_lock);
    auto it = m_map.find(transactionId);
    if (it == m_map.end()) {
        // this is a new transaction. Not found in the map.
        transactionLogEntries = new (std::nothrow) RedoLogTransactionSegments(transactionId);
        if (transactionLogEntries == nullptr) {
            return false;
        }
        if (!transactionLogEntries->Append(segment)) {
            MOT_LOG_ERROR("InsertLogSegment: could not append log segment, error re-allocating log segments array");
            return false;
        }
        m_map[transactionId] = transactionLogEntries;
        m_numEntries++;
    } else {
        transactionLogEntries = it->second;
        if (!transactionLogEntries->Append(segment)) {
            MOT_LOG_ERROR("InsertLogSegment: could not append log segment, error re-allocating log segments array");
            return false;
        }
    }

    if (segment->m_controlBlock.m_externalTransactionId != INVALID_TRANSACTION_ID) {
        m_extToInt[segment->m_controlBlock.m_externalTransactionId] = segment->m_controlBlock.m_internalTransactionId;
    }
    if (segment->m_replayLsn > m_replayLsn) {
        m_replayLsn = segment->m_replayLsn;
    }
    return true;
}

bool InProcessTransactions::FindTransactionId(uint64_t externalId, uint64_t& internalId)
{
    internalId = 0;
    const std::lock_guard<std::mutex> lock(m_lock);
    auto it = m_extToInt.find(externalId);
    if (it != m_extToInt.end()) {
        internalId = it->second;
        return true;
    }
    return false;
}
}  // namespace MOT
