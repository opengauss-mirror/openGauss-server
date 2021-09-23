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
 * redo_log_transaction_iterator.cpp
 *    Iterator for iterating over redo log transactions.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/recovery/redo_log_transaction_iterator.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "redo_log_transaction_iterator.h"

namespace MOT {
bool RedoLogTransactionIterator::End()
{
    return (m_position >= m_bufferLength);
}

bool RedoLogTransactionIterator::Next()
{
    m_position += m_txnLength;
    if (End()) {
        m_txnLength = 0;
        return false;
    } else {
        m_txnLength = *(reinterpret_cast<uint32_t*>(m_buffer + m_position));
        m_endSegment = *(reinterpret_cast<EndSegmentBlock*>(m_buffer + m_txnLength - sizeof(EndSegmentBlock)));
        return true;
    }
}

void* RedoLogTransactionIterator::GetTransactionEntry()
{
    if (End()) {
        return nullptr;
    }
    return reinterpret_cast<void*>(m_buffer + m_position);
}

LogSegment* RedoLogTransactionIterator::AllocRedoSegment(uint64_t replayLsn)
{
    LogSegment* segment = new (std::nothrow) LogSegment();
    if (segment == nullptr) {
        return nullptr;
    }
    segment->m_len = m_txnLength - sizeof(uint32_t);
    segment->m_data = new (std::nothrow) char[segment->m_len];
    if (segment->m_data == nullptr) {
        delete segment;
        return nullptr;
    }
    segment->m_controlBlock = m_endSegment;
    errno_t erc = memcpy_s(segment->m_data,
        segment->m_len,
        reinterpret_cast<void*>(m_buffer + m_position + sizeof(uint32_t)),
        segment->m_len);
    securec_check(erc, "\0", "\0");
    segment->m_replayLsn = replayLsn;
    return segment;
}
}  // namespace MOT
