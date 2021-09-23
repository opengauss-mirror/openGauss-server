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
 * redo_log_transaction_segments.h
 *    Implements an array of log segments that are part of a specific transaction id.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/recovery/redo_log_transaction_segments.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef REDO_LOG_TRANSACTION_SEGMENTS_H
#define REDO_LOG_TRANSACTION_SEGMENTS_H

#include "log_segment.h"

namespace MOT {
class RedoLogTransactionSegments {
public:
    explicit RedoLogTransactionSegments(TransactionId id)
        : m_transactionId(id), m_segments(nullptr), m_count(0), m_size(0), m_maxSegments(0)
    {}

    ~RedoLogTransactionSegments();

    bool Append(LogSegment* segment);

    uint64_t GetTransactionId() const
    {
        return m_transactionId;
    }

    size_t GetSize() const
    {
        return m_size;
    }

    char* GetData(size_t position, size_t length) const;

    uint32_t GetCount() const
    {
        return m_count;
    }

    LogSegment* GetSegment(uint32_t index) const
    {
        if (index > m_count || m_count == 0) {
            return nullptr;
        }
        return m_segments[index];
    }

private:
    static constexpr uint32_t DEFAULT_SEGMENT_NUM = 1024;

    uint64_t m_transactionId;

    LogSegment** m_segments;

    uint32_t m_count;

    size_t m_size;

    uint32_t m_maxSegments;
};
}  // namespace MOT

#endif /* REDO_LOG_TRANSACTION_SEGMENTS_H */
