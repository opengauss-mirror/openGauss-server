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
 * redo_log_transaction_segments.cpp
 *    Implements an array of log segments that are part of a specific transaction id.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/recovery/redo_log_transaction_segments.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "redo_log_transaction_segments.h"

namespace MOT {
RedoLogTransactionSegments::~RedoLogTransactionSegments()
{
    if (m_segments != nullptr) {
        for (uint32_t i = 0; i < m_count; i++) {
            delete m_segments[i];
        }
        free(m_segments);
    }
}

bool RedoLogTransactionSegments::Append(LogSegment* segment)
{
    if (m_count == m_maxSegments) {
        // max segments allocated, need to extend the number of allocated LogSegments pointers
        uint32_t newMaxSegments = m_maxSegments + DEFAULT_SEGMENT_NUM;
        LogSegment** newSegments = (LogSegment**)malloc(newMaxSegments * sizeof(LogSegment*));
        if (newSegments != nullptr) {
            if (m_segments != nullptr) {
                errno_t erc = memcpy_s(
                    newSegments, newMaxSegments * sizeof(LogSegment*), m_segments, m_maxSegments * sizeof(LogSegment*));
                securec_check(erc, "\0", "\0");
                free(m_segments);
            }
            m_segments = newSegments;
            m_maxSegments = newMaxSegments;
        } else {
            return false;
        }
    }

    m_size += segment->m_len;
    m_segments[m_count] = segment;
    m_count += 1;
    return true;
}

char* RedoLogTransactionSegments::GetData(size_t position, size_t length) const
{
    if (position + length > m_size) {
        return nullptr;
    }

    uint32_t currentEntry = 0;
    while (currentEntry < m_count) {
        if (position > m_segments[currentEntry]->m_len) {
            position -= m_segments[currentEntry]->m_len;
            currentEntry++;
        } else {
            if (position + length > m_segments[currentEntry]->m_len) {
                // Cross segments is not supported for now
                return nullptr;
            }
            return (m_segments[currentEntry]->m_data + position);
        }
    }
    return nullptr;
}
}  // namespace MOT
