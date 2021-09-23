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
 * log_segment.h
 *    Redo log data container.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/recovery/log_segment.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef LOG_SEGMENT_H
#define LOG_SEGMENT_H

#include "redo_log_global.h"
#include "redo_log_writer.h"
#include "serializable.h"

namespace MOT {
/**
 * @struct LogSegment
 * @brief encapsulates a chunk of logging data
 */
struct LogSegment : public Serializable {
    char* m_data;

    size_t m_len;

    EndSegmentBlock m_controlBlock;

    uint64_t m_replayLsn;

    ~LogSegment()
    {
        if (m_data != nullptr) {
            delete[] m_data;
        }
    }

    /**
     * @brief checks if this log segment is part of a two-phase transaction
     * @return Boolean value denoting if it is part of a two-phase transaction or not.
     */
    bool IsTwoPhase()
    {
        return (m_controlBlock.m_opCode == PREPARE_TX || m_controlBlock.m_opCode == COMMIT_PREPARED_TX);
    }

    /**
     * @brief fetches the size of the log segment
     * @return Size_t value denoting the size of the segment.
     */
    virtual size_t SerializeSize();

    /**
     * @brief serialize the log segment into a given buffer
     * @param dataOut the output buffer
     */
    virtual void Serialize(char* dataOut);

    /**
     * @brief creates a log segment from a data buffer.
     * @param dataIn the input buffer.
     */
    virtual void Deserialize(const char* dataIn);
};
}  // namespace MOT

#endif /* LOG_SEGMENT_H */
