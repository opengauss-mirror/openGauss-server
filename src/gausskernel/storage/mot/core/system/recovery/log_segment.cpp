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
 * log_segment.cpp
 *    Redo log data container.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/recovery/log_segment.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "log_segment.h"
#include "redo_log_writer.h"

namespace MOT {
size_t LogSegment::SerializeSize()
{
    return SerializableCharBuf::SerializeSize(m_len) + EndSegmentBlockSerializer::SerializeSize(&m_controlBlock);
}

void LogSegment::Serialize(char* dataOut)
{
    dataOut = SerializableCharBuf::Serialize(dataOut, m_data, m_len);
    EndSegmentBlockSerializer::Serialize(&m_controlBlock, dataOut);
}

void LogSegment::Deserialize(const char* in)
{
    char* dataIn = (char*)in;
    dataIn = SerializableCharBuf::Deserialize(dataIn, m_data, m_len);
    EndSegmentBlockSerializer::Deserialize(&m_controlBlock, dataIn);
}
}  // namespace MOT
