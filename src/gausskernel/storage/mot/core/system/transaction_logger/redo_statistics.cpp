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
 * redo_statistics.cpp
 *    Manages global redo log statistics.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/transaction_logger/redo_statistics.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "redo_statistics.h"

namespace MOT {
RedoStatistics::RedoStatistics() : m_operationsPerCode(), m_consumedBytes(0)
{}

void RedoStatistics::OperationProcessed(OperationCode op, uint32_t size)
{
    uint32_t x = static_cast<uint32_t>(op);
    m_operationsPerCode[x]++;
    m_consumedBytes += size;
}

std::string RedoStatistics::ToString() const
{
    std::string result = "redo_bytes:";
    result += std::to_string(m_consumedBytes);
    for (uint i = 0; i < m_operationsPerCode.size(); i++) {
        OperationCode op = static_cast<OperationCode>(i);
        result += ", redo_";
        result += MOT::OperationCodeToString(op);
        result += "=";
        result += std::to_string(m_operationsPerCode[i]);
    }
    result += "}";
    return result;
}
}  // namespace MOT
