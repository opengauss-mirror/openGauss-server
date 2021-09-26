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
 * memory_statistic_variable.cpp
 *    A statistic variable for memory monitoring.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/stats/memory_statistic_variable.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "memory_statistic_variable.h"

namespace MOT {
void MemoryStatisticVariable::Print(LogLevel logLevel) const
{
    m_level.Print(logLevel);
    m_rate.Print(logLevel);
}

void MemoryStatisticVariable::Assign(const StatisticVariable& rhs)
{
    const MemoryStatisticVariable& memRhs = (const MemoryStatisticVariable&)rhs;
    m_count = memRhs.m_count;
    m_level.Assign(memRhs.m_level);
    m_rate.Assign(memRhs.m_rate);
}

void MemoryStatisticVariable::Add(const StatisticVariable& rhs)
{
    const MemoryStatisticVariable& memRhs = (const MemoryStatisticVariable&)rhs;
    m_count += memRhs.m_count;
    m_level.Add(memRhs.m_level);
    m_rate.Add(memRhs.m_rate);
}

void MemoryStatisticVariable::Subtract(const StatisticVariable& rhs)
{
    const MemoryStatisticVariable& memRhs = (const MemoryStatisticVariable&)rhs;
    m_count -= memRhs.m_count;
    m_level.Subtract(memRhs.m_level);
    m_rate.Subtract(memRhs.m_rate);
}

void MemoryStatisticVariable::Divide(uint32_t factor)
{
    m_level.Divide(factor);
    m_rate.Divide(factor);
}

void MemoryStatisticVariable::Reset()
{
    m_count = 0;
    m_level.Reset();
    m_rate.Reset();
}
}  // namespace MOT
