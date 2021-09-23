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
 * boolean_statistic_variable.cpp
 *    A statistic variable used to collect statistics for a boolean value.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/stats/boolean_statistic_variable.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "boolean_statistic_variable.h"

#include <cmath>
#include <iomanip>

namespace MOT {
void BooleanStatisticVariable::Summarize(bool updateTstamp)
{
    m_countSaved = m_count;
    m_percentage = ((double)m_sum) / ((double)m_countSaved);
}

void BooleanStatisticVariable::Print(LogLevel logLevel) const
{
    MOT_LOG(logLevel, "%s={ samples: %" PRIu64 ", percent: %0.4f%%}", m_name, m_countSaved, m_percentage * 100.0f);
}

void BooleanStatisticVariable::Assign(const StatisticVariable& rhs)
{
    const BooleanStatisticVariable& boolRhs = static_cast<const BooleanStatisticVariable&>(rhs);
    m_sum = boolRhs.m_sum;
    m_count = boolRhs.m_count;
}

void BooleanStatisticVariable::Add(const StatisticVariable& rhs)
{
    const BooleanStatisticVariable& boolRhs = static_cast<const BooleanStatisticVariable&>(rhs);
    m_sum += boolRhs.m_sum;
    m_count += boolRhs.m_count;
}

void BooleanStatisticVariable::Subtract(const StatisticVariable& rhs)
{
    const BooleanStatisticVariable& boolRhs = static_cast<const BooleanStatisticVariable&>(rhs);
    m_sum -= boolRhs.m_sum;
    m_count -= boolRhs.m_count;
}

void BooleanStatisticVariable::Divide(uint32_t factor)
{
    if (factor > 0) {
        m_count /= factor;
        m_sum /= factor;
    }
}

void BooleanStatisticVariable::Reset()
{
    m_sum = 0;
    m_count = 0;
}
}  // namespace MOT
