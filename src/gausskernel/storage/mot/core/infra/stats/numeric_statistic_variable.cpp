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
 * numeric_statistic_variable.cpp
 *    A statistic variable for integral types.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/stats/numeric_statistic_variable.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <cmath>
#include <cstdio>

#include "numeric_statistic_variable.h"

namespace MOT {
NumericStatisticVariable::NumericStatisticVariable(
    const char* name, uint64_t factor /* = 1 */, const char* units /* = "" */, uint64_t limit /* = ULONG_LONG_MAX */)
    : StatisticVariable(name),
      m_factor(factor),
      m_units{0},
      m_limit(limit),
      m_sum(0),
      m_squareSum(0),
      m_avg(0.0f),
      m_var(0.0f),
      m_std(0.0f),
      m_countSaved(0)
{
    errno_t erc = snprintf_s(m_units, STAT_VAR_MAX_NAME_LEN, STAT_VAR_MAX_NAME_LEN - 1, "%s", units);
    securec_check_ss(erc, "\0", "\0");
}

void NumericStatisticVariable::Summarize(bool updateTstamp)
{
    (void)updateTstamp;
    m_countSaved = m_count;
    m_avg = ((double)m_sum) / ((double)m_countSaved);
    m_var = ((double)m_squareSum) - m_avg * m_avg;
    m_std = sqrt(m_var);
}

void NumericStatisticVariable::Print(LogLevel logLevel) const
{
    MOT_LOG(logLevel,
        "%s={ samples: %" PRIu64 ", avg: %0.4f %s, std: %0.4f %s }",
        m_name,
        m_countSaved,
        m_avg / m_factor,
        m_units,
        m_std / m_factor,
        m_units);
}

void NumericStatisticVariable::Assign(const StatisticVariable& rhs)
{
    auto numRhs = static_cast<const NumericStatisticVariable&>(rhs);
    m_sum = numRhs.m_sum;
    m_squareSum = numRhs.m_squareSum;
    m_count = numRhs.m_count;
}

void NumericStatisticVariable::Add(const StatisticVariable& rhs)
{
    auto numRhs = static_cast<const NumericStatisticVariable&>(rhs);
    m_sum += numRhs.m_sum;
    m_squareSum += numRhs.m_squareSum;
    m_count += numRhs.m_count;
}

void NumericStatisticVariable::Subtract(const StatisticVariable& rhs)
{
    auto numRhs = static_cast<const NumericStatisticVariable&>(rhs);
    m_sum -= numRhs.m_sum;
    m_squareSum -= numRhs.m_squareSum;
    m_count -= numRhs.m_count;
}

void NumericStatisticVariable::Divide(uint32_t factor)
{
    if (factor > 0) {
        m_count /= factor;
        m_sum /= factor;
        m_squareSum /= factor;
    }
}

void NumericStatisticVariable::Reset()
{
    m_sum = 0;
    m_squareSum = 0;
    m_count = 0;
}
}  // namespace MOT
