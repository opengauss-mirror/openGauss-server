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
 * rate_statistic_variable.cpp
 *    A statistic variable for rate types.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/stats/rate_statistic_variable.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <cmath>
#include <iomanip>
#include "rate_statistic_variable.h"
#include "utilities.h"

namespace MOT {
void RateStatisticVariable::Summarize(bool updateTstamp)
{
    if (updateTstamp) {
        m_tstamp = CpuCyclesLevelTime::Rdtscp();
    }
    m_countSaved = m_count;
    m_sumSaved = m_sum;
    m_intervalSeconds = CpuCyclesLevelTime::CyclesToSeconds(m_tstamp - m_initTstamp);
    m_rate = (((double)(m_sumSaved)) / m_intervalSeconds) / m_factor;
    m_frequency = ((double)(m_countSaved)) / m_intervalSeconds;
}

void RateStatisticVariable::Print(LogLevel logLevel) const
{
    MOT_LOG(logLevel,
        "%s={ rate: %0.4f (%s/sec), frequency: %0.4f (evt/sec) [%" PRIu64 " samples] }",
        m_name,
        m_rate,
        m_units,
        m_frequency,
        m_countSaved);
}

void RateStatisticVariable::Assign(const StatisticVariable& rhs)
{
    auto rateRhs = static_cast<const RateStatisticVariable&>(rhs);
    m_count = rateRhs.m_count;
    m_sum = rateRhs.m_sum;
    m_initTstamp = rateRhs.m_initTstamp;
    m_tstamp = rateRhs.m_tstamp;
    m_rate = rateRhs.m_rate;
    m_intervalSeconds = rateRhs.m_intervalSeconds;
    m_frequency = rateRhs.m_frequency;
    m_countSaved = rateRhs.m_count;
    m_sumSaved = rateRhs.m_sumSaved;
}

void RateStatisticVariable::Add(const StatisticVariable& rhs)
{
    auto rateRhs = static_cast<const RateStatisticVariable&>(rhs);
    m_count += rateRhs.m_countSaved;
    m_sum += rateRhs.m_sumSaved;
    m_rate += rateRhs.m_rate;
    m_frequency += rateRhs.m_frequency;
    if ((m_initTstamp == 0) || ((rateRhs.m_initTstamp > 0) && (rateRhs.m_initTstamp < m_initTstamp))) {
        m_initTstamp = rateRhs.m_initTstamp;
    }
    if (rateRhs.m_tstamp > m_tstamp) {
        m_tstamp = rateRhs.m_tstamp;
    }
}

void RateStatisticVariable::Subtract(const StatisticVariable& rhs)
{
    auto rateRhs = static_cast<const RateStatisticVariable&>(rhs);
    m_count -= rateRhs.m_count;
    m_sum -= rateRhs.m_sum;
    if (rateRhs.m_tstamp != 0) {
        m_initTstamp = rateRhs.m_tstamp;
    }
}

void RateStatisticVariable::Divide(uint32_t factor)
{
    if (factor > 0) {
        m_count /= factor;
        m_rate /= factor;
        m_frequency /= factor;
    }
}

void RateStatisticVariable::Reset()
{
    m_count = 0;
    m_sum = 0;
    m_rate = 0.0f;
    m_frequency = 0.0f;
    m_initTstamp = 0;
    m_tstamp = 0;
    m_intervalSeconds = 0.0f;
}
}  // namespace MOT
