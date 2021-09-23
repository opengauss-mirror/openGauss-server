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
 * frequency_statistic_variable.cpp
 *    A statistic variable for monitoring the frequency of an event.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/stats/frequency_statistic_variable.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <cmath>
#include <iomanip>

#include "frequency_statistic_variable.h"
#include "utilities.h"

namespace MOT {
void FrequencyStatisticVariable::Summarize(bool updateTstamp)
{
    if (updateTstamp) {
        m_tstamp = CpuCyclesLevelTime::Rdtscp();
    }
    m_countSaved = m_count;
    m_intervalSeconds = CpuCyclesLevelTime::CyclesToSeconds(m_tstamp - m_initTstamp);
    m_frequency = ((double)(m_countSaved)) / m_intervalSeconds;
}

void FrequencyStatisticVariable::Print(LogLevel logLevel) const
{
    MOT_LOG(logLevel,
        "%s={ samples: %" PRIu64 ", interval: %0.2f seconds, frequency: %0.4f (evt/sec) }",
        m_name,
        m_countSaved,
        m_intervalSeconds,
        m_frequency);
}

void FrequencyStatisticVariable::Assign(const StatisticVariable& rhs)
{
    const FrequencyStatisticVariable& freqRhs = static_cast<const FrequencyStatisticVariable&>(rhs);
    m_count = freqRhs.m_count;
    m_initTstamp = freqRhs.m_initTstamp;
    m_tstamp = freqRhs.m_tstamp;
    m_frequency = freqRhs.m_frequency;
}

void FrequencyStatisticVariable::Add(const StatisticVariable& rhs)
{
    const FrequencyStatisticVariable& freqRhs = static_cast<const FrequencyStatisticVariable&>(rhs);
    m_count += freqRhs.m_count;
    m_frequency += freqRhs.m_frequency;
    if ((m_initTstamp == 0) || ((freqRhs.m_initTstamp > 0) && (freqRhs.m_initTstamp < m_initTstamp))) {
        m_initTstamp = freqRhs.m_initTstamp;
    }
    if (freqRhs.m_tstamp > m_tstamp) {
        m_tstamp = freqRhs.m_tstamp;
    }
}

void FrequencyStatisticVariable::Subtract(const StatisticVariable& rhs)
{
    const FrequencyStatisticVariable& freqRhs = static_cast<const FrequencyStatisticVariable&>(rhs);
    m_count -= freqRhs.m_count;
    if (freqRhs.m_tstamp != 0) {
        m_initTstamp = freqRhs.m_tstamp;
    }
    // summarize should be called to re-compute frequency
}

void FrequencyStatisticVariable::Divide(uint32_t factor)
{
    if (factor > 0) {
        m_count /= factor;
        m_frequency /= factor;
    }
}

void FrequencyStatisticVariable::Reset()
{
    m_count = 0;
    m_frequency = 0.0f;
}
}  // namespace MOT
