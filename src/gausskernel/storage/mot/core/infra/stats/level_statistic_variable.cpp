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
 * level_statistic_variable.cpp
 *    A statistic variable for level monitoring (e.g. memory consumption).
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/stats/level_statistic_variable.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <cmath>

#include "level_statistic_variable.h"

namespace MOT {
void LevelStatisticVariable::Summarize(bool updateTstamp)
{
    if (updateTstamp) {
        m_tstamp = CpuCyclesLevelTime::Rdtscp();
    }
    m_countSaved = m_count;
    m_levelSaved = m_level;
    m_peakSaved = m_peak;
    m_avg = ((double)m_integral) / ((double)(m_tstamp - m_initTstamp));
    m_intervalSeconds = CpuCyclesLevelTime::CyclesToSeconds(m_tstamp - m_initTstamp);
}

void LevelStatisticVariable::Print(LogLevel logLevel) const
{
    MOT_LOG(logLevel,
        "%s={ current level: %" PRIu64 " %s, peak: %" PRIu64 " %s [%" PRIu64 " samples] }",
        m_name,
        static_cast<uint64_t>(m_levelSaved) / m_factor,
        m_units,
        m_peakSaved / m_factor,
        m_units,
        m_countSaved);
}

void LevelStatisticVariable::Assign(const StatisticVariable& rhs)
{
    const LevelStatisticVariable& levelRhs = static_cast<const LevelStatisticVariable&>(rhs);
    m_level = levelRhs.m_level;
    m_peak = levelRhs.m_peak;
    m_tstamp = levelRhs.m_tstamp;
    m_initTstamp = levelRhs.m_initTstamp;
    m_integral = levelRhs.m_integral;
    m_count = levelRhs.m_count;
}

void LevelStatisticVariable::Add(const StatisticVariable& rhs)
{
    const LevelStatisticVariable& levelRhs = static_cast<const LevelStatisticVariable&>(rhs);
    m_level += levelRhs.m_level;
    m_peak += levelRhs.m_peak;
    if (levelRhs.m_tstamp > m_tstamp) {
        m_tstamp = levelRhs.m_tstamp;
    }
    if (levelRhs.m_initTstamp < m_initTstamp) {
        m_initTstamp = levelRhs.m_initTstamp;
    }
    m_integral += levelRhs.m_integral;
    m_count += levelRhs.m_count;
}

void LevelStatisticVariable::Subtract(const StatisticVariable& rhs)
{
    const LevelStatisticVariable& levelRhs = static_cast<const LevelStatisticVariable&>(rhs);
    m_initTstamp = levelRhs.m_tstamp;
    m_integral -= levelRhs.m_integral;
    m_count -= levelRhs.m_count;
    // we do not subtract level, since even in periodic diff report we would like to show current level
    // in addition, peak cannot be inferred within interval, unless we actively maintain it
}

void LevelStatisticVariable::Divide(uint32_t factor)
{
    if (factor > 0) {
        m_count /= factor;
        m_level /= factor;
        m_integral /= factor;
    }
}

void LevelStatisticVariable::Reset()
{
    m_level = 0;
    m_peak = 0;
    m_integral = 0;
    m_count = 0;
}
}  // namespace MOT
