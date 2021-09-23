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
 * global_statistics.cpp
 *    A class used to manage global set of statistic variables for a single component.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/stats/global_statistics.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "global_statistics.h"

namespace MOT {
DECLARE_LOGGER(GlobalStatistics, Statistics)

void GlobalStatistics::Summarize(bool updateTstamp)
{
    uint32_t statCount = m_statVars.size();
    for (uint32_t i = 0; i < statCount; ++i) {
        m_statVars[i]->Summarize(updateTstamp);
    }
}

void GlobalStatistics::Print(uint32_t statId, LogLevel logLevel) const
{
    uint32_t statCount = m_statVars.size();
    if (statId < statCount) {
        if (m_statVars[statId]->GetSampleCount() > 0) {
            m_statVars[statId]->Print(logLevel);
        }
    } else {
        for (uint32_t i = 0; i < statCount; ++i) {
            if (m_statVars[i]->GetSampleCount() > 0) {
                m_statVars[i]->Print(logLevel);
            }
        }
    }
}

void GlobalStatistics::Reset()
{
    uint32_t statCount = m_statVars.size();
    for (uint32_t i = 0; i < statCount; ++i) {
        m_statVars[i]->Reset();
    }
}

void GlobalStatistics::Assign(const GlobalStatistics& rhs)
{
    uint32_t statCount = m_statVars.size();
    for (uint32_t i = 0; i < statCount; ++i) {
        m_statVars[i]->Assign(*rhs.m_statVars[i]);
    }
}

void GlobalStatistics::Subtract(const GlobalStatistics& rhs)
{
    uint32_t statCount = m_statVars.size();
    for (uint32_t i = 0; i < statCount; ++i) {
        m_statVars[i]->Subtract(*rhs.m_statVars[i]);
    }
}

bool GlobalStatistics::HasValidSamples() const
{
    bool result = false;
    uint32_t statCount = m_statVars.size();
    for (uint32_t i = 0; i < statCount; ++i) {
        if (m_statVars[i]->GetSampleCount() > 0) {
            result = true;
            break;
        }
    }
    return result;
}

mot_string GlobalStatistics::MakeName(const char* baseName, NamingScheme namingScheme)
{
    mot_string result;

    if ((namingScheme == NamingScheme::NAMING_SCHEME_TOTAL) ||
        (namingScheme == NamingScheme::NAMING_SCHEME_TOTAL_PREV)) {
        result.format("%s[TOTAL]", baseName);
    } else if (namingScheme == NamingScheme::NAMING_SCHEME_DIFF) {
        result.format("%s[DIFF]", baseName);
    }

    return result;
}

void GlobalStatistics::RegisterStatistics(StatisticVariable* statVar)
{
    if (!m_statVars.push_back(statVar)) {
        MOT_REPORT_ERROR(
            MOT_ERROR_OOM, "Register Statistics", "Failed to register statistics variable %s", statVar->GetName());
    }
}
}  // namespace MOT
