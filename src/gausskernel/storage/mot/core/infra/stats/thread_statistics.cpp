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
 * thread_statistics.cpp
 *    A class used to manage per-thread set of statistic variables for a single component.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/stats/thread_statistics.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "thread_statistics.h"

namespace MOT {
DECLARE_LOGGER(ThreadStatistics, Statistics)

void ThreadStatistics::Summarize(bool updateTstamp)
{
    uint32_t statCount = m_statVars.size();
    for (uint32_t i = 0; i < statCount; ++i) {
        m_statVars[i]->Summarize(updateTstamp);
    }
}

void ThreadStatistics::Print(uint32_t statId, LogLevel logLevel) const
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

void ThreadStatistics::Reset()
{
    uint32_t statCount = m_statVars.size();
    for (uint32_t i = 0; i < statCount; ++i) {
        m_statVars[i]->Reset();
        m_validThreads[i] = 0;
    }
}

void ThreadStatistics::Assign(const ThreadStatistics& rhs)
{
    uint32_t statCount = m_statVars.size();
    for (uint32_t i = 0; i < statCount; ++i) {
        m_statVars[i]->Assign(*rhs.m_statVars[i]);
        m_validThreads[i] = rhs.m_validThreads[i];
    }
}

void ThreadStatistics::Add(const ThreadStatistics& rhs)
{
    uint32_t statCount = m_statVars.size();
    for (uint32_t i = 0; i < statCount; ++i) {
        m_statVars[i]->Add(*rhs.m_statVars[i]);
        if (rhs.m_statVars[i]->GetSampleCount() > 0)
            ++m_validThreads[i];
    }
}

void ThreadStatistics::Subtract(const ThreadStatistics& rhs)
{
    uint32_t statCount = m_statVars.size();
    for (uint32_t i = 0; i < statCount; ++i) {
        m_statVars[i]->Subtract(*rhs.m_statVars[i]);
    }
}

void ThreadStatistics::Normalize()
{
    uint32_t statCount = m_statVars.size();
    for (uint32_t i = 0; i < statCount; ++i) {
        if (m_validThreads[i] > 0) {
            m_statVars[i]->Divide(m_validThreads[i]);
        }
    }
}

bool ThreadStatistics::HasValidSamples() const
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

mot_string ThreadStatistics::MakeName(const char* baseName, uint64_t threadId)
{
    mot_string result;
    if (threadId == THREAD_ID_TOTAL) {
        result.format("%s[TOTAL]", baseName);
    } else if (threadId == THREAD_ID_AVG) {
        result.format("%s[AVG]", baseName);
    } else if (threadId == THREAD_ID_DIFF) {
        result.format("%s[DIFF]", baseName);
    } else if (threadId == THREAD_ID_DIFF_AVG) {
        result.format("%s[DIFF-AVG]", baseName);
    } else {
        result.format("%s[%u]", baseName, (unsigned)threadId);
    }

    return result;
}

void ThreadStatistics::RegisterStatistics(StatisticVariable* statVar)
{
    if (!m_statVars.push_back(statVar)) {
        MOT_REPORT_ERROR(
            MOT_ERROR_OOM, "Register Statistics", "Failed to add statistic variable %s", statVar->GetName());
    } else if (!m_validThreads.push_back(0)) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Register Statistics",
            "Failed to add valid thread slot for statistic variable %s",
            statVar->GetName());
    }
}
}  // namespace MOT
