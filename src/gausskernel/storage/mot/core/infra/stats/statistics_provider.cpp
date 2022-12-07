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
 * statistics_provider.cpp
 *    A component which is responsible for providing system statistics.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/stats/statistics_provider.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "statistics_provider.h"
#include "mot_engine.h"
#include "thread_id.h"
#include "session_context.h"

#include "mm_numa.h"
#include "mm_api.h"
#include "mot_error.h"

namespace MOT {
DECLARE_LOGGER(StatisticsProvider, Statistics)

StatisticsProvider::StatisticsProvider(
    const char* name, StatisticsGenerator* generator, bool enable, bool extended /* = false */)
    : m_enable(enable),
      m_generator(generator),
      m_threadStats(nullptr),
      m_threadStatCount(0),
      m_hasExtendedStats(extended),
      m_statLock(0),
      m_globalStats(nullptr),
      m_aggregateStats(nullptr),
      m_prevAggregateStats(nullptr),
      m_averageStats(nullptr),
      m_diffStats(nullptr),
      m_diffAverageStats(nullptr),
      m_deadThreadStats(nullptr),
      m_prevGlobalStats(nullptr),
      m_diffGlobalStats(nullptr)
{
    errno_t erc = snprintf_s(m_name, MAX_PROVIDER_NAME, MAX_PROVIDER_NAME - 1, "%s", name);
    securec_check_ss(erc, "\0", "\0");
}

StatisticsProvider::~StatisticsProvider()
{
    if (m_globalStats) {
        delete m_globalStats;
    }
    if (m_prevGlobalStats) {
        delete m_prevGlobalStats;
    }
    if (m_diffGlobalStats) {
        delete m_diffGlobalStats;
    }

    if (m_threadStats) {
        for (uint32_t i = 0; i < m_threadStatCount; ++i) {
            if (m_threadStats[i] != nullptr) {
                FreeThreadStats(i, m_threadStats[i]);
            }
        }
        free(m_threadStats);
    }

    if (m_aggregateStats) {
        delete m_aggregateStats;
    }
    if (m_prevAggregateStats) {
        delete m_prevAggregateStats;
    }
    if (m_averageStats) {
        delete m_averageStats;
    }
    if (m_diffStats) {
        delete m_diffStats;
    }
    if (m_diffAverageStats) {
        delete m_diffAverageStats;
    }

    if (m_deadThreadStats) {
        delete m_deadThreadStats;
    }

    // Generator will be destroyed by the provider
    m_generator = nullptr;
}

bool StatisticsProvider::Initialize()
{
    m_aggregateStats = m_generator->CreateThreadStatistics(ThreadStatistics::THREAD_ID_TOTAL);
    m_prevAggregateStats = m_generator->CreateThreadStatistics(ThreadStatistics::THREAD_ID_TOTAL);
    m_averageStats = m_generator->CreateThreadStatistics(ThreadStatistics::THREAD_ID_AVG);
    m_diffStats = m_generator->CreateThreadStatistics(ThreadStatistics::THREAD_ID_DIFF);
    m_diffAverageStats = m_generator->CreateThreadStatistics(ThreadStatistics::THREAD_ID_DIFF_AVG);
    m_deadThreadStats = m_generator->CreateThreadStatistics(ThreadStatistics::THREAD_ID_TOTAL);
    if (!m_aggregateStats || !m_prevAggregateStats || !m_averageStats || !m_diffStats || !m_diffAverageStats ||
        !m_deadThreadStats) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Load Statistics", "Failed to create thread statistics object(s)");
        return false;  // safe cleanup in object destruction
    }

    m_threadStatCount = GetGlobalConfiguration().m_maxThreads;
    m_threadStats = (ThreadStatistics**)calloc(m_threadStatCount, sizeof(ThreadStatistics*));
    if (!m_threadStats) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Load Statistics",
            "Failed to create thread statistics array in size of %u slots",
            m_threadStatCount);
        return false;
    }

    m_globalStats = m_generator->CreateGlobalStatistics(GlobalStatistics::NamingScheme::NAMING_SCHEME_TOTAL);
    m_prevGlobalStats = m_generator->CreateGlobalStatistics(GlobalStatistics::NamingScheme::NAMING_SCHEME_TOTAL);
    m_diffGlobalStats = m_generator->CreateGlobalStatistics(GlobalStatistics::NamingScheme::NAMING_SCHEME_DIFF);
    if (!m_globalStats || !m_prevGlobalStats || !m_diffGlobalStats) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Load Statistics", "Failed to create global statistics object(s)");
        return false;
    }

    int rc = pthread_spin_init(&m_statLock, 0);
    if (rc != 0) {
        MOT_REPORT_SYSTEM_ERROR_CODE(rc, pthread_spin_init, "Load Statistics", "Failed to create statistics lock");
        return false;
    }

    return true;
}

bool StatisticsProvider::ReserveThreadSlot()
{
    bool result = false;

    MOTThreadId threadId = MOTCurrThreadId;
    int node = MOTCurrentNumaNodeId;
    if ((threadId == INVALID_THREAD_ID) || (node == MEM_INVALID_NODE)) {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
            "Reserve Thread Slot for Statistics",
            "Invalid attempt to reserve statistics thread slot without current thread/node identifier denied (thread "
            "id: %u, node id: %d)",
            (unsigned)threadId,
            node);
    } else {
        // there is no race here because only the current thread modifies its own slot
        if (m_threadStats[threadId] != nullptr) {
            MOT_LOG_TRACE("Double attempt to reserve statistics thread slot for thread %" PRIu16
                          " silently ignored: thread slot already reserved",
                threadId);
            result = true;
        } else {
            MOT_LOG_TRACE("Reserving %s statistics thread slot for thread id %" PRIu16, GetName(), threadId);
            void* buffer = nullptr;
            // since statistics provider is created before MemInit(), it is preferred to keep it clean from MM API calls
            if (GetGlobalConfiguration().m_numaNodes > 1) {
                buffer = MemNumaAllocLocal(m_generator->GetObjectSize(), node);
            } else {
                buffer = malloc(m_generator->GetObjectSize());
            }
            if (buffer == nullptr) {
                MOT_REPORT_ERROR(MOT_ERROR_OOM,
                    "Reserve Thread Slot for Statistics",
                    "Failed to allocate buffer in size of %u bytes",
                    m_generator->GetObjectSize());
            } else {
                (void)pthread_spin_lock(&m_statLock);
                m_threadStats[threadId] = m_generator->CreateThreadStatistics(threadId, buffer);
                (void)pthread_spin_unlock(&m_statLock);
                MOT_LOG_DEBUG("m_threadStats[%u] = %p", (unsigned)threadId, m_threadStats[threadId]);
                result = true;
            }
        }
    }

    return result;
}

void StatisticsProvider::UnreserveThreadSlot()
{
    // move the statistics to the dead thread list
    // it will stay there until next summary printing, and then reclaimed
    MOTThreadId threadId = MOTCurrThreadId;
    if (threadId == INVALID_THREAD_ID) {
        MOT_LOG_ERROR("Invalid attempt to un-reserve statistics thread slot without current thread identifier denied");
    } else {
        if (m_threadStats[threadId] == nullptr) {
            MOT_LOG_TRACE("Attempt to unreserve statistics thread slot for thread %" PRIu16
                          " silently ignored: thread slot already unreserved",
                threadId);
        } else {
            // aggregate dead thread statistics and cleanup
            (void)pthread_spin_lock(&m_statLock);
            ThreadStatistics* threadStats = m_threadStats[threadId];
            m_deadThreadStats->Add(*threadStats);
            m_threadStats[threadId] = nullptr;  // this must be guarded with a lock due to race with Summarize()
            (void)pthread_spin_unlock(&m_statLock);
            FreeThreadStats(threadId, threadStats);  // cleanup now outside lock scope
            MOT_LOG_TRACE("Unreserved %s statistics thread slot for thread id %" PRIu16, GetName(), threadId);
        }
    }
}

void StatisticsProvider::Summarize()
{
    (void)pthread_spin_lock(&m_statLock);

    m_prevAggregateStats->Assign(*m_aggregateStats);
    m_aggregateStats->Reset();
    m_averageStats->Reset();

    // accumulate all thread statistics to aggregate statistics
    for (uint32_t i = 0; i < m_threadStatCount; ++i) {
        if (m_threadStats[i] != nullptr) {
            m_threadStats[i]->Summarize(true);
            m_aggregateStats->Add(*m_threadStats[i]);
        }
    }

    // accumulate all dead thread statistics into aggregate statistics
    m_aggregateStats->Add(*m_deadThreadStats);
    m_deadThreadStats->Reset();

    (void)pthread_spin_unlock(&m_statLock);

    // from this point onward there is no race over m_threadStats or m_deadThreadStats
    m_aggregateStats->Summarize(false);

    // compute engine-level average statistics over all threads since start of run
    m_averageStats->Assign(*m_aggregateStats);
    m_averageStats->Normalize();
    m_averageStats->Summarize(false);

    // compute diff statistics since last report (per-thread and total average)
    m_diffStats->Assign(*m_aggregateStats);
    m_diffStats->Subtract(*m_prevAggregateStats);
    m_diffStats->Summarize(false);
    m_diffAverageStats->Assign(*m_diffStats);
    m_diffAverageStats->Normalize();
    m_diffAverageStats->Summarize(false);

    // compute global statistics since last report
    m_globalStats->Summarize(true);
    m_diffGlobalStats->Assign(*m_globalStats);
    m_diffGlobalStats->Subtract(*m_prevGlobalStats);
    m_diffGlobalStats->Summarize(false);
    m_prevGlobalStats->Assign(*m_globalStats);
}

bool StatisticsProvider::HasStatisticsFor(uint32_t statOpts)
{
    bool result = m_hasExtendedStats;
    if (!result && (statOpts & STAT_OPT_SCOPE_THREAD)) {
        if (statOpts & STAT_OPT_LEVEL_SUMMARY) {
            result = m_diffStats->HasValidSamples();
        }
        if (!result && (statOpts & STAT_OPT_LEVEL_DETAIL)) {
            result = m_diffAverageStats->HasValidSamples();
        }
    }

    if (!result && (statOpts & STAT_OPT_SCOPE_GLOBAL)) {
        if (statOpts & STAT_OPT_LEVEL_DETAIL) {
            (void)pthread_spin_lock(&m_statLock);
            for (uint32_t i = 0; i < m_threadStatCount; ++i) {
                if (m_threadStats[i] != nullptr) {
                    result = m_threadStats[i]->HasValidSamples();
                    if (result) {
                        break;
                    }
                }
            }
            (void)pthread_spin_unlock(&m_statLock);
            if (!result) {
                result = m_averageStats->HasValidSamples();
            }
        }
        if (!result && (statOpts & STAT_OPT_LEVEL_SUMMARY)) {
            result = m_aggregateStats->HasValidSamples();
        }
    }

    return result;
}

void StatisticsProvider::PrintStatistics(LogLevel logLevel, uint32_t statOpts /* = STAT_OPT_DEFAULT */)
{
    if (statOpts & STAT_OPT_SCOPE_THREAD) {
        for (uint32_t statId = 0; statId < m_aggregateStats->GetStatCount(); ++statId) {
            PrintThreadStats(statOpts, statId, logLevel);
        }
    }
    if (statOpts & STAT_OPT_SCOPE_GLOBAL) {
        for (uint32_t statId = 0; statId < m_globalStats->GetStatCount(); ++statId) {
            PrintGlobalStats(statOpts, statId, logLevel);
        }
    }

    PrintStatisticsEx();
}

void StatisticsProvider::PrintThreadStats(uint32_t statOpts, uint32_t statId, LogLevel logLevel)
{
    // print diff stats
    if (statOpts & STAT_OPT_PERIOD_DIFF) {
        if (statOpts & STAT_OPT_LEVEL_SUMMARY) {
            m_diffStats->Print(statId, logLevel);
        }
        if (statOpts & STAT_OPT_LEVEL_DETAIL) {
            m_diffAverageStats->Print(statId, logLevel);
        }
    }

    // print total stats
    if (statOpts & STAT_OPT_PERIOD_TOTAL) {
        if (statOpts & STAT_OPT_LEVEL_DETAIL) {
            (void)pthread_spin_lock(&m_statLock);
            for (uint32_t i = 0; i < m_threadStatCount; ++i) {
                if (m_threadStats[i] != nullptr) {
                    m_threadStats[i]->Print(statId, logLevel);
                }
            }
            (void)pthread_spin_unlock(&m_statLock);
            m_averageStats->Print(statId, logLevel);
        }
        if (statOpts & STAT_OPT_LEVEL_SUMMARY) {
            m_aggregateStats->Print(statId, logLevel);
        }
    }
}

void StatisticsProvider::PrintGlobalStats(uint32_t statOpts, uint32_t statId, LogLevel logLevel) const
{
    // print diff stats
    if (statOpts & STAT_OPT_PERIOD_DIFF) {
        m_diffGlobalStats->Print(statId, logLevel);
    }

    // print total stats
    if ((statOpts & STAT_OPT_PERIOD_TOTAL) && !(statOpts & STAT_OPT_LEVEL_DETAIL)) {
        m_globalStats->Print(statId, logLevel);
    }
}

void StatisticsProvider::FreeThreadStats(MOTThreadId threadId, ThreadStatistics* threadStats)
{
    MOT_LOG_TRACE("Reclaiming %s statistics thread slot for thread id %" PRIu16, GetName(), threadId);
    void* buffer = (void*)threadStats->GetInPlaceBuffer();
    int node = threadStats->GetNodeId();
    threadStats->~ThreadStatistics();
    if (GetGlobalConfiguration().m_numaNodes > 1) {
        MemNumaFreeLocal(buffer, m_generator->GetObjectSize(), node);
    } else {
        free(buffer);
    }
}
}  // namespace MOT
