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
 * statistics_provider.h
 *    A component which is responsible for providing system statistics.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/stats/statistics_provider.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef STATISTICS_PROVIDER_H
#define STATISTICS_PROVIDER_H

#include "statistics_generator.h"
#include "thread_id.h"

namespace MOT {
/** @define Maximum number of character in the name of a statistics provider. */
constexpr uint32_t MAX_PROVIDER_NAME = 64;

/** @define STAT_OPT_GLOBAL Print global statistics. */
constexpr uint32_t STAT_OPT_SCOPE_GLOBAL = 0x00000001;

/** @define Print thread-level statistics. */
constexpr uint32_t STAT_OPT_SCOPE_THREAD = 0x00000002;

/** @define Print statistics collected since application startup. */
constexpr uint32_t STAT_OPT_PERIOD_TOTAL = 0x00000010;

/** @define Print statistics collected since last report. */
constexpr uint32_t STAT_OPT_PERIOD_DIFF = 0x00000020;

/** @define Print statistics summary (not per-thread). */
constexpr uint32_t STAT_OPT_LEVEL_SUMMARY = 0x00000100;

/** @define Print detailed statistics (per-thread). */
constexpr uint32_t STAT_OPT_LEVEL_DETAIL = 0x00000200;

/** @define Print statistics in all scopes. */
constexpr uint32_t STAT_OPT_SCOPE_ALL = 0x0000000F;

/** @define Print statistics in all periods. */
constexpr uint32_t STAT_OPT_PERIOD_ALL = 0x000000F0;

/** @define Print statistics in all levels. */
constexpr uint32_t STAT_APT_LEVEL_ALL = 0x00000F00;

/** @define Print all statistics. */
constexpr uint32_t STAT_OPT_ALL = 0xFFFFFFFF;

/** @define The default statistics printing option. */
constexpr uint32_t STAT_OPT_DEFAULT = (STAT_OPT_SCOPE_GLOBAL | STAT_OPT_PERIOD_TOTAL | STAT_OPT_LEVEL_SUMMARY);

/**
 * @class StatisticsProvider
 * @brief Designates a components which is responsible for providing system
 * statistics. Each such provider usually represents a single sub-system.
 * @see StatisticsManager.
 */
class StatisticsProvider {
public:
    /**
     * @brief Constructor
     * @param name The unique name that identifies the provider.
     * @param generator The statistics generator.
     * @param enable Specifies whether the statistics provider is enabled.
     * @param[opt] extended Specifies whether extended statistics are printed by this provider.
     */
    StatisticsProvider(const char* name, StatisticsGenerator* generator, bool enable, bool extended = false);

    /** @brief Destructor. */
    virtual ~StatisticsProvider();

    /**
     * @brief Initializes the object.
     * @return True if initialization succeeded, otherwise false.
     */
    bool Initialize();

    /**
     * @brief Reserves a statistics reporting slot for the current thread.
     * @return True if reservation succeeded, otherwise false.
     */
    bool ReserveThreadSlot();

    /** @brief Marks the statistics slot for the current thread as free for reuse. */
    void UnreserveThreadSlot();

    /**
     * @brief Queries whether the provider is enabled.
     * @return True is the provider is enabled, otherwise false.
     */
    inline bool IsEnabled() const
    {
        return m_enable;
    }

    /**
     * @brief Retrieves the name of the statistics provider.
     * @return The name of the statistics provider.
     */
    inline const char* GetName() const
    {
        return m_name;
    }

    /**
     * @brief Summarizes statistics for printing.
     */
    void Summarize();

    /** @brief Queries whether there are valid statistics for the given options. */
    bool HasStatisticsFor(uint32_t statOpts);

    /**
     * @brief Prints statistics managed by this provider to log file using the
     * given log level.
     * @param log_levelThe log level to use.
     * @param statOpts Printing options controlling what should be printed.
     * @see STAT_OPT_GLOBAL
     * @see STAT_OPT_THREAD
     * @see STAT_OPT_TOTAL
     * @see STAT_OPT_DIFF
     * @see STAT_OPT_DEFAULT
     */
    void PrintStatistics(LogLevel logLevel, uint32_t statOpts = STAT_OPT_DEFAULT);

    /**
     * @brief Prints statistics in all scopes and periods managed by this provider
     * to log file using the given log level.
     * @param log_levelThe log level to use.
     */
    inline void PrintAllStatistics(LogLevel logLevel)
    {
        PrintStatistics(logLevel, STAT_OPT_ALL);
    }

protected:
    /**
     * @brief Allow deriving class to print more non-standard statistics
     */
    virtual void PrintStatisticsEx(){};

    /**
     * @brief Retrieves thread-level statistics.
     * @tparam The resulting statistic variable type.
     * @param threadId The logical identifier of the thread for which statistics are to be retrieved.
     * @return The thread statistics object.
     */
    template <typename T>
    inline T* GetThreadStatisticsAt(MOTThreadId threadId)
    {
        T* result = nullptr;
        if (threadId < m_threadStatCount) {
            ThreadStatistics* stats = m_threadStats[threadId];
            if (stats != nullptr) {
                result = static_cast<T*>(stats);
            }
        }
        return result;
    }

    /**
     * @brief Retrieves current thread-level statistics.
     * @tparam The resulting statistic variable type.
     * @param threadId The logical identifier of the thread for which statistics are to be retrieved.
     * @return The thread statistics object.
     */
    template <typename T>
    inline T* GetCurrentThreadStatistics()
    {
        // we cannot reserve a thread here due to race conditions
        T* result = nullptr;
        MOTThreadId tid = MOTCurrThreadId;
        if (likely(tid != INVALID_THREAD_ID)) {
            result = GetThreadStatisticsAt<T>(tid);
        }
        return result;
    }

    /**
     * @brief Retrieves global statistics.
     * @return The global statistics object.
     */
    template <typename T>
    inline T* GetGlobalStatistics()
    {
        return static_cast<T*>(m_globalStats);
    }

    /** @var Specifies whether the provider is enabled (by default not). */
    bool m_enable;

private:
    /** @var statistics group name. */
    char m_name[MAX_PROVIDER_NAME];

    /** @var generator used to create ThreadStatistrics and GlobalStatistics objects. */
    StatisticsGenerator* m_generator;

    /** @var all thread statistics for this provider. */
    ThreadStatistics** m_threadStats;

    /** @var The number of thread statistics objects. */
    uint32_t m_threadStatCount;

    /** @brief Specified whether this provider prints extended statistics. */
    bool m_hasExtendedStats;

    /** @var Serialize access to class members. */
    pthread_spinlock_t m_statLock;

    /** @var Global statistics. */
    GlobalStatistics* m_globalStats;

    /** @var aggregate thread statistic variable. */
    ThreadStatistics* m_aggregateStats;

    /** @var previous aggregate statistic variable. */
    ThreadStatistics* m_prevAggregateStats;

    /** @var average per-thread statistic variable. */
    ThreadStatistics* m_averageStats;

    /** @var difference average per-thread statistic variable. */
    ThreadStatistics* m_diffStats;

    /** @var difference average per-thread statistic variable. */
    ThreadStatistics* m_diffAverageStats;

    /** @var Aggregate dead thread statistic variable. */
    ThreadStatistics* m_deadThreadStats;

    /** @var Global statistics. */
    GlobalStatistics* m_prevGlobalStats;

    /** @var difference average global statistic variable. */
    GlobalStatistics* m_diffGlobalStats;

    /**
     * @brief Print thread-level statistics.
     * @param statOpts Printing options.
     * @param statId Ordinal statistic variable identifier.
     * @param logLevel Printing log level.
     */
    void PrintThreadStats(uint32_t statOpts, uint32_t statId, LogLevel logLevel);

    /**
     * @brief Print global statistics.
     * @param statOpts Printing options.
     * @param statId Ordinal statistic variable identifier.
     * @param logLevel Printing log level.
     */
    void PrintGlobalStats(uint32_t statOpts, uint32_t statId, LogLevel logLevel);

    /**
     * @brief Reclaims all resources associated with a thread statistics object.
     * @param threadId The thread identifier.
     * @param threadStats The thread statistics object.
     */
    void FreeThreadStats(MOTThreadId threadId, ThreadStatistics* threadStats);
};
}  // namespace MOT
#endif /* STATISTICS_PROVIDER_H */
