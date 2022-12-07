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
 * statistics_manager.h
 *    Singleton access point to system statistics.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/stats/statistics_manager.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef STATISTICS_MANAGER_H
#define STATISTICS_MANAGER_H

#include "iconfig_change_listener.h"
#include "statistics_provider.h"
#include "mot_list.h"

#include <pthread.h>

namespace MOT {
/**
 * @class StatisticsManager
 * @brief Singleton access point to system statistics.
 */
class StatisticsManager : public IConfigChangeListener {
public:
    /**
     * @brief Creates singleton instance. Must be called once during engine startup.
     * @return true if succeeded otherwise false.
     */
    static bool CreateInstance();

    /**
     * @brief Destroys singleton instance. Must be called once during engine shutdown.
     */
    static void DestroyInstance();

    /**
     * @brief Retrieves reference to singleton instance.
     * @return Statistics manager
     */
    static StatisticsManager& GetInstance();

    /** @brief Starts the statistics printing thread. */
    inline bool Start()
    {
        return StartStatsPrintThread();
    }

    /** @brief Stops the statistics printing thread. */
    inline void Stop()
    {
        StopStatsPrintThread();
    }

    /**
     * @brief Registers a statistics provider.
     * @param statisticsProvider The statistics provider to register.
     * @return Boolean value denoting success or failure.
     */
    bool RegisterStatisticsProvider(StatisticsProvider* statisticsProvider);

    /**
     * @brief Cancels registration of a statistics provider.
     * @param statisticsProvider The statistics provider to cancel its registration.
     * @return Boolean value denoting success or failure.
     */
    bool UnregisterStatisticsProvider(StatisticsProvider* statisticsProvider);

    /**
     * @brief Retrieves a statistics provider by its name.
     * @return The requested statistics provider or nullptr it none was found.
     */
    StatisticsProvider* GetStatisticsProvider(const char* name);

    /**
     * @brief Reserves a statistics reporting slot for the current thread.
     * @return True if reservation succeeded, otherwise false.
     */
    bool ReserveThreadSlot();

    /** @brief Marks the statistics slot for the current thread as free for reuse. */
    void UnreserveThreadSlot();

    /**
     * @brief Prints all system statistics.
     * @param scope Specifies whether to print global or thread-level statistics.
     * @param period Specifies whether to print total or differential report
     * since last report.
     * @param logLevel Specifies the printing log level.
     */
    void PrintStatistics(LogLevel logLevel, uint32_t statOpts = STAT_OPT_DEFAULT);

    /**
     * @brief Prints all system statistics in all scopes and periods.
     * @param logLevel Specifies the printing log level.
     */
    inline void PrintAllStatistics(LogLevel logLevel)
    {
        PrintStatistics(logLevel, STAT_OPT_ALL);
    }

    /**
     * @brief Derives classes should react to a notification that configuration changed. New
     * configuration is accessible via the ConfigManager.
     */
    void OnConfigChange() override;

private:
    /** @var The single instance. */
    static StatisticsManager* m_manager;

    /** @var All registered statistics providers in the system. */
    mot_list<StatisticsProvider*> m_providers;

    /** @var Statistics providers lock. */
    pthread_mutex_t m_providersLock;

    /** @var Statistics printing period in seconds. */
    uint64_t m_statsPrintPeriodSeconds;

    /** @var Full statistics printing period in seconds. */
    uint64_t m_fullStatsPrintPeriodSeconds;

    /** @var The statistics printing worker thread handle. */
    pthread_t m_statsThread;

    /** @var Statistics printing lock for implementing interruptible sleep. */
    pthread_mutex_t m_statsPrintLock;

    /** @var Statistics printing condition variable for implementing interruptible sleep. */
    pthread_cond_t m_statsPrintCond;

    /** @var Designates whether the statistics printing thread is running. */
    bool m_running;

    /** @var Initialization phase (for safe cleanup). */
    enum InitPhase { INIT, INIT_PROVIDERS_LOCK, INIT_STAT_PRINT_LOCK, INIT_STAT_PRINT_CV, DONE } m_initPhase;

    /** @brief Constructor. */
    StatisticsManager();

    /** @brief Destructor. */
    ~StatisticsManager() override;

    /**
     *  @brief Initializes the manager.
     *  @return True if initialization succeeded, otherwise false.
     */
    bool Initialize();

    /**
     * @brief Starts the statistics printing thread running.
     *  @return True if the statistic printing thread started successfully, otherwise false.
     */
    bool StartStatsPrintThread();

    /**
     * @brief Stops the statistics printing thread running.
     */
    void StopStatsPrintThread();

    /**
     * @brief A static delegating function to class method.
     * @param param The thread parameter (this pointer).
     * @return Unused.
     */
    static void* StatsPrintThreadStatic(void* param);

    /**
     * @brief The statistics printing thread routine.
     */
    void StatsPrintThread();

    /**
     * @brief Waits for the next statistics printing period. May be interrupted to work earlier when shutting down.
     */
    void WaitNextPrint();
};
}  // namespace MOT

/**
 * Debug print statistics. May be called through gdb.
 */
extern void dumpStats();

#endif /* STATISTICS_MANAGER_H */
