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
 * system_statistics.h
 *    Provides up-to-date system metrics for the whole machine.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/statistics/system_statistics.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef SYSTEM_STATISTICS_H
#define SYSTEM_STATISTICS_H

#include "iconfig_change_listener.h"
#include "statistics_provider.h"
#include "typed_statistics_generator.h"

namespace MOT {
/**
 * @brief The system statistics provider presents up-to-date system metrics for the whole machine, and therefore
 * does not have any statistic variables/counters.
 */
class SystemStatisticsProvider : public StatisticsProvider, public IConfigChangeListener {
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
     * @return Database session statistics provider
     */
    static SystemStatisticsProvider& GetInstance();

    /** @brief Takes an initial snapshot of monitored system metrics. */
    bool SnapshotInitialMetrics();

    /**
     * @brief Derives classes should react to a notification that configuration changed. New
     * configuration is accessible via the ConfigManager.
     */
    virtual void OnConfigChange();

protected:
    /**
     * @brief Override default behavior, and print current system status summary.
     */
    virtual void PrintStatisticsEx();

private:
    /** @brief Constructor. */
    SystemStatisticsProvider();

    /** @brief Destructor. */
    virtual ~SystemStatisticsProvider();

    /** @brief Registers the provider in the manager. */
    void RegisterProvider();

    /** @var The single instance. */
    static SystemStatisticsProvider* m_provider;

    /** @var The generator used for creating global and thread-level statistics objects. */
    static TypedStatisticsGenerator<ThreadStatistics, EmptyGlobalStatistics> m_generator;

    /** @var Time spent in user mode. */
    uint64_t m_lastTotalUser;

    /** @var Time spent in user mode with low priority (nice). */
    uint64_t m_lastTotalUserLow;

    /** @var Time spent in system mode. */
    uint64_t m_lastTotalSys;

    /** @var Time spent in the idle task. */
    uint64_t m_lastTotalIdle;

    /**
     * @brief Take a snapshot of total system CPU usage.
     * @param totalUser Total time spent in user mode.
     * @param totalUserLow Total time spent in user mode with low priority (nice).
     * @param totalSys Total time spent in system mode.
     * @param totalIdle Total time spent in the idle task.
     * @return True if operations succeeded and output data is valid, otherwise false.
     */
    bool SnapshotCpuStats(uint64_t& totalUser, uint64_t& totalUserLow, uint64_t& totalSys, uint64_t& totalIdle);

    /** @brief Prints memory usage statistics. */
    void PrintMemoryInfo();

    /** @brief Prints CPU usage statistics. */
    void PrintTotalCpuUsage();
};
}  // namespace MOT

#endif /* SYSTEM_STATISTICS_H */
