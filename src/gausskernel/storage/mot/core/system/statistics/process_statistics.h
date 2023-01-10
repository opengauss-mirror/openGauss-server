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
 * process_statistics.h
 *    Provides process level up-to-date system metrics.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/statistics/process_statistics.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef PROCESS_STATISTICS_H
#define PROCESS_STATISTICS_H

#include "iconfig_change_listener.h"
#include "statistics_provider.h"
#include "typed_statistics_generator.h"

#include "sys/times.h"
#ifndef OPENEULER_MAJOR
#include "sys/vtimes.h"
#endif

namespace MOT {
/**
 * @brief The process statistics provider presents up-to-date system metrics for the current process, and therefore
 * does not have any statistic variables/counters.
 */
class ProcessStatisticsProvider : public StatisticsProvider, public IConfigChangeListener {
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
    static ProcessStatisticsProvider& GetInstance();

    /**
     * @brief Derives classes should react to a notification that configuration changed. New
     * configuration is accessible via the ConfigManager.
     */
    void OnConfigChange() override;

protected:
    /**
     * @brief Override default behavior, and print current process status summary.
     */
    void PrintStatisticsEx() override;

private:
    /** @brief Constructor. */
    ProcessStatisticsProvider();

    /** @brief Destructor. */
    ~ProcessStatisticsProvider() override;

    /** @brief Registers the provider in the manager. */
    void RegisterProvider();

    /** @var The single instance. */
    static ProcessStatisticsProvider* m_provider;

    /** @var The generator used for creating global and thread-level statistics objects. */
    static TypedStatisticsGenerator<ThreadStatistics, EmptyGlobalStatistics> m_generator;

    /** @var last cpu count seen for this process. */
    clock_t m_lastCpu;

    /** @var last user-space cpu count seen for this process. */
    clock_t m_lastSysCpu;

    /** @var last kernel-space cpu count seen for this process. */
    clock_t m_lastUserCpu;

    /** @var Number of processors. */
    uint32_t m_processorCount;

    /** @var Number of minor page faults. */
    uint64_t m_minorFaults;

    /** @var Number of major page faults. */
    uint64_t m_majorFaults;

    /** @var Number of input operations. */
    uint64_t m_inputOps;

    /** @var Number of output operations. */
    uint64_t m_outputOps;

    /**
     * @brief Take a snapshot of CPU counters for this process.
     * @param cpu Total CPU usage count.
     * @param sysCpu Total CPU usage count in kernel-space.
     * @param userCpu Total CPU usage count in user-space.
     */
    void SnapshotCpuStats(clock_t& cpu, clock_t& sysCpu, clock_t& userCpu) const;

    /**
     * @brief Take a snapshot of memory and I/O counters for this process.
     * @param minorFaults Total minor page faults count.
     * @param majorFfaults Total major page faults count.
     * @param inputOps Total input operations.
     * @param outputOps Total output operations.
     * @param maxRss Maximum resident set size.
     * @return True if operations succeeded and output data is valid, otherwise false.
     */
    bool SnapMemoryIOStats(
        uint64_t& minorFaults, uint64_t& majorFfaults, uint64_t& inputOps, uint64_t& outputOps, uint64_t& maxRss) const;

    /** @brief Prints memory usage statistics. */
    void PrintMemoryInfo();

    /** @brief Prints CPU usage statistics. */
    void PrintTotalCpuUsage();
};
}  // namespace MOT

#endif /* PROCESS_STATISTICS_H */
