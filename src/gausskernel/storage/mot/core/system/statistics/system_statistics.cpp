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
 * system_statistics.cpp
 *    Provides up-to-date system metrics for the whole machine.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/statistics/system_statistics.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "global.h"
#include "system_statistics.h"
#include "mot_configuration.h"
#include "config_manager.h"
#include "statistics_manager.h"
#include "mot_error.h"

#include "sys/types.h"
#include "sys/sysinfo.h"

namespace MOT {
DECLARE_LOGGER(SystemStatistics, Statistics)

TypedStatisticsGenerator<ThreadStatistics, EmptyGlobalStatistics> SystemStatisticsProvider::m_generator;

SystemStatisticsProvider* SystemStatisticsProvider::m_provider = nullptr;

SystemStatisticsProvider::SystemStatisticsProvider()
    : StatisticsProvider("System", &m_generator, GetGlobalConfiguration().m_enableSystemStatistics, true),
      m_lastTotalUser(0),
      m_lastTotalUserLow(0),
      m_lastTotalSys(0),
      m_lastTotalIdle(0)
{}

void SystemStatisticsProvider::RegisterProvider()
{
    if (m_enable) {
        (void)StatisticsManager::GetInstance().RegisterStatisticsProvider(this);
    }
    (void)ConfigManager::GetInstance().AddConfigChangeListener(this);
}

SystemStatisticsProvider::~SystemStatisticsProvider()
{
    (void)ConfigManager::GetInstance().RemoveConfigChangeListener(this);
    if (m_enable) {
        (void)StatisticsManager::GetInstance().UnregisterStatisticsProvider(this);
    }
}

bool SystemStatisticsProvider::CreateInstance()
{
    bool result = false;
    MOT_ASSERT(m_provider == nullptr);
    if (m_provider == nullptr) {
        m_provider = new (std::nothrow) SystemStatisticsProvider();
        if (m_provider == nullptr) {
            MOT_REPORT_ERROR(
                MOT_ERROR_OOM, "Load Statistics", "Failed to allocate memory for System Statistics Provider, aborting");
        } else {
            if (!m_provider->Initialize()) {
                MOT_REPORT_ERROR(
                    MOT_ERROR_INTERNAL, "Load Statistics", "Failed to initialize System Statistics Provider, aborting");
                delete m_provider;
                m_provider = nullptr;
            } else {
                if (!m_provider->SnapshotInitialMetrics()) {
                    MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                        "Load Statistics",
                        "Failed to take initial metrics snapshot in System Statistics Provider, aborting");
                    delete m_provider;
                    m_provider = nullptr;
                } else {
                    m_provider->RegisterProvider();
                    result = true;
                }
            }
        }
    }
    return result;
}

void SystemStatisticsProvider::DestroyInstance()
{
    MOT_ASSERT(m_provider != nullptr);
    if (m_provider != nullptr) {
        delete m_provider;
        m_provider = nullptr;
    }
}

SystemStatisticsProvider& SystemStatisticsProvider::GetInstance()
{
    MOT_ASSERT(m_provider != nullptr);
    return *m_provider;
}

bool SystemStatisticsProvider::SnapshotInitialMetrics()
{
    return SnapshotCpuStats(m_lastTotalUser, m_lastTotalUserLow, m_lastTotalSys, m_lastTotalIdle);
}

void SystemStatisticsProvider::OnConfigChange()
{
    if (m_enable != GetGlobalConfiguration().m_enableSystemStatistics) {
        m_enable = GetGlobalConfiguration().m_enableSystemStatistics;
        if (m_enable) {
            (void)StatisticsManager::GetInstance().RegisterStatisticsProvider(this);
        } else {
            (void)StatisticsManager::GetInstance().UnregisterStatisticsProvider(this);
        }
    }
}

void SystemStatisticsProvider::PrintStatisticsEx()
{
    PrintMemoryInfo();
    PrintTotalCpuUsage();
}

bool SystemStatisticsProvider::SnapshotCpuStats(
    uint64_t& totalUser, uint64_t& totalUserLow, uint64_t& totalSys, uint64_t& totalIdle) const
{
    bool result = false;
    FILE* file = fopen("/proc/stat", "r");
    if (file != nullptr) {
        unsigned long long ullTotalUser, ullTotalUserLow, ullTotalSys, ullTotalIdle;
        // 4 here is the number of inputs given in fscanf (&ullTotalUser, &ullTotalUserLow, &ullTotalSys, &ullTotalIdle)
        if (4 ==
            fscanf_s(file, "cpu %llu %llu %llu %llu", &ullTotalUser, &ullTotalUserLow, &ullTotalSys, &ullTotalIdle)) {
            totalUser = ullTotalUser;
            totalUserLow = ullTotalUserLow;
            totalSys = ullTotalSys;
            totalIdle = ullTotalIdle;
            result = true;
        }
        if (!result) {
            MOT_LOG_DEBUG("Failed to get system stats from /proc/stat: cpu line not found");
        }
        (void)fclose(file);
    } else {
        MOT_LOG_DEBUG("Failed to get system stats from /proc/stat: cannot open file for reading");
    }
    MOT_LOG_DEBUG("Snapshot system CPU stats: totalUser=%" PRIu64 ", totalUserLow=%" PRIu64 ", totalSys=%" PRIu64
                  ", totalIdle=%" PRIu64,
        totalUser,
        totalUserLow,
        totalSys,
        totalIdle);
    return result;
}

void SystemStatisticsProvider::PrintMemoryInfo() const
{
    struct sysinfo memInfo;
    if (sysinfo(&memInfo) != 0) {
        MOT_LOG_TRACE("Failed to retrieve sysinfo");
        return;
    }

    // total virtual memory in the system
    uint64_t totalVirtualMem = memInfo.totalram;
    totalVirtualMem += memInfo.totalswap;
    totalVirtualMem *= memInfo.mem_unit;

    // currently used virtual memory in the system
    uint64_t virtualMemUsed = memInfo.totalram - memInfo.freeram;
    virtualMemUsed += memInfo.totalswap - memInfo.freeswap;
    virtualMemUsed *= memInfo.mem_unit;
    MOT_LOG_INFO("Total system virtual memory usage: %" PRIu64 "/%" PRIu64 " MB",
        virtualMemUsed / MEGA_BYTE,
        totalVirtualMem / MEGA_BYTE);

    // total physical memory in the system
    uint64_t totalPhysMem = memInfo.totalram;
    totalPhysMem *= memInfo.mem_unit;

    // currently used virtual memory in the system
    uint64_t physMemUsed = memInfo.totalram - memInfo.freeram;
    physMemUsed *= memInfo.mem_unit;
    MOT_LOG_INFO("Total system physical memory usage: %" PRIu64 "/%" PRIu64 " MB",
        physMemUsed / MEGA_BYTE,
        totalPhysMem / MEGA_BYTE);
}

void SystemStatisticsProvider::PrintTotalCpuUsage()
{
    double percent = 0.0f;
    double percentUser = 0.0f;
    double percentSys = 0.0f;
    uint64_t totalUser = 0;
    uint64_t totalUserLow = 0;
    uint64_t totalSys = 0;
    uint64_t totalIdle = 0;
    uint64_t total = 0;
    uint64_t diffUser = 0;
    uint64_t diffUserLow = 0;
    uint64_t diffSys = 0;
    uint64_t diffIdle = 0;

    if (SnapshotCpuStats(totalUser, totalUserLow, totalSys, totalIdle)) {
        if (totalUser < m_lastTotalUser || totalUserLow < m_lastTotalUserLow || totalSys < m_lastTotalSys ||
            totalIdle < m_lastTotalIdle) {
            // Overflow detection. Just skip this value.
            percent = -1.0;
        } else {
            diffUser = (totalUser - m_lastTotalUser);
            diffUserLow = (totalUserLow - m_lastTotalUserLow);
            diffSys = (totalSys - m_lastTotalSys);
            diffIdle = (totalIdle - m_lastTotalIdle);
            total = diffUser + diffUserLow + diffSys + diffIdle;
            if (total > 0) {
                percentUser = (((double)(diffUser + diffUserLow)) / total) * 100.0f;
                percentSys = (((double)diffSys) / total) * 100.0f;
                percent = percentUser + percentSys;
            }
        }

        m_lastTotalUser = totalUser;
        m_lastTotalUserLow = totalUserLow;
        m_lastTotalSys = totalSys;
        m_lastTotalIdle = totalIdle;

        MOT_LOG_INFO("Total system CPU usage: %0.2f%% (user-space: %0.2f%%, kernel-space: %0.2f%%",
            percent,
            percentUser,
            percentSys);
    }
}
}  // namespace MOT
