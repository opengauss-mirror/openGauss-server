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
 * process_statistics.cpp
 *    Provides up-to-date system metrics for the current process.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/statistics/process_statistics.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "process_statistics.h"
#include "mot_configuration.h"
#include "config_manager.h"
#include "statistics_manager.h"
#include "mot_error.h"

#include <sys/time.h>
#include <sys/resource.h>

namespace MOT {
DECLARE_LOGGER(ProcessStatistics, Statistics)

TypedStatisticsGenerator<ThreadStatistics, EmptyGlobalStatistics> ProcessStatisticsProvider::m_generator;
ProcessStatisticsProvider* ProcessStatisticsProvider::m_provider = nullptr;

ProcessStatisticsProvider::ProcessStatisticsProvider()
    : StatisticsProvider("Process", &m_generator, GetGlobalConfiguration().m_enableProcessStatistics, true),
      m_lastCpu(0),
      m_lastSysCpu(0),
      m_lastUserCpu(0),
      m_processorCount(0),
      m_minorFaults(0),
      m_majorFaults(0),
      m_inputOps(0),
      m_outputOps(0)
{
    // register
    if (m_enable) {
        StatisticsManager::GetInstance().RegisterStatisticsProvider(this);
    }
    ConfigManager::GetInstance().AddConfigChangeListener(this);

    // take initial snapshot
    SnapshotCpuStats(m_lastCpu, m_lastSysCpu, m_lastUserCpu);

    uint64_t maxRss;
    if (!SnapMemoryIOStats(m_minorFaults, m_majorFaults, m_inputOps, m_outputOps, maxRss)) {
        MOT_LOG_WARN("Get memory IO stats failed.");
    }

    // count number of physical processors
    m_processorCount = 0;
    FILE* file = fopen("/proc/cpuinfo", "r");
    if (file != nullptr) {
        const int bufSize = 128;
        char line[bufSize];
        while (fgets(line, bufSize, file) != nullptr) {
            if (strncmp(line, "processor", 9) == 0) {  // 9 is the strlen of "processor"
                ++m_processorCount;
            }
        }
        (void)fclose(file);
    }
    MOT_LOG_DEBUG("Detected %u processors", m_processorCount);
}

ProcessStatisticsProvider::~ProcessStatisticsProvider()
{
    ConfigManager::GetInstance().RemoveConfigChangeListener(this);
    if (m_enable) {
        StatisticsManager::GetInstance().UnregisterStatisticsProvider(this);
    }
}

void ProcessStatisticsProvider::RegisterProvider()
{
    if (m_enable) {
        StatisticsManager::GetInstance().RegisterStatisticsProvider(this);
    }
    ConfigManager::GetInstance().AddConfigChangeListener(this);
}

bool ProcessStatisticsProvider::CreateInstance()
{
    bool result = false;
    MOT_ASSERT(m_provider == nullptr);
    if (m_provider == nullptr) {
        m_provider = new (std::nothrow) ProcessStatisticsProvider();
        if (m_provider == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM,
                "Load Statistics",
                "Failed to allocate memory for Process Statistics Provider, aborting");
        } else {
            result = m_provider->Initialize();
            if (!result) {
                MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                    "Load Statistics",
                    "Failed to initialize Process Statistics Provider, aborting");
                delete m_provider;
                m_provider = nullptr;
            } else {
                m_provider->RegisterProvider();
            }
        }
    }
    return result;
}

void ProcessStatisticsProvider::DestroyInstance()
{
    MOT_ASSERT(m_provider != nullptr);
    if (m_provider != nullptr) {
        delete m_provider;
        m_provider = nullptr;
    }
}

ProcessStatisticsProvider& ProcessStatisticsProvider::GetInstance()
{
    MOT_ASSERT(m_provider != nullptr);
    return *m_provider;
}

void ProcessStatisticsProvider::OnConfigChange()
{
    if (m_enable != GetGlobalConfiguration().m_enableProcessStatistics) {
        m_enable = GetGlobalConfiguration().m_enableProcessStatistics;
        if (m_enable) {
            StatisticsManager::GetInstance().RegisterStatisticsProvider(this);
        } else {
            StatisticsManager::GetInstance().UnregisterStatisticsProvider(this);
        }
    }
}

void ProcessStatisticsProvider::PrintStatisticsEx()
{
    PrintMemoryInfo();
    if (m_processorCount > 0) {
        PrintTotalCpuUsage();
    }
}

static unsigned long long ParseLine(char* line, size_t length)
{
    // This assumes that a digit will be found and the line ends in " Kb".

    // waste non-digits in beginning of line
    const char* p = line;
    size_t startPos = 0;
    while ((startPos < length) && !isdigit(*p)) {
        ++p;
        ++startPos;
    }

    // convert disregarding any size units string in the end
    unsigned long long result = strtoull(p, nullptr, 10);  // Conversion base, 10 for decimal.
    return result;
}

static void GetMemValues(unsigned long long& vmem, unsigned long long& pmem)
{
    const int bufSize = 128;
    char line[bufSize];

    int valuesParsed = 0;
    FILE* file = fopen("/proc/self/status", "r");
    if (file != nullptr) {
        while (fgets(line, bufSize, file) != nullptr) {
            if (strncmp(line, "VmSize:", 7) == 0) {  // 7 is strlen of "VmSize:"
                vmem = ParseLine(line, (size_t)bufSize);
                ++valuesParsed;
            } else if (strncmp(line, "VmRSS:", 6) == 0) {  // 6 is strlen of "VmRSS:"
                pmem = ParseLine(line, (size_t)bufSize);
                ++valuesParsed;
            }
            if (valuesParsed == 2) {  // valuesParsed 2 means "VmSize:" & "VmRSS:"
                break;
            }
        }
        fclose(file);
    }
}

void ProcessStatisticsProvider::SnapshotCpuStats(clock_t& cpu, clock_t& sysCpu, clock_t& userCpu)
{
    struct tms timeSample;
    cpu = times(&timeSample);
    sysCpu = timeSample.tms_stime;
    userCpu = timeSample.tms_utime;
    MOT_LOG_DEBUG("Snapshot CPU stats: time=%" PRIu64 ", kernel-count=%" PRIu64 ", user-count=%" PRIu64,
        (uint64_t)cpu,
        (uint64_t)sysCpu,
        (uint64_t)userCpu);
}

bool ProcessStatisticsProvider::SnapMemoryIOStats(
    uint64_t& minorFaults, uint64_t& majorFaults, uint64_t& inputOps, uint64_t& outputOps, uint64_t& maxRss)
{
    bool result = false;
    struct rusage usage;
    if (getrusage(RUSAGE_SELF, &usage) == 0) {
        minorFaults = usage.ru_minflt;
        majorFaults = usage.ru_majflt;
        inputOps = usage.ru_inblock;
        outputOps = usage.ru_oublock;
        maxRss = usage.ru_maxrss / KILO_BYTE;
        result = true;
    }
    return result;
}

void ProcessStatisticsProvider::PrintMemoryInfo()
{
    // print current process memory usage from rusage (with some other stats of page faults and io)
    uint64_t minorFaults = 0;
    uint64_t majorFaults = 0;
    uint64_t inputOps = 0;
    uint64_t outputOps = 0;
    uint64_t maxRss = 0;

    if (SnapMemoryIOStats(minorFaults, majorFaults, inputOps, outputOps, maxRss)) {
        uint64_t minorFaultsDiff = minorFaults - this->m_minorFaults;
        uint64_t majorFaultsDiff = majorFaults - this->m_majorFaults;
        uint64_t inputOpsDiff = inputOps - this->m_inputOps;
        uint64_t outputOpsDiff = outputOps - this->m_outputOps;

        MOT_LOG_INFO("rusage() Max RSS = %" PRIu64 " MB, Soft/Hard page faults: %" PRIu64 "/%" PRIu64
                     ", Input/Output operations: %" PRIu64 "/%" PRIu64,
            maxRss,
            minorFaultsDiff,
            majorFaultsDiff,
            inputOpsDiff,
            outputOpsDiff);

        this->m_minorFaults = minorFaults;
        this->m_majorFaults = majorFaults;
        this->m_inputOps = inputOps;
        this->m_outputOps = outputOps;
    }

    // print from /proc
    unsigned long size, resident, share, text, lib, data, dt;
    FILE* f = fopen("/proc/self/statm", "r");
    if (f != nullptr) {
        // 7 here is the number of inputs given in the fscanf statement
        // (&size, &resident, &share, &text, &lib, &data, &dt)
        if (7 == fscanf_s(f, "%lu %lu %lu %lu %lu %lu %lu", &size, &resident, &share, &text, &lib, &data, &dt)) {
            MOT_LOG_INFO("statm: VmSize = %lu MB, VmRSS = %lu MB, VmData+VmStk = %lu MB, VmShare = %lu KB, VmExe = %lu "
                         "KB, VmLib = %lu KB",
                size / KILO_BYTE,
                resident / KILO_BYTE,
                data / KILO_BYTE,
                share,
                text,
                lib);
        }
        fclose(f);
    }

    // third way: from /proc/self/status
    // virtual memory used by current process
    unsigned long long vmem = 0;
    unsigned long long pmem = 0;
    GetMemValues(vmem, pmem);
    MOT_LOG_INFO("Total process virtual/physical memory usage: %llu/%llu MB", vmem / KILO_BYTE, pmem / KILO_BYTE);
}

void ProcessStatisticsProvider::PrintTotalCpuUsage()
{
    clock_t cpu;
    clock_t sysCpu;
    clock_t userCpu;
    double percent;
    double percentUser;
    double percentSys;

    SnapshotCpuStats(cpu, sysCpu, userCpu);
    if (cpu <= m_lastCpu || sysCpu < m_lastSysCpu || userCpu < m_lastUserCpu) {
        // Overflow detection. Just skip this sample.
        percent = -1.0;
        percentSys = -1.0;
        percentUser = -1.0;
    } else {
        percentSys = ((double)(sysCpu - m_lastSysCpu)) / (cpu - m_lastCpu) / m_processorCount * 100.0f;
        percentUser = ((double)(userCpu - m_lastUserCpu)) / (cpu - m_lastCpu) / m_processorCount * 100.0f;
        percent = percentSys + percentUser;
    }

    m_lastCpu = cpu;
    m_lastSysCpu = sysCpu;
    m_lastUserCpu = userCpu;

    MOT_LOG_INFO("Total process CPU usage: %0.2f%% (user-space: %0.2f%%, kernel-space: %0.2f%%)",
        percent,
        percentUser,
        percentSys);
}
}  // namespace MOT
