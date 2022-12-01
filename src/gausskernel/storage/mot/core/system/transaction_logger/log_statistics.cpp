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
 * log_statistics.cpp
 *    Transaction log statistics.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/transaction_logger/log_statistics.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "log_statistics.h"
#include "mot_configuration.h"
#include "config_manager.h"
#include "statistics_manager.h"
#include "mot_error.h"

#include <cmath>    // sqrt
#include <sstream>  // stringstream
using namespace std;

namespace MOT {
DECLARE_LOGGER(LogStatisticsProvider, Logger)

LogThreadStatistics::LogThreadStatistics(uint64_t threadId, void* inplaceBuffer)
    : ThreadStatistics(threadId, inplaceBuffer),
      m_txnBuffersDrained(MakeName("txn-buffers-drained", threadId).c_str()),
      m_txnBytesDrained(MakeName("txn-bytes-drained", threadId).c_str()),
      m_bytesWritten(MakeName("log-bytes-written", threadId).c_str())
{
    RegisterStatistics(&m_txnBuffersDrained);
    RegisterStatistics(&m_txnBytesDrained);
}

LogGlobalStatistics::LogGlobalStatistics(GlobalStatistics::NamingScheme namingScheme)
    : GlobalStatistics(), m_logFlushes(MakeName("log-flushes", namingScheme).c_str())
{
    RegisterStatistics(&m_logFlushes);
}

TypedStatisticsGenerator<LogThreadStatistics, LogGlobalStatistics> LogStatisticsProvider::m_generator;

LogStatisticsProvider* LogStatisticsProvider::m_provider = nullptr;

LogStatisticsProvider::LogStatisticsProvider()
    : StatisticsProvider("Log", &m_generator, GetGlobalConfiguration().m_enableLogStatistics)
{}

LogStatisticsProvider::~LogStatisticsProvider()
{
    (void)ConfigManager::GetInstance().RemoveConfigChangeListener(this);
    if (m_enable) {
        (void)StatisticsManager::GetInstance().UnregisterStatisticsProvider(this);
    }
}

void LogStatisticsProvider::RegisterProvider()
{
    if (m_enable) {
        (void)StatisticsManager::GetInstance().RegisterStatisticsProvider(this);
    }
    (void)ConfigManager::GetInstance().AddConfigChangeListener(this);
}

bool LogStatisticsProvider::CreateInstance()
{
    bool result = false;
    MOT_ASSERT(m_provider == nullptr);
    if (m_provider == nullptr) {
        m_provider = new (std::nothrow) LogStatisticsProvider();
        if (m_provider == nullptr) {
            MOT_REPORT_ERROR(
                MOT_ERROR_OOM, "Load Statistics", "Failed to allocate memory for Log Statistics Provider, aborting");
        } else {
            result = m_provider->Initialize();
            if (!result) {
                MOT_REPORT_ERROR(
                    MOT_ERROR_INTERNAL, "Load Statistics", "Failed to initialize Log Statistics Provider, aborting");
                delete m_provider;
                m_provider = nullptr;
            } else {
                m_provider->RegisterProvider();
            }
        }
    }
    return result;
}

void LogStatisticsProvider::DestroyInstance()
{
    MOT_ASSERT(m_provider != nullptr);
    if (m_provider != nullptr) {
        delete m_provider;
        m_provider = nullptr;
    }
}

LogStatisticsProvider& LogStatisticsProvider::GetInstance()
{
    MOT_ASSERT(m_provider != nullptr);
    return *m_provider;
}

void LogStatisticsProvider::OnConfigChange()
{
    if (m_enable != GetGlobalConfiguration().m_enableLogStatistics) {
        m_enable = GetGlobalConfiguration().m_enableLogStatistics;
        if (m_enable) {
            (void)StatisticsManager::GetInstance().RegisterStatisticsProvider(this);
        } else {
            (void)StatisticsManager::GetInstance().UnregisterStatisticsProvider(this);
        }
    }
}
}  // namespace MOT
