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
 * network_statistics.cpp
 *    Thread-level statistics collector for network events.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/statistics/network_statistics.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "network_statistics.h"
#include "mot_configuration.h"
#include "config_manager.h"
#include "statistics_manager.h"
#include "mot_error.h"

/** @define Maximum total transaction time (including I/O) in micro-seconds. */
#define MAX_TXN_TOTAL_TIME_USEC 10000000  // 10 seconds

/** @define Maximum total transaction execution time (<b>not</b> including I/O) in micro-seconds. */
#define MAX_TXN_NET_EXEC_TIME_USEC 1000000  // 1 second

/** @define Maximum single command execution time in micro-seconds. */
#define MAX_TXN_COMMAND_TIME_USEC 100000  // 100 milli-seconds

namespace MOT {
DECLARE_LOGGER(NetworkStatisticsProvider, System)

NetworkThreadStatistics::NetworkThreadStatistics(uint64_t threadId, void* inplaceBuffer)
    : ThreadStatistics(threadId, inplaceBuffer),
      m_connectCount(MakeName("connect", threadId).c_str()),
      m_disconnectCount(MakeName("disconnect", threadId).c_str()),
      m_txnTotalTime(MakeName("txn_total_time", threadId).c_str(), MAX_TXN_TOTAL_TIME_USEC),
      m_txnNetExecTime(MakeName("txn_net_exec_time", threadId).c_str(), MAX_TXN_NET_EXEC_TIME_USEC),
      m_txnCommandCount(MakeName("txn_command_count", threadId).c_str()),
      m_txnCommandTime(MakeName("txn_command_time", threadId).c_str(), MAX_TXN_COMMAND_TIME_USEC)
{
    RegisterStatistics(&m_connectCount);
    RegisterStatistics(&m_disconnectCount);
    RegisterStatistics(&m_txnTotalTime);
    RegisterStatistics(&m_txnNetExecTime);
    RegisterStatistics(&m_txnCommandCount);
    RegisterStatistics(&m_txnCommandTime);
}

TypedStatisticsGenerator<NetworkThreadStatistics, EmptyGlobalStatistics> NetworkStatisticsProvider::m_generator;

NetworkStatisticsProvider* NetworkStatisticsProvider::m_provider = nullptr;

NetworkStatisticsProvider::NetworkStatisticsProvider()
    : StatisticsProvider("Network", &m_generator, GetGlobalConfiguration().m_enableNetworkStatistics)
#ifdef MOT_DEBUG
      ,
      m_connectionCount(0)
#endif
{}

NetworkStatisticsProvider::~NetworkStatisticsProvider()
{
    ConfigManager::GetInstance().RemoveConfigChangeListener(this);
    if (m_enable) {
        StatisticsManager::GetInstance().UnregisterStatisticsProvider(this);
    }
}

void NetworkStatisticsProvider::RegisterProvider()
{
    if (m_enable) {
        StatisticsManager::GetInstance().RegisterStatisticsProvider(this);
    }
    ConfigManager::GetInstance().AddConfigChangeListener(this);
}

bool NetworkStatisticsProvider::CreateInstance()
{
    bool result = false;
    MOT_ASSERT(m_provider == nullptr);
    if (m_provider == nullptr) {
        m_provider = new (std::nothrow) NetworkStatisticsProvider();
        if (m_provider == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM,
                "Load Statistics",
                "Failed to allocate memory for Network Statistics Provider, aborting");
        } else {
            result = m_provider->Initialize();
            if (!result) {
                MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                    "Load Statistics",
                    "Failed to initialize Network Statistics Provider, aborting");
                delete m_provider;
                m_provider = nullptr;
            } else {
                m_provider->RegisterProvider();
            }
        }
    }
    return result;
}

void NetworkStatisticsProvider::DestroyInstance()
{
    MOT_ASSERT(m_provider != nullptr);
    if (m_provider != nullptr) {
        delete m_provider;
        m_provider = nullptr;
    }
}

NetworkStatisticsProvider& NetworkStatisticsProvider::GetInstance()
{
    MOT_ASSERT(m_provider != nullptr);
    return *m_provider;
}

void NetworkStatisticsProvider::OnConfigChange()
{
    if (m_enable != GetGlobalConfiguration().m_enableNetworkStatistics) {
        m_enable = GetGlobalConfiguration().m_enableNetworkStatistics;
        if (m_enable) {
            StatisticsManager::GetInstance().RegisterStatisticsProvider(this);
        } else {
            StatisticsManager::GetInstance().UnregisterStatisticsProvider(this);
        }
    }
}
}  // namespace MOT
