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
 * db_session_statistics.cpp
 *    Thread-level statistics collector for database events.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/statistics/db_session_statistics.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "db_session_statistics.h"
#include "mot_configuration.h"
#include "config_manager.h"
#include "statistics_manager.h"
#include "mot_error.h"

namespace MOT {
DECLARE_LOGGER(DbSessionStatisticsProvider, System)

DbSessionThreadStatistics::DbSessionThreadStatistics(uint64_t threadId, void* inplaceBuffer)
    : ThreadStatistics(threadId, inplaceBuffer),
      m_txnCount(MakeName("txn", threadId).c_str()),
      m_rowPerTxnCount(MakeName("rows-per-txn", threadId).c_str()),
      m_insertRowsCount(MakeName("insert-row", threadId).c_str()),
      m_updateRowsCount(MakeName("update-row", threadId).c_str()),
      m_deleteRowsCount(MakeName("delete-row", threadId).c_str()),
      m_commitTxnCount(MakeName("commit-txn", threadId).c_str()),
      m_rollbackTxnCount(MakeName("rollback-txn", threadId).c_str()),
      m_commitPreparedTxnCount(MakeName("commit-prepared-txn", threadId).c_str()),
      m_rollbackPreparedTxnCount(MakeName("rollback-prepared-txn", threadId).c_str())
{
    RegisterStatistics(&m_txnCount);
    RegisterStatistics(&m_rowPerTxnCount);
    RegisterStatistics(&m_insertRowsCount);
    RegisterStatistics(&m_updateRowsCount);
    RegisterStatistics(&m_deleteRowsCount);
    RegisterStatistics(&m_commitTxnCount);
    RegisterStatistics(&m_rollbackTxnCount);
    RegisterStatistics(&m_commitPreparedTxnCount);
    RegisterStatistics(&m_rollbackPreparedTxnCount);
}

TypedStatisticsGenerator<DbSessionThreadStatistics, EmptyGlobalStatistics> DbSessionStatisticsProvider::m_generator;
DbSessionStatisticsProvider* DbSessionStatisticsProvider::m_provider = nullptr;

DbSessionStatisticsProvider::DbSessionStatisticsProvider()
    : StatisticsProvider("DbSession", &m_generator, GetGlobalConfiguration().m_enableDbSessionStatistics)
{}

DbSessionStatisticsProvider::~DbSessionStatisticsProvider()
{
    ConfigManager::GetInstance().RemoveConfigChangeListener(this);
    if (m_enable) {
        StatisticsManager::GetInstance().UnregisterStatisticsProvider(this);
    }
}

void DbSessionStatisticsProvider::RegisterProvider()
{
    if (m_enable) {
        StatisticsManager::GetInstance().RegisterStatisticsProvider(this);
    }
    ConfigManager::GetInstance().AddConfigChangeListener(this);
}

bool DbSessionStatisticsProvider::CreateInstance()
{
    bool result = false;
    MOT_ASSERT(m_provider == nullptr);
    if (m_provider == nullptr) {
        m_provider = new (std::nothrow) DbSessionStatisticsProvider();
        if (m_provider == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM,
                "Load Statistics",
                "Failed to allocate memory for DB Session Statistics Provider, aborting");
        } else {
            result = m_provider->Initialize();
            if (!result) {
                MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                    "Load Statistics",
                    "Failed to initialize DB Session Statistics Provider, aborting");
                delete m_provider;
                m_provider = nullptr;
            } else {
                m_provider->RegisterProvider();
            }
        }
    }
    return result;
}

void DbSessionStatisticsProvider::DestroyInstance()
{
    MOT_ASSERT(m_provider != nullptr);
    if (m_provider != nullptr) {
        delete m_provider;
        m_provider = nullptr;
    }
}

DbSessionStatisticsProvider& DbSessionStatisticsProvider::GetInstance()
{
    MOT_ASSERT(m_provider != nullptr);
    return *m_provider;
}

void DbSessionStatisticsProvider::OnConfigChange()
{
    if (m_enable != GetGlobalConfiguration().m_enableDbSessionStatistics) {
        m_enable = GetGlobalConfiguration().m_enableDbSessionStatistics;
        if (m_enable) {
            StatisticsManager::GetInstance().RegisterStatisticsProvider(this);
        } else {
            StatisticsManager::GetInstance().UnregisterStatisticsProvider(this);
        }
    }
}
}  // namespace MOT
