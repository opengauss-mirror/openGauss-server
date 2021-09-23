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
 * db_session_statistics.h
 *    Thread-level statistics collector for database events.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/statistics/db_session_statistics.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef DB_SESSION_STATISTICS_H
#define DB_SESSION_STATISTICS_H

#include "frequency_statistic_variable.h"
#include "iconfig_change_listener.h"
#include "numeric_statistic_variable.h"
#include "statistics_provider.h"
#include "stats/frequency_statistic_variable.h"
#include "typed_statistics_generator.h"

namespace MOT {
/**
 * @brief Thread-level statistics collector for database events.
 */
class DbSessionThreadStatistics : public ThreadStatistics {
public:
    /**
     * @brief Constructor.
     * @param threadId The identifier of the thread for which statistics are collected.
     * @param inplaceBuffer In-place buffer (preferably NUMA-aware) for placing statistic variables.
     */
    explicit DbSessionThreadStatistics(uint64_t threadId, void* inplaceBuffer = nullptr);

    /** @brief Destructor. */
    virtual ~DbSessionThreadStatistics()
    {}

    /** @brief Updates the transaction count statistics. */
    inline void AddTxnCount()
    {
        m_txnCount.AddSample();
    }

    /** @brief Updates the rows-per-transaction count statistics. */
    inline void AddRowPerTxCount(uint64_t rowCount)
    {
        m_rowPerTxnCount.AddSample(rowCount);
    }

    /** @brief Updates the inserted-row count statistics. */
    inline void AddInsertRowCount()
    {
        m_insertRowsCount.AddSample();
    }

    /** @brief Updates the updated-row count statistics. */
    inline void AddUpdateRowCount()
    {
        m_updateRowsCount.AddSample();
    }

    /** @brief Updates the deleted-row count statistics. */
    inline void AddDeleteRowCount()
    {
        m_deleteRowsCount.AddSample();
    }

    /** @brief Updates the committed-transaction count statistics. */
    inline void AddCommitTxnCount()
    {
        m_commitTxnCount.AddSample();
    }

    /** @brief Updates the rolled-back-transaction count statistics. */
    inline void AddRollbackTxnCount()
    {
        m_rollbackTxnCount.AddSample();
    }

    /** @brief Updates the committed-prepared-transaction count statistics. */
    inline void AddCommitPreparedTxnCount()
    {
        m_commitPreparedTxnCount.AddSample();
    }

    /** @brief Updates the roll-backed-prepared-transaction count statistics. */
    inline void AddRollbackPreparedTxnCount()
    {
        m_rollbackPreparedTxnCount.AddSample();
    }

private:
    /** @var The transaction count statistic variable. */
    FrequencyStatisticVariable m_txnCount;

    /** @var The rows-per-transaction count statistic variable. */
    NumericStatisticVariable m_rowPerTxnCount;

    /** @var The inserted-row count statistic variable. */
    FrequencyStatisticVariable m_insertRowsCount;

    /** @var The updated-row count statistic variable. */
    FrequencyStatisticVariable m_updateRowsCount;

    /** @var The deleted-row count statistic variable. */
    FrequencyStatisticVariable m_deleteRowsCount;

    /** @var The committed-transaction count statistic variable. */
    FrequencyStatisticVariable m_commitTxnCount;

    /** @var The rolled-back-transaction count statistic variable. */
    FrequencyStatisticVariable m_rollbackTxnCount;

    /** @var The committed-prepared-transaction count statistic variable. */
    FrequencyStatisticVariable m_commitPreparedTxnCount;

    /** @var The rolled-back-prepared-transaction count statistic variable. */
    FrequencyStatisticVariable m_rollbackPreparedTxnCount;
};

/**
 * @brief Statistics provider for database counters.
 */
class DbSessionStatisticsProvider : public StatisticsProvider, public IConfigChangeListener {
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
    static DbSessionStatisticsProvider& GetInstance();

    /** @brief Records a transaction event. */
    inline void AddTxn()
    {
        DbSessionThreadStatistics* dbts = GetCurrentThreadStatistics<DbSessionThreadStatistics>();
        if (dbts != nullptr) {
            dbts->AddTxnCount();
        }
    }

    /** @brief Records a rows-per-transaction event. */
    inline void AddRowsPerTx(uint64_t rowCount)
    {
        DbSessionThreadStatistics* dbts = GetCurrentThreadStatistics<DbSessionThreadStatistics>();
        if (dbts != nullptr) {
            dbts->AddRowPerTxCount(rowCount);
        }
    }

    /** @brief Records a insert-row event. */
    inline void AddInsertRow()
    {
        DbSessionThreadStatistics* dbts = GetCurrentThreadStatistics<DbSessionThreadStatistics>();
        if (dbts != nullptr) {
            dbts->AddInsertRowCount();
        }
    }

    /** @brief Records a update-row event. */
    inline void AddUpdateRow()
    {
        DbSessionThreadStatistics* dbts = GetCurrentThreadStatistics<DbSessionThreadStatistics>();
        if (dbts != nullptr) {
            dbts->AddUpdateRowCount();
        }
    }

    /** @brief Records a delete-row event. */
    inline void AddDeleteRow()
    {
        DbSessionThreadStatistics* dbts = GetCurrentThreadStatistics<DbSessionThreadStatistics>();
        if (dbts != nullptr) {
            dbts->AddDeleteRowCount();
        }
    }

    /** @brief Records a commit-transaction event. */
    inline void AddCommitTxn()
    {
        DbSessionThreadStatistics* dbts = GetCurrentThreadStatistics<DbSessionThreadStatistics>();
        if (dbts != nullptr) {
            dbts->AddCommitTxnCount();
        }
    }

    /** @brief Records a rollback-transaction event. */
    inline void AddRollbackTxn()
    {
        DbSessionThreadStatistics* dbts = GetCurrentThreadStatistics<DbSessionThreadStatistics>();
        if (dbts != nullptr) {
            dbts->AddRollbackTxnCount();
        }
    }

    /** @brief Records a commit-prepared-transaction event. */
    inline void AddCommitPreparedTxn()
    {
        DbSessionThreadStatistics* dbts = GetCurrentThreadStatistics<DbSessionThreadStatistics>();
        if (dbts != nullptr) {
            dbts->AddCommitPreparedTxnCount();
        }
    }

    /** @brief Records a roll-backed-prepared-transaction event. */
    inline void AddRollbackPreparedTxn()
    {
        DbSessionThreadStatistics* dbts = GetCurrentThreadStatistics<DbSessionThreadStatistics>();
        if (dbts != nullptr) {
            dbts->AddRollbackPreparedTxnCount();
        }
    }

    /**
     * @brief Derives classes should react to a notification that configuration changed. New
     * configuration is accessible via the ConfigManager.
     */
    virtual void OnConfigChange();

private:
    /** @brief Constructor. */
    DbSessionStatisticsProvider();

    /** @brief Destructor. */
    virtual ~DbSessionStatisticsProvider();

    /** @brief Registers the provider in the manager. */
    void RegisterProvider();

    /** @var The single instance. */
    static DbSessionStatisticsProvider* m_provider;

    /** @var The generator used for creating global and thread-level statistics objects. */
    static TypedStatisticsGenerator<DbSessionThreadStatistics, EmptyGlobalStatistics> m_generator;
};
}  // namespace MOT

#endif /* DB_SESSION_STATISTICS_H */
