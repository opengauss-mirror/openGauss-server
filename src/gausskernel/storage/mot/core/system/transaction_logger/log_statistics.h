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
 * log_statistics.h
 *    Transaction log statistics.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/transaction_logger/log_statistics.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef LOG_STATISTICS_H
#define LOG_STATISTICS_H

#include "frequency_statistic_variable.h"
#include "rate_statistic_variable.h"
#include "iconfig_change_listener.h"
#include "numeric_statistic_variable.h"
#include "statistics_provider.h"
#include "stats/frequency_statistic_variable.h"
#include "typed_statistics_generator.h"

namespace MOT {
class LogThreadStatistics : public ThreadStatistics {
public:
    explicit LogThreadStatistics(uint64_t threadId, void* inplaceBuffer = nullptr);

    virtual ~LogThreadStatistics()
    {}

    inline void AddTxnBuffersDrained()
    {
        m_txnBuffersDrained.AddSample();
    }

    inline void AddTxnBytesDrained(uint64_t bytes)
    {
        m_txnBytesDrained.AddSample(bytes);
    }

    inline void AddBytesWritten(uint64_t bytes)
    {
        m_bytesWritten.AddSample(bytes);
    }

private:
    FrequencyStatisticVariable m_txnBuffersDrained;
    DataRateStatisticVariable m_txnBytesDrained;
    DataRateStatisticVariable m_bytesWritten;
};

class LogGlobalStatistics : public GlobalStatistics {
public:
    explicit LogGlobalStatistics(GlobalStatistics::NamingScheme namingScheme);

    virtual ~LogGlobalStatistics()
    {}

    inline void AddLogFlush()
    {
        m_logFlushes.AddSample();
    }

private:
    FrequencyStatisticVariable m_logFlushes;
};

class LogStatisticsProvider : public StatisticsProvider, public IConfigChangeListener {
private:
    LogStatisticsProvider();
    virtual ~LogStatisticsProvider();

    /** @brief Registers the provider in the manager. */
    void RegisterProvider();

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
    static LogStatisticsProvider& GetInstance();

    inline void AddTxnBytesDrained(uint64_t bytes)
    {
        LogThreadStatistics* lts = GetCurrentThreadStatistics<LogThreadStatistics>();
        if (lts != nullptr) {
            lts->AddTxnBytesDrained(bytes);
            lts->AddTxnBuffersDrained();
        }
    }

    inline void AddBytesWritten(uint64_t bytes)
    {
        LogThreadStatistics* lts = GetCurrentThreadStatistics<LogThreadStatistics>();
        if (lts != nullptr) {
            lts->AddBytesWritten(bytes);
        }
    }

    inline void AddLogFlush()
    {
        LogGlobalStatistics* lts = GetGlobalStatistics<LogGlobalStatistics>();
        if (lts != nullptr) {
            lts->AddLogFlush();
        }
    }

    /**
     * @brief Derives classes should react to a notification that configuration changed. New
     * configuration is accessible via the ConfigManager.
     */
    virtual void OnConfigChange();

private:
    /** @var The single instance. */
    static LogStatisticsProvider* m_provider;
    static TypedStatisticsGenerator<LogThreadStatistics, LogGlobalStatistics> m_generator;
};
}  // namespace MOT

#endif /* LOG_STATISTICS_H */
