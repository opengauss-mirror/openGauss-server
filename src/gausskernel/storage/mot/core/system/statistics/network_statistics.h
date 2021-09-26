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
 * network_statistics.h
 *    Thread-level statistics collector for network events.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/statistics/network_statistics.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef NETWORK_STATISTICS_H
#define NETWORK_STATISTICS_H

#include "frequency_statistic_variable.h"
#include "iconfig_change_listener.h"
#include "numeric_statistic_variable.h"
#include "statistics_provider.h"
#include "typed_statistics_generator.h"
#include "debug_utils.h"

namespace MOT {
/**
 * @brief Thread-level statistics collector for network events.
 */
class NetworkThreadStatistics : public ThreadStatistics {
public:
    /**
     * @brief Constructor.
     * @param threadId The identifier of the thread for which statistics are collected.
     * @param inplaceBuffer In-place buffer (preferably NUMA-aware) for placing statistic variables.
     */
    explicit NetworkThreadStatistics(uint64_t threadId, void* inplaceBuffer = nullptr);

    /** @brief Destructor. */
    virtual ~NetworkThreadStatistics()
    {}

    /** @brief Retrieves the connect count statistic variable. */
    inline void AddConnectCount()
    {
        m_connectCount.AddSample();
    }

    /** @brief Retrieves the disconnect count statistic variable. */
    inline void AddDisconnectCount()
    {
        m_disconnectCount.AddSample();
    }

    /** @brief Retrieves the transaction total time statistic variable. */
    inline void AddTxnTotalTime(uint64_t totalTimeUsec)
    {
        m_txnTotalTime.AddSample(totalTimeUsec);
    }

    /** @brief Retrieves the transaction net execution time time statistic variable. */
    inline void AddTxnNetExecTime(uint64_t netExecTimeUsec)
    {
        m_txnNetExecTime.AddSample(netExecTimeUsec);
    }

    /** @brief Retrieves the transaction command count statistic variable. */
    inline void AddTxnCommandCount(uint64_t commandCount)
    {
        m_txnCommandCount.AddSample(commandCount);
    }

    /** @brief Retrieves the transaction command time statistic variable. */
    inline void AddTxnCommandTime(uint64_t commandTimeUsec)
    {
        m_txnCommandTime.AddSample(commandTimeUsec);
    }

private:
    /** @var The connect count statistic variable. */
    FrequencyStatisticVariable m_connectCount;

    /** @var The disconnect count statistic variable. */
    FrequencyStatisticVariable m_disconnectCount;

    /** @var The transaction total time statistic variable. */
    NumericStatisticVariable m_txnTotalTime;

    /** @var The transaction net execution time statistic variable. */
    NumericStatisticVariable m_txnNetExecTime;

    /** @var The transaction command count statistic variable. */
    NumericStatisticVariable m_txnCommandCount;

    /** @var The transaction command time statistic variable. */
    NumericStatisticVariable m_txnCommandTime;
};

/**
 * @brief Statistics provider for network counters.
 */
class NetworkStatisticsProvider : public StatisticsProvider, public IConfigChangeListener {
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
     * @return Network statistics provider
     */
    static NetworkStatisticsProvider& GetInstance();

    /** @brief Records a connect event. */
    inline void AddConnectEvent()
    {
        NetworkThreadStatistics* nts = GetCurrentThreadStatistics<NetworkThreadStatistics>();
        if (nts != nullptr) {
            nts->AddConnectCount();
        }
#ifdef MOT_DEBUG
        __sync_fetch_and_add(&m_connectionCount, 1);
#endif
    }

    /** @brief Records a disconnect event. */
    inline void AddDisconnectEvent()
    {
        NetworkThreadStatistics* nts = GetCurrentThreadStatistics<NetworkThreadStatistics>();
        if (nts != nullptr) {
            nts->AddDisconnectCount();
        }
#ifdef MOT_DEBUG
        __sync_fetch_and_add(&m_connectionCount, -1);
#endif
    }

#ifdef MOT_DEBUG
    /** @brief Retrieves the number of active connections. */
    inline uint32_t GetCurrentConnectionCount()
    {
        return (uint32_t)__sync_fetch_and_add(&m_connectionCount, 0);
    }
#endif

    /** @brief Records a transaction total time. */
    inline void AddTxnTotalTime(uint64_t totalTimeUsec)
    {
        NetworkThreadStatistics* nts = GetCurrentThreadStatistics<NetworkThreadStatistics>();
        if (nts != nullptr) {
            nts->AddTxnTotalTime(totalTimeUsec);
        }
    }

    /** @brief Records a transaction net execution time. */
    inline void AddTxnNetExecTime(uint64_t netExecTimeUsec)
    {
        NetworkThreadStatistics* nts = GetCurrentThreadStatistics<NetworkThreadStatistics>();
        if (nts != nullptr) {
            nts->AddTxnNetExecTime(netExecTimeUsec);
        }
    }

    /** @brief Records a transaction command count. */
    inline void AddTxnCommandCount(uint64_t commandCount)
    {
        NetworkThreadStatistics* nts = GetCurrentThreadStatistics<NetworkThreadStatistics>();
        if (nts != nullptr) {
            nts->AddTxnCommandCount(commandCount);
        }
    }

    /** @brief Records a transaction command time part event. */
    inline void AddTxnCommandTime(uint64_t commandTimeUsec)
    {
        NetworkThreadStatistics* nts = GetCurrentThreadStatistics<NetworkThreadStatistics>();
        if (nts != nullptr) {
            nts->AddTxnCommandTime(commandTimeUsec);
        }
    }

    /**
     * @brief Derives classes should react to a notification that configuration changed. New
     * configuration is accessible via the ConfigManager.
     */
    virtual void OnConfigChange();

private:
    /** @brief Constructor. */
    NetworkStatisticsProvider();

    /** @brief Destructor. */
    virtual ~NetworkStatisticsProvider();

    /** @brief Registers the provider in the manager. */
    void RegisterProvider();

    /** @var The single instance. */
    static NetworkStatisticsProvider* m_provider;

    /** @var The generator used for creating global and thread-level statistics objects. */
    static TypedStatisticsGenerator<NetworkThreadStatistics, EmptyGlobalStatistics> m_generator;

    /** @var A global connection count. */
#ifdef MOT_DEBUG

    volatile int32_t m_connectionCount;
#endif
};
}  // namespace MOT

#endif /* NETWORK_STATISTICS_H */
