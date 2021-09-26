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
 * statistics_manager.cpp
 *    Singleton access point to system statistics.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/stats/statistics_manager.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "statistics_manager.h"
#include "mot_configuration.h"
#include "config_manager.h"
#include "cycles.h"
#include "mot_error.h"
#include <unistd.h>
#include <algorithm>

namespace MOT {
DECLARE_LOGGER(StatisticsManager, Statistics)

StatisticsManager* StatisticsManager::m_manager = nullptr;

#define STAT_PRINT_CHECK_PERIOD_SECONDS 1

// Initialization helper macro (for system calls)
#define CHECK_SYS_INIT_STATUS(rc, syscall, format, ...)                                                        \
    if (rc != 0) {                                                                                             \
        MOT_REPORT_SYSTEM_PANIC_CODE(rc, syscall, "Statistics Manager Initialization", format, ##__VA_ARGS__); \
        break;                                                                                                 \
    }

// Initialization helper macro (for sub-service initialization)
#define CHECK_INIT_STATUS(result, format, ...)                                                            \
    if (!result) {                                                                                        \
        MOT_REPORT_PANIC(MOT_ERROR_INTERNAL, "Statistics Manager Initialization", format, ##__VA_ARGS__); \
        break;                                                                                            \
    }

static bool CreateRecursiveMutex(pthread_mutex_t* mutex)
{
    bool result = false;

    pthread_mutexattr_t lockattr;
    int rc = pthread_mutexattr_init(&lockattr);
    if (rc != 0) {
        MOT_REPORT_SYSTEM_ERROR_CODE(rc,
            pthread_mutexattr_init,
            "Statistics Manager Initialization",
            "Failed to initialize recursive mutex attribute for the statistics manager");
    } else {
        rc = pthread_mutexattr_settype(&lockattr, PTHREAD_MUTEX_RECURSIVE);
        if (rc != 0) {
            MOT_REPORT_SYSTEM_ERROR_CODE(rc,
                pthread_mutexattr_settype,
                "Statistics Manager Initialization",
                "Failed to configure recursive mutex type for the statistics manager");
        } else {
            rc = pthread_mutex_init(mutex, &lockattr);
            if (rc != 0) {
                MOT_REPORT_SYSTEM_ERROR_CODE(rc,
                    pthread_mutex_init,
                    "Statistics Manager Initialization",
                    "Failed to create recursive mutex for the statistics manager");
            } else {
                result = true;
            }
        }
        pthread_mutexattr_destroy(&lockattr);
    }

    return result;
}

bool StatisticsManager::Initialize()
{
    bool result = true;
    m_initPhase = INIT;

    do {
        // create providers lock
        result = CreateRecursiveMutex(&m_providersLock);
        CHECK_INIT_STATUS(result, "Failed to create providers lock");
        m_initPhase = INIT_PROVIDERS_LOCK;

        // create statistics printing period lock and condition variable
        result = CreateRecursiveMutex(&m_statsPrintLock);
        CHECK_INIT_STATUS(result, "Failed to create statistics printing lock");
        m_initPhase = INIT_STAT_PRINT_LOCK;

        int rc = pthread_cond_init(&m_statsPrintCond, nullptr);
        result = (rc == 0);
        CHECK_SYS_INIT_STATUS(rc, pthread_cond_init, "Failed to initialize statistics printing condition variable");
        m_initPhase = INIT_STAT_PRINT_CV;

        // register to configuration change notifications
        ConfigManager::GetInstance().AddConfigChangeListener(this);
    } while (0);

    if (result) {
        m_initPhase = DONE;
    }

    return result;
}

StatisticsManager::StatisticsManager()
    : m_statsPrintPeriodSeconds(GetGlobalConfiguration().m_statPrintPeriodSeconds),
      m_fullStatsPrintPeriodSeconds(GetGlobalConfiguration().m_statPrintFullPeriodSeconds),
      m_running(false),
      m_initPhase(INIT)
{
    // the statistics thread should be started explicitly
}

StatisticsManager::~StatisticsManager()
{
    switch (m_initPhase) {
        case DONE:
            // stop the statistics printing thread
            StopStatsPrintThread();
            // unregister from configuration change notifications
            ConfigManager::GetInstance().RemoveConfigChangeListener(this);
            // fall through
        case INIT_STAT_PRINT_CV:
            pthread_cond_destroy(&m_statsPrintCond);
            // fall through
        case INIT_STAT_PRINT_LOCK:
            pthread_mutex_destroy(&m_statsPrintLock);
            // fall through
        case INIT_PROVIDERS_LOCK:
            pthread_mutex_destroy(&m_providersLock);
            // fall through
        case INIT:
        default:
            break;
    }
}

bool StatisticsManager::CreateInstance()
{
    bool result = false;
    MOT_ASSERT(m_manager == nullptr);
    if (m_manager == nullptr) {
        m_manager = new (std::nothrow) StatisticsManager();
        if (!m_manager) {
            MOT_LOG_ERROR("Failed to allocate memory for statistics manager, aborting");
            SetLastError(MOT_ERROR_OOM, MOT_SEVERITY_FATAL);
        } else {
            result = m_manager->Initialize();
            if (!result) {
                delete m_manager;
                m_manager = nullptr;
            }
        }
    }
    return result;
}

void StatisticsManager::DestroyInstance()
{
    MOT_ASSERT(m_manager != nullptr);
    if (m_manager != nullptr) {
        delete m_manager;
        m_manager = nullptr;
    }
}

StatisticsManager& StatisticsManager::GetInstance()
{
    MOT_ASSERT(m_manager != nullptr);
    return *m_manager;
}

bool StatisticsManager::RegisterStatisticsProvider(StatisticsProvider* statisticsProvider)
{
    bool result = false;
    pthread_mutex_lock(&m_providersLock);

    mot_list<StatisticsProvider*>::iterator itr = find(m_providers.begin(), m_providers.end(), statisticsProvider);
    if (itr == m_providers.end()) {
        if (!m_providers.push_back(statisticsProvider)) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM,
                "Register Statistics",
                "Failed to register statistics provider %s",
                statisticsProvider->GetName());
        } else {
            result = true;
            MOT_LOG_TRACE("Registered statistics provider: %s", statisticsProvider->GetName());
        }
    }

    pthread_mutex_unlock(&m_providersLock);
    return result;
}

bool StatisticsManager::UnregisterStatisticsProvider(StatisticsProvider* statisticsProvider)
{
    bool result = false;
    pthread_mutex_lock(&m_providersLock);

    mot_list<StatisticsProvider*>::iterator itr = find(m_providers.begin(), m_providers.end(), statisticsProvider);
    if (itr != m_providers.end()) {
        m_providers.erase(itr);
        MOT_LOG_TRACE("Unregistered statistics provider: %s", statisticsProvider->GetName());
        result = true;
    }

    pthread_mutex_unlock(&m_providersLock);
    return result;
}

void StatisticsManager::OnConfigChange()
{
    m_statsPrintPeriodSeconds = GetGlobalConfiguration().m_statPrintPeriodSeconds;
    m_fullStatsPrintPeriodSeconds = GetGlobalConfiguration().m_statPrintFullPeriodSeconds;
}

// functor for std::find_if()
struct StatisticsProviderFinder {
    const char* m_name;

    explicit StatisticsProviderFinder(const char* name) : m_name(name)
    {}

    inline bool operator()(StatisticsProvider* const& provider) const
    {
        return strcmp(m_name, provider->GetName()) == 0;
    }
};

// functor for std::for_each()
struct StatisticsProviderPrinter {
    LogLevel m_logLevel;

    uint32_t m_statOpts;

    explicit StatisticsProviderPrinter(LogLevel logLevel, uint32_t statOpts)
        : m_logLevel(logLevel), m_statOpts(statOpts)
    {}

    inline void operator()(StatisticsProvider* const& provider)
    {
        if (provider->IsEnabled()) {
            provider->PrintStatistics(m_logLevel, m_statOpts);
        }
    }
};

StatisticsProvider* StatisticsManager::GetStatisticsProvider(const char* name)
{
    StatisticsProvider* result = nullptr;
    pthread_mutex_lock(&m_providersLock);

    mot_list<StatisticsProvider*>::iterator itr =
        find_if(m_providers.begin(), m_providers.end(), StatisticsProviderFinder(name));
    if (itr != m_providers.end()) {
        result = *itr;
    }

    pthread_mutex_unlock(&m_providersLock);
    return result;
}

bool StatisticsManager::ReserveThreadSlot()
{
    bool result = false;

    MOTThreadId tid = MOTCurrThreadId;
    if (tid == INVALID_THREAD_ID) {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
            "Reserve Thread Slot for Statistics",
            "Invalid attempt to reserve statistics thread slot without current thread identifier denied");
    } else {
        pthread_mutex_lock(&m_providersLock);

        result = true;
        MOT_LOG_TRACE("Reserving statistics thread slot for thread id %" PRIu16, tid);
        mot_list<StatisticsProvider*>::iterator itr = m_providers.begin();
        while (itr != m_providers.end()) {
            StatisticsProvider* provider = *itr;
            if (!provider->ReserveThreadSlot()) {
                result = false;
                break;
            }
            ++itr;
        }

        pthread_mutex_unlock(&m_providersLock);
    }

    return result;
}

void StatisticsManager::UnreserveThreadSlot()
{
    MOTThreadId tid = MOTCurrThreadId;
    if (tid == INVALID_THREAD_ID) {
        MOT_LOG_ERROR("Invalid attempt to un-reserve statistics thread slot without current thread identifier denied");
    } else {
        pthread_mutex_lock(&m_providersLock);

        MOT_LOG_TRACE("Un-reserving statistics thread slot for thread id %" PRIu16, tid);
        mot_list<StatisticsProvider*>::iterator itr = m_providers.begin();
        while (itr != m_providers.end()) {
            StatisticsProvider* provider = *itr;
            provider->UnreserveThreadSlot();
            ++itr;
        }

        pthread_mutex_unlock(&m_providersLock);
    }
}

void StatisticsManager::PrintStatistics(LogLevel logLevel, uint32_t statOpts /* = STAT_OPT_DEFAULT */)
{
    pthread_mutex_lock(&m_providersLock);

    // make sure there is at least one enabled provider with some statistics
    if (!m_providers.empty()) {
        bool hasEnabled = false;
        bool hasStats = false;
        mot_list<StatisticsProvider*>::iterator itr = m_providers.begin();
        while (itr != m_providers.end()) {
            StatisticsProvider* provider = *itr;
            if (provider->IsEnabled()) {
                hasEnabled = true;
                provider->Summarize();
                if (provider->HasStatisticsFor(statOpts)) {
                    hasStats = true;
                }
                // continue summarizing statistics for other statistics providers
            }
            ++itr;
        }

        if (hasEnabled && hasStats) {
            if (statOpts & STAT_OPT_PERIOD_DIFF) {
                MOT_LOG_INFO("--> Periodic Report (last %lu seconds) <--", m_statsPrintPeriodSeconds);
            } else if (statOpts & STAT_OPT_LEVEL_SUMMARY) {
                MOT_LOG_INFO("--> Summary Report <--");
            } else if (statOpts & STAT_OPT_LEVEL_DETAIL) {
                MOT_LOG_INFO("--> Detailed Report <--");
            }

            MOT_LOG(logLevel, "======================================================");
            for_each(m_providers.cbegin(), m_providers.cend(), StatisticsProviderPrinter(logLevel, statOpts));
            MOT_LOG(logLevel, "======================================================");
        }
    }

    pthread_mutex_unlock(&m_providersLock);
}

bool StatisticsManager::StartStatsPrintThread()
{
    if (!m_running) {
        int rc = pthread_create(&m_statsThread, nullptr, StatsPrintThreadStatic, this);
        if (rc != 0) {
            MOT_REPORT_SYSTEM_ERROR_CODE(
                rc, pthread_create, "Statistics Manager Initialization", "Failed to create statistics printing thread");
        } else {
            m_running = true;
        }
    }
    return m_running;
}

void StatisticsManager::StopStatsPrintThread()
{
    // signal done flag and wake up statistics printing thread
    if (m_running) {
        pthread_mutex_lock(&m_statsPrintLock);
        m_running = false;
        pthread_cond_signal(&m_statsPrintCond);
        pthread_mutex_unlock(&m_statsPrintLock);

        // wait for statistics printing thread to finish
        pthread_join(m_statsThread, nullptr);
    }
}

void* StatisticsManager::StatsPrintThreadStatic(void* param)
{
    auto pThis = reinterpret_cast<StatisticsManager*>(param);
    knl_thread_mot_init();
    pThis->StatsPrintThread();
    return nullptr;
}

void StatisticsManager::StatsPrintThread()
{
    MOT_LOG_INFO("Statistics thread started");

    uint64_t statsPrintCount = 0;
    uint64_t fullStatsPrintCount = 0;

    while (m_running) {
        WaitNextPrint();

        statsPrintCount += STAT_PRINT_CHECK_PERIOD_SECONDS;
        fullStatsPrintCount += STAT_PRINT_CHECK_PERIOD_SECONDS;
        bool statsPrinted = false;

        // print periodic report
        if (statsPrintCount >= m_statsPrintPeriodSeconds) {
            PrintStatistics(LogLevel::LL_INFO, STAT_OPT_SCOPE_ALL | STAT_OPT_PERIOD_DIFF | STAT_OPT_LEVEL_SUMMARY);
            statsPrintCount = 0;
            statsPrinted = true;
        }

        // print full report
        if (fullStatsPrintCount >= m_fullStatsPrintPeriodSeconds) {
            PrintStatistics(LogLevel::LL_INFO, STAT_OPT_SCOPE_ALL | STAT_OPT_PERIOD_TOTAL | STAT_OPT_LEVEL_SUMMARY);
            PrintStatistics(LogLevel::LL_INFO, STAT_OPT_SCOPE_ALL | STAT_OPT_PERIOD_TOTAL | STAT_OPT_LEVEL_DETAIL);
            fullStatsPrintCount = 0;
            statsPrinted = true;
        }
    }

    // print full report one last time
    PrintStatistics(LogLevel::LL_INFO, STAT_OPT_SCOPE_ALL | STAT_OPT_PERIOD_TOTAL | STAT_OPT_LEVEL_SUMMARY);
    PrintStatistics(LogLevel::LL_INFO, STAT_OPT_SCOPE_ALL | STAT_OPT_PERIOD_TOTAL | STAT_OPT_LEVEL_DETAIL);

    MOT_LOG_INFO("Statistics thread stopped");
}

void StatisticsManager::WaitNextPrint()
{
    struct timeval now;
    gettimeofday(&now, nullptr);
    struct timespec ts = {(time_t)(now.tv_sec + STAT_PRINT_CHECK_PERIOD_SECONDS), now.tv_usec * 1000L};

    pthread_mutex_lock(&m_statsPrintLock);
    pthread_cond_timedwait(&m_statsPrintCond, &m_statsPrintLock, &ts);
    pthread_mutex_unlock(&m_statsPrintLock);
}
}  // namespace MOT

void dumpStats()
{
    MOT::StatisticsManager::GetInstance().PrintAllStatistics(MOT::LogLevel::LL_INFO);
}
