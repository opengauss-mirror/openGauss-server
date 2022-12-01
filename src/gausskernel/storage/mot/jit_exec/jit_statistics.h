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
 * jit_statistics.h
 *    Statistics collection for JIT module.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/jit_exec/jit_statistics.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef JIT_STATISTICS_H
#define JIT_STATISTICS_H

#include "frequency_statistic_variable.h"
#include "level_statistic_variable.h"
#include "numeric_statistic_variable.h"
#include "memory_statistic_variable.h"
#include "iconfig_change_listener.h"
#include "statistics_provider.h"
#include "typed_statistics_generator.h"

namespace JitExec {
/**
 * @brief Thread-level statistics collector for database events.
 */
class JitThreadStatistics : public MOT::ThreadStatistics {
public:
    /**
     * @brief Constructor.
     * @param threadId The identifier of the thread for which statistics are collected.
     * @param inplaceBuffer In-place buffer (preferably NUMA-aware) for placing statistic variables.
     */
    explicit JitThreadStatistics(uint64_t threadId, void* inplaceBuffer = nullptr);

    /** @brief Destructor. */
    virtual ~JitThreadStatistics()
    {}

    /** @brief Updates the successful JIT query execution count statistics. */
    inline void AddExecQuery()
    {
        m_execQueryCount.AddSample();
    }

    /** @brief Updates the successful JIT query invocation count statistics. */
    inline void AddInvokeQuery()
    {
        m_invokeQueryCount.AddSample();
    }

    /** @brief Updates the failed JIT query execution count statistics. */
    inline void AddExecFailQuery()
    {
        m_execFailQueryCount.AddSample();
    }

    /** @brief Updates the aborted JIT query execution count statistics. */
    inline void AddExecAbortQuery()
    {
        m_execAbortQueryCount.AddSample();
    }

    /** @var Updates the number of session-bytes allocated for JIT query execution. */
    inline void AddSessionBytes(int64_t bytes)
    {
        m_sessionBytes.AddSample(bytes);
    }

private:
    /** @var The successful JIT query execution count statistic variable. */
    MOT::FrequencyStatisticVariable m_execQueryCount;

    /** @var The successful JIT query invocation count statistic variable. */
    MOT::FrequencyStatisticVariable m_invokeQueryCount;

    /** @var The failed JIT query execution count statistic variable. */
    MOT::FrequencyStatisticVariable m_execFailQueryCount;

    /** @var The aborted JIT query execution count statistic variable. */
    MOT::FrequencyStatisticVariable m_execAbortQueryCount;

    /** @var The number of session-bytes allocated for JIT query execution. */
    MOT::MemoryStatisticVariable m_sessionBytes;
};

class JitGlobalStatistics : public MOT::GlobalStatistics {
public:
    explicit JitGlobalStatistics(MOT::GlobalStatistics::NamingScheme namingScheme);

    virtual ~JitGlobalStatistics()
    {}

    /** @brief Updates the statistics for total amount of jittable queries. */
    inline void AddJittableQuery()
    {
        m_jittableQueryCount.AddSample(1);
    }

    /** @brief Updates the statistics for total amount of un-jittable queries due to query limit. */
    inline void AddUnjittableLimitQuery()
    {
        m_unjittableLimitQueryCount.AddSample(1);
    }

    /** @brief Updates the statistics for total amount of un-jittable queries due to disqualification. */
    inline void AddUnjittableDisqualifiedQuery()
    {
        m_unjittableDisqualifiedQueryCount.AddSample(1);
    }

    /** @brief Updates the statistics for total amount of JIT query code generation. */
    inline void AddCodeGenQuery()
    {
        m_codeGenQueryCount.AddSample(1);
    }

    /** @brief Updates the statistics for total time required to generate code for a single query.. */
    inline void AddCodeGenTime(uint64_t micros)
    {
        m_codeGenTime.AddSample(micros);
    }

    /** @brief Updates the statistics for total amount of JIT query code generation error. */
    inline void AddCodeGenErrorQuery()
    {
        m_codeGenErrorQueryCount.AddSample(1);
    }

    /** @brief Updates the statistics for total amount of JIT query code cloning. */
    inline void AddCodeCloneQuery()
    {
        m_codeCloneQueryCount.AddSample(1);
    }

    /** @brief Updates the statistics for total amount of JIT query code cloning error. */
    inline void AddCodeCloneErrorQuery()
    {
        m_codeCloneErrorQueryCount.AddSample(1);
    }

    /** @brief Updates the statistics for total amount of JIT queries that expired. */
    inline void AddCodeExpiredQuery()
    {
        m_codeExpiredQueryCount.AddSample(1);
    }

    /** @var Updates the number of global-bytes allocated for JIT query execution. */
    inline void AddGlobalBytes(int64_t bytes)
    {
        m_globalBytes.AddSample(bytes);
    }

private:
    MOT::LevelStatisticVariable m_jittableQueryCount;
    MOT::LevelStatisticVariable m_unjittableLimitQueryCount;
    MOT::LevelStatisticVariable m_unjittableDisqualifiedQueryCount;
    MOT::LevelStatisticVariable m_codeGenQueryCount;
    MOT::NumericStatisticVariable m_codeGenTime;
    MOT::LevelStatisticVariable m_codeGenErrorQueryCount;
    MOT::LevelStatisticVariable m_codeCloneQueryCount;
    MOT::LevelStatisticVariable m_codeCloneErrorQueryCount;
    MOT::LevelStatisticVariable m_codeExpiredQueryCount;
    MOT::MemoryStatisticVariable m_globalBytes;
};

/**
 * @brief Statistics provider for database counters.
 */
class JitStatisticsProvider : public MOT::StatisticsProvider, public MOT::IConfigChangeListener {
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
    static JitStatisticsProvider& GetInstance();

    /** @brief Updates the statistics for total amount of jittable queries. */
    inline void AddJittableQuery()
    {
        JitGlobalStatistics* jgs = GetGlobalStatistics<JitGlobalStatistics>();
        if (jgs) {
            jgs->AddJittableQuery();
        }
    }

    /** @brief Updates the statistics for total amount of un-jittable queries due to query limit. */
    inline void AddUnjittableLimitQuery()
    {
        JitGlobalStatistics* jgs = GetGlobalStatistics<JitGlobalStatistics>();
        if (jgs) {
            jgs->AddUnjittableLimitQuery();
        }
    }

    /** @brief Updates the statistics for total amount of un-jittable queries  due to disqualification. */
    inline void AddUnjittableDisqualifiedQuery()
    {
        JitGlobalStatistics* jgs = GetGlobalStatistics<JitGlobalStatistics>();
        if (jgs) {
            jgs->AddUnjittableDisqualifiedQuery();
        }
    }

    /** @brief Updates the statistics for total amount of compiled queries. */
    inline void AddCodeGenQuery()
    {
        JitGlobalStatistics* jgs = GetGlobalStatistics<JitGlobalStatistics>();
        if (jgs) {
            jgs->AddCodeGenQuery();
        }
    }

    /** @brief Updates the statistics for total time required to generate code for a single query.. */
    inline void AddCodeGenTime(uint64_t micros)
    {
        JitGlobalStatistics* jgs = GetGlobalStatistics<JitGlobalStatistics>();
        if (jgs) {
            jgs->AddCodeGenTime(micros);
        }
    }

    /** @brief Updates the statistics for total amount of JIT query code generation error. */
    inline void AddCodeGenErrorQuery()
    {
        JitGlobalStatistics* jgs = GetGlobalStatistics<JitGlobalStatistics>();
        if (jgs) {
            jgs->AddCodeGenErrorQuery();
        }
    }

    /** @brief Updates the statistics for total amount of JIT query code cloning. */
    inline void AddCodeCloneQuery()
    {
        JitGlobalStatistics* jgs = GetGlobalStatistics<JitGlobalStatistics>();
        if (jgs) {
            jgs->AddCodeCloneQuery();
        }
    }

    /** @brief Updates the statistics for total amount of JIT query code cloning error. */
    inline void AddCodeCloneErrorQuery()
    {
        JitGlobalStatistics* jgs = GetGlobalStatistics<JitGlobalStatistics>();
        if (jgs) {
            jgs->AddCodeCloneErrorQuery();
        }
    }

    /** @brief Updates the statistics for total amount of JIT queries that expired. */
    inline void AddCodeExpiredQuery()
    {
        JitGlobalStatistics* jgs = GetGlobalStatistics<JitGlobalStatistics>();
        if (jgs) {
            jgs->AddCodeExpiredQuery();
        }
    }

    /** @brief Records a transaction event. */
    inline void AddExecQuery()
    {
        JitThreadStatistics* jts = GetCurrentThreadStatistics<JitThreadStatistics>();
        if (jts != nullptr) {
            jts->AddExecQuery();
        }
    }

    inline void AddInvokeQuery()
    {
        JitThreadStatistics* jts = GetCurrentThreadStatistics<JitThreadStatistics>();
        if (jts != nullptr) {
            jts->AddInvokeQuery();
        }
    }

    /** @brief Records a insert-row event. */
    inline void AddFailExecQuery()
    {
        JitThreadStatistics* jts = GetCurrentThreadStatistics<JitThreadStatistics>();
        if (jts != nullptr) {
            jts->AddExecFailQuery();
        }
    }

    /** @brief Records a update-row event. */
    inline void AddAbortExecQuery()
    {
        JitThreadStatistics* jts = GetCurrentThreadStatistics<JitThreadStatistics>();
        if (jts != nullptr) {
            jts->AddExecAbortQuery();
        }
    }

    /** @var Updates the number of session-bytes allocated for JIT query execution. */
    inline void AddSessionBytes(int64_t bytes)
    {
        JitThreadStatistics* jts = GetCurrentThreadStatistics<JitThreadStatistics>();
        if (jts != nullptr) {
            jts->AddSessionBytes(bytes);
        }
    }

    /** @var Updates the number of global-bytes allocated for JIT query execution. */
    inline void AddGlobalBytes(int64_t bytes)
    {
        JitGlobalStatistics* jgs = GetGlobalStatistics<JitGlobalStatistics>();
        if (jgs) {
            jgs->AddGlobalBytes(bytes);
        }
    }

    /**
     * @brief Derives classes should react to a notification that configuration changed. New
     * configuration is accessible via the ConfigManager.
     */
    void OnConfigChange() override;

private:
    /** @brief Constructor. */
    JitStatisticsProvider();

    /** @brief Destructor. */
    ~JitStatisticsProvider() override;

    /** @brief Registers the provider in the manager. */
    void RegisterProvider();

    /** @var The single instance. */
    static JitStatisticsProvider* m_provider;

    /** @var The generator used for creating global and thread-level statistics objects. */
    static MOT::TypedStatisticsGenerator<JitThreadStatistics, JitGlobalStatistics> m_generator;
};
}  // namespace JitExec

#endif /* JIT_STATISTICS_H */
