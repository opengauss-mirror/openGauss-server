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
 * jit_statistics.cpp
 *    Statistics collection for JIT module.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/jit_exec/jit_statistics.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "jit_statistics.h"
#include "mot_configuration.h"
#include "config_manager.h"
#include "statistics_manager.h"
#include "mot_error.h"

namespace JitExec {
DECLARE_LOGGER(JitStatistics, System)

JitThreadStatistics::JitThreadStatistics(uint64_t threadId, void* inplaceBuffer)
    : MOT::ThreadStatistics(threadId, inplaceBuffer),
      m_execQueryCount(MakeName("jit-exec", threadId).c_str()),
      m_invokeQueryCount(MakeName("jit-invoke", threadId).c_str()),
      m_execFailQueryCount(MakeName("jit-exec-fail", threadId).c_str()),
      m_execAbortQueryCount(MakeName("jit-exec-abort", threadId).c_str())
{
    RegisterStatistics(&m_execQueryCount);
    RegisterStatistics(&m_invokeQueryCount);
    RegisterStatistics(&m_execFailQueryCount);
    RegisterStatistics(&m_execAbortQueryCount);
}

JitGlobalStatistics::JitGlobalStatistics(GlobalStatistics::NamingScheme namingScheme)
    : GlobalStatistics(),
      m_jittableQueryCount(MakeName("jittable-queries", namingScheme).c_str(), 1, "queries"),
      m_unjittableLimitQueryCount(MakeName("unjittable-limit-queries", namingScheme).c_str(), 1, "queries"),
      m_unjittableDisqualifiedQueryCount(MakeName("disqualified-queries", namingScheme).c_str(), 1, "queries"),
      m_codeGenQueryCount(MakeName("code-gen-queries", namingScheme).c_str(), 1, "queries"),
      m_codeGenTime(MakeName("code-gen-time", namingScheme).c_str(), 1000, "millis"),
      m_codeGenErrorQueryCount(MakeName("code-gen-error-queries", namingScheme).c_str(), 1, "queries"),
      m_codeCloneQueryCount(MakeName("code-clone-queries", namingScheme).c_str(), 1, "queries"),
      m_codeCloneErrorQueryCount(MakeName("code-clone-error-queries", namingScheme).c_str(), 1, "queries"),
      m_codeExpiredQueryCount(MakeName("code-expired-queries", namingScheme).c_str(), 1, "queries")
{
    RegisterStatistics(&m_jittableQueryCount);
    RegisterStatistics(&m_unjittableLimitQueryCount);
    RegisterStatistics(&m_unjittableDisqualifiedQueryCount);
    RegisterStatistics(&m_codeGenQueryCount);
    RegisterStatistics(&m_codeGenTime);
    RegisterStatistics(&m_codeGenErrorQueryCount);
    RegisterStatistics(&m_codeCloneQueryCount);
    RegisterStatistics(&m_codeCloneErrorQueryCount);
    RegisterStatistics(&m_codeExpiredQueryCount);
}

MOT::TypedStatisticsGenerator<JitThreadStatistics, JitGlobalStatistics> JitStatisticsProvider::m_generator;
JitStatisticsProvider* JitStatisticsProvider::m_provider = nullptr;

JitStatisticsProvider::JitStatisticsProvider()
    : MOT::StatisticsProvider("JIT", &m_generator, MOT::GetGlobalConfiguration().m_enableJitStatistics)
{}

JitStatisticsProvider::~JitStatisticsProvider()
{
    MOT::ConfigManager::GetInstance().RemoveConfigChangeListener(this);
    if (m_enable) {
        MOT::StatisticsManager::GetInstance().UnregisterStatisticsProvider(this);
    }
}

void JitStatisticsProvider::RegisterProvider()
{
    if (m_enable) {
        MOT::StatisticsManager::GetInstance().RegisterStatisticsProvider(this);
    }
    MOT::ConfigManager::GetInstance().AddConfigChangeListener(this);
}

bool JitStatisticsProvider::CreateInstance()
{
    bool result = false;
    MOT_ASSERT(m_provider == nullptr);
    if (m_provider == nullptr) {
        m_provider = new (std::nothrow) JitStatisticsProvider();
        if (m_provider == nullptr) {
            MOT_REPORT_ERROR(
                MOT_ERROR_OOM, "Load Statistics", "Failed to allocate memory for JIT Statistics Provider, aborting");
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

void JitStatisticsProvider::DestroyInstance()
{
    MOT_ASSERT(m_provider != nullptr);
    if (m_provider != nullptr) {
        delete m_provider;
        m_provider = nullptr;
    }
}

JitStatisticsProvider& JitStatisticsProvider::GetInstance()
{
    MOT_ASSERT(m_provider != nullptr);
    return *m_provider;
}

void JitStatisticsProvider::OnConfigChange()
{
    if (m_enable != MOT::GetGlobalConfiguration().m_enableJitStatistics) {
        m_enable = MOT::GetGlobalConfiguration().m_enableJitStatistics;
        if (m_enable) {
            MOT::StatisticsManager::GetInstance().RegisterStatisticsProvider(this);
        } else {
            MOT::StatisticsManager::GetInstance().UnregisterStatisticsProvider(this);
        }
    }
}
}  // namespace JitExec