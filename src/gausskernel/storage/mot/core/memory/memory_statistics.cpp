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
 * memory_statistics.cpp
 *    Memory statistics implementation.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/memory/memory_statistics.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "memory_statistics.h"
#include "mot_configuration.h"
#include "config_manager.h"
#include "statistics_manager.h"
#include "mm_cfg.h"
#include "mm_numa.h"
#include "mm_raw_chunk_store.h"
#include "mm_session_large_buffer_store.h"
#include "mm_huge_object_allocator.h"
#include "mm_session_api.h"
#include "mm_global_api.h"
#include "mot_error.h"
#include "mm_api.h"

namespace MOT {
DECLARE_LOGGER(MemoryStatistics, Memory)

DetailedMemoryThreadStatistics::DetailedMemoryThreadStatistics(uint64_t threadId, void* inplaceBuffer)
    : ThreadStatistics(threadId, inplaceBuffer),
      m_globalBytesRequested(MakeName("global-bytes-requested", threadId).c_str(), MEGA_BYTE, "MB"),
      m_sessionBytesRequested(MakeName("session-bytes-requested", threadId).c_str(), MEGA_BYTE, "MB")
{
    errno_t erc;
    // configure name of all statistic variables
    char name[64];
    uint32_t nodeCount = GetGlobalConfiguration().m_numaNodes;
    // node level statistics
    for (uint32_t i = 0; i < nodeCount; ++i) {
        erc = snprintf_s(name, sizeof(name), sizeof(name) - 1, "global-[%u]-chunks-used", i);
        securec_check_ss(erc, "\0", "\0");
        m_globalChunksUsedNode[i].Configure(MakeName(name, threadId).c_str(), MEGA_BYTE, "MB");

        erc = snprintf_s(name, sizeof(name), sizeof(name) - 1, "local-[%u]-chunks-used", i);
        securec_check_ss(erc, "\0", "\0");
        m_localChunksUsedNode[i].Configure(MakeName(name, threadId).c_str(), MEGA_BYTE, "MB");

        erc = snprintf_s(name, sizeof(name), sizeof(name) - 1, "global-[%u]-buffers-used", i);
        securec_check_ss(erc, "\0", "\0");
        m_globalBuffersUsedNode[i].Configure(MakeName(name, threadId).c_str(), MEGA_BYTE, "MB");

        erc = snprintf_s(name, sizeof(name), sizeof(name) - 1, "local-[%u]-buffers-used", i);
        securec_check_ss(erc, "\0", "\0");
        m_localBuffersUsedNode[i].Configure(MakeName(name, threadId).c_str(), MEGA_BYTE, "MB");
    }

    // per node per per buffer type
    for (int i = 0; i < (int)nodeCount; ++i) {
        for (MemBufferClass j = MEM_BUFFER_CLASS_SMALLEST; j < MEM_BUFFER_CLASS_COUNT; ++j) {
            erc =
                snprintf_s(name, sizeof(name), sizeof(name) - 1, "global-[%d]-[%s]-used", i, MemBufferClassToString(j));
            securec_check_ss(erc, "\0", "\0");
            m_globalBuffersUsed[i][j].Configure(MakeName(name, threadId).c_str(), MEGA_BYTE, "MB");

            erc =
                snprintf_s(name, sizeof(name), sizeof(name) - 1, "local-[%d]-[%s]-used", i, MemBufferClassToString(j));
            securec_check_ss(erc, "\0", "\0");
            m_localBuffersUsed[i][j].Configure(MakeName(name, threadId).c_str(), MEGA_BYTE, "MB");
        }
    }

    // register all statistic variables
    RegisterStatistics(&m_globalBytesRequested);
    RegisterStatistics(&m_sessionBytesRequested);

    for (uint32_t i = 0; i < nodeCount; ++i) {
        RegisterStatistics(&m_globalChunksUsedNode[i]);
        RegisterStatistics(&m_localChunksUsedNode[i]);
        RegisterStatistics(&m_globalBuffersUsedNode[i]);
        RegisterStatistics(&m_localBuffersUsedNode[i]);
    }

    for (uint32_t i = 0; i < nodeCount; ++i) {
        for (MemBufferClass j = MEM_BUFFER_CLASS_SMALLEST; j < MEM_BUFFER_CLASS_COUNT; ++j) {
            RegisterStatistics(&m_globalBuffersUsed[i][j]);
            RegisterStatistics(&m_localBuffersUsed[i][j]);
        }
    }
}

DetailedMemoryGlobalStatistics::DetailedMemoryGlobalStatistics(GlobalStatistics::NamingScheme namingScheme)
    : GlobalStatistics()
{
    errno_t erc;
    const uint32_t nameLen = 64;
    char name[nameLen];
    uint32_t nodeCount = GetGlobalConfiguration().m_numaNodes;
    for (uint32_t i = 0; i < nodeCount; ++i) {
        erc = snprintf_s(name, nameLen, nameLen - 1, "numa-local-[%u]-allocated", i);
        securec_check_ss(erc, "\0", "\0");
        m_numaLocalAllocated[i].Configure(MakeName(name, namingScheme).c_str(), MEGA_BYTE, "MB");

        erc = snprintf_s(name, nameLen, nameLen - 1, "global-[%u]-chunks-reserved", i);
        securec_check_ss(erc, "\0", "\0");
        m_globalChunksReserved[i].Configure(MakeName(name, namingScheme).c_str(), MEGA_BYTE, "MB");

        erc = snprintf_s(name, nameLen, nameLen - 1, "local-[%u]-chunks-reserved", i);
        securec_check_ss(erc, "\0", "\0");
        m_localChunksReserved[i].Configure(MakeName(name, namingScheme).c_str(), MEGA_BYTE, "MB");
    }

    for (uint32_t i = 0; i < nodeCount; ++i) {
        RegisterStatistics(&m_numaLocalAllocated[i]);
        RegisterStatistics(&m_globalChunksReserved[i]);
        RegisterStatistics(&m_localChunksReserved[i]);
    }
}

MemoryThreadStatistics::MemoryThreadStatistics(uint64_t threadId, void* inplaceBuffer)
    : ThreadStatistics(threadId, inplaceBuffer),
      m_globalChunksUsed(MakeName("global-chunks-used", threadId).c_str(), MEGA_BYTE, "MB"),
      m_localChunksUsed(MakeName("local-chunks-used", threadId).c_str(), MEGA_BYTE, "MB"),
      m_globalBuffersUsed(MakeName("global-buffers-used", threadId).c_str(), MEGA_BYTE, "MB"),
      m_localBuffersUsed(MakeName("local-buffers-used", threadId).c_str(), MEGA_BYTE, "MB"),
      m_globalBytesUsed(MakeName("global-bytes-used", threadId).c_str(), MEGA_BYTE, "MB"),
      m_sessionBytesUsed(MakeName("session-bytes-used", threadId).c_str(), MEGA_BYTE, "MB"),
      m_mallocTime(MakeName("malloc-time", threadId).c_str(), 1, "nanos"),
      m_freeTime(MakeName("free-time", threadId).c_str(), 1, "nanos"),
      m_gcRetiredBytes(MakeName("gc-retired-bytes", threadId).c_str(), MEGA_BYTE, "MB"),
      m_gcReclaimedBytes(MakeName("gc-reclaimed-bytes", threadId).c_str(), MEGA_BYTE, "MB"),
      m_masstreeBytesUsed(MakeName("masstree-bytes-used", threadId).c_str(), MEGA_BYTE, "MB")
{
    // register all statistic variables
    RegisterStatistics(&m_globalChunksUsed);
    RegisterStatistics(&m_localChunksUsed);
    RegisterStatistics(&m_globalBuffersUsed);
    RegisterStatistics(&m_localBuffersUsed);
    RegisterStatistics(&m_globalBytesUsed);
    RegisterStatistics(&m_sessionBytesUsed);
    RegisterStatistics(&m_mallocTime);
    RegisterStatistics(&m_freeTime);
    RegisterStatistics(&m_gcRetiredBytes);
    RegisterStatistics(&m_gcReclaimedBytes);
    RegisterStatistics(&m_masstreeBytesUsed);
}

MemoryGlobalStatistics::MemoryGlobalStatistics(GlobalStatistics::NamingScheme namingScheme)
    : GlobalStatistics(),
      m_numaInterleavedAllocated(MakeName("numa-interleaved-allocated", namingScheme).c_str(), MEGA_BYTE, "MB"),
      m_numaLocalAllocated(MakeName("numa-local-allocated", namingScheme).c_str(), MEGA_BYTE, "MB"),
      m_globalChunksReserved(MakeName("global-chunks-reserved", namingScheme).c_str(), MEGA_BYTE, "MB"),
      m_localChunksReserved(MakeName("local-chunks-reserved", namingScheme).c_str(), MEGA_BYTE, "MB")
{
    RegisterStatistics(&m_numaInterleavedAllocated);
    RegisterStatistics(&m_numaLocalAllocated);
    RegisterStatistics(&m_globalChunksReserved);
    RegisterStatistics(&m_localChunksReserved);
}

TypedStatisticsGenerator<DetailedMemoryThreadStatistics, DetailedMemoryGlobalStatistics>
    DetailedMemoryStatisticsProvider::m_generator;
DetailedMemoryStatisticsProvider* DetailedMemoryStatisticsProvider::m_provider = nullptr;

DetailedMemoryStatisticsProvider::DetailedMemoryStatisticsProvider()
    : StatisticsProvider("DetailedMemory", &m_generator, GetGlobalConfiguration().m_enableDetailedMemoryStatistics)
{}

DetailedMemoryStatisticsProvider::~DetailedMemoryStatisticsProvider()
{
    ConfigManager::GetInstance().RemoveConfigChangeListener(this);
    if (m_enable) {
        StatisticsManager::GetInstance().UnregisterStatisticsProvider(this);
    }
}

void DetailedMemoryStatisticsProvider::RegisterProvider()
{
    if (m_enable) {
        StatisticsManager::GetInstance().RegisterStatisticsProvider(this);
    }
    ConfigManager::GetInstance().AddConfigChangeListener(this);
}

bool DetailedMemoryStatisticsProvider::CreateInstance()
{
    bool result = false;
    MOT_ASSERT(m_provider == nullptr);
    if (m_provider == nullptr) {
        m_provider = new (std::nothrow) DetailedMemoryStatisticsProvider();
        if (!m_provider) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM,
                "Load Statistics",
                "Failed to allocate memory for Detailed Memory Statistics Provider, aborting");
        } else {
            result = m_provider->Initialize();
            if (!result) {
                MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                    "Load Statistics",
                    "Failed to initialize Detailed Memory Statistics Provider, aborting");
                delete m_provider;
                m_provider = nullptr;
            } else {
                m_provider->RegisterProvider();
            }
        }
    }
    return result;
}

void DetailedMemoryStatisticsProvider::DestroyInstance()
{
    MOT_ASSERT(m_provider != nullptr);
    if (m_provider != nullptr) {
        delete m_provider;
        m_provider = nullptr;
    }
}

DetailedMemoryStatisticsProvider& DetailedMemoryStatisticsProvider::GetInstance()
{
    MOT_ASSERT(m_provider != nullptr);
    return *m_provider;
}

void DetailedMemoryStatisticsProvider::OnConfigChange()
{
    if (m_enable != GetGlobalConfiguration().m_enableDetailedMemoryStatistics) {
        m_enable = GetGlobalConfiguration().m_enableDetailedMemoryStatistics;
        if (m_enable) {
            StatisticsManager::GetInstance().RegisterStatisticsProvider(this);
        } else {
            StatisticsManager::GetInstance().UnregisterStatisticsProvider(this);
        }
    }
}

TypedStatisticsGenerator<MemoryThreadStatistics, MemoryGlobalStatistics> MemoryStatisticsProvider::m_generator;
MemoryStatisticsProvider* MemoryStatisticsProvider::m_provider = nullptr;

MemoryStatisticsProvider::MemoryStatisticsProvider()
    : StatisticsProvider("Memory", &m_generator, GetGlobalConfiguration().m_enableMemoryStatistics, true)
{}

MemoryStatisticsProvider::~MemoryStatisticsProvider()
{
    ConfigManager::GetInstance().RemoveConfigChangeListener(this);
    if (m_enable) {
        StatisticsManager::GetInstance().UnregisterStatisticsProvider(this);
    }
}

void MemoryStatisticsProvider::RegisterProvider()
{
    if (m_enable) {
        StatisticsManager::GetInstance().RegisterStatisticsProvider(this);
    }
    ConfigManager::GetInstance().AddConfigChangeListener(this);
}

bool MemoryStatisticsProvider::CreateInstance()
{
    bool result = false;
    MOT_ASSERT(m_provider == nullptr);
    if (m_provider == nullptr) {
        m_provider = new (std::nothrow) MemoryStatisticsProvider();
        if (!m_provider) {
            MOT_REPORT_ERROR(
                MOT_ERROR_OOM, "Load Statistics", "Failed to allocate memory for Memory Statistics Provider, aborting");
        } else {
            result = m_provider->Initialize();
            if (!result) {
                MOT_REPORT_ERROR(
                    MOT_ERROR_INTERNAL, "Load Statistics", "Failed to initialize Memory Statistics Provider, aborting");
                delete m_provider;
                m_provider = nullptr;
            } else {
                m_provider->RegisterProvider();
            }
        }
    }
    return result;
}

void MemoryStatisticsProvider::DestroyInstance()
{
    MOT_ASSERT(m_provider != nullptr);
    if (m_provider != nullptr) {
        delete m_provider;
        m_provider = nullptr;
    }
}

MemoryStatisticsProvider& MemoryStatisticsProvider::GetInstance()
{
    MOT_ASSERT(m_provider != nullptr);
    return *m_provider;
}

void MemoryStatisticsProvider::OnConfigChange()
{
    if (m_enable != GetGlobalConfiguration().m_enableMemoryStatistics) {
        m_enable = GetGlobalConfiguration().m_enableMemoryStatistics;
        if (m_enable) {
            StatisticsManager::GetInstance().RegisterStatisticsProvider(this);
        } else {
            StatisticsManager::GetInstance().UnregisterStatisticsProvider(this);
        }
    }
}

void MemoryStatisticsProvider::PrintStatisticsEx()
{
    MemPrint("Periodic Status", LogLevel::LL_INFO, MEM_REPORT_SUMMARY);
}
}  // namespace MOT
