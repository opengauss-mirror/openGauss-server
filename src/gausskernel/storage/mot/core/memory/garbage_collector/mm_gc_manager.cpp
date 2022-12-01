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
 * mm_gc_manager.cpp
 *    Garbage-collector manager per-session.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/memory/garbage_collector/mm_gc_manager.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "mm_gc_manager.h"
#include "mot_configuration.h"
#include "mot_engine.h"

namespace MOT {
IMPLEMENT_CLASS_LOGGER(GcManager, GC);

class Table;

/** @var Lock to sync GC Manager's Limbo groups access by other threads */
GcLock g_gcGlobalEpochLock;

GcManager* GcManager::allGcManagers = nullptr;

static GcQueueEntry gcQueuesParams[static_cast<uint8_t>(GC_QUEUE_TYPE::GC_QUEUES)] = {
    {GC_QUEUE_TYPE::DELETE_QUEUE, 1024, 8388608, 500},
    {GC_QUEUE_TYPE::VERSION_QUEUE, 102400, 8388608, 1000},
    {GC_QUEUE_TYPE::UPDATE_COLUMN_QUEUE, 1, 8388608, 100000},
    {GC_QUEUE_TYPE::GENERIC_QUEUE, 102400, 8388608, 1000}};

uint64_t GetGlobalEpoch()
{
    return MOTEngine::GetInstance()->GetGcEpoch();
}

uint64_t GetCurrentSnapshotCSN()
{
    return MOTEngine::GetInstance()->GetCurrentCSN();
}

inline GcManager::GcManager(GC_TYPE purpose, int threadId, bool global /* = false */)
    : m_gcEpoch(0),
      m_limboGroupPool(nullptr),
      m_next(nullptr),
      m_limboGroupAllocations(0),
      m_tid((uint16_t)threadId),
      m_purpose(purpose),
      m_global(global)
{}

bool GcManager::Initialize()
{
    bool result = true;
    m_limboGroupPool = ObjAllocInterface::GetObjPool(sizeof(LimboGroup), !m_global);
    if (!m_limboGroupPool) {
        MOT_REPORT_ERROR(
            MOT_ERROR_OOM, "Initialize ObjectPool", "Failed to allocate LimboGroup pool for thread %d", GetThreadId());
        return false;
    }

    uint32_t queueSize = static_cast<uint32_t>(GC_QUEUE_TYPE::GC_QUEUES);
    m_GcQueues.reserve(queueSize);
    for (uint32_t q = 0; q < queueSize; q++) {
        m_GcQueues.push_back(GcQueue(gcQueuesParams[q]));
        result = m_GcQueues[q].Initialize(this);
        if (result == false) {
            return result;
        }
    }

    if (!m_reservedManager.initialize()) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Create GC Context", "Failed to create GcQueueReservedMemory");
        result = false;
    }

    return result;
}

GcManager* GcManager::Make(GC_TYPE purpose, int threadId, bool global)
{
    static bool once = false;
    GcManager* gc = nullptr;
    void* gcBuffer = MemAlloc(sizeof(GcManager), global);

    if (gcBuffer == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Create GC Context",
            "Failed to allocate %u bytes for GcManager object",
            (unsigned)sizeof(GcManager));
        return nullptr;
    } else {
        errno_t erc = memset_s(gcBuffer, sizeof(GcManager), 0, sizeof(GcManager));
        securec_check(erc, "\0", "\0");
        gc = new (gcBuffer) GcManager(purpose, threadId, global);
        if (!gc->Initialize()) {
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL, "Create GC Context", "Failed to initialize GC context object");
            gc->~GcManager();
            gc = nullptr;
            MemFree(gcBuffer, global);
        } else {
            if (!once) {
                MOT_LOG_INFO("GC PARAMS: ");
                for (uint32_t queue = 0; queue < gc->m_GcQueues.size(); ++queue) {
                    MOT_LOG_INFO("QUEUE%u:%s SizeLimit = %uKB, SizeLimitHigh = %uKB, Count = %u",
                        queue,
                        GcQueue::enGcQueue[queue],
                        gcQueuesParams[queue].m_limboSizeLimit / 1024,
                        gcQueuesParams[queue].m_limboSizeLimitHigh / 1024,
                        gcQueuesParams[queue].m_rcuFreeCount)
                }
                once = true;
            }
        }
    }

    return gc;
}

void GcManager::CleanIndexItems(uint32_t indexId, GC_OPERATION_TYPE oper)
{
    uint32_t counter = 0;

    m_managerLock.lock();
    for (uint32_t queue = 0; queue < m_GcQueues.size(); ++queue) {
        counter += m_GcQueues[queue].CleanIndexItems(indexId, oper);
    }
    m_managerLock.unlock();

    if (counter) {
        uint64_t items = GetTotalLimboInuseElements();
        MOT_LOG_INFO("Entity:%s threadId = %d cleaned from index id = %d items = %u inuseItems = %lu",
            enGcTypes[static_cast<uint8_t>(m_purpose)],
            m_tid,
            indexId,
            counter,
            items);
    }
}

bool GcManager::ReserveGCMemory(uint32_t elements)
{
    bool res = true;
    for (uint32_t queue = 0; queue < m_GcQueues.size(); ++queue) {
        if (m_GcQueues[queue].GetFreeAllocations() < elements) {
            res = m_GcQueues[queue].ReserveGCMemory(elements);
            if (res == false) {
                return false;
            }
        }
    }
    return res;
}

bool GcManager::ReserveGCMemoryPerQueue(GC_QUEUE_TYPE queueId, uint32_t elements, bool reserveGroups)
{
    if (elements == 0) {
        return true;
    }

    MOT_ASSERT(queueId < GC_QUEUE_TYPE::GC_QUEUES);

    return m_GcQueues[static_cast<uint8_t>(queueId)].ReserveGCMemory(elements, reserveGroups);
}

bool GcManager::ReserveGCRollbackMemory(uint32_t elements)
{
    LimboGroup* list = nullptr;
    if (elements == 0) {
        return true;
    }

    uint32_t limboGroups = static_cast<uint32_t>(std::ceil(double(elements) / LimboGroup::CAPACITY));
    list = AllocLimboGroups(limboGroups);
    if (list == nullptr) {
        return false;
    }
    m_reservedManager.ReserveGCMemory(list, elements);
    return true;
}

LimboGroup* GcManager::AllocLimboGroups(uint32_t limboGroups)
{
    LimboGroup* currentTail = nullptr;
    LimboGroup* currentHead = nullptr;
    for (uint32_t group = 0; group < limboGroups; group++) {
        LimboGroup* limboGroup = CreateNewLimboGroup();
        if (limboGroup != nullptr) {
            if (!currentTail) {
                currentTail = limboGroup;
                currentHead = currentTail;
            } else {
                limboGroup->m_next = currentHead;
                currentHead = limboGroup;
            }
        } else {
            // Release memory
            while (currentHead) {
                LimboGroup* tmp = currentHead;
                currentHead = currentHead->m_next;
                DestroyLimboGroup(tmp);
            }
            MOT_REPORT_ERROR(MOT_ERROR_OOM,
                "GC Reserve Limbo Group",
                "Failed to allocate %u bytes for limbo group",
                (unsigned)sizeof(LimboGroup));
            return nullptr;
        }
    }

    return currentHead;
}

void GcManager::RunQuicese()
{
    bool isBarrierReached = false;
    GcEpochType performGCEpoch = 0;
    for (uint32_t queue = 0; queue < m_GcQueues.size(); ++queue) {
        isBarrierReached = false;
        m_gcEpoch = GetGlobalEpoch();
        bool isThresholdReached = m_GcQueues[queue].IsThresholdReached();
        if (isThresholdReached) {
            if (!m_isThresholdReached) {
                m_GcQueues[queue].SetPerformGcEpoch(g_gcActiveEpoch);
            } else {
                m_GcQueues[queue].SetPerformGcEpoch(performGCEpoch);
            }

            if (!m_isThresholdReached) {
                if (m_GcQueues[queue].GetPerformGcEpoch() <= m_GcQueues[queue].FirstEpoch()) {
                    MOT_LOG_DEBUG("Entity:%s THD_ID:%d Increase Epoch", GcQueue::enGcQueue[queue], m_tid);
                    SetActiveEpoch(isBarrierReached, GetThreadId());
                    m_GcQueues[queue].SetPerformGcEpoch(g_gcActiveEpoch);
                    performGCEpoch = m_GcQueues[queue].GetPerformGcEpoch();
                    m_isThresholdReached = true;
                }
            }

            m_GcQueues[queue].RunQuiesce();
        }
    }
    GcMaintenance();
    m_isThresholdReached = false;
    m_gcEpoch = 0;
}

void GcManager::GcCleanAll()
{
    m_gcEpoch = GetGlobalEpoch();
    bool isBarrierReached = false;
    MOT_LOG_DEBUG("Entity:%s THD_ID:%d start closing session .... #allocations = %d",
        enGcTypes[static_cast<uint8_t>(m_purpose)],
        m_tid,
        m_limboGroupAllocations);

    uint64_t inuseElements = 0;
    for (uint32_t queue = 0; queue < m_GcQueues.size(); ++queue) {
        inuseElements += m_GcQueues[queue].m_stats.m_totalLimboInuseElements;
        while (m_GcQueues[queue].m_stats.m_totalLimboInuseElements > 0) {
            // Read Latest snapshot
            m_gcEpoch = GetGlobalEpoch();
            m_managerLock.lock();
            // Increase the global epoch to insure all elements are from a lower epoch
            SetActiveEpoch(isBarrierReached, GetThreadId());
            if ((queue == static_cast<uint32_t>(GC_QUEUE_TYPE::GENERIC_QUEUE) or
                    MOTEngine::GetInstance()->IsRecovering()) and
                isBarrierReached) {
                m_GcQueues[queue].SetPerformGcEpoch(g_gcActiveEpoch + 1);
            } else {
                m_GcQueues[queue].SetPerformGcEpoch(g_gcActiveEpoch);
            }
            uint64_t cleaned = m_GcQueues[queue].m_stats.m_totalLimboInuseElements;
            m_GcQueues[queue].HardQuiesce(m_GcQueues[queue].m_stats.m_totalLimboInuseElements);
            if (cleaned != m_GcQueues[queue].m_stats.m_totalLimboInuseElements) {
                MOT_LOG_DEBUG("Entity:%s THD_ID:%d cleaned %lu from queue %s elements left: %lu elements",
                    enGcTypes[static_cast<uint8_t>(m_purpose)],
                    m_tid,
                    cleaned - m_GcQueues[queue].m_stats.m_totalLimboInuseElements,
                    GcQueue::enGcQueue[queue],
                    m_GcQueues[queue].m_stats.m_totalLimboInuseElements);
            }
            // Reset local epoch
            m_gcEpoch = 0;
            m_GcQueues[queue].RegisterDeletedSentinels();
            m_managerLock.unlock();
            // every 20ms + break symmetry
            if (m_GcQueues[queue].m_stats.m_totalLimboInuseElements > 0) {
                (void)usleep(GC_CLEAN_SLEEP + m_tid * GC_CLEAN_SESSION_SLEEP);
            }
        }
        m_managerLock.lock();
        m_GcQueues[queue].Maintenance();
        m_managerLock.unlock();
    }

    m_gcEpoch = 0;
    MOT_LOG_DEBUG("Entity:%s THD_ID:%d closed session cleaned %lu elements from limbo! #allocations = %u",
        enGcTypes[static_cast<uint8_t>(m_purpose)],
        m_tid,
        inuseElements,
        m_limboGroupAllocations);
}

void GcManager::RemoveFromGcList(GcManager* n)
{
    // When node to be deleted is head node
    MOT_ASSERT(n != nullptr);
    g_gcGlobalEpochLock.lock();

    GcManager* head = allGcManagers;

    if (head == n) {
        head = head->m_next;
        allGcManagers = head;
        g_gcGlobalEpochLock.unlock();
        return;
    }

    // When not first node, follow the normal deletion process
    // find the previous node
    GcManager* prev = head;
    while (prev->m_next != nullptr && prev->m_next != n) {
        prev = prev->m_next;
    }

    // Check if node really exists in Linked List
    if (prev->m_next == nullptr) {
        g_gcGlobalEpochLock.unlock();
        return;
    }

    // Remove node from Linked List
    prev->m_next = prev->m_next->m_next;
    g_gcGlobalEpochLock.unlock();
    return;
}

void GcManager::ReportGcStats()
{
    MOT_LOG_INFO("----------GC Thd:%d m_gcEpoch = %lu-----------", m_tid, m_gcEpoch);
    MOT_LOG_INFO("total_limbo_inuse_elements = %lu", GetTotalLimboInuseElements());
    MOT_LOG_INFO("limbo_group_allocations = %u", GetLimboGroupAllocations());
    MOT_LOG_INFO("total_limbo_reclaimed_size_in_bytes = %lu in MB = %lf",
        GetTotalLimboReclaimedSizeInBytes(),
        double((double)GetTotalLimboReclaimedSizeInBytes() / ONE_MB));
    MOT_LOG_INFO("total_limbo_retired_size_in_bytes = %lu in MB = %lf",
        GetTotalLimboRetiredSizeInBytes(),
        double((double)GetTotalLimboRetiredSizeInBytes() / ONE_MB));
    PoolStatsSt stats;
    errno_t erc = memset_s(&stats, sizeof(PoolStatsSt), 0, sizeof(PoolStatsSt));
    securec_check(erc, "\0", "\0");
    stats.m_type = PoolStatsT::POOL_STATS_ALL;

    m_limboGroupPool->GetStats(stats);
    m_limboGroupPool->PrintStats(stats, "Limbo Group Pool", LogLevel::LL_INFO);
}

void GcManager::ReportGcAll()
{
    for (GcManager* ti = allGcManagers; ti; ti = ti->Next()) {
        ti->ReportGcStats();
    }
}

void GcManager::GcReinitEpoch()
{
    m_gcEpoch = 0;
    COMPILER_BARRIER;
    if (MOTEngine::GetInstance()->IsRecovering()) {
        while (MOTEngine::GetInstance()->IsRecoveryPerformingCleanup()) {
            PAUSE;
        }
    }
    if (m_isTxnSnapshotTaken) {
        m_gcEpoch = GetGlobalEpoch();
    }
}

}  // namespace MOT
