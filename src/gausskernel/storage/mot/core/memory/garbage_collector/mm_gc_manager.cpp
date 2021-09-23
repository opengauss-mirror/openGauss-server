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

namespace MOT {
IMPLEMENT_CLASS_LOGGER(GcManager, GC);

class Table;

/** @var Global epoch, updated every barrier */
volatile GcEpochType g_gcGlobalEpoch = 1;
/** @var Current lowest epoch among all active GC Managers */
volatile GcEpochType g_gcActiveEpoch = 1;

/** @var Lock to sync GC Manager's Limbo groups access by other threads */
GcLock g_gcGlobalEpochLock;

GcManager* GcManager::allGcManagers = nullptr;

inline GcManager::GcManager(GC_TYPE purpose, int getThreadId, int rcuMaxFreeCount)
    : m_rcuFreeCount(rcuMaxFreeCount), m_tid(getThreadId), m_purpose(purpose)
{}

bool GcManager::Initialize()
{
    bool result = true;

    if (m_purpose == GC_MAIN) {
        m_limboGroupPool = ObjAllocInterface::GetObjPool(sizeof(LimboGroup), true);
        if (!m_limboGroupPool) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM,
                "Initialize ObjectPool",
                "Failed to allocate LimboGroup pool for thread %d",
                GetThreadId());
            return false;
        }

#ifdef MOT_DEBUG
        PoolStatsSt stats;
        errno_t erc = memset_s(&stats, sizeof(PoolStatsSt), 0, sizeof(PoolStatsSt));
        securec_check(erc, "\0", "\0");
        stats.m_type = PoolStatsT::POOL_STATS_ALL;
        m_limboGroupPool->GetStats(stats);
        m_limboGroupPool->PrintStats(stats, "GC_MANAGER", LogLevel::LL_INFO);
#endif

        LimboGroup* limboGroup = CreateNewLimboGroup();
        if (limboGroup == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM,
                "Create GC Context",
                "Failed to allocate %u bytes for limbo group",
                (unsigned)sizeof(LimboGroup));
            result = false;
        } else {
            m_limboHead = m_limboTail = limboGroup;
            m_limboGroupAllocations = 1;
        }
    } else {
        m_limboHead = m_limboTail = nullptr;
        m_limboGroupAllocations = 0;
    }

    if (result) {
        m_totalLimboInuseElements = 0;
        m_totalLimboSizeInBytes = 0;
        m_totalLimboReclaimedSizeInBytes = 0;
        m_totalLimboRetiredSizeInBytes = 0;
        m_totalLimboSizeInBytesByCleanIndex = 0;
    }

    return result;
}

GcManager* GcManager::Make(GC_TYPE purpose, int threadId, int rcuMaxFreeCount)
{
    GcManager* gc = nullptr;

#ifdef MEM_SESSION_ACTIVE
    void* gcBuffer = MemSessionAlloc(sizeof(GcManager));
#else
    void* gcBuffer = malloc(sizeof(GcManager));
#endif

    if (gcBuffer == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Create GC Context",
            "Failed to allocate %u bytes for GcManager object",
            (unsigned)sizeof(GcManager));
        return nullptr;
    } else {
        errno_t erc = memset_s(gcBuffer, sizeof(GcManager), 0, sizeof(GcManager));
        securec_check(erc, "\0", "\0");
        gc = new (gcBuffer) GcManager(purpose, threadId, rcuMaxFreeCount);
        if (!gc->Initialize()) {
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL, "Create GC Context", "Failed to initialize GC context object");
            gc->~GcManager();

#ifdef MEM_SESSION_ACTIVE
            MemSessionFree(gcBuffer);
#else
            free(gcBuffer);
#endif
        } else {
            MOTConfiguration& cfg = GetGlobalConfiguration();
            gc->m_isGcEnabled = cfg.m_gcEnable;
            gc->m_limboSizeLimit = (uint32_t)cfg.m_gcReclaimThresholdBytes;
            gc->m_limboSizeLimitHigh = (uint32_t)cfg.m_gcHighReclaimThresholdBytes;
            gc->m_rcuFreeCount = cfg.m_gcReclaimBatchSize;
            if (threadId == 0) {
                MOT_LOG_INFO(
                    "GC PARAMS: isGcEnabled = %s, limboSizeLimit = %d, limboSizeLimitHigh = %d, rcuFreeCount = %d",
                    gc->m_isGcEnabled ? "true" : "false",
                    gc->m_limboSizeLimit,
                    gc->m_limboSizeLimitHigh,
                    gc->m_rcuFreeCount);
            }
        }
    }

    return gc;
}

void GcManager::HardQuiesce(uint32_t numOfElementsToClean)
{
    LimboGroup* emptyHead = nullptr;
    LimboGroup* emptyTail = nullptr;
    unsigned count = numOfElementsToClean;
    unsigned locCount = 0;

    // Update values from cleanIndexItemPerGroup flow
    if (m_totalLimboSizeInBytesByCleanIndex) {
        m_totalLimboSizeInBytes -= m_totalLimboSizeInBytesByCleanIndex;
        m_totalLimboReclaimedSizeInBytes += m_totalLimboSizeInBytesByCleanIndex;
        m_totalLimboSizeInBytesByCleanIndex = 0;
    }

    GcEpochType epochBound = g_gcActiveEpoch - 1;
    if (m_limboHead->m_head == m_limboHead->m_tail || GcSignedEpochType(epochBound - m_limboHead->FirstEpoch()) < 0)
        goto done;

    // clean [limbo_head_, limbo_tail_]
    while (count) {
        locCount = m_limboHead->CleanUntil(*this, epochBound, count);
        m_totalLimboInuseElements -= (count - locCount);
        count = locCount;
        if (m_limboHead->m_head != m_limboHead->m_tail) {
            break;
        }
        if (emptyHead == nullptr) {
            emptyHead = m_limboHead;
        }
        emptyTail = m_limboHead;
        if (m_limboHead == m_limboTail) {
            m_limboHead = m_limboTail = emptyHead;
            goto done;
        }
        m_limboHead = m_limboHead->m_next;
    }
    // hook empties after limbo_tail_
    if (emptyHead != nullptr) {
        emptyTail->m_next = m_limboTail->m_next;
        m_limboTail->m_next = emptyHead;
    }

done:
    if (!count) {
        m_performGcEpoch = epochBound;  // do GC again immediately
    } else {
        m_performGcEpoch = epochBound + 1;
    }

    MOT_LOG_DEBUG("threadId = %d cleaned items = %d\n", m_tid, m_rcuFreeCount - count);
}

inline unsigned LimboGroup::CleanUntil(GcManager& ti, GcEpochType epochBound, unsigned count)
{
    EpochType epoch = 0;
    uint32_t size = 0;
    while (m_head != m_tail) {
        if (m_elements[m_head].m_objectPtr) {
            size = m_elements[m_head].m_cb(m_elements[m_head].m_objectPtr, m_elements[m_head].m_objectPool, false);
            MemoryStatisticsProvider::m_provider->AddGCReclaimedBytes(size);
            ti.m_totalLimboSizeInBytes -= size;
            ti.m_totalLimboReclaimedSizeInBytes += size;  // stats
            --count;
            if (!count) {
                m_elements[m_head].m_objectPtr = nullptr;
                m_elements[m_head].m_epoch = epoch;
                break;
            }
        } else {
            epoch = m_elements[m_head].m_epoch;
            if (SignedEpochType(epochBound - epoch) < 0) {
                break;
            }
        }
        ++m_head;
    }
    if (m_head == m_tail) {
        m_head = m_tail = 0;
    }
    return count;
}

inline unsigned LimboGroup::CleanIndexItemPerGroup(GcManager& ti, uint32_t indexId, bool dropIndex)
{
    unsigned gHead = m_head;
    unsigned gTail = m_tail;
    uint32_t size = 0;
    uint32_t itemCleaned = 0;

    while (gHead != gTail) {
        if (m_elements[gHead].m_objectPtr != nullptr && m_elements[gHead].m_indexId == indexId &&
            m_elements[gHead].m_cb != ti.NullDtor) {
            size = m_elements[gHead].m_cb(m_elements[gHead].m_objectPtr, m_elements[gHead].m_objectPool, dropIndex);
            MemoryStatisticsProvider::m_provider->AddGCReclaimedBytes(size);
            ti.m_totalLimboSizeInBytesByCleanIndex += size;
            m_elements[gHead].m_cb = ti.NullDtor;
            itemCleaned++;
        }
        ++gHead;
    }
    return itemCleaned;
}

void GcManager::CleanIndexItems(uint32_t indexId, bool dropIndex)
{
    if (m_isGcEnabled == false) {
        return;
    }
    uint32_t counter = 0;
    m_managerLock.lock();
    LimboGroup* head = m_limboHead;
    LimboGroup* tail = m_limboTail;
    while (head != nullptr) {
        counter += head->CleanIndexItemPerGroup(*this, indexId, dropIndex);
        head = head->m_next;
    }
    m_managerLock.unlock();
    if (counter) {
        MOT_LOG_INFO("Entity:%s threadId = %d cleaned from index id = %u items = %u\n",
            enGcTypes[m_purpose],
            m_tid,
            indexId,
            counter);
    }
}

bool GcManager::RefillLimboGroup()
{
    if (!m_limboTail->m_next) {
        LimboGroup* limboGroup = CreateNewLimboGroup();
        if (limboGroup == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM,
                "GC Refill Limbo Group",
                "Failed to allocate %u bytes for limbo group",
                (unsigned)sizeof(LimboGroup));
            return false;
        }
        m_limboTail->m_next = limboGroup;
        m_limboGroupAllocations++;
    }
    m_limboTail = m_limboTail->m_next;
    MOT_ASSERT(m_limboTail->m_head == 0 && m_limboTail->m_tail == 0);
    return true;
}

void GcManager::RemoveFromGcList(GcManager* n)
{
    // When node to be deleted is head node
    if (m_isGcEnabled == false) {
        return;
    }
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
        printf("\nGiven node is not present in Linked List\n");
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
    MOT_LOG_INFO("----------GC Thd:%d-----------\n", m_tid);
    MOT_LOG_INFO("total_limbo_inuse_elements = %d\n", m_totalLimboInuseElements);
    MOT_LOG_INFO("limbo_group_allocations = %d\n", m_limboGroupAllocations);
    MOT_LOG_INFO("total_limbo_reclaimed_size_in_bytes = %lld in MB = %lf \n",
        m_totalLimboReclaimedSizeInBytes,
        double((double)m_totalLimboReclaimedSizeInBytes / ONE_MB));
    MOT_LOG_INFO("total_limbo_retired_size_in_bytes = %lld in MB = %lf \n",
        m_totalLimboRetiredSizeInBytes,
        double((double)m_totalLimboRetiredSizeInBytes / ONE_MB));
    MOT_LOG_INFO("------------------------------\n");
}

void GcManager::ReportGcAll()
{
    for (GcManager* ti = allGcManagers; ti; ti = ti->Next()) {
        ti->ReportGcStats();
    }
}
}  // namespace MOT
