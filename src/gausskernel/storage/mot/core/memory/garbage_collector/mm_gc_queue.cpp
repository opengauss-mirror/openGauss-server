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
 * mm_gc_manager.h
 *    Garbage-collector queues per-session.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/memory/garbage_collector/mm_gc_queue.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "mm_gc_queue.h"
#include "sentinel.h"
#include "index.h"
#include <cmath>

namespace MOT {
IMPLEMENT_CLASS_LOGGER(GcQueue, GC);

class Sentinel;
class Index;

const char* const GcQueue::enGcQueue[] = {
    stringify(DELETE_QUEUE), stringify(VERSION_QUEUE), stringify(UPDATE_COLUMN_QUEUE), stringify(GENERIC_QUEUE)};

/** @var Current lowest epoch among all active GC Managers */
volatile GcEpochType g_gcActiveEpoch = 0;

inline uint64_t LimboGroup::CleanUntil(GcQueue& ti, GcEpochType epochBound, uint64_t count)
{
    EpochType csn = 0;
    uint64_t size = 0;
    GC_OPERATION_TYPE gcOper = GC_OPERATION_TYPE::GC_OPER_LOCAL;
    while (m_head != m_tail) {
        LimboElement* elem = &m_elements[m_head];
        if (elem->m_objectPtr) {
            if (elem->m_cb != GcQueue::NullDtor) {
                size = elem->m_cb(elem, &gcOper, ti.GetDeleteVector());
                MemoryStatisticsProvider::GetInstance().AddGCReclaimedBytes(size);
                ti.m_stats.m_totalLimboSizeInBytes -= size;
                ti.m_stats.m_totalLimboReclaimedSizeInBytes += size;  // stats
                elem->m_cb = GcQueue::NullDtor;
                --count;
                if (!count) {
                    m_elements[m_head].m_objectPtr = nullptr;
                    m_elements[m_head].m_csn = csn;
                    break;
                }
            }
        } else {
            csn = m_elements[m_head].m_csn;
            if (SignedEpochType(epochBound - csn) < 0) {
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

inline unsigned LimboGroup::CleanIndexItemPerGroup(GcQueue& ti, uint32_t indexId, GC_OPERATION_TYPE oper)
{
    unsigned gHead = m_head;
    uint32_t size = 0;
    uint32_t itemCleaned = 0;

    while (gHead != m_tail) {
        if (m_elements[gHead].m_objectPtr != nullptr && m_elements[gHead].m_indexId == indexId &&
            m_elements[gHead].m_cb != GcQueue::NullDtor) {
            size = m_elements[gHead].m_cb(&m_elements[gHead], &oper, ti.GetDeleteVector());
            MemoryStatisticsProvider::GetInstance().AddGCReclaimedBytes(size);
            ti.m_stats.m_totalLimboSizeInBytesByCleanIndex += size;
            m_elements[gHead].m_cb = GcQueue::NullDtor;
            itemCleaned++;
        }
        ++gHead;
    }
    return itemCleaned;
}

uint32_t LimboGroup::GetListLength()
{
    uint32_t count = 0;
    LimboGroup* head = this;
    while (head) {
        count++;
        head = head->m_next;
    }
    return count;
}

GcQueue::GcQueue(const GcQueueEntry& entry) : m_queueType(entry.m_type)
{
    m_stats.m_limboSizeLimit = entry.m_limboSizeLimit;
    m_stats.m_limboSizeLimitHigh = entry.m_limboSizeLimitHigh;
    m_stats.m_rcuFreeCount = entry.m_rcuFreeCount;
}

GcQueue::~GcQueue()
{
    if (m_deleteVector != nullptr) {
        delete m_deleteVector;
        m_deleteVector = nullptr;
    }

    m_manager = nullptr;
    m_limboHead = nullptr;
    m_limboTail = nullptr;
}

bool GcQueue::Initialize(GcManager* manager)
{
    m_manager = manager;

    LimboGroup* limboGroup = manager->CreateNewLimboGroup();
    if (limboGroup == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Create GC Context",
            "Failed to allocate %u bytes for limbo group",
            (unsigned)sizeof(LimboGroup));
        return false;
    }

    m_limboHead = m_limboTail = limboGroup;
    m_stats.m_limboGroupAllocations = 1;

    m_deleteVector = new (std::nothrow) DeleteVector();
    if (m_deleteVector == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Create GC Context", "Failed to allocate deleteList for limbo group");
        return false;
    }

    if (!m_reservedManager.initialize()) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Create GC Context", "Failed to create GcQueueReservedMemory");
        return false;  // safe cleanup during destroy
    }

    m_deleteVector->clear();
    if (manager->GetGcType() == GcManager::GC_TYPE::GC_RECOVERY) {
        m_stats.m_limboSizeLimit = RECOVERY_LIMBO_SIZE_LIMIT;
        m_stats.m_limboSizeLimitHigh = 2 * RECOVERY_LIMBO_SIZE_LIMIT;
    }
    return true;
}

bool GcQueue::RefillLimboGroup()
{
    LimboGroup* limboGroup = nullptr;
    if (!m_limboTail->m_next) {
        // First Tier - rollback pool
        limboGroup = m_manager->AllocLimboGroupFromReserved();
        if (limboGroup == nullptr) {
            // Second Tier - queue pool
            limboGroup = m_reservedManager.AllocLimboGroup();
            if (limboGroup == nullptr) {
                // Third Tier - global pool
                limboGroup = m_manager->CreateNewLimboGroup();
                if (limboGroup == nullptr) {
                    MOT_REPORT_ERROR(MOT_ERROR_OOM,
                        "GC Refill Limbo Group",
                        "Failed to allocate %u bytes for limbo group",
                        (unsigned)sizeof(LimboGroup));
                    return false;
                } else {
                    m_stats.m_limboGroupAllocations++;
                }
            }
        } else {
            // Update the stats - current queue using manager reserve
            m_stats.m_limboGroupAllocations++;
        }
        m_limboTail->m_next = limboGroup;
    }
    m_limboTail = m_limboTail->m_next;
    MOT_ASSERT(m_limboTail->m_head == 0 && m_limboTail->m_tail == 0);
    return true;
}

bool GcQueue::AllocLimboGroups(uint32_t limboGroups, uint32_t totalElements, bool reserveGroups)
{
    LimboGroup* currentTail = nullptr;
    LimboGroup* currentHead = nullptr;
    LimboGroup* limboGroup = nullptr;
    bool isAllocatedfromReserved = false;
    for (uint32_t group = 0; group < limboGroups; group++) {
        isAllocatedfromReserved = false;
        if (m_queueType == GC_QUEUE_TYPE::GENERIC_QUEUE and !reserveGroups) {
            // If we are using the generic queue - try first to allocate from reserved pool
            limboGroup = m_reservedManager.AllocLimboGroup();
            if (limboGroup) {
                isAllocatedfromReserved = true;
            }
        }
        if (!isAllocatedfromReserved) {
            limboGroup = m_manager->CreateNewLimboGroup();
        }
        if (limboGroup != nullptr) {
            if (!currentTail) {
                currentTail = limboGroup;
                currentHead = currentTail;
            } else {
                limboGroup->m_next = currentHead;
                currentHead = limboGroup;
            }
            if (!isAllocatedfromReserved) {
                m_stats.m_limboGroupAllocations++;
            }
        } else {
            // Release memory
            while (currentHead) {
                LimboGroup* tmp = currentHead;
                currentHead = currentHead->m_next;
                m_manager->DestroyLimboGroup(tmp);
                m_stats.m_limboGroupAllocations--;
            }
            MOT_REPORT_ERROR(MOT_ERROR_OOM,
                "GC Reserve Limbo Group",
                "Failed to allocate %u bytes for limbo group",
                (unsigned)sizeof(LimboGroup));
            return false;
        }
    }

    if (reserveGroups) {
        m_reservedManager.ReserveGCMemory(currentHead, totalElements);
    } else {
        currentTail->m_next = m_limboTail->m_next;
        m_limboTail->m_next = currentHead;
    }

    return true;
}

bool GcQueue::ReserveGCMemory(uint32_t elements, bool reserveGroups)
{
    uint32_t limboGroups = 0;
    uint32_t freeElements = 0;
    uint64_t totalElements = elements;
    if (m_queueType == GC_QUEUE_TYPE::GENERIC_QUEUE) {
        totalElements *= MASSTREE_RESERVE;
    }

    if (reserveGroups) {
        freeElements = m_reservedManager.GetFreeAllocations();
        if (freeElements > totalElements) {
            m_reservedManager.ReserveGCMemory(nullptr, static_cast<uint32_t>(totalElements));
            return true;
        } else {
            totalElements -= freeElements;
            limboGroups = static_cast<uint32_t>(std::ceil(double(totalElements) / LimboGroup::CAPACITY));
        }
    } else {
        freeElements = GetFreeAllocations();
        if (freeElements < (totalElements + 2)) {
            limboGroups =
                static_cast<uint32_t>(std::ceil(double(totalElements + 2 - freeElements) / LimboGroup::CAPACITY));
        }
    }

    if (limboGroups == 0) {
        return true;
    }

    return AllocLimboGroups(limboGroups, static_cast<uint32_t>(totalElements), reserveGroups);
}

void GcQueue::HardQuiesce(uint64_t numOfElementsToClean)
{
    LimboGroup* emptyHead = nullptr;
    LimboGroup* emptyTail = nullptr;
    uint64_t count = numOfElementsToClean;
    uint64_t locCount = 0;

    MOT_ASSERT(GetPerformGcEpoch() > 0);
    GcEpochType epochBound = GetPerformGcEpoch() - 1;
    if (m_limboHead->m_head == m_limboHead->m_tail || GcSignedEpochType(epochBound - m_limboHead->FirstEpoch()) < 0) {
        goto done;
    }

    // clean [limbo_head_, limbo_tail_]
    while (count) {
        locCount = m_limboHead->CleanUntil(*this, epochBound, count);
        m_stats.m_totalLimboInuseElements -= (count - locCount);
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
    MOT_LOG_DEBUG(
        "Entity:%s cleaned items = %lu", enGcQueue[static_cast<uint8_t>(m_queueType)], numOfElementsToClean - count);
}

void GcQueue::RunQuiesce()
{
    MOT_LOG_DEBUG("Entity:%s min_csn = %lu performCSN = %lu, firstEpoch = %lu",
        enGcQueue[static_cast<uint8_t>(m_queueType)],
        g_gcActiveEpoch,
        m_performGcEpoch,
        m_limboHead->FirstEpoch());

    // Perform reclamation if possible
    HardQuiesce(m_stats.m_rcuFreeCount);

    // If we still exceed the high size limit, (e.g. we had a major gc addition in this txn run), clean all elements
    // (up to epoch limitation)
    if (m_stats.m_totalLimboSizeInBytes > m_stats.m_limboSizeLimitHigh) {
        HardQuiesce(m_stats.m_totalLimboInuseElements);
        MOT_LOG_DEBUG("Thread %d Entity:%s Reached high-threshold m_totalLimboSizeInBytes = %lu ",
            m_manager->GetThreadId(),
            enGcQueue[static_cast<uint8_t>(m_queueType)],
            m_stats.m_totalLimboSizeInBytes);
    }

    RegisterDeletedSentinels();
    ShrinkMem();
}

void GcQueue::RegisterDeletedSentinels()
{
    if (m_deleteVector->size() == 0) {
        return;
    }

    Sentinel* s = nullptr;
    uint64_t csn = GetGlobalEpoch();
    for (void* sentinel : *m_deleteVector) {
        s = static_cast<Sentinel*>(sentinel);
        Index* index = s->GetIndex();
        m_manager->GcRecordObject(GC_QUEUE_TYPE::GENERIC_QUEUE,
            index->GetIndexId(),
            s,
            nullptr,
            Index::SentinelDtor,
            SENTINEL_SIZE(index),
            csn);
    }

    m_deleteVector->clear();
}
uint32_t GcQueue::CleanIndexItems(uint32_t indexId, GC_OPERATION_TYPE oper)
{
    uint32_t counter = 0;
    LimboGroup* head = m_limboHead;
    LimboGroup* tail = m_limboTail;
    while (head != nullptr) {
        counter += head->CleanIndexItemPerGroup(*this, indexId, oper);
        head = head->m_next;
    }

    if (oper != GC_OPERATION_TYPE::GC_OPER_DROP_INDEX) {
        RegisterDeletedSentinels();
    }

    if (oper == GC_OPERATION_TYPE::GC_OPER_DROP_INDEX) {
        MOT_ASSERT(m_deleteVector->size() == 0);
    }

    // Update values from cleanIndexItemPerGroup flow
    if (m_stats.m_totalLimboSizeInBytesByCleanIndex) {
        m_stats.m_totalLimboInuseElements -= counter;
        m_stats.m_totalLimboSizeInBytes -= m_stats.m_totalLimboSizeInBytesByCleanIndex;
        m_stats.m_totalLimboReclaimedSizeInBytes += m_stats.m_totalLimboSizeInBytesByCleanIndex;
        m_stats.m_totalLimboSizeInBytesByCleanIndex = 0;
    }

    return counter;
}

void GcQueue::ShrinkMem()
{
    if (m_stats.m_limboGroupAllocations > LIMBO_GROUP_SHRINK_THRESHOLD) {
        LimboGroup* temp = m_limboTail->m_next;
        LimboGroup* next = nullptr;
        bool isCleaned = false;
        m_manager->ValidateAllocations();
        while (temp) {
            next = temp->m_next;
            m_manager->DestroyLimboGroup(temp);
            temp = next;
            MOT_ASSERT(m_stats.m_limboGroupAllocations > 0);
            m_stats.m_limboGroupAllocations--;
            isCleaned = true;
        }
        m_manager->ValidateAllocations();
        if (m_queueType == GC_QUEUE_TYPE::GENERIC_QUEUE and m_reservedManager.GetFreeAllocations()) {
            uint64_t deletedElements = m_manager->GetLimboInuseElementsByQueue(GC_QUEUE_TYPE::DELETE_QUEUE);
            if (!m_stats.m_totalLimboInuseElements and !deletedElements) {
                LimboGroup* head = m_reservedManager.AllocLimboGroup();
                while (head) {
                    m_manager->DestroyLimboGroup(head);
                    m_stats.m_limboGroupAllocations--;
                    head = m_reservedManager.AllocLimboGroup();
                }
                m_reservedManager.Clear();
            }
        }
        m_manager->ValidateAllocations();
        if (isCleaned) {
            m_limboTail->m_next = nullptr;
            m_manager->ClearLimboGroupCache();
        }
        m_manager->ValidateAllocations();
    }
}

}  // namespace MOT
