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
 *    Garbage-collector manager per-session.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/memory/garbage_collector/mm_gc_manager.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef MM_GC_MANAGER_H
#define MM_GC_MANAGER_H

#include "global.h"
#include "spin_lock.h"
#include "utilities.h"
#include "memory_statistics.h"
#include "mm_session_api.h"
#include "mm_gc_queue.h"
#include <vector>
#include "object_pool.h"

namespace MOT {
class GcManager;

extern volatile GcEpochType g_gcActiveEpoch;
extern GcLock g_gcGlobalEpochLock;

uint64_t GetGlobalEpoch();

uint64_t GetCurrentSnapshotCSN();

static const char* const enGcTypes[] = {
    stringify(GC_MAIN), stringify(GC_INDEX), stringify(GC_LOG), stringify(GC_CHECKPOINT)};

/**
 * @class GcManager
 * @brief Garbage-collector manager per-session
 */
class alignas(64) GcManager {
public:
    ~GcManager()
    {
        if (m_limboGroupPool) {
            ObjAllocInterface::FreeObjPool(&m_limboGroupPool);
        }
        m_limboGroupPool = nullptr;
        m_next = nullptr;
    }

    GcManager(const GcManager&) = delete;

    GcManager& operator=(const GcManager&) = delete;

    /** @var GC managers types   */
    enum class GC_TYPE : uint8_t { GC_MAIN, GC_INDEX, GC_RECOVERY, GC_CHECKPOINT };

    /** @var List of all GC Managers */
    static GcManager* allGcManagers;

    /** @brief Get next GC Manager in list
     *  @return Next GC Manager after the current one
     */
    GcManager* Next() const
    {
        return m_next;
    }

    /**
     * @brief Create a new instance of GC manager
     * @param purpose GC manager type
     * @param threadId Thread Identifier
     * @param rcuMaxFreeCount How many objects to reclaim
     * @return Pointer to instance of a GC manager
     */
    static GcManager* Make(GC_TYPE purpose, int threadId, bool global = false);
    /**
     * @brief Add Current manager to the global list
     */
    inline void AddToGcList()
    {
        g_gcGlobalEpochLock.lock();
        m_next = allGcManagers;
        allGcManagers = this;
        g_gcGlobalEpochLock.unlock();
    }

    /** @brief Reserver GC Memory */
    bool ReserveGCMemory(uint32_t elements);

    /** @brief Reserver GC Memory for specific queue */
    bool ReserveGCMemoryPerQueue(GC_QUEUE_TYPE queueId, uint32_t elements, bool reserveGroups = false);

    /** @brief Reserver GC Memory for transaction rollback */
    bool ReserveGCRollbackMemory(uint32_t elements);

    /** @brief allocate multiple limbo groups */
    LimboGroup* AllocLimboGroups(uint32_t limboGroups);

    /** @brief allocate limbo group */
    LimboGroup* CreateNewLimboGroup()
    {
        LimboGroup* group = m_limboGroupPool->Alloc<LimboGroup>();
        if (group == nullptr) {
            MOT_REPORT_ERROR(
                MOT_ERROR_OOM, "Create LimboGroup", "Failed to create new LimboGroup in Thread %d", GetThreadId());
        } else {
            m_limboGroupAllocations++;
        }
        return group;
    }

    /** @brief allocate limbo group from reserved pool */
    LimboGroup* AllocLimboGroupFromReserved()
    {
        return m_reservedManager.AllocLimboGroup();
    }

    /** @brief release limbo group */
    void DestroyLimboGroup(LimboGroup* obj)
    {
        m_limboGroupPool->Release<LimboGroup>(obj);
        m_limboGroupAllocations--;
    }

    void ClearLimboGroupCache()
    {
        if (m_global) {
            m_limboGroupPool->ClearThreadCache();
        } else {
            m_limboGroupPool->ClearFreeCache();
        }
    }
    /** @brief remove manager from global list   */
    void RemoveFromGcList(GcManager* ti);

    /** @brief Print report locally   */
    void ReportGcStats();

    /** @brief Print report of all threads   */
    static void ReportGcAll();

    void SetThreadId(uint16_t threadId)
    {
        m_tid = threadId;
    }

    uint16_t GetThreadId() const
    {
        return m_tid;
    }

    void SetGcType(GC_TYPE type)
    {
        m_purpose = type;
    }

    const char* GetGcTypeStr() const
    {
        return enGcTypes[static_cast<uint8_t>(m_purpose)];
    }

    GC_TYPE GetGcType() const
    {
        return m_purpose;
    }

    GcEpochType GcStartInnerTxn()
    {
        m_gcEpoch = GetGlobalEpoch();

        return m_gcEpoch;
    }

    void GcEndInnerTxn(bool clean_gc)
    {
        if (clean_gc) {
            RunQuicese();
        }
        m_gcEpoch = 0;
    }

    uint64_t GetCurrentEpoch() const
    {
        return m_gcEpoch;
    }

    bool IsGcSnapshotTaken() const
    {
        return m_isTxnSnapshotTaken;
    }

    /** @brief Start GC session   */
    RC GcStartTxn(uint64_t csn = 0)
    {
        RC rc = RC_OK;

        if (!m_isTxnSnapshotTaken) {
            m_isTxnSnapshotTaken = true;
            m_isGcSessionStarted = true;
            if (csn) {
                m_gcEpoch = csn;
            } else {
                // local guard the current epoch acquisition for correct minimum calculation
                m_managerLock.lock();
                if (GetGcType() == GC_TYPE::GC_CHECKPOINT || GetGcType() == GC_TYPE::GC_RECOVERY) {
                    m_gcEpoch = GetGlobalEpoch();
                } else {
                    m_gcEpoch = GetCurrentSnapshotCSN();
                }
                m_managerLock.unlock();
            }
            bool res = ReserveGCMemory(LIMBO_GROUP_INIT_SIZE * LimboGroup::CAPACITY);
            if (!res) {
                MOT_REPORT_ERROR(MOT_ERROR_OOM,
                    "Transaction Execution",
                    "GC Failed to refill %d elements  (while reallocating)",
                    LIMBO_GROUP_INIT_SIZE * LimboGroup::CAPACITY);
                SetLastError(MOT_ERROR_OOM, MOT_SEVERITY_ERROR);
                return RC::RC_MEMORY_ALLOCATION_ERROR;
            }
        }
        return rc;
    }

    /**
     * @brief Signal to the Gc manager that a transaction is re-started in terms of reclaimable memory usage
     */
    void GcReinitEpoch();

    /**
     * @brief Signal to the Gc manger that a statement in a read-only transaction has ended.
     * and remove yourself from the barrier
     */
    void GcEndStatement()
    {
        m_isTxnSnapshotTaken = false;
        m_gcEpoch = 0;
    }

    void GcRecoveryEndTxn()
    {
        m_isTxnSnapshotTaken = false;
        m_isGcSessionStarted = false;
        m_gcEpoch = 0;
    }

    /**
     * @brief Signal to the Gc manger that a transaction block has ended and try to reclaim objects
     */
    void GcEndTxn()
    {
        if (!m_isGcSessionStarted) {
            return;
        }
        ValidateAllocations();
        // Always lock before quiesce to allow drop-table/check-point operations
        if (m_managerLock.try_lock()) {
            RunQuicese();
            m_managerLock.unlock();
        }
        m_isTxnSnapshotTaken = false;
        m_isGcSessionStarted = false;
        m_gcEpoch = 0;
#ifdef GC_MASSTREE_DEBUG
        m_mastreeElements = 0;
#endif
        GcCleanRollbackMemory();
        ValidateAllocations();
    }

    /**
     * @brief Perform cleanup after the quiescent barrier
     *        1. Try to increase the global epoch
     *        2. Perform reclamation if possible
     *        3. Check hard-limit
     */
    void RunQuicese();

    /** @brief Clean all object at the end of the session */
    void GcCleanAll();

    /** @brief Clean all object at the end of the session */
    inline void GcCheckPointClean()
    {
        uint64_t inuseElements = 0;
        bool isBarrierReached = false;

        // Increase the global epoch to insure all elements are from a lower epoch
        SetActiveEpoch(isBarrierReached, GetThreadId());
        m_managerLock.lock();
        for (uint32_t queue = 0; queue < m_GcQueues.size(); ++queue) {
            m_GcQueues[queue].SetPerformGcEpoch(g_gcActiveEpoch);
            inuseElements += m_GcQueues[queue].m_stats.m_totalLimboInuseElements;
            m_GcQueues[queue].HardQuiesce(m_GcQueues[queue].m_stats.m_totalLimboInuseElements);
            m_GcQueues[queue].RegisterDeletedSentinels();
        }
        GcMaintenance();
        m_managerLock.unlock();

#ifdef MOT_DEBUG
        uint64_t cleandObjects = inuseElements - GetTotalLimboInuseElements();
        if (cleandObjects) {
            MOT_LOG_DEBUG("Entity:%s THD_ID:%u cleaned %lu elements from limbo!",
                enGcTypes[static_cast<uint8_t>(m_purpose)],
                m_tid,
                cleandObjects);
        }
#endif
    }

    /**
     * @brief Records a new object and push it in the limbo-group
     * @param indexId Index identifier
     * @param objectPtr Pointer of the object
     * @param objectPool Memory pool (optional)
     * @param cb Callback function
     * @param objSize Size of the object
     */
    void GcRecordObject(GC_QUEUE_TYPE m_type, uint32_t indexId, void* objectPtr, void* objectPool,
        DestroyValueCbFunc cb, uint32_t objSize, GcEpochType csn = 0)
    {
        uint64_t epoch = csn;
        if (!csn) {
            epoch = GetGlobalEpoch();
        }

        m_GcQueues[static_cast<uint8_t>(m_type)].PushBack(indexId, objectPtr, objectPool, cb, objSize, epoch);
        MemoryStatisticsProvider::GetInstance().AddGCRetiredBytes(objSize);
    }

    /** @brief Try to upgrade the global minimum or let other thread do it   */
    void SetActiveEpoch(bool& isBarrierReached, const uint16_t tid)
    {
        isBarrierReached = false;
        uint32_t activeConnections = 0;
        uint16_t latestActiveTid = static_cast<uint16_t>(-1);
        bool rc = g_gcGlobalEpochLock.try_lock();
        if (rc) {
            g_gcActiveEpoch = GcManager::MinActiveEpoch(activeConnections, latestActiveTid);
            if (activeConnections <= 1) {
                // Either no active connections or i am seeing myself
                if (activeConnections == 0 or latestActiveTid == tid) {
                    // Self view
                    isBarrierReached = true;
                }
            }
            g_gcGlobalEpochLock.unlock();
        }
    }

    /** @brief API for global index cleanup - used by vacuum/drop table
     *  @param indexId Index identifier
     *  @return True for success
     */
    static inline void ClearIndexElements(
        uint32_t indexId, GC_OPERATION_TYPE oper = GC_OPERATION_TYPE::GC_OPER_DROP_INDEX);

    uint64_t inline GetTotalLimboInuseElements() const
    {
        uint64_t totalLimboInuseElements = 0;
        for (uint32_t queue = 0; queue < m_GcQueues.size(); ++queue) {
            totalLimboInuseElements += m_GcQueues[queue].m_stats.m_totalLimboInuseElements;
        }
        return totalLimboInuseElements;
    }

    uint64_t inline GetLimboInuseElementsByQueue(GC_QUEUE_TYPE queue) const
    {
        return m_GcQueues[static_cast<uint8_t>(queue)].m_stats.m_totalLimboInuseElements;
    }

    uint32_t inline GetLimboGroupAllocations() const
    {
        uint32_t limboGroupAllocations = 0;
        for (uint32_t queue = 0; queue < m_GcQueues.size(); ++queue) {
            limboGroupAllocations += m_GcQueues[queue].m_stats.m_limboGroupAllocations;
        }
        return limboGroupAllocations;
    }

    void inline ValidateAllocations() const
    {
        MOT_ASSERT(m_limboGroupAllocations - m_reservedManager.GetLimboAlloctions() == GetLimboGroupAllocations());
    }

    uint32_t GetFreeAllocations() const
    {
        uint32_t freeAllocations = 0;
        for (uint32_t queue = 0; queue < m_GcQueues.size(); ++queue) {
            freeAllocations += m_GcQueues[queue].GetFreeAllocations();
        }
        return freeAllocations;
    }

    uint32_t GetFreeAllocationsPerQueue(GC_QUEUE_TYPE queueId) const
    {
        return m_GcQueues[static_cast<uint8_t>(queueId)].GetFreeAllocations();
    }

    uint64_t inline GetTotalLimboReclaimedSizeInBytes()
    {
        uint64_t totalLimboReclaimedSizeInBytes = 0;
        for (uint32_t queue = 0; queue < m_GcQueues.size(); ++queue) {
            totalLimboReclaimedSizeInBytes += m_GcQueues[queue].m_stats.m_totalLimboReclaimedSizeInBytes;
        }
        return totalLimboReclaimedSizeInBytes;
    }

    uint64_t inline GetTotalLimboRetiredSizeInBytes()
    {
        uint64_t totalLimboRetiredSizeInBytes = 0;
        for (uint32_t queue = 0; queue < m_GcQueues.size(); ++queue) {
            totalLimboRetiredSizeInBytes += m_GcQueues[queue].m_stats.m_totalLimboRetiredSizeInBytes;
        }
        return totalLimboRetiredSizeInBytes;
    }

    inline void GcMaintenance()
    {
        for (uint32_t queue = 0; queue < m_GcQueues.size(); ++queue) {
            m_GcQueues[queue].Maintenance();
        }
    }

    void GcCleanRollbackMemory()
    {
        uint32_t allocations = m_reservedManager.GetFreeAllocations();
        if (allocations) {
            LimboGroup* head = m_reservedManager.AllocLimboGroup();
            while (head) {
                DestroyLimboGroup(head);
                head = m_reservedManager.AllocLimboGroup();
            }
            m_reservedManager.Clear();
            if (allocations > GcQueue::LIMBO_GROUP_SHRINK_THRESHOLD) {
                ClearLimboGroupCache();
            }
        }
    }

    static constexpr int LIMBO_GROUP_INIT_SIZE = 4;
    static constexpr int64_t INVALID_EPOCH = -1;
    static constexpr uint16_t GC_CLEAN_SLEEP = 20 * 1000;
    static constexpr uint16_t GC_CLEAN_SESSION_SLEEP = 50;

private:
    /** @var Vector of priority queues   */
    std::vector<GcQueue> m_GcQueues;

    /** @var Current snapshot of the global epoch   */
    GcEpochType m_gcEpoch;

    /** @var Manager local row   */
    GcLock m_managerLock;

    /** @var Limbo group memory pool */
    ObjAllocInterface* m_limboGroupPool = nullptr;

    GcReservedMemoryPool m_reservedManager;

    /** @var Next manager in the global list */
    GcManager* m_next = nullptr;

    /** @var Number of allocations   */
    uint32_t m_limboGroupAllocations;

    /** @var Thread identification   */
    uint16_t m_tid;

    /** @var Flag to signal if we took a snapshot  */
    bool m_isTxnSnapshotTaken = false;

    /** @var Flag to signal if we started a transaction   */
    bool m_isGcSessionStarted = false;

    /** @var Flag to signal if we reached a threshold   */
    bool m_isThresholdReached = false;

    /** @var GC manager type   */
    GC_TYPE m_purpose;

    bool m_global;

    /** @brief Calculate the minimum epoch among all active GC Managers.
     *  @return The minimum epoch among all active GC Managers.
     */
    static inline GcEpochType MinActiveEpoch(uint32_t& activeConnections, uint16_t& tid);

    /** @brief Constructor   */
    inline GcManager(GC_TYPE purpose, int threadId, bool global = false);

    /** @brief Initialize GC Manager's structures.
     *  @return True for success
     */
    bool Initialize();

    /** @brief Remove all elements of elements of a specific index from all Limbo groups and reclaim them */
    void CleanIndexItems(uint32_t indexId, GC_OPERATION_TYPE oper);

    DECLARE_CLASS_LOGGER()
};

inline GcEpochType GcManager::MinActiveEpoch(uint32_t& activeConnections, uint16_t& tid)
{
    GcEpochType minimalEpoch = static_cast<GcEpochType>(INVALID_EPOCH);
    activeConnections = 0;
    for (GcManager* ti = allGcManagers; ti; ti = ti->Next()) {
        Prefetch((const void*)ti->Next());
        GcEpochType currentEpoch = ti->m_gcEpoch;
        if (currentEpoch > 0) {
            activeConnections++;
            tid = ti->GetThreadId();
            if (currentEpoch < minimalEpoch) {
                minimalEpoch = currentEpoch;
            }
        }
    }

    // If all sessions are closed the min_epoch is the current clock
    // We can declare that we passed a barrier
    if (GcSignedEpochType(minimalEpoch) == INVALID_EPOCH) {
        return GetGlobalEpoch();
    }

    return minimalEpoch;
}

inline void GcManager::ClearIndexElements(uint32_t indexId, GC_OPERATION_TYPE oper)
{
    g_gcGlobalEpochLock.lock();
    for (GcManager* gcManager = allGcManagers; gcManager; gcManager = gcManager->Next()) {
        Prefetch((const void*)gcManager->Next());
        gcManager->CleanIndexItems(indexId, oper);
    }
    g_gcGlobalEpochLock.unlock();
}
}  // namespace MOT
#endif /* MM_GC_MANAGER */
