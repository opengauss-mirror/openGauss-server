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
#include "object_pool.h"

namespace MOT {
class GcManager;

typedef uint64_t GcEpochType;
typedef int64_t GcSignedEpochType;
typedef MOT::spin_lock GcLock;

/** @def GC callback function signature   */
typedef uint32_t (*DestroyValueCbFunc)(void*, void*, bool);
extern volatile GcEpochType g_gcGlobalEpoch;
extern volatile GcEpochType g_gcActiveEpoch;
extern GcLock g_gcGlobalEpochLock;

inline uint64_t GetGlobalEpoch()
{
    return g_gcGlobalEpoch;
}

/**
 * @struct LimboGroup
 * @brief Contains capacity elements and handle push/pop operations
 */
struct LimboGroup {
    typedef GcEpochType EpochType;
    typedef GcSignedEpochType SignedEpochType;

    /**
     * @struct LimboElement
     * @brief Contains all the necessary members to reclaim the object
     */
    struct LimboElement {
        void* m_objectPtr;

        void* m_objectPool;

        DestroyValueCbFunc m_cb;

        EpochType m_epoch;

        uint32_t m_indexId;
    };

    static_assert(sizeof(LimboElement) > (sizeof(LimboGroup*) + sizeof(EpochType) + 2 * sizeof(unsigned)),
        "ERROR:Please Modify group size!");

    static constexpr uint32_t GROUP_SIZE = 8 * KILO_BYTE - sizeof(LimboElement);

    /** Calculate the capacity of the group for minimal memory overhead (S-1/S)
     * Where a single element is at least larger from the rest of the Struct members
     */
    static constexpr uint32_t CAPACITY = GROUP_SIZE / sizeof(LimboElement);

    unsigned m_head;

    unsigned m_tail;

    EpochType m_epoch;

    LimboGroup* m_next;

    LimboElement m_elements[CAPACITY];

    LimboGroup() : m_head(0), m_tail(0), m_next()
    {}

    EpochType FirstEpoch() const
    {
        MOT_ASSERT(m_head != m_tail);
        return m_elements[m_head].m_epoch;
    }

    /**
     * @brief Push an element to the list, if the epoch is new create a new dummy element
     * @param indexId Element index-id
     * @param objectPtr Pointer to reclaim
     * @param objectPool Memory pool (optional)
     * @param cb Callback function
     * @param epoch Recorded epoch
     */
    void PushBack(uint32_t indexId, void* objectPtr, void* objectPool, DestroyValueCbFunc cb, GcEpochType epoch)
    {
        MOT_ASSERT(m_tail + 2 <= CAPACITY);
        if (m_head == m_tail || m_epoch != epoch) {
            m_elements[m_tail].m_objectPtr = nullptr;
            m_elements[m_tail].m_epoch = epoch;
            m_epoch = epoch;
            ++m_tail;
        }
        m_elements[m_tail].m_indexId = indexId;
        m_elements[m_tail].m_objectPtr = objectPtr;
        m_elements[m_tail].m_objectPool = objectPool;
        m_elements[m_tail].m_cb = cb;
        ++m_tail;
    }

    /** @brief Clean all element until epochBound
     *  @param ti GcManager object to clean
     *  @param epochBound Epoch boundary to limit cleanup
     *  @param count Max items to clean
     */
    inline unsigned CleanUntil(GcManager& ti, GcEpochType epochBound, unsigned count);

    /**
     * @brief Clean All elements from the current GC manager tagged with index_id
     * @param ti GcManager object to clean
     * @param indexId Index Identifier to clean
     * @return Number of elements cleaned
     */
    inline unsigned CleanIndexItemPerGroup(GcManager& ti, uint32_t indexId, bool dropIndex);
};

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
        LimboGroup* temp = m_limboHead;
        LimboGroup* next = nullptr;
        while (temp) {
            next = temp->m_next;
            DestroyLimboGroup(temp);
            temp = next;
        }

        if (m_limboGroupPool) {
            ObjAllocInterface::FreeObjPool(&m_limboGroupPool);
        }
        m_limboGroupPool = nullptr;
    }

    GcManager(const GcManager&) = delete;

    GcManager& operator=(const GcManager&) = delete;

    /** @var GC managers types   */
    enum GC_TYPE : uint8_t { GC_MAIN, GC_INDEX, GC_LOG, GC_CHECKPOINT };

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
    static GcManager* Make(GC_TYPE purpose, int threadId, int rcuMaxFreeCount = MASSTREE_OBJECT_COUNT_PER_CLEANUP);

    /**
     * @brief Add Current manager to the global list
     */
    inline void AddToGcList()
    {
        if (m_isGcEnabled == false) {
            return;
        }
        g_gcGlobalEpochLock.lock();
        m_next = allGcManagers;
        allGcManagers = this;
        g_gcGlobalEpochLock.unlock();
    }

    /** @brief remove manager from global list   */
    void RemoveFromGcList(GcManager* ti);

    /** @brief Print report locally   */
    void ReportGcStats();

    /** @brief Print report of all threads   */
    static void ReportGcAll();

    int GetThreadId() const
    {
        return m_tid;
    }

    void SetGcType(GC_TYPE type)
    {
        m_purpose = type;
    }

    const char* GetGcTypeStr()
    {
        return enGcTypes[m_purpose];
    }

    GC_TYPE GetGcType()
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

    void GcStartTxn()
    {

        if (m_isGcEnabled == true and m_isTxnStarted == false) {
            m_isTxnStarted = true;
            if (m_gcEpoch != GetGlobalEpoch())
                m_gcEpoch = GetGlobalEpoch();
        }
    }

    /**
     * @brief Signal to the Gc manger that a transaction block has ended and try to reclaim objects
     */
    void GcEndTxn()
    {
        if (m_isGcEnabled == false || m_isTxnStarted == false) {
            return;
        }
        // Always lock before quicese to allow drop-table/check-point operations
        if (m_managerLock.try_lock()) {
            RunQuicese();
            m_managerLock.unlock();
        }
        m_gcEpoch = 0;
        m_isTxnStarted = false;
    }

    /**
     * @brief Perform cleanup after the quiescent barrier
     *        1. Try to increase the global epoch
     *        2. Perform reclamation if possible
     *        3. Check hard-limit
     */
    void RunQuicese()
    {
        // Update values from cleanIndexItemPerGroup flow
        if (m_totalLimboSizeInBytesByCleanIndex) {
            m_totalLimboSizeInBytes -= m_totalLimboSizeInBytesByCleanIndex;
            m_totalLimboReclaimedSizeInBytes += m_totalLimboSizeInBytesByCleanIndex;
            m_totalLimboSizeInBytesByCleanIndex = 0;
        }

        // Increase Local epoch when the threashold is reached
        if (m_totalLimboSizeInBytes > m_limboSizeLimit) {
            m_gcEpoch++;
        }
        // if local epoch is greater then global set the global and calculate minimum
        if (m_gcEpoch > g_gcGlobalEpoch) {
            SetGlobalEpoch(m_gcEpoch);
        }
        // Perform reclamation if possible
        Quiesce();

        // If we still exceed the high size limit, (e.g. we had a major gc addition in this txn run), clean all elements
        // (up to epoch limitation)
        if (m_totalLimboSizeInBytes > m_limboSizeLimitHigh) {
            HardQuiesce(m_totalLimboInuseElements);
            ShrinkMem();
        }
    }

    inline void Quiesce()
    {
        if (m_performGcEpoch != g_gcActiveEpoch)
            HardQuiesce(m_rcuFreeCount);
    }

    /** @brief Clean all object at the end of the session */
    inline void GcCleanAll()
    {
        if (m_isGcEnabled == false) {
            return;
        }
        uint32_t inuseElements = m_totalLimboInuseElements;
        // Increase the global epoch to insure all elements are from a lower epoch
        while (m_totalLimboSizeInBytes > 0) {
            SetGlobalEpoch(GetGlobalEpoch() + 1);
            m_managerLock.lock();
            HardQuiesce(m_totalLimboInuseElements);
            m_managerLock.unlock();
            // every 200ms
            usleep(200 * 1000);
        }
        m_managerLock.lock();
        ShrinkMem();
        m_managerLock.unlock();
        MOT_LOG_DEBUG("Entity:%s THD_ID:%d closed session cleaned %d elements from limbo!\n",
            enGcTypes[m_purpose],
            m_tid,
            inuseElements);
    }

    /** @brief Clean all object at the end of the session */
    inline void GcCheckPointClean()
    {
        if (m_isGcEnabled == false) {
            return;
        }

        // End CP Txn
        m_gcEpoch = 0;
        m_isTxnStarted = false;

        // Increase the global epoch to insure all elements are from a lower epoch
        SetGlobalEpoch(GetGlobalEpoch() + 1);
        m_managerLock.lock();
        HardQuiesce(m_totalLimboInuseElements);
        m_managerLock.unlock();

#ifdef MOT_DEBUG
        uint32_t inuseElements = m_totalLimboInuseElements;
        uint32_t cleandObjects = inuseElements - m_totalLimboInuseElements;
        if (cleandObjects) {
            MOT_LOG_INFO(
                "Entity:%s THD_ID:%d cleaned %d elements from limbo!\n", enGcTypes[m_purpose], m_tid, (cleandObjects));
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
    void GcRecordObject(uint32_t indexId, void* objectPtr, void* objectPool, DestroyValueCbFunc cb, uint32_t objSize)
    {
        if (m_isGcEnabled == false) {
            return;
        }
        if (m_limboTail->m_tail + 2 > LimboGroup::CAPACITY) {
            bool res = RefillLimboGroup();
            if (res == false) {
                MOT_REPORT_ERROR(MOT_ERROR_OOM, "GC Operation", "Failed to refill limbo group");
                return;
            }
        }
        uint64_t epoch = GetGlobalEpoch();
        m_limboTail->PushBack(indexId, objectPtr, objectPool, cb, epoch);
        ++m_totalLimboInuseElements;
        m_totalLimboSizeInBytes += objSize;
        m_totalLimboRetiredSizeInBytes += objSize;  // stats
        MemoryStatisticsProvider::m_provider->AddGCRetiredBytes(objSize);
    }

    /** @brief Try to upgrade the global epoch or let other thread do it */
    void SetGlobalEpoch(GcEpochType e)
    {
        bool rc = g_gcGlobalEpochLock.try_lock();
        if (rc == true) {
            if (GcSignedEpochType(e - g_gcGlobalEpoch) > 0) {
                g_gcGlobalEpoch = e;
                g_gcActiveEpoch = GcManager::MinActiveEpoch();
            }
            g_gcGlobalEpochLock.unlock();
        }
    }

    /** @brief realloc limbo group */
    bool RefillLimboGroup();

    /** @brief allocate limbo group */
    LimboGroup* CreateNewLimboGroup()
    {
        LimboGroup* group = m_limboGroupPool->Alloc<LimboGroup>();
        if (group == nullptr) {
            MOT_REPORT_ERROR(
                MOT_ERROR_OOM, "Create LimboGroup", "Failed to create new LimboGroup in Thread %d", GetThreadId());
        }
        return group;
    }

    /** @brief release limbo group */
    void DestroyLimboGroup(LimboGroup* obj)
    {
        m_limboGroupPool->Release<LimboGroup>(obj);
    }

    /** @brief shrink limbogroups memory */
    inline void ShrinkMem()
    {
        if (m_limboGroupAllocations > 1) {

            LimboGroup* temp = m_limboTail->m_next;
            LimboGroup* next = nullptr;
            while (temp) {
                next = temp->m_next;
                DestroyLimboGroup(temp);
                temp = next;
                m_limboGroupAllocations--;
            }
            m_limboTail->m_next = nullptr;
        }
    }

    /** @brief null destructor callback function
     *  @param buf Ignored
     *  @param buf2 Ignored
     *  @param dropIndex Ignored
     *  @return 0 (for success)
     */
    static uint32_t NullDtor(void* buf, void* buf2, bool dropIndex)
    {
        return 0;
    }

    /** @brief API for global index cleanup - used by vacuum/drop table
     *  @param indexId Index identifier
     *  @return True for success
     */
    static inline bool ClearIndexElements(uint32_t indexId, bool dropIndex = true);

    int GetFreeCount() const
    {
        return m_rcuFreeCount;
    }

private:
    /** @var Current snapshot of the global epoch   */
    GcEpochType m_gcEpoch;

    /** @var Calculated perform epoch   */
    GcEpochType m_performGcEpoch;

    /** @var Limbo group HEAD   */
    LimboGroup* m_limboHead = nullptr;

    /** @var Limbo group TAIL   */
    LimboGroup* m_limboTail = nullptr;

    /** @var Limbo group memory pool */
    ObjAllocInterface* m_limboGroupPool = nullptr;

    /** @var Next manager in the global list */
    GcManager* m_next = nullptr;

    /** @var Manager local row   */
    GcLock m_managerLock;

    /** @var Flag for feature availability   */
    bool m_isGcEnabled = false;

    /** @var Flag to signal if we started a transaction   */
    bool m_isTxnStarted = false;

    /** @var RCU free count   */
    uint16_t m_rcuFreeCount;

    /** @var Total limbo elements in use */
    uint32_t m_totalLimboInuseElements;

    /**@var Total size of limbo in bytes   */
    uint32_t m_totalLimboSizeInBytes;

    /** @var Total clean object by clean index in bytes   */
    uint32_t m_totalLimboSizeInBytesByCleanIndex;

    /** @var Total reclaimed objects in bytes   */
    uint32_t m_totalLimboReclaimedSizeInBytes;

    /** @var Total retired object in bytes   */
    uint32_t m_totalLimboRetiredSizeInBytes;

    /** @var Limbo size limit   */
    uint32_t m_limboSizeLimit;

    /** @var Limbo size hard limit   */
    uint32_t m_limboSizeLimitHigh;

    /** @var Number of allocations   */
    uint16_t m_limboGroupAllocations;

    /** @var Thread identification   */
    uint16_t m_tid;

    /** @var GC manager type   */
    GC_TYPE m_purpose;

    /** @brief Calculate the minimum epoch among all active GC Managers.
     *  @return The minimum epoch among all active GC Managers.
     */
    static inline GcEpochType MinActiveEpoch();

    inline uint32_t GetOccupiedElements() const
    {
        return m_totalLimboInuseElements;
    }

    /** @brief Constructor   */
    inline GcManager(GC_TYPE purpose, int index, int rcuMaxFreeCount);

    /** @brief Initialize GC Manager's structures.
     *  @return True for success
     */
    bool Initialize();

    /** @brief Clean\reclaim elements from Limbo groups   */
    void HardQuiesce(uint32_t numOfElementsToClean);

    /** @brief Remove all elements of elements of a specific index from all Limbo groups and reclaim them */
    void CleanIndexItems(uint32_t indexId, bool dropIndex);
    friend struct LimboGroup;

    DECLARE_CLASS_LOGGER()
};

inline GcEpochType GcManager::MinActiveEpoch()
{
    GcEpochType ae = g_gcGlobalEpoch;
    for (GcManager* ti = allGcManagers; ti; ti = ti->Next()) {
        Prefetch((const void*)ti->Next());
        GcEpochType te = ti->m_gcEpoch;
        if (te && (GcSignedEpochType(te - ae) < 0))
            ae = te;
    }
    return ae;
}

inline bool GcManager::ClearIndexElements(uint32_t indexId, bool dropIndex)
{
    g_gcGlobalEpochLock.lock();
    for (GcManager* gcManager = allGcManagers; gcManager; gcManager = gcManager->Next()) {
        Prefetch((const void*)gcManager->Next());
        gcManager->CleanIndexItems(indexId, dropIndex);
    }
    g_gcGlobalEpochLock.unlock();
    return true;
}
}  // namespace MOT
#endif /* MM_GC_MANAGER */
