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
 *    src/gausskernel/storage/mot/core/memory/garbage_collector/mm_gc_queue.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef MM_GC_QUEUE_H
#define MM_GC_QUEUE_H

#include "global.h"
#include "spin_lock.h"
#include "utilities.h"
#include "memory_statistics.h"
#include "mm_session_api.h"
#include <vector>
#include <cmath>
#include <list>

namespace MOT {

class GcManager;
class GcQueue;

using GcEpochType = uint64_t;
using GcSignedEpochType = int64_t;
using GcLock = MOT::spin_lock;

/** @def GC callback function signature   */
typedef uint32_t (*DestroyValueCbFunc)(void*, void*, void*);

/** @def GC epoch type   */
using EpochType = GcEpochType;

/** @def GC signed epoch type   */
using SignedEpochType = GcSignedEpochType;

/** @def GC callback function signature   */
enum class GC_QUEUE_TYPE : uint8_t { DELETE_QUEUE, VERSION_QUEUE, UPDATE_COLUMN_QUEUE, GENERIC_QUEUE, GC_QUEUES };

/** @def GC operation type   */
enum class GC_OPERATION_TYPE : uint8_t { GC_OPER_LOCAL, GC_OPER_DROP_INDEX, GC_OPER_TRUNCATE, GC_ALTER_TABLE };

/** @def GC RC from RunQuicese operation   */
enum class GC_QUEUE_RC : uint8_t { RC_QUEUE_NONE, RC_QUEUE_PARTIAL, RC_QUEUE_FULL, RC_QUEUE_HIGH_LIMIT };

/**
 * @struct GcQueueEntry
 * @brief Contains all the necessary members to describe the queue
 */
struct GcQueueEntry {
    GC_QUEUE_TYPE m_type;
    uint32_t m_limboSizeLimit;
    uint32_t m_limboSizeLimitHigh;
    uint32_t m_rcuFreeCount;
};

/**
 * @struct LimboElement
 * @brief Contains all the necessary members to reclaim the object
 */
struct PACKED LimboElement {
    void* m_objectPtr;

    void* m_objectPool;

    DestroyValueCbFunc m_cb;

    EpochType m_csn;

    uint32_t m_indexId;
};

/**
 * @struct LimboGroup
 * @brief Contains capacity elements and handle push/pop operations
 */
struct LimboGroup {
    static constexpr uint16_t LIMBO_PAGE_SIZE = 4096 - (sizeof(EpochType) + sizeof(LimboGroup*) + 2 * sizeof(unsigned));

    static constexpr uint32_t CAPACITY = (LIMBO_PAGE_SIZE) / sizeof(LimboElement);

    EpochType m_epoch;

    LimboGroup* m_next = nullptr;

    unsigned m_head = 0;

    unsigned m_tail = 0;

    LimboElement m_elements[CAPACITY];

    LimboGroup() : m_epoch(0), m_next(nullptr), m_head(0), m_tail(0)
    {}

    EpochType FirstEpoch() const
    {
        MOT_ASSERT(m_head != m_tail);
        return m_elements[m_head].m_csn;
    }

    void Reset()
    {
        m_head = 0;
        m_tail = 0;
        m_next = nullptr;
    }

    bool isGroupFull() const
    {
        return (m_tail + 1 == CAPACITY);
    }

    uint32_t GetListLength();

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
            m_elements[m_tail].m_csn = epoch;
            m_epoch = epoch;
            ++m_tail;
        }
        m_elements[m_tail].m_indexId = indexId;
        m_elements[m_tail].m_objectPtr = objectPtr;
        m_elements[m_tail].m_csn = epoch;
        m_elements[m_tail].m_objectPool = objectPool;
        m_elements[m_tail].m_cb = cb;
        ++m_tail;
    }

    /** @brief Clean all element until epochBound
     *  @param ti GcManager object to clean
     *  @param epochBound Epoch boundary to limit cleanup
     *  @param count Max items to clean
     */
    inline uint64_t CleanUntil(GcQueue& ti, GcEpochType epochBound, uint64_t count);

    /**
     * @brief Clean All elements from the current GC manager tagged with index_id
     * @param ti GcManager object to clean
     * @param indexId Index Identifier to clean
     * @return Number of elements cleaned
     */
    inline unsigned CleanIndexItemPerGroup(GcQueue& ti, uint32_t indexId, GC_OPERATION_TYPE oper);
};

class GcReservedMemoryPool {
public:
    using ReservedList = std::list<LimboGroup*>;
    GcReservedMemoryPool() : isInitialized(false), m_limboReservedMemory(nullptr)
    {}

    ~GcReservedMemoryPool()
    {
        if (isInitialized) {
            delete m_limboReservedMemory;
            isInitialized = false;
        }

        m_limboReservedMemory = nullptr;
    }

    bool initialize()
    {
        if (!isInitialized) {
            m_limboReservedMemory = new (std::nothrow) ReservedList();
            if (m_limboReservedMemory == nullptr) {
                return false;
            }
            isInitialized = true;
        }
        return true;
    }

    void ReserveGCMemory(LimboGroup* list, uint32_t count = 0)
    {
        LimboGroup* tmp = nullptr;
        MOT_ASSERT(m_limboReservedMemory != nullptr);
        if (list) {
            while (list) {
                tmp = list;
                list = list->m_next;
                tmp->Reset();
                MOT_ASSERT(tmp->m_head == 0 && tmp->m_tail == 0);
                m_limboReservedMemory->push_back(tmp);
            }
        }
        registeredElements += count;
        groupAllocations += static_cast<uint32_t>(std::ceil(double(count) / LimboGroup::CAPACITY));
    }

    uint32_t GetFreeAllocations() const
    {
        return groupAllocations * LimboGroup::CAPACITY - registeredElements;
    }

    uint32_t GetLimboAlloctions() const
    {
        return groupAllocations;
    }

    LimboGroup* AllocLimboGroup()
    {
        LimboGroup* e = nullptr;
        if (!m_limboReservedMemory->empty()) {
            e = m_limboReservedMemory->front();
            m_limboReservedMemory->pop_front();
            groupAllocations--;
            if (registeredElements > LimboGroup::CAPACITY) {
                registeredElements -= LimboGroup::CAPACITY;
            } else {
                registeredElements = 0;
            }
        }
        return e;
    }

    void Clear()
    {
        MOT_ASSERT(m_limboReservedMemory->size() == 0);
        groupAllocations = 0;
        registeredElements = 0;
    }

private:
    bool isInitialized = false;
    uint32_t groupAllocations = 0;
    uint32_t registeredElements = 0;
    ReservedList* m_limboReservedMemory = nullptr;
};

class GcQueue {
public:
    using DeleteVector = std::vector<void*>;

    struct GcStats {
        /** @var Total limbo elements in use */
        uint64_t m_totalLimboInuseElements;

        /**@var Total size of limbo in bytes */
        uint64_t m_totalLimboSizeInBytes;

        /** @var Total clean object by clean index in bytes */
        uint64_t m_totalLimboSizeInBytesByCleanIndex;

        /** @var Total reclaimed objects in bytes */
        uint64_t m_totalLimboReclaimedSizeInBytes;

        /** @var Total retired object in bytes */
        uint64_t m_totalLimboRetiredSizeInBytes;

        /** @var Limbo size limit */
        uint32_t m_limboSizeLimit;

        /** @var Limbo size hard limit */
        uint32_t m_limboSizeLimitHigh;

        /** @var Number of allocations */
        uint32_t m_limboGroupAllocations;

        /** @var RCU free count */
        uint32_t m_rcuFreeCount;
    };

    explicit GcQueue(const GcQueueEntry& entry);
    ~GcQueue();

    /** @brief Initialize the GC Queue */
    bool Initialize(GcManager* manager);

    /** @brief Refill GC Queue */
    bool RefillLimboGroup();

    /** @brief Reserve GC Memory */
    bool ReserveGCMemory(uint32_t elements, bool reserveGroups = false);

    bool AllocLimboGroups(uint32_t limboGroups, uint32_t totalElements, bool reserveGroups);

    /** @brief Clean\reclaim elements from Limbo groups if possible
     *   @return Queue size compared to threshold
     */
    void RunQuiesce();

    /** @brief Clean\reclaim elements from Limbo groups
     *   @return Queue size compared to threshold
     */
    void HardQuiesce(uint64_t numOfElementsToClean);

    bool IsThresholdReached() const
    {
        if (m_stats.m_totalLimboSizeInBytes > m_stats.m_limboSizeLimit) {
            return true;
        }
        return false;
    }

    inline EpochType FirstEpoch() const
    {
        return m_limboHead->FirstEpoch();
    }

    /** @brief Push new element to the GC Queue   */
    inline void PushBack(
        uint32_t indexId, void* objectPtr, void* objectPool, DestroyValueCbFunc cb, uint32_t objSize, GcEpochType csn)
    {
        if (m_limboTail->m_tail + 2 > LimboGroup::CAPACITY) {
            bool res = RefillLimboGroup();
            if (!res) {
                MOT_REPORT_ERROR(MOT_ERROR_OOM, "GC Operation", "Failed to refill limbo group");
                return;
            }
        }
        m_limboTail->PushBack(indexId, objectPtr, objectPool, cb, csn);
        ++m_stats.m_totalLimboInuseElements;
        m_stats.m_totalLimboSizeInBytes += objSize;
        m_stats.m_totalLimboRetiredSizeInBytes += objSize;  // stats
    }

    /** @brief null destructor callback function
     *  @param buf Ignored
     *  @param buf2 Ignored
     *  @param dropIndex Ignored
     *  @return 0 (for success)
     */
    static uint32_t NullDtor(void* buf, void* dropIndex, void* aux)
    {
        return 0;
    }

    /** @brief Perform memory maintenance on Queue */
    void Maintenance()
    {
        ShrinkMem();
    }

    /** @brief Register all the deleted elements to the generic queue priority 3 */
    void RegisterDeletedSentinels();

    /** @brief Clean all items for current index id
     *  @param indexId
     *  @param oper type of GC operation to perform
     *  @return # of cleaned elements
     */
    uint32_t CleanIndexItems(uint32_t indexId, GC_OPERATION_TYPE oper);

    DeleteVector* GetDeleteVector()
    {
        return m_deleteVector;
    }

    GcEpochType GetPerformGcEpoch() const
    {
        return m_performGcEpoch;
    }

    void SetPerformGcEpoch(GcEpochType e)
    {
        m_performGcEpoch = e;
    }

    uint32_t GetFreeAllocations() const
    {
        uint32_t freeAllocations = (LimboGroup::CAPACITY - m_limboTail->m_tail);
        LimboGroup* next = m_limboTail->m_next;
        while (next) {
            freeAllocations += LimboGroup::CAPACITY;
            next = next->m_next;
        }
        return freeAllocations;
    }

    /** @var Queue statistics */
    GcStats m_stats = {0};

    static constexpr uint32_t LIMBO_GROUP_SHRINK_THRESHOLD = 10;

    static constexpr uint32_t MASSTREE_RESERVE = 2;

    static constexpr uint32_t RECOVERY_LIMBO_SIZE_LIMIT = 4096;

    static const char* const enGcQueue[];

private:
    /** @var Calculated perform epoch   */
    GcEpochType m_performGcEpoch = 0;

    /** @var Limbo group HEAD   */
    LimboGroup* m_limboHead = nullptr;

    /** @var Limbo group TAIL   */
    LimboGroup* m_limboTail = nullptr;

    /** @var Limbo group TAIL   */
    DeleteVector* m_deleteVector = nullptr;

    /** @var pointer to manager   */
    GcManager* m_manager = nullptr;

    GcReservedMemoryPool m_reservedManager;

    /** @var Queue type   */
    GC_QUEUE_TYPE m_queueType;

    /** @brief shrink limbo groups memory   */
    void ShrinkMem();

    DECLARE_CLASS_LOGGER()
};

}  // namespace MOT
#endif /* MM_GC_QUEUE */
