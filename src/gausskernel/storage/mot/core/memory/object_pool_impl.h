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
 * object_pool_impl.h
 *    Object pool implementation.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/memory/object_pool_impl.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef OBJECT_POOL_IMPL_H
#define OBJECT_POOL_IMPL_H

#include <new>
#include <atomic>
#include <signal.h>
#include <pthread.h>
#include <string.h>
#include <unordered_map>

#include "global.h"
#include "mot_atomic_ops.h"
#include "utilities.h"
#include "cycles.h"
#include "thread_id.h"
#include "mm_def.h"
#include "mm_buffer_api.h"
#include "mm_session_api.h"
#include "memory_statistics.h"
#include "mm_api.h"
#include "spin_lock.h"

namespace MOT {
#define INITIAL_NUM_OBJPOOL 1
#define NUM_OBJS (uint8_t)(255)
#define NOT_VALID (uint8_t)(-1)
#define G_THREAD_ID ((int16_t)MOTCurrThreadId)
#define OBJ_INDEX_SIZE 1
#define MEMORY_BARRIER asm volatile("" ::: "memory");

#define OBJ_RELEASE_START_NOMARK(ptr, size)                                              \
    uint8_t* p = (uint8_t*)ptr;                                                          \
    uint8_t* oix_ptr = (p + size - 1);                                                   \
    uint8_t oix = *oix_ptr;                                                              \
    if (oix == NOT_VALID) {                                                              \
        printf("Detected double free of pointer or corruption: 0x%lx\n", (uint64_t)ptr); \
        MOTAbort((void*)ptr);                                                            \
    }                                                                                    \
    ObjPoolPtr op = (ObjPool*)(p - sizeof(ObjPool) - oix * size);

#define OBJ_RELEASE_MARK(ptr) (*(uint8_t*)ptr = NOT_VALID)

#define OBJ_RELEASE_START(ptr, size)                                                         \
    uint8_t* p = (uint8_t*)ptr;                                                              \
    uint8_t* oix_ptr = (p + size - 1);                                                       \
    uint8_t oix = *oix_ptr;                                                                  \
    if (oix == NOT_VALID) {                                                                  \
        printf("Detected double free of pointer or corruption: 0x%lx\n", (uint64_t)ptr);     \
        MOTAbort(ptr);                                                                       \
    } else {                                                                                 \
        if (!__sync_bool_compare_and_swap(oix_ptr, oix, NOT_VALID)) {                        \
            printf("Detected double free of pointer or corruption: 0x%lx\n", (uint64_t)ptr); \
            MOTAbort(ptr);                                                                   \
        }                                                                                    \
    }                                                                                        \
    ObjPoolPtr op = (ObjPool*)(p - sizeof(ObjPool) - oix * size);

#define CAS(ptr, oldval, newval) \
    __sync_bool_compare_and_swap((uint64_t*)&ptr, *(uint64_t*)&(oldval), *(uint64_t*)&newval)

#define PUSH_NOLOCK(list, obj) \
    {                          \
        obj->m_objNext = list; \
        list = obj;            \
    }

#define POP_NOLOCK(list)              \
    {                                 \
        if (list.Get() != nullptr) {  \
            list = (list)->m_objNext; \
        }                             \
    }

#define PUSH(list, obj)                            \
    {                                              \
        ++obj;                                     \
        do {                                       \
            obj->m_objNext = list;                 \
        } while (!CAS(list, obj->m_objNext, obj)); \
    }

#define POP(list, obj)                             \
    {                                              \
        do {                                       \
            obj = list;                            \
            if (obj.Get() == nullptr)              \
                break;                             \
        } while (!CAS(list, obj, obj->m_objNext)); \
    }

#define ADD_TO_LIST_NOLOCK(list, obj) \
    {                                 \
        obj->m_next = list;           \
        if (list != nullptr)          \
            list->m_prev = obj;       \
        list = obj;                   \
    }

#define DEL_FROM_LIST_NOLOCK(list, obj)        \
    {                                          \
        if (obj->m_prev != nullptr) {          \
            obj->m_prev->m_next = obj->m_next; \
        }                                      \
        if (obj->m_next != nullptr) {          \
            obj->m_next->m_prev = obj->m_prev; \
        }                                      \
    }

#define ADD_TO_LIST(list, obj)                  \
    {                                           \
        do {                                    \
            obj->m_next = list;                 \
        } while (!CAS(list, obj->m_next, obj)); \
        if (likely(obj->m_next != nullptr))     \
            obj->m_next->m_prev = obj;          \
    }

#define DEL_FROM_LIST(locker, list, obj)           \
    {                                              \
        do {                                       \
            if (list == obj) {                     \
                if (CAS(list, obj, obj->m_next)) { \
                    obj->m_next = nullptr;         \
                    obj->m_prev = nullptr;         \
                    break;                         \
                }                                  \
            }                                      \
            locker.lock();                         \
            DEL_FROM_LIST_NOLOCK(list, obj)        \
            locker.unlock();                       \
        } while (0);                               \
    }

typedef enum PoolAllocState { PAS_FIRST, PAS_EMPTY, PAS_NONE } PoolAllocStateT;

class ObjPool;

// DO NOT CHANGE ORDER OF THE MEMBERS
typedef struct PACKED tagObjPoolSt {
public:
    friend ObjPool;
    uint8_t m_nextFreeObj;

private:
    uint8_t m_fill1[7];

public:
    uint8_t m_objIndexArr[NUM_OBJS + 1];
    uint8_t m_nextOccupiedObj;

private:
    uint8_t m_fill2[7];

public:
    uint8_t m_data[0];
} ObjPoolSt;

typedef enum PoolStatsType : uint8_t { POOL_STATS_ALL = 0, POOL_STATS_FREE } PoolStatsT;

typedef struct tagPoolStatsSt {
    PoolStatsT m_type;
    uint16_t m_objSize;
    uint16_t m_perPoolTotalCount;
    uint32_t m_poolCount;
    uint32_t m_poolFreeCount;
    uint64_t m_totalObjCount;
    uint64_t m_freeObjCount;
    uint64_t m_poolGrossSize;
    int16_t m_fragmentationPercent;
    uint32_t m_perPoolOverhead;
    uint32_t m_perPoolWaist;
} PoolStatsSt;

#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
#define LIST_PTR_SLICE_IX 3
#else
#define LIST_PTR_SLICE_IX 0
#endif
#define LIST_PTR_MASK 0xffffffffffff  // usable address space is 48 bits

class PACKED ObjPoolPtr {
public:
    /**
     * @brief Default constructor for a NULL pointer.
     */
    ObjPoolPtr()
    {
        m_data.m_ptr = nullptr;
    }

    /**
     * @brief Constructs an objects on pre-allocated memory buffer.
     * @param target Address of an object.
     */
    ObjPoolPtr(ObjPool* target);

    /**
     * @brief Copy constructor.
     * @param other The object.
     */
    ObjPoolPtr(ObjPoolPtr& other)
    {
        m_data.m_ptr = other.m_data.m_ptr;
    }

    ObjPoolPtr(const ObjPoolPtr& other)
    {
        m_data.m_ptr = other.m_data.m_ptr;
    }

    /**
     * @brief Retrieves a pointer to the managed object.
     * @return A pointer to the object.
     */
    inline ObjPool* Get()
    {
        return (ObjPool*)(((uint64_t)m_data.m_ptr) & LIST_PTR_MASK);
    }

    /**
     * @brief Member access operator implementation.
     * @return Retrieves a pointer to the managed object.
     */
    inline ObjPool* operator->()
    {
        return Get();
    }

    /**
     * @brief Retrieves a pointer to the managed object.
     * @return A pointer to the object.
     */
    inline ObjPool* Get() const
    {
        return Get();
    }

    /**
     * @brief Member access operator implementation.
     * @return Retrieves a pointer to the managed object.
     */
    inline ObjPool* operator->() const
    {
        return Get();
    }

    inline void operator++();

    ObjPoolPtr& operator=(const ObjPoolPtr& right)
    {
        m_data.m_ptr = right.m_data.m_ptr;
        return *this;
    }

    ObjPoolPtr& operator=(ObjPool* right);

    bool operator==(const ObjPoolPtr& right) const
    {
        return (m_data.m_ptr == right.m_data.m_ptr);
    }

    bool operator==(ObjPoolPtr& right) const
    {
        return (m_data.m_ptr == right.m_data.m_ptr);
    }

    /**
     * @brief Destructor. Deallocates the object.
     */
    ~ObjPoolPtr()
    {}

private:
    union {
        ObjPool* m_ptr;
        uint16_t m_slice[4];
    } m_data;
};

class ObjAllocInterface {
public:
    friend class ObjPool;
    spin_lock m_listLock;
    ObjPool* m_objList;
    ObjPoolPtr m_nextFree;
    uint16_t m_size;
    uint16_t m_oixOffset;
    MemBufferClass m_type;
    bool m_global;

    static ObjAllocInterface* GetObjPool(uint16_t size, bool local, uint8_t align = 8);
    static void FreeObjPool(ObjAllocInterface** pool);

    explicit ObjAllocInterface(bool isGlobal) : m_global(isGlobal)
    {}

    virtual ~ObjAllocInterface();

    virtual bool Initialize()
    {
        return true;
    }

    virtual void* Alloc() = 0;
    template <typename T, class... Args>
    inline T* Alloc(Args&&... args)
    {
        void* buf = Alloc();
        if (unlikely(buf == nullptr))
            return nullptr;
        return new (buf) T(std::forward<Args>(args)...);
    }

    virtual void Release(void* ptr) = 0;
    template <typename T>
    inline void Release(T* obj)
    {
        if (likely(obj != nullptr)) {
            obj->~T();
            Release((void*)obj);
        }
    }

    virtual void ClearThreadCache() = 0;
    virtual void ClearFreeCache() = 0;

    void GetStats(PoolStatsSt& stats);
    void PrintStats(PoolStatsSt& stats, const char* prefix = "", LogLevel level = LogLevel::LL_DEBUG);
    void Print(const char* prefix, LogLevel level = LogLevel::LL_DEBUG);

protected:
    static MemBufferClass CalcBufferClass(uint16_t size);
    DECLARE_CLASS_LOGGER();
};

// DO NOT CHANGE ORDER OF THE MEMBERS
class PACKED ObjPool {
public:
    ObjPool* m_next;
    ObjPool* m_prev;
    uint16_t m_freeCount;
    uint16_t m_totalCount;
    int16_t m_owner;
    uint16_t m_listCounter;
    ObjPoolPtr m_objNext;
    ObjAllocInterface* m_parent;
    int m_notUsedBytes;
    int m_overheadBytes;
    ObjPoolSt m_head;

    ObjPool(uint16_t size, MemBufferClass type, ObjAllocInterface* app)
    {
        m_parent = app;
        m_owner = -1;
        m_objNext = nullptr;
        m_next = m_prev = nullptr;
        *(uint32_t*)(&m_head.m_fill1[3]) = 0xDEADBEEF;
        *(uint32_t*)(&m_head.m_fill2[3]) = 0xDEADBEEF;
        m_overheadBytes = sizeof(ObjPool);
        uint8_t* ptr = m_head.m_data;
        uint8_t* end = ptr + (1024 * MemBufferClassToSizeKb(type)) - sizeof(ObjPool);

        m_freeCount = m_totalCount = (uint16_t)((end - m_head.m_data) / m_parent->m_size);
        uint8_t i = 0;
        for (; i < m_totalCount; i++) {
            m_head.m_objIndexArr[i] = i;
            ptr += m_parent->m_size;
            ptr[-1] = i;
        }
        for (; i < NUM_OBJS; i++) {
            m_head.m_objIndexArr[i] = NOT_VALID;
        }
        m_head.m_objIndexArr[i] = NOT_VALID;
        m_head.m_nextFreeObj = -1;
        m_head.m_nextOccupiedObj = m_totalCount - 1;
        m_overheadBytes += m_totalCount * OBJ_INDEX_SIZE;
        m_notUsedBytes = (int)(end - ptr);
    }

    ~ObjPool()
    {}

    inline void AllocNoLock(void** ret, PoolAllocStateT* state)
    {
        uint8_t ix = ++(m_head.m_nextFreeObj);
        uint8_t oix = m_head.m_objIndexArr[ix];
        m_head.m_objIndexArr[ix] = NOT_VALID;
        *ret = (m_head.m_data + oix * m_parent->m_size);
        ((uint8_t*)(*ret))[m_parent->m_oixOffset] = oix;
        --m_freeCount;
        if (m_freeCount == 0) {
            *state = PAS_EMPTY;
        }
    }

    inline void Alloc(void** ret, PoolAllocStateT* state)
    {
        uint8_t ix = ++(m_head.m_nextFreeObj);
        while (m_head.m_objIndexArr[ix] == NOT_VALID) {
            PAUSE
        }
        uint8_t oix = m_head.m_objIndexArr[ix];
        m_head.m_objIndexArr[ix] = NOT_VALID;
        *ret = (m_head.m_data + oix * m_parent->m_size);
        ((uint8_t*)(*ret))[m_parent->m_oixOffset] = oix;
        uint16_t c = __sync_sub_and_fetch(&m_freeCount, 1);
        if (c == 0) {
            *state = PAS_EMPTY;
        }
    }

    inline void ReleaseNoLock(uint8_t oix, PoolAllocStateT* state)
    {
        ++(m_head.m_nextOccupiedObj);
        m_head.m_objIndexArr[m_head.m_nextOccupiedObj] = oix;
        ++m_freeCount;

        if (m_freeCount == 1) {
            *state = PAS_FIRST;
            return;
        }
    }

    inline void Release(uint8_t oix, PoolAllocStateT* state)
    {
        uint8_t ix = __sync_add_and_fetch(&m_head.m_nextOccupiedObj, 1);
        m_head.m_objIndexArr[ix] = oix;
        uint16_t c = __sync_add_and_fetch(&m_freeCount, 1);

        if (c == 1) {
            *state = PAS_FIRST;
            return;
        }
    }

    static ObjPool* GetObjPool(uint16_t size, ObjAllocInterface* app, MemBufferClass type, bool global)
    {
#ifdef TEST_STAT_ALLOC
        uint64_t start_time = GetSysClock();
#endif
#ifndef MEM_ACTIVE
        void* p = (void*)malloc((1024 * MemBufferClassToSizeKb(type)));
#else
        void* p;

        if (global == true) {
            p = MemBufferAllocGlobal(type);
        } else {
#ifdef MEM_SESSION_ACTIVE
            uint32_t bufferSize = 1024 * MemBufferClassToSizeKb(type);
            p = MemSessionAlloc(bufferSize);
            if (p) {
                DetailedMemoryStatisticsProvider::m_provider->AddLocalBuffersUsed(MOTCurrentNumaNodeId, type);
            }
#else
            p = MemBufferAllocLocal(type);
#endif  // MEM_SESSION_ACTIVE
        }
#endif  // MEM_ACTIVE
#ifdef TEST_STAT_ALLOC
        uint64_t end_time = GetSysClock();
        MemoryStatisticsProvider::m_provider->AddMallocTime(
            CpuCyclesLevelTime::CyclesToNanoseconds(end_time - start_time));
#endif
        ObjPool* o = nullptr;
        if (p) {
            o = new (p) ObjPool(size, type, app);
        } else {
            MOT_REPORT_ERROR(MOT_ERROR_OOM,
                "N/A",
                "Failed to allocate %s %s buffer for object pool",
                MemBufferClassToString(type),
                global ? "global" : "local");
        }
        return o;
    }

    static void DelObjPool(void* ptr, MemBufferClass type, bool global)
    {
#ifdef TEST_STAT_ALLOC
        uint64_t start_time = GetSysClock();
#endif
#ifndef MEM_ACTIVE
        free(ptr);
#else
        if (global == true) {
            MemBufferFreeGlobal(ptr, type);
        } else {
#ifdef MEM_SESSION_ACTIVE
            MemSessionFree(ptr);
            DetailedMemoryStatisticsProvider::m_provider->AddLocalBuffersFreed(MOTCurrentNumaNodeId, type);
#else
            MemBufferFreeLocal(ptr, type);
#endif  // MEM_SESSION_ACTIVE
        }
#endif  // MEM_ACTIVE
#ifdef TEST_STAT_ALLOC
        uint64_t end_time = GetSysClock();
        MemoryStatisticsProvider::m_provider->AddFreeTime(
            CpuCyclesLevelTime::CyclesToNanoseconds(end_time - start_time));
#endif
    }

private:
    DECLARE_CLASS_LOGGER();
};

inline void ObjPoolPtr::operator++()
{
    Get()->m_listCounter++;
    m_data.m_slice[LIST_PTR_SLICE_IX] = Get()->m_listCounter;
    MEMORY_BARRIER;
}
}  // namespace MOT

#endif /* OBJECT_POOL_IMPL_H */
