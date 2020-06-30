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
 * object_pool.h
 *    Object pool implementation.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/src/memory/object_pool.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef OBJECT_POOL_H
#define OBJECT_POOL_H

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
#define MAX_THR_NUM 4096
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
typedef struct PACKED ObjPool_st {
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

typedef struct PoolStats_st {
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
        if (m_freeCount == 0)
            *state = PAS_EMPTY;
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
        if (c == 0)
            *state = PAS_EMPTY;
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

    void Release(uint8_t oix, PoolAllocStateT* state)
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

        if (global == true)
            p = MemBufferAllocGlobal(type);
        else
#ifdef MEM_SESSION_ACTIVE
        {
            uint32_t buffer_size = 1024 * MemBufferClassToSizeKb(type);
            p = MemSessionAlloc(buffer_size);
            if (p) {
                DetailedMemoryStatisticsProvider::m_provider->AddLocalBuffersUsed(MOTCurrentNumaNodeId, type);
            }
        }
#else
            p = MemBufferAllocLocal(type);
#endif  // MEM_SESSION_ACTIVE
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
        if (global == true)
            MemBufferFreeGlobal(ptr, type);
        else
#ifdef MEM_SESSION_ACTIVE
        {
            MemSessionFree(ptr);
            DetailedMemoryStatisticsProvider::m_provider->AddLocalBuffersFreed(MOTCurrentNumaNodeId, type);
        }
#else
            MemBufferFreeLocal(ptr, type);
#endif  // MEM_SESSION_ACTIVE
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

class LocalObjPool : public ObjAllocInterface {
public:
    LocalObjPool(uint16_t sz, uint8_t align) : ObjAllocInterface(false)
    {
        m_nextFree = nullptr;
        m_objList = nullptr;
        m_size = ALIGN_N(sz + OBJ_INDEX_SIZE, align);
        m_oixOffset = m_size - 1;
        m_type = ObjAllocInterface::CalcBufferClass(sz);
    };

    ~LocalObjPool() override
    {
        Print("Local");
    };

    bool Initialize() override
    {
        bool result = true;
        for (int i = 0; i < INITIAL_NUM_OBJPOOL; i++) {
            ObjPool* op = ObjPool::GetObjPool(m_size, this, m_type, false);
            if (op == nullptr) {
                // memory deallocated when object is destroyed (see ~ObjAllocInterface())
                MOT_REPORT_ERROR(MOT_ERROR_OOM, "N/A", "Failed to initialize local object pool");
                result = false;
                break;
            } else {
                ObjPoolPtr op_ptr = op;
                ADD_TO_LIST_NOLOCK(m_objList, op);
                PUSH_NOLOCK(m_nextFree, op_ptr);
            }
        }
        return result;
    }

    inline void Release(void* ptr) override
    {
        PoolAllocStateT state = PAS_NONE;

        OBJ_RELEASE_START(ptr, m_size);
        op->ReleaseNoLock(oix, &state);
        if (state == PAS_FIRST)
            PUSH_NOLOCK(m_nextFree, op);
    }

    inline void* Alloc() override
    {
        PoolAllocStateT state = PAS_NONE;
        void* data = nullptr;
        if (MOT_EXPECT_FALSE(m_nextFree.Get() == nullptr)) {
            ObjPool* op = ObjPool::GetObjPool(m_size, this, m_type, false);

            if (op != nullptr) {
                ObjPoolPtr op_ptr = op;
                ADD_TO_LIST_NOLOCK(m_objList, op);
                PUSH_NOLOCK(m_nextFree, op_ptr);
            } else {
                MOT_REPORT_ERROR(MOT_ERROR_OOM, "N/A", "Failed to allocate sub-pool");
                return nullptr;
            }
        }

        m_nextFree->AllocNoLock(&data, &state);

        if (state == PAS_EMPTY)
            POP_NOLOCK(m_nextFree);

        return data;
    }

    void ClearThreadCache() override
    {
        return;
    }

    void ClearFreeCache() override
    {
        ObjPoolPtr prev = m_nextFree;
        ObjPoolPtr p = (m_nextFree.Get() != nullptr ? m_nextFree->m_objNext : nullptr);

        while (p.Get() != nullptr) {
            if (p->m_freeCount == p->m_totalCount) {
                ObjPool* op = p.Get();

                prev->m_objNext = p->m_objNext;
                p = p->m_objNext;

                if (unlikely(op == m_objList)) {
                    m_objList = op->m_next;
                    m_objList->m_prev = nullptr;
                } else {
                    DEL_FROM_LIST_NOLOCK(m_objList, op);
                }
                ObjPool::DelObjPool(op, m_type, m_global);
            } else {
                prev = p;
                p = p->m_objNext;
            }
        }
    }
};

typedef struct PACKED __ThreadAOP {
    ObjPoolPtr m_nextFree;
} ThreadAOP;

class GlobalObjPool : public ObjAllocInterface {
public:
    ThreadAOP m_threadAOP[MAX_THR_NUM];

    GlobalObjPool(uint16_t sz, uint8_t align) : ObjAllocInterface(true)
    {
        m_objList = nullptr;
        m_nextFree = nullptr;
        m_size = ALIGN_N(sz + OBJ_INDEX_SIZE, align);
        m_oixOffset = m_size - 1;
        m_type = ObjAllocInterface::CalcBufferClass(sz);
        errno_t erc = memset_s(m_threadAOP, MAX_THR_NUM * sizeof(ThreadAOP), 0, MAX_THR_NUM * sizeof(ThreadAOP));
        securec_check(erc, "\0", "\0");
    };

    ~GlobalObjPool() override
    {
        Print("Global");
    };

    bool Initialize() override
    {
        bool result = true;
        for (int i = 0; i < INITIAL_NUM_OBJPOOL; i++) {
            ObjPool* op = ObjPool::GetObjPool(m_size, this, m_type, true);
            if (op == nullptr) {
                // memory deallocated when object is destroyed (see ~ObjAllocInterface())
                result = false;
                break;
            } else {
                ObjPoolPtr op_ptr = op;
                ADD_TO_LIST_NOLOCK(m_objList, op);
                PUSH_NOLOCK(m_nextFree, op_ptr);
            }
        }
        return result;
    }

    inline ObjPoolPtr Reserve()
    {
        ObjPoolPtr tmp = nullptr;

        if (m_nextFree.Get() != nullptr) {
            POP(m_nextFree, tmp);
            if (tmp.Get() != nullptr) {
                tmp->m_owner = G_THREAD_ID;
                tmp->m_objNext = nullptr;
                MEMORY_BARRIER;
                return tmp;
            }
        }

        ObjPool* op = ObjPool::GetObjPool(m_size, this, m_type, true);

        if (op != nullptr) {
            ADD_TO_LIST(m_objList, op);
            op->m_owner = G_THREAD_ID;
            tmp = op;
        }

        return tmp;
    }

    inline void Unreserve(ObjPoolPtr op)
    {
        op->m_owner = -1;
        PUSH(m_nextFree, op);
    }

    inline void Release(void* ptr) override
    {
        PoolAllocStateT state = PAS_NONE;
        ThreadAOP* t = &m_threadAOP[G_THREAD_ID];

        OBJ_RELEASE_START(ptr, m_size);
        op->Release(oix, &state);

        if (state == PAS_FIRST) {
            if (t->m_nextFree.Get() == nullptr) {
                op->m_owner = G_THREAD_ID;
                t->m_nextFree = op;
            } else {
                op->m_owner = -1;
                MEMORY_BARRIER;
                PUSH(m_nextFree, op);
            }
        }
    }

    inline void* Alloc() override
    {
        PoolAllocStateT state = PAS_NONE;
        void* data = nullptr;

        ThreadAOP* t = &m_threadAOP[G_THREAD_ID];

        if (t->m_nextFree.Get() == nullptr) {
            t->m_nextFree = Reserve();
            if (unlikely(t->m_nextFree.Get() == nullptr)) {  // out of memory
                MOT_REPORT_ERROR(
                    MOT_ERROR_OOM, "N/A", "Failed to reserve sub-pool in thread %u", (unsigned)G_THREAD_ID);
                return nullptr;
            }
        }

        t->m_nextFree->Alloc(&data, &state);

        if (state == PAS_EMPTY) {
            t->m_nextFree = nullptr;
        }
        return data;
    }

    void ClearThreadCache() override
    {
        ObjPoolPtr op = m_threadAOP[G_THREAD_ID].m_nextFree;

        while (op.Get() != nullptr) {
            ObjPoolPtr tmp = op->m_objNext;
            Unreserve(op);
            op = tmp;
        }

        m_threadAOP[G_THREAD_ID].m_nextFree = nullptr;
    }

    void ClearFreeCache() override
    {
        ObjPoolPtr orig = nullptr;
        ObjPoolPtr p = nullptr;
        do {
            orig = m_nextFree;
        } while (!CAS(m_nextFree, orig, p));
        ObjPoolPtr prev = orig;
        p = (orig.Get() != nullptr ? orig->m_objNext : nullptr);

        while (p.Get() != nullptr) {
            if (p->m_freeCount == p->m_totalCount) {
                ObjPool* op = p.Get();

                prev->m_objNext = p->m_objNext;
                p = p->m_objNext;

                DEL_FROM_LIST(m_listLock, m_objList, op);
                ObjPool::DelObjPool(op, m_type, m_global);
            } else {
                ++(prev->m_objNext);
                prev = p;
                p = p->m_objNext;
            }
        }

        if (unlikely(prev.Get() != nullptr)) {
            ++orig;
            do {
                prev->m_objNext = m_nextFree;
            } while (!CAS(m_nextFree, prev->m_objNext, orig));
        }
    }
};

#define SLUB_MAX_BIN 15  // up to 32KB
#define SLUB_MIN_BIN 3

class SlabAllocator {
public:
    SlabAllocator(int min, int max, bool local) : m_isLocal(local)
    {
        m_isInitialized = true;
        if (min < SLUB_MIN_BIN) {
            m_minBin = SLUB_MIN_BIN;
        } else {
            m_minBin = min;
        }

        if (max > SLUB_MAX_BIN) {
            m_isInitialized = false;
            MOT_LOG_PANIC(
                "Max allowed slub size is 64KB (bin 16), failed to create slab allocator for min: %d, max: %d",
                min,
                max);
            MOTAbort();
        } else {
            m_maxBin = max;
        }

        if (m_minBin >= m_maxBin) {
            m_isInitialized = false;
            MOT_LOG_PANIC("Failed to create slab allocator for min: %d, max: %d", min, max);
            MOTAbort();
        }

        for (int i = 0; i <= SLUB_MAX_BIN; i++) {
            m_bins[i] = nullptr;
        }

        for (int i = min; i <= m_maxBin; i++) {
            int s = (1 << i) - 1;
            m_bins[i] = ObjAllocInterface::GetObjPool(s, local);
            if (m_bins[i] == nullptr) {
                m_isInitialized = false;
            }
        }
    }

    ~SlabAllocator()
    {
        for (int i = 0; i <= SLUB_MAX_BIN; i++) {
            if (m_bins[i] != nullptr) {
                ObjAllocInterface::FreeObjPool(&m_bins[i]);
            }
        }
    }

    inline bool Initialize()
    {
        bool result = true;
        for (int i = m_minBin; i <= m_maxBin; i++) {
            int s = (1 << i) - 1;
            m_bins[i] = ObjAllocInterface::GetObjPool(s, m_isLocal);
            if (m_bins[i] == nullptr) {
                result = false;
                break;
            }
        }

        return result;
    }

    inline int CalcBinNum(int size) const
    {
        int b = (__builtin_clz(size) ^ 31) + 1;
        if (unlikely(b > m_maxBin)) {
            MOT_LOG_PANIC("Unsupported size %d, max allowed size %d", size, (1 << m_maxBin) - 1);
            MOTAbort();
        }

        if (b < m_minBin) {
            return m_minBin;
        }

        return b;
    }

    inline void Release(void* ptr, int size)
    {
        int i = CalcBinNum(size);

        m_bins[i]->Release(ptr);
    }

    inline void* Alloc(int& size)
    {
        int i = CalcBinNum(size);
        size = (1 << i) - 1;
        return m_bins[i]->Alloc();
    }

    void ClearThreadCache()
    {
        if (!m_isLocal) {
            for (int i = m_minBin; i <= m_maxBin; i++) {
                if (m_bins[i] != nullptr) {
                    m_bins[i]->ClearThreadCache();
                }
            }
        }
    }

    void ClearFreeCache()
    {
        if (m_isLocal) {
            for (int i = m_minBin; i <= m_maxBin; i++) {
                if (m_bins[i] != nullptr) {
                    m_bins[i]->ClearFreeCache();
                }
            }
        }
    }

    inline bool IsSlabInitialized() const
    {
        return m_isInitialized;
    }

    PoolStatsSt* GetStats();
    void FreeStats(PoolStatsSt* stats);
    void GetSize(uint64_t& size, uint64_t& netto);
    void PrintStats(PoolStatsSt* stats, const char* prefix = "", LogLevel level = LogLevel::LL_DEBUG);
    void Print(const char* prefix, LogLevel level = LogLevel::LL_DEBUG);

private:
    bool m_isLocal;
    bool m_isInitialized;
    int m_minBin;
    int m_maxBin;
    ObjAllocInterface* m_bins[SLUB_MAX_BIN + 1];

    DECLARE_CLASS_LOGGER();
};

inline void ObjPoolPtr::operator++()
{
    Get()->m_listCounter++;
    m_data.m_slice[LIST_PTR_SLICE_IX] = Get()->m_listCounter;
    MEMORY_BARRIER;
}
}  // namespace MOT

#endif /* OBJECT_POOL_H */
