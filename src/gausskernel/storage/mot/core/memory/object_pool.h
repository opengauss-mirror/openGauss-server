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
 *    Object pool interfaces.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/memory/object_pool.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef OBJECT_POOL_H
#define OBJECT_POOL_H

#include "object_pool_impl.h"

namespace MOT {
class LocalObjPool : public ObjAllocInterface {
public:
    LocalObjPool(uint16_t sz, uint8_t align) : ObjAllocInterface(false)
    {
        m_nextFree = nullptr;
        m_objList = nullptr;
#ifdef ENABLE_MEMORY_CHECK
        m_size = MEMCHECK_OBJPOOL_SIZE;
        m_actualSize = ALIGN_N(sz + MEMCHECK_METAINFO_SIZE, align);
#else
        m_size = ALIGN_N(sz + OBJ_INDEX_SIZE, align);
#endif
        m_oixOffset = m_size - 1;
        m_type = ObjAllocInterface::CalcBufferClass(m_size);
    };

    ~LocalObjPool() override
    {
        Print("Local");
    }

    bool Initialize() override
    {
        bool result = true;
        for (int i = 0; i < INITIAL_NUM_OBJPOOL; i++) {
            ObjPool* op = ObjPool::GetObjPool(m_size, this, m_type, false);
            if (op == nullptr) {
                // memory de-allocated when object is destroyed (see ~ObjAllocInterface())
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

    void Release(void* ptr) override
    {
        PoolAllocStateT state = PAS_NONE;

        OBJ_RELEASE_START(ptr, m_size);
#ifdef ENABLE_MEMORY_CHECK
        errno_t erc = memset_s(ptr, m_actualSize - MEMCHECK_METAINFO_SIZE, 'Z', m_actualSize - MEMCHECK_METAINFO_SIZE);
        securec_check(erc, "\0", "\0");
#elif defined(DEBUG)
        errno_t erc = memset_s(ptr, m_oixOffset, 'Z', m_oixOffset);
        securec_check(erc, "\0", "\0");
#endif
        op->ReleaseNoLock(oix, &state);
        if (state == PAS_FIRST) {
            PUSH_NOLOCK(m_nextFree, op);
        }
#ifdef ENABLE_MEMORY_CHECK
        free(((uint8_t*)ptr) - MEMCHECK_METAINFO_SIZE);
#endif
    }

    void* Alloc() override
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

        if (state == PAS_EMPTY) {
            POP_NOLOCK(m_nextFree);
        }

        return data;
    }

    void ClearThreadCache() override
    {
        return;
    }

    void ClearAllThreadCache() override
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
    ThreadAOP m_threadAOP[MAX_THREAD_COUNT];
    pthread_mutex_t m_thrLock;
    GlobalObjPool(uint16_t sz, uint8_t align) : ObjAllocInterface(true)
    {
        m_objList = nullptr;
        m_nextFree = nullptr;
#ifdef ENABLE_MEMORY_CHECK
        m_size = MEMCHECK_OBJPOOL_SIZE;
        m_actualSize = ALIGN_N(sz + MEMCHECK_METAINFO_SIZE, align);
#else
        m_size = ALIGN_N(sz + OBJ_INDEX_SIZE, align);
#endif
        m_oixOffset = m_size - 1;
        m_type = ObjAllocInterface::CalcBufferClass(m_size);
        errno_t erc =
            memset_s(m_threadAOP, MAX_THREAD_COUNT * sizeof(ThreadAOP), 0, MAX_THREAD_COUNT * sizeof(ThreadAOP));
        securec_check(erc, "\0", "\0");
        m_thrLock = PTHREAD_MUTEX_INITIALIZER;
    };

    ~GlobalObjPool() override
    {
        Print("Global");
    }

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
                COMPILER_BARRIER;
                return tmp;
            }
        }

        ObjPool* op = ObjPool::GetObjPool(m_size, this, m_type, true);

        if (op != nullptr) {
            ADD_TO_LIST(m_listLock, m_objList, op);
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

    void Release(void* ptr) override
    {
        PoolAllocStateT state = PAS_NONE;
        ThreadAOP* t = &m_threadAOP[G_THREAD_ID];

        OBJ_RELEASE_START(ptr, m_size);
#ifdef ENABLE_MEMORY_CHECK
        errno_t erc = memset_s(ptr, m_actualSize - MEMCHECK_METAINFO_SIZE, 'Z', m_actualSize - MEMCHECK_METAINFO_SIZE);
        securec_check(erc, "\0", "\0");
#elif defined(DEBUG)
        errno_t erc = memset_s(ptr, m_oixOffset, 'Z', m_oixOffset);
        securec_check(erc, "\0", "\0");
#endif
        op->Release(oix, &state);

        if (state == PAS_FIRST) {
            op->m_owner = G_THREAD_ID;
            PUSH_NOLOCK(t->m_nextFree, op);
        }
#ifdef ENABLE_MEMORY_CHECK
        free(((uint8_t*)ptr) - MEMCHECK_METAINFO_SIZE);
#endif
    }

    void* Alloc() override
    {
        PoolAllocStateT state = PAS_NONE;
        void* data = nullptr;
        ObjPoolPtr opNext;
        ThreadAOP* t = &m_threadAOP[G_THREAD_ID];

        if (t->m_nextFree.Get() == nullptr) {
            t->m_nextFree = Reserve();
            if (unlikely(t->m_nextFree.Get() == nullptr)) {  // out of memory
                MOT_REPORT_ERROR(MOT_ERROR_OOM, "N/A", "Failed to reserve sub-pool in thread %u" PRId16, G_THREAD_ID);
                return nullptr;
            }
        }

        opNext = t->m_nextFree->m_objNext;
        t->m_nextFree->Alloc(&data, &state);

        if (state == PAS_EMPTY) {
            t->m_nextFree = opNext;
        }
        return data;
    }

    void ClearThreadCache() override
    {
        (void)pthread_mutex_lock(&m_thrLock);
        ObjPoolPtr op = m_threadAOP[G_THREAD_ID].m_nextFree;
        ObjPoolPtr head = op;
        ObjPoolPtr tail = op;
        while (op.Get() != nullptr) {
            if (op->m_freeCount == op->m_totalCount) {
                ObjPool* p = op.Get();
                if (head.Get() == op.Get()) {
                    head = op->m_objNext;
                    tail = op->m_objNext;
                } else {
                    tail->m_objNext = op->m_objNext;
                }
                op = op->m_objNext;
                DEL_FROM_LIST(m_listLock, m_objList, p);
                ObjPool::DelObjPool(p, m_type, m_global);
            } else {
                tail = op;
                op = op->m_objNext;
            }
        }
        m_threadAOP[G_THREAD_ID].m_nextFree = nullptr;

        if (head.Get() == nullptr) {
            (void)pthread_mutex_unlock(&m_thrLock);
            return;
        }
        if (tail.Get() == nullptr) {
            tail = head;
        }
        do {
            tail->m_objNext = m_nextFree;
        } while (!CAS(m_nextFree, tail->m_objNext, head));

        (void)pthread_mutex_unlock(&m_thrLock);
    }
    void ClearAllThreadCache() override
    {
        for (int i = 0; i < MAX_THREAD_COUNT; i++) {
            ObjPoolPtr op = m_threadAOP[i].m_nextFree;

            while (op.Get() != nullptr) {
                ObjPoolPtr tmp = op->m_objNext;
                Unreserve(op);
                op = tmp;
            }

            m_threadAOP[i].m_nextFree = nullptr;
        }
        m_nextFree = nullptr;
    }
    void ClearFreeCache() override
    {
        ObjPoolPtr op = m_threadAOP[G_THREAD_ID].m_nextFree;
        ObjPoolPtr head = op;
        ObjPoolPtr prev = op;
        while (op.Get() != nullptr) {
            if (op->m_freeCount == op->m_totalCount) {
                ObjPool* p = op.Get();
                if (head.Get() == op.Get()) {
                    head = op->m_objNext;
                    prev = op->m_objNext;
                } else {
                    prev->m_objNext = op->m_objNext;
                }
                op = op->m_objNext;
                DEL_FROM_LIST(m_listLock, m_objList, p);
                ObjPool::DelObjPool(p, m_type, m_global);
            } else {
                prev = op;
                op = op->m_objNext;
            }
        }
        m_threadAOP[G_THREAD_ID].m_nextFree = head;
    }
};

class SlabAllocator {
public:
    SlabAllocator(int min, int max, bool local) : m_isLocal(local)
    {
        m_isInitialized = true;
        if (min < SLAB_MIN_BIN) {
            m_minBin = SLAB_MIN_BIN;
        } else {
            m_minBin = min;
        }

        if (max > SLAB_MAX_BIN) {
            m_isInitialized = false;
            MOT_LOG_PANIC(
                "Max allowed slub size is 32KB (bin 15), failed to create slab allocator for min: %d, max: %d",
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

        for (int i = 0; i <= SLAB_MAX_BIN; i++) {
            m_bins[i] = nullptr;
        }

        for (int i = min; i <= m_maxBin; i++) {
            int s = (1U << i) - 1;
            m_bins[i] = ObjAllocInterface::GetObjPool(s, local);
            if (m_bins[i] == nullptr) {
                m_isInitialized = false;
            }
        }
    }

    ~SlabAllocator()
    {
        for (int i = 0; i <= SLAB_MAX_BIN; i++) {
            if (m_bins[i] != nullptr) {
                ObjAllocInterface::FreeObjPool(&m_bins[i]);
            }
        }
    }

    inline bool Initialize()
    {
        bool result = true;
        for (int i = m_minBin; i <= m_maxBin; i++) {
            int s = (1U << i) - 1;
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
            MOT_LOG_PANIC("Unsupported size %d, max allowed size %d", size, (1U << m_maxBin) - 1);
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
        size = (1U << i) - 1;
        return m_bins[i]->Alloc();
    }

    void ClearThreadCache()
    {
        for (int i = m_minBin; i <= m_maxBin; i++) {
            if (m_bins[i] != nullptr) {
                m_bins[i]->ClearThreadCache();
            }
        }
    }

    void ClearFreeCache()
    {
        for (int i = m_minBin; i <= m_maxBin; i++) {
            if (m_bins[i] != nullptr) {
                m_bins[i]->ClearFreeCache();
            }
        }
    }

    void Compact();

    inline bool IsSlabInitialized() const
    {
        return m_isInitialized;
    }

    PoolStatsSt* GetStats();
    void FreeStats(PoolStatsSt* stats);
    void GetSize(uint64_t& size, uint64_t& netto);
    void PrintSize(uint64_t& size, uint64_t& netto, const char* prefix = "");
    void PrintStats(PoolStatsSt* stats, const char* prefix = "", LogLevel level = LogLevel::LL_DEBUG) const;
    void Print(const char* prefix, LogLevel level = LogLevel::LL_DEBUG);

    static constexpr int SLAB_MIN_BIN = 3;
    static constexpr int SLAB_MAX_BIN = 15;  // up to 32KB

private:
    bool m_isLocal;
    bool m_isInitialized;
    int m_minBin;
    int m_maxBin;
    ObjAllocInterface* m_bins[SLAB_MAX_BIN + 1];

    DECLARE_CLASS_LOGGER();
};
}  // namespace MOT

#endif /* OBJECT_POOL_H */
