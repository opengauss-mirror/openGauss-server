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
    ThreadAOP m_threadAOP[MAX_THREAD_COUNT];

    GlobalObjPool(uint16_t sz, uint8_t align) : ObjAllocInterface(true)
    {
        m_objList = nullptr;
        m_nextFree = nullptr;
        m_size = ALIGN_N(sz + OBJ_INDEX_SIZE, align);
        m_oixOffset = m_size - 1;
        m_type = ObjAllocInterface::CalcBufferClass(sz);
        errno_t erc =
            memset_s(m_threadAOP, MAX_THREAD_COUNT * sizeof(ThreadAOP), 0, MAX_THREAD_COUNT * sizeof(ThreadAOP));
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
}  // namespace MOT

#endif /* OBJECT_POOL_H */
