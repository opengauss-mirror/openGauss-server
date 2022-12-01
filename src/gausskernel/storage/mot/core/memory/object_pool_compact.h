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
 * object_pool_compact.h
 *    Object pool compaction interfaces.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/memory/object_pool_compact.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef OBJECT_POOL_COMPACT_H
#define OBJECT_POOL_COMPACT_H

#include <unordered_map>
#include "object_pool_impl.h"

namespace MOT {
#define PTR_MASK (((uint64_t)-1) << 10)
typedef enum : uint8_t { COMPACT_SIMPLE = 0, COMPACT_REALLOC = 1, COMPACT_DEEP = 2 } CompactTypeT;

struct hashing_func {
    uint64_t operator()(const ObjPool* key) const
    {
        return std::hash<uint64_t>()((uint64_t)key);
    }
};

struct key_equal_fn {
    bool operator()(const ObjPool* t1, const ObjPool* t2) const
    {
        return ((uint64_t)t1 == (uint64_t)t2);
    }
};

typedef std::unordered_map<ObjPool*, ObjPool*, struct hashing_func, struct key_equal_fn> addrMap_t;

class CompactHandler {
public:
    CompactHandler(ObjAllocInterface* pool, const char* prefix);
    ~CompactHandler()
    {
        EndCompaction();
        m_curr = nullptr;
        m_orig = nullptr;
        m_logPrefix = nullptr;
        m_compactedPools = nullptr;
    }
    /**
     * @brief Prepares orig for compaction, calculates fragmentation percent, initializes addrMap and set
     * comactionNeeded to true (if indeed)
     */
    void StartCompaction(CompactTypeT type = COMPACT_REALLOC);
    /** @brief Applies new ObjPools to a general use, and releases empty ObjPools.
     */
    void EndCompaction();

    bool IsCompactionNeeded() const
    {
        return m_compactionNeeded;
    }

    /** @brief Reallocates the object. Allocates a new memory buffer and calls the copy constructor of T.
     */
    template <typename T>
    T* CompactObj(T* obj)
    {
        T* res = nullptr;

        if (!m_compactionNeeded) {
            return res;
        }

        OBJ_RELEASE_START_NOMARK(obj, m_orig->m_size);

        if (m_addrMap.find(op.Get()) != m_addrMap.end()) {
            PoolAllocStateT state = PAS_NONE;
            void* data = nullptr;

            if (m_curr == nullptr) {
                m_curr = ObjPool::GetObjPool(m_orig->m_size, m_orig, m_orig->m_type, true);
            }

            m_curr->Alloc(&data, &state);

            if (state == PAS_EMPTY) {
                ADD_TO_LIST_NOLOCK(m_compactedPools, m_curr);
                m_curr = nullptr;
            }

            res = new (data) T(*(const T*)obj);

            OBJ_RELEASE_MARK(oix_ptr);
            state = PAS_NONE;
            obj->~T();
            op->Release(oix, &state);
#ifdef ENABLE_MEMORY_CHECK
            free(((uint8_t*)obj) - MEMCHECK_METAINFO_SIZE);
#endif
        }

        return res;
    }

    ObjAllocInterface* m_orig;
    bool m_compactionNeeded;
    CompactTypeT m_ctype;
    addrMap_t m_addrMap;

    ObjPoolPtr m_poolsToCompact;
    ObjPool* m_compactedPools;
    ObjPool* m_curr;

    const char* m_logPrefix;
    DECLARE_CLASS_LOGGER();
};
}  // namespace MOT

#endif /* OBJECT_POOL_COMPACT_H */
