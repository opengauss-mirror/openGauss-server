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
 * spsc_allocator.h
 *    SPSC Variable size allocator implementation.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/memory/spsc_allocator.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef SPSC_ALLOCATOR_H
#define SPSC_ALLOCATOR_H

#include "global.h"
#include "utilities.h"
#include "mot_atomic_ops.h"
#include "mm_def.h"
#include "mot_error.h"

namespace MOT {

#define SPSC_ALLOCATOR_MAXSIZE (1 << 30)  // 1GB
#define SPSC_ALIGN_SIZE 8
#define PTR_ALLOC_MASK (uint32_t)0x80EADBEAF
#define PTR_FREE_MASK (uint32_t)0x7FEADBEAF
#define PTR_UNUSED_MASK (uint32_t)0x81EADBEAF

typedef struct PACKED tagSPSCPtrSt {
    uint32_t m_mask;
    uint32_t m_size;
    uint8_t m_data[0];
} SPSCPtrSt;

#define PTR_META_OFFSET (sizeof(SPSCPtrSt))

class PACKED SPSCVarSizeAllocator {
public:
    static SPSCVarSizeAllocator* GetSPSCAllocator(uint32_t size);
    static void FreeSPSCAllocator(SPSCVarSizeAllocator* spsc);

    inline uint32_t GetSize() const
    {
        return (m_size + sizeof(SPSCVarSizeAllocator));
    }

    explicit SPSCVarSizeAllocator(uint32_t size)
        : m_head((SPSCPtrSt*)m_data), m_size(size - sizeof(SPSCVarSizeAllocator))
    {
        m_head->m_mask = PTR_FREE_MASK;
        m_tail = m_head;
        m_end = (SPSCPtrSt*)(m_data + m_size);
        for (int i = 0; i < 6; i++) {
            m_filler[i] = 0xDEADBEEFDEADBEEF;
        }
    }

    ~SPSCVarSizeAllocator()
    {
        m_head = nullptr;
        m_tail = nullptr;
        m_end = nullptr;
    }

    inline void* Alloc(uint32_t size)
    {
        void* res = nullptr;
        uint32_t asize = ALIGN8(size + PTR_META_OFFSET);
        SPSCPtrSt* newhead = (SPSCPtrSt*)(((uint8_t*)m_head) + asize);
        SPSCPtrSt* currTail = m_tail;  // read tail once
        Prefetch(m_head);
        if (m_head < currTail) {
            if (newhead <= currTail) {
                m_head->m_mask = PTR_ALLOC_MASK;
                m_head->m_size = asize;
                res = (void*)m_head->m_data;
                m_head = newhead;
            }
        } else {
            if (m_head == currTail && m_head->m_mask == PTR_ALLOC_MASK) {
                return res;
            }
            if (newhead <= m_end) {
                m_head->m_mask = PTR_ALLOC_MASK;
                m_head->m_size = asize;
                res = (void*)m_head->m_data;
                if (newhead == m_end) {
                    m_head = (SPSCPtrSt*)m_data;
                } else {
                    m_head = newhead;
                }
            } else {
                newhead = (SPSCPtrSt*)(m_data + asize);
                if (newhead <= currTail) {
                    m_head->m_mask = PTR_UNUSED_MASK;
                    m_head = (SPSCPtrSt*)m_data;
                    m_head->m_mask = PTR_ALLOC_MASK;
                    m_head->m_size = asize;
                    res = (void*)m_head->m_data;
                    m_head = newhead;
                }
            }
        }
        COMPILER_BARRIER;
        return res;
    }

    inline void Release(void* ptr)
    {
        if (ptr != nullptr) {
            SPSCPtrSt* currHead = m_head;  // load once
            SPSCPtrSt* freePtr = (SPSCPtrSt*)(((uint8_t*)ptr) - PTR_META_OFFSET);

            if (freePtr < (SPSCPtrSt*)m_data || freePtr > m_end) {
                MOT_LOG_PANIC("Pointer is not associated with current allocator: 0x%lx\n", (uintptr_t)ptr);
                MOTAbort(ptr);
                return;
            }
            if (freePtr->m_mask != PTR_ALLOC_MASK) {
                MOT_LOG_PANIC("Detected double free of pointer or corruption: 0x%lx\n", (uintptr_t)ptr);
                MOTAbort(ptr);
                return;
            }
            freePtr->m_mask = PTR_FREE_MASK;
            if (m_tail == freePtr) {
                m_tail = (SPSCPtrSt*)(((uint8_t*)freePtr) + freePtr->m_size);
                if (m_tail == m_end) {
                    m_tail = (SPSCPtrSt*)m_data;
                }
            }

            // check if we had un-ordered releases
            while (true) {
                if (m_tail->m_mask == PTR_ALLOC_MASK) {
                    break;
                } else if (currHead == m_tail) {
                    break;
                } else if (m_tail->m_mask == PTR_FREE_MASK) {
                    m_tail = (SPSCPtrSt*)(((uint8_t*)m_tail) + m_tail->m_size);
                    if (m_tail == m_end) {
                        m_tail = (SPSCPtrSt*)m_data;
                    }
                } else if (m_tail->m_mask == PTR_UNUSED_MASK) {
                    m_tail = (SPSCPtrSt*)m_data;
                } else {
                    break;
                }
            }
            COMPILER_BARRIER;
        }
    }

private:
    DECLARE_CLASS_LOGGER();
    // DO NOT change the order of the members
    SPSCPtrSt* m_head;
    SPSCPtrSt* m_end;  // should not be de-referenced, points beyond the data
    uint64_t m_size;
    uint64_t m_filler[6];  // m_head and m_tail should be CACHE_LINE_SIZE away from each other
    SPSCPtrSt* m_tail;
    // do not add members after
    uint8_t m_data[0];
};
}  // namespace MOT

#endif /* SPSC_ALLOCATOR_H */
