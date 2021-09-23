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
 * policy_memory_alloc.h
 *
 * IDENTIFICATION
 *    src/include/gs_policy/policy_memory_alloc.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef GS_POLICY_MEMORY_ALLOC_H_
#define GS_POLICY_MEMORY_ALLOC_H_

#include <exception>
#include "knl/knl_instance.h"
#include <memory>
#include "postgres.h"
#include <string>
#include <stddef.h>
#include "utils/palloc.h"

#include "knl/knl_thread.h"


namespace Memory_allocator 
{
    typedef enum MemoryContextType {
        MemoryCxt_Audit,
        MemoryCxt_Mask,
        MemoryCxt_RLS,
        MemoryCxt_policy_instance
    } MemoryContextType;

    template<typename T, MemoryContextType type = MemoryCxt_policy_instance>
    class MemoryContextAdapterAllocator {
    public:
        typedef T* pointer;
        typedef const T * const_pointer;
        typedef T& reference;
        typedef const T & const_reference;
        typedef T value_type;
        typedef size_t size_type;
        typedef ptrdiff_t difference_type;

        MemoryContextAdapterAllocator () {}
        ~MemoryContextAdapterAllocator () {}
        template<class U> MemoryContextAdapterAllocator (const MemoryContextAdapterAllocator<U, type>&) {}
        template<class U>
        struct rebind { typedef MemoryContextAdapterAllocator<U, type> other; };
        pointer address(reference node) const
        {
            return &node;
        }
        const_pointer address(const_reference node) const
        {
            return &node;
        }

        static bool has_free_memory(size_t size)
        {
            return true;
        }
    
        pointer allocate(size_type n, const_pointer hint = 0)
        {
            switch (type) {
                case MemoryCxt_Audit:
                    return static_cast<pointer>(
                        MemoryContextAlloc(t_thrd.security_policy_cxt.policy_audit_context, n * sizeof(T)));
                case MemoryCxt_Mask:
                    return static_cast<pointer>(
                        MemoryContextAlloc(t_thrd.security_policy_cxt.policy_mask_context, n * sizeof(T)));
                case MemoryCxt_RLS:
                    return static_cast<pointer>(
                        MemoryContextAlloc(t_thrd.security_policy_cxt.policy_rls_context, n * sizeof(T)));
                default:
                    return static_cast<pointer>(
                        MemoryContextAlloc(g_instance.policy_cxt.policy_instance_cxt, n * sizeof(T)));
            }
        }
    
        void deallocate(pointer p, size_type n)
        {
            pfree(p);
        }
    
        void construct(pointer p, const T& val)
        {
            new(static_cast<void*>(p)) T(val);
        }
    
        void construct(pointer p)
        {
            new(static_cast<void*>(p)) T();
        }
    
        void destroy(pointer p)
        {
            p->~T();
        }
    };

    template<typename T, typename U, MemoryContextType type1, MemoryContextType type2>
    inline bool operator == (const MemoryContextAdapterAllocator<T, type1> &,
        const MemoryContextAdapterAllocator<U, type2> &)
    {
        return true;
    }

    template<typename T, typename U, MemoryContextType type1, MemoryContextType type2>
    inline bool operator != (const MemoryContextAdapterAllocator<T, type1> &,
        const MemoryContextAdapterAllocator<U, type2> &)
    {
        return false;
    }
}

#endif /* GS_POLICY_MEMORY_ALLOC_H_ */
