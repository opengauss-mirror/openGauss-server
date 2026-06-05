/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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
 * --------------------------------------------------------------------------------------
 *
 * gv_allocator_impl.h
 *
 * Portions Copyright (c) 2026, Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2020, AWS
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *        contrib/gv_index/env/gv_allocator_impl.h
 *
 * --------------------------------------------------------------------------------------
 */

#ifndef GVGRAPH_ALLOCATOR_IMPL_H
#define GVGRAPH_ALLOCATOR_IMPL_H

#include <cstdint>
#include <new>
#include <memory>
#include <mutex>
#include <thread>
#include <limits>
#include <cstdlib>
#include "c.h"
#include "nodes/memnodes.h"
#include "utils/palloc.h"

#include "adaptor/shared_allocator.h"
#include "lite/index/light_env/allocator.h"

namespace gs_vector {

struct GVAllocatorImpl : public annlite::light_env::Allocator {
    GVAllocatorImpl() noexcept : m_cxt(CurrentMemoryContext) {}
    GVAllocatorImpl(MemoryContext cxt) noexcept : m_cxt(cxt) {}
    static constexpr size_t byte_to_gb = 1ULL << 30;
    bool operator==(const GVAllocatorImpl& other) const
    {
        return context() == other.context();
    }
    void *allocate(const size_t bytes) override
    {
        Assert(bytes < byte_to_gb);
        return palloc(bytes);
    }

    void deallocate(void *const ptr, size_t bytes) noexcept override
    {
        if (ptr != nullptr) {
            Assert(((AllocSet)m_cxt)->totalSpace >= bytes || bytes == 0);
            pfree(ptr);
        }
        (void)bytes;
    }

    void *reallocate(void *ptr, size_t old_bytes, size_t bytes) override
    {
        (void)old_bytes;
        return repalloc(ptr, bytes);
    }
    template <typename T, typename... Args>
    T *construct(Args &&...args)
    {
        T *data = (T *)annotate_allocate_internal(sizeof(T), __FILE__, __LINE__);
        construct_internal<T>(data, std::forward<Args>(args)...);
        return data;
    }
    template <typename T>
    void destroy(T *ptr)
    {
        if constexpr (!std::is_pod_v<T>) {
            ptr->~T();
        }
        deallocate(ptr, sizeof(T));
    }
    size_t allocated_memory_bytes()
    {
        return ((AllocSet)m_cxt)->totalSpace;
    }

private:  // adaptor for BaseZeroObject::new
    template <class U, class... Args>
    void construct_internal(U *p, Args &&...args)
    {
        ::new ((void *)p) U(std::forward<Args>(args)...);
    }

    MemoryContext context() const
    {
        return m_cxt;
    }

    void *annotate_allocate_internal(size_t size, const char *file, int line)
    {
        return palloc(size);
    }

private:
    MemoryContext m_cxt;
};

}  // namespace gs_vector

#endif // GVGRAPH_ALLOCATOR_IMPL_H