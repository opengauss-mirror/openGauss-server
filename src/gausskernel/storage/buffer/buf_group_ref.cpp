/*
 * Copyright (c) 2026 Huawei Technologies Co.,Ltd.
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
 * buf_group_ref.cpp
 *        Implementation of per-group buffer reference counting.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/buffer/buf_group_ref.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "miscadmin.h"
#include "storage/buf/buf_group_ref.h"
#include "storage/buf/buf_internals.h"
#include "storage/ipc.h"
#include "threadpool/threadpool_group.h"
#include "threadpool/threadpool_controler.h"
#ifdef __USE_NUMA
#include "numa.h"
#include "utils/matrix_adaptor.h"
#endif

/* Global per-group buffer reference counts */
GroupRefCounts g_group_ref_counts = {NULL, 0, 0};

#ifdef __USE_NUMA
static void InitGroupRefCountsOnNuma(int32 num_groups, int32 num_buffers);
static void DestroyGroupRefCounts(int code, Datum arg);
#endif

void InitWorkerCPUGroup(int32 group)
{
    if (g_group_ref_counts.initialized) {
        Assert(group >= 0 && group < g_group_ref_counts.num_groups);
        t_thrd.storage_cxt.cached_group_ref_counts = g_group_ref_counts.counts[group];
    }
}

/*
 * Initialize per-group buffer reference counts.
 * This function should be called after buffer pool initialization.
 * Each group's data is allocated on its corresponding NUMA node to minimize
 * cross-NUMA access during Pin/Unpin operations.
 */
void InitGroupRefCounts(int32 num_buffers)
{
    /* Only initialize if feature is enabled and not already initialized */
    if (!g_instance.attr.attr_memory.enable_group_ref_cnt || g_group_ref_counts.initialized) {
        return;
    }

#ifndef __aarch64__
    elog(WARNING, "Group ref cnt only supported on ARM64, disabling");
    g_instance.attr.attr_memory.enable_group_ref_cnt = false;
    return;
#endif

#ifndef __USE_NUMA
    elog(WARNING, "Group ref cnt requires NUMA support, disabling");
    g_instance.attr.attr_memory.enable_group_ref_cnt = false;
#else
    if (g_instance.attr.attr_storage.enable_adio_function || ENABLE_DSS) {
        elog(WARNING, "Group ref cnt is incompatible with ADIO or DSS, disabling");
        g_instance.attr.attr_memory.enable_group_ref_cnt = false;
        return;
    }

    int32 num_groups = MatrixMaxNumaNode();
    if (num_groups <= 1 ||
        g_threadPoolControler == NULL ||
        !g_threadPoolControler->CheckNumaDistribute(num_groups)) {
        elog(WARNING, "Group ref cnt requires valid thread pool NUMA binding, disabling");
        g_instance.attr.attr_memory.enable_group_ref_cnt = false;
        return;
    }

    InitGroupRefCountsOnNuma(num_groups, num_buffers);
#endif
}

#ifdef __USE_NUMA
static void InitGroupRefCountsOnNuma(int32 num_groups, int32 num_buffers)
{
    size_t group_buf_size = num_buffers * sizeof(pg_atomic_uint16);

    /* Allocate 2D array: [group_id][buf_id] */
    g_group_ref_counts.counts = (pg_atomic_uint16**)MemoryContextAllocZero(
        INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), (Size)num_groups * sizeof(pg_atomic_uint16*));
    g_group_ref_counts.num_groups = num_groups;
    g_group_ref_counts.num_buffers = num_buffers;

    for (int32 group = 0; group < num_groups; group++) {
        /* Use numa_alloc_onnode to allocate memory on specific NUMA node */
        g_group_ref_counts.counts[group] = (pg_atomic_uint16*)numa_alloc_onnode(
            group_buf_size,
            group
        );

        if (!g_group_ref_counts.counts[group]) {
            elog(WARNING, "Failed to allocate group %d ref counts on NUMA node %d",
                  group, group);
            g_instance.attr.attr_memory.enable_group_ref_cnt = false;
            DestroyGroupRefCounts(0, 0);
            return;
        }

        /* Initialize all counters to 0 */
        for (int32 buf = 0; buf < num_buffers; buf++) {
            pg_atomic_init_u16(&g_group_ref_counts.counts[group][buf], 0);
        }
    }

    g_group_ref_counts.initialized = true;
    InitWorkerCPUGroup();
    on_shmem_exit(DestroyGroupRefCounts, 0);

    elog(LOG, "Group ref counts initialized: %d groups, %d buffers",
          num_groups, num_buffers);
}
#endif

#ifdef __USE_NUMA
/*
 * Destroy per-group buffer reference counts.
 */
static void DestroyGroupRefCounts(int code, Datum arg)
{
    size_t group_buf_size = g_group_ref_counts.num_buffers * sizeof(pg_atomic_uint16);

    if (g_group_ref_counts.counts != NULL) {
        for (int32 group = 0; group < g_group_ref_counts.num_groups; group++) {
            if (g_group_ref_counts.counts[group]) {
                /* Use numa_free since we allocated with numa_alloc_onnode */
                numa_free((void*)g_group_ref_counts.counts[group], group_buf_size);
            }
        }
        pfree(g_group_ref_counts.counts);
    }

    g_group_ref_counts.counts = NULL;
    g_group_ref_counts.num_groups = 0;
    g_group_ref_counts.num_buffers = 0;
    g_group_ref_counts.initialized = false;
}
#endif

/*
 * Helper function to get buffer's effective refcount.
 * In group mode, it sums all group counters; otherwise uses global state.
 */
uint32 GetBufferRefCount(uint64 buf_state, int32 buf_id)
{
    if (g_group_ref_counts.initialized && IsNormalBufferID(buf_id)) {
        /* Sum all group counters for this buffer, must use uint16, to deal with: 1 + 65535(-1) = 0 */
        uint16 total_ref = 0;
        for (int32 group = 0; group < g_group_ref_counts.num_groups; group++) {
            total_ref += pg_atomic_read_u16(&g_group_ref_counts.counts[group][buf_id]);
        }
        return total_ref;
    }

    /* NVM and segment buffers continue to use the state refcount. */
    return BUF_STATE_GET_REFCOUNT(buf_state);
}

bool IsBufferRefCountZero(uint64 buf_state, int32 buf_id)
{
    return GetBufferRefCount(buf_state, buf_id) == 0;
}

bool IsBufferRefCountOne(uint64 buf_state, int32 buf_id)
{
    return GetBufferRefCount(buf_state, buf_id) == 1;
}

bool IsBufferRefCountGreaterThanZero(uint64 buf_state, int32 buf_id)
{
    return GetBufferRefCount(buf_state, buf_id) > 0;
}

bool IsBufferRefCountNotZero(uint64 buf_state, int32 buf_id)
{
    return GetBufferRefCount(buf_state, buf_id) != 0;
}