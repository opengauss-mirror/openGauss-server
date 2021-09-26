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
 * mot_masstree_kvthread.hpp
 *    Replace Masstree's thread info implementations with MOT functionality.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/storage/index/masstree/mot_masstree_kvthread.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "masstree_index.h"
#include "kvthread.hh"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <new>
#include <sys/mman.h>
#if HAVE_SUPERPAGE && !NOSUPERPAGE
#include <sys/types.h>
#include <dirent.h>
#endif

#include "mm_api.h"
#include "mm_gc_manager.h"

// This is the thread info which serves the current masstree operation. It is set before the operation starts.
__thread threadinfo* mtSessionThreadInfo = nullptr;

volatile mrcu_epoch_type globalepoch;

inline threadinfo::threadinfo(int purpose, int index, int rcu_max_free_count)
{
    errno_t erc = memset_s(this, sizeof(*this), 0, sizeof(*this));
    securec_check(erc, "\0", "\0");

    purpose_ = purpose;
    index_ = index;
    rcu_free_count = rcu_max_free_count;

    ts_ = 2;
}

threadinfo* threadinfo::make(void* obj_mem, int purpose, int index, int rcu_max_free_count)
{
    threadinfo* ti = new (obj_mem) threadinfo(purpose, index, rcu_max_free_count);

    if (use_pool()) {
        void* limbo_space = ti->allocate(MAX_MEMTAG_MASSTREE_LIMBO_GROUP_ALLOCATION_SIZE, memtag_limbo);
        if (!limbo_space) {
            return nullptr;
        }

        ti->mark(tc_limbo_slots, mt_limbo_group::capacity);
        ti->limbo_head_ = ti->limbo_tail_ = new (limbo_space) mt_limbo_group;
    }

    return ti;
}

void* threadinfo::allocate(size_t sz, memtag tag, size_t* actual_size)
{
    int size = sz;
    void* p = nullptr;
    if (likely(!use_pool())) {
        p = cur_working_index->AllocateMem(size, tag);
    } else {
        p = malloc(sz + memdebug_size);
    }

    p = memdebug::make(p, sz, tag);
    if (p) {
        if (actual_size) {
            *actual_size = size;
        }
        mark(threadcounter(tc_alloc + (tag > memtag_value)), sz);
    }
    return p;
}

void threadinfo::deallocate(void* p, size_t sz, memtag tag)
{
    MOT_ASSERT(p);
    p = memdebug::check_free(p, sz, tag);
    if (likely(!use_pool())) {
        cur_working_index->DeallocateMem(p, sz, tag);
    } else {
        free(p);
    }
    mark(threadcounter(tc_alloc + (tag > memtag_value)), -sz);
}

void threadinfo::ng_record_rcu(void* p, int sz, memtag tag)
{
    MOT_ASSERT(p);
    memdebug::check_rcu(p, sz, tag);
    cur_working_index->RecordMemRcu(p, sz, tag);
    mark(threadcounter(tc_alloc + (tag > memtag_value)), -sz);
}

void threadinfo::set_gc_session(MOT::GcManager* gc_session)
{
    gc_session_ = gc_session;
}

inline MOT::GcManager* threadinfo::get_gc_session()
{
    return gc_session_;
}
