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
 * object_pool_compact.cpp
 *    Object pool compaction interfaces.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/memory/object_pool_compact.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "object_pool_compact.h"

namespace MOT {
static constexpr addrMap_t::size_type ADDR_MAP_SIZE = 1024;

IMPLEMENT_CLASS_LOGGER(CompactHandler, System)

// Class: CompactHandler implementation
CompactHandler::CompactHandler(ObjAllocInterface* pool, const char* prefix)
    : m_orig(pool),
      m_compactionNeeded(false),
      m_ctype(COMPACT_SIMPLE),
      m_addrMap(ADDR_MAP_SIZE, hashing_func(), key_equal_fn()),
      m_poolsToCompact(0),
      m_compactedPools(nullptr),
      m_curr(nullptr),
      m_logPrefix(prefix)
{}

void CompactHandler::StartCompaction(CompactTypeT type)
{
    PoolStatsSt stats;
    errno_t erc = memset_s(&stats, sizeof(PoolStatsSt), 0, sizeof(PoolStatsSt));
    securec_check(erc, "\0", "\0");
    stats.m_type = PoolStatsT::POOL_STATS_ALL;

    m_ctype = type;
    m_orig->GetStats(stats);
    m_orig->PrintStats(stats, m_logPrefix, LogLevel::LL_INFO);

    if (stats.m_fragmentationPercent <= 0 && stats.m_freeObjCount < stats.m_perPoolTotalCount) {
        m_compactionNeeded = false;
        return;
    }

    m_compactionNeeded = true;
    // get pools with empty spaces
    ObjPoolPtr p = nullptr;
    do {
        m_poolsToCompact = m_orig->m_nextFree;
    } while (!CAS(m_orig->m_nextFree, m_poolsToCompact, p));

    p = m_poolsToCompact;
    while (p.Get() != nullptr) {
        ObjPool* op = p.Get();
        if (p->m_freeCount < p->m_totalCount)
            m_addrMap[op] = op;
        p = p->m_objNext;
    }
}

void CompactHandler::EndCompaction()
{
    if (!m_compactionNeeded)
        return;

    if (m_compactedPools != nullptr) {
        ObjPool* p = m_compactedPools;
        ObjPool* tail = nullptr;
        while (p != nullptr) {
            tail = p;
            p = p->m_next;
        }

        // re-insert to global pool list
        do {
            tail->m_next = m_orig->m_objList;
        } while (!CAS(m_orig->m_objList, tail->m_next, m_compactedPools));
        if (tail->m_next != nullptr) {
            tail->m_next->m_prev = tail;
        }

        m_compactedPools = nullptr;
    }

    // add current object pool to free list
    if (m_curr != nullptr) {
        ADD_TO_LIST(m_orig->m_listLock, m_orig->m_objList, m_curr);
        if (m_curr->m_freeCount < m_curr->m_totalCount) {
            ObjPoolPtr p = m_curr;
            PUSH(m_orig->m_nextFree, p);
            m_curr = nullptr;
        }
    }

    // release compacted pools
    if (m_poolsToCompact.Get() != nullptr) {
        ObjPoolPtr p = m_poolsToCompact;
        while (p.Get() != nullptr) {
            ObjPoolPtr tmp = p->m_objNext;
            if (p->m_freeCount == p->m_totalCount) {
                ObjPool* op = p.Get();
                DEL_FROM_LIST(m_orig->m_listLock, m_orig->m_objList, op);
                ObjPool::DelObjPool(op, m_orig->m_type, true);
            } else {
                if (m_ctype != COMPACT_SIMPLE)
                    MOT_LOG_ERROR("Compaction error: pool not empty, re-inserting to free pools");
                PUSH(m_orig->m_nextFree, p);
            }
            p = tmp;
        }
    }

    m_orig->Print(m_logPrefix, LogLevel::LL_INFO);

    m_compactionNeeded = false;
}
}  // namespace MOT
