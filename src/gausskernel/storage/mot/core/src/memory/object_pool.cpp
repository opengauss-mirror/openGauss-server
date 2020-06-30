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
 * object_pool.cpp
 *    Object pool implementation.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/src/memory/object_pool.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "object_pool.h"

namespace MOT {
IMPLEMENT_CLASS_LOGGER(ObjAllocInterface, Memory)
IMPLEMENT_CLASS_LOGGER(SlabAllocator, Memory)
IMPLEMENT_CLASS_LOGGER(ObjPool, Memory)

ObjPoolPtr::ObjPoolPtr(ObjPool* target)
{
    m_data.m_ptr = target;
    if (target != NULL)
        m_data.m_slice[LIST_PTR_SLICE_IX] = target->m_listCounter;
}

ObjPoolPtr& ObjPoolPtr::operator=(ObjPool* right)
{
    m_data.m_ptr = right;
    if (right != NULL)
        m_data.m_slice[LIST_PTR_SLICE_IX] = right->m_listCounter;
    return *this;
}

ObjAllocInterface* ObjAllocInterface::GetObjPool(uint16_t size, bool local, uint8_t align)
{
    ObjAllocInterface* result = NULL;
    if (local) {
        result = new (std::nothrow) LocalObjPool(size, align);
    } else {
        result = new (std::nothrow) GlobalObjPool(size, align);
    }

    if (result == NULL) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Allocate Object Pool",
            "Failed to allocate memory for %s object pool",
            local ? "local" : "global");
    } else if (!result->Initialize()) {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
            "Allocate Object Pool",
            "Failed to pre-allocate memory for %s object pool",
            local ? "local" : "global");
        delete result;
        result = NULL;
    }
    return result;
}

void ObjAllocInterface::FreeObjPool(ObjAllocInterface** pool)
{
    if (pool != NULL && *pool != NULL) {
        delete *pool;
        *pool = NULL;
    }
}

ObjAllocInterface::~ObjAllocInterface()
{
    ObjPool* p = m_objList;
    while (p != NULL) {
        ObjPool* tmp = p;
        p = p->m_next;
        ObjPool::DelObjPool(tmp, m_type, m_global);
    }
}

inline MemBufferClass ObjAllocInterface::CalcBufferClass(uint16_t size)
{
    int pool_size = sizeof(ObjPool) + NUM_OBJS * size;
    // 1KB has 11 bit set (starting from less significant)
    // we use 32 bit int for size, so first 1 bit will 32 - __builtin_clz(pool_size)
    // the buffer classes start from 1KB
    return MemBufferClassLowerBound(pool_size);
}

void ObjAllocInterface::GetStats(PoolStatsSt& stats)
{
    ObjPoolPtr p;

    if (stats.m_type == PoolStatsT::POOL_STATS_ALL)
        p = m_objList;
    else
        p = m_nextFree;

    stats.m_objSize = m_size;
    stats.m_poolGrossSize = (1024 * MemBufferClassToSizeKb(m_type));

    if (p.Get()) {
        stats.m_perPoolTotalCount = p->m_totalCount;
        stats.m_perPoolOverhead = p->m_overheadBytes;
        stats.m_perPoolWaist = p->m_notUsedBytes;
    }
    while (p.Get() != NULL) {
        stats.m_poolCount++;
        if (p->m_totalCount == p->m_freeCount)
            stats.m_poolFreeCount++;
        stats.m_totalObjCount += p->m_totalCount;
        stats.m_freeObjCount += p->m_freeCount;
        if (stats.m_type == PoolStatsT::POOL_STATS_ALL)
            p = p->m_next;
        else
            p = p->m_objNext;
    }

    if (stats.m_poolCount > 0)
        stats.m_fragmentationPercent = (int16_t)(stats.m_poolFreeCount * 100 / stats.m_poolCount);
    else
        stats.m_fragmentationPercent = 0;
}

void ObjAllocInterface::PrintStats(PoolStatsSt& stats, const char* prefix, LogLevel level)
{
    const char* hist_str = "";

    MOT_LOG(level,
        "%s: type: %d, size: %d, pools: %u(%u), total objects: %lu, free objects: %lu"
        ", overhead: %lu, waist: %lu"
        "\n%s",
        prefix,
        m_type,
        m_size,
        stats.m_poolCount,
        stats.m_poolFreeCount,
        stats.m_totalObjCount,
        stats.m_freeObjCount,
        stats.m_poolCount * stats.m_perPoolOverhead,
        stats.m_poolCount * stats.m_perPoolWaist,
        hist_str);
}

void ObjAllocInterface::Print(const char* prefix, LogLevel level)
{
    PoolStatsSt stats;

    errno_t erc = memset_s(&stats, sizeof(PoolStatsSt), 0, sizeof(PoolStatsSt));
    securec_check(erc, "\0", "\0");
    stats.m_type = PoolStatsT::POOL_STATS_ALL;

    GetStats(stats);
    PrintStats(stats, prefix, level);
}

PoolStatsSt* SlabAllocator::GetStats()
{
    PoolStatsSt* stats = (PoolStatsSt*)calloc(SLUB_MAX_BIN + 1, sizeof(PoolStatsSt));

    if (stats == NULL) {
        MOT_LOG_ERROR("Failed to allocate memory for stats.");
        return NULL;
    }

    for (int i = 0; i <= SLUB_MAX_BIN; i++) {
        if (m_bins[i] != NULL) {
            stats[i].m_type = PoolStatsT::POOL_STATS_ALL;
            m_bins[i]->GetStats(stats[i]);
        }
    }

    return stats;
}

void SlabAllocator::FreeStats(PoolStatsSt* stats)
{
    if (stats != NULL)
        free(stats);
}

void SlabAllocator::GetSize(uint64_t& size, uint64_t& netto)
{
    PoolStatsSt* stats = GetStats();

    if (stats == NULL)
        return;

    for (int i = 0; i <= SLUB_MAX_BIN; i++) {
        if (m_bins[i] != NULL) {
            size += stats[i].m_poolCount * stats[i].m_poolGrossSize;
            netto += (stats[i].m_totalObjCount - stats[i].m_freeObjCount) * stats[i].m_objSize;
        }
    }

    FreeStats(stats);
}

void SlabAllocator::PrintStats(PoolStatsSt* stats, const char* prefix, LogLevel level)
{
    if (stats == NULL)
        return;

    for (int i = 0; i <= SLUB_MAX_BIN; i++) {
        if (m_bins[i] != NULL) {
            m_bins[i]->PrintStats(stats[i], prefix, level);
        }
    }
}

void SlabAllocator::Print(const char* prefix, LogLevel level)
{
    PoolStatsSt* stats = GetStats();

    PrintStats(stats, prefix, level);

    FreeStats(stats);
}
}  // namespace MOT
