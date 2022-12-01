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
 * object_pool_impl.cpp
 *    Object pool implementation.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/memory/object_pool_impl.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <unordered_set>

#include "object_pool_impl.h"
#include "object_pool.h"

namespace MOT {
IMPLEMENT_CLASS_LOGGER(ObjAllocInterface, Memory)
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

void ObjAllocInterface::FreeObjPool(ObjAllocInterface** pool) noexcept
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
    m_objList = nullptr;
}

MemBufferClass ObjAllocInterface::CalcBufferClass(uint16_t size)
{
#ifdef ENABLE_MEMORY_CHECK
    int pool_size = sizeof(ObjPool) + NUM_OBJS * MEMCHECK_OBJPOOL_SIZE;
#else
    int pool_size = sizeof(ObjPool) + NUM_OBJS * size;
#endif
    // 1KB has 11 bit set (starting from less significant)
    // we use 32 bit int for size, so first 1 bit will 32 - __builtin_clz(pool_size)
    // the buffer classes start from 1KB
    return MemBufferClassLowerBound(pool_size);
}

size_t ObjAllocInterface::CalcRequiredMemDiff(const PoolStatsSt& stats, size_t newSize, uint8_t align) const
{
    if (stats.m_poolCount == 0) {
        return MIN_CHUNK_REQUIRED;
    }
#ifdef ENABLE_MEMORY_CHECK
    newSize = MEMCHECK_OBJPOOL_SIZE;
#else
    newSize = ALIGN_N(newSize + OBJ_INDEX_SIZE, align);
#endif
    MemBufferClass memType = ObjAllocInterface::CalcBufferClass(newSize);
    uint64_t newPoolGrossSize = K2B((uint64_t)MemBufferClassToSizeKb(memType));
    uint16_t newObjCountInPool = (uint16_t)((newPoolGrossSize - sizeof(ObjPool)) / newSize);
    uint16_t oldObjCountInPool = stats.m_totalObjCount / stats.m_poolCount;
    uint32_t newPoolCount = ((stats.m_totalObjCount - stats.m_freeObjCount) / newObjCountInPool) + 1;
    uint32_t newNumOf2MBChunks = (newPoolCount * newPoolGrossSize) / MEM_CHUNK_SIZE_BYTES + 1;
    uint32_t oldNumOf2MBChunks =
        ((stats.m_poolCount - stats.m_poolFreeCount) * stats.m_poolGrossSize) / MEM_CHUNK_SIZE_BYTES + 1;
    uint32_t extraChunks = MIN_CHUNK_REQUIRED;

    MOT_LOG_INFO("old pools %lu:%lu:%lu, new pools %lu:%lu:%lu chunks %u:%u actual chunks used %u",
        stats.m_poolCount,
        stats.m_poolGrossSize,
        oldObjCountInPool,
        newPoolCount,
        newPoolGrossSize,
        newObjCountInPool,
        oldNumOf2MBChunks,
        newNumOf2MBChunks,
        stats.m_usedChunkCount);
    if (oldNumOf2MBChunks < stats.m_usedChunkCount) {
        extraChunks += stats.m_usedChunkCount - oldNumOf2MBChunks;
    }
    if (newNumOf2MBChunks > oldNumOf2MBChunks) {
        extraChunks += (newNumOf2MBChunks - oldNumOf2MBChunks);
    }
    return extraChunks;
}

void ObjAllocInterface::GetStats(PoolStatsSt& stats)
{
    ObjPoolPtr p;

    if (stats.m_type == PoolStatsT::POOL_STATS_ALL) {
        p = m_objList;
    } else {
        p = m_nextFree;
    }

    stats.m_objSize = m_size;
    stats.m_poolGrossSize = (1024 * MemBufferClassToSizeKb(m_type));

    if (p.Get()) {
        stats.m_perPoolTotalCount = p->m_totalCount;
        stats.m_perPoolOverhead = p->m_overheadBytes;
        stats.m_perPoolWaste = p->m_notUsedBytes;
    }
    while (p.Get() != NULL) {
        stats.m_poolCount++;
        if (p->m_totalCount == p->m_freeCount) {
            stats.m_poolFreeCount++;
        }
        stats.m_totalObjCount += p->m_totalCount;
        stats.m_freeObjCount += p->m_freeCount;
        if (stats.m_type == PoolStatsT::POOL_STATS_ALL) {
            p = p->m_next;
        } else {
            p = p->m_objNext;
        }
    }

    if (stats.m_poolCount > 0) {
        stats.m_fragmentationPercent = (int16_t)(stats.m_poolFreeCount * 100 / stats.m_poolCount);
    } else {
        stats.m_fragmentationPercent = 0;
    }
}

void ObjAllocInterface::GetStatsEx(PoolStatsSt& stats, ObjPoolAddrSet_t* poolSet)
{
    ObjPoolPtr p;
    std::unordered_set<uint64_t> chunkIdSet;
    std::pair<std::unordered_set<uint64_t>::iterator, bool> ret;
    p = m_objList;
    stats.m_objSize = m_size;
    stats.m_poolGrossSize = (1024 * MemBufferClassToSizeKb(m_type));

    if (p.Get()) {
        stats.m_perPoolTotalCount = p->m_totalCount;
        stats.m_perPoolOverhead = p->m_overheadBytes;
        stats.m_perPoolWaste = p->m_notUsedBytes;
    }
    while (p.Get() != NULL) {
        uint64_t chunkId = (uint64_t)MemRawChunkDirLookup(p.Get());
        ret = chunkIdSet.insert(chunkId);
        if (ret.second == true) {
            stats.m_usedChunkCount++;
        }
        if (poolSet != nullptr) {
            (void)poolSet->insert(p.Get());
        }
        stats.m_poolCount++;
        if (p->m_totalCount == p->m_freeCount) {
            stats.m_poolFreeCount++;
        }
        stats.m_totalObjCount += p->m_totalCount;
        stats.m_freeObjCount += p->m_freeCount;
        p = p->m_next;
    }

    if (stats.m_poolCount > 0) {
        stats.m_fragmentationPercent = (int16_t)(stats.m_poolFreeCount * 100 / stats.m_poolCount);
    } else {
        stats.m_fragmentationPercent = 0;
    }
}

void ObjAllocInterface::PrintStats(const PoolStatsSt& stats, const char* prefix, LogLevel level) const
{
    const char* hist_str = "";

    MOT_LOG(level,
        "%s: type: %d, size: %d, pools: %u(%u), total objects: %lu, free objects: %lu"
        ", overhead: %lu, waste: %lu"
        "\n%s",
        prefix,
        m_type,
        m_size,
        stats.m_poolCount,
        stats.m_poolFreeCount,
        stats.m_totalObjCount,
        stats.m_freeObjCount,
        stats.m_poolCount * stats.m_perPoolOverhead,
        stats.m_poolCount * stats.m_perPoolWaste,
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

ObjPool::~ObjPool()
{
#ifdef ENABLE_MEMORY_CHECK
    ObjPoolItr itr(this);
    while (itr.Next() != nullptr) {
        itr.ReleaseCurrent();
    }
#endif
    m_next = nullptr;
    m_prev = nullptr;
    m_parent = nullptr;
}

ObjPool* ObjPool::GetObjPool(uint16_t size, ObjAllocInterface* app, MemBufferClass type, bool global)
{
#ifdef TEST_STAT_ALLOC
    uint64_t start_time = GetSysClock();
#endif
#ifndef MEM_ACTIVE
    void* p = (void*)malloc((1024 * MemBufferClassToSizeKb(type)));
#else
    void* p;

    if (global) {
        p = MemBufferAllocGlobal(type);
    } else {
#ifdef MEM_SESSION_ACTIVE
        uint32_t bufferSize = 1024 * MemBufferClassToSizeKb(type);
        p = MemSessionAlloc(bufferSize);
        if (p) {
            DetailedMemoryStatisticsProvider::GetInstance().AddLocalBuffersUsed(MOTCurrentNumaNodeId, type);
        }
#else
        p = MemBufferAllocLocal(type);
#endif  // MEM_SESSION_ACTIVE
    }
#endif  // MEM_ACTIVE
#ifdef TEST_STAT_ALLOC
    uint64_t end_time = GetSysClock();
    MemoryStatisticsProvider::GetInstance().AddMallocTime(
        CpuCyclesLevelTime::CyclesToNanoseconds(end_time - start_time));
#endif
    ObjPool* o = nullptr;
    if (p) {
        o = new (p) ObjPool(size, type, app);
    } else {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "N/A",
            "Failed to allocate %s %s buffer for object pool",
            MemBufferClassToString(type),
            global ? "global" : "local");
    }
    return o;
}

void ObjPool::DelObjPool(void* ptr, MemBufferClass type, bool global)
{
    ((ObjPool*)ptr)->~ObjPool();
#ifdef TEST_STAT_ALLOC
    uint64_t start_time = GetSysClock();
#endif
#ifndef MEM_ACTIVE
    free(ptr);
#else
    if (global) {
        MemBufferFreeGlobal(ptr, type);
    } else {
#ifdef MEM_SESSION_ACTIVE
        MemSessionFree(ptr);
        DetailedMemoryStatisticsProvider::GetInstance().AddLocalBuffersFreed(MOTCurrentNumaNodeId, type);
#else
        MemBufferFreeLocal(ptr, type);
#endif  // MEM_SESSION_ACTIVE
    }
#endif  // MEM_ACTIVE
#ifdef TEST_STAT_ALLOC
    uint64_t end_time = GetSysClock();
    MemoryStatisticsProvider::GetInstance().AddFreeTime(CpuCyclesLevelTime::CyclesToNanoseconds(end_time - start_time));
#endif
}

#ifdef ENABLE_MEMORY_CHECK
void ObjPool::AllocForMemCheck(void** ret)
{
    uint8_t* tmp = (uint8_t*)(*ret);
    *ret = malloc(m_parent->m_actualSize);
    if (*ret == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "N/A", "Failed to allocate actual memory");
        MOTAbort();
    }
    *(uint64_t*)(*ret) = (uint64_t)tmp;
    *(uint64_t*)tmp = (uint64_t)(uint8_t*)(*ret);
    tmp = (uint8_t*)*ret;
    tmp += MEMCHECK_METAINFO_SIZE;
    *ret = tmp;
}
#endif
}  // namespace MOT
