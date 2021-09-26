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
 *    Object pool interfaces.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/memory/object_pool.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "object_pool.h"

namespace MOT {
IMPLEMENT_CLASS_LOGGER(SlabAllocator, Memory)

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
    if (stats != NULL) {
        free(stats);
    }
}

void SlabAllocator::GetSize(uint64_t& size, uint64_t& netto)
{
    PoolStatsSt* stats = GetStats();

    if (stats == NULL) {
        return;
    }

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
    if (stats == NULL) {
        return;
    }

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
