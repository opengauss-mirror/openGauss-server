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
 * masstree_index.cpp
 *    Primary index implementation using Masstree.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/storage/index/masstree_index.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "masstree_index.h"
#include "mot_engine.h"

namespace MOT {
typedef MasstreePrimaryIndex::IndexImpl PrimaryMasstree;
IMPLEMENT_TEMPLATE_LOGGER(PrimaryMasstree, Storage)

IMPLEMENT_CLASS_LOGGER(MasstreePrimaryIndex, Storage);

RC MasstreePrimaryIndex::IndexInitImpl(void** args)
{
    if (!InitPools()) {
        DestroyPools();
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Initialize Index", "Failed to initialize masstree pools");
        return RC_MEMORY_ALLOCATION_ERROR;
    }

    // Operation might allocate memory. therefore, we need to set the index so it can use it's pools
    mtSessionThreadInfo->set_working_index((MasstreePrimaryIndex*)this);
    mtSessionThreadInfo->set_gc_session(
        MOTEngine::GetInstance()->GetCurrentGcSession());  // set current GC session in thread-pooled envelope

    RC rc = m_index.init(m_keyLength, m_name) ? RC_OK : RC_ERROR;

    mtSessionThreadInfo->set_gc_session(NULL);
    mtSessionThreadInfo->set_working_index(NULL);

    if (rc != RC_OK) {
        DestroyPools();
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL, "Initialize Index", "Failed to initialize index");
        return rc;
    }

    m_initialized = true;
    return rc;
}

Sentinel* MasstreePrimaryIndex::IndexInsertImpl(const Key* key, Sentinel* sentinel, bool& inserted, uint32_t pid)
{
    Sentinel* result = nullptr;
    void* existingItem = nullptr;

    // Operation might allocate memory. therefore, we need to set the index so it can use it's pools
    mtSessionThreadInfo->set_working_index((MasstreePrimaryIndex*)this);
    mtSessionThreadInfo->set_gc_session(
        MOTEngine::GetInstance()->GetCurrentGcSession());  // set current GC session in thread-pooled envelope

    mtSessionThreadInfo->set_last_error(MT_MERR_OK);

    existingItem = m_index.insert(key, sentinel, inserted, pid);

    mtSessionThreadInfo->set_gc_session(NULL);
    mtSessionThreadInfo->set_working_index(NULL);

    if (!inserted && existingItem) {  // key mapping already exists in unique index
        result = reinterpret_cast<Sentinel*>(existingItem);
    }  // otherwise return null pointer (if !inserted && !existingItem, Key does not exist and insertation failed due to
       // memory issue)

    return result;
}

Sentinel* MasstreePrimaryIndex::IndexReadImpl(const Key* key, uint32_t pid) const
{
    Sentinel* sentinel = nullptr;
    bool result = false;
    void* output = nullptr;

    // Operation does not allocate memory from pools nor remove nodes. No need to set index's ptr
    m_index.find(key, output, result, pid);

    if (result) {
        sentinel = reinterpret_cast<Sentinel*>(output);
    }

    return sentinel;
}

Sentinel* MasstreePrimaryIndex::IndexRemoveImpl(const Key* key, uint32_t pid)
{
    bool result = false;
    void* output = nullptr;
    Sentinel* sentinel = nullptr;

    // Operation might allocate memory or remove nodes. therefore, we need to set the index so it can use it's pools
    mtSessionThreadInfo->set_working_index((MasstreePrimaryIndex*)this);
    mtSessionThreadInfo->set_gc_session(
        MOTEngine::GetInstance()->GetCurrentGcSession());  // set current GC session in thread-pooled envelope

    mtSessionThreadInfo->set_last_error(MT_MERR_OK);

    output = m_index.remove(key->GetKeyBuf(), key->GetKeyLength(), result, pid);

    mtSessionThreadInfo->set_gc_session(NULL);
    mtSessionThreadInfo->set_working_index(NULL);

    if (result) {
        sentinel = reinterpret_cast<Sentinel*>(output);
    }

    return sentinel;
}

uint64_t MasstreePrimaryIndex::GetIndexSize()
{
    PoolStatsSt stats;

    errno_t erc = memset_s(&stats, sizeof(PoolStatsSt), 0, sizeof(PoolStatsSt));
    securec_check(erc, "\0", "\0");
    stats.m_type = PoolStatsT::POOL_STATS_ALL;
    m_keyPool->GetStats(stats);
    uint64_t res = stats.m_poolCount * stats.m_poolGrossSize;
    uint64_t netto = (stats.m_totalObjCount - stats.m_freeObjCount) * stats.m_objSize;

    erc = memset_s(&stats, sizeof(PoolStatsSt), 0, sizeof(PoolStatsSt));
    securec_check(erc, "\0", "\0");
    stats.m_type = PoolStatsT::POOL_STATS_ALL;
    m_sentinelPool->GetStats(stats);
    res += stats.m_poolCount * stats.m_poolGrossSize;
    netto += (stats.m_totalObjCount - stats.m_freeObjCount) * stats.m_objSize;

    erc = memset_s(&stats, sizeof(PoolStatsSt), 0, sizeof(PoolStatsSt));
    securec_check(erc, "\0", "\0");
    stats.m_type = PoolStatsT::POOL_STATS_ALL;
    m_leafsPool->GetStats(stats);
    res += stats.m_poolCount * stats.m_poolGrossSize;
    netto += (stats.m_totalObjCount - stats.m_freeObjCount) * stats.m_objSize;

    erc = memset_s(&stats, sizeof(PoolStatsSt), 0, sizeof(PoolStatsSt));
    securec_check(erc, "\0", "\0");
    stats.m_type = PoolStatsT::POOL_STATS_ALL;
    m_internodesPool->GetStats(stats);
    res += stats.m_poolCount * stats.m_poolGrossSize;
    netto += (stats.m_totalObjCount - stats.m_freeObjCount) * stats.m_objSize;

    m_ksuffixSlab->GetSize(res, netto);

    MOT_LOG_INFO("Index %s memory size: gross: %lu, netto: %lu", m_name.c_str(), res, netto);
    return res;
}

// Iterator API
IndexIterator* MasstreePrimaryIndex::Begin(uint32_t pid, bool passive) const
{
    bool result = false;
    const char* minKey = nullptr;

    return Search(minKey, /* search key */
        0,                /* key size. Ignored if key is null */
        true,             /* match key*/
        true,             /* Forward */
        pid,              /* pid */
        result,           /* found */
        passive);
}

IndexIterator* MasstreePrimaryIndex::Search(
    const Key* key, bool matchKey, bool forward, uint32_t pid, bool& found, bool passive) const
{
    return Search(reinterpret_cast<char const*>(key->GetKeyBuf()),
        ALIGN8(key->GetKeyLength()),
        matchKey,
        forward,
        pid,
        found,
        passive);
}

IndexIterator* MasstreePrimaryIndex::Search(
    char const* keybuf, uint32_t keylen, bool matchKey, bool forward, uint32_t pid, bool& found, bool passive) const
{
    IndexIterator* itr = nullptr;

    // Operation does not allocate memory from pools nor remove nodes. No need to set index's ptr
    if (forward) {
        IndexImpl::ForwardIterator* itrImpl = new (std::nothrow) IndexImpl::ForwardIterator;

        if (!itrImpl) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Index Search", "Failed to create iterator");
            return nullptr;
        }

        const_cast<IndexImpl&>(m_index).iteratorScan(keybuf, keylen, matchKey, itrImpl, true, found, pid);
        itr = new (std::nothrow) MTIterator<IteratorType::ITERATOR_TYPE_FORWARD>(itrImpl);

        if (!itr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Index Search", "Failed to create forward iterator");
            delete itrImpl;
            return nullptr;
        }
    } else {
        IndexImpl::ReverseIterator* itr_impl = new (std::nothrow) IndexImpl::ReverseIterator;

        if (!itr_impl) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Index Search", "Failed to create iterator");
            return nullptr;
        }

        const_cast<IndexImpl&>(m_index).iteratorScan(keybuf, keylen, matchKey, itr_impl, false, found, pid);
        itr = new (std::nothrow) MTIterator<IteratorType::ITERATOR_TYPE_REVERSE>(itr_impl);

        if (!itr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Index Search", "Failed to create reverse iterator");
            delete itr_impl;
            return nullptr;
        }
    }

    return itr;
}

GcManager* MasstreePrimaryIndex::GetCurrentGcSession()
{
    return MOTEngine::GetInstance()->GetCurrentGcSession();
}
}  // namespace MOT
