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
 * index.cpp
 *    Base class for primary and secondary index.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/storage/index/index.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "index.h"
#include "index_defs.h"
#include "row.h"
#include "sentinel.h"
#include "object_pool_compact.h"
#include "mot_configuration.h"

namespace MOT {
IMPLEMENT_CLASS_LOGGER(Index, Storage);

std::atomic<uint32_t> MOT::Index::m_indexCounter(0);

uint64_t Index::GetSize() const
{
    // by default not implemented
    return 0;
}

IndexIterator* Index::ReverseBegin(uint32_t pid) const
{
    return nullptr;
}

IndexIterator* Index::Find(const Key* key, uint32_t pid) const
{
    bool found = false;
    IndexIterator* itr = Search(key, true, true, pid, found);
    if (!found) {
        itr->Invalidate();
    }
    return itr;
}

IndexIterator* Index::FindLast(const Key* key, uint32_t pid) const
{
    IndexIterator* itr = Find(key, pid);
    if (itr != nullptr && itr->IsValid()) {
        while (itr->IsValid() && (key == itr->GetKey())) {
            itr->Next();
        }
        itr->Prev();
    }
    return itr;
}

IndexIterator* Index::ReverseFind(const Key* key, uint32_t pid) const
{
    bool found = false;
    IndexIterator* itr = Search(key, true, false, pid, found);
    if (!found) {
        itr->Invalidate();
    }
    return itr;
}

IndexIterator* Index::ReverseFindLast(const Key* key, uint32_t pid) const
{
    IndexIterator* itr = ReverseFind(key, pid);
    if (itr != nullptr && itr->IsValid()) {
        while (itr->IsValid() && (key == itr->GetKey())) {
            itr->Prev();  // moving forward in the index
        }
        itr->Next();
    }
    return itr;
}

IndexIterator* Index::LowerBound(const Key* key, uint32_t pid) const
{
    bool found = false;
    return Search(key, true, true, pid, found);
}

IndexIterator* Index::UpperBound(const Key* key, uint32_t pid) const
{
    IndexIterator* itr = LowerBound(key, pid);

    while (itr != nullptr && itr->IsValid() && (key == itr->GetKey())) {
        itr->Next();
    }
    return itr;
}

IndexIterator* Index::ReverseLowerBound(const Key* key, uint32_t pid) const
{
    bool found = false;
    return Search(key, true, false, pid, found);
}

IndexIterator* Index::ReverseUpperBound(const Key* key, uint32_t pid) const
{
    IndexIterator* itr = ReverseLowerBound(key, pid);

    while (itr != nullptr && itr->IsValid() && (key == itr->GetKey())) {
        itr->Prev();
    }
    return itr;
}

RC Index::IndexInitImpl(void** args)
{
    return RC_OK;
}

void Index::BuildKey(Table* table, const Row* row, Key* key)
{
    uint16_t offset = 0;
    uint8_t* data = const_cast<uint8_t*>(row->GetData());
    uint8_t* buf = key->GetKeyBuf();

    errno_t erc = memset_s(buf, m_keyLength, 0, m_keyLength);
    securec_check(erc, "\0", "\0");
    // Need to verify we copy secondary index keys
    if (IsFakePrimary()) {
        uint64_t surrogateprimaryKey = row->GetSurrogateKey();
        key->CpKey((uint8_t*)(&surrogateprimaryKey), m_keyLength);
    } else if (row->IsInternalKey()) {
        buf = (const_cast<Row*>(row))->GetInternalKeyBuff(GetIndexOrder());
        key->CpKey(buf, m_keyLength);
    } else {
        for (int i = 0; i < m_numKeyFields; i++) {
            Column* col = table->GetField(m_columnKeyFields[i]);

            if (!col->GetIsCommitted() && row->GetTable() != table) {
                if (col->m_hasDefault) {
                    (void)col->PackKey(buf + offset, col->m_defValue, col->m_defSize);
                } else {
                    erc = memset_s(buf + offset, m_keyLength - offset, 0x00, m_lengthKeyFields[i]);
                    securec_check(erc, "\0", "\0");
                }
            } else if (BITMAP_GET(data, (col->m_id - 1))) {
                uintptr_t val = 0;
                size_t len = 0;

                col->Unpack(data, &val, len);
                (void)col->PackKey(buf + offset, val, len);
            } else {
                MOT_ASSERT((offset + m_lengthKeyFields[i]) <= m_keyLength);
                // NOTE: we should consider different data types and fill NULL value according to a data type
                erc = memset_s(buf + offset, m_keyLength - offset, 0x00, m_lengthKeyFields[i]);
                securec_check(erc, "\0", "\0");
            }
            offset += m_lengthKeyFields[i];
        }
    }

    if (!m_unique) {
        uint64_t rowId = row->GetRowId();
        (void)const_cast<Key*>(key)->FillValue(reinterpret_cast<const uint8_t*>(&rowId),
            NON_UNIQUE_INDEX_SUFFIX_LEN,
            m_keyLength - NON_UNIQUE_INDEX_SUFFIX_LEN);
    }
}

void Index::BuildErrorMsg(Table* table, const Row* row, char* destBuf, size_t len)
{
    errno_t erc;
    uint16_t offset = 0;
    uint8_t* data = const_cast<uint8_t*>(row->GetData());
    if (m_fake) { /* no fields for fake primaries */
        return;
    }

    destBuf[offset++] = '(';

    for (int i = 0; i < m_numKeyFields; i++) {
        Column* col = m_table->GetField(m_columnKeyFields[i]);
        erc = memcpy_s(destBuf + offset, len - offset, col->m_name, col->m_nameLen);
        securec_check(erc, "\0", "\0");
        offset += col->m_nameLen;
        destBuf[offset++] = ',';
    }

    // replace last "," with ")"
    destBuf[offset - 1] = ')';
    destBuf[offset++] = '=';
    destBuf[offset++] = '(';

    for (int i = 0; i < m_numKeyFields; i++) {
        Column* col = m_table->GetField(m_columnKeyFields[i]);

        if (!col->GetIsCommitted() && row->GetTable() != table) {
            if (col->m_hasDefault) {
                offset += col->PrintValue(data, destBuf + offset, len - offset, true);
            } else {
                erc = snprintf_s(destBuf, len - offset, 4, "NULL");
                securec_check_ss(erc, "\0", "\0");
                offset += erc;
            }
        } else if (BITMAP_GET(data, (col->m_id - 1))) {
            offset += col->PrintValue(data, destBuf + offset, len - offset);
        } else {
            erc = snprintf_s(destBuf, len - offset, 4, "NULL");
            securec_check_ss(erc, "\0", "\0");
            offset += erc;
        }
        destBuf[offset++] = ',';
    }

    destBuf[offset - 1] = ')';
    destBuf[offset] = 0;
}

RC Index::Truncate(bool isDrop)
{
    ObjAllocInterface::FreeObjPool(&m_keyPool);
    ObjAllocInterface::FreeObjPool(&m_sentinelPool);
    ObjAllocInterface::FreeObjPool(&m_sSentinelVersionPool);

    RC res = this->ReInitIndex(isDrop);
    if (res != RC_OK) {
        return res;
    }

    if (!isDrop) {
        m_keyPool = ObjAllocInterface::GetObjPool(sizeof(Key) + ALIGN8(m_keyLength), false);
        if (m_keyPool == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM,
                "Truncate Index",
                "Failed to allocate key pool for index %s after truncation",
                m_name.c_str());
            return RC_MEMORY_ALLOCATION_ERROR;
        }
        if (m_indexOrder == IndexOrder::INDEX_ORDER_PRIMARY) {
            m_sentinelPool = ObjAllocInterface::GetObjPool(sizeof(PrimarySentinel), false);
        } else {
            if (m_unique) {
                m_sSentinelVersionPool = ObjAllocInterface::GetObjPool(sizeof(PrimarySentinelNode), false);
                if (m_sSentinelVersionPool == nullptr) {
                    MOT_REPORT_ERROR(MOT_ERROR_OOM,
                        "Truncate Index",
                        "Failed to allocate sentinel objects pool for index %s after truncation",
                        m_name.c_str());
                }
                m_sentinelPool = ObjAllocInterface::GetObjPool(sizeof(SecondarySentinelUnique), false);
            } else {
                m_sentinelPool = ObjAllocInterface::GetObjPool(sizeof(SecondarySentinel), false);
            }
        }
        if (m_sentinelPool == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM,
                "Truncate Index",
                "Failed to allocate sentinel pool for index %s after truncation",
                m_name.c_str());
            return RC_MEMORY_ALLOCATION_ERROR;
        }
    }
    return RC_OK;
}

bool Index::IndexInsert(Sentinel*& outputSentinel, const Key* key, uint32_t pid, RC& rc, bool isRecovery)
{
    bool inserted = false;
    Sentinel* sentinel = SentinelAlloc();
    if (unlikely(sentinel == nullptr)) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Index Insert",
            "Failed to allocate sentinel for index %s during index insert",
            m_name.c_str());
        rc = RC_MEMORY_ALLOCATION_ERROR;
        return false;
    }
    sentinel->Init(this, nullptr);

    MOT_ASSERT(sentinel->GetCounter() == 1);
    sentinel->SetIndexOrder(m_indexOrder);

    bool retryInsert = true;
    while (retryInsert) {
        outputSentinel = IndexInsertImpl(key, sentinel, inserted, pid);
        // sync between rollback/delete and insert
        if (!inserted) {
            if (unlikely(outputSentinel == nullptr)) {
                MOT_REPORT_ERROR(
                    MOT_ERROR_OOM, "Index Insert", "Failed to insert sentinel to index %s", m_name.c_str());
                rc = RC_MEMORY_ALLOCATION_ERROR;
                SentinelRelease(sentinel);
                return false;
            }

            if (outputSentinel->IsCounterReachedSoftLimit() and isRecovery == false) {
                if (GetIndexOrder() == IndexOrder::INDEX_ORDER_PRIMARY) {
                    Row* row = outputSentinel->GetData();
                    if (row) {
                        if (row->IsRowDeleted() == false) {
                            rc = RC_UNIQUE_VIOLATION;
                            retryInsert = false;
                            continue;
                        }
                    }
                } else {
                    rc = RC_UNIQUE_VIOLATION;
                    retryInsert = false;
                    continue;
                }
            }
            // Spin if the counter is 0 - aborting in parallel or sentinel is marks for commit
            if (outputSentinel->RefCountUpdate(INC) == RC_OK) {
                retryInsert = false;
            }
        } else {
            retryInsert = false;
        }
    }

    if (!inserted) {
        MOT_ASSERT(outputSentinel != sentinel);
        // Failed sentinels return to the pool
        SentinelRelease(sentinel);
        return false;
    } else {
        // I am the owner of the inserted sentinel counter = 1
        // Earty abort stage
        MOT_ASSERT(outputSentinel == nullptr);
        outputSentinel = sentinel;
        return true;
    }
}

Sentinel* Index::IndexInsert(const Key* key, Row* row, uint32_t pid)
{
    bool inserted = false;
    PrimarySentinelNode* node = nullptr;
    Sentinel* currSentinel = nullptr;
    Sentinel* sentinel = SentinelAlloc();
    if (unlikely(sentinel == nullptr)) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Index Insert",
            "Failed to allocate sentinel for index %s during index insert",
            m_name.c_str());
        return nullptr;
    }
    if (GetIndexOrder() == IndexOrder::INDEX_ORDER_SECONDARY_UNIQUE) {
        node = SentinelNodeAlloc();
        if (node == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Sentinel node", "Failed to create new sentinel entry");
            SentinelRelease(sentinel);
            return nullptr;
        }
    }
    sentinel->Init(this, nullptr);
    sentinel->UnSetDirty();
    currSentinel = IndexInsertImpl(key, sentinel, inserted, pid);
    if (currSentinel != nullptr) {
        // no need to report to full error stack
        SetLastError(MOT_ERROR_UNIQUE_VIOLATION, MOT_SEVERITY_NORMAL);
        SentinelRelease(sentinel);
        if (GetIndexOrder() == IndexOrder::INDEX_ORDER_SECONDARY_UNIQUE) {
            SentinelNodeRelease(node);
        }
        return nullptr;
    } else {
        if (!inserted) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Index Insert", "Failed to insert sentinel to index %s", m_name.c_str());
            SentinelRelease(sentinel);
            if (GetIndexOrder() == IndexOrder::INDEX_ORDER_SECONDARY_UNIQUE) {
                SentinelNodeRelease(node);
            }
            return nullptr;
        }

        sentinel->SetIndexOrder(m_indexOrder);
        if (GetIndexOrder() == IndexOrder::INDEX_ORDER_PRIMARY) {
            sentinel->SetNextPtr(row);
            row->SetPrimarySentinel(sentinel);
        } else if (GetIndexOrder() == IndexOrder::INDEX_ORDER_SECONDARY) {
            MOT_ASSERT(row->GetPrimarySentinel() != nullptr);
            sentinel->SetNextPtr(row->GetPrimarySentinel());
        } else {
            MOT_ASSERT(node != nullptr);
            node->Init(0, Sentinel::SENTINEL_INIT_CSN, row->GetPrimarySentinel());
            sentinel->SetNextPtr(node);
        }
        MOT_ASSERT(sentinel->IsCommited() == true);
        return sentinel;
    }
}

Row* Index::IndexRead(const Key* key, uint32_t pid) const
{
    Row* row = nullptr;

    // find returns the sentinel, we should return what the sentinel points to
    // to hide the sentinel from the outside
    Sentinel* sentinel = IndexReadImpl(key, pid);
    if (sentinel != nullptr) {
        row = sentinel->GetData();
        if (row == nullptr || row->IsRowDeleted())
            row = nullptr;
    }

    return row;
}

Sentinel* Index::IndexReadHeader(const Key* key, uint32_t pid) const
{
    // find returns the sentinel, we should return what the sentinel points to
    // to hide the sentinel from the outside
    Sentinel* sentinel = IndexReadImpl(key, pid);

    return sentinel;
}

Sentinel* Index::IndexRemove(const Key* key, uint32_t pid)
{
    Sentinel* sentinel = IndexRemoveImpl(key, pid);

    return sentinel;
}

void Index::Compact(Table* table, uint32_t pid)
{
    IndexIterator* it = nullptr;
    char sentinelPrefix[256];
    errno_t erc = snprintf_s(
        sentinelPrefix, sizeof(sentinelPrefix), sizeof(sentinelPrefix) - 1, "%s(sentinel pool)", m_name.c_str());
    securec_check_ss(erc, "\0", "\0");
    sentinelPrefix[erc] = 0;

    do {
        CompactHandler chSentinel(m_sentinelPool, sentinelPrefix);

        chSentinel.StartCompaction(CompactTypeT::COMPACT_SIMPLE);
        it = Begin(pid);
        if (it == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Index Compaction", "Failed to begin iterating over index");
            return;
        }

        // return if empty
        if (!it->IsValid()) {
            break;
        }

        if (m_indexOrder == IndexOrder::INDEX_ORDER_PRIMARY) {
            char tabPrefix[256];
            erc = snprintf_s(
                tabPrefix, sizeof(tabPrefix), sizeof(tabPrefix) - 1, "%s(row pool)", table->GetTableName().c_str());
            securec_check_ss(erc, "\0", "\0");
            tabPrefix[erc] = 0;
            CompactHandler chRow(table->m_rowPool, tabPrefix);

            chRow.StartCompaction();

            if (chRow.IsCompactionNeeded()) {
                // do compaction
                while (it->IsValid()) {
                    PrimarySentinel* ps = static_cast<PrimarySentinel*>(it->GetPrimarySentinel());
                    Row* head = ps->GetData();
                    Row* next = nullptr;
                    if (head != nullptr and !head->IsRowDeleted()) {
                        next = head->GetNextVersion();
                        Row* newHead = chRow.CompactObj<Row>(head);
                        if (newHead != nullptr) {
                            ps->SetNextPtr(newHead);
                            head = newHead;
                            head->SetNextVersion(nullptr);
                        }
                        // Traverse on next
                        Row* tmp = next;
                        while (tmp) {
                            if (tmp->IsRowDeleted()) {
                                head->SetNextVersion(tmp);
                                head = tmp;
                                next = next->GetNextVersion();
                                tmp = next;
                                continue;
                            }
                            next = next->GetNextVersion();
                            Row* newRow = chRow.CompactObj<Row>(tmp);
                            if (newRow != nullptr) {
                                head->SetNextVersion(newRow);
                                head = newRow;
                            } else {
                                head->SetNextVersion(tmp);
                                head = tmp;
                            }
                            tmp = next;
                        }
                    }

                    it->Next();
                }
            }

            // end compaction
            chRow.EndCompaction();

            // Compact TombStone Pool
            erc = snprintf_s(tabPrefix,
                sizeof(tabPrefix),
                sizeof(tabPrefix) - 1,
                "%s(tombstone pool)",
                table->GetTableName().c_str());
            securec_check_ss(erc, "\0", "\0");
            tabPrefix[erc] = 0;
            CompactHandler chTombStone(table->m_tombStonePool, tabPrefix);
            chTombStone.StartCompaction(CompactTypeT::COMPACT_SIMPLE);
            chTombStone.EndCompaction();
        }

        chSentinel.EndCompaction();

        if (m_sSentinelVersionPool != nullptr) {
            erc = snprintf_s(sentinelPrefix,
                sizeof(sentinelPrefix),
                sizeof(sentinelPrefix) - 1,
                "%s(sentinel version pool)",
                m_name.c_str());
            securec_check_ss(erc, "\0", "\0");
            sentinelPrefix[erc] = 0;
            CompactHandler chSentinelVersion(m_sSentinelVersionPool, sentinelPrefix);
            chSentinelVersion.StartCompaction(CompactTypeT::COMPACT_SIMPLE);
            chSentinelVersion.EndCompaction();
        }

        erc = snprintf_s(
            sentinelPrefix, sizeof(sentinelPrefix), sizeof(sentinelPrefix) - 1, "%s(key pool)", m_name.c_str());
        securec_check_ss(erc, "\0", "\0");
        sentinelPrefix[erc] = 0;
        CompactHandler chKey(m_keyPool, sentinelPrefix);
        chKey.StartCompaction(CompactTypeT::COMPACT_SIMPLE);
        chKey.EndCompaction();
    } while (false);

    if (it != nullptr) {
        it->Destroy();
        delete it;
    }
}

uint64_t Index::GetIndexSize(uint64_t& netTotal)
{
    PoolStatsSt stats;

    errno_t erc = memset_s(&stats, sizeof(PoolStatsSt), 0, sizeof(PoolStatsSt));
    securec_check(erc, "\0", "\0");
    stats.m_type = PoolStatsT::POOL_STATS_ALL;
    m_keyPool->GetStats(stats);
    m_keyPool->PrintStats(stats, "Key Pool", LogLevel::LL_INFO);
    uint64_t res = stats.m_poolCount * stats.m_poolGrossSize;
    netTotal = (stats.m_totalObjCount - stats.m_freeObjCount) * stats.m_objSize;

    erc = memset_s(&stats, sizeof(PoolStatsSt), 0, sizeof(PoolStatsSt));
    securec_check(erc, "\0", "\0");
    stats.m_type = PoolStatsT::POOL_STATS_ALL;
    m_sentinelPool->GetStats(stats);
    m_sentinelPool->PrintStats(stats, "Sentinel Pool", LogLevel::LL_INFO);
    res += stats.m_poolCount * stats.m_poolGrossSize;
    netTotal += (stats.m_totalObjCount - stats.m_freeObjCount) * stats.m_objSize;

    if (m_sSentinelVersionPool) {
        erc = memset_s(&stats, sizeof(PoolStatsSt), 0, sizeof(PoolStatsSt));
        securec_check(erc, "\0", "\0");
        stats.m_type = PoolStatsT::POOL_STATS_ALL;
        m_sSentinelVersionPool->GetStats(stats);
        m_sSentinelVersionPool->PrintStats(stats, "Sentinel Version Pool", LogLevel::LL_INFO);
        res += stats.m_poolCount * stats.m_poolGrossSize;
        netTotal += (stats.m_totalObjCount - stats.m_freeObjCount) * stats.m_objSize;
    }

    MOT_LOG_INFO("Index %s memory size - Gross: %lu, NetTotal: %lu", m_name.c_str(), res, netTotal);
    return res;
}

Index* Index::CloneEmpty()
{
    Index* clonedIndex =
        IndexFactory::CreateIndex(m_indexOrder, m_indexingMethod, GetGlobalConfiguration().m_indexTreeFlavor);
    if (clonedIndex == nullptr) {
        // error could not allocate memory for new index
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Clone Index", "Failed to allocate index object");
        return nullptr;
    }

    clonedIndex->m_indexExtId = m_indexExtId;
    clonedIndex->m_indexOrder = m_indexOrder;
    clonedIndex->m_indexingMethod = m_indexingMethod;
    clonedIndex->m_keyLength = m_keyLength;
    clonedIndex->m_name = m_name;
    clonedIndex->m_table = m_table;
    clonedIndex->m_keyPool = ObjAllocInterface::GetObjPool(sizeof(Key) + ALIGN8(m_keyLength), false);
    if (m_indexOrder == IndexOrder::INDEX_ORDER_PRIMARY) {
        clonedIndex->m_sentinelPool = ObjAllocInterface::GetObjPool(sizeof(PrimarySentinel), false);
    } else {
        if (m_unique) {
            clonedIndex->m_sSentinelVersionPool = ObjAllocInterface::GetObjPool(sizeof(PrimarySentinelNode), false);
            if (clonedIndex->m_sSentinelVersionPool == nullptr) {
                MOT_REPORT_ERROR(
                    MOT_ERROR_OOM, "Clone Index", "Failed to allocate sentinel object pool for cloned index");
                delete clonedIndex;
                return nullptr;
            }
            clonedIndex->m_sentinelPool = ObjAllocInterface::GetObjPool(sizeof(SecondarySentinelUnique), false);
        } else {
            clonedIndex->m_sentinelPool = ObjAllocInterface::GetObjPool(sizeof(SecondarySentinel), false);
        }
    }
    clonedIndex->m_fake = m_fake;
    clonedIndex->m_indexId = m_indexId;
    clonedIndex->m_isCommited = false;
    clonedIndex->m_numKeyFields = m_numKeyFields;
    clonedIndex->m_unique = m_unique;

    if (clonedIndex->m_keyPool == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Clone Index", "Failed to allocate key pool for cloned index");
        delete clonedIndex;
        return nullptr;
    }

    if (clonedIndex->m_sentinelPool == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Clone Index", "Failed to allocate sentinel pool for cloned index");
        delete clonedIndex;
        return nullptr;
    }

    if (!clonedIndex->SetNumTableFields(m_numTableFields)) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Clone Index", "Failed to set index field count to %u", m_numTableFields);
        delete clonedIndex;
        return nullptr;
    }
    for (int i = 0; i < m_numKeyFields; i++) {
        clonedIndex->SetLenghtKeyFields(i, m_columnKeyFields[i], m_lengthKeyFields[i]);
    }

    RC rc = clonedIndex->IndexInitImpl(nullptr);
    if (rc != RC_OK) {
        MOT_REPORT_ERROR(rc, "Clone Index", "Failed to initialize cloned index");
        delete clonedIndex;
        return nullptr;
    }

    return clonedIndex;
}

void Index::ReclaimSentinel(Sentinel* sentinel)
{
    Row* r = nullptr;
    switch (GetIndexOrder()) {
        case IndexOrder::INDEX_ORDER_PRIMARY:
            r = sentinel->GetData();
            while (r) {
                Row* reclaimRow = r;
                r = r->GetNextVersion();
                reclaimRow->GetTable()->DestroyRow(reclaimRow);
            }
            break;
        case IndexOrder::INDEX_ORDER_SECONDARY:
            break;
        case IndexOrder::INDEX_ORDER_SECONDARY_UNIQUE:
            static_cast<SecondarySentinelUnique*>(sentinel)->ReleaseAllNodes();
            break;
    }

    m_sentinelPool->Release(sentinel);
}

}  // namespace MOT
