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
 * txn_table.cpp
 *    Wrapper class around Table class to handle DDL operations.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/storage/txn_table.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <malloc.h>
#include <cstring>
#include <algorithm>
#include "txn_table.h"
#include "mot_engine.h"
#include "utilities.h"
#include "txn.h"
#include "txn_access.h"

namespace MOT {
DECLARE_LOGGER(TxnTable, Storage);

bool TxnTable::InitTxnTable()
{
    int initRc = pthread_rwlock_init(&m_rwLock, NULL);
    if (initRc != 0) {
        MOT_LOG_ERROR("failed to initialize TxnTable %s, could not init rwlock (%d)", m_tableName.c_str(), initRc);
        return false;
    }
    initRc = pthread_mutex_init(&m_metaLock, NULL);
    if (initRc != 0) {
        MOT_LOG_ERROR("failed to initialize Table %s, could not init metalock (%d)", m_tableName.c_str(), initRc);
        return false;
    }
    m_columns = (Column**)memalign(CL_SIZE, m_fieldCnt * sizeof(Column*));
    if (m_columns == nullptr) {
        return false;
    }
    for (uint32_t i = 0; i < m_fieldCnt; i++) {
        m_columns[i] = Column::AllocColumn(m_origTab->m_columns[i]->m_type);
        if (m_columns[i] == nullptr) {
            MOT_LOG_ERROR("failed to allocate column for TxnTable %s", m_tableName.c_str());
            return false;
        }
        if (m_columns[i]->Clone(m_origTab->m_columns[i]) != RC_OK) {
            MOT_LOG_ERROR("failed to clone column for TxnTable %s", m_tableName.c_str());
            return false;
        }
    }

    m_indexes = (Index**)memalign(CL_SIZE, MAX_NUM_INDEXES * sizeof(Index*));
    if (m_indexes == nullptr) {
        return false;
    }
    for (int i = 0; i < m_origTab->m_numIndexes; i++) {
        m_indexes[i] = m_origTab->m_indexes[i];
    }
    m_secondaryIndexes.insert(m_origTab->m_secondaryIndexes.begin(), m_origTab->m_secondaryIndexes.end());
    return true;
}

bool TxnTable::InitRowPool(bool local)
{
    bool result = true;
    if (m_rowPool != m_origTab->m_rowPool) {
        ObjAllocInterface::FreeObjPool(&m_rowPool);
    }
    m_rowPool = ObjAllocInterface::GetObjPool(sizeof(Row) + m_tupleSize, local);
    if (!m_rowPool) {
        MOT_REPORT_ERROR(
            MOT_ERROR_OOM, "Initialize Table", "Failed to allocate row pool for table %s", m_longTableName.c_str());
        result = false;
    }
    return result;
}

bool TxnTable::InitTombStonePool(bool local)
{
    bool result = true;
    if (m_tombStonePool != m_origTab->m_tombStonePool) {
        ObjAllocInterface::FreeObjPool(&m_tombStonePool);
    }
    m_tombStonePool = ObjAllocInterface::GetObjPool(sizeof(Row), local);
    if (!m_tombStonePool) {
        MOT_REPORT_ERROR(
            MOT_ERROR_OOM, "Initialize Table", "Failed to allocate TS pool for table %s", m_longTableName.c_str());
        result = false;
    }
    return result;
}

void TxnTable::ApplyAddIndexFromOtherTxn()
{
    if (m_hasIndexChanges && !m_tooManyIndexes && m_origMetadataVer != m_origTab->m_metadataVer) {
        bool localLock = false;
        if (!m_isLocked) {
            (void)pthread_mutex_lock(&m_origTab->m_metaLock);
            localLock = true;
        }
        for (int i = 1; i < m_origTab->m_numIndexes; i++) {
            MOT::Index* ix = m_origTab->m_indexes[i];
            SecondaryIndexMap::iterator it = m_secondaryIndexes.find(ix->GetName());
            if (it == m_secondaryIndexes.end()) {
                if (m_numIndexes + 1 > (uint16_t)MAX_NUM_INDEXES) {
                    m_tooManyIndexes = true;
                    break;
                } else {
                    m_secondaryIndexes[ix->GetName()] = ix;
                    // move current txn index
                    m_indexes[m_numIndexes] = m_indexes[i];
                    // set index from other transaction to it's original place
                    m_indexes[i] = ix;
                    ++m_numIndexes;
                    IncIndexColumnUsage(ix);
                }
            }
        }
        m_origMetadataVer = m_origTab->m_metadataVer;
        if (localLock) {
            (void)pthread_mutex_unlock(&m_origTab->m_metaLock);
        }
    }
}

RC TxnTable::ValidateDDLChanges(TxnManager* txn)
{
    RC res = RC_OK;
    if (m_isDropped) {
        return res;
    }
    m_origTab->WrLock();
    (void)pthread_mutex_lock(&m_origTab->m_metaLock);
    m_isLocked = true;
    if (m_hasIndexChanges) {
        ApplyAddIndexFromOtherTxn();
        if (m_tooManyIndexes) {
            res = RC_TABLE_EXCEEDS_MAX_INDEXES;
        }
    }
    return res;
}

void TxnTable::ApplyDDLChanges(TxnManager* txn)
{
    if (m_isDropped) {
        return;
    }
    // Need to guard critical section
    if (!m_isLocked) {
        m_origTab->WrLock();
    }
    // apply index changes if any
    if (m_hasIndexChanges) {
        ApplyDDLIndexChanges(txn);
    }
    // apply column changes
    if (m_hasColumnChanges || m_hasColumnRename) {
        AlterConvertGlobalData(txn);
        for (uint i = 0; i < m_origTab->m_fieldCnt; i++) {
            if (m_origTab->m_columns[i] != nullptr) {
                delete m_origTab->m_columns[i];
            }
        }

        free(m_origTab->m_columns);
        m_origTab->m_columns = m_columns;
        m_origTab->m_fieldCnt = m_fieldCnt;
        m_origTab->m_maxFields = m_maxFields;
        m_origTab->m_tupleSize = m_tupleSize;
        for (uint i = 0; i < m_origTab->m_fieldCnt; i++) {
            m_origTab->m_columns[i]->SetIsCommitted(true);
        }

        for (int i = 0; i < m_numIndexes; i++) {
            if (!m_origTab->m_indexes[i]->SetNumTableFields(m_origTab->m_fieldCnt)) {
                elog(PANIC, "ApplyDDLChanges with alter table operation failed due to insufficient memory");
            }
        }
        m_columns = nullptr;
    }
    // apply row pool changes
    if (m_rowPool != m_origTab->m_rowPool) {
        if (m_origTab->m_rowPool != nullptr) {
            ObjAllocInterface::FreeObjPool(&m_origTab->m_rowPool);
        }
        m_origTab->m_rowPool = m_rowPool;
    }
    // apply tombstone pool changes
    if (m_tombStonePool != m_origTab->m_tombStonePool) {
        if (m_origTab->m_tombStonePool != nullptr) {
            ObjAllocInterface::FreeObjPool(&m_origTab->m_tombStonePool);
        }
        m_origTab->m_tombStonePool = m_tombStonePool;
    }
    m_origTab->m_rowCount = m_rowCount;
    m_origTab->m_metadataVer++;
    if (m_isLocked) {
        (void)pthread_mutex_unlock(&m_origTab->m_metaLock);
        m_isLocked = false;
    }
    m_origTab->Unlock();
}

void TxnTable::ApplyDDLIndexChanges(TxnManager* txn)
{
    if (!m_hasIndexChanges) {
        return;
    }

    // envelope allows concurrent 'create index ...'
    // this case handles index addition from multiple connections
    if (m_hasOnlyAddIndex) {
        for (int i = m_origTab->m_numIndexes; i < m_numIndexes; i++) {
            m_indexes[i]->SetTable(m_origTab);
            m_indexes[i]->SetSnapshot(txn->GetCommitSequenceNumber());
            m_indexes[i]->SetIsCommited(true);
            m_origTab->AddSecondaryIndexToMetaData(m_indexes[i]);
        }
    } else {
        if (m_primaryIndex != m_origTab->m_primaryIndex) {
            if (m_origTab->m_primaryIndex != nullptr) {
                m_origTab->DeleteIndex(m_origTab->m_primaryIndex);
            }
            m_origTab->m_primaryIndex = m_primaryIndex;
        }
        for (int i = 1; i < m_origTab->m_numIndexes; i++) {
            SecondaryIndexMap::iterator it = m_secondaryIndexes.find(m_origTab->m_indexes[i]->GetName());
            if (it != m_secondaryIndexes.end()) {
                if (it->second != m_origTab->m_indexes[i]) {
                    m_origTab->DeleteIndex(m_origTab->m_indexes[i]);
                }
            } else {
                m_origTab->DeleteIndex(m_origTab->m_indexes[i]);
            }
            m_origTab->m_indexes[i] = nullptr;
        }
        m_origTab->m_secondaryIndexes.clear();
        m_origTab->m_secondaryIndexes.insert(m_secondaryIndexes.begin(), m_secondaryIndexes.end());
        for (int i = 0; i < m_numIndexes; i++) {
            m_origTab->m_indexes[i] = m_indexes[i];
            m_indexes[i]->SetTable(m_origTab);
            if (m_indexes[i]->GetIsCommited() == false) {
                m_indexes[i]->SetSnapshot(txn->GetCommitSequenceNumber());
            }
            m_indexes[i]->SetIsCommited(true);
        }
        m_origTab->m_numIndexes = m_numIndexes;

        // apply column changes caused by indexes
        if (!m_hasColumnChanges && !m_hasColumnRename) {
            for (uint32_t i = 0; i < m_fieldCnt; i++) {
                m_origTab->m_columns[i]->m_numIndexesUsage = m_columns[i]->m_numIndexesUsage;
            }
        }
    }
}

void TxnTable::RollbackDDLChanges(TxnManager* txn)
{
    // Need to guard critical section
    if (!m_isLocked) {
        m_origTab->WrLock();
    }
    // apply index changes if any
    if (m_hasIndexChanges) {
        m_secondaryIndexes.clear();
        for (int i = 0; i < m_numIndexes; i++) {
            if (!m_indexes[i]->GetIsCommited()) {
                DeleteIndex(m_indexes[i]);
            }
        }
        m_numIndexes = 0;
    }
    // apply column changes
    if (m_hasColumnChanges) {
        for (uint i = 0; i < m_fieldCnt; i++) {
            if (m_columns[i] != nullptr) {
                delete m_columns[i];
                m_columns[i] = nullptr;
            }
        }
    }
    // apply row pool changes
    if (m_rowPool != m_origTab->m_rowPool) {
        if (m_rowPool != nullptr) {
            ObjAllocInterface::FreeObjPool(&m_rowPool);
        }
    }
    // apply tombstone row pool changes
    if (m_tombStonePool != m_origTab->m_tombStonePool) {
        if (m_tombStonePool != nullptr) {
            ObjAllocInterface::FreeObjPool(&m_tombStonePool);
        }
    }
    if (m_isLocked) {
        (void)pthread_mutex_unlock(&m_origTab->m_metaLock);
        m_isLocked = false;
    }
    m_origTab->Unlock();
}

RC TxnTable::AlterAddColumn(TxnManager* txn, Column* newColumn)
{
    uint32_t newColCount = m_fieldCnt + 1;
    uint64_t newNullBytesSize = BITMAP_GETLEN(newColCount);
    bool isNullBytesSizeChanged = (newNullBytesSize > m_columns[0]->m_size ? true : false);
    // calculate new row size by adding size of new column and difference of NULLBUTES column size
    uint32_t newTupleSize = m_tupleSize + newColumn->m_size + newNullBytesSize - m_columns[0]->m_size;
    Column** newColumns = nullptr;
    ObjAllocInterface* newRowPool = nullptr;

    RC rc = AlterReserveMem(txn, newTupleSize, newColumn);
    if (rc != RC_OK) {
        return rc;
    }

    MOT_LOG_INFO("Alter table add column memory chunks required %u", m_reservedChunks);

    do {
        // reallocate columns
        newColumns = (Column**)memalign(CL_SIZE, newColCount * sizeof(Column*));
        if (newColumns == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Alter Add Column", "Failed to allocate columns placeholder");
            rc = RC_MEMORY_ALLOCATION_ERROR;
            break;
        }
        newRowPool = ObjAllocInterface::GetObjPool(sizeof(Row) + newTupleSize, false);
        if (!newRowPool) {
            MOT_REPORT_ERROR(
                MOT_ERROR_OOM, "Alter Add Column", "Failed to allocate row pool for table %s", m_longTableName.c_str());
            rc = RC_MEMORY_ALLOCATION_ERROR;
            break;
        }

        // adjust offsets, the null bytes column size may grow only by 1 byte
        // so we need to shift all column offsets by one byte
        for (uint32_t i = 0; i < m_fieldCnt; i++) {
            newColumns[i] = m_columns[i];
            if (isNullBytesSizeChanged) {
                if (i == 0) {
                    newColumns[i]->m_size = newNullBytesSize;
                } else {
                    newColumns[i]->m_offset += 1;
                }
            }
        }
        newColumns[m_fieldCnt] = newColumn;
        rc = AlterAddConvertTxnData(txn, newColumns, newRowPool, newTupleSize, isNullBytesSizeChanged);
        if (rc != RC_OK) {
            break;
        }

        // from this point failure can not happen
        // free old data structures
        free(m_columns);
        if (m_rowPool != m_origTab->m_rowPool) {
            m_rowPool->m_objList = nullptr;
            ObjAllocInterface::FreeObjPool(&m_rowPool);
        }
        // set new data structures
        m_columns = newColumns;
        m_rowPool = newRowPool;
        m_fieldCnt++;
        m_maxFields++;
        m_tupleSize = newTupleSize;
    } while (false);

    // cleanup
    if (rc != RC_OK) {
        if (newColumns != nullptr) {
            delete newColumns;
        }
        if (newRowPool != nullptr) {
            ObjAllocInterface::FreeObjPool(&newRowPool);
        }
        if (isNullBytesSizeChanged) {
            for (uint32_t i = 0; i < m_fieldCnt; i++) {
                if (i == 0) {
                    m_columns[i]->m_size -= 1;
                } else {
                    m_columns[i]->m_offset -= 1;
                }
            }
        }
        if (rc == RC_PANIC) {
            elog(PANIC, "Alter table operation failed due to insufficient memory");
        }
    }
    return rc;
}

RC TxnTable::AlterReserveMem(TxnManager* txn, uint32_t newTupleSize, Column* newColumn)
{
    PoolStatsSt stats;
    errno_t erc = memset_s(&stats, sizeof(PoolStatsSt), 0, sizeof(PoolStatsSt));
    securec_check(erc, "\0", "\0");
    stats.m_type = PoolStatsT::POOL_STATS_ALL;
    m_rowPool->GetStatsEx(stats, nullptr);

    if (newColumn != nullptr) {
        uint64_t actualRecCount = stats.m_totalObjCount - stats.m_freeObjCount;
        if (actualRecCount > 0 && newColumn->m_isNotNull && !newColumn->m_hasDefault) {
            MOT_LOG_ERROR("NOT Null constraint violation");
            return RC::RC_NULL_VIOLATION;
        }
    }

    // calculate potential memory required
    // if not enough memory available return an error
    // reserve memory
    size_t required2MBChunks = m_rowPool->CalcRequiredMemDiff(stats, newTupleSize);
    if (!m_hasTruncate && m_rowPool != m_origTab->m_rowPool) {
        erc = memset_s(&stats, sizeof(PoolStatsSt), 0, sizeof(PoolStatsSt));
        securec_check(erc, "\0", "\0");
        stats.m_type = PoolStatsT::POOL_STATS_ALL;
        m_origTab->m_rowPool->GetStatsEx(stats, nullptr);
        if (newColumn != nullptr) {
            uint64_t actualRecCount = stats.m_totalObjCount - stats.m_freeObjCount;
            if (actualRecCount > 0 && newColumn->m_isNotNull && !newColumn->m_hasDefault) {
                MOT_LOG_ERROR("NOT Null constraint violation");
                return RC::RC_NULL_VIOLATION;
            }
        }
        required2MBChunks += m_origTab->m_rowPool->CalcRequiredMemDiff(stats, newTupleSize);
    }

    if (required2MBChunks > m_reservedChunks) {
        required2MBChunks -= m_reservedChunks;
        if (MemBufferReserveGlobal(required2MBChunks) != 0) {
            MOT_LOG_ERROR("Failed to reserve memory for alter table add/drop column");
            return RC_MEMORY_ALLOCATION_ERROR;
        }
        m_reservedChunks += required2MBChunks;
        txn->SetReservedChunks(required2MBChunks);
    } else {
        required2MBChunks = m_reservedChunks - required2MBChunks;
        txn->DecReservedChunks(required2MBChunks);
        m_reservedChunks -= required2MBChunks;
    }

    return RC_OK;
}

RC TxnTable::AlterAddConvertTxnData(TxnManager* txn, Column** newColumns, MOT::ObjAllocInterface* newRowPool,
    uint32_t newTupleSize, bool nullBitsChanged)
{
    RC rc = RC_OK;
    rowAddrMap_t oldToNewMap;
    TxnAccess* txnAc = txn->m_accessMgr;
    uint64_t oldNullBytesSize = (nullBitsChanged ? newColumns[0]->m_size - 1 : newColumns[0]->m_size);
    TxnOrderedSet_t& orderedSet = txnAc->GetOrderedRowSet();
    TxnOrderedSet_t::iterator it = orderedSet.begin();
    while (it != orderedSet.end()) {
        Access* ac = it->second;
        if (!ac->m_params.IsPrimarySentinel()) {
            (void)++it;
            continue;
        }
        if (ac->GetTxnRow()->GetOrigTable() == GetOrigTable()) {
            Row** row = nullptr;
            switch (ac->m_type) {
                case AccessType::RD:
                case AccessType::RD_FOR_UPDATE:
                case AccessType::DEL:
                    // Nothing to do
                    break;
                case AccessType::WR:
                    if (ac->GetLocalVersion() != nullptr) {
                        if (ac->m_localRow != nullptr && ac->m_localRow->GetOrigTable() == GetOrigTable()) {
                            rc = txnAc->AddColumn(ac);
                            if (rc != RC_OK) {
                                break;
                            }
                            row = &ac->m_localRow;
                        }
                    }
                    break;
                case AccessType::INS:
                    if (ac->GetLocalVersion() != nullptr) {
                        row = &ac->m_localInsertRow;
                    }
                    break;
                default:
                    break;
            }
            if (row != nullptr) {
                rc = AlterAddConvertRow(*row, newColumns, newRowPool, nullBitsChanged, oldNullBytesSize, oldToNewMap);
            }
        }
        if (rc != RC_OK) {
            break;
        }
        (void)++it;
    }

    // change pointers to a new rows for AC containing secondary sentinels
    // this has to be done even in case of error,
    // so all pointers will be pointing to a valid memory
    AlterConvertSecondaryTxnData(oldToNewMap, txnAc);
    return rc;
}

RC TxnTable::AlterAddConvertRow(Row*& oldRow, Column** newColumns, MOT::ObjAllocInterface* newRowPool,
    bool nullBitsChanged, uint64_t oldNullBytesSize, rowAddrMap_t& oldToNewMap)
{
    Row* newRow = newRowPool->Alloc<Row>(
        *oldRow, nullBitsChanged, oldNullBytesSize, newColumns[0]->m_size, m_tupleSize, newColumns[m_fieldCnt]);
    if (newRow == nullptr) {
        MOT_LOG_ERROR("Failed to allocate new ObjPool");
        return RC_PANIC;
    }
    if (newRow->m_pSentinel != nullptr && newRow->m_pSentinel->GetData() == oldRow) {
        newRow->m_pSentinel->SetNextPtr(newRow);
    }
    newRow->SetTable(this);
    oldToNewMap[oldRow] = newRow;
    m_rowPool->Release<Row>(oldRow);
    oldRow = newRow;
    return RC_OK;
}

RC TxnTable::AlterDropColumn(TxnManager* txn, Column* col)
{
    uint32_t newTupleSize = m_tupleSize - col->m_size;
    ObjAllocInterface* newRowPool = nullptr;

    RC rc = AlterReserveMem(txn, newTupleSize, nullptr);
    if (rc != RC_OK) {
        return rc;
    }

    MOT_LOG_INFO("Alter table drop column memory chunks required %u", m_reservedChunks);

    do {
        newRowPool = ObjAllocInterface::GetObjPool(sizeof(Row) + newTupleSize, false);
        if (!newRowPool) {
            MOT_REPORT_ERROR(
                MOT_ERROR_OOM, "Alter Add Column", "Failed to allocate row pool for table %s", m_longTableName.c_str());
            rc = RC_MEMORY_ALLOCATION_ERROR;
            break;
        }
        rc = AlterDropConvertTxnData(txn, col, newRowPool, newTupleSize);
        if (rc != RC_OK) {
            break;
        }

        // from this point failure can not happen
        // adjust column offsets after dropped column
        for (uint32_t i = col->m_id + 1; i < m_fieldCnt; i++) {
            if (!m_columns[i]->m_isDropped) {
                m_columns[i]->m_offset -= col->m_size;
            }
        }
        m_columns[col->m_id]->SetDropped();
        // free old data structures
        if (m_rowPool != m_origTab->m_rowPool) {
            m_rowPool->m_objList = nullptr;
            ObjAllocInterface::FreeObjPool(&m_rowPool);
        }
        // set new metadata structures
        m_rowPool = newRowPool;
        m_tupleSize = newTupleSize;
    } while (false);

    // cleanup
    if (rc != RC_OK) {
        if (newRowPool != nullptr) {
            ObjAllocInterface::FreeObjPool(&newRowPool);
        }
        rc = RC_PANIC;
        elog(PANIC, "Alter table operation failed due to insufficient memory");
    }
    return rc;
}

RC TxnTable::AlterDropConvertTxnData(
    TxnManager* txn, Column* col, MOT::ObjAllocInterface* newRowPool, uint32_t newTupleSize)
{
    RC res = RC_OK;
    rowAddrMap_t oldToNewMap;
    size_t offset1 = sizeof(Row) + col->m_offset;
    size_t offset2 = sizeof(Row) + col->m_offset + col->m_size;
    size_t len2 = sizeof(Row) + m_tupleSize - offset2;
    TxnAccess* txnAc = txn->m_accessMgr;
    TxnOrderedSet_t& orderedSet = txnAc->GetOrderedRowSet();
    TxnOrderedSet_t::iterator itr = orderedSet.begin();
    while (itr != orderedSet.end() && res == RC_OK) {
        Access* access = itr->second;
        if (!access->m_params.IsPrimarySentinel()) {
            (void)++itr;
            continue;
        }
        if (access->GetTxnRow()->GetOrigTable() == GetOrigTable()) {
            Row** row = nullptr;
            switch (access->m_type) {
                case AccessType::RD:
                case AccessType::RD_FOR_UPDATE:
                case AccessType::DEL:
                    // Nothing to do
                    break;
                case AccessType::WR:
                    if (access->GetLocalVersion() != nullptr) {
                        if (access->m_localRow != nullptr && access->m_localRow->GetOrigTable() == GetOrigTable()) {
                            txnAc->DropColumn(access, col);
                            row = &access->m_localRow;
                        }
                    }
                    break;
                case AccessType::INS:
                    if (access->GetLocalVersion() != nullptr) {
                        row = &access->m_localInsertRow;
                    }
                    break;
                default:
                    break;
            }
            if (row != nullptr) {
                res = AlterDropConvertRow(*row, newRowPool, col, offset1, offset2, len2, oldToNewMap);
            }
        }
        if (res != RC_OK) {
            break;
        }
        (void)++itr;
    }

    // change pointers to a new rows for AC containing secondary sentinels
    AlterConvertSecondaryTxnData(oldToNewMap, txnAc);
    return res;
}

RC TxnTable::AlterDropConvertRow(Row*& oldRow, MOT::ObjAllocInterface* newRowPool, Column* col, size_t offset1,
    size_t offset2, size_t len2, rowAddrMap_t& oldToNewMap)
{
    Row* newRow = newRowPool->Alloc<Row>(this);
    if (newRow == nullptr) {
        MOT_LOG_ERROR("Failed to allocate new ObjPool");
        return RC_PANIC;
    }
    // copy including row header until dropped column
    errno_t erc = memcpy_s(static_cast<void*>(newRow), offset1, static_cast<void*>(oldRow), offset1);
    securec_check(erc, "\0", "\0");
    // copy data after dropped column
    if (len2 > 0) {
        erc = memcpy_s(((uint8_t*)newRow) + offset1, len2, ((uint8_t*)oldRow) + offset2, len2);
        securec_check(erc, "\0", "\0");
    }
    if (newRow->m_pSentinel != nullptr && newRow->m_pSentinel->GetData() == oldRow) {
        newRow->m_pSentinel->SetNextPtr(newRow);
    }
    uint8_t* data = (uint8_t*)newRow->GetData();
    BITMAP_CLEAR(data, col->m_id - 1);
    newRow->SetTable(this);
    oldToNewMap[oldRow] = newRow;
    m_rowPool->Release<Row>(oldRow);
    oldRow = newRow;
    return RC_OK;
}

void TxnTable::AlterConvertSecondaryTxnData(rowAddrMap_t& oldToNewMap, TxnAccess* txnAc)
{
    TxnOrderedSet_t& orderedSet = txnAc->GetOrderedRowSet();
    // change pointers to a new rows for AC containing secondary sentinels
    for (TxnOrderedSet_t::iterator it = orderedSet.begin(); it != orderedSet.end(); (void)++it) {
        Access* ac = it->second;
        if (ac->m_type == AccessType::INS && !ac->m_params.IsPrimarySentinel()) {
            rowAddrMap_t::iterator itr = oldToNewMap.find(ac->m_localInsertRow);
            if (itr != oldToNewMap.end()) {
                ac->m_localInsertRow = itr->second;
            }
        }
    }
}

void TxnTable::AlterConvertGlobalData(TxnManager* txn)
{
    if (!m_hasColumnChanges || m_hasTruncate) {
        return;
    }

    // clean all GC elements for the table, this call should actually release
    // all elements into an appropriate object pool
    for (uint16_t i = 0; i < m_origTab->GetNumIndexes(); i++) {
        MOT::Index* index = m_origTab->GetIndex(i);
        GcManager::ClearIndexElements(index->GetIndexId(), GC_OPERATION_TYPE::GC_ALTER_TABLE);
    }

    RC rc = RC_OK;
    uint32_t count = 0;
    ObjPool* oldCurr = nullptr;
    PoolStatsSt stats;
    ObjPoolAddrSet_t poolSet;
    errno_t erc = memset_s(&stats, sizeof(PoolStatsSt), 0, sizeof(PoolStatsSt));
    securec_check(erc, "\0", "\0");
    stats.m_type = PoolStatsT::POOL_STATS_ALL;
    m_origTab->m_rowPool->GetStatsEx(stats, &poolSet);
    ObjPoolAddrSet_t::iterator it = poolSet.begin();

    m_origTab->m_rowPool->ClearAllThreadCache();
    acRowAddrMap_t oldToNewMap;
    TxnAccess* txnAc = txn->m_accessMgr;
    TxnOrderedSet_t& orderedSet = txnAc->GetOrderedRowSet();
    TxnOrderedSet_t::iterator oit = orderedSet.begin();
    while (oit != orderedSet.end()) {
        Access* ac = oit->second;
        if (!ac->m_params.IsPrimarySentinel() && ac->m_localInsertRow != nullptr &&
            ac->m_localInsertRow->GetOrigTable() == m_origTab) {
            (void)oldToNewMap.insert({ac->m_localInsertRow, ac});
        }
        (void)++oit;
    }
    // NOTE: there should not be any transactional data at this stage
    while (it != poolSet.end()) {
        oldCurr = *it;
        Row* oldRow;
        ObjPoolItr itr(oldCurr);
        while ((oldRow = (Row*)itr.Next()) != nullptr) {
            rc = AlterConvertGlobalRow(oldRow, oldToNewMap);
            if (rc != RC_OK) {
                break;
            }
            // release old object
            itr.ReleaseCurrent();
            count++;
        }
        if (rc != RC_OK) {
            break;
        }
        // free empty object pool
        ObjPool::DelObjPool(oldCurr, m_origTab->m_rowPool->m_type, m_origTab->m_rowPool->m_global);
        (void)++it;
    }
    // Convert global rows pointers in transactional data
    oit = orderedSet.begin();
    while (oit != orderedSet.end()) {
        Access* ac = oit->second;
        if (ac->m_params.IsPrimarySentinel()) {
            if (ac->m_type == WR or ac->m_type == DEL or ac->m_params.IsUpgradeInsert()) {
                ac->m_globalRow = ac->m_origSentinel->GetData();
            }
        }
        (void)++oit;
    }
    if (rc == RC_OK) {
        MOT_LOG_INFO("Rows converted %u", count);
        m_origTab->m_rowPool->m_objList = nullptr;
    } else {
        elog(PANIC, "Alter table operation failed due to insufficient memory");
    }
}

RC TxnTable::AlterConvertGlobalRow(Row* oldRow, acRowAddrMap_t& oldToNewMap)
{
    Row* newRow =
        (Row*)m_rowPool->Alloc<Row>(*oldRow, m_columns, m_fieldCnt, m_origTab->m_columns, m_origTab->m_fieldCnt);
    if (newRow == nullptr) {
        MOT_LOG_ERROR("Failed to allocate new ObjPool");
        // non-recoverable error
        // should not happen, see required memory calculations
        return RC_PANIC;
    }

    MOT_ASSERT(newRow->GetNextVersion() == nullptr);

    // assign row to a sentinel
    if (newRow->m_pSentinel != nullptr) {
        Row* t = newRow->m_pSentinel->GetData();
        if (t != nullptr) {
            if (t->IsRowDeleted()) {
                if (t->GetNextVersion() == oldRow) {
                    t->SetNextVersion(newRow);
                } else {
                    MOT_ASSERT(false);
                }
            } else if (t == oldRow) {
                MOT_ASSERT(t->GetNextVersion() == nullptr);
                newRow->m_pSentinel->SetNextPtr(newRow);
            } else {
                MOT_ASSERT(false);
            }
        }
    }

    newRow->SetTable(m_origTab);
    auto nit = oldToNewMap.equal_range(oldRow);
    for (auto nitR = nit.first; nitR != nit.second; ++nitR) {
        nitR->second->m_localInsertRow = newRow;
    }

    return RC_OK;
}

RC TxnTable::AlterRenameColumn(TxnManager* txn, Column* col, char* newname, DDLAlterTableRenameColumn* alter)
{
    RC rc = RC_OK;
    Column* txnCol = m_columns[col->m_id];
    MOT_LOG_INFO("Alter table rename column %s to %s", col->m_name, newname);
    size_t len = strlen(newname);
    errno_t erc = memset_s(txnCol->m_name, sizeof(txnCol->m_name), 0, sizeof(txnCol->m_name));
    securec_check(erc, "\0", "\0");
    erc = memcpy_s(txnCol->m_name, Column::MAX_COLUMN_NAME_LEN, newname, len);
    securec_check(erc, "\0", "\0");
    txnCol->m_nameLen = len;
    // copy names for redo log operations
    erc = memcpy_s(alter->m_newname, Column::MAX_COLUMN_NAME_LEN, newname, len);
    securec_check(erc, "\0", "\0");
    alter->m_newname[len] = 0;
    erc = memcpy_s(alter->m_oldname, Column::MAX_COLUMN_NAME_LEN, col->m_name, col->m_nameLen);
    securec_check(erc, "\0", "\0");
    alter->m_oldname[col->m_nameLen] = 0;
    return rc;
}

TxnTable::~TxnTable()
{
    if (m_columns != nullptr) {
        for (uint i = 0; i < m_fieldCnt; i++) {
            if (m_columns[i] != nullptr) {
                delete m_columns[i];
                m_columns[i] = nullptr;
            }
        }
        free(m_columns);
        m_columns = nullptr;
    }
    if (m_indexes != nullptr) {
        free(m_indexes);
        m_indexes = nullptr;
    }
    m_rowPool = nullptr;
    m_tombStonePool = nullptr;
    m_primaryIndex = nullptr;
    m_secondaryIndexes.clear();
    m_numIndexes = 0;
    int destroyRc = pthread_rwlock_destroy(&m_rwLock);
    if (destroyRc != 0) {
        MOT_LOG_ERROR("~TxnTable: rwlock destroy failed (%d)", destroyRc);
    }
    destroyRc = pthread_mutex_destroy(&m_metaLock);
    if (destroyRc != 0) {
        MOT_LOG_ERROR("~Table: metaLock destroy failed (%d)", destroyRc);
    }

    m_origTab = nullptr;
}
}  // namespace MOT
