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
 * table.cpp
 *    The Table class holds all that is required to manage an in-memory table in the database.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/storage/table.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <malloc.h>
#include <string.h>
#include <algorithm>
#include "table.h"
#include "mot_engine.h"
#include "utilities.h"
#include "txn.h"
#include "txn_access.h"
#include "txn_insert_action.h"
#include "redo_log_writer.h"
#include "recovery_manager.h"

namespace MOT {
IMPLEMENT_CLASS_LOGGER(Table, Storage);

std::atomic<uint32_t> Table::tableCounter(0);

Table::~Table()
{
    if (m_numIndexes > 0) {
        m_secondaryIndexes.clear();
        for (int i = m_numIndexes - 1; i >= 0; i--) {
            if (m_indexes[i] != nullptr) {
                delete m_indexes[i];
            }
        }
        m_numIndexes = 0;
    }

    if (m_columns != nullptr) {
        for (uint i = 0; i < m_fieldCnt; i++) {
            if (m_columns[i] != nullptr) {
                delete m_columns[i];
            }
        }

        free(m_columns);
    }

    if (m_indexes != nullptr) {
        free(m_indexes);
    }

    if (m_rowPool) {
        ObjAllocInterface::FreeObjPool(&m_rowPool);
    }

    int destroyRc = pthread_rwlock_destroy(&m_rwLock);
    if (destroyRc != 0) {
        MOT_LOG_ERROR("~Table: rwlock destroy failed (%d)", destroyRc);
    }
}

bool Table::Init(const char* tableName, const char* longName, unsigned int fieldCnt, uint64_t tableExId)
{
    int initRc = pthread_rwlock_init(&m_rwLock, NULL);
    if (initRc != 0) {
        MOT_LOG_ERROR("failed to initialize Table %s, could not init rwlock (%d)", tableName, initRc);
        return false;
    }

    m_tableName.assign(tableName);
    m_longTableName.assign(longName);

    // allocate columns
    MOT_LOG_DEBUG("GC Create table id %d table name %s table addr = %p \n", m_tableId, tableName, this);
    this->m_columns = (Column**)memalign(CL_SIZE, fieldCnt * sizeof(Column*));
    if (m_columns == nullptr) {
        return false;
    }

    m_indexes = (Index**)memalign(CL_SIZE, MAX_NUM_INDEXES * sizeof(Index*));
    if (m_indexes == nullptr) {
        return false;
    }

    if (tableExId == 0) {
        m_tableExId = m_tableId;
    } else {
        this->m_tableExId = tableExId;
    }

    this->m_fieldCnt = 0;
    this->m_tupleSize = 0;
    this->m_maxFields = (unsigned int)fieldCnt;

    MOT_LOG_DEBUG("Table::%s %s TableId:%d ExId:%d\n", __func__, this->m_longTableName.c_str(), m_tableId, m_tableExId);
    return true;
}

bool Table::InitRowPool(bool local)
{
    bool result = true;
    m_rowPool = ObjAllocInterface::GetObjPool(sizeof(Row) + m_tupleSize, local);
    if (!m_rowPool) {
        MOT_REPORT_ERROR(
            MOT_ERROR_OOM, "Initialize Table", "Failed to allocate row pool for table %s", m_longTableName.c_str());
        result = false;
    }
    return result;
}

void Table::ClearThreadMemoryCache()
{
    for (int i = 0; i < m_numIndexes; i++) {
        if (m_indexes[i] != nullptr) {
            m_indexes[i]->ClearThreadMemoryCache();
        }
    }

    if (m_rowPool != nullptr) {
        m_rowPool->ClearThreadCache();
    }
}

void Table::IncIndexColumnUsage(MOT::Index* index)
{
    int16_t const* index_cols = index->GetColumnKeyFields();
    for (int16_t i = 0; i < index->GetNumFields(); i++) {
        int16_t column_id = index_cols[i];
        if (column_id >= 0) {
            GetField(column_id)->IncIndexUsage();
        }
    }
}

void Table::DecIndexColumnUsage(MOT::Index* index)
{
    int16_t const* index_cols = index->GetColumnKeyFields();
    for (int16_t i = 0; i < index->GetNumFields(); i++) {
        int16_t column_id = index_cols[i];
        if (column_id >= 0) {
            GetField(column_id)->DecIndexUsage();
        }
    }
}

bool Table::IsTableEmpty(uint32_t tid)
{
    bool res = false;
    IndexIterator* it = GetPrimaryIndex()->Begin(tid);

    // report error if failed to allocate
    if (it == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "IsTableEmpty", "Failed to begin iterating over primary index");
        return false;
    }

    if (!it->IsValid()) {
        res = true;
    }

    delete it;
    return res;
}

void Table::SetPrimaryIndex(MOT::Index* index)
{
    if (index != nullptr) {
        index->SetTable(this);
    }
    this->m_primaryIndex = index;
    m_indexes[0] = index;
}

bool Table::UpdatePrimaryIndex(MOT::Index* index, TxnManager* txn, uint32_t tid)
{
    if (this->m_primaryIndex) {
        DecIndexColumnUsage(this->m_primaryIndex);
        if (txn == nullptr) {
            if (DeleteIndex(this->m_primaryIndex) != RC_OK) {
                return false;
            }
        } else {
            if (txn->DropIndex(this->m_primaryIndex) != RC_OK) {
                return false;
            }
        }
    } else {
        if (m_numIndexes == 0) {
            ++m_numIndexes;
        }
    }

    IncIndexColumnUsage(index);
    SetPrimaryIndex(index);

    return true;
}

RC Table::DeleteIndex(MOT::Index* index)
{
    GcManager::ClearIndexElements(index->GetIndexId());
    delete index;

    return RC::RC_OK;
}

bool Table::AddSecondaryIndex(const string& indexName, MOT::Index* index, TxnManager* txn, uint32_t tid)
{
    // Should we check for duplicate indices with same name?
    // first create secondary index data
    bool createdIndexData =
        (txn != nullptr) ? CreateSecondaryIndexData(index, txn) : CreateSecondaryIndexDataNonTransactional(index, tid);
    if (!createdIndexData) {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
            "Add Secondary Index",
            "Failed to add secondary index %s to table %s",
            indexName.c_str(),
            m_longTableName.c_str());
        return false;
    }

    // add index to table structure after the data is in place
    // this order prevents index usage before all rows are indexed
    index->SetTable(this);
    IncIndexColumnUsage(index);

    m_secondaryIndexes[indexName] = index;
    m_indexes[m_numIndexes] = index;
    ++m_numIndexes;

    return true;
}

bool Table::CreateSecondaryIndexDataNonTransactional(MOT::Index* index, uint32_t tid)
{
    MaxKey key;
    bool ret = true;
    IndexIterator* it = m_indexes[0]->Begin(tid);

    // report error if failed to allocate
    if (it == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Create Secondary Index", "Failed to begin iterating over primary index");
        return false;
    }

    // iterate over primary index and insert secondary index keys
    while (it->IsValid()) {
        Row* row = it->GetRow();
        if (row == nullptr) {
            it->Next();
            continue;
        }
        key.InitKey(index->GetKeyLength());
        index->BuildKey(this, row, &key);
        if (index->IndexInsert(&key, row, tid) == nullptr) {
            if (MOT_IS_SEVERE()) {
                MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                    "Insert row",
                    "Failed to insert row to secondary index, key: %s",
                    key.GetKeyStr().c_str());
            }
            ret = false;
            break;
        }
        it->Next();
    }

    if (it != nullptr) {
        delete it;
    }
    return ret;
}
bool Table::CreateSecondaryIndexData(MOT::Index* index, TxnManager* txn)
{
    RC status = RC_OK;
    bool error = false;
    Key* key = nullptr;
    bool ret = true;
    IndexIterator* it = m_indexes[0]->Begin(txn->GetThdId());

    // report error if failed to allocate
    if (!it) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Create Secondary Index", "Failed to begin iterating over primary index");
        return false;
    }

    do {
        // return if empty
        if (!it->IsValid())
            break;

        // increment statement count to avoid not seeing inserted index rows...
        // this was found as part of a RC_LOCAL_ROW_NOT_VISIBLE on index creation
        txn->IncStmtCount();

        // iterate over primary index and insert secondary index keys
        while (it->IsValid()) {
            Row* tmpRow = nullptr;
            Row* row = it->GetRow();
            status = txn->AccessLookup(RD, it->GetPrimarySentinel(), tmpRow);
            switch (status) {
                case RC::RC_LOCAL_ROW_DELETED:
                    row = nullptr;
                    break;
                case RC::RC_LOCAL_ROW_NOT_FOUND:
                    break;
                case RC::RC_LOCAL_ROW_FOUND:
                    if (row == nullptr) {
                        row = tmpRow;
                    }
                    break;
                case RC::RC_MEMORY_ALLOCATION_ERROR:
                    // error handling
                    error = true;
                    break;
                default:
                    break;
            }

            if (row == nullptr) {
                it->Next();
                continue;
            }

            if (!error) {
                key = txn->GetTxnKey(index);
                index->BuildKey(this, row, key);
                txn->GetNextInsertItem()->SetItem(row, index, key);
                status = txn->InsertRow(row);
                if (status != RC_OK) {
                    error = true;
                    if (MOT_IS_SEVERE()) {  // report to error stack only in severe error conditions (we do not want to
                                            // burden "unique violation" scenario)
                        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                            "Create Secondary Index",
                            "Failed to insert row into unique secondary index %s in table %s",
                            index->GetName().c_str(),
                            m_longTableName.c_str());
                    }
                }
            }

            if (error) {
                txn->RollbackSecondaryIndexInsert(index);
                GcManager::ClearIndexElements(index->GetIndexId());
                if (status == RC::RC_MEMORY_ALLOCATION_ERROR) {
                    txn->m_err = RC_MEMORY_ALLOCATION_ERROR;
                } else {
                    txn->m_err = RC_UNIQUE_VIOLATION;
                }
                // index is not part of table yet, so we cannot save it in error info of transaction (and even worse,
                // soon it will be recycled by the envelope).
                txn->m_errIx = nullptr;
                index->BuildErrorMsg(this, row, txn->m_errMsgBuf, sizeof(txn->m_errMsgBuf));

                ret = false;
                break;
            }
            it->Next();
        }

        if (!ret) {
            break;
        }
    } while (0);

    if (it != nullptr) {
        delete it;
    }

    return ret;
}

RC Table::InsertRowNonTransactional(Row* row, uint64_t tid, Key* k, bool skipSecIndex)
{
    RC rc = RC_OK;
    MaxKey key;
    Key* pk = nullptr;
    uint64_t surrogateprimaryKey = 0;
    MOT::Index* ix = GetPrimaryIndex();
    uint32_t numIndexes = GetNumIndexes();
    SurrogateKeyGenerator& _surr_gen = GetSurrogateKeyManager()->GetSurrogateSlot(MOT_GET_CURRENT_CONNECTION_ID());
    if (row->GetRowId() == 0) {
        row->SetRowId(_surr_gen.GetSurrogateKey(MOT_GET_CURRENT_CONNECTION_ID()));
    }

    // add row
    if (k != nullptr) {
        pk = k;
    } else {
        pk = &key;
        pk->InitKey(ix->GetKeyLength());
        // set primary key
        if (ix->IsFakePrimary()) {
            surrogateprimaryKey = _surr_gen.GetSurrogateKey(MOT_GET_CURRENT_CONNECTION_ID());
            surrogateprimaryKey = htobe64(surrogateprimaryKey);
            row->SetSurrogateKey(surrogateprimaryKey);
            pk->CpKey((uint8_t*)&surrogateprimaryKey, sizeof(uint64_t));
        } else {
            ix->BuildKey(this, row, pk);
        }
    }
    Sentinel* res = ix->IndexInsert(pk, row, tid);

    if (res == nullptr) {
        if (MOT_IS_SEVERE()) {
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL, "Insert row", "Failed to insert row to index");
        }
        return MOT_GET_LAST_ERROR_RC();
    } else {
        row->SetPrimarySentinel(res);
    }

    // add secondary indexes
    if (!skipSecIndex) {
        for (uint16_t i = 1; i < numIndexes; i++) {
            ix = GetSecondaryIndex(i);
            key.InitKey(ix->GetKeyLength());
            ix->BuildKey(this, row, &key);
            if (ix->IndexInsert(&key, row, tid) == nullptr) {
                if (MOT_IS_SEVERE()) {
                    MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                        "Insert row",
                        "Failed to insert row to secondary index %u, key: %s",
                        i,
                        key.GetKeyStr().c_str());
                }
                return MOT_GET_LAST_ERROR_RC();
            }
        }
    }

    return rc;
}

RC Table::InsertRow(Row* row, TxnManager* txn)
{
    MOT::Key* key = nullptr;
    uint64_t surrogateprimaryKey = 0;
    MOT::Index* ix = GetPrimaryIndex();
    uint32_t numIndexes = GetNumIndexes();
    MOT::Key* cleanupKeys[numIndexes] = {nullptr};

    // add row
    // set primary key
    row->SetRowId(txn->GetSurrogateKey());

    key = txn->GetTxnKey(ix);
    if (key == nullptr) {
        DestroyRow(row);
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Insert Row", "Failed to create primary key");
        return RC_MEMORY_ALLOCATION_ERROR;
    }
    cleanupKeys[0] = key;
    if (ix->IsFakePrimary()) {
        surrogateprimaryKey = htobe64(row->GetRowId());
        row->SetSurrogateKey(surrogateprimaryKey);
        key->CpKey((uint8_t*)&surrogateprimaryKey, sizeof(uint64_t));
    } else {
        ix->BuildKey(this, row, key);
    }

    txn->GetNextInsertItem()->SetItem(row, ix, key);

    // add secondary indexes
    for (uint16_t i = 1; i < numIndexes; i++) {
        ix = GetSecondaryIndex(i);
        key = txn->GetTxnKey(ix);
        if (key == nullptr) {
            MOT_REPORT_ERROR(
                MOT_ERROR_OOM, "Insert Row", "Failed to create key for secondary index %s", ix->GetName().c_str());
            for (uint16_t j = 0; j < numIndexes; j++) {
                if (cleanupKeys[j] != nullptr) {
                    MOTCurrTxn->DestroyTxnKey(cleanupKeys[j]);
                }
            }
            DestroyRow(row);
            MOTCurrTxn->Rollback();
            return RC_MEMORY_ALLOCATION_ERROR;
        }
        cleanupKeys[i] = key;
        ix->BuildKey(this, row, key);
        txn->GetNextInsertItem()->SetItem(row, ix, key);
    }

    return txn->InsertRow(row);
}

Row* Table::RemoveRow(Row* row, uint64_t tid, GcManager* gc)
{
    MaxKey key;
    Row* OutputRow = nullptr;
    uint32_t numIndexes = GetNumIndexes();
    Sentinel* currSentinel = nullptr;
    // Build keys and mark sentinels for delete
    for (uint16_t i = 0; i < numIndexes; i++) {
        MOT::Index* ix = GetIndex(i);
        switch (ix->GetIndexOrder()) {
            case IndexOrder::INDEX_ORDER_PRIMARY: {
                key.InitKey(ix->GetKeyLength());
                ix->BuildKey(this, row, &key);
                currSentinel = ix->IndexReadImpl(&key, tid);
                MOT_ASSERT(currSentinel != nullptr);
                OutputRow = currSentinel->GetData();
                RC rc = currSentinel->RefCountUpdate(DEC, tid);
                if (rc == RC::RC_INDEX_DELETE) {
                    currSentinel = ix->IndexRemove(&key, tid);
                    if (likely(gc != nullptr)) {
                        gc->GcRecordObject(
                            ix->GetIndexId(), currSentinel, nullptr, ix->SentinelDtor, SENTINEL_SIZE(ix));
                        gc->GcRecordObject(ix->GetIndexId(), row, nullptr, row->RowDtor, ROW_SIZE_FROM_POOL(this));
                    } else {
                        if (!MOTEngine::GetInstance()->IsRecovering()) {
                            MOT_LOG_ERROR("RemoveRow called without GC when not recovering");
                            return nullptr;
                        }
                        DestroyRow(row);
                        ix->SentinelDtor(currSentinel, nullptr, false);
                    }
                }
                break;
            }
            case IndexOrder::INDEX_ORDER_SECONDARY: {
                key.InitKey(ix->GetKeyLength());
                ix->BuildKey(this, row, &key);
                currSentinel = ix->IndexReadImpl(&key, tid);
                MOT_ASSERT(currSentinel != nullptr);
                RC rc = currSentinel->RefCountUpdate(DEC, tid);
                if (rc == RC::RC_INDEX_DELETE) {
                    currSentinel = ix->IndexRemove(&key, tid);
                    if (likely(gc != nullptr)) {
                        gc->GcRecordObject(
                            ix->GetIndexId(), currSentinel, nullptr, ix->SentinelDtor, SENTINEL_SIZE(ix));
                    } else {
                        if (!MOTEngine::GetInstance()->IsRecovering()) {
                            MOT_LOG_ERROR("RemoveRow called without GC when not recovering");
                            return nullptr;
                        }
                        ix->SentinelDtor(currSentinel, nullptr, false);
                    }
                }
                break;
            }
        }
        MOT_ASSERT(currSentinel != nullptr);
        MOT_ASSERT(currSentinel->IsCommited() == true);
    }
    return OutputRow;
}

Row* Table::RemoveKeyFromIndex(Row* row, Sentinel* sentinel, uint64_t tid, GcManager* gc)
{
    MaxKey key;
    Row* OutputRow = nullptr;
    MOT::Index* ix = sentinel->GetIndex();
    Sentinel* currSentinel = nullptr;
    MOT_ASSERT(sentinel != nullptr);
    RC rc = sentinel->RefCountUpdate(DEC, tid);
    if (rc == RC::RC_INDEX_DELETE) {
        key.InitKey(ix->GetKeyLength());
        ix->BuildKey(this, row, &key);
#ifdef MOT_DEBUG
        currSentinel = ix->IndexReadImpl(&key, tid);
        MOT_ASSERT(currSentinel == sentinel);
        MOT_ASSERT(currSentinel->GetCounter() == 0);
#endif
        currSentinel = ix->IndexRemove(&key, tid);
        MOT_ASSERT(currSentinel == sentinel);
        MOT_ASSERT(currSentinel->GetCounter() == 0);
        if (likely(gc != nullptr)) {
            if (ix->GetIndexOrder() == IndexOrder::INDEX_ORDER_PRIMARY) {
                OutputRow = currSentinel->GetData();
                MOT_ASSERT(OutputRow != nullptr);
                gc->GcRecordObject(ix->GetIndexId(), currSentinel, nullptr, Index::SentinelDtor, SENTINEL_SIZE(ix));
                gc->GcRecordObject(ix->GetIndexId(), OutputRow, nullptr, Row::RowDtor, ROW_SIZE_FROM_POOL(this));
            } else {
                gc->GcRecordObject(ix->GetIndexId(), currSentinel, nullptr, Index::SentinelDtor, SENTINEL_SIZE(ix));
            }
        }
    }
    return OutputRow;
}

Row* Table::CreateNewRow()
{
    Row* row = m_rowPool->Alloc<Row>(this);
    if (row == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Create Row", "Failed to create new row in table %s", m_longTableName.c_str());
    }
    return row;
}

void Table::DestroyRow(Row* row)
{
    m_rowPool->Release<Row>(row);
}

bool Table::CreateMultipleRows(size_t numRows, Row* rows[])
{
    size_t failed_row = 0;
    bool res = true;
    Key* key = nullptr;

    for (size_t i = 0; i < numRows; i++) {
        rows[i] = m_rowPool->Alloc<Row>(this);
        if (rows[i] == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM,
                "Create Rows",
                "Failed to allocate row %u out of %u during multiple row creation in table %s",
                i,
                numRows,
                m_longTableName.c_str());
            failed_row = i;
            res = false;
            break;
        }

        // allocate primary key for row
        key = GetPrimaryIndex()->CreateNewKey();
        if (key == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM,
                "Create Rows",
                "Failed to allocate key while allocating row %u out of %u during multiple row creation in table %s",
                i,
                numRows,
                m_longTableName.c_str());
            failed_row = i;
            res = false;
            break;
        }
    }

    for (size_t i = 0; i < failed_row; i++) {
        m_rowPool->Release<Row>(rows[i]);
    }

    return res;
}

RC Table::AddColumn(
    const char* colName, uint64_t size, MOT_CATALOG_FIELD_TYPES type, bool isNotNull, unsigned int envelopeType)
{
    // validate input parameters
    if (!colName || type >= MOT_CATALOG_FIELD_TYPES::MOT_TYPE_UNKNOWN)
        return RC_UNSUPPORTED_COL_TYPE;

    if (m_fieldCnt == m_maxFields)
        return RC_TABLE_EXCEEDS_MAX_DECLARED_COLS;

    // column size is uint64_t but tuple size is uint32_t, so we must check for overflow
    if (size >= (uint64_t)std::numeric_limits<decltype(this->m_tupleSize)>::max())
        return RC_EXCEEDS_MAX_ROW_SIZE;

    decltype(this->m_tupleSize) old_size = m_tupleSize;
    decltype(this->m_tupleSize) new_size = old_size + size;
    if (new_size < old_size)
        return RC_COL_SIZE_INVALID;

    m_columns[m_fieldCnt] = Column::AllocColumn(type);
    if (m_columns[m_fieldCnt] == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Add Column",
            "Failed to allocate column %s of type %u, having size %" PRIu64 " bytes, for table %s",
            colName,
            (unsigned)type,
            size,
            m_longTableName.c_str());
        return RC_MEMORY_ALLOCATION_ERROR;
    }
    m_columns[m_fieldCnt]->m_nameLen = strlen(colName);
    if (m_columns[m_fieldCnt]->m_nameLen >= Column::MAX_COLUMN_NAME_LEN)
        return RC_COL_NAME_EXCEEDS_MAX_SIZE;
    errno_t erc = memcpy_s(&(m_columns[m_fieldCnt]->m_name[0]),
        Column::MAX_COLUMN_NAME_LEN,
        colName,
        m_columns[m_fieldCnt]->m_nameLen + 1);
    securec_check(erc, "\0", "\0");

    m_columns[m_fieldCnt]->m_type = type;
    m_columns[m_fieldCnt]->m_size = size;
    m_columns[m_fieldCnt]->m_id = m_fieldCnt;
    m_columns[m_fieldCnt]->m_offset = m_tupleSize;
    m_columns[m_fieldCnt]->m_isNotNull = isNotNull;
    m_columns[m_fieldCnt]->m_envelopeType = envelopeType;
    m_columns[m_fieldCnt]->SetKeySize();

    m_tupleSize += size;
    ++m_fieldCnt;
    return RC_OK;
}

bool Table::ModifyColumnSize(const uint32_t& id, const uint64_t& size)
{
    // validate input parameters
    if (id >= m_fieldCnt) {
        return false;
    }

    // column size is uint64_t but tuple size is uint32_t, so we must check for overflow
    if (size >= (uint64_t)std::numeric_limits<decltype(this->m_tupleSize)>::max()) {
        return false;
    }

    uint64_t oldColSize = m_columns[id]->m_size;
    uint64_t newTupleSize = ((uint64_t)m_tupleSize) - oldColSize + size;
    if (newTupleSize >= (uint64_t)std::numeric_limits<decltype(this->m_tupleSize)>::max()) {
        return false;
    }

    m_tupleSize = newTupleSize;
    m_columns[id]->m_size = size;

    // now we need to fix the offset of all subsequent fields
    for (uint32_t i = id + 1; i < m_fieldCnt; ++i) {
        m_columns[id]->m_offset = m_columns[id]->m_offset - oldColSize + size;
    }

    return true;
}

uint64_t Table::GetFieldId(const char* name) const
{
    uint32_t i;
    if (name == nullptr) {
        return (uint64_t)-1;
    }

    for (i = 0; i < m_fieldCnt; i++) {
        if (strcmp(name, m_columns[i]->m_name) == 0) {
            break;
        }
    }
    return (i < m_fieldCnt ? i : (uint64_t)-1);
}

void Table::PrintSchema()
{
    printf("\n[Table] %s\n", m_tableName.c_str());
    for (uint32_t i = 0; i < m_fieldCnt; i++) {
        printf("\t%s\t%s\t%lu\n", GetFieldName(i), GetFieldTypeStr(i), GetFieldSize(i));
    }
}

void Table::Truncate(TxnManager* txn)
{
    uint32_t pid = txn->GetThdId();
    (void)pthread_rwlock_wrlock(&m_rwLock);

    // first destroy secondary index data
    for (int i = 1; i < m_numIndexes; i++) {
        GcManager::ClearIndexElements(m_indexes[i]->GetIndexId());
        m_indexes[i]->Truncate(false);
    }

    // destroy primary index data and row data
    GcManager::ClearIndexElements(m_indexes[0]->GetIndexId());
    m_indexes[0]->Truncate(false);
    ObjAllocInterface::FreeObjPool(&m_rowPool);
    m_rowPool = ObjAllocInterface::GetObjPool(sizeof(Row) + m_tupleSize, false);
    if (!m_rowPool) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Truncate Table",
            "Failed to allocate row pool after truncate in table %s",
            m_longTableName.c_str());
    }

    (void)pthread_rwlock_unlock(&m_rwLock);
}

void Table::Compact(TxnManager* txn)
{
    uint32_t pid = txn->GetThdId();
    // first destroy secondary index data
    for (int i = 0; i < m_numIndexes; i++) {
        GcManager::ClearIndexElements(m_indexes[i]->GetIndexId(), false);
        m_indexes[i]->Compact(this, pid);
    }
}

uint64_t Table::GetTableSize()
{
    PoolStatsSt stats;
    errno_t erc = memset_s(&stats, sizeof(PoolStatsSt), 0, sizeof(PoolStatsSt));
    securec_check(erc, "\0", "\0");
    stats.m_type = PoolStatsT::POOL_STATS_ALL;

    m_rowPool->GetStats(stats);
    if (MOT_CHECK_LOG_LEVEL(LogLevel::LL_TRACE)) {
        m_rowPool->PrintStats(stats, "GC_TEST", LogLevel::LL_TRACE);
    }
    uint64_t res = stats.m_poolCount * stats.m_poolGrossSize;
    uint64_t netto = (stats.m_totalObjCount - stats.m_freeObjCount) * stats.m_objSize;

    erc = memset_s(&stats, sizeof(PoolStatsSt), 0, sizeof(PoolStatsSt));
    securec_check(erc, "\0", "\0");
    stats.m_type = PoolStatsT::POOL_STATS_ALL;

    MOT_LOG_INFO("Table %s memory size: gross: %lu", m_tableName.c_str(), res);
    return res;
}

size_t Table::SerializeItemSize(Column* column)
{
    size_t ret = SerializableARR<char, Column::MAX_COLUMN_NAME_LEN>::SerializeSize(column->m_name) +
                 SerializablePOD<uint64_t>::SerializeSize(column->m_size) +
                 SerializablePOD<MOT_CATALOG_FIELD_TYPES>::SerializeSize(column->m_type) +
                 SerializablePOD<bool>::SerializeSize(column->m_isNotNull) +
                 SerializablePOD<unsigned int>::SerializeSize(column->m_envelopeType);  // required for MOT JIT
    return ret;
}

char* Table::SerializeItem(char* dataOut, Column* column)
{
    if (!column || !dataOut) {
        return nullptr;
    }
    dataOut = SerializableARR<char, Column::MAX_COLUMN_NAME_LEN>::Serialize(dataOut, column->m_name);
    dataOut = SerializablePOD<uint64_t>::Serialize(dataOut, column->m_size);
    dataOut = SerializablePOD<MOT_CATALOG_FIELD_TYPES>::Serialize(dataOut, column->m_type);
    dataOut = SerializablePOD<bool>::Serialize(dataOut, column->m_isNotNull);
    dataOut = SerializablePOD<unsigned int>::Serialize(dataOut, column->m_envelopeType);  // required for MOT JIT
    return dataOut;
}

char* Table::DeserializeMeta(char* dataIn, CommonColumnMeta& meta, uint32_t metaVersion)
{
    dataIn = SerializableARR<char, Column::MAX_COLUMN_NAME_LEN>::Deserialize(dataIn, meta.m_name);
    dataIn = SerializablePOD<uint64_t>::Deserialize(dataIn, meta.m_size);
    dataIn = SerializablePOD<MOT_CATALOG_FIELD_TYPES>::Deserialize(dataIn, meta.m_type);
    dataIn = SerializablePOD<bool>::Deserialize(dataIn, meta.m_isNotNull);
    dataIn = SerializablePOD<unsigned int>::Deserialize(dataIn, meta.m_envelopeType);  // required for MOT JIT
    return dataIn;
}

size_t Table::SerializeItemSize(MOT::Index* index)
{
    size_t ret = SerializableSTR::SerializeSize(index->m_name) +
                 SerializablePOD<uint32_t>::SerializeSize(index->m_keyLength) +
                 SerializablePOD<IndexOrder>::SerializeSize(index->m_indexOrder) +
                 SerializablePOD<IndexingMethod>::SerializeSize(index->m_indexingMethod) +
                 SerializablePOD<uint64_t>::SerializeSize(index->m_indexExtId) +
                 SerializablePOD<bool>::SerializeSize(index->GetUnique()) +
                 SerializablePOD<int16_t>::SerializeSize(index->m_numKeyFields) +
                 SerializablePOD<uint32_t>::SerializeSize(index->m_numTableFields) +
                 SerializablePOD<bool>::SerializeSize(index->m_fake) +
                 SerializableARR<uint16_t, MAX_KEY_COLUMNS>::SerializeSize(index->m_lengthKeyFields) +
                 SerializableARR<int16_t, MAX_KEY_COLUMNS>::SerializeSize(index->m_columnKeyFields);
    return ret;
}

char* Table::SerializeItem(char* dataOut, MOT::Index* index)
{
    if (!index || !dataOut) {
        return nullptr;
    }
    uint32_t keyLength = index->m_keyLength;
    if (!index->GetUnique()) {
        keyLength -= NON_UNIQUE_INDEX_SUFFIX_LEN;
    }
    dataOut = SerializableSTR::Serialize(dataOut, index->m_name);
    dataOut = SerializablePOD<uint32_t>::Serialize(dataOut, keyLength);
    dataOut = SerializablePOD<IndexOrder>::Serialize(dataOut, index->m_indexOrder);
    dataOut = SerializablePOD<IndexingMethod>::Serialize(dataOut, index->m_indexingMethod);
    dataOut = SerializablePOD<uint64_t>::Serialize(dataOut, index->m_indexExtId);
    dataOut = SerializablePOD<bool>::Serialize(dataOut, index->GetUnique());
    dataOut = SerializablePOD<int16_t>::Serialize(dataOut, index->m_numKeyFields);
    dataOut = SerializablePOD<uint32_t>::Serialize(dataOut, index->m_numTableFields);
    dataOut = SerializablePOD<bool>::Serialize(dataOut, index->m_fake);
    dataOut = SerializableARR<uint16_t, MAX_KEY_COLUMNS>::Serialize(dataOut, index->m_lengthKeyFields);
    dataOut = SerializableARR<int16_t, MAX_KEY_COLUMNS>::Serialize(dataOut, index->m_columnKeyFields);
    return dataOut;
}

RC Table::RemoveSecondaryIndex(MOT::Index* index, TxnManager* txn)
{
    int rmIx = -1;
    RC res = RC_OK;
    do {
        SecondaryIndexMap::iterator itr = m_secondaryIndexes.find(index->GetName());
        if (MOT_EXPECT_TRUE(itr != m_secondaryIndexes.end())) {
            MOT_LOG_DEBUG("logging drop index operation (tableId %u), index name: %s index id = %d \n",
                GetTableId(),
                index->GetName().c_str(),
                index->GetIndexId());
            m_secondaryIndexes.erase(itr);
        } else {
            if (m_numIndexes > 0 && (strcmp(m_indexes[0]->GetName().c_str(), index->GetName().c_str()) == 0)) {
                MOT_LOG_INFO("Trying to remove primary index %s, not supported", index->GetName().c_str());
                break;
            }

            res = RC_INDEX_NOT_FOUND;
            break;
        }

        for (int i = 1; i < m_numIndexes; i++) {
            if (m_indexes[i] == index) {
                rmIx = i;
                break;
            }
        }

        // prevent removing primary by mistake
        if (rmIx > 0) {
            m_numIndexes--;
            for (int i = rmIx; i < m_numIndexes; i++) {
                m_indexes[i] = m_indexes[i + 1];
            }

            m_indexes[m_numIndexes] = nullptr;
        }
        GcManager::ClearIndexElements(index->GetIndexId());
        index->Truncate(true);

        DecIndexColumnUsage(index);
        delete index;
    } while (0);
    return res;
}

char* Table::DeserializeMeta(char* dataIn, CommonIndexMeta& meta, uint32_t metaVersion)
{
    uint32_t order, method, constr;
    dataIn = SerializableSTR::Deserialize(dataIn, meta.m_name);
    dataIn = SerializablePOD<uint32_t>::Deserialize(dataIn, meta.m_keyLength);
    dataIn = SerializablePOD<IndexOrder>::Deserialize(dataIn, meta.m_indexOrder);
    dataIn = SerializablePOD<IndexingMethod>::Deserialize(dataIn, meta.m_indexingMethod);
    dataIn = SerializablePOD<uint64_t>::Deserialize(dataIn, meta.m_indexExtId);
    dataIn = SerializablePOD<bool>::Deserialize(dataIn, meta.m_unique);
    dataIn = SerializablePOD<int16_t>::Deserialize(dataIn, meta.m_numKeyFields);
    dataIn = SerializablePOD<uint32_t>::Deserialize(dataIn, meta.m_numTableFields);
    dataIn = SerializablePOD<bool>::Deserialize(dataIn, meta.m_fake);
    dataIn = SerializableARR<uint16_t, MAX_KEY_COLUMNS>::Deserialize(dataIn, meta.m_lengthKeyFields);
    dataIn = SerializableARR<int16_t, MAX_KEY_COLUMNS>::Deserialize(dataIn, meta.m_columnKeyFields);
    MOT_LOG_DEBUG("%s: %s keyLen: %d Unique: %u", __func__, meta.m_name.c_str(), meta.m_keyLength, meta.m_unique);
    return dataIn;
}

RC Table::CreateIndexFromMeta(CommonIndexMeta& meta, bool primary, uint32_t tid, bool addToTable /* = true */,
    MOT::Index** outIndex /* = nullptr */)
{
    IndexTreeFlavor flavor = DEFAULT_TREE_FLAVOR;
    MOT::Index* ix = nullptr;

    MOT_LOG_DEBUG("%s: %s (%s)", __func__, meta.m_name.c_str(), primary ? "primary" : "secondary");
    if (meta.m_indexingMethod == IndexingMethod::INDEXING_METHOD_TREE) {
        flavor = GetGlobalConfiguration().m_indexTreeFlavor;
    }

    ix = IndexFactory::CreateIndex(meta.m_indexOrder, meta.m_indexingMethod, flavor);
    if (ix == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
            "Create Index from meta-data",
            "Failed to create index for table %s",
            m_longTableName.c_str());
        return RC_ERROR;
    }

    ix->SetUnique(meta.m_unique);
    if (!ix->SetNumTableFields(meta.m_numTableFields)) {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
            "Create Index from meta-data",
            "Failed to set number of columns in table %s to %u",
            m_longTableName.c_str(),
            meta.m_numTableFields);
        delete ix;
        return RC_ERROR;
    }

    for (int i = 0; i < meta.m_numKeyFields; i++) {
        ix->SetLenghtKeyFields(i, meta.m_columnKeyFields[i], meta.m_lengthKeyFields[i]);
    }
    ix->SetFakePrimary(meta.m_fake);
    ix->SetNumIndexFields(meta.m_numKeyFields);
    ix->SetTable(this);
    ix->SetExtId(meta.m_indexExtId);
    if (ix->IndexInit(meta.m_keyLength, meta.m_unique, meta.m_name, nullptr) != RC_OK) {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL, "Create Index from meta-data", "Failed to initialize index");
        delete ix;
        return RC_ERROR;
    }

    if (addToTable) {
        // In transactional recovery we set index as committed only during commit.
        ix->SetIsCommited(true);
        WrLock();
        if (primary) {
            if (UpdatePrimaryIndex(ix, nullptr, tid) != true) {
                Unlock();
                MOT_REPORT_ERROR(MOT_ERROR_INTERNAL, "Create Index from meta-data", "Failed to add primary index");
                delete ix;
                return RC_ERROR;
            }
        } else {
            if (AddSecondaryIndex(ix->GetName(), (Index*)ix, nullptr, tid) != true) {
                Unlock();
                MOT_REPORT_ERROR(MOT_ERROR_INTERNAL, "Create Index from meta-data", "Failed to add secondary index");
                delete ix;
                return RC_ERROR;
            }
        }
        Unlock();
    }

    if (outIndex != nullptr) {
        *outIndex = ix;
    }
    return RC_OK;
}

size_t Table::SerializeSize()
{
    uint32_t metaVersion = MetadataProtoVersion::METADATA_VER_CURR;
    size_t colsSize = 0;
    for (uint32_t i = 0; i < m_fieldCnt; i++) {
        colsSize += SerializeItemSize(m_columns[i]);
    }

    size_t idxsSize = 0;
    for (uint16_t i = 0; i < m_numIndexes; i++) {
        idxsSize += SerializeItemSize(m_indexes[i]);
    }

    size_t ret =
        SerializablePOD<uint32_t>::SerializeSize(metaVersion) + SerializableSTR::SerializeSize(m_tableName) +
        SerializableSTR::SerializeSize(m_longTableName) + SerializablePOD<uint16_t>::SerializeSize(m_numIndexes) +
        SerializablePOD<uint32_t>::SerializeSize(m_tableId) + SerializablePOD<uint64_t>::SerializeSize(m_tableExId) +
        SerializablePOD<bool>::SerializeSize(m_fixedLengthRows) + SerializablePOD<uint32_t>::SerializeSize(m_fieldCnt) +
        SerializablePOD<uint32_t>::SerializeSize(m_tupleSize) + SerializablePOD<uint32_t>::SerializeSize(m_maxFields) +
        colsSize + idxsSize;
    return ret;
}

void Table::Serialize(char* dataOut)
{
    uint32_t metaVersion = MetadataProtoVersion::METADATA_VER_CURR;
    char* savedDO = dataOut;
    dataOut = SerializablePOD<uint32_t>::Serialize(dataOut, metaVersion);
    dataOut = SerializableSTR::Serialize(dataOut, m_tableName);
    dataOut = SerializableSTR::Serialize(dataOut, m_longTableName);
    dataOut = SerializablePOD<uint16_t>::Serialize(dataOut, m_numIndexes);
    dataOut = SerializablePOD<uint32_t>::Serialize(dataOut, m_tableId);
    dataOut = SerializablePOD<uint64_t>::Serialize(dataOut, m_tableExId);
    dataOut = SerializablePOD<bool>::Serialize(dataOut, m_fixedLengthRows);
    dataOut = SerializablePOD<uint32_t>::Serialize(dataOut, m_fieldCnt);
    dataOut = SerializablePOD<uint32_t>::Serialize(dataOut, m_tupleSize);
    dataOut = SerializablePOD<uint32_t>::Serialize(dataOut, m_maxFields);

    /* serialize the columns */
    for (uint32_t i = 0; i < m_fieldCnt; i++) {
        dataOut = SerializeItem(dataOut, GetField(i));
    }

    /* primary key */
    if (m_numIndexes && m_primaryIndex) {
        dataOut = SerializeItem(dataOut, m_primaryIndex);
    }

    /* secondaries */
    if (m_numIndexes > 1) {
        for (int i = 1; i < m_numIndexes; i++) {
            dataOut = SerializeItem(dataOut, m_indexes[i]);
        }
    }
}

void Table::Deserialize(const char* in)
{
    // m_numIndexes will be incremented during each index addition
    uint16_t savedNumIndexes = 0;
    uint32_t metaVersion = 0;
    SetDeserialized(false);
    char* dataIn = (char*)in;
    dataIn = SerializablePOD<uint32_t>::Deserialize(dataIn, metaVersion);
    dataIn = SerializableSTR::Deserialize(dataIn, m_tableName);
    dataIn = SerializableSTR::Deserialize(dataIn, m_longTableName);
    dataIn = SerializablePOD<uint16_t>::Deserialize(dataIn, savedNumIndexes);
    dataIn = SerializablePOD<uint32_t>::Deserialize(dataIn, m_tableId);
    dataIn = SerializablePOD<uint64_t>::Deserialize(dataIn, m_tableExId);
    dataIn = SerializablePOD<bool>::Deserialize(dataIn, m_fixedLengthRows);
    dataIn = SerializablePOD<uint32_t>::Deserialize(dataIn, m_fieldCnt);
    dataIn = SerializablePOD<uint32_t>::Deserialize(dataIn, m_tupleSize);
    dataIn = SerializablePOD<uint32_t>::Deserialize(dataIn, m_maxFields);

    MOT_LOG_DEBUG("Table::%s: %s  num indexes: %d current Id: %u counter: %u",
        __func__,
        m_longTableName.c_str(),
        savedNumIndexes,
        m_tableId,
        tableCounter.load());
    if (m_tableId >= tableCounter.load()) {
        tableCounter = m_tableId + 1;
        MOT_LOG_DEBUG("Setting tableCounter to %u", tableCounter.load());
    }

    uint32_t saveFieldCount = m_fieldCnt;
    // use interleaved allocation for table columns
    if (!Init(m_tableName.c_str(), m_longTableName.c_str(), m_fieldCnt, m_tableExId)) {
        MOT_LOG_ERROR("Table::Deserialize - failed to init table");
        return;
    }
    m_fieldCnt = 0; /* we used it in init, addColumn() will update it again */

    /* deserialize the columns */
    CommonColumnMeta col;
    for (uint32_t i = 0; i < saveFieldCount; i++) {
        dataIn = DeserializeMeta(dataIn, col, metaVersion);
        if (AddColumn(col.m_name, col.m_size, col.m_type, col.m_isNotNull, col.m_envelopeType) != RC_OK) {
            MOT_LOG_ERROR("Table::deserialize - failed to add column %u", i);
            return;
        }
    }

    if (!InitRowPool()) {
        MOT_LOG_ERROR("Table::deserialize - failed to create row pool");
        return;
    }

    if (savedNumIndexes > 0) {
        CommonIndexMeta idx;
        /* primary key */
        dataIn = DeserializeMeta(dataIn, idx, metaVersion);
        if (CreateIndexFromMeta(idx, true, MOTCurrThreadId) != RC_OK) {
            MOT_LOG_ERROR("Table::deserialize - failed to create primary index");
            return;
        }

        /* secondaries */
        for (uint16_t i = 2; i <= savedNumIndexes; i++) {
            dataIn = DeserializeMeta(dataIn, idx, metaVersion);
            if (CreateIndexFromMeta(idx, false, MOTCurrThreadId) != RC_OK) {
                MOT_LOG_ERROR("Table::deserialize - failed to create secondary index [%u]", (i - 2));
                return;
            }
        }
    }
    MOT_ASSERT(m_numIndexes == savedNumIndexes);
    SetDeserialized(true);
}

RC Table::DropImpl()
{
    RC res = RC_OK;

    if (m_numIndexes == 0)
        return res;

    (void)pthread_rwlock_wrlock(&m_rwLock);
    do {
        m_secondaryIndexes.clear();
        MOT_LOG_DEBUG("DropImpl numIndexes = %d \n", m_numIndexes);
        for (int i = m_numIndexes - 1; i >= 0; i--) {
            if (m_indexes[i] != nullptr) {
                MOT::Index* index = m_indexes[i];
                // first remove index from table metadata to prevent it's usage
                m_indexes[i] = nullptr;
                GcManager::ClearIndexElements(index->GetIndexId());
                index->Truncate(true);
                DecIndexColumnUsage(index);
                delete index;
            }
        }
        m_numIndexes = 0;
    } while (0);
    (void)pthread_rwlock_unlock(&m_rwLock);
    return res;
}

Table* Table::CreateDummyTable()
{
    bool result = false;
    Table* tab = new (std::nothrow) Table();
    if (!tab) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Dummy Table Creation",
            "Failed to allocate memory for a new Table object while creating dummy table");
    } else {
        if (!tab->Init("dummy", "dummy", 1)) {
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL, "Dummy Table Creation", "Failed to initialize dummy table");
        } else {
            if (tab->AddColumn("dummy", MAX_TUPLE_SIZE, MOT_CATALOG_FIELD_TYPES::MOT_TYPE_CHAR) != RC_OK) {
                MOT_REPORT_ERROR(
                    MOT_ERROR_INTERNAL, "Dummy Table Creation", "Failed to add column 'dummy' to dummy table");
            } else {
                if (!tab->InitRowPool(true)) {
                    MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                        "Dummy Table Creation",
                        "Failed to initialize row pool, while creating dummy table");
                } else {
                    result = true;
                }
            }
        }
    }

    if (tab && !result) {
        delete tab;
        tab = nullptr;
    }

    return tab;
}

void Table::DeserializeNameAndIds(
    const char* data, uint32_t& metaVersion, uint32_t& intId, uint64_t& extId, string& name, string& longName)
{
    uint16_t nIndexes;
    char* dataIn = (char*)data;
    dataIn = SerializablePOD<uint32_t>::Deserialize(dataIn, metaVersion);
    dataIn = SerializableSTR::Deserialize(dataIn, name);
    dataIn = SerializableSTR::Deserialize(dataIn, longName);
    dataIn = SerializablePOD<uint16_t>::Deserialize(dataIn, nIndexes);
    dataIn = SerializablePOD<uint32_t>::Deserialize(dataIn, intId);
    dataIn = SerializablePOD<uint64_t>::Deserialize(dataIn, extId);
}

size_t Table::SerializeRedoSize()
{
    size_t colsSize = 0;
    for (uint32_t i = 0; i < m_fieldCnt; i++) {
        colsSize += SerializeItemSize(m_columns[i]);
    }

    /*
     * This method is just for serializing the table meta data for CreateTable Redo, without any indexes.
     * For all the indexes (including primary index), there will be separate redo entry. So we serialize with number of
     * indexes as 0.
     */
    uint16_t numIndexes = 0;
    uint32_t metaVersion = MetadataProtoVersion::METADATA_VER_CURR;
    size_t ret =
        SerializablePOD<uint32_t>::SerializeSize(metaVersion) + SerializableSTR::SerializeSize(m_tableName) +
        SerializableSTR::SerializeSize(m_longTableName) + SerializablePOD<uint16_t>::SerializeSize(numIndexes) +
        SerializablePOD<uint32_t>::SerializeSize(m_tableId) + SerializablePOD<uint64_t>::SerializeSize(m_tableExId) +
        SerializablePOD<bool>::SerializeSize(m_fixedLengthRows) + SerializablePOD<uint32_t>::SerializeSize(m_fieldCnt) +
        SerializablePOD<uint32_t>::SerializeSize(m_tupleSize) + SerializablePOD<uint32_t>::SerializeSize(m_maxFields) +
        colsSize;
    return ret;
}

void Table::SerializeRedo(char* dataOut)
{
    /*
     * This method is just for serializing the table meta data for CreateTable Redo, without any indexes.
     * For all the indexes (including primary index), there will be separate redo entry. So we serialize with number of
     * indexes as 0.
     */
    uint16_t numIndexes = 0;
    uint32_t metaVersion = MetadataProtoVersion::METADATA_VER_CURR;
    char* savedDO = dataOut;
    dataOut = SerializablePOD<uint32_t>::Serialize(dataOut, metaVersion);
    dataOut = SerializableSTR::Serialize(dataOut, m_tableName);
    dataOut = SerializableSTR::Serialize(dataOut, m_longTableName);
    dataOut = SerializablePOD<uint16_t>::Serialize(dataOut, numIndexes);
    dataOut = SerializablePOD<uint32_t>::Serialize(dataOut, m_tableId);
    dataOut = SerializablePOD<uint64_t>::Serialize(dataOut, m_tableExId);
    dataOut = SerializablePOD<bool>::Serialize(dataOut, m_fixedLengthRows);
    dataOut = SerializablePOD<uint32_t>::Serialize(dataOut, m_fieldCnt);
    dataOut = SerializablePOD<uint32_t>::Serialize(dataOut, m_tupleSize);
    dataOut = SerializablePOD<uint32_t>::Serialize(dataOut, m_maxFields);

    /* serialize the columns */
    for (uint32_t i = 0; i < m_fieldCnt; i++) {
        dataOut = SerializeItem(dataOut, GetField(i));
    }
}
}  // namespace MOT
