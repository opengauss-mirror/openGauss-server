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
 * recovery_ops.cpp
 *    Recovery logic for various operation types.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/src/system/recovery/recovery_ops.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "mot_engine.h"
#include "serializable.h"
#include "recovery_manager.h"
#include "checkpoint_utils.h"
#include "bitmapset.h"
#include "column.h"

#include <string>

namespace MOT {
DECLARE_LOGGER(RecoveryOps, Recovery);

uint32_t RecoveryManager::RecoverLogOperation(
    uint8_t* data, uint64_t csn, uint64_t transactionId, uint32_t tid, SurrogateState& sState, RC& status)
{
    OperationCode opCode = *static_cast<OperationCode*>((void*)data);
    switch (opCode) {
        case CREATE_ROW:
            return RecoverLogOperationInsert(data, csn, tid, sState, status);
        case UPDATE_ROW:
            return RecoverLogOperationUpdate(data, csn, tid, status);
        case OVERWRITE_ROW:
            return RecoverLogOperationOverwrite(data, csn, tid, sState, status);
        case REMOVE_ROW:
            return RecoverLogOperationDelete(data, csn, tid, status);
        case CREATE_TABLE:
            return RecoverLogOperationCreateTable(data, status, COMMIT, transactionId);
        case CREATE_INDEX:
            return RecoverLogOperationCreateIndex(data, tid, status, COMMIT);
        case DROP_TABLE:
            return RecoverLogOperationDropTable(data, status, COMMIT);
        case DROP_INDEX:
            return RecoverLogOperationDropIndex(data, status, COMMIT);
        case TRUNCATE_TABLE:
            return RecoverLogOperationTruncateTable(data, status, COMMIT);
        case COMMIT_TX:
        case COMMIT_PREPARED_TX:
            return RecoverLogOperationCommit(data, csn, tid);
        case PARTIAL_REDO_TX:
        case PREPARE_TX:
            return sizeof(EndSegmentBlock);
        default:
            return 0;
    }
}

uint32_t RecoveryManager::RecoverLogOperationCreateTable(
    uint8_t* data, RC& status, RecoveryOpState state, uint64_t transactionId)
{
    if (GetGlobalConfiguration().m_enableIncrementalCheckpoint) {
        status = RC_ERROR;
        MOT_LOG_ERROR("RecoverLogOperationCreateTable: failed to create table. "
                      "MOT does not support incremental checkpoint");
        return 0;
    }

    uint32_t tableId;
    uint64_t extId;
    std::string tableName;
    std::string longName;
    TableInfo* tableInfo = nullptr;
    std::map<uint64_t, TableInfo*>::iterator it;
    OperationCode opCode = *(OperationCode*)data;

    MOT_ASSERT(opCode == CREATE_TABLE);
    data += sizeof(OperationCode);
    size_t bufSize = 0;
    Extract(data, bufSize);
    Table* table = nullptr;
    switch (state) {
        case COMMIT:
            MOT_LOG_INFO("RecoverLogOperationCreateTable: COMMIT");
            CreateTable((char*)data, status, table, true);
            break;

        case TPC_APPLY:
            CreateTable((char*)data, status, table, false /* don't add to engine yet */);
            if (status == RC_OK && table != nullptr) {
                tableInfo = new (std::nothrow) TableInfo(table, transactionId);
                if (tableInfo != nullptr) {
                    MOT::GetRecoveryManager()->m_preCommitedTables[table->GetTableId()] = tableInfo;
                } else {
                    status = RC_ERROR;
                    MOT_LOG_ERROR("RecoverLogOperationCreateTable: failed to create table info");
                }
            }
            if (status != RC_OK) {
                if (table != nullptr)
                    delete table;
                if (tableInfo != nullptr)
                    delete tableInfo;
            }
            break;

        case TPC_COMMIT:
        case TPC_ABORT:
            MOT_LOG_INFO("RecoverLogOperationCreateTable: %s", (state == TPC_COMMIT) ? "TPC_COMMIT" : "TPC_ABORT");
            Table::DeserializeNameAndIds((const char*)data, tableId, extId, tableName, longName);
            it = GetRecoveryManager()->m_preCommitedTables.find(tableId);
            if (it != GetRecoveryManager()->m_preCommitedTables.end()) {
                tableInfo = (TableInfo*)it->second;
                if (tableInfo != nullptr) {
                    if (state == TPC_COMMIT) {
                        MOT_LOG_DEBUG("RecoverLogOperationCreateTable - adding table %s to engine", longName.c_str());
                        status = GetTableManager()->AddTable(tableInfo->m_table) ? RC_OK : RC_ERROR;
                    } else
                        status = RC_OK;
                } else {
                    MOT_LOG_ERROR("RecoverLogOperationCreateTable: no data on table info");
                    status = RC_ERROR;
                }
                if (tableInfo != nullptr && tableInfo->m_table != nullptr && state == TPC_ABORT)
                    delete tableInfo->m_table;
                if (tableInfo != nullptr)
                    delete tableInfo;
                GetRecoveryManager()->m_preCommitedTables.erase(it);
            } else {
                MOT_LOG_ERROR(
                    "RecoverLogOperationCreateTable: could not find table [%lu] %s", tableId, tableName.c_str());
                status = RC_ERROR;
            }
            break;

        default:
            MOT_LOG_ERROR("RecoverLogOperationCreateTable: bad state");
            status = RC_ERROR;
            break;
    }

    return sizeof(OperationCode) + sizeof(bufSize) + bufSize;
}

uint32_t RecoveryManager::RecoverLogOperationDropTable(uint8_t* data, RC& status, RecoveryOpState state)
{
    OperationCode opCode = *(OperationCode*)data;
    MOT_ASSERT(opCode == DROP_TABLE);
    data += sizeof(OperationCode);
    switch (state) {
        case COMMIT:
        case TPC_COMMIT:
            DropTable((char*)data, status);
            break;
        case ABORT:
        case TPC_APPLY:
        case TPC_ABORT:
            break;
        default:
            MOT_LOG_ERROR("RecoverLogOperationDropTable: bad state");
            status = RC_ERROR;
            break;
    }
    return sizeof(OperationCode) + sizeof(uint32_t);
}

uint32_t RecoveryManager::RecoverLogOperationCreateIndex(uint8_t* data, uint32_t tid, RC& status, RecoveryOpState state)
{
    OperationCode opCode = *(OperationCode*)data;
    MOT_ASSERT(opCode == CREATE_INDEX);
    data += sizeof(OperationCode);
    size_t bufSize = 0;
    Extract(data, bufSize);
    switch (state) {
        case COMMIT:
        case TPC_COMMIT:
            CreateIndex((char*)data, tid, status);
            break;
        case ABORT:
        case TPC_APPLY:
        case TPC_ABORT:
            break;
        default:
            MOT_LOG_ERROR("RecoverLogOperationCreateIndex: bad state");
            status = RC_ERROR;
            break;
    }
    return sizeof(OperationCode) + sizeof(bufSize) + sizeof(uint32_t) + bufSize;  // sizeof(uint32_t) is for tableId
}

uint32_t RecoveryManager::RecoverLogOperationDropIndex(uint8_t* data, RC& status, RecoveryOpState state)
{
    OperationCode opCode = *(OperationCode*)data;
    MOT_ASSERT(opCode == DROP_INDEX);
    data += sizeof(OperationCode);
    char* extracted = (char*)data;
    switch (state) {
        case COMMIT:
        case TPC_COMMIT:
            DropIndex(extracted, status);
            break;
        case ABORT:
        case TPC_APPLY:
        case TPC_ABORT:
            break;
        default:
            MOT_LOG_ERROR("RecoverLogOperationDropIndex: bad state");
            status = RC_ERROR;
            break;
    }
    uint32_t tableId = 0;
    size_t nameLen = 0;
    Extract(data, tableId);
    Extract(data, nameLen);
    return sizeof(OperationCode) + sizeof(uint32_t) + sizeof(size_t) + nameLen;
}

uint32_t RecoveryManager::RecoverLogOperationTruncateTable(uint8_t* data, RC& status, RecoveryOpState state)
{
    OperationCode opCode = *(OperationCode*)data;
    MOT_ASSERT(opCode == TRUNCATE_TABLE);
    data += sizeof(OperationCode);
    switch (state) {
        case COMMIT:
        case TPC_COMMIT:
            TruncateTable((char*)data, status);
            break;
        case ABORT:
        case TPC_APPLY:
        case TPC_ABORT:
            break;
        default:
            MOT_LOG_ERROR("RecoverLogOperationTruncateTable: bad state");
            status = RC_ERROR;
            break;
    }
    return sizeof(OperationCode) + sizeof(uint32_t);
}

uint32_t RecoveryManager::RecoverLogOperationInsert(
    uint8_t* data, uint64_t csn, uint32_t tid, SurrogateState& sState, RC& status)
{
    uint64_t tableId, rowLength, exId;
    uint16_t keyLength;
    uint8_t* keyData = nullptr;
    uint8_t* rowData = nullptr;
    uint64_t row_id;

    OperationCode opCode = *(OperationCode*)data;
    MOT_ASSERT(opCode == CREATE_ROW);
    data += sizeof(OperationCode);

    Extract(data, tableId);
    Extract(data, exId);
    Extract(data, row_id);
    Extract(data, keyLength);
    keyData = ExtractPtr(data, keyLength);
    Extract(data, rowLength);
    rowData = ExtractPtr(data, rowLength);
    InsertRow(tableId, exId, (char*)keyData, keyLength, (char*)rowData, rowLength, csn, tid, sState, status, row_id);
    if (MOT::GetRecoveryManager()->m_logStats != nullptr)
        MOT::GetRecoveryManager()->m_logStats->IncInsert(tableId);
    return sizeof(OperationCode) + sizeof(tableId) + sizeof(exId) + sizeof(row_id) + sizeof(keyLength) + keyLength +
           sizeof(rowLength) + rowLength;
}

uint32_t RecoveryManager::RecoverLogOperationUpdate(uint8_t* data, uint64_t csn, uint32_t tid, RC& status)
{
    uint64_t tableId, rowLength, exId;
    uint16_t keyLength;
    uint8_t *keyData, *rowData;
    status = RC_OK;

    OperationCode opCode = *(OperationCode*)data;
    MOT_ASSERT(opCode == UPDATE_ROW);
    data += sizeof(OperationCode);

    Extract(data, tableId);
    Extract(data, exId);
    Extract(data, keyLength);
    keyData = ExtractPtr(data, keyLength);

    Table* table = GetTableManager()->GetTable(tableId);
    if (table == nullptr) {
        status = RC_ERROR;
        MOT_REPORT_ERROR(MOT_ERROR_INVALID_ARG,
            "Recovery Manager Update Row",
            "table %u with exId %lu does not exist",
            tableId,
            exId);
        return 0;
    }

    uint64_t tableExId = table->GetTableExId();
    if (tableExId != exId) {
        status = RC_ERROR;
        MOT_REPORT_ERROR(
            MOT_ERROR_INTERNAL, "Recovery Manager Update Row", "exId mismatch: my %lu - pkt %lu", tableExId, exId);
        return 0;
    }

    Index* index = table->GetPrimaryIndex();
    Key* key = index->CreateNewKey();
    if (key == nullptr) {
        status = RC_ERROR;
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Recovery Manager Update Row", "failed to allocate key");
        return 0;
    }
    key->CpKey((const uint8_t*)keyData, keyLength);
    Row* row = index->IndexRead(key, tid);

    if (row == nullptr) {
        /// row not found...
        /// error, got an update for non existing row
        index->DestroyKey(key);
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
            "Recovery Manager Update Row",
            "row not found, key: %s, tableId: %lu",
            key->GetKeyStr().c_str(),
            tableId);
        status = RC_ERROR;
        return 0;
    }

    bool doUpdate = true;

    // In case row has higher CSN, don't perform the update
    // we still need to calculate the length of the operation
    // in order to skip for the next one
    // CSNs can be equal if updated during the same transaction
    if (row->GetCommitSequenceNumber() > csn) {
        MOT_LOG_WARN("RecoveryManager::updateRow, tableId: %llu -  row csn is newer! %llu > %llu {%s}",
            tableId,
            row->GetCommitSequenceNumber(),
            csn,
            key->GetKeyStr().c_str());
        doUpdate = false;
    }

    uint16_t num_columns = table->GetFieldCount() - 1;
    rowData = const_cast<uint8_t*>(row->GetData());
    BitmapSet row_valid_columns(rowData + table->GetFieldOffset((uint64_t)0), num_columns);
    BitmapSet updated_columns(ExtractPtr(data, BitmapSet::GetLength(num_columns)), num_columns);
    BitmapSet valid_columns(ExtractPtr(data, BitmapSet::GetLength(num_columns)), num_columns);
    BitmapSet::BitmapSetIterator updated_columns_it(updated_columns);
    BitmapSet::BitmapSetIterator valid_columns_it(valid_columns);
    uint64_t size = 0;
    errno_t erc;
    while (!updated_columns_it.End()) {
        if (updated_columns_it.IsSet()) {
            if (valid_columns_it.IsSet()) {
                Column* column = table->GetField(updated_columns_it.GetPosition() + 1);
                if (doUpdate) {
                    row_valid_columns.SetBit(updated_columns_it.GetPosition());
                    erc = memcpy_s(rowData + column->m_offset, column->m_size, data, column->m_size);
                    securec_check(erc, "\0", "\0");
                }
                size += column->m_size;
                data += column->m_size;
            } else {
                if (doUpdate) {
                    row_valid_columns.UnsetBit(updated_columns_it.GetPosition());
                }
            }
        }
        valid_columns_it.Next();
        updated_columns_it.Next();
    }

    index->DestroyKey(key);
    if (MOT::GetRecoveryManager()->m_logStats != nullptr && doUpdate)
        MOT::GetRecoveryManager()->m_logStats->IncUpdate(tableId);
    return sizeof(OperationCode) + sizeof(tableId) + sizeof(exId) + sizeof(keyLength) + keyLength +
           updated_columns.GetLength() + valid_columns.GetLength() + size;
}

uint32_t RecoveryManager::RecoverLogOperationOverwrite(
    uint8_t* data, uint64_t csn, uint32_t tid, SurrogateState& sState, RC& status)
{
    uint64_t tableId, rowLength, exId;
    uint16_t keyLength;
    uint8_t* keyData = nullptr;
    uint8_t* rowData = nullptr;

    OperationCode opCode = *(OperationCode*)data;
    MOT_ASSERT(opCode == OVERWRITE_ROW);
    data += sizeof(OperationCode);

    Extract(data, tableId);
    Extract(data, exId);
    Extract(data, keyLength);
    keyData = ExtractPtr(data, keyLength);
    Extract(data, rowLength);
    rowData = ExtractPtr(data, rowLength);

    UpdateRow(tableId, exId, (char*)keyData, keyLength, (char*)rowData, rowLength, csn, tid, sState, status);
    if (MOT::GetRecoveryManager()->m_logStats != nullptr)
        MOT::GetRecoveryManager()->m_logStats->IncUpdate(tableId);
    return sizeof(OperationCode) + sizeof(tableId) + sizeof(exId) + sizeof(keyLength) + keyLength + sizeof(rowLength) +
           rowLength;
}

uint32_t RecoveryManager::RecoverLogOperationDelete(uint8_t* data, uint64_t csn, uint32_t tid, RC& status)
{
    uint64_t tableId, exId;
    uint16_t keyLength;
    uint8_t* keyData = nullptr;

    OperationCode opCode = *(OperationCode*)data;
    MOT_ASSERT(opCode == REMOVE_ROW);
    data += sizeof(OperationCode);

    Extract(data, tableId);
    Extract(data, exId);
    Extract(data, keyLength);
    keyData = ExtractPtr(data, keyLength);

    DeleteRow(tableId, exId, (char*)keyData, keyLength, csn, tid, status);
    if (MOT::GetRecoveryManager()->m_logStats != nullptr)
        MOT::GetRecoveryManager()->m_logStats->IncDelete(tableId);
    return sizeof(OperationCode) + sizeof(tableId) + sizeof(exId) + sizeof(keyLength) + keyLength;
}

uint32_t RecoveryManager::RecoverLogOperationCommit(uint8_t* data, uint64_t csn, uint32_t tid)
{
    // OperationCode + CSN + transaction_type + commit_counter + transaction_id
    if (MOT::GetRecoveryManager()->m_logStats != nullptr)
        MOT::GetRecoveryManager()->m_logStats->m_tcls++;
    return sizeof(EndSegmentBlock);
}

void RecoveryManager::InsertRow(uint64_t tableId, uint64_t exId, char* keyData, uint16_t keyLen, char* rowData,
    uint64_t rowLen, uint64_t csn, uint32_t tid, SurrogateState& sState, RC& status, uint64_t rowId, bool insertLocked)
{
    Table* table = nullptr;
    if (!GetRecoveryManager()->FetchTable(tableId, table)) {
        status = RC_ERROR;
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL, "RecoveryManager::insertRow", "Table %llu does not exist", tableId);
        return;
    }

    uint64_t tableExId = table->GetTableExId();
    if (tableExId != exId) {
        status = RC_ERROR;
        MOT_REPORT_ERROR(
            MOT_ERROR_INTERNAL, "Recovery Manager Insert Row", "exId mismatch: my %llu - pkt %llu", tableExId, exId);
        return;
    }

    Row* row = table->CreateNewRow();
    if (row == nullptr) {  // OA: check result before using pointer
        status = RC_ERROR;
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Recovery Manager Insert Row", "failed to create row");
        return;
    }
    row->CopyData((const uint8_t*)rowData, rowLen);
    row->SetCommitSequenceNumber(csn);

    if (insertLocked == true) {
        row->SetTwoPhaseMode(true);
        row->m_rowHeader.Lock();
    }

    uint64_t surrogateprimaryKey = 0;
    uint8_t* keyBytes = nullptr;
    Key* key = nullptr;
    MOT::Index* ix = nullptr;
    uint32_t numIndexes = table->GetNumIndexes();

    ix = table->GetPrimaryIndex();
    key = ix->CreateNewKey();
    if (key == nullptr) {
        table->DestroyRow(row);
        status = RC_ERROR;
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Recovery Manager Insert Row", "failed to create key");
        return;
    }
    if (ix->IsFakePrimary()) {
        row->SetSurrogateKey(*(uint64_t*)keyData);
        sState.UpdateMaxKey(rowId);
    }
    key->CpKey((const uint8_t*)keyData, keyLen);
    status = table->InsertRowNonTransactional(row, tid, key);

    if (insertLocked == true) {
        row->GetPrimarySentinel()->Lock(0);
    }

    if (status == RC_UNIQUE_VIOLATION && DuplicateRow(table, keyData, keyLen, rowData, rowLen, tid)) {
        /* Same row already exists. ok. */
        table->DestroyRow(row);
        status = RC_OK;
    } else if (status == RC_MEMORY_ALLOCATION_ERROR) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Recovery Manager Insert Row", "failed to insert row");
        table->DestroyRow(row);
    } else if (status != RC_OK) {
        table->DestroyRow(row);
    }

    ix->DestroyKey(key);
}

void RecoveryManager::DeleteRow(
    uint64_t tableId, uint64_t exId, char* keyData, uint16_t keyLen, uint64_t csn, uint32_t tid, RC& status)
{
    Sentinel* pSentinel = nullptr;
    RC rc;
    Row* row = nullptr;
    Key* key = nullptr;
    Index* index = nullptr;
    uint64_t tableExId;

    Table* table = nullptr;
    if (!GetRecoveryManager()->FetchTable(tableId, table)) {
        MOT_REPORT_ERROR(MOT_ERROR_INVALID_ARG, "Recovery Manager Delete Row", "table %u does not exist", tableId);
        status = RC_ERROR;
        MOT_LOG_ERROR("RecoveryManager::deleteRow - table %u does not exist", tableId);
        return;
    }

    index = table->GetPrimaryIndex();
    key = index->CreateNewKey();
    if (key == nullptr) {  // OA: check result before using pointer
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Recovery Manager Delete Row", "failed to create key");
        status = RC_ERROR;
        return;
    }
    key->CpKey((const uint8_t*)keyData, keyLen);

    tableExId = table->GetTableExId();
    if (tableExId != exId) {
        MOT_REPORT_ERROR(
            MOT_ERROR_INTERNAL, "Recovery Manager Delete Row", "exId mismatch: my %lu - pkt %lu", tableExId, exId);
        status = RC_ERROR;
        index->DestroyKey(key);
        return;
    }
    rc = table->FindRow(key, pSentinel, tid);
    if ((rc != RC_OK) || (pSentinel == 0)) {
        MOT_LOG_ERROR("RecoveryManager::deleteRow - findRow rc %u, sentinel %p", rc, pSentinel);
        status = RC_OK;
        index->DestroyKey(key);
        return;
    }

    row = pSentinel->GetData();
    if (row != 0) {
        if (!table->RemoveRow(row, tid)) {
            if (MOT_IS_OOM()) {
                // OA: report error if remove row failed due to OOM (what about other errors?)
                MOT_REPORT_ERROR(
                    MOT_ERROR_OOM, "Recovery Manager Delete Row", "failed to remove row due to lack of memory");
                status = RC_MEMORY_ALLOCATION_ERROR;
            } else {
                status = RC_ERROR;
            }
            MOT_LOG_ERROR("RecoveryManager::deleteRow2 - findRow rc %u, sentinel %p", rc, pSentinel);
        } else {
            GetRecoveryManager()->IncreaseTableDeletesStat(table);
        }
    } else {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL, "Recovery Manager Delete Row", "getData failed");
        status = RC_ERROR;
    }
    index->DestroyKey(key);
    status = RC_OK;
}

void RecoveryManager::UpdateRow(uint64_t tableId, uint64_t exId, char* keyData, uint16_t keyLen, char* rowData,
    uint64_t rowLen, uint64_t csn, uint32_t tid, SurrogateState& sState, RC& status)
{
    Table* table = nullptr;
    if (!GetRecoveryManager()->FetchTable(tableId, table)) {
        status = RC_ERROR;
        MOT_REPORT_ERROR(MOT_ERROR_INVALID_ARG, "Recovery Manager Update Row", "table %u does not exist", tableId);
        return;
    }

    uint64_t tableExId = table->GetTableExId();
    if (tableExId != exId) {
        status = RC_ERROR;
        MOT_REPORT_ERROR(
            MOT_ERROR_INTERNAL, "Recovery Manager Update Row", "exId mismatch: my %lu - pkt %lu", tableExId, exId);
        return;
    }

    Index* index = table->GetPrimaryIndex();
    Key* key = index->CreateNewKey();
    if (key == nullptr) {  // OA: check result before using pointer
        status = RC_ERROR;
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Recovery Manager Update Row", "failed to create key");
        return;
    }
    key->CpKey((const uint8_t*)keyData, keyLen);
    Row* row = index->IndexRead(key, tid);
    if (row == nullptr) {
        /// row not found... need to check row version
        // if row version is less than the updated row version it means that we
        // missed an insert. Treat this update as an insert
        // in order to avoid code copy, I just create a new insert operation
        // and replay it
        MOT_LOG_DEBUG("RecoveryManager::updateRow - row not found - inserting");
        InsertRow(tableId, exId, keyData, keyLen, rowData, rowLen, csn, tid, sState, status, 0 /* row id hack */);
    } else {
        // CSNs can be equal if updated during the same transaction
        if (row->GetCommitSequenceNumber() <= csn) {
            row->CopyData((const uint8_t*)rowData, rowLen);
            row->SetCommitSequenceNumber(csn);
            if (row->IsAbsentRow()) {
                row->UnsetAbsentRow();
            }
        } else {
            MOT_LOG_DEBUG("RecoveryManager::updateRow [%d] -  row csn is newer! %d > %d ",
                tableId,
                row->GetCommitSequenceNumber(),
                csn);
        }
    }
    index->DestroyKey(key);
}

void RecoveryManager::CreateTable(char* data, RC& status, Table*& table, bool addToEngine)
{
    /* first verify that the table does not exists */
    string name;
    string longName;
    uint32_t intId = 0;
    uint64_t extId = 0;
    Table::DeserializeNameAndIds((const char*)data, intId, extId, name, longName);
    MOT_LOG_DEBUG("CreateTable: got intId: %u, extId: %lu, %s/%s", intId, extId, name.c_str(), longName.c_str());
    if (GetTableManager()->VerifyTableExists(intId, extId, name, longName)) {
        MOT_LOG_DEBUG("CreateTable: table %u already exists", intId);
        return;
    }

    table = new (std::nothrow) Table();
    if (table == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Recovery Manager Create Table", "failed to allocate table's memory");
        status = RC_ERROR;
        return;
    }

    table->Deserialize((const char*)data);
    do {
        if (!table->IsDeserialized()) {
            MOT_LOG_ERROR("RecoveryManager::CreateTable: failed to deserialize table");
            break;
        }

        if (addToEngine && !GetTableManager()->AddTable(table)) {
            MOT_LOG_ERROR("RecoveryManager::CreateTable: failed to add table to engine");
            break;
        }

        MOT_LOG_DEBUG("RecoveryManager::CreateTable: table %s [%lu] created (%s to engine)",
            table->GetLongTableName().c_str(),
            table->GetTableId(),
            addToEngine ? "added" : "not added");
        status = RC_OK;
        return;

    } while (0);

    MOT_LOG_ERROR("RecoveryManager::CreateTable: failed to recover table");
    delete table;
    status = RC_ERROR;
    return;
}

void RecoveryManager::DropTable(char* data, RC& status)
{
    char* in = (char*)data;
    uint32_t tableId;
    Table* table;
    string tableName;

    in = SerializablePOD<uint32_t>::Deserialize(in, tableId);
    table = GetTableManager()->GetTable(tableId);
    if (table == nullptr) {
        MOT_LOG_DEBUG("DropTable: could not find table %u", tableId);
        /* this might happen if we try to replay an outdated xlog entry - currently we do not error out */
        return;
    }
    tableName.assign(table->GetLongTableName());
    if (GetTableManager()->DropTable(table, MOT_GET_CURRENT_SESSION_CONTEXT()) != RC_OK) {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
            "Recovery Manager Drop Table",
            "Failed to drop table %s [%lu])",
            tableName.c_str(),
            tableId);
        status = RC_ERROR;
    } else {
        MOT::GetRecoveryManager()->m_tableDeletesStat.erase(table);
    }
    MOT_LOG_DEBUG("RecoveryManager::DropTable: table %s [%lu] dropped", tableName.c_str(), tableId);
}

void RecoveryManager::CreateIndex(char* data, uint32_t tid, RC& status)
{
    char* in = (char*)data;
    uint32_t tableId;
    Table* table;
    Table::CommonIndexMeta idx;

    in = SerializablePOD<uint32_t>::Deserialize(in, tableId);
    table = GetTableManager()->GetTable(tableId);
    if (table == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_INVALID_ARG, "Recovery Manager Create Index", "Could not find table %u", tableId);
        status = RC_ERROR;
        return;
    }

    in = table->DesrializeMeta(in, idx);
    if (idx.m_indexOrder == IndexOrder::INDEX_ORDER_PRIMARY) {
        MOT_LOG_DEBUG("createIndex: creating Primary Index");
        table->CreateIndexFromMeta(idx, true, tid);
    } else {
        MOT_LOG_DEBUG("createIndex: creating Secondary Index");
        table->CreateIndexFromMeta(idx, false, tid);
    }
}

void RecoveryManager::DropIndex(char* data, RC& status)
{
    RC res;
    char* in = (char*)data;
    uint32_t tableId;
    Table* table;
    uint32_t indexNameLength;
    string indexName;

    in = SerializablePOD<uint32_t>::Deserialize(in, tableId);
    table = GetTableManager()->GetTable(tableId);
    if (table == nullptr) {
        /* this might happen if we try to replay an outdated xlog entry - currently we do not error out */
        MOT_LOG_DEBUG("dropIndex: could not find table %u", tableId);
        return;
    }

    in = SerializableSTR::Deserialize(in, indexName);
    res = table->RemoveSecondaryIndex((char*)(indexName.c_str()), MOT_GET_CURRENT_SESSION_CONTEXT()->GetTxnManager());
    if (res != RC_OK) {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
            "Recovery Manager Drop Index",
            "Drop index %s error, secondary index not found.",
            indexName.c_str());
        status = RC_ERROR;
    }
}

void RecoveryManager::TruncateTable(char* data, RC& status)
{
    RC res = RC_OK;
    char* in = (char*)data;
    uint32_t tableId;
    Table* table;

    in = SerializablePOD<uint32_t>::Deserialize(in, tableId);
    table = GetTableManager()->GetTable(tableId);
    if (table == nullptr) {
        /* this might happen if we try to replay an outdated xlog entry - currently we do not error out */
        MOT_LOG_DEBUG("truncateTable: could not find table %u", tableId);
        return;
    }

    table->Truncate(MOT_GET_CURRENT_SESSION_CONTEXT()->GetTxnManager());
}

// in-process (2pc) transactions recovery
uint32_t RecoveryManager::TwoPhaseRecoverOp(RecoveryOpState state, uint8_t* data, uint64_t csn, uint64_t transactionId,
    uint32_t tid, SurrogateState& sState, RC& status)
{
    OperationCode opCode = *static_cast<OperationCode*>((void*)data);
    uint64_t tableId;
    uint64_t rowLength = 0;
    uint64_t exId;
    uint16_t keyLength;
    uint64_t rowId = 0;
    uint8_t* keyData = nullptr;
    uint8_t* rowData = nullptr;

    if (IsSupportedOp(opCode) == false) {
        MOT_REPORT_ERROR(MOT_ERROR_INVALID_ARG,
            "Recovery Manager 2PC Op",
            "applyInProcessOperation: op %u is not supported!",
            opCode);
        status = RC_ERROR;
        return 0;
    }

    // DDLs
    if (opCode == CREATE_TABLE)
        return RecoverLogOperationCreateTable(data, status, state, transactionId);

    if (opCode == DROP_TABLE)
        return RecoverLogOperationDropTable(data, status, state);

    if (opCode == CREATE_INDEX)
        return RecoverLogOperationCreateIndex(data, tid, status, state);

    if (opCode == DROP_INDEX)
        return RecoverLogOperationDropIndex(data, status, state);

    if (opCode == TRUNCATE_TABLE)
        return RecoverLogOperationTruncateTable(data, status, state);

    if (opCode == PREPARE_TX || opCode == COMMIT_PREPARED_TX)
        return sizeof(EndSegmentBlock);

    // DMLs
    data += sizeof(OperationCode);
    Extract(data, tableId);
    Extract(data, exId);
    if (opCode == CREATE_ROW)
        Extract(data, rowId);
    Extract(data, keyLength);
    keyData = ExtractPtr(data, keyLength);
    if (opCode != REMOVE_ROW) {
        Extract(data, rowLength);
        rowData = ExtractPtr(data, rowLength);
    }

    size_t ret = (opCode == REMOVE_ROW)
                     ? sizeof(OperationCode) + sizeof(tableId) + sizeof(exId) + sizeof(keyLength) + keyLength
                     : sizeof(OperationCode) + sizeof(tableId) + sizeof(exId) + sizeof(keyLength) + keyLength +
                           sizeof(rowLength) + rowLength;
    if (opCode == CREATE_ROW)
        ret += sizeof(uint64_t);  // rowId

    Table* table = nullptr;
    if (!GetRecoveryManager()->FetchTable(tableId, table)) {
        status = RC_ERROR;
        MOT_LOG_ERROR("RecoveryManager::applyInProcessInsert: fetch table failed (id %lu)", tableId);
        return ret;
    }

    uint64_t tableExId = table->GetTableExId();
    if (tableExId == exId) {
        Index* index = table->GetPrimaryIndex();
        Key* key = index->CreateNewKey();
        if (key == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Recovery Manager 2PC Op", "failed to create key");
            status = RC_ERROR;
            return 0;
        }
        key->CpKey((const uint8_t*)keyData, keyLength);
        Row* row = index->IndexRead(key, tid);

        switch (state) {
            case RecoveryOpState::TPC_APPLY:
                RecoverTwoPhaseApply(opCode,
                    tableId,
                    exId,
                    csn,
                    keyData,
                    keyLength,
                    rowData,
                    rowLength,
                    transactionId,
                    tid,
                    row,
                    rowId,
                    sState,
                    status);
                break;

            case RecoveryOpState::TPC_COMMIT:
                RecoverTwoPhaseCommit(table, opCode, csn, rowData, rowLength, transactionId, tid, row, status);
                break;

            case RecoveryOpState::TPC_ABORT:
                RecoverTwoPhaseAbort(table, opCode, csn, transactionId, tid, row, status);
                break;

            default:
                MOT_REPORT_ERROR(MOT_ERROR_INTERNAL, "Recovery Manager 2PC Op", "bad state");
                break;
        }
        index->DestroyKey(key);
    } else {
        status = RC_ERROR;
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
            "Recovery Manager 2PC Op",
            "meta-data error for table %lu:%lu --> %p:%lu",
            tableId,
            exId,
            table,
            tableExId);
    }
    return ret;
}

/*
 * +-----------+-----------------+---------------+
 * | Operation |      Found      |  Not Found    |
 * +-----------+-----------------+---------------+
 * | INSERT    | Replace Locked  | Insert locked |
 * | UPDATE    | Lock            | ERROR         |
 * | DELETE    | Lock            | ERROR         |
 * +-----------+-----------------+---------------+
 */
void RecoveryManager::RecoverTwoPhaseApply(OperationCode opCode, uint64_t tableId, uint64_t exId, uint64_t csn,
    uint8_t* keyData, uint64_t keyLength, uint8_t* rowData, uint64_t rowLength, uint64_t transactionId, uint32_t tid,
    Row* row, uint64_t rowId, SurrogateState& sState, RC& status)
{
    if (row == nullptr) {
        if (opCode == UPDATE_ROW || opCode == OVERWRITE_ROW || opCode == REMOVE_ROW) {
            MOT_REPORT_ERROR(
                MOT_ERROR_INVALID_ARG, "Recovery Manager 2PC Apply", "got op %u but row does not exist!", opCode);
        } else {
            MOT_LOG_DEBUG("recoverTwoPhaseApply: insert row [%lu]", transactionId);
            InsertRow(tableId,
                exId,
                (char*)keyData,
                keyLength,
                (char*)rowData,
                rowLength,
                csn,
                tid,
                sState,
                status,
                rowId,
                true);
        }
    } else {
        row->SetTwoPhaseMode(true);  // set 2pc recovery mode indication
        row->m_rowHeader.TryLock();
        if (opCode == CREATE_ROW) {
            MOT_LOG_DEBUG("recoverTwoPhaseApply: insert - updating row [%lu]", transactionId);
            row->CopyData((const uint8_t*)rowData, rowLength);
            row->SetCommitSequenceNumber(csn);
        } else
            MOT_LOG_DEBUG("recoverTwoPhaseApply: update / delete [%lu]", transactionId);
    }
}

/*
 * +-----------+---------------------+-----------+
 * | Operation |        Found        | Not Found |
 * +-----------+---------------------+-----------+
 * | INSERT    | Unlock              | ERROR     |
 * | UPDATE    | Replace row, unlock | ERROR     |
 * | DELETE    | Delete row          | ERROR     |
 * +-----------+---------------------+-----------+
 */
void RecoveryManager::RecoverTwoPhaseCommit(Table* table, OperationCode opCode, uint64_t csn, uint8_t* rowData,
    uint64_t rowLength, uint64_t transactionId, uint32_t tid, Row* row, RC& status)
{
    if (row == nullptr) {
        MOT_REPORT_ERROR(
            MOT_ERROR_INVALID_ARG, "Recovery Manager 2PC Commit", "row does not exist! [%lu]", transactionId);
    } else {
        if (opCode == CREATE_ROW) {
            MOT_LOG_DEBUG("recoverTwoPhaseCommit: - insert row [%lu]", transactionId);
            row->GetPrimarySentinel()->TryLock(tid);
            row->m_rowHeader.TryLock();
            row->SetTwoPhaseMode(false);
            row->GetPrimarySentinel()->Release();
            row->m_rowHeader.Release();
        } else if (opCode == REMOVE_ROW) {
            MOT_LOG_DEBUG("recoverTwoPhaseCommit: - remove row [%lu]", transactionId);
            if (!table->RemoveRow(row, tid)) {
                if (MOT_IS_OOM()) {
                    // OA: report error if remove row failed due to OOM (what about other errors?)
                    MOT_REPORT_ERROR(
                        MOT_ERROR_OOM, "Recovery Manager 2PC Commit", "failed to remove row due to lack of memory");
                    status = RC_MEMORY_ALLOCATION_ERROR;
                } else {
                    status = RC_ERROR;
                }
            }
        } else {
            MOT_LOG_DEBUG("recoverTwoPhaseCommit: -  update row [%lu]", transactionId);
            row->GetPrimarySentinel()->TryLock(tid);
            row->m_rowHeader.TryLock();
            row->CopyData((const uint8_t*)rowData, rowLength);
            row->SetCommitSequenceNumber(csn);
            if (row->IsAbsentRow())
                row->UnsetAbsentRow();
            row->SetTwoPhaseMode(false);
            row->GetPrimarySentinel()->Release();
            row->m_rowHeader.Release();
        }
    }
}

/*
 * +-----------+---------+-----------+
 * | Operation |  Found  | Not Found |
 * +-----------+---------+-----------+
 * | INSERT    | Del row | ignore    |
 * | UPDATE    | unlock  | ignore    |
 * | DELETE    | unlock  | ignore    |
 * +-----------+---------+-----------+
 */
void RecoveryManager::RecoverTwoPhaseAbort(
    Table* table, OperationCode opCode, uint64_t csn, uint64_t transactionId, uint32_t tid, Row* row, RC& status)
{
    if (row == nullptr) {
        MOT_REPORT_ERROR(
            MOT_ERROR_INVALID_ARG, "Recovery Manager 2PC Abort", "row does not exist! [%lu]", transactionId);
    } else {
        if (opCode == CREATE_ROW) {
            MOT_LOG_DEBUG("recoverTwoPhaseAbort - insert row [%lu]", transactionId);
            if (!table->RemoveRow(row, tid)) {
                if (MOT_IS_OOM()) {
                    // OA: report error if remove row failed due to OOM (what about other errors?)
                    MOT_REPORT_ERROR(
                        MOT_ERROR_OOM, "Recovery Manager 2PC Abort", "failed to remove row due to lack of memory");
                    status = RC_MEMORY_ALLOCATION_ERROR;
                } else {
                    status = RC_ERROR;
                }
            }
        } else {
            MOT_LOG_DEBUG("recoverTwoPhaseAbort - remove / update row [%lu]", transactionId);
            row->GetPrimarySentinel()->TryLock(tid);
            row->m_rowHeader.TryLock();
            row->SetTwoPhaseMode(false);
            row->GetPrimarySentinel()->Release();
            row->m_rowHeader.Release();
        }
    }
}

bool RecoveryManager::DuplicateRow(
    Table* table, char* keyData, uint16_t keyLen, char* rowData, uint64_t rowLen, uint32_t tid)
{
    bool res = false;
    Key* key = nullptr;
    RC rc = RC_ERROR;
    Row* row = nullptr;
    Index* index = nullptr;
    do {
        index = table->GetPrimaryIndex();
        if (index == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL, "Recovery Manager Duplicate Row", "failed to find the primary index");
            break;
        }
        key = index->CreateNewKey();
        if (key == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Recovery Manager Duplicate Row", "failed to create a key");
            break;
        }
        key->CpKey((const uint8_t*)keyData, keyLen);

        Row* row = index->IndexRead(key, tid);
        if (row == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL, "Recovery Manager Duplicate Row", "failed to find row");
            break;
        }
        if (memcmp(row->GetData(), rowData, rowLen)) {
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                "Recovery Manager Duplicate Row",
                "rows differ! (Table %lu:%u:%s nidx: %u)",
                table->GetTableExId(),
                table->GetTableId(),
                table->GetTableName().c_str(),
                table->GetNumIndexes());
            CheckpointUtils::Hexdump("New Row", rowData, rowLen);
            CheckpointUtils::Hexdump("Orig Row", (char*)row->GetData(), rowLen);
            break;
        }
        res = true;
    } while (0);

    if (key != nullptr && index != nullptr)
        index->DestroyKey(key);
    return res;
}
}  // namespace MOT
