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
 *    src/gausskernel/storage/mot/core/system/recovery/recovery_ops.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "mot_engine.h"
#include "serializable.h"
#include "recovery_manager.h"
#include "checkpoint_utils.h"
#include "bitmapset.h"
#include "column.h"
#include "txn_insert_action.h"

#include <string>
#include <algorithm>

namespace MOT {
DECLARE_LOGGER(RecoveryOps, Recovery);

uint32_t RecoveryOps::RecoverLogOperation(TxnManager* txn, uint8_t* data, uint64_t csn, uint64_t transactionId,
    uint32_t tid, SurrogateState& sState, RC& status, bool& wasCommit)
{
    if (txn == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL, "%s: invalid TxnManager object", __FUNCTION__);
        status = RC_ERROR;
        return 0;
    }

    OperationCode opCode = *static_cast<OperationCode*>((void*)data);
    switch (opCode) {
        case CREATE_ROW:
            return RecoverLogOperationInsert(txn, data, csn, tid, sState, status);
        case UPDATE_ROW:
            return RecoverLogOperationUpdate(txn, data, csn, tid, status);
        case OVERWRITE_ROW:
            return RecoverLogOperationOverwrite(txn, data, csn, tid, sState, status);
        case REMOVE_ROW:
            return RecoverLogOperationDelete(txn, data, csn, tid, status);
        case CREATE_TABLE:
            return RecoverLogOperationCreateTable(txn, data, status, COMMIT, transactionId);
        case CREATE_INDEX:
            return RecoverLogOperationCreateIndex(txn, data, tid, status, COMMIT);
        case DROP_TABLE:
            return RecoverLogOperationDropTable(txn, data, status, COMMIT);
        case DROP_INDEX:
            return RecoverLogOperationDropIndex(txn, data, status, COMMIT);
        case TRUNCATE_TABLE:
            return RecoverLogOperationTruncateTable(txn, data, status, COMMIT);
        case COMMIT_TX:
        case COMMIT_PREPARED_TX:
            wasCommit = true;
            return RecoverLogOperationCommit(txn, data, csn, tid, status);
        case PARTIAL_REDO_TX:
        case PREPARE_TX:
            return sizeof(EndSegmentBlock);
        case ROLLBACK_TX:
        case ROLLBACK_PREPARED_TX:
            return RecoverLogOperationRollback(txn, data, csn, tid, status);
        default:
            MOT_LOG_ERROR("Unknown recovery redo record op-code: %u", (unsigned)opCode);
            status = RC_ERROR;
            return 0;
    }
}

uint32_t RecoveryOps::RecoverLogOperationCreateTable(
    TxnManager* txn, uint8_t* data, RC& status, RecoveryOpState state, uint64_t transactionId)
{
    if (GetGlobalConfiguration().m_enableIncrementalCheckpoint) {
        status = RC_ERROR;
        MOT_LOG_ERROR("RecoverLogOperationCreateTable: failed to create table. "
                      "MOT does not support incremental checkpoint");
        return 0;
    }

    uint32_t tableId;
    uint64_t extId;
    uint32_t metaVersion;
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

    MOT_LOG_DEBUG("RecoverLogOperationCreateTable: state %u", state);

    switch (state) {
        case COMMIT:
            MOT_LOG_DEBUG("RecoverLogOperationCreateTable: COMMIT");
            CreateTable(txn, (char*)data, status, table, TRANSACTIONAL);
            break;

        case TPC_APPLY:
            CreateTable(txn, (char*)data, status, table, DONT_ADD_TO_ENGINE);
            if (status == RC_OK && table != nullptr) {
                tableInfo = new (std::nothrow) TableInfo(table, transactionId);
                if (tableInfo != nullptr) {
                    ((RecoveryManager*)GetRecoveryManager())->m_preCommitedTables[table->GetTableId()] = tableInfo;
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
            Table::DeserializeNameAndIds((const char*)data, metaVersion, tableId, extId, tableName, longName);
            it = ((RecoveryManager*)GetRecoveryManager())->m_preCommitedTables.find(tableId);
            if (it != ((RecoveryManager*)GetRecoveryManager())->m_preCommitedTables.end()) {
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
                ((RecoveryManager*)GetRecoveryManager())->m_preCommitedTables.erase(it);
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

uint32_t RecoveryOps::RecoverLogOperationDropTable(TxnManager* txn, uint8_t* data, RC& status, RecoveryOpState state)
{
    OperationCode opCode = *(OperationCode*)data;
    MOT_ASSERT(opCode == DROP_TABLE);
    data += sizeof(OperationCode);
    switch (state) {
        case COMMIT:
        case TPC_COMMIT:
            DropTable(txn, (char*)data, status);
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
    return sizeof(OperationCode) + sizeof(uint32_t) + sizeof(uint64_t);
}

uint32_t RecoveryOps::RecoverLogOperationCreateIndex(
    TxnManager* txn, uint8_t* data, uint32_t tid, RC& status, RecoveryOpState state)
{
    OperationCode opCode = *(OperationCode*)data;
    MOT_ASSERT(opCode == CREATE_INDEX);
    data += sizeof(OperationCode);
    size_t bufSize = 0;
    Extract(data, bufSize);
    switch (state) {
        case COMMIT:
        case TPC_COMMIT:
            CreateIndex(txn, (char*)data, tid, status);
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
    return sizeof(OperationCode) + sizeof(bufSize) + sizeof(uint32_t) + sizeof(uint64_t) + bufSize;
}

uint32_t RecoveryOps::RecoverLogOperationDropIndex(TxnManager* txn, uint8_t* data, RC& status, RecoveryOpState state)
{
    OperationCode opCode = *(OperationCode*)data;
    MOT_ASSERT(opCode == DROP_INDEX);
    data += sizeof(OperationCode);
    char* extracted = (char*)data;
    switch (state) {
        case COMMIT:
        case TPC_COMMIT:
            DropIndex(txn, extracted, status);
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
    uint64_t tableId = 0;
    size_t nameLen = 0;
    uint32_t metaVersion;
    Extract(data, metaVersion);
    Extract(data, tableId);
    Extract(data, nameLen);
    return sizeof(OperationCode) + sizeof(uint32_t) + sizeof(uint64_t) + sizeof(size_t) + nameLen;
}

uint32_t RecoveryOps::RecoverLogOperationTruncateTable(
    TxnManager* txn, uint8_t* data, RC& status, RecoveryOpState state)
{
    OperationCode opCode = *(OperationCode*)data;
    MOT_ASSERT(opCode == TRUNCATE_TABLE);
    data += sizeof(OperationCode);
    switch (state) {
        case COMMIT:
        case TPC_COMMIT:
            TruncateTable(txn, (char*)data, status);
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
    return sizeof(OperationCode) + sizeof(uint32_t) + sizeof(uint64_t);
}

uint32_t RecoveryOps::RecoverLogOperationInsert(
    TxnManager* txn, uint8_t* data, uint64_t csn, uint32_t tid, SurrogateState& sState, RC& status)
{
    uint64_t tableId, rowLength, exId;
    uint32_t metaVersion;
    uint16_t keyLength;
    uint8_t* keyData = nullptr;
    uint8_t* rowData = nullptr;
    uint64_t row_id;

    OperationCode opCode = *(OperationCode*)data;
    MOT_ASSERT(opCode == CREATE_ROW);
    data += sizeof(OperationCode);

    Extract(data, metaVersion);
    if (metaVersion > MetadataProtoVersion::METADATA_VER_CURR) {
        MOT_LOG_ERROR("CreateRow: metadata version %u is greater than current %u",
            metaVersion,
            MetadataProtoVersion::METADATA_VER_CURR);
        status = RC_ERROR;
        return 0;
    }
    Extract(data, tableId);
    Extract(data, exId);
    Extract(data, row_id);
    Extract(data, keyLength);
    keyData = ExtractPtr(data, keyLength);
    Extract(data, rowLength);
    rowData = ExtractPtr(data, rowLength);
    InsertRow(
        txn, tableId, exId, (char*)keyData, keyLength, (char*)rowData, rowLength, csn, tid, sState, status, row_id);
    if (((RecoveryManager*)GetRecoveryManager())->m_logStats != nullptr)
        ((RecoveryManager*)GetRecoveryManager())->m_logStats->IncInsert(tableId);
    return sizeof(OperationCode) + sizeof(uint32_t) + sizeof(tableId) + sizeof(exId) + sizeof(row_id) +
           sizeof(keyLength) + keyLength + sizeof(rowLength) + rowLength;
}

uint32_t RecoveryOps::RecoverLogOperationUpdate(TxnManager* txn, uint8_t* data, uint64_t csn, uint32_t tid, RC& status)
{
    uint64_t tableId;
    uint32_t metaVersion;
    uint64_t exId;
    uint16_t keyLength;
    uint8_t *keyData, *rowData;
    uint64_t version;

    status = RC_OK;

    OperationCode opCode = *(OperationCode*)data;
    MOT_ASSERT(opCode == UPDATE_ROW);
    data += sizeof(OperationCode);

    Extract(data, metaVersion);
    if (metaVersion > MetadataProtoVersion::METADATA_VER_CURR) {
        MOT_LOG_ERROR("UpdateRow: metadata version %u is greater than current %u",
            metaVersion,
            MetadataProtoVersion::METADATA_VER_CURR);
        status = RC_ERROR;
        return 0;
    }
    Extract(data, tableId);
    Extract(data, exId);
    Extract(data, keyLength);
    keyData = ExtractPtr(data, keyLength);
    Extract(data, version);

    Table* table = txn->GetTableByExternalId(exId);
    if (table == nullptr) {
        status = RC_ERROR;
        MOT_REPORT_ERROR(MOT_ERROR_INVALID_ARG,
            "Recovery Manager Update Row",
            "table %lu with exId %lu does not exist",
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
    Key* key = txn->GetTxnKey(index);
    if (key == nullptr) {
        status = RC_ERROR;
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Recovery Manager Update Row", "failed to allocate key");
        return 0;
    }

    key->CpKey((const uint8_t*)keyData, keyLength);
    Row* row = txn->RowLookupByKey(table, RD_FOR_UPDATE, key);
    if (row == nullptr) {
        // Row not found. Error!!! Got an update for non existing row.
        txn->DestroyTxnKey(key);
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
            "Recovery Manager Update Row",
            "row not found, key: %s, tableId: %lu",
            key->GetKeyStr().c_str(),
            tableId);
        status = RC_ERROR;
        return 0;
    }

    if (row->GetCommitSequenceNumber() > csn) {
        // Row CSN is newer. Error!!!
        txn->DestroyTxnKey(key);
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
            "Recovery Manager Update Row",
            "row CSN is newer! %lu > %lu, key: %s, tableId: %lu",
            row->GetCommitSequenceNumber(),
            csn,
            key->GetKeyStr().c_str(),
            tableId);
        status = RC_ERROR;
        return 0;
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
                row_valid_columns.SetBit(updated_columns_it.GetPosition());
                erc = memcpy_s(rowData + column->m_offset, column->m_size, data, column->m_size);
                securec_check(erc, "\0", "\0");
                size += column->m_size;
                data += column->m_size;
            } else {
                row_valid_columns.UnsetBit(updated_columns_it.GetPosition());
            }
        }
        valid_columns_it.Next();
        updated_columns_it.Next();
    }

    txn->UpdateLastRowState(MOT::AccessType::WR);
    txn->DestroyTxnKey(key);
    if (((RecoveryManager*)GetRecoveryManager())->m_logStats != nullptr)
        ((RecoveryManager*)GetRecoveryManager())->m_logStats->IncUpdate(tableId);
    return sizeof(OperationCode) + sizeof(uint32_t) + sizeof(tableId) + sizeof(exId) + sizeof(keyLength) + keyLength +
           sizeof(version) + updated_columns.GetLength() + valid_columns.GetLength() + size;
}

uint32_t RecoveryOps::RecoverLogOperationOverwrite(
    TxnManager* txn, uint8_t* data, uint64_t csn, uint32_t tid, SurrogateState& sState, RC& status)
{
    uint64_t tableId, rowLength, exId;
    uint32_t metaVersion;
    uint16_t keyLength;
    uint8_t* keyData = nullptr;
    uint8_t* rowData = nullptr;

    OperationCode opCode = *(OperationCode*)data;
    MOT_ASSERT(opCode == OVERWRITE_ROW);
    data += sizeof(OperationCode);

    Extract(data, metaVersion);
    if (metaVersion > MetadataProtoVersion::METADATA_VER_CURR) {
        MOT_LOG_ERROR("OverwriteRow: metadata version %u is greater than current %u",
            metaVersion,
            MetadataProtoVersion::METADATA_VER_CURR);
        status = RC_ERROR;
        return 0;
    }
    Extract(data, tableId);
    Extract(data, exId);
    Extract(data, keyLength);
    keyData = ExtractPtr(data, keyLength);
    Extract(data, rowLength);
    rowData = ExtractPtr(data, rowLength);

    UpdateRow(txn, tableId, exId, (char*)keyData, keyLength, (char*)rowData, rowLength, csn, tid, sState, status);
    if (((RecoveryManager*)GetRecoveryManager())->m_logStats != nullptr)
        ((RecoveryManager*)GetRecoveryManager())->m_logStats->IncUpdate(tableId);
    return sizeof(OperationCode) + sizeof(uint32_t) + sizeof(tableId) + sizeof(exId) + sizeof(keyLength) + keyLength +
           sizeof(rowLength) + rowLength;
}

uint32_t RecoveryOps::RecoverLogOperationDelete(TxnManager* txn, uint8_t* data, uint64_t csn, uint32_t tid, RC& status)
{
    uint64_t tableId, exId, version;
    uint32_t metaVersion;
    uint16_t keyLength;
    uint8_t* keyData = nullptr;

    OperationCode opCode = *(OperationCode*)data;
    MOT_ASSERT(opCode == REMOVE_ROW);
    data += sizeof(OperationCode);

    Extract(data, metaVersion);
    if (metaVersion > MetadataProtoVersion::METADATA_VER_CURR) {
        MOT_LOG_ERROR("DeleteRow: metadata version %u is greater than current %u",
            metaVersion,
            MetadataProtoVersion::METADATA_VER_CURR);
        status = RC_ERROR;
        return 0;
    }
    Extract(data, tableId);
    Extract(data, exId);
    Extract(data, keyLength);
    keyData = ExtractPtr(data, keyLength);
    Extract(data, version);

    DeleteRow(txn, tableId, exId, (char*)keyData, keyLength, csn, tid, status);
    if (((RecoveryManager*)GetRecoveryManager())->m_logStats != nullptr)
        ((RecoveryManager*)GetRecoveryManager())->m_logStats->IncDelete(tableId);
    return sizeof(OperationCode) + sizeof(uint32_t) + sizeof(tableId) + sizeof(exId) + sizeof(keyLength) + keyLength +
           sizeof(version);
}

uint32_t RecoveryOps::RecoverLogOperationCommit(TxnManager* txn, uint8_t* data, uint64_t csn, uint32_t tid, RC& status)
{
    // OperationCode + CSN + transaction_type + commit_counter + transaction_id
    if (((RecoveryManager*)GetRecoveryManager())->m_logStats != nullptr)
        ((RecoveryManager*)GetRecoveryManager())->m_logStats->IncCommit();
    status = CommitTransaction(txn, csn);
    if (status != RC_OK) {
        MOT_LOG_ERROR("Failed to commit row recovery from log: %s (error code: %d)", RcToString(status), (int)status);
    }
    return sizeof(EndSegmentBlock);
}

uint32_t RecoveryOps::RecoverLogOperationRollback(
    TxnManager* txn, uint8_t* data, uint64_t csn, uint32_t tid, RC& status)
{
    // OperationCode + CSN + transaction_type + commit_counter + transaction_id
    RecoveryOps::RollbackTransaction(txn);
    return sizeof(EndSegmentBlock);
}

void RecoveryOps::InsertRow(TxnManager* txn, uint64_t tableId, uint64_t exId, char* keyData, uint16_t keyLen,
    char* rowData, uint64_t rowLen, uint64_t csn, uint32_t tid, SurrogateState& sState, RC& status, uint64_t rowId,
    bool insertLocked)
{
    Table* table = txn->GetTableByExternalId(exId);
    if (table == nullptr) {
        status = RC_ERROR;
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL, "Recover Insert Row", "Table %" PRIu64 " does not exist", exId);
        return;
    }

    Row* row = table->CreateNewRow();
    if (row == nullptr) {
        status = RC_ERROR;
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Recover Insert Row", "Failed to create row");
        return;
    }
    row->CopyData((const uint8_t*)rowData, rowLen);
    row->SetCommitSequenceNumber(csn);
    row->SetRowId(rowId);

    if (insertLocked == true) {
        row->SetTwoPhaseMode(true);
        row->m_rowHeader.Lock();
    }

    Key* key = nullptr;
    MOT::Index* ix = nullptr;
    MOT::Key* cleanupKeys[table->GetNumIndexes()] = {nullptr};
    ix = table->GetPrimaryIndex();
    key = txn->GetTxnKey(ix);
    if (key == nullptr) {
        table->DestroyRow(row);
        status = RC_ERROR;
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Recover Insert Row", "Failed to create primary key");
        return;
    }
    cleanupKeys[0] = key;
    if (ix->IsFakePrimary()) {
        row->SetSurrogateKey(*(uint64_t*)keyData);
        sState.UpdateMaxKey(rowId);
    }
    key->CpKey((const uint8_t*)keyData, keyLen);
    txn->GetNextInsertItem()->SetItem(row, ix, key);
    for (uint16_t i = 1; i < table->GetNumIndexes(); i++) {
        ix = table->GetSecondaryIndex(i);
        key = txn->GetTxnKey(ix);
        if (key == nullptr) {
            status = RC_MEMORY_ALLOCATION_ERROR;
            MOT_REPORT_ERROR(MOT_ERROR_OOM,
                "Recover Insert Row",
                "Failed to create key for secondary index %s",
                ix->GetName().c_str());
            for (uint16_t j = 0; j < table->GetNumIndexes(); j++) {
                if (cleanupKeys[j] != nullptr) {
                    txn->DestroyTxnKey(cleanupKeys[j]);
                }
            }
            table->DestroyRow(row);
            txn->Rollback();
            return;
        }
        cleanupKeys[i] = key;
        ix->BuildKey(table, row, key);
        txn->GetNextInsertItem()->SetItem(row, ix, key);
    }
    status = txn->InsertRow(row);

    if (insertLocked == true) {
        row->GetPrimarySentinel()->Lock(0);
    }

    if (status != RC_OK) {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL, "Recovery Manager Insert Row", "failed to insert row (status %u)", status);
    }
}

void RecoveryOps::DeleteRow(TxnManager* txn, uint64_t tableId, uint64_t exId, char* keyData, uint16_t keyLen,
    uint64_t csn, uint32_t tid, RC& status)
{
    Row* row = nullptr;
    Key* key = nullptr;
    Index* index = nullptr;

    Table* table = txn->GetTableByExternalId(exId);
    if (table == nullptr) {
        MOT_REPORT_ERROR(
            MOT_ERROR_INVALID_ARG, "Recovery Manager Delete Row", "table %" PRIu64 " does not exist", exId);
        status = RC_ERROR;
        return;
    }

    index = table->GetPrimaryIndex();
    key = txn->GetTxnKey(index);
    if (key == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Recovery Manager Delete Row", "failed to create key");
        status = RC_ERROR;
        return;
    }
    key->CpKey((const uint8_t*)keyData, keyLen);
    row = txn->RowLookupByKey(table, WR, key);
    if (row != nullptr) {
        status = txn->DeleteLastRow();
        if (status != RC_OK) {
            if (MOT_IS_OOM()) {
                MOT_REPORT_ERROR(
                    MOT_ERROR_OOM, "Recovery Manager Delete Row", "failed to remove row due to lack of memory");
                status = RC_MEMORY_ALLOCATION_ERROR;
            } else {
                MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                    "Recovery Manager Delete Row",
                    "failed to remove row: %s (error code: %d)",
                    RcToString(status),
                    status);
            }
        }
    } else {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL, "Recovery Manager Delete Row", "getData failed");
        status = RC_ERROR;
    }
    txn->DestroyTxnKey(key);
    status = RC_OK;
}

void RecoveryOps::UpdateRow(TxnManager* txn, uint64_t tableId, uint64_t exId, char* keyData, uint16_t keyLen,
    char* rowData, uint64_t rowLen, uint64_t csn, uint32_t tid, SurrogateState& sState, RC& status)
{
    Table* table = txn->GetTableByExternalId(exId);
    if (table == nullptr) {
        status = RC_ERROR;
        MOT_REPORT_ERROR(
            MOT_ERROR_INVALID_ARG, "Recovery Manager Update Row", "table %" PRIu64 " does not exist", exId);
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
    Key* key = txn->GetTxnKey(index);
    if (key == nullptr) {
        status = RC_ERROR;
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Recovery Manager Update Row", "failed to create key");
        return;
    }

    key->CpKey((const uint8_t*)keyData, keyLen);
    Row* row = txn->RowLookupByKey(table, RD_FOR_UPDATE, key);
    if (row == nullptr) {
        // Row not found. Error!!! Got an update for non existing row.
        txn->DestroyTxnKey(key);
        status = RC_ERROR;
        MOT_REPORT_ERROR(
            MOT_ERROR_INTERNAL, "updateRow", "row not found, key: %s, tableId: %lu", key->GetKeyStr().c_str(), tableId);
        return;
    } else {
        // CSNs can be equal if updated during the same transaction
        if (row->GetCommitSequenceNumber() <= csn) {
            row->CopyData((const uint8_t*)rowData, rowLen);
            row->SetCommitSequenceNumber(csn);
            if (row->IsAbsentRow()) {
                row->UnsetAbsentRow();
            }
            txn->UpdateLastRowState(MOT::AccessType::WR);
        } else {
            // Row CSN is newer. Error!!!
            txn->DestroyTxnKey(key);
            status = RC_ERROR;
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                "updateRow",
                "row CSN is newer! %lu > %lu, key: %s, tableId: %lu",
                row->GetCommitSequenceNumber(),
                csn,
                key->GetKeyStr().c_str(),
                tableId);
            return;
        }
    }
    txn->DestroyTxnKey(key);
}

void RecoveryOps::CreateTable(TxnManager* txn, char* data, RC& status, Table*& table, CreateTableMethod method)
{
    /* first verify that the table does not exists */
    string name;
    string longName;
    uint32_t intId = 0;
    uint64_t extId = 0;
    uint32_t metaVersion = MetadataProtoVersion::METADATA_VER_CURR;
    Table::DeserializeNameAndIds((const char*)data, metaVersion, intId, extId, name, longName);
    MOT_LOG_DEBUG("CreateTable: got intId: %u, extId: %lu, %s/%s", intId, extId, name.c_str(), longName.c_str());
    if (GetTableManager()->VerifyTableExists(intId, extId, name, longName)) {
        MOT_LOG_DEBUG("CreateTable: table %u already exists", intId);
        return;
    }

    if (metaVersion > MetadataProtoVersion::METADATA_VER_CURR) {
        MOT_LOG_ERROR("CreateTable: metadata version %u is greater than current %u",
            metaVersion,
            MetadataProtoVersion::METADATA_VER_CURR);
        status = RC_ERROR;
        return;
    }
    table = new (std::nothrow) Table();
    if (table == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Recovery Manager Create Table", "failed to allocate table object");
        status = RC_ERROR;
        return;
    }

    table->Deserialize((const char*)data);
    do {
        if (!table->IsDeserialized()) {
            MOT_LOG_ERROR("CreateTable: failed to de-serialize table");
            status = RC_ERROR;
            break;
        }

        switch (method) {
            case TRANSACTIONAL:
                status = txn->CreateTable(table);
                break;

            case ADD_TO_ENGINE:
                status = GetTableManager()->AddTable(table) ? RC_OK : RC_ERROR;
                break;

            case DONT_ADD_TO_ENGINE:
            default:
                status = RC_OK;
                break;
        }

        if (status != RC_OK) {
            MOT_LOG_ERROR("CreateTable: failed to add table %s (id: %u) to engine (method %u)",
                table->GetLongTableName().c_str(),
                table->GetTableId(),
                method);
            break;
        }

        MOT_LOG_DEBUG("CreateTable: table %s (id %u) created (method %u)",
            table->GetLongTableName().c_str(),
            table->GetTableId(),
            method);
        return;
    } while (0);

    MOT_LOG_ERROR("CreateTable: failed to recover table");
    delete table;

    if (status == RC_OK) {
        status = RC_ERROR;
    }
    return;
}

void RecoveryOps::DropTable(TxnManager* txn, char* data, RC& status)
{
    char* in = (char*)data;
    uint64_t externalTableId;
    uint32_t metaVersion;
    string tableName;

    in = SerializablePOD<uint32_t>::Deserialize(in, metaVersion);
    if (metaVersion > MetadataProtoVersion::METADATA_VER_CURR) {
        MOT_LOG_ERROR("DropTable: metadata version %u is greater than current %u",
            metaVersion,
            MetadataProtoVersion::METADATA_VER_CURR);
        status = RC_ERROR;
        return;
    }
    in = SerializablePOD<uint64_t>::Deserialize(in, externalTableId);
    Table* table = txn->GetTableByExternalId(externalTableId);
    if (table == nullptr) {
        MOT_LOG_DEBUG("DropTable: could not find table %" PRIu64, externalTableId);
        /* this might happen if we try to replay an outdated xlog entry - currently we do not error out */
        return;
    }
    tableName.assign(table->GetLongTableName());
    status = txn->DropTable(table);
    if (status != RC_OK) {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
            "Recovery Manager Drop Table",
            "Failed to drop table %s [%" PRIu64 "])",
            tableName.c_str(),
            externalTableId);
    }
    MOT_LOG_DEBUG("DropTable: table %s [%" PRIu64 "] dropped", tableName.c_str(), externalTableId);
}

void RecoveryOps::CreateIndex(TxnManager* txn, char* data, uint32_t tid, RC& status)
{
    char* in = (char*)data;
    uint64_t externalTableId;
    uint32_t metaVersion;
    Table::CommonIndexMeta idx;

    in = SerializablePOD<uint32_t>::Deserialize(in, metaVersion);
    if (metaVersion > MetadataProtoVersion::METADATA_VER_CURR) {
        MOT_LOG_ERROR("Recover Create Index: metadata version %u is greater than current %u",
            metaVersion,
            MetadataProtoVersion::METADATA_VER_CURR);
        status = RC_ERROR;
        return;
    }
    in = SerializablePOD<uint64_t>::Deserialize(in, externalTableId);
    Table* table = txn->GetTableByExternalId(externalTableId);
    if (table == nullptr) {
        MOT_REPORT_ERROR(
            MOT_ERROR_INVALID_ARG, "Recover Create Index", "Could not find table %" PRIu64, externalTableId);
        status = RC_ERROR;
        return;
    }
    in = table->DeserializeMeta(in, idx, metaVersion);
    bool primary = idx.m_indexOrder == IndexOrder::INDEX_ORDER_PRIMARY;
    MOT_LOG_DEBUG("createIndex: creating %s Index, %s %lu",
        primary ? "Primary" : "Secondary",
        idx.m_name.c_str(),
        idx.m_indexExtId);
    Index* index = nullptr;
    status = table->CreateIndexFromMeta(idx, primary, tid, false, &index);
    if (status != RC_OK) {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
            "Recover Create Index",
            "Failed to create index for table %" PRIu64 " from meta-data: %s (error code: %d)",
            externalTableId,
            RcToString(status),
            status);
    } else {
        status = txn->CreateIndex(table, index, primary);
        if (status != RC_OK) {
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                "Recover Create Index",
                "Failed to create index for table %" PRIu64 ": %s (error code: %d)",
                externalTableId,
                RcToString(status),
                status);
        }
    }
}

void RecoveryOps::DropIndex(TxnManager* txn, char* data, RC& status)
{
    RC res;
    char* in = (char*)data;
    uint64_t externalTableId;
    uint32_t metaVersion;
    string indexName;

    in = SerializablePOD<uint32_t>::Deserialize(in, metaVersion);
    if (metaVersion > MetadataProtoVersion::METADATA_VER_CURR) {
        MOT_LOG_ERROR("Recover Drop Index: metadata version %u is greater than current %u",
            metaVersion,
            MetadataProtoVersion::METADATA_VER_CURR);
        status = RC_ERROR;
        return;
    }
    in = SerializablePOD<uint64_t>::Deserialize(in, externalTableId);
    Table* table = txn->GetTableByExternalId(externalTableId);
    if (table == nullptr) {
        /* this might happen if we try to replay an outdated xlog entry - currently we do not error out */
        MOT_LOG_DEBUG("dropIndex: could not find table %" PRIu64, externalTableId);
        return;
    }

    in = SerializableSTR::Deserialize(in, indexName);
    Index* index = table->GetSecondaryIndex(indexName);
    if (index == nullptr) {
        res = RC_INDEX_NOT_FOUND;
    } else {
        table->WrLock();
        res = txn->DropIndex(index);
        table->Unlock();
    }
    if (res != RC_OK) {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
            "Recovery Manager Drop Index",
            "Drop index %s error, secondary index not found.",
            indexName.c_str());
        status = RC_ERROR;
    }
}

void RecoveryOps::TruncateTable(TxnManager* txn, char* data, RC& status)
{
    char* in = (char*)data;
    uint64_t externalTableId;
    uint32_t metaVersion;

    in = SerializablePOD<uint32_t>::Deserialize(in, metaVersion);
    if (metaVersion > MetadataProtoVersion::METADATA_VER_CURR) {
        MOT_LOG_ERROR("Recover Truncate: metadata version %u is greater than current %u",
            metaVersion,
            MetadataProtoVersion::METADATA_VER_CURR);
        status = RC_ERROR;
        return;
    }
    in = SerializablePOD<uint64_t>::Deserialize(in, externalTableId);
    Table* table = txn->GetTableByExternalId(externalTableId);
    if (table == nullptr) {
        /* this might happen if we try to replay an outdated xlog entry - currently we do not error out */
        MOT_LOG_DEBUG("truncateTable: could not find table %" PRIu64, externalTableId);
        return;
    }

    table->WrLock();
    status = txn->TruncateTable(table);
    table->Unlock();
}

RC RecoveryOps::BeginTransaction(TxnManager* txn, uint64_t replayLsn)
{
    if (txn == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL, "%s: invalid TxnManager object", __FUNCTION__);
        return RC_ERROR;
    }
    txn->StartTransaction(INVALID_TRANSACTION_ID, READ_COMMITED);
    txn->SetReplayLsn(replayLsn);
    return RC_OK;
}

RC RecoveryOps::CommitTransaction(TxnManager* txn, uint64_t csn)
{
    txn->SetCommitSequenceNumber(csn);
    RC status = txn->Commit();
    if (status != RC_OK) {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
            "Recover DB",
            "Failed to commit recovery transaction: %s (error code: %u)",
            RcToString(status),
            status);
        txn->Rollback();
    } else {
        txn->EndTransaction();
    }

    return status;
}

void RecoveryOps::RollbackTransaction(TxnManager* txn)
{
    txn->Rollback();
}
}  // namespace MOT
