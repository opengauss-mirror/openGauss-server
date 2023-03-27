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
#include "recovery_ops.h"
#include "mot_engine.h"
#include "serializable.h"
#include "checkpoint_utils.h"
#include "bitmapset.h"
#include "column.h"
#include "txn_insert_action.h"
#include <string>
#include <algorithm>

namespace MOT {
DECLARE_LOGGER(RecoveryOps, Recovery);

uint32_t RecoveryOps::RecoverLogOperation(
    IRecoveryOpsContext* ctx, uint8_t* data, uint64_t transactionId, uint32_t tid, SurrogateState& sState, RC& status)
{
    if (ctx == nullptr || ctx->GetTxn() == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL, "%s: invalid TxnManager object", __FUNCTION__);
        status = RC_ERROR;
        return 0;
    }

    status = RC_OK;
    OperationCode opCode = *static_cast<OperationCode*>((void*)data);
    MOT_LOG_DEBUG("cur opcode %s", OperationCodeToString(opCode));
    switch (opCode) {
        case CREATE_ROW:
            return RecoverLogOperationInsert(ctx, data, tid, sState, status);
        case UPDATE_ROW:
            return RecoverLogOperationUpdate(ctx, data, tid, status);
        case REMOVE_ROW:
            return RecoverLogOperationDelete(ctx, data, tid, status);
        case CREATE_TABLE:
            return RecoverLogOperationCreateTable(ctx, data, status, transactionId);
        case CREATE_INDEX:
            return RecoverLogOperationCreateIndex(ctx, data, tid, status);
        case DROP_TABLE:
            return RecoverLogOperationDropTable(ctx, data, status);
        case DROP_INDEX:
            return RecoverLogOperationDropIndex(ctx, data, status);
        case TRUNCATE_TABLE:
            return RecoverLogOperationTruncateTable(ctx, data, status);
        case ALTER_TABLE_ADD_COLUMN:
            return RecoverLogOperationAlterTableAddColumn(ctx, data, status);
        case ALTER_TABLE_DROP_COLUMN:
            return RecoverLogOperationAlterTableDropColumn(ctx, data, status);
        case ALTER_TABLE_RENAME_COLUMN:
            return RecoverLogOperationAlterTableRenameColumn(ctx, data, status);
        case COMMIT_TX_1VCC:
        case COMMIT_TX:
        case COMMIT_DDL_TX:
        case COMMIT_CROSS_TX:
        case COMMIT_PREPARED_TX:
            return RecoverLogOperationCommit(ctx, data, tid, status);
        case PARTIAL_REDO_TX_1VCC:
        case PARTIAL_REDO_TX:
        case PARTIAL_REDO_DDL_TX:
        case PARTIAL_REDO_CROSS_TX:
        case PREPARE_TX:
            return sizeof(EndSegmentBlock);
        case ROLLBACK_TX:
        case ROLLBACK_PREPARED_TX:
            return RecoverLogOperationRollback(ctx, data, tid, status);
        default:
            MOT_LOG_ERROR("Unknown recovery redo record op-code: %u", (unsigned)opCode);
            status = RC_ERROR;
            return 0;
    }
}

uint32_t RecoveryOps::RecoverLogOperationCreateTable(
    IRecoveryOpsContext* ctx, uint8_t* data, RC& status, uint64_t transactionId)
{
    if (GetGlobalConfiguration().m_enableIncrementalCheckpoint) {
        status = RC_ERROR;
        MOT_LOG_ERROR("RecoverLogOperationCreateTable: failed to create table. "
                      "MOT does not support incremental checkpoint");
        return 0;
    }

    OperationCode opCode = *(OperationCode*)data;
    MOT_ASSERT(opCode == CREATE_TABLE);
    data += sizeof(OperationCode);
    size_t bufSize = 0;
    Extract(data, bufSize);
    CreateTable(ctx, (char*)data, status, TRANSACTIONAL);
    return sizeof(OperationCode) + sizeof(bufSize) + bufSize;
}

uint32_t RecoveryOps::RecoverLogOperationDropTable(IRecoveryOpsContext* ctx, uint8_t* data, RC& status)
{
    OperationCode opCode = *(OperationCode*)data;
    MOT_ASSERT(opCode == DROP_TABLE);
    data += sizeof(OperationCode);
    DropTable(ctx, (char*)data, status);
    return sizeof(OperationCode) + sizeof(uint64_t) + sizeof(uint32_t);
}

uint32_t RecoveryOps::RecoverLogOperationCreateIndex(IRecoveryOpsContext* ctx, uint8_t* data, uint32_t tid, RC& status)
{
    OperationCode opCode = *(OperationCode*)data;
    MOT_ASSERT(opCode == CREATE_INDEX);
    data += sizeof(OperationCode);
    size_t bufSize = 0;
    Extract(data, bufSize);
    CreateIndex(ctx, (char*)data, tid, status);
    return sizeof(OperationCode) + sizeof(bufSize) + sizeof(uint64_t) + sizeof(uint32_t) +
           bufSize;  // sizeof(uint64_t) is for tableId
}

uint32_t RecoveryOps::RecoverLogOperationDropIndex(IRecoveryOpsContext* ctx, uint8_t* data, RC& status)
{
    OperationCode opCode = *(OperationCode*)data;
    MOT_ASSERT(opCode == DROP_INDEX);
    data += sizeof(OperationCode);
    char* extracted = (char*)data;
    DropIndex(ctx, extracted, status);
    uint64_t tableId = 0;
    size_t nameLen = 0;
    uint32_t metaVersion;
    Extract(data, metaVersion);
    Extract(data, tableId);
    Extract(data, nameLen);
    return sizeof(OperationCode) + sizeof(uint32_t) + sizeof(uint64_t) + sizeof(size_t) + nameLen;
}

uint32_t RecoveryOps::RecoverLogOperationTruncateTable(IRecoveryOpsContext* ctx, uint8_t* data, RC& status)
{
    OperationCode opCode = *(OperationCode*)data;
    MOT_ASSERT(opCode == TRUNCATE_TABLE);
    data += sizeof(OperationCode);
    TruncateTable(ctx, (char*)data, status);
    return sizeof(OperationCode) + sizeof(uint64_t) + sizeof(uint32_t);
}

uint32_t RecoveryOps::RecoverLogOperationAlterTableAddColumn(IRecoveryOpsContext* ctx, uint8_t* data, RC& status)
{
    OperationCode opCode = *(OperationCode*)data;
    MOT_ASSERT(opCode == ALTER_TABLE_ADD_COLUMN);
    data += sizeof(OperationCode);
    size_t bufSize = 0;
    Extract(data, bufSize);
    AlterTableAddColumn(ctx, (char*)data, status);
    return sizeof(OperationCode) + sizeof(bufSize) + sizeof(uint64_t) + sizeof(uint32_t) + bufSize;
}

uint32_t RecoveryOps::RecoverLogOperationAlterTableDropColumn(IRecoveryOpsContext* ctx, uint8_t* data, RC& status)
{
    OperationCode opCode = *(OperationCode*)data;
    MOT_ASSERT(opCode == ALTER_TABLE_DROP_COLUMN);
    data += sizeof(OperationCode);
    size_t bufSize = 0;
    Extract(data, bufSize);
    AlterTableDropColumn(ctx, (char*)data, status);
    return sizeof(OperationCode) + sizeof(bufSize) + sizeof(uint64_t) + sizeof(uint32_t) + bufSize;
}

uint32_t RecoveryOps::RecoverLogOperationAlterTableRenameColumn(IRecoveryOpsContext* ctx, uint8_t* data, RC& status)
{
    OperationCode opCode = *(OperationCode*)data;
    MOT_ASSERT(opCode == ALTER_TABLE_RENAME_COLUMN);
    data += sizeof(OperationCode);
    size_t bufSize = 0;
    Extract(data, bufSize);
    AlterTableRenameColumn(ctx, (char*)data, status);
    return sizeof(OperationCode) + sizeof(bufSize) + sizeof(uint64_t) + sizeof(uint32_t) + bufSize;
}

uint32_t RecoveryOps::RecoverLogOperationInsert(
    IRecoveryOpsContext* ctx, uint8_t* data, uint32_t tid, SurrogateState& sState, RC& status)
{
    uint64_t tableId, rowLength, exId, rowId;
    uint32_t metaVersion;
    uint16_t keyLength;
    uint8_t* keyData = nullptr;
    uint8_t* rowData = nullptr;

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
    Extract(data, rowId);
    Extract(data, keyLength);
    keyData = ExtractPtr(data, keyLength);
    Extract(data, rowLength);
    rowData = ExtractPtr(data, rowLength);
    InsertRow(ctx, tableId, exId, (char*)keyData, keyLength, (char*)rowData, rowLength, tid, sState, status, rowId);
    GetRecoveryManager()->LogInsert(tableId);
    return sizeof(OperationCode) + sizeof(metaVersion) + sizeof(tableId) + sizeof(exId) + sizeof(rowId) +
           sizeof(keyLength) + keyLength + sizeof(rowLength) + rowLength;
}

RC RecoveryOps::LookupRow(
    IRecoveryOpsContext* ctx, Table* table, Key* key, const AccessType type, uint64_t prev_version, bool isMvccUpgrade)
{
    RC rc = RC_OK;
    TxnManager* txn = ctx->GetTxn();
    const char* opType = (type == WR) ? "Update" : "Delete";

    do {
        Row* row = txn->RowLookupByKey(table, RD, key, rc);
        if (rc == RC_MEMORY_ALLOCATION_ERROR) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM,
                "Recovery Manager Lookup Row",
                "%s: Initial row lookup failed due to lack of memory",
                opType);
            return RC_MEMORY_ALLOCATION_ERROR;
        }

        if (row == nullptr && !ctx->ShouldRetryOp()) {
            // Row not found. Error!!! Got an update for non existing row.
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                "Recovery Manager Lookup Row",
                "%s: Row not found, key: %s, tableId: (%lu:%u)",
                opType,
                key->GetKeyStr().c_str(),
                table->GetTableExId(),
                table->GetTableId());
            return RC_ERROR;
        }

        if (row == nullptr) {
            (void)usleep(1000);
            continue;
        }

        PrimarySentinel* ps = row->GetPrimarySentinel();
        row = ps->GetData();
        if (isMvccUpgrade || (row && ps->GetTransactionId() == prev_version)) {
            // The correct version is in store, can continue.
            row = txn->RowLookupByKey(table, type, key, rc);
            if (rc == RC_MEMORY_ALLOCATION_ERROR) {
                MOT_REPORT_ERROR(MOT_ERROR_OOM,
                    "Recovery Manager Lookup Row",
                    "%s: Row lookup after version check failed due to lack of memory",
                    opType);
                return RC_MEMORY_ALLOCATION_ERROR;
            }
            break;
        }

        // Wrong version in store, if retry is not supported (synchronous recovery)
        // an error occurred. Cannot recover operation
        if (!ctx->ShouldRetryOp()) {
            row = ps->GetData();
            if (ps->GetTransactionId() == prev_version) {
                row = txn->RowLookupByKey(table, type, key, rc);
                if (rc == RC_MEMORY_ALLOCATION_ERROR) {
                    MOT_REPORT_ERROR(MOT_ERROR_OOM,
                        "Recovery Manager Lookup Row",
                        "%s: Retried row lookup failed due to lack of memory");
                    return RC_MEMORY_ALLOCATION_ERROR;
                }
                break;
            } else {
                MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                    "Recovery Manager Lookup Row",
                    "%s: TID version incorrect, tableId: (%lu:%u), expected version %lu, found version %lu",
                    table->GetTableExId(),
                    table->GetTableId(),
                    prev_version,
                    ps->GetTransactionId());
                return RC_ERROR;
            }
        }

        // If we got here, we need to retry and wait till the right version will be committed to store.
        (void)usleep(1000);
    } while (true);

    return RC_OK;
}

uint32_t RecoveryOps::RecoverLogOperationUpdate(IRecoveryOpsContext* ctx, uint8_t* data, uint32_t tid, RC& status)
{
    uint64_t tableId, exId, prev_version;
    uint32_t metaVersion;
    uint16_t keyLength;
    uint8_t *keyData, *rowData;
    bool idxUpd = false;
    TxnManager* txn = ctx->GetTxn();
    OperationCode opCode = *(OperationCode*)data;
    MOT_ASSERT(opCode == UPDATE_ROW);
    uint32_t retSize = sizeof(OperationCode) + sizeof(metaVersion) + sizeof(tableId) + sizeof(exId) +
                       sizeof(keyLength) + sizeof(prev_version);
    data += sizeof(OperationCode);

    Extract(data, metaVersion);
    if (metaVersion > MetadataProtoVersion::METADATA_VER_CURR) {
        MOT_LOG_ERROR("UpdateRow: metadata version %u is greater than current %u",
            metaVersion,
            MetadataProtoVersion::METADATA_VER_CURR);
        status = RC_ERROR;
        return 0;
    }

    bool isMvccUpgrade = (metaVersion < MetadataProtoVersion::METADATA_VER_MVCC) ? true : false;

    Extract(data, tableId);
    Extract(data, exId);
    Extract(data, keyLength);
    keyData = ExtractPtr(data, keyLength);
    Extract(data, prev_version);
    if (metaVersion >= MetadataProtoVersion::METADATA_VER_IDX_COL_UPD) {
        Extract(data, idxUpd);
        retSize += sizeof(idxUpd);
    }

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
        status = RC_MEMORY_ALLOCATION_ERROR;
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Recovery Manager Update Row", "failed to allocate key");
        return 0;
    }

    key->CpKey((const uint8_t*)keyData, keyLength);

    status = LookupRow(ctx, table, key, MOT::AccessType::WR, prev_version, isMvccUpgrade);
    if (status != RC_OK) {
        txn->DestroyTxnKey(key);
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
            "Recovery Manager Update Row",
            "Failed to lookup row: %s (%d)",
            RcToString(status),
            status);
        return 0;
    }

    status = txn->UpdateLastRowState(MOT::AccessType::WR);
    if (status != RC_OK) {
        txn->DestroyTxnKey(key);
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
            "Recovery Manager Update Row",
            "Failed to update last row state: %s (%d)",
            RcToString(status),
            status);
        return 0;
    }

    Row* localRow = txn->GetLastAccessedDraft();

    uint16_t num_columns = table->GetFieldCount() - 1;
    rowData = const_cast<uint8_t*>(localRow->GetData());
    BitmapSet row_valid_columns(rowData + table->GetFieldOffset((uint64_t)0), num_columns);
    BitmapSet updated_columns(ExtractPtr(data, BitmapSet::GetLength(num_columns)), num_columns);
    BitmapSet valid_columns(ExtractPtr(data, BitmapSet::GetLength(num_columns)), num_columns);
    BitmapSet::BitmapSetIterator updated_columns_it(updated_columns);
    BitmapSet::BitmapSetIterator valid_columns_it(valid_columns);
    UpdateIndexColumnType idxUpdType = idxUpd ? UPDATE_COLUMN_SECONDARY : UPDATE_COLUMN_NONE;
    MOT::TxnIxColUpdate colUpd(table, txn, updated_columns.GetData(), idxUpdType);
    uint64_t size = 0;
    errno_t erc;
    // generate old keys in case of indexed column update
    if (unlikely(idxUpd)) {
        status = colUpd.InitAndBuildOldKeys(localRow);
        if (status != RC_OK) {
            txn->DestroyTxnKey(key);
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                "Recovery Manager Update Row - Index Column Update",
                "Failed to init old keys with error: %s (%d)",
                RcToString(status),
                status);
            return 0;
        }
    }
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
        (void)valid_columns_it.Next();
        (void)updated_columns_it.Next();
    }

    if (unlikely(idxUpd)) {
        status = colUpd.FilterColumnUpdate(localRow);
        if (status != RC_OK) {
            txn->DestroyTxnKey(key);
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                "Recovery Manager Update Row - Index Column Update",
                "FilterColumnUpdate failed: %s (%d)",
                RcToString(status),
                status);
            return 0;
        }

        status = txn->UpdateRow(updated_columns, &colUpd);
        if (status != RC_OK) {
            txn->DestroyTxnKey(key);
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                "Recovery Manager Update Row - Index Column Update",
                "Failed to update last row state: %s (%d)",
                RcToString(status),
                status);
            return 0;
        }
    }
    txn->DestroyTxnKey(key);
    GetRecoveryManager()->LogUpdate(tableId);

    retSize += keyLength + updated_columns.GetLength() + valid_columns.GetLength() + size;
    return retSize;
}

uint32_t RecoveryOps::RecoverLogOperationDelete(IRecoveryOpsContext* ctx, uint8_t* data, uint32_t tid, RC& status)
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

    bool isMvccUpgrade = (metaVersion < MetadataProtoVersion::METADATA_VER_MVCC) ? true : false;

    Extract(data, tableId);
    Extract(data, exId);
    Extract(data, keyLength);
    keyData = ExtractPtr(data, keyLength);
    Extract(data, version);
    DeleteRow(ctx, tableId, exId, (char*)keyData, keyLength, version, tid, isMvccUpgrade, status);
    GetRecoveryManager()->LogDelete(tableId);
    return sizeof(OperationCode) + sizeof(uint32_t) + sizeof(tableId) + sizeof(exId) + sizeof(keyLength) + keyLength +
           sizeof(version);
}

uint32_t RecoveryOps::RecoverLogOperationCommit(IRecoveryOpsContext* ctx, uint8_t* data, uint32_t tid, RC& status)
{
    GetRecoveryManager()->LogCommit();
    return sizeof(EndSegmentBlock);
}

uint32_t RecoveryOps::RecoverLogOperationRollback(IRecoveryOpsContext* ctx, uint8_t* data, uint32_t tid, RC& status)
{
    return sizeof(EndSegmentBlock);
}

void RecoveryOps::InsertRow(IRecoveryOpsContext* ctx, uint64_t tableId, uint64_t exId, char* keyData, uint16_t keyLen,
    char* rowData, uint64_t rowLen, uint32_t tid, SurrogateState& sState, RC& status, uint64_t rowId)
{
    TxnManager* txn = ctx->GetTxn();
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
    row->SetRowId(rowId);
    if (!sState.UpdateMaxKey(rowId)) {
        status = RC_ERROR;
        MOT_REPORT_ERROR(MOT_ERROR_INVALID_ARG, "Recovery Manager Insert Row", "Failed to update surrogate state");
        table->DestroyRow(row);
        return;
    }

    MOT::Index* ix = nullptr;
    ix = table->GetPrimaryIndex();
    if (ix->IsFakePrimary()) {
        row->SetSurrogateKey();
    }
    InsItem* insItem = txn->GetNextInsertItem();
    if (insItem == nullptr) {
        status = RC_MEMORY_ALLOCATION_ERROR;
        MOT_REPORT_ERROR(MOT_ERROR_RESOURCE_LIMIT, "Recovery Manager Insert Row", "Failed to get insert item");
        table->DestroyRow(row);
        return;
    }
    insItem->SetItem(row, ix);
    for (uint16_t i = 1; i < table->GetNumIndexes(); i++) {
        ix = table->GetSecondaryIndex(i);
        insItem = txn->GetNextInsertItem();
        if (insItem == nullptr) {
            status = RC_MEMORY_ALLOCATION_ERROR;
            MOT_REPORT_ERROR(MOT_ERROR_RESOURCE_LIMIT, "Recovery Manager Insert Row", "Failed to get insert item");
            table->DestroyRow(row);
            return;
        }
        insItem->SetItem(row, ix);
    }

    do {
        status = txn->InsertRecoveredRow(row);
        if (status == RC_OK) {
            break;
        } else {
            if (ctx->ShouldRetryOp()) {
                (void)usleep(1000);
            } else {
                break;
            }
        }
    } while (true);

    if (status != RC_OK) {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL, "Recovery Manager Insert Row", "failed to insert row (status %u)", status);
    }
}

void RecoveryOps::DeleteRow(IRecoveryOpsContext* ctx, uint64_t tableId, uint64_t exId, char* keyData, uint16_t keyLen,
    uint64_t prev_version, uint32_t tid, bool isMvccUpgrade, RC& status)
{
    TxnManager* txn = ctx->GetTxn();
    Table* table = txn->GetTableByExternalId(exId);
    if (table == nullptr) {
        MOT_REPORT_ERROR(
            MOT_ERROR_INVALID_ARG, "Recovery Manager Delete Row", "table %" PRIu64 " does not exist", exId);
        status = RC_ERROR;
        return;
    }

    Index* index = table->GetPrimaryIndex();
    Key* key = txn->GetTxnKey(index);
    if (key == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Recovery Manager Delete Row", "failed to create key");
        status = RC_ERROR;
        return;
    }

    key->CpKey((const uint8_t*)keyData, keyLen);

    status = LookupRow(ctx, table, key, MOT::AccessType::DEL, prev_version, isMvccUpgrade);
    if (status != RC_OK) {
        txn->DestroyTxnKey(key);
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
            "Recovery Manager Delete Row",
            "Failed to lookup row: %s (%d)",
            RcToString(status),
            status);
        return;
    }

    status = txn->DeleteLastRow();
    if (status != RC_OK) {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
            "Recovery Manager Delete Row",
            "Failed to remove row: %s (%d)",
            RcToString(status),
            status);
    }
    txn->DestroyTxnKey(key);
}

void RecoveryOps::CreateTable(IRecoveryOpsContext* ctx, char* data, RC& status, CreateTableMethod method)
{
    /* first verify that the table does not exists */
    string name;
    string longName;
    uint32_t intId = 0;
    uint64_t extId = 0;
    TxnManager* txn = ctx->GetTxn();
    uint32_t metaVersion = MetadataProtoVersion::METADATA_VER_CURR;
    Table::DeserializeNameAndIds((const char*)data, metaVersion, intId, extId, name, longName);

    MOT_LOG_TRACE("Recover Create Table: Creating %s/%s (%lu:%u)", name.c_str(), longName.c_str(), extId, intId);

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

    Table* table = new (std::nothrow) Table();
    if (table == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Recovery Manager Create Table", "failed to allocate table object");
        status = RC_MEMORY_ALLOCATION_ERROR;
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

void RecoveryOps::DropTable(IRecoveryOpsContext* ctx, char* data, RC& status)
{
    char* in = data;
    uint64_t externalTableId;
    uint32_t metaVersion;
    string tableName;
    TxnManager* txn = ctx->GetTxn();

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
    (void)tableName.assign(table->GetLongTableName());

    MOT_LOG_TRACE("Recover Drop Table: Dropping %s (%" PRIu64 ")", tableName.c_str(), externalTableId);

    status = txn->DropTable(table);
    if (status != RC_OK) {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
            "Recovery Manager Drop Table",
            "Failed to drop table %s [%" PRIu64 "])",
            tableName.c_str(),
            externalTableId);
    }
}

void RecoveryOps::CreateIndex(IRecoveryOpsContext* ctx, char* data, uint32_t tid, RC& status)
{
    char* in = data;
    uint64_t externalTableId;
    uint32_t metaVersion;
    Table::CommonIndexMeta idx;
    TxnManager* txn = ctx->GetTxn();

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

    MOT_LOG_TRACE("Recover Create Index: Creating %s Index, %s (%lu) of table %s (%lu:%u)",
        primary ? "Primary" : "Secondary",
        idx.m_name.c_str(),
        idx.m_indexExtId,
        table->GetTableName().c_str(),
        table->GetTableExId(),
        table->GetTableId());

    Index* index = nullptr;
    status = table->CreateIndexFromMeta(idx, primary, tid, metaVersion, false, &index);
    if (status != RC_OK) {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
            "Recover Create Index",
            "Failed to create index for table %" PRIu64 " from meta-data: %s (error code: %d)",
            externalTableId,
            RcToString(status),
            status);
    } else {
        table->GetOrigTable()->WrLock();
        status = txn->CreateIndex(table, index, primary);
        table->GetOrigTable()->Unlock();
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

void RecoveryOps::DropIndex(IRecoveryOpsContext* ctx, char* data, RC& status)
{
    RC res = RC_OK;
    char* in = data;
    uint64_t externalTableId;
    uint32_t metaVersion;
    string indexName;
    TxnManager* txn = ctx->GetTxn();

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
        MOT_LOG_DEBUG("Recover Drop Index: could not find table %" PRIu64, externalTableId);
        return;
    }

    in = SerializableSTR::Deserialize(in, indexName);

    MOT_LOG_TRACE("Recover Drop Index: Dropping %s of table %s (%lu:%u)",
        indexName.c_str(),
        table->GetTableName().c_str(),
        table->GetTableExId(),
        table->GetTableId());

    Index* index = table->GetSecondaryIndex(indexName);
    if (index == nullptr) {
        res = RC_INDEX_NOT_FOUND;
    } else {
        table->GetOrigTable()->WrLock();
        res = txn->DropIndex(index);
        table->GetOrigTable()->Unlock();
    }
    if (res != RC_OK) {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
            "Recovery Manager Drop Index",
            "Drop index %s error, secondary index not found.",
            indexName.c_str());
        status = RC_ERROR;
    }
}

void RecoveryOps::TruncateTable(IRecoveryOpsContext* ctx, char* data, RC& status)
{
    char* in = data;
    uint64_t externalTableId;
    uint32_t metaVersion;
    TxnManager* txn = ctx->GetTxn();

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
        MOT_LOG_DEBUG("Recover Truncate: could not find table %" PRIu64, externalTableId);
        return;
    }

    table->GetOrigTable()->WrLock();
    status = txn->TruncateTable(table);
    table->GetOrigTable()->Unlock();
}

void RecoveryOps::AlterTableAddColumn(IRecoveryOpsContext* ctx, char* data, RC& status)
{
    char* in = data;
    uint64_t externalTableId;
    uint32_t metaVersion;
    Table::CommonColumnMeta col;
    Column* newColumn = nullptr;
    size_t defSize = 0;
    uintptr_t defValue = 0;
    bool freeDefValue = false;
    TxnManager* txn = ctx->GetTxn();

    in = SerializablePOD<uint32_t>::Deserialize(in, metaVersion);
    if (metaVersion > MetadataProtoVersion::METADATA_VER_CURR) {
        MOT_LOG_ERROR("Recover Add Column: metadata version %u is greater than current %u",
            metaVersion,
            MetadataProtoVersion::METADATA_VER_CURR);
        status = RC_ERROR;
        return;
    }
    in = SerializablePOD<uint64_t>::Deserialize(in, externalTableId);
    Table* table = txn->GetTableByExternalId(externalTableId);
    if (table == nullptr) {
        /* this might happen if we try to replay an outdated xlog entry - currently we do not error out */
        MOT_LOG_DEBUG("Recover Add Column: could not find table %" PRIu64, externalTableId);
        return;
    }
    in = table->DeserializeMeta(in, col, metaVersion);
    if (col.m_hasDefault) {
        switch (col.m_type) {
            case MOT_CATALOG_FIELD_TYPES::MOT_TYPE_BLOB:
            case MOT_CATALOG_FIELD_TYPES::MOT_TYPE_VARCHAR:
            case MOT_CATALOG_FIELD_TYPES::MOT_TYPE_DECIMAL:
            case MOT_CATALOG_FIELD_TYPES::MOT_TYPE_TIMETZ:
            case MOT_CATALOG_FIELD_TYPES::MOT_TYPE_TINTERVAL:
            case MOT_CATALOG_FIELD_TYPES::MOT_TYPE_INTERVAL:
                SerializableCharBuf::DeserializeSize(in, defSize);
                if (defSize > 0) {
                    defValue = (uintptr_t)malloc(defSize);
                    if (defValue == 0) {
                        MOT_LOG_ERROR(
                            "Recover Add Column: failed to allocate memory for default value of column %s", col.m_name);
                        status = MOT::RC_MEMORY_ALLOCATION_ERROR;
                        return;
                    }
                    in = SerializableCharBuf::Deserialize(in, (char*)defValue, defSize);
                    freeDefValue = true;
                }
                break;
            default:
                in = SerializablePOD<uint64_t>::Deserialize(in, defValue);
                defSize = col.m_size;
                break;
        }
    }
    table->GetOrigTable()->WrLock();
    status = table->CreateColumn(newColumn,
        col.m_name,
        col.m_size,
        col.m_type,
        col.m_isNotNull,
        col.m_envelopeType,
        col.m_hasDefault,
        defValue,
        defSize);
    if (status == RC_OK && newColumn != nullptr) {
        status = txn->AlterTableAddColumn(table, newColumn);
    }
    table->GetOrigTable()->Unlock();

    if (freeDefValue && defValue != 0) {
        free((void*)defValue);
    }
    if (status != RC_OK) {
        if (newColumn != nullptr) {
            delete newColumn;
        }
    }
}

void RecoveryOps::AlterTableDropColumn(IRecoveryOpsContext* ctx, char* data, RC& status)
{
    char* in = data;
    uint64_t externalTableId;
    uint32_t metaVersion;
    char colName[Column::MAX_COLUMN_NAME_LEN];
    TxnManager* txn = ctx->GetTxn();

    in = SerializablePOD<uint32_t>::Deserialize(in, metaVersion);
    if (metaVersion > MetadataProtoVersion::METADATA_VER_CURR) {
        MOT_LOG_ERROR("Recover Drop Column: metadata version %u is greater than current %u",
            metaVersion,
            MetadataProtoVersion::METADATA_VER_CURR);
        status = RC_ERROR;
        return;
    }
    in = SerializablePOD<uint64_t>::Deserialize(in, externalTableId);
    Table* table = txn->GetTableByExternalId(externalTableId);
    if (table == nullptr) {
        /* this might happen if we try to replay an outdated xlog entry - currently we do not error out */
        MOT_LOG_DEBUG("Recover Drop Column: could not find table %" PRIu64, externalTableId);
        return;
    }
    in = SerializableARR<char, Column::MAX_COLUMN_NAME_LEN>::Deserialize(in, colName);
    uint64_t colId = table->GetFieldId(colName);
    if (colId == (uint64_t)-1) {
        MOT_LOG_ERROR("Recover Drop Column: Column %s not found in table %s", colName, table->GetTableName().c_str());
        status = RC_ERROR;
        return;
    }
    Column* col = table->GetField(colId);
    table->GetOrigTable()->WrLock();
    status = txn->AlterTableDropColumn(table, col);
    table->GetOrigTable()->Unlock();
}

void RecoveryOps::AlterTableRenameColumn(IRecoveryOpsContext* ctx, char* data, RC& status)
{
    char* in = data;
    uint64_t externalTableId;
    uint32_t metaVersion;
    char oldName[Column::MAX_COLUMN_NAME_LEN];
    char newName[Column::MAX_COLUMN_NAME_LEN];
    TxnManager* txn = ctx->GetTxn();

    in = SerializablePOD<uint32_t>::Deserialize(in, metaVersion);
    if (metaVersion > MetadataProtoVersion::METADATA_VER_CURR) {
        MOT_LOG_ERROR("Recover Rename Column: metadata version %u is greater than current %u",
            metaVersion,
            MetadataProtoVersion::METADATA_VER_CURR);
        status = RC_ERROR;
        return;
    }
    in = SerializablePOD<uint64_t>::Deserialize(in, externalTableId);
    Table* table = txn->GetTableByExternalId(externalTableId);
    if (table == nullptr) {
        /* this might happen if we try to replay an outdated xlog entry - currently we do not error out */
        MOT_LOG_DEBUG("Recover Rename Column: could not find table %" PRIu64, externalTableId);
        return;
    }
    in = SerializableARR<char, Column::MAX_COLUMN_NAME_LEN>::Deserialize(in, oldName);
    in = SerializableARR<char, Column::MAX_COLUMN_NAME_LEN>::Deserialize(in, newName);
    uint64_t colId = table->GetFieldId(oldName);
    if (colId == (uint64_t)-1) {
        MOT_LOG_ERROR("Recover Rename Column: Column %s not found in table %s", oldName, table->GetTableName().c_str());
        status = RC_ERROR;
        return;
    }
    uint16_t len = strlen(newName);
    if (len >= MOT::Column::MAX_COLUMN_NAME_LEN) {
        MOT_LOG_ERROR("Recover Rename Column: Column name %s is longer %u than allowed %u",
            newName,
            len,
            MOT::Column::MAX_COLUMN_NAME_LEN);
        status = RC_ERROR;
        return;
    }
    Column* col = table->GetField(colId);
    table->GetOrigTable()->WrLock();
    status = txn->AlterTableRenameColumn(table, col, newName);
    table->GetOrigTable()->Unlock();
}

RC RecoveryOps::BeginTransaction(IRecoveryOpsContext* ctx, uint64_t replayLsn)
{
    RC rc = RC_OK;
    TxnManager* txn = ctx->GetTxn();
    txn->StartTransaction(INVALID_TRANSACTION_ID, READ_COMMITED);
    txn->SetReplayLsn(replayLsn);
    rc = txn->GcSessionStart();
    if (rc != RC_OK) {
        return rc;
    }
    txn->GcSessionEnd();
    txn->m_dummyIndex->ClearKeyCache();
    txn->m_accessMgr->ClearTableCache();
    txn->m_accessMgr->ClearDummyTableCache();
    return rc;
}
}  // namespace MOT
