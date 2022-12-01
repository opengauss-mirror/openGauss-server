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
 * redo_log_writer.cpp
 *    Helper class for writing redo log entries.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/transaction_logger/redo_log_writer.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <iostream>

#include "redo_log_writer.h"
#include "txn.h"
#include "utilities.h"
#include "log_statistics.h"
#include "key.h"
#include "row.h"

#define FORCE_FILE_LOGGER false

namespace MOT {
DECLARE_LOGGER(RedoLog, TxnLogger);

bool RedoLogWriter::AppendUpdate(
    RedoLogBuffer& redoLogBuffer, Row* row, BitmapSet* modifiedColumns, uint64_t previous_version, bool idxUpd)
{
    MaxKey maxKey;
    MOT::Index* index = row->GetTable()->GetPrimaryIndex();
    maxKey.InitKey(index->GetKeyLength());
    index->BuildKey(row->GetTable(), row, &maxKey);
    Table* table = row->GetTable();
    uint64_t tableId = table->GetTableId();
    uint64_t externalId = table->GetTableExId();
    uint64_t entrySize = sizeof(OperationCode) + sizeof(uint32_t) + sizeof(tableId) + sizeof(externalId) +
                         sizeof(uint16_t) /* key_length */ + index->GetKeyLength() +
                         sizeof(uint64_t) /* prev row version */ +
                         sizeof(bool) +                 /* indicates if indexed column was updated */
                         modifiedColumns->GetLength() + /* updated columns bitmap */
                         modifiedColumns->GetLength() + /* null fields bitmap (should be the same length) */
                         row->GetTupleSize();           /* not accurate, avoid double looping on updated column */
    entrySize += sizeof(EndSegmentBlock);
    if (redoLogBuffer.FreeSize() < entrySize) {
        return false;
    }

    const Key* key = &maxKey;
    uint8_t* rowData = const_cast<uint8_t*>(row->GetData());
    BitmapSet::BitmapSetIterator it(*modifiedColumns);
    BitmapSet validBitmap(rowData + table->GetFieldOffset((uint64_t)0), table->GetFieldCount() - 1);

    uint32_t metaVersion = MetadataProtoVersion::METADATA_VER_CURR;
    redoLogBuffer.Append(OperationCode::UPDATE_ROW);
    redoLogBuffer.Append(metaVersion);
    redoLogBuffer.Append(tableId);
    redoLogBuffer.Append(externalId);
    redoLogBuffer.Append(key->GetKeyLength());
    redoLogBuffer.Append(key->GetKeyBuf(), key->GetKeyLength());
    redoLogBuffer.Append(previous_version);
    redoLogBuffer.Append(idxUpd);
    redoLogBuffer.Append(modifiedColumns->GetData(), modifiedColumns->GetLength());
    redoLogBuffer.Append(validBitmap.GetData(), validBitmap.GetLength());

    (void)it.Start();
    while (!it.End()) {
        if (it.IsSet() && validBitmap.GetBit(it.GetPosition())) {
            Column* column = table->GetField(it.GetPosition() + 1);
            redoLogBuffer.Append((uint8_t*)(rowData + column->m_offset), column->m_size);
        }
        (void)it.Next();
    }
    return true;
}

bool RedoLogWriter::AppendCreateRow(RedoLogBuffer& redoLogBuffer, uint64_t table, Key* primaryKey, const void* rowData,
    uint64_t rowDataSize, uint64_t externalId, uint64_t rowId)
{
    uint64_t entrySize = sizeof(OperationCode) + sizeof(uint32_t) + sizeof(table) + sizeof(externalId) + sizeof(rowId) +
                         sizeof(uint16_t) /* key_length */ + primaryKey->GetKeyLength() + sizeof(rowDataSize) +
                         rowDataSize;
    entrySize += sizeof(EndSegmentBlock);
    if (redoLogBuffer.FreeSize() < entrySize) {
        return false;
    }

    uint32_t metaVersion = MetadataProtoVersion::METADATA_VER_CURR;
    redoLogBuffer.Append(OperationCode::CREATE_ROW);
    redoLogBuffer.Append(metaVersion);
    redoLogBuffer.Append(table);
    redoLogBuffer.Append(externalId);
    redoLogBuffer.Append(rowId);
    redoLogBuffer.Append(primaryKey->GetKeyLength());
    redoLogBuffer.Append(primaryKey->GetKeyBuf(), primaryKey->GetKeyLength());
    redoLogBuffer.Append(rowDataSize);
    redoLogBuffer.Append(rowData, rowDataSize);
    return true;
}

bool RedoLogWriter::AppendRemove(
    RedoLogBuffer& redoLogBuffer, uint64_t table, const Key* primaryKey, uint64_t version, uint64_t externalId)
{
    uint64_t entrySize = sizeof(OperationCode) + sizeof(uint32_t) + sizeof(table) + sizeof(externalId) +
                         sizeof(uint16_t) /* key_length */ + primaryKey->GetKeyLength() + sizeof(version);
    entrySize += sizeof(EndSegmentBlock);
    if (redoLogBuffer.FreeSize() < entrySize) {
        return false;
    }

    uint32_t metaVersion = MetadataProtoVersion::METADATA_VER_CURR;
    redoLogBuffer.Append(OperationCode::REMOVE_ROW);
    redoLogBuffer.Append(metaVersion);
    redoLogBuffer.Append(table);
    redoLogBuffer.Append(externalId);
    redoLogBuffer.Append(primaryKey->GetKeyLength());
    redoLogBuffer.Append(primaryKey->GetKeyBuf(), primaryKey->GetKeyLength());
    redoLogBuffer.Append(version);
    return true;
}

void RedoLogWriter::AppendCommit(RedoLogBuffer& redoLogBuffer, TxnManager* txn)
{
    uint16_t flags = 0;
    OperationCode opCode;
    if (txn->hasDDL()) {
        opCode = OperationCode::COMMIT_DDL_TX;
    } else if (txn->IsCrossEngineTxn()) {
        opCode = OperationCode::COMMIT_CROSS_TX;
    } else {
        opCode = OperationCode::COMMIT_TX;
    }

    if (txn->IsUpdateIndexColumn()) {
        flags |= EndSegmentBlock::MOT_UPDATE_INDEX_COLUMN_FLAG;
    }
    // buffer must have enough space for control entry
    EndSegmentBlock block(
        opCode, flags, txn->GetCommitSequenceNumber(), txn->GetTransactionId(), txn->GetInternalTransactionId());
    redoLogBuffer.Append(block);
}

void RedoLogWriter::AppendPrepare(RedoLogBuffer& redoLogBuffer, TxnManager* txn)
{
    uint16_t flags = 0;
    if (txn->IsUpdateIndexColumn()) {
        flags |= EndSegmentBlock::MOT_UPDATE_INDEX_COLUMN_FLAG;
    }
    // buffer must have enough space for control entry
    EndSegmentBlock block(
        OperationCode::PREPARE_TX, flags, 0, txn->GetTransactionId(), txn->GetInternalTransactionId());
    redoLogBuffer.Append(block);
}

void RedoLogWriter::AppendCommitPrepared(RedoLogBuffer& redoLogBuffer, TxnManager* txn)
{
    uint16_t flags = 0;
    if (txn->IsUpdateIndexColumn()) {
        flags |= EndSegmentBlock::MOT_UPDATE_INDEX_COLUMN_FLAG;
    }
    // buffer must have enough space for control entry
    EndSegmentBlock block(OperationCode::COMMIT_PREPARED_TX,
        flags,
        txn->GetCommitSequenceNumber(),
        txn->GetTransactionId(),
        txn->GetInternalTransactionId());
    redoLogBuffer.Append(block);
}

void RedoLogWriter::AppendRollbackPrepared(RedoLogBuffer& redoLogBuffer, TxnManager* txn)
{
    uint16_t flags = 0;
    if (txn->IsUpdateIndexColumn()) {
        flags |= EndSegmentBlock::MOT_UPDATE_INDEX_COLUMN_FLAG;
    }
    // buffer must have enough space for control entry
    EndSegmentBlock block(
        OperationCode::ROLLBACK_PREPARED_TX, flags, 0, txn->GetTransactionId(), txn->GetInternalTransactionId());
    redoLogBuffer.Append(block);
}

void RedoLogWriter::AppendRollback(RedoLogBuffer& redoLogBuffer, TxnManager* txn)
{
    uint16_t flags = 0;
    if (txn->IsUpdateIndexColumn()) {
        flags |= EndSegmentBlock::MOT_UPDATE_INDEX_COLUMN_FLAG;
    }
    // buffer must have enough space for control entry
    EndSegmentBlock block(
        OperationCode::ROLLBACK_TX, flags, 0, txn->GetTransactionId(), txn->GetInternalTransactionId());
    redoLogBuffer.Append(block);
}

void RedoLogWriter::AppendPartial(RedoLogBuffer& redoLogBuffer, TxnManager* txn)
{
    uint16_t flags = 0;
    OperationCode opCode;
    if (txn->hasDDL()) {
        opCode = OperationCode::PARTIAL_REDO_DDL_TX;
    } else if (txn->IsCrossEngineTxn()) {
        opCode = OperationCode::PARTIAL_REDO_CROSS_TX;
    } else {
        opCode = OperationCode::PARTIAL_REDO_TX;
    }

    if (txn->IsUpdateIndexColumn()) {
        flags |= EndSegmentBlock::MOT_UPDATE_INDEX_COLUMN_FLAG;
    }
    // buffer must have enough space for control entry
    EndSegmentBlock block(
        opCode, flags, txn->GetCommitSequenceNumber(), txn->GetTransactionId(), txn->GetInternalTransactionId());
    redoLogBuffer.Append(block);
}

bool RedoLogWriter::AppendIndex(RedoLogBuffer& buffer, Table* table, Index* index)
{
    size_t serializeSize = table->SerializeItemSize(index);
    uint64_t entrySize = REDO_CREATE_INDEX_FORMAT_OVERHEAD + serializeSize;
    if (buffer.FreeSize() < entrySize) {
        return false;
    }

    uint64_t tableId = table->GetTableExId();
    uint32_t metaVersion = MetadataProtoVersion::METADATA_VER_CURR;
    buffer.Append(OperationCode::CREATE_INDEX);
    buffer.Append(serializeSize);
    buffer.Append(metaVersion);
    buffer.Append(tableId);
    char* serializeBuf = (char*)buffer.AllocAppend(serializeSize);
    MOT_ASSERT(serializeBuf != nullptr);
    (void)table->SerializeItem(serializeBuf, index);

    MOT_LOG_TRACE("AppendIndex %s (%lu:%u) of table %s (%lu:%u), numIndexes %u",
        index->GetName().c_str(),
        index->GetExtId(),
        index->GetIndexId(),
        table->GetTableName().c_str(),
        table->GetTableExId(),
        table->GetTableId(),
        table->GetNumIndexes());
    return true;
}

bool RedoLogWriter::AppendDropIndex(RedoLogBuffer& buffer, Table* table, MOT::Index* index)
{
    uint64_t entrySize = sizeof(OperationCode) + sizeof(uint32_t) + sizeof(uint64_t) + sizeof(size_t) +
                         index->GetName().length() + sizeof(EndSegmentBlock);
    if (buffer.FreeSize() < entrySize) {
        return false;
    }

    uint64_t tableId = table->GetTableExId();
    uint32_t metaVersion = MetadataProtoVersion::METADATA_VER_CURR;
    buffer.Append(OperationCode::DROP_INDEX);
    buffer.Append(metaVersion);
    buffer.Append(tableId);
    buffer.Append(index->GetName().length());
    buffer.Append(index->GetName().c_str(), index->GetName().length());

    MOT_LOG_TRACE("AppendDropIndex %s (%lu:%u) of table %s (%lu:%u), numIndexes %u",
        index->GetName().c_str(),
        index->GetExtId(),
        index->GetIndexId(),
        table->GetTableName().c_str(),
        table->GetTableExId(),
        table->GetTableId(),
        table->GetNumIndexes());
    return true;
}

bool RedoLogWriter::AppendTable(RedoLogBuffer& buffer, Table* table)
{
    size_t serializeSize = table->SerializeRedoSize();
    uint64_t entrySize = REDO_CREATE_TABLE_FORMAT_OVERHEAD + serializeSize;
    if (buffer.FreeSize() < entrySize) {
        return false;
    }

    buffer.Append(OperationCode::CREATE_TABLE);
    buffer.Append(serializeSize);
    char* serializeBuf = (char*)buffer.AllocAppend(serializeSize);
    MOT_ASSERT(serializeBuf != nullptr);
    table->SerializeRedo(serializeBuf);

    MOT_LOG_TRACE("AppendTable %s (%lu:%u)", table->GetTableName().c_str(), table->GetTableExId(), table->GetTableId());
    return true;
}

bool RedoLogWriter::AppendDropTable(RedoLogBuffer& buffer, Table* table)
{
    uint64_t entrySize = sizeof(OperationCode) + sizeof(uint32_t) + sizeof(uint64_t) + sizeof(EndSegmentBlock);
    if (buffer.FreeSize() < entrySize) {
        return false;
    }

    uint64_t tableId = table->GetTableExId();
    uint32_t metaVersion = MetadataProtoVersion::METADATA_VER_CURR;
    buffer.Append(OperationCode::DROP_TABLE);
    buffer.Append(metaVersion);
    buffer.Append(tableId);

    MOT_LOG_TRACE(
        "AppendDropTable %s (%lu:%u)", table->GetTableName().c_str(), table->GetTableExId(), table->GetTableId());
    return true;
}

bool RedoLogWriter::AppendTruncateTable(RedoLogBuffer& buffer, Table* table)
{
    uint64_t entrySize = sizeof(OperationCode) + sizeof(uint32_t) + sizeof(uint64_t) + sizeof(EndSegmentBlock);
    if (buffer.FreeSize() < entrySize) {
        return false;
    }

    uint64_t tableId = table->GetTableExId();
    uint32_t metaVersion = MetadataProtoVersion::METADATA_VER_CURR;
    buffer.Append(OperationCode::TRUNCATE_TABLE);
    buffer.Append(metaVersion);
    buffer.Append(tableId);
    return true;
}

bool RedoLogWriter::AppendAlterTableAddColumn(RedoLogBuffer& buffer, Table* table, Column* col)
{
    size_t serializeSize = table->SerializeItemSize(col);
    uint64_t entrySize = REDO_ALTER_TABLE_FORMAT_OVERHEAD + serializeSize;
    if (buffer.FreeSize() < entrySize) {
        return false;
    }

    uint64_t tableId = table->GetTableExId();
    uint32_t metaVersion = MetadataProtoVersion::METADATA_VER_CURR;
    buffer.Append(OperationCode::ALTER_TABLE_ADD_COLUMN);
    buffer.Append(serializeSize);
    buffer.Append(metaVersion);
    buffer.Append(tableId);
    char* serializeBuf = (char*)buffer.AllocAppend(serializeSize);
    MOT_ASSERT(serializeBuf != nullptr);
    (void)table->SerializeItem(serializeBuf, col);
    return true;
}

bool RedoLogWriter::AppendAlterTableDropColumn(RedoLogBuffer& buffer, Table* table, Column* col)
{
    size_t serializeSize = SerializableARR<char, Column::MAX_COLUMN_NAME_LEN>::SerializeSize(col->m_name);
    uint64_t entrySize = REDO_ALTER_TABLE_FORMAT_OVERHEAD + serializeSize;
    if (buffer.FreeSize() < entrySize) {
        return false;
    }

    uint64_t tableId = table->GetTableExId();
    uint32_t metaVersion = MetadataProtoVersion::METADATA_VER_CURR;
    buffer.Append(OperationCode::ALTER_TABLE_DROP_COLUMN);
    buffer.Append(serializeSize);
    buffer.Append(metaVersion);
    buffer.Append(tableId);
    char* serializeBuf = (char*)buffer.AllocAppend(serializeSize);
    MOT_ASSERT(serializeBuf != nullptr);
    (void)SerializableARR<char, Column::MAX_COLUMN_NAME_LEN>::Serialize(serializeBuf, col->m_name);
    return true;
}

bool RedoLogWriter::AppendAlterTableRenameColumn(RedoLogBuffer& buffer, Table* table, DDLAlterTableRenameColumn* alter)
{
    size_t serializeSize = SerializableARR<char, Column::MAX_COLUMN_NAME_LEN>::SerializeSize(alter->m_oldname) +
                           SerializableARR<char, Column::MAX_COLUMN_NAME_LEN>::SerializeSize(alter->m_newname);
    uint64_t entrySize = REDO_ALTER_TABLE_FORMAT_OVERHEAD + serializeSize;
    if (buffer.FreeSize() < entrySize) {
        return false;
    }

    uint64_t tableId = table->GetTableExId();
    uint32_t metaVersion = MetadataProtoVersion::METADATA_VER_CURR;
    buffer.Append(OperationCode::ALTER_TABLE_RENAME_COLUMN);
    buffer.Append(serializeSize);
    buffer.Append(metaVersion);
    buffer.Append(tableId);
    char* serializeBuf = (char*)buffer.AllocAppend(serializeSize);
    MOT_ASSERT(serializeBuf != nullptr);
    serializeBuf = SerializableARR<char, Column::MAX_COLUMN_NAME_LEN>::Serialize(serializeBuf, alter->m_oldname);
    (void)SerializableARR<char, Column::MAX_COLUMN_NAME_LEN>::Serialize(serializeBuf, alter->m_newname);
    return true;
}

void RedoLogWriter::ApplyData(const void* data, size_t len)
{
    OperationCode opCode = *(reinterpret_cast<const OperationCode*>(data));
    MOT_LOG_DEBUG("REPL: accepted data OP: %s LEN: %lu", to_string(opCode).c_str(), len);
}

size_t EndSegmentBlockSerializer::SerializeSize(EndSegmentBlock* b)
{
    return SerializablePOD<OperationCode>::SerializeSize(b->m_opCode) +
           SerializablePOD<uint16_t>::SerializeSize(b->m_flags) +
           SerializablePOD<uint32_t>::SerializeSize(b->m_reserved) +
           SerializablePOD<uint64_t>::SerializeSize(b->m_csn) +
           SerializablePOD<uint64_t>::SerializeSize(b->m_externalTransactionId) +
           SerializablePOD<uint64_t>::SerializeSize(b->m_internalTransactionId);
}

void EndSegmentBlockSerializer::Serialize(EndSegmentBlock* b, char* dataOut)
{
    dataOut = SerializablePOD<OperationCode>::Serialize(dataOut, b->m_opCode);
    dataOut = SerializablePOD<uint16_t>::Serialize(dataOut, b->m_flags);
    dataOut = SerializablePOD<uint32_t>::Serialize(dataOut, b->m_reserved);
    dataOut = SerializablePOD<uint64_t>::Serialize(dataOut, b->m_csn);
    dataOut = SerializablePOD<uint64_t>::Serialize(dataOut, b->m_externalTransactionId);
    dataOut = SerializablePOD<uint64_t>::Serialize(dataOut, b->m_internalTransactionId);
}

void EndSegmentBlockSerializer::Deserialize(EndSegmentBlock* b, const char* in)
{
    char* dataIn = (char*)in;
    dataIn = SerializablePOD<OperationCode>::Deserialize(dataIn, b->m_opCode);
    dataIn = SerializablePOD<uint16_t>::Deserialize(dataIn, b->m_flags);
    dataIn = SerializablePOD<uint32_t>::Deserialize(dataIn, b->m_reserved);
    dataIn = SerializablePOD<uint64_t>::Deserialize(dataIn, b->m_csn);
    dataIn = SerializablePOD<uint64_t>::Deserialize(dataIn, b->m_externalTransactionId);
    dataIn = SerializablePOD<uint64_t>::Deserialize(dataIn, b->m_internalTransactionId);
}
}  // namespace MOT
