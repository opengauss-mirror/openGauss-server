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
 * redo_log.cpp
 *    Provides a redo logger interface.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/src/system/transaction_logger/redo_log.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "mot_engine.h"
#include "row.h"
#include "table.h"
#include "redo_log_handler.h"
#include "redo_log_writer.h"
#include "redo_log.h"

namespace MOT {
RedoLog::RedoLog(TxnManager* txn)
    : m_redoLogHandler(nullptr),
      m_redoBuffer(nullptr),
      m_configuration(GetGlobalConfiguration()),
      m_txn(txn),
      m_flushed(false),
      m_forceWrite(false)
{}

RedoLog::~RedoLog()
{
    if (m_redoBuffer != nullptr)
        m_redoLogHandler->DestroyBuffer(m_redoBuffer);
}

bool RedoLog::Init()
{
    if (m_configuration.m_enableRedoLog) {
        m_redoLogHandler = MOTEngine::GetInstance()->GetRedoLogHandler();
        if (m_redoLogHandler != nullptr) {
            m_redoBuffer = m_redoLogHandler->CreateBuffer();
        }
        if (m_redoBuffer == nullptr)
            return false;
    }
    return true;
}

void RedoLog::Reset()
{
    ResetBuffer();
    m_flushed = false;
}

RC RedoLog::Commit()
{
    RC status = RC_OK;
    // write only on primary therefore we have the isRecovering condition
    if (m_configuration.m_enableRedoLog && !MOTEngine::GetInstance()->IsRecovering()) {
        // write commit op to transaction wal buffer
        status = SerializeTransaction();
        if (status == RC_OK && (m_flushed || !m_redoBuffer->Empty() || m_forceWrite)) {
            RedoLogWriter::AppendCommit(*m_redoBuffer, m_txn);
            WriteToLog();
        }
    }
    return status;
}

void RedoLog::Rollback()
{
    // if no partial done, no need to write abort transaction to the log
    if (m_configuration.m_enableRedoLog && (m_flushed || m_forceWrite)) {
        if (!m_redoBuffer->Empty()) {
            m_redoBuffer->Reset();  // no need to write transaction ops only abort
        }
        RedoLogWriter::AppendRollback(*m_redoBuffer, m_txn);
        WriteToLog();
    }
}

RC RedoLog::Prepare()
{
    RC status = RC_OK;
    // write only on primary therefore we have the isRecovering condition
    if (m_configuration.m_enableRedoLog && !MOTEngine::GetInstance()->IsRecovering()) {
        status = SerializeTransaction();
        if (status == RC_OK && (m_flushed || !m_redoBuffer->Empty())) {
            // write commit op to transaction wal buffer
            RedoLogWriter::AppendPrepare(*m_redoBuffer, m_txn);
            WriteToLog();
        }
    }
    return status;
}

void RedoLog::RollbackPrepared()
{
    if (m_configuration.m_enableRedoLog && m_flushed) {
        RedoLogWriter::AppendRollbackPrepared(*m_redoBuffer, m_txn);
        WriteToLog();
    }
}

void RedoLog::CommitPrepared()
{
    if (m_configuration.m_enableRedoLog && (m_flushed || !m_redoBuffer->Empty()) &&
        !MOTEngine::GetInstance()->IsRecovering()) {
        // write commit op to transaction wal buffer
        RedoLogWriter::AppendCommitPrepared(*m_redoBuffer, m_txn);
        WriteToLog();
    }
}

RC RedoLog::InsertRow(Row* row)
{
    if (!m_configuration.m_enableRedoLog)
        return RC_OK;
    MaxKey key;
    Index* index = row->GetTable()->GetPrimaryIndex();
    key.InitKey(index->GetKeyLength());
    index->BuildKey(row->GetTable(), row, &key);
    uint64_t tableId = row->GetTable()->GetTableId();
    uint64_t exId = row->GetTable()->GetTableExId();
    bool success = RedoLogWriter::AppendCreateRow(
        *m_redoBuffer, tableId, &key, row->GetData(), row->GetTupleSize(), exId, row->GetRowId());
    if (!success) {
        WritePartial();
        success = RedoLogWriter::AppendCreateRow(
            *m_redoBuffer, tableId, &key, row->GetData(), row->GetTupleSize(), exId, row->GetRowId());
    }
    return (success == true) ? RC_OK : RC_ERROR;
}

RC RedoLog::OverwriteRow(Row* row)
{
    if (!m_configuration.m_enableRedoLog)
        return RC_OK;
    MaxKey key;
    Index* index = row->GetTable()->GetPrimaryIndex();
    key.InitKey(index->GetKeyLength());
    index->BuildKey(row->GetTable(), row, &key);
    uint64_t tableId = row->GetTable()->GetTableId();
    uint64_t exId = row->GetTable()->GetTableExId();
    bool success =
        RedoLogWriter::AppendOverwriteRow(*m_redoBuffer, tableId, &key, row->GetData(), row->GetTupleSize(), exId);
    if (!success) {
        WritePartial();
        success =
            RedoLogWriter::AppendOverwriteRow(*m_redoBuffer, tableId, &key, row->GetData(), row->GetTupleSize(), exId);
    }
    return (success == true) ? RC_OK : RC_ERROR;
}

RC RedoLog::UpdateRow(Row* row, BitmapSet& modifiedColumns)
{
    if (!m_configuration.m_enableRedoLog)
        return RC_OK;

    bool success = RedoLogWriter::AppendUpdate(*m_redoBuffer, row, &modifiedColumns);
    if (!success) {
        WritePartial();
        success = RedoLogWriter::AppendUpdate(*m_redoBuffer, row, &modifiedColumns);
    }
    return (success == true) ? RC_OK : RC_ERROR;
}

RC RedoLog::DeleteRow(Row* row)
{
    if (!m_configuration.m_enableRedoLog)
        return RC_OK;
    MaxKey key;
    Index* index = row->GetTable()->GetPrimaryIndex();
    key.InitKey(index->GetKeyLength());
    index->BuildKey(row->GetTable(), row, &key);
    uint64_t tableId = row->GetTable()->GetTableId();
    uint64_t exId = row->GetTable()->GetTableExId();
    bool success = RedoLogWriter::AppendRemove(*m_redoBuffer, tableId, &key, exId);
    if (!success) {
        WritePartial();
        success = RedoLogWriter::AppendRemove(*m_redoBuffer, tableId, &key, exId);
    }
    return (success == true) ? RC_OK : RC_ERROR;
}

RC RedoLog::CreateTable(Table* table)
{
    if (!m_configuration.m_enableRedoLog)
        return RC_OK;
    bool success = RedoLogWriter::AppendTable(*m_redoBuffer, table);
    if (!success) {
        WritePartial();
        success = RedoLogWriter::AppendTable(*m_redoBuffer, table);
    }
    return (success == true) ? RC_OK : RC_ERROR;
}

RC RedoLog::DropTable(Table* table)
{
    if (!m_configuration.m_enableRedoLog)
        return RC_OK;
    bool success = RedoLogWriter::AppendDropTable(*m_redoBuffer, table);
    if (!success) {
        WritePartial();
        success = RedoLogWriter::AppendDropTable(*m_redoBuffer, table);
    }
    return (success == true) ? RC_OK : RC_ERROR;
}

RC RedoLog::CreateIndex(Index* index)
{
    if (!m_configuration.m_enableRedoLog)
        return RC_OK;
    bool success = RedoLogWriter::AppendIndex(*m_redoBuffer, index->GetTable(), index);
    if (!success) {
        WritePartial();
        success = RedoLogWriter::AppendIndex(*m_redoBuffer, index->GetTable(), index);
    }
    return (success == true) ? RC_OK : RC_ERROR;
}

RC RedoLog::DropIndex(Index* index)
{
    if (!m_configuration.m_enableRedoLog)
        return RC_OK;
    // No need to serialize delete of primary index. Delete of primary index can
    // only happen in case of drop table. A drop table operation will follow the
    // drop primary index operation so can disregard the drop primary index.
    if (index->IsPrimaryKey())
        return RC_OK;
    bool success = RedoLogWriter::AppendDropIndex(*m_redoBuffer, index->GetTable(), index);
    if (!success) {
        WritePartial();
        success = RedoLogWriter::AppendDropIndex(*m_redoBuffer, index->GetTable(), index);
    }
    return (success == true) ? RC_OK : RC_ERROR;
}

RC RedoLog::TruncateTable(Table* table)
{
    if (!m_configuration.m_enableRedoLog)
        return RC_OK;
    bool success = RedoLogWriter::AppendTruncateTable(*m_redoBuffer, table);
    if (!success) {
        WritePartial();
        success = RedoLogWriter::AppendTruncateTable(*m_redoBuffer, table);
    }
    return (success == true) ? RC_OK : RC_ERROR;
}

void RedoLog::WritePartial()
{
    RedoLogWriter::AppendPartial(*m_redoBuffer, m_txn);
    WriteToLog();
}

void RedoLog::WriteToLog()
{
    if (!m_redoBuffer->Empty() || m_forceWrite) {
        m_redoLogHandler->RdLock();
        m_redoBuffer = m_redoLogHandler->WriteToLog(m_redoBuffer);
        m_redoLogHandler->RdUnlock();
        ResetBuffer();
        m_flushed = true;
    }
}

RC RedoLog::SerializeTransaction()
{
    RC status = RC_OK;
    TxnDDLAccess* transactionDDLAccess = m_txn->m_txnDdlAccess;
    if (transactionDDLAccess != nullptr && transactionDDLAccess->Size() > 0) {
        RC status = RC_ERROR;
        for (uint16_t i = 0; i < transactionDDLAccess->Size(); i++) {
            Table* truncatedTable = nullptr;
            TxnDDLAccess::DDLAccess* DDLAccess = transactionDDLAccess->Get(i);
            if (DDLAccess == nullptr)
                return RC_ERROR;
            DDLAccessType accessType = DDLAccess->GetDDLAccessType();
            switch (accessType) {
                case DDL_ACCESS_CREATE_TABLE:
                    status = CreateTable((Table*)DDLAccess->GetEntry());
                    break;

                case DDL_ACCESS_DROP_TABLE:
                    status = DropTable((Table*)DDLAccess->GetEntry());
                    break;

                case DDL_ACCESS_CREATE_INDEX:
                    status = CreateIndex((Index*)DDLAccess->GetEntry());
                    break;

                case DDL_ACCESS_DROP_INDEX:
                    status = DropIndex((Index*)DDLAccess->GetEntry());
                    break;
                case DDL_ACCESS_TRUNCATE_TABLE:
                    // in case of truncate table the DDLAccess entry holds the
                    // the old indexes. We need to serialize the tableId. In this
                    // case we take it from the ddl access Oid.
                    truncatedTable = GetTableManager()->GetTableByExternal(DDLAccess->GetOid());
                    if (truncatedTable == nullptr) {
                        // This should not happen. Truncate table is protected
                        // by lock. While doing truncate table, the table cannot
                        // not be removed
                        return RC_ERROR;
                    }
                    status = TruncateTable(truncatedTable);
                    break;
                default:
                    return RC_ERROR;
                    break;
            }
            if (status != RC_OK)
                return RC_ERROR;
        }
    }

    if (m_txn->m_isLightSession)
        return RC_OK;

    for (uint32_t index = 0; index < m_txn->m_accessMgr->m_rowCnt ; index++) {
        Access* access = m_txn->m_accessMgr->GetAccessPtr(index);
        if (access != nullptr) {
            switch (access->m_type) {
                case INS:
                    if (access->m_params.IsPrimarySentinel()) {
                        if (access->m_params.IsUpgradeInsert()) {
                            status = DeleteRow(access->m_localRow);
                            if (status != RC_OK) {
                                return status;
                            }
                        }
                        status = InsertRow(access->GetTxnRow());
                    }
                    break;
                case DEL:
                    if (access->m_params.IsPrimarySentinel())
                        status = DeleteRow(access->GetTxnRow());
                    break;
                case WR:
                    status = UpdateRow(access->GetTxnRow(), access->m_modifiedColumns);
                    // case WR: status = overwriteRow(access->getRow());
                    break;
                default:
                    break;
            }
        }
        if (status != RC_OK)
            return status;
    }
    return RC_OK;
}

void RedoLog::ResetBuffer()
{
    if (m_configuration.m_enableRedoLog && m_redoLogHandler) {
        if (m_redoBuffer == nullptr) {
            m_redoBuffer = m_redoLogHandler->CreateBuffer();
        }
        m_redoBuffer->Reset();
    }
}

void RedoLog::WriteToInProcessTxns()
{
    uint32_t len = 0;
    uint8_t* data = m_redoBuffer->Serialize(&len);
    GetRecoveryManager()->ApplyLogSegmentFromData((char*)data, (size_t)len);
    ResetBuffer();
    m_flushed = true;
}

RC RedoLog::PrepareToInProcessTxns()
{
    RC status = SerializeTransaction();
    if (status == RC_OK && (m_flushed || !m_redoBuffer->Empty())) {
        RedoLogWriter::AppendPrepare(*m_redoBuffer, m_txn);
        WriteToInProcessTxns();
    }
    return status;
}
}  // namespace MOT
