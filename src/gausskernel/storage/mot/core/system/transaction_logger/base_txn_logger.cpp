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
 * base_txn_logger.cpp
 *    Base class for transaction logging and serialization.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/transaction_logger/base_txn_logger.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "mot_engine.h"
#include "row.h"
#include "txn_table.h"
#include "redo_log_writer.h"
#include "base_txn_logger.h"

namespace MOT {
DECLARE_LOGGER(RedoLog, BaseTxnLogger);

BaseTxnLogger::BaseTxnLogger() : m_redoBuffer(nullptr), m_configuration(GetGlobalConfiguration()), m_txn(nullptr)
{}

BaseTxnLogger::~BaseTxnLogger()
{
    // m_redoBuffer should be freed by derived class
    m_redoBuffer = nullptr;
    // m_txn should be freed by derived class
    m_txn = nullptr;
}

RC BaseTxnLogger::InsertRow(Access* ac, uint64_t transaction_id)
{
    if (!m_configuration.m_enableRedoLog) {
        return RC_OK;
    }

    MaxKey key;
    Row* row = ac->GetTxnRow();
    MOT::Index* index = row->GetTable()->GetPrimaryIndex();
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

RC BaseTxnLogger::UpdateRow(Access* ac, BitmapSet& modifiedColumns, bool idxUpd)
{
    if (!m_configuration.m_enableRedoLog) {
        return RC_OK;
    }

    Row* row = ac->GetTxnRow();
    uint64_t previous_version = row->GetPrimarySentinel()->GetTransactionId();
    bool success = RedoLogWriter::AppendUpdate(*m_redoBuffer, row, &modifiedColumns, previous_version, idxUpd);
    if (!success) {
        WritePartial();
        success = RedoLogWriter::AppendUpdate(*m_redoBuffer, row, &modifiedColumns, previous_version, idxUpd);
    }
    return (success == true) ? RC_OK : RC_ERROR;
}

RC BaseTxnLogger::DeleteRow(Access* ac)
{
    if (!m_configuration.m_enableRedoLog) {
        return RC_OK;
    }

    MaxKey key;
    Row* row = ac->GetGlobalVersion();
    MOT::Index* index = row->GetTable()->GetPrimaryIndex();
    key.InitKey(index->GetKeyLength());
    index->BuildKey(row->GetTable(), row, &key);
    uint64_t tableId = row->GetTable()->GetTableId();
    uint64_t previous_version = row->GetPrimarySentinel()->GetTransactionId();
    uint64_t exId = row->GetTable()->GetTableExId();
    bool success = RedoLogWriter::AppendRemove(*m_redoBuffer, tableId, &key, previous_version, exId);
    if (!success) {
        WritePartial();
        success = RedoLogWriter::AppendRemove(*m_redoBuffer, tableId, &key, previous_version, exId);
    }
    return (success == true) ? RC_OK : RC_ERROR;
}

RC BaseTxnLogger::CreateTable(Table* table)
{
    if (!m_configuration.m_enableRedoLog) {
        return RC_OK;
    }

    bool success = RedoLogWriter::AppendTable(*m_redoBuffer, table);
    if (!success) {
        WritePartial();
        success = RedoLogWriter::AppendTable(*m_redoBuffer, table);
    }
    return (success == true) ? RC_OK : RC_ERROR;
}

RC BaseTxnLogger::DropTable(Table* table)
{
    if (!m_configuration.m_enableRedoLog) {
        return RC_OK;
    }

    bool success = RedoLogWriter::AppendDropTable(*m_redoBuffer, table);
    if (!success) {
        WritePartial();
        success = RedoLogWriter::AppendDropTable(*m_redoBuffer, table);
    }
    return (success == true) ? RC_OK : RC_ERROR;
}

RC BaseTxnLogger::CreateIndex(MOT::Index* index)
{
    if (!m_configuration.m_enableRedoLog) {
        return RC_OK;
    }

    bool success = RedoLogWriter::AppendIndex(*m_redoBuffer, index->GetTable(), index);
    if (!success) {
        WritePartial();
        success = RedoLogWriter::AppendIndex(*m_redoBuffer, index->GetTable(), index);
    }
    return (success == true) ? RC_OK : RC_ERROR;
}

RC BaseTxnLogger::DropIndex(MOT::Index* index)
{
    if (!m_configuration.m_enableRedoLog) {
        return RC_OK;
    }

    // No need to serialize delete of primary index. Delete of primary index can
    // only happen in case of drop table. A drop table operation will follow the
    // drop primary index operation so can disregard the drop primary index.
    if (index->IsPrimaryKey()) {
        return RC_OK;
    }

    bool success = RedoLogWriter::AppendDropIndex(*m_redoBuffer, index->GetTable(), index);
    if (!success) {
        WritePartial();
        success = RedoLogWriter::AppendDropIndex(*m_redoBuffer, index->GetTable(), index);
    }
    return (success == true) ? RC_OK : RC_ERROR;
}

RC BaseTxnLogger::TruncateTable(Table* table)
{
    if (!m_configuration.m_enableRedoLog) {
        return RC_OK;
    }

    bool success = RedoLogWriter::AppendTruncateTable(*m_redoBuffer, table);
    if (!success) {
        WritePartial();
        success = RedoLogWriter::AppendTruncateTable(*m_redoBuffer, table);
    }
    return (success == true) ? RC_OK : RC_ERROR;
}

RC BaseTxnLogger::AlterTableAddColumn(Table* table, Column* col)
{
    if (!m_configuration.m_enableRedoLog) {
        return RC_OK;
    }

    bool success = RedoLogWriter::AppendAlterTableAddColumn(*m_redoBuffer, table, col);
    if (!success) {
        WritePartial();
        success = RedoLogWriter::AppendAlterTableAddColumn(*m_redoBuffer, table, col);
    }
    return (success == true) ? RC_OK : RC_ERROR;
}

RC BaseTxnLogger::AlterTableDropColumn(Table* table, Column* col)
{
    if (!m_configuration.m_enableRedoLog) {
        return RC_OK;
    }

    bool success = RedoLogWriter::AppendAlterTableDropColumn(*m_redoBuffer, table, col);
    if (!success) {
        WritePartial();
        success = RedoLogWriter::AppendAlterTableDropColumn(*m_redoBuffer, table, col);
    }
    return (success == true) ? RC_OK : RC_ERROR;
}

RC BaseTxnLogger::AlterTableRenameColumn(Table* table, DDLAlterTableRenameColumn* alter)
{
    if (!m_configuration.m_enableRedoLog) {
        return RC_OK;
    }

    bool success = RedoLogWriter::AppendAlterTableRenameColumn(*m_redoBuffer, table, alter);
    if (!success) {
        WritePartial();
        success = RedoLogWriter::AppendAlterTableRenameColumn(*m_redoBuffer, table, alter);
    }
    return (success == true) ? RC_OK : RC_ERROR;
}

void BaseTxnLogger::WritePartial()
{
    RedoLogWriter::AppendPartial(*m_redoBuffer, m_txn);
    WriteToLog();
}

RC BaseTxnLogger::SerializeDropIndex(TxnDDLAccess::DDLAccess* ddlAccess, bool hasDML, IdxDDLAccessMap& idxDDLMap)
{
    RC status = RC_ERROR;
    MOT::Index* index = (MOT::Index*)ddlAccess->GetEntry();
    if (index->IsPrimaryKey()) {
        status = DropIndex(index);
    } else {
        IdxDDLAccessMap::iterator it = idxDDLMap.find(index->GetExtId());
        if (it != idxDDLMap.end()) {
            // we create and drop no need for both them
            MOT_LOG_DEBUG("Erase create index: %s %lu", index->GetName().c_str(), index->GetExtId());
            (void)idxDDLMap.erase(it);
            status = RC_OK;
        } else {
            idxDDLMap[index->GetExtId()] = ddlAccess;
            status = DropIndex(index);
        }
    }
    return status;
}

RC BaseTxnLogger::SerializeDDLs(IdxDDLAccessMap& idxDDLMap)
{
    MOT::Index* index = nullptr;
    bool hasDML = (m_txn->m_accessMgr->Size() > 0 && !m_txn->m_isLightSession);
    TxnDDLAccess* transactionDDLAccess = m_txn->m_txnDdlAccess;
    if (transactionDDLAccess != nullptr && transactionDDLAccess->Size() > 0) {
        RC status = RC_ERROR;
        for (uint16_t i = 0; i < transactionDDLAccess->Size(); i++) {
            TxnDDLAccess::DDLAccess* ddlAccess = transactionDDLAccess->Get(i);
            if (ddlAccess == nullptr) {
                return RC_ERROR;
            }
            DDLAccessType accessType = ddlAccess->GetDDLAccessType();
            switch (accessType) {
                case DDL_ACCESS_CREATE_TABLE:
                    status = CreateTable((Table*)ddlAccess->GetEntry());
                    break;

                case DDL_ACCESS_DROP_TABLE:
                    status = DropTable((Table*)ddlAccess->GetEntry());
                    break;

                case DDL_ACCESS_CREATE_INDEX:
                    index = (MOT::Index*)ddlAccess->GetEntry();
                    if (index->IsPrimaryKey()) {
                        status = CreateIndex(index);
                    } else {
                        MOT_LOG_DEBUG("Defer create index: %s %lu", index->GetName().c_str(), index->GetExtId());
                        idxDDLMap[index->GetExtId()] = ddlAccess;
                        status = RC_OK;
                    }
                    break;

                case DDL_ACCESS_DROP_INDEX:
                    status = SerializeDropIndex(ddlAccess, hasDML, idxDDLMap);
                    break;
                case DDL_ACCESS_TRUNCATE_TABLE:
                    status = TruncateTable((Table*)ddlAccess->GetEntry());
                    break;
                case DDL_ACCESS_ADD_COLUMN: {
                    DDLAlterTableAddDropColumn* alterAdd = (DDLAlterTableAddDropColumn*)ddlAccess->GetEntry();
                    if (!alterAdd->m_table->GetIsNew()) {
                        status = AlterTableAddColumn(alterAdd->m_table, alterAdd->m_column);
                    }
                    break;
                }
                case DDL_ACCESS_DROP_COLUMN: {
                    DDLAlterTableAddDropColumn* alterDrop = (DDLAlterTableAddDropColumn*)ddlAccess->GetEntry();
                    if (!alterDrop->m_table->GetIsNew()) {
                        status = AlterTableDropColumn(alterDrop->m_table, alterDrop->m_column);
                    }
                    break;
                }
                case DDL_ACCESS_RENAME_COLUMN: {
                    DDLAlterTableRenameColumn* alterRename = (DDLAlterTableRenameColumn*)ddlAccess->GetEntry();
                    if (!alterRename->m_table->GetIsNew()) {
                        status = AlterTableRenameColumn(alterRename->m_table, alterRename);
                    }
                    break;
                }
                default:
                    return RC_ERROR;
            }
            if (status != RC_OK) {
                MOT_LOG_ERROR("Serialize DDL finished with error: %d", status);
                return RC_ERROR;
            }
        }
    }

    return RC_OK;
}

RC BaseTxnLogger::SerializeDMLs()
{
    RC status = RC_OK;
    std::multimap<uint32_t, Access*> orderedLog;
    TxnOrderedSet_t& orderedSet = m_txn->m_accessMgr->GetOrderedRowSet();
    for (const auto& raPair : orderedSet) {
        Access* access = raPair.second;
        if (access->m_params.IsPrimarySentinel()) {
            (void)orderedLog.insert(std::pair<uint32_t, Access*>(access->m_redoStmt, access));
        }
    }
    for (const auto& raPair : orderedLog) {
        Access* access = raPair.second;
        status = SerializeDMLObject(access);
        if (status != RC_OK) {
            MOT_LOG_ERROR("Serialize DML finished with error: %d", status);
            return status;
        }
    }

    return RC_OK;
}

RC BaseTxnLogger::SerializeDMLObject(Access* access)
{
    RC status = RC_OK;
    if (access != nullptr) {
        switch (access->m_type) {
            case INS:
                if (access->m_params.IsPrimarySentinel()) {
                    if (access->m_params.IsUpgradeInsert() == true) {
                        // Check if we insert on a tombstone or deleted internally
                        if (access->m_params.IsInsertOnDeletedRow() == false) {
                            status = DeleteRow(access);
                            if (status != RC_OK) {
                                return status;
                            }
                        }
                    }
                    status = InsertRow(access, m_txn->GetInternalTransactionId());
                }
                break;
            case DEL:
                if (access->m_params.IsPrimarySentinel()) {
                    status = DeleteRow(access);
                }
                break;
            case WR:
                if (access->m_params.IsPrimarySentinel()) {
                    status = UpdateRow(access, access->m_modifiedColumns, access->m_params.IsIndexUpdate());
                }
                break;
            default:
                break;
        }
    }

    return status;
}

RC BaseTxnLogger::SerializeTransaction()
{
    MOT_LOG_TRACE("BaseTransactionLogger::SerializeTransaction - Serializing TXN [%lu:%lu]",
        m_txn->GetInternalTransactionId(),
        m_txn->GetTransactionId());

    IdxDDLAccessMap idxDDLMap;
    RC status = SerializeDDLs(idxDDLMap);
    if (status != RC_OK) {
        MOT_LOG_ERROR("Failed to serialize DDLs: %d", status);
        return status;
    }

    if (m_txn->m_isLightSession) {
        MOT_LOG_DEBUG("Serialize DDL light session finished");
        return RC_OK;
    }

    status = SerializeDMLs();
    if (status != RC_OK) {
        MOT_LOG_ERROR("Failed to serialize DMLs: %d", status);
        return status;
    }

    // create operations for unique indexes
    IdxDDLAccessMap::iterator it = idxDDLMap.begin();
    while (it != idxDDLMap.end()) {
        DDLAccessType accessType = it->second->GetDDLAccessType();
        switch (accessType) {
            case DDL_ACCESS_CREATE_INDEX: {
                MOT::Index* index = (MOT::Index*)it->second->GetEntry();
                MOT_LOG_DEBUG("Send create index: %s %lu", index->GetName().c_str(), index->GetExtId());
                status = CreateIndex(index);
                break;
            }
            default:
                break;
        }

        if (status != RC_OK) {
            return RC_ERROR;
        }

        (void)++it;
    }
    idxDDLMap.clear();
    return RC_OK;
}
}  // namespace MOT
