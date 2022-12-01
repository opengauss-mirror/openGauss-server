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
 * base_txn_logger.h
 *    Base class for transaction logging and serialization.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/transaction_logger/base_txn_logger.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef BASE_TXN_LOGGER_H
#define BASE_TXN_LOGGER_H

#include "redo_log_buffer.h"
#include "bitmapset.h"
#include "txn_ddl_access.h"

namespace MOT {
class MOTConfiguration;
class TxnManager;
class Table;
class Row;
class Index;
class DDLAlterTableRenameColumn;
class Column;
class Access;
/**
 * @class BaseTxnLogger
 * @brief Base class for transaction logging and serialization
 */
class BaseTxnLogger {
public:
    BaseTxnLogger();
    BaseTxnLogger(const BaseTxnLogger& orig) = delete;
    BaseTxnLogger& operator=(const BaseTxnLogger& orig) = delete;
    virtual ~BaseTxnLogger();

    /**
     * @brief Appends a row into the redo log buffer
     * @param row the row to insert
     * @return The status of the operation.
     */
    RC InsertRow(Access* ac, uint64_t transaction_id);

    /**
     * @brief Delta updates a row
     * @param row The updated row
     * @param modifiedColumns The delta columns
     * @return The status of the operation.
     */
    RC UpdateRow(Access* ac, BitmapSet& modifiedColumns, bool idxUpd);

    /**
     * @brief Inserts a delete row to the redo log buffer
     * @param row The row to delete
     * @return The status of the operation.
     */
    RC DeleteRow(Access* ac);

    /**
     * @brief Inserts a table's creation data to the redo log buffer
     * @param table The table to add.
     * @return The status of the operation.
     */
    RC CreateTable(Table* table);

    /**
     * @brief Inserts a delete table op into the redo log buffer
     * @param table the table to delete.
     * @return RC value that indicates the status of the operation.
     */
    RC DropTable(Table* table);

    /**
     * @brief Inserts a create index op into the redo log buffer
     * @param index The index to create.
     * @return The status of the operation.
     */
    RC CreateIndex(Index* index);

    /**
     * @brief inserts a drop index op into the redo log buffer
     * @param index the index to delete.
     * @return RC value that indicates the status of the operation.
     */
    RC DropIndex(Index* index);

    /**
     * @brief inserts an add column op into the redo log buffer
     * @param table the table that the added column belongs to.
     * @param col added column.
     * @return RC value that indicates the status of the operation.
     */
    RC AlterTableAddColumn(Table* table, Column* col);

    /**
     * @brief inserts a drop column op into the redo log buffer
     * @param table the table that the dropped column belongs to.
     * @param col dropped column.
     * @return RC value that indicates the status of the operation.
     */
    RC AlterTableDropColumn(Table* table, Column* col);

    /**
     * @brief inserts a table rename op into the redo log buffer
     * @param table the table that the column belongs to.
     * @param alter the renamed column.
     * @return RC value that indicates the status of the operation.
     */
    RC AlterTableRenameColumn(Table* table, DDLAlterTableRenameColumn* alter);

    /**
     * @brief inserts a truncate table op into the redo log buffer
     * @param table the table to truncate.
     * @return RC value that indicates the status of the operation.
     */
    RC TruncateTable(Table* table);

    /**
     * @brief Writes the whole transaction's DDLs and DMLs into the redo buffer
     * @return The status of the operation.
     */
    RC SerializeTransaction();

    /**
     * @brief Writes buffer data to the logger
     */
    virtual void WriteToLog() = 0;

    inline void SetTxn(TxnManager* txn)
    {
        m_txn = txn;
    }

    inline RedoLogBuffer& GetBuffer()
    {
        return *m_redoBuffer;
    }

protected:
    /* Map of Index ID to DDLAccess. */
    using IdxDDLAccessMap = std::map<uint64_t, TxnDDLAccess::DDLAccess*>;
    /**
     * @brief When the buffer is full, flush its contents to the log
     */
    void WritePartial();

    /**
     * @brief Writes the whole transaction's DDLs into the redo buffer
     * @param[out] idxDDLMap Map of Index ID to DDLAccess.
     * @return The status of the operation.
     */
    RC SerializeDDLs(IdxDDLAccessMap& idxDDLMap);

    RC SerializeDropIndex(TxnDDLAccess::DDLAccess* ddlAccess, bool hasDML, IdxDDLAccessMap& idxDDLMap);

    /**
     * @brief Writes the whole transaction's DMLs into the redo buffer
     * @return The status of the operation.
     */
    RC SerializeDMLs();

    RC SerializeDMLObject(Access* access);
    /* Member variables */
    RedoLogBuffer* m_redoBuffer;
    MOTConfiguration& m_configuration;
    TxnManager* m_txn;
};
}  // namespace MOT

#endif /* BASE_TXN_LOGGER_H */
