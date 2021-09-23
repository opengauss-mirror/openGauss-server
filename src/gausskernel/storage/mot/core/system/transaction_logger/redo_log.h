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
 * redo_log.h
 *    Provides a redo logger interface.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/transaction_logger/redo_log.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef REDO_LOG_H
#define REDO_LOG_H

#include "redo_log_buffer.h"
#include "bitmapset.h"
#include "txn_ddl_access.h"

namespace MOT {
class MOTConfiguration;
class RedoLogHandler;
class TxnManager;
class Table;
class Row;
class Index;

/**
 * @class RedoLog
 * @brief Provides a redo logger interface
 */
class RedoLog {
public:
    RedoLog(TxnManager* txn);
    RedoLog(const RedoLog& orig) = delete;
    virtual ~RedoLog();
    /**
     * @brief Initializes the redo log state and creates
     * the redo log buffer
     */
    void Reset();

    /**
     * @brief Initializes the the redo log buffer
     * @return Boolean value that indicates success or failure
     */
    bool Init();

    void SetForceWrite()
    {
        m_forceWrite = true;
    }; /* If set, log will always be written immediately */

    /**
     * @brief Serializes the transaction and appends a commit op to it
     * @return The status of the operation.
     */
    RC Commit();

    /**
     * @brief Appends a rollback op to the buffer
     */
    void Rollback();

    /**
     * @brief Serializes the transaction and appends a prepare op to it
     * @return The status of the operation.
     */
    RC Prepare();

    /**
     * @brief Appends a commit prepared op to the buffer
     */
    void CommitPrepared();

    /**
     * @brief Appends a rollback prepared op to the buffer
     */
    void RollbackPrepared();

    /**
     * @brief Appends a row into the redo log buffer
     * @param row the row to insert
     * @return The status of the operation.
     */
    RC InsertRow(Row* row);

    /**
     * @brief Inserts an updated row into the redo log buffer
     * @param row The updaed row
     * @return The status of the operation.
     */
    RC OverwriteRow(Row* row);

    /**
     * @brief Delta updates a row
     * @param row The updated row
     * @param modifiedColumns The delta columns
     * @return The status of the operation.
     */
    RC UpdateRow(Row* row, BitmapSet& modifiedColumns);

    /**
     * @brief Inserts a delete row to the redo log buffer
     * @param row The row to delete
     * @return The status of the operation.
     */
    RC DeleteRow(Row* row);

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
     * @brief inserts a trucate table op into the redo log buffer
     * @param tablethe table to turncate.
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
    void WriteToLog();

    inline RedoLogBuffer& GetBuffer()
    {
        return *m_redoBuffer;
    }

private:
    /* Map of Index ID to DDLAccess. */
    typedef std::map<uint64_t, TxnDDLAccess::DDLAccess*> IdxDDLAccessMap;

    /**
     * @brief When the buffer is full, flush its contents to the log
     */
    void WritePartial();

    /**
     * @brief Resets the redo log buffer
     */
    void ResetBuffer();

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

    /* Member variables */
    RedoLogHandler* m_redoLogHandler;
    RedoLogBuffer* m_redoBuffer;
    MOTConfiguration& m_configuration;
    TxnManager* m_txn;
    bool m_flushed;
    bool m_forceWrite;
};
}  // namespace MOT

#endif /* REDO_LOG_H */
