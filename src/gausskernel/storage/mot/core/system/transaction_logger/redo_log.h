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
#include "base_txn_logger.h"

namespace MOT {
class MOTConfiguration;
class RedoLogHandler;
class TxnManager;
class Table;
class DDLAlterTableRenameColumn;
class Column;
class Row;
class Index;

/**
 * @class RedoLog
 * @brief Provides a redo logger interface
 */
class RedoLog : public BaseTxnLogger {
public:
    explicit RedoLog(TxnManager* txn);
    RedoLog(const RedoLog& orig) = delete;
    RedoLog& operator=(const RedoLog& orig) = delete;
    ~RedoLog() override;

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
     * @brief Writes buffer data to the logger
     */
    void WriteToLog() override;

private:
    /**
     * @brief Resets the redo log buffer
     */
    void ResetBuffer();

    /* Member variables */
    RedoLogHandler* m_redoLogHandler;
    bool m_flushed;
    bool m_forceWrite;
};
}  // namespace MOT

#endif /* REDO_LOG_H */
