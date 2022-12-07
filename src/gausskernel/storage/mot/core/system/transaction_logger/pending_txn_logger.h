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
 * pending_txn_logger.h
 *    Provides a redo logger interface for pending recovery transaction.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/transaction_logger/pending_txn_logger.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef PENDING_TXN_LOGGER_H
#define PENDING_TXN_LOGGER_H

#include "redo_log_buffer.h"
#include "base_txn_logger.h"

namespace MOT {
class MOTConfiguration;
class TxnManager;

/**
 * @class PendingTxnLogger
 * @brief Provides a redo logger interface for pending recovery transaction.
 */
class PendingTxnLogger : public BaseTxnLogger {
public:
    static const size_t MAGIC_NUMBER = 0xaabbccdd;

    PendingTxnLogger();
    PendingTxnLogger(const PendingTxnLogger& orig) = delete;
    PendingTxnLogger& operator=(const PendingTxnLogger& orig) = delete;
    ~PendingTxnLogger() override;

    /**
     * @brief Initializes the the redo log buffer
     * @return Boolean value that indicates success or failure
     */
    bool Init();

    RC SerializePendingTransaction(TxnManager* txn, int fd);

    /**
     * @brief Writes buffer data to the logger
     */
    void WriteToLog() override;

    /**
     * @struct log entries header
     */
    struct Header {
        uint64_t m_magic = MAGIC_NUMBER;
        uint64_t m_len;
        uint64_t m_replayLsn;
    };

private:
    /**
     * @brief Resets the redo log buffer
     */
    void ResetBuffer();
    int m_fd;
};
}  // namespace MOT

#endif /* PENDING_TXN_LOGGER_H */
