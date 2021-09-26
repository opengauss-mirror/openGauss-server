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
 * recovery_ops.h
 *   Recovery logic for various operation types.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/recovery/recovery_ops.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef RECOVERY_OPS_H
#define RECOVERY_OPS_H

#include "redo_log_global.h"
#include "redo_log_transaction_iterator.h"
#include "txn.h"
#include "global.h"
#include "surrogate_state.h"

namespace MOT {
class RecoveryOps {
public:
    /**
     * @struct TableInfo
     * @brief  Describes a table by its id and the transaction
     * id that it was created with.
     */
    struct TableInfo {
    public:
        TableInfo(Table* t, uint64_t i) : m_table(t), m_transactionId(i)
        {}

        ~TableInfo()
        {}

        Table* m_table;

        uint64_t m_transactionId;
    };

    /**
     * @brief a helper to extract a type from a buffer
     * @param data the data buffer to extract from.
     * @param out the output value.
     */
    template <typename T>
    static void Extract(uint8_t*& data, T& out)
    {
        T* temp = static_cast<T*>((void*)data);
        data += sizeof(T);
        out = *temp;
    }

    /**
     * @brief a helper to extract a pointer type from a  buffer
     * @param data the data buffer to extract from.
     * @param size the size of the buffer to extract.
     * @return the pointer that was extracted.
     */
    static uint8_t* ExtractPtr(uint8_t*& data, uint32_t size)
    {
        uint8_t* outptr = data;
        data += size;
        return outptr;
    }

    enum RecoveryOpState { COMMIT = 1, ABORT = 2, TPC_APPLY = 3, TPC_COMMIT = 4, TPC_ABORT = 5 };

    enum CreateTableMethod { TRANSACTIONAL = 1, ADD_TO_ENGINE = 2, DONT_ADD_TO_ENGINE = 3 };

    /**
     * @brief performs a recovery operation on a data buffer.
     * @param transaction manager object.
     * @param data the buffer to recover.
     * @param csn the operations's csn.
     * @param transactionId the transaction id
     * @param tid the thread id of the recovering thread
     * @param sState the returned surrogate state of this operation
     * @param[out] status the returned status of the operation
     * @param[out] Was this a commit operation (required for managing transactional state).
     * @return Int value denoting the number of bytes recovered
     */
    static uint32_t RecoverLogOperation(TxnManager* txn, uint8_t* data, uint64_t csn, uint64_t transactionId,
        uint32_t tid, SurrogateState& sState, RC& status, bool& wasCommit);

    /**
     * @brief Starts a new transaction for recovery operations.
     * @param transaction manager object.
     * @param replayLsn the redo LSN for this transaction during replay.
     */
    static RC BeginTransaction(TxnManager* txn, uint64_t replayLsn = 0);

private:
    /**
     * @brief performs an insert operation of a data buffer.
     * @param transaction manager object.
     * @param data the buffer to recover.
     * @param csn the operations's csn.
     * @param tid the thread id of the recovering thread.
     * @param sState the returned surrogate state of this operation.
     * @param status the returned status of the operation.
     * @return Int value denoting the number of bytes recovered.
     */
    static uint32_t RecoverLogOperationInsert(
        TxnManager* txn, uint8_t* data, uint64_t csn, uint32_t tid, SurrogateState& sState, RC& status);

    /**
     * @brief performs a delta update operation of a data buffer.
     * @param data the buffer to recover.
     * @param csn the operations's csn.
     * @param tid the thread id of the recovering thread.
     * @param status the returned status of the operation.
     * @return Int value denoting the number of bytes recovered.
     */
    static uint32_t RecoverLogOperationUpdate(TxnManager* txn, uint8_t* data, uint64_t csn, uint32_t tid, RC& status);

    /**
     * @brief performs an update operation of a data buffer.
     * @param transaction manager object.
     * @param data the buffer to recover.
     * @param csn the operations's csn.
     * @param tid the thread id of the recovering thread.
     * @param sState the returned surrogate state of this operation.
     * @param status the returned status of the operation..
     * @return Int value denoting the number of bytes recovered..
     */
    static uint32_t RecoverLogOperationOverwrite(
        TxnManager* txn, uint8_t* data, uint64_t csn, uint32_t tid, SurrogateState& sState, RC& status);

    /**
     * @brief performs a delete operation of a data buffer.
     * @param transaction manager object.
     * @param data the buffer to recover.
     * @param csn the operations's csn.
     * @param tid the thread id of the recovering thread.
     * @param status the returned status of the operation.
     * @return Int value denoting the number of bytes recovered.
     */
    static uint32_t RecoverLogOperationDelete(TxnManager* txn, uint8_t* data, uint64_t csn, uint32_t tid, RC& status);

    /**
     * @brief performs a commit operation of a data buffer.
     * @param transaction manager object.
     * @param data the buffer to recover.
     * @param csn the operations's csn.
     * @param tid the thread id of the recovering thread.
     * @return Int value denoting the number of bytes recovered.
     */
    static uint32_t RecoverLogOperationCommit(TxnManager* txn, uint8_t* data, uint64_t csn, uint32_t tid, RC& status);

    /**
     * @brief performs a rollback operation of a data buffer.
     * @param transaction manager object.
     * @param data the buffer to recover.
     * @param csn The CSN of the operation.
     * @param tid the thread id of the recovering thread.
     * @return Int value denoting the number of bytes recovered.
     */
    static uint32_t RecoverLogOperationRollback(TxnManager* txn, uint8_t* data, uint64_t csn, uint32_t tid, RC& status);

    /**
     * @brief performs a create table operation from a data buffer.
     * @param transaction manager object.
     * @param data the buffer to recover.
     * @param status the returned status of the operation
     * @param state the operation's state.
     * @param transactionId the transaction id of the operation.
     * @return Int value denoting the number of bytes recovered.
     */
    static uint32_t RecoverLogOperationCreateTable(
        TxnManager* txn, uint8_t* data, RC& status, RecoveryOpState state, uint64_t transactionId);

    /**
     * @brief performs a drop table operation from a data buffer.
     * @param transaction manager object.
     * @param data the buffer to recover.
     * @param status the returned status of the operation
     * @param state the operation's state.
     * @return Int value denoting the number of bytes recovered
     */
    static uint32_t RecoverLogOperationDropTable(TxnManager* txn, uint8_t* data, RC& status, RecoveryOpState state);

    /**
     * @brief performs a create index operation from a data buffer.
     * @param data the buffer to recover.
     * @param tid the thread id of the recovering thread.
     * @param status the returned status of the operation.
     * @param state the operation's state.
     * @return Int value denoting the number of bytes recovered.
     */
    static uint32_t RecoverLogOperationCreateIndex(
        TxnManager* txn, uint8_t* data, uint32_t tid, RC& status, RecoveryOpState state);

    /**
     * @brief performs a drop index operation from a data buffer.
     * @param transaction manager object.
     * @param data the buffer to recover.
     * @param status the returned status of the operation.
     * @param state the operation's state.
     * @return Int value denoting the number of bytes recovered.
     */
    static uint32_t RecoverLogOperationDropIndex(TxnManager* txn, uint8_t* data, RC& status, RecoveryOpState state);

    /**
     * @brief performs a truncate table operation from a data buffer.
     * @param transaction manager object.
     * @param data the buffer to recover.
     * @param status the returned status of the operation.
     * @param state the operation's state.
     * @return Int value denoting the number of bytes recovered.
     */
    static uint32_t RecoverLogOperationTruncateTable(TxnManager* txn, uint8_t* data, RC& status, RecoveryOpState state);

    /**
     * @brief performs the actual row deletion from the storage.
     * @param transaction manager object.
     * @param tableId the table's id.
     * @param exId the the table's external id.
     * @param keyData key's data buffer.
     * @param keyLen key's data buffer len.
     * @param csn the operations's csn.
     * @param tid the thread id of the recovering thread.
     * @param status the returned status of the operation
     */
    static void DeleteRow(TxnManager* txn, uint64_t tableId, uint64_t exId, char* keyData, uint16_t keyLen,
        uint64_t csn, uint32_t tid, RC& status);

    /**
     * @brief performs the actual row insertion to the storage.
     * @param transaction manager object.
     * @param tableId the table's id.
     * @param exId the table's external id.
     * @param keyData key's data buffer.
     * @param keyLen key's data buffer len.
     * @param rowData row's data buffer.
     * @param rowLen row's data buffer len.
     * @param csn the operations's csn.
     * @param tid the thread id of the recovering thread.
     * @param sState the returned surrugate state.
     * @param status the returned status of the operation
     * @param rowId the row's internal id
     * @param insertLocked should this row be inserted locked
     */
    static void InsertRow(TxnManager* txn, uint64_t tableId, uint64_t exId, char* keyData, uint16_t keyLen,
        char* rowData, uint64_t rowLen, uint64_t csn, uint32_t tid, SurrogateState& sState, RC& status, uint64_t rowId,
        bool insertLocked = false);

    /**
     * @brief performs the actual row update in the storage.
     * @param transaction manager object.
     * @param tableId the table's id.
     * @param exId the the table's external id.
     * @param keyData key's data buffer.
     * @param keyLen key's data buffer len.
     * @param rowData row's data buffer.
     * @param rowLen row's data buffer len.
     * @param csn the operations's csn.
     * @param tid the thread id of the recovering thread.
     * @param sState the returned surrugate state.
     * @param status the returned status of the operation.
     */
    static void UpdateRow(TxnManager* txn, uint64_t tableId, uint64_t exId, char* keyData, uint16_t keyLen,
        char* rowData, uint64_t rowLen, uint64_t csn, uint32_t tid, SurrogateState& sState, RC& status);

    /**
     * @brief performs the actual table creation.
     * @param transaction manager object.
     * @param data the table's data.
     * @param status the returned status of the operation.
     * @param table the returned table object.
     * @param method controls whether the table is added to the engine or not.
     */
    static void CreateTable(TxnManager* txn, char* data, RC& status, Table*& table, CreateTableMethod method);

    /**
     * @brief performs the actual table deletion.
     * @param transaction manager object.
     * @param data the table's data.
     * @param status the returned status of the operation.
     */
    static void DropTable(TxnManager* txn, char* data, RC& status);

    /**
     * @brief performs the actual index creation.
     * @param transaction manager object.
     * @param data the table's data
     * @param the thread identifier
     * @param status the returned status of the operation
     */
    static void CreateIndex(TxnManager* txn, char* data, uint32_t tid, RC& status);

    /**
     * @brief performs the actual index deletion.
     * @param transaction manager object.
     * @param data the table's data.
     * @param status the returned status of the operation.
     */
    static void DropIndex(TxnManager* txn, char* data, RC& status);

    /**
     * @brief performs the actual table truncation.
     * @param transaction manager object.
     * @param data the table's data.
     * @param status the returned status of the operation.
     */
    static void TruncateTable(TxnManager* txn, char* data, RC& status);

    /**
     * @brief Commits the current recovery transaction.
     * @param transaction manager object.
     * @param transaction's commit sequence number.
     */
    static RC CommitTransaction(TxnManager* txn, uint64_t csn);

    /**
     * @brief Rolls back the current recovery transaction.
     * @param transaction manager object.
     */
    static void RollbackTransaction(TxnManager* txn);
};  // class RecoveryOps
}  // namespace MOT

#endif /* RECOVERY_OPS_H */
