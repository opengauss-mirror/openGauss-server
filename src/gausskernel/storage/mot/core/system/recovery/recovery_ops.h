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
#include "irecovery_ops_context.h"

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
        {
            m_table = nullptr;
        }

    private:
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

    enum CreateTableMethod { TRANSACTIONAL = 1, ADD_TO_ENGINE = 2, DONT_ADD_TO_ENGINE = 3 };

    /**
     * @brief performs a recovery operation on a data buffer.
     * @param recovery operation context.
     * @param data the buffer to recover.
     * @param transactionId the transaction id
     * @param tid the thread id of the recovering thread
     * @param sState the returned surrogate state of this operation
     * @param[out] status the returned status of the operation
     * @return Int value denoting the number of bytes recovered
     */
    static uint32_t RecoverLogOperation(IRecoveryOpsContext* ctx, uint8_t* data, uint64_t transactionId, uint32_t tid,
        SurrogateState& sState, RC& status);

    /**
     * @brief Starts a new transaction for recovery operations.
     * @param recovery operation context.
     * @param replayLsn the redo LSN for this transaction during replay.
     */
    static RC BeginTransaction(IRecoveryOpsContext* ctx, uint64_t replayLsn = 0);

private:
    /**
     * @brief performs an insert operation of a data buffer.
     * @param recovery operation context.
     * @param data the buffer to recover.
     * @param tid the thread id of the recovering thread.
     * @param sState the returned surrogate state of this operation.
     * @param status the returned status of the operation.
     * @return Int value denoting the number of bytes recovered.
     */
    static uint32_t RecoverLogOperationInsert(
        IRecoveryOpsContext* ctx, uint8_t* data, uint32_t tid, SurrogateState& sState, RC& status);

    /**
     * @brief performs a delta update operation of a data buffer.
     * @param recovery operation context.
     * @param data the buffer to recover.
     * @param tid the thread id of the recovering thread.
     * @param status the returned status of the operation.
     * @return Int value denoting the number of bytes recovered.
     */
    static uint32_t RecoverLogOperationUpdate(IRecoveryOpsContext* ctx, uint8_t* data, uint32_t tid, RC& status);

    /**
     * @brief performs a delete operation of a data buffer.
     * @param recovery operation context.
     * @param data the buffer to recover.
     * @param tid the thread id of the recovering thread.
     * @param status the returned status of the operation.
     * @return Int value denoting the number of bytes recovered.
     */
    static uint32_t RecoverLogOperationDelete(IRecoveryOpsContext* ctx, uint8_t* data, uint32_t tid, RC& status);

    /**
     * @brief performs a commit operation of a data buffer.
     * @param recovery operation context.
     * @param data the buffer to recover.
     * @param tid the thread id of the recovering thread.
     * @return Int value denoting the number of bytes recovered.
     */
    static uint32_t RecoverLogOperationCommit(IRecoveryOpsContext* ctx, uint8_t* data, uint32_t tid, RC& status);

    /**
     * @brief performs a rollback operation of a data buffer.
     * @param recovery operation context.
     * @param data the buffer to recover.
     * @param tid the thread id of the recovering thread.
     * @return Int value denoting the number of bytes recovered.
     */
    static uint32_t RecoverLogOperationRollback(IRecoveryOpsContext* ctx, uint8_t* data, uint32_t tid, RC& status);

    /**
     * @brief performs a create table operation from a data buffer.
     * @param recovery operation context.
     * @param data the buffer to recover.
     * @param status the returned status of the operation
     * @param transactionId the transaction id of the operation.
     * @return Int value denoting the number of bytes recovered.
     */
    static uint32_t RecoverLogOperationCreateTable(
        IRecoveryOpsContext* ctx, uint8_t* data, RC& status, uint64_t transactionId);

    /**
     * @brief performs a drop table operation from a data buffer.
     * @param recovery operation context.
     * @param data the buffer to recover.
     * @param status the returned status of the operation
     * @return Int value denoting the number of bytes recovered
     */
    static uint32_t RecoverLogOperationDropTable(IRecoveryOpsContext* ctx, uint8_t* data, RC& status);

    /**
     * @brief performs a create index operation from a data buffer.
     * @param recovery operation context.
     * @param data the buffer to recover.
     * @param tid the thread id of the recovering thread.
     * @param status the returned status of the operation.
     * @return Int value denoting the number of bytes recovered.
     */
    static uint32_t RecoverLogOperationCreateIndex(IRecoveryOpsContext* ctx, uint8_t* data, uint32_t tid, RC& status);

    /**
     * @brief performs a drop index operation from a data buffer.
     * @param recovery operation context.
     * @param data the buffer to recover.
     * @param status the returned status of the operation.
     * @return Int value denoting the number of bytes recovered.
     */
    static uint32_t RecoverLogOperationDropIndex(IRecoveryOpsContext* ctx, uint8_t* data, RC& status);

    /**
     * @brief performs a truncate table operation from a data buffer.
     * @param recovery operation context..
     * @param data the buffer to recover.
     * @param status the returned status of the operation.
     * @return Int value denoting the number of bytes recovered.
     */
    static uint32_t RecoverLogOperationTruncateTable(IRecoveryOpsContext* ctx, uint8_t* data, RC& status);

    /**
     * @brief performs an alter table add column operation from a data buffer.
     * @param recovery operation context.
     * @param data the buffer to recover.
     * @param status the returned status of the operation.
     * @return Int value denoting the number of bytes recovered.
     */
    static uint32_t RecoverLogOperationAlterTableAddColumn(IRecoveryOpsContext* ctx, uint8_t* data, RC& status);

    /**
     * @brief performs an alter table drop column operation from a data buffer.
     * @param recovery operation context.
     * @param data the buffer to recover.
     * @param status the returned status of the operation.
     * @param state the operation's state.
     * @return Int value denoting the number of bytes recovered.
     */
    static uint32_t RecoverLogOperationAlterTableDropColumn(IRecoveryOpsContext* ctx, uint8_t* data, RC& status);

    /**
     * @brief performs an alter table rename column operation from a data buffer.
     * @param recovery operation context.
     * @param data the buffer to recover.
     * @param status the returned status of the operation.
     * @param state the operation's state.
     * @return Int value denoting the number of bytes recovered.
     */
    static uint32_t RecoverLogOperationAlterTableRenameColumn(IRecoveryOpsContext* ctx, uint8_t* data, RC& status);

    /**
     * @brief performs the actual row deletion from the storage.
     * @param recovery operation context.
     * @param tableId the table's id.
     * @param exId the the table's external id.
     * @param keyData key's data buffer.
     * @param keyLen key's data buffer len.
     * @param version of the deleted row in store.
     * @param tid the thread id of the recovering thread.
     * @param isMvccUpgrade indicates if this record is from a pre mvcc version.
     * @param status the returned status of the operation
     */
    static void DeleteRow(IRecoveryOpsContext* ctx, uint64_t tableId, uint64_t exId, char* keyData, uint16_t keyLen,
        uint64_t prev_version, uint32_t tid, bool isMvccUpgrade, RC& status);

    static RC LookupRow(IRecoveryOpsContext* ctx, Table* table, Key* key, const AccessType type, uint64_t prev_version,
        bool isMvccUpgrade);

    /**
     * @brief performs the actual row insertion to the storage.
     * @param recovery operation context.
     * @param tableId the table's id.
     * @param exId the table's external id.
     * @param keyData key's data buffer.
     * @param keyLen key's data buffer len.
     * @param rowData row's data buffer.
     * @param rowLen row's data buffer len.
     * @param tid the thread id of the recovering thread.
     * @param sState the returned surrugate state.
     * @param status the returned status of the operation
     * @param rowId the row's internal id
     */
    static void InsertRow(IRecoveryOpsContext* ctx, uint64_t tableId, uint64_t exId, char* keyData, uint16_t keyLen,
        char* rowData, uint64_t rowLen, uint32_t tid, SurrogateState& sState, RC& status, uint64_t rowId);

    /**
     * @brief performs the actual table creation.
     * @param recovery operation context.
     * @param data the table's data.
     * @param status the returned status of the operation.
     * @param method controls whether the table is added to the engine or not.
     */
    static void CreateTable(IRecoveryOpsContext* ctx, char* data, RC& status, CreateTableMethod method);

    /**
     * @brief performs the actual table deletion.
     * @param recovery operation context.
     * @param data the table's data.
     * @param status the returned status of the operation.
     */
    static void DropTable(IRecoveryOpsContext* ctx, char* data, RC& status);

    /**
     * @brief performs the actual index creation.
     * @param recovery operation context.
     * @param data the table's data
     * @param the thread identifier
     * @param status the returned status of the operation
     */
    static void CreateIndex(IRecoveryOpsContext* ctx, char* data, uint32_t tid, RC& status);

    /**
     * @brief performs the actual index deletion.
     * @param recovery operation context.
     * @param data the table's data.
     * @param status the returned status of the operation.
     */
    static void DropIndex(IRecoveryOpsContext* ctx, char* data, RC& status);

    /**
     * @brief performs the actual table truncation.
     * @param recovery operation context.
     * @param data the table's data.
     * @param status the returned status of the operation.
     */
    static void TruncateTable(IRecoveryOpsContext* ctx, char* data, RC& status);

    /**
     * @brief performs the actual alter table add column.
     * @param transaction manager object.
     * @param data the table's data.
     * @param status the returned status of the operation.
     */
    static void AlterTableAddColumn(IRecoveryOpsContext* ctx, char* data, RC& status);

    /**
     * @brief performs the actual alter table drop column.
     * @param transaction manager object.
     * @param data the table's data.
     * @param status the returned status of the operation.
     */
    static void AlterTableDropColumn(IRecoveryOpsContext* ctx, char* data, RC& status);

    /**
     * @brief performs the actual alter table rename column.
     * @param transaction manager object.
     * @param data the table's data.
     * @param status the returned status of the operation.
     */
    static void AlterTableRenameColumn(IRecoveryOpsContext* ctx, char* data, RC& status);
};  // class RecoveryOps
}  // namespace MOT

#endif /* RECOVERY_OPS_H */
