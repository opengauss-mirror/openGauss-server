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
 * recovery_manager.h
 *    Handles all recovery tasks, including recovery from a checkpoint, xlog and 2 pc operations.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/src/system/recovery/recovery_manager.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef RECOVERY_MANAGER_H
#define RECOVERY_MANAGER_H

#include <set>
#include <vector>
#include "checkpoint_ctrlfile.h"
#include "redo_log_global.h"
#include "transaction_buffer_iterator.h"
#include "txn.h"
#include "global.h"
#include "mot_configuration.h"

namespace MOT {
typedef TxnCommitStatus (*commitLogStatusCallback)(uint64_t);

/**
 * @class RecoveryManager
 * @brief handles all recovery tasks, including recovery from
 * a checkpoint, xlog and 2 pc operations
 */
class RecoveryManager {
public:
    class SurrogateState;

    enum ErrCodes {
        NO_ERROR = 0,
        CP_SETUP = 1,
        CP_META = 2,
        CP_RECOVERY = 3,
        XLOG_SETUP = 4,
        XLOG_RECOVERY = 5,
        SURROGATE = 6
    };

    enum RecoveryOpState { COMMIT = 1, ABORT = 2, TPC_APPLY = 3, TPC_COMMIT = 4, TPC_ABORT = 5 };

private:
    /**
     * @class RedoTransactionSegments
     * @brief Holds an array of log segments that
     * are part of a specific transaction id
     */
    class RedoTransactionSegments {
    public:
        explicit RedoTransactionSegments(TransactionId id)
            : m_transactionId(id), m_segments(nullptr), m_count(0), m_size(0), m_maxSegments(0)
        {}

        ~RedoTransactionSegments()
        {
            if (m_segments != nullptr) {
                for (uint32_t i = 0; i < m_count; i++) {
                    FreeRedoSegment(m_segments[i]);
                }
                free(m_segments);
            }
        }

        bool Append(LogSegment* segment)
        {
            if (m_count == m_maxSegments) {
                // max segments allocated, need to extend the number of allocated LogSegments pointers
                uint32_t newMaxSegments = m_maxSegments + DEFAULT_SEGMENT_NUM;
                LogSegment** newSegments = (LogSegment**)malloc(newMaxSegments * sizeof(LogSegment*));
                if (newSegments != nullptr) {
                    if (m_segments != nullptr) {
                        errno_t erc = memcpy_s(newSegments,
                            newMaxSegments * sizeof(LogSegment*),
                            m_segments,
                            m_maxSegments * sizeof(LogSegment*));
                        securec_check(erc, "\0", "\0");
                        free(m_segments);
                    }
                    m_segments = newSegments;
                    m_maxSegments = newMaxSegments;
                } else {
                    return false;
                }
            }

            m_size += segment->m_len;
            m_segments[m_count] = segment;
            m_count += 1;
            return true;
        }

        uint64_t GetTransactionId() const
        {
            return m_transactionId;
        }

        size_t GetSize() const
        {
            return m_size;
        }

        char* GetData(size_t position, size_t length) const
        {
            if (position + length > m_size) {
                return nullptr;
            }

            uint32_t currentEntry = 0;
            while (currentEntry < m_count) {
                if (position > m_segments[currentEntry]->m_len) {
                    position -= m_segments[currentEntry]->m_len;
                    currentEntry++;
                } else {
                    if (position + length > m_segments[currentEntry]->m_len) {
                        // Cross segments is not supported for now
                        return nullptr;
                    }
                    return (m_segments[currentEntry]->m_data + position);
                }
            }
            return nullptr;
        }

        uint32_t GetCount() const
        {
            return m_count;
        }

        LogSegment* GetSegment(uint32_t index) const
        {
            if (index > m_count || m_count == 0) {
                return nullptr;
            }
            return m_segments[index];
        }

    private:
        static constexpr uint32_t DEFAULT_SEGMENT_NUM = 1024;

        uint64_t m_transactionId;

        LogSegment** m_segments;

        uint32_t m_count;

        size_t m_size;

        uint32_t m_maxSegments;
    };

public:
    RecoveryManager()
        : m_logStats(nullptr),
          m_initialized(false),
          m_checkpointId(0),
          m_lsn(0),
          m_numWorkers(GetGlobalConfiguration().m_checkpointRecoveryWorkers),
          m_tid(0),
          m_maxRecoveredCsn(0),
          m_enableLogStats(GetGlobalConfiguration().m_enableLogRecoveryStats),
          m_checkpointWorkerStop(false),
          m_errorCode(0),
          m_errorSet(false),
          m_clogCallback(nullptr),
          m_threadId(AllocThreadId()),
          m_maxConnections(GetGlobalConfiguration().m_maxConnections),
          m_numRedoOps(0)
    {}

    ~RecoveryManager()
    {}

private:
    /**
     * @brief Recovers the database state from the last valid
     * checkpoint
     * @return Boolean value denoting success or failure.
     */
    bool RecoverFromCheckpoint();

    /**
     * @brief Implements the a checkpoint recovery worker
     */
    void CpWorkerFunc();

    /**
     * @brief Reads and inserts rows from a checkpoint file
     * @param tableId The table id to recover.
     * @param seg Segment file number to recover from.
     * @param tid The current thread id
     * @param maxCsn The returned maxCsn encountered during the recovery.
     * @param sState Surrogate key state structure that will be filled
     * during the recovery
     * @return Boolean value denoting success or failure.
     */
    bool RecoverTableRows(uint32_t tableId, uint32_t seg, uint32_t tid, uint64_t& maxCsn, SurrogateState& sState);

    /**
     * @brief Reads and creates a table's defenition from a checkpoint
     * metadata file
     * @param tableId The table id to recover.
     * @return Boolean value denoting success or failure.
     */
    bool RecoverTableMetadata(uint32_t tableId);

    /**
     * @brief Pops a taske (table id and seg number) from the
     * tasks queue.
     * @param tableId The returned table id to recover.
     * @param seg the returned segment number.
     * @return Boolean value denoting if a task were retrieved or not
     */
    bool GetTask(uint32_t& tableId, uint32_t& seg);

    /**
     * @brief Reads the checkpoint map file and fills the tasks queue
     * with the relevant information.
     * @return Int value where 0 indicates no tasks (empty checkpoint),
     * -1 denotes an error has occured and 1 means a sucess.
     */
    int FillTasksFromMapFile();

    /**
     * @brief Checks if there are any more tasks left in the queue
     * @return Int value where 0 means failure and 1 success
     */
    uint32_t HaveTasks();

    /**
     * @brief Recovers the in process two phase commit related transactions
     * from the checkpoint data file.
     * @return Boolean value denoting success or failure.
     */
    bool RecoverTpcFromCheckpoint();

    /**
     * @brief Deserializes the in process two phase commit data from the
     * checkpoint data file. called by RecoverTpcFromCheckpoint.
     * @return Boolean value denoting success or failure.
     */
    bool DeserializeInProcessTxns(int fd, uint64_t numEntries);

    /**
     * @struct RecoveryTask
     * @brief Describes a recovery task by its table id and
     * segment file number.
     */
    struct RecoveryTask {
        uint32_t m_id;
        uint32_t m_seg;
    };

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
     * @class SurrogateState
     * @brief Implements a surrogate state array in order to
     * properly recover tables that were created with surrogate
     * primary keys.
     */
    class SurrogateState {
    public:
        SurrogateState();

        /**
         * @brief Extracts the surrogate counter from the key and updates
         * the array according to the connection id
         * @param key The key to extract.
         * @return Boolean value denoting success or failure.
         */
        bool UpdateMaxKey(uint64_t key);

        /**
         * @brief A helper that updates the max insertions of a thread id.
         * @param insertions The insertions count.
         * @param tid The thread id.
         */
        void UpdateMaxInsertions(uint64_t insertions, uint32_t tid);

        /**
         * @brief A helper that extracts the insertions count and
         * thread id from a key
         * @param key The key to extract the info from.
         * @param pid The returned thread id.
         * @param insertions The returned number of insertions.
         */
        inline void ExtractInfoFromKey(uint64_t key, uint64_t& pid, uint64_t& insertions);

        /**
         * @brief merges some max insertions arrays into a one single state
         * @param arrays an arrays list.
         * @param global the returned merged SurrogateState.
         */
        static void Merge(std::list<uint64_t*>& arrays, SurrogateState& global);

        bool IsEmpty() const
        {
            return m_empty;
        }

        const uint64_t* GetArray() const
        {
            return m_insertsArray;
        }

        uint32_t GetMaxConnections() const
        {
            return m_maxConnections;
        }

        bool IsValid() const
        {
            return (m_insertsArray != nullptr);
        }

        ~SurrogateState();

    private:
        uint64_t* m_insertsArray;

        bool m_empty;

        uint32_t m_maxConnections;
    };

private:
    /**
     * @class LogStats
     * @brief A per-table recovery stats collector
     */
    class LogStats {
    public:
        struct Entry {
            explicit Entry(uint64_t tableId) : m_inserts(0), m_updates(0), m_deletes(0), m_id(tableId)
            {}

            void IncInsert()
            {
                ++m_inserts;
            }

            void IncUpdate()
            {
                ++m_updates;
            }

            void IncDelete()
            {
                ++m_deletes;
            }

            std::atomic<uint64_t> m_inserts;

            std::atomic<uint64_t> m_updates;

            std::atomic<uint64_t> m_deletes;

            uint64_t m_id;
        };

        LogStats() : m_tcls(0), m_numEntries(0)
        {}

        ~LogStats()
        {
            for (std::vector<Entry*>::iterator it = m_tableStats.begin(); it != m_tableStats.end(); ++it)
                if (*it)
                    delete *it;
        }

        void IncInsert(uint64_t id)
        {
            uint64_t idx = 0;
            if (FindIdx(id, idx))
                m_tableStats[idx]->IncInsert();
        }

        void IncUpdate(uint64_t id)
        {
            uint64_t idx = 0;
            if (FindIdx(id, idx))
                m_tableStats[idx]->IncUpdate();
        }

        void IncDelete(uint64_t id)
        {
            uint64_t idx = 0;
            if (FindIdx(id, idx))
                m_tableStats[idx]->IncDelete();
        }

        /**
         * @brief Returns a table id array index. it will create
         * a new table entry if necessary.
         * @param tableId The id of the table.
         * @param id The returned array index.
         * @return Boolean value denoting success or failure.
         */
        bool FindIdx(uint64_t tableId, uint64_t& id);

        /**
         * @brief Prints the stats data to the log
         */
        void Print();

        std::map<uint64_t, int> m_idToIdx;

        std::vector<Entry*> m_tableStats;

        std::atomic<uint64_t> m_tcls;

    private:
        spin_lock m_slock;

        int m_numEntries;
    };

public:
    /**
     * @brief Performs the necessary tasks to initialize the object.
     * @return Boolean value denoting success or failure.
     */
    bool Initialize();

    /**
     * @brief Cleans up the recovery object.
     */
    void CleanUp();

    inline void SetCommitLogCallback(commitLogStatusCallback clogCallback)
    {
        this->m_clogCallback = clogCallback;
    }

    /**
     * @brief Starts the recovery process which currently consists of
     * checkpoint recovery.
     * @return Boolean value denoting success or failure.
     */
    bool RecoverDbStart();

    /**
     * @brief Performs the post recovery operations: apply the in-process
     * transactions and surrogate array, set the max csn and prints the stats
     * @return Boolean value denoting success or failure.
     */
    bool RecoverDbEnd();

    /**
     * @brief attempts to insert a data chunk into the in-process
     * transactions map and operate on it
     * @return Boolean value denoting success or failure.
     */
    bool ApplyLogSegmentFromData(char* data, size_t len);

    /**
     * @brief attempts to insert a data chunk into the in-process
     * transactions map and operate on it. Checks that the redo lsn is after the
     * checkpoint snapshot lsn taken. Redo records that are prior snapshot are
     * ignored.
     * @return Boolean value denoting success or failure.
     */
    bool ApplyLogSegmentFromData(uint64_t redoLsn, char* data, size_t len);
    /**
     * @brief performs a commit on an in-process transaction,
     * @return Boolean value denoting success or failure to commit.
     */
    bool CommitRecoveredTransaction(uint64_t externalTransactionId);

    /**
     * @brief performs an abort on an in-process transaction,
     */
    void AbortRecoveredTransaction(uint64_t internalTransactionId, uint64_t externalTransactionId);

    /**
     * @brief destroys a LogSegment and its associated data.
     */
    static void FreeRedoSegment(LogSegment* segment);

    void SetCsnIfGreater(uint64_t csn);

    /**
     * @brief adds the surrogate array inside surrogatet to the surrogate list
     */
    void AddSurrogateArrayToList(SurrogateState& surrogate);

    /**
     * @brief restores the surrogate counters to their last good known state
     */
    void ApplySurrogate();

    int GetErrorCode() const
    {
        return m_errorCode;
    }

    const char* GetErrorString() const
    {
        return m_errorMessage.c_str();
    }

    bool GetCheckpointWorkerStop() const
    {
        return m_checkpointWorkerStop;
    }

    bool IsErrorSet() const
    {
        return m_errorSet;
    }

    /**
     * @brief restores the surrogate counters to their last good known state
     */
    void OnError(int errCode, const char* errMsg, const char* optionalMsg = nullptr);

    bool IsInProcessTx(uint64_t id);

    /**
     * @brief performs a commit or abort on an in-prcoess transaction
     * @param id the transaction id.
     * @param isCommit specifies commit or abort.
     * @return Int indicates the internal transaction id or 0 in case
     * the transaction id was not found
     */
    uint64_t PerformInProcessTx(uint64_t id, bool isCommit);

    /**
     * @brief writes the contents of the in-process transactions map
     * to a file
     * @param fd the file descriptor to write to.
     * @return Boolean value denoting success or failure.
     */
    bool SerializeInProcessTxns(int fd);

    void SetCheckpointId(uint64_t id)
    {
        m_checkpointId = id;
    }

    /**
     * @brief applies a failed 2pc transaction according to its type.
     * a detailed info is described in the function's implementation.
     * @param internalTransactionId the transaction id to apply
     * @return RC value denoting the operation's status
     */
    RC ApplyInProcessTransaction(uint64_t internalTransactionId);

    void UpdateTxIdMap(uint64_t intTx, uint64_t extTx)
    {
        m_transactionIdToInternalId[extTx] = intTx;
    }

    size_t GetInProcessTxnsSize() const
    {
        return m_inProcessTransactionMap.size();
    }

    void LockInProcessTxns()
    {
        m_inProcessTxLock.lock();
    }

    void UnlockInProcessTxns()
    {
        m_inProcessTxLock.unlock();
    }

    inline void IncreaseTableDeletesStat(Table* t)
    {
        m_tableDeletesStat[t]++;
    }

    void ClearTableCache();

    LogStats* m_logStats;

    std::map<uint64_t, TableInfo*> m_preCommitedTables;

    std::unordered_map<Table*, uint32_t> m_tableDeletesStat;

private:
    static constexpr uint32_t NUM_REDO_RECOVERY_THREADS = 1;

    /**
     * @brief performs a redo on a segment, which is either a recovery op
     * or a segment that belongs to a 2pc recovered transaction.
     * @param segment the segment to redo.
     * @param csn the segment's csn
     * @param transactionId the transaction id of the segment
     * @param rState the operation to perform on the segment.
     * @return RC value denoting the operation's status
     */
    RC RedoSegment(LogSegment* segment, uint64_t csn, uint64_t transactionId, RecoveryOpState rState);

    /**
     * @brief inserts a segment in to the in-process transactions map
     * @param segment the segment to redo.
     * @return Boolean value denoting success or failure.
     */
    bool InsertLogSegment(LogSegment* segment);

    /**
     * @brief performs an operation on a recovered transaction.
     * @param internalTransactionId the internal transaction id to operate on.
     * @param externalTransactionId the external transaction id to operate on.
     * @param rState the transaction id of the segment
     * @return Boolean value denoting success or failure.
     */
    bool OperateOnRecoveredTransaction(
        uint64_t internalTransactionId, uint64_t externalTransactionId, RecoveryOpState rState);

    /**
     * @brief performs 'apply' on the in-process transactions map in the
     * post recovery stage.
     * @return RC value denoting the operation's status
     */
    RC ApplyInProcessTransactions();

    /**
     * @brief performs 'apply' on a specific segment
     * @param segment the log segment to appy
     * @param csn the segment's csn
     * @param transactionId the transaction id
     * @return RC value denoting the operation's status
     */
    RC ApplyInProcessSegment(LogSegment* segment, uint64_t csn, uint64_t transactionId);

    /**
     * @brief performs a recovery operation on a data buffer
     * @param data the buffer to recover.
     * @param csn the operations's csn.
     * @param transactionId the transaction id
     * @param tid the thread id of the recovering thread
     * @param sState the returned surrogate state of this operation
     * @param status the returned status of the operation
     * @return Int value denoting the number of bytes recovered
     */
    static uint32_t RecoverLogOperation(
        uint8_t* data, uint64_t csn, uint64_t transactionId, uint32_t tid, SurrogateState& sState, RC& status);

    /**
     * @brief performs an insert operation of a data buffer
     * @param data the buffer to recover.
     * @param csn the operations's csn.
     * @param tid the thread id of the recovering thread
     * @param sState the returned surrogate state of this operation
     * @param status the returned status of the operation
     * @return Int value denoting the number of bytes recovered
     */
    static uint32_t RecoverLogOperationInsert(
        uint8_t* data, uint64_t csn, uint32_t tid, SurrogateState& sState, RC& status);

    /**
     * @brief performs a delta update operation of a data buffer
     * @param data the buffer to recover.
     * @param csn the operations's csn.
     * @param tid the thread id of the recovering thread
     * @param status the returned status of the operation
     * @return Int value denoting the number of bytes recovered
     */
    static uint32_t RecoverLogOperationUpdate(uint8_t* data, uint64_t csn, uint32_t tid, RC& status);

    /**
     * @brief performs an update operation of a data buffer
     * @param data the buffer to recover.
     * @param csn the operations's csn.
     * @param tid the thread id of the recovering thread
     * @param sState the returned surrogate state of this operation
     * @param status the returned status of the operation
     * @return Int value denoting the number of bytes recovered
     */
    static uint32_t RecoverLogOperationOverwrite(
        uint8_t* data, uint64_t csn, uint32_t tid, SurrogateState& sState, RC& status);

    /**
     * @brief performs a delete operation of a data buffer
     * @param data the buffer to recover.
     * @param csn the operations's csn.
     * @param tid the thread id of the recovering thread
     * @param status the returned status of the operation
     * @return Int value denoting the number of bytes recovered
     */
    static uint32_t RecoverLogOperationDelete(uint8_t* data, uint64_t csn, uint32_t tid, RC& status);

    /**
     * @brief performs a commit operation of a data buffer
     * @param data the buffer to recover.
     * @param csn the operations's csn.
     * @param tid the thread id of the recovering thread
     * @return Int value denoting the number of bytes recovered
     */
    static uint32_t RecoverLogOperationCommit(uint8_t* data, uint64_t csn, uint32_t tid);

    /**
     * @brief performs a create table operation from a data buffer
     * @param data the buffer to recover.
     * @param status the returned status of the operation
     * @param state the operation's state.
     * @param transactionId the transaction id of the operation.
     * @return Int value denoting the number of bytes recovered
     */
    static uint32_t RecoverLogOperationCreateTable(
        uint8_t* data, RC& status, RecoveryOpState state, uint64_t transactionId);

    /**
     * @brief performs a drop table operation from a data buffer
     * @param data the buffer to recover.
     * @param status the returned status of the operation
     * @param state the operation's state.
     * @return Int value denoting the number of bytes recovered
     */
    static uint32_t RecoverLogOperationDropTable(uint8_t* data, RC& status, RecoveryOpState state);

    /**
     * @brief performs a create index operation from a data buffer
     * @param data the buffer to recover.
     * @param tid the thread id of the recovering thread.
     * @param status the returned status of the operation
     * @param state the operation's state.
     * @return Int value denoting the number of bytes recovered
     */
    static uint32_t RecoverLogOperationCreateIndex(uint8_t* data, uint32_t tid, RC& status, RecoveryOpState state);

    /**
     * @brief performs a drop index operation from a data buffer
     * @param data the buffer to recover.
     * @param status the returned status of the operation
     * @param state the operation's state.
     * @return Int value denoting the number of bytes recovered
     */
    static uint32_t RecoverLogOperationDropIndex(uint8_t* data, RC& status, RecoveryOpState state);

    /**
     * @brief performs a truncate table operation from a data buffer
     * @param data the buffer to recover.
     * @param status the returned status of the operation
     * @param state the operation's state.
     * @return Int value denoting the number of bytes recovered
     */
    static uint32_t RecoverLogOperationTruncateTable(uint8_t* data, RC& status, RecoveryOpState state);

    /**
     * @brief performs the actual row deletion from the storage.
     * @param tableId the table's id.
     * @param exId the the table's external id.
     * @param keyData key's data buffer.
     * @param keyLen key's data buffer len.
     * @param csn the operations's csn.
     * @param tid the thread id of the recovering thread.
     * @param status the returned status of the operation
     */
    static void DeleteRow(
        uint64_t tableId, uint64_t exId, char* keyData, uint16_t keyLen, uint64_t csn, uint32_t tid, RC& status);

    /**
     * @brief performs the actual row insertion to the storage.
     * @param tableId the table's id.
     * @param exId the the table's external id.
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
    static void InsertRow(uint64_t tableId, uint64_t exId, char* keyData, uint16_t keyLen, char* rowData,
        uint64_t rowLen, uint64_t csn, uint32_t tid, SurrogateState& sState, RC& status, uint64_t rowId,
        bool insertLocked = false);

    /**
     * @brief performs the actual row update in the storage.
     * @param tableId the table's id.
     * @param exId the the table's external id.
     * @param keyData key's data buffer.
     * @param keyLen key's data buffer len.
     * @param rowData row's data buffer.
     * @param rowLen row's data buffer len.
     * @param csn the operations's csn.
     * @param tid the thread id of the recovering thread.
     * @param sState the returned surrugate state.
     * @param status the returned status of the operation
     */
    static void UpdateRow(uint64_t tableId, uint64_t exId, char* keyData, uint16_t keyLen, char* rowData,
        uint64_t rowLen, uint64_t csn, uint32_t tid, SurrogateState& sState, RC& status);

    /**
     * @brief performs the actual table creation.
     * @param data the table's data
     * @param status the returned status of the operation
     * @param table the returned table object.
     * @param addToEngine should the table be added to the engine's metadata
     */
    static void CreateTable(char* data, RC& status, Table*& table, bool addToEngine);

    /**
     * @brief performs the actual table deletion.
     * @param data the table's data
     * @param status the returned status of the operation
     */
    static void DropTable(char* data, RC& status);

    /**
     * @brief performs the actual index creation.
     * @param data the table's data
     * @param the thread identifier
     * @param status the returned status of the operation
     */
    static void CreateIndex(char* data, uint32_t tid, RC& status);

    /**
     * @brief performs the actual index deletion.
     * @param data the table's data
     * @param status the returned status of the operation
     */
    static void DropIndex(char* data, RC& status);

    /**
     * @brief performs the actual table truncation.
     * @param data the table's data
     * @param status the returned status of the operation
     */
    static void TruncateTable(char* data, RC& status);

    /**
     * @brief attempts to recover a 2pc operation
     * @param state the state of the operation
     * @param data buffer to operate on
     * @param csn the operation's csn
     * @param transactionId the transaction's id
     * @param tid the thread id of the recovering thread.
     * @param sState the returned surrugate state.
     * @param status the returned status of the operation
     * @return Int value denoting the number of bytes recovered
     */
    static uint32_t TwoPhaseRecoverOp(RecoveryOpState state, uint8_t* data, uint64_t csn, uint64_t transactionId,
        uint32_t tid, SurrogateState& sState, RC& status);

    /**
     * @brief attempts to apply a 2pc operation
     * @param opCode the operation code.
     * @param tableId the table id that the recovered op belongs to
     * @param exId the external table id that the recovered op belongs to
     * @param csn the operation's csn
     * @param keyData key's data buffer.
     * @param keyLen key's data buffer len.
     * @param rowData row's data buffer.
     * @param rowLen row's data buffer len.
     * @param transactionId the transaction's id
     * @param tid the thread id of the recovering thread.
     * @param row a row pointer (if exists)
     * @param rowId the row id
     * @param sState the returned surrugate state
     * @param status the returned status of the operation
     */
    static void RecoverTwoPhaseApply(OperationCode opCode, uint64_t tableId, uint64_t exId, uint64_t csn,
        uint8_t* keyData, uint64_t keyLength, uint8_t* rowData, uint64_t rowLength, uint64_t transactionId,
        uint32_t tid, Row* row, uint64_t rowId, SurrogateState& sState, RC& status);

    /**
     * @brief attempts to commit a 2pc operation
     * @param table a pointer to the table object that the op belongs to.
     * @param opCode the operation code.
     * @param csn the operation's csn.
     * @param rowData row's data buffer.
     * @param rowLen row's data buffer len.
     * @param transactionId the transaction's id
     * @param tid the thread id of the recovering thread.
     * @param row a row pointer (if exists)
     * @param status the returned status of the operation
     */
    static void RecoverTwoPhaseCommit(Table* table, OperationCode opCode, uint64_t csn, uint8_t* rowData,
        uint64_t rowLength, uint64_t transactionId, uint32_t tid, Row* row, RC& status);

    /**
     * @brief attempts to abort a 2pc operation
     * @param table a pointer to the table object that the op belongs to.
     * @param opCode the operation code.
     * @param csn the operation's csn.
     * @param transactionId the transaction's id.
     * @param tid the thread id of the recovering thread.
     * @param row a row pointer (if exists)
     * @param status the returned status of the operation
     */
    static void RecoverTwoPhaseAbort(
        Table* table, OperationCode opCode, uint64_t csn, uint64_t transactionId, uint32_t tid, Row* row, RC& status);

    /**
     * @brief checks if a duplicate row exists in the table
     * @param table the table object pointer
     * @param keyData key's data buffer.
     * @param keyLen key's data buffer len.
     * @param rowData row's data buffer.
     * @param rowLen row's data buffer len.
     * @param tid the thread id of the recovering thread.
     */
    static bool DuplicateRow(
        Table* table, char* keyData, uint16_t keyLen, char* rowData, uint64_t rowLen, uint32_t tid);

    /**
     * @brief checks if an operation is supported by the recovery.
     * @param op the operation code to check.
     * @return Boolean value denoting if the op is supported.
     */
    static bool IsSupportedOp(OperationCode op);

    /**
     * @brief returns a pointer of the table by its id either from the engine or
     * from the pre-comitted table map.
     * @param id the id of the table to obtain.
     * @param table the returned table.
     * @return Boolean value denoting success or failure.
     */
    bool FetchTable(uint64_t id, Table*& table);

    /**
     * @brief checks if a transaction id specified in the log segment is an mot
     * only one.
     * @param segment the log segment to check.
     * @return Boolean value that is true if the id is an mot one.
     */
    static bool IsMotTransactionId(LogSegment* segment);

    /**
     * @brief checks the clog if a transaction id is in commit state.
     * @param xid the transaction id.
     * @return Boolean value that is true if the transaction is committed.
     */
    bool IsTransactionIdCommitted(uint64_t xid);

    /**
     * @brief returns if a checkpoint is valid by its id.
     * @param id the checkpoint's id.
     * @return Boolean value that is true if the transaction is committed.
     */
    bool IsCheckpointValid(uint64_t id);

    /**
     * @brief returns if a recovery memory limit reached.
     * @return Boolean value that is true if there is not enough memory for
     * recovery.
     */
    bool IsRecoveryMemoryLimitReached(uint32_t numThreads);

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

    bool m_initialized;

    uint64_t m_checkpointId;

    uint64_t m_lsn;

    std::string m_workingDir;

    std::set<uint32_t> m_tableIds;

    std::list<RecoveryTask*> m_tasksList;

    std::mutex m_tasksLock;

    uint32_t m_numWorkers;

    std::atomic<uint32_t> m_tid;

    std::atomic<uint64_t> m_maxRecoveredCsn;

    bool m_enableLogStats;

    SurrogateState m_surrogateState;

    std::mutex m_surrogateListLock;

    std::list<uint64_t*> m_surrogateList;

    bool m_checkpointWorkerStop;

    int m_errorCode;

    std::string m_errorMessage;

    spin_lock m_errorLock;

    bool m_errorSet;

    commitLogStatusCallback m_clogCallback;

    std::mutex m_inProcessTxLock;

    std::map<uint64_t, RedoTransactionSegments*> m_inProcessTransactionMap;

    std::map<uint64_t, uint64_t> m_transactionIdToInternalId;

    int m_threadId;

    SurrogateState m_sState;

    uint16_t m_maxConnections;

    uint32_t m_numRedoOps;
};
}  // namespace MOT

#endif  // RECOVERY_MANAGER_H
