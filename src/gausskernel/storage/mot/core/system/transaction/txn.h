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
 * txn.h
 *    Transaction manager used to manage the life cycle of a single transaction.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/transaction/txn.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef MOT_TXN_H
#define MOT_TXN_H

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <unordered_map>

#include "global.h"
#include "redo_log.h"
#include "occ_transaction_manager.h"
#include "session_context.h"
#include "surrogate_key_generator.h"
#include "mm_gc_manager.h"
#include "bitmapset.h"
#include "txn_ddl_access.h"
#include "sub_txn_mgr.h"
#include "mm_session_api.h"
#include "mm_buffer_api.h"

namespace MOT {
class MOTContext;
class IndexIterator;
class CheckpointManager;
class AsyncRedoLogHandler;

class Sentinel;
class TxnInsertAction;
class TxnAccess;
class InsItem;
class LoggerTask;
class Key;
class Index;
class TxnTable;
class DummyIndex;
class TxnIxColUpdate;

class TransactionIdManager {
public:
    TransactionIdManager() : m_id(0)
    {}

    ~TransactionIdManager()
    {}

    /** @brief Used to enforce the last transaction id value at the end of the recovery. */
    void SetId(uint64_t value)
    {
        if (m_id.load() < value) {
            m_id.store(value);
        }
    }

    /** @brief Get the next id. This method also increment the current id. */
    uint64_t GetNextId()
    {
        uint64_t current = 0;
        uint64_t next = 0;
        do {
            current = m_id.load();
            next = current + 1;
        } while (!m_id.compare_exchange_strong(current, next, std::memory_order_acq_rel));
        return next;
    }

    /** @brief Get the current id. This method does not change the value of the current id. */
    inline uint64_t GetCurrentId() const
    {
        return m_id.load();
    }

private:
    /** @brief Atomic uint64_t holds the current id. */
    std::atomic<uint64_t> m_id;
};

#define MOTCurrTxn MOT_GET_CURRENT_SESSION_CONTEXT()->GetTxnManager()

/**
 * @class TxnManager
 * @brief Transaction manager is used to manage the life cycle of a single
 * transaction. It is a pooled object, meaning it is allocated from a NUMA-aware
 * memory pool.
 */
class alignas(64) TxnManager {
public:
    /** @brief Destructor. */
    ~TxnManager();

    /**
     * @brief Constructor.
     * @param session_context The owning session's context.
     */
    explicit TxnManager(SessionContext* sessionContext, bool global = false);

    /**
     * @brief Initializes the object
     * @param threadId The logical identifier of the thread executing this transaction.
     * @param connectionId The logical identifier of the connection executing this transaction.
     * @param isRecoveryTxn Type of txnManager.
     * @return Boolean value denoting success or failure.
     */
    bool Init(uint64_t threadId, uint64_t connectionId, bool isRecoveryTxn, bool isLightTxn = false);

    /**
     * @brief Override placement-new operator.
     * @param size Unused.
     * @param ptr The pointer to the memory buffer.
     * @return The pointer to the memory buffer.
     */
    static inline __attribute__((always_inline)) void* operator new(size_t size, void* ptr) noexcept
    {
        (void)size;
        return ptr;
    }

    /**
     * @brief Retrieves the logical identifier of the thread in which the
     * transaction is being executed.
     * @return The thread identifier.
     */
    inline uint64_t GetThdId() const
    {
        return m_threadId;
    }  // Attention: this may be invalid in thread-pooled envelope!

    inline void SetThdId(uint64_t tid)
    {
        m_threadId = tid;
        if (m_gcSession != nullptr) {
            m_gcSession->SetThreadId(tid);
        }
    }

    inline uint64_t GetConnectionId() const
    {
        return m_connectionId;
    }

    inline void SetConnectionId(uint64_t id)
    {
        m_connectionId = id;
    }

    inline uint64_t GetCommitSequenceNumber() const
    {
        return m_csn;
    }

    inline void SetCommitSequenceNumber(uint64_t commitSequenceNumber)
    {
        m_csn = commitSequenceNumber;
    }

    inline uint64_t GetTransactionId() const
    {
        return m_transactionId;
    }

    inline void SetTransactionId(uint64_t transactionId)
    {
        m_transactionId = transactionId;
    }

    inline uint64_t GetInternalTransactionId() const
    {
        return m_internalTransactionId;
    }

    inline void SetInternalTransactionId(uint64_t transactionId)
    {
        m_internalTransactionId = transactionId;
    }

    inline void SetReplayLsn(uint64_t replayLsn)
    {
        m_replayLsn = replayLsn;
    }

    inline uint64_t GetReplayLsn() const
    {
        return m_replayLsn;
    }

    bool IsReadOnlyTxn() const;

    bool hasDDL() const;

    void RemoveTableFromStat(Table* t);

    /**
     * @brief Start transaction, set transaction id and isolation level.
     * @return void.
     */
    void StartTransaction(uint64_t transactionId, int isolationLevel);

    /**
     * @brief Start sub transaction, set sub-transaction Id and isolation level.
     * @return void.
     */
    void StartSubTransaction(uint64_t subTransactionId, int isolationLevel);

    /**
     * @brief Commit sub transaction.
     * @return void.
     */
    void CommitSubTransaction(uint64_t subTransactionId);

    /**
     * @brief Rollback sub transaction.
     * @return Result code denoting success or failure.
     */
    RC RollbackSubTransaction(uint64_t subTransactionId);

    void SetTxnOper(SubTxnOperType subOper)
    {
        m_subTxnManager.SetSubTxnOper(subOper);
    }

    bool IsSubTxnStarted() const
    {
        return m_subTxnManager.IsSubTxnStarted();
    }

    bool IsTxnAborted() const
    {
        return m_isTxnAborted;
    }

    void SetTxnAborted()
    {
        m_isTxnAborted = true;
    }

    void SetHasCommitedSubTxnDDL()
    {
        m_hasCommitedSubTxnDDL = true;
    }

    bool HasCommitedSubTxnDDL() const
    {
        return m_hasCommitedSubTxnDDL;
    }

    /**
     * @brief Checks if the transaction has any modifications in disk engine as well.
     * Attention: This flag is set only in validate commit phase, so this interface should not be used before that.
     * Used only by RedoLogWriter to mark the transaction as a cross engine transaction. Recovery needs to know if
     * the transaction is MOT only or Cross Engine transaction.
     */
    bool IsCrossEngineTxn() const
    {
        return m_isCrossEngineTxn;
    }

    /**
     * @brief Sets the flag to indicate that the transaction has modifications in disk engine. This is set only
     * during the validate commit phase (before writing redo) to indicate that the transaction has already inserted
     * XLOG records for the modifications done in the disk engine.
     */
    void MarkAsCrossEngineTxn()
    {
        m_isCrossEngineTxn = true;
    }

    bool IsRecoveryTxn() const
    {
        return m_isRecoveryTxn;
    }

    bool IsUpdateIndexColumn() const
    {
        return m_hasIxColUpd;
    }

    /**
     * @brief Performs pre-commit validation (OCC validation).
     * @return Result code denoting success or failure.
     */
    RC ValidateCommit();

    /**
     * @brief Performs the actual commit (write redo and apply changes).
     */
    void RecordCommit();

    /**
     * @brief Convenience interface which does both ValidateCommit and RecordCommit.
     */
    RC Commit();
    void LiteCommit();

    /**
     * @brief End transaction, release locks and perform cleanups.
     * @detail Finishes a transaction. This method is automatically called after
     * commit and is not necessary to be called manually. It propagates the
     * changes to the redo log and prepares this object to execute another
     * transaction.
     */
    void EndTransaction();

    /**
     * @brief Commits the prepared transaction in 2PC (performs OCC last validation phase).
     * @return Result code denoting success or failure.
     */
    void CommitPrepared();
    void LiteCommitPrepared();

    /**
     * @brief Prepare the transaction in 2PC (performs OCC first validation phase).
     * @return Result code denoting success or failure.
     */
    RC Prepare();
    void LitePrepare();

    /**
     * @brief Rolls back the transaction. No changes are propagated to the logs.
     */
    inline void Rollback()
    {
        RollbackInternal(false);
    }

    void LiteRollback();

    /**
     * @brief Rollback the prepared transaction in 2PC.
     */
    inline void RollbackPrepared()
    {
        RollbackInternal(true);
    }

    void LiteRollbackPrepared();

    /**
     * @brief Rolls back the transaction. No changes are propagated to the logs.
     */
    void CleanTxn();

    /**
     * @brief Retrieves the owning session context.
     * @return The session context.
     */
    inline SessionContext* GetSessionContext()
    {
        return m_sessionContext;
    }

    /**
     * @brief Inserts a row and updates all affected indices.
     * @param row The row to insert.
     * @param isUpdateColumn Indicate whether the insert source is update column
     * @return Return code denoting the execution result.
     */
    RC InsertRow(Row* row, Key* updateColumnKey = nullptr);

    RC InsertRecoveredRow(Row* row);

    InsItem* GetNextInsertItem(Index* index = nullptr);
    Key* GetTxnKey(Index* index);

    void DestroyTxnKey(Key* key) const;

    /**
     * @brief Searches for a row in the local cache by a primary key.
     * @detail The row is searched in the local cache and if not found then the
     * primary index of the table is consulted. In such a case the found row is
     * stored in the local cache for subsequent searches.
     * @param table The table in which the row is to be searched.
     * @param type The purpose for retrieving the row.
     * @param currentKey The primary key by which to search the row.
     * @return The row or null pointer if none was found.
     */
    Row* RowLookupByKey(Table* const& table, const AccessType type, MOT::Key* const currentKey, RC& rc);

    /**
     * @brief Searches for a row in the local cache by a row.
     * @detail Rows may be updated concurrently, so the cache layer needs to be
     * consulted to retrieves the most recent version of a row.
     * @param type The operation requested RD/WR/DEL/INS.
     * @param originalSentinel The key for the local cache.
     * @param rc The return code.
     * @return The cached row or the original row if none was found in the cache (in
     * which case the original row is stored in the cache for subsequent searches).
     */
    Row* RowLookup(const AccessType type, Sentinel* const& originalSentinel, RC& rc);

    bool IsRowExist(Sentinel* const& sentinel, RC& rc);

    /**
     * @brief Searches for a row in the local cache by a row.
     * @detail Rows may be updated concurrently, so the cache layer needs to be
     * consulted to retrieves the most recent version of a row.
     * @param type The operation requested RD/WR/DEL/INS.
     * @param originalSentinel The key for the local cache.
     * @param localRow The cached row or null pointer if the row is not cached.
     * @return The cached row or the original row if none was found in the cache (in
     * which case the original row is stored in the cache for subsequent searches).
     */
    RC AccessLookup(const AccessType type, Sentinel* const& originalSentinel, Row*& localRow);

    RC DeleteLastRow();

    /**
     * @brief Updates the state of the last row accessed in the local cache.
     * @param state The new row state.
     * @return Return code denoting the execution result.
     */
    RC UpdateLastRowState(AccessType state);

    void UndoLocalDMLChanges();

    void RollbackInsert(Access* ac);

    void RollbackSecondaryIndexInsert(Index* index);

    void RollbackDDLs();

    RC OverwriteRow(Row* updatedRow, const BitmapSet& modifiedColumns);
    RC UpdateRow(const BitmapSet& modifiedColumns, TxnIxColUpdate* colUpd);

    /**
     * @brief Updates the value in a single field in a row.
     * @param row The row to update.
     * @param attr_id The column identifier.
     * @param attr_value The new field value.
     */
    RC UpdateRow(Row* row, const int attr_id, double attr_value);
    RC UpdateRow(Row* row, const int attr_id, uint64_t attr_value);

    Row* GetLastAccessedDraft();

    /**
     * @brief Retrieves the latest epoch seen by this transaction.
     * @return The latest epoch.
     */
    inline uint64_t GetVisibleCSN() const
    {
        return m_visibleCSN;
    }

    inline void SetVisibleCSN(uint64_t csn)
    {
        m_visibleCSN = csn;
    }

    /**
     * @brief Retrieves the transaction state.
     * @return The transaction state.
     */
    inline TxnState GetTxnState() const
    {
        return m_state;
    }

    /**
     * @brief Sets the transaction state.
     */
    void SetTxnState(TxnState envelopeState);

    /**
     * @brief Retrieves the transaction isolation level.
     * @return The transaction isolation level.
     */
    inline ISOLATION_LEVEL GetIsolationLevel() const
    {
        return m_isolationLevel;
    }

    /**
     * @brief Sets the transaction isolation level.
     */
    inline void SetIsolationLevel(int envelopeIsoLevel)
    {
        m_isolationLevel = static_cast<ISOLATION_LEVEL>(envelopeIsoLevel);
        if (m_isolationLevel == SERIALIZABLE) {
            m_isolationLevel = REPEATABLE_READ;
        }
    }

    inline void IncStmtCount()
    {
        m_internalStmtCount++;
    }

    inline uint32_t GetStmtCount() const
    {
        return m_internalStmtCount;
    }

    uint64_t GetSurrogateKey()
    {
        return m_surrogateGen.GetSurrogateKey(m_connectionId);
    }

    uint64_t GetSurrogateCount() const
    {
        return m_surrogateGen.GetCurrentCount();
    }

    RC GcSessionStart(uint64_t csn = 0)
    {
        return m_gcSession->GcStartTxn(csn);
    }

    void GcSessionRecordRcu(GC_QUEUE_TYPE m_type, uint32_t index_id, void* object_ptr, void* object_pool,
        DestroyValueCbFunc cb, uint32_t obj_size, uint64_t csn = 0);

    inline void GcSessionEnd()
    {
        m_gcSession->GcEndTxn();
    }

    inline void GcSessionEndRecovery()
    {
        m_gcSession->GcRecoveryEndTxn();
    }

    inline void GcEndStatement()
    {
        MOT_ASSERT(IsReadOnlyTxn());
        m_gcSession->GcEndStatement();
    }

    inline void GcAddSession()
    {
        if (m_isLightSession) {
            return;
        }
        m_gcSession->AddToGcList();
    }

    inline void GcRemoveSession()
    {
        if (m_isLightSession) {
            return;
        }
        m_gcSession->GcCleanAll();
        m_gcSession->RemoveFromGcList(m_gcSession);
        m_gcSession->~GcManager();
        MemFree(m_gcSession, m_global);
        m_gcSession = nullptr;
    }
    void RedoWriteAction(bool isCommit);

    inline GcManager* GetGcSession()
    {
        return m_gcSession;
    }

    inline bool GetSnapshotStatus() const
    {
        return m_isSnapshotTaken;
    }

    inline void SetSnapshotStatus(bool status)
    {
        m_isSnapshotTaken = status;
    }

    inline void FinishStatement()
    {
        if (GetIsolationLevel() == READ_COMMITED) {
            SetVisibleCSN(0);
            m_isSnapshotTaken = false;
            // For read-only transactions allow GC to reclaim data between statements
            if (IsReadOnlyTxn()) {
                GcEndStatement();
            }
        }
        IncStmtCount();
    }

    RC SetSnapshot();
    /**
     * @brief Sets or clears the validate-no-wait flag in OccTransactionManager.
     * @detail Determines whether to call Access::lock() or
     * Access::try_lock() on each access item in the write set.
     * @param b The new validate-no-wait flag state.
     */
    void SetValidationNoWait(bool b)
    {
        m_occManager.SetValidationNoWait(b);
    }

    bool IsUpdatedInCurrStmt();

    Key* GetLocalKey() const
    {
        return m_key;
    }

    void SetReservedChunks(size_t chunks)
    {
        m_reservedChunks += chunks;
    }

    void DecReservedChunks(size_t chunks)
    {
        if (m_reservedChunks > chunks) {
            (void)MemBufferUnreserveGlobal(chunks);
            m_reservedChunks -= chunks;
        } else {
            (void)MemBufferUnreserveGlobal(0);
            m_reservedChunks = 0;
        }
    }

    void ClearReservedChunks()
    {
        if (m_reservedChunks > 0) {
            (void)MemBufferUnreserveGlobal(0);
            m_reservedChunks = 0;
        }
    }

    inline void SetCheckpointCommitEnded(bool value)
    {
        m_checkpointCommitEnded = value;
    }

    Table* GetTableByExternalId(uint64_t id);
    Index* GetIndexByExternalId(uint64_t tableId, uint64_t indexId);
    RC CreateTxnTable(Table* table, TxnTable*& txnTable);
    Table* GetTxnTable(uint64_t tableId);
    RC CreateTable(Table* table);
    RC DropTable(Table* table);
    RC CreateIndex(Table* table, Index* index, bool isPrimary);
    RC DropIndex(Index* index);
    RC TruncateTable(Table* table);
    RC AlterTableAddColumn(Table* table, Column* newColumn);
    RC AlterTableDropColumn(Table* table, Column* col);
    RC AlterTableRenameColumn(Table* table, Column* col, char* newname);

private:
    static constexpr uint32_t SESSION_ID_BITS = 32;

    /**
     * @brief Apply all transactional DDL changes. DDL changes are not handled
     * by occ.
     */
    void ApplyDDLChanges();

    /**
     * @brief Reclaims all resources associated with the transaction and
     * prepares this object to execute another transaction.
     */
    inline void Cleanup();

    /**
     * @brief Reset redo log or allocate new one
     */
    inline void RedoCleanup();

    /**
     * @brief Internal commit
     */
    void CommitInternal();

    void RollbackInternal(bool isPrepared);

    RC TruncateIndexes(TxnTable* txnTable);

    void ReclaimAccessSecondaryDelKey(Access* ac);

    // Disable class level new operator
    /** @cond EXCLUDE_DOC */
    void* operator new(std::size_t size) = delete;
    void* operator new(std::size_t size, const std::nothrow_t& tag) = delete;
    /** @endcond */

    // allow privileged access
    friend TxnInsertAction;
    friend TxnAccess;
    friend class MOTContext;
    friend void benchmarkLoggerTask(LoggerTask*, TxnManager*);
    friend class CheckpointManager;
    friend class AsyncRedoLogHandler;
    friend class RedoLog;
    friend class OccTransactionManager;
    friend class SessionContext;

    /** @var The latest epoch seen. */
    uint64_t m_visibleCSN = 0;

    /** @var The logical identifier of the thread executing this transaction. */
    uint64_t m_threadId;

    /** @var The logical identifier of the connection executing this transaction. */
    uint64_t m_connectionId;

    /** @var Owning session context. */
    SessionContext* m_sessionContext;

    /** @var The redo log. */
    RedoLog m_redoLog;

    /** @var Concurrency control. */
    OccTransactionManager m_occManager;

    GcManager* m_gcSession;

    /** @var Checkpoint phase captured during transaction start. */
    volatile CheckpointPhase m_checkpointPhase;

    /** @var Checkpoint not available capture during being transaction. */
    volatile bool m_checkpointNABit;

    /** @var Indicates if the checkpoint EndCommit was called. */
    volatile bool m_checkpointCommitEnded;

    /** @var CSN taken at the commit stage. */
    uint64_t m_csn;

    /** @var transaction_id Provided by envelop on start transaction. */
    uint64_t m_transactionId;

    /** @var Replay LSN for this transaction, used only during replay in standby. */
    uint64_t m_replayLsn;

    /** @var surrogate_counter Promotes every insert transaction. */
    SurrogateKeyGenerator m_surrogateGen;

    bool m_flushDone;

    /** @var Unique Internal transaction id (Used only for redo and recovery). */
    uint64_t m_internalTransactionId;

    uint32_t m_internalStmtCount;

    /**
     * @brief Transaction state.
     * This state only reflects the envelope states to avoid invalid transitions.
     * Transaction state machine is managed by the envelope!
     */
    TxnState m_state;

    ISOLATION_LEVEL m_isolationLevel;

    bool m_isSnapshotTaken = false;

    bool m_isTxnAborted;

    bool m_hasCommitedSubTxnDDL;

    /**
     * @var Indicates whether this is a cross engine (write) transaction which has modifications in disk engine also.
     * Attention: This is set only during XACT_EVENT_COMMIT callback so that this information is written in the redo
     * and used in the recovery. So this should not be used/queried before XACT_EVENT_COMMIT phase.
     * Used only by RedoLogWriter to mark the transaction as a cross engine transaction. Recovery needs to know if
     * the transaction is MOT only or Cross Engine transaction.
     */
    bool m_isCrossEngineTxn;

    bool m_isRecoveryTxn = false;

    bool m_hasIxColUpd = false;

public:
    /** @var Transaction cache (OCC optimization). */
    TxnAccess* m_accessMgr;

    TxnDDLAccess* m_txnDdlAccess;

    SubTxnMgr m_subTxnManager;

    MOT::Key* m_key;

    bool m_isLightSession;

    /** @var In case of unique violation this will be set to a violating index */
    MOT::Index* m_errIx;

    /** @var In case of error this will contain the exact error code */
    RC m_err;

    char m_errMsgBuf[1024];

    /** @var holds query states from MOTAdaptor */
    std::unordered_map<uint64_t, uint64_t> m_queryState;

    size_t m_reservedChunks;

    // use global or local memory allocators
    bool m_global;

    MOT::DummyIndex* m_dummyIndex;
};

class TxnIxColUpdate {
public:
    MOT::Index* m_ix[MAX_NUM_INDEXES] = {nullptr};
    MOT::Key* m_oldKeys[MAX_NUM_INDEXES] = {nullptr};
    MOT::Key* m_newKeys[MAX_NUM_INDEXES] = {nullptr};
    TxnManager* m_txn = nullptr;
    Table* m_tab = nullptr;
    uint8_t* m_modBitmap;
    uint16_t m_arrLen = 0;
    uint64_t m_cols = 0;
    UpdateIndexColumnType m_hasIxColUpd = UpdateIndexColumnType::UPDATE_COLUMN_NONE;

    TxnIxColUpdate(Table* tab, TxnManager* txn, uint8_t* modBitmap, UpdateIndexColumnType hasIxColUpd);
    ~TxnIxColUpdate();

    RC InitAndBuildOldKeys(Row* row);
    RC FilterColumnUpdate(Row* row);
};
}  // namespace MOT

#endif  // MOT_TXN_H
