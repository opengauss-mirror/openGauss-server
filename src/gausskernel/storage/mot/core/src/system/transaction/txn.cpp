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
 * txn.cpp
 *    Transaction manager used to manage the life cycle of a single transaction.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/src/system/transaction/txn.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <stdlib.h>
#include <algorithm>
#include <unordered_map>

#include "../storage/table.h"  // explicit path in order to solve collision with B header file with the same name
#include "mot_engine.h"
#include "redo_log_writer.h"
#include "sentinel.h"
#include "txn.h"
#include "txn_access.h"
#include "txn_insert_action.h"
#include "db_session_statistics.h"
#include "utilities.h"
#include "mm_api.h"

namespace MOT {
DECLARE_LOGGER(TxnManager, System);

void TxnManager::RemoveTableFromStat(Table* t)
{
    m_accessMgr->RemoveTableFromStat(t);
}

void TxnManager::UpdateRow(Row* row, const int attr_id, double attr_value)
{
    row->SetValue(attr_id, attr_value);
    UpdateLastRowState(AccessType::WR);
}

void TxnManager::UpdateRow(Row* row, const int attr_id, uint64_t attr_value)
{
    row->SetValue(attr_id, attr_value);
    UpdateLastRowState(AccessType::WR);
}

InsItem* TxnManager::GetNextInsertItem(Index* index)
{
    return m_accessMgr->GetInsertMgr()->GetInsertItem(index);
}

Key* TxnManager::GetTxnKey(Index* index)
{
    int size = index->GetAlignedKeyLength() + sizeof(Key);
    void* buf = MemSessionAlloc(size);
    if (buf == nullptr) {
        return nullptr;
    }
    return new (buf) Key(index->GetAlignedKeyLength());
}

RC TxnManager::InsertRow(Row* row)
{

    GcSessionStart();
    RC result = m_accessMgr->GetInsertMgr()->ExecuteOptimisticInsert(row);
    if (result == RC_OK) {
        MOT::DbSessionStatisticsProvider::GetInstance().AddInsertRow();
    }
    return result;
}

Row* TxnManager::RowLookup(const AccessType type, Sentinel* const& originalSentinel, RC& rc)
{
    rc = RC_OK;
    // Look for the Sentinel in the cache
    Row* local_row = nullptr;
    // error handling
    if (unlikely(originalSentinel == nullptr)) {
        return nullptr;
    }
    // if txn not started, tag as started and take global epoch
    GcSessionStart();

    RC res = AccessLookup(type, originalSentinel, local_row);

    switch (res) {
        case RC::RC_LOCAL_ROW_DELETED:
            return nullptr;
        case RC::RC_LOCAL_ROW_FOUND:
            return local_row;
        case RC::RC_LOCAL_ROW_NOT_FOUND:
            if (likely(originalSentinel->IsCommited() == true)) {
                // For Read-Only Txn return the Commited row
                if (GetTxnIsoLevel() == READ_COMMITED and type == AccessType::RD) {
                    return m_accessMgr->GetReadCommitedRow(originalSentinel);
                } else {
                    // Row is not in the cache,map it and return the local row
                    return m_accessMgr->MapRowtoLocalTable(AccessType::RD, originalSentinel, rc);
                }
            } else
                return nullptr;
        case RC::RC_MEMORY_ALLOCATION_ERROR:
            rc = RC_MEMORY_ALLOCATION_ERROR;
            return nullptr;
        default:
            return nullptr;
    }
}

RC TxnManager::AccessLookup(const AccessType type, Sentinel* const& originalSentinel, Row*& localRow)
{
    return m_accessMgr->AccessLookup(type, originalSentinel, localRow);
}

RC TxnManager::DeleteLastRow()
{
    RC rc;
    Access* access = m_accessMgr->GetLastAccess();
    if (access == nullptr)
        return RC_ERROR;

    rc = m_accessMgr->UpdateRowState(AccessType::DEL, access);
    if (rc != RC_OK)
        return rc;
    return rc;
}

RC TxnManager::UpdateLastRowState(AccessType state)
{
    return m_accessMgr->UpdateRowState(state, m_accessMgr->GetLastAccess());
}

RC TxnManager::StartTransaction(uint64_t transactionId, int isolationLevel)
{
    m_transactionId = transactionId;
    m_isolationLevel = isolationLevel;
    m_state = TxnState::TXN_START;
    GcSessionStart();
    return RC_OK;
}

RC TxnManager::LiteRollback(TransactionId transactionId)
{
    if (m_txnDdlAccess->Size() > 0) {
        if (transactionId != INVALID_TRANSACTIOIN_ID)
            m_transactionId = transactionId;

        RollbackDDLs();
        Cleanup();
    }
    MOT::DbSessionStatisticsProvider::GetInstance().AddRollbackTxn();
    return RC::RC_ABORT;
}

RC TxnManager::LiteRollbackPrepared(TransactionId transactionId)
{
    if (m_txnDdlAccess->Size() > 0) {
        if (transactionId != INVALID_TRANSACTIOIN_ID)
            m_transactionId = transactionId;

        m_redoLog.RollbackPrepared();
        RollbackDDLs();
        Cleanup();
    }
    MOT::DbSessionStatisticsProvider::GetInstance().AddRollbackPreparedTxn();
    return RC::RC_ABORT;
}

RC TxnManager::RollbackInternal(bool isPrepared)
{
    if (isPrepared) {
        m_occManager.ReleaseHeaders(this);
        m_redoLog.RollbackPrepared();
    } else {
        m_occManager.ReleaseLocks(this);
    }
    // We have to undo changes to secondary indexes and ddls
    m_occManager.RollbackInserts(this);
    RollbackDDLs();
    Cleanup();
    if (isPrepared)
        MOT::DbSessionStatisticsProvider::GetInstance().AddRollbackPreparedTxn();
    else
        MOT::DbSessionStatisticsProvider::GetInstance().AddRollbackTxn();
    return RC::RC_ABORT;
}

void TxnManager::CleanTxn()
{
    Cleanup();
}

RC TxnManager::Prepare(TransactionId transactionId)
{
    if (transactionId != INVALID_TRANSACTIOIN_ID)
        m_transactionId = transactionId;
    // Run only first validation phase
    RC rc = m_occManager.ValidateOcc(this);
    if (rc == RC_OK)
        m_redoLog.Prepare();
    return rc;
}

RC TxnManager::LitePrepare(TransactionId transactionId)
{
    if (m_txnDdlAccess->Size() == 0)
        return RC_OK;

    if (transactionId != INVALID_TRANSACTIOIN_ID)
        m_transactionId = transactionId;

    m_redoLog.Prepare();
    return RC_OK;
}

RC TxnManager::CommitInternal()
{
    SetCommitSequenceNumber(GetCSNManager().GetNextCSN());
    // Record the start write phase for this transaction
    if (GetGlobalConfiguration().m_enableCheckpoint) {
        GetCheckpointManager()->BeginTransaction(this);
    }
    // first write to redo log, then write changes
    m_redoLog.Commit();
    WriteDDLChanges();
    if (!m_occManager.WriteChanges(this))
        return RC_PANIC;
    if (GetGlobalConfiguration().m_enableCheckpoint) {
        GetCheckpointManager()->TransactionCompleted(this);
    }
    if (!GetGlobalConfiguration().m_enableRedoLog ||
        GetGlobalConfiguration().m_redoLogHandlerType == RedoLogHandlerType::ASYNC_REDO_LOG_HANDLER) {
        m_occManager.ReleaseLocks(this);
        m_occManager.CleanRowsFromIndexes(this);
    }
    return RC_OK;
}

RC TxnManager::Commit()
{
    return Commit(INVALID_TRANSACTIOIN_ID);
}

RC TxnManager::Commit(uint64_t transcationId)
{
    // Validate concurrency control
    if (transcationId != INVALID_TRANSACTIOIN_ID)
        m_transactionId = transcationId;
    RC rc = m_occManager.ValidateOcc(this);
    if (rc == RC_OK) {
        rc = CommitInternal();
        MOT::DbSessionStatisticsProvider::GetInstance().AddCommitTxn();
    }
    return rc;
}

RC TxnManager::LiteCommit(uint64_t transcationId)
{
    if (m_txnDdlAccess->Size() > 0) {
        if (transcationId != INVALID_TRANSACTIOIN_ID)
            m_transactionId = transcationId;

        SetCommitSequenceNumber(GetCSNManager().GetNextCSN());

        // first write to redo log, then write changes
        m_redoLog.Commit();
        WriteDDLChanges();
        Cleanup();
    }
    MOT::DbSessionStatisticsProvider::GetInstance().AddCommitTxn();
    return RC_OK;
}

RC TxnManager::CommitPrepared()
{
    return CommitPrepared(INVALID_TRANSACTIOIN_ID);
}

RC TxnManager::CommitPrepared(uint64_t transactionId)
{
    if (transactionId != INVALID_TRANSACTIOIN_ID)
        m_transactionId = transactionId;

    SetCommitSequenceNumber(GetCSNManager().GetNextCSN());
    // Record the start write phase for this transaction
    if (GetGlobalConfiguration().m_enableCheckpoint) {
        GetCheckpointManager()->BeginTransaction(this);
    }
    // first write to redo log, then write changes
    m_redoLog.CommitPrepared();

    // Run second validation phase
    WriteDDLChanges();
    if (!m_occManager.WriteChanges(this))
        return RC_PANIC;
    GetCheckpointManager()->TransactionCompleted(this);
    if (!GetGlobalConfiguration().m_enableRedoLog ||
        GetGlobalConfiguration().m_redoLogHandlerType == RedoLogHandlerType::ASYNC_REDO_LOG_HANDLER) {
        m_occManager.ReleaseLocks(this);
        m_occManager.CleanRowsFromIndexes(this);
    }
    MOT::DbSessionStatisticsProvider::GetInstance().AddCommitPreparedTxn();
    return RC_OK;
}

RC TxnManager::LiteCommitPrepared(uint64_t transactionId)
{
    if (m_txnDdlAccess->Size() > 0) {
        if (transactionId != INVALID_TRANSACTIOIN_ID)
            m_transactionId = transactionId;

        // first write to redo log, then write changes
        m_redoLog.CommitPrepared();
        WriteDDLChanges();
        Cleanup();
    }
    MOT::DbSessionStatisticsProvider::GetInstance().AddCommitPreparedTxn();
    return RC_OK;
}

RC TxnManager::EndTransaction()
{
    if (GetGlobalConfiguration().m_enableRedoLog &&
        GetGlobalConfiguration().m_redoLogHandlerType != RedoLogHandlerType::ASYNC_REDO_LOG_HANDLER &&
        IsFailedCommitPrepared() == false) {
        m_occManager.ReleaseLocks(this);
        m_occManager.CleanRowsFromIndexes(this);
    }
    Cleanup();
    return RC::RC_OK;
}

void TxnManager::RedoWriteAction(bool isCommit)
{
    m_redoLog.SetForceWrite();
    if (isCommit)
        m_redoLog.Commit();
    else
        m_redoLog.Rollback();
}

RC TxnManager::FailedCommitPrepared(uint64_t transcationId)
{
    if (m_isLightSession && m_txnDdlAccess->Size() == 0)
        return RC_OK;
    uint64_t used_tid = m_transactionId;
    if (transcationId != INVALID_TRANSACTIOIN_ID)
        used_tid = transcationId;

    SetCommitSequenceNumber(GetCSNManager().GetNextCSN());

    if (GetGlobalConfiguration().m_enableCheckpoint)
        GetCheckpointManager()->BeginTransaction(this);

    if (m_isLightSession == false) {
        // Row already Locked!
        if (!m_occManager.WriteChanges(this))
            return RC_PANIC;
    }

    if (GetGlobalConfiguration().m_enableCheckpoint)
        GetCheckpointManager()->TransactionCompleted(this);

    if (SavePreparedData() != RC_OK)
        return RC_ERROR;

    Cleanup();
    return RC_OK;
}

void TxnManager::Cleanup()
{
    if (m_isLightSession == false) {
        m_accessMgr->ClearSet();
    }
    m_txnDdlAccess->Reset();
    m_checkpointPhase = CheckpointPhase::NONE;
    m_csn = 0;
    m_occManager.CleanUp();
    m_err = RC_OK;
    m_errIx = nullptr;
    m_flushDone = false;
    m_internalTransactionId++;
    m_internalStmtCount = 0;
    m_redoLog.Reset();
    SetFailedCommitPrepared(false);
    GcSessionEnd();
    ClearErrorStack();
    m_accessMgr->ClearTableCache();
    m_queryState.clear();
}

void TxnManager::UndoInserts()
{
    uint32_t rollbackCounter = 0;
    TxnOrderedSet_t& OrderedSet = m_accessMgr->GetOrderedRowSet();
    for (const auto& ra_pair : OrderedSet) {
        Access* ac = ra_pair.second;
        if (ac->m_type != AccessType::INS) {
            continue;
        } else {
            rollbackCounter++;
            RollbackInsert(ac);
            m_accessMgr->IncreaseTableStat(ac->GetTxnRow()->GetTable());
        }
    }

    uint32_t counter = rollbackCounter;

    // Release local rows!
    for (const auto& ra_pair : OrderedSet) {
        Access* ac = ra_pair.second;
        if (ac->m_type == AccessType::INS) {
            Index* index_ = ac->GetSentinel()->GetIndex();
            // Row is local and was not inserted in the commit
            if (index_->GetIndexOrder() == IndexOrder::INDEX_ORDER_PRIMARY) {
                // Release local row to the GC!!!!!
                ac->GetTxnRow()->GetTable()->DestroyRow(ac->GetTxnRow());
            }
            rollbackCounter--;
        }
        if (!rollbackCounter) {
            break;
        }
    }
}

RC TxnManager::RollbackInsert(Access* ac)
{
    Sentinel* outputSen = nullptr;
    RC rc;
    Sentinel* sentinel = ac->GetSentinel();
    Index* index_ = sentinel->GetIndex();

    MOT_ASSERT(sentinel != nullptr);
    rc = sentinel->RefCountUpdate(DEC, GetThdId());
    MOT_ASSERT(rc != RC::RC_INDEX_RETRY_INSERT);
    if (rc == RC::RC_INDEX_DELETE) {
        MaxKey m_key;
        // Memory reclamation need to release the key from the primary sentinel back to the pool
        m_key.InitKey(index_->GetKeyLength());
        index_->BuildKey(ac->GetTxnRow()->GetTable(), ac->GetTxnRow(), &m_key);
        MOT_ASSERT(sentinel->GetCounter() == 0);
#ifdef MOT_DEBUG
        Sentinel* curr_sentinel = index_->IndexReadHeader(&m_key, GetThdId());
        MOT_ASSERT(curr_sentinel == sentinel);
#endif
        outputSen = index_->IndexRemove(&m_key, GetThdId());
        MOT_ASSERT(outputSen != nullptr);
        GcSessionRecordRcu(index_->GetIndexId(), outputSen, nullptr, index_->SentinelDtor, SENTINEL_SIZE);
    }
    return rc;
}

void TxnManager::RollbackSecondaryIndexInsert(Index* index)
{
    if (m_isLightSession)
        return;

    TxnOrderedSet_t& access_row_set = m_accessMgr->GetOrderedRowSet();
    TxnOrderedSet_t::iterator it = access_row_set.begin();
    while (it != access_row_set.end()) {
        Access* ac = it->second;
        if (ac->m_type == INS && ac->GetSentinel()->GetIndex() == index) {
            RollbackInsert(ac);
            it = access_row_set.erase(it);
            // need to perform index clean-up!
            m_accessMgr->PubReleaseAccess(ac);
        } else {
            it++;
        }
    }
}

void TxnManager::RollbackDDLs()
{
    // early exit
    if (m_txnDdlAccess->Size() == 0)
        return;

    // rollback DDLs in reverse order (avoid rolling back parent object before rolling back child)
    for (int i = m_txnDdlAccess->Size() - 1; i >= 0; i--) {
        Index* index = nullptr;
        Index** indexes = nullptr;
        Table* table = nullptr;
        TxnDDLAccess::DDLAccess* ddl_access = m_txnDdlAccess->Get(i);
        switch (ddl_access->GetDDLAccessType()) {
            case DDL_ACCESS_CREATE_TABLE:
                table = (Table*)ddl_access->GetEntry();
                MOT_LOG_INFO("Rollback of create table %s", table->GetLongTableName().c_str());
                table->DropImpl();
                RemoveTableFromStat(table);
                if (table != nullptr)
                    delete table;
                break;
            case DDL_ACCESS_DROP_TABLE:
                table = (Table*)ddl_access->GetEntry();
                MOT_LOG_INFO("Rollback of drop table %s", table->GetLongTableName().c_str());
                break;
            case DDL_ACCESS_TRUNCATE_TABLE:
                indexes = (Index**)ddl_access->GetEntry();
                table = indexes[0]->GetTable();
                MOT_LOG_INFO("Rollback of truncate table %s", table->GetLongTableName().c_str());
                for (int idx = 0; idx < table->GetNumIndexes(); idx++) {
                    index = table->m_indexes[idx];
                    table->m_indexes[idx] = indexes[idx];
                    if (idx == 0)
                        table->m_primaryIndex = indexes[idx];
                    else
                        table->m_secondaryIndexes[indexes[idx]->GetName()] = indexes[idx];
                    GcManager::ClearIndexElements(index->GetIndexId());
                    index->Truncate(true);
                    delete index;
                }
                delete[] indexes;
                break;
            case DDL_ACCESS_CREATE_INDEX:
                index = (Index*)ddl_access->GetEntry();
                table = index->GetTable();
                MOT_LOG_INFO("Rollback of create index %s for table %s",
                    index->GetName().c_str(),
                    table->GetLongTableName().c_str());
                table->RemoveSecondaryIndex((char*)index->GetName().c_str(), this);
                break;
            case DDL_ACCESS_DROP_INDEX:
                index = (Index*)ddl_access->GetEntry();
                table = index->GetTable();
                MOT_LOG_INFO("Rollback of drop index %s for table %s",
                    index->GetName().c_str(),
                    table->GetLongTableName().c_str());
                break;
            default:
                break;
        }
    }
}

void TxnManager::WriteDDLChanges()
{
    // early exit
    if (m_txnDdlAccess->Size() == 0)
        return;

    Index* index = nullptr;
    Index** indexes = nullptr;
    Table* table = nullptr;
    for (uint16_t i = 0; i < m_txnDdlAccess->Size(); i++) {
        TxnDDLAccess::DDLAccess* ddl_access = m_txnDdlAccess->Get(i);
        switch (ddl_access->GetDDLAccessType()) {
            case DDL_ACCESS_CREATE_TABLE:
                GetTableManager()->AddTable((Table*)ddl_access->GetEntry());
                break;
            case DDL_ACCESS_DROP_TABLE:
                GetTableManager()->DropTable((Table*)ddl_access->GetEntry(), m_sessionContext);
                break;
            case DDL_ACCESS_TRUNCATE_TABLE:
                indexes = (Index**)ddl_access->GetEntry();
                table = indexes[0]->GetTable();
                table->WrLock();
                table->m_rowCount = 0;
                for (int i = 0; i < table->GetNumIndexes(); i++) {
                    index = indexes[i];
                    GcManager::ClearIndexElements(index->GetIndexId());
                    index->Truncate(true);
                    delete index;
                }
                table->Unlock();
                delete[] indexes;
                break;
            case DDL_ACCESS_CREATE_INDEX:
                ((Index*)ddl_access->GetEntry())->SetIsCommited(true);
                break;
            case DDL_ACCESS_DROP_INDEX:
                index = (Index*)ddl_access->GetEntry();
                if (index->IsPrimaryKey())
                    break;
                table = index->GetTable();
                table->WrLock();
                table->RemoveSecondaryIndex((char*)index->GetName().c_str(), this);
                table->Unlock();
                break;
            default:
                break;
        }
    }
}

Row* TxnManager::RemoveRow(Row* row)
{
    Table* table = row->GetTable();

    Row* outputRow = nullptr;
    if (row->GetStable() == nullptr) {
        outputRow = table->RemoveRow(row, m_threadId, GetGcSession());
        m_accessMgr->IncreaseTableStat(table);
    } else {
        outputRow = row;
    }

    return outputRow;
}

Row* TxnManager::RemoveKeyFromIndex(Row* row, Sentinel* sentinel)
{
    Table* table = row->GetTable();

    Row* outputRow = nullptr;
    if (row->GetStable() == nullptr) {
        outputRow = table->RemoveKeyFromIndex(row, sentinel, m_threadId, GetGcSession());
    } else {
        outputRow = row;
    }

    return outputRow;
}

RC TxnManager::RowDel()
{
    return UpdateLastRowState(AccessType::DEL);
}

// Use this function when we have only the key!
// Not Used with FDW!
Row* TxnManager::RowLookupByKey(Table* const& table, const AccessType type, Key* const currentKey)
{
    RC rc = RC_OK;
    Row* originalRow = nullptr;
    Sentinel* pSentinel = nullptr;
    table->FindRow(currentKey, pSentinel, GetThdId());
    if (pSentinel == nullptr) {
        MOT_LOG_DEBUG("Cannot find key:%" PRIu64 " from table:%s", m_key, table->GetLongTableName().c_str());
        return nullptr;
    } else {
        return RowLookup(type, pSentinel, rc);
    }
}

TxnManager::TxnManager(SessionContext* session_context)
    : m_latestEpoch(~uint64_t(0)),
      m_threadId((uint64_t)-1),
      m_connectionId((uint64_t)-1),
      m_sessionContext(session_context),
      m_redoLog(this),
      m_occManager(),
      m_gcSession(nullptr),
      m_checkpointPhase(CheckpointPhase::NONE),
      m_checkpointNABit(false),
      m_csn(0),
      m_transactionId(INVALID_TRANSACTIOIN_ID),
      m_surrogateGen(0),
      m_flushDone(false),
      m_internalTransactionId(((uint64_t)m_sessionContext->GetSessionId()) << SESSION_ID_BITS),
      m_internalStmtCount(0),
      m_isolationLevel(READ_COMMITED),
      m_failedCommitPrepared(false),
      m_isLightSession(false),
      m_errIx(nullptr),
      m_err(RC_OK)
{
    m_key = nullptr;
    m_state = TxnState::TXN_START;
}

TxnManager::~TxnManager()
{
    if (GetGlobalConfiguration().m_enableCheckpoint) {
        GetCheckpointManager()->AbortTransaction(this);
    }

    if (m_state == MOT::TxnState::TXN_PREPARE) {
        if (SavePreparedData() != RC_OK) {
            MOT_LOG_ERROR("savePreparedData failed");
        }
        Cleanup();
    }

    MOT_LOG_DEBUG("txn_man::~txn_man - memory pools released for thread_id=%lu", m_threadId);

    if (m_gcSession != nullptr) {
        m_gcSession->GcCleanAll();
        m_gcSession->RemoveFromGcList(m_gcSession);
        m_gcSession->~GcManager();
        MemSessionFree(m_gcSession);
    }
    delete m_key;
    delete m_txnDdlAccess;
}

bool TxnManager::Init(uint64_t _thread_id, uint64_t connection_id, bool isLightTxn)
{
    this->m_threadId = _thread_id;
    this->m_connectionId = connection_id;
    m_isLightSession = isLightTxn;

    if (!m_occManager.Init())
        return false;

    m_txnDdlAccess = new (std::nothrow) TxnDDLAccess(this);
    if (!m_txnDdlAccess) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Initialize Transaction", "Failed to allocate memory for DDL access data");
        return false;
    }
    m_txnDdlAccess->Init();

    // make node-local allocations
    if (isLightTxn == false) {
        m_accessMgr = MemSessionAllocAlignedObjectPtr<TxnAccess>(L1_CACHE_LINE);

        if (!m_accessMgr->Init(this))
            return false;

        m_surrogateGen = GetSurrogateKeyManager()->GetSurrogateSlot(connection_id);
    }

    m_gcSession = GcManager::Make(GcManager::GC_MAIN, _thread_id);
    if (!m_gcSession)
        return false;

    if (!m_redoLog.Init())
        return false;

    static const TxnValidation validation_lock = GetGlobalConfiguration().m_validationLock;
    if (m_key == nullptr) {
        MOT_LOG_DEBUG("Init Key");
        m_key = new (std::nothrow) MaxKey(MAX_KEY_SIZE);
        if (m_key == nullptr)
            MOTAbort();
    }

    m_occManager.SetPreAbort(GetGlobalConfiguration().m_preAbort);
    if (validation_lock == TxnValidation::TXN_VALIDATION_NO_WAIT)
        m_occManager.SetValidationNoWait(true);
    else if (validation_lock == TxnValidation::TXN_VALIDATION_WAITING) {
        m_occManager.SetValidationNoWait(false);
    } else {
        MOT_ASSERT(false);
    }

    return true;
}

RC TxnManager::OverwriteRow(Row* updatedRow, BitmapSet& modifiedColumns)
{
    if (updatedRow == nullptr)
        return RC_ERROR;

    Access* access = m_accessMgr->GetLastAccess();
    if (access->m_type == AccessType::WR)
        access->m_modifiedColumns |= modifiedColumns;
    return RC_OK;
}

void TxnManager::SetTxnState(TxnState envelopeState)
{
    m_state = envelopeState;
}

void TxnManager::SetTxnIsoLevel(int envelopeIsoLevel)
{
    m_isolationLevel = envelopeIsoLevel;
}

void TxnManager::GcSessionRecordRcu(
    uint32_t index_id, void* object_ptr, void* object_pool, DestroyValueCbFunc cb, uint32_t obj_size)
{
    return m_gcSession->GcRecordObject(index_id, object_ptr, object_pool, cb, obj_size);
}

TxnInsertAction::~TxnInsertAction()
{
    if (m_manager != nullptr && m_insertSet != nullptr) {
        MemSessionFree(m_insertSet);
    }
}

bool TxnInsertAction::Init(TxnManager* _manager)
{
    bool rc = true;
    if (this->m_manager)
        return false;

    this->m_manager = _manager;
    m_insertSetSize = 0;
    m_insertArraySize = INSERT_ARRAY_DEFAULT_SIZE;

    void* ptr = MemSessionAlloc(sizeof(InsItem) * m_insertArraySize);
    if (ptr == nullptr)
        return false;
    m_insertSet = reinterpret_cast<InsItem*>(ptr);
    for (uint64_t item = 0; item < m_insertArraySize; item++) {
        new (&m_insertSet[item]) InsItem();
    }
    return rc;
}

void TxnInsertAction::ReportError(RC rc, InsItem* currentItem)
{

    switch (rc) {
        case RC_OK:
            break;
        case RC_UNIQUE_VIOLATION:
            // set error
            m_manager->m_err = RC_UNIQUE_VIOLATION;
            m_manager->m_errIx = currentItem->m_index;
            m_manager->m_errIx->BuildErrorMsg(currentItem->m_row->GetTable(),
                currentItem->m_row,
                m_manager->m_errMsgBuf,
                sizeof(m_manager->m_errMsgBuf));
            break;
        case RC_MEMORY_ALLOCATION_ERROR:
            SetLastError(MOT_ERROR_OOM, MOT_SEVERITY_ERROR);
            break;
        case RC_ILLEGAL_ROW_STATE:
            SetLastError(MOT_ERROR_INTERNAL, MOT_SEVERITY_ERROR);
            break;
        default:
            SetLastError(MOT_ERROR_INTERNAL, MOT_SEVERITY_ERROR);
            break;
    }
}

RC TxnInsertAction::ExecuteOptimisticInsert(Row* row)
{
    Sentinel* pIndexInsertResult = nullptr;
    Row* accessRow = nullptr;
    RC rc = RC_OK;
    bool isInserted = true;
    bool isMappedToCache = false;

    auto currentItem = BeginCursor();

    /*
     * 1.Add all sentinels to the Access SET type P_SENTINEL or S_SENTINEL
     * 2.IF Sentinel is committed abort!
     * 3.We perform lookup directly on sentinels
     * 4.We do not attach the row to the index,we map it to the access
     * 5.We can release the row in the case of early abort only for Primary
     * 6.No need to copy the row to the local_access
     */
    while (currentItem != EndCursor()) {
        isInserted = true;
        isMappedToCache = false;
        bool res = reinterpret_cast<Index*>(currentItem->m_index)
                       ->IndexInsert(pIndexInsertResult, currentItem->m_key, m_manager->GetThdId(), rc);
        if (unlikely(rc == RC_MEMORY_ALLOCATION_ERROR)) {
            ReportError(rc);
            // Failed on Memory
            isInserted = false;
            goto end;
        } else {
            if (currentItem->getIndexOrder() == IndexOrder::INDEX_ORDER_PRIMARY) {
                row->SetAbsentRow();
                row->SetPrimarySentinel(pIndexInsertResult);
                MOT_ASSERT(row->IsAbsentRow());
            }
        }
        if (pIndexInsertResult->IsCommited() == true) {
            // Lets check and see if we deleted the row
            Row* local_row = nullptr;
            rc = m_manager->AccessLookup(RD, pIndexInsertResult, local_row);
            switch (rc) {
                case RC_LOCAL_ROW_DELETED:
                    // promote delete to Insert!
                    // In this case the sentinel is committed and the previous scenario was delete
                    // Insert succeeded Sentinel was not committed before!
                    // At this point the row is not in the cache and can be mapped!
                    MOT_ASSERT(currentItem->m_index->GetUnique() == true);
                    accessRow = m_manager->m_accessMgr->AddInsertToLocalAccess(pIndexInsertResult, row, rc, true);
                    if (accessRow != nullptr) {
                        isMappedToCache = true;
                    }
                    ReportError(rc, currentItem);
                    break;
                case RC_LOCAL_ROW_NOT_FOUND:
                    // Header is committed
                case RC_LOCAL_ROW_FOUND:
                    // Found But not deleted = self duplicated!
                    ReportError(RC_UNIQUE_VIOLATION, currentItem);
                    rc = RC_UNIQUE_VIOLATION;
                    goto end;
                default:
                    break;
            }
        } else if (res == true or pIndexInsertResult->IsCommited() == false) {
            // tag all the sentinels the insert metadata
            MOT_ASSERT(pIndexInsertResult->GetCounter() != 0);
            // Insert succeeded Sentinel was not committed before!
            accessRow = m_manager->m_accessMgr->AddInsertToLocalAccess(pIndexInsertResult, row, rc);
            if (accessRow == nullptr) {
                ReportError(rc, currentItem);
                goto end;
            }
            isMappedToCache = true;
        }
        ++currentItem;
    }

end:
    if (rc != RC_OK) {
        // Clean current aborted row and clean secondary indexes that were not inserts
        // Clean first Object! - wither primary or secondary!
        // Return Local Row to pull for PI
        Table* table = currentItem->m_row->GetTable();
        if (currentItem->getIndexOrder() == IndexOrder::INDEX_ORDER_PRIMARY) {
            table->DestroyRow(currentItem->m_row);
        }
        if (isInserted == true) {
            if (isMappedToCache == false) {
                RC rc = pIndexInsertResult->RefCountUpdate(DEC, m_manager->GetThdId());
                Index* index_ = pIndexInsertResult->GetIndex();
                if (rc == RC::RC_INDEX_DELETE) {
                    // Memory reclamation need to release the key from the primary sentinel back to the pool
                    MOT_ASSERT(pIndexInsertResult->GetCounter() == 0);
                    Sentinel* outputSen = index_->IndexRemove(currentItem->m_key, m_manager->GetThdId());
                    MOT_ASSERT(outputSen != nullptr);
                    m_manager->GcSessionRecordRcu(
                        index_->GetIndexId(), outputSen, nullptr, index_->SentinelDtor, SENTINEL_SIZE);
                    m_manager->m_accessMgr->IncreaseTableStat(table);
                }
            }
        }
    }

    // Clean keys
    currentItem = BeginCursor();
    while (currentItem < EndCursor()) {
        m_manager->DestroyTxnKey(currentItem->m_key);
        currentItem++;
    }

    // Clear the current set size;
    m_insertSetSize = 0;
    return rc;
}

bool TxnInsertAction::ReallocInsertSet()
{
    bool rc = true;
    uint64_t new_array_size = (uint64_t)m_insertArraySize * INSERT_ARRAY_EXTEND_FACTOR;
    void* ptr = MemSessionAlloc(sizeof(InsItem) * new_array_size);
    if (__builtin_expect(ptr == nullptr, 0)) {
        MOT_LOG_ERROR("%s: failed", __func__);
        MOT_ASSERT(ptr != nullptr);
        return false;
    }
    errno_t erc = memset_s(ptr, sizeof(InsItem) * new_array_size, 0, sizeof(InsItem) * new_array_size);
    securec_check(erc, "\0", "\0");
    erc = memcpy_s(ptr, sizeof(InsItem) * new_array_size, m_insertSet, sizeof(InsItem) * m_insertArraySize);
    securec_check(erc, "\0", "\0");
    MemSessionFree(m_insertSet);
    m_insertSet = reinterpret_cast<InsItem*>(ptr);
    m_insertArraySize = new_array_size;
    return rc;
}

void TxnInsertAction::ShrinkInsertSet()
{
    uint64_t new_array_size = INSERT_ARRAY_DEFAULT_SIZE;
    void* ptr = MemSessionAlloc(sizeof(InsItem) * new_array_size);
    if (__builtin_expect(ptr == nullptr, 0)) {
        MOT_LOG_ERROR("%s: failed", __func__);
        return;
    }
    errno_t erc = memcpy_s(ptr, sizeof(InsItem) * new_array_size, m_insertSet, sizeof(InsItem) * new_array_size);
    securec_check(erc, "\0", "\0");
    MemSessionFree(m_insertSet);
    m_insertSet = reinterpret_cast<InsItem*>(ptr);
    m_insertArraySize = new_array_size;
}

/******************** DDL SUPPORT ********************/
Table* TxnManager::GetTableByExternalId(uint64_t id)
{
    TxnDDLAccess::DDLAccess* ddl_access = m_txnDdlAccess->GetByOid(id);
    if (ddl_access != nullptr) {
        switch (ddl_access->GetDDLAccessType()) {
            case DDL_ACCESS_CREATE_TABLE:
                return (Table*)ddl_access->GetEntry();
            case DDL_ACCESS_DROP_TABLE:
                return nullptr;
            default:
                break;
        }
    }

    return GetTableManager()->GetTableByExternal(id);
}

Index* TxnManager::GetIndexByExternalId(uint64_t table_id, uint64_t index_id)
{
    TxnDDLAccess::DDLAccess* ddl_access = m_txnDdlAccess->GetByOid(index_id);
    if (ddl_access != nullptr) {
        switch (ddl_access->GetDDLAccessType()) {
            case DDL_ACCESS_CREATE_INDEX:
                return (Index*)ddl_access->GetEntry();
            case DDL_ACCESS_DROP_INDEX:
                return nullptr;
            default:
                break;
        }
    }

    Table* table = GetTableManager()->GetTableByExternal(table_id);
    if (table == nullptr) {
        return nullptr;
    } else {
        return table->GetIndexByExtId(index_id);
    }
}

Index* TxnManager::GetIndex(uint64_t table_id, uint16_t position)
{
    TxnDDLAccess::DDLAccess* ddl_access = m_txnDdlAccess->GetByOid(table_id);
    Table* table = GetTableByExternalId(table_id);
    return GetIndex(table, position);
}

Index* TxnManager::GetIndex(Table* table, uint16_t position)
{
    MOT_ASSERT(table != nullptr);
    Index* index = table->GetIndex(position);
    MOT_ASSERT(index != nullptr);
    TxnDDLAccess::DDLAccess* ddl_access = m_txnDdlAccess->GetByOid(index->GetExtId());
    if (ddl_access != nullptr) {
        switch (ddl_access->GetDDLAccessType()) {
            case DDL_ACCESS_CREATE_INDEX:
                return index;
            case DDL_ACCESS_DROP_INDEX:
                return nullptr;
            default:
                // should print error, the only index operation which are supported
                // are create and drop
                return nullptr;
        }
    } else {
        if (index->GetIsCommited())
            return index;
        else
            return nullptr;
    }
}

RC TxnManager::CreateTable(Table* table)
{
    TxnDDLAccess::DDLAccess* ddl_access =
        new (std::nothrow) TxnDDLAccess::DDLAccess(table->GetTableExId(), DDL_ACCESS_CREATE_TABLE, (void*)table);
    if (ddl_access == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Create Table", "Failed to allocate memory for DDL Access object");
        return RC_MEMORY_ALLOCATION_ERROR;
    }

    m_txnDdlAccess->Add(ddl_access);
    return RC_OK;
}

RC TxnManager::DropTable(Table* table)
{
    RC res = RC_OK;

    // we allocate all memory before action takes place, so that if memory allocation fails, we can report error safely
    TxnDDLAccess::DDLAccess* new_ddl_access = nullptr;
    TxnDDLAccess::DDLAccess* ddl_access = m_txnDdlAccess->GetByOid(table->GetTableExId());
    if ((ddl_access == nullptr) || (ddl_access->GetDDLAccessType() == DDL_ACCESS_TRUNCATE_TABLE)) {
        new_ddl_access =
            new (std::nothrow) TxnDDLAccess::DDLAccess(table->GetTableExId(), DDL_ACCESS_DROP_TABLE, (void*)table);
        if (new_ddl_access == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Drop Table", "Failed to allocate DDL Access object");
            return RC_MEMORY_ALLOCATION_ERROR;
        }
    }

    if (!m_isLightSession) {
        TxnOrderedSet_t& access_row_set = m_accessMgr->GetOrderedRowSet();
        TxnOrderedSet_t::iterator it = access_row_set.begin();
        while (it != access_row_set.end()) {
            Access* ac = it->second;
            if (ac->GetTxnRow()->GetTable() == table) {
                if (ac->m_type == INS)
                    RollbackInsert(ac);
                it = access_row_set.erase(it);
                // need to perform index clean-up!
                m_accessMgr->PubReleaseAccess(ac);
            } else {
                it++;
            }
        }
    }

    if (ddl_access != nullptr) {
        if (ddl_access->GetDDLAccessType() == DDL_ACCESS_CREATE_TABLE) {
            // this table was created in this transaction, can delete it from the ddl_access
            m_txnDdlAccess->EraseByOid(table->GetTableExId());
            table->DropImpl();
            RemoveTableFromStat(table);
            delete table;
        } else if (ddl_access->GetDDLAccessType() == DDL_ACCESS_TRUNCATE_TABLE) {
            Index** indexes = (Index**)ddl_access->GetEntry();
            for (int i = 0; i < table->GetNumIndexes(); i++) {
                Index* newIndex = table->m_indexes[i];
                Index* oldIndex = indexes[i];
                table->m_indexes[i] = oldIndex;
                if (i != 0)
                    table->m_secondaryIndexes[oldIndex->GetName()] = oldIndex;
                else
                    table->m_primaryIndex = oldIndex;
                // need to check if need to release memory in a different way? GC?
                // assumption is the we deleted all rows
                delete newIndex;
            }
            delete[] indexes;
            m_txnDdlAccess->EraseByOid(table->GetTableExId());
            m_txnDdlAccess->Add(new_ddl_access);
        }
    } else {
        m_txnDdlAccess->Add(new_ddl_access);
    }

    return RC_OK;
}

RC TxnManager::TruncateTable(Table* table)
{
    RC res = RC_OK;
    if (m_isLightSession)  // really?
        return res;
    TxnDDLAccess::DDLAccess* ddl_access = m_txnDdlAccess->GetByOid(table->GetTableExId());
    if (ddl_access != nullptr) {
        // must be create table or truncate table
        MOT_ASSERT(ddl_access->GetDDLAccessType() == DDL_ACCESS_CREATE_TABLE ||
                   ddl_access->GetDDLAccessType() == DDL_ACCESS_TRUNCATE_TABLE);
        // this is a table that we created or truncated before, should remove all the rows
        // belonging to this table from the access and continue
        TxnOrderedSet_t& access_row_set = m_accessMgr->GetOrderedRowSet();
        TxnOrderedSet_t::iterator it = access_row_set.begin();
        while (it != access_row_set.end()) {
            Access* ac = it->second;
            if (ac->GetTxnRow()->GetTable() == table) {
                if (ac->m_type == INS)
                    RollbackInsert(ac);
                it = access_row_set.erase(it);
                // need to perform index clean-up!
                m_accessMgr->PubReleaseAccess(ac);
            } else {
                it++;
            }
        }
    } else {
        Index** indexes = nullptr;
        indexes = new (std::nothrow) Index*[MAX_NUM_INDEXES];
        if (indexes == nullptr) {
            // print error, clould not allocate memory
            MOT_REPORT_ERROR(MOT_ERROR_OOM,
                "Truncate Table",
                "Failed to allocate memory for %u index objects",
                (unsigned)MAX_NUM_INDEXES);
            return RC_MEMORY_ALLOCATION_ERROR;
        }
        // allocate DDL before work and fail immediately if required
        ddl_access = new (std::nothrow)
            TxnDDLAccess::DDLAccess(table->GetTableExId(), DDL_ACCESS_TRUNCATE_TABLE, (void*)indexes);
        if (ddl_access == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Truncate Table", "Failed to allocate memory for DDL Access object");
            delete[] indexes;
            return RC_MEMORY_ALLOCATION_ERROR;
        }

        for (int i = 0; i < table->GetNumIndexes(); i++) {
            Index* index_copy = table->GetIndex(i)->CloneEmpty();
            if (index_copy == nullptr) {
                // print error, clould not allocate memory for index
                MOT_REPORT_ERROR(MOT_ERROR_OOM,
                    "Truncate Table",
                    "Failed to clone empty index %s",
                    table->GetIndex(i)->GetName().c_str());
                for (int j = 0; j < i; j++) {
                    // cleanup of previous created indexes copy
                    Index* newIndex = table->m_indexes[j];
                    Index* oldIndex = indexes[j];
                    table->m_indexes[j] = oldIndex;
                    if (j != 0)  // is secondary
                        table->m_secondaryIndexes[oldIndex->GetName()] = oldIndex;
                    else  // is primary
                        table->m_primaryIndex = oldIndex;
                    delete newIndex;
                }
                delete ddl_access;
                return RC_MEMORY_ALLOCATION_ERROR;
            }
            indexes[i] = table->GetIndex(i);
            table->m_indexes[i] = index_copy;
            if (i != 0)  // is secondary
                table->m_secondaryIndexes[index_copy->GetName()] = index_copy;
            else  // is primary
                table->m_primaryIndex = index_copy;
        }
        m_txnDdlAccess->Add(ddl_access);
    }

    return res;
}

RC TxnManager::CreateIndex(Table* table, Index* index, bool is_primary)
{

    // allocate DDL before work and fail immediately if required
    TxnDDLAccess::DDLAccess* ddl_access =
        new (std::nothrow) TxnDDLAccess::DDLAccess(index->GetExtId(), DDL_ACCESS_CREATE_INDEX, (void*)index);
    if (ddl_access == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Create Index", "Failed to allocate DDL Access object");
        return RC_MEMORY_ALLOCATION_ERROR;
    }

    if (is_primary) {
        table->UpdatePrimaryIndex((MOT::Index*)index);
    } else {
        // currently we are still adding the index to the table although
        // is should only be added on successful commit. Assuming that if
        // a client did a create index, all other clients are waiting on a lock
        // until the changes are either commited or aborted
        table->WrLock();  // for concurrent access
        if (table->GetNumIndexes() == MAX_NUM_INDEXES) {
            table->Unlock();
            MOT_REPORT_ERROR(MOT_ERROR_RESOURCE_LIMIT,
                "Create Index",
                "Cannot create index in table %s: reached limit of %u indices per table",
                table->GetLongTableName().c_str(),
                (unsigned)MAX_NUM_INDEXES);
            delete ddl_access;
            return RC_TABLE_EXCEEDS_MAX_INDEXES;
        }

        if (!table->AddSecondaryIndex(index->GetName(), index, this, m_threadId)) {
            table->Unlock();
            if (MOT_IS_OOM()) {  // do not report error in "unique violation" scenario
                MOT_REPORT_ERROR(
                    MOT_ERROR_INTERNAL, "Create Index", "Failed to add secondary index %s", index->GetName().c_str());
            }
            delete ddl_access;
            return m_err;
        }
        table->Unlock();
    }

    m_txnDdlAccess->Add(ddl_access);
    return RC_OK;
}

RC TxnManager::DropIndex(Index* index)
{
    // allocate DDL before work and fail immediately if required
    TxnDDLAccess::DDLAccess* new_ddl_access = nullptr;
    TxnDDLAccess::DDLAccess* ddl_access = m_txnDdlAccess->GetByOid(index->GetExtId());
    if (ddl_access == nullptr) {
        new_ddl_access =
            new (std::nothrow) TxnDDLAccess::DDLAccess(index->GetExtId(), DDL_ACCESS_DROP_INDEX, (void*)index);
        if (new_ddl_access == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Drop Index", "Failed to allocate DDL Access object");
            return RC_MEMORY_ALLOCATION_ERROR;
        }
    }

    RC res = RC_OK;
    Table* table = index->GetTable();

    RollbackSecondaryIndexInsert(index);

    if (ddl_access != nullptr) {
        // this index was created in this transaction, can delete it from the ddl_access
        // table->removeSecondaryIndex also performs releases the object
        m_txnDdlAccess->EraseByOid(index->GetExtId());
        res = table->RemoveSecondaryIndex((char*)index->GetName().c_str(), this);
        if (res != RC_OK) {
            // print Error
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL, "Drop Index", "Failed to remove secondary index");
            return res;
        }
    } else {
        m_txnDdlAccess->Add(new_ddl_access);
    }

    return res;
}

RC TxnManager::SavePreparedData()
{
    m_redoLog.Reset();
    SetFailedCommitPrepared(true);

    if (m_redoLog.PrepareToInProcessTxns() != RC_OK) {
        MOT_LOG_ERROR("PrepareToInProcessTxns failed [%lu]", m_transactionId);
        return RC_ERROR;
    }

    if (MOT::GetRecoveryManager()->ApplyInProcessTransaction(m_internalTransactionId) != RC_OK) {
        MOT_LOG_ERROR("ApplyInProcessTransaction failed [%lu]", m_transactionId);
        return RC_ERROR;
    }

    if (m_transactionId != INVALID_TRANSACTIOIN_ID) {
        MOT_LOG_DEBUG("mapping ext txid %lu to %lu", m_transactionId, m_internalTransactionId);
        MOT::GetRecoveryManager()->UpdateTxIdMap(m_internalTransactionId, m_transactionId);
    }

    return RC_OK;
}
}  // namespace MOT
