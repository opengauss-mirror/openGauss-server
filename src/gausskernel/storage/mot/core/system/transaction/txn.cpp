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
 *    src/gausskernel/storage/mot/core/system/transaction/txn.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <stdlib.h>
#include <algorithm>
#include <unordered_map>

#include "table.h"
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

Key* TxnManager::GetTxnKey(MOT::Index* index)
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
                // For Read-Only Txn return the Committed row
                if (GetTxnIsoLevel() == READ_COMMITED and (type == RD or type == INS)) {
                    return m_accessMgr->GetReadCommitedRow(originalSentinel);
                } else {
                    // Row is not in the cache,map it and return the local row
                    AccessType rd_type = (type != RD_FOR_UPDATE) ? RD : RD_FOR_UPDATE;
                    return m_accessMgr->MapRowtoLocalTable(rd_type, originalSentinel, rc);
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
    access->m_stmtCount = GetStmtCount();
    return rc;
}

RC TxnManager::UpdateLastRowState(AccessType state)
{
    return m_accessMgr->UpdateRowState(state, m_accessMgr->GetLastAccess());
}

bool TxnManager::IsUpdatedInCurrStmt()
{
    Access* access = m_accessMgr->GetLastAccess();
    if (access == nullptr) {
        return false;
    }
    if (m_internalStmtCount == m_accessMgr->GetLastAccess()->m_stmtCount) {
        return true;
    }
    return false;
}

RC TxnManager::StartTransaction(uint64_t transactionId, int isolationLevel)
{
    m_transactionId = transactionId;
    m_isolationLevel = isolationLevel;
    m_state = TxnState::TXN_START;
    GcSessionStart();
    return RC_OK;
}

void TxnManager::LiteRollback()
{
    if (m_txnDdlAccess->Size() > 0) {
        RollbackDDLs();
        Cleanup();
    }
    MOT::DbSessionStatisticsProvider::GetInstance().AddRollbackTxn();
}

void TxnManager::LiteRollbackPrepared()
{
    if (m_txnDdlAccess->Size() > 0) {
        m_redoLog.RollbackPrepared();
        RollbackDDLs();
        Cleanup();
    }
    MOT::DbSessionStatisticsProvider::GetInstance().AddRollbackPreparedTxn();
}

void TxnManager::RollbackInternal(bool isPrepared)
{
    if (isPrepared) {
        if (GetGlobalConfiguration().m_enableCheckpoint) {
            GetCheckpointManager()->FreePreAllocStableRows(this);
        }

        m_occManager.ReleaseHeaders(this);
        m_redoLog.RollbackPrepared();
    } else {
        m_occManager.ReleaseLocks(this);
    }

    // We have to undo changes to secondary indexes and ddls
    m_occManager.RollbackInserts(this);
    RollbackDDLs();
    Cleanup();
    if (isPrepared) {
        MOT::DbSessionStatisticsProvider::GetInstance().AddRollbackPreparedTxn();
    } else {
        MOT::DbSessionStatisticsProvider::GetInstance().AddRollbackTxn();
    }
}

void TxnManager::CleanTxn()
{
    Cleanup();
}

RC TxnManager::Prepare()
{
    // Run only first validation phase
    RC rc = m_occManager.ValidateOcc(this);
    if (rc == RC_OK) {
        m_redoLog.Prepare();
    }
    return rc;
}

void TxnManager::LitePrepare()
{
    if (m_txnDdlAccess->Size() == 0) {
        return;
    }

    m_redoLog.Prepare();
}

void TxnManager::CommitInternal()
{
    if (m_csn == CSNManager::INVALID_CSN) {
        SetCommitSequenceNumber(GetCSNManager().GetNextCSN());
    }

    // first write to redo log, then write changes
    m_redoLog.Commit();
    m_occManager.WriteChanges(this);

    if (GetGlobalConfiguration().m_enableCheckpoint) {
        GetCheckpointManager()->EndCommit(this);
    }

    if (!GetGlobalConfiguration().m_enableRedoLog) {
        m_occManager.ReleaseLocks(this);
    }
}

RC TxnManager::ValidateCommit()
{
    return m_occManager.ValidateOcc(this);
}

void TxnManager::RecordCommit()
{
    CommitInternal();
    MOT::DbSessionStatisticsProvider::GetInstance().AddCommitTxn();
}

RC TxnManager::Commit()
{
    // Validate concurrency control
    RC rc = ValidateCommit();
    if (rc == RC_OK) {
        RecordCommit();
    }
    return rc;
}

void TxnManager::LiteCommit()
{
    if (m_txnDdlAccess->Size() > 0) {
        // write to redo log
        m_redoLog.Commit();
        CleanDDLChanges();
        Cleanup();
    }
    MOT::DbSessionStatisticsProvider::GetInstance().AddCommitTxn();
}

void TxnManager::CommitPrepared()
{
    if (m_csn == CSNManager::INVALID_CSN) {
        SetCommitSequenceNumber(GetCSNManager().GetNextCSN());
    }

    // first write to redo log, then write changes
    m_redoLog.CommitPrepared();
    m_occManager.WriteChanges(this);

    if (GetGlobalConfiguration().m_enableCheckpoint) {
        GetCheckpointManager()->EndCommit(this);
    }

    if (!GetGlobalConfiguration().m_enableRedoLog) {
        m_occManager.ReleaseLocks(this);
    }
    MOT::DbSessionStatisticsProvider::GetInstance().AddCommitPreparedTxn();
}

void TxnManager::LiteCommitPrepared()
{
    if (m_txnDdlAccess->Size() > 0) {
        // first write to redo log, then write changes
        m_redoLog.CommitPrepared();
        CleanDDLChanges();
        Cleanup();
    }
    MOT::DbSessionStatisticsProvider::GetInstance().AddCommitPreparedTxn();
}

void TxnManager::EndTransaction()
{
    if (GetGlobalConfiguration().m_enableRedoLog) {
        m_occManager.ReleaseLocks(this);
    }
    CleanDDLChanges();
    Cleanup();
}

void TxnManager::RedoWriteAction(bool isCommit)
{
    m_redoLog.SetForceWrite();
    if (isCommit)
        m_redoLog.Commit();
    else
        m_redoLog.Rollback();
}

void TxnManager::Cleanup()
{
    if (m_isLightSession == false) {
        m_accessMgr->ClearSet();
    }
    m_txnDdlAccess->Reset();
    m_checkpointPhase = CheckpointPhase::NONE;
    m_csn = CSNManager::INVALID_CSN;
    m_occManager.CleanUp();
    m_err = RC_OK;
    m_errIx = nullptr;
    m_flushDone = false;
    m_internalTransactionId = GetTxnIdManager().GetNextId();
    m_internalStmtCount = 0;
    m_redoLog.Reset();
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
        }

        rollbackCounter++;
        RollbackInsert(ac);
        m_accessMgr->IncreaseTableStat(ac->GetTxnRow()->GetTable());
    }

    if (rollbackCounter == 0) {
        return;
    }

    // Release local rows!
    for (const auto& ra_pair : OrderedSet) {
        Access* ac = ra_pair.second;
        if (ac->m_type == AccessType::INS) {
            MOT::Index* index_ = ac->GetSentinel()->GetIndex();
            // Row is local and was not inserted in the commit
            if (index_->GetIndexOrder() == IndexOrder::INDEX_ORDER_PRIMARY) {
                // Release local row to the GC!!!!!
                ac->GetTxnRow()->GetTable()->DestroyRow(ac->GetTxnRow());
            }
            rollbackCounter--;
            if (rollbackCounter == 0) {
                break;
            }
        }
    }
}

RC TxnManager::RollbackInsert(Access* ac)
{
    Sentinel* outputSen = nullptr;
    RC rc;
    Sentinel* sentinel = ac->GetSentinel();
    MOT::Index* index_ = sentinel->GetIndex();

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
        MOT_ASSERT(outputSen->GetCounter() == 0);

        GcSessionRecordRcu(index_->GetIndexId(), outputSen, nullptr, Index::SentinelDtor, SENTINEL_SIZE(index_));
        // If we are the owner of the key and insert on top of a deleted row,
        // lets check if we can reclaim the deleted row
        if (ac->m_params.IsUpgradeInsert() and index_->IsPrimaryKey()) {
            MOT_ASSERT(sentinel->GetData() != nullptr);
            GcSessionRecordRcu(index_->GetIndexId(),
                sentinel->GetData(),
                nullptr,
                Row::RowDtor,
                ROW_SIZE_FROM_POOL(ac->GetTxnRow()->GetTable()));
        }
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
        MOT::Index* index = nullptr;
        MOTIndexArr* indexArr = nullptr;
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
                indexArr = (MOTIndexArr*)ddl_access->GetEntry();
                table = indexArr->GetTable();
                table->WrLock();
                if (indexArr->GetNumIndexes() > 0) {
                    MOT_ASSERT(indexArr->GetNumIndexes() == table->GetNumIndexes());
                    MOT_LOG_INFO("Rollback of truncate table %s", table->GetLongTableName().c_str());
                    for (int idx = 0; idx < indexArr->GetNumIndexes(); idx++) {
                        uint16_t oldIx = indexArr->GetIndexIx(idx);
                        MOT::Index* oldIndex = indexArr->GetIndex(idx);
                        index = table->m_indexes[oldIx];
                        table->m_indexes[oldIx] = oldIndex;
                        if (idx == 0)
                            table->m_primaryIndex = oldIndex;
                        else
                            table->m_secondaryIndexes[oldIndex->GetName()] = oldIndex;
                        GcManager::ClearIndexElements(index->GetIndexId());
                        index->Truncate(true);
                        delete index;
                    }
                }
                table->ReplaceRowPool(indexArr->GetRowPool());
                table->Unlock();
                delete indexArr;
                break;
            case DDL_ACCESS_CREATE_INDEX:
                index = (Index*)ddl_access->GetEntry();
                table = index->GetTable();
                MOT_LOG_INFO("Rollback of create index %s for table %s",
                    index->GetName().c_str(),
                    table->GetLongTableName().c_str());
                table->WrLock();
                if (index->IsPrimaryKey()) {
                    table->DecIndexColumnUsage(index);
                    table->SetPrimaryIndex(nullptr);
                    table->DeleteIndex(index);
                } else {
                    table->RemoveSecondaryIndex(index, this);
                }
                table->Unlock();
                break;
            case DDL_ACCESS_DROP_INDEX:
                index = (Index*)ddl_access->GetEntry();
                table = index->GetTable();
                MOT_LOG_INFO("Rollback of drop index %s for table %s",
                    index->GetName().c_str(),
                    table->GetLongTableName().c_str());
                table->WrLock();
                if (index->IsPrimaryKey()) {
                    table->IncIndexColumnUsage(index);
                    table->SetPrimaryIndex(index);
                } else {
                    table->AddSecondaryIndexToMetaData(index);
                }
                table->Unlock();
                break;
            default:
                break;
        }
    }
}

void TxnManager::CleanDDLChanges()
{
    // early exit
    if (m_txnDdlAccess->Size() == 0)
        return;

    MOT::Index* index = nullptr;
    MOTIndexArr* indexArr = nullptr;
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
                indexArr = (MOTIndexArr*)ddl_access->GetEntry();
                table = indexArr->GetTable();
                if (indexArr->GetNumIndexes() > 0) {
                    table->m_rowCount = 0;
                    for (int i = 0; i < indexArr->GetNumIndexes(); i++) {
                        index = indexArr->GetIndex(i);
                        table->DeleteIndex(index);
                    }
                }
                table->FreeObjectPool(indexArr->GetRowPool());
                delete indexArr;
                break;
            case DDL_ACCESS_CREATE_INDEX:
                index = (Index*)ddl_access->GetEntry();
                table = index->GetTable();
                index->SetIsCommited(true);
                break;
            case DDL_ACCESS_DROP_INDEX:
                index = (Index*)ddl_access->GetEntry();
                table = index->GetTable();
                table->DeleteIndex(index);
                break;
            default:
                break;
        }
    }
}

Row* TxnManager::RemoveKeyFromIndex(Row* row, Sentinel* sentinel)
{
    Table* table = row->GetTable();

    Row* outputRow = nullptr;
    if (sentinel->GetStable() == nullptr) {
        outputRow = table->RemoveKeyFromIndex(row, sentinel, m_threadId, GetGcSession());
    } else {
        // Checkpoint works on primary-sentinel only!
        if (sentinel->IsPrimaryIndex() == false) {
            outputRow = table->RemoveKeyFromIndex(row, sentinel, m_threadId, GetGcSession());
        }
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
      m_csn(CSNManager::INVALID_CSN),
      m_transactionId(INVALID_TRANSACTION_ID),
      m_replayLsn(0),
      m_surrogateGen(),
      m_flushDone(false),
      m_internalStmtCount(0),
      m_isolationLevel(READ_COMMITED),
      m_isLightSession(false),
      m_errIx(nullptr),
      m_err(RC_OK)
{
    m_key = nullptr;
    m_state = TxnState::TXN_START;
    m_internalTransactionId = GetTxnIdManager().GetNextId();
}

TxnManager::~TxnManager()
{
    if (GetGlobalConfiguration().m_enableCheckpoint) {
        GetCheckpointManager()->EndCommit(this);
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
    if (access->m_type == AccessType::WR) {
        access->m_modifiedColumns |= modifiedColumns;
        access->m_stmtCount = GetStmtCount();
    }
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

void TxnInsertAction::CleanupOptimisticInsert(
    InsItem* currentItem, Sentinel* pIndexInsertResult, bool isInserted, bool isMappedToCache)
{
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
            MOT::Index* index_ = pIndexInsertResult->GetIndex();
            if (rc == RC::RC_INDEX_DELETE) {
                // Memory reclamation need to release the key from the primary sentinel back to the pool
                MOT_ASSERT(pIndexInsertResult->GetCounter() == 0);
                Sentinel* outputSen = index_->IndexRemove(currentItem->m_key, m_manager->GetThdId());
                MOT_ASSERT(outputSen != nullptr);
                MOT_ASSERT(outputSen->GetCounter() == 0);

                m_manager->GcSessionRecordRcu(
                    index_->GetIndexId(), outputSen, nullptr, Index::SentinelDtor, SENTINEL_SIZE(index_));
                m_manager->m_accessMgr->IncreaseTableStat(table);
            }
        }
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
        bool res = reinterpret_cast<MOT::Index*>(currentItem->m_index)
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
                    goto end;
            }
        } else if (res == true or pIndexInsertResult->IsCommited() == false) {
            // tag all the sentinels the insert metadata
            MOT_ASSERT(pIndexInsertResult->GetCounter() != 0);
            // Reuse the row connected to header
            if (unlikely(pIndexInsertResult->GetData() != nullptr)) {
                if (pIndexInsertResult->IsCommited() == false) {
                    accessRow = m_manager->m_accessMgr->AddInsertToLocalAccess(pIndexInsertResult, row, rc, true);
                }
            } else {
                // Insert succeeded Sentinel was not committed before!
                accessRow = m_manager->m_accessMgr->AddInsertToLocalAccess(pIndexInsertResult, row, rc);
            }
            if (accessRow == nullptr) {
                ReportError(rc, currentItem);
                goto end;
            }
            isMappedToCache = true;
        }
        ++currentItem;
    }

end:
    if ((rc != RC_OK) && (currentItem != EndCursor())) {
        CleanupOptimisticInsert(currentItem, pIndexInsertResult, isInserted, isMappedToCache);
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
            case DDL_ACCESS_TRUNCATE_TABLE:
                return ((MOTIndexArr*)ddl_access->GetEntry())->GetTable();
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
    Table* table = GetTableByExternalId(table_id);
    if (table == nullptr) {
        return nullptr;
    } else {
        return table->GetIndexByExtId(index_id);
    }
}

RC TxnManager::CreateTable(Table* table)
{
    size_t serializeRedoSize = table->SerializeRedoSize();
    if (serializeRedoSize > RedoLogWriter::REDO_MAX_TABLE_SERIALIZE_SIZE) {
        MOT_REPORT_ERROR(MOT_ERROR_RESOURCE_LIMIT,
            "Create Table",
            "Table Serialize size %zu exceeds the maximum allowed limit %u",
            serializeRedoSize,
            RedoLogWriter::REDO_MAX_TABLE_SERIALIZE_SIZE);
        return RC_ERROR;
    }

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
    TxnDDLAccess::DDLAccess* new_ddl_access =
        new (std::nothrow) TxnDDLAccess::DDLAccess(table->GetTableExId(), DDL_ACCESS_DROP_TABLE, (void*)table);
    if (new_ddl_access == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Drop Table", "Failed to allocate DDL Access object");
        return RC_MEMORY_ALLOCATION_ERROR;
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

    m_txnDdlAccess->Add(new_ddl_access);
    return RC_OK;
}

RC TxnManager::TruncateTable(Table* table)
{
    RC res = RC_OK;
    if (m_isLightSession)  // really?
        return res;

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

    // clean all GC elements for the table, this call should actually release
    // all elements into an appropriate object pool
    for (uint16_t i = 0; i < table->GetNumIndexes(); i++) {
        MOT::Index* index = table->GetIndex(i);
        GcManager::ClearIndexElements(index->GetIndexId(), false);
    }

    TxnDDLAccess::DDLAccess* ddl_access = m_txnDdlAccess->GetByOid(table->GetTableExId());
    if (ddl_access == nullptr) {
        MOTIndexArr* indexesArr = nullptr;
        indexesArr = new (std::nothrow) MOTIndexArr(table);
        if (indexesArr == nullptr) {
            // print error, could not allocate memory
            MOT_REPORT_ERROR(MOT_ERROR_OOM,
                "Truncate Table",
                "Failed to allocate memory for %u index objects",
                (unsigned)MAX_NUM_INDEXES);
            return RC_MEMORY_ALLOCATION_ERROR;
        }
        // allocate DDL before work and fail immediately if required
        ddl_access = new (std::nothrow)
            TxnDDLAccess::DDLAccess(table->GetTableExId(), DDL_ACCESS_TRUNCATE_TABLE, (void*)indexesArr);
        if (ddl_access == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Truncate Table", "Failed to allocate memory for DDL Access object");
            delete indexesArr;
            return RC_MEMORY_ALLOCATION_ERROR;
        }

        if (!table->InitRowPool()) {
            table->m_rowPool = indexesArr->GetRowPool();
            delete indexesArr;
            delete ddl_access;
            return RC_MEMORY_ALLOCATION_ERROR;
        }

        for (uint16_t i = 0; i < table->GetNumIndexes(); i++) {
            MOT::Index* index = table->GetIndex(i);
            MOT::Index* index_copy = index->CloneEmpty();
            if (index_copy == nullptr) {
                // print error, could not allocate memory for index
                MOT_REPORT_ERROR(
                    MOT_ERROR_OOM, "Truncate Table", "Failed to clone empty index %s", index->GetName().c_str());
                for (uint16_t j = 0; j < indexesArr->GetNumIndexes(); j++) {
                    // cleanup of previous created indexes copy
                    MOT::Index* oldIndex = indexesArr->GetIndex(j);
                    uint16_t oldIx = indexesArr->GetIndexIx(j);
                    MOT::Index* newIndex = table->m_indexes[oldIx];
                    table->m_indexes[oldIx] = oldIndex;
                    if (oldIx != 0)  // is secondary
                        table->m_secondaryIndexes[oldIndex->GetName()] = oldIndex;
                    else  // is primary
                        table->m_primaryIndex = oldIndex;
                    delete newIndex;
                }
                delete ddl_access;
                delete indexesArr;
                return RC_MEMORY_ALLOCATION_ERROR;
            }
            indexesArr->Add(i, index);
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

RC TxnManager::CreateIndex(Table* table, MOT::Index* index, bool is_primary)
{
    size_t serializeRedoSize = table->SerializeItemSize(index);
    if (serializeRedoSize > RedoLogWriter::REDO_MAX_INDEX_SERIALIZE_SIZE) {
        MOT_REPORT_ERROR(MOT_ERROR_RESOURCE_LIMIT,
            "Create Index",
            "Index Serialize size %zu exceeds the maximum allowed limit %u",
            serializeRedoSize,
            RedoLogWriter::REDO_MAX_INDEX_SERIALIZE_SIZE);
        return RC_ERROR;
    }

    // allocate DDL before work and fail immediately if required
    TxnDDLAccess::DDLAccess* ddl_access =
        new (std::nothrow) TxnDDLAccess::DDLAccess(index->GetExtId(), DDL_ACCESS_CREATE_INDEX, (void*)index);
    if (ddl_access == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Create Index", "Failed to allocate DDL Access object");
        return RC_MEMORY_ALLOCATION_ERROR;
    }

    table->WrLock();  // for concurrent access
    if (is_primary) {
        if (!table->UpdatePrimaryIndex(index, this, m_threadId)) {
            table->Unlock();
            if (MOT_IS_OOM()) {  // do not report error in "unique violation" scenario
                MOT_REPORT_ERROR(
                    MOT_ERROR_INTERNAL, "Create Index", "Failed to add primary index %s", index->GetName().c_str());
            }
            delete ddl_access;
            return m_err;
        }
    } else {
        // currently we are still adding the index to the table although
        // is should only be added on successful commit. Assuming that if
        // a client did a create index, all other clients are waiting on a lock
        // until the changes are either commited or aborted
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
    }
    table->Unlock();
    m_txnDdlAccess->Add(ddl_access);
    return RC_OK;
}

RC TxnManager::DropIndex(MOT::Index* index)
{
    // allocate DDL before work and fail immediately if required
    TxnDDLAccess::DDLAccess* new_ddl_access =
        new (std::nothrow) TxnDDLAccess::DDLAccess(index->GetExtId(), DDL_ACCESS_DROP_INDEX, (void*)index);
    if (new_ddl_access == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Drop Index", "Failed to allocate DDL Access object");
        return RC_MEMORY_ALLOCATION_ERROR;
    }

    RC res = RC_OK;
    Table* table = index->GetTable();

    if (!m_isLightSession && !index->IsPrimaryKey()) {
        TxnOrderedSet_t& access_row_set = m_accessMgr->GetOrderedRowSet();
        TxnOrderedSet_t::iterator it = access_row_set.begin();
        while (it != access_row_set.end()) {
            Access* ac = it->second;
            if (ac->GetSentinel()->GetIndex() == index) {
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

    table->RemoveSecondaryIndexFromMetaData(index);
    m_txnDdlAccess->Add(new_ddl_access);
    return res;
}
}  // namespace MOT
