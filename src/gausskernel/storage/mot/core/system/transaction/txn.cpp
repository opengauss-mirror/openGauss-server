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

#include <cstdlib>
#include <algorithm>
#include <unordered_map>

#include "txn_table.h"
#include "mot_engine.h"
#include "redo_log_writer.h"
#include "sentinel.h"
#include "txn.h"
#include "txn_access.h"
#include "txn_insert_action.h"
#include "db_session_statistics.h"
#include "utilities.h"
#include "mm_api.h"
#include "txn_local_allocators.h"

namespace MOT {
DECLARE_LOGGER(TxnManager, System);

bool TxnManager::IsReadOnlyTxn() const
{
    return ((m_accessMgr->Size() == 0) and (m_txnDdlAccess->Size() == 0));
}

bool TxnManager::hasDDL() const
{
    return (m_txnDdlAccess->Size() > 0);
}

void TxnManager::RemoveTableFromStat(Table* t)
{
    m_accessMgr->RemoveTableFromStat(t);
}

RC TxnManager::UpdateRow(Row* row, const int attr_id, double attr_value)
{
    RC rc = UpdateLastRowState(AccessType::WR);
    if (rc != RC_OK) {
        return rc;
    }

    Row* draftVersion = m_accessMgr->GetLastAccess()->GetLocalVersion();
    draftVersion->SetValue(attr_id, attr_value);
    return rc;
}

RC TxnManager::UpdateRow(Row* row, const int attr_id, uint64_t attr_value)
{
    RC rc = UpdateLastRowState(AccessType::WR);
    if (rc != RC_OK) {
        return rc;
    }

    Row* draftVersion = m_accessMgr->GetLastAccess()->GetLocalVersion();
    draftVersion->SetValue(attr_id, attr_value);
    return rc;
}

Row* TxnManager::GetLastAccessedDraft()
{
    return m_accessMgr->GetLastAccessedDraft();
}

InsItem* TxnManager::GetNextInsertItem(Index* index)
{
    return m_accessMgr->GetInsertMgr()->GetInsertItem(index);
}

Key* TxnManager::GetTxnKey(MOT::Index* index)
{
    return m_dummyIndex->CreateNewKey(index);
}

void TxnManager::DestroyTxnKey(Key* key) const
{
    m_dummyIndex->DestroyKey(key);
}

RC TxnManager::InsertRow(Row* row, Key* updateColumnKey)
{
    RC rc = RC_OK;
    rc = SetSnapshot();
    SetTxnOper(SubTxnOperType::SUB_TXN_DML);
    if (rc != RC_OK) {
        return rc;
    }

    rc = m_accessMgr->GetInsertMgr()->ExecuteOptimisticInsert(row, updateColumnKey);
    if (rc == RC_OK) {
        m_accessMgr->IncreaseTableStat(row->GetTable());
        MOT::DbSessionStatisticsProvider::GetInstance().AddInsertRow();
    }
    return rc;
}

RC TxnManager::InsertRecoveredRow(Row* row)
{
    RC rc = RC_OK;
    rc = SetSnapshot();
    SetTxnOper(SubTxnOperType::SUB_TXN_DML);
    if (rc != RC_OK) {
        return rc;
    }

    rc = m_accessMgr->GetInsertMgr()->ExecuteRecoveryOCCInsert(row);
    if (rc == RC_OK) {
        m_accessMgr->IncreaseTableStat(row->GetTable());
        MOT::DbSessionStatisticsProvider::GetInstance().AddInsertRow();
    }
    return rc;
}

bool TxnManager::IsRowExist(Sentinel* const& sentinel, RC& rc)
{
    rc = RC_OK;
    Row* row = RowLookup(RD, sentinel, rc);
    if (row != nullptr) {
        return true;
    }

    return false;
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

    // Set Snapshot for statement
    rc = SetSnapshot();
    if (rc != RC_OK) {
        return nullptr;
    }

    Prefetch((const void*)(originalSentinel));
    // if txn not started, tag as started and take global epoch
    RC res = AccessLookup(type, originalSentinel, local_row);

    switch (res) {
        case RC::RC_LOCAL_ROW_FOUND:
            break;
        case RC::RC_LOCAL_ROW_NOT_FOUND:
            // For Read-Only Txn return the Commited row
            if (type == AccessType::RD or type == AccessType::INS) {
                local_row = m_accessMgr->GetVisibleRowVersion(originalSentinel, GetVisibleCSN());
            } else {
                // Row is not in the cache,map it and return the local row
                local_row = m_accessMgr->MapRowtoLocalTable(type, originalSentinel, rc);
            }
            break;
        case RC::RC_LOCAL_ROW_NOT_VISIBLE:
        case RC::RC_LOCAL_ROW_DELETED:
            return nullptr;
        case RC_PRIMARY_SENTINEL_NOT_MAPPED:
            local_row = m_accessMgr->FetchRowFromPrimarySentinel(type, originalSentinel, rc);
            break;
        case RC::RC_MEMORY_ALLOCATION_ERROR:
            rc = RC_MEMORY_ALLOCATION_ERROR;
            return nullptr;
        default:
            return nullptr;
    }
    if (local_row) {
        return m_accessMgr->GetRowZeroCopyIfAny(local_row);
    }
    return nullptr;
}

RC TxnManager::AccessLookup(const AccessType type, Sentinel* const& originalSentinel, Row*& localRow)
{
    return m_accessMgr->AccessLookup(type, originalSentinel, localRow);
}

RC TxnManager::DeleteLastRow()
{
    RC rc;
    SetTxnOper(SubTxnOperType::SUB_TXN_DML);
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

bool TxnManager::IsUpdatedInCurrStmt()
{
    Access* access = m_accessMgr->GetLastAccess();
    if (access == nullptr) {
        return true;
    }
    if (access->GetOpsCount() > 0 and access->GetStmtCount() == GetStmtCount()) {
        return true;
    }
    return false;
}

void TxnManager::StartTransaction(uint64_t transactionId, int isolationLevel)
{
    m_transactionId = transactionId;
    m_isolationLevel = static_cast<ISOLATION_LEVEL>(isolationLevel);
    m_state = TxnState::TXN_START;
}

void TxnManager::StartSubTransaction(uint64_t subTransactionId, int isolationLevel)
{
    m_subTxnManager.Start(subTransactionId);
}

void TxnManager::CommitSubTransaction(uint64_t subTransactionId)
{
    m_subTxnManager.Commit(subTransactionId);
}

RC TxnManager::RollbackSubTransaction(uint64_t subTransactionId)
{
    RC rc = RC_OK;
    rc = m_subTxnManager.Rollback(subTransactionId);
    return rc;
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
            GetCheckpointManager()->EndCommit(this);
        }

        m_occManager.ReleaseHeaders(this);
        m_redoLog.RollbackPrepared();
    } else {
        m_occManager.ReleaseLocks(this);
    }

    // We have to undo changes to secondary indexes and ddls
    UndoLocalDMLChanges();
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
    RC rc = RC_OK;
    bool validateOccIsDone = false;
    if (GetGlobalConfiguration().m_enableCheckpoint) {
        GetCheckpointManager()->BeginCommit(this);
    }
    if (m_txnDdlAccess->Size() != 0) {
        rc = m_txnDdlAccess->ValidateDDLChanges(this);
    }
    // Run only first validation phase
    if (rc == RC_OK) {
        rc = m_occManager.ValidateOcc(this);
    }
    if (rc == RC_OK) {
        validateOccIsDone = true;
        rc = m_redoLog.Prepare();
    }
    if (rc != RC_OK && GetGlobalConfiguration().m_enableCheckpoint) {
        if (validateOccIsDone) {
            GetCheckpointManager()->FreePreAllocStableRows(this);
        }
        GetCheckpointManager()->EndCommit(this);
    }
    return rc;
}

void TxnManager::LitePrepare()
{
    if (m_txnDdlAccess->Size() == 0) {
        return;
    }

    (void)m_redoLog.Prepare();
}

void TxnManager::CommitInternal()
{
    if (m_csn == INVALID_CSN) {
        SetCommitSequenceNumber(GetCSNManager().GetNextCSN());
    }

    // first write to redo log, then write changes
    (void)m_redoLog.Commit();
    ApplyDDLChanges();
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
    RC res = RC_OK;
    if (IsTxnAborted()) {
        return MOT::RC_ABORT;
    }
    if (!IsReadOnlyTxn()) {
        if (!IsRecoveryTxn() and MOTEngine::GetInstance()->IsRecovering()) {
            return MOT::RC_ABORT;
        }
    }
    if (GetGlobalConfiguration().m_enableCheckpoint) {
        GetCheckpointManager()->BeginCommit(this);
    }
    if (m_txnDdlAccess->Size() != 0) {
        res = m_txnDdlAccess->ValidateDDLChanges(this);
    }
    if (res == RC_OK) {
        res = m_occManager.ValidateOcc(this);
    }
    if (res != RC_OK && GetGlobalConfiguration().m_enableCheckpoint) {
        GetCheckpointManager()->EndCommit(this);
    }
    return res;
}

void TxnManager::RecordCommit()
{
    CommitInternal();
    MOT::DbSessionStatisticsProvider::GetInstance().AddCommitTxn();
}

RC TxnManager::Commit()
{
    if (IsReadOnlyTxn()) {
        return RC_OK;
    }

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
        (void)m_redoLog.Commit();
        ApplyDDLChanges();
        Cleanup();
    }
    MOT::DbSessionStatisticsProvider::GetInstance().AddCommitTxn();
}

void TxnManager::CommitPrepared()
{
    if (m_csn == INVALID_CSN) {
        SetCommitSequenceNumber(GetCSNManager().GetNextCSN());
    }

    // first write to redo log, then write changes
    m_redoLog.CommitPrepared();
    ApplyDDLChanges();
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
        ApplyDDLChanges();
        Cleanup();
    }
    MOT::DbSessionStatisticsProvider::GetInstance().AddCommitPreparedTxn();
}

void TxnManager::EndTransaction()
{
    if (!IsReadOnlyTxn()) {
        if (GetGlobalConfiguration().m_enableRedoLog) {
            m_occManager.ReleaseLocks(this);
        }
    }
    Cleanup();
}

void TxnManager::RedoWriteAction(bool isCommit)
{
    m_redoLog.SetForceWrite();
    if (isCommit)
        (void)m_redoLog.Commit();
    else
        m_redoLog.Rollback();
}

inline void TxnManager::Cleanup()
{
    if (m_occManager.IsTransactionCommited()) {
        m_accessMgr->GcMaintenance();
    }
    if (!m_isLightSession) {
        m_accessMgr->ClearSet();
    }
    m_isTxnAborted = false;
    m_hasCommitedSubTxnDDL = false;
    m_isCrossEngineTxn = false;
    m_txnDdlAccess->Reset();
    if (!m_checkpointCommitEnded) {
        GetCheckpointManager()->EndCommit(this);
    }
    m_checkpointCommitEnded = true;
    m_checkpointPhase = CheckpointPhase::NONE;
    m_csn = INVALID_CSN;
    m_occManager.CleanUp();
    m_err = RC_OK;
    m_errIx = nullptr;
    m_flushDone = false;
    m_internalStmtCount = 0;
    m_isSnapshotTaken = false;
    m_hasIxColUpd = false;
    if (GetGlobalConfiguration().m_enableRedoLog) {
        m_internalTransactionId = GetTxnIdManager().GetNextId();
    }
    m_redoLog.Reset();
    SetVisibleCSN(0);
    if (!m_isRecoveryTxn) {
        GcSessionEnd();
    } else {
        GcSessionEndRecovery();
    }
    ClearErrorStack();
    m_dummyIndex->ClearKeyCache();
    m_accessMgr->ClearDummyTableCache();
    if (!m_isRecoveryTxn) {
        m_accessMgr->ClearTableCache();
    }
    m_queryState.clear();
    m_subTxnManager.ResetSubTransactionMgr();
}

void TxnManager::UndoLocalDMLChanges()
{
    uint32_t rollbackCounter = 0;
    bool hasTxnDDL = hasDDL();
    TxnOrderedSet_t& OrderedSet = m_accessMgr->GetOrderedRowSet();
    for (const auto& ra_pair : OrderedSet) {
        Access* ac = ra_pair.second;
        if (ac->m_type != AccessType::INS) {
            if (unlikely(hasTxnDDL)) {
                rollbackCounter++;
            }
            continue;
        }

        rollbackCounter++;
        // the index will de-allocated, so there is no need to remove data
        if (ac->GetSentinel()->GetIndex()->GetIsCommited()) {
            RollbackInsert(ac);
        } else {
            if (ac->m_secondaryDelKey != nullptr) {
                ac->GetSentinel()->GetIndex()->DestroyKey(ac->m_secondaryDelKey);
                ac->m_secondaryDelKey = nullptr;
            }
        }
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
                Row* r = ac->GetTxnRow();
                r->GetTable()->DestroyRow(r);
                ac->m_localInsertRow = nullptr;
            }
            rollbackCounter--;
        } else {
            if (unlikely(hasTxnDDL)) {
                m_accessMgr->DestroyAccess(ac);
                rollbackCounter--;
            }
        }
        if (rollbackCounter == 0) {
            break;
        }
    }
}

void TxnManager::RollbackInsert(Access* ac)
{
    Sentinel* outputSen = nullptr;
    RC rc = RC_OK;
    Sentinel* sentinel = ac->GetSentinel();
    MOT::Index* index_ = sentinel->GetIndex();
    /*
     * First we check if this is a new insert
     * If so we can try remove it from the tree as it is not visible by any
     * transaction We put the sentinels in the generic Queue
     */
    if (ac->m_params.IsUpgradeInsert() == false) {
        MOT_ASSERT(sentinel != nullptr);
        // If the sentinel was never committed we may try to remove it
        rc = sentinel->RefCountUpdate(DEC);
        MOT_ASSERT(rc != RC::RC_INDEX_RETRY_INSERT);
        if (rc == RC::RC_INDEX_DELETE) {
            if (sentinel->IsDirty()) {
                MOT::Key* pKey = m_key;
                if (likely(ac->m_secondaryDelKey == nullptr)) {
                    MOT::Table* table =
                        (index_->GetTable()->IsTxnTable() ? index_->GetTable() : ac->GetTxnRow()->GetTable());
                    // If refCount == 0, this sentinel passed GC barrier and can be removed from tree!
                    // Memory reclamation need to release the key from the primary sentinel back to the pool
                    m_key->InitKey(index_->GetKeyLength());
                    index_->BuildKey(table, ac->GetTxnRow(), m_key);
                } else {
                    pKey = ac->m_secondaryDelKey;
                }
                MOT_ASSERT(sentinel->GetCounter() == 0);
#ifdef MOT_DEBUG
                Sentinel* curr_sentinel = index_->IndexReadHeader(pKey, GetThdId());
                MOT_ASSERT(curr_sentinel == sentinel);
#endif
                outputSen = index_->IndexRemove(pKey, GetThdId());
                MOT_ASSERT(outputSen != nullptr);
                MOT_ASSERT(outputSen->GetCounter() == 0);
                if (index_->GetIndexOrder() == IndexOrder::INDEX_ORDER_PRIMARY) {
                    (void)static_cast<PrimarySentinel*>(outputSen)->GetGcInfo().RefCountUpdate(INC);
                }
                GcSessionRecordRcu(GC_QUEUE_TYPE::GENERIC_QUEUE,
                    index_->GetIndexId(),
                    outputSen,
                    nullptr,
                    Index::SentinelDtor,
                    SENTINEL_SIZE(index_));
            } else {
                // If we rollback we can assume the top is deleted!
                // We can never reach here since GC cleanup is
                // blocked till will finish commit
                MOT_ASSERT(false);
            }
        }
    } else {
        /*
         * At this point we rollback the newly inserted row and responsible to reclaim
         * the old deleted row synced with the other threads
         * 1. Increase the gc_info and put the tombstone in the GC with the original CSN!
         * 2. Try to reclaim the current insert using the refCount mechanism
         * 3. SentinelDtor should support gcInfo synchronization
         */
        if (ac->m_params.IsInsertOnDeletedRow()) {
            rc = sentinel->RollBackUnique();
            if (ac->m_params.IsPrimarySentinel() == true) {
                // If we are not the last owner skip the element
                // If we are the last one (RC_INDEX_DELETE) Lets do the work of the original DELETE
                if (rc == RC_INDEX_DELETE) {
                    // First Step register to gcInfo
                    (void)static_cast<PrimarySentinel*>(ac->m_origSentinel)->GetGcInfo().RefCountUpdate(INC);
                    // Add the Sentinel to the GC - off load the work
                    Row* r = ac->GetGlobalVersion();
                    MOT_ASSERT(r->IsRowDeleted() == true);
                    GcSessionRecordRcu(GC_QUEUE_TYPE::DELETE_QUEUE,
                        r->GetTable()->GetPrimaryIndex()->GetIndexId(),
                        r,
                        ac->m_origSentinel,
                        Row::DeleteRowDtor,
                        ROW_SIZE_FROM_POOL(r->GetTable()));
                }
            } else if (ac->m_params.IsUniqueIndex() == false) {
                if (ac->m_params.IsIndexUpdate() == true) {
                    if (rc == RC_INDEX_DELETE) {
                        ReclaimAccessSecondaryDelKey(ac);
                    }
                }
            }
        } else {
            // Since the operation was on a committed version just decrease reference count
            // Reference count must be greater then 1 after decrement!
            rc = sentinel->RollBackUnique();
            if (ac->m_params.IsIndexUpdate() == true) {
                if (rc == RC_INDEX_DELETE) {
                    ReclaimAccessSecondaryDelKey(ac);
                }
            }
        }
    }
}

void TxnManager::ReclaimAccessSecondaryDelKey(Access* ac)
{
    GcSessionRecordRcu(GC_QUEUE_TYPE::UPDATE_COLUMN_QUEUE,
        ac->m_origSentinel->GetIndex()->GetIndexId(),
        ac->m_origSentinel,
        ac->m_secondaryDelKey,
        Index::DeleteKeyDtor,
        SENTINEL_SIZE(ac->m_origSentinel->GetIndex()));
    ac->m_secondaryDelKey = nullptr;
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
            (void)++it;
        }
    }
}

void TxnManager::RollbackDDLs()
{
    // early exit
    if (m_txnDdlAccess->Size() == 0) {
        ClearReservedChunks();
        return;
    }

    // we need to generate notification also for each create table/index rollback to allow any key cleanup before row
    // and key pools are deleted
    for (int i = m_txnDdlAccess->Size() - 1; i >= 0; i--) {
        MOT::Index* index = nullptr;
        TxnTable* table = nullptr;
        TxnDDLAccess::DDLAccess* ddl_access = m_txnDdlAccess->Get(i);
        switch (ddl_access->GetDDLAccessType()) {
            case DDL_ACCESS_CREATE_TABLE:
                table = (TxnTable*)ddl_access->GetEntry();
                MOTEngine::GetInstance()->NotifyDDLEvent(
                    table->GetTableExId(), MOT::DDL_ACCESS_CREATE_TABLE, TxnDDLPhase::TXN_DDL_PHASE_ROLLBACK);
                break;

            case DDL_ACCESS_CREATE_INDEX:
                index = (Index*)ddl_access->GetEntry();
                MOTEngine::GetInstance()->NotifyDDLEvent(
                    index->GetExtId(), MOT::DDL_ACCESS_CREATE_INDEX, TxnDDLPhase::TXN_DDL_PHASE_ROLLBACK);
                break;
            default:
                break;
        }
    }

    // generate one more notification for entire transaction
    MOTEngine::GetInstance()->NotifyDDLEvent(0, DDL_ACCESS_UNKNOWN, TxnDDLPhase::TXN_DDL_PHASE_ROLLBACK);

    m_txnDdlAccess->RollbackDDLChanges(this);
    // rollback DDLs in reverse order (avoid rolling back parent object before rolling back child)
    for (int i = m_txnDdlAccess->Size() - 1; i >= 0; i--) {
        MOT::Index* index = nullptr;
        TxnTable* table = nullptr;
        TxnDDLAccess::DDLAccess* ddl_access = m_txnDdlAccess->Get(i);
        switch (ddl_access->GetDDLAccessType()) {
            case DDL_ACCESS_CREATE_TABLE:
                table = (TxnTable*)ddl_access->GetEntry();
                MOT_LOG_INFO("Rollback of create table %s", table->GetLongTableName().c_str());
                table->GetOrigTable()->DropImpl();
                RemoveTableFromStat(table);
                delete table->GetOrigTable();
                break;
            case DDL_ACCESS_DROP_TABLE:
                table = (TxnTable*)ddl_access->GetEntry();
                MOT_LOG_INFO("Rollback of drop table %s", table->GetLongTableName().c_str());
                break;
            case DDL_ACCESS_TRUNCATE_TABLE:
                break;
            case DDL_ACCESS_CREATE_INDEX:
                break;
            case DDL_ACCESS_DROP_INDEX:
                index = (Index*)ddl_access->GetEntry();
                if (index->GetTable()->IsTxnTable()) {
                    index->GetTable()->DeleteIndex(index);
                }
                break;
            case DDL_ACCESS_ADD_COLUMN: {
                DDLAlterTableAddDropColumn* alter = (DDLAlterTableAddDropColumn*)ddl_access->GetEntry();
                if (alter->m_column != nullptr) {
                    delete alter->m_column;
                }
                delete alter;
                break;
            }
            case DDL_ACCESS_DROP_COLUMN: {
                DDLAlterTableAddDropColumn* alter = (DDLAlterTableAddDropColumn*)ddl_access->GetEntry();
                if (alter->m_column != nullptr) {
                    delete alter->m_column;
                }
                delete alter;
                break;
            }
            case DDL_ACCESS_RENAME_COLUMN: {
                DDLAlterTableRenameColumn* alter = (DDLAlterTableRenameColumn*)ddl_access->GetEntry();
                delete alter;
                break;
            }
            default:
                break;
        }
    }
    ClearReservedChunks();
}

void TxnManager::ApplyDDLChanges()
{
    // early exit
    if (m_txnDdlAccess->Size() == 0) {
        ClearReservedChunks();
        return;
    }

    MOTEngine::GetInstance()->NotifyDDLEvent(0, DDL_ACCESS_UNKNOWN, MOT::TxnDDLPhase::TXN_DDL_PHASE_COMMIT);

    MOT::Index* index = nullptr;
    TxnTable* table = nullptr;
    for (uint16_t i = 0; i < m_txnDdlAccess->Size(); i++) {
        TxnDDLAccess::DDLAccess* ddl_access = m_txnDdlAccess->Get(i);
        switch (ddl_access->GetDDLAccessType()) {
            case DDL_ACCESS_DROP_INDEX:
                index = (Index*)ddl_access->GetEntry();
                if (index->GetTable()->IsTxnTable()) {
                    index->GetTable()->DeleteIndex(index);
                }
                break;
            case DDL_ACCESS_ADD_COLUMN: {
                DDLAlterTableAddDropColumn* alterAdd = (DDLAlterTableAddDropColumn*)ddl_access->GetEntry();
                if (alterAdd->m_column != nullptr) {
                    delete alterAdd->m_column;
                }
                delete alterAdd;
                break;
            }
            case DDL_ACCESS_DROP_COLUMN: {
                DDLAlterTableAddDropColumn* alterDrop = (DDLAlterTableAddDropColumn*)ddl_access->GetEntry();
                if (alterDrop->m_column != nullptr) {
                    delete alterDrop->m_column;
                }
                delete alterDrop;
                break;
            }
            case DDL_ACCESS_RENAME_COLUMN: {
                DDLAlterTableRenameColumn* alterRename = (DDLAlterTableRenameColumn*)ddl_access->GetEntry();
                delete alterRename;
                break;
            }
            default:
                break;
        }
    }

    m_txnDdlAccess->ApplyDDLChanges(this);

    for (uint16_t i = 0; i < m_txnDdlAccess->Size(); i++) {
        TxnDDLAccess::DDLAccess* ddl_access = m_txnDdlAccess->Get(i);
        if (ddl_access->GetDDLAccessType() == DDL_ACCESS_CREATE_TABLE) {
            table = (TxnTable*)ddl_access->GetEntry();
            if (table != nullptr && !table->IsDropped()) {
                (void)GetTableManager()->AddTable(table->GetOrigTable());
            }
        } else if (ddl_access->GetDDLAccessType() == DDL_ACCESS_DROP_TABLE) {
            table = (TxnTable*)ddl_access->GetEntry();
            table->RollbackDDLChanges(this);
            (void)GetTableManager()->DropTable(table->GetOrigTable(), this);
        }
    }

    ClearReservedChunks();
}

// Use this function when we have only the key!
// Not Used with FDW!
Row* TxnManager::RowLookupByKey(Table* const& table, const AccessType type, Key* const currentKey, RC& rc)
{
    rc = RC_OK;
    Sentinel* pSentinel = nullptr;
    (void)table->FindRow(currentKey, pSentinel, GetThdId());
    if (pSentinel == nullptr) {
        MOT_LOG_DEBUG("Cannot find key:%" PRIu64 " from table:%s Visible CSN %lu",
            m_key,
            table->GetLongTableName().c_str(),
            GetVisibleCSN());
        return nullptr;
    } else {
        return RowLookup(type, pSentinel, rc);
    }
}

RC TxnManager::SetSnapshot()
{
    RC rc = RC_OK;
    if (!GetSnapshotStatus()) {
        if (m_gcSession->IsGcSnapshotTaken() == true) {
            SetVisibleCSN(MOTEngine::GetInstance()->GetCurrentCSN());
        } else {
            // Need to Guaranty GC work on latest snapshot
            rc = GcSessionStart();
            SetVisibleCSN(m_gcSession->GetCurrentEpoch());
        }
        SetSnapshotStatus(true);
    }
    if (IsRecoveryTxn()) {
        SetVisibleCSN(static_cast<uint64_t>(-1));
    }
    return rc;
}

TxnManager::TxnManager(SessionContext* session_context, bool global /* = false */)
    : m_visibleCSN(0),
      m_threadId((uint64_t)-1),
      m_connectionId((uint64_t)-1),
      m_sessionContext(session_context),
      m_redoLog(this),
      m_occManager(),
      m_gcSession(nullptr),
      m_checkpointPhase(CheckpointPhase::NONE),
      m_checkpointNABit(false),
      m_checkpointCommitEnded(false),
      m_csn(INVALID_CSN),
      m_transactionId(INVALID_TRANSACTION_ID),
      m_replayLsn(0),
      m_surrogateGen(),
      m_flushDone(false),
      m_internalTransactionId(0),
      m_internalStmtCount(0),
      m_state(TxnState::TXN_START),
      m_isolationLevel(READ_COMMITED),
      m_isSnapshotTaken(false),
      m_isTxnAborted(false),
      m_hasCommitedSubTxnDDL(false),
      m_isCrossEngineTxn(false),
      m_isRecoveryTxn(false),
      m_hasIxColUpd(false),
      m_key(nullptr),
      m_isLightSession(false),
      m_errIx(nullptr),
      m_err(RC_OK),
      m_reservedChunks(0),
      m_global(global),
      m_dummyIndex(nullptr)
{
    if (GetGlobalConfiguration().m_enableRedoLog) {
        m_internalTransactionId = GetTxnIdManager().GetNextId();
    }
}

TxnManager::~TxnManager()
{
    if (GetGlobalConfiguration().m_enableCheckpoint) {
        GetCheckpointManager()->EndCommit(this);
    }

    MOT_LOG_DEBUG("txn_man::~txn_man - memory pools released for thread_id=%lu", m_threadId);

    if (m_dummyIndex != nullptr) {
        delete m_dummyIndex;
    }
    if (m_gcSession != nullptr) {
        m_gcSession->GcCleanAll();
        m_gcSession->RemoveFromGcList(m_gcSession);
        m_gcSession->~GcManager();
        MemFree(m_gcSession, m_global);
        m_gcSession = nullptr;
    }

    MemFreeObject<TxnAccess>(m_accessMgr, m_global);
    m_accessMgr = nullptr;
    delete m_key;
    delete m_txnDdlAccess;
    m_errIx = nullptr;
    m_sessionContext = nullptr;
}

bool TxnManager::Init(uint64_t threadId, uint64_t connectionId, bool isRecoveryTxn, bool isLightTxn)
{
    m_threadId = threadId;
    m_connectionId = connectionId;
    m_isLightSession = isLightTxn;
    m_isRecoveryTxn = isRecoveryTxn;

    m_dummyIndex = new (std::nothrow) DummyIndex();
    if (m_dummyIndex == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Initialize Transaction", "Failed to allocate memory for dummy index");
        return false;
    }
    if (!m_dummyIndex->Init(m_global)) {
        return false;
    }

    m_txnDdlAccess = new (std::nothrow) TxnDDLAccess(this);
    if (m_txnDdlAccess == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Initialize Transaction", "Failed to allocate memory for DDL access data");
        return false;
    }
    m_txnDdlAccess->Init();

    // make node-local allocations
    if (!isLightTxn) {
        m_accessMgr = MemAllocAlignedObject<TxnAccess>(L1_CACHE_LINE, m_global);

        if (!m_accessMgr->Init(this)) {
            return false;
        }

        m_surrogateGen = GetSurrogateKeyManager()->GetSurrogateSlot(connectionId);
    }

    if (!m_subTxnManager.Init(this)) {
        return false;
    }
    if (isRecoveryTxn) {
        m_gcSession = GcManager::Make(GcManager::GC_TYPE::GC_RECOVERY, threadId, m_global);
    } else {
        m_gcSession = GcManager::Make(GcManager::GC_TYPE::GC_MAIN, threadId, m_global);
    }
    if (!m_gcSession) {
        return false;
    }

    if (!m_redoLog.Init()) {
        return false;
    }

    static const TxnValidation validation_lock = GetGlobalConfiguration().m_validationLock;
    if (m_key == nullptr) {
        MOT_LOG_DEBUG("Init Key");
        m_key = new (std::nothrow) MaxKey(MAX_KEY_SIZE);
        if (m_key == nullptr) {
            MOTAbort();
        }
    }

    m_occManager.SetPreAbort(GetGlobalConfiguration().m_preAbort);
    if (validation_lock == TxnValidation::TXN_VALIDATION_NO_WAIT) {
        m_occManager.SetValidationNoWait(true);
    } else if (validation_lock == TxnValidation::TXN_VALIDATION_WAITING) {
        m_occManager.SetValidationNoWait(false);
    } else {
        MOT_ASSERT(false);
    }

    return true;
}

RC TxnManager::OverwriteRow(Row* updatedRow, const BitmapSet& modifiedColumns)
{
    if (updatedRow == nullptr) {
        return RC_ERROR;
    }

    SetTxnOper(SubTxnOperType::SUB_TXN_DML);
    Access* access = m_accessMgr->GetLastAccess();
    if (access->m_type == AccessType::WR) {
        access->m_modifiedColumns |= modifiedColumns;
    }
    access->IncreaseOps();
    access->m_stmtCount = GetStmtCount();
    return RC_OK;
}

RC TxnManager::UpdateRow(const BitmapSet& modifiedColumns, TxnIxColUpdate* colUpd)
{
    SetTxnOper(SubTxnOperType::SUB_TXN_DML);
    Access* access = m_accessMgr->GetLastAccess();
    if (access->m_type == AccessType::WR) {
        access->m_modifiedColumns |= modifiedColumns;
    }
    access->m_params.SetIndexUpdate();
    access->IncreaseOps();
    access->m_stmtCount = GetStmtCount();
    m_hasIxColUpd = true;
    if (colUpd->m_hasIxColUpd == UpdateIndexColumnType::UPDATE_COLUMN_PRIMARY) {
        return m_accessMgr->GeneratePrimaryIndexUpdate(access, colUpd);
    } else {
        return m_accessMgr->GenerateSecondaryIndexUpdate(access, colUpd);
    }
}

void TxnManager::SetTxnState(TxnState envelopeState)
{
    m_state = envelopeState;
}

void TxnManager::GcSessionRecordRcu(GC_QUEUE_TYPE m_type, uint32_t index_id, void* object_ptr, void* object_pool,
    DestroyValueCbFunc cb, uint32_t obj_size, uint64_t csn)
{
    return m_gcSession->GcRecordObject(m_type, index_id, object_ptr, object_pool, cb, obj_size, csn);
}

TxnInsertAction::~TxnInsertAction()
{
    if (m_manager != nullptr && m_insertSet != nullptr) {
        MemFree(m_insertSet, m_manager->m_global);
    }

    m_insertSet = nullptr;
    m_manager = nullptr;
}

bool TxnInsertAction::Init(TxnManager* _manager)
{
    bool rc = true;
    if (this->m_manager)
        return false;

    this->m_manager = _manager;
    m_insertSetSize = 0;
    m_insertArraySize = INSERT_ARRAY_DEFAULT_SIZE;

    void* ptr = MemAlloc(sizeof(InsItem) * m_insertArraySize, m_manager->m_global);
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
            m_manager->m_errIx->BuildErrorMsg(currentItem->m_index->GetTable(),
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

void TxnInsertAction::CleanupInsertReclaimKey(Row* row, Sentinel* sentinel)
{
    // Memory reclamation need to release the key from the primary sentinel back to the pool
    MOT_ASSERT(sentinel->GetCounter() == 0);
    Table* table = row->GetTable();
    MOT::Index* index = sentinel->GetIndex();
    Key* localKey = m_manager->GetLocalKey();
    localKey->InitKey(index->GetKeyLength());
    index->BuildKey(table, row, localKey);
    Sentinel* outputSen = index->IndexRemove(localKey, m_manager->GetThdId());
    MOT_ASSERT(outputSen != nullptr);
    MOT_ASSERT(outputSen->GetCounter() == 0);
    if (index->GetIndexOrder() == IndexOrder::INDEX_ORDER_PRIMARY) {
        (void)static_cast<PrimarySentinel*>(outputSen)->GetGcInfo().RefCountUpdate(INC);
    }
    m_manager->GcSessionRecordRcu(GC_QUEUE_TYPE::GENERIC_QUEUE,
        index->GetIndexId(),
        outputSen,
        nullptr,
        Index::SentinelDtor,
        SENTINEL_SIZE(index));
    m_manager->m_accessMgr->IncreaseTableStat(table);
}

void TxnInsertAction::CleanupOptimisticInsert(
    InsItem* currentItem, Sentinel* pIndexInsertResult, bool isInserted, bool isMappedToCache)
{
    // Clean current aborted row and clean secondary indexes that were not inserts
    // Clean first Object! - wither primary or secondary!
    // Return Local Row to pull for PI
    Table* table = currentItem->m_row->GetTable();
    if (isInserted) {
        if (!isMappedToCache) {
            RC rc = pIndexInsertResult->RefCountUpdate(DEC);
            if (rc == RC::RC_INDEX_DELETE) {
                CleanupInsertReclaimKey(currentItem->m_row, pIndexInsertResult);
            }
        }
    }
    if (currentItem->getIndexOrder() == IndexOrder::INDEX_ORDER_PRIMARY) {
        table->DestroyRow(currentItem->m_row);
    }
}

RC TxnInsertAction::AddInsertToLocalAccess(
    Row* row, InsItem* currentItem, Sentinel* pIndexInsertResult, bool& isMappedToCache)
{
    RC rc = RC_OK;
    Row* accessRow = nullptr;
    TxnAccess* accessMgr = m_manager->m_accessMgr;
    isMappedToCache = false;
    if (pIndexInsertResult->IsCommited() == true) {
        // Lets check and see if we deleted the row
        rc = accessMgr->CheckDuplicateInsert(pIndexInsertResult);
        switch (rc) {
            case RC_LOCAL_ROW_DELETED:
                // promote delete to Insert!
                // In this case the sentinel is committed and the previous scenario was delete
                // Insert succeeded Sentinel was not committed before!
                // At this point the row is not in the cache and can be mapped!
                MOT_ASSERT(currentItem->m_index->GetUnique() == true);
            // fall through
            case RC_LOCAL_ROW_NOT_FOUND:
                // Header is committed
                accessRow = accessMgr->AddInsertToLocalAccess(pIndexInsertResult, row, rc, true);
                if (accessRow == nullptr) {
                    ReportError(rc, currentItem);
                    return rc;
                }
                isMappedToCache = true;
                break;
            case RC_LOCAL_ROW_FOUND:
                // Found But not deleted = self duplicated!
                ReportError(RC_UNIQUE_VIOLATION, currentItem);
                return RC_UNIQUE_VIOLATION;
            default:
                return rc;
        }
    } else {
        // tag all the sentinels the insert metadata
        MOT_ASSERT(pIndexInsertResult->GetCounter() != 0);
        // At this point the sentinel must be either committed/not-committed
        if (unlikely(pIndexInsertResult->IsCommited())) {
            ReportError(RC_UNIQUE_VIOLATION, currentItem);
            return RC_UNIQUE_VIOLATION;
        }

        // Insert succeeded Sentinel was not committed before!
        accessRow = accessMgr->AddInsertToLocalAccess(pIndexInsertResult, row, rc);
        if (accessRow == nullptr) {
            ReportError(rc, currentItem);
            return rc;
        }
        isMappedToCache = true;
    }
    return rc;
}

RC TxnInsertAction::ExecuteOptimisticInsert(Row* row, Key* updateColumnKey)
{
    MaxKey key;
    Key* tKey = &key;
    Sentinel* pIndexInsertResult = nullptr;
    RC rc = RC_OK;
    bool isInserted = true;
    bool isMappedToCache = false;
    auto currentItem = BeginCursor();
    Table* table = row->GetTable();
    Table* txnTable = m_manager->GetTxnTable(table->GetTableExId());
    if (txnTable != nullptr) {
        table = txnTable;
    }

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
        if (unlikely(updateColumnKey != nullptr)) {
            tKey = updateColumnKey;
        } else {
            tKey->InitKey(currentItem->m_index->GetKeyLength());
            currentItem->m_index->BuildKey(table, row, tKey);
        }
        (void)reinterpret_cast<MOT::Index*>(currentItem->m_index)
            ->IndexInsert(pIndexInsertResult, tKey, m_manager->GetThdId(), rc, m_manager->IsRecoveryTxn());
        if (unlikely(rc == RC_MEMORY_ALLOCATION_ERROR)) {
            ReportError(rc);
            // Failed on Memory
            isInserted = false;
            goto end;
        } else {
            if (rc == RC_UNIQUE_VIOLATION) {
                ReportError(RC_UNIQUE_VIOLATION, currentItem);
                isInserted = false;
                goto end;
            }
            if (currentItem->getIndexOrder() == IndexOrder::INDEX_ORDER_PRIMARY) {
                row->SetPrimarySentinel(pIndexInsertResult);
            }
        }

        rc = AddInsertToLocalAccess(row, currentItem, pIndexInsertResult, isMappedToCache);
        if (rc != RC_OK) {
            // ReportError already done inside AddInsertToLocalAccess().
            goto end;
        }
        ++currentItem;
    }

end:
    if ((rc != RC_OK) && (currentItem != EndCursor())) {
        CleanupOptimisticInsert(currentItem, pIndexInsertResult, isInserted, isMappedToCache);
    }

    // Clear the current set size;
    m_insertSetSize = 0;
    return rc;
}

void TxnInsertAction::CleanupRecoveryOCCInsert(Row* row, std::vector<Sentinel*>& sentinels)
{
    TxnOrderedSet_t& orderedSet = m_manager->m_accessMgr->GetOrderedRowSet();
    for (auto s : sentinels) {
        if (s->RefCountUpdate(DEC) == RC::RC_INDEX_DELETE) {
            CleanupInsertReclaimKey(row, s);
        }
        // Remove the access if mapped
        auto search = orderedSet.find(s);
        if (search != orderedSet.end()) {
            Access* ac = (*search).second;
            if (ac->m_params.IsUpgradeInsert() == true and ac->m_params.IsInsertOnDeletedRow() == false) {
                ac->m_type = DEL;
                ac->m_params.UnsetUpgradeInsert();
                ac->m_localInsertRow = nullptr;
                if (ac->GetSentinel()->GetIndexOrder() == IndexOrder::INDEX_ORDER_SECONDARY_UNIQUE) {
                    S_SentinelNodePool* sentinelObjectPool = m_manager->m_accessMgr->GetSentinelObjectPool();
                    auto object = sentinelObjectPool->find(ac->m_origSentinel->GetIndex());
                    PrimarySentinelNode* node = object->second;
                    (void)sentinelObjectPool->erase(object);
                    ac->m_origSentinel->GetIndex()->SentinelNodeRelease(node);
                }
                continue;
            }
            (void)orderedSet.erase(search);
            // need to perform index clean-up!
            m_manager->m_accessMgr->PubReleaseAccess(ac);
        }
    }
}

RC TxnInsertAction::ExecuteRecoveryOCCInsert(Row* row)
{
    MaxKey key;
    Sentinel* pIndexInsertResult = nullptr;
    RC rc = RC_OK;
    std::vector<Sentinel*> sentinels;
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
        key.InitKey(currentItem->m_index->GetKeyLength());
        currentItem->m_index->BuildKey(row->GetTable(), row, &key);
        (void)reinterpret_cast<MOT::Index*>(currentItem->m_index)
            ->IndexInsert(pIndexInsertResult, &key, m_manager->GetThdId(), rc, m_manager->IsRecoveryTxn());
        if (unlikely(rc == RC_MEMORY_ALLOCATION_ERROR)) {
            // Failed on Memory
            ReportError(rc);
            goto end;
        } else {
            if (rc == RC_UNIQUE_VIOLATION) {
                ReportError(RC_UNIQUE_VIOLATION, currentItem);
                goto end;
            }
            if (currentItem->getIndexOrder() == IndexOrder::INDEX_ORDER_PRIMARY) {
                row->SetPrimarySentinel(pIndexInsertResult);
            }
            sentinels.push_back(pIndexInsertResult);
        }

        // Wait for previous transaction to finish
        while (pIndexInsertResult->IsLocked() == true) {
            PAUSE;
        }

        bool isMappedToCache = false;
        rc = AddInsertToLocalAccess(row, currentItem, pIndexInsertResult, isMappedToCache);
        if (rc != RC_OK) {
            // ReportError already done inside AddInsertToLocalAccess().
            goto end;
        }
        ++currentItem;
    }

end:
    if (rc != RC_OK) {
        CleanupRecoveryOCCInsert(row, sentinels);
    } else {
        // Clear the current set size;
        m_insertSetSize = 0;
    }
    return rc;
}

bool TxnInsertAction::ReallocInsertSet()
{
    bool rc = true;
    uint64_t newArraySize = (uint64_t)m_insertArraySize * INSERT_ARRAY_EXTEND_FACTOR;
    // first check for overflow
    if (newArraySize >= UINT32_MAX) {
        MOT_REPORT_ERROR(MOT_ERROR_RESOURCE_LIMIT,
            "Transaction Processing",
            "Failed to reallocate insert set: Insert set size overflow");
        return false;
    }
    uint64_t allocSize = sizeof(InsItem) * newArraySize;
    void* ptr = MemAlloc(allocSize, m_manager->m_global);
    if (__builtin_expect(ptr == nullptr, 0)) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Transaction Processing",
            "Failed to reallocate insert set: Failed to allocate %" PRIu64 " bytes for %" PRIu64 " insert items",
            allocSize,
            newArraySize);
        return false;
    }
    errno_t erc = memset_s(ptr, allocSize, 0, allocSize);
    securec_check(erc, "\0", "\0");
    erc = memcpy_s(ptr, allocSize, static_cast<void*>(m_insertSet), sizeof(InsItem) * m_insertArraySize);
    securec_check(erc, "\0", "\0");
    MemFree(m_insertSet, m_manager->m_global);
    m_insertSet = static_cast<InsItem*>(ptr);
    m_insertArraySize = static_cast<uint32_t>(newArraySize);  // safe cast, limit for type uint32_t was checked
    return rc;
}

void TxnInsertAction::ShrinkInsertSet()
{
    uint64_t new_array_size = INSERT_ARRAY_DEFAULT_SIZE;
    void* ptr = MemAlloc(sizeof(InsItem) * new_array_size, m_manager->m_global);
    if (__builtin_expect(ptr == nullptr, 0)) {
        MOT_LOG_ERROR("%s: failed", __func__);
        return;
    }
    errno_t erc = memcpy_s(
        ptr, sizeof(InsItem) * new_array_size, static_cast<void*>(m_insertSet), sizeof(InsItem) * new_array_size);
    securec_check(erc, "\0", "\0");
    MemFree(m_insertSet, m_manager->m_global);
    m_insertSet = static_cast<InsItem*>(ptr);
    m_insertArraySize = new_array_size;
}

/******************** DDL SUPPORT ********************/
Table* TxnManager::GetTableByExternalId(uint64_t id)
{
    Table* table = GetTxnTable(id);

    if (table == nullptr) {
        return GetTableManager()->GetTableByExternal(id);
    } else {
        return table;
    }
}

Index* TxnManager::GetIndexByExternalId(uint64_t tableId, uint64_t indexId)
{
    Table* table = GetTableByExternalId(tableId);
    if (table == nullptr) {
        return nullptr;
    } else {
        return table->GetIndexByExtId(indexId);
    }
}

Table* TxnManager::GetTxnTable(uint64_t tableId)
{
    return m_txnDdlAccess->GetTxnTable(tableId);
}

RC TxnManager::CreateTxnTable(Table* table, TxnTable*& txnTable)
{
    if (table->IsTxnTable()) {
        txnTable = static_cast<TxnTable*>(table);
        return RC_OK;
    }

    txnTable = (TxnTable*)GetTxnTable(table->GetTableExId());
    if (txnTable != nullptr) {
        return RC_OK;
    }

    TxnTable* tab = new (std::nothrow) MOT::TxnTable(table);
    if (tab == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Create TxnTable", "Failed to allocate memory for DDL Access object");
        return RC_MEMORY_ALLOCATION_ERROR;
    }
    if (!tab->InitTxnTable()) {
        delete tab;
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Init TxnTable", "Failed to allocate memory for DDL Access object");
        return RC_MEMORY_ALLOCATION_ERROR;
    }
    txnTable = tab;
    m_txnDdlAccess->AddTxnTable(txnTable);
    return RC_OK;
}

RC TxnManager::CreateTable(Table* table)
{
    if (!m_txnDdlAccess->HasEnoughSpace()) {
        return RC_TXN_EXCEEDS_MAX_DDLS;
    }

    SetTxnOper(SubTxnOperType::SUB_TXN_DDL);
    size_t serializeRedoSize = table->SerializeRedoSize();
    if (serializeRedoSize > RedoLogWriter::REDO_MAX_TABLE_SERIALIZE_SIZE) {
        MOT_REPORT_ERROR(MOT_ERROR_RESOURCE_LIMIT,
            "Create Table",
            "Table Serialize size %zu exceeds the maximum allowed limit %u",
            serializeRedoSize,
            RedoLogWriter::REDO_MAX_TABLE_SERIALIZE_SIZE);
        return RC_ERROR;
    }

    TxnTable* txnTable = nullptr;
    RC res = CreateTxnTable(table, txnTable);
    if (res != RC_OK) {
        return res;
    }
    txnTable->SetIsNew();

    TxnDDLAccess::DDLAccess* ddl_access =
        new (std::nothrow) TxnDDLAccess::DDLAccess(txnTable->GetTableExId(), DDL_ACCESS_CREATE_TABLE, (void*)txnTable);
    if (ddl_access == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Create Table", "Failed to allocate memory for DDL Access object");
        return RC_MEMORY_ALLOCATION_ERROR;
    }

    (void)m_txnDdlAccess->Add(ddl_access);
    return RC_OK;
}

RC TxnManager::DropTable(Table* table)
{
    if (!m_txnDdlAccess->HasEnoughSpace()) {
        return RC_TXN_EXCEEDS_MAX_DDLS;
    }

    TxnTable* txnTable = nullptr;
    RC res = CreateTxnTable(table, txnTable);
    if (res != RC_OK) {
        return res;
    }

    SetTxnOper(SubTxnOperType::SUB_TXN_DDL);
    // we allocate all memory before action takes place, so that if memory allocation fails, we can report error safely
    TxnDDLAccess::DDLAccess* new_ddl_access =
        new (std::nothrow) TxnDDLAccess::DDLAccess(table->GetTableExId(), DDL_ACCESS_DROP_TABLE, (void*)txnTable);
    if (new_ddl_access == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Drop Table", "Failed to allocate DDL Access object");
        return RC_MEMORY_ALLOCATION_ERROR;
    }

    if (!m_isLightSession) {
        rowAddrMap_t rows;
        TxnOrderedSet_t& access_row_set = m_accessMgr->GetOrderedRowSet();
        TxnOrderedSet_t::iterator it = access_row_set.begin();
        while (it != access_row_set.end()) {
            Access* ac = it->second;
            if (ac->GetTxnRow()->GetOrigTable() == table->GetOrigTable()) {
                if (ac->m_type == INS) {
                    RollbackInsert(ac);
                    // Row is local and was not inserted in the commit
                    if (ac->m_params.IsPrimarySentinel()) {
                        rows[ac->GetTxnRow()] = ac->GetTxnRow();
                    }
                }
                it = access_row_set.erase(it);
                // need to perform index clean-up!
                m_accessMgr->PubReleaseAccess(ac);
            } else {
                (void)++it;
            }
        }
        for (rowAddrMap_t::iterator itr = rows.begin(); itr != rows.end(); ++itr) {
            itr->second->GetTable()->DestroyRow(itr->second);
        }
    }

    txnTable->SetIsDropped();
    (void)m_txnDdlAccess->Add(new_ddl_access);
    return RC_OK;
}

RC TxnManager::TruncateTable(Table* table)
{
    if (!m_txnDdlAccess->HasEnoughSpace()) {
        return RC_TXN_EXCEEDS_MAX_DDLS;
    }

    SetTxnOper(SubTxnOperType::SUB_TXN_DDL);
    if (m_isLightSession) {
        return RC_OK;
    }

    TxnTable* txnTable = nullptr;
    RC res = CreateTxnTable(table, txnTable);
    if (res != RC_OK) {
        return res;
    }

    rowAddrMap_t rows;
    TxnOrderedSet_t& access_row_set = m_accessMgr->GetOrderedRowSet();
    TxnOrderedSet_t::iterator it = access_row_set.begin();
    while (it != access_row_set.end()) {
        Access* ac = it->second;
        if (ac->GetTxnRow()->GetOrigTable() == table->GetOrigTable()) {
            if (ac->m_type == INS) {
                RollbackInsert(ac);
                // Row is local and was not inserted in the commit
                if (ac->m_params.IsPrimarySentinel()) {
                    rows[ac->GetTxnRow()] = ac->GetTxnRow();
                }
            }
            it = access_row_set.erase(it);
            // need to perform index clean-up!
            m_accessMgr->PubReleaseAccess(ac);
        } else {
            (void)++it;
        }
    }
    for (rowAddrMap_t::iterator itr = rows.begin(); itr != rows.end(); (void)++itr) {
        itr->second->GetTable()->DestroyRow(itr->second);
    }
    // clean all GC elements for the table, this call should actually release
    // all elements into an appropriate object pool
    for (uint16_t i = 0; i < table->GetNumIndexes(); i++) {
        MOT::Index* index = table->GetIndex(i);
        GcManager::ClearIndexElements(index->GetIndexId(), GC_OPERATION_TYPE::GC_OPER_TRUNCATE);
    }

    if (!txnTable->IsHasTruncate()) {
        txnTable->SetHasTruncate();
        // allocate DDL before work and fail immediately if required
        TxnDDLAccess::DDLAccess* ddl_access = new (std::nothrow)
            TxnDDLAccess::DDLAccess(table->GetTableExId(), DDL_ACCESS_TRUNCATE_TABLE, (void*)txnTable);
        if (ddl_access == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Truncate Table", "Failed to allocate memory for DDL Access object");
            return RC_MEMORY_ALLOCATION_ERROR;
        }

        if (!txnTable->InitRowPool(false) or !txnTable->InitTombStonePool(false)) {
            delete ddl_access;
            return RC_MEMORY_ALLOCATION_ERROR;
        }

        res = TruncateIndexes(txnTable);
        if (res != RC_OK) {
            delete ddl_access;
            return res;
        }

        (void)m_txnDdlAccess->Add(ddl_access);
    } else {
        txnTable->SetHasTruncate();
    }

    return res;
}

RC TxnManager::TruncateIndexes(TxnTable* txnTable)
{
    for (uint16_t i = 0; i < txnTable->GetNumIndexes(); i++) {
        MOT::Index* index = txnTable->GetIndex(i);
        if (!index->GetIsCommited()) {
            GcManager::ClearIndexElements(index->GetIndexId());
            RC res = index->Truncate(false);
            if (res != RC_OK) {
                return res;
            }
            continue;
        }
        MOT::Index* index_copy = index->CloneEmpty();
        if (index_copy == nullptr) {
            // print error, could not allocate memory for index
            MOT_REPORT_ERROR(
                MOT_ERROR_OOM, "Truncate Table", "Failed to clone empty index %s", index->GetName().c_str());
            return RC_MEMORY_ALLOCATION_ERROR;
        }
        index_copy->SetTable(txnTable);
        txnTable->m_indexes[i] = index_copy;
        if (i != 0) {  // is secondary
            txnTable->m_secondaryIndexes[index_copy->GetName()] = index_copy;
        } else {  // is primary
            txnTable->m_primaryIndex = index_copy;
        }
    }

    return RC_OK;
}

RC TxnManager::CreateIndex(Table* table, MOT::Index* index, bool isPrimary)
{
    if (!m_txnDdlAccess->HasEnoughSpace()) {
        return RC_TXN_EXCEEDS_MAX_DDLS;
    }

    SetTxnOper(SubTxnOperType::SUB_TXN_DDL);
    size_t serializeRedoSize = table->SerializeItemSize(index);
    if (serializeRedoSize > RedoLogWriter::REDO_MAX_INDEX_SERIALIZE_SIZE) {
        MOT_REPORT_ERROR(MOT_ERROR_RESOURCE_LIMIT,
            "Create Index",
            "Index Serialize size %zu exceeds the maximum allowed limit %u",
            serializeRedoSize,
            RedoLogWriter::REDO_MAX_INDEX_SERIALIZE_SIZE);
        return RC_ERROR;
    }

    TxnTable* txnTable = nullptr;
    RC res = CreateTxnTable(table, txnTable);
    if (res != RC_OK) {
        return res;
    }

    txnTable->SetHasIndexChanges(isPrimary);

    // allocate DDL before work and fail immediately if required
    TxnDDLAccess::DDLAccess* ddl_access =
        new (std::nothrow) TxnDDLAccess::DDLAccess(index->GetExtId(), DDL_ACCESS_CREATE_INDEX, (void*)index);
    if (ddl_access == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Create Index", "Failed to allocate DDL Access object");
        return RC_MEMORY_ALLOCATION_ERROR;
    }

    if (isPrimary) {
        if (!txnTable->UpdatePrimaryIndex(index, this, m_threadId)) {
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
        if (txnTable->GetNumIndexes() >= MAX_NUM_INDEXES) {
            MOT_REPORT_ERROR(MOT_ERROR_RESOURCE_LIMIT,
                "Create Index",
                "Cannot create index in table %s: reached limit of %u indices per table",
                txnTable->GetLongTableName().c_str(),
                (unsigned)MAX_NUM_INDEXES);
            delete ddl_access;
            return RC_TABLE_EXCEEDS_MAX_INDEXES;
        }

        if (!txnTable->AddSecondaryIndex(index->GetName(), index, this, m_threadId)) {
            if (MOT_IS_OOM()) {  // do not report error in "unique violation" scenario
                MOT_REPORT_ERROR(
                    MOT_ERROR_INTERNAL, "Create Index", "Failed to add secondary index %s", index->GetName().c_str());
            }
            delete ddl_access;
            return m_err;
        }
    }
    (void)m_txnDdlAccess->Add(ddl_access);
    return RC_OK;
}

RC TxnManager::DropIndex(MOT::Index* index)
{
    if (!m_txnDdlAccess->HasEnoughSpace()) {
        return RC_TXN_EXCEEDS_MAX_DDLS;
    }

    Table* table = index->GetTable();
    TxnTable* txnTable = nullptr;
    RC res = CreateTxnTable(table, txnTable);
    if (res != RC_OK) {
        return res;
    }

    SetTxnOper(SubTxnOperType::SUB_TXN_DDL);

    // allocate DDL before work and fail immediately if required
    TxnDDLAccess::DDLAccess* new_ddl_access =
        new (std::nothrow) TxnDDLAccess::DDLAccess(index->GetExtId(), DDL_ACCESS_DROP_INDEX, (void*)index);
    if (new_ddl_access == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Drop Index", "Failed to allocate DDL Access object");
        return RC_MEMORY_ALLOCATION_ERROR;
    }

    txnTable->SetHasIndexChanges(true);

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
                (void)++it;
            }
        }
    }

    txnTable->RemoveSecondaryIndexFromMetaData(index);
    (void)m_txnDdlAccess->Add(new_ddl_access);
    return res;
}

RC TxnManager::AlterTableAddColumn(Table* table, Column* newColumn)
{
    if (!m_txnDdlAccess->HasEnoughSpace()) {
        return RC_TXN_EXCEEDS_MAX_DDLS;
    }

    TxnTable* txnTable = nullptr;
    DDLAlterTableAddDropColumn* alterAdd = nullptr;
    TxnDDLAccess::DDLAccess* new_ddl_access = nullptr;
    RC res = CreateTxnTable(table, txnTable);
    if (res != RC_OK) {
        return res;
    }

    SetTxnOper(SubTxnOperType::SUB_TXN_DDL);

    do {
        alterAdd = new (std::nothrow) DDLAlterTableAddDropColumn();
        if (alterAdd == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Add Column", "Failed to allocate alter structure");
            res = RC_MEMORY_ALLOCATION_ERROR;
            break;
        }
        // allocate DDL before work and fail immediately if required
        new_ddl_access =
            new (std::nothrow) TxnDDLAccess::DDLAccess(table->GetTableExId(), DDL_ACCESS_ADD_COLUMN, (void*)alterAdd);
        if (new_ddl_access == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Add Column", "Failed to allocate DDL Access object");
            res = RC_MEMORY_ALLOCATION_ERROR;
            break;
        }
        // clone column, in case we have drop this column later
        alterAdd->m_column = Column::AllocColumn(newColumn->m_type);
        if (alterAdd->m_column == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Add Column", "Failed to allocate column");
            res = RC_MEMORY_ALLOCATION_ERROR;
            break;
        }
        res = alterAdd->m_column->Clone(newColumn);
        if (res != RC_OK) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Add Column", "Failed to clone column");
            break;
        }
        txnTable->SetHasColumnChanges();
        res = txnTable->AlterAddColumn(this, newColumn);
    } while (false);
    if (res != RC_OK) {
        if (new_ddl_access != nullptr) {
            delete new_ddl_access;
        }
        if (alterAdd != nullptr) {
            if (alterAdd->m_column != nullptr) {
                delete alterAdd->m_column;
            }
            delete alterAdd;
        }
    } else {
        alterAdd->m_table = txnTable;
        (void)m_txnDdlAccess->Add(new_ddl_access);
    }

    return res;
}

RC TxnManager::AlterTableDropColumn(Table* table, Column* col)
{
    if (!m_txnDdlAccess->HasEnoughSpace()) {
        return RC_TXN_EXCEEDS_MAX_DDLS;
    }

    TxnDDLAccess::DDLAccess* new_ddl_access = nullptr;
    DDLAlterTableAddDropColumn* alterDrop = nullptr;
    TxnTable* txnTable = nullptr;
    RC res = CreateTxnTable(table, txnTable);
    if (res != RC_OK) {
        return res;
    }

    SetTxnOper(SubTxnOperType::SUB_TXN_DDL);

    do {
        alterDrop = new (std::nothrow) DDLAlterTableAddDropColumn();
        if (alterDrop == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Drop Column", "Failed to allocate alter structure");
            res = RC_MEMORY_ALLOCATION_ERROR;
            break;
        }
        // allocate DDL before work and fail immediately if required
        new_ddl_access =
            new (std::nothrow) TxnDDLAccess::DDLAccess(table->GetTableExId(), DDL_ACCESS_DROP_COLUMN, (void*)alterDrop);
        if (new_ddl_access == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Drop Column", "Failed to allocate DDL Access object");
            res = RC_MEMORY_ALLOCATION_ERROR;
            break;
        }
        // clone column, in case we have drop this column later
        alterDrop->m_column = Column::AllocColumn(col->m_type);
        if (alterDrop->m_column == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Drop Column", "Failed to allocate column");
            res = RC_MEMORY_ALLOCATION_ERROR;
            break;
        }
        res = alterDrop->m_column->Clone(col);
        if (res != RC_OK) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Drop Column", "Failed to clone column");
            break;
        }
        txnTable->SetHasColumnChanges();
        res = txnTable->AlterDropColumn(this, col);
    } while (false);

    if (res != RC_OK) {
        if (new_ddl_access != nullptr) {
            delete new_ddl_access;
        }
        if (alterDrop != nullptr) {
            if (alterDrop->m_column != nullptr) {
                delete alterDrop->m_column;
            }
            delete alterDrop;
        }
    } else {
        alterDrop->m_table = txnTable;
        (void)m_txnDdlAccess->Add(new_ddl_access);
    }

    return res;
}

RC TxnManager::AlterTableRenameColumn(Table* table, Column* col, char* newname)
{
    if (!m_txnDdlAccess->HasEnoughSpace()) {
        return RC_TXN_EXCEEDS_MAX_DDLS;
    }

    TxnTable* txnTable = nullptr;
    RC rc = CreateTxnTable(table, txnTable);
    if (rc != RC_OK) {
        return rc;
    }

    SetTxnOper(SubTxnOperType::SUB_TXN_DDL);

    // clean all GC elements for the table, this call should actually release
    // all elements into an appropriate object pool
    for (uint16_t i = 0; i < table->GetNumIndexes(); i++) {
        MOT::Index* index = table->GetIndex(i);
        GcManager::ClearIndexElements(index->GetIndexId(), GC_OPERATION_TYPE::GC_ALTER_TABLE);
    }

    DDLAlterTableRenameColumn* alterRename = new (std::nothrow) DDLAlterTableRenameColumn();
    if (alterRename == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Rename Column", "Failed to allocate alter structure");
        return RC_MEMORY_ALLOCATION_ERROR;
    }
    // allocate DDL before work and fail immediately if required
    TxnDDLAccess::DDLAccess* new_ddl_access =
        new (std::nothrow) TxnDDLAccess::DDLAccess(table->GetTableExId(), DDL_ACCESS_RENAME_COLUMN, (void*)alterRename);
    if (new_ddl_access == nullptr) {
        delete alterRename;
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Rename Column", "Failed to allocate DDL Access object");
        return RC_MEMORY_ALLOCATION_ERROR;
    }

    txnTable->SetHasColumnRename();
    rc = txnTable->AlterRenameColumn(this, col, newname, alterRename);
    if (rc != RC_OK) {
        delete new_ddl_access;
        delete alterRename;
    } else {
        alterRename->m_table = txnTable;
        (void)m_txnDdlAccess->Add(new_ddl_access);
    }

    return rc;
}
TxnIxColUpdate::TxnIxColUpdate(Table* tab, TxnManager* txn, uint8_t* modBitmap, UpdateIndexColumnType hasIxColUpd)
    : m_txn(txn),
      m_tab(tab),
      m_modBitmap(modBitmap),
      m_arrLen(tab->GetNumIndexes()),
      m_cols(tab->GetFieldCount() - 1),
      m_hasIxColUpd(hasIxColUpd)
{}

TxnIxColUpdate::~TxnIxColUpdate()
{
    if (m_hasIxColUpd) {
        for (uint16_t j = 0; j < m_arrLen; j++) {
            if (m_oldKeys[j] != nullptr) {
                m_ix[j]->DestroyKey(m_oldKeys[j]);
            }
            if (m_newKeys[j] != nullptr) {
                m_ix[j]->DestroyKey(m_newKeys[j]);
            }
        }
    }

    m_txn = nullptr;
    m_tab = nullptr;
    m_modBitmap = nullptr;
}

RC TxnIxColUpdate::InitAndBuildOldKeys(Row* row)
{
    RC rc = RC_OK;

    for (uint64_t i = 0; i < m_cols; i++) {
        if (BITMAP_GET(m_modBitmap, i)) {
            if (m_tab->GetField((i + 1))->IsUsedByIndex()) {
                for (uint16_t j = 0; j < m_arrLen; j++) {
                    if (m_ix[j] == nullptr) {
                        MOT::Index* ix = m_tab->GetIndex(j);
                        if (ix->IsFieldPresent(static_cast<int16_t>(i + 1))) {
                            m_ix[j] = ix;
                        }
                    }
                }
            }
        }
    }
    for (uint16_t j = 0; j < m_arrLen; j++) {
        if (m_ix[j] != nullptr) {
            m_oldKeys[j] = m_ix[j]->CreateNewKey();
            if (m_oldKeys[j] == nullptr) {
                MOT_REPORT_ERROR(MOT_ERROR_OOM,
                    "Insert Row",
                    "Failed to create key for secondary index %s",
                    m_ix[j]->GetName().c_str());
                rc = MOT::RC::RC_MEMORY_ALLOCATION_ERROR;
                break;
            }
            m_ix[j]->BuildKey(m_tab, row, m_oldKeys[j]);
        }
    }
    return rc;
}

RC TxnIxColUpdate::FilterColumnUpdate(Row* row)
{
    RC rc = RC_OK;
    for (uint16_t j = 0; j < m_arrLen; j++) {
        if (m_ix[j] != nullptr) {
            m_newKeys[j] = m_ix[j]->CreateNewKey();
            if (m_newKeys[j] == nullptr) {
                MOT_REPORT_ERROR(MOT_ERROR_OOM,
                    "Insert Row",
                    "Failed to create key for secondary index %s",
                    m_ix[j]->GetName().c_str());
                rc = MOT::RC::RC_MEMORY_ALLOCATION_ERROR;
                break;
            }
            m_ix[j]->BuildKey(m_tab, row, m_newKeys[j]);
            // check if key has changed, can happen in case we update to a same value
            if (memcmp(m_newKeys[j]->GetKeyBuf(), m_oldKeys[j]->GetKeyBuf(), m_ix[j]->GetKeySizeNoSuffix()) == 0) {
                m_ix[j]->DestroyKey(m_oldKeys[j]);
                m_ix[j]->DestroyKey(m_newKeys[j]);
                m_ix[j] = nullptr;
                m_oldKeys[j] = nullptr;
                m_newKeys[j] = nullptr;
            }
        }
    }
    return rc;
}
}  // namespace MOT
