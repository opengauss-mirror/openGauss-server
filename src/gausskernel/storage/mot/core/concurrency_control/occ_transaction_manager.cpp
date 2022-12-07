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
 * occ_transaction_manager.cpp
 *    Optimistic Concurrency Control (OCC) implementation
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/concurrency_control/occ_transaction_manager.h
 *
 * -------------------------------------------------------------------------
 */

#include "occ_transaction_manager.h"
#include "utilities.h"
#include "cycles.h"
#include "mot_engine.h"
#include "row.h"
#include "txn.h"
#include "txn_access.h"
#include "checkpoint_manager.h"
#include "mm_session_api.h"
#include "mot_error.h"
#include <pthread.h>

namespace MOT {
DECLARE_LOGGER(OccTransactionManager, ConcurrenyControl);

OccTransactionManager::OccTransactionManager()
    : m_txnCounter(0),
      m_abortsCounter(0),
      m_writeSetSize(0),
      m_insertSetSize(0),
      m_dynamicSleep(100),
      m_rowsLocked(false),
      m_preAbort(true),
      m_validationNoWait(true),
      m_isTransactionCommited(false)
{}

OccTransactionManager::~OccTransactionManager()
{}

bool OccTransactionManager::PreAbortCheck(TxnManager* txMan, GcMaintenanceInfo& gcMemoryReserve)
{
    TxnAccess* tx = txMan->m_accessMgr;
    TxnOrderedSet_t& orderedSet = tx->GetOrderedRowSet();
    auto itr = orderedSet.begin();
    while (itr != orderedSet.end()) {
        Access* ac = (*itr).second;
        switch (ac->m_type) {
            case WR:
                m_writeSetSize++;
                gcMemoryReserve.m_version_queue++;
                break;
            case DEL:
                m_writeSetSize++;
                if (ac->m_params.IsPrimarySentinel()) {
                    gcMemoryReserve.m_delete_queue++;
                } else {
                    if (ac->m_params.IsIndexUpdate()) {
                        gcMemoryReserve.m_update_column_queue++;
                    }
                }
                gcMemoryReserve.m_generic_queue++;
                break;
            case INS:
                m_insertSetSize++;
                m_writeSetSize++;
                if (ac->m_params.IsUpgradeInsert()) {
                    gcMemoryReserve.m_version_queue++;
                }
                break;
            case RD_FOR_UPDATE:
            case RD:
                itr = orderedSet.erase(itr);
                txMan->m_accessMgr->PubReleaseAccess(ac);
                continue;
            default:
                break;
        }

        if (m_preAbort) {
            if (!QuickHeaderValidation(ac)) {
                if (MOTEngine::GetInstance()->IsRecovering() && ResolveRecoveryOccConflict(txMan, ac) == RC_OK) {
                    (void)++itr;
                    continue;
                }
                return false;
            }
        }
        (void)++itr;
    }
    return true;
}

bool OccTransactionManager::QuickVersionCheck(const Access* access)
{
    if (access->m_params.IsPrimarySentinel()) {
        // For Upgrade IOD - Verify the sentinel snapshot is still valid
        // Check if the key is still visible
        if (access->m_snapshot <= access->m_origSentinel->GetStartCSN()) {
            return false;
        }
        MOT_ASSERT(access->m_csn == access->m_globalRow->GetCommitSequenceNumber());
        return (access->m_csn == access->m_origSentinel->GetData()->GetCommitSequenceNumber());
    } else {
        if (access->m_params.IsSecondaryUniqueSentinel()) {
            PrimarySentinelNode* node = static_cast<SecondarySentinelUnique*>(access->m_origSentinel)->GetTopNode();
            if (node->GetEndCSN() != Sentinel::SENTINEL_INIT_CSN) {
                return false;
            }
            return (access->m_secondaryUniqueNode == node);
        } else {
            MOT_ASSERT(access->GetType() == AccessType::DEL);
            // Check that the Sentinel is not deleted!
            return (static_cast<SecondarySentinel*>(access->GetSentinel())->GetEndCSN() == Sentinel::SENTINEL_INIT_CSN);
        }
    }
}

bool OccTransactionManager::QuickInsertCheck(const Access* access)
{
    // Lets verify the inserts
    Sentinel* sent = access->m_origSentinel;
    if (access->m_params.IsUpgradeInsert() == false) {
        // if the sent is committed we abort!
        if (sent->IsCommited()) {
            return false;
        }
    } else {
        if (access->m_params.IsPrimarySentinel()) {
            // For Upgrade IOD - Verify the sentinel snapshot is still valid
            if (access->m_params.IsInsertOnDeletedRow()) {
                if (access->m_origSentinel->GetData()->IsRowDeleted() == false) {
                    return false;
                }
            }
            return (access->m_csn == access->m_origSentinel->GetData()->GetCommitSequenceNumber());
        } else {
            if (access->m_params.IsSecondaryUniqueSentinel()) {
                PrimarySentinelNode* node = static_cast<SecondarySentinelUnique*>(access->m_origSentinel)->GetTopNode();
                if (access->m_params.IsInsertOnDeletedRow()) {
                    // Check if node is deleted
                    if (node->GetEndCSN() == Sentinel::SENTINEL_INIT_CSN) {
                        return false;
                    }
                } else {
                    // Check if node is committed
                    if (node->GetEndCSN() != Sentinel::SENTINEL_INIT_CSN) {
                        return false;
                    }
                }
                return (access->m_secondaryUniqueNode == node);
            } else {
                MOT_ASSERT(access->GetType() == AccessType::INS);
                MOT_ASSERT(access->m_params.IsIndexUpdate() == true);
                if (static_cast<SecondarySentinel*>(access->GetSentinel())->GetEndCSN() ==
                    Sentinel::SENTINEL_INIT_CSN) {
                    return false;
                }
                return (static_cast<SecondarySentinel*>(access->GetSentinel())->GetStartCSN() == access->m_csn);
            }
        }
    }

    return true;
}

bool OccTransactionManager::QuickHeaderValidation(const Access* access)
{
    if (access->m_type != INS) {
        // For WR/DEL/RD_FOR_UPDATE lets verify CSN
        return QuickVersionCheck(access);
    } else {
        return QuickInsertCheck(access);
    }
}

bool OccTransactionManager::ValidateWriteSet(TxnManager* txMan)
{
    TxnOrderedSet_t& orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
    for (const auto& raPair : orderedSet) {
        const Access* ac = raPair.second;
        if (!QuickHeaderValidation(ac)) {
            return false;
        }
    }
    return true;
}

RC OccTransactionManager::LockHeaders(TxnManager* txMan, uint32_t& numSentinelsLock)
{
    RC rc = RC_OK;
    uint64_t sleepTime = 1;
    uint64_t thdId = txMan->GetThdId();
    TxnOrderedSet_t& orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
    numSentinelsLock = 0;
    if (m_validationNoWait) {
        while (numSentinelsLock != m_writeSetSize) {
            for (const auto& raPair : orderedSet) {
                const Access* ac = raPair.second;
                Sentinel* sent = ac->m_origSentinel;
                if (!sent->TryLock(thdId)) {
                    break;
                }
                numSentinelsLock++;
                // New insert row is already committed!
                // Check if row has changed in sentinel
                if (!QuickHeaderValidation(ac)) {
                    rc = RC_ABORT;
                    goto final;
                }
            }

            if (numSentinelsLock != m_writeSetSize) {
                ReleaseHeaderLocks(txMan, numSentinelsLock);
                numSentinelsLock = 0;
                if (m_preAbort) {
                    for (const auto& acPair : orderedSet) {
                        const Access* ac = acPair.second;
                        if (!QuickHeaderValidation(ac)) {
                            return RC_ABORT;
                        }
                    }
                }
                if (!MOTEngine::GetInstance()->IsRecovering()) {
                    if (sleepTime > LOCK_TIME_OUT) {
                        return RC_ABORT;
                    } else {
                        if (!IsHighContention()) {
                            CpuCyclesLevelTime::Sleep(5);
                        } else {
                            (void)usleep(m_dynamicSleep);
                        }
                        sleepTime = sleepTime << 1;
                    }
                } else {
                    (void)usleep(1000);
                }
            }
        }
    } else {
        for (const auto& raPair : orderedSet) {
            const Access* ac = raPair.second;
            Sentinel* sent = ac->m_origSentinel;
            sent->Lock(thdId);
            numSentinelsLock++;
            // New insert row is already committed!
            // Check if row has changed in sentinel
            if (!QuickHeaderValidation(ac)) {
                rc = RC_ABORT;
                goto final;
            }
        }
    }
final:
    return rc;
}

bool OccTransactionManager::PreAllocStableRow(TxnManager* txMan)
{
    if (GetGlobalConfiguration().m_enableCheckpoint) {
        TxnOrderedSet_t& orderedSet = txMan->m_accessMgr->GetOrderedRowSet();
        for (const auto& raPair : orderedSet) {
            const Access* access = raPair.second;
            if (access->m_type == RD || (access->m_type == INS && access->m_params.IsUpgradeInsert() == false)) {
                continue;
            }
            if (access->m_params.IsPrimarySentinel()) {
                if (!GetCheckpointManager()->PreAllocStableRow(txMan, access->GetRowFromHeader(), access->m_type)) {
                    GetCheckpointManager()->FreePreAllocStableRows(txMan);
                    return false;
                }
            }
        }
    }
    return true;
}

bool OccTransactionManager::ReserveGcMemory(TxnManager* txMan, const GcMaintenanceInfo& gcMemoryReserve)
{
    bool res = true;
    GcManager* gc_manager = txMan->GetGcSession();
    MOT_ASSERT(gc_manager != nullptr);
    res = gc_manager->ReserveGCMemoryPerQueue(GC_QUEUE_TYPE::DELETE_QUEUE, gcMemoryReserve.m_delete_queue);
    if (!res) {
        return false;
    }
    res = gc_manager->ReserveGCMemoryPerQueue(GC_QUEUE_TYPE::VERSION_QUEUE, gcMemoryReserve.m_version_queue);
    if (!res) {
        return false;
    }
    res =
        gc_manager->ReserveGCMemoryPerQueue(GC_QUEUE_TYPE::UPDATE_COLUMN_QUEUE, gcMemoryReserve.m_update_column_queue);
    if (!res) {
        return false;
    }
    res = gc_manager->ReserveGCMemoryPerQueue(GC_QUEUE_TYPE::GENERIC_QUEUE, gcMemoryReserve.m_generic_queue, true);
    if (!res) {
        return false;
    }

    return res;
}

RC OccTransactionManager::ValidateOcc(TxnManager* txMan)
{
    uint32_t numSentinelLock = 0;
    m_rowsLocked = false;
    TxnAccess* txnAccess = txMan->m_accessMgr;
    RC rc = RC_OK;
    const uint32_t rowCount = txnAccess->Size();

    m_writeSetSize = 0;
    m_insertSetSize = 0;
    m_txnCounter++;

    if (rowCount == 0) {
        // READONLY
        return rc;
    }

    GcMaintenanceInfo gcMemoryReserve{};

    MOT_ASSERT(rowCount == txnAccess->GetOrderedRowSet().size());

    do {
        /* 1.Perform pre-abort check and pre-processing */
        if (!PreAbortCheck(txMan, gcMemoryReserve)) {
            rc = RC_ABORT;
            break;
        }

        rc = LockHeaders(txMan, numSentinelLock);
        if (rc != RC_OK) {
            break;
        }

        if (!ValidateWriteSet(txMan)) {
            rc = RC_ABORT;
            break;
        }

        // Pre-allocate stable row according to the checkpoint state.
        if (!PreAllocStableRow(txMan)) {
            rc = RC_MEMORY_ALLOCATION_ERROR;
            break;
        }

        if (!ReserveGcMemory(txMan, gcMemoryReserve)) {
            rc = RC_MEMORY_ALLOCATION_ERROR;
            break;
        }
    } while (0);

    if (likely(rc == RC_OK)) {
        MOT_ASSERT(numSentinelLock == m_writeSetSize);
        m_rowsLocked = true;
    } else {
        ReleaseHeaderLocks(txMan, numSentinelLock);
        if (likely(rc == RC_ABORT)) {
            m_abortsCounter++;
        }
    }

    return rc;
}

RC OccTransactionManager::ResolveRecoveryOccConflict(TxnManager* txMan, Access* access)
{
    Row* row = nullptr;
    RC rc = RC_ABORT;
    uint64_t endCSN = static_cast<uint64_t>(-1);
    MOT_ASSERT(access->m_type == INS);
    switch (access->m_origSentinel->GetIndexOrder()) {
        case IndexOrder::INDEX_ORDER_PRIMARY:
            // Check what is the current row the sentinel is pointing
            row = access->m_origSentinel->GetData();
            if (row) {
                // Row must be deleted with Smaller CSN
                if (row->IsRowDeleted() == false) {
                    MOT_LOG_ERROR("ERROR In Recovery Order!");
                    return RC_ABORT;
                }
                MOT_ASSERT(access->m_origSentinel->GetData()->GetCommitSequenceNumber() > access->m_csn);
                // Reset the global version and set to insert on delete
                access->m_params.SetUpgradeInsert();
                access->m_params.SetInsertOnDeletedRow();
                access->m_globalRow = row;
                access->m_csn = row->GetCommitSequenceNumber();
                access->m_snapshot = static_cast<uint64_t>(-1);
                rc = RC_OK;
            } else {
                MOT_ASSERT(false);
                return RC_ABORT;
            }
            break;
        case IndexOrder::INDEX_ORDER_SECONDARY:
            endCSN = static_cast<SecondarySentinel*>(access->m_origSentinel)->GetEndCSN();
            if (txMan->GetCommitSequenceNumber() <= endCSN) {
                MOT_LOG_ERROR("ERROR In Recovery Order!");
                return RC_ABORT;
            }
            if (endCSN == Sentinel::SENTINEL_INIT_CSN) {
                MOT_LOG_ERROR("ERROR In Recovery Order!");
                return RC_ABORT;
            }
            access->m_params.SetUpgradeInsert();
            access->m_params.SetInsertOnDeletedRow();
            access->m_csn = static_cast<SecondarySentinel*>(access->m_origSentinel)->GetStartCSN();
            rc = RC_OK;
            break;
        case IndexOrder::INDEX_ORDER_SECONDARY_UNIQUE:
            PrimarySentinelNode* node = static_cast<SecondarySentinelUnique*>(access->m_origSentinel)->GetTopNode();
            if (node != nullptr) {
                if (txMan->GetCommitSequenceNumber() <= node->GetEndCSN()) {
                    MOT_LOG_ERROR("ERROR In Recovery Order!");
                    return RC_ABORT;
                }
            }
            // Reset Visible node
            access->m_params.SetUpgradeInsert();
            if (node->GetEndCSN() < Sentinel::SENTINEL_INIT_CSN) {
                access->m_params.SetInsertOnDeletedRow();
            } else {
                MOT_LOG_ERROR("ERROR In Recovery Order!");
                return RC_ABORT;
            }
            access->m_secondaryUniqueNode = node;
            rc = RC_OK;
            break;
    }
    return rc;
}
void OccTransactionManager::WriteChanges(TxnManager* txMan)
{
    if (m_writeSetSize == 0 && m_insertSetSize == 0) {
        return;
    }

    MOTConfiguration& cfg = GetGlobalConfiguration();
    uint64_t commit_csn = txMan->GetCommitSequenceNumber();
    uint64_t transaction_id = txMan->GetInternalTransactionId();
    TxnOrderedSet_t& orderedSet = txMan->m_accessMgr->GetOrderedRowSet();

    // Stable rows for checkpoint needs to be created (copied from original row) before modifying the global rows.
    if (cfg.m_enableCheckpoint) {
        for (const auto& raPair : orderedSet) {
            const Access* access = raPair.second;
            if (access->m_type == RD) {
                continue;
            }
            if (access->m_params.IsPrimarySentinel()) {
                // Pass the actual global row (access->GetRowFromHeader()), so that the stable row will have the
                // same CSN, rowid, etc as the original row before the modifications are applied.
                GetCheckpointManager()->ApplyWrite(txMan, access->GetRowFromHeader(), access);
            }
        }
    }

    // Update CSN with all relevant information on global rows
    // For deletes invalidate sentinels - rows still locked!
    for (const auto& raPair : orderedSet) {
        Access* access = raPair.second;
        access->WriteGlobalChanges(commit_csn, transaction_id);
    }

    WriteSentinelChanges(txMan);

    // For Recovery operation:Update transactionID
    for (const auto& raPair : orderedSet) {
        const Access* access = raPair.second;
        if (access->m_type == RD) {
            continue;
        }
        if (access->m_params.IsPrimarySentinel()) {
            static_cast<PrimarySentinel*>(access->m_origSentinel)->SetTransactionId(transaction_id);
        }
    }

    m_isTransactionCommited = true;
}

void OccTransactionManager::WriteSentinelChanges(TxnManager* txMan)
{
    uint64_t commit_csn = txMan->GetCommitSequenceNumber();
    S_SentinelNodePool* sentinelObjectPool = txMan->m_accessMgr->GetSentinelObjectPool();
    TxnOrderedSet_t& orderedSet = txMan->m_accessMgr->GetOrderedRowSet();

    if (m_insertSetSize > 0) {
        for (const auto& raPair : orderedSet) {
            Access* access = raPair.second;
            if (access->m_type != INS) {
                continue;
            }
            MOT_ASSERT(access->m_origSentinel->IsLocked() == true);
            if (access->m_params.IsUpgradeInsert() == false) {
                if (access->m_params.IsPrimarySentinel()) {
                    MOT_ASSERT(access->m_origSentinel->IsDirty() == true);
                    MOT_ASSERT(access->m_origSentinel->IsLocked() == true);
                    // Connect row and sentinel, row is set to absent and locked
                    access->m_origSentinel->SetStartCSN(commit_csn);
                    access->m_origSentinel->SetNextPtr(access->GetLocalInsertRow());
                    // Current state: row is set to absent,sentinel is locked and not dirty
                    // Readers will not see the row
                    COMPILER_BARRIER;
                    access->m_origSentinel->SetWriteBit();
                    access->GetTxnRow()->GetTable()->UpdateRowCount(1);
                } else {
                    if (access->m_params.IsSecondaryUniqueSentinel() == false) {
                        // We only set the in the secondary sentinel!
                        access->m_origSentinel->SetStartCSN(commit_csn);
                        access->m_origSentinel->SetEndCSN(Sentinel::SENTINEL_INIT_CSN);
                        access->m_origSentinel->SetNextPtr(access->GetLocalInsertRow()->GetPrimarySentinel());
                    } else {
                        access->m_origSentinel->SetStartCSN(commit_csn);
                        auto object = sentinelObjectPool->find(access->m_origSentinel->GetIndex());
                        PrimarySentinelNode* node = object->second;
                        (void)sentinelObjectPool->erase(object);
                        node->Init(
                            commit_csn, Sentinel::SENTINEL_INIT_CSN, access->GetLocalInsertRow()->GetPrimarySentinel());
                        access->m_origSentinel->SetNextPtr(node);
                    }
                }
                // Unset Dirty - still locked!
                access->m_origSentinel->UnSetDirty();
            } else {
                if (access->m_params.IsInsertOnDeletedRow()) {
                    if (access->m_params.IsPrimarySentinel()) {
                        access->GetTxnRow()->GetTable()->UpdateRowCount(1);
                        access->m_origSentinel->SetNextPtr(access->GetLocalInsertRow());
                        COMPILER_BARRIER;
                        access->m_origSentinel->SetWriteBit();
                    } else if (access->m_params.IsUniqueIndex() == true) {
                        auto object = sentinelObjectPool->find(access->m_origSentinel->GetIndex());
                        PrimarySentinelNode* node = object->second;
                        (void)sentinelObjectPool->erase(object);
                        node->Init(
                            commit_csn, Sentinel::SENTINEL_INIT_CSN, access->GetLocalInsertRow()->GetPrimarySentinel());
                        auto oldNode = static_cast<SecondarySentinelUnique*>(access->m_origSentinel)->GetTopNode();
                        node->SetNextVersion(oldNode);
                        access->m_origSentinel->SetNextPtr(node);
                    } else {
                        MOT_ASSERT(access->m_params.IsIndexUpdate());
                        // Revalidate End CSN
                        static_cast<SecondarySentinel*>(access->m_origSentinel)->SetEndCSN(Sentinel::SENTINEL_INIT_CSN);
                    }
                } else {
                    // We delete row internally and insert a new row
                    if (access->m_params.IsPrimarySentinel()) {
                        access->m_origSentinel->SetNextPtr(access->GetLocalInsertRow());
                        COMPILER_BARRIER;
                        access->m_origSentinel->SetWriteBit();
                    } else {
                        auto object = sentinelObjectPool->find(access->m_origSentinel->GetIndex());
                        PrimarySentinelNode* node = object->second;
                        (void)sentinelObjectPool->erase(object);
                        node->Init(
                            commit_csn, Sentinel::SENTINEL_INIT_CSN, access->GetLocalInsertRow()->GetPrimarySentinel());
                        auto oldNode = static_cast<SecondarySentinelUnique*>(access->m_origSentinel)->GetTopNode();
                        oldNode->SetEndCSN(commit_csn);
                        node->SetNextVersion(oldNode);
                        access->m_origSentinel->SetNextPtr(node);
                    }
                }
            }
        }
    }
}

void OccTransactionManager::ReleaseHeaderLocks(TxnManager* txMan, uint32_t numOfLocks)
{
    if (numOfLocks == 0) {
        return;
    }

    TxnAccess* tx = txMan->m_accessMgr;
    TxnOrderedSet_t& orderedSet = tx->GetOrderedRowSet();
    // use local counter to optimize
    for (const auto& raPair : orderedSet) {
        const Access* access = raPair.second;
        if (access->m_type == RD) {
            continue;
        } else {
            numOfLocks--;
            access->m_origSentinel->Unlock();
        }
        if (!numOfLocks) {
            break;
        }
    }
}

void OccTransactionManager::CleanUp()
{
    m_writeSetSize = 0;
    m_insertSetSize = 0;
    m_isTransactionCommited = false;
}
}  // namespace MOT
