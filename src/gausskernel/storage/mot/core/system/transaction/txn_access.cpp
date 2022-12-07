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
 * txn_access.cpp
 *    Cache manager for current transaction.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/transaction/txn_access.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <map>
#include <iostream>
#include <iomanip>
#include "table.h"
#include "row.h"
#include "txn.h"
#include "txn_access.h"
#include "txn_insert_action.h"
#include "mot_engine.h"

namespace MOT {
IMPLEMENT_CLASS_LOGGER(TxnInsertAction, TxMan);
IMPLEMENT_CLASS_LOGGER(TxnAccess, TxMan);

const char* const TxnAccess::enTxnStates[] = {stringify(INV),
    stringify(RD),
    stringify(RD_FOR_UPDATE),
    stringify(WR),
    stringify(DEL),
    stringify(INS),
    stringify(SCAN),
    stringify(TEST)};

TxnAccess::TxnAccess()
    : m_rowCnt(0),
      m_accessesSetBuff(nullptr),
      m_txnManager(nullptr),
      m_insertManager(nullptr),
      m_lastAcc(nullptr),
      m_initPhase(Startup)
{}

TxnAccess::~TxnAccess()
{
    switch (m_initPhase) {
        case Done:
            ClearAccessSet();

        // fall through
        case CreateRowSet:
            if (m_rowsSet) {
                delete m_rowsSet;
            }
            if (m_insertManager) {
                delete m_insertManager;
            }
            if (m_sentinelObjectPool) {
                delete m_sentinelObjectPool;
            }
        // fall through
        case CreateRowZero:
            if (m_rowZero) {
                MemFree(m_rowZero, m_txnManager->m_global);
            }
        // fall through
        case InitDummyTab:
        // fall through
        case AllocBuf:
            if (m_accessesSetBuff) {
                MemFree(m_accessesSetBuff, m_txnManager->m_global);
            }
        // fall through
        case Startup:
        default:
            break;
    }

    if (m_accessPool) {
        ObjAllocInterface::FreeObjPool(&m_accessPool);
        m_accessPool = nullptr;
    }

    m_tableStat.clear();

    m_txnManager = nullptr;
    m_insertManager = nullptr;
    m_accessesSetBuff = nullptr;
    m_sentinelObjectPool = nullptr;
    m_rowsSet = nullptr;
    m_rowZero = nullptr;
    m_lastAcc = nullptr;
}

void TxnAccess::ClearAccessSet()
{
    for (unsigned int i = 0; i < m_accessSetSize; i++) {
        Access* ac = GetAccessPtr(i);
        if (ac != nullptr) {
            DestroyAccess(ac);
            ReleaseAccessToPool(ac);
        } else {
            break;
        }
    }

    m_allocatedAc = 0;
    MemFree(m_accessesSetBuff, m_txnManager->m_global);
    m_accessesSetBuff = nullptr;
}

bool TxnAccess::Init(TxnManager* manager)
{
    m_initPhase = Startup;
    SessionId sessionId =
        manager->GetSessionContext() ? manager->GetSessionContext()->GetSessionId() : INVALID_SESSION_ID;

    // allocate buffer
    m_txnManager = manager;
    uint32_t alloc_size = sizeof(Access*) * m_accessSetSize;
    void* ptr = MemAlloc(alloc_size, m_txnManager->m_global);
    if (ptr == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Transaction Initialization",
            "Failed to allocate %u bytes aligned to 64 bytes for TxnAccess in session %u",
            alloc_size,
            sessionId);
        return false;
    }
    m_initPhase = AllocBuf;

    m_accessesSetBuff = static_cast<Access**>(ptr);
    for (uint64_t idx = 0; idx < m_accessSetSize; idx++) {
        m_accessesSetBuff[idx] = nullptr;
    }

    // initialize dummy table
    if (!m_dummyTable.Init(m_txnManager->m_isRecoveryTxn, m_txnManager->m_global)) {
        MOT_REPORT_ERROR(
            MOT_ERROR_OOM, "Transaction Initialization", "Failed to initialize dummy table for session %u", sessionId);
        return false;
    }
    m_initPhase = InitDummyTab;

    // create row zero
    ptr = MemAlloc(sizeof(Row) + MAX_TUPLE_SIZE, m_txnManager->m_global);
    if (!ptr) {
        MOT_REPORT_ERROR(
            MOT_ERROR_OOM, "Transaction Initialization", "Failed to allocate row zero for session %u", sessionId);
        return false;
    }
    m_rowZero = new (ptr) Row(nullptr);
    m_rowZero->m_rowType = RowType::ROW_ZERO;
    m_initPhase = CreateRowZero;

    // create rows set
    m_rowsSet = new (std::nothrow) TxnOrderedSet_t();
    if (m_rowsSet == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Transaction Initialization",
            "Failed to allocate row set for session %u",
            manager->GetSessionContext()->GetSessionId());
        return false;
    }
    m_sentinelObjectPool = new (std::nothrow) S_SentinelNodePool();
    if (m_sentinelObjectPool == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Transaction Initialization",
            "Failed to allocate sentinel node set for session %u",
            manager->GetSessionContext()->GetSessionId());
        return false;
    }

    m_initPhase = CreateInsertSet;
    m_insertManager = new (std::nothrow) TxnInsertAction();
    if (m_insertManager == nullptr) {
        MOT_REPORT_ERROR(
            MOT_ERROR_OOM, "Transaction Initialization", "Failed to allocate Insert set for session %u", sessionId);
        return false;
    }

    if (!m_insertManager->Init(manager)) {
        MOT_REPORT_ERROR(
            MOT_ERROR_OOM, "Transaction Initialization", "Failed to INIT Insert set for session %u", sessionId);
        return false;
    }
    m_initPhase = CreateAccessPool;
    m_accessPool = ObjAllocInterface::GetObjPool(sizeof(Access), !manager->m_global);
    if (!m_accessPool) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Initialize ObjectPool",
            "Failed to allocate Access pool for session %d",
            manager->GetSessionContext()->GetSessionId());
        return false;
    }
    m_initPhase = Done;
    return true;
}

void TxnAccess::ClearSet()
{
    m_lastAcc = nullptr;
    MOT_ASSERT(m_rowCnt == m_rowsSet->size());
    m_rowsSet->clear();
    if (unlikely(m_accessSetSize > DEFAULT_ACCESS_SIZE)) {
        ShrinkAccessSet();
    } else {
        for (unsigned int i = 0; i < m_rowCnt; i++) {
            DestroyAccess(m_accessesSetBuff[i]);
        }
        m_rowCnt = 0;
    }

    m_sentinelObjectPool->clear();
    m_insertManager->ClearSet();
}

void TxnAccess::DestroyAccess(Access* access)
{
    if (access->m_localRow != nullptr) {
        if (access->m_params.IsPrimarySentinel()) {
            Row* row = access->m_localRow;
            Table* table = row->GetTable();
            table->DestroyRow(row);
        }
        access->m_localRow = nullptr;
    }

    if (access->m_params.IsPrimarySentinel()) {
        if (access->m_modifiedColumns.IsInitialized()) {
            m_dummyTable.DestroyBitMapBuffer(access->m_modifiedColumns.GetData(), access->m_modifiedColumns.GetSize());
            access->m_modifiedColumns.Reset();
        }
    } else {
        if (access->m_secondaryDelKey != nullptr) {
            access->m_origSentinel->GetIndex()->DestroyKey(access->m_secondaryDelKey);
            access->m_secondaryDelKey = nullptr;
        }
    }

    if (access->m_type == INS and access->m_secondaryUniqueNode) {
        auto object = m_sentinelObjectPool->find(access->m_origSentinel->GetIndex());
        if (object != m_sentinelObjectPool->end()) {
            PrimarySentinelNode* node = object->second;
            (void)m_sentinelObjectPool->erase(object);
            access->m_origSentinel->GetIndex()->SentinelNodeRelease(node);
        }
        access->m_secondaryUniqueNode = nullptr;
    }
    access->m_origSentinel = nullptr;
    access->m_type = AccessType::INV;
}

void TxnAccess::ReleaseSubTxnAccesses(uint32_t accessId)
{
    Access* ac = nullptr;
    uint32_t endIdx = m_rowCnt;
    if (m_accessesSetBuff[accessId] == nullptr) {
        return;
    }
    MOT_LOG_DEBUG("Rollback Access range %d to %d ", accessId, endIdx);
    for (uint32_t startIdx = accessId; startIdx < endIdx; startIdx++) {
        ac = m_accessesSetBuff[startIdx];
        auto it = m_rowsSet->find(ac->m_origSentinel);
        MOT_ASSERT(it != m_rowsSet->end());
        (void)m_rowsSet->erase(it);
        DestroyAccess(ac);
        m_rowCnt--;
    }
}

bool TxnAccess::ReallocAccessSet()
{
    bool rc = true;
    uint64_t new_array_size = m_accessSetSize * ACCESS_SET_EXTEND_FACTOR;
    if (new_array_size >= UINT32_MAX) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Transaction Execution",
            "Txn crossed limit of 32bit elements %lu in session %u (while reallocating)",
            new_array_size,
            m_txnManager->GetSessionContext()->GetSessionId());
        SetLastError(MOT_ERROR_OOM, MOT_SEVERITY_ERROR);
        return false;
    }

    // Reserve memory for rollback
    rc = m_txnManager->GetGcSession()->ReserveGCRollbackMemory(static_cast<uint32_t>(new_array_size));
    if (!rc) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Transaction Execution",
            "GC Failed to reserve %d elements in session %u (while reallocating)",
            m_accessSetSize,
            m_txnManager->GetSessionContext()->GetSessionId());
        SetLastError(MOT_ERROR_OOM, MOT_SEVERITY_ERROR);
        return rc;
    }
    MOT_LOG_DEBUG("Increasing  Access Size! from %d to %lu", m_accessSetSize, new_array_size);

    uint64_t alloc_size = sizeof(Access*) * new_array_size;
    void* ptr = MemAlloc(alloc_size, m_txnManager->m_global);
    if (ptr == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Transaction Execution",
            "Failed to allocate %lu bytes aligned to 64 bytes for TxnAccess in session %u (while reallocating)",
            alloc_size,
            m_txnManager->GetSessionContext()->GetSessionId());
        SetLastError(MOT_ERROR_OOM, MOT_SEVERITY_ERROR);
        rc = false;
    } else {
        errno_t erc = memset_s(ptr, sizeof(Access*) * new_array_size, 0, sizeof(Access*) * new_array_size);
        securec_check(erc, "\0", "\0");
        erc = memcpy_s(ptr, sizeof(Access*) * new_array_size, m_accessesSetBuff, sizeof(Access*) * m_accessSetSize);
        securec_check(erc, "\0", "\0");
        MemFree(m_accessesSetBuff, m_txnManager->m_global);
        SetAccessesSet(reinterpret_cast<Access**>(ptr));
        m_accessSetSize = new_array_size;
    }
    return rc;
}

void TxnAccess::ShrinkAccessSet()
{
    errno_t erc;
    uint64_t new_array_size = DEFAULT_ACCESS_SIZE;
    // Clear access set
    for (unsigned int i = 0; i < m_allocatedAc; i++) {
        DestroyAccess(m_accessesSetBuff[i]);
        ReleaseAccessToPool(m_accessesSetBuff[i]);
    }

    m_allocatedAc = 0;
    m_rowCnt = 0;
    if (new_array_size < m_accessSetSize) {
        m_dummyTable.ClearRowCache();
        m_accessPool->ClearFreeCache();
    }

    MOT_LOG_DEBUG("Shrinking Access Size! from %d to %lu", m_accessSetSize, new_array_size);

    uint32_t alloc_size = sizeof(Access*) * new_array_size;
    void* ptr = MemAlloc(alloc_size, m_txnManager->m_global);
    if (ptr == nullptr) {
        MOT_LOG_ERROR("Failed to allocate %u bytes aligned to 64 bytes for TxnAccess in session %u (while shrinking)",
            alloc_size,
            m_txnManager->GetSessionContext()->GetSessionId());
        return;
    }

    erc = memset_s(ptr, alloc_size, 0, sizeof(Access*) * new_array_size);
    securec_check(erc, "\0", "\0");
    MemFree(m_accessesSetBuff, m_txnManager->m_global);
    SetAccessesSet(reinterpret_cast<Access**>(ptr));
    m_accessSetSize = new_array_size;
}

Access* TxnAccess::GetNewInsertAccess(Sentinel* const& sentinel, Row* row, RC& rc, bool isUpgrade)
{
    rc = RC_OK;
    Row* oldVersion = nullptr;
    PrimarySentinelNode* node = nullptr;
    uint64_t primary_csn = 0;
    uint64_t csn = m_txnManager->GetVisibleCSN();
    if (isUpgrade) {
        switch (sentinel->GetIndexOrder()) {
            case IndexOrder::INDEX_ORDER_PRIMARY:
                // Guard the sentinel from reclamation when validating the row
                static_cast<PrimarySentinel*>(sentinel)->GetGcInfo().Lock();
                oldVersion = sentinel->GetData();
                MOT_ASSERT(oldVersion != nullptr);
                if (oldVersion->IsRowDeleted() == false) {
                    rc = RC_UNIQUE_VIOLATION;
                    static_cast<PrimarySentinel*>(sentinel)->GetGcInfo().Release();
                    return nullptr;
                }
                primary_csn = oldVersion->GetCommitSequenceNumber();
                static_cast<PrimarySentinel*>(sentinel)->GetGcInfo().Release();
                break;
            case IndexOrder::INDEX_ORDER_SECONDARY_UNIQUE:
                node = static_cast<SecondarySentinelUnique*>(sentinel)->GetTopNode();
                if (node->GetEndCSN() >= csn) {
                    rc = RC_UNIQUE_VIOLATION;
                    return nullptr;
                }
                // Use for validation
                primary_csn = node->GetStartCSN();
                break;
            case IndexOrder::INDEX_ORDER_SECONDARY:
                SecondarySentinel* s = static_cast<SecondarySentinel*>(sentinel);
                if (s->GetEndCSN() >= csn) {
                    rc = RC_UNIQUE_VIOLATION;
                    return nullptr;
                }
                primary_csn = s->GetStartCSN();
                break;
        }
    }

    // Needed for doing ApplyAddIndexFromOtherTxn
    Table* tab = m_txnManager->GetTxnTable(row->GetTable()->GetTableExId());
    if (likely(tab == nullptr)) {
        tab = row->GetTable();
        MOT_ASSERT(tab != nullptr);
    }

    Access* ac = GetAccessPtr(m_rowCnt);
    if (unlikely(ac == nullptr)) {
        if (!CreateNewAccess(INS)) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Access Row", "Failed to create new access entry");
            rc = RC::RC_MEMORY_ALLOCATION_ERROR;
            return nullptr;
        }
        ac = GetAccessPtr(m_rowCnt);
    }

    ac->ResetUsedParameters();

    if (isUpgrade) {
        ac->m_globalRow = oldVersion;
        ac->m_csn = primary_csn;
        ac->m_snapshot = m_txnManager->GetVisibleCSN();
    }

    ac->m_params.UnsetRowCommited();
    ac->m_localInsertRow = row;
    ac->m_secondaryUniqueNode = node;
    ac->m_type = INS;
    MOT_LOG_DEBUG("Row Count = %d, access_set_size = %d", m_rowCnt, m_accessSetSize);
    m_rowCnt++;
    return ac;
}

Access* TxnAccess::GetNewSecondaryAccess(Sentinel* const& originalSentinel, Access* primary_access, RC& rc)
{
    // Create new Access
    Access* ac = GetAccessPtr(m_rowCnt);
    if (unlikely(ac == nullptr)) {
        if (!CreateNewAccess(primary_access->m_type)) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Access Row", "Failed to create new access entry");
            rc = RC::RC_MEMORY_ALLOCATION_ERROR;
            return nullptr;
        }
        ac = GetAccessPtr(m_rowCnt);
    }
    ac->ResetUsedParameters();

    ac->m_origSentinel = originalSentinel;
    ac->m_globalRow = primary_access->GetGlobalVersion();
    ac->m_params.SetRowCommited();
    ac->m_params.SetSecondarySentinel();
    ac->m_type = primary_access->m_type;
    if (ac->m_type == WR or ac->m_type == DEL) {
        ac->m_localRow = primary_access->GetLocalVersion();
    }
    ac->m_stmtCount = m_txnManager->GetStmtCount();
    ac->m_redoStmt = ac->m_stmtCount;
    MOT_LOG_DEBUG("Row Count = %d, access_set_size = %d", m_rowCnt, m_accessSetSize);
    m_rowCnt++;
    return ac;
}

Access* TxnAccess::GetNewRowAccess(Sentinel* const& originalSentinel, AccessType type, RC& rc)
{
    rc = RC_OK;
    PrimarySentinel* ps = GetVisiblePrimarySentinel(originalSentinel, m_txnManager->GetVisibleCSN());
    if (ps == nullptr) {
        /* Key is not Visible from Secondary index */
        return nullptr;
    }

    // Extract the visible row MVCC!
    Row* row = GetVisibleRow(ps, type, rc);
    if (row == nullptr) {
        return nullptr;
    }

    Access* ac = GetAccessPtr(m_rowCnt);
    if (unlikely(ac == nullptr)) {
        if (!CreateNewAccess(type)) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Access Row", "Failed to create new access entry");
            rc = RC::RC_MEMORY_ALLOCATION_ERROR;
            return nullptr;
        }
        ac = GetAccessPtr(m_rowCnt);
    }
    ac->ResetUsedParameters();

    ac->m_origSentinel = ps;
    ac->m_globalRow = row;

    ac->m_params.SetRowCommited();
    ac->m_csn = row->GetCommitSequenceNumber();
    ac->m_snapshot = m_txnManager->GetVisibleCSN();
    ac->m_type = type;
    MOT_LOG_DEBUG("Row Count = %d, access_set_size = %d", m_rowCnt, m_accessSetSize);
    m_rowCnt++;
    return ac;
}

bool TxnAccess::CreateNewAccess(AccessType type)
{
    // row_cnt should be smaller than the array size
    if (unlikely(m_rowCnt >= (m_accessSetSize - 1))) {
        bool rc = ReallocAccessSet();
        if (!rc) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Create Access", "Failed to reallocate access set");
            return rc;
        }
    }
    m_accessesSetBuff[m_rowCnt] = AllocNewAccess(m_rowCnt);
    Access* ac = GetAccessPtr(m_rowCnt);
    if (__builtin_expect(ac == nullptr, 0)) {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL, "Create Access", "Failed to get access pointer");
        return false;
    }
    m_allocatedAc++;
    return true;
}

Access* TxnAccess::RowLookup(void* const currentKey)
{
    Access* current_access = nullptr;
    auto search = m_rowsSet->find(currentKey);
    // Key found!
    if (search != m_rowsSet->end()) {
        current_access = search->second;
        // Set Last access
        SetLastAccess(current_access);
        return current_access;
    }
    SetLastAccess(nullptr);
    return nullptr;
}

RC TxnAccess::CheckDuplicateInsert(Sentinel* const sentinel)
{
    if (m_rowCnt == 0) {
        return RC::RC_LOCAL_ROW_NOT_FOUND;
    }
    // type is the external operation.. for access the operation is always RD
    Access* curr_acc = nullptr;
    curr_acc = RowLookup(sentinel);
    if (curr_acc != nullptr) {
        // Filter rows
        switch (curr_acc->m_type) {
            case AccessType::RD:
            case AccessType::RD_FOR_UPDATE:
            case AccessType::WR:
            case AccessType::INS:
                break;
            case AccessType::DEL:
                return RC::RC_LOCAL_ROW_DELETED;
            default:
                break;
        }
    } else {
        return RC::RC_LOCAL_ROW_NOT_FOUND;
    }

    return RC::RC_LOCAL_ROW_FOUND;
}

RC TxnAccess::AccessLookup(const AccessType type, Sentinel* const originalSentinel, Row*& r_local_Row)
{
    if (m_rowCnt == 0) {
        return RC::RC_LOCAL_ROW_NOT_FOUND;
    }
    // type is the external operation.. for access the operation is always RD
    Access* curr_acc = nullptr;

    bool isCommitted = originalSentinel->IsCommited();
    IndexOrder order = originalSentinel->GetIndexOrder();
    if (!isCommitted) {
        curr_acc = RowLookup(originalSentinel);
    } else {
        switch (order) {
            case IndexOrder::INDEX_ORDER_PRIMARY:
                curr_acc = RowLookup(originalSentinel);
                break;
            case IndexOrder::INDEX_ORDER_SECONDARY:
                curr_acc = RowLookup(originalSentinel);
                if (curr_acc == nullptr) {
                    curr_acc = RowLookup(originalSentinel->GetPrimarySentinel());
                    if (curr_acc) {
                        // Lets Verify the relation between Primary/Secondary Key using the Snapshot
                        if (GetVisibleRowVersion(originalSentinel, curr_acc->m_snapshot) == nullptr) {
                            curr_acc = nullptr;
                            return RC::RC_LOCAL_ROW_NOT_VISIBLE;
                        }
                    }
                }
                break;
            case IndexOrder::INDEX_ORDER_SECONDARY_UNIQUE:
                curr_acc = RowLookup(originalSentinel);
                if (curr_acc == nullptr) {
                    PrimarySentinelNode* node = nullptr;
                    if (m_txnManager->GetIsolationLevel() > ISOLATION_LEVEL::READ_COMMITED) {
                        node = static_cast<SecondarySentinelUnique*>(originalSentinel)
                                   ->GetNodeByCSN(m_txnManager->GetVisibleCSN());
                        if (node != nullptr) {
                            curr_acc = RowLookup(node->GetPrimarySentinel());
                        }
                    } else {
                        node = static_cast<SecondarySentinelUnique*>(originalSentinel)->GetTopNode();
                        while (node != nullptr) {
                            curr_acc = RowLookup(node->GetPrimarySentinel());
                            if (curr_acc) {
                                Row* r = GetVisibleRowVersion(originalSentinel, curr_acc->m_snapshot);
                                if (r == nullptr or r->GetCommitSequenceNumber() != curr_acc->m_csn) {
                                    curr_acc = nullptr;
                                    return RC::RC_LOCAL_ROW_NOT_VISIBLE;
                                }
                                break;
                            }
                            node = node->GetNextVersion();
                        }
                    }
                }
                break;
            default:
                MOT_ASSERT(false);
                break;
        }
    }

    return LookupFilterRow(curr_acc, type, isCommitted, r_local_Row);
}

RC TxnAccess::LookupFilterRow(Access* curr_acc, const AccessType type, bool isCommitted, Row*& r_local_Row)
{
    r_local_Row = nullptr;
    if (curr_acc != nullptr) {
        // Filter rows
        switch (curr_acc->m_type) {
            case AccessType::RD:
                // Cached row is not valid remove it!!
                if (curr_acc->m_stmtCount != m_txnManager->GetStmtCount()) {
                    auto it = m_rowsSet->find(curr_acc->m_origSentinel);
                    MOT_ASSERT(it != m_rowsSet->end());
                    auto res = m_rowsSet->erase(it);
                    // need to perform index clean-up!
                    ReleaseAccess(curr_acc);
                    return RC::RC_LOCAL_ROW_NOT_FOUND;
                }
                break;
            case AccessType::RD_FOR_UPDATE:
            case AccessType::WR:
                break;
            case AccessType::INS:
                if (curr_acc->m_stmtCount == m_txnManager->GetStmtCount() && type != AccessType::WR) {
                    return RC::RC_LOCAL_ROW_NOT_VISIBLE;
                }
                if (curr_acc->GetSentinel()->GetIndexOrder() != IndexOrder::INDEX_ORDER_PRIMARY) {
                    Access* primaryAccess = RowLookup(curr_acc->GetTxnRow()->GetPrimarySentinel());
                    if (primaryAccess == nullptr) {
                        return RC::RC_PRIMARY_SENTINEL_NOT_MAPPED;
                    } else {
                        r_local_Row = primaryAccess->GetTxnRow();
                        return RC::RC_LOCAL_ROW_FOUND;
                    }
                }
                break;
            case AccessType::DEL:
                return RC::RC_LOCAL_ROW_DELETED;
            default:
                MOT_ASSERT(false);
                break;
        }
    } else {
        if (isCommitted) {
            return RC::RC_LOCAL_ROW_NOT_FOUND;
        } else {
            return RC::RC_LOCAL_ROW_NOT_VISIBLE;
        }
    }

    r_local_Row = curr_acc->GetTxnRow();
    return RC::RC_LOCAL_ROW_FOUND;
}

Row* TxnAccess::MapRowtoLocalTable(const AccessType type, Sentinel* const& originalSentinel, RC& rc)
{
    Access* current_access = nullptr;
    rc = RC_OK;

    current_access = GetNewRowAccess(originalSentinel, type, rc);
    // Check if draft is valid
    if (current_access == nullptr)
        return nullptr;

    current_access->m_type = (type != RD_FOR_UPDATE) ? RD : RD_FOR_UPDATE;
    current_access->m_params.SetPrimarySentinel();

    // We map the p_sentinel for the case of commited Row!
    void* key = (void*)current_access->m_origSentinel;
    auto ret = m_rowsSet->insert(RowAccessPair_t(key, current_access));
    MOT_ASSERT(ret.second == true);
    // Set Last access
    SetLastAccess(current_access);
    current_access->m_stmtCount = m_txnManager->GetStmtCount();
    current_access->m_redoStmt = current_access->m_stmtCount;
    return current_access->GetGlobalVersion();
}

Row* TxnAccess::AddInsertToLocalAccess(Sentinel* org_sentinel, Row* org_row, RC& rc, bool isUpgrade)
{
    rc = RC_OK;
    Access* curr_access = nullptr;

    // Search key from the unordered_map
    auto search = m_rowsSet->find(org_sentinel);
    if (likely(search == m_rowsSet->end())) {
        if (isUpgrade) {
            curr_access = GetNewInsertAccess(org_sentinel, org_row, rc, isUpgrade);
            // Check if draft is valid
            if (curr_access == nullptr) {
                return nullptr;
            }
            curr_access->m_params.SetUpgradeInsert();
            // Not in the cache - insert on deleted key
            curr_access->m_params.SetInsertOnDeletedRow();
        } else {
            curr_access = GetNewInsertAccess(org_sentinel, org_row, rc, isUpgrade);
            // Check if draft is valid
            if (curr_access == nullptr) {
                return nullptr;
            }
        }
        curr_access->m_stmtCount = m_txnManager->GetStmtCount();
        curr_access->m_redoStmt = curr_access->m_stmtCount;
        curr_access->m_origSentinel = org_sentinel;
        if (org_sentinel->IsPrimaryIndex()) {
            curr_access->m_params.SetPrimarySentinel();
        } else {
            curr_access->m_params.SetSecondarySentinel();
            if (org_sentinel->GetIndex()->GetUnique()) {
                Index* index = org_sentinel->GetIndex();
                PrimarySentinelNode* node = index->SentinelNodeAlloc();
                if (node == nullptr) {
                    MOT_REPORT_ERROR(MOT_ERROR_OOM, "Sentinel node", "Failed to create new sentinel entry");
                    rc = RC::RC_MEMORY_ALLOCATION_ERROR;
                    return nullptr;
                }
                (void)m_sentinelObjectPool->insert({index, node});
                curr_access->m_params.SetUniqueIndex();
            }
        }

        auto ret = m_rowsSet->insert(RowAccessPair_t(org_sentinel, curr_access));
        MOT_ASSERT(ret.second == true);
        SetLastAccess(curr_access);
        return curr_access->GetTxnRow();
    } else {
        curr_access = search->second;
        // Promote state of row to Delete
        if (curr_access->m_type == DEL) {
            // If we found a deleted row and the sentinel is not committed!
            // the row was deleted in between statement - we must abort!
            if (!isUpgrade) {
                MOT_LOG_ERROR("Invalid Insert! Row was deleted before i was able to delete!")
                rc = RC_UNIQUE_VIOLATION;
                return nullptr;
            }
            if (org_sentinel->GetIndexOrder() == IndexOrder::INDEX_ORDER_SECONDARY_UNIQUE) {
                PrimarySentinelNode* topNode =
                    static_cast<SecondarySentinelUnique*>(curr_access->m_origSentinel)->GetTopNode();
                if ((curr_access->m_secondaryUniqueNode != topNode) or
                    (topNode->GetEndCSN() != Sentinel::SENTINEL_INIT_CSN)) {
                    MOT_LOG_ERROR("Invalid Insert! Key already changed")
                    rc = RC_UNIQUE_VIOLATION;
                    return nullptr;
                }
                Index* index = org_sentinel->GetIndex();
                PrimarySentinelNode* node = index->SentinelNodeAlloc();
                if (node == nullptr) {
                    MOT_REPORT_ERROR(MOT_ERROR_OOM, "Sentinel node", "Failed to create new sentinel entry");
                    rc = RC::RC_MEMORY_ALLOCATION_ERROR;
                    return nullptr;
                }
                (void)m_sentinelObjectPool->insert({index, node});
            }
            if (curr_access->m_secondaryDelKey != nullptr) {
                org_sentinel->GetIndex()->DestroyKey(curr_access->m_secondaryDelKey);
                curr_access->m_secondaryDelKey = nullptr;
            }
            curr_access->m_type = INS;
            curr_access->m_params.SetUpgradeInsert();
            curr_access->m_localInsertRow = org_row;
            curr_access->m_stmtCount = m_txnManager->GetStmtCount();
            SetLastAccess(curr_access);
            return curr_access->GetTxnRow();
        }
    }
    rc = RC_UNIQUE_VIOLATION;
    return nullptr;
}

enum NS_ACTIONS : uint32_t { NOCHANGE, GENERATE_DRAFT, FILTER_DELETES, NS_ERROR };

typedef struct {
    uint32_t nextState;
    uint32_t action;
} TxnStateMachineEntry;

static const TxnStateMachineEntry txnStateMachine[TxnAccess::TSM_SIZE][TxnAccess::TSM_SIZE] = {
    /* INVALID STATE */
    {{INV, NS_ACTIONS::NS_ERROR},               // INV
        {RD, NS_ACTIONS::NOCHANGE},             // RD
        {RD_FOR_UPDATE, NS_ACTIONS::NOCHANGE},  // RD_FOT_UPDATE
        {WR, NS_ACTIONS::GENERATE_DRAFT},       // WR
        {DEL, NS_ACTIONS::GENERATE_DRAFT},      // DEL
        {INS, NS_ACTIONS::NOCHANGE}},           // INS
    /* READ STATE */
    {{RD, NS_ACTIONS::NS_ERROR},
        {RD, NS_ACTIONS::NOCHANGE},
        {RD_FOR_UPDATE, NS_ACTIONS::NOCHANGE},
        {WR, NS_ACTIONS::GENERATE_DRAFT},
        {DEL, NS_ACTIONS::GENERATE_DRAFT},
        {RD, NS_ACTIONS::NS_ERROR}},
    /* READ_FOR_UPDATE STATE */
    {{RD_FOR_UPDATE, NS_ACTIONS::NS_ERROR},
        {RD_FOR_UPDATE, NS_ACTIONS::NOCHANGE},
        {RD_FOR_UPDATE, NS_ACTIONS::GENERATE_DRAFT},
        {WR, NS_ACTIONS::GENERATE_DRAFT},
        {DEL, NS_ACTIONS::GENERATE_DRAFT},
        {RD_FOR_UPDATE, NS_ACTIONS::NS_ERROR}},
    /* WRITE STATE */
    {{WR, NS_ACTIONS::NS_ERROR},
        {WR, NS_ACTIONS::NOCHANGE},
        {WR, NS_ACTIONS::NOCHANGE},
        {WR, NS_ACTIONS::NOCHANGE},
        {DEL, NS_ACTIONS::GENERATE_DRAFT},
        {WR, NS_ACTIONS::NS_ERROR}},
    /* DELETE STATE */
    {{DEL, NS_ACTIONS::NS_ERROR},
        {DEL, NS_ACTIONS::NOCHANGE},
        {DEL, NS_ACTIONS::NOCHANGE},
        {DEL, NS_ACTIONS::NS_ERROR},
        {DEL, NS_ACTIONS::NS_ERROR},
        {INS, NS_ACTIONS::NOCHANGE}},
    /* INSERT STATE */
    {{INS, NS_ACTIONS::NS_ERROR},
        {INS, NS_ACTIONS::NOCHANGE},
        {INS, NS_ACTIONS::NOCHANGE},
        {INS, NS_ACTIONS::NOCHANGE},
        {DEL, NS_ACTIONS::FILTER_DELETES},
        {INS, NS_ACTIONS::NS_ERROR}},
};

// Currently supporting RD/WR transitions
RC TxnAccess::UpdateRowState(AccessType type, Access* ac)
{
    MOT_LOG_DEBUG("Switch key State from: %s to: %s", enTxnStates[ac->m_type], enTxnStates[type]);
    AccessType currentState = ac->m_type;

    auto result = txnStateMachine[currentState][type];
    if (unlikely(result.action == NS_ACTIONS::NS_ERROR)) {
        MOT_LOG_ERROR("Invalid State: current_state=%u, type=%u", currentState, type);
        return RC_ERROR;
    }

    RC rc = RC_OK;
    AccessType currentType = ac->m_type;
    ac->m_type = (AccessType)result.nextState;
    NS_ACTIONS action = (NS_ACTIONS)result.action;
    switch (action) {
        case NS_ACTIONS::FILTER_DELETES:
            // Check and filter the access_set for cases of delete after insert in the same transaction.
            (void)FilterOrderedSet(ac);
            break;
        case NS_ACTIONS::GENERATE_DRAFT:
            rc = GenerateDraft(ac, currentType);
            if (rc != RC_OK) {
                break;
            }
            if (ac->m_type == DEL) {
                rc = GenerateSecondaryAccess(ac, ac->m_type);
            }
            MOT_LOG_DEBUG(
                "ACTION REQUIRED Switch key state from: %s, to: %s", enTxnStates[currentState], enTxnStates[type]);
            break;
        case NS_ACTIONS::NOCHANGE:
            MOT_LOG_DEBUG(
                "NO ACTION REQUIRED Switch key state from: %s, to: %s", enTxnStates[currentState], enTxnStates[type]);
            break;
        case NS_ACTIONS::NS_ERROR:
            MOT_LOG_DEBUG(
                "ERROR in state Switch key state from: %s, to: %s", enTxnStates[currentState], enTxnStates[type]);
            rc = RC_ERROR;
            break;
    }

    return rc;
}

Row* TxnAccess::GetVisibleRowVersion(Sentinel* sentinel, uint64_t csn)
{
    Row* row = sentinel->GetVisibleRowVersion(csn);
    if (row) {
        if (row->IsRowDeleted() == false) {
            return row;
        }
    }
    return nullptr;
}

PrimarySentinel* TxnAccess::GetVisiblePrimarySentinel(Sentinel* sentinel, uint64_t csn)
{
    return static_cast<PrimarySentinel*>(sentinel->GetVisiblePrimaryHeader(csn));
}

Row* TxnAccess::GetVisibleRow(PrimarySentinel* sentinel, AccessType type, RC& rc)
{
    Row* row = nullptr;
    rc = RC_OK;
    ISOLATION_LEVEL isolationLevel = m_txnManager->GetIsolationLevel();
    uint64_t csn = m_txnManager->GetVisibleCSN();
    if (type == RD) {
        return sentinel->GetVisibleRowVersion(csn);
    } else {
        switch (isolationLevel) {
            case READ_COMMITED:
                row = sentinel->GetVisibleRowForUpdate(csn, isolationLevel);
                break;
            case REPEATABLE_READ:
                row = sentinel->GetVisibleRowForUpdate(csn, isolationLevel);
                if (row) {
                    if (row->GetCommitSequenceNumber() >= csn) {
                        row = nullptr;
                        rc = RC_SERIALIZATION_FAILURE;
                    }
                } else {
                    if (sentinel->GetStartCSN() >= csn) {
                        rc = RC_SERIALIZATION_FAILURE;
                    }
                }
                break;
            case SERIALIZABLE:
                break;
            default:
                break;
        }
    }

    if (row and row->IsRowDeleted()) {
        return nullptr;
    }

    return row;
}

RC TxnAccess::SecondaryAccessDeleteTxnRows(Row* row, Table* table)
{
    Key* key = m_txnManager->GetLocalKey();
    uint16_t numIndexes = table->GetNumIndexes();
    for (uint16_t i = 1; i < numIndexes; i++) {
        Sentinel* outSentinel = nullptr;
        Index* ix = table->GetIndex(i);
        key->InitKey(ix->GetKeyLength());
        ix->BuildKey(table, row, key);
        RC rc = table->FindRowByIndexId(ix, key, outSentinel, m_txnManager->GetThdId());
        if (rc != RC_OK) {
            // row already deleted!! Abort
            m_txnManager->m_err = RC_SERIALIZATION_FAILURE;
            return RC_SERIALIZATION_FAILURE;
        }
        auto it = m_rowsSet->find(outSentinel);
        // Filter Rows
        if (it != m_rowsSet->end()) {
            Access* ac = (*it).second;
            if (ac->m_type == DEL) {
                m_txnManager->m_err = RC_SERIALIZATION_FAILURE;
                return RC_SERIALIZATION_FAILURE;
            }
            // Use primary row for rollback
            // In some cases we created an Index we cannot rely on access row
            ac->m_localInsertRow = row;
            m_txnManager->RollbackInsert(ac);
            (void)m_rowsSet->erase(it);
            // need to perform index clean-up!
            ReleaseAccess(ac);
        }
    }

    return RC_OK;
}

RC TxnAccess::SecondaryAccessDeleteGlobalRows(Access* primaryAccess, Row* row, Table* table)
{
    RC rc = RC_OK;
    Access* current_access = nullptr;
    Key* key = m_txnManager->GetLocalKey();
    uint64_t csn = m_txnManager->GetVisibleCSN();
    uint16_t numIndexes = table->GetNumIndexes();
    for (uint16_t i = 1; i < numIndexes; i++) {
        current_access = nullptr;
        Sentinel* outSentinel = nullptr;
        Index* ix = table->GetIndex(i);
        key->InitKey(ix->GetKeyLength());
        ix->BuildKey(table, row, key);
        rc = table->FindRowByIndexId(ix, key, outSentinel, m_txnManager->GetThdId());
        if (rc != RC_OK) {
            // row already deleted!! Abort
            m_txnManager->m_err = RC_SERIALIZATION_FAILURE;
            return RC_SERIALIZATION_FAILURE;
        }
        auto it = m_rowsSet->find(outSentinel);
        // Filter Rows
        if (it != m_rowsSet->end()) {
            Access* ac = (*it).second;
            if (ac->m_params.IsIndexUpdate() and ac->m_type == DEL) {
                ac->m_params.UnsetIndexUpdate();
                ac->m_localRow = primaryAccess->m_localRow;
                if (ac->m_secondaryDelKey != nullptr) {
                    ix->DestroyKey(ac->m_secondaryDelKey);
                    ac->m_secondaryDelKey = nullptr;
                }
                continue;
            }
            if (ac->m_type == DEL) {
                m_txnManager->m_err = RC_SERIALIZATION_FAILURE;
                return RC_SERIALIZATION_FAILURE;
            }
            // Use primary row for rollback
            // In some cases we created an Index we cannot rely on access row
            ac->m_localInsertRow = row;
            m_txnManager->RollbackInsert(ac);
            (void)m_rowsSet->erase(it);
            // need to perform index clean-up!
            ReleaseAccess(ac);
        } else {
            if (m_txnManager->GetIsolationLevel() > READ_COMMITED) {
                PrimarySentinel* ps = outSentinel->GetVisiblePrimaryHeader(csn);
                if (ps == nullptr) {
                    // Sentinel is not visible for current transaction abort!
                    m_txnManager->m_err = RC_SERIALIZATION_FAILURE;
                    return RC_SERIALIZATION_FAILURE;
                }
            }
            // generate New Accesses!
            current_access = GetNewSecondaryAccess(outSentinel, primaryAccess, rc);
            if (current_access == nullptr) {
                return rc;
            }

            if (ix->GetUnique()) {
                current_access->m_params.SetUniqueIndex();
                current_access->m_secondaryUniqueNode =
                    static_cast<SecondarySentinelUnique*>(outSentinel)->GetNodeByCSN(csn);
                if ((current_access->m_secondaryUniqueNode == nullptr) ||
                    (current_access->m_secondaryUniqueNode->GetEndCSN() != Sentinel::SENTINEL_INIT_CSN)) {
                    m_txnManager->m_err = RC_SERIALIZATION_FAILURE;
                    rc = RC_SERIALIZATION_FAILURE;
                    break;
                }
                current_access->m_csn = current_access->m_secondaryUniqueNode->GetStartCSN();
            }
            auto ret = m_rowsSet->insert(RowAccessPair_t(outSentinel, current_access));
            MOT_ASSERT(ret.second == true);
        }
    }

    // In case of error destroy the access
    if (rc != RC_OK and current_access != nullptr) {
        ReleaseAccess(current_access);
        current_access = nullptr;
    }

    return rc;
}

RC TxnAccess::GenerateSecondaryAccess(Access* primaryAccess, AccessType acType)
{
    Row* row = primaryAccess->GetTxnRow();
    Table* table = row->GetTable();
    if (m_txnManager->m_txnDdlAccess->Size() > 0 && !table->IsTxnTable()) {
        Table* tmp = m_txnManager->GetTxnTable(table->GetTableExId());
        if (tmp != nullptr) {
            table = tmp;
        }
    }
    uint16_t numIndexes = table->GetNumIndexes();
    if (numIndexes == 1) {
        return RC_OK;
    }

    bool isIndexUpdate = false;

    // Transactional row is a tombstone in the case of delete
    if (primaryAccess->m_params.IsUpdateDeleted() == true) {
        // Need to remove all keys before and after the update
        // Old keys are in IndexUpdate, new keys are inserts
        row = m_rowZero;
        primaryAccess->m_params.UnsetUpdateDeleted();
        if (primaryAccess->m_params.IsIndexUpdate() == true) {
            primaryAccess->m_params.UnsetIndexUpdate();
            isIndexUpdate = true;
        }
    } else {
        MOT_ASSERT(row->GetRowType() == RowType::TOMBSTONE);
        row = GetRowZeroCopyIfAny(primaryAccess->GetGlobalVersion());
    }

    if (unlikely(isIndexUpdate)) {
        // first phase go through updated row and filter new rows
        RC rc = SecondaryAccessDeleteTxnRows(row, table);
        if (rc != RC_OK) {
            return rc;
        }

        // Switch back to global row!
        row = GetRowZeroCopyIfAny(primaryAccess->GetGlobalVersion());
    }

    return SecondaryAccessDeleteGlobalRows(primaryAccess, row, table);
}

RC TxnAccess::GenerateDraft(Access* ac, AccessType acType)
{
    RC rc = RC_OK;
    MOT_ASSERT(ac->GetGlobalVersion() != nullptr);
    switch (ac->m_type) {
        case DEL:
        case WR:
            return CreateMVCCDraft(ac, acType);
        case RD_FOR_UPDATE:
        default:
            break;
    }
    return rc;
}

Row* TxnAccess::FetchRowFromPrimarySentinel(const AccessType type, Sentinel* const& originalSentinel, RC& rc)
{
    rc = RC_OK;
    MOT_ASSERT(originalSentinel->GetIndexOrder() != IndexOrder::INDEX_ORDER_PRIMARY);
    Access* current_access = RowLookup(originalSentinel);
    MOT_ASSERT(current_access != nullptr);
    Sentinel* primarySentinel = current_access->GetTxnRow()->GetPrimarySentinel();
    MOT_ASSERT(primarySentinel != nullptr);
    if (type == RD) {
        return GetVisibleRowVersion(primarySentinel, m_txnManager->GetVisibleCSN());
    } else {
        return MapRowtoLocalTable(type, primarySentinel, rc);
    }
}

RC TxnAccess::CreateMVCCDraft(Access* ac, AccessType acType)
{
    if (ac->m_localRow != nullptr) {
        // If we already generated a draft for update
        // Lets replace it with a draft for DEL
        if (acType == WR and ac->m_type == DEL) {
            MOT_ASSERT(ac->m_localRow != nullptr);
            // Indicate that DELETE should use rowZero
            ac->m_params.SetUpdateDeleted();
            GetRowZeroCopy(ac->m_localRow);
            ac->m_localRow->GetTable()->DestroyRow(ac->m_localRow);
            ac->m_localRow = nullptr;
            if (ac->m_modifiedColumns.IsInitialized()) {
                m_dummyTable.DestroyBitMapBuffer(ac->m_modifiedColumns.GetData(), ac->m_modifiedColumns.GetSize());
                ac->m_modifiedColumns.Reset();
            }
        } else {
            MOT_ASSERT(false);
            return RC_ABORT;
        }
    }

    Row* globalRow = ac->GetGlobalVersion();
    Table* tab = m_txnManager->GetTxnTable(globalRow->GetTable()->GetTableExId());
    if (likely(tab == nullptr)) {
        tab = globalRow->GetTable();
    }
    Row* newRow = tab->CreateNewRowCopy(globalRow, ac->m_type);
    if (__builtin_expect(newRow == nullptr, 0)) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Access Row", "Failed to copy or create new row");
        return RC_MEMORY_ALLOCATION_ERROR;
    }
    newRow->SetTable(tab);
    if (ac->m_type != DEL) {
        int fieldCount = tab->GetFieldCount() - 1;
        RC rc = InitBitMap(ac, fieldCount);
        if (rc != RC_OK) {
            DestroyAccess(ac);
            tab->DestroyRow(newRow);
            return rc;
        }
    }

    ac->m_localRow = newRow;
    ac->m_localRow->SetPrimarySentinel(ac->m_origSentinel);
    ac->m_localRow->SetCommitSequenceNumber(Sentinel::SENTINEL_INVISIBLE_CSN);
    ac->m_localRow->SetNextVersion(nullptr);

    IncreaseTableStat(tab);

    return RC_OK;
}
bool TxnAccess::FilterOrderedSet(Access* element)
{
    bool res = false;
    // no need to filter current delete
    // the delete is performed on an index item
    if (element->m_origSentinel == nullptr) {
        return false;
    } else {
        MOT_ASSERT(element->m_type == DEL);
        // revert type to INS
        element->m_type = INS;
        Sentinel* outSentinel = nullptr;
        Row* releasedRow = nullptr;
        Key* key = m_txnManager->GetLocalKey();
        // there are other indices elements that need to be removed.
        // case of BEGIN->INS->DEL
        // For blocked INSERTS the key is the orig_sentinel and the shared element is the orig_row!
        // Lets get all the keys and remove them from the cache and try to remove from the Index if possible
        Row* row = element->GetTxnRow();
        Table* table = row->GetTable();
        if (m_txnManager->m_txnDdlAccess->Size() > 0 && !table->IsTxnTable()) {
            Table* tmp = m_txnManager->GetTxnTable(table->GetTableExId());
            if (tmp != nullptr) {
                table = tmp;
            }
        }
        uint16_t numIndexes = table->GetNumIndexes();
        for (uint16_t i = 0; i < numIndexes; i++) {
            outSentinel = nullptr;
            Index* ix = table->GetIndex(i);
            key->InitKey(ix->GetKeyLength());
            ix->BuildKey(table, row, key);
            (void)table->FindRowByIndexId(ix, key, outSentinel, m_txnManager->GetThdId());
            auto it = m_rowsSet->find(outSentinel);
            // Filter Rows
            if (it != m_rowsSet->end()) {
                Access* ac = (*it).second;
                res = true;
                m_txnManager->RollbackInsert(ac);
                releasedRow = ac->m_localInsertRow;
                if (ac->m_params.IsUpgradeInsert() == false or ac->m_params.IsInsertOnDeletedRow() == true) {
                    (void)m_rowsSet->erase(it);
                    // need to perform index clean-up!
                    ReleaseAccess(ac);
                } else {
                    // Transform INS to DEL
                    ac->m_type = DEL;
                    ac->m_params.UnsetUpgradeInsert();
                    ac->m_localInsertRow = nullptr;
                    ac->m_params.UnsetIndexUpdate();
                }
            }
        }
        if (releasedRow) {
            table->DestroyRow(releasedRow);
        }
    }

    return res;
}

RC TxnAccess::InitBitMap(Access* ac, int fieldCount)
{
    RC rc = RC_OK;
    if (!ac->m_modifiedColumns.IsInitialized()) {
        uint8_t* bms = m_dummyTable.CreateBitMapBuffer(fieldCount);
        if (__builtin_expect(bms == nullptr, 0)) {
            // if modified_columns allocation failed, release allocated row
            rc = RC::RC_MEMORY_ALLOCATION_ERROR;
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Access Row", "Failed to create columns bitmap");
            DestroyAccess(ac);
            return rc;
        }
        ac->m_modifiedColumns.Init(bms, fieldCount);
    } else {
        MOT_ASSERT(false);
        rc = RC_ABORT;
    }
    return rc;
}

void TxnAccess::GcMaintenance()
{
    Row* newVersion = nullptr;
    uint64_t csn = m_txnManager->GetCommitSequenceNumber();
    for (const auto& raPair : GetOrderedRowSet()) {
        Access* access = raPair.second;
        switch (access->m_type) {
            case WR:
                (void)static_cast<PrimarySentinel*>(access->m_origSentinel)->GetGcInfo().RefCountUpdate(INC);
                newVersion = access->m_localRow;
                m_txnManager->GcSessionRecordRcu(GC_QUEUE_TYPE::VERSION_QUEUE,
                    newVersion->GetTable()->GetPrimaryIndex()->GetIndexId(),
                    newVersion,
                    access->m_origSentinel,
                    Row::RowVersionDtor,
                    ROW_SIZE_FROM_POOL(newVersion->GetTable()),
                    csn);
                access->m_localRow = nullptr;
                break;
            case DEL:
                if (access->m_params.IsPrimarySentinel() == true) {
                    if (access->GetSentinel()->GetStable() == nullptr) {
                        (void)static_cast<PrimarySentinel*>(access->m_origSentinel)->GetGcInfo().RefCountUpdate(INC);
                        newVersion = access->m_localRow;
                        m_txnManager->GcSessionRecordRcu(GC_QUEUE_TYPE::DELETE_QUEUE,
                            newVersion->GetTable()->GetPrimaryIndex()->GetIndexId(),
                            newVersion,
                            access->m_origSentinel,
                            Row::DeleteRowDtor,
                            ROW_SIZE_FROM_POOL(newVersion->GetTable()),
                            csn);
                    }
                    access->m_localRow = nullptr;
                } else {
                    if (access->m_params.IsIndexUpdate()) {
                        MOT_ASSERT(access->m_secondaryDelKey != nullptr);
                        MOT_ASSERT(access->m_origSentinel->GetCounter() > 0);
                        m_txnManager->GcSessionRecordRcu(GC_QUEUE_TYPE::UPDATE_COLUMN_QUEUE,
                            access->m_origSentinel->GetIndex()->GetIndexId(),
                            access->m_origSentinel,
                            access->m_secondaryDelKey,
                            Index::DeleteKeyDtor,
                            SENTINEL_SIZE(access->m_origSentinel->GetIndex()),
                            csn);
                        access->m_secondaryDelKey = nullptr;
                    }
                }
                break;
            case INS:
                if (access->m_params.IsPrimarySentinel() == true) {
                    // For both upgrades we insert the latest row
                    // Latest Version will perform the delete
                    if (access->m_params.IsUpgradeInsert() == true) {
                        (void)static_cast<PrimarySentinel*>(access->m_origSentinel)->GetGcInfo().RefCountUpdate(INC);
                        newVersion = access->GetLocalInsertRow();
                        m_txnManager->GcSessionRecordRcu(GC_QUEUE_TYPE::VERSION_QUEUE,
                            newVersion->GetTable()->GetPrimaryIndex()->GetIndexId(),
                            newVersion,
                            access->m_origSentinel,
                            Row::RowVersionDtor,
                            ROW_SIZE_FROM_POOL(newVersion->GetTable()),
                            csn);
                        access->m_localInsertRow = nullptr;
                        access->m_localRow = nullptr;
                    }
                }
                break;
            default:
                break;
        }
    }
}

Row* TxnAccess::GetRowZeroCopyIfAny(Row* localRow)
{
    Table* rowTable = localRow->GetTable();
    Table* txnTable = m_txnManager->GetTxnTable(rowTable->GetTableExId());
    if (txnTable == nullptr or rowTable->IsTxnTable()) {
        return localRow;
    } else {
        if (txnTable->GetHasColumnChanges()) {
            m_rowZero->CopyRowZero(localRow, txnTable);
        } else {
            return localRow;
        }
    }

    return m_rowZero;
}

void TxnAccess::GetRowZeroCopy(Row* localRow)
{
    Table* rowTable = localRow->GetTable();
    Table* txnTable = m_txnManager->GetTxnTable(rowTable->GetTableExId());
    if (txnTable != nullptr) {
        m_rowZero->CopyRowZero(localRow, txnTable);
    } else {
        m_rowZero->CopyRowZero(localRow, rowTable);
    }
}

void TxnAccess::Print()
{
    multimap<std::string, Access*> GlobalMap;
    cout << "\nAccess Map THREAD:" << m_txnManager->GetThdId() << endl;
    cout << "-----------------------" << endl;
    if (m_rowCnt > ACCESS_SET_PRINT_THRESHOLD) {
        MOT_LOG_INFO("Access Table too big to print, printing table statistics");
        PrintStats();
        return;
    }

    if (m_rowCnt == 0) {
        MOT_LOG_INFO("Access Table is empty");
        return;
    }

    uint32_t max_table_size = 0;
    uint32_t max_index_size = 0;
    for (const auto& raPair : GetOrderedRowSet()) {
        Access* access = raPair.second;
        Table* table = access->GetSentinel()->GetIndex()->GetTable();
        (void)GlobalMap.insert({table->GetTableName(), access});
        if (max_table_size < table->GetTableName().length()) {
            max_table_size = table->GetTableName().length();
        }
        if (max_index_size < access->GetSentinel()->GetIndex()->GetName().length()) {
            max_index_size = access->GetSentinel()->GetIndex()->GetName().length();
        }
    }

    auto itr = GlobalMap.begin();
    string table = itr->first;
    Access* access = itr->second;
    cout << left << setw(max_table_size) << setfill(' ') << "Table";
    cout << '\t' << "| ";
    cout << left << setw(max_index_size) << setfill(' ') << "Index";
    cout << '\t' << "| ";
    cout << left << setw(max_index_size) << setfill(' ') << "Index Order";
    cout << '\t' << "| ";
    cout << left << setw(13) << setfill(' ') << "Access Type";
    cout << '\t' << "| ";
    cout << left << setw(max_index_size) << setfill(' ') << "Sentinel" << endl;
    cout << "-------------------------------------------------"
         << "-----------------------------------------------" << endl;
    cout << left << setw(max_table_size) << setfill(' ') << itr->first;
    cout << '\t' << "| ";
    cout << left << setw(max_index_size) << setfill(' ') << access->GetSentinel()->GetIndex()->GetName();
    cout << '\t' << "| ";
    cout << left << setw(max_index_size) << setfill(' ')
         << enIndexOrder[static_cast<uint8_t>(access->GetSentinel()->GetIndexOrder())];
    cout << '\t' << "| ";
    cout << left << setw(13) << setfill(' ') << enTxnStates[access->GetType()];
    cout << '\t' << "| ";
    cout << left << setw(max_index_size) << setfill(' ') << access->GetSentinel() << endl;
    (void)++itr;
    for (; itr != GlobalMap.end(); (void)++itr) {
        if (table == itr->first) {
            cout << left << setw(max_table_size) << setfill(' ') << " ";
        } else {
            cout << "-------------------------------------------------"
                 << "-----------------------------------------------" << endl;

            cout << left << setw(max_table_size) << setfill(' ') << itr->first;
            table = itr->first;
        }
        cout << '\t' << "| ";
        cout << left << setw(max_index_size) << setfill(' ') << (itr->second)->GetSentinel()->GetIndex()->GetName();
        cout << '\t' << "| ";
        cout << left << setw(max_index_size) << setfill(' ')
             << enIndexOrder[static_cast<uint8_t>((itr->second)->GetSentinel()->GetIndexOrder())];
        cout << '\t' << "| ";
        cout << left << setw(13) << setfill(' ') << enTxnStates[(itr->second)->GetType()];
        cout << " | ";
        cout << left << setw(14) << setfill(' ') << (itr->second)->GetSentinel() << endl;
    }
    cout << "-------------------------------------------------"
         << "-----------------------------------------------" << endl;
    PrintStats();
}

void TxnAccess::PrintStats()
{
    map<std::string, std::vector<uint32_t>> GlobalMap;
    uint8_t maxAccessTypes = static_cast<uint8_t>(AccessType::INS) + 1;
    for (const auto& raPair : GetOrderedRowSet()) {
        Access* access = raPair.second;
        Table* table = access->GetSentinel()->GetIndex()->GetTable();
        auto itr = GlobalMap.find(table->GetTableName());
        if (itr == GlobalMap.end()) {
            (void)GlobalMap.insert(
                pair<string, vector<uint32_t>>(table->GetTableName(), vector<uint32_t>(maxAccessTypes, 0)));
        }
        GlobalMap.at(table->GetTableName())[access->GetType()]++;
        auto test = GlobalMap.at(table->GetTableName());
    }
    for (auto itr = GlobalMap.begin(); itr != GlobalMap.end(); (void)++itr) {
        MOT_LOG_INFO("TABLE: %s", (itr->first).c_str());
        auto vec = itr->second;
        for (uint8_t index = 0; index < vec.size(); index++) {
            if (vec[index] > 0) {
                MOT_LOG_INFO("%s = %d", enTxnStates[index], (uint32_t)vec[index]);
            }
        }
        cout << "-------------\n";
    }
}

void TxnAccess::PrintPoolStats()
{
    if (m_accessPool != nullptr) {
        PoolStatsSt stats;
        errno_t erc = memset_s(&stats, sizeof(PoolStatsSt), 0, sizeof(PoolStatsSt));
        securec_check(erc, "\0", "\0");
        stats.m_type = PoolStatsT::POOL_STATS_ALL;

        m_accessPool->GetStats(stats);
        m_accessPool->PrintStats(stats, "Access Pool", LogLevel::LL_INFO);
    }
    m_dummyTable.PrintStats();
}

RC TxnAccess::AddColumn(Access* ac)
{
    RC rc = RC_OK;
    uint8_t* bms = nullptr;
    uint16_t newSize = 0;
    if (ac->m_modifiedColumns.IsInitialized()) {
        uint16_t oldSize = ac->m_modifiedColumns.GetSize();
        newSize = oldSize + 1;
        int newSizeBytes = BitmapSet::GetLength(newSize);
        int oldSizeBytes = BitmapSet::GetLength(oldSize);
        if (newSizeBytes != oldSizeBytes) {
            bms = m_dummyTable.CreateBitMapBuffer(newSize);
            if (__builtin_expect(bms == nullptr, 0)) {
                // if modified_columns allocation failed, release allocated row
                rc = RC::RC_PANIC;
                MOT_REPORT_ERROR(MOT_ERROR_OOM, "BitmapAddColumn", "Failed to create columns bitmap");
            } else {
                uint8_t* oldBms = ac->m_modifiedColumns.GetData();
                errno_t erc = memcpy_s(bms, newSizeBytes, oldBms, oldSizeBytes);
                securec_check(erc, "\0", "\0");
                ac->m_modifiedColumns.Init(bms, newSize);
                m_dummyTable.DestroyBitMapBuffer(oldBms, oldSize);
            }
        } else {
            ac->m_modifiedColumns.AddBit();
        }
    }

    return rc;
}

void TxnAccess::DropColumn(Access* ac, Column* col)
{
    if (ac->m_modifiedColumns.IsInitialized()) {
        ac->m_modifiedColumns.UnsetBit(col->m_id - 1);
    }
}

RC TxnAccess::GeneratePrimaryIndexUpdate(Access* primaryAccess, TxnIxColUpdate* colUpd)
{
    RC rc = RC_OK;
    Row* row = primaryAccess->GetTxnRow();
    MOT_ASSERT(row != nullptr);
    MOT_ASSERT(false);
    // insert new keys
    for (uint16_t i = 1; i < colUpd->m_arrLen && rc == RC_OK; i++) {
        if (colUpd->m_ix[i] != nullptr) {
            InsItem* insItem = m_txnManager->GetNextInsertItem();
            if (insItem == nullptr) {
                MOT_REPORT_ERROR(MOT_ERROR_RESOURCE_LIMIT, "Update Index", "Cannot get insert item");
                return RC_MEMORY_ALLOCATION_ERROR;
            }
            insItem->SetItem(row, colUpd->m_ix[i]);
            rc = m_txnManager->InsertRow(row);
        }
    }
    return rc;
}

RC TxnAccess::SecIdxUpdGenerateDeleteKeys(Access* primaryAccess, TxnIxColUpdate* colUpd)
{
    RC rc = RC_OK;
    Access* current_access = nullptr;
    Row* row = primaryAccess->GetTxnRow();
    MOT_ASSERT(row != nullptr);
    Sentinel* outSentinel = nullptr;
    // find references to old keys and create Access to remove it
    for (uint16_t i = 1; i < colUpd->m_arrLen; i++) {
        current_access = nullptr;
        if (colUpd->m_ix[i] != nullptr) {
            // delete by old key
            rc = colUpd->m_tab->FindRowByIndexId(
                colUpd->m_ix[i], colUpd->m_oldKeys[i], outSentinel, m_txnManager->GetThdId());
            if (rc != RC_OK) {
                // row already deleted!! Abort
                m_txnManager->m_err = RC_SERIALIZATION_FAILURE;
                rc = RC_SERIALIZATION_FAILURE;
                break;
            }
            auto it = m_rowsSet->find(outSentinel);
            // Filter Rows
            if (it != m_rowsSet->end()) {
                Access* ac = (*it).second;
                if (ac->m_type != INS) {
                    m_txnManager->m_err = RC_SERIALIZATION_FAILURE;
                    rc = RC_SERIALIZATION_FAILURE;
                    break;
                }
                if (ac->m_secondaryDelKey != nullptr) {
                    ac->m_origSentinel->GetIndex()->DestroyKey(ac->m_secondaryDelKey);
                }
                ac->m_secondaryDelKey = colUpd->m_oldKeys[i];
                // Use primary row for rollback
                // In some cases we created an Index we cannot rely on access row
                ac->m_localInsertRow = row;
                m_txnManager->RollbackInsert(ac);
                if (ac->m_params.IsUpgradeInsert() == false or ac->m_params.IsInsertOnDeletedRow() == true) {
                    (void)m_rowsSet->erase(it);
                    // need to perform index clean-up!
                    ReleaseAccess(ac);
                } else {
                    // Transform INS to DEL
                    ac->m_type = DEL;
                    ac->m_params.UnsetUpgradeInsert();
                    ac->m_localInsertRow = nullptr;
                }
            } else {
                if (m_txnManager->GetIsolationLevel() > READ_COMMITED) {
                    PrimarySentinel* ps = outSentinel->GetVisiblePrimaryHeader(m_txnManager->GetVisibleCSN());
                    if (ps == nullptr) {
                        // Sentinel is not visible for current transaction abort!
                        m_txnManager->m_err = RC_SERIALIZATION_FAILURE;
                        rc = RC_SERIALIZATION_FAILURE;
                        break;
                    }
                }
                // generate New Accesses!
                current_access = GetNewSecondaryAccess(outSentinel, primaryAccess, rc);
                if (current_access == nullptr) {
                    return rc;
                }

                if (colUpd->m_ix[i]->GetUnique()) {
                    current_access->m_params.SetUniqueIndex();
                    current_access->m_secondaryUniqueNode =
                        static_cast<SecondarySentinelUnique*>(outSentinel)->GetNodeByCSN(m_txnManager->GetVisibleCSN());
                    if ((current_access->m_secondaryUniqueNode == nullptr) ||
                        (current_access->m_secondaryUniqueNode->GetEndCSN() != Sentinel::SENTINEL_INIT_CSN)) {
                        m_txnManager->m_err = RC_SERIALIZATION_FAILURE;
                        rc = RC_SERIALIZATION_FAILURE;
                        break;
                    }
                    current_access->m_csn = current_access->m_secondaryUniqueNode->GetStartCSN();
                }
                current_access->m_type = DEL;
                current_access->m_secondaryDelKey = colUpd->m_oldKeys[i];
                current_access->m_params.SetIndexUpdate();
                auto ret = m_rowsSet->insert(RowAccessPair_t(outSentinel, current_access));
                MOT_ASSERT(ret.second == true);
            }
            // in case of error the key will be released by access destroy function
            colUpd->m_oldKeys[i] = nullptr;
        }
    }

    // In case of error destroy the access
    if (rc != RC_OK and current_access != nullptr) {
        ReleaseAccess(current_access);
        current_access = nullptr;
    }

    return rc;
}

RC TxnAccess::GenerateSecondaryIndexUpdate(Access* primaryAccess, TxnIxColUpdate* colUpd)
{
    Row* row = primaryAccess->GetTxnRow();
    MOT_ASSERT(row != nullptr);
    Sentinel* outSentinel = nullptr;

    // find references to old keys and create Access to remove it
    // insert new values
    RC rc = SecIdxUpdGenerateDeleteKeys(primaryAccess, colUpd);

    // insert new keys
    for (uint16_t i = 1; i < colUpd->m_arrLen && rc == RC_OK; i++) {
        if (colUpd->m_ix[i] != nullptr) {
            MOT_ASSERT(colUpd->m_newKeys[i] != nullptr);
            (void)colUpd->m_tab->FindRowByIndexId(
                colUpd->m_ix[i], colUpd->m_newKeys[i], outSentinel, m_txnManager->GetThdId());
            if (outSentinel != nullptr) {
                // Check A->B->A problem for update column
                auto it = m_rowsSet->find(outSentinel);
                if (it != m_rowsSet->end()) {
                    Access* current_access = it->second;
                    if (current_access->m_type != DEL) {
                        rc = RC_UNIQUE_VIOLATION;
                        auto err = m_txnManager->GetNextInsertItem();
                        err->SetItem(row, colUpd->m_ix[i]);
                        GetInsertMgr()->ReportError(rc, err);
                        continue;
                    }
                    // Detect dependency
                    if (primaryAccess->m_stmtCount > current_access->m_stmtCount) {
                        if (primaryAccess->m_redoStmt < current_access->m_redoStmt) {
                            // Error detected dependency
                            m_txnManager->m_err = RC_SERIALIZATION_FAILURE;
                            rc = RC_SERIALIZATION_FAILURE;
                            continue;
                        }
                    }
                    if (colUpd->m_ix[i]->GetIndexOrder() == IndexOrder::INDEX_ORDER_SECONDARY) {
                        MOT_ASSERT(current_access->m_type == DEL);
                        // Undo update column
                        (void)m_rowsSet->erase(it);
                        ReleaseAccess(current_access);
                        continue;
                    }
                }
            }
            m_txnManager->GetNextInsertItem()->SetItem(row, colUpd->m_ix[i]);
            rc = m_txnManager->InsertRow(row, colUpd->m_newKeys[i]);
            if (rc == RC_OK) {
                GetLastAccess()->m_params.SetIndexUpdate();
                GetLastAccess()->m_secondaryDelKey = colUpd->m_newKeys[i];
                colUpd->m_newKeys[i] = nullptr;
            }
        }
    }
    return rc;
}

void TxnAccess::ClearTableCache()
{
    for (auto itr = m_tableStat.begin(); itr != m_tableStat.end();) {
        if (itr->second >= FREE_THREAD_CACHE_THRESHOLD) {
            Table* table = nullptr;
            if (m_txnManager->IsRecoveryTxn()) {
                table = GetTableManager()->GetTableSafeByExId(itr->first);
            } else {
                table = GetTableManager()->GetTableByExternal(itr->first);
            }
            if (table != nullptr) {
                table->ClearRowCache();
                if (m_txnManager->IsRecoveryTxn()) {
                    table->Unlock();
                }
            }
            itr = m_tableStat.erase(itr);
        } else {
            (void)++itr;
        }
    }
}
}  // namespace MOT
