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

#include "table.h"
#include "row.h"
#include "txn.h"
#include "txn_access.h"
#include "txn_insert_action.h"

namespace MOT {
IMPLEMENT_CLASS_LOGGER(TxnInsertAction, TxMan);
IMPLEMENT_CLASS_LOGGER(TxnAccess, TxMan);

TxnAccess::TxnAccess()
    : m_accessesSetBuff(nullptr),
      m_rowCnt(0),
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

        // fall through
        case CreateRowZero:
            if (m_rowZero) {
                m_dummyTable.DestroyMaxRow(m_rowZero);
            }

        // fall through
        case InitDummyTab:
        // fall through
        case AllocBuf:
            if (m_accessesSetBuff) {
                MemSessionFree(m_accessesSetBuff);
            }

        // fall through
        case Startup:
        default:
            break;
    }
}

void TxnAccess::ClearAccessSet()
{
    for (unsigned int i = 0; i < m_accessSetSize; i++) {
        Access* ac = GetAccessPtr(i);
        if (ac != nullptr) {
            DestroyAccess(ac);
            delete ac;
        } else {
            break;
        }
    }

    m_allocatedAc = 0;
    MemSessionFree(m_accessesSetBuff);
    m_accessesSetBuff = nullptr;
}

bool TxnAccess::Init(TxnManager* manager)
{
    m_initPhase = Startup;

    // allocate buffer
    m_txnManager = manager;
    uint32_t alloc_size = sizeof(Access*) * m_accessSetSize;
    void* ptr = MemSessionAlloc(alloc_size);
    if (ptr == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Transaction Initialization",
            "Failed to allocate %u bytes aligned to 64 bytes for TxnAccess in session %u",
            alloc_size,
            manager->GetSessionContext()->GetSessionId());
        return false;
    }
    m_initPhase = AllocBuf;

    m_accessesSetBuff = reinterpret_cast<Access**>(ptr);
    for (uint64_t idx = 0; idx < m_accessSetSize; idx++) {
        m_accessesSetBuff[idx] = nullptr;
    }

    // initialize dummy table
    if (!m_dummyTable.Init()) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Transaction Initialization",
            "Failed to initialize dummy table for session %u",
            manager->GetSessionContext()->GetSessionId());
        return false;
    }
    m_initPhase = InitDummyTab;

    // create row zero
    m_rowZero = m_dummyTable.CreateMaxRow();
    if (!m_rowZero) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Transaction Initialization",
            "Failed to allocate row zero for session %u",
            manager->GetSessionContext()->GetSessionId());
        return false;
    }
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
    m_initPhase = CreateRowSet;

    m_insertManager = new (std::nothrow) TxnInsertAction();
    if (m_insertManager == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Transaction Initialization",
            "Failed to allocate Insert set for session %u",
            manager->GetSessionContext()->GetSessionId());
        return false;
    }

    if (!m_insertManager->Init(manager)) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Transaction Initialization",
            "Failed to INIT Insert set for session %u",
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
    m_insertManager->ClearSet();
}

void TxnAccess::DestroyAccess(Access* access)
{
    Row* row = access->m_localRow;
    if (row != nullptr) {
        m_dummyTable.DestroyRow(row, access);
        access->m_localRow = nullptr;
    }
    if (access->m_modifiedColumns.IsInitialized()) {
        m_dummyTable.DestroyBitMapBuffer(access->m_modifiedColumns.GetData(), access->m_modifiedColumns.GetSize());
        access->m_modifiedColumns.Reset();
    }
}

bool TxnAccess::ReallocAccessSet()
{
    bool rc = true;
    uint64_t new_array_size = m_accessSetSize * ACCESS_SET_EXTEND_FACTOR;
    MOT_LOG_DEBUG("Increasing  Access Size! from %d to %lu", m_accessSetSize, new_array_size);

    uint32_t alloc_size = sizeof(Access*) * new_array_size;
    void* ptr = MemSessionAlloc(alloc_size);
    if (ptr == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Transaction Execution",
            "Failed to allocate %u bytes aligned to 64 bytes for TxnAccess in session %u (while reallocating)",
            alloc_size,
            m_txnManager->GetSessionContext()->GetSessionId());
        SetLastError(MOT_ERROR_OOM, MOT_SEVERITY_ERROR);
        rc = false;
    } else {
        errno_t erc = memset_s(ptr, sizeof(Access*) * new_array_size, 0, sizeof(Access*) * new_array_size);
        securec_check(erc, "\0", "\0");
        erc = memcpy_s(ptr, sizeof(Access*) * new_array_size, m_accessesSetBuff, sizeof(Access*) * m_accessSetSize);
        securec_check(erc, "\0", "\0");
        MemSessionFree(m_accessesSetBuff);
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
        delete m_accessesSetBuff[i];
    }

    if (new_array_size < m_accessSetSize) {
        m_dummyTable.ClearRowCache();
    }

    MOT_LOG_DEBUG("Shrinking Access Size! from %d to %lu", m_accessSetSize, new_array_size);

    uint32_t alloc_size = sizeof(Access*) * new_array_size;
    void* ptr = MemSessionAlloc(alloc_size);
    if (ptr == nullptr) {
        MOT_LOG_ERROR("Failed to allocate %u bytes aligned to 64 bytes for TxnAccess in session %u (while shrinking)",
            alloc_size,
            m_txnManager->GetSessionContext()->GetSessionId());
        return;
    }

    erc = memset_s(ptr, alloc_size, 0, sizeof(Access*) * new_array_size);
    securec_check(erc, "\0", "\0");
    MemSessionFree(m_accessesSetBuff);
    SetAccessesSet(reinterpret_cast<Access**>(ptr));
    m_accessSetSize = new_array_size;
    m_allocatedAc = 0;
    m_rowCnt = 0;
}

Access* TxnAccess::GetNewRowAccess(const Row* row, AccessType type, RC& rc)
{
    rc = RC_OK;
    TransactionId last_tid;
    Access* ac = GetAccessPtr(m_rowCnt);

    if (unlikely(ac == nullptr)) {
        if (!CreateNewAccess(row->GetTable(), type)) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Access Row", "Failed to create new access entry");
            rc = RC::RC_MEMORY_ALLOCATION_ERROR;
            return nullptr;
        }
        ac = GetAccessPtr(m_rowCnt);
    }

    ac->ResetUsedParameters();

    if (type != INS) {
        if (ac->m_localRow == nullptr) {
            Row* new_row = m_dummyTable.CreateNewRow(row->GetTable(), ac);
            if (__builtin_expect(new_row == nullptr, 0)) {
                MOT_REPORT_ERROR(MOT_ERROR_OOM, "Access Row", "Failed to create new row");
                rc = RC_MEMORY_ALLOCATION_ERROR;
                return nullptr;
            }
            ac->m_localRow = new_row;
        }

        int fieldCount = row->GetTable()->GetFieldCount() - 1;
        if (!ac->m_modifiedColumns.IsInitialized()) {
            uint8_t* bms = m_dummyTable.CreateBitMapBuffer(fieldCount);
            if (__builtin_expect(bms == nullptr, 0)) {
                // if modified_columns allocation failed, release allocated row
                rc = RC::RC_MEMORY_ALLOCATION_ERROR;
                MOT_REPORT_ERROR(MOT_ERROR_OOM, "Access Row", "Failed to create columns bitmap");
                m_dummyTable.DestroyRow(ac->m_localRow, ac);
                ac->m_localRow = nullptr;
                return nullptr;
            }
            ac->m_modifiedColumns.Init(bms, fieldCount);
        } else {
            ac->m_modifiedColumns.Reset(fieldCount);
        }

        rc = row->GetRow(type, this, ac->m_localRow, last_tid);
        if (__builtin_expect(rc != RC_OK, 0)) {
            if (rc != RC_ABORT) {  // do not log error if aborted due to cc conflict
                MOT_REPORT_ERROR(MOT_ERROR_INTERNAL, "Access Row", "Failed to get row");
            } else {
                // row can not be fetched due to a conflict, return RC_OK and nullptr
                rc = RC_OK;
            }
            m_dummyTable.DestroyRow(ac->m_localRow, ac);
            ac->m_localRow = nullptr;
            return nullptr;
        }
        ac->m_localRow->SetPrimarySentinel(const_cast<Row*>(row)->GetPrimarySentinel());
        ac->m_params.SetRowCommited();
        ac->m_localRow->SetRowId(row->GetRowId());
        ac->m_localRow->CopySurrogateKey(row);
        ac->m_localRow->SetCommitSequenceNumber(last_tid);
    } else {
        // For inserts local-row is the orig-row
        last_tid = row->GetCommitSequenceNumber();  // 0;
        ac->m_params.UnsetRowCommited();
        ac->m_localInsertRow = const_cast<Row*>(row);
    }
    ac->m_type = type;
    ac->m_tid = last_tid;
    MOT_LOG_DEBUG("Row Count = %d, access_set_size = %d", m_rowCnt, m_accessSetSize);
    m_rowCnt++;
    return ac;
}

bool TxnAccess::CreateNewAccess(Table* table, AccessType type)
{
    // row_cnt should be smaller than the array size
    if (unlikely(m_rowCnt >= (m_accessSetSize - 1))) {
        bool rc = ReallocAccessSet();
        if (rc == false) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Create Access", "Failed to reallocate access set");
            return rc;
        }
    }
    m_accessesSetBuff[m_rowCnt] = new (std::nothrow) Access(m_rowCnt);
    Access* ac = GetAccessPtr(m_rowCnt);
    if (__builtin_expect(ac == nullptr, 0)) {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL, "Create Access", "Failed to get access pointer");
        return false;
    }

    // Must create special Row object with maximal 'data' payload since this
    // Row object can be reused by different getRow() calls for different Tables.
    if (type != AccessType::INS) {
        Row* new_row = m_dummyTable.CreateNewRow(table, ac);
        if (__builtin_expect(new_row == nullptr, 0)) {
            delete ac;
            m_accessesSetBuff[m_rowCnt] = nullptr;
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Create Access", "Failed to create new row");
            return false;
        }
        new_row->m_table = table;
        ac->m_localRow = new_row;

        int fieldCount = table->GetFieldCount() - 1;
        uint8_t* bms = m_dummyTable.CreateBitMapBuffer(fieldCount);
        if (__builtin_expect(bms == nullptr, 0)) {
            // if modified_columns allocation failed, release allocated row
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Create Access", "Failed to create columns bitmap");
            m_dummyTable.DestroyRow(ac->m_localRow, ac);
            ac->m_localRow = nullptr;
            delete ac;
            m_accessesSetBuff[m_rowCnt] = nullptr;
            return false;
        }
        ac->m_modifiedColumns.Init(bms, fieldCount);
    } else {
        ac->m_localRow = nullptr;
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
    return nullptr;
}

RC TxnAccess::AccessLookup(const AccessType type, Sentinel* const originalSentinel, Row*& r_local_Row)
{

    if (m_rowCnt == 0) {
        return RC::RC_LOCAL_ROW_NOT_FOUND;
    }
    // type is the external operation.. for access the operation is always RD
    void* key = nullptr;
    Access* curr_acc = nullptr;

    /*
     * 2-Level caching
     * Minimum is 2 cache misses for first access.
     * For second mapped access Worse case 2 misses,
     * Average 1 cache miss.
     * Search:try look for row - if row not found search for sentinel!(INS type)
     * If Row found and not delete return the row
     * If Row is in Del verify we dont have an INS on top of the DEL
     */
    if (originalSentinel->IsCommited()) {
        key = (void*)originalSentinel;
        curr_acc = RowLookup(key);
        // Maybe Our Insert got commited!
        if (curr_acc == nullptr) {
            key = originalSentinel->GetPrimarySentinel();
            curr_acc = RowLookup(key);
        }
    } else {
        key = (void*)originalSentinel;
        curr_acc = RowLookup(key);
    }
    r_local_Row = nullptr;
    if (curr_acc != nullptr) {
        // Filter rows
        switch (curr_acc->m_type) {
            case AccessType::RD:
                if (m_txnManager->GetTxnIsoLevel() == READ_COMMITED) {
                    // If Cached row is not valid, remove it!!
                    if (type == RD and curr_acc->m_stmtCount != m_txnManager->GetStmtCount()) {
                        auto it = m_rowsSet->find(curr_acc->m_origSentinel);
                        MOT_ASSERT(it != m_rowsSet->end());
                        m_rowsSet->erase(it);
                        ReleaseAccess(curr_acc);
                        return RC::RC_LOCAL_ROW_NOT_FOUND;
                    }
                }
                break;
            case AccessType::RD_FOR_UPDATE:
            case AccessType::WR:
                break;
            case AccessType::DEL:
                return RC::RC_LOCAL_ROW_DELETED;
                break;
            case AccessType::INS:
                if (m_txnManager->GetStmtCount() != 0 && curr_acc->m_stmtCount == m_txnManager->GetStmtCount()) {
                    return RC::RC_LOCAL_ROW_NOT_VISIBLE;
                }
                // If current state is insert and next state is delete
                // do not alloacte new row
                if (curr_acc->m_localRow == nullptr and (type == AccessType::WR or type == AccessType::RD_FOR_UPDATE)) {
                    Table* table = curr_acc->GetRowFromHeader()->GetTable();
                    Row* new_row = m_dummyTable.CreateNewRow(table, curr_acc);
                    if (__builtin_expect(new_row == nullptr, 0)) {
                        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Lookup Access", "Failed to create new row");
                        return RC_MEMORY_ALLOCATION_ERROR;
                    }

                    int fieldCount = table->GetFieldCount() - 1;
                    uint8_t* bms = m_dummyTable.CreateBitMapBuffer(fieldCount);
                    if (__builtin_expect(bms == nullptr, 0)) {
                        // if modified_columns allocation failed, release allocated row
                        m_dummyTable.DestroyRow(new_row, curr_acc);
                        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Lookup Access", "Failed to create columns bitmap");
                        return RC::RC_MEMORY_ALLOCATION_ERROR;
                    }

                    new_row->m_table = table;
                    curr_acc->m_localRow = new_row;
                    curr_acc->m_modifiedColumns.Init(bms, fieldCount);
                }
                break;
            default:
                break;
        }
    } else {
        return RC::RC_LOCAL_ROW_NOT_FOUND;
    }

    r_local_Row = curr_acc->GetTxnRow();
    return RC::RC_LOCAL_ROW_FOUND;
}

Row* TxnAccess::MapRowtoLocalTable(const AccessType type, Sentinel* const& originalSentinel, RC& rc)
{
    Access* current_access = nullptr;
    rc = RC_OK;

    current_access = GetNewRowAccess(originalSentinel->GetData(), type, rc);
    // Check if draft is valid
    if (current_access == nullptr)
        return nullptr;

    // Set Last access
    SetLastAccess(current_access);
    current_access->m_origSentinel = reinterpret_cast<Sentinel*>(originalSentinel->GetPrimarySentinel());

    current_access->m_params.SetPrimarySentinel();
    // We map the p_sentinel for the case of commited Row!
    void* key = (void*)current_access->m_origSentinel;
    m_rowsSet->insert(RowAccessPair_t(key, current_access));
    return current_access->m_localRow;
}

Row* TxnAccess::AddInsertToLocalAccess(Sentinel* org_sentinel, Row* org_row, RC& rc, bool isUpgrade)
{
    rc = RC_OK;
    Access* curr_access = nullptr;

    // Search key from the unordered_map
    auto search = m_rowsSet->find(org_sentinel);
    if (likely(search == m_rowsSet->end())) {
        if (isUpgrade == true) {
            curr_access = GetNewRowAccess(org_sentinel->GetData(), INS, rc);
            // Check if draft is valid
            if (curr_access == nullptr) {
                return nullptr;
            }
            curr_access->m_params.SetUpgradeInsert();
            // First row in the transaction, updrade is on deleted commited row
            // Deleter did not remove the row due to checkpoint or concurrent inserter
            // DummyDeletedRow - No need to serialize the row
            curr_access->m_params.SetDummyDeletedRow();
            // First Insert in the transaction
            curr_access->m_auxRow = org_row;
        } else {
            curr_access = GetNewRowAccess(org_row, INS, rc);
            // Check if draft is valid
            if (curr_access == nullptr) {
                return nullptr;
            }
            curr_access->m_stmtCount = m_txnManager->GetStmtCount();
        }

        curr_access->m_origSentinel = org_sentinel;
        if (org_sentinel->IsPrimaryIndex()) {
            curr_access->m_params.SetPrimarySentinel();
        } else {
            curr_access->m_params.SetSecondarySentinel();
            if (org_sentinel->GetIndex()->GetUnique()) {
                curr_access->m_params.SetUniqueIndex();
            }
        }

        m_rowsSet->insert(RowAccessPair_t(org_sentinel, curr_access));
        return curr_access->GetTxnRow();

    } else {
        Access* curr_access = search->second;
        // Promote state of row to Delete
        if (curr_access->m_type == DEL) {
            // If we found a deleted row and the sentinel is not commited!
            // the row was deleted in between statement - we must abort!
            if (isUpgrade == false) {
                MOT_LOG_ERROR("Invalid Insert! Row was deleted before i was able to delete!\n")
                rc = RC_UNIQUE_VIOLATION;
                return nullptr;
            }
            curr_access->m_type = INS;
            curr_access->m_params.SetUpgradeInsert();
            // row to be commited
            curr_access->m_auxRow = org_row;
            return curr_access->GetTxnRow();
        }
    }
    rc = RC_UNIQUE_VIOLATION;
    return nullptr;
}

static const char* const enTxnStates[] = {stringify(INV),
    stringify(RD),
    stringify(RD_FOR_UPDATE),
    stringify(WR),
    stringify(DEL),
    stringify(INS),
    stringify(SCAN),
    stringify(TEST)};

enum NS_ACTIONS : uint32_t { NOCHANGE, INC_WRITES, FILTER_DELETES, GENERATE_ACCESS, NS_ERROR };

typedef union {
    struct {
        uint64_t next_state : 32;

        uint64_t action : 32;
    } entry;

    uint64_t val;
} Table_Entry;

static const Table_Entry txnStateMachine[TSM_SIZE][TSM_SIZE] = {
    /* INVALID STATE */
    {{INV, NS_ACTIONS::NS_ERROR},               // INV
        {RD, NS_ACTIONS::NOCHANGE},             // RD
        {RD_FOR_UPDATE, NS_ACTIONS::NOCHANGE},  // RD_FOT_UPDATE
        {WR, NS_ACTIONS::INC_WRITES},           // WR
        {DEL, NS_ACTIONS::GENERATE_ACCESS},     // DEL
        {INS, NS_ACTIONS::INC_WRITES}},         // INS
    /* READ STATE */
    {{RD, NS_ACTIONS::NS_ERROR},
        {RD, NS_ACTIONS::NOCHANGE},
        {RD_FOR_UPDATE, NS_ACTIONS::NOCHANGE},
        {WR, NS_ACTIONS::INC_WRITES},
        {DEL, NS_ACTIONS::GENERATE_ACCESS},
        {RD, NS_ACTIONS::NS_ERROR}},
    /* READ_FOR_UPDATE STATE */
    {{RD_FOR_UPDATE, NS_ACTIONS::NS_ERROR},
        {RD_FOR_UPDATE, NS_ACTIONS::NOCHANGE},
        {RD_FOR_UPDATE, NS_ACTIONS::NOCHANGE},
        {WR, NS_ACTIONS::INC_WRITES},
        {DEL, NS_ACTIONS::GENERATE_ACCESS},
        {RD_FOR_UPDATE, NS_ACTIONS::NS_ERROR}},
    /* WRITE STATE */
    {{WR, NS_ACTIONS::NS_ERROR},
        {WR, NS_ACTIONS::NOCHANGE},
        {WR, NS_ACTIONS::NOCHANGE},
        {WR, NS_ACTIONS::NOCHANGE},
        {DEL, NS_ACTIONS::GENERATE_ACCESS},
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
    RC rc = RC_OK;
    MOT_LOG_DEBUG("Switch key State from: %s to: %s", enTxnStates[ac->m_type], enTxnStates[type]);
    AccessType current_state = ac->m_type;

    auto result = txnStateMachine[current_state][type];
    if (unlikely(result.entry.action == NS_ACTIONS::NS_ERROR)) {
        MOT_LOG_ERROR("Invalid State: current_state=%d, type=%d\n", (int)current_state, (int)type);
        return RC_ERROR;
    }

    ac->m_type = (AccessType)result.entry.next_state;
    bool res = false;
    NS_ACTIONS action = (NS_ACTIONS)result.entry.action;
    switch (action) {
        case NS_ACTIONS::FILTER_DELETES:
            // Check and filter the access_set for cases of delete after insert in the same transaction.
            res = FilterOrderedSet(ac);
            break;
        case NS_ACTIONS::GENERATE_ACCESS:
            rc = GenerateDeletes(ac);
            break;
        case NS_ACTIONS::INC_WRITES:
            MOT_LOG_DEBUG(
                "ACTION REQUIRED Switch key state from:%s, to:%s", enTxnStates[current_state], enTxnStates[type]);
            break;
        case NS_ACTIONS::NOCHANGE:
            MOT_LOG_DEBUG(
                "NO ACTION REQUIRED Switch key state from:%s, to:%s", enTxnStates[current_state], enTxnStates[type]);
            break;
        case NS_ACTIONS::NS_ERROR:
            MOT_LOG_DEBUG(
                "ERROR in state Switch key state from:%s, to:%s", enTxnStates[current_state], enTxnStates[type]);
            rc = RC_ERROR;
            break;
    }

    return rc;
}

Row* TxnAccess::GetReadCommitedRow(Sentinel* sentinel)
{
    TransactionId last_tid;
    if (likely(sentinel->IsCommited() == true)) {
        Row* row = sentinel->GetData();
        RC rc = row->GetRow(AccessType::RD, this, m_rowZero, last_tid);
        if (rc != RC::RC_OK) {
            return nullptr;
        } else {
            m_rowZero->SetCommitSequenceNumber(last_tid);
            m_rowZero->SetPrimarySentinel(row->GetPrimarySentinel());
            return m_rowZero;
        }
    } else
        return nullptr;
}
RC TxnAccess::GenerateDeletes(Access* element)
{
    RC rc = RC_OK;
    Sentinel* outSentinel = nullptr;
    MaxKey key;
    Row* row = element->GetTxnRow();
    Table* table = row->GetTable();
    uint16_t numIndexes = table->GetNumIndexes();
    for (uint16_t i = 1; i < numIndexes; i++) {
        Index* ix = table->GetIndex(i);
        key.InitKey(ix->GetKeyLength());
        ix->BuildKey(table, row, &key);
        rc = table->FindRowByIndexId(ix, &key, outSentinel, m_txnManager->GetThdId());
        if (rc != RC_OK) {
            // row already deleted!! Abort
            m_txnManager->m_err = RC_SERIALIZATION_FAILURE;
            return RC_SERIALIZATION_FAILURE;
        }
        // generate New Accesses!
        Access* current_access = GetNewRowAccess(element->GetRowFromHeader(), DEL, rc);
        if (current_access == nullptr) {
            return rc;
        }
        current_access->m_origSentinel = outSentinel;
        current_access->m_params.SetSecondarySentinel();
        if (current_access->m_origSentinel->GetIndex()->GetUnique()) {
            current_access->m_params.SetUniqueIndex();
        }
        m_rowsSet->insert(RowAccessPair_t(outSentinel, current_access));
    }
    return rc;
}
bool TxnAccess::FilterOrderedSet(Access* element)
{
    RC rc = RC_OK;
    bool res = false;
    // no need to filter current delete
    // the delete is performed on an index item
    if (element->m_origSentinel == nullptr) {
        return false;
    } else {
        MOT_ASSERT(element->m_type == DEL);
        // revert type to INS
        element->m_type = INS;
        MaxKey max_key;
        Sentinel* outSentinel = nullptr;
        Key* key = nullptr;
        // there are other indices elements that need to be removed.
        // case of BEGIN->INS->DEL
        // For blocked INSERTS the key is the orig_sentienl and the shared element is the orig_row!
        // Lets get all the keys and remove them from the cache and try to remove from the Index if possible
        Row* row = element->GetTxnRow();
        Table* table = row->GetTable();
        uint16_t numIndexes = table->GetNumIndexes();
        for (uint16_t i = 0; i < numIndexes; i++) {
            Index* ix = table->GetIndex(i);
            key = &max_key;
            key->InitKey(ix->GetKeyLength());
            ix->BuildKey(table, row, key);
            rc = table->FindRowByIndexId(ix, key, outSentinel, m_txnManager->GetThdId());
            auto it = m_rowsSet->find(outSentinel);
            // Filter Rows
            if (it != m_rowsSet->end()) {
                Access* ac = (*it).second;
                res = true;
                rc = m_txnManager->RollbackInsert(ac);
                if (ac->m_params.IsUpgradeInsert() == false or ac->m_params.IsDummyDeletedRow() == true) {
                    m_rowsSet->erase(it);
                    // need to perform index clean-up!
                    ReleaseAccess(ac);
                } else {
                    // Transform INS to DEL
                    ac->m_type = DEL;
                    MOT_ASSERT(ac->m_auxRow != nullptr);
                    ac->m_params.UnsetUpgradeInsert();
                    ac->m_auxRow = nullptr;
                }
            }
        }
    }

    return res;
}
}  // namespace MOT
