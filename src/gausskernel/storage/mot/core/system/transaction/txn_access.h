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
 * txn_access.h
 *    Cache manager for current transaction.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/transaction/txn_access.h
 *
 * -------------------------------------------------------------------------
 */

#pragma once

#ifndef TXN_ACCESS_H
#define TXN_ACCESS_H

#include <cstdint>

#include <map>
#include <unordered_map>
#include "global.h"
#include "key.h"
#include "row.h"
#include "sentinel.h"
#include "utilities.h"
#include "txn_local_allocators.h"
#include "txn_insert_action.h"
#include "access_params.h"
#include "access.h"
#include "bitmapset.h"

namespace MOT {
// forward declarations
class Table;
class Row;
class TxnManager;

typedef std::pair<void*, Access*> RowAccessPair_t;

using S_SentinelNodePool = unordered_multimap<Index*, PrimarySentinelNode*>;

typedef DummyIndex DummyIndexImpl;

typedef std::map<void*, Access*, std::less<void*>> TxnOrderedSet_t;

/**
 * @class TxnAccess
 * @brief Cache manager for current transaction
 *
 */
class alignas(64) TxnAccess {
public:
    /** Constructor. */
    TxnAccess();

    /** Destructor. */
    ~TxnAccess();

    /**
     * @brief Initializes the transaction cache.
     * @param manager The transaction.
     * @return Boolean value denoting success or failure.
     */
    bool Init(TxnManager* manager);

    /** @brief Clears all cached data. */
    void ClearSet(void);

    /**
     * @brief Return Access object to memory pools
     * @param access
     */
    void DestroyAccess(Access* access);

    /**
     * @brief Searches for a row in the local cache, and if found, promotes the
     * row state accordingly.
     * @param currentKey The key where the row is mapped to
     * @return The cached row or null pointer if the row is not cached.
     */
    Access* RowLookup(void* const currentKey);

    /**
     * @brief Searches for a row in the cache using the sentinel as a key
     * @param type The operation requested RD/WR/DEL/INS
     * @param originalSentinel The key for the cache
     * @param r_local_Row The cached row or null pointer if the row is not cached.
     * @return
     */
    RC AccessLookup(const AccessType type, Sentinel* const originalSentinel, Row*& r_local_Row);

    /**
     * @brief Searches for a row in the cache using the sentinel as a key
     * @param sentinel The key for the cache
     * @return
     */
    RC CheckDuplicateInsert(Sentinel* const sentinel);

    /**
     * @brief Stores row access entry in the local cache.
     * @param table The table to which the row belongs.
     * @param type The new row state to update.
     * @param originalRow The original row to store
     * @return The row as it is stored in the cache.
     */
    Row* MapRowtoLocalTable(const AccessType type, Sentinel* const& originalSentinel, RC& rc);

    /**
     * @brief Fetch Row from PS when SS is Mapped.
     * @param type The new row state to update.
     * @param originalSentinel The global secondary sentinel
     * @return The row as it is stored in the cache.
     */
    Row* FetchRowFromPrimarySentinel(const AccessType type, Sentinel* const& originalSentinel, RC& rc);

    /**
     * @brief Applies row state changes according to state machine rules.
     * @param type The new row access type.
     * @param ac The row access entry.
     * @return The operation result.
     */
    RC UpdateRowState(AccessType type, Access* ac);

    /**
     * @brief Generate a new Access object for the current inserted sentinel
     * @param sentinel The inserted sentinel.
     * @param row the new row.
     * @param isUpgrade is the insert mode == upgrade
     * @return The new access entry.
     */
    Access* GetNewInsertAccess(Sentinel* const& sentinel, Row* row, RC& rc, bool isUpgrade);

    /**
     * @brief Generate a new Access object for the current modified key
     * @param sentinel The inserted sentinel.
     * @param rc return value.
     * @return The new access entry.
     */
    Access* GetNewRowAccess(Sentinel* const& originalSentinel, AccessType type, RC& rc);

    /**
     * @brief Generate a new Access object for the current secondary inserted sentinel
     * @param sentinel The inserted sentinel.
     * @param primary_access the primary access info.
     * @param rc return value.
     * @return The new access entry.
     */
    Access* GetNewSecondaryAccess(Sentinel* const& originalSentinel, Access* primary_access, RC& rc);

    /**
     * @brief Retrieves the last row access entry that was accessed.
     * @return The last row access entry.
     */
    inline Access* GetLastAccess()
    {
        return m_lastAcc;
    }

    inline Row* GetLastAccessedDraft()
    {
        return m_lastAcc->GetLocalVersion();
    }

    inline TxnOrderedSet_t& GetOrderedRowSet()
    {
        return *m_rowsSet;
    }

    inline S_SentinelNodePool* GetSentinelObjectPool()
    {
        return m_sentinelObjectPool;
    }

    inline void IncreaseTableStat(Table* t)
    {
        m_tableStat[t->GetTableExId()]++;
    }

    inline void RemoveTableFromStat(Table* t)
    {
        (void)m_tableStat.erase(t->GetTableExId());
    }

    inline uint32_t Size() const
    {
        return m_rowCnt;
    }

    /**
     * @brief Retrieves all the used tables and try to clean the cache if possible.
     */
    void ClearTableCache();

    void ClearDummyTableCache()
    {
        m_dummyTable.ClearRowCache();
    }

    /**
     * @brief Adds an "insert-row" row access entry into the local cache.
     * @param org_row The row to update its state.
     * @param type The access type (can be INS or INS_CMT).
     * @return The row as it is stored in the cache (i.e. up-to-date before commit).
     */
    Row* AddInsertToLocalAccess(Sentinel* org_sentinel, Row* org_row, RC& rc, bool isUpgrade = false);

    /**
     * @brief Get a visible version for the sentinel
     * @param sentinel The row-header
     * @param csn the txn snapshot
     * @return Visible row version
     */
    Row* GetVisibleRowVersion(Sentinel* sentinel, uint64_t csn);

    /**
     * @brief Get a primary sentinel from a secondary sentinel
     * @param sentinel The row-header
     * @param csn the txn snapshot
     * @return Visible row version
     */
    PrimarySentinel* GetVisiblePrimarySentinel(Sentinel* sentinel, uint64_t csn);

    /**
     * @brief Get a visible row per operation type and isolation-level
     * @param sentinel The primary sentinel
     * @param the DML
     * @param rc return code
     * @return Visible row version
     */
    Row* GetVisibleRow(PrimarySentinel* sentinel, AccessType type, RC& rc);

    /**
     * @brief Undo insert operation if possible after delete
     * @param element Current row to be deleted
     * @param type
     * @return
     */
    bool FilterOrderedSet(Access* element);

    /**
     * @brief Generate all index accesses for current row
     * @param element The current deleted row
     * @return RC
     */
    RC GenerateSecondaryAccess(Access* primaryAccess, AccessType acType);

    /**
     * @brief Wrapper to Generate draft version
     * @param ac The new access object
     * @param acType the operation
     * @return RC
     */
    RC GenerateDraft(Access* ac, AccessType acType);

    /**
     * @brief Generate draft version and initialize access
     * @param ac The new access object
     * @param acType the operation
     * @return RC
     */
    RC CreateMVCCDraft(Access* ac, AccessType acType);

    /**
     * @brief Generate missing objects for update on indexed column
     * @return RC
     */
    RC GenerateSecondaryIndexUpdate(Access* primaryAccess, TxnIxColUpdate* colUpd);

    RC GeneratePrimaryIndexUpdate(Access* primaryAccess, TxnIxColUpdate* colUpd);

    // Temporarily added since releaseAccess is private
    void PubReleaseAccess(Access* access)
    {
        ReleaseAccess(access);
    }

    TxnInsertAction* GetInsertMgr()
    {
        return m_insertManager;
    }

    /**
     * @brief Retrieves the row access entry stored at the specified index.
     * @param index The row access entry index.
     * @return The row access entry.
     */
    inline Access* GetAccessPtr(uint64_t index)
    {
        if (index >= m_accessSetSize) {
            return nullptr;
        }
        return m_accessesSetBuff[index];
    }

    /**
     * @brief Perform GC maintenance after every commit.
     */
    void GcMaintenance();

    // those functions are called on add/drop column in case
    // we have updated rows in transaction and column nullbits has changed size
    RC AddColumn(Access* ac);
    void DropColumn(Access* ac, Column* col);

    /** @brief allocate new Access */
    Access* AllocNewAccess(uint32_t rowCnt)
    {
        Access* ac = m_accessPool->Alloc<Access>(rowCnt);
        if (ac == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Create Access", "Failed to create new Access");
        }
        return ac;
    }

    /** @brief release Access */
    void ReleaseAccessToPool(Access* obj)
    {
        m_accessPool->Release<Access>(obj);
    }

    /** @brief Rollback sub-transaction accesses */
    void RollbackSubTxn(uint32_t accessId)
    {
        return ReleaseSubTxnAccesses(accessId);
    }

    void Print();

    void PrintStats();

    bool IsTxnAccessEmpty()
    {
        if (m_rowCnt == 0 and GetInsertMgr()->IsInsertSetEmpty()) {
            return true;
        }
        return false;
    }

    Row* GetRowZeroCopyIfAny(Row* localRow);

    void GetRowZeroCopy(Row* localRow);

    void PrintPoolStats();

    /** @var Transaction State Machine size. */
    static constexpr uint8_t TSM_SIZE = 6;

    static const char* const enTxnStates[];

private:
    /** @var default access size   */
    static constexpr uint32_t DEFAULT_ACCESS_SIZE = 500;

    static constexpr uint32_t FREE_THREAD_CACHE_THRESHOLD = 1024;

    static constexpr uint32_t ACCESS_SET_EXTEND_FACTOR = 2;

    static constexpr uint32_t ACCESS_SET_PRINT_THRESHOLD = 100;

    /** @var The number of rows stored in the access set array. */
    uint32_t m_rowCnt = 0;

    /** @var The number of allocated accesses in the access set array. */
    uint32_t m_allocatedAc = 0;

    /** @var The initial access set array size. */
    uint32_t m_accessSetSize = DEFAULT_ACCESS_SIZE;

    /** @var The access set. */
    Access** m_accessesSetBuff = nullptr;

    /** @var The transaction for which this cache is maintained. */
    TxnManager* m_txnManager = nullptr;

    /** @var Access objects memory pool. */
    ObjAllocInterface* m_accessPool = nullptr;

    /** @var */
    TxnInsertAction* m_insertManager = nullptr;

    /** @var Maintains last row access entry that was accessed. */
    Access* m_lastAcc = nullptr;

    /** @var Maintains ordered Access pointers by row address */
    TxnOrderedSet_t* m_rowsSet = nullptr;

    /** @var row_zero Used for altered row (New-Column) */
    Row* m_rowZero = nullptr;

    /** @var sentinelObject pool for SU usage */
    S_SentinelNodePool* m_sentinelObjectPool = nullptr;

    /** @var m_dummyTable Points to a dummy table with tuple size of MAX_TUPLE_SIZE
     * used for transaction row caching
     **/
    DummyTable m_dummyTable;

    /** @var m_tableStat Used for table cache management */
    std::unordered_map<ExternalTableId, uint32_t> m_tableStat;

    /**
     * @brief Reset the row access entry stored at the specified index to nullptr.
     * @param index The row access entry index.
     */
    inline void ResetAccessPtr(uint64_t index)
    {
        if (index >= m_accessSetSize) {
            return;
        }
        m_accessesSetBuff[index] = nullptr;
    }

    /**
     * @brief Creates a new row access entry for the specified table and caches
     * it.
     * @return Boolean value denoting success or failure.
     */
    bool CreateNewAccess(AccessType type);

    /**
     * @brief Installs a new access set array.
     * @param set The new access set array.
     */
    inline void SetAccessesSet(Access** set)
    {
        m_accessesSetBuff = set;
    }

    /**
     * @brief Stores the last row access entry that was encountered.
     * @param acc The row access entry.
     */
    inline void SetLastAccess(Access* acc)
    {
        m_lastAcc = acc;
    }

    /**
     * @brief Release current Access to be reused
     * @param ac The current Access
     */
    void ReleaseAccess(Access* ac)
    {
        m_rowCnt--;
        uint32_t release_id = ac->GetBufferId();
        // Swap buffer_id's
        ac->SwapId(*m_accessesSetBuff[m_rowCnt]);
        m_accessesSetBuff[release_id] = m_accessesSetBuff[m_rowCnt];
        m_accessesSetBuff[m_rowCnt] = ac;
        DestroyAccess(ac);
    }

    /** @brief Rollback RD-ONLY Accesses. */
    void ReleaseSubTxnAccesses(uint32_t accessId);

    /** @brief Doubles the size of the access set. */
    bool ReallocAccessSet();

    /** @brief Doubles the size of the access set. */
    void ShrinkAccessSet();

    /** @brief Release all objects to the memory pool   */
    void ClearAccessSet();

    RC InitBitMap(Access* ac, int fieldCount);

    /** @var Manage safe initialization/destruction in case of failure. */
    enum InitPhase {
        Startup,
        AllocBuf,
        InitDummyTab,
        InitDummyInd,
        CreateRowZero,
        CreateRowSet,
        CreateInsertSet,
        CreateAccessPool,
        Done
    } m_initPhase;

    RC LookupFilterRow(Access* curr_acc, const AccessType type, bool isCommitted, Row*& r_local_Row);

    RC SecIdxUpdGenerateDeleteKeys(Access* primaryAccess, TxnIxColUpdate* colUpd);

    RC SecondaryAccessDeleteTxnRows(Row* row, Table* table);

    RC SecondaryAccessDeleteGlobalRows(Access* primaryAccess, Row* row, Table* table);

    DECLARE_CLASS_LOGGER();
};
}  // namespace MOT

#endif  // TXN_ACCESS_H
