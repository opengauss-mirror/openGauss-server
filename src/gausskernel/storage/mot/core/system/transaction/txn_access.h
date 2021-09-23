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

#include <stdint.h>

#include <map>
#include <unordered_map>
#include "global.h"
#include "key.h"
#include "row.h"
#include "sentinel.h"
#include "utilities.h"
#include "txn_local_allocators.h"
#include "access_params.h"
#include "access.h"
#include "bitmapset.h"

namespace MOT {
// forward declarations
class Table;
class Row;
class TxnManager;

/** @var Transaction State Machine size. */
constexpr uint8_t TSM_SIZE = 6;
/** @var default access size   */
constexpr uint32_t DEFAULT_ACCESS_SIZE = 500;

typedef std::pair<void*, Access*> RowAccessPair_t;

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
     * @param table The table in which the row is to be found.
     * @param type The new row state to update.
     * @param key The pointer to the key buffer.
     * @param keyLen The length of the key buffer.
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
     * @brief Stores row access entry in the local cache.
     * @param table The table to which the row belongs.
     * @param type The new row state to update.
     * @param originalRow The original row to store
     * @return The row as it is stored in the cache.
     */
    Row* MapRowtoLocalTable(const AccessType type, Sentinel* const& originalSentinel, RC& rc);

    /**
     * @brief Applies row state changes according to state machine rules.
     * @param type The new row access type.
     * @param ac The row access entry.
     * @return The operation result.
     */
    RC UpdateRowState(AccessType type, Access* ac);

    /**
     * @brief Retrieves a row using the concurrency control mechanism.
     * @param row The row to retrieve.
     * @param type The type of access required.
     * @return The row access entry.
     */
    Access* GetNewRowAccess(const Row* row, AccessType type, RC& rc);

    /**
     * @brief Retrieves the last row access entry that was accessed.
     * @return The last row access entry.
     */
    inline Access* GetLastAccess()
    {
        return m_lastAcc;
    }

    inline TxnOrderedSet_t& GetOrderedRowSet()
    {
        return *m_rowsSet;
    }

    inline void IncreaseTableStat(Table* t)
    {
        m_tableStat[t]++;
    }

    inline void RemoveTableFromStat(Table* t)
    {
        m_tableStat.erase(t);
    }

    /**
     * @brief Retrieves all the used tables and try to clean the cache if possible.
     */
    inline void ClearTableCache()
    {
        for (auto& table : m_tableStat) {
            if (table.second > DEFAULT_ACCESS_SIZE) {
                MOT_LOG_INFO(
                    "ClearTableStat: Table = %s items = %lu\n", table.first->GetTableName().c_str(), table.second);
                table.first->ClearRowCache();
            }
        }
        m_tableStat.clear();
    }

    /**
     * @brief Adds an "insert-row" row access entry into the local cache.
     * @param org_row The row to update its state.
     * @param type The access type (can be INS or INS_CMT).
     * @return The row as it is stored in the cache (i.e. up-to-date before commit).
     */
    Row* AddInsertToLocalAccess(Sentinel* org_sentinel, Row* org_row, RC& rc, bool isUpgrade = false);

    /**
     * @brief For Read-Commited we return a copy of the current row
     * @param sentinel The row-header
     * @return row zero with current copy
     */
    Row* GetReadCommitedRow(Sentinel* sentinel);

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
    RC GenerateDeletes(Access* element);

    // Temporarily added since releaseAccess is private
    void PubReleaseAccess(Access* access)
    {
        ReleaseAccess(access);
    }

    TxnInsertAction* GetInsertMgr() const
    {
        return m_insertManager;
    }

    /**
     * @brief Retrieves the row access entry stored at the specified index.
     * @param index The row access entry index.
     * @return The row access entry.
     */
    inline Access* GetAccessPtr(uint64_t index) const
    {
        if (index >= m_accessSetSize) {
            return nullptr;
        }
        return m_accessesSetBuff[index];
    }

    /** @var The access set. */
    Access** m_accessesSetBuff = nullptr;

    /** @var The number of rows stored in the access set array. */
    uint32_t m_rowCnt = 0;

    /** @var The number of allocated accesses in the access set array. */
    uint32_t m_allocatedAc = 0;

    /** @var The initial access set array size. */
    uint32_t m_accessSetSize = DEFAULT_ACCESS_SIZE;

private:
    static constexpr uint32_t ACCESS_SET_EXTEND_FACTOR = 2;

    /** @var The transaction for which this cache is maintained. */
    TxnManager* m_txnManager = nullptr;

    /** @var */
    TxnInsertAction* m_insertManager = nullptr;

    /** @var Maintains last row access entry that was accessed. */
    Access* m_lastAcc = nullptr;

    /** @var Maintains ordered Access pointers by row address */
    TxnOrderedSet_t* m_rowsSet = nullptr;

    /** @var row_zero Used for read-only txn in
     *  RC isolation level
     * */
    Row* m_rowZero = nullptr;

    /** @var m_dummyTable Points to a dummy table with tuple size of MAX_TUPLE_SIZE
     * used for transaction row caching
     **/
    DummyTable m_dummyTable;

    /** @var m_tableStat Used for table cache management */
    std::unordered_map<Table*, uint32_t> m_tableStat;

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
     * @param table The table for which a row access entry id to be created.
     * @return Boolean value denoting success or failure.
     */
    bool CreateNewAccess(Table* table, AccessType type);

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

    /** @brief Doubles the size of the access set. */
    bool ReallocAccessSet();

    /** @brief Doubles the size of the access set. */
    void ShrinkAccessSet();

    /** @brief Release all objects to the memory pool   */
    void ClearAccessSet();

    /** @var Manage safe initialization/destruction in case of failure. */
    enum InitPhase { Startup, AllocBuf, InitDummyTab, InitDummyInd, CreateRowZero, CreateRowSet, Done } m_initPhase;

    DECLARE_CLASS_LOGGER();
};
}  // namespace MOT

#endif  // TXN_ACCESS_H
