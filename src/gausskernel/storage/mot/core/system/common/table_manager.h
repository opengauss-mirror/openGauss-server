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
 * table_manager.h
 *    Manages all APIs related to memory optimized table life-cycle.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/common/table_manager.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef TABLE_MANAGER_H
#define TABLE_MANAGER_H

#include "table.h"
#include "session_context.h"
#include "utilities.h"
#include "rw_lock.h"

#include <stdint.h>
#include <map>
#include <list>
#include <mutex>

namespace MOT {
/**
 * @class TableManager.
 * @brief Manages all APIs related to memory optimized table life-cycle.
 */
class TableManager {
public:
    TableManager()
    {}
    ~TableManager()
    {}

    /**
     * @brief Adds a table to the engine.
     * @detail A table is built manually be the caller before being added to the engine (see Table object API).
     * @param table The table to add.
     * @return Zero if the operation succeeded or an error code.
     */
    bool AddTable(Table* table);

    /**
     * @brief Deletes a table from the engine. The table object is also reclaimed.
     * @param table The table to delete.
     * @param sessionContext The session context for the operation.
     * @return Zero if the operation succeeded or an error code.
     */
    inline RC DropTable(Table* table, SessionContext* sessionContext)
    {
        if (table == nullptr) {
            return RC_ERROR;
        }

        MOT_LOG_INFO("Dropping table %s", table->GetLongTableName().c_str());
        RC status = DropTableInternal(table, sessionContext);
        delete table;
        return status;
    }

    /**
     * @brief Retrieves a table from the engine.
     * @param tableId The internal (engine-given) identifier of the table to retrieve.
     * @return The table object or null pointer if not found.
     * @note It is assumed that the envelope guards against concurrent removal of the table.
     */
    inline Table* GetTable(InternalTableId tableId)
    {
        Table* table = nullptr;

        m_rwLock.RdLock();
        InternalTableMap::iterator it = m_tablesById.find(tableId);
        if (it != m_tablesById.end()) {
            table = it->second;
        }
        m_rwLock.RdUnlock();

        return table;
    }

    /**
     * @brief Retrieves a locked table from the engine. Caller is responsible for unlocking the table when done using
     * it, by calling @ref Table::Unlock.
     * @param tableId The external (envelope) identifier of the table to retrieve.
     * @return The table object or null pointer if not found.
     * @note It is assumed that the envelope guards against concurrent removal of the table.
     */
    inline Table* GetTableSafeByExId(ExternalTableId tableId)
    {
        Table* table = nullptr;
        m_rwLock.RdLock();
        ExternalTableMap::iterator it = m_tablesByExId.find(tableId);
        if (it != m_tablesByExId.end()) {
            table = it->second;
            if (table != nullptr) {
                table->RdLock();
            }
        }
        m_rwLock.RdUnlock();
        return table;
    }

    /**
     * @brief Queries whether a table exists. All given details must match.
     * @param internalId The internal (MOT-given) table identifier.
     * @param externalId The external (envelope-given) table identifier.
     * @param name The short table name.
     * @param longName The long (fully-qualified) table name.
     */
    inline bool VerifyTableExists(
        InternalTableId internalId, ExternalTableId externalId, const std::string& name, const std::string& longName)
    {
        bool ret = false;
        Table* table = GetTableSafeByExId(externalId);
        if (table != nullptr) {
            ret = ((table->GetTableName() == name) && (table->GetLongTableName() == longName) &&
                   (table->GetTableId() == internalId));
            table->Unlock();
        }
        return ret;
    }

    /**
     * @brief Retrieves a table by its external identifier.
     * @param tableId The external (envelope-given) identifier of the table to retrieve.
     * @return The table object or null pointer if not found.
     */
    inline Table* GetTableByExternal(ExternalTableId tableId)
    {
        Table* table = nullptr;
        m_rwLock.RdLock();
        ExternalTableMap::iterator it = m_tablesByExId.find(tableId);
        if (it != m_tablesByExId.end()) {
            table = it->second;
        }
        m_rwLock.RdUnlock();
        return table;
    }

    /**
     * @brief Retrieves a table by its name.
     * @param name The table name.
     * @return The table object or null pointer if not found.
     */
    inline Table* GetTable(const std::string& name)
    {
        Table* table = nullptr;
        m_rwLock.RdLock();
        NameTableMap::iterator it = m_tablesByName.find(name);
        if (it != m_tablesByName.end()) {
            table = it->second;
        }
        m_rwLock.RdUnlock();
        return table;
    }

    /**
     * @brief Retrieves a table by its name.
     * @param name The table name.
     * @return The table object or null pointer if not found.
     */
    inline Table* GetTable(const char* name)
    {
        const std::string nameStr(name);
        return GetTable(nameStr);
    }

    /**
     * @brief Adds the pointers of all tables into a list.
     * @param[out] tablesQueue Receives all the tables.
     * @return The number of tables added.
     */
    inline uint32_t AddTablesToList(std::list<Table*>& tablesQueue)
    {
        m_rwLock.RdLock();
        for (InternalTableMap::iterator it = m_tablesById.begin(); it != m_tablesById.end(); ++it) {
            tablesQueue.push_back(it->second);
            // lock the table, so it won't get deleted/truncated
            it->second->RdLock();
        }
        m_rwLock.RdUnlock();
        return (uint32_t)tablesQueue.size();
    }

    /** @brief Clears all object-pool table caches for the current thread. */
    void ClearTablesThreadMemoryCache();

    /** @brief Clears all tables and all releases all associated resources. */
    void ClearAllTables();

private:
    /**
     * @brief Helper method for table dropping.
     * @param table The table to delete.
     * @param sessionContext The session context for the operation.
     * @return Zero if the operation succeeded or an error code.
     */
    inline RC DropTableInternal(Table* table, SessionContext* sessionContext)
    {
        m_rwLock.WrLock();
        m_tablesById.erase(table->GetTableId());
        m_tablesByExId.erase(table->GetTableExId());
        m_tablesByName.erase(table->GetLongTableName());
        m_rwLock.WrUnlock();
        sessionContext->GetTxnManager()->RemoveTableFromStat(table);
        return table->DropImpl();
    }

    /** @typedef internal table map */
    typedef std::map<uint32_t, Table*> InternalTableMap;

    /** @typedef external table map */
    typedef std::map<uint64_t, Table*> ExternalTableMap;

    /** @typedef names-based table map */
    typedef std::map<std::string, Table*> NameTableMap;

    /** @var Table map indexed by internal (engine-given) identifier.. */
    InternalTableMap m_tablesById;

    /** @var Table map indexed by external (envelope-given) identifier. */
    ExternalTableMap m_tablesByExId;

    /* @var Table map indexed by name. */
    NameTableMap m_tablesByName;

    /** @var Synchronize table insert/delete in 3 concurrent maps. */
    RwLock m_rwLock;

    DECLARE_CLASS_LOGGER()
};
}  // namespace MOT

#endif /* TABLE_MANAGER_H */
