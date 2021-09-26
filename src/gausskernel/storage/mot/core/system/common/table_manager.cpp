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
 * table_manager.cpp
 *    Manages all APIs related to memory optimized table life-cycle.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/common/table_manager.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "table_manager.h"

namespace MOT {
IMPLEMENT_CLASS_LOGGER(TableManager, System);

bool TableManager::AddTable(Table* table)
{
    MOT_LOG_INFO(
        "Adding table %s with external id: %" PRIu64, table->GetLongTableName().c_str(), table->GetTableExId());
    m_rwLock.WrLock();
    InternalTableMap::iterator it = m_tablesById.find(table->GetTableId());
    if (it != m_tablesById.end()) {
        m_rwLock.WrUnlock();
        MOT_LOG_ERROR(
            "The table [%s] with id: %u already exists", table->GetLongTableName().c_str(), table->GetTableId());
        return false;
    }

    m_tablesById[table->GetTableId()] = table;
    m_tablesByExId[table->GetTableExId()] = table;
    m_tablesByName[table->GetLongTableName()] = table;
    m_rwLock.WrUnlock();
    return true;
}

void TableManager::ClearTablesThreadMemoryCache()
{
    m_rwLock.RdLock();
    InternalTableMap::iterator it = m_tablesById.begin();
    while (it != m_tablesById.end()) {
        // lock table to prevent concurrent ddl and truncate operations
        it->second->RdLock();
        it->second->ClearThreadMemoryCache();
        it->second->Unlock();
        it++;
    }
    m_rwLock.RdUnlock();
}

void TableManager::ClearAllTables()
{
    // clear all table maps
    InternalTableMap::iterator it = m_tablesById.begin();
    while (it != m_tablesById.end()) {
        Table* table = it->second;
        delete table;
        ++it;
    }

    m_tablesById.clear();
    m_tablesByName.clear();
    m_tablesByExId.clear();
}
}  // namespace MOT
