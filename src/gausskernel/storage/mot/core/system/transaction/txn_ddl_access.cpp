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
 * txn_ddl_access.cpp
 *    Implements TxnDDLAccess which is used to cache and access transactional DDL changes.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/transaction/txn_ddl_access.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "txn_ddl_access.h"
#include "debug_utils.h"
#include "txn_table.h"

namespace MOT {
TxnDDLAccess::DDLAccess::DDLAccess()
{
    m_oid = 0;
    m_type = DDLAccessType::DDL_ACCESS_UNKNOWN;
    m_entry = nullptr;
}

void* TxnDDLAccess::DDLAccess::GetEntry()
{
    return m_entry;
}

void TxnDDLAccess::DDLAccess::Set(uint64_t oid, DDLAccessType accessType, void* ddlEntry)
{
    m_oid = oid;
    m_type = accessType;
    m_entry = ddlEntry;
}

void TxnDDLAccess::DDLAccess::ResetEntry()
{
    m_entry = nullptr;
}

TxnDDLAccess::TxnDDLAccess(TxnManager* txn) : m_txn(txn), m_initialized(false), m_size(0), m_accessList()
{}

void TxnDDLAccess::Init()
{
    m_initialized = true;
}

void TxnDDLAccess::Reset()
{
    for (int i = 0; i < m_size; i++) {
        delete m_accessList[i];
        m_accessList[i] = nullptr;
    }

    for (ExternalTableMap::iterator it = m_txnTableMap.begin(); it != m_txnTableMap.end(); (void)++it) {
        TxnTable* t = it->second;
        delete t;
    }
    m_txnTableMap.clear();
    m_size = 0;
}

RC TxnDDLAccess::Add(TxnDDLAccess::DDLAccess* ddlAccess)
{
    if (m_size == MAX_DDL_ACCESS_SIZE) {
        return RC_TXN_EXCEEDS_MAX_DDLS;
    }
    m_accessList[m_size++] = ddlAccess;
    return RC_OK;
}

TxnDDLAccess::DDLAccess* TxnDDLAccess::Get(uint16_t index)
{
    MOT_ASSERT(index < m_size);
    return m_accessList[index];
}

TxnDDLAccess::DDLAccess* TxnDDLAccess::GetByOid(uint64_t oid)
{
    if (m_size == 0) {
        return nullptr;
    }
    for (int i = m_size - 1; i >= 0; i--) {
        if (m_accessList[i]->GetOid() == oid) {
            return m_accessList[i];
        }
    }

    return nullptr;
}

TxnDDLAccess::~TxnDDLAccess()
{
    // assumption is that caller destroyed referenced objects already
    if (m_initialized) {
        Reset();
        m_initialized = false;
    }
    m_txn = nullptr;
}

void TxnDDLAccess::EraseByOid(uint64_t oid)
{
    uint16_t i = 0;
    while (i < m_size) {
        if (m_accessList[i]->GetOid() == oid) {
            EraseAt(i);

            // EraseAt will move the remaining entries forward to remove the hole created by entry removal,
            // so we still have to continue from i (without advancing).
            continue;
        }
        ++i;
    }
}

void TxnDDLAccess::EraseAt(uint16_t index)
{
    MOT_ASSERT(index < m_size);
    delete m_accessList[index];
    // start from the next element and move back entries to previous location
    for (int i = index + 1; i < m_size; i++) {
        m_accessList[i - 1] = m_accessList[i];
    }
    m_size--;
}

void TxnDDLAccess::AddTxnTable(TxnTable* table)
{
    m_txnTableMap[table->GetTableExId()] = table;
}

void TxnDDLAccess::DelTxnTable(TxnTable* table)
{
    ExternalTableMap::iterator it = m_txnTableMap.find(table->GetTableExId());
    if (it != m_txnTableMap.end()) {
        TxnTable* tab = it->second;
        (void)m_txnTableMap.erase(it);
        delete tab;
    }
}

Table* TxnDDLAccess::GetTxnTable(uint64_t tabId)
{
    ExternalTableMap::iterator it = m_txnTableMap.find(tabId);
    if (it != m_txnTableMap.end()) {
        if (!it->second->IsDropped()) {
            it->second->ApplyAddIndexFromOtherTxn();
            return it->second;
        }
    }
    return nullptr;
}

RC TxnDDLAccess::ValidateDDLChanges(TxnManager* txn)
{
    RC res = RC_OK;
    for (ExternalTableMap::iterator it = m_txnTableMap.begin(); it != m_txnTableMap.end() && res == RC_OK; (void)++it) {
        TxnTable* tab = (TxnTable*)it->second;
        res = tab->ValidateDDLChanges(txn);
    }
    return res;
}

void TxnDDLAccess::ApplyDDLChanges(TxnManager* txn)
{
    for (ExternalTableMap::iterator it = m_txnTableMap.begin(); it != m_txnTableMap.end(); (void)++it) {
        TxnTable* tab = (TxnTable*)it->second;
        tab->ApplyDDLChanges(txn);
    }
}

void TxnDDLAccess::RollbackDDLChanges(TxnManager* txn)
{
    for (ExternalTableMap::iterator it = m_txnTableMap.begin(); it != m_txnTableMap.end(); (void)++it) {
        TxnTable* tab = (TxnTable*)it->second;
        tab->RollbackDDLChanges(txn);
    }
}
}  // namespace MOT
