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

namespace MOT {
TxnDDLAccess::DDLAccess::DDLAccess()
{
    m_oid = 0;
    m_type = DDLAccessType::DDL_ACCESS_UNKNOWN;
    m_entry = nullptr;
}

uint64_t TxnDDLAccess::DDLAccess::GetOid()
{
    return m_oid;
}

void* TxnDDLAccess::DDLAccess::GetEntry()
{
    return m_entry;
}

DDLAccessType TxnDDLAccess::DDLAccess::GetDDLAccessType()
{
    return m_type;
}

void TxnDDLAccess::DDLAccess::Set(uint64_t oid, DDLAccessType accessType, void* ddlEntry)
{
    m_oid = oid;
    m_type = accessType;
    m_entry = ddlEntry;
}

TxnDDLAccess::TxnDDLAccess(TxnManager* txn) : m_txn(txn), m_initialized(false), m_size(0), m_accessList()
{}

RC TxnDDLAccess::Init()
{
    if (m_initialized)
        return RC_OK;

    m_initialized = true;
    return RC_OK;
}

uint32_t TxnDDLAccess::Size()
{
    return m_size;
}

void TxnDDLAccess::Reset()
{
    for (int i = 0; i < m_size; i++) {
        delete m_accessList[i];
        m_accessList[i] = nullptr;
    }

    m_size = 0;
}

RC TxnDDLAccess::Add(TxnDDLAccess::DDLAccess* ddlAccess)
{
    if (m_size == MAX_DDL_ACCESS_SIZE)
        return RC_TXN_EXCEEDS_MAX_DDLS;
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
    }
}

void TxnDDLAccess::EraseByOid(uint64_t oid)
{
    bool deleted = false;
    for (int i = 0; i < m_size; i++) {
        if (m_accessList[i]->GetOid() == oid) {
            delete m_accessList[i];
            deleted = true;
        } else if (deleted) {
            // remove empty spaces created by entry removal;
            m_accessList[i - 1] = m_accessList[i];
        }
    }
    if (deleted)
        m_size--;
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
}  // namespace MOT
