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
 * access.cpp
 *    Holds data for single row access.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/transaction/access.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "row.h"
#include "sentinel.h"
#include "access_params.h"
#include "access.h"
#include "txn_access.h"

namespace MOT {
IMPLEMENT_CLASS_LOGGER(Access, Transaction);

void Access::Print() const
{
    MOT_LOG_INFO("---------------------------------------------------------------");
    MOT_LOG_INFO("Access Type = %s", TxnAccess::enTxnStates[m_type]);
    MOT_LOG_INFO("Table = %s", GetSentinel()->GetIndex()->GetTable()->GetTableName().c_str());
    MOT_LOG_INFO("Index = %s", GetSentinel()->GetIndex()->GetName().c_str());
    MOT_LOG_INFO("Index Order = %s", enIndexOrder[static_cast<uint8_t>(GetSentinel()->GetIndexOrder())]);
    if (m_type == INS) {
        MOT_LOG_INFO("Upgrade Insert = %s", m_params.IsUpgradeInsert() ? "true" : "false");
        MOT_LOG_INFO("Insert on delete = %s", m_params.IsInsertOnDeletedRow() ? "true" : "false");
    } else {
        if (m_type != RD) {
            MOT_LOG_INFO("Global Row CSN = %lu", GetGlobalVersion()->GetCommitSequenceNumber());
        }
    }
    MOT_LOG_INFO("Buffer ID = %d", GetBufferId());
}

void Access::WriteGlobalChanges(uint64_t csn, uint64_t transaction_id)
{
    if (m_type == RD) {
        return;
    }

    Row* newVersion = nullptr;
    MOT_ASSERT(m_origSentinel->IsLocked() == true);
    switch (m_type) {
        case WR:
            MOT_ASSERT(m_params.IsPrimarySentinel() == true);
            MOT_ASSERT(m_globalRow->GetCommitSequenceNumber() == m_origSentinel->GetData()->GetCommitSequenceNumber());
            newVersion = m_localRow;
            newVersion->SetTable(m_localRow->GetOrigTable());
            newVersion->SetCommitSequenceNumber(csn);
            MOT_ASSERT(m_globalRow == m_origSentinel->GetData());
            newVersion->SetNextVersion(GetGlobalVersion());
            COMPILER_BARRIER;
            m_origSentinel->SetNextPtr(newVersion);
            COMPILER_BARRIER;
            m_origSentinel->SetWriteBit();
            break;
        case DEL:
            if (m_params.IsPrimarySentinel() == true) {
                newVersion = m_localRow;
                MOT_ASSERT(newVersion->IsRowDeleted() == true);
                MOT_ASSERT(
                    m_globalRow->GetCommitSequenceNumber() == m_origSentinel->GetData()->GetCommitSequenceNumber());
                newVersion->SetTable(m_localRow->GetOrigTable());
                newVersion->SetCommitSequenceNumber(csn);
                MOT_ASSERT(m_globalRow == m_origSentinel->GetData());
                newVersion->SetNextVersion(GetGlobalVersion());
                COMPILER_BARRIER;
                m_origSentinel->SetNextPtr(newVersion);
                COMPILER_BARRIER;
                m_origSentinel->SetWriteBit();
                m_localRow->GetTable()->UpdateRowCount(-1);
            } else {
                m_origSentinel->SetEndCSN(csn);
            }
            break;
        case INS:
            if (m_params.IsPrimarySentinel()) {
                GetLocalInsertRow()->SetCommitSequenceNumber(csn);
                GetLocalInsertRow()->SetTable(GetLocalInsertRow()->GetOrigTable());
                if (m_params.IsUpgradeInsert() == true) {
                    m_origSentinel->SetStartCSN(csn);
                    MOT_ASSERT(m_globalRow == m_origSentinel->GetData());
                    if (m_params.IsInsertOnDeletedRow()) {
                        GetLocalInsertRow()->SetNextVersion(GetGlobalVersion());
                    } else {
                        // Connect the new insert to a tombstone
                        MOT_ASSERT(m_localRow->IsRowDeleted() == true);
                        // Set csn to the tombstome
                        m_localRow->SetCommitSequenceNumber(csn);
                        m_localRow->SetTable(m_localRow->GetOrigTable());
                        GetLocalInsertRow()->SetNextVersion(m_localRow);
                        m_localRow->SetNextVersion(GetGlobalVersion());
                        // NewVersion--->Tombstone--->OldVersion
                    }
                }
            }
            break;
        default:
            break;
    }
}
}  // namespace MOT
