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
 * primary_sentinel.cpp
 *    Primary Index Sentinel
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/storage/primary_sentinel.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "sentinel.h"
#include "row.h"

namespace MOT {

IMPLEMENT_CLASS_LOGGER(PrimarySentinel, Storage);

void PrimarySentinel::SetStable(Row* const& row)
{
    m_stable = (m_stable & ~S_OBJ_ADDRESS_MASK) | ((uint64_t)(row)&S_OBJ_ADDRESS_MASK);
    if (row != nullptr) {
        row->SetStableRow();
    }
}

Row* PrimarySentinel::GetStable() const
{
    return reinterpret_cast<Row*>((uint64_t)(m_stable & S_OBJ_ADDRESS_MASK));
};

bool PrimarySentinel::GetStableStatus() const
{
    return (m_stable & STABLE_BIT) == STABLE_BIT;
}

void PrimarySentinel::SetStableStatus(bool val)
{
    if (val == true) {
        m_stable |= STABLE_BIT;
    } else {
        m_stable &= ~STABLE_BIT;
    }
}

Row* PrimarySentinel::GetVisibleRowVersion(uint64_t csn)
{
    // 1.Need to retrieve a valid snapshot
    // 2.First we take a snapshot of the current top version
    // 3.To return a valid snapshot we need to guaranty that at least one commit cycle as passed.
    // 4. Either by getting a more updated row or a lock cycle is finished(If we started in a lock state)
    Row* topRow = GetData();
    bool isUncommittedLocked = false;
    bool isLongLock = false;
    uint32_t waitCount = 0;

    while (!IsCommited() and IsLocked()) {
        CpuCyclesLevelTime::Sleep(5);
        isUncommittedLocked = true;
    }

    // If the sentinel is locked and uncommitted and we exited the loop
    // We finished a commit cycle - sentinel is committed
    if (isUncommittedLocked) {
        return GetRowFromChain(csn, GetData());
    }

    while (IsLocked()) {
        Row* r = GetData();
        if (r != nullptr) {
            if (r != topRow or (csn < r->GetCommitSequenceNumber())) {
                // Finished cycle
                return GetRowFromChain(csn, GetData());
            }
        }
        if (!isLongLock) {
            CpuCyclesLevelTime::Sleep(5);
            waitCount++;
            if (waitCount == SPIN_WAIT_COUNT) {
                isLongLock = true;
            }
        } else {
            std::this_thread::sleep_for(chrono::microseconds(100));
        }
    }

    // At this point our snapshot origin is guarantied to be after commit
    return GetRowFromChain(csn, GetData());
}

Row* PrimarySentinel::GetRowFromChain(uint64_t csn, Row* row)
{
    while (row) {
        if (csn > row->GetCommitSequenceNumber()) {
            return row;
        }
        row = row->GetNextVersion();
    }
    return nullptr;
}

Row* PrimarySentinel::GetVisibleRowForUpdate(uint64_t csn, ISOLATION_LEVEL isolationLevel)
{
    bool isUncommittedLocked = false;
    Row* topRow = GetData();

    // First check if the key is valid!
    if (IsCommited()) {
        if (csn <= m_startCSN) {
            // Phantom insert
            return nullptr;
        }
    }

    // If Sentinel is not committed and locked - spin
    while (IsLockedAndDirty() == true) {
        CpuCyclesLevelTime::Sleep(5);
        isUncommittedLocked = true;
    }

    /* If the sentinel is locked and uncommitted and we exited the loop
     * We finished a commit cycle - sentinel is committed UP/DEL over new INSERT
     */
    if (isUncommittedLocked) {
        // Either Sentinel is committed or txn is aborted - sentinel is dirty
        topRow = GetData();
        return topRow;
    }

    // We can reach this point if Sentinel is committed or dirty
    if (isolationLevel == READ_COMMITED) {
        while (IsLocked()) {
            Row* r = GetData();
            if (r != nullptr) {
                if (r != topRow or IsWriteBitOn() == true) {
                    return r;
                }
            }
        }
    } else {
        MOT_ASSERT(isolationLevel == REPEATABLE_READ);
        while (IsLocked()) {
            Row* r = GetData();
            if (r != nullptr) {
                if (csn <= r->GetCommitSequenceNumber()) {
                    return r;
                }
                if (r != topRow) {
                    return GetData();
                }
            }
        }
    }
    if (IsDirty() == true) {
        return nullptr;
    }

    return GetData();
}

PrimarySentinel* PrimarySentinel::GetVisiblePrimaryHeader(uint64_t csn)
{
    // If Sentinel is locked and uncommitted wait for commit to end
    while (IsLockedAndDirty()) {
        CpuCyclesLevelTime::Sleep(5);
    }
    /* At this point Sentinel is  either committed/aborted in
       case it is committed we can return a valid Sentinel. For
       a committed Sentinel we need further examination for
       deleted sentinel. */
    if (IsCommited()) {
        if (csn > m_startCSN) {
            return this;
        } else {
            // Phantom insert
            return nullptr;
        }
    }
    return nullptr;
}

void PrimarySentinel::Print()
{
    MOT_LOG_INFO("PrimarySentinel: Index name: %s startCSN = %lu indexOrder = PRIMARY_INDEX TID=%lu",
        GetIndex()->GetName().c_str(),
        GetStartCSN(),
        GetTransactionId());
    MOT_LOG_INFO("|");
    MOT_LOG_INFO("V");

    if (IsCommited()) {
        Row* row = GetData();
        row->Print();
    } else {
        MOT_LOG_INFO("NULL");
    }
    MOT_LOG_INFO("---------------------------------------------------------------");
}

void PrimarySentinel::ReclaimSentinel(Sentinel* s)
{
    MOT_ASSERT(s != nullptr);
    Row* row = s->GetData();
    Index* index = s->GetIndex();
    Table* t = index->GetTable();
    index->SentinelRelease(s);
    while (row) {
        Row* tmp = row;
        row = row->GetNextVersion();
        t->DestroyRow(tmp);
    }
}

}  // namespace MOT
