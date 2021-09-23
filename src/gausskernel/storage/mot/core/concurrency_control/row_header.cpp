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
 * row_header.cpp
 *    Row header implementation in OCC
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/concurrency_control/row_header.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "row_header.h"
#include "global.h"
#include "mot_atomic_ops.h"
#include "row.h"
#include "txn_access.h"
#include "utilities.h"
#include "cycles.h"
#include "debug_utils.h"
#include "mot_engine.h"

namespace MOT {
DECLARE_LOGGER(RowHeader, ConcurrenyControl);

RC RowHeader::GetLocalCopy(
    TxnAccess* txn, AccessType type, Row* localRow, const Row* origRow, TransactionId& lastTid) const
{
    uint64_t sleepTime = 1;
    uint64_t v = 0;
    uint64_t v2 = 1;

    // concurrent update/delete after delete is not allowed - abort current transaction
    if ((m_csnWord & ABSENT_BIT) && type != AccessType::INS) {
        return RC_ABORT;
    }

    while (v2 != v) {
        // contend for exclusive access
        v = m_csnWord;
        while (v & LOCK_BIT) {
            if (sleepTime > LOCK_TIME_OUT) {
                sleepTime = LOCK_TIME_OUT;
                struct timespec ts = {0, 5000};
                (void)nanosleep(&ts, NULL);
            } else {
                CpuCyclesLevelTime::Sleep(1);
                sleepTime = sleepTime << 1;
            }

            v = m_csnWord;
        }
        // No need to copy new-row.
        if (type != AccessType::INS) {  // get current row contents (not required during insertion of new row)
            localRow->Copy(origRow);
        }
        COMPILER_BARRIER
        v2 = m_csnWord;
    }
    if ((v & ABSENT_BIT) && (v & LATEST_VER_BIT)) {
        return RC_ABORT;
    }
    lastTid = v & (~LOCK_BIT);

    if (type == AccessType::INS) {
        // ROW ALREADY COMMITED
        if ((m_csnWord & (ABSENT_BIT)) == 0) {
            return RC_ABORT;
        }
        lastTid &= (~ABSENT_BIT);
    }

    return RC_OK;
}

bool RowHeader::ValidateWrite(TransactionId tid) const
{
    return (tid == GetCSN());
}

bool RowHeader::ValidateRead(TransactionId tid) const
{
    if (IsLocked() or (tid != GetCSN())) {
        return false;
    }

    return true;
}

void RowHeader::WriteChangesToRow(const Access* access, uint64_t csn)
{
    Row* row = access->GetRowFromHeader();
    AccessType type = access->m_type;

    if (type == RD) {
        return;
    }
#ifdef MOT_DEBUG
    if (access->m_params.IsPrimarySentinel()) {
        uint64_t v = m_csnWord;
        if (!MOTEngine::GetInstance()->IsRecovering()) {
            if (!(csn > GetCSN() && (v & LOCK_BIT))) {
                MOT_LOG_ERROR(
                    "csn=%ld, v & LOCK_BIT=%ld, v & (~LOCK_BIT)=%ld\n", csn, (v & LOCK_BIT), (v & (~LOCK_BIT)));
                MOT_ASSERT(false);
            }
        }
    }
#endif
    switch (type) {
        case WR:
            MOT_ASSERT(access->m_params.IsPrimarySentinel() == true);
            row->Copy(access->m_localRow);
            m_csnWord = (csn | LOCK_BIT);
            break;
        case DEL:
            MOT_ASSERT(access->m_origSentinel->IsCommited() == true);
            if (access->m_params.IsPrimarySentinel()) {
                m_csnWord = (csn | LOCK_BIT | ABSENT_BIT | LATEST_VER_BIT);
                // and allow reuse of the original row
            }
            // Invalidate sentinel  - row is still locked!
            access->m_origSentinel->SetDirty();
            break;
        case INS:
            if (access->m_params.IsPrimarySentinel()) {
                // At this case we have the new-row and the old row
                if (access->m_params.IsUpgradeInsert()) {
                    // We set the global-row to be locked and deleted
                    m_csnWord = (csn | LOCK_BIT | LATEST_VER_BIT);
                    // The new row is locked and absent!
                    access->m_auxRow->UnsetAbsentRow();
                    access->m_auxRow->SetCommitSequenceNumber(csn);
                } else {
                    m_csnWord = (csn | LOCK_BIT);
                }
            }
            break;
        default:
            break;
    }
}

void RowHeader::Lock()
{
    uint64_t v = m_csnWord;
    while ((v & LOCK_BIT) || !__sync_bool_compare_and_swap(&m_csnWord, v, v | LOCK_BIT)) {
        PAUSE
        v = m_csnWord;
    }
}
}  // namespace MOT
