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
 * sub_txn_mgr.cpp
 *    Implements SubTxnMgr which is used to manage sub-transactions
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/transaction/sub_txn_mgr.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "txn.h"
#include "txn_access.h"

namespace MOT {
void SubTxnMgr::Start(uint64_t subTransactionId)
{
    TxnAccess* access_manager = m_manager->m_accessMgr;
    if (m_subTxnStack.empty()) {
        m_isSubTxnStarted = true;
    }
    SubTxnNode s;
    s.m_snapshotAccessId = access_manager->Size();
    s.m_subTransactionId = subTransactionId;
    s.m_subOper = SubTxnOperType::SUB_TXN_READ;
    s.m_isSubTxnStartedEmpty = m_manager->IsReadOnlyTxn();
    m_subTxnStack.push(s);
}

void SubTxnMgr::SetSubTxnOper(SubTxnOperType subOper)
{
    if (m_isSubTxnStarted) {
        SubTxnNode& s = m_subTxnStack.top();
        s.SetSubOper(subOper);
    }
}

void SubTxnMgr::SetHardTxnAbort()
{
    m_manager->SetTxnAborted();
}

void SubTxnMgr::SetHasCommitedTxnDDL()
{
    m_manager->SetHasCommitedSubTxnDDL();
}

bool SubTxnMgr::IsHardTxnAbort() const
{
    return m_manager->IsTxnAborted();
}

RC SubTxnMgr::Rollback(uint64_t subTransactionId)
{
    RC rc = RC_OK;
    if (m_isSubTxnStarted) {
        SubTxnNode& s = m_subTxnStack.top();
        if (s.m_subTransactionId < subTransactionId) {
            return rc;
        }
        MOT_ASSERT(s.m_subTransactionId == subTransactionId);
        if (IsHardTxnAbort() == false) {
            if (s.GetSubOper() == SubTxnOperType::SUB_TXN_READ) {
                // Cleanup of SELECT/SELECT-FOR-UPDATE
                m_manager->m_accessMgr->RollbackSubTxn(s.m_snapshotAccessId);
            } else {
                // Scenario not recoverable
                if (s.IsSubTxnStartedEmpty() == false) {
                    SetHardTxnAbort();
                }
                rc = RC_ABORT;
            }
        } else {
            // Txn is already rollbacked.
            rc = RC_OK;
        }
        m_subTxnStack.pop();
        if (m_subTxnStack.empty()) {
            m_isSubTxnStarted = false;
        }
    } else {
        if (m_manager->IsReadOnlyTxn() == false) {
            rc = RC_ABORT;
        }
    }
    return rc;
}
}  // namespace MOT
