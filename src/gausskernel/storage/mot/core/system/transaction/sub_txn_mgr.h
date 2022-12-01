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
 * sub_txn_mgr.h
 *    Manages the current sub-transaction.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/transaction/sub_txn_mgr.h
 *
 * -------------------------------------------------------------------------
 */

#pragma once

#ifndef SUB_TXN_MGR_H
#define SUB_TXN_MGR_H

#include <cstdint>
#include <stack>
#include "debug_utils.h"

namespace MOT {
/**
 * @class SubTxnMgr
 * @brief Manages the current sub-transaction.
 * Allows rollback of RD-ONLY sub-transactions. In case of abort of a sub-transaction that contains DDL/DML,
 * the root transaction and all sub transactions are aborted.
 */
class SubTxnMgr {
public:
    struct PACKED SubTxnNode {
        /** @var Sub-transaction ID. */
        uint64_t m_subTransactionId = 0;

        /** @var Access snapshot ID. */
        uint32_t m_snapshotAccessId = 0;

        /** @var A flag indicating whether the sub-transaction is read-only. */
        SubTxnOperType m_subOper = SubTxnOperType::SUB_TXN_READ;

        bool m_isSubTxnStartedEmpty = true;

        void SetSubOper(SubTxnOperType subOper)
        {
            if (m_subOper < subOper) {
                m_subOper = subOper;
            }
        }

        SubTxnOperType GetSubOper() const
        {
            return m_subOper;
        }

        bool IsSubTxnStartedEmpty() const
        {
            return m_isSubTxnStartedEmpty;
        }
    };

    /** Constructor. */
    SubTxnMgr()
    {}

    /** Destructor. */
    ~SubTxnMgr()
    {
        m_manager = nullptr;
    }

    /** @brief Init manager */
    inline bool Init(TxnManager* manager)
    {
        if (!manager) {
            return false;
        }
        m_manager = manager;
        m_isSubTxnStarted = false;
        return true;
    }

    /** @brief Start sub-transaction */
    void Start(uint64_t subTransactionId);

    /** @brief Commit sub-transaction */
    inline void Commit(uint64_t subTransactionId)
    {
        if (m_isSubTxnStarted) {
            if (!m_subTxnStack.empty()) {
                SubTxnNode& s = m_subTxnStack.top();
                MOT_ASSERT(s.m_subTransactionId == subTransactionId);
                SubTxnOperType subOper = s.GetSubOper();
                m_subTxnStack.pop();
                if (subOper == SubTxnOperType::SUB_TXN_DDL) {
                    SetHasCommitedTxnDDL();
                }
                if (m_subTxnStack.empty()) {
                    m_isSubTxnStarted = false;
                } else if (subOper != SubTxnOperType::SUB_TXN_READ) {
                    SubTxnNode& s1 = m_subTxnStack.top();
                    s1.SetSubOper(subOper);
                }
            }
        }
    }

    /** @brief Set RD-only flag */
    void SetSubTxnOper(SubTxnOperType subOper);

    /** @brief Rollback sub-transaction */
    RC Rollback(uint64_t subTransactionId);

    /** @brief Reset local arguments */
    inline void ResetSubTransactionMgr()
    {
        if (m_isSubTxnStarted == true) {
            m_isSubTxnStarted = false;
            m_subTxnStack = std::stack<SubTxnNode>();
            MOT_ASSERT(m_subTxnStack.empty());
        }
    }

    inline bool IsSubTxnStarted() const
    {
        return m_isSubTxnStarted;
    }

    bool IsHardTxnAbort() const;

    void SetHardTxnAbort();

    void SetHasCommitedTxnDDL();

private:
    /** @var The parent transaction manager object. */
    TxnManager* m_manager = nullptr;

    /** @var A stack containing all the current sub-transaction info */
    std::stack<SubTxnNode> m_subTxnStack;

    /** @var A flag indicating whether the sub-transaction started. */
    bool m_isSubTxnStarted = false;
};
}  // namespace MOT

#endif  // SUB_TXN_MGR_H
