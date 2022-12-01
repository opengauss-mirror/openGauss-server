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
 * transaction_committer.h
 *    Transaction Committer Worker Thread
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/recovery/mtls_transaction_committer.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef MTLS_TRANSACTION_COMMITTER_H
#define MTLS_TRANSACTION_COMMITTER_H

#include "mot_engine.h"
#include "spsc_queue.h"
#include "redo_log_transaction_player.h"
#include "thread_utils.h"

namespace MOT {
/**
 * @class MTLSTransactionCommitterContext
 * @brief Defines a context for the MTLSTransactionCommitter Thread.
 */
class MTLSTransactionCommitterContext : public ThreadContext {
public:
    MTLSTransactionCommitterContext(IRecoveryManager* recoveryManager, ThreadNotifier* notifier, uint32_t queueSize)
        : ThreadContext(),
          m_recoveryManager(recoveryManager),
          m_notifier(notifier),
          m_queueSize(queueSize),
          m_queue(queueSize)
    {}

    ~MTLSTransactionCommitterContext()
    {
        Cleanup();

        // RecoveryManager will be destroyed by the provider
        m_recoveryManager = nullptr;

        // Notifier will be destroyed by the provider
        m_notifier = nullptr;
    }

    bool Initialize()
    {
        if (!m_queue.Init()) {
            MOT_LOG_ERROR("MTLSTransactionCommitterContext::Initialize - Failed to initialize the player queue");
            return false;
        }
        return true;
    }

    void Cleanup()
    {
        while (m_queue.Top() != nullptr) {
            /*
             * Committer queue still has a player. That means the transaction is not committed.
             * Rollback the transaction first and then release it back to the txnPool.
             */
            RedoLogTransactionPlayer* player = m_queue.Take();
            player->GetTxn()->Rollback();
            CleanupTxnAndReleasePlayer(player);
        }
    }

    inline RedoLogTransactionPlayer* QueuePeek()
    {
        return m_queue.Top();
    }

    inline bool QueuePut(RedoLogTransactionPlayer* player)
    {
        return m_queue.Put(player);
    }

    inline void QueuePop()
    {
        m_queue.Pop();
    }

    inline bool QueueEmpty()
    {
        return m_queue.IsEmpty();
    }

    inline void CleanupTxnAndReleasePlayer(RedoLogTransactionPlayer* player)
    {
        /*
         * Setting prevId as the transactionId to indicate that the TXN is committed and
         * so player can be removed from txnMap and reused for new a transaction.
         * Caution: We don't want the committer to access the txnMap directly to avoid contention
         * and avoid deadlock by trying to acquire recovery manager lock for accessing the txnMap.
         */
        player->SetPrevId(player->GetTransactionId());
        player->CleanupTransaction();
        MOT_LOG_TRACE(
            "MTLSTransactionCommitter::CleanupTxnAndReleasePlayer - Releasing player [%p:%lu:%lu] back to the txnPool",
            player,
            player->GetTransactionId(),
            player->GetPrevId());
        m_recoveryManager->ReleasePlayer(player);
    }

    void SetError() override
    {
        ThreadContext::SetError();
        m_recoveryManager->SetError();
    }

    inline ThreadNotifier* GetThreadNotifier()
    {
        return m_notifier;
    }

private:
    IRecoveryManager* m_recoveryManager;
    ThreadNotifier* m_notifier;
    uint32_t m_queueSize;
    SPSCQueue<RedoLogTransactionPlayer> m_queue;

    DECLARE_CLASS_LOGGER();
};

/**
 * @class MTLSTransactionCommitter
 * @brief Transaction Committer Worker Thread
 */
class MTLSTransactionCommitter {
public:
    explicit MTLSTransactionCommitter(MTLSTransactionCommitterContext* context) : m_context(context)
    {}

    ~MTLSTransactionCommitter()
    {
        m_context = nullptr;
    }

    void Start();

private:
    RC CommitTransaction(RedoLogTransactionPlayer* player);

    MTLSTransactionCommitterContext* m_context;
};
}  // namespace MOT

#endif  // MTLS_TRANSACTION_COMMITTER_H
