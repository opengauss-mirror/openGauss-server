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
 * transaction_committer.cpp
 *    Transaction Committer Worker Thread
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/recovery/mtls_transaction_committer.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "mtls_transaction_committer.h"

namespace MOT {
IMPLEMENT_CLASS_LOGGER(MTLSTransactionCommitterContext, Recovery);
DECLARE_LOGGER(MTLSTransactionCommitter, Recovery);

static bool Wakeup(void* obj)
{
    MTLSTransactionCommitterContext* ctx = (MTLSTransactionCommitterContext*)obj;
    if (ctx != nullptr) {
        return ((ctx->GetThreadNotifier()->GetState() == ThreadNotifier::ThreadState::TERMINATE) ||
                (ctx->GetThreadNotifier()->GetState() == ThreadNotifier::ThreadState::ACTIVE &&
                    ctx->QueuePeek() != nullptr));
    }
    return true;
}

void MTLSTransactionCommitter::Start()
{
    if (m_context == nullptr) {
        MOT_LOG_ERROR("MTLSTransactionCommitter::Start - Not initialized");
        return;
    }

    SessionContext* sessionContext =
        GetSessionManager()->CreateSessionContext(false, 0, nullptr, INVALID_CONNECTION_ID, true);
    if (sessionContext == nullptr) {
        MOT_LOG_ERROR("MTLSTransactionCommitter::Start - Failed to initialize Session Context");
        m_context->SetError();
        MOT::MOTEngine::GetInstance()->OnCurrentThreadEnding();
        return;
    }

    if (GetGlobalConfiguration().m_enableNuma && !GetTaskAffinity().SetAffinity(MOTCurrThreadId)) {
        MOT_LOG_WARN("MTLSTransactionCommitter::Start - Failed to set affinity for the committer worker");
    }

    m_context->SetReady();

    while (m_context->GetThreadNotifier()->GetState() != ThreadNotifier::ThreadState::TERMINATE) {
        if (m_context->GetThreadNotifier()->Wait(Wakeup, (void*)m_context) == ThreadNotifier::ThreadState::TERMINATE) {
            MOT_LOG_INFO("MTLSCommitter - Terminating");
            break;
        }

        RedoLogTransactionPlayer* player = m_context->QueuePeek();
        if (player != nullptr && player->IsProcessed()) {
            SessionContext::SetTxnContext(player->GetTxn());
            if (CommitTransaction(player) != RC_OK) {
                MOT_LOG_ERROR("MTLSTransactionCommitter::Start - Commit failed");
                m_context->SetError();
                break;
            }

            /*
             * To avoid contention, the committer thread will not erase the txn entry from the txnMap. Instead,
             * it will just set the m_prevId in the player (to indicate that the TXN is committed) and
             * release it back to the txnPool.
             * We erase the txn entry later in the following cases:
             *      1. Next time when the player is used in AssignPlayer.
             *      2. In SerializePendingRecoveryData during checkpoint.
             * Downside of this approach is that the txn entry stays longer in the map. On the other hand, we avoid
             *      - Contention between the committer thread and envelope's thread which calls the MOTRedo.
             *      - Avoid deadlock by trying to acquire recovery manager lock for accessing the txnMap.
             */
            m_context->QueuePop();
            m_context->CleanupTxnAndReleasePlayer(player);
        }
    }

    GetSessionManager()->DestroySessionContext(sessionContext);
    MOT::MOTEngine::GetInstance()->OnCurrentThreadEnding();
}

RC MTLSTransactionCommitter::CommitTransaction(RedoLogTransactionPlayer* player)
{
    MOT_LOG_TRACE("MTLSTransactionCommitter::CommitTransaction Committing TXN [%lu:%lu], NumSegments: %lu",
        player->GetTransactionId(),
        player->GetExternalId(),
        player->GetNumSegs());
    RC status = player->CommitTransaction();
    if (status != RC_OK) {
        MOT_LOG_ERROR("MTLSTransactionCommitter::CommitTransaction: commit failed");
        return status;
    }
    return RC_OK;
}
}  // namespace MOT
