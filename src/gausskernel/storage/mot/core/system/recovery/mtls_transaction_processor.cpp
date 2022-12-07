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
 * transaction_processor.cpp
 *    Transaction Processor Worker Thread
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/recovery/mtls_transaction_processor.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "mtls_transaction_processor.h"

namespace MOT {
IMPLEMENT_CLASS_LOGGER(MTLSTransactionProcessorContext, Recovery);
DECLARE_LOGGER(MTLSTransactionProcessor, Recovery);

static bool Wakeup(void* obj)
{
    MTLSTransactionProcessorContext* ctx = (MTLSTransactionProcessorContext*)obj;
    if (ctx != nullptr) {
        return ((ctx->GetThreadNotifier()->GetState() == ThreadNotifier::ThreadState::TERMINATE) ||
                (ctx->GetThreadNotifier()->GetState() == ThreadNotifier::ThreadState::ACTIVE &&
                    ctx->QueuePeek() != nullptr));
    }
    return true;
}

void MTLSTransactionProcessor::Start()
{
    if (m_context == nullptr) {
        MOT_LOG_ERROR("MTLSTransactionProcessor::Start: not initialized");
        return;
    }

    SessionContext* sessionContext =
        GetSessionManager()->CreateSessionContext(false, 0, nullptr, INVALID_CONNECTION_ID, true);
    if (sessionContext == nullptr) {
        MOT_LOG_ERROR("MTLSTransactionProcessor::Start: Failed to initialize Session Context");
        m_context->SetError();
        MOT::MOTEngine::GetInstance()->OnCurrentThreadEnding();
        return;
    }

    if (GetGlobalConfiguration().m_enableNuma && !GetTaskAffinity().SetAffinity(MOTCurrThreadId)) {
        MOT_LOG_WARN("Failed to set affinity for the processor worker");
    }

    m_context->SetReady();

    while (m_context->GetThreadNotifier()->GetState() != ThreadNotifier::ThreadState::TERMINATE) {
        if (m_context->GetThreadNotifier()->Wait(Wakeup, (void*)m_context) == ThreadNotifier::ThreadState::TERMINATE) {
            MOT_LOG_INFO("MTLSProcessor%u - Terminating", m_context->GetId());
            break;
        }

        LogSegment* segment = m_context->QueuePeek();
        if (segment != nullptr) {
            RedoLogTransactionPlayer* player = segment->GetPlayer();
            if (player == nullptr) {
                MOT_LOG_ERROR("MTLSTransactionProcessor::Start: no player / segment");
                m_context->SetError();
                break;
            }

            SessionContext::SetTxnContext(player->GetTxn());
            player->SetSurrogateState(m_context->GetSurrogateStatePtr());
            if (player->IsFirstSegment()) {
                if (player->BeginTransaction() != RC_OK) {
                    MOT_LOG_ERROR("MTLSTransactionProcessor::Start: failed to start the transaction");
                    m_context->SetError();
                    break;
                }
                player->SetFirstSegment(false);
            }

            if (player->RedoSegment(segment) != RC_OK) {
                MOT_LOG_ERROR("MTLSTransactionProcessor::Start: failed to redo segment");
                m_context->SetError();
                break;
            }

            if (IsCommitOp(segment->m_controlBlock.m_opCode)) {
                player->MarkProcessed();
                m_context->GetThreadNotifier()->Notify(ThreadNotifier::ThreadState::ACTIVE);
            }

            uint64_t slen = segment->m_len;
            m_context->IncFreed(slen);
            m_context->QueuePop();
            delete segment;
        }
    }

    GetSessionManager()->DestroySessionContext(sessionContext);
    MOT::MOTEngine::GetInstance()->OnCurrentThreadEnding();
}

void MTLSTransactionProcessorContext::PrintInfo() const
{
    MOT_LOG_INFO("Processor%u : NumAlloc %lu, NumFreed %lu, Delta %lu, Usage: %u",
        m_processorId,
        m_sizeAlloc,
        m_sizeFreed,
        (m_sizeAlloc - m_sizeFreed) / 1024,
        GetMemUsagePercentage());
}

uint32_t MTLSTransactionProcessorContext::GetMemUsagePercentage() const
{
    double allocDiff = m_sizeAlloc - m_sizeFreed;
    double percent = (allocDiff / ALLOCATOR_SIZE) * 100;
    return (uint32_t)percent;
}
}  // namespace MOT
