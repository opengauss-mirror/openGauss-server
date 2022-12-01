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
 * transaction_processor.h
 *    Transaction Processor Worker Thread
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/recovery/mtls_transaction_processor.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef MTLS_TRANSACTION_PROCESSOR_H
#define MTLS_TRANSACTION_PROCESSOR_H

#include "mot_engine.h"
#include "spsc_queue.h"
#include "redo_log_transaction_player.h"
#include "thread_utils.h"

namespace MOT {
/**
 * @class MTLSTransactionProcessorContext
 * @brief Defines a context for the MTLSTransactionProcessor Thread.
 */
class MTLSTransactionProcessorContext : public ThreadContext {
public:
    MTLSTransactionProcessorContext(
        IRecoveryManager* recoveryManager, ThreadNotifier* notifier, uint32_t id, uint32_t queueSize)
        : ThreadContext(),
          m_recoveryManager(recoveryManager),
          m_notifier(notifier),
          m_processorId(id),
          m_queueSize(queueSize),
          m_queue(queueSize),
          m_allocator(nullptr),
          m_sizeAlloc(0),
          m_sizeFreed(0)
    {}

    ~MTLSTransactionProcessorContext()
    {
        Cleanup();
        m_recoveryManager = nullptr;
        m_notifier = nullptr;
    }

    bool Initialize()
    {
        if (!m_queue.Init()) {
            MOT_LOG_ERROR("MTLSTransactionProcessorContext: Failed to initialize the log segment queue");
            return false;
        }
        if (!m_surrogateState.Init()) {
            MOT_LOG_ERROR("MTLSTransactionProcessorContext: Failed to initialize the surrogate state");
            return false;
        }
        m_allocator = SPSCVarSizeAllocator::GetSPSCAllocator(ALLOCATOR_SIZE);
        if (m_allocator == nullptr) {
            MOT_LOG_ERROR("MTLSTransactionProcessorContext:: failed to create allocator");
            return false;
        }
        return true;
    }

    void Cleanup()
    {
        while (m_queue.Top() != nullptr) {
            LogSegment* segment = m_queue.Take();
            delete segment;
        }
        if (m_allocator != nullptr) {
            SPSCVarSizeAllocator::FreeSPSCAllocator(m_allocator);
            m_allocator = nullptr;
        }
    }

    inline SPSCVarSizeAllocator* GetAllocator()
    {
        return m_allocator;
    }

    inline bool QueuePut(LogSegment* op)
    {
        return m_queue.Put(op);
    }

    inline LogSegment* QueuePeek()
    {
        return m_queue.Top();
    }

    inline void QueuePop()
    {
        m_queue.Pop();
    }

    inline bool QueueEmpty()
    {
        return m_queue.IsEmpty();
    }

    inline SurrogateState* GetSurrogateStatePtr()
    {
        return &m_surrogateState;
    }

    inline uint32_t GetId() const
    {
        return m_processorId;
    }

    inline void SetId(uint32_t id)
    {
        m_processorId = id;
    }

    void SetError() override
    {
        ThreadContext::SetError();
        m_recoveryManager->SetError();
    }

    inline void IncAlloc(uint64_t size)
    {
        m_sizeAlloc += size;
    }

    inline void IncFreed(uint64_t size)
    {
        m_sizeFreed += size;
    }

    void PrintInfo() const;

    uint32_t GetMemUsagePercentage() const;

    inline ThreadNotifier* GetThreadNotifier()
    {
        return m_notifier;
    }

    static constexpr uint32_t ALLOCATOR_SIZE = (1024 * 1024 * 100);

private:
    IRecoveryManager* m_recoveryManager;
    ThreadNotifier* m_notifier;
    uint32_t m_processorId;
    uint32_t m_queueSize;
    SPSCQueue<LogSegment> m_queue;
    SPSCVarSizeAllocator* m_allocator;
    SurrogateState m_surrogateState;
    volatile uint64_t m_sizeAlloc;
    volatile uint64_t m_sizeFreed;

    DECLARE_CLASS_LOGGER();
};

/**
 * @class MTLSTransactionProcessor
 * @brief Transaction Processor Worker Thread
 */
class MTLSTransactionProcessor {
public:
    explicit MTLSTransactionProcessor(MTLSTransactionProcessorContext* context) : m_context(context)
    {}

    ~MTLSTransactionProcessor()
    {
        m_context = nullptr;
    }

    void Start();

private:
    MTLSTransactionProcessorContext* m_context;
};
}  // namespace MOT

#endif  // MTLS_TRANSACTION_PROCESSOR_H
