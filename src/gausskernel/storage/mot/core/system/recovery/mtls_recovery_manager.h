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
 * mtls_recovery_manager.h
 *    Implements a low footprint multi-threaded recovery manager
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/recovery/mtls_recovery_manager.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef MTLS_RECOVERY_MANAGER_H
#define MTLS_RECOVERY_MANAGER_H

#include "spsc_allocator.h"
#include "mtls_transaction_processor.h"
#include "mtls_transaction_committer.h"
#include <vector>
#include <thread>
#include <atomic>
#include "mot_configuration.h"
#include "base_recovery_manager.h"
#include "surrogate_state.h"
#include "spsc_queue.h"
#include "redo_log_transaction_player.h"
#include "thread_utils.h"

namespace MOT {
/**
 * @class MTLSRecoveryManager
 * @brief Implements multi threaded recovery
 */
class MTLSRecoveryManager : public BaseRecoveryManager {
public:
    MTLSRecoveryManager()
        : m_numThreads(0),
          m_confQueueSize(GetGlobalConfiguration().m_parallelRecoveryQueueSize),
          m_confNumProcessors(GetGlobalConfiguration().m_parallelRecoveryWorkers),
          m_numAllocatedPlayers(0),
          m_txnPool(nullptr),
          m_processorPlayerCounts(nullptr),
          m_processorQueueSize(m_confQueueSize / m_confNumProcessors),
          m_committer(nullptr)
    {}

    ~MTLSRecoveryManager() override;

    /**
     * @brief Performs the necessary tasks to initialize the object.
     * @return Boolean value denoting success or failure.
     */
    bool Initialize() override;

    /**
     * @brief Starts the recovery process.
     * @return Boolean value denoting success or failure.
     */
    bool RecoverDbStart() override;

    /**
     * @brief Performs post recovery operations.
     * @return Boolean value denoting success or failure.
     */
    bool RecoverDbEnd() override;

    /**
     * @brief commits an 'external' transaction.
     * @param extTxnId the transaction id.
     * @return Boolean value denoting success or failure.
     */
    bool CommitTransaction(uint64_t extTxnId) override;

    /**
     * @brief Given a log segment, enqueue it in the appropriate queues.
     * @return Boolean value denoting success or failure.
     */
    bool ProcessSegment(LogSegment* segment);

    /**
     * @brief Checks if the operation should be retried.
     * @param player the player to check.
     * @return Boolean value denoting true or false.
     */
    bool ShouldRetryOp(RedoLogTransactionPlayer* player) override
    {
        // If there was an error or the current transaction is the next one to
        // be committed, no point in retrying.
        if (IsErrorSet()) {
            return false;
        }
        if (m_committer->QueuePeek() == player) {
            if (player->IsRetried()) {
                return false;
            } else {
                // Mark the transaction as retried in order to avoid replaying it again.
                player->SetRetried();
                return true;
            }
        }
        return true;
    }

    /**
     * @brief Gets a txn player object from the pool. Tries to grow the pool upto
     * m_confQueueSize if no players are available.
     * @return a RedoLogTransactionPlayer object or nullptr if the queue is NA.
     */
    RedoLogTransactionPlayer* GetPlayerFromPool(uint32_t queueId);

    void ReleasePlayer(RedoLogTransactionPlayer* player) override
    {
        MOT_ASSERT(!player->m_inPool);
        player->m_inPool = true;
        if (!m_txnPool[player->m_queueId]->Put(player)) {
            // Cannot happen as the maximum number of players will never exceed the queue's capacity.
            MOTAbort();
        }
    }

    /**
     * @brief Returns the number of currently running threads.
     * @return uint32 as the value of num threads
     */
    uint32_t GetNumRecoveryThreads() const override
    {
        return m_numThreads.load();
    }

    /**
     * @brief Flushes all queues.
     */
    void Flush() override;

    /**
     * @brief Flushes the committer's queue.
     */
    void DrainCommitter();

    /**
     * @brief Waits up to 60 secs for all processor queues to be empty.
     */
    void DrainProcessors();

    /**
     * @brief Initializes the processor threads.
     * @return Boolean value denoting success or failure.
     */
    bool InitializeProcessorsContext();

    /**
     * @brief Releases the processor thread's resources.
     */
    void CleanupProcessorsContext();

    /**
     * @brief Destroys the processor thread contexts.
     */
    void DestroyProcessorsContext();

    /**
     * @brief Creates the processor threads and wait for them to start.
     * @return Boolean value denoting success or failure.
     */
    bool StartProcessors();

    /**
     * @brief Initializes the committer thread.
     * @return Boolean value denoting success or failure.
     */
    bool InitializeCommitterContext();

    /**
     * @brief Releases the committer thread's resources.
     */
    void CleanupCommitterContext();

    /**
     * @brief Destroys the committer thread context.
     */
    void DestroyCommitterContext();

    /**
     * @brief Creates the committer thread and wait for it to start.
     * @return Boolean value denoting success or failure.
     */
    bool StartCommitter();

    uint64_t SerializePendingRecoveryData(int fd) override;

    bool DeserializePendingRecoveryData(int fd) override;

    void Lock() override
    {
        m_lock.lock();
    }

    void Unlock() override
    {
        m_lock.unlock();
    }

private:
    /**
     * @brief Extracts log segments from wal.
     * @return Boolean value denoting success or failure.
     */
    bool ApplyLogSegmentData(char* data, size_t len, uint64_t replayLsn) override;

    /**
     * @brief Cleans up the resources.
     */
    void Cleanup();

    /**
     * @brief Destroys the recovery manager object.
     */
    void Destroy();

    /**
     * @brief Cleans up the transaction map.
     */
    void CleanupTxnMap();

    /**
     * @brief Stops all worker threads.
     */
    void StopThreads();

    /**
     * @brief Creates the txn player pool.
     * @return Boolean value denoting success or failure.
     */
    bool InitTxnPool();

    /**
     * @brief Cleans up the txn player pool.
     */
    void CleanupTxnPool();

    /**
     * @brief Destroys the txn player pool.
     */
    void DestroyTxnPool();

    /**
     * @brief Starts the processors and committer threads.
     * @return Boolean value denoting success or failure.
     */
    bool StartThreads();

    /**
     * @brief Creates a new player object and enqueues it in the txn pool.
     * @return Boolean value denoting success or failure.
     */
    RedoLogTransactionPlayer* CreatePlayer();

    /**
     * @brief Assigns a player for a log segment.
     * @param LogSegment the segment to assign a player to.
     * @return A player object or nullptr if one could not be obtained on time.
     */
    RedoLogTransactionPlayer* AssignPlayer(LogSegment* segment);

    /**
     * @brief Computes the processor queue id from EndSegmentBlock.
     * @param endSegmentBlock EndSegmentBlock of the segment to be processed.
     * @return Processor queue id.
     */
    inline uint32_t ComputeProcessorQueueId(const EndSegmentBlock& endSegmentBlock) const
    {
        // In case of upgrade from 1VCC to MVCC, we fallback to single threaded recovery (queueId 0).
        uint32_t queueId = (Is1VCCEndSegmentOpCode(endSegmentBlock.m_opCode)
                                ? 0
                                : (endSegmentBlock.m_internalTransactionId % m_confNumProcessors));
        return queueId;
    }

    /**
     * @brief Checks if a queue has enough space (less than 80% capacity), if not sleeps until
     * it frees up or timeout (10 secs).
     * @param queueId the queue's identifier to check.
     * @return True flow control check was ok, False - timeout occured.
     */
    bool FlowControl(uint32_t queueId);

    /**
     * @brief Helper func that extracts a log segment from a buffer according to a meta version.
     * @param buf the buffer to extract the data from.
     * @param len the length of the buffer.
     * @param replayLsn the replayLsn for the segment.
     * @param metaVersion the metadata version of the pending transactions data file.
     * @return Boolean value denoting success or failure.
     */
    bool ApplyPendingRecoveryLogSegmentData(char* buf, uint64_t len, uint64_t replayLsn, uint32_t metaVersion);

    std::vector<std::thread> m_threads;
    std::atomic<uint32_t> m_numThreads;
    uint32_t m_confQueueSize;
    uint32_t m_confNumProcessors;
    uint32_t m_numAllocatedPlayers;

    /*
     * SPSCQueue (per processor) to hold the TXN players.
     * Committer is the producer - after the TXN is committed, it releases the player back to the pool.
     * Envelope's thread which calls MOTRedo is the consumer - takes a free player from the pool and assign it for a
     * new transaction. When there are no free players, it allocates a new player and assign it for a new transaction
     * (without putting it to the pool) or waits until a player becomes free if the current number of allocated players
     * already reached the configured queue size.
     */
    SPSCQueue<RedoLogTransactionPlayer>** m_txnPool;
    uint32_t* m_processorPlayerCounts;
    uint32_t m_processorQueueSize;

    std::mutex m_lock;
    std::map<uint64_t, RedoLogTransactionPlayer*> m_txnMap;
    std::map<uint64_t, uint64_t> m_extToInt;
    std::vector<MTLSTransactionProcessorContext*> m_processors;
    MTLSTransactionCommitterContext* m_committer;
    ThreadNotifier m_notifier;
};

}  // namespace MOT
#endif /* MTLS_RECOVERY_MANAGER_H */
