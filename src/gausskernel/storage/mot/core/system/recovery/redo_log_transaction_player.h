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
 * redo_log_transaction_player.h
 *    Implements a transaction replay mechanism.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/recovery/redo_log_transaction_player.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef REDOLOG_TRANSACTION_PLAYER_H
#define REDOLOG_TRANSACTION_PLAYER_H

#include "global.h"
#include "txn.h"
#include "irecovery_manager.h"
#include "recovery_ops.h"
#include "surrogate_state.h"

namespace MOT {
/**
 * @brief Class for managing transaction replay mechanism.
 */
class RedoLogTransactionPlayer : public IRecoveryOpsContext {
public:
    explicit RedoLogTransactionPlayer(IRecoveryManager* recoveryManager);
    RedoLogTransactionPlayer(const RedoLogTransactionPlayer& orig) = delete;
    RedoLogTransactionPlayer& operator=(const RedoLogTransactionPlayer& orig) = delete;
    ~RedoLogTransactionPlayer() override;

    void Init(TxnManager* txn);
    void InitRedoTransactionData(uint64_t transactionId, uint64_t externalId, uint64_t csn, uint64_t replayLsn);
    RC BeginTransaction();
    RC CommitTransaction();
    void CleanupTransaction();

    TxnManager* GetTxn() override
    {
        return m_txn;
    }

    bool ShouldRetryOp() override
    {
        return m_recoveryManager->ShouldRetryOp(this);
    }

    bool IsProcessed() const
    {
        return m_processed.load();
    }

    void MarkProcessed()
    {
        m_processed.store(true);
    }

    void SetSurrogateState(SurrogateState* state)
    {
        m_surrogateState = state;
    }

    inline bool IsRetried() const
    {
        return m_retried;
    }

    inline void SetRetried()
    {
        m_retried = true;
    }

    inline uint64_t GetTransactionId() const
    {
        return m_transactionId;
    }

    inline uint64_t GetExternalId() const
    {
        return m_externalId;
    }

    inline void SetReplayLSN(uint64_t lsn)
    {
        m_replayLsn = lsn;
        m_txn->SetReplayLsn(lsn);
    }

    inline uint64_t GetReplayLSN() const
    {
        return m_replayLsn;
    }

    inline bool IsFirstSegment() const
    {
        return m_firstSegment;
    }

    inline void SetFirstSegment(bool value)
    {
        m_firstSegment = value;
    }

    inline uint64_t GetCSN() const
    {
        return m_csn;
    }

    inline void SetPrevId(uint64_t id)
    {
        m_prevId = id;
    }

    inline uint64_t GetPrevId() const
    {
        return m_prevId;
    }

    inline void IncNumSegs()
    {
        m_numSegs++;
    }

    inline uint64_t GetNumSegs() const
    {
        return m_numSegs;
    }

    /**
     * @brief performs a redo on a segment, which is either a recovery op
     * or a segment that belongs to a 2pc recovered transaction.
     * @param segment the segment to redo.
     * @return RC value denoting the operation's status
     */
    RC RedoSegment(LogSegment* segment);

    volatile bool m_inPool;

    uint32_t m_queueId;

private:
    bool IsRecoveryMemoryLimitReached(uint32_t numThreads) const;

    uint64_t m_transactionId;
    uint64_t m_externalId;
    uint64_t m_csn;
    uint64_t m_replayLsn;
    uint64_t m_prevId;
    volatile uint64_t m_numSegs;
    std::atomic<bool> m_processed;
    volatile bool m_firstSegment;
    bool m_initialized;
    volatile bool m_retried = false;
    IRecoveryManager* m_recoveryManager;
    SurrogateState* m_surrogateState;
    TxnManager* m_txn;
};
}  // namespace MOT

#endif /* REDOLOG_TRANSACTION_PLAYER_H */
