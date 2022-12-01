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
 * mtls_recovery_manager.cpp
 *    Implements a low footprint multi-threaded recovery manager
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/recovery/mtls_recovery_manager.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "mot_engine.h"
#include "mtls_recovery_manager.h"
#include "checkpoint_utils.h"
#include "checkpoint_manager.h"
#include "pending_txn_logger.h"
#include "utils/memutils.h"

namespace MOT {
DECLARE_LOGGER(MTLSRecoveryManager, Recovery);

static const char* const MTLS_PROCESSOR_NAME = "MTLSProcessor";
static const char* const MTLS_COMMITTER_NAME = "MTLSCommitter";
static constexpr uint32_t WAIT_FOR_PLAYER_TIMEOUT = 1000 * 1000 * 10;           // 10 sec
static constexpr uint32_t WAIT_FOR_PLAYER_WARNING_LOG_THRESHOLD = 1000 * 1000;  // 1 sec
static constexpr uint32_t QUEUE_MAX_CAPACITY_THRESHOLD = 80;
static constexpr uint32_t QUEUE_MAX_CAPACITY_TIMEOUT = 1000 * 1000 * 10;  // 10 sec

static void TransactionProcessorThread(MTLSTransactionProcessorContext* context)
{
    MOT_ASSERT(context != nullptr);
    MemoryContextInit();
    MOT_DECLARE_NON_KERNEL_THREAD();
    char name[ThreadContext::THREAD_NAME_LEN];
    errno_t rc = snprintf_s(name,
        ThreadContext::THREAD_NAME_LEN,
        ThreadContext::THREAD_NAME_LEN - 1,
        "%s%u",
        MTLS_PROCESSOR_NAME,
        context->GetId());
    securec_check_ss(rc, "", "");
    (void)pthread_setname_np(pthread_self(), name);
    MTLSTransactionProcessor processor(context);
    MOT_LOG_INFO("%s - Starting", name);
    processor.Start();
    MemoryContextDestroyAtThreadExit(TopMemoryContext);
    MOT_LOG_DEBUG("%s - Exited", name);
}

static void TransactionCommitterThread(MTLSTransactionCommitterContext* context)
{
    MOT_ASSERT(context != nullptr);
    MemoryContextInit();
    MOT_DECLARE_NON_KERNEL_THREAD();
    (void)pthread_setname_np(pthread_self(), MTLS_COMMITTER_NAME);
    MTLSTransactionCommitter committer(context);
    MOT_LOG_INFO("%s - Starting", MTLS_COMMITTER_NAME);
    committer.Start();
    MemoryContextDestroyAtThreadExit(TopMemoryContext);
    MOT_LOG_DEBUG("%s - Exited", MTLS_COMMITTER_NAME);
}

MTLSRecoveryManager::~MTLSRecoveryManager()
{
    StopThreads();
    Destroy();
}

bool MTLSRecoveryManager::Initialize()
{
    do {
        if (!BaseRecoveryManager::Initialize()) {
            break;
        }
        if (!InitTxnPool()) {
            break;
        }
        if (!InitializeProcessorsContext()) {
            break;
        }
        if (!InitializeCommitterContext()) {
            break;
        }
        MOT_LOG_INFO("MTLSRecoveryManager: Initialized successfully")
        return true;
    } while (0);

    MOT_LOG_ERROR("MTLSRecoveryManager: Failed to initialize");
    return false;
}

void MTLSRecoveryManager::Cleanup()
{
    MOT_ASSERT(m_initialized);
    CleanupProcessorsContext();
    CleanupCommitterContext();
    CleanupTxnMap();
    m_extToInt.clear();
    CleanupTxnPool();
}

void MTLSRecoveryManager::Destroy()
{
    if (!m_initialized) {
        return;
    }
    DestroyProcessorsContext();
    DestroyCommitterContext();
    CleanupTxnMap();
    DestroyTxnPool();
    m_initialized = false;
}

bool MTLSRecoveryManager::RecoverDbStart()
{
    MOT_LOG_INFO("Starting MOT recovery");
    return BaseRecoveryManager::RecoverDbStart();
}

bool MTLSRecoveryManager::RecoverDbEnd()
{
    Flush();
    StopThreads();
    if (m_maxCsn) {
        GetCSNManager().SetCSN(m_maxCsn);
    }

    /* Take MTLSRecoveryManager lock to protect it against checkpoint. */
    const std::lock_guard<std::mutex> lock(m_lock);
    if (!m_txnMap.empty()) {
        MOT_LOG_WARN("MTLSRecoveryManager::RecoverDbEnd - txnMap contains pending transactions!");
    }
    if (!m_extToInt.empty()) {
        MOT_LOG_WARN("MTLSRecoveryManager::RecoverDbEnd - extToInt map is not empty!");
    }
    for (uint32_t i = 0; i < m_confNumProcessors; i++) {
        AddSurrogateArrayToList(*(m_processors[i]->GetSurrogateStatePtr()));
    }
    Cleanup();
    return BaseRecoveryManager::RecoverDbEnd();
}

bool MTLSRecoveryManager::StartThreads()
{
    do {
        if (!StartProcessors()) {
            break;
        }
        if (!StartCommitter()) {
            break;
        }
        return true;
    } while (0);
    return false;
}

void MTLSRecoveryManager::StopThreads()
{
    m_notifier.Notify(ThreadNotifier::ThreadState::TERMINATE);
    for (auto& worker : m_threads) {
        if (worker.joinable()) {
            worker.join();
            (void)--m_numThreads;
        }
    }
    if (m_numThreads.load()) {
        MOT_LOG_WARN("MTLSRecoveryManager: Failed to stop all threads");
        m_numThreads.store(0);
    }
    m_threads.clear();
}

RedoLogTransactionPlayer* MTLSRecoveryManager::AssignPlayer(LogSegment* segment)
{
    // In case of upgrade from 1VCC to MVCC, we use INITIAL_CSN, because the CSN in the 1VCC log segment
    // is not compatible with envelope CSN.
    uint64_t csn = (Is1VCCLogSegment(segment) ? INITIAL_CSN : segment->m_controlBlock.m_csn);
    uint64_t inId = segment->m_controlBlock.m_internalTransactionId;
    uint64_t exId = segment->m_controlBlock.m_externalTransactionId;
    RedoLogTransactionPlayer* player = nullptr;
    uint32_t waitedUs = 0;

    if (!IsMotOnlyTransaction(segment)) {
        MOT_ASSERT(exId != INVALID_TRANSACTION_ID);
        MOT_LOG_TRACE("MTLSRecoveryManager::AssignPlayer: Inserting TXN [%lu:%lu] into extToInt map", inId, exId);
        m_extToInt[exId] = inId;
    }
    auto it = (m_txnMap.find(inId));
    if (it != m_txnMap.end()) {
        player = it->second;
        if (player == nullptr) {
            MOT_LOG_ERROR("MTLSRecoveryManager::AssignPlayer: Null player");
            return nullptr;
        }
        MOT_LOG_TRACE("Found player [%p:%lu:%lu] in txnMap for TXN [%lu:%lu], segment [%p:%u:%lu:%lu]",
            player,
            player->GetTransactionId(),
            player->GetPrevId(),
            inId,
            exId,
            segment,
            segment->m_controlBlock.m_opCode,
            segment->m_controlBlock.m_csn,
            segment->m_replayLsn);
        MOT_ASSERT(player->GetCSN() == csn && player->GetTransactionId() == inId && player->GetExternalId() == exId &&
                   !player->m_inPool && player->GetPrevId() == INVALID_TRANSACTION_ID);
        player->SetReplayLSN(segment->m_replayLsn);
    } else {
        while (true) {
            if (waitedUs > WAIT_FOR_PLAYER_TIMEOUT) {
                MOT_LOG_ERROR("MTLSRecoveryManager::AssignPlayer: Timed out after waiting for %uus", waitedUs);
                return nullptr;
            }
            uint32_t queueId = ComputeProcessorQueueId(segment->m_controlBlock);
            player = GetPlayerFromPool(queueId);
            if (player == nullptr) {
                (void)usleep(ThreadContext::THREAD_SLEEP_TIME_US);
                waitedUs += ThreadContext::THREAD_SLEEP_TIME_US;
                if (waitedUs % WAIT_FOR_PLAYER_WARNING_LOG_THRESHOLD == 0) {
                    MOT_LOG_WARN("MTLSRecoveryManager::AssignPlayer: Waiting for %uus to get a player", waitedUs);
                }
                continue;
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
            if (player->GetPrevId() != INVALID_TRANSACTION_ID) {
                (void)m_txnMap.erase(player->GetPrevId());
                player->SetPrevId(INVALID_TRANSACTION_ID);
            }

            // We only need to insert to the map if this is not a MOT only transaction or
            // a transaction that consists of more than one log segment
            if (!IsCommitOp(segment->m_controlBlock.m_opCode) || !IsMotOnlyTransaction(segment)) {
                MOT_LOG_TRACE("Inserting player %p into txnMap for TXN [%lu:%lu]", player, inId, exId);
                m_txnMap[inId] = player;
            }
            MOT_LOG_TRACE("Assigning player %p for TXN [%lu:%lu], segment [%p:%u:%lu:%lu]",
                player,
                inId,
                exId,
                segment,
                segment->m_controlBlock.m_opCode,
                segment->m_controlBlock.m_csn,
                segment->m_replayLsn);

            player->InitRedoTransactionData(inId, exId, csn, segment->m_replayLsn);
            player->SetFirstSegment(true);
            break;
        }
    }
    return player;
}

bool MTLSRecoveryManager::ApplyLogSegmentData(char* data, size_t len, uint64_t replayLsn)
{
    const std::lock_guard<std::mutex> lock(m_lock);

    MOT_LOG_DEBUG("Apply lsn %llx", replayLsn);
    if (m_numThreads.load() == 0 && StartThreads() == false) {
        MOT_LOG_ERROR("ApplyLogSegmentFromData - Failed to start recovery threads");
        return false;
    }

    SetLastReplayLsn(replayLsn);

    char* curData = data;
    while (data + len > curData) {
        if (m_errorSet.load() == true) {
            MOT_LOG_ERROR("ApplyLogSegmentFromData - Encountered an error during recovery");
            return false;
        }

        RedoLogTransactionIterator iterator(curData, len);
        uint32_t queueId = ComputeProcessorQueueId(iterator.GetEndSegmentBlock());
        if (!FlowControl(queueId)) {
            MOT_LOG_ERROR("ApplyLogSegmentFromData - Timeout");
            return false;
        }

        LogSegment* segment = iterator.AllocRedoSegment(replayLsn, m_processors[queueId]->GetAllocator());
        if (segment == nullptr) {
            MOT_LOG_ERROR("ApplyLogSegmentFromData - Failed to allocate log segment");
            m_processors[queueId]->PrintInfo();
            return false;
        }
        m_processors[queueId]->IncAlloc(segment->m_len);
        OperationCode opCode = segment->m_controlBlock.m_opCode;
        if (opCode >= OperationCode::INVALID_OPERATION_CODE) {
            MOT_LOG_ERROR("ApplyLogSegmentFromData - Encountered a bad opCode %u", opCode);
            delete segment;
            return false;
        }
        if (!ProcessSegment(segment)) {
            MOT_LOG_ERROR("ApplyLogSegmentFromData: Failed to process log segment");
            delete segment;
            return false;
        }
        curData += iterator.GetRedoTransactionLength();
    }
    m_notifier.Notify(ThreadNotifier::ThreadState::ACTIVE);
    return true;
}

bool MTLSRecoveryManager::ProcessSegment(LogSegment* segment)
{
    uint64_t intTxnId = segment->m_controlBlock.m_internalTransactionId;
    uint32_t queueId = ComputeProcessorQueueId(segment->m_controlBlock);
    RedoLogTransactionPlayer* player = AssignPlayer(segment);
    if (player == nullptr) {
        MOT_LOG_ERROR("ProcessSegment - Failed to assign a player for the segment");
        return false;
    }

    /* Set the player in the segment. */
    segment->SetPlayer(player);

    player->GetTxn()->SetInternalTransactionId(intTxnId);

    /* Update the max transaction id. */
    SetMaxTransactionId(intTxnId);

    if (IsDDLTransaction(segment) || HasUpdateIndexColumn(segment) ||
        (Is1VCCLogSegment(segment) && player->IsFirstSegment())) {
        /*
         * In case of a DDL transaction we will flush in order to avoid a case when
         * a DDL (create index for example) is processed in parallel to a big insert
         * like generate series that might not be indexed because of that.
         */
        MOT_LOG_TRACE("MTLSRecoveryManager::ProcessSegment - Draining Committer");
        DrainCommitter();
    }

    /*
     * Commit only if this is not a cross transaction, since cross transactions
     * will be committed by the commit callback.
     */
    if (IsCommitOp(segment->m_controlBlock.m_opCode) && IsMotOnlyTransaction(segment)) {
        MOT_LOG_TRACE("ProcessSegment - Putting player [%p:%lu:%lu] to the commit queue",
            player,
            player->GetTransactionId(),
            player->GetPrevId());
        if (!m_committer->QueuePut(player)) {
            MOT_LOG_ERROR("ProcessSegment - Failed to put the player [%p:%lu:%lu] to the commit queue",
                player,
                player->GetTransactionId(),
                player->GetPrevId());
            if (m_txnMap.find(player->GetTransactionId()) == m_txnMap.end()) {
                /*
                 * This is a MOT only transaction having only one log segment, which was not put in the txn map.
                 * Caution: We should not put it back to the txnPool as it is a SPSCQueue (Committer is the producer
                 * and this thread is the consumer).
                 * Nothing is replayed yet in this transaction. It is safe to directly delete without rolling back the
                 * transaction.
                 */
                MOT_LOG_TRACE("ProcessSegment - Deleting player [%p:%lu:%lu] directly on error",
                    player,
                    player->GetTransactionId(),
                    player->GetPrevId());
                delete player;
                m_numAllocatedPlayers--;
            }
            return false;
        }
        SetMaxCsn(segment->m_controlBlock.m_csn);
    }

    if (!m_processors[queueId]->QueuePut(segment)) {
        /* Player will be either in the txnMap or the commit queue. */
        MOT_LOG_ERROR("ProcessSegment - Failed to put the player [%p:%lu:%lu] to the processor queue",
            player,
            player->GetTransactionId(),
            player->GetPrevId());
        return false;
    }
    return true;
}

void MTLSRecoveryManager::DrainProcessors()
{
    m_notifier.Notify(ThreadNotifier::ThreadState::ACTIVE);
    uint32_t waitedUs = 0;
    bool shouldWait = true;
    while (shouldWait) {
        shouldWait = false;
        for (uint32_t i = 0; i < m_confNumProcessors; i++) {
            if (!m_processors[i]->QueueEmpty()) {
                (void)usleep(ThreadContext::THREAD_SLEEP_TIME_US);
                waitedUs += ThreadContext::THREAD_SLEEP_TIME_US;
                shouldWait = true;
                break;
            }
        }
        if (!shouldWait) {
            return;
        }
        if (waitedUs >= ThreadContext::THREAD_START_TIMEOUT_US) {
            MOT_LOG_WARN("DrainProcessors: processor queues are not empty");
            return;
        }
    }
}

void MTLSRecoveryManager::Flush()
{
    MOT_LOG_TRACE("MTLSRecoveryManager::Flush - Draining Processors and Committer");
    DrainProcessors();
    DrainCommitter();
}

void MTLSRecoveryManager::DrainCommitter()
{
    m_notifier.Notify(ThreadNotifier::ThreadState::ACTIVE);
    while (!m_committer->QueueEmpty() && !m_errorSet) {
        (void)usleep(ThreadContext::THREAD_SLEEP_TIME_US);
    }
}

bool MTLSRecoveryManager::CommitTransaction(uint64_t extTxnId)
{
    uint64_t intTxnId = INVALID_TRANSACTION_ID;
    const std::lock_guard<std::mutex> lock(m_lock);
    auto it = m_extToInt.find(extTxnId);
    if (it != m_extToInt.end()) {
        intTxnId = it->second;
        (void)m_extToInt.erase(it);
        MOT_LOG_TRACE(
            "MTLSRecoveryManager::CommitTransaction - Removing TXN [%lu:%lu] from extToInt map", intTxnId, extTxnId);
    }
    if (intTxnId != INVALID_TRANSACTION_ID) {
        auto txnIt = m_txnMap.find(intTxnId);
        if (txnIt != m_txnMap.end()) {
            RedoLogTransactionPlayer* player = txnIt->second;
            if (!player) {
                MOT_LOG_ERROR(
                    "MTLSRecoveryManager::CommitTransaction - Null player for TXN [%lu:%lu]", intTxnId, extTxnId);
                return false;
            }

            MOT_LOG_TRACE("MTLSRecoveryManager::CommitTransaction - Putting player [%p:%lu:%lu] for TXN [%lu:%lu] "
                          "to the commit queue",
                player,
                player->GetTransactionId(),
                player->GetPrevId(),
                intTxnId,
                extTxnId);

            MOT_ASSERT(!player->m_inPool);
            MOT_ASSERT(player->GetTransactionId() == player->GetTxn()->GetInternalTransactionId());
            if (!m_committer->QueuePut(player)) {
                MOT_LOG_ERROR("MTLSRecoveryManager::CommitTransaction - Failed to put player [%p:%lu:%lu] for "
                              "TXN [%lu:%lu] to the commit queue",
                    player,
                    player->GetTransactionId(),
                    player->GetPrevId(),
                    intTxnId,
                    extTxnId);
                return false;
            }

            MOT_LOG_TRACE("MTLSRecoveryManager::CommitTransaction - Draining Committer");
            DrainCommitter();
            return true;
        } else {
            MOT_LOG_ERROR(
                "MTLSRecoveryManager::CommitTransaction - No player found for TXN [%lu:%lu]", intTxnId, extTxnId);
            return false;
        }
    }
    return true;
}

bool MTLSRecoveryManager::InitializeCommitterContext()
{
    m_committer = new (std::nothrow) MTLSTransactionCommitterContext(this, &m_notifier, m_confQueueSize);
    if (m_committer == nullptr) {
        MOT_LOG_ERROR("MTLSRecoveryManager::InitializeProcessorsContext: failed to allocate committer");
        return false;
    }
    if (!m_committer->Initialize()) {
        MOT_LOG_ERROR("MTLSRecoveryManager::InitializeProcessorsContext: failed to init committer");
        delete m_committer;
        m_committer = nullptr;
        return false;
    }
    return true;
}

void MTLSRecoveryManager::CleanupCommitterContext()
{
    if (m_committer != nullptr) {
        m_committer->Cleanup();
    }
}

void MTLSRecoveryManager::DestroyCommitterContext()
{
    if (m_committer != nullptr) {
        delete m_committer;
        m_committer = nullptr;
    }
}

bool MTLSRecoveryManager::StartCommitter()
{
    m_threads.push_back(std::thread(TransactionCommitterThread, m_committer));
    if (!WaitForThreadStart(m_committer)) {
        MOT_LOG_ERROR("MTLSRecoveryManager::StartProcessors: Failed to start committer");
        return false;
    }
    (void)++m_numThreads;
    return true;
}

bool MTLSRecoveryManager::InitializeProcessorsContext()
{
    for (uint32_t i = 0; i < m_confNumProcessors; i++) {
        MTLSTransactionProcessorContext* processor =
            new (std::nothrow) MTLSTransactionProcessorContext(this, &m_notifier, i, m_confQueueSize);
        if (processor == nullptr) {
            MOT_LOG_ERROR("MTLSRecoveryManager::InitializeProcessorsContext: failed to init processor %u", i);
            return false;
        }
        if (!processor->Initialize()) {
            MOT_LOG_ERROR("MTLSRecoveryManager::InitializeProcessorsContext: failed to init processor %u", i);
            delete processor;
            return false;
        }
        m_processors.push_back(processor);
    }
    return true;
}

void MTLSRecoveryManager::CleanupProcessorsContext()
{
    for (uint32_t i = 0; i < m_confNumProcessors; i++) {
        if (m_processors[i] != nullptr) {
            m_processors[i]->Cleanup();
        }
    }
}

void MTLSRecoveryManager::DestroyProcessorsContext()
{
    for (uint32_t i = 0; i < m_confNumProcessors; i++) {
        if (m_processors[i] != nullptr) {
            delete m_processors[i];
        }
    }
    m_processors.clear();
}

bool MTLSRecoveryManager::StartProcessors()
{
    bool result = true;
    for (uint32_t i = 0; i < m_confNumProcessors; i++) {
        m_threads.push_back(std::thread(TransactionProcessorThread, m_processors[i]));
        if (!WaitForThreadStart(m_processors[i])) {
            result = false;
            MOT_LOG_ERROR("MTLSRecoveryManager::StartProcessors: Failed to start processor %u", i);
            break;
        }
        (void)++m_numThreads;
    }
    return result;
}

RedoLogTransactionPlayer* MTLSRecoveryManager::CreatePlayer()
{
    RedoLogTransactionPlayer* player = new (std::nothrow) RedoLogTransactionPlayer(this);
    if (player == nullptr) {
        MOT_LOG_ERROR("MTLSRecoveryManager::CreatePlayer: failed to create player object");
        return nullptr;
    }
    void* txnMem = malloc(sizeof(TxnManager));
    if (txnMem == nullptr) {
        MOT_LOG_ERROR("MTLSRecoveryManager::CreatePlayer: failed to allocate txn manager");
        delete player;
        return nullptr;
    }
    TxnManager* txn = new (txnMem) TxnManager(nullptr, true);
    if (!txn->Init(0, 0, true)) {
        MOT_LOG_ERROR("MTLSRecoveryManager::CreatePlayer: failed to init txn manager");
        free(txnMem);
        delete player;
        return nullptr;
    }
    txn->GcAddSession();
    txn->GetGcSession()->SetGcType(GcManager::GC_TYPE::GC_RECOVERY);
    player->Init(txn);
    return player;
}

bool MTLSRecoveryManager::InitTxnPool()
{
    m_txnPool = (SPSCQueue<RedoLogTransactionPlayer>**)calloc(
        m_confNumProcessors, sizeof(SPSCQueue<RedoLogTransactionPlayer>*));
    if (m_txnPool == nullptr) {
        return false;
    }

    m_processorPlayerCounts = (uint32_t*)calloc(m_confNumProcessors, sizeof(uint32_t));
    if (m_processorPlayerCounts == nullptr) {
        free(m_txnPool);
        m_txnPool = nullptr;
        return false;
    }

    uint32_t queueSize = ComputeNearestHighPow2(m_processorQueueSize);
    for (uint32_t i = 0; i < m_confNumProcessors; i++) {
        m_txnPool[i] = new (std::nothrow) SPSCQueue<RedoLogTransactionPlayer>(queueSize);
        if (m_txnPool[i] == nullptr) {
            DestroyTxnPool();
            return false;
        }
        if (!m_txnPool[i]->Init()) {
            DestroyTxnPool();
            return false;
        }
    }

    return true;
}

uint64_t MTLSRecoveryManager::SerializePendingRecoveryData(int fd)
{
    uint64_t retErr = (uint64_t)(-1);
    uint64_t numTxns = 0;
    if (fd < 0) {
        MOT_LOG_ERROR("MTLSRecoveryManager::SerializePendingRecoveryData: bad fd");
        return retErr;
    }

    PendingTxnLogger logger;
    if (!logger.Init()) {
        MOT_LOG_ERROR("MTLSRecoveryManager::SerializePendingRecoveryData: failed to init logger");
        return retErr;
    }

    auto it = m_txnMap.begin();
    while (it != m_txnMap.end()) {
        RedoLogTransactionPlayer* player = it->second;
        if (player == nullptr) {
            MOT_LOG_ERROR("MTLSRecoveryManager::SerializePendingRecoveryData: unexpected null player");
            return retErr;
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
        if (player->GetPrevId() != INVALID_TRANSACTION_ID) {
            MOT_LOG_DEBUG(
                "MTLSRecoveryManager::SerializePendingRecoveryData: txn %lu is committed", player->GetPrevId());
            MOT_ASSERT(player->GetPrevId() == player->GetTransactionId());
            it = m_txnMap.erase(it);
            player->SetPrevId(INVALID_TRANSACTION_ID);
            continue;
        }

        if (logger.SerializePendingTransaction(player->GetTxn(), fd) != RC_OK) {
            MOT_LOG_ERROR("MTLSRecoveryManager::SerializePendingRecoveryData: failed to serialize");
            return retErr;
        }

        MOT_LOG_INFO("MTLSRecoveryManager::SerializePendingRecoveryData serialized %d (%d) (%lu)",
            numTxns + 1,
            player->IsProcessed(),
            player->GetTransactionId());
        ++numTxns;
        (void)++it;
    }
    MOT_LOG_TRACE("MTLSRecoveryManager::SerializePendingRecoveryData fd: %d txns: %lu", fd, numTxns);
    return numTxns;
}

bool MTLSRecoveryManager::DeserializePendingRecoveryData(int fd)
{
    MOT_LOG_INFO("MTLSRecoveryManager::DeserializePendingRecoveryData %d", fd);
    if (m_numThreads.load() == 0 && StartThreads() == false) {
        MOT_LOG_ERROR("DeserializePendingRecoveryData: Failed to start recovery threads");
        return false;
    }

    uint32_t metaVersion = CheckpointControlFile::GetCtrlFile()->GetMetaVersion();
    uint32_t readEntries = 0;
    size_t bufSize = 0;
    char* buf = nullptr;
    bool result = false;
    while (true) {
        size_t readSize = 0;
        PendingTxnLogger::Header header;

        result = false;
        if (metaVersion < METADATA_VER_LOW_RTO) {
            CheckpointUtils::TpcEntryHeader tpcHeader;
            readSize = CheckpointUtils::ReadFile(fd, (char*)&tpcHeader, sizeof(CheckpointUtils::TpcEntryHeader));
            if (readSize == 0) {
                MOT_LOG_INFO("DeserializePendingRecoveryData: TpcFile EOF, read %d entries", readEntries);
                result = true;
                break;
            } else if (readSize != sizeof(CheckpointUtils::TpcEntryHeader)) {
                MOT_LOG_ERROR("DeserializePendingRecoveryData: Failed to read TpcEntryHeader",
                    readSize,
                    errno,
                    gs_strerror(errno));
                break;
            }
            header.m_magic = tpcHeader.m_magic;
            header.m_len = tpcHeader.m_len;
            header.m_replayLsn = 0;
        } else {
            readSize = CheckpointUtils::ReadFile(fd, (char*)&header, sizeof(PendingTxnLogger::Header));
            if (readSize == 0) {
                MOT_LOG_INFO("DeserializePendingRecoveryData: PendingTxnDataFile EOF, read %d entries", readEntries);
                result = true;
                break;
            } else if (readSize != sizeof(PendingTxnLogger::Header)) {
                MOT_LOG_ERROR("DeserializePendingRecoveryData: Failed to read TransactionLogger::Header",
                    readSize,
                    errno,
                    gs_strerror(errno));
                break;
            }
        }

        if (header.m_magic != PendingTxnLogger::MAGIC_NUMBER ||
            header.m_len > RedoLogBuffer::REDO_DEFAULT_BUFFER_SIZE) {
            MOT_LOG_ERROR("DeserializePendingRecoveryData: Bad entry %lx - %lu", header.m_magic, header.m_len);
            break;
        }

        MOT_LOG_TRACE("DeserializePendingRecoveryData: Entry length %lu", header.m_len);
        if (buf == nullptr || header.m_len > bufSize) {
            if (buf != nullptr) {
                free(buf);
            }
            buf = (char*)malloc(header.m_len);
            bufSize = header.m_len;
        }
        if (buf == nullptr) {
            MOT_LOG_ERROR("DeserializePendingRecoveryData: Failed to allocate buffer (%lu bytes)", header.m_len);
            break;
        }

        if (CheckpointUtils::ReadFile(fd, buf, header.m_len) != header.m_len) {
            MOT_LOG_ERROR("DeserializePendingRecoveryData: Failed to read data from file (%lu bytes)", header.m_len);
            break;
        }

        if (!ApplyPendingRecoveryLogSegmentData(buf, header.m_len, header.m_replayLsn, metaVersion)) {
            MOT_LOG_ERROR("DeserializePendingRecoveryData: Failed to allocate segment");
            break;
        }

        m_checkpointRecovery.SetLastReplayLsn(header.m_replayLsn);
        SetLastReplayLsn(header.m_replayLsn);

        readEntries++;
    }

    if (buf != nullptr) {
        free(buf);
    }

    MOT_LOG_INFO("DeserializePendingRecoveryData: readEntries %lu, mapsize %lu", readEntries, m_txnMap.size());
    m_notifier.Notify(ThreadNotifier::ThreadState::ACTIVE);
    return result;
}

bool MTLSRecoveryManager::ApplyPendingRecoveryLogSegmentData(
    char* buf, uint64_t len, uint64_t replayLsn, uint32_t metaVersion)
{
    uint32_t queueId = 0;
    LogSegment* segment = nullptr;

    if (metaVersion < METADATA_VER_LOW_RTO) {
        segment = new (std::nothrow) LogSegment();
        if (segment == nullptr) {
            MOT_LOG_ERROR("ApplyPendingRecoveryLogSegmentData (upgrade): Failed to allocate log segment");
            return false;
        }
        segment->m_replayLsn = replayLsn;
        errno_t erc = memcpy_s(&segment->m_len, sizeof(size_t), buf, sizeof(size_t));
        securec_check(erc, "\0", "\0");

        EndSegmentBlock* controlBlock =
            (EndSegmentBlock*)(buf + sizeof(size_t) + segment->m_len - sizeof(EndSegmentBlock));
        queueId = ComputeProcessorQueueId(*controlBlock);
        segment->m_data = (char*)(m_processors[queueId]->GetAllocator()->Alloc(segment->m_len * sizeof(char)));
        if (segment->m_data == nullptr) {
            MOT_LOG_ERROR(
                "ApplyPendingRecoveryLogSegmentData (upgrade): Failed to allocate memory for log segment data");
            delete segment;
            return false;
        }
        segment->m_allocator = m_processors[queueId]->GetAllocator();
        segment->Deserialize(buf);
    } else {
        RedoLogTransactionIterator iterator(buf, len);
        queueId = ComputeProcessorQueueId(iterator.GetEndSegmentBlock());
        segment = iterator.AllocRedoSegment(replayLsn, m_processors[queueId]->GetAllocator());
        if (segment == nullptr) {
            MOT_LOG_ERROR("ApplyPendingRecoveryLogSegmentData: Failed to allocate log segment");
            return false;
        }
    }

    m_processors[queueId]->IncAlloc(segment->m_len);

    if (!ProcessSegment(segment)) {
        MOT_LOG_ERROR("ApplyPendingRecoveryLogSegmentData: Failed to process log segment");
        delete segment;
        return false;
    }

    return true;
}

bool MTLSRecoveryManager::FlowControl(uint32_t queueId)
{
    uint32_t waitedUs = 0;
    while (m_processors[queueId]->GetMemUsagePercentage() >= QUEUE_MAX_CAPACITY_THRESHOLD) {
        (void)usleep(1000);
        waitedUs += 1000;
        if (waitedUs >= QUEUE_MAX_CAPACITY_TIMEOUT) {
            MOT_LOG_ERROR("MTLSRecoveryManager: flow control - timeout");
            m_processors[queueId]->PrintInfo();
            return false;
        }
    }
    if (waitedUs) {
        MOT_LOG_DEBUG("MTLSRecoveryManager: flow control waited %lu us", waitedUs);
    }
    return true;
}

RedoLogTransactionPlayer* MTLSRecoveryManager::GetPlayerFromPool(uint32_t queueId)
{
    RedoLogTransactionPlayer* player = m_txnPool[queueId]->Take();
    if (player == nullptr) {
        /* SPSCQueue can hold only maximum of (QUEUE_SIZE - 1) entries. */
        if ((m_numAllocatedPlayers + 1) >= m_confQueueSize) {
            MOT_LOG_DEBUG(
                "MTLSRecoveryManager::GetPlayerFromPool - Recovery queue limit (%u) reached", m_confQueueSize);
            return nullptr;
        }

        if ((m_processorPlayerCounts[queueId] + 1) >= m_processorQueueSize) {
            MOT_LOG_DEBUG("MTLSRecoveryManager::GetPlayerFromPool - Recovery queue (%u) limit (%u) reached",
                queueId,
                m_processorQueueSize);
            return nullptr;
        }

        player = CreatePlayer();
        if (player == nullptr) {
            MOT_LOG_ERROR("MTLSRecoveryManager::GetPlayerFromPool - Failed to create player");
            return nullptr;
        }

        player->m_queueId = queueId;

        /*
         * Caution: Do not put the player to the txnPool (SPSCQueue) as it will break the SPSCQueue.
         * This thread is the consumer and committer is the producer of the txnPool, so we should not try to put
         * the player to the txnPool. Caller will put the player either to the commit queue (for single-segment MOT
         * only txn) or to the txnMap (for multi-segment MOT only txn or cross txn).
         */
        MOT_ASSERT(!player->m_inPool);

        /*
         * Note: Even though we are not putting the player to the txnPool, we still have to increase the
         * m_numAllocatedPlayers in order to respect the configured queue limit. Whenever a player is
         * deleted/destroyed, m_numAllocatedPlayers needs to be decremented accordingly.
         */
        m_numAllocatedPlayers++;
        m_processorPlayerCounts[queueId]++;
        MOT_LOG_TRACE("MTLSRecoveryManager::GetPlayerFromPool - Current number of players: %u", m_numAllocatedPlayers);
        MOT_LOG_TRACE("MTLSRecoveryManager::GetPlayerFromPool - Allocated new player [%p:%lu:%lu]",
            player,
            player->GetTransactionId(),
            player->GetPrevId());
        if ((m_numAllocatedPlayers + 1) == m_confQueueSize) {
            MOT_LOG_WARN("MTLSRecoveryManager::GetPlayerFromPool - Recovery queue limit (%u) reached", m_confQueueSize);
        }
    } else {
        MOT_ASSERT(player->m_inPool);
        player->m_inPool = false;
        MOT_LOG_TRACE("MTLSRecoveryManager::GetPlayerFromPool - Got player [%p:%lu:%lu] from the txnPool",
            player,
            player->GetTransactionId(),
            player->GetPrevId());
    }
    return player;
}

void MTLSRecoveryManager::CleanupTxnPool()
{
    MOT_LOG_TRACE("MTLSRecoveryManager::CleanupTxnPool - Cleaning txnPool");
    uint32_t deleteCount = 0;
    if (m_txnPool != nullptr) {
        for (uint32_t i = 0; i < m_confNumProcessors; i++) {
            if (m_txnPool[i] == nullptr) {
                continue;
            }
            while (m_txnPool[i]->Top() != nullptr) {
                RedoLogTransactionPlayer* player = m_txnPool[i]->Take();
                MOT_LOG_TRACE("MTLSRecoveryManager::CleanupTxnPool - Deleting player %p", player);
                SessionContext::SetTxnContext(player->GetTxn());
                delete player;
                m_numAllocatedPlayers--;
                deleteCount++;
            }
        }
    }

    if (m_processorPlayerCounts != nullptr) {
        for (uint32_t i = 0; i < m_confNumProcessors; i++) {
            m_processorPlayerCounts[i] = 0;
        }
    }

    MOT_ASSERT(m_numAllocatedPlayers == 0);
    m_numAllocatedPlayers = 0;
    MOT_LOG_TRACE("MTLSRecoveryManager::CleanupTxnPool - Deleted %u players from the txnPool", deleteCount);
}

void MTLSRecoveryManager::DestroyTxnPool()
{
    CleanupTxnPool();
    if (m_txnPool != nullptr) {
        for (uint32_t i = 0; i < m_confNumProcessors; i++) {
            if (m_txnPool[i] == nullptr) {
                continue;
            }
            delete m_txnPool[i];
            m_txnPool[i] = nullptr;
        }

        free(m_txnPool);
        m_txnPool = nullptr;
    }

    if (m_processorPlayerCounts != nullptr) {
        free(m_processorPlayerCounts);
        m_processorPlayerCounts = nullptr;
    }
}

void MTLSRecoveryManager::CleanupTxnMap()
{
    MOT_LOG_TRACE("MTLSRecoveryManager::CleanupTxnMap - Cleaning txnMap");
    uint32_t releaseCount = 0;
    auto it = m_txnMap.begin();
    while (it != m_txnMap.end()) {
        RedoLogTransactionPlayer* player = it->second;
        if (player != nullptr && player->GetPrevId() == INVALID_TRANSACTION_ID) {
            it = m_txnMap.erase(it);

            /*
             * First rollback the transaction and then release the player back to the pool.
             * Caution: This is necessary for GC to work properly. Rollback and delete transaction object cannot happen
             * together. First phase, we need to rollback all the transactions and then all the transactions objects
             * can be deleted.
             * We can safely release the player back to the txnPool, because at this point there is no committer thread
             * and hence we don't mess with the SPSCQueue.
             */
            SessionContext::SetTxnContext(player->GetTxn());
            player->GetTxn()->Rollback();
            MOT_LOG_TRACE("MTLSRecoveryManager::CleanupTxnMap - Releasing player [%p:%lu:%lu] back to the txnPool",
                player,
                player->GetTransactionId(),
                player->GetPrevId());
            ReleasePlayer(player);
            releaseCount++;
            continue;
        }
        (void)++it;
    }
    m_txnMap.clear();
    MOT_LOG_TRACE("MTLSRecoveryManager::CleanupTxnMap - Released %u players back to the txnPool", releaseCount);
}
}  // namespace MOT
