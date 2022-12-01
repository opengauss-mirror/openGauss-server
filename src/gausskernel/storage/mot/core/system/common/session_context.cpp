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
 * session_context.cpp
 *    A session context stands for all resources related to managing a user connection.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/common/session_context.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "session_context.h"
#include "txn.h"
#include "utilities.h"
#include "mot_engine.h"
#include "sys_numa_api.h"

// For Masstree
#include "kvthread.hh"

#include <pthread.h>
#include <cstdio>
#include <utmpx.h>

namespace MOT {
DECLARE_LOGGER(SessionContext, System);

std::atomic<SessionId> SessionContext::m_nextSessionId(0);

static int GetRealCurrentNumaMode()
{
    // We default to Node 0 to avoid failures in other places in the code.
    int node = 0;
    if (GetGlobalConfiguration().m_enableNuma) {
        int cpu = sched_getcpu();
        if (cpu < 0) {
            MOT_REPORT_SYSTEM_ERROR(sched_getcpu, "Initialize Session", "Failed to get current CPU id");
            return -1;
        }
        node = MotSysNumaGetNode(cpu);
        if (node < 0) {
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL, "Initialize Session", "Failed to get NUMA node for CPU %d", cpu);
            return -1;
        }
    }
    return node;
}

extern bool InitCurrentNumaNodeId()
{
    if (MOTCurrentNumaNodeId != MEM_INVALID_NODE) {
        MOT_LOG_TRACE(
            "Ignoring double attempt to initialize current NUMA node identifier for current thread (thread: %" PRIu16
            ", node: %d)",
            MOTCurrThreadId,
            MOTCurrentNumaNodeId);
        return true;
    }

    if (MOTCurrThreadId == INVALID_THREAD_ID) {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
            "Init Engine",
            "Invalid attempt to initialize current NUMA node identifier without current thread identifier denied");
        return false;
    }

    if (IS_AFFINITY_ACTIVE(GetSessionAffinity().GetAffinityMode())) {
        int nodeId = GetSessionAffinity().GetAffineNuma(MOTCurrThreadId);
        if (nodeId == -1) {
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL, "Init Engine", "Failed to get NUMA node id for current thread");
            return false;
        }
        MOTCurrentNumaNodeId = nodeId;
        MOT_LOG_TRACE("Initialized current NUMA node identifier to %d for thread %" PRIu16 " by affinity map",
            MOTCurrentNumaNodeId,
            MOTCurrThreadId);
    } else {
        int nodeId = GetRealCurrentNumaMode();
        if (nodeId == -1) {
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL, "Init Engine", "Failed to get real NUMA node id");
            return false;
        }
        MOTCurrentNumaNodeId = nodeId;
        MOT_LOG_TRACE("Initialized current NUMA node identifier to %d for thread %" PRIu16
                      " by kernel info (pthread: %" PRIu64 ")",
            MOTCurrentNumaNodeId,
            MOTCurrThreadId,
            (uint64_t)pthread_self());
    }
    return true;
}

extern void ClearCurrentNumaNodeId()
{
    if (MOTCurrentNumaNodeId != MEM_INVALID_NODE) {
        MOT_LOG_TRACE("Clearing current NUMA node identifier %d for thread %u", MOTCurrentNumaNodeId, MOTCurrThreadId);
        MOTCurrentNumaNodeId = MEM_INVALID_NODE;
    } else {
        MOT_LOG_WARN(
            "Ignoring double attempt to clear current NUMA node identifier for current thread %u", MOTCurrThreadId);
    }
}

extern bool InitMasstreeThreadinfo()
{
    if (mtSessionThreadInfo == nullptr) {
        MOT_LOG_TRACE("InitMasstreeThreadinfo(): Create mtSessionThreadInfo %p. MOTCurrThreadId: %u\n",
            mtSessionThreadInfo,
            MOTCurrThreadId);
        mtSessionThreadInfo =
            threadinfo::make(mtSessionThreadInfo, threadinfo::TI_PROCESS, MOTCurrThreadId, 0 /* Create object */);
        if (mtSessionThreadInfo == nullptr) {
            return false;
        }
    }
    return true;
}

extern void DestroyMasstreeThreadinfo()
{
    if (mtSessionThreadInfo != nullptr) {
        MOT_LOG_TRACE("DestroyMasstreeThreadinfo(): Destroy mtSessionThreadInfo %p. MOTCurrThreadId: %u\n",
            mtSessionThreadInfo,
            MOTCurrThreadId);

        (void)threadinfo::make(mtSessionThreadInfo, threadinfo::TI_PROCESS, MOTCurrThreadId, -1 /* Destroy object */);
        mtSessionThreadInfo = nullptr;
    }
}

void SessionContext::SetTxnContext(TxnManager* txn)
{
    if (txn == nullptr) {
        return;
    }

    txn->SetThdId(MOTCurrThreadId);
    txn->SetConnectionId(MOT_GET_CURRENT_CONNECTION_ID());
    if (u_sess != nullptr) {
        u_sess->mot_cxt.txn_manager = txn;
    }
}

SessionContext::SessionContext(
    SessionId sessionId, ConnectionId connectionId, void* userData, bool isMtls /* = false */)
    : m_sessionId(sessionId), m_connectionId(connectionId), m_refCount(1), m_txn(nullptr), m_isMtls(isMtls ? 1 : 0)
{
    MOT_LOG_TRACE("Session Id: %u, Connection Id: %u, NUMA node id: %d, thread id: %u",
        m_sessionId,
        connectionId,
        MOTCurrentNumaNodeId,
        (unsigned)MOTCurrThreadId);
}

SessionContext::~SessionContext()
{
    if (m_txn != nullptr) {
        MOT_ASSERT(!m_isMtls);
        MemSessionFreeObject(m_txn);
        m_txn = nullptr;
    }
}

bool SessionContext::Init(bool isLightSession)
{
    if (!m_isMtls) {
        // ATTENTION: using internal thread id for NUMA affinity
        MOTThreadId threadId = MOTCurrThreadId;
        MOT_LOG_DEBUG("SessionContext::Init - memory pools allocated for threadId=%u", (unsigned)threadId);
        m_txn = CreateTransaction(this, threadId, m_connectionId, isLightSession);
        if (m_txn == nullptr) {
            MOT_LOG_ERROR(
                "Failed to create transaction manager for session %u on thread %u", m_sessionId, (unsigned)threadId);
            return false;
        }
    }

    if (!InitMasstreeThreadinfo()) {
        if (!m_isMtls) {
            MemSessionFreeObject(m_txn);
            m_txn = nullptr;
        }
        return false;
    }

    return true;
}

TxnManager* SessionContext::CreateTransaction(
    SessionContext* sessionContext, MOTThreadId threadId, ConnectionId connectionId, bool isLightTxn)
{
    // allocate as session-local object
    TxnManager* txnManager = MemSessionAllocAlignedObject<TxnManager>(L1_CACHE_LINE, sessionContext);
    if (txnManager == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Create Session",
            "Failed to allocate %u session-local bytes for reusable session transaction object",
            (unsigned)sizeof(TxnManager));
        return nullptr;
    }

    // Do it before txn_man::init call since it allocates memory
    bool rc = true;
    if (!isLightTxn) {
        if (GetGlobalConfiguration().m_enableNuma && IS_AFFINITY_ACTIVE(GetSessionAffinity().GetAffinityMode())) {
            if (!GetSessionAffinity().SetAffinity(threadId)) {
                MOT_LOG_WARN("Failed to set current session affinity, performance may be affected");
            }
        }
        rc = txnManager->Init(threadId, connectionId, false);
    } else {
        rc = txnManager->Init(threadId, connectionId, false, true);
    }

    if (unlikely(!rc)) {
        MemSessionFreeObject(txnManager);
        return nullptr;
    }

    return txnManager;
}
}  // namespace MOT
