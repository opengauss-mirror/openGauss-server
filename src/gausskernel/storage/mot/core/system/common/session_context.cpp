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
#include <stdio.h>
#include <utmpx.h>

namespace MOT {
DECLARE_LOGGER(SessionContext, System);

std::atomic<SessionId> SessionContext::m_nextSessionId(0);

static int GetRealCurrentNumaMode()
{
    int node = 0;  // We default to Node 0 to avoid failures in other places in the code.
    if (GetGlobalConfiguration().m_enableNuma) {
        int cpu = sched_getcpu();
        node = MotSysNumaGetNode(cpu);
    }
    return node;
}

extern void InitCurrentNumaNodeId()
{
    if (MOTCurrentNumaNodeId == MEM_INVALID_NODE) {
        if (MOTCurrThreadId == INVALID_THREAD_ID) {
            MOT_LOG_ERROR(
                "Invalid attempt to initialize current NUMA node identifier without current thread identifier denied");
            SetLastError(MOT_ERROR_INTERNAL, MOT_SEVERITY_ERROR);
        } else {
            if (IS_AFFINITY_ACTIVE(GetSessionAffinity().GetAffinityMode())) {
                MOTCurrentNumaNodeId = GetSessionAffinity().GetAffineNuma(MOTCurrThreadId);
                MOT_LOG_TRACE("Initialized current NUMA node identifier to %d for thread %" PRIu16 " by affinity map",
                    MOTCurrentNumaNodeId,
                    MOTCurrThreadId);
            } else {
                MOTCurrentNumaNodeId = GetRealCurrentNumaMode();
                MOT_LOG_TRACE("Initialized current NUMA node identifier to %d for thread %" PRIu16
                              " by kernel info (pthread: %" PRIu64 ")",
                    MOTCurrentNumaNodeId,
                    MOTCurrThreadId,
                    (uint64_t)pthread_self());
            }
        }
    } else {
        MOT_LOG_TRACE(
            "Ignoring double attempt to initialize current NUMA node identifier for current thread (thread: %" PRIu16
            ", node: %d)",
            MOTCurrThreadId,
            MOTCurrentNumaNodeId);
    }
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
        mtSessionThreadInfo = (threadinfo*)malloc(sizeof(threadinfo));

        if (mtSessionThreadInfo == nullptr) {
            return false;
        }

        mtSessionThreadInfo = threadinfo::make(mtSessionThreadInfo, threadinfo::TI_PROCESS, MOTCurrThreadId, 0);
    }
    return true;
}

extern void DestroyMasstreeThreadinfo()
{
    if (mtSessionThreadInfo != nullptr) {
        free(mtSessionThreadInfo);
        mtSessionThreadInfo = nullptr;
    }
}

SessionContext::SessionContext(SessionId sessionId, ConnectionId connectionId, void* userData)
    : m_sessionId(sessionId), m_connectionId(connectionId), m_refCount(1), m_txn(nullptr)
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
        MemSessionFreeObject(m_txn);
        m_txn = nullptr;
    }
}

bool SessionContext::Init(bool isLightSession)
{
    // ATTENTION: using internal thread id for NUMA affinity
    MOTThreadId threadId = MOTCurrThreadId;
    MOT_LOG_DEBUG("txn_man::init - memory pools allocated for threadId=%u", (unsigned)threadId);
    m_txn = CreateTransaction(this, threadId, m_connectionId, isLightSession);
    if (m_txn == nullptr) {
        MOT_LOG_ERROR(
            "Failed to create transaction manager for session %u on thread %u", m_sessionId, (unsigned)threadId);
        return false;
    }

    if (!InitMasstreeThreadinfo()) {
        MemSessionFreeObject(m_txn);
        m_txn = nullptr;
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
    if (isLightTxn == false) {
        if (GetGlobalConfiguration().m_enableNuma && IS_AFFINITY_ACTIVE(GetSessionAffinity().GetAffinityMode())) {
            if (!GetSessionAffinity().SetAffinity(threadId)) {
                MOT_LOG_WARN("Failed to set current session affinity, performance may be affected");
            }
        }
        rc = txnManager->Init(threadId, connectionId);
    } else {
        rc = txnManager->Init(threadId, connectionId, true);
    }

    if (unlikely(!rc)) {
        MemSessionFreeObject(txnManager);
        return nullptr;
    }

    return txnManager;
}
}  // namespace MOT
