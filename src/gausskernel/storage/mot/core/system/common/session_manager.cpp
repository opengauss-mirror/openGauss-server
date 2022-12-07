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
 * session_manager.cpp
 *    Manages all the sessions.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/common/session_manager.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "session_manager.h"
#include "utilities.h"
#include "mm_def.h"
#include "mm_session_api.h"
#include "mm_cfg.h"
#include "mot_engine.h"
#include "statistics_manager.h"
#include "mm_numa.h"

#include "knl/knl_thread.h"
#include "knl/knl_session.h"

namespace MOT {
DECLARE_LOGGER(SessionManager, System)

static void PrintActiveSession(SessionId sessionId, SessionContext* sessionContext)
{
    MOT_LOG_WARN("Still active session: %u", sessionId);
}

bool SessionManager::Initialize(uint32_t nodeCount, uint32_t threadCount)
{
    bool result = true;

    m_nodeCount = nodeCount;
    m_threadCount = threadCount;

    MOT_LOG_TRACE("Startup: Session manager initialized successfully");

    return result;
}

void SessionManager::Destroy()
{}

void SessionManager::CleanupFailedSessionContext(
    ConnectionId connectionId, SessionId sessionId, SessionInitPhase initPhase, SessionContext*& sessionContext)
{
    MOT_ASSERT(initPhase != SESSION_INIT_DONE);
    MOT_LOG_ERROR("Failed to create session object for thread id %" PRIu16, MOTCurrThreadId);
    switch (initPhase) {
        case RESERVE_THREAD_SLOT_PHASE:
            StatisticsManager::GetInstance().UnreserveThreadSlot();

        // fall through
        case INIT_SESSION_PHASE:
            // Remove from link list
            sessionContext->GetTxnManager()->GcRemoveSession();

            // remove from session map
            if (!m_sessionContextMap.remove(MOT_GET_CURRENT_SESSION_ID())) {
                MOT_LOG_WARN(
                    "Failed to remove session %u from global session map - not found", MOT_GET_CURRENT_SESSION_ID());
            }

        // fall through
        case ALLOC_SESSION_BUFFER_PHASE:
            sessionContext->~SessionContext();
#ifdef MEM_SESSION_ACTIVE
            MemSessionFree(sessionContext);
#else
            free(sessionContext);
#endif
            sessionContext = nullptr;

        // fall through
        case RESERVE_SESSION_MEMORY_PHASE:
#ifdef MEM_SESSION_ACTIVE
            MemSessionUnreserve();
#endif

        // fall through
        case SETUP_MASSTREE_INFO_PHASE:
            DestroyMasstreeThreadinfo();

        // fall through
        // we keep thread-level attribute of NUMA node identifier initialized, since on thread-pooled envelope
        // other sessions might still use this worker thread
        case SETUP_NODE_ID_PHASE:

        // fall through
        case ALLOC_CONNECTION_ID_PHASE:
            FreeConnectionId(connectionId);
            MOT_ASSERT(connectionId == MOT_GET_CURRENT_CONNECTION_ID());
            MOT_ASSERT(sessionId == MOT_GET_CURRENT_SESSION_ID());
            MOT_SET_CURRENT_CONNECTION_ID(INVALID_CONNECTION_ID);
            MOT_SET_CURRENT_SESSION_ID(INVALID_SESSION_ID);

        // fall through
        // we keep thread-level attribute of thread identifier initialized, since on thread-pooled envelope
        // other sessions might still use this worker thread
        case ALLOC_THREAD_ID_PHASE:

        // fall through
        case SESSION_INIT_START:
        default:
            break;
    }
}

SessionContext* SessionManager::CreateSessionContext(bool isLightSession /* = false */,
    uint64_t reserveMemoryKb /* = 0 */, void* userData /* = nullptr */,
    ConnectionId connectionId /* = INVALID_CONNECTION_ID */, bool isMtls /* = false */)
{
    // reject request if a session was already defined for the current thread
    SessionContext* sessionContext = MOT_GET_CURRENT_SESSION_CONTEXT();
    if (sessionContext != nullptr) {
        MOT_LOG_TRACE(
            "Silently ignoring request to create session context and returning existing one instead (session id: %u)",
            (unsigned)sessionContext->GetSessionId());
        sessionContext->IncRefCount();
        return sessionContext;
    }

    // session initialization is complex, so let's divide it into phases, and cleanup once at the end if required
    SessionInitPhase initPhase = SESSION_INIT_START;

    SessionId sessionId = INVALID_SESSION_ID;
    do {  // instead of goto
        // phase 1: setup all identifiers
        MOTThreadId threadId = AllocThreadId();
        if (threadId == INVALID_THREAD_ID) {
            MOT_REPORT_ERROR(
                MOT_ERROR_INTERNAL, "Create Session Context", "Failed to allocate thread id for new session");
            break;
        }
        initPhase = ALLOC_THREAD_ID_PHASE;

        // current connection cannot be already defined
        // if the session is to have a pre-allocated connection id, then use the connectionId argument
        if (MOT_GET_CURRENT_CONNECTION_ID() != INVALID_CONNECTION_ID) {
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                "Create Session Context",
                "Connection identifier already defined for current session");
            break;
        }
        if (connectionId == INVALID_CONNECTION_ID) {
            connectionId = AllocConnectionId();
        }
        if (connectionId == INVALID_CONNECTION_ID) {
            MOT_REPORT_ERROR(
                MOT_ERROR_INTERNAL, "Create Session Context", "Failed to allocate connection id for new session");
            break;
        }
        MOT_SET_CURRENT_CONNECTION_ID(connectionId);
        initPhase = ALLOC_CONNECTION_ID_PHASE;

        // initialize session id
        if (MOT_GET_CURRENT_SESSION_ID() != INVALID_SESSION_ID) {
            MOT_REPORT_ERROR(
                MOT_ERROR_INTERNAL, "Create Session Context", "Session identifier already defined for current session");
            break;
        }
        sessionId = SessionContext::AllocSessionId();
        MOT_SET_CURRENT_SESSION_ID(sessionId);
        initPhase = ALLOC_SESSION_ID_PHASE;

        // initialize NUMA node id for current thread if not initialized already
        // although this is thread-level attribute (even in a thread-pooled envelope) we still initialize it on-demand
        // whenever a new session starts, since we do not have a "on-thread-started" event (and delivering such an event
        // in the correct timing could be difficult, cumbersome and eventually counter-productive)
        if (!InitCurrentNumaNodeId()) {
            MOT_REPORT_ERROR(
                MOT_ERROR_INTERNAL, "Create Session Context", "Failed to initializes NUMA node identifier");
            break;
        }
        MOT_LOG_TRACE("Creating session for thread id %" PRIu16
                      ", connection id %u, node id %u (light session: %s, memory reservation: %" PRIu64 " KB)",
            threadId,
            connectionId,
            MOTCurrentNumaNodeId,
            isLightSession ? "yes" : "no",
            reserveMemoryKb);
        initPhase = SETUP_NODE_ID_PHASE;

        // initialize index access for current session
        if (!InitMasstreeThreadinfo()) {
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL, "Create Session Context", "Failed to initializes masstree threadinfo");
            break;
        }
        initPhase = SETUP_MASSTREE_INFO_PHASE;

        // phase 2: reserve session memory (only if MEM_SESSION_ACTIVE is defined)
#ifdef MEM_SESSION_ACTIVE
        if (reserveMemoryKb == 0) {
            reserveMemoryKb = g_memGlobalCfg.m_minSessionMemoryKb;
        }
        int res = MemSessionReserve(reserveMemoryKb * KILO_BYTE);
        if (res != 0) {
            MOT_REPORT_ERROR(res,
                "Create Session Context",
                "Failed to reserve %" PRIu64 " KB for new session on connection %u",
                reserveMemoryKb,
                connectionId);
            break;
        }
        initPhase = RESERVE_SESSION_MEMORY_PHASE;
#endif

        // phase 3: allocate session context buffer (and create object)
#ifdef MEM_SESSION_ACTIVE
        void* inplaceBuffer = MemSessionAllocAligned(sizeof(SessionContext), L1_CACHE_LINE);
#else
        void* inplaceBuffer = nullptr;
        int res = posix_memalign(&inplaceBuffer, L1_CACHE_LINE, sizeof(SessionContext));
        if (res != 0) {
            MOT_REPORT_SYSTEM_ERROR_CODE(res,
                posix_memalign,
                "Create Session Context",
                "Failed to allocate session context buffer for new session on connection %u",
                connectionId);
            break;
        }
#endif
        if (unlikely(inplaceBuffer == nullptr)) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM,
                "Create Session Context",
                "Failed to allocate session context buffer for new session on connection %u",
                connectionId);
            break;
        }

        sessionContext = new (inplaceBuffer) SessionContext(sessionId, connectionId, userData, isMtls);
        MOT_LOG_TRACE("Created new session %u", sessionContext->GetSessionId());
        initPhase = ALLOC_SESSION_BUFFER_PHASE;

        // phase 4: initialize session
        if (!sessionContext->Init(isLightSession)) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM,
                "Create Session Context",
                "Failed to initialize new session %u",
                sessionContext->GetSessionId());
            break;
        }
        (void)m_sessionContextMap.insert(sessionContext->GetSessionId(), sessionContext);
        if (!isMtls) {
            // Add Garbage collection to active_list
            sessionContext->GetTxnManager()->GcAddSession();
        }
        initPhase = INIT_SESSION_PHASE;

        // phase 5: reserve statistics thread slot
        if (!StatisticsManager::GetInstance().ReserveThreadSlot()) {
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                "Create Session Context",
                "Failed to reserve statistics thread slot for new session %u",
                sessionContext->GetSessionId());
            break;
        }
        initPhase = RESERVE_THREAD_SLOT_PHASE;

        MOT_LOG_TRACE("Session %u for thread id %" PRIu16 " and connection %u created successfully",
            sessionContext->GetSessionId(),
            threadId,
            connectionId);
        initPhase = SESSION_INIT_DONE;
        MOT_SET_CURRENT_SESSION_CONTEXT(sessionContext);
    } while (0);

    // cleanup if initialization failed
    if (initPhase != SESSION_INIT_DONE) {
        // sessionContext will be freed and set to nullptr in CleanupFailedSessionContext
        CleanupFailedSessionContext(connectionId, sessionId, initPhase, sessionContext);
    }

    return sessionContext;
}

void SessionManager::DestroySessionContext(SessionContext* sessionContext)
{
    // reject request if a session was already deleted for the current thread
    SessionContext* result = MOT_GET_CURRENT_SESSION_CONTEXT();
    if (result == nullptr) {
        MOT_LOG_WARN("Ignoring request to delete session context: already deleted");
        return;
    } else if (result != sessionContext) {
        MOT_LOG_WARN(
            "Ignoring attempt to delete mismatching session context (attempting to delete from another thread?)");
        return;
    }
    bool isMtls = sessionContext->IsMtlsRecoverySession();
    MOT_LOG_DEBUG("deleteSessionContext(): Session %u for thread id %" PRIu16 ", connection %u: ref-cnt=%" PRIu64,
        sessionContext->GetSessionId(),
        MOTCurrThreadId,
        sessionContext->GetConnectionId(),
        sessionContext->GetRefCount());
    if (sessionContext->DecRefCount() == 0) {
        ConnectionId connectionId = sessionContext->GetConnectionId();
        SessionId sessionId = sessionContext->GetSessionId();
        if (!isMtls) {
            MOT_LOG_TRACE("Destroying session %u, connection %u with surrogate key %" PRIu64,
                sessionId,
                connectionId,
                sessionContext->GetTxnManager()->GetSurrogateCount());
        } else {
            MOT_LOG_TRACE("Destroying session %u, connection %u (mtls)", sessionId, connectionId);
        }

#ifdef MEM_SESSION_ACTIVE
        MemSessionPrintStats(connectionId, "Pre-Shutdown report", LogLevel::LL_TRACE);
#endif

        // NOTE:
        // We keep all thread-level attributes and resources initialized, since in a thread-pooled envelope other
        // sessions might still use this worker thread.
        // In particular, these attributes and resources are kept initialized:
        //      1. Statistics thread slot
        //      2. Table thread caches (object pools)
        //      3. Buffer allocator caches
        //      4. Current NUMA node identifier
        //      5. Current thread identifier
        if (!m_sessionContextMap.remove(sessionId)) {
            MOT_LOG_WARN("Failed to remove session %u from global session map - not found", sessionId);
        }
        if (!isMtls) {
            GetSurrogateKeyManager()->SetSurrogateSlot(
                connectionId, sessionContext->GetTxnManager()->GetSurrogateCount());
            // Remove from link list
            sessionContext->GetTxnManager()->GcRemoveSession();
        }
        sessionContext->~SessionContext();

#ifdef MEM_SESSION_ACTIVE
        MemSessionFree(sessionContext);
        // we must set the current session context point to null right now, otherwise the subsequent call to
        // MemSessionUnreserve() touches deallocated memory
        MOT_SET_CURRENT_SESSION_CONTEXT(nullptr);
        MemSessionUnreserve();
#else
        free(sessionContext);
        MOT_SET_CURRENT_SESSION_CONTEXT(nullptr);
#endif

        MOT_LOG_DEBUG(
            "Session %u for thread id %" PRIu16 ", connection %u destroyed", sessionId, MOTCurrThreadId, connectionId);

        FreeConnectionId(connectionId);
        MOT_ASSERT(connectionId == MOT_GET_CURRENT_CONNECTION_ID());
        MOT_ASSERT(sessionId == MOT_GET_CURRENT_SESSION_ID());
        MOT_SET_CURRENT_CONNECTION_ID(INVALID_CONNECTION_ID);
        MOT_SET_CURRENT_SESSION_ID(INVALID_SESSION_ID);
    }
}

void SessionManager::ReportActiveSessions()
{
    if (!m_sessionContextMap.empty()) {
        MOT_LOG_WARN("Attempting to Destroy MOT Engine while there are still %u active sessions",
            (unsigned)m_sessionContextMap.size());
        m_sessionContextMap.for_each(PrintActiveSession);
    }
}

ScopedSessionManager::ScopedSessionManager()
{
    // initialize thread members
    knl_thread_mot_init();

    // initialize session members
    errno_t erc =
        memset_s(static_cast<void*>(&m_localUSess), sizeof(knl_session_context), 0, sizeof(knl_session_context));
    securec_check(erc, "\0", "\0");
    knl_u_mot_init(&m_localUSess.mot_cxt);

    // setup the session context
    u_sess = &m_localUSess;
}

ScopedSessionManager::~ScopedSessionManager()
{
    // clear the session context
    u_sess = nullptr;
}
}  // namespace MOT
