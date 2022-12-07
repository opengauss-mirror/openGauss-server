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
 * session_manager.h
 *    Manages all the sessions.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/common/session_manager.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef SESSION_MANAGER_H
#define SESSION_MANAGER_H

#include "session_context.h"
#include "concurrent_map.h"
#include "mm_gc_manager.h"
#include "txn.h"

namespace MOT {
class SessionManager {
public:
    SessionManager() : m_nodeCount(0), m_threadCount(0)
    {}

    ~SessionManager()
    {}

    /**
     * @brief Initializes the session manager.
     * @param nodeCount The number of NUMA nodes used.
     * @param threadCount The maximum number of thread used.
     * @return True if initialization succeeded, otherwise false.
     */
    bool Initialize(uint32_t nodeCount, uint32_t threadCount);

    /** @brief Destroys the session manager. */
    void Destroy();

    /**
     * @brief Creates a new session context.
     * @detail A session context stands for all resources related to managing a user connection.
     * This includes memory management, as well as the currently executing transaction if any.
     * This method should be called whenever a user connection is established.
     * @param[opt] isLightSession Used in coordinator. Implies lightweight session handling.
     * @param[opt] reserveMemoryKb Specify a memory reservation for the session in kilo bytes.
     * Pass zero to use default session reservation (see workMemoryMaxLimitKB in mot_configuration.h).
     * @param[opt] userData User data associated with the session.
     * @param[opt] is this an MTLS recovery session.
     * @return The new session context.
     * @see @ref workMemoryMaxLimitKB)
     */
    SessionContext* CreateSessionContext(bool isLightSession = false, uint64_t reserveMemoryKb = 0,
        void* userData = nullptr, ConnectionId connectionId = INVALID_CONNECTION_ID, bool isMtls = false);

    /**
     * @brief Destroys a session context.
     * @detail This method should be called whenever a user connection is closed.
     * @param sessionContext The session context to delete.
     */
    void DestroySessionContext(SessionContext* sessionContext);

    /**
     * @brief Finds a session context by its identifier.
     * @param The session identifier.
     * @return The session or null pointer if none was found.
     */
    inline SessionContext* GetSessionContext(SessionId sessionId)
    {
        SessionContext* result = nullptr;
        if (!m_sessionContextMap.get(sessionId, &result)) {
            result = nullptr;  // just in-case
        }
        return result;
    }

    /**
     * @brief Retrieves the current session context.
     * @return The session or null pointer if none was defined for the current thread.
     */
    inline SessionContext* GetCurrentSessionContext() const
    {
        return MOT_GET_CURRENT_SESSION_CONTEXT();
    }

    inline GcManager* GetCurrentGcSession()
    {
        GcManager* result = nullptr;
        SessionContext* sessionContext = MOT_GET_CURRENT_SESSION_CONTEXT();
        if (sessionContext != nullptr && !sessionContext->IsMtlsRecoverySession()) {
            result = sessionContext->GetTxnManager()->GetGcSession();
        } else {
            if (u_sess->mot_cxt.txn_manager != nullptr) {
                result = u_sess->mot_cxt.txn_manager->GetGcSession();
            }
        }
        return result;
    }

    void ReportActiveSessions();

private:
    // session initialization phases
    enum SessionInitPhase {
        SESSION_INIT_START,
        ALLOC_THREAD_ID_PHASE,
        ALLOC_CONNECTION_ID_PHASE,
        ALLOC_SESSION_ID_PHASE,
        SETUP_NODE_ID_PHASE,
        SETUP_MASSTREE_INFO_PHASE,
        RESERVE_SESSION_MEMORY_PHASE,
        ALLOC_SESSION_BUFFER_PHASE,
        INIT_SESSION_PHASE,
        RESERVE_THREAD_SLOT_PHASE,
        SESSION_INIT_DONE
    };

    /**
     * @brief Helper to clean up if session initialization is failed.
     * @param connectionId Connection ID.
     * @param sessionId Session ID.
     * @param initPhase Initialization phase.
     * @param[in,out] sessionContext Session context, freed and set to nullptr if it was allocated.
     */
    void CleanupFailedSessionContext(
        ConnectionId connectionId, SessionId sessionId, SessionInitPhase initPhase, SessionContext*& sessionContext);

    /** @typedef session map */
    typedef ConcurrentMap<SessionId, SessionContext*> SessionContextMap;

    /** @var Session context map. */
    SessionContextMap m_sessionContextMap;

    /** @var The number of NUMA nodes used for managing per-node session buffers. */
    uint32_t m_nodeCount;

    /** @var The number of threads used for managing per-node session buffers. */
    uint32_t m_threadCount;
};

class ScopedSessionManager {
private:
    knl_session_context m_localUSess;

public:
    ScopedSessionManager();
    ~ScopedSessionManager();
};

/**
 * @define MOT_DECLARE_NON_KERNEL_THREAD Each non-kernel thread that wishes to use a session context, must declare this
 * macro at the beginning of the thread function before creating a session context.
 */
#define MOT_DECLARE_NON_KERNEL_THREAD() ScopedSessionManager scopedSessionManager;
}  // namespace MOT

#endif /* SESSION_MANAGER_H */
