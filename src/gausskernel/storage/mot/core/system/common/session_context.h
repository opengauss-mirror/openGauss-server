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
 * session_context.h
 *    A session context stands for all resources related to managing a user connection.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/common/session_context.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef SESSION_CONTEXT_H
#define SESSION_CONTEXT_H

#include <atomic>
#include "mm_def.h"
#include "thread_id.h"
#include "connection_id.h"

#include "postgres.h"
#include "knl/knl_session.h"

// forward declaration (global scope)
class threadinfo;

namespace MOT {
// forward declaration
class MOTEngine;
class TxnManager;

/** @typedef session identifier. */
typedef uint32_t SessionId;

/** @define Invalid thread identifier. */
#define INVALID_SESSION_ID ((MOT::SessionId)-1)

#define MOTCurrentNumaNodeId t_thrd.mot_cxt.currentNumaNodeId

/**
 * @class SessionContext
 * @details Maintains session-level resources.
 */
class __attribute__((packed)) SessionContext {
private:
    /**
     * @brief Constructor.
     * @param sessionId The unique session identifier.
     * @param sessionId The reusable connection identifier.
     * @param userData Session-level envelope data.
     * @param isMtls Whether it is an MTLS thread's session context.
     */
    SessionContext(SessionId sessionId, ConnectionId connectionId, void* userData, bool isMtls = false);

    /** @brief Destructor. */
    ~SessionContext();

    // only SessionManager can create an instance of this class.
    friend class SessionManager;

public:
    /**
     * @brief Initializes the session object.
     * @param isLightSession Specifies whether this is a light session.
     * @return True if initialization succeeded, otherwise false.
     */
    bool Init(bool isLightSession);

    /**
     * @brief Allocates a unique session identifier.
     * @return The session identifier.
     */
    static inline SessionId AllocSessionId()
    {
        return m_nextSessionId.fetch_add(1);
    }

    /** @brief Retrieves the current reference count of this session (used for multiple initialization). */
    inline uint64_t GetRefCount() const
    {
        return m_refCount;
    }

    /** @brief Increments the reference count of this session. */
    inline void IncRefCount()
    {
        ++m_refCount;
    }

    /** @brief Decrements the reference count of this session. */
    inline uint64_t DecRefCount()
    {
        return --m_refCount;
    }

    /**
     * @brief Retrieves the unique session identifier.
     * @return The session identifier.
     */
    inline SessionId GetSessionId() const
    {
        return m_sessionId;
    }

    /**
     * @brief Retrieves the connection identifier of the session.
     * @return The connection identifier.
     */
    inline ConnectionId GetConnectionId() const
    {
        return m_connectionId;
    }

    /**
     * @brief Retrieves the transaction object associated with the session.
     * @return The transaction.
     */
    inline TxnManager* GetTxnManager()
    {
        return m_txn;
    }

    static void SetTxnContext(TxnManager* txn);

    inline bool IsMtlsRecoverySession() const
    {
        return (bool)m_isMtls;
    }

private:
    /** @brief Creates a transaction object. */
    static TxnManager* CreateTransaction(
        SessionContext* sessionContext, MOTThreadId threadId, ConnectionId connectionId, bool isLightTxn);

    /** @var The next session identifier. */
    static std::atomic<SessionId> m_nextSessionId;

    /** @var The unique session id. */
    SessionId m_sessionId;  // L1 offset 0-4

    /** @var Align next member to 8 byte offset. */
    uint8_t m_padding1[4];  // L1 offset 4-8

    /** @var The connection id. */
    ConnectionId m_connectionId;  // L1 offset 8-12

    /** @var Align next member to 8 byte offset. */
    uint8_t m_padding2[4];  // L1 offset 12-16

    /** @var Reference count for number of times session was initialized. */
    uint64_t m_refCount;  // L1 offset 16-24

    /** @var The (reusable) transaction object associated with the session. */
    TxnManager* m_txn;  // L1 offset 24-32

    /** @var parallel recovery thread indication. */
    uint8_t m_isMtls;  // L1 offset 32-33

    /** @var Align class size to L1 cache line. */
    uint8_t m_padding3[31];  // L1 offset 33-64
};

/** @define Thread-local quick access to current session identifier. */
#define MOT_GET_CURRENT_SESSION_ID() (u_sess ? u_sess->mot_cxt.session_id : INVALID_SESSION_ID)
#define MOT_SET_CURRENT_SESSION_ID(sessionId) u_sess->mot_cxt.session_id = sessionId;

/** @define Thread-local quick access to current connection identifier. */
#define MOT_GET_CURRENT_CONNECTION_ID() (u_sess ? u_sess->mot_cxt.connection_id : INVALID_CONNECTION_ID)
#define MOT_SET_CURRENT_CONNECTION_ID(connectionId) u_sess->mot_cxt.connection_id = connectionId;

/** @define Thread-local quick access to current session context. */
#define MOT_GET_CURRENT_SESSION_CONTEXT() (u_sess ? u_sess->mot_cxt.session_context : nullptr)
#define MOT_SET_CURRENT_SESSION_CONTEXT(sessionContext) u_sess->mot_cxt.session_context = sessionContext;

/** @brief Initializes the current NUMA node identifier TLS. */
extern bool InitCurrentNumaNodeId();

/** @brief Clears the current NUMA node identifier TLS. */
extern void ClearCurrentNumaNodeId();

extern bool InitMasstreeThreadinfo();
extern void DestroyMasstreeThreadinfo();
}  // namespace MOT

#endif /* SESSION_CONTEXT_H */
