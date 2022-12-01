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
 * connection_id.h
 *    Encapsulates the logic of a reusable connection id.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/common/connection_id.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef CONNECTION_ID_H
#define CONNECTION_ID_H

#include <cstdint>

namespace MOT {
// In Thread-Pooled environments we need a reusable connection id (not a running number like
// SessionContext::m_sessionId), so that we can have an anchor for session allocators and
// other scenarios. Previously the current thread identifier was the anchor, but when a thread-pool
// exists this no longer holds true.
/**
 * @typedef Connection identifier type.
 */
typedef uint32_t ConnectionId;

/** @define Invalid connection identifier. */
#define INVALID_CONNECTION_ID ((MOT::ConnectionId)-1)

/**
 * @brief Initializes the pool of reusable connection identifiers.
 * @param maxConnections The maximum number of connections in the system.
 * @return Zero if initialization succeeded, otherwise an error code.
 */
extern int InitConnectionIdPool(uint32_t maxConnections);

/**
 * @brief Releases all resources associated with the pool of reusable connection identifiers.
 */
extern void DestroyConnectionIdPool();

/**
 * @brief Allocates a reusable connection identifier for the current connection.
 * @return A valid connection id, or @ref INVALID_CONNECTION_ID if failed.
 */
extern ConnectionId AllocConnectionId();

/**
 * @brief Allocates the highest available reusable connection identifier for the current connection.
 * @return A valid connection id, or @ref INVALID_CONNECTION_ID if failed.
 */
extern ConnectionId AllocConnectionIdHighest();

/**
 * @brief Frees a reusable connection identifier for the current session. This identifier can be
 * reused by another connection. When a session finishes its execution it must call this function,
 * so its connection identifier can be returned to the pool of reusable connection identifiers.
 * @param connectionId The connection identifier to be returned to the pool.
 */
extern void FreeConnectionId(ConnectionId connectionId);

/**
 * @brief Retrieves the maximum number of connections allowed in the system.
 */
extern uint32_t GetMaxConnectionCount();

/**
 * @brief Retrieves the current number of allocated connections.
 */
extern uint32_t GetCurrentConnectionCount();

// Testing API - This API exists only for testing and should not be used by the engine
extern bool IsConnectionIdFree(ConnectionId connectionId);
extern void DumpConnectionIds(const char* reportName);
}  // namespace MOT

#endif /* CONNECTION_ID_H */
