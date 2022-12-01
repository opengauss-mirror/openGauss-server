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
 * connection_id.cpp
 *    Encapsulates the logic of a reusable connection id.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/common/connection_id.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "connection_id.h"
#include "utilities.h"
#include "mot_error.h"
#include "mot_atomic_ops.h"

namespace MOT {
DECLARE_LOGGER(ConnectionIdPool, System)

#define CONNECTIONS_PER_WORD ((uint32_t)64)

static uint32_t maxConnectionCount = 0;
static volatile uint32_t currentConnectionCount = 0;
static uint64_t* connectionIdBitsetArray = nullptr;
static uint32_t connectionIdArraySize = 0;
static pthread_spinlock_t connectionIdLock;

static enum ConnectionIdInitPhase { INIT, LOCK_INIT, ARRAY_ALLOC, DONE } initPhase = INIT;

// the bit index is counted from MSB[0] to LSB[63]!
#define CONNECTION_ID_BIT_AT(bitIndex) (((uint64_t)1) << ((CONNECTIONS_PER_WORD - (bitIndex)) - 1))
#define IS_CONNECTION_ID_BIT_SET(arrayIndex, bitIndex) \
    (connectionIdBitsetArray[arrayIndex] & CONNECTION_ID_BIT_AT(bitIndex))
#define RAISE_CONNECTION_ID_BIT(arrayIndex, bitIndex) \
    (connectionIdBitsetArray[arrayIndex] |= CONNECTION_ID_BIT_AT(bitIndex))
#define CLEAR_CONNECTION_ID_BIT(arrayIndex, bitIndex) \
    (connectionIdBitsetArray[arrayIndex] &= ~CONNECTION_ID_BIT_AT(bitIndex))

// pay attention: the bit set is inverted - "0" means occupied and "1" means free for usage
#define MARK_CONNECTION_ID_USED(arrayIndex, bitIndex) CLEAR_CONNECTION_ID_BIT(arrayIndex, bitIndex)
#define MARK_CONNECTION_ID_FREE(arrayIndex, bitIndex) RAISE_CONNECTION_ID_BIT(arrayIndex, bitIndex)
#define IS_CONNECTION_ID_FREE(arrayIndex, bitIndex) IS_CONNECTION_ID_BIT_SET(arrayIndex, bitIndex)
#define IS_CONNECTION_ID_USED(arrayIndex, bitIndex) (!IS_CONNECTION_ID_FREE(arrayIndex, bitIndex))

// Initialization helper macro (for system calls)
#define CHECK_SYS_INIT_STATUS(rc, syscall, format, ...)                                                        \
    if (rc != 0) {                                                                                             \
        MOT_REPORT_SYSTEM_PANIC_CODE(rc, syscall, "Connection Id Pool Initialization", format, ##__VA_ARGS__); \
        result = MOT_ERROR_SYSTEM_FAILURE;                                                                     \
        break;                                                                                                 \
    }

// Initialization helper macro (for sub-service initialization)
#define CHECK_INIT_STATUS(result, format, ...)                                                \
    if (result != 0) {                                                                        \
        MOT_REPORT_PANIC(result, "Connection Id Pool Initialization", format, ##__VA_ARGS__); \
        break;                                                                                \
    }

static int AllocConnectionIdArray(uint32_t maxConnections)
{
    int result = 0;

    MOT_LOG_TRACE("Initializing connection identifier pool with a maximum of %u connections", maxConnections);
    maxConnectionCount = maxConnections;
    connectionIdArraySize = (maxConnectionCount + CONNECTIONS_PER_WORD - 1) / CONNECTIONS_PER_WORD;
    size_t allocSize = sizeof(uint64_t) * connectionIdArraySize;
    connectionIdBitsetArray = (uint64_t*)malloc(allocSize);
    if (connectionIdBitsetArray == nullptr) {
        MOT_REPORT_PANIC(MOT_ERROR_OOM,
            "Connection Id Pool Initialization",
            "Failed to allocate %u bytes",
            sizeof(uint64_t) * connectionIdArraySize);
        result = MOT_ERROR_OOM;
    } else {
        // pay attention: the bit-set is inverted: "1" bit means free, "0" bit means allocated
        errno_t erc = memset_s(connectionIdBitsetArray, allocSize, 0xFF, allocSize);
        securec_check(erc, "\0", "\0");
    }

    return result;
}

extern int InitConnectionIdPool(uint32_t maxConnections)
{
    int result = 0;

    MOT_LOG_TRACE("Initializing connection identifier pool with %u connection identifiers", maxConnections);
    do {  // instead of goto
        int rc = pthread_spin_init(&connectionIdLock, 0);
        if (rc != 0) {
            MOT_REPORT_SYSTEM_PANIC_CODE(rc,
                pthread_spin_init,
                "Connection Id Pool Initialization",
                "Failed to initialize reusable connection-id pool spin-lock");
            result = MOT_ERROR_SYSTEM_FAILURE;
            break;
        }
        initPhase = LOCK_INIT;

        result = AllocConnectionIdArray(maxConnections);
        if (result != 0) {
            MOT_REPORT_PANIC(result, "Connection Id Pool Initialization", "Failed to allocate connection id array");
        } else {
            initPhase = DONE;
        }
    } while (0);

    if (result != 0) {
        MOT_LOG_PANIC("Connection Id pool initialization failed, see errors above");
    }

    return result;
}

extern void DestroyConnectionIdPool()
{
    MOT_LOG_TRACE("Destroying connection identifier pool");
    if (connectionIdBitsetArray != nullptr) {
        free(connectionIdBitsetArray);
        connectionIdBitsetArray = nullptr;
    }
    switch (initPhase) {
        case DONE:
        case ARRAY_ALLOC:
            free(connectionIdBitsetArray);
            // fall through
        case LOCK_INIT:
            (void)pthread_spin_destroy(&connectionIdLock);
            // fall through
        default:
            break;
    }
    initPhase = INIT;
}

extern ConnectionId AllocConnectionId()
{
    ConnectionId result = INVALID_CONNECTION_ID;

    (void)pthread_spin_lock(&connectionIdLock);
    for (uint32_t arrayIndex = 0; arrayIndex < connectionIdArraySize; ++arrayIndex) {
        if (connectionIdBitsetArray[arrayIndex] != 0) {
            uint32_t bitIndex = __builtin_clzll(connectionIdBitsetArray[arrayIndex]);
            uint32_t connectionId = CONNECTIONS_PER_WORD * arrayIndex + bitIndex;
            if (connectionId < maxConnectionCount) {
                result = connectionId;
                MOT_ASSERT(IS_CONNECTION_ID_FREE(arrayIndex, bitIndex));
                MARK_CONNECTION_ID_USED(arrayIndex, bitIndex);
                MOT_LOG_TRACE("Allocated internal connection id %u", connectionId);
            } else {
                MOT_LOG_TRACE("Disregarding free connection id %u since maximum connection count is %u",
                    connectionId,
                    maxConnectionCount);
            }
            break;
        }
    }
    (void)pthread_spin_unlock(&connectionIdLock);

    if (result == INVALID_CONNECTION_ID) {
        MOT_REPORT_ERROR(MOT_ERROR_RESOURCE_LIMIT,
            "N/A",
            "Failed to allocate connection identifier: all connection identifiers are in use.");
    } else {
        MOT_ATOMIC_INC(currentConnectionCount);
    }

    return result;
}

extern ConnectionId AllocConnectionIdHighest()
{
    ConnectionId result = INVALID_CONNECTION_ID;

    (void)pthread_spin_lock(&connectionIdLock);
    // since bit-set computation is tedious in case maxThreadCount is not a full multiple of 64, we do it in the
    // opposite way
    for (uint32_t i = 0; i < maxConnectionCount; ++i) {
        ConnectionId connectionId = (maxConnectionCount - i) - 1;
        uint32_t arrayIndex = connectionId / CONNECTIONS_PER_WORD;
        uint32_t bitIndex = connectionId % CONNECTIONS_PER_WORD;
        if (IS_CONNECTION_ID_FREE(arrayIndex, bitIndex)) {
            MARK_CONNECTION_ID_USED(arrayIndex, bitIndex);
            result = connectionId;
            break;
        }
    }
    (void)pthread_spin_unlock(&connectionIdLock);

    if (result == INVALID_CONNECTION_ID) {
        MOT_REPORT_ERROR(MOT_ERROR_RESOURCE_LIMIT,
            "N/A",
            "Failed to allocate highest connection identifier: all connection identifiers are in use.");
    } else {
        MOT_ATOMIC_INC(currentConnectionCount);
    }

    return result;
}

extern void FreeConnectionId(ConnectionId connectionId)
{
    uint32_t arrayIndex = connectionId / CONNECTIONS_PER_WORD;
    uint32_t bitIndex = connectionId % CONNECTIONS_PER_WORD;

    (void)pthread_spin_lock(&connectionIdLock);
    MOT_ASSERT(IS_CONNECTION_ID_USED(arrayIndex, bitIndex));
    MARK_CONNECTION_ID_FREE(arrayIndex, bitIndex);
    MOT_ATOMIC_DEC(currentConnectionCount);
    (void)pthread_spin_unlock(&connectionIdLock);
    MOT_LOG_TRACE("De-allocated internal connection id %u", connectionId);
}

extern uint32_t GetMaxConnectionCount()
{
    return maxConnectionCount;
}

extern uint32_t GetCurrentConnectionCount()
{
    return MOT_ATOMIC_LOAD(currentConnectionCount);
}

// Testing API - This API exists only for testing and should not be used by the engine
extern bool IsConnectionIdFree(ConnectionId connectionId)
{
    bool result = false;

    uint32_t arrayIndex = connectionId / CONNECTIONS_PER_WORD;
    uint32_t bitIndex = connectionId % CONNECTIONS_PER_WORD;

    // pay attention: the bit-set is inverted: "1" bit means free, "0" bit means allocated
    if (IS_CONNECTION_ID_FREE(arrayIndex, bitIndex)) {
        result = true;
    }

    return result;
}

extern void DumpConnectionIds(const char* reportName)
{
    MOT_LOG_INFO("Allocated connection id report: %s", reportName);
    for (uint32_t i = 0; i < connectionIdArraySize; ++i) {
        for (uint32_t j = 0; j < CONNECTIONS_PER_WORD; ++j) {
            // pay attention: the bit-set is inverted: "1" bit means free, "0" bit means allocated
            ConnectionId connectionId = i * CONNECTIONS_PER_WORD + j;
            if (connectionId < maxConnectionCount) {
                if (IS_CONNECTION_ID_USED(i, j)) {
                    MOT_LOG_INFO("connection id: %" PRIu32, connectionId);
                }
            } else {
                break;
            }
        }
    }
    MOT_LOG_INFO("End of allocated connection id report: %s", reportName);
}
}  // namespace MOT
