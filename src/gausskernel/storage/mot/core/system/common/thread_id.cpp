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
 * thread_id.cpp
 *    Encapsulates the logic of a reusable thread id.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/common/thread_id.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "thread_id.h"
#include "utilities.h"
#include "mot_error.h"
#include "mot_configuration.h"
#include "mot_engine.h"
#include "sys_numa_api.h"
#include "mot_atomic_ops.h"

#include <pthread.h>
#include <cstring>

namespace MOT {
DECLARE_LOGGER(ThreadIdPool, System)

// how much thread identifiers fit in one word
#define THREADS_PER_WORD ((uint16_t)64)

static uint16_t maxThreadCount = 0;
static volatile uint16_t currentThreadCount = 0;
static uint64_t* threadIdBitsetArray = nullptr;
static uint16_t threadIdArraySize = 0;
static pthread_spinlock_t threadIdLock;
static pthread_key_t threadIdCleanupKey;
static MOTThreadId numaOrdinalThreadMap[MAX_THREAD_COUNT];

static enum ThreadIdInitPhase { INIT, LOCK_INIT, KEY_CREATE, ARRAY_ALLOC, DONE } initPhase = INIT;

// the bit index is counted from MSB[0] to LSB[63]!
#define THREAD_ID_BIT_AT(bitIndex) (((uint64_t)1) << ((THREADS_PER_WORD - (bitIndex)) - 1))
#define IS_THREAD_ID_BIT_SET(arrayIndex, bitIndex) (threadIdBitsetArray[arrayIndex] & THREAD_ID_BIT_AT(bitIndex))
#define RAISE_THREAD_ID_BIT(arrayIndex, bitIndex) (threadIdBitsetArray[arrayIndex] |= THREAD_ID_BIT_AT(bitIndex))
#define CLEAR_THREAD_ID_BIT(arrayIndex, bitIndex) (threadIdBitsetArray[arrayIndex] &= ~THREAD_ID_BIT_AT(bitIndex))

// pay attention: the bit set is inverted - "0" means occupied and "1" means free for usage
#define MARK_THREAD_ID_USED(arrayIndex, bitIndex) CLEAR_THREAD_ID_BIT(arrayIndex, bitIndex)
#define MARK_THREAD_ID_FREE(arrayIndex, bitIndex) RAISE_THREAD_ID_BIT(arrayIndex, bitIndex)
#define IS_THREAD_ID_FREE(arrayIndex, bitIndex) IS_THREAD_ID_BIT_SET(arrayIndex, bitIndex)
#define IS_THREAD_ID_USED(arrayIndex, bitIndex) (!IS_THREAD_ID_FREE(arrayIndex, bitIndex))

// Initialization helper macro (for system calls)
#define CHECK_SYS_INIT_STATUS(rc, syscall, format, ...)                                                    \
    if (rc != 0) {                                                                                         \
        MOT_REPORT_SYSTEM_PANIC_CODE(rc, syscall, "Thread Id Pool Initialization", format, ##__VA_ARGS__); \
        result = MOT_ERROR_SYSTEM_FAILURE;                                                                 \
        break;                                                                                             \
    }

// Initialization helper macro (for sub-service initialization)
#define CHECK_INIT_STATUS(result, format, ...)                                            \
    if (result != 0) {                                                                    \
        MOT_REPORT_PANIC(result, "Thread Id Pool Initialization", format, ##__VA_ARGS__); \
        break;                                                                            \
    }

static void FreeThreadIdInternal(MOTThreadId threadId);
static int InitNumaOrdinalThreadMap();

static void CleanupThreadId(void* key)
{
    // in order to ensure thread-id cleanup for thread 0 we use positive values
    MOTThreadId threadId = ((MOTThreadId)(uint64_t)key) - 1;
    MOT_LOG_DEBUG("CleanupThreadId() invoked with %" PRIu64, (uint64_t)threadId);
    if (threadId != INVALID_THREAD_ID) {
        MOT_LOG_DEBUG(
            "Cleaning up thread identifier %" PRIu16 " after thread ended (missing call to FreeThreadId()?)", threadId);
        FreeThreadIdInternal(threadId);
    }
}

static void ScheduleThreadIdCleanup()
{
    // install auto-cleanup for current thread in case a call to FreeThreadId() is missing
    // pay attention we add 1 to thread id because pthread key destructor is called only if key for
    // the current thread is non-zero, and we want to recycle also thread id zero
    (void)pthread_setspecific(threadIdCleanupKey, (const void*)(uint64_t)(MOTCurrThreadId + 1));
}

static void SetCurrentThreadId(MOTThreadId threadId)
{
    MOTCurrThreadId = threadId;

    uint16_t arrayIndex = threadId / THREADS_PER_WORD;
    uint16_t bitIndex = threadId % THREADS_PER_WORD;

    // pay attention: the bit-set is inverted: "1" bit means free, "0" bit means allocated
    MOT_ASSERT(IS_THREAD_ID_BIT_SET(arrayIndex, bitIndex));
    MARK_THREAD_ID_USED(arrayIndex, bitIndex);
    MOT_LOG_TRACE("Allocated internal thread id %" PRIu16, MOTCurrThreadId);
}

static int AllocThreadIdArray(uint16_t maxThreads)
{
    int result = 0;

    MOT_LOG_TRACE("Initializing thread identifier pool with a maximum of %" PRIu16 " threads", maxThreads);
    maxThreadCount = maxThreads;
    threadIdArraySize = (maxThreadCount + THREADS_PER_WORD - 1) / THREADS_PER_WORD;  // align to multiple of 64
    uint32_t allocSize = sizeof(uint64_t) * threadIdArraySize;
    threadIdBitsetArray = (uint64_t*)malloc(allocSize);
    if (threadIdBitsetArray == nullptr) {
        MOT_REPORT_PANIC(MOT_ERROR_OOM,
            "Thread Id Pool Initialization",
            "Failed to allocate %u bytes",
            sizeof(uint64_t) * threadIdArraySize);
        result = MOT_ERROR_OOM;
    } else {
        // pay attention: the bit-set is inverted: "1" bit means free, "0" bit means allocated
        errno_t erc = memset_s(threadIdBitsetArray, allocSize, 0xFF, allocSize);
        securec_check(erc, "\0", "\0");
    }

    return result;
}

extern int InitThreadIdPool(uint16_t maxThreadCountCfg)
{
    int result = 0;

    if (maxThreadCountCfg > MAX_THREAD_COUNT) {
        MOT_REPORT_PANIC(MOT_ERROR_INVALID_CFG,
            "Thread Id Pool Initialization",
            "Invalid maximum thread count configuration: Maximum thread count %" PRIu16 " cannot exceed %" PRIu16,
            maxThreadCountCfg,
            MAX_THREAD_COUNT);
        result = MOT_ERROR_INVALID_CFG;
    } else {
        do {  // instead of goto
            int rc = pthread_spin_init(&threadIdLock, 0);
            CHECK_SYS_INIT_STATUS(rc, pthread_spin_init, "Failed to initialize reusable thread-id pool spin-lock");
            initPhase = LOCK_INIT;

            rc = pthread_key_create(&threadIdCleanupKey, CleanupThreadId);
            CHECK_SYS_INIT_STATUS(
                rc, pthread_key_create, "Failed to create thread-specific key (thread-id auto-cleanup on thread exit)");
            initPhase = KEY_CREATE;

            result = AllocThreadIdArray(maxThreadCountCfg);
            CHECK_INIT_STATUS(result, "Failed to allocate thread id array");
            initPhase = ARRAY_ALLOC;

            // initialize the NUMA ordinal thread map
            result = InitNumaOrdinalThreadMap();
            CHECK_INIT_STATUS(result, "Failed to initialize NUMA ordinal thread map");
            initPhase = DONE;
        } while (0);
    }

    if (result != 0) {
        MOT_LOG_PANIC("Thread Id pool initialization failed, see errors above");
    }

    return result;
}

extern void DestroyThreadIdPool()
{
    switch (initPhase) {
        case DONE:
        case ARRAY_ALLOC:
            free(threadIdBitsetArray);

        // fall through
        case KEY_CREATE:
            (void)pthread_key_delete(threadIdCleanupKey);

        // fall through
        case LOCK_INIT:
            (void)pthread_spin_destroy(&threadIdLock);

        // fall through
        default:
            break;
    }
    initPhase = INIT;
}

extern MOTThreadId AllocThreadId()
{
    // guard against accidental double allocation
    if (MOTCurrThreadId != INVALID_THREAD_ID) {
        MOT_LOG_TRACE("Double attempt to allocate thread identifier for thread %" PRIu16 " declined", MOTCurrThreadId);
    } else {
        (void)pthread_spin_lock(&threadIdLock);
        for (uint16_t arrayIndex = 0; arrayIndex < threadIdArraySize; ++arrayIndex) {
            if (threadIdBitsetArray[arrayIndex] != 0) {
                uint16_t bitIndex = __builtin_clzll(threadIdBitsetArray[arrayIndex]);
                uint16_t threadId = THREADS_PER_WORD * arrayIndex + bitIndex;
                if (threadId < maxThreadCount) {
                    SetCurrentThreadId(threadId);
                }
                break;
            }
        }
        (void)pthread_spin_unlock(&threadIdLock);

        if (MOTCurrThreadId == INVALID_THREAD_ID) {
            MOT_REPORT_ERROR(MOT_ERROR_RESOURCE_LIMIT,
                "N/A",
                "Failed to allocate thread identifier: all thread identifiers are in use.");
        } else {
            MOT_ATOMIC_INC(currentThreadCount);
            ScheduleThreadIdCleanup();
        }
    }
    return MOTCurrThreadId;
}

extern MOTThreadId AllocThreadIdHighest()
{
    // guard against accidental double allocation
    if (MOTCurrThreadId != INVALID_THREAD_ID) {
        MOT_LOG_TRACE("Double attempt to allocate thread identifier for thread %" PRIu16 " declined", MOTCurrThreadId);
    } else {
        (void)pthread_spin_lock(&threadIdLock);
        // since computation is tedious in case maxThreadCount is not a full multiple of 64, we do it in the opposite
        // way
        for (uint16_t i = 0; i < maxThreadCount; ++i) {
            MOTThreadId threadId = (maxThreadCount - i) - 1;
            uint16_t arrayIndex = threadId / THREADS_PER_WORD;
            uint16_t bitIndex = threadId % THREADS_PER_WORD;
            if (IS_THREAD_ID_FREE(arrayIndex, bitIndex)) {
                SetCurrentThreadId(threadId);
                break;
            }
        }
        (void)pthread_spin_unlock(&threadIdLock);

        if (MOTCurrThreadId == INVALID_THREAD_ID) {
            MOT_REPORT_ERROR(MOT_ERROR_RESOURCE_LIMIT,
                "N/A",
                "Failed to allocate thread identifier: all thread identifiers are in use.");
        } else {
            MOT_ATOMIC_INC(currentThreadCount);
            ScheduleThreadIdCleanup();
        }
    }
    return MOTCurrThreadId;
}

extern MOTThreadId AllocThreadIdNumaHighest(int nodeId)
{
    // guard against accidental double allocation
    if (MOTCurrThreadId != INVALID_THREAD_ID) {
        MOT_LOG_TRACE("Double attempt to allocate thread identifier for thread %" PRIu16 " declined", MOTCurrThreadId);
    } else {
        Affinity& affinity = GetTaskAffinity();
        (void)pthread_spin_lock(&threadIdLock);
        // since computation is tedious in case maxThreadCount is not a full multiple of 64, we do it in the opposite
        // way
        for (uint16_t i = 0; i < maxThreadCount; ++i) {
            MOTThreadId threadId = (maxThreadCount - i) - 1;
            uint16_t arrayIndex = threadId / THREADS_PER_WORD;
            uint16_t bitIndex = threadId % THREADS_PER_WORD;
            if (IS_THREAD_ID_FREE(arrayIndex, bitIndex)) {
                int node = affinity.GetAffineNuma(threadId);
                if (node == nodeId) {
                    // found a slot that matches required node
                    SetCurrentThreadId(threadId);
                    break;
                }
            }
        }
        (void)pthread_spin_unlock(&threadIdLock);

        if (MOTCurrThreadId == INVALID_THREAD_ID) {
            MOT_REPORT_ERROR(MOT_ERROR_RESOURCE_LIMIT,
                "N/A",
                "Failed to allocate highest thread identifier on node %d: all thread identifiers for this node are in "
                "use.",
                nodeId);
        } else {
            MOT_ATOMIC_INC(currentThreadCount);
            ScheduleThreadIdCleanup();
        }
    }
    return MOTCurrThreadId;
}

extern MOTThreadId AllocThreadIdNumaCurrentHighest()
{
    int nodeId = 0;  // We default to Node 0 to avoid failures in other places in the code.
    if (GetGlobalConfiguration().m_enableNuma) {
        int cpu = sched_getcpu();
        nodeId = MotSysNumaGetNode(cpu);
    }
    return AllocThreadIdNumaHighest(nodeId);
}

extern MOTThreadId ReserveThreadId()
{
    MOTThreadId result = INVALID_THREAD_ID;

    (void)pthread_spin_lock(&threadIdLock);
    for (uint16_t arrayIndex = 0; arrayIndex < threadIdArraySize; ++arrayIndex) {
        if (threadIdBitsetArray[arrayIndex] != 0) {
            uint16_t bitIndex = __builtin_clzll(threadIdBitsetArray[arrayIndex]);
            uint16_t threadId = THREADS_PER_WORD * arrayIndex + bitIndex;
            if (threadId < maxThreadCount) {
                MOT_ASSERT(IS_THREAD_ID_BIT_SET(arrayIndex, bitIndex));  // bit-is-set means thread-id id free
                MARK_THREAD_ID_USED(arrayIndex, bitIndex);
                result = threadId;
            }
            break;
        }
    }
    (void)pthread_spin_unlock(&threadIdLock);

    return result;
}

extern void ApplyReservedThreadId(MOTThreadId threadId)
{
    MOTCurrThreadId = threadId;
    MOT_ATOMIC_INC(currentThreadCount);
    ScheduleThreadIdCleanup();
}

extern void FreeThreadId()
{
    // guard against repeated free
    if (MOTCurrThreadId == INVALID_THREAD_ID) {
        MOT_LOG_TRACE("Double attempt to free thread identifier declined");
    } else {
        MOTThreadId threadId = MOTCurrThreadId;
        FreeThreadIdInternal(MOTCurrThreadId);
        MOTCurrThreadId = INVALID_THREAD_ID;
        MOT_ATOMIC_DEC(currentThreadCount);

        // cancel auto-cleanup for current thread (reset pthread key to zero)
        (void)pthread_setspecific(threadIdCleanupKey, nullptr);
        MOT_LOG_TRACE("De-allocated internal thread id %" PRIu16, threadId);
    }
}

extern MOTThreadId GetNumaOrdinalThreadId(MOTThreadId threadId)
{
    MOTThreadId result = INVALID_THREAD_ID;
    if (threadId < maxThreadCount) {
        result = numaOrdinalThreadMap[threadId];
    } else {
        MOT_REPORT_ERROR(MOT_ERROR_INVALID_ARG,
            "N/A",
            "Request for NUMA ordinal thread identifier for invalid thread identifier %" PRIu16 " denied",
            threadId);
    }
    return result;
}

extern uint16_t GetMaxThreadCount()
{
    return maxThreadCount;
}

extern uint16_t GetCurrentThreadCount()
{
    return MOT_ATOMIC_LOAD(currentThreadCount);
}

static void FreeThreadIdInternal(MOTThreadId threadId)
{
    uint16_t arrayIndex = threadId / THREADS_PER_WORD;
    uint16_t bitIndex = threadId % THREADS_PER_WORD;

    (void)pthread_spin_lock(&threadIdLock);
    MOT_ASSERT(IS_THREAD_ID_USED(arrayIndex, bitIndex));
    MARK_THREAD_ID_FREE(arrayIndex, bitIndex);
    (void)pthread_spin_unlock(&threadIdLock);
}

static int InitNumaOrdinalThreadMap()
{
    // we rely on ordering done in affinity, and count one by one.
    // this we we are ready for any future thread distribution over NUMA sockets.
    int result = 0;

    int nodeCount = GetGlobalConfiguration().m_numaNodes;
    if (nodeCount > MEM_MAX_NUMA_NODES) {
        MOT_REPORT_PANIC(MOT_ERROR_INVALID_CFG,
            "Thread Id Pool Initialization",
            "Invalid configuration for number of NUMA node %d (exceeds allowed maximum %u)",
            nodeCount,
            (unsigned)MEM_MAX_NUMA_NODES);
        result = MOT_ERROR_INVALID_CFG;
    } else {
        uint32_t counterMap[MEM_MAX_NUMA_NODES];
        errno_t erc = memset_s(counterMap, sizeof(counterMap), 0, sizeof(counterMap));
        securec_check(erc, "\0", "\0");
        Affinity& affinity = GetTaskAffinity();  // this is always valid, even on thread-pool environment
        for (MOTThreadId i = 0; i < maxThreadCount; ++i) {
            int nodeId = affinity.GetAffineNuma(i);
            if (nodeId < nodeCount) {
                // record the number of thread affined to this socket, and the ordinal number of current thread in this
                // socket
                numaOrdinalThreadMap[i] = counterMap[nodeId]++;
            } else {
                MOT_REPORT_PANIC(MOT_ERROR_INVALID_CFG,
                    "Thread Id Pool Initialization",
                    "Invalid NUMA node id %u computed for thread id %u (while building NUMA ordinal thread map)",
                    (unsigned)nodeId,
                    (unsigned)i);
                result = MOT_ERROR_INVALID_CFG;
                break;
            }
        }
    }

    return result;
}

// Testing API - This API exists only for testing and should not be used by the engine
extern bool IsThreadIdFree(MOTThreadId threadId)
{
    bool result = false;

    uint16_t arrayIndex = threadId / THREADS_PER_WORD;
    uint16_t bitIndex = threadId % THREADS_PER_WORD;

    // pay attention: the bit-set is inverted: "1" bit means free, "0" bit means allocated
    if (IS_THREAD_ID_FREE(arrayIndex, bitIndex)) {
        result = true;
    }

    return result;
}

extern void DumpThreadIds(const char* reportName)
{
    MOT_LOG_INFO("Allocated thread id report: %s", reportName);
    for (uint16_t i = 0; i < threadIdArraySize; ++i) {
        for (uint16_t j = 0; j < THREADS_PER_WORD; ++j) {
            // pay attention: the bit-set is inverted: "1" bit means free, "0" bit means allocated
            MOTThreadId threadId = i * THREADS_PER_WORD + j;
            if (threadId < maxThreadCount) {
                if (IS_THREAD_ID_USED(i, j)) {
                    MOT_LOG_INFO("thread id: %" PRIu16, threadId);
                }
            } else {
                break;
            }
        }
    }
    MOT_LOG_INFO("End of allocated thread id report: %s", reportName);
}
}  // namespace MOT
