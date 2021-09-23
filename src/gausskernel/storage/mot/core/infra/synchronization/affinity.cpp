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
 * affinity.cpp
 *    Utility class for managing thread affinity to cores by a configurable policy.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/synchronization/affinity.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <pthread.h>
#include <sched.h>
#include <stdlib.h>

#include "affinity.h"
#include "global.h"
#include "utilities.h"
#include "mot_engine.h"
#include "mot_error.h"

namespace MOT {

DECLARE_LOGGER(Affinity, System)

Affinity::Affinity(uint64_t numaNodes, uint64_t physicalCores, AffinityMode affinityMode)
    : m_numaNodes(numaNodes), m_physicalCoresNuma(physicalCores), m_affinityMode(affinityMode)
{}

void Affinity::Configure(uint64_t numaNodes, uint64_t physicalCoresNuma, AffinityMode affinityMode)
{
    m_numaNodes = numaNodes;
    m_physicalCoresNuma = physicalCoresNuma;
    m_affinityMode = affinityMode;
}

uint32_t Affinity::GetAffineProcessor(uint64_t threadId) const
{
    uint32_t result = INVALID_CPU_ID;
    threadId = threadId % (m_numaNodes * m_physicalCoresNuma);

    switch (m_affinityMode) {
        case AffinityMode::FILL_SOCKET_FIRST: {
            result = (uint32_t)GetGlobalConfiguration().GetMappedCore(threadId);
            break;
        }

        case AffinityMode::EQUAL_PER_SOCKET: {
            threadId = threadId % (m_numaNodes * m_physicalCoresNuma);
            uint32_t numaId = threadId % m_numaNodes;
            uint32_t localProc = threadId / m_numaNodes;
            result = (uint32_t)(numaId * m_physicalCoresNuma + localProc);
            break;
        }

        case AffinityMode::FILL_PHYSICAL_FIRST:
            result = (uint32_t)GetGlobalConfiguration().GetCoreByConnidFP(threadId);
            break;

        default:
            MOT_LOG_ERROR("%s: Invalid affinity configuration: %d", __func__, (int)m_affinityMode);
            result = INVALID_CPU_ID;
            break;
    }
    return result;
}

uint32_t Affinity::GetAffineNuma(uint64_t threadId) const
{
    uint32_t result = INVALID_NODE_ID;
    threadId = threadId % (m_numaNodes * m_physicalCoresNuma);

    switch (m_affinityMode) {
        case AffinityMode::FILL_SOCKET_FIRST: {
            result = (uint32_t)GetGlobalConfiguration().GetCpuNode(GetGlobalConfiguration().GetMappedCore(threadId));
            break;
        }

        case AffinityMode::EQUAL_PER_SOCKET:
            result = (uint32_t)threadId % m_numaNodes;
            break;

        case AffinityMode::FILL_PHYSICAL_FIRST: {
            uint64_t realCoreCount = 0;
            if (GetGlobalConfiguration().IsHyperThread() == true) {
                realCoreCount = m_physicalCoresNuma / 2;
            } else {
                realCoreCount = m_physicalCoresNuma;
            }
            threadId = threadId % (m_numaNodes * realCoreCount);
            result = (uint32_t)(threadId / realCoreCount);
            break;
        }

        default:
            MOT_LOG_ERROR("%s: Invalid affinity configuration: %d", __func__, (int)m_affinityMode);
            result = (uint32_t)INVALID_NODE_ID;
            break;
    }

    return result;
}

bool Affinity::SetAffinity(uint64_t threadId, uint32_t* threadCore /* = nullptr */) const
{
    bool result = true;
    uint32_t coreId = GetAffineProcessor(threadId);

    cpu_set_t mask;
    CPU_ZERO(&mask);
    GetGlobalConfiguration().SetMaskToAllCoresinNumaSocket(mask, coreId);

    pthread_t currentThread = pthread_self();
    // The following call forces migration of the thread if it is
    // incorrectly placed
    int error = pthread_setaffinity_np(currentThread, sizeof(cpu_set_t), &mask);
    if (error == -1) {
        // just warn, we can still succeed with sched_setaffinity()
        MOT_LOG_SYSTEM_WARN_CODE(error, pthread_setaffinity_np, "Failed to set current thread affinity");

        error = sched_setaffinity(0, sizeof(cpu_set_t), &mask);
        if (error == -1) {
            // this time we log a full error report
            MOT_REPORT_SYSTEM_ERROR_CODE(error, sched_setaffinity, "", "Failed to set current thread affinity");
            result = false;
        }
    } else {
        MOT_LOG_TRACE("Set current thread %u affinity to core %u (socket %u)",
            (unsigned)threadId,
            (unsigned)coreId,
            (unsigned)GetAffineNuma(threadId));
    }

    if (threadCore) {
        *threadCore = coreId;
    }
    return result;
}

bool Affinity::SetNodeAffinity(int nodeId)
{
    bool result = true;
    cpu_set_t mask;
    CPU_ZERO(&mask);
    GetGlobalConfiguration().SetMaskToAllCoresinNumaSocket2(mask, nodeId);

    pthread_t currentThread = pthread_self();
    // The following call forces migration of the thread if it is incorrectly placed
    int error = pthread_setaffinity_np(currentThread, sizeof(cpu_set_t), &mask);
    if (error == -1) {
        // just warn, we can still succeed with sched_setaffinity()
        MOT_LOG_SYSTEM_WARN_CODE(error, pthread_setaffinity_np, "Failed to set current thread affinity");

        error = sched_setaffinity(0, sizeof(cpu_set_t), &mask);
        if (error == -1) {
            MOT_REPORT_SYSTEM_ERROR_CODE(error, sched_setaffinity, "", "Failed to set current thread affinity");
            result = false;
        }
    }
    return result;
}

static const char* AFFINITY_FILL_SOCKET_FIRST_STR = "fill-socket-first";
static const char* AFFINITY_EQUAL_PER_SOCKET_STR = "equal-per-socket";
static const char* AFFINITY_FILL_PHYSICAL_FIRST_STR = "fill-physical-first";
static const char* AFFINITY_NONE_STR = "none";

extern AffinityMode AffinityModeFromString(const char* affinityModeStr)
{
    AffinityMode result = AffinityMode::AFFINITY_INVALID;
    if (strcmp(affinityModeStr, AFFINITY_FILL_SOCKET_FIRST_STR) == 0) {
        result = AffinityMode::FILL_SOCKET_FIRST;
    } else if (strcmp(affinityModeStr, AFFINITY_EQUAL_PER_SOCKET_STR) == 0) {
        result = AffinityMode::EQUAL_PER_SOCKET;
    } else if (strcmp(affinityModeStr, AFFINITY_FILL_PHYSICAL_FIRST_STR) == 0) {
        result = AffinityMode::FILL_PHYSICAL_FIRST;
    } else if (strcmp(affinityModeStr, AFFINITY_NONE_STR) == 0) {
        result = AffinityMode::AFFINITY_NONE;
    }
    return result;
}

extern const char* AffinityModeToString(AffinityMode affinityMode)
{
    switch (affinityMode) {
        case AffinityMode::FILL_SOCKET_FIRST:
            return AFFINITY_FILL_SOCKET_FIRST_STR;

        case AffinityMode::EQUAL_PER_SOCKET:
            return AFFINITY_EQUAL_PER_SOCKET_STR;

        case AffinityMode::FILL_PHYSICAL_FIRST:
            return AFFINITY_FILL_PHYSICAL_FIRST_STR;

        case AffinityMode::AFFINITY_NONE:
            return AFFINITY_NONE_STR;

        default:
            return "N/A";
    }
}
}  // namespace MOT
