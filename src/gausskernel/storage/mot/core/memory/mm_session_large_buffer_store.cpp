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
 * mm_session_large_buffer_store.cpp
 *    Global store used for large session buffer allocations.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/memory/mm_session_large_buffer_store.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "mm_session_large_buffer_store.h"
#include "mm_cfg.h"
#include "mot_error.h"
#include "mm_numa.h"
#include "session_context.h"
#include "utilities.h"

namespace MOT {
DECLARE_LOGGER(SessionLargeBufferStore, Memory)

static MemSessionLargeBufferPool** g_globalPools = nullptr;
uint64_t g_memSessionLargeBufferMaxObjectSizeBytes = 0;

static uint64_t ComputeRealMaxObjectSize(uint64_t poolSizeBytes, uint64_t maxObjectSizeBytes);

extern int MemSessionLargeBufferStoreInit()
{
    int result = 0;
    MOT_LOG_TRACE("Initializing Session Large Buffer Store");
    uint64_t storeSizeBytes = g_memGlobalCfg.m_sessionLargeBufferStoreSizeMb * MEGA_BYTE;
    if (storeSizeBytes == 0) {  // pool disabled
        MOT_LOG_TRACE("Session large buffer stored is disabled");
        return 0;
    }
    MOT_LOG_TRACE("Using store size %" PRIu64 " bytes", storeSizeBytes);

    uint64_t maxObjectSizeBytes = g_memGlobalCfg.m_sessionLargeBufferStoreMaxObjectSizeMb * MEGA_BYTE;
    uint64_t poolSizeBytes = storeSizeBytes / g_memGlobalCfg.m_nodeCount;
    MOT_LOG_TRACE(
        "Using pool size %" PRIu64 " bytes, according to %u nodes", poolSizeBytes, g_memGlobalCfg.m_nodeCount);
    MOT_LOG_TRACE("Using configured max object size %" PRIu64 " bytes", maxObjectSizeBytes);
    g_memSessionLargeBufferMaxObjectSizeBytes = ComputeRealMaxObjectSize(poolSizeBytes, maxObjectSizeBytes);
    MOT_LOG_TRACE("Using computed max object size %" PRIu64 " bytes", g_memSessionLargeBufferMaxObjectSizeBytes);
    g_globalPools = (MemSessionLargeBufferPool**)calloc(g_memGlobalCfg.m_nodeCount, sizeof(MemSessionLargeBufferPool*));
    if (unlikely(g_globalPools == nullptr)) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Session Large Buffer Store Initialization",
            "Failed to allocate %" PRIu64 " bytes for session large buffer pool array",
            poolSizeBytes);
        result = MOT_ERROR_OOM;
    } else {
        for (uint32_t node = 0; node < g_memGlobalCfg.m_nodeCount; ++node) {
            g_globalPools[node] =
                (MemSessionLargeBufferPool*)MemNumaAllocLocal(sizeof(MemSessionLargeBufferPool), node);
            if (unlikely(g_globalPools[node] == nullptr)) {
                MOT_REPORT_ERROR(MOT_ERROR_OOM,
                    "Session Large Buffer Store Initialization",
                    "Failed to allocate %" PRIu64 " bytes for session large buffer pool header on node %u",
                    poolSizeBytes,
                    node);
                result = MOT_ERROR_OOM;
                break;
            }
            result = MemSessionLargeBufferPoolInit(
                g_globalPools[node], node, poolSizeBytes, g_memSessionLargeBufferMaxObjectSizeBytes);
            if (unlikely(result != 0)) {
                break;
            }
        }
    }
    if (result == 0) {
        MOT_LOG_TRACE("Session Large Buffer Store initialized successfully");
    }
    return result;
}

extern void MemSessionLargeBufferStoreDestroy()
{
    if (g_globalPools != nullptr) {
        for (uint32_t node = 0; node < g_memGlobalCfg.m_nodeCount; ++node) {
            if (g_globalPools[node] != nullptr) {
                MemSessionLargeBufferPoolDestroy(g_globalPools[node]);
                MemNumaFreeLocal(g_globalPools[node], sizeof(MemSessionLargeBufferPool), node);
            }
        }
        free(g_globalPools);
        g_globalPools = nullptr;
    }
}

extern void* MemSessionLargeBufferAlloc(uint64_t sizeBytes, MemSessionLargeBufferHeader** bufferHeaderList)
{
    void* result = nullptr;
    if (g_globalPools != nullptr) {
        result = MemSessionLargeBufferPoolAlloc(g_globalPools[MOTCurrentNumaNodeId], sizeBytes, bufferHeaderList);
    }
    return result;
}

extern void MemSessionLargeBufferFree(void* buffer, MemSessionLargeBufferHeader** bufferHeaderList)
{
    MemSessionLargeBufferPoolFree(g_globalPools[MOTCurrentNumaNodeId], buffer, bufferHeaderList);
}

extern void* MemSessionLargeBufferRealloc(
    void* buffer, uint64_t newSizeBytes, MemReallocFlags flags, MemSessionLargeBufferHeader** bufferHeaderList)
{
    return MemSessionLargeBufferPoolRealloc(
        g_globalPools[MOTCurrentNumaNodeId], buffer, newSizeBytes, flags, bufferHeaderList);
}

extern uint64_t MemSessionLargeBufferGetObjectSize(void* buffer)
{
    return MemSessionLargeBufferPoolGetObjectSize(g_globalPools[MOTCurrentNumaNodeId], buffer);
}

extern void MemSessionLargeBufferPrint(
    const char* name, LogLevel logLevel, MemReportMode reportMode /* = MEM_REPORT_SUMMARY */)
{
    if (g_globalPools != nullptr) {
        if (MOT_CHECK_LOG_LEVEL(logLevel)) {
            StringBufferApply([name, logLevel, reportMode](StringBuffer* stringBuffer) {
                MemSessionLargeBufferToString(0, name, stringBuffer, reportMode);
                MOT_LOG(logLevel, "%s", stringBuffer->m_buffer);
            });
        }
    }
}

extern void MemSessionLargeBufferToString(
    int indent, const char* name, StringBuffer* stringBuffer, MemReportMode reportMode /* = MEM_REPORT_SUMMARY */)
{
    if (g_globalPools != nullptr) {
        errno_t erc;
        StringBufferAppend(stringBuffer, "%*sSession Large Buffer Store %s:\n", indent, "", name);
        for (uint32_t node = 0; node < g_memGlobalCfg.m_nodeCount; ++node) {
            const uint32_t nameLen = 32;
            char poolName[nameLen];
            erc = snprintf_s(poolName, nameLen, nameLen - 1, "Global[%u]", node);
            securec_check_ss(erc, "\0", "\0");
            MemSessionLargeBufferPoolToString(
                indent + PRINT_REPORT_INDENT, poolName, g_globalPools[node], stringBuffer, reportMode);
        }
    }
}

static uint64_t ComputeRealMaxObjectSize(uint64_t poolSizeBytes, uint64_t maxObjectSizeBytes)
{
    // as long as 4 largest objects do not fit in half of the pool, then we continue dividing it by 2
    while ((4 * maxObjectSizeBytes) > (poolSizeBytes / 2)) {
        maxObjectSizeBytes /= 2;
    }
    return maxObjectSizeBytes;
}
}  // namespace MOT

extern "C" void MemSessionLargeBufferDump()
{
    if (MOT::g_globalPools != nullptr) {
        MOT::StringBufferApply([](MOT::StringBuffer* stringBuffer) {
            MOT::MemSessionLargeBufferToString(0, "Debug Dump", stringBuffer, MOT::MEM_REPORT_DETAILED);
            (void)fprintf(stderr, "%s", stringBuffer->m_buffer);
            (void)fflush(stderr);
        });
    }
}

extern "C" int MemSessionLargeBufferStoreAnalyze(void* buffer)
{
    if (MOT::g_globalPools != nullptr) {
        for (uint32_t node = 0; node < MOT::g_memGlobalCfg.m_nodeCount; ++node) {
            (void)fprintf(stderr, "Searching buffer %p in session large buffer pool %u...\n", buffer, node);
            if (MOT::g_globalPools[node]) {
                if (MemSessionLargeBufferPoolAnalyze(MOT::g_globalPools[node], buffer)) {
                    return 1;
                }
            }
        }
    }
    return 0;
}
