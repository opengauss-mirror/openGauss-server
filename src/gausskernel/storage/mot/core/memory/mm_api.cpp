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
 * mm_api.cpp
 *    Memory management API implementation.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/memory/mm_api.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "mm_api.h"
#include "mm_cfg.h"
#include "mm_numa.h"
#include "mm_raw_chunk_store.h"
#include "mm_raw_chunk_dir.h"
#include "mm_global_api.h"
#include "mm_buffer_api.h"
#include "mm_virtual_huge_chunk.h"
#include "mm_session_api.h"
#include "mm_session_large_buffer_store.h"
#include "mm_huge_object_allocator.h"
#include "mot_error.h"

namespace MOT {
DECLARE_LOGGER(MMApi, Memory);

// Initialization helper macro (for system calls)
#define CHECK_SYS_INIT_STATUS(rc, syscall, format, ...)                                       \
    if (rc != 0) {                                                                            \
        MOT_REPORT_SYSTEM_PANIC_CODE(rc, syscall, "MM Layer Startup", format, ##__VA_ARGS__); \
        break;                                                                                \
    }

// Initialization helper macro (for sub-service initialization)
#define CHECK_INIT_STATUS(rc, format, ...)                                               \
    if (rc != 0) {                                                                       \
        MOT_REPORT_PANIC(MOT_ERROR_INTERNAL, "MM Layer Startup", format, ##__VA_ARGS__); \
        break;                                                                           \
    }

extern int MemInit(MemReserveMode reserveMode /* = MEM_RESERVE_PHYSICAL */)
{
    int rc = 0;
    MOT_LOG_TRACE("Initializing Memory Management Layer");

    enum InitPhase {
        Init,
        CfgInit,
        NumaInit,
        RawChunkDirInit,
        HugeInit,
        RawChunkStoreInit,
        BufferApiInit,
        GlobalApiInit,
        VHChunkHeaderInit,
        SessionApiInit,
        SessionLBSInit,
        Done
    } initPhase = Init;

    do {  // instead of goto
        rc = MemCfgInit();
        CHECK_INIT_STATUS(rc, "Failed to initialize configuration, aborting");
        initPhase = CfgInit;

        MemNumaInit();
        initPhase = NumaInit;

        rc = MemRawChunkDirInit(g_memGlobalCfg.m_lazyLoadChunkDirectory);
        CHECK_INIT_STATUS(rc, "Failed to initialize raw chunk directory, aborting");
        initPhase = RawChunkDirInit;

        MemHugeInit();
        initPhase = HugeInit;

        rc = MemRawChunkStoreInit(reserveMode);
        CHECK_INIT_STATUS(rc, "Failed to initialize raw chunk store, aborting");
        initPhase = RawChunkStoreInit;

        rc = MemBufferApiInit();
        CHECK_INIT_STATUS(rc, "Failed to initialize buffer allocation API, aborting");
        initPhase = BufferApiInit;

        rc = MemGlobalApiInit();
        CHECK_INIT_STATUS(rc, "Failed to initialize global object allocation API, aborting");
        initPhase = GlobalApiInit;

        rc = MemVirtualHugeChunkHeaderInit();
        CHECK_INIT_STATUS(rc, "Failed to initialize virtual huge chunk header API, aborting");
        initPhase = VHChunkHeaderInit;

#ifdef MEM_SESSION_ACTIVE
        rc = MemSessionApiInit();
        CHECK_INIT_STATUS(rc, "Failed to initialize session-local allocation API, aborting");
        initPhase = SessionApiInit;

        rc = MemSessionLargeBufferStoreInit();
        CHECK_INIT_STATUS(rc, "Failed to initialize session large buffer store, aborting");
        initPhase = SessionLBSInit;
#endif
        initPhase = Done;
    } while (0);

    if (rc == 0) {
        MOT_LOG_TRACE("Memory Management Layer initialized successfully");
        MemNumaPrintCurrentStats("Initial footprint", LogLevel::LL_TRACE);
    } else {
        MOT_LOG_PANIC("Memory Management Layer initialization failed, see errors above");

        // roll-back
        switch (initPhase) {
            case Done:
            case SessionLBSInit:
                // impossible
                MOT_LOG_PANIC("Internal MM Layer initialization error: invalid failed phase");
                break;

            case SessionApiInit:
                MemSessionApiDestroy();
                // fall through

            case VHChunkHeaderInit:
                MemVirtualHugeChunkHeaderDestroy();
                // fall through

            case GlobalApiInit:
                MemGlobalApiDestroy();
                // fall through

            case BufferApiInit:
                MemBufferApiDestroy();
                // fall through

            case RawChunkStoreInit:
                MemRawChunkStoreDestroy();
                // fall through

            case HugeInit:
                MemHugeDestroy();
                // fall through

            case RawChunkDirInit:
                MemRawChunkDirDestroy();
                // fall through

            case NumaInit:
                MemNumaDestroy();
                // fall through

            case CfgInit:
                // nothing do to, fall through

            case Init:
                // nothing to do, stop
                break;

            default:
                MOT_LOG_PANIC("Internal MM Layer initialization error: unknown failed phase");
                break;
        }
    }

    return rc;
}

extern void MemDestroy()
{
    MOT_LOG_TRACE("Destroying Memory Management Layer");
    MemNumaPrint("MM Layer Pre-Shutdown Report", LogLevel::LL_INFO);
    MemRawChunkStorePrint("Chunk Store Pre-Shutdown Report", LogLevel::LL_TRACE, MEM_REPORT_SUMMARY);
#ifdef MEM_SESSION_ACTIVE
    MemSessionLargeBufferStoreDestroy();
    MemSessionApiDestroy();
#endif
    MemGlobalApiDestroy();
    MemBufferApiDestroy();
    MemRawChunkStoreDestroy();
    MemHugeDestroy();
    MemRawChunkDirDestroy();
    MemNumaDestroy();
    MOT_LOG_INFO("Memory Management Layer destroyed");
}

extern void MemClearSessionThreadCaches()
{
    MemBufferClearSessionCache();
}

extern uint64_t MemGetCurrentGlobalMemoryBytes()
{
    // we need only the first statistics slot (accumulation of all global pools)
    MemRawChunkPoolStats chunkPoolStats = {0};
    (void)MemRawChunkStoreGetGlobalStats(&chunkPoolStats, 1);
    return chunkPoolStats.m_usedBytes;
}

extern void MemPrint(const char* name, LogLevel logLevel, MemReportMode reportMode /* = MEM_REPORT_SUMMARY */)
{
    if (MOT_CHECK_LOG_LEVEL(logLevel)) {
        StringBufferApply([name, logLevel, reportMode](StringBuffer* stringBuffer) {
            MemToString(name, stringBuffer, reportMode);
            MOT_LOG(logLevel, "\n%s", stringBuffer->m_buffer);
        });
    }
}

extern void MemToString(
    const char* name, StringBuffer* stringBuffer, MemReportMode reportMode /* = MEM_REPORT_SUMMARY */)
{
    StringBufferAppend(stringBuffer, "MM API %s Report:\n", name);
    MemNumaToString(PRINT_REPORT_INDENT, name, stringBuffer);
    MemRawChunkStoreToString(PRINT_REPORT_INDENT, name, stringBuffer, reportMode);
    MemBufferApiToString(PRINT_REPORT_INDENT, name, stringBuffer, reportMode);
#ifdef MEM_SESSION_ACTIVE
    MemSessionApiToString(PRINT_REPORT_INDENT, name, stringBuffer, reportMode);
    MemSessionLargeBufferToString(PRINT_REPORT_INDENT, name, stringBuffer, reportMode);
#endif
    MemHugeToString(PRINT_REPORT_INDENT, name, stringBuffer, reportMode);
    MemGlobalApiToString(PRINT_REPORT_INDENT, name, stringBuffer, reportMode);
    StringBufferAppend(stringBuffer, "\n");
}
}  // namespace MOT

extern "C" void MemDump()
{
    MOT::StringBufferApply([](MOT::StringBuffer* stringBuffer) {
        MOT::MemToString("Debug Dump", stringBuffer, MOT::MEM_REPORT_SUMMARY);
        (void)fprintf(stderr, "%s", stringBuffer->m_buffer);
        (void)fflush(stderr);
    });
}

extern "C" void MemAnalyze(void* address)
{
    // find source chunk
    (void)fprintf(stderr, "Analysis report for address %p\n", address);
    (void)fprintf(stderr, "==============================\n");
    MOT::MemRawChunkHeader* chunk = (MOT::MemRawChunkHeader*)MOT::MemRawChunkDirLookup(address);
    if (chunk) {
        (void)fprintf(
            stderr, "Found source chunk %p (chunk type: %s)\n", chunk, MOT::MemChunkTypeToString(chunk->m_chunkType));
        // continue according to chunk type
        int found = 0;
        switch (chunk->m_chunkType) {
            case MOT::MEM_CHUNK_TYPE_RAW:
                // a raw chunk means it was deallocated, this is the simplest case, just search for it in all chunk
                // pools
                break;

            case MOT::MEM_CHUNK_TYPE_BUFFER:
                // according to node and buffer class we can trace the source allocator and continue from there
                found = MemBufferApiAnalyze(address);
                break;

            case MOT::MEM_CHUNK_TYPE_SMALL_OBJECT:
                // according to node and size we can trace the source allocator and continue from there
                found = MemGlobalApiAnalyze(address);
                break;

            case MOT::MEM_CHUNK_TYPE_HUGE_OBJECT:
                break;

#ifdef MEM_SESSION_ACTIVE
                /** @var Constant designating session chunk type. */
            case MOT::MEM_CHUNK_TYPE_SESSION:
                found = MemSessionApiAnalyze(address);
                break;

                /** @var Constant designating session large buffer pool chunk type. */
            case MOT::MEM_CHUNK_TYPE_SESSION_LARGE_BUFFER_POOL:
                found = MemSessionLargeBufferStoreAnalyze(address);
                break;
#endif

            default:
                break;
        }
        if (!found) {
            (void)fprintf(stderr, "Address not found yet, searching in chunk store...\n");
            MemRawChunkStoreAnalyze(address);
        }
    }
}
