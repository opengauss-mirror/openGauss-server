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
 * mm_session_large_buffer_list.cpp
 *    A list of large buffers used for session allocations.
 *    A single list may be used by all sessions running on the same NUMA node.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/memory/mm_session_large_buffer_list.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "mm_session_large_buffer_list.h"
#include "utilities.h"
#include "session_context.h"
#include "mm_api.h"

namespace MOT {
DECLARE_LOGGER(SessionLargeBufferList, Memory)

extern int MemSessionLargeBufferListInit(
    MemSessionLargeBufferList* sessionBufferList, void* bufferList, uint64_t bufferSize, uint64_t bufferCount)
{
    int result = MemLockInitialize(&sessionBufferList->m_lock);
    if (result != 0) {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
            "Session Large Buffer List Initialization",
            "Failed to initialize large buffer list lock");
    } else {
        sessionBufferList->m_bufferList = bufferList;
        sessionBufferList->m_bufferSize = bufferSize;
        sessionBufferList->m_maxBufferCount = bufferCount;
        sessionBufferList->m_allocatedCount = 0;
        sessionBufferList->m_freeBitsetCount = (bufferCount + sizeof(uint64_t) - 1) / sizeof(uint64_t);
        for (uint32_t i = 0; i < sessionBufferList->m_freeBitsetCount; ++i) {
            sessionBufferList->m_freeBitset[i] = 0xFFFFFFFFFFFFFFFF;
        }

        sessionBufferList->m_bufferHeaderList =
            (MemSessionLargeBufferHeader*)(sessionBufferList->m_freeBitset + sessionBufferList->m_freeBitsetCount);

        for (uint32_t bufferIndex = 0; bufferIndex < sessionBufferList->m_maxBufferCount; ++bufferIndex) {
            MemSessionLargeBufferHeader* bufferHeader =
                (MemSessionLargeBufferHeader*)(sessionBufferList->m_bufferHeaderList + bufferIndex);
            bufferHeader->m_buffer =
                (void*)(((uint8_t*)sessionBufferList->m_bufferList) + bufferIndex * (sessionBufferList->m_bufferSize));
            bufferHeader->m_next = nullptr;
            bufferHeader->m_realObjectSize = 0;
        }
    }
    return result;
}

extern void MemSessionLargeBufferListOnInvalidFree(
    MemSessionLargeBufferList* sessionBufferList, void* buffer, uint64_t bufferIndex, const char* invalidType)
{
    MOT_LOG_PANIC("%s free of session large buffer %p [@%" PRIu64
                  "] in buffer list %p  (node: %d, buffer-size=%" PRIu64 " MB, allocated=%u/%u)",
        invalidType,
        buffer,
        bufferIndex,
        sessionBufferList,
        MOTCurrentNumaNodeId,
        sessionBufferList->m_bufferSize / MEGA_BYTE,
        sessionBufferList->m_allocatedCount,
        sessionBufferList->m_maxBufferCount);

    // this is a very extreme case, we force abort in the hope of a better root cause analysis
    MOTAbort(buffer);
}

extern void MemSessionLargeBufferListPrint(const char* name, LogLevel logLevel,
    MemSessionLargeBufferList* sessionBufferList, MemReportMode reportMode /* = MEM_REPORT_SUMMARY */)
{
    if (MOT_CHECK_LOG_LEVEL(logLevel)) {
        StringBufferApply([name, logLevel, sessionBufferList, reportMode](StringBuffer* stringBuffer) {
            MemSessionLargeBufferListToString(0, name, sessionBufferList, stringBuffer, reportMode);
            MOT_LOG(logLevel, "%s", stringBuffer->m_buffer);
        });
    }
}

extern void MemSessionLargeBufferListToString(int indent, const char* name,
    MemSessionLargeBufferList* sessionBufferList, StringBuffer* stringBuffer,
    MemReportMode reportMode /* = MEM_REPORT_SUMMARY */)
{
    MemSessionLargeBufferStats stats = {};
    MemSessionLargeBufferListGetStats(sessionBufferList, &stats);
    if (stats.m_allocatedBytes > 0) {
        if (reportMode == MEM_REPORT_SUMMARY) {
            StringBufferAppend(stringBuffer,
                "%*sSession Large Buffer List %s [buffer-size=%" PRIu64 " MB, max-buffers=%u]: %" PRIu64 " MB allocated"
                ", %" PRIu64 " MB requested\n",
                indent,
                "",
                name,
                sessionBufferList->m_bufferSize / MEGA_BYTE,
                sessionBufferList->m_maxBufferCount,
                stats.m_allocatedBytes / MEGA_BYTE,
                stats.m_requestedBytes / MEGA_BYTE);
        } else {
            StringBufferAppend(stringBuffer,
                "%*sSession Large Buffer List %s: [buffer-size=%" PRIu64 " MB, max-buffers=%u]\n",
                indent,
                "",
                name,
                sessionBufferList->m_bufferSize / MEGA_BYTE,
                sessionBufferList->m_allocatedCount,
                sessionBufferList->m_maxBufferCount);
            StringBufferAppend(stringBuffer, "%*sBit-set: { ", indent + PRINT_REPORT_INDENT, "");
            for (uint32_t i = 0; i < sessionBufferList->m_freeBitsetCount; ++i) {
                StringBufferAppend(stringBuffer, "%p", (void*)sessionBufferList->m_freeBitset[i]);
                if (i + 1 < sessionBufferList->m_freeBitsetCount) {
                    StringBufferAppend(stringBuffer, ", ");
                }
            }
            StringBufferAppend(stringBuffer, " }\n");

            StringBufferAppend(stringBuffer, "%*sBuffers: { ", indent + PRINT_REPORT_INDENT, "");
            uint32_t bufferCount = 0;
            for (uint32_t bufferIndex = 0; bufferIndex < sessionBufferList->m_maxBufferCount; ++bufferIndex) {
                uint32_t slot = bufferIndex / 64;
                uint32_t index = bufferIndex % 64;
                if ((sessionBufferList->m_freeBitset[slot] & (((uint64_t)1) << (63 - index))) ==
                    0) {                                                           // buffer allocated to application
                    void* buffer = ((uint8_t*)sessionBufferList->m_bufferList) +   // beginning offset
                                   sessionBufferList->m_bufferSize * bufferIndex;  // size * index
                    MemSessionLargeBufferHeader* bufferHeader =
                        (MemSessionLargeBufferHeader*)(sessionBufferList->m_bufferHeaderList +  // beginning offset
                                                       bufferIndex);                            // buffer header index
                    StringBufferAppend(stringBuffer, "%p (%" PRIu64 " bytes)", buffer, bufferHeader->m_realObjectSize);
                    ++bufferCount;
                    if (bufferCount + 1 < sessionBufferList->m_allocatedCount) {
                        StringBufferAppend(stringBuffer, ", ");
                    }
                }
            }
            StringBufferAppend(stringBuffer, " }\n");
        }
    }
}
}  // namespace MOT

extern "C" void MemSessionLargeBufferListDump(void* arg)
{
    MOT::MemSessionLargeBufferList* list = (MOT::MemSessionLargeBufferList*)arg;

    MOT::StringBufferApply([list](MOT::StringBuffer* stringBuffer) {
        MOT::MemSessionLargeBufferListToString(0, "Debug Dump", list, stringBuffer, MOT::MEM_REPORT_DETAILED);
        (void)fprintf(stderr, "%s", stringBuffer->m_buffer);
        (void)fflush(stderr);
    });
}

extern "C" int MemSessionLargeBufferListAnalyze(void* list, void* buffer)
{
    int result = 0;
    MOT::MemSessionLargeBufferList* sessionBufferList = (MOT::MemSessionLargeBufferList*)list;
    if (((uint8_t*)buffer) >= ((uint8_t*)sessionBufferList->m_bufferList)) {
        uint64_t bufferOffset = (uint64_t)(((uint8_t*)buffer) - ((uint8_t*)sessionBufferList->m_bufferList));
        uint64_t bufferIndex = bufferOffset / sessionBufferList->m_bufferSize;
        if (bufferIndex < sessionBufferList->m_maxBufferCount) {
            MOT::MemSessionLargeBufferHeader* bufferHeader =
                (MOT::MemSessionLargeBufferHeader*)(sessionBufferList->m_bufferHeaderList + bufferIndex);
            (void)fprintf(stderr,
                "Object %p found in session buffer list of %" PRIu64 " bytes buffers at index %" PRIu64 ", with real "
                "size %" PRIu64 "\n",
                buffer,
                sessionBufferList->m_bufferSize,
                bufferIndex,
                bufferHeader->m_realObjectSize);
            result = 1;
        }
    }
    return result;
}
