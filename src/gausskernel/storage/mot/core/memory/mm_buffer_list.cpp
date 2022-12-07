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
 * mm_buffer_list.cpp
 *    A singly-linked list of buffers, usually used a cache of buffers.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/memory/mm_buffer_list.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "mm_buffer_list.h"

namespace MOT {
DECLARE_LOGGER(BufferList, Memory)

extern void MemBufferListPrint(const char* name, LogLevel logLevel, MemBufferList* bufferList)
{
    if (MOT_CHECK_LOG_LEVEL(logLevel)) {
        StringBufferApply([name, logLevel, bufferList](StringBuffer* stringBuffer) {
            MemBufferListToString(0, name, bufferList, stringBuffer);
            MOT_LOG(logLevel, "%s", stringBuffer->m_buffer);
        });
    }
}

extern void MemBufferListToString(int indent, const char* name, MemBufferList* bufferList, StringBuffer* stringBuffer)
{
    StringBufferAppend(stringBuffer, "%*s%s Buffer List [count=%u] = { ", indent, "", name, bufferList->m_count);
    MemBufferHeader* itr = bufferList->m_head;
    while (itr != NULL) {
        StringBufferAppend(stringBuffer, "%p", itr);
        if (itr->m_next != nullptr) {
            StringBufferAppend(stringBuffer, ", ");
        }
        itr = itr->m_next;
    }
    StringBufferAppend(stringBuffer, " }");
}
}  // namespace MOT

extern "C" void MemBufferListDump(void* arg)
{
    MOT::MemBufferList* bufferList = (MOT::MemBufferList*)arg;

    MOT::StringBufferApply([bufferList](MOT::StringBuffer* stringBuffer) {
        MOT::MemBufferListToString(0, "Debug Dump", bufferList, stringBuffer);
        (void)fprintf(stderr, "%s", stringBuffer->m_buffer);
        (void)fflush(stderr);
    });
}
