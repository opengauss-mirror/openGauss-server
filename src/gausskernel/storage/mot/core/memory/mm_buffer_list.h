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
 * mm_buffer_list.h
 *    A singly-linked list of buffers, usually used a cache of buffers.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/memory/mm_buffer_list.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef MM_BUFFER_LIST_H
#define MM_BUFFER_LIST_H

#include "mm_def.h"
#include "mm_lock.h"
#include "mm_buffer_header.h"
#include "utilities.h"
#include "string_buffer.h"

namespace MOT {
/**
 * @struct MemBufferList
 * @brief A singly-linked list of buffers, usually used a cache of buffers.
 */
struct PACKED MemBufferList {
    /** @var The number of items in the list. */
    uint32_t m_count;

    /** @var The first buffer in the list. */
    MemBufferHeader* m_head;

    /** @var The last buffer in the list. */
    MemBufferHeader* m_tail;
};

/**
 * @brief Initializes a buffer list without locking.
 * @param bufferList The buffer list to initialize.
 */
inline void MemBufferListInit(MemBufferList* bufferList)
{
    bufferList->m_count = 0;
    bufferList->m_head = NULL;
    bufferList->m_tail = NULL;
}

/**
 * @brief Pushes a buffer to a buffer list (single-link, no locking).
 * @param bufferList The buffer list.
 * @param bufferHeader The buffer header to push.
 */
inline void MemBufferListPush(MemBufferList* bufferList, MemBufferHeader* bufferHeader)
{
    bufferHeader->m_next = bufferList->m_head;
    bufferList->m_head = bufferHeader;
    if (bufferList->m_tail == NULL) {
        bufferList->m_tail = bufferList->m_head;
    }
    ++bufferList->m_count;
}

/**
 * @brief Pops a buffer from a buffer list (single-link, no locking).
 * @param bufferList The buffer list.
 * @return The popped buffer header.
 */
inline MemBufferHeader* MemBufferListPop(MemBufferList* bufferList)
{
    MemBufferHeader* bufferHeader = bufferList->m_head;
    if (bufferList->m_head != nullptr) {
        bufferList->m_head = bufferList->m_head->m_next;
        --bufferList->m_count;
        if (bufferList->m_head == NULL) {
            bufferList->m_tail = NULL;
        }
    }
    return bufferHeader;
}

/**
 * @brief Prints a buffer list into log.
 * @param name The name of the buffer list to print.
 * @param logLevel The log level to use in printing.
 * @param bufferList The buffer list to print.
 */
extern void MemBufferListPrint(const char* name, LogLevel logLevel, MemBufferList* bufferList);

/**
 * @brief Dumps a buffer list into string buffer.
 * @param indent The indentation level.
 * @param name The name of the buffer list to print.
 * @param bufferList The buffer list to print.
 * @param stringBuffer The string buffer.
 */
extern void MemBufferListToString(int indent, const char* name, MemBufferList* bufferList, StringBuffer* stringBuffer);
}  // namespace MOT

/**
 * @brief Dumps a buffer list into standard error stream.
 * @param arg The buffer list to print.
 */
extern "C" void MemBufferListDump(void* arg);

#endif /* MM_BUFFER_LIST_H */
