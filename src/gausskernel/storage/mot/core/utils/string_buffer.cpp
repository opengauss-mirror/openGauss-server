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
 * string_buffer.cpp
 *    An expanding string buffer for formatting debug messages.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/utils/string_buffer.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "global.h"
#include "string_buffer.h"
#include "debug_utils.h"

#include <stdarg.h>
#include <stdio.h>
#include <string.h>

namespace MOT {
static bool StringBufferRealloc(StringBuffer* stringBuffer, int sppendSize);

extern void StringBufferAppend(StringBuffer* stringBuffer, const char* format, ...)
{
    // if we previously failed to allocate memory then we silently ignore the request
    if (stringBuffer->m_buffer == nullptr) {
        return;
    }

    va_list args;
    va_start(args, format);

    StringBufferAppendV(stringBuffer, format, args);

    va_end(args);
}

extern void StringBufferAppendV(StringBuffer* stringBuffer, const char* format, va_list args)
{
    // if we previously failed to allocate memory then we silently ignore the request
    if (stringBuffer->m_buffer == nullptr) {
        return;
    }

    va_list argsCopy;
    va_copy(argsCopy, args);

    // find out how many characters are required and realloc if needed
    int size = ::vsnprintf(nullptr, 0, format, argsCopy);
    va_end(argsCopy);
    if (size >= (int)(stringBuffer->m_length - stringBuffer->m_pos)) {
        if (!StringBufferRealloc(stringBuffer, size)) {
            // silently eject
            return;
        }
    }

    // format the message
    va_copy(argsCopy, args);  // must reaffirm after previous call to vsnprintf()!
    errno_t erc = vsnprintf_s(stringBuffer->m_buffer + stringBuffer->m_pos,
        stringBuffer->m_length - stringBuffer->m_pos,
        stringBuffer->m_length - stringBuffer->m_pos - 1,
        format,
        argsCopy);
    securec_check_ss(erc, "\0", "\0");
    stringBuffer->m_pos += (uint32_t)erc;

    va_end(argsCopy);
}

extern void StringBufferAppendN(StringBuffer* stringBuffer, const char* str, int size)
{
    // if we previously failed to allocate memory then we silently ignore the request
    if (stringBuffer->m_buffer == nullptr) {
        return;
    }

    // make sure we have enough room
    if (size >= (int)(stringBuffer->m_length - stringBuffer->m_pos)) {
        if (!StringBufferRealloc(stringBuffer, size + 1)) {  // one extra-char for terminating null
            // silently eject
            return;
        }
    }

    uint32_t destSize = stringBuffer->m_length - stringBuffer->m_pos;
    errno_t erc = strncpy_s(stringBuffer->m_buffer + stringBuffer->m_pos, destSize, str, size);
    securec_check(erc, "\0", "\0");
    stringBuffer->m_pos += size;
    stringBuffer->m_buffer[stringBuffer->m_pos++] = 0;
}

static bool StringBufferRealloc(StringBuffer* stringBuffer, int sppendSize)
{
    bool result = false;

    // if buffer is fixed we cannot grow
    if (stringBuffer->m_growMethod == StringBuffer::FIXED) {
        return false;
    }

    // compute new size
    int newLength = stringBuffer->m_length;
    int requiredSize = stringBuffer->m_pos + sppendSize;
    if (stringBuffer->m_growMethod == StringBuffer::ADD) {
        newLength =
            (requiredSize + stringBuffer->m_growFactor - 1) / stringBuffer->m_growFactor * stringBuffer->m_growFactor;
    } else {  // multiply
        while (newLength <= requiredSize) {
            newLength *= stringBuffer->m_growFactor;
        }
    }

    // reallocate buffer
    void* newBuffer = ::malloc(newLength);
    if (newBuffer != NULL) {
        errno_t erc = memcpy_s(newBuffer, newLength, stringBuffer->m_buffer, stringBuffer->m_length);
        securec_check(erc, "\0", "\0");
        ::free(stringBuffer->m_buffer);
        stringBuffer->m_buffer = (char*)newBuffer;
        stringBuffer->m_length = newLength;
        MOT_ASSERT(stringBuffer->m_length > (stringBuffer->m_pos + sppendSize));
        result = true;
    }

    return result;
}
}  // namespace MOT
