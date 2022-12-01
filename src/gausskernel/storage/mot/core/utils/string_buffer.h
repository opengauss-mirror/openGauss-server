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
 * string_buffer.h
 *    An expanding string buffer for formatting debug messages.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/utils/string_buffer.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef STRING_BUFFER_H
#define STRING_BUFFER_H

#include <cstdint>
#include <cstdlib>
#include <cstdarg>

namespace MOT {
/**
 * @brief An expanding string buffer for formatting debug messages. Unlike @mot_string this is a lightweight object with
 * much less logic.
 */
struct StringBuffer {
    /** @var The format buffer. */
    char* m_buffer;

    /** @var The buffer length. */
    uint32_t m_length;

    /** @var The buffer pointer position. */
    uint32_t m_pos;

    /** @var The factor of growth (either multiplicand or addend) */
    uint32_t m_growFactor;

    /** @enum The buffer growth method (multiplicative, additive or fixed). */
    enum GrowMethod {
        /** @var Buffer size multiplies each time buffer is full. */
        MULTIPLY,

        /** @var Buffer size increases by fixed amount of bytes each time buffer is full. */
        ADD,

        /** @var Buffer size is fixed. */
        FIXED
    } m_growMethod;

    static constexpr uint32_t GROWTH_FACTOR = 2;
};

/**
 * @brief Initializes a string buffer.
 * @param stringBuffer The string buffer to initialize.
 * @param length The initial string buffer length.
 * @param growFactor The buffer grow factor.
 * @param growMethod The buffer grow method.
 */
inline void StringBufferInit(
    StringBuffer* stringBuffer, uint32_t length, uint32_t growFactor, StringBuffer::GrowMethod growMethod)
{
    stringBuffer->m_buffer = (char*)::malloc(length);
    if (stringBuffer->m_buffer != nullptr) {
        stringBuffer->m_buffer[0] = 0;
        stringBuffer->m_length = length;
        stringBuffer->m_pos = 0;
        stringBuffer->m_growFactor = growFactor;
        stringBuffer->m_growMethod = growMethod;
    } else {
        stringBuffer->m_length = 0;
        stringBuffer->m_pos = 0;
    }
}

/**
 * @brief Initializes a string buffer with a fixed external buffer.
 * @param stringBuffer The string buffer to initialize.
 * @param buffer The external fixed buffer to use.
 * @param length The external fixed buffer length.
 */
inline void StringBufferInitFixed(StringBuffer* stringBuffer, char* buffer, uint32_t length)
{
    stringBuffer->m_buffer = buffer;
    stringBuffer->m_buffer[0] = 0;
    stringBuffer->m_length = length;
    stringBuffer->m_pos = 0;
    stringBuffer->m_growFactor = 0;
    stringBuffer->m_growMethod = StringBuffer::FIXED;
}

/**
 * @brief Destroys a string buffer.
 * @param stringBuffer The string buffer to destroy.
 */
inline void StringBufferDestroy(StringBuffer* stringBuffer)
{
    if ((stringBuffer->m_buffer != nullptr) && (stringBuffer->m_growMethod != StringBuffer::FIXED)) {
        ::free(stringBuffer->m_buffer);
        stringBuffer->m_buffer = nullptr;
    }
}

/**
 * @brief Resets a string buffer (puts pointer back to first position).
 * @param stringBuffer The string buffer to reset.
 */
inline void StringBufferReset(StringBuffer* stringBuffer)
{
    stringBuffer->m_pos = 0;
}

/**
 * @brief Appends formatted text to the string buffer.
 * @param stringBuffer The string buffer.
 * @param format The format string.
 * @param ... Format parameters.
 */
extern void StringBufferAppend(StringBuffer* stringBuffer, const char* format, ...);

/**
 * @brief Appends formatted text to the string buffer.
 * @param stringBuffer The string buffer.
 * @param format The format string.
 * @param args Format parameters.
 */
extern void StringBufferAppendV(StringBuffer* stringBuffer, const char* format, va_list args);

/**
 * @brief Appends a specified number of character from another string to the string buffer.
 * @param stringBuffer The string buffer.
 * @param str The string.
 * @param size The number of characters to append.
 */
extern void StringBufferAppendN(StringBuffer* stringBuffer, const char* str, int size);

/**
 * @brief Utility function for using a string buffer. The buffer is managed by the utility function, while the functor
 * uses the string buffer.
 * @tparam Functor The functor type.
 * @param functor The functor. Could be a lambda expression.
 * @param[opt] initialSize The initial string buffer size.
 */
template <typename Functor>
inline void StringBufferApply(const Functor& functor, uint32_t initialSize = 1024)
{
    StringBuffer sb = {0};
    StringBufferInit(&sb, initialSize, StringBuffer::GROWTH_FACTOR, MOT::StringBuffer::MULTIPLY);
    functor(&sb);
    StringBufferDestroy(&sb);
}

/**
 * @brief Utility function for using a string buffer. The buffer is managed by the utility function, while the functor
 * uses the string buffer. This variant uses a fixed buffer provided by the user.
 * @tparam Functor The functor type.
 * @param functor The functor. Could be a lambda expression.
 * @param buffer The fixed string buffer to use.
 * @param length The fixed string buffer length.
 */
template <typename Functor>
inline void StringBufferApplyFixed(const Functor& functor, char* buffer, uint32_t length)
{
    StringBuffer sb = {0};
    StringBufferInitFixed(&sb, buffer, length);
    functor(&sb);
    StringBufferDestroy(&sb);
}
}  // namespace MOT

#endif /* STRING_BUFFER_H */
