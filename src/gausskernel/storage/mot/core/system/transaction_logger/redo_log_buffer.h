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
 * redo_log_buffer.h
 *    Implements a class for managing a single redo log buffer.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/transaction_logger/redo_log_buffer.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef REDO_LOG_BUFFER_H
#define REDO_LOG_BUFFER_H

#include <stdint.h>
#include <string.h>
#include <istream>
#include <string>
#include <sstream>
#include "global.h"
#include "redo_log_global.h"
#include "buffer.h"

namespace MOT {
/**
 * @brief Class for managing a single redo log buffer.
 */
class RedoLogBuffer {
public:
    inline RedoLogBuffer()
        : m_bufferSize(REDO_DEFAULT_BUFFER_SIZE),
          m_nextFree(sizeof(uint32_t)),
          m_buffer(nullptr),
          m_allocated(false),
          m_next(nullptr)
    {}

    inline bool Initialize()
    {
        if (!m_allocated) {
            m_buffer = new (std::nothrow) uint8_t[m_bufferSize];
            if (m_buffer != nullptr) {
                m_allocated = true;
            }
        }
        return m_allocated;
    }

    RedoLogBuffer(const RedoLogBuffer& orig) = delete;

    RedoLogBuffer(RedoLogBuffer&& other)
        : m_bufferSize(other.m_bufferSize), m_nextFree(other.m_nextFree), m_buffer(other.m_buffer)
    {
        other.m_buffer = nullptr;
    }

    inline ~RedoLogBuffer()
    {
        if (m_buffer != nullptr) {
            delete[] m_buffer;
        }
    }

    RedoLogBuffer& operator=(RedoLogBuffer&& other)
    {
        if (this == &other) {
            return *this;
        }
        if (m_buffer != nullptr) {
            delete[] m_buffer;
        }
        m_bufferSize = other.m_bufferSize;
        m_nextFree = other.m_nextFree;
        m_buffer = other.m_buffer;
        other.m_buffer = nullptr;
        return *this;
    }

    RedoLogBuffer& operator=(const RedoLogBuffer& other) = delete;

    /**
     * @brief Reserves space in the redo buffer for the given size.
     * @param size The size to be reserved.
     * @return The pointer to the start of the reserved space.
     */
    inline void* AllocAppend(size_t size)
    {
        if (m_nextFree + size <= m_bufferSize) {
            uint32_t offset = m_nextFree;
            m_nextFree += size;
            return &m_buffer[offset];
        }

        return nullptr;
    }

    /**
     * @brief Appends a typed item to the buffer.
     * @param x The typed item to Append.
     */
    template <typename T>
    inline void Append(const T& x)
    {
        MOT_ASSERT(m_nextFree + sizeof(T) <= m_bufferSize);
        T* ptr = (T*)&m_buffer[m_nextFree];
        *ptr = x;
        m_nextFree += sizeof(T);
    }

    /**
     * @brief Appends a raw buffer to this buffer.
     * @param data The buffer pointer.
     * @param size The buffer size.
     */
    inline void Append(const void* data, uint32_t size)
    {
        MOT_ASSERT(m_nextFree + size <= m_bufferSize);
        errno_t erc = memcpy_s(&m_buffer[m_nextFree], m_bufferSize - m_nextFree, data, size);
        securec_check(erc, "\0", "\0");
        m_nextFree += size;
    }

    /**
     * @brief Appends raw data from input stream.
     * @param is The input stream
     * @param size The amount of bytes to append from the stream.
     */
    void Append(std::istream& is, uint32_t size)
    {
        MOT_ASSERT(m_nextFree + size <= m_bufferSize);
        char* ptr = (char*)&m_buffer[m_nextFree];
        is.read(ptr, size);
        m_nextFree += size;
    }

    /**
     * @brief Appends typed items to the buffer.
     * @param x1 The first typed item to append.
     * @param x2 The second typed item to append.
     * @param x3 The third typed item to append.
     * @param x4 The fourth typed item to append.
     */
    template <typename T1, typename T2, typename T3, typename T4>
    inline void Append(const T1& x1, const T2& x2, const T3& x3, const T4& x4)
    {
        MOT_ASSERT(m_nextFree + (sizeof(T1) + sizeof(T2) + sizeof(T3) + sizeof(T4)) <= m_bufferSize);
        uint8_t* ptr = &m_buffer[m_nextFree];
        *(reinterpret_cast<T1*>(ptr)) = x1;
        *(reinterpret_cast<T2*>(ptr + sizeof(T1))) = x2;
        *(reinterpret_cast<T3*>(ptr + sizeof(T1) + sizeof(T2))) = x3;
        *(reinterpret_cast<T4*>(ptr + sizeof(T1) + sizeof(T2) + sizeof(T3))) = x4;
        m_nextFree += sizeof(T1) + sizeof(T2) + sizeof(T3) + sizeof(T4);
    }

    /**
     * @brief Appends typed items to the buffer.
     * @param x1 The first typed item to append.
     * @param x2 The second typed item to append.
     * @param x3 The third typed item to append.
     * @param x4 The fourth typed item to append.
     * @param x5 The fifth typed item to append.
     */
    template <typename T1, typename T2, typename T3, typename T4, typename T5>
    inline void Append(const T1& x1, const T2& x2, const T3& x3, const T4& x4, const T5& x5)
    {
        MOT_ASSERT(m_nextFree + (sizeof(T1) + sizeof(T2) + sizeof(T3) + sizeof(T4) + sizeof(T5)) <= m_bufferSize);
        uint8_t* ptr = &m_buffer[m_nextFree];
        *(reinterpret_cast<T1*>(ptr)) = x1;
        *(reinterpret_cast<T2*>(ptr + sizeof(T1))) = x2;
        *(reinterpret_cast<T3*>(ptr + sizeof(T1) + sizeof(T2))) = x3;
        *(reinterpret_cast<T4*>(ptr + sizeof(T1) + sizeof(T2) + sizeof(T3))) = x4;
        *(reinterpret_cast<T5*>(ptr + sizeof(T1) + sizeof(T2) + sizeof(T3) + sizeof(T4))) = x5;
        m_nextFree += sizeof(T1) + sizeof(T2) + sizeof(T3) + sizeof(T4) + sizeof(T5);
    }

    /**
     * @brief Appends typed items to the buffer.
     * @param x1 The first typed item to append.
     * @param x2 The second typed item to append.
     * @param x3 The third typed item to append.
     * @param x4 The fourth typed item to append.
     * @param x5 The fifth typed item to append.
     * @param x6 The sixth typed item to append.
     */
    template <typename T1, typename T2, typename T3, typename T4, typename T5, typename T6>
    inline void Append(const T1& x1, const T2& x2, const T3& x3, const T4& x4, const T5& x5, const T6& x6)
    {
        MOT_ASSERT(
            m_nextFree + (sizeof(T1) + sizeof(T2) + sizeof(T3) + sizeof(T4) + sizeof(T5) + sizeof(T6)) <= m_bufferSize);
        uint8_t* ptr = &m_buffer[m_nextFree];
        *(reinterpret_cast<T1*>(ptr)) = x1;
        *(reinterpret_cast<T2*>(ptr + sizeof(T1))) = x2;
        *(reinterpret_cast<T3*>(ptr + sizeof(T1) + sizeof(T2))) = x3;
        *(reinterpret_cast<T4*>(ptr + sizeof(T1) + sizeof(T2) + sizeof(T3))) = x4;
        *(reinterpret_cast<T5*>(ptr + sizeof(T1) + sizeof(T2) + sizeof(T3) + sizeof(T4))) = x5;
        *(reinterpret_cast<T6*>(ptr + sizeof(T1) + sizeof(T2) + sizeof(T3) + sizeof(T4) + sizeof(T5))) = x6;
        m_nextFree += sizeof(T1) + sizeof(T2) + sizeof(T3) + sizeof(T4) + sizeof(T5) + sizeof(T6);
    }

    /**
     * @brief Prepares the buffer for serialization.
     * @param[out] serialized_size Receives the serialized buffer size.
     * @return A pointer to the serialized buffer after preparation. */
    inline uint8_t* Serialize(uint32_t* serializedSize)
    {
        uint32_t* size = (uint32_t*)&m_buffer[0];
        *size = this->Size();
        *serializedSize = *size;
        return m_buffer;
    }

    /**
     * @brief Retrieves the raw data of the buffer past the buffer size header.
     * @param[out] raw_size Receives the raw buffer size.
     * @return The raw buffer past the buffer header (serialized data starting point).
     */
    inline void* RawData(uint32_t* rawSize) const
    {
        *rawSize = Size() - sizeof(uint32_t);
        return &m_buffer[0] + sizeof(uint32_t);
    }

    /** @brief Resets the buffer. */
    inline void Reset()
    {
        m_nextFree = sizeof(uint32_t);
    }

    /**
     * @brief Retrieves the free size for more data in the buffer.
     */
    inline uint32_t FreeSize() const
    {
        return m_bufferSize - m_nextFree;
    }

    /**
     * @brief Retrieves the size of the buffer, including the headers and raw
     * data.
     * @return Buffer size.
     */
    inline uint32_t Size() const
    {
        return m_nextFree;
    }

    /**
     * @brief Retrieves the raw size of the buffer, not including the headers
     * @return RAW buffer size.
     */
    inline uint32_t RawSize() const
    {
        return m_nextFree - sizeof(uint32_t);
    }

    /**
     * @brief Queries whether the buffer is empty.
     * @return True if the buffer is empty.
     */
    inline bool Empty() const
    {
        return m_nextFree == sizeof(uint32_t);
    }

    /**
     * @brief Dumps the buffer into string format.
     * @return The dumped buffer in string format.
     */
    std::string Dump(uint32_t len = 0) const
    {
        if (len == 0) {
            len = m_nextFree;
        }
        std::stringstream ss;
        ss << std::hex;
        for (uint32_t i = 0; i < len; i++) {
            if (m_buffer[i] < 16)
                ss << "0";
            ss << (uint32_t)m_buffer[i] << "::";
        }
        return ss.str();
    }

    inline void SetNext(RedoLogBuffer* nextBuffer)
    {
        m_next = nextBuffer;
    }

    inline RedoLogBuffer* GetNext()
    {
        return m_next;
    }

    /** @define By default use 1 MB buffers for redo log. */
    static constexpr uint32_t REDO_DEFAULT_BUFFER_SIZE = 1024 * 1024;

private:
    /** @var Buffer size. */
    uint32_t m_bufferSize;

    /** @var Next write offset. */
    uint32_t m_nextFree;

    /** @var The buffer. */
    uint8_t* m_buffer;

    /** @var Indicates whether buffer was allocated. */
    bool m_allocated;

    /** @var Manages in-place list of objects. */
    RedoLogBuffer* m_next;
};
}  // namespace MOT

#endif /* REDO_LOG_BUFFER_H */
