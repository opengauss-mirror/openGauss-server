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
 * buffer.h
 *    Buffer implementation.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/utils/buffer.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef MOT_BUFFER_H
#define MOT_BUFFER_H

#define DEFAULT_BUFFER_SIZE 4096 * 1000  // 4MB

#include <stdint.h>
#include <string>
#include <sstream>
#include "utilities.h"

namespace MOT {
class Buffer {
public:
    inline Buffer() : Buffer(DEFAULT_BUFFER_SIZE)
    {}

    inline Buffer(uint8_t* buffer, uint32_t size)
        : m_bufferSize(size), m_nextFree(0), m_buffer(buffer), m_allocated(true)
    {}

    inline Buffer(uint32_t size) : m_bufferSize(size), m_nextFree(0), m_allocated(false)
    {}

    inline virtual ~Buffer()
    {
        if (m_buffer) {
            delete[] m_buffer;
        }
    }

    inline bool Initialize()
    {
        if (!m_allocated) {
            m_buffer = new (std::nothrow) uint8_t[m_bufferSize];
            if (m_buffer) {
                m_allocated = true;
            }
        }
        return m_allocated;
    }

    Buffer& operator=(Buffer&& other) = delete;
    Buffer& operator=(const Buffer& other) = delete;

    inline static std::string Dump(const uint8_t* buffer, uint32_t size)
    {
        std::stringstream ss;
        ss << std::hex;
        for (uint32_t i = 0; i < size; i++) {
            if (buffer[i] < 16)
                ss << "0";
            ss << (uint)buffer[i] << "::";
        }
        return ss.str();
    }

    inline bool AppendAt(const void* data, uint32_t size, uint32_t offset)
    {
        if (offset + size <= m_bufferSize) {
            errno_t erc = memcpy_s(&m_buffer[offset], m_bufferSize - offset, data, size);
            securec_check(erc, "\0", "\0");
            return true;
        } else {
            return false;
        }
    }

    /**
     * @brief Appends a typed item to the buffer.
     * @param x The typed item to append.
     */
    template <typename T>
    inline bool Append(const T x)
    {
        if (m_nextFree + sizeof(T) <= m_bufferSize) {
            T* ptr = (T*)&m_buffer[m_nextFree];
            *ptr = x;
            m_nextFree += sizeof(T);
            return true;
        } else {
            return false;
        }
    }

    /**
     * @brief Appends a raw buffer to this buffer.
     * @param data The buffer pointer.
     * @param size The buffer size.
     */
    inline bool Append(const void* data, uint32_t size)
    {
        if (m_nextFree + size <= m_bufferSize) {
            errno_t erc = memcpy_s(&m_buffer[m_nextFree], m_bufferSize - m_nextFree, data, size);
            securec_check(erc, "\0", "\0");
            m_nextFree += size;
            return true;
        } else {
            return false;
        }
    }

    /**
     * @brief Appends raw data from input stream.
     * @param is The input stream
     * @param size The amount of bytes to append from the stream.
     */
    bool Append(std::istream& is, uint32_t size)
    {
        if (m_nextFree + size <= m_bufferSize) {
            char* ptr = (char*)&m_buffer[m_nextFree];
            is.read(ptr, size);
            m_nextFree += size;
            return true;
        } else {
            return false;
        }
    }

    /**
     * @brief Retrieves the data of the buffer.
     * @return The buffer.
     */
    inline void* Data() const
    {
        return m_buffer;
    }

    /**
     * @brief Retrieves the data of the buffer.
     * @param[out] size of the buffer.
     * @return The buffer.
     */
    inline void* Data(uint32_t* size) const
    {
        *size = m_nextFree;
        return m_buffer;
    }

    /** @brief Resets the buffer. */
    inline void Reset()
    {
        m_nextFree = 0;
    }

    /**
     * @brief Retrieves the free size for more data in the buffer.
     */
    inline uint32_t MaxSize() const
    {
        return m_bufferSize;
    }

    /**
     * @brief Retrieves the free size for more data in the buffer.
     */
    inline uint32_t FreeSize() const
    {
        return m_bufferSize - m_nextFree;
    }

    /**
     * @brief Retrieves the size of the buffer.
     * @return Buffer size.
     */
    inline uint32_t Size() const
    {
        return m_nextFree;
    }

    /**
     * @brief Queries whether the buffer is empty.
     * @return True if the buffer is empty.
     */
    inline bool Empty() const
    {
        return m_nextFree == 0;
    }

    /**
     * @brief Dumps the buffer into string format.
     * @return The dumped buffer in string format.
     */
    std::string Dump() const
    {
        return Buffer::Dump(m_buffer, m_nextFree);
    }

private:
    /** @var Next write offset. */
    uint32_t m_bufferSize;

    /** @var Next write offset. */
    uint32_t m_nextFree;

    /** @var The buffer. */
    uint8_t* m_buffer;

    /** @val indicates whether memory was already allocated for the buffer */
    bool m_allocated;
};
}  // namespace MOT
#endif /* MOT_BUFFER_H */
