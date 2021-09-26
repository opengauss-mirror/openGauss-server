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
 * key.h
 *    Generic implementation of index key.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/storage/key.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef MOT_KEY_H
#define MOT_KEY_H

#include <stdint.h>
#include <string.h>

#include "utilities.h"
#include "global.h"

namespace MOT {
enum class KeyType : uint8_t {
    PRIMARY_KEY,
    SECONDARY_KEY,
    EMPTY_KEY,
    SURROGATE_KEY,
    INTERNAL_KEY,
    TXN_KEY,
    INS_KEY_BASE,
    INS_KEY_LONG_TXN
};

/**
 * @class Key
 * @brief Generic implementation of index key
 */
class __attribute__((__packed__)) Key {
public:
    /** @brief Type of ikeys. */
    typedef KeyType IkeyType;

    // Default constructor for empty key
    Key() : m_type(KeyType::TXN_KEY), m_keyLen(0)
    {}

    // For static allocation and testing
    Key(uint16_t size, KeyType type = KeyType::TXN_KEY) : m_type(type), m_keyLen(size)
    {
        errno_t erc = memset_s(const_cast<uint8_t*>(m_keyBuf), ALIGN8(m_keyLen), 0, ALIGN8(size));
        securec_check(erc, "\0", "\0");
    }

    Key(const Key& src) : m_type(src.m_type), m_keyLen(src.m_keyLen)
    {
        errno_t erc = memcpy_s(m_keyBuf, ALIGN8(m_keyLen), src.m_keyBuf, ALIGN8(src.m_keyLen));
        securec_check(erc, "\0", "\0");
    }

    inline __attribute__((always_inline)) virtual ~Key()
    {}

    /**
     * @brief Initialzie key type and length
     * @param keyLen Key length
     * @param type Used to distiguish between different flows
     */
    inline void InitKey(uint16_t keyLen, KeyType type = KeyType::TXN_KEY)
    {
        m_keyLen = keyLen;
        m_type = type;
        errno_t erc = memset_s(const_cast<uint8_t*>(m_keyBuf), ALIGN8(m_keyLen), 0, ALIGN8(m_keyLen));
        securec_check(erc, "\0", "\0");
    }

    inline void SetKeyType(KeyType type)
    {
        m_type = type;
    }

    inline uint8_t* GetKeyBuf() const
    {
        return (uint8_t*)m_keyBuf;
    }

    inline uint16_t GetKeyLength() const
    {
        return m_keyLen;
    }

    inline uint16_t GetAlignedKeyLength() const
    {
        return ALIGN8(m_keyLen);
    }

    inline void SetKeyLen(uint16_t newLen)
    {
        MOT_ASSERT(newLen <= MAX_KEY_SIZE);
        m_keyLen = newLen;
    }

    inline void CpKey(const uint8_t* buf, uint16_t len)
    {
        MOT_ASSERT(len <= m_keyLen);
        errno_t erc = memcpy_s(const_cast<uint8_t*>(m_keyBuf), m_keyLen, buf, len);
        securec_check(erc, "\0", "\0");
        m_keyLen = len;
    }

    /**
     * @brief Fills a patern in a specific locaion of the key
     *        to break similarity in secondary non-unique indices
     * @param buf Buffer to copy
     * @param len Length of the buffer
     * @param offset Offset in the key
     * @return False on error
     */
    inline bool FillValue(const uint8_t* buf, uint16_t len, uint16_t offset)
    {
        if (unlikely((offset + len) > m_keyLen)) {
            MOT_LOG_ERROR("Value is too big: offset %u + len %u > m_keyLen %u",
                (unsigned)offset,
                (unsigned)len,
                (unsigned)m_keyLen);
            return false;
        }

        errno_t erc = memcpy_s(const_cast<uint8_t*>(m_keyBuf + offset), m_keyLen - offset, buf, len);
        securec_check(erc, "\0", "\0");
        return true;
    }

    /**
     * @brief Fills a pattern in a specific location of the key
     *        to break similarity in secondary non-unique indices
     * @param buf buffer to copy
     * @param len length of the buffer
     * @param offset inside the key
     * @return False on error
     */
    inline bool FillPattern(const uint8_t pattern, uint16_t len, uint16_t offset)
    {
        if (unlikely((offset + len) > m_keyLen)) {
            MOT_LOG_ERROR("Value is too big");
            return false;
        }

        errno_t erc = memset_s(const_cast<uint8_t*>(m_keyBuf + offset), m_keyLen - offset, pattern, len);
        securec_check(erc, "\0", "\0");
        return true;
    }

    inline void CpKey(const Key& key)
    {
        CpKey(key.GetKeyBuf(), key.GetKeyLength());
    }

    void PrintKey() const
    {
        MOT_LOG_INFO("Key:%s Key length = %d ", HexStr(m_keyBuf, m_keyLen).c_str(), m_keyLen);
    }

    std::string GetKeyStr() const
    {
        return HexStr(m_keyBuf, m_keyLen).c_str();
    }

    bool operator==(const Key& key) const
    {
        MOT_ASSERT(m_keyLen == key.GetKeyLength());
        return ((memcmp(m_keyBuf, key.GetKeyBuf(), m_keyLen) == 0) ? true : false);
    }

    bool operator<(const Key& key) const
    {
        MOT_ASSERT(m_keyLen == key.GetKeyLength());
        return ((memcmp(m_keyBuf, key.GetKeyBuf(), m_keyLen) < 0) ? true : false);
    }

    bool operator<=(const Key& key) const
    {
        MOT_ASSERT(m_keyLen == key.GetKeyLength());
        return ((memcmp(m_keyBuf, key.GetKeyBuf(), m_keyLen) <= 0) ? true : false);
    }

    Key& operator=(const Key& other)
    {
        CpKey(other);
        return *this;
    }

    /** @cond EXCLUDE_DOC */
    Key(Key&&) = delete;

    Key& operator=(Key&&) = delete;

    bool operator>(const Key& key) = delete;

    bool operator>=(const Key& key) = delete;
    /** @endcond */

    /** @var key type */
    IkeyType m_type;

    /** @var length of the key */
    uint16_t m_keyLen;

    /** @var key raw buffer */
    uint8_t m_keyBuf[0];

    DECLARE_CLASS_LOGGER();
};

/**
 * @class MaxKey
 * @brief Max key used as transactional key
 */
class __attribute__((__packed__)) MaxKey : public Key {
public:
    MaxKey() : Key(MAX_KEY_SIZE, KeyType::TXN_KEY)
    {}

    MaxKey(uint16_t size, KeyType keyType = KeyType::TXN_KEY) : Key(size, keyType)
    {}

    virtual ~MaxKey()
    {}

private:
    /** @var maximal key supported   */
    uint8_t m_actualBuf[MAX_KEY_SIZE];
};
}  // namespace MOT

#endif  // MOT_KEY_H
