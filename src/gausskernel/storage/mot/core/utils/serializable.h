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
 * serializable.h
 *    Serialization interfaces.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/utils/serializable.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef __SERIALIZABLE
#define __SERIALIZABLE

namespace MOT {
// Interface class
class Serializable {
public:
    virtual size_t SerializeSize() = 0;
    virtual void Serialize(char* dataOut) = 0;
    virtual void Deserialize(const char* dataIn) = 0;
    virtual ~Serializable()
    {}
};

// Type serialization
template <typename POD>
class SerializablePOD {
public:
    static size_t SerializeSize(POD str)
    {
        return sizeof(POD);
    }

    static char* Serialize(char* target, POD value)
    {
        errno_t erc = memcpy_s(target, SerializeSize(value), &value, SerializeSize(value));
        securec_check(erc, "\0", "\0");
        return target + SerializeSize(value);
    }

    static char* Deserialize(char* source, POD& target)
    {
        errno_t erc = memcpy_s(&target, SerializeSize(target), source, SerializeSize(target));
        securec_check(erc, "\0", "\0");
        return source + SerializeSize(target);
    }
};

// String serialization
class SerializableSTR {
public:
    static size_t SerializeSize(string str)
    {
        return str.length() + sizeof(size_t);
    }

    static char* Serialize(char* target, string value)
    {
        size_t length = value.length();
        errno_t erc = memcpy_s(target, sizeof(size_t), &length, sizeof(size_t));
        securec_check(erc, "\0", "\0");
        erc = memcpy_s(target + sizeof(size_t), length, value.c_str(), length);
        securec_check(erc, "\0", "\0");
        return target + sizeof(size_t) + length;
    }

    static char* Deserialize(char* source, string& target)
    {
        size_t length;
        errno_t erc = memcpy_s(&length, sizeof(size_t), source, sizeof(size_t));
        securec_check(erc, "\0", "\0");
        target.assign(source + sizeof(size_t), length);
        return source + sizeof(size_t) + length;
    }
};

// Char* serialization
class SerializableCharBuf {
public:
    static size_t SerializeSize(size_t len)
    {
        return sizeof(size_t) + len * sizeof(char);
    }

    static char* Serialize(char* target, char* value, size_t len)
    {
        errno_t erc = memcpy_s(target, sizeof(size_t), &len, sizeof(size_t));
        securec_check(erc, "\0", "\0");
        erc = memcpy_s(target + sizeof(size_t), len, value, len);
        securec_check(erc, "\0", "\0");
        return target + sizeof(size_t) + len;
    }

    static char* Deserialize(char* source, char* target, size_t target_len)
    {
        errno_t erc = memcpy_s(target, target_len, source + sizeof(size_t), target_len);
        securec_check(erc, "\0", "\0");
        return source + sizeof(size_t) + target_len;
    }
};

// Array serialization
template <typename T, size_t N>
class SerializableARR {
public:
    static size_t SerializeSize(const T (&arr)[N])
    {
        return (N * sizeof(T) + sizeof(size_t));
    }
    static char* Serialize(char* target, const T (&arr)[N])
    {
        size_t length = N;
        errno_t erc = memcpy_s(target, sizeof(size_t), &length, sizeof(size_t));
        securec_check(erc, "\0", "\0");
        erc = memcpy_s(target + sizeof(size_t), N * sizeof(T), (char*)arr, N * sizeof(T));
        securec_check(erc, "\0", "\0");
        return target + sizeof(size_t) + N * sizeof(T);
    }
    static char* Deserialize(char* source, T (&arr)[N])
    {
        size_t length;
        errno_t erc = memcpy_s(&length, sizeof(size_t), source, sizeof(size_t));
        securec_check(erc, "\0", "\0");
        erc = memcpy_s((char*)arr, length * sizeof(T), source + sizeof(size_t), length * sizeof(T));
        securec_check(erc, "\0", "\0");
        return source + sizeof(size_t) + (length * sizeof(T));
    }
};
}  // namespace MOT
#endif
