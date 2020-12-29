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
 * ---------------------------------------------------------------------------------------
 * 
 * gs_string.h
 *     Containers common parts and string declare
 * 
 * 
 * IDENTIFICATION
 *        src/include/gs_policy/gs_string.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef INCLUDE_UTILS_GS_STRING_H_
#define INCLUDE_UTILS_GS_STRING_H_

#include "c.h"
#include "utils/palloc.h"
#include "utils/memutils.h"
#include <boost/functional/hash.hpp>

namespace gs_stl {
    size_t cstr_ihash(const char* arg);
    // map memory
    MemoryContext GetMapMemory();
    void DeleteMapMemory();
    void *_HashMapAllocFunc(Size request);
    // set memory
    MemoryContext GetSetMemory();
    void DeleteSetMemory();
    void *_HashSetAllocFunc(Size request);
    /* vector memory */
    MemoryContext GetVectorMemory();
    void DeleteVectorMemory();
    /* string memory */
    MemoryContext GetStringMemory();
    void DeleteStringMemory();

    /**
     * This class couples together a pair of values, which may be of different types (T1 and T2).
     * The individual values can be accessed through its public members first and second.
     */
    template<class T1, class T2>
    struct Pair {
        Pair(const T1& f, const T2& s) : first(f), second(s) {}
        Pair() : first(T1()), second(T2()) {}

        Pair& operator = (const Pair& arg)
        {
            first     = arg.first;
            second     = arg.second;
            return *this;
        }
        T1 first;
        T2 second;
    };

    template<typename T>
    int defaultCompareKeyFunc(const void *keyA, const void *keyB) 
    {
        return *(T*)keyA - *(T*)keyB;
    }

    // Key is const char*
    int matchStr(const void *key1, const void *key2, Size keysize);

    class gs_string {
    public:
        static const size_t npos = -1;
        gs_string(const char *str = "", size_t len = 0);
        ~gs_string();
        gs_string(const gs_string &arg);
        gs_string &operator =(const gs_string &arg);
        bool operator ==(const gs_string &arg) const;
        bool operator <(const gs_string &arg) const;
        int operator -(const gs_string &arg) const;
        const char *c_str() const 
        {
            return m_buff;
        }
        size_t size() const 
        {
            return m_len;
        }
        size_t capacity() const 
        {
            return m_capacity;
        }
        bool empty() const
        {
            return !size();
        }
        gs_string &append(const char *str, size_t len = 0);
        gs_string &append(const gs_string &str);
        void push_back(char ch);
        void pop_back();
        char operator[](int idx) const;
        void clear();
        size_t find(char arg, size_t start = 0) const;
        char back() const;
        gs_string substr(size_t pos, size_t len) const;
        gs_string &replace(size_t pos, size_t len, const char *s);
        void erase(size_t pos, size_t len);
    private:
        inline char *AllocFunc(size_t _size) const;
        inline char *ReallocFunc(size_t _size);
        inline bool InitBuff(const char *str, size_t len = 0);
        char *m_buff;
        size_t m_len;
        size_t m_capacity;
    };

    int gs_stringCompareKeyFunc(const void *keyA, const void *keyB);
}

#endif /* INCLUDE_UTILS_GS_STRING_H_ */
