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
 * gs_vector.h
 *     vector declare and implementation.
 * 
 * 
 * IDENTIFICATION
 *        src/include/gs_policy/gs_vector.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef INCLUDE_UTILS_GS_VECTOR_H_
#define INCLUDE_UTILS_GS_VECTOR_H_

#include "gs_string.h"


namespace gs_stl {
#define MIN_VECTOR_CAPACITY 16
    template<typename T, bool is_sorted = false>
    class gs_vector {
    public:
        typedef T *iterator;
        typedef const T * const_iterator;

        gs_vector(size_t size = 1):m_buff(nullptr), m_len(0), m_capacity(0)
        {
            m_capacity = Max(size, MIN_VECTOR_CAPACITY);
            InitBuff();
        }

        ~gs_vector()
        {
            if ((m_buff) != NULL && (t_thrd.port_cxt.thread_is_exiting == false)) {
                Assert(t_thrd.top_mem_cxt != NULL);
                for (size_t i = 0; i < m_len; ++i) {
                    m_buff[i].~T();
                }
                pfree(m_buff);
                m_buff = nullptr;
            }
        }

        gs_vector(const gs_vector &arg):m_buff(nullptr), m_len(0), m_capacity(0)
        {
            operator =(arg);
        }

        gs_vector &operator =(const gs_vector &arg)
        {
            if (arg.size() > 0) {
                if (m_buff != NULL) {
                    clear();
                    pfree(m_buff);
                }
                m_len = m_capacity = arg.size();
                m_buff = (T*)AllocFunc(sizeof(T) * m_capacity);
                for (size_t i = 0; i < m_len; ++i) {
                    m_buff[i] = arg.m_buff[i];
                }
            } else if (m_buff == NULL) {
                m_len = 0;
                m_capacity = MIN_VECTOR_CAPACITY;
                InitBuff();
            }
            return *this;
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

        void insert(iterator it, const T &arg)
        {
            if ((m_len + 1) >= m_capacity) {
                ReallocFunc();
            }
            size_t moved_len = m_len - (it - begin());
            size_t remain_len = m_capacity - (it - begin()) - 1;
            errno_t rc = memmove_s((void*)(it + 1), remain_len * sizeof(T), (const void*)it, moved_len * sizeof(T));
            securec_check(rc, "\0", "\0");
            new(static_cast<void*>(it))T(arg);
            ++m_len;
        }

        void insert(const_iterator it, const_iterator eit)
        {
            for (; it != eit; ++it) {
                push_back(*it);
            }
        }

        /* add an element into the vector (at end) */
        void push_back(const T& arg)
        {
            if ((m_len + 1) >= m_capacity) {
                ReallocFunc();
            }
            if (is_sorted) {
                if (find(arg) != end()) {
                    return;
                }
                for (size_t i = 0; i < m_len; ++i) {
                    if (m_buff[i] < arg) {
                        iterator it(m_buff + i);
                        errno_t rc = memmove_s((void*)(it + 1), (m_capacity - i - 1) * sizeof(T),
                                               (const void*)it, (m_len - i) * sizeof(T));
                        securec_check(rc, "\0", "\0");
                        new(static_cast<void*>(m_buff + i))T(arg);
                        ++m_len;
                        return;
                    }
                }
            }

            new(static_cast<void*>(m_buff + m_len))T(arg);
            ++m_len;
        }

        /* Return the first item */
        T &front()
        {
            return m_buff[0];
        }

        /* return the last item */
        T &back()
        {
            if (m_len > 0) {
                return m_buff[m_len - 1];
            }
            return m_buff[0];
        }

        const_iterator find(const T &arg) const
        {
            if (is_sorted) {
                if (m_len < 10) {
                    for (size_t i = 0; i < m_len; ++i) {
                        if (m_buff[i] < arg) {
                            return end();
                        }
                        if (arg < m_buff[i]) {
                            continue;
                        }
                        return const_iterator(m_buff + i);
                    }
                } else {
                    return binary_search(arg);
                }
            } else {
                Assert(false);
            }
            return end();
        }

        bool has_intersect(const gs_vector &arg) const
        {
            iterator it = begin();
            iterator eit = end();
            for (; it != eit; ++it) {
                if (arg.find(*it) != arg.end()) {
                    return true;
                }
            }
            return false;
        }

        void pop_front()
        {
            if (m_len > 0) {
                --m_len;
                m_buff[0].~T();
                if (m_len > 0) {
                    errno_t rc = memmove_s((void*)m_buff, m_capacity * sizeof(T),
                                           (const void*)(m_buff + 1), m_len * sizeof(T));
                    securec_check(rc, "\0", "\0");
                }
            }
        }

        void pop_back()
        {
            if (m_len > 0) {
                --m_len;
                m_buff[m_len].~T();
            }
        }

        T &operator[](size_t idx)
        {
            if (idx < m_len) {
                return m_buff[idx];
            }
            Assert(false);
            return m_buff[0];
        }

        const T &operator[](size_t idx) const
        {
            if (idx < m_len) {
                return m_buff[idx];
            }
            Assert(false);
            return m_buff[0];
        }

        void clear()
        {
            for (size_t i = 0; i < m_len; ++i) {
                m_buff[i].~T();
            }
            m_len = 0;
        }

        void swap(gs_vector& arg)
        {
            m_len = arg.size();
            m_capacity = arg.capacity();
            std::swap(arg.m_buff, m_buff);
        }

        iterator begin()
        {
            return iterator(m_buff);
        }

        iterator end()
        {
            return iterator(m_buff + m_len);
        }

        const iterator begin() const
        {
            return iterator(m_buff);
        }

        const iterator end() const
        {
            return iterator(m_buff + m_len);
        }
    private:

        const_iterator binary_search(const T& arg) const
        {
            size_t start = 0;
            size_t mid = m_len / 2;
            size_t up = m_len;
            while (start < up) {
                if (m_buff[start] < arg) {
                    return end();
                }
                if (arg < m_buff[start]) {
                    ++start;
                    if (start == m_len) {
                        return end();
                    }
                    if (m_buff[mid] < arg) {
                        start = mid + 1;
                        mid = (up - start) / 2;
                    } else if (arg < m_buff[mid]) {
                        up = mid - 1;
                        mid = (up - start) / 2;
                    } else {
                        return const_iterator(m_buff + mid);
                    }
                } else {
                    return const_iterator(m_buff + start);
                }
            }
            return end();
        }

        inline bool InitBuff()
        {
            if (m_buff == NULL && m_capacity > 0) {
                m_buff = (T*)AllocFunc(sizeof(T) * m_capacity);
                return true;
            }
            return false;
        }
        inline void *AllocFunc(size_t _size)
        {
            return MemoryContextAllocZero(GetVectorMemory(), _size);
        }

        inline void ReallocFunc()
        {
            m_capacity += MIN_VECTOR_CAPACITY;
            /* allocate a new array */
            T *buff = (T*)AllocFunc(sizeof(T) * m_capacity);
            /* copy old data */
            errno_t rc = memcpy_s((void*)buff, sizeof(T) * m_capacity, (void*)m_buff, m_len * sizeof(T));
            securec_check(rc, "\0", "\0");
            pfree(m_buff);
            m_buff = buff;
        }
        T *m_buff;
        size_t m_len;
        size_t m_capacity;
    };
}

#endif /* INCLUDE_UTILS_GS_VECTOR_H_ */
