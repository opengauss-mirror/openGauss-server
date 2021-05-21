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
 * dfs_vector.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/utils/dfs_vector.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef VECTOR_H_
#define VECTOR_H_

#include "postgres.h"
#include "securec.h"
#include "securec_check.h"

#include <algorithm>

/* stands for non-int type */
struct __type_true {};

/* stands for int type */
struct __type_false {};

template <typename T>
struct _type_traits {
    typedef __type_false is_POD_type;
};

template <>
struct _type_traits<int> {
    typedef __type_true is_POD_type;
};

/*
 * Afford the dynamic array function.
 */
template <typename T>
class Vector {
public:
    typedef T value_type;
    typedef value_type* iterator;
    typedef typename _type_traits<T>::is_POD_type is_POD;

    /* Return the start iterator. */
    iterator begin()
    {
        if (currentSize == 0) {
            return 0;
        }

        return iterator(vectorHead);
    }

    /* Return the last iterator. */
    iterator end()
    {
        return iterator(vectorHead + currentSize);
    }

    /* constructor */
    Vector(int n = 0) : currentSize(n), currentCapacity(n > VECTOR_INIT_SIZE ? n : VECTOR_INIT_SIZE)
    {
        vectorHead = allocateMemory(is_POD());
    }

    /* constructor */
    Vector(int n, T val) : currentSize(n), currentCapacity(n > VECTOR_INIT_SIZE ? n : VECTOR_INIT_SIZE)
    {
        vectorHead = allocateMemory(is_POD());
        for (int i = 0; i < n; i++)
            vectorHead[i] = val;
    }

    /* constructor */
    Vector(const Vector& b) : currentSize(b.currentSize), currentCapacity(b.currentCapacity)
    {
        errno_t rc = EOK;

        vectorHead = allocateMemory(is_POD());
        if (NULL == vectorHead) {
            elog(ERROR, "memory alloc failed!\n");
        }

        rc = memcpy_s(vectorHead, currentCapacity * sizeof(T), b.vectorHead, currentSize * sizeof(T));
        securec_check(rc, "\0", "\0");
    }

    ~Vector()
    {
        freeVectorHead(is_POD());
    }

    /* Add a element into the vector. */
    void push_back(T val)
    {
        /* Current capacity is enough. */
        if (currentSize < currentCapacity) {
            vectorHead[currentSize++] = val;
        }
        /* Enlarge the capacity of vector. */
        else {
            currentCapacity = currentCapacity + CAPICITY_INCREASE_STEP;
            T* newAllocated = allocateMemory(is_POD());
            errno_t rc = EOK;
            rc = memcpy_s(newAllocated, currentCapacity * sizeof(T), vectorHead, currentSize * sizeof(T));
            securec_check(rc, "\0", "\0");
            newAllocated[currentSize++] = val;
            freeVectorHead(is_POD());
            vectorHead = newAllocated;
        }
    }

    /* Pop off a element from the tail. */
    void pop_back()
    {
        if (currentSize)
            currentSize--;
    }

    /* Return the first one. */
    const T& front() const
    {
        return vectorHead[0];
    }

    /* Return the last one. */
    const T& back() const
    {
        return vectorHead[currentSize - 1];
    }

    /* Implement the array visit operator. */
    const T& operator[](int i) const
    {
        return vectorHead[i];
    }

    /* Implement the array visit operator. */
    T& operator[](int i)
    {
        return vectorHead[i];
    }

    /* Return the size of the current vector. */
    int size()
    {
        return currentSize;
    }

    /* Return the size of the current vector. */
    int size() const
    {
        return currentSize;
    }

    /* Resize the current vector and fill it with the special value. */
    void resize(uint32 new_size, T x)
    {
        /* Current capacity is enough. */
        if (new_size < currentCapacity) {
            if (currentSize < new_size) {
                for (uint32 i = currentSize; i < new_size; i++) {
                    vectorHead[i] = x;
                }
            }
            currentSize = new_size;
        }
        /* Need to enlarge the capacity. */
        else {
            currentCapacity = new_size;
            T* newAllocated = allocateMemory(is_POD());

            if (NULL == newAllocated) {
                elog(ERROR, "memory alloc failed!\n");
            }

            errno_t rc = EOK;

            rc = memcpy_s(newAllocated, currentCapacity * sizeof(T), vectorHead, currentSize * sizeof(T));
            securec_check(rc, "\0", "\0");

            for (uint32 i = currentSize; i < currentCapacity; i++) {
                newAllocated[i] = x;
            }
            freeVectorHead(is_POD());
            vectorHead = newAllocated;
            currentSize = new_size;
        }
    }

    /* Resize the current vector and fill it with NULL. */
    void resize(int new_size)
    {
        resize(new_size, (T)0);
    }

    /* Return the capacity of the current vector. */
    int capacity()
    {
        return currentCapacity;
    }

    /* Implement the equal check operator. */
    bool operator==(const Vector& b) const
    {
        for (int i = 0; i < std::min(currentSize, b.currentSize); i++) {
            if (!(vectorHead[i] == b.vectorHead[i]))
                return false;
        }

        return currentSize == b.currentSize;
    }

    /* Implement the non-equal check operator. */
    bool operator!=(const Vector& b) const
    {
        return !(*this == b);
    }

    /* Clear the dynamic arrays. */
    void clear()
    {
        errno_t errNo = EOK;
        errNo = ::memset_s(vectorHead, sizeof(T) * currentCapacity, 0, sizeof(T) * currentCapacity);
        if (EOK != errNo) {
            elog(ERROR, "Initialize memory failed.");
        }
        currentSize = 0;
    }

    /* Erase the element on the special position. */
    iterator erase(iterator postion)
    {
        if (postion < iterator(vectorHead)) {
            return vectorHead;
        }

        if (postion >= iterator(vectorHead + currentSize)) {
            return vectorHead + currentSize;
        }

        T* succeedor = postion + 1;
        errno_t errNo = EOK;
        errNo = ::memmove_s(postion, end() - succeedor, succeedor, end() - succeedor);
        if (EOK != errNo) {
            elog(ERROR, "Initialize memory failed.");
        }
        currentSize--;

        return succeedor;
    }

private:
    T* allocateMemory(__type_false)
    {
        if (currentCapacity == 0) {
            return 0;
        }
        T* newAllocated = new T[currentCapacity];
        return newAllocated;
    }

    T* allocateMemory(__type_true)
    {
        if (currentCapacity == 0) {
            return 0;
        }
        T* newAllocated = (T*)palloc(currentCapacity * sizeof(T));
        return newAllocated;
    }

    void freeVectorHead(__type_false)
    {
        delete[] vectorHead;
        vectorHead = NULL;
    }
    void freeVectorHead(__type_true)
    {
        pfree(vectorHead);
    }

private:
    enum { VECTOR_INIT_SIZE = 25 };
    enum { CAPICITY_INCREASE_STEP = 15 };

    uint32 currentSize;      // size
    uint32 currentCapacity;  // capacity
    T* vectorHead;           // value array
};

#endif /* VECTOR_H_ */
