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
 * index_base.h
 *    Index iterator interface for all index types.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/storage/index/index_base.h
 *
 * -------------------------------------------------------------------------
 */

#pragma once

#ifndef MOT_INDEX_BASE_H
#define MOT_INDEX_BASE_H

#include <sched.h>
#include <stdint.h>
#include <iostream>
#include <string>

#include "global.h"  // for RC

namespace MOT {
// forward declarations
class Key;
class Row;
class Sentinel;

/**
 * @class Iterator.
 * @brief Index iterator interface for all index types.
 */
class Iterator {
public:
    /**
     * @brief Advances the iterator to the next index item.
     * @return The index after being advanced.
     */
    virtual Iterator* operator++(void) = 0;

    /**
     * @brief Dereference operator.
     * @return Retrieves the currently iterated item.
     */
    virtual void* operator*(void) = 0;

    /**
     * @brief Boolean conversion operator.
     * @return True if the iterator is valid.
     */
    virtual operator bool(void) const = 0;

    /**
     * @brief Invalidates the iterator. Subsequent calls to operator bool will
     * return false. Any other calls besides destroy() are not allowed, and
     * their behavior is undefined in such case.
     */
    virtual void Invalidate(void) = 0;

    /** @brief Reclaims all resources used by the iterator. */
    virtual void Destroy(void) = 0;

    /** @brief Destructor. */
    virtual ~Iterator(void)
    {}
};

/**
 * @class IteratorCRTP<I>
 * @brief Iterator CRTP template-interface for all index iterator type.
 */
template <class I>
class IteratorCRTP : public Iterator {
public:
    /**
     * @brief Advances the iterator to the next index item.
     * @return The index after being advanced.
     * @tparam I Iterator class
     */
    Iterator* operator++(void)
    {
        return static_cast<I*>(this)->operator++();
    }

    /**
     * @brief Dereference operator.
     * @return Retrieves the currently iterated item.
     */
    void* operator*(void)
    {
        return static_cast<I*>(this)->operator*();
    }

    /**
     * @brief Boolean conversion operator.
     * @return True if the iterator is valid.
     */
    operator bool(void) const
    {
        return !static_cast<I const*>(this)->Exhausted();
    }

    /**
     * @brief Invalidates the iterator. Subsequent calls to operator bool will
     * return false. Any other calls besides destroy() are not allowed, and
     * their behavior is undefined in such case.
     */
    void Invalidate(void)
    {
        static_cast<I*>(this)->Invalidate();
    }

    /** @brief Reclaims all resources used by the iterator. */
    void Destroy(void)
    {
        static_cast<I*>(this)->Destroy();
    }

    /** @brief Destructor. */
    virtual ~IteratorCRTP(void)
    {}
};
}  // namespace MOT

#endif  // MOT_INDEX_BASE_H
