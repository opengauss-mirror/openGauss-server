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
 * index_iterator.h
 *    A utility class that implements STL like iterator API over an IndexIterator object.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/storage/index/index_iterator.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef INDEX_ITERATOR_H
#define INDEX_ITERATOR_H

#include "index_defs.h"
#include "sentinel.h"

namespace MOT {
/**
 * @class IndexIterator
 * @brief A utility class that implements STL like iterator API over an IndexIterator object.
 */
class IndexIterator {
public:
    /**
     * @brief Destructor.
     */
    virtual ~IndexIterator()
    {}

    /**
     * @@brief Retrieves the type of the iterator.
     * @return The type of the iterator.
     */
    inline IteratorType GetType() const
    {
        return m_type;
    }

    /**
     * @@brief Queries whether the iterator is bidirectional.
     * @detail Bidirectional iterators support the call to operator--().
     * @return Boolean values specifying whether the iterator is bidirectional or not.
     */
    inline bool IsBidirectional() const
    {
        return m_bid;
    }

    /**
     * @brief Queries whether this iterator is valid. And iterator is said to be valid if it still
     * points to a valid index items and it has not been invalidated due to concurrent modification.
     * @return True if the iterator is valid.
     */
    virtual bool IsValid() const
    {
        return m_valid;
    }

    /**
     * @brief Invalidates the iterator such that subsequent calls to isValid() return true.
     */
    virtual void Invalidate()
    {
        m_valid = false;
    }

    /**
     * @brief Performs any implementation-specific cleanup.
     */
    virtual void Destroy()
    {}

    /**
     * @brief Moves forward the iterator to the next item.
     * @detail Pay attention that a reverse iterator moves to the previous index item in normal
     * forward order.
     */
    virtual void Next() = 0;

    /**
     * @brief Moves backwards the iterator to the previous item.
     * @detail Pay attention that a reverse iterator moves to the next index item in normal forward
     * order.
     */
    virtual void Prev() = 0;

    /**
     * @brief Queries whether this index iterator equals to another.
     * @param rhs The index iterator with which to compare this iterator.
     * @return True if iterators point to the same index item, otherwise false.
     */
    virtual bool Equals(const IndexIterator* rhs) const = 0;

    /**
     * @brief Serializes the iterator into a buffer.
     * @param serializeFunc The serialization function.
     * @param buff The buffer into which the iterator is to be serialized.
     */
    virtual void Serialize(serialize_func_t serializeFunc, unsigned char* buf) const = 0;

    /**
     * @brief De-serializes the iterator from a buffer.
     * @param deserializeFunc The de-serialization function.
     * @param buff The buffer from which the iterator is to be de-serialized.
     */
    virtual void Deserialize(deserialize_func_t deserializeFunc, unsigned char* buf) = 0;

    /**
     * @brief Retrieves the key of the currently iterated item.
     * @return A pointer to the key of the currently iterated item.
     */
    virtual const void* GetKey() const = 0;

    /**
     * @brief Retrieves the row of the currently iterated item.
     * @return A pointer to the row of the currently iterated item.
     */
    virtual Row* GetRow() const = 0;

    /**
     * @brief Retrieves the currently iterated primary sentinel.
     * @return The primary sentinel.
     */
    virtual Sentinel* GetPrimarySentinel() const = 0;

protected:
    /**
     * @brief Constructs a builtin end or pre-begin iterator.
     * @param type The type of the iterator.
     * @param bid Specifies whether the iterator is bidirectional.
     * @param valid Specifies whether the iterator is valid.
     */
    IndexIterator(IteratorType type = IteratorType::ITERATOR_TYPE_FORWARD, bool bid = false, bool valid = true)
        : m_type(type), m_bid(bid), m_valid(valid)
    {}

    /**
     * @brief Move constructor.
     * @param other The moved index iterator.
     */
    IndexIterator(IndexIterator&& other) : m_type(other.m_type), m_bid(other.m_bid), m_valid(other.m_valid)
    {}

    /** @var The iterator type. */
    IteratorType m_type;

    /** @var Specifies whether this is a bidirectional iterator. */
    bool m_bid;

    /** @var Specifies whether this iterator is valid. */
    bool m_valid;
};
}  // namespace MOT

#endif /* INDEX_ITERATOR_H */
