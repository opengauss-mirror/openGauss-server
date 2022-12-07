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
 * mot_vector.h
 *    A simple resize-able vector bound by an upper limit.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/containers/mot_vector.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef MOT_VECTOR_H
#define MOT_VECTOR_H

#include "infra.h"
#include "utilities.h"
#include "mot_error.h"

// allow using our iterators with STL algorithms
#include <iterator>

namespace MOT {
/**
 * @class mot_vector_iterator
 * @brief A modifying iterator over a vector object.
 * @tparam T The iterated item type.
 */
template <typename T>
class mot_vector_iterator : public std::iterator<std::bidirectional_iterator_tag, T> {
private:
    /** @var The iterated vector. */
    T* _vec;

    /** @var The iterated item position. */
    uint32_t _pos;

public:
    /** @brief Copy constructor. */
    mot_vector_iterator(const mot_vector_iterator& other) : _vec(other._vec), _pos(other._pos)
    {}

    /**
     * @brief Constructs a vector iterator from an array.
     * @param vec The item array.
     * @param pos The iterated item position.
     */
    mot_vector_iterator(T* vec, uint32_t pos) : _vec(vec), _pos(pos)
    {}

    /** @brief Equality operator. */
    bool inline operator==(const mot_vector_iterator& other) const
    {
        return _pos == other._pos;
    }

    /** @brief Inequality operator. */
    bool inline operator!=(const mot_vector_iterator& other) const
    {
        return _pos != other._pos;
    }

    /** @brief Moves the iterator to the next item (post-increment). */
    inline mot_vector_iterator operator++(int dummy)
    {
        mot_vector_iterator itr = *this;
        ++_pos;
        return itr;
    }

    /** @brief Moves the iterator to the next item (pre-increment). */
    inline mot_vector_iterator& operator++()
    {
        ++_pos;
        return *this;
    }

    /** @brief Moves the iterator to the previous item (post-decrement). */
    inline mot_vector_iterator operator--(int dummy)
    {
        mot_vector_iterator itr = *this;
        --_pos;
        return itr;
    }

    /** @brief Moves the iterator to the previous item (pre-decrement). */
    inline mot_vector_iterator& operator--()
    {
        --_pos;
        return *this;
    }

    /** @brief Retrieves the position of the iterated item. */
    inline uint32_t get_pos() const
    {
        return _pos;
    }

    /** @brief Dereference operator. */
    inline T& operator*()
    {
        return _vec[_pos];
    }

    /** @brief Member-of-pointer operator. */
    inline T* operator->()
    {
        return &_vec[_pos];
    }
};

/**
 * @class mot_vector_const_iterator
 * @brief A non-modifying iterator over a vector object.
 * @tparam T The iterated item type.
 */
template <typename T>
class mot_vector_const_iterator : public std::iterator<std::bidirectional_iterator_tag, T> {
private:
    /** @var The iterated vector. */
    const T* _vec;

    /** @var The iterated item position. */
    uint32_t _pos;

public:
    /** @brief Copy constructor. */
    mot_vector_const_iterator(const mot_vector_const_iterator& other) : _vec(other._vec), _pos(other._pos)
    {}

    /**
     * @brief Constructs a vector iterator from an array.
     * @param vec The item array.
     * @param pos The iterated item position.
     */
    mot_vector_const_iterator(const T* vec, uint32_t pos) : _vec(vec), _pos(pos)
    {}

    /** @brief Equality operator. */
    bool inline operator==(const mot_vector_const_iterator& other) const
    {
        return _pos == other._pos;
    }

    /** @brief Inequality operator. */
    bool inline operator!=(const mot_vector_const_iterator& other) const
    {
        return _pos != other._pos;
    }

    /** @brief Moves the iterator to the next item (post-increment). */
    inline mot_vector_const_iterator operator++(int dummy)
    {
        mot_vector_const_iterator itr = *this;
        ++_pos;
        return itr;
    }

    /** @brief Moves the iterator to the next item (pre-increment). */
    inline mot_vector_const_iterator& operator++()
    {
        ++_pos;
        return *this;
    }

    /** @brief Moves the iterator to the previous item (post-decrement). */
    inline mot_vector_const_iterator operator--(int dummy)
    {
        mot_vector_const_iterator itr = *this;
        --_pos;
        return itr;
    }

    /** @brief Moves the iterator to the previous item (pre-decrement). */
    inline mot_vector_const_iterator& operator--()
    {
        --_pos;
        return *this;
    }

    /** @brief Retrieves the position of the iterated item. */
    inline uint32_t get_pos() const
    {
        return _pos;
    }

    /** @brief Dereference operator. */
    inline const T& operator*() const
    {
        return _vec[_pos];
    }

    /** @brief Member-of-pointer operator. */
    inline const T* operator->() const
    {
        return &_vec[_pos];
    }
};

/**
 * @class mot_vector
 * @brief A simple resize-able vector bound by an upper limit.
 * @tparam T The item type.
 * @tparam Allocator The allocator used for allocating vector item buffer.
 * @tparam Assigner The assigner used to assign and move vector items.
 */
template <typename T, typename Allocator = mot_default_allocator, typename Assigner = typename mot_helper<T>::assigner>
class mot_vector {
public:
    /**
     * @brief Default constructor.
     * @param size_limit The upper size limit for the vector.
     * @param capacity The initial capacity for the vector.
     */
    mot_vector(uint32_t size_limit = DEFAULT_SIZE_LIMIT, uint32_t capacity = INIT_CAPACITY)
        : _array(nullptr), _size(0), _capacity(capacity), _size_limit(size_limit)
    {
        // we initialize capacity member to zero on purpose because vector array has not been allocated yet
        if (_capacity > 0) {
            if (!this->realloc(capacity)) {
                _capacity = 0;
            }
        }
    }

    /** @brief Destructor. */
    ~mot_vector() noexcept
    {
        if (_array) {
            for (uint32_t i = 0; i < _size; ++i) {
                _array[i].~T();
            }
            Allocator::free(_array);
        }
    }

    /** @typedef Vector iterator. */
    typedef mot_vector_iterator<T> iterator;

    /** @typedef Non-modifying vector iterator. */
    typedef mot_vector_const_iterator<T> const_iterator;

    /** @typedef Pair of iterator and boolean. */
    typedef mot_pair<iterator, bool> pairib;

    /**
     * @brief Pushes an item at the back of the vector.
     * @param item The item to push.
     * @return bool True if the item was pushed, otherwise false.
     */
    bool push_back(const T& item)
    {
        bool result = true;
        uint32_t new_capacity = _size + 1;
        if (new_capacity > _capacity) {
            result = this->realloc(new_capacity);
        }
        if (result) {
            MOT_INFRA_ASSERT(new_capacity <= _capacity);
            result = Assigner::assign(_array[_size], item);
            if (!result) {
                mot_infra_report_error(MOT_ERROR_INTERNAL, "", "Failed to assign object");
            } else {
                ++_size;
            }
        }
        return result;
    }

    /** @brief Queries for the number of items in the list. */
    inline uint32_t size() const
    {
        return _size;
    }

    /** @brief Queries whether the list is empty. */
    inline bool empty() const
    {
        return size() == 0;
    }

    /** @brief Retrieves a modifiable reference to the item at the specified position. */
    T& at(uint32_t pos)
    {
        MOT_INFRA_ASSERT(pos < _size);
        return _array[pos];
    }

    /** @brief Retrieves a non-modifiable reference to the item at the specified position. */
    const T& at(uint32_t pos) const
    {
        MOT_INFRA_ASSERT(pos < _size);
        return _array[pos];
    }

    /** @brief Removes all the items from the vector. */
    inline void clear()
    {
        // we make sure that removed elements are properly destroyed
        for (uint32_t i = 0; i < _size; ++i) {
            _array[i].~T();
        }
        _size = 0;
    }

    /**
     * @brief Inserts an item into the vector.
     * @param pos The insertion position. The item will be inserted before the item pointed by the iterator.
     * @param item The item to insert.
     * @return A pair of iterator and boolean value, denoting whether the item was inserted successfully, and if so, an
     * iterator pointing to the inserted item. Otherwise, the boolean value is false, in which case the returned
     * iterator is invalid.
     */
    pairib insert(const iterator& pos, const T& item)
    {
        bool result = true;
        uint32_t result_pos = _size;  // end iterator if not inserted
        uint32_t new_capacity = _size + 1;
        if (new_capacity > _capacity) {
            result = this->realloc(new_capacity);
        }
        if (result) {
            MOT_INFRA_ASSERT(new_capacity <= _capacity);
            // move items
            uint32_t items_to_move = _size - pos.get_pos();
            for (uint32_t i = 0; i < items_to_move; ++i) {
                Assigner::move(_array[_size - i], std::move(_array[(_size - i) - 1]));
            }
            result = Assigner::assign(_array[pos.get_pos()], item);
            if (!result) {
                mot_infra_report_error(MOT_ERROR_INTERNAL, "", "Failed to assign object at position %u", pos.get_pos());
            } else {
                result_pos = pos.get_pos();
                ++_size;
            }
        }
        return mot_pair<iterator, bool>(iterator(result_pos), result);
    }

    /**
     * @brief Erases an item from the vector.
     * @param pos An iterator pointing to the item to remove.
     * @return iterator The resulting iterator, pointing to the following item (used for erasing while iterating).
     */
    inline iterator erase(const iterator& pos)
    {
        for (uint32_t i = pos.get_pos(); i < _size - 1; ++i) {
            Assigner::move(_array[i], std::move(_array[i + 1]));
        }
        --_size;
        return iterator(pos.get_pos());
    }

    /** @brief Retrieves a modifiable reference to the item at the specified position. */
    inline T& operator[](uint32_t pos)
    {
        return at(pos);
    }

    /** @brief Retrieves a non-modifiable reference to the item at the specified position. */
    inline const T& operator[](uint32_t pos) const
    {
        return at(pos);
    }

    /** @brief Retrieves a modifying iterator pointing to the first item in the vector. */
    inline iterator begin()
    {
        return iterator(_array, 0);
    }

    /** @brief Retrieves a non-modifying iterator pointing to the first item in the vector. */
    inline const_iterator begin() const
    {
        return const_iterator(_array, 0);
    }

    /** @brief Retrieves a modifying iterator pointing past the last item in the vector. */
    inline iterator end()
    {
        return iterator(_array, _size);
    }

    /** @brief Retrieves a non-modifying iterator pointing past the last item in the vector. */
    inline const_iterator end() const
    {
        return const_iterator(_array, _size);
    }

    /** @brief Retrieves a non-modifying iterator pointing to the first item in the vector. */
    inline const_iterator cbegin() const
    {
        return const_iterator(_array, 0);
    }

    /** @brief Retrieves a non-modifying iterator pointing past the last item in the vector. */
    inline const_iterator cend() const
    {
        return const_iterator(_array, _size);
    }

    /** @brief Retrieves a modifying iterator pointing to the item in the specified position in the vector. */
    inline iterator itr_at(uint32_t pos)
    {
        return iterator(_array, pos);
    }

    /** @brief Retrieves a non-modifying iterator pointing to the item in the specified position in the vector. */
    inline const_iterator citr_at(uint32_t pos) const
    {
        return const_iterator(_array, pos);
    }

private:
    /**
     * @brief Helper function for selecting a new capacity based a given capacity demand.
     * @param new_capacity The minimum required new capacity.
     * @return The selected capacity or @ref INVALID_CAPACITY if failed.
     */
    uint32_t select_capacity(uint32_t new_capacity)
    {
        uint32_t selected_capacity = INVALID_CAPACITY;
        if (_capacity > _size_limit) {
            mot_infra_report_error(MOT_ERROR_RESOURCE_LIMIT,
                "",
                "Cannot increase vector capacity to %u elements: reached size limit %u",
                new_capacity,
                _size_limit);
        } else {
            // align and check with limit
            uint32_t aligned_capacity = ((new_capacity + GROW_SIZE - 1) / GROW_SIZE) * GROW_SIZE;
            if (aligned_capacity > _size_limit) {
                mot_infra_report_error(MOT_ERROR_RESOURCE_LIMIT,
                    "",
                    "Cannot increase vector capacity to %u elements: aligned size %u reached size limit %u",
                    new_capacity,
                    aligned_capacity,
                    _size_limit);
            } else {
                selected_capacity = aligned_capacity;
            }
        }
        return selected_capacity;
    }

    /**
     * @brief Reallocates the vector according to new capacity specification.
     * @param new_capacity The new required capacity.
     * @return True if reallocation succeeded, otherwise false.
     */
    bool realloc(uint32_t new_capacity)
    {
        bool result = false;
        uint32_t selected_capacity = select_capacity(new_capacity);
        if (selected_capacity != INVALID_CAPACITY) {
            size_t alloc_size = selected_capacity * sizeof(T);
            void* new_ptr = Allocator::realloc(_array, _capacity * sizeof(T), (uint32_t)alloc_size);
            if (new_ptr != NULL) {
                _array = (T*)new_ptr;
                // all new elements must be constructed
                for (uint32_t i = _capacity; i < selected_capacity; ++i) {
                    new (_array + i) T();
                }
                _capacity = selected_capacity;
                result = true;
            } else {
                mot_infra_report_error(MOT_ERROR_OOM,
                    "",
                    "Failed to allocate %u bytes for vector while attempting to grow to capacity of %u items "
                    "(originally requested %u items)",
                    alloc_size,
                    selected_capacity,
                    new_capacity);
            }
        }
        return result;
    }

    /** @var The elements array. */
    T* _array;

    /** @var The current size of the array. */
    uint32_t _size;

    /** @var The current capacity of the array. */
    uint32_t _capacity;

    /** @var The fixes size limit of the array. */
    uint32_t _size_limit;

    /** @var Initial capacity. */
    static constexpr uint32_t INIT_CAPACITY = 0;

    /** @var Vector size increases by 64 elements each time it needs to grow. */
    static constexpr uint32_t GROW_SIZE = 64;

    /** @var The default size limit if none was specified. */
    static constexpr uint32_t DEFAULT_SIZE_LIMIT = 4096;

    /** @var Constant denoting invalid capacity. */
    static constexpr uint32_t INVALID_CAPACITY = (uint32_t)-1;
};
}  // namespace MOT

#endif /* MOT_VECTOR_H */
