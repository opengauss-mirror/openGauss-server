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
 * mot_list.h
 *    An STL-like list with safe memory management and size limit.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/containers/mot_list.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef MOT_LIST_H
#define MOT_LIST_H

#include "infra.h"
#include "utilities.h"
#include "mot_error.h"

// allow using our iterators with STL algorithms
#include <iterator>

namespace MOT {
/**
 * @class mot_list_element
 * @brief A single list element (doubly-linked).
 * @tparam T The contained item type.
 * @tparam Allocator The allocator used for managing list element memory.
 */
template <typename T, typename Allocator>
struct mot_list_element {
    /** @brief Default constructor. */
    mot_list_element() : _prev(nullptr), _next(nullptr)
    {}

    /** @brief Destructor. */
    ~mot_list_element()
    {
        _prev = nullptr;
        _next = nullptr;
    }

    /** @brief Destroys recursively the element list. */
    void destroy()
    {
        // recursively destroy next pointer
        if (_next) {
            _next->destroy();
            _next = nullptr;
        }
        _prev = nullptr;

        // now destroy this pointer
        this->~mot_list_element();
        Allocator::free(this);
    }

    /** @var The stored item. */
    T _item;

    /** @var The previous list element. */
    mot_list_element* _prev;

    /** @var The next list element. */
    mot_list_element* _next;
};

/**
 * @class mot_list_iterator
 * @brief An iterator over a list of elements.
 * @tparam T The iterated item type.
 * @tparam Allocator The allocator used for managing list element memory.
 */
template <typename T, typename Allocator>
class mot_list_iterator final : public std::iterator<std::bidirectional_iterator_tag, T> {
private:
    /** @var The list element pointer. */
    mot_list_element<T, Allocator>* _itr;

    // allow friend access to mot_list
    template <typename U, typename Al, typename As>
    friend class mot_list;

public:
    /** @brief Copy constructor. */
    mot_list_iterator(const mot_list_iterator& other) : _itr(other._itr)
    {}

    /**
     * @brief Constructs an iterator over a list.
     * @param head The list head.
     */
    mot_list_iterator(mot_list_element<T, Allocator>* head) : _itr(head)
    {}

    /** @brief Equality operator. */
    bool inline operator==(const mot_list_iterator& other) const
    {
        return _itr == other._itr;
    }

    /** @brief Inequality operator. */
    bool inline operator!=(const mot_list_iterator& other) const
    {
        return _itr != other._itr;
    }

    /** @brief Assignment operator. */
    inline mot_list_iterator& operator=(const mot_list_iterator& other)
    {
        // self-assign check
        if (this != &other) {
            _itr = other._itr;
        }
        return *this;
    }

    /** @brief Moves the iterator to the next item (post-increment). */
    inline mot_list_iterator operator++(int dummy)
    {
        mot_list_iterator itr = *this;
        _itr = _itr->_next;
        return itr;
    }

    /** @brief Moves the iterator to the next item (pre-increment). */
    inline mot_list_iterator& operator++()
    {
        _itr = _itr->_next;
        return *this;
    }

    /** @brief Moves the iterator to the previous item (post-decrement). */
    inline mot_list_iterator operator--(int dummy)
    {
        mot_list_iterator itr = *this;
        _itr = _itr->_prev;
        return itr;
    }

    /** @brief Moves the iterator to the previous item (pre-decrement). */
    inline mot_list_iterator& operator--()
    {
        _itr = _itr->_prev;
        return *this;
    }

    /** @brief Dereference operator. */
    inline T& operator*() const
    {
        return _itr->_item;
    }

    /** @brief Member-of-pointer operator. */
    inline T* operator->()
    {
        return &_itr->_item;
    }
};

/**
 * @class mot_list_const_iterator
 * @brief A non-modifying iterator over a list of elements.
 * @tparam T The iterated item type.
 * @tparam Allocator The allocator used for managing list element memory.
 */
template <typename T, typename Allocator>
class mot_list_const_iterator final : public std::iterator<std::bidirectional_iterator_tag, T> {
private:
    /** @var The list element pointer. */
    const mot_list_element<T, Allocator>* _itr;

public:
    /** @brief Copy constructor. */
    mot_list_const_iterator(const mot_list_const_iterator& other) : _itr(other._itr)
    {}

    /**
     * @brief Constructs an iterator over a list.
     * @param head The list head.
     */
    mot_list_const_iterator(const mot_list_element<T, Allocator>* head) : _itr(head)
    {}

    /** @brief Equality operator. */
    bool inline operator==(const mot_list_const_iterator& other) const
    {
        return _itr == other._itr;
    }

    /** @brief Inequality operator. */
    bool inline operator!=(const mot_list_const_iterator& other) const
    {
        return _itr != other._itr;
    }

    /** @brief Assignment operator. */
    inline mot_list_const_iterator& operator=(const mot_list_const_iterator& other)
    {
        if (this != &other) {
            _itr = other._itr;
        }
        return *this;
    }

    /** @brief Postfix increment operator. */
    inline mot_list_const_iterator operator++(int dummy)
    {
        mot_list_const_iterator itr = *this;
        _itr = _itr->_next;
        return itr;
    }

    /** @brief Prefix increment operator. */
    inline mot_list_const_iterator& operator++()
    {
        _itr = _itr->_next;
        return *this;
    }

    /** @brief Postfix decrement operator. */
    inline mot_list_const_iterator operator--(int dummy)
    {
        mot_list_const_iterator itr = *this;
        _itr = _itr->_prev;
        return itr;
    }

    /** @brief Prefix decrement operator. */
    inline mot_list_const_iterator& operator--()
    {
        _itr = _itr->_prev;
        return *this;
    }

    /** @brief Dereference operator. */
    inline const T& operator*() const
    {
        return _itr->_item;
    }

    /** @brief Member-of-pointer operator. */
    inline const T* operator->() const
    {
        return &_itr->_item;
    }
};

/**
 * @brief mot_list
 * @brief An STL-like list with safe memory management and size limit. Since value assignment may fail, due to resource
 * allocation, an assignment abstraction is required, such that value assignment can be verified for success.
 * @tparam T The item type.
 * @tparam Allocator The allocator used for allocating list elements.
 * @tparam Assigner The assigner used to assign and move list items.
 */
template <typename T, typename Allocator = mot_default_allocator, typename Assigner = typename mot_helper<T>::assigner>
class mot_list {
private:
    /** @typedef Single list element type. */
    typedef mot_list_element<T, Allocator> element_type;

    /** @var The list head. */
    element_type* _head;

    /** @var The list tail. */
    element_type* _tail;

    /** @var The length of the list. */
    uint32_t _size;

    /** @var The size limit above which the list cannot grow. */
    uint32_t _size_limit;

public:
    /**
     * @brief Constructs an empty list.
     * @param size_limit The maximum list size.
     */
    mot_list(uint32_t size_limit = DEFAULT_SIZE_LIMIT)
        : _head(nullptr), _tail(nullptr), _size(0), _size_limit(size_limit)
    {}

    /** @brief Destructor. */
    ~mot_list()
    {
        clear();
    }

    /** @typedef List iterator. */
    typedef mot_list_iterator<T, Allocator> iterator;

    /** @typedef Non-modifying list iterator. */
    typedef mot_list_const_iterator<T, Allocator> const_iterator;

    /** @typedef Pair of iterator and boolean. */
    typedef mot_pair<iterator, bool> pairib;

    /**
     * @brief Pushes an item at the front of the list.
     * @param item The item to push.
     * @return bool True if the item was pushed, otherwise false.
     */
    bool push_front(const T& item)
    {
        bool result = false;
        element_type* e = allocate_element(item);
        if (e != nullptr) {
            e->_next = _head;
            if (_head) {
                _head->_prev = e;
            }
            _head = e;
            if (!_tail) {
                _tail = _head;
            }
            ++_size;
            result = true;
        }
        return result;
    }

    /**
     * @brief Pushes an item at the back of the list.
     * @param item The item to push.
     * @return bool True if the item was pushed, otherwise false.
     */
    bool push_back(const T& item)
    {
        bool result = false;
        element_type* e = allocate_element(item);
        if (e != nullptr) {
            if (_tail) {
                _tail->_next = e;
                e->_prev = _tail;
                _tail = e;
            } else {
                _head = _tail = e;
            }
            ++_size;
            result = true;
        }
        return result;
    }

    /**
     * @brief Inserts an item into the list.
     * @param pos The insertion position. The item will be inserted before the item pointed by the iterator.
     * @param item The item to insert.
     * @return A pair of iterator and boolean value, denoting whether the item was inserted successfully, and if so, an
     * iterator pointing to the inserted item. Otherwise, the boolean value is false, in which case the returned
     * iterator is invalid.
     */
    pairib insert(iterator pos, const T& item)
    {
        bool result = false;
        element_type* e = allocate_element(item);
        if (e != nullptr) {
            // link e next-prev
            e->_next = pos._itr;
            if (pos._itr) {  // any but last element
                e->_prev = pos._itr->_prev;
            } else {
                e->_prev = _tail;  // insert after tail
            }
            // link e next backwards
            if (e->_next) {
                e->_next->_prev = e;
            }
            // link e prev forward
            if (e->_prev) {
                e->_prev->_next = e;
            }
            // special case: inserting before head
            if (pos._itr == _head) {
                _head = e;
            }
            // special case: inserting after tail
            if (pos._itr == nullptr) {
                _tail = e;
            }
            ++_size;
            result = true;
        }
        return pairib(iterator(e), result);
    }

    /**
     * @brief Erases an item from the list.
     * @param pos An iterator pointing to the item to remove.
     * @return iterator The resulting iterator, pointing to the following item (used for erasing while iterating).
     */
    iterator erase(iterator pos)
    {
        element_type* next_e = nullptr;
        element_type* e = pos._itr;
        if (e != nullptr) {
            if (e->_next) {
                e->_next->_prev = e->_prev;
            }
            if (e->_prev) {
                e->_prev->_next = e->_next;
            }
            if (e == _head) {
                _head = e->_next;
            }
            if (e == _tail) {
                _tail = e->_prev;
            }
            --_size;
            next_e = e->_next;
            e->~element_type();
            Allocator::free(e);
        }

        return iterator(next_e);
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

    /** @brief Removes all the items from the list. */
    inline void clear() noexcept
    {
        if (_head) {
            _head->destroy();
            _head = nullptr;
        }
        _tail = nullptr;
        _size = 0;
    }

    /** @brief Retrieves a modifiable reference to the first item at the front of the list. */
    inline T& front()
    {
        MOT_INFRA_ASSERT(!empty());
        return _head->_item;
    }

    /** @brief Retrieves a non-modifiable reference to the first item at the front of the list. */
    inline const T& front() const
    {
        MOT_INFRA_ASSERT(!empty());
        return _head->_item;
    }

    /** @brief Retrieves a modifiable reference to the last item at the back of the list. */
    inline T& back()
    {
        MOT_INFRA_ASSERT(!empty());
        return _tail->_item;
    }

    /** @brief Retrieves a non-modifiable reference to the last item at the back of the list. */
    inline const T& back() const
    {
        MOT_INFRA_ASSERT(!empty());
        return _tail->_item;
    }

    /** @brief Retrieves a modifying iterator pointing to the first item in the list. */
    inline iterator begin()
    {
        return iterator(_head);
    }

    /** @brief Retrieves a non-modifying iterator pointing to the first item in the list. */
    inline const_iterator begin() const
    {
        return const_iterator(_head);
    }

    /** @brief Retrieves a modifying iterator pointing past the last item in the list. */
    inline iterator end()
    {
        return iterator(nullptr);
    }

    /** @brief Retrieves a non-modifying iterator pointing past the last item in the list. */
    inline const_iterator end() const
    {
        return const_iterator(nullptr);
    }

    /** @brief Retrieves a non-modifying iterator pointing to the first item in the list. */
    inline const_iterator cbegin() const
    {
        return const_iterator(_head);
    }

    /** @brief Retrieves a non-modifying iterator pointing past the last item in the list. */
    inline const_iterator cend() const
    {
        return const_iterator(nullptr);
    }

    /** @brief Retrieves a modifying iterator pointing to the last item in the list. */
    inline iterator last()
    {
        return iterator(_tail);
    }

    /** @brief Retrieves a non-modifying iterator pointing to the last item in the list. */
    inline const_iterator last() const
    {
        return const_iterator(_tail);
    }

    /** @brief Retrieves a non-modifying iterator pointing to the last item in the list. */
    inline const_iterator clast() const
    {
        return const_iterator(_tail);
    }

private:
    /**
     * @brief Allocates a single element to hold a list item.
     * @param item The item to store in the list.
     * @return element_type* The allocated element, or null if failed.
     */
    element_type* allocate_element(const T& item)
    {
        element_type* result = nullptr;
        if (_size >= _size_limit) {
            mot_infra_report_error(
                MOT_ERROR_RESOURCE_LIMIT, "", "Cannot allocate list item, reached size limit %u", _size_limit);
        } else {
            void* buf = Allocator::allocate(sizeof(element_type));
            if (!buf) {
                mot_infra_report_error(MOT_ERROR_OOM, "", "Failed to allocate list item");
            } else {
                result = new (buf) element_type();
                MOT_ASSERT(result != nullptr);  // in-place allocation cannot fail
                if (!Assigner::assign(result->_item, item)) {
                    mot_infra_report_error(MOT_ERROR_OOM, "", "Failed to assign list item");
                    result->destroy();  // buf already released here
                    result = nullptr;
                }
            }
        }
        return result;
    }

    /** @var The default size limit if none was specified. */
    static constexpr uint32_t DEFAULT_SIZE_LIMIT = 4096;
};
}  // namespace MOT

#endif /* MOT_LIST_H */
