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
 * mot_set.h
 *    A naive set implementation.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/containers/mot_set.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef MOT_SET_H
#define MOT_SET_H

#include "infra.h"
#include "mot_list.h"
#include "utilities.h"
#include "mot_error.h"

namespace MOT {
/**
 * @class mot_set
 * @brief A Naive set implementation.
 * @tparam T The item type.
 * @tparam Allocator The allocator used for allocating memory for set management.
 * @tparam Assigner The assigner used to assign and move set items.
 */
template <typename T, typename Allocator = mot_default_allocator, typename Assigner = typename mot_helper<T>::assigner>
class mot_set {
public:
    /** @typedef The set implementation (currently a simple list of items). */
    typedef mot_list<T, Allocator, Assigner> set_impl;

private:
    /** @var The set of items. */
    set_impl _set;

public:
    /** @brief Consturctor. */
    mot_set()
    {}

    /** @brief Destructor. */
    ~mot_set()
    {}

    /** @typedef Set iterator. */
    typedef typename set_impl::iterator iterator;

    /** @typedef Non-modifying set iterator. */
    typedef typename set_impl::const_iterator const_iterator;

    /** @typedef Pair of iterator and insert status. */
    typedef mot_pair<iterator, InsertStatus> pairis;

    /**
     * @brief Inserts an item into the set.
     * @param item The item to insert.
     * @return A pair of iterator and insertion status, denoting whether the item was inserted successfully, and
     * if so, an iterator pointing to the inserted item. Otherwise, the status may denote the set already contains such
     * an item, in which case the returned iterator points to the existing item. Otherwise, the insertion status
     * denotes failure, in which case the returned iterator is invalid. Call @ref MOT::mm_get_last_error() to find
     * out the failure reason.
     */
    pairis insert(const T& item)
    {
        iterator itr = find(item);
        if (itr == end()) {
            typename set_impl::pairib pairib = _set.insert(itr, item);
            if (pairib.second) {
                return pairis(pairib.first, INSERT_SUCCESS);
            } else {
                return pairis(pairib.first, INSERT_FAILED);
            }
        }
        return pairis(itr, INSERT_EXISTS);
    }

    /**
     * @brief Searches for an item in the set.
     * @param item The item to search.
     * @return An iterator to the item, or the end iterator if the item was not found.
     */
    iterator find(const T& item)
    {
        iterator itr = begin();
        iterator end_itr = end();
        while (itr != end_itr) {
            if (*itr == item) {
                break;
            }
            ++itr;
        }
        return itr;
    }

    /**
     * @brief Searches for an item in the set (non-modifying variant).
     * @param item The item to search.
     * @return A non-modifying iterator to the item, or the end iterator if the item was not found.
     */
    const_iterator find(const T& item) const
    {
        const_iterator itr = begin();
        const_iterator end_itr = end();
        while (itr != end_itr) {
            if (*itr == item) {
                break;
            }
            ++itr;
        }
        return itr;
    }

    /** @brief Clears the set for all items. */
    inline void clear()
    {
        _set.clear();
    }

    /**
     * @brief Erases an item from the set.
     * @param pos An iterator pointing to the item to remove.
     * @return iterator The resulting iterator, pointing to the following item (used for erasing while iterating).
     */
    inline iterator erase(const iterator& itr)
    {
        return _set.erase(itr);
    }

    /** @brief Retrieves the number of items stored in the set. */
    inline uint32_t size() const
    {
        return _set.size();
    }

    /** @brief Queries whether the set is empty. */
    inline bool empty() const
    {
        return size() == 0;
    }

    /** @brief Retrieves a modifying iterator pointing to the first item in the set. */
    inline iterator begin()
    {
        return _set.begin();
    }

    /** @brief Retrieves a non-modifying iterator pointing to the first item in the set. */
    inline const_iterator begin() const
    {
        return _set.cbegin();
    }

    /** @brief Retrieves a modifying iterator pointing past the last item in the set. */
    inline iterator end()
    {
        return _set.end();
    }

    /** @brief Retrieves a non-modifying iterator pointing past the last item in the set. */
    inline const_iterator end() const
    {
        return _set.cend();
    }

    /** @brief Retrieves a non-modifying iterator pointing to the first item in the set. */
    inline const_iterator cbegin() const
    {
        return _set.cbegin();
    }

    /** @brief Retrieves a non-modifying iterator pointing past the last item in the set. */
    inline const_iterator cend() const
    {
        return _set.cend();
    }
};
}  // namespace MOT

#endif /* MOT_SET_H */
