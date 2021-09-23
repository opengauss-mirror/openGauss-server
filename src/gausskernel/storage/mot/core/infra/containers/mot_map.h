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
 * mot_map.h
 *    A naive map implementation.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/containers/mot_map.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef MOT_MAP_H
#define MOT_MAP_H

#include "infra.h"
#include "mot_list.h"
#include "utilities.h"
#include "mot_error.h"

namespace MOT {
/**
 * @class mot_map
 * @brief A naive map implementation.
 * @tparam K The key type.
 * @tparam V The value type.
 * @tparam Allocator The allocator used for allocating memory for map management.
 * @tparam KeyAssigner The assigner used to assign and move map keys.
 * @tparam ValueAssigner The assigner used to assign and move map values.
 * @return
 */
template <typename K, typename V, typename Allocator = mot_default_allocator,
    typename KeyAssigner = typename mot_helper<K>::assigner, typename ValueAssigner = typename mot_helper<V>::assigner>
class mot_map {
public:
    /** @typedef The map implementation (currently a simple list of key-value pairs). */
    typedef mot_list<mot_pair<K, V>, Allocator, mot_pair_assigner<K, V, KeyAssigner, ValueAssigner> > map_impl;

private:
    /** @var The key-value map. */
    map_impl _map;

public:
    /** @brief Default constructor. */
    mot_map()
    {}

    /** @brief Destructor. */
    ~mot_map()
    {}

    /** @typedef Map iterator. */
    typedef typename map_impl::iterator iterator;

    /** @typedef Non-modifying iterator. */
    typedef typename map_impl::const_iterator const_iterator;

    /** @typedef The map value type. */
    typedef mot_pair<K, V> value_type;

    /** @typedef Pair of iterator and insert status. */
    typedef mot_pair<iterator, InsertStatus> pairis;

    /**
     * @brief Inserts a key-value pair into the map.
     * @param vt The key-value pair to insert.
     * @return A pair of iterator and insertion status, denoting whether the key-value pair was inserted successfully,
     * and if so, an iterator pointing to the inserted key-value pair. Otherwise, the status may denote the map already
     * contains an existing key-value pair with a matching key, in which case the returned iterator points to the
     * existing key-value pair. Otherwise, the insertion status denotes failure, in which case the returned iterator is
     * invalid. Call @ref MOT::mm_get_last_error() to find out the failure reason.
     */
    pairis insert(const value_type& vt)
    {
        iterator itr = find(vt.first);
        if (itr == end()) {
            typename map_impl::pairib pairib = _map.insert(itr, vt);
            if (pairib.second) {
                return pairis(pairib.first, INSERT_SUCCESS);
            } else {
                return pairis(pairib.first, INSERT_FAILED);
            }
        }
        return pairis(itr, INSERT_EXISTS);
    }

    /**
     * @brief Searches for an item in the map.
     * @param k The key by which the item is to be searched.
     * @return An iterator pointing to the item, or the end iterator if the item was not found.
     */
    iterator find(const K& k)
    {
        iterator itr = begin();
        iterator end_itr = end();
        while (itr != end_itr) {
            if (itr->first == k) {
                break;
            }
            ++itr;
        }
        return itr;
    }

    /**
     * @brief Searches for an item in the map (non-modifying variant).
     * @param k The key by which the item is to be searched.
     * @return An iterator pointing to the item, or the end iterator if the item was not found.
     */
    const_iterator find(const K& k) const
    {
        const_iterator itr = begin();
        const_iterator end_itr = end();
        while (itr != end_itr) {
            if (itr->first == k) {
                break;
            }
            ++itr;
        }
        return itr;
    }

    /** @brief Clears the map for all items. */
    inline void clear()
    {
        _map.clear();
    }

    /**
     * @brief Erases an item from the map.
     * @param pos An iterator pointing to the item to remove.
     * @return iterator The resulting iterator, pointing to the following item (used for erasing while iterating).
     */
    inline iterator erase(const iterator& itr)
    {
        return _map.erase(itr);
    }

    /** @brief Retrieves the number of items stored in the map. */
    inline uint32_t size() const
    {
        return _map.size();
    }

    /** @brief Queries whether the map is empty. */
    inline bool empty() const
    {
        return size() == 0;
    }

    /** @brief Retrieves a modifying iterator pointing to the first item in the map. */
    inline iterator begin()
    {
        return _map.begin();
    }

    /** @brief Retrieves a non-modifying iterator pointing to the first item in the map. */
    inline const_iterator begin() const
    {
        return _map.cbegin();
    }

    /** @brief Retrieves a modifying iterator pointing past the last item in the map. */
    inline iterator end()
    {
        return _map.end();
    }

    /** @brief Retrieves a non-modifying iterator pointing past the last item in the map. */
    inline const_iterator end() const
    {
        return _map.cend();
    }

    /** @brief Retrieves a non-modifying iterator pointing to the first item in the map. */
    inline const_iterator cbegin() const
    {
        return _map.cbegin();
    }

    /** @brief Retrieves a non-modifying iterator pointing past the last item in the map. */
    inline const_iterator cend() const
    {
        return _map.cend();
    }
};
}  // namespace MOT

#endif /* MOT_MAP_H */
