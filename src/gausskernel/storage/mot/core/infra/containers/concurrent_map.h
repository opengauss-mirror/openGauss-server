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
 * concurrent_map.h
 *    General purpose concurrent map based on simple read-write lock.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/containers/concurrent_map.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef CONCURRENT_MAP_H
#define CONCURRENT_MAP_H

#include <unordered_map>

// shared mutex is only from C++ 14... we use simple pthread read write lock instead
#include <pthread.h>

namespace MOT {
/**
 * @class ScopedRWLock
 * @brief Helper class for scoping pthread read-write lock (using RAII).
 */
class ScopedRWLock {
private:
    /** @var The lock to scope its usage. */
    pthread_rwlock_t* _rwlock;

public:
    /** @enum Lock mode. */
    enum RWLockMode { RWLockRead, RWLockWrite };

    /**
     * @brief Constructor. Locks the managed lock.
     * @param rwlock The lock to manage.
     * @param lock_mode Specifies whether locking for read or write.
     */
    ScopedRWLock(pthread_rwlock_t* rwlock, RWLockMode lock_mode) : _rwlock(rwlock)
    {
        if (lock_mode == RWLockRead) {
            (void)pthread_rwlock_rdlock(_rwlock);
        } else {
            (void)pthread_rwlock_wrlock(_rwlock);
        }
    }

    /**
     * @brief Destructor. Unlocks the managed lock.
     */
    ~ScopedRWLock()
    {
        (void)pthread_rwlock_unlock(_rwlock);
        _rwlock = nullptr;
    }
};

/**
 * @class ConcurrentMap<id_t, obj_t>
 * @brief General purpose concurrent map based on simple read-write lock.
 */
template <typename id_t, typename obj_t>
class ConcurrentMap {
private:
    /** @typedef Underlying map type. */
    typedef std::unordered_map<id_t, obj_t> obj_map_t;

    /** @typedef Underlying map iterator type. */
    typedef typename obj_map_t::iterator map_itr_t;

    /** @var The underlying map. */
    obj_map_t _obj_map;

    /** @var Read-write lock used to serialize concurrent map access. */
    pthread_rwlock_t _rwlock;

public:
    /** @brief Constructor. */
    ConcurrentMap()
    {
        (void)pthread_rwlock_init(&_rwlock, NULL);
    }

    /** @brief Destruct. */
    ~ConcurrentMap()
    {
        (void)pthread_rwlock_destroy(&_rwlock);
    }

    /**
     * @brief Inserts a key-value pair into the map.
     * @param id The key.
     * @param obj The value.
     * @return True if the insertion was successful or false if an object with the same id is already
     * stored in the map.
     */
    inline bool insert(const id_t& id, obj_t& obj)
    {
        ScopedRWLock _scoped_lock(&_rwlock, ScopedRWLock::RWLockWrite);
        std::pair<map_itr_t, bool> pairib = _obj_map.insert(typename obj_map_t::value_type(id, obj));
        return pairib.second;
    }

    /**
     * @brief Retrieves an value from the map.
     * @param id The key to search.
     * @param[out] obj The resulting mapped value if found.
     * @return True of the searched identifier was found.
     */
    inline bool get(const id_t& id, obj_t* obj)
    {
        ScopedRWLock _scoped_lock(&_rwlock, ScopedRWLock::RWLockRead);
        bool result = false;
        map_itr_t itr = _obj_map.find(id);
        if (itr != _obj_map.end()) {
            *obj = itr->second;
            result = true;
        }
        return result;
    }

    /**
     * @brief Removes a key-value pair from the map.
     * @param id The key to remove.
     * @return True if the key was found and the key-value pair was removed, otherwise false.
     */
    inline bool remove(const id_t& id)
    {
        ScopedRWLock _scoped_lock(&_rwlock, ScopedRWLock::RWLockWrite);
        bool result = false;
        map_itr_t itr = _obj_map.find(id);
        if (itr != _obj_map.end()) {
            (void)_obj_map.erase(itr);
            result = true;
        }
        return result;
    }

    /**
     * @brief Queries whether the map contains a mapping or not.
     * @param id The key to search.
     * @return True if the mapping is found.
     */
    inline bool contains(const id_t& id)
    {
        return (get(id) != nullptr);
    }

    /**
     * @brief Queries for the map size.
     * @return The size of the map.
     */
    inline size_t size()
    {
        ScopedRWLock _scoped_lock(&_rwlock, ScopedRWLock::RWLockRead);
        return _obj_map.size();
    }

    /**
     * @brief Queries if the map is empty.
     * @return True if the map is empty.
     */
    inline bool empty()
    {
        ScopedRWLock _scoped_lock(&_rwlock, ScopedRWLock::RWLockRead);
        return _obj_map.empty();
    }

    /** @typedef functor type used in thread-safe visiting the container elements. */
    typedef void (*functor)(id_t, obj_t);

    /** @brief Thread-safe visiting of the container elements. */
    inline void for_each(functor f)
    {
        ScopedRWLock _scoped_lock(&_rwlock, ScopedRWLock::RWLockRead);
        map_itr_t itr = _obj_map.begin();
        while (itr != _obj_map.end()) {
            f(itr->first, itr->second);
            ++itr;
        }
    }
};
};  // namespace MOT

#endif /* CONCURRENT_MAP_H */
