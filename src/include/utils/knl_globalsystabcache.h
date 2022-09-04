/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
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
 * knl_globalsystabcache.h
 *
 *
 *
 * IDENTIFICATION
 *        src/include/utils/knl_globalsystabcache.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef KNL_GLOBALSYSTABCACHE_H
#define KNL_GLOBALSYSTABCACHE_H
#include "access/skey.h"
#include "utils/knl_globalsyscache_common.h"
#include "utils/atomic.h"
#include "utils/relcache.h"
#include "utils/knl_globalsystupcache.h"
#include "utils/rel.h"

/*
 * GlobalSysTabCache is a database level object, by design it contains table level
 * CatCache of all none-shared catalog, note, shared catalog is not considered as
 * current database
 */
class GlobalSysTabCache : public BaseObject {
public:
    /* Constructor and DeConstructor */
    GlobalSysTabCache(Oid dbOid, bool isShared, struct GlobalSysDBCacheEntry *dbEntry);
    ~GlobalSysTabCache(){}

    /* GlobalSysTabCache's export routines */
    void Init();

    /*
     * Function to return list of tuple stat with given relOid, tuple stat object type
     * is "GlobalCatalogTupleStat"
     */
    List *GetCatalogTupleStats(Oid relOid);

    /*
     * Function to find GloablSysTupCache object with given cache_id
     */
    GlobalSysTupCache *CacheIdGetGlobalSysTupCache(int cache_id);

    /*
     * Reset all underlaying CatCaches, where mark each systup as dead and put them to
     * dead_list, note the mem-free handled by next systup search, see the invoke of
     * GlobalSysTupCache::FreeDeadCls() for details
     */
    template <bool force>
    void ResetCatCaches();

    void RemoveAllTailElements();

    /*
     * Invalid/Reset CatCaches with given cache_id and hash_value, note, only mark systup
     * as "dead" and put to deap sys tuple to "dead-list"
     */
    void InvalidTuples(int cache_id, uint32 hash_value, bool reset);

    /*
     * Return the number of "active elements" in current CatCache
     *
     * Reminding! we may abstract fetching active/dead/space in every sys cache object
     * as an interface, lets do it later
     */
    inline uint64 GetActiveElementsNum()
    {
        return m_tup_count;
    }

    /* Return the number of "dead elements" in current CatCache */
    inline uint64 GetDeadElementsNum()
    {
        uint64 dead_count = 0;
        for (int i = 0; i < SysCacheSize; i++) {
            // shared table is separated from normal db
            if (m_global_systupcaches[i] == NULL) {
                continue;
            }
            dead_count += m_global_systupcaches[i]->GetDeadNum();
        }
        return dead_count;
    }

    /* Return the memory consumption of current */
    inline uint64 GetSysCacheSpaceNum()
    {
        return m_tup_space + (sizeof(GlobalCatCTup) + MAXIMUM_ALIGNOF) * m_tup_count;
    }

private:
    /* oid of current DB for this GlobalSysTabCache object, means its identifier */
    Oid m_dbOid;
    volatile bool m_isInited;

    /*
     * Flag to indicate whether contains shared catalog, true means current GlobalSysTabCache
     * is the special one, by design it contains all shared catalog table (instance level)
     */
    bool m_isShared;

    /*
     * array of GlobalSysTupCache(in table level) and control locks in curernt
     * GlobalSysTabCache(in db level), with length SysCacheSize
     */
    GlobalSysTupCache **m_global_systupcaches;
    pthread_rwlock_t   *m_systab_locks;

    /*
     * Pointer back refers current DB object's "GlobalSysDBCacheEntry" which is held
     * as an array in GlobalSysDBCache
     */
    struct GlobalSysDBCacheEntry *m_dbEntry;

    /* stat info fields */
    volatile uint64 m_tup_count;
    volatile uint64 m_tup_space;
};

#endif