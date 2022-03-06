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
 * knl_globalbasedefcache.h
 *
 *
 *
 * IDENTIFICATION
 *        src/include/utils/knl_globalbasedefcache.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef KNL_GLOBALBASEDEFCACHE_H
#define KNL_GLOBALBASEDEFCACHE_H
#include "nodes/memnodes.h"
#include "postgres.h"
#include "utils/knl_globalsyscache_common.h"
#include "utils/knl_globalsystupcache.h"
#include "utils/palloc.h"

/*
 * GSC's base class, normally as super class for RelCache CatCache
 *  (1). class GlobalTabDefCache for "RelCache" see details in knl_globaltabdefcache.cpp
 *  (2). class GlobalPartDefCache for "PartCache" see details in knl_globalpartdefcache.cpp
 */
class GlobalBaseDefCache : public BaseObject {
public:
    GlobalBaseDefCache(Oid dbOid, bool isShared, struct GlobalSysDBCacheEntry *entry, char relkind);
    virtual ~GlobalBaseDefCache() {}

    /* simple getter/setter routines in GSC base class */

    /*
     * Return the number of "active elements" in current GSC object(RelCache, PartCache)
     * depends on where it inheritend
     */
    inline uint64 GetActiveElementsNum()
    {
        return m_bucket_list.GetActiveElementCount();
    }

    /*
     * Return the number of "dead elements" in current GSC object(RelCache, PartCache)
     * depends on where it inheritend
     */
    inline uint64 GetDeadElementsNum()
    {
        return m_dead_entries.GetLength();
    }

    /*
     * Return the object lock for rel when being inserted into current GSC
     */
    inline pthread_rwlock_t *GetHashValueLock(uint32 hash_value)
    {
        Assert(m_oid_locks != NULL);
        Index hash_index = HASH_INDEX(hash_value, m_nbuckets);
        return m_oid_locks + hash_index;
    }
    template <bool is_relation>
    void RemoveAllTailElements();

protected:
    /* base class initialization funciton */
    void Init(int nbucket);
    void InitHashTable();

    /*
     * Cache entry lookup related functions
     *
     * Note: Normally, besides objOid we also pass in hash_value/hash_index to avoid
     *       hashfunc recalculation, oid here indicates its inheritant class relOid or
     *       partRelOid.
     */

    /* search an entry with given obj_oid and hash_value, */
    GlobalBaseEntry *SearchReadOnly(Oid objOid, uint32 hash_value);

    /* search and entry with given objOid/hash_index*/
    GlobalBaseEntry *FindEntryWithIndex(Oid objOid, Index hash_index, int *location);

    /* check given objOid and hash_index exist */
    bool EntryExist(Oid objOid, Index hash_index);

    /* functions to handle message invalidation */
    template <bool is_relation> void Invalidate(Oid dbOid, Oid objOid);
    template <bool is_relation> void InvalidateRelationNodeListBy(bool (*IsInvalidEntry)(GlobalBaseEntry *));

    /* fucntions to remove/free elem from GSC hashtable */
    template <bool is_relation> void HandleDeadEntry(GlobalBaseEntry *entry); /* remove from hashtable */
    template <bool is_relation> void FreeDeadEntrys(); /* free elem */

    /* function to handle GSC memory swapout */
    template <bool is_relation, bool force> void ResetCaches();
    template <bool is_relation> void RemoveTailElements(Index hash_index);

    /* function to add/remove elem to GSC hashtable */
    template <bool is_relation> void RemoveElemFromBucket(GlobalBaseEntry *base);
    template <bool is_relation> void AddHeadToBucket(Index hash_index, GlobalBaseEntry *base);

    /* GSC Identifier fields */
    Oid  m_db_oid;

    /* GSC status control fields */
    bool m_is_shared;
    bool m_is_inited;
    char m_relkind; /* dev-debug only, no real process so far */

    volatile uint32 *m_is_swappingouts;

    /* GSC statistic information, assigned from GlobalSysCacheStat class */
    volatile uint64 *m_searches;
    volatile uint64 *m_hits;
    volatile uint64 *m_newloads;
    volatile uint64  m_base_space;

    /* GSC container fields */
    GlobalBucketList m_bucket_list;  /* GSC hashtable to hold buckets/elements */
    int m_nbuckets;                  /* GSC hashtable's bucket num, assigned in constructor */
    pthread_rwlock_t *m_obj_locks;   /* GSC internal bucket level locks, type as array
                                        with length m_nbuckets */
    DllistWithLock m_dead_entries;   /* List with elem removed from current GSC */

    pthread_rwlock_t *m_oid_locks;   /* locks for GSC object, partRelOid or relOid, avoid xact
                                        commit thread conflict with other threads */

    /* GSC other fields */
    struct GlobalSysDBCacheEntry *m_db_entry; /* pointer to global DB-level syscache pointer */
};

#endif