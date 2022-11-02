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
 * knl_globalsysdbcache.h
 *
 *
 *
 * IDENTIFICATION
 *        src/include/utils/knl_globalsysdbcache.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef KNL_GLOBALDBSTATMANAGER_H
#define KNL_GLOBALDBSTATMANAGER_H
#include "utils/knl_globalsyscache_common.h"
#include "utils/atomic.h"
#include "nodes/memnodes.h"
#include "utils/knl_globalsystabcache.h"
#include "utils/knl_globaltabdefcache.h"
#include "utils/knl_globalpartdefcache.h"
#include "utils/knl_globalrelmapcache.h"

const Oid ALL_DB_OID = (Oid)-1;
const Oid ALL_REL_OID = (Oid)-1;
const Index ALL_DB_INDEX = (Index)-1;
const Index INVALID_INDEX = (Index)-2;
const int INVALID_LOCATION = (int)-2;

/*
 * Data struct to describe global DB objects statistic info, normally support for GSC
 * memory swap algorithm
 */
typedef struct GlobalSysCacheStat {
    Oid              db_oid;
    Index            hash_index;
    pg_atomic_uint64 tup_searches;
    pg_atomic_uint64 tup_hits;
    pg_atomic_uint64 tup_newloads;
    pg_atomic_uint64 rel_searches;
    pg_atomic_uint64 rel_hits;
    pg_atomic_uint64 rel_newloads;
    pg_atomic_uint64 part_searches;
    pg_atomic_uint64 part_hits;
    pg_atomic_uint64 part_newloads;
    uint64 swapout_count;
    void CleanStat()
    {
        tup_searches = 0;
        tup_hits = 0;
        tup_newloads = 0;
        rel_searches = 0;
        rel_hits = 0;
        rel_newloads = 0;
        part_searches = 0;
        part_hits = 0;
        part_newloads = 0;
        swapout_count = 0;
    }
} GlobalSysCacheStat;

/*
 * Data Structure to describe catalog table's statistic info in current DB, normally
 * indicate each cached "heap tuple" in catlog tables of current database object
 */
typedef struct GlobalCatalogTupleStat {
    /* database oid & name(in datum) for current GSC catalog sys-tupe */
    Oid             db_oid;
    Datum           db_name_datum;

    /* rel oid & name(in datum) for current GSC catalog sys-tupe */
    Oid             rel_oid;
    Datum           rel_name_datum;

    int             cache_id;

    /* catalog tuple header info */
    ItemPointerData self;
    ItemPointerData ctid;
    uint16          infomask;
    uint16          infomask2;
    uint64          hash_value;

    uint64          refcount;
} GlobalCatalogTupleStat;

/*
 * noen-catalog table's stat
 */
struct GlobalCatalogTableStat{
    Oid db_id;
    Datum db_name;
    Oid rel_id;
    Form_pg_class rd_rel;
    TupleDesc rd_att;
};
/*
 * Global handler to each DB object info, where its underlaying SysTabCache, TabDefCache,
 * PartDefCache, RelMapDef handlers is avaialble via pointer
 *
 * Stored as an element of GlobalBucketList(HT TODO will change), visible all globally
 */
typedef struct GlobalSysDBCacheEntry {
    /*
     * dbOid/dbName of current database handler
     */
    Oid   m_dbOid;
    char *m_dbName;

    /*
     * flag indicating if current DB entry is spilled out, if it is true, we need
     * fetch its newest version by GlobalSysDBCache::GetGSCEntry(dbOid, dbName)
     */
    bool m_isDead;

    /* hash value of dbOid in GlobalBucketList */
    uint32 m_hash_value;

    /* bucket index of GlobalBucketList's internal buckets array */
    Index m_hash_index;

    /* num of memory context in current DB info entry */
    uint32 m_memcxt_nums;

    /* index of memory context to supoort GetRandomMemCxt()'s random fetch */
    pg_atomic_uint32 m_memcxt_index;

    /*
     * pointer of GSC hanlders in current DB, including CatCache, RelCache, PartCache, RelMapCache
     */
    GlobalSysTabCache  *m_systabCache;
    GlobalTabDefCache  *m_tabdefCache;
    GlobalPartDefCache *m_partdefCache;
    GlobalRelMapCache  *m_relmapCache;

    /* reference count, normally used in GSC memory-spilling algorithm */
    pg_atomic_uint64 m_refcount;

    /* pointer of current DB element in GlobalBucketList */
    Dlelem  m_cache_elem;

    /* pointer of GSC stats info of current database */
    GlobalSysCacheStat *m_dbstat;

    /* a rough estimate of memory context's used space */
    pg_atomic_uint64 m_rough_used_space;

    /* array of memory context hold by current database */
    MemoryContext m_mem_cxt_groups[FLEXIBLE_ARRAY_MEMBER];

    MemoryContext GetRandomMemCxt()
    {
        uint64 index = pg_atomic_fetch_add_u32(&m_memcxt_index, 1);
        return m_mem_cxt_groups[(index & (m_memcxt_nums -1 ))];
    }

    uint64 GetDBTotalSpace()
    {
        Size total_space = 0;
        for (uint32 i = 0; i < m_memcxt_nums; i++) {
            total_space += ((AllocSet)m_mem_cxt_groups[i])->totalSpace;
        }
        return total_space + m_tabdefCache->GetSysCacheSpaceNum();
    }

    uint64 GetDBUsedSpace()
    {
        Size total_space = 0;
        for (uint32 i = 0; i < m_memcxt_nums; i++) {
            total_space += AllocSetContextUsedSpace((AllocSet)m_mem_cxt_groups[i]);
        }
        return total_space + m_tabdefCache->GetSysCacheSpaceNum();
    }

    void MemoryEstimateAdd(uint64 size);
    void MemoryEstimateSub(uint64 size);

    template <bool force> void ResetDBCache();
    void RemoveTailElements();

    static void Free(GlobalSysDBCacheEntry *entry);
    void Release();
} GlobalSysDBCacheEntry;

typedef struct GlobalDBStatManager {
    int m_nbuckets;
    int *m_dbstat_nbuckets;
    List **m_dbstat_buckets;


    uint32 m_max_backend_id;
    GlobalSysDBCacheEntry **m_mydb_refs; // record all threads' ref
    int *m_mydb_roles;
    pthread_rwlock_t *m_backend_ref_lock;

    MemoryContext m_dbstat_memcxt;

    GlobalDBStatManager();

    void InitDBStat(int nbuckets, MemoryContext top);

    void RecordSwapOutDBEntry(GlobalSysDBCacheEntry *entry);
    void DropDB(Oid db_id, Index hash_index);
    void GetDBStat(GlobalSysCacheStat *db_stat);

    bool IsDBUsedByProc(GlobalSysDBCacheEntry *entry);
    void RepallocThreadEntryArray(Oid backend_id);
    void ThreadHoldDB(GlobalSysDBCacheEntry *db);
} GlobalDBStatManager;

#endif