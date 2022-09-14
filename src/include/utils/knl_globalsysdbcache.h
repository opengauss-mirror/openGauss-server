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

#ifndef KNL_GLOBALSYSDBCACHE_H
#define KNL_GLOBALSYSDBCACHE_H
#include "access/skey.h"
#include "utils/knl_globaldbstatmanager.h"
#include "utils/relcache.h"


enum DynamicGSCMemoryLevel {
    DynamicGSCMemoryLow = 0,        /* less than 80%  */
    DynamicGSCMemoryHigh,           /* less than 100% */
    DynamicGSCMemoryOver,           /* less than 150% */
    DynamicGSCMemoryOutOfControl    /* more than 150% */
};

enum DynamicHashBucketStrategy {
    DynamicHashBucketDefault,
    DynamicHashBucketHalf,
    DynamicHashBucketQuarter,
    DynamicHashBucketEighth,
    DynamicHashBucketMin
};

enum GscStatDetail {
    GscStatDetailDBInfo,
    GscStatDetailTable,
    GscStatDetailTuple
};

extern bool atomic_compare_exchange_u32(volatile uint32* ptr, uint32 expected, uint32 newval);

/*
 * GlobalSysDBCache is DB instance level object, oversee all database object operations,
 * each database plays as a "DB entry" where its global caching status, handlers to CatCache,
 * RelCache/PartCache are available, more details see "struct GlobalSysDBCacheEntry"
 */
class GlobalSysDBCache : public BaseObject {
public:
    GlobalSysDBCache();

    /*
     * Find temp GSC entry with given dbOid, there are three cases:
     *      case1: return NULL if dbOid does not index to any db object or Cache not loaded
     *      case2: return an entry can use normally
     *
     * Note: for the term "temp GSC entry", we use it for cache invalidation with desired
     *      dbOid and no need to create one if not found
     */
    GlobalSysDBCacheEntry *FindTempGSCEntry(Oid dbOid);
    void ReleaseTempGSCEntry(GlobalSysDBCacheEntry *entry);

    /*
     * Find normal GSC entry with given dbOid, if DB is not loaded, do normal init and
     * return to caller, the real content is build up by LSC access
     */
    GlobalSysDBCacheEntry *GetGSCEntry(Oid dbOid, char *dbName);
    void ReleaseGSCEntry(GlobalSysDBCacheEntry *entry);

    /* Fetch shared GSC entry */
    GlobalSysDBCacheEntry *GetSharedGSCEntry();

    /* Remove the GSC entry from m_bucket */
    void DropDB(Oid dbOid, bool need_clear);

    void Init(MemoryContext parent);

    inline bool HashSearchSharedRelation(Oid relOid)
    {
        if (relOid > (Oid)FirstBootstrapObjectId) {
            return false;
        }
        return m_rel_store_in_shared[relOid];
    }

    inline bool IsCritialForInitSysCache(Oid relOid)
    {
        if (relOid > (Oid)FirstNormalObjectId) {
            return false;
        }
        return m_rel_for_init_syscache[relOid];
    }

    inline bool RelationHasSysCache(Oid relOid)
    {
        if (relOid > (Oid)FirstNormalObjectId) {
            return false;
        }
        return m_syscache_relids[relOid];
    }

    void UpdateGSCConfig(int tmp_global_syscache_threshold)
    {
        Assert(tmp_global_syscache_threshold > 0);
        if (m_global_syscache_threshold == tmp_global_syscache_threshold) {
            return;
        }
        m_global_syscache_threshold = tmp_global_syscache_threshold;
        REAL_GSC_MEMORY_SPACE = ((uint64)m_global_syscache_threshold) << 10;
        SAFETY_GSC_MEMORY_SPACE = (uint64)(REAL_GSC_MEMORY_SPACE * 0.8);
        MAX_GSC_MEMORY_SPACE = (uint64)(REAL_GSC_MEMORY_SPACE * 1.5);
        EXPECT_MAX_DB_COUNT = REAL_GSC_MEMORY_SPACE / (GLOBAL_DB_MEMORY_MIN) + 1;
#ifdef ENABLE_LIET_MODE
        /* since db_rough_used_space is an esitmate of memory, reduce the upperlimit in lite mode
            * 1.5 * 0.7 = 1.05. it is the upperlimit */
        REAL_GSC_MEMORY_SPACE = REAL_GSC_MEMORY_SPACE * 0.7;
        SAFETY_GSC_MEMORY_SPACE = SAFETY_GSC_MEMORY_SPACE * 0.7;
        MAX_GSC_MEMORY_SPACE = MAX_GSC_MEMORY_SPACE * 0.7;
#endif
    }

    inline bool StopInsertGSC()
    {
        return GetGSCUsedMemorySpace() > REAL_GSC_MEMORY_SPACE;
    }

    inline bool MemoryUnderControl()
    {
        return GetGSCUsedMemorySpace() < SAFETY_GSC_MEMORY_SPACE;
    }

    void Refresh();

    List* GetGlobalDBStatDetail(Oid dbOid, Oid relOid, GscStatDetail stat_detail);
    template <bool force>
    void ResetCache(Oid db_id);
    void InvalidAllRelations();
    void InvalidateAllRelationNodeList();

    /* in standby mode, and wal_level is less than hot_standby, then gsc is unusable */
    bool hot_standby;
    void RefreshHotStandby();
    bool recovery_finished;

    DynamicHashBucketStrategy dynamic_hash_bucket_strategy;

    /** a rough estimate of memory context's used space
      * it doesnt include the head of blocks belong to memcxt
      * for example: 1GB memcxt, and block size is 1kb, then there are 1000,000 blocks, and head of block is 64 bytes,
      *              64MB is out of estimating */
    pg_atomic_uint64 db_rough_used_space;
    pg_atomic_uint64 GetGSCUsedMemorySpace()
    {
        return db_rough_used_space + AllocSetContextUsedSpace((AllocSet)m_global_sysdb_mem_cxt);
    }
    void GSCMemThresholdCheck();
private:
    void FreeDeadDBs();
    void HandleDeadDB(GlobalSysDBCacheEntry *exist_db);
    void FixDBName(GlobalSysDBCacheEntry *entry);

    void CalcDynamicHashBucketStrategy();
    DynamicGSCMemoryLevel CalcDynamicGSCMemoryLevel(uint64 total_space);
    void SwapOutGivenDBContent(Index hash_index, Oid db_id, DynamicGSCMemoryLevel mem_level);
    void SwapoutGivenDBInstance(Index hash_index, Oid db_id);

    GlobalSysDBCacheEntry *CreateGSCEntry(Oid dbOid, Index hash_index, char *dbName);
    GlobalSysDBCacheEntry *CreateSharedGSCEntry();
    GlobalSysDBCacheEntry *SearchGSCEntry(Oid dbOid, Index hash_index, char *dbName);
    GlobalSysDBCacheEntry *FindGSCEntryWithoutLock(Oid dbOid, Index hash_index, int *location);

    void InitRelStoreInSharedFlag();
    void InitRelForInitSysCacheFlag();
    void InitSysCacheRelIds();

    void RemoveElemFromBucket(GlobalSysDBCacheEntry *entry);

    void AddHeadToBucket(Index hash_index, GlobalSysDBCacheEntry *entry);

    /* Flag to indicate if inited */
    bool m_is_inited;

    /* Fields to hold shared-catalog of GlobalSysDBCacheEntry */
    GlobalSysDBCacheEntry *m_global_shared_db_entry;

    /*
     * A hash table (key:dbOid) to hold all none-shared GlobalSysDBCacheEntry, more details
     * see class GlobalBucketList's intro
     */
    GlobalBucketList m_bucket_list;

    /* Array to hold each bucket lock and mem-useage with length of m_nbuckets */
    pthread_rwlock_t *m_db_locks;
    int     m_nbuckets;

    /* Fields to hold dbstat manager, used for memory spill out */
    GlobalDBStatManager m_dbstat_manager;

    /* du-list to hold dead db objects, element type is GlobalSysDBCacheEntry */
    DllistWithLock m_dead_dbs;

    /* Global memory control fields */
    MemoryContext m_global_sysdb_mem_cxt;

    /* variables to control memory-swapping for SysDB entry */
    volatile uint32 m_is_memorychecking;
    Index m_swapout_hash_index;

    int m_global_syscache_threshold;

    /* Fields attaching to GUC, not change frequently */
    uint64 REAL_GSC_MEMORY_SPACE;
    uint64 SAFETY_GSC_MEMORY_SPACE;
    uint64 MAX_GSC_MEMORY_SPACE;
    uint64 EXPECT_MAX_DB_COUNT;

    static const int FirstBootstrapObjectId = 10000;
    static const int FirstNormalObjectId = 16384;
    bool m_rel_store_in_shared[FirstBootstrapObjectId];
    bool m_rel_for_init_syscache[FirstNormalObjectId];
    bool m_syscache_relids[FirstNormalObjectId];
    pthread_rwlock_t *m_special_lock;
};

void NotifyGscRecoveryStarted();
void NotifyGscRecoveryFinished();
void NotifyGscSigHup();
void NotifyGscHotStandby();
void NotifyGscPgxcPoolReload();
void NotifyGscDropDB(Oid db_id, bool need_clear);
extern Datum gs_gsc_dbstat_info(PG_FUNCTION_ARGS);
extern Datum gs_gsc_clean(PG_FUNCTION_ARGS);
extern Datum gs_gsc_catalog_detail(PG_FUNCTION_ARGS);
extern Datum gs_gsc_table_detail(PG_FUNCTION_ARGS);
extern int ResizeHashBucket(int origin_nbucket, DynamicHashBucketStrategy strategy);

/* only primary node check recovery, recovery from upgrade may have misorder cacheids */
#define IsPrimaryRecoveryFinished() \
    (g_instance.global_sysdbcache.recovery_finished || t_thrd.postmaster_cxt.HaShmData->current_mode == STANDBY_MODE)
#endif
