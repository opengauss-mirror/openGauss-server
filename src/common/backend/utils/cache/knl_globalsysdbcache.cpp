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
 */

#include "utils/knl_globalsysdbcache.h"
#include "utils/memutils.h"
#include "utils/builtins.h"
#include "access/xlog.h"
#include "access/multi_redo_api.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_auth_members.h"
#include "catalog/pg_database.h"
#include "catalog/pg_pltemplate.h"
#include "catalog/pg_shdescription.h"
#include "catalog/pg_shdepend.h"
#include "catalog/pg_shseclabel.h"
#include "catalog/pg_auth_history.h"
#include "catalog/pg_user_status.h"
#include "catalog/pgxc_group.h"
#include "catalog/pg_workload_group.h"
#include "catalog/pg_app_workloadgroup_mapping.h"
#include "catalog/gs_global_config.h"
#include "catalog/pg_db_role_setting.h"
#include "catalog/pg_job.h"
#include "catalog/pg_job_proc.h"
#include "catalog/pg_extension_data_source.h"
#include "catalog/gs_obsscaninfo.h"
#include "catalog/indexing.h"
#include "catalog/toasting.h"
#include "catalog/pg_am.h"
#include "postgres.h"
#include "executor/executor.h"
#include "utils/knl_globalsystupcache.h"
#include "utils/knl_catcache.h"
#include "knl/knl_session.h"
#include "utils/syscache.h"
#include "storage/proc.h"
#include "funcapi.h"
#include "commands/dbcommands.h"

bool atomic_compare_exchange_u32(volatile uint32* ptr, uint32 expected, uint32 newval)
{
    Assert((expected == 0 && newval == 1) || (expected == 1 && newval == 0));
    uint32 current = expected;
    bool ret = pg_atomic_compare_exchange_u32(ptr, &current, newval);
    Assert((ret && current == expected) || (!ret && current != expected));
    Assert(expected == 0 || ret);
    return ret;
}

void TopnLruMoveToFront(Dlelem *e, Dllist *list, pthread_rwlock_t *lock, int location)
{
    if (location <= GLOBAL_BUCKET_DEFAULT_TOP_N) {
        return;
    }
    if (PthreadRWlockTryWrlock(LOCAL_SYSDB_RESOWNER, lock) != 0) {
        return;
    }
    if (DLGetListHdr(e) == list) {
        DLMoveToFront(e);
    }
    PthreadRWlockUnlock(LOCAL_SYSDB_RESOWNER, lock);
}

void GlobalSysDBCache::ReleaseGSCEntry(GlobalSysDBCacheEntry *entry)
{
    Assert(entry != NULL && entry->m_dbOid != InvalidOid);
    Assert(entry->m_hash_index == HASH_INDEX(oid_hash((void *)&(entry->m_dbOid),
        sizeof(Oid)), m_nbuckets));
    Assert(entry->m_refcount > 0);

    /* minus m_refcount */
    pg_atomic_fetch_sub_u64(&entry->m_refcount, 1);
}

void GlobalSysDBCache::RemoveElemFromBucket(GlobalSysDBCacheEntry *entry)
{
    /* shared db never remove */
    Assert(entry->m_dbOid != InvalidOid);
    m_bucket_list.RemoveElemFromBucket(&entry->m_cache_elem);
    m_dbstat_manager.RecordSwapOutDBEntry(entry);
}

void GlobalSysDBCache::AddHeadToBucket(Index hash_index, GlobalSysDBCacheEntry *entry)
{
    m_bucket_list.AddHeadToBucket(hash_index, &entry->m_cache_elem);

    /* add this for hashtable */
    entry->MemoryEstimateAdd(entry->GetDBUsedSpace());
    m_dbstat_manager.ThreadHoldDB(entry);
}

void GlobalSysDBCache::HandleDeadDB(GlobalSysDBCacheEntry *entry)
{
    RemoveElemFromBucket(entry);
    if (entry->m_refcount == 0) {
        m_dead_dbs.AddHead(&entry->m_cache_elem);
    } else {
        m_dead_dbs.AddTail(&entry->m_cache_elem);
    }
}

void GlobalSysDBCache::FreeDeadDBs()
{
    while (m_dead_dbs.GetLength() > 0) {
        Dlelem *elt = m_dead_dbs.RemoveHead();
        if (elt == NULL) {
            break;
        }
        GlobalSysDBCacheEntry *dbEntry = (GlobalSysDBCacheEntry *)DLE_VAL(elt);
        Assert(dbEntry->m_dbOid != InvalidOid);
        /* refcount means ref may leak */
        if (dbEntry->m_refcount != 0) {
            GSC_CACHE1_elog("GlobalSysDBCacheEntry used can not be freed");
            /* clear memory, this proc may exit, and forget to call releasedb */
            dbEntry->ResetDBCache<false>();
            /* we move the active entry to tail of list and let next call free it */
            m_dead_dbs.AddTail(&dbEntry->m_cache_elem);
            break;
        } else {
            /* sub all to delete, make sure no one use the entry */
            dbEntry->MemoryEstimateSub(dbEntry->m_rough_used_space);
            Assert(dbEntry->m_rough_used_space == 0);
            dbEntry->Free(dbEntry);
        }
    }
}

GlobalSysDBCacheEntry *GlobalSysDBCache::FindGSCEntryWithoutLock(Oid db_id, Index hash_index, int *location)
{
    int index = 0;
    for (Dlelem *elt = DLGetHead(m_bucket_list.GetBucket(hash_index)); elt != NULL; elt = DLGetSucc(elt)) {
        index++;
        GlobalSysDBCacheEntry *entry = (GlobalSysDBCacheEntry *)DLE_VAL(elt);
        if (entry->m_dbOid != db_id) {
            continue;
        }
        pg_atomic_fetch_add_u64(&entry->m_refcount, 1);
        *location = index;
        return entry;
    }
    return NULL;
}

GlobalSysDBCacheEntry *GlobalSysDBCache::SearchGSCEntry(Oid db_id, Index hash_index, char *db_name)
{
    int location = INVALID_LOCATION;
    PthreadRWlockRdlock(LOCAL_SYSDB_RESOWNER, &m_db_locks[hash_index]);
    GlobalSysDBCacheEntry *existDbEntry = FindGSCEntryWithoutLock(db_id, hash_index, &location);
    if (existDbEntry != NULL) {
        m_dbstat_manager.ThreadHoldDB(existDbEntry);
    }
    PthreadRWlockUnlock(LOCAL_SYSDB_RESOWNER, &m_db_locks[hash_index]);
    if (existDbEntry != NULL) {
        TopnLruMoveToFront(&existDbEntry->m_cache_elem, m_bucket_list.GetBucket(hash_index), &m_db_locks[hash_index],
            location);
        return existDbEntry;
    }

    /* create existDbEntry is a simple operator, so put the code in the write lock is ok */
    GlobalSysDBCacheEntry *newDbEntry = CreateGSCEntry(db_id, hash_index, db_name);
    PthreadRWlockWrlock(LOCAL_SYSDB_RESOWNER, &m_db_locks[hash_index]);

    /* otherwise other thread insert one db before */
    existDbEntry = FindGSCEntryWithoutLock(db_id, hash_index, &location);
    if (existDbEntry != NULL) {
        m_dbstat_manager.ThreadHoldDB(existDbEntry);
        PthreadRWlockUnlock(LOCAL_SYSDB_RESOWNER, &m_db_locks[hash_index]);
        newDbEntry->Free(newDbEntry);
        return existDbEntry;
    }

    AddHeadToBucket(hash_index, newDbEntry);
    newDbEntry->m_refcount = 1;
    PthreadRWlockUnlock(LOCAL_SYSDB_RESOWNER, &m_db_locks[hash_index]);
    return newDbEntry;
}

GlobalSysDBCacheEntry *GlobalSysDBCache::GetGSCEntry(Oid db_id, char *db_name)
{
    Assert(db_id != InvalidOid);
    uint32 hash_value = oid_hash((void *)&(db_id), sizeof(Oid));
    Index hash_index = HASH_INDEX(hash_value, m_nbuckets);
    GlobalSysDBCacheEntry *entry = SearchGSCEntry(db_id, hash_index, db_name);

    Refresh();
    return entry;
}

GlobalSysDBCacheEntry *GlobalSysDBCache::GetSharedGSCEntry()
{
    if (unlikely(m_global_shared_db_entry->GetDBUsedSpace() > GLOBAL_DB_MEMORY_MAX)) {
        m_global_shared_db_entry->ResetDBCache<false>();
    }

    return m_global_shared_db_entry;
}

void GlobalSysDBCache::ReleaseTempGSCEntry(GlobalSysDBCacheEntry *entry)
{
    ResourceOwner owner = LOCAL_SYSDB_RESOWNER;
    ResourceOwnerForgetGlobalDBEntry(owner, entry);
    pg_atomic_fetch_sub_u64(&entry->m_refcount, 1);
    PthreadRWlockUnlock(owner, &m_db_locks[entry->m_hash_index]);
}

GlobalSysDBCacheEntry *GlobalSysDBCache::FindTempGSCEntry(Oid db_id)
{
    ResourceOwner owner = LOCAL_SYSDB_RESOWNER;
    ResourceOwnerEnlargeGlobalDBEntry(owner);
    uint32 hash_value = oid_hash((void *)&(db_id), sizeof(Oid));
    Index hash_index = HASH_INDEX(hash_value, m_nbuckets);
    int location = INVALID_LOCATION;
    PthreadRWlockRdlock(owner, &m_db_locks[hash_index]);
    GlobalSysDBCacheEntry *exist_db = FindGSCEntryWithoutLock(db_id, hash_index, &location);
    if (exist_db == NULL) {
        PthreadRWlockUnlock(owner, &m_db_locks[hash_index]);
        return NULL;
    }
    Assert(exist_db->m_hash_index == hash_index);
    ResourceOwnerRememberGlobalDBEntry(owner, exist_db);
    return exist_db;
}

void GlobalSysDBCache::DropDB(Oid db_id, bool need_clear)
{
    Assert(db_id != InvalidOid);
    uint32 hash_value = oid_hash((void *)&(db_id), sizeof(Oid));
    Index hash_index = HASH_INDEX(hash_value, m_nbuckets);
    PthreadRWlockWrlock(LOCAL_SYSDB_RESOWNER, &m_db_locks[hash_index]);
    for (Dlelem *elt = DLGetHead(m_bucket_list.GetBucket(hash_index)); elt != NULL; elt = DLGetSucc(elt)) {
        GlobalSysDBCacheEntry *entry = (GlobalSysDBCacheEntry *)DLE_VAL(elt);
        if (entry->m_dbOid != db_id) {
            continue; /* ignore dead entries */
        }
        /* we dont care refcount here, all dropdb caller make sure that no other active backend accesses this db
         * if any new session access dead db, ereport fatal and retry */
        entry->m_isDead = true;
        HandleDeadDB(entry);
        if (need_clear) {
            m_dbstat_manager.DropDB(db_id, hash_index);
        }
        break;
    }
    PthreadRWlockUnlock(LOCAL_SYSDB_RESOWNER, &m_db_locks[hash_index]);
    FreeDeadDBs();
}

static void InitGlobalSysDBCacheEntry(GlobalSysDBCacheEntry *entry, MemoryContext entry_parent, Oid db_id,
    Index hash_index, char *db_name)
{
    entry->m_refcount = 0;
    entry->m_isDead = false;
    entry->m_hash_value = oid_hash((void *)&(db_id), sizeof(Oid));
    entry->m_hash_index = hash_index;
    entry->m_dbOid = db_id;
    entry->m_dbstat =
        (GlobalSysCacheStat *)MemoryContextAllocZero(entry_parent, sizeof(GlobalSysCacheStat));
    entry->m_dbstat->hash_index = hash_index;
    entry->m_dbstat->db_oid = db_id;
    /* the follow code may bring about exception like error report, which will be converted to FATAL */
    const int MAX_CXT_NAME = 100;
    char cxtname[MAX_CXT_NAME];
    error_t rc = sprintf_s(cxtname, MAX_CXT_NAME, "%s_%u", "GlobalSysDBCacheEntryMemCxt", db_id);
    securec_check_ss(rc, "\0", "\0");
    for (uint32 i = 0; i < entry->m_memcxt_nums; i++) {
        entry->m_mem_cxt_groups[i] =
#ifdef ENABLE_LIET_MODE
            AllocSetContextCreate(entry_parent, cxtname, ALLOCSET_SMALL_MINSIZE, ALLOCSET_SMALL_INITSIZE,
                                  ALLOCSET_SMALL_MAXSIZE, SHARED_CONTEXT);
#else
            AllocSetContextCreate(entry_parent, cxtname, ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE,
                                  ALLOCSET_DEFAULT_MAXSIZE, SHARED_CONTEXT);
#endif
    }
    entry->m_dbName = MemoryContextStrdup(entry->m_mem_cxt_groups[0], db_name);

    /* allocate global CatCache memory */
    entry->m_systabCache = New(entry->m_mem_cxt_groups[0]) GlobalSysTabCache(db_id,
        db_id == InvalidOid, entry);
    entry->m_systabCache->Init();

    /* allocate global RelCache memory */
    entry->m_tabdefCache = New(entry->m_mem_cxt_groups[0]) GlobalTabDefCache(db_id,
        db_id == InvalidOid, entry);
    entry->m_tabdefCache->Init();

    /* emptry shared partdef, need refine */
    if (db_id == InvalidOid) {
        entry->m_partdefCache = NULL;
    } else {
        entry->m_partdefCache = New(entry->m_mem_cxt_groups[0]) GlobalPartDefCache(db_id,
            db_id == InvalidOid, entry);
        entry->m_partdefCache->Init();
    }

    /* allocate global RelmapCache */
    entry->m_relmapCache = New(entry->m_mem_cxt_groups[0]) GlobalRelMapCache(db_id,
        db_id == InvalidOid);
    entry->m_relmapCache->Init();
}

/**
  * assign appropriate nums of memory context for one db according to nums of active dbs
  * for high concurrent scene, nums of dbs is small, conflict of palloc is high, so need more nums of memcxts
  * for multidbs scene, conflict of palloc in one db is low, so need less nums of memcxts
  **/
static uint32 GetMemoryContextNum(uint32 active_db_num)
{
    uint32 memcxt_nums;
    if (active_db_num < 16) {
        memcxt_nums = 16;
    } else if (active_db_num < 64) {
        memcxt_nums = 8;
    } else {
        memcxt_nums = 4;
    }
    return memcxt_nums;
}

GlobalSysDBCacheEntry *GlobalSysDBCache::CreateGSCEntry(Oid db_id, Index hash_index, char *db_name)
{
    Assert(hash_index == HASH_INDEX(oid_hash((void *)&(db_id), sizeof(Oid)), m_nbuckets));
    ResourceOwner owner = LOCAL_SYSDB_RESOWNER;
    Assert(t_thrd.lsc_cxt.lsc != NULL);
    Assert(owner != NULL);
    ResourceOwnerEnlargeGlobalDBEntry(owner);
    uint32 memcxt_nums = GetMemoryContextNum(m_bucket_list.GetActiveElementCount());

    GlobalSysDBCacheEntry *entry =
        (GlobalSysDBCacheEntry *)MemoryContextAllocZero(m_global_sysdb_mem_cxt,
        offsetof(GlobalSysDBCacheEntry, m_mem_cxt_groups) + memcxt_nums * sizeof(MemoryContext));
    entry->m_memcxt_nums = memcxt_nums;
    ResourceOwnerRememberGlobalDBEntry(owner, entry);
    InitGlobalSysDBCacheEntry(entry, m_global_sysdb_mem_cxt, db_id, hash_index, db_name);
    ResourceOwnerForgetGlobalDBEntry(owner, entry);
    DLInitElem(&entry->m_cache_elem, (void *)entry);

    GSC_CACHE2_elog("GlobalSysDBCacheEntry Create with db oid %u", db_id);
    return entry;
}

GlobalSysDBCacheEntry *GlobalSysDBCache::CreateSharedGSCEntry()
{
    Oid db_id = InvalidOid;
    uint32 hash_value = oid_hash((void *)&(db_id), sizeof(Oid));
    Index hash_index = HASH_INDEX(hash_value, m_nbuckets);
    uint32 memcxt_nums = GetMemoryContextNum(0);
    GlobalSysDBCacheEntry *entry =
        (GlobalSysDBCacheEntry *)MemoryContextAllocZero(m_global_sysdb_mem_cxt,
        offsetof(GlobalSysDBCacheEntry, m_mem_cxt_groups) + memcxt_nums * sizeof(MemoryContext));
    entry->m_memcxt_nums = memcxt_nums;
    InitGlobalSysDBCacheEntry(entry, m_global_sysdb_mem_cxt, db_id, hash_index, "");

    DLInitElem(&entry->m_cache_elem, (void *)entry);
    entry->MemoryEstimateAdd(entry->GetDBUsedSpace());
    return entry;
}

void GlobalSysDBCache::CalcDynamicHashBucketStrategy()
{
    uint64 expect_max_db_count = EXPECT_MAX_DB_COUNT;
    uint64 rel_db_count = m_bucket_list.GetActiveElementCount();
    if (rel_db_count < expect_max_db_count) {
        dynamic_hash_bucket_strategy = DynamicHashBucketDefault;
        return;
    }

    if (rel_db_count <= (expect_max_db_count << 1)) {
        dynamic_hash_bucket_strategy = DynamicHashBucketHalf;
        return;
    }
    if (rel_db_count <= (expect_max_db_count << 2)) {
        dynamic_hash_bucket_strategy = DynamicHashBucketQuarter;
        return;
    }
    if (rel_db_count <= (expect_max_db_count << 3)) {
        dynamic_hash_bucket_strategy = DynamicHashBucketEighth;
        return;
    }
    dynamic_hash_bucket_strategy = DynamicHashBucketMin;
}

DynamicGSCMemoryLevel GlobalSysDBCache::CalcDynamicGSCMemoryLevel(uint64 total_space)
{
    if (total_space < SAFETY_GSC_MEMORY_SPACE) {
        return DynamicGSCMemoryLow;
    }

    if (total_space < REAL_GSC_MEMORY_SPACE) {
        return DynamicGSCMemoryHigh;
    }

    if (total_space < MAX_GSC_MEMORY_SPACE) {
        return DynamicGSCMemoryOver;
    }
    return DynamicGSCMemoryOutOfControl;
}

void GlobalSysDBCache::GSCMemThresholdCheck()
{
    if (!StopInsertGSC()) {
        return;
    }

    /* only one clean is enough */
    ResourceOwnerEnlargeGlobalIsExclusive(LOCAL_SYSDB_RESOWNER);
    if (!atomic_compare_exchange_u32(&m_is_memorychecking, 0, 1)) {
        return;
    }
    ResourceOwnerRememberGlobalIsExclusive(LOCAL_SYSDB_RESOWNER, &m_is_memorychecking);

    CalcDynamicHashBucketStrategy();
    DynamicGSCMemoryLevel memory_level = CalcDynamicGSCMemoryLevel(GetGSCUsedMemorySpace());

    Index last_hash_index = m_swapout_hash_index;
    switch (memory_level) {
        /* clean all to recycle memory */
        case DynamicGSCMemoryOutOfControl:
            /* swapout once per cycle */
            if (last_hash_index == 0) {
                m_global_shared_db_entry->ResetDBCache<false>();
            }
            while (last_hash_index < (Index)m_nbuckets) {
                SwapoutGivenDBInstance(last_hash_index, ALL_DB_OID);
                SwapOutGivenDBContent(last_hash_index, ALL_DB_OID, memory_level);
                last_hash_index++;
#ifndef ENABLE_LITE_MODE
                if (MemoryUnderControl()) {
                    break;
                }
#endif
            }
            break;
        /* swapout all to recycle memory */
        case DynamicGSCMemoryOver:
            /* fall through */
        
        /* swapout one to recycle memory */
        case DynamicGSCMemoryHigh:
            /* swapout once per cycle */
            if (last_hash_index == 0) {
                m_global_shared_db_entry->RemoveTailElements();
            }
            while (last_hash_index < (Index)m_nbuckets) {
#ifdef ENABLE_LITE_MODE
                SwapoutGivenDBInstance(last_hash_index, ALL_DB_OID);
#endif
                SwapOutGivenDBContent(last_hash_index, ALL_DB_OID, memory_level);
                last_hash_index++;
#ifndef ENABLE_LITE_MODE
                if (MemoryUnderControl()) {
                    break;
                }
#endif
            }
            break;
        case DynamicGSCMemoryLow:
            /* memory is enough, needn't swapout */
            break;
    }
    m_swapout_hash_index = last_hash_index % ((Index)m_nbuckets);

    Assert(m_is_memorychecking == 1);
    atomic_compare_exchange_u32(&m_is_memorychecking, 1, 0);
    ResourceOwnerForgetGlobalIsExclusive(LOCAL_SYSDB_RESOWNER, &m_is_memorychecking);
    FreeDeadDBs();
}

void GlobalSysDBCache::InvalidateAllRelationNodeList()
{
    for (Index hash_index = 0; hash_index < (Index)m_nbuckets; hash_index++) {
        PthreadRWlockRdlock(LOCAL_SYSDB_RESOWNER, &m_db_locks[hash_index]);
        for (Dlelem *elt = DLGetTail(m_bucket_list.GetBucket(hash_index)); elt != NULL;) {
            GlobalSysDBCacheEntry *entry = (GlobalSysDBCacheEntry *)DLE_VAL(elt);
            elt = DLGetPred(elt);
            entry->m_tabdefCache->InvalidateRelationNodeList();
        }
        PthreadRWlockUnlock(LOCAL_SYSDB_RESOWNER, &m_db_locks[hash_index]);
    }
    /* shared relations don't contain locatorinfo */
}

void GlobalSysDBCache::InvalidAllRelations()
{
    for (Index hash_index = 0; hash_index < (Index)m_nbuckets; hash_index++) {
        PthreadRWlockRdlock(LOCAL_SYSDB_RESOWNER, &m_db_locks[hash_index]);
        for (Dlelem *elt = DLGetTail(m_bucket_list.GetBucket(hash_index)); elt != NULL;) {
            GlobalSysDBCacheEntry *entry = (GlobalSysDBCacheEntry *)DLE_VAL(elt);
            elt = DLGetPred(elt);
            entry->m_tabdefCache->ResetRelCaches<true>();
        }
        PthreadRWlockUnlock(LOCAL_SYSDB_RESOWNER, &m_db_locks[hash_index]);
    }
    m_global_shared_db_entry->m_tabdefCache->ResetRelCaches<true>();
}

void GlobalSysDBCache::SwapOutGivenDBContent(Index hash_index, Oid db_id, DynamicGSCMemoryLevel mem_level)
{
    PthreadRWlockRdlock(LOCAL_SYSDB_RESOWNER, &m_db_locks[hash_index]);
    for (Dlelem *elt = DLGetTail(m_bucket_list.GetBucket(hash_index)); elt != NULL;) {
        GlobalSysDBCacheEntry *entry = (GlobalSysDBCacheEntry *)DLE_VAL(elt);
        elt = DLGetPred(elt);
        if (db_id != (Oid)ALL_DB_OID && entry->m_dbOid != db_id) {
            continue;
        }
        if (mem_level == DynamicGSCMemoryOutOfControl) {
            /* we need drop the element, but dbentry is special, we clean it instead */
            entry->ResetDBCache<false>();
        } else if (mem_level == DynamicGSCMemoryOver) {
            entry->RemoveTailElements();
        } else if (mem_level == DynamicGSCMemoryHigh) {
            if (entry->GetDBUsedSpace() > GLOBAL_DB_MEMORY_MAX) {
                entry->RemoveTailElements();
                /* memory is under control, swapout one db is enough */
                break;
            }
        } else {
            Assert(false);
        }
#ifndef ENABLE_LITE_MODE
        /* we are not in gs_gsc_clean, so break when memory is under control */
        if (mem_level != DynamicGSCMemoryOutOfControl && MemoryUnderControl()) {
            break;
        }
#endif

        /* clean given db only */
        if (db_id != (Oid)ALL_DB_OID) {
            break;
        }
    }
    PthreadRWlockUnlock(LOCAL_SYSDB_RESOWNER, &m_db_locks[hash_index]);
}

void GlobalSysDBCache::SwapoutGivenDBInstance(Index hash_index, Oid db_id)
{
    PthreadRWlockWrlock(LOCAL_SYSDB_RESOWNER, &m_db_locks[hash_index]);
    for (Dlelem *elt = DLGetTail(m_bucket_list.GetBucket(hash_index)); elt != NULL;) {
        GlobalSysDBCacheEntry *entry = (GlobalSysDBCacheEntry *)DLE_VAL(elt);
        elt = DLGetPred(elt);
        if (db_id != (Oid)ALL_DB_OID && entry->m_dbOid != db_id) {
            continue;
        }

        if (entry->m_refcount == 0) {
            GSC_CACHE2_elog("GlobalSysDBCacheEntry Weedout with db oid %u", entry->db_id);
            HandleDeadDB(entry);
        }

        /* clean given db only */
        if (db_id != (Oid)ALL_DB_OID) {
            break;
        }
    }
    PthreadRWlockUnlock(LOCAL_SYSDB_RESOWNER, &m_db_locks[hash_index]);
}

void GlobalSysDBCache::InitRelStoreInSharedFlag()
{
    errno_t rc = memset_s(m_rel_store_in_shared, FirstBootstrapObjectId, 0, FirstBootstrapObjectId);
    securec_check(rc, "\0", "\0");
    m_rel_store_in_shared[AuthIdRelationId] = true;
    m_rel_store_in_shared[AuthMemRelationId] = true;
    m_rel_store_in_shared[DatabaseRelationId] = true;
    m_rel_store_in_shared[PLTemplateRelationId] = true;
    m_rel_store_in_shared[SharedDescriptionRelationId] = true;
    m_rel_store_in_shared[SharedDependRelationId] = true;
    m_rel_store_in_shared[SharedSecLabelRelationId] = true;
    m_rel_store_in_shared[TableSpaceRelationId] = true;
    m_rel_store_in_shared[AuthHistoryRelationId] = true;
    m_rel_store_in_shared[UserStatusRelationId] = true;

    m_rel_store_in_shared[PgxcGroupRelationId] = true;
    m_rel_store_in_shared[PgxcNodeRelationId] = true;
    m_rel_store_in_shared[ResourcePoolRelationId] = true;
    m_rel_store_in_shared[WorkloadGroupRelationId] = true;
    m_rel_store_in_shared[AppWorkloadGroupMappingRelationId] = true;
    m_rel_store_in_shared[GsGlobalConfigRelationId] = true;

    m_rel_store_in_shared[DbRoleSettingRelationId] = true;
    m_rel_store_in_shared[PgJobRelationId] = true;
    m_rel_store_in_shared[PgJobProcRelationId] = true;
    m_rel_store_in_shared[DataSourceRelationId] = true;
    m_rel_store_in_shared[GSObsScanInfoRelationId] = true;
    m_rel_store_in_shared[AuthIdRolnameIndexId] = true;
    m_rel_store_in_shared[AuthIdOidIndexId] = true;
    m_rel_store_in_shared[AuthMemRoleMemIndexId] = true;
    m_rel_store_in_shared[AuthMemMemRoleIndexId] = true;
    m_rel_store_in_shared[DatabaseNameIndexId] = true;
    m_rel_store_in_shared[DatabaseOidIndexId] = true;
    m_rel_store_in_shared[PLTemplateNameIndexId] = true;
    m_rel_store_in_shared[SharedDescriptionObjIndexId] = true;
    m_rel_store_in_shared[SharedDependDependerIndexId] = true;
    m_rel_store_in_shared[SharedDependReferenceIndexId] = true;
    m_rel_store_in_shared[SharedSecLabelObjectIndexId] = true;
    m_rel_store_in_shared[TablespaceOidIndexId] = true;
    m_rel_store_in_shared[TablespaceNameIndexId] = true;
    m_rel_store_in_shared[AuthHistoryIndexId] = true;
    m_rel_store_in_shared[AuthHistoryOidIndexId] = true;
    m_rel_store_in_shared[UserStatusRoleidIndexId] = true;
    m_rel_store_in_shared[UserStatusOidIndexId] = true;

    m_rel_store_in_shared[PgxcNodeNodeNameIndexId] = true;
    m_rel_store_in_shared[PgxcNodeNodeNameIndexIdOld] = true;
    m_rel_store_in_shared[PgxcNodeNodeIdIndexId] = true;
    m_rel_store_in_shared[PgxcNodeOidIndexId] = true;
    m_rel_store_in_shared[PgxcGroupGroupNameIndexId] = true;
    m_rel_store_in_shared[PgxcGroupOidIndexId] = true;
    m_rel_store_in_shared[PgxcGroupToastTable] = true;
    m_rel_store_in_shared[PgxcGroupToastIndex] = true;
    m_rel_store_in_shared[ResourcePoolPoolNameIndexId] = true;
    m_rel_store_in_shared[ResourcePoolOidIndexId] = true;
    m_rel_store_in_shared[WorkloadGroupGroupNameIndexId] = true;
    m_rel_store_in_shared[WorkloadGroupOidIndexId] = true;
    m_rel_store_in_shared[AppWorkloadGroupMappingNameIndexId] = true;
    m_rel_store_in_shared[AppWorkloadGroupMappingOidIndexId] = true;

    m_rel_store_in_shared[DbRoleSettingDatidRolidIndexId] = true;
    m_rel_store_in_shared[PgJobOidIndexId] = true;
    m_rel_store_in_shared[PgJobIdIndexId] = true;
    m_rel_store_in_shared[PgJobProcOidIndexId] = true;
    m_rel_store_in_shared[PgJobProcIdIndexId] = true;
    m_rel_store_in_shared[DataSourceOidIndexId] = true;
    m_rel_store_in_shared[DataSourceNameIndexId] = true;
    m_rel_store_in_shared[PgShdescriptionToastTable] = true;
    m_rel_store_in_shared[PgShdescriptionToastIndex] = true;
    m_rel_store_in_shared[PgDbRoleSettingToastTable] = true;
    m_rel_store_in_shared[PgDbRoleSettingToastIndex] = true;

    m_rel_store_in_shared[SubscriptionRelationId] = true;
    m_rel_store_in_shared[SubscriptionObjectIndexId] = true;
    m_rel_store_in_shared[SubscriptionNameIndexId] = true;
    m_rel_store_in_shared[ReplicationOriginRelationId] = true;
    m_rel_store_in_shared[ReplicationOriginIdentIndex] = true;
    m_rel_store_in_shared[ReplicationOriginNameIndex] = true;
}

void GlobalSysDBCache::InitRelForInitSysCacheFlag()
{
    errno_t rc = memset_s(m_rel_for_init_syscache, FirstNormalObjectId, 0, FirstNormalObjectId);
    securec_check(rc, "\0", "\0");
    m_rel_for_init_syscache[ClassOidIndexId] = true;
    m_rel_for_init_syscache[AttributeRelidNumIndexId] = true;
    m_rel_for_init_syscache[IndexRelidIndexId] = true;
    m_rel_for_init_syscache[OpclassOidIndexId] = true;
    m_rel_for_init_syscache[AccessMethodProcedureIndexId] = true;
    m_rel_for_init_syscache[RewriteRelRulenameIndexId] = true;
    m_rel_for_init_syscache[TriggerRelidNameIndexId] = true;
    m_rel_for_init_syscache[DatabaseNameIndexId] = true;
    m_rel_for_init_syscache[DatabaseOidIndexId] = true;
    m_rel_for_init_syscache[AuthIdRolnameIndexId] = true;
    m_rel_for_init_syscache[AuthIdOidIndexId] = true;
    m_rel_for_init_syscache[AuthMemMemRoleIndexId] = true;
    m_rel_for_init_syscache[UserStatusRoleidIndexId] = true;
}

void GlobalSysDBCache::InitSysCacheRelIds()
{
    errno_t rc = memset_s(m_syscache_relids, FirstNormalObjectId, 0, FirstNormalObjectId);
    securec_check(rc, "\0", "\0");
    for (int i = 0; i < SysCacheSize; i++) {
        m_syscache_relids[cacheinfo[i].reloid] = true;
    }
}

/*
 * whenever you change server_mode, call RefreshHotStandby plz
 * we need know current_mode to support gsc feature
 */
void GlobalSysDBCache::RefreshHotStandby()
{
    Assert(EnableGlobalSysCache());
    hot_standby = (t_thrd.postmaster_cxt.HaShmData->current_mode != STANDBY_MODE || (XLogStandbyInfoActive() &&
                   !IsExtremeRedo()));
    if (hot_standby || !m_is_inited) {
        return;
    }
    ResetCache<true>(ALL_DB_OID);
}

static List *GetHashIndexList(Oid db_id, int nbucket)
{
    List *hash_index_list = NIL;
    if (db_id != ALL_DB_OID) {
        uint32 hash_value = oid_hash((void *)&(db_id), sizeof(Oid));
        Index hash_index = HASH_INDEX(hash_value, nbucket);
        hash_index_list = lappend_int(hash_index_list, (int)hash_index);
    } else {
        for (int hash_index = 0; hash_index < nbucket; hash_index++) {
            hash_index_list = lappend_int(hash_index_list, hash_index);
        }
    }
    return hash_index_list;
}

/* @param db_id 0 means clean shared db, >0 means clean given db and shared db, ALL_DB_OID/null means clean all dbs
 * @param force mean reset or swapout */
template <bool force>
void GlobalSysDBCache::ResetCache(Oid db_id)
{
    if (m_global_shared_db_entry != NULL) {
        m_global_shared_db_entry->ResetDBCache<force>();
        if (force && m_global_shared_db_entry->m_dbstat != NULL) {
            m_global_shared_db_entry->m_dbstat->CleanStat();
        }
    }
    if (db_id == InvalidOid) {
        return;
    }

    List *hash_index_list = GetHashIndexList(db_id, m_nbuckets);

    ListCell *lc;
    /* try to delete db entry first */
    foreach(lc, hash_index_list) {
        Index hash_index = lfirst_oid(lc);
        SwapoutGivenDBInstance(hash_index, db_id);
    }
    /* delete failed, clean all */
    foreach(lc, hash_index_list) {
        Index hash_index = lfirst_oid(lc);
        PthreadRWlockRdlock(LOCAL_SYSDB_RESOWNER, &m_db_locks[hash_index]);
        for (Dlelem *elt = DLGetTail(m_bucket_list.GetBucket(hash_index)); elt != NULL;) {
            GlobalSysDBCacheEntry *entry = (GlobalSysDBCacheEntry *)DLE_VAL(elt);
            elt = DLGetPred(elt);
            if (db_id != (Oid)ALL_DB_OID && entry->m_dbOid != db_id) {
                continue;
            }
            entry->ResetDBCache<force>();
            if (force && entry->m_dbstat != NULL) {
                entry->m_dbstat->CleanStat();
            }
            /* clean dbstat info too */
            m_dbstat_manager.DropDB(entry->m_dbOid, hash_index);
            /* clean given db only */
            if (db_id != (Oid)ALL_DB_OID) {
                break;
            }
        }
        PthreadRWlockUnlock(LOCAL_SYSDB_RESOWNER, &m_db_locks[hash_index]);
    }
    FreeDeadDBs();
}

void GlobalSysDBCache::Init(MemoryContext parent)
{
    /* every process should call this func once */
    Assert(!m_is_inited);
    if (!EnableGlobalSysCache()) {
        return;
    }
    Assert(m_global_sysdb_mem_cxt == NULL);
    InitRelStoreInSharedFlag();
    InitRelForInitSysCacheFlag();
    InitSysCacheRelIds();
#ifdef ENABLE_LIET_MODE
    m_global_sysdb_mem_cxt = AllocSetContextCreate(parent, "GlobalSysDBCache", ALLOCSET_SMALL_MINSIZE,
                                                   ALLOCSET_SMALL_INITSIZE, ALLOCSET_SMALL_MAXSIZE, SHARED_CONTEXT);
#else
    m_global_sysdb_mem_cxt = AllocSetContextCreate(parent, "GlobalSysDBCache", ALLOCSET_DEFAULT_MINSIZE,
                                                   ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE, SHARED_CONTEXT);
#endif

    MemoryContext old = MemoryContextSwitchTo(m_global_sysdb_mem_cxt);
    m_nbuckets = INIT_DB_SIZE;
    m_bucket_list.Init(m_nbuckets);
    m_db_locks = (pthread_rwlock_t *)palloc0(sizeof(pthread_rwlock_t) * m_nbuckets);
    for (int i = 0; i < m_nbuckets; i++) {
        PthreadRwLockInit(&m_db_locks[i], NULL);
    }
    m_special_lock = (pthread_rwlock_t *)palloc0(sizeof(pthread_rwlock_t));
    PthreadRwLockInit(m_special_lock, NULL);
    m_dbstat_manager.InitDBStat(m_nbuckets, m_global_sysdb_mem_cxt);
    Assert(g_instance.attr.attr_memory.global_syscache_threshold != 0);
    /* the conf file dont have value of gsc threshold, so we need init it by default value */
    if (m_global_syscache_threshold == 0) {
        UpdateGSCConfig(g_instance.attr.attr_memory.global_syscache_threshold);
    }
    m_global_shared_db_entry = CreateSharedGSCEntry();
    MemoryContextSwitchTo(old);
    dynamic_hash_bucket_strategy = DynamicHashBucketDefault;
    m_is_inited = true;
}

GlobalSysDBCache::GlobalSysDBCache()
{
    m_db_locks = NULL;
    m_global_sysdb_mem_cxt = NULL;
    m_nbuckets = 0;
    m_global_shared_db_entry = NULL;
    m_global_syscache_threshold = 0;
    m_is_memorychecking = 0;
    hot_standby = true;
    recovery_finished = false;
    m_swapout_hash_index = 0;
    m_is_inited = false;
    m_special_lock = NULL;
}

void GlobalSysDBCache::Refresh()
{
    if (unlikely(GetGSCUsedMemorySpace() > MAX_GSC_MEMORY_SPACE)) {
        GSCMemThresholdCheck();
    }
    FreeDeadDBs();
}

void GlobalSysDBCache::FixDBName(GlobalSysDBCacheEntry *entry)
{
    HeapTuple tup = SearchSysCache1(DATABASEOID, ObjectIdGetDatum(entry->m_dbOid));
    if (!HeapTupleIsValid(tup)) {
        return;
    }
    Form_pg_database dbform = (Form_pg_database)GETSTRUCT(tup);
    if (strcmp(entry->m_dbName, NameStr(dbform->datname)) == 0) {
        ReleaseSysCache(tup);
        return;
    }
    char *old_db_name = entry->m_dbName;
    char *new_db_name = MemoryContextStrdup(entry->m_mem_cxt_groups[0], NameStr(dbform->datname));
    PthreadRWlockWrlock(LOCAL_SYSDB_RESOWNER, m_special_lock);
    entry->m_dbName = new_db_name;
    PthreadRWlockUnlock(LOCAL_SYSDB_RESOWNER, m_special_lock);
    ReleaseSysCache(tup);
    pfree_ext(old_db_name);
}

struct GlobalDBStatInfo : GlobalSysCacheStat {
    Datum db_name;
    uint64 tup_count;
    uint64 tup_dead;
    uint64 tup_space;

    uint64 rel_count;
    uint64 rel_dead;
    uint64 rel_space;

    uint64 part_count;
    uint64 part_dead;
    uint64 part_space;
    uint64 total_space;
    uint64 refcount;
};

static GlobalDBStatInfo *ConstructGlobalDBStatInfo(GlobalSysDBCacheEntry *entry, GlobalDBStatManager *dbstat_manager)
{
    GlobalDBStatInfo *statinfo = (GlobalDBStatInfo *)palloc0(sizeof(GlobalDBStatInfo));
    statinfo->db_oid = entry->m_dbOid;
    statinfo->db_name = CStringGetTextDatum(entry->m_dbName);
    statinfo->hash_index = entry->m_hash_index;
    statinfo->swapout_count = 0;
    statinfo->tup_searches = entry->m_dbstat->tup_searches;
    statinfo->tup_hits = entry->m_dbstat->tup_hits;
    statinfo->tup_newloads = entry->m_dbstat->tup_newloads;
    statinfo->tup_count = entry->m_systabCache->GetActiveElementsNum();
    statinfo->tup_dead = entry->m_systabCache->GetDeadElementsNum();
    statinfo->tup_space = entry->m_systabCache->GetSysCacheSpaceNum();

    statinfo->rel_searches = entry->m_dbstat->rel_searches;
    statinfo->rel_hits = entry->m_dbstat->rel_hits;
    statinfo->rel_newloads = entry->m_dbstat->rel_newloads;
    statinfo->rel_count = entry->m_tabdefCache->GetActiveElementsNum();
    statinfo->rel_dead = entry->m_tabdefCache->GetDeadElementsNum();
    statinfo->rel_space = entry->m_tabdefCache->GetSysCacheSpaceNum();
    if (entry->m_partdefCache == NULL) {
        statinfo->part_searches = 0;
        statinfo->part_hits = 0;
        statinfo->part_newloads = 0;
        statinfo->part_count = 0;
        statinfo->part_dead = 0;
        statinfo->part_space = 0;
    } else {
        statinfo->part_searches = entry->m_dbstat->part_searches;
        statinfo->part_hits = entry->m_dbstat->part_hits;
        statinfo->part_newloads = entry->m_dbstat->part_newloads;
        statinfo->part_count = entry->m_partdefCache->GetActiveElementsNum();
        statinfo->part_dead = entry->m_partdefCache->GetDeadElementsNum();
        statinfo->part_space = entry->m_partdefCache->GetSysCacheSpaceNum();
    }
    statinfo->total_space = entry->GetDBUsedSpace();
    statinfo->refcount = entry->m_refcount;
    if (dbstat_manager != NULL) {
        dbstat_manager->GetDBStat((GlobalSysCacheStat *)statinfo);
    }
    return statinfo;
}

/* @param db_id 0 means fetch shared db, >0 means fetch given db and shared db, ALL_DB_OID/null means fetch all dbs
   @param ALL_REL_OID/null means fetch all cache, otherwise fetch given cache */
List *GlobalSysDBCache::GetGlobalDBStatDetail(Oid db_id, Oid rel_id, GscStatDetail stat_detail)
{
    List *stat_list = NIL;
    /* collect stat info of shared db */
    GlobalSysDBCacheEntry *shared = GetSharedGSCEntry();
    if (stat_detail == GscStatDetailDBInfo) {
        stat_list = lappend(stat_list, ConstructGlobalDBStatInfo(shared, NULL));
    } else if (stat_detail == GscStatDetailTuple) {
        stat_list = lappend3(stat_list, shared->m_systabCache->GetCatalogTupleStats(rel_id));
    } else {
        Assert(stat_detail == GscStatDetailTable);
        stat_list = lappend3(stat_list, shared->m_tabdefCache->GetTableStats(rel_id));
    }
    if (db_id == InvalidOid) {
        return stat_list;
    }

    List *given_hash_index_list = GetHashIndexList(db_id, m_nbuckets);

    ListCell *lc;
    /* try to delete db entry first */
    foreach(lc, given_hash_index_list) {
        Index hash_index = lfirst_oid(lc);
        if (DLIsNIL(m_bucket_list.GetBucket(hash_index))) {
            continue;
        }

        CHECK_FOR_INTERRUPTS();
        PthreadRWlockRdlock(LOCAL_SYSDB_RESOWNER, &m_db_locks[hash_index]);
        for (Dlelem *elt = DLGetHead(m_bucket_list.GetBucket(hash_index)); elt != NULL; elt = DLGetSucc(elt)) {
            GlobalSysDBCacheEntry *entry = (GlobalSysDBCacheEntry *)DLE_VAL(elt);
            if (db_id != ALL_DB_OID && db_id != entry->m_dbOid) {
                continue;
            }
            FixDBName(entry);
            if (stat_detail == GscStatDetailDBInfo) {
                stat_list = lappend(stat_list, ConstructGlobalDBStatInfo(entry, &m_dbstat_manager));
            } else if (stat_detail == GscStatDetailTuple) {
                stat_list = lappend3(stat_list, entry->m_systabCache->GetCatalogTupleStats(rel_id));
            } else {
                Assert(stat_detail == GscStatDetailTable);
                stat_list = lappend3(stat_list, entry->m_tabdefCache->GetTableStats(rel_id));
            }
        }
        PthreadRWlockUnlock(LOCAL_SYSDB_RESOWNER, &m_db_locks[hash_index]);
    }
    return stat_list;
}

static void CheckDbOidValid(Oid dbOid)
{
    if (dbOid != InvalidOid && dbOid != ALL_DB_OID &&
        !SearchSysCacheExists(DATABASEOID, ObjectIdGetDatum(dbOid), 0, 0, 0)) {
        ereport(ERROR, (errmodule(MOD_GSC), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("dbOid doesn't exist."),
            errdetail("dbOid is invalid, please pass valid dbOid."), errcause("N/A"),
            erraction("Please search pg_database table to find valid dbOid.")));
    }
}

static const int GLOBAL_STAT_INFO_NUM = 23;
bool gs_gsc_dbstat_firstcall(PG_FUNCTION_ARGS)
{
    if (!superuser()) {
        ereport(ERROR, (errmodule(MOD_GSC), errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            errmsg("Fail to see gsc dbstat info."),
            errdetail("Insufficient privilege to see gsc dbstat info."), errcause("N/A"),
            erraction("Please login in with superuser or sysdba role or contact database administrator.")));
    }
    Oid dbOid = PG_ARGISNULL(0) ? ALL_DB_OID : PG_GETARG_OID(0);
    CheckDbOidValid(dbOid);
    FuncCallContext *funcctx = NULL;
    if (!SRF_IS_FIRSTCALL()) {
        return true;
    }
    funcctx = SRF_FIRSTCALL_INIT();
    MemoryContext oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
    funcctx->tuple_desc = CreateTemplateTupleDesc(GLOBAL_STAT_INFO_NUM, false);
    int i = 1;
    TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)i++, "database_id", INT8OID, -1, 0);
    TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)i++, "database_name", TEXTOID, -1, 0);
    TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)i++, "tup_searches", INT8OID, -1, 0);
    TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)i++, "tup_hit", INT8OID, -1, 0);
    TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)i++, "tup_miss", INT8OID, -1, 0);
    TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)i++, "tup_count", INT8OID, -1, 0);
    TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)i++, "tup_dead", INT8OID, -1, 0);
    TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)i++, "tup_memory", INT8OID, -1, 0);
    TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)i++, "rel_searches", INT8OID, -1, 0);
    TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)i++, "rel_hit", INT8OID, -1, 0);
    TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)i++, "rel_miss", INT8OID, -1, 0);
    TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)i++, "rel_count", INT8OID, -1, 0);
    TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)i++, "rel_dead", INT8OID, -1, 0);
    TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)i++, "rel_memory", INT8OID, -1, 0);
    TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)i++, "part_searches", INT8OID, -1, 0);
    TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)i++, "part_hit", INT8OID, -1, 0);
    TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)i++, "part_miss", INT8OID, -1, 0);
    TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)i++, "part_count", INT8OID, -1, 0);
    TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)i++, "part_dead", INT8OID, -1, 0);
    TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)i++, "part_memory", INT8OID, -1, 0);
    TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)i++, "total_memory", INT8OID, -1, 0);
    TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)i++, "swapout_count", INT8OID, -1, 0);
    TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)i++, "refcount", INT8OID, -1, 0);
    funcctx->tuple_desc = BlessTupleDesc(funcctx->tuple_desc);

    List *tmp_list = NIL;
    if (EnableGlobalSysCache()) {
        tmp_list = g_instance.global_sysdbcache.GetGlobalDBStatDetail(dbOid, ALL_REL_OID, GscStatDetailDBInfo);
    }
    if (list_length(tmp_list) == 0) {
        MemoryContextSwitchTo(oldcontext);
        return false;
    }
    funcctx->max_calls = list_length(tmp_list);
    GlobalDBStatInfo **stat_infos = (GlobalDBStatInfo **)palloc(sizeof(GlobalDBStatInfo *) * funcctx->max_calls);
    MemoryContextSwitchTo(oldcontext);
    funcctx->user_fctx = stat_infos;
    ListCell *cell = NULL;
    foreach (cell, tmp_list) {
        *stat_infos = (GlobalDBStatInfo *)lfirst(cell);
        stat_infos++;
    }
    list_free_ext(tmp_list);
    return true;
}

Datum gs_gsc_dbstat_info(PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx = NULL;
    if (!gs_gsc_dbstat_firstcall(fcinfo)) {
        SRF_RETURN_DONE(funcctx);
    }

    funcctx = SRF_PERCALL_SETUP();
    if (funcctx->call_cntr >= funcctx->max_calls) {
        SRF_RETURN_DONE(funcctx);
    }
    Datum values[GLOBAL_STAT_INFO_NUM];
    bool nulls[GLOBAL_STAT_INFO_NUM] = {0};
    GlobalDBStatInfo *stat_info = ((GlobalDBStatInfo **)funcctx->user_fctx)[funcctx->call_cntr];
    int i = 0;
    values[i++] = Int64GetDatum(stat_info->db_oid);
    values[i++] = stat_info->db_name;

    values[i++] = Int64GetDatum((int64)stat_info->tup_searches);
    values[i++] = Int64GetDatum((int64)stat_info->tup_hits);
    values[i++] = Int64GetDatum((int64)stat_info->tup_newloads);
    values[i++] = Int64GetDatum((int64)stat_info->tup_count);
    values[i++] = Int64GetDatum((int64)stat_info->tup_dead);
    values[i++] = Int64GetDatum((int64)stat_info->tup_space);

    values[i++] = Int64GetDatum((int64)stat_info->rel_searches);
    values[i++] = Int64GetDatum((int64)stat_info->rel_hits);
    values[i++] = Int64GetDatum((int64)stat_info->rel_newloads);
    values[i++] = Int64GetDatum((int64)stat_info->rel_count);
    values[i++] = Int64GetDatum((int64)stat_info->rel_dead);
    values[i++] = Int64GetDatum((int64)stat_info->rel_space);

    values[i++] = Int64GetDatum((int64)stat_info->part_searches);
    values[i++] = Int64GetDatum((int64)stat_info->part_hits);
    values[i++] = Int64GetDatum((int64)stat_info->part_newloads);
    values[i++] = Int64GetDatum((int64)stat_info->part_count);
    values[i++] = Int64GetDatum((int64)stat_info->part_dead);
    values[i++] = Int64GetDatum((int64)stat_info->part_space);

    values[i++] = Int64GetDatum((int64)stat_info->total_space);
    values[i++] = Int64GetDatum((int64)stat_info->swapout_count);
    values[i++] = Int64GetDatum((int64)stat_info->refcount);
    HeapTuple tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
    Datum result = HeapTupleGetDatum(tuple);
    SRF_RETURN_NEXT(funcctx, result);
}

Datum gs_gsc_clean(PG_FUNCTION_ARGS)
{
    if (!superuser()) {
        ereport(ERROR, (errmodule(MOD_GSC), errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            errmsg("Fail to clean gsc."),
            errdetail("Insufficient privilege to clean gsc."), errcause("N/A"),
            erraction("Please login in with superuser or sysdba role or contact database administrator.")));
    }
    if (!EnableGlobalSysCache()) {
        PG_RETURN_BOOL(false);
    }
    Oid dbOid = PG_ARGISNULL(0) ? ALL_DB_OID : PG_GETARG_OID(0);
    CheckDbOidValid(dbOid);
    if (dbOid == ALL_DB_OID) {
        g_instance.global_sysdbcache.ResetCache<true>(dbOid);
    } else {
        g_instance.global_sysdbcache.ResetCache<false>(dbOid);
    }
    PG_RETURN_BOOL(true);
}

static const int GLOBAL_CATALOG_INFO_NUM = 11;
bool gs_gsc_catalog_firstcall(PG_FUNCTION_ARGS)
{
    if (!superuser()) {
        ereport(ERROR, (errmodule(MOD_GSC), errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            errmsg("Fail to see gsc catalog detail."),
            errdetail("Insufficient privilege to see gsc catalog detail."), errcause("N/A"),
            erraction("Please login in with superuser or sysdba role or contact database administrator.")));
    }
    Oid dbOid = PG_ARGISNULL(0) ? ALL_DB_OID : PG_GETARG_OID(0);
    Oid relOid = PG_ARGISNULL(1) ? ALL_REL_OID : PG_GETARG_INT32(1);
    CheckDbOidValid(dbOid);
    FuncCallContext *funcctx = NULL;
    if (!SRF_IS_FIRSTCALL()) {
        return true;
    }
    funcctx = SRF_FIRSTCALL_INIT();
    MemoryContext oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
    funcctx->tuple_desc = CreateTemplateTupleDesc(GLOBAL_CATALOG_INFO_NUM, false);
    int i = 1;
    TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)i++, "database_id", INT8OID, -1, 0);
    TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)i++, "database_name", TEXTOID, -1, 0);
    TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)i++, "rel_id", INT8OID, -1, 0);
    TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)i++, "rel_name", TEXTOID, -1, 0);
    TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)i++, "cache_id", INT8OID, -1, 0);
    TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)i++, "self", TEXTOID, -1, 0);
    TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)i++, "ctid", TEXTOID, -1, 0);
    TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)i++, "infomask", INT8OID, -1, 0);
    TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)i++, "infomask2", INT8OID, -1, 0);
    TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)i++, "hash_value", INT8OID, -1, 0);
    TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)i++, "refcount", INT8OID, -1, 0);
    funcctx->tuple_desc = BlessTupleDesc(funcctx->tuple_desc);

    List *tmp_list = NIL;
    if (EnableGlobalSysCache()) {
        tmp_list = g_instance.global_sysdbcache.GetGlobalDBStatDetail(dbOid, relOid, GscStatDetailTuple);
    }
    if (list_length(tmp_list) == 0) {
        MemoryContextSwitchTo(oldcontext);
        return false;
    }
    funcctx->max_calls = list_length(tmp_list);
    GlobalCatalogTupleStat **tuple_stat_list =
        (GlobalCatalogTupleStat **)palloc(sizeof(GlobalCatalogTupleStat *) * funcctx->max_calls);
    MemoryContextSwitchTo(oldcontext);
    funcctx->user_fctx = tuple_stat_list;
    ListCell *cell = NULL;
    foreach (cell, tmp_list) {
        *tuple_stat_list = (GlobalCatalogTupleStat *)lfirst(cell);
        tuple_stat_list++;
    }
    list_free_ext(tmp_list);
    return true;
}

Datum gs_gsc_catalog_detail(PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx = NULL;
    if (!gs_gsc_catalog_firstcall(fcinfo)) {
        SRF_RETURN_DONE(funcctx);
    }

    funcctx = SRF_PERCALL_SETUP();
    if (funcctx->call_cntr >= funcctx->max_calls) {
        SRF_RETURN_DONE(funcctx);
    }
    Datum values[GLOBAL_CATALOG_INFO_NUM];
    bool nulls[GLOBAL_CATALOG_INFO_NUM] = {0};
    GlobalCatalogTupleStat *tuple_stat = ((GlobalCatalogTupleStat **)funcctx->user_fctx)[funcctx->call_cntr];
    int i = 0;
    char str[NAMEDATALEN];
    values[i++] = Int64GetDatum(tuple_stat->db_oid);
    values[i++] = tuple_stat->db_name_datum;
    values[i++] = Int64GetDatum((int64)tuple_stat->rel_oid);
    values[i++] = tuple_stat->rel_name_datum;
    values[i++] = Int64GetDatum((int64)tuple_stat->cache_id);

    error_t rc = sprintf_s(str, NAMEDATALEN, "(%u, %u)", BlockIdGetBlockNumber(&tuple_stat->self.ip_blkid),
        (uint)tuple_stat->self.ip_posid);
    securec_check_ss(rc, "\0", "\0");
    values[i++] = CStringGetTextDatum(str);
    rc = sprintf_s(str, NAMEDATALEN, "(%u, %u)", BlockIdGetBlockNumber(&tuple_stat->ctid.ip_blkid),
        (uint)tuple_stat->ctid.ip_posid);
    securec_check_ss(rc, "\0", "\0");
    values[i++] = CStringGetTextDatum(str);

    values[i++] = Int64GetDatum((int64)tuple_stat->infomask);
    values[i++] = Int64GetDatum((int64)tuple_stat->infomask2);
    values[i++] = Int64GetDatum((int64)tuple_stat->hash_value);
    values[i++] = Int64GetDatum((int64)tuple_stat->refcount);
    HeapTuple tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
    Datum result = HeapTupleGetDatum(tuple);
    SRF_RETURN_NEXT(funcctx, result);
}

static const int GLOBAL_TABLE_INFO_NUM = 21;
bool gs_gsc_table_firstcall(PG_FUNCTION_ARGS)
{
    if (!superuser()) {
        ereport(ERROR, (errmodule(MOD_GSC), errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            errmsg("Fail to see gsc table detail."),
            errdetail("Insufficient privilege to see gsc table detail."), errcause("N/A"),
            erraction("Please login in with superuser or sysdba role or contact database administrator.")));
    }

    Oid dbOid = PG_ARGISNULL(0) ? ALL_DB_OID : PG_GETARG_OID(0);
    Oid relOid = PG_ARGISNULL(1) ? ALL_REL_OID : PG_GETARG_INT32(1);
    CheckDbOidValid(dbOid);

    FuncCallContext *funcctx = NULL;
    if (!SRF_IS_FIRSTCALL()) {
        return true;
    }

    funcctx = SRF_FIRSTCALL_INIT();
    MemoryContext oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
    funcctx->tuple_desc = CreateTemplateTupleDesc(GLOBAL_TABLE_INFO_NUM, false);
    int attrno = 1;

    TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)attrno++, "database_id", OIDOID, -1, 0);
    TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)attrno++, "database_name", TEXTOID, -1, 0);
    TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)attrno++, "reloid", OIDOID, -1, 0);
    TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)attrno++, "relname", TEXTOID, -1, 0);
    TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)attrno++, "relnamespace", OIDOID, -1, 0);
    TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)attrno++, "reltype", OIDOID, -1, 0);
    TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)attrno++, "reloftype", OIDOID, -1, 0);
    TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)attrno++, "relowner", OIDOID, -1, 0);
    TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)attrno++, "relam", OIDOID, -1, 0);
    TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)attrno++, "relfilenode", OIDOID, -1, 0);
    TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)attrno++, "reltablespace", OIDOID, -1, 0);
    TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)attrno++, "relhasindex", BOOLOID, -1, 0);
    TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)attrno++, "relisshared", BOOLOID, -1, 0);
    TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)attrno++, "relkind", CHAROID, -1, 0);
    TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)attrno++, "relnatts", INT2OID, -1, 0);
    TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)attrno++, "relhasoids", BOOLOID, -1, 0);
    TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)attrno++, "relhaspkey", BOOLOID, -1, 0);
    TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)attrno++, "parttype", CHAROID, -1, 0);
    TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)attrno++, "tdhasuids", BOOLOID, -1, 0);
    TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)attrno++, "attnames", TEXTOID, -1, 0);
    TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)attrno++, "extinfo", TEXTOID, -1, 0);

    funcctx->tuple_desc = BlessTupleDesc(funcctx->tuple_desc);

    List *tmp_list = NIL;
    if (EnableGlobalSysCache()) {
        tmp_list = g_instance.global_sysdbcache.GetGlobalDBStatDetail(dbOid, relOid, GscStatDetailTable);
    }

    if (list_length(tmp_list) == 0) {
        MemoryContextSwitchTo(oldcontext);
        return false;
    }

    funcctx->max_calls = list_length(tmp_list);
    GlobalCatalogTableStat **table_stat_list =
        (GlobalCatalogTableStat **)palloc(sizeof(GlobalCatalogTableStat *) * funcctx->max_calls);
    MemoryContextSwitchTo(oldcontext);
    funcctx->user_fctx = table_stat_list;
    ListCell *cell = NULL;
    foreach (cell, tmp_list) {
        *table_stat_list = (GlobalCatalogTableStat *)lfirst(cell);
        table_stat_list++;
    }
    list_free_ext(tmp_list);

    return true;
}

int rd_rel_to_datum(Datum *values, Form_pg_class rd_rel)
{
    int attrno = 0;
    values[attrno++] = rd_rel->relnamespace;
    values[attrno++] = rd_rel->reltype;
    values[attrno++] = rd_rel->reloftype;
    values[attrno++] = rd_rel->relowner;
    values[attrno++] = rd_rel->relam;
    values[attrno++] = rd_rel->relfilenode;
    values[attrno++] = rd_rel->reltablespace;
    values[attrno++] = rd_rel->relhasindex;
    values[attrno++] = rd_rel->relisshared;
    values[attrno++] = rd_rel->relkind;
    values[attrno++] = rd_rel->relnatts;
    values[attrno++] = rd_rel->relhasoids;
    values[attrno++] = rd_rel->relhaspkey;
    values[attrno++] = rd_rel->parttype;
    return attrno;
}

int rd_att_to_datum(Datum *values, TupleDesc rd_att)
{
    int attrno = 0;
    values[attrno++] = rd_att->tdhasuids;
    StringInfoData strinfo;
    initStringInfo(&strinfo);

    for (int i = 0; i < rd_att->natts; i++) {
        if (i != 0) {
            appendStringInfoString(&strinfo, ",");
        }
        appendStringInfoString(&strinfo, "'");
        appendStringInfoString(&strinfo, rd_att->attrs[i].attname.data);
        appendStringInfoString(&strinfo, "'");
    }
    values[attrno++] = CStringGetTextDatum(strinfo.data);
    pfree_ext(strinfo.data);

    return attrno;
}

Datum gs_gsc_table_detail(PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx = NULL;
    if (!gs_gsc_table_firstcall(fcinfo)) {
        SRF_RETURN_DONE(funcctx);
    }

    funcctx = SRF_PERCALL_SETUP();
    if (funcctx->call_cntr >= funcctx->max_calls) {
        SRF_RETURN_DONE(funcctx);
    }

    Datum values[GLOBAL_TABLE_INFO_NUM];
    bool nulls[GLOBAL_TABLE_INFO_NUM] = {0};
    nulls[GLOBAL_TABLE_INFO_NUM - 1] = true;
    GlobalCatalogTableStat *table_stat = ((GlobalCatalogTableStat **)funcctx->user_fctx)[funcctx->call_cntr];
    int attrno = 0;

    values[attrno++] = Int64GetDatum(table_stat->db_id);
    values[attrno++] = table_stat->db_name;
    values[attrno++] = Int64GetDatum((int64)table_stat->rel_id);
    values[attrno++] = CStringGetTextDatum(table_stat->rd_rel->relname.data);
    attrno = rd_rel_to_datum(values + attrno, table_stat->rd_rel) + attrno;
    attrno = rd_att_to_datum(values + attrno, table_stat->rd_att) + attrno;
    Assert(attrno == GLOBAL_TABLE_INFO_NUM - 1);
    
    /* form physical tuple and return as datum tuple */
    HeapTuple tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
    Datum result = HeapTupleGetDatum(tuple);

    SRF_RETURN_NEXT(funcctx, result);
}

int ResizeHashBucket(int origin_nbucket, DynamicHashBucketStrategy strategy)
{
    int cc_nbuckets = origin_nbucket;
    Assert(cc_nbuckets > 0 && (cc_nbuckets & -cc_nbuckets) == cc_nbuckets);
    if (cc_nbuckets <= MinHashBucketSize) {
        return cc_nbuckets;
    }
    switch (strategy) {
        case DynamicHashBucketDefault:
            break;
        case DynamicHashBucketHalf:
            cc_nbuckets = cc_nbuckets >> 1;
            break;
        case DynamicHashBucketQuarter:
            cc_nbuckets = cc_nbuckets >> 2;
            break;
        case DynamicHashBucketEighth:
            cc_nbuckets = cc_nbuckets >> 3;
            break;
        case DynamicHashBucketMin:
            /* when off, dont do palloc */
            cc_nbuckets = MinHashBucketSize;
            break;
    }
    if (cc_nbuckets <= MinHashBucketSize) {
        return MinHashBucketSize;
    }
    return cc_nbuckets;
}

void NotifyGscRecoveryStarted()
{
    if (!EnableGlobalSysCache()) {
        return;
    }
    g_instance.global_sysdbcache.recovery_finished = false;
}
void NotifyGscRecoveryFinished()
{
    if (!EnableGlobalSysCache()) {
        return;
    }
    g_instance.global_sysdbcache.recovery_finished = true;
}

void NotifyGscPgxcPoolReload()
{
    if (!EnableGlobalSysCache()) {
        return;
    }
    g_instance.global_sysdbcache.InvalidateAllRelationNodeList();
}

/*
 * whenever you change server_mode, call RefreshHotStandby plz
 * we need know current_mode to support gsc feature
 */
void NotifyGscHotStandby()
{
    if (!EnableGlobalSysCache()) {
        return;
    }
    g_instance.global_sysdbcache.ResetCache<true>(ALL_DB_OID);
    g_instance.global_sysdbcache.RefreshHotStandby();
}

void NotifyGscSigHup()
{
    if (!EnableGlobalSysCache()) {
        return;
    }
    g_instance.global_sysdbcache.UpdateGSCConfig(g_instance.attr.attr_memory.global_syscache_threshold);
}

void NotifyGscDropDB(Oid db_id, bool need_clear)
{
    if (!EnableGlobalSysCache()) {
        return;
    }
    g_instance.global_sysdbcache.DropDB(db_id, need_clear);
}
