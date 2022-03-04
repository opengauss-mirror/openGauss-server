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
#include "executor/executor.h"

void GlobalSysDBCacheEntry::Free(GlobalSysDBCacheEntry *entry)
{
    GSC_CACHE2_elog("GlobalSysDBCacheEntry Free with db oid %u", entry->db_id);
    /* in case palloc fail */
    for (uint i = 0; i < entry->m_memcxt_nums; i++) {
        if (entry->m_mem_cxt_groups[i] != NULL) {
            MemoryContextDelete(entry->m_mem_cxt_groups[i]);
        }
    }
    pfree_ext(entry->m_dbstat);
    pfree(entry);
}

void GlobalSysDBCacheEntry::Release()
{
    g_instance.global_sysdbcache.ReleaseGSCEntry(this);
}

template <bool force>
void GlobalSysDBCacheEntry::ResetDBCache()
{
    m_systabCache->ResetCatCaches<force>();
    m_tabdefCache->ResetRelCaches<force>();
    if (m_dbOid != InvalidOid) {
        m_partdefCache->ResetPartCaches<force>();
    }
}

void GlobalSysDBCacheEntry::RemoveTailElements()
{
    m_systabCache->RemoveAllTailElements();
    m_tabdefCache->RemoveAllTailElements<true>();
    if (m_dbOid != InvalidOid) {
        m_partdefCache->RemoveAllTailElements<false>();
    }
}

template void GlobalSysDBCacheEntry::ResetDBCache<false>();
template void GlobalSysDBCacheEntry::ResetDBCache<true>();

void GlobalSysDBCacheEntry::MemoryEstimateAdd(uint64 size)
{
    (void)pg_atomic_fetch_add_u64(&m_rough_used_space, size);
    (void)pg_atomic_fetch_add_u64(&g_instance.global_sysdbcache.gsc_rough_used_space, size);
}
void GlobalSysDBCacheEntry::MemoryEstimateSub(uint64 size)
{
    (void)pg_atomic_fetch_sub_u64(&m_rough_used_space, size);
    (void)pg_atomic_fetch_sub_u64(&g_instance.global_sysdbcache.gsc_rough_used_space, size);
}

GlobalDBStatManager::GlobalDBStatManager()
{
    m_nbuckets = 0;
    m_dbstat_nbuckets= NULL;
    m_dbstat_buckets = NULL;

    m_mydb_refs = NULL;
    m_mydb_roles = NULL;
    m_max_backend_id = 0;
    m_backend_ref_lock = NULL;

    m_dbstat_memcxt = NULL;
}

void GlobalDBStatManager::InitDBStat(int nbuckets, MemoryContext top)
{
    m_dbstat_memcxt =
#ifdef ENABLE_LITE_MODE
            AllocSetContextCreate(top, "GlobalSysDBStatCache", ALLOCSET_SMALL_MINSIZE, ALLOCSET_SMALL_INITSIZE,
                                  ALLOCSET_SMALL_MAXSIZE, SHARED_CONTEXT);
#else
            AllocSetContextCreate(top, "GlobalSysDBStatCache", ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE,
                                  ALLOCSET_DEFAULT_MAXSIZE, SHARED_CONTEXT);
#endif
    MemoryContext old = MemoryContextSwitchTo(m_dbstat_memcxt);
    /* dbstat use lock as dbentry */
    m_nbuckets = nbuckets;
    m_dbstat_nbuckets = (int *)palloc0(nbuckets * sizeof(int));
    m_dbstat_buckets = (List**)palloc0(nbuckets * sizeof(List *));

    m_backend_ref_lock = (pthread_rwlock_t *)palloc0(sizeof(pthread_rwlock_t));
    PthreadRwLockInit(m_backend_ref_lock, NULL);
#ifdef ENABLE_LITE_MODE
    m_max_backend_id = 64;
#else
    m_max_backend_id = 1024;
#endif
    m_mydb_refs = (GlobalSysDBCacheEntry **)palloc0(sizeof(GlobalSysDBCacheEntry *) * m_max_backend_id);
    m_mydb_roles = (int *)palloc0(sizeof(int) * m_max_backend_id);

    (void)MemoryContextSwitchTo(old);
}

void GlobalDBStatManager::RecordSwapOutDBEntry(GlobalSysDBCacheEntry *entry)
{
    List *cur_dbstat_bucket = m_dbstat_buckets[entry->m_hash_index];
    ListCell *lc;
    foreach(lc, cur_dbstat_bucket) {
        GlobalSysCacheStat *cur_dbstat = (GlobalSysCacheStat *)lfirst(lc);
        if (cur_dbstat->db_oid == entry->m_dbOid) {
            cur_dbstat->tup_searches += entry->m_dbstat->tup_searches;
            cur_dbstat->tup_hits += entry->m_dbstat->tup_hits;
            cur_dbstat->tup_newloads += entry->m_dbstat->tup_newloads;

            cur_dbstat->rel_searches += entry->m_dbstat->rel_searches;
            cur_dbstat->rel_hits += entry->m_dbstat->rel_hits;
            cur_dbstat->rel_newloads += entry->m_dbstat->rel_newloads;

            cur_dbstat->part_searches += entry->m_dbstat->part_searches;
            cur_dbstat->part_hits += entry->m_dbstat->part_hits;
            cur_dbstat->part_newloads += entry->m_dbstat->part_newloads;

            cur_dbstat->swapout_count += 1;

            break;
        }
    }
    if (lc != NULL) {
        return;
    }
    MemoryContext old = MemoryContextSwitchTo(m_dbstat_memcxt);
    m_dbstat_buckets[entry->m_hash_index] = lappend(m_dbstat_buckets[entry->m_hash_index], entry->m_dbstat);
    entry->m_dbstat = NULL;
    (void)MemoryContextSwitchTo(old);
}

void GlobalDBStatManager::DropDB(Oid db_oid, Index hash_index)
{
    GlobalSysCacheStat *cur_dbstat = NULL;

    ListCell *lc = NULL;
    foreach(lc, m_dbstat_buckets[hash_index]) {
        cur_dbstat = (GlobalSysCacheStat *)lfirst(lc);
        if (cur_dbstat->db_oid == db_oid) {
            m_dbstat_buckets[hash_index] = list_delete_ptr(m_dbstat_buckets[hash_index], cur_dbstat);
            pfree_ext(cur_dbstat);
            break;
        }
    }
}

void GlobalDBStatManager::GetDBStat(GlobalSysCacheStat *db_stat)
{
    List *cur_dbstat_bucket = m_dbstat_buckets[db_stat->hash_index];
    ListCell *lc = NULL;
    foreach(lc, cur_dbstat_bucket) {
        GlobalSysCacheStat *cur_dbstat = (GlobalSysCacheStat *)lfirst(lc);
        if (cur_dbstat->db_oid == db_stat->db_oid) {
            db_stat->tup_searches += cur_dbstat->tup_searches;
            db_stat->tup_hits += cur_dbstat->tup_hits;
            db_stat->tup_newloads += cur_dbstat->tup_newloads;

            db_stat->rel_searches += cur_dbstat->rel_searches;
            db_stat->rel_hits += cur_dbstat->rel_hits;
            db_stat->rel_newloads += cur_dbstat->rel_newloads;

            db_stat->part_searches += cur_dbstat->part_searches;
            db_stat->part_hits += cur_dbstat->part_hits;
            db_stat->part_newloads += cur_dbstat->part_newloads;

            db_stat->swapout_count = cur_dbstat->swapout_count;
            break;
        }
    }
}


bool GlobalDBStatManager::IsDBUsedByProc(GlobalSysDBCacheEntry *entry)
{
    Assert(entry->m_dbOid != InvalidOid);
    PthreadRWlockRdlock(LOCAL_SYSDB_RESOWNER, m_backend_ref_lock);
    bool used = false;
    for (int i = 0; i < (int)m_max_backend_id; i++) {
        if (m_mydb_refs[i] == entry) {
            used = true;
            break;
        }
    }
    PthreadRWlockUnlock(LOCAL_SYSDB_RESOWNER, m_backend_ref_lock);
    return used;
}

void GlobalDBStatManager::RepallocThreadEntryArray(Oid backend_id)
{
    while ((uint)backend_id >= m_max_backend_id) {
        PthreadRWlockWrlock(LOCAL_SYSDB_RESOWNER, m_backend_ref_lock);
        if ((uint)backend_id < m_max_backend_id) {
            PthreadRWlockUnlock(LOCAL_SYSDB_RESOWNER, m_backend_ref_lock);
            return;
        }

        GSC_CACHE3_elog("MyBackendId %d is greater equal than m_max_backend_id %u", backend_id, m_max_backend_id);
        uint max_backend_id = m_max_backend_id << 1;
        int len_refs = sizeof(GlobalSysDBCacheEntry *) * max_backend_id;
        m_mydb_refs = (GlobalSysDBCacheEntry **)repalloc(m_mydb_refs, len_refs);
        int len_roles = sizeof(int) * max_backend_id;
        m_mydb_roles = (int *)repalloc(m_mydb_roles, len_roles);
        errno_t rc = memset_s(m_mydb_refs + m_max_backend_id, len_refs >> 1, 0, len_refs >> 1);
        securec_check(rc, "\0", "\0");
        rc = memset_s(m_mydb_roles + m_max_backend_id, len_roles >> 1, 0, len_roles >> 1);
        securec_check(rc, "\0", "\0");
        m_max_backend_id = max_backend_id;
        PthreadRWlockUnlock(LOCAL_SYSDB_RESOWNER, m_backend_ref_lock);
    }
}

void GlobalDBStatManager::ThreadHoldDB(GlobalSysDBCacheEntry *db)
{
    RepallocThreadEntryArray(t_thrd.proc_cxt.MyBackendId);
    PthreadRWlockRdlock(LOCAL_SYSDB_RESOWNER, m_backend_ref_lock);
    BackendId MyBackendId = t_thrd.proc_cxt.MyBackendId;
    m_mydb_refs[MyBackendId] = db;
    m_mydb_roles[MyBackendId] = t_thrd.role;
    PthreadRWlockUnlock(LOCAL_SYSDB_RESOWNER, m_backend_ref_lock);
}