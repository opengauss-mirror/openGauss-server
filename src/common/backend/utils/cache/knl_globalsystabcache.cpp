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

#include "executor/executor.h"
#include "utils/knl_globalsystabcache.h"
#include "utils/knl_globaldbstatmanager.h"
#include "knl/knl_instance.h"
#include "knl/knl_session.h"
#include "utils/memutils.h"
#include "storage/lmgr.h"

GlobalSysTabCache::GlobalSysTabCache(Oid dbOid, bool isShared, GlobalSysDBCacheEntry *dbEntry)
{
    Assert((isShared && dbOid == InvalidOid) || (!isShared && dbOid != InvalidOid));
    m_dbOid = dbOid;
    m_isShared = isShared;
    m_dbEntry = dbEntry;
    m_isInited = false;
    m_global_systupcaches = NULL;
    m_systab_locks = NULL;
    m_tup_count = 0;
    m_tup_space = 0;
}

/*
 * @Description:
 *    Initialization function for current GlobalSysTabCache objects, in lite-mode to reduce
 *    memory consumption we do lazy initialization, here lazy says do init when it gets used
 *
 * @param[IN] void
 *
 * @return: void
 */
void GlobalSysTabCache::Init()
{
    Assert(!m_isInited);
    MemoryContext old = MemoryContextSwitchTo(m_dbEntry->GetRandomMemCxt());

    /* allocate GlobalSysTabCache array for each tobe cached DB objects */
    m_systab_locks = (pthread_rwlock_t *)palloc0(sizeof(pthread_rwlock_t) * SysCacheSize);
    m_global_systupcaches = (GlobalSysTupCache **)palloc0(sizeof(GlobalSysTupCache *) * SysCacheSize);

    /*
     * For lite-mode, we do not create GlobalSysTupCache aggresively to limit memory
     * consumption as lower as possible
     */
#ifndef ENABLE_LITE_MODE
    for (int cache_id = 0; cache_id < SysCacheSize; cache_id++) {
        PthreadRwLockInit(&m_systab_locks[cache_id], NULL);
        /* we split catcache into shared_db_entry and mydb_entry,
         * so for different db_entry, just init systupcaches which are necessary */
        if ((m_isShared && g_instance.global_sysdbcache.HashSearchSharedRelation(cacheinfo[cache_id].reloid)) ||
            (!m_isShared && !g_instance.global_sysdbcache.HashSearchSharedRelation(cacheinfo[cache_id].reloid))) {
            m_global_systupcaches[cache_id] = New(m_dbEntry->GetRandomMemCxt()) GlobalSysTupCache(
                m_dbOid, cache_id, m_isShared, m_dbEntry);
            m_global_systupcaches[cache_id]->SetStatInfoPtr(&m_tup_count, &m_tup_space);
        }
    }
#endif
    MemoryContextSwitchTo(old);

    /* Mark initialization stage is done */
    m_isInited = true;
}

/*
 * @Description:
 *    Reset all none-shared catlog table in current, here reset means delete the GloablCTup
 *    from cc_list and cc_bucket and move all systuple dead_list
 *
 * @param[IN] void
 *
 * @return: void
 */
template <bool force>
void GlobalSysTabCache::ResetCatCaches()
{
    /* global reset dont reset shared cache */
    for (int i = 0; i < SysCacheSize; i++) {
        /* shared table is separated from normal db */
        if (m_global_systupcaches[i] == NULL) {
            continue;
        }

        /* handle SysTupCache's cc_list and cc_bucket's CatCTup elements */
        m_global_systupcaches[i]->ResetCatalogCache<force>();
    }
}
template void GlobalSysTabCache::ResetCatCaches<false>();
template void GlobalSysTabCache::ResetCatCaches<true>();

void GlobalSysTabCache::RemoveAllTailElements()
{
    for (int i = 0; i < SysCacheSize; i++) {
        // shared table is separated from normal db
        if (m_global_systupcaches[i] == NULL) {
            continue;
        }
        m_global_systupcaches[i]->RemoveAllTailElements();
#ifndef ENABLE_LITE_MODE
        /* memory is under control, so stop swapout */
        if (g_instance.global_sysdbcache.MemoryUnderControl()) {
            break;
        }
#endif
    }
}

/*
 * @Description:
 *    Find GlobalSysTupCatch(table level) with given cache_id
 *
 * @param[IN] cache_id: GSTC's cache id (array index of SysTabCache)
 *
 * @return: target GlobalSysTupCache object
 */
GlobalSysTupCache *GlobalSysTabCache::CacheIdGetGlobalSysTupCache(int cache_id)
{
    Assert(m_isInited);
    Assert((m_isShared && g_instance.global_sysdbcache.HashSearchSharedRelation(cacheinfo[cache_id].reloid)) ||
           (!m_isShared && !g_instance.global_sysdbcache.HashSearchSharedRelation(cacheinfo[cache_id].reloid)));

#ifdef ENABLE_LITE_MODE
    PthreadRWlockRdlock(LOCAL_SYSDB_RESOWNER, &m_systab_locks[cache_id]);
    if (unlikely(m_global_systupcaches[cache_id] == NULL)) {
        PthreadRWlockUnlock(LOCAL_SYSDB_RESOWNER, &m_systab_locks[cache_id]);
        PthreadRWlockWrlock(LOCAL_SYSDB_RESOWNER, &m_systab_locks[cache_id]);
        if (m_global_systupcaches[cache_id] == NULL) {
            m_global_systupcaches[cache_id] = New(m_dbEntry->GetRandomMemCxt()) GlobalSysTupCache(
                m_dbOid, cache_id, m_isShared, m_dbEntry);
            m_global_systupcaches[cache_id]->SetStatInfoPtr(&m_tup_count, &m_tup_space);
            m_dbEntry->MemoryEstimateAdd(GetMemoryChunkSpace(m_global_systupcaches[cache_id]));
        }
        m_global_systupcaches[cache_id]->Init();
    }
    PthreadRWlockUnlock(LOCAL_SYSDB_RESOWNER, &m_systab_locks[cache_id]);
#endif

    /* if GSC is already setup, just return the cache handler */
    if (likely(m_global_systupcaches[cache_id]->Inited())) {
        return m_global_systupcaches[cache_id];
    }

    /* do cache initialization if not set up yet */
    PthreadRWlockWrlock(LOCAL_SYSDB_RESOWNER, &m_systab_locks[cache_id]);
    m_global_systupcaches[cache_id]->Init();
    PthreadRWlockUnlock(LOCAL_SYSDB_RESOWNER, &m_systab_locks[cache_id]);

    return m_global_systupcaches[cache_id];
}

/*
 * @Description:
 *    Find GlobalSysTupCatch(table level) with given cache_id and hash_value and mark
 *    it(them) as dead and move to dead_list
 *
 *    Invalid: means mark systuple satisfy cache_id and hash_value as "dead"
 *    Reset: means mark all systuples under cache_id as "dead"
 *
 * @param[IN] cache_id: GSTC's cache id (array index of SysTabCache), hash_value
 *
 * @return: target GlobalSysTupCache object
 */
void GlobalSysTabCache::InvalidTuples(int cache_id, uint32 hash_value, bool reset)
{
#ifdef ENABLE_LITE_MODE
    PthreadRWlockRdlock(LOCAL_SYSDB_RESOWNER, &m_systab_locks[cache_id]);
    if (unlikely(m_global_systupcaches[cache_id] == NULL)) {
        PthreadRWlockUnlock(LOCAL_SYSDB_RESOWNER, &m_systab_locks[cache_id]);
        return;
    }
    PthreadRWlockUnlock(LOCAL_SYSDB_RESOWNER, &m_systab_locks[cache_id]);
#endif

    /* maybe upgrade from version before v5r2c00, the cacheid is out of order
     * whatever, we cache nothing except relmap, so just ignore the catcache invalmsg */
    if (unlikely(!g_instance.global_sysdbcache.recovery_finished && m_global_systupcaches[cache_id] == NULL)) {
        return;
    }

    if (!m_global_systupcaches[cache_id]->Inited()) {
        return;
    }

    if (reset) {
        /* for all systuple */
        m_global_systupcaches[cache_id]->ResetCatalogCache<true>();
    } else {
        /* for systuples satisfy hash_value */
        m_global_systupcaches[cache_id]->HashValueInvalidate(hash_value);
    }
}

/*
 * @Description:
 *    Fetch tuple stats for given relOid from GlobalSysTabCache object (current
 *    database)
 *
 * @param[IN] relOid: parsed query tree.
 *
 * @return: list of tup stats in "GlobalCatalogTupleStat"
 */
List *GlobalSysTabCache::GetCatalogTupleStats(Oid relOid)
{
    List *tuple_stat_list = NIL;

    /* Scan each GlobalSysCache object's holding GlobalCTup's statinfo */
    for (int i = 0; i < SysCacheSize; i++) {
        /*
         * Skip GSC object for shared tables, because shared-table's GSC by design is
         * stored in a special GlobalSysTabCache(DB) and set NULL to its entry
         */
        if (m_global_systupcaches[i] == NULL) {
            continue;
        }

        /* Skip those not initiliazed */
        if (!m_global_systupcaches[i]->Inited()) {
            continue;
        }

        Assert(cacheinfo[i].reloid == m_global_systupcaches[i]->GetCCRelOid());
        /* skip undesired rel */
        if (relOid != ALL_REL_OID && (Oid)relOid != cacheinfo[i].reloid) {
            continue;
        }

        /* Fetch current GSC's info */
        List *tmp = m_global_systupcaches[i]->GetGlobalCatCTupStat();
        tuple_stat_list = lappend3(tuple_stat_list, tmp);

        pfree_ext(tmp);
    }

    return tuple_stat_list;
}
