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

#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/genam.h"
#include "access/hash.h"
#include "access/heapam.h"
#include "access/relscan.h"
#include "access/sysattr.h"
#include "access/transam.h"
#include "access/tuptoaster.h"
#include "access/valid.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_proc_fn.h"
#include "catalog/pg_type.h"
#include "catalog/pg_attribute.h"
#include "catalog/heap.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "parser/parse_relation.h"
#include "parser/parse_type.h"
#include "pgstat.h"
#ifdef CatCache_STATS
#include "storage/ipc.h" /* for on_proc_exit */
#endif
#include "storage/lmgr.h"
#include "utils/acl.h"
#include "utils/datum.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/extended_statistics.h"
#include "utils/fmgroids.h"
#include "utils/fmgrtab.h"
#include "utils/hashutils.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/relcache.h"
#include "utils/resowner.h"
#include "utils/syscache.h"
#include "miscadmin.h"
#include "utils/knl_catcache.h"
#include "utils/knl_localsystupcache.h"
#include "utils/knl_globalsystabcache.h"

void LocalSysTupCache::ResetInitFlag()
{
    DLInitList(&m_dead_cts);
    DLInitList(&m_dead_cls);
    DLInitList(&cc_lists);
    invalid_entries.ResetInitFlag();
    /* DLInitList(Bucket); is  already called by releaseglobalrefcount */
    m_global_systupcache = NULL;
    m_relinfo.cc_relname = NULL;
    m_relinfo.cc_tupdesc = NULL;
    for (int i = 0; i < CATCACHE_MAXKEYS; i++) {
        m_relinfo.cc_hashfunc[i] = NULL;
        m_relinfo.cc_fastequal[i] = NULL;
    }
    m_db_id = InvalidOid;
    cc_searches = 0;
    cc_hits = 0;
    cc_neg_hits = 0;
    cc_newloads = 0;
    cc_invals = 0;
    cc_lsearches = 0;
    cc_lhits = 0;
    m_is_inited = false;
    m_is_inited_phase2 = false;
    m_rls_user = InvalidOid;
}

void LocalSysTupCache::HandleDeadLocalCatCTup(LocalCatCTup *ct)
{
    DLRemove(&ct->cache_elem);
    if (ct->refcount == 0) {
        FreeLocalCatCTup(ct);
    } else {
        DLAddTail(&m_dead_cts, &ct->cache_elem);
    }
}

void LocalSysTupCache::FreeLocalCatCTup(LocalCatCTup *ct)
{
    Assert(ct->refcount == 0);
    if (ct->global_ct == NULL) {
        CatCacheFreeKeys(m_relinfo.cc_tupdesc, m_relinfo.cc_nkeys, m_relinfo.cc_keyno, ct->keys);
        pfree_ext(ct);
        return;
    }
    ct->global_ct->Release();
    pfree_ext(ct);
}

void LocalSysTupCache::FreeDeadCts()
{
    while (unlikely(m_dead_cts.dll_len > 0)) {
        Dlelem *elt = DLRemHead(&m_dead_cts);
        LocalCatCTup *ct = (LocalCatCTup *)DLE_VAL(elt);
        if (unlikely(ct->refcount != 0)) {
            /* we move the active entry to tail of list and let next call free it */
            DLAddTail(&m_dead_cts, &ct->cache_elem);
            break;
        } else {
            FreeLocalCatCTup(ct);
        }
    }
}

void LocalSysTupCache::HandleDeadLocalCatCList(LocalCatCList *cl)
{
    DLRemove(&cl->cache_elem);
    if (cl->refcount == 0) {
        FreeLocalCatCList(cl);
    } else {
        DLAddTail(&m_dead_cls, &cl->cache_elem);
    }
}

void LocalSysTupCache::FreeLocalCatCList(LocalCatCList *cl)
{
    Assert(cl->refcount == 0);
    cl->global_cl->Release();
    pfree_ext(cl);
}

void LocalSysTupCache::FreeDeadCls()
{
    while (unlikely(m_dead_cls.dll_len > 0)) {
        Dlelem *elt = DLRemHead(&m_dead_cls);
        LocalCatCList *cl = (LocalCatCList *)DLE_VAL(elt);
        if (unlikely(cl->refcount != 0)) {
            /* we move the active entry to tail of list and let next call free it */
            DLAddTail(&m_dead_cls, &cl->cache_elem);
            break;
        } else {
            FreeLocalCatCList(cl);
        }
    }
}


/* call when switch db, reset memcxt after it func */
void LocalSysTupCache::ReleaseGlobalRefcount()
{
    /* not inited, nothing to do */
    if (unlikely(!m_is_inited_phase2)) {
        return;
    }
    /* release cl */
    for (Dlelem *elt = DLGetHead(&cc_lists); elt; elt = DLGetSucc(elt)) {
        LocalCatCList *cl = (LocalCatCList *)DLE_VAL(elt);
        cl->global_cl->Release();
    }
    DLInitList(&cc_lists);
    while (m_dead_cls.dll_len > 0) {
        Dlelem *elt = DLRemHead(&m_dead_cls);
        LocalCatCList *cl = (LocalCatCList *)DLE_VAL(elt);
        cl->global_cl->Release();
    }

    /* release ct */
    for (int hash_index = 0; hash_index < cc_nbuckets; hash_index++) {
        for (Dlelem *elt = DLGetHead(GetBucket(hash_index)); elt;) {
            LocalCatCTup *ct = (LocalCatCTup *)DLE_VAL(elt);
            elt = DLGetSucc(elt);
            Assert(ct->global_ct == NULL || ct->global_ct->refcount > 0);
            /* we dont release exclusive tuple, because it is a local tuple */
            if (ct->global_ct != NULL && ct->global_ct->canInsertGSC) {
                ct->global_ct->Release();
            }
        }
        DLInitList(GetBucket(hash_index));
    }
    /* we dont reset mydb tuple buckets, instead we use bucket_db array to record them */
    while (m_dead_cts.dll_len > 0) {
        Dlelem *elt = DLRemHead(&m_dead_cts);
        LocalCatCTup *ct = (LocalCatCTup *)DLE_VAL(elt);
        if (ct->global_ct != NULL) {
            ct->global_ct->Release();
        }
    }
}

void LocalSysTupCache::ResetCatalogCache()
{
    /* not inited, nothing to do */
    if (unlikely(!m_is_inited_phase2)) {
        return;
    }
    for (Dlelem *elt = DLGetHead(&cc_lists); elt;) {
        LocalCatCList *cl = (LocalCatCList *)DLE_VAL(elt);
        elt = DLGetSucc(elt);
        HandleDeadLocalCatCList(cl);
    }
    /* Remove each tuple in this cache, now all cl are released. */
    for (int hash_index = 0; hash_index < cc_nbuckets; hash_index++) {
        for (Dlelem *elt = DLGetHead(GetBucket(hash_index)); elt;) {
            LocalCatCTup *ct = (LocalCatCTup *)DLE_VAL(elt);
            elt = DLGetSucc(elt);
            Assert(ct->global_ct == NULL || ct->global_ct->refcount > 0);
            HandleDeadLocalCatCTup(ct);
        }
    }
}

void LocalSysTupCache::HashValueInvalidateLocal(uint32 hash_value)
{
    /* not inited, nothing to do */
    if (unlikely(!m_is_inited_phase2)) {
        return;
    }
    /*
     * inspect caches to find the proper cache
     * list of cache_header is never changed after init, so lock is not needed
     * Invalidate *all *CatCLists in this cache; it's too hard to tell
     * which searches might still be correct, so just zap 'em all.
     */
    for (Dlelem *elt = DLGetHead(&cc_lists); elt;) {
        LocalCatCList *cl = (LocalCatCList *)DLE_VAL(elt);
        elt = DLGetSucc(elt);
        HandleDeadLocalCatCList(cl);
    }
    uint32 hash_index = HASH_INDEX(hash_value, cc_nbuckets);
    for (Dlelem *elt = DLGetHead(GetBucket(hash_index)); elt;) {
        LocalCatCTup *ct = (LocalCatCTup *)DLE_VAL(elt);
        elt = DLGetSucc(elt);
        if (hash_value == ct->hash_value) {
            HandleDeadLocalCatCTup(ct);
        }
    }
}

template <bool reset>
void LocalSysTupCache::FlushGlobalByInvalidMsg(Oid db_id, uint32 hash_value)
{
    if (db_id == InvalidOid) {
        Assert(!m_is_inited_phase2 || m_relinfo.cc_relisshared);
        GlobalSysTabCache *global_systab = t_thrd.lsc_cxt.lsc->GetSharedSysTabCache();
        global_systab->InvalidTuples(m_cache_id, hash_value, reset);
        return;
    }
    if (m_is_inited) {
        InitPhase2();
        Assert(db_id == m_db_id);
    }
    if (!m_is_inited_phase2) {
        Assert(!m_is_inited);
        /* redoxact meand !m_is_inited_phase2 */
        GlobalSysDBCacheEntry *entry = g_instance.global_sysdbcache.FindTempGSCEntry(db_id);
        if (entry == NULL) {
            return;
        }
        entry->m_systabCache->InvalidTuples(m_cache_id, hash_value, reset);
        g_instance.global_sysdbcache.ReleaseTempGSCEntry(entry);
    } else {
        Assert(db_id == m_db_id);
        if (reset) {
            m_global_systupcache->ResetCatalogCache<true>();
        } else {
            m_global_systupcache->HashValueInvalidate(hash_value);
        }
    }
}

template void LocalSysTupCache::FlushGlobalByInvalidMsg<true>(Oid db_id, uint32 hash_value);
template void LocalSysTupCache::FlushGlobalByInvalidMsg<false>(Oid db_id, uint32 hash_value);


void LocalSysTupCache::PrepareToInvalidateCacheTuple(HeapTuple tuple, HeapTuple newtuple,
                                                     void (*function)(int, uint32, Oid))
{
    if (m_relinfo.cc_indexoid == ProcedureNameAllArgsNspIndexId
        && t_thrd.proc->workingVersionNum < 92470) {
        return;
    }
    InitPhase2();
    Assert(m_global_systupcache != NULL);
    Assert(CheckMyDatabaseMatch());
    uint32 hash_value = CatalogCacheComputeTupleHashValue(cc_id, m_relinfo.cc_keyno, m_relinfo.cc_tupdesc,
        m_relinfo.cc_hashfunc, m_relinfo.cc_reloid, m_relinfo.cc_nkeys, tuple);
    Oid dbid = m_relinfo.cc_relisshared ? (Oid)0 : t_thrd.lsc_cxt.lsc->my_database_id;
    /* for every session si msg */
    (*function)(cc_id, hash_value, dbid);
    if (newtuple) {
        uint32 new_hash_value = CatalogCacheComputeTupleHashValue(cc_id, m_relinfo.cc_keyno, m_relinfo.cc_tupdesc,
        m_relinfo.cc_hashfunc, m_relinfo.cc_reloid, m_relinfo.cc_nkeys, newtuple);
        if (new_hash_value != hash_value) {
            (*function)(cc_id, new_hash_value, dbid);
        }
    }
}

LocalSysTupCache::LocalSysTupCache(int cache_id)
{
    m_cache_id = cache_id;
    cc_buckets = NULL;
    m_local_mem_cxt = NULL;
    ResetInitFlag();
    const cachedesc *cur_cache_info = &cacheinfo[m_cache_id];
    cc_nbuckets = ResizeHashBucket(cur_cache_info->nbuckets, g_instance.global_sysdbcache.dynamic_hash_bucket_strategy);
    cc_id = m_cache_id;
    m_relinfo.cc_reloid = cur_cache_info->reloid;
    m_relinfo.cc_indexoid = cur_cache_info->indoid;
    m_relinfo.cc_nkeys = cur_cache_info->nkeys;
    for (int i = 0; i < m_relinfo.cc_nkeys; ++i) {
        m_relinfo.cc_keyno[i] = cur_cache_info->key[i];
    }
    m_relinfo.cc_relisshared = g_instance.global_sysdbcache.HashSearchSharedRelation(m_relinfo.cc_reloid);
}

void LocalSysTupCache::CreateCatBucket()
{
    invalid_entries.Init();
    Assert(cc_nbuckets > 0 && (cc_nbuckets & -cc_nbuckets) == cc_nbuckets);
    size_t sz = (cc_nbuckets) * (sizeof(Dllist));
    cc_buckets = (Dllist *)palloc0(sz);
    if (m_relinfo.cc_relisshared) {
        m_local_mem_cxt = t_thrd.lsc_cxt.lsc->lsc_share_memcxt;
    } else {
        m_local_mem_cxt = t_thrd.lsc_cxt.lsc->lsc_mydb_memcxt;
    }
}

void LocalSysTupCache::FlushRlsUserImpl()
{
    Oid cur_user = GetCurrentUserId();
    if (likely(m_rls_user == GetCurrentUserId()) || cur_user == InvalidOid) {
        return;
    }
    /* we must flush the local cache, which dont belong to current user */
    ResetCatalogCache();
    m_rls_user = GetCurrentUserId();
}


void LocalSysTupCache::InitPhase2Impl()
{
    Assert(m_is_inited);
    Assert(!m_is_inited_phase2);
    /* CacheIdGetGlobalSysTupCache maybe fail when memory fault */
    Assert(m_global_systupcache == NULL);
    /* for now we even dont know which db to connect */
    if (m_relinfo.cc_relisshared) {
        m_db_id = InvalidOid;
        GlobalSysTabCache *global_shared_systab = t_thrd.lsc_cxt.lsc->GetSharedSysTabCache();
        m_global_systupcache = global_shared_systab->CacheIdGetGlobalSysTupCache(m_cache_id);
    } else {
        Assert(CheckMyDatabaseMatch());
        GlobalSysTabCache *global_systabcache = t_thrd.lsc_cxt.lsc->GetGlobalSysTabCache();
        m_db_id = t_thrd.lsc_cxt.lsc->my_database_id;
        m_global_systupcache = global_systabcache->CacheIdGetGlobalSysTupCache(m_cache_id);
    }
    Assert(m_global_systupcache != NULL);
    m_relinfo.cc_relname = m_global_systupcache->GetCCRelName();
    for (int i = 0; i < CATCACHE_MAXKEYS; i++) {
        m_relinfo.cc_hashfunc[i] = m_global_systupcache->GetCCHashFunc()[i];
        m_relinfo.cc_fastequal[i] = m_global_systupcache->GetCCFastEqual()[i];
    }
    m_relinfo.cc_tupdesc = m_global_systupcache->GetCCTupleDesc();
    m_is_inited_phase2 = true;
}

void LocalSysTupCache::RemoveTailTupleElements(Index hash_index)
{
    bool listBelowThreshold = GetBucket(hash_index)->dll_len < MAX_LSC_LIST_LENGTH;
    if (listBelowThreshold && !t_thrd.lsc_cxt.lsc->LocalSysDBCacheNeedSwapOut()) {
        return;
    }
    Dllist *list = GetBucket(hash_index);
    for (Dlelem *elt = DLGetTail(list); elt != NULL;) {
        LocalCatCTup *ct = (LocalCatCTup *)DLE_VAL(elt);
        elt = DLGetPred(elt);
        if (ct->refcount != 0) {
            DLMoveToFront(&ct->cache_elem);
            /* lsc do lru strategy, so the front of this cl are all refered probably, just break */
            break;
        }
        HandleDeadLocalCatCTup(ct);
    }
}

void LocalSysTupCache::RemoveTailListElements()
{
    bool listBelowThreshold = cc_lists.dll_len < MAX_LSC_LIST_LENGTH;
    if (listBelowThreshold && !t_thrd.lsc_cxt.lsc->LocalSysDBCacheNeedSwapOut()) {
        return;
    }
    int swapout_count = 0;
    int max_swapout_count_once = cc_lists.dll_len >> 1;
    for (Dlelem *elt = DLGetTail(&cc_lists); elt != NULL;) {
        LocalCatCList *cl = (LocalCatCList *)DLE_VAL(elt);
        elt = DLGetPred(elt);
        if (cl->refcount != 0) {
            DLMoveToFront(&cl->cache_elem);
            /* lsc do lru strategy, so the front of this cl are all refered probably, just break */
            break;
        }
        HandleDeadLocalCatCList(cl);
        swapout_count++;
        /* lsc is limitted by local_syscache_threshold, so dont be aggressive to swapout */
        if (listBelowThreshold || swapout_count > max_swapout_count_once) {
            break;
        }
    }
}

LocalCatCTup *LocalSysTupCache::SearchTupleFromGlobal(Datum *arguments, uint32 hash_value, Index hash_index, int level)
{
    LocalCatCTup *ct = NULL;
    ResourceOwnerEnlargeGlobalCatCTup(LOCAL_SYSDB_RESOWNER);
    GlobalCatCTup *global_ct;
    /* gsc only cache snapshotnow
     * for rls, we cann't know how to store the rls info, so dont cache it */
    bool bypass_gsc = HistoricSnapshotActive() ||
        m_global_systupcache->enable_rls ||
        !g_instance.global_sysdbcache.hot_standby ||
        unlikely(!IsPrimaryRecoveryFinished());
    if (invalid_entries.ExistTuple(hash_value) || bypass_gsc) {
        global_ct = m_global_systupcache->SearchTupleFromFile(hash_value, arguments, true);
    } else {
        global_ct = m_global_systupcache->SearchTuple(hash_value, arguments);
    }

    /* In bootstrap mode, we don't build negative entries, because the cache
        * invalidation mechanism isn't alive and can't clear them if the tuple
        * gets created later.	(Bootstrap doesn't do UPDATEs, so it doesn't need
        * cache inval for that.)
        */
    if (global_ct == NULL) {
        if (IsBootstrapProcessingMode()) {
            return NULL;
        }
        ct = CreateLocalCatCTup(NULL, arguments, hash_value, hash_index);
    } else {
        Assert(global_ct->refcount > 0);
        ResourceOwnerRememberGlobalCatCTup(LOCAL_SYSDB_RESOWNER, global_ct);
        ct = CreateLocalCatCTup(global_ct, arguments, hash_value, hash_index);
        ResourceOwnerForgetGlobalCatCTup(LOCAL_SYSDB_RESOWNER, global_ct);
    }
    return ct;
}

/*
 * Work-horse for SearchTuple/SearchTupleN.
 */
LocalCatCTup *LocalSysTupCache::SearchTupleInternal(int nkeys, Datum v1, Datum v2, Datum v3, Datum v4, int level)
{
    Assert(m_relinfo.cc_nkeys == nkeys);
    SearchCatCacheCheck();
    FreeDeadCts();
    FreeDeadCls();
    cc_searches++;
    /* Initialize local parameter array */
    Datum arguments[CATCACHE_MAXKEYS];
    arguments[0] = v1;
    arguments[1] = v2;
    arguments[2] = v3;
    arguments[3] = v4;
    /*
     * find the hash bucket in which to look for the tuple
     */
    uint32 hash_value = CatalogCacheComputeHashValue(m_relinfo.cc_hashfunc, nkeys, arguments);
    Index hash_index = HASH_INDEX(hash_value, (uint32)cc_nbuckets);
    /*
     * scan the hash bucket until we find a match or exhaust our tuples
     * remove dead tuple by the way
     */
    bool found = false;
    LocalCatCTup *ct = NULL;
    for (Dlelem *elt = DLGetHead(GetBucket(hash_index)); elt;) {
        ct = (LocalCatCTup *)DLE_VAL(elt);
        elt = DLGetSucc(elt);
        if (unlikely(ct->hash_value != hash_value)) {
            continue; /* quickly skip entry if wrong hash val */
        }
        if (unlikely(!CatalogCacheCompareTuple(m_relinfo.cc_fastequal, nkeys, ct->keys, arguments))) {
            continue;
        }
        /*
         * We found a match in the cache.  Move it to the front of the list
         * for its hashbucket, in order to speed subsequent searches.  (The
         * most frequently accessed elements in any hashbucket will tend to be
         * near the front of the hashbucket's list.)
         */
        DLMoveToFront(&ct->cache_elem);
        found = true;
        break;
    }

    /* if not found, search from global cache */
    if (unlikely(!found)) {
        ct = SearchTupleFromGlobal(arguments, hash_value, hash_index, level);
        if (unlikely(ct == NULL)) {
            return NULL;
        }
    }

    /*
     * If it's a positive entry, bump its refcount and return it. If it's
     * negative, we can report failure to the caller.
     */
    if (likely(ct->global_ct != NULL)) {
        CACHE3_elog(DEBUG2, "SearchLocalCatCache(%s): found in bucket %d", m_relinfo.cc_relname, hash_index);
        ResourceOwnerEnlargeLocalCatCTup(LOCAL_SYSDB_RESOWNER);
        ct->refcount++;
        cc_hits++;
        ResourceOwnerRememberLocalCatCTup(LOCAL_SYSDB_RESOWNER, ct);
    } else {
        CACHE3_elog(DEBUG2, "SearchLocalCatCache(%s): found neg entry in bucket %d", m_relinfo.cc_relname, hash_index);
        cc_neg_hits++;
        ct = NULL;
    }
    if (unlikely(!found)) {
        RemoveTailTupleElements(hash_index);
    }

    return ct;
}

/*
 *		Create a new CatCTup entry, point to global_ct, The new entry initially has refcount 0.
 */
LocalCatCTup *LocalSysTupCache::CreateLocalCatCTup(GlobalCatCTup *global_ct, Datum *arguments, uint32 hash_value,
                                                   Index hash_index)
{
    MemoryContext oldcxt = MemoryContextSwitchTo(m_local_mem_cxt);
    LocalCatCTup *ct = (LocalCatCTup *)palloc(sizeof(LocalCatCTup));

    /*
     * Finish initializing the CatCTup header, and add it to the cache's
     * linked list and counts.
     */
    ct->ct_magic = CT_MAGIC;
    DLInitElem(&ct->cache_elem, (void *)ct);
    ct->refcount = 0;
    ct->hash_value = hash_value;
    ct->global_ct = global_ct;
    /* palloc maybe fail, but that means global_ct is null, so nothing need to do */
    if (global_ct != NULL) {
        errno_t rc = memcpy_s(ct->keys, sizeof(Datum) * CATCACHE_MAXKEYS,
            global_ct->keys, sizeof(Datum) * CATCACHE_MAXKEYS);
        securec_check(rc, "", "");
    } else {
        errno_t rc = memset_s(ct->keys, m_relinfo.cc_nkeys * sizeof(Datum), 0, m_relinfo.cc_nkeys * sizeof(Datum));
        securec_check(rc, "\0", "\0");
        CatCacheCopyKeys(m_relinfo.cc_tupdesc, m_relinfo.cc_nkeys, m_relinfo.cc_keyno, arguments, ct->keys);
    }
    MemoryContextSwitchTo(oldcxt);
    DLAddHead(&cc_buckets[hash_index], &ct->cache_elem);
    return ct;
}

LocalCatCList *LocalSysTupCache::SearchListFromGlobal(int nkeys, Datum *arguments, uint32 hash_value, int level)
{
    ResourceOwnerEnlargeGlobalCatCList(LOCAL_SYSDB_RESOWNER);
    /*
     * List was not found in cache, so we have to build it by reading the
     * relation.  For each matching tuple found in the relation, use an
     * existing cache entry if possible, else build a new one.
     *
     * We have to bump the member refcounts temporarily to ensure they won't
     * get dropped from the cache while loading other members. We use a PG_TRY
     * block to ensure we can undo those refcounts if we get an error before
     * we finish constructing the CatCList.
     */
    bool bypass_gsc = HistoricSnapshotActive() ||
        m_global_systupcache->enable_rls ||
        !g_instance.global_sysdbcache.hot_standby ||
        unlikely(!IsPrimaryRecoveryFinished());
    GlobalCatCList *global_cl;
    if (invalid_entries.ExistList() || bypass_gsc) {
        global_cl = m_global_systupcache->SearchListFromFile(hash_value, nkeys, arguments, true);
    } else {
        global_cl = m_global_systupcache->SearchList(hash_value, nkeys, arguments);
    }
    Assert(global_cl != NULL);

    MemoryContext oldcxt = MemoryContextSwitchTo(m_local_mem_cxt);
    LocalCatCList *new_cl = (LocalCatCList *)palloc0(sizeof(LocalCatCList));
    MemoryContextSwitchTo(oldcxt);
    new_cl->cl_magic = CL_MAGIC;
    new_cl->hash_value = hash_value;
    DLInitElem(&new_cl->cache_elem, new_cl);
    errno_t rc = memcpy_s(new_cl->keys, nkeys * sizeof(Datum), global_cl->keys, nkeys * sizeof(Datum));
    securec_check(rc, "", "");
    new_cl->refcount = 1; /* for the moment */
    new_cl->ordered = global_cl->ordered;
    new_cl->nkeys = nkeys;
    new_cl->n_members = global_cl->n_members;
    new_cl->global_cl = global_cl;
    new_cl->systups = (CatCTup **)global_cl->members;
    ResourceOwnerForgetGlobalCatCList(LOCAL_SYSDB_RESOWNER, global_cl);
    DLAddHead(&cc_lists, &new_cl->cache_elem);
    CACHE3_elog(DEBUG2, "SearchLocalCatCacheList(%s): made list of %d members",
        m_relinfo.cc_relname, new_cl->n_members);
    return new_cl;
}

/*
 *	SearchListInternal
 *
 *		Generate a list of all tuples matching a partial key (that is,
 *		a key specifying just the first K of the cache's N key columns).
 *
 *		The caller must not modify the list object or the pointed-to tuples,
 *		and must call ReleaseLocalCatCList() when done with the list.
 */
LocalCatCList *LocalSysTupCache::SearchListInternal(int nkeys, Datum v1, Datum v2, Datum v3, Datum v4, int level)
{
    SearchCatCacheCheck();
    Assert(nkeys > 0 && nkeys < m_relinfo.cc_nkeys);
    cc_lsearches++;

    /* Initialize local parameter array */
    Datum arguments[CATCACHE_MAXKEYS];
    arguments[0] = v1;
    arguments[1] = v2;
    arguments[2] = v3;
    arguments[3] = v4;
    /*
     * compute a hash value of the given keys for faster search.  We don't
     * presently divide the CatCList items into buckets, but this still lets
     * us skip non-matching items quickly most of the time.
     */
    uint32 hash_value = CatalogCacheComputeHashValue(m_relinfo.cc_hashfunc, nkeys, arguments);

    ResourceOwnerEnlargeLocalCatCList(LOCAL_SYSDB_RESOWNER);
    /*
     * scan the items until we find a match or exhaust our list
     * remove dead list by the way
     */
    bool found = false;
    LocalCatCList *cl = NULL;
    for (Dlelem *elt = DLGetHead(&cc_lists); elt; elt = DLGetSucc(elt)) {
        cl = (LocalCatCList *)DLE_VAL(elt);
        if (likely(cl->hash_value != hash_value)) {
            continue; /* quickly skip entry if wrong hash val */
        }
        if (unlikely(cl->nkeys != nkeys)) {
            continue;
        }
        if (unlikely(!CatalogCacheCompareTuple(m_relinfo.cc_fastequal, nkeys, cl->keys, arguments))) {
            continue;
        }
        DLMoveToFront(&cl->cache_elem);
        /* Bump the list's refcount */
        cl->refcount++;
        CACHE2_elog(DEBUG2, "SearchLocalCatCacheList(%s): found list", m_relinfo.cc_relname);
        cc_lhits++;
        found = true;
        ResourceOwnerRememberLocalCatCList(LOCAL_SYSDB_RESOWNER, cl);
        break;
    }

    if (unlikely(!found)) {
        cl = SearchListFromGlobal(nkeys, arguments, hash_value, level);
        ResourceOwnerRememberLocalCatCList(LOCAL_SYSDB_RESOWNER, cl);
        RemoveTailListElements();
    }

    return cl;
}

uint32 LocalSysTupCache::GetCatCacheHashValue(Datum v1, Datum v2, Datum v3, Datum v4)
{
    InitPhase2();
    Datum arguments[CATCACHE_MAXKEYS];
    arguments[0] = v1;
    arguments[1] = v2;
    arguments[2] = v3;
    arguments[3] = v4;
    return CatalogCacheComputeHashValue(m_relinfo.cc_hashfunc, m_relinfo.cc_nkeys, arguments);
}

#ifndef ENABLE_MULTIPLE_NODES
LocalCatCTup *LocalSysTupCache::SearchTupleFromGlobalForProcAllArgs(
    Datum *arguments, uint32 hash_value, Index hash_index, oidvector* argModes)
{
    LocalCatCTup *ct = NULL;
    ResourceOwnerEnlargeGlobalCatCTup(LOCAL_SYSDB_RESOWNER);
    GlobalCatCTup *global_ct;
    /* gsc only cache snapshotnow
     * for rls, we cann't know how to store the rls info, so dont cache it */
    bool bypass_gsc = HistoricSnapshotActive() ||
        m_global_systupcache->enable_rls ||
        !g_instance.global_sysdbcache.hot_standby ||
        unlikely(!IsPrimaryRecoveryFinished());
    if (invalid_entries.ExistTuple(hash_value) || bypass_gsc) {
        global_ct = m_global_systupcache->SearchTupleFromFileWithArgModes(hash_value, arguments, argModes, true);
    } else {
        global_ct = m_global_systupcache->SearchTupleWithArgModes(hash_value, arguments, argModes);
    }

    /*
     * In this specific function for procallargs, we no longer build
     * negative entry any more, because when we create a overload pg-style
     * function with the same number intype parameters and different outtype
     * parameters (a in int) vs (a in int, b out int), syscache will find no
     * suitable cache tuple and make a new negative cache entry, but error will
     * raise in such case, and no one will free the negative cache entry!
     * In fact, the case metioned above should find a suitable tuple to return
     * the caller, but for the reason we adapt a suit of Specific Function
     * to support ProcedureCreate, syscache couldn't find the tuple. Someone
     * may find new methods to solve the problem and refactor this!
     */
    if (global_ct == NULL) {
        return NULL;
    }

    Assert(global_ct == NULL || global_ct->refcount > 0);
    ResourceOwnerRememberGlobalCatCTup(LOCAL_SYSDB_RESOWNER, global_ct);
    ct = CreateLocalCatCTup(global_ct, arguments, hash_value, hash_index);
    ResourceOwnerForgetGlobalCatCTup(LOCAL_SYSDB_RESOWNER, global_ct);
    return ct;
}

/*
 * Specific SearchLocalCatCTuple Function to support ProcedureCreate!
 */
LocalCatCTup *LocalSysTupCache::SearchLocalCatCTupleForProcAllArgs(
    Datum v1, Datum v2, Datum v3, Datum v4, Datum proArgModes)
{
    InitPhase2();

    SearchCatCacheCheck();
    FreeDeadCts();
    FreeDeadCls();
    cc_searches++;

    Datum arguments[CATCACHE_MAXKEYS];
    /*
     * Logic here is the same in Sys/CatCache Search
     */
    oidvector* allArgTypes = (oidvector*)DatumGetPointer(v2);

    Assert(allArgTypes != NULL);

    oidvector* argModes = ConvertArgModesToMd5Vector(proArgModes);
    oidvector* v2WithArgModes = MergeOidVector(allArgTypes, argModes);
    Datum newKey2 = PointerGetDatum(v2WithArgModes);

    /* Initialize local parameter array */
    arguments[0] = v1;
    arguments[1] = newKey2;
    arguments[2] = v3;
    arguments[3] = v4;
    /*
     * find the hash bucket in which to look for the tuple
     */
    uint32 hash_value = CatalogCacheComputeHashValue(m_relinfo.cc_hashfunc, m_relinfo.cc_nkeys, arguments);
    Index hash_index = HASH_INDEX(hash_value, (uint32)cc_nbuckets);

    /* reset parameter array */
    pfree_ext(v2WithArgModes);
    arguments[1] = v2;

    /*
     * scan the hash bucket until we find a match or exhaust our tuples
     * remove dead tuple by the way
     */
    bool found = false;
    LocalCatCTup *ct = NULL;
    for (Dlelem *elt = DLGetHead(GetBucket(hash_index)); elt;) {
        ct = (LocalCatCTup *)DLE_VAL(elt);
        elt = DLGetSucc(elt);
        if (unlikely(ct->hash_value != hash_value)) {
            continue; /* quickly skip entry if wrong hash val */
        }
        if (unlikely(!CatalogCacheCompareTuple(m_relinfo.cc_fastequal, m_relinfo.cc_nkeys, ct->keys, arguments))) {
            continue;
        }

        /*
         * The comparison of hashvalue and keys is not enough.
         */
        if (!IsProArgModesEqualByTuple(&ct->global_ct->tuple, m_relinfo.cc_tupdesc, argModes)) {
            continue;
        }
        /*
         * We found a match in the cache.  Move it to the front of the list
         * for its hashbucket, in order to speed subsequent searches.  (The
         * most frequently accessed elements in any hashbucket will tend to be
         * near the front of the hashbucket's list.)
         */
        DLMoveToFront(&ct->cache_elem);
        found = true;
        break;
    }

    /* if not found, search from global cache */
    if (unlikely(!found)) {
        ct = SearchTupleFromGlobalForProcAllArgs(arguments, hash_value, hash_index, argModes);
        if (ct == NULL) {
            pfree_ext(argModes);
            return NULL;
        }
    }
    /*
     * If it's a positive entry, bump its refcount and return it. If it's
     * negative, we can report failure to the caller.
     */
    if (likely(ct->global_ct != NULL)) {
        CACHE3_elog(DEBUG2, "SearchLocalCatCache(%s): found in bucket %d", m_relinfo.cc_relname, hash_index);
        ResourceOwnerEnlargeLocalCatCTup(LOCAL_SYSDB_RESOWNER);
        ct->refcount++;
        cc_hits++;
        ResourceOwnerRememberLocalCatCTup(LOCAL_SYSDB_RESOWNER, ct);
    }
    if (unlikely(!found)) {
        RemoveTailTupleElements(hash_index);
    }

    pfree_ext(argModes);
    return ct;
}
#endif
