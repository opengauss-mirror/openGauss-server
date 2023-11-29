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

#include "utils/knl_globalsystupcache.h"
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
#include "catalog/gs_obsscaninfo.h"
#include "catalog/gs_policy_label.h"
#include "catalog/indexing.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_amop.h"
#include "catalog/pg_amproc.h"
#include "catalog/pg_auth_members.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_obsscaninfo.h"
#include "catalog/pg_cast.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_conversion.h"
#include "catalog/pg_database.h"
#include "catalog/pg_db_role_setting.h"
#include "catalog/pg_default_acl.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_description.h"
#include "catalog/pg_directory.h"
#include "catalog/pg_enum.h"
#include "catalog/pg_foreign_data_wrapper.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_job.h"
#include "catalog/pg_job_proc.h"
#include "catalog/pg_language.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_object.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_opfamily.h"
#include "catalog/pg_partition.h"
#include "catalog/pg_partition_fn.h"
#include "catalog/pg_hashbucket.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_proc_fn.h"
#include "catalog/pg_range.h"
#include "catalog/pg_rewrite.h"
#include "catalog/pg_seclabel.h"
#include "catalog/pg_shseclabel.h"
#include "catalog/pg_shdescription.h"
#include "catalog/pg_shdepend.h"
#include "catalog/pg_statistic.h"
#include "catalog/pg_statistic_ext.h"
#include "catalog/pg_synonym.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_ts_config.h"
#include "catalog/pg_ts_config_map.h"
#include "catalog/pg_ts_dict.h"
#include "catalog/pg_ts_parser.h"
#include "catalog/pg_ts_template.h"
#include "catalog/pg_type.h"
#include "catalog/pg_user_mapping.h"
#include "catalog/pg_user_status.h"
#include "catalog/pg_extension_data_source.h"
#include "catalog/pg_streaming_stream.h"
#include "catalog/pg_streaming_cont_query.h"
#include "catalog/heap.h"
#include "executor/executor.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "parser/parse_relation.h"
#include "parser/parse_type.h"
#include "pgstat.h"
#ifdef ENABLE_MULTIPLE_NODES
#include "catalog/pgxc_class.h"
#include "catalog/pgxc_node.h"
#include "catalog/pgxc_group.h"
#include "catalog/pg_resource_pool.h"
#include "catalog/pg_workload_group.h"
#include "catalog/pg_app_workloadgroup_mapping.h"
#endif
#ifdef CatCache_STATS
#include "storage/ipc.h" /* for on_proc_exit */
#endif
#include "storage/lmgr.h"
#include "storage/sinvaladt.h"
#include "utils/acl.h"
#include "utils/atomic.h"
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
#include "utils/snapmgr.h"
#include "utils/knl_relcache.h"
#include "utils/knl_catcache.h"
#include "utils/knl_globaltabdefcache.h"
#include "utils/sec_rls_utils.h"

void GlobalCatCTup::Release()
{
    /* Decrement the reference count of a GlobalCatCache tuple */
    Assert(ct_magic == CT_MAGIC);
    Assert(refcount > 0);
    my_cache->ReleaseGlobalCatCTup(this);
}

void GlobalCatCList::Release()
{
    /* Safety checks to ensure we were handed a cache entry */
    Assert(cl_magic == CL_MAGIC);
    Assert(refcount > 0);
    my_cache->ReleaseGlobalCatCList(this);
}

void GlobalSysTupCache::ReleaseGlobalCatCTup(GlobalCatCTup *ct)
{
    if (unlikely(!ct->canInsertGSC)) {
        pfree(ct);
        return;
    }
    (void)pg_atomic_fetch_sub_u64(&ct->refcount, 1);
}

void GlobalSysTupCache::ReleaseGlobalCatCList(GlobalCatCList *cl)
{
    if (unlikely(!cl->canInsertGSC)) {
        FreeGlobalCatCList(cl);
        return;
    }
    (void)pg_atomic_fetch_sub_u64(&cl->refcount, 1);
}

static uint64 GetClEstimateSize(GlobalCatCList *cl)
{
    uint64 cl_size = GetMemoryChunkSpace(cl) +
        cl->nkeys * (NAMEDATALEN + CHUNK_ALGIN_PAD); /* estimate space of keys */
    return cl_size;
}

static uint64 GetCtEstimateSize(GlobalCatCTup *ct)
{
    uint64 ct_size = GetMemoryChunkSpace(ct);
    return ct_size;
}

void GlobalSysTupCache::AddHeadToCCList(GlobalCatCList *cl)
{
    uint64 cl_size = GetClEstimateSize(cl);
    /* record space of list */
    m_dbEntry->MemoryEstimateAdd(cl_size);
    pg_atomic_fetch_add_u64(m_tup_space, cl_size);
    DLAddHead(&cc_lists, &cl->cache_elem);
}
void GlobalSysTupCache::RemoveElemFromCCList(GlobalCatCList *cl)
{
    uint64 cl_size = GetClEstimateSize(cl);
    m_dbEntry->MemoryEstimateSub(cl_size);
    pg_atomic_fetch_sub_u64(m_tup_space, cl_size);
    DLRemove(&cl->cache_elem);
}

void GlobalSysTupCache::AddHeadToBucket(Index hash_index, GlobalCatCTup *ct)
{
    uint64 ct_size = GetCtEstimateSize(ct);
    pg_atomic_fetch_add_u64(m_tup_space, ct_size);
    /* record space of tup */
    m_dbEntry->MemoryEstimateAdd(ct_size);
    pg_atomic_fetch_add_u64(m_tup_count, 1);
    DLAddHead(&cc_buckets[hash_index], &ct->cache_elem);
}

void GlobalSysTupCache::RemoveElemFromBucket(GlobalCatCTup *ct)
{
    uint64 ct_size = GetCtEstimateSize(ct);
    pg_atomic_fetch_sub_u64(m_tup_space, ct_size);
    /* free space of tup */
    m_dbEntry->MemoryEstimateSub(ct_size);
    pg_atomic_fetch_sub_u64(m_tup_count, 1);
    DLRemove(&ct->cache_elem);
}

void GlobalSysTupCache::HandleDeadGlobalCatCTup(GlobalCatCTup *ct)
{
    /* this func run in wr lock, so dont call free directly */
    RemoveElemFromBucket(ct);
    ct->dead = true;
    if (ct->refcount == 0) {
        m_dead_cts.AddHead(&ct->cache_elem);
    } else {
        m_dead_cts.AddTail(&ct->cache_elem);
    }
}

void GlobalSysTupCache::FreeDeadCts()
{
    while (m_dead_cts.GetLength() > 0) {
        Dlelem *elt = m_dead_cts.RemoveHead();
        if (elt == NULL) {
            break;
        }
        GlobalCatCTup *ct = (GlobalCatCTup *)DLE_VAL(elt);
        if (ct->refcount != 0) {
            /* we move the active entry to tail of list and let next call free it */
            m_dead_cts.AddTail(&ct->cache_elem);
            break;
        } else {
            pfree(ct);
        }
    }
}

void GlobalSysTupCache::RemoveTailTupleElements(Index hash_index)
{
    /* only one thread can do swapout for the bucket */
    ResourceOwnerEnlargeGlobalIsExclusive(LOCAL_SYSDB_RESOWNER);
    if (!atomic_compare_exchange_u32(&m_is_tup_swappingouts[hash_index], 0, 1)) {
        return;
    }
    ResourceOwnerRememberGlobalIsExclusive(LOCAL_SYSDB_RESOWNER, &m_is_tup_swappingouts[hash_index]);

    bool listBelowThreshold = GetBucket(hash_index)->dll_len < MAX_GSC_LIST_LENGTH;

    uint64 swapout_count_once = 0;
    /* we are the only one to do swapout, so acquire wrlock is ok */
    PthreadRWlockWrlock(LOCAL_SYSDB_RESOWNER, &m_bucket_rw_locks[hash_index]);

    uint64 max_swapout_count_once = GetSwapOutNum(listBelowThreshold, GetBucket(hash_index)->dll_len);
    for (Dlelem *elt = DLGetTail(GetBucket(hash_index)); elt != NULL;) {
        GlobalCatCTup *ct = (GlobalCatCTup *)DLE_VAL(elt);
        elt = DLGetPred(elt);
        if (ct->refcount != 0) {
            /* we dont know how many ct are unused, maybe no one, so break to avoid meaningless work
             * whatever, another thread does swappout again */
            DLMoveToFront(&ct->cache_elem);
            break;
        }
        HandleDeadGlobalCatCTup(ct);
        swapout_count_once++;

        /* swapout elements as many as possible */
        if (swapout_count_once == max_swapout_count_once) {
            break;
        }
    }
    PthreadRWlockUnlock(LOCAL_SYSDB_RESOWNER, &m_bucket_rw_locks[hash_index]);

    Assert(m_is_tup_swappingouts[hash_index] == 1);
    atomic_compare_exchange_u32(&m_is_tup_swappingouts[hash_index], 1, 0);
    ResourceOwnerForgetGlobalIsExclusive(LOCAL_SYSDB_RESOWNER, &m_is_tup_swappingouts[hash_index]);
}

void GlobalSysTupCache::FreeGlobalCatCList(GlobalCatCList *cl)
{
    Assert(cl->refcount == 0 || !cl->canInsertGSC);
    for (int i = 0; i < cl->n_members; i++) {
        cl->members[i]->Release();
    }
    CatCacheFreeKeys(m_relinfo.cc_tupdesc, cl->nkeys, m_relinfo.cc_keyno, cl->keys);
    pfree(cl);
}

void GlobalSysTupCache::HandleDeadGlobalCatCList(GlobalCatCList *cl)
{
    /* this func run in wr lock, so dont call free directly */
    RemoveElemFromCCList(cl);
    if (cl->refcount == 0) {
        m_dead_cls.AddHead(&cl->cache_elem);
    } else {
        m_dead_cls.AddTail(&cl->cache_elem);
    }
}

void GlobalSysTupCache::FreeDeadCls()
{
    while (m_dead_cls.GetLength() > 0) {
        Dlelem *elt = m_dead_cls.RemoveHead();
        if (elt == NULL) {
            break;
        }
        GlobalCatCList *cl = (GlobalCatCList *)DLE_VAL(elt);
        if (cl->refcount != 0) {
            /* we move the active entry to tail of list and let next call free it */
            m_dead_cls.AddTail(&cl->cache_elem);
            break;
        } else {
            FreeGlobalCatCList(cl);
        }
    }
    FreeDeadCts();
}

void GlobalSysTupCache::RemoveTailListElements()
{
    /* only one thread can do swapout for the bucket */
    ResourceOwnerEnlargeGlobalIsExclusive(LOCAL_SYSDB_RESOWNER);
    if (!atomic_compare_exchange_u32(&m_is_list_swappingout, 0, 1)) {
        return;
    }
    ResourceOwnerRememberGlobalIsExclusive(LOCAL_SYSDB_RESOWNER, &m_is_list_swappingout);

    /* cllist search is slow, so limit cc_lists's length */
    bool listBelowThreshold = cc_lists.dll_len < MAX_GSC_LIST_LENGTH;

    uint64 swapout_count = 0;
    /* the code here is exclusive by set m_cclist_in_swapping 1, so acquire wrlock is ok */
    PthreadRWlockWrlock(LOCAL_SYSDB_RESOWNER, m_list_rw_lock);
    uint64 max_swapout_count_once = GetSwapOutNum(listBelowThreshold, cc_lists.dll_len);

    for (Dlelem *elt = DLGetTail(&cc_lists); elt != NULL;) {
        GlobalCatCList *cl = (GlobalCatCList *)DLE_VAL(elt);
        elt = DLGetPred(elt);
        if (cl->refcount != 0) {
            DLMoveToFront(&cl->cache_elem);
            /* we dont know how many cl are unused, maybe no one, so break to avoid meaningless work
             * whatever, another thread does swappout again */
            break;
        }
        HandleDeadGlobalCatCList(cl);
        swapout_count++;

        if (swapout_count > max_swapout_count_once) {
            break;
        }
    }
    PthreadRWlockUnlock(LOCAL_SYSDB_RESOWNER, m_list_rw_lock);

    Assert(m_is_list_swappingout == 1);
    atomic_compare_exchange_u32(&m_is_list_swappingout, 1, 0);
    ResourceOwnerForgetGlobalIsExclusive(LOCAL_SYSDB_RESOWNER, &m_is_list_swappingout);
}

void GlobalSysTupCache::RemoveAllTailElements()
{
    if (!m_isInited) {
        return;
    }
    RemoveTailListElements();
#ifndef ENABLE_LITE_MODE
    /* memory is under control, so stop swapout */
    if (g_instance.global_sysdbcache.MemoryUnderControl()) {
        return;
    }
#endif
    for (int hash_index = 0; hash_index < cc_nbuckets; hash_index++) {
        RemoveTailTupleElements(hash_index);
#ifndef ENABLE_LITE_MODE
        /* memory is under control, so stop swapout */
        if (g_instance.global_sysdbcache.MemoryUnderControl()) {
            break;
        }
#endif
    }
}

GlobalSysTupCache::GlobalSysTupCache(Oid dbOid, int cache_id, bool isShared, GlobalSysDBCacheEntry *entry)
{
    m_isInited = false;

    /* global systup share cxt group with global systab */
    m_searches = &entry->m_dbstat->tup_searches;
    m_hits = &entry->m_dbstat->tup_hits;
    m_newloads = &entry->m_dbstat->tup_newloads;
    m_dbEntry = entry;
    m_dbOid = dbOid;
    m_cache_id = cache_id;
    m_bucket_rw_locks = NULL;
    m_list_rw_lock = NULL;
    m_concurrent_lock = NULL;
    cc_id = -1;
    cc_nbuckets = -1;
    m_relinfo.cc_tupdesc = NULL;
    m_relinfo.cc_relname = NULL;
    m_relinfo.cc_tupdesc = NULL;
    m_relinfo.cc_reloid = -1;
    m_relinfo.cc_indexoid = -1;
    m_relinfo.cc_nkeys = -1;
    m_relinfo.cc_relisshared = isShared;
    Assert((isShared && m_dbOid == InvalidOid) || (!isShared && m_dbOid != InvalidOid));

    for (int i = 0; i < CATCACHE_MAXKEYS; i++) {
        m_relinfo.cc_keyno[i] = -1;
        errno_t rc = memset_s(&cc_skey[i], sizeof(ScanKeyData), 0, sizeof(ScanKeyData));
        securec_check(rc, "", "");
        m_relinfo.cc_hashfunc[i] = NULL;
        m_relinfo.cc_fastequal[i] = NULL;
    }
    cc_buckets = NULL;
    m_is_tup_swappingouts = NULL;
    m_is_list_swappingout = 0;
    enable_rls = false;
}

void GlobalSysTupCache::SetStatInfoPtr(volatile uint64 *tup_count, volatile uint64 *tup_space)
{
    m_tup_count = tup_count;
    m_tup_space = tup_space;
}

/*cat tuple***********************************************************************************************************/
/* SEARCH_TUPLE_SKIP used by searchtupleinternal */
GlobalCatCTup *GlobalSysTupCache::FindSearchKeyTupleFromCache(InsertCatTupInfo *tup_info, int *location)
{
    uint32 hash_value = tup_info->hash_value;
    Index hash_index = tup_info->hash_index;
    Datum *arguments = tup_info->arguments;
    int index = 0;
    for (Dlelem *elt = DLGetHead(GetBucket(hash_index)); elt != NULL; elt = DLGetSucc(elt)) {
        index++;
        GlobalCatCTup *ct = (GlobalCatCTup *)DLE_VAL(elt);
        if (ct->hash_value != hash_value) {
            continue;
        }
        if (!CatalogCacheCompareTuple(m_relinfo.cc_fastequal, m_relinfo.cc_nkeys, ct->keys, arguments)) {
            continue;
        }
        if (unlikely(u_sess->attr.attr_common.IsInplaceUpgrade) && ct->tuple.t_self.ip_posid == 0) {
            Assert(ct->tuple.t_tableOid == InvalidOid);
            continue;
        }
        pg_atomic_fetch_add_u64(&ct->refcount, 1);
        *location = index;
        return ct;
    }
    return NULL;
}

/* SCAN_TUPLE_SKIP used by searchtupleinternal */
GlobalCatCTup *GlobalSysTupCache::FindScanKeyTupleFromCache(InsertCatTupInfo *tup_info)
{
    uint32 hash_value = tup_info->hash_value;
    Index hash_index = tup_info->hash_index;
    Datum *arguments = tup_info->arguments;
    for (Dlelem *elt = DLGetHead(GetBucket(hash_index)); elt != NULL; elt = DLGetSucc(elt)) {
        GlobalCatCTup *ct = (GlobalCatCTup *)DLE_VAL(elt);
        if (ct->hash_value != hash_value) {
            continue;
        }
        if (!CatalogCacheCompareTuple(m_relinfo.cc_fastequal, m_relinfo.cc_nkeys, ct->keys, arguments)) {
            continue;
        }
        if (unlikely(u_sess->attr.attr_common.IsInplaceUpgrade) && ct->tuple.t_self.ip_posid == 0) {
            Assert(ct->tuple.t_tableOid == InvalidOid);
            continue;
        }
        pg_atomic_fetch_add_u64(&ct->refcount, 1);
        return ct;
    }
    return NULL;
}

/* PROC_LIST_SKIP used by SearchBuiltinProcCacheList */
GlobalCatCTup *GlobalSysTupCache::FindHashTupleFromCache(InsertCatTupInfo *tup_info)
{
    uint32 hash_value = tup_info->hash_value;
    Index hash_index = tup_info->hash_index;
    for (Dlelem *elt = DLGetHead(GetBucket(hash_index)); elt != NULL; elt = DLGetSucc(elt)) {
        GlobalCatCTup *ct = (GlobalCatCTup *)DLE_VAL(elt);
        if (ct->hash_value != hash_value) {
            continue; /* quickly skip entry if wrong hash val */
        }
        pg_atomic_fetch_add_u64(&ct->refcount, 1);
        return ct;
    }
    return NULL;
}

/* PGATTR_LIST_SKIP used by SearchPgAttributeCacheList */
GlobalCatCTup *GlobalSysTupCache::FindPgAttrTupleFromCache(InsertCatTupInfo *tup_info)
{
    Index hash_index = tup_info->hash_index;
    int16 attnum = tup_info->attnum;
    CatalogRelationBuildParam *catalogDesc = tup_info->catalogDesc;
    for (Dlelem *elt = DLGetHead(GetBucket(hash_index)); elt != NULL; elt = DLGetSucc(elt)) {
        GlobalCatCTup *ct = (GlobalCatCTup *)DLE_VAL(elt);
        bool attnumIsNull = false;
        int curAttnum = DatumGetInt16(SysCacheGetAttr(cc_id, &ct->tuple, Anum_pg_attribute_attnum, &attnumIsNull));
        /* quickly skip entry if wrong tuple */
        if ((attnum < catalogDesc->natts && curAttnum != attnum) ||
            (attnum >= catalogDesc->natts && curAttnum != -(attnum - catalogDesc->natts + 1))) {
            continue;
        }
        pg_atomic_fetch_add_u64(&ct->refcount, 1);
        return ct;
    }
    return NULL;
}

/* SCAN_LIST_SKIP used by searchlistinternal when scan */
GlobalCatCTup *GlobalSysTupCache::FindSameTupleFromCache(InsertCatTupInfo *tup_info)
{
    uint32 hash_value = tup_info->hash_value;
    Index hash_index = tup_info->hash_index;
    HeapTuple ntp = tup_info->ntp;
    for (Dlelem *elt = DLGetHead(GetBucket(hash_index)); elt != NULL; elt = DLGetSucc(elt)) {
        GlobalCatCTup *ct = (GlobalCatCTup *)DLE_VAL(elt);
        if (ct->hash_value != hash_value) {
            continue; /* ignore dead entries */
        }
        if (IsProcCache(m_relinfo.cc_reloid) && IsSystemObjOid(HeapTupleGetOid(&(ct->tuple))) &&
            likely(u_sess->attr.attr_common.IsInplaceUpgrade == false)) {
            continue;
        }
        if (IsAttributeCache(m_relinfo.cc_reloid)) {
            bool attIsNull = false;
            Oid attrelid =
                DatumGetObjectId(SysCacheGetAttr(cc_id, &(ct->tuple), Anum_pg_attribute_attrelid, &attIsNull));
            if (IsSystemObjOid(attrelid) && IsValidCatalogParam(GetCatalogParam(attrelid))) {
                continue;
            }
        }

        if (!ItemPointerEqualsNoCheck(&(ct->tuple.t_self), &(ntp->t_self))) {
            continue; /* not same tuple */
        }
        pg_atomic_fetch_add_u64(&ct->refcount, 1);
        return ct;
    }
    return NULL;
}

/**
 * insert a new tuple into globalcatcache, if there is already one, do nothing
 */
GlobalCatCTup *GlobalSysTupCache::InsertHeapTupleIntoGlobalCatCache(InsertCatTupInfo *tup_info)
{
    /* palloc before write lock */
    /* Allocate memory for GlobalCatCTup and the cached tuple in one go */
    MemoryContext oldcxt = MemoryContextSwitchTo(m_dbEntry->GetRandomMemCxt());
    GlobalCatCTup *new_ct = (GlobalCatCTup *)palloc(sizeof(GlobalCatCTup) + MAXIMUM_ALIGNOF + tup_info->ntp->t_len);
    MemoryContextSwitchTo(oldcxt);
    new_ct->ct_magic = CT_MAGIC;
    /* releases and free by my_cache when palloc fail */
    new_ct->my_cache = this;
    new_ct->dead = false;
    new_ct->hash_value = tup_info->hash_value;
    DLInitElem(&new_ct->cache_elem, (void *)new_ct);
    CopyTupleIntoGlobalCatCTup(new_ct, tup_info->ntp);
    /* not find, now we insert the tuple into cache and unlock lock */
    new_ct->refcount = 1;
    new_ct->canInsertGSC = true;

    /* insert bucket, only w bucket_lock, we has r m_concurrent_lock already */
    pthread_rwlock_t *bucket_lock = &m_bucket_rw_locks[tup_info->hash_index];
    /* find again, in case insert op when we create tuple */
    PthreadRWlockWrlock(LOCAL_SYSDB_RESOWNER, bucket_lock);
    GlobalCatCTup *ct = FindTupleFromCache(tup_info);
    if (unlikely(ct != NULL)) {
        /* other thread has inserted one */
        PthreadRWlockUnlock(LOCAL_SYSDB_RESOWNER, bucket_lock);
        pfree_ext(new_ct);
        return ct;
    }

    AddHeadToBucket(tup_info->hash_index, new_ct);
    PthreadRWlockUnlock(LOCAL_SYSDB_RESOWNER, bucket_lock);
    return new_ct;
}

static inline MemoryContext GetLSCMemcxt(int cache_id)
{
    /* use lsc's memcxt to palloc, and insert the element into lsc's hashtable */
    return t_thrd.lsc_cxt.lsc->systabcache.GetLocalTupCacheMemoryCxt(cache_id);
}
/**
 * insert committed tuple into globalcatcache, uncommitted tuple into localcatcache
 */
GlobalCatCTup *GlobalSysTupCache::InsertHeapTupleIntoLocalCatCache(InsertCatTupInfo *tup_info)
{
    /* Allocate memory for GlobalCatCTup and the cached tuple in one struct */
    MemoryContext oldcxt =
        MemoryContextSwitchTo(GetLSCMemcxt(m_cache_id));
    GlobalCatCTup *new_ct = (GlobalCatCTup *)palloc(sizeof(GlobalCatCTup) + MAXIMUM_ALIGNOF + tup_info->ntp->t_len);
    MemoryContextSwitchTo(oldcxt);
    new_ct->ct_magic = CT_MAGIC;
    /* releases and free by my_cache when palloc fail */
    new_ct->my_cache = this;
    new_ct->dead = false;
    new_ct->hash_value = tup_info->hash_value;
    DLInitElem(&new_ct->cache_elem, (void *)new_ct);
    CopyTupleIntoGlobalCatCTup(new_ct, tup_info->ntp);
    new_ct->refcount = 1;
    new_ct->canInsertGSC = false;
    return new_ct;
}
/**
 * insert  tuple into globalcatcache memory, others into localcatcache memory
 */
GlobalCatCTup *GlobalSysTupCache::InsertHeapTupleIntoCatCacheInSingle(InsertCatTupInfo *tup_info)
{
    HeapTuple ntp = tup_info->ntp;
    if (HeapTupleHasExternal(ntp)) {
        tup_info->ntp = toast_flatten_tuple(ntp, m_relinfo.cc_tupdesc);
        Assert(tup_info->hash_value ==
            CatalogCacheComputeTupleHashValue(cc_id, m_relinfo.cc_keyno, m_relinfo.cc_tupdesc, m_relinfo.cc_hashfunc,
                m_relinfo.cc_reloid, m_relinfo.cc_nkeys, tup_info->ntp));
    }
    GlobalCatCTup *ct;
    if (tup_info->canInsertGSC) {
        /* the tuple must meet condition that xmin is committed and xmax == 0 */
        ct = InsertHeapTupleIntoGlobalCatCache(tup_info);
    } else {
        /* xmin uncommitted or xmax != 0, we dont care whether xmax is committed, just store it in localcatcache  */
        ct = InsertHeapTupleIntoLocalCatCache(tup_info);
    }
    if (tup_info->ntp != ntp) {
        heap_freetuple_ext(tup_info->ntp);
        tup_info->ntp = ntp;
    }
    return ct;
}

/**
 * insert tuple into globalcatcache memory, others into localcatcache memory
 * should call only by searchsyscachelist
 * we do this check because when accept inval msg, we clear cc_list, even they are valid
 */
GlobalCatCTup *GlobalSysTupCache::InsertHeapTupleIntoCatCacheInList(InsertCatTupInfo *tup_info)
{
    HeapTuple ntp = tup_info->ntp;
    if (HeapTupleHasExternal(ntp)) {
        tup_info->ntp = toast_flatten_tuple(ntp, m_relinfo.cc_tupdesc);
        Assert(tup_info->hash_value ==
            CatalogCacheComputeTupleHashValue(cc_id, m_relinfo.cc_keyno, m_relinfo.cc_tupdesc, m_relinfo.cc_hashfunc,
                m_relinfo.cc_reloid, m_relinfo.cc_nkeys, tup_info->ntp));
    }
    GlobalCatCTup *ct = NULL;
    if (tup_info->canInsertGSC) {
        /* insert bucket, only rd bucket_lock, built in func never be changed */
        pthread_rwlock_t *bucket_lock = &m_bucket_rw_locks[tup_info->hash_index];
        PthreadRWlockRdlock(LOCAL_SYSDB_RESOWNER, bucket_lock);
        ct = FindTupleFromCache(tup_info);
        PthreadRWlockUnlock(LOCAL_SYSDB_RESOWNER, bucket_lock);
        if (unlikely(ct == NULL)) {
            /* the tuple must meet condition that xmin is committed and xmax == 0 */
            ct = InsertHeapTupleIntoGlobalCatCache(tup_info);
        }
    } else {
        /* the tuple must meet condition that xmin is committed and xmax == 0 */
        ct = InsertHeapTupleIntoLocalCatCache(tup_info);
    }

    if (tup_info->ntp != ntp) {
        heap_freetuple_ext(tup_info->ntp);
        tup_info->ntp = ntp;
    }
    return ct;
}

GlobalCatCTup *GlobalSysTupCache::SearchMissFromProcAndAttribute(InsertCatTupInfo *tup_info)
{
    /* for now, tup_info->type is SCAN_TUPLE_SKIP. it's ok */
    Datum *arguments = tup_info->arguments;
    /* For search a function, we firstly try to search it in built-in function list */
    if (IsProcCache(m_relinfo.cc_reloid) && likely(u_sess->attr.attr_common.IsInplaceUpgrade == false)) {
        CACHE2_elog(DEBUG2, "SearchGlobalCatCacheMiss(%d): function not found in pg_proc", cc_id);
        HeapTuple ntp = SearchBuiltinProcCacheMiss(cc_id, m_relinfo.cc_nkeys, arguments);
        if (HeapTupleIsValid(ntp)) {
            CACHE2_elog(DEBUG2, "SearchGlobalCatCacheMiss(%d): match a built-in function", cc_id);
            tup_info->ntp = ntp;
            /* hard code, never change */
            Assert(ntp->t_tableOid == InvalidOid);
            tup_info->canInsertGSC = true;
            GlobalCatCTup *ct = InsertHeapTupleIntoCatCacheInSingle(tup_info);
            heap_freetuple(ntp);
            return ct;
        }
    }

    /* Insert hardcoded system catalogs' attributes into pg_attribute's syscache. */
    if (IsAttributeCache(m_relinfo.cc_reloid) && IsSystemObjOid(DatumGetObjectId(arguments[0]))) {
        CACHE2_elog(DEBUG2, "SearchGlobalCatCacheMiss: cat tuple not in cat cache %d", cc_id);
        HeapTuple ntp = SearchPgAttributeCacheMiss(cc_id, m_relinfo.cc_tupdesc, m_relinfo.cc_nkeys, arguments);
        if (HeapTupleIsValid(ntp)) {
            tup_info->ntp = ntp;
            /* hard code, never change */
            tup_info->canInsertGSC = true;
            GlobalCatCTup *ct = InsertHeapTupleIntoCatCacheInSingle(tup_info);
            heap_freetuple(ntp);
            return ct;
        }
    }
    return NULL;
}

void AcquireGSCTableReadLock(bool *has_concurrent_lock, pthread_rwlock_t *concurrent_lock)
{
    int cur_index = t_thrd.lsc_cxt.lsc->rdlock_info.count;
    if (unlikely(cur_index == MAX_GSC_READLOCK_COUNT) ||
        PthreadRWlockTryRdlock(LOCAL_SYSDB_RESOWNER, concurrent_lock) != 0) {
        *has_concurrent_lock = false;
        return;
    }
    *has_concurrent_lock = true;
    t_thrd.lsc_cxt.lsc->rdlock_info.concurrent_lock[cur_index] = concurrent_lock;
    t_thrd.lsc_cxt.lsc->rdlock_info.has_concurrent_lock[cur_index] = has_concurrent_lock;
    t_thrd.lsc_cxt.lsc->rdlock_info.count++;
}
void ReleaseGSCTableReadLock(bool *has_concurrent_lock, pthread_rwlock_t *concurrent_lock)
{
    Assert(*has_concurrent_lock);
    *has_concurrent_lock = false;
    PthreadRWlockUnlock(LOCAL_SYSDB_RESOWNER, concurrent_lock);
    Assert(t_thrd.lsc_cxt.lsc->rdlock_info.concurrent_lock[t_thrd.lsc_cxt.lsc->rdlock_info.count - 1] ==
        concurrent_lock);
    Assert(t_thrd.lsc_cxt.lsc->rdlock_info.has_concurrent_lock[t_thrd.lsc_cxt.lsc->rdlock_info.count - 1] ==
        has_concurrent_lock);
    t_thrd.lsc_cxt.lsc->rdlock_info.count--;
}

GlobalCatCTup *GlobalSysTupCache::SearchTupleFromFile(uint32 hash_value, Datum *arguments, bool is_exclusive)
{
    InsertCatTupInfo tup_info;
    tup_info.find_type = SCAN_TUPLE_SKIP;
    tup_info.arguments = arguments;
    tup_info.hash_value = hash_value;
    tup_info.hash_index = HASH_INDEX(hash_value, (uint32)cc_nbuckets);
    /* for cache read, make sure no one can clear syscache before we insert the result */
    tup_info.has_concurrent_lock = !is_exclusive;
    tup_info.is_exclusive = is_exclusive;
    Assert(is_exclusive);
    GlobalCatCTup *ct = SearchTupleMiss(&tup_info);
    if (ct != NULL) {
        pg_atomic_fetch_add_u64(m_newloads, 1);
    }
    return ct;
}

GlobalCatCTup *GlobalSysTupCache::SearchTupleMiss(InsertCatTupInfo *tup_info)
{
    GlobalCatCTup *ct = SearchMissFromProcAndAttribute(tup_info);
    if (ct != NULL) {
        return ct;
    }

    Relation relation = heap_open(m_relinfo.cc_reloid, AccessShareLock);
    ereport(DEBUG1, (errmsg("cache->cc_reloid - %d", m_relinfo.cc_reloid)));
    /*
     * Ok, need to make a lookup in the relation, copy the scankey and fill
     * out any per-call fields.
     */
    ScanKeyData cur_skey[CATCACHE_MAXKEYS];
    errno_t rc = memcpy_s(cur_skey, sizeof(ScanKeyData) * CATCACHE_MAXKEYS, cc_skey,
        sizeof(ScanKeyData) * m_relinfo.cc_nkeys);
    securec_check(rc, "", "");
    Datum *arguments = tup_info->arguments;
    cur_skey[0].sk_argument = arguments[0];
    cur_skey[1].sk_argument = arguments[1];
    cur_skey[2].sk_argument = arguments[2];
    cur_skey[3].sk_argument = arguments[3];

    if (tup_info->has_concurrent_lock) {
        AcquireGSCTableReadLock(&tup_info->has_concurrent_lock, m_concurrent_lock);
    }
    HeapTuple ntp;
    SysScanDesc scandesc = NULL;
    if (u_sess->hook_cxt.pluginSearchCatHook != NULL) {
        if (HeapTupleIsValid(ntp = ((searchCatFunc)(u_sess->hook_cxt.pluginSearchCatHook))(relation,
            m_relinfo.cc_indexoid, cc_id, m_relinfo.cc_nkeys, cur_skey, &scandesc))) {
            tup_info->ntp = ntp;
            if (!tup_info->has_concurrent_lock) {
                tup_info->canInsertGSC = false;
            } else {
                tup_info->canInsertGSC = CanTupleInsertGSC(ntp);
                if (!tup_info->canInsertGSC) {
                    /* unlock concurrent immediately, any one can invalid cache now */
                    ReleaseGSCTableReadLock(&tup_info->has_concurrent_lock, m_concurrent_lock);
                }
            }
            ct = InsertHeapTupleIntoCatCacheInSingle(tup_info);
        }
    } else {
        scandesc =
            systable_beginscan(relation, m_relinfo.cc_indexoid, IndexScanOK(cc_id), NULL,
                m_relinfo.cc_nkeys, cur_skey);
        while (HeapTupleIsValid(ntp = systable_getnext(scandesc))) {
            tup_info->ntp = ntp;
            if (!tup_info->has_concurrent_lock) {
                tup_info->canInsertGSC = false;
            } else {
                tup_info->canInsertGSC = CanTupleInsertGSC(ntp);
                if (!tup_info->canInsertGSC) {
                    /* unlock concurrent immediately, any one can invalid cache now */
                    ReleaseGSCTableReadLock(&tup_info->has_concurrent_lock, m_concurrent_lock);
                }
            }
            ct = InsertHeapTupleIntoCatCacheInSingle(tup_info);
            break; /* assume only one match */
        }
    }
    /* unlock finally */
    if (tup_info->has_concurrent_lock) {
        ReleaseGSCTableReadLock(&tup_info->has_concurrent_lock, m_concurrent_lock);
    }
    systable_endscan(scandesc);
    heap_close(relation, AccessShareLock);

#ifdef USE_SPQ
    if ((cc_id == ATTNUM && DatumGetInt16(arguments[1]) == RootSelfItemPointerAttributeNumber) ||
        (cc_id == ATTNAME && (strcmp(DatumGetCString(arguments[1]), "_root_ctid") == 0))) {

        Form_pg_attribute tmp_attribute;
        int attno = DatumGetInt16(arguments[1]);
        Assert(attno > FirstLowInvalidHeapAttributeNumber && attno < 0);
        tmp_attribute = SystemAttributeDefinition(attno, false, false, false);
        relation = heap_open(m_relinfo.cc_reloid, AccessShareLock);
        ntp = heaptuple_from_pg_attribute(relation, tmp_attribute);
        heap_close(relation, AccessShareLock);
        tup_info->ntp = ntp;
        if (!tup_info->has_concurrent_lock) {
            tup_info->canInsertGSC = false;
        } else {
            tup_info->canInsertGSC = CanTupleInsertGSC(ntp);
            if (!tup_info->canInsertGSC) {
                ReleaseGSCTableReadLock(&tup_info->has_concurrent_lock, m_concurrent_lock);
            }
        }
        ct = InsertHeapTupleIntoCatCacheInSingle(tup_info);
        if (tup_info->has_concurrent_lock) {
            ReleaseGSCTableReadLock(&tup_info->has_concurrent_lock, m_concurrent_lock);
        }
    }
#endif

    /*
     * global catcache match disk , not need negative tuple
     */
    return ct;
}
/*
 *	SearchTupleInternal
 *
 *		This call searches a system cache for a tuple, opening the relation
 *		if necessary (on the first access to a particular cache).
 *
 *		The result is NULL if not found, or a pointer to a HeapTuple in
 *		the cache.	The caller must not modify the tuple, and must call
 *		Release() when done with it.
 *
 * The search key values should be expressed as Datums of the key columns'
 * datatype(s).  (Pass zeroes for any unused parameters.)  As a special
 * exception, the passed-in key for a NAME column can be just a C string;
 * the caller need not go to the trouble of converting it to a fully
 * null-padded NAME.
 */
/*
 * Work-horse for SearchGlobalCatCache/SearchGlobalCatCacheN.
 */

GlobalCatCTup *GlobalSysTupCache::SearchTupleInternal(uint32 hash_value, Datum *arguments)
{
    FreeDeadCts();
    pg_atomic_fetch_add_u64(m_searches, 1);
    /*
     * scan the hash bucket until we find a match or exhaust our tuples
     */
    Index hash_index = HASH_INDEX(hash_value, (uint32)cc_nbuckets);
    pthread_rwlock_t *bucket_lock = &m_bucket_rw_locks[hash_index];
    InsertCatTupInfo tup_info;
    tup_info.find_type = SEARCH_TUPLE_SKIP;
    tup_info.arguments = arguments;
    tup_info.hash_value = hash_value;
    tup_info.hash_index = hash_index;
    int location = INVALID_LOCATION;
    PthreadRWlockRdlock(LOCAL_SYSDB_RESOWNER, bucket_lock);
    GlobalCatCTup *ct = FindSearchKeyTupleFromCache(&tup_info, &location);
    PthreadRWlockUnlock(LOCAL_SYSDB_RESOWNER, bucket_lock);

    if (ct != NULL) {
        pg_atomic_fetch_add_u64(m_hits, 1);
        TopnLruMoveToFront(&ct->cache_elem, GetBucket(hash_index), bucket_lock, location);
        return ct;
    }

    /* not match */
    tup_info.find_type = SCAN_TUPLE_SKIP;
    bool canInsertGSC = !g_instance.global_sysdbcache.StopInsertGSC();
    if (unlikely(GetBucket(hash_index)->dll_len >= MAX_GSC_LIST_LENGTH)) {
        RemoveTailTupleElements(hash_index);
        /* maybe no element can be swappedout */
        canInsertGSC = canInsertGSC && GetBucket(hash_index)->dll_len < MAX_GSC_LIST_LENGTH;
    }
    tup_info.is_exclusive = !canInsertGSC;
    tup_info.has_concurrent_lock = canInsertGSC;

    ct = SearchTupleMiss(&tup_info);
    if (ct != NULL) {
        pg_atomic_fetch_add_u64(m_newloads, 1);
    }
    return ct;
}

GlobalCatCList *GlobalSysTupCache::FindListInternal(uint32 hash_value, int nkeys, Datum *arguments, int *location)
{
    int index = 0;
    for (Dlelem *elt = DLGetHead(&cc_lists); elt; elt = DLGetSucc(elt)) {
        if (index > MAX_GSC_LIST_LENGTH) {
            /* cc_lists is too long, the tail elements should be swapout */
            break;
        }
        index++;
        GlobalCatCList *cl = (GlobalCatCList *)DLE_VAL(elt);
        if (likely(cl->hash_value != hash_value)) {
            continue; /* quickly skip entry if wrong hash val */
        }
        if (cl->nkeys != nkeys) {
            continue;
        }
        if (!CatalogCacheCompareTuple(m_relinfo.cc_fastequal, nkeys, cl->keys, arguments)) {
            continue;
        }
        /* Bump the list's refcount */
        pg_atomic_fetch_add_u64(&cl->refcount, 1);
        CACHE2_elog(DEBUG2, "SearchGlobalCatCacheList(%s): found list", m_relinfo.cc_relname);
        *location = index;
        return cl;
        /* Global list never be negative */
    }
    return NULL;
}
/*
 *	SearchGlobalCatCacheList
 *
 *		Generate a list of all tuples matching a partial key (that is,
 *		a key specifying just the first K of the cache's N key columns).
 *
 *		The caller must not modify the list object or the pointed-to tuples,
 *		and must call Release() when done with the list.
 */
GlobalCatCList *GlobalSysTupCache::SearchListInternal(uint32 hash_value, int nkeys, Datum *arguments)
{
    FreeDeadCls();
    Assert(nkeys > 0 && nkeys < m_relinfo.cc_nkeys);
    int location = INVALID_LOCATION;
    PthreadRWlockRdlock(LOCAL_SYSDB_RESOWNER, m_list_rw_lock);
    GlobalCatCList *cl = FindListInternal(hash_value, nkeys, arguments, &location);
    PthreadRWlockUnlock(LOCAL_SYSDB_RESOWNER, m_list_rw_lock);

    if (cl != NULL) {
        ResourceOwnerRememberGlobalCatCList(LOCAL_SYSDB_RESOWNER, cl);
        TopnLruMoveToFront(&cl->cache_elem, &cc_lists, m_list_rw_lock, location);
        return cl;
    }

    bool canInsertGSC = !g_instance.global_sysdbcache.StopInsertGSC();
    /* if cc_lists is too long, swapout it */
    if (cc_lists.dll_len >= MAX_GSC_LIST_LENGTH) {
        RemoveTailListElements();
        /* maybe no element can be swappedout */
        canInsertGSC = canInsertGSC && cc_lists.dll_len < MAX_GSC_LIST_LENGTH;
    }
    cl = SearchListFromFile(hash_value, nkeys, arguments, !canInsertGSC);
    return cl;
}

GlobalCatCList *GlobalSysTupCache::CreateCatCacheList(InsertCatListInfo *list_info)
{
    MemoryContext old;
    if (list_info->canInsertGSC) {
        old = MemoryContextSwitchTo(m_dbEntry->GetRandomMemCxt());
    } else {
        old = MemoryContextSwitchTo(GetLSCMemcxt(m_cache_id));
    }
    GlobalCatCList *new_cl = (GlobalCatCList *)palloc(offsetof(GlobalCatCList, members) +
                                                      (list_length(list_info->ctlist) + 1) * sizeof(GlobalCatCTup *));
    ResourceOwnerRememberGlobalCatCList(LOCAL_SYSDB_RESOWNER, new_cl);
    new_cl->refcount = 1;
    new_cl->canInsertGSC = false;
    new_cl->nkeys = list_info->nkeys;
    /* releases and free by my_cache when palloc fail */
    new_cl->my_cache = this;
    new_cl->cl_magic = CL_MAGIC;
    new_cl->hash_value = list_info->hash_value;
    DLInitElem(&new_cl->cache_elem, new_cl);
    new_cl->ordered = list_info->ordered;
    new_cl->n_members = list_length(list_info->ctlist);
    int index = 0;
    ListCell *lc;
    foreach (lc, list_info->ctlist) {
        new_cl->members[index] = (GlobalCatCTup *)lfirst(lc);
        index++;
    }
    /*
     * Avoid double-free elemnts of list_info->ctlist by Resowner and Try-Catch(),
     * to be safely we free info_list->ctlist here to avoid double-free happen, normally
     * Resowner will do this
     */
    list_free_ext(list_info->ctlist);

    Assert(index == new_cl->n_members);
    errno_t rc = memset_s(new_cl->keys, list_info->nkeys * sizeof(Datum), 0, list_info->nkeys * sizeof(Datum));
    securec_check(rc, "", "");

    /* palloc maybe fail */
    CatCacheCopyKeys(m_relinfo.cc_tupdesc, list_info->nkeys, m_relinfo.cc_keyno, list_info->arguments, new_cl->keys);
    MemoryContextSwitchTo(old);
    new_cl->canInsertGSC = list_info->canInsertGSC;
    return new_cl;
}

GlobalCatCList *GlobalSysTupCache::InsertListIntoCatCacheList(InsertCatListInfo *list_info, GlobalCatCList *cl)
{
    if (!cl->canInsertGSC) {
        return cl;
    }

    int location = INVALID_LOCATION;
    PthreadRWlockWrlock(LOCAL_SYSDB_RESOWNER, m_list_rw_lock);
    GlobalCatCList *exist_cl = FindListInternal(list_info->hash_value, list_info->nkeys, list_info->arguments,
        &location);
    if (exist_cl != NULL) {
        PthreadRWlockUnlock(LOCAL_SYSDB_RESOWNER, m_list_rw_lock);
        ResourceOwnerForgetGlobalCatCList(LOCAL_SYSDB_RESOWNER, cl);
        ResourceOwnerRememberGlobalCatCList(LOCAL_SYSDB_RESOWNER, exist_cl);
        /* we need mark clist's refcont to 0 then do real free up. */
        cl->refcount = 0;
        FreeGlobalCatCList(cl);
        cl = exist_cl;
    } else {
        AddHeadToCCList(cl);
        PthreadRWlockUnlock(LOCAL_SYSDB_RESOWNER, m_list_rw_lock);
    }
    return cl;
}

GlobalCatCList *GlobalSysTupCache::SearchListMiss(InsertCatListInfo *list_info)
{
    Relation relation;
    ScanKeyData cur_skey[CATCACHE_MAXKEYS];
    SysScanDesc scandesc;
    errno_t rc;

    /*
        * Ok, need to make a lookup in the relation, copy the scankey and
        * fill out any per-call fields.
        */
    rc = memcpy_s(cur_skey, sizeof(ScanKeyData) * CATCACHE_MAXKEYS, cc_skey,
        sizeof(ScanKeyData) * m_relinfo.cc_nkeys);
    securec_check(rc, "", "");
    cur_skey[0].sk_argument = list_info->arguments[0];
    cur_skey[1].sk_argument = list_info->arguments[1];
    cur_skey[2].sk_argument = list_info->arguments[2];
    cur_skey[3].sk_argument = list_info->arguments[3];

    relation = heap_open(m_relinfo.cc_reloid, AccessShareLock);

    scandesc = systable_beginscan(relation, m_relinfo.cc_indexoid, IndexScanOK(cc_id),
        NULL, list_info->nkeys, cur_skey);
    if (list_info->has_concurrent_lock) {
        AcquireGSCTableReadLock(&list_info->has_concurrent_lock, m_concurrent_lock);
    }

    /* The list will be ordered iff we are doing an index scan */
    list_info->ordered = (scandesc->irel != NULL);

    HeapTuple ntp;
    InsertCatTupInfo tup_info;
    tup_info.find_type = SCAN_LIST_SKIP;
    /* for list check, we cannot use arguments, instead we use t_self */
    tup_info.arguments = NULL;
    while (HeapTupleIsValid(ntp = systable_getnext(scandesc))) {
        if (IsProcCache(m_relinfo.cc_reloid) && IsSystemObjOid(HeapTupleGetOid(ntp)) &&
            likely(u_sess->attr.attr_common.IsInplaceUpgrade == false)) {
            continue;
        }
        if (IsAttributeCache(m_relinfo.cc_reloid)) {
            bool attIsNull = false;
            Oid attrelid = DatumGetObjectId(SysCacheGetAttr(cc_id, ntp, Anum_pg_attribute_attrelid, &attIsNull));
            if (IsSystemObjOid(attrelid) && IsValidCatalogParam(GetCatalogParam(attrelid))) {
                continue;
            }
        }
        /*
            * See if there's an entry for this tuple already.
            */
        InitInsertCatTupInfo(&tup_info, ntp, list_info->arguments);
        tup_info.canInsertGSC = list_info->has_concurrent_lock && CanTupleInsertGSC(ntp);
        GlobalCatCTup *ct = InsertHeapTupleIntoCatCacheInList(&tup_info);
        list_info->canInsertGSC = list_info->canInsertGSC && ct->canInsertGSC;
        /*
            * Careful here: enlarge resource owner catref array, add entry to ctlist, then bump its refcount.
            * This way leaves state correct if enlarge or lappend runs out of memory_context_list
            * We use resource owner to track referenced cachetups for safety reasons. Because ctlist is now
            * built from different sources, i.e. built-in catalogs and physical relation tuples. If failure in later
            * sources is not caught, resource owner will clean up ref count for cachetups got from previous sources.
            */
        list_info->ctlist = lappend(list_info->ctlist, ct);
    }
    systable_endscan(scandesc);
    heap_close(relation, AccessShareLock);
    if (list_info->has_concurrent_lock && (!list_info->canInsertGSC || list_length(list_info->ctlist) == 0)) {
        ReleaseGSCTableReadLock(&list_info->has_concurrent_lock, m_concurrent_lock);
        list_info->canInsertGSC = false;
    }
    GlobalCatCList *cl = CreateCatCacheList(list_info);
    return cl;
}

GlobalCatCList *GlobalSysTupCache::SearchListFromFile(uint32 hash_value, int nkeys, Datum *arguments,
    bool is_exclusive)
{
    InsertCatListInfo list_info;
    list_info.ordered = false;
    list_info.ctlist = NIL;
    list_info.nkeys = nkeys;
    list_info.arguments = arguments;
    list_info.hash_value = hash_value;
    list_info.is_exclusive = is_exclusive;
    list_info.has_concurrent_lock = !is_exclusive;
    list_info.canInsertGSC = !is_exclusive;
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

    /* Firstly, check the builtin functions. if there are functions
     * which has the same name with the one we want to find, lappend it
     * into the ctlist
     */
    if (IsProcCache(m_relinfo.cc_reloid) && CacheIsProcNameArgNsp(cc_id) &&
        likely(u_sess->attr.attr_common.IsInplaceUpgrade == false)) {
        SearchBuiltinProcCacheList(&list_info);
    }

    if (IsAttributeCache(m_relinfo.cc_reloid) && IsSystemObjOid(DatumGetObjectId(arguments[0]))) {
        SearchPgAttributeCacheList(&list_info);
    }
    GlobalCatCList *cl = NULL;
    PG_TRY();
    {
        cl = SearchListMiss(&list_info);
    }
    PG_CATCH();
    {
        ReleaseAllGSCRdConcurrentLock();
        ReleaseTempList(list_info.ctlist);
        PG_RE_THROW();
    }
    PG_END_TRY();

    cl = InsertListIntoCatCacheList(&list_info, cl);
    if (list_info.has_concurrent_lock) {
        ReleaseGSCTableReadLock(&list_info.has_concurrent_lock, m_concurrent_lock);
    }
    return cl;
}

/*cat list***********************************************************************************************************/
void GlobalSysTupCache::ReleaseTempList(const List *ctlist)
{
    ListCell *ctlist_item = NULL;
    GlobalCatCTup *ct = NULL;
    foreach (ctlist_item, ctlist) {
        ct = (GlobalCatCTup *)lfirst(ctlist_item);
        ct->Release();
    }
}

void GlobalSysTupCache::InitInsertCatTupInfo(InsertCatTupInfo *tup_info, HeapTuple ntp, Datum *arguments)
{
    uint32 hash_value = CatalogCacheComputeTupleHashValue(cc_id, m_relinfo.cc_keyno, m_relinfo.cc_tupdesc,
        m_relinfo.cc_hashfunc, m_relinfo.cc_reloid, m_relinfo.cc_nkeys, ntp);
    Index hash_index = HASH_INDEX(hash_value, (uint32)(cc_nbuckets));
    tup_info->arguments = arguments;
    tup_info->hash_value = hash_value;
    tup_info->hash_index = hash_index;
    tup_info->ntp = ntp;
}

void GlobalSysTupCache::SearchPgAttributeCacheList(InsertCatListInfo *list_info)
{
    const FormData_pg_attribute *catlogAttrs = NULL;
    Assert(list_info->nkeys == 1);
    Oid relOid = ObjectIdGetDatum(list_info->arguments[0]);
    CatalogRelationBuildParam catalogDesc = GetCatalogParam(relOid);
    catlogAttrs = catalogDesc.attrs;
    if (catalogDesc.oid == InvalidOid) {
        return;
    }
    PG_TRY();
    {
        bool hasBucketAttr = false;
        for (int16 attnum = 0; attnum < catalogDesc.natts + GetSysAttLength(hasBucketAttr); attnum++) {
            Form_pg_attribute attr;
            FormData_pg_attribute tempAttr;
            if (attnum < catalogDesc.natts) {
                tempAttr = catlogAttrs[attnum];
                attr = &tempAttr;
            } else {
                int16 index = attnum - catalogDesc.natts;
                if (!catalogDesc.hasoids && index == 1) {
                    continue;
                }
                /* The system table does not have the bucket column, so incoming false */
                attr = SystemAttributeDefinition(-(index + 1), catalogDesc.hasoids, false, false);
                attr->attrelid = relOid;
            }
            HeapTuple ntp = GetPgAttributeAttrTuple(m_relinfo.cc_tupdesc, attr);
            InsertCatTupInfo tup_info;
            tup_info.find_type = PGATTR_LIST_SKIP;
            InitInsertCatTupInfo(&tup_info, ntp, list_info->arguments);
            /* hard code, never change */
            tup_info.is_exclusive = list_info->is_exclusive;
            tup_info.canInsertGSC = true;
            tup_info.attnum = attnum;
            tup_info.catalogDesc = &catalogDesc;
            GlobalCatCTup *ct = InsertHeapTupleIntoCatCacheInList(&tup_info);
            heap_freetuple(ntp);
            /*
             * Careful here: enlarge resource owner catref array, add entry to ctlist, then bump its refcount.
             * This way leaves state correct if enlarge or lappend runs out of memory_context_list
             * We use resource owner to track referenced cachetups for safety reasons. Because ctlist is now
             * built from different sources, i.e. built-in catalogs and physical relation tuples. If failure in later
             * sources is not caught, resource owner will clean up ref count for cachetups got from previous sources.
             */
            list_info->ctlist = lappend(list_info->ctlist, ct);
        }
    }
    PG_CATCH();
    {
        ReleaseAllGSCRdConcurrentLock();
        ReleaseTempList(list_info->ctlist);
        PG_RE_THROW();
    }
    PG_END_TRY();
}

void GlobalSysTupCache::SearchBuiltinProcCacheList(InsertCatListInfo *list_info)
{
    int i;
    GlobalCatCTup *ct = NULL;
    const FuncGroup *gfuncs = NULL;
    Assert(list_info->nkeys == 1);
    char *funcname = NameStr(*(DatumGetName(list_info->arguments[0])));
    gfuncs = SearchBuiltinFuncByName(funcname);
    if (gfuncs == NULL) {
        CACHE3_elog(DEBUG2, "%s: the function \"%s\" does not in built-in list", __FUNCTION__, funcname);
        return;
    }
    PG_TRY();
    {
        for (i = 0; i < gfuncs->fnums; i++) {
            const Builtin_func *bfunc = &gfuncs->funcs[i];
            HeapTuple ntp = CreateHeapTuple4BuiltinFunc(bfunc, NULL);
            InsertCatTupInfo tup_info;
            tup_info.find_type = PROC_LIST_SKIP;
            InitInsertCatTupInfo(&tup_info, ntp, list_info->arguments);
            /* hard code, never change */
            tup_info.is_exclusive = list_info->is_exclusive;
            tup_info.canInsertGSC = true;
            ct = InsertHeapTupleIntoCatCacheInList(&tup_info);
            heap_freetuple(ntp);
            list_info->ctlist = lappend(list_info->ctlist, ct);
        }
    }
    PG_CATCH();
    {
        ReleaseAllGSCRdConcurrentLock();
        ReleaseTempList(list_info->ctlist);
        PG_RE_THROW();
    }
    PG_END_TRY();
}

/*create init cat****************************************************************************************************/
/*
 * CatalogCacheCreateEntry
 *		Create a new GlobalCatCTup entry, copying the given HeapTuple and other
 *		supplied data into it.	The new entry initially has refcount 0.
 *      this func makes sure the tuple has a shared memory context
 *      this func never fail
 */
void GlobalSysTupCache::CopyTupleIntoGlobalCatCTup(GlobalCatCTup *ct, HeapTuple dtp)
{
    Assert(!HeapTupleHasExternal(dtp));
    ct->tuple = *dtp;
    Assert(dtp->tupTableType == HEAP_TUPLE);
    ct->tuple.tupTableType = HEAP_TUPLE;
    ct->tuple.t_data = (HeapTupleHeader)MAXALIGN(((char *)ct) + sizeof(GlobalCatCTup));
    /* copy tuple contents */
    errno_t rc = memcpy_s((char *)ct->tuple.t_data, dtp->t_len, (const char *)dtp->t_data, dtp->t_len);
    securec_check(rc, "", "");

    /* extract keys - they'll point into the tuple if not by-value */
    for (int i = 0; i < m_relinfo.cc_nkeys; i++) {
        Datum atp;
        bool isnull = false;
        atp = heap_getattr(&ct->tuple, m_relinfo.cc_keyno[i], m_relinfo.cc_tupdesc, &isnull);
#ifndef ENABLE_MULTIPLE_NODES
            Assert(!isnull || ((m_relinfo.cc_reloid == ProcedureRelationId) && atp == 0));
#else
            Assert(!isnull);
#endif
        ct->keys[i] = atp;
    }
}

void GlobalSysTupCache::InitHashTable()
{
    MemoryContext old = MemoryContextSwitchTo(m_dbEntry->GetRandomMemCxt());
    /* this func allow palloc fail */
    size_t sz = (cc_nbuckets + 1) * sizeof(Dllist) + PG_CACHE_LINE_SIZE;
    if (cc_buckets == NULL) {
        void *origin_ptr = palloc0(sz);
        cc_buckets = (Dllist *)CACHELINEALIGN(origin_ptr);
        m_dbEntry->MemoryEstimateAdd(GetMemoryChunkSpace(origin_ptr));
    }
    if (m_bucket_rw_locks == NULL) {
        m_bucket_rw_locks = (pthread_rwlock_t *)palloc0((cc_nbuckets + 1) * sizeof(pthread_rwlock_t));
        m_dbEntry->MemoryEstimateAdd(GetMemoryChunkSpace(m_bucket_rw_locks));
        for (int i = 0; i <= cc_nbuckets; i++) {
            PthreadRwLockInit(&m_bucket_rw_locks[i], NULL);
        }
    }
    if (m_is_tup_swappingouts == NULL) {
        m_is_tup_swappingouts = (volatile uint32 *)palloc0(sizeof(volatile uint32) * (cc_nbuckets + 1));
        m_dbEntry->MemoryEstimateAdd(GetMemoryChunkSpace((void *)m_is_tup_swappingouts));
    }
    m_is_list_swappingout = 0;
    
    (void)MemoryContextSwitchTo(old);
}

/* call on global memory context */
void GlobalSysTupCache::InitCacheInfo(Oid reloid, Oid indexoid, int nkeys, const int *key, int nbuckets)
{
    int i;

    /*
     * nbuckets is the number of hash buckets to use in this GlobalCatCache.
     * Currently we just use a hard-wired estimate of an appropriate size for
     * each cache; maybe later make them dynamically resizable?
     *
     * nbuckets must be a power of two.  We check this via Assert rather than
     * a full runtime check because the values will be coming from constant
     * tables.
     *
     * If you're confused by the power-of-two check, see comments in
     * bitmapset.c for an explanation.
     */
    Assert(nbuckets > 0 && (nbuckets & -nbuckets) == nbuckets);
    /*
     * first switch to the cache context so our allocations do not vanish at
     * the end of a transaction
     */
    cc_nbuckets = ResizeHashBucket(nbuckets, g_instance.global_sysdbcache.dynamic_hash_bucket_strategy);
    /*
     * Allocate a new cache structure, aligning to a cacheline boundary
     *
     * Note: we assume zeroing initializes the Dllist headers correctly
     */
    InitHashTable();
    MemoryContext old = MemoryContextSwitchTo(m_dbEntry->GetRandomMemCxt());
    DLInitList(&cc_lists);
    /*
     * initialize the cache's relation information for the relation
     * corresponding to this cache, and initialize some of the new cache's
     * other internal fields.  But don't open the relation yet.
     */
    cc_id = m_cache_id;
    m_relinfo.cc_relname = NULL;
    m_relinfo.cc_reloid = reloid;
    m_relinfo.cc_indexoid = indexoid;
    m_relinfo.cc_tupdesc = (TupleDesc)NULL;
    m_relinfo.cc_nkeys = nkeys;
    for (i = 0; i < nkeys; ++i) {
        m_relinfo.cc_keyno[i] = key[i];
    }

    if (m_list_rw_lock == NULL) {
        m_list_rw_lock = (pthread_rwlock_t *)palloc0(sizeof(pthread_rwlock_t));
        m_dbEntry->MemoryEstimateAdd(GetMemoryChunkSpace(m_list_rw_lock));
        PthreadRwLockInit(m_list_rw_lock, NULL);
    }
    if (m_concurrent_lock == NULL) {
        m_concurrent_lock = (pthread_rwlock_t *)palloc0(sizeof(pthread_rwlock_t));
        m_dbEntry->MemoryEstimateAdd(GetMemoryChunkSpace(m_concurrent_lock));
        PthreadRwLockInit(m_concurrent_lock, NULL);
    }

    (void)MemoryContextSwitchTo(old);
}

uint64 GetTupdescSize(TupleDesc tupdesc)
{
    uint64 main_size = GetMemoryChunkSpace(tupdesc);
    uint64 constr_size = 0;
    uint64 clusterkey_size = 0;
    if (tupdesc->constr != NULL) {
        TupleConstr *constr = tupdesc->constr;
        constr_size += GetMemoryChunkSpace(constr);
        if (constr->num_defval > 0) {
            constr_size += GetMemoryChunkSpace(constr->defval);
            for (int i = 0; i < constr->num_defval; i++) {
                constr_size += GetMemoryChunkSpace(constr->defval[i].adbin);
            }
            constr_size += GetMemoryChunkSpace(constr->generatedCols);
        }
        
        if (constr->num_check) {
            constr_size += GetMemoryChunkSpace(constr->check);
            for (int i = 0; i < constr->num_check; i++) {
                if (constr->check[i].ccname) {
                    constr_size += GetMemoryChunkSpace(constr->check[i].ccname);
                }
                if (constr->check[i].ccbin) {
                    constr_size += GetMemoryChunkSpace(constr->check[i].ccbin);
                }
            }
        }
        if (constr->clusterKeyNum != 0) {
            clusterkey_size += GetMemoryChunkSpace(constr->clusterKeys);
        }
    }

    uint64 defval_size = 0;
    if (tupdesc->initdefvals != NULL) {
        defval_size += GetMemoryChunkSpace(tupdesc->initdefvals);
        TupInitDefVal *pInitDefVal = tupdesc->initdefvals;
        for (int i = 0; i < tupdesc->natts; i++) {
            if (!pInitDefVal[i].isNull) {
                defval_size += GetMemoryChunkSpace(pInitDefVal[i].datum);
            }
        }
    }

    uint64 tupdesc_size = main_size + clusterkey_size + constr_size + defval_size;
    return tupdesc_size;
}

void GlobalSysTupCache::InitRelationInfo()
{
    Relation relation;
    int i;

    /*
     * During inplace or online upgrade, the to-be-fabricated catalogs are still missing,
     * for which we can not throw an ERROR.
     */
    LockRelationOid(m_relinfo.cc_reloid, AccessShareLock);

    relation = RelationIdGetRelation(m_relinfo.cc_reloid);
    if (!RelationIsValid(relation)) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("could not open relation with OID %u", m_relinfo.cc_reloid)));
    }

    pgstat_initstats(relation);

    /*
     * switch to the cache context so our allocations do not vanish at the end
     * of a transaction
     */

    MemoryContext old = MemoryContextSwitchTo(m_dbEntry->GetRandomMemCxt());

    /*
     * copy the relcache's tuple descriptor to permanent cache storage
     */
    if (m_relinfo.cc_tupdesc == NULL) {
        m_relinfo.cc_tupdesc = CopyTupleDesc(RelationGetDescr(relation));
        m_dbEntry->MemoryEstimateAdd(GetTupdescSize(m_relinfo.cc_tupdesc));
    }

    /*
     * save the relation's name and relisshared flag, too (cc_relname is used
     * only for debugging purposes)
     */
    if (m_relinfo.cc_relname == NULL) {
        m_relinfo.cc_relname = pstrdup(RelationGetRelationName(relation));
        m_dbEntry->MemoryEstimateAdd(GetMemoryChunkSpace((void *)m_relinfo.cc_relname));
    }
    Assert(m_relinfo.cc_relisshared == RelationGetForm(relation)->relisshared);

    /*
     * return to the caller's memory context and close the rel
     */
    MemoryContextSwitchTo(old);

    enable_rls = RelationEnableRowSecurity(relation);

    heap_close(relation, AccessShareLock);

    /*
     * initialize cache's key information
     */
    for (i = 0; i < m_relinfo.cc_nkeys; ++i) {
        Oid keytype;
        RegProcedure eqfunc;
        if (m_relinfo.cc_keyno[i] > 0)
            keytype = m_relinfo.cc_tupdesc->attrs[m_relinfo.cc_keyno[i] - 1].atttypid;
        else {
            if (m_relinfo.cc_keyno[i] != ObjectIdAttributeNumber)
                ereport(FATAL, (errmsg("only sys attr supported in caches is OID")));
            keytype = OIDOID;
        }
        if (u_sess->hook_cxt.pluginCCHashEqFuncs == NULL ||
            !((pluginCCHashEqFuncs)(u_sess->hook_cxt.pluginCCHashEqFuncs))(keytype, &m_relinfo.cc_hashfunc[i], &eqfunc,
                &m_relinfo.cc_fastequal[i], cc_id)) {
            GetCCHashEqFuncs(keytype, &m_relinfo.cc_hashfunc[i], &eqfunc, &m_relinfo.cc_fastequal[i]);
        }
        /*
         * Do equality-function lookup (we assume this won't need a catalog
         * lookup for any supported type)
         */
        pfree_ext(cc_skey[i].sk_func.fnLibPath);
        fmgr_info_cxt(eqfunc, &cc_skey[i].sk_func, m_dbEntry->GetRandomMemCxt());
        /* Initialize sk_attno suitably for HeapKeyTest() and heap scans */
        cc_skey[i].sk_attno = m_relinfo.cc_keyno[i];
        /* Fill in sk_strategy as well --- always standard equality */
        cc_skey[i].sk_strategy = BTEqualStrategyNumber;
        cc_skey[i].sk_subtype = InvalidOid;
        if (keytype == TEXTOID) {
            cc_skey[i].sk_collation = DEFAULT_COLLATION_OID;
        } else {
            cc_skey[i].sk_collation = InvalidOid;
        }
    }
}

/*class func*******************************************************************************************************/
void GlobalSysTupCache::Init()
{
    if (m_isInited) {
        return;
    }
    /* this init called by systab, when a write lock is aquired
     * init func followed by new, so m_isInited must be false
     * lock first when call this func */
    const cachedesc *cache_info = &cacheinfo[m_cache_id];
    InitCacheInfo(cache_info->reloid, cache_info->indoid, cache_info->nkeys, cache_info->key, cache_info->nbuckets);
    InitRelationInfo();
    pg_memory_barrier();
    m_isInited = true;
}

/*
 *	CatalogCacheIdInvalidate
 *
 *	Invalidate entries in the specified cache, given a hash value.
 *
 *	We delete cache entries that match the hash value.
 *  We don't care whether the invalidation is the result
 *	of a tuple insertion or a deletion.
 *
 *	We used to try to match positive cache entries by TID, but that is
 *	unsafe after a VACUUM FULL on a system catalog: an inval event could
 *	be queued before VACUUM FULL, and then processed afterwards, when the
 *	target tuple that has to be invalidated has a different TID than it
 *	did when the event was created.  So now we just compare hash values and
 *	accept the small risk of unnecessary invalidations due to false matches.
 *
 *	This routine is only quasi-public: it should only be used by inval.c.
 */
/*
 *	HeapTupleInvalidate()
 *
 *	pay attention:
 *  match disk one by one, so call this func when update , insert or delete every time
 */
void GlobalSysTupCache::HashValueInvalidate(uint32 hash_value)
{
    if (!m_isInited) {
        return;
    }
    /*
     * inspect caches to find the proper cache
     * list of cache_header is never changed after init, so lock is not needed
     * Invalidate *all *CatCLists in this cache; it's too hard to tell
     * which searches might still be correct, so just zap 'em all.
     */
    /* avoid other session read from table and havn't insert into gsc */
    PthreadRWlockWrlock(LOCAL_SYSDB_RESOWNER, m_concurrent_lock);

    if (cc_lists.dll_len > 0) {
        PthreadRWlockWrlock(LOCAL_SYSDB_RESOWNER, m_list_rw_lock);
        for (Dlelem *elt = DLGetHead(&cc_lists); elt;) {
            GlobalCatCList *cl = (GlobalCatCList *)DLE_VAL(elt);
            elt = DLGetSucc(elt);
            HandleDeadGlobalCatCList(cl);
        }
        PthreadRWlockUnlock(LOCAL_SYSDB_RESOWNER, m_list_rw_lock);
    } else {
        /* no one can insert because we have m_concurrent_lock exclusive lock
         * no one can scan table now */
    }

    uint32 hash_index = HASH_INDEX(hash_value, cc_nbuckets);
    if (GetBucket(hash_index)->dll_len > 0) {
        pthread_rwlock_t *bucket_lock = &m_bucket_rw_locks[hash_index];
        PthreadRWlockWrlock(LOCAL_SYSDB_RESOWNER, bucket_lock);
        for (Dlelem *elt = DLGetHead(GetBucket(hash_index)); elt;) {
            GlobalCatCTup *ct = (GlobalCatCTup *)DLE_VAL(elt);
            elt = DLGetSucc(elt);
            if (hash_value != ct->hash_value) {
                continue;
            }
            HandleDeadGlobalCatCTup(ct);
            /* maybe multi dead tuples, find them all */
        }
        PthreadRWlockUnlock(LOCAL_SYSDB_RESOWNER, bucket_lock);
    } else {
        /* no one can insert because we have m_concurrent_lock exclusive lock
         * no one can scan table now */
    }

    PthreadRWlockUnlock(LOCAL_SYSDB_RESOWNER, m_concurrent_lock);
}

/*
 *		ResetGlobalCatCache
 *
 * Reset one catalog cache to empty.
 *
 * This is not very efficient if the target cache is nearly empty.
 * However, it shouldn't need to be efficient; we don't invoke it often.
 */
template <bool force>
void GlobalSysTupCache::ResetCatalogCache()
{
    if (!m_isInited) {
        return;
    }

    /* if not force, we are swappingout, m_concurrent_lock is not needed */
    if (force) {
        PthreadRWlockWrlock(LOCAL_SYSDB_RESOWNER, m_concurrent_lock);
    }

    if (cc_lists.dll_len > 0) {
        PthreadRWlockWrlock(LOCAL_SYSDB_RESOWNER, m_list_rw_lock);
        for (Dlelem *elt = DLGetHead(&cc_lists); elt;) {
            GlobalCatCList *cl = (GlobalCatCList *)DLE_VAL(elt);
            elt = DLGetSucc(elt);
            if (force || cl->refcount == 0) {
                HandleDeadGlobalCatCList(cl);
            }
        }
        PthreadRWlockUnlock(LOCAL_SYSDB_RESOWNER, m_list_rw_lock);
    }

    /* Remove each tuple in this cache */
    for (int i = 0; i < cc_nbuckets; i++) {
        if (GetBucket(i)->dll_len == 0) {
            continue;
        }
        pthread_rwlock_t *bucket_lock = &m_bucket_rw_locks[i];
        PthreadRWlockWrlock(LOCAL_SYSDB_RESOWNER, bucket_lock);
        for (Dlelem *elt = DLGetHead(&cc_buckets[i]); elt;) {
            GlobalCatCTup *ct = (GlobalCatCTup *)DLE_VAL(elt);
            elt = DLGetSucc(elt);
            if (force || ct->refcount == 0) {
                HandleDeadGlobalCatCTup(ct);
            }
        }
        PthreadRWlockUnlock(LOCAL_SYSDB_RESOWNER, bucket_lock);
    }

    if (force) {
        PthreadRWlockUnlock(LOCAL_SYSDB_RESOWNER, m_concurrent_lock);
    }
}
template void GlobalSysTupCache::ResetCatalogCache<false>();
template void GlobalSysTupCache::ResetCatalogCache<true>();

List *GlobalSysTupCache::GetGlobalCatCTupStat()
{
    List *tuple_stat_list = NIL;
    if (!m_isInited) {
        return tuple_stat_list;
    }

    /* Remove each tuple in this cache */
    for (int i = 0; i < cc_nbuckets; i++) {
        if (GetBucket(i)->dll_len == 0) {
            continue;
        }
        pthread_rwlock_t *bucket_lock = &m_bucket_rw_locks[i];
        PthreadRWlockRdlock(LOCAL_SYSDB_RESOWNER, bucket_lock);
        for (Dlelem *elt = DLGetHead(&cc_buckets[i]); elt;) {
            GlobalCatCTup *ct = (GlobalCatCTup *)DLE_VAL(elt);
            elt = DLGetSucc(elt);
            GlobalCatalogTupleStat *tup_stat = (GlobalCatalogTupleStat*)palloc(sizeof(GlobalCatalogTupleStat));
            tup_stat->db_oid = m_dbOid;
            tup_stat->db_name_datum = CStringGetTextDatum(m_dbEntry->m_dbName);
            tup_stat->rel_oid = m_relinfo.cc_reloid;
            tup_stat->rel_name_datum = CStringGetTextDatum(m_relinfo.cc_relname);
            tup_stat->cache_id = m_cache_id;
            tup_stat->self = ct->tuple.t_self;
            tup_stat->ctid = ct->tuple.t_data->t_ctid;
            tup_stat->infomask = ct->tuple.t_data->t_infomask;
            tup_stat->infomask2 = ct->tuple.t_data->t_infomask2;
            tup_stat->hash_value = ct->hash_value;
            tup_stat->refcount = ct->refcount;
            tuple_stat_list = lappend(tuple_stat_list, tup_stat);
        }
        PthreadRWlockUnlock(LOCAL_SYSDB_RESOWNER, bucket_lock);
    }
    return tuple_stat_list;
}

#ifndef ENABLE_MULTIPLE_NODES
GlobalCatCTup *GlobalSysTupCache::SearchTupleWithArgModes(uint32 hash_value, Datum *arguments, oidvector* argModes)
{
    FreeDeadCts();
    pg_atomic_fetch_add_u64(m_searches, 1);
    /*
     * scan the hash bucket until we find a match or exhaust our tuples
     */
    Index hash_index = HASH_INDEX(hash_value, (uint32)cc_nbuckets);
    pthread_rwlock_t *bucket_lock = &m_bucket_rw_locks[hash_index];
    InsertCatTupInfo tup_info;
    tup_info.find_type = SEARCH_TUPLE_SKIP;
    tup_info.arguments = arguments;
    tup_info.hash_value = hash_value;
    tup_info.hash_index = hash_index;
    int location = INVALID_LOCATION;

    int index = 0;
    GlobalCatCTup *ct = NULL;
    PthreadRWlockRdlock(LOCAL_SYSDB_RESOWNER, bucket_lock);
    for (Dlelem *elt = DLGetHead(GetBucket(hash_index)); elt != NULL; elt = DLGetSucc(elt)) {
        index++;
        ct = (GlobalCatCTup *)DLE_VAL(elt);
        if (ct->hash_value != hash_value) {
            ct = NULL;
            continue;
        }
        if (!CatalogCacheCompareTuple(m_relinfo.cc_fastequal, m_relinfo.cc_nkeys, ct->keys, arguments)) {
            ct = NULL;
            continue;
        }
        if (unlikely(u_sess->attr.attr_common.IsInplaceUpgrade) && ct->tuple.t_self.ip_posid == 0) {
            Assert(ct->tuple.t_tableOid == InvalidOid);
            ct = NULL;
            continue;
        }
        /*
         * Make sure that the in-out-info(argmodes) should be the same.
         */
        if (!IsProArgModesEqualByTuple(&ct->tuple, m_relinfo.cc_tupdesc, argModes)) {
            ct = NULL;
            continue;
        }
        pg_atomic_fetch_add_u64(&ct->refcount, 1);
        location = index;
        break;
    }
    PthreadRWlockUnlock(LOCAL_SYSDB_RESOWNER, bucket_lock);

    if (ct != NULL) {
        pg_atomic_fetch_add_u64(m_hits, 1);
        TopnLruMoveToFront(&ct->cache_elem, GetBucket(hash_index), bucket_lock, location);
        return ct;
    }

    /* not match */
    tup_info.find_type = SCAN_TUPLE_SKIP;
    bool canInsertGSC = !g_instance.global_sysdbcache.StopInsertGSC();
    /* if cc_lists is too long, swapout it */
    if (cc_lists.dll_len >= MAX_GSC_LIST_LENGTH) {
        RemoveTailListElements();
        /* maybe no element can be swappedout */
        canInsertGSC = canInsertGSC && cc_lists.dll_len < MAX_GSC_LIST_LENGTH;
    }

    tup_info.is_exclusive = !canInsertGSC;
    tup_info.has_concurrent_lock = canInsertGSC;
    ct = SearchTupleMissWithArgModes(&tup_info, argModes);
    if (ct != NULL) {
        pg_atomic_fetch_add_u64(m_newloads, 1);
    }
    return ct;
}

GlobalCatCTup *GlobalSysTupCache::SearchTupleFromFileWithArgModes(
    uint32 hash_value, Datum *arguments, oidvector* argModes, bool is_exclusive)
{
    InsertCatTupInfo tup_info;
    tup_info.find_type = SCAN_TUPLE_SKIP;
    tup_info.arguments = arguments;
    tup_info.hash_value = hash_value;
    tup_info.hash_index = HASH_INDEX(hash_value, (uint32)cc_nbuckets);
    /* not match */
    tup_info.has_concurrent_lock = !is_exclusive;
    tup_info.is_exclusive = is_exclusive;
    Assert(is_exclusive);
    GlobalCatCTup *ct = SearchTupleMissWithArgModes(&tup_info, argModes);
    if (ct != NULL) {
        pg_atomic_fetch_add_u64(m_newloads, 1);
    }
    return ct;
}

GlobalCatCTup *GlobalSysTupCache::SearchTupleMissWithArgModes(InsertCatTupInfo *tup_info, oidvector* argModes)
{
    GlobalCatCTup *ct = SearchMissFromProcAndAttribute(tup_info);
    if (ct != NULL) {
        return ct;
    }
    Relation relation = heap_open(m_relinfo.cc_reloid, AccessShareLock);
    ereport(DEBUG1, (errmsg("cache->cc_reloid - %d", m_relinfo.cc_reloid)));
    /*
     * Ok, need to make a lookup in the relation, copy the scankey and fill
     * out any per-call fields.
     */
    ScanKeyData cur_skey[CATCACHE_MAXKEYS];
    errno_t rc = memcpy_s(cur_skey, sizeof(ScanKeyData) * CATCACHE_MAXKEYS, cc_skey,
        sizeof(ScanKeyData) * m_relinfo.cc_nkeys);
    securec_check(rc, "", "");
    Datum *arguments = tup_info->arguments;
    cur_skey[0].sk_argument = arguments[0];
    cur_skey[1].sk_argument = arguments[1];
    cur_skey[2].sk_argument = arguments[2];
    cur_skey[3].sk_argument = arguments[3];
    SysScanDesc scandesc =
        systable_beginscan(relation, m_relinfo.cc_indexoid, IndexScanOK(cc_id), NULL,
            m_relinfo.cc_nkeys, cur_skey);
    /* for cache read, make sure no one can clear syscache before we insert the result */
    tup_info->has_concurrent_lock = !tup_info->is_exclusive;

    if (tup_info->has_concurrent_lock) {
        AcquireGSCTableReadLock(&tup_info->has_concurrent_lock, m_concurrent_lock);
    }
    HeapTuple ntp;
    while (HeapTupleIsValid(ntp = systable_getnext(scandesc))) {
        /*
         * The key2 (pg_proc_allargtypes) can be duplicate in table.
         * We need to compare the proargmodes to make sure the function is correct.
         */
        if (!IsProArgModesEqualByTuple(ntp, m_relinfo.cc_tupdesc, argModes)) {
            continue;
        }

        tup_info->ntp = ntp;
        if (!tup_info->has_concurrent_lock) {
            tup_info->canInsertGSC = false;
        } else {
            tup_info->canInsertGSC = CanTupleInsertGSC(ntp);
            if (!tup_info->canInsertGSC) {
                /* unlock concurrent immediately, any one can invalid cache now */
                ReleaseGSCTableReadLock(&tup_info->has_concurrent_lock, m_concurrent_lock);
            }
        }
        ct = InsertHeapTupleIntoCatCacheInSingle(tup_info);
        break; /* assume only one match */
    }
    /* unlock finally */
    if (tup_info->has_concurrent_lock) {
        ReleaseGSCTableReadLock(&tup_info->has_concurrent_lock, m_concurrent_lock);
    }
    systable_endscan(scandesc);
    heap_close(relation, AccessShareLock);

    /*
     * global catcache match disk , not need negative tuple
     */
    return ct;
}
#endif
