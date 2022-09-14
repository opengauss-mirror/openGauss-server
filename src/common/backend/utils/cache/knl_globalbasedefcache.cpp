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

#include "access/transam.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "postgres.h"
#include "utils/knl_catcache.h"
#include "utils/knl_globalbasedefcache.h"
#include "utils/knl_globaldbstatmanager.h"
#include "utils/knl_partcache.h"
#include "utils/memutils.h"

static uint64 GetRelEstimateSize(GlobalRelationEntry *entry)
{
    uint64 rel_size =
        ((AllocSet)entry->rel_mem_manager)->totalSpace +        /* memcxt total space */
        GetMemoryChunkSpace(entry) +                            /* palloc GlobalRelationEntry with chunk head */
        GetMemoryChunkSpace(entry->rel_mem_manager);            /* create MemoryContext with chunk head */
    return rel_size;
}

static uint64 GetPartEstimateSize(GlobalPartitionEntry *entry)
{
    /* every palloc attached with a chunk head */
    uint64 part_size =
        GetMemoryChunkSpace(entry) +                            /* palloc GlobalPartitionEntry with chunk head */
        GetMemoryChunkSpace(entry->part) +                      /* palloc PartitionData with chunk head */
        GetMemoryChunkSpace(entry->part->pd_part) +             /* palloc pd_part with chunk head */
        (entry->part->rd_options == NULL ? 0 : GetMemoryChunkSpace(entry->part->rd_options)) +
                                                                /* palloc rd_options with chunk head */
        (entry->part->pd_indexattr == NULL ? 0 :                /* palloc pd_indexattr with chunk head */
            GetMemoryChunkSpace(entry->part->pd_indexattr)) +
        (entry->part->pd_indexlist == NULL ? 0 :
            (sizeof(List) + CHUNK_ALGIN_PAD +                   /* palloc pd_indexlist with chunk head */
                (sizeof(ListCell) + CHUNK_ALGIN_PAD) * entry->part->pd_indexlist->length));
                                                                /* and its elements */
    return part_size;
}

template <bool is_relation>
void GlobalBaseDefCache::RemoveElemFromBucket(GlobalBaseEntry *base)
{
    if (is_relation) {
        GlobalRelationEntry *entry = (GlobalRelationEntry *)base;
        uint64 rel_size = GetRelEstimateSize(entry);
        pg_atomic_fetch_sub_u64(&m_base_space, AllocSetContextUsedSpace(((AllocSet)entry->rel_mem_manager)));
        m_db_entry->MemoryEstimateSub(rel_size);
    } else {
        GlobalPartitionEntry *entry = (GlobalPartitionEntry *)base;
        uint64 part_size = GetPartEstimateSize(entry);
        pg_atomic_fetch_sub_u64(&m_base_space, part_size);
        m_db_entry->MemoryEstimateSub(part_size);
    }
    m_bucket_list.RemoveElemFromBucket(&base->cache_elem);
}
template <bool is_relation>
void GlobalBaseDefCache::AddHeadToBucket(Index hash_index, GlobalBaseEntry *base)
{
    if (is_relation) {
        GlobalRelationEntry *entry = (GlobalRelationEntry *)base;
        uint64 rel_size = GetRelEstimateSize(entry);
        pg_atomic_fetch_add_u64(&m_base_space,  AllocSetContextUsedSpace(((AllocSet)entry->rel_mem_manager)));
        m_db_entry->MemoryEstimateAdd(rel_size);
    } else {
        GlobalPartitionEntry *entry = (GlobalPartitionEntry *)base;
        uint64 part_size = GetPartEstimateSize(entry);
        pg_atomic_fetch_add_u64(&m_base_space, part_size);
        m_db_entry->MemoryEstimateAdd(part_size);
    }
    m_bucket_list.AddHeadToBucket(hash_index, &base->cache_elem);
}

template void GlobalBaseDefCache::AddHeadToBucket<true>(Index hash_index, GlobalBaseEntry *base);
template void GlobalBaseDefCache::AddHeadToBucket<false>(Index hash_index, GlobalBaseEntry *base);
template void GlobalBaseDefCache::RemoveElemFromBucket<true>(GlobalBaseEntry *base);
template void GlobalBaseDefCache::RemoveElemFromBucket<false>(GlobalBaseEntry *base);


template <bool is_relation>
void GlobalBaseEntry::Free(GlobalBaseEntry *entry)
{
    Assert(entry->refcount == 0);
    if (is_relation) {
        Assert(entry->type == GLOBAL_RELATION_ENTRY);
        if (((GlobalRelationEntry *)entry)->rel_mem_manager != NULL) {
            MemoryContextDelete(((GlobalRelationEntry *)entry)->rel_mem_manager);
        }
    } else {
        Assert(entry->type == GLOBAL_PARTITION_ENTRY);
        if (((GlobalPartitionEntry *)entry)->part != NULL) {
            PartitionDestroyPartition(((GlobalPartitionEntry *)entry)->part);
        }
    }
    pfree(entry);
}
template void GlobalBaseEntry::Free<true>(GlobalBaseEntry *entry);
template void GlobalBaseEntry::Free<false>(GlobalBaseEntry *entry);

void GlobalBaseEntry::Release()
{
    /* we dont free entry here, free when search */
    Assert(this->refcount > 0);
    (void)pg_atomic_fetch_sub_u64(&this->refcount, 1);
}

void GlobalBaseDefCache::InitHashTable()
{
    MemoryContext old = MemoryContextSwitchTo(m_db_entry->GetRandomMemCxt());
    m_bucket_list.Init(m_nbuckets);
    m_obj_locks = (pthread_rwlock_t *)palloc0(sizeof(pthread_rwlock_t) * m_nbuckets);
    for (int i = 0; i < m_nbuckets; i++) {
        PthreadRwLockInit(&m_obj_locks[i], NULL);
    }

    m_is_swappingouts = (volatile uint32 *)palloc0(sizeof(volatile uint32) * m_nbuckets);

    /* acquire more oid locks for concurrent ddl */
    if (!m_is_shared) {
        m_oid_locks = (pthread_rwlock_t *)palloc0(sizeof(pthread_rwlock_t) * m_nbuckets);
        for (int i = 0; i < m_nbuckets; i++) {
            PthreadRwLockInit(&m_oid_locks[i], NULL);
        }
    } else {
        m_oid_locks = NULL;
    }
    (void)MemoryContextSwitchTo(old);
}

void GlobalBaseDefCache::Init(int nbucket)
{
    Assert(!m_is_inited);
    m_nbuckets =  ResizeHashBucket(nbucket, g_instance.global_sysdbcache.dynamic_hash_bucket_strategy);
    InitHashTable();
}

GlobalBaseEntry *GlobalBaseDefCache::SearchReadOnly(Oid obj_oid, uint32 hash_value)
{
    pg_atomic_fetch_add_u64(m_searches, 1);
    Index hash_index = HASH_INDEX(hash_value, (uint32)m_nbuckets);
    pthread_rwlock_t *obj_lock = &m_obj_locks[hash_index];

    int location = INVALID_LOCATION;
    PthreadRWlockRdlock(LOCAL_SYSDB_RESOWNER, obj_lock);
    GlobalBaseEntry *entry = FindEntryWithIndex(obj_oid, hash_index, &location);
    PthreadRWlockUnlock(LOCAL_SYSDB_RESOWNER, obj_lock);
    if (entry == NULL) {
        return NULL;
    }
    pg_atomic_fetch_add_u64(m_hits, 1);
    TopnLruMoveToFront(&entry->cache_elem, m_bucket_list.GetBucket(hash_index), obj_lock, location);
    return entry;
}

template <bool is_relation>
void GlobalBaseDefCache::FreeDeadEntrys()
{
    while (m_dead_entries.GetLength() > 0) {
        Dlelem *elt = m_dead_entries.RemoveHead();
        if (elt == NULL) {
            break;
        }
        GlobalBaseEntry *entry = (GlobalBaseEntry *)DLE_VAL(elt);
        if (entry->refcount != 0) {
            /* we move the active entry to tail of list and let next call free it */
            m_dead_entries.AddTail(&entry->cache_elem);
            break;
        } else {
            entry->Free<is_relation>(entry);
        }
    }
}
template void GlobalBaseDefCache::FreeDeadEntrys<false>();
template void GlobalBaseDefCache::FreeDeadEntrys<true>();

template <bool is_relation>
void GlobalBaseDefCache::Invalidate(Oid dbid, Oid obj_oid)
{
    uint32 hash_value = oid_hash((void *)&(obj_oid), sizeof(Oid));
    pthread_rwlock_t *oid_lock = NULL;
    bool need_oid_lock = !is_relation || !IsSystemObjOid(obj_oid);
    if (need_oid_lock) {
        oid_lock = GetHashValueLock(hash_value);
        PthreadRWlockWrlock(LOCAL_SYSDB_RESOWNER, oid_lock);
    }

    Index hash_index = HASH_INDEX(hash_value, (uint32)m_nbuckets);
    pthread_rwlock_t *obj_lock = &m_obj_locks[hash_index];
    PthreadRWlockWrlock(LOCAL_SYSDB_RESOWNER, obj_lock);
    for (Dlelem *elt = DLGetHead(m_bucket_list.GetBucket(hash_index)); elt != NULL;) {
        GlobalBaseEntry *entry = (GlobalBaseEntry *)DLE_VAL(elt);
        elt = DLGetSucc(elt);
        if (entry->oid != obj_oid) {
            continue;
        }
        HandleDeadEntry<is_relation>(entry);
    }
    PthreadRWlockUnlock(LOCAL_SYSDB_RESOWNER, obj_lock);
    if (need_oid_lock) {
        PthreadRWlockUnlock(LOCAL_SYSDB_RESOWNER, oid_lock);
    }
}
template void GlobalBaseDefCache::Invalidate<true>(Oid dbid, Oid obj_oid);
template void GlobalBaseDefCache::Invalidate<false>(Oid dbid, Oid obj_oid);

template <bool is_relation>
void GlobalBaseDefCache::InvalidateRelationNodeListBy(bool (*IsInvalidEntry)(GlobalBaseEntry *))
{
    for (int hash_index = 0; hash_index < m_nbuckets; hash_index++) {
        pthread_rwlock_t *obj_lock = &m_obj_locks[hash_index];
        PthreadRWlockWrlock(LOCAL_SYSDB_RESOWNER, obj_lock);
        for (Dlelem *elt = DLGetHead(m_bucket_list.GetBucket(hash_index)); elt != NULL;) {
            GlobalBaseEntry *entry = (GlobalBaseEntry *)DLE_VAL(elt);
            elt = DLGetSucc(elt);
            if (IsInvalidEntry(entry)) {
                HandleDeadEntry<is_relation>(entry);
            }
        }
        PthreadRWlockUnlock(LOCAL_SYSDB_RESOWNER, obj_lock);
    }
}
template void GlobalBaseDefCache::InvalidateRelationNodeListBy<true>(bool (*IsInvalidEntry)(GlobalBaseEntry *));

template <bool is_relation, bool force>
void GlobalBaseDefCache::ResetCaches()
{
    for (int hash_index = 0; hash_index < m_nbuckets; hash_index++) {
        /* if not force, we are swappingout, oid lock is not needed */
        if (force && !m_is_shared) {
            PthreadRWlockWrlock(LOCAL_SYSDB_RESOWNER, &m_oid_locks[hash_index]);
        }

        pthread_rwlock_t *obj_lock = &m_obj_locks[hash_index];
        PthreadRWlockWrlock(LOCAL_SYSDB_RESOWNER, obj_lock);
        for (Dlelem *elt = DLGetHead(m_bucket_list.GetBucket(hash_index)); elt;) {
            GlobalBaseEntry *entry = (GlobalBaseEntry *)DLE_VAL(elt);
            elt = DLGetSucc(elt);
            if (force || entry->refcount == 0) {
                HandleDeadEntry<is_relation>(entry);
            }
        }
        PthreadRWlockUnlock(LOCAL_SYSDB_RESOWNER, obj_lock);

        if (force && !m_is_shared) {
            PthreadRWlockUnlock(LOCAL_SYSDB_RESOWNER, &m_oid_locks[hash_index]);
        }
    }
}
template void GlobalBaseDefCache::ResetCaches<false, false>();
template void GlobalBaseDefCache::ResetCaches<false, true>();
template void GlobalBaseDefCache::ResetCaches<true, false>();
template void GlobalBaseDefCache::ResetCaches<true, true>();


template <bool is_relation>
void GlobalBaseDefCache::HandleDeadEntry(GlobalBaseEntry *entry)
{
    RemoveElemFromBucket<is_relation>(entry);
    if (entry->refcount == 0) {
        m_dead_entries.AddHead(&entry->cache_elem);
    } else {
        m_dead_entries.AddTail(&entry->cache_elem);
    }
}
template void GlobalBaseDefCache::HandleDeadEntry<false>(GlobalBaseEntry *entry);
template void GlobalBaseDefCache::HandleDeadEntry<true>(GlobalBaseEntry *entry);

GlobalBaseEntry *GlobalBaseDefCache::FindEntryWithIndex(Oid obj_oid, Index hash_index, int *location)
{
    int index = 0;
    for (Dlelem *elt = DLGetHead(m_bucket_list.GetBucket(hash_index)); elt != NULL; elt = DLGetSucc(elt)) {
        index++;
        GlobalBaseEntry *entry = (GlobalBaseEntry *)DLE_VAL(elt);
        if (entry->oid != obj_oid) {
            continue;
        }
        pg_atomic_fetch_add_u64(&entry->refcount, 1);
        *location = index;
        return entry;
    }
    return NULL;
}

bool GlobalBaseDefCache::EntryExist(Oid obj_oid, Index hash_index)
{
    for (Dlelem *elt = DLGetHead(m_bucket_list.GetBucket(hash_index)); elt != NULL; elt = DLGetSucc(elt)) {
        GlobalBaseEntry *entry = (GlobalBaseEntry *)DLE_VAL(elt);
        if (entry->oid != obj_oid) {
            continue;
        }
        return true;
    }
    return false;
}

template <bool is_relation>
void GlobalBaseDefCache::RemoveTailElements(Index hash_index)
{
    /* shared db never do lru on tabdef */
    if (m_is_shared) {
        return;
    }

    /* only one thread can do swapout for the bucket */
    ResourceOwnerEnlargeGlobalIsExclusive(LOCAL_SYSDB_RESOWNER);
    if (!atomic_compare_exchange_u32(&m_is_swappingouts[hash_index], 0, 1)) {
        return;
    }
    ResourceOwnerRememberGlobalIsExclusive(LOCAL_SYSDB_RESOWNER, &m_is_swappingouts[hash_index]);

    bool listBelowThreshold = m_bucket_list.GetBucket(hash_index)->dll_len < MAX_GSC_LIST_LENGTH;

    uint64 swapout_count_once = 0;
    PthreadRWlockWrlock(LOCAL_SYSDB_RESOWNER, &m_obj_locks[hash_index]);
    uint64 max_swapout_count_once = GetSwapOutNum(listBelowThreshold, m_bucket_list.GetBucket(hash_index)->dll_len);
    for (Dlelem *elt = DLGetTail(m_bucket_list.GetBucket(hash_index)); elt != NULL;) {
        Dlelem *tmp = elt;
        elt = DLGetPred(elt);
        if (is_relation) {
            GlobalRelationEntry *entry = (GlobalRelationEntry *)DLE_VAL(tmp);
            if (g_instance.global_sysdbcache.RelationHasSysCache(entry->rel->rd_id) ||
                unlikely(!RelationHasReferenceCountZero(entry->rel))) {
                Assert(entry->type == GLOBAL_RELATION_ENTRY);
                DLMoveToFront(&entry->cache_elem);
                break;
            }
        }
        HandleDeadEntry<is_relation>((GlobalBaseEntry *)DLE_VAL(tmp));
        swapout_count_once++;

        /* keep elements as many as possible */
        if (swapout_count_once == max_swapout_count_once) {
            break;
        }
    }
    PthreadRWlockUnlock(LOCAL_SYSDB_RESOWNER, &m_obj_locks[hash_index]);

    Assert(m_is_swappingouts[hash_index] == 1);
    atomic_compare_exchange_u32(&m_is_swappingouts[hash_index], 1, 0);
    ResourceOwnerForgetGlobalIsExclusive(LOCAL_SYSDB_RESOWNER, &m_is_swappingouts[hash_index]);
}
template void GlobalBaseDefCache::RemoveTailElements<false>(Index hash_index);
template void GlobalBaseDefCache::RemoveTailElements<true>(Index hash_index);

template <bool is_relation>
void GlobalBaseDefCache::RemoveAllTailElements()
{
    /* shared db never do lru on tabdef */
    if (m_is_shared) {
        return;
    }
    for (int hash_index = 0; hash_index < m_nbuckets; hash_index++) {
        RemoveTailElements<is_relation>(hash_index);
#ifndef ENABLE_LITE_MODE
        /* memory is under control, so stop swapout */
        if (g_instance.global_sysdbcache.MemoryUnderControl()) {
            break;
        }
#endif
    }
}

template void GlobalBaseDefCache::RemoveAllTailElements<false>();
template void GlobalBaseDefCache::RemoveAllTailElements<true>();

GlobalBaseDefCache::GlobalBaseDefCache(Oid db_oid, bool is_shared, GlobalSysDBCacheEntry *entry, char relkind)
{
    m_db_oid = db_oid;
    m_is_shared = is_shared;
    m_is_inited = false;

    m_relkind = relkind;
    if (m_relkind == PARTTYPE_PARTITIONED_RELATION) {
        m_searches = &entry->m_dbstat->part_searches;
        m_hits = &entry->m_dbstat->part_hits;
        m_newloads = &entry->m_dbstat->part_newloads;
    } else {
        Assert(m_relkind == RELKIND_RELATION);
        m_searches = &entry->m_dbstat->rel_searches;
        m_hits = &entry->m_dbstat->rel_hits;
        m_newloads = &entry->m_dbstat->rel_newloads;
    }
    m_base_space = 0;
    m_obj_locks = NULL;
    m_db_entry = entry;
}
