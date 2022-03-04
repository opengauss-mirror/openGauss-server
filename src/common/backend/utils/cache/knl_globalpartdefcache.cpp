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
#include "miscadmin.h"
#include "utils/knl_catcache.h"
#include "utils/knl_globaldbstatmanager.h"
#include "utils/knl_globalpartdefcache.h"
#include "utils/knl_globaltabdefcache.h"
#include "utils/knl_partcache.h"
#include "utils/memutils.h"

static void PartitionPointerToNULL(Partition part)
{
    part->pd_indexattr = NULL;
    part->pd_indexlist = NULL;
    part->pd_part = NULL;
    part->pd_smgr = NULL;
    part->rd_options = NULL;
    part->pd_pgstat_info = NULL;
}

void CopyPartitionData(Partition dest_partition, Partition src_partition)
{
    /* if you add variable to partition, please check if you need put it in gsc,
     * if not, set it zero when copy, and reinit it when local get the copy result
     * if the variable changed, there is no lock and no part inval msg,
     * set it zero and reinit it when copy into local */
    Assert(sizeof(PartitionData) == 168);
    *dest_partition = *src_partition;
    /* init all pointers to NULL, so we can free memory correctly when meeting exception */
    PartitionPointerToNULL(dest_partition);
    dest_partition->pd_indexattr = bms_copy(src_partition->pd_indexattr);
    dest_partition->pd_indexlist = list_copy(src_partition->pd_indexlist);
    Assert(src_partition->pd_refcnt == 0);
    dest_partition->pd_refcnt = 0;

    /* We just copy fixed field */
    dest_partition->pd_part = (Form_pg_partition)palloc(PARTITION_TUPLE_SIZE);
    memcpy_s(dest_partition->pd_part, PARTITION_TUPLE_SIZE, src_partition->pd_part, PARTITION_TUPLE_SIZE);

    dest_partition->pd_smgr = NULL;
    Assert(src_partition->pd_isvalid);
    dest_partition->pd_isvalid = true;
    dest_partition->rd_options = CopyOption(src_partition->rd_options);
    Assert(src_partition->pd_pgstat_info == NULL);
    dest_partition->pd_pgstat_info = NULL;
}

void GlobalPartDefCache::Insert(Partition part, uint32 hash_value)
{
    Index hash_index = HASH_INDEX(hash_value, (uint32)m_nbuckets);
    /* dllist is too long, swapout some */
    if (m_bucket_list.GetBucket(hash_index)->dll_len >= MAX_GSC_LIST_LENGTH) {
        GlobalBaseDefCache::RemoveTailElements<false>(hash_index);
        /* maybe no element can be swappedout */
        return;
    }

    pthread_rwlock_t *obj_lock = &m_obj_locks[hash_index];
    GlobalPartitionEntry *entry = CreateEntry(part);
    PthreadRWlockWrlock(LOCAL_SYSDB_RESOWNER, obj_lock);
    bool found = GlobalBaseDefCache::EntryExist(part->pd_id, hash_index);
    if (found) {
        PthreadRWlockUnlock(LOCAL_SYSDB_RESOWNER, obj_lock);
        entry->Free<false>(entry);
        return;
    }
    GlobalBaseDefCache::AddHeadToBucket<false>(hash_index, entry);
    PthreadRWlockUnlock(LOCAL_SYSDB_RESOWNER, obj_lock);
    pg_atomic_fetch_add_u64(m_newloads, 1);
}

GlobalPartitionEntry *GlobalPartDefCache::CreateEntry(Partition part)
{
    ResourceOwnerEnlargeGlobalBaseEntry(LOCAL_SYSDB_RESOWNER);
    MemoryContext old = MemoryContextSwitchTo(m_db_entry->GetRandomMemCxt());
    GlobalPartitionEntry *entry = (GlobalPartitionEntry *)palloc(sizeof(GlobalPartitionEntry));
    entry->type = GLOBAL_PARTITION_ENTRY;
    entry->oid = part->pd_id;
    entry->refcount = 0;
    entry->part = NULL;
    DLInitElem(&entry->cache_elem, (void *)entry);
    ResourceOwnerRememberGlobalBaseEntry(LOCAL_SYSDB_RESOWNER, entry);
    entry->part = (Partition)palloc0(sizeof(PartitionData));
    CopyPartitionData(entry->part, part);
    ResourceOwnerForgetGlobalBaseEntry(LOCAL_SYSDB_RESOWNER, entry);
    MemoryContextSwitchTo(old);
    return entry;
}

void GlobalPartDefCache::Init()
{
    MemoryContext old = MemoryContextSwitchTo(m_db_entry->GetRandomMemCxt());
    GlobalBaseDefCache::Init(GLOBAL_INIT_PARTCACHE_SIZE);
    MemoryContextSwitchTo(old);
    m_is_inited = true;
}

GlobalPartDefCache::GlobalPartDefCache(Oid dbOid, bool isShared, struct GlobalSysDBCacheEntry *entry)
    : GlobalBaseDefCache(dbOid, isShared, entry, PARTTYPE_PARTITIONED_RELATION)
{
    m_is_inited = false;
}

template void GlobalPartDefCache::ResetPartCaches<false>();
template void GlobalPartDefCache::ResetPartCaches<true>();