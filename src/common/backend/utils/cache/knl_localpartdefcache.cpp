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

#include "access/xact.h"
#include "knl/knl_instance.h"
#include "storage/smgr/smgr.h"
#include "utils/knl_catcache.h"
#include "utils/knl_localtabdefcache.h"
#include "utils/knl_partcache.h"
#include "utils/memutils.h"
#include "utils/rel_gs.h"
#include "utils/resowner.h"
#include "utils/snapmgr.h"

static bool IsPartitionStoreInglobal(Partition part);
LocalPartDefCache::LocalPartDefCache()
{
    ResetInitFlag();
}

Partition LocalPartDefCache::SearchPartitionFromLocal(Oid part_oid)
{
    if (unlikely(!m_is_inited)) {
        return NULL;
    }
    uint32 hash_value = oid_hash((void *)&(part_oid), sizeof(Oid));
    Index hash_index = HASH_INDEX(hash_value, (uint32)m_nbuckets);
    LocalPartitionEntry *entry = (LocalPartitionEntry *)LocalBaseDefCache::SearchEntryFromLocal(part_oid, hash_index);
    if (unlikely(entry == NULL)) {
        return NULL;
    }
    return entry->part;
}

static inline void SpecialWorkForLocalPart(Partition part)
{
    if (part->pd_part->parentid != InvalidOid || part->pd_part->relfilenode != InvalidOid) {
        PartitionInitPhysicalAddr(part);
    }
}

void LocalPartDefCache::CopyLocalPartition(Partition dest, Partition src)
{
    CopyPartitionData(dest, src);
    SpecialWorkForLocalPart(dest);
}

template <bool insert_into_local>
Partition LocalPartDefCache::SearchPartitionFromGlobalCopy(Oid part_oid)
{
    if (unlikely(!m_is_inited)) {
        return NULL;
    }
    if (invalid_entries.ExistDefValue(part_oid)) {
        return NULL;
    }
    if (HistoricSnapshotActive()) {
        return NULL;
    }

    if (!g_instance.global_sysdbcache.hot_standby) {
        return NULL;
    }
    if (unlikely(!IsPrimaryRecoveryFinished())) {
        return NULL;
    }
    if (unlikely(u_sess->attr.attr_common.IsInplaceUpgrade)) {
        return NULL;
    }
    uint32 hash_value = oid_hash((void *)&(part_oid), sizeof(Oid));
    ResourceOwnerEnlargeGlobalBaseEntry(LOCAL_SYSDB_RESOWNER);
    GlobalPartitionEntry *global = (GlobalPartitionEntry *)m_global_partdefcache->SearchReadOnly(part_oid, hash_value);
    if (global == NULL) {
        return NULL;
    }
    ResourceOwnerRememberGlobalBaseEntry(LOCAL_SYSDB_RESOWNER, global);

    MemoryContext old = MemoryContextSwitchTo(LocalMyDBCacheMemCxt());
    Partition copy = (Partition)palloc(sizeof(PartitionData));
    CopyLocalPartition(copy, global->part);
    MemoryContextSwitchTo(old);

    ResourceOwnerForgetGlobalBaseEntry(LOCAL_SYSDB_RESOWNER, global);
    global->Release();
    if (insert_into_local) {
        Index hash_index = HASH_INDEX(hash_value, (uint32)m_nbuckets);
        RemovePartitionByOid(part_oid, hash_index);
        CreateLocalPartEntry(copy, hash_index);
    }
    return copy;
}

template Partition LocalPartDefCache::SearchPartitionFromGlobalCopy<true>(Oid part_id);
template Partition LocalPartDefCache::SearchPartitionFromGlobalCopy<false>(Oid part_id);

Partition LocalPartDefCache::SearchPartition(Oid part_oid)
{
    if (unlikely(!m_is_inited)) {
        return NULL;
    }
    Partition local = SearchPartitionFromLocal(part_oid);
    if (likely(local != NULL || IsBootstrapProcessingMode())) {
        return local;
    }
    return SearchPartitionFromGlobalCopy<true>(part_oid);
}

void LocalPartDefCache::RemovePartition(Partition part)
{
    m_bucket_list.RemoveElemFromBucket(&part->entry->cache_elem);
    pfree_ext(part->entry);
}

void LocalPartDefCache::CreateLocalPartEntry(Partition part, Index hash_index)
{
    if (t_thrd.lsc_cxt.lsc->LocalSysDBCacheNeedSwapOut()) {
        LocalBaseDefCache::RemoveTailDefElements<false>();
    }
    LocalPartitionEntry *entry =
        (LocalPartitionEntry *)LocalBaseDefCache::CreateEntry(hash_index, sizeof(LocalPartitionEntry));
    entry->part = part;
    entry->oid = part->pd_id;
    entry->obj_is_nailed = false;
    part->entry = entry;
}

void LocalPartDefCache::InsertPartitionIntoLocal(Partition part)
{
    uint32 hash_value = oid_hash((void *)&(part->pd_id), sizeof(Oid));
    Index hash_index = HASH_INDEX(hash_value, (uint32)m_nbuckets);
    CreateLocalPartEntry(part, hash_index);
}

static bool IsPartOidStoreInGlobal(Oid part_oid)
{
    if (unlikely(IsBootstrapProcessingMode())) {
        return false;
    }
    if (unlikely(t_thrd.lsc_cxt.lsc->GetThreadDefExclusive())) {
        return false;
    }

    if (unlikely(t_thrd.lsc_cxt.lsc->partdefcache.invalid_entries.ExistDefValue(part_oid))) {
        return false;
    }

    if (unlikely(u_sess->attr.attr_common.IsInplaceUpgrade)) {
        return false;
    }

    if (HistoricSnapshotActive()) {
        return false;
    }

    if (!g_instance.global_sysdbcache.hot_standby) {
        return false;
    }
    if (unlikely(!IsPrimaryRecoveryFinished())) {
        return false;
    }
    if (g_instance.global_sysdbcache.StopInsertGSC()) {
        return false;
    }
    return true;
}

static bool IsPartitionStoreInglobal(Partition part)
{
    Assert(part->pd_createSubid == InvalidSubTransactionId);
    Assert(part->pd_newRelfilenodeSubid == InvalidSubTransactionId);
    Assert(part->pd_isvalid);

    return true;
}

void LocalPartDefCache::InsertPartitionIntoGlobal(Partition part, uint32 hash_value)
{
    if (!IsPartitionStoreInglobal(part)) {
        return;
    }
    /* when insert, we must make sure m_global_shared_tabdefcache is inited at least */
    Assert(m_global_partdefcache != NULL);
    m_global_partdefcache->Insert(part, hash_value);
}

Partition LocalPartDefCache::RemovePartitionByOid(Oid part_oid, Index hash_index)
{
    LocalPartitionEntry *entry = (LocalPartitionEntry *)LocalBaseDefCache::SearchEntryFromLocal(part_oid, hash_index);
    if (entry == NULL) {
        return NULL;
    }
    Partition old_part = entry->part;
    RemovePartition(entry->part);
    return old_part;
}

void LocalPartDefCache::Init()
{
    if (m_is_inited) {
        return;
    }
    m_global_partdefcache = t_thrd.lsc_cxt.lsc->GetGlobalPartDefCache();
    m_db_id = t_thrd.lsc_cxt.lsc->my_database_id;
    PartCacheNeedEOXActWork = false;
    m_is_inited = true;
}

void LocalPartDefCache::InvalidateGlobalPartition(Oid db_id, Oid part_oid, bool is_commit)
{
    if (!is_commit) {
        invalid_entries.InsertInvalidDefValue(part_oid);
        return;
    }
    Assert(db_id != InvalidOid);
    Assert(CheckMyDatabaseMatch());
    if (m_global_partdefcache == NULL) {
        Assert(!m_is_inited);
        GlobalSysDBCacheEntry *entry = g_instance.global_sysdbcache.FindTempGSCEntry(db_id);
        if (entry == NULL) {
            return;
        }
        entry->m_partdefCache->Invalidate(db_id, part_oid);
        g_instance.global_sysdbcache.ReleaseTempGSCEntry(entry);
    } else {
        Assert(db_id == t_thrd.lsc_cxt.lsc->my_database_id);
        m_global_partdefcache->Invalidate(db_id, part_oid);
    }
}

void LocalPartDefCache::InvalidateAll(void)
{
    if (unlikely(!m_is_inited)) {
        SetInvalMsgProcListInvalAll();
        return;
    }
    List *rebuildList = NIL;
    Dlelem *bucket_elt;
    forloopactivebucketlist(bucket_elt, m_bucket_list.GetActiveBucketList()) {
        Dlelem *elt;
        forloopbucket(elt, bucket_elt) {
            LocalPartitionEntry *entry = (LocalPartitionEntry *)DLE_VAL(elt);
            elt = DLGetSucc(elt);
            Partition part = entry->part;
            Assert(part->entry == entry);

            /* Must close all smgr references to avoid leaving dangling ptrs */
            PartitionCloseSmgr(part);

            /* Ignore new relations, since they are never cross-backend targets */
            if (part->pd_createSubid != InvalidSubTransactionId)
                continue;

            if (PartitionHasReferenceCountZero(part)) {
                /* Delete this entry immediately */
                PartitionClearPartition(part, false);
            } else {
                rebuildList = lappend(rebuildList, part);
            }
        }
    }

    /*
     * Now zap any remaining smgr cache entries.  This must happen before we
     * start to rebuild entries, since that may involve catalog fetches which
     * will re-open catalog files.
     */
    smgrcloseall();

    ListCell *l = NULL;
    /* Phase 2: rebuild the items found to need rebuild in phase 1 */
    foreach (l, rebuildList) {
        Partition part = (Partition)lfirst(l);
        PartitionClearPartition(part, true);
    }
    list_free_ext(rebuildList);

    SetInvalMsgProcListInvalAll();
}

void LocalPartDefCache::AtEOXact_PartitionCache(bool isCommit)
{
    invalid_entries.ResetInitFlag();
    /*
     * To speed up transaction exit, we want to avoid scanning the partitioncache
     * unless there is actually something for this routine to do.  Other than
     * the debug-only Assert checks, most transactions don't create any work
     * for us to do here, so we keep a static flag that gets set if there is
     * anything to do.	(Currently, this means either a partition is created in
     * the current xact, or one is given a new relfilenode, or an index list
     * is forced.)	For simplicity, the flag remains set till end of top-level
     * transaction, even though we could clear it at subtransaction end in
     * some cases.
     */
    if (!GetPartCacheNeedEOXActWork()
#ifdef USE_ASSERT_CHECKING
        && !assert_enabled
#endif
    ) {
        return;
    }

    Dlelem *bucket_elt;
    forloopactivebucketlist(bucket_elt, m_bucket_list.GetActiveBucketList()) {
        Dlelem *elt;
        forloopbucket(elt, bucket_elt) {
            LocalPartitionEntry *entry = (LocalPartitionEntry *)DLE_VAL(elt);
            elt = DLGetSucc(elt);
            Partition part = entry->part;

            /*
             * The relcache entry's ref count should be back to its normal
             * not-in-a-transaction state: 0 unless it's nailed in cache.
             *
             * In bootstrap mode, this is NOT true, so don't check it --- the
             * bootstrap code expects relations to stay open across start/commit
             * transaction calls.  (That seems bogus, but it's not worth fixing.)
             */
#ifdef USE_ASSERT_CHECKING
            if (!IsBootstrapProcessingMode()) {
                const int expected_refcnt = 0;
                Assert(part->pd_refcnt == expected_refcnt);
            }
#endif

            /*
             * Is it a partition created in the current transaction?
             *
             * During commit, reset the flag to zero, since we are now out of the
             * creating transaction.  During abort, simply delete the relcache
             * entry --- it isn't interesting any longer.  (NOTE: if we have
             * forgotten the new-ness of a new relation due to a forced cache
             * flush, the entry will get deleted anyway by shared-cache-inval
             * processing of the aborted pg_class insertion.)
             */
            if (part->pd_createSubid != InvalidSubTransactionId) {
                if (isCommit) {
                    part->pd_createSubid = InvalidSubTransactionId;
                } else {
                    PartitionClearPartition(part, false);
                    continue;
                }
            }

            /*
             * Likewise, reset the hint about the relfilenode being new.
             */
            part->pd_newRelfilenodeSubid = InvalidSubTransactionId;
        }
    }

    /* Once done with the transaction, we can reset need_eoxact_work */
    SetPartCacheNeedEOXActWork(false);
}

void LocalPartDefCache::AtEOSubXact_PartitionCache(bool isCommit, SubTransactionId mySubid,
    SubTransactionId parentSubid)
{
    /*
     * Skip the relcache scan if nothing to do --- see notes for
     * AtEOXact_PartitionCache.
     */
    if (!GetPartCacheNeedEOXActWork())
        return;

    Dlelem *bucket_elt;
    forloopactivebucketlist(bucket_elt, m_bucket_list.GetActiveBucketList()) {
        Dlelem *elt;
        forloopbucket(elt, bucket_elt) {
            LocalPartitionEntry *entry = (LocalPartitionEntry *)DLE_VAL(elt);
            elt = DLGetSucc(elt);
            Partition part = entry->part;

            /*
             * Is it a partition created in the current subtransaction?
             *
             * During subcommit, mark it as belonging to the parent, instead.
             * During subabort, simply delete the partition entry.
             */
            if (part->pd_createSubid == mySubid) {
                if (isCommit)
                    part->pd_createSubid = parentSubid;
                else {
                    PartitionClearPartition(part, false);
                    continue;
                }
            }

            /*
             * Likewise, update or drop any new-relfilenode-in-subtransaction
             * hint.
             */
            if (part->pd_newRelfilenodeSubid == mySubid) {
                if (isCommit)
                    part->pd_newRelfilenodeSubid = parentSubid;
                else
                    part->pd_newRelfilenodeSubid = InvalidSubTransactionId;
            }
        }
    }
}

Partition LocalPartDefCache::PartitionIdGetPartition(Oid part_oid, StorageType storage_type)
{
    Partition pd;
    /*
     * first try to find reldesc in the cache
     */
    pd = SearchPartition(part_oid);
    if (PartitionIsValid(pd)) {
        PartitionIncrementReferenceCount(pd);
        /* revalidate cache entry if necessary */
        if (!pd->pd_isvalid) {
            /*
             * Indexes only have a limited number of possible schema changes,
             * and we don't want to use the full-blown procedure because it's
             * a headache for indexes that reload itself depends on.
             */
            if (pd->pd_part->parttype == PART_OBJ_TYPE_INDEX_PARTITION) {
                PartitionReloadIndexInfo(pd);
            } else {
                PartitionClearPartition(pd, true);
            }
        }
        return pd;
    }

    /*
     * no partdesc in the cache, so have PartitionBuildDesc() build one and add
     * it.
     */
    bool is_oid_store_in_global = IsPartOidStoreInGlobal(part_oid);
    bool has_concurrent_lock = is_oid_store_in_global;
    uint32 hash_value = oid_hash((void *)&(part_oid), sizeof(Oid));
    pthread_rwlock_t *oid_lock = NULL;
    if (has_concurrent_lock) {
        oid_lock = m_global_partdefcache->GetHashValueLock(hash_value);
        AcquireGSCTableReadLock(&has_concurrent_lock, oid_lock);
    }
    pd = PartitionBuildDesc(part_oid, storage_type, true, false);
    if (PartitionIsValid(pd) && is_oid_store_in_global && has_concurrent_lock) {
        InsertPartitionIntoGlobal(pd, hash_value);
    }
    if (has_concurrent_lock) {
        ReleaseGSCTableReadLock(&has_concurrent_lock, oid_lock);
    }

    if (PartitionIsValid(pd)) {
        PartitionIncrementReferenceCount(pd);
    }

    return pd;
}
