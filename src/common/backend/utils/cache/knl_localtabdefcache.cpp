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


#include "catalog/indexing.h"
#include "catalog/pg_amproc.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_rewrite.h"
#include "catalog/pg_trigger.h"
#include "catalog/storage_gtt.h"
#include "catalog/heap.h"
#include "commands/matview.h"
#include "commands/sec_rls_cmds.h"
#include "postmaster/autovacuum.h"
#include "pgxc/bucketmap.h"
#include "utils/knl_catcache.h"
#include "knl/knl_instance.h"
#include "utils/knl_localtabdefcache.h"
#include "utils/knl_relcache.h"
#include "utils/memutils.h"
#include "utils/relmapper.h"
#include "utils/sec_rls_utils.h"


bool has_locator_info(GlobalBaseEntry *entry)
{
    return ((GlobalRelationEntry *)entry)->rel->rd_locator_info != NULL;
}

static bool IsRelationStoreInglobal(Relation rel);

LocalTabDefCache::LocalTabDefCache()
{
    ResetInitFlag();
}

static void SetGttInfo(Relation rel)
{
    if (rel->rd_rel->relpersistence == RELPERSISTENCE_GLOBAL_TEMP && rel->rd_backend != BackendIdForTempRelations) {
        RelationCloseSmgr(rel);
        rel->rd_backend = BackendIdForTempRelations;
        BlockNumber relpages = 0;
        double reltuples = 0;
        BlockNumber relallvisible = 0;
        get_gtt_relstats(RelationGetRelid(rel), &relpages, &reltuples, &relallvisible, NULL);
        rel->rd_rel->relpages = (float8)relpages;
        rel->rd_rel->reltuples = (float8)reltuples;
        rel->rd_rel->relallvisible = (int4)relallvisible;
    }
}

Relation LocalTabDefCache::SearchRelationFromLocal(Oid rel_oid)
{
    uint32 hash_value = oid_hash((void *)&(rel_oid), sizeof(Oid));
    Index hash_index = HASH_INDEX(hash_value, (uint32)m_nbuckets);
    LocalRelationEntry *entry = (LocalRelationEntry *)LocalBaseDefCache::SearchEntryFromLocal(rel_oid, hash_index);

    if (unlikely(entry == NULL)) {
        return NULL;
    }
    Assert(entry->rel->rd_node.spcNode != InvalidOid);
    Assert(entry->rel->rd_node.relNode != InvalidOid);
    Assert(entry->rel->rd_islocaltemp == false);
    SetGttInfo(entry->rel);
    return entry->rel;
}

template <bool insert_into_local>
Relation LocalTabDefCache::SearchRelationFromGlobalCopy(Oid rel_oid)
{
    if (unlikely(!m_is_inited)) {
        return NULL;
    }
    if (invalid_entries.ExistDefValue(rel_oid)) {
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
    uint32 hash_value = oid_hash((void *)&(rel_oid), sizeof(Oid));
    Index hash_index = HASH_INDEX(hash_value, (uint32)m_nbuckets);
    ResourceOwner owner = LOCAL_SYSDB_RESOWNER;
    ResourceOwnerEnlargeGlobalBaseEntry(owner);
    GlobalRelationEntry *global;
    if (g_instance.global_sysdbcache.HashSearchSharedRelation(rel_oid)) {
        global = m_global_shared_tabdefcache->SearchReadOnly(rel_oid, hash_value);
    } else {
        Assert(m_global_tabdefcache != NULL);
        Assert(m_is_inited_phase2);
        if (!m_is_inited_phase2 || m_global_tabdefcache == NULL) {
            ereport(FATAL,
                (errcode(ERRCODE_INVALID_STATUS),
                    errmsg("rel_oid %u is shared but search return false", rel_oid)));
        }
        global = m_global_tabdefcache->SearchReadOnly(rel_oid, hash_value);
    }

    if (global == NULL) {
        return NULL;
    }
    ResourceOwnerRememberGlobalBaseEntry(owner, global);

    MemoryContext old = MemoryContextSwitchTo(LocalMyDBCacheMemCxt());
    Relation copy = (Relation)palloc(sizeof(RelationData));
    CopyLocalRelation(copy, global->rel);
    MemoryContextSwitchTo(old);

    ResourceOwnerForgetGlobalBaseEntry(owner, global);
    global->Release();
    if (insert_into_local) {
        Assert(RemoveRelationByOid(rel_oid, hash_index) == NULL);
        CreateLocalRelEntry(copy, hash_index);
    }

    Assert(copy->rd_node.spcNode != InvalidOid);
    Assert(copy->rd_node.relNode != InvalidOid);
    return copy;
}
template Relation LocalTabDefCache::SearchRelationFromGlobalCopy<true>(Oid rel_oid);
template Relation LocalTabDefCache::SearchRelationFromGlobalCopy<false>(Oid rel_oid);

Relation LocalTabDefCache::SearchRelation(Oid rel_oid)
{
    Relation rel = SearchRelationFromLocal(rel_oid);
    if (rel == NULL) {
        rel = SearchRelationFromGlobalCopy<true>(rel_oid);
    }
    return rel;
}

void LocalTabDefCache::CreateLocalRelEntry(Relation rel, Index hash_index)
{
    if (t_thrd.lsc_cxt.lsc->LocalSysDBCacheNeedSwapOut()) {
        LocalBaseDefCache::RemoveTailDefElements<true>();
    }
    Assert(rel->rd_fdwroutine == NULL);
    Assert(rel->rd_isnailed ? rel->rd_refcnt == 1 : rel->rd_refcnt == 0);
    Assert(rel->rd_att->tdrefcount > 0);
    LocalRelationEntry *entry =
        (LocalRelationEntry *)LocalBaseDefCache::CreateEntry(hash_index, sizeof(LocalRelationEntry));
    entry->rel = rel;
    entry->oid = rel->rd_id;
    entry->obj_is_nailed = rel->rd_isnailed;
    rel->entry = entry;
    RememberRelSonMemCxtSpace(rel);
}

static bool IsRelOidStoreInGlobal(Oid rel_oid)
{
    /* pgxc node need reload, we invalid all relcache with locator info */
    if (unlikely(IsGotPoolReload() && IS_PGXC_COORDINATOR && !IsSystemObjOid(rel_oid))) {
        return false;
    }
    if (unlikely(IsBootstrapProcessingMode())) {
        return false;
    }
    if (unlikely(t_thrd.lsc_cxt.lsc->GetThreadDefExclusive())) {
        return false;
    }

    if (unlikely(t_thrd.lsc_cxt.lsc->tabdefcache.invalid_entries.ExistDefValue(rel_oid))) {
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

static bool IsRelationStoreInglobal(Relation rel)
{
    Assert(rel->rd_createSubid == InvalidSubTransactionId);
    Assert(rel->rd_newRelfilenodeSubid == InvalidSubTransactionId);
    Assert(rel->rd_rel->relowner != InvalidOid);
    Assert(rel->rd_isvalid);
    /* 2 is tmp index */
    Assert(rel->rd_indexvalid != 2);

    return true;
}

void LocalTabDefCache::InsertRelationIntoGlobal(Relation rel, uint32 hash_value)
{
    /* not insert tmp rel or creating rel of session into global cache */
    if (!IsRelationStoreInglobal(rel)) {
        return;
    }
    /* when insert, we must make sure m_global_shared_tabdefcache is inited at least */
    Assert(m_global_tabdefcache != NULL || (rel->rd_rel->relisshared && m_global_shared_tabdefcache != NULL));
    /* upgrade mode never do insert */
    if (g_instance.global_sysdbcache.HashSearchSharedRelation(rel->rd_id)) {
        Assert(rel->rd_rel->relisshared);
        m_global_shared_tabdefcache->Insert(rel, hash_value);
    } else {
        Assert(!rel->rd_rel->relisshared);
        m_global_tabdefcache->Insert(rel, hash_value);
    }
}

void LocalTabDefCache::InsertRelationIntoLocal(Relation rel)
{
    uint32 hash_value = oid_hash((void *)&(rel->rd_id), sizeof(Oid));
    Index hash_index = HASH_INDEX(hash_value, (uint32)m_nbuckets);
    CreateLocalRelEntry(rel, hash_index);
}

void LocalTabDefCache::RemoveRelation(Relation rel)
{
    ForgetRelSonMemCxtSpace(rel);
    m_bucket_list.RemoveElemFromBucket(&rel->entry->cache_elem);
    pfree_ext(rel->entry);
}

Relation LocalTabDefCache::RemoveRelationByOid(Oid rel_oid, Index hash_index)
{
    LocalRelationEntry *entry = (LocalRelationEntry *)LocalBaseDefCache::SearchEntryFromLocal(rel_oid, hash_index);
    if (entry == NULL) {
        return NULL;
    }
    Relation old_rel = entry->rel;
    RemoveRelation(old_rel);
    return old_rel;
}

static void SpecialWorkOfRelationLocInfo(Relation rel)
{
    if (rel->rd_locator_info == NULL) {
        return;
    }
    Assert(IS_PGXC_COORDINATOR && rel->rd_id >= FirstNormalObjectId);
    /* global store nodeoid, we need convert it to nodeid */
    ListCell *lc;
    foreach (lc, rel->rd_locator_info->nodeList) {
        Oid datanode_oid = lfirst_oid(lc);
        int seqNum = PGXCNodeGetNodeId(datanode_oid, PGXC_NODE_DATANODE);
        if (likely(seqNum >= 0)) {
            lfirst_oid(lc) = seqNum;
        } else {
            /* convert failed, so free locator info and refrush global, and call RelationBuildLocator
             * pgxc_pool_reload may cause this, we loss some datanodes after pgxc_pool_reload.
             * actually we only need refrush rd_locator_info->nodeList. but we assumpt that
             * pgxc_pool_reload is a small probability event
             */
            t_thrd.lsc_cxt.lsc->tabdefcache.InvalidateGlobalRelationNodeList();
            FreeRelationLocInfo(rel->rd_locator_info);
            RelationBuildLocator(rel);
            return;
        }
    }

    /* code below can be packaged as a func */
    RelationLocInfo *rd_locator_info = rel->rd_locator_info;
    Assert(rd_locator_info->buckets_ptr == NULL);
    rd_locator_info->buckets_ptr = NULL;
    rd_locator_info->buckets_cnt = 0;
    if (!IsAutoVacuumWorkerProcess()) {
        InitBuckets(rd_locator_info, rel);
    }

    /*
     * If the locator type is round robin, we set a node to
     * use next time. In addition, if it is replicated,
     * we choose a node to use for balancing reads.
     */
    if (rd_locator_info->locatorType == LOCATOR_TYPE_RROBIN ||
        rd_locator_info->locatorType == LOCATOR_TYPE_REPLICATED) {
        int offset;
        /*
         * pick a random one to start with,
         * since each process will do this independently
         */
        offset = compute_modulo(abs(rand()), list_length(rd_locator_info->nodeList));

        srand(time(NULL));
        rd_locator_info->roundRobinNode = rd_locator_info->nodeList->head; /* initialize */
        for (int j = 0; j < offset && rd_locator_info->roundRobinNode->next != NULL; j++)
            rd_locator_info->roundRobinNode = rd_locator_info->roundRobinNode->next;
    }
}

static void SpecialWorkForLocalRel(Relation rel)
{
    if (RelationIsIndex(rel)) {
        rel->rd_aminfo = (RelationAmInfo *)MemoryContextAllocZero(rel->rd_indexcxt, sizeof(RelationAmInfo));
    }
    SetGttInfo(rel);

    if (unlikely(rel->rd_rel->relkind == RELKIND_MATVIEW) && !rel->rd_isscannable && !heap_is_matview_init_state(rel)) {
        /* matview may open smgr, whatever, we dont care */
        rel->rd_isscannable = true;
    }
    SpecialWorkOfRelationLocInfo(rel);
    Assert(rel->rd_mlogoid == InvalidOid ||
        rel->rd_mlogoid == find_matview_mlog_table(rel->rd_id) ||
        find_matview_mlog_table(rel->rd_id) == InvalidOid);
    RelationInitPhysicalAddr(rel);
    Assert(rel->rd_node.spcNode != InvalidOid);
    Assert(rel->rd_node.relNode != InvalidOid);
}

void LocalTabDefCache::CopyLocalRelation(Relation dest, Relation src)
{
    MemoryContext rules_cxt = NULL;
    if (src->rd_rules != NULL) {
        rules_cxt = AllocSetContextCreate(LocalMyDBCacheMemCxt(), RelationGetRelationName(src),
            ALLOCSET_SMALL_MINSIZE, ALLOCSET_SMALL_INITSIZE, ALLOCSET_SMALL_MAXSIZE);
    }
    MemoryContext rls_cxt = NULL;
    if (src->rd_rlsdesc != NULL) {
        rls_cxt = AllocSetContextCreate(LocalMyDBCacheMemCxt(), RelationGetRelationName(src),
            ALLOCSET_SMALL_MINSIZE, ALLOCSET_SMALL_INITSIZE, ALLOCSET_SMALL_MAXSIZE);
    }
    MemoryContext index_cxt = NULL;
    if (RelationIsIndex(src)) {
        index_cxt = AllocSetContextCreate(LocalMyDBCacheMemCxt(), RelationGetRelationName(src),
            ALLOCSET_SMALL_MINSIZE, ALLOCSET_SMALL_INITSIZE, ALLOCSET_SMALL_MAXSIZE);
    }

    CopyRelationData(dest, src, rules_cxt, rls_cxt, index_cxt);
    SpecialWorkForLocalRel(dest);
}

void LocalTabDefCache::Init()
{
    if (m_is_inited) {
        SetLocalRelCacheCriticalRelcachesBuilt(false);
        SetLocalRelCacheCriticalSharedRelcachesBuilt(false);
        return;
    }
    /* we dont know what dbid is now */

    needNewLocalCacheFile = false;
    criticalRelcachesBuilt = false;
    criticalSharedRelcachesBuilt = false;

    relcacheInvalsReceived = 0;
    initFileRelationIds = NIL;
    RelCacheNeedEOXActWork = false;

    g_bucketmap_cache = NIL;
    max_bucket_map_size = BUCKET_MAP_SIZE;

    EOXactTupleDescArray = NULL;
    NextEOXactTupleDescNum = 0;
    EOXactTupleDescArrayLen = 0;

    /*
     * relation mapper needs to be initialized too
     */
    RelationMapInitialize();
    m_is_inited = true;
}

void LocalTabDefCache::FormrDesc(const char *relationName, Oid relationReltype, bool is_shared, bool hasoids, int natts,
                                 const FormData_pg_attribute *attrs)
{
    if (SearchRelationFromLocal(attrs[0].attrelid) != NULL) {
        return;
    }
    if (SearchRelationFromGlobalCopy<true>(attrs[0].attrelid) != NULL) {
        return;
    }
    formrdesc(relationName, relationReltype, is_shared, hasoids, natts, attrs);
    /* fake-up rel, never insert into gsc */
}

void LocalTabDefCache::LoadCriticalIndex(Oid indexoid, Oid heapoid)
{
    if (SearchRelationFromLocal(indexoid) != NULL) {
        return;
    }
    if (SearchRelationFromGlobalCopy<true>(indexoid) != NULL) {
        return;
    }

    Relation ird = load_critical_index(indexoid, heapoid);
    if (IsRelOidStoreInGlobal(indexoid)) {
        /* system table never need a readwrite lock if not on upgrade mode */
        uint32 hash_value = oid_hash((void *)&indexoid, sizeof(Oid));
        InsertRelationIntoGlobal(ird, hash_value);
    }
}

void LocalTabDefCache::InitPhase2()
{
    if (m_is_inited_phase2) {
        return;
    }

    /* load shared relcache only */
    m_global_shared_tabdefcache = t_thrd.lsc_cxt.lsc->GetSharedTabDefCache();

    MemoryContext oldcxt;

    /*
     * relation mapper needs initialized too
     */
    RelationMapInitializePhase2();

    /*
     * In bootstrap mode, the shared catalog isn't there yet, so do nothing.
     */
    if (IsBootstrapProcessingMode()) {
        m_is_inited_phase2 = true;
        return;
    }

    /*
     * switch to cache memory context
     */
    oldcxt = MemoryContextSwitchTo(LocalMyDBCacheMemCxt());

    /*
     * Try to load the shared relcache cache file.	If unsuccessful, bootstrap
     * the cache with pre-made descriptors for the critical shared catalogs.
     */

    FormrDesc("pg_database", DatabaseRelation_Rowtype_Id, true, true, Natts_pg_database, Desc_pg_database);
    FormrDesc("pg_authid", AuthIdRelation_Rowtype_Id, true, true, Natts_pg_authid, Desc_pg_authid);
    FormrDesc("pg_auth_members", AuthMemRelation_Rowtype_Id, true, false, Natts_pg_auth_members, Desc_pg_auth_members);
    FormrDesc("pg_user_status", UserStatusRelation_Rowtype_Id, true, true, Natts_pg_user_status, Desc_pg_user_status);

#define NUM_CRITICAL_SHARED_RELS 4 /* fix if you change list above */
    (void)MemoryContextSwitchTo(oldcxt);
    m_is_inited_phase2 = true;
}

static bool FlushInitRelation(Relation rel)
{
    bool restart = false;
    /*
     * If it's a faked-up entry, read the real pg_class tuple.
     */
    if (rel->rd_rel->relowner == InvalidOid) {
        RelationCacheInvalidOid(rel);

        /* relowner had better be OK now, else we'll loop forever */
        if (rel->rd_rel->relowner == InvalidOid)
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("invalid relowner in pg_class entry for \"%s\"", RelationGetRelationName(rel))));
        restart = true;
    }

    /*
        * Fix data that isn't saved in relcache cache file.
        *
        * relhasrules or relhastriggers could possibly be wrong or out of
        * date.  If we don't actually find any rules or triggers, clear the
        * local copy of the flag so that we don't get into an infinite loop
        * here.  We don't make any attempt to fix the pg_class entry, though.
        */
    if (rel->rd_rel->relhasrules && rel->rd_rules == NULL) {
        RelationBuildRuleLock(rel);
        if (rel->rd_rules == NULL)
            rel->rd_rel->relhasrules = false;
        restart = true;
    }
    if (rel->rd_rel->relhastriggers && rel->trigdesc == NULL) {
        RelationBuildTriggers(rel);
        if (rel->trigdesc == NULL)
            rel->rd_rel->relhastriggers = false;
        restart = true;
    }

    /* get row level security policies for this rel */
    if (RelationEnableRowSecurity(rel) && rel->rd_rlsdesc == NULL) {
        RelationBuildRlsPolicies(rel);
        Assert(rel->rd_rlsdesc != NULL);
        restart = true;
    }
    return restart;
}

void LocalTabDefCache::InitPhase3(void)
{
    if (m_is_inited_phase3) {
        Assert(CheckMyDatabaseMatch());
        SetLocalRelCacheCriticalRelcachesBuilt(true);
        SetLocalRelCacheCriticalSharedRelcachesBuilt(true);
        return;
    }
    m_global_tabdefcache = t_thrd.lsc_cxt.lsc->GetGlobalTabDefCache();
    m_db_id = t_thrd.lsc_cxt.lsc->my_database_id;
    m_pgclassdesc = NULL;
    m_pgindexdesc = NULL;
    MemoryContext oldcxt;
    /*
     * relation mapper needs initialized too
     */
    RelationMapInitializePhase3();

    /*
     * switch to cache memory context
     */
    oldcxt = MemoryContextSwitchTo(LocalMyDBCacheMemCxt());
    /*
     * Try to load the local relcache cache file.  If unsuccessful, bootstrap
     * the cache with pre-made descriptors for the critical "nailed-in" system
     * catalogs.
     *
     * Vacuum-full pg_class will move entries of indexes on pg_class to the end
     * of pg_class. When bootstraping critical catcaches and relcached from scratch,
     * we have to do sequential scan for these entries. For databases with millions
     * of pg_class entries, this could take quite a long time. To make matters worse,
     * when multiple backends parallelly do this, they may block one another severly
     * due to lock on the same share buffer partition. To avoid such case, we only allow
     * one backend to bootstrap critical catcaches at one time. When it is done, the other
     * parallel backends would first try to load from init file once again.
     * Since share catalogs usually have much less entries, we only do so for local catalogs
     * at present.
     */
    FormrDesc("pg_class", RelationRelation_Rowtype_Id, false, true, Natts_pg_class, Desc_pg_class);
    FormrDesc("pg_attribute", AttributeRelation_Rowtype_Id, false, false, Natts_pg_attribute, Desc_pg_attribute);
    FormrDesc("pg_proc", ProcedureRelation_Rowtype_Id, false, true, Natts_pg_proc, Desc_pg_proc);
    FormrDesc("pg_type", TypeRelation_Rowtype_Id, false, true, Natts_pg_type, Desc_pg_type);
    (void)MemoryContextSwitchTo(oldcxt);
#define NUM_CRITICAL_LOCAL_RELS 4 /* fix if you change list above */
    if (IsBootstrapProcessingMode()) {
        /* In bootstrap mode, the faked-up formrdesc info is all we'll have */
        m_is_inited_phase3 = true;
        return;
    }

    /*
     * If we didn't get the critical system indexes loaded into relcache, do
     * so now.	These are critical because the catcache and/or opclass cache
     * depend on them for fetches done during relcache load.  Thus, we have an
     * infinite-recursion problem.	We can break the recursion by doing
     * heapscans instead of indexscans at certain key spots. To avoid hobbling
     * performance, we only want to do that until we have the critical indexes
     * loaded into relcache.  Thus, the flag u_sess->relcache_cxt.criticalRelcachesBuilt is used to
     * decide whether to do heapscan or indexscan at the key spots, and we set
     * it true after we've loaded the critical indexes.
     *
     * The critical indexes are marked as "nailed in cache", partly to make it
     * easy for load_relcache_init_file to count them, but mainly because we
     * cannot flush and rebuild them once we've set u_sess->relcache_cxt.criticalRelcachesBuilt to
     * true.  (NOTE: perhaps it would be possible to reload them by
     * temporarily setting u_sess->relcache_cxt.criticalRelcachesBuilt to false again.  For now,
     * though, we just nail 'em in.)
     *
     * RewriteRelRulenameIndexId and TriggerRelidNameIndexId are not critical
     * in the same way as the others, because the critical catalogs don't
     * (currently) have any rules or triggers, and so these indexes can be
     * rebuilt without inducing recursion.	However they are used during
     * relcache load when a rel does have rules or triggers, so we choose to
     * nail them for performance reasons.
     */
    /* we dont load from file */
    Assert(!LocalRelCacheCriticalRelcachesBuilt());
    LoadCriticalIndex(ClassOidIndexId, RelationRelationId);
    LoadCriticalIndex(AttributeRelidNumIndexId, AttributeRelationId);
    LoadCriticalIndex(IndexRelidIndexId, IndexRelationId);
    LoadCriticalIndex(OpclassOidIndexId, OperatorClassRelationId);
    LoadCriticalIndex(AccessMethodProcedureIndexId, AccessMethodProcedureRelationId);
    LoadCriticalIndex(RewriteRelRulenameIndexId, RewriteRelationId);
    LoadCriticalIndex(TriggerRelidNameIndexId, TriggerRelationId);
#define NUM_CRITICAL_LOCAL_INDEXES 7 /* fix if you change list above */
    SetLocalRelCacheCriticalRelcachesBuilt(true);

    /*
     * Process critical shared indexes too.
     *
     * DatabaseNameIndexId isn't critical for relcache loading, but rather for
     * initial lookup of u_sess->proc_cxt.MyDatabaseId, without which we'll never find any
     * non-shared catalogs at all.	Autovacuum calls InitPostgres with a
     * database OID, so it instead depends on DatabaseOidIndexId.  We also
     * need to nail up some indexes on pg_authid and pg_auth_members for use
     * during client authentication.
     */
    Assert(!LocalRelCacheCriticalSharedRelcachesBuilt());
    LoadCriticalIndex(DatabaseNameIndexId, DatabaseRelationId);
    LoadCriticalIndex(DatabaseOidIndexId, DatabaseRelationId);
    LoadCriticalIndex(AuthIdRolnameIndexId, AuthIdRelationId);
    LoadCriticalIndex(AuthIdOidIndexId, AuthIdRelationId);
    LoadCriticalIndex(AuthMemMemRoleIndexId, AuthMemRelationId);
    LoadCriticalIndex(UserStatusRoleidIndexId, UserStatusRelationId);
#define NUM_CRITICAL_SHARED_INDEXES 6 /* fix if you change list above */
    SetLocalRelCacheCriticalSharedRelcachesBuilt(true);

    /*
     * Now, scan all the relcache entries and update anything that might be
     * wrong in the results from formrdesc or the relcache cache file. If we
     * faked up relcache entries using formrdesc, then read the real pg_class
     * rows and replace the fake entries with them. Also, if any of the
     * relcache entries have rules or triggers, load that info the hard way
     * since it isn't recorded in the cache file.
     *
     * Whenever we access the catalogs to read data, there is a possibility of
     * a shared-inval cache flush causing relcache entries to be removed.
     * Since hash_seq_search only guarantees to still work after the *current*
     * entry is removed, it's unsafe to continue the hashtable scan afterward.
     * We handle this by restarting the scan from scratch after each access.
     * This is theoretically O(N^2), but the number of entries that actually
     * need to be fixed is small enough that it doesn't matter.
     */
    Dlelem *bucket_elt;
    forloopactivebucketlist(bucket_elt, m_bucket_list.GetActiveBucketList()) {
        Dlelem *elt;
        forloopbucket(elt, bucket_elt) {
            LocalRelationEntry *entry = (LocalRelationEntry *)DLE_VAL(elt);
            Assert(entry->rel->entry == entry);
            elt = DLGetSucc(elt);
            Relation rel = entry->rel;
            /*
             * Make sure *this* entry doesn't get flushed while we work with it.
             */
            RelationIncrementReferenceCount(rel);
            bool restart = FlushInitRelation(rel);
            /* Release hold on the rel */
            RelationDecrementReferenceCount(rel);

            /* Now, restart the hashtable scan if needed */
            if (restart) {
                if (IsRelOidStoreInGlobal(rel->rd_id)) {
                    /* system table never need a readwrite lock if not on upgrade mode */
                    uint32 hash_value = oid_hash((void *)&(rel->rd_id), sizeof(Oid));
                    InsertRelationIntoGlobal(rel, hash_value);
                }
                bucket_elt = DLGetHead(m_bucket_list.GetActiveBucketList());
                elt = NULL;
            }
        }
    }
    m_is_inited_phase3 = true;
#ifdef MEMORY_CONTEXT_CHECKING
    MemoryContextCheck(LocalMyDBCacheMemCxt(), false);
#endif
}

void LocalTabDefCache::InvalidateGlobalRelation(Oid db_id, Oid rel_oid, bool is_commit)
{
    if (unlikely(db_id == InvalidOid && rel_oid == InvalidOid)) {
        /* This is used by alter publication as changes in publications may affect
         * large number of tables. see function CacheInvalidateRelcacheAll */
        g_instance.global_sysdbcache.InvalidAllRelations();
        return;
    }
    if (!is_commit) {
        invalid_entries.InsertInvalidDefValue(rel_oid);
        return;
    }
    if (db_id == InvalidOid) {
        t_thrd.lsc_cxt.lsc->GetSharedTabDefCache()->Invalidate(db_id, rel_oid);
    } else if (m_global_tabdefcache == NULL) {
        Assert(!m_is_inited_phase3);
        Assert(CheckMyDatabaseMatch());
        GlobalSysDBCacheEntry *entry = g_instance.global_sysdbcache.FindTempGSCEntry(db_id);
        if (entry == NULL) {
            return;
        }
        entry->m_tabdefCache->Invalidate(db_id, rel_oid);
        g_instance.global_sysdbcache.ReleaseTempGSCEntry(entry);
    } else {
        Assert(CheckMyDatabaseMatch());
        Assert(m_db_id == t_thrd.lsc_cxt.lsc->my_database_id);
        Assert(m_db_id == db_id);
        m_global_tabdefcache->Invalidate(db_id, rel_oid);
    }
}

void LocalTabDefCache::InvalidateRelationAll()
{
    /*
     * Reload relation mapping data before starting to reconstruct cache.
     */
    RelationMapInvalidateAll();

    List *rebuildFirstList = NIL;
    List *rebuildList = NIL;
    Dlelem *bucket_elt;
    forloopactivebucketlist(bucket_elt, m_bucket_list.GetActiveBucketList()) {
        Dlelem *elt;
        forloopbucket(elt, bucket_elt) {
            LocalRelationEntry *entry = (LocalRelationEntry *)DLE_VAL(elt);
            Assert(entry->rel->entry == entry);
            elt = DLGetSucc(elt);
            Relation rel = entry->rel;
            /* Must close all smgr references to avoid leaving dangling ptrs */
            RelationCloseSmgr(rel);

            /* Ignore new rels, since they are never cross-backend targets */
            if (rel->rd_createSubid != InvalidSubTransactionId) {
                continue;
            }
            AddLocalRelCacheInvalsReceived(1);

            if (RelationHasReferenceCountZero(rel)) {
                /* Delete this entry immediately */
                Assert(!rel->rd_isnailed);
                RelationClearRelation(rel, false);
            } else {
                /*
                 * If it's a mapped rel, immediately update its rd_node in
                 * case its relfilenode changed.  We must do this during phase 1
                 * in case the rel is consulted during rebuild of other
                 * relcache entries in phase 2.  It's safe since consulting the
                 * map doesn't involve any access to relcache entries.
                 */
                if (RelationIsMapped(rel))
                    RelationInitPhysicalAddr(rel);

                /*
                 * Add this entry to list of stuff to rebuild in second pass.
                 * pg_class goes to the front of rebuildFirstList while
                 * pg_class_oid_index goes to the back of rebuildFirstList, so
                 * they are done first and second respectively.  Other nailed
                 * rels go to the front of rebuildList, so they'll be done
                 * next in no particular order; and everything else goes to the
                 * back of rebuildList.
                 */
                if (RelationGetRelid(rel) == RelationRelationId)
                    rebuildFirstList = lcons(rel, rebuildFirstList);
                else if (RelationGetRelid(rel) == ClassOidIndexId)
                    rebuildFirstList = lappend(rebuildFirstList, rel);
                else if (rel->rd_isnailed)
                    rebuildList = lcons(rel, rebuildList);
                else
                    rebuildList = lappend(rebuildList, rel);
            }
            /* RelationClearRelation call RelationCacheDelete to remove rel not rebuilt
             * RelationClearRelation needn't insert into global, global already updated */
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
    foreach (l, rebuildFirstList) {
        Relation rel = (Relation)lfirst(l);
        /* RelationClearRelation call RelationBuildDesc to rebuild the rel,
         * and RelationBuildDesc call SEARCH_RELATION_FROM_GLOBAL to search first */
        RelationClearRelation(rel, true);
    }
    list_free_ext(rebuildFirstList);
    foreach (l, rebuildList) {
        Relation rel = (Relation)lfirst(l);
        RelationClearRelation(rel, true);
    }
    list_free_ext(rebuildList);
}

void LocalTabDefCache::InvalidateRelationNodeList()
{
    Dlelem *bucket_elt;
    forloopactivebucketlist(bucket_elt, m_bucket_list.GetActiveBucketList()) {
        Dlelem *elt;
        forloopbucket(elt, bucket_elt) {
            LocalRelationEntry *entry = (LocalRelationEntry *)DLE_VAL(elt);
            Assert(entry->rel->entry == entry);
            elt = DLGetSucc(elt);
            Relation rel = entry->rel;
            if (rel->rd_locator_info != NULL) {
                RelationClearRelation(rel, !RelationHasReferenceCountZero(rel));
            }
        }
    }
}

void LocalTabDefCache::InvalidateRelationBucketsAll()
{
    Dlelem *bucket_elt;
    forloopactivebucketlist(bucket_elt, m_bucket_list.GetActiveBucketList()) {
        Dlelem *elt;
        forloopbucket(elt, bucket_elt) {
            LocalRelationEntry *entry = (LocalRelationEntry *)DLE_VAL(elt);
            Assert(entry->rel->entry == entry);
            elt = DLGetSucc(elt);
            Relation rel = entry->rel;
            if (rel->rd_locator_info != NULL) {
                InvalidateBuckets(rel->rd_locator_info);
            }
        }
    }
}

void LocalTabDefCache::RememberToFreeTupleDescAtEOX(TupleDesc td)
{
    if (EOXactTupleDescArray == NULL) {
        MemoryContext oldcxt = MemoryContextSwitchTo(LocalMyDBCacheMemCxt());
        const int default_len = 16;
        EOXactTupleDescArray = (TupleDesc *)palloc(default_len * sizeof(TupleDesc));
        EOXactTupleDescArrayLen = default_len;
        NextEOXactTupleDescNum = 0;
        MemoryContextSwitchTo(oldcxt);
    } else if (NextEOXactTupleDescNum >= EOXactTupleDescArrayLen) {
        int32 newlen = EOXactTupleDescArrayLen * 2;
        Assert(EOXactTupleDescArrayLen > 0);
        EOXactTupleDescArray = (TupleDesc *)repalloc(EOXactTupleDescArray, newlen * sizeof(TupleDesc));
        EOXactTupleDescArrayLen = newlen;
    }
    EOXactTupleDescArray[NextEOXactTupleDescNum++] = td;
}

/* Free all tupleDescs remembered in RememberToFreeTupleDescAtEOX in a batch when a transaction ends */
void LocalTabDefCache::AtEOXact_FreeTupleDesc()
{
    if (EOXactTupleDescArrayLen > 0) {
        Assert(EOXactTupleDescArray != NULL);
        for (int i = 0; i < NextEOXactTupleDescNum; i++) {
            Assert(EOXactTupleDescArray[i]->tdrefcount == 0);
            FreeTupleDesc(EOXactTupleDescArray[i]);
        }
        pfree_ext(EOXactTupleDescArray);
    }
    NextEOXactTupleDescNum = 0;
    EOXactTupleDescArrayLen = 0;
}

/*
 * AtEOXact_RelationCache
 *
 *	Clean up the relcache at main-transaction commit or abort.
 *
 * Note: this must be called *before* processing invalidation messages.
 * In the case of abort, we don't want to try to rebuild any invalidated
 * cache entries (since we can't safely do database accesses).  Therefore
 * we must reset refcnts before handling pending invalidations.
 *
 *
 */
void LocalTabDefCache::AtEOXact_RelationCache(bool isCommit)
{
    invalid_entries.ResetInitFlag();
#ifdef MEMORY_CONTEXT_CHECKING
    MemoryContextCheck(LocalMyDBCacheMemCxt(), false);
#endif
    /*
     * To speed up transaction exit, we want to avoid scanning the relcache
     * unless there is actually something for this routine to do.  Other than
     * the debug-only Assert checks, most transactions don't create any work
     * for us to do here, so we keep a static flag that gets set if there is
     * anything to do.	(Currently, this means either a relation is created in
     * the current xact, or one is given a new relfilenode, or an index list
     * is forced.)	For simplicity, the flag remains set till end of top-level
     * transaction, even though we could clear it at subtransaction end in
     * some cases.
     */
    if (!GetRelCacheNeedEOXActWork()
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
            LocalRelationEntry *entry = (LocalRelationEntry *)DLE_VAL(elt);
            elt = DLGetSucc(elt);
            Relation rel = entry->rel;

            /*
             * The relcache entry's ref count should be back to its normal
             * not-in-a-transaction state: 0 unless it's nailed in cache.
             *
             * In bootstrap mode, this is NOT true, so don't check it --- the
             * bootstrap code expects rels to stay open across start/commit
             * transaction calls.  (That seems bogus, but it's not worth fixing.)
             */
            if (!IsBootstrapProcessingMode()) {
                int expected_refcnt;

                expected_refcnt = rel->rd_isnailed ? 1 : 0;
                if (rel->rd_refcnt != expected_refcnt && IsolatedResourceOwner != NULL) {
                    elog(WARNING, "relation \"%s\" rd_refcnt is %d but expected_refcnt %d. ",
                         RelationGetRelationName(rel), rel->rd_refcnt, expected_refcnt);
                    PrintResourceOwnerLeakWarning();
                }
#ifdef USE_ASSERT_CHECKING
                Assert(rel->rd_refcnt == expected_refcnt);
#endif
            }

            /*
             * Is it a rel created in the current transaction?
             *
             * During commit, reset the flag to zero, since we are now out of the
             * creating transaction.  During abort, simply delete the relcache
             * entry --- it isn't interesting any longer.  (NOTE: if we have
             * forgotten the new-ness of a new rel due to a forced cache
             * flush, the entry will get deleted anyway by shared-cache-inval
             * processing of the aborted pg_class insertion.)
             */
            if (rel->rd_createSubid != InvalidSubTransactionId) {
                if (isCommit)
                    rel->rd_createSubid = InvalidSubTransactionId;
                else if (RelationHasReferenceCountZero(rel)) {
                    RelationClearRelation(rel, false);
                    continue;
                } else {
                    /*
                     * Hmm, somewhere there's a (leaked?) reference to the rel.
                     * We daren't remove the entry for fear of dereferencing a
                     * dangling pointer later.  Bleat, and mark it as not belonging to
                     * the current transaction.  Hopefully it'll get cleaned up
                     * eventually.  This must be just a WARNING to avoid
                     * error-during-error-recovery loops.
                     */
                    rel->rd_createSubid = InvalidSubTransactionId;
                    ereport(WARNING, (errmsg("cannot remove relcache entry for \"%s\" because it has nonzero refcount",
                                             RelationGetRelationName(rel))));
                }
            }

            /*
             * Likewise, reset the hint about the relfilenode being new.
             */
            rel->rd_newRelfilenodeSubid = InvalidSubTransactionId;

            /*
             * Flush any temporary index list.
             */
            if (rel->rd_indexvalid == 2) {
                list_free_ext(rel->rd_indexlist);
                rel->rd_indexlist = NIL;
                rel->rd_oidindex = InvalidOid;
                rel->rd_indexvalid = 0;
            }
            if (rel->partMap != NULL && unlikely(rel->partMap->isDirty)) {
                RelationClearRelation(rel, false);
            }
        }
    }
    /* Once done with the transaction, we can reset u_sess->relcache_cxt.need_eoxact_work */
    SetRelCacheNeedEOXActWork(false);
}

/*
 * AtEOSubXact_RelationCache
 *
 *	Clean up the relcache at sub-transaction commit or abort.
 *
 * Note: this must be called *before* processing invalidation messages.
 */
void LocalTabDefCache::AtEOSubXact_RelationCache(bool isCommit, SubTransactionId mySubid, SubTransactionId parentSubid)
{
    /* dont update global info here, even we commit this relation create operator */
    /*
     * Skip the relcache scan if nothing to do --- see notes for
     * AtEOXact_RelationCache.
     */
    if (!GetRelCacheNeedEOXActWork())
        return;

    Dlelem *bucket_elt;
    forloopactivebucketlist(bucket_elt, m_bucket_list.GetActiveBucketList()) {
        Dlelem *elt;
        forloopbucket(elt, bucket_elt) {
            LocalRelationEntry *entry = (LocalRelationEntry *)DLE_VAL(elt);
            elt = DLGetSucc(elt);
            Relation rel = entry->rel;

            /*
             * Is it a rel created in the current subtransaction?
             *
             * During subcommit, mark it as belonging to the parent, instead.
             * During subabort, simply delete the relcache entry.
             */
            if (rel->rd_createSubid == mySubid) {
                if (isCommit)
                    rel->rd_createSubid = parentSubid;
                else if (RelationHasReferenceCountZero(rel)) {
                    RelationClearRelation(rel, false);
                    continue;
                } else {
                    /*
                     * Hmm, somewhere there's a (leaked?) reference to the rel.
                     * We daren't remove the entry for fear of dereferencing a
                     * dangling pointer later.  Bleat, and transfer it to the parent
                     * subtransaction so we can try again later.  This must be just a
                     * WARNING to avoid error-during-error-recovery loops.
                     */
                    rel->rd_createSubid = parentSubid;
                    ereport(WARNING, (errmsg("cannot remove relcache entry for \"%s\" because it has nonzero refcount",
                                             RelationGetRelationName(rel))));
                }
            }

            /*
             * Likewise, update or drop any new-relfilenode-in-subtransaction
             * hint.
             */
            if (rel->rd_newRelfilenodeSubid == mySubid) {
                if (isCommit)
                    rel->rd_newRelfilenodeSubid = parentSubid;
                else
                    rel->rd_newRelfilenodeSubid = InvalidSubTransactionId;
            }

            /*
             * Flush any temporary index list.
             */
            if (rel->rd_indexvalid == 2) {
                list_free_ext(rel->rd_indexlist);
                rel->rd_indexlist = NIL;
                rel->rd_oidindex = InvalidOid;
                rel->rd_indexvalid = 0;
            }
        }
    }
}

Relation LocalTabDefCache::RelationIdGetRelation(Oid rel_oid)
{
    Assert(CheckMyDatabaseMatch());
    Relation rd = SearchRelation(rel_oid);
    if (RelationIsValid(rd)) {
        RelationIncrementReferenceCount(rd);
        /* revalidate cache entry if necessary */
        if (!rd->rd_isvalid) {
            /*
             * Indexes only have a limited number of possible schema changes,
             * and we don't want to use the full-blown procedure because it's
             * a headache for indexes that reload itself depends on.
             */
            if (RelationIsIndex(rd)) {
                RelationReloadIndexInfo(rd);
            } else {
                RelationClearRelation(rd, true);
            }
        }

        /*
         * In some cases, after the relcache is built, the temp table's node group is dropped
         * because of cluster resizeing, so we should do checking when get the rel directly from
         * relcache.
         */
        if (rd->rd_rel->relpersistence == RELPERSISTENCE_TEMP)
            (void)checkGroup(rel_oid, RELATION_IS_OTHER_TEMP(rd));

        return rd;
    }

    /*
     * no reldesc in the cache, so have RelationBuildDesc() build one and add
     * it.
     */
    Assert(m_is_inited_phase2);
    bool is_oid_store_in_global = IsRelOidStoreInGlobal(rel_oid);
    /* sys table dont need a rwlock */
    Assert(m_global_tabdefcache != NULL || g_instance.global_sysdbcache.HashSearchSharedRelation(rel_oid));
    bool has_concurrent_lock = is_oid_store_in_global &&
        !IsSystemObjOid(rel_oid);
    uint32 hash_value = oid_hash((void *)&(rel_oid), sizeof(Oid));
    pthread_rwlock_t *oid_lock = NULL;
    if (has_concurrent_lock) {
        oid_lock = m_global_tabdefcache->GetHashValueLock(hash_value);
        AcquireGSCTableReadLock(&has_concurrent_lock, oid_lock);
    }
    rd = RelationBuildDesc(rel_oid, true, true);
    /* system table dont insert into global only when upgrading, who never be modified except on upgrade mode */
    if (RelationIsValid(rd) && is_oid_store_in_global && (IsSystemObjOid(rel_oid) || has_concurrent_lock)) {
        InsertRelationIntoGlobal(rd, hash_value);
    }
    if (has_concurrent_lock) {
        ReleaseGSCTableReadLock(&has_concurrent_lock, oid_lock);
    }

    if (RelationIsValid(rd)) {
        RelationIncrementReferenceCount(rd);
        /* Insert TDE key to buffer cache for tde table */
        if (g_instance.attr.attr_security.enable_tde && IS_PGXC_DATANODE && RelationisEncryptEnable(rd)) {
            RelationInsertTdeInfoToCache(rd);
        }
    }

    return rd;
}

void LocalTabDefCache::ResetInitFlag()
{
    m_bucket_list.ResetContent();
    invalid_entries.ResetInitFlag();
    needNewLocalCacheFile = false;
    criticalRelcachesBuilt = false;
    criticalSharedRelcachesBuilt = false;

    relcacheInvalsReceived = 0;
    initFileRelationIds = NIL;
    RelCacheNeedEOXActWork = false;

    g_bucketmap_cache = NIL;
    max_bucket_map_size = 0;

    EOXactTupleDescArray = NULL;
    NextEOXactTupleDescNum = 0;
    EOXactTupleDescArrayLen = 0;

    m_global_tabdefcache = NULL;
    m_pgclassdesc = NULL;
    m_pgindexdesc = NULL;
    m_global_shared_tabdefcache = NULL;

    m_is_inited = false;
    m_is_inited_phase2 = false;
    m_is_inited_phase3 = false;

    m_db_id = InvalidOid;
}
