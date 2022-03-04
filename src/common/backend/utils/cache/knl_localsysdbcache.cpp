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

#include "utils/knl_localsysdbcache.h"
#include "utils/knl_globalsysdbcache.h"
#include "utils/resowner.h"
#include "knl/knl_instance.h"
#include "executor/executor.h"
#include "utils/postinit.h"
#include "storage/sinvaladt.h"
#include "utils/hashutils.h"
#include "utils/knl_relcache.h"
#include "utils/acl.h"
#include "utils/spccache.h"
#include "commands/dbcommands.h"
#include "tsearch/ts_cache.h"
#include "optimizer/predtest.h"
#include "utils/attoptcache.h"
#include "parser/parse_oper.h"
#include "utils/typcache.h"
#include "utils/relfilenodemap.h"
#include "postmaster/bgworker.h"
#include "storage/lmgr.h"
#ifdef USE_ASSERT_CHECKING
class LSCCloseCheck {
public:
    LSCCloseCheck()
    {
        m_lsc_closed = true;
    }
    ~LSCCloseCheck()
    {
        Assert(!EnableGlobalSysCache() || m_lsc_closed || g_instance.distribute_test_param_instance->elevel == PANIC);
    }
    void setCloseFlag(bool value)
    {
        m_lsc_closed = value;
    }
private:
    bool m_lsc_closed;
};
thread_local LSCCloseCheck lsc_close_check = LSCCloseCheck();
#endif

void ReLoadLSCWhenWaitMission()
{
    if (!EnableLocalSysCache()) {
        return;
    }
    LocalSysDBCache *lsc = t_thrd.lsc_cxt.lsc;
    if (unlikely(lsc == NULL)) {
        return;
    }
    if (lsc->GetMyGlobalDBEntry() != NULL && lsc->GetMyGlobalDBEntry()->m_isDead) {
        t_thrd.proc_cxt.PostInit->InitLoadLocalSysCache(u_sess->proc_cxt.MyDatabaseId,
            u_sess->proc_cxt.MyProcPort->database_name);
    }
    g_instance.global_sysdbcache.GSCMemThresholdCheck();
}

void RememberRelSonMemCxtSpace(Relation rel)
{
    if (rel->rd_rulescxt != NULL) {
        t_thrd.lsc_cxt.lsc->rel_index_rule_space += ((AllocSet)rel->rd_rulescxt)->totalSpace;
    }
    if (rel->rd_indexcxt != NULL) {
        t_thrd.lsc_cxt.lsc->rel_index_rule_space += ((AllocSet)rel->rd_indexcxt)->totalSpace;
    }
}
void ForgetRelSonMemCxtSpace(Relation rel)
{
    if (rel->rd_rulescxt != NULL) {
        t_thrd.lsc_cxt.lsc->rel_index_rule_space -= ((AllocSet)rel->rd_rulescxt)->totalSpace;
    }
    if (rel->rd_indexcxt != NULL) {
        t_thrd.lsc_cxt.lsc->rel_index_rule_space -= ((AllocSet)rel->rd_indexcxt)->totalSpace;
    }
    if (t_thrd.lsc_cxt.lsc->rel_index_rule_space < 0) {
        t_thrd.lsc_cxt.lsc->rel_index_rule_space = 0;
    }
}

/* call all access dbid after initsession */
bool CheckMyDatabaseMatch()
{
    if (EnableLocalSysCache()) {
        return u_sess->proc_cxt.MyDatabaseId == InvalidOid ||
            u_sess->proc_cxt.MyDatabaseId == t_thrd.lsc_cxt.lsc->my_database_id;
    } else {
        return true;
    }
}
char *GetMyDatabasePath()
{
    if (EnableLocalSysCache()) {
        return t_thrd.lsc_cxt.lsc->my_database_path;
    } else {
        return u_sess->proc_cxt.DatabasePath;
    }
}
Oid GetMyDatabaseId()
{
    if (EnableLocalSysCache()) {
        return t_thrd.lsc_cxt.lsc->my_database_id;
    } else {
        return u_sess->proc_cxt.MyDatabaseId;
    }
}
Oid GetMyDatabaseTableSpace()
{
    if (EnableLocalSysCache()) {
        return t_thrd.lsc_cxt.lsc->my_database_tablespace;
    } else {
        return u_sess->proc_cxt.MyDatabaseTableSpace;
    }
}

bool IsGotPoolReload()
{
    if (EnableLocalSysCache()) {
        return u_sess->sig_cxt.got_pool_reload || t_thrd.lsc_cxt.lsc->got_pool_reload;
    } else {
        return u_sess->sig_cxt.got_pool_reload;
    }
}
void ResetGotPoolReload(bool value)
{
    if (EnableLocalSysCache()) {
        u_sess->sig_cxt.got_pool_reload = value;
        t_thrd.lsc_cxt.lsc->got_pool_reload = value;
    } else {
        u_sess->sig_cxt.got_pool_reload = value;
    }
}

bool DeepthInAcceptInvalidationMessageNotZero()
{
    if (EnableLocalSysCache()) {
        return u_sess->inval_cxt.DeepthInAcceptInvalidationMessage > 0 ||
            t_thrd.lsc_cxt.lsc->inval_cxt.DeepthInAcceptInvalidationMessage > 0;
    } else {
        return u_sess->inval_cxt.DeepthInAcceptInvalidationMessage > 0;
    }
}
void ResetDeepthInAcceptInvalidationMessage(int value)
{
    if (EnableLocalSysCache()) {
        u_sess->inval_cxt.DeepthInAcceptInvalidationMessage = value;
        t_thrd.lsc_cxt.lsc->inval_cxt.DeepthInAcceptInvalidationMessage = value;
    } else {
        u_sess->inval_cxt.DeepthInAcceptInvalidationMessage = value;
    }
}

static bool SwitchToSessionSysCache()
{
    if (
#ifdef ENABLE_MULTIPLE_NODES
        /* ts code dont use gsc */
        t_thrd.role != TS_COMPACTION &&
        t_thrd.role != TS_COMPACTION_CONSUMER &&
        t_thrd.role != TS_COMPACTION_AUXILIAY
#else
        true
#endif
        ) {
        return false;
    }
    return true;
}

/* after call close, you should never use syscache before rebuild it */
void CloseLocalSysDBCache()
{
    if (!EnableLocalSysCache()) {
        closeAllVfds();
    }
    if (t_thrd.lsc_cxt.lsc == NULL) {
        return;
    }
    t_thrd.lsc_cxt.lsc->CloseLocalSysDBCache();
}

static inline HeapTuple GetTupleFromLscCatList(CatCList *cl, int index)
{
    Assert(EnableLocalSysCache());
    return &(((GlobalCatCTup **)cl->systups)[index]->tuple);
}

static inline HeapTuple GetTupleFromSessCatList(CatCList *cl, int index)
{
    Assert(!EnableLocalSysCache());
    return &(cl->systups[index]->tuple);
}

void CreateLocalSysDBCache()
{
    /* every thread should call this func once */
    if (!EnableGlobalSysCache()) {
        t_thrd.lsc_cxt.enable_lsc = false;
        t_thrd.lsc_cxt.FetchTupleFromCatCList = GetTupleFromSessCatList;
        return;
    }

    Assert(t_thrd.lsc_cxt.lsc == NULL);
    t_thrd.lsc_cxt.lsc = New(THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_DEFAULT))LocalSysDBCache();
    /* use this object to invalid gsc, only work with timeseries worker */
    t_thrd.lsc_cxt.lsc->CreateDBObject();
    t_thrd.lsc_cxt.FetchTupleFromCatCList = GetTupleFromLscCatList;
    t_thrd.lsc_cxt.lsc->recovery_finished = g_instance.global_sysdbcache.recovery_finished;
    t_thrd.lsc_cxt.enable_lsc = !SwitchToSessionSysCache();
    if (!t_thrd.lsc_cxt.enable_lsc) {
        t_thrd.lsc_cxt.FetchTupleFromCatCList = GetTupleFromSessCatList;
        t_thrd.lsc_cxt.lsc->is_closed = true;
#ifdef USE_ASSERT_CHECKING
        lsc_close_check.setCloseFlag(true);
#endif
    } else {
#ifdef USE_ASSERT_CHECKING
        lsc_close_check.setCloseFlag(false);
#endif
    }
}
static void ReleaseBadPtrList(bool isCommit);
static void ThreadNodeGroupCallback(Datum arg, int cacheid, uint32 hashvalue)
{
    RelationCacheInvalidateBuckets();
}

bool EnableGlobalSysCache()
{
    return g_instance.attr.attr_common.enable_global_syscache;
}

MemoryContext LocalSharedCacheMemCxt()
{
    if (EnableLocalSysCache()) {
        return t_thrd.lsc_cxt.lsc->lsc_share_memcxt;
    } else {
        return u_sess->cache_mem_cxt;
    }
}

MemoryContext LocalMyDBCacheMemCxt()
{
    if (EnableLocalSysCache()) {
        return t_thrd.lsc_cxt.lsc->lsc_mydb_memcxt;
    } else {
        return u_sess->cache_mem_cxt;
    }
}

extern MemoryContext LocalGBucketMapMemCxt()
{
    if (EnableLocalSysCache()) {
        return t_thrd.lsc_cxt.lsc->lsc_mydb_memcxt;
    } else {
        return SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR);
    }
}

MemoryContext LocalSmgrStorageMemoryCxt()
{
    if (EnableLocalSysCache()) {
        return t_thrd.lsc_cxt.lsc->lsc_mydb_memcxt;
    } else {
        return SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE);
    }
}

struct HTAB *GetTypeCacheHash()
{
    if (EnableLocalSysCache()) {
        return t_thrd.lsc_cxt.lsc->TypeCacheHash;
    } else {
        return u_sess->tycache_cxt.TypeCacheHash;
    }
}

knl_u_inval_context *GetInvalCxt()
{
    if (EnableLocalSysCache()) {
        return &t_thrd.lsc_cxt.lsc->inval_cxt;
    } else {
        return &u_sess->inval_cxt;
    }
}

void UnRegisterSysCacheCallBack(knl_u_inval_context *inval_cxt, int cacheid, SyscacheCallbackFunction func)
{
    for (int i = 0; i < inval_cxt->syscache_callback_count; i++) {
        if (inval_cxt->syscache_callback_list[i].id != cacheid ||
            inval_cxt->syscache_callback_list[i].function != func) {
            continue;
        }
        for (; i < inval_cxt->syscache_callback_count - 1; i++) {
            inval_cxt->syscache_callback_list[i].id = inval_cxt->syscache_callback_list[i + 1].id;
            inval_cxt->syscache_callback_list[i].function = inval_cxt->syscache_callback_list[i + 1].function;
            inval_cxt->syscache_callback_list[i].arg = inval_cxt->syscache_callback_list[i + 1].arg;
        }
        --inval_cxt->syscache_callback_count;
        break;
    }
}

void UnRegisterRelCacheCallBack(knl_u_inval_context *inval_cxt, RelcacheCallbackFunction func)
{
    for (int i = 0; i < inval_cxt->relcache_callback_count; i++) {
        if (inval_cxt->relcache_callback_list[i].function != func) {
            continue;
        }
        for (; i < inval_cxt->relcache_callback_count - 1; i++) {
            inval_cxt->relcache_callback_list[i].function = inval_cxt->relcache_callback_list[i + 1].function;
            inval_cxt->relcache_callback_list[i].arg = inval_cxt->relcache_callback_list[i + 1].arg;
        }
        --inval_cxt->relcache_callback_count;
        break;
    }
}

void UnRegisterPartCacheCallBack(knl_u_inval_context *inval_cxt, PartcacheCallbackFunction func)
{
    for (int i = 0; i < inval_cxt->partcache_callback_count; i++) {
        if (inval_cxt->partcache_callback_list[i].function != func) {
            continue;
        }
        for (; i < inval_cxt->partcache_callback_count - 1; i++) {
            inval_cxt->partcache_callback_list[i].function = inval_cxt->partcache_callback_list[i + 1].function;
            inval_cxt->partcache_callback_list[i].arg = inval_cxt->partcache_callback_list[i + 1].arg;
        }
        --inval_cxt->partcache_callback_count;
        break;
    }
}

static void ClearMyDBOfRelMapCxt(knl_u_relmap_context *relmap_cxt)
{
    relmap_cxt->local_map->magic = 0;
    relmap_cxt->local_map->num_mappings = 0;
    relmap_cxt->active_shared_updates->num_mappings = 0;
    relmap_cxt->active_local_updates->num_mappings = 0;
    relmap_cxt->pending_shared_updates->num_mappings = 0;
    relmap_cxt->pending_local_updates->num_mappings = 0;

    /* since clear when switchdb, just set their memcxt lsc_mydb_cxt */
    relmap_cxt->RelfilenodeMapHash = NULL;
    relmap_cxt->UHeapRelfilenodeMapHash = NULL;
}

struct HTAB *GetTableSpaceCacheHash()
{
    if (EnableLocalSysCache()) {
        return t_thrd.lsc_cxt.lsc->TableSpaceCacheHash;
    } else {
        return u_sess->cache_cxt.TableSpaceCacheHash;
    }
}

struct HTAB *GetSMgrRelationHash()
{
    if (EnableLocalSysCache()) {
        return t_thrd.lsc_cxt.lsc->SMgrRelationHash;
    } else {
        return u_sess->storage_cxt.SMgrRelationHash;
    }
}

struct vfd *GetVfdCache()
{
    if (EnableLocalSysCache()) {
        return t_thrd.lsc_cxt.lsc->VfdCache;
    } else {
        return u_sess->storage_cxt.VfdCache;
    }
}

struct vfd **GetVfdCachePtr()
{
    if (EnableLocalSysCache()) {
        return &t_thrd.lsc_cxt.lsc->VfdCache;
    } else {
        return &u_sess->storage_cxt.VfdCache;
    }
}

void SetVfdCache(vfd *value)
{
    if (EnableLocalSysCache()) {
        t_thrd.lsc_cxt.lsc->VfdCache = value;
    } else {
        u_sess->storage_cxt.VfdCache = value;
    }
}

void SetSizeVfdCache(Size value)
{
    if (EnableLocalSysCache()) {
        t_thrd.lsc_cxt.lsc->SizeVfdCache = value;
    } else {
        u_sess->storage_cxt.SizeVfdCache = value;
    }
}

Size GetSizeVfdCache()
{
    if (EnableLocalSysCache()) {
        return t_thrd.lsc_cxt.lsc->SizeVfdCache;
    } else {
        return u_sess->storage_cxt.SizeVfdCache;
    }
}

Size *GetSizeVfdCachePtr()
{
    if (EnableLocalSysCache()) {
        return &t_thrd.lsc_cxt.lsc->SizeVfdCache;
    } else {
        return &u_sess->storage_cxt.SizeVfdCache;
    }
}

int GetVfdNfile()
{
    if (EnableLocalSysCache()) {
        return t_thrd.lsc_cxt.lsc->nfile;
    } else {
        return u_sess->storage_cxt.nfile;
    }
}
void AddVfdNfile(int n)
{
    Assert(n == 1 || n == -1);
    if (EnableLocalSysCache()) {
        t_thrd.lsc_cxt.lsc->nfile += n;
    } else {
        u_sess->storage_cxt.nfile += n;
    }
}

dlist_head *getUnownedReln()
{
    if (EnableLocalSysCache()) {
        return &t_thrd.lsc_cxt.lsc->unowned_reln;
    } else {
        return &u_sess->storage_cxt.unowned_reln;
    }
}

knl_u_relmap_context *GetRelMapCxt()
{
    if (EnableLocalSysCache()) {
        return &t_thrd.lsc_cxt.lsc->relmap_cxt;
    } else {
        return &u_sess->relmap_cxt;
    }
}

void LocalSysDBCache::LocalSysDBCacheReleaseGlobalReSource(bool is_commit)
{
    ResourceOwnerReleaseRWLock(local_sysdb_resowner, is_commit);
    ResourceOwnerReleaseGlobalCatCList(local_sysdb_resowner, is_commit);
    ResourceOwnerReleaseGlobalCatCTup(local_sysdb_resowner, is_commit);
    ResourceOwnerReleaseGlobalBaseEntry(local_sysdb_resowner, is_commit);
    ResourceOwnerReleaseGlobalDBEntry(local_sysdb_resowner, is_commit);
    ResourceOwnerReleaseGlobalIsExclusive(local_sysdb_resowner, is_commit);
}

void LocalSysDBCache::LocalSysDBCacheReleaseCritialReSource(bool include_shared)
{
    closeAllVfds();
    LocalSysDBCacheReleaseGlobalReSource(false);
    ReleaseBadPtrList(false);

    systabcache.ReleaseGlobalRefcount(include_shared);
    if (m_global_db != NULL) {
        m_global_db->Release();
        m_global_db = NULL;
    }

    rdlock_info.count = 0;

    tabdefcache.ResetInitFlag();
    partdefcache.ResetInitFlag();
    systabcache.ResetInitFlag(include_shared);

    /* not zero when ereport error on searching */
    SetThreadDefExclusive(IS_THREAD_POOL_STREAM || IsBgWorkerProcess());
}

/* switch db, alter db */
bool LocalSysDBCache::LocalSysDBCacheNeedClearMyDB(Oid db_id, const char *db_name)
{
    if (likely(db_name != NULL && db_id == InvalidOid)) {
        if (strcmp(my_database_name, db_name) != 0) {
            return true;
        }
    } else if (unlikely(db_name == NULL && db_id != InvalidOid)) {
        if (my_database_id != db_id) {
            return true;
        }
    } else if (unlikely(db_name != NULL && db_id != InvalidOid)) {
        if (strcmp(my_database_name, db_name) != 0 || my_database_id != db_id) {
            return true;
        }
    } else {
        return true;
        Assert(t_thrd.role == AUTOVACUUM_LAUNCHER || t_thrd.role == UNDO_LAUNCHER || t_thrd.role == CATCHUP ||
            (IsBootstrapProcessingMode() &&
                t_thrd.role == MASTER_THREAD &&
                    strcmp(t_thrd.proc_cxt.MyProgName, "BootStrap") == 0
            ));
    }
    /* it is a weird design that we need access mydatabaseid before initsession.
     * but for GSC mode, we do need aquire lock when cache hit and there are invalid msgs
     * with session uninited and so u_sess->proc_cxt.MyDatabaseId is InvalidOid.
     * 1    the publication feature will send all rels' invalmsgs even no refered ddl.
     * 2    relations may have refcount leak, so we must rebuild them if cache hit.
     * when we rebuild a relation, the session may be not uninited,
     * and so u_sess->proc_cxt.MyDatabaseId is InvalidOid,
     * so we use t_thrd.lsc_cxt.lsc->my_database_id on GSC mode */
    Assert(CheckMyDatabaseMatch());
    Assert(m_global_db != NULL);
    /* if u_sess->proc_cxt.MyDatabaseId is InvalidOid, the session's status is uninit
     * we will call SetDatabase to rewrite it.
     * but beofre SetDatabase, we need the dbid to Accept Invalid msg */
    bool lock_db_advance = u_sess->proc_cxt.MyDatabaseId == InvalidOid && IS_THREAD_POOL_WORKER;

    /* cache hit, when initsession, we lock db to avoid alter db */
    if (lock_db_advance) {
        Assert(u_sess->proc_cxt.MyDatabaseTableSpace == InvalidOid);
        Assert(u_sess->proc_cxt.DatabasePath == NULL);
        Assert(t_thrd.proc->databaseId == InvalidOid);

        u_sess->proc_cxt.MyDatabaseId = my_database_id;
        u_sess->proc_cxt.MyDatabaseTableSpace = my_database_tablespace;
        /* use refer not copy, it will be rewritten when initsession */
        u_sess->proc_cxt.DatabasePath = my_database_path;

        /* we dont want to accept inval msg here, so use LockSharedObjectForSession to avoid it. */
        LockSharedObjectForSession(DatabaseRelationId, my_database_id, 0, RowExclusiveLock);
        t_thrd.proc->databaseId = my_database_id;
        UnlockSharedObjectForSession(DatabaseRelationId, my_database_id, 0, RowExclusiveLock);

        /* when we acquired the dblock, alter db transaction happened, and we should clear cache of mydb. */
        if (m_global_db->m_isDead) {
            t_thrd.proc->databaseId = InvalidOid;
            u_sess->proc_cxt.MyDatabaseId = InvalidOid;
            u_sess->proc_cxt.MyDatabaseTableSpace = InvalidOid;
            u_sess->proc_cxt.DatabasePath = NULL;
            return true;
        }
    } else if (m_global_db->m_isDead) {
        return true;
    }

    return false;
}

/* clear cache of mydb memcxt */
void LocalSysDBCache::LocalSysDBCacheClearMyDB(Oid db_id, const char *db_name)
{
    LocalSysDBCacheReleaseCritialReSource(false);

    TypeCacheHash = NULL;
    SMgrRelationHash = NULL;
    VfdCache = NULL;
    SizeVfdCache = 0;
    Assert(nfile == 0);
    nfile = 0;

    my_database_id = InvalidOid;
    my_database_tablespace = InvalidOid;
    my_database_name[0] = '\0';
    dlist_init(&unowned_reln);
    pfree_ext(my_database_path);
    ClearMyDBOfRelMapCxt(&relmap_cxt);
    UnRegisterRelCacheCallBack(&inval_cxt, TypeCacheRelCallback);
    UnRegisterRelCacheCallBack(&inval_cxt, RelfilenodeMapInvalidateCallback);
    UnRegisterRelCacheCallBack(&inval_cxt, UHeapRelfilenodeMapInvalidateCallback);
    MemoryContextResetAndDeleteChildren(lsc_mydb_memcxt);

    is_inited = false;
    rel_index_rule_space = 0;
}

void LocalSysDBCache::LocalSysDBCacheReSet()
{
    LocalSysDBCacheReleaseCritialReSource(true);

    /* reset flag */
    abort_count = 0;
    my_database_id = InvalidOid;
    my_database_tablespace = InvalidOid;
    my_database_name[0] = '\0';
    my_database_path = NULL;
    SMgrRelationHash = NULL;
    TableSpaceCacheHash = NULL;
    TypeCacheHash = NULL;
    VfdCache = NULL;
    SizeVfdCache = 0;
    Assert(nfile == 0);
    nfile = 0;
    dlist_init(&unowned_reln);
    bad_ptr_obj.ResetInitFlag();

    /* unregist callback who cache is on lsc */
    UnRegisterRelCacheCallBack(&inval_cxt, TypeCacheRelCallback);
    UnRegisterRelCacheCallBack(&inval_cxt, RelfilenodeMapInvalidateCallback);
    UnRegisterSysCacheCallBack(&inval_cxt, TABLESPACEOID, InvalidateTableSpaceCacheCallback);

    MemoryContextDelete(lsc_mydb_memcxt);
    MemoryContextDelete(lsc_share_memcxt);
    lsc_share_memcxt =
        AllocSetContextCreate(lsc_top_memcxt, "LocalSysCacheShareMemoryContext", ALLOCSET_DEFAULT_MINSIZE,
                              ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE, STANDARD_CONTEXT);

    lsc_mydb_memcxt =
        AllocSetContextCreate(lsc_top_memcxt, "LocalSysCacheMyDBMemoryContext", ALLOCSET_DEFAULT_MINSIZE,
                              ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE, STANDARD_CONTEXT);

    MemoryContext old = MemoryContextSwitchTo(lsc_share_memcxt);
    knl_u_relmap_init(&relmap_cxt);
    MemoryContextSwitchTo(old);

    rel_index_rule_space = 0;
    is_inited = false;
    is_closed = false;
    is_lsc_catbucket_created = false;
}

bool LocalSysDBCache::LocalSysDBCacheNeedReBuild()
{
    /* we have recovered from startup, redo may dont tell us inval msgs, so discard all lsc */
    if (unlikely(!recovery_finished && g_instance.global_sysdbcache.recovery_finished)) {
        return true;
    } else if (!g_instance.global_sysdbcache.hot_standby) {
        /* for standby, if not hot, the cache may be invalid */
        return true;
    }

    /* we assum 1kb memory leaked once */
    if (unlikely(abort_count > (uint64)g_instance.attr.attr_memory.local_syscache_threshold)) {
        return true;
    }

    uint64 total_space =
        ((AllocSet)lsc_top_memcxt)->totalSpace +
        ((AllocSet)lsc_share_memcxt)->totalSpace +
        ((AllocSet)lsc_mydb_memcxt)->totalSpace +
        ((AllocSet)u_sess->cache_mem_cxt)->totalSpace +
        rel_index_rule_space;

    uint64 memory_upper_limit = ((uint64)g_instance.attr.attr_memory.local_syscache_threshold) << 10;

    return total_space * (1 - MAX_LSC_FREESIZE_RATIO) > memory_upper_limit;
}

/* rebuild cache on memcxt of mydb or share, call it only before initsyscache */
void LocalSysDBCache::LocalSysDBCacheReBuild()
{
    /* invalid session cache */
    int i;
    knl_u_inval_context *inval_cxt = &u_sess->inval_cxt;
    for (i = 0; i < inval_cxt->syscache_callback_count; i++) {
        struct SYSCACHECALLBACK* ccitem = inval_cxt->syscache_callback_list + i;
        (*ccitem->function)(ccitem->arg, ccitem->id, 0);
    }

    for (i = 0; i < inval_cxt->relcache_callback_count; i++) {
        struct RELCACHECALLBACK* ccitem = inval_cxt->relcache_callback_list + i;
        (*ccitem->function)(ccitem->arg, InvalidOid);
    }

    for (i = 0; i < inval_cxt->partcache_callback_count; i++) {
        struct PARTCACHECALLBACK* ccitem = inval_cxt->partcache_callback_list + i;
        (*ccitem->function)(ccitem->arg, InvalidOid);
    }

    LocalSysDBCacheReSet();
    CreateCatBucket();
}

bool LocalSysDBCache::LocalSysDBCacheNeedSwapOut()
{
    uint64 used_space =
        AllocSetContextUsedSpace((AllocSet)lsc_top_memcxt) +
        AllocSetContextUsedSpace((AllocSet)lsc_share_memcxt) +
        AllocSetContextUsedSpace((AllocSet)lsc_mydb_memcxt) +
        AllocSetContextUsedSpace((AllocSet)u_sess->cache_mem_cxt) +
        rel_index_rule_space;
    uint64 memory_upper_limit =
        (((uint64)g_instance.attr.attr_memory.local_syscache_threshold) << 10) * cur_swapout_ratio;
    bool need_swapout = used_space > memory_upper_limit;

    /* swapout until memory used space is from MAX_LSC_SWAPOUT_RATIO=90% to MIN_LSC_SWAPOUT_RATIO=70% */
    if (unlikely(need_swapout && cur_swapout_ratio == MAX_LSC_SWAPOUT_RATIO)) {
        cur_swapout_ratio = MIN_LSC_SWAPOUT_RATIO;
    } else if (unlikely(!need_swapout && cur_swapout_ratio == MIN_LSC_SWAPOUT_RATIO)) {
        cur_swapout_ratio = MAX_LSC_SWAPOUT_RATIO;
    }
    return need_swapout;
}

void LocalSysDBCache::CloseLocalSysDBCache()
{
    if (is_closed) {
        return;
    }
    LocalSysDBCacheReleaseCritialReSource(true);
    is_inited = false;
    is_closed = true;
#ifdef USE_ASSERT_CHECKING
    lsc_close_check.setCloseFlag(true);
#endif
}

/* every cache knowes whether it is inited */
void LocalSysDBCache::ClearSysCacheIfNecessary(Oid db_id, const char *db_name)
{
    CreateCatBucket();
    if (unlikely(!is_inited)) {
        return;
    }
    /* rebuild if memory gt double of threshold */
    if (unlikely(LocalSysDBCacheNeedReBuild())) {
        recovery_finished = g_instance.global_sysdbcache.recovery_finished;
        LocalSysDBCacheReBuild();
        return;
    }

    if (LocalSysDBCacheNeedClearMyDB(db_id, db_name)) {
        LocalSysDBCacheClearMyDB(db_id, db_name);
        return;
    }

    /* only thread who switch sess need clean smgr block info */
    /* actually, ts task_producter need do this too, but those code need restructure. ts is discarded */
    if (IS_THREAD_POOL_WORKER) {
        smgrcleanblocknumall();
    }
    g_instance.global_sysdbcache.Refresh(m_global_db);
}

void LocalSysDBCache::CreateDBObject()
{
    Assert(lsc_top_memcxt == NULL);
    lsc_top_memcxt = AllocSetContextCreate(
        THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_DEFAULT), "LocalSysCacheTopMemoryContext", ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE, STANDARD_CONTEXT);
    lsc_share_memcxt =
        AllocSetContextCreate(lsc_top_memcxt, "LocalSysCacheShareMemoryContext", ALLOCSET_DEFAULT_MINSIZE,
                              ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE, STANDARD_CONTEXT);

    lsc_mydb_memcxt =
        AllocSetContextCreate(lsc_top_memcxt, "LocalSysCacheMyDBMemoryContext", ALLOCSET_DEFAULT_MINSIZE,
                              ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE, STANDARD_CONTEXT);
    MemoryContext old = MemoryContextSwitchTo(lsc_top_memcxt);
    systabcache.CreateObject();
    tabdefcache.CreateDefBucket();
    partdefcache.CreateDefBucket();
    knl_u_inval_init(&inval_cxt);
    dlist_init(&unowned_reln);
    MemoryContextSwitchTo(old);
    if (IS_PGXC_COORDINATOR) {
        CacheRegisterThreadSyscacheCallback(PGXCGROUPOID, ThreadNodeGroupCallback, (Datum)0);
    }
    /* init t_thrd resource owner */
    local_sysdb_resowner =
        ResourceOwnerCreate(NULL, "InitLocalSysCache", lsc_top_memcxt);

    old = MemoryContextSwitchTo(lsc_share_memcxt);
    knl_u_relmap_init(&relmap_cxt);
    MemoryContextSwitchTo(old);
    m_shared_global_db = g_instance.global_sysdbcache.GetSharedGSCEntry();
    rel_index_rule_space = 0;
    is_lsc_catbucket_created = false;
}

void LocalSysDBCache::CreateCatBucket()
{
    if (likely(is_lsc_catbucket_created)) {
        return;
    }
    MemoryContext old = MemoryContextSwitchTo(lsc_share_memcxt);
    systabcache.CreateCatBuckets();
    MemoryContextSwitchTo(old);
    rel_index_rule_space = 0;
    is_lsc_catbucket_created = true;
    cur_swapout_ratio = MAX_LSC_SWAPOUT_RATIO;
}

void LocalSysDBCache::SetDatabaseName(const char *db_name)
{
    if (db_name != NULL && db_name[0] != '\0') {
        size_t len = strlen(db_name);
        Assert(len > 0 && len < NAMEDATALEN);
        errno_t rc = memcpy_s(my_database_name, len + 1, db_name, len + 1);
        securec_check(rc, "\0", "\0");
        return;
    }

    if (my_database_name[0] == '\0') {
        t_thrd.proc_cxt.PostInit->GetDatabaseName(my_database_name);
    }

    if (my_database_name[0] == '\0') {
        char *tmp = get_database_name(my_database_id);
        size_t len = strlen(tmp);
        errno_t rc = memcpy_s(my_database_name, len + 1, tmp, len + 1);
        securec_check(rc, "\0", "\0");
        pfree_ext(tmp);
    }
}

void LocalSysDBCache::InitThreadDatabase(Oid db_id, const char *db_name, Oid db_tabspc)
{
    Assert(db_id != InvalidOid);
    if (my_database_id == db_id) {
        Assert(my_database_name[0] != '\0');
        Assert(my_database_tablespace == db_tabspc);
        Assert(db_name == NULL || strcmp(my_database_name, db_name) == 0);
        return;
    } else if (my_database_id != InvalidOid) {
        /* we has lock db and set thrd.proc.dbid, this should never happened */
        Assert(false);
        ereport(FATAL, (errno, errmsg("lsc has some error, please try again!")));
    }
    Assert(db_id == u_sess->proc_cxt.MyDatabaseId);
    Assert(my_database_id == InvalidOid);
    Assert(my_database_tablespace == InvalidOid);
    Assert(my_database_name[0] == '\0');
    my_database_id = db_id;
    my_database_tablespace = db_tabspc;
    if (db_id == TemplateDbOid) {
        my_database_name[0] = '\0';
        return;
    }
    SetDatabaseName(db_name);
}

void LocalSysDBCache::InitSessionDatabase(Oid db_id, const char *db_name, Oid db_tabspc)
{
    if (m_global_db != NULL && m_global_db->m_isDead) {
        ereport(FATAL, (errno, errmsg("It is too later to fix syscache, please try again!")));
        return;
    }
    Assert(db_id != InvalidOid);
    if (!is_inited) {
        my_database_id = db_id;
        if (db_name != NULL && !IsBootstrapProcessingMode()) {
            SetDatabaseName(db_name);
        }
        my_database_tablespace = db_tabspc;
    } else {
        Assert(my_database_id == db_id);
        Assert(my_database_tablespace == db_tabspc);
        Assert(strcmp(my_database_name, db_name) == 0);
    }
}

void LocalSysDBCache::InitDatabasePath(const char *db_path)
{
    Assert(db_path != NULL);
    Assert(strcmp(u_sess->proc_cxt.DatabasePath, db_path) == 0);
    if (my_database_path == NULL) {
        my_database_path = MemoryContextStrdup(lsc_share_memcxt, db_path);
    } else if (strcmp(my_database_path, db_path) != 0) {
        pfree_ext(my_database_path);
        my_database_path = MemoryContextStrdup(lsc_share_memcxt, db_path);
    }
}

void LocalSysDBCache::Init()
{
    Assert(!is_inited);
    Assert(m_global_db == NULL);
    Assert(my_database_id != InvalidOid);
    Assert(my_database_id == u_sess->proc_cxt.MyDatabaseId);
    m_global_db = g_instance.global_sysdbcache.GetGSCEntry(my_database_id, my_database_name);
    is_inited = true;
}

void LocalSysDBCache::InitRelMapPhase2()
{
    Assert(m_shared_global_db != NULL);
    m_shared_global_db->m_relmapCache->InitPhase2();
    if (!IS_MAGIC_EXIST(relmap_cxt.shared_map->magic)) {
        m_shared_global_db->m_relmapCache->CopyInto(relmap_cxt.shared_map);
    }
}

void LocalSysDBCache::InitRelMapPhase3()
{
    Assert(m_global_db != NULL);
    m_global_db->m_relmapCache->InitPhase2();
    if (!IS_MAGIC_EXIST(relmap_cxt.local_map->magic)) {
        m_global_db->m_relmapCache->CopyInto(relmap_cxt.local_map);
    }
}

void LocalSysDBCache::LoadRelMapFromGlobal(bool shared)
{
    GlobalSysDBCacheEntry *global_db = shared ? m_shared_global_db : m_global_db;
    RelMapFile *rel_map = shared ? relmap_cxt.shared_map : relmap_cxt.local_map;
    Assert(global_db != NULL);
    global_db->m_relmapCache->CopyInto(rel_map);
}

void LocalSysDBCache::InvalidateGlobalRelMap(bool shared, Oid db_id, RelMapFile *rel_map)
{
    if (shared) {
        Assert(m_shared_global_db != NULL);
        GlobalSysDBCacheEntry *global_db = m_shared_global_db;
        global_db->m_relmapCache->UpdateBy(rel_map);
    } else if (!is_inited) {
        Assert(m_global_db == NULL && !is_inited);
        GlobalSysDBCacheEntry *entry = g_instance.global_sysdbcache.FindTempGSCEntry(db_id);
        if (entry == NULL) {
            return;
        }
        entry->m_relmapCache->UpdateBy(rel_map);
        g_instance.global_sysdbcache.ReleaseTempGSCEntry(entry);
    } else {
        Assert(my_database_id == db_id);
        m_global_db->m_relmapCache->UpdateBy(rel_map);
    }
}

void LocalSysDBCache::SetThreadDefExclusive(bool is_exclusive)
{
    m_is_def_exclusive = is_exclusive;
}

LocalSysDBCache::LocalSysDBCache()
{
    lsc_top_memcxt = NULL;
    lsc_share_memcxt = NULL;
    lsc_mydb_memcxt = NULL;
    m_global_db = NULL;
    my_database_id = InvalidOid;
    my_database_name[0] = '\0';
    my_database_path = NULL;
    my_database_tablespace = InvalidOid;
    TableSpaceCacheHash = NULL;
    TypeCacheHash = NULL;
    SMgrRelationHash = NULL;
    VfdCache = NULL;
    SizeVfdCache = 0;
    nfile = 0;
    local_sysdb_resowner = NULL;

    abort_count = 0;

    rdlock_info.count = 0;
    got_pool_reload = false;
    m_shared_global_db = NULL;

    cur_swapout_ratio = MAX_LSC_SWAPOUT_RATIO;

    is_lsc_catbucket_created = false;
    is_closed = false;
    is_inited = false;
#ifdef USE_ASSERT_CHECKING
    lsc_close_check.setCloseFlag(false);
#endif
}

void AtEOXact_SysDBCache(bool is_commit)
{
    if (!EnableLocalSysCache()) {
        return;
    }
    Assert(t_thrd.lsc_cxt.lsc != NULL);
    ResourceOwnerReleaseLocalCatCList(t_thrd.lsc_cxt.lsc->local_sysdb_resowner, is_commit);
    ResourceOwnerReleaseLocalCatCTup(t_thrd.lsc_cxt.lsc->local_sysdb_resowner, is_commit);
    ResourceOwnerReleaseRelationRef(t_thrd.lsc_cxt.lsc->local_sysdb_resowner, is_commit);
    ResourceOwnerReleasePartitionRef(t_thrd.lsc_cxt.lsc->local_sysdb_resowner, is_commit);

    t_thrd.lsc_cxt.lsc->LocalSysDBCacheReleaseGlobalReSource(is_commit);

    ReleaseBadPtrList(is_commit);
    if (!is_commit) {
        t_thrd.lsc_cxt.lsc->abort_count++;
    }
    /* resowner make sure the lock released */
    t_thrd.lsc_cxt.lsc->rdlock_info.count = 0;

    t_thrd.lsc_cxt.lsc->SetThreadDefExclusive(IS_THREAD_POOL_STREAM || IsBgWorkerProcess());
}

void ReBuildLSC()
{
    if (!EnableLocalSysCache()) {
        return;
    }
    if (t_thrd.lsc_cxt.lsc == NULL || t_thrd.lsc_cxt.lsc->is_closed) {
        return;
    }
    t_thrd.lsc_cxt.lsc->LocalSysDBCacheReBuild();
}

void AppendBadPtr(void *elem)
{
    BadPtrObj *obj = &t_thrd.lsc_cxt.lsc->bad_ptr_obj;
    /* enlarge size of dad ptr list if necessary */
    int newmax = 0;
    if (obj->nbadptr >= obj->maxbadptr) {
        if (obj->bad_ptr_lists == NULL) {
            newmax = 16;
            obj->bad_ptr_lists = (void **)MemoryContextAlloc(t_thrd.lsc_cxt.lsc->lsc_share_memcxt,
                newmax * sizeof(void *));
            obj->maxbadptr = newmax;
        } else {
            newmax = obj->maxbadptr * 2;
            obj->bad_ptr_lists = (void **)repalloc(obj->bad_ptr_lists, newmax * sizeof(void *));
            obj->maxbadptr = newmax;
        }
    }

    /* remember bad ptr */
    Assert(obj->nbadptr < obj->maxbadptr);
    obj->bad_ptr_lists[obj->nbadptr] = elem;
    obj->nbadptr++;
}

void RemoveBadPtr(void *elem)
{
    BadPtrObj *obj = &t_thrd.lsc_cxt.lsc->bad_ptr_obj;
    void **bad_lists = obj->bad_ptr_lists;
    int nc = obj->nbadptr - 1;
    for (int i = nc; i >= 0; i--) {
        if (bad_lists[i] == elem) {
            while (i < nc) {
                bad_lists[i] = bad_lists[i + 1];
                i++;
            }
            obj->nbadptr = nc;
            return;
        }
    }
}

static void ReleaseBadPtrList(bool isCommit)
{
    BadPtrObj *obj = &t_thrd.lsc_cxt.lsc->bad_ptr_obj;
    while (obj->nbadptr > 0) {
        if (isCommit) {
            /* DFX: print some debug info here */
        }
        pfree_ext(obj->bad_ptr_lists[obj->nbadptr - 1]); /* 只释放了指针 */
        obj->nbadptr--;
    }
}

void StreamTxnContextSaveInvalidMsg(void *stc)
{
    if (!EnableLocalSysCache()) {
        STCSaveElem(((StreamTxnContext *)stc)->lsc_dbcache, NULL);
        return;
    }
    STCSaveElem(((StreamTxnContext *)stc)->lsc_dbcache, t_thrd.lsc_cxt.lsc);
    /* we don't know what bgworker do,
     * just stop insert rel/part into gsc,
     * tuple has its flag to decide hot to do insert*/
    t_thrd.lsc_cxt.lsc->SetThreadDefExclusive(true);
}
void StreamTxnContextRestoreInvalidMsg(void *stc)
{
    if (!EnableLocalSysCache()) {
        return;
    }
    LocalSysDBCache *lsc_dbcache = ((StreamTxnContext *)stc)->lsc_dbcache;
    InvalidBaseEntry *src_part = &lsc_dbcache->partdefcache.invalid_entries;
    InvalidBaseEntry *dst_part = &t_thrd.lsc_cxt.lsc->partdefcache.invalid_entries;
    for (int i = 0; i < src_part->count; i++) {
        dst_part->InsertInvalidDefValue(src_part->invalid_values[i]);
    }

    InvalidBaseEntry *src_rel = &lsc_dbcache->tabdefcache.invalid_entries;
    InvalidBaseEntry *dst_rel = &t_thrd.lsc_cxt.lsc->tabdefcache.invalid_entries;
    for (int i = 0; i < src_rel->count; i++) {
        dst_rel->InsertInvalidDefValue(src_rel->invalid_values[i]);
    }

    for (int i = 0; i < SysCacheSize; i++) {
        InvalidBaseEntry *src_tup = &lsc_dbcache->systabcache.local_systupcaches[i]->invalid_entries;
        InvalidBaseEntry *dst_tup = &t_thrd.lsc_cxt.lsc->systabcache.local_systupcaches[i]->invalid_entries;
        for (int i = 0; i < src_tup->count; i++) {
            dst_tup->InsertInvalidDefValue(src_tup->invalid_values[i]);
            dst_tup->is_reset |= src_tup->is_reset;
        }
    }
}

void ReleaseAllGSCRdConcurrentLock()
{
    if (!EnableLocalSysCache() || t_thrd.lsc_cxt.lsc->rdlock_info.count == 0) {
        return;
    }
    while (t_thrd.lsc_cxt.lsc->rdlock_info.count > 0) {
        int cur_index = t_thrd.lsc_cxt.lsc->rdlock_info.count - 1;
        ReleaseGSCTableReadLock(t_thrd.lsc_cxt.lsc->rdlock_info.has_concurrent_lock[cur_index],
            t_thrd.lsc_cxt.lsc->rdlock_info.concurrent_lock[cur_index]);
    }
}