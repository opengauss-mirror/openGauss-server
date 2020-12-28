/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
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
 * -------------------------------------------------------------------------
 *
 * globalplancache.cpp
 *    global plan cache
 *
 * IDENTIFICATION
 *     src/gausskernel/process/globalplancache/globalplancache.cpp
 *
 * -------------------------------------------------------------------------
 */


#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/hash.h"
#include "access/xact.h"
#include "catalog/pgxc_node.h"
#include "commands/prepare.h"
#include "executor/lightProxy.h"
#include "executor/spi_priv.h"
#include "optimizer/nodegroups.h"
#include "pgxc/groupmgr.h"
#include "pgxc/pgxcnode.h"
#include "utils/dynahash.h"
#include "utils/globalplancache.h"
#include "utils/globalpreparestmt.h"
#include "utils/memutils.h"
#include "utils/plancache.h"
#include "utils/syscache.h"
#include "utils/plpgsql.h"

static bool
CompareSearchPath(struct OverrideSearchPath* path1, struct OverrideSearchPath* path2)
{
    Assert(path1 != NULL);

    if (path2 == NULL) {
        return OverrideSearchPathMatchesCurrent(path1);
    }

    if (path1 == path2) {
        return true;
    }

    if (path1->addTemp != path2->addTemp) {
        return false;
    }
    if (path1->addCatalog != path2->addCatalog) {
        return false;
    }
    if (list_difference_oid(path1->schemas, path2->schemas) != NULL) {
        return false;
    }
    return true;
}
/*
 * Return false when the given compilation environment matches the current
 * session compilation environment, mainly compares GUC parameter settings.
 */
static bool
GPCCompareEnv(GPCEnv *env1, GPCEnv *env2)
{
    Assert (env1 != NULL);
    Assert (env2 != NULL);
    if (memcmp(&env1->plainenv, &env2->plainenv, sizeof(GPCPlainEnv)) == 0
        && strncmp(env1->default_storage_nodegroup, env2->default_storage_nodegroup, NAMEDATALEN) == 0
         && strncmp(env1->expected_computing_nodegroup, env2->expected_computing_nodegroup, NAMEDATALEN) == 0
         && env1->num_params == env2->num_params)
    {
        if (CompareSearchPath(env1->search_path, env2->search_path)) {
            if (env1->depends_on_role != env2->depends_on_role && env1->user_oid != env2->user_oid) {
                return false;
            } else if (env1->depends_on_role && env2->depends_on_role && env1->user_oid != env2->user_oid) {
                return false;
            } else {
                return true;
            }
        }
    }

    return false;
}

uint32 GPCHashFunc(const void *key, Size keysize)
{
    const GPCKey *item = (const GPCKey *) key;
    uint32 val1 = DatumGetUInt32(hash_any((const unsigned char *)item->query_string, item->query_length));
    uint32 val2 = DatumGetUInt32(hash_any((const unsigned char *)(&item->env.plainenv), sizeof(GPCPlainEnv)));
    uint32 val3 = DatumGetUInt32(hash_any((const unsigned char *)(&item->spi_signature), sizeof(SPISign)));
    val1 ^= val2;
    val1 ^= val3;

    return val1;
}

int GPCKeyMatch(const void *left, const void *right, Size keysize)
{
    GPCKey *leftItem = (GPCKey*)left;
    GPCKey *rightItem = (GPCKey*)right;
    Assert(NULL != leftItem);
    Assert(NULL != rightItem);

    /* we just care whether the result is 0 or not. */
    if (leftItem->query_length != rightItem->query_length) {
        return 1;
    }

    if(strncmp(leftItem->query_string, rightItem->query_string, leftItem->query_length)) {
        return 1;
    }

    if(GPCCompareEnv(&(leftItem->env), &(rightItem->env)) == false) {
        return 1;
    }

    return 0;
}

/*****************
 global plan cache
 *****************/

GlobalPlanCache::GlobalPlanCache()
{
    Init();
}

GlobalPlanCache::~GlobalPlanCache()
{
}

void GlobalPlanCache::Init()
{
    HASHCTL ctl;
    errno_t rc = 0;
    rc = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
    securec_check(rc, "\0", "\0");
    ctl.keysize = sizeof(GPCKey);
    ctl.entrysize = sizeof(GPCEntry);
    ctl.hash = (HashValueFunc)GPCHashFunc;
    ctl.match = (HashCompareFunc)GPCKeyMatch;

    int flags = HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT | HASH_COMPARE | HASH_EXTERN_CONTEXT | HASH_NOEXCEPT;

    m_array = (GPCHashCtl *) MemoryContextAllocZero(g_instance.cache_cxt.global_cache_mem,
                                                                      sizeof(GPCHashCtl) * GPC_NUM_OF_BUCKETS); 

    for (uint32 i = 0; i < GPC_NUM_OF_BUCKETS; i++) {
        m_array[i].count = 0;
        m_array[i].lockId = FirstGPCMappingLock + i;
        
        /*
        * Create a MemoryContext per hash bucket so that all entries, plans etc under the bucket will live
        * under this Memory context. This is for performance purposes. We do not want everything to be under
        * the shared GlobalPlanCacheContext because more threads would need to synchronize everytime it needs a chunk 
        * of memory and that would become a bottleneck.
        */
        m_array[i].context = AllocSetContextCreate(g_instance.cache_cxt.global_cache_mem,
                                                                   "GPC_Plan_Bucket_Context",
                                                                   ALLOCSET_DEFAULT_MINSIZE,
                                                                   ALLOCSET_DEFAULT_INITSIZE,
                                                                   ALLOCSET_DEFAULT_MAXSIZE,
                                                                   SHARED_CONTEXT);


        ctl.hcxt = m_array[i].context;
        m_array[i].hash_tbl = hash_create("Global_Plan_Cache",
                                                        GPC_HTAB_SIZE,
                                                        &ctl,
                                                        flags);
 
    }

    m_invalid_list = NULL;
}

bool GlobalPlanCache::TryStore(CachedPlanSource *plansource,  PreparedStatement *ps)
{
    Assert (plansource != NULL);
    Assert (plansource->magic == CACHEDPLANSOURCE_MAGIC);
    Assert (!plansource->gpc.status.InShareTable());
    Assert (plansource->gpc.status.IsSharePlan());
    Assert (plansource->is_support_gplan || (plansource->gplan == NULL && plansource->cplan == NULL));

    GPCKey* key = plansource->gpc.key;

    uint32 hashCode = GPCHashFunc((const void *) key, sizeof(*key));

    uint32 bucket_id = GetBucket(hashCode);
    Assert (bucket_id >= 0 && bucket_id < GPC_NUM_OF_BUCKETS);
    int lock_id = m_array[bucket_id].lockId;

    (void)LWLockAcquire(GetMainLWLockByIndex(lock_id), LW_EXCLUSIVE);
    MemoryContext oldcontext = MemoryContextSwitchTo(m_array[bucket_id].context);

    bool found = false;
    GPCEntry *entry = (GPCEntry *)hash_search_with_hash_value(m_array[bucket_id].hash_tbl,
                                                                (const void*)key, hashCode, HASH_ENTER, &found);
    if (entry == NULL) {
        MemoryContextSwitchTo(oldcontext);
        LWLockRelease(GetMainLWLockByIndex(lock_id));
        ereport(ERROR,
              (errcode(ERRCODE_UNDEFINED_PSTATEMENT),
               errmsg("store global plan source failed due to memory allocation failed")));
        return false;
    }

    if (found == false) {
        START_CRIT_SECTION();
        /* Deep copy the query_string to the GPC entry's query_string */
        entry->key.query_string = key->query_string;
        entry->key.query_length = key->query_length;
        /* Set the magic number. */
        entry->val.plansource = plansource;
        /* off the link */
        plansource->next_saved = NULL;
        /* initialize the ref count .*/
        plansource->gpc.status.AddRefcount();

        m_array[bucket_id].count++;
        Assert(plansource->context->is_shared);
        MemoryContextSeal(plansource->context);
        Assert(plansource->query_context->is_shared);
        MemoryContextSeal(plansource->query_context);
        if (plansource->gplan) {
            pg_atomic_fetch_add_u32((volatile uint32*)&plansource->gplan->global_refcount, 1);
            plansource->gplan->is_share = true;
            Assert(plansource->gplan->context->is_shared);
            MemoryContextSeal(plansource->gplan->context);
        }
#ifdef USE_ASSERT_CHECKING
        else {
            Assert(IS_PGXC_COORDINATOR && plansource->single_exec_node &&
                   plansource->gplan == NULL && plansource->cplan == NULL);
        }
#endif
        plansource->gpc.status.SetLoc(GPC_SHARE_IN_SHARE_TABLE);
        END_CRIT_SECTION();

    } else {
        /* some guys win. */
        CachedPlanSource* newsource = entry->val.plansource;
        ps->plansource = newsource;
        newsource->gpc.status.AddRefcount();
        u_sess->pcache_cxt.gpc_in_try_store = true;

        /* purge old one. */
        GPC_LOG("drop cache plan in try store", plansource, 0);
#ifdef ENABLE_MULTIPLE_NODES
        if (IS_PGXC_COORDINATOR) {
            GPCDropLPIfNecessary(ps->stmt_name, false, false, newsource);
        }
#endif
        DropCachedPlan(plansource);
        u_sess->pcache_cxt.gpc_in_try_store = false;

    }

    MemoryContextSwitchTo(oldcontext);
    LWLockRelease(GetMainLWLockByIndex(lock_id));
    return true;
}

CachedPlanSource* GlobalPlanCache::Fetch(const char *query_string, uint32 query_len,
                                         int num_params, SPISign* spi_sign_ptr)
{
    GPCKey key;
    key.env.filled = false;
    key.query_string = query_string;
    key.query_length = query_len;
    EnvFill(&key.env, false);
    key.env.search_path = NULL;
    key.env.num_params = num_params;
    if (spi_sign_ptr != NULL)
        key.spi_signature = *spi_sign_ptr;
    else
        key.spi_signature = {(uint32)-1, 0, (uint32)-1, -1};
#ifdef ENABLE_MULTIPLE_NODES
    GPCCheckGuc();
#endif
    uint32 hashCode = GPCHashFunc((const void *) &key, sizeof(key));

    uint32 bucket_id = GetBucket(hashCode);
    Assert (bucket_id >= 0 && bucket_id < GPC_NUM_OF_BUCKETS);
    int lock_id = m_array[bucket_id].lockId;

    (void)LWLockAcquire(GetMainLWLockByIndex(lock_id), LW_SHARED);
    MemoryContext oldcontext = MemoryContextSwitchTo(m_array[bucket_id].context);

    bool foundCachedEntry = false;
    GPCEntry *entry = (GPCEntry *) hash_search_with_hash_value(m_array[bucket_id].hash_tbl,
                                                                     (const void*)(&key),
                                                                     hashCode,
                                                                     HASH_FIND,
                                                                     &foundCachedEntry);

    if (!foundCachedEntry) {
        MemoryContextSwitchTo(oldcontext);
        LWLockRelease(GetMainLWLockByIndex(lock_id));
        return NULL;
    }
    else {
        entry->val.plansource->gpc.status.AddRefcount();
        MemoryContextSwitchTo(oldcontext);
        LWLockRelease(GetMainLWLockByIndex(lock_id));
        return entry->val.plansource;
    }

    return NULL;
}

void GlobalPlanCache::AddInvalidList(CachedPlanSource* plansource)
{
    (void)LWLockAcquire(GPCClearLock, LW_EXCLUSIVE);
    MemoryContext oldcontext = MemoryContextSwitchTo(g_instance.cache_cxt.global_cache_mem);
    START_CRIT_SECTION();
    m_invalid_list = dlappend(m_invalid_list, plansource);
    plansource->gpc.status.SetLoc(GPC_SHARE_IN_SHARE_TABLE_INVALID_LIST);
    plansource->gpc.status.SetStatus(GPC_INVALID);
    END_CRIT_SECTION();
    MemoryContextSwitchTo(oldcontext);
    LWLockRelease(GPCClearLock);
}

void GlobalPlanCache::DropInvalid()
{
    (void)LWLockAcquire(GPCClearLock, LW_EXCLUSIVE);
    if (m_invalid_list != NULL) {
        DListCell *cell = m_invalid_list->head;
        while (cell != NULL) {
            CachedPlanSource *curr = (CachedPlanSource *)cell->data.ptr_value;
            if (!curr->gpc.status.CheckRefCount())
                ereport(PANIC,
                        (errmsg("plancache refcount %d less than 0: %s.(%lu,%u,%u)",
                         curr->gpc.status.GetRefCount(),  curr->stmt_name, u_sess->sess_ident.cn_sessid,
                         u_sess->sess_ident.cn_timeline, u_sess->sess_ident.cn_nodeid)));
            if (curr->gpc.status.RefCountZero()) {
                Assert(curr->next_saved == NULL);
                DListCell *next = cell->next;
                GPC_LOG("drop invalid shared plancache", curr, curr->stmt_name);
                m_invalid_list = dlist_delete_cell(m_invalid_list, cell, false);
                DropCachedPlanInternal(curr);
                curr->magic = 0;
                MemoryContextUnSeal(curr->context);
                MemoryContextUnSeal(curr->query_context);
                MemoryContextDelete(curr->context);

                cell = next;
            } else {
                cell = cell->next;
            }
        }
    }
    LWLockRelease(GPCClearLock);
}

void GlobalPlanCache::RemovePlanSourceInRecreate(CachedPlanSource* plansource)
{
    if(plansource->gpc.status.InShareTable()) {
        GPCKey* key = plansource->gpc.key;
        uint32 hashCode = GPCHashFunc((const void *) key, sizeof(*key));
        uint32 bucket_id = GetBucket(hashCode);
        int lock_id = m_array[bucket_id].lockId;
        (void)LWLockAcquire(GetMainLWLockByIndex(lock_id), LW_EXCLUSIVE);
        if (plansource->gpc.status.InShareTableInvalidList() == false) {
            bool found = false;
            hash_search(m_array[bucket_id].hash_tbl, (void *)key, HASH_REMOVE, &found);
            m_array[bucket_id].count--;
            AddInvalidList(plansource);
        }
        plansource->gpc.status.SubRefCount();
        DropInvalid();
        LWLockRelease(GetMainLWLockByIndex(lock_id));
    } else {
        GPC_LOG("remove private plansource", plansource, plansource->stmt_name);
        DropCachedPlan(plansource);
    }
}

void GlobalPlanCache::InvalidPlanSourceForReload(CachedPlanSource* plansource, const char* stmt_name)
{
    Assert(IS_PGXC_COORDINATOR);
    if (plansource->gpc.status.InShareTable()) {
        /* Close any active planned Datanode statements */
#ifdef ENABLE_MULTIPLE_NODES
        if (plansource->gplan != NULL) {
            GPCCleanDatanodeStatement(plansource->gplan->dn_stmt_num, stmt_name);
        } else
            GPCDropLPIfNecessary(stmt_name, true, true, NULL);
#endif
        /* add into invalidlist */
        CN_GPC_LOG("invalid global plan for reload ", plansource, stmt_name);
        GPCKey* key = plansource->gpc.key;
        uint32 hashCode = GPCHashFunc((const void *) key, sizeof(*key));
        uint32 bucket_id = GetBucket(hashCode);
        int lock_id = m_array[bucket_id].lockId;
        (void)LWLockAcquire(GetMainLWLockByIndex(lock_id), LW_EXCLUSIVE);
        if (plansource->gpc.status.InShareTableInvalidList() == false) {
            bool found = false;
            hash_search(m_array[bucket_id].hash_tbl, (void *)key, HASH_REMOVE, &found);
            m_array[bucket_id].count--;
            AddInvalidList(plansource);
        }
        LWLockRelease(GetMainLWLockByIndex(lock_id));
    } else {
        CN_GPC_LOG("invalid plan for reload ", plansource, stmt_name);
        plansource->gpc.status.SetStatus(GPC_INVALID);
        plansource->is_valid = false;
        if (plansource->gplan == NULL) {
            GPCDropLPIfNecessary(stmt_name, true, true, NULL);
        } else {
            plansource->gplan->is_valid = false;
            Assert(!plansource->gplan->isShared());
        }
        DropCachedPlanInternal(plansource);
    }
}

void GlobalPlanCache::InvalidPlanSourceForCNretry(CachedPlanSource* plansource)
{
    if (plansource->gpc.status.InShareTable()) {
        GPCKey* key = plansource->gpc.key;
        uint32 hashCode = GPCHashFunc((const void *) key, sizeof(*key));
        uint32 bucket_id = GetBucket(hashCode);
        int lock_id = m_array[bucket_id].lockId;
        CN_GPC_LOG("invalid global plan for cnretry ", plansource, 0);
        (void)LWLockAcquire(GetMainLWLockByIndex(lock_id), LW_EXCLUSIVE);
        if (plansource->gpc.status.InShareTableInvalidList() == false) {
            bool found = false;
            hash_search(m_array[bucket_id].hash_tbl, (void *)key, HASH_REMOVE, &found);
            m_array[bucket_id].count--;
            AddInvalidList(plansource);
        }
        DropInvalid();
        LWLockRelease(GetMainLWLockByIndex(lock_id));
    } else {
        CN_GPC_LOG("invalid plan for cnretry ", plansource, 0);
        plansource->gpc.status.SetStatus(GPC_INVALID);
        plansource->is_valid = false;
        if (plansource->gplan)
            plansource->gplan->is_valid = false;
    }
}

void GlobalPlanCache::RemoveEntry(uint32 htblIdx, GPCEntry *entry)
{
    CachedPlanSource *plansource = entry->val.plansource;

    bool found = false;
    hash_search(m_array[htblIdx].hash_tbl, (void *) &(entry->key), HASH_REMOVE, &found);
    Assert(found == true);
    m_array[htblIdx].count--;
    AddInvalidList(plansource);
    DropInvalid();
}


bool GlobalPlanCache::CheckRecreateCachePlan(PreparedStatement *entry)
{
    /*
     * Start up a transaction command so we can run parse analysis etc. (Note
     * that this will normally change current memory context.) Nothing happens
     * if we are already in one.
     */
    start_xact_command();
    Assert(entry->plansource->magic == CACHEDPLANSOURCE_MAGIC);

    if (entry->plansource->gpc.status.InSavePlanList(GPC_SHARED)) {
        return false;
    }
#ifdef ENABLE_MULTIPLE_NODES
    if (IS_PGXC_COORDINATOR && !entry->plansource->gpc.status.InShareTable()) {
        return false;
    }
#else
    if (!entry->plansource->gpc.status.InShareTable()) {
        return false;
    }
#endif

    if (u_sess->pcache_cxt.gpc_in_ddl == true) {
        return true;
    }
    if (!entry->plansource->gpc.status.IsValid()) {
        return true;
    }
    if (entry->plansource->dependsOnRole && (entry->plansource->rewriteRoleId != GetUserId())) {
        return true;
    }
    if ((entry->plansource->gplan != NULL && TransactionIdIsValid(entry->plansource->gplan->saved_xmin))) {
        return true;
    }

    if (entry->plansource->search_path && !OverrideSearchPathMatchesCurrent(entry->plansource->search_path)) {
        return true;
    }

    return false;
}

bool GlobalPlanCache::CheckRecreateSPICachePlan(SPIPlanPtr spi_plan)
{
    start_xact_command();
    ListCell* cell = NULL;
    Assert(spi_plan->magic == _SPI_PLAN_MAGIC);
    foreach(cell, spi_plan->plancache_list) {
        CachedPlanSource* plansource = (CachedPlanSource*)lfirst(cell);
        if (!plansource->gpc.status.InShareTable()) {
            continue;
        }
        if (u_sess->pcache_cxt.gpc_in_ddl == true) {
            return true;
        }
        if (!plansource->gpc.status.IsValid()) {
            return true;
        }
        if (plansource->dependsOnRole && (plansource->rewriteRoleId != GetUserId())) {
            return true;
        }
        if ((plansource->gplan != NULL &&
            TransactionIdIsValid(plansource->gplan->saved_xmin) &&
            !TransactionIdEquals(plansource->gplan->saved_xmin, u_sess->utils_cxt.TransactionXmin))) {
            return true;
        }

        if (plansource->search_path && !OverrideSearchPathMatchesCurrent(plansource->search_path)) {
            return true;
        }
    }

    return false;
}

void GlobalPlanCache::RecreateSPICachePlan(SPIPlanPtr spiplan)
{
    ListCell* cell = NULL;
    Assert(spiplan->magic == _SPI_PLAN_MAGIC);
    /* push error context stack */
    ErrorContextCallback spi_err_context;
    spi_err_context.callback = _SPI_error_callback;
    spi_err_context.arg = NULL; /* we'll fill this below */
    spi_err_context.previous = t_thrd.log_cxt.error_context_stack;
    t_thrd.log_cxt.error_context_stack = &spi_err_context;
    foreach(cell, spiplan->plancache_list) {
        CachedPlanSource* oldsource = (CachedPlanSource*)lfirst(cell);
        if (!oldsource->gpc.status.InShareTable())
            continue;
        GPC_LOG("recreate spi cachedplan", oldsource, 0);
        CachedPlanSource *newsource = CopyCachedPlan(oldsource, true);
        spi_err_context.arg = (void *)newsource->query_string;
        Assert (u_sess->SPI_cxt._current->spi_hash_key == oldsource->spi_signature.spi_key);
        MemoryContext oldcxt = MemoryContextSwitchTo(newsource->context);
        newsource->stream_enabled = u_sess->attr.attr_sql.enable_stream_operator;
        Assert (oldsource->gpc.status.IsSharePlan());
        newsource->gpc.status.ShareInit();
        newsource->spi_signature = oldsource->spi_signature;
        newsource->parserSetup = spiplan->parserSetup;
        newsource->parserSetupArg = spiplan->parserSetupArg;
        // If the planSource is set to invalid, the AST must be analyzed again
        // because the meta has changed.
        newsource->is_valid = false;
        (void)RevalidateCachedQuery(newsource);
        newsource->next_saved = u_sess->pcache_cxt.first_saved_plan;
        u_sess->pcache_cxt.first_saved_plan = newsource;
        newsource->is_saved = true;
        newsource->gpc.status.SetLoc(GPC_SHARE_IN_LOCAL_SAVE_PLAN_LIST);
        cell->data.ptr_value = (void*)newsource;
        MemoryContextSwitchTo(oldcxt);
        RemovePlanSourceInRecreate(oldsource);
    }
    /* pop error context stack */
    t_thrd.log_cxt.error_context_stack = spi_err_context.previous;
    Assert(SPIPlanCacheTableLookup(u_sess->SPI_cxt._current->spi_hash_key));
}

void GlobalPlanCache::RecreateCachePlan(PreparedStatement *entry, const char* stmt_name)
{
    CachedPlanSource *oldsource = entry->plansource;
    GPC_LOG("recreate plan", oldsource, stmt_name);
    CachedPlanSource *newsource = CopyCachedPlan(entry->plansource, true);
    MemoryContext oldcxt = MemoryContextSwitchTo(newsource->context);
    newsource->stream_enabled = u_sess->attr.attr_sql.enable_stream_operator;
    newsource->stmt_name = pstrdup(stmt_name);
    // If the planSource is set to invalid, the AST must be analyzed again
    // because the meta has changed.
    newsource->is_valid = false;
    Assert (oldsource->gpc.status.IsSharePlan());
    newsource->gpc.status.ShareInit();
#ifdef ENABLE_MULTIPLE_NODES
    bool had_lp = (IS_PGXC_COORDINATOR && oldsource->single_exec_node != NULL &&
                    oldsource->gplan == NULL && oldsource->cplan == NULL);
    (void)RevalidateCachedQuery(newsource, had_lp);
    /* clean session's datanode statment on cn */
    if (had_lp) {
        /* no lp in newsource, delete old lp */
        GPCDropLPIfNecessary(entry->stmt_name, false, true, NULL);
    } else {
        if (IS_PGXC_COORDINATOR && oldsource->gplan != NULL) {
            /* Close any active planned Datanode statements, recreate in BuildCachedPlan later */
            GPCCleanDatanodeStatement(oldsource->gplan->dn_stmt_num, stmt_name);
        }
    }
#else
    (void)RevalidateCachedQuery(newsource);
#endif
    newsource->next_saved = u_sess->pcache_cxt.first_saved_plan;
    u_sess->pcache_cxt.first_saved_plan = newsource;
    newsource->is_saved = true;
    newsource->gpc.status.SetLoc(GPC_SHARE_IN_LOCAL_SAVE_PLAN_LIST);
    u_sess->exec_cxt.CurrentOpFusionObj = NULL;
    entry->plansource = newsource;
    MemoryContextSwitchTo(oldcxt);
    RemovePlanSourceInRecreate(oldsource);
}

void GlobalPlanCache::Commit()
{
#ifdef ENABLE_MULTIPLE_NODES
    if (IS_PGXC_COORDINATOR) {
        CNCommit();
    } else {
        DNCommit();
    }
#else
    CNCommit();
#endif
}

/*
 * @Description: Store the global plancache into the hash table and mark them as shared.
 * this function called when a transction commited. All unsaved global plancaches are stored
 * in the list named first_saved_plan. When we found that a plancache was saved by others, we
 * just drop it and update the prepare pointer to the shared glboal plancache.
 * @in num: void
 * @return - void
 */
void GlobalPlanCache::DNCommit()
{
    CachedPlanSource *next_plansource = NULL;
    CachedPlanSource *plansource = u_sess->pcache_cxt.first_saved_plan;
    u_sess->pcache_cxt.first_saved_plan = NULL;
    while (plansource != NULL) {
        Assert(plansource->magic == CACHEDPLANSOURCE_MAGIC);
        Assert(!plansource->gpc.status.InShareTable());
        if (unlikely(plansource->magic != CACHEDPLANSOURCE_MAGIC)) {
            ereport(PANIC,
                  (errcode(ERRCODE_UNDEFINED_PSTATEMENT),
                   errmsg("In gpc commit stage, plansource has already been freed(%lu,%u,%u)",
                          u_sess->sess_ident.cn_sessid, u_sess->sess_ident.cn_timeline, u_sess->sess_ident.cn_nodeid)));
        }

        next_plansource = plansource->next_saved;

        if (!plansource->is_valid || plansource->gpc.status.IsPrivatePlan() ||
            plansource->gplan == NULL || !plansource->is_support_gplan) {
            GPC_LOG("invalid plan in commit", plansource, plansource->stmt_name);
            plansource->is_valid = false;
            plansource->next_saved = NULL;
            plansource->gpc.status.SetLoc(GPC_SHARE_IN_PREPARE_STATEMENT);
            plansource->gpc.status.SetStatus(GPC_INVALID);
        } else {
            PreparedStatement* ps = g_instance.prepare_cache->Fetch(plansource->stmt_name, true);

            if (unlikely(ps == NULL)) {
#ifdef MEMORY_CONTEXT_CHECKING
                ereport(PANIC,
#else
                ereport(ERROR,
#endif
                      (errcode(ERRCODE_UNDEFINED_PSTATEMENT),
                       errmsg("In gpc commit stage, fail to fetch prepare statement: %s.(%lu,%u,%u)", plansource->stmt_name,
                              u_sess->sess_ident.cn_sessid, u_sess->sess_ident.cn_timeline, u_sess->sess_ident.cn_nodeid)));
            }
            TryStore(plansource, ps);
        }

        plansource = next_plansource;
    }
}

void GlobalPlanCache::CNCommit()
{
    CachedPlanSource *next_plansource = NULL;
    CachedPlanSource *plansource = u_sess->pcache_cxt.first_saved_plan;
    u_sess->pcache_cxt.first_saved_plan = NULL;
    while (plansource != NULL) {
        Assert(plansource->magic == CACHEDPLANSOURCE_MAGIC);
        Assert(!plansource->gpc.status.InShareTable());
        if (unlikely(plansource->magic != CACHEDPLANSOURCE_MAGIC)) {
            ereport(PANIC,
                  (errcode(ERRCODE_UNDEFINED_PSTATEMENT),
                   errmsg("In gpc commit stage, plansource has already been freed(%lu,%u,%u)",
                          u_sess->sess_ident.cn_sessid, u_sess->sess_ident.cn_timeline, u_sess->sess_ident.cn_nodeid)));
        }

        next_plansource = plansource->next_saved;
        bool has_lp = false;
#ifdef ENABLE_MULTIPLE_NODES
        has_lp = plansource->single_exec_node &&
                 plansource->gplan == NULL && plansource->cplan == NULL && plansource->stmt_name;
        if (has_lp) {
            has_lp = (lightProxy::locateLpByStmtName(plansource->stmt_name) != NULL);
        }
#endif
        if (!plansource->gpc.status.IsSharePlan() || (plansource->gplan == NULL && plansource->cplan)) {
            /* stream or private plan or cplan need put into ungpc_save_plan */
            plansource->is_saved = true;
            if (!plansource->is_support_gplan && plansource->gpc.status.IsSharePlan())
                plansource->gpc.status.SetKind(GPC_CPLAN);
            Assert (!plansource->gpc.status.IsSharePlan());
            plansource->gpc.status.SetLoc(GPC_SHARE_IN_LOCAL_UNGPC_PLAN_LIST);
            plansource->next_saved = u_sess->pcache_cxt.ungpc_saved_plan;
            u_sess->pcache_cxt.ungpc_saved_plan = plansource;
        } else if (!plansource->is_valid || (plansource->gplan && !plansource->gplan->is_valid)) {
            plansource->is_valid = false;
            plansource->is_saved = true;
            Assert (plansource->gpc.status.IsSharePlan());
            plansource->gpc.status.SetStatus(GPC_INVALID);
            plansource->gpc.status.SetLoc(GPC_SHARE_IN_LOCAL_SAVE_PLAN_LIST);
            plansource->next_saved = u_sess->pcache_cxt.first_saved_plan;
            u_sess->pcache_cxt.first_saved_plan = plansource;
        } else if (plansource->gplan == NULL && plansource->cplan == NULL && !has_lp) {
            /* get commit before create cachedplan or lp, not init gpckey. keep in first_saved_plan */
            plansource->is_saved = true;
            Assert (plansource->gpc.status.IsSharePlan());
            plansource->gpc.status.SetLoc(GPC_SHARE_IN_LOCAL_SAVE_PLAN_LIST);
            plansource->next_saved = u_sess->pcache_cxt.first_saved_plan;
            u_sess->pcache_cxt.first_saved_plan = plansource;
        } else {
            if (plansource->spi_signature.spi_key != INVALID_SPI_KEY) {
                Assert(!has_lp);
                Assert(plansource->gplan);
                Assert(plansource->is_support_gplan);
                g_instance.plan_cache->SPICommit(plansource);
            } else {
                PreparedStatement* ps = FetchPreparedStatement(plansource->stmt_name, true, false);
                if (unlikely(ps == NULL)) {
#ifdef MEMORY_CONTEXT_CHECKING
                    ereport(PANIC,
#else
                    ereport(ERROR,
#endif
                            (errcode(ERRCODE_UNDEFINED_PSTATEMENT),
                             errmsg("In gpc commit stage, fail to fetch prepare statement: %s.(%lu,%u,%u)", plansource->stmt_name,
                                    u_sess->sess_ident.cn_sessid, u_sess->sess_ident.cn_timeline, u_sess->sess_ident.cn_nodeid)));
                }
                TryStore(plansource, ps);
            }
        }

        plansource = next_plansource;
    }
}

void GlobalPlanCache::SPITryStore(CachedPlanSource* plansource, SPIPlanPtr spiplan, int nth)
{
    Assert (plansource != NULL);
    Assert (plansource->magic == CACHEDPLANSOURCE_MAGIC);
    Assert (!plansource->gpc.status.InShareTable());
    Assert (plansource->gpc.status.IsSharePlan());
    Assert (spiplan->saved);

    GPCKey* key = plansource->gpc.key;

    uint32 hashCode = GPCHashFunc((const void *) key, sizeof(*key));

    uint32 bucket_id = GetBucket(hashCode);
    Assert (bucket_id >= 0 && bucket_id < GPC_NUM_OF_BUCKETS);
    int lock_id = m_array[bucket_id].lockId;

    (void)LWLockAcquire(GetMainLWLockByIndex(lock_id), LW_EXCLUSIVE);
    MemoryContext oldcontext = MemoryContextSwitchTo(m_array[bucket_id].context);

    bool found = false;
    GPCEntry *entry = (GPCEntry *)hash_search_with_hash_value(m_array[bucket_id].hash_tbl,
                                                                (const void*)key, hashCode, HASH_ENTER, &found);
    if (entry == NULL) {
        MemoryContextSwitchTo(oldcontext);
        LWLockRelease(GetMainLWLockByIndex(lock_id));
        ereport(ERROR,
              (errcode(ERRCODE_UNDEFINED_PSTATEMENT),
               errmsg("store global plan source failed due to memory allocation failed")));
    }

    if (found == false) {
        /* Deep copy the query_string to the GPC entry's query_string */
        entry->key.query_string = key->query_string;
        entry->key.query_length = key->query_length;
        entry->key.spi_signature = key->spi_signature;
        /* Set the magic number. */
        entry->val.plansource = plansource;
        //off the link
        plansource->next_saved = NULL;
        //initialize the ref count .
        plansource->gpc.status.AddRefcount();

        m_array[bucket_id].count++;
        pg_atomic_fetch_add_u32((volatile uint32*)&plansource->gplan->global_refcount, 1);
        plansource->gplan->is_share = true;
        Assert(plansource->context->is_shared);
        MemoryContextSeal(plansource->context);
        Assert(plansource->query_context->is_shared);
        MemoryContextSeal(plansource->query_context);
        Assert(plansource->gplan->context->is_shared);
        MemoryContextSeal(plansource->gplan->context);
        plansource->gpc.status.SetLoc(GPC_SHARE_IN_SHARE_TABLE);

    } else {
        //some guys win.
        CachedPlanSource* newsource = entry->val.plansource;
        ListCell* n_cell = list_nth_cell(spiplan->plancache_list, nth);
        n_cell->data.ptr_value = (void*)newsource;
        newsource->gpc.status.AddRefcount();

        // purge old one.
        CN_GPC_LOG("drop cache plan in try store", plansource, 0);
        DropCachedPlan(plansource);
        CN_GPC_LOG("change to cache plan", newsource, 0);
    }

    MemoryContextSwitchTo(oldcontext);
    LWLockRelease(GetMainLWLockByIndex(lock_id));
}

void GlobalPlanCache::SPICommit(CachedPlanSource* plansource)
{
    Assert (u_sess->SPI_cxt.SPICacheTable != NULL);
    plpgsql_SPIPlanCacheEnt* entry = SPIPlanCacheTableLookup(plansource->spi_signature.spi_key);
    Assert(entry != NULL);
    Assert(entry->func_oid != InvalidOid);
    List* spiplan_list = entry->SPIplan_list;
    ListCell* cell = NULL;
    bool has_cachedplan = false;
    foreach(cell, spiplan_list) {
        SPIPlanPtr spi_plan = (SPIPlanPtr)lfirst(cell);
        if (spi_plan->id == plansource->spi_signature.spi_id) {
            SPITryStore(plansource, spi_plan, plansource->spi_signature.plansource_id);
            has_cachedplan = true;
            break;
        }
    }
    Assert(has_cachedplan);
    if (unlikely(has_cachedplan == false)) {
#ifdef MEMORY_CONTEXT_CHECKING
        ereport(PANIC,
#else
        ereport(ERROR,
#endif
                (errcode(ERRCODE_UNDEFINED_PSTATEMENT),
                 errmsg("In gpc spi finish stage, fail to get spi func: %u. hashkey: %u",
                        plansource->spi_signature.func_oid, plansource->spi_signature.spi_key)));
    }

#ifdef USE_ASSERT_CHECKING
    foreach(cell, spiplan_list) {
        SPIPlanPtr spi_plan = (SPIPlanPtr)lfirst(cell);
        ListCell* cl = NULL;
        if (list_length(spi_plan->plancache_list) == 0)
            continue;
        foreach(cl, spi_plan->plancache_list) {
            CachedPlanSource *cur = (CachedPlanSource*)(cl->data.ptr_value);
            Assert(cur->magic == CACHEDPLANSOURCE_MAGIC);
        }
    }
#endif
}

void GlobalPlanCache::RemovePlanCacheInSPIPlan(SPIPlanPtr plan)
{
    Assert (plan->magic == _SPI_PLAN_MAGIC);
    if (list_length(plan->plancache_list) > 0) {
        ListCell* cell = NULL;
        foreach(cell, plan->plancache_list) {
            CachedPlanSource* plansource = (CachedPlanSource*)lfirst(cell);
            if (plansource->gpc.status.InShareTable()) {
                Assert(plan->saved);
                CN_GPC_LOG("drop shared spi plan, subrefcount", plansource, 0);
                /* move plansource into invalid list if during delet func */
                if (u_sess->plsql_cxt.is_delete_function) {
                    GPCKey* gpckey = plansource->gpc.key;
                    uint32 hashCode = GPCHashFunc((const void *) gpckey, sizeof(*gpckey));
                    uint32 bucket_id = GetBucket(hashCode);
                    int lock_id = m_array[bucket_id].lockId;
                    (void)LWLockAcquire(GetMainLWLockByIndex(lock_id), LW_EXCLUSIVE);
                    if (!plansource->gpc.status.InShareTableInvalidList()) {
                        bool found = false;
                        (void)hash_search(m_array[bucket_id].hash_tbl, (void *)gpckey, HASH_REMOVE, &found);
                        m_array[bucket_id].count--;
                        AddInvalidList(plansource);
                    }
                    plansource->gpc.status.SubRefCount();
                    DropInvalid();
                    LWLockRelease(GetMainLWLockByIndex(lock_id));
                } else {
                    plansource->gpc.status.SubRefCount();
                }
            } else {
                CN_GPC_LOG("drop unshared spi plan", plansource, 0);
                DropCachedPlan(plansource);
            }
        }
    }
    if (plan->spi_key != INVALID_SPI_KEY)
        SPIPlanCacheTableDeletePlan(plan->spi_key, plan);
}

