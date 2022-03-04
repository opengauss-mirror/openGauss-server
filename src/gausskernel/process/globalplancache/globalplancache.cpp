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
#include "opfusion/opfusion.h"
#include "pgxc/groupmgr.h"
#include "pgxc/pgxcnode.h"
#include "utils/dynahash.h"
#include "utils/globalplancache.h"
#include "utils/memutils.h"
#include "utils/plancache.h"
#include "utils/syscache.h"
#include "utils/plpgsql.h"
#include "nodes/pg_list.h"
#include "commands/sqladvisor.h"

template void GlobalPlanCache::RemovePlanSource<ACTION_RECREATE>(CachedPlanSource* plansource, const char* stmt_name);

template void GlobalPlanCache::RemovePlanSource<ACTION_RELOAD>(CachedPlanSource* plansource, const char* stmt_name);

template void GlobalPlanCache::RemovePlanSource<ACTION_CN_RETRY>(CachedPlanSource* plansource, const char* stmt_name);

static bool has_diff_schema(const List *list1, const List *list2)
{
    const ListCell *cell = NULL;

    if (list2 == NIL) {
        return list1 != NULL;
    }
    foreach (cell, list1) {
        if (!list_member_oid(list2, lfirst_oid(cell))) {
            return true;
        }
    }
    return false;
}

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
    if (has_diff_schema(path1->schemas, path2->schemas)) {
        return false;
    }
    if (has_diff_schema(path2->schemas, path1->schemas)) {
        return false;
    }
    return true;
}

static bool GPCCompareParam(Oid* params1, Oid* params2, int paramNum)
{
    for (int i = 0; i < paramNum; i++) {
        if (params1[i] != params2[i]) {
            return false;
        }
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
        if (!GPCCompareParam(env1->param_types, env2->param_types, env1->num_params)) {
            return false;
        }
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

void GPCKeyDeepCopy(const GPCKey *srcGpckey, GPCKey *destGpckey)
{
    *destGpckey = *srcGpckey;
    if (srcGpckey->query_string)
        destGpckey->query_string = pstrdup(srcGpckey->query_string);

    if (destGpckey->env.num_params > 0) {
        destGpckey->env.param_types = (Oid*)palloc(sizeof(Oid) * destGpckey->env.num_params);
        errno_t rc = 0;
        rc = memcpy_s(destGpckey->env.param_types, sizeof(Oid) * destGpckey->env.num_params,
                      srcGpckey->env.param_types, sizeof(Oid) * destGpckey->env.num_params);
        securec_check(rc, "", "");
    }

    if (destGpckey->env.schema_name) {
        destGpckey->env.search_path = (struct OverrideSearchPath *)palloc(sizeof(struct OverrideSearchPath));
        *destGpckey->env.search_path = *srcGpckey->env.search_path;
        destGpckey->env.search_path->schemas = list_copy(srcGpckey->env.search_path->schemas);
    }
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

    m_array = (GPCHashCtl *) MemoryContextAllocZero(GLOBAL_PLANCACHE_MEMCONTEXT,
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
        m_array[i].context = AllocSetContextCreate(GLOBAL_PLANCACHE_MEMCONTEXT,
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
        entry->val.used_count = 0;
        INSTR_TIME_SET_CURRENT(entry->val.last_use_time);
        /* off the link */
        plansource->next_saved = NULL;
        plansource->is_checked_opfusion = true;
        if (plansource->opFusionObj != NULL) {
            OpFusion::SaveInGPC((OpFusion*)(plansource->opFusionObj));
        }
        /* initialize the ref count .*/
#ifdef ENABLE_MULTIPLE_NODES
        /* dn only count reference on cur_stmt_psrc, no prepare statement.
           cn count reference on prepare statement */
        if (IS_PGXC_COORDINATOR)
            plansource->gpc.status.AddRefcount();
#else
        plansource->gpc.status.AddRefcount();
#endif
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
        if (ps == NULL) {
            Assert (IS_PGXC_DATANODE);
            GPC_LOG("drop cache plan in try store", plansource, 0);
            DropCachedPlan(plansource);
        } else {
            CachedPlanSource* newsource = entry->val.plansource;
            ps->plansource = newsource;
            newsource->gpc.status.AddRefcount();
            INSTR_TIME_SET_CURRENT(entry->val.last_use_time);
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
    }

    MemoryContextSwitchTo(oldcontext);
    LWLockRelease(GetMainLWLockByIndex(lock_id));
    return true;
}

CachedPlanSource* GlobalPlanCache::Fetch(const char *query_string, uint32 query_len,
                                         int num_params, Oid* paramTypes, SPISign* spi_sign_ptr)
{
    GPCKey key;
    key.env.filled = false;
    key.query_string = query_string;
    key.query_length = query_len;
    EnvFill(&key.env, false);
    key.env.search_path = NULL;
    key.env.num_params = num_params;
    key.env.param_types = paramTypes;
    if (spi_sign_ptr != NULL)
        key.spi_signature = *spi_sign_ptr;
    else
        key.spi_signature = {(uint32)-1, 0, (uint32)-1, -1};

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
    } else {
        CachedPlanSource* psrc = entry->val.plansource;
        psrc->gpc.status.AddRefcount();
        if (!psrc->gpc.status.IsValid()) {
            MemoryContextSwitchTo(oldcontext);
            LWLockRelease(GetMainLWLockByIndex(lock_id));
            MoveIntoInvalidPlanList(psrc);
            psrc->gpc.status.SubRefCount();
            return NULL;
        }
        if (ENABLE_DN_GPC)
            u_sess->pcache_cxt.private_refcount++;
        pg_atomic_fetch_add_u32(&entry->val.used_count, 1);
        MemoryContextSwitchTo(oldcontext);
        LWLockRelease(GetMainLWLockByIndex(lock_id));
        return psrc;
    }

    return NULL;
}

void GlobalPlanCache::AddInvalidList(CachedPlanSource* plansource)
{
    (void)LWLockAcquire(GPCClearLock, LW_EXCLUSIVE);
    MemoryContext oldcontext = MemoryContextSwitchTo(GLOBAL_PLANCACHE_MEMCONTEXT);
    START_CRIT_SECTION();
    plansource->gpc.status.SetLoc(GPC_SHARE_IN_SHARE_TABLE_INVALID_LIST);
    m_invalid_list = dlappend(m_invalid_list, plansource);
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
            if (curr->gpc.status.RefCountZero()) {
                Assert(curr->next_saved == NULL);
                DListCell *next = cell->next;
                GPC_LOG("drop invalid shared plancache", curr, curr->stmt_name);
                m_invalid_list = dlist_delete_cell(m_invalid_list, cell, false);
                DropCachedPlanInternal(curr);
                curr->magic = 0;
                MemoryContextUnSeal(curr->context);
                MemoryContextUnSeal(curr->query_context);
                if (curr->opFusionObj) {
                    OpFusion::DropGlobalOpfusion((OpFusion*)(curr->opFusionObj));
                }
                MemoryContextDelete(curr->context);

                cell = next;
            } else {
                cell = cell->next;
            }
        }
    }
    LWLockRelease(GPCClearLock);
}

template<PlansourceInvalidAction action_type>
void GlobalPlanCache::RemovePlanSource(CachedPlanSource* plansource, const char* stmt_name)
{
    Assert(plansource->magic == CACHEDPLANSOURCE_MAGIC);

    if(plansource->gpc.status.InShareTable()) {
        GPCKey* key = plansource->gpc.key;
        uint32 hashCode = GPCHashFunc((const void *) key, sizeof(*key));
        uint32 bucket_id = GetBucket(hashCode);
        int lock_id = m_array[bucket_id].lockId;
        (void)LWLockAcquire(GetMainLWLockByIndex(lock_id), LW_EXCLUSIVE);
        if (plansource->gpc.status.InShareTableInvalidList() == false) {
            bool found = false;
            hash_search(m_array[bucket_id].hash_tbl, (void *)key, HASH_REMOVE, &found);
            if (unlikely(found == false))
                elog(PANIC, "should found plan in gpc when RemovePlanSource");
            m_array[bucket_id].count--;
            AddInvalidList(plansource);
        }
        /* Has hold refcount for ACTION_RECREATE */
        if (action_type == ACTION_RECREATE)
            plansource->gpc.status.SubRefCount();
        DropInvalid();
        LWLockRelease(GetMainLWLockByIndex(lock_id));

        if (ENABLE_DN_GPC) {
            u_sess->pcache_cxt.private_refcount--;
            if (u_sess->pcache_cxt.private_refcount != 0)
                elog(PANIC, "wrong refcount in subrefcount");
        }

        if (action_type == ACTION_RELOAD) {
            /* clear Datanode statements */
#ifdef ENABLE_MULTIPLE_NODES
            if (plansource->gplan != NULL)
                GPCCleanDatanodeStatement(plansource->gplan->dn_stmt_num, stmt_name);
            else
                GPCDropLPIfNecessary(stmt_name, true, true, NULL);
#endif
        }
        
    } else {
        if (action_type == ACTION_RECREATE) {
            GPC_LOG("remove private plansource", plansource, plansource->stmt_name);
            DropCachedPlan(plansource);
        } else {
            CN_GPC_LOG("invalid plan", plansource, stmt_name);
            plansource->gpc.status.SetStatus(GPC_INVALID);
            plansource->is_valid = false;
            if (plansource->gplan) {
                plansource->gplan->is_valid = false;
                Assert(!plansource->gplan->isShared());
            }
            if (action_type == ACTION_RELOAD) {
                DropCachedPlanInternal(plansource);
                if (plansource->gplan == NULL)
                    GPCDropLPIfNecessary(stmt_name, true, true, NULL);
            }
        }
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


bool GlobalPlanCache::CheckRecreateCachePlan(CachedPlanSource* psrc, bool* hasGetLock)
{
    /*
     * Start up a transaction command so we can run parse analysis etc. (Note
     * that this will normally change current memory context.) Nothing happens
     * if we are already in one.
     */
    start_xact_command();
    Assert(psrc->magic == CACHEDPLANSOURCE_MAGIC);
    /* get lock before check plan is valid or not, release it if need recreate plan */
    if (psrc->gpc.status.InShareTable()) {
        AcquirePlannerLocks(psrc->query_list, true);
        if (psrc->gplan) {
            AcquireExecutorLocks(psrc->gplan->stmt_list, true);
        }
        *hasGetLock = true;
    }

#ifdef ENABLE_MULTIPLE_NODES
    if (IS_PGXC_COORDINATOR && !psrc->gpc.status.InShareTable()) {
        return false;
    }
#else
    if (!psrc->gpc.status.InShareTable()) {
        return false;
    }
#endif

    if (u_sess->pcache_cxt.gpc_in_ddl == true) {
        return true;
    }
    if (!psrc->gpc.status.IsValid()) {
        return true;
    }
    if (psrc->dependsOnRole && (psrc->rewriteRoleId != GetUserId())) {
        return true;
    }
    if ((psrc->gplan != NULL && TransactionIdIsValid(psrc->gplan->saved_xmin))) {
        return true;
    }

    if (psrc->search_path && !OverrideSearchPathMatchesCurrent(psrc->search_path)) {
        return true;
    }

    return false;
}

bool GlobalPlanCache::CheckRecreateSPICachePlan(SPIPlanPtr spi_plan)
{
    ListCell* cell = NULL;
    Assert(spi_plan->magic == _SPI_PLAN_MAGIC);
    foreach(cell, spi_plan->plancache_list) {
        CachedPlanSource* plansource = (CachedPlanSource*)lfirst(cell);
        bool hasGetLock = false;
        if (CheckRecreateCachePlan(plansource, &hasGetLock)) {
            if (hasGetLock) {
                AcquirePlannerLocks(plansource->query_list, false);
                if (plansource->gplan) {
                    AcquireExecutorLocks(plansource->gplan->stmt_list, false);
                }
            }
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
        RecreateCachePlan(oldsource, NULL, NULL, spiplan, cell, false);
    }
    /* pop error context stack */
    t_thrd.log_cxt.error_context_stack = spi_err_context.previous;
    Assert(SPIPlanCacheTableLookup(u_sess->SPI_cxt._current->spi_hash_key));
}

void GlobalPlanCache::MoveIntoInvalidPlanList(CachedPlanSource* psrc)
{
    if (psrc->gpc.status.InShareTable() && !psrc->gpc.status.IsValid()) {
        GPCKey* key = psrc->gpc.key;
        uint32 hashCode = GPCHashFunc((const void *) key, sizeof(*key));
        uint32 bucket_id = GetBucket(hashCode);
        int lock_id = m_array[bucket_id].lockId;
        (void)LWLockAcquire(GetMainLWLockByIndex(lock_id), LW_EXCLUSIVE);
        if (psrc->gpc.status.InShareTableInvalidList() == false) {
            bool found = false;
            hash_search(m_array[bucket_id].hash_tbl, (void *)key, HASH_REMOVE, &found);
            if (unlikely(found == false))
                elog(PANIC, "should found plan in gpc");
            m_array[bucket_id].count--;
            AddInvalidList(psrc);
        }
        LWLockRelease(GetMainLWLockByIndex(lock_id));
    }
}

void GlobalPlanCache::RecreateCachePlan(CachedPlanSource* oldsource, const char* stmt_name, PreparedStatement *entry,
                                        SPIPlanPtr spiplan, ListCell* spiplanCell, bool hasGetLock)
{
    GPC_LOG("recreate plan", oldsource, oldsource->stmt_name);
    /* these operator may throw error, make sure shared plan is invalid first */
    CachedPlanSource *newsource = NULL;
    PG_TRY();
    {
        if (hasGetLock) {
            AcquirePlannerLocks(oldsource->query_list, false);
            if (oldsource->gplan) {
                AcquireExecutorLocks(oldsource->gplan->stmt_list, false);
            }
        }
        newsource = CopyCachedPlan(oldsource, true);
        MemoryContext oldcxt = MemoryContextSwitchTo(newsource->context);
        newsource->stream_enabled = IsStreamSupport();
        u_sess->exec_cxt.CurrentOpFusionObj = NULL;
        Assert (oldsource->gpc.status.IsSharePlan());
        newsource->gpc.status.ShareInit();
        // If the planSource is set to invalid, the AST must be analyzed again
        // because the meta has changed.
        newsource->is_valid = false;
        bool has_lp = false;

        if (spiplan != NULL) {
            t_thrd.log_cxt.error_context_stack->arg = (void *)newsource->query_string;
            newsource->spi_signature = oldsource->spi_signature;
            newsource->parserSetup = spiplan->parserSetup;
            newsource->parserSetupArg = spiplan->parserSetupArg;
        } else if (IS_PGXC_DATANODE) {
            newsource->stmt_name = pstrdup(stmt_name);
        } else {
            newsource->stmt_name = pstrdup(stmt_name);
#ifdef ENABLE_MULTIPLE_NODES
            has_lp = (oldsource->single_exec_node != NULL && oldsource->gplan == NULL && oldsource->cplan == NULL);
            /* clean session's datanode statment on cn */
            if (has_lp) {
                /* no lp in newsource, delete old lp */
                GPCDropLPIfNecessary(stmt_name, false, true, NULL);
            } else if (oldsource->gplan != NULL) {
                /* Close any active planned Datanode statements, recreate in BuildCachedPlan later */
                GPCCleanDatanodeStatement(oldsource->gplan->dn_stmt_num, stmt_name);
            }
#endif
        }
        (void)RevalidateCachedQuery(newsource, has_lp);
        MemoryContextSwitchTo(oldcxt);
    }
    PG_CATCH();
    {
        /* catch only move invalid plansource into gpc invalid list when error occurs */
        MoveIntoInvalidPlanList(oldsource);
        PG_RE_THROW();
    }
    PG_END_TRY();

    /* newsource has reference on session, forget resource owner */
    ResourceOwnerForgetGMemContext(t_thrd.utils_cxt.TopTransactionResourceOwner, newsource->context);
    newsource->next_saved = u_sess->pcache_cxt.first_saved_plan;
    u_sess->pcache_cxt.first_saved_plan = newsource;
    newsource->is_saved = true;
    newsource->gpc.status.SetLoc(GPC_SHARE_IN_LOCAL_SAVE_PLAN_LIST);
    if (spiplan != NULL)
        spiplanCell->data.ptr_value = newsource;
    else {
#ifdef ENABLE_MULTIPLE_NODES
        if (IS_PGXC_DATANODE)
            u_sess->pcache_cxt.cur_stmt_psrc = newsource;
        else
            entry->plansource = newsource;
#else
        entry->plansource = newsource;
#endif
    }

    RemovePlanSource<ACTION_RECREATE>(oldsource, stmt_name);
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
    CleanSessGPCPtr(u_sess);
    if (u_sess->pcache_cxt.private_refcount != 0) {
        elog(PANIC, "wrong refcount");
    }
    while (plansource != NULL) {
        Assert(plansource->magic == CACHEDPLANSOURCE_MAGIC);
        Assert(!plansource->gpc.status.InShareTable());
        if (unlikely(plansource->magic != CACHEDPLANSOURCE_MAGIC)) {
            ereport(PANIC,
                  (errcode(ERRCODE_UNDEFINED_PSTATEMENT),
                   errmsg("In gpc commit stage, plansource has already been freed")));
        }

        next_plansource = plansource->next_saved;
        if (plansource->gpc.status.IsPrivatePlan()) {
            /* private plan has reference on pointer like unname_stmt_psrc or spiplan */
            GPC_LOG("invalid plan in commit", plansource, plansource->stmt_name);
            plansource->is_valid = false;
            plansource->next_saved = NULL;
            plansource->gpc.status.SetLoc(GPC_SHARE_IN_PREPARE_STATEMENT);
            plansource->gpc.status.SetStatus(GPC_INVALID);
        } else if (!plansource->is_valid || plansource->gplan == NULL || !plansource->is_support_gplan) {
            GPC_LOG("drop plan in commit", plansource, plansource->stmt_name);
            /* no prepare statement on dn, so we just drop shared plansource if can't save it in gpc, in case leak */
            DropCachedPlan(plansource);
        } else {
            TryStore(plansource, NULL);
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
                   errmsg("In gpc commit stage, plansource has already been freed")));
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
                             errmsg("In gpc commit stage, fail to fetch prepare statement:%s", plansource->stmt_name)));
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
        INSTR_TIME_SET_CURRENT(entry->val.last_use_time);
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

void GlobalPlanCache::CleanUpByTime()
{
    List *gpckey_list = NULL;
    const int  maxlen_gpckey_list = 100;
    instr_time curTime;
    INSTR_TIME_SET_CURRENT(curTime);
    for (uint32 bucket_id = 0; bucket_id < GPC_NUM_OF_BUCKETS; bucket_id++)
    {
        int lock_id = m_array[bucket_id].lockId;

        /* Step 1: Try to find the code plan cache */
        LWLockAcquire(GetMainLWLockByIndex(lock_id), LW_SHARED);
        if (m_array[bucket_id].count == 0) {
            LWLockRelease(GetMainLWLockByIndex(lock_id));
            continue;
        }
        HASH_SEQ_STATUS hash_seq;
        GPCEntry *entry = NULL;
        CachedPlanSource* cur_plansource = NULL;

        hash_seq_init(&hash_seq, m_array[bucket_id].hash_tbl);
        while ((entry = (GPCEntry*)hash_seq_search(&hash_seq)) != NULL) {
            if (entry->val.used_count > 0) {
                entry->val.last_use_time = curTime;
                entry->val.used_count = 0;
                continue;
            }
            cur_plansource = entry->val.plansource;
            if (cur_plansource->gpc.status.RefCountZero() &&
                INSTR_TIME_GET_DOUBLE(curTime) - INSTR_TIME_GET_DOUBLE(entry->val.last_use_time) >
                u_sess->attr.attr_common.gpc_clean_timeout) {
                GPCKey  *dest_gpckey = (GPCKey *)palloc(sizeof(GPCKey));
                GPCKeyDeepCopy(&entry->key, dest_gpckey);
                gpckey_list = lappend(gpckey_list, dest_gpckey);

                /* should not be long */
                if (gpckey_list->length >= maxlen_gpckey_list) {
                    hash_seq_term(&hash_seq);
                    break;
                }
            }
        }
        LWLockRelease(GetMainLWLockByIndex(lock_id));

        /* Step 2: Try to remove plan cache */
        if (gpckey_list && list_length(gpckey_list) > 0) {
            LWLockAcquire(GetMainLWLockByIndex(lock_id), LW_EXCLUSIVE);
            ListCell* l = NULL;
            foreach(l, gpckey_list) {
                bool found = false;
                GPCKey  *key = (GPCKey *)lfirst(l);
                GPCEntry *entry = NULL;
                entry = (GPCEntry *)hash_search(m_array[bucket_id].hash_tbl, (void *)key, HASH_FIND, &found);
                if (entry) {
                    cur_plansource = entry->val.plansource;
                    if (cur_plansource->gpc.status.RefCountZero() &&
                        INSTR_TIME_GET_DOUBLE(curTime) - INSTR_TIME_GET_DOUBLE(entry->val.last_use_time) >
                        u_sess->attr.attr_common.gpc_clean_timeout) {
                        GPC_LOG("drop shared plancache by time", cur_plansource, cur_plansource->stmt_name);
                        DropCachedPlanInternal(cur_plansource);
                        hash_search(m_array[bucket_id].hash_tbl, (void *) key, HASH_REMOVE, &found);
                        cur_plansource->magic = 0;
                        MemoryContextUnSeal(cur_plansource->context);
                        MemoryContextUnSeal(cur_plansource->query_context);
                        if (cur_plansource->opFusionObj) {
                            OpFusion::DropGlobalOpfusion((OpFusion*)(cur_plansource->opFusionObj));
                        }
                        MemoryContextDelete(cur_plansource->context);
                        m_array[bucket_id].count--;
                    }
                }
                pfree((void *)key->query_string);
                pfree_ext(key->env.param_types);
                pfree_ext(key->env.search_path->schemas);
                pfree_ext(key->env.search_path);
                pfree_ext(key);
            }
            LWLockRelease(GetMainLWLockByIndex(lock_id));

            list_free_ext(gpckey_list);
        }
    }
}

void CleanSessGPCPtr(knl_session_context* currentSession)
{
    CachedPlanSource *psrc = currentSession->pcache_cxt.cur_stmt_psrc;
    currentSession->pcache_cxt.cur_stmt_psrc = NULL;
    if (psrc && psrc->magic != CACHEDPLANSOURCE_MAGIC)
        elog(PANIC, "cur psrc wrong");
    if (psrc && psrc->gpc.status.InShareTable()) {
        psrc->gpc.status.SubRefCount();
        currentSession->pcache_cxt.private_refcount--;
    }
    if (unlikely(currentSession->pcache_cxt.private_refcount != 0))
        elog(PANIC, "wrong refcount");
}

void CleanSessionGPCDetach(knl_session_context* currentSession)
{
    if (IS_PGXC_COORDINATOR)
        return;
    if (currentSession->pcache_cxt.cur_stmt_psrc != NULL) {
        elog(PANIC, "session's cur_stmt_psrc should be null when detach");
    }

    CachedPlanSource* plansource = currentSession->pcache_cxt.first_saved_plan;
    while (plansource != NULL) {
        /*
         * When turing on the enable_global_plancache, there are some cases that
         * we cannot insert the plancache in the shared HTAB. No Prepare Statement
         * on DN, so we can just drop shared plan and wait for next parse message 
         * to create it again.
         */
        CachedPlanSource* next_plansource = plansource->next_saved;
        if (plansource->gpc.status.IsPrivatePlan()) {
            /* private plan has reference on pointer like unname_stmt_psrc or spiplan */
            GPC_LOG("invalid plan in sess detach", plansource, plansource->stmt_name);
            plansource->is_valid = false;
            plansource->next_saved = NULL;
            plansource->gpc.status.SetLoc(GPC_SHARE_IN_PREPARE_STATEMENT);
            plansource->gpc.status.SetStatus(GPC_INVALID);
        } else {
            DropCachedPlan(plansource);
        }
        plansource = next_plansource;
    }

    currentSession->pcache_cxt.first_saved_plan = NULL;
    currentSession->pcache_cxt.gpc_in_ddl = false;
}

