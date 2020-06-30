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
#include "optimizer/nodegroups.h"
#include "pgxc/groupmgr.h"
#include "pgxc/pgxcnode.h"
#include "utils/dynahash.h"
#include "utils/globalplancache.h"
#include "utils/memutils.h"
#include "utils/plancache.h"
#include "utils/syscache.h"

extern List *RevalidateCachedQuery(CachedPlanSource *plansource);

/* global plan cache */
static void GPCReleaseCachedPlan(CachedPlan *plan)
{
    Assert(plan->magic == CACHEDPLAN_MAGIC);

    /* Mark it no longer valid */
    plan->magic = 0;

    MemoryContextDelete(plan->context);
}

typedef struct TimelineEntry {
    uint32 id;      // CN index
    uint32 timeline;
} TimelineEntry;

GlobalPlanCache::GlobalPlanCache()
{
    PlanInit();
    PrepareInit();

    HASHCTL ctl_func;
    errno_t rc;
    rc = memset_s(&ctl_func, sizeof(ctl_func), 0, sizeof(ctl_func));
    securec_check_c(rc, "\0", "\0");

    ctl_func.keysize = sizeof(uint32);
    ctl_func.entrysize = sizeof(TimelineEntry);
    ctl_func.hcxt = g_instance.cache_cxt.global_cache_mem;
    m_cn_timeline =
        hash_create("cn_timeline", 64, &ctl_func, HASH_ELEM | HASH_SHRCTX);
}

/*
 * @Description: generate the env the plancache belongs to 
 * @in num: void
 * @return - GPCEnv
*/
GPCEnv *GlobalPlanCache::EnvCreate()
{
    GPCEnv *environment = NULL;

    MemoryContext env_context = AllocSetContextCreate(g_instance.cache_cxt.global_cache_mem,
                                                      "CachedEnvironmentContext",
                                                      ALLOCSET_SMALL_MINSIZE,
                                                      ALLOCSET_SMALL_INITSIZE,
                                                      ALLOCSET_DEFAULT_MAXSIZE,
                                                      SHARED_CONTEXT);

    MemoryContext oldcxt = MemoryContextSwitchTo(env_context);

    environment = (GPCEnv *) palloc0(sizeof(GPCEnv));
    environment->filled = false;
    environment->env_signature_pgxc = 0;
    environment->env_signature = 0;
    environment->env_signature2 = 0;
    environment->globalplancacheentry = NULL;
    environment->plansource = NULL;
    environment->context = env_context;
    environment->memory_size = 0;

    MemoryContextSwitchTo(oldcxt);

    return environment;
}

static void GPCFillClassicEnvSignatures(GPCEnv *env)
{
    env->env_signature = 0;
    env->env_signature |= u_sess->attr.attr_sql.enable_fast_numeric;
    env->env_signature |= u_sess->attr.attr_sql.enable_global_stats << 1;
#ifdef ENABLE_MULTIPLE_NODES
    env->env_signature |= u_sess->attr.attr_sql.enable_hdfs_predicate_pushdown << 2;
#endif
    env->env_signature |= u_sess->attr.attr_sql.enable_absolute_tablespace << 3;
#ifdef ENABLE_MULTIPLE_NODES
    env->env_signature |= u_sess->attr.attr_sql.enable_hadoop_env << 4;
#endif
    env->env_signature |= u_sess->attr.attr_sql.enable_valuepartition_pruning << 5;
#ifdef ENABLE_MULTIPLE_NODES
    env->env_signature |= u_sess->attr.attr_sql.enable_constraint_optimization << 6;
#endif
    env->env_signature |= u_sess->attr.attr_sql.enable_bloom_filter << 7;
    env->env_signature |= u_sess->attr.attr_sql.enable_codegen << 8;
    env->env_signature |= u_sess->attr.attr_sql.enable_codegen_print << 9;
    env->env_signature |= u_sess->attr.attr_sql.enable_seqscan << 10;
    env->env_signature |= u_sess->attr.attr_sql.enable_indexscan << 11;
    env->env_signature |= u_sess->attr.attr_sql.enable_indexonlyscan << 12;
    env->env_signature |= u_sess->attr.attr_sql.enable_bitmapscan << 13;
    env->env_signature |= u_sess->attr.attr_sql.force_bitmapand << 14;
    env->env_signature |= u_sess->attr.attr_sql.enable_tidscan << 15;
    env->env_signature |= u_sess->attr.attr_sql.enable_sort << 16;
    env->env_signature |= u_sess->attr.attr_sql.enable_compress_spill << 17;
    env->env_signature |= u_sess->attr.attr_sql.enable_hashagg << 18;
    env->env_signature |= u_sess->attr.attr_sql.enable_material << 19;
    env->env_signature |= u_sess->attr.attr_sql.enable_nestloop << 20;
    env->env_signature |= u_sess->attr.attr_sql.enable_mergejoin << 21;
    env->env_signature |= u_sess->attr.attr_sql.enable_hashjoin << 22;
    env->env_signature |= u_sess->attr.attr_sql.enable_index_nestloop << 23;
    env->env_signature |= u_sess->attr.attr_sql.enable_nodegroup_debug << 24;
    env->env_signature |= u_sess->attr.attr_sql.enable_partitionwise << 25;
    env->env_signature |= u_sess->attr.attr_sql.enable_kill_query << 26;
#ifdef ENABLE_MULTIPLE_NODES        
    env->env_signature |= u_sess->attr.attr_sql.agg_redistribute_enhancement << 27;
#endif
    env->env_signature |= u_sess->attr.attr_sql.enable_broadcast << 28;
#ifdef ENABLE_MULTIPLE_NODES
    env->env_signature |= g_instance.attr.attr_sql.enable_orc_cache << 29;
    env->env_signature |= u_sess->attr.attr_sql.acceleration_with_compute_pool << 30;
#endif
    env->env_signature |= u_sess->attr.attr_sql.enable_extrapolation_stats << 31;
}


/*
 * @Description:Fill in the environment signatures which are bitmaps for the boolean type GUC parameters
 * @in num: GPCEnv
 * @return - void
*/
static void GPCFillEnvSignatures(GPCEnv *env)
{
    /* We should only call this function if env is not NULL and it's not filled */
    Assert(env && !env->filled);
#ifdef PGXC
    env->env_signature_pgxc = 0;
    env->env_signature_pgxc |= g_instance.attr.attr_sql.string_hash_compatible;
#ifdef ENABLE_MULTIPLE_NODES    
    env->env_signature_pgxc |= u_sess->attr.attr_sql.enable_remotejoin << 1;
    env->env_signature_pgxc |= u_sess->attr.attr_sql.enable_fast_query_shipping << 2;
    env->env_signature_pgxc |= u_sess->attr.attr_sql.enable_remotegroup << 3;
    env->env_signature_pgxc |= u_sess->attr.attr_sql.enable_remotesort << 4;
    env->env_signature_pgxc |= u_sess->attr.attr_sql.enable_remotelimit << 5;
    env->env_signature_pgxc |= u_sess->attr.attr_sql.gtm_backup_barrier << 6;
    env->env_signature_pgxc |= u_sess->attr.attr_sql.enable_stream_operator << 7;
#endif    
    env->env_signature_pgxc |= u_sess->attr.attr_sql.enable_vector_engine << 8;
    env->env_signature_pgxc |= u_sess->attr.attr_sql.enable_force_vector_engine << 9;
#ifdef ENABLE_MULTIPLE_NODES 
    env->env_signature_pgxc |= u_sess->attr.attr_sql.enable_random_datanode << 10;
    env->env_signature_pgxc |= u_sess->attr.attr_sql.enable_fstream << 11;
#endif
#endif
    GPCFillClassicEnvSignatures(env);
    env->env_signature2 = 0;
    env->env_signature2 |= u_sess->attr.attr_sql.enable_geqo;
    env->env_signature2 |= u_sess->attr.attr_sql.td_compatible_truncation << 1;
    env->env_signature2 |= u_sess->attr.attr_sql.enable_upgrade_merge_lock_mode << 2;
    env->env_signature2 |= u_sess->attr.attr_sql.enforce_a_behavior << 3;
    env->env_signature2 |= u_sess->attr.attr_sql.standard_conforming_strings << 4;

    /* some new GUC parameters which affect the plan */
#ifdef ENABLE_MULTIPLE_NODES 
    env->env_signature2 |= u_sess->attr.attr_sql.enable_stream_concurrent_update << 5;
    env->env_signature2 |= u_sess->attr.attr_sql.enable_stream_recursive << 6;
#endif    
    env->env_signature2 |= u_sess->attr.attr_sql.enable_change_hjcost << 7;
    env->env_signature2 |= u_sess->attr.attr_sql.enable_analyze_check << 8;
    env->env_signature2 |= u_sess->attr.attr_sql.enable_sonic_hashagg << 9;
    env->env_signature2 |= u_sess->attr.attr_sql.enable_sonic_hashjoin << 10;
    env->env_signature2 |= u_sess->attr.attr_sql.enable_sonic_optspill << 11;
#ifdef ENABLE_MULTIPLE_NODES
    env->env_signature2 |= u_sess->attr.attr_sql.enable_csqual_pushdown << 13;
#endif
    env->env_signature2 |= u_sess->attr.attr_sql.enable_pbe_optimization << 14;
    env->env_signature2 |= u_sess->attr.attr_sql.enable_light_proxy << 15;
    env->env_signature2 |= u_sess->attr.attr_sql.enable_early_free << 16;
    env->env_signature2 |= u_sess->attr.attr_sql.enable_opfusion << 17;
}

void GlobalPlanCache::GetSchemaName(GPCEnv *env)
{
    /* get schema name */
    if (u_sess->attr.attr_common.namespace_current_schema) {
        int rc = memcpy_s(env->schema_name,
                          NAMEDATALEN,
                          u_sess->attr.attr_common.namespace_current_schema,
                          strlen(u_sess->attr.attr_common.namespace_current_schema));
        securec_check(rc, "", "");
    } else {
        env->schema_name[0] = '\0';
    }
}

void GlobalPlanCache::EnvFill(GPCEnv *env)
{
    /* We should only call this function if env is not NULL and it's not filled */
    Assert(env && !env->filled);
    GPCFillEnvSignatures(env);
#ifdef ENABLE_MULTIPLE_NODES
    env->best_agg_plan = u_sess->attr.attr_sql.best_agg_plan;
    env->query_dop_tmp = u_sess->attr.attr_sql.query_dop_tmp;
#endif
#ifdef DEBUG_BOUNDED_SORT
    env->optimize_bounded_sort = optimize_bounded_sort;
#endif
    env->rewrite_rule = u_sess->attr.attr_sql.rewrite_rule;
    env->codegen_strategy = u_sess->attr.attr_sql.codegen_strategy;
    env->plan_mode_seed = u_sess->attr.attr_sql.plan_mode_seed;

    MemoryContext old_context = MemoryContextSwitchTo(env->context);
    env->expected_computing_nodegroup = pstrdup(u_sess->attr.attr_sql.expected_computing_nodegroup);
    env->default_storage_nodegroup = pstrdup(u_sess->attr.attr_sql.default_storage_nodegroup);
    MemoryContextSwitchTo(old_context);

    env->effective_cache_size = u_sess->attr.attr_sql.effective_cache_size;
    env->codegen_cost_threshold = u_sess->attr.attr_sql.codegen_cost_threshold;
    env->seq_page_cost = u_sess->attr.attr_sql.seq_page_cost;
    env->random_page_cost = u_sess->attr.attr_sql.random_page_cost;
    env->cpu_tuple_cost = u_sess->attr.attr_sql.cpu_tuple_cost;
    env->allocate_mem_cost = u_sess->attr.attr_sql.allocate_mem_cost;
    env->cpu_index_tuple_cost = u_sess->attr.attr_sql.cpu_index_tuple_cost;
    env->cpu_operator_cost = u_sess->attr.attr_sql.cpu_operator_cost;
#ifdef ENABLE_MULTIPLE_NODES
    env->stream_multiple = u_sess->attr.attr_sql.stream_multiple;
#endif
    env->geqo_threshold = u_sess->attr.attr_sql.geqo_threshold;
    env->Geqo_effort = u_sess->attr.attr_sql.Geqo_effort;
    env->Geqo_pool_size = u_sess->attr.attr_sql.Geqo_pool_size;
    env->Geqo_generations = u_sess->attr.attr_sql.Geqo_generations;
    env->Geqo_selection_bias = u_sess->attr.attr_sql.Geqo_selection_bias;
    env->Geqo_seed = u_sess->attr.attr_sql.Geqo_seed;
    env->default_statistics_target = u_sess->attr.attr_sql.default_statistics_target;
    env->from_collapse_limit = u_sess->attr.attr_sql.from_collapse_limit;
    env->join_collapse_limit = u_sess->attr.attr_sql.join_collapse_limit;
    env->cost_param = u_sess->attr.attr_sql.cost_param;
#ifdef ENABLE_MULTIPLE_NODES
    env->schedule_splits_threshold = u_sess->attr.attr_sql.schedule_splits_threshold;
#endif
    env->hashagg_table_size = u_sess->attr.attr_sql.hashagg_table_size;
    env->cursor_tuple_fraction = u_sess->attr.attr_sql.cursor_tuple_fraction;
    env->constraint_exclusion = u_sess->attr.attr_sql.constraint_exclusion;
    env->behavior_compat_flags = u_sess->utils_cxt.behavior_compat_flags;
    env->datestyle = u_sess->time_cxt.DateStyle;
    env->dateorder = u_sess->time_cxt.DateOrder;
    env->filled = true;

    /* new GUC parameters which affect the plan */
    env->qrw_inlist2join_optmode = u_sess->opt_cxt.qrw_inlist2join_optmode;
    env->skew_strategy_store = u_sess->attr.attr_sql.skew_strategy_store;

    env->database_id = u_sess->proc_cxt.MyDatabaseId;
    GetSchemaName(env);
}

static bool GPCCheckOtherEnvSignature(GPCEnv *env)
{
    bool is_same = false;
    is_same = (env->database_id == u_sess->proc_cxt.MyDatabaseId &&
#ifdef ENABLE_MULTIPLE_NODES
               env->query_dop_tmp == u_sess->attr.attr_sql.query_dop_tmp &&
#endif
               env->rewrite_rule == u_sess->attr.attr_sql.rewrite_rule &&
               env->codegen_strategy == u_sess->attr.attr_sql.codegen_strategy &&
               env->plan_mode_seed == u_sess->attr.attr_sql.plan_mode_seed &&
               env->effective_cache_size == u_sess->attr.attr_sql.effective_cache_size &&
               env->codegen_cost_threshold == u_sess->attr.attr_sql.codegen_cost_threshold &&
               env->seq_page_cost == u_sess->attr.attr_sql.seq_page_cost &&
               env->random_page_cost == u_sess->attr.attr_sql.random_page_cost &&
               env->cpu_tuple_cost == u_sess->attr.attr_sql.cpu_tuple_cost &&
               env->allocate_mem_cost == u_sess->attr.attr_sql.allocate_mem_cost &&
               env->cpu_index_tuple_cost == u_sess->attr.attr_sql.cpu_index_tuple_cost &&
               env->cpu_operator_cost == u_sess->attr.attr_sql.cpu_operator_cost &&
               env->stream_multiple == u_sess->attr.attr_sql.stream_multiple &&
               env->geqo_threshold == u_sess->attr.attr_sql.geqo_threshold &&
               env->Geqo_effort == u_sess->attr.attr_sql.Geqo_effort &&
               env->Geqo_pool_size == u_sess->attr.attr_sql.Geqo_pool_size &&
               env->Geqo_generations == u_sess->attr.attr_sql.Geqo_generations &&
               env->Geqo_selection_bias == u_sess->attr.attr_sql.Geqo_selection_bias &&
               env->Geqo_seed == u_sess->attr.attr_sql.Geqo_seed &&
               env->default_statistics_target == u_sess->attr.attr_sql.default_statistics_target &&
               env->from_collapse_limit == u_sess->attr.attr_sql.from_collapse_limit &&
               env->join_collapse_limit == u_sess->attr.attr_sql.join_collapse_limit &&
               env->cost_param == u_sess->attr.attr_sql.cost_param &&
               env->schedule_splits_threshold == u_sess->attr.attr_sql.schedule_splits_threshold &&
               env->hashagg_table_size == u_sess->attr.attr_sql.hashagg_table_size &&
               env->cursor_tuple_fraction == u_sess->attr.attr_sql.cursor_tuple_fraction &&
               env->constraint_exclusion == u_sess->attr.attr_sql.constraint_exclusion &&
               env->behavior_compat_flags == u_sess->utils_cxt.behavior_compat_flags &&
               env->datestyle == u_sess->time_cxt.DateStyle &&
               env->dateorder == u_sess->time_cxt.DateOrder &&
               env->qrw_inlist2join_optmode == u_sess->opt_cxt.qrw_inlist2join_optmode &&
               env->skew_strategy_store == u_sess->attr.attr_sql.skew_strategy_store);
    return is_same;
}
/* 
 * Return false when the given compilation environment matches the current
 * session compilation environment, mainly compares GUC parameter settings.
 */
static bool GPCCompareEnvSignature(GPCEnv *env)
{
    // OverrideSearchPath *currPath;
    GPCEnv sessEnv;
    bool diff = true;
    sessEnv.filled = false;
    GPCFillEnvSignatures(&sessEnv);
    
    // If the cached env is not filled, we don't have a match
    if (env->filled &&
#ifdef PGXC
        env->env_signature_pgxc == sessEnv.env_signature_pgxc &&
        env->best_agg_plan == u_sess->attr.attr_sql.best_agg_plan &&
#endif
#ifdef DEBUG_BOUNDED_SORT
        env->optimize_bounded_sort == optimize_bounded_sort &&
#endif
        env->env_signature == sessEnv.env_signature &&
        env->env_signature2 == sessEnv.env_signature2 &&
        GPCCheckOtherEnvSignature(env)) {
        uint len1 = strlen(env->expected_computing_nodegroup);
        uint len2 = strlen(env->default_storage_nodegroup);
        if (len1 == strlen(CNG_OPTION_QUERY) &&
            0 == memcmp(env->expected_computing_nodegroup, CNG_OPTION_QUERY, len1) &&
            len2 == strlen(INSTALLATION_MODE) &&
            0 == memcmp(env->default_storage_nodegroup, INSTALLATION_MODE, len2)) {
            if (u_sess->attr.attr_common.namespace_current_schema &&
                strcmp(env->schema_name, u_sess->attr.attr_common.namespace_current_schema) == 0) {
                diff = false;
            }
        }
    }

    return diff;
}

DListCell* GPCFetchStmtInList(DList* target, const char* stmt) 
{
    Assert(target != NULL);
    DListCell* iter = target->head;
    for (; iter != NULL; iter = iter->next) {
        PreparedStatement *prepare_statement = (PreparedStatement *)(iter->data.ptr_value);
        if (strcmp(stmt, prepare_statement->stmt_name) == 0)
            return iter;
    }
    return NULL;
}

uint32 GPCHashFunc(const void *key, Size keysize)
{
    const GPCKey *item = (const GPCKey *) key; 
    return DatumGetUInt32(hash_any((const unsigned char *)item->query_string, item->query_length));
}

int GPCHashMatch(const void *left, const void *right, Size keysize)
{
    const GPCKey *leftItem = (GPCKey*)left;
    const GPCKey *rightItem = (GPCKey*)right;
    Assert(NULL != leftItem && NULL != rightItem);

    /* we just care whether the result is 0 or not. */
    if (leftItem->query_length != rightItem->query_length) {
        return 1;
    }

    return memcmp(leftItem->query_string, rightItem->query_string, leftItem->query_length);
}

void GlobalPlanCache::PlanInit()
{
    HASHCTL ctl;
    errno_t rc = 0;
    rc = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
    securec_check(rc, "\0", "\0");
    ctl.hcxt = g_instance.cache_cxt.global_cache_mem;
    ctl.keysize = sizeof(GPCKey);
    ctl.entrysize = sizeof(GPCEntry);
    ctl.hash = (HashValueFunc)GPCHashFunc;
    ctl.match = (HashCompareFunc)GPCHashMatch;
    ctl.num_partitions = GPC_NUM_OF_BUCKETS;

    int flags = HASH_ELEM | HASH_FUNCTION | HASH_COMPARE | HASH_PARTITION | HASH_SHRCTX;
    m_global_plan_cache = hash_create("Global_Plan_Cache",
                                      GPC_NUM_OF_BUCKETS,
                                      &ctl,
                                      flags);

    m_gpc_bucket_info_array = (GPCBucketInfo*) MemoryContextAllocZero(g_instance.cache_cxt.global_cache_mem,
                                                                      sizeof(GPCBucketInfo) * GPC_NUM_OF_BUCKETS); 

    for (uint32 i = 0; i < GPC_NUM_OF_BUCKETS; i++) {
        /*
        * Create a MemoryContext per hash bucket so that all entries, plans etc under the bucket will live
        * under this Memory context. This is for performance purposes. We do not want everything to be under
        * the shared GlobalPlanCacheContext because more threads would need to synchronize everytime it needs a chunk 
        * of memory and that would become a bottleneck.
        */
        m_gpc_bucket_info_array[i].context = AllocSetContextCreate(g_instance.cache_cxt.global_cache_mem,
                                                                   "GPC_Bucket_Context",
                                                                   ALLOCSET_DEFAULT_MINSIZE,
                                                                   ALLOCSET_DEFAULT_INITSIZE,
                                                                   ALLOCSET_DEFAULT_MAXSIZE,
                                                                   SHARED_CONTEXT);
    }

    m_gpc_invalid_plansource = NULL;
}

/* Get the HTAB Bucket index based on the hashvalue. 
 * This function is a direct copy from dynahash.cpp.
 */
uint32 GlobalPlanCache::GetBucket(uint32 hashvalue)
{
    HASHHDR *hctl = (HASHHDR *)(m_global_plan_cache->hctl);
    uint32 bucket = hashvalue & hctl->high_mask;
    
    if (bucket > hctl->max_bucket)
        bucket = bucket & hctl->low_mask;
    
    Assert(bucket < GPC_NUM_OF_BUCKETS);
    return bucket;
}

void GlobalPlanCache::PlanStore(CachedPlanSource *plansource)
{
    Assert (plansource != NULL);
    Assert (plansource->magic == CACHEDPLANSOURCE_MAGIC);
    Assert (plansource->gpc.is_insert == true);

    GPCKey key;
    key.query_string = plansource->query_string;
    key.query_length = strlen(plansource->query_string);
    uint32 hashCode = GPCHashFunc((const void *) &key, sizeof(key));
    plansource->gpc.query_hash_code = hashCode;

    uint32 gpc_bucket_index = GetBucket(hashCode);
    int partitionLock = (int) (FirstGPCMappingLock + gpc_bucket_index);
    Assert ((int) partitionLock >= FirstGPCMappingLock);
    Assert ((int) partitionLock < FirstGPCMappingLock + GPC_NUM_OF_BUCKETS);

    LWLockAcquire(GetMainLWLockByIndex(partitionLock), LW_EXCLUSIVE);
    Assert (plansource->gpc.entry == NULL);

    bool found = false;
    GPCEntry *entry = (GPCEntry *)hash_search_with_hash_value(m_global_plan_cache,
                                                              (const void*)&key, hashCode, HASH_ENTER, &found);
    Assert (entry != NULL);
    if (found == false) {
        // Initialize the new GPC entry
        entry->cachedPlans = NULL;
        entry->refcount = 0;
        entry->is_valid = true;
        entry->lockId = partitionLock;
        entry->CAS_flag = false;

        /* Deep copy the query_string to the GPC entry's query_string */
        MemoryContext oldcontext = MemoryContextSwitchTo(m_gpc_bucket_info_array[gpc_bucket_index].context);
        entry->key.query_string = pnstrdup(key.query_string, key.query_length);
        MemoryContextSwitchTo(oldcontext);

        /* Set the magic number. */
        entry->magic = GLOBALPLANCACHEKEY_MAGIC;
    
        gs_atomic_add_32(&m_gpc_bucket_info_array[gpc_bucket_index].entries_count, 1);
    }
    plansource->gpc.entry = entry;
    plansource->gpc.env->globalplancacheentry = entry;
    gs_atomic_add_32(&plansource->gpc.entry->refcount, 1);

    /* Each GPC entry maintains a List of CachedEnvironments. 
    * Append the new CachedEnvironment to the entry's List. 
    * But make sure there is only one thread inserting into the List.
    */
    while (!gs_compare_and_swap_32(&entry->CAS_flag, FALSE, TRUE))
        pg_usleep(CAS_SLEEP_DURATION); // microseconds.

    MemoryContext oldcontext = MemoryContextSwitchTo(m_gpc_bucket_info_array[gpc_bucket_index].context);
    entry->cachedPlans = dlappend(entry->cachedPlans, plansource->gpc.env);
    MemoryContextSwitchTo(oldcontext);

    gs_compare_and_swap_32(&entry->CAS_flag, TRUE, FALSE);

    plansource->gpc.is_share = true;
    plansource->gpc.is_insert = false;
    Assert (plansource->gplan != NULL);
    Assert (plansource->gplan->is_share == true);
    Assert (plansource->gplan->context->parent == g_instance.cache_cxt.global_cache_mem);

    LWLockRelease(GetMainLWLockByIndex(partitionLock));
}

GPCEnv* GlobalPlanCache::PlanFetch(const char *query_string, uint32 query_len, int num_params)
{
    GPCKey key;
    key.query_string = query_string;
    key.query_length = query_len;
    uint32 hashCode = GPCHashFunc((const void *) &key, sizeof(key));

    uint32 gpc_bucket_index = GetBucket(hashCode);
    int partitionLock = (int) (FirstGPCMappingLock + gpc_bucket_index);
    Assert ((int) partitionLock >= FirstGPCMappingLock);
    Assert ((int) partitionLock < FirstGPCMappingLock + GPC_NUM_OF_BUCKETS);

    LWLockAcquire(GetMainLWLockByIndex(partitionLock), LW_SHARED);

    bool foundCachedEntry = false;
    GPCEntry *cachedEntry = (GPCEntry *) hash_search_with_hash_value(m_global_plan_cache,
                                                                     (const void*)(&key),
                                                                     hashCode,
                                                                     HASH_FIND,
                                                                     &foundCachedEntry);

    if (cachedEntry == NULL || cachedEntry->cachedPlans == NULL) {
        LWLockRelease(GetMainLWLockByIndex(partitionLock));
        return NULL;
    }

    GPCEnv *gpc_env = NULL;
    int numCachedPlans = cachedEntry->cachedPlans->length;
    DListCell *cell = cachedEntry->cachedPlans->head;

    for (int i = 0; i < numCachedPlans; i++) {
        Assert(cell != NULL);

        gpc_env = (GPCEnv *) cell->data.ptr_value;
        if (gpc_env != NULL && num_params == gpc_env->num_params &&
            GPCCompareEnvSignature(gpc_env) == false) {
            if (gpc_env->plansource->gpc.is_share == true) {
                Assert (gpc_env->plansource->gplan != NULL);
                Assert (gpc_env->plansource->gplan->is_share == true);
                Assert (gpc_env->plansource->gplan->context->parent == g_instance.cache_cxt.global_cache_mem);
                break;
            }
        }
        gpc_env = NULL;
        cell = cell->next;
    }

    LWLockRelease(GetMainLWLockByIndex(partitionLock));

    return gpc_env;
}

void GlobalPlanCache::InvalidPlanDrop()
{
    if (m_gpc_invalid_plansource != NULL) {
        DListCell *cell = m_gpc_invalid_plansource->head;

        while (cell != NULL) {
            CachedPlanSource *curr = (CachedPlanSource *)cell->data.ptr_value;
            
            if (curr->gpc.refcount == 0) {
                DListCell *next = cell->next;
                m_gpc_invalid_plansource = dlist_delete_cell(m_gpc_invalid_plansource, cell, false);

                CachedPlan *gplan = curr->gplan;
                DropCachedPlanInternal(curr);
                GPCReleaseCachedPlan(gplan);
                curr->magic = 0;
                MemoryContextDelete(curr->context);
                
                cell = next;
            } else {
                cell = cell->next;
            }
        }
    }
}

void GlobalPlanCache::PlanDrop(GPCEnv *cachedenv)
{
    LWLockAcquire(GPCClearLock, LW_EXCLUSIVE);

    CachedPlanSource *plansource = cachedenv->plansource;

    if (plansource->gpc.is_valid == false) {
        LWLockRelease(GPCClearLock);
        return ;
    }
    plansource->gpc.is_valid = false;

    Assert (plansource->gpc.env == cachedenv);
    GPCEntry *entry = cachedenv->globalplancacheentry;
    Assert (entry->cachedPlans != NULL);

    int numCachedPlans = entry->cachedPlans->length;
    Assert(numCachedPlans <= entry->refcount);
    DListCell *cell = entry->cachedPlans->head;
    for (int i = 0; i < numCachedPlans; i++) {
        GPCEnv *curr = (GPCEnv *)cell->data.ptr_value;
        if (curr == cachedenv) {
            cachedenv->globalplancacheentry->cachedPlans = 
                dlist_delete_cell(cachedenv->globalplancacheentry->cachedPlans, cell, false);
            gs_atomic_add_32(&entry->refcount, -1);

            if (entry->refcount == 0) {
                /* Remove the GPC entry */
                Assert(entry->cachedPlans == NULL);

                bool found = false;
                hash_search(m_global_plan_cache, (void *) &(entry->key), HASH_REMOVE, &found);
                Assert(true == found);
                uint32 gpc_bucket_index = (uint32)(entry->lockId - FirstGPCMappingLock);
                m_gpc_bucket_info_array[gpc_bucket_index].entries_count--;

                entry->magic = 0;
                pfree((void *)entry->key.query_string);

                cachedenv->plansource = NULL;
                MemoryContextDelete(cachedenv->context);
            }

            break;
        }
        cell = cell->next;
    }
    InvalidPlanDrop();
    MemoryContext oldcontext = MemoryContextSwitchTo(g_instance.cache_cxt.global_cache_mem);
    m_gpc_invalid_plansource = dlappend(m_gpc_invalid_plansource, plansource);
    MemoryContextSwitchTo(oldcontext);
    
    LWLockRelease(GPCClearLock);
}

uint32 GPCPrepareHashFunc(const void *key, Size keysize)
{
    return ((*(uint64 *)key) % NUM_GPC_PARTITIONS);
}

int GPCPrepareHashMatch(const void *left, const void *right, Size keysize)
{
    if (*((uint64 *)left) == *((uint64 *)right)) {
        return 0;
    } else {
        return 1;
    }
}

/* init the HTAB which stores the prepare statements refer to global plancache */
void GlobalPlanCache::PrepareInit()
{
    HASHCTL     hash_ctl;
    errno_t     rc = 0;

    rc = memset_s(&hash_ctl, sizeof(hash_ctl), 0, sizeof(hash_ctl));
    securec_check(rc, "\0", "\0");

    hash_ctl.keysize = sizeof(uint64);
    hash_ctl.entrysize = sizeof(GPCPreparedStatement);
    hash_ctl.hcxt = g_instance.cache_cxt.global_cache_mem;
    hash_ctl.hash = (HashValueFunc)GPCPrepareHashFunc;
    hash_ctl.match = (HashCompareFunc)GPCPrepareHashMatch;
    hash_ctl.num_partitions = GPC_NUM_OF_BUCKETS;

    int flags = HASH_ELEM | HASH_SHRCTX | HASH_PARTITION | HASH_FUNCTION | HASH_COMPARE;
    m_global_prepared = hash_create("Global Prepared Queries",
                                    GPC_NUM_OF_BUCKETS,
                                    &hash_ctl,
                                    flags);
}

void GlobalPlanCache::PrepareStore(const char *stmt_name,
                                   CachedPlanSource *plansource,
                                   bool from_sql)
{
    ereport(DEBUG3, (errmodule(MOD_GPC), errcode(ERRCODE_LOG),
            errmsg("gpc  <prepare store>  global_sess_id:%lu  stmt_name:%s  session_id:%lu",
                   u_sess->global_sess_id, stmt_name, u_sess->session_id)));

    GPCPreparedStatement *entry = NULL;
    TimestampTz cur_ts = GetCurrentStatementStartTimestamp();
    bool        found = false;

    uint32 gpc_bucket_index = GPCPrepareHashFunc(&u_sess->global_sess_id, sizeof(u_sess->global_sess_id));
    int lockid = (int)(FirstGPCPrepareMappingLock + gpc_bucket_index);
    LWLockAcquire(GetMainLWLockByIndex(lockid), LW_EXCLUSIVE);

    /* Add entry to hash table */
    entry = (GPCPreparedStatement *) hash_search(m_global_prepared,
                                                 &u_sess->global_sess_id,
                                                 HASH_ENTER,
                                                 &found);

    entry->global_sess_id = u_sess->global_sess_id;
    /* Shouldn't get a duplicate entry */
    if (found && entry->prepare_statement_list != NULL) {
        DListCell* find = GPCFetchStmtInList(entry->prepare_statement_list, stmt_name);
        if (find != NULL) {
            LWLockRelease(GetMainLWLockByIndex(lockid));
            ereport(ERROR,
                    (errcode(ERRCODE_DUPLICATE_PSTATEMENT),
                     errmsg("global prepared statement \"%s\" already exists",
                            stmt_name)));
        }
    }
    if (found == false) {
        entry->prepare_statement_list = NULL;
        gs_atomic_add_32(&m_gpc_bucket_info_array[gpc_bucket_index].prepare_count, 1);
    }

    /* Fill in the hash table entry */
    MemoryContext old_cxt = MemoryContextSwitchTo(g_instance.cache_cxt.global_cache_mem);
    PreparedStatement* stmt = (PreparedStatement*)palloc(sizeof(PreparedStatement));
    stmt->plansource = plansource;
    stmt->from_sql = from_sql;
    stmt->prepare_time = cur_ts;
    int rc = memcpy_s(stmt->stmt_name, NAMEDATALEN, stmt_name, NAMEDATALEN);
    securec_check(rc, "", "");

    entry->prepare_statement_list = dlappend(entry->prepare_statement_list, (void*)stmt);

    MemoryContextSwitchTo(old_cxt);

    LWLockRelease(GetMainLWLockByIndex(lockid));

    if (plansource->gpc.is_share == false) {
        /* mark insert global plan cache */
        plansource->gpc.is_insert = true;
        SaveCachedPlan(plansource);
    }

    ereport(DEBUG3, (errmodule(MOD_GPC), errcode(ERRCODE_LOG),
            errmsg("gpc  <prepare store success>  global_sess_id:%lu  stmt_name:%s  session_id:%lu",
                   u_sess->global_sess_id, stmt_name, u_sess->session_id))); 
}

PreparedStatement* GlobalPlanCache::PrepareFetch(const char *stmt_name, bool throwError)
{
    GPCPreparedStatement *entry = NULL;

    int lockid = (int)(FirstGPCPrepareMappingLock +
                        GPCPrepareHashFunc(&u_sess->global_sess_id, sizeof(u_sess->global_sess_id)));
    LWLockAcquire(GetMainLWLockByIndex(lockid), LW_SHARED);
    entry = (GPCPreparedStatement *) hash_search(m_global_prepared,
                                                 &u_sess->global_sess_id,
                                                 HASH_FIND,
                                                 NULL);
    if (entry == NULL) {
        if (throwError == true) {
            ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_PSTATEMENT),
                     errmsg("global prepared statement \"%s\" does not exist",
                            stmt_name)));
        } else {
            LWLockRelease(GetMainLWLockByIndex(lockid));
            return NULL;
        }
    }

    DListCell* result = GPCFetchStmtInList(entry->prepare_statement_list, stmt_name);
    if (result == NULL) {
        LWLockRelease(GetMainLWLockByIndex(lockid));
        return NULL;
    }

    LWLockRelease(GetMainLWLockByIndex(lockid));
    return (PreparedStatement *)(result->data.ptr_value);
}

void GlobalPlanCache::PrepareDrop(const char *stmt_name, bool showError)
{
    ereport(DEBUG3, (errmodule(MOD_GPC), errcode(ERRCODE_LOG),
            errmsg("gpc  <prepare remove>  global_sess_id:%lu  stmt_name:%s  session_id:%lu",
                   u_sess->global_sess_id, stmt_name, u_sess->session_id)));
    /* Find the query's hash table entry; raise error if wanted */
    GPCPreparedStatement *entry = NULL;

    uint32 gpc_bucket_index = GPCPrepareHashFunc(&u_sess->global_sess_id, sizeof(u_sess->global_sess_id));
    int lockid = (int)(FirstGPCPrepareMappingLock + gpc_bucket_index);
    LWLockAcquire(GetMainLWLockByIndex(lockid), LW_EXCLUSIVE);
    entry = (GPCPreparedStatement *) hash_search(m_global_prepared,
                                                 &u_sess->global_sess_id,
                                                 HASH_FIND,
                                                 NULL);
    if (entry == NULL) {
        LWLockRelease(GetMainLWLockByIndex(lockid));
        return;
    }
    DListCell* result = GPCFetchStmtInList(entry->prepare_statement_list, stmt_name);

    if (result == NULL) {
        LWLockRelease(GetMainLWLockByIndex(lockid));
        return;
    }
    
    PreparedStatement* target = (PreparedStatement *)(result->data.ptr_value);
    if (target != NULL) {
        if (target->plansource->gpc.is_share == true) {
            ereport(DEBUG3, (errmodule(MOD_GPC), errcode(ERRCODE_LOG),
                    errmsg("gpc  <prepare remove success>  global_sess_id:%lu  stmt_name:%s  session_id:%lu",
                           u_sess->global_sess_id, stmt_name, u_sess->session_id)));
            RefcountSub(target->plansource);
        } else {
            Assert (target->plansource->gpc.is_insert == true);
            DropCachedPlan(target->plansource);
        }
        entry->prepare_statement_list = dlist_delete_cell(entry->prepare_statement_list, result, false);
    }

    if (entry->prepare_statement_list == NULL) {
        ereport(DEBUG3, (errmodule(MOD_GPC), errcode(ERRCODE_LOG),
                errmsg("gpc  <prepare drop>  global_sess_id:%lu  stmt_name:%s  session_id:%lu",
                       u_sess->global_sess_id, stmt_name, u_sess->session_id)));

        /* Now we can remove the hash table entry */
        hash_search(m_global_prepared, &u_sess->global_sess_id, HASH_REMOVE, NULL);
        m_gpc_bucket_info_array[gpc_bucket_index].prepare_count--;
    }

    LWLockRelease(GetMainLWLockByIndex(lockid));
}
/*
 * @Description: Drop ALL global plancaches which belongs to current session.
 * This function only be called before the session aborts.
 * @in num: uint64 global_sess_id ,boolean need_lock
 * @return - void
 */
void GlobalPlanCache::PrepareDropAll(uint64 global_sess_id, bool need_lock)
{
    ereport(DEBUG3, (errmodule(MOD_GPC), errcode(ERRCODE_LOG),
            errmsg("gpc  <prepare drop all>  global_sess_id:%lu  session_id:%lu need_lock:%s",
                   global_sess_id, u_sess->session_id, need_lock ? "true" : "false")));
    /* Find the query's hash table entry; raise error if wanted */
    GPCPreparedStatement *entry = NULL;

    uint32 gpc_bucket_index = GPCPrepareHashFunc(&global_sess_id, sizeof(global_sess_id));
    int lockid = (int)(FirstGPCPrepareMappingLock + gpc_bucket_index);
    if (need_lock) {
        LWLockAcquire(GetMainLWLockByIndex(lockid), LW_EXCLUSIVE);
    }
    entry = (GPCPreparedStatement *) hash_search(m_global_prepared,
                                                 &global_sess_id,
                                                 HASH_FIND,
                                                 NULL);
    if (entry == NULL) {
        if (need_lock) {
            LWLockRelease(GetMainLWLockByIndex(lockid));
        }
        return;
    }
    DListCell* iter = entry->prepare_statement_list->head;
    for (; iter != NULL; iter = iter->next) {
        PreparedStatement *prepare_statement = (PreparedStatement *)(iter->data.ptr_value);
        RefcountSub(prepare_statement->plansource);
        ereport(DEBUG3, (errmodule(MOD_GPC), errcode(ERRCODE_LOG),
                errmsg("gpc  <prepare drop all success>  global_sess_id:%lu  session_id:%lu",
                       global_sess_id, u_sess->session_id)));
    }

    dlist_free(entry->prepare_statement_list, true);
    hash_search(m_global_prepared, &global_sess_id, HASH_REMOVE, NULL);
    m_gpc_bucket_info_array[gpc_bucket_index].prepare_count--;

    if (need_lock) {
        LWLockRelease(GetMainLWLockByIndex(lockid));
    }
}

void GlobalPlanCache::PrepareClean(uint32 cn_id)
{
    GPCPreparedStatement *entry = NULL;

    for (uint32 currBucket = 0; currBucket < GPC_NUM_OF_BUCKETS; currBucket++) {
        LWLockAcquire(GetMainLWLockByIndex(FirstGPCPrepareMappingLock + currBucket), LW_EXCLUSIVE);

        uint32 bucketEntriesCount = m_gpc_bucket_info_array[currBucket].prepare_count;
        if (bucketEntriesCount != 0) {
            HASH_SEQ_STATUS hash_seq;
            hash_seq_init(&hash_seq, m_global_prepared);
            hash_seq.curBucket = currBucket;
            hash_seq.curEntry = NULL;

            for (uint32 entryIndex = 0; entryIndex < bucketEntriesCount; entryIndex++) {
                 entry = (GPCPreparedStatement *)hash_seq_search(&hash_seq);
                 if (cn_id == (entry->global_sess_id >> 48)) {
                     PrepareDropAll(entry->global_sess_id, false);
                 }
            }
            /* hash_seq_search terminates the sequence if it reaches the last entry in the hash
             * ie: if it returns a NULL. So if it has not returned a NULL, we terminate it explicitly.
             */
            if (entry != NULL) {
                hash_seq_term(&hash_seq);
            }
        }

        LWLockRelease(GetMainLWLockByIndex(FirstGPCPrepareMappingLock + currBucket));
    }
}

void GlobalPlanCache::CheckTimeline(uint32 timeline)
{
    LWLockAcquire(GPCTimelineLock, LW_SHARED);
    TimelineEntry *entry = NULL;
    bool found = false;
    uint32 id = (uint32)(u_sess->global_sess_id >> 48);

    ereport(DEBUG3, (errmodule(MOD_GPC), errcode(ERRCODE_LOG),
            errmsg("gpc  <timeline check>  global_sess_id:%lu  timeline:%u  cn_id:%u",
                   u_sess->global_sess_id, timeline, id)));

    entry = (TimelineEntry *) hash_search(m_cn_timeline,
                                          &id,
                                          HASH_FIND,
                                          &found);

    if (found == false) {
        LWLockRelease(GPCTimelineLock);
        LWLockAcquire(GPCTimelineLock, LW_EXCLUSIVE);

        entry = (TimelineEntry *) hash_search(m_cn_timeline,
                                              &id,
                                              HASH_ENTER,
                                              &found);
        if (found == false) {
            entry->id = id;
            entry->timeline = timeline;
        }
    } else {
        if (entry->timeline != timeline) {
            LWLockRelease(GPCTimelineLock);
            LWLockAcquire(GPCTimelineLock, LW_EXCLUSIVE);

            if (entry->timeline != timeline) {
                ereport(DEBUG3, (errmodule(MOD_GPC), errcode(ERRCODE_LOG),
                        errmsg("gpc  <timeline check fail>  global_sess_id:%lu  old_timeline:%u  new_timeline:%u",
                               u_sess->global_sess_id, entry->timeline, timeline)));
                PrepareClean(id);
            }

            entry->timeline = timeline;
        }
    }

    LWLockRelease(GPCTimelineLock);
}

void GlobalPlanCache::PrepareUpdate(CachedPlanSource *plansource, CachedPlanSource *share_plansource, bool throwError)
{
    PreparedStatement *entry = NULL;
    entry = PrepareFetch(plansource->stmt_name, false);
    if (entry == NULL) {
        if (throwError == true) {
            ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_PSTATEMENT),
                     errmsg("global prepared statement \"%s\" does not exist",
                            plansource->stmt_name)));
        }
        return;
    }

    entry->plansource = share_plansource;
    RefcountAdd(share_plansource);
}

/*
 * @Description: Send k message to DNs with a clean flag.
 * this function will first get the handles from all DNs due to the fact that
 * in some cases there is no handles between CN and DN because GPC, but we still need to clean their global plancaches
 * @in num: void
 * @return - void
 */
void GlobalPlanCache::SendPrepareDestoryMsg()
{
    if(ENABLE_CN_GPC && HaveActiveDatanodeStatements() && IS_PGXC_COORDINATOR) {
        PGXCNodeAllHandles *pgxc_handles = NULL;

        List *DataNodeList = GetAllDataNodes();
        pgxc_handles = get_handles(DataNodeList, NULL, false, NULL);
        for (int i = 0; i < pgxc_handles->dn_conn_count; i++) {
            if (IS_VALID_CONNECTION(pgxc_handles->datanode_handles[i])) {
                pgxc_handles->datanode_handles[i]->state = DN_CONNECTION_STATE_IDLE;
                if (pgxc_node_send_sessid(pgxc_handles->datanode_handles[i], u_sess->global_sess_id, HANDLE_CLEAN) != 0) {
                    pgxc_handles->datanode_handles[i]->state = DN_CONNECTION_STATE_ERROR_FATAL;
                    ereport(WARNING,
                        (errcode(ERRCODE_CONNECTION_EXCEPTION),
                            errmsg("Failed to send global session id to Datanode %u", 
                                   pgxc_handles->datanode_handles[i]->nodeoid)));
                }
                pgxc_node_flush(pgxc_handles->datanode_handles[i]);
            }
        }
        release_handles();
    }
}

void GlobalPlanCache::RecreateCachePlan(PreparedStatement *entry)
{
    /*
     * Start up a transaction command so we can run parse analysis etc. (Note
     * that this will normally change current memory context.) Nothing happens
     * if we are already in one.
     */
    start_xact_command();

    CachedPlanSource *newsource = CopyCachedPlan(entry->plansource, true);

    newsource->gpc.is_share = false;
    newsource->gpc.is_insert = true;
    newsource->gpc.is_valid = true;
    newsource->gpc.query_hash_code = 0;

    (void)RevalidateCachedQuery(newsource);

    RefcountSub(entry->plansource);
    entry->plansource = newsource;
}

/*
 * @Description: Store the global plancache into the hash table and mark them as shared.
 * this function called when a transction commited. All unsaved global plancaches are stored
 * in the list named first_saved_plan. When we found that a plancache was saved by others, we
 * just drop it and update the prepare pointer to the shared glboal plancache.
 * @in num: void
 * @return - void
 */
void GlobalPlanCache::Commit()
{
    CachedPlanSource *plansource = NULL;
    CachedPlanSource *prev_plansource = NULL;
    CachedPlanSource *next_plansource = NULL;
    GPCEnv *env = NULL;

    plansource = u_sess->pcache_cxt.first_saved_plan;
    prev_plansource = u_sess->pcache_cxt.first_saved_plan;
    while (plansource != NULL) {
        Assert(plansource->magic == CACHEDPLANSOURCE_MAGIC);
        Assert(plansource->gpc.is_share == false);

        if (!plansource->is_valid || plansource->gpc.is_insert == false) {
            prev_plansource = plansource;
            plansource = plansource->next_saved;
            continue;
        }

        LWLockAcquire(GPCCommitLock, LW_EXCLUSIVE);
        env = PlanFetch(plansource->query_string, strlen(plansource->query_string), plansource->num_params);
        ereport(DEBUG3, (errmodule(MOD_GPC), errcode(ERRCODE_LOG),
                errmsg("gpc  <commit>  global_sess_id:%lu  stmt_name:%s  session_id:%lu find:%s",
                       u_sess->global_sess_id, plansource->stmt_name, u_sess->session_id,
                       env != NULL ? "YES" : "NO")));

        if (env == NULL) {
            PlanStore(plansource);

            if (prev_plansource == u_sess->pcache_cxt.first_saved_plan) {
                u_sess->pcache_cxt.first_saved_plan = plansource->next_saved;
                prev_plansource = plansource->next_saved;
            } else {
                prev_plansource->next_saved = plansource->next_saved;
            }

            plansource = plansource->next_saved;
        } else {
            PrepareUpdate(plansource, env->plansource, true);

            if (prev_plansource == u_sess->pcache_cxt.first_saved_plan) {
                prev_plansource = plansource->next_saved;
            }

            next_plansource = plansource->next_saved;
            CachedPlan *gplan = plansource->gplan;
            DropCachedPlan(plansource);
            GPCReleaseCachedPlan(gplan);
            plansource = next_plansource;
        }
        LWLockRelease(GPCCommitLock);
    }
}
