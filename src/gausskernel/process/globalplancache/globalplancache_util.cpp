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
* globalplancache_util.cpp
*    global plan cache
*
* IDENTIFICATION
*     src/gausskernel/process/globalplancache/globalplancache_util.cpp
*
* -------------------------------------------------------------------------
*/

#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/genam.h"
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
void
GlobalPlanCache::FillClassicEnvSignatures(GPCEnv *env)
{
    env->plainenv.env_signature = 0;
    env->plainenv.env_signature |= u_sess->attr.attr_sql.enable_fast_numeric;
    env->plainenv.env_signature |= u_sess->attr.attr_sql.enable_global_stats << 1;
    env->plainenv.env_signature |= u_sess->attr.attr_sql.enable_hdfs_predicate_pushdown << 2;
    env->plainenv.env_signature |= u_sess->attr.attr_sql.enable_absolute_tablespace << 3;
    env->plainenv.env_signature |= u_sess->attr.attr_sql.enable_hadoop_env << 4;
    env->plainenv.env_signature |= u_sess->attr.attr_sql.enable_valuepartition_pruning << 5;
    env->plainenv.env_signature |= u_sess->attr.attr_sql.enable_constraint_optimization << 6;
    env->plainenv.env_signature |= u_sess->attr.attr_sql.enable_bloom_filter << 7;
    env->plainenv.env_signature |= u_sess->attr.attr_sql.enable_codegen << 8;
    env->plainenv.env_signature |= u_sess->attr.attr_sql.enable_codegen_print << 9;
    env->plainenv.env_signature |= u_sess->attr.attr_sql.enable_seqscan << 10;
    env->plainenv.env_signature |= u_sess->attr.attr_sql.enable_indexscan << 11;
    env->plainenv.env_signature |= u_sess->attr.attr_sql.enable_indexonlyscan << 12;
    env->plainenv.env_signature |= u_sess->attr.attr_sql.enable_bitmapscan << 13;
    env->plainenv.env_signature |= u_sess->attr.attr_sql.force_bitmapand << 14;
    env->plainenv.env_signature |= u_sess->attr.attr_sql.enable_tidscan << 15;
    env->plainenv.env_signature |= u_sess->attr.attr_sql.enable_sort << 16;
    env->plainenv.env_signature |= u_sess->attr.attr_sql.enable_compress_spill << 17;
    env->plainenv.env_signature |= u_sess->attr.attr_sql.enable_hashagg << 18;
    env->plainenv.env_signature |= u_sess->attr.attr_sql.enable_material << 19;
    env->plainenv.env_signature |= u_sess->attr.attr_sql.enable_nestloop << 20;
    env->plainenv.env_signature |= u_sess->attr.attr_sql.enable_mergejoin << 21;
    env->plainenv.env_signature |= u_sess->attr.attr_sql.enable_hashjoin << 22;
    env->plainenv.env_signature |= u_sess->attr.attr_sql.enable_index_nestloop << 23;
    env->plainenv.env_signature |= u_sess->attr.attr_sql.enable_nodegroup_debug << 24;
    env->plainenv.env_signature |= u_sess->attr.attr_sql.enable_partitionwise << 25;
    env->plainenv.env_signature |= u_sess->attr.attr_sql.enable_kill_query << 26;
    env->plainenv.env_signature |= u_sess->attr.attr_sql.agg_redistribute_enhancement << 27;
    env->plainenv.env_signature |= u_sess->attr.attr_sql.enable_broadcast << 28;
    env->plainenv.env_signature |= g_instance.attr.attr_sql.enable_orc_cache << 29;
    env->plainenv.env_signature |= u_sess->attr.attr_sql.acceleration_with_compute_pool << 30;
    env->plainenv.env_signature |= u_sess->attr.attr_sql.enable_extrapolation_stats << 31;

}


/*
 * @Description:Fill in the environment signatures which are bitmaps for the boolean type GUC parameters
 * @in num: GPCEnv
 * @return - void
*/
void
GlobalPlanCache::FillEnvSignatures(GPCEnv *env)
{
    /* We should only call this function if env is not NULL and it's not filled */
    Assert(env && !env->filled);
    env->plainenv.env_signature_pgxc = 0;
    env->plainenv.env_signature_pgxc |= g_instance.attr.attr_sql.string_hash_compatible;
    env->plainenv.env_signature_pgxc |= u_sess->attr.attr_sql.enable_remotejoin << 1;
    env->plainenv.env_signature_pgxc |= u_sess->attr.attr_sql.enable_fast_query_shipping << 2;
    env->plainenv.env_signature_pgxc |= u_sess->attr.attr_sql.enable_remotegroup << 3;
    env->plainenv.env_signature_pgxc |= u_sess->attr.attr_sql.enable_remotesort << 4;
    env->plainenv.env_signature_pgxc |= u_sess->attr.attr_sql.enable_remotelimit << 5;
    env->plainenv.env_signature_pgxc |= u_sess->attr.attr_sql.gtm_backup_barrier << 6;
    env->plainenv.env_signature_pgxc |= u_sess->attr.attr_sql.enable_stream_operator << 7;
    env->plainenv.env_signature_pgxc |= u_sess->attr.attr_sql.enable_vector_engine << 8;
    env->plainenv.env_signature_pgxc |= u_sess->attr.attr_sql.enable_force_vector_engine << 9;
    env->plainenv.env_signature_pgxc |= u_sess->attr.attr_sql.enable_random_datanode << 10;
    env->plainenv.env_signature_pgxc |= u_sess->attr.attr_sql.enable_fstream << 11;
    FillClassicEnvSignatures(env);
    env->plainenv.env_signature2 = 0;
    env->plainenv.env_signature2 |= u_sess->attr.attr_sql.enable_geqo;
    env->plainenv.env_signature2 |= u_sess->attr.attr_sql.td_compatible_truncation << 1;
    env->plainenv.env_signature2 |= u_sess->attr.attr_sql.enable_upgrade_merge_lock_mode << 2;
    env->plainenv.env_signature2 |= u_sess->attr.attr_sql.enforce_a_behavior << 3;
    env->plainenv.env_signature2 |= u_sess->attr.attr_sql.standard_conforming_strings << 4;

    /* some new GUC parameters which affect the plan */
    env->plainenv.env_signature2 |= u_sess->attr.attr_sql.enable_stream_concurrent_update << 5;
    env->plainenv.env_signature2 |= u_sess->attr.attr_sql.enable_stream_recursive << 6;
    env->plainenv.env_signature2 |= u_sess->attr.attr_sql.enable_change_hjcost << 7;
    env->plainenv.env_signature2 |= u_sess->attr.attr_sql.enable_analyze_check << 8;
    env->plainenv.env_signature2 |= u_sess->attr.attr_sql.enable_sonic_hashagg << 9;
    env->plainenv.env_signature2 |= u_sess->attr.attr_sql.enable_sonic_hashjoin << 10;
    env->plainenv.env_signature2 |= u_sess->attr.attr_sql.enable_sonic_optspill << 11;
    env->plainenv.env_signature2 |= u_sess->attr.attr_sql.enable_csqual_pushdown << 13;
    env->plainenv.env_signature2 |= u_sess->attr.attr_sql.enable_pbe_optimization << 14;
    /* spi unable light CN */
    bool elp = (u_sess->SPI_cxt._connected >= 0) ? false : u_sess->attr.attr_sql.enable_light_proxy;
    env->plainenv.env_signature2 |= elp << 15;
    env->plainenv.env_signature2 |= u_sess->attr.attr_sql.enable_early_free << 16;
    env->plainenv.env_signature2 |= u_sess->attr.attr_sql.enable_opfusion << 17;
    env->plainenv.env_signature2 |= u_sess->attr.attr_sql.enable_partition_opfusion << 18;
    env->plainenv.env_signature2 |= u_sess->attr.attr_sql.partition_iterator_elimination << 19;
}

void
GlobalPlanCache::EnvFill(GPCEnv *env, bool depends_on_role)
{
    /* We should only call this function if env is not NULL and it's not filled */
    Assert(env && !env->filled);

    
    errno_t rc = memset_s(env, sizeof(GPCEnv), 0, sizeof(GPCEnv));
    securec_check(rc, "\0", "\0");

    FillEnvSignatures(env);

    env->plainenv.best_agg_plan = u_sess->attr.attr_sql.best_agg_plan;
    env->plainenv.query_dop_tmp = u_sess->attr.attr_sql.query_dop_tmp;
    env->plainenv.rewrite_rule = u_sess->attr.attr_sql.rewrite_rule;
    env->plainenv.codegen_strategy = u_sess->attr.attr_sql.codegen_strategy;
    env->plainenv.plan_mode_seed = u_sess->attr.attr_sql.plan_mode_seed;
    env->plainenv.effective_cache_size = u_sess->attr.attr_sql.effective_cache_size;
    env->plainenv.codegen_cost_threshold = u_sess->attr.attr_sql.codegen_cost_threshold;
    env->plainenv.seq_page_cost = u_sess->attr.attr_sql.seq_page_cost;
    env->plainenv.random_page_cost = u_sess->attr.attr_sql.random_page_cost;
    env->plainenv.cpu_tuple_cost = u_sess->attr.attr_sql.cpu_tuple_cost;
    env->plainenv.allocate_mem_cost = u_sess->attr.attr_sql.allocate_mem_cost;
    env->plainenv.cpu_index_tuple_cost = u_sess->attr.attr_sql.cpu_index_tuple_cost;
    env->plainenv.cpu_operator_cost = u_sess->attr.attr_sql.cpu_operator_cost;
    env->plainenv.stream_multiple = u_sess->attr.attr_sql.stream_multiple;
    env->plainenv.default_limit_rows = u_sess->attr.attr_sql.default_limit_rows;
    env->plainenv.geqo_threshold = u_sess->attr.attr_sql.geqo_threshold;
    env->plainenv.Geqo_effort = u_sess->attr.attr_sql.Geqo_effort;
    env->plainenv.Geqo_pool_size = u_sess->attr.attr_sql.Geqo_pool_size;
    env->plainenv.Geqo_generations = u_sess->attr.attr_sql.Geqo_generations;
    env->plainenv.Geqo_selection_bias = u_sess->attr.attr_sql.Geqo_selection_bias;
    env->plainenv.Geqo_seed = u_sess->attr.attr_sql.Geqo_seed;
    env->plainenv.default_statistics_target = u_sess->attr.attr_sql.default_statistics_target;
    env->plainenv.from_collapse_limit = u_sess->attr.attr_sql.from_collapse_limit;
    env->plainenv.join_collapse_limit = u_sess->attr.attr_sql.join_collapse_limit;
    env->plainenv.cost_param = u_sess->attr.attr_sql.cost_param;
    env->plainenv.schedule_splits_threshold = u_sess->attr.attr_sql.schedule_splits_threshold;
    env->plainenv.hashagg_table_size = u_sess->attr.attr_sql.hashagg_table_size;
    env->plainenv.cursor_tuple_fraction = u_sess->attr.attr_sql.cursor_tuple_fraction;
    env->plainenv.constraint_exclusion = u_sess->attr.attr_sql.constraint_exclusion;
    env->plainenv.behavior_compat_flags = u_sess->utils_cxt.behavior_compat_flags;
    env->plainenv.plsql_compile_behavior_compat_flags = u_sess->utils_cxt.plsql_compile_behavior_compat_flags;
    env->plainenv.datestyle = u_sess->time_cxt.DateStyle;
    env->plainenv.dateorder = u_sess->time_cxt.DateOrder;
    env->plainenv.sql_beta_feature = u_sess->attr.attr_sql.sql_beta_feature;

    /* new GUC parameters which affect the plan */
    env->plainenv.qrw_inlist2join_optmode = u_sess->opt_cxt.qrw_inlist2join_optmode;
    env->plainenv.skew_strategy_store = u_sess->attr.attr_sql.skew_strategy_store;
    env->plainenv.database_id = u_sess->proc_cxt.MyDatabaseId;
    env->plainenv.plancachemode = u_sess->attr.attr_sql.g_planCacheMode;
    GlobalPlanCache::GetSchemaName(env);


    if (u_sess->attr.attr_sql.expected_computing_nodegroup) {
        int rc = memcpy_s(env->expected_computing_nodegroup,
                         NAMEDATALEN,
                         u_sess->attr.attr_sql.expected_computing_nodegroup,
                         strlen(u_sess->attr.attr_sql.expected_computing_nodegroup));
        securec_check(rc, "", "");
    } else {
       env->expected_computing_nodegroup[0] = '\0';
    }

    if (u_sess->attr.attr_sql.default_storage_nodegroup) {
        int rc = memcpy_s(env->default_storage_nodegroup,
                         NAMEDATALEN,
                         u_sess->attr.attr_sql.default_storage_nodegroup,
                         strlen(u_sess->attr.attr_sql.default_storage_nodegroup));
        securec_check(rc, "", "");
    } else {
        env->default_storage_nodegroup[0] = '\0';
    }

    env->depends_on_role = depends_on_role;
    env->user_oid = GetUserId();
}


void
GlobalPlanCache::GetSchemaName(GPCEnv *env)
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


void GPCResetAll()
{
    pthread_mutex_lock(&g_instance.gpc_reset_lock);

    for (int i = 0; i < MAX_GLOBAL_CACHEMEM_NUM; ++i) {
        MemoryContextUnSealChildren(g_instance.cache_cxt.global_plancache_mem[i]);
        MemoryContextReset(g_instance.cache_cxt.global_plancache_mem[i]);
    }
    g_instance.plan_cache->Init();
    pthread_mutex_unlock(&g_instance.gpc_reset_lock);
    GPC_LOG("gpc reset all", 0, 0);
}

void GPCCleanDatanodeStatement(int dn_stmt_num, const char* stmt_name)
{
    if (stmt_name == NULL || stmt_name[0] == '\0' || !IS_PGXC_COORDINATOR)
        return;
    int n = 0;
    char *tmp_name = NULL;
    for (n = 0; n < dn_stmt_num; n++) {
        tmp_name = get_datanode_statement_name(stmt_name, n);
        DropDatanodeStatement(tmp_name);
        pfree_ext(tmp_name);
    }
}

void GPCReGplan(CachedPlanSource* plansource)
{
    /* if is unshared for has cplan, and create gplan this time,
     * reset to shared and move to first_saved_plan */
    if (!plansource->is_support_gplan || !plansource->gpc.status.IsUnShareCplan())
        return;
    Assert(plansource->is_saved);
    plansource->gpc.status.SetKind(GPC_SHARED);
    /* move into first_saved_plan */
    if (u_sess->pcache_cxt.ungpc_saved_plan == plansource) {
        u_sess->pcache_cxt.ungpc_saved_plan = plansource->next_saved;
    } else {
        CachedPlanSource* psrc = NULL;
        for (psrc = u_sess->pcache_cxt.ungpc_saved_plan; psrc; psrc = psrc->next_saved) {
            if (psrc->next_saved == plansource) {
                psrc->next_saved = plansource->next_saved;
                break;
            }
        }
    }
    plansource->gpc.status.SetLoc(GPC_SHARE_IN_LOCAL_SAVE_PLAN_LIST);
    plansource->next_saved = u_sess->pcache_cxt.first_saved_plan;
    u_sess->pcache_cxt.first_saved_plan = plansource;
}

void GPCCleanUpSessionSavedPlan()
{
    if (!ENABLE_GPC) {
        return;
    }
    /* clean gpc refcount and plancache in shared memory */
    if (ENABLE_DN_GPC)
        CleanSessGPCPtr(u_sess);

    /* unnamed_stmt_psrc only save shared gpc plan or private plan,
     * so we only need to sub refcount for shared plan. */
    if (u_sess->pcache_cxt.unnamed_stmt_psrc && u_sess->pcache_cxt.unnamed_stmt_psrc->gpc.status.InShareTable()) {
        u_sess->pcache_cxt.unnamed_stmt_psrc->gpc.status.SubRefCount();
        u_sess->pcache_cxt.unnamed_stmt_psrc = NULL;
    }
    /* if in shared memory, delete context. */
    /* For DN and CN */
    CachedPlanSource* psrc = u_sess->pcache_cxt.first_saved_plan;
    CachedPlanSource* next = NULL;
    while (psrc != NULL) {
        next = psrc->next_saved;
        Assert (!psrc->gpc.status.InShareTable());
        if (!psrc->gpc.status.IsPrivatePlan())
            DropCachedPlan(psrc);
        psrc = next;
    }
    /* For CN */
    psrc = u_sess->pcache_cxt.ungpc_saved_plan;
    next = NULL;
    while (psrc != NULL) {
        next = psrc->next_saved;
        Assert (!psrc->gpc.status.InShareTable());
        if (!psrc->gpc.status.IsPrivatePlan())
            DropCachedPlan(psrc);
        psrc = next;
    }
}

/* incase change shared plan in execute stage, copy stmt into sess */
List* CopyLocalStmt(const List* stmt_list, const MemoryContext parent_cxt, MemoryContext* plan_context)
{
    *plan_context = AllocSetContextCreate(parent_cxt,
                                          "CopyedStmt",
                                          ALLOCSET_DEFAULT_MINSIZE,
                                          ALLOCSET_DEFAULT_INITSIZE,
                                          ALLOCSET_DEFAULT_MAXSIZE);
    /*
     * Copy plan into the new context.
     */
    MemoryContext oldcxt = MemoryContextSwitchTo(*plan_context);
    List* stmts = (List*)copyObject(stmt_list);
    (void)MemoryContextSwitchTo(oldcxt);
    return stmts;
}
/* function for modify SPICacheTable, global procedure plancache */

uint32 SPICacheHashFunc(const void *key, Size keysize)
{
    if (unlikely(key == NULL)) {
        return INVALID_SPI_KEY;
    }
    uint32 val1 = DatumGetUInt32(hash_any((const unsigned char *)key, (int)keysize));

    return val1;
}

/* add new func */
void SPICacheTableInsert(uint32 key, Oid func_oid)
{
    if (unlikely(u_sess->SPI_cxt.SPICacheTable == NULL || key == INVALID_SPI_KEY))
        return;
    bool found = false;
    SPIPlanCacheEnt* hentry = (SPIPlanCacheEnt*)hash_search(u_sess->SPI_cxt.SPICacheTable,
                                                            (void*)(&key), HASH_ENTER, &found);
    SPI_GPC_LOG("insert spiplan entry", NULL, func_oid);

    if (found) {
        list_free_ext(hentry->SPIplan_list);
        hentry->SPIplan_list = NULL;
        elog(WARNING, "should not has same old function entry in SPICacheTable");
    } else {
        hentry->SPIplan_list = NULL;
    }
    hentry->func_oid = func_oid;
}

/* add plan for spi keep plan */
void SPICacheTableInsertPlan(uint32 key, SPIPlanPtr spi_plan)
{
    if (unlikely(u_sess->SPI_cxt.SPICacheTable == NULL))
        return;
    bool found = false;
    Assert (spi_plan->saved);
    SPIPlanCacheEnt* hentry = (SPIPlanCacheEnt*)hash_search(u_sess->SPI_cxt.SPICacheTable,
                                                            (void*)(&key), HASH_ENTER, &found);
    if (found) {
#ifdef USE_ASSERT_CHECKING
        /* cannot has same spiplan in list */
        ListCell* cell = NULL;
        if (hentry->SPIplan_list != NULL) {
            foreach(cell, hentry->SPIplan_list) {
                SPIPlanPtr cur = (SPIPlanPtr)lfirst(cell);
                Assert (cur->id != spi_plan->id);
                Assert (cur != spi_plan);
            }
        }
#endif
        SPI_GPC_LOG("insert spiplan into entry", spi_plan, hentry->func_oid);
        MemoryContext old_cxt = MemoryContextSwitchTo(u_sess->SPI_cxt.SPICacheTable->hcxt);
        hentry->SPIplan_list = lappend(hentry->SPIplan_list, spi_plan);
        (void)MemoryContextSwitchTo(old_cxt);
    } else {
        Assert(0);
    }
}

/* only use when delete function, plancache will be freed in SPI_freeplan */
void SPIPlanCacheTableDelete(uint32 key)
{
    if (unlikely(u_sess->SPI_cxt.SPICacheTable == NULL)) {
        return;
    }
    bool found = false;
    SPIPlanCacheEnt* hentry = (SPIPlanCacheEnt*)hash_search(u_sess->SPI_cxt.SPICacheTable,
                                                            (void*)(&key), HASH_REMOVE, &found);
    if (hentry) {
        SPI_GPC_LOG("delete spiplan entry", NULL, hentry->func_oid);
        list_free_ext(hentry->SPIplan_list);
    }
}

void SPIPlanCacheTableDeletePlan(uint32 key, SPIPlanPtr plan)
{
    if (unlikely(u_sess->SPI_cxt.SPICacheTable == NULL))
        return;
    SPIPlanCacheEnt* entry = SPIPlanCacheTableLookup(key);
    if (entry != NULL) {
#ifdef USE_ASSERT_CHECKING
        ListCell* cell = NULL;
        foreach(cell, entry->SPIplan_list) {
            SPIPlanPtr spiplan = (SPIPlanPtr)lfirst(cell);
            if (spiplan->id == plan->id)
                Assert(plan == spiplan);
        }
#endif
        SPI_GPC_LOG("delete spiplan from entry", plan, entry->func_oid);
        entry->SPIplan_list = list_delete_ptr(entry->SPIplan_list, (void*)plan);
    }
}

void SPIPlanCacheTableInvalidPlan(uint32 key)
{
    if (unlikely(u_sess->SPI_cxt.SPICacheTable == NULL)) {
        return;
    }
    bool found = false;
    SPIPlanCacheEnt* hentry = (SPIPlanCacheEnt*)hash_search(u_sess->SPI_cxt.SPICacheTable,
                                                            (void*)(&key), HASH_REMOVE, &found);
    if (hentry) {
        SPI_GPC_LOG("invalid each spiplan entry", NULL, hentry->func_oid);
        ListCell* cell = NULL;
        foreach (cell, hentry->SPIplan_list) {
            SPIPlanPtr spiplan = (SPIPlanPtr)lfirst(cell);
            if (list_length(spiplan->plancache_list) == 0)
                continue;
            ListCell* cl = NULL;
            foreach (cl, spiplan->plancache_list) {
                CachedPlanSource* plansource = (CachedPlanSource*)lfirst(cl);
                if (plansource->gpc.status.InShareTable()) {
                    plansource->gpc.status.SetStatus(GPC_INVALID);
                } else {
                    plansource->is_valid = false;
                    if (plansource->gplan)
                        plansource->gplan->is_valid = false;

                    /*
                     * All context under planManager should be invalid if any dependent
                     * relation has been changed. Thus, all candidate plans and the cached
                     * parsing context, 'GenericRoot', need to be rebuilt.
                     */
                    if (ENABLE_CACHEDPLAN_MGR && plansource->planManager != NULL) {
                        plansource->planManager->is_valid = false;
                    }
                }
            }
        }
    }
}

void SPICacheTableInit()
{
    HASHCTL ctl;
    errno_t rc = EOK;

    /* don't allow double-initialization */
    AssertEreport(u_sess->SPI_cxt.SPICacheTable == NULL, MOD_GPC, "don't allow double-initialization.");
    const int func_per_user = 128;
    rc = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
    securec_check(rc, "\0", "\0");
    ctl.keysize = sizeof(uint32);
    ctl.entrysize = sizeof(SPIPlanCacheEnt);
    ctl.hash = uint32_hash;
    ctl.hcxt = u_sess->cache_mem_cxt;
    u_sess->SPI_cxt.SPICacheTable =
        hash_create("SPIPlanCacheTable", func_per_user, &ctl, HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
    SPI_GPC_LOG("init spiplan cache table", NULL, 0);
}

SPIPlanCacheEnt*  SPIPlanCacheTableLookup(uint32 key)
{
    if (unlikely(u_sess->SPI_cxt.SPICacheTable == NULL))
        return NULL;
    bool found = false;
    SPIPlanCacheEnt* hentry = (SPIPlanCacheEnt*)hash_search(
        u_sess->SPI_cxt.SPICacheTable, (void*)(&key), HASH_FIND, &found);
    if (found)
        return hentry;
    else
        return NULL;
}

/* set unique id in PLpgSQL_expr for gpc */
static void set_id_stmt(PLpgSQL_stmt* stmt, uint32* unique_id);
static void set_id_block(PLpgSQL_stmt_block* block, uint32* unique_id);
static void set_id_if(PLpgSQL_stmt_if* stmt, uint32* unique_id);
static void set_id_case(PLpgSQL_stmt_case* stmt, uint32* unique_id);
static void set_id_return_query(PLpgSQL_stmt_return_query* stmt, uint32* unique_id);
static void set_id_raise(PLpgSQL_stmt_raise* stmt, uint32* unique_id);
static void set_id_dynexecute(PLpgSQL_stmt_dynexecute* stmt, uint32* unique_id);
static void set_id_dynfors(PLpgSQL_stmt_dynfors* stmt, uint32* unique_id);
static void set_id_open(PLpgSQL_stmt_open* stmt, uint32* unique_id);
static void set_id_expr(PLpgSQL_expr* expr, uint32* unique_id);
static void set_id_stmts(List* stmts, uint32* unique_id);

static void set_id_expr(PLpgSQL_expr* expr, uint32* unique_id)
{
    if (expr == NULL)
        return;
    expr->idx = ++(*unique_id);
}
static void set_id_stmts(List* stmts, uint32* unique_id)
{
    ListCell* s = NULL;

    foreach (s, stmts) {
        set_id_stmt((PLpgSQL_stmt*)lfirst(s), unique_id);
    }
}
static void set_id_block(PLpgSQL_stmt_block* block, uint32* unique_id)
{
    set_id_stmts(block->body, unique_id);
    if (block->exceptions != NULL) {
        ListCell* e = NULL;

        foreach (e, block->exceptions->exc_list) {
            PLpgSQL_exception* exc = (PLpgSQL_exception*)lfirst(e);

            set_id_stmts(exc->action, unique_id);
        }
    }
}

static void set_id_if(PLpgSQL_stmt_if* stmt, uint32* unique_id)
{
    ListCell* l = NULL;
    set_id_expr(stmt->cond, unique_id);
    set_id_stmts(stmt->then_body, unique_id);
    foreach (l, stmt->elsif_list) {
        PLpgSQL_if_elsif* elif = (PLpgSQL_if_elsif*)lfirst(l);

        set_id_expr(elif->cond, unique_id);
        set_id_stmts(elif->stmts, unique_id);
    }
    set_id_stmts(stmt->else_body, unique_id);
}
static void set_id_case(PLpgSQL_stmt_case* stmt, uint32* unique_id)
{
    ListCell* l = NULL;
    set_id_expr(stmt->t_expr, unique_id);
    foreach (l, stmt->case_when_list) {
        PLpgSQL_case_when* cwt = (PLpgSQL_case_when*)lfirst(l);
        set_id_expr(cwt->expr, unique_id);
        set_id_stmts(cwt->stmts, unique_id);
    }
    set_id_stmts(stmt->else_stmts, unique_id);
}

static void set_id_open(PLpgSQL_stmt_open* stmt, uint32* unique_id)
{
    ListCell* lc = NULL;
    set_id_expr(stmt->argquery, unique_id);
    set_id_expr(stmt->query, unique_id);
    set_id_expr(stmt->dynquery, unique_id);
    foreach (lc, stmt->params) {
        set_id_expr((PLpgSQL_expr*)lfirst(lc), unique_id);
    }
}
static void set_id_return_query(PLpgSQL_stmt_return_query* stmt, uint32* unique_id)
{
    ListCell* lc = NULL;
    set_id_expr(stmt->query, unique_id);
    set_id_expr(stmt->dynquery, unique_id);
    foreach (lc, stmt->params) {
        set_id_expr((PLpgSQL_expr*)lfirst(lc), unique_id);
    }
}
static void set_id_raise(PLpgSQL_stmt_raise* stmt, uint32* unique_id)
{
    ListCell* lc = NULL;
    foreach (lc, stmt->params) {
        set_id_expr((PLpgSQL_expr*)lfirst(lc), unique_id);
    }
    foreach (lc, stmt->options) {
        PLpgSQL_raise_option* opt = (PLpgSQL_raise_option*)lfirst(lc);
        set_id_expr(opt->expr, unique_id);
    }
}

static void set_id_dynexecute(PLpgSQL_stmt_dynexecute* stmt, uint32* unique_id)
{
    ListCell* lc = NULL;
    set_id_expr(stmt->query, unique_id);
    foreach (lc, stmt->params) {
        set_id_expr((PLpgSQL_expr*)lfirst(lc), unique_id);
    }
}

static void set_id_dynfors(PLpgSQL_stmt_dynfors* stmt, uint32* unique_id)
{
    ListCell* lc = NULL;
    set_id_stmts(stmt->body, unique_id);
    set_id_expr(stmt->query, unique_id);
    foreach (lc, stmt->params) {
        set_id_expr((PLpgSQL_expr*)lfirst(lc), unique_id);
    }
}

static void set_id_stmt(PLpgSQL_stmt* stmt, uint32* unique_id)
{
    switch ((enum PLpgSQL_stmt_types)stmt->cmd_type) {
        case PLPGSQL_STMT_BLOCK:
            set_id_block((PLpgSQL_stmt_block*)stmt, unique_id);
            break;
        case PLPGSQL_STMT_ASSIGN:
            set_id_expr(((PLpgSQL_stmt_assign*)stmt)->expr, unique_id);
            break;
        case PLPGSQL_STMT_IF:
            set_id_if((PLpgSQL_stmt_if*)stmt, unique_id);
            break;
        case PLPGSQL_STMT_CASE:
            set_id_case((PLpgSQL_stmt_case*)stmt, unique_id);
            break;
        case PLPGSQL_STMT_LOOP:
            set_id_stmts(((PLpgSQL_stmt_loop*)stmt)->body, unique_id);
            break;
        case PLPGSQL_STMT_WHILE:
            set_id_expr(((PLpgSQL_stmt_while*)stmt)->cond, unique_id);
            set_id_stmts(((PLpgSQL_stmt_while*)stmt)->body, unique_id);
            break;
        case PLPGSQL_STMT_FORI:
            set_id_expr(((PLpgSQL_stmt_fori*)stmt)->lower, unique_id);
            set_id_expr(((PLpgSQL_stmt_fori*)stmt)->upper, unique_id);
            set_id_expr(((PLpgSQL_stmt_fori*)stmt)->step, unique_id);
            set_id_stmts(((PLpgSQL_stmt_fori*)stmt)->body, unique_id);
            break;
        case PLPGSQL_STMT_FORS:
            set_id_stmts(((PLpgSQL_stmt_fors*)stmt)->body, unique_id);
            set_id_expr(((PLpgSQL_stmt_fors*)stmt)->query, unique_id);
            break;
        case PLPGSQL_STMT_FORC:
            set_id_stmts(((PLpgSQL_stmt_forc*)stmt)->body, unique_id);
            set_id_expr(((PLpgSQL_stmt_forc*)stmt)->argquery, unique_id);
            break;
        case PLPGSQL_STMT_FOREACH_A:
            set_id_expr(((PLpgSQL_stmt_foreach_a*)stmt)->expr, unique_id);
            set_id_stmts(((PLpgSQL_stmt_foreach_a*)stmt)->body, unique_id);
            break;
        case PLPGSQL_STMT_EXIT:
            set_id_expr(((PLpgSQL_stmt_exit*)stmt)->cond, unique_id);
            break;
        case PLPGSQL_STMT_RETURN:
            set_id_expr(((PLpgSQL_stmt_return*)stmt)->expr, unique_id);
            break;
        case PLPGSQL_STMT_RETURN_NEXT:
            set_id_expr(((PLpgSQL_stmt_return_next*)stmt)->expr, unique_id);
            break;
        case PLPGSQL_STMT_RETURN_QUERY:
            set_id_return_query((PLpgSQL_stmt_return_query*)stmt, unique_id);
            break;
        case PLPGSQL_STMT_RAISE:
            set_id_raise((PLpgSQL_stmt_raise*)stmt, unique_id);
            break;
        case PLPGSQL_STMT_EXECSQL:
            set_id_expr(((PLpgSQL_stmt_execsql*)stmt)->sqlstmt, unique_id);
            break;
        case PLPGSQL_STMT_DYNEXECUTE:
            set_id_dynexecute((PLpgSQL_stmt_dynexecute*)stmt, unique_id);
            break;
        case PLPGSQL_STMT_DYNFORS:
            set_id_dynfors((PLpgSQL_stmt_dynfors*)stmt, unique_id);
            break;
        case PLPGSQL_STMT_OPEN:
            set_id_open((PLpgSQL_stmt_open*)stmt, unique_id);
            break;
        case PLPGSQL_STMT_FETCH:
            set_id_expr(((PLpgSQL_stmt_fetch*)stmt)->expr, unique_id);
            break;
        case PLPGSQL_STMT_PERFORM:
            set_id_expr(((PLpgSQL_stmt_perform*)stmt)->expr, unique_id);
            break;
        default:
            break;
    }
}

void set_func_expr_unique_id(PLpgSQL_function* func)
{
    uint32 unique_id = 0;
    int i;
    for (i = 0; i < func->ndatums; i++) {
        PLpgSQL_datum* d = func->datums[i];
        switch (d->dtype) {
            case PLPGSQL_DTYPE_VAR: {
                PLpgSQL_var* var = (PLpgSQL_var*)d;
                set_id_expr(var->default_val, &unique_id);
                set_id_expr(var->cursor_explicit_expr, &unique_id);
            } break;
            case PLPGSQL_DTYPE_ARRAYELEM:
                set_id_expr(((PLpgSQL_arrayelem*)d)->subscript, &unique_id);
                break;
            case PLPGSQL_DTYPE_ROW:
            case PLPGSQL_DTYPE_RECORD: {
                PLpgSQL_row* row = (PLpgSQL_row*)d;
                set_id_expr(row->default_val, &unique_id);
            } break;
            default:
                break;
        }
    }
    if (func->action != NULL) {
        set_id_block(func->action, &unique_id);
    }
}

bool SPIParseEnableGPC(const Node *node)
{
    if (node == NULL)
        return false;
    switch (nodeTag(node)) {
        case T_SelectStmt:
            if (((SelectStmt*)node)->intoClause)
                return false;
            /* fall through */
        case T_MergeStmt:
        case T_UpdateStmt:
        case T_DeleteStmt:
        case T_InsertStmt:
            return true;
        /* like needRecompilePlan */
        case T_CreateTableAsStmt:
        case T_TransactionStmt:
        default:
            return false;
    }
    /* keep compiler quite */
    return false;
}

