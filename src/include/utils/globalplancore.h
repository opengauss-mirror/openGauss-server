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
 * ---------------------------------------------------------------------------------------
 *
 * globalplancore.h
 *
 *        global plan cache kernel header
 *
 * IDENTIFICATION
 *        /src/include/utils/globalplancore.h
 *
 *---------------------------------------------------------------------------------------
 */
#ifndef GLOBALPLANCORE_H
#define GLOBALPLANCORE_H

#include "executor/spi.h"
#include "executor/spi_priv.h"
#include "knl/knl_variable.h"
#include "pgxc/pgxc.h"
#include "storage/sinval.h"
#include "utils/plancache.h"

#ifndef ENABLE_LITE_MODE
#define GPC_NUM_OF_BUCKETS (128)
#else
#define GPC_NUM_OF_BUCKETS (2)
#endif

#define GPC_HTAB_SIZE (128)
#define GLOBALPLANCACHEKEY_MAGIC (953717831)
#define CAS_SLEEP_DURATION (2)

#define ENABLE_GPC (g_instance.attr.attr_common.enable_global_plancache == true && \
                       g_instance.attr.attr_common.enable_thread_pool == true)

#define ENABLE_DN_GPC (IS_PGXC_DATANODE && \
                       (!(IS_SINGLE_NODE)) && \
                       g_instance.attr.attr_common.enable_global_plancache == true && \
                       g_instance.attr.attr_common.enable_thread_pool == true)

#ifdef ENABLE_MULTIPLE_NODES
#define ENABLE_CN_GPC (IS_PGXC_COORDINATOR && \
                       g_instance.attr.attr_common.enable_global_plancache == true && \
                       g_instance.attr.attr_common.enable_thread_pool == true)
#define IN_GPC_GRAYRELEASE_CHANGE (u_sess->attr.attr_common.enable_gpc_grayrelease_mode == true)
#else
#define ENABLE_CN_GPC (g_instance.attr.attr_common.enable_global_plancache == true && \
                       g_instance.attr.attr_common.enable_thread_pool == true)
#define IN_GPC_GRAYRELEASE_CHANGE (false)
#endif

typedef enum PGXCNode_HandleGPC
{
    HANDLE_RUN,
    HANDLE_FIRST_SEND
} PGXCNode_HandleGPC;

typedef struct GPCHashCtl
{
    int             count;
    int             lockId;
    HTAB           *hash_tbl;
    MemoryContext   context;
} GPCHashCtl;

typedef struct GPCPlainEnv
{
    Oid database_id;
    uint16 env_signature_pgxc;
    // stores the bool GUC parameters
    uint32 env_signature;
    uint32 env_signature2;
    int best_agg_plan;    // QUERY_TUNING_METHOD
    int query_dop_tmp;    // QUERY_TUNING
    int rewrite_rule;    // QUERY_TUNING
    int codegen_strategy;    // QUERY_TUNING
    int plan_mode_seed;            // QUERY_TUNING_METHOD
    int effective_cache_size;    // QUERY_TUNING_COST
    int codegen_cost_threshold;    // QUERY_TUNING_COST
    int geqo_threshold;        // QUERY_TUNING_GEQO
    int Geqo_effort;        // QUERY_TUNING_GEQO
    int Geqo_pool_size;        // QUERY_TUNING_GEQO
    int Geqo_generations;        // QUERY_TUNING_GEQO
    int default_statistics_target;    // QUERY_TUNING_OTHER
    int from_collapse_limit;    // QUERY_TUNING_OTHER
    int join_collapse_limit;    // QUERY_TUNING_OTHER
    int cost_param;            // QUERY_TUNING_OTHER
    int schedule_splits_threshold;    // QUERY_TUNING_OTHER
    int hashagg_table_size;        // QUERY_TUNING_OTHER
    int constraint_exclusion;    // QUERY_TUNING_OTHER
    int qrw_inlist2join_optmode;// QUERY_TUNING_OTHER2
    int skew_strategy_store;// QUERY_TUNING_OTHER2
    unsigned int b_format_behavior_compat_flags;
    unsigned int behavior_compat_flags;
    unsigned int plsql_compile_behavior_compat_flags;
    int datestyle;
    int dateorder;
    int plancachemode; // QUERY_TUNING_OTHER
    int sql_beta_feature; // QUERY_TUNING

    double seq_page_cost;        // QUERY_TUNING_COST
    double random_page_cost;    // QUERY_TUNING_COST
    double cpu_tuple_cost;        // QUERY_TUNING_COST
    double allocate_mem_cost;    // QUERY_TUNING_COST
    double cpu_index_tuple_cost;    // QUERY_TUNING_COST
    double cpu_operator_cost;    // QUERY_TUNING_COST
    double stream_multiple;        // QUERY_TUNING_COST
    double default_limit_rows;      // QUERY_TUNING_COST
    double Geqo_selection_bias;    // QUERY_TUNING_GEQO
    double Geqo_seed;        		// QUERY_TUNING_GEQO
    double cursor_tuple_fraction;    // QUERY_TUNING_OTHER
}GPCPlainEnv;

typedef struct GPCEnv
{
    bool filled;
    GPCPlainEnv  plainenv;

    /* vary name.*/
    int num_params;
    Oid* param_types;
    struct OverrideSearchPath* search_path;
    char schema_name[NAMEDATALEN];
    char expected_computing_nodegroup[NAMEDATALEN];    // QUERY_TUNING_METHOD
    char default_storage_nodegroup[NAMEDATALEN];    // QUERY_TUNING_METHOD
    /* row security */
    bool depends_on_role;
    Oid user_oid; /* only check if has row security */
} GPCEnv;

typedef struct GPCKey
{
    uint32          query_length;
    /* query_string is plansource->querystring */
    const char     *query_string;
    GPCEnv          env;
    SPISign         spi_signature;
} GPCKey;

typedef struct GPCVal
{
    CachedPlanSource*  plansource;
    instr_time  last_use_time;
    volatile uint32 used_count; /* fetched times */
} GPCVal;

typedef struct GPCEntry
{
    /*
     * We will hash the query_string and its length to find the hash bucket
     */
    GPCKey  key;
    GPCVal  val;

} GPCEntry;

typedef struct GPCViewStatus
{
    char *query;
    //int option;
    int refcount;
    bool valid;
    Oid DatabaseID;
    char *schema_name;
    int params_num;
    Oid func_id;
} GPCViewStatus;


/*
 * The data structure representing a prepared statement.  This is now just
 * a thin veneer over a plancache entry --- the main addition is that of
 * a name.
 *
 * Note: all subsidiary storage lives in the referenced plancache entry.
 */
typedef struct PreparedStatement
{
    /* dynahash.c requires key to be first field */
    char        stmt_name[NAMEDATALEN];
    CachedPlanSource *plansource;        /* the actual cached plan */
    bool        from_sql;        /* prepared via SQL, not FE/BE protocol? */
    TimestampTz prepare_time;    /* the time when the stmt was prepared */
    bool        has_prepare_dn_stmt; /* datanode statment has prepared or not */
} PreparedStatement;

/* for gpc, help to locate plancache and spiplan */
typedef struct SPIPlanCacheEnt {
    uint32 key;
    Oid func_oid;
    List* SPIplan_list;
} plpgsql_SPIPlanCacheEnt;

inline uint32 GetBucket(uint32 hashvalue)
{
    return (hashvalue % GPC_NUM_OF_BUCKETS);
}

extern Datum GPCPlanClean(PG_FUNCTION_ARGS);
extern void GPCResetAll();
void GPCCleanDatanodeStatement(int dn_stmt_num, const char* stmt_name);
void GPCReGplan(CachedPlanSource* plansource);
void GPCCleanUpSessionSavedPlan();
List* CopyLocalStmt(const List* stmt_list, const MemoryContext parent_cxt, MemoryContext* plan_context);
bool SPIParseEnableGPC(const Node *node);
void CleanSessGPCPtr(knl_session_context* currentSession);
void CleanSessionGPCDetach(knl_session_context* currentSession);

/* for HTAB SPICacheTable, global procedure plancache */
extern SPIPlanCacheEnt*  SPIPlanCacheTableLookup(uint32 key);
extern void SPIPlanCacheTableDeletePlan(uint32 key, SPIPlanPtr plan);
extern void SPIPlanCacheTableDelete(uint32 key);
extern void SPIPlanCacheTableInvalidPlan(uint32 key);
extern void SPIPlanCacheTableDeleteFunc(Oid func_oid);
extern void SPICacheTableInit();
extern void SPICacheTableInsert(uint32 key, Oid func_oid);
extern void SPICacheTableInsertPlan(uint32 key, SPIPlanPtr spi_plan);
extern uint32 SPICacheHashFunc(const void *key, Size keysize);
/* set unique id for each PLpgSQL_expr in func */
extern void set_func_expr_unique_id(PLpgSQL_function* func);

inline void gpc_record_log(const char* action, const char* filename, int lineno,
                           CachedPlanSource* plansource, const char* stmtname)
{
    ereport(DEBUG3, (errmodule(MOD_GPC), errcode(ERRCODE_LOG),
        errmsg("GPC (Action:%s, Location %s,%d), (dn_session:%lu), plansource %p, stmt: %s ",
                action, filename, lineno, u_sess->session_id, plansource, stmtname)));
}

inline void gpc_spi_record_log(const char* action, const char* filename, int lineno, SPIPlanPtr spiplan, Oid func_oid)
{

    ereport(DEBUG3, (errmodule(MOD_GPC), errcode(ERRCODE_LOG),
        errmsg("SPIGPC (Action:%s, Location %s,%d), (cn_session:%lu), (func_oid: %u) spiplan %p, plansource %p ",
                action, filename, lineno, u_sess->session_id, func_oid, spiplan,
                ((spiplan != NULL) && (list_length(spiplan->plancache_list) == 1)) ?
                 (CachedPlanSource*)(lfirst(spiplan->plancache_list->head)) : NULL)));
}

#define SPI_GPC_LOG(action, spiplan, func_oid) \
        (gpc_spi_record_log(action, __FILE__, __LINE__, spiplan, func_oid))

#define GPC_LOG(action, plansource, stmt) \
    (gpc_record_log(action, __FILE__, __LINE__, plansource, stmt))

inline void cn_gpc_record_log(const char* action, const char* filename, int lineno,
                           CachedPlanSource* plansource, const char* stmtname)
{
    ereport(DEBUG3, (errmodule(MOD_GPC), errcode(ERRCODE_LOG),
        errmsg("CNGPC (Action:%s, Location %s,%d), (cn_session:%lu), plansource %p, stmt: %s ",
                action, filename, lineno, u_sess->session_id, plansource, stmtname)));
}

#define CN_GPC_LOG(action, plansource, stmt) \
    (cn_gpc_record_log(action, __FILE__, __LINE__, plansource, stmt))

#endif   /* GLOBALPLANCORE_H */
