/*
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2002-2007, PostgreSQL Global Development Group
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
 * globalplancache.h
 *
 * IDENTIFICATION
 *        src/include/utils/globalplancache.h
 *
 *---------------------------------------------------------------------------------------
 */
#ifndef GLOBALPLANCACHE_H
#define GLOBALPLANCACHE_H

#include "knl/knl_variable.h"

#include "pgxc/pgxc.h"
#include "storage/sinval.h"

#define GPC_NUM_OF_BUCKETS (128)
#define GLOBALPLANCACHEKEY_MAGIC (953717831)
#define CAS_SLEEP_DURATION (2)

#define ENABLE_GPC (g_instance.attr.attr_common.enable_global_plancache == true)
#define ENABLE_CN_GPC (IS_PGXC_COORDINATOR && \
                       g_instance.attr.attr_common.enable_global_plancache == true && \
                       g_instance.attr.attr_common.enable_thread_pool == true)
#define ENABLE_DN_GPC (IS_PGXC_DATANODE && \
                       (!(IS_SINGLE_NODE)) && \
                       g_instance.attr.attr_common.enable_global_plancache == true && \
                       g_instance.attr.attr_common.enable_thread_pool == true)

typedef enum PGXCNode_HandleGPC
{
    HANDLE_RUN,
    HANDLE_CLEAN,
    HANDLE_FIRST_SEND
} PGXCNode_HandleGPC;


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
} PreparedStatement;

typedef struct GPCPreparedStatement
{
    uint64 global_sess_id;
    DList* prepare_statement_list; /* the list of the prepare statements of the different queries with the same gid,
                                    * we assume the amount of different prepare statements wont be huge, so we chose
                                    * linked list instead of HTAB */
} GPCPreparedStatement;

typedef struct GPCBucketInfo
{
    int64    bucket_size;
    int32    CAS_flag;
    int        entries_count;
    int        prepare_count;
    MemoryContext context;
    uint32     curr_hash_code;
    uint32    curr_cache_votes;
} GPCBucketInfo;

typedef struct GPCKey
{
    uint32        query_length;
    const char    *query_string;
} GPCKey;

typedef struct GPCEntry
{
    /*
     * We will hash the query_string and its length to find the hash bucket
     */
    GPCKey key;

    /*
     * This is a List of CachedEnvironment.
     * A statement plan could be different between two environments.
     * Hence, we need to keep a list of cached plans based on some environment
     * signature. Note that we use a List instead of another Hash table because
     * we assume we will not have that many variance of environments.
     */
    DList *cachedPlans;
    
    /* Number of CachedPlanSource referencing to this GPC Entry. A CachedPlanSource
    * could be pointing to this entry before the CachedPlanSource is inserted into the GPC.
    */
    int refcount;
    bool is_valid;
    int lockId;
    int32 CAS_flag;
    int magic;
} GPCEntry;

typedef struct GPCEnv
{
    GPCEntry *globalplancacheentry;
    CachedPlanSource *plansource;
    MemoryContext context;
    int64 memory_size;
    bool filled;

    int num_params;
    Oid database_id;
    char schema_name[NAMEDATALEN];
    
#ifdef PGXC
    // stores the bool GUC parameters for PGXC
    uint16 env_signature_pgxc;
    int best_agg_plan;    // QUERY_TUNING_METHOD
#endif
#ifdef DEBUG_BOUNDED_SORT
    bool optimize_bounded_sort;    // QUERY_TUNING_METHOD
#endif
    // stores the bool GUC parameters
    uint32 env_signature;
    uint32 env_signature2;
    int query_dop_tmp;    // QUERY_TUNING
    int rewrite_rule;    // QUERY_TUNING
    int codegen_strategy;    // QUERY_TUNING
    int plan_mode_seed;            // QUERY_TUNING_METHOD
    char *expected_computing_nodegroup;    // QUERY_TUNING_METHOD
    char *default_storage_nodegroup;    // QUERY_TUNING_METHOD
    int effective_cache_size;    // QUERY_TUNING_COST
    int codegen_cost_threshold;    // QUERY_TUNING_COST
    double seq_page_cost;        // QUERY_TUNING_COST
    double random_page_cost;    // QUERY_TUNING_COST
    double cpu_tuple_cost;        // QUERY_TUNING_COST
    double allocate_mem_cost;    // QUERY_TUNING_COST
    double cpu_index_tuple_cost;    // QUERY_TUNING_COST
    double cpu_operator_cost;    // QUERY_TUNING_COST
    double stream_multiple;        // QUERY_TUNING_COST
    int geqo_threshold;        // QUERY_TUNING_GEQO
    int Geqo_effort;        // QUERY_TUNING_GEQO
    int Geqo_pool_size;        // QUERY_TUNING_GEQO
    int Geqo_generations;        // QUERY_TUNING_GEQO
    double Geqo_selection_bias;    // QUERY_TUNING_GEQO
    double Geqo_seed;        // QUERY_TUNING_GEQO
    int default_statistics_target;    // QUERY_TUNING_OTHER
    int from_collapse_limit;    // QUERY_TUNING_OTHER
    int join_collapse_limit;    // QUERY_TUNING_OTHER
    int cost_param;            // QUERY_TUNING_OTHER
    int schedule_splits_threshold;    // QUERY_TUNING_OTHER
    int hashagg_table_size;        // QUERY_TUNING_OTHER
    double cursor_tuple_fraction;    // QUERY_TUNING_OTHER
    int constraint_exclusion;    // QUERY_TUNING_OTHER
    int qrw_inlist2join_optmode;// QUERY_TUNING_OTHER2
    int skew_strategy_store;// QUERY_TUNING_OTHER2
    unsigned int behavior_compat_flags;
    int datestyle;
    int dateorder;
} GPCEnv;

typedef struct GPCStatus
{
    char *query;
    // int option;
    int refcount;
    bool valid;
    Oid DatabaseID;
    char *schema_name;
    int params_num;
} GPCStatus;

typedef struct GPCPrepareStatus
{
    char *statement_name;
    int refcount;
    int global_session_id;
    bool is_shared;
} GPCPrepareStatus;

class GlobalPlanCache : public BaseObject
{
public:
    GlobalPlanCache();
    
    /* refconut control */
    void RefcountAdd(CachedPlanSource *plansource) const;
    void RefcountSub(CachedPlanSource *plansource) const;

    /* global plan cache htab control */
    void PlanInit();
    void PlanStore(CachedPlanSource *plansource);
    GPCEnv* PlanFetch(const char *query_string, uint32 query_len, int num_params);
    void InvalidPlanDrop();
    void PlanDrop(GPCEnv *cachedenv);
    Datum PlanClean();
    uint32 GetBucket(uint32 hashvalue);

    /* global prepare stmt htab control */
    void PrepareInit();
    void PrepareStore(const char *stmt_name,
                           CachedPlanSource *plansource,
                           bool from_sql);
    PreparedStatement* PrepareFetch(const char *stmt_name, bool throwError);
    void PrepareDrop(const char *stmt_name, bool showError);
    void PrepareUpdate(CachedPlanSource *plansource, CachedPlanSource *share_plansource, bool throwError);

    /* cache invalid */
    bool MsgCheck(const SharedInvalidationMessage *msg);
    void LocalMsgCheck(GPCEntry *entry, int tot, const int *idx, const SharedInvalidationMessage *msgs);
    void InvalMsg(const SharedInvalidationMessage *msgs, int n);

    /* transaction */
    void RecreateCachePlan(PreparedStatement *entry);
    void Commit();
    GPCEnv *EnvCreate();
    void GetSchemaName(GPCEnv *env);
    void EnvFill(GPCEnv *env);
    void PrepareDropAll(uint64 global_sess_id, bool need_lock);
    void PrepareClean(uint32 cn_id);
    void CheckTimeline(uint32 timeline);

    /* system function */
    void* GetStatus(uint32 *num);
    void* GetPrepareStatus(uint32 *num);
    void SendPrepareDestoryMsg();

private:

    HTAB* m_global_plan_cache;
    struct GPCBucketInfo *m_gpc_bucket_info_array;
    DList *m_gpc_invalid_plansource;

    HTAB* m_global_prepared;

    HTAB* m_cn_timeline;
};

extern GlobalPlanCache *GPC;
extern uint64 generate_global_sessid(uint64 local_id);

extern Datum GPCPlanClean(PG_FUNCTION_ARGS);

#endif   /* PLANCACHE_H */
