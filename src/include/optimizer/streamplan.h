/* ---------------------------------------------------------------------------------------
 * 
 * streamplan.h
 *        prototypes for stream plan.
 * 
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *        src/include/optimizer/streamplan.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef STREAMPLAN_H
#define STREAMPLAN_H

#include "catalog/pgxc_class.h"
#include "nodes/parsenodes.h"
#include "optimizer/pgxcplan.h"
#include "optimizer/pgxcship.h"
#include "optimizer/paths.h"
#include "pgxc/nodemgr.h"
#include "pgxc/pgxc.h"

#define NOTPLANSHIPPING_LENGTH 256

typedef struct ShippingInfo {
    bool need_log;
    char not_shipping_reason[NOTPLANSHIPPING_LENGTH];
} ShippingInfo;

#ifndef STREAMPLAN
#define STREAMPLAN
#endif

#define IS_STREAM (u_sess->opt_cxt.is_stream && !IsConnFromCoord())
/* Is stream come from datanode for the  scene of gpu  acceleration. */
#define IS_STREAM_DATANODE (u_sess->opt_cxt.is_stream && IsConnFromDatanode())
#define INITIAL_PLAN_NODE_ID 1
#define INITIAL_PARENT_NODE_ID 0
#define FLAG_SERIALIZED_PLAN 'Z'
#define IS_STREAM_PLAN (IS_PGXC_COORDINATOR && IS_STREAM && check_stream_support())
#define STREAM_IS_LOCAL_NODE(type) (type == LOCAL_DISTRIBUTE || type == LOCAL_ROUNDROBIN || type == LOCAL_BROADCAST)
#define SET_DOP(dop) (dop > 1 ? dop : 1)

#define RANDOM_SHIPPABLE (u_sess->opt_cxt.is_randomfunc_shippable && IS_STREAM_PLAN)

extern THR_LOCAL ShippingInfo notshippinginfo;

#define STREAM_RECURSIVECTE_SUPPORTED (IS_STREAM_PLAN && u_sess->attr.attr_sql.enable_stream_recursive)

#define NO_FORM_CLAUSE(query) ((!query->jointree || !query->jointree->fromlist) && (!query->setOperations))

typedef enum {
    STREAM_GATHER = 0,
    STREAM_BROADCAST,
    STREAM_REDISTRIBUTE,
    STREAM_ROUNDROBIN,
    STREAM_HYBRID,
    STREAM_LOCAL,
    STREAM_NONE
} StreamType;

extern char* StreamTypeToString(StreamType type);

typedef enum {
    PARALLEL_NONE = 0,       /* Default value, means no parallelization. */
    REMOTE_DISTRIBUTE,       /* Distribute data to all nodes. */
    REMOTE_SPLIT_DISTRIBUTE, /* Distribute data to all parallel threads at all nodes. */
    REMOTE_BROADCAST,        /* Broadcast data to all nodes. */
    REMOTE_SPLIT_BROADCAST,  /* Broadcast data to all parallel threads all nodes. */
    REMOTE_HYBRID,           /* Hybrid send data. */
    LOCAL_DISTRIBUTE,        /* Distribute data to all threads at local node. */
    LOCAL_BROADCAST,         /* Broadcast data to all threads at local node. */
    LOCAL_ROUNDROBIN         /* Roundrobin data to all threads at local node. */
} SmpStreamType;

extern char* SmpStreamTypeToString(SmpStreamType type);

typedef struct ParallelDesc {
    int consumerDop;          /* DOP of consumer side. */
    int producerDop;          /* DOP of producer side. */
    SmpStreamType distriType; /* Tag the data distribution type. */
} ParallelDesc;

typedef struct StreamInfo {
    StreamType type;      /* Stream type. */
    Path* subpath;        /* Subpath for this stream path. */
    List* stream_keys;    /* Distribute keys for this stream. */
    List* ssinfo;         /* Skew info list. */
    ParallelDesc smpDesc; /* Parallel execution info. */
    double multiple;      /* Skew multiple for redistribution. */
} StreamInfo;

typedef struct StreamInfoPair {
    StreamInfo inner_info; /* Stream info for inner side of join. */
    StreamInfo outer_info; /* Stream info for outer side of join. */
    uint32 skew_optimize;
} StreamInfoPair;

typedef enum StreamReason {
    STREAMREASON_NONE = 0x00,
    STREAMREASON_DISTRIBUTEKEY = 0x01,
    STREAMREASON_NODEGROUP = 0x02
} StreamReason;

typedef struct Stream {
    Scan scan;
    StreamType type;           /* indicate it's Redistribute/Broadcast. */
    char* plan_statement;
    ExecNodes* consumer_nodes; /* indicate nodes group that send to. */
    List* distribute_keys;
    bool is_sorted;            /* true if the underlying node is sorted, default false. */
    SimpleSort* sort;          /* do the merge sort if producer is sorted */
    bool is_dummy;             /* is dummy means the stream thread init by this stream node
                                * will end asap and init a child stream thread. */

    ParallelDesc smpDesc;      /* the parallel description of the stream. */
    char* jitted_serialize;    /* jitted serialize function for vecstream */
    List* skew_list;
    int stream_level;          /* number of stream level (starts from 1), normally
                                * used for recursive sql execution that under recursive-union operator. */
    ExecNodes* origin_consumer_nodes;
    bool is_recursive_local;   /* LOCAL GATHER for recursive */
} Stream;

typedef Stream VecStream;

typedef struct {
    short nodeid;
    volatile uint32 seed;
    volatile bool initialized;
} IdGen;

typedef struct {
    short nodeid;
    volatile uint64 seed;
    volatile bool initialized;
} Id64Gen;

/*
 * All stream-type paths share these fields.
 */
typedef struct StreamPath {
    Path path;
    StreamType type;
    Path* subpath;
    ParallelDesc* smpDesc; /* the parallel description of the stream. */
    Distribution consumer_distribution;
    List* skew_list;
} StreamPath;

typedef struct {
    bool collect_vars;
    List* aggs;
    List* vars;
} foreign_qual_context;

typedef struct {
    Relids outer_relids;         /* relids to judge if supplying parameters in subpath */
    bool only_check_stream;      /* if we should only check stream or also check paramterized path */
    bool under_materialize_all;  /* if traversing under rescan-avoid materialize */
    bool has_stream;             /* if there's stream node in the path */
    bool has_parameterized_path; /* If parameter is passed in the path */
    bool has_cstore_index_delta; /* if there is cstore index scan with delta data */
} ContainStreamContext;

typedef enum {
    CPLN_DEFAULT = 0x00,
    CPLN_ONE_WAY = 0x01,           /* Search in one way without go into operators with many branchs */
    CPLN_NO_IGNORE_MATERIAL = 0x10 /* search all plannode, including material */
} ContainPlanNodeMode;

extern Id64Gen gt_queryId;

extern Plan* create_stream_plan(PlannerInfo* root, StreamPath* best_path);
extern uint32 generate_unique_id(IdGen* gen);
extern uint64 generate_unique_id64(Id64Gen* gen);
extern void set_default_stream();
extern void set_stream_off();
extern bool stream_walker(Node* node, void* context);
extern List* contains_specified_func(Node* node, contain_func_context* context);
extern contain_func_context init_contain_func_context(List* funcids, bool find_all = false);
extern int2vector* get_baserel_distributekey_no(Oid relid);
extern List* build_baserel_distributekey(RangeTblEntry* rte, int relindex);
extern char locator_type_join(char inner_locator_type, char outer_locator_type);
extern Plan* make_simple_RemoteQuery(
    Plan* lefttree, PlannerInfo* root, bool is_subplan, ExecNodes* target_exec_nodes = NULL);
extern void add_remote_subplan(PlannerInfo* root, RemoteQuery* result_node);
extern void build_remote_subplanOrSQL(PlannerInfo* root, RemoteQuery* result_node);
extern ExecNodes* get_random_data_nodes(char locatortype, Plan* plan);
extern void inherit_plan_locator_info(Plan* plan, Plan* subplan);
extern void inherit_path_locator_info(Path* path, Path* subpath);
extern void finalize_node_id(Plan* result_plan, int* plan_node_id, int* parent_node_id, int* num_streams,
    int* num_plannodes, int* total_num_streams, int* max_push_sql_num, int* gather_count, List* subplans,
    List* subroots, List** initplans, int* subplan_ids, bool is_under_stream, bool is_under_ctescan,
    bool is_data_node_exec, bool is_read_only, NodeGroupInfoContext* node_group_info_context);
extern Path* create_stream_path(PlannerInfo* root, RelOptInfo* rel, StreamType type, List* distribute_keys,
    List* pathkeys, Path* subpath, double skew, Distribution* target_distribution = NULL, ParallelDesc* smp_desc = NULL,
    List* ssinfo = NIL);
extern bool is_execute_on_coordinator(Plan* plan);
extern bool is_execute_on_datanodes(Plan* plan);
extern bool is_execute_on_allnodes(Plan* plan);
extern bool is_replicated_plan(Plan* plan);
extern bool is_hashed_plan(Plan* plan);
extern ExecNodes* stream_merge_exec_nodes(Plan* lefttree, Plan* righttree);
extern ExecNodes* get_all_data_nodes(char locatortype);
extern void pushdown_execnodes(Plan* plan, ExecNodes* exec_nodes, bool add_node = false, bool only_nodelist = false);
extern void stream_join_plan(PlannerInfo* root, Plan* join_plan, JoinPath* join_path);
extern char* CompressSerializedPlan(const char* plan_string, int* cLen);
extern char* DecompressSerializedPlan(const char* comp_plan_string, int cLen, int oLen);
extern void SerializePlan(Plan* node, PlannedStmt* planned_stmt, StringInfoData* str, int num_stream, int num_gather,
    bool push_subplan = true);
extern NodeDefinition* get_all_datanodes_def();
extern Plan* mark_distribute_dml(
    PlannerInfo* root, Plan** sourceplan, ModifyTable* mt_plan, List** resultRelations, List* mergeActionList);
extern List* distributeKeyIndex(PlannerInfo* root, List* distributed_keys, List* targetlist);
extern List* make_groupcl_for_append(PlannerInfo* root, List* targetlist);
extern bool is_broadcast_stream(Stream* stream);
extern bool is_redistribute_stream(Stream* stream);
extern bool is_gather_stream(Stream* stream);
extern bool is_hybid_stream(Stream* stream);
extern void mark_distribute_setop(PlannerInfo* root, Node* node, bool isunionall, bool canDiskeyChange);
extern void cost_stream(StreamPath* stream, int width, bool isJoin = false);
extern void compute_stream_cost(StreamType type, char locator_type, double subrows, double subgblrows,
    double skew_ratio, int width, bool isJoin, List* distribute_keys, Cost* total_cost, double* gblrows,
    unsigned int producer_num_dn, unsigned int consumer_num_dn, ParallelDesc* smpDesc = NULL, List* ssinfo = NIL);

extern void foreign_qual_context_init(foreign_qual_context* context);
extern void foreign_qual_context_free(foreign_qual_context* context);
extern bool is_foreign_expr(Node* node, foreign_qual_context* context);
extern char get_locator_type(Plan* plan);
extern bool contain_special_plan_node(Plan* plan, NodeTag planTag, ContainPlanNodeMode mode = CPLN_DEFAULT);
extern void mark_stream_unsupport();
extern bool check_stream_support();
extern bool is_compatible_type(Oid type1, Oid type2);
extern bool is_args_type_compatible(OpExpr* op_expr);
extern void materialize_remote_query(Plan* result_plan, bool* materialized, bool sort_to_store);
extern void stream_path_walker(Path* path, ContainStreamContext* context);
extern Var* locate_distribute_var(Expr* node);
extern bool add_hashfilter_for_replication(PlannerInfo* root, Plan* plan, List* distribute_keys);
extern bool IsModifyTableForDfsTable(Plan* AppendNode);
extern bool has_subplan(
    Plan* result_plan, Plan* parent, ListCell* cell, bool is_left, List** initplans, bool is_search);
extern void confirm_parallel_info(Plan* plan, int dop);
extern List* check_subplan_list(Plan* result_plan);
extern List* check_vartype_list(Plan* result_plan);
extern bool trivial_subqueryscan(SubqueryScan* plan);
extern List* check_random_expr(Plan* result_plan);
extern List* check_func_list(Plan* result_plan);
extern Plan* add_broacast_under_local_sort(PlannerInfo* root, PlannerInfo* subroot, Plan* plan);
extern void disable_unshipped_log(Query* query, shipping_context* context);
extern void output_unshipped_log();

/* Function for smp. */
extern ParallelDesc* create_smpDesc(int consumer_dop, int producer_dop, SmpStreamType smp_type);
extern Plan* create_local_gather(Plan* plan);
extern Plan* create_local_redistribute(PlannerInfo* root, Plan* lefttree, List* redistribute_keys, double multiple);
extern bool is_local_redistribute_needed(Plan* subplan);
extern uint2* get_bucketmap_by_execnode(ExecNodes* exec_node, PlannedStmt* plannedstmt);
extern Oid get_oridinary_or_foreign_relid(List* rtable);
extern uint2* GetGlobalStreamBucketMap(PlannedStmt* planned_stmt);
extern int pickup_random_datanode_from_plan(Plan* plan);
extern bool canSeparateComputeAndStorageGroupForDelete(PlannerInfo* root);
extern bool isAllParallelized(List* subplans);
extern bool judge_lockrows_need_redistribute(
    PlannerInfo* root, Plan* subplan, Form_pgxc_class target_classForm, Index result_rte_idx);
extern List* getSubPlan(Plan* node, List* subplans, List* initplans);
extern char* GetStreamTypeStrOf(StreamPath* path);
extern void GetHashTableCount(Query* parse, List* cteList, int* ccontext);

/* Macros for LZ4 error handle */
#define validate_LZ4_compress_result(res, module, hint)                                                               \
    do {                                                                                                              \
        if (unlikely(res < 0)) {                                                                                      \
            /* should never happen, otherwise bugs inside LZ4 */                                                      \
            ereport(ERROR,                                                                                            \
                (errcode(ERRCODE_DATA_CORRUPTED),                                                                     \
                    errmodule(module),                                                                                \
                    errmsg("%s : LZ4_compress_default failed trying to compress the data", hint)));                   \
        } else if (unlikely(res == 0)) {                                                                              \
            /* should never happen, otherwise bugs inside LZ4_COMPRESSBOUND */                                        \
            ereport(ERROR,                                                                                            \
                (errcode(ERRCODE_DATA_CORRUPTED),                                                                     \
                    errmodule(module),                                                                                \
                    errmsg("%s : LZ4_compress_default destination buffer couldn't hold all the information", hint))); \
        }                                                                                                             \
    } while (0)
#endif /* STREAMPLAN_H */
