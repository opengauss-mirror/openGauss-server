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
#include "optimizer/stream_check.h"
#include "optimizer/stream_cost.h"
#include "optimizer/stream_util.h"
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
#ifdef ENABLE_MULTIPLE_NODES
#define IS_STREAM_PLAN (IS_PGXC_COORDINATOR && IS_STREAM && check_stream_support())
#else
#define IS_STREAM_PLAN (IS_STREAM && check_stream_support())
#endif
#define STREAM_IS_LOCAL_NODE(type) (type == LOCAL_DISTRIBUTE || type == LOCAL_ROUNDROBIN || type == LOCAL_BROADCAST)
#define SET_DOP(dop) (dop > 1 ? dop : 1)

#define RANDOM_SHIPPABLE (u_sess->opt_cxt.is_randomfunc_shippable && IS_STREAM_PLAN)
 
#define STREAM_RECURSIVECTE_SUPPORTED (IS_STREAM_PLAN && u_sess->attr.attr_sql.enable_stream_recursive)

#define NO_FORM_CLAUSE(query) ((!query->jointree || !query->jointree->fromlist) && (!query->setOperations))
#define IS_STREAM_TYPE(node, stype) (IsA(node, StreamPath) ? ((StreamPath*)node)->type == stype : \
    (IsA(node, Stream) ? ((Stream*)node)->type == stype : false))

extern char* StreamTypeToString(StreamType type);

extern char* SmpStreamTypeToString(SmpStreamType type);

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

extern Id64Gen gt_queryId;

extern Plan* create_stream_plan(PlannerInfo* root, StreamPath* best_path);
extern uint32 generate_unique_id(IdGen* gen);
extern uint64 generate_unique_id64(Id64Gen* gen);
extern void set_default_stream();
extern void set_stream_off();
extern bool stream_walker(Node* node, void* context);
extern contain_func_context init_contain_func_context(List* funcids, bool find_all = false);
extern int2vector* get_baserel_distributekey_no(Oid relid);
extern List* build_baserel_distributekey(RangeTblEntry* rte, int relindex);
extern char locator_type_join(char inner_locator_type, char outer_locator_type);
extern void ProcessRangeListJoinType(Path* joinPath, Path* outerPath, Path* innerPath);
extern Plan* make_simple_RemoteQuery(
    Plan* lefttree, PlannerInfo* root, bool is_subplan, ExecNodes* target_exec_nodes = NULL);
extern void add_remote_subplan(PlannerInfo* root, RemoteQuery* result_node);
extern void build_remote_subplanOrSQL(PlannerInfo* root, RemoteQuery* result_node);
extern ExecNodes* get_random_data_nodes(char locatortype, Plan* plan);
extern void inherit_plan_locator_info(Plan* plan, Plan* subplan);
extern void inherit_path_locator_info(Path* path, Path* subpath);
extern Path* create_stream_path(PlannerInfo* root, RelOptInfo* rel, StreamType type, List* distribute_keys,
    List* pathkeys, Path* subpath, double skew, Distribution* target_distribution = NULL, ParallelDesc* smp_desc = NULL,
    List* ssinfo = NIL);
extern bool is_execute_on_coordinator(Plan* plan);
extern bool is_execute_on_datanodes(Plan* plan);
extern bool is_execute_on_allnodes(Plan* plan);
extern bool is_replicated_plan(Plan* plan);
extern bool is_hashed_plan(Plan* plan);
extern bool is_rangelist_plan(Plan* plan);
extern ExecNodes* stream_merge_exec_nodes(Plan* lefttree, Plan* righttree, bool push_nodelist = false);
extern ExecNodes* get_all_data_nodes(char locatortype);
extern void pushdown_execnodes(Plan* plan, ExecNodes* exec_nodes, bool add_node = false, bool only_nodelist = false);
extern void stream_join_plan(PlannerInfo* root, Plan* join_plan, JoinPath* join_path);
extern void disaster_read_array_init();
extern NodeDefinition* get_all_datanodes_def();
extern List* distributeKeyIndex(PlannerInfo* root, List* distributed_keys, List* targetlist);
extern List* make_groupcl_for_append(PlannerInfo* root, List* targetlist);
extern bool is_broadcast_stream(Stream* stream);
extern bool is_redistribute_stream(Stream* stream);
extern bool is_gather_stream(Stream* stream);
extern bool is_hybid_stream(Stream* stream);
extern void mark_distribute_setop(PlannerInfo* root, Node* node, bool isunionall, bool canDiskeyChange);
extern void CreateGatherPaths(PlannerInfo* root, RelOptInfo* rel, bool is_join);

extern void foreign_qual_context_init(foreign_qual_context* context);
extern void foreign_qual_context_free(foreign_qual_context* context);
extern bool is_foreign_expr(Node* node, foreign_qual_context* context);
extern char get_locator_type(Plan* plan);
extern bool is_compatible_type(Oid type1, Oid type2);
extern bool is_args_type_compatible(OpExpr* op_expr);
extern void materialize_remote_query(Plan* result_plan, bool* materialized, bool sort_to_store);
extern Var* locate_distribute_var(Expr* node);
extern bool add_hashfilter_for_replication(PlannerInfo* root, Plan* plan, List* distribute_keys);
extern bool IsModifyTableForDfsTable(Plan* AppendNode);
extern void confirm_parallel_info(Plan* plan, int dop);
extern bool trivial_subqueryscan(SubqueryScan* plan);
extern Plan* add_broacast_under_local_sort(PlannerInfo* root, PlannerInfo* subroot, Plan* plan);
extern void disable_unshipped_log(Query* query, shipping_context* context);
extern void output_unshipped_log();
extern void stream_walker_context_init(shipping_context *context);

/* Function for smp. */
extern ParallelDesc* create_smpDesc(int consumer_dop, int producer_dop, SmpStreamType smp_type);
extern Plan* create_local_gather(Plan* plan);
extern Plan* create_local_redistribute(PlannerInfo* root, Plan* lefttree, List* redistribute_keys, double multiple);
extern uint2* get_bucketmap_by_execnode(ExecNodes* exec_node, PlannedStmt* plannedstmt, int *bucketCnt);
extern Oid get_oridinary_or_foreign_relid(List* rtable);
extern uint2* GetGlobalStreamBucketMap(PlannedStmt* planned_stmt);
extern int pickup_random_datanode_from_plan(Plan* plan);
extern bool canSeparateComputeAndStorageGroupForDelete(PlannerInfo* root);
extern bool isAllParallelized(List* subplans);
extern List* getSubPlan(Plan* node, List* subplans, List* initplans);
extern char* GetStreamTypeStrOf(StreamPath* path);
extern void GetHashTableCount(Query* parse, List* cteList, int* ccontext);
extern bool IsBucketmapNeeded(PlannedStmt* pstmt);
extern bool remove_local_plan(Plan* stream_plan, Plan* parent, ListCell* lc, bool is_left);

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

#if (!defined(ENABLE_MULTIPLE_NODES)) && (!defined(ENABLE_PRIVATEGAUSS))
typedef bool (*aggSmpFunc)(Oid funcId);
#endif

#endif /* STREAMPLAN_H */
