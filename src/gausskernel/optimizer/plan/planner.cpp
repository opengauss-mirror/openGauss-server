/* -------------------------------------------------------------------------
 *
 * planner.cpp
 * The query optimizer external interface.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/optimizer/plan/planner.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <limits.h>
#include <math.h>

#include "access/transam.h"
#include "catalog/indexing.h"
#include "catalog/pg_cast.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_constraint.h"
#include "catalog/pgxc_group.h"
#include "catalog/pgxc_node.h"
#include "executor/executor.h"
#include "executor/node/nodeAgg.h"
#include "executor/node/nodeRecursiveunion.h"
#include "gaussdb_version.h"
#include "knl/knl_instance.h"
#include "miscadmin.h"
#include "lib/bipartite_match.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/primnodes.h"
#ifdef OPTIMIZER_DEBUG
#include "nodes/print.h"
#endif
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/dynsmp.h"
#include "optimizer/nodegroups.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/plancat.h"
#include "optimizer/planmain.h"
#include "optimizer/planmem_walker.h"
#include "optimizer/planner.h"
#include "optimizer/prep.h"
#include "optimizer/subselect.h"
#include "optimizer/tlist.h"
#include "parser/analyze.h"
#include "optimizer/gtmfree.h"
#include "parser/parsetree.h"
#include "parser/parse_agg.h"
#include "parser/parse_oper.h"
#include "parser/parse_hint.h"
#include "parser/parse_type.h"
#include "rewrite/rewriteManip.h"
#include "securec.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#ifdef PGXC
#include "commands/prepare.h"
#include "pgxc/pgxc.h"
#include "optimizer/pgxcplan.h"
#include "optimizer/streamplan.h"
#include "workload/cpwlm.h"
#include "workload/workload.h"
#endif
#include "optimizer/streamplan.h"
#include "utils/relcache.h"
#include "utils/selfuncs.h"
#include "utils/fmgroids.h"
#include "utils/syscache.h"
#include "utils/snapmgr.h"
#include "vecexecutor/vecfunc.h"
#include "optimizer/randomplan.h"
#include "optimizer/optimizerdebug.h"
#include "optimizer/dataskew.h"
#ifdef ENABLE_MULTIPLE_NODES
#include "tsdb/optimizer/planner.h"
#endif
#include "optimizer/stream_remove.h"
#include "executor/node/nodeModifyTable.h"

#ifndef MIN
#define MIN(A, B) ((B) < (A) ? (B) : (A))
#endif

#ifdef ENABLE_UT
bool estimate_acceleration_cost_for_HDFS(Plan* plan, const char* relname);
#else
static bool estimate_acceleration_cost_for_HDFS(Plan* plan, const char* relname);
#endif

static int g_agglist[] = {AGG_HASHED, AGG_SORTED};

#define TWOLEVELWINFUNSELECTIVITY (1.0 / 3.0)

const char* ESTIMATION_ITEM = "EstimationItem";

/* From experiment, we assume 2.5 times dn number of distinct value can give all dn work to do */
#define DN_MULTIPLIER_FOR_SATURATION 2.5

#define PLAN_HAS_DELTA(plan)                                                                          \
    ((IsA((plan), CStoreScan) && HDFS_STORE == ((CStoreScan*)(plan))->relStoreLocation) \
		||(IsA((plan), CStoreIndexScan) && HDFS_STORE == ((CStoreIndexScan*)(plan)->relStoreLocation) \
		|| IsA((plan), DfsScan) || IsA((plan), DfsIndexScan))

/* For performance reasons, memory context will be dropped only when the totalSpace larger than 1MB. */
#define MEMORY_CONTEXT_DELETE_THRESHOLD (1024 * 1024)
#define IS_NEED_FREE_MEMORY_CONTEXT(MemContext) \
    ((MemContext) != NULL && ((AllocSetContext*)(MemContext))->totalSpace > MEMORY_CONTEXT_DELETE_THRESHOLD)

const static Oid VectorEngineUnsupportType[] = {
    POINTOID,
    LSEGOID,
    BOXOID,
    LINEOID,
    CIRCLEOID,
    POLYGONOID,
    PATHOID,
    HASH32OID
    };

extern PGXCNodeAllHandles* connect_compute_pool(int srvtype);
extern uint64 get_datasize(Plan* plan, int srvtype, int* filenum);

extern RangeTblEntry* make_dummy_remote_rte(char* relname, Alias* alias);
extern ForeignOptions* setForeignOptions(Oid relid);
extern List* reassign_nodelist(RangeTblEntry* rte, List* ori_node_list);

extern Node* preprocess_expression(PlannerInfo* root, Node* expr, int kind);
static Plan* inheritance_planner(PlannerInfo* root);
static Plan* grouping_planner(PlannerInfo* root, double tuple_fraction);
static void preprocess_rowmarks(PlannerInfo* root);
static void estimate_limit_offset_count(PlannerInfo* root, int64* offset_est, int64* count_est);
static double preprocess_limit(PlannerInfo* root, double tuple_fraction, int64* offset_est, int64* count_est);

static bool grouping_is_can_hash(Query* parse, AggClauseCosts* agg_costs);
static Size compute_hash_entry_size(bool vectorized, Path* cheapest_path, int path_width, AggClauseCosts* agg_costs);
static bool choose_hashed_grouping(PlannerInfo* root, double tuple_fraction, double limit_tuples, int path_width,
    Path* cheapest_path, Path* sorted_path, const double* dNumGroups, AggClauseCosts* agg_costs, Size* hash_entry_size);
static void compute_distinct_sorted_path_cost(Path* sorted_p, List* sorted_pathkeys, Query* parse, PlannerInfo* root, 
    int numDistinctCols, Cost sorted_startup_cost, Cost sorted_total_cost, double path_rows, 
    Distribution* sorted_distribution, int path_width, double dNumDistinctRows, double limit_tuples);
static bool choose_hashed_distinct(PlannerInfo* root, double tuple_fraction, double limit_tuples, double path_rows,
    int path_width, Cost cheapest_startup_cost, Cost cheapest_total_cost, Distribution* cheapest_distribution,
    Cost sorted_startup_cost, Cost sorted_total_cost, Distribution* sorted_distribution, List* sorted_pathkeys,
    double dNumDistinctRows, Size hashentrysize);
static List* make_subplanTargetList(PlannerInfo* root, List* tlist, AttrNumber** groupColIdx, bool* need_tlist_eval);
static void locate_grouping_columns(PlannerInfo* root, List* tlist, List* sub_tlist, AttrNumber* groupColIdx);
static List* postprocess_setop_tlist(List* new_tlist, List* orig_tlist);
static List* make_windowInputTargetList(PlannerInfo* root, List* tlist, List* activeWindows);
static void get_column_info_for_window(PlannerInfo* root, WindowClause* wc, List* tlist, int numSortCols,
    AttrNumber* sortColIdx, int* partNumCols, AttrNumber** partColIdx, Oid** partOperators, int* ordNumCols,
    AttrNumber** ordColIdx, Oid** ordOperators);
static List* add_groupingIdExpr_to_tlist(List* tlist);
static List* get_group_expr(List* sortrefList, List* tlist);
static void build_grouping_itst_keys(PlannerInfo* root, List* active_windows);
static Plan* build_grouping_chain(PlannerInfo* root, Query* parse, List** tlist, bool need_sort_for_grouping,
    List* rollup_groupclauses, List* rollup_lists, AttrNumber* groupColIdx, AggClauseCosts* agg_costs, long numGroups,
    Plan* result_plan, WindowLists* wflists, bool need_stream);
static bool group_member(List* list, Expr* node);
static Plan* build_groupingsets_plan(PlannerInfo* root, Query* parse, List** tlist, bool need_sort_for_grouping,
    List* rollup_groupclauses, List* rollup_lists, AttrNumber** groupColIdx, AggClauseCosts* agg_costs, long numGroups,
    Plan* result_plan, WindowLists* wflists, bool* need_hash, List* collectiveGroupExpr);
static bool vector_engine_preprocess_walker(Node* node, void* rtables);
static void init_optimizer_context(PlannerGlobal* glob);
static void deinit_optimizer_context(PlannerGlobal* glob);
static void check_index_column();
static bool check_sort_for_upsert(PlannerInfo* root);

extern void PushDownFullPseudoTargetlist(PlannerInfo *root, Plan *topNode, Plan *botNode,
            List *fullEntryList);

#ifdef PGXC
static void separate_rowmarks(PlannerInfo* root);
#endif

#ifdef STREAMPLAN

typedef struct {
    Node* expr;
    double multiple;
} ExprMultipleData;

typedef enum path_key { windows_func_pathkey = 0, distinct_pathkey, sort_pathkey } path_key;

typedef struct {
    List* queries;
    List* vars;
} ImplicitCastVarContext;
THR_LOCAL List* g_index_vars;

static SAggMethod g_hashagg_option_list[] = {DN_REDISTRIBUTE_AGG, DN_AGG_REDISTRIBUTE_AGG, DN_AGG_CN_AGG};
#define ALL_HASHAGG_OPTION 3
#define HASHAGG_OPTION_WITH_STREAM 2

typedef struct {
    List* rqs;
    bool include_all_plans;
    bool has_modify_table;
    bool under_mergeinto;
    int elevel;
} FindRQContext;

/*
 * This struct is used to find:
 * 1) How many RemoteQuery(VecRemoteQuery) in a plan?
 * 2) Does this plan contain write operations?
 * 3) How many DNs will be involved in?
 *
 * This struct is only used to check if we should allow a query to be executed in GTM-Free mode.
 * In GTM-Free mode:
 *     if 1) the query need to split into multiple queries and 2) the query need to write to the database:
 *         Report error
 *     if the query needs more than one DN to be involved in:
 *         Without multinode hint:
 *             if application_type is:
 *                 not_perfect_sharding_type(Default): allow the query to be executed, no warning/error
 *                 perfect_sharding_type: report ERROR
 *         With multinode hint:
 *             allow the query to continue, report no warnings or errors.
 */
typedef struct {
    int remote_query_count;
    bool has_modify_table;

    /*
     * All nodes involved in the query. It stores IDs(int type) of DNs, such as 1,2,3...
     * The same name exists in ExecNodes.
     */
    List *nodeList;
} FindNodesContext;

typedef struct {
    bool has_redis_stream;
    int broadcast_stream_cnt;
} FindStreamNodesForLoopContext;

static bool needs_two_level_groupagg(PlannerInfo* root, Plan* plan, Node* distinct_node, List* distributed_key,
    bool* need_redistribute, bool* need_local_redistribute);
static Plan* mark_agg_stream(PlannerInfo* root, List* tlist, Plan* plan, List* group_or_distinct_cls,
    AggOrientation agg_orientation, bool* has_second_agg_sort);
static Plan* mark_top_agg(
    PlannerInfo* root, List* tlist, Plan* agg_plan, Plan* sub_plan, AggOrientation agg_orientation);
static Plan* mark_group_stream(PlannerInfo* root, List* tlist, Plan* result_plan);
static Plan* mark_distinct_stream(
    PlannerInfo* root, List* tlist, Plan* plan, List* groupcls, Index query_level, List* current_pathkeys);
static List* get_optimal_distribute_key(PlannerInfo* root, List* groupClause, Plan* plan, double* multiple);
static bool vector_engine_walker_internal(Plan* result_plan, bool check_rescan, VectorPlanContext* planContext);
static bool vector_engine_expression_walker(Node* node, DenseRank_context* context);
static bool vector_engine_walker(Plan* result_plan, bool check_rescan);
static Plan* fallback_plan(Plan* result_plan);
static Plan* vectorize_plan(Plan* result_plan, bool ignore_remotequery, bool forceVectorEngine);
static Plan* build_vector_plan(Plan* plan);
static Plan* mark_windowagg_stream(
    PlannerInfo* root, Plan* plan, List* tlist, WindowClause* wc, List* pathkeys, WindowLists* wflists);
static uint32 get_hashagg_skew(AggSkewInfo* skew_info, List* distribute_keys);
static SAggMethod get_optimal_hashagg(PlannerInfo* root, Plan* lefttree, const AggClauseCosts* aggcosts,
    int numGroupCols, double numGroups, List* distributed_key, List* target_list, double final_groups, double multiple,
    List* distribute_key_less_skew, double multiple_less_skew, AggOrientation agg_orientation, Cost* final_cost,
    Distribution** final_distribution, bool need_stream, AggSkewInfo* skew_info,
    uint32 aggmethod_filter = ALLOW_ALL_AGG);
static Plan* generate_hashagg_plan(PlannerInfo* root, Plan* plan, List* final_list, AggClauseCosts* agg_costs,
    int numGroupCols, const double* numGroups, WindowLists* wflists, AttrNumber* groupColIdx, Oid* groupColOps,
    bool* needs_stream, Size hash_entry_size, AggOrientation agg_orientation, RelOptInfo* rel_info);
static Plan* get_count_distinct_partial_plan(PlannerInfo* root, Plan* result_plan, List** final_tlist,
    Node* distinct_node, AggClauseCosts agg_costs, const double* numGroups, WindowLists* wflists,
    AttrNumber* groupColIdx, bool* needs_stream, Size hash_entry_size, RelOptInfo* rel_info);
static Node* get_multiple_from_expr(
    PlannerInfo* root, Node* expr, double rows, double* skew_multiple, double* bias_multiple);
static void set_root_matching_key(PlannerInfo* root, List* targetlist);
static List* add_groupId_to_groupExpr(List* query_group, List* tlist);

static Path* cost_agg_convert_to_path(Plan* plan);
static StreamPath* cost_agg_do_redistribute(Path* subpath, List* distributed_key, double multiple,
    Distribution* target_distribution, int width, bool vec_output, int dop, bool needs_stream);
static StreamPath* cost_agg_do_gather(Path* subpath, int width, bool vec_output);
static Path* cost_agg_do_agg(Path* subpath, PlannerInfo* root, AggStrategy agg_strategy, const AggClauseCosts* aggcosts,
    int numGroupCols, double numGroups, Size hashentrysize, QualCost total_cost, int width, bool vec_output, int dop);

static void get_hashagg_gather_hashagg_path(PlannerInfo* root, Plan* lefttree, const AggClauseCosts* aggcosts,
    int numGroupCols, double numGroups, double final_groups, QualCost total_cost, Size hashentrysize,
    AggStrategy agg_strategy, bool needs_stream, Path* result_path);
#ifdef ENABLE_MULTIPLE_NODES
static void get_redist_hashagg_gather_hashagg_path(PlannerInfo* root, Plan* lefttree, const AggClauseCosts* aggcosts,
    int numGroupCols, double numGroups, double final_groups, List* distributed_key_less_skew, double multiple_less_skew,
    Distribution* target_distribution, QualCost total_cost, Size hashentrysize, AggStrategy agg_strategy,
    bool needs_stream, Path* result_path);
#endif
static void get_redist_hashagg_path(PlannerInfo* root, Plan* lefttree, const AggClauseCosts* aggcosts, int numGroupCols,
    double numGroups, double final_groups, List* distributed_key, double multiple, Distribution* target_distribution,
    QualCost total_cost, Size hashentrysize, bool needs_stream, Path* result_path);
static void get_hashagg_redist_hashagg_path(PlannerInfo* root, Plan* lefttree, const AggClauseCosts* aggcosts,
    int numGroupCols, double numGroups, double final_groups, List* distributed_key, double multiple,
    Distribution* target_distribution, QualCost total_cost, Size hashentrysize, bool needs_stream, Path* result_path);
static void get_redist_hashagg_redist_hashagg_path(PlannerInfo* root, Plan* lefttree, const AggClauseCosts* aggcosts,
    int numGroupCols, double numGroups, double final_groups, List* distributed_key_less_skew, double multiple_less_skew,
    Distribution* target_distribution, List* distributed_key, double multiple, QualCost total_cost, Size hashentrysize,
    bool needs_stream, Path* result_path);

static void get_count_distinct_param(PlannerInfo* root, Plan** result_plan, List* tlist, Node* distinct_node,
    int* numGrpColsNew, AttrNumber* groupColIdx, AttrNumber** groupColIdx_new, Oid** groupingOps_new, List** orig_tlist,
    List** duplicate_tlist, List** newtlist);
static List* get_count_distinct_newtlist(PlannerInfo* root, List* tlist, Node* distinct_node, List** orig_tlist,
    List** duplicate_tlist, Oid* distinct_eq_op);
static void make_dummy_targetlist(Plan* plan);
static void passdown_itst_keys_to_subroot(PlannerInfo* root, ItstDisKey* diskeys);
static List* add_itst_node_to_list(List* result_list, List* target_list, Expr* node, bool is_matching_key);
static void copy_path_costsize(Path* dest, Path* src);
static bool walk_plan(Plan* plantree, PlannerInfo* root);
static bool walk_normal_plan(Plan* plantree, PlannerInfo* root);
static void walk_set_plan(Plan* plantree, PlannerInfo* root);
static Plan* insert_gather_node(Plan* child, PlannerInfo* root);
static bool has_dfs_node(Plan* plantree, PlannerGlobal* glob);
static Plan* try_accelerate_plan(Plan* plantree, PlannerInfo* root, PlannerGlobal* glob);
static Plan* try_deparse_agg(Plan* plan, PlannerInfo* root, PlannerGlobal* glob);
static bool dfs_node_exists(Plan* plan);
static bool is_dfs_node(Plan* plan);
static void add_metadata(Plan* plan, PlannerInfo* root);
static bool precheck_before_accelerate();
static bool is_pushdown_node(Plan *plan);
static bool estimate_acceleration_cost(Plan *plan);
#ifdef ENABLE_MULTIPLE_NODES
static bool walk_plan_for_coop_analyze(Plan *plan, PlannerInfo *root);
static void walk_set_plan_for_coop_analyze(Plan *plan, PlannerInfo *root);
static bool walk_normal_plan_for_coop_analyze(Plan *plan, PlannerInfo *root);
static bool find_right_agg(Plan *plan);
static bool has_pgfdw_rel(PlannerInfo* root);
extern Plan *deparse_agg_node(Plan *agg, PlannerInfo *root);
#endif
static void find_remotequery(Plan *plan, PlannerInfo *root);
static void gtm_process_top_node(Plan *plan, void *context);
void GetRemoteQuery(PlannedStmt *plan, const char *queryString);
void GetRemoteQueryWalker(Plan* plan, void* context, const char *queryString);
void PlanTreeWalker(Plan* plan, void (*walker)(Plan*, void*, const char*), void*, const char *queryString);
static void find_implicit_cast_var(Query *query);
static bool implicit_cast_var_walker(Node *node, void *context);
static void save_implicit_cast_var(Node *node, void *context);
#endif

void preprocess_const_params(PlannerInfo* root, Node* jtnode);
static Node* preprocess_const_params_worker(PlannerInfo* root, Node* expr, int kind);

/*****************************************************************************
 *
 *	   Query optimizer entry point
 *
 * To support loadable plugins that monitor or modify planner behavior,
 * we provide a hook variable that lets a plugin get control before and
 * after the standard planning process.  The plugin would normally
 * call standard_planner().
 *
 * Note to plugin authors: standard_planner() scribbles on its Query input,
 * so you'd better copy that data structure if you want to plan more than once.
 *
 *****************************************************************************/
PlannedStmt* planner(Query* parse, int cursorOptions, ParamListInfo boundParams)
{
    PlannedStmt* result = NULL;
    instr_time starttime;
    double totaltime = 0;

    INSTR_TIME_SET_CURRENT(starttime);

#ifdef PGXC
    /*
     * streaming engine hook for agg rewrite.
     */
    if (t_thrd.streaming_cxt.streaming_planner_hook)
        (*(planner_hook_type) t_thrd.streaming_cxt.streaming_planner_hook)\
                 (parse, cursorOptions, boundParams);
    /*
     * A Coordinator receiving a query from another Coordinator
     * is not allowed to go into PGXC planner.
     */
    if ((IS_PGXC_COORDINATOR || IS_SINGLE_NODE) && !IsConnFromCoord())
        result = pgxc_planner(parse, cursorOptions, boundParams);
    else
#endif
        result = standard_planner(parse, cursorOptions, boundParams);

    totaltime += elapsed_time(&starttime);
    result->plannertime = totaltime;
    if (u_sess->attr.attr_common.max_datanode_for_plan > 0 && IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        GetRemoteQuery(result, NULL);
    }

    return result;
}

static bool queryIsReadOnly(Query* query)
{
    if (IsA(query, Query)) {
        switch (query->commandType) {
            case CMD_SELECT: {
                /* SELECT FOR [KEY] UPDATE/SHARE */
                if (query->rowMarks != NIL)
                    return false;

                /* data-modifying CTE */
                if (query->hasModifyingCTE)
                    return false;
            }
                return true;
            case CMD_UTILITY:
            case CMD_UPDATE:
            case CMD_INSERT:
            case CMD_DELETE:
            case CMD_MERGE:
                return false;
            default: {
                ereport(ERROR,
                    (errmodule(MOD_OPT_PLANNER), errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                        errmsg("Unrecognized commandType when checking read-only attribute."),
                        errdetail("CommandType: %d", (int)query->commandType),
                        errcause("System error."),
                        erraction("Contact Huawei Engineer.")));
            } break;
        }
    }

    return false;
}

/*
 * @Description: fill bucketmap info into planstmt.
 *
 * @param[IN] result:  plan info
 * @param[IN] node_group_info_context:  bucketmap info
 * @return: void
 */
static void FillPlanBucketmap(PlannedStmt *result,
    NodeGroupInfoContext *nodeGroupInfoContext)
{
#ifdef ENABLE_MULTIPLE_NODES
    /* bucketmap is not needed, just return. */
    if (!IsBucketmapNeeded(result)) {
        result->num_bucketmaps = 0;
        return;
    }
#endif

    result->num_bucketmaps = nodeGroupInfoContext->num_bucketmaps;
    for (int i = 0; i < result->num_bucketmaps; i++) {
        result->bucketMap[i] = nodeGroupInfoContext->bucketMap[i];
        result->bucketCnt[i] = nodeGroupInfoContext->bucketCnt[i];
    }
    pfree_ext(nodeGroupInfoContext);

    if (IS_PGXC_COORDINATOR && result->num_bucketmaps == 0) {
        result->bucketMap[0] = GetGlobalStreamBucketMap(result);
        if (result->bucketMap[0] != NULL) {
            result->num_bucketmaps = 1;
            result->bucketCnt[0] = BUCKETDATALEN;
        }
    }
}

/*
 * @Description: disable tsstore delete sql for pgxc plan.
 *
 * @param[IN] query:  query info
 * @return: void
 */
static void checkTsstoreQuery(Query* query)
{
    if (query->commandType == CMD_DELETE) {
        RangeTblEntry *rte = rt_fetch(query->resultRelation, query->rtable);
        Relation rel = heap_open(rte->relid, AccessShareLock);
        if (RelationIsTsStore(rel)) {
            ereport(ERROR, (errmodule(MOD_EXECUTOR), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("PGXC Plan is not supported for tsdb deleting sql"),
                        errdetail("modify parameters enable_stream_operator or enable_fast_query_shipping"),
                        errcause("not supported"),
                        erraction("modify parameters enable_stream_operator or enable_fast_query_shipping")));
        }
        heap_close(rel, AccessShareLock);
    }
}

PlannedStmt* standard_planner(Query* parse, int cursorOptions, ParamListInfo boundParams)
{
    PlannedStmt* result = NULL;
    PlannerGlobal* glob = NULL;
    double tuple_fraction;
    PlannerInfo* root = NULL;
    Plan* top_plan = NULL;
    ListCell* lp = NULL;
    ListCell* lr = NULL;
    int max_mem = 0;
    int available_mem = 0;
    int esti_op_mem = 0;
    bool use_query_mem = false;
    bool use_tenant = false;
    List* parse_hint_warning = NIL;

    //if it is pgxc plan for tsstore delete sql.errport
    if((!u_sess->attr.attr_sql.enable_stream_operator || !u_sess->opt_cxt.is_stream) && IS_PGXC_COORDINATOR) {
        checkTsstoreQuery(parse);
    }

    /*
     * Dynamic smp
     */
    if (IsDynamicSmpEnabled()) {
        InitDynamicSmp();
        int hashTableCount = 0;

        if (isIntergratedMachine) {
            GetHashTableCount(parse, parse->cteList, &hashTableCount);
        }

        ChooseStartQueryDop(hashTableCount);
    }

    if (enable_check_implicit_cast())
        find_implicit_cast_var(parse);

    /* Initilizing the work mem used by optimizer */
    if (IS_STREAM_PLAN) {
        dywlm_client_get_memory_info(&max_mem, &available_mem, &use_tenant);

        if (max_mem != 0) {
            use_query_mem = true;
            esti_op_mem = (double)available_mem / 2.0;
            u_sess->opt_cxt.op_work_mem = Min(esti_op_mem, OPT_MAX_OP_MEM);
            AssertEreport(u_sess->opt_cxt.op_work_mem > 0,
                MOD_OPT,
                "invalid operator work memory when initilizing the work memory used by optimizer");
        } else {
            u_sess->opt_cxt.op_work_mem = u_sess->attr.attr_memory.work_mem;
            esti_op_mem = u_sess->opt_cxt.op_work_mem;
        }
    } else {
        u_sess->opt_cxt.op_work_mem = u_sess->attr.attr_memory.work_mem;
        esti_op_mem = u_sess->opt_cxt.op_work_mem;
    }

    /* Cursor options may come from caller or from DECLARE CURSOR stmt */
    if (parse->utilityStmt && IsA(parse->utilityStmt, DeclareCursorStmt))
        cursorOptions = (uint32)cursorOptions | (uint32)(((DeclareCursorStmt*)parse->utilityStmt)->options);

    /*
     * Set up global state for this planner invocation.  This data is needed
     * across all levels of sub-Query that might exist in the given command,
     * so we keep it in a separate struct that's linked to by each per-Query
     * PlannerInfo.
     */
    glob = makeNode(PlannerGlobal);

    glob->boundParams = boundParams;
    glob->subplans = NIL;
    glob->subroots = NIL;
    glob->rewindPlanIDs = NULL;
    glob->finalrtable = NIL;
    glob->finalrowmarks = NIL;
    glob->resultRelations = NIL;
    glob->relationOids = NIL;
    glob->invalItems = NIL;
    glob->nParamExec = 0;
    glob->lastPHId = 0;
    glob->lastRowMarkId = 0;
    glob->transientPlan = false;
    glob->dependsOnRole = false;
    glob->insideRecursion = false;
    glob->bloomfilter.bloomfilter_index = -1;
    glob->bloomfilter.add_index = true;
    glob->estiopmem = esti_op_mem;
    
    if (IS_STREAM_PLAN)
        glob->vectorized = !vector_engine_preprocess_walker((Node*)parse, parse->rtable);
    else
        glob->vectorized = false;
    /* Assume work mem is at least 1/4 of query mem */
    glob->minopmem = Min(available_mem / 4, OPT_MAX_OP_MEM);
    parse_hint_warning = retrieve_query_hint_warning((Node*)parse);

    /*
     * Set up default exec_nodes, we fist build re-cursively iterate parse->rtable
     * to check if we are refering base relations from different node group(error-out),
     * then fetch 1st RTE entry to build default ExecNodes and put the reference to
     * the top-most PlannerInfo->glob
     */
    bool ngbk_is_multiple_nodegroup_scenario = false;
    int ngbk_different_nodegroup_count = 1;
    Distribution* ngbk_in_redistribution_group_distribution = NULL;
    Distribution* ngbk_compute_permission_group_distribution = NULL;
    Distribution* ngbk_query_union_set_group_distribution = NULL;
    Distribution* ngbk_single_node_distribution = NULL;
    ng_backup_nodegroup_options(&ngbk_is_multiple_nodegroup_scenario,
        &ngbk_different_nodegroup_count,
        &ngbk_in_redistribution_group_distribution,
        &ngbk_compute_permission_group_distribution,
        &ngbk_query_union_set_group_distribution,
        &ngbk_single_node_distribution);
    ng_init_nodegroup_optimizer(parse);

    /* Must assign value after call ng_init_nodegroup_optimizer(). */
    u_sess->opt_cxt.is_dngather_support = 
        u_sess->opt_cxt.is_dngather_support && ng_get_single_node_distribution() != NULL;

    /* Determine what fraction of the plan is likely to be scanned */
    if ((uint32)cursorOptions & CURSOR_OPT_FAST_PLAN) {
        /*
         * We have no real idea how many tuples the user will ultimately FETCH
         * from a cursor, but it is often the case that he doesn't want 'em
         * all, or would prefer a fast-start plan anyway so that he can
         * process some of the tuples sooner.  Use a GUC parameter to decide
         * what fraction to optimize for.
         */
        tuple_fraction = u_sess->attr.attr_sql.cursor_tuple_fraction;

        /*
         * We document cursor_tuple_fraction as simply being a fraction, which
         * means the edge cases 0 and 1 have to be treated specially here.	We
         * convert 1 to 0 ("all the tuples") and 0 to a very small fraction.
         */
        if (tuple_fraction >= 1.0) {
            tuple_fraction = 0.0;
        } else if (tuple_fraction <= 0.0) {
            tuple_fraction = 1e-10;
        }
    } else {
        /* Default assumption is we need all the tuples */
        tuple_fraction = 0.0;
    }

    /* reset u_sess->analyze_cxt.need_autoanalyze */
    u_sess->analyze_cxt.need_autoanalyze = false;

    MemoryContext old_context = CurrentMemoryContext;
    init_optimizer_context(glob);
    old_context = MemoryContextSwitchTo(glob->plannerContext->plannerMemContext);

    /* primary planning entry point (may recurse for subqueries) */
    top_plan = subquery_planner(glob, parse, NULL, false, tuple_fraction, &root);

    MemoryContextSwitchTo(old_context);

    /* Are there OBS/HDFS ForeignScan node(s) in the plan tree? */
    u_sess->opt_cxt.srvtype = T_INVALID;
    u_sess->opt_cxt.has_obsrel = has_dfs_node(top_plan, glob);

    /*
     * try to accelerate the query for HDFS/OBS foreign table by pushing
     * scan/agg node down to the compute pool.
     */
    if (u_sess->opt_cxt.has_obsrel) {
        AssertEreport(u_sess->opt_cxt.srvtype != T_INVALID,
            MOD_OPT,
            "invalid server type when push scan/agg node down to the compute pool to the accelerate the query.");

        top_plan = try_accelerate_plan(top_plan, root, glob);
    }

    /*
     * If creating a plan for a scrollable cursor, make sure it can run
     * backwards on demand.  Add a Material node at the top at need.
     */
    if ((unsigned int)cursorOptions & CURSOR_OPT_SCROLL) {
        if (!ExecSupportsBackwardScan(top_plan))
            top_plan = materialize_finished_plan(top_plan);
    }

    /* final cleanup of the plan */
    AssertEreport(glob->finalrtable == NIL,
        MOD_OPT,
        "finalrtable is not empty when finish creating a plan for a scrollable cursor");
    AssertEreport(glob->finalrowmarks == NIL,
        MOD_OPT,
        "finalrowmarks is not empty when finish creating a plan for a scrollable cursor");
    AssertEreport(glob->resultRelations == NIL,
        MOD_OPT,
        "resultRelations is not empty when finish creating a plan for a scrollable cursor");

    if ((IS_STREAM_PLAN || (IS_PGXC_DATANODE && (!IS_STREAM || IS_STREAM_DATANODE))) && root->query_level == 1) {
        /* remote query and windowagg do not support vectorize rescan, so fallback to row plan */
        top_plan = try_vectorize_plan(top_plan, parse, cursorOptions & CURSOR_OPT_HOLD);
    }

    top_plan = set_plan_references(root, top_plan);
    delete_redundant_streams_of_remotequery((RemoteQuery *)top_plan);

    /*
     * just for cooperation analysis on client cluster,
     * try deparse agg node to remote sql in ForeignScan node.
     * NOTE: call try_deparse_agg() must be after set_plan_references().
     */
    top_plan = try_deparse_agg(top_plan, root, glob);

    /*
     * just for cooperation analysis on source data cluster,
     * reassign dn list scaned of RemoteQuery node for the request from client cluster.
     */
    find_remotequery(top_plan, root);

    if (IS_PGXC_COORDINATOR && root->query_level == 1) {
        bool materialize = false;
        bool sort_to_store = false;
        /*
         * if is with hold cursor, remotequery tuplestore should be used,
         * and because we do not rescan sortstore in execRemoteQueryResacn,
         * tuples in sortstore should be stored into tuplestore, to avoid missing tuples.
         */
        if (cursorOptions & CURSOR_OPT_HOLD) {
            materialize = true;
            sort_to_store = true;
        }
        materialize_remote_query(top_plan, &materialize, sort_to_store);
    }

    /*
     * Handle subplan situation.
     * We have to put this under set_plan_references() function,
     * otherwise we will mis-identify the subplan.
     */
    if (u_sess->opt_cxt.query_dop > 1) {
        List* subplan_list = NIL;
        (void)has_subplan(top_plan, NULL, NULL, true, &subplan_list, true);
    }
    confirm_parallel_info(top_plan, 1);

#ifdef STREAMPLAN
    /*
     * Mark plan node id and parent node id for all the plan nodes.
     */
    int parent_node_id = INITIAL_PARENT_NODE_ID; /* beginning with INITIAL_PARENT_NODE_ID */
    int plan_node_id = INITIAL_PLAN_NODE_ID;     /* beginning with INITIAL_PLAN_NODE_ID */
    int num_streams = 0;
    int num_plannodes = 0;
    int total_num_streams = 0;

    /* mark gather count for query */
    int gather_count = 0;
    /*
     * When enable_stream_operator = off;
     * The current mechanism has such a problem that a CN will split complex SQL into multiple simple SQL and send it to
     * the DN for execution. In order to ensure data consistency, the DN needs to use the same Snapshot for visibility
     * judgment of such SQL. Therefore, the CN side sends such SQL identifier to the DN. Use
     * PlannedStmt->Max_push_sql_num records the maximum number of SQL statements split by a SQL statement.
     */
    int max_push_sql_num = 0;
    /*
     * the array to store parent plan node id of each subplan, start from 1.
     * First item is for parent id when traverse the tree.
     */
    int* subplan_ids = (int*)palloc0(sizeof(int) * (list_length(glob->subplans) + 1));
    List* init_plan = NIL;
    int i = 1;

    NodeGroupInfoContext* node_group_info_context = (NodeGroupInfoContext*)palloc0(sizeof(NodeGroupInfoContext));

    /*
     * MPP with-recursive support
     *
     * Vectorize the each plan nodes under RecursiveUnion
     */
    Assert(list_length(glob->subplans) == list_length(glob->subroots));
    forboth(lp, glob->subplans, lr, glob->subroots)
    {
        Plan* subplan = (Plan*)lfirst(lp);
        PlannerInfo* subroot = (PlannerInfo*)lfirst(lr);

        /* Vectorize the subplan with RecursiveUnion plan node */
        if (STREAM_RECURSIVECTE_SUPPORTED && IsA(subplan, RecursiveUnion)) {
            subplan = try_vectorize_plan(subplan, subroot->parse, true);
            lfirst(lp) = subplan;
        }
    }

    /* Assign plan node id for each plan node */
#ifdef ENABLE_MULTIPLE_NODES
    if (IS_PGXC_COORDINATOR && root->query_level == 1) {
#else
    if (root->query_level == 1) {
#endif
        finalize_node_id(top_plan,
            &plan_node_id,
            &parent_node_id,
            &num_streams,
            &num_plannodes,
            &total_num_streams,
            &max_push_sql_num,
            &gather_count,
            glob->subplans,
            glob->subroots,
            &init_plan,
            subplan_ids,
            true,
            false,
            false,
            queryIsReadOnly(parse),
            node_group_info_context);
    }
#endif

    /* ... and the subplans (both regular subplans and initplans) */
    AssertEreport(list_length(glob->subplans) == list_length(glob->subroots),
        MOD_OPT,
        "The length of subplans is not equal to that of subroots when standardize planner");
    forboth(lp, glob->subplans, lr, glob->subroots)
    {
        Plan* subplan = (Plan*)lfirst(lp);
        PlannerInfo* subroot = (PlannerInfo*)lfirst(lr);

#ifdef STREAMPLAN
        /* We set reference of some plans in finalize_node_id. For undone plan, set plan references */
        if (subplan_ids[i] == 0 || IsA(subplan, RecursiveUnion) ||
            IsA(subplan, StartWithOp)) {
            if (STREAM_RECURSIVECTE_SUPPORTED && IsA(subplan, RecursiveUnion)) {
                RecursiveRefContext context;
                errno_t rc = EOK;
                rc = memset_s(&context, sizeof(RecursiveRefContext), 0, sizeof(RecursiveRefContext));
                securec_check(rc, "\0", "\0");
                context.join_type = T_Invalid;
                context.ru_plan = (RecursiveUnion*)subplan;
                context.control_plan = subplan;
                context.nested_stream_depth = 0;
                context.set_control_plan_nodeid = false;
                context.initplans = init_plan;
                context.subplans = glob->subplans;

                /* EntryPoint for iterating the underlying plan node */
                set_recursive_cteplan_ref(subplan, &context);
            } else {
                subplan = try_vectorize_plan(subplan, subroot->parse, true);
                lfirst(lp) = set_plan_references(subroot, subplan);
            }

            /* for start with processing */
            if (IsA(subplan, StartWithOp)) {
                ProcessStartWithOpMixWork(root, top_plan, subroot, (StartWithOp *)subplan);
            }

            /*
             * When enable_stream_operator = off, Subquery SQL  is not processed by finalize_node_id.
             * In this case we default each subquery to a SQL statement pushed down to the DN.
             * Here we may misjudge the subquery executed only on the CN,
             * but in order to maintain The independence of the set_plan_references function,
             * there is no further judgment on such subqueries, and it is considered that the sub-query is issued to the
             * DN. This operation does not affect the correctness.
             */
            max_push_sql_num++;
        }
#endif
        i++;
    }

    /* Juse copy these fields only when the memory context total size meets the dropping condition. */
    if (IS_NEED_FREE_MEMORY_CONTEXT(glob->plannerContext->plannerMemContext)) {
        top_plan = (Plan*)copyObject(top_plan);
        glob->finalrtable = (List*)copyObject(glob->finalrtable);
        glob->resultRelations = (List*)copyObject(glob->resultRelations);
        glob->subplans = (List*)copyObject(glob->subplans);
        glob->rewindPlanIDs = bms_copy(glob->rewindPlanIDs);
        glob->finalrowmarks = (List*)copyObject(glob->finalrowmarks);
        glob->relationOids = (List*)copyObject(glob->relationOids);
        glob->invalItems = (List*)copyObject(glob->invalItems);
        init_plan = (List*)copyObject(init_plan);
    }

    glob->hint_warning = list_concat(parse_hint_warning, (List*)copyObject(glob->hint_warning));

    /* build the PlannedStmt result */
    result = makeNode(PlannedStmt);

    result->commandType = parse->commandType;
    result->queryId = parse->queryId;
    result->uniqueSQLId = parse->uniqueSQLId;
    result->hasReturning = (parse->returningList != NIL);
    result->hasModifyingCTE = parse->hasModifyingCTE;
    result->canSetTag = parse->canSetTag;
    result->transientPlan = glob->transientPlan;
    result->dependsOnRole = glob->dependsOnRole;
    result->planTree = top_plan;
    result->rtable = glob->finalrtable;
    result->resultRelations = glob->resultRelations;
    result->utilityStmt = parse->utilityStmt;
    result->subplans = glob->subplans;
    result->rewindPlanIDs = glob->rewindPlanIDs;
    result->rowMarks = glob->finalrowmarks;
    result->relationOids = glob->relationOids;
    result->invalItems = glob->invalItems;
    result->nParamExec = glob->nParamExec;
    result->noanalyze_rellist = (List*)copyObject(t_thrd.postgres_cxt.g_NoAnalyzeRelNameList);

    if (IS_PGXC_COORDINATOR &&
        (t_thrd.proc->workingVersionNum < 92097 || total_num_streams > 0)) {
        result->nodesDefinition = get_all_datanodes_def();
    }
    result->num_nodes = u_sess->pgxc_cxt.NumDataNodes;
    result->num_streams = total_num_streams;
    result->max_push_sql_num = max_push_sql_num;
    result->gather_count = gather_count;
    result->num_plannodes = num_plannodes;

    FillPlanBucketmap(result, node_group_info_context);

    result->query_string = NULL;
    result->MaxBloomFilterNum = root->glob->bloomfilter.bloomfilter_index + 1;
    /* record which suplan belongs to which thread */
#ifdef ENABLE_MULTIPLE_NODES
    if (IS_STREAM_PLAN) {
#else
    if (result->num_streams > 0) {
#endif
        for (i = 1; i <= list_length(result->subplans); i++)
            result->subplan_ids = lappend_int(result->subplan_ids, subplan_ids[i]);
        result->initPlan = init_plan;
    }
    pfree_ext(subplan_ids);

    /* dynamic query dop main entry */
    if (IsDynamicSmpEnabled()) {
        /* the main plan */
        OptimizePlanDop(result);
    }

    /* Query mem calculation and control main entry */
    if (IS_STREAM_PLAN && use_query_mem) {
        result->assigned_query_mem[1] = max_mem;
        result->assigned_query_mem[0] = available_mem;

        ereport(DEBUG2,
            (errmodule(MOD_MEM),
                errmsg("[standard_planner]Passing in max mem %d and available mem %d", max_mem, available_mem)));
        CalculateQueryMemMain(result, use_tenant, false);
        ereport(DEBUG2,
            (errmodule(MOD_MEM),
                errmsg("[standard_planner]Calucated query max %d and min mem %d",
                    result->query_mem[0],
                    result->query_mem[1])));
    }

    /* data redistribution for DFS table. */
    if (u_sess->attr.attr_sql.enable_cluster_resize && root->query_level == 1 &&
        root->parse->commandType == CMD_INSERT) {
        result->dataDestRelIndex = root->dataDestRelIndex;
    } else {
        result->dataDestRelIndex = 0;
    }

    result->query_dop = u_sess->opt_cxt.query_dop;

    if (u_sess->opt_cxt.has_obsrel) {
        result->has_obsrel = true;
    }
    result->plan_hint_warning = glob->hint_warning;

    ng_restore_nodegroup_options(ngbk_is_multiple_nodegroup_scenario,
        ngbk_different_nodegroup_count,
        ngbk_in_redistribution_group_distribution,
        ngbk_compute_permission_group_distribution,
        ngbk_query_union_set_group_distribution,
        ngbk_single_node_distribution);

    deinit_optimizer_context(glob);

    if (enable_check_implicit_cast() && g_index_vars != NIL)
        check_index_column();

    result->isRowTriggerShippable = parse->isRowTriggerShippable;
    return result;
}

/*
 * We will not rewrite full joins if the query tree contain these members now.
 */
bool fulljoin_2_left_union_right_anti_support(Query* parse)
{
    if (parse->commandType != CMD_SELECT && parse->commandType != CMD_INSERT && parse->commandType != CMD_MERGE)
        return false;
    if (parse->utilityStmt != NULL)
        return false;
    if (parse->hasRecursive)
        return false;
    if (parse->hasModifyingCTE)
        return false;
    if (parse->hasForUpdate)
        return false;
    if (parse->returningList != NIL)
        return false;
    if (parse->rowMarks != NIL)
        return false;
    if (parse->has_to_save_cmd_id)
        return false;
    if (parse->equalVars != NIL)
        return false;
    return true;
}

/*
 * return true if the funcexpr is a implicit conversion.$
 */
static bool IsImplicitConversion(FuncExpr* expr)
{
    if (list_length(expr->args) != 1 || expr->funcformat != COERCE_IMPLICIT_CAST) {
        return false;
    }

    Oid srctype = exprType((Node*)linitial(expr->args));
    Oid targettype = expr->funcresulttype;

    HeapTuple tuple = SearchSysCache2(CASTSOURCETARGET, ObjectIdGetDatum(srctype), ObjectIdGetDatum(targettype));

    if (HeapTupleIsValid(tuple)) {
        Form_pg_cast castForm = (Form_pg_cast)GETSTRUCT(tuple);

        if (castForm->castfunc == expr->funcid && castForm->castcontext == COERCION_CODE_IMPLICIT) {
            ReleaseSysCache(tuple);
            return true;
        }

        ReleaseSysCache(tuple);
    }

    return false;
}

/*
 * preprocessOperator
 *     Recursively scan the query and do subquery_planner's
 *     preprocessing work on each opexpr node, regenerate
 *     these nodes when the string_digit_to_numeric is on.
 */
bool PreprocessOperator(Node* node, void* context)
{
    if (node == NULL) {
        return false;
    }

    if (IsA(node, Query)) {
        return query_tree_walker((Query*)node, (bool (*)())PreprocessOperator, (void*)context, 0);
    } else if (IsA(node, OpExpr)) {
        OpExpr* expr = (OpExpr*)node;

        /* Only regenerate the operator when opresulttype is bool. */
        if (list_length(expr->args) == 2 && expr->opresulttype == BOOLOID && expr->inputcollid == 0) {
            Node* ltree = (Node*)list_nth(expr->args, 0);
            Node* rtree = (Node*)list_nth(expr->args, 1);

            /* Determine if the left and right subtrees are implicit type conversion */
            if (IsA(ltree, FuncExpr) && IsImplicitConversion((FuncExpr*)ltree) &&
                ((FuncExpr*)ltree)->funcresulttype != NUMERICOID) {
                ltree = (Node*)linitial(((FuncExpr*)ltree)->args);
            }

            if (IsA(rtree, FuncExpr) && IsImplicitConversion((FuncExpr*)rtree) &&
                ((FuncExpr*)rtree)->funcresulttype != NUMERICOID) {
                rtree = (Node*)linitial(((FuncExpr*)rtree)->args);
            }

            Oid ltypeId = exprType(ltree);
            Oid rtypeId = exprType(rtree);

            if ((IsIntType(ltypeId) && IsCharType(rtypeId)) || (IsIntType(rtypeId) && IsCharType(ltypeId))) {
                HeapTuple tp = SearchSysCache1(OPEROID, ObjectIdGetDatum(expr->opno));

                if (HeapTupleIsValid(tp)) {
                    Form_pg_operator optup = (Form_pg_operator)GETSTRUCT(tp);
                    List* name = list_make1(makeString(NameStr(optup->oprname)));

                    /* Regenerate the opexpr node. */
                    OpExpr* newNode = (OpExpr*)make_op(NULL, name, ltree, rtree, expr->location, true);

                    Node* lexpr = (Node*)list_nth(newNode->args, 0);
                    Node* rexpr = (Node*)list_nth(newNode->args, 1);
                    ltypeId = exprType(lexpr);
                    rtypeId = exprType(rexpr);

                    if (newNode->opresulttype == BOOLOID && ltypeId == NUMERICOID && rtypeId == NUMERICOID) {

                        /* Determine if the new subtrees are implicit type conversion */
                        if (IsA(lexpr, FuncExpr) && IsImplicitConversion((FuncExpr*)lexpr)) {
                            exprSetInputCollation((Node*)list_nth(newNode->args, 0), exprCollation(ltree));
                        }

                        if (IsA(rexpr, FuncExpr) && IsImplicitConversion((FuncExpr*)rexpr)) {
                            exprSetInputCollation((Node*)list_nth(newNode->args, 1), exprCollation(rtree));
                        }

                        errno_t errorno = EOK;
                        errorno = memcpy_s(node, sizeof(OpExpr), (Node*)newNode, sizeof(OpExpr));
                        securec_check_c(errorno, "\0", "\0");
                    }

                    pfree_ext(newNode);
                    list_free_ext(name);
                    ReleaseSysCache(tp);
                }
            }
        }
    }

    return expression_tree_walker(node, (bool (*)())PreprocessOperator, (void*)context);
}

/**
 * Check whether the current nodegroup state support recursive cte.  
 * This must be called after calling ng_init_nodegroup_optimizer and
 * is_dngather_support is assigned.
 */
void check_is_support_recursive_cte(PlannerInfo* root) 
{
    if (!IS_STREAM_PLAN || !root->is_under_recursive_cte) {
        return;
    }

    errno_t sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason, 
        NOTPLANSHIPPING_LENGTH, "With-Recursive under multi-nodegroup scenario is not shippable");
    int different_nodegroup_count = ng_get_different_nodegroup_count();

    /* 1. Installation nodegroup, compute nodegroup, single nodegroup. */
    if (different_nodegroup_count > 2) {
       securec_check_ss_c(sprintf_rc, "\0", "\0");
       mark_stream_unsupport();
       return;
    }   

    /* 2. Installation nodegroup, compute nodegroup. */
    if (different_nodegroup_count == 2 && ng_get_single_node_distribution() == NULL) {
       securec_check_ss_c(sprintf_rc, "\0", "\0");
       mark_stream_unsupport();
       return;
    }

    /* 3. Installation nodegroup, single nodegroup which is used */
    if (different_nodegroup_count == 2 && u_sess->opt_cxt.is_dngather_support == true) {
       securec_check_ss_c(sprintf_rc, "\0", "\0");
       mark_stream_unsupport();
       return;
    }

    /* 4. Installation nodegroup, single nodegroup but not used. */
    /*    Installation nodegroup. */
    return;
}

/*
 * Process set hint at top level. DO NOT handle subquery.
 * apply_set_hint and recover_set_hint should wrap around pg_plan_query
 * Returns the guc level.
 */
int apply_set_hint(const Query* parse)
{
    HintState* hintstate = parse->hintState;
    if (hintstate == NULL) {
        return -1;
    }
    int gucNestLevel = 0;
    int ret;
    ListCell* lc = NULL;
    gucNestLevel = NewGUCNestLevel();
    foreach (lc, hintstate->set_hint) {
        SetHint* hint = (SetHint*)lfirst(lc);
        if (unlikely(strcmp(hint->name, "node_name") == 0)) {
            u_sess->attr.attr_common.node_name = hint->value;
        } else {
            ret = set_config_option(hint->name,
                                    hint->value,
                                    PGC_USERSET,     /* for now set hint only support */
                                    PGC_S_SESSION,   /* session-level userset guc     */
                                    GUC_ACTION_SAVE, /* need to rollback later        */
                                    true,
                                    WARNING,
                                    false);
            hint->base.state = (ret == 1) ? (HINT_STATE_USED) : (HINT_STATE_ERROR);
        }
    }
    return gucNestLevel;
}

void recover_set_hint(int savedNestLevel)
{
    if (savedNestLevel < 0) {
        return;
    }
    AtEOXact_GUC(true, savedNestLevel);
    u_sess->attr.attr_common.node_name = "";
}

/* --------------------
 * subquery_planner
 *	  Invokes the planner on a subquery.  We recurse to here for each
 *	  sub-SELECT found in the query tree.
 *
 * glob is the global state for the current planner run.
 * parse is the querytree produced by the parser & rewriter.
 * parent_root is the immediate parent Query's info (NULL at the top level).
 * hasRecursion is true if this is a recursive WITH query.
 * tuple_fraction is the fraction of tuples we expect will be retrieved.
 * tuple_fraction is interpreted as explained for grouping_planner, below.
 *
 * If subroot isn't NULL, we pass back the query's final PlannerInfo struct;
 * among other things this tells the output sort ordering of the plan.
 *
 * Basically, this routine does the stuff that should only be done once
 * per Query object.  It then calls grouping_planner.  At one time,
 * grouping_planner could be invoked recursively on the same Query object;
 * that's not currently true, but we keep the separation between the two
 * routines anyway, in case we need it again someday.
 *
 * subquery_planner will be called recursively to handle sub-Query nodes
 * found within the query's expressions and rangetable.
 *
 * Returns a query plan.
 * --------------------
 */
Plan* subquery_planner(PlannerGlobal* glob, Query* parse, PlannerInfo* parent_root, bool hasRecursion,
    double tuple_fraction, PlannerInfo** subroot, int options, ItstDisKey* diskeys, List* subqueryRestrictInfo)
{
    int num_old_subplans = list_length(glob->subplans);
    PlannerInfo* root = NULL;
    Plan* plan = NULL;
    List* newHaving = NIL;
    bool hasOuterJoins = false;
    bool hasResultRTEs = false;
    ListCell* l = NULL;
    StringInfoData buf;
    char RewriteContextName[NAMEDATALEN] = {0};
    MemoryContext QueryRewriteContext = NULL;
    MemoryContext oldcontext = NULL;
    errno_t rc = EOK;

/* We used DEBUG5 log to print SQL after each rewrite */
#define DEBUG_QRW(message)                                                                      \
    do {                                                                                        \
        if (log_min_messages <= DEBUG5) {                                                       \
            initStringInfo(&buf);                                                               \
            deparse_query(root->parse, &buf, NIL, false, false, NULL, true);                    \
            ereport(DEBUG5, (errmodule(MOD_OPT_REWRITE), errmsg("%s: %s", message, buf.data))); \
            pfree_ext(buf.data);                                                                \
        }                                                                                       \
    } while (0)

    /* Create a PlannerInfo data structure for this subquery */
    root = makeNode(PlannerInfo);
    root->parse = parse;
    root->glob = glob;
    root->query_level = parent_root ? parent_root->query_level + 1 : 1;
    root->parent_root = parent_root;
    root->plan_params = NIL;
    root->planner_cxt = CurrentMemoryContext;
    root->init_plans = NIL;
    root->cte_plan_ids = NIL;
    root->eq_classes = NIL;
    root->append_rel_list = NIL;
    root->rowMarks = NIL;
    root->hasInheritedTarget = false;
    root->grouping_map = NULL;
    root->subquery_type = options;
    root->param_upper = NULL;
	root->hasRownumQual = false;

    /*
     * Apply memory context for query rewrite in optimizer.
     * OptimizerContext is NULL in PBE condition which we need to consider.
     */
    rc = snprintf_s(RewriteContextName, NAMEDATALEN, NAMEDATALEN - 1, "QueryRewriteContext_%d", root->query_level);
    securec_check_ss(rc, "\0", "\0");

    QueryRewriteContext = AllocSetContextCreate(CurrentMemoryContext,
        RewriteContextName,
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);

    oldcontext = MemoryContextSwitchTo(QueryRewriteContext);

    /*
     * Mark the current PlannerInfo is working for a query-block in recursive CTE planning,
     * in general we want to let the sub-planning stages to know we are under a recursive-cte,
     * planning, two case need
     * [1]. Call subquery_planner() to planning the query block inside of with-block
     * [2]. Call subquery_planner() to planning each query-block consists of the union
     *      operation, so consequtially inherit the "is_recursive_cte" properties from
     *      parent root
     */
    if (hasRecursion || (parent_root && parent_root->is_under_recursive_cte)) {
        root->is_under_recursive_cte = true;
        root->is_under_recursive_tree = parent_root->is_under_recursive_tree;
    } else {
        root->is_under_recursive_cte = false;
    }

    check_is_support_recursive_cte(root);

#ifdef PGXC
    root->rs_alias_index = 1;
#endif
    root->hasRecursion = hasRecursion;
    if (hasRecursion)
        root->wt_param_id = SS_assign_special_param(root);
    else
        root->wt_param_id = -1;
    root->qualSecurityLevel = 0;
    root->non_recursive_plan = NULL;
    root->subqueryRestrictInfo = subqueryRestrictInfo;

    /* Mark current planner root is correlated as well */
    if (parent_root != NULL && parent_root->is_under_recursive_cte && parent_root->is_correlated) {
        root->is_correlated = true;
    }

    DEBUG_QRW("Before rewrite");

    preprocess_const_params(root, (Node*)parse->jointree);

    DEBUG_QRW("After const params replace ");

    /*
     * If there is a WITH list, process each WITH query and build an initplan
     * SubPlan structure for it. For stream plan, it's already be replaced, so
     * no need to do this.
     *
     * For recursive cte we still process this in same way
     */
    if (parse->cteList) {
        SS_process_ctes(root);
    }

    DEBUG_QRW("After CTE substitution");

    /*
     * If the FROM clause is empty, replace it with a dummy RTE_RESULT RTE, so
     * that we don't need so many special cases to deal with that situation.
     */
    replace_empty_jointree(parse);

#ifdef STREAMPLAN
    /*
     * Since count(distinct) conversion can push down subquery, for sake of
     * duplicate of sublink pullup, we put it ahead of sublink pullup
     */
    if (IS_STREAM_PLAN && parse->hasAggs) {
        convert_multi_count_distinct(root);
        DEBUG_QRW("After multi count distinct rewrite");
    }
#endif

    /*
     * Look for ANY and EXISTS SubLinks in WHERE and JOIN/ON clauses, and try
     * to transform them into joins.  Note that this step does not descend
     * into subqueries; if we pull up any subqueries below, their SubLinks are
     * processed just before pulling them up.
     */
    if (parse->hasSubLinks) {
        pull_up_sublinks(root);
        DEBUG_QRW("After sublink pullup");
    }

    /* Reduce orderby clause in subquery for join */
    reduce_orderby(parse, false);

    DEBUG_QRW("After order by reduce");

    if (u_sess->attr.attr_sql.enable_constraint_optimization) {
        removeNotNullTest(root);
        DEBUG_QRW("After soft constraint removal");
    }

    /*
     * Scan the rangetable for set-returning functions, and inline them if
     * possible (producing subqueries that might get pulled up next).
     * Recursion issues here are handled in the same way as for SubLinks.
     */
    inline_set_returning_functions(root);

    if ((LAZY_AGG & u_sess->attr.attr_sql.rewrite_rule) && permit_from_rewrite_hint(root, LAZY_AGG)) {
        lazyagg_main(parse);
        DEBUG_QRW("After lazyagg");
    }

    /*
     * Here we only control the select permission for pan_table_data. Details see in checkPTRelkind().
     * The flag will be used in ExecCheckRTEPerms.
     */
    if (parse->commandType == CMD_SELECT && checkSelectStmtForPlanTable(parse->rtable)) {
        OnlySelectFromPlanTable = true;
    }

#ifndef ENABLE_MULTIPLE_NODES
    /* Change ROWNUM to LIMIT if possible */
    preprocess_rownum(root, parse);
    DEBUG_QRW("After preprocess rownum");
#endif

    /*
     * Check to see if any subqueries in the jointree can be merged into this
     * query.
     */
    parse->jointree = (FromExpr*)pull_up_subqueries(root, (Node*)parse->jointree);

    DEBUG_QRW("After simple subquery pull up");

    /*
     * If this is a simple UNION ALL query, flatten it into an appendrel. We
     * do this now because it requires applying pull_up_subqueries to the leaf
     * queries of the UNION ALL, which weren't touched above because they
     * weren't referenced by the jointree (they will be after we do this).
     */
    if (parse->setOperations) {
        flatten_simple_union_all(root);
        DEBUG_QRW("After simple union all flatten");
    }

    /* Transform hint.*/
    transform_hints(root, parse, parse->hintState);

    DEBUG_QRW("After transform hint");

    /*
     * Detect whether any rangetable entries are RTE_JOIN kind; if not, we can
     * avoid the expense of doing flatten_join_alias_vars().  Likewise check
     * whether any are RTE_RESULT kind; if not, we can skip
     * remove_useless_result_rtes().  Also check for outer joins --- if none,
     * we can skip reduce_outer_joins().  And check for LATERAL RTEs, too.
     * This must be done after we have done pull_up_subqueries(), of course.
     */
    root->hasJoinRTEs = false;
    root->hasLateralRTEs = false;
    hasOuterJoins = false;

    foreach (l, parse->rtable) {
        RangeTblEntry* rte = (RangeTblEntry*)lfirst(l);

        if (rte->swAborted) {
            continue;
        }

        if (rte->rtekind == RTE_JOIN) {
            root->hasJoinRTEs = true;
            if (IS_OUTER_JOIN(rte->jointype)) {
                hasOuterJoins = true;
            }
        } else if (rte->rtekind == RTE_RESULT) {
            hasResultRTEs = true;
        }
        if (rte->lateral)
            root->hasLateralRTEs = true;
    }

    /*
     * Preprocess RowMark information.	We need to do this after subquery
     * pullup (so that all non-inherited RTEs are present) and before
     * inheritance expansion (so that the info is available for
     * expand_inherited_tables to examine and modify).
     */
    preprocess_rowmarks(root);

#ifdef PGXC
    /*
     * In Coordinators we separate row marks in two groups
     * one comprises of row marks of types ROW_MARK_EXCLUSIVE & ROW_MARK_SHARE
     * and the other contains the rest of the types of row marks
     * The former is handeled on Coordinator in such a way that
     * FOR UPDATE/SHARE gets added in the remote query, whereas
     * the later needs to be handeled the way pg does
     *
     * Notice : This is not a very efficient way of handling row marks
     * Consider this join query
     * select * from t1, t2 where t1.val = t2.val for update
     * It results in this query to be fired at the Datanodes
     * SELECT val, val2, ctid FROM ONLY t2 WHERE true FOR UPDATE OF t2
     * We are locking the complete table where as we should have locked
     * only the rows where t1.val = t2.val is met
     *
     * We won't really call separate_rowmarks before we support for update with
     * reomtequery.
     */
    if (!IS_STREAM_PLAN)
        separate_rowmarks(root);
#endif

    /*
     * When the SQL dose not support stream mode in coordinator node, must send remotequery
     * to datanode, and need not expand dfs table into dfs main table and delta table.
     * Always support dfs table to expanding in data node.
     */
    if (u_sess->opt_cxt.is_stream || IS_PGXC_DATANODE) {
        /*
         * Expand the Dfs table.
         */
        expand_dfs_tables(root);
    }

    /*
     * Expand any rangetable entries that are inheritance sets into "append
     * relations".  This can add entries to the rangetable, but they must be
     * plain RTE_RELATION entries, so it's OK (and marginally more efficient)
     * to do it after checking for joins and other special RTEs.  We must do
     * this after pulling up subqueries, else we'd fail to handle inherited
     * tables in subqueries.
     */
    expand_inherited_tables(root);

    /*
     * Set hasHavingQual to remember if HAVING clause is present.  Needed
     * because preprocess_expression will reduce a constant-true condition to
     * an empty qual list ... but "HAVING TRUE" is not a semantic no-op.
     */
    root->hasHavingQual = (parse->havingQual != NULL);

    /* Clear this flag; might get set in distribute_qual_to_rels */
    root->hasPseudoConstantQuals = false;

    /*
     * Calculate how many tables in current query level, and give a
     * rought estimation of work mem for each relation
     */
    int work_mem_orig = u_sess->opt_cxt.op_work_mem;
    int esti_op_mem_orig = root->glob->estiopmem;
    if (root->glob->minopmem > 0) {
        int num_rel = 0;
        foreach (l, parse->rtable) {
            RangeTblEntry* rte = (RangeTblEntry*)lfirst(l);

            if (rte->rtekind == RTE_RELATION || rte->rtekind == RTE_SUBQUERY) {
                num_rel++;
            }
        }
        if (num_rel <= 1) {
            if ((parse->groupClause || parse->sortClause || parse->distinctClause))
                num_rel = 2;
            else
                num_rel = 1;
        }
        root->glob->estiopmem = Max(root->glob->minopmem, (double)root->glob->estiopmem / ceil(LOG2(num_rel + 1)));
        u_sess->opt_cxt.op_work_mem = Min(root->glob->estiopmem, OPT_MAX_OP_MEM);
        AssertEreport(u_sess->opt_cxt.op_work_mem > 0,
            MOD_OPT,
            "invalid operator work mem when roughtly estimating the work memory for each relation");
    }

    /*
     * Do expression preprocessing on targetlist and quals, as well as other
     * random expressions in the querytree.  Note that we do not need to
     * handle sort/group expressions explicitly, because they are actually
     * part of the targetlist.
     */
    parse->targetList = (List*)preprocess_expression(root, (Node*)parse->targetList, EXPRKIND_TARGET);

    parse->returningList = (List*)preprocess_expression(root, (Node*)parse->returningList, EXPRKIND_TARGET);

    preprocess_qual_conditions(root, (Node*)parse->jointree);

    parse->havingQual = preprocess_expression(root, parse->havingQual, EXPRKIND_QUAL);

    foreach (l, parse->windowClause) {
        WindowClause* wc = (WindowClause*)lfirst(l);

        /* partitionClause/orderClause are sort/group expressions */
        wc->startOffset = preprocess_expression(root, wc->startOffset, EXPRKIND_LIMIT);
        wc->endOffset = preprocess_expression(root, wc->endOffset, EXPRKIND_LIMIT);
    }

    parse->limitOffset = preprocess_expression(root, parse->limitOffset, EXPRKIND_LIMIT);
    if (parse->limitCount != NULL && !IsA(parse->limitCount, Const)) {
        parse->limitCount = preprocess_expression(root, parse->limitCount, EXPRKIND_LIMIT);
    }

    foreach (l, parse->mergeActionList) {
        MergeAction* action = (MergeAction*)lfirst(l);

        action->targetList = (List*)preprocess_expression(root, (Node*)action->targetList, EXPRKIND_TARGET);

        action->pulluped_targetList =
            (List*)preprocess_expression(root, (Node*)(action->pulluped_targetList), EXPRKIND_TARGET);

        action->qual = preprocess_expression(root, (Node*)action->qual, EXPRKIND_QUAL);
    }

    parse->mergeSourceTargetList =
        (List*)preprocess_expression(root, (Node*)parse->mergeSourceTargetList, EXPRKIND_TARGET);

    if (parse->upsertClause) {
        parse->upsertClause->updateTlist = (List*)
            preprocess_expression(root, (Node*)parse->upsertClause->updateTlist, EXPRKIND_TARGET);
        parse->upsertClause->upsertWhere = (Node*)
            preprocess_expression(root, (Node*)parse->upsertClause->upsertWhere, EXPRKIND_QUAL);
    }
    root->append_rel_list = (List*)preprocess_expression(root, (Node*)root->append_rel_list, EXPRKIND_APPINFO);

    /* Also need to preprocess expressions for function and values RTEs */
    foreach (l, parse->rtable) {
        RangeTblEntry* rte = (RangeTblEntry*)lfirst(l);
        int kind;

        if (rte->rtekind == RTE_RELATION) {
            if (rte->tablesample) {
                rte->tablesample =
                    (TableSampleClause*)preprocess_expression(root, (Node*)rte->tablesample, EXPRKIND_TABLESAMPLE);
            }
            if (rte->timecapsule) {
#ifndef ENABLE_MULTIPLE_NODES
                if (IS_STREAM) {
                    mark_stream_unsupport();
                }
#endif
                rte->timecapsule =
                    (TimeCapsuleClause*)preprocess_expression(root, (Node*)rte->timecapsule, EXPRKIND_TIMECAPSULE);
            }
        } else if (rte->rtekind == RTE_SUBQUERY) {
            /*
             * We don't want to do all preprocessing yet on the subquery's
             * expressions, since that will happen when we plan it.  But if it
             * contains any join aliases of our level, those have to get
             * expanded now, because planning of the subquery won't do it.
             * That's only possible if the subquery is LATERAL.
             */
            if (rte->lateral && root->hasJoinRTEs)
                rte->subquery = (Query *)flatten_join_alias_vars(root, (Node *) rte->subquery);
        } else if (rte->rtekind == RTE_FUNCTION) {
            /* Preprocess the function expression fully */
            kind = rte->lateral ? EXPRKIND_RTFUNC_LATERAL : EXPRKIND_RTFUNC;
            rte->funcexpr = preprocess_expression(root, rte->funcexpr, kind);
        } else if (rte->rtekind == RTE_VALUES) {
            /* Preprocess the values lists fully */
            kind = rte->lateral ? EXPRKIND_VALUES_LATERAL : EXPRKIND_VALUES;
            rte->values_lists = (List*)preprocess_expression(root, (Node*)rte->values_lists, kind);
        }

        /*
         * Process each element of the securityQuals list as if it were a
         * separate qual expression (as indeed it is).  We need to do it this
         * way to get proper canonicalization of AND/OR structure.  Note that
         * this converts each element into an implicit-AND sublist.
         */
        ListCell* cell = NULL;
        foreach (cell, rte->securityQuals) {
            lfirst(cell) = preprocess_expression(root, (Node*)lfirst(cell), EXPRKIND_QUAL);
        }
    }

    DEBUG_QRW("After preprocess expressions");

    u_sess->opt_cxt.op_work_mem = work_mem_orig;
    root->glob->estiopmem = esti_op_mem_orig;

    /*
     * In some cases we may want to transfer a HAVING clause into WHERE. We
     * cannot do so if the HAVING clause contains aggregates (obviously) or
     * volatile functions (since a HAVING clause is supposed to be executed
     * only once per group).  Also, it may be that the clause is so expensive
     * to execute that we're better off doing it only once per group, despite
     * the loss of selectivity.  This is hard to estimate short of doing the
     * entire planning process twice, so we use a heuristic: clauses
     * containing subplans are left in HAVING.	Otherwise, we move or copy the
     * HAVING clause into WHERE, in hopes of eliminating tuples before
     * aggregation instead of after.
     *
     * If the query has explicit grouping then we can simply move such a
     * clause into WHERE; any group that fails the clause will not be in the
     * output because none of its tuples will reach the grouping or
     * aggregation stage.  Otherwise we must have a degenerate (variable-free)
     * HAVING clause, which we put in WHERE so that query_planner() can use it
     * in a gating Result node, but also keep in HAVING to ensure that we
     * don't emit a bogus aggregated row. (This could be done better, but it
     * seems not worth optimizing.)
     *
     * Note that both havingQual and parse->jointree->quals are in
     * implicitly-ANDed-list form at this point, even though they are declared
     * as Node *.
     */
    if (!parse->unique_check)  {
        newHaving = NIL;
        foreach(l, (List *) parse->havingQual)  {
            Node       *havingclause = (Node *)lfirst(l);

            /* 
             * For groupingSets, having clause can only be calculate in havingQual, can not push-down to lefttree's qual.
             * 
             * For example:
             * select sum(a), b from group by rollup(a, b) having b > 10;
             * this mean: group by a, b
             *            group by a
             *            group by ()
             *
             * For "group by ()", we need calculate sum(a) for all lefttree's rows.
             */
            if (contain_agg_clause(havingclause) ||
                contain_volatile_functions(havingclause) ||
                contain_subplans(havingclause)
                || parse->groupingSets) {
                /* keep it in HAVING */
                newHaving = lappend(newHaving, havingclause);
            } else if (parse->groupClause)  {
                /* move it to WHERE */
                parse->jointree->quals = (Node *)
                    lappend((List *) parse->jointree->quals, havingclause);
            } else {
                /* put a copy in WHERE, keep it in HAVING */
                parse->jointree->quals = (Node *)
                    lappend((List *) parse->jointree->quals,
                            copyObject(havingclause));
                newHaving = lappend(newHaving, havingclause);
            }
        }
        parse->havingQual = (Node *) newHaving;
    }


    DEBUG_QRW("After having qual rewrite");

    passdown_itst_keys_to_subroot(root, diskeys);

    /*
     * If we have any outer joins, try to reduce them to plain inner joins.
     * This step is most easily done after we've done expression
     * preprocessing.
     */
    if (hasOuterJoins) {
        reduce_outer_joins(root);
        DEBUG_QRW("After outer-to-inner conversion");
        if (IS_STREAM_PLAN) {
            bool support_rewrite = true;
            if (!fulljoin_2_left_union_right_anti_support(root->parse))
                support_rewrite = false;
            if (contain_volatile_functions((Node*)root->parse))
                support_rewrite = false;
            contain_func_context context =
                init_contain_func_context(list_make3_oid(ECEXTENSIONFUNCOID, ECHADOOPFUNCOID, RANDOMFUNCOID));
            if (contains_specified_func((Node*)root->parse, &context)) {
                char* func_name = get_func_name(((FuncExpr*)linitial(context.func_exprs))->funcid);
                ereport(DEBUG2,
                    (errmodule(MOD_OPT_REWRITE),
                        (errmsg("[Not rewrite full Join on true]: %s functions contained.", func_name))));
                pfree_ext(func_name);
                list_free_ext(context.funcids);
                context.funcids = NIL;
                list_free_ext(context.func_exprs);
                context.func_exprs = NIL;
                support_rewrite = false;
            }
            if (support_rewrite) {
                reduce_inequality_fulljoins(root);
                DEBUG_QRW("After full join conversion");
            }
        }
    }

    /*
     * If we have any RTE_RESULT relations, see if they can be deleted from
     * the jointree.  This step is most effectively done after we've done
     * expression preprocessing and outer join reduction.
     */
    if (hasResultRTEs)
        remove_useless_result_rtes(root);

    /*
     * Check if need auto-analyze for current query level.
     * No need to do auto-analyze for query on one table without Groupby.
     */
    if (u_sess->attr.attr_sql.enable_autoanalyze && !u_sess->analyze_cxt.need_autoanalyze && IS_STREAM_PLAN &&
        (list_length(parse->rtable) > 1 || parse->groupClause)) {
        /* inherit upper level and check for current query level */
        u_sess->analyze_cxt.need_autoanalyze = true;
    }
    (void)MemoryContextSwitchTo(oldcontext);

    /*
     * Do the main planning.  If we have an inherited target relation, that
     * needs special processing, else go straight to grouping_planner.
     */
    if (parse->resultRelation && parse->commandType != CMD_INSERT &&
        rt_fetch(parse->resultRelation, parse->rtable)->inh)
        plan = inheritance_planner(root);
    else {
        plan = grouping_planner(root, tuple_fraction);
        /* If it's not SELECT, we need a ModifyTable node */
        if (parse->commandType != CMD_SELECT) {
            List* returningLists = NIL;
            List* rowMarks = NIL;
            Relation mainRel = NULL;
            Oid taleOid = rt_fetch(parse->resultRelation, parse->rtable)->relid;
            bool partKeyUpdated = targetListHasPartitionKey(parse->targetList, taleOid);
            mainRel = RelationIdGetRelation(taleOid);
            bool isDfsStore = RelationIsDfsStore(mainRel);
            RelationClose(mainRel);

            /*
             * Set up the RETURNING list-of-lists, if needed.
             */
            if (parse->returningList)
                returningLists = list_make1(parse->returningList);
            else
                returningLists = NIL;

            /*
             * If there was a FOR [KEY] UPDATE/SHARE clause, the LockRows node will
             * have dealt with fetching non-locked marked rows, else we need
             * to have ModifyTable do that.
             */
            if (parse->rowMarks)
                rowMarks = NIL;
            else
                rowMarks = root->rowMarks;
#ifdef STREAMPLAN
            plan = (Plan*)make_modifytable(root,
                parse->commandType,
                parse->canSetTag,
                list_make1_int(parse->resultRelation),
                list_make1(plan),
                returningLists,
                rowMarks,
                SS_assign_special_param(root),
                partKeyUpdated,
                parse->mergeTarget_relation,
                parse->mergeSourceTargetList,
                parse->mergeActionList,
                parse->upsertClause,
                isDfsStore);
#else
            plan = (Plan*)make_modifytable(parse->commandType,
                parse->canSetTag,
                list_make1_int(parse->resultRelation),
                list_make1(plan),
                returningLists,
                rowMarks,
                SS_assign_special_param(root),
                partKeyUpdated,
                parse->mergeTarget_relation,
                parse->mergeSourceTargetList,
                parse->mergeActionList,
                parse->upsertClause,
                isDfsStore);
#endif
#ifdef PGXC
            plan = pgxc_make_modifytable(root, plan);
#endif
        }
    }

    /*
     * If any subplans were generated, or if there are any parameters to worry
     * about, build initPlan list and extParam/allParam sets for plan nodes,
     * and attach the initPlans to the top plan node.
     */
    if (plan == NULL)
        ereport(ERROR,
            (errmodule(MOD_OPT_PLANNER), errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("Fail to generate subquery plan."),
                errdetail("N/A"),
                errcause("System error."),
                erraction("Contact Huawei Engineer.")));

    if (list_length(glob->subplans) != num_old_subplans || root->glob->nParamExec > 0)
        SS_finalize_plan(root, plan, true);

    /* Return internal info if caller wants it */
    if (subroot != NULL)
        *subroot = root;

    /* add not-used hints information to warning string */
    if (parse->hintState)
        desc_hint_in_state(root, parse->hintState);

    /* Fix var's if we have changed var */
    if (root->var_mappings != NIL) {
        fix_vars_plannode(root, plan);
        root->parse->is_from_inlist2join_rewrite = true;
    }

    return plan;
}

/*
 * preprocess_expression
 *		Do subquery_planner's preprocessing work for an expression,
 *		which can be a targetlist, a WHERE clause (including JOIN/ON
 *		conditions), or a HAVING clause.
 */
Node* preprocess_expression(PlannerInfo* root, Node* expr, int kind)
{
    /*
     * Fall out quickly if expression is empty.  This occurs often enough to
     * be worth checking.  Note that null->null is the correct conversion for
     * implicit-AND result format, too.
     */
    if (expr == NULL)
        return NULL;

    /*
     * If the query has any join RTEs, replace join alias variables with
     * base-relation variables. We must do this before sublink processing,
     * else sublinks expanded out from join aliases wouldn't get processed. We
     * can skip it in VALUES lists, however, since they can't contain any Vars
     * at all.
     */
    if (root->hasJoinRTEs && !(kind == EXPRKIND_RTFUNC || kind == EXPRKIND_VALUES))
        expr = flatten_join_alias_vars(root, expr);

    /*
     * Simplify constant expressions.
     *
     * Note: an essential effect of this is to convert named-argument function
     * calls to positional notation and insert the current actual values of
     * any default arguments for functions.  To ensure that happens, we *must*
     * process all expressions here.  Previous PG versions sometimes skipped
     * const-simplification if it didn't seem worth the trouble, but we can't
     * do that anymore.
     *
     * Note: this also flattens nested AND and OR expressions into N-argument
     * form.  All processing of a qual expression after this point must be
     * careful to maintain AND/OR flatness --- that is, do not generate a tree
     * with AND directly under AND, nor OR directly under OR.
     */
    expr = eval_const_expressions(root, expr);

    /*
     * If it's a qual or havingQual, canonicalize it.
     */
    if (kind == EXPRKIND_QUAL) {
        expr = (Node*)canonicalize_qual((Expr*)expr, false);

#ifdef OPTIMIZER_DEBUG
        printf("After canonicalize_qual()\n");
        pprint(expr);
#endif
    }

    /* Expand SubLinks to SubPlans */
    if (root->parse->hasSubLinks)
        expr = SS_process_sublinks(root, expr, (kind == EXPRKIND_QUAL));

    /*
     * XXX do not insert anything here unless you have grokked the comments in
     * SS_replace_correlation_vars ...
     * 
     * Replace uplevel vars with Param nodes (this IS possible in VALUES)
     */
    if (root->query_level > 1)
        expr = SS_replace_correlation_vars(root, expr);

    /*
     * If it's a qual or havingQual, convert it to implicit-AND format. (We
     * don't want to do this before eval_const_expressions, since the latter
     * would be unable to simplify a top-level AND correctly. Also,
     * SS_process_sublinks expects explicit-AND format.)
     */
    if (kind == EXPRKIND_QUAL)
        expr = (Node*)make_ands_implicit((Expr*)expr);

    return expr;
}

/*
 * preprocess_qual_conditions
 *		Recursively scan the query's jointree and do subquery_planner's
 *		preprocessing work on each qual condition found therein.
 */
void preprocess_qual_conditions(PlannerInfo* root, Node* jtnode)
{
    if (jtnode == NULL)
        return;
    if (IsA(jtnode, RangeTblRef)) {
        /* nothing to do here */
    } else if (IsA(jtnode, FromExpr)) {
        FromExpr* f = (FromExpr*)jtnode;
        ListCell* l = NULL;

        foreach (l, f->fromlist)
            preprocess_qual_conditions(root, (Node*)lfirst(l));

        f->quals = preprocess_expression(root, f->quals, EXPRKIND_QUAL);
    } else if (IsA(jtnode, JoinExpr)) {
        JoinExpr* j = (JoinExpr*)jtnode;

        preprocess_qual_conditions(root, j->larg);
        preprocess_qual_conditions(root, j->rarg);

        j->quals = preprocess_expression(root, j->quals, EXPRKIND_QUAL);
    } else {
        ereport(ERROR,
            (errmodule(MOD_OPT_PLANNER), errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("Unrecognized node type when processing qual condition."),
                errdetail("Qual condition: %d", (int)nodeTag(jtnode)),
                errcause("System error."),
                erraction("Contact Huawei Engineer.")));
    }
}

/*
 * preprocess_phv_expression
 *   Do preprocessing on a PlaceHolderVar expression that's been pulled up.
 *
 * If a LATERAL subquery references an output of another subquery, and that
 * output must be wrapped in a PlaceHolderVar because of an intermediate outer
 * join, then we'll push the PlaceHolderVar expression down into the subquery
 * and later pull it back up during find_lateral_references, which runs after
 * subquery_planner has preprocessed all the expressions that were in the
 * current query level to start with.  So we need to preprocess it then.
 */
Expr *
preprocess_phv_expression(PlannerInfo *root, Expr *expr)
{
   return (Expr *) preprocess_expression(root, (Node *) expr, EXPRKIND_PHV);
}

/*
 * preprocess_const_params
 *		Recursively scan the query's jointree and do subquery_planner's
 *		preprocessing work on each qual condition found therein to replace
 *		params with const value if possible
 */
void preprocess_const_params(PlannerInfo* root, Node* jtnode)
{
    if (jtnode == NULL)
        return;
    if (IsA(jtnode, RangeTblRef)) {
        /* nothing to do here */
    } else if (IsA(jtnode, FromExpr)) {
        FromExpr* f = (FromExpr*)jtnode;
        ListCell* l = NULL;

        foreach (l, f->fromlist)
            preprocess_const_params(root, (Node*)lfirst(l));

        f->quals = preprocess_const_params_worker(root, f->quals, EXPRKIND_QUAL);
    } else if (IsA(jtnode, JoinExpr)) {
        JoinExpr* j = (JoinExpr*)jtnode;

        preprocess_const_params(root, j->larg);
        preprocess_const_params(root, j->rarg);

        j->quals = preprocess_const_params_worker(root, j->quals, EXPRKIND_QUAL);
    } else {
        ereport(ERROR,
            (errmodule(MOD_OPT_PLANNER), errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("Unrecognized node type when processing const parameters."),
                errdetail("Qual condition: %d", (int)nodeTag(jtnode)),
                errcause("System error."),
                erraction("Contact Huawei Engineer.")));
    }
}

/*
 * preprocess_const_params_worker
 *		worker func for params to const replacement
 *		so much like preprocess_expression() but only do eval_const_expressions
 */
static Node* preprocess_const_params_worker(PlannerInfo* root, Node* expr, int kind)
{
    /*
     * Fall out quickly if expression is empty.  This occurs often enough to
     * be worth checking.  Note that null->null is the correct conversion for
     * implicit-AND result format, too.
     */
    if (expr == NULL)
        return NULL;

    /*
     * Simplify constant expressions.
     *
     * Note: an essential effect of this is to convert named-argument function
     * calls to positional notation and insert the current actual values of
     * any default arguments for functions.  To ensure that happens, we *must*
     * process all expressions here.  Previous PG versions sometimes skipped
     * const-simplification if it didn't seem worth the trouble, but we can't
     * do that anymore.
     *
     * Note: this also flattens nested AND and OR expressions into N-argument
     * form.  All processing of a qual expression after this point must be
     * careful to maintain AND/OR flatness --- that is, do not generate a tree
     * with AND directly under AND, nor OR directly under OR.
     */
    expr = eval_const_expressions(root, expr);

    return expr;
}

/*
 * inheritance_planner
 *	  Generate a plan in the case where the result relation is an
 *	  inheritance set.
 *
 * We have to handle this case differently from cases where a source relation
 * is an inheritance set. Source inheritance is expanded at the bottom of the
 * plan tree (see allpaths.c), but target inheritance has to be expanded at
 * the top.  The reason is that for UPDATE, each target relation needs a
 * different targetlist matching its own column set.  Fortunately,
 * the UPDATE/DELETE target can never be the nullable side of an outer join,
 * so it's OK to generate the plan this way.
 *
 * Returns a query plan.
 */
static Plan* inheritance_planner(PlannerInfo* root)
{
    Query* parse = root->parse;
    int parentRTindex = parse->resultRelation;
    List* final_rtable = NIL;
    int save_rel_array_size = 0;
    RelOptInfo** save_rel_array = NULL;
    RangeTblEntry** save_rte_array = NULL;
    List* subplans = NIL;
    List* resultRelations = NIL;
    List* returningLists = NIL;
    List* rowMarks = NIL;
    ListCell* lc = NULL;
    bool isDfsStore = false;
    bool partKeyUpdated = false;
    Oid taleOid = rt_fetch(parse->resultRelation, parse->rtable)->relid;
    Relation mainRel = NULL;
    partKeyUpdated = targetListHasPartitionKey(parse->targetList, taleOid);

    mainRel = RelationIdGetRelation(taleOid);
    AssertEreport(RelationIsValid(mainRel),
        MOD_OPT,
        "The relation descriptor is invalid"
        "when generating a query plan and the result relation is an inherient plan.");

    isDfsStore = RelationIsDfsStore(mainRel);
    RelationClose(mainRel);

    /*
     * We generate a modified instance of the original Query for each target
     * relation, plan that, and put all the plans into a list that will be
     * controlled by a single ModifyTable node.  All the instances share the
     * same rangetable, but each instance must have its own set of subquery
     * RTEs within the finished rangetable because (1) they are likely to get
     * scribbled on during planning, and (2) it's not inconceivable that
     * subqueries could get planned differently in different cases.  We need
     * not create duplicate copies of other RTE kinds, in particular not the
     * target relations, because they don't have either of those issues.  Not
     * having to duplicate the target relations is important because doing so
     * (1) would result in a rangetable of length O(N^2) for N targets, with
     * at least O(N^3) work expended here; and (2) would greatly complicate
     * management of the rowMarks list.
     */
    foreach (lc, root->append_rel_list) {
        AppendRelInfo* appinfo = (AppendRelInfo*)lfirst(lc);
        PlannerInfo subroot;
        Plan* subplan = NULL;
        Index rti;

        /* append_rel_list contains all append rels; ignore others */
        if (appinfo->parent_relid != (uint)parentRTindex)
            continue;

        /*
         * We need a working copy of the PlannerInfo so that we can control
         * propagation of information back to the main copy.
         */
        errno_t rc = memcpy_s(&subroot, sizeof(PlannerInfo), root, sizeof(PlannerInfo));
        securec_check(rc, "\0", "\0");

        /*
         * Generate modified query with this rel as target.  We first apply
         * adjust_appendrel_attrs, which copies the Query and changes
         * references to the parent RTE to refer to the current child RTE,
         * then fool around with subquery RTEs.
         */
        subroot.parse = (Query*)adjust_appendrel_attrs(root, (Node*)parse, appinfo);

        /*
         * The rowMarks list might contain references to subquery RTEs, so
         * make a copy that we can apply ChangeVarNodes to.  (Fortunately, the
         * executor doesn't need to see the modified copies --- we can just
         * pass it the original rowMarks list.)
         */
        subroot.rowMarks = (List*)copyObject(root->rowMarks);

        /*
         * Add placeholders to the child Query's rangetable list to fill the
         * RT indexes already reserved for subqueries in previous children.
         * These won't be referenced, so there's no need to make them very
         * valid-looking.
         */
        while (list_length(subroot.parse->rtable) < list_length(final_rtable))
            subroot.parse->rtable = lappend(subroot.parse->rtable, makeNode(RangeTblEntry));

        /*
         * If this isn't the first child Query, generate duplicates of all
         * subquery RTEs, and adjust Var numbering to reference the
         * duplicates. To simplify the loop logic, we scan the original rtable
         * not the copy just made by adjust_appendrel_attrs; that should be OK
         * since subquery RTEs couldn't contain any references to the target
         * rel.
         */
        if (final_rtable != NIL) {
            ListCell* lr = NULL;

            rti = 1;
            foreach (lr, parse->rtable) {
                RangeTblEntry* rte = (RangeTblEntry*)lfirst(lr);

                if (rte->rtekind == RTE_SUBQUERY) {
                    Index newrti;

                    /*
                     * The RTE can't contain any references to its own RT
                     * index, so we can save a few cycles by applying
                     * ChangeVarNodes before we append the RTE to the
                     * rangetable.
                     */
                    newrti = list_length(subroot.parse->rtable) + 1;
                    ChangeVarNodes((Node*)subroot.parse, rti, newrti, 0);
                    ChangeVarNodes((Node*)subroot.rowMarks, rti, newrti, 0);
                    rte = (RangeTblEntry*)copyObject(rte);
                    subroot.parse->rtable = lappend(subroot.parse->rtable, rte);
                }
                rti++;
            }
        }

        /* We needn't modify the child's append_rel_list */
        /* There shouldn't be any OJ info to translate, as yet */
        AssertEreport(subroot.join_info_list == NIL,
            MOD_OPT,
            "the list of SpecialJoinInfos is not NIL when generating a modified instance of the original Query for "
            "each target relation.");
        Assert(subroot.lateral_info_list == NIL);
        /* and we haven't created PlaceHolderInfos, either */
        AssertEreport(subroot.placeholder_list == NIL,
            MOD_OPT,
            "the list of PlaceHolderInfos is not NIL when generating a modified instance of the original Query for "
            "each target relation.");
        /* hack to mark target relation as an inheritance partition */
        subroot.hasInheritedTarget = true;

        /* Generate plan */
        subplan = grouping_planner(&subroot, 0.0 /* retrieve all tuples */);
        AssertEreport(subplan != NULL, MOD_OPT, "subplan is NULL when generating a plan.");

        /*
         * If this child rel was excluded by constraint exclusion, exclude it
         * from the result plan.
         */
        if (is_dummy_plan(subplan))
            continue;

        subplans = lappend(subplans, subplan);

        /*
         * If this is the first non-excluded child, its post-planning rtable
         * becomes the initial contents of final_rtable; otherwise, append
         * just its modified subquery RTEs to final_rtable.
         */
        if (final_rtable == NIL)
            final_rtable = subroot.parse->rtable;
        else
            final_rtable = list_concat(final_rtable, list_copy_tail(subroot.parse->rtable, list_length(final_rtable)));

        /*
         * We need to collect all the RelOptInfos from all child plans into
         * the main PlannerInfo, since setrefs.c will need them.  We use the
         * last child's simple_rel_array (previous ones are too short), so we
         * have to propagate forward the RelOptInfos that were already built
         * in previous children.
         */
        AssertEreport(subroot.simple_rel_array_size >= save_rel_array_size,
            MOD_OPT,
            "the allocated size of array is smaller when collecting all the RelOptInfos.");
        for (rti = 1; rti < (unsigned int)save_rel_array_size; rti++) {
            RelOptInfo* brel = save_rel_array[rti];
            RangeTblEntry* rte = save_rte_array[rti];

            if (brel != NULL) {
                subroot.simple_rel_array[rti] = brel;
            }
            if (rte != NULL) {
                subroot.simple_rte_array[rti] = rte;
            }
        }
        save_rel_array_size = subroot.simple_rel_array_size;
        save_rel_array = subroot.simple_rel_array;
        save_rte_array = subroot.simple_rte_array;

        /* Make sure any initplans from this rel get into the outer list */
        root->init_plans = subroot.init_plans;

        /* Build list of target-relation RT indexes */
        resultRelations = lappend_int(resultRelations, appinfo->child_relid);

        /* Build list of per-relation RETURNING targetlists */
        if (parse->returningList)
            returningLists = lappend(returningLists, subroot.parse->returningList);

        AssertEreport(parse->mergeActionList == NIL,
            MOD_OPT,
            "list of actions for MERGE is not empty when generating a plan in the case where the result relation is an "
            "inheritance set");
    }

    /* Mark result as unordered (probably unnecessary) */
    root->query_pathkeys = NIL;

    /*
     * If we managed to exclude every child rel, return a dummy plan; it
     * doesn't even need a ModifyTable node.
     */
    if (subplans == NIL) {
        /* although dummy, it must have a valid tlist for executor */
        List* tlist = NIL;

        tlist = preprocess_targetlist(root, parse->targetList);
        return (Plan*)make_result(root, tlist, (Node*)list_make1(makeBoolConst(false, false)), NULL);
    }

    /*
     * Put back the final adjusted rtable into the master copy of the Query.
     */
    parse->rtable = final_rtable;
    root->simple_rel_array_size = save_rel_array_size;
    root->simple_rel_array = save_rel_array;
    root->simple_rte_array = save_rte_array;
    /*
     * If there was a FOR [KEY] UPDATE/SHARE clause, the LockRows node will have
     * dealt with fetching non-locked marked rows, else we need to have
     * ModifyTable do that.
     */
    if (parse->rowMarks)
        rowMarks = NIL;
    else
        rowMarks = root->rowMarks;

        /* And last, tack on a ModifyTable node to do the UPDATE/DELETE work */
#ifdef STREAMPLAN
    return make_modifytables(root,
        parse->commandType,
        parse->canSetTag,
        resultRelations,
        subplans,
        returningLists,
        rowMarks,
        SS_assign_special_param(root),
        partKeyUpdated,
        isDfsStore,
        0,
        NULL,
        NULL,
        NULL);
#else
    return make_modifytables(parse->commandType,
        parse->canSetTag,
        resultRelations,
        subplans,
        returningLists,
        rowMarks,
        SS_assign_special_param(root),
        partKeyUpdated,
        isDfsStore,
        0,
        NULL,
        NULL,
        NULL);
#endif
}

/*
 * @Description: set SortGroupClause's groupSet which will be set to true if it appears in group by clause
 * and do not appear in collectiveGroupExpr that means it's value will be altered grouping set after,
 * and delete this expr from EquivalenceMember.
 *
 * @in root: Per-query information for planning/optimization
 * @in sort_group_clauses: Sort group clause, include sort, group by, partition by, distinct, etc.
 * @in tlist: target lists.
 * @in collectiveGroupExpr: collective group expr that appear in all group by clause for OLAP function.
 *
 */
static void set_groupset_for_sortgroup_items(
    PlannerInfo* root, List* sort_group_clauses, List* tlist, List* collectiveGroupExpr)
{
    List* groupClause = root->parse->groupClause;
    List* groupExpr = get_sortgrouplist_exprs(groupClause, tlist);
    SortGroupClause* sortcl = NULL;
    Expr* sortExpr = NULL;

    /*
     * If this expr in partake group but not include in all group clause, set ec_group_set to true.
     * It mean we will again make an pathkey to distinct, sort or windows function.
     */
    ListCell* cell = NULL;

    foreach (cell, sort_group_clauses) {
        sortcl = (SortGroupClause*)lfirst(cell);
        sortExpr = (Expr*)get_sortgroupclause_expr(sortcl, tlist);

        /*
         * This expr is in group expr and is not collectiveGroupExpr, set groupSet to false.
         * group expr is used to do comparison since sortgroupclause can be different in some property
         */
        if (list_member(groupExpr, sortExpr) && !list_member(collectiveGroupExpr, sortExpr)) {
            sortcl->groupSet = true;
        }
    }

    list_free_ext(groupExpr);
}

/*
 * @Description: If node is member of list.
 * @in list: Group expr list.
 * @in node: Expr.
 */
static bool group_member(List* list, Expr* node)
{
    Var* var1 = locate_distribute_var(node);

    ListCell* cell = NULL;
    foreach (cell, list) {
        Expr* expr = (Expr*)lfirst(cell);
        if (equal(expr, node)) {
            return true;
        } else {
            Var* var2 = locate_distribute_var(expr);
            if (var2 != NULL && equal(var1, var2)) {
                return true;
            }
        }
    }

    return false;
}

/*
 * @Description: Set sort+group distribute keys.
 * @in root - Per-query information for planning/optimization.
 * @in result_plan - group agg plan.
 * @in collectiveGroupExpr - collective group exprs.
 *
 */
static void adjust_plan_dis_key(PlannerInfo* root, Plan* result_plan, List* collectiveGroupExpr)
{
    EquivalenceClass* ec = NULL;
    ListCell* cell = NULL;
    ListCell* lc2 = NULL;

    /* Do a copy since distribute key is shared by multiple operators */
    result_plan->distributed_keys = list_copy(result_plan->distributed_keys);

    foreach (cell, result_plan->distributed_keys) {
        Expr* dis_key = (Expr*)lfirst(cell);

        /*
         * If this distribute key is not in collectiveGroupExpr, we need find it's EquivalenceClass.
         * If already found, replace it's members expr which be included in collectiveGroupExpr to this distribut key.
         */
        if (!group_member(collectiveGroupExpr, dis_key)) {
            /* Find include this dis expr equivalence class. */
            ec = get_expr_eqClass(root, dis_key);

            AssertEreport(ec != NULL, MOD_OPT, "invalid EquivalenceClass when setting sort+group distribute keys.");

            foreach (lc2, ec->ec_members) {
                EquivalenceMember* em = (EquivalenceMember*)lfirst(lc2);

                /* Replace this dis_key with em_expr. */
                if (group_member(collectiveGroupExpr, em->em_expr) &&
                    judge_node_compatible(root, (Node*)dis_key, (Node*)em->em_expr)) {
                    lfirst(cell) = copyObject(em->em_expr);
                    break;
                }
            }

            if (lc2 == NULL) {
                result_plan->distributed_keys = NIL;
                return;
            }
        }
    }
}

/*
 * @Description: We need set SortGroupClause's groupSet when groupingSets is not null,
 * avoid sort_pathkeys can be deleted if exist equivalence class.
 *
 * For exanple:
 *     select t1.a, t2.a from t1 inner join t2  on t1.a = t2.a
 *     group by grouping sets(t1.a, t2.a) order by 1, 2;
 *
 * In this case, sort_pathkeys only have t1.a, t2.a already be removed because t1.a = t2.a,
 * but because of grouping sets(Ap Function), some value of t1.a and t2.a can be seted to NULL so that
 * t1.a and t2.a is not equal, so t2.a can not be removed. Here we will again build sort path keys.
 * @in root - Per-query information for planning/optimization.
 * @in activeWindows - windows function list.
 * @in collectiveGroupExpr - collective group exprs if have grouping set clause.
 */
template <path_key pathKey>
static void rebuild_pathkey_for_groupingSet(
    PlannerInfo* root, List* tlist, List* activeWindows, List* collectiveGroupExpr)
{
    Query* parse = root->parse;

    if (!parse->groupingSets || !parse->groupClause) {
        return;
    }

    /*
     * To window function, if only need set SortGroupClause's groupset, it's pathkey will
     * be maked in grouping_planer's activeWindows part.
     */
    if (pathKey == windows_func_pathkey) {
        if (activeWindows != NIL) {
            WindowClause* wc = NULL;
            ListCell* l = NULL;

            foreach (l, activeWindows) {
                wc = (WindowClause*)lfirst(l);
                set_groupset_for_sortgroup_items(root, wc->partitionClause, tlist, collectiveGroupExpr);
                set_groupset_for_sortgroup_items(root, wc->orderClause, tlist, collectiveGroupExpr);
            }
        }
    } else if (pathKey == distinct_pathkey) {
        /* Make distinct pathkeys which groupSet is true. */
        if (parse->distinctClause && grouping_is_sortable(parse->distinctClause)) {
            set_groupset_for_sortgroup_items(root, parse->distinctClause, tlist, collectiveGroupExpr);

            root->distinct_pathkeys = make_pathkeys_for_sortclauses(root, parse->distinctClause, tlist, true);
        }
    } else if (pathKey == sort_pathkey) {
        /* Make sort pathkeys which groupSet is true. */
        if (parse->sortClause) {
            set_groupset_for_sortgroup_items(root, parse->sortClause, tlist, collectiveGroupExpr);

            root->sort_pathkeys = make_pathkeys_for_sortclauses(root, parse->sortClause, tlist, true);
        }
    }
}

static inline Path* choose_best_path(bool use_cheapest_path, PlannerInfo* root, Path* cheapest_path, Path* sorted_path)
{
        Path* best_path;
        if (use_cheapest_path) {
            best_path = cheapest_path;
        }
        else {
            best_path = sorted_path;
            ereport(DEBUG2, (errmodule(MOD_OPT), (errmsg("Use presorted path instead of cheapest path."))));
            /* print more details */
            if (log_min_messages <= DEBUG2)
                debug1_print_new_path(root, best_path, false);
        }

        return best_path;
}

#ifdef ENABLE_MULTIPLE_NODES
static bool has_ts_func(List* tlist)
{
    FillWalkerContext fill_context;
    error_t rc = memset_s(&fill_context, sizeof(fill_context), 0, sizeof(fill_context));
    securec_check(rc, "\0", "\0");

    expression_tree_walker((Node*)tlist, (walker)fill_function_call_walker, &fill_context);
    if (fill_context.fill_func_calls > 0 || fill_context.fill_last_func_calls > 0 ||
        fill_context.column_calls > 0) {
        return true;
    }
    return false;    
}
#endif

/* --------------------
 * grouping_planner
 *	  Perform planning steps related to grouping, aggregation, etc.
 *	  This primarily means adding top-level processing to the basic
 *	  query plan produced by query_planner.
 *
 * tuple_fraction is the fraction of tuples we expect will be retrieved
 *
 * tuple_fraction is interpreted as follows:
 *	  0: expect all tuples to be retrieved (normal case)
 *	  0 < tuple_fraction < 1: expect the given fraction of tuples available
 *		from the plan to be retrieved
 *	  tuple_fraction >= 1: tuple_fraction is the absolute number of tuples
 *		expected to be retrieved (ie, a LIMIT specification)
 *
 * Returns a query plan.  Also, root->query_pathkeys is returned as the
 * actual output ordering of the plan (in pathkey format).
 * --------------------
 */
static Plan* grouping_planner(PlannerInfo* root, double tuple_fraction)
{
    Query* parse = root->parse;
    List* tlist = parse->targetList;
    int64 offset_est = 0;
    int64 count_est = 0;
    double limit_tuples = -1.0;
    Plan* result_plan = NULL;
    List* current_pathkeys = NIL;
    double dNumGroups[2] = {1, 1}; /* dNumGroups[0] is local distinct, dNumGroups[1] is global distinct. */
    bool use_hashed_distinct = false;
    bool tested_hashed_distinct = false;
    bool needs_stream = false;
    bool has_second_agg_sort = false;
    List* collectiveGroupExpr = NIL;
    RelOptInfo* rel_info = NULL;
    char PlanContextName[NAMEDATALEN] = {0};
    MemoryContext PlanGenerateContext = NULL;
    MemoryContext oldcontext = NULL;
    errno_t rc = EOK;

    /*
     * Apply memory context for generate plan in optimizer.
     * OptimizerContext is NULL in PBE condition which we need to consider.
     */
    rc = snprintf_s(PlanContextName, NAMEDATALEN, NAMEDATALEN - 1, "PlanGenerateContext_%d", root->query_level);
    securec_check_ss(rc, "\0", "\0");

    PlanGenerateContext = AllocSetContextCreate(CurrentMemoryContext,
        PlanContextName,
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);

    /* Tweak caller-supplied tuple_fraction if have LIMIT/OFFSET */
    if (parse->limitCount || parse->limitOffset) {
        tuple_fraction = preprocess_limit(root, tuple_fraction, &offset_est, &count_est);

        /*
         * If we have a known LIMIT, and don't have an unknown OFFSET, we can
         * estimate the effects of using a bounded sort.
         */
        if (count_est > 0 && offset_est >= 0)
            limit_tuples = (double)count_est + (double)offset_est;
    }

    if (parse->setOperations) {
        List* set_sortclauses = NIL;

        /*
         * If there's a top-level ORDER BY, assume we have to fetch all the
         * tuples.	This might be too simplistic given all the hackery below
         * to possibly avoid the sort; but the odds of accurate estimates here
         * are pretty low anyway.
         */
        if (parse->sortClause)
            tuple_fraction = 0.0;

        /*
         * Construct the plan for set operations.  The result will not need
         * any work except perhaps a top-level sort and/or LIMIT.  Note that
         * any special work for recursive unions is the responsibility of
         * plan_set_operations.
         */
        result_plan = plan_set_operations(root, tuple_fraction, &set_sortclauses);

        /*
         * Calculate pathkeys representing the sort order (if any) of the set
         * operation's result.  We have to do this before overwriting the sort
         * key information...
         */
        current_pathkeys = make_pathkeys_for_sortclauses(root, set_sortclauses, result_plan->targetlist, true);

        /*
         * We should not need to call preprocess_targetlist, since we must be
         * in a SELECT query node.	Instead, use the targetlist returned by
         * plan_set_operations (since this tells whether it returned any
         * resjunk columns!), and transfer any sort key information from the
         * original tlist.
         */
        AssertEreport(
            parse->commandType == CMD_SELECT, MOD_OPT, "unexpected command type when performing grouping planner.");

        tlist = postprocess_setop_tlist((List*)copyObject(result_plan->targetlist), tlist);

        /*
         * Can't handle FOR [KEY] UPDATE/SHARE here (parser should have checked
         * already, but let's make sure).
         */
        if (parse->rowMarks)
            ereport(ERROR,
                (errmodule(MOD_OPT), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
#ifndef ENABLE_MULTIPLE_NODES
                    errmsg("SELECT FOR UPDATE/SHARE/NO KEY UPDATE/KEY SHARE is not allowed "
                           "with UNION/INTERSECT/EXCEPT"),
#else
                    errmsg("SELECT FOR UPDATE/SHARE is not allowed with UNION/INTERSECT/EXCEPT"),
#endif
                    errdetail("N/A"),
                    errcause("SQL uses unsupported feature."),
                    erraction("Modify SQL statement according to the manual.")));

        /*
         * Calculate pathkeys that represent result ordering requirements
         */
        AssertEreport(parse->distinctClause == NIL,
            MOD_OPT,
            "The distinct clause is not allowed when calculating pathkeys for sortclauses.");
        root->sort_pathkeys = make_pathkeys_for_sortclauses(root, parse->sortClause, tlist, true);
    } else {
        /* No set operations, do regular planning */
        List* sub_tlist = NIL;
        double sub_limit_tuples;
        AttrNumber* groupColIdx = NULL;
        bool need_tlist_eval = true;
        Path* cheapest_path = NULL;
        Path* sorted_path = NULL;
        Path* best_path = NULL;
        double numGroups[2] = {1, 1};
        long localNumGroup = 1;
        AggClauseCosts agg_costs;
        int numGroupCols;
        double path_rows;
        int path_width;
        bool use_hashed_grouping = false;
        WindowLists* wflists = NULL;
        uint32 maxref = 0;
        int* tleref_to_colnum_map = NULL;
        List* rollup_lists = NIL;
        List* rollup_groupclauses = NIL;
        bool  needSecondLevelAgg = true;	/* For olap function*/
        List* superset_key = root->dis_keys.superset_keys;
        Size hash_entry_size = 0;
        char PathContextName[NAMEDATALEN] = {0};
        MemoryContext PathGenerateContext = NULL;
        RelOptInfo* final_rel = NULL;
        standard_qp_extra qp_extra;

        /* Apply memory context for generate path in optimizer. */
        rc = snprintf_s(PathContextName, NAMEDATALEN, NAMEDATALEN - 1, "PathGenerateContext_%d", root->query_level);
        securec_check_ss(rc, "\0", "\0");

        PathGenerateContext = AllocSetContextCreate(CurrentMemoryContext,
            PathContextName,
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE);
        oldcontext = MemoryContextSwitchTo(PathGenerateContext);

        errno_t errorno = memset_s(&agg_costs, sizeof(AggClauseCosts), 0, sizeof(AggClauseCosts));
        securec_check(errorno, "\0", "\0");

        /* A recursive query should always have setOperations */
        AssertEreport(!root->hasRecursion, MOD_OPT, "A recursive query is not allowed when doing regular planning.");

        /* Preprocess GROUP BY clause, if any */
        /* Preprocess Grouping set, if any */
        if (parse->groupingSets)
            parse->groupingSets = expand_grouping_sets(parse->groupingSets, -1);

        if (parse->groupClause) {
            ListCell* lc = NULL;

            foreach (lc, parse->groupClause) {
                SortGroupClause* gc = (SortGroupClause*)lfirst(lc);

                if (gc->tleSortGroupRef > maxref)
                    maxref = gc->tleSortGroupRef;
            }
        }
        tleref_to_colnum_map = (int*)palloc((maxref + 1) * sizeof(int));

        if (parse->groupingSets) {
            ListCell* lc = NULL;
            ListCell* lc2 = NULL;
            ListCell* lc_set = NULL;
            List* sets = extract_rollup_sets(parse->groupingSets);
            bool isfirst = true;

            /* Keep all groupby columns in sets, each cell of sets is a rollup, the cell include many list */
            foreach (lc_set, sets) {
                List* current_sets =
                    reorder_grouping_sets((List*)lfirst(lc_set), (list_length(sets) == 1 ? parse->sortClause : NIL));

                List* groupclause = preprocess_groupclause(root, (List*)linitial(current_sets));

                if (isfirst) {
                    collectiveGroupExpr = get_group_expr((List*)llast(current_sets), tlist);
                } else if (collectiveGroupExpr != NIL) {
                    /* Last group idxs intersection */
                    collectiveGroupExpr =
                        list_intersection(collectiveGroupExpr, get_group_expr((List*)llast(current_sets), tlist));
                }
                isfirst = false;

                int ref = 0;

                /*
                 * Now that we've pinned down an order for the groupClause for
                 * this list of grouping sets, we need to remap the entries in
                 * the grouping sets from sortgrouprefs to plain indices
                 * (0-based) into the groupClause for this collection of
                 * grouping sets.
                 */
                foreach (lc, groupclause) {
                    SortGroupClause* gc = (SortGroupClause*)lfirst(lc);

                    tleref_to_colnum_map[gc->tleSortGroupRef] = ref++;
                }

                foreach (lc, current_sets) {
                    foreach (lc2, (List*)lfirst(lc)) {
                        lfirst_int(lc2) = tleref_to_colnum_map[lfirst_int(lc2)];
                    }
                }

                rollup_lists = lcons(current_sets, rollup_lists);
                rollup_groupclauses = lcons(groupclause, rollup_groupclauses);
            }
        } else {
            /* Preprocess GROUP BY clause, if any */
            if (parse->groupClause)
                parse->groupClause = preprocess_groupclause(root, NIL);
            rollup_groupclauses = list_make1(parse->groupClause);
        }

        numGroupCols = list_length(parse->groupClause);

        /* Preprocess targetlist */
        tlist = preprocess_targetlist(root, tlist);

        if (parse->upsertClause) {
            UpsertExpr* upsertClause = parse->upsertClause;
            upsertClause->updateTlist =
                preprocess_upsert_targetlist(upsertClause->updateTlist, parse->resultRelation, parse->rtable);
        }
        /*
         * Locate any window functions in the tlist.  (We don't need to look
         * anywhere else, since expressions used in ORDER BY will be in there
         * too.)  Note that they could all have been eliminated by constant
         * folding, in which case we don't need to do any more work.
         */
        if (parse->hasWindowFuncs) {
            wflists = make_windows_lists(list_length(parse->windowClause));
            find_window_functions((Node*)tlist, wflists);

            if (wflists->numWindowFuncs > 0)
                select_active_windows(root, wflists);
            else
                parse->hasWindowFuncs = false;
        }

        /*
         * Check this query if is correlation subquery, if is we will
         * set correlated flag from correlative root to current root.
         */
        check_plan_correlation(root, (Node*)parse);

        /*
         * Generate appropriate target list for subplan; may be different from
         * tlist if grouping or aggregation is needed.
         */
        sub_tlist = make_subplanTargetList(root, tlist, &groupColIdx, &need_tlist_eval);

        /* Set matching and superset key for planner info of current query level */
        if (IS_STREAM_PLAN) {
            set_root_matching_key(root, tlist);

            build_grouping_itst_keys(root, wflists ? wflists->activeWindows : NULL);
        }

        /*
         * Do aggregate preprocessing, if the query has any aggs.
         *
         * Note: think not that we can turn off hasAggs if we find no aggs. It
         * is possible for constant-expression simplification to remove all
         * explicit references to aggs, but we still have to follow the
         * aggregate semantics (eg, producing only one output row).
         */
        if (parse->hasAggs) {
            /*
             * Collect statistics about aggregates for estimating costs. Note:
             * we do not attempt to detect duplicate aggregates here; a
             * somewhat-overestimated cost is okay for our present purposes.
             */
            count_agg_clauses(root, (Node*)tlist, &agg_costs);
            count_agg_clauses(root, parse->havingQual, &agg_costs);

            /*
             * Preprocess MIN/MAX aggregates, if any.  Note: be careful about
             * adding logic between here and the optimize_minmax_aggregates
             * call.  Anything that is needed in MIN/MAX-optimizable cases
             * will have to be duplicated in planagg.c.
             */
            /* Set u_sess->opt_cxt.query_dop to forbidden the parallel of subplan. */
            int dop_tmp = u_sess->opt_cxt.query_dop;
            u_sess->opt_cxt.query_dop = 1;
            preprocess_minmax_aggregates(root, tlist);
            /* Reset u_sess->opt_cxt.query_dop. */
            u_sess->opt_cxt.query_dop = dop_tmp;
        }

        /*
         * Figure out whether there's a hard limit on the number of rows that
         * query_planner's result subplan needs to return.  Even if we know a
         * hard limit overall, it doesn't apply if the query has any
         * grouping/aggregation operations.
         */
        if (parse->groupClause || parse->groupingSets || parse->distinctClause || parse->hasAggs ||
            parse->hasWindowFuncs || root->hasHavingQual)
            sub_limit_tuples = -1.0;
        else
            sub_limit_tuples = limit_tuples;

        /* Make tuple_fraction, limit_tuples accessible to lower-level routines */
        root->tuple_fraction = tuple_fraction;
        root->limit_tuples = sub_limit_tuples;

        /* Set up data needed by standard_qp_callback */
        standard_qp_init(root, (void*)&qp_extra, tlist,
                    wflists ? wflists->activeWindows : NIL,
                    parse->groupingSets ?
                        (rollup_groupclauses ? (List*)llast(rollup_groupclauses) : NIL) :
                        parse->groupClause);

        /* 
         * Generate pathlist by query_planner for final_rel and canonicalize 
         * all the pathkeys.
         */
        final_rel = query_planner(root, sub_tlist, standard_qp_callback, &qp_extra);

        /* 
         * In the following, generate the best unsorted and presorted paths for 
         * this Query (but note there may not be any presorted path). 
         */
        bool has_groupby = true;

        /* First of all, estimate the number of groups in the query. */
        has_groupby = get_number_of_groups(root, 
                                           final_rel, 
                                           dNumGroups,
                                           rollup_groupclauses, 
                                           rollup_lists);

        /* Then update the tuple_fraction by the number of groups in the query. */
        update_tuple_fraction(root, 
                              final_rel, 
                              dNumGroups);

        /* 
         * Finally, generate the best unsorted and presorted paths for 
         * this Query. 
         */
        generate_cheapest_and_sorted_path(root, 
                                          final_rel,
                                          &cheapest_path, 
                                          &sorted_path, 
                                          dNumGroups, 
                                          has_groupby);

        /* restore superset keys */
        root->dis_keys.superset_keys = superset_key;

        /*
         * Extract rowcount and width estimates for possible use in grouping
         * decisions.  Beware here of the possibility that
         * cheapest_path->parent is NULL (ie, there is no FROM clause).  Also,
         * if the final rel has been proven dummy, its rows estimate will be
         * zero; clamp it to one to avoid zero-divide in subsequent
         * calculations.
         */
        if (cheapest_path->parent) {
            path_rows = clamp_row_est(PATH_LOCAL_ROWS(cheapest_path));
            path_width = cheapest_path->parent->width;
        } else {
            path_rows = 1;    /* assume non-set result */
            path_width = 100; /* arbitrary */
        }

        /* If grouping sets are present, we can currently do only sorted
         * grouping.
         */
        if (parse->groupingSets) {
            use_hashed_grouping = false;
            /* We need to set numGroups, HashAgg is necessary above sortAgg
             * in stream plan.
             */
            numGroups[0] = dNumGroups[0];
            numGroups[1] = dNumGroups[1];
            localNumGroup = (long)Min(dNumGroups[0], (double)LONG_MAX);
        } else if (parse->groupClause ||
                   (IS_STREAM_PLAN && list_length(agg_costs.exprAggs) == 1 && !agg_costs.hasDnAggs)) {
            /*
             * If grouping, decide whether to use sorted or hashed grouping.
             */
            use_hashed_grouping = choose_hashed_grouping(root,
                tuple_fraction,
                limit_tuples,
                path_width,
                cheapest_path,
                sorted_path,
                dNumGroups,
                &agg_costs,
                &hash_entry_size);
            /* Also convert # groups to long int --- but 'ware overflow! */
            numGroups[0] = dNumGroups[0];
            numGroups[1] = dNumGroups[1];
            localNumGroup = (long)Min(dNumGroups[0], (double)LONG_MAX);
        } else if (parse->distinctClause && sorted_path && !root->hasHavingQual && !parse->hasAggs &&
                   (wflists == NULL || !wflists->activeWindows)) {
            Size hashentrysize;

            /*
             * Don't do it if it doesn't look like the hashtable will fit into
             * work_mem.
             */
            if (root->glob->vectorized)
                hashentrysize = get_path_actual_total_width(cheapest_path, root->glob->vectorized, OP_HASHAGG);
            else
                hashentrysize = get_hash_entry_size(path_width);

            /*
             * We'll reach the DISTINCT stage without any intermediate
             * processing, so figure out whether we will want to hash or not
             * so we can choose whether to use cheapest or sorted path.
             */
            use_hashed_distinct = choose_hashed_distinct(root,
                tuple_fraction,
                limit_tuples,
                path_rows,
                path_width,
                cheapest_path->startup_cost,
                cheapest_path->total_cost,
                ng_get_dest_distribution(cheapest_path),
                sorted_path->startup_cost,
                sorted_path->total_cost,
                ng_get_dest_distribution(sorted_path),
                sorted_path->pathkeys,
                dNumGroups[0],
                hashentrysize);
            tested_hashed_distinct = true;
        }

        /*
         * Select the best path.  If we are doing hashed grouping, we will
         * always read all the input tuples, in addition cn gather permit on
         * then use the cheapest-total path.
         * Otherwise, trust query_planner's decision about which to use.
         */
        best_path = choose_best_path((use_hashed_grouping || use_hashed_distinct ||
                                        sorted_path == NULL || permit_gather(root)),
                                    root, cheapest_path, sorted_path);

        (void)MemoryContextSwitchTo(PlanGenerateContext);

        /* record the param */
        root->param_upper = PATH_REQ_UPPER(cheapest_path);

        /*
         * Check to see if it's possible to optimize MIN/MAX aggregates. If
         * so, we will forget all the work we did so far to choose a "regular"
         * path ... but we had to do it anyway to be able to tell which way is
         * cheaper.
         */
        result_plan = optimize_minmax_aggregates(root, tlist, &agg_costs, best_path);
        if (result_plan != NULL) {
            /*
             * optimize_minmax_aggregates generated the full plan, with the
             * right tlist, and it has no sort order.
             */
            current_pathkeys = NIL;
        } else {
            /*
             * Normal case --- create a plan according to query_planner's
             * results.
             */
            bool need_sort_for_grouping = false;

            /* TOD for some cases, we need backup the original subtlist, e.g. we will add  */
            root->origin_tlist = sub_tlist;

            result_plan = create_plan(root, best_path);

            rel_info = best_path->parent;

            /*
             * For dummy plan, we should return it quickly. Meanwhile, we should
             * eliminate agg node, or an error will thrown out later
             *
             * groupClause == NULL can lead to return 1 rows result, e.g. count(*)
             * parse->groupingSets != NULL also can lead to 1 rows result, e.g. include group by ().
             * So these situation can not enter this branch.
             *
             */
            if (is_dummy_plan(result_plan) && parse->groupingSets == NIL && parse->groupClause != NIL) {
                if (parse->hasAggs || parse->hasWindowFuncs) {
                    ListCell* lc = NULL;
                    foreach (lc, tlist) {
                        TargetEntry* tle = (TargetEntry*)lfirst(lc);
                        List* exprList = pull_var_clause(
                            (Node*)tle->expr, PVC_INCLUDE_AGGREGATES_OR_WINAGGS, PVC_RECURSE_PLACEHOLDERS);
                        ListCell* lc2 = NULL;
                        Node* node = NULL;
                        foreach (lc2, exprList) {
                            node = (Node*)lfirst(lc2);
                            if (IsA(node, Aggref) || IsA(node, GroupingFunc) || IsA(node, WindowFunc))
                                break;
                        }

                        /*
                         * For aggref, grouping or windows expr, we need replace them by NULL, else error will
                         * happen, because AggRef not in agg node.
                         */
                        if (lc2 != NULL) {
                            tle->expr = (Expr*)makeNullConst(exprType(node), exprTypmod(node), exprCollation(node));
                        }
                        list_free_ext(exprList);
                    }
                }
                result_plan->targetlist = tlist;
                return result_plan;
            }

            if (use_hashed_grouping && list_length(agg_costs.exprAggs) == 1 &&
                (!is_execute_on_datanodes(result_plan) || is_replicated_plan(result_plan))) {
                if (!grouping_is_sortable(parse->groupClause)) {
                    ereport(ERROR,
                        (errmodule(MOD_OPT_PLANNER), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("GROUP BY cannot be implemented."),
                            errdetail("Some of the datatypes only support hashing, "
                                "while others only support sorting."),
                            errcause("GROUP BY uses unsupported datatypes."),
                            erraction("Modify SQL statement according to the manual.")));
                }

                use_hashed_grouping = false;
                if (sorted_path != NULL && sorted_path != cheapest_path) {
                    best_path = sorted_path;
                    result_plan = create_plan(root, best_path);
                }
            }
            current_pathkeys = best_path->pathkeys;

            /* Detect if we'll need an explicit sort for grouping */
            if (parse->groupClause && !use_hashed_grouping &&
                !pathkeys_contained_in(root->group_pathkeys, current_pathkeys)) {
                need_sort_for_grouping = true;

                /*
                 * Always override create_plan's tlist, so that we don't sort
                 * useless data from a "physical" tlist.
                 */
                need_tlist_eval = true;
            }

            /*
             * create_plan returns a plan with just a "flat" tlist of required
             * Vars.  Usually we need to insert the sub_tlist as the tlist of
             * the top plan node.  However, we can skip that if we determined
             * that whatever create_plan chose to return will be good enough.
             */
            if (need_tlist_eval) {
                /*
                 * If the top-level plan node is one that cannot do expression
                 * evaluation, we must insert a Result node to project the
                 * desired tlist.
                 */
                if (!is_projection_capable_plan(result_plan) ||
                    (is_vector_scan(result_plan) && vector_engine_unsupport_expression_walker((Node*)sub_tlist))) {
                    result_plan = (Plan*)make_result(root, sub_tlist, NULL, result_plan);
                } else {
                    /*
                     * Otherwise, just replace the subplan's flat tlist with
                     * the desired tlist.
                     */
                    result_plan->targetlist = sub_tlist;

                    if (IsA(result_plan, PartIterator)) {
                        /*
                         * If is a PartIterator + Scan, push the PartIterator's
                         * tlist to Scan.
                         */
                        result_plan->lefttree->targetlist = sub_tlist;
                    }
#ifdef PGXC
                    /*
                     * If the Join tree is completely shippable, adjust the
                     * target list of the query according to the new targetlist
                     * set above. For now do this only for SELECT statements.
                     */
                    if (IsA(result_plan, RemoteQuery) && parse->commandType == CMD_SELECT && !permit_gather(root)) {
                        pgxc_rqplan_adjust_tlist(
                            root, (RemoteQuery*)result_plan, ((RemoteQuery*)result_plan)->is_simple ? false : true);
                        if (((RemoteQuery*)result_plan)->is_simple)
                            AssertEreport(((RemoteQuery*)result_plan)->sql_statement == NULL,
                                MOD_OPT,
                                "invalid sql statement of result plan when adjusting the targetlist of remote query.");
                    }
#endif /* PGXC */
                }

                /*
                 * Also, account for the cost of evaluation of the sub_tlist.
                 * See comments for add_tlist_costs_to_plan() for more info.
                 */
                add_tlist_costs_to_plan(root, result_plan, sub_tlist);
            } else {
                /*
                 * Since we're using create_plan's tlist and not the one
                 * make_subplanTargetList calculated, we have to refigure any
                 * grouping-column indexes make_subplanTargetList computed.
                 *
                 * We don't want any excess columns for hashagg, since we support hashagg write-out-to-disk now
                 */
                if (use_hashed_grouping)
                    disuse_physical_tlist(result_plan, best_path);

                locate_grouping_columns(root, tlist, result_plan->targetlist, groupColIdx);
            }
#ifdef ENABLE_MULTIPLE_NODES
            /* shuffle to another node group in FORCE mode (CNG_MODE_FORCE) */
            if (IS_STREAM_PLAN && !parse->hasForUpdate &&
                (parse->hasAggs || parse->groupClause != NIL || parse->groupingSets != NIL ||
                    parse->distinctClause != NIL || parse->sortClause != NIL ||
                    (wflists != NULL && wflists->activeWindows))) {
                Plan* old_result_plan = result_plan;
                List* groupcls = parse->groupClause;
                Path* subpath = NULL;
                bool can_shuffle = true;

                /* deal with window agg if no group clause */
                if (groupcls == NIL && wflists != NULL && wflists->activeWindows) {
                    WindowClause* wc1 = (WindowClause*)linitial(wflists->activeWindows);
                    groupcls = wc1->partitionClause;
                    /* need to reduce targetlist here if no group clause */
                    subpath = best_path;

                    /* not shuffle if partitionClause contains aggregates */
                    can_shuffle = check_windowagg_can_shuffle(wc1->partitionClause, tlist);
                }

                if (can_shuffle)
                    result_plan = ng_agg_force_shuffle(root, groupcls, result_plan, tlist, subpath);

                /*
                 * 1. the stream will make data unsorted, mark it.
                 * 2. The groupClause cannot be NIL when add sort for group which will
                 *    generate sort key from group key.
                 */
                if (old_result_plan != result_plan) {
                    current_pathkeys = NIL;
                    if (parse->groupClause != NIL) {
                        need_sort_for_grouping = true;
                    }
                }
            }
#endif
            /*
             * groupColIdx is now cast in stone, so record a mapping from
             * tleSortGroupRef to column index. setrefs.c needs this to
             * finalize GROUPING() operations.
             */
            if (parse->groupingSets) {
                AttrNumber* grouping_map = (AttrNumber*)palloc0(sizeof(AttrNumber) * (maxref + 1));
                ListCell* lc = NULL;
                int i = 0;

                /* All take part in group columns */
                foreach (lc, parse->groupClause) {
                    SortGroupClause* gc = (SortGroupClause*)lfirst(lc);

                    grouping_map[gc->tleSortGroupRef] = groupColIdx[i++];
                }

                root->grouping_map = grouping_map;

                result_plan = build_groupingsets_plan(root,
                    parse,
                    &tlist,
                    need_sort_for_grouping,
                    rollup_groupclauses,
                    rollup_lists,
                    &groupColIdx,
                    &agg_costs,
                    localNumGroup,
                    result_plan,
                    wflists,
                    &needSecondLevelAgg,
                    collectiveGroupExpr);

                /* Delete eq class expr after grouping */
                delete_eq_member(root, tlist, collectiveGroupExpr);

                numGroupCols = list_length(parse->groupClause);
                /*
                 * these are destroyed by build_grouping_chain, so make sure
                 * we don't try and touch them again
                 */
                rollup_groupclauses = NIL;
                rollup_lists = NIL;

                if (grouping_is_hashable(parse->groupClause)) {
                    /* for advantage, hashagg is my first choice */
                    use_hashed_grouping = true;
                } else if (grouping_is_sortable(parse->groupClause)) {
                    /* or do sortagg */
                    use_hashed_grouping = false;
                } else {
                    ereport(ERROR,
                        (errmodule(MOD_OPT_PLANNER), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("GROUP BY cannot be implemented."),
                            errdetail("Some of the datatypes only support hashing, "
                                "while others only support sorting."),
                            errcause("GROUP BY uses unsupported datatypes."),
                            erraction("Modify SQL statement according to the manual.")));
                }

                if (IS_STREAM_PLAN && (is_hashed_plan(result_plan) || is_rangelist_plan(result_plan))) {
                    if (expression_returns_set((Node*)tlist)) {
                        errno_t sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
                            NOTPLANSHIPPING_LENGTH,
                            "set-valued function + groupingsets");
                        securec_check_ss_c(sprintf_rc, "\0", "\0");
                        mark_stream_unsupport();
                    }

                    if (check_subplan_in_qual(tlist, result_plan->qual)) {
                        errno_t sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
                            NOTPLANSHIPPING_LENGTH,
                            "var in quals doesn't exist in targetlist");
                        securec_check_ss_c(sprintf_rc, "\0", "\0");
                        mark_stream_unsupport();
                    }
                }
            }

            if (IS_STREAM_PLAN) {
                if (is_execute_on_coordinator(result_plan) || is_execute_on_allnodes(result_plan) ||
                    is_replicated_plan(result_plan)) {
                    needs_stream = false;
                } else {
                    needs_stream = needs_agg_stream(root, tlist, result_plan->distributed_keys, &result_plan->exec_nodes->distribution);
                }
#ifndef ENABLE_MULTIPLE_NODES
                needs_stream = needs_stream && (result_plan->dop > 1);
#endif
            }

            /*
             * Insert AGG or GROUP node if needed, plus an explicit sort step
             * if necessary.
             *
             * HAVING clause, if any, becomes qual of the Agg or Group node.
             */
            bool contain_sets_expression =
                expression_returns_set((Node*)tlist) || expression_returns_set((Node*)parse->havingQual);
            bool next_is_second_level_group = false;

            /* Don't need second level agg when distribute key is in group clause of groupingsets. */
            if (!needSecondLevelAgg) {
                /* need do nothing*/
            } else if (use_hashed_grouping)  {
                /* Hashed aggregate plan --- no sort needed */
                if (IS_STREAM_PLAN &&
                    is_execute_on_datanodes(result_plan) &&
                    !is_replicated_plan(result_plan)) {
                    if (agg_costs.hasDnAggs || list_length(agg_costs.exprAggs) == 0) {
                        result_plan = generate_hashagg_plan(root,
                                                            result_plan,
                                                            tlist,
                                                            &agg_costs,
                                                            numGroupCols,
                                                            numGroups,
                                                            wflists,
                                                            groupColIdx,
                                                            extract_grouping_ops(parse->groupClause),
                                                            &needs_stream,
                                                            hash_entry_size,
                                                            AGG_LEVEL_1_INTENT,
                                                            rel_info);
                    } else {
                        Node    *node = (Node *) linitial(agg_costs.exprAggs);
                        AssertEreport(list_length(agg_costs.exprAggs) == 1,
                                      MOD_OPT,
                                      "invalid length of distinct expression when generating plan for hashed aggregate.");

                        result_plan = get_count_distinct_partial_plan(root,
                                                                      result_plan,
                                                                      &tlist,
                                                                      node,
                                                                      agg_costs,
                                                                      numGroups,
                                                                      wflists,
                                                                      groupColIdx,
                                                                      &needs_stream,
                                                                      hash_entry_size,
                                                                      rel_info);
                    }

                    next_is_second_level_group = true;
                } else if (!parse->groupingSets) {
                    /*
                     * To Ap function, need not do hashagg if it is not stream plan,
                     * in this case, all work always is finished in sort agg.
                    */
                    result_plan = (Plan *) make_agg(root,
                                    tlist,
                                    (List *) parse->havingQual,
                                    AGG_HASHED,
                                    &agg_costs,
                                    numGroupCols,
                                    groupColIdx,
                                    extract_grouping_ops(parse->groupClause),
                                    localNumGroup,
                                    result_plan,
                                    wflists,
                                    needs_stream,
                                    true,
                                    NIL,
                                    false,
                                    hash_entry_size);

                    next_is_second_level_group = true;
                }

                if (IS_STREAM_PLAN &&
                    needs_stream &&
                    is_execute_on_datanodes(result_plan) &&
                    !is_replicated_plan(result_plan))  {

                    if (next_is_second_level_group && contain_sets_expression) {
                        errno_t sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
                            NOTPLANSHIPPING_LENGTH,
                            "\"set-valued expression in qual/targetlist + two-level Groupagg\"");
                        securec_check_ss_c(sprintf_rc, "\0", "\0");
                        mark_stream_unsupport();
                    }

                    if (wflists != NULL && wflists->activeWindows) {
                        result_plan->targetlist = make_windowInputTargetList(root, result_plan->targetlist,
                                                            wflists->activeWindows);
                    }
                    result_plan = (Plan *)mark_agg_stream(root, tlist, result_plan, parse->groupClause,
                                        AGG_LEVEL_1_INTENT, &has_second_agg_sort);
                    if (!has_second_agg_sort) {
                        current_pathkeys = NIL;
                    } else {
                        current_pathkeys = root->group_pathkeys;
                    }
                } else {
                    /* Hashed aggregation produces randomly-ordered results */
                    current_pathkeys = NIL;
                }
            } else if (parse->hasAggs) {
                /* Plain aggregate plan --- sort if needed */
                AggStrategy aggstrategy;
                bool count_distinct_optimization =
                    IS_STREAM_PLAN && is_execute_on_datanodes(result_plan) && !is_replicated_plan(result_plan);
                Node* distinct_node = NULL;
                List* distinct_node_list = NIL;
                Plan* partial_plan = NULL;
                bool two_level_sort = false;
                bool distinct_needs_stream = false;
                bool distinct_needs_local_stream = false;
                List* orig_list = NIL;
                List* replace_list = NIL;
                double multiple = 0.0;
                List* distributed_key = NIL;

#ifndef ENABLE_MULTIPLE_NODES
                count_distinct_optimization = count_distinct_optimization && (result_plan->dop > 1);
#endif

                /* check whether two_level_sort is needed, only in two case:
                 * 1. there's no group by clause, AGG_PLAIN used.
                 * 2. there's group by clause, and a redistribution on group by clause is needed.
                 * If so, we should remove aggdistinct and then restore it back */
                if (count_distinct_optimization && list_length(agg_costs.exprAggs) == 1 &&
                    (!(parse->groupClause && !needs_stream)) && !agg_costs.hasDnAggs && !agg_costs.hasdctDnAggs &&
                    agg_costs.numOrderedAggs == 0) {
                    ListCell* lc = NULL;
                    List* var_list = NIL;
                    List* duplicate_list = NIL;

                    distributed_key = get_distributekey_from_tlist(
                        root, tlist, parse->groupClause, result_plan->plan_rows, &multiple);
                    var_list = make_agg_var_list(root, tlist, &duplicate_list);
                    distinct_node = (Node*)linitial(agg_costs.exprAggs);
                    distinct_node_list = list_make1(distinct_node);
                    two_level_sort = needs_two_level_groupagg(root,
                        result_plan,
                        distinct_node,
                        distributed_key,
                        &distinct_needs_stream,
                        &distinct_needs_local_stream);
                    if (two_level_sort) {
                        foreach (lc, var_list) {
                            Aggref* node = (Aggref*)lfirst(lc);
                            if (IsA(node, Aggref) && node->aggdistinct != NIL) {
                                List* aggdistinct = node->aggdistinct;
                                Aggref* n = NULL;

                                node->aggdistinct = NIL;
                                n = (Aggref*)copyObject(node);

                                if (need_adjust_agg_inner_func_type(n))
                                    n->aggtype = n->aggtrantype;

                                orig_list = lappend(orig_list, copyObject(n));
                                n->aggdistinct = aggdistinct;
                                replace_list = lappend(replace_list, n);
                            }
                        }
                        foreach (lc, duplicate_list) {
                            Aggref* node = (Aggref*)lfirst(lc);
                            if (IsA(node, Aggref) && node->aggdistinct != NIL) {
                                pfree_ext(node->aggdistinct);
                                node->aggdistinct = NIL;
                            }
                        }
                    }
                    list_free_ext(var_list);
                    list_free_ext(duplicate_list);
                }

                if (parse->groupClause) {
                    /* if there's count(distinct), we now only support redistribute by group clause */
                    if (count_distinct_optimization && needs_stream &&
                        (agg_costs.exprAggs != NIL || agg_costs.hasDnAggs || agg_costs.numOrderedAggs > 0)) {
                        distributed_key = get_distributekey_from_tlist(
                            root, tlist, parse->groupClause, result_plan->plan_rows, &multiple);
                        /* we can apply local sortagg if count(distinct) expr is distribute column */
                        if (two_level_sort) {
                            if (distinct_needs_local_stream) {
                                result_plan =
                                    create_local_redistribute(root, result_plan, list_make1(distinct_node), 0);
                            }

                            if (distinct_needs_stream) {
                                result_plan =
                                    make_redistribute_for_agg(root, result_plan, list_make1(distinct_node), 0);
                                need_sort_for_grouping = true;
                            }

                            if (need_sort_for_grouping)
                                result_plan =
                                    (Plan*)make_sort_from_groupcols(root, parse->groupClause, groupColIdx, result_plan);

                            result_plan = (Plan*)make_agg(root,
                                tlist,
                                (List*)parse->havingQual,
                                AGG_SORTED,
                                &agg_costs,
                                numGroupCols,
                                groupColIdx,
                                extract_grouping_ops(parse->groupClause),
                                localNumGroup,
                                result_plan,
                                wflists,
                                needs_stream,
                                true,
                                NIL,
                                0,
                                true);

                            if (wflists != NULL && wflists->activeWindows) {
                                /* If have windows we need alter agg's targetlist. */
                                result_plan->targetlist =
                                    make_windowInputTargetList(root, result_plan->targetlist, wflists->activeWindows);
                            }

                            partial_plan = result_plan;
                            next_is_second_level_group = true;
                        }

                        if (distributed_key != NIL) {
                            result_plan = make_redistribute_for_agg(root, result_plan, distributed_key, multiple);
                            needs_stream = false;
                        } else {
                            errno_t sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
                                NOTPLANSHIPPING_LENGTH,
                                "\"Count(Distinct) + Group by\" on redistribution unsupported data type");
                            securec_check_ss_c(sprintf_rc, "\0", "\0");
                            mark_stream_unsupport();
                        }
                        need_sort_for_grouping = true;
                    }

                    /* Add local redistribute for a local group cols. */
                    if (IS_STREAM_PLAN && !needs_stream && result_plan->dop > 1)
                        result_plan = create_local_redistribute(root, result_plan, result_plan->distributed_keys, 0);

                    if (need_sort_for_grouping && partial_plan == NULL &&
                        (IS_STREAM_PLAN || parse->groupingSets == NULL)) {
                        result_plan =
                            (Plan*)make_sort_from_groupcols(root, parse->groupClause, groupColIdx, result_plan);
                        current_pathkeys = root->group_pathkeys;
                    }
                    aggstrategy = AGG_SORTED;

                } else {
                    if (IS_STREAM_PLAN && count_distinct_optimization) {
                        if (list_length(agg_costs.exprAggs) > 1 ||
                            agg_costs.hasDnAggs ||
                            agg_costs.numOrderedAggs > 0) {
                            /*
                             * Add gather here for listagg&array_agg is confused with sort in result_plan,
                             * as gather may lead to disordered in MPP scenario, but user can add order by
                             * in agg function which can avoid this changed and is recommended in SQL standard.
                             */
                            if ((u_sess->attr.attr_sql.rewrite_rule & PARTIAL_PUSH) && permit_from_rewrite_hint(root, PARTIAL_PUSH))  {
                                needs_stream = false;
                                result_plan = make_simple_RemoteQuery(result_plan, root, false);
                            } else {
                                errno_t sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
                                    NOTPLANSHIPPING_LENGTH,
                                    "multi count(distinct) or agg which need order can not ship.");
                                securec_check_ss_c(sprintf_rc, "\0", "\0");
                                mark_stream_unsupport();
                            }
                        } else if (agg_costs.exprAggs != NIL) {

                            if (distinct_needs_local_stream)
                                result_plan =
                                    create_local_redistribute(root, result_plan, list_make1(distinct_node), 0);

                            if (distinct_needs_stream) {
                                result_plan =
                                    make_redistribute_for_agg(root, result_plan, list_make1(distinct_node), 0);
                            } else if (!two_level_sort) {
                                /* we don't support non-distributable count(distinct) expr to push down */
                                errno_t sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
                                    NOTPLANSHIPPING_LENGTH,
                                    "\"Count(Distinct)\" on redistribution unsupported data type");
                                securec_check_ss_c(sprintf_rc, "\0", "\0");
                                mark_stream_unsupport();
                            } else if (result_plan->dop > 1) {
                                result_plan =
                                    create_local_redistribute(root, result_plan, result_plan->distributed_keys, 0);
                            }
                        }
                    }

                    aggstrategy = AGG_PLAIN;
                    /* Result will be only one row anyway; no sort order */
                    current_pathkeys = NIL;
                }

                if (IS_STREAM_PLAN && needs_stream && agg_costs.hasPolymorphicType) {
                    errno_t sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
                        NOTPLANSHIPPING_LENGTH,
                        "\"Aggregate on polymorphic argument type \"");
                    securec_check_ss_c(sprintf_rc, "\0", "\0");
                    mark_stream_unsupport();
                }

                /* If two level agg is needs and we have non-exist var in subplan of qual, push down is unsupported */
                if (IS_STREAM_PLAN && (partial_plan != NULL || needs_stream) &&
                    check_subplan_in_qual(tlist, (List*)parse->havingQual)) {
                    errno_t sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
                        NOTPLANSHIPPING_LENGTH,
                        "\"Subplan in having qual + two-level Groupagg\"");
                    securec_check_ss_c(sprintf_rc, "\0", "\0");
                    mark_stream_unsupport();
                }

                /* If two level agg is needs and we have non-exist var in subplan of qual, push down is unsupported */
                if (IS_STREAM_PLAN && next_is_second_level_group && contain_sets_expression) {
                    errno_t sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
                        NOTPLANSHIPPING_LENGTH,
                        "\"set-valued expression in qual/targetlist + two-level Groupagg\"");
                    securec_check_ss_c(sprintf_rc, "\0", "\0");
                    mark_stream_unsupport();
                }

                if (partial_plan != NULL) {
                    result_plan = mark_top_agg(root, tlist, partial_plan, result_plan, AGG_LEVEL_1_INTENT);
                } else if (parse->groupingSets == NIL || IS_STREAM) {
                    result_plan = (Plan*)make_agg(root,
                        tlist,
                        (List*)parse->havingQual,
                        aggstrategy,
                        &agg_costs,
                        numGroupCols,
                        groupColIdx,
                        extract_grouping_ops(parse->groupClause),
                        localNumGroup,
                        result_plan,
                        wflists,
                        needs_stream,
                        true,
                        NIL,
                        0,
                        true);
                }
                next_is_second_level_group = true;

                /* Save the agg plan to restore aggdistinct node */
                if (distinct_node != NULL) {
                    list_free_ext(distinct_node_list);
                    if (needs_stream)
                        partial_plan = result_plan;
                }

#ifdef STREAMPLAN
                if (IS_STREAM_PLAN && needs_stream && is_execute_on_datanodes(result_plan) &&
                    !is_replicated_plan(result_plan)) {
                    if (next_is_second_level_group && contain_sets_expression) {
                        errno_t sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
                            NOTPLANSHIPPING_LENGTH,
                            "\"set-valued expression in qual/targetlist + two-level Groupagg\"");
                        securec_check_ss_c(sprintf_rc, "\0", "\0");
                        mark_stream_unsupport();
                    }

                    if (wflists != NULL && wflists->activeWindows)
                        result_plan->targetlist =
                            make_windowInputTargetList(root, result_plan->targetlist, wflists->activeWindows);

                    result_plan = (Plan*)mark_agg_stream(
                        root, tlist, result_plan, parse->groupClause, AGG_LEVEL_1_INTENT, &has_second_agg_sort);

                    if (!has_second_agg_sort)
                        current_pathkeys = NIL;
                    else
                        current_pathkeys = root->group_pathkeys;
                }
#endif
                if (partial_plan != NULL) {
                    partial_plan->targetlist = (List*)replace_node_clause((Node*)partial_plan->targetlist,
                        (Node*)orig_list,
                        (Node*)replace_list,
                        RNC_COPY_NON_LEAF_NODES);
                    list_free_ext(replace_list);
                    list_free_deep(orig_list);
                }
            } else if (parse->groupClause) {
                /*
                 * GROUP BY without aggregation, so insert a group node (plus
                 * the appropriate sort node, if necessary).
                 *
                 * Add an explicit sort if we couldn't make the path come out
                 * the way the GROUP node needs it.
                 */
                if (IS_STREAM || parse->groupingSets == NIL) {
                    /*
                     * For SQL that is not shippable, we have done group operator with
                     * function build_groupingsets_plan, so we skip add group operator
                     * here, and if query is not shippable, needs_stream must be false,
                     * and group operatror will not be added
                     */
                    if (!needs_stream && result_plan->dop > 1)
                        result_plan = create_local_redistribute(root, result_plan, result_plan->distributed_keys, 0);

                    if (need_sort_for_grouping) {
                        result_plan =
                            (Plan*)make_sort_from_groupcols(root, parse->groupClause, groupColIdx, result_plan);
                        current_pathkeys = root->group_pathkeys;
                    }

                    /*
                     * If need two level sort+group and non-var exists, this group's targetlist
                     * should be result_plan->targetlist rather than tlist, if not Error(Var can not
                     * be found) can happend.
                     */
                    result_plan = (Plan*)make_group(root,
                        needs_stream && need_tlist_eval ? result_plan->targetlist : tlist,
                        (List*)parse->havingQual,
                        numGroupCols,
                        groupColIdx,
                        extract_grouping_ops(parse->groupClause),
                        dNumGroups[0],
                        result_plan);
                    next_is_second_level_group = true;
                }

#ifdef STREAMPLAN
                if (needs_stream) {
                    if (next_is_second_level_group && contain_sets_expression) {
                        errno_t sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
                            NOTPLANSHIPPING_LENGTH,
                            "\"set-valued expression in qual/targetlist + two-level Groupagg\"");
                        securec_check_ss_c(sprintf_rc, "\0", "\0");
                        mark_stream_unsupport();
                    }

                    if (wflists != NULL && wflists->activeWindows)
                        result_plan->targetlist =
                            make_windowInputTargetList(root, result_plan->targetlist, wflists->activeWindows);

                    result_plan = (Plan*)mark_group_stream(root, tlist, result_plan);
                    current_pathkeys = root->group_pathkeys;
                }
#endif
            } else if (root->hasHavingQual || parse->groupingSets) {
                int nrows = list_length(parse->groupingSets);

                /*
                 * No aggregates, and no GROUP BY, but we have a HAVING qual
                 * or grouping sets (which by elimination of cases above must
                 * consist solely of empty grouping sets, since otherwise
                 * groupClause will be non-empty).
                 *
                 * This is a degenerate case in which we are supposed to emit
                 * either 0 or 1 row for each grouping set depending on
                 * whether HAVING succeeds.  Furthermore, there cannot be any
                 * variables in either HAVING or the targetlist, so we
                 * actually do not need the FROM table at all!	We can just
                 * throw away the plan-so-far and generate a Result node. This
                 * is a sufficiently unusual corner case that it's not worth
                 * contorting the structure of this routine to avoid having to
                 * generate the plan in the first place.
                 */
                result_plan = (Plan*)make_result(root, tlist, parse->havingQual, NULL);
                /*
                 * Doesn't seem worthwhile writing code to cons up a
                 * generate_series or a values scan to emit multiple rows.
                 * Instead just clone the result in an Append.
                 */
                if (nrows > 1) {
                    List* plans = list_make1(result_plan);

                    while (--nrows > 0)
                        plans = lappend(plans, copyObject(result_plan));

                    result_plan = (Plan*)make_append(plans, tlist);
                }
            }
#ifdef PGXC
            /*
             * Grouping will certainly not increase the number of rows
             * Coordinator fetches from Datanode, in fact it's expected to
             * reduce the number drastically. Hence, try pushing GROUP BY
             * clauses and aggregates to the Datanode, thus saving bandwidth.
             */
            if (IS_PGXC_COORDINATOR && !IsConnFromCoord() && !IS_STREAM)
                result_plan = create_remotegrouping_plan(root, result_plan);
#endif    /* PGXC */
        } /* end of non-minmax-aggregate case */

        /*
         * Since each window function could require a different sort order, we
         * stack up a WindowAgg node for each window, with sort steps between
         * them as needed.
         */
        if (wflists != NULL && wflists->activeWindows) {
            List* window_tlist = NIL;
            ListCell* l = NULL;

            /*
             * If the top-level plan node is one that cannot do expression
             * evaluation, we must insert a Result node to project the desired
             * tlist.  (In some cases this might not really be required, but
             * it's not worth trying to avoid it.)  Note that on second and
             * subsequent passes through the following loop, the top-level
             * node will be a WindowAgg which we know can project; so we only
             * need to check once.
             */
            if (!is_projection_capable_plan(result_plan)) {
                result_plan = (Plan*)make_result(root, NIL, NULL, result_plan);
            }

            /*
             * The "base" targetlist for all steps of the windowing process is
             * a flat tlist of all Vars and Aggs needed in the result.  (In
             * some cases we wouldn't need to propagate all of these all the
             * way to the top, since they might only be needed as inputs to
             * WindowFuncs.  It's probably not worth trying to optimize that
             * though.)  We also add window partitioning and sorting
             * expressions to the base tlist, to ensure they're computed only
             * once at the bottom of the stack (that's critical for volatile
             * functions).  As we climb up the stack, we'll add outputs for
             * the WindowFuncs computed at each level.
             */
            window_tlist = make_windowInputTargetList(root, tlist, wflists->activeWindows);

            /*
             * The copyObject steps here are needed to ensure that each plan
             * node has a separately modifiable tlist.  (XXX wouldn't a
             * shallow list copy do for that?)
             */
            if (window_tlist != NULL)
                result_plan->targetlist = (List*)copyObject(window_tlist);

            if (IsA(result_plan, PartIterator)) {
                /*
                 * If is a PartIterator + Scan, push the PartIterator's
                 * tlist to Scan.
                 *
                 * Notes  : In cstore schema, when window_tlist is NULL, we should make sure
                 *     the CStoreScan's targetlist not become NULL again. So only when the wondow_tlist
                 *     is not NULL, we do the copy. We can make this, becasuse when the CStoreScan's
                 *     targetlist is NULL, we choose the first column as the targetlist:create_cstore_plan.
                 */
                if (window_tlist != NULL)
                    result_plan->lefttree->targetlist = (List*)copyObject(window_tlist);
            }

            /* Set group_set and again for windows function. */
            rebuild_pathkey_for_groupingSet<windows_func_pathkey>(
                root, tlist, wflists->activeWindows, collectiveGroupExpr);

            foreach (l, wflists->activeWindows) {
                WindowClause* wc = (WindowClause*)lfirst(l);
                List* window_pathkeys = NIL;
                int partNumCols;
                AttrNumber* partColIdx = NULL;
                Oid* partOperators = NULL;
                int ordNumCols;
                AttrNumber* ordColIdx = NULL;
                Oid* ordOperators = NULL;

                window_pathkeys = make_pathkeys_for_window(root, wc, tlist, true);

                /*
                 * This is a bit tricky: we build a sort node even if we don't
                 * really have to sort.  Even when no explicit sort is needed,
                 * we need to have suitable resjunk items added to the input
                 * plan's tlist for any partitioning or ordering columns that
                 * aren't plain Vars.  (In theory, make_windowInputTargetList
                 * should have provided all such columns, but let's not assume
                 * that here.)  Furthermore, this way we can use existing
                 * infrastructure to identify which input columns are the
                 * interesting ones.
                 */
                if (window_pathkeys != NIL) {
                    Sort* sort_plan = NULL;

                    /*
                     * If the window func has 'partitin by',
                     * then we can parallelize it.
                     */
                    sort_plan =
                        make_sort_from_pathkeys(root, result_plan, window_pathkeys, -1.0, (wc->partitionClause != NIL));
                    if (!pathkeys_contained_in(window_pathkeys, current_pathkeys)) {
                        /* we do indeed need to sort */
                        result_plan = (Plan*)sort_plan;
                        current_pathkeys = window_pathkeys;
                    }
                    /* In either case, extract the per-column information */
                    get_column_info_for_window(root,
                        wc,
                        tlist,
                        sort_plan->numCols,
                        sort_plan->sortColIdx,
                        &partNumCols,
                        &partColIdx,
                        &partOperators,
                        &ordNumCols,
                        &ordColIdx,
                        &ordOperators);
                } else {
                    /* empty window specification, nothing to sort */
                    partNumCols = 0;
                    partColIdx = NULL;
                    partOperators = NULL;
                    ordNumCols = 0;
                    ordColIdx = NULL;
                    ordOperators = NULL;
                }

                if (lnext(l)) {
                    /* Add the current WindowFuncs to the running tlist */
                    window_tlist = add_to_flat_tlist(window_tlist, wflists->windowFuncs[wc->winref]);
                } else {
                    /* Install the original tlist in the topmost WindowAgg */
                    window_tlist = tlist;
                }

                /* ... and make the WindowAgg plan node */
                result_plan = (Plan*)make_windowagg(root,
                    (List*)copyObject(window_tlist),
                    wflists->windowFuncs[wc->winref],
                    wc->winref,
                    partNumCols,
                    partColIdx,
                    partOperators,
                    ordNumCols,
                    ordColIdx,
                    ordOperators,
                    wc->frameOptions,
                    wc->startOffset,
                    wc->endOffset,
                    result_plan);
#ifdef STREAMPLAN
                if (IS_STREAM_PLAN && is_execute_on_datanodes(result_plan) && !is_replicated_plan(result_plan)) {
                    result_plan = (Plan*)mark_windowagg_stream(root, result_plan, tlist, wc, current_pathkeys, wflists);
                }
#endif
            }
        }
        (void)MemoryContextSwitchTo(oldcontext);
    } /* end of if (setOperations) */

    oldcontext = MemoryContextSwitchTo(PlanGenerateContext);
    /*
     * If there is a DISTINCT clause, add the necessary node(s).
     */
    bool next_is_second_level_distinct = false; /* flag for DISTINCT agg */
    bool contain_sets_expression = expression_returns_set((Node*)tlist);

    if (parse->distinctClause) {
        double dNumDistinctRows[2];
        double numDistinctRows[2];

        /*
         * If there was grouping or aggregation, use the current number of
         * rows as the estimated number of DISTINCT rows (ie, assume the
         * result was already mostly unique).  If not, use the number of
         * distinct-groups calculated by query_planner.
         */
        if (parse->groupClause || parse->groupingSets || root->hasHavingQual || parse->hasAggs) {
            dNumDistinctRows[0] = PLAN_LOCAL_ROWS(result_plan);
            dNumDistinctRows[1] = result_plan->plan_rows;
        } else {
            dNumDistinctRows[0] = dNumGroups[0];
            dNumDistinctRows[1] = dNumGroups[1];
        }

        /* Also convert to long int --- but 'ware overflow! */
        numDistinctRows[0] = dNumDistinctRows[0];
        numDistinctRows[1] = dNumDistinctRows[1];

        /* Choose implementation method if we didn't already */
        if (!tested_hashed_distinct) {
            Size hashentrysize;

            /*
             * Don't do it if it doesn't look like the hashtable will fit into
             * work_mem.
             */
            if (root->glob->vectorized)
                hashentrysize = get_plan_actual_total_width(result_plan, root->glob->vectorized, OP_HASHAGG);
            else
                hashentrysize = get_hash_entry_size(result_plan->plan_width);

            /*
             * At this point, either hashed or sorted grouping will have to
             * work from result_plan, so we pass that as both "cheapest" and
             * "sorted".
             */
            use_hashed_distinct = choose_hashed_distinct(root,
                tuple_fraction,
                limit_tuples,
                PLAN_LOCAL_ROWS(result_plan),
                result_plan->plan_width,
                result_plan->startup_cost,
                result_plan->total_cost,
                ng_get_dest_distribution(result_plan),
                result_plan->startup_cost,
                result_plan->total_cost,
                ng_get_dest_distribution(result_plan),
                current_pathkeys,
                dNumDistinctRows[0],
                hashentrysize);
        }

        /* need to judge if we should redistribute according to distinct clause */
        if (IS_STREAM_PLAN) {
            if (is_execute_on_coordinator(result_plan) || is_execute_on_allnodes(result_plan) ||
                is_replicated_plan(result_plan))
                needs_stream = false;
            else {
                List* distinct_expr = get_sortgrouplist_exprs(parse->distinctClause, parse->targetList);

                needs_stream = needs_agg_stream(root, distinct_expr, result_plan->distributed_keys);
                list_free_ext(distinct_expr);
            }
#ifndef ENABLE_MULTIPLE_NODES
            needs_stream = needs_stream && (result_plan->dop > 1);
#endif
        }

        if (use_hashed_distinct) {
            Size hash_entry_size = MAXALIGN(result_plan->plan_width) + MAXALIGN(sizeof(MinimalTupleData));

            /* Hashed aggregate plan --- no sort needed */
            if (IS_STREAM_PLAN && is_execute_on_datanodes(result_plan) && !is_replicated_plan(result_plan)) {
                result_plan = generate_hashagg_plan(root,
                    result_plan,
                    result_plan->targetlist,
                    NULL,
                    list_length(parse->distinctClause),
                    numDistinctRows,
                    NULL,
                    NULL,
                    extract_grouping_ops(parse->distinctClause),
                    &needs_stream,
                    hash_entry_size,
                    DISTINCT_INTENT,
                    rel_info);
            } else {
                result_plan = (Plan*)make_agg(root,
                    result_plan->targetlist,
                    NIL,
                    AGG_HASHED,
                    NULL,
                    list_length(parse->distinctClause),
                    extract_grouping_cols(parse->distinctClause, result_plan->targetlist),
                    extract_grouping_ops(parse->distinctClause),
                    (long)Min(numDistinctRows[0], (double)LONG_MAX),
                    result_plan,
                    NULL,
                    false,
                    false,
                    NIL,
                    hash_entry_size);
            }
            next_is_second_level_distinct = true;

            if (IS_STREAM_PLAN && needs_stream && is_execute_on_datanodes(result_plan) &&
                !is_replicated_plan(result_plan)) {
                if (next_is_second_level_distinct && contain_sets_expression) {
                    errno_t sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
                        NOTPLANSHIPPING_LENGTH,
                        "\"set-valued expression in qual/targetlist + two-level distinct\"");
                    securec_check_ss_c(sprintf_rc, "\0", "\0");
                    mark_stream_unsupport();
                }

                result_plan = (Plan*)mark_agg_stream(
                    root, tlist, result_plan, parse->distinctClause, DISTINCT_INTENT, &has_second_agg_sort);
                if (!has_second_agg_sort)
                    current_pathkeys = NIL;
                else
                    current_pathkeys = root->group_pathkeys;
            } else /* Hashed aggregation produces randomly-ordered results */
                current_pathkeys = NIL;
        } else {

            /*
             * Set group_set and again build pathkeys, data's value can be altered groupingSet after,
             * so equal expr can not be deleted from pathkeys. Rebuild pathkey EquivalenceClass's ec_group_set
             * is true.
             */
            rebuild_pathkey_for_groupingSet<distinct_pathkey>(root, tlist, NULL, collectiveGroupExpr);

            /*
             * Use a Unique node to implement DISTINCT.  Add an explicit sort
             * if we couldn't make the path come out the way the Unique node
             * needs it.  If we do have to sort, always sort by the more
             * rigorous of DISTINCT and ORDER BY, to avoid a second sort
             * below.  However, for regular DISTINCT, don't sort now if we
             * don't have to --- sorting afterwards will likely be cheaper,
             * and also has the possibility of optimizing via LIMIT.  But for
             * DISTINCT ON, we *must* force the final sort now, else it won't
             * have the desired behavior.
             */
            List* needed_pathkeys = NIL;

            if (parse->hasDistinctOn && list_length(root->distinct_pathkeys) < list_length(root->sort_pathkeys))
                needed_pathkeys = root->sort_pathkeys;
            else
                needed_pathkeys = root->distinct_pathkeys;

            /* we also need to add sort if the sub node is parallized. */
            if (!pathkeys_contained_in(needed_pathkeys, current_pathkeys) ||
                (result_plan->dop > 1 && needed_pathkeys)) {
                if (list_length(root->distinct_pathkeys) >= list_length(root->sort_pathkeys))
                    current_pathkeys = root->distinct_pathkeys;
                else {
                    current_pathkeys = root->sort_pathkeys;
                    /* AssertEreport checks that parser didn't mess up... */
                    AssertEreport(pathkeys_contained_in(root->distinct_pathkeys, current_pathkeys),
                        MOD_OPT,
                        "the parser does not mess up when adding sort for pathkeys.");
                }

                result_plan = (Plan*)make_sort_from_pathkeys(root, result_plan, current_pathkeys, -1.0);
            }

            result_plan = (Plan*)make_unique(result_plan, parse->distinctClause);
            set_plan_rows(
                result_plan, get_global_rows(dNumDistinctRows[0], 1.0, ng_get_dest_num_data_nodes(result_plan)));

            /* The Unique node won't change sort ordering */
#ifdef STREAMPLAN
            if (IS_STREAM_PLAN && needs_stream && is_execute_on_datanodes(result_plan) &&
                !is_replicated_plan(result_plan)) {
                result_plan = (Plan*)mark_distinct_stream(
                    root, tlist, result_plan, parse->distinctClause, root->query_level, current_pathkeys);
            }
#endif
        }
    }

    /*
     * If there is a FOR [KEY] UPDATE/SHARE clause, add the LockRows node. (Note: we
     * intentionally test parse->rowMarks not root->rowMarks here. If there
     * are only non-locking rowmarks, they should be handled by the
     * ModifyTable node instead.)
     */
    if (parse->rowMarks) {
#ifdef ENABLE_MOT
        if (!IsMOTEngineUsed()) {
#endif
            result_plan = (Plan*)make_lockrows(root, result_plan);
#ifdef ENABLE_MOT
        }
#endif

        /*
         * The result can no longer be assumed sorted, since redistribute add
         * for lockrows may cause the data unsorted.
         */
#ifndef PGXC
        current_pathkeys = NIL;
#endif
    }

    /*
     * If ORDER BY was given and we were not able to make the plan come out in
     * the right order, add an explicit sort step.
     */
    if (parse->sortClause) {
        /*
         * Set group_set and again build pathkeys, data's value can be altered groupingSet after,
         * so equal expr can not be deleted from pathkeys. Rebuild pathkey EquivalenceClass's ec_group_set
         * is true.
         */
        rebuild_pathkey_for_groupingSet<sort_pathkey>(root, tlist, NULL, collectiveGroupExpr);

        /* we also need to add sort if the sub node is parallized. */
        if (!pathkeys_contained_in(root->sort_pathkeys, current_pathkeys) ||
            (result_plan->dop > 1 && root->sort_pathkeys)) {
            result_plan = (Plan*)make_sort_from_pathkeys(root, result_plan, root->sort_pathkeys, limit_tuples);
#ifdef PGXC
#ifdef STREAMPLAN
            if (IS_STREAM_PLAN && check_sort_for_upsert(root))
                result_plan = make_stream_sort(root, result_plan);
#endif /* STREAMPLAN */
            if (IS_PGXC_COORDINATOR && !IS_STREAM && !IsConnFromCoord())
                result_plan = (Plan*)create_remotesort_plan(root, result_plan);
#endif /* PGXC */
            current_pathkeys = root->sort_pathkeys;
        }
    }

    /*
     * Finally, if there is a LIMIT/OFFSET clause, add the LIMIT node.
     */
    if (parse->limitCount || parse->limitOffset) {
#ifdef STREAMPLAN
        if (IS_STREAM_PLAN) {
            bool needs_sort = !pathkeys_contained_in(root->sort_pathkeys, current_pathkeys);

            result_plan = (Plan*)make_stream_limit(root,
                result_plan,
                parse->limitOffset,
                parse->limitCount,
                offset_est,
                count_est,
                limit_tuples,
                needs_sort);
            if (needs_sort)
                current_pathkeys = root->sort_pathkeys;
        } else
#endif
            result_plan =
                (Plan*)make_limit(root, result_plan, parse->limitOffset, parse->limitCount, offset_est, count_est);
#ifdef PGXC
        /* See if we can push LIMIT or OFFSET clauses to Datanodes */
        if (IS_PGXC_COORDINATOR && !IsConnFromCoord() && !IS_STREAM)
            result_plan = (Plan*)create_remotelimit_plan(root, result_plan);
#endif /* PGXC */
    }

#ifdef STREAMPLAN
    /*
     * Add Remote Query for stream plan at the end.
     * Don't add gather for non-select statement.
     */
    if (IS_STREAM_PLAN && (!IsConnFromCoord()) && root->query_level == 1 && (parse->commandType == CMD_SELECT) &&
        is_execute_on_datanodes(result_plan)) {

        bool single_node = (result_plan->exec_nodes != NULL && list_length(result_plan->exec_nodes->nodeList) == 1);

        result_plan = make_simple_RemoteQuery(result_plan, root, false);

        /*
         * if result plan is a simple remote query, and we got a sort pathkey in the plan
         * we can deduct that we have already add sort in the data node, so we only need
         * add a mergesort to remote query if there are multiple dn involved.
         */
        if ((IsA(result_plan, RemoteQuery) || IsA(result_plan, Stream))&&
            !is_replicated_plan(result_plan->lefttree) && !single_node &&
            root->sort_pathkeys != NIL && pathkeys_contained_in(root->sort_pathkeys, current_pathkeys)) {
            Sort* sortPlan = make_sort_from_pathkeys(root, result_plan, current_pathkeys, limit_tuples);

            SimpleSort* streamSort = makeNode(SimpleSort);
            streamSort->numCols = sortPlan->numCols;
            streamSort->sortColIdx = sortPlan->sortColIdx;
            streamSort->sortOperators = sortPlan->sortOperators;
            streamSort->nullsFirst = sortPlan->nullsFirst;
            streamSort->sortToStore = false;
            streamSort->sortCollations = sortPlan->collations;

            if (IsA(result_plan, RemoteQuery)) {
                ((RemoteQuery*)result_plan)->sort = streamSort;
            } else if (IsA(result_plan, Stream)) {
                ((Stream*)result_plan)->sort = streamSort;
            }
        }
    }

    /*
     * Return the actual output ordering in query_pathkeys for possible use by
     * an outer query level.
     */
    root->query_pathkeys = current_pathkeys;
#endif

#ifdef ENABLE_MULTIPLE_NODES
    if (g_instance.attr.attr_common.enable_tsdb) {
        result_plan = tsdb_modifier(root, tlist, result_plan);
    } else if (has_ts_func(tlist)) {
        ereport(ERROR,
            (errmodule(MOD_OPT_PLANNER), errcode(ERRCODE_INVALID_OPERATION),
                errmsg("TSDB functions cannot be used if enable_tsdb is off."),
                errdetail("N/A"),
                errcause("Functions are not loaded."),
                erraction("Turn on enable_tsdb according to manual.")));
    }
#endif
    (void)MemoryContextSwitchTo(oldcontext);

    return result_plan;
}

/*
 * Given a groupclause for a collection of grouping sets, produce the
 * corresponding groupColIdx.
 *
 * root->grouping_map maps the tleSortGroupRef to the actual column position in
 * the input tuple. So we get the ref from the entries in the groupclause and
 * look them up there.
 */
static AttrNumber* remap_groupColIdx(PlannerInfo* root, List* groupClause)
{
    AttrNumber* grouping_map = root->grouping_map;
    AttrNumber* new_grpColIdx = NULL;
    ListCell* lc = NULL;
    int i;

    AssertEreport(grouping_map != NULL, MOD_OPT, "invalid grouping map when generating the corresponding groupColIdx.");

    if (list_length(groupClause) > 0) {
        new_grpColIdx = (AttrNumber*)palloc0(sizeof(AttrNumber) * list_length(groupClause));

        i = 0;
        foreach (lc, groupClause) {
            SortGroupClause* clause = (SortGroupClause*)lfirst(lc);

            new_grpColIdx[i++] = grouping_map[clause->tleSortGroupRef];
        }
    } else {
        new_grpColIdx = NULL;
    }

    return new_grpColIdx;
}

/* Judge if have distribute keys for groupingsets */
static List* get_group_expr(List* sortrefList, List* tlist)
{
    List* group_list = NULL;
    ListCell* lc = NULL;
    foreach (lc, sortrefList) {
        int i = (int)lfirst_int(lc);
        TargetEntry* tle = get_sortgroupref_tle(i, tlist);
        group_list = lappend(group_list, tle->expr);
    }

    return group_list;
}

/*
 * @Description: set aggref's distinct to NULL
 * @in tlist - query's target list.
 * @in havingQual - query having clause.
 */
static void set_distinct_to_null(List* tlist, List* havingQual)
{
    List* tlist_var_agg = NULL;
    List* havin_var_agg = NULL;
    List* list_var_agg = NULL;

    tlist_var_agg = pull_var_clause((Node*)tlist, PVC_INCLUDE_AGGREGATES, PVC_INCLUDE_PLACEHOLDERS);

    havin_var_agg = pull_var_clause((Node*)havingQual, PVC_INCLUDE_AGGREGATES, PVC_INCLUDE_PLACEHOLDERS);

    list_var_agg = list_concat(tlist_var_agg, havin_var_agg);

    ListCell* lc = NULL;

    /* Set distinct to NULL, in this function, distinct_node only have one. */
    foreach (lc, list_var_agg) {
        Node* node = (Node*)lfirst(lc);

        if (IsA(node, Aggref) && ((Aggref*)node)->aggdistinct) {
            Aggref* tmp_agg = (Aggref*)node;

            list_free_deep(tmp_agg->aggdistinct);
            tmp_agg->aggdistinct = NULL;
        }
    }
}

/*
 * @Description: Build Agg and Sort nodes to implement sorted grouping with one or more
 * grouping sets. (A plain GROUP BY or just the presence of aggregates counts
 * for this purpose as a single grouping set; the calling code is responsible
 * for providing a non-empty rollup_groupclauses list for such cases, though
 * rollup_lists may be null.)
 *
 * The last entry in rollup_groupclauses (which is the one the input is sorted
 * on, if at all) is the one used for the returned Agg node. Any additional
 * rollups are attached, with corresponding sort info, to subsidiary Agg and
 * Sort nodes attached to the side of the real Agg node; these nodes don't
 * participate in the plan directly, but they are both a convenient way to
 * represent the required data and a convenient way to account for the costs
 * of execution.
 * @in root - Per-query information for planning/optimization.
 * @in parse - Query tree.
 * @in tlist - targetlist.
 * @in need_sort_for_grouping - If we need a Sort operation on the input.
 * @in rollup_groupclauses - is a list of grouping clauses for grouping sets
 * @in rollup_lists -  is a list of grouping sets
 * @in groupColIdx - group column  idx
 * @in agg_costs - agg costs
 * @in numGroups - is the estimated number of groups
 * @in result_plan - left plan
 * @in wflists - windows fun info.
 * @in need_stream - if need stream
 * @out - agg plan
 */
static Plan* build_grouping_chain(PlannerInfo* root, Query* parse, List** tlist, bool need_sort_for_grouping,
    List* rollup_groupclauses, List* rollup_lists, AttrNumber* groupColIdx, AggClauseCosts* agg_costs, long numGroups,
    Plan* result_plan, WindowLists* wflists, bool need_stream)
{
    AttrNumber* top_grpColIdx = groupColIdx;
    List* chain = NIL;
    List* newTlist = *tlist;

    /*
     * Prepare the grpColIdx for the real Agg node first, because we may need
     * it for sorting
     */
    if (parse->groupingSets) {
        top_grpColIdx = remap_groupColIdx(root, (List*)llast(rollup_groupclauses));
    }

    /* Need hashagg above sort+group */
    if (need_stream) {
        /* Append group by expr to targelists of sort agg, because redistribute node and hashagg node will used it */
        newTlist = add_groupingIdExpr_to_tlist(*tlist);
    } else if (result_plan->dop > 1) {
        result_plan = create_local_redistribute(root, result_plan, result_plan->distributed_keys, 0);
    }

    /* If we need a Sort operation on the input, generate that. */
    if (need_sort_for_grouping) {
        result_plan =
            (Plan*)make_sort_from_groupcols(root, (List*)llast(rollup_groupclauses), top_grpColIdx, result_plan);
    }

    /*
     * Generate the side nodes that describe the other sort and group
     * operations besides the top one.
     */
    while (list_length(rollup_groupclauses) > 1) {
        List* groupClause = (List*)linitial(rollup_groupclauses);
        List* gsets = (List*)linitial(rollup_lists);
        AttrNumber* new_grpColIdx = NULL;
        Plan* sort_plan = NULL;
        Plan* agg_plan = NULL;

        AssertEreport(groupClause != NIL,
            MOD_OPT,
            "invalid group clause when generating the side nodes that describe the other sort"
            "and group operations besides the top one.");
        AssertEreport(gsets != NIL,
            MOD_OPT,
            "invalid gsets when generating the side nodes that describe the other sort"
            "and group operations besides the top one.");

        new_grpColIdx = remap_groupColIdx(root, groupClause);

        sort_plan = (Plan*)make_sort_from_groupcols(root, groupClause, new_grpColIdx, result_plan);

        /*
         * sort_plan includes the cost of result_plan over again, which is not
         * what we want (since it's not actually running that plan). So
         * correct the cost figures.
         */
        sort_plan->startup_cost -= result_plan->total_cost;
        sort_plan->total_cost -= result_plan->total_cost;

        agg_plan = (Plan*)make_agg(root,
            *tlist,
            (List*)parse->havingQual,
            AGG_SORTED,
            agg_costs,
            list_length((List*)linitial(gsets)),
            new_grpColIdx,
            extract_grouping_ops(groupClause),
            numGroups,
            sort_plan,
            wflists,
            false,
            false,
            gsets);

        sort_plan->lefttree = NULL;

        chain = (List*)lappend(chain, agg_plan);

        if (rollup_lists != NULL)
            rollup_lists = list_delete_first(rollup_lists);

        rollup_groupclauses = list_delete_first(rollup_groupclauses);
    }

    /*
     * Now make the final Agg node
     */
    {
        List* groupClause = (List*)linitial(rollup_groupclauses);

        List* gsets = rollup_lists ? (List*)linitial(rollup_lists) : NIL;

        int numGroupCols;
        ListCell* lc = NULL;

        if (gsets != NULL)
            numGroupCols = list_length((List*)linitial(gsets));
        else
            numGroupCols = list_length(parse->groupClause);

        result_plan = (Plan*)make_agg(root,
            newTlist,
            (List*)parse->havingQual,
            (numGroupCols > 0) ? AGG_SORTED : AGG_PLAIN,
            agg_costs,
            numGroupCols,
            top_grpColIdx,
            extract_grouping_ops(groupClause),
            numGroups,
            result_plan,
            wflists,
            need_stream,
            !need_stream,
            gsets,
            0,
            true);

        if (wflists != NULL && wflists->activeWindows) {
            result_plan->targetlist = make_windowInputTargetList(root, result_plan->targetlist, wflists->activeWindows);
        }

        ((Agg*)result_plan)->chain = chain;

        if (need_stream) {
            result_plan->distributed_keys = NIL;
        }

        /*
         * Add the additional costs. But only the total costs count, since the
         * additional sorts aren't run on startup.
         */
        foreach (lc, chain) {
            Plan* subplan = (Plan*)lfirst(lc);

            result_plan->total_cost += subplan->total_cost;

            /*
             * Nuke stuff we don't need to avoid bloating debug output.
             */
            subplan->targetlist = NIL;
            subplan->qual = NIL;
            subplan->lefttree->targetlist = NIL;
        }
    }
    if (newTlist != *tlist)
        *tlist = newTlist;

    return result_plan;
}

/*
 * @Description: Generate stream redistribute plan to distinct if need; Build sort agg plain for Ap function;
 * Generate new group clause to hash agg.
 * @in root - Per-query information for planning/optimization.
 * @in parse - Query tree.
 * @in tlist - targetlist.
 * @in need_sort_for_grouping - If we need a Sort operation on the input.
 * @in rollup_groupclauses - is a list of grouping clauses for grouping sets
 * @in rollup_lists -  is a list of grouping sets
 * @in groupColIdx - group column  idx
 * @in agg_costs - agg costs
 * @in numGroups - is the estimated number of groups
 * @in result_plan - left plan
 * @in wflists - windows fun info.
 * @in need_stream - if need stream
 * @out needSecondLevelAgg  - if need second level agg
 * @in collectiveGroupExpr - collective group exprs
 * @out - agg plan
 */
static Plan* build_groupingsets_plan(PlannerInfo* root, Query* parse, List** tlist, bool need_sort_for_grouping,
    List* rollup_groupclauses, List* rollup_lists, AttrNumber** groupColIdx, AggClauseCosts* agg_costs, long numGroups,
    Plan* result_plan, WindowLists* wflists, bool* need_hash, List* collectiveGroupExpr)
{
    bool hasDistinct = false;
    bool need_stream = false;
    bool stream_plan = IS_STREAM_PLAN && is_execute_on_datanodes(result_plan) && !is_replicated_plan(result_plan);

#ifndef ENABLE_MULTIPLE_NODES
    if (result_plan->dop == 1) {
        stream_plan = false;
        *need_hash = false;
    }
#endif

    /* We need add redistribute for distinct */
    if (stream_plan) {
        /* Judge above sort agg if need stream and hashagg */
        need_stream = needs_agg_stream(root, collectiveGroupExpr, result_plan->distributed_keys);
#ifndef ENABLE_MULTIPLE_NODES
        need_stream = need_stream && (result_plan->dop > 1);
#endif

        if (list_length(agg_costs->exprAggs) == 1 && !agg_costs->hasdctDnAggs && !agg_costs->hasDnAggs) {
            double multiple;
            Node* distinct_node = (Node*)linitial(agg_costs->exprAggs);
            List* distinct_node_list = list_make1(distinct_node);
            bool need_redis = needs_agg_stream(root, distinct_node_list, result_plan->distributed_keys);

            if (need_redis) {
                distinct_node_list =
                    get_distributekey_from_tlist(root, NIL, distinct_node_list, result_plan->plan_rows, &multiple);
                if (distinct_node_list != NIL) {
                    result_plan = make_redistribute_for_agg(root, result_plan, distinct_node_list, 0);
                    need_stream = needs_agg_stream(root, collectiveGroupExpr, result_plan->distributed_keys);
                } else {
                    errno_t sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
                        NOTPLANSHIPPING_LENGTH,
                        "\"Count(Distinct)\" on redistribution unsupported data type");
                    securec_check_ss_c(sprintf_rc, "\0", "\0");
                    mark_stream_unsupport();
                }
            }

            /*
             * Distinct will be compute complete in sort+group operator.
             * Upper levels hash agg can not build agg(distinct) plan, here need set exprAggs to null and set
             * hasDistinct which will be used under code to true.
             */
            hasDistinct = true;
            agg_costs->exprAggs = NIL;
        } else if (need_stream && (agg_costs->exprAggs != NIL || agg_costs->hasDnAggs)) { /* Array agg */
            double multiple;
            List* distinct_node_list =
                get_distributekey_from_tlist(root, NIL, collectiveGroupExpr, result_plan->plan_rows, &multiple);
            if (distinct_node_list != NIL) {
                result_plan = make_redistribute_for_agg(root, result_plan, distinct_node_list, 0);
                need_stream = false;
            } else {
                errno_t sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
                    NOTPLANSHIPPING_LENGTH,
                    "\"String_agg\" or \"Array_agg\" or \"Listagg\" + \"Grouping sets\"");
                securec_check_ss_c(sprintf_rc, "\0", "\0");
                mark_stream_unsupport();
            }
        }
    }

    result_plan = build_grouping_chain(root,
        parse,
        tlist,
        need_sort_for_grouping,
        rollup_groupclauses,
        rollup_lists,
        *groupColIdx,
        agg_costs,
        numGroups,
        result_plan,
        wflists,
        need_stream);

    if (stream_plan) {
        /* This case hash agg not need */
        if (result_plan->distributed_keys) {
            *need_hash = false;
            /*
             * Alter this plan's distribute keys, current distribute keys value can be altered
             * grouping set after.
             * For example:
             * create table t1(location_id integer )distribute by hash(location_id);
             * create table t2(item_id varchar(20), location_id integer)distribute by hash(item_id);

             * select loc.location_id as c1, ale.location_id as c2
             * from t1 as loc, t1 ale
             * where c2 = c1
             * group by c2, grouping sets(c1, c2);
             *
             * Plan:
             *
             * Group By Key: ale.location_id, loc.location_id
             * Group By Key: ale.location_id
             *
             * c1 will be set to NULL grouping sets after, but c2 will keep primary values because it
             * in all group clauses. so need set distribute keys of plan to c2 replace of c1.
             */
            adjust_plan_dis_key(root, result_plan, collectiveGroupExpr);
        }

        /* This case mean we need add hashagg nodes. */
        if (result_plan->distributed_keys == NIL) {
            /*
             * Distinct has be compute complete in result_plan therefore
             * upper levels hashagg can not need consider distinct questions, it only need count to results of distinct.
             * we need will aggdistinct set to NULL.
             */
            if (hasDistinct) {
                /*
                 * Set agg's distinct to NULL. In this case, agg(distinct) already be computed in leftnode,
                 * upper levels node only need do agg.
                 */
                set_distinct_to_null(*tlist, result_plan->qual);

                /* we need adjust sort_pathkeys etc content, otherwise possible find fail */
                adjust_all_pathkeys_by_agg_tlist(root, *tlist, wflists);
            }

            /* GroupClause of query and grouping expr as hashagg groupclause */
            parse->groupClause = add_groupId_to_groupExpr(parse->groupClause, *tlist);

            /* Keep group keys */
            *groupColIdx = (AttrNumber*)palloc0(sizeof(AttrNumber) * list_length(parse->groupClause));

            locate_grouping_columns(root, *tlist, result_plan->targetlist, *groupColIdx);
        }
    }

    return result_plan;
}

/*
 * add_tlist_costs_to_plan
 *
 * Estimate the execution costs associated with evaluating the targetlist
 * expressions, and add them to the cost estimates for the Plan node.
 *
 * If the tlist contains set-returning functions, also inflate the Plan's cost
 * and plan_rows estimates accordingly.  (Hence, this must be called *after*
 * any logic that uses plan_rows to, eg, estimate qual evaluation costs.)
 *
 * Note: during initial stages of planning, we mostly consider plan nodes with
 * "flat" tlists, containing just Vars.  So their evaluation cost is zero
 * according to the model used by cost_qual_eval() (or if you prefer, the cost
 * is factored into cpu_tuple_cost).  Thus we can avoid accounting for tlist
 * cost throughout query_planner() and subroutines.  But once we apply a
 * tlist that might contain actual operators, sub-selects, etc, we'd better
 * account for its cost.  Any set-returning functions in the tlist must also
 * affect the estimated rowcount.
 *
 * Once grouping_planner() has applied a general tlist to the topmost
 * scan/join plan node, any tlist eval cost for added-on nodes should be
 * accounted for as we create those nodes.  Presently, of the node types we
 * can add on later, only Agg, WindowAgg, and Group project new tlists (the
 * rest just copy their input tuples) --- so make_agg(), make_windowagg() and
 * make_group() are responsible for calling this function to account for their
 * tlist costs.
 */
void add_tlist_costs_to_plan(PlannerInfo* root, Plan* plan, List* tlist)
{
    QualCost tlist_cost;
    double tlist_rows;

    cost_qual_eval(&tlist_cost, tlist, root);
    plan->startup_cost += tlist_cost.startup;
    plan->total_cost += tlist_cost.startup + tlist_cost.per_tuple * PLAN_LOCAL_ROWS(plan);

    tlist_rows = tlist_returns_set_rows(tlist);
    if (tlist_rows > 1) {
        /*
         * We assume that execution costs of the tlist proper were all
         * accounted for by cost_qual_eval.  However, it still seems
         * appropriate to charge something more for the executor's general
         * costs of processing the added tuples.  The cost is probably less
         * than cpu_tuple_cost, though, so we arbitrarily use half of that.
         */
        plan->total_cost += PLAN_LOCAL_ROWS(plan) * (tlist_rows - 1) * u_sess->attr.attr_sql.cpu_tuple_cost / 2;

        plan->plan_rows *= tlist_rows;
    }
}

/*
 * Detect whether a plan node is a "dummy" plan created when a relation
 * is deemed not to need scanning due to constraint exclusion.
 *
 * Currently, such dummy plans are Result nodes with constant FALSE
 * filter quals (see set_dummy_rel_pathlist and create_append_plan).
 *
 * XXX this probably ought to be somewhere else, but not clear where.
 */
bool is_dummy_plan(Plan* plan)
{
    if (IsA(plan, BaseResult)) {
        List* rcqual = (List*)((BaseResult*)plan)->resconstantqual;

        if (list_length(rcqual) == 1) {
            Const* constqual = (Const*)linitial(rcqual);

            if (constqual && IsA(constqual, Const)) {
                if (!constqual->constisnull && !DatumGetBool(constqual->constvalue))
                    return true;
            }
        }
    }
    return false;
}

/*
 * Create a bitmapset of the RT indexes of live base relations
 *
 * Helper for preprocess_rowmarks ... at this point in the proceedings,
 * the only good way to distinguish baserels from appendrel children
 * is to see what is in the join tree.
 */
Bitmapset* get_base_rel_indexes(Node* jtnode)
{
    Bitmapset* result = NULL;

    if (jtnode == NULL)
        return NULL;
    if (IsA(jtnode, RangeTblRef)) {
        int varno = ((RangeTblRef*)jtnode)->rtindex;

        result = bms_make_singleton(varno);
    } else if (IsA(jtnode, FromExpr)) {
        FromExpr* f = (FromExpr*)jtnode;
        ListCell* l = NULL;

        result = NULL;
        foreach (l, f->fromlist)
            result = bms_join(result, get_base_rel_indexes((Node*)lfirst(l)));
    } else if (IsA(jtnode, JoinExpr)) {
        JoinExpr* j = (JoinExpr*)jtnode;

        result = bms_join(get_base_rel_indexes(j->larg), get_base_rel_indexes(j->rarg));
    } else {
        ereport(ERROR,
            (errmodule(MOD_OPT_PLANNER), errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("Unrecognized node type when extracting index."),
                errdetail("Node Type: %d", (int)nodeTag(jtnode)),
                errcause("System error."),
                erraction("Contact Huawei Engineer.")));
        result = NULL; /* keep compiler quiet */
    }
    return result;
}

/*
 * preprocess_rowmarks - set up PlanRowMarks if needed
 */
static void preprocess_rowmarks(PlannerInfo* root)
{
    Query* parse = root->parse;
    Bitmapset* rels = NULL;
    List* prowmarks = NIL;
    ListCell* l = NULL;
    int i;

    if (parse->rowMarks) {
        /*
         * We've got trouble if FOR [KEY] UPDATE/SHARE appears inside grouping,
         * since grouping renders a reference to individual tuple CTIDs
         * invalid.  This is also checked at parse time, but that's
         * insufficient because of rule substitution, query pullup, etc.
         */
        CheckSelectLocking(parse);
    } else {
        /*
         * We only need rowmarks for UPDATE, DELETE, MEREG INTO, or FOR [KEY] UPDATE/SHARE.
         */
        if (parse->commandType != CMD_UPDATE && parse->commandType != CMD_DELETE &&
            (parse->commandType != CMD_MERGE || (u_sess->opt_cxt.is_stream == false && IS_SINGLE_NODE == false)))
            return;
    }

    /*
     * We need to have rowmarks for all base relations except the target. We
     * make a bitmapset of all base rels and then remove the items we don't
     * need or have FOR [KEY] UPDATE/SHARE marks for.
     */
    rels = get_base_rel_indexes((Node*)parse->jointree);
    if (parse->resultRelation)
        rels = bms_del_member(rels, parse->resultRelation);

    /*
     * Convert RowMarkClauses to PlanRowMark representation.
     */
    prowmarks = NIL;
    foreach (l, parse->rowMarks) {
        RowMarkClause* rc = (RowMarkClause*)lfirst(l);
        RangeTblEntry* rte = rt_fetch(rc->rti, parse->rtable);
        PlanRowMark* newrc = NULL;

        /*
         * Currently, it is syntactically impossible to have FOR UPDATE
         * applied to an update/delete target rel.	If that ever becomes
         * possible, we should drop the target from the PlanRowMark list.
         */
        AssertEreport(rc->rti != (uint)parse->resultRelation,
            MOD_OPT,
            "invalid range table index when converting RowMarkClauses to PlanRowMark representation.");

        /*
         * Ignore RowMarkClauses for subqueries; they aren't real tables and
         * can't support true locking.  Subqueries that got flattened into the
         * main query should be ignored completely.  Any that didn't will get
         * ROW_MARK_COPY items in the next loop.
         */
        if (rte->rtekind != RTE_RELATION)
            continue;

        /*
         * Similarly, ignore RowMarkClauses for foreign tables; foreign tables
         * will instead get ROW_MARK_COPY items in the next loop.  (FDWs might
         * choose to do something special while fetching their rows, but that
         * is of no concern here.)
         */
        if (rte->relkind == RELKIND_FOREIGN_TABLE || rte->relkind == RELKIND_STREAM)
            continue;

        rels = bms_del_member(rels, rc->rti);

        newrc = makeNode(PlanRowMark);
        newrc->rti = newrc->prti = rc->rti;
        newrc->rowmarkId = ++(root->glob->lastRowMarkId);
        /* The strength of lc is not set at old version and distribution. Set it according to forUpdate. */
        if (t_thrd.proc->workingVersionNum < ENHANCED_TUPLE_LOCK_VERSION_NUM
#ifdef ENABLE_MULTIPLE_NODES
            || true
#endif
            ) {
            rc->strength = rc->forUpdate ? LCS_FORUPDATE : LCS_FORSHARE;
        }
        switch (rc->strength) {
            case LCS_FORUPDATE:
                newrc->markType = ROW_MARK_EXCLUSIVE;
                break;
            case LCS_FORNOKEYUPDATE:
                newrc->markType = ROW_MARK_NOKEYEXCLUSIVE;
                break;
            case LCS_FORSHARE:
                newrc->markType = ROW_MARK_SHARE;
                break;
            case LCS_FORKEYSHARE:
                newrc->markType = ROW_MARK_KEYSHARE;
                break;
            default:
                ereport(ERROR, (errmsg("unknown lock type: %d", rc->strength)));
                break;
        }
        newrc->noWait = rc->noWait;
        newrc->waitSec = rc->waitSec;
        newrc->isParent = false;
        newrc->bms_nodeids = ng_get_baserel_data_nodeids(rte->relid, rte->relkind);

        prowmarks = lappend(prowmarks, newrc);
    }

    /*
     * Now, add rowmarks for any non-target, non-locked base relations.
     */
    i = 0;
    foreach (l, parse->rtable) {
        RangeTblEntry* rte = (RangeTblEntry*)lfirst(l);
        PlanRowMark* newrc = NULL;

        i++;
        if (!bms_is_member(i, rels))
            continue;

        newrc = makeNode(PlanRowMark);
        newrc->rti = newrc->prti = i;
        newrc->rowmarkId = ++(root->glob->lastRowMarkId);
        /* real tables support REFERENCE, anything else needs COPY */
        if (rte->rtekind == RTE_RELATION && rte->relkind != RELKIND_FOREIGN_TABLE && rte->relkind != RELKIND_STREAM)
            newrc->markType = ROW_MARK_REFERENCE;
        else
            newrc->markType = ROW_MARK_COPY;
        newrc->noWait = false; /* doesn't matter */
        newrc->waitSec = 0;
        newrc->isParent = false;
        newrc->bms_nodeids = (RTE_RELATION == rte->rtekind && RELKIND_FOREIGN_TABLE != rte->relkind 
                              && RELKIND_STREAM != rte->relkind)
                                 ? ng_get_baserel_data_nodeids(rte->relid, rte->relkind)
                                 : NULL;

        prowmarks = lappend(prowmarks, newrc);
    }

    root->rowMarks = prowmarks;
}

#ifdef PGXC
/*
 * separate_rowmarks - In XC Coordinators are supposed to skip handling
 *                of type ROW_MARK_EXCLUSIVE & ROW_MARK_SHARE.
 *                In order to do that we simply remove such type
 *                of row marks from the list. Instead they are saved
 *                in another list that is then handeled to add
 *                FOR UPDATE/SHARE in the remote query
 *                in the function create_remotequery_plan
 */
static void separate_rowmarks(PlannerInfo* root)
{
    List* rml_1 = NULL;
    List* rml_2 = NULL;
    ListCell* rm = NULL;

    if (IS_PGXC_DATANODE || IsConnFromCoord() || root->rowMarks == NULL)
        return;

    foreach (rm, root->rowMarks) {
        PlanRowMark* prm = (PlanRowMark*)lfirst(rm);

        if (prm->markType == ROW_MARK_EXCLUSIVE || prm->markType == ROW_MARK_SHARE)
            rml_1 = lappend(rml_1, prm);
        else
            rml_2 = lappend(rml_2, prm);
    }
    list_free_ext(root->rowMarks);
    root->rowMarks = rml_2;
    root->xc_rowMarks = rml_1;
}

#endif /*PGXC*/

/*
 * Try to obtain the clause values.  We use estimate_expression_value
 * primarily because it can sometimes do something useful with Params.
 */
static void estimate_limit_offset_count(PlannerInfo* root, int64* offset_est, int64* count_est)
{
    Query* parse = root->parse;
    Node* est = NULL;

    /* Should not be called unless LIMIT or OFFSET */
    AssertEreport(parse->limitCount || parse->limitOffset,
        MOD_OPT,
        "invalid result tuples when doing pre-estimation for LIMIT and/or OFFSET clauses.");
    
    if (parse->limitCount) {
        est = estimate_expression_value(root, parse->limitCount);
        if (est && IsA(est, Const)) {
            if (((Const*)est)->constisnull) {
                /* NULL indicates LIMIT ALL, ie, no limit */
                *count_est = 0; /* treat as not present */
            } else {
                *count_est = DatumGetInt64(((Const*)est)->constvalue);
                if (*count_est <= 0) {
                    *count_est = 1; /* force to at least 1 */
                }
            }
        } else {
            *count_est = -1; /* can't estimate */
        }
    } else {
        *count_est = 0; /* not present */
    }

    if (parse->limitOffset) {
        est = estimate_expression_value(root, parse->limitOffset);
        if (est && IsA(est, Const)) {
            if (((Const*)est)->constisnull) {
                /* Treat NULL as no offset; the executor will too */
                *offset_est = 0; /* treat as not present */
            } else {
                *offset_est = DatumGetInt64(((Const*)est)->constvalue);
                if (*offset_est < 0) {
                    *offset_est = 0; /* less than 0 is same as 0 */
                }
            }
        } else {
            *offset_est = -1; /* can't estimate */
        }
    } else {
        *offset_est = 0; /* not present */
    }
}

/*
 * preprocess_limit - do pre-estimation for LIMIT and/or OFFSET clauses
 *
 * We try to estimate the values of the LIMIT/OFFSET clauses, and pass the
 * results back in *count_est and *offset_est.	These variables are set to
 * 0 if the corresponding clause is not present, and -1 if it's present
 * but we couldn't estimate the value for it.  (The "0" convention is OK
 * for OFFSET but a little bit bogus for LIMIT: effectively we estimate
 * LIMIT 0 as though it were LIMIT 1.  But this is in line with the planner's
 * usual practice of never estimating less than one row.)  These values will
 * be passed to make_limit, which see if you change this code.
 *
 * The return value is the suitably adjusted tuple_fraction to use for
 * planning the query.	This adjustment is not overridable, since it reflects
 * plan actions that grouping_planner() will certainly take, not assumptions
 * about context.
 */
static double preprocess_limit(PlannerInfo* root, double tuple_fraction, int64* offset_est, int64* count_est)
{
    double limit_fraction;

    estimate_limit_offset_count(root, offset_est, count_est);

    if (*count_est != 0) {
        /*
         * A LIMIT clause limits the absolute number of tuples returned.
         * However, if it's not a constant LIMIT then we have to guess; for
         * lack of a better idea, assume 10% of the plan's result is wanted.
         */
        if (*count_est < 0 || *offset_est < 0) {
            /* LIMIT or OFFSET is an expression ... punt ... */
            limit_fraction = 0.10;
        } else {
            /* LIMIT (plus OFFSET, if any) is max number of tuples needed */
            limit_fraction = (double)*count_est + (double)*offset_est;
        }

        /*
         * If we have absolute limits from both caller and LIMIT, use the
         * smaller value; likewise if they are both fractional.  If one is
         * fractional and the other absolute, we can't easily determine which
         * is smaller, but we use the heuristic that the absolute will usually
         * be smaller.
         */
        if (tuple_fraction >= 1.0) {
            /*
             * if true, both absolute
             * else, caller absolute, limit fractional; use caller's value
             */
            tuple_fraction = limit_fraction >= 1.0 ?
                Min(tuple_fraction, limit_fraction) : tuple_fraction;
        } else if (tuple_fraction > 0.0) {
            /*
             * if true, caller fractional, limit absolute; use limit
             * else, both fractional
             */
            tuple_fraction = limit_fraction >= 1.0 ?
                limit_fraction : Min(tuple_fraction, limit_fraction);
        } else {
            /* no info from caller, just use limit */
            tuple_fraction = limit_fraction;
        }
    } else if (*offset_est != 0 && tuple_fraction > 0.0) {
        /*
         * We have an OFFSET but no LIMIT.	This acts entirely differently
         * from the LIMIT case: here, we need to increase rather than decrease
         * the caller's tuple_fraction, because the OFFSET acts to cause more
         * tuples to be fetched instead of fewer.  This only matters if we got
         * a tuple_fraction > 0, however.
         *
         * As above, use 10% if OFFSET is present but unestimatable.
         */
        limit_fraction = *offset_est < 0 ? 0.10 : (double)*offset_est;

        /*
         * If we have absolute counts from both caller and OFFSET, add them
         * together; likewise if they are both fractional.	If one is
         * fractional and the other absolute, we want to take the larger, and
         * we heuristically assume that's the fractional one.
         */
        if (tuple_fraction >= 1.0) {
            tuple_fraction = limit_fraction >= 1.0 ?
                tuple_fraction + limit_fraction : limit_fraction;
        } else {
            if (limit_fraction >= 1.0) {
                /* caller fractional, limit absolute; use caller's value */
            } else {
                /* both fractional, so add them together */
                tuple_fraction += limit_fraction;
                /* assume fetch all */
                tuple_fraction = tuple_fraction >= 1.0 ? 0.0 : tuple_fraction;
            }
        }
    }

    return tuple_fraction;
}

/*
 * preprocess_groupclause - do preparatory work on GROUP BY clause
 *
 * The idea here is to adjust the ordering of the GROUP BY elements
 * (which in itself is semantically insignificant) to match ORDER BY,
 * thereby allowing a single sort operation to both implement the ORDER BY
 * requirement and set up for a Unique step that implements GROUP BY.
 *
 * In principle it might be interesting to consider other orderings of the
 * GROUP BY elements, which could match the sort ordering of other
 * possible plans (eg an indexscan) and thereby reduce cost.  We don't
 * bother with that, though.  Hashed grouping will frequently win anyway.
 *
 * Note: we need no comparable processing of the distinctClause because
 * the parser already enforced that that matches ORDER BY.
 */
List* preprocess_groupclause(PlannerInfo* root, List* force)
{
    Query* parse = root->parse;
    List* new_groupclause = NIL;
    bool partial_match = false;
    ListCell* sl = NULL;
    ListCell* gl = NULL;

    /* For grouping sets, we need to force the ordering */
    if (force != NIL) {
        foreach (sl, force) {
            Index ref = lfirst_int(sl);
            SortGroupClause* cl = get_sortgroupref_clause(ref, parse->groupClause);

            if (!OidIsValid(cl->sortop)) {
                Node* expr = get_sortgroupclause_expr(cl, parse->targetList);
                ereport(ERROR,
                    (errmodule(MOD_OPT_PLANNER), errcode(ERRCODE_UNDEFINED_FUNCTION),
                        errmsg("Ordering operator cannot be identified."),
                        errdetail("Operator Type: %s", format_type_be(exprType(expr))),
                        errcause("Grouping set columns must be able to sort their inputs."),
                        erraction("Modify SQL statement according to the manual.")));
            }

            new_groupclause = lappend(new_groupclause, cl);
        }

        return new_groupclause;
    }

    /* If no ORDER BY, nothing useful to do here */
    if (parse->sortClause == NIL) {
        return parse->groupClause;
    }

    /*
     * Scan the ORDER BY clause and construct a list of matching GROUP BY
     * items, but only as far as we can make a matching prefix.
     *
     * This code assumes that the sortClause contains no duplicate items.
     */
    foreach (sl, parse->sortClause) {
        SortGroupClause* sc = (SortGroupClause*)lfirst(sl);

        foreach (gl, parse->groupClause) {
            SortGroupClause* gc = (SortGroupClause*)lfirst(gl);

            if (equal(gc, sc)) {
                new_groupclause = lappend(new_groupclause, gc);
                break;
            }
        }
        if (gl == NULL) {
            break; /* no match, so stop scanning */
        }
    }

    /* Did we match all of the ORDER BY list, or just some of it? */
    partial_match = (sl != NULL);

    /* If no match at all, no point in reordering GROUP BY */
    if (new_groupclause == NIL) {
        return parse->groupClause;
    }

    /*
     * Add any remaining GROUP BY items to the new list, but only if we were
     * able to make a complete match.  In other words, we only rearrange the
     * GROUP BY list if the result is that one list is a prefix of the other
     * --- otherwise there's no possibility of a common sort.  Also, give up
     * if there are any non-sortable GROUP BY items, since then there's no
     * hope anyway.
     */
    foreach (gl, parse->groupClause) {
        SortGroupClause* gc = (SortGroupClause*)lfirst(gl);

        if (list_member_ptr(new_groupclause, gc)) {
            continue; /* it matched an ORDER BY item */
        }
        if (partial_match) {
            return parse->groupClause; /* give up, no common sort possible */
        }
        if (!OidIsValid(gc->sortop)) {
            return parse->groupClause; /* give up, GROUP BY can't be sorted */
        }
        new_groupclause = lappend(new_groupclause, gc);
    }

    /* Success --- install the rearranged GROUP BY list */
    AssertEreport(list_length(parse->groupClause) == list_length(new_groupclause),
        MOD_OPT,
        "the length of new group clause does not match to the group clause of parse tree"
        "when doing preparatory work on GROUP BY clause.");

    return new_groupclause;
}

/*
 * Extract lists of grouping sets that can be implemented using a single
 * rollup-type aggregate pass each. Returns a list of lists of grouping sets.
 *
 * Input must be sorted with smallest sets first. Result has each sublist
 * sorted with smallest sets first.
 *
 * We want to produce the absolute minimum possible number of lists here to
 * avoid excess sorts. Fortunately, there is an algorithm for this; the problem
 * of finding the minimal partition of a partially-ordered set into chains
 * (which is what we need, taking the list of grouping sets as a poset ordered
 * by set inclusion) can be mapped to the problem of finding the maximum
 * cardinality matching on a bipartite graph, which is solvable in polynomial
 * time with a worst case of no worse than O(n^2.5) and usually much
 * better. Since our N is at most 4096, we don't need to consider fallbacks to
 * heuristic or approximate methods.  (Planning time for a 12-d cube is under
 * half a second on my modest system even with optimization off and assertions
 * on.)
 */
List* extract_rollup_sets(List* groupingSets)
{
    int num_sets_raw = list_length(groupingSets);
    int num_empty = 0;
    int num_sets = 0; /* distinct sets */
    int num_chains = 0;
    List* result = NIL;
    List** results;
    List** orig_sets;
    Bitmapset** set_masks;
    int* chains = NULL;
    short** adjacency;
    short* adjacency_buf = NULL;
    BipartiteMatchState* state = NULL;
    int i;
    int j;
    int j_size;
    ListCell* lc1 = list_head(groupingSets);
    ListCell* lc = NULL;

    /*
     * Start by stripping out empty sets.  The algorithm doesn't require this,
     * but the planner currently needs all empty sets to be returned in the
     * first list, so we strip them here and add them back after.
     */
    while (lc1 && lfirst(lc1) == NIL) {
        ++num_empty;
        lc1 = lnext(lc1);
    }

    /* bail out now if it turns out that all we had were empty sets. */
    if (lc1 == NULL)
        return list_make1(groupingSets);

    /* ----------
     * We don't strictly need to remove duplicate sets here, but if we don't,
     * they tend to become scattered through the result, which is a bit
     * confusing (and irritating if we ever decide to optimize them out).
     * So we remove them here and add them back after.
     *
     * For each non-duplicate set, we fill in the following:
     *
     * orig_sets[i] = list of the original set lists
     * set_masks[i] = bitmapset for testing inclusion
     * adjacency[i] = array [n, v1, v2, ... vn] of adjacency indices
     *
     * chains[i] will be the result group this set is assigned to.
     *
     * We index all of these from 1 rather than 0 because it is convenient
     * to leave 0 free for the NIL node in the graph algorithm.
     * ----------
     */
    orig_sets = (List**)palloc0((num_sets_raw + 1) * sizeof(List*));
    set_masks = (Bitmapset**)palloc0((num_sets_raw + 1) * sizeof(Bitmapset*));
    adjacency = (short**)palloc0((num_sets_raw + 1) * sizeof(short*));
    adjacency_buf = (short*)palloc((num_sets_raw + 1) * sizeof(short));

    j_size = 0;
    j = 0;
    i = 1;

    for_each_cell(lc, lc1)
    {
        List* candidate = (List*)lfirst(lc);
        Bitmapset* candidate_set = NULL;
        ListCell* lc2 = NULL;
        int dup_of = 0;

        foreach (lc2, candidate) {
            candidate_set = bms_add_member(candidate_set, lfirst_int(lc2));
        }

        /* we can only be a dup if we're the same length as a previous set */
        if (j_size == list_length(candidate)) {
            int k;

            for (k = j; k < i; ++k) {
                if (bms_equal(set_masks[k], candidate_set)) {
                    dup_of = k;
                    break;
                }
            }
        } else if (j_size < list_length(candidate)) {
            j_size = list_length(candidate);
            j = i;
        }

        if (dup_of > 0) {
            orig_sets[dup_of] = lappend(orig_sets[dup_of], candidate);
            bms_free_ext(candidate_set);
        } else {
            int k;
            int n_adj = 0;

            orig_sets[i] = list_make1(candidate);
            set_masks[i] = candidate_set;

            /* fill in adjacency list; no need to compare equal-size sets */
            for (k = j - 1; k > 0; --k) {
                if (bms_is_subset(set_masks[k], candidate_set))
                    adjacency_buf[++n_adj] = k;
            }

            if (n_adj > 0) {
                adjacency_buf[0] = n_adj;
                adjacency[i] = (short*)palloc((n_adj + 1) * sizeof(short));
                errno_t errorno =
                    memcpy_s(adjacency[i], (n_adj + 1) * sizeof(short), adjacency_buf, (n_adj + 1) * sizeof(short));
                securec_check_c(errorno, "\0", "\0");
            } else
                adjacency[i] = NULL;

            ++i;
        }
    }

    num_sets = i - 1;

    /*
     * Apply the graph matching algorithm to do the work.
     */
    state = BipartiteMatch(num_sets, num_sets, adjacency);

    /*
     * Now, the state->pair* fields have the info we need to assign sets to
     * chains. Two sets (u,v) belong to the same chain if pair_uv[u] = v or
     * pair_vu[v] = u (both will be true, but we check both so that we can do
     * it in one pass)
     */
    chains = (int*)palloc0((num_sets + 1) * sizeof(int));

    for (i = 1; i <= num_sets; ++i) {
        int u = state->pair_vu[i];
        int v = state->pair_uv[i];

        if (u > 0 && u < i) {
            chains[i] = chains[u];
        } else if (v > 0 && v < i) {
            chains[i] = chains[v];
        } else {
            chains[i] = ++num_chains;
        }
    }

    /* build result lists. */
    results = (List**)palloc0((num_chains + 1) * sizeof(List*));

    for (i = 1; i <= num_sets; ++i) {
        int c = chains[i];

        AssertEreport(c > 0, MOD_OPT, "invalid chains item when building result lists.");

        results[c] = list_concat(results[c], orig_sets[i]);
    }

    /* push any empty sets back on the first list. */
    while (num_empty-- > 0)
        results[1] = lcons(NIL, results[1]);

    /* make result list */
    for (i = 1; i <= num_chains; ++i)
        result = lappend(result, results[i]);

    /*
     * Free all the things.
     *
     * (This is over-fussy for small sets but for large sets we could have
     * tied up a nontrivial amount of memory.)
     */
    BipartiteMatchFree(state);
    pfree_ext(results);
    pfree_ext(chains);
    for (i = 1; i <= num_sets; ++i)
        if (adjacency[i])
            pfree_ext(adjacency[i]);
    pfree_ext(adjacency);
    pfree_ext(adjacency_buf);
    pfree_ext(orig_sets);
    for (i = 1; i <= num_sets; ++i)
        bms_free_ext(set_masks[i]);
    pfree_ext(set_masks);

    return result;
}

/*
 * Reorder the elements of a list of grouping sets such that they have correct
 * prefix relationships.
 *
 * The input must be ordered with smallest sets first; the result is returned
 * with largest sets first.
 *
 * If we're passed in a sortclause, we follow its order of columns to the
 * extent possible, to minimize the chance that we add unnecessary sorts.
 * (We're trying here to ensure that GROUPING SETS ((a,b,c),(c)) ORDER BY c,b,a
 * gets implemented in one pass.)
 */
List* reorder_grouping_sets(List* groupingsets, List* sortclause)
{
    ListCell* lc = NULL;
    ListCell* lc2 = NULL;
    List* previous = NIL;
    List* result = NIL;

    foreach (lc, groupingsets) {
        List* candidate = (List*)lfirst(lc);
        List* new_elems = list_difference_int(candidate, previous);

        if (list_length(new_elems) > 0) {
            while (list_length(sortclause) > list_length(previous)) {
                SortGroupClause* sc = (SortGroupClause*)list_nth(sortclause, list_length(previous));
                int ref = sc->tleSortGroupRef;

                if (list_member_int(new_elems, ref)) {
                    previous = lappend_int(previous, ref);
                    new_elems = list_delete_int(new_elems, ref);
                } else {
                    /* diverged from the sortclause; give up on it */
                    sortclause = NIL;
                    break;
                }
            }

            foreach (lc2, new_elems) {
                previous = lappend_int(previous, lfirst_int(lc2));
            }
        }

        result = lcons(list_copy(previous), result);
        list_free_ext(new_elems);
    }

    list_free_ext(previous);

    return result;
}

/*
 * get_optimal_hashed_path: get optimal hash path from three hashagg paths.
 *
 * Parameters:
 *	@in root: plan info node
 *	@in	path_rows: the parent's rows of cheapest path
 *	@in path_width: the parent's width of cheapest path
 *	@in	cheapest_path: the cheapest path
 *	@in	needs_stream: we can generate two paths with redistribute if it is true
 *	@in	numGroups: the distinct for group by clause
 *	@in agg_costs: the execution costs of the aggregates' input expressions
 *	@in	distributed_key: the distribute key for stream
 *	@in	multiple: the multiple for stream
 *	@in	hashentrysize: hash entry size include space for per tuple width, space for pass-by-ref transition values,
 *		the per-hash-entry overhead
 *	@in/out hashed_p: the optimal hashagg path
 *
 * Returns: void
 */
static void get_optimal_hashed_path(PlannerInfo* root, Path* cheapest_path, bool needs_stream, int path_width,
    AggClauseCosts* agg_costs, int numGroupCols, const double* numGroups, List* distributed_key, double multiple,
    Size hashentrysize, AggStrategy agg_strategy, Path* hashed_p)
{
    QualCost total_cost;
    Plan* subplan = makeNode(Plan);
    double best_cost = 0.0;
    Path result_path;
    errno_t rc = EOK;

    rc = memset_s(&result_path, sizeof(Path), 0, sizeof(Path));
    securec_check(rc, "\0", "\0");

    subplan->startup_cost = cheapest_path->startup_cost;
    subplan->total_cost = cheapest_path->total_cost;
    subplan->plan_rows = cheapest_path->rows;
    subplan->multiple = cheapest_path->multiple;
    subplan->plan_width = path_width;
    subplan->vec_output = false;
    subplan->exec_nodes =
        ng_convert_to_exec_nodes(&cheapest_path->distribution, cheapest_path->locator_type, RELATION_ACCESS_READ);
    subplan->dop = cheapest_path->dop;

    total_cost.startup = 0.0;
    total_cost.per_tuple = 0.0;

    if (root->query_level == 1) {
        /* Get total cost for hashagg (dn) + gather + hashagg (cn). */
        get_hashagg_gather_hashagg_path(root,
            subplan,
            agg_costs,
            numGroupCols,
            numGroups[0],
            numGroups[1],
            total_cost,
            hashentrysize,
            agg_strategy,
            needs_stream,
            &result_path);
        if ((best_cost == 0.0) || (result_path.total_cost < best_cost)) {
            best_cost = result_path.total_cost;
            copy_path_costsize(hashed_p, &result_path);
        }
    }

    if (needs_stream && (distributed_key != NIL)) {
        /* Get total cost for redistribute(dn) + hashagg (dn). */
        Distribution* distribution = ng_get_dest_distribution(subplan);
        get_redist_hashagg_path(root,
            subplan,
            agg_costs,
            numGroupCols,
            numGroups[0],
            numGroups[1],
            distributed_key,
            multiple,
            distribution,
            total_cost,
            hashentrysize,
            needs_stream,
            &result_path);
        if ((best_cost == 0.0) || (result_path.total_cost < best_cost)) {
            best_cost = result_path.total_cost;
            copy_path_costsize(hashed_p, &result_path);
        }

        /* Get total cost for hashagg (dn) + redistribute(dn) + hashagg (dn). */
        get_hashagg_redist_hashagg_path(root,
            subplan,
            agg_costs,
            numGroupCols,
            numGroups[0],
            numGroups[1],
            distributed_key,
            multiple,
            distribution,
            total_cost,
            hashentrysize,
            needs_stream,
            &result_path);

        /* Save the best cost for hashed path. */
        if ((best_cost == 0.0) || (result_path.total_cost < best_cost)) {
            copy_path_costsize(hashed_p, &result_path);
        }
    }

    pfree_ext(subplan);
    subplan = NULL;
}

/*
 * compute_hashed_path_cost: compute hashagg path cost for choose.
 *
 * Parameters:
 *	@in root: plan info node
 *	@in	limit_tuples: estimate tuples for LIMIT
 *	@in	path_rows: the parent's rows of cheapest path
 *	@in path_width: the parent's width of cheapest path
 *	@in	cheapest_path: the cheapest path
 *	@in	dNumGroups: the distinct for group by clause
 *	@in agg_costs: the execution costs of the aggregates' input expressions
 *	@in	hashentrysize: hash entry size include space for per tuple width, space for pass-by-ref transition values,
 *		the per-hash-entry overhead
 *	@in	target_pathkeys: the available pathkeys for plan info
 *	@in/out hashed_p: result hash path with total cost
 *
 * Returns: void
 */
static void compute_hashed_path_cost(PlannerInfo* root, double limit_tuples, int path_width, Path* cheapest_path,
    const double* dNumGroups, AggClauseCosts* agg_costs, Size hashentrysize, List* target_pathkeys, Path* hashed_p)
{
    Query* parse = root->parse;
    int numGroupCols = list_length(parse->groupClause);
    List* distributed_key = NIL;
    double multiple = 0.0;
    bool needs_stream = false;
    bool need_second_hashagg = false;

    /*
     * See if the estimated cost is no more than doing it the other way. While
     * avoiding the need for sorted input is usually a win, the fact that the
     * output won't be sorted may be a loss; so we need to do an actual cost
     * comparison.
     *
     * We need to consider cheapest_path + hashagg [+ final sort] versus
     * either cheapest_path [+ sort] + group or agg [+ final sort] or
     * presorted_path + group or agg [+ final sort] where brackets indicate a
     * step that may not be needed. We assume query_planner() will have
     * returned a presorted path only if it's a winner compared to
     * cheapest_path for this purpose.
     *
     * These path variables are dummies that just hold cost fields; we don't
     * make actual Paths for these steps.
     *
     * We need two hashagg for count(distinct) case, so do estimation twice.
     * It be affirm a stream plan if agg_costs->exprAggs is not null.
     */
    if (agg_costs->exprAggs != NIL) {
        List* group_exprs = get_sortgrouplist_exprs(parse->groupClause, parse->targetList);
        List* newtlist = NIL;
        List* orig_tlist = NIL;
        List* duplicate_tlist = NIL;
        Oid distinct_eq_op = InvalidOid;
        double numGroups[2] = {0};
        Node* distinct_node = (Node*)linitial(agg_costs->exprAggs);
        AggStrategy strategy = (parse->groupClause != NIL) ? AGG_HASHED : AGG_PLAIN;
        group_exprs = lappend(group_exprs, distinct_node);
        get_num_distinct(root,
            group_exprs,
            PATH_LOCAL_ROWS(cheapest_path),
            cheapest_path->rows,
            ng_get_dest_num_data_nodes(cheapest_path),
            numGroups);

        /* generate new targetlist for the first level which using to judge whether do redistribute or not. */
        newtlist = get_count_distinct_newtlist(
            root, parse->targetList, distinct_node, &orig_tlist, &duplicate_tlist, &distinct_eq_op);
        /* check whether need stream or not according to distribute_keys. */
        if (IS_STREAM_PLAN && cheapest_path->locator_type != LOCATOR_TYPE_REPLICATED)
            needs_stream = needs_agg_stream(root, newtlist, cheapest_path->distribute_keys);

        root->query_level++;
        /* generate the optimizer hashagg path for the first level. */
        if (needs_stream) {
            distributed_key = get_distributekey_from_tlist(root, newtlist, group_exprs, cheapest_path->rows, &multiple);
            if (distributed_key != NIL) {
                get_optimal_hashed_path(root,
                    cheapest_path,
                    needs_stream,
                    path_width,
                    agg_costs,
                    numGroupCols + 1,
                    numGroups,
                    distributed_key,
                    multiple,
                    hashentrysize,
                    AGG_HASHED,
                    hashed_p);
                elog(DEBUG1,
                    "[choose optimal hashagg]: the total cost of hashagg "
                    "with redistribute for the first level: %lf",
                    hashed_p->total_cost);
            }
        } else {
            cost_agg(hashed_p,
                root,
                AGG_HASHED,
                agg_costs,
                numGroupCols + 1,
                numGroups[0],
                cheapest_path->startup_cost,
                cheapest_path->total_cost,
                PATH_LOCAL_ROWS(cheapest_path),
                path_width,
                hashentrysize);
            elog(DEBUG1,
                "[choose optimal hashagg]: the total cost of hashagg "
                "with no redistribute for the first level: %lf",
                hashed_p->total_cost);
        }

        /* generate the optimizer hashagg path for the second level. */
        root->query_level--;
        if (AGG_PLAIN == strategy) {
            /* only generate path of plainagg+gather+plainagg for the second level. */
            get_optimal_hashed_path(root,
                hashed_p,
                needs_stream,
                path_width,
                agg_costs,
                numGroupCols,
                dNumGroups,
                NULL,
                multiple,
                hashentrysize,
                AGG_PLAIN,
                hashed_p);
            elog(DEBUG1,
                "[choose optimize hashagg]: the total cost of plain hashagg "
                "for the second level: %lf",
                hashed_p->total_cost);
        } else {
            need_second_hashagg = true;
        }
        list_free_ext(group_exprs);
        list_free_ext(duplicate_tlist);
        list_free_ext(orig_tlist);
        list_free_ext(newtlist);
    }

    /*
     * get optimal hashagg if only have group by or the second hashagg
     * for count(distinct) with group by.
     */
    if ((agg_costs->exprAggs == NIL) || need_second_hashagg) {
        Path* path = NULL;
        distributed_key = NIL;

        path = need_second_hashagg ? hashed_p : cheapest_path;

        /* need compare three hashagg cost. */
        if (IS_STREAM_PLAN && path->locator_type != LOCATOR_TYPE_REPLICATED) {
            needs_stream = needs_agg_stream(root, parse->targetList, path->distribute_keys);

            if (needs_stream) {
                distributed_key =
                    get_distributekey_from_tlist(root, parse->targetList, parse->groupClause, path->rows, &multiple);
            }
            /* get the optimizer hashagg path for three path. */
            get_optimal_hashed_path(root,
                path,
                needs_stream,
                path_width,
                agg_costs,
                numGroupCols,
                dNumGroups,
                distributed_key,
                multiple,
                hashentrysize,
                AGG_HASHED,
                hashed_p);
        } else {
            /* regress to original aggpath if not stream plan or boardcast+hashagg in subplan. */
            cost_agg(hashed_p,
                root,
                AGG_HASHED,
                agg_costs,
                numGroupCols,
                dNumGroups[0],
                path->startup_cost,
                path->total_cost,
                PATH_LOCAL_ROWS(cheapest_path),
                path_width,
                hashentrysize);
        }

        if (need_second_hashagg) {
            elog(DEBUG1,
                "[choose optimize hashagg]: the total cost of hashagg with redistribute for the second level: %lf",
                hashed_p->total_cost);
        } else {
            elog(DEBUG1,
                "[choose optimize hashagg]: the total cost of hashagg with no count(distinct): %lf",
                hashed_p->total_cost);
        }
    }

    list_free_ext(distributed_key);

    /* Result of hashed agg is always unsorted */
    if (target_pathkeys != NULL) {
        cost_sort(hashed_p,
            target_pathkeys,
            hashed_p->total_cost,
            dNumGroups[0],
            path_width,
            0.0,
            u_sess->opt_cxt.op_work_mem,
            limit_tuples,
            root->glob->vectorized);
    }

    elog(DEBUG1, "[final hashed path total cost]: %lf", hashed_p->total_cost);
}

/*
 * get_optimal_sorted_path: get optimal sort path for choose.
 *
 * Parameters:
 *	@in root: plan info node
 *	@in	path_rows: the parent's rows of cheapest path
 *	@in path_width: the parent's width of cheapest path
 *	@in	dNumGroups: the distinct for group by clause
 *	@in agg_costs: the execution costs of the aggregates' input expressions
 *	@in	hashentrysize: hash entry size include space for per tuple width, space for pass-by-ref transition values,
 *		the per-hash-entry overhead
 *	@in	numGroupCols: how many cols in group by clause
 *	@in/out sorted_p: result sort path with total cost
 *
 * Returns: void
 */
static void get_optimal_sorted_path(PlannerInfo* root, Path* sorted_p, int path_width, AggClauseCosts* agg_costs,
    int numGroupCols, const double* dNumGroups, Size hashentrysize, double limit_tuples, bool needs_stream,
    bool need_sort_for_grouping)
{
    Query* parse = root->parse;
    Node* distinct_node = NULL;
    List* distributed_key = NIL;
    List* distinct_node_list = NIL;
    double multiple = 0.0;
    bool two_level_groupagg = false;
    bool has_stream = false;
    bool has_local_stream = false;
    bool distinct_needs_stream = false;
    bool distinct_needs_local_stream = false;
    StreamPath stream_p, stream_local_p;
    errno_t rc = EOK;
    AggStrategy strategy = (parse->groupClause != NIL) ? AGG_SORTED : AGG_PLAIN;
    Path* top_level_path = NULL;

    rc = memset_s(&stream_p, sizeof(stream_p), 0, sizeof(stream_p));
    securec_check(rc, "\0", "\0");
    rc = memset_s(&stream_local_p, sizeof(stream_local_p), 0, sizeof(stream_local_p));
    securec_check(rc, "\0", "\0");

    if (IS_STREAM_PLAN && needs_stream) {
        /* get distribute key and multiple from groupClause */
        distributed_key =
            get_distributekey_from_tlist(root, parse->targetList, parse->groupClause, sorted_p->rows, &multiple);
    }

    /* check whether two_level_sort is needed. */
    if (IS_STREAM_PLAN && (list_length(agg_costs->exprAggs) == 1) && (!(parse->groupClause && !needs_stream)) &&
        !agg_costs->hasDnAggs && !agg_costs->hasdctDnAggs && agg_costs->numOrderedAggs == 0) {
        Plan* subplan = makeNode(Plan);
        /* construct subplan for sort path. */
        subplan->startup_cost = sorted_p->startup_cost;
        subplan->total_cost = sorted_p->total_cost;
        subplan->plan_rows = sorted_p->rows;
        subplan->multiple = sorted_p->multiple;
        subplan->plan_width = path_width;
        subplan->distributed_keys = sorted_p->distribute_keys;

        distinct_node = (Node*)linitial(agg_costs->exprAggs);
        distinct_node_list = list_make1(distinct_node);
        two_level_groupagg = needs_two_level_groupagg(
            root, subplan, distinct_node, distributed_key, &distinct_needs_stream, &distinct_needs_local_stream);

        pfree_ext(subplan);
        subplan = NULL;
    }

    /*
     * we should consider optimal groupagg path below:
     * 1. if we have count(distinct) and group by clause
     *  (1) need two level groupagg, and distinct expr need stream, generate path
     *       redistribute(distinct_node) + groupagg + redistribute(groupby_node) + groupagg
     *  (2) need two level groupagg, and distinct expr need not stream, generate path
     *       groupagg+redistribute(groupby_node) + groupagg
     *  (3) need one level groupagg, and there is distribute key for group by clause, generate path
     *       redistribute(groupby_node) + groupagg
     * 2. if we have count(distinct) and have no group by clause
     *  (1) distinct expr need stream, generate path
     *	   redistribute(distinct_node) + groupagg + gather + agg
     *  (2) distinct expr need not stream, generate path
     *	   groupagg + gather + agg
     * 3. if we have no count(distinct) and have group by clause
     */
    if (parse->groupClause) {
        /* if there's count(distinct), we now only support redistribute by group clause */
        if (IS_STREAM_PLAN && needs_stream && (agg_costs->exprAggs != NIL || agg_costs->hasDnAggs)) {
            /* we can apply local sortagg if count(distinct) expr is distribute column */
            if (two_level_groupagg) {
                Distribution* distribution = ng_get_dest_distribution(sorted_p);
                ng_copy_distribution(&stream_p.path.distribution, distribution);
                ng_copy_distribution(&stream_p.consumer_distribution, distribution);
                ng_copy_distribution(&stream_local_p.path.distribution, distribution);
                ng_copy_distribution(&stream_local_p.consumer_distribution, distribution);

                if (distinct_needs_stream) {
                    /* compute stream path of redistribute for distinct expr. */
                    stream_p.path.distribute_keys = distinct_node_list;
                    stream_p.subpath = sorted_p;
                    stream_p.type = STREAM_REDISTRIBUTE;
                    stream_p.path.multiple = 1.0;
                    cost_stream(&stream_p, path_width);
                    need_sort_for_grouping = true;
                } else {
                    copy_path_costsize(&stream_p.path, sorted_p);
                }

                if (need_sort_for_grouping) {
                    cost_sort(sorted_p,
                        root->group_pathkeys,
                        stream_p.path.total_cost,
                        PATH_LOCAL_ROWS(&stream_p.path),
                        path_width,
                        0.0,
                        u_sess->opt_cxt.op_work_mem,
                        -1.0,
                        root->glob->vectorized);
                    copy_path_costsize(&stream_p.path, sorted_p);
                }

                /* comput groupagg path. */
                cost_agg(sorted_p,
                    root,
                    strategy,
                    agg_costs,
                    numGroupCols,
                    dNumGroups[0],
                    stream_p.path.startup_cost,
                    stream_p.path.total_cost,
                    PATH_LOCAL_ROWS(&stream_p.path),
                    path_width,
                    hashentrysize);

                ereport(DEBUG1,
                    (errmodule(MOD_OPT_AGG),
                        (errmsg("[choose optimize groupagg]: the total cost of groupagg with redistribute for the "
                                "first level: %lf",
                            sorted_p->total_cost))));
            }

            /* there's group by clause, and a redistribution on group by clause is needed. */
            if (distributed_key != NIL) {
                /* compute stream path of redistribute for distinct expr. */
                stream_p.path.distribute_keys = distributed_key;
                Distribution* distribution = ng_get_dest_distribution(sorted_p);
                ng_copy_distribution(&stream_p.path.distribution, distribution);
                ng_copy_distribution(&stream_p.consumer_distribution, distribution);
                stream_p.subpath = sorted_p;
                stream_p.type = STREAM_REDISTRIBUTE;
                stream_p.path.multiple = multiple;
                cost_stream(&stream_p, path_width);
                has_stream = true;
                needs_stream = false;
            }
        }
    } else { /* there's no group by clause, AGG_PLAIN used. */
        if (IS_STREAM_PLAN && (agg_costs->exprAggs != NIL) && distinct_needs_stream) {
            /* compute stream path of redistribute for distinct expr. */
            stream_p.path.distribute_keys = distinct_node_list;
            Distribution* distribution = ng_get_dest_distribution(sorted_p);
            ng_copy_distribution(&stream_p.path.distribution, distribution);
            ng_copy_distribution(&stream_p.consumer_distribution, distribution);
            stream_p.subpath = sorted_p;
            stream_p.type = STREAM_REDISTRIBUTE;
            stream_p.path.multiple = 1.0;
            cost_stream(&stream_p, path_width);
            has_stream = true;
        }
    }

    /* group by has stream. */
    if (has_stream && has_local_stream)
        top_level_path = &stream_p.path;
    else if (!has_stream && has_local_stream)
        top_level_path = &stream_local_p.path;
    else if (has_stream && !has_local_stream)
        top_level_path = &stream_p.path;
    else
        top_level_path = sorted_p;

    /* compute groupagg path. */
    if (need_sort_for_grouping) {
        cost_sort(sorted_p,
            root->group_pathkeys,
            top_level_path->total_cost,
            PATH_LOCAL_ROWS(top_level_path),
            path_width,
            0.0,
            u_sess->opt_cxt.op_work_mem,
            -1.0,
            root->glob->vectorized);
        copy_path_costsize(top_level_path, sorted_p);
    }

    cost_agg(sorted_p,
        root,
        strategy,
        agg_costs,
        numGroupCols,
        dNumGroups[0],
        top_level_path->startup_cost,
        top_level_path->total_cost,
        PATH_LOCAL_ROWS(top_level_path),
        path_width,
        hashentrysize);

    ereport(DEBUG1,
        (errmodule(MOD_OPT_AGG),
            (errmsg(
                "[choose optimize groupagg]: the total cost of groupagg with redistribute for the second level: %lf",
                sorted_p->total_cost))));

    /* compute the cost of gather to CN and AGG_PLAIN. */
    Distribution* distribution = ng_get_dest_distribution(sorted_p);
    ng_copy_distribution(&stream_p.path.distribution, distribution);
    stream_p.subpath = sorted_p;
    stream_p.consumer_distribution.group_oid = InvalidOid;
    stream_p.consumer_distribution.bms_data_nodeids = ng_get_single_node_group_nodeids();
    stream_p.type = STREAM_GATHER;
    stream_p.path.locator_type = LOCATOR_TYPE_REPLICATED;
    cost_stream(&stream_p, path_width);
    copy_path_costsize(sorted_p, &stream_p.path);

    if (IS_STREAM_PLAN && needs_stream) {
        /* For plain agg, there's no sort needed */
        if (parse->groupClause != NIL)
            cost_sort(sorted_p,
                root->group_pathkeys,
                sorted_p->total_cost,
                PATH_LOCAL_ROWS(sorted_p),
                path_width,
                0.0,
                u_sess->opt_cxt.op_work_mem,
                -1.0,
                root->glob->vectorized);

        cost_agg(sorted_p,
            root,
            strategy,
            agg_costs,
            numGroupCols,
            dNumGroups[1],
            sorted_p->startup_cost,
            sorted_p->total_cost,
            PATH_LOCAL_ROWS(sorted_p),
            path_width,
            hashentrysize);
    }

    if (distinct_node_list != NIL) {
        list_free_ext(distinct_node_list);
        distinct_node_list = NIL;
    }

    if (distributed_key != NIL) {
        list_free_ext(distributed_key);
        distributed_key = NIL;
    }
}

/*
 * compute_sorted_path_cost: compute sort path cost for choose.
 *
 * Parameters:
 *	@in root: plan info node
 *	@in	limit_tuples: estimate tuples for LIMIT
 *	@in	path_rows: the parent's rows of cheapest path
 *	@in path_width: the parent's width of cheapest path
 *	@in	cheapest_path: the cheapest path
 *	@in	sorted_path: the initial sort path
 *	@in	dNumGroups: the distinct for group by clause
 *	@in agg_costs: the execution costs of the aggregates' input expressions
 *	@in	hashentrysize: hash entry size include space for per tuple width, space for pass-by-ref transition values,
 *		the per-hash-entry overhead
 *	@in	target_pathkeys: the available pathkeys for plan info
 *	@in/out sorted_p: result sort path with total cost
 *
 * Returns: void
 */
static void compute_sorted_path_cost(PlannerInfo* root, double limit_tuples, int path_width, Path* cheapest_path,
    Path* sorted_path, const double* dNumGroups, AggClauseCosts* agg_costs, Size hashentrysize, List* target_pathkeys,
    Path* sorted_p)
{
    Query* parse = root->parse;
    int numGroupCols = list_length(parse->groupClause);
    List* current_pathkeys = NIL;
    StreamPath stream_p;
    bool needs_stream = false;
    bool need_sort_for_grouping = false;
    errno_t rc = EOK;
    bool is_replicate = (!IS_STREAM_PLAN || cheapest_path->locator_type == LOCATOR_TYPE_REPLICATED);

    rc = memset_s(&stream_p, sizeof(stream_p), 0, sizeof(stream_p));
    securec_check(rc, "\0", "\0");

    /* use sorted path if it exists, other wise we use cheapest path.  */
    if (sorted_path != NULL) {
        copy_path_costsize(sorted_p, sorted_path);
        sorted_p->distribute_keys = sorted_path->distribute_keys;
        current_pathkeys = sorted_path->pathkeys;
        Distribution* distribution = ng_get_dest_distribution(sorted_path);
        ng_copy_distribution(&sorted_p->distribution, distribution);
    } else {
        copy_path_costsize(sorted_p, cheapest_path);
        sorted_p->distribute_keys = cheapest_path->distribute_keys;
        current_pathkeys = cheapest_path->pathkeys;
        Distribution* distribution = ng_get_dest_distribution(cheapest_path);
        ng_copy_distribution(&sorted_p->distribution, distribution);
    }

    if (!pathkeys_contained_in(root->group_pathkeys, current_pathkeys)) {
        current_pathkeys = root->group_pathkeys;
        need_sort_for_grouping = true;
    }

    if (is_replicate || !parse->hasAggs) {
        if (need_sort_for_grouping) {
            cost_sort(sorted_p,
                root->group_pathkeys,
                sorted_p->total_cost,
                PATH_LOCAL_ROWS(sorted_p),
                path_width,
                0.0,
                u_sess->opt_cxt.op_work_mem,
                -1.0,
                root->glob->vectorized);
        }
    }

    if (!is_replicate)
        needs_stream = needs_agg_stream(root, parse->targetList, sorted_p->distribute_keys);

    /* get optimal sort path if have count(distinct). */
    if (parse->hasAggs) {
        if (is_replicate)
            cost_agg(sorted_p,
                root,
                AGG_SORTED,
                agg_costs,
                numGroupCols,
                dNumGroups[0],
                sorted_p->startup_cost,
                sorted_p->total_cost,
                PATH_LOCAL_ROWS(sorted_p),
                path_width,
                hashentrysize);
        else
            get_optimal_sorted_path(root,
                sorted_p,
                path_width,
                agg_costs,
                numGroupCols,
                dNumGroups,
                hashentrysize,
                limit_tuples,
                needs_stream,
                need_sort_for_grouping);
    } else {
        cost_group(sorted_p,
            root,
            numGroupCols,
            dNumGroups[0],
            sorted_p->startup_cost,
            sorted_p->total_cost,
            PATH_LOCAL_ROWS(sorted_p));

        if (!is_replicate) {
            /* we should consider the cost of gather because sort+group has include it. */
            stream_p.subpath = sorted_p;
            Distribution* distribution = ng_get_dest_distribution(sorted_p);
            ng_copy_distribution(&stream_p.path.distribution, distribution);
            ng_copy_distribution(&stream_p.consumer_distribution, distribution);
            stream_p.type = STREAM_GATHER;
            stream_p.path.locator_type = LOCATOR_TYPE_REPLICATED;
            cost_stream(&stream_p, path_width);
            copy_path_costsize(sorted_p, &stream_p.path);

            /* compute sort+group cost on CN. */
            if (needs_stream) {
                cost_sort(sorted_p,
                    root->group_pathkeys,
                    sorted_p->total_cost,
                    PATH_LOCAL_ROWS(sorted_p),
                    path_width,
                    0.0,
                    u_sess->opt_cxt.op_work_mem,
                    -1.0,
                    root->glob->vectorized);

                cost_group(sorted_p,
                    root,
                    numGroupCols,
                    dNumGroups[0],
                    sorted_p->startup_cost,
                    sorted_p->total_cost,
                    PATH_LOCAL_ROWS(sorted_p));
            }
        }
    }

    /* The Agg or Group node will preserve ordering */
    if (target_pathkeys && !pathkeys_contained_in(target_pathkeys, current_pathkeys))
        cost_sort(sorted_p,
            target_pathkeys,
            sorted_p->total_cost,
            dNumGroups[0],
            path_width,
            0.0,
            u_sess->opt_cxt.op_work_mem,
            limit_tuples,
            root->glob->vectorized);

    ereport(DEBUG1, (errmodule(MOD_OPT_AGG), (errmsg("[final sorted path total cost]: %lf", sorted_p->total_cost))));
}

/*
 * Executor doesn't support hashed aggregation with DISTINCT or ORDER BY
 * aggregates.	(Doing so would imply storing *all* the input values in
 * the hash table, and/or running many sorts in parallel, either of which
 * seems like a certain loser.)
 */
static bool grouping_is_can_hash(Query* parse, AggClauseCosts* agg_costs)
{
    bool can_hash = false;
    can_hash = grouping_is_hashable(parse->groupClause);
    if (IS_STREAM_PLAN) {
        can_hash = can_hash && (agg_costs->numOrderedAggs == 0 &&
                                   (list_length(agg_costs->exprAggs) == 0 ||
                                       (list_length(agg_costs->exprAggs) == 1 && !agg_costs->hasDnAggs &&
                                           !agg_costs->hasdctDnAggs && !agg_costs->unhashable)));
    } else {
        can_hash = can_hash && (agg_costs->numOrderedAggs == 0 && list_length(agg_costs->exprAggs) == 0);
    }

    return can_hash;
}

/* Estimate per-hash-entry space at tuple width... and per-hash-entry overhead */
static Size compute_hash_entry_size(bool vectorized, Path* cheapest_path, int path_width, AggClauseCosts* agg_costs)
{
    Size hash_entry_size;
    if (vectorized){
        hash_entry_size =
            get_path_actual_total_width(cheapest_path, vectorized, OP_HASHAGG, agg_costs->numAggs);
    } else {
        hash_entry_size = get_hash_entry_size(path_width, agg_costs->numAggs);
    }
        
    /* plus space for pass-by-ref transition values... */
    hash_entry_size += agg_costs->transitionSpace;
    return hash_entry_size;
}

/*
 * choose_hashed_grouping - should we use hashed grouping?
 *
 * Returns TRUE to select hashing, FALSE to select sorting.
 */
static bool choose_hashed_grouping(PlannerInfo* root, double tuple_fraction, double limit_tuples, int path_width,
    Path* cheapest_path, Path* sorted_path, const double* dNumGroups, AggClauseCosts* agg_costs, Size* hash_entry_size)
{
    Query* parse = root->parse;
    bool can_hash = false;
    bool can_sort = false;
    Size hashentrysize;
    List* target_pathkeys = NIL;
    Path hashed_p, sorted_p;
    errno_t rc = EOK;

    can_hash = grouping_is_can_hash(parse, agg_costs);
    can_sort = grouping_is_sortable(parse->groupClause) && !root->parse->unique_check;

    /* Quick out if only one choice is workable */
    if (!(can_hash && can_sort)) {
        if (can_hash) {
            return true;
        } else if (can_sort) {
            return false;
        } else {
            ereport(ERROR,
                (errmodule(MOD_OPT_PLANNER), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("GROUP BY cannot be implemented."),
                    errdetail("Some of the datatypes only support hashing, "
                        "while others only support sorting."),
                    errcause("GROUP BY uses unsupported datatypes."),
                    erraction("Modify SQL statement according to the manual.")));
        }
    }

    /*
     * Check count(distinct) case in advance.
     * case1: If expr in count distinct is set-returned, only hashagg plan supported.
     * 		Actually aggregate function cannot contain set-returning expr(executor
     * 		does not support). But hashagg plan will pushdown the set-returning expr
     * 		to subplan(only support count(distinct) for now, may support other case future),
     *		that make set-returning expr in count distinct calculated correctly possible.
     *
     * case2: If groupClause is not distributable and expr in count distinct is distributable,
     * 		only hashagg plan can be shipped.
     *
     * case3: If groupClause and expr in count distinct both are not distributable,
     *		the SQL can't be shipped.
     */
    if (list_length(agg_costs->exprAggs) == 1) {
        /* case1 */
        if (expression_returns_set((Node*)agg_costs->exprAggs)) {
            return true;
        }
#ifdef ENABLE_MULTIPLE_NODES
        if (parse->groupClause) {
            bool grp_is_distributable = grouping_is_distributable(parse->groupClause, parse->targetList);
            bool expr_is_distributable = IsTypeDistributable(exprType((Node*)linitial(agg_costs->exprAggs)));

            /* case2 */
            if (!grp_is_distributable) {
                if (expr_is_distributable) {
                    return true;
                } else { /* case3 */
                    errno_t sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
                        NOTPLANSHIPPING_LENGTH,
                        "\"Count(Distinct)\" on redistribution unsupported data type");
                    securec_check_ss_c(sprintf_rc, "\0", "\0");
                    mark_stream_unsupport();
                }
            }
        }
#endif
    }

    /* Prefer hashagg or sort when guc is set */
    if (!u_sess->attr.attr_sql.enable_hashagg && u_sess->attr.attr_sql.enable_sort)
        return false;
    if (!u_sess->attr.attr_sql.enable_sort && u_sess->attr.attr_sql.enable_hashagg)
        return true;

    /* If guc plan_mode_seed is random plan, we should choose random path between AGG_HASHED and AGG_SORTED */
    if (u_sess->attr.attr_sql.plan_mode_seed != OPTIMIZE_PLAN) {
        int random_option = choose_random_option(lengthof(g_agglist));
        return (AGG_HASHED == g_agglist[random_option]);
    }

    hashentrysize = compute_hash_entry_size(root->glob->vectorized, cheapest_path, path_width, agg_costs);
    *hash_entry_size = hashentrysize;

    /*
     * When we have both GROUP BY and DISTINCT, use the more-rigorous of
     * DISTINCT and ORDER BY as the assumed required output sort order. This
     * is an oversimplification because the DISTINCT might get implemented via
     * hashing, but it's not clear that the case is common enough (or that our
     * estimates are good enough) to justify trying to solve it exactly.
     */
    target_pathkeys = list_length(root->distinct_pathkeys) > list_length(root->sort_pathkeys) ?
        root->distinct_pathkeys : root->sort_pathkeys;

    /* init hash path and sort path. */
    rc = memset_s(&hashed_p, sizeof(hashed_p), 0, sizeof(hashed_p));
    securec_check(rc, "\0", "\0");
    rc = memset_s(&sorted_p, sizeof(sorted_p), 0, sizeof(sorted_p));
    securec_check(rc, "\0", "\0");

    /* compute the minimal total cost for hash path. */
    Distribution* distribution = ng_get_dest_distribution(cheapest_path);
    ng_copy_distribution(&hashed_p.distribution, distribution);
    compute_hashed_path_cost(root,
        limit_tuples,
        path_width,
        cheapest_path,
        dNumGroups,
        agg_costs,
        hashentrysize,
        target_pathkeys,
        &hashed_p);

    /* compute the minimal total cost for sort path. */
    compute_sorted_path_cost(root,
        limit_tuples,
        path_width,
        cheapest_path,
        sorted_path,
        dNumGroups,
        agg_costs,
        hashentrysize,
        target_pathkeys,
        &sorted_p);

    /*
     * Now make the decision using the top-level tuple fraction.  First we
     * have to convert an absolute count (LIMIT) into fractional form.
     */
    tuple_fraction = tuple_fraction >= 1.0 ? tuple_fraction / dNumGroups[0] : tuple_fraction;

    if (compare_fractional_path_costs(&hashed_p, &sorted_p, tuple_fraction) < 0) {
        /* Hashed is cheaper, so use it */
        return true;
    }
    return false;
}

static void compute_distinct_sorted_path_cost(Path* sorted_p, List* sorted_pathkeys, Query* parse, PlannerInfo* root, 
    int numDistinctCols, Cost sorted_startup_cost, Cost sorted_total_cost, double path_rows, 
    Distribution* sorted_distribution, int path_width, double dNumDistinctRows, double limit_tuples)
{
    List* current_pathkeys = NIL;
    List* needed_pathkeys = NIL;

    sorted_p->startup_cost = sorted_startup_cost;
    sorted_p->total_cost = sorted_total_cost;
    ng_copy_distribution(&sorted_p->distribution, sorted_distribution);
    current_pathkeys = sorted_pathkeys;
    if (parse->hasDistinctOn && list_length(root->distinct_pathkeys) < list_length(root->sort_pathkeys)) {
        needed_pathkeys = root->sort_pathkeys;
    } else {
        needed_pathkeys = root->distinct_pathkeys;
    }
    if (!pathkeys_contained_in(needed_pathkeys, current_pathkeys)) {
        if (list_length(root->distinct_pathkeys) >= list_length(root->sort_pathkeys)) {
            current_pathkeys = root->distinct_pathkeys;
        } else {
            current_pathkeys = root->sort_pathkeys;
        }
        cost_sort(sorted_p,
            current_pathkeys,
            sorted_p->total_cost,
            path_rows,
            path_width,
            0.0,
            u_sess->opt_cxt.op_work_mem,
            -1.0,
            root->glob->vectorized);
    }
    cost_group(
        sorted_p, root, numDistinctCols, dNumDistinctRows, sorted_p->startup_cost, sorted_p->total_cost, path_rows);
    if (parse->sortClause && !pathkeys_contained_in(root->sort_pathkeys, current_pathkeys)) {
        cost_sort(sorted_p,
            root->sort_pathkeys,
            sorted_p->total_cost,
            dNumDistinctRows,
            path_width,
            0.0,
            u_sess->opt_cxt.op_work_mem,
            limit_tuples,
            root->glob->vectorized);
    }
}

/*
 * choose_hashed_distinct - should we use hashing for DISTINCT?
 *
 * This is fairly similar to choose_hashed_grouping, but there are enough
 * differences that it doesn't seem worth trying to unify the two functions.
 * (One difference is that we sometimes apply this after forming a Plan,
 * so the input alternatives can't be represented as Paths --- instead we
 * pass in the costs as individual variables.)
 *
 * But note that making the two choices independently is a bit bogus in
 * itself.	If the two could be combined into a single choice operation
 * it'd probably be better, but that seems far too unwieldy to be practical,
 * especially considering that the combination of GROUP BY and DISTINCT
 * isn't very common in real queries.  By separating them, we are giving
 * extra preference to using a sorting implementation when a common sort key
 * is available ... and that's not necessarily wrong anyway.
 *
 * Returns TRUE to select hashing, FALSE to select sorting.
 */
static bool choose_hashed_distinct(PlannerInfo* root, double tuple_fraction, double limit_tuples, double path_rows,
    int path_width, Cost cheapest_startup_cost, Cost cheapest_total_cost, Distribution* cheapest_distribution,
    Cost sorted_startup_cost, Cost sorted_total_cost, Distribution* sorted_distribution, List* sorted_pathkeys,
    double dNumDistinctRows, Size hashentrysize)
{
    Query* parse = root->parse;
    int numDistinctCols = list_length(parse->distinctClause);
    bool can_sort = false;
    bool can_hash = false;
    Path hashed_p;
    Path sorted_p;

    errno_t rc = EOK;
    rc = memset_s(&hashed_p, sizeof(Path), 0, sizeof(Path));
    securec_check(rc, "\0", "\0");
    rc = memset_s(&sorted_p, sizeof(Path), 0, sizeof(Path));
    securec_check(rc, "\0", "\0");

    /*
     * If we have a sortable DISTINCT ON clause, we always use sorting. This
     * enforces the expected behavior of DISTINCT ON.
     */
    can_sort = grouping_is_sortable(parse->distinctClause);
    if (can_sort && parse->hasDistinctOn)
        return false;

    can_hash = grouping_is_hashable(parse->distinctClause);

    /* Quick out if only one choice is workable */
    if (!(can_hash && can_sort)) {
        if (can_hash) {
            return true;
        } else if (can_sort) {
            return false;
        } else {
            ereport(ERROR,
                (errmodule(MOD_OPT_PLANNER), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("DISTINCT cannot be implemented."),
                    errdetail("Some of the datatypes only support hashing, "
                        "while others only support sorting."),
                    errcause("DISTINCT uses unsupported datatypes."),
                    erraction("Modify SQL statement according to the manual.")));
        }
    }

    /* Prefer hashagg or sort when guc is set */
    if (!u_sess->attr.attr_sql.enable_hashagg && u_sess->attr.attr_sql.enable_sort)
        return false;
    if (!u_sess->attr.attr_sql.enable_sort && u_sess->attr.attr_sql.enable_hashagg)
        return true;

    /* If guc plan_mode_seed is random plan, we should choose random path between AGG_HASHED and AGG_SORTED */
    if (u_sess->attr.attr_sql.plan_mode_seed != OPTIMIZE_PLAN) {
        int random_option = choose_random_option(lengthof(g_agglist));
        return (AGG_HASHED == g_agglist[random_option]);
    }

    /*
     * See if the estimated cost is no more than doing it the other way. While
     * avoiding the need for sorted input is usually a win, the fact that the
     * output won't be sorted may be a loss; so we need to do an actual cost
     * comparison.
     *
     * We need to consider cheapest_path + hashagg [+ final sort] versus
     * sorted_path [+ sort] + group [+ final sort] where brackets indicate a
     * step that may not be needed.
     *
     * These path variables are dummies that just hold cost fields; we don't
     * make actual Paths for these steps.
     */
    ng_copy_distribution(&hashed_p.distribution, cheapest_distribution);
    cost_agg(&hashed_p,
        root,
        AGG_HASHED,
        NULL,
        numDistinctCols,
        dNumDistinctRows,
        cheapest_startup_cost,
        cheapest_total_cost,
        path_rows,
        path_width,
        hashentrysize);

    /*
     * Result of hashed agg is always unsorted, so if ORDER BY is present we
     * need to charge for the final sort.
     */
    if (parse->sortClause)
        cost_sort(&hashed_p,
            root->sort_pathkeys,
            hashed_p.total_cost,
            dNumDistinctRows,
            path_width,
            0.0,
            u_sess->opt_cxt.op_work_mem,
            limit_tuples,
            root->glob->vectorized);

    /*
     * Now for the GROUP case.	See comments in grouping_planner about the
     * sorting choices here --- this code should match that code.
     */
    compute_distinct_sorted_path_cost(&sorted_p, 
        sorted_pathkeys,
        parse,
        root,
        numDistinctCols,
        sorted_startup_cost,
        sorted_total_cost,
        path_rows,
        sorted_distribution,
        path_width,
        dNumDistinctRows,
        limit_tuples);

    /*
     * Now make the decision using the top-level tuple fraction.  First we
     * have to convert an absolute count (LIMIT) into fractional form.
     */
    tuple_fraction = tuple_fraction >= 1.0 ? tuple_fraction / dNumDistinctRows : tuple_fraction;

    if (compare_fractional_path_costs(&hashed_p, &sorted_p, tuple_fraction) < 0) {
        /* Hashed is cheaper, so use it */
        return true;
    }
    return false;
}

/*
 * make_subplanTargetList
 *	  Generate appropriate target list when grouping is required.
 *
 * When grouping_planner inserts grouping or aggregation plan nodes
 * above the scan/join plan constructed by query_planner+create_plan,
 * we typically want the scan/join plan to emit a different target list
 * than the outer plan nodes should have.  This routine generates the
 * correct target list for the scan/join subplan.
 *
 * The initial target list passed from the parser already contains entries
 * for all ORDER BY and GROUP BY expressions, but it will not have entries
 * for variables used only in HAVING clauses; so we need to add those
 * variables to the subplan target list.  Also, we flatten all expressions
 * except GROUP BY items into their component variables; the other expressions
 * will be computed by the inserted nodes rather than by the subplan.
 * For example, given a query like
 *		SELECT a+b,SUM(c+d) FROM table GROUP BY a+b;
 * we want to pass this targetlist to the subplan:
 *		a+b,c,d
 * where the a+b target will be used by the Sort/Group steps, and the
 * other targets will be used for computing the final results.
 *
 * If we are grouping or aggregating, *and* there are no non-Var grouping
 * expressions, then the returned tlist is effectively dummy; we do not
 * need to force it to be evaluated, because all the Vars it contains
 * should be present in the "flat" tlist generated by create_plan, though
 * possibly in a different order.  In that case we'll use create_plan's tlist,
 * and the tlist made here is only needed as input to query_planner to tell
 * it which Vars are needed in the output of the scan/join plan.
 *
 * 'tlist' is the query's target list.
 * 'groupColIdx' receives an array of column numbers for the GROUP BY
 *			expressions (if there are any) in the returned target list.
 * 'need_tlist_eval' is set true if we really need to evaluate the
 *			returned tlist as-is.
 *
 * The result is the targetlist to be passed to query_planner.
 */
static List* make_subplanTargetList(PlannerInfo* root, List* tlist, AttrNumber** groupColIdx, bool* need_tlist_eval)
{
    Query* parse = root->parse;
    List* sub_tlist = NIL;
    List* non_group_cols = NIL;
    List* non_group_vars = NIL;
    int numCols;

    *groupColIdx = NULL;

    /*
     * If we're not grouping or aggregating, there's nothing to do here;
     * query_planner should receive the unmodified target list.
     */
    if (!parse->hasAggs && !parse->groupClause && !parse->groupingSets && !root->hasHavingQual &&
        !parse->hasWindowFuncs) {
        *need_tlist_eval = true;
        return tlist;
    }

    /*
     * Otherwise, we must build a tlist containing all grouping columns, plus
     * any other Vars mentioned in the targetlist and HAVING qual.
     */
    sub_tlist = NIL;
    non_group_cols = NIL;
    *need_tlist_eval = false; /* only eval if not flat tlist */

    get_tlist_group_vars_split(parse, tlist, &sub_tlist, &non_group_cols);

    numCols = list_length(parse->groupClause);
    if (numCols > 0) {
        /*
         * If grouping, create sub_tlist entries for all GROUP BY columns, and
         * make an array showing where the group columns are in the sub_tlist.
         *
         * Note: with this implementation, the array entries will always be
         * 1..N, but we don't want callers to assume that.
         */
        AttrNumber* grpColIdx = NULL;
        ListCell* tl = NULL;
        int i = 1;

        /* Reserve one position for adding groupingid column for grouping set query */
        if (parse->groupingSets)
            grpColIdx = (AttrNumber*)palloc0(sizeof(AttrNumber) * (numCols + 1));
        else
            grpColIdx = (AttrNumber*)palloc0(sizeof(AttrNumber) * numCols);
        *groupColIdx = grpColIdx;

        foreach (tl, sub_tlist) {
            TargetEntry* tle = (TargetEntry*)lfirst(tl);
            int colno;

            colno = get_grouping_column_index(parse, tle, parse->groupClause);

            AssertEreport(colno >= 0, MOD_OPT, "invalid GROUP BY column position.");

            /*
             * It's a grouping column, so add it to the result tlist and
             * remember its resno in grpColIdx[].
             */
            TargetEntry* newtle = NULL;

            newtle = makeTargetEntry(tle->expr, i++, NULL, false);
            newtle->ressortgroupref = tle->ressortgroupref;

            lfirst(tl) = newtle;

            AssertEreport(grpColIdx[colno] == 0,
                MOD_OPT,
                "invalid grpColIdx item when adding a grouping column to the result tlist."); /* no dups expected */
            grpColIdx[colno] = newtle->resno;

            if (!(newtle->expr && IsA(newtle->expr, Var)))
                *need_tlist_eval = true; /* tlist contains non Vars */
        }
    }

    /*
     * Pull out all the Vars mentioned in non-group cols (plus HAVING), and
     * add them to the result tlist if not already present.  (A Var used
     * directly as a GROUP BY item will be present already.)  Note this
     * includes Vars used in resjunk items, so we are covering the needs of
     * ORDER BY and window specifications.	Vars used within Aggrefs will be
     * pulled out here, too.
     */
    non_group_vars = pull_var_clause((Node*)non_group_cols, PVC_RECURSE_AGGREGATES, PVC_INCLUDE_PLACEHOLDERS);
    sub_tlist = add_to_flat_tlist(sub_tlist, non_group_vars);

    /* clean up cruft */
    list_free_ext(non_group_vars);
    list_free_ext(non_group_cols);

    return sub_tlist;
}

/*
 * build_grouping_itst_keys
 *	we add group by items to superset keys, so redistribution during agg is eliminated
 *	if superset key path is chosen
 *
 * Parameters:
 *	@in root: planner info struct for current query level
 *	@in active_windosws: in-use window funcs in current query level
 */
static void build_grouping_itst_keys(PlannerInfo* root, List* active_windows)
{
    Query* parse = root->parse;
    List* targetlist = parse->targetList;
    List* groupClause = NIL;
    List* superset_keys = NIL;
    ListCell* lc = NULL;

    /* reset for superset key of current query level */
    root->dis_keys.superset_keys = NIL;

    /* find the bottom level group key, except alway redistributed groupingsets */
    if (parse->groupingSets != NIL)
        groupClause = NIL;
    else if (parse->groupClause != NIL)
        groupClause = parse->groupClause;
    else if (active_windows != NIL) {
        WindowClause* wc = (WindowClause*)linitial(active_windows);
        groupClause = wc->partitionClause;
    } else if (parse->distinctClause != NIL)
        groupClause = parse->distinctClause;

    /* find corresponding super set key from group by clause */
    if (groupClause != NIL) {
        foreach (lc, targetlist) {
            TargetEntry* tle = (TargetEntry*)lfirst(lc);
            int colno;

            colno = get_grouping_column_index(parse, tle, groupClause);
            /* it's group by expr if colno no less then 0 */
            if (colno >= 0 && IsTypeDistributable(exprType((Node*)tle->expr))) {
                superset_keys = list_append_unique(superset_keys, tle->expr);
            }
        }
    }

    if (superset_keys != NIL)
        root->dis_keys.superset_keys = list_make1(superset_keys);
}

/*
 * locate_grouping_columns
 *		Locate grouping columns in the tlist chosen by create_plan.
 *
 * This is only needed if we don't use the sub_tlist chosen by
 * make_subplanTargetList.	We have to forget the column indexes found
 * by that routine and re-locate the grouping exprs in the real sub_tlist.
 */
static void locate_grouping_columns(PlannerInfo* root, List* tlist, List* sub_tlist, AttrNumber* groupColIdx)
{
    int keyno = 0;
    ListCell* gl = NULL;

    /*
     * No work unless grouping.
     */
    if (!root->parse->groupClause) {
        AssertEreport(groupColIdx == NULL,
            MOD_OPT,
            "invalid group column index when locating grouping columns in the target list.");
        return;
    }
    AssertEreport(
        groupColIdx != NULL, MOD_OPT, "invalid group column index when locating grouping columns in the target list.");

    foreach (gl, root->parse->groupClause) {
        SortGroupClause* grpcl = (SortGroupClause*)lfirst(gl);
        Node* groupexpr = get_sortgroupclause_expr(grpcl, tlist);

        TargetEntry* te = tlist_member(groupexpr, sub_tlist);

        if (te == NULL)
            ereport(ERROR,
                (errmodule(MOD_OPT_PLANNER), errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                    errmsg("Failed to locate grouping columns."),
                    errdetail("N/A"),
                    errcause("System error."),
                    erraction("Contact Huawei Engineer.")));

        groupColIdx[keyno++] = te->resno;
    }
}

/*
 * postprocess_setop_tlist
 *	  Fix up targetlist returned by plan_set_operations().
 *
 * We need to transpose sort key info from the orig_tlist into new_tlist.
 * NOTE: this would not be good enough if we supported resjunk sort keys
 * for results of set operations --- then, we'd need to project a whole
 * new tlist to evaluate the resjunk columns.  For now, just ereport if we
 * find any resjunk columns in orig_tlist.
 */
static List* postprocess_setop_tlist(List* new_tlist, List* orig_tlist)
{
    ListCell* l = NULL;
    ListCell* orig_tlist_item = list_head(orig_tlist);

    foreach (l, new_tlist) {
        TargetEntry* new_tle = (TargetEntry*)lfirst(l);
        TargetEntry* orig_tle = NULL;

        /* ignore resjunk columns in setop result */
        if (new_tle->resjunk)
            continue;

        AssertEreport(orig_tlist_item != NULL, MOD_OPT, "invalid origin targetlist item when fixing up targetlist.");
        orig_tle = (TargetEntry*)lfirst(orig_tlist_item);
        orig_tlist_item = lnext(orig_tlist_item);
        if (orig_tle->resjunk) /* should not happen */
            ereport(ERROR,
                (errmodule(MOD_OPT_PLANNER), errcode(ERRCODE_CASE_NOT_FOUND),
                    errmsg("Resjunk output columns are not implemented."),
                    errdetail("N/A"),
                    errcause("System error."),
                    erraction("Contact Huawei Engineer.")));

        AssertEreport(new_tle->resno == orig_tle->resno,
            MOD_OPT,
            "The resno of new target entry does not match to the resno of origin target entry.");
        new_tle->ressortgroupref = orig_tle->ressortgroupref;
    }

    if (orig_tlist_item != NULL) {
        TargetEntry *extTle = (TargetEntry *)lfirst(orig_tlist_item);

        /*
         * In case of start with debug, we add a pseudo return column under CteScan plan,
         * so we don't process such kind of exception case, once we verify the extra tle
         * is a pseudo return column, then pass new_tlist as return value.
         */
        if (IsPseudoReturnTargetEntry(extTle)) {
            return new_tlist;
        }

        ereport(ERROR,
            (errmodule(MOD_OPT_PLANNER), errcode(ERRCODE_CASE_NOT_FOUND),
                errmsg("Resjunk output columns are not implemented."),
                errdetail("N/A"),
                errcause("System error."),
                erraction("Contact Huawei Engineer.")));
    }
    return new_tlist;
}

/*
 * select_active_windows
 *		Create a list of the "active" window clauses (ie, those referenced
 *		by non-deleted WindowFuncs) in the order they are to be executed.
 */
void select_active_windows(PlannerInfo* root, WindowLists* wflists)
{
    List* actives = NIL;
    ListCell* lc = NULL;

    /* First, make a list of the active windows */
    actives = NIL;
    foreach (lc, root->parse->windowClause) {
        WindowClause* wc = (WindowClause*)lfirst(lc);

        /* It's only active if wflists shows some related WindowFuncs */
        AssertEreport(
            wc->winref <= wflists->maxWinRef, MOD_OPT, "the window function index is out of range of wflists");
        if (wflists->windowFuncs[wc->winref] != NIL)
            actives = lappend(actives, wc);
    }

    /*
     * Now, ensure that windows with identical partitioning/ordering clauses
     * are adjacent in the list.  This is required by the SQL standard, which
     * says that only one sort is to be used for such windows, even if they
     * are otherwise distinct (eg, different names or framing clauses).
     *
     * There is room to be much smarter here, for example detecting whether
     * one window's sort keys are a prefix of another's (so that sorting for
     * the latter would do for the former), or putting windows first that
     * match a sort order available for the underlying query.  For the moment
     * we are content with meeting the spec.
     */
    while (actives != NIL) {
        WindowClause* wc = (WindowClause*)linitial(actives);
        ListCell* prev = NULL;
        ListCell* next = NULL;

        /* Move wc from actives to wflists->activeWindows */
        actives = list_delete_first(actives);
        wflists->activeWindows = lappend(wflists->activeWindows, wc);

        /* Now move any matching windows from actives to wflists->activeWindows */
        prev = NULL;
        for (lc = list_head(actives); lc; lc = next) {
            WindowClause* wc2 = (WindowClause*)lfirst(lc);

            next = lnext(lc);
            /* framing options are NOT to be compared here! */
            if (equal(wc->partitionClause, wc2->partitionClause) && equal(wc->orderClause, wc2->orderClause)) {
                actives = list_delete_cell(actives, lc, prev);
                wflists->activeWindows = lappend(wflists->activeWindows, wc2);
            } else
                prev = lc;
        }
    }
}

/*
 * make_windowInputTargetList
 *	  Generate appropriate target list for initial input to WindowAgg nodes.
 *
 * When grouping_planner inserts one or more WindowAgg nodes into the plan,
 * this function computes the initial target list to be computed by the node
 * just below the first WindowAgg.  This list must contain all values needed
 * to evaluate the window functions, compute the final target list, and
 * perform any required final sort step.  If multiple WindowAggs are needed,
 * each intermediate one adds its window function results onto this tlist;
 * only the topmost WindowAgg computes the actual desired target list.
 *
 * This function is much like make_subplanTargetList, though not quite enough
 * like it to share code.  As in that function, we flatten most expressions
 * into their component variables.  But we do not want to flatten window
 * PARTITION BY/ORDER BY clauses, since that might result in multiple
 * evaluations of them, which would be bad (possibly even resulting in
 * inconsistent answers, if they contain volatile functions).  Also, we must
 * not flatten GROUP BY clauses that were left unflattened by
 * make_subplanTargetList, because we may no longer have access to the
 * individual Vars in them.
 *
 * Another key difference from make_subplanTargetList is that we don't flatten
 * Aggref expressions, since those are to be computed below the window
 * functions and just referenced like Vars above that.
 *
 * 'tlist' is the query's final target list.
 * 'activeWindows' is the list of active windows previously identified by
 *			select_active_windows.
 *
 * The result is the targetlist to be computed by the plan node immediately
 * below the first WindowAgg node.
 */
static List* make_windowInputTargetList(PlannerInfo* root, List* tlist, List* activeWindows)
{
    Query* parse = root->parse;
    Bitmapset* sgrefs = NULL;
    List* new_tlist = NIL;
    List* flattenable_cols = NIL;
    List* flattenable_vars = NIL;
    ListCell* lc = NULL;

    AssertEreport(parse->hasWindowFuncs,
        MOD_OPT,
        "the window function is empty"
        "when generating appropriate target list for initial input to WindowAgg nodes.");

    /*
     * Collect the sortgroupref numbers of window PARTITION/ORDER BY clauses
     * into a bitmapset for convenient reference below.
     */
    sgrefs = NULL;
    foreach (lc, activeWindows) {
        WindowClause* wc = (WindowClause*)lfirst(lc);
        ListCell* lc2 = NULL;

        foreach (lc2, wc->partitionClause) {
            SortGroupClause* sortcl = (SortGroupClause*)lfirst(lc2);

            sgrefs = bms_add_member(sgrefs, sortcl->tleSortGroupRef);
        }
        foreach (lc2, wc->orderClause) {
            SortGroupClause* sortcl = (SortGroupClause*)lfirst(lc2);

            sgrefs = bms_add_member(sgrefs, sortcl->tleSortGroupRef);
        }
    }

    /* Add in sortgroupref numbers of GROUP BY clauses, too */
    foreach (lc, parse->groupClause) {
        SortGroupClause* grpcl = (SortGroupClause*)lfirst(lc);

        sgrefs = bms_add_member(sgrefs, grpcl->tleSortGroupRef);
    }

    /*
     * Construct a tlist containing all the non-flattenable tlist items, and
     * save aside the others for a moment.
     */
    new_tlist = NIL;
    flattenable_cols = NIL;

    foreach (lc, tlist) {
        TargetEntry* tle = (TargetEntry*)lfirst(lc);

        /*
         * Don't want to deconstruct window clauses or GROUP BY items.  (Note
         * that such items can't contain window functions, so it's okay to
         * compute them below the WindowAgg nodes.)
         */
        if (tle->ressortgroupref != 0 && bms_is_member(tle->ressortgroupref, sgrefs)) {
            /* Don't want to deconstruct this value, so add to new_tlist */
            TargetEntry* newtle = NULL;

            newtle = makeTargetEntry(tle->expr, list_length(new_tlist) + 1, NULL, false);
            /* Preserve its sortgroupref marking, in case it's volatile */
            newtle->ressortgroupref = tle->ressortgroupref;
            new_tlist = lappend(new_tlist, newtle);
        } else {
            /*
             * Column is to be flattened, so just remember the expression for
             * later call to pull_var_clause.  There's no need for
             * pull_var_clause to examine the TargetEntry node itself.
             */
            flattenable_cols = lappend(flattenable_cols, tle->expr);
        }
    }

    /*
     * Pull out all the Vars and Aggrefs mentioned in flattenable columns, and
     * add them to the result tlist if not already present.  (Some might be
     * there already because they're used directly as window/group clauses.)
     *
     * Note: it's essential to use PVC_INCLUDE_AGGREGATES here, so that the
     * Aggrefs are placed in the Agg node's tlist and not left to be computed
     * at higher levels.
     */
    flattenable_vars = pull_var_clause((Node*)flattenable_cols, PVC_INCLUDE_AGGREGATES, PVC_INCLUDE_PLACEHOLDERS);

    /*
     * If there's a group by with expression, we don't have single var in targetlist of
     * lefttree, so we'll not add it to new targetlist, like the following example, column
     * a will not be added to the targetlist.
     *	select coalesce(a, 1), rank() over (partition by b) from t group by coalesce(a, 1), b;
     *
     * However, if group by on unique columns, any column is allowed to add to the targetlist,
     * so handle this special case here.
     */
    if (parse->groupClause != NIL) {
        List* dep_oids = get_parse_dependency_rel_list(parse->constraintDeps);
        List* flattenable_vars_final = NIL;

        foreach (lc, flattenable_vars) {
            Node* node = (Node*)lfirst(lc);

            if (IsA(node, Var)) {
                if (!tlist_member(node, new_tlist)) {
                    if (var_from_dependency_rel(parse, (Var *)node, dep_oids) ||
                        var_from_sublink_pulluped(parse, (Var *) node)) {
                        flattenable_vars_final = lappend(flattenable_vars_final, node);
                    }
                }
            } else
                flattenable_vars_final = lappend(flattenable_vars_final, node);
        }
        new_tlist = add_to_flat_tlist(new_tlist, flattenable_vars_final);
        list_free_ext(flattenable_vars_final);
    } else
        new_tlist = add_to_flat_tlist(new_tlist, flattenable_vars);

    /* clean up cruft */
    list_free_ext(flattenable_vars);
    list_free_ext(flattenable_cols);

    return new_tlist;
}

/*
 * make_pathkeys_for_window
 *		Create a pathkeys list describing the required input ordering
 *		for the given WindowClause.
 *
 * The required ordering is first the PARTITION keys, then the ORDER keys.
 * In the future we might try to implement windowing using hashing, in which
 * case the ordering could be relaxed, but for now we always sort.
 */
List* make_pathkeys_for_window(PlannerInfo* root, WindowClause* wc, List* tlist, bool canonicalize)
{
    List* window_pathkeys = NIL;
    List* window_sortclauses = NIL;

    /* Throw error if can't sort */
    if (!grouping_is_sortable(wc->partitionClause))
        ereport(ERROR,
            (errmodule(MOD_OPT_PLANNER), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("PARTITION BY cannot be implemented."),
                errdetail("Window partitioning columns must be of sortable datatypes."),
                errcause("PARTITION BY uses unsupported datatypes."),
                erraction("Modify SQL statement according to the manual.")));
    if (!grouping_is_sortable(wc->orderClause))
        ereport(ERROR,
            (errmodule(MOD_OPT_PLANNER), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("ORDER BY cannot be implemented."),
                errdetail("Window partitioning columns must be of sortable datatypes."),
                errcause("ORDER BY uses unsupported datatypes."),
                erraction("Modify SQL statement according to the manual.")));

    /* Okay, make the combined pathkeys */
    window_sortclauses = list_concat(list_copy(wc->partitionClause), list_copy(wc->orderClause));
    window_pathkeys = make_pathkeys_for_sortclauses(root, window_sortclauses, tlist, canonicalize);
    list_free_ext(window_sortclauses);
    return window_pathkeys;
}

/* ----------
 * get_column_info_for_window
 *		Get the partitioning/ordering column numbers and equality operators
 *		for a WindowAgg node.
 *
 * This depends on the behavior of make_pathkeys_for_window()!
 *
 * We are given the target WindowClause and an array of the input column
 * numbers associated with the resulting pathkeys.	In the easy case, there
 * are the same number of pathkey columns as partitioning + ordering columns
 * and we just have to copy some data around.  However, it's possible that
 * some of the original partitioning + ordering columns were eliminated as
 * redundant during the transformation to pathkeys.  (This can happen even
 * though the parser gets rid of obvious duplicates.  A typical scenario is a
 * window specification "PARTITION BY x ORDER BY y" coupled with a clause
 * "WHERE x = y" that causes the two sort columns to be recognized as
 * redundant.)	In that unusual case, we have to work a lot harder to
 * determine which keys are significant.
 *
 * The method used here is a bit brute-force: add the sort columns to a list
 * one at a time and note when the resulting pathkey list gets longer.	But
 * it's a sufficiently uncommon case that a faster way doesn't seem worth
 * the amount of code refactoring that'd be needed.
 * ----------
 */
static void get_column_info_for_window(PlannerInfo* root, WindowClause* wc, List* tlist, int numSortCols,
    AttrNumber* sortColIdx, int* partNumCols, AttrNumber** partColIdx, Oid** partOperators, int* ordNumCols,
    AttrNumber** ordColIdx, Oid** ordOperators)
{
    int numPart = list_length(wc->partitionClause);
    int numOrder = list_length(wc->orderClause);

    if (numSortCols == numPart + numOrder) {
        /* easy case */
        *partNumCols = numPart;
        *partColIdx = sortColIdx;
        *partOperators = extract_grouping_ops(wc->partitionClause);
        *ordNumCols = numOrder;
        *ordColIdx = sortColIdx + numPart;
        *ordOperators = extract_grouping_ops(wc->orderClause);
    } else {
        List* sortclauses = NIL;
        List* pathkeys = NIL;
        int scidx;
        ListCell* lc = NULL;

        /* first, allocate what's certainly enough space for the arrays */
        *partNumCols = 0;
        *partColIdx = (AttrNumber*)palloc(numPart * sizeof(AttrNumber));
        *partOperators = (Oid*)palloc(numPart * sizeof(Oid));
        *ordNumCols = 0;
        *ordColIdx = (AttrNumber*)palloc(numOrder * sizeof(AttrNumber));
        *ordOperators = (Oid*)palloc(numOrder * sizeof(Oid));
        sortclauses = NIL;
        pathkeys = NIL;
        scidx = 0;
        foreach (lc, wc->partitionClause) {
            SortGroupClause* sgc = (SortGroupClause*)lfirst(lc);
            List* new_pathkeys = NIL;

            sortclauses = lappend(sortclauses, sgc);
            new_pathkeys = make_pathkeys_for_sortclauses(root, sortclauses, tlist, true);
            if (list_length(new_pathkeys) > list_length(pathkeys)) {
                /* this sort clause is actually significant */
                (*partColIdx)[*partNumCols] = sortColIdx[scidx++];
                (*partOperators)[*partNumCols] = sgc->eqop;
                (*partNumCols)++;
                pathkeys = new_pathkeys;
            }
        }
        foreach (lc, wc->orderClause) {
            SortGroupClause* sgc = (SortGroupClause*)lfirst(lc);
            List* new_pathkeys = NIL;

            sortclauses = lappend(sortclauses, sgc);
            new_pathkeys = make_pathkeys_for_sortclauses(root, sortclauses, tlist, true);
            if (list_length(new_pathkeys) > list_length(pathkeys)) {
                /* this sort clause is actually significant */
                (*ordColIdx)[*ordNumCols] = sortColIdx[scidx++];
                (*ordOperators)[*ordNumCols] = sgc->eqop;
                (*ordNumCols)++;
                pathkeys = new_pathkeys;
            }
        }
        /* complain if we didn't eat exactly the right number of sort cols */
        if (scidx != numSortCols)
            ereport(ERROR,
                (errmodule(MOD_OPT_PLANNER), errcode(ERRCODE_INVALID_OPERATION),
                    errmsg("Failed to deconstruct sort operators into partitioning/ordering operators."),
                    errdetail("Deconstructed: %d, Total: %d", scidx, numSortCols),
                    errcause("System error."),
                    erraction("Contact Huawei Engineer.")));
    }
}

/*
 * expression_planner
 *		Perform planner's transformations on a standalone expression.
 *
 * Various utility commands need to evaluate expressions that are not part
 * of a plannable query.  They can do so using the executor's regular
 * expression-execution machinery, but first the expression has to be fed
 * through here to transform it from parser output to something executable.
 *
 * Currently, we disallow sublinks in standalone expressions, so there's no
 * real "planning" involved here.  (That might not always be true though.)
 * What we must do is run eval_const_expressions to ensure that any function
 * calls are converted to positional notation and function default arguments
 * get inserted.  The fact that constant subexpressions get simplified is a
 * side-effect that is useful when the expression will get evaluated more than
 * once.  Also, we must fix operator function IDs.
 *
 * Note: this must not make any damaging changes to the passed-in expression
 * tree.  (It would actually be okay to apply fix_opfuncids to it, but since
 * we first do an expression_tree_mutator-based walk, what is returned will
 * be a new node tree.)
 */
Expr* expression_planner(Expr* expr)
{
    Node* result = NULL;

    /*
     * Convert named-argument function calls, insert default arguments and
     * simplify constant subexprs
     */
    result = eval_const_expressions(NULL, (Node*)expr);

    /* Fill in opfuncid values if missing */
    fix_opfuncids(result);

    return (Expr*)result;
}

/*
 * plan_cluster_use_sort
 *		Use the planner to decide how CLUSTER should implement sorting
 *
 * tableOid is the OID of a table to be clustered on its index indexOid
 * (which is already known to be a btree index).  Decide whether it's
 * cheaper to do an indexscan or a seqscan-plus-sort to execute the CLUSTER.
 * Return TRUE to use sorting, FALSE to use an indexscan.
 *
 * Note: caller had better already hold some type of lock on the table.
 */
bool plan_cluster_use_sort(Oid tableOid, Oid indexOid)
{
    PlannerInfo* root = NULL;
    Query* query = NULL;
    PlannerGlobal* glob = NULL;
    RangeTblEntry* rte = NULL;
    RelOptInfo* rel = NULL;
    IndexOptInfo* indexInfo = NULL;
    QualCost indexExprCost;
    Cost comparisonCost;
    Path* seqScanPath = NULL;
    Path seqScanAndSortPath;
    IndexPath* indexScanPath = NULL;
    ListCell* lc = NULL;

    /* Set up mostly-dummy planner state */
    query = makeNode(Query);
    query->commandType = CMD_SELECT;

    glob = makeNode(PlannerGlobal);

    root = makeNode(PlannerInfo);
    root->parse = query;
    root->glob = glob;
    root->query_level = 1;
    root->planner_cxt = CurrentMemoryContext;
    root->wt_param_id = -1;

    /* Build a minimal RTE for the rel */
    rte = makeNode(RangeTblEntry);
    rte->rtekind = RTE_RELATION;
    rte->relid = tableOid;
    rte->relkind = RELKIND_RELATION;
    rte->lateral = false;
    rte->inh = false;
    rte->inFromCl = true;
    query->rtable = list_make1(rte);

    /* Set up RTE/RelOptInfo arrays */
    setup_simple_rel_arrays(root);

    /* Build RelOptInfo */
    rel = build_simple_rel(root, 1, RELOPT_BASEREL);

    /* Locate IndexOptInfo for the target index */
    indexInfo = NULL;
    foreach (lc, rel->indexlist) {
        indexInfo = (IndexOptInfo*)lfirst(lc);
        if (indexInfo->indexoid == indexOid)
            break;
    }

    /*
     * It's possible that get_relation_info did not generate an IndexOptInfo
     * for the desired index; this could happen if it's not yet reached its
     * indcheckxmin usability horizon, or if it's a system index and we're
     * ignoring system indexes.  In such cases we should tell CLUSTER to not
     * trust the index contents but use seqscan-and-sort.
     */
    if (lc == NULL)  /* not in the list? */
        return true; /* use sort */

    /*
     * Rather than doing all the pushups that would be needed to use
     * set_baserel_size_estimates, just do a quick hack for rows and width.
     */
    rel->rows = rel->tuples;
    rel->width = get_relation_data_width(tableOid, InvalidOid, NULL);

    root->total_table_pages = rel->pages;

    /*
     * Determine eval cost of the index expressions, if any.  We need to
     * charge twice that amount for each tuple comparison that happens during
     * the sort, since tuplesort.c will have to re-evaluate the index
     * expressions each time.  (XXX that's pretty inefficient...)
     */
    cost_qual_eval(&indexExprCost, indexInfo->indexprs, root);
    comparisonCost = 2.0 * (indexExprCost.startup + indexExprCost.per_tuple);

    /* Estimate the cost of seq scan + sort */
    seqScanPath = create_seqscan_path(root, rel, NULL);
    cost_sort(&seqScanAndSortPath,
        NIL,
        seqScanPath->total_cost,
        RELOPTINFO_LOCAL_FIELD(root, rel, rows),
        rel->width,
        comparisonCost,
        u_sess->attr.attr_memory.maintenance_work_mem,
        -1.0,
        (rel->orientation != REL_ROW_ORIENTED));

    /* Estimate the cost of index scan */
    indexScanPath = create_index_path(root, indexInfo, NIL, NIL, NIL, NIL, NIL, ForwardScanDirection, false, NULL, NULL, 1.0);

    return (seqScanAndSortPath.total_cost < indexScanPath->path.total_cost);
}

/*
 * @@GaussDB@@
 * Target       : data partition
 * Brief        : Use the planner to decide how CLUSTER should implement sorting
 * Description  :
 * Notes        : caller had better already hold some type of lock on the table.
 * Input        :
 * Output       : Return TRUE to use sorting, FALSE to use an indexscan.
 */
bool planClusterPartitionUseSort(Relation partRel, Oid indexOid, PlannerInfo* root, RelOptInfo* relOptInfo)
{
    IndexOptInfo* indexInfo = NULL;
    QualCost indexExprCost;
    Cost comparisonCost = 0;
    Path* seqScanPath = NULL;
    Path seqScanAndSortPath;
    IndexPath* indexScanPath = NULL;
    ListCell* lc = NULL;

    /* Locate IndexOptInfo for the target index */
    indexInfo = NULL;
    foreach (lc, relOptInfo->indexlist) {
        indexInfo = (IndexOptInfo*)lfirst(lc);
        if (indexInfo->indexoid == indexOid) {
            break;
        }
    }

    /*
     * It's possible that get_relation_info did not generate an IndexOptInfo
     * for the desired index; this could happen if it's not yet reached its
     * indcheckxmin usability horizon, or if it's a system index and we're
     * ignoring system indexes.  In such cases we should tell CLUSTER to not
     * trust the index contents but use seqscan-and-sort.
     */
    if (lc == NULL) { /* not in the list? */
        return true;  /* use sort */
    }

    /*
     * Determine eval cost of the index expressions, if any.  We need to
     * charge twice that amount for each tuple comparison that happens during
     * the sort, since tuplesort.c will have to re-evaluate the index
     * expressions each time.  (XXX that's pretty inefficient...)
     */
    cost_qual_eval(&indexExprCost, indexInfo->indexprs, root);
    comparisonCost = 2.0 * (indexExprCost.startup + indexExprCost.per_tuple);

    /* Estimate the cost of seq scan + sort */
    seqScanPath = create_seqscan_path(root, relOptInfo, NULL);
    cost_sort(&seqScanAndSortPath,
        NIL,
        seqScanPath->total_cost,
        relOptInfo->tuples,
        relOptInfo->width,
        comparisonCost,
        u_sess->attr.attr_memory.maintenance_work_mem,
        -1.0,
        (relOptInfo->orientation != REL_ROW_ORIENTED));

    /* Estimate the cost of index scan */
    indexScanPath = create_index_path(root, indexInfo, NIL, NIL, NIL, NIL, NIL, ForwardScanDirection, false, NULL, NULL, 1.0);

    return (seqScanAndSortPath.total_cost < indexScanPath->path.total_cost);
}

/* Get aligned hash entry size from Agg width and number of agg functions */
Size get_hash_entry_size(int width, int numAggs)
{
    return alloc_trunk_size(MAXALIGN(width) + MAXALIGN(sizeof(MinimalTupleData))) + hash_agg_entry_size(numAggs);
}

#ifdef STREAMPLAN

static bool needs_two_level_groupagg(PlannerInfo* root, Plan* plan, Node* distinct_node, List* distributed_key,
    bool* need_redistribute, bool* need_local_redistribute)
{
    Query* parse = root->parse;
    bool located = false;
    bool two_level_sort = false;
    double multiple = 1.0;
    List* locate_node_list = NIL;
    List* group_exprs = get_sortgrouplist_exprs(parse->groupClause, parse->targetList);
    bool force_single_group = expression_returns_set((Node*)parse->targetList);
    ListCell* lc = NULL;

    foreach (lc, group_exprs) {
        if (equal(distinct_node, lfirst(lc))) {
            located = true;
            break;
        }
    }
    list_free_ext(group_exprs);
    if (!located) {
        List* distinct_node_list = list_make1(distinct_node);
        bool needs_redistribute_distinct = needs_agg_stream(root, distinct_node_list, plan->distributed_keys);

        /*
         * for smp plan, when doing agg we have to do a local redistribute even
         * if the agg col is the distribute key
         */
        if (!needs_redistribute_distinct && SET_DOP(plan->dop) > 1) {
            *need_local_redistribute = true;
        }

        /* For AGG_PLAIN case, and no redistribution on count(distinct) expr case, we can directly judge two_level_sort
         * is needed. Else, if distinct number of group by clause is too small to give work to all dn, we can do
         * two_level_sort to get better performance */
        if (!needs_redistribute_distinct) {
            if ((!parse->groupClause || distributed_key) && !force_single_group) {
                two_level_sort = true;
            }
        } else if (!parse->groupClause) {
            locate_node_list = get_distributekey_from_tlist(root, NIL, distinct_node_list, plan->plan_rows, &multiple);

            if (locate_node_list != NIL && !force_single_group) {
                AssertEreport(list_member(locate_node_list, distinct_node),
                    MOD_OPT,
                    "The distinct node is not a member of the locate node list");
                list_free_ext(locate_node_list);
                two_level_sort = true;
                *need_redistribute = true;
            }
        } else {
            List* groupby_node_list = NIL;
            double groupby_multiple = 1.0;

            groupby_node_list =
                get_distributekey_from_tlist(root, NIL, distributed_key, plan->plan_rows, &groupby_multiple);
            if (groupby_node_list != NIL && groupby_multiple > 1) {
                locate_node_list =
                    get_distributekey_from_tlist(root, NIL, distinct_node_list, plan->plan_rows, &multiple);
                if (locate_node_list != NIL && multiple < groupby_multiple && !force_single_group) {
                    AssertEreport(list_member(locate_node_list, distinct_node),
                        MOD_OPT,
                        "The distinct node is not a member of the locate node list");
                    two_level_sort = true;
                    *need_redistribute = true;
                }
                list_free_ext(locate_node_list);
            }
            list_free_ext(groupby_node_list);
        }
        list_free_ext(distinct_node_list);
    }

    return two_level_sort;
}
#ifdef ENABLE_MULTIPLE_NODES
static List* append_distribute_var_list(List* varlist, Node* tlist_node)
{
    ListCell* lc = NULL;
    ListCell* lc2 = NULL;
    List* expr_varlist =
        pull_var_clause(tlist_node, PVC_INCLUDE_AGGREGATES, PVC_RECURSE_PLACEHOLDERS, PVC_INCLUDE_SPECIAL_EXPR);
    List* resultlist = NIL;

    foreach (lc, expr_varlist) {
        Node* node = (Node*)lfirst(lc);
        if (IsA(node, Var))
            varlist = list_append_unique(varlist, node);
        else {
            lc2 = NULL;
            AssertEreport(IsA(node, EstSPNode), MOD_OPT, "invalid node type.");
            foreach (lc2, varlist) {
                Node* tmp = (Node*)lfirst(lc2);
                if (IsA(tmp, Var)) {
                    if (list_member(((EstSPNode*)node)->varlist, tmp))
                        break;
                } else {
                    AssertEreport(IsA(tmp, EstSPNode), MOD_OPT, "invalid node type.");
                    if (((resultlist = list_intersection(((EstSPNode*)node)->varlist, ((EstSPNode*)tmp)->varlist))) !=
                        NIL) {
                        list_free_ext(resultlist);
                        break;
                    }
                }
            }
            if (lc2 == NULL)
                varlist = lappend(varlist, node);
            else
                list_free_ext(((EstSPNode*)node)->varlist);
        }
    }
    list_free_ext(expr_varlist);

    return varlist;
}
#endif
/*
 * @Description: Generate top agg.
 * @in root: Per-query information for planning/optimization.
 * @in tlist: Targetlist.
 * @in agg_plan: Under agg_plan.
 * @in stream_plan: Under stream plan.
 * @in agg_orientation: Agg strategy.
 */
static Plan* mark_top_agg(
    PlannerInfo* root, List* tlist, Plan* agg_plan, Plan* stream_plan, AggOrientation agg_orientation)
{
    Plan* top_node = NULL;
    Plan* leftchild = agg_plan->lefttree;
    Plan* sub_plan = NULL;
    agg_plan->lefttree = NULL;

    if (((Agg*)agg_plan)->aggstrategy == AGG_SORTED) {
        AssertEreport(
            agg_orientation != DISTINCT_INTENT, MOD_OPT, "unexpected aggregate orientation when generating top agg.");
        AttrNumber* groupColIdx = (AttrNumber*)palloc(list_length(root->parse->groupClause) * sizeof(AttrNumber));
        locate_grouping_columns(root, agg_plan->targetlist, ((Plan*)stream_plan)->targetlist, groupColIdx);
        sub_plan = (Plan*)make_sort_from_groupcols(root, root->parse->groupClause, groupColIdx, (Plan*)stream_plan);
        inherit_plan_locator_info(sub_plan, stream_plan);
#ifdef ENABLE_MULTIPLE_NODES
        if (IsA(stream_plan, RemoteQuery)) {
            ((RemoteQuery*)stream_plan)->mergesort_required = true;
            sub_plan->plan_rows = PLAN_LOCAL_ROWS(sub_plan);
        } else {
            AssertEreport(IsA(stream_plan, Stream), MOD_OPT, "unexpected node type when generating top agg.");
            ((Stream*)stream_plan)->is_sorted = true;
        }
#else
        if (IsA(stream_plan, Stream)) {
            ((Stream*)stream_plan)->is_sorted = true;
        }
#endif
    } else
        sub_plan = (Plan*)stream_plan;

    top_node = (Plan*)copyObject(agg_plan);
    /* remove the skew opt from low layer agg, we only display the flag on top agg. */
    ((Agg*)agg_plan)->skew_optimize = SKEW_RES_NONE;

    // restore the lefttree pointer of original plan
    /* The having qual of second agg node is copied from first agg and has been processed to second agg expression.
     *  Remove having qual for the first aggregation.
     */
    agg_plan->qual = NIL;
    agg_plan->lefttree = leftchild;

    top_node->startup_cost = sub_plan->startup_cost;
    top_node->total_cost = sub_plan->total_cost;
    top_node->lefttree = sub_plan;
    top_node->targetlist = tlist;

    /* Set smp info. */
    if (IsA(stream_plan, Stream)) {
        top_node->dop = stream_plan->dop;
    } else {
        top_node->dop = 1;
    }

    if (IsA(stream_plan, RemoteQuery))
        top_node->plan_rows = PLAN_LOCAL_ROWS(top_node);
    inherit_plan_locator_info(top_node, top_node->lefttree);

    if (IsA(top_node, Agg)) {
        Agg* agg_node = (Agg*)top_node;

        if (agg_orientation != DISTINCT_INTENT && root->parse->groupClause != NIL)
            locate_grouping_columns(root, top_node->targetlist, top_node->lefttree->targetlist, agg_node->grpColIdx);

        agg_node->is_final = true;

        if (IsA(stream_plan, RemoteQuery))
            agg_node->numGroups = (long)Min(top_node->plan_rows, (double)LONG_MAX);
    }

    return (Plan*)top_node;
}

static Plan* mark_agg_stream(PlannerInfo* root, List* tlist, Plan* plan, List* group_or_distinct_cls,
    AggOrientation agg_orientation, bool* has_second_agg_sort)
{
    Plan* streamplan = NULL;
    bool subplan_exec_on_dn = false;

    if (plan == NULL || !IsA(plan, Agg) || is_execute_on_coordinator(plan) || is_execute_on_allnodes(plan))
        return plan;

    *has_second_agg_sort = false;

    subplan_exec_on_dn = check_subplan_exec_datanode(root, (Node*)plan->qual);
    if (root->query_level == 1 && !subplan_exec_on_dn) {
        streamplan = make_simple_RemoteQuery(plan, root, false);
    } else { // subquery
        List* distribute_keys = NIL;
        double multiple = 0.0;

        distribute_keys = get_optimal_distribute_key(root, group_or_distinct_cls, plan, &multiple);
        Distribution* distribution = (distribute_keys == NULL)
                                     ? ng_get_correlated_subplan_group_distribution() : ng_get_dest_distribution(plan);
        streamplan = make_stream_plan(root, plan, distribute_keys, multiple, distribution);
        AssertEreport(streamplan->exec_nodes != NULL,
            MOD_OPT,
            "The list of datanodes where to execute stream plan is empty when generating top agg.");

        /* Handle of parallelism of plain agg when we create broadcast for it. */
        Stream* stream = (Stream*)streamplan;
        if (stream->type == STREAM_BROADCAST) {
            stream->smpDesc.consumerDop = 1;
            streamplan->dop = 1;
        }
    }
#ifdef ENABLE_MULTIPLE_NODES
    if (plan == streamplan) {
        // in some cases, there is no new stream node added (such as in called by add_remote_subplan)
        // if that's case, nothing else should be done. otherwise, we may create agg over agg operator
        // which is unnecessary and incorrrect for some agg function such as (avg)
        return plan;
    }
#endif
    if (((Agg*)plan)->aggstrategy == AGG_SORTED)
        *has_second_agg_sort = true;

    return mark_top_agg(root, tlist, plan, streamplan, agg_orientation);
}

static Plan* mark_group_stream(PlannerInfo* root, List* tlist, Plan* result_plan)
{
    Plan* streamplan = NULL;
    Plan* bottom_node = NULL;
    Plan* top_node = NULL;
    Plan* leftchild = NULL;
    Plan* sort_node = NULL;
    List* groupClause = NIL;
    AttrNumber* groupColIdx = NULL;
    bool subplan_exec_on_dn = false;

    AssertEreport(
        result_plan && IsA(result_plan, Group), MOD_OPT, "The result plan is NULL or its type is not T_Group.");

    groupClause = root->parse->groupClause;
    groupColIdx = (AttrNumber*)palloc(sizeof(AttrNumber) * list_length(groupClause));
    locate_grouping_columns(root, result_plan->targetlist, result_plan->targetlist, groupColIdx);

    /*
     * If the qual contains subplan exec on DN, group + gather + group will cause
     * the query unshippable(see finalize_node_id), we should never gen such plan.
     */
    subplan_exec_on_dn = subplan_exec_on_dn || check_subplan_exec_datanode(root, (Node*)result_plan->qual);
    if (!equal(tlist, result_plan->targetlist)) {
        subplan_exec_on_dn = subplan_exec_on_dn || check_subplan_exec_datanode(root, (Node*)tlist);
    }

    if (root->query_level == 1 && !subplan_exec_on_dn)
        streamplan = make_simple_RemoteQuery(result_plan, root, false);
    else {
        double multiple = 0.0;
        List* groupcls = root->parse->groupClause;
        List* distribute_keys = NIL;

        distribute_keys = get_optimal_distribute_key(root, groupcls, result_plan, &multiple);
        streamplan = make_stream_plan(root, result_plan, distribute_keys, multiple);
    }

    sort_node = (Plan*)make_sort_from_groupcols(root, groupClause, groupColIdx, streamplan);
    bottom_node = sort_node;

    leftchild = result_plan->lefttree;
    result_plan->lefttree = NULL;

    top_node = (Plan*)copyObject(result_plan);

    /* If the sub group by is parallelized, and the top group is above gather. */
    if (root->query_level == 1 && !subplan_exec_on_dn)
        top_node->dop = 1;

    result_plan->qual = NIL;
    result_plan->lefttree = leftchild;

    top_node->lefttree = bottom_node;
    top_node->targetlist = tlist;
    inherit_plan_locator_info(top_node, top_node->lefttree);
    ((Group*)top_node)->grpColIdx = groupColIdx;

    return top_node;
}

/*
 * @Description: Check this winagg node if include other winfuns expect rank and row_number.
 * @in wfc: windows function.
 * @return: Return true if include other windows fun besides rank and row_number else return false.
 */
static bool contain_other_windowfuncs(List* tlist, WindowFunc* wfc)
{
    ListCell* cell = NULL;

    foreach (cell, tlist) {
        TargetEntry* tle = (TargetEntry*)lfirst(cell);
        Expr* expr = tle->expr;

        if (IsA(expr, WindowFunc)) {
            WindowFunc* winFun = (WindowFunc*)expr;

            /* If this winFun and wfc be computed in same node and is not rank or row_number return true.*/
            if (winFun->winref == wfc->winref && winFun->winfnoid != ROWNUMBERFUNCOID &&
                winFun->winfnoid != RANKFUNCOID) {
                return true;
            }
        }
    }

    return false;
}

/*
 * @Description: Build lower windows agg
 * @in root: Per-query information for planning/optimization.
 * @in plan: windows agg plan.
 * @in wc: windows agg clause.
 * @return: If build lower plan return this plan else return NULL.
 */
static Plan* build_lower_winagg_plan(PlannerInfo* root, Plan* plan, WindowClause* wc, List* partitionExprs)
{
    Plan* bottomPlan = NULL;

    if (list_length(root->subqueryRestrictInfo) == 1) {
        Expr* expr = (Expr*)(((RestrictInfo*)linitial(root->subqueryRestrictInfo))->clause);

        if (IsA(expr, OpExpr) && list_length(((OpExpr*)expr)->args) == 2) {
            OpExpr* op_expr = (OpExpr*)expr;
            Node* arg1 = (Node*)linitial(op_expr->args);
            Node* arg2 = (Node*)lsecond(op_expr->args);

            /* We only support parameter is const and operator is less than or less equal. */
            if (IsA(arg1, Var) && IsA(arg2, Const) && (op_expr->opno == INT84LTOID || op_expr->opno == INT84LEOID)) {
                Var* var = (Var*)arg1;
                Const* con = (Const*)arg2;

                TargetEntry* tge = (TargetEntry*)list_nth(plan->targetlist, var->varattno - 1);

                Node* arg = (Node*)tge->expr;

                /* 
                 * Only support rank() and row_number() and this node targetlist only can include rank()
                 * and row_number().
                 */
                if (IsA(arg, WindowFunc) &&
                    (((WindowFunc*)arg)->winfnoid == ROWNUMBERFUNCOID || ((WindowFunc*)arg)->winfnoid == RANKFUNCOID) &&
                    !contain_other_windowfuncs(plan->targetlist, (WindowFunc*)arg)) {
                    double selec = get_windowagg_selectivity(root,
                        wc,
                        (WindowFunc*)arg,
                        partitionExprs,
                        DatumGetInt32(con->constvalue),
                        PLAN_LOCAL_ROWS(plan),
                        ng_get_dest_num_data_nodes(plan));

                    /*
                     * When less than or less equal filtration ratio less than 1/3, we will
                     * generate two levels winfunc plan.
                     */
                    if (selec < TWOLEVELWINFUNSELECTIVITY) {
                        Plan* leftree = plan->lefttree;
                        List* winPlanTarget = plan->targetlist;

                        List* leftTarget = plan->lefttree->targetlist;

                        TargetEntry* tle = NULL;

                        OpExpr* winFunCondition = (OpExpr*)copyObject(expr);

                        /* This qual's left arg is windowagg's rank or row_number. */
                        linitial(winFunCondition->args) = copyObject(arg);

                        /* Copy a windows agg plan as lower windows agg node. */
                        plan->lefttree = NULL;
                        plan->targetlist = NIL;
                        Plan* lowerWindowPlan = (Plan*)copyObject(plan);

                        /* Lower windows plan's targetlist should be lefttree's targetlist and windows fun. */
                        lowerWindowPlan->targetlist = (List*)copyObject(leftTarget);
                        tle = (TargetEntry*)copyObject(tge);
                        lowerWindowPlan->targetlist = lappend(lowerWindowPlan->targetlist, tle);

                        tle->resno = list_length(lowerWindowPlan->targetlist);

                        lowerWindowPlan->lefttree = leftree;

                        /* Restore plan's targetlist. */
                        plan->targetlist = winPlanTarget;

                        bottomPlan = (Plan*)make_result(
                            root, (List*)copyObject(leftTarget), NULL, lowerWindowPlan, list_make1(winFunCondition));

                        set_plan_rows(bottomPlan, clamp_row_est(plan->plan_rows * selec));
                    }
                }
            }
        }
    }

    return bottomPlan;
}

Distribution* get_windows_best_distribution(Plan* plan) 
{
    if (!u_sess->attr.attr_sql.enable_dngather || !u_sess->opt_cxt.is_dngather_support) {    
        return ng_get_dest_distribution(plan);
    } 

    if (plan->plan_rows <= u_sess->attr.attr_sql.dngather_min_rows) {
        return ng_get_single_node_distribution();
    }
        
    return ng_get_dest_distribution(plan);
}

/*
 * @Description: Add stream node under windowAgg node if need.
 * @in root: Per-query information for planning/optimization.
 * @in plan: windows agg node.
 * @in tlist: target list.
 * @in wc: current window clause.
 * @in pathkeys: current sort pathkeys.
 * @in wflists: window function list.
 */
static Plan* mark_windowagg_stream(
    PlannerInfo* root, Plan* plan, List* tlist, WindowClause* wc, List* pathkeys, WindowLists* wflists)
{
    WindowAgg* wa_plan = NULL;
    Plan* resultPlan = plan;
    Plan* bottomPlan = NULL;
    Plan* lefttree = NULL;

    AssertEreport(plan && IsA(plan, WindowAgg), MOD_OPT, "The plan is null or its type is not T_WindowAgg.");

    wa_plan = (WindowAgg*)plan;

    bottomPlan = plan->lefttree;

    if (IsA(bottomPlan, Sort))
        bottomPlan = bottomPlan->lefttree;

    if (wa_plan->partNumCols > 0) {
        AssertEreport(
            wa_plan->partColIdx, MOD_OPT, "invalid part column index when adding stream node upder windowAgg node.");

        List* partitionExprs = get_sortgrouplist_exprs(wc->partitionClause, tlist);
        bool need_stream = needs_agg_stream(root, partitionExprs, plan->distributed_keys, &plan->exec_nodes->distribution);

        /*
         * Two case we need stream:
         * 1. partition keys not in subplan's distribute keys <=> need_stream = true;
         * 2. partition keys in subplan's keys but we want to parallelize it.
         */
        if (need_stream || plan->dop > 1) {
            double multiple = 0.0;
            List* bestDistExpr = NIL;

            /* For local redistribute in parallelization. */
            if (!need_stream)
                bestDistExpr = plan->distributed_keys;
            else
                bestDistExpr = get_optimal_distribute_key(root, partitionExprs, bottomPlan, &multiple);

            /* only build two-level plan for the last windowagg */
            if (wc == llast(wflists->activeWindows)) {
                Plan* lower_plan = build_lower_winagg_plan(root, plan, wc, partitionExprs);

                /* Can generate two level windows agg. */
                if (lower_plan != NULL) {
                    bottomPlan = lower_plan;
                    resultPlan = plan;
                }
            }

            Plan* streamplan = make_stream_plan(root, bottomPlan, bestDistExpr, multiple, get_windows_best_distribution(plan));
            if (!need_stream)
                ((Stream*)streamplan)->smpDesc.distriType = LOCAL_DISTRIBUTE;

            /* We need sort node stream after. */
            if (pathkeys != NIL) {
                lefttree = (Plan*)make_sort_from_pathkeys(root, streamplan, pathkeys, -1.0, true);
            } else {
                lefttree = streamplan;
            }

            resultPlan->lefttree = lefttree;

            /* Recalculate windowagg cost for two-level plan or plan multiple change */
            if (wc == llast(wflists->activeWindows) || lefttree->multiple != plan->multiple) {
                Path windowagg_path; /* dummy for result of cost_windowagg */

                set_plan_rows(resultPlan, lefttree->plan_rows, lefttree->multiple);
                cost_windowagg(&windowagg_path,
                    root,
                    wflists->windowFuncs[wa_plan->winref],
                    wa_plan->partNumCols,
                    wa_plan->ordNumCols,
                    lefttree->startup_cost,
                    lefttree->total_cost,
                    PLAN_LOCAL_ROWS(lefttree));
                plan->startup_cost = windowagg_path.startup_cost;
                plan->total_cost = windowagg_path.total_cost;
            } else {
                /* Add stream and lower_plan cost. */
                resultPlan->startup_cost += (lefttree->startup_cost - plan->startup_cost);
                resultPlan->total_cost += (lefttree->total_cost - plan->total_cost);
            }

            inherit_plan_locator_info(resultPlan, lefttree);
        } else {
            resultPlan = plan;
        }
    } else { /* Have not partition by */
        Plan* gatherPlan = NULL;
        Sort* sortPlan = NULL;
        SimpleSort* streamSort = NULL;

        if (pathkeys != NIL) {
            sortPlan = make_sort_from_pathkeys(root, bottomPlan, pathkeys, -1.0);

            streamSort = makeNode(SimpleSort);
            streamSort->numCols = sortPlan->numCols;
            streamSort->sortColIdx = sortPlan->sortColIdx;
            streamSort->sortOperators = sortPlan->sortOperators;
            streamSort->nullsFirst = sortPlan->nullsFirst;
            streamSort->sortToStore = false;
            streamSort->sortCollations = sortPlan->collations;
        }

        /* for plan in sub level, we push it down to dn */
        if (root->query_level == 1) {
            /*
             * If have pathkeys, we can push down Sort to Datanode and then merge partial
             * sorted results in RemoteQuery.
             */
            if (pathkeys != NIL) {
                gatherPlan = make_simple_RemoteQuery((Plan*)sortPlan, root, false);
                if (IsA(gatherPlan, RemoteQuery)) {
                    ((RemoteQuery*)gatherPlan)->sort = streamSort;
                } else if (IsA(gatherPlan, Stream)) {
                    ((Stream*)gatherPlan)->sort = streamSort;
                }
            } else {
                gatherPlan = make_simple_RemoteQuery(bottomPlan, root, false);
            }
        } else {
            if (((unsigned int)u_sess->attr.attr_sql.cost_param & COST_ALTERNATIVE_MERGESORT) ||
                root->is_under_recursive_cte) {
                gatherPlan = make_stream_plan(root, bottomPlan, NIL, 1.0);
                if (pathkeys != NIL)
                    gatherPlan = (Plan*)make_sort_from_pathkeys(root, gatherPlan, pathkeys, -1.0);
            } else {
                bool single_node =
#ifndef ENABLE_MULTIPLE_NODES
                    plan->dop <= 1 &&
#endif
                    (bottomPlan->exec_nodes != NULL && list_length(bottomPlan->exec_nodes->nodeList) == 1);
                /* If there's a sort, we need it to do merge sort */
                if (IsA(plan->lefttree, Sort)) {
                    /*
                     * If bottom plan is already run single node and need a global sort, we should
                     * redistribute to all datanodes to do sort to avoid bottleneck
                     */
                    if (single_node) {
                        bottomPlan = plan->lefttree->lefttree;

                        /* Construct group clause using targetlist. */
                        List* grplist = make_groupcl_for_append(root, bottomPlan->targetlist);

                        if (grplist != NIL) {
                            double multiple;
                            List* distkeys = NIL;

                            /* Get distkeys according to bias. */
                            distkeys = get_distributekey_from_tlist(
                                root, bottomPlan->targetlist, grplist, bottomPlan->plan_rows, &multiple);
                            /* If distribute key is much less skewed, we use it */
                            if (distkeys != NIL &&
                                multiple < u_sess->pgxc_cxt.NumDataNodes * TWOLEVELWINFUNSELECTIVITY) {
                                bottomPlan = make_stream_plan(root, bottomPlan, distkeys, 1.0);
                                plan->lefttree->lefttree = bottomPlan;
                                inherit_plan_locator_info((Plan*)plan->lefttree, bottomPlan);
                                single_node = false;
                            }
                        }
                    }
                    bottomPlan = plan->lefttree;
                }
                /* If there are multiple producers, add a merge sort */
                if (!single_node) {
                    gatherPlan = make_stream_plan(root, bottomPlan, NIL, 1.0);
                    pick_single_node_plan_for_replication(gatherPlan);
                    if (pathkeys != NIL)
                        ((Stream*)gatherPlan)->sort = streamSort;
                } else
                    gatherPlan = bottomPlan;
            }
        }

        plan->lefttree = gatherPlan;
        inherit_plan_locator_info(plan, gatherPlan);

        resultPlan = plan;
    }

    return resultPlan;
}

static Plan* mark_distinct_stream(
    PlannerInfo* root, List* tlist, Plan* plan, List* distinctcls, Index query_level, List* current_pathkeys)
{
    Plan* leftchild = NULL;
    Plan* bottom_node = NULL;
    Plan* top_node = NULL;
    Plan* streamplan = NULL;

    if (plan == NULL || !IsA(plan, Unique) || is_execute_on_coordinator(plan) || is_execute_on_allnodes(plan))
        return plan;

#ifndef ENABLE_MULTIPLE_NODES
    /* if on single-node and dop is 1, no need add a stream operator */
    if (plan->dop == 1) {
        return plan;
    }
#endif

    if (query_level == 1)
        streamplan = make_simple_RemoteQuery(plan, root, false);
    else {
        List* distribute_keys = NIL;
        double multiple = 0.0;

        distribute_keys = get_optimal_distribute_key(root, distinctcls, plan, &multiple);
        streamplan = make_stream_plan(root, plan, distribute_keys, multiple);
    }

    if (current_pathkeys != NULL) {
        bottom_node = (Plan*)make_sort_from_pathkeys(root, streamplan, current_pathkeys, -1.0);
        inherit_plan_locator_info(bottom_node, streamplan);
    } else {
        bottom_node = streamplan;
    }

    if (IsA(streamplan, RemoteQuery))
        ((RemoteQuery*)streamplan)->mergesort_required = true;
    else
        ((Stream*)streamplan)->is_sorted = true;

    leftchild = plan->lefttree;
    plan->lefttree = NULL;

    top_node = (Plan*)copyObject(plan);

    // restore the lefttree pointer of original plan
    plan->lefttree = leftchild;

    top_node->lefttree = bottom_node;
    top_node->targetlist = tlist;
    inherit_plan_locator_info(top_node, bottom_node);

    return top_node;
}

/*
 * get_optimal_distribute_key
 *	For distinct, windowagg, group aggregation, we can't get close estimation, so roughly
 *	check if matching key can be the optimal distribute key, only if it has less biase than
 *	actual optimal distribute key
 *
 * Parameters:
 *	@in root: plannerinfo struct from current query level
 *	@in groupClause: group by clause of current query level
 *	@in plan: plan node to find best distribute key
 *	@out multiple: skew multiple of optimal distribute key
 * Return:
 *	list of optimal distribute key
 */
static List* get_optimal_distribute_key(PlannerInfo* root, List* groupClause, Plan* plan, double* multiple)
{
    double multiple_matched = -1.0;
    bool use_skew = true;
    bool use_bias = true;
    double skew_multiple = 0.0;
    double bias_multiple = 0.0;
    List* distribute_keys = NIL;

    if (root->dis_keys.matching_keys != NIL &&
        list_is_subset(root->dis_keys.matching_keys, groupClause)) { /* have a inserting table */
        get_multiple_from_exprlist(
            root, root->dis_keys.matching_keys, plan->plan_rows, &use_skew, use_bias, &skew_multiple, &bias_multiple);
        multiple_matched = Max(skew_multiple, bias_multiple);
        if (multiple_matched <= 1.0) {
            *multiple = multiple_matched;
            distribute_keys = root->dis_keys.matching_keys;
            ereport(DEBUG1, (errmodule(MOD_OPT_AGG), (errmsg("matching key distribution is chosen due to no skew."))));
        }
    }

    if (distribute_keys == NIL) {
        List* local_distribute_keys = NIL;

        local_distribute_keys =
            get_distributekey_from_tlist(root, plan->targetlist, groupClause, plan->plan_rows, multiple);

        /* Compare match key with local distribute key. */
        if (multiple_matched > 0.0 && multiple_matched <= *multiple) {
            *multiple = multiple_matched;
            distribute_keys = root->dis_keys.matching_keys;
            ereport(DEBUG1,
                (errmodule(MOD_OPT_AGG),
                    (errmsg("matching key distribution is chosen due to no skewer than best distribute key."))));
        } else {
            distribute_keys = local_distribute_keys;
        }
    }

    return distribute_keys;
}

/*
 * @Description: Check if has array operator
 *
 * @param[IN] plan: plan to check
 * @return: bool, true if has
 */
static bool has_array_operator(Plan* plan)
{
    if (IsA(plan, Agg)) {
        for (int i = 0; i < ((Agg*)plan)->numCols; i++) {
            if (((Agg*)plan)->grpOperators[i] == ARRAY_EQ_OP) {
                return true;
            }
        }
    }

    if (IsA(plan, Group)) {
        for (int i = 0; i < ((Group*)plan)->numCols; i++) {
            if (((Group*)plan)->grpOperators[i] == ARRAY_EQ_OP) {
                return true;
            }
        }
    }

    return false;
}

/*
 * @Description: Check if has column store relation
 *
 * @param[IN] top_plan:  current plan node
 * @return: bool, true if has one
 */
static bool has_column_store_relation(Plan* top_plan)
{
    switch (nodeTag(top_plan)) {
        /* Node which vec_output is true */
        case T_DfsScan:
        case T_DfsIndexScan:
        case T_CStoreScan:
        case T_CStoreIndexScan:
        case T_CStoreIndexCtidScan:
        case T_CStoreIndexHeapScan:
#ifdef ENABLE_MULTIPLE_NODES
        case T_TsStoreScan:
#endif   /* ENABLE_MULTIPLE_NODES */
            return true;

        case T_ForeignScan:
            /* If it is column store relation, return true */
            if (IsVecOutput(top_plan))
                return true;
            break;

        case T_ExtensiblePlan: {
            ExtensiblePlan* ext_plans = (ExtensiblePlan*)top_plan;
            ListCell* lc = NULL;

            /* If result table is column store, return true */
            if (IsVecOutput(top_plan))
                return true;

            foreach (lc, ext_plans->extensible_plans) {
                Plan* plan = (Plan*)lfirst(lc);
                if (has_column_store_relation(plan))
                    return true;
            }
        } break;

        case T_MergeAppend: {
            MergeAppend* ma = (MergeAppend*)top_plan;
            ListCell* lc = NULL;
            foreach (lc, ma->mergeplans) {
                Plan* plan = (Plan*)lfirst(lc);
                if (has_column_store_relation(plan))
                    return true;
            }
        } break;

        case T_Append: {
            Append* append = (Append*)top_plan;
            ListCell* lc = NULL;
            foreach (lc, append->appendplans) {
                Plan* plan = (Plan*)lfirst(lc);
                if (has_column_store_relation(plan))
                    return true;
            }
        } break;

        case T_ModifyTable: {
            ModifyTable* mt = (ModifyTable*)top_plan;
            ListCell* lc = NULL;

            /* If result table is column store, return true */
            if (IsVecOutput(top_plan))
                return true;

            foreach (lc, mt->plans) {
                Plan* plan = (Plan*)lfirst(lc);
                if (has_column_store_relation(plan))
                    return true;
            }
        } break;

        case T_SubqueryScan: {
            SubqueryScan* ss = (SubqueryScan*)top_plan;

            if (ss->subplan && has_column_store_relation(ss->subplan))
                return true;
        } break;

        case T_BitmapAnd:
        case T_CStoreIndexAnd: {
            BitmapAnd* ba = (BitmapAnd*)top_plan;
            ListCell* lc = NULL;
            foreach (lc, ba->bitmapplans) {
                Plan* plan = (Plan*)lfirst(lc);
                if (has_column_store_relation(plan))
                    return true;
            }
        } break;

        case T_BitmapOr:
        case T_CStoreIndexOr: {
            BitmapOr* bo = (BitmapOr*)top_plan;
            ListCell* lc = NULL;
            foreach (lc, bo->bitmapplans) {
                Plan* plan = (Plan*)lfirst(lc);
                if (has_column_store_relation(plan))
                    return true;
            }
        } break;

        default:
            if (outerPlan(top_plan)) {
                if (has_column_store_relation(outerPlan(top_plan)))
                    return true;
            }

            if (innerPlan(top_plan)) {
                if (has_column_store_relation(innerPlan(top_plan)))
                    return true;
            }
            break;
    }

    return false;
}

/*
 * @Description: Check if it is vetor scan
 *
 * @param[IN] plan:  current plan node
 * @return: bool, true if it is
 */
bool is_vector_scan(Plan* plan)
{
    if (plan == NULL) {
        return false;
    }
    switch (nodeTag(plan)) {
        case T_CStoreScan:
        case T_DfsScan:
        case T_DfsIndexScan:
        case T_CStoreIndexScan:
        case T_CStoreIndexCtidScan:
        case T_CStoreIndexHeapScan:
        case T_CStoreIndexAnd:
        case T_CStoreIndexOr:
#ifdef ENABLE_MULTIPLE_NODES
        case T_TsStoreScan:
#endif   /* ENABLE_MULTIPLE_NODES */
            return true;
        case T_PartIterator:
            if (is_vector_scan(plan->lefttree))
                return true;
            break;
        default:
            break;
    }
    return false;
}

static bool IsTypeUnSupportedByVectorEngine(Oid typeOid)
{
    /* we don't support user defined type. */
    if (typeOid >= FirstNormalObjectId) {
        ereport(DEBUG2, (errmodule(MOD_OPT_PLANNER),
            errmsg("Vectorize plan failed due to has unsupport type: %u", typeOid)));
        return true;
    }

    for (uint32 i = 0; i < sizeof(VectorEngineUnsupportType) / sizeof(Oid); ++i) {
        if (VectorEngineUnsupportType[i] == typeOid) {
            ereport(DEBUG2, (errmodule(MOD_OPT_PLANNER),
                errmsg("Vectorize plan failed due to has unsupport type: %u", typeOid)));
            return true;
        }
    }
    return false;
}
/*
 * @Description: Check if it has unsupport expression in vector engine
 *
 * @param[IN] node:  current expr node
 * @return: bool, true if it has
 */
bool vector_engine_unsupport_expression_walker(Node* node, VectorPlanContext* planContext)
{
    if (node == NULL) {
        return false;
    }

    /* Find the vector engine not support expression */
    switch (nodeTag(node)) {
        case T_ArrayRef:
        case T_AlternativeSubPlan:
        case T_FieldSelect:
        case T_FieldStore:
        case T_ArrayCoerceExpr:
        case T_ConvertRowtypeExpr:
        case T_ArrayExpr:
        case T_RowExpr:
        case T_Rownum:
        case T_XmlExpr:
        case T_CoerceToDomain:
        case T_CoerceToDomainValue:
        case T_CurrentOfExpr:
            ereport(DEBUG2, (errmodule(MOD_OPT_PLANNER),
                errmsg("Vectorize plan failed due to has unsupport expression: %d", nodeTag(node))));
            return true;
        case T_Var: {
            Var *var = (Var *)node;
            if (var->varattno == InvalidAttrNumber) {
                ereport(DEBUG2, (errmodule(MOD_OPT_PLANNER),
                    errmsg("Vectorize plan failed due to has system column")));
                return true;
            } else {
                return IsTypeUnSupportedByVectorEngine(var->vartype);
            }
            break;
        }
        case T_Const: {
            Const* c = (Const *)node;
            return IsTypeUnSupportedByVectorEngine(c->consttype);
        }
        case T_Param: {
            Param *par = (Param *)node;
            return IsTypeUnSupportedByVectorEngine(par->paramtype);
        }
        case T_SubPlan: {
            SubPlan* subplan = (SubPlan*)node;
            /* make sure that subplan return type must supported by vector engine */
            if (!IsTypeSupportedByCStore(subplan->firstColType)) {
                return true;
            }
            break;
        }
        /* count the number of complex expression, for vectorized plan of row tables */
        case T_CoerceViaIO:
        case T_GroupingFunc:
        case T_WindowFunc:
        case T_FuncExpr: {
            /*
	     * make sure that expr return type must supported by vector engine.
	     * When the expression is filter, the type is not checked because
	     * the result value is not passed up for calculation.
	     */
            if (planContext && !planContext->currentExprIsFilter
                && !IsTypeSupportedByCStore(exprType(node))) {
                return true;
            }
            break;
        }
        default:
            break;
    }

    return expression_tree_walker(node, (bool (*)())vector_engine_unsupport_expression_walker, (void*)planContext);
}

/*
 * @Description: Try to generate vectorized plan
 *
 * The GUC 'try_vector_engine_strategy' is off, the function only processes the plan with
 * column store relation. Otherwish, the function will force vectorize the plan,
 * deal with these 8 scans: SeqScan, IndexScan, IndexOnlyScan, BitmapHeapScan, TidScan, FunctionScan,
 * ValuesScan, ForeignScan.
 *
 * @param[IN] top_plan:  current plan node
 * @param[IN] parse:  query tree
 * @param[IN] from_subplan:  if node from subplan
 * @param[IN] subroot: plan root of current subquery plan tree
 * @return: Plan*, vectorized plan, fallbacked plan, or leave unchanged
 */
Plan* try_vectorize_plan(Plan* top_plan, Query* parse, bool from_subplan, PlannerInfo* subroot)
{
    /*
     * If has the three conditions, just leave unchanged:
     * 1.The GUC 'try_vector_engine_strategy' is off, and it has no column store relation;
     */
    if (u_sess->attr.attr_sql.vectorEngineStrategy == OFF_VECTOR_ENGINE &&
        !has_column_store_relation(top_plan)) {
        return top_plan;
    }

    /*
     * Fallback to original non-vectorized plan, if either the GUC 'enable_vector_engine'
     * is turned off or the plan cannot go through vector_engine_walker.
     */
    if (!u_sess->attr.attr_sql.enable_vector_engine ||
        (subroot != NULL && subroot->is_under_recursive_tree) ||
        vector_engine_walker(top_plan, from_subplan) ||
        (ENABLE_PRED_PUSH_ALL(NULL) || (subroot != NULL && SUBQUERY_PREDPUSH(subroot)))) {
        /*
         * Distributed Recursive CTE Support
         *
         * In case of a SubPlan node appears under a recursive CTE's recursive plan
         * branch, we don't try vectorization plan, instead we do fallback to just
         * add vec2row on top of CStore operators
         *
         * In the future if we support native-recursive execution e.g. says VecRecursiveUnion,
         * then we need revisit this part to lift this restriction
         *
         *
         * We go through fallback_plan to transfer plan to row engine.
         * If it's already for row engine, it leaves unchanged
         */
        top_plan = fallback_plan(top_plan);
    } else {
        bool forceVectorEngine = (u_sess->attr.attr_sql.vectorEngineStrategy != OFF_VECTOR_ENGINE);
        top_plan = vectorize_plan(top_plan, from_subplan, forceVectorEngine);

        if (from_subplan && !IsVecOutput(top_plan))
            top_plan = fallback_plan(top_plan);
    }

    if (IsVecOutput(top_plan))
        top_plan = (Plan*)make_vectorow(top_plan);

    return top_plan;
}

/*
 * @Description: Walk through the expression tree to see if it's supported in Vector Engine
 *
 * @param[IN] node:  points to query tree or expr node
 * @param[IN] context:  points to a struct that holds whatever context information
 *                      the walker routine needs
 * @return: bool, true means unsupported, false means supported
 */
static bool vector_engine_expression_walker(Node* node, DenseRank_context* context)
{
    Oid funcOid = InvalidOid;
    List* agg_distinct = NIL;
    List* agg_order = NIL;


    if (node == NULL)
        return false;

    if (IsA(node, Aggref)) {
        Aggref* aggref = (Aggref*)node;
        funcOid = aggref->aggfnoid;
        if (list_length(aggref->args) > 1) {
            agg_distinct = aggref->aggdistinct;
            agg_order = aggref->aggorder;
        }
    } else if (IsA(node, WindowFunc)) {
        WindowFunc* wfunc = (WindowFunc*)node;
        funcOid = wfunc->winfnoid;

        if (context != NULL) {
            if (wfunc->winagg)
                context->has_agg = true;

            if (funcOid == DENSERANKFUNCOID)
                context->has_denserank = true;
        }
    }

    if (funcOid != InvalidOid) {
        bool found = false;

        /* distince and order by inside such agg are not yet supported */
        if (agg_distinct || agg_order) {
            return true;
        }

        /*
         * Only ROW_NUMBER, RANK, AVG, COUNT, MAX, MIN and SUM are supported now
         * and their func oid must be found in hash table g_instance.vec_func_hash.
         *
         * For the system internal thread, VecFuncHash is not be Initialize,
         * these scenes do not need to vectorized plan.
         */
        if (g_instance.vec_func_hash != NULL) {
            (void)hash_search(g_instance.vec_func_hash, &funcOid, HASH_FIND, &found);
        }
        /* If not found means that the Agg function is not yet implemented */
        if (!found)
            return true;
    }

    return expression_tree_walker(node, (bool (*)())vector_engine_expression_walker, context);
}

/*
 * @Description: Walk through the expression tree to see if it's supported in Vector Engine,
 *				 if have set-returning function, then not support.
 *
 * @param[IN] node:  points to query tree or expr node
 * @param[IN] context:  points to a struct that holds whatever context information
 *                      the walker routine needs
 * @return: bool, true means unsupported, false means supported
 */
static bool vector_engine_setfunc_walker(Node* node, DenseRank_context* context)
{
    if (node == NULL)
        return false;

    if (IsA(node, FuncExpr)) {
        FuncExpr* expr = (FuncExpr*)node;

        if (expr->funcretset == true) {
            ereport(DEBUG2, (errmodule(MOD_OPT_PLANNER),
                errmsg("Vectorize plan failed due to has set function: %u", expr->funcid)));
            return true;
        }
    }

    return expression_tree_walker(node, (bool (*)())vector_engine_setfunc_walker, context);
}

/*
 * Check if there is any data type unsupported by cstore. If so, stop rowtovec
 */
bool CheckTypeSupportRowToVec(List* targetlist, int errLevel)
{
    ListCell* cell = NULL;
    TargetEntry* entry = NULL;
    Var* var = NULL;
    foreach(cell, targetlist) {
        entry = (TargetEntry*)lfirst(cell);
        if (IsA(entry->expr, Var)) {
            var = (Var*)entry->expr;
            if (var->varattno > 0 && var->varoattno > 0
                && var->vartype != TIDOID // cstore support for hidden column CTID
                && !IsTypeSupportedByCStore(var->vartype)) {
                ereport(errLevel, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("type \"%s\" is not supported in column store",
                           format_type_with_typemod(var->vartype, var->vartypmod))));
                return false;
            }
        }
    }
    return true;
}

/*
 * @Description: Check if the plan node is supported by VecMarkPos
 *
 * @param[IN] node: current plan node
 * @return: bool, true means support
 */
static bool VecMarkPosSupport(Plan *plan)
{
    if (IsA(plan, Sort) || IsA(plan, Material)) {
        return true;
    }

    return false;
}

static inline bool CheckVectorEngineUnsupportedFeature(Plan* plan, VectorPlanContext* planContext)
{
    /* if have set-returning function, not support. */
    if (vector_engine_setfunc_walker((Node*)(plan->targetlist), NULL))
        return true;

    /* check whether there is unsupport expression in vector engine */
    if (vector_engine_unsupport_expression_walker((Node*)plan->targetlist, planContext))
        return true;

    planContext->currentExprIsFilter = true;
    if (vector_engine_unsupport_expression_walker((Node*)plan->qual, planContext))
        return true;
    planContext->currentExprIsFilter = false;

    return false;
}

inline void ComputeExprMapCost(Oid typeId, VectorExprContext* context)
{
    if (!COL_IS_ENCODE(typeId)) {
        Cost rowMapExprCost = 0.3;
        Cost vecMapExprCost = 0.2;
        context->planContext->rowCost += rowMapExprCost * context->rows;
        context->planContext->vecCost += vecMapExprCost * context->rows;
    } else {
        Cost rowMapExprCost = 0.6;
        Cost vecMapExprCost = 0.3;
        context->planContext->rowCost += rowMapExprCost * context->rows;
        context->planContext->vecCost += vecMapExprCost * context->rows;
    }

    return;
}

inline void ComputeExprEvalCost(Oid typeId, VectorExprContext* context)
{
    if (!COL_IS_ENCODE(typeId)) {
        Cost rowExprEvalCost = 0.3;
        Cost vecExprEvalCost = 0.3;
        context->planContext->rowCost += rowExprEvalCost * context->rows;
        context->planContext->vecCost += vecExprEvalCost * context->rows;
    } else {
        Cost rowExprEvalCost = 2.5;
        Cost vecExprEvalCost = 0.8;
        context->planContext->rowCost += rowExprEvalCost * context->rows;
        context->planContext->vecCost += vecExprEvalCost * context->rows;
    }

    return;
}

/*
 * @Description: Check if it has unsupport expression and get the costs of expression
 *
 * @param[IN] node:  current expr node
 * @return: bool, true if it has
 */
bool VectorEngineCheckExpressionInternal(Node* node, VectorExprContext* context)
{
    if (node == NULL) {
        return false;
    }

    /* get costs of using row engine and vector engine */
    if (context) {
        Cost rowArrayExprCost = 0.2;
        Cost vecArrayExprCost = 1.3;

        switch (nodeTag(node)) {
            case T_Var: {
                Var* var = (Var*)node;
                int varattno = (int)var->varattno;
                if (!list_member_int(context->varList, varattno)) {
                    context->varList = lappend_int(context->varList, varattno);
                }
                ComputeExprMapCost(var->vartype, context);
                return false;
            }
            case T_Const: {
                Const* con = (Const*)node;
                ComputeExprMapCost(con->consttype, context);
                return false;
            }
            case T_Param: {
                Param* param = (Param*)node;
                ComputeExprMapCost(param->paramtype, context);
                return false;
            }
            case T_FuncExpr: {
                FuncExpr* funcExpr = (FuncExpr*)node;
                ComputeExprEvalCost(funcExpr->funcresulttype, context);
                break;
            }
            case T_OpExpr: {
                OpExpr* opExpr = (OpExpr*)node;
                ComputeExprEvalCost(opExpr->opresulttype, context);
                break;
            }
            case T_Aggref: {
                Aggref* aggref = (Aggref*)node;
                /* Aggref compute tuple is it's lefttree output count */
                int savedRows = context->rows;
                context->rows = context->lefttreeRows;
                ComputeExprEvalCost(aggref->aggtrantype, context);
                expression_tree_walker(node, (bool (*)())VectorEngineCheckExpressionInternal, (void*)context);
                context->rows = savedRows;
                return false;;
            }
            case T_ScalarArrayOpExpr: {
                context->planContext->rowCost += rowArrayExprCost * context->rows;
                context->planContext->vecCost += vecArrayExprCost * context->rows;
                break;
            }
            default:
                break;
        }
    }

    return expression_tree_walker(node, (bool (*)())VectorEngineCheckExpressionInternal, (void*)context);
}

static inline void ComputeQualCost(List* qual, VectorExprContext* context)
{
    ListCell* l = NULL;
    foreach (l, qual) {
        Expr* node = (Expr*)lfirst(l);
        ereport(DEBUG2, (errmodule(MOD_OPT_PLANNER),
            errmsg("[ROWTOVEC OPTIMAL] compute qual. current rows: %f, select: %f",
            context->rows, node->selec)));
        VectorEngineCheckExpressionInternal((Node*)node, context);
        context->rows = context->rows * node->selec;
    }
}

/* return true means not support vector engine */
template<bool isSeqscan>
bool CostVectorScan(Scan* scanPlan, VectorPlanContext* planContext)
{
    if (!planContext->forceVectorEngine || !CheckTypeSupportRowToVec(scanPlan->plan.targetlist, DEBUG2)) {
        return true;
    }

    /* only optimal mode need collect costs. */
    if (u_sess->attr.attr_sql.vectorEngineStrategy != OPT_VECTOR_ENGINE) {
        return false;
    }

    Cost rowScanCost = 1.0;
    Cost vecBatchScanCost = 0.9;
    Cost rowToVecTransCost = 0.7;
    Cost origRowCost = planContext->rowCost;
    Cost origVecCost = planContext->vecCost;
    VectorExprContext exprContext;
    exprContext.planContext = planContext;
    exprContext.varList = NIL;
    int qualColCount = 0;
    int dop = SET_DOP(scanPlan->plan.dop);
    /* seqscan will scan all rows in table, index scan do not. */
    if (isSeqscan) {
        exprContext.rows = scanPlan->tableRows / dop;
        planContext->rowCost += rowScanCost * exprContext.rows;
        planContext->vecCost += vecBatchScanCost * exprContext.rows;
    } else {
        exprContext.rows = scanPlan->plan.plan_rows / dop;
        planContext->rowCost += rowScanCost * exprContext.rows;
    }

    ComputeQualCost(scanPlan->plan.qual, &exprContext);
    if (isSeqscan) {
        /* batch mode will trans qual column first */
        qualColCount = list_length(exprContext.varList);
        planContext->vecCost += rowToVecTransCost * exprContext.rows * qualColCount;
    }
    exprContext.rows = scanPlan->plan.plan_rows / dop;

    VectorEngineCheckExpressionInternal((Node*)scanPlan->plan.targetlist, &exprContext);
    if (isSeqscan) {
        int lateTransColCount = list_length(exprContext.varList) - qualColCount;
        planContext->vecCost += rowToVecTransCost * exprContext.rows * lateTransColCount;
    } else {
        /* index scan add rowtovec on scan, so the costs is (row scan costs + rowtovec costs) */
        planContext->vecCost = origVecCost + planContext->rowCost - origRowCost;
        planContext->vecCost += rowToVecTransCost * exprContext.rows * list_length(scanPlan->plan.targetlist);
    }
    planContext->containRowTable = true;

    ereport(DEBUG2, (errmodule(MOD_OPT_PLANNER),
        errmsg("[ROWTOVEC OPTIMAL] scan cost: row: %f, vec: %f",
        planContext->rowCost - origRowCost, planContext->vecCost - origVecCost)));

    return false;
}

static bool CheckWindowsAggExpr(Plan* resultPlan, bool check_rescan, VectorPlanContext* planContext)
{
    /* Only default window clause is supported now */
    if (((WindowAgg*)resultPlan)->frameOptions !=
        (FRAMEOPTION_RANGE | FRAMEOPTION_START_UNBOUNDED_PRECEDING | FRAMEOPTION_END_CURRENT_ROW))
        return true;

    /* Check if targetlist contains unsupported feature */
    DenseRank_context context;
    context.has_agg = false;
    context.has_denserank = false;
    if (vector_engine_expression_walker((Node*)(resultPlan->targetlist), &context))
        return true;

    /* Only single denserank is supported now */
    if (context.has_agg && context.has_denserank)
        return true;

    /*
     * WindowAgg nodes never have quals, since they can only occur at the
     * logical top level of a query (ie, after any WHERE or HAVING filters)
     */
    WindowAgg* wa = (WindowAgg*)resultPlan;
    if (vector_engine_unsupport_expression_walker((Node*)wa->startOffset, planContext))
        return true;
    if (vector_engine_unsupport_expression_walker((Node*)wa->endOffset, planContext))
        return true;

    if (vector_engine_walker_internal(resultPlan->lefttree, check_rescan, planContext))
        return true;

    return false;
}

static bool CheckForeignScanExpr(Plan* resultPlan, VectorPlanContext* planContext)
{
    ForeignScan* fscan = (ForeignScan*)resultPlan;
    if (IsSpecifiedFDWFromRelid(fscan->scan_relid, GC_FDW) ||
        IsSpecifiedFDWFromRelid(fscan->scan_relid, LOG_FDW)) {
        resultPlan->vec_output = false;
    }
#ifdef ENABLE_MOT
    /* do not support row to vector for mot table */
    if (IsSpecifiedFDWFromRelid(fscan->scan_relid, MOT_FDW)) {
        resultPlan->vec_output = false;
        return true;
    }
#endif
    if (!CheckTypeSupportRowToVec(resultPlan->targetlist, DEBUG2)) {
        resultPlan->vec_output = false;
        return true;
    }
    CostVectorScan<false>((Scan*)resultPlan, planContext);

    return false;
}

void CostVectorAgg(Plan* plan, VectorPlanContext* planContext)
{
    /* only optimal mode need collect costs. */
    if (u_sess->attr.attr_sql.vectorEngineStrategy != OPT_VECTOR_ENGINE) {
        return;
    }

    int dop = SET_DOP(plan->dop);
    VectorExprContext exprContext;
    exprContext.planContext = planContext;
    exprContext.varList = NIL;
    exprContext.rows = plan->plan_rows / dop;
    if (likely(plan->lefttree != NULL)) {
        exprContext.lefttreeRows = plan->lefttree->plan_rows / SET_DOP(plan->lefttree->dop);
    } else {
        exprContext.lefttreeRows = plan->plan_rows / dop;
    }
    Cost origRowCost = planContext->rowCost;
    Cost origVecCost = planContext->vecCost;

    VectorEngineCheckExpressionInternal((Node*)plan->targetlist, &exprContext);
    ComputeQualCost(plan->qual, &exprContext);

    ereport(DEBUG2, (errmodule(MOD_OPT_PLANNER),
        errmsg("[ROWTOVEC OPTIMAL] agg cost: row: %f, vec: %f",
        planContext->rowCost - origRowCost, planContext->vecCost - origVecCost)));
}

void CostVectorNestJoin(Join* join, VectorPlanContext* planContext)
{
    /* only optimal mode need collect costs. */
    if (u_sess->attr.attr_sql.vectorEngineStrategy != OPT_VECTOR_ENGINE) {
        return;
    }

    int dop = SET_DOP(join->plan.dop);
    VectorExprContext exprContext;
    exprContext.planContext = planContext;
    exprContext.varList = NIL;
    exprContext.lefttreeRows = join->plan.righttree->plan_rows / dop;
    Cost origRowCost = planContext->rowCost;
    Cost origVecCost = planContext->vecCost;

    /* joinqual execute count is determined by lefttree */
    exprContext.rows = (join->plan.righttree->plan_rows * join->plan.lefttree->plan_rows) / dop;
    ComputeQualCost(join->joinqual, &exprContext);
    ComputeQualCost(join->nulleqqual, &exprContext);

    exprContext.rows = join->plan.plan_rows;
    VectorEngineCheckExpressionInternal((Node*)join->plan.targetlist, &exprContext);

    ereport(DEBUG2, (errmodule(MOD_OPT_PLANNER),
        errmsg("[ROWTOVEC OPTIMAL] nestloop cost: row: %f, vec: %f",
        planContext->rowCost - origRowCost, planContext->vecCost - origVecCost)));
}


void CostVectorHashJoin(Join* join, VectorPlanContext* planContext)
{
    /* only optimal mode need collect costs. */
    if (u_sess->attr.attr_sql.vectorEngineStrategy != OPT_VECTOR_ENGINE) {
        return;
    }

    HashJoin* hashjoin = (HashJoin*)join;
    int dop = SET_DOP(join->plan.dop);
    VectorExprContext exprContext;
    exprContext.planContext = planContext;
    exprContext.varList = NIL;
    exprContext.lefttreeRows = join->plan.righttree->plan_rows / dop;
    Cost origRowCost = planContext->rowCost;
    Cost origVecCost = planContext->vecCost;

    exprContext.rows = hashjoin->joinRows;
    VectorEngineCheckExpressionInternal((Node*)hashjoin->hashclauses, &exprContext);
    ComputeQualCost(join->joinqual, &exprContext);
    ComputeQualCost(join->nulleqqual, &exprContext);

    Cost rowJoinCost = 0.8;
    Cost rowHashCost = 1.5;
    Cost vecJoinCost = 0.4;
    exprContext.rows = hashjoin->joinRows;
    exprContext.planContext->rowCost += rowHashCost * exprContext.lefttreeRows;
    exprContext.planContext->rowCost += rowJoinCost * exprContext.rows;
    exprContext.planContext->vecCost += vecJoinCost * exprContext.rows;

    exprContext.rows = join->plan.plan_rows;
    VectorEngineCheckExpressionInternal((Node*)join->plan.targetlist, &exprContext);

    ereport(DEBUG2, (errmodule(MOD_OPT_PLANNER),
        errmsg("[ROWTOVEC OPTIMAL] hashjoin cost: row: %f, vec: %f",
        planContext->rowCost - origRowCost, planContext->vecCost - origVecCost)));
}

/*
 * @Description: Walk through the plan tree to see if it's supported in Vector Engine
 *
 * @param[IN] result_plan:  current plan node
 * @param[IN] check_rescan:  if need check rescan
 * @return: bool, true means unsupported, false means supported
 */
static bool vector_engine_walker_internal(Plan* result_plan, bool check_rescan, VectorPlanContext* planContext)
{
    if (result_plan == NULL)
        return false;

    if (CheckVectorEngineUnsupportedFeature(result_plan, planContext)) {
        return true;
    }

    switch (nodeTag(result_plan)) {
        /*
         * If the GUC 'try_vector_engine_strategy' is off, Operators below cannot be vectorized.
         * If the GUC 'try_vector_engine_strategy' is force/optimal, 8 scans(SeqScan, IndexScan, IndexOnlyScan,
         * BitmapHeapScan, TidScan, FunctionScan, ValuesScan, Foreignscan) can be force to vector engine,
         * unless the targetlist has datatype unsupported by column store.
         */
        case T_SeqScan: {
            if (result_plan->isDeltaTable) {
                return false;
            }
            
            return CostVectorScan<true>((Scan*)result_plan, planContext);
        }
        case T_IndexScan:
        case T_IndexOnlyScan:
        case T_BitmapHeapScan:
        case T_TidScan:
        case T_FunctionScan: {
            if (!planContext->forceVectorEngine || !CheckTypeSupportRowToVec(result_plan->targetlist, DEBUG2)) {
                return true;
            }

            return CostVectorScan<false>((Scan*)result_plan, planContext);
        }
        case T_ValuesScan: {
            if (!CheckTypeSupportRowToVec(result_plan->targetlist, DEBUG2)) {
                return true;
            }
            break;
        }
        case T_CteScan:
        case T_LockRows:
        case T_MergeAppend:
        case T_RecursiveUnion:
        case T_StartWithOp:
            return true;

        case T_RemoteQuery:
            /* ExecReScanVecRemoteQuery is not yet implemented */
            if (check_rescan)
                return true;

            if (vector_engine_walker_internal(result_plan->lefttree, check_rescan, planContext))
                return true;
            break;

        case T_Stream: {
            check_rescan = false;
            Stream* sj = (Stream*)result_plan;
            if (vector_engine_unsupport_expression_walker((Node*)sj->distribute_keys, planContext))
                return true;
            if (vector_engine_walker_internal(result_plan->lefttree, check_rescan, planContext))
                return true;
        } break;
        case T_Limit: {
            Limit* lm = (Limit*)result_plan;
            if (vector_engine_unsupport_expression_walker((Node*)lm->limitCount, planContext))
                return true;
            if (vector_engine_unsupport_expression_walker((Node*)lm->limitOffset, planContext))
                return true;
            if (vector_engine_walker_internal(result_plan->lefttree, check_rescan, planContext))
                return true;
        } break;
        case T_BaseResult: {
            BaseResult* br = (BaseResult*)result_plan;
            if (vector_engine_unsupport_expression_walker((Node*)br->resconstantqual, planContext))
                return true;
            if (vector_engine_walker_internal(result_plan->lefttree, check_rescan, planContext))
                return true;
            if (!CheckTypeSupportRowToVec(result_plan->targetlist, DEBUG2)) {
                return true;
            }
        } break;
        case T_PartIterator:
        case T_SetOp:
        case T_Group:
            /* Check if contains array operator, not support distrtribute on ARRAY type now */
            if (has_array_operator(result_plan))
                return true;
        case T_Unique:
        case T_Material:
        case T_Hash:
        case T_Sort:
            if (vector_engine_walker_internal(result_plan->lefttree, check_rescan, planContext))
                return true;
            break;

        case T_Agg: {
            /* Check if targetlist contains unsupported feature */
            if (vector_engine_expression_walker((Node*)(result_plan->targetlist), NULL))
                return true;

            /* Check if qual contains unsupported feature */
            if (vector_engine_expression_walker((Node*)(result_plan->qual), NULL))
                return true;

            if (vector_engine_walker_internal(result_plan->lefttree, check_rescan, planContext))
                return true;

            /* Check if contains array operator, not support distrtribute on ARRAY type now */
            if (has_array_operator(result_plan))
                return true;
            CostVectorAgg(result_plan, planContext);
        } break;

        case T_WindowAgg: {
            if (CheckWindowsAggExpr(result_plan, check_rescan, planContext)) {
                return true;
            }
        } break;

        case T_MergeJoin: {
            MergeJoin* mj = (MergeJoin*)result_plan;
            if (vector_engine_unsupport_expression_walker((Node*)mj->mergeclauses, planContext))
                return true;
            /* Find unsupport expr *Join* clause */
            if (vector_engine_unsupport_expression_walker((Node*)mj->join.joinqual, planContext))
                return true;
            if (vector_engine_unsupport_expression_walker((Node*)mj->join.nulleqqual, planContext))
                return true;

            if (vector_engine_walker_internal(result_plan->lefttree, check_rescan, planContext))
                return true;
            if (vector_engine_walker_internal(result_plan->righttree, check_rescan, planContext))
                return true;

            /*
             * If the top plan nodes of mergejoin righttree is unsupported by VecMarkPos,
             * cannot generate vectorized plan.
             */
            if (!VecMarkPosSupport(result_plan->righttree)) {
                return true;
            }
        } break;

        case T_NestLoop: {
            NestLoop* nl = (NestLoop*)result_plan;
            /* Find unsupport expr in *Join* clause */
            if (vector_engine_unsupport_expression_walker((Node*)nl->join.joinqual, planContext))
                return true;
            if (vector_engine_unsupport_expression_walker((Node*)nl->join.nulleqqual, planContext))
                return true;

            if (vector_engine_walker_internal(result_plan->lefttree, check_rescan, planContext))
                return true;
            if (IsA(result_plan->righttree, Material) && result_plan->righttree->allParam == NULL)
                check_rescan = false;
            else
                check_rescan = true;
            if (vector_engine_walker_internal(result_plan->righttree, check_rescan, planContext))
                return true;
            CostVectorNestJoin((Join*)result_plan, planContext);
        } break;

        case T_HashJoin: {
            /* Vector Hash Full Join is not yet implemented */
            Join* j = (Join*)result_plan;
            if (j->jointype == JOIN_FULL)
                return true;

            HashJoin* hj = (HashJoin*)result_plan;
            /* Find unsupport expr in *Hash* clause */
            if (vector_engine_unsupport_expression_walker((Node*)hj->hashclauses, planContext))
                return true;
            /* Find unsupport expr in *Join* clause */
            if (vector_engine_unsupport_expression_walker((Node*)hj->join.joinqual, planContext))
                return true;
            if (vector_engine_unsupport_expression_walker((Node*)hj->join.nulleqqual, planContext))
                return true;

            if (vector_engine_walker_internal(result_plan->lefttree, check_rescan, planContext))
                return true;
            if (vector_engine_walker_internal(result_plan->righttree, check_rescan, planContext))
                return true;
            CostVectorHashJoin((Join*)result_plan, planContext);
        } break;

        case T_Append: {
            Append* append = (Append*)result_plan;
            ListCell* lc = NULL;
            foreach (lc, append->appendplans) {
                if (vector_engine_walker_internal((Plan*)lfirst(lc), check_rescan, planContext))
                    return true;
            }
        } break;

        case T_ModifyTable: {
            ModifyTable* mt = (ModifyTable*)result_plan;
            ListCell* lc = NULL;
            foreach (lc, mt->plans) {
                if (vector_engine_walker_internal((Plan*)lfirst(lc), check_rescan, planContext))
                    return true;
            }
        } break;

        case T_SubqueryScan: {
            SubqueryScan* ss = (SubqueryScan*)result_plan;
            if (ss->subplan && vector_engine_walker_internal(ss->subplan, check_rescan, planContext))
                return true;
        } break;

        case T_ForeignScan: {
            return CheckForeignScanExpr(result_plan, planContext);
        } break;

        case T_ExtensiblePlan: {
            ExtensiblePlan* ext_plan = (ExtensiblePlan*)result_plan;
            ListCell* lc = NULL;
            foreach (lc, ext_plan->extensible_plans) {
                if (vector_engine_walker_internal((Plan*)lfirst(lc), check_rescan, planContext))
                    return true;
            }
        } break;

        default:
            break;
    }

    return false;
}

/*
 * @Description: Walk through the plan tree to see if it's supported in Vector Engine
 *
 * @param[IN] result_plan:  current plan node
 * @param[IN] check_rescan:  if need check rescan
 * @return: bool, true means unsupported, false means supported
 */
static bool vector_engine_walker(Plan* result_plan, bool check_rescan)
{
    VectorPlanContext planContext;
    planContext.containRowTable = false;
    planContext.currentExprIsFilter = false;
    planContext.rowCost = 0.0;
    planContext.vecCost = 0.0;

    /* for OPT_VECTOR_ENGINE, we treat plan can be transformed to vectorized plan,
     * and if the plan not satisfied rules to vectorize, will return false later.
     */
    if (u_sess->attr.attr_sql.vectorEngineStrategy == OFF_VECTOR_ENGINE) {
        planContext.forceVectorEngine = false;
    } else {
        planContext.forceVectorEngine = true;
    }

    bool res = vector_engine_walker_internal(result_plan, check_rescan, &planContext);

    if (!res && u_sess->attr.attr_sql.vectorEngineStrategy == OPT_VECTOR_ENGINE && planContext.containRowTable) {
        /* add vectorow cost for using vector engine */
        Cost vecToRowCost = 0.2;
        Cost vecToRowCosts = vecToRowCost * result_plan->plan_rows;
        planContext.vecCost += vecToRowCosts;
        /* vector cost multiply 1.2 to ensure that performance not degree */
        planContext.vecCost *= 1.2;
        ereport(DEBUG2, (errmodule(MOD_OPT_PLANNER),
            errmsg("[ROWTOVEC OPTIMAL] total cost: row: %f, vector: %f, choose %s",
            planContext.rowCost, planContext.vecCost,
            (planContext.vecCost >= planContext.rowCost ? "row" : "vector"))));
        /* while using vector engine cost is larger than using row engine, do not transform to vector plan */
        if (planContext.vecCost >= planContext.rowCost) {
            res = true;
        }
    }

    return res;
}

/*
 * @Description: Fallback plan, generate hybrid row-column plan
 *
 * @param[IN] result_plan:  current plan node
 * @return: Plan*, fallbacked plan
 */
static Plan* fallback_plan(Plan* result_plan)
{
    if (result_plan == NULL)
        return NULL;

    switch (nodeTag(result_plan)) {
        /* Add Row Adapter */
        case T_CStoreScan:
        case T_DfsScan:
        case T_DfsIndexScan:
        case T_CStoreIndexScan:
        case T_CStoreIndexHeapScan:
        case T_CStoreIndexCtidScan:
#ifdef ENABLE_MULTIPLE_NODES
        case T_TsStoreScan:
#endif   /* ENABLE_MULTIPLE_NODES */
            result_plan = (Plan*)make_vectorow(build_vector_plan(result_plan));
            break;
        /* vec_output was set to 'true' initially, change to 'false' in row plan */
        case T_ForeignScan:
            result_plan->vec_output = false;
            break;
        case T_ExtensiblePlan: {
            ListCell* lc = NULL;
            ExtensiblePlan* ext_plans = (ExtensiblePlan*) result_plan;
            foreach (lc, ext_plans->extensible_plans) {
                Plan* plan = (Plan*)lfirst(lc);
                plan = (Plan*)fallback_plan(plan);
                if (IsVecOutput(plan)) {
                    plan = (Plan*)make_vectorow(plan);
                }
                lfirst(lc) = plan;
            }
        } break;
        case T_RemoteQuery:
            if (!IsVecOutput(result_plan) && IsVecOutput(result_plan->lefttree) &&
                IsA(result_plan->lefttree, ModifyTable)) {
                result_plan->type = T_VecRemoteQuery;
                result_plan->vec_output = true;
                result_plan = (Plan*)make_vectorow(result_plan);
            }
            result_plan->lefttree = fallback_plan(result_plan->lefttree);
            break;

        case T_Limit:
        case T_PartIterator:
        case T_SetOp:
        case T_Group:
        case T_Unique:
        case T_BaseResult:
        case T_Sort:
        case T_Stream:
        case T_Material:
        case T_StartWithOp:
        case T_WindowAgg:
        case T_Hash:
        case T_Agg:
        case T_RowToVec:
        case T_VecRemoteQuery:
            result_plan->lefttree = fallback_plan(result_plan->lefttree);
            break;

        case T_MergeJoin:
        case T_NestLoop:
        case T_HashJoin:
        case T_RecursiveUnion:
            result_plan->lefttree = fallback_plan(result_plan->lefttree);
            result_plan->righttree = fallback_plan(result_plan->righttree);
            break;

        case T_Append: {
            Append* append = (Append*)result_plan;
            ListCell* lc = NULL;
            foreach (lc, append->appendplans) {
                Plan* plan = (Plan*)lfirst(lc);
                plan = (Plan*)fallback_plan(plan);
                if (IsVecOutput(plan)) {
                    plan = (Plan*)make_vectorow(plan);
                }
                lfirst(lc) = plan;
            }
        } break;

        case T_ModifyTable: {
            ModifyTable* mt = (ModifyTable*)result_plan;
            ListCell* lc = NULL;
            foreach (lc, mt->plans) {
                Plan* plan = (Plan*)lfirst(lc);

                if (IsVecOutput(result_plan)) {
                    result_plan->type = T_VecModifyTable;

                    if (!IsVecOutput(plan))
                        lfirst(lc) = (Plan*)fallback_plan((Plan*)make_rowtovec(plan));
                    else if (IsA(plan, CStoreScan) || IsA(plan, CStoreIndexScan))
                        break;
                } else
                    lfirst(lc) = (Plan*)fallback_plan(plan);
            }
        } break;

        case T_SubqueryScan: {
            SubqueryScan* ss = (SubqueryScan*)result_plan;
            if (ss->subplan)
                ss->subplan = (Plan*)fallback_plan(ss->subplan);
        } break;

        case T_MergeAppend: {
            MergeAppend* ma = (MergeAppend*)result_plan;
            ListCell* lc = NULL;
            foreach (lc, ma->mergeplans) {
                Plan* plan = (Plan*)lfirst(lc);
                lfirst(lc) = (Plan*)fallback_plan(plan);
            }
        } break;

        default:
            break;
    }

    return result_plan;
}

static inline Plan* make_rowtove_plan(Plan* plan)
{
    make_dummy_targetlist(plan);
    return (Plan *)make_rowtovec(plan);
}

/*
 * @Description: Generate vectorized plan
 *
 * @param[IN] result_plan:  current plan node
 * @param[IN] ignore_remotequery:  if ignore RemoteQuery node
 * @return: Plan*, vectorized plan
 */
Plan* vectorize_plan(Plan* result_plan, bool ignore_remotequery, bool forceVectorEngine)
{
    if (result_plan == NULL)
        return NULL;

    switch (nodeTag(result_plan)) {
        /*
         * For Scan node, just leave it.
         */
        case T_DfsScan:
        case T_DfsIndexScan:
        case T_CStoreScan:
        case T_CStoreIndexCtidScan:
        case T_CStoreIndexHeapScan:
        case T_CStoreIndexScan:
#ifdef ENABLE_MULTIPLE_NODES
        case T_TsStoreScan:
#endif   /* ENABLE_MULTIPLE_NODES */
            result_plan = build_vector_plan(result_plan);
            break;
        case T_ForeignScan:
            if (IsVecOutput(result_plan)) {
                return build_vector_plan(result_plan);
            } else if (forceVectorEngine) {
                result_plan = make_rowtove_plan(result_plan);
            }
            break;
        case T_ExtensiblePlan: {
            ExtensiblePlan* ext_plans = (ExtensiblePlan*)result_plan;
            ListCell* lc = NULL;
            List* newPlans = NIL;

            foreach (lc, ext_plans->extensible_plans) {
                Plan* plan = (Plan*)lfirst(lc);
                lfirst(lc) = vectorize_plan(plan, ignore_remotequery, forceVectorEngine);
                if (IsVecOutput(result_plan) && !IsVecOutput(plan)) {
                    if (IsA(plan, ForeignScan)) {
                        build_vector_plan(plan);
                    } else {
                        plan = (Plan*)make_rowtovec(plan);
                    }
                } else if (!IsVecOutput(result_plan) && IsVecOutput(plan)) {
                    plan = (Plan*)make_vectorow(plan);
                }
                newPlans = lappend(newPlans, plan);
            }
            ext_plans->extensible_plans = newPlans;
            if (IsVecOutput(result_plan)) {
                build_vector_plan(result_plan);
            }
            break;
        }
        case T_SeqScan: {
            if (result_plan->isDeltaTable || forceVectorEngine) {
                result_plan = make_rowtove_plan(result_plan);
            }
            break;
        }
        case T_IndexScan:
        case T_IndexOnlyScan:
        case T_BitmapHeapScan:
        case T_TidScan:
        case T_FunctionScan: {
            if (forceVectorEngine) {
                result_plan = make_rowtove_plan(result_plan);
            }
            break;
        }
        case T_ValuesScan: {
            result_plan = (Plan*)make_rowtovec(result_plan);
        } break;
        /*
         * For those node that support vectorize, build vector node if child is
         * vector or enable_force_vector_engine.
         */
        case T_RemoteQuery:
            if (ignore_remotequery)
                return result_plan;
        case T_Limit:
        case T_PartIterator:
        case T_SetOp:
        case T_Group:
        case T_Unique:
        case T_BaseResult:
        case T_Sort:
        case T_StartWithOp:
        case T_Stream:
        case T_Material:
        case T_WindowAgg:
            result_plan->lefttree = vectorize_plan(result_plan->lefttree, ignore_remotequery, forceVectorEngine);
            if (result_plan->lefttree && IsVecOutput(result_plan->lefttree)) {
                return build_vector_plan(result_plan);
            } else if ((result_plan->lefttree && !IsVecOutput(result_plan->lefttree)) &&
                     u_sess->attr.attr_sql.enable_force_vector_engine) {
                result_plan->lefttree = (Plan*)make_rowtovec(result_plan->lefttree);
                return build_vector_plan(result_plan);
            } else if (IsA(result_plan, BaseResult) && result_plan->lefttree == NULL) {
                return make_rowtove_plan(result_plan);
            }
            break;

        case T_MergeJoin:
        case T_NestLoop:
            result_plan->lefttree = vectorize_plan(result_plan->lefttree, ignore_remotequery, forceVectorEngine);
            result_plan->righttree = vectorize_plan(result_plan->righttree, ignore_remotequery, forceVectorEngine);

            if (IsVecOutput(result_plan->lefttree) && IsVecOutput(result_plan->righttree)) {
                return build_vector_plan(result_plan);
            }

            if (u_sess->attr.attr_sql.enable_force_vector_engine) {
                if (!IsVecOutput(result_plan->lefttree))
                    result_plan->lefttree = (Plan*)make_rowtovec(result_plan->lefttree);
                if (!IsVecOutput(result_plan->righttree))
                    result_plan->righttree = (Plan*)make_rowtovec(result_plan->righttree);
                return build_vector_plan(result_plan);
            } else {
                if (IsVecOutput(result_plan->lefttree))
                    result_plan->lefttree = (Plan*)make_vectorow(result_plan->lefttree);
                if (IsVecOutput(result_plan->righttree))
                    result_plan->righttree = (Plan*)make_vectorow(result_plan->righttree);
                return result_plan;
            }
        /*
         * For those node with only child node that support vectorize, we just mark the vector flag
         * according to its child node flag.
         */
        case T_Hash:
            break;
        case T_Agg: {
            result_plan->lefttree = vectorize_plan(result_plan->lefttree, ignore_remotequery, forceVectorEngine);
            if (IsVecOutput(result_plan->lefttree))
                return build_vector_plan(result_plan);
        } break;
        /*
         * For those node with only two nodes that support vectorize, we try to go vector.
         */
        case T_HashJoin: {
            /* HashJoin supports vector right now */
            result_plan->lefttree = vectorize_plan(result_plan->lefttree, ignore_remotequery, forceVectorEngine);
            result_plan->righttree->lefttree =
                vectorize_plan(result_plan->righttree->lefttree, ignore_remotequery, forceVectorEngine);

            if (IsVecOutput(result_plan->lefttree) && IsVecOutput(result_plan->righttree->lefttree)) {
                /* Remove hash node */
                result_plan->righttree = result_plan->righttree->lefttree;

                return build_vector_plan(result_plan);
            } else {
                if (IsVecOutput(result_plan->lefttree))
                    result_plan->lefttree = (Plan*)make_vectorow(result_plan->lefttree);
                if (IsVecOutput(result_plan->righttree->lefttree))
                    result_plan->righttree->lefttree = (Plan*)make_vectorow(result_plan->righttree->lefttree);
            }
        } break;

        case T_Append: {
            Append* append = (Append*)result_plan;
            ListCell* lc = NULL;
            bool isVec = true;
            foreach (lc, append->appendplans) {
                Plan* plan = (Plan*)lfirst(lc);
                plan = vectorize_plan(plan, ignore_remotequery, forceVectorEngine);
                lfirst(lc) = plan;
                if (!IsVecOutput(plan)) {
                    if (u_sess->attr.attr_sql.enable_force_vector_engine)
                        lfirst(lc) = (Plan*)make_rowtovec(plan);
                    isVec = false;
                }
            }
            if (isVec == true || u_sess->attr.attr_sql.enable_force_vector_engine) {
                return build_vector_plan(result_plan);
            } else {
                foreach (lc, append->appendplans) {
                    Plan* plan = (Plan*)lfirst(lc);
                    if (IsVecOutput(plan)) {
                        lfirst(lc) = (Plan*)make_vectorow(plan);
                    }
                }
                return result_plan;
            }
        } break;

        case T_ModifyTable: {
            /* ModifyTable doesn't support vector right now */
            ModifyTable* mt = (ModifyTable*)result_plan;
            ListCell* lc = NULL;
            List* newPlans = NIL;

            foreach (lc, mt->plans) {
                Plan* plan = (Plan*)lfirst(lc);
                lfirst(lc) = vectorize_plan(plan, ignore_remotequery, forceVectorEngine);
                /* If we support vectorize ModifyTable, please remove it */
                if (IsVecOutput(result_plan) && !IsVecOutput(plan)) {
                    if (IsA(plan, ForeignScan)) {
                        build_vector_plan(plan);
                    } else {
                        plan = (Plan*)make_rowtovec(plan);
                    }
                } else if (!IsVecOutput(result_plan) && IsVecOutput(plan)) {
                    plan = (Plan*)make_vectorow(plan);
                }
                newPlans = lappend(newPlans, plan);
            }
            mt->plans = newPlans;
            if (IsVecOutput(result_plan)) {
                build_vector_plan(result_plan);
            }
            break;
        }

        case T_SubqueryScan: {
            /* SubqueryScan supports vector right now */
            SubqueryScan* ss = (SubqueryScan*)result_plan;
            if (ss->subplan)
                ss->subplan = vectorize_plan(ss->subplan, ignore_remotequery, forceVectorEngine);
            if (IsVecOutput(ss->subplan)) {  // If we support vectorize ModifyTable, please remove it
                build_vector_plan(result_plan);
            }
            break;
        }

        default:
            break;
    }

    return result_plan;
}

/*
 * @Description: Generate vectorized plan
 *
 * @param[IN] result_plan:  current plan node
 * @return: Plan*, vectorized plan node
 */
static Plan* build_vector_plan(Plan* plan)
{
    make_dummy_targetlist(plan);
    plan->vec_output = true;

    /*
     * For nodetype T_CStoreIndexHeapScan/T_CStoreIndexCtidScan/
     * T_CStoreIndexAnd/T_CStoreIndexOr, we have dealed with colstore
     * case in create_scan_plan.
     *
     */
    switch (nodeTag(plan)) {
        case T_NestLoop:
            plan->type = T_VecNestLoop;
            break;
        case T_MergeJoin:
            plan->type = T_VecMergeJoin;
            break;
        case T_WindowAgg:
            plan->type = T_VecWindowAgg;
            break;
        case T_Limit:
            plan->type = T_VecLimit;
            break;
        case T_Agg:
            plan->type = T_VecAgg;
            break;
        case T_DfsScan:
        case T_DfsIndexScan:
        case T_CStoreScan:
        case T_CStoreIndexCtidScan:
        case T_CStoreIndexHeapScan:
        case T_CStoreIndexScan:
#ifdef ENABLE_MULTIPLE_NODES
        case T_TsStoreScan:
#endif   /* ENABLE_MULTIPLE_NODES */
            break;
        case T_Hash:  // we should remove hash node in the vector plan
            break;
        case T_HashJoin:
            plan->type = T_VecHashJoin;
            break;
        case T_RemoteQuery:
            plan->type = T_VecRemoteQuery;
            break;
        case T_Stream:
            plan->type = T_VecStream;
            break;
        case T_SubqueryScan:
            plan->type = T_VecSubqueryScan;
            break;
        case T_BaseResult:
            plan->type = T_VecResult;
            break;
        case T_PartIterator:
            plan->type = T_VecPartIterator;
            break;
        case T_ForeignScan:
            plan->type = T_VecForeignScan;
            break;
        case T_Append:
            plan->type = T_VecAppend;
            break;
        case T_Group:
            plan->type = T_VecGroup;
            break;
        case T_Unique:
            plan->type = T_VecUnique;
            break;
        case T_SetOp:
            plan->type = T_VecSetOp;
            break;
        case T_ModifyTable:
            plan->type = T_VecModifyTable;
            break;
        case T_Sort:
            plan->type = T_VecSort;
            break;
        case T_Material:
            plan->type = T_VecMaterial;
            break;
        default:
            plan->vec_output = false;
            break;
    }
    return plan;
}

bool CheckColumnsSuportedByBatchMode(List *targetList, List *qual)
{
    List *vars = NIL;
    ListCell *l = NULL;

    /* Consider the  targetList */
    foreach (l, targetList) {
        ListCell *vl = NULL;
        GenericExprState *gstate = (GenericExprState *)lfirst(l);
        TargetEntry *tle = (TargetEntry *)gstate->xprstate.expr;

        /* if have set-returning function, not support. */
        if (vector_engine_setfunc_walker((Node*)tle, NULL)) {
            return false;
        }

        /* Pull vars from  the targetlist. */
        vars = pull_var_clause((Node *)tle, PVC_RECURSE_AGGREGATES, PVC_RECURSE_PLACEHOLDERS);

        foreach (vl, vars) {
            Var *var = (Var *)lfirst(vl);
            if (var->varattno < 0 || !IsTypeSupportedByCStore(var->vartype)) {
                return false;
            }
        }
    }

    /* Now consider the quals */
    vars = pull_var_clause((Node *)qual, PVC_RECURSE_AGGREGATES, PVC_RECURSE_PLACEHOLDERS);
    foreach (l, vars) {
        Var *var = (Var *)lfirst(l);
        if (var->varattno < 0 || !IsTypeSupportedByCStore(var->vartype)) {
            return false;
        }
    }
    return true;
}

/*
 * cost_agg_convert_to_path
 *     convert subplan to path before we calculate agg cost of each hash agg method
 *
 * @param (in) plan:
 *     the sub-plan
 *
 * @return:
 *     the converted path
 */
static Path* cost_agg_convert_to_path(Plan* plan)
{
    Path* path = makeNode(Path);

    path->type = T_Path;
    path->pathtype = plan->type;

    /*
     * This distribution is used for cost estimation,
     * we should get it from exec nodes (not data nodes).
     */
    ExecNodes* exec_nodes = ng_get_dest_execnodes(plan);
    Distribution* distribution = ng_convert_to_distribution(exec_nodes);
    ng_set_distribution(&path->distribution, distribution);
    path->locator_type = ng_get_dest_locator_type(plan);
    path->distribute_keys = ng_get_dest_distribute_keys(plan);

    if (IsLocatorReplicated(path->locator_type)) {
        path->rows = PLAN_LOCAL_ROWS(plan);
    } else {
        path->rows = plan->plan_rows;
    }
    path->multiple = plan->multiple;
    path->startup_cost = plan->startup_cost;
    path->total_cost = plan->total_cost;

    return path;
}

/*
 * cost_agg_do_redistribute
 *     add redistribute path node and calculate it's cost
 *     when we choose optimal method from all hash agg methods
 *
 * @param (in) subpath:
 *     the subpath
 * @param (in) distributed_key:
 *     distribute key of redistribute stream node
 * @param (in) multiple:
 *     the multiple
 * @param (in) target_distribution:
 *     the target node group
 * @param (in) width:
 *     the path width
 * @param (in) vec_output:
 *     mark wheather it's a vector plan node
 * @param (in) dop:
 *     the dop of SMP
 *
 * @return:
 *     the stream path
 */
static StreamPath* cost_agg_do_redistribute(Path* subpath, List* distributed_key, double multiple,
    Distribution* target_distribution, int width, bool vec_output, int dop, bool needs_stream)
{
    StreamPath* spath = makeNode(StreamPath);

    spath->path.type = T_StreamPath;
    spath->path.pathtype = vec_output ? T_VecStream : T_Stream;

    spath->path.multiple = multiple;

    Distribution* distribution = ng_get_dest_distribution(subpath);
    ng_set_distribution(&spath->path.distribution, distribution);
    spath->path.locator_type = LOCATOR_TYPE_HASH;
    spath->path.distribute_keys = distributed_key;

    spath->type = STREAM_REDISTRIBUTE;
    spath->subpath = subpath;
    ng_set_distribution(&spath->consumer_distribution, target_distribution);

    if (dop > 1) {
        spath->smpDesc = create_smpDesc(dop, dop, needs_stream ? REMOTE_SPLIT_DISTRIBUTE : LOCAL_DISTRIBUTE);
    }

    cost_stream(spath, width);

    return spath;
}

/*
 * cost_agg_do_gather
 *     add gather path node and calculate it's cost
 *     when we choose optimal method from all hash agg methods
 *
 * @param (in) subpath:
 *     the subpath
 * @param (in) width:
 *     the path width
 * @param (in) vec_output:
 *     mark wheather it's a vector plan node
 *
 * @return:
 *     the gather path
 */
static StreamPath* cost_agg_do_gather(Path* subpath, int width, bool vec_output)
{
    StreamPath* spath = makeNode(StreamPath);

    spath->path.type = T_StreamPath;
    spath->path.pathtype = vec_output ? T_VecStream : T_Stream;

    spath->path.multiple = 1.0;

    Distribution* producer_distribution = ng_get_dest_distribution(subpath);
    ng_set_distribution(&spath->path.distribution, producer_distribution);
    spath->path.locator_type = LOCATOR_TYPE_REPLICATED;

    spath->type = STREAM_GATHER;
    spath->subpath = subpath;
    /* It's in CN, NOT really in DN_0. Just make local rows and global rows calculation happy */
    Distribution* consumer_distribution = ng_get_single_node_group_distribution();
    ng_set_distribution(&spath->consumer_distribution, consumer_distribution);

    cost_stream(spath, width);

    return spath;
}

/*
 * cost_agg_do_agg
 *     add agg path node and calculate it's cost
 *     when we choose optimal method from all hash agg methods
 *
 * @return:
 *     the agg path
 */
static Path* cost_agg_do_agg(Path* subpath, PlannerInfo* root, AggStrategy agg_strategy, const AggClauseCosts* aggcosts,
    int numGroupCols, double numGroups, Size hashentrysize, QualCost total_cost, int width, bool vec_output, int dop)
{
    Path* agg_path = makeNode(Path);

    agg_path->type = T_Path;
    agg_path->pathtype = vec_output ? T_VecAgg : T_Agg;

    Distribution* distribution = ng_get_dest_distribution(subpath);
    ng_set_distribution(&agg_path->distribution, distribution);
    agg_path->locator_type = subpath->locator_type;
    agg_path->distribute_keys = subpath->distribute_keys;

    cost_agg(agg_path,
        root,
        agg_strategy,
        aggcosts,
        numGroupCols,
        numGroups,
        subpath->startup_cost,
        subpath->total_cost,
        PATH_LOCAL_ROWS(subpath),
        width,
        hashentrysize,
        dop);
    agg_path->startup_cost += total_cost.startup;
    agg_path->total_cost += total_cost.startup + total_cost.per_tuple * PATH_LOCAL_ROWS(agg_path);

    return agg_path;
}

/*
 * get_hashagg_gather_hashagg_path: get result path for agg(dn)->gather->agg(cn).
 *
 * Parameters:
 *	@in root: plan info node
 *	@in	lefttree: the current plan
 *	@in	aggcosts: the execution costs of the aggregates' input expressions
 *	@in numGroupCols: the column num of group by clause
 *	@in	numGroups: the local distinct of group by clause for the first level
 *	@in	final_groups: the global distinct of group by clause for the final leve
 *	@in total_cost: the initial total cost for qual
 *	@in	hashentrysize: hash entry size include space for per tuple width, space for pass-by-ref transition values,
 *		the per-hash-entry overhead
 *	@in	result_path: the result path for agg(dn)->gather->agg(cn) with total cost
 *
 * Returns: void
 */
static void get_hashagg_gather_hashagg_path(PlannerInfo* root, Plan* lefttree, const AggClauseCosts* aggcosts,
    int numGroupCols, double numGroups, double final_groups, QualCost total_cost, Size hashentrysize,
    AggStrategy agg_strategy, bool needs_stream, Path* result_path)
{
    Path* subpath = cost_agg_convert_to_path(lefttree);

    Path* agg_path_1 = cost_agg_do_agg(subpath,
        root,
        agg_strategy,
        aggcosts,
        numGroupCols,
        numGroups,
        hashentrysize,
        total_cost,
        lefttree->plan_width,
        lefttree->vec_output,
        lefttree->dop);

    ereport(DEBUG1, (errmodule(MOD_OPT_AGG), errmsg("Agg method1 cost after dn agg: %lf", agg_path_1->total_cost)));

    StreamPath* gather_path = cost_agg_do_gather(agg_path_1, lefttree->plan_width, lefttree->vec_output);

    ereport(
        DEBUG1, (errmodule(MOD_OPT_AGG), errmsg("Agg method1 cost after gather: %lf", gather_path->path.total_cost)));

    if (needs_stream) {
        /* The agg above gather can not be parallelized. */
        Path* agg_path_2 = cost_agg_do_agg((Path*)gather_path,
            root,
            agg_strategy,
            aggcosts,
            numGroupCols,
            final_groups,
            hashentrysize,
            total_cost,
            lefttree->plan_width,
            lefttree->vec_output,
            1);

        ereport(DEBUG1, (errmodule(MOD_OPT_AGG), errmsg("Agg method1 cost after cn agg: %lf", agg_path_2->total_cost)));

        debug_print_agg_detail(root, AGG_HASHED, DN_AGG_CN_AGG, agg_path_2, &gather_path->path, agg_path_1);

        copy_path_costsize(result_path, agg_path_2);
    } else {
        if (root->query_level == 1) {
            debug_print_agg_detail(root, AGG_HASHED, DN_AGG_CN_AGG, &gather_path->path, agg_path_1);

            copy_path_costsize(result_path, &gather_path->path);
        } else {
            debug_print_agg_detail(root, AGG_HASHED, DN_AGG_CN_AGG, agg_path_1);

            copy_path_costsize(result_path, agg_path_1);
        }
    }
}
#ifdef ENABLE_MULTIPLE_NODES
/*
 * get_redist_hashagg_gather_hashagg_path
 *     get result path for redist->agg(dn)->gather->agg(cn).
 *
 * Parameters:
 *	@in root: plan info node
 *	@in	lefttree: the current plan
 *	@in	aggcosts: the execution costs of the aggregates' input expressions
 *	@in numGroupCols: the column num of group by clause
 *	@in	numGroups: the local distinct of group by clause for the first level
 *	@in	final_groups: the global distinct of group by clause for the final leve
 *  @in distributed_key_less_skew: the less skewed distribute key
 *  @in multiple_less_skew: the multiple of the less skewed distribute key
 *  @in target_distribution: the target node group
 *	@in total_cost: the initial total cost for qual
 *	@in	hashentrysize: hash entry size include space for per tuple width, space for pass-by-ref transition values,
 *		the per-hash-entry overhead
 *	@in	result_path: the result path for agg(dn)->gather->agg(cn) with total cost
 *
 * Returns: void
 */
static void get_redist_hashagg_gather_hashagg_path(PlannerInfo* root, Plan* lefttree, const AggClauseCosts* aggcosts,
    int numGroupCols, double numGroups, double final_groups, List* distributed_key_less_skew, double multiple_less_skew,
    Distribution* target_distribution, QualCost total_cost, Size hashentrysize, AggStrategy agg_strategy,
    bool needs_stream, Path* result_path)
{
    Path* subpath = cost_agg_convert_to_path(lefttree);

    AssertEreport(target_distribution != NULL && target_distribution->bms_data_nodeids != NULL,
        MOD_OPT,
        "invalid target distribution information or its bitmap set is null.");

    StreamPath* redist_path = cost_agg_do_redistribute(subpath,
        distributed_key_less_skew,
        multiple_less_skew,
        target_distribution,
        lefttree->plan_width,
        lefttree->vec_output,
        lefttree->dop,
        needs_stream);

    ereport(DEBUG1,
        (errmodule(MOD_OPT_AGG), errmsg("Agg method 4 (1+) cost after dn redist: %lf", redist_path->path.total_cost)));

    Path* agg_path_1 = cost_agg_do_agg((Path*)redist_path,
        root,
        agg_strategy,
        aggcosts,
        numGroupCols,
        numGroups,
        hashentrysize,
        total_cost,
        lefttree->plan_width,
        lefttree->vec_output,
        lefttree->dop);

    ereport(
        DEBUG1, (errmodule(MOD_OPT_AGG), errmsg("Agg method 4 (1+) cost after dn agg: %lf", agg_path_1->total_cost)));

    StreamPath* gather_path = cost_agg_do_gather(agg_path_1, lefttree->plan_width, lefttree->vec_output);

    ereport(DEBUG1,
        (errmodule(MOD_OPT_AGG), errmsg("Agg method 4 (1+) cost after gather: %lf", gather_path->path.total_cost)));

    if (needs_stream) {
        /* The agg above gather can not be parallelized. */
        Path* agg_path_2 = cost_agg_do_agg((Path*)gather_path,
            root,
            agg_strategy,
            aggcosts,
            numGroupCols,
            final_groups,
            hashentrysize,
            total_cost,
            lefttree->plan_width,
            lefttree->vec_output,
            1);

        ereport(DEBUG1,
            (errmodule(MOD_OPT_AGG), errmsg("Agg method 4 (1+) cost after cn agg: %lf", agg_path_2->total_cost)));

        debug_print_agg_detail(root,
            AGG_HASHED,
            DN_REDISTRIBUTE_AGG_CN_AGG,
            agg_path_2,
            &gather_path->path,
            agg_path_1,
            &redist_path->path);

        copy_path_costsize(result_path, agg_path_2);
    } else {
        if (root->query_level == 1) {
            debug_print_agg_detail(
                root, AGG_HASHED, DN_REDISTRIBUTE_AGG_CN_AGG, &gather_path->path, agg_path_1, &redist_path->path);

            copy_path_costsize(result_path, &gather_path->path);
        } else {
            debug_print_agg_detail(root, AGG_HASHED, DN_REDISTRIBUTE_AGG_CN_AGG, agg_path_1, &redist_path->path);

            copy_path_costsize(result_path, agg_path_1);
        }
    }
}
#endif
/*
 * get_redist_hashagg_path: get result path for distributecost() + aggcost() + gathercost().
 *
 * Parameters:
 *	@in root: plan info node
 *	@in	lefttree: the current plan
 *	@in	aggcosts: the execution costs of the aggregates' input expressions
 *	@in numGroupCols: the column num of group by clause
 *	@in	numGroups: the local distinct of group by clause for the first level
 *	@in	final_groups: the global distinct of group by clause for the final level
 *	@in	distributed_key: the distribute key for stream
 *	@in	multiple: the multiple for stream
 *	@in total_cost: the initial total cost for qual
 *	@in	hashentrysize: hash entry size include space for per tuple width, space for pass-by-ref transition values,
 *		the per-hash-entry overhead
 *	@in	result_path: the result path for distributecost() + aggcost() + gathercost() with total cost
 *
 * Returns: void
 */
static void get_redist_hashagg_path(PlannerInfo* root, Plan* lefttree, const AggClauseCosts* aggcosts, int numGroupCols,
    double numGroups, double final_groups, List* distributed_key, double multiple, Distribution* target_distribution,
    QualCost total_cost, Size hashentrysize, bool needs_stream, Path* result_path)
{
    Path* subpath = cost_agg_convert_to_path(lefttree);

    if (target_distribution == NULL || (
        target_distribution->bms_data_nodeids == NULL &&
        target_distribution->group_oid == InvalidOid)) {
        target_distribution = ng_get_installation_group_distribution();
    }

    StreamPath* redist_path = cost_agg_do_redistribute(subpath,
        distributed_key,
        multiple,
        target_distribution,
        lefttree->plan_width,
        lefttree->vec_output,
        lefttree->dop,
        needs_stream);

    ereport(DEBUG1,
        (errmodule(MOD_OPT_AGG), errmsg("Agg method2 cost after redistribute: %lf", redist_path->path.total_cost)));

    double numGroups_agg_path =
        clamp_row_est(get_local_rows(final_groups, 1.0, false, ng_get_dest_num_data_nodes((Path*)redist_path)));
    Path* agg_path = cost_agg_do_agg((Path*)redist_path,
        root,
        AGG_HASHED,
        aggcosts,
        numGroupCols,
        numGroups_agg_path,
        hashentrysize,
        total_cost,
        lefttree->plan_width,
        lefttree->vec_output,
        lefttree->dop);

    ereport(DEBUG1, (errmodule(MOD_OPT_AGG), errmsg("Agg method2 cost after agg: %lf", agg_path->total_cost)));

    if (root->query_level == 1) {
        StreamPath* gather_path = cost_agg_do_gather(agg_path, lefttree->plan_width, lefttree->vec_output);

        ereport(DEBUG1,
            (errmodule(MOD_OPT_AGG), errmsg("Agg method2 cost after gather: %lf", gather_path->path.total_cost)));

        debug_print_agg_detail(root, AGG_HASHED, DN_REDISTRIBUTE_AGG, &gather_path->path, agg_path, &redist_path->path);

        copy_path_costsize(result_path, &gather_path->path);
    } else {
        debug_print_agg_detail(root, AGG_HASHED, DN_REDISTRIBUTE_AGG, agg_path, &redist_path->path);

        copy_path_costsize(result_path, agg_path);
    }

    result_path->distribute_keys = distributed_key;
}

/*
 * get_hashagg_redist_hashagg_path: get result path for aggcost() + distributecost() + aggcost() + gathercost().
 *
 * Parameters:
 *	@in root: plan info node
 *	@in	lefttree: the current plan
 *	@in	aggcosts: the execution costs of the aggregates' input expressions
 *	@in numGroupCols: the column num of group by clause
 *	@in	numGroups: the local distinct of group by clause for the first level
 *	@in	final_groups: the global distinct of group by clause for the final level
 *	@in	distributed_key: the distribute key for stream
 *	@in	multiple: the multiple for stream
 *	@in total_cost: the initial total cost for qual
 *	@in	hashentrysize: hash entry size include space for per tuple width, space for pass-by-ref transition values,
 *		the per-hash-entry overhead
 *	@in	result_path: the result path for aggcost() + distributecost() + aggcost() + gathercost() with total cost
 *
 * Returns: void
 */
static void get_hashagg_redist_hashagg_path(PlannerInfo* root, Plan* lefttree, const AggClauseCosts* aggcosts,
    int numGroupCols, double numGroups, double final_groups, List* distributed_key, double multiple,
    Distribution* target_distribution, QualCost total_cost, Size hashentrysize, bool needs_stream, Path* result_path)
{
    Path* subpath = cost_agg_convert_to_path(lefttree);

    if (target_distribution == NULL || (
        target_distribution->bms_data_nodeids == NULL &&
        target_distribution->group_oid == InvalidOid)) {
        target_distribution = ng_get_installation_group_distribution();
    }

    Path* agg_path_1 = cost_agg_do_agg(subpath,
        root,
        AGG_HASHED,
        aggcosts,
        numGroupCols,
        numGroups,
        hashentrysize,
        total_cost,
        lefttree->plan_width,
        lefttree->vec_output,
        lefttree->dop);

    ereport(DEBUG1, (errmodule(MOD_OPT_AGG), errmsg("Agg method3 cost after first agg: %lf", agg_path_1->total_cost)));

    StreamPath* redist_path = cost_agg_do_redistribute(agg_path_1,
        distributed_key,
        multiple,
        target_distribution,
        lefttree->plan_width,
        lefttree->vec_output,
        lefttree->dop,
        needs_stream);

    ereport(DEBUG1,
        (errmodule(MOD_OPT_AGG), errmsg("Agg method3 cost after redistribute: %lf", redist_path->path.total_cost)));

    double numGroups_agg_path_2 =
        clamp_row_est(get_local_rows(final_groups, 1.0, false, ng_get_dest_num_data_nodes((Path*)redist_path)));
    Path* agg_path_2 = cost_agg_do_agg((Path*)redist_path,
        root,
        AGG_HASHED,
        aggcosts,
        numGroupCols,
        numGroups_agg_path_2,
        hashentrysize,
        total_cost,
        lefttree->plan_width,
        lefttree->vec_output,
        lefttree->dop);

    ereport(DEBUG1, (errmodule(MOD_OPT_AGG), errmsg("Agg method3 cost after second agg: %lf", agg_path_2->total_cost)));

    if (root->query_level == 1) {
        StreamPath* gather_path = cost_agg_do_gather(agg_path_2, lefttree->plan_width, lefttree->vec_output);

        ereport(DEBUG1,
            (errmodule(MOD_OPT_AGG), errmsg("Agg method3 cost after gather: %lf", gather_path->path.total_cost)));

        debug_print_agg_detail(
            root, AGG_HASHED, DN_AGG_REDISTRIBUTE_AGG, &gather_path->path, agg_path_2, &redist_path->path, agg_path_1);

        copy_path_costsize(result_path, &gather_path->path);
    } else {
        debug_print_agg_detail(root, AGG_HASHED, DN_AGG_REDISTRIBUTE_AGG, agg_path_2, &redist_path->path, agg_path_1);

        copy_path_costsize(result_path, agg_path_2);
    }

    result_path->distribute_keys = distributed_key;
}

/*
 * get_redist_hashagg_redist_hashagg_path:
 *     get result path for redist -> agg -> distribute -> agg -> gather.
 *
 * Parameters:
 *	@in root: plan info node
 *	@in	lefttree: the current plan
 *	@in	aggcosts: the execution costs of the aggregates' input expressions
 *	@in numGroupCols: the column num of group by clause
 *	@in	numGroups: the local distinct of group by clause for the first level
 *	@in	final_groups: the global distinct of group by clause for the final level
 *  @in distributed_key_less_skew: the less skewed distribute key
 *  @in multiple_less_skew: the multiple of the less skewed distribute key
 *  @in target_distribution: the target node group
 *	@in	distributed_key: the distribute key for stream
 *	@in	multiple: the multiple for stream
 *	@in total_cost: the initial total cost for qual
 *	@in	hashentrysize: hash entry size include space for per tuple width, space for pass-by-ref transition values,
 *		the per-hash-entry overhead
 *	@in	result_path: the result path for aggcost() + distributecost() + aggcost() + gathercost() with total cost
 *
 * Returns: void
 */
static void get_redist_hashagg_redist_hashagg_path(PlannerInfo* root, Plan* lefttree, const AggClauseCosts* aggcosts,
    int numGroupCols, double numGroups, double final_groups, List* distributed_key_less_skew, double multiple_less_skew,
    Distribution* target_distribution, List* distributed_key, double multiple, QualCost total_cost, Size hashentrysize,
    bool needs_stream, Path* result_path)
{
    Path* subpath = cost_agg_convert_to_path(lefttree);

    if (target_distribution == NULL || (
        target_distribution->bms_data_nodeids == NULL &&
        target_distribution->group_oid == InvalidOid)) {
        target_distribution = ng_get_installation_group_distribution();
    }

    StreamPath* redist_path_1 = cost_agg_do_redistribute(subpath,
        distributed_key_less_skew,
        multiple_less_skew,
        target_distribution,
        lefttree->plan_width,
        lefttree->vec_output,
        lefttree->dop,
        needs_stream);

    ereport(DEBUG1,
        (errmodule(MOD_OPT_AGG),
            errmsg("Agg method 5 (3+) cost after first redistribute: %lf", redist_path_1->path.total_cost)));

    Path* agg_path_1 = cost_agg_do_agg((Path*)redist_path_1,
        root,
        AGG_HASHED,
        aggcosts,
        numGroupCols,
        numGroups,
        hashentrysize,
        total_cost,
        lefttree->plan_width,
        lefttree->vec_output,
        lefttree->dop);

    ereport(DEBUG1,
        (errmodule(MOD_OPT_AGG), errmsg("Agg method 5 (3+) cost after first agg: %lf", agg_path_1->total_cost)));

    StreamPath* redist_path_2 = cost_agg_do_redistribute(agg_path_1,
        distributed_key,
        multiple,
        target_distribution,
        lefttree->plan_width,
        lefttree->vec_output,
        lefttree->dop,
        needs_stream);

    ereport(DEBUG1,
        (errmodule(MOD_OPT_AGG),
            errmsg("Agg method 5 (3+) cost after second redistribute: %lf", redist_path_2->path.total_cost)));

    double numGroups_agg_path_2 =
        clamp_row_est(get_local_rows(final_groups, 1.0, false, ng_get_dest_num_data_nodes((Path*)redist_path_2)));
    Path* agg_path_2 = cost_agg_do_agg((Path*)redist_path_2,
        root,
        AGG_HASHED,
        aggcosts,
        numGroupCols,
        numGroups_agg_path_2,
        hashentrysize,
        total_cost,
        lefttree->plan_width,
        lefttree->vec_output,
        lefttree->dop);

    ereport(DEBUG1,
        (errmodule(MOD_OPT_AGG), errmsg("Agg method 5 (3+) cost after second agg: %lf", agg_path_2->total_cost)));

    if (root->query_level == 1) {
        StreamPath* gather_path = cost_agg_do_gather(agg_path_2, lefttree->plan_width, lefttree->vec_output);

        ereport(DEBUG1,
            (errmodule(MOD_OPT_AGG), errmsg("Agg method 5 (3+) cost after gather: %lf", gather_path->path.total_cost)));

        debug_print_agg_detail(root,
            AGG_HASHED,
            DN_REDISTRIBUTE_AGG_REDISTRIBUTE_AGG,
            &gather_path->path,
            agg_path_2,
            &redist_path_2->path,
            agg_path_1,
            &redist_path_1->path);

        copy_path_costsize(result_path, &gather_path->path);
    } else {
        debug_print_agg_detail(root,
            AGG_HASHED,
            DN_REDISTRIBUTE_AGG_REDISTRIBUTE_AGG,
            agg_path_2,
            &redist_path_2->path,
            agg_path_1,
            &redist_path_1->path);

        copy_path_costsize(result_path, agg_path_2);
    }

    result_path->distribute_keys = distributed_key;
}

/*
 * @Description: Confirm whether the distributed_key has skew when do redistribution.
 * There are two cases : hint skew and null skew.
 *  Firstly, confirm whether the distributed_key has hint skew: SKEW_RES_HINT.
 *  Secondly, confirm whether the distributed_key has null skew becasue of outer join of sub plan: SKEW_RES_RELU.
 *  If has skew, then choose to do agg first to avoid redis skew.
 */
static uint32 get_hashagg_skew(AggSkewInfo* skew_info, List* distribute_keys)
{
    /* If guc 'skew_option' is setting to off, just return. */
    if (u_sess->opt_cxt.skew_strategy_opt == SKEW_OPT_OFF)
        return SKEW_RES_NONE;

    if (distribute_keys == NIL)
        return SKEW_RES_NONE;

    skew_info->setDistributeKeys(distribute_keys);
    skew_info->findStreamSkewInfo();
    return skew_info->getSkewInfo();
}

DistrbutionPreferenceType get_agg_distribution_perference_type(Plan* plan) 
{
    if (!u_sess->attr.attr_sql.enable_dngather || !u_sess->opt_cxt.is_dngather_support) {
        return DPT_SHUFFLE;
    }

    if (plan->plan_rows <= u_sess->attr.attr_sql.dngather_min_rows) {
        return DPT_SINGLE;
    }

    return DPT_SHUFFLE;
}

/*
 * Agg's single node distribution comparision function.
 */
bool compare_agg_single_node_distribution(Distribution* new_distribution, Distribution* old_distribution, 
                                      double new_cost, double old_cost) 
{
    if (!u_sess->attr.attr_sql.enable_dngather || !u_sess->opt_cxt.is_dngather_support) {
        return new_cost < old_cost;
    }

    bool better_distribution = false;
    bool is_new_single_node_distribution = ng_is_single_node_group_distribution(new_distribution);
    bool is_old_single_node_distribution = ng_is_single_node_group_distribution(old_distribution);
    if (is_new_single_node_distribution && !is_old_single_node_distribution) {
        better_distribution = true;
    } else if (!is_new_single_node_distribution && is_old_single_node_distribution) {
        better_distribution = false;
    } else {
        better_distribution = new_cost < old_cost;
    }

    return better_distribution;
}

/*
 * Choose the cheapest plan for agg from the following three paths
 * 1. agg(dn)->gather->agg(cn)
 * 2. distribute(dn)->agg(dn)->gather
 * 3. agg(dn)->distribute(dn)->agg(dn)->gather
 */
static SAggMethod get_optimal_hashagg(PlannerInfo* root, Plan* lefttree, const AggClauseCosts* aggcosts,
    int numGroupCols, double numGroups, List* distributed_key, List* target_list, double final_groups, double multiple,
    List* distribute_key_less_skew, double multiple_less_skew, AggOrientation agg_orientation, Cost* final_cost,
    Distribution** final_distribution, bool need_stream, AggSkewInfo* skew_info, uint32 aggmethod_filter)
{
    Query* parse = root->parse;
    double best_cost = 0.0;
    Distribution* best_target_distribution = NULL;
    SAggMethod option = DN_AGG_CN_AGG;
    QualCost qual_cost, tlist_cost, total_cost;
    Path result_path;
    errno_t rc = EOK; /* Initialize rc to keep compiler slient */
    bool force_slvl_agg = aggmethod_filter & FOREC_SLVL_AGG;
#ifdef ENABLE_MULTIPLE_NODES
    bool disallow_cn_agg = aggmethod_filter & DISALLOW_CN_AGG;
#endif
    /*
     *  Confirm whether the distributed_key has skew when do redistribution.
     *  With two cases : hint skew and null skew.
     *  Notice, the priority:
     *  force_slvl_agg avoid > plan_mode_seed > SKEW_RES_HINT > best_agg_plan > SKEW_RES_RULE
     */
    uint32 has_skew = SKEW_RES_NONE;
    uint32 has_skew_for_redisfirst = SKEW_RES_NONE;

    if (!force_slvl_agg) {
        has_skew_for_redisfirst = get_hashagg_skew(skew_info, distribute_key_less_skew);
        has_skew = get_hashagg_skew(skew_info, distributed_key);
    }

    /* Get target computing node group list with heuristic method */
    List* distribution_list = ng_get_agg_candidate_distribution_list(lefttree, root->is_correlated, 
         get_agg_distribution_perference_type(lefttree));

    /* If guc u_sess->attr.attr_sql.plan_mode_seed is random plan, we should choose random path between AGG_HASHED and
     * AGG_SORTED */
    if (u_sess->attr.attr_sql.plan_mode_seed != OPTIMIZE_PLAN) {
        int hashagg_option_num = 0;
        int random_option = 0;

        /* If the distribute keys is NIL, we can only choose DN_AGG_CN_AGG. */
        if (distributed_key == NIL) {
            return DN_AGG_CN_AGG;
        } else {
            if (root->query_level == 1) {
                /*
                 * When the agg's group by keys include distribute key,
                 * we do not need stream, but when parallel this situation,
                 * we have to add local redistribute, so we can not choose
                 * DN_AGG_CN_AGG.
                 */
                if (lefttree->dop <= 1 || need_stream)
                    hashagg_option_num = ALL_HASHAGG_OPTION;
                else
                    hashagg_option_num = HASHAGG_OPTION_WITH_STREAM;
            } else {
                hashagg_option_num = HASHAGG_OPTION_WITH_STREAM;
            }
        }

        if (list_length(distribution_list) != 1) {
            random_option = choose_random_option(list_length(distribution_list));
            *final_distribution = (Distribution*)list_nth(distribution_list, random_option);
        }
        if (force_slvl_agg)
            random_option = 0;
        else
            random_option = choose_random_option(hashagg_option_num);
        return g_hashagg_option_list[random_option];
    }

    if (parse->havingQual != NULL)
        cost_qual_eval(&qual_cost, (List*)parse->havingQual, root);
    else {
        rc = memset_s(&qual_cost, sizeof(QualCost), 0, sizeof(QualCost));
        securec_check(rc, "\0", "\0");
    }
    cost_qual_eval(&tlist_cost, target_list, root);
    total_cost.startup = qual_cost.startup + tlist_cost.startup;
    total_cost.per_tuple = qual_cost.per_tuple + tlist_cost.per_tuple;

    ereport(DEBUG1, (errmodule(MOD_OPT_AGG), errmsg("Local groups %lf, final groups: %lf.", numGroups, final_groups)));

    rc = memset_s(&result_path, sizeof(Path), 0, sizeof(Path));
    securec_check(rc, "\0", "\0");
#ifdef ENABLE_MULTIPLE_NODES
    /* If lefttree is parallel, we need local redistribute, thus DN_AGG_CN_AGG is not allowed. */
    if (((root->query_level == 1 && (lefttree->dop <= 1 || need_stream)) ||
            (root->query_level > 1 && !need_stream && lefttree->dop <= 1)) &&
        !force_slvl_agg && !disallow_cn_agg) {
        if (u_sess->attr.attr_sql.best_agg_plan == OPTIMAL_AGG ||
            u_sess->attr.attr_sql.best_agg_plan == DN_AGG_CN_AGG || (SKEW_RES_HINT & has_skew)) {
            /* 1. get total cost for hashagg (dn) + gather + hashagg (cn). */
            get_hashagg_gather_hashagg_path(root,
                lefttree,
                aggcosts,
                numGroupCols,
                numGroups,
                final_groups,
                total_cost,
                0,
                AGG_HASHED,
                need_stream,
                &result_path);
            if (best_cost == 0.0 || result_path.total_cost < best_cost) {
                best_cost = result_path.total_cost;
                best_target_distribution = NULL;
                option = DN_AGG_CN_AGG;
            }
        }

        /* We consider this kind of path only when we successfully got a less skewed distribute key */
        if (((u_sess->attr.attr_sql.best_agg_plan == OPTIMAL_AGG && !(has_skew_for_redisfirst & SKEW_RES_RULE)) ||
              u_sess->attr.attr_sql.best_agg_plan == DN_REDISTRIBUTE_AGG_CN_AGG) &&
            distribute_key_less_skew != NULL &&
            (!equal_distributekey(root, distributed_key, distribute_key_less_skew)) &&
            !(has_skew_for_redisfirst & SKEW_RES_HINT)) {
            ListCell* lc = NULL;
            foreach (lc, distribution_list) {
                Distribution* target_distribution = (Distribution*)lfirst(lc);

                /* 1+. get total cost for redistribute(dn) + hashagg(dn) + gather + hashagg(cn). */
                get_redist_hashagg_gather_hashagg_path(root,
                    lefttree,
                    aggcosts,
                    numGroupCols,
                    numGroups,
                    final_groups,
                    distribute_key_less_skew,
                    multiple_less_skew,
                    target_distribution,
                    total_cost,
                    0,
                    AGG_HASHED,
                    need_stream,
                    &result_path);

                /*2. Compare new cost with the last.*/
                bool better_distribution = compare_agg_single_node_distribution(target_distribution, best_target_distribution, result_path.total_cost, best_cost);
 
                if (1 == u_sess->opt_cxt.query_dop && (best_cost == 0.0 || better_distribution) &&
                    best_cost < NG_FORBIDDEN_COST) {
                    best_cost = result_path.total_cost;
                    best_target_distribution = target_distribution;
                    option = DN_REDISTRIBUTE_AGG_CN_AGG;
                }
            }
        }
    }
#endif
    if (distributed_key != NIL) {
        ListCell* lc = NULL;
        foreach (lc, distribution_list) {
            Distribution* target_distribution = (Distribution*)lfirst(lc);

            if ((u_sess->attr.attr_sql.best_agg_plan == OPTIMAL_AGG && !(SKEW_RES_RULE & has_skew)) ||
                 u_sess->attr.attr_sql.best_agg_plan == DN_REDISTRIBUTE_AGG || force_slvl_agg) {
                if (force_slvl_agg || !(SKEW_RES_HINT & has_skew)) {
                    /* 2. get total cost for redistribute(dn) + hashagg (dn) + gather. */
                    get_redist_hashagg_path(root,
                        lefttree,
                        aggcosts,
                        numGroupCols,
                        numGroups,
                        final_groups,
                        distributed_key,
                        multiple,
                        target_distribution,
                        total_cost,
                        0,
                        need_stream,
                        &result_path);


                    /* Compare new cost with the last.*/
                    bool better_distribution = compare_agg_single_node_distribution(target_distribution, best_target_distribution, result_path.total_cost, best_cost);
                    if (best_cost == 0.0 || better_distribution) {
                        best_cost = result_path.total_cost;
                        best_target_distribution = target_distribution;
                        option = DN_REDISTRIBUTE_AGG;
                    }
                }
            }

            if ((u_sess->attr.attr_sql.best_agg_plan == OPTIMAL_AGG ||
                 u_sess->attr.attr_sql.best_agg_plan == DN_AGG_REDISTRIBUTE_AGG || (SKEW_RES_HINT & has_skew)) &&
                !force_slvl_agg) {
                /* 3. get total cost for hashagg (dn) + redistribute(dn) + hashagg (dn) + gather. */
                get_hashagg_redist_hashagg_path(root,
                    lefttree,
                    aggcosts,
                    numGroupCols,
                    numGroups,
                    final_groups,
                    distributed_key,
                    multiple,
                    target_distribution,
                    total_cost,
                    0,
                    need_stream,
                    &result_path);

                /* Compare new cost with the last.*/
                bool better_distribution = compare_agg_single_node_distribution(target_distribution, best_target_distribution, result_path.total_cost, best_cost);
                if (best_cost == 0.0 || better_distribution) {
                    best_cost = result_path.total_cost;
                    best_target_distribution = target_distribution;
                    option = DN_AGG_REDISTRIBUTE_AGG;
                }
            }

            /* We consider this kind of path only when we successfully got a less skewed distribute key */
            if (((u_sess->attr.attr_sql.best_agg_plan == OPTIMAL_AGG && !(SKEW_RES_RULE & has_skew)) ||
                  u_sess->attr.attr_sql.best_agg_plan == DN_REDISTRIBUTE_AGG_REDISTRIBUTE_AGG) &&
                distribute_key_less_skew != NULL && multiple_less_skew < multiple &&
                (!equal_distributekey(root, distributed_key, distribute_key_less_skew)) && !force_slvl_agg &&
                !(SKEW_RES_HINT & has_skew_for_redisfirst)) {
                /* 3+. get total cost for redistribute(dn) + hashagg(dn) + redistribute(dn) + hashagg(dn) + gather */
                get_redist_hashagg_redist_hashagg_path(root,
                    lefttree,
                    aggcosts,
                    numGroupCols,
                    numGroups,
                    final_groups,
                    distribute_key_less_skew,
                    multiple_less_skew,
                    target_distribution,
                    distributed_key,
                    multiple,
                    total_cost,
                    0,
                    need_stream,
                    &result_path);

                /* Compare new cost with the last.*/
                bool better_distribution = compare_agg_single_node_distribution(target_distribution, best_target_distribution, result_path.total_cost, best_cost);
                if (1 == u_sess->opt_cxt.query_dop && (best_cost == 0.0 || better_distribution) &&
                    best_cost < NG_FORBIDDEN_COST) {
                    best_cost = result_path.total_cost;
                    best_target_distribution = target_distribution;
                    option = DN_REDISTRIBUTE_AGG_REDISTRIBUTE_AGG;
                }
            }
        }
    }
    *final_cost = best_cost;
    *final_distribution = best_target_distribution;

    /* Add optimal info to log for hint skew and null skew. */
    if (!force_slvl_agg && u_sess->attr.attr_sql.plan_mode_seed == OPTIMIZE_PLAN && (has_skew & SKEW_RES_HINT)) {
        ereport(DEBUG1,
            (errmodule(MOD_OPT_SKEW),
                errmsg(
                    "[SkewAgg : SKEW_RES_HINT] The optimal hash agg method is: %d (cost: %lf).", option, *final_cost)));
    } else if (!force_slvl_agg && u_sess->attr.attr_sql.plan_mode_seed == OPTIMIZE_PLAN &&
               u_sess->attr.attr_sql.best_agg_plan == OPTIMAL_AGG && (has_skew & SKEW_RES_RULE)) {
        ereport(DEBUG1,
            (errmodule(MOD_OPT_SKEW),
                errmsg(
                    "[SkewAgg : SKEW_RES_RULE] The optimal hash agg method is: %d (cost: %lf).", option, *final_cost)));
    } else {
        ereport(DEBUG1,
            (errmodule(MOD_OPT_AGG), errmsg("The optimal hash agg method is: %d (cost: %lf).", option, *final_cost)));
    }

    if (*final_distribution) {
        ereport(DEBUG1,
            (errmodule(MOD_OPT_AGG),
                errmsg("The optimal hash agg computing group has %d datanode(s).",
                    bms_num_members((*final_distribution)->bms_data_nodeids))));
    }

    if (agg_orientation != AGG_LEVEL_2_1_INTENT && u_sess->attr.attr_sql.best_agg_plan != OPTIMAL_AGG) {
        if (distributed_key == NIL)
            return DN_AGG_CN_AGG;
        else if (force_slvl_agg)
            return DN_REDISTRIBUTE_AGG;
        /*
         * If distributed_key has skew, then choose the lowest cost plan between
         * DN_AGG_CN_AGG and  DN_AGG_REDISTRIBUTE_AGG.
         */
        else if (SKEW_RES_HINT & has_skew)
            return option;
        return (SAggMethod)u_sess->attr.attr_sql.best_agg_plan;
    }

    return option;
}

/*
 * For compare agg's distribution with child's to reduce stream plan.
 */
bool is_agg_distribution_compalible_with_child(Distribution* aggDistribution, Distribution* childDistribution) 
{
    if (ng_is_single_node_group_distribution(aggDistribution)
        && ng_is_single_node_group_distribution(childDistribution)) {
        return true;
    }

    // For other Distribution type, they may compalible too, but now
    // we only handle the single distribution.
    return false;
}

static Plan* generate_hashagg_plan(PlannerInfo* root, Plan* plan, List* final_list, AggClauseCosts* agg_costs,
    int numGroupCols, const double* numGroups, WindowLists* wflists, AttrNumber* groupColIdx, Oid* groupColOps,
    bool* needs_stream, Size hash_entry_size, AggOrientation agg_orientation, RelOptInfo* rel_info)
{
    SAggMethod agg_option = DN_AGG_CN_AGG;
    SAggMethod final_agg_mp_option = DN_AGG_CN_AGG;
    Plan* agg_plan = NULL;
    List* distributed_key = NIL;
    List* distribute_key_less_skew = NIL;
    Query* parse = root->parse;
    List* groupClause = groupColIdx != NULL ? parse->groupClause : parse->distinctClause;
    List* qual = NIL;
    AttrNumber* local_groupColIdx =
        groupColIdx != NULL ? groupColIdx : extract_grouping_cols(parse->distinctClause, plan->targetlist);
    bool trans_agg = groupColIdx != NULL ? true : false;
    double temp_num_groups[2];
    double final_groups = numGroups[1];
    double local_distinct;
    List* group_exprs = NIL;
    double multiple = 0.0;
    double multiple_less_skew = 0.0;
    Distribution* final_distribution = NULL;
    int dop = plan->dop > 1 ? plan->dop : 1;
    AggSkewInfo* skew_info = NULL;
    uint32 skew_opt = SKEW_RES_NONE;
    uint32 aggmethod_filter = ALLOW_ALL_AGG;

    temp_num_groups[0] = numGroups[0];
    temp_num_groups[1] = numGroups[1];

    /*
     * If plan is agg, having qual has be moved to plan->qual.
     * To distinct clause, we need not to move having qual which must be filter in lower levels agg operator,
     * and if having qual be moved, error can happend because this aggplan's targetlist is parse->targetlist.
     */
    if (IsA(plan, Agg) && plan->qual && groupClause != parse->distinctClause) {
        qual = plan->qual;
        plan->qual = NIL;
    } else {
        qual = groupColIdx != NULL ? (List*)parse->havingQual : NIL;
    }
#ifndef ENABLE_MULTIPLE_NODES
    if (plan->dop == 1) {
        plan = (Plan*)make_agg(root,
            final_list,
            qual,
            AGG_HASHED,
            agg_costs,
            numGroupCols,
            local_groupColIdx,
            groupColOps,
            final_groups,
            plan,
            wflists,
            *needs_stream,
            trans_agg,
            NIL,
            hash_entry_size,
            true,
            agg_orientation);
        return plan;
    }
#endif
    /* Confirm whether the distributed_key has skew */
    if (SKEW_OPT_OFF != u_sess->opt_cxt.skew_strategy_opt)
        skew_info = New(CurrentMemoryContext) AggSkewInfo(root, plan, rel_info);


    if (groupColIdx == NULL) {
        group_exprs = get_sortgrouplist_exprs(groupClause, final_list);
    } else {
        int i;
        for (i = 0; i < numGroupCols; i++) {
            TargetEntry* tle = (TargetEntry*)list_nth(plan->targetlist, groupColIdx[i] - 1);
            group_exprs = lappend(group_exprs, tle->expr);
        }

        if (numGroupCols != list_length(groupClause))
            get_num_distinct(root,
                group_exprs,
                PLAN_LOCAL_ROWS(plan),
                plan->plan_rows,
                ng_get_dest_num_data_nodes(plan),
                temp_num_groups);
    }

    /* Estimate distinct for hashagg. */
    local_distinct = estimate_agg_num_distinct(root, group_exprs, plan, temp_num_groups);

    /*
     * If there are subplan in qual, then find the vars in subplan in targetlist, if not exists, then only support
     * dn_redistribute_agg, because for two-level agg, we'll use final list as the target list of first level agg,
     * and qual is calculated in second level, then the var will not be found
     */
    bool subplan_in_qual = check_subplan_in_qual(final_list, qual);
    /* string_agg only support dn_redistribute_agg */
    bool has_dnagg = (agg_costs != NULL && (agg_costs->hasdctDnAggs || agg_costs->hasDnAggs));

    /*
     * If the qual contains subplan exec on DN, DN_AGG_CN_AGG and DN_REDISTRIBUTE_AGG_CN_AGG will cause
     * the query unshippable(see finalize_node_id), we should never gen such plan.
     */
    bool subplan_exec_on_dn = check_subplan_exec_datanode(root, (Node*)qual);

    /*
     * If tlist contains expressions that return sets, force to do single level agg to avoid
     * twice calculation which may cause wrong resulsts
     */
    bool contain_sets_expr = expression_returns_set((Node*)final_list);

    if (subplan_in_qual || has_dnagg || contain_sets_expr)
        aggmethod_filter |= FOREC_SLVL_AGG;
    else if (subplan_exec_on_dn)
        aggmethod_filter |= DISALLOW_CN_AGG;

    if ((!*needs_stream && (!ng_is_multiple_nodegroup_scenario())) || root->glob->insideRecursion) {
        agg_option = DN_AGG_CN_AGG;
    } else {
        double multiple_matched = -1.0;
        bool choose_matched = false;
        bool use_skew = true;
        bool use_bias = true;
        double skew_multiple = 0.0;
        double bias_multiple = 0.0;
        List* local_distributed_key = NIL;
        List* desired_exprs = NIL;
        double desired_exprs_numdistinct[2];

        /* get final groups */
        final_groups = temp_num_groups[1];

        /*
         * Choose the optimal distribute key.
         * First, we find ideal target distribute key, in two case: 1. target distribute key of inserting table.
         * 2. actual group clause of count(distinct) case, stored in local_distributed_key.
         * Then, if the ideal target distribute key has multiple no more than 1, we use it. or we find the
         * optimal distribute key of all the group by clause, stored in distributed_key. Now, if the multiple
         * of ideal key is no more than optimal key, we just use the ideal key.
         * Finally, we should compare the cost of ideal key and optimal key plus redistribution cost, and
         * choose the better one to do redistribution.
         */
        if (root->dis_keys.matching_keys != NIL && parse->groupingSets == NIL) { /* don't count grouping set case */
            ListCell* lc = NULL;
            foreach (lc, root->dis_keys.matching_keys) {
                if (!list_member(group_exprs, lfirst(lc))) {
                    break;
                }
            }
            if (lc == NULL) {
                get_multiple_from_exprlist(root,
                    root->dis_keys.matching_keys,
                    plan->plan_rows,
                    &use_skew,
                    use_bias,
                    &skew_multiple,
                    &bias_multiple);
                multiple_matched = Max(skew_multiple, bias_multiple);
                if (multiple_matched <= 1.0) {
                    multiple = multiple_matched;
                    choose_matched = true;
                    distributed_key = root->dis_keys.matching_keys;

                    ereport(DEBUG1,
                        (errmodule(MOD_OPT_AGG), errmsg("matching key distribution is chosen due to no skew.")));
                } else {
                    local_distributed_key = root->dis_keys.matching_keys;
                }
            }
        }

        if (multiple_matched == -1 && parse->groupingSets == NIL) { /* don't count grouping set case */
            if (root->dis_keys.superset_keys != NIL) {
                ListCell* lc = NULL;
                double desired_multiple = -1.0;
                /* loop all the possible superset key to find keys with lowest multiple */
                foreach (lc, root->dis_keys.superset_keys) {
                    List* superset_keys = (List*)lfirst(lc);
                    desired_exprs = list_intersection(superset_keys, group_exprs);
                    List* new_local_distributed_key = get_distributekey_from_tlist(
                        root, final_list, desired_exprs, plan->plan_rows, &desired_multiple, skew_info);
                    if (multiple_matched == -1.0 || desired_multiple < multiple_matched) {
                        multiple_matched = desired_multiple;
                        local_distributed_key = new_local_distributed_key;
                    }
                    if (multiple_matched <= 1.0) {
                        break;
                    }
                }
            } else if (numGroupCols != list_length(groupClause)) { /* the first level of count(distinct) */
                desired_exprs = get_sortgrouplist_exprs(groupClause, parse->targetList);
                local_distributed_key = get_distributekey_from_tlist(
                    root, final_list, desired_exprs, plan->plan_rows, &multiple_matched, skew_info);
            }

            if (desired_exprs != NIL && local_distributed_key != NIL) {
                if (multiple_matched <= 1.0) {
                    choose_matched = true;
                    distributed_key = local_distributed_key;
                    multiple = multiple_matched;
                }

                /*
                 * get distinct of desired exprs for compute the cost of paths
                 * agg+redistribute+agg and redistribute+agg,
                 * in order to judge which distribute_key we want to use.
                 */
                get_num_distinct(root,
                    desired_exprs,
                    clamp_row_est(final_groups / ng_get_dest_num_data_nodes(plan) / dop),
                    final_groups,
                    ng_get_dest_num_data_nodes(plan),
                    desired_exprs_numdistinct,
                    NULL);
            }
        }

        if (!choose_matched) {
            distributed_key =
                get_distributekey_from_tlist(root, final_list, group_exprs, plan->plan_rows, &multiple, skew_info);
            if (multiple_matched > 0.0 && multiple_matched <= multiple) {
                multiple = multiple_matched;
                choose_matched = true;

                ereport(DEBUG1,
                    (errmodule(MOD_OPT_AGG),
                        errmsg("matching key distribution is chosen due to no more skew than best distribute key.")));
            }
        }

        /* Generate less skew distribute key for potential shuffle */
        if (Abs(plan->multiple - 1.0) > 0.001 || plan->distributed_keys == NIL) {
            List* final_list_exprs = get_tlist_exprs(plan->targetlist, false);
            distribute_key_less_skew = get_distributekey_from_tlist(
                root, NIL, final_list_exprs, plan->plan_rows, &multiple_less_skew, skew_info);
        }

        /* set skew optimization method. */
        if (skew_info != NULL) {
            skew_opt = skew_info->getSkewInfo();
        }

        if (aggmethod_filter == FOREC_SLVL_AGG) {
            if (distributed_key == NIL) {
                if (subplan_in_qual) {
                    errno_t sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
                        NOTPLANSHIPPING_LENGTH,
                        "\"Subplan in having qual + Group by\" on redistribution unsupported data type");
                    securec_check_ss_c(sprintf_rc, "\0", "\0");
                } else {
                    errno_t sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
                        NOTPLANSHIPPING_LENGTH,
                        "\"String_agg/Array_agg/Listagg + Group by\" on redistribution unsupported data type");
                    securec_check_ss_c(sprintf_rc, "\0", "\0");
                }
                mark_stream_unsupport();
                *needs_stream = false;
                return plan;
            } else {
                Cost final_cost;

                agg_option = get_optimal_hashagg(root,
                    plan,
                    agg_costs,
                    numGroupCols,
                    local_distinct,
                    distributed_key,
                    final_list,
                    final_groups,
                    multiple,
                    distribute_key_less_skew,
                    multiple_less_skew,
                    agg_orientation,
                    &final_cost,
                    &final_distribution,
                    *needs_stream,
                    skew_info,
                    aggmethod_filter);
            }
        } else {
            Cost final_cost;

            agg_option = get_optimal_hashagg(root,
                plan,
                agg_costs,
                numGroupCols,
                local_distinct,
                distributed_key,
                final_list,
                final_groups,
                multiple,
                distribute_key_less_skew,
                multiple_less_skew,
                agg_orientation,
                &final_cost,
                &final_distribution,
                *needs_stream,
                skew_info,
                aggmethod_filter);
            if (!choose_matched && multiple_matched > 0.0) {
                Cost final_cost_matched;
                SAggMethod agg_option_matched;
                Cost redistribute_cost = 0.0;
                Cost agg_redis_cost = 0.0;
                Cost cheapest_cost = 0.0;
                double glbrows;

                agg_option_matched = get_optimal_hashagg(root,
                    plan,
                    agg_costs,
                    numGroupCols,
                    local_distinct,
                    local_distributed_key,
                    final_list,
                    final_groups,
                    multiple_matched,
                    distribute_key_less_skew,
                    multiple_less_skew,
                    agg_orientation,
                    &final_cost_matched,
                    &final_distribution,
                    *needs_stream,
                    skew_info,
                    aggmethod_filter);

                unsigned int path_num_datanodes = ng_get_dest_num_data_nodes(plan);
                /* redistribution cost of redistribute+agg path */
                compute_stream_cost(STREAM_REDISTRIBUTE,
                    plan->exec_nodes ? plan->exec_nodes->baselocatortype : LOCATOR_TYPE_REPLICATED,
                    clamp_row_est(final_groups / path_num_datanodes),
                    final_groups,
                    multiple_matched,
                    plan->plan_width,
                    false,
                    local_distributed_key,
                    &redistribute_cost,
                    &glbrows,
                    path_num_datanodes,
                    path_num_datanodes);

                if (desired_exprs != NIL) {
                    /* redistribution cost of agg+redistribute+agg path */
                    compute_stream_cost(STREAM_REDISTRIBUTE,
                        plan->exec_nodes ? plan->exec_nodes->baselocatortype : LOCATOR_TYPE_REPLICATED,
                        desired_exprs_numdistinct[0],
                        desired_exprs_numdistinct[0] * path_num_datanodes,
                        multiple_matched,
                        plan->plan_width,
                        false,
                        local_distributed_key,
                        &agg_redis_cost,
                        &glbrows,
                        path_num_datanodes,
                        path_num_datanodes);

                    cheapest_cost =
                        final_cost +
                        Min(agg_redis_cost * (1 + desired_exprs_numdistinct[0] /
                                                      clamp_row_est(final_groups / path_num_datanodes / dop)),
                            redistribute_cost);
                } else {
                    cheapest_cost = final_cost + redistribute_cost;
                }

                if (final_cost_matched <= cheapest_cost) {
                    choose_matched = true;
                    agg_option = agg_option_matched;
                    multiple = multiple_matched;

                    ereport(DEBUG1,
                        (errmodule(MOD_OPT_AGG),
                            errmsg("matching key distribution is chosen due to less cost than best distribute key.")));
                }
            }
            if (choose_matched && local_distributed_key != NIL) {
                distributed_key = local_distributed_key;
            }
        }
    }

    /*
     * The final agg has two parallel methods:
     * 1: local redistribute + agg
     * 2: agg + local redistribute + agg
     * We can still use the get_optimal_hashagg() function to
     * get the best parallel agg path for final hashagg.
     */
    if (agg_option == DN_AGG_CN_AGG && plan->dop > 1 && !*needs_stream && is_local_redistribute_needed(plan)) {
        Cost final_cost_matched;
        Distribution* final_distribution_matched = NULL;

        final_agg_mp_option = get_optimal_hashagg(root,
            plan,
            agg_costs,
            numGroupCols,
            local_distinct,
            plan->distributed_keys,
            final_list,
            final_groups,
            multiple,
            NIL,
            0.0,
            agg_orientation,
            &final_cost_matched,
            &final_distribution_matched,
            *needs_stream,
            skew_info,
            aggmethod_filter);

        /*
         * if plan->dop > 1 && 1 == best_agg_plan,
         * DN_AGG_CN_AGG should be replaced by DN_AGG_REDISTRIBUTE_AGG.
         */
        if (final_agg_mp_option == DN_AGG_CN_AGG) {
            final_agg_mp_option = DN_AGG_REDISTRIBUTE_AGG;
        }
    }
#ifdef ENABLE_MULTIPLE_NODES
    // Single node distribution.
    if (is_agg_distribution_compalible_with_child(final_distribution, &(plan->exec_nodes->distribution))) {
        plan = (Plan*)make_agg(root,
            plan->targetlist,
            qual,
            AGG_HASHED,
            agg_costs,
            numGroupCols,
            local_groupColIdx,
            groupColOps,
            final_groups,
            plan,
            wflists,
            *needs_stream,
            trans_agg,
            NIL,
            hash_entry_size,
            true,
            agg_orientation);

        if (skew_info != NULL) {
            if (skew_opt == SKEW_RES_NONE) {
               skew_opt = skew_info->getSkewInfo();
            }
            ((Agg*)plan)->skew_optimize = skew_opt;
            delete skew_info;
        }
        return plan;
    }
#endif
    if (agg_option == DN_REDISTRIBUTE_AGG_CN_AGG || agg_option == DN_REDISTRIBUTE_AGG_REDISTRIBUTE_AGG) {
        /* add first stream node */
        AssertEreport(distribute_key_less_skew != NULL, MOD_OPT, "invalid distribute key less skew.");
        plan = make_redistribute_for_agg(root, plan, distribute_key_less_skew, multiple_less_skew, final_distribution);
    }

    if (agg_option == DN_AGG_REDISTRIBUTE_AGG || agg_option == DN_REDISTRIBUTE_AGG_REDISTRIBUTE_AGG ||
        final_agg_mp_option == DN_AGG_REDISTRIBUTE_AGG) {
        /*
         * not pass need_stream here, when local redistribute is needed for
         * smp but need_stream is false
         */
        agg_plan = (Plan*)make_agg(root,
            final_list,
            qual,
            AGG_HASHED,
            agg_costs,
            numGroupCols,
            local_groupColIdx,
            groupColOps,
            (long)Min(local_distinct, (double)LONG_MAX),
            plan,
            wflists,
            true, /* pass true instead of need_stream */
            trans_agg,
            NIL,
            hash_entry_size,
            true,
            agg_orientation);
        if (wflists != NULL && wflists->activeWindows) {
            agg_plan->targetlist = make_windowInputTargetList(root, agg_plan->targetlist, wflists->activeWindows);
        }
    } else {
        agg_plan = plan;
    }

    if (agg_option == DN_REDISTRIBUTE_AGG || agg_option == DN_AGG_REDISTRIBUTE_AGG ||
        agg_option == DN_REDISTRIBUTE_AGG_REDISTRIBUTE_AGG) {
        /* add distribute stream plan */
        plan = make_redistribute_for_agg(root, agg_plan, distributed_key, multiple, final_distribution);
        *needs_stream = false;
    } else if (final_agg_mp_option == DN_REDISTRIBUTE_AGG || final_agg_mp_option == DN_AGG_REDISTRIBUTE_AGG) {
        /* Parallel the final agg. */
        plan = create_local_redistribute(root, agg_plan, agg_plan->distributed_keys, multiple);
    }

    if (agg_option == DN_AGG_REDISTRIBUTE_AGG || agg_option == DN_REDISTRIBUTE_AGG_REDISTRIBUTE_AGG ||
        final_agg_mp_option == DN_AGG_REDISTRIBUTE_AGG) {
        Path hashed_p;
        errno_t rc = EOK;
        rc = memset_s(&hashed_p, sizeof(Path), 0, sizeof(Path));
        securec_check(rc, "\0", "\0");
        plan = mark_top_agg(root, final_list, agg_plan, plan, agg_orientation);
        plan->plan_rows = final_groups;
        ((Agg*)plan)->numGroups = (long)Min(plan->plan_rows, (double)LONG_MAX);
        /* add new agg node cost */
        Distribution* distribution = ng_get_dest_distribution(plan);
        ng_copy_distribution(&hashed_p.distribution, distribution);
        cost_agg(&hashed_p,
            root,
            AGG_HASHED,
            agg_costs,
            numGroupCols,
            PLAN_LOCAL_ROWS(plan),
            plan->startup_cost,
            plan->total_cost,
            PLAN_LOCAL_ROWS(plan->lefttree),
            plan->lefttree->plan_width,
            hash_entry_size,
            plan->dop,
            &((Agg*)plan)->mem_info);
        /* Consider the selectivity of having qual */
        if (plan->qual != NIL && plan->plan_rows >= HAVING_THRESHOLD) {
            plan->plan_rows = clamp_row_est(plan->plan_rows * DEFAULT_MATCH_SEL);
        }
        plan->startup_cost = hashed_p.startup_cost;
        plan->total_cost = hashed_p.total_cost;
    } else {
        long rows;

        if (agg_option == DN_AGG_CN_AGG && final_agg_mp_option == DN_AGG_CN_AGG)
            rows = (long)Min(clamp_row_est(local_distinct), (double)LONG_MAX);
        else {
            unsigned int num_datanodes = ng_get_dest_num_data_nodes(plan);
            rows = (long)Min(clamp_row_est(final_groups / num_datanodes), (double)LONG_MAX);
        }

        plan = (Plan*)make_agg(root,
            final_list,
            qual,
            AGG_HASHED,
            agg_costs,
            numGroupCols,
            local_groupColIdx,
            groupColOps,
            rows,
            plan,
            wflists,
            *needs_stream,
            trans_agg,
            NIL,
            hash_entry_size,
            true,
            agg_orientation);
    }

    if (skew_info != NULL) {
        if (skew_opt == SKEW_RES_NONE) {
            skew_opt = skew_info->getSkewInfo();
        }
        ((Agg*)plan)->skew_optimize = skew_opt;
        delete skew_info;
    }
    return plan;
}

/*
 * @Description: Find the informational constraint info by Var.
 * @in var: the specified var, find constraint on the column of var.
 * @in relid: Relation id.
 * @in conType: Constraint type.
 * @return: true or false.
 */
bool findConstraintByVar(Var* var, Oid relid, constraintType conType)
{
    Relation conrel;
    HeapTuple htup;
    bool result = false;

    ScanKeyData skey[1];
    SysScanDesc conscan;
    ScanKeyInit(&skey[0], Anum_pg_constraint_conrelid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(relid));

    conrel = heap_open(ConstraintRelationId, AccessShareLock);
    conscan = systable_beginscan(conrel, ConstraintRelidIndexId, true, NULL, 1, skey);

    /* Forantion table only can exist one information constraint now. */
    while (HeapTupleIsValid(htup = systable_getnext(conscan))) {
        bool isNull = false;
        Datum adatum;
        int16* attnums = NULL;
        ArrayType* arr = NULL;

        adatum = SysCacheGetAttr(CONSTROID, htup, Anum_pg_constraint_conkey, &isNull);

        arr = DatumGetArrayTypeP(adatum);
        attnums = (int16*)ARR_DATA_PTR(arr);

        /*
         * Currently, the foreign table support only primary key and unique constraint,
         * Multi-column constraint unsupported, so the length of array attnums is 1.
         */
        Form_pg_constraint conform = (Form_pg_constraint)GETSTRUCT(htup);

        /* This constraint is informantional constraint and can used when building plan. */
        if (var->varattno == attnums[0] && conform->consoft && conform->conopt) {
            if (conType == UNIQUE_CONSTRAINT) {
                /* Primary key and unique have unique affect. */
                if (CONSTRAINT_PRIMARY == conform->contype || CONSTRAINT_UNIQUE == conform->contype) {
                    result = true;
                    break;
                }
            } else if (conType == NOT_NULL_CONSTRAINT) { /* Primary key have not null affect. */
                if (CONSTRAINT_PRIMARY == conform->contype) {
                    result = true;
                    break;
                }
            }
        }
    }

    systable_endscan(conscan);
    heap_close(conrel, AccessShareLock);

    return result;
}

/*
 * get_count_distinct_newtlist: get new tlist as merge orig tlist with distinct node.
 *
 * Parameters:
 *	@in root: plan info node
 *	@in	tlist: the final targetlist
 *	@in distinct_node: the node for distinct expr
 *	@in/out	orig_tlist: group by exprs and aggref exprs from final targetlist
 *	@in/out	duplicate_tlist: the targetEntry exisit in both final targetlist and group by clause
 *	@in distinct_eq_op: the equality operator oid for count(distinct)
 *
 * Returns: new tlist as merge orig tlist with distinct node
 */
static List* get_count_distinct_newtlist(
    PlannerInfo* root, List* tlist, Node* distinct_node, List** orig_tlist, List** duplicate_tlist, Oid* distinct_eq_op)
{
    List* new_tlist = NIL;
    ListCell* lc = NULL;
    ListCell* lc2 = NULL;
    int i = 0;

    /* Extract group by exprs and aggref exprs from final targetlist */
    *orig_tlist = make_agg_var_list(root, tlist, duplicate_tlist);
    i = 0;

    /* Make expr to TargetEntry in sub targetlist, and replace count(distinct(b)) with b */
    foreach (lc, *orig_tlist) {
        Node* n = (Node*)lfirst(lc);
        Node* expr = NULL;
        TargetEntry* tle = NULL;

        /* We only add one distinct node to new targetlist */
        if (IsA(n, Aggref) && ((Aggref*)n)->aggdistinct != NIL) {
            Aggref* agg_node = (Aggref*)n;
            expr = distinct_node;
            if (!OidIsValid(*distinct_eq_op))
                *distinct_eq_op = ((SortGroupClause*)linitial(agg_node->aggdistinct))->eqop;
            else {
                AssertEreport(*distinct_eq_op == ((SortGroupClause*)linitial(agg_node->aggdistinct))->eqop,
                    MOD_OPT,
                    "The equality operator of distinct node is not the head of aggdistinct.");
                continue;
            }
        } else
            expr = (Node*)copyObject(n);

        if (IsA(n, TargetEntry)) {
            ((TargetEntry*)expr)->resno = i + 1;
            new_tlist = lappend(new_tlist, expr);
        } else {
            foreach (lc2, tlist) {
                TargetEntry* te = (TargetEntry*)lfirst(lc2);

                if (equal(te->expr, n)) {
                    tle = flatCopyTargetEntry(te);
                    tle->expr = (Expr*)expr;
                    tle->resno = i + 1;
                    break;
                }
            }
            if (tle == NULL)
                tle = makeTargetEntry((Expr*)expr, i + 1, NULL, false);
            new_tlist = lappend(new_tlist, tle);
        }

        i++;
    }

    return new_tlist;
}

/*
 * get_count_distinct_param: get new targetlist and group cols for count(distinct) and group by clause.
 *
 * Parameters:
 *	@in root: plan info node
 *	@in	subplan: input plan
 *	@in	tlist: the final targetlist
 *	@in distinct_node: the node for distinct expr
 *	@in/out	numGrpColsNew: the new group cols include count(distinct) and orig tlist
 *	@in	groupColIdx: the idx for original group by clause
 *	@in/out	groupColIdx_new: the new idx for the new group cols
 *	@in/out groupingOps_new: the equality operator oid for count(distinct)
 *	@in/out	orig_tlist: group by exprs and aggref exprs from final targetlist
 *	@in/out	duplicate_tlist: the targetEntry exisit in both final targetlist and group by clause
 *	@in/out newtlist: new tlist as merge orig tlist with distinct node
 *
 * Returns: void
 */
static void get_count_distinct_param(PlannerInfo* root, Plan** subplan, List* tlist, Node* distinct_node,
    int* numGrpColsNew, AttrNumber* groupColIdx, AttrNumber** groupColIdx_new, Oid** groupingOps_new, List** orig_tlist,
    List** duplicate_tlist, List** newtlist)
{
    Query* parse = root->parse;
    Oid* orig_grouping_ops = extract_grouping_ops(parse->groupClause);
    int numGroupCols = list_length(parse->groupClause);
    ListCell* lc = NULL;
    bool located = false;
    int i;
    Oid distinct_eq_op = InvalidOid;
    Plan* result_plan = *subplan;
    /* Initialize new groupCols for additional level of hashagg */   
    Oid* groupingOps_tmp = (Oid*)palloc0(sizeof(Oid) * (numGroupCols + 1));
    AttrNumber* groupColIdx_tmp = (AttrNumber*)palloc0(sizeof(AttrNumber) * (numGroupCols + 1));
    if (numGroupCols != 0) {
        errno_t rc = EOK; /* Initialize rc to keep compiler slient */

        rc = memcpy_s(groupingOps_tmp, sizeof(Oid) * (numGroupCols + 1), orig_grouping_ops, sizeof(Oid) * numGroupCols);
        securec_check(rc, "\0", "\0");
        rc = memcpy_s(
            groupColIdx_tmp, sizeof(AttrNumber) * (numGroupCols + 1), groupColIdx, sizeof(AttrNumber) * numGroupCols);
        securec_check(rc, "\0", "\0");
    }

    /* construct new tlist as merge orig tlist with distinct node. */
    *newtlist = get_count_distinct_newtlist(root, tlist, distinct_node, orig_tlist, duplicate_tlist, &distinct_eq_op);

    /* Add groupCol and groupOp for new group by item */
    foreach (lc, result_plan->targetlist) {
        TargetEntry* te = (TargetEntry*)lfirst(lc);
        if (equal(te->expr, distinct_node)) {
            located = true;
            for (i = 0; i < numGroupCols; i++)
                if (te->resno == groupColIdx_tmp[i])
                    break;
            if (i == numGroupCols) {
                groupColIdx_tmp[numGroupCols] = te->resno;
            } else
                *numGrpColsNew = numGroupCols;
            break;
        }
    }

    /* If count(distinct) expr is not in target list yet, so we add it to target list for aggregation */
    if (!located) {
        if (!is_projection_capable_plan(result_plan)) {
            result_plan = (Plan*)make_result(root, (List*)copyObject(result_plan->targetlist), NULL, result_plan);
            *subplan = result_plan;
        }

        TargetEntry* newtlist_entry =
            makeTargetEntry((Expr*)distinct_node, list_length(result_plan->targetlist) + 1, NULL, true);
        result_plan->targetlist = lappend(result_plan->targetlist, newtlist_entry);
        groupColIdx_tmp[numGroupCols] = newtlist_entry->resno;
    }

    groupingOps_tmp[numGroupCols] = distinct_eq_op;
    *groupColIdx_new = groupColIdx_tmp;
    *groupingOps_new = groupingOps_tmp;
    return;
}

/*
 * get_count_distinct_partial_plan: return a hashagg supporting count(distinct) plan
 *
 * A new hashagg level should be formed here to support count(distinct) case, like:
 * select a, count(distinct(b)), sum(c) from t group by a;
 * We should first group by a, b, output is a, b, sum(c), then group by a,
 * output is a, count(b), sum(c).
 */
static Plan* get_count_distinct_partial_plan(PlannerInfo* root, Plan* result_plan, List** final_tlist,
    Node* distinct_node, AggClauseCosts agg_costs, const double* numGroups, WindowLists* wflists,
    AttrNumber* groupColIdx, bool* needs_stream, Size hash_entry_size, RelOptInfo* rel_info)
{
    Query* parse = root->parse;
    Oid* groupingOps_new = NULL;
    AttrNumber* groupColIdx_new = NULL;
    Oid* orig_grouping_ops = extract_grouping_ops(parse->groupClause);
    List* tlist = *final_tlist;
    List* new_tlist = NIL;
    List* orig_tlist = NIL;
    List* duplicate_tlist = NIL;
    int numGroupCols = list_length(parse->groupClause);
    ListCell* lc = NULL;
    ListCell* lc2 = NULL;
    ListCell* lc3 = NULL;
    bool located = false;
    int numGrpColsNew = numGroupCols + 1;
    Node* qual = NULL;

    /* get new groupcols and targetlist for count(distinct) and group by clause. */
    get_count_distinct_param(root,
        &result_plan,
        tlist,
        distinct_node,
        &numGrpColsNew,
        groupColIdx,
        &groupColIdx_new,
        &groupingOps_new,
        &orig_tlist,
        &duplicate_tlist,
        &new_tlist);

    *needs_stream = needs_agg_stream(root, new_tlist, result_plan->distributed_keys);
#ifndef ENABLE_MULTIPLE_NODES
    *needs_stream = *needs_stream && (result_plan->dop > 1);
#endif

    if (check_subplan_in_qual(new_tlist, (List*)parse->havingQual)) {
        errno_t sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
            NOTPLANSHIPPING_LENGTH,
            "\"Subplan in having qual + Count(distinct)\"");
        securec_check_ss_c(sprintf_rc, "\0", "\0");
        mark_stream_unsupport();
    }

    /* Since we don't want to final to CN for this level of hashagg, then mark it as subquery */
    root->query_level++;
    qual = parse->havingQual;
    parse->havingQual = NULL;

    result_plan = generate_hashagg_plan(root,
        result_plan,
        new_tlist,
        &agg_costs,
        numGrpColsNew,
        numGroups,
        NULL,
        groupColIdx_new,
        groupingOps_new,
        needs_stream,
        hash_entry_size,
        AGG_LEVEL_2_1_INTENT,
        rel_info);
    parse->havingQual = qual;
    root->query_level--;
    if (!IsA(result_plan, Agg) || *needs_stream) {
        errno_t sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
            NOTPLANSHIPPING_LENGTH,
            "\"Count(Distinct)\" on redistribution unsupported data type");
        securec_check_ss_c(sprintf_rc, "\0", "\0");
        mark_stream_unsupport();
        /* Set final_tlist to plan targetlist, this plan will be discarded. */
        result_plan->targetlist = *final_tlist;
        return result_plan;
    }

    /* We should make proper change to generated first level plan */
    if (((Agg*)result_plan)->is_final && numGrpColsNew == numGroupCols + 1) {
        Agg* agg_plan = (Agg*)result_plan;
        List* sub_targetlist = result_plan->lefttree->targetlist;
        int sub_seq_no = 1;

        foreach (lc, sub_targetlist) {
            TargetEntry* te = (TargetEntry*)lfirst(lc);
            if (equal(te->expr, distinct_node))
                break;
            sub_seq_no++;
        }
        AssertEreport(sub_seq_no <= list_length(sub_targetlist),
            MOD_OPT,
            "invalid sub_seq_no when getting a hashagg supporting count(distinct) plan.");
        agg_plan->grpColIdx[numGroupCols] = sub_seq_no;
    }
    ((Agg*)result_plan)->is_final = false;
    ((Agg*)result_plan)->single_node = false;

    /*
     * Make proper change of some property to final target list and having qual, actually referred by orig_tlist and
     * duplicate_tlist Following changes are made: 1. change count(distinct(b)) to count(b); 2. Increase aggstage of
     * non-count_distinct node
     */
    lc2 = list_head(result_plan->targetlist);
    located = false;
    foreach (lc, orig_tlist) {
        Node* n = (Node*)lfirst(lc);
        List* matched_node = NIL;
        ListCell* lc4 = NULL;
        bool next = true;

        foreach (lc3, duplicate_tlist) {
            if (equal(lfirst(lc3), n))
                matched_node = lappend(matched_node, lfirst(lc3));
        }
        if (IsA(n, Aggref) && ((Aggref*)n)->aggdistinct != NULL) {
            foreach (lc4, matched_node) {
                Aggref* m = (Aggref*)lfirst(lc4);
                list_free_deep(((Aggref*)m)->aggdistinct);
                ((Aggref*)m)->aggdistinct = NULL;
            }
            list_free_deep(((Aggref*)n)->aggdistinct);
            ((Aggref*)n)->aggdistinct = NULL;
            if (!located)
                located = true;
            else
                next = false;
        } else if (IsA(n, Aggref)) {
            TargetEntry* te = (TargetEntry*)lfirst(lc2);

            AssertEreport(IsA(te->expr, Aggref), MOD_OPT, "The type of expression is not T_Aggref.");
            foreach (lc4, matched_node) {
                Aggref* m = (Aggref*)lfirst(lc4);
                ((Aggref*)m)->aggstage = ((Aggref*)te->expr)->aggstage + 1;
            }
            ((Aggref*)n)->aggstage = ((Aggref*)te->expr)->aggstage + 1;
            ((Aggref*)te->expr)->aggtype = ((Aggref*)te->expr)->aggtrantype;
            te->resjunk = false;
        }
        list_free_ext(matched_node);
        matched_node = NIL;
        if (next)
            lc2 = lnext(lc2);
    }
    if (numGroupCols != 0)
        locate_grouping_columns(root, tlist, result_plan->targetlist, groupColIdx);
    *needs_stream = needs_agg_stream(root, tlist, result_plan->distributed_keys);
#ifndef ENABLE_MULTIPLE_NODES
    *needs_stream = *needs_stream && (result_plan->dop > 1);
#endif
    *final_tlist = tlist;

    if (numGroupCols != 0)
        result_plan = generate_hashagg_plan(root,
            result_plan,
            tlist,
            &agg_costs,
            numGroupCols,
            numGroups,
            wflists,
            groupColIdx,
            orig_grouping_ops,
            needs_stream,
            hash_entry_size,
            AGG_LEVEL_2_2_INTENT,
            rel_info);
    else
        result_plan = (Plan*)make_agg(root,
            tlist,
            (List*)parse->havingQual,
            AGG_PLAIN,
            &agg_costs,
            0,
            NULL,
            NULL,
            (long)Min(numGroups[0], (double)LONG_MAX),
            result_plan,
            wflists,
            *needs_stream,
            true,
            NIL,
            hash_entry_size,
            true,
            AGG_LEVEL_2_2_INTENT);

    return result_plan;
}

#ifdef ENABLE_MULTIPLE_NODES
static void free_est_varlist(List* varlist)
{
    ListCell* lc = NULL;
    foreach (lc, varlist) {
        EstSPNode* node = (EstSPNode*)lfirst(lc);
        if (IsA(node, EstSPNode)) {
            list_free_ext(node->varlist);
        }
    }
    list_free_ext(varlist);
}
#endif

double get_bias_from_varlist(PlannerInfo* root, List* varlist, double rows, bool isCoalesceExpr)
{
    double bias = 1.0;
    ListCell* lc = NULL;

    foreach (lc, varlist) {
        Node* node = (Node*)lfirst(lc);
        double mcf = 1.0;

        if (IsA(node, Var)) {

            mcf = get_node_mcf(root, node, (isCoalesceExpr ? rows : 0.0));
            bias *= mcf;
        } else {
            EstSPNode* sp = (EstSPNode*)node;
            double var_bias;
            AssertEreport(IsA(sp, EstSPNode), MOD_OPT, "The node type is not T_EstSPNode.");
            if (IsA(sp->expr, CoalesceExpr))
                var_bias = get_bias_from_varlist(root, sp->varlist, rows, true);
            else if (IsA(sp->expr, Aggref))
                var_bias = 0.0;
            else
                var_bias = get_bias_from_varlist(root, sp->varlist, 0.0);

            bias *= Max(DEFAULT_SPECIAL_EXPR_BIASE, var_bias / u_sess->pgxc_cxt.NumDataNodes);
        }
    }

    bias = Max(1.0, bias * u_sess->pgxc_cxt.NumDataNodes);
    return bias;
}

void get_multiple_from_exprlist(PlannerInfo* root, List* exprList, double rows, bool* useskewmultiple,
    bool usebiasmultiple, double* skew_multiple, double* bias_multiple)
{
#ifdef ENABLE_MULTIPLE_NODES
    ListCell* lc = NULL;
    List* varlist = NIL;
    Node* expr = NULL;

    if (*useskewmultiple) {
        /* Estimate distinct for expr in global datanode. */
        double ndistinct = estimate_num_groups(root, exprList, rows, u_sess->pgxc_cxt.NumDataNodes, STATS_TYPE_GLOBAL);

        /* we can't increase ndistinct by add more distribute column */
        if (ndistinct == rows)
            *useskewmultiple = false;

        *skew_multiple = get_skew_ratio(ndistinct);
    }

    if (usebiasmultiple) {
        foreach (lc, exprList) {
            expr = (Node*)lfirst(lc);
            varlist = append_distribute_var_list(varlist, expr);
        }

        /* Get multiple from varlist */
        if (varlist == NIL)
            *bias_multiple = u_sess->pgxc_cxt.NumDataNodes;
        else
            *bias_multiple = get_bias_from_varlist(root, varlist, rows);
    }
    free_est_varlist(varlist);
#else
    *skew_multiple = 1.0;
    *bias_multiple = 0.0;
#endif
    return;
}

static Node* get_multiple_from_expr(
    PlannerInfo* root, Node* expr, double rows, double* skew_multiple, double* bias_multiple)
{
    List* groupExprs = NIL;
    Oid datatype = exprType((Node*)(expr));
    bool use_skew_multiple = true;

    if (!OidIsValid(datatype) || !IsTypeDistributable(datatype))
        return NULL;

    groupExprs = list_make1(expr);
    get_multiple_from_exprlist(root, groupExprs, rows, &use_skew_multiple, true, skew_multiple, bias_multiple);
    list_free_ext(groupExprs);

    return expr;
}

static List* add_multiple_to_list(Node* expr, double multiple, List* varMultipleList)
{
    ExprMultipleData* pstExprMultipleData = NULL;
    ListCell* prev_node = NULL;

    /* Add bias to varBiasList order by asc. */
    prev_node = NULL;
    if (varMultipleList != NULL) {
        ListCell* lc = NULL;
        ExprMultipleData* vmd = NULL;

        foreach (lc, varMultipleList) {
            vmd = (ExprMultipleData*)lfirst(lc);
            if (vmd->multiple > multiple)
                break;

            if (equal(vmd->expr, expr))
                return varMultipleList;

            prev_node = lc;
        }
    }

    pstExprMultipleData = (ExprMultipleData*)palloc(sizeof(ExprMultipleData));
    pstExprMultipleData->expr = expr;
    pstExprMultipleData->multiple = multiple;

    if (prev_node == NULL)
        varMultipleList = lcons(pstExprMultipleData, varMultipleList);
    else
        lappend_cell(varMultipleList, prev_node, pstExprMultipleData);

    return varMultipleList;
}

/* Get distribute keys of group by or partition by operates, maybe an column or multi columns */
static List* get_mix_diskey_by_exprlist(
    PlannerInfo* root, List* exprMultipleList, double rows, double* result_multiple, AggSkewInfo* skew_info = NULL)
{
    List* distkey = NIL;
    Node* expr = NULL;
    ExprMultipleData* exprMultipleData = (ExprMultipleData*)linitial(exprMultipleList);
    double bias_multiple = 0.0;
    double skew_multiple = 0.0;
    bool useskewmultiple = true;
    bool usebiasmultiple = true;
    int group_num = 2;

    expr = get_multiple_from_expr(root, exprMultipleData->expr, rows, &skew_multiple, &bias_multiple);
    if (expr != NULL) {
        useskewmultiple = skew_multiple == 1.0 ? false : true;
        usebiasmultiple = bias_multiple <= 1.0 ? false : true;
        distkey = lappend(distkey, exprMultipleData->expr);
    }

    if (list_length(exprMultipleList) >= group_num) {
        while (group_num <= list_length(exprMultipleList)) {
            skew_multiple = 1.0;
            bias_multiple = 0.0;
            exprMultipleData = (ExprMultipleData*)list_nth(exprMultipleList, group_num - 1);
            distkey = lappend(distkey, exprMultipleData->expr);
            get_multiple_from_exprlist(
                root, distkey, rows, &useskewmultiple, usebiasmultiple, &skew_multiple, &bias_multiple);

            useskewmultiple = skew_multiple == 1 ? false : true;
            usebiasmultiple = bias_multiple <= 1 ? false : true;

            group_num++;

            if ((skew_multiple == 1) && (bias_multiple <= 1)) {
                /* Check if this distribute key is skew. */
                if (skew_info != NULL && (list_length(distkey) < list_length(exprMultipleList))) {
                    uint32 skew_opt = get_hashagg_skew(skew_info, distkey);

                    if ((skew_opt & SKEW_RES_HINT) || (skew_opt & SKEW_RES_STAT)) {
                        ereport(DEBUG1,
                            (errmodule(MOD_OPT_SKEW),
                                (errmsg("[SkewAgg] The distribute keys have skew according to [SKEW_RES:%u],"
                                        " we will add more column into distribute keys.",
                                    skew_opt))));

                        continue;
                    }
                }

                /* !usebiasmultiple && !useskewmultiple */
                *result_multiple = 1;
                return distkey;
            }
        }
    }

    *result_multiple = Max(bias_multiple, skew_multiple);
    AssertEreport(*result_multiple >= 1, MOD_OPT, "invalid result of multiple columns.");
    return distkey;
}

List* get_distributekey_from_tlist(
    PlannerInfo* root, List* tlist, List* groupcls, double rows, double* result_multiple, void* skew_info)
{
    ListCell* lcell = NULL;
    List* distkey = NIL;
    double multiple = 0.0;
    double bias_multiple = 0.0;
    double skew_multiple = 0.0;
    List* exprMultipleList = NIL;

    foreach (lcell, groupcls) {
        Node* expr = (Node*)lfirst(lcell);

        if (IsA(expr, SortGroupClause))
            expr = get_sortgroupclause_expr((SortGroupClause*)expr, tlist);

        expr = get_multiple_from_expr(root, expr, rows, &skew_multiple, &bias_multiple);
        if (expr != NULL) {
            /*
             * we can't estimate skew of grouping sets because there's
             * null added, so just add all columns and set mutiple to 1
             */
            if (root->parse->groupingSets) {
                distkey = lappend(distkey, expr);
                *result_multiple = 1;
                continue;
            }
            if ((skew_multiple == 1.0) && (bias_multiple <= 1.0)) {
                *result_multiple = 1;
                list_free_ext(exprMultipleList);
                return list_make1(expr);
            } else if ((u_sess->pgxc_cxt.NumDataNodes == skew_multiple) &&
                       (u_sess->pgxc_cxt.NumDataNodes ==
                           bias_multiple)) { /* All the expr are const, return the first expr.  */
                if (distkey == NULL)
                    distkey = lappend(distkey, expr);
                *result_multiple = u_sess->pgxc_cxt.NumDataNodes;

                continue;
            } else {
                if (skew_multiple == 1.0) {
                    /*
                     * If distinct num of multiple has no skew, we should use bias multiple to
                     * compute mix multiple.
                     */
                    multiple = bias_multiple;
                }
                else if (bias_multiple <= 1.0) /* mcf has no skew, handle skew_multiple */
                    multiple = skew_multiple;
                else
                    multiple = Max(bias_multiple, skew_multiple);

                exprMultipleList = add_multiple_to_list(expr, multiple, exprMultipleList);
            }
        }
    }

    if (exprMultipleList != NULL) {
        distkey = get_mix_diskey_by_exprlist(root, exprMultipleList, rows, result_multiple, (AggSkewInfo*)skew_info);
        list_free_ext(exprMultipleList);
    }

    return distkey;
}

static void copy_path_costsize(Path* dest, Path* src)
{
    set_path_rows(dest, src->rows, src->multiple);
    dest->startup_cost = src->startup_cost;
    dest->total_cost = src->total_cost;
    dest->locator_type = src->locator_type;
}
#endif

static ExecNodes* initExecNodes()
{
    ExecNodes* exec_nodes = (ExecNodes*)makeNode(ExecNodes);
    exec_nodes->baselocatortype = LOCATOR_TYPE_HASH;
    exec_nodes->accesstype = RELATION_ACCESS_READ;
    exec_nodes->primarynodelist = NIL;
    exec_nodes->en_expr = NULL;
    return exec_nodes;
}

// ----- new functions for node group support
ExecNodes* getExecNodesByGroupName(const char* gname)
{
    ExecNodes* exec_nodes = NULL;
    Oid* members = NULL;
    int nmembers = 0;

    Oid groupoid = get_pgxc_groupoid(gname);
    if (groupoid == InvalidOid) {
        return get_all_data_nodes(LOCATOR_TYPE_HASH);
    }

    /* First check if we already have default nodegroup set */
    nmembers = get_pgxc_groupmembers(groupoid, &members);
    AssertEreport(nmembers > 0, MOD_OPT, "invalid number of pgxc group members.");

    exec_nodes = initExecNodes();
    /* Creating executing-node list */
    for (int i = 0; i < nmembers; i++) {
        int nodeId = PGXCNodeGetNodeId(members[i], PGXC_NODE_DATANODE);
        exec_nodes->nodeList = lappend_int(exec_nodes->nodeList, nodeId);
    }
    Distribution* distribution = ng_convert_to_distribution(exec_nodes->nodeList);
    ng_set_distribution(&exec_nodes->distribution, distribution);
    exec_nodes->distribution.group_oid = ng_get_group_groupoid(gname);

    return exec_nodes;
}

ExecNodes* getRelationExecNodes(Oid tableoid)
{
    ExecNodes* exec_nodes = NULL;
    Oid* members = NULL;
    int nmembers = 0;

    /*
     * For system table, PGXC doesn't make it in pgxc_class so we are considering they
     * are belonging to installation group
     */
    if (tableoid < FirstNormalObjectId) {
        return getExecNodesByGroupName(PgxcGroupGetInstallationGroup());
    }

    /* Otherwise we are get first rtable's exec_node as default */
    nmembers = get_pgxc_classnodes(tableoid, &members);

    AssertEreport(nmembers > 0, MOD_OPT, "invalid number of datanodes.");

    exec_nodes = initExecNodes();
    /* Creating executing-node list */
    for (int i = 0; i < nmembers; i++) {
        int nodeId = PGXCNodeGetNodeId(members[i], PGXC_NODE_DATANODE);
        exec_nodes->nodeList = lappend_int(exec_nodes->nodeList, nodeId);
    }
    Distribution* distribution = ng_convert_to_distribution(exec_nodes->nodeList);
    ng_set_distribution(&exec_nodes->distribution, distribution);
    char relkind = get_rel_relkind(tableoid);
    exec_nodes->distribution.group_oid = ng_get_baserel_groupoid(tableoid, relkind);

    return exec_nodes;
}

/* Append groupingId expr to targest list
 * generate group by clauses include groupingId expr
 */
static List* add_groupingIdExpr_to_tlist(List* tlist)
{
    List* newTlist = NULL;
    ListCell* lc = NULL;
    bool include_groupId = false;

    foreach (lc, tlist) {
        TargetEntry* tl = (TargetEntry*)lfirst(lc);
        if (IsA(tl->expr, GroupingId)) {
            tl->resjunk = true;
            include_groupId = true;
            break;
        }
    }
    /* Add groupingId to targetlist */
    if (include_groupId == false) {
        GroupingId* groupId = makeNode(GroupingId);
        TargetEntry* tle = makeTargetEntry((Expr*)groupId, list_length(tlist) + 1, "groupingid", true);
        newTlist = lappend(newTlist, tle);
    }

    newTlist = list_concat((List*)copyObject(tlist), newTlist);

    return newTlist;
}

List* add_groupId_to_groupExpr(List* query_group, List* tlist)
{
    int groupMaxLen = 0;
    ListCell* target_lc = NULL;
    TargetEntry* target_group_id = NULL;

    /* Get max ressortgroupref from target list */
    foreach (target_lc, tlist) {
        TargetEntry* tg = (TargetEntry*)lfirst(target_lc);
        if ((int)tg->ressortgroupref > groupMaxLen) {
            groupMaxLen = tg->ressortgroupref;
        }
        if (IsA(tg->expr, GroupingId)) {
            target_group_id = tg;
        }
    }

    if (target_group_id != NULL) {
        List* grouplist = NIL;

        /* Add gropuingId expr to group by clause */
        groupMaxLen++;
        target_group_id->ressortgroupref = groupMaxLen;
        /* Add gropuing() expr to group by clause */
        SortGroupClause* grpcl = makeNode(SortGroupClause);
        grpcl->tleSortGroupRef = groupMaxLen;
        grpcl->eqop = INT4EQOID;
        grpcl->sortop = INT4LTOID;
        grpcl->nulls_first = false;
        grpcl->hashable = true;
        grouplist = lappend(list_copy(query_group), grpcl);
        return grouplist;
    } else
        return query_group;
}

/* set_root_matching_key
 *	for insert, update, delete statement, set matching keys in plannerinfo
 *
 * Parameters:
 *	root: plannerinfo struct in current query level
 *	targetlist: final output targetlist
 */
static void set_root_matching_key(PlannerInfo* root, List* targetlist)
{
    Query* parse = root->parse;

    /* we only set interesting matching key for non-select query */
    if (parse->commandType != CMD_SELECT) {
        RangeTblEntry* rte = rt_fetch(parse->resultRelation, parse->rtable); /* target udi relation */
        List* partAttrNum = rte->partAttrNum;                                /* partition columns in target relation */

        /* only hash table is cared */
        if (rte->locator_type == LOCATOR_TYPE_HASH) {
            ListCell* lc1 = NULL;
            ListCell* lc2 = NULL;
            bool isinvalid = false;
            foreach (lc1, partAttrNum) {
                int num = lfirst_int(lc1);
                TargetEntry* tle = NULL;

                /* For insert and update statement, we only check the expr position */
                if (parse->commandType == CMD_INSERT || parse->commandType == CMD_UPDATE) {
                    tle = (TargetEntry*)list_nth(targetlist, num - 1);
                    /* If exprs are all distributable, record them in matching key and matching nos */
                    if (IsTypeDistributable(exprType((Node*)tle->expr))) {
                        Var* var = locate_distribute_var(tle->expr);
                        if (var != NULL)
                            root->dis_keys.matching_keys = lappend(root->dis_keys.matching_keys, tle->expr);
                        else
                            isinvalid = true;
                    } else
                        isinvalid = true;
                } else {
                    /*
                     * for update, delete statement, we check the exact expr, since the position
                     * is not matched with final target list.
                     * find the exact matching expr.
                     */
                    foreach (lc2, targetlist) {
                        tle = (TargetEntry*)lfirst(lc2);
                        Var* var = (Var*)tle->expr;
                        if (IsA(var, Var) && var->varno == (Index)parse->resultRelation &&
                            var->varattno == (AttrNumber)num)
                            break;
                    }
                    if (lc2 != NULL && IsTypeDistributable(exprType((Node*)tle->expr))) {
                        root->dis_keys.matching_keys = lappend(root->dis_keys.matching_keys, tle->expr);
                    } else {
                        isinvalid = true;
                    }
                }
            }
            /* free the already allocated space if no matching key at all */
            if (isinvalid) {
                list_free_ext(root->dis_keys.matching_keys);
                root->dis_keys.matching_keys = NIL;
            }
        }
    }
}

/*
 * @Description: make dummy targetlist to current plan if the targetlist is NULL for vector engine.
 *
 * @in plan:  current plan
 */
static void make_dummy_targetlist(Plan* plan)
{
    /*
     * vector engine doesn't support empty targetlist
     * Notes: We should exclude ModifyTable nodes, because they also have empty targetlist
     */
    if (plan->targetlist == NIL && !IsA(plan, ModifyTable) &&
        !(plan->lefttree != NULL && IsA(plan->lefttree, VecModifyTable))) {
        Const* c = make_const(NULL, makeString("Dummy"), 0);
        plan->targetlist = lappend(plan->targetlist, makeTargetEntry((Expr*)c, 1, NULL, true));
    }
}

/*
 * passdown_itst_keys_to_subroot:
 *		Pass the interested keys to root of lower query level
 * Paramters:
 *	@in root: planner info of current query level
 *	@in diskeys: interested distribute keys of current subquery rel
 */
static void passdown_itst_keys_to_subroot(PlannerInfo* root, ItstDisKey* diskeys)
{
    /* for subquery, we should inherit superset key and matching key from parent root */
    if (diskeys != NULL) {
        ListCell* lc = NULL;
        List* result = NIL;
        foreach (lc, diskeys->superset_keys) {
            ListCell* lc2 = NULL;
            List* superset_keys = (List*)lfirst(lc);
            result = NIL;
            foreach (lc2, superset_keys) {
                Expr* key = (Expr*)lfirst(lc2);
                result = add_itst_node_to_list(result, root->parse->targetList, key, false);
            }
            if (result != NIL)
                root->dis_keys.superset_keys = lappend(root->dis_keys.superset_keys, result);
        }
        root->dis_keys.superset_keys = remove_duplicate_superset_keys(root->dis_keys.superset_keys);

        result = NIL;
        foreach (lc, diskeys->matching_keys) {
            Expr* key = (Expr*)lfirst(lc);
            result = add_itst_node_to_list(result, root->parse->targetList, key, true);
            if (result == NIL)
                break;
        }
        root->dis_keys.matching_keys = result;
    }
}

/*
 * add_itst_node_to_list
 *	add single node of interested keys to the target interested dis keys
 * Parameters:
 *	@in result_list: input list to add node
 *	@in target_list: targetlist of current parse tree
 *	@in node: input iterested node
 *	@is_matching_key: whether to add to matching key
 * Return:
 *	result dis keys with node added, or NIL if failed in exact match mode
 */
static List* add_itst_node_to_list(List* result_list, List* target_list, Expr* node, bool is_matching_key)
{
    Var* var = locate_distribute_var(node);

    if (var != NULL) {
        /*
         * Any item of interested key is (type cast) of var, so the varattno
         * point to the key in subquery targetlist
         */
        TargetEntry* tle = (TargetEntry*)list_nth(target_list, var->varattno - 1);
        if (is_matching_key)
            result_list = lappend(result_list, tle->expr);
        else
            result_list = list_append_unique(result_list, tle->expr);
    } else if (is_matching_key) {
        /* superset key can be partially inherited, and we remove duplicate for optimization */
        list_free_ext(result_list);
        result_list = NIL;
    }

    return result_list;
}

/*
 * vector_engine_preprocess_walker:
 *	Before join planner, check the tables and expressions in query to see if
 *	vectorized plan can be generated.
 * Parameters:
 *	@in node: parse or sub node to be check
 *	@in context: the context to mark base tables in the query
 * Return:
 *	true if there's row store table or expressions that vector engine doesn't support
 */
static bool vector_engine_preprocess_walker(Node* node, void* rtables)
{
    if (node == NULL) {
        return false;
    } else if (IsA(node, RangeTblRef)) {
        /*
         * If a row relation is found, all the plan will change to row engine.
         * For subquery, we should recursively call this walker routine.
         */
        int varno = ((RangeTblRef*)node)->rtindex;

        RangeTblEntry* rte = rt_fetch(varno, (List*)rtables);

        if (rte->rtekind == RTE_RELATION) {
            if (rte->orientation == REL_ROW_ORIENTED)
                return true;
        } else if (rte->rtekind == RTE_SUBQUERY) {
            Query* subquery = rte->subquery;

            if (vector_engine_preprocess_walker((Node*)subquery, rtables)) {
                return true;
            }
        }
    } else if (IsA(node, Query)) {
        Query* subquery = (Query*)node;

        if (subquery->hasAggs || subquery->windowClause) {
            /* Check if targetlist contains vector unsupported feature */
            if (vector_engine_expression_walker((Node*)(subquery->targetList), NULL))
                return true;

            /* Check if qual contains vector unsupported feature */
            if (vector_engine_expression_walker((Node*)(subquery->havingQual), NULL))
                return true;
        }
        /* Check if contains unsupport windowagg option */
        if (subquery->windowClause) {
            ListCell* lc = NULL;

            foreach (lc, subquery->windowClause) {
                WindowClause* wc = (WindowClause*)lfirst(lc);
                if (wc->frameOptions !=
                    (FRAMEOPTION_RANGE | FRAMEOPTION_START_UNBOUNDED_PRECEDING | FRAMEOPTION_END_CURRENT_ROW)) {
                    return true;
                }
            }
        }

        if (query_tree_walker(subquery, (bool (*)())vector_engine_preprocess_walker, (void*)subquery->rtable, 0)) {
            return true;
        }
    }

    return expression_tree_walker(node, (bool (*)())vector_engine_preprocess_walker, (void*)rtables);
}

/*
 * @Description: return true if the plan node is ForeignScan node for HDFS/OBS
 *               foreign table.
 * @param[IN] plan :  current plan node
 */
static bool is_dfs_node(Plan* plan)
{
    if (IsA(plan, VecForeignScan) || IsA(plan, ForeignScan)) {
        ForeignScan* fs = (ForeignScan*)plan;

        AssertEreport(InvalidOid != fs->scan_relid, MOD_OPT, "invalid oid of scan relation.");

        ServerTypeOption srvType = getServerType(fs->scan_relid);

        if (T_OBS_SERVER == srvType || T_HDFS_SERVER == srvType || T_TXT_CSV_OBS_SERVER == srvType) {
            /* OBS and HDFS foreign table can NOT be in the same plan. */
            if (((T_OBS_SERVER == srvType || T_TXT_CSV_OBS_SERVER == srvType) &&
                    u_sess->opt_cxt.srvtype == T_HDFS_SERVER) ||
                ((u_sess->opt_cxt.srvtype == T_OBS_SERVER || u_sess->opt_cxt.srvtype == T_TXT_CSV_OBS_SERVER) &&
                    T_HDFS_SERVER == srvType)) {
                ereport(ERROR,
                    (errmodule(MOD_ACCELERATE), errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                        errmsg("OBS and HDFS foreign table can NOT be in the same plan."),
                        errdetail("N/A"),
                        errcause("SQL uses unsupported feature."),
                        erraction("Modify SQL statement according to the manual.")));
            }

            u_sess->opt_cxt.srvtype = srvType;

            return true;
        }
    }

    return false;
}

/*
 * @Description: traverse the plan tree to find ForeignScan node for HDFS/OBS
 *               foreign table.
 *
 * @param[IN] plan :  current plan node
 * @return: if true, there are ForeignScan node(s) for HDFS/OBS foreign table in
 *          the plan tree.
 */
static bool dfs_node_exists(Plan* plan)
{
    bool found = false;

    check_stack_depth();

    /* specical case for append and modifytable node */
    if (IsA(plan, Append) || IsA(plan, ModifyTable) || IsA(plan, SubqueryScan) || IsA(plan, MergeAppend)) {
        Plan* child = NULL;
        List* plans = NIL;
        ListCell* lc = NULL;
        switch (nodeTag(plan)) {
            case T_Append:
                // VecAppend is the same as Append, so plan is casted to Append here.
                plans = ((Append*)plan)->appendplans;
                break;
            case T_ModifyTable:
                // VecModifyTable is the same as ModifyTable, so plan is casted to ModifyTable here.
                plans = ((ModifyTable*)plan)->plans;
                break;
            case T_SubqueryScan:
                plans = lappend(plans, ((SubqueryScan*)plan)->subplan);
                break;
            case T_MergeAppend:
                plans = ((MergeAppend*)plan)->mergeplans;
                break;
            default:
                break; 
        }
        /* no leaf node */
        if (plans == NIL) {
            return false;
        }

        foreach (lc, plans) {
            child = (Plan*)lfirst(lc);

            /*
             * NOT walk plan tree any more when find ForeignScan for HDFS/OBS
             * foreign table
             */
            if (dfs_node_exists(child))
                return true;
        }

        /* there is no ForeignScan node in any subtree, so return false */
        return false;
    }

    if (plan->lefttree == NULL && plan->righttree == NULL) {
        /* plan is leaf node */
        return is_dfs_node(plan);
    }

    /*
     * return true directly, if find ForeignScan for HDFS/OBS foreign table in
     * lefttree
     */
    if (plan->lefttree)
        found = dfs_node_exists(plan->lefttree);

    if (found) {
        return true;
    }

    /* walk righttree */
    if (plan->righttree)
        found = dfs_node_exists(plan->righttree);

    if (found) {
        return true;
    }

    /* there is no ForeignScan node in any subtree, so return false */
    return false;
}

static bool contains_pushdown_constraint(Plan* plan)
{
    /* is there subplan in plan node? */
    List* subplan = check_subplan_list(plan);

    /* if thers is/are subplan in child tree, do NOT pushdown the child plan
     * tree to the compute pool.
     */
    if (list_length(subplan) != 0) {
        ereport(DEBUG1,
            (errmsg("Can not push down the plan node "
                    "because there is/are subplan(s) in the targetlist or qual of the plan node.")));

        return true;
    }

    /* is there (vartype > FirstNormalObjectId) in plan node? */
    List* rs = check_vartype_list(plan);

    /* if vartype > FirstNormalObjectId, do NOT pushdown the plan node to the
     * compute pool.
     */
    if (list_length(rs) != 0) {
        ereport(DEBUG1,
            (errmsg("Can not push down the plan node "
                    "because there is user-defined data type in the targetlist or qual of the plan node.")));

        return true;
    }

    /* is there (func_oid > FirstNormalObjectId) in plan node? */
    rs = check_func_list(plan);

    /* if func_oid > FirstNormalObjectId, do NOT pushdown the plan node to the
     * compute pool.
     */
    if (list_length(rs) != 0) {
        ereport(DEBUG1,
            (errmsg("Can not push down the plan node "
                    "because there is user-defined function in the targetlist or qual of the plan node.")));

        return true;
    }

    return false;
}

/*
 * @Description: return true if the plan node can run on the compute pool.
 *
 * @param[IN] plan :  current plan node
 */
static bool is_pushdown_node(Plan* plan)
{
    const char* planname = "unknown";

    bool found = false;

    if (IsA(plan, Agg)) {
        found = true;
        planname = "Agg";
    }

    if (IsA(plan, ForeignScan)) {
        planname = "ForeignScan";

        ForeignScan* fs = (ForeignScan*)plan;

        AssertEreport(InvalidOid != fs->scan_relid, MOD_OPT, "invalid oid of scan relation.");

        ServerTypeOption srvType = getServerType(fs->scan_relid);

        if (T_OBS_SERVER == srvType || T_HDFS_SERVER == srvType || T_TXT_CSV_OBS_SERVER == srvType) {
            AssertEreport(InvalidOid != fs->scan_relid, MOD_OPT, "invalid oid of scan relation.");

            found = (fs->objectNum > u_sess->pgxc_cxt.NumDataNodes);

            Relation rel = heap_open(fs->scan_relid, NoLock);

            const char* relname = RelationGetRelationName(rel);

            heap_close(rel, NoLock);

            ereport(DEBUG1,
                (errmodule(MOD_ACCELERATE),
                    errmsg("relname: %s, file number: %ld, dn number: %d",
                        relname,
                        fs->objectNum,
                        u_sess->pgxc_cxt.NumDataNodes)));

            if (!found) {
                ereport(LOG,
                    (errmodule(MOD_ACCELERATE),
                        errmsg("%s: relname: %s, file number: %ld, dn number: %d",
                            planname,
                            relname,
                            fs->objectNum,
                            u_sess->pgxc_cxt.NumDataNodes)));
            }
        }
    }

    if (!found) {
        return false;
    }

    if (contains_pushdown_constraint(plan))
        return false;

    return true;
}

/*
 * @Description: modify the subplan to insert 2 gather nodes atop of the child.
 *
 * @param[IN] plan :  current plan node
 * @param[IN] root :  PlannerInfo*
 * @return: Plan*: accelerated sub plan(insert 2 gather node atop of the child)
 */
static Plan* insert_gather_node(Plan* child, PlannerInfo* root)
{
    RemoteQuery* plan_router = NULL;
    RemoteQuery* scan_gather = NULL;

    Index dummy_rtindex;
    char* rte_name = NULL;
    RangeTblEntry* dummy_rte = NULL; /* RTE for the remote query node being added. */

    /* make "PLAN ROUTER" RemoteQuery node */
    plan_router = makeNode(RemoteQuery);

    plan_router->position = PLAN_ROUTER;

    /*
     * Create and append the dummy range table entry to the range table.
     * Note that this modifies the master copy the caller passed us, otherwise
     * e.g EXPLAIN VERBOSE will fail to find the rte the Vars built below.
     * NOTICE: If there is only a single table, should we set the table name as
     * the name of the rte?
     */
    rte_name = "_PLAN_ROUTER_";
    dummy_rte = make_dummy_remote_rte(rte_name, makeAlias("_PLAN_ROUTER_", NIL));

    root->parse->rtable = lappend(root->parse->rtable, dummy_rte);
    dummy_rtindex = list_length(root->parse->rtable);

    plan_router->scan.scanrelid = dummy_rtindex;

    plan_router->scan.plan.targetlist = child->targetlist;

    /*
     * RemoteQuery is inserted between upper node and foreign scan, and exec_nodes
     * is meaningless for RemoteQuery and foreign scan. BUT upper node needs
     * exec_nodes, so do a copy of exec_nodes here for upper node.
     */
    inherit_plan_locator_info((Plan*)plan_router, child);

    /*
     * ExecInitRemoteQuery() uses base_tlist as scan tupledesc, so do a copy of
     * the plan->targetlist of child plan node here.
     */
    plan_router->base_tlist = NIL;

    /* appropriate values for data fields in RemoteQuery */
    plan_router->is_simple = true;
    plan_router->read_only = true;
    plan_router->is_temp = false;
    plan_router->force_autocommit = true;

    plan_router->exec_type = EXEC_ON_DATANODES;
    plan_router->spool_no_data = true;
    plan_router->poll_multi_channel = true;

    /* get better estimates */
    plan_router->scan.plan.exec_type = EXEC_ON_DATANODES;

    // queryonobs, more think about cost estimate
    plan_router->scan.plan.total_cost = child->total_cost;
    plan_router->scan.plan.startup_cost = child->startup_cost;
    plan_router->scan.plan.plan_rows = child->plan_rows;
    plan_router->scan.plan.plan_width = child->plan_width;

    /* make "SCAN GATHER" RemoteQuery node */
    scan_gather = (RemoteQuery*)copyObject(plan_router);

    scan_gather->position = SCAN_GATHER;
    scan_gather->poll_multi_channel = true;

    rte_name = "_SCAN_GATHER_";
    dummy_rte = make_dummy_remote_rte(rte_name, makeAlias("_SCAN_GATHER_", NIL));

    root->parse->rtable = lappend(root->parse->rtable, dummy_rte);
    dummy_rtindex = list_length(root->parse->rtable);

    scan_gather->scan.scanrelid = dummy_rtindex;

    /* assemble the child, "plan router" and "scan gather" together */
    scan_gather->scan.plan.lefttree = child;
    plan_router->scan.plan.lefttree = (Plan*)scan_gather;

    add_metadata(child, root);

    return (Plan*)plan_router;
}

/*
 * @Description: append metadata to the plan node.
 *
 * @param[IN] plan :  current plan node
 * @param[IN] root :  PlannerInfo*
 * @return: void
 */
static void add_metadata(Plan* plan, PlannerInfo* root)
{
    RangeTblEntry* rte = NULL;

    while (plan->lefttree)
        plan = plan->lefttree;

    if (IsA(plan, ForeignScan)) {
        ForeignScan* scan_plan = (ForeignScan*)plan;

        rte = planner_rt_fetch(scan_plan->scan.scanrelid, root);

        /*
         * put the meta data, the configuration options of the foreign table into
         * the ForeignScan node.
         */
        AssertEreport(rte->relid != InvalidOid, MOD_OPT, "invalid oid of foreign scan relation.");

        if (isObsOrHdfsTableFormTblOid(rte->relid) || IS_OBS_CSV_TXT_FOREIGN_TABLE(rte->relid)) {
            /* package the meta data of the HDFS/OBS foreign table. */
            Relation r = heap_open(rte->relid, NoLock);

            scan_plan->rel = make_relmeta(r);

            /* package the configuration options of the HDFS/OBS foreign table. */
            scan_plan->options = setForeignOptions(rte->relid);

            heap_close(r, NoLock);
        }
    }
}

/*
 * @Description: adjust pl size if file format is orc.
 *
 * @param[IN] relid :  relation oid of the obs foreign table
 * @param[IN] plan_width :  current plan width in ForeignScan node.
 * @param[IN] pl_size : input pl size
 * @param[IN/OUT] width: if not null, return the width of all columns in one relation.
 * @return: void
 */
uint64 adjust_plsize(Oid relid, uint64 plan_width, uint64 pl_size, uint64* width)
{
    uint64 rel_width = 0;

    Relation rel = heap_open(relid, NoLock);
    TupleDesc desc = rel->rd_att;

    for (int i = 0; i < desc->natts; i++) {
        Oid typoid = desc->attrs[i]->atttypid;
        int32 typmod = desc->attrs[i]->atttypmod;
        rel_width += get_typavgwidth(typoid, typmod);
    }

    heap_close(rel, NoLock);

    if (width != NULL)
        *width = rel_width;

    double real_rate = 1.0;
    if (rel_width > 0 && plan_width > 0 && plan_width < rel_width)
        real_rate = double(plan_width) / double(rel_width);
    pl_size = uint64((double)pl_size / real_rate);

    return pl_size;
}

/*
 * @Description: check whether push down the plan to the compute pool.
 *
 * @param[IN] plan :  current plan node
 * @param[IN] relname : the relation name of current ForeignScan node.
 * @return: if true, the plan will run in the compute pool.
 *
 * PL do not need to be adjusted if data format is text/csv, becasue all data
 * in files/objects will be scanned.
 *
 * If data format is orc, we need to adjust the PL according to the plan width
 * of current foreign scan node and total width of all attributes in the relation.
 * Because there is meta data in a orc file, these meta data can help reader
 * to skip data that is unused in the query.
 * For example, there are 10 columns in one relation, but there are just 2 columns
 * in some query, so reader can skip 8 unused columns data in orc file.
 */
static bool estimate_acceleration_cost_for_obs(Plan* plan, const char* relname)
{
    AssertEreport(plan, MOD_OPT, "invalid plan node.");

    ereport(DEBUG5, (errmodule(MOD_ACCELERATE), errmsg("in %s", __FUNCTION__)));

    ForeignScan* fsplan = (ForeignScan*)get_foreign_scan(plan);
    List* fOptions = getFdwOptions(fsplan->scan_relid);
    char* file_format = getFTOptionValue(fOptions, OPTION_NAME_FORMAT);

    AssertEreport(file_format, MOD_OPT, "invalid file format.");

    ComputePoolConfig** conf = get_cp_conninfo();
    uint64 pl_size = conf[0]->pl;
    pl_size *= 1024;

    int rpthreshold = conf[0]->rpthreshold;
    if (rpthreshold < 2) {
        rpthreshold = 2;
    }

    uint64 rel_width = 0;
    if (!pg_strcasecmp(file_format, "orc"))
        pl_size = adjust_plsize(fsplan->scan_relid, (uint64)((Plan*)fsplan)->plan_width, pl_size, &rel_width);

    int fnum = 0;
    uint64 totalSize = get_datasize(plan, u_sess->opt_cxt.srvtype, &fnum);

    uint64 size_per_file;
    if (0 == fnum)
        size_per_file = 0;
    else
        size_per_file = totalSize / fnum;

    uint64 fnum_per_thread = fnum / (u_sess->pgxc_cxt.NumDataNodes * u_sess->opt_cxt.query_dop);
    uint64 size_per_thread = totalSize / (u_sess->pgxc_cxt.NumDataNodes * u_sess->opt_cxt.query_dop);

    if (unlikely(pl_size == 0)) {
        ereport(ERROR,
            (errmodule(MOD_ACCELERATE), errcode(ERRCODE_DIVISION_BY_ZERO),
                errmsg("Pool size should not be zero"),
                errdetail("N/A"),
                errcause("Compute pool configuration file contains error."),
                erraction("Please check the value of \"pl\" in cp_client.conf.")));
    }
    uint64 rp_per_thread = size_per_thread / pl_size + 1;

    bool fnum_cond = fnum_per_thread < 2;
    bool rp_cond = rp_per_thread < (uint64)rpthreshold;

    ereport(DEBUG1,
        (errmodule(MOD_ACCELERATE),
            errmsg("cp_runtime_info->freerp: %d, config->pl: %dKB",
                u_sess->wlm_cxt->cp_runtime_info->freerp,
                conf[0]->pl)));

    ereport(DEBUG1,
        (errmodule(MOD_ACCELERATE),
            errmsg("u_sess->pgxc_cxt.NumDataNodes: %d, query_dop: %d",
                u_sess->pgxc_cxt.NumDataNodes,
                u_sess->opt_cxt.query_dop)));

    ereport(DEBUG1,
        (errmodule(MOD_ACCELERATE),
            errmsg("relname: %s, totalSize: %lu, filenum: %d, size_per_file: %lu, fnum_per_thread: %lu, rp_per_thread: "
                   "%lu",
                relname,
                totalSize,
                fnum,
                size_per_file,
                fnum_per_thread,
                rp_per_thread)));

    /* save the estimate detail to fdw_private list. */
    if (u_sess->attr.attr_sql.show_acce_estimate_detail && u_sess->opt_cxt.srvtype == T_OBS_SERVER) {
        uint64 orig_pl = uint64(conf[0]->pl) * 1024;

        StringInfo estimate_detail = makeStringInfo();
        appendStringInfo(
            estimate_detail, "file format: %s, pl: %lu, rp threshold: %d, ", file_format, orig_pl, rpthreshold);
        appendStringInfo(estimate_detail, "total size: %lu, file number: %d, ", totalSize, fnum);
        appendStringInfo(
            estimate_detail, "data size/thread: %lu, file number/thread: %lu, ", size_per_thread, fnum_per_thread);
        appendStringInfo(estimate_detail,
            "relation width: %lu, plan width: %d, adjusted pl: %lu, ",
            rel_width,
            fsplan->scan.plan.plan_width,
            pl_size);
        appendStringInfo(estimate_detail, "rp/thread: %lu", rp_per_thread);

        Value* val = makeString(estimate_detail->data);
        DefElem* elem = makeDefElem((char*)ESTIMATION_ITEM, (Node*)val);
        fsplan->fdw_private = lappend(fsplan->fdw_private, elem);
    }

    if (fnum_cond || rp_cond) {
        ereport(DEBUG1, (errmodule(MOD_ACCELERATE), errmsg("scan %s at the local cluster", relname)));

        return false;
    }

    ereport(DEBUG1, (errmodule(MOD_ACCELERATE), errmsg("scan %s at the compute pool", relname)));

    return true;
}

MemoryContext SwitchToPlannerTempMemCxt(PlannerInfo *root)
{
    Assert(root && root->glob && root->glob->plannerContext);

    root->glob->plannerContext->refCounter++;
    return MemoryContextSwitchTo(root->glob->plannerContext->tempMemCxt);
}

MemoryContext ResetPlannerTempMemCxt(PlannerInfo *root, MemoryContext cxt)
{
    Assert(root && root->glob && root->glob->plannerContext);
    root->glob->plannerContext->refCounter--;

    if (root->glob->plannerContext->refCounter == 0) {
        /* avoid early release caused by nested invocation. */
        MemoryContextResetAndDeleteChildren(root->glob->plannerContext->tempMemCxt);
    }

    return MemoryContextSwitchTo(cxt);
}

static void init_optimizer_context(PlannerGlobal* glob)
{
    glob->plannerContext = (PlannerContext*)palloc0(sizeof(PlannerContext));

    glob->plannerContext->plannerMemContext = AllocSetContextCreate(CurrentMemoryContext,
        "PlannerContext",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);

    if (u_sess->opt_cxt.skew_strategy_opt != SKEW_OPT_OFF) {
        glob->plannerContext->dataSkewMemContext = AllocSetContextCreate(glob->plannerContext->plannerMemContext,
            "DataSkewContext",
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE);
    }

    glob->plannerContext->tempMemCxt = AllocSetContextCreate(glob->plannerContext->plannerMemContext,
                                                            "Planner Temp MemoryContext",
                                                            ALLOCSET_DEFAULT_MINSIZE,
                                                            ALLOCSET_DEFAULT_INITSIZE,
                                                            ALLOCSET_DEFAULT_MAXSIZE);
    glob->plannerContext->refCounter = 0;
}

static void deinit_optimizer_context(PlannerGlobal* glob)
{
    if (IS_NEED_FREE_MEMORY_CONTEXT(glob->plannerContext->plannerMemContext)) {
        MemoryContextDelete(glob->plannerContext->plannerMemContext);
        glob->plannerContext->plannerMemContext = NULL;
        glob->plannerContext->dataSkewMemContext = NULL;
        glob->plannerContext->tempMemCxt = NULL;
        glob->plannerContext->refCounter = 0;
    }
}

#ifdef ENABLE_UT
bool estimate_acceleration_cost_for_HDFS(Plan* plan, const char* relname)
#else
static bool estimate_acceleration_cost_for_HDFS(Plan* plan, const char* relname)
#endif
{
    AssertEreport(plan, MOD_OPT, "invalid plan node.");

    ereport(DEBUG5, (errmodule(MOD_ACCELERATE), errmsg("in %s", __FUNCTION__)));

    if (u_sess->wlm_cxt->cp_runtime_info->dnnum == 0) {
        ereport(DEBUG1, (errmodule(MOD_ACCELERATE), "No available dn in the compute pool."));
        return false;
    }

    int fnum = 0;
    uint64 totalSize = get_datasize(plan, u_sess->opt_cxt.srvtype, &fnum);

    uint64 size_per_file;
    if (0 == fnum)
        size_per_file = 0;
    else
        size_per_file = totalSize / fnum;

    ereport(DEBUG1,
        (errmodule(MOD_ACCELERATE), errmsg("relname: %s, totalSize: %lu, filenum: %d", relname, totalSize, fnum)));

    ereport(DEBUG1,
        (errmodule(MOD_ACCELERATE),
            errmsg("u_sess->pgxc_cxt.NumDataNodes: %d, query_dop: %d",
                u_sess->pgxc_cxt.NumDataNodes,
                u_sess->opt_cxt.query_dop)));

    uint64 fnum_per_thread = fnum / (u_sess->pgxc_cxt.NumDataNodes * u_sess->opt_cxt.query_dop);
    uint64 size_per_thread = size_per_file * fnum_per_thread;

    if (fnum_per_thread < 2 || size_per_thread < (uint64)u_sess->attr.attr_sql.acce_min_datasize_per_thread * 1024) {
        ereport(DEBUG1,
            (errmodule(MOD_ACCELERATE),
                errmsg("scan %s at local cluster, reason: fnum_per_thread(%lu) < 2 || size_per_thread(%lu) < "
                       "u_sess->attr.attr_sql.acce_min_datasize_per_thread(%dMB)",
                    relname,
                    fnum_per_thread,
                    size_per_thread,
                    u_sess->attr.attr_sql.acce_min_datasize_per_thread / 1024)));

        return false;
    }

    ereport(DEBUG1, (errmodule(MOD_ACCELERATE), errmsg("scan %s at the compute pool", relname)));

    return true;
}

/*
 * @Description:
 *
 * @param[IN] plan :  current plan node
 * @return: true if the plan node can be pushdown.
 */
static bool estimate_acceleration_cost(Plan* plan)
{
    ForeignScan* fs = (ForeignScan*)get_foreign_scan(plan);

    Relation rel = heap_open(fs->scan_relid, NoLock);
    const char* rname = RelationGetRelationName(rel);
    char* relname = pstrdup(rname);
    heap_close(rel, NoLock);

    DistributeBy* dist_type = getTableDistribution(fs->scan_relid);

    AssertEreport(dist_type, MOD_OPT, "The distributeBy object is null.");

    if (NULL == dist_type) {
        ereport(DEBUG1, (errmodule(MOD_ACCELERATE), "no distribute mode in relation: %s.", relname));
        return false;
    }

    if (DISTTYPE_ROUNDROBIN != dist_type->disttype) {
        ereport(DEBUG1,
            (errmodule(MOD_ACCELERATE), "just foreign table with roundrobin option can run in the compute pool."));
        return false;
    }

    if (u_sess->opt_cxt.srvtype == T_HDFS_SERVER) {
        return estimate_acceleration_cost_for_HDFS(plan, relname);
    } else if (u_sess->opt_cxt.srvtype == T_OBS_SERVER || u_sess->opt_cxt.srvtype == T_TXT_CSV_OBS_SERVER) {
        return estimate_acceleration_cost_for_obs(plan, relname);
    } else {
        return false;
    }
}

/*
 * @Description: traverse the plan tree to add "PLAN ROUTER" and "SCAN GATHER"
 *               node for HDFS/OBS foreign table.
 *
 * @param[IN] plan :  current plan node
 * @param[IN] root :  PlannerInfo*
 * @return: if true, the lefttree is left-hand tree and can be pushed down to
 *          the compute pool.
 */
static bool walk_plan(Plan* plan, PlannerInfo* root)
{
    check_stack_depth();

    if (IsA(plan, Append) || IsA(plan, ModifyTable) || IsA(plan, SubqueryScan) || IsA(plan, MergeAppend)) {
        walk_set_plan(plan, root);
        return false;
    } else {
        return walk_normal_plan(plan, root);
    }
}

/*
 * @Description: traverse the subplans of the append and modifytable node to add
 *              "PLAN ROUTER" and "SCAN GATHER" node for HDFS/OBS foreign table.
 *
 * @param[IN] plan :  current plan node
 * @param[IN] root :  PlannerInfo*
 */
static void walk_set_plan(Plan* plan, PlannerInfo* root)
{
    RelOptInfo* rel = NULL;
    List* plans = NIL;
    switch (nodeTag(plan)) {
        case T_Append:
            plans = ((Append*)plan)->appendplans;
            break;
        case T_ModifyTable:
            plans = ((ModifyTable*)plan)->plans;
            break;
        case T_SubqueryScan:
            if (((SubqueryScan*)plan)->subplan == NULL)
                return;
            plans = lappend(plans, ((SubqueryScan*)plan)->subplan);
            /* Need to look up the subquery's RelOptInfo, since we need its subroot */
            rel = find_base_rel(root, ((SubqueryScan*)plan)->scan.scanrelid);
            AssertEreport(rel->subroot, MOD_OPT, "invalid subroot for the relation.");            
            root = rel->subroot;
            break;
        case T_MergeAppend:
            plans = ((MergeAppend*)plan)->mergeplans;
            break;
        default:
            break; 
    }
    if (plans == NIL) {
        return;
    }
    ListCell* lc = NULL;
    List* new_plans = NIL;
    foreach (lc, plans) {
        Plan* child = (Plan*)lfirst(lc);
        if (walk_plan(child, root) && estimate_acceleration_cost(child)) {
            child = insert_gather_node(child, root);
            u_sess->opt_cxt.has_obsrel = true;
            u_sess->opt_cxt.disable_dop_change = true;
        }
        new_plans = lappend(new_plans, child);
    }
    switch (nodeTag(plan)) {
        case T_Append:
            ((Append*)plan)->appendplans = new_plans;
            break;
        case T_ModifyTable:
            ((ModifyTable*)plan)->plans = new_plans;
            break;
        case T_SubqueryScan:
            ((SubqueryScan*)plan)->subplan = (Plan*)linitial(new_plans);
            rel->subplan = ((SubqueryScan*)plan)->subplan;
            break;
        case T_MergeAppend:
            ((MergeAppend*)plan)->mergeplans = new_plans;
            break;
        default:
            break; 
    }
}

/*
 * @Description: traverse the lefttree and righttree to add "PLAN ROUTER" and
 *               "SCAN GATHER" node for HDFS/OBS foreign table.
 *
 * @param[IN] plan : current plan node
 * @param[IN] root : PlannerInfo*
 * @return:  case 1: true if plan is the leaf node and (T_ForeignScan or
 *                   T_VecForeignScan);
 *           case 2: true if plan is the left-hand tree, the subplan of
 *                   lefttree returns true, and (T_Agg or T_VecAgg)
 *           return false for other cases;
 */
static bool walk_normal_plan(Plan* plan, PlannerInfo* root)
{
    bool left_found = false;
    bool right_found = false;

    /* can sub-tree pushdown ? */
    if (plan->lefttree)
        left_found = walk_plan(plan->lefttree, root);

    if (plan->righttree)
        right_found = walk_plan(plan->righttree, root);

    /* leaf node */
    if (plan->lefttree == NULL && plan->righttree == NULL) {
        /* T_VecForeignScan, T_ForeignScan */
        if (is_pushdown_node(plan))
            return true;

        return false;
    }

    /*
     * intermediate node
     * 
     * left-hand tree
     */
    if (left_found && plan->righttree == NULL && is_pushdown_node(plan)) {
        return true;
    }

    /* find right position and insert gather node */
    if (left_found && estimate_acceleration_cost(plan->lefttree)) {
        plan->lefttree = insert_gather_node(plan->lefttree, root);
        u_sess->opt_cxt.has_obsrel = true;
        u_sess->opt_cxt.disable_dop_change = true;
    }

    if (right_found && estimate_acceleration_cost(plan->righttree)) {
        plan->righttree = insert_gather_node(plan->righttree, root);
        u_sess->opt_cxt.has_obsrel = true;
        u_sess->opt_cxt.disable_dop_change = true;
    }

    return false;
}

/*
 * @Description: return true if HDFS/OBS foreign scan node found.
 *
 * @param[IN] plantree :  the root of the plan tree
 * @param[IN] glob       :  for subplan
 * @return: true if HDFS/OBS foreign scan node found
 */
static bool has_dfs_node(Plan* plantree, PlannerGlobal* glob)
{
    bool has_obsrel = false;

    /* test guc option: u_sess->attr.attr_sql.acceleration_with_compute_pool */
    if (!u_sess->attr.attr_sql.acceleration_with_compute_pool) {
        return false;
    }

    if (!IS_PGXC_COORDINATOR || IsInitdb || u_sess->analyze_cxt.is_under_analyze) {
        return false;
    }

    ereport(DEBUG5, (errmodule(MOD_ACCELERATE), errmsg("in %s", __FUNCTION__)));

    has_obsrel = dfs_node_exists(plantree);

    if (has_obsrel) {
        return true;
    } else {
        ListCell* lp = NULL;

        foreach (lp, glob->subplans) {
            Plan* plan = (Plan*)lfirst(lp);

            has_obsrel = dfs_node_exists(plan);

            if (has_obsrel) {
                return true;
            }
        }
    }

    return false;
}

/*
 * @Description: return true if precheck are ok.
 *
 * @return: true if precheck are ok
 */
static bool precheck_before_accelerate()
{
    ereport(DEBUG5, (errmodule(MOD_ACCELERATE), errmsg("in %s", __FUNCTION__)));

    char* version = NULL;

    bool available = false;
    MemoryContext current_ctx;
    PGXCNodeAllHandles* handles = NULL;

    available = true;
    current_ctx = CurrentMemoryContext;

    PG_TRY();
    {
        handles = connect_compute_pool(u_sess->opt_cxt.srvtype);

        get_cp_runtime_info(handles->datanode_handles[0]);

        if (u_sess->wlm_cxt->cp_runtime_info == NULL) {
            ereport(ERROR,
                (errmodule(MOD_ACCELERATE), errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                    errmsg("Failed to get the runtime info from the compute pool."),
                    errdetail("N/A"),
                    errcause("System error."),
                    erraction("Contact Huawei Engineer.")));
        }

        ereport(DEBUG1,
            (errmodule(MOD_ACCELERATE),
                errmsg("cp_runtime_info->dnnum  : %d", u_sess->wlm_cxt->cp_runtime_info->dnnum)));

        if (u_sess->opt_cxt.srvtype == T_OBS_SERVER) {
            ereport(DEBUG1,
                (errmodule(MOD_ACCELERATE),
                    errmsg("cp_runtime_info->freerp : %d", u_sess->wlm_cxt->cp_runtime_info->freerp)));
        } else if (u_sess->opt_cxt.srvtype == T_HDFS_SERVER) {
            ereport(DEBUG1,
                (errmodule(MOD_ACCELERATE),
                    errmsg("cp active statements : %d", u_sess->wlm_cxt->cp_runtime_info->freerp)));
        }

        if (u_sess->wlm_cxt->cp_runtime_info->version)
            ereport(DEBUG1,
                (errmodule(MOD_ACCELERATE),
                    errmsg("cp_runtime_info->version: %s", u_sess->wlm_cxt->cp_runtime_info->version)));

        if (!check_version_compatibility(u_sess->wlm_cxt->cp_runtime_info->version)) {
            ereport(ERROR,
                (errmodule(MOD_ACCELERATE), errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                    errmsg("Version is not compatible between local cluster and the compute pool."),
                    errdetail("Remote version: %s", u_sess->wlm_cxt->cp_runtime_info->version),
                    errcause("Compute pool is not installed appropriately."),
                    erraction("Configure compute pool according to manual.")));
        }
    }
    PG_CATCH();
    {
        /*
         * the compute pool is unavailable, so reset memory contex and clear
         * error stack.
         */
        MemoryContextSwitchTo(current_ctx);

        /* Save error info */
        ErrorData* edata = CopyErrorData();

        ereport(WARNING,
            (errmodule(MOD_ACCELERATE),
                errmsg("The compute pool is unavailable temporarily "
                       "when acceleration_with_compute_pool is on!\nreason: %s",
                    edata->message)));

        FlushErrorState();

        FreeErrorData(edata);

        available = false;
    }
    PG_END_TRY();

    if (version != NULL)
        pfree_ext(version);

    release_conn_to_compute_pool();

    if (available == false) {
        return false;
    }

    return true;
}

/*
 * @Description: Try to accelerate plan by pushing scan/agg node down to the
 *               compute pool, and just for HDFS/OBS foreign table.
 *
 * @param[IN] top_plan :  current plan node
 * @param[IN] root     :  PlannerInfo*
 * @return: Plan*: accelerated plan(insert 2 gather node), or leave unchanged
 */
static Plan* try_accelerate_plan(Plan* plan, PlannerInfo* root, PlannerGlobal* glob)
{
    /* test guc option: u_sess->attr.attr_sql.acceleration_with_compute_pool */
    if (!u_sess->attr.attr_sql.acceleration_with_compute_pool) {
        return plan;
    }

    if (!IS_PGXC_COORDINATOR || IsInitdb || u_sess->analyze_cxt.is_under_analyze) {
        return plan;
    }

    if (is_feature_disabled(EXPRESS_CLUSTER) == true) {
        ereport(WARNING, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Express Cluster is not supported.")));

        return plan;
    }

    ereport(DEBUG5, (errmodule(MOD_ACCELERATE), errmsg("in %s", __FUNCTION__)));

    u_sess->opt_cxt.has_obsrel = false;

    /* some precheck before do real job. */
    if (false == precheck_before_accelerate()) {
        return plan;
    }

    /* try to modify plan to add "PLAN ROUTER" and "SCAN GATHER" node */
    (void)walk_plan(plan, root);

    List* new_subplans = NIL;
    ListCell* lc1 = NULL;
    ListCell* lc2 = NULL;
    forboth(lc1, glob->subplans, lc2, glob->subroots)
    {
        Plan* aplan = (Plan*)lfirst(lc1);

        PlannerInfo* aroot = (PlannerInfo*)lfirst(lc2);

        (void)walk_plan(aplan, aroot);

        new_subplans = lappend(new_subplans, aplan);
    }

    glob->subplans = new_subplans;

    return plan;
}

bool enable_check_implicit_cast()
{
    if (u_sess->attr.attr_common.check_implicit_conversions_for_indexcol &&
        !u_sess->attr.attr_sql.enable_fast_query_shipping && IS_PGXC_COORDINATOR && !IsConnFromCoord())
        return true;

    return false;
}

static void find_implicit_cast_var(Query* query)
{
    g_index_vars = NIL;

    ImplicitCastVarContext* ctx = (ImplicitCastVarContext*)palloc0(sizeof(ImplicitCastVarContext));

    (void)implicit_cast_var_walker((Node*)query, (void*)ctx);

    g_index_vars = ctx->vars;
}

static bool implicit_cast_var_walker(Node* node, void* context)
{
    if (node == NULL)
        return false;

    ImplicitCastVarContext* ctx = (ImplicitCastVarContext*)context;

    if (IsA(node, Query)) {
        Query* query = (Query*)node;

        ctx->queries = lappend(ctx->queries, query);

        bool result = query_tree_walker(query, (bool (*)())implicit_cast_var_walker, context, 0);

        ctx->queries = list_delete_ptr(ctx->queries, query);

        return result;
    } else if (IsA(node, FuncExpr)) {
        FuncExpr* func = (FuncExpr*)node;
        if (func->funcformat == COERCE_IMPLICIT_CAST && list_length(func->args) == 1) {
            Node* arg = (Node*)linitial(func->args);
            save_implicit_cast_var(arg, context);
            return false;
        }
    } else
        return expression_tree_walker(node, (bool (*)())implicit_cast_var_walker, (void*)context);

    return false;
}

static void save_implicit_cast_var(Node* node, void* context)
{
    if (!IsA(node, Var))
        return;

    Var* var = (Var*)node;
    if (var->varlevelsup != 0 || var->varattno < 1)
        return;

    ImplicitCastVarContext* ctx = (ImplicitCastVarContext*)context;
    Query* query = (Query*)llast(ctx->queries);

    RangeTblEntry* rtable = (RangeTblEntry*)list_nth(query->rtable, var->varno - 1);
    if (rtable == NULL)
        return;

    if (rtable->rtekind != RTE_RELATION)
        return;

    Relation rel = heap_open(rtable->relid, AccessShareLock);

    IndexVar* new_node = makeNode(IndexVar);

    new_node->relid = rtable->relid;
    new_node->attno = var->varattno;
    new_node->indexcol = false;

    new_node->relname = pstrdup(rtable->relname);
    new_node->attname = pstrdup(rel->rd_att->attrs[var->varattno - 1]->attname.data);

    heap_close(rel, AccessShareLock);

    ctx->vars = lappend(ctx->vars, new_node);
}

/*
 * notify user to check plan for potential problem, if does not
 * create index path for index column with type conversion.
 */
static void check_index_column()
{
    bool found = false;
    ListCell* lc = NULL;
    StringInfo si;

    si = makeStringInfo();

    foreach (lc, g_index_vars) {
        IndexVar* var = (IndexVar*)lfirst(lc);
        if (var == NULL || !var->indexcol)
            continue;

        if (!var->indexpath) {
            found = true;
            appendStringInfo(si, "\"%s\".\"%s\", ", var->relname, var->attname);
        }
    }

    g_index_vars = NIL;

    if (found) {
        si->data[si->len - 2] = '\0';
        ereport(ERROR,
            (errmodule(MOD_OPT_PLANNER), errcode(ERRCODE_WARNING),
                errmsg("No optional index path is found."),
                errdetail("Index column: %s", si->data),
                errcause("System error."),
                erraction("Contact Huawei Engineer.")));
    }
}

#ifdef ENABLE_MULTIPLE_NODES
/*
 * @Description: find (vec)agg -> (vec)foreignscan structure.
 *
 * @param[IN] plan :  current plan node
 */
static bool find_right_agg(Plan* plan)
{
    /* check plan structure, just for agg -> foreignscan */
    if (plan && (IsA(plan, Agg) || IsA(plan, VecAgg))) {
        ereport(DEBUG1, (errmodule(MOD_COOP_ANALYZE), errmsg("Agg node found")));

        /* Is there any subplan, vartyep, or user-defined function in agg node? */
        if (contains_pushdown_constraint(plan))
            return false;
    } else
        return false;

    if (plan->lefttree && (IsA(plan->lefttree, ForeignScan) || IsA(plan->lefttree, VecForeignScan))) {
        ereport(DEBUG1, (errmodule(MOD_COOP_ANALYZE), errmsg("ForeignScan node found")));
    } else
        return false;

    /* check fdw type  */
    ForeignScan* fscan = (ForeignScan*)plan->lefttree;
    if (IsSpecifiedFDWFromRelid(fscan->scan_relid, GC_FDW)) {
        ereport(DEBUG1, (errmodule(MOD_COOP_ANALYZE), errmsg("ForeignScan node is gc_fdw type")));
    } else
        return false;

    /* Is there any subplan, vartyep, or user-defined function in foreignscan node? */
    if (contains_pushdown_constraint((Plan*)fscan))
        return false;

    /* if there is local qual in foreignscan node, don't pushdown agg to remote server. */
    if (plan->lefttree->qual)
        return false;

    return true;
}

/*
 * @Description: traverse the plan to deparse agg node.
 *
 * @param[IN] plan :  current plan node
 * @param[IN] root :  PlannerInfo*
 */
static bool walk_plan_for_coop_analyze(Plan* plan, PlannerInfo* root)
{
    Query* query = root->parse;

    /*
     * These optimizations do not work in presence of the window functions,
     * because of the target list adjustments. The targetlist set for the passed
     * in Group/Agg plan nodes contains window functions if any, but gets
     * changed while planning for windowing. So, for now stay away :)
     */
    if (query->hasWindowFuncs)
        return false;

    /*
     * To Ap function, all aggregation operators need be computed on coordinator when plan can not push-down
     * because need do more than once group by handles and different group
     * need separate by GROUPINGID.
     */
    if (query->groupingSets)
        return false;

    check_stack_depth();

    if (IsA(plan, Append) || IsA(plan, VecAppend) || IsA(plan, ModifyTable) || IsA(plan, VecModifyTable) ||
        IsA(plan, SubqueryScan) || IsA(plan, VecSubqueryScan) || IsA(plan, MergeAppend) || IsA(plan, VecMergeAppend)) {
        walk_set_plan_for_coop_analyze(plan, root);
        return false;
    } else {
        return walk_normal_plan_for_coop_analyze(plan, root);
    }
}

/*
 * @Description: traverse the subplans of the append and modifytable node.
 *
 * @param[IN] plan :  current plan node
 * @param[IN] root :  PlannerInfo*
 */
static void walk_set_plan_for_coop_analyze(Plan* plan, PlannerInfo* root)
{
    RelOptInfo* rel = NULL;
    List* plans = NIL;

    if (IsA(plan, Append) || IsA(plan, VecAppend)) {
        plans = ((Append*)plan)->appendplans;
    }

    if (IsA(plan, ModifyTable) || IsA(plan, VecModifyTable)) {
        plans = ((ModifyTable*)plan)->plans;
    }

    if (IsA(plan, SubqueryScan) || IsA(plan, VecSubqueryScan)) {
        if (((SubqueryScan*)plan)->subplan == NULL)
            return;

        plans = lappend(plans, ((SubqueryScan*)plan)->subplan);

        /* Need to look up the subquery's RelOptInfo, since we need its subroot */
        int relid = ((SubqueryScan*)plan)->scan.scanrelid;

        AssertEreport(relid > 0, MOD_OPT, "invalid oid of scan relation.");

        if (relid < root->simple_rel_array_size) {
            rel = root->simple_rel_array[relid];
            if (rel != NULL && rel->subroot && rel->reloptkind == RELOPT_BASEREL && rel->rtekind == RTE_SUBQUERY) {
                root = rel->subroot;
            } else
                return;
        } else
            return;
    }

    if (IsA(plan, MergeAppend) || IsA(plan, VecMergeAppend)) {
        plans = ((MergeAppend*)plan)->mergeplans;
    }

    if (plans == NIL) {
        return;
    }

    ListCell* lc = NULL;
    List* new_plans = NIL;
    foreach (lc, plans) {
        Plan* child = (Plan*)lfirst(lc);

        if (walk_plan_for_coop_analyze(child, root) && find_right_agg(child)) {
            child = deparse_agg_node(child, root);
        }

        new_plans = lappend(new_plans, child);
    }

    if (IsA(plan, Append) || IsA(plan, VecAppend)) {
        ((Append*)plan)->appendplans = new_plans;
    }

    if (IsA(plan, ModifyTable) || IsA(plan, VecModifyTable)) {
        ((ModifyTable*)plan)->plans = new_plans;
    }

    if (IsA(plan, SubqueryScan) || IsA(plan, VecSubqueryScan)) {
        ((SubqueryScan*)plan)->subplan = (Plan*)linitial(new_plans);
        rel->subplan = ((SubqueryScan*)plan)->subplan;
    }

    if (IsA(plan, MergeAppend) || IsA(plan, VecMergeAppend)) {
        ((MergeAppend*)plan)->mergeplans = new_plans;
    }
}

/*
 * @Description: traverse the lefttree and righttree to find (vec)agg -> (vec)foreignscan
 *
 * @param[IN] plan : current plan node
 * @param[IN] root : PlannerInfo*
 * @return:  case 1: true if plan is the leaf node and (T_ForeignScan or
 *                   T_VecForeignScan);
 *           case 2: true if plan is the left-hand tree, the subplan of
 *                   lefttree returns true, and (T_Agg or T_VecAgg)
 *           return false for other cases;
 */
static bool walk_normal_plan_for_coop_analyze(Plan* plan, PlannerInfo* root)
{
    bool left_found = false;
    bool right_found = false;

    /* can sub-tree pushdown ? */
    if (plan->lefttree)
        left_found = walk_plan_for_coop_analyze(plan->lefttree, root);

    if (plan->righttree)
        right_found = walk_plan_for_coop_analyze(plan->righttree, root);

    /* leaf node */
    if (plan->lefttree  == NULL && plan->righttree  == NULL) {
        if (IsA(plan, Agg) || IsA(plan, ForeignScan) || IsA(plan, VecAgg) || IsA(plan, VecForeignScan))
            return true;

        return false;
    }

    /* 
     * intermediate node
     * 
     * left-hand tree
     */
    if (left_found &&  plan->righttree == NULL &&
        (IsA(plan, Agg) || IsA(plan, ForeignScan) || IsA(plan, VecAgg) || IsA(plan, VecForeignScan))) {
        return true;
    }

    /* find right position and deparse agg node */
    if (left_found && find_right_agg(plan->lefttree)) {
        plan->lefttree = deparse_agg_node(plan->lefttree, root);
    }

    if (right_found && find_right_agg(plan->righttree)) {
        plan->righttree = deparse_agg_node(plan->righttree, root);
    }

    return false;
}

/*
 * return true if pg relation exists.
 */
static bool has_pgfdw_rel(PlannerInfo* root)
{
    for (int i = 1; i < root->simple_rel_array_size; i++) {
        RangeTblEntry* rte = root->simple_rte_array[i];

        if (rte->rtekind == RTE_RELATION && (rte->relkind == RELKIND_FOREIGN_TABLE || rte->relkind == RELKIND_STREAM)
            && IsSpecifiedFDWFromRelid(rte->relid, GC_FDW)) {
            return true;
        }
    }

    return false;
}
#endif

/*
 * @Description: just for cooperation analysis on client cluster,
 *               try to deparse agg node to remote sql in ForeignScan node.
 *
 * @param[IN] top_plan :  current plan node
 * @param[IN] root     :  PlannerInfo*
 * @return: Plan*: remote sql includes agg functions, or leave unchanged
 */
static Plan* try_deparse_agg(Plan* plan, PlannerInfo* root, PlannerGlobal* glob)
#ifndef ENABLE_MULTIPLE_NODES
{
    return plan;
}
#else
{
    if (IS_PGXC_DATANODE || !u_sess->attr.attr_sql.enable_agg_pushdown_for_cooperation_analysis)
        return plan;

    MemoryContext current_ctx = CurrentMemoryContext;

    /* walk plantree to deparse agg node to remote sql in foreign scan. */
    PG_TRY();
    {
        if (true == has_pgfdw_rel(root)) {
            /* try to modify plan to covert agg node to sql */
            (void)walk_plan_for_coop_analyze(plan, root);
        }
    }
    PG_CATCH();
    {
        /* reset memory contex and clear error stack. */
        MemoryContextSwitchTo(current_ctx);

        /* Save error info */
        ErrorData* edata = CopyErrorData();

        StringInfo warning = makeStringInfo();
        appendStringInfo(warning, "Failed to deparse agg node. cause: %s", edata->message);
        glob->hint_warning = lappend(glob->hint_warning, makeString(warning->data));

        FlushErrorState();

        FreeErrorData(edata);
    }
    PG_END_TRY();

    /* walk all sub-plantree to deparse agg node to remote sql in foreign scan. */
    ListCell* lc1 = NULL;
    ListCell* lc2 = NULL;
    forboth(lc1, glob->subplans, lc2, glob->subroots)
    {
        Plan* aplan = (Plan*)lfirst(lc1);

        PlannerInfo* aroot = (PlannerInfo*)lfirst(lc2);

        if (false == has_pgfdw_rel(aroot))
            continue;

        current_ctx = CurrentMemoryContext;

        PG_TRY();
        {
            (void)walk_plan_for_coop_analyze(aplan, aroot);
        }
        PG_CATCH();
        {
            /* reset memory contex and clear error stack. */
            MemoryContextSwitchTo(current_ctx);

            /* Save error info */
            ErrorData* edata = CopyErrorData();

            StringInfo warning = makeStringInfo();
            appendStringInfo(warning, "Failed to deparse agg node. cause: %s", edata->message);
            glob->hint_warning = lappend(glob->hint_warning, makeString(warning->data));

            FlushErrorState();

            FreeErrorData(edata);
        }
        PG_END_TRY();
    }

    return plan;
}
#endif

/*
 * @Description: find the remote query node in plan tree and modify nodelst in exec_nodes
 *                       just for cooperation analysis on source data cluster,
 *                       reassign dn list scaned of RemoteQuery node for the request from client cluster.
 *
 * @param[IN] plan : current plan node
 * @param[IN] root : PlannerInfo*
 */
static void find_remotequery_in_normal_plan(Plan* plan, PlannerInfo* root)
{
    if (IsA(plan, RemoteQuery) || IsA(plan, VecRemoteQuery)) {
        /*
         * currently, sql from client cluster just includes ONE relation.
         * so, root->simple_rte_array[1] is always valid.
         */
        RangeTblEntry* rte = root->simple_rte_array[1];

        RemoteQuery* rq = (RemoteQuery*)plan;

        rq->exec_nodes->nodeList = reassign_nodelist(rte, rq->exec_nodes->nodeList);
        return;
    }

    if (plan->lefttree)
        find_remotequery(plan->lefttree, root);

    if (plan->righttree)
        find_remotequery(plan->righttree, root);
}

/*
 * @Description: find the remote query node in plan tree and modify nodelst in exec_nodes
 *                       just for cooperation analysis on source data cluster,
 *                       reassign dn list scaned of RemoteQuery node for the request from client cluster.
 *
 * @param[IN] plan :  current plan node
 * @param[IN] root :  PlannerInfo*
 */
static void find_remotequery_in_set_plan(Plan* plan, PlannerInfo* root)
{
    RelOptInfo* rel = NULL;
    List* plans = NIL;

    if (IsA(plan, Append)) {
        plans = ((Append*)plan)->appendplans;
    }

    if (IsA(plan, ModifyTable)) {
        plans = ((ModifyTable*)plan)->plans;
    }

    if (IsA(plan, SubqueryScan)) {
        if (((SubqueryScan*)plan)->subplan == NULL)
            return;

        plans = lappend(plans, ((SubqueryScan*)plan)->subplan);

        /* Need to look up the subquery's RelOptInfo, since we need its subroot */
        rel = find_base_rel(root, ((SubqueryScan*)plan)->scan.scanrelid);

        /* special case for inlist join */
        if (rel->subroot == NULL) {
            /* rel->alternatives's length always be 1. */
            AssertEreport(rel->alternatives, MOD_OPT, "invalid alternative reltion.");
            AssertEreport(list_length(rel->alternatives) == 1, MOD_OPT, "invalid length of alternative relation list.");

            RelOptInfo* subquery_rel = (RelOptInfo*)linitial(rel->alternatives);

            AssertEreport(subquery_rel->subroot, MOD_OPT, "invalid PlannerInfo object for subquery relation.");

            root = subquery_rel->subroot;
        } else
            root = rel->subroot;
    }

    if (IsA(plan, MergeAppend)) {
        plans = ((MergeAppend*)plan)->mergeplans;
    }

    if (plans == NIL) {
        return;
    }

    ListCell* lc = NULL;
    foreach (lc, plans) {
        Plan* child = (Plan*)lfirst(lc);
        find_remotequery(child, root);
    }
}

/*
 * @Description: find the remote query node in plan tree and modify nodelst in exec_nodes
 *                       just for cooperation analysis on source data cluster,
 *                       reassign dn list scaned of RemoteQuery node for the request from client cluster.
 *
 * @param[IN] plan :  current plan node
 * @param[IN] root :  PlannerInfo*
 */
static void find_remotequery(Plan* plan, PlannerInfo* root)
{
    check_stack_depth();

    /* gc_fdw_max_idx means that request is from client cluster */
    if (IS_PGXC_COORDINATOR && u_sess->pgxc_cxt.is_gc_fdw && u_sess->pgxc_cxt.gc_fdw_max_idx > 0) {
        if (IsA(plan, Append) || IsA(plan, ModifyTable) || IsA(plan, SubqueryScan) || IsA(plan, MergeAppend)) {
            find_remotequery_in_set_plan(plan, root);
        } else {
            find_remotequery_in_normal_plan(plan, root);
        }
    }
}

/*
 * - contains recursive subplan
 */
bool ContainRecursiveUnionSubplan(PlannedStmt* pstmt)
{
    bool with_recursive = false;
    ListCell* lc = NULL;
    List* subplans = pstmt->subplans;

    Assert(pstmt != NULL);

    if (!u_sess->attr.attr_sql.enable_stream_recursive || subplans == NIL) {
        return false;
    }

    foreach (lc, subplans) {
        Plan* plan = (Plan*)lfirst(lc);

        if (plan && IsA(plan, RecursiveUnion)) {
            with_recursive = true;
            break;
        }
    }

    return with_recursive;
}

/*
 * @Description: get all remotequerys and fill in g_RemoteQueryList.
 * @in stmt - PlannedStmt information
 * @in queryString - executed statement or NULL when simple query
 * @out - void
 */
void GetRemoteQuery(PlannedStmt* stmt, const char* queryString)
{
    FindRQContext context;
    context.rqs = NIL;
    context.include_all_plans = false;
    context.has_modify_table = false;
    context.under_mergeinto = false;
    context.elevel = 0;
    ListCell* lc = NULL;

    if (stmt == NULL)
        return;

    GetRemoteQueryWalker(stmt->planTree, &context, queryString);

    foreach (lc, stmt->subplans) {
        Plan* plan = (Plan*)lfirst(lc);

        GetRemoteQueryWalker(plan, &context, queryString);
    }

    u_sess->exec_cxt.remotequery_list = context.rqs;
    context.rqs = NIL;
}

/*
 * @Description: remotequery walker for find remotequery plannode.
 * @in plan - Plan information
 * @in context - FindRQContext
 * @in queryString - execute sql statement in pbe.
 * @out - void
 */
void GetRemoteQueryWalker(Plan* plan, void* context, const char* queryString)
{
    if (plan == NULL)
        return;

    if (IsA(plan, RemoteQuery) || IsA(plan, VecRemoteQuery)) {
        RemoteQuery* rq = (RemoteQuery*)plan;

        if (rq->position == PLAN_ROUTER || rq->position == SCAN_GATHER)
            return;

        /* is_simple is ture means do not push down query */
        if (rq->is_simple)
            return;

        /* remember queryString in sql_statement */
        if (queryString != NULL) {
            rq->execute_statement = (char*)queryString;
        } else {
            rq->execute_statement = rq->sql_statement;
        }
        FindRQContext* ctx = (FindRQContext*)context;
        ctx->rqs = lappend(ctx->rqs, plan);
        return;
    }

    PlanTreeWalker(plan, GetRemoteQueryWalker, context, queryString);
}

static void
collect_exec_nodes(ExecNodes *exec_nodes, void *context)
{
    Assert(context != NULL);

    if (exec_nodes == NULL) {
        return;
    }

    FindNodesContext *ctx = (FindNodesContext *)context;
     ctx->nodeList = list_concat_unique_int(ctx->nodeList, exec_nodes->nodeList);
}

/* Process the top node. */
static void gtm_process_top_node(Plan *plan, void *context)
{
    FindNodesContext *ctx = (FindNodesContext*)context;
    if (IsA(plan, RemoteQuery) || IsA(plan, VecRemoteQuery)) {
        RemoteQuery *rq = (RemoteQuery*)plan;

        /* Refer to ng_get_dest_execnodes */
        if (rq->exec_nodes != NULL) {
            collect_exec_nodes(rq->exec_nodes, context);
        } else {
            collect_exec_nodes(plan->exec_nodes, context);
        }

        if (rq->position != PLAN_ROUTER && rq->position != SCAN_GATHER) {
            ctx->remote_query_count++;
        }
    } else if (IsA(plan, ModifyTable) || IsA(plan, VecModifyTable)) {
        FindNodesContext *ctx = (FindNodesContext*)context;
        ctx->has_modify_table = true;
    } else if (IsA(plan, Stream) || IsA(plan, VecStream)) {
        Stream *st= (Stream*)plan;
        collect_exec_nodes(st->scan.plan.exec_nodes, context);
        collect_exec_nodes(st->consumer_nodes, context);
    }
}

/*
 * ONLY used in GTM-Free mode.
 *
 * This function will iterate the plan tree and find:
 *     1) How many RemoteQuery/VecRemoteQuery in this plan
 *     2) The DNs on which the query will execute.
 *     3) Will this query write to the database?
 */
static void
gtm_free_rqs_nodes_walker(Plan *plan, void *context)
{

    ListCell *lc = NULL;
    List *children_nodes  = NIL;

    Assert(context != NULL);

    if (plan == NULL) {
        return;
    }

    gtm_process_top_node(plan, context);

    /* Find the children node and call gtm_free_rqs_nodes_walker recursively. */
    if (IsA(plan, Append) || IsA(plan, VecAppend)) {
        children_nodes = ((Append*)plan)->appendplans;
    } else if (IsA(plan, ModifyTable) || IsA(plan, VecModifyTable)) {
        /* list_concat will destory the plantree, so use lappend */
        foreach(lc, ((ModifyTable*)plan)->plans) {
            children_nodes = lappend(children_nodes, lfirst(lc));
        }

        foreach(lc, ((ModifyTable*)plan)->remote_plans) {
            children_nodes = lappend(children_nodes, lfirst(lc));
        }

        foreach(lc, ((ModifyTable*)plan)->remote_insert_plans) {
            children_nodes = lappend(children_nodes, lfirst(lc));
        }

        foreach(lc, ((ModifyTable*)plan)->remote_update_plans) {
            children_nodes = lappend(children_nodes, lfirst(lc));
        }

        foreach(lc, ((ModifyTable*)plan)->remote_delete_plans) {
            children_nodes = lappend(children_nodes, lfirst(lc));
        }
    } else if (IsA(plan, MergeAppend) || IsA(plan, VecMergeAppend)) {
        children_nodes = ((MergeAppend*)plan)->mergeplans;
    } else if (IsA(plan, SubqueryScan) || IsA(plan, VecSubqueryScan)) {
        children_nodes = lappend(children_nodes, ((SubqueryScan*)plan)->subplan);
    } else {
        if (plan->lefttree) {
            children_nodes = lappend(children_nodes, plan->lefttree);
        }
        if (plan->righttree) {
            children_nodes = lappend(children_nodes, plan->righttree);
        }
    }

    foreach(lc, children_nodes) {
        Plan *child = (Plan*)lfirst(lc);
        gtm_free_rqs_nodes_walker(child, context);
    }
}

/*
 * @Description: plan tree walker for find remotequery plannode.
 * @in plan - Plan information
 * @in walker - function pointer
 * @in context - FindRQContext
 * @in queryString - execute sql statement in pbe.
 * @out - void
 */
void PlanTreeWalker(Plan* plan, void (*walker)(Plan*, void*, const char*), void* context, const char* queryString)
{
    ListCell* lc = NULL;
    List* plans = NIL;

    if (plan == NULL)
        return;

    if (IsA(plan, Append) || IsA(plan, VecAppend)) {
        plans = ((Append*)plan)->appendplans;
    } else if (IsA(plan, ModifyTable) || IsA(plan, VecModifyTable)) {
        plans = ((ModifyTable*)plan)->plans;
    } else if (IsA(plan, MergeAppend) || IsA(plan, VecMergeAppend)) {
        plans = ((MergeAppend*)plan)->mergeplans;
    } else if (IsA(plan, SubqueryScan) || IsA(plan, VecSubqueryScan)) {
        plans = lappend(plans, ((SubqueryScan*)plan)->subplan);
    } else {
        if (plan->lefttree)
            plans = lappend(plans, plan->lefttree);
        if (plan->righttree)
            plans = lappend(plans, plan->righttree);
    }

    if (plans == NIL)
        return;

    foreach (lc, plans) {
        Plan* child = (Plan*)lfirst(lc);
        walker(child, context, queryString);
    }
}

/*
 * Fill FindNodesContext, see FindNodesContext for more information.
 *
 * ONLY used in GTM-Free mode.
 */
static void
collect_query_info(PlannedStmt *stmt, FindNodesContext *context)
{
    Assert(stmt != NULL);
    Assert(context != NULL);

    context->remote_query_count = 0;
    context->has_modify_table = false;
    context->nodeList = NIL;

    gtm_free_rqs_nodes_walker(stmt->planTree, context);

    ListCell* lc = NULL;
    foreach(lc, stmt->subplans) {
        Plan *plan = (Plan*)lfirst(lc);
        gtm_free_rqs_nodes_walker(plan, context);
    }
}

/*
 * Report error if 1) the query need to split into multiple queries and 2) the query need to write to the database.
 * see FindNodesContext for more information.
 *
 * ONLY used in GTM-Free mode.
 */
static void
block_write_when_split_queries(PlannedStmt *stmt, FindNodesContext *context, int elevel)
{
    if (context->remote_query_count > 1 && context->has_modify_table) {
        ereport(elevel,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("INSERT/UPDATE/DELETE/MERGE contains multiple remote queries under GTM-free mode"),
                 errhint("modify your SQL to generate light-proxy or fast-query-shipping plan")));
    }
}

/*
 * ONLY used in GTM-Free mode.
 *
 * If the query needs more than one DN to be involved in:
 *
 * When multinode hint is used:
 *     allow the query to be executed, no warning/error
 * When multinode hint is not used:
 *     if application_type is:
 *         not_perfect_sharding_type(Default): allow the query to be executed, no warning/error
 *         perfect_sharding_type: report ERROR
 */
static void
block_query_need_multi_nodes(PlannedStmt *stmt, FindNodesContext *context, int elevel)
{
    if (u_sess->attr.attr_sql.application_type != PERFECT_SHARDING_TYPE) {
        return;
    }

    if (stmt->multi_node_hint) {
        return;
    }

    if (list_length(context->nodeList) > 1) {
        const char *sql = t_thrd.postgres_cxt.debug_query_string;
        if (sql == NULL) {
            sql = "No sql in this query, please check other warnings and ignore this one.";
        }
        ereport(elevel,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("Your SQL needs more than one datanode to be involved in. SQL Statement:\n%s", sql),
                 errhint("(1) use hint /*+ multinode */\n"
                  "       (2) make your SQL be perfect sharding."
                  "\nChoose either of the above options is OK.")));
    }
}

/*
 * ONLY used in GTM-Free mode.
 *
 * In GTM-Free mode, we should apply some restrictions on the query.
 * check_gtm_free_plan is the ENTRY to check if the query satisfies these restrictioins.
 */
void check_gtm_free_plan(PlannedStmt *stmt, int elevel)
{
    if (stmt == NULL)
        return;

    /* only check under gtm free */
    if (!g_instance.attr.attr_storage.enable_gtm_free)
        return;

    if (u_sess->attr.attr_sql.enable_cluster_resize)
        return;

    /* no need to check on DN for now */
    if (IS_PGXC_DATANODE || IS_SINGLE_NODE)
        return;

    /* In redistribution, the user's distributed query is allowed. */
    elevel = (elevel > WARNING && ClusterResizingInProgress()) ? WARNING : elevel;

    FindNodesContext context;

    collect_query_info(stmt, &context);
    block_write_when_split_queries(stmt, &context, elevel);
    block_query_need_multi_nodes(stmt, &context, elevel);
}

/*
 * @Description: plan walker to find MERGE INTO -> BROADCAST pattern.
 * @in node - Plan information
 * @in context - FindRQContext
 * @in queryString - execute sql statement in pbe.
 * @out - void
 */
static void check_plan_mergeinto_replicate_walker(Plan* node, void* context, const char *queryString)
{
    Assert(context != NULL);

    if (node == NULL) {
        return;
    }

    FindRQContext *ctx = (FindRQContext*)context;
    if (ctx->under_mergeinto) {
        if (IsA(node, Stream) || IsA(node, VecStream)) {
            ereport(ctx->elevel,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("MERGE INTO on replicated table does not yet support followed stream.")));
        }
    }

    ctx->under_mergeinto = ctx->under_mergeinto ||
        (IsA(node, ModifyTable) && ((ModifyTable*)node)->operation == CMD_MERGE) ||
        (IsA(node, VecModifyTable) && ((VecModifyTable*)node)->operation == CMD_MERGE);

    PlanTreeWalker(node, check_plan_mergeinto_replicate_walker, context, NULL);
}

/**
 * @Description: plan tree walker for find remotequery plannode.
 *      Here we walk through the entire plantree for MERGE INTO statement with
 *      replcate target relation to find the pattern:
 *              MODIFYTABLE (MERGE INTO)
 *              ...
 *              └─ STREAM
 *      where SREAM appears under MERGE INTO if merge target is replicated.
 *      This is because when join results are streamed to other datanode, the
 *      junkfilter ctid will become a mess. This may be an overkill since planner
 *      only allows all replicate relations in source subquery. However, we
 *      double-check the plan here for extra safety.
 *      This should be later fixed by replacing ctid by PK or attributes which
 *      yields to unique and not null constraint.
 * @in stmt - Plan information
 * @out - void
 */
void check_plan_mergeinto_replicate(PlannedStmt* stmt, int elevel)
{
    if (stmt == NULL || stmt->commandType != CMD_MERGE) {
        return;
    }

    /* No need to check if none of the targets are replicate */
    bool no_replicate = true;
    ListCell* lc = NULL;
    foreach(lc, stmt->resultRelations) {
        Index rti = lfirst_int(lc);
        RangeTblEntry* target = rt_fetch(rti, stmt->rtable);
        if (GetLocatorType(target->relid) == LOCATOR_TYPE_REPLICATED) {
            no_replicate = false;
            break;
        }
    }
    if (no_replicate) {
        return;
    }
    FindRQContext context;
    context.rqs = NIL;
    context.include_all_plans = false;
    context.has_modify_table = false;
    context.under_mergeinto = false;
    context.elevel = elevel;
    PlanTreeWalker((Plan*)stmt->planTree, check_plan_mergeinto_replicate_walker, (void*) &context, NULL);
}

/*
 * @Description:
 *    For mergeinto commands with replicated target table,
 *    we only allow source relation/subquery to be fully
 *    replicated.
 *
 * @param[IN] parse: parsed query tree.
 *
 * @return void
 */
void check_entry_mergeinto_replicate(Query* parse)
{
    RangeTblEntry* target = rt_fetch(parse->mergeTarget_relation, parse->rtable);
    Assert(target != NULL);
    /* only deal with replicated target */
    if (GetLocatorType(target->relid) != LOCATOR_TYPE_REPLICATED) {
        return;
    }
    ListCell* lc = NULL;
    foreach(lc, parse->rtable) {
        RangeTblEntry* rte = (RangeTblEntry*)lfirst(lc);
        /* only deal with plain relations */
        if (rte->rtekind != RTE_RELATION)
            continue;
        if (GetLocatorType(rte->relid) != LOCATOR_TYPE_REPLICATED) {
            ereport(ERROR,
                (errmodule(MOD_OPT_PLANNER), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("MERGE INTO on replicated table does not yet support using distributed tables."),
                    errdetail("N/A"),
                    errcause("SQL uses unsupported feature."),
                    erraction("Modify SQL statement according to the manual.")));
        }
    }
}

static bool check_sort_for_upsert(PlannerInfo* root)
{
    PlannerInfo* plan_info = root;
    Query* parse = plan_info->parse;

    /* ORDER BY LIMIT can guarentee the ordering */
    if (parse->limitCount || parse->limitOffset)
        return false;

    while (plan_info->parent_root != NULL) {
        plan_info = plan_info->parent_root;
        parse = plan_info->parse;
        if (parse != NULL && parse->upsertClause != NULL) {
            return true;
        }
    }
    return false;
}

List* get_plan_list(Plan* plan)
{
    List* plan_list = NIL;

    switch (nodeTag(plan)) {
        case T_ModifyTable:
        case T_VecModifyTable: {
            ModifyTable* mt = (ModifyTable*)plan;

            Assert(mt->plans != NULL);
            plan_list = mt->plans;

            break;
        }
        case T_Append:
        case T_VecAppend: {
            Append* append = (Append*)plan;

            Assert(append->appendplans != NULL);
            plan_list = append->appendplans;

            break;
        }
        case T_MergeAppend:
        case T_VecMergeAppend: {
            MergeAppend* append = (MergeAppend*)plan;

            Assert(append->mergeplans != NULL);
            plan_list = append->mergeplans;

            break;
        }
        case T_BitmapAnd:
        case T_BitmapOr:
        case T_CStoreIndexAnd:
        case T_CStoreIndexOr: {
            BitmapAnd* ba = (BitmapAnd*)plan;

            Assert(ba->bitmapplans != NULL);
            plan_list = ba->bitmapplans;

            break;
        }
        default:
            /* do nothing */
            break;
    }

    return plan_list;
}

/*
 * @Description: find ForeignScan node in left-hand tree.
 *
 * @param[IN] plan :  the root of the sub plan tree
 * @return: Plan*: ForeignScan node found.
 */
Plan* get_foreign_scan(Plan* plan)
{
    Assert(plan);

    ereport(DEBUG5, (errmodule(MOD_ACCELERATE), errmsg("in %s", __FUNCTION__)));

    bool found = false;

    while (plan != NULL) {
        if (T_ForeignScan == nodeTag(plan) || T_VecForeignScan == nodeTag(plan)) {
            found = true;
            break;
        }

        plan = plan->lefttree;
    }

    if (!found) {
        ereport(ERROR,
            (errmodule(MOD_ACCELERATE), errcode(ERRCODE_NO_DATA_FOUND),
                errmsg("Fail to find ForeignScan node!"),
                errdetail("Result node type: %d", (int)nodeTag(plan)),
                errcause("System error."),
                erraction("Contact Huawei Engineer.")));
    }

    return plan;
}

static void check_redistribute_stream_walker(Plan* plan, void* context, const char* query_string)
{
    if (plan == NULL) {
        return;
    }
    FindStreamNodesForLoopContext *ctx = (FindStreamNodesForLoopContext*)context;
    if (IsA(plan, Stream) && ((Stream*)plan)->type == STREAM_REDISTRIBUTE) {
        ctx->has_redis_stream = true;
        return;
    }
    if (IsA(plan, Stream) && ((Stream*)plan)->type == STREAM_BROADCAST) {
        ctx->broadcast_stream_cnt++;
        return;
    }
    return PlanTreeWalker(plan, check_redistribute_stream_walker, context, NULL);
}

bool check_stream_for_loop_fetch(Portal portal)
{
    if (unlikely(portal == NULL || portal->cplan == NULL)) {
        return false;
    }
    bool has_stream = false;
    ListCell* lc = NULL;
    foreach(lc, portal->cplan->stmt_list) {
        if (has_stream)
            break;
        PlannedStmt* plannedstmt = (PlannedStmt*)lfirst(lc);
        FindStreamNodesForLoopContext context;
        /* only redistribute stream or more than one broadcast stream may cause hang in loop sql */
        if (IsA(plannedstmt, PlannedStmt)) {
            errno_t rc = 0;
            rc = memset_s(&context, sizeof(FindStreamNodesForLoopContext), 0, sizeof(FindStreamNodesForLoopContext));
            securec_check(rc, "\0", "\0");
            context.has_redis_stream = false;
            context.broadcast_stream_cnt = 0;
            check_redistribute_stream_walker(plannedstmt->planTree, &context, NULL);
            has_stream |= context.has_redis_stream;
            has_stream |= (context.broadcast_stream_cnt > 1);
        }
    }
    portal->hasStreamForPlpgsql = has_stream;
    return has_stream;
}

