/* -------------------------------------------------------------------------
 *
 * createplan.cpp
 *	  Routines to create the desired plan for processing a query.
 *	  Planning is complete, we just need to convert the selected
 *	  Path into a Plan.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/optimizer/plan/createplan.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "access/dfs/dfs_query.h"
#include "postgres.h"
#include "knl/knl_variable.h"

#include <limits.h>
#include <math.h>

#include "access/skey.h"
#include "access/transam.h"
#include "bulkload/foreignroutine.h"
#include "catalog/pg_class.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_opfamily.h"
#include "catalog/pgxc_group.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/plannodes.h"
#include "nodes/primnodes.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/dataskew.h"
#include "optimizer/nodegroups.h"
#include "optimizer/pgxcship.h"
#include "optimizer/paths.h"
#include "optimizer/pathnode.h"
#include "optimizer/placeholder.h"
#include "optimizer/plancat.h"
#include "optimizer/planmain.h"
#include "optimizer/planner.h"
#include "optimizer/predtest.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/subselect.h"
#include "optimizer/tlist.h"
#include "optimizer/var.h"
#include "parser/parse_clause.h"
#include "parser/parse_collate.h"
#include "parser/parse_expr.h"
#include "parser/parse_relation.h"
#include "parser/parsetree.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/selfuncs.h"
#include "workload/workload.h"
#ifdef PGXC
#include "optimizer/streamplan.h"
#include "optimizer/pgxcplan.h"
#include "pgxc/pgxc.h"
#include "pgxc/locator.h"
#endif /* PGXC */
#include "catalog/pg_operator.h"
#ifdef STREAMPLAN
#include "optimizer/streamplan.h"
#include "catalog/pg_aggregate.h"
#include "utils/syscache.h"
#include "parser/parse_oper.h"
#include "catalog/pg_proc.h"
#endif
#include "executor/node/nodeExtensible.h"

#define EQUALJOINVARRATIO ((2.0) / (3.0))

static Plan* create_plan_recurse(PlannerInfo* root, Path* best_path);
static Plan* create_scan_plan(PlannerInfo* root, Path* best_path);
static List* build_relation_tlist(RelOptInfo* rel);
static bool use_physical_tlist(PlannerInfo* root, RelOptInfo* rel);
static Plan* create_gating_plan(PlannerInfo* root, Plan* plan, List* quals);
static Plan* create_join_plan(PlannerInfo* root, JoinPath* best_path);
static Plan* create_append_plan(PlannerInfo* root, AppendPath* best_path);
static Plan* create_merge_append_plan(PlannerInfo* root, MergeAppendPath* best_path);
static BaseResult* create_result_plan(PlannerInfo* root, ResultPath* best_path);
static void adjust_scan_targetlist(ResultPath* best_path, Plan* subplan);
static Material* create_material_plan(PlannerInfo* root, MaterialPath* best_path);
static Plan* create_unique_plan(PlannerInfo* root, UniquePath* best_path);
static SeqScan* create_seqscan_plan(PlannerInfo* root, Path* best_path, List* tlist, List* scan_clauses);
static CStoreScan* create_cstorescan_plan(PlannerInfo* root, Path* best_path, List* tlist, List* scan_clauses);
static DfsScan* create_dfsscan_plan(PlannerInfo* root, Path* best_path, List* tlist, List* scan_clauses,
    bool indexFlag = false, List* excludedCol = NIL, bool indexOnly = false);
#ifdef ENABLE_MULTIPLE_NODES
static TsStoreScan* create_tsstorescan_plan(PlannerInfo* root, Path* best_path, List* tlist, List* scan_clauses);
#endif   /* ENABLE_MULTIPLE_NODES */
static Scan* create_indexscan_plan(
    PlannerInfo* root, IndexPath* best_path, List* tlist, List* scan_clauses, bool indexonly);
static BitmapHeapScan* create_bitmap_scan_plan(
    PlannerInfo* root, BitmapHeapPath* best_path, List* tlist, List* scan_clauses);
static Plan* create_bitmap_subplan(PlannerInfo* root, Path* bitmapqual, List** qual, List** indexqual, List** indexECs);
static TidScan* create_tidscan_plan(PlannerInfo* root, TidPath* best_path, List* tlist, List* scan_clauses);
static SubqueryScan* create_subqueryscan_plan(PlannerInfo* root, Path* best_path, List* tlist, List* scan_clauses);
static FunctionScan* create_functionscan_plan(PlannerInfo* root, Path* best_path, List* tlist, List* scan_clauses);
static ValuesScan* create_valuesscan_plan(PlannerInfo* root, Path* best_path, List* tlist, List* scan_clauses);
static Plan* create_ctescan_plan(PlannerInfo* root, Path* best_path, List* tlist, List* scan_clauses);
static WorkTableScan* create_worktablescan_plan(PlannerInfo* root, Path* best_path, List* tlist, List* scan_clauses);
static BaseResult *create_resultscan_plan(PlannerInfo *root, Path *best_path, List *tlist, List *scan_clauses);
static ExtensiblePlan* create_extensible_plan(
    PlannerInfo* root, ExtensiblePath* best_path, List* tlist, List* scan_clauses);
static ForeignScan* create_foreignscan_plan(PlannerInfo* root, ForeignPath* best_path, List* tlist, List* scan_clauses);
static NestLoop* create_nestloop_plan(PlannerInfo* root, NestPath* best_path, Plan* outer_plan, Plan* inner_plan);
static MergeJoin* create_mergejoin_plan(PlannerInfo* root, MergePath* best_path, Plan* outer_plan, Plan* inner_plan);
static HashJoin* create_hashjoin_plan(PlannerInfo* root, HashPath* best_path, Plan* outer_plan, Plan* inner_plan);
static Node* replace_nestloop_params(PlannerInfo* root, Node* expr);
static Node* replace_nestloop_params_mutator(Node* node, PlannerInfo* root);
static void process_subquery_nestloop_params(PlannerInfo *root, List *subplan_params);
static List* fix_indexqual_references(PlannerInfo* root, IndexPath* index_path);
static List* fix_indexorderby_references(PlannerInfo* root, IndexPath* index_path);
static Node* fix_indexqual_operand(Node* node, IndexOptInfo* index, int indexcol);
static List* get_switched_clauses(List* clauses, Relids outerrelids);
static List* order_qual_clauses(PlannerInfo* root, List* clauses);
static void copy_path_costsize(Plan* dest, Path* src);
static void copy_generic_path_info(Plan *dest, Path *src);
static SeqScan* make_seqscan(List* qptlist, List* qpqual, Index scanrelid);
static CStoreScan* make_cstorescan(List* qptlist, List* qpqual, Index scanrelid);
#ifdef ENABLE_MULTIPLE_NODES
static TsStoreScan* make_tsstorescan(List* qptlist, List* qpqual, Index scanrelid);
#endif   /* ENABLE_MULTIPLE_NODES */
static DfsScan* make_dfsscan(List* qptlist, List* qpqual, Index scanrelid, List* privateList, int dop);

static PartIterator* create_partIterator_plan(
    PlannerInfo* root, PartIteratorPath* pIterpath, GlobalPartIterator* gpIter);
static Plan* setPartitionParam(PlannerInfo* root, Plan* plan, RelOptInfo* rel);
static Plan* setBucketInfoParam(PlannerInfo* root, Plan* plan, RelOptInfo* rel);
Plan* create_globalpartInterator_plan(PlannerInfo* root, PartIteratorPath* pIterpath);

static IndexScan* make_indexscan(List* qptlist, List* qpqual, Index scanrelid, Oid indexid, List* indexqual,
    List* indexqualorig, List* indexorderby, List* indexorderbyorig, ScanDirection indexscandir);
static IndexOnlyScan* make_indexonlyscan(List* qptlist, List* qpqual, Index scanrelid, Oid indexid, List* indexqual,
    List* indexorderby, List* indextlist, ScanDirection indexscandir);
static CStoreIndexScan* make_cstoreindexscan(PlannerInfo* root, Path* best_path, List* qptlist, List* qpqual,
    Index scanrelid, Oid indexid, List* indexqual, List* indexqualorig, List* indexorderby, List* indexorderbyorig,
    List* indextlist, ScanDirection indexscandir, bool indexonly);
static DfsIndexScan* make_dfsindexscan(PlannerInfo* root, Path* best_path, List* qptlist, List* qpqual, Index scanrelid,
    Oid indexid, List* indexqual, List* indexqualorig, List* indexorderby, List* indexorderbyorig,
    IndexOptInfo* indexinfo, ScanDirection indexscandir, bool indexonly);
static BitmapIndexScan* make_bitmap_indexscan(Index scanrelid, Oid indexid, List* indexqual, List* indexqualorig);
static BitmapHeapScan* make_bitmap_heapscan(
    List* qptlist, List* qpqual, Plan* lefttree, List* bitmapqualorig, Index scanrelid);
static CStoreIndexCtidScan* make_cstoreindex_ctidscan(
    PlannerInfo* root, Index scanrelid, Oid indexid, List* indexqual, List* indexqualorig, List* indextlist);
static CStoreIndexHeapScan* make_cstoreindex_heapscan(PlannerInfo* root, Path* best_path, List* qptlist, List* qpqual,
    Plan* lefttree, List* bitmapqualorig, Index scanrelid);
static CStoreIndexAnd* make_cstoreindex_and(List* ctidplans);
static CStoreIndexOr* make_cstoreindex_or(List* ctidplans);
static TidScan* make_tidscan(List* qptlist, List* qpqual, Index scanrelid, List* tidquals);
static FunctionScan* make_functionscan(List* qptlist, List* qpqual, Index scanrelid, Node* funcexpr, List* funccolnames,
    List* funccoltypes, List* funccoltypmods, List* funccolcollations);
static ValuesScan* make_valuesscan(List* qptlist, List* qpqual, Index scanrelid, List* values_lists);
static CteScan* make_ctescan(List* qptlist, List* qpqual, Index scanrelid, int ctePlanId, int cteParam);
static WorkTableScan* make_worktablescan(List* qptlist, List* qpqual, Index scanrelid, int wtParam);
static BitmapAnd* make_bitmap_and(List* bitmapplans);
static BitmapOr* make_bitmap_or(List* bitmapplans);
static NestLoop* make_nestloop(List* tlist, List* joinclauses, List* otherclauses, List* nestParams, Plan* lefttree,
    Plan* righttree, JoinType jointype);
static HashJoin* make_hashjoin(List* tlist, List* joinclauses, List* otherclauses, List* hashclauses, Plan* lefttree,
    Plan* righttree, JoinType jointype);
static Hash* make_hash(
    Plan* lefttree, Oid skewTable, AttrNumber skewColumn, bool skewInherit, Oid skewColType, int32 skewColTypmod);
static MergeJoin* make_mergejoin(List* tlist, List* joinclauses, List* otherclauses, List* mergeclauses,
    Oid* mergefamilies, Oid* mergecollations, int* mergestrategies, bool* mergenullsfirst, Plan* lefttree,
    Plan* righttree, JoinType jointype);
static Plan* prepare_sort_from_pathkeys(PlannerInfo* root, Plan* lefttree, List* pathkeys, Relids relids,
    const AttrNumber* reqColIdx, bool adjust_tlist_in_place, int* p_numsortkeys, AttrNumber** p_sortColIdx,
    Oid** p_sortOperators, Oid** p_collations, bool** p_nullsFirst);
static EquivalenceMember* find_ec_member_for_tle(EquivalenceClass* ec, TargetEntry* tle, Relids relids);
static List* fix_cstore_scan_qual(PlannerInfo* root, List* qpqual);
static List* fix_dfs_index_target_list(
    PlannerInfo* root, Path* best_path, DfsIndexScan* node, IndexOptInfo* indexinfo, List* plan_qual);
static List* make_null_eq_clause(List* joinqual, List** otherqual, List* nullinfo);
static Node* get_null_eq_restrictinfo(Node* restrictinfo, List* nullinfo);

#ifdef STREAMPLAN
static List* add_agg_node_to_tlist(List* remote_tlist, Node* expr, Index ressortgroupref);
static List* process_agg_having_clause(
    PlannerInfo* root, List* remote_tlist, Node* havingQual, List** local_qual, bool* reduce_plan);
#endif

static bool isHoldUniqueOperator(OpExpr* expr);
static bool isHoldUniqueness(Node* node);

static List* build_one_column_tlist(PlannerInfo* root, RelOptInfo* rel);
static void min_max_optimization(PlannerInfo* root, CStoreScan* scan_plan);
static bool find_var_from_targetlist(Expr* expr, List* targetList);
static Plan* parallel_limit_sort(
    PlannerInfo* root, Plan* lefttree, Node* limitOffset, Node* limitCount, int64 offset_est, int64 count_est);
static void estimate_directHashjoin_Cost(
    PlannerInfo* root, List* hashclauses, Plan* outerPlan, Plan* hash_plan, HashJoin* join_plan);

static bool is_result_random(PlannerInfo* root, Plan* lefttree);

extern bool isSonicHashJoinEnable(HashJoin* hj);
extern bool isSonicHashAggEnable(VecAgg* agg);

extern void init_plan_cost(Plan* plan);

static PlanRowMark* check_lockrows_permission(PlannerInfo* root, Plan* lefttree);

/**
 * @Description: Whether the relation of relOptInfo is delta.
 * @in root, A PlannerInfo struct.
 * @in relOptInfo, A RelOptInfo struct.
 * @return If the relation is delta, return true, otherwise return false.
 */
static bool relIsDeltaNode(PlannerInfo* root, RelOptInfo* relOptInfo);

static void ModifyWorktableWtParam(Node* planNode, int oldWtParam, int newWtParam);

static bool ScanQualsViolateNotNullConstr(PlannerInfo* root, RelOptInfo* rel, Path* best_path);

#define SATISFY_INFORMATIONAL_CONSTRAINT(joinPlan, joinType)                                                    \
    (u_sess->attr.attr_sql.enable_constraint_optimization && true == innerPlan((joinPlan))->hasUniqueResults && \
        JOIN_SEMI != (joinType) && JOIN_ANTI != (joinType))

#define GetAllValueFrom 0
#define GetMinValueFromCu 1
#define GetMaxValueFromCu 2
#define GetMinAndMaxValueFromCu 3

FORCE_INLINE bool CanTransferInJoin(JoinType jointype)
{
    return (!(jointype == JOIN_LEFT || jointype == JOIN_FULL || jointype == JOIN_ANTI || jointype == JOIN_RIGHT_ANTI ||
              jointype == JOIN_LEFT_ANTI_FULL || jointype == JOIN_RIGHT_ANTI_FULL));
}

/*
 * set_plan_rows
 *     set plan node's rows and multiple from a global rows
 *
 * @param (in) plan:
 *     the plan node
 * @param (in) globalRows:
 *     the input global rows
 * @param (in) multiple:
 *     the input multiple
 *
 * @return: void
 */
void set_plan_rows(Plan* plan, double globalRows, double multiple)
{
    plan->multiple = multiple;

    /*
     * for global stats, We should reset global rows as localRows*u_sess->pgxc_cxt.NumDataNodes for replication except
     * RemoteQuery, because the local rows is equal to global rows.
     */
    if (is_replicated_plan(plan) && is_execute_on_datanodes(plan)) {
        plan->plan_rows = get_global_rows(globalRows, multiple, ng_get_dest_num_data_nodes(plan));
    } else {
        plan->plan_rows = globalRows;
    }
}

/*
 * set_plan_rows_from_plan
 *     set plan node's rows and multiple from a local rows
 *
 * @param (in) plan:
 *     the plan node
 * @param (in) localRows:
 *     the input local rows
 * @param (in) multiple:
 *     the input multiple
 *
 * @return: void
 */
void set_plan_rows_from_plan(Plan* plan, double localRows, double multiple)
{
    plan->multiple = multiple;
    plan->plan_rows = get_global_rows(localRows, plan->multiple, ng_get_dest_num_data_nodes(plan));
}

/*
 * create_plan
 *	  Creates the access plan for a query by recursively processing the
 *	  desired tree of pathnodes, starting at the node 'best_path'.	For
 *	  every pathnode found, we create a corresponding plan node containing
 *	  appropriate id, target list, and qualification information.
 *
 *	  The tlists and quals in the plan tree are still in planner format,
 *	  ie, Vars still correspond to the parser's numbering.  This will be
 *	  fixed later by setrefs.c.
 *
 *	  best_path is the best access path
 *
 *	  Returns a Plan tree.
 */
Plan* create_plan(PlannerInfo* root, Path* best_path)
{
    Plan* plan = NULL;

    /* plan_params should not be in use in current query level */
    Assert(root->plan_params == NIL);

    /* Initialize this module's private workspace in PlannerInfo */
    root->curOuterRels = NULL;
    root->curOuterParams = NIL;
    u_sess->opt_cxt.is_under_append_plan = false;

    /* Recursively process the path tree */
    plan = create_plan_recurse(root, best_path);

    /* Check we successfully assigned all NestLoopParams to plan nodes */
    if (root->curOuterParams != NIL)
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                (errmsg("failed to assign all NestLoopParams to plan nodes"))));
    /*
     * Reset plan_params to ensure param IDs used for nestloop params are not
     * re-used later
     */
    root->plan_params = NIL;

    /* Try to find the changed vars in chosed inlist2join path */
    if (u_sess->opt_cxt.qrw_inlist2join_optmode == QRW_INLIST2JOIN_CBO && root->var_mappings != NIL) {
        find_inlist2join_path(root, best_path);
    }

    return plan;
}

/*
 * create_plan_recurse
 *	  Recursive guts of create_plan().
 */
static Plan* create_plan_recurse(PlannerInfo* root, Path* best_path)
{
    Plan* plan = NULL;

    /* Guard against stack overflow due to overly complex plans */
    check_stack_depth();

    switch (best_path->pathtype) {
        case T_CStoreScan:
#ifdef ENABLE_MULTIPLE_NODES
        case T_TsStoreScan:
#endif   /* ENABLE_MULTIPLE_NODES */
        case T_IndexScan:
        case T_IndexOnlyScan:
        case T_SeqScan:
        case T_DfsScan:
        case T_BitmapHeapScan:
        case T_TidScan:
        case T_SubqueryScan:
        case T_FunctionScan:
        case T_ValuesScan:
        case T_CteScan:
        case T_WorkTableScan:
        case T_ForeignScan:
        case T_ExtensiblePlan:
            plan = create_scan_plan(root, best_path);
            break;
        case T_HashJoin:
        case T_MergeJoin:
        case T_NestLoop:
            plan = create_join_plan(root, (JoinPath*)best_path);
            break;
        case T_Append:
            plan = create_append_plan(root, (AppendPath*)best_path);
            break;
        case T_MergeAppend:
            plan = create_merge_append_plan(root, (MergeAppendPath*)best_path);
            break;
        case T_BaseResult:
            plan = (Plan*)create_result_plan(root, (ResultPath*)best_path);
            break;
        case T_Material:
            plan = (Plan*)create_material_plan(root, (MaterialPath*)best_path);
            break;
        case T_Unique:
            plan = create_unique_plan(root, (UniquePath*)best_path);
            break;
        case T_PartIterator:
            plan = (Plan*)create_globalpartInterator_plan(root, (PartIteratorPath*)best_path);
            break;
#ifdef PGXC
        case T_RemoteQuery:
            plan = create_remotequery_plan(root, (RemoteQueryPath*)best_path);
            break;
#endif
#ifdef STREAMPLAN
        case T_Stream:
            plan = create_stream_plan(root, (StreamPath*)best_path);
            break;
#endif
        default: {
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("unrecognized node type: %d", (int)best_path->pathtype)));

            plan = NULL; /* keep compiler quiet */
        } break;
    }
    AssertEreport(PointerIsValid(plan), MOD_OPT, "plan value is null");

    if (root->isPartIteratorPlanning) {
        plan->ispwj = true;
        plan->paramno = root->curIteratorParamIndex;
        plan->subparamno = root->curSubPartIteratorParamIndex;
    } else {
        plan->ispwj = false;
        plan->paramno = -1;
    }

    /*
     * Set smp info for Plan.
     * If the plan is on CN, we should not parallelize.
     */
    plan->dop = is_execute_on_datanodes(plan) ? SET_DOP(best_path->dop) : 1;

    return plan;
}

#ifdef STREAMPLAN
/*
 * create_stream_plan
 * The function creates a stream plan corresponding to the path passed in.
 * It should creates a plan statement which's serilized the left plan tree.
 * But the left plan tree is not stable for execution. So the serilizing
 * process is postponed to the Runtime.
 */
Plan* create_stream_plan(PlannerInfo* root, StreamPath* best_path)
{
    Stream* stream = NULL;
    Plan* subplan = NULL;
    Plan* plan = NULL;

    subplan = create_plan_recurse(root, best_path->subpath);

    if (is_execute_on_coordinator(subplan)) {
        return subplan;
    }

    /* return as cn gather plan when switch on */
    if (permit_gather(root) && IS_STREAM_TYPE(best_path, STREAM_GATHER)) {
        Plan* result_plan = make_simple_RemoteQuery(subplan, root, false);
        return result_plan;
    }

    /* We don't want any excess columns when streaming */
    disuse_physical_tlist(subplan, best_path->subpath);

    /*
     * If there has stream path for replication node,
     * we should not make stream plan node instead of using hash filter expr.
     * We may add local broadcast or local redistribute on replica table for parallelization.
     */
    if (is_replicated_plan(subplan) &&
        (best_path->smpDesc == NULL || best_path->smpDesc->distriType != LOCAL_BROADCAST)) {
        subplan->total_cost = best_path->path.total_cost;
        subplan->distributed_keys = best_path->path.distribute_keys;

        /*
         * Add hashfilter qual for replication if some node type which
         * unsupport stream and add hashfilter success.
         */
        if (best_path->type == STREAM_REDISTRIBUTE &&
            add_hashfilter_for_replication(root, subplan, subplan->distributed_keys)) {
            /*
             * If the replicated table's nodegroup is different from the computing nodegroup,
             * we still need to add the stream plan node (shuffle it to target node group)
             * after we added hash filter expression.
             */
            bool needs_shuffle =
                ng_is_shuffle_needed(root, best_path->subpath, ng_get_dest_distribution((Path*)best_path));
            if (!needs_shuffle &&
                (best_path->smpDesc == NULL || best_path->smpDesc->distriType != REMOTE_SPLIT_DISTRIBUTE)) {
                return subplan;
            }
        } else {
            /*
             * It should add stream node for some node type which unsupport
             * hashfilter node type and add hashfilter fail for replication.
             */
            ExecNodes* exec_nodes = get_random_data_nodes(LOCATOR_TYPE_REPLICATED, subplan);
            pushdown_execnodes(subplan, exec_nodes);
        }
    }

    stream = makeNode(Stream);

    stream->type = best_path->type;
    stream->distribute_keys = best_path->path.distribute_keys;

    stream->is_sorted = false;
    stream->sort = NULL;
    stream->smpDesc.consumerDop = 1;
    stream->smpDesc.producerDop = subplan->dop;
#ifdef ENABLE_MULTIPLE_NODES    
    stream->smpDesc.distriType = REMOTE_DISTRIBUTE;
#else
    stream->smpDesc.distriType = LOCAL_DISTRIBUTE;
#endif
    stream->origin_consumer_nodes = NULL;

    plan = &stream->scan.plan;
    /* Copy the smpDesc from path */
    if (best_path->smpDesc) {
        stream->smpDesc.consumerDop = best_path->smpDesc->consumerDop > 1 ? best_path->smpDesc->consumerDop : 1;
        stream->smpDesc.producerDop = best_path->smpDesc->producerDop > 1 ? best_path->smpDesc->producerDop : 1;
        plan->dop = stream->smpDesc.consumerDop;
        stream->smpDesc.distriType = best_path->smpDesc->distriType;

        /*
         * Local roundrobin and local broadcast do not need distribute keys,
         * so we set to NIL, in case it add extra targetlist.
         */
        if (stream->smpDesc.distriType == LOCAL_ROUNDROBIN || stream->smpDesc.distriType == LOCAL_BROADCAST) {
            stream->distribute_keys = NIL;
        }
    }

#ifndef ENABLE_MULTIPLE_NODES
    /* if number of producer and consumer is 1, means stream node is not needed */
    if (stream->smpDesc.consumerDop == 1 && stream->smpDesc.producerDop == 1) {
        pfree(stream);
        return subplan;
    }

    /* single node mode only has LOCAL Stream operator */
    if (stream->type == STREAM_BROADCAST) {
        stream->smpDesc.distriType = LOCAL_BROADCAST;
    }
#endif

    /*
     * Local stream's distribute key can be NIL,
     * but we should set plan distribute key.
     */
    plan->distributed_keys = best_path->path.distribute_keys;
    plan->targetlist = subplan->targetlist;
    plan->lefttree = subplan;
    plan->righttree = NULL;
    plan->exec_nodes = (ExecNodes*)copyObject(ng_get_dest_execnodes(subplan));
    /* If we need to add split redistribute on replicate table. */
    if (list_length(plan->exec_nodes->nodeList) == 0) {
        plan->exec_nodes->nodeList = GetAllDataNodes();
        elog(DEBUG1, "[create_stream_plan] nodelist is empty");
    }
    plan->exec_nodes->baselocatortype = best_path->path.locator_type;
    plan->hasUniqueResults = subplan->hasUniqueResults;

    /* Assign consumer_nodes to StreamNode */
    if (STREAM_IS_LOCAL_NODE(stream->smpDesc.distriType)) {
        stream->consumer_nodes = (ExecNodes*)copyObject(plan->exec_nodes);
    } else {
        stream->consumer_nodes = ng_convert_to_exec_nodes(
            &best_path->consumer_distribution, best_path->path.locator_type, RELATION_ACCESS_READ);
    }

    copy_path_costsize(plan, &(best_path->path));
    /* Confirm if the distribute keys in targetlist. */
    stream->distribute_keys = confirm_distribute_key(root, plan, stream->distribute_keys);

    stream->skew_list = best_path->skew_list;

    if (best_path->path.pathkeys) {
        return (Plan*)make_sort_from_pathkeys(root, plan, best_path->path.pathkeys, -1.0);
    } else {
        return (Plan*)stream;
    }
}
#endif

/*
 * create_scan_plan
 *	 Create a scan plan for the parent relation of 'best_path'.
 */
static Plan* create_scan_plan(PlannerInfo* root, Path* best_path)
{
    RelOptInfo* rel = best_path->parent;
    List* tlist = NIL;
    List* scan_clauses = NIL;
    Plan* plan = NULL;

    /*
     * If planning is uner recursive CTE, we need check if we are going to generate
     * path that not supported with current Recursive-Execution mode.
     */
    if (STREAM_RECURSIVECTE_SUPPORTED && root->is_under_recursive_cte) {
        if (best_path->pathtype == T_DfsScan || best_path->pathtype == T_DfsIndexScan ||
            best_path->pathtype == T_ForeignScan) {
            errno_t sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
                NOTPLANSHIPPING_LENGTH,
                "RecursiveUnion contains DFSScan or ForeignScan is not shippable");
            securec_check_ss_c(sprintf_rc, "\0", "\0");

            mark_stream_unsupport();
        }
    }

    /*
     * For table scans, rather than using the relation targetlist (which is
     * only those Vars actually needed by the query), we prefer to generate a
     * tlist containing all Vars in order.	This will allow the executor to
     * optimize away projection of the table tuples, if possible.  (Note that
     * planner.c may replace the tlist we generate here, forcing projection to
     * occur.)
     */
    if (use_physical_tlist(root, rel) && best_path->pathtype != T_ForeignScan) {
        if (best_path->pathtype == T_IndexOnlyScan) {
            /* For index-only scan, the preferred tlist is the index's */
            tlist = (List*)copyObject(((IndexPath*)best_path)->indexinfo->indextlist);
        } else {
            tlist = build_physical_tlist(root, rel);
            /* if fail because of dropped cols, use regular method */
            if (tlist == NIL) {
                tlist = build_relation_tlist(rel);
            }
        }
    } else {
        tlist = build_relation_tlist(rel);
        /*
         * If it's a parameterized otherrel, there might be lateral references
         * in the tlist, which need to be replaced with Params.  This cannot
         * happen for regular baserels, though.  Note use_physical_tlist()
         * always fails for otherrels, so we don't need to check this above.
         */
        if (rel->reloptkind != RELOPT_BASEREL && best_path->param_info)
            tlist = (List *) replace_nestloop_params(root, (Node *) tlist);
    }

    /*
     * Extract the relevant restriction clauses from the parent relation. The
     * executor must apply all these restrictions during the scan, except for
     * pseudoconstants which we'll take care of below.
     */
    scan_clauses = rel->baserestrictinfo;

    /*
     * If this is a parameterized scan, we also need to enforce all the join
     * clauses available from the outer relation(s).
     *
     * For paranoia's sake, don't modify the stored baserestrictinfo list.
     */
    if (best_path->param_info) {
        scan_clauses = list_concat(list_copy(scan_clauses), best_path->param_info->ppi_clauses);
    }

    switch (best_path->pathtype) {
        case T_SeqScan:
            plan = (Plan*)create_seqscan_plan(root, best_path, tlist, scan_clauses);
            break;
        case T_CStoreScan:
            plan = (Plan*)create_cstorescan_plan(root, best_path, tlist, scan_clauses);
            break;
#ifdef ENABLE_MULTIPLE_NODES
        case T_TsStoreScan:
            plan = (Plan*)create_tsstorescan_plan(root, best_path, tlist, scan_clauses);
            break;
#endif   /* ENABLE_MULTIPLE_NODES */
        case T_DfsScan:
            plan = (Plan*)create_dfsscan_plan(root, best_path, tlist, scan_clauses);
            break;
        case T_IndexScan:
            if (SUBQUERY_IS_PARAM(root) && PATH_REQ_UPPER(best_path) != NULL) {
                scan_clauses = list_concat(scan_clauses, rel->subplanrestrictinfo);
            }

            plan = (Plan*)create_indexscan_plan(root, (IndexPath*)best_path, tlist, scan_clauses, false);
            break;

        case T_IndexOnlyScan:
            if (SUBQUERY_IS_PARAM(root) && PATH_REQ_UPPER(best_path) != NULL) {
                scan_clauses = list_concat(scan_clauses, rel->subplanrestrictinfo);
            }

            plan = (Plan*)create_indexscan_plan(root, (IndexPath*)best_path, tlist, scan_clauses, true);
            break;

        case T_BitmapHeapScan:
            if (SUBQUERY_IS_PARAM(root) && PATH_REQ_UPPER(best_path) != NULL) {
                scan_clauses = list_concat(scan_clauses, rel->subplanrestrictinfo);
            }

            plan = (Plan*)create_bitmap_scan_plan(root, (BitmapHeapPath*)best_path, tlist, scan_clauses);
            break;

        case T_TidScan:
            plan = (Plan*)create_tidscan_plan(root, (TidPath*)best_path, tlist, scan_clauses);
            break;

        case T_SubqueryScan:
            plan = (Plan*)create_subqueryscan_plan(root, best_path, tlist, scan_clauses);
            break;

        case T_FunctionScan:
            plan = (Plan*)create_functionscan_plan(root, best_path, tlist, scan_clauses);
            break;

        case T_ValuesScan:
            plan = (Plan*)create_valuesscan_plan(root, best_path, tlist, scan_clauses);
            break;

        case T_CteScan:
            plan = (Plan*)create_ctescan_plan(root, best_path, tlist, scan_clauses);
            break;

        case T_BaseResult:
            plan = (Plan *) create_resultscan_plan(root, best_path, tlist, scan_clauses);
            break;

        case T_WorkTableScan:
            plan = (Plan*)create_worktablescan_plan(root, best_path, tlist, scan_clauses);
            break;

        case T_ExtensiblePlan:
            plan = (Plan*)create_extensible_plan(root, (ExtensiblePath*)best_path, tlist, scan_clauses);
            break;

        case T_ForeignScan:
            plan = (Plan*)create_foreignscan_plan(root, (ForeignPath*)best_path, tlist, scan_clauses);
            break;

        default: {
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("unrecognized node type: %d", (int)best_path->pathtype)));
            plan = NULL; /* keep compiler quiet */
        } break;
    }

    /* update IO cost */
    if (ENABLE_WORKLOAD_CONTROL && plan != NULL)
        WLMmonitor_check_and_update_IOCost(root, best_path->pathtype, plan->total_cost);

    if (!CheckPathUseGlobalPartIndex(best_path)) {
        (void*)setPartitionParam(root, plan, best_path->parent);
    }
    (void*)setBucketInfoParam(root, plan, best_path->parent);

    /*
     * If there are any pseudoconstant clauses attached to this node, insert a
     * gating Result node that evaluates the pseudoconstants as one-time
     * quals.
     */
    if (root->hasPseudoConstantQuals) {
        plan = create_gating_plan(root, plan, scan_clauses);
    } else if (ScanQualsViolateNotNullConstr(root, rel, best_path)) {
        /*
        * If there is IS NULL qual on a known NOT-NULL attribute, insert a Result node to prevent needless execution.
        */
        plan = (Plan*)make_result(root, plan->targetlist, (Node*)list_make1(makeBoolConst(false, false)), plan);
    }

    return plan;
}

static bool IsScanPath(NodeTag type)
{
    return (
        type == T_CStoreScan || type == T_CStoreIndexScan || type == T_CStoreIndexHeapScan || type == T_SeqScan ||
        type == T_DfsScan || type == T_IndexScan || type == T_IndexOnlyScan || type == T_BitmapHeapScan
    );
}

static bool ScanQualsViolateNotNullConstr(PlannerInfo* root, RelOptInfo* rel, Path* best_path)
{
    /* For now, we only support table scan optimization */
    if (rel->rtekind != RTE_RELATION || !IsScanPath(best_path->pathtype)) {
        return false;
    }

    List* scan_clauses = rel->baserestrictinfo;
    ListCell* lc = NULL;
    foreach (lc, scan_clauses) {
        Node* clause = (Node*)lfirst(lc);
        if (IsA(clause, RestrictInfo) && IsA(((RestrictInfo*)clause)->clause, NullTest)) {
            NullTest* expr = (NullTest*)((RestrictInfo*)clause)->clause;
            /* For attribute with NOT-NULL constraint, IS-NULL expression can be short-circuited */
            if (expr->nulltesttype != IS_NULL || !IsA(expr->arg, Var)) {
                continue;
            }
            if (check_var_nonnullable(root->parse, (Node*)expr->arg)) {
                return true;
            }
        }
    }
    return false;
}

/*
 * Build a target list (ie, a list of TargetEntry) for a relation.
 */
static List* build_relation_tlist(RelOptInfo* rel)
{
    List* tlist = NIL;
    int resno = 1;
    ListCell* v = NULL;

    /* For alternative rels, we use its base rel to generate targetlist in Path stage */
    if (rel->base_rel != NULL) {
        Assert(u_sess->opt_cxt.qrw_inlist2join_optmode > QRW_INLIST2JOIN_DISABLE);
        Assert((rel->reloptkind == RELOPT_OTHER_MEMBER_REL ||
                rel->reloptkind == RELOPT_BASEREL) &&
               rel->rtekind == RTE_SUBQUERY);
        rel = rel->base_rel;
    }

    foreach (v, rel->reltargetlist) {
        /* Do we really need to copy here?	Not sure */
        Node* node = (Node*)copyObject(lfirst(v));

        /* Don't lose the resjunk info */
        bool resjunk = false;
        if (((Var *)node)->varattno == 0 && ((Var *)node)->varno > 0) {
            resjunk = true;
        }
        tlist = lappend(tlist, makeTargetEntry((Expr*)node, resno, NULL, resjunk));
        resno++;
    }
    return tlist;
}

/*
 * use_physical_tlist
 *		Decide whether to use a tlist matching relation structure,
 *		rather than only those Vars actually referenced.
 */
static bool use_physical_tlist(PlannerInfo* root, RelOptInfo* rel)
{
    int i;
    ListCell* lc = NULL;

    /*
     * We can do this for real relation scans, subquery scans, function scans,
     * values scans, and CTE scans (but not for, eg, joins).
     */
    if (rel->rtekind != RTE_RELATION && rel->rtekind != RTE_SUBQUERY && rel->rtekind != RTE_FUNCTION &&
        rel->rtekind != RTE_VALUES && rel->rtekind != RTE_CTE)
        return false;

    /* In case of cost-based rewrite we can not use physical scan */
    if (rel->base_rel != NULL) {
        return false;
    }

    /*
     * Can't do it with inheritance cases either (mainly because Append
     * doesn't project).
     */
    if (rel->reloptkind != RELOPT_BASEREL)
        return false;

    /*
     * Can't do it if any system columns or whole-row Vars are requested.
     * (This could possibly be fixed but would take some fragile assumptions
     * in setrefs.c, I think.)
     */
    for (i = rel->min_attr; i <= 0; i++) {
        if (!bms_is_empty(rel->attr_needed[i - rel->min_attr])) {
            return false;
        }
    }

    /*
     * Can't do it if the rel is required to emit any placeholder expressions,
     * either.
     */
    foreach (lc, root->placeholder_list) {
        PlaceHolderInfo* phinfo = (PlaceHolderInfo*)lfirst(lc);

        if (bms_nonempty_difference(phinfo->ph_needed, rel->relids) && bms_is_subset(phinfo->ph_eval_at, rel->relids)) {
            return false;
        }
    }

    return true;
}

/*
 * @Description:
 * 		In some cases, the path do not create plan and just return the subplan.
 * 		When we create unique or stream plan from path, there is possibility
 * 		to occur this situation.
 *
 * @param[IN] plan: the plan need to compare
 * @param[IN] path: the path need to be check
 *
 * @return Path* : return the real path relate to the plan.
 */
static Path* wipe_dummy_path(Plan* plan, Path* path)
{
    while (nodeTag(plan) != path->pathtype && (IsA(path, UniquePath) || IsA(path, StreamPath))) {
        /* If the under is unique index scan, then the unique path is dummy. */
        if (IsA(path, UniquePath)) {
            UniquePath* up = (UniquePath*)path;
            path = up->subpath;
        } else if (IsA(path, StreamPath)) { /* If add hashfilter for replication, then the stream path is dummy */
            StreamPath* sp = (StreamPath*)path;
            path = sp->subpath;
        }
    }

    Assert(nodeTag(plan) == path->pathtype);
    return path;
}

/*
 * disuse_physical_tlist
 *		Switch a plan node back to emitting only Vars actually referenced.
 *
 * If the plan node immediately above a scan would prefer to get only
 * needed Vars and not a physical tlist, it must call this routine to
 * undo the decision made by use_physical_tlist().	Currently, Hash, Sort,
 * and Material nodes want this, so they don't have to store useless columns.
 */
void disuse_physical_tlist(Plan* plan, Path* path)
{
#ifndef ENABLE_MULTIPLE_NODES
    /* StreamPath may not create stream plan, but it already do this step, so just return. */
    if (IsA(path, StreamPath)) {
        return;
    }
#endif

    /* Only need to undo it for path types handled by create_scan_plan() */
    switch (nodeTag(plan)) {
        case T_SubqueryScan: {
            /* For converted SubqueryScan path we keep the tlist not changed */
            if (path->parent != NULL && path->parent->base_rel != NULL) {
                /* Assert the func-invocation context */
                Assert(u_sess->opt_cxt.qrw_inlist2join_optmode > QRW_INLIST2JOIN_DISABLE);
                Assert((path->parent->reloptkind == RELOPT_OTHER_MEMBER_REL ||
                        path->parent->reloptkind == RELOPT_BASEREL) &&
                       path->parent->rtekind == RTE_SUBQUERY);
                break;
            }
            /* Stream plan would be reduced when it's executed on coordinator, while path wouldn't. */
            if (is_execute_on_coordinator(plan) && IsA(path, StreamPath)) {
                path = ((StreamPath*)path)->subpath;
            }
        }
        /* fall throuth */
        case T_SeqScan:
        case T_IndexScan:
        case T_IndexOnlyScan:
        case T_BitmapHeapScan:
        case T_CStoreIndexHeapScan:
        case T_TidScan:
        case T_FunctionScan:
        case T_ValuesScan:
        case T_WorkTableScan:
        case T_CteScan:
        case T_ForeignScan:
        case T_ExtensiblePlan:
        case T_BaseResult: {
            if (IsA(plan, WorkTableScan) && (((WorkTableScan *)plan)->forStartWith)) {
                /* In StartWith-ConnectBy cases we keep the whole worktable scan results
                 * for the upcoming processing steps. */
                break;
            }
            List* tarList = NULL;
            if (path->parent != NULL)
                tarList = build_relation_tlist(path->parent);
            if (tarList != NULL)
                plan->targetlist = tarList;
            break;
        }
        case T_PartIterator: {
            path = wipe_dummy_path(plan, path);
            PartIteratorPath* piPath = (PartIteratorPath*)path;
            Plan* subPlan = plan->lefttree;
            switch (piPath->subPath->pathtype) {
                case T_SeqScan:
                case T_IndexScan:
                case T_IndexOnlyScan:
                case T_BitmapHeapScan:
                case T_CStoreIndexHeapScan:
                case T_TidScan: {
                    List* sub_parList = build_relation_tlist(piPath->subPath->parent);
                    if (sub_parList != NULL) {
                        subPlan->targetlist = sub_parList;
                        plan->targetlist = sub_parList;
                    }
                    break;
                }
                default:
                    break;
            }
        } break;
        default:
            break;
    }
}

/*
 * create_gating_plan
 *	  Deal with pseudoconstant qual clauses
 *
 * If the node's quals list includes any pseudoconstant quals, put them
 * into a gating Result node atop the already-built plan.  Otherwise,
 * return the plan as-is.
 *
 * Note that we don't change cost or size estimates when doing gating.
 * The costs of qual eval were already folded into the plan's startup cost.
 * Leaving the size alone amounts to assuming that the gating qual will
 * succeed, which is the conservative estimate for planning upper queries.
 * We certainly don't want to assume the output size is zero (unless the
 * gating qual is actually constant FALSE, and that case is dealt with in
 * clausesel.c).  Interpolating between the two cases is silly, because
 * it doesn't reflect what will really happen at runtime, and besides which
 * in most cases we have only a very bad idea of the probability of the gating
 * qual being true.
 */
static Plan* create_gating_plan(PlannerInfo* root, Plan* plan, List* quals)
{
    List* pseudoconstants = NIL;

    /* Sort into desirable execution order while still in RestrictInfo form */
    quals = order_qual_clauses(root, quals);

    /* Pull out any pseudoconstant quals from the RestrictInfo list */
    pseudoconstants = extract_actual_clauses(quals, true);

    if (pseudoconstants == NIL) {
        return plan;
    }

    return (Plan*)make_result(root, plan->targetlist, (Node*)pseudoconstants, plan);
}

/* If inner plan or outer plan exec on CN and other side has add stream or hashfilter for replication join,
 * it should delete Stream node or delete qual If plan->qual is T_HashFilter.
 */
static void reduce_stream_plan(
    PlannerInfo* root, JoinPath* best_path, Plan** outer_plan, Plan** inner_plan, Relids saveOuterRels)
{
    Relids saveOuterRelsOld = root->curOuterRels;

    if (is_execute_on_datanodes(*inner_plan) || is_execute_on_datanodes(*outer_plan)) {
        Path* joinpath = NULL;

        if (is_execute_on_coordinator(*inner_plan)) {
            joinpath = best_path->outerjoinpath;
        } else if (is_execute_on_coordinator(*outer_plan)) {
            joinpath = best_path->innerjoinpath;
        }

        if (joinpath != NULL) {
            if (IsA(joinpath, MaterialPath)) {
                joinpath = ((MaterialPath*)joinpath)->subpath;
            }

            /* Find stream node and recreate plan. */
            if (IsA(joinpath, StreamPath)) {
                if (is_execute_on_coordinator(*inner_plan)) {
                    /* curOuterRels is used by inner for nestloop, it is unused for outer. */
                    root->curOuterRels = saveOuterRels;
                    best_path->outerjoinpath = ((StreamPath*)joinpath)->subpath;
                    *outer_plan = create_plan_recurse(root, best_path->outerjoinpath);
                } else if (is_execute_on_coordinator(*outer_plan)) {
                    best_path->innerjoinpath = ((StreamPath*)joinpath)->subpath;
                    *inner_plan = create_plan_recurse(root, best_path->innerjoinpath);
                }
            }
        }
    }

    root->curOuterRels = saveOuterRelsOld;
}

/*
 * create_join_plan
 *	  Create a join plan for 'best_path' and (recursively) plans for its
 *	  inner and outer paths.
 */
static Plan* create_join_plan(PlannerInfo* root, JoinPath* best_path)
{
    Plan* outer_plan = NULL;
    Plan* inner_plan = NULL;
    Plan* plan = NULL;
    Relids saveOuterRels = root->curOuterRels;

    outer_plan = create_plan_recurse(root, best_path->outerjoinpath);

    /* For a nestloop, include outer relids in curOuterRels for inner side */
    if (best_path->path.pathtype == T_NestLoop)
        root->curOuterRels = bms_union(root->curOuterRels, best_path->outerjoinpath->parent->relids);

    inner_plan = create_plan_recurse(root, best_path->innerjoinpath);

    reduce_stream_plan(root, best_path, &outer_plan, &inner_plan, saveOuterRels);

    switch (best_path->path.pathtype) {
        case T_MergeJoin:
            plan = (Plan*)create_mergejoin_plan(root, (MergePath*)best_path, outer_plan, inner_plan);
            break;
        case T_HashJoin:
            plan = (Plan*)create_hashjoin_plan(root, (HashPath*)best_path, outer_plan, inner_plan);
            break;
        case T_NestLoop:
            /* Restore curOuterRels */
            bms_free_ext(root->curOuterRels);
            root->curOuterRels = saveOuterRels;

            plan = (Plan*)create_nestloop_plan(root, (NestPath*)best_path, outer_plan, inner_plan);
            break;
        default: {
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("unrecognized node type: %d", (int)best_path->path.pathtype)));
            plan = NULL; /* keep compiler quiet */
        } break;
    }

    if (root->isPartIteratorPlanning && PointerIsValid(plan)) {
        plan->ispwj = true;
    }

#ifdef STREAMPLAN
    if (IS_STREAM_PLAN) {
        stream_join_plan(root, plan, best_path);
    } else {
        plan->exec_type = EXEC_ON_COORDS;
        plan->exec_nodes = ng_get_single_node_group_exec_node();
        plan->distributed_keys = NIL;
    }
#endif

    ((Join*)plan)->skewoptimize = best_path->skewoptimize;

    /* After setting exec_nodes, we can safely copy cardinality info from path */
    copy_path_costsize(plan, &best_path->path);

    /*
     * If there are any pseudoconstant clauses attached to this node, insert a
     * gating Result node that evaluates the pseudoconstants as one-time
     * quals.
     */
    if (root->hasPseudoConstantQuals)
        plan = create_gating_plan(root, plan, best_path->joinrestrictinfo);

#ifdef NOT_USED

    /*
     * * Expensive function pullups may have pulled local predicates * into
     * this path node.	Put them in the qpqual of the plan node. * JMH,
     * 6/15/92
     */
    if (get_loc_restrictinfo(best_path) != NIL)
        set_qpqual(
            (Plan)plan, list_concat(get_qpqual((Plan)plan), get_actual_clauses(get_loc_restrictinfo(best_path))));
#endif

    return plan;
}

/*
 * create_append_plan
 *	  Create an Append plan for 'best_path' and (recursively) plans
 *	  for its subpaths.
 *
 *	  Returns a Plan node.
 */
static Plan* create_append_plan(PlannerInfo* root, AppendPath* best_path)
{
    Append* plan = NULL;
    List* tlist = build_relation_tlist(best_path->path.parent);
    List* subplans = NIL;
    ListCell* subpaths = NULL;

    if (tlist == NIL) {
        Const* c = make_const(NULL, makeString("Dummy"), 0);
        tlist = lappend(tlist, makeTargetEntry((Expr*)c, 1, NULL, true));
    }

    /*
     * It is possible for the subplans list to contain only one entry, or even
     * no entries.	Handle these cases specially.
     *
     * XXX ideally, if there's just one entry, we'd not bother to generate an
     * Append node but just return the single child.  At the moment this does
     * not work because the varno of the child scan plan won't match the
     * parent-rel Vars it'll be asked to emit.
     */
    if (best_path->subpaths == NIL) {
        /* Generate a Result plan with constant-FALSE gating qual */
        return (Plan*)make_result(root, tlist, (Node*)list_make1(makeBoolConst(false, false)), NULL);
    }

    /* Normal case with multiple subpaths */
    foreach (subpaths, best_path->subpaths) {
        Path* subpath = (Path*)lfirst(subpaths);
        u_sess->opt_cxt.is_under_append_plan = true;
        subplans = lappend(subplans, create_plan_recurse(root, subpath));
        u_sess->opt_cxt.is_under_append_plan = false;
    }

    plan = make_append(subplans, tlist);

    /* For hdfs append rel, we set plan rows according to hint value previous applied */
    if (best_path->path.parent->rtekind == RTE_RELATION) {
        plan->plan.plan_rows = best_path->path.rows;
    }

    if (IS_STREAM_PLAN) {
        mark_distribute_setop(root, (Node*)plan, true, best_path->path.distribute_keys == NIL);
#ifdef ENABLE_MULTIPLE_NODES
        /*
         * If we have got distribute keys from best_path, we can not change it any more.
         * But if the diskeys in best_path is not in targetlist, that means the diskeys
         * is redundant, so skip the check of this scene.
         */
        if (best_path->path.distribute_keys != NIL &&
            distributeKeyIndex(root, best_path->path.distribute_keys, tlist) != NIL)
            AssertEreport(equal_distributekey(root, best_path->path.distribute_keys, plan->plan.distributed_keys),
                MOD_OPT_SETOP,
                "Distribute keys error when create append plan.");
#endif
    }

    /*
     * If there are any pseudoconstant clauses attached to this node, insert a
     * gating Result node that evaluates the pseudoconstants as one-time
     * quals.
     */
    if (root->hasPseudoConstantQuals) {
        return create_gating_plan(root, (Plan*)plan, best_path->path.parent->baserestrictinfo);
    }

    return (Plan*)plan;
}

/*
 * create_merge_append_plan
 *	  Create a MergeAppend plan for 'best_path' and (recursively) plans
 *	  for its subpaths.
 *
 *	  Returns a Plan node.
 */
static Plan* create_merge_append_plan(PlannerInfo* root, MergeAppendPath* best_path)
{
    MergeAppend* node = makeNode(MergeAppend);
    Plan* plan = &node->plan;
    List* tlist = build_relation_tlist(best_path->path.parent);
    List* pathkeys = best_path->path.pathkeys;
    List* subplans = NIL;
    ListCell* subpaths = NULL;

    /*
     * We don't have the actual creation of the MergeAppend node split out
     * into a separate make_xxx function.  This is because we want to run
     * prepare_sort_from_pathkeys on it before we do so on the individual
     * child plans, to make cross-checking the sort info easier.
     */
    plan->targetlist = tlist;
    plan->qual = NIL;
    plan->lefttree = NULL;
    plan->righttree = NULL;

    /* Compute sort column info, and adjust MergeAppend's tlist as needed */
    (void)prepare_sort_from_pathkeys(root,
        plan,
        pathkeys,
        NULL,
        NULL,
        true,
        &node->numCols,
        &node->sortColIdx,
        &node->sortOperators,
        &node->collations,
        &node->nullsFirst);

    plan->exec_nodes = NULL;

    /*
     * Now prepare the child plans.  We must apply prepare_sort_from_pathkeys
     * even to subplans that don't need an explicit sort, to make sure they
     * are returning the same sort key columns the MergeAppend expects.
     */
    int i = 0;
    foreach (subpaths, best_path->subpaths) {
        Path* subpath = (Path*)lfirst(subpaths);
        Plan* subplan = NULL;
        int numsortkeys = 0;
        AttrNumber* sortColIdx = NULL;
        Oid* sortOperators = NULL;
        Oid* collations = NULL;
        bool* nullsFirst = NULL;
        u_sess->opt_cxt.is_under_append_plan = true;
        /* Build the child plan */
        subplan = create_plan_recurse(root, subpath);
        u_sess->opt_cxt.is_under_append_plan = false;
        /* Compute sort column info, and adjust subplan's tlist as needed */
        subplan = prepare_sort_from_pathkeys(root,
            subplan,
            pathkeys,
            subpath->parent->relids,
            node->sortColIdx,
            false,
            &numsortkeys,
            &sortColIdx,
            &sortOperators,
            &collations,
            &nullsFirst);

        /*
         * Check that we got the same sort key information.  We just Assert
         * that the sortops match, since those depend only on the pathkeys;
         * but it seems like a good idea to check the sort column numbers
         * explicitly, to ensure the tlists really do match up.
         */
        Assert(numsortkeys == node->numCols);
        if (memcmp(sortColIdx, node->sortColIdx, numsortkeys * sizeof(AttrNumber)) != 0)
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                    (errmsg("MergeAppend child's targetlist doesn't match MergeAppend"))));
        Assert(memcmp(sortOperators, node->sortOperators, numsortkeys * sizeof(Oid)) == 0);
        Assert(memcmp(collations, node->collations, numsortkeys * sizeof(Oid)) == 0);
        Assert(memcmp(nullsFirst, node->nullsFirst, numsortkeys * sizeof(bool)) == 0);

        /* Now, insert a Sort node if subplan isn't sufficiently ordered */
        if (!pathkeys_contained_in(pathkeys, subpath->pathkeys)) {
            subplan = (Plan*)make_sort(
                root, subplan, numsortkeys, sortColIdx, sortOperators, collations, nullsFirst, best_path->limit_tuples);
            copy_mem_info(&((Sort*)subplan)->mem_info, &best_path->mem_info[i]);
        }

        subplans = lappend(subplans, subplan);

        ExecNodes* subplan_exec_nodes = ng_get_dest_execnodes(subplan);
        if (plan->exec_nodes == NULL) {
            plan->exec_nodes = (ExecNodes*)copyObject(subplan_exec_nodes);
        } else if (!ng_is_same_group(&plan->exec_nodes->distribution, &subplan_exec_nodes->distribution)) {
            plan->exec_nodes->distribution.group_oid = InvalidOid;
        }
        i++;
    }

    node->mergeplans = subplans;

#ifdef STREAMPLAN
    if (IS_STREAM_PLAN) {
        mark_distribute_setop(root, (Node*)node, true, best_path->path.distribute_keys == NIL);

        /*
         * If we have got distribute keys from best_path, we can not change it any more.
         * But if the diskeys in best_path is not in targetlist, that means the diskeys
         * is redundant, so skip the check of this scene.
         */
        if (best_path->path.distribute_keys != NIL &&
            distributeKeyIndex(root, best_path->path.distribute_keys, tlist) != NIL)
            AssertEreport(equal_distributekey(root, best_path->path.distribute_keys, node->plan.distributed_keys),
                MOD_OPT_SETOP,
                "Distribute keys error when create merge append plan.");
    } else {
        plan->distributed_keys = NIL;
        plan->exec_type = EXEC_ON_COORDS;
        plan->exec_nodes = ng_get_single_node_group_exec_node();
    }
#endif

    copy_path_costsize(plan, (Path*)best_path);

    return (Plan*)node;
}

/*
 * adjust_scan_targetlist
 *	  When the path is added because scan cannot process vector engine
 *	  unsupport expression, We need add extra targetlist to scan.
 *	  Adjust scan targetlist by result's pathqual
 *
 *	  Returns void.
 */
static void adjust_scan_targetlist(ResultPath* best_path, Plan* subplan)
{
    ListCell* lc = NULL;
    List* var_list = NIL;
    List* vartar_list = NIL;
    List* sub_targetlist = NIL;
    Assert(best_path->ispulledupqual);

    vartar_list = pull_var_clause((Node*)subplan->targetlist, PVC_REJECT_AGGREGATES, PVC_REJECT_PLACEHOLDERS);
    if (best_path->pathqual != NULL) {
        /*
         * Process the var has not been added into Scan->targetlist,
         * Add result->pathqual vars to Scan->targetlist
         * Case 1:
         * 		select c3 from t1 where expr(c1) > 10;
         * 		Cannot just return c3 in Scan, because the result->pathqual need c1 to filter the results.
         * Case 2:
         *		select c3 from t1 where expr(c3) > 10;
         * 		c3 is already contained in Scan->targetList, do nothing.
         */
        var_list = pull_var_clause((Node*)best_path->pathqual, PVC_REJECT_AGGREGATES, PVC_REJECT_PLACEHOLDERS);
        vartar_list = list_union(vartar_list, var_list);
    }

    if (IsA(subplan, DfsScan) && ((DfsScan*)subplan)->privateData != NIL) {
        /*
         * We add privatData for hdfs scan which will be pushed down,
         * if we don't add vars to DfsScan->reltargetlist, privateData will
         * be mark output vars that related quals as null.
         *
         * PREPARE pbe(int,int) SELECT max(col1) FROM t WHERE col2 in ($1,$2);
         * origin rel->reltargetlist: col1
         * privateData:
         * 			-> targetList: col1
         *			-> columnList: col1, col2
         *			-> restrictColList: col2
         * and this will be mark col2 as null when do projection
         */
        DfsScan* node = (DfsScan*)subplan;
        DfsPrivateItem* item = (DfsPrivateItem*)((DefElem*)linitial(node->privateData))->arg;

        item->columnList = list_union(item->columnList, var_list);
        item->targetList = list_union(item->targetList, var_list);
        var_list = pull_var_clause((Node*)item->restrictColList, PVC_REJECT_AGGREGATES, PVC_REJECT_PLACEHOLDERS);
        vartar_list = list_union(vartar_list, var_list);
    }

    /* Pull all exprs to result operator */
    foreach (lc, vartar_list) {
        Assert(IsA(lfirst(lc), Var));
        Var* var = (Var*)lfirst(lc);
        /* Don't lose the resjunk info */
        bool resjunk = false;
        if (var->varattno == 0 && var->varno > 0) {
            resjunk = true;
        }
        TargetEntry* entry = makeTargetEntry((Expr*)var, list_length(sub_targetlist) + 1, NULL, resjunk);
        entry->resorigcol = var->varattno;
        sub_targetlist = lappend(sub_targetlist, entry);
    }
    subplan->targetlist = sub_targetlist;
    if (IsA(subplan, PartIterator)) {
        subplan->lefttree->targetlist = subplan->targetlist;
    }
}

/*
 * create_result_plan
 *	  Create a Result plan for 'best_path'.
 *	  This is only used for the case of a query with an empty jointree.
 *
 *	  Returns a Plan node.
 */
static BaseResult* create_result_plan(PlannerInfo* root, ResultPath* best_path)
{
    List* tlist = NIL;
    List* quals = NIL;
    List* subquals = NIL;
    Plan* subplan = NULL;

    if (best_path->subpath != NULL) {
        subplan = create_plan_recurse(root, best_path->subpath);
        tlist = subplan->targetlist;

        /*
         * If the path is added because the lower pathnode cannot process vector
         * engine unsupport expression, We need add extra targetlist to lower plannode
         * to process the result->pathqual pulled up from Scan qual
         */
        if (best_path->ispulledupqual) {
            tlist = list_copy(subplan->targetlist);
            adjust_scan_targetlist(best_path, subplan);
        }
    } else {
        /* The tlist will be installed later, since we have no RelOptInfo */
        Assert(best_path->path.parent == NULL);
        tlist = NIL;
    }

    /* best_path->quals is just bare clauses */
    quals = order_qual_clauses(root, best_path->quals);
    subquals = order_qual_clauses(root, best_path->pathqual);

    return make_result(root, tlist, (Node*)quals, subplan, subquals);
}

/*
 * create_material_plan
 *	  Create a Material plan for 'best_path' and (recursively) plans
 *	  for its subpaths.
 *
 *	  Returns a Plan node.
 */
static Material* create_material_plan(PlannerInfo* root, MaterialPath* best_path)
{
    Material* plan = NULL;
    Plan* subplan = NULL;

    subplan = create_plan_recurse(root, best_path->subpath);

    /* We don't want any excess columns in the materialized tuples */
    disuse_physical_tlist(subplan, best_path->subpath);

    plan = make_material(subplan, best_path->materialize_all);

    plan->plan.hasUniqueResults = subplan->hasUniqueResults;

    copy_path_costsize(&plan->plan, (Path*)best_path);
    copy_mem_info(&plan->mem_info, &best_path->mem_info);

    if (root->isPartIteratorPlanning) {
        plan->plan.ispwj = true;
    }

    return plan;
}

/*
 * create_unique_plan
 *	  Create a Unique plan for 'best_path' and (recursively) plans
 *	  for its subpaths.
 *
 *	  Returns a Plan node.
 */
static Plan* create_unique_plan(PlannerInfo* root, UniquePath* best_path)
{
    Plan* plan = NULL;
    Plan* subplan = NULL;
    List* in_operators = NIL;
    List* uniq_exprs = NIL;
    List* newtlist = NIL;
    int nextresno;
    bool itemChange = false;
    int numGroupCols;
    AttrNumber* groupColIdx = NULL;
    int groupColPos;
    ListCell* l = NULL;

    subplan = create_plan_recurse(root, best_path->subpath);

    /* Done if we don't need to do any actual unique-ifying */
    if (best_path->umethod == UNIQUE_PATH_NOOP)
        return subplan;

    /*
     * As constructed, the subplan has a "flat" tlist containing just the Vars
     * needed here and at upper levels.  The values we are supposed to
     * unique-ify may be expressions in these variables.  We have to add any
     * such expressions to the subplan's tlist.
     *
     * The subplan may have a "physical" tlist if it is a simple scan plan. If
     * we're going to sort, this should be reduced to the regular tlist, so
     * that we don't sort more data than we need to.  For hashing, the tlist
     * should be left as-is if we don't need to add any expressions; but if we
     * do have to add expressions, then a projection step will be needed at
     * runtime anyway, so we may as well remove unneeded items. Therefore
     * newtlist starts from build_relation_tlist() not just a copy of the
     * subplan's tlist; and we don't install it into the subplan unless we are
     * sorting or stuff has to be added.
     */
    in_operators = best_path->in_operators;
    uniq_exprs = best_path->uniq_exprs;

    /* initialize modified subplan tlist as just the "required" vars */
    newtlist = build_relation_tlist(best_path->path.parent);
    nextresno = list_length(newtlist) + 1;
    itemChange = false;

    foreach (l, uniq_exprs) {
        Node* uniqexpr = (Node*)lfirst(l);
        TargetEntry* tle = NULL;

        tle = tlist_member(uniqexpr, newtlist);
        if (tle == NULL) {
            tle = makeTargetEntry((Expr*)uniqexpr, nextresno, NULL, false);
            newtlist = lappend(newtlist, tle);
            nextresno++;
            itemChange = true;
        }
    }

    /* For stream plan, its targetlist maybe different from newtlist,
     * then need add result node
     */
    if (IsA(subplan, Stream)) {
        itemChange = !equal(subplan->targetlist, newtlist);
    }

    /*
     * If the top plan node can't do projections, we need to add a Result
     * node to help it along.
     */
    if (itemChange && !is_projection_capable_plan(subplan)) {
        subplan = (Plan*)make_result(root, newtlist, NULL, subplan);
    } else { /* Now we should handle hash agg besides sort, since we support hashagg write-out-to-disk now */
        Assert(best_path->umethod == UNIQUE_PATH_SORT || best_path->umethod == UNIQUE_PATH_HASH);
        subplan->targetlist = newtlist;

        /* target: Data Partition */
        if (IsA(subplan, PartIterator)) {
            /*
             * If is a PartIterator + Scan, push the PartIterator's
             * tlist to Scan.
             */
            subplan->lefttree->targetlist = newtlist;
        } else if (IsA(subplan, Append) && best_path->path.parent->rtekind == RTE_RELATION) {
            ListCell* subnode = NULL;
            foreach (subnode, ((Append*)subplan)->appendplans) {
                Plan* sub_plan = (Plan*)lfirst(subnode);
                if (IsA(sub_plan, PartIterator)) {
                    /*
                     * If it is RTE_RELATION + Append + PartIterator + Scan,
                     * push the PartIterator's tlist to Scan.
                     */
                    sub_plan->targetlist = newtlist;
                    sub_plan->lefttree->targetlist = newtlist;
                }
            }
        }
    }

    /*
     * Build control information showing which subplan output columns are to
     * be examined by the grouping step.  Unfortunately we can't merge this
     * with the previous loop, since we didn't then know which version of the
     * subplan tlist we'd end up using.
     */
    newtlist = subplan->targetlist;
    numGroupCols = list_length(uniq_exprs);
    groupColIdx = (AttrNumber*)palloc(numGroupCols * sizeof(AttrNumber));

    groupColPos = 0;
    foreach (l, uniq_exprs) {
        Node* uniqexpr = (Node*)lfirst(l);
        TargetEntry* tle = NULL;

        tle = tlist_member(uniqexpr, newtlist);
        if (tle == NULL) /* shouldn't happen */
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                    (errmsg("failed to find unique expression in subplan tlist"))));
        groupColIdx[groupColPos++] = tle->resno;
    }

    if (best_path->umethod == UNIQUE_PATH_HASH) {
        long numGroups[2];
        Oid* groupOperators = NULL;
        List* tlist = build_relation_tlist(best_path->path.parent);

        numGroups[0] = (long)Min(PATH_LOCAL_ROWS(&best_path->path), (double)LONG_MAX);
        numGroups[1] = (long)Min(best_path->path.rows, (double)LONG_MAX);

        /*
         * Get the hashable equality operators for the Agg node to use.
         * Normally these are the same as the IN clause operators, but if
         * those are cross-type operators then the equality operators are the
         * ones for the IN clause operators' RHS datatype.
         */
        groupOperators = (Oid*)palloc(numGroupCols * sizeof(Oid));
        groupColPos = 0;
        foreach (l, in_operators) {
            Oid in_oper = lfirst_oid(l);
            Oid eq_oper;

            if (!get_compatible_hash_operators(in_oper, NULL, &eq_oper))
                ereport(ERROR,
                    (errmodule(MOD_OPT),
                        errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                        (errmsg("could not find compatible hash operator for operator %u", in_oper))));
            groupOperators[groupColPos++] = eq_oper;
        }

        /*
         * Since the Agg node is going to project anyway, we can give it the
         * minimum output tlist, without any stuff we might have added to the
         * subplan tlist.
         */
#ifdef STREAMPLAN
        if (IS_STREAM_PLAN && best_path->hold_tlist) {
            tlist = newtlist;
        }
#endif

        /* Can't figure out the real size, so give a rough estimation */
        Size hashentrysize = alloc_trunk_size((subplan->plan_width) + MAXALIGN(sizeof(MinimalTupleData)));

        plan = (Plan*)make_agg(root,
            tlist,
            NIL,
            AGG_HASHED,
            NULL,
            numGroupCols,
            groupColIdx,
            groupOperators,
            numGroups[0],
            subplan,
            NULL,
            false,
            false,
            NIL,
            hashentrysize,
            false,
            UNIQUE_INTENT,
            false);
        copy_mem_info(&((Agg*)plan)->mem_info, &best_path->mem_info);
    } else {
        List* sortList = NIL;

        /* Create an ORDER BY list to sort the input compatibly */
        groupColPos = 0;
        foreach (l, in_operators) {
            Oid in_oper = lfirst_oid(l);
            Oid sortop;
            Oid eqop;
            TargetEntry* tle = NULL;
            SortGroupClause* sortcl = NULL;

            sortop = get_ordering_op_for_equality_op(in_oper, false);
            if (!OidIsValid(sortop)) /* shouldn't happen */
                ereport(ERROR,
                    (errmodule(MOD_OPT),
                        errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                        (errmsg("could not find ordering operator for equality operator %u", in_oper))));

            /*
             * The Unique node will need equality operators.  Normally these
             * are the same as the IN clause operators, but if those are
             * cross-type operators then the equality operators are the ones
             * for the IN clause operators' RHS datatype.
             */
            eqop = get_equality_op_for_ordering_op(sortop, NULL);
            if (!OidIsValid(eqop)) /* shouldn't happen */
                ereport(ERROR,
                    (errmodule(MOD_OPT),
                        errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                        (errmsg("could not find equality operator for ordering operator %u", sortop))));

            tle = get_tle_by_resno(subplan->targetlist, groupColIdx[groupColPos]);
            if (tle == NULL) {
                ereport(ERROR,
                    (errmodule(MOD_OPT),
                        errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                        errmsg("tle can not be found from targetlist")));
            }

            sortcl = makeNode(SortGroupClause);
            sortcl->tleSortGroupRef = assignSortGroupRef(tle, subplan->targetlist);
            sortcl->eqop = eqop;
            sortcl->sortop = sortop;
            sortcl->nulls_first = false;
            sortcl->hashable = false; /* no need to make this accurate */
            sortList = lappend(sortList, sortcl);
            groupColPos++;
        }
        plan = (Plan*)make_sort_from_sortclauses(root, sortList, subplan);
        copy_mem_info(&((Sort*)plan)->mem_info, &best_path->mem_info);
        plan = (Plan*)make_unique(plan, sortList);
    }

    /* Adjust output size estimate (other fields should be OK already) */
    set_plan_rows(plan, best_path->path.rows, best_path->path.multiple);

#ifdef STREAMPLAN
    if (plan->lefttree) {
        inherit_plan_locator_info(plan, plan->lefttree);
    }
#endif

    return plan;
}

/*
 * Brief        : just for cooperation analysis, the request from client clust just need to scan some not all DNs.
 * Input        : RangeTblEntry* rte, by which we can get all DNs that table data is saved on.
 * Return Value : some DNs scaned by the request
 */
List* reassign_nodelist(RangeTblEntry* rte, List* ori_node_list)
{
    if (GetLocatorType(rte->relid) == LOCATOR_TYPE_REPLICATED) {
        return ori_node_list;
    }

    int dn_num = 0;
    int index = 0;
    int current_idx = 0;
    Oid* nodeoids = NULL;

    ListCell* lc = NULL;
    List* old_node_list = NIL;
    List* new_node_list = NIL;

    /* get dn number and oids */
    dn_num = get_pgxc_classnodes(rte->relid, &nodeoids);

    Assert(dn_num);
    if (dn_num == 0 ) {
        ereport(ERROR,
            (errmodule(MOD_COOP_ANALYZE),
                errcode(ERRCODE_UNEXPECTED_NODE_STATE),
                (errmsg("No data node information for table: %s", rte->relname))));
    }

    /* get all index of data node */
    for (index = 0; index < dn_num; index++) {
        int nodeId = PGXCNodeGetNodeId(nodeoids[index], PGXC_NODE_DATANODE);
        old_node_list = lappend_int(old_node_list, nodeId);
    }

    /* get needed DNs with gc_fdw_current_idx and gc_fdw_max_idx */
    foreach (lc, old_node_list) {
        if (current_idx % u_sess->pgxc_cxt.gc_fdw_max_idx == u_sess->pgxc_cxt.gc_fdw_current_idx) {
            new_node_list = lappend_int(new_node_list, lfirst_int(lc));
        }
        current_idx++;
    }

    Assert(new_node_list);
    if (new_node_list == NIL) {
        ereport(ERROR,
            (errmodule(MOD_COOP_ANALYZE),
                errcode(ERRCODE_UNEXPECTED_NODE_STATE),
                (errmsg("No data node found for u_sess->pgxc_cxt.gc_fdw_current_idx: %d, "
                        "u_sess->pgxc_cxt.gc_fdw_max_idx: %d",
                    u_sess->pgxc_cxt.gc_fdw_current_idx,
                    u_sess->pgxc_cxt.gc_fdw_max_idx))));
    }

    pfree_ext(nodeoids);
    list_free_ext(old_node_list);

    return new_node_list;
}

/*****************************************************************************
 *
 *	BASE-RELATION SCAN METHODS
 *****************************************************************************/
/*
 * Brief        : Add the distribution inforamtion for scanPlan.
 * Input        : root, the PlannerInfo struct which includes all infomation in optimizer phase.
 * Output       : scanPlan, the operator plan.
 *                relIndex, the table index in range table entry.
 *                bestPath, the best path for scanPlan.
 *                scanClause, the qual cluase for this plan.
 * Return Value : None.
 * Notes        : None.
 */
static void add_distribute_info(PlannerInfo* root, Plan* scanPlan, Index relIndex, Path* bestPath, List* scanClauses)
{
    ExecNodes* execNodes = NULL;
    if (unlikely(root->simple_rte_array == NULL)) {
        ereport(ERROR,
            (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("Range table should not be null")));
    }
    RangeTblEntry* rte = root->simple_rte_array[relIndex];

    if (is_sys_table(rte->relid) || RELKIND_IS_SEQUENCE(rte->relkind)) {
        execNodes = ng_convert_to_exec_nodes(&bestPath->distribution, bestPath->locator_type, RELATION_ACCESS_READ);
        scanPlan->exec_type = EXEC_ON_COORDS;
    } else if (IsToastNamespace(get_rel_namespace(rte->relid))) {
        execNodes = ng_convert_to_exec_nodes(&bestPath->distribution, bestPath->locator_type, RELATION_ACCESS_READ);
        scanPlan->exec_type = IS_PGXC_COORDINATOR ? EXEC_ON_COORDS : EXEC_ON_DATANODES;
    } else {
        if (GetLocatorType(rte->relid) == LOCATOR_TYPE_REPLICATED) {
            Assert(bestPath->locator_type == LOCATOR_TYPE_REPLICATED);
            execNodes = ng_convert_to_exec_nodes(&bestPath->distribution, bestPath->locator_type, RELATION_ACCESS_READ);
        } else {
            List* quals = scanClauses;

            /*
             * If the type of scanPlan is DfsScan, must use the following opExpressionList
             * instead of scanClauses which do not include pushdown quals.
             */
            if (IsA(scanPlan, DfsScan)) {
                DfsPrivateItem* item = (DfsPrivateItem*)((DefElem*)linitial(((DfsScan*)scanPlan)->privateData))->arg;
                quals = item->opExpressionList;
            }

            /* A hashed table may have filter quals, it's exec nodes are not equal to it's data nodes */
            execNodes = GetRelationNodesByQuals(
                    (void*)root->parse, rte->relid, relIndex, (Node*)quals, RELATION_ACCESS_READ, NULL, false);

            if (execNodes == NULL) {
                elog(DEBUG1, "[add_distribute_info] execNodes is NULL. Oid [%u]", rte->relid);
                Assert(rte->relid < FirstNormalObjectId || IS_PGXC_DATANODE || IsA(scanPlan, DfsScan));

                execNodes = makeNode(ExecNodes);
                Distribution* distribution = ng_get_installation_group_distribution();
                ng_set_distribution(&execNodes->distribution, distribution);
                execNodes->nodeList = ng_convert_to_nodeid_list(execNodes->distribution.bms_data_nodeids);
            }

            if (!IsLocatorDistributedBySlice(execNodes->baselocatortype)) {
                execNodes->baselocatortype = LOCATOR_TYPE_HASH;
            }

            if (IS_PGXC_COORDINATOR && u_sess->pgxc_cxt.is_gc_fdw && u_sess->pgxc_cxt.gc_fdw_max_idx > 0) {
                execNodes->nodeList = reassign_nodelist(rte, execNodes->nodeList);
            }
        }
        scanPlan->exec_type = EXEC_ON_DATANODES;
    }

    scanPlan->exec_nodes = execNodes;
    scanPlan->distributed_keys = bestPath->parent->distribute_keys;
}

/**
 * @Description: Whether the relation of relOptInfo is delta.
 * @in root, A PlannerInfo struct.
 * @in relOptInfo, A RelOptInfo struct.
 * @return If the relation is delta, return true, otherwise return false.
 */
static bool relIsDeltaNode(PlannerInfo* root, RelOptInfo* relOptInfo)
{
    Index varno = relOptInfo->relid;
    if (varno == 0) {
        /* If the varno is less then 1, it means that the relOptInfo is not
         * base rel relOptInfo, but it is a join rels.
         */
        return false;
    }
    RangeTblEntry* rte = planner_rt_fetch(varno, root);
    Relation rel = relation_open(rte->relid, AccessShareLock);
    bool isDelta = false;

    if (RELATION_IS_DELTA(rel)) {
        isDelta = true;
    }

    relation_close(rel, AccessShareLock);
    return isDelta;
}

/*
 * create_seqscan_plan
 *	 Returns a seqscan plan for the base relation scanned by 'best_path'
 *	 with restriction clauses 'scan_clauses' and targetlist 'tlist'.
 */
static SeqScan* create_seqscan_plan(PlannerInfo* root, Path* best_path, List* tlist, List* scan_clauses)
{
    SeqScan* scan_plan = NULL;
    Index scan_relid = best_path->parent->relid;
    RangeTblEntry* rte = NULL;

    /* it should be a base rel... */
    Assert(scan_relid > 0);
    Assert(best_path->parent->rtekind == RTE_RELATION);

    /* Sort clauses into best execution order */
    scan_clauses = order_qual_clauses(root, scan_clauses);

    /* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
    scan_clauses = extract_actual_clauses(scan_clauses, false);

    /* Replace any outer-relation variables with nestloop params */
    if (best_path->param_info) {
        scan_clauses = (List*)replace_nestloop_params(root, (Node*)scan_clauses);
    }

    if (tlist == NIL) {
        tlist = build_one_column_tlist(root, best_path->parent);
    }

    scan_plan = make_seqscan(tlist, scan_clauses, scan_relid);

    /* get tablesample clause... */
    rte = planner_rt_fetch(scan_relid, root);
    if (rte->tablesample) {
        Assert(rte->rtekind == RTE_RELATION);
        scan_plan->tablesample = rte->tablesample;
    }

    /* Set if the table of plan is delta. */
    if (relIsDeltaNode(root, best_path->parent)) {
        scan_plan->plan.isDeltaTable = true;
    }

#ifdef STREAMPLAN
    add_distribute_info(root, &scan_plan->plan, scan_relid, best_path, scan_clauses);
#endif

    copy_path_costsize(&scan_plan->plan, best_path);

    /*
     * While u_sess->attr.attr_sql.vectorEngineStrategy is OPT_VECTOR_ENGINE, we will use the
     * tuples number of relation to compute cost to determine using vector engine or not.
     * Partition table may have pruning, so for patition table using path rows instead of tuples.
     */
    if (u_sess->attr.attr_sql.vectorEngineStrategy == OPT_VECTOR_ENGINE &&
        (best_path->parent->pruning_result == NULL ||
         best_path->parent->pruning_result->state == PRUNING_RESULT_FULL)) {
        scan_plan->tableRows = best_path->parent->tuples;
    } else {
        scan_plan->tableRows = best_path->rows;
    }

    return scan_plan;
}

/* Support predicate pushing down to cstore scan.
 * Identify the qual which could be push down to cstore scan.
 */
static List* fix_cstore_scan_qual(PlannerInfo* root, List* qpqual)
{
    List* fixed_quals = NIL;
    ListCell* lc = NULL;

    /* Here only support and clause.If the qual is or clause, the length of qpqual is 1. */
    foreach (lc, qpqual) {
        Expr* clause = (Expr*)copyObject(lfirst(lc));

        if (!filter_cstore_clause(root, clause))
            continue;

        fixed_quals = lappend(fixed_quals, clause);
    }

    return fixed_quals;
}

#ifdef ENABLE_MULTIPLE_NODES
/* Support predicate pushing down to tsstore scan.
 * Identify the qual which could be push down to tsstore scan.
 */
static List* fix_tsstore_scan_qual(PlannerInfo* root, List* qpqual)
{
    List* fixed_quals = NIL;
    ListCell* lc = NULL;

    /*Here only support and clause.If the qual is or clause, the length of qpqual is 1.*/
    foreach (lc, qpqual) {
        Expr* clause = (Expr*)copyObject(lfirst(lc));
        /* reuse filters for tsstore */
        if (!filter_cstore_clause(root, clause)) {
            continue;
        }

        fixed_quals = lappend(fixed_quals, clause);
    }

    return fixed_quals; 
}
#endif   /* ENABLE_MULTIPLE_NODES */

/*
 * @Description: Adjust the target list to scan the index table.
 * @IN root: plan information
 * @IN best_path: path information
 * @IN node: DfsIndexScan node
 * @IN indexinfo: index information
 * @IN plan_qual: the qual on the dfsscan
 * @Return: the adjusted target list
 * @See also:
 */
static List* fix_dfs_index_target_list(
    PlannerInfo* root, Path* best_path, DfsIndexScan* node, IndexOptInfo* indexinfo, List* plan_qual)
{
    ListCell* cell = NULL;
    List* targetColumns = NIL;
    List* tlist = NIL;
    List* indexlist = indexinfo->indextlist;
    List* targetlist = node->dfsScan->plan.targetlist;
    Oid indexoid = node->indexid;
    Relation indexRel = relation_open(indexoid, AccessShareLock);
    int idxAttNo = indexRel->rd_att->natts;
    Expr* indexvar = NULL;
    ListCell* lc = NULL;
    AttrNumber resno = 1;

    /*
     * For psort,btree index, we don't scan other columns except tid in the index rel.
     * Notice : for index-only scan, we should scan the target columns for btree index.
     */
    List* vars = NIL;
    if (node->indexonly && (list_length(targetlist) > 0 || list_length(plan_qual) > 0)) {
        /* Prepare the primitive target column list. */
        foreach (cell, targetlist) {
            TargetEntry* tle = (TargetEntry*)lfirst(cell);

            /* Pull vars from  the targetlist . */
            vars = pull_var_clause((Node*)tle, PVC_RECURSE_AGGREGATES, PVC_RECURSE_PLACEHOLDERS);
            foreach (lc, vars) {
                Var* var = (Var*)lfirst(lc);
                int varattno = (int)var->varattno;

                if (varattno >= 0) {
                    targetColumns = lappend_int(targetColumns, varattno);
                }
            }
        }

        list_free_ext(vars);    /* free list before use */

        /* Prepare the primitive vars from quals. */
        foreach (cell, plan_qual) {
            Node* expr = (Node*)lfirst(cell);

            /* Pull vars from  the expr. */
            vars = pull_var_clause(expr, PVC_RECURSE_AGGREGATES, PVC_RECURSE_PLACEHOLDERS);
            foreach (lc, vars) {
                Var* var = (Var*)lfirst(lc);
                int varattno = (int)var->varattno;

                if (varattno >= 0) {
                    targetColumns = lappend_int(targetColumns, varattno);
                }
            }
        }

        list_free_ext(vars);    /* free list */

        /*
         * Extract the target entries from indexlist and use the ones which are inside
         * the primitive target column list to build the adjusted target column list.
         */
        for (int col = 0; col < list_length(indexlist); col++) {
            TargetEntry* tle = (TargetEntry*)list_nth(indexlist, col);
            Assert(IsA(tle->expr, Var));
            int pos = ((Var*)tle->expr)->varattno;
            if (list_member_int(targetColumns, pos)) {
                Form_pg_attribute att_tup = indexRel->rd_att->attrs[col];
                indexvar = (Expr*)makeVar(
                    ((Var*)tle->expr)->varno, col + 1, att_tup->atttypid, att_tup->atttypmod, att_tup->attcollation, 0);
                tlist = lappend(tlist, makeTargetEntry(indexvar, resno, NULL, false));
                resno++;
            }
        }
    }

    /* Add tid target column. */
    Expr* idxVar = (Expr*)makeVar(0, idxAttNo + 1, TIDOID, -1, InvalidOid, 0);
    tlist = lappend(tlist, makeTargetEntry(idxVar, resno, NULL, false));

    relation_close(indexRel, AccessShareLock);

    return tlist;
}

/*
 * @Description: set value to array MaxOrMin.
 * @in agg - agg of targetlist.
 * @in scan_targetlist - targetlist of cstore scan.
 * @in MaxOrMin - array keep GetMaxValueFromCu, GetMaxValueFromCu or GetMinAndMaxValueFromCu
 * @in strategy - mark max or min.
 */
static bool setMinMaxInfo(Aggref* agg, List* scan_targetlist, int16* MaxOrMin, int16 strategy)
{
    /* Only one parameter. */
    Assert(list_length(agg->args) == 1);
    TargetEntry* curTarget = (TargetEntry*)linitial(agg->args);

    /* The parameter can only be var type. */
    Assert(IsA(curTarget->expr, Var));

    int i = 0;
    ListCell* lcell = NULL;
    foreach (lcell, scan_targetlist) {
        TargetEntry* tle = (TargetEntry*)lfirst(lcell);

        /* Assign to tle's aggStrategy, indicate get min or max values. */
        if (equal(tle->expr, curTarget->expr)) {
            if (strategy == BTGreaterStrategyNumber) {
                MaxOrMin[i] = (uint16)MaxOrMin[i] | GetMaxValueFromCu;
            } else {
                MaxOrMin[i] = (uint16)MaxOrMin[i] | GetMinValueFromCu;
            }
            return true;
        }
        i++;
    }

    return false;
}

/*
 * @Description: adjust StoreScan's targetlist to col table's min/max optimization.
 * @in scan_plan - StoreScan plan
 * @in tlist - Final targetlist
 */
static void min_max_optimization(PlannerInfo* root, CStoreScan* scan_plan)
{
    Query* parse = root->parse;
    List* tlist = parse->targetList;
    List* scan_targetlist = scan_plan->plan.targetlist;
    ListCell* lc = NULL;
    FromExpr* jtnode = NULL;
    Node* node = NULL;
    RangeTblEntry* rte = NULL;
    RangeTblRef* rtr = NULL;
    int16* MaxOrMin = NULL;

    jtnode = parse->jointree;

    if (!parse->hasAggs || jtnode->quals || list_length(jtnode->fromlist) != 1) {
        return;
    }

    /* If query include groupClause, or whereClause etc, it can not be optimize. */
    if (parse->groupClause || parse->hasWindowFuncs || parse->groupingSets || parse->havingQual || jtnode->quals) {
        return;
    }

    node = (Node*)linitial(jtnode->fromlist);

    if (!IsA(node, RangeTblRef)) {
        return;
    }

    rtr = (RangeTblRef*)node;
    rte = planner_rt_fetch(rtr->rtindex, root);

    /* Only one base table. */
    if (rte->rtekind != RTE_RELATION) {
        return;
    }

    /* It must be column table. */
    Assert(rte->rtekind == RTE_RELATION && rte->orientation == REL_COL_ORIENTED);

    MaxOrMin = (int16*)palloc0(sizeof(int16) * list_length(scan_targetlist));

    foreach (lc, tlist) {
        int16 strategy = 0;

        TargetEntry* target = (TargetEntry*)lfirst(lc);

        if (IsA(target->expr, Aggref)) {
            Aggref* agg = (Aggref*)target->expr;

            /* Check this agg if can optimize and get strategy indicate agg is min or max. */
            if (check_agg_optimizable(agg, &strategy)) {
                /* Agg only is max or min. */
                Assert(strategy == BTGreaterStrategyNumber || strategy == BTLessStrategyNumber);

                /* Set GetMaxValueFromCu, GetMaxValueFromCu or GetMinAndMaxValueFromCu to array MaxOrMin. */
                if (!setMinMaxInfo(agg, scan_targetlist, MaxOrMin, strategy)) {
                    pfree_ext(MaxOrMin);
                    return;
                }
            } else {
                pfree_ext(MaxOrMin);
                return;
            }
        } else if (!IsA(target->expr, Const)) { /* target->expr may also be a const. */
            pfree_ext(MaxOrMin);
            return;
        }
    }

    for (int i = 0; i < list_length(scan_targetlist); i++) {
        scan_plan->minMaxInfo = lappend_int(scan_plan->minMaxInfo, MaxOrMin[i]);
    }

    pfree_ext(MaxOrMin);
}

/*
 * create_cstorescan_plan
 *	 Returns a cstorescan plan for the column store scanned by 'best_path'
 *	 with restriction clauses 'scan_clauses' and targetlist 'tlist'.
 */
static CStoreScan* create_cstorescan_plan(PlannerInfo* root, Path* best_path, List* tlist, List* scan_clauses)
{
    CStoreScan* scan_plan = NULL;
    Index scan_relid = best_path->parent->relid;
    RangeTblEntry* rte = NULL;

    /* it should be a base rel... */
    Assert(scan_relid > 0);
    Assert(best_path->parent->rtekind == RTE_RELATION);

    /* build cstore scan tlist, should only contain columns referenced, one column at least */
    if (tlist != NIL)
        list_free_ext(tlist);
    tlist = build_relation_tlist(best_path->parent);
    if (tlist == NIL)
        tlist = build_one_column_tlist(root, best_path->parent);

    /* Sort clauses into best execution order */
    scan_clauses = order_qual_clauses(root, scan_clauses);

    /* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
    scan_clauses = extract_actual_clauses(scan_clauses, false);

    scan_plan = make_cstorescan(tlist, scan_clauses, scan_relid);

    rte = planner_rt_fetch(scan_relid, root);
    if (rte->tablesample == NULL) {
        min_max_optimization(root, scan_plan);
    } else {
        Assert(rte->rtekind == RTE_RELATION);
        scan_plan->tablesample = rte->tablesample;
    }

    scan_plan->relStoreLocation = best_path->parent->relStoreLocation;

#ifdef STREAMPLAN
    add_distribute_info(root, &scan_plan->plan, scan_relid, best_path, scan_clauses);
#endif

    // set replica info.
    if (IsExecNodesReplicated(scan_plan->plan.exec_nodes))
        scan_plan->is_replica_table = true;

    copy_path_costsize(&scan_plan->plan, best_path);

    // Add some hints for better performance
    //
    scan_plan->selectionRatio = scan_plan->plan.plan_rows / best_path->parent->tuples;

    /* Fix the qual to support pushing predicate down to cstore scan. */
    if (u_sess->attr.attr_sql.enable_csqual_pushdown)
        scan_plan->cstorequal = fix_cstore_scan_qual(root, scan_clauses);
    else
        scan_plan->cstorequal = NIL;

    return scan_plan;
}

static DfsScan* create_dfsscan_plan(PlannerInfo* root, Path* bestPath, List* tList, List* scanClauses, bool indexFlag,
    List* excludedCol, bool indexOnly)
{
    DfsScan* dfsScan = NULL;
    Index scanRelid = bestPath->parent->relid;
    RelOptInfo* rel = bestPath->parent;
    int columnCount = 0;
    bool isPartTbl = false;
    List* partList = NIL;
    List* hdfsQualColumn = NIL;
    List* hdfsQual = NIL;
    double* selectivity = NULL;
    List* columnList = NIL;
    List* restrictColumnList = NIL;
    List* privateList = NIL;
    Oid dfsTblOid = InvalidOid;
    Relation dfsRelation = NULL;
    const char* storeFormat = NULL;
    List* dfsReadTList = NIL;
    int dop = SET_DOP(bestPath->dop);

    /* Fetch Relation handler */
    dfsTblOid = rt_fetch(scanRelid, root->parse->rtable)->relid;
    dfsRelation = RelationIdGetRelation(dfsTblOid);
    if (dfsRelation == NULL) {
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                errmsg("could not open relation with OID %u", dfsTblOid)));
    }

    /*
     * It should be a base rel.
     */
    Assert(scanRelid > 0);
    Assert(bestPath->parent->rtekind == RTE_RELATION);

    /*
     * Build dfs scan tlist, should only contain columns referenced, one column at least.
     */
    if (tList != NIL) {
        list_free_ext(tList);
    }
    tList = build_relation_tlist(bestPath->parent);

    /* Build dfs reader tlist, can contain no columns */
    dfsReadTList = build_dfs_reader_tlist(tList, excludedCol);

    if (!indexFlag) {
        /*
         * Sort clauses into best execution order.
         */
        scanClauses = order_qual_clauses(root, scanClauses);

        /*
         * Reduce RestrictInfo list to bare expressions; ignore pseudoconstants.
         */
        scanClauses = extract_actual_clauses(scanClauses, false);
    }

    if (tList == NIL) {
        tList = build_one_column_tlist(root, bestPath->parent);
    }

    /*
     * As an optimization, we only add columns that are present in the query to the
     * column mapping hash. we need baserel to find these columns. Because we don't
     * have access to baserel in executor's callback functions, we get the column list
     * here and put it into foreign scan node's private list.
     */
    restrictColumnList = GetRestrictColumns(scanClauses, rel->max_attr);

    columnList = MergeList(dfsReadTList, restrictColumnList, rel->max_attr);

    /* Check if the relation is partitioned. */
    if (RelationIsValuePartitioned(dfsRelation)) {
        isPartTbl = true;
        partList = ((ValuePartitionMap*)dfsRelation->partMap)->partList;
    }

    /*
     * We parse quals which is needed and can be pushed down to orc reader from
     * scanClauses for each column.
     */
    if (u_sess->attr.attr_sql.enable_hdfs_predicate_pushdown && !indexOnly) {
        columnCount = rel->max_attr;
        hdfsQualColumn = fix_pushdown_qual(&hdfsQual, &scanClauses, partList);
        selectivity = (double*)palloc0(sizeof(double) * columnCount);
        CalculateWeightByColumns(root, hdfsQualColumn, hdfsQual, selectivity, columnCount);
    }

    /*
     * Wrapper DfsPrivateItem using DefElem struct.
     */
    DfsPrivateItem* item = MakeDfsPrivateItem(
        columnList, dfsReadTList, restrictColumnList, scanClauses, NULL, hdfsQual, selectivity, columnCount, partList);
    privateList = list_make1(makeDefElem(DFS_PRIVATE_ITEM, (Node*)item));

    dfsScan = make_dfsscan(tList, scanClauses, scanRelid, privateList, dop);
    dfsScan->isPartTbl = isPartTbl;

    /*
     * set store information.
     */
    dfsScan->relStoreLocation = bestPath->parent->relStoreLocation;

    /*
     * We have to *copy* reloptions->orientation instead of assigning pointer
     * here as dfsRelation is a local relcache entry that may get released later.
     */
    storeFormat = (const char*)RelationGetOrientation(dfsRelation);
    if (storeFormat == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                errmsg("Unable to get store format for Dfs relation %s", RelationGetRelationName(dfsRelation))));
    }
    dfsScan->storeFormat = pstrdup(storeFormat);

    RelationClose(dfsRelation);

#ifdef STREAMPLAN
    add_distribute_info(root, &dfsScan->plan, scanRelid, bestPath, scanClauses);
#endif

    copy_path_costsize(&dfsScan->plan, bestPath);

    return dfsScan;
}

#ifdef ENABLE_MULTIPLE_NODES
/*
 * create_tsstorescan_plan
 *  Returns a tsstorescan plan for the time series store scanned by 'best_path'
 *  with restriction clauses 'scan_clauses' and targetlist 'tlist'.
 */
static TsStoreScan* create_tsstorescan_plan(PlannerInfo* root, Path* best_path, List* tlist, List* scan_clauses)
{
    TsStoreScan* scan_plan = NULL;
    Index scan_relid = best_path->parent->relid;
    RangeTblEntry* rte = NULL;
 
    /* build tsstore scan tlist, should only contain columns referenced, one column at least */
    if (tlist != NIL) {
        list_free_ext(tlist); 
    }
    tlist = build_relation_tlist(best_path->parent);
    if (tlist == NIL) {
        tlist = build_one_column_tlist(root, best_path->parent);
    }
    
    /* Sort clauses into best execution order */
    scan_clauses = order_qual_clauses(root, scan_clauses);
 
    /* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
    scan_clauses = extract_actual_clauses(scan_clauses, false);
 
    scan_plan = make_tsstorescan(tlist, scan_clauses, scan_relid);
 
    rte = planner_rt_fetch(scan_relid, root);
    if (rte->tablesample != NULL) {
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("Unsupported Table Sample FOR TIMESERIES.")));
    }
 
    scan_plan->relStoreLocation = best_path->parent->relStoreLocation;
 
#ifdef STREAMPLAN
    add_distribute_info(root, &scan_plan->plan, scan_relid, best_path, scan_clauses);   
#endif
 
    // set replica info.
    if (IsExecNodesReplicated(scan_plan->plan.exec_nodes)) {
        scan_plan->is_replica_table = true;
    }
    
    copy_path_costsize(&scan_plan->plan, best_path);
 
    // Add some hints for better performance
    scan_plan->selectionRatio = scan_plan->plan.plan_rows / best_path->parent->tuples;
 
    /*Fix the qual to support pushing predicate down to tsstore scan.*/
    scan_plan->tsstorequal = (u_sess->attr.attr_sql.enable_csqual_pushdown ?
                              fix_tsstore_scan_qual(root, scan_clauses) : NIL);
 
    return scan_plan;
 }
#endif   /* ENABLE_MULTIPLE_NODES */

/*
 * create_indexscan_plan
 *	  Returns an indexscan plan for the base relation scanned by 'best_path'
 *	  with restriction clauses 'scan_clauses' and targetlist 'tlist'.
 *
 * We use this for both plain IndexScans and IndexOnlyScans, because the
 * qual preprocessing work is the same for both.  Note that the caller tells
 * us which to build --- we don't look at best_path->path.pathtype, because
 * create_bitmap_subplan needs to be able to override the prior decision.
 */
static Scan* create_indexscan_plan(
    PlannerInfo* root, IndexPath* best_path, List* tlist, List* scan_clauses, bool indexonly)
{
    Scan* scan_plan = NULL;
    List* indexquals = best_path->indexquals;
    List* indexorderbys = best_path->indexorderbys;
    Index baserelid = best_path->path.parent->relid;
    Oid indexoid = best_path->indexinfo->indexoid;
    List* qpqual = NIL;
    List* stripped_indexquals = NIL;
    List* fixed_indexquals = NIL;
    List* fixed_indexorderbys = NIL;
    List* opquals = NIL;
    ListCell* l = NULL;

    /* it should be a base rel... */
    Assert(baserelid > 0);
    Assert(best_path->path.parent->rtekind == RTE_RELATION);

    /*
     * Build "stripped" indexquals structure (no RestrictInfos) to pass to
     * executor as indexqualorig
     */
    stripped_indexquals = get_actual_clauses(indexquals);

    /*
     * The executor needs a copy with the indexkey on the left of each clause
     * and with index Vars substituted for table ones.
     */
    fixed_indexquals = fix_indexqual_references(root, best_path);

    /*
     * Likewise fix up index attr references in the ORDER BY expressions.
     */
    fixed_indexorderbys = fix_indexorderby_references(root, best_path);

    /*
     * The qpqual list must contain all restrictions not automatically handled
     * by the index, other than pseudoconstant clauses which will be handled
     * by a separate gating plan node.	All the predicates in the indexquals
     * will be checked (either by the index itself, or by nodeIndexscan.c),
     * but if there are any "special" operators involved then they must be
     * included in qpqual.	The upshot is that qpqual must contain
     * scan_clauses minus whatever appears in indexquals.
     *
     * In normal cases simple pointer equality checks will be enough to spot
     * duplicate RestrictInfos, so we try that first.
     *
     * Another common case is that a scan_clauses entry is generated from the
     * same EquivalenceClass as some indexqual, and is therefore redundant
     * with it, though not equal.  (This happens when indxpath.c prefers a
     * different derived equality than what generate_join_implied_equalities
     * picked for a parameterized scan's ppi_clauses.)
     *
     * In some situations (particularly with OR'd index conditions) we may
     * have scan_clauses that are not equal to, but are logically implied by,
     * the index quals; so we also try a predicate_implied_by() check to see
     * if we can discard quals that way.  (predicate_implied_by assumes its
     * first input contains only immutable functions, so we have to check
     * that.)
     *
     * We can also discard quals that are implied by a partial index's
     * predicate, but only in a plain SELECT; when scanning a target relation
     * of UPDATE/DELETE/SELECT FOR UPDATE, we must leave such quals in the
     * plan so that they'll be properly rechecked by EvalPlanQual testing.
     */
    qpqual = NIL;
    foreach (l, scan_clauses) {
        RestrictInfo* rinfo = (RestrictInfo*)lfirst(l);

        Assert(IsA(rinfo, RestrictInfo));
        if (rinfo->pseudoconstant)
            continue; /* we may drop pseudoconstants here */
        opquals = lappend(opquals, rinfo->clause);

        if (list_member_ptr(indexquals, rinfo))
            continue; /* simple duplicate */
        if (is_redundant_derived_clause(rinfo, indexquals))
            continue; /* derived from same EquivalenceClass */
        if (!contain_mutable_functions((Node*)rinfo->clause)) {
            List* clausel = list_make1(rinfo->clause);

            if (predicate_implied_by(clausel, indexquals))
                continue; /* provably implied by indexquals */
            if (best_path->indexinfo->indpred) {
                if (baserelid != (unsigned int)root->parse->resultRelation &&
                    get_parse_rowmark(root->parse, baserelid) == NULL)
                    if (predicate_implied_by(clausel, best_path->indexinfo->indpred))
                        continue; /* implied by index predicate */
            }
        }
        qpqual = lappend(qpqual, rinfo);
    }

    /* Sort clauses into best execution order */
    qpqual = order_qual_clauses(root, qpqual);

    /* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
    qpqual = extract_actual_clauses(qpqual, false);

    /*
     * We have to replace any outer-relation variables with nestloop params in
     * the indexqualorig, qpqual, and indexorderbyorig expressions.  A bit
     * annoying to have to do this separately from the processing in
     * fix_indexqual_references --- rethink this when generalizing the inner
     * indexscan support.  But note we can't really do this earlier because
     * it'd break the comparisons to predicates above ... (or would it?  Those
     * wouldn't have outer refs)
     */
    if (best_path->path.param_info) {
        stripped_indexquals = (List*)replace_nestloop_params(root, (Node*)stripped_indexquals);
        qpqual = (List*)replace_nestloop_params(root, (Node*)qpqual);
        indexorderbys = (List*)replace_nestloop_params(root, (Node*)indexorderbys);
    }

    /* Finally ready to build the plan node */
    if (best_path->path.parent->orientation == REL_COL_ORIENTED) {
        scan_plan = (Scan*)make_cstoreindexscan(root,
            &best_path->path,
            tlist,
            qpqual,
            baserelid,
            indexoid,
            fixed_indexquals,
            stripped_indexquals,
            fixed_indexorderbys,
            indexorderbys,
            best_path->indexinfo->indextlist,
            best_path->indexscandir,
            indexonly);
    } else if (best_path->path.parent->orientation == REL_PAX_ORIENTED) {
        scan_plan = (Scan*)make_dfsindexscan(root,
            &best_path->path,
            tlist,
            qpqual,
            baserelid,
            indexoid,
            fixed_indexquals,
            stripped_indexquals,
            fixed_indexorderbys,
            indexorderbys,
            best_path->indexinfo,
            best_path->indexscandir,
            indexonly);
    } else if (best_path->path.parent->orientation == REL_TIMESERIES_ORIENTED) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Unsupported Index Scan FOR TIMESERIES.")));
    } else {
        if (indexonly) {
            scan_plan = (Scan*)make_indexonlyscan(tlist,
                qpqual,
                baserelid,
                indexoid,
                fixed_indexquals,
                fixed_indexorderbys,
                best_path->indexinfo->indextlist,
                best_path->indexscandir);
        } else {
            scan_plan = (Scan*)make_indexscan(tlist,
                qpqual,
                baserelid,
                indexoid,
                fixed_indexquals,
                stripped_indexquals,
                fixed_indexorderbys,
                indexorderbys,
                best_path->indexscandir);
            ((IndexScan*)scan_plan)->is_ustore = best_path->is_ustore;
        }
    }

#ifdef STREAMPLAN
    add_distribute_info(root, &(scan_plan->plan), baserelid, &(best_path->path), opquals);
#endif

    copy_path_costsize(&scan_plan->plan, &best_path->path);

    return scan_plan;
}

/*
 * create_bitmap_scan_plan
 *	  Returns a bitmap scan plan for the base relation scanned by 'best_path'
 *	  with restriction clauses 'scan_clauses' and targetlist 'tlist'.
 */
static BitmapHeapScan* create_bitmap_scan_plan(
    PlannerInfo* root, BitmapHeapPath* best_path, List* tlist, List* scan_clauses)
{
    Index baserelid = best_path->path.parent->relid;
    Plan* bitmapqualplan = NULL;
    List* bitmapqualorig = NIL;
    List* indexquals = NIL;
    List* indexECs = NIL;
    List* qpqual = NIL;
    ListCell* l = NULL;
    BitmapHeapScan* scan_plan = NULL;

    /* it should be a base rel... */
    Assert(baserelid > 0);
    Assert(best_path->path.parent->rtekind == RTE_RELATION);

    /* Process the bitmapqual tree into a Plan tree and qual lists */
    bitmapqualplan = create_bitmap_subplan(root, best_path->bitmapqual, &bitmapqualorig, &indexquals, &indexECs);

    /*
     * The qpqual list must contain all restrictions not automatically handled
     * by the index, other than pseudoconstant clauses which will be handled
     * by a separate gating plan node.	All the predicates in the indexquals
     * will be checked (either by the index itself, or by
     * nodeBitmapHeapscan.c), but if there are any "special" operators
     * involved then they must be added to qpqual.	The upshot is that qpqual
     * must contain scan_clauses minus whatever appears in indexquals.
     *
     * This loop is similar to the comparable code in create_indexscan_plan(),
     * but with some differences because it has to compare the scan clauses to
     * stripped (no RestrictInfos) indexquals.	See comments there for more
     * info.
     *
     * In normal cases simple equal() checks will be enough to spot duplicate
     * clauses, so we try that first.  We next see if the scan clause is
     * redundant with any top-level indexqual by virtue of being generated
     * from the same EC.  After that, try predicate_implied_by().
     *
     * Unlike create_indexscan_plan(), we need take no special thought here
     * for partial index predicates; this is because the predicate conditions
     * are already listed in bitmapqualorig and indexquals.  Bitmap scans have
     * to do it that way because predicate conditions need to be rechecked if
     * the scan becomes lossy, so they have to be included in bitmapqualorig.
     */
    qpqual = NIL;
    foreach (l, scan_clauses) {
        RestrictInfo* rinfo = (RestrictInfo*)lfirst(l);
        Node* clause = (Node*)rinfo->clause;

        Assert(IsA(rinfo, RestrictInfo));
        if (rinfo->pseudoconstant)
            continue; /* we may drop pseudoconstants here */
        if (list_member(indexquals, clause))
            continue; /* simple duplicate */
        if (rinfo->parent_ec && list_member_ptr(indexECs, rinfo->parent_ec))
            continue; /* derived from same EquivalenceClass */
        if (!contain_mutable_functions(clause)) {
            List* clausel = list_make1(clause);

            if (predicate_implied_by(clausel, indexquals))
                continue; /* provably implied by indexquals */
        }
        qpqual = lappend(qpqual, rinfo);
    }

    /* Release indexquals */
    list_free_ext(indexquals);

    /* Sort clauses into best execution order */
    qpqual = order_qual_clauses(root, qpqual);

    /* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
    qpqual = extract_actual_clauses(qpqual, false);

    /*
     * When dealing with special operators, we will at this point have
     * duplicate clauses in qpqual and bitmapqualorig.	We may as well drop
     * 'em from bitmapqualorig, since there's no point in making the tests
     * twice.
     */
    bitmapqualorig = list_difference_ptr(bitmapqualorig, qpqual);

    /*
     * We have to replace any outer-relation variables with nestloop params in
     * the qpqual and bitmapqualorig expressions.  (This was already done for
     * expressions attached to plan nodes in the bitmapqualplan tree.)
     */
    if (best_path->path.param_info) {
        qpqual = (List*)replace_nestloop_params(root, (Node*)qpqual);
        bitmapqualorig = (List*)replace_nestloop_params(root, (Node*)bitmapqualorig);
    }

    /* Finally ready to build the plan node */
    if (best_path->path.parent->orientation == REL_COL_ORIENTED)
        scan_plan = (BitmapHeapScan*)make_cstoreindex_heapscan(
            root, &best_path->path, tlist, qpqual, bitmapqualplan, bitmapqualorig, baserelid);
    else if (best_path->path.parent->orientation == REL_TIMESERIES_ORIENTED)
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Unsupported Bitmap Heap Scan FOR TIMESERIES.")));
    else
        scan_plan = make_bitmap_heapscan(tlist, qpqual, bitmapqualplan, bitmapqualorig, baserelid);

    copy_path_costsize(&scan_plan->scan.plan, &best_path->path);

    return scan_plan;
}
/*
 * Given a bitmapqual tree, generate the Plan tree that implements it
 *
 * As byproducts, we also return in *qual and *indexqual the qual lists
 * (in implicit-AND form, without RestrictInfos) describing the original index
 * conditions and the generated indexqual conditions.  (These are the same in
 * simple cases, but when special index operators are involved, the former
 * list includes the special conditions while the latter includes the actual
 * indexable conditions derived from them.)  Both lists include partial-index
 * predicates, because we have to recheck predicates as well as index
 * conditions if the bitmap scan becomes lossy.
 *
 * In addition, we return a list of EquivalenceClass pointers for all the
 * top-level indexquals that were possibly-redundantly derived from ECs.
 * This allows removal of scan_clauses that are redundant with such quals.
 * (We do not attempt to detect such redundancies for quals that are within
 * OR subtrees.  This could be done in a less hacky way if we returned the
 * indexquals in RestrictInfo form, but that would be slower and still pretty
 * messy, since we'd have to build new RestrictInfos in many cases.)
 *
 * Note: if you find yourself changing this, you probably need to change
 * make_restrictinfo_from_bitmapqual too.
 */
static Plan* create_bitmap_subplan(PlannerInfo* root, Path* bitmapqual, List** qual, List** indexqual, List** indexECs)
{
    Plan* plan = NULL;

    if (IsA(bitmapqual, BitmapAndPath)) {
        BitmapAndPath* apath = (BitmapAndPath*)bitmapqual;
        List* subplans = NIL;
        List* subquals = NIL;
        List* subindexquals = NIL;
        List* subindexECs = NIL;
        ListCell* l = NULL;

        /*
         * There may well be redundant quals among the subplans, since a
         * top-level WHERE qual might have gotten used to form several
         * different index quals.  We don't try exceedingly hard to eliminate
         * redundancies, but we do eliminate obvious duplicates by using
         * list_concat_unique.
         */
        foreach (l, apath->bitmapquals) {
            Plan* subplan = NULL;
            List* subqual = NIL;
            List* subindexqual = NIL;
            List* subindexEC = NIL;

            subplan = create_bitmap_subplan(root, (Path*)lfirst(l), &subqual, &subindexqual, &subindexEC);
            subplans = lappend(subplans, subplan);
            subquals = list_concat_unique(subquals, subqual);
            subindexquals = list_concat_unique(subindexquals, subindexqual);
            /* Duplicates in indexECs aren't worth getting rid of */
            subindexECs = list_concat(subindexECs, subindexEC);
        }
        if (apath->path.parent->orientation == REL_COL_ORIENTED) {
            plan = (Plan*)make_cstoreindex_and(subplans);
        } else if (apath->path.parent->orientation == REL_TIMESERIES_ORIENTED) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Unsupported Bitmap And FOR TIMESERIES.")));
        } else
            plan = (Plan*)make_bitmap_and(subplans);
        ((BitmapAnd*)plan)->is_ustore = apath->is_ustore;
        plan->startup_cost = apath->path.startup_cost;
        plan->total_cost = apath->path.total_cost;
        set_plan_rows(
            plan, clamp_row_est(apath->bitmapselectivity * apath->path.parent->tuples), apath->path.parent->multiple);
        plan->plan_width = 0; /* meaningless */
        *qual = subquals;
        *indexqual = subindexquals;
        *indexECs = subindexECs;
    } else if (IsA(bitmapqual, BitmapOrPath)) {
        BitmapOrPath* opath = (BitmapOrPath*)bitmapqual;
        List* subplans = NIL;
        List* subquals = NIL;
        List* subindexquals = NIL;
        bool const_true_subqual = false;
        bool const_true_subindexqual = false;
        ListCell* l = NULL;

        /*
         * Here, we only detect qual-free subplans.  A qual-free subplan would
         * cause us to generate "... OR true ..."  which we may as well reduce
         * to just "true".	We do not try to eliminate redundant subclauses
         * because (a) it's not as likely as in the AND case, and (b) we might
         * well be working with hundreds or even thousands of OR conditions,
         * perhaps from a long IN list.  The performance of list_append_unique
         * would be unacceptable.
         */
        foreach (l, opath->bitmapquals) {
            Plan* subplan = NULL;
            List* subqual = NIL;
            List* subindexqual = NIL;
            List* subindexEC = NIL;

            subplan = create_bitmap_subplan(root, (Path*)lfirst(l), &subqual, &subindexqual, &subindexEC);
            subplans = lappend(subplans, subplan);
            if (subqual == NIL)
                const_true_subqual = true;
            else if (!const_true_subqual)
                subquals = lappend(subquals, make_ands_explicit(subqual));
            if (subindexqual == NIL)
                const_true_subindexqual = true;
            else if (!const_true_subindexqual)
                subindexquals = lappend(subindexquals, make_ands_explicit(subindexqual));
        }

        /*
         * In the presence of ScalarArrayOpExpr quals, we might have built
         * BitmapOrPaths with just one subpath; don't add an OR step.
         */
        if (list_length(subplans) == 1) {
            plan = (Plan*)linitial(subplans);
        } else {
            if (opath->path.parent->orientation == REL_COL_ORIENTED)
                plan = (Plan*)make_cstoreindex_or(subplans);
            else if (opath->path.parent->orientation == REL_TIMESERIES_ORIENTED) {
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("Unsupported Bitmap OR FOR TIMESERIES.")));
            } else
                plan = (Plan*)make_bitmap_or(subplans);
            ((BitmapOr*)plan)->is_ustore = opath->is_ustore;
            plan->startup_cost = opath->path.startup_cost;
            plan->total_cost = opath->path.total_cost;
            set_plan_rows(plan,
                clamp_row_est(opath->bitmapselectivity * opath->path.parent->tuples),
                opath->path.parent->multiple);
            plan->plan_width = 0; /* meaningless */
        }

        /*
         * If there were constant-TRUE subquals, the OR reduces to constant
         * TRUE.  Also, avoid generating one-element ORs, which could happen
         * due to redundancy elimination or ScalarArrayOpExpr quals.
         */
        if (const_true_subqual)
            *qual = NIL;
        else if (list_length(subquals) <= 1)
            *qual = subquals;
        else
            *qual = list_make1(make_orclause(subquals));
        if (const_true_subindexqual)
            *indexqual = NIL;
        else if (list_length(subindexquals) <= 1)
            *indexqual = subindexquals;
        else
            *indexqual = list_make1(make_orclause(subindexquals));
        *indexECs = NIL;
    } else if (IsA(bitmapqual, IndexPath)) {
        IndexPath* ipath = (IndexPath*)bitmapqual;
        Plan* indexscan = NULL;
        List* subindexECs = NIL;
        ListCell* l = NULL;

        if (ipath->path.parent->orientation == REL_COL_ORIENTED) {
            CStoreIndexScan* iscan = (CStoreIndexScan*)create_indexscan_plan(root, ipath, NIL, NIL, false);
            Assert(IsA(iscan, CStoreIndexScan));
            plan = (Plan*)make_cstoreindex_ctidscan(
                root, iscan->scan.scanrelid, iscan->indexid, iscan->indexqual, iscan->indexqualorig, iscan->indextlist);
            indexscan = (Plan*)iscan;
        } else if (ipath->path.parent->orientation == REL_TIMESERIES_ORIENTED) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Unsupported Bitmap Index Scan FOR TIMESERIES.")));
        } else {
            /* Use the regular indexscan plan build machinery... */
            IndexScan* iscan = (IndexScan*)create_indexscan_plan(root, ipath, NIL, NIL, false);
            Assert(IsA(iscan, IndexScan));
            /* then convert to a bitmap indexscan */
            plan = (Plan*)make_bitmap_indexscan(
                iscan->scan.scanrelid, iscan->indexid, iscan->indexqual, iscan->indexqualorig);
            ((BitmapIndexScan*)plan)->is_ustore = iscan->is_ustore;
            indexscan = (Plan*)iscan;
        }
#ifdef STREAMPLAN
        inherit_plan_locator_info(plan, indexscan);
#endif

        BitmapIndexScan* btindexscan = (BitmapIndexScan*)plan;
        btindexscan->scan.bucketInfo = bitmapqual->parent->bucketInfo;
        /* Global partition index don't need set part interator infomartition */
        if (root->isPartIteratorPlanning && !CheckIndexPathUseGPI(ipath)) {
            btindexscan = (BitmapIndexScan*)plan;
            btindexscan->scan.isPartTbl = true;
            btindexscan->scan.itrs = root->curItrs;
            btindexscan->scan.pruningInfo = bitmapqual->parent->pruning_result;
            btindexscan->scan.plan.paramno = root->curIteratorParamIndex;
            btindexscan->scan.plan.subparamno = root->curSubPartIteratorParamIndex;
            btindexscan->scan.partScanDirection = ForwardScanDirection;
        }

        plan->startup_cost = 0.0;
        plan->total_cost = ipath->indextotalcost;
        set_plan_rows(
            plan, clamp_row_est(ipath->indexselectivity * ipath->path.parent->tuples), ipath->path.parent->multiple);
        plan->plan_width = 0; /* meaningless */
        *qual = get_actual_clauses(ipath->indexclauses);
        *indexqual = get_actual_clauses(ipath->indexquals);
        foreach (l, ipath->indexinfo->indpred) {
            Expr* pred = (Expr*)lfirst(l);

            /*
             * We know that the index predicate must have been implied by the
             * query condition as a whole, but it may or may not be implied by
             * the conditions that got pushed into the bitmapqual.	Avoid
             * generating redundant conditions.
             */
            if (!predicate_implied_by(list_make1(pred), ipath->indexclauses)) {
                *qual = lappend(*qual, pred);
                *indexqual = lappend(*indexqual, pred);
            }
        }
        subindexECs = NIL;
        foreach (l, ipath->indexquals) {
            RestrictInfo* rinfo = (RestrictInfo*)lfirst(l);

            if (rinfo->parent_ec)
                subindexECs = lappend(subindexECs, rinfo->parent_ec);
        }
        *indexECs = subindexECs;
    } else {
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("unrecognized node type: %d", nodeTag(bitmapqual))));
        plan = NULL; /* keep compiler quiet */
    }

    return plan;
}

/*
 * create_tidscan_plan
 *	 Returns a tidscan plan for the base relation scanned by 'best_path'
 *	 with restriction clauses 'scan_clauses' and targetlist 'tlist'.
 */
static TidScan* create_tidscan_plan(PlannerInfo* root, TidPath* best_path, List* tlist, List* scan_clauses)
{
    TidScan* scan_plan = NULL;
    Index scan_relid = best_path->path.parent->relid;
    List* ortidquals = NIL;

    /* it should be a base rel... */
    Assert(scan_relid > 0);
    Assert(best_path->path.parent->rtekind == RTE_RELATION);

    /* Sort clauses into best execution order */
    scan_clauses = order_qual_clauses(root, scan_clauses);

    /* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
    scan_clauses = extract_actual_clauses(scan_clauses, false);

    /*
     * Remove any clauses that are TID quals.  This is a bit tricky since the
     * tidquals list has implicit OR semantics.
     */
    ortidquals = best_path->tidquals;
    if (list_length(ortidquals) > 1)
        ortidquals = list_make1(make_orclause(ortidquals));
    scan_clauses = list_difference(scan_clauses, ortidquals);

    scan_plan = make_tidscan(tlist, scan_clauses, scan_relid, best_path->tidquals);

#ifdef STREAMPLAN
    add_distribute_info(root, &(scan_plan->scan.plan), scan_relid, &(best_path->path), scan_clauses);
#endif

    copy_path_costsize(&scan_plan->scan.plan, &best_path->path);

    return scan_plan;
}

/*
 * create_subqueryscan_plan
 *	 Returns a subqueryscan plan for the base relation scanned by 'best_path'
 *	 with restriction clauses 'scan_clauses' and targetlist 'tlist'.
 */
static SubqueryScan* create_subqueryscan_plan(PlannerInfo* root, Path* best_path, List* tlist, List* scan_clauses)
{
    SubqueryScanPath *scan_path = (SubqueryScanPath *)best_path;
    SubqueryScan* scan_plan = NULL;
    Index scan_relid = best_path->parent->relid;

    if (u_sess->opt_cxt.is_under_append_plan && tlist == NIL) {
        Const *c = makeConst(INT4OID, -1, InvalidOid, -2, (Datum)0, true, false);
        tlist = lappend(tlist, makeTargetEntry((Expr*)c, 1, NULL, true));
    }

    /* it should be a subquery base rel... */
    Assert(scan_relid > 0);
    Assert(best_path->parent->rtekind == RTE_SUBQUERY);

    /* Sort clauses into best execution order */
    scan_clauses = order_qual_clauses(root, scan_clauses);

    /* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
    scan_clauses = extract_actual_clauses(scan_clauses, false);

    /* Replace any outer-relation variables with nestloop params */
    if (best_path->param_info) {
        scan_clauses = (List*)replace_nestloop_params(root, (Node*)scan_clauses);
        process_subquery_nestloop_params(root, scan_path->subplan_params);
    }

    scan_plan = make_subqueryscan(tlist, scan_clauses, scan_relid, scan_path->subplan);
#ifdef STREAMPLAN
    scan_plan->scan.plan.distributed_keys = best_path->parent->distribute_keys;

    /*
     * For partial-plan pushdown, add the gather operator to subplan in
     * create_subqueryscan_path. When the pathkeys information of the
     * subquery is included in sort_keys of the parent query, add the
     * simpleSort operator to gather immediately to ensure the order.
     * However, when pathkeys is used to create a sort plan in
     * create_subqueryscan_path, the pathkeys does not match the targetlist
     * of the lower-layer subplan. As a result, the simpleSort is placed here.
     */
    if (IS_STREAM_PLAN && !root->parse->can_push &&
        IsA(best_path->parent->subplan, RemoteQuery) &&
        !((RemoteQuery *)best_path->parent->subplan)->sort &&
        root->sort_pathkeys != NIL &&
        pathkeys_contained_in(root->sort_pathkeys, best_path->pathkeys)) {

        SimpleSort *simple_sort = makeNode(SimpleSort);
        Sort *sortPlan = make_sort_from_pathkeys(root, (Plan *) scan_plan, best_path->pathkeys, -1.0);
        simple_sort->numCols = sortPlan->numCols;
        simple_sort->sortColIdx = sortPlan->sortColIdx;
        simple_sort->sortOperators = sortPlan->sortOperators;
        simple_sort->nullsFirst = sortPlan->nullsFirst;
        simple_sort->sortToStore = false;
        simple_sort->sortCollations = sortPlan->collations;
        ((RemoteQuery *)best_path->parent->subplan)->sort = simple_sort;
    }

#endif

    copy_path_costsize(&scan_plan->scan.plan, best_path);

    /* If we apply row hint, modify rows of subplan as well */
    if (scan_clauses == NIL)
        scan_plan->subplan->plan_rows = scan_plan->scan.plan.plan_rows;
    
    return scan_plan;
}

/*
 * create_functionscan_plan
 *	 Returns a functionscan plan for the base relation scanned by 'best_path'
 *	 with restriction clauses 'scan_clauses' and targetlist 'tlist'.
 */
static FunctionScan* create_functionscan_plan(PlannerInfo* root, Path* best_path, List* tlist, List* scan_clauses)
{
    FunctionScan* scan_plan = NULL;
    Index scan_relid = best_path->parent->relid;
    RangeTblEntry* rte = NULL;
    Node *funcexpr = NULL;

    /* it should be a function base rel... */
    Assert(scan_relid > 0);
    rte = planner_rt_fetch(scan_relid, root);
    Assert(rte->rtekind == RTE_FUNCTION);
    funcexpr = rte->funcexpr;

    /* Sort clauses into best execution order */
    scan_clauses = order_qual_clauses(root, scan_clauses);

    /* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
    scan_clauses = extract_actual_clauses(scan_clauses, false);

    /* Replace any outer-relation variables with nestloop params */
    if (best_path->param_info != NULL) {
        scan_clauses = (List *)replace_nestloop_params(root, (Node *) scan_clauses);
        /* The func expression itself could contain nestloop params, too */
        funcexpr = replace_nestloop_params(root, funcexpr);
    }

    scan_plan = make_functionscan(tlist,
        scan_clauses,
        scan_relid,
        funcexpr,
        rte->eref->colnames,
        rte->funccoltypes,
        rte->funccoltypmods,
        rte->funccolcollations);

#ifdef ENABLE_MULTIPLE_NODES
    if (IS_STREAM_PLAN && rte->funcexpr) {
        scan_plan->scan.plan.exec_nodes =
            ng_convert_to_exec_nodes(&best_path->distribution, best_path->locator_type, RELATION_ACCESS_READ);
        if (IS_EC_FUNC(rte)) {
            /* We treate EC function specially */
            if (pgxc_is_funcRTE_shippable((Expr*)rte->funcexpr)) {
                scan_plan->scan.plan.exec_nodes = get_random_data_nodes(LOCATOR_TYPE_HASH, (Plan*)scan_plan);
                scan_plan->scan.plan.exec_type = EXEC_ON_DATANODES;
            } else {
                scan_plan->scan.plan.exec_type = EXEC_ON_COORDS;
            }
        } else {
            if (pgxc_is_funcRTE_shippable((Expr*)rte->funcexpr)) {

                if (vector_search_func_shippable(((FuncExpr*)rte->funcexpr)->funcid)) {
                    scan_plan->scan.plan.exec_nodes = get_all_data_nodes(LOCATOR_TYPE_HASH);
                    scan_plan->scan.plan.exec_type = EXEC_ON_DATANODES;
                } else {
                    scan_plan->scan.plan.exec_type = EXEC_ON_ALL_NODES;
                }
            } else {
                scan_plan->scan.plan.exec_type = EXEC_ON_COORDS;
            }
        }
    } else {
        scan_plan->scan.plan.exec_type = EXEC_ON_ALL_NODES;
        scan_plan->scan.plan.exec_nodes =
            ng_convert_to_exec_nodes(&best_path->distribution, best_path->locator_type, RELATION_ACCESS_READ);
    }
#else
    scan_plan->scan.plan.exec_type = EXEC_ON_ALL_NODES;
    scan_plan->scan.plan.exec_nodes =
        ng_convert_to_exec_nodes(&best_path->distribution, best_path->locator_type, RELATION_ACCESS_READ);
#endif

    copy_path_costsize(&scan_plan->scan.plan, best_path);

    return scan_plan;
}

/*
 * create_valuesscan_plan
 *	 Returns a valuesscan plan for the base relation scanned by 'best_path'
 *	 with restriction clauses 'scan_clauses' and targetlist 'tlist'.
 */
static ValuesScan* create_valuesscan_plan(PlannerInfo* root, Path* best_path, List* tlist, List* scan_clauses)
{
    ValuesScan* scan_plan = NULL;
    Index scan_relid = best_path->parent->relid;
    RangeTblEntry* rte = NULL;
    List *values_lists = NIL;

    /* it should be a values base rel... */
    Assert(scan_relid > 0);
    rte = planner_rt_fetch(scan_relid, root);
    Assert(rte->rtekind == RTE_VALUES);
    values_lists = rte->values_lists;

    /* Sort clauses into best execution order */
    scan_clauses = order_qual_clauses(root, scan_clauses);

    /* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
    scan_clauses = extract_actual_clauses(scan_clauses, false);

    /* Replace any outer-relation variables with nestloop params */
    if (best_path->param_info != NULL) {
        scan_clauses = (List *)replace_nestloop_params(root, (Node *) scan_clauses);
        /* The values lists could contain nestloop params, too */
        values_lists = (List *)replace_nestloop_params(root, (Node *) values_lists);
    }

    scan_plan = make_valuesscan(tlist, scan_clauses, scan_relid, values_lists);

#ifdef STREAMPLAN
    scan_plan->scan.plan.exec_nodes =
        ng_convert_to_exec_nodes(&best_path->distribution, best_path->locator_type, RELATION_ACCESS_READ);
    scan_plan->scan.plan.exec_type = EXEC_ON_DATANODES;
#endif

    copy_path_costsize(&scan_plan->scan.plan, best_path);

    return scan_plan;
}

/*
 * ModifyWorktableWtParam
 *	 When the with-recursive plan is referenced for multiple times (including CTE),
 *	 the plan is insufficient. This problem is solved by copying the plan. However,
 *	 after the RU is copied, the index of the worktable under the RU is the same as
 *	 that of the original RU. As a result, the ExecWorkTableScan scanning result is
 *	 missing. Therefore, you need to modify the worktable under the RU accordingly
 *	 to prevent the RU and original RU from sharing the same worktable.
 */
static void ModifyWorktableWtParam(Node* planNode, int oldWtParam, int newWtParam)
{
    if (planNode == NULL) {
        return;
    }

    switch (nodeTag(planNode)) {
        case T_WorkTableScan: {
            WorkTableScan* wtScan = (WorkTableScan*)planNode;
            if (wtScan->wtParam == oldWtParam) {
                wtScan->wtParam = newWtParam;
            }
            break;
        }

        case T_CStoreScan:
        case T_CStoreIndexScan:
        case T_CStoreIndexCtidScan:
        case T_CStoreIndexHeapScan:
#ifdef ENABLE_MULTIPLE_NODES
        case T_TsStoreScan:
#endif   /* ENABLE_MULTIPLE_NODES */
        case T_IndexScan:
        case T_IndexOnlyScan:
        case T_SeqScan:
        case T_DfsScan:
        case T_DfsIndexScan:
        case T_ForeignScan:
        case T_ExtensiblePlan:
        case T_BitmapHeapScan:
        case T_BitmapIndexScan:
        case T_TidScan:
        case T_FunctionScan:
        case T_ValuesScan:
        case T_RecursiveUnion:
        case T_CteScan:
        case T_ModifyTable: {
            break;
        }

        case T_BitmapAnd: {
            ListCell* lc = NULL;
            foreach (lc, ((BitmapAnd*)planNode)->bitmapplans) {
                ModifyWorktableWtParam((Node*)lfirst(lc), oldWtParam, newWtParam);
            }
            break;
        }

        case T_BitmapOr: {
            ListCell* lc = NULL;
            foreach (lc, ((BitmapOr*)planNode)->bitmapplans) {
                ModifyWorktableWtParam((Node*)lfirst(lc), oldWtParam, newWtParam);
            }
            break;
        }

        case T_CStoreIndexAnd: {
            ListCell* lc = NULL;
            foreach (lc, ((CStoreIndexAnd*)planNode)->bitmapplans) {
                ModifyWorktableWtParam((Node*)lfirst(lc), oldWtParam, newWtParam);
            }
            break;
        }

        case T_CStoreIndexOr: {
            ListCell* lc = NULL;
            foreach (lc, ((CStoreIndexOr*)planNode)->bitmapplans) {
                ModifyWorktableWtParam((Node*)lfirst(lc), oldWtParam, newWtParam);
            }
            break;
        }

        case T_Append: {
            Append* appendPlan = (Append*)planNode;
            ListCell* lc = NULL;
            foreach (lc, appendPlan->appendplans) {
                Plan* subPlan = (Plan*)lfirst(lc);
                ModifyWorktableWtParam((Node*)subPlan, oldWtParam, newWtParam);
            }
            break;
        }

        case T_MergeAppend: {
            MergeAppend* mgAppendPlan = (MergeAppend*)planNode;
            ListCell* lc = NULL;
            foreach (lc, mgAppendPlan->mergeplans) {
                Plan* subPlan = (Plan*)lfirst(lc);
                ModifyWorktableWtParam((Node*)subPlan, oldWtParam, newWtParam);
            }
            break;
        }

        case T_HashJoin:
        case T_NestLoop:
        case T_MergeJoin: {
            Plan* plan = &((Join*)planNode)->plan;
            ModifyWorktableWtParam((Node*)outerPlan(plan), oldWtParam, newWtParam);
            ModifyWorktableWtParam((Node*)innerPlan(plan), oldWtParam, newWtParam);
            break;
        }

        case T_Agg:
        case T_BaseResult:
        case T_Group:
        case T_Hash:
        case T_Limit:
        case T_LockRows:
        case T_Material:
        case T_PartIterator:
        case T_SetOp:
        case T_Sort:
        case T_Stream:
        case T_Unique:
        case T_WindowAgg: {
            ModifyWorktableWtParam((Node*)((Plan*)planNode)->lefttree, oldWtParam, newWtParam);
            break;
        }

        case T_SubqueryScan: {
            ModifyWorktableWtParam((Node*)((SubqueryScan*)planNode)->subplan, oldWtParam, newWtParam);
            break;
        }

#ifdef PGXC
        case T_RemoteQuery: {
            break;
        }
#endif
        default: {
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("unrecognized node type: %d when modify worktable wtParam.", (int)nodeTag(planNode))));
            break;
        }
    }

    /* Modify extParam and allParam of plan related to wtParam. */
    Plan* plan = (Plan*)planNode;
    if (bms_is_member(oldWtParam, plan->extParam)) {
        plan->extParam = bms_del_member(plan->extParam, oldWtParam);
        plan->extParam = bms_add_member(plan->extParam, newWtParam);
        plan->allParam = bms_del_member(plan->allParam, oldWtParam);
        plan->allParam = bms_add_member(plan->allParam, newWtParam);
    }

    return;
}

/*
 * create_ctescan_plan
 *	 Returns a ctescan plan for the base relation scanned by 'best_path'
 *	 with restriction clauses 'scan_clauses' and targetlist 'tlist'.
 */
static Plan* create_ctescan_plan(PlannerInfo* root, Path* best_path, List* tlist, List* scan_clauses)
{
    CteScan* scan_plan = NULL;
    Index scan_relid = best_path->parent->relid;
    RangeTblEntry* rte = NULL;
    SubPlan* ctesplan = NULL;
    int plan_id;
    int cte_param_id;
    PlannerInfo* cteroot = NULL;
    Index levelsup;
    int ndx;
    ListCell* lc = NULL;
    Plan* cte_plan = NULL;
    CommonTableExpr* cte = NULL;

    Assert(scan_relid > 0);
    rte = planner_rt_fetch(scan_relid, root);
    Assert(rte->rtekind == RTE_CTE);
    Assert(!rte->self_reference);

    /*
     * Find the referenced CTE, and locate the SubPlan previously made for it.
     */
    levelsup = rte->ctelevelsup;
    cteroot = root;
    while (levelsup-- > 0) {
        cteroot = cteroot->parent_root;
        if (cteroot == NULL) { /* shouldn't happen */
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                    errmsg("bad levelsup for CTE \"%s\"", rte->ctename)));
        }
    }

    /*
     * Note: cte_plan_ids can be shorter than cteList, if we are still working
     * on planning the CTEs (ie, this is a side-reference from another CTE).
     * So we mustn't use forboth here.
     */
    ndx = 0;
    foreach (lc, cteroot->parse->cteList) {
        cte = (CommonTableExpr*)lfirst(lc);

        if (strcmp(cte->ctename, rte->ctename) == 0)
            break;

        ndx++;
    }
    if (lc == NULL) { /* shouldn't happen */
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                errmsg("could not find CTE \"%s\"", rte->ctename)));
    }
    if (ndx >= list_length(cteroot->cte_plan_ids))
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                (errmsg("could not find plan for CTE \"%s\"", rte->ctename))));

    if (STREAM_RECURSIVECTE_SUPPORTED) {
        /*
         * We do ctescan plan separately when there is more than one refereneces in
         * Stream mode, so to locate the subplan node we need specially processed
         * here, see more comments in SS_process_ctes()
         */
        for (int i = 0; i < cte->cterefcount; i++) {
            plan_id = list_nth_int(cteroot->cte_plan_ids, ndx + i);
            RecursiveUnion* ru_plan = (RecursiveUnion*)list_nth(cteroot->glob->subplans, plan_id - 1);

            if (!ru_plan->is_used) {
                ru_plan->is_used = true;
                break;
            }

            plan_id = 0;
        }

        /* When we can't find valid unused plan, we should copy a base plan, and change the
         * plan id and param id. This case can happen when the subquery that contains the
         * recursive cte is copied multiple times, and we don't increase the refcount of recursive
         * cte accordingly. The reason that we don't modify the refcount is because the copy
         * can be happend in the subquery, and we use refcount to generate plan is at the
         * current level before it. With current planner logic, we can't solve the refcount problem
         * completely. */
        if (plan_id == 0) {
            /* find the first recursive cte plan */
            int orig_plan_id = list_nth_int(cteroot->cte_plan_ids, ndx);
            RecursiveUnion* ru_plan = (RecursiveUnion*)list_nth(cteroot->glob->subplans, orig_plan_id - 1);
            PlannerInfo* ru_root = (PlannerInfo*)list_nth(cteroot->glob->subroots, orig_plan_id - 1);
            foreach (lc, cteroot->init_plans) {
                ctesplan = (SubPlan*)lfirst(lc);
                if (ctesplan->plan_id == orig_plan_id)
                    break;
            }

            /* copy the plan and change plan and param id, then add to global plans list */
            ctesplan = (SubPlan*)copyObject(ctesplan);
            ru_plan = (RecursiveUnion*)copyObject(ru_plan);

            cteroot->init_plans = lappend(cteroot->init_plans, ctesplan);
            cteroot->glob->subplans = lappend(cteroot->glob->subplans, ru_plan);
            cteroot->glob->subroots = lappend(cteroot->glob->subroots, ru_root);
            plan_id = list_length(cteroot->glob->subplans);
            ctesplan->plan_id = plan_id;
            ctesplan->setParam = list_make1_int(SS_assign_special_param(cteroot));
            cteroot->cte_plan_ids = lappend_int(cteroot->cte_plan_ids, ctesplan->plan_id);

            /*
             * The lower-layer worktable operator of the copied RU is the same as that
             * of the original RU. We need to distinguish them and allocate a new wtParam
             * to the copied RU. Otherwise, the scanning result is incorrect because the
             * ExecWorkTableScan interface shares the worktable.
             */
            int oldWtParam = ru_plan->wtParam;
            int newWtParam = SS_assign_special_param(cteroot);
            ru_plan->wtParam = newWtParam;
            ModifyWorktableWtParam((Node*)ru_plan->plan.lefttree, oldWtParam, newWtParam);
            ModifyWorktableWtParam((Node*)ru_plan->plan.righttree, oldWtParam, newWtParam);
            if (bms_is_member(oldWtParam, ru_plan->plan.extParam)) {
                ru_plan->plan.extParam = bms_del_member(ru_plan->plan.extParam, oldWtParam);
                ru_plan->plan.extParam = bms_add_member(ru_plan->plan.extParam, newWtParam);
                ru_plan->plan.allParam = bms_del_member(ru_plan->plan.allParam, oldWtParam);
                ru_plan->plan.allParam = bms_add_member(ru_plan->plan.allParam, newWtParam);
            }
        }
    } else {
        plan_id = list_nth_int(cteroot->cte_plan_ids, ndx);
    }

    Assert(plan_id > 0);

    foreach (lc, cteroot->init_plans) {
        ctesplan = (SubPlan*)lfirst(lc);
        if (ctesplan->plan_id == plan_id)
            break;
    }
    if (lc == NULL) /* shouldn't happen */
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                (errmsg("could not find plan for CTE \"%s\"", rte->ctename))));

    /*
     * We need the CTE param ID, which is the sole member of the SubPlan's
     * setParam list.
     */
    cte_param_id = linitial_int(ctesplan->setParam);

    /* Sort clauses into best execution order */
    scan_clauses = order_qual_clauses(root, scan_clauses);

    /* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
    scan_clauses = extract_actual_clauses(scan_clauses, false);

    scan_plan = make_ctescan(tlist, scan_clauses, scan_relid, plan_id, cte_param_id);
    scan_plan->cteRef = cte;

#ifdef STREAMPLAN
    cte_plan = (Plan*)list_nth(root->glob->subplans, ctesplan->plan_id - 1);

    Plan* ctescan = (Plan*)scan_plan;
    if (cte_plan != NULL) {
        inherit_plan_locator_info(ctescan, cte_plan);
        ctescan->distributed_keys = best_path->parent->distribute_keys;
    }

    if (STREAM_RECURSIVECTE_SUPPORTED) {
        RecursiveUnion* recursive_union_plan = (RecursiveUnion*)cte_plan;

        /* Binding recursive union plan to CteScan */
        scan_plan->subplan = recursive_union_plan;
        if (recursive_union_plan == NULL) {
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                    errmsg("recursive_union_plan can not be NULL")));
        }

        /*
         * In order to keep recursive union can be safely executed in recursive-control context,
         * we need the exec_nodes of CteScan's nodeList can cover all its underlying plan nodes'
         * nodeList, in order to do so, if we found the target CteScan node is a replicated plan
         * we may need add a hashfilter on top of it to avoid DN-pruning happend on CteScan layer
         * to eliminate out a case where an incomplete recursive DN executing scope
         *
         * But, if RecursiveUnion is correlated with a upper layer's range table rel, or no need
         * do distributed recursive control(both lplan, rplan is no stream) we don't add hashfilter,
         * as in such cases DN pruning is safe
         */
        if (is_replicated_plan((Plan*)scan_plan) &&
            !(recursive_union_plan->is_correlated || (root->is_correlated && cte->cterecursive)) &&
            (recursive_union_plan->has_inner_stream || recursive_union_plan->has_outer_stream)) {
            Plan* output_plan = (Plan*)scan_plan;
            ListCell* lc2 = NULL;

            foreach (lc2, output_plan->targetlist) {
                TargetEntry* tle = (TargetEntry*)lfirst(lc2);

                Node* node = (Node*)tle->expr;
                if (IsTypeDistributable(exprType(node))) {
                    if (add_hashfilter_for_replication(root, output_plan, list_make1(node))) {
                        output_plan->total_cost += PLAN_LOCAL_ROWS(output_plan) * g_instance.cost_cxt.cpu_hash_cost;
                        break;
                    }
                }
            }

            /* can't choose a valid distribute key, so disable stream plan */
            if (lc2 == NULL) {
                errno_t sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
                    NOTPLANSHIPPING_LENGTH,
                    "Recursive Cte is going to be pruned but no hashfilter is not found");
                securec_check_ss_c(sprintf_rc, "\0", "\0");
                mark_stream_unsupport();
            }
        }

        /*
         * If recursive has Stream operator in inner side, add a
         * LOCAL GATHER operator upon CteScan, thus recursive steps
         * can run independently. So join operators which return early due to
         * no data of one side will not lead to recursive sync steps hang.
         */
        if (recursive_union_plan->has_inner_stream && !recursive_union_plan->is_correlated) {
            Stream* stream = makeNode(Stream);

            stream->type = STREAM_REDISTRIBUTE;
            stream->is_sorted = false;
            stream->sort = NULL;
            stream->smpDesc.consumerDop = 1;
            stream->smpDesc.producerDop = 1;
            stream->smpDesc.distriType = LOCAL_ROUNDROBIN;
            stream->origin_consumer_nodes = NULL;
            stream->distribute_keys = NIL;
            stream->consumer_nodes = (ExecNodes*)copyObject(ctescan->exec_nodes);
            stream->is_recursive_local = true;

            Plan* plan = &stream->scan.plan;
            plan->dop = stream->smpDesc.consumerDop;
            plan->targetlist = ctescan->targetlist;
            plan->lefttree = ctescan;
            plan->righttree = NULL;
            inherit_plan_locator_info(plan, ctescan);
            copy_path_costsize(&scan_plan->scan.plan, best_path);
            copy_plan_costsize(plan, ctescan);

            return plan;
        }
    }

#endif

    copy_path_costsize(&scan_plan->scan.plan, best_path);

    if (IsCteScanProcessForStartWith(scan_plan)) {
        CteScan *cteplan = (CteScan *)scan_plan;
        RecursiveUnion *ruplan = (RecursiveUnion *)cte_plan;

        Plan *plan = (Plan *)AddStartWithOpProcNode(root, cteplan, ruplan);
        return plan;
    }

    return (Plan*)scan_plan;
}

/*
 * create_resultscan_plan
 *      Returns a Result plan for the RTE_RESULT base relation scanned by
 *     'best_path' with restriction clauses 'scan_clauses' and targetlist
 *     'tlist'.
 */
static BaseResult *create_resultscan_plan(PlannerInfo *root, Path *best_path,
    List *tlist, List *scan_clauses)
{
    BaseResult     *scan_plan;
    Index           scan_relid = best_path->parent->relid;
    RangeTblEntry *rte PG_USED_FOR_ASSERTS_ONLY;

    Assert(scan_relid > 0);
    rte = planner_rt_fetch(scan_relid, root);
    Assert(rte->rtekind == RTE_RESULT);

    /* Sort clauses into best execution order */
    scan_clauses = order_qual_clauses(root, scan_clauses);

    /* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
    scan_clauses = extract_actual_clauses(scan_clauses, false);

    /* Replace any outer-relation variables with nestloop params */
    if (best_path->param_info) {
        scan_clauses = (List *)
        replace_nestloop_params(root, (Node *) scan_clauses);
    }

    scan_plan = make_result(root, tlist, (Node *) scan_clauses, NULL);

    copy_generic_path_info(&scan_plan->plan, best_path);

    return scan_plan;
}

/*
 * create_worktablescan_plan
 *	 Returns a worktablescan plan for the base relation scanned by 'best_path'
 *	 with restriction clauses 'scan_clauses' and targetlist 'tlist'.
 */
static WorkTableScan* create_worktablescan_plan(PlannerInfo* root, Path* best_path, List* tlist, List* scan_clauses)
{
    WorkTableScan* scan_plan = NULL;
    Index scan_relid = best_path->parent->relid;
    RangeTblEntry* rte = NULL;
    Index levelsup;
    PlannerInfo* cteroot = NULL;

    Assert(scan_relid > 0);
    rte = planner_rt_fetch(scan_relid, root);
    Assert(rte->rtekind == RTE_CTE);
    Assert(rte->self_reference);

    /*
     * We need to find the worktable param ID, which is in the plan level
     * that's processing the recursive UNION, which is one level *below* where
     * the CTE comes from.
     */
    levelsup = rte->ctelevelsup;
    if (levelsup == 0) { /* shouldn't happen */
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                errmsg("bad levelsup for CTE \"%s\"", rte->ctename)));
    }
    levelsup--;
    cteroot = root;
    while (levelsup-- > 0) {
        cteroot = cteroot->parent_root;
        if (cteroot == NULL) { /* shouldn't happen */
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                    errmsg("bad levelsup for CTE \"%s\"", rte->ctename)));
        }
    }
    if (cteroot->wt_param_id < 0) /* shouldn't happen */
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                (errmsg("could not find param ID for CTE \"%s\"", rte->ctename))));

    /* Sort clauses into best execution order */
    scan_clauses = order_qual_clauses(root, scan_clauses);

    /* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
    scan_clauses = extract_actual_clauses(scan_clauses, false);

    scan_plan = make_worktablescan(tlist, scan_clauses, scan_relid, cteroot->wt_param_id);

#ifdef STREAMPLAN
    if (STREAM_RECURSIVECTE_SUPPORTED) {
        /* Need revisit */
        scan_plan->scan.plan.exec_nodes = ng_get_installation_group_exec_node();
        scan_plan->scan.plan.exec_nodes->baselocatortype = best_path->locator_type;
    } else {
        /* For a self-reference CTE, it could not be push down to DN */
        scan_plan->scan.plan.exec_nodes = ng_get_single_node_group_exec_node();
    }
#endif

    /* Mark current worktablescan is working for startwith */
    if (IsRteForStartWith(root, rte)) {
        rte->swConverted = true;
        scan_plan->forStartWith = true;
    }

    copy_path_costsize(&scan_plan->scan.plan, best_path);

    return scan_plan;
}

/* build one column at least for count(*) */
static List* build_one_column_tlist(PlannerInfo* root, RelOptInfo* rel)
{
    List* tlist = NIL;
    Index varno = rel->relid;
    RangeTblEntry* rte = planner_rt_fetch(varno, root);

    switch (rte->rtekind) {
        case RTE_RELATION: {
              
              Expr *colexpr = NULL;
              /* append plan need a real tlist for later mark_distribute_setop() */
              if (u_sess->opt_cxt.is_under_append_plan) {
                  Relation relation = relation_open(rte->relid, AccessShareLock);
                  TupleDesc tupdesc = relation->rd_att;
                  int maxattrs = tupdesc->natts;
                  int varattno = 0;
                  Var* varnode = NULL;

                  for (varattno = 0; varattno < maxattrs; varattno++) {
                      Form_pg_attribute attr = tupdesc->attrs[varattno];

                      if (attr->attisdropped || !IsTypeDistributable(attr->atttypid)) {
                          continue;
                      }

                      varnode = makeVar(varno, attr->attnum, attr->atttypid, attr->atttypmod, attr->attcollation, 0);
                      varnode->location = -1;
                      colexpr = (Expr *)varnode;
                      break;
                  }

                  relation_close(relation, AccessShareLock);

              } 

              /* otherwise, use a Dummy tlist */
              if (colexpr == NULL) {
                  colexpr = (Expr *)make_const(NULL, makeString("Dummy"), 0);
              }

              tlist = lappend(tlist,
                          makeTargetEntry(colexpr, 1, NULL, true));

            break;
        }
        default:
            /* caller error */
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                    (errmsg("unsupported RTE kind %d in build_one_column_tlist", (int)rte->rtekind))));
            break;
    }

    return tlist;
}

/*
 * create_foreignscan_plan
 *	 Returns a foreignscan plan for the base relation scanned by 'best_path'
 *	 with restriction clauses 'scan_clauses' and targetlist 'tlist'.
 */
static ForeignScan* create_foreignscan_plan(PlannerInfo* root, ForeignPath* best_path, List* tlist, List* scan_clauses)
{
    ExecNodes* exec_nodes = NULL;
    ForeignScan* scan_plan = NULL;
    RelOptInfo* rel = best_path->path.parent;
    Index scan_relid = rel->relid;
    RangeTblEntry* rte = NULL;
    RangeTblEntry* target_rte = NULL;
    int i;

    /* it should be a base rel... */
    Assert(scan_relid > 0);
    Assert(rel->rtekind == RTE_RELATION);
    rte = planner_rt_fetch(scan_relid, root);
    Assert(rte->rtekind == RTE_RELATION);

    /*
     * Sort clauses into best execution order.	We do this first since the FDW
     * might have more info than we do and wish to adjust the ordering.
     */
    scan_clauses = order_qual_clauses(root, scan_clauses);

    Assert(root->parse != NULL);

    /*
     * In order to support error table in multi-nodegroup situation,
     * execute foreign scan only on DNs where the insert-targeted table exists.
     * First, find the target table.
     * For now, error table only support insert statement.
     */
    target_rte = rte;
    if (root->parse->commandType == CMD_INSERT) {
        // Confirm whether exists error table.
        DefElem* def = GetForeignTableOptionByName(rte->relid, optErrorRel);
        if (def != NULL) {
            /* do not support HDFS error table with multi-nodegroup yet */
            if (rel->fdwroutine->GetFdwType && rel->fdwroutine->GetFdwType() == HDFS_ORC) {
                ereport(DEBUG1,
                    (errmodule(MOD_OPT), (errmsg("DFS Error table is not surpported with multi-nodegroup yet."))));
            } else {
                ListCell* lc = NULL;
                foreach (lc, root->parse->rtable) {
                    // search for the insert-targeted table.
                    target_rte = (RangeTblEntry*)lfirst(lc);
                    if (target_rte->rtekind == RTE_RELATION)
                        break;
                }
            }
        }
    }

    Assert(rel->fdwroutine != NULL);

    /*
     * Let the FDW perform its processing on the restriction clauses and
     * generate the plan node.	Note that the FDW might remove restriction
     * clauses that it intends to execute remotely, or even add more (if it
     * has selected some join clauses for remote use but also wants them
     * rechecked locally).
     * Assign task to the datanodes where target table exists so that
     * the error information will be saved only in these nodes.
     */
    scan_plan = rel->fdwroutine->GetForeignPlan(root, rel, target_rte->relid, best_path, tlist, scan_clauses);
    scan_plan->scan_relid = rte->relid;

    char locator_type = GetLocatorType(rte->relid);

    /*
     * In order to support error table in multi-nodegroup situation,
     * execute foreign scan only on DNs where the insert-targeted table exists.
     * Secondly, find the DNs where the target table lies and set them as the exec_nodes.
     */
    exec_nodes = GetRelationNodesByQuals(
        (void*)root->parse, target_rte->relid, scan_relid, (Node*)scan_clauses, RELATION_ACCESS_READ, NULL);

    if (exec_nodes == NULL) {
        /* foreign scan is executed on installation group */
        exec_nodes = ng_get_installation_group_exec_node();
    }
    scan_plan->scan.plan.exec_nodes = exec_nodes;

    if (locator_type == LOCATOR_TYPE_NONE)
        exec_nodes->baselocatortype = LOCATOR_TYPE_HASH;
    else {
        /* use the same logic as in seqscan */
        if (locator_type == LOCATOR_TYPE_REPLICATED)
            ereport(DEBUG1,
                (errmodule(MOD_OPT),
                    (errmsg("For replicated table, foreign scan will be excuted on all table-exist DNs."))));
        else
            exec_nodes->baselocatortype = best_path->path.locator_type;

        scan_plan->scan.plan.exec_type = EXEC_ON_DATANODES;
        scan_plan->scan.plan.distributed_keys = best_path->path.parent->distribute_keys;
    }

    /* Copy cost data from Path to Plan; no need to make FDW do this */
    copy_path_costsize(&scan_plan->scan.plan, &best_path->path);

    /*
     * Replace any outer-relation variables with nestloop params in the qual
     * and fdw_exprs expressions.  We do this last so that the FDW doesn't
     * have to be involved.  (Note that parts of fdw_exprs could have come
     * from join clauses, so doing this beforehand on the scan_clauses
     * wouldn't work.)
     */
    if (best_path->path.param_info) {
        scan_plan->scan.plan.qual = (List*)replace_nestloop_params(root, (Node*)scan_plan->scan.plan.qual);
        scan_plan->fdw_exprs = (List*)replace_nestloop_params(root, (Node*)scan_plan->fdw_exprs);
    }

    /*
     * Detect whether any system columns are requested from rel.  This is a
     * bit of a kluge and might go away someday, so we intentionally leave it
     * out of the API presented to FDWs.
     */
    scan_plan->fsSystemCol = false;
    for (i = rel->min_attr; i < 0; i++) {
        if (!bms_is_empty(rel->attr_needed[i - rel->min_attr])) {
            scan_plan->fsSystemCol = true;
            break;
        }
    }

    return scan_plan;
}

/*
 * create_extensible_plan
 *
 * Transform a ExtensiblePath into a Plan.
 */
static ExtensiblePlan* create_extensible_plan(
    PlannerInfo* root, ExtensiblePath* best_path, List* tlist, List* scan_clauses)
{
    ExtensiblePlan* eplan = NULL;
    RelOptInfo* rel = best_path->path.parent;
    List* extensible_plans = NIL;
    ListCell* lc = NULL;

    /* Recursively transform child paths. */
    foreach (lc, best_path->extensible_paths) {
        Plan* plan = create_plan_recurse(root, (Path*)lfirst(lc));

        extensible_plans = lappend(extensible_plans, plan);
    }

    /*
     * Sort clauses into the best execution order, although extensible-scan
     * provider can reorder them again.
     */
    scan_clauses = order_qual_clauses(root, scan_clauses);

    /*
     * Invoke extensible plan provider to create the Plan node represented by the
     * ExtensiblePath.
     */
    eplan = (ExtensiblePlan*)best_path->methods->PlanExtensiblePath(
        root, rel, best_path, tlist, scan_clauses, extensible_plans);

    /* Likewise, copy the relids that are represented by this extensible scan */
    eplan->extensible_relids = best_path->path.parent->relids;

    /*
     * Copy cost data from Path to Plan; no need to make extensible-plan providers
     * do this
     */
    copy_path_costsize(&eplan->scan.plan, &best_path->path);

    /*
     * Replace any outer-relation variables with nestloop params in the qual
     * and extensible_exprs expressions.  We do this last so that the extensible-plan
     * provider doesn't have to be involved.  (Note that parts of extensible_exprs
     * could have come from join clauses, so doing this beforehand on the
     * scan_clauses wouldn't work.)  We assume extensible_scan_tlist contains no
     * such variables.
     */
    if (best_path->path.param_info) {
        eplan->extensible_exprs = (List*)replace_nestloop_params(root, (Node*)eplan->extensible_exprs);

        eplan->scan.plan.qual = (List*)replace_nestloop_params(root, (Node*)eplan->scan.plan.qual);
    }

    return eplan;
}

/*****************************************************************************
 *
 *	JOIN METHODS
 *****************************************************************************/
static NestLoop* create_nestloop_plan(PlannerInfo* root, NestPath* best_path, Plan* outer_plan, Plan* inner_plan)
{
    NestLoop* join_plan = NULL;
    List* tlist = build_relation_tlist(best_path->path.parent);
    List* joinrestrictclauses = best_path->joinrestrictinfo;
    List* joinclauses = NIL;
    List* otherclauses = NIL;
    Relids outerrelids;
    List* nestParams = NIL;
    ListCell* cell = NULL;
    ListCell* prev = NULL;
    ListCell* next = NULL;

    /* Sort join qual clauses into best execution order */
    joinrestrictclauses = order_qual_clauses(root, joinrestrictclauses);

    /* Get the join qual clauses (in plain expression form) */
    /* Any pseudoconstant clauses are ignored here */
    if (IS_OUTER_JOIN(best_path->jointype)) {
        extract_actual_join_clauses(joinrestrictclauses, &joinclauses, &otherclauses);
    } else {
        /* We can treat all clauses alike for an inner join */
        joinclauses = extract_actual_clauses(joinrestrictclauses, false);
        otherclauses = NIL;
    }

    /* Replace any outer-relation variables with nestloop params */
    if (best_path->path.param_info) {
        joinclauses = (List*)replace_nestloop_params(root, (Node*)joinclauses);
        otherclauses = (List*)replace_nestloop_params(root, (Node*)otherclauses);
    }

    /*
     * Identify any nestloop parameters that should be supplied by this join
     * node, and move them from root->curOuterParams to the nestParams list.
     */
    outerrelids = best_path->outerjoinpath->parent->relids;
    nestParams = NIL;
    prev = NULL;
    for (cell = list_head(root->curOuterParams); cell; cell = next) {
        NestLoopParam* nlp = (NestLoopParam*)lfirst(cell);

        next = lnext(cell);
        if (IsA(nlp->paramval, Var) && bms_is_member(nlp->paramval->varno, outerrelids)) {
            root->curOuterParams = list_delete_cell(root->curOuterParams, cell, prev);
            nestParams = lappend(nestParams, nlp);
        } else if (IsA(nlp->paramval, PlaceHolderVar) &&
                   bms_overlap(((PlaceHolderVar*)nlp->paramval)->phrels, outerrelids) &&
                   bms_is_subset(
                       find_placeholder_info(root, (PlaceHolderVar*)nlp->paramval, false)->ph_eval_at, outerrelids)) {
            root->curOuterParams = list_delete_cell(root->curOuterParams, cell, prev);
            nestParams = lappend(nestParams, nlp);
        } else
            prev = cell;
    }
#ifdef STREAMPLAN
    if (IS_STREAM_PLAN) {
        if (contain_special_plan_node(inner_plan, T_Stream) || contain_special_plan_node(inner_plan, T_ForeignScan)) {
            if (!IsA(inner_plan, Material)) {
                inner_plan = (Plan*)make_material(inner_plan);
                /*
                 * We assume the materialize will not spill to disk, and therefore
                 * charge just cpu_operator_cost per tuple.  (Keep this estimate in
                 * sync with final_cost_mergejoin.)
                 */
                copy_path_costsize(inner_plan, best_path->innerjoinpath);
                (void)cost_rescan_material(PLAN_LOCAL_ROWS(inner_plan),
                    get_plan_actual_total_width(inner_plan, root->glob->vectorized, OP_SORT),
                    &((Material*)inner_plan)->mem_info,
                    root->glob->vectorized,
                    SET_DOP(inner_plan->dop));
                inner_plan->total_cost += u_sess->attr.attr_sql.cpu_operator_cost * PLAN_LOCAL_ROWS(inner_plan);
            }
        }
    }
#endif

    join_plan =
        make_nestloop(tlist, joinclauses, otherclauses, nestParams, outer_plan, inner_plan, best_path->jointype);

    /*
     * @hdfs
     * Determine whether optimize plan by using informatioanal constraint.
     * The semi join and anti join are not suitable for informatioanal constraint.
     */
    if (SATISFY_INFORMATIONAL_CONSTRAINT(join_plan, best_path->jointype)) {
        join_plan->join.optimizable =
            useInformationalConstraint(root, joinclauses, best_path->innerjoinpath->parent->relids);
    }

    /* if we allow null = null in multi-count-distinct case, change joinqual */
    if (root->join_null_info)
        join_plan->join.nulleqqual = make_null_eq_clause(NIL, &join_plan->join.joinqual, root->join_null_info);

    return join_plan;
}

static MergeJoin* create_mergejoin_plan(PlannerInfo* root, MergePath* best_path, Plan* outer_plan, Plan* inner_plan)
{
    List* tlist = build_relation_tlist(best_path->jpath.path.parent);
    List* joinclauses = NIL;
    List* otherclauses = NIL;
    List* mergeclauses = NIL;
    List* outerpathkeys = NIL;
    List* innerpathkeys = NIL;
    int nClauses;
    Oid* mergefamilies = NULL;
    Oid* mergecollations = NULL;
    int* mergestrategies = NULL;
    bool* mergenullsfirst = NULL;
    PathKey* opathkey = NULL;
    EquivalenceClass* opeclass = NULL;
    MergeJoin* join_plan = NULL;
    int i;
    ListCell* lc = NULL;
    ListCell* lop = NULL;
    ListCell* lip = NULL;

    /* Sort join qual clauses into best execution order */
    /* NB: do NOT reorder the mergeclauses */
    joinclauses = order_qual_clauses(root, best_path->jpath.joinrestrictinfo);

    /* Get the join qual clauses (in plain expression form) */
    /* Any pseudoconstant clauses are ignored here */
    if (IS_OUTER_JOIN((uint32)(best_path->jpath.jointype))) {
        extract_actual_join_clauses(joinclauses, &joinclauses, &otherclauses);
    } else {
        /* We can treat all clauses alike for an inner join */
        joinclauses = extract_actual_clauses(joinclauses, false);
        otherclauses = NIL;
    }

    /*
     * Remove the mergeclauses from the list of join qual clauses, leaving the
     * list of quals that must be checked as qpquals.
     */
    mergeclauses = get_actual_clauses(best_path->path_mergeclauses);
    joinclauses = list_difference(joinclauses, mergeclauses);

    /*
     * Replace any outer-relation variables with nestloop params.  There
     * should not be any in the mergeclauses.
     */
    if (best_path->jpath.path.param_info) {
        joinclauses = (List*)replace_nestloop_params(root, (Node*)joinclauses);
        otherclauses = (List*)replace_nestloop_params(root, (Node*)otherclauses);
    }

    /*
     * Rearrange mergeclauses, if needed, so that the outer variable is always
     * on the left; mark the mergeclause restrictinfos with correct
     * outer_is_left status.
     */
    mergeclauses = get_switched_clauses(best_path->path_mergeclauses, best_path->jpath.outerjoinpath->parent->relids);

    /*
     * Create explicit sort nodes for the outer and inner paths if necessary.
     * Make sure there are no excess columns in the inputs if sorting.
     */
    if (best_path->outersortkeys) {
        disuse_physical_tlist(outer_plan, best_path->jpath.outerjoinpath);
        outer_plan = (Plan*)make_sort_from_pathkeys(root, outer_plan, best_path->outersortkeys, -1.0);
        copy_mem_info(&((Sort*)outer_plan)->mem_info, &best_path->outer_mem_info);

#ifdef PGXC
        if (IS_PGXC_COORDINATOR && !IsConnFromCoord() && !IS_STREAM)
            outer_plan = (Plan*)create_remotesort_plan(root, outer_plan, best_path->outersortkeys);
#endif /* PGXC */
        outerpathkeys = best_path->outersortkeys;
    } else {
        outerpathkeys = best_path->jpath.outerjoinpath->pathkeys;
#ifdef STREAMPLAN
        if (IS_STREAM_PLAN && IsA(outer_plan, Stream)) {
            outer_plan = (Plan*)make_sort_from_pathkeys(root, outer_plan, outerpathkeys, -1.0);
            copy_mem_info(&((Sort*)outer_plan)->mem_info, &best_path->outer_mem_info);
        }
#endif
    }

    if (best_path->innersortkeys) {
        disuse_physical_tlist(inner_plan, best_path->jpath.innerjoinpath);
        inner_plan = (Plan*)make_sort_from_pathkeys(root, inner_plan, best_path->innersortkeys, -1.0);
        copy_mem_info(&((Sort*)inner_plan)->mem_info, &best_path->inner_mem_info);

#ifdef PGXC
        if (IS_PGXC_COORDINATOR && !IsConnFromCoord() && !IS_STREAM) {
            inner_plan = (Plan*)create_remotesort_plan(root, inner_plan, best_path->innersortkeys);
            /* If Sort node is not needed on top of RemoteQuery node, we
             * will need to materialize the datanode result so that
             * mark/restore on the inner node can be handled.
             * We shouldn't be changing the members in path structure while
             * creating plan, but changing the one below isn't harmful.
             */
            if (IsA(inner_plan, RemoteQuery))
                best_path->materialize_inner = true;
        }
#endif /* PGXC */
        innerpathkeys = best_path->innersortkeys;
    } else {
        innerpathkeys = best_path->jpath.innerjoinpath->pathkeys;
#ifdef STREAMPLAN
        if (IS_STREAM_PLAN && IsA(inner_plan, Stream)) {
            inner_plan = (Plan*)make_sort_from_pathkeys(root, inner_plan, innerpathkeys, -1.0);
            copy_mem_info(&((Sort*)inner_plan)->mem_info, &best_path->inner_mem_info);
        }
#endif
    }

    /*
     * If specified, add a materialize node to shield the inner plan from the
     * need to handle mark/restore.
     */
    if (best_path->materialize_inner) {
        Plan* matplan = (Plan*)make_material(inner_plan);

        /*
         * We assume the materialize will not spill to disk, and therefore
         * charge just cpu_operator_cost per tuple.  (Keep this estimate in
         * sync with final_cost_mergejoin.)
         */
        copy_plan_costsize(matplan, inner_plan);
        copy_mem_info(&((Material*)matplan)->mem_info, &best_path->mat_mem_info);
        matplan->total_cost += u_sess->attr.attr_sql.cpu_operator_cost * PLAN_LOCAL_ROWS(matplan);

        inner_plan = matplan;
    }

    /*
     * Compute the opfamily/collation/strategy/nullsfirst arrays needed by the
     * executor.  The information is in the pathkeys for the two inputs, but
     * we need to be careful about the possibility of mergeclauses sharing a
     * pathkey, as well as the possibility that the inner pathkeys are not in
     * an order matching the mergeclauses.
     */
    nClauses = list_length(mergeclauses);
    Assert(nClauses == list_length(best_path->path_mergeclauses));
    mergefamilies = (Oid*)palloc(nClauses * sizeof(Oid));
    mergecollations = (Oid*)palloc(nClauses * sizeof(Oid));
    mergestrategies = (int*)palloc(nClauses * sizeof(int));
    mergenullsfirst = (bool*)palloc(nClauses * sizeof(bool));

    lop = list_head(outerpathkeys);
    lip = list_head(innerpathkeys);
    i = 0;
    foreach (lc, best_path->path_mergeclauses) {
        RestrictInfo* rinfo = (RestrictInfo*)lfirst(lc);
        EquivalenceClass* oeclass = NULL;
        EquivalenceClass* ieclass = NULL;
        PathKey* ipathkey = NULL;
        EquivalenceClass* ipeclass = NULL;
        bool first_inner_match = false;
        bool onewpathkey = false;
        bool inewpathkey = false;

        /* fetch outer/inner eclass from mergeclause */
        Assert(IsA(rinfo, RestrictInfo));
        if (rinfo->outer_is_left) {
            oeclass = rinfo->left_ec;
            ieclass = rinfo->right_ec;
        } else {
            oeclass = rinfo->right_ec;
            ieclass = rinfo->left_ec;
        }
        Assert(oeclass != NULL);
        Assert(ieclass != NULL);

        /*
         * We must identify the pathkey elements associated with this clause
         * by matching the eclasses (which should give a unique match, since
         * the pathkey lists should be canonical).  In typical cases the merge
         * clauses are one-to-one with the pathkeys, but when dealing with
         * partially redundant query conditions, things are more complicated.
         *
         * lop and lip reference the first as-yet-unmatched pathkey elements.
         * If they're NULL then all pathkey elements have been matched.
         *
         * The ordering of the outer pathkeys should match the mergeclauses,
         * by construction (see find_mergeclauses_for_outer_pathkeys()). There
         * could be more than one mergeclause for the same outer pathkey, but
         * no pathkey may be entirely skipped over.
         */
        if (oeclass != opeclass) {
            /* doesn't match the current opathkey, so must match the next */
            if (lop == NULL)
                ereport(ERROR,
                    (errmodule(MOD_OPT),
                        errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                        (errmsg("outer pathkeys do not match mergeclauses"))));

            opathkey = (PathKey*)lfirst(lop);
            opeclass = opathkey->pk_eclass;

            lop = lnext(lop);
            if (oeclass != opeclass)
                ereport(ERROR,
                    (errmodule(MOD_OPT),
                        errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                        (errmsg("outer pathkeys do not match mergeclauses"))));
        }

        /*
         * The inner pathkeys likewise should not have skipped-over keys, but
         * it's possible for a mergeclause to reference some earlier inner
         * pathkey if we had redundant pathkeys.  For example we might have
         * mergeclauses like "o.a = i.x AND o.b = i.y AND o.c = i.x".  The
         * implied inner ordering is then "ORDER BY x, y, x", but the pathkey
         * mechanism drops the second sort by x as redundant, and this code
         * must cope.
         *
         * It's also possible for the implied inner-rel ordering to be like
         * "ORDER BY x, y, x DESC".  We still drop the second instance of x as
         * redundant; but this means that the sort ordering of a redundant
         * inner pathkey should not be considered significant.  So we must
         * detect whether this is the first clause matching an inner pathkey.
         */
        if (lip != NULL) {
            ipathkey = (PathKey*)lfirst(lip);
            ipeclass = ipathkey->pk_eclass;
            if (ieclass == ipeclass) {
                /* successful first match to this inner pathkey */
                lip = lnext(lip);
                inewpathkey = true;
                first_inner_match = true;
            }
        }
        if (!first_inner_match) {
            /* redundant clause ... must match something before lip */
            ListCell* l2 = NULL;

            foreach (l2, innerpathkeys) {
                ipathkey = (PathKey*)lfirst(l2);
                ipeclass = ipathkey->pk_eclass;
                if (ieclass == ipeclass)
                    break;
            }
            if (ieclass != ipeclass)
                ereport(ERROR,
                    (errmodule(MOD_OPT),
                        errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                        (errmsg("inner pathkeys do not match mergeclauses"))));
        }

        /*
         * The pathkeys should always match each other as to opfamily and
         * collation (which affect equality), but if we're considering a
         * redundant inner pathkey, its sort ordering might not match.  In
         * such cases we may ignore the inner pathkey's sort ordering and use
         * the outer's.  (In effect, we're lying to the executor about the
         * sort direction of this inner column, but it does not matter since
         * the run-time row comparisons would only reach this column when
         * there's equality for the earlier column containing the same eclass.
         * There could be only one value in this column for the range of inner
         * rows having a given value in the earlier column, so it does not
         * matter which way we imagine this column to be ordered.)  But a
         * non-redundant inner pathkey had better match outer's ordering too.
         */
        if ((onewpathkey && inewpathkey && opathkey) &&
            (!OpFamilyEquals(opathkey->pk_opfamily, ipathkey->pk_opfamily) ||
                opathkey->pk_eclass->ec_collation != ipathkey->pk_eclass->ec_collation))
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                    (errmsg("left and right pathkeys do not match in mergejoin"))));

        if (first_inner_match && opathkey != NULL &&
            (opathkey->pk_strategy != ipathkey->pk_strategy || opathkey->pk_nulls_first != ipathkey->pk_nulls_first))
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                    (errmsg("left and right pathkeys do not match in mergejoin"))));

        if (opathkey) {
            /* OK, save info for executor */
            mergefamilies[i] = opathkey->pk_opfamily;
            mergecollations[i] = opathkey->pk_eclass->ec_collation;
            mergestrategies[i] = opathkey->pk_strategy;
            mergenullsfirst[i] = opathkey->pk_nulls_first;
            i++;
        } else {
            ereport(ERROR,
                (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                    errmsg("Unable to save pathkey info for executor.")));
        }
    }

    /*
     * Note: it is not an error if we have additional pathkey elements (i.e.,
     * lop or lip isn't NULL here).  The input paths might be better-sorted
     * than we need for the current mergejoin.
     */

    /*
     * Now we can build the mergejoin node.
     */
    join_plan = make_mergejoin(tlist,
        joinclauses,
        otherclauses,
        mergeclauses,
        mergefamilies,
        mergecollations,
        mergestrategies,
        mergenullsfirst,
        outer_plan,
        inner_plan,
        best_path->jpath.jointype);

    /*
     * @hdfs
     * Determine whether optimize plan by using informatioanal constraint.
     * The semi join and anti join are not suitable for informatioanal constraint.
     */
    if (SATISFY_INFORMATIONAL_CONSTRAINT(join_plan, best_path->jpath.jointype)) {
        join_plan->join.optimizable =
            useInformationalConstraint(root, mergeclauses, best_path->jpath.innerjoinpath->parent->relids);
    }

    if (root->join_null_info)
        join_plan->join.nulleqqual = make_null_eq_clause(mergeclauses, &join_plan->join.joinqual, root->join_null_info);

    return join_plan;
}

/*
 * @Description: Find this expr from targetList.
 * @in expr: Need find expr.
 * @in targetList: Source target List.
 * @return: If can find return this expr else return false.
 */
static bool find_var_from_targetlist(Expr* expr, List* targetList)
{
    if (!IsA(expr, Var)) {
        return false;
    }

    ListCell* l = NULL;
    foreach (l, targetList) {
        TargetEntry* tge = (TargetEntry*)lfirst(l);

        if (IsA(tge->expr, Var) && equal(tge->expr, expr)) {
            return true;
        }
    }

    return false;
}

/*
 * @Description: Foreach HashJoin hashclauses and set bloomfilter.
 * @in root: Per-query information for planning/optimization.
 * @in plan: Hashjoin plan.
 * @in context: Bloomfilter_context.
 */
static void search_var_and_mark_bloomfilter(PlannerInfo* root, Expr* expr, Plan* plan, bloomfilter_context* context)
{
    if (plan == NULL) {
        return;
    }

    switch (nodeTag(plan)) {
        case T_ForeignScan:
        case T_DfsScan: {
            if (IsA(plan, ForeignScan)) {
                ForeignScan* splan = (VecForeignScan*)plan;

                /* This scan is not hdfs foreign table scan. */
                if (!isObsOrHdfsTableFormTblOid(splan->scan_relid)) {
                    return;
                }
            }

            /* Find equal expr from scan plan targetlist, if found append it to scan var_list. */
            if (find_var_from_targetlist(expr, plan->targetlist)) {
                if (context->add_index) {
                    context->bloomfilter_index++;

                    /* To expr's equal class, filter index is the same. */
                    context->add_index = false;
                }

                plan->var_list = lappend(plan->var_list, copyObject(expr));
                plan->filterIndexList = lappend_int(plan->filterIndexList, context->bloomfilter_index);
            }

            break;
        }
        case T_NestLoop:
        case T_MergeJoin:
        case T_HashJoin: {
            search_var_and_mark_bloomfilter(root, expr, outerPlan(plan), context);
            search_var_and_mark_bloomfilter(root, expr, innerPlan(plan), context);

            break;
        }
        case T_Append: {
            Append* splan = (Append*)plan;
            AttrNumber attnum = 0;
            ListCell* l = NULL;

            foreach (l, splan->plan.targetlist) {
                TargetEntry* target = (TargetEntry*)lfirst(l);

                if (equal(target->expr, expr)) {
                    attnum = target->resno;
                    break;
                }
            }

            if (attnum > 0) {
                foreach (l, splan->appendplans) {
                    Plan* subplan = (Plan*)lfirst(l);

                    TargetEntry* sub_tle = (TargetEntry*)list_nth(subplan->targetlist, attnum - 1);

                    if (IsA(sub_tle->expr, Var)) {
                        /* To append, we need find this expr */
                        search_var_and_mark_bloomfilter(root, sub_tle->expr, subplan, context);
                    }
                }
            }
            break;
        }
        case T_Material:
        case T_Sort:
        case T_Unique:
        case T_SetOp:
        case T_Limit:
        case T_Group:
        case T_WindowAgg:
        case T_BaseResult: {
            search_var_and_mark_bloomfilter(root, expr, outerPlan(plan), context);
            break;
        }
        case T_Agg: {
            /* Return false if ap function is meet. */
            if (!((Agg*)plan)->groupingSets) {
                search_var_and_mark_bloomfilter(root, expr, outerPlan(plan), context);
            }
            break;
        }
        case T_SubqueryScan: {
            SubqueryScan* subqueryplan = (SubqueryScan*)plan;
            RelOptInfo* rel = NULL;
            AttrNumber attnum = 0;
            ListCell* l = NULL;

            /*  find the target var resno */
            foreach (l, subqueryplan->scan.plan.targetlist) {
                TargetEntry* target = (TargetEntry*)lfirst(l);

                if (equal(target->expr, expr)) {
                    attnum = ((Var*)expr)->varattno;
                    break;
                }
            }

            if (attnum > 0) {
                /* To subplan, we need fint it's root. */
                rel = find_base_rel(root, subqueryplan->scan.scanrelid);

                /*
                 * If the SubQuery plan is derived from inlist2join conversion, we don't
                 * apply bloomfilter optimization here, also for now inlist2join is not
                 * support HDFS, so it is OK leave it(improve later)
                 */
                if (rel->alternatives != NIL) {
                    break;
                }

                TargetEntry* sub_tle = (TargetEntry*)list_nth(subqueryplan->subplan->targetlist, attnum - 1);

                if (IsA(sub_tle->expr, Var)) {
                    search_var_and_mark_bloomfilter(rel->subroot, sub_tle->expr, subqueryplan->subplan, context);
                }
            }
            break;
        }
        case T_PartIterator: {
            PartIterator* splan = (PartIterator*)plan;
            search_var_and_mark_bloomfilter(root, expr, splan->plan.lefttree, context);
            break;
        }
        default: {
            break;
        }
    }
}

/*
 * @Descrition: Find var's ratio.
 * @in rvar: Join condition right args var.
 * @in lvar: Join condition left args var.
 */
static double join_var_ratio(PlannerInfo* root, Var* rvar, Var* lvar)
{
    RelOptInfo* baseRel = find_base_rel(root, lvar->varno);

    Relids joinrelids = NULL;

    joinrelids = bms_add_member(joinrelids, rvar->varno);
    joinrelids = bms_add_member(joinrelids, lvar->varno);

    ListCell* lc = NULL;

    /* Find var ration from RelOptInfo's varEqRatio. */
    foreach (lc, baseRel->varEqRatio) {
        VarEqRatio* var_ratio = (VarEqRatio*)lfirst(lc);

        if (equal(var_ratio->var, lvar) && bms_equal(var_ratio->joinrelids, joinrelids)) {
            bms_free_ext(joinrelids);
            return var_ratio->ratio;
        }
    }

    bms_free_ext(joinrelids);
    return EQUALJOINVARRATIO;
}

/*
 * @Description: Judge this var type if can bloom filter.
 * @in var: Var.
 * @return: If can filter return true else return false.
 */
static bool valid_bloom_filter_type(Var* var)
{
    bool result = false;

    switch (var->vartype) {
        case INT2OID:
        case INT4OID:
        case INT8OID:
        case FLOAT4OID:
        case FLOAT8OID:
        case VARCHAROID:
        case BPCHAROID:
        case TEXTOID:
        case CLOBOID: {
            result = true;
            break;
        }
        default: {
            result = false;
            break;
        }
    }

    return result;
}

/*
 * @Description: Foreach HashJoin hashclauses and set bloomfilter.
 * @in root: Per-query information for planning/optimization.
 * @in lefttree_relids: Left tree relids.
 * @in joinrel: Hashjoin rel.
 */
static void set_bloomfilter(PlannerInfo* root, Relids lefttree_relids, HashJoin* hash_join)
{
    bloomfilter_context* context = &(root->glob->bloomfilter);

    /* Only can support these join type. */
    if (hash_join->join.jointype == JOIN_INNER || hash_join->join.jointype == JOIN_RIGHT ||
        hash_join->join.jointype == JOIN_SEMI) {
        Plan* outer_plan = outerPlan(hash_join);

        ListCell* lc = NULL;

        foreach (lc, hash_join->hashclauses) {
            Node* node = (Node*)lfirst(lc);

            Assert(is_opclause(node));

            OpExpr* clause = (OpExpr*)node;

            Assert(list_length(clause->args) == 2);

            Expr* lexpr = (Expr*)linitial(clause->args);
            Expr* rexpr = (Expr*)lsecond(clause->args);

            /* Bloom filter only support opExpr's args is var. */
            if (!IsA(lexpr, Var) || !IsA(rexpr, Var) || !valid_bloom_filter_type((Var*)lexpr)) {
                continue;
            }

            /* If this hash query can filter 1/3 data, we will add bloom filter. */
            if (join_var_ratio(root, (Var*)rexpr, (Var*)lexpr) <= EQUALJOINVARRATIO) {
                search_var_and_mark_bloomfilter(root, lexpr, outer_plan, context);
            }

            /* Get EquivalenceClass member, lexpr and rexpr must be equivalence. */
            EquivalenceClass* eqclass = get_expr_eqClass(root, lexpr);

            /* Set equivalence class expr's bloomfilter. */
            if (eqclass != NULL) {
                ListCell* l = NULL;
                foreach (l, eqclass->ec_members) {
                    EquivalenceMember* em = (EquivalenceMember*)lfirst(l);

                    if (IsA(em->em_expr, Var)) {
                        Var* eq_var = (Var*)em->em_expr;

                        /* This eq_var need be left_rel's subset. We only can add bloom filter on left plan. */
                        if (!equal(eq_var, lexpr) && !equal(eq_var, rexpr) && valid_bloom_filter_type((Var*)eq_var) &&
                            bms_is_member(eq_var->varno, lefttree_relids)) {
                            if (join_var_ratio(root, (Var*)rexpr, eq_var) <= EQUALJOINVARRATIO) {
                                search_var_and_mark_bloomfilter(root, (Expr*)eq_var, outer_plan, context);
                            }
                        }
                    }
                }
            }

            /* Have successful bloom filter. */
            if (!context->add_index) {
                ((Plan*)hash_join)->filterIndexList =
                    lappend_int(((Plan*)hash_join)->filterIndexList, context->bloomfilter_index);
                ((Plan*)hash_join)->var_list = lappend(((Plan*)hash_join)->var_list, copyObject(rexpr));
            }

            context->add_index = true;
        }
    }

    context->add_index = true;
}

/*
 * @Description: Create hash join plan.
 * @in root: Per-query information for planning/optimization.
 * @in best_path: Hashjoin path.
 * @in outer_plan: Outer plan.
 * @in inner_plan: Inner plan.
 */
static HashJoin* create_hashjoin_plan(PlannerInfo* root, HashPath* best_path, Plan* outer_plan, Plan* inner_plan)
{
    List* tlist = build_relation_tlist(best_path->jpath.path.parent);
    List* joinclauses = NIL;
    List* otherclauses = NIL;
    List* hashclauses = NIL;
    Oid skewTable = InvalidOid;
    AttrNumber skewColumn = InvalidAttrNumber;
    bool skewInherit = false;
    Oid skewColType = InvalidOid;
    int32 skewColTypmod = -1;
    HashJoin* join_plan = NULL;
    Hash* hash_plan = NULL;
    Relids left_relids = NULL;

    /* Sort join qual clauses into best execution order */
    joinclauses = order_qual_clauses(root, best_path->jpath.joinrestrictinfo);
    /* There's no point in sorting the hash clauses ... */

    /* Get the join qual clauses (in plain expression form) */
    /* Any pseudoconstant clauses are ignored here */
    if (IS_OUTER_JOIN((uint32)(best_path->jpath.jointype))) {
        extract_actual_join_clauses(joinclauses, &joinclauses, &otherclauses);
    } else {
        /* We can treat all clauses alike for an inner join */
        joinclauses = extract_actual_clauses(joinclauses, false);
        otherclauses = NIL;
    }

    /*
     * Remove the hashclauses from the list of join qual clauses, leaving the
     * list of quals that must be checked as qpquals.
     */
    hashclauses = get_actual_clauses(best_path->path_hashclauses);
    joinclauses = list_difference(joinclauses, hashclauses);

    /*
     * Replace any outer-relation variables with nestloop params.  There
     * should not be any in the hashclauses.
     */
    if (best_path->jpath.path.param_info) {
        joinclauses = (List*)replace_nestloop_params(root, (Node*)joinclauses);
        otherclauses = (List*)replace_nestloop_params(root, (Node*)otherclauses);
    }

    /*
     * Rearrange hashclauses, if needed, so that the outer variable is always
     * on the left.
     */
    hashclauses = get_switched_clauses(best_path->path_hashclauses, best_path->jpath.outerjoinpath->parent->relids);

    /* We don't want any excess columns in the hashed tuples */
    disuse_physical_tlist(inner_plan, best_path->jpath.innerjoinpath);

    /* If we expect batching, suppress excess columns in outer tuples too */
    if (best_path->num_batches > 1)
        disuse_physical_tlist(outer_plan, best_path->jpath.outerjoinpath);

    /*
     * If there is a single join clause and we can identify the outer variable
     * as a simple column reference, supply its identity for possible use in
     * skew optimization.  (Note: in principle we could do skew optimization
     * with multiple join clauses, but we'd have to be able to determine the
     * most common combinations of outer values, which we don't currently have
     * enough stats for.)
     */
    if (list_length(hashclauses) == 1) {
        OpExpr* clause = (OpExpr*)linitial(hashclauses);
        Node* node = NULL;

        Assert(is_opclause(clause));
        node = (Node*)linitial(clause->args);
        if (IsA(node, RelabelType))
            node = (Node*)((RelabelType*)node)->arg;
        if (IsA(node, Var)) {
            Var* var = (Var*)node;
            RangeTblEntry* rte = NULL;

            rte = root->simple_rte_array[var->varno];
            if (rte->rtekind == RTE_RELATION) {
                skewTable = rte->relid;
                skewColumn = var->varattno;
                skewInherit = rte->inh;
                skewColType = var->vartype;
                skewColTypmod = var->vartypmod;
            }
        }
    }

    /*
     * Build the hash node and hash join node.
     */
    hash_plan = make_hash(inner_plan, skewTable, skewColumn, skewInherit, skewColType, skewColTypmod);
    join_plan = make_hashjoin(
        tlist, joinclauses, otherclauses, hashclauses, outer_plan, (Plan*)hash_plan, best_path->jpath.jointype);

    /*
     * @hdfs
     * For hashjoin, it is not necessery to optimize by using informational constraint.
     * If the hash key is uniuqe column, only one tuple exists in hashcell of hash buckect.
     * If the outer tuple matches the one inner tuple, this outer tuple will do not find
     * suitable inner tuple. So do not use informational constraint here.
     */

    copy_mem_info(&join_plan->mem_info, &best_path->mem_info);
    join_plan->transferFilterFlag = CanTransferInJoin(join_plan->join.jointype);

    if (root->join_null_info)
        join_plan->join.nulleqqual = make_null_eq_clause(hashclauses, &join_plan->join.joinqual, root->join_null_info);

    /* Set dop from path. */
    join_plan->join.plan.dop = best_path->jpath.path.dop;
    hash_plan->plan.dop = best_path->jpath.path.dop;
    join_plan->joinRows = best_path->joinRows;

    join_plan->isSonicHash = u_sess->attr.attr_sql.enable_sonic_hashjoin && isSonicHashJoinEnable(join_plan);

    if (IS_STREAM_PLAN && u_sess->attr.attr_sql.enable_bloom_filter) {
        left_relids = best_path->jpath.outerjoinpath->parent->relids;
        set_bloomfilter(root, left_relids, join_plan);
    }

    return join_plan;
}

/*****************************************************************************
 *
 *	SUPPORTING ROUTINES
 *****************************************************************************/
/*
 * replace_nestloop_params
 *	  Replace outer-relation Vars and PlaceHolderVars in the given expression
 *	  with nestloop Params
 *
 * All Vars and PlaceHolderVars belonging to the relation(s) identified by
 * root->curOuterRels are replaced by Params, and entries are added to
 * root->curOuterParams if not already present.
 */
static Node* replace_nestloop_params(PlannerInfo* root, Node* expr)
{
    /* No setup needed for tree walk, so away we go */
    return replace_nestloop_params_mutator(expr, root);
}

static Node* replace_nestloop_params_mutator(Node* node, PlannerInfo* root)
{
    if (node == NULL)
        return NULL;
    if (IsA(node, Var)) {
        Var* var = (Var*)node;
        Param* param = NULL;
        NestLoopParam* nlp = NULL;
        ListCell* lc = NULL;

        /* Upper-level Vars should be long gone at this point */
        Assert(var->varlevelsup == 0);
        /* If not to be replaced, we can just return the Var unmodified */
        if (!bms_is_member(var->varno, root->curOuterRels))
            return node;
        /* Create a Param representing the Var */
        param = assign_nestloop_param_var(root, var);
        /* Is this param already listed in root->curOuterParams? */
        foreach (lc, root->curOuterParams) {
            nlp = (NestLoopParam*)lfirst(lc);
            if (nlp->paramno == param->paramid) {
                Assert(equal(var, nlp->paramval));
                /* Present, so we can just return the Param */
                return (Node*)param;
            }
        }
        /* No, so add it */
        nlp = makeNode(NestLoopParam);
        nlp->paramno = param->paramid;
        nlp->paramval = var;
        root->curOuterParams = lappend(root->curOuterParams, nlp);
        /* And return the replacement Param */
        return (Node*)param;
    }
    if (IsA(node, PlaceHolderVar)) {
        PlaceHolderVar* phv = (PlaceHolderVar*)node;
        Param* param = NULL;
        NestLoopParam* nlp = NULL;
        ListCell* lc = NULL;

        /* Upper-level PlaceHolderVars should be long gone at this point */
        Assert(phv->phlevelsup == 0);

        /*
         * If not to be replaced, just return the PlaceHolderVar unmodified.
         * We use bms_overlap as a cheap/quick test to see if the PHV might be
         * evaluated in the outer rels, and then grab its PlaceHolderInfo to
         * tell for sure.
         */
        if (!bms_overlap(phv->phrels, root->curOuterRels))
            return node;
        if (!bms_is_subset(find_placeholder_info(root, phv, false)->ph_eval_at, root->curOuterRels))
            return node;
        /* Create a Param representing the PlaceHolderVar */
        param = assign_nestloop_param_placeholdervar(root, phv);
        /* Is this param already listed in root->curOuterParams? */
        foreach (lc, root->curOuterParams) {
            nlp = (NestLoopParam*)lfirst(lc);
            if (nlp->paramno == param->paramid) {
                Assert(equal(phv, nlp->paramval));
                /* Present, so we can just return the Param */
                return (Node*)param;
            }
        }
        /* No, so add it */
        nlp = makeNode(NestLoopParam);
        nlp->paramno = param->paramid;
        nlp->paramval = (Var*)phv;
        root->curOuterParams = lappend(root->curOuterParams, nlp);
        /* And return the replacement Param */
        return (Node*)param;
    }
    return expression_tree_mutator(node, (Node * (*)(Node*, void*)) replace_nestloop_params_mutator, (void*)root);
}

/*
 * process_subquery_nestloop_params
 *	  Handle params of a parameterized subquery that need to be fed
 *	  from an outer nestloop.
 *
 * Currently, that would be *all* params that a subquery in FROM has demanded
 * from the current query level, since they must be LATERAL references.
 *
 * subplan_params is a list of PlannerParamItems that we intend to pass to
 * a subquery-in-FROM.  (This was constructed in root->plan_params while
 * planning the subquery, but isn't there anymore when this is called.)
 *
 * The subplan's references to the outer variables are already represented
 * as PARAM_EXEC Params, since that conversion was done by the routines above
 * while planning the subquery.  So we need not modify the subplan or the
 * PlannerParamItems here.  What we do need to do is add entries to
 * root->curOuterParams to signal the parent nestloop plan node that it must
 * provide these values.  This differs from replace_nestloop_param_var in
 * that the PARAM_EXEC slots to use have already been determined.
 *
 * Note that we also use root->curOuterRels as an implicit parameter for
 * sanity checks.
 */
void
process_subquery_nestloop_params(PlannerInfo *root, List *subplan_params)
{
	ListCell *lc = NULL;

	foreach(lc, subplan_params) {
		PlannerParamItem *pitem = castNode(PlannerParamItem, lfirst(lc));

		if (IsA(pitem->item, Var)) {
			Var		   *var = (Var *) pitem->item;
			NestLoopParam *nlp = NULL;
			ListCell   *lc = NULL;

			/* If not from a nestloop outer rel, complain */
			if (!bms_is_member(var->varno, root->curOuterRels)) {
				ereport(ERROR,
						(errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
						errmsg("non-LATERAL parameter required by subquery")));
			}

			/* Is this param already listed in root->curOuterParams? */
			foreach(lc, root->curOuterParams) {
				nlp = (NestLoopParam *) lfirst(lc);
				if (nlp->paramno == pitem->paramId) {
					Assert(equal(var, nlp->paramval));
					/* Present, so nothing to do */
					break;
				}
			}
			if (lc == NULL) {
				/* No, so add it */
				nlp = makeNode(NestLoopParam);
				nlp->paramno = pitem->paramId;
				nlp->paramval = (Var *)copyObject(var);
				root->curOuterParams = lappend(root->curOuterParams, nlp);
			}
		}else if (IsA(pitem->item, PlaceHolderVar)) {
			PlaceHolderVar *phv = (PlaceHolderVar *) pitem->item;
			NestLoopParam *nlp = NULL;
			ListCell *lc = NULL;

			/* If not from a nestloop outer rel, complain */
			if (!bms_is_subset(find_placeholder_info(root, phv, false)->ph_eval_at,
							   root->curOuterRels)) {
				ereport(ERROR,
						(errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
						errmsg("non-LATERAL parameter required by subquery")));
			}

			/* Is this param already listed in root->curOuterParams? */
			foreach(lc, root->curOuterParams) {
				nlp = (NestLoopParam *) lfirst(lc);
				if (nlp->paramno == pitem->paramId) {
					Assert(equal(phv, nlp->paramval));
					/* Present, so nothing to do */
					break;
				}
			}
			if (lc == NULL) {
				/* No, so add it */
				nlp = makeNode(NestLoopParam);
				nlp->paramno = pitem->paramId;
				nlp->paramval = (Var *) copyObject(phv);
				root->curOuterParams = lappend(root->curOuterParams, nlp);
			}
		} else {
			ereport(ERROR,
					(errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
					errmsg("unexpected type of subquery parameter")));
		}
	}
}

/*
 * fix_indexqual_references
 *	  Adjust indexqual clauses to the form the executor's indexqual
 *	  machinery needs.
 *
 * We have four tasks here:
 *	* Remove RestrictInfo nodes from the input clauses.
 *	* Replace any outer-relation Var or PHV nodes with nestloop Params.
 *	  (XXX eventually, that responsibility should go elsewhere?)
 *	* Index keys must be represented by Var nodes with varattno set to the
 *	  index's attribute number, not the attribute number in the original rel.
 *	* If the index key is on the right, commute the clause to put it on the
 *	  left.
 *
 * The result is a modified copy of the path's indexquals list --- the
 * original is not changed.  Note also that the copy shares no substructure
 * with the original; this is needed in case there is a subplan in it (we need
 * two separate copies of the subplan tree, or things will go awry).
 */
static List* fix_indexqual_references(PlannerInfo* root, IndexPath* index_path)
{
    IndexOptInfo* index = index_path->indexinfo;
    List* fixed_indexquals = NIL;
    ListCell* lcc = NULL;
    ListCell* lci = NULL;

    fixed_indexquals = NIL;

    forboth(lcc, index_path->indexquals, lci, index_path->indexqualcols)
    {
        RestrictInfo* rinfo = (RestrictInfo*)lfirst(lcc);
        int indexcol = lfirst_int(lci);
        Node* clause = NULL;

        Assert(IsA(rinfo, RestrictInfo));

        /*
         * Replace any outer-relation variables with nestloop params.
         *
         * This also makes a copy of the clause, so it's safe to modify it
         * in-place below.
         */
        clause = replace_nestloop_params(root, (Node*)rinfo->clause);

        if (IsA(clause, OpExpr)) {
            OpExpr* op = (OpExpr*)clause;

            if (list_length(op->args) != 2)
                ereport(ERROR,
                    (errmodule(MOD_OPT),
                        errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                        (errmsg("indexqual clause is not binary opclause"))));

            /*
             * Check to see if the indexkey is on the right; if so, commute
             * the clause.	The indexkey should be the side that refers to
             * (only) the base relation.
             */
            if (!bms_equal(rinfo->left_relids, index->rel->relids))
                CommuteOpExpr(op);

            /*
             * Now replace the indexkey expression with an index Var.
             */
            linitial(op->args) = fix_indexqual_operand((Node*)linitial(op->args), index, indexcol);
        } else if (IsA(clause, RowCompareExpr)) {
            RowCompareExpr* rc = (RowCompareExpr*)clause;
            Expr* newrc = NULL;
            List* indexcolnos = NIL;
            bool var_on_left = false;
            ListCell* lca = NULL;
            ListCell* lcai = NULL;

            /*
             * Re-discover which index columns are used in the rowcompare.
             */
            newrc = adjust_rowcompare_for_index(rc, index, indexcol, &indexcolnos, &var_on_left);

            /*
             * Trouble if adjust_rowcompare_for_index thought the
             * RowCompareExpr didn't match the index as-is; the clause should
             * have gone through that routine already.
             */
            if (newrc != (Expr*)rc)
                ereport(ERROR,
                    (errmodule(MOD_OPT),
                        errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                        (errmsg("inconsistent results from adjust_rowcompare_for_index"))));

            /*
             * Check to see if the indexkey is on the right; if so, commute
             * the clause.
             */
            if (!var_on_left)
                CommuteRowCompareExpr(rc);

            /*
             * Now replace the indexkey expressions with index Vars.
             */
            Assert(list_length(rc->largs) == list_length(indexcolnos));
            forboth(lca, rc->largs, lcai, indexcolnos)
            {
                lfirst(lca) = fix_indexqual_operand((Node*)lfirst(lca), index, lfirst_int(lcai));
            }
        } else if (IsA(clause, ScalarArrayOpExpr)) {
            ScalarArrayOpExpr* saop = (ScalarArrayOpExpr*)clause;

            /* Never need to commute... */
            /* Replace the indexkey expression with an index Var. */
            linitial(saop->args) = fix_indexqual_operand((Node*)linitial(saop->args), index, indexcol);
        } else if (IsA(clause, NullTest)) {
            NullTest* nt = (NullTest*)clause;

            /* Replace the indexkey expression with an index Var. */
            nt->arg = (Expr*)fix_indexqual_operand((Node*)nt->arg, index, indexcol);
        } else
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    (errmsg("unsupported indexqual type: %d", (int)nodeTag(clause)))));

        fixed_indexquals = lappend(fixed_indexquals, clause);
    }

    return fixed_indexquals;
}

/*
 * fix_indexorderby_references
 *	  Adjust indexorderby clauses to the form the executor's index
 *	  machinery needs.
 *
 * This is a simplified version of fix_indexqual_references.  The input does
 * not have RestrictInfo nodes, and we assume that indxpath.c already
 * commuted the clauses to put the index keys on the left.	Also, we don't
 * bother to support any cases except simple OpExprs, since nothing else
 * is allowed for ordering operators.
 */
static List* fix_indexorderby_references(PlannerInfo* root, IndexPath* index_path)
{
    IndexOptInfo* index = index_path->indexinfo;
    List* fixed_indexorderbys = NIL;
    ListCell* lcc = NULL;
    ListCell* lci = NULL;

    fixed_indexorderbys = NIL;

    forboth(lcc, index_path->indexorderbys, lci, index_path->indexorderbycols)
    {
        Node* clause = (Node*)lfirst(lcc);
        int indexcol = lfirst_int(lci);

        /*
         * Replace any outer-relation variables with nestloop params.
         *
         * This also makes a copy of the clause, so it's safe to modify it
         * in-place below.
         */
        clause = replace_nestloop_params(root, clause);

        if (IsA(clause, OpExpr)) {
            OpExpr* op = (OpExpr*)clause;

            if (list_length(op->args) != 2)
                ereport(ERROR,
                    (errmodule(MOD_OPT),
                        errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                        (errmsg("indexorderby clause is not binary opclause"))));

            /*
             * Now replace the indexkey expression with an index Var.
             */
            linitial(op->args) = fix_indexqual_operand((Node*)linitial(op->args), index, indexcol);
        } else
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
                    errmsg("unsupported indexorderby type: %d", (int)nodeTag(clause))));

        fixed_indexorderbys = lappend(fixed_indexorderbys, clause);
    }

    return fixed_indexorderbys;
}

/*
 * fix_indexqual_operand
 *	  Convert an indexqual expression to a Var referencing the index column.
 *
 * We represent index keys by Var nodes having varno == INDEX_VAR and varattno
 * equal to the index's attribute number (index column position).
 *
 * Most of the code here is just for sanity cross-checking that the given
 * expression actually matches the index column it's claimed to.
 */
static Node* fix_indexqual_operand(Node* node, IndexOptInfo* index, int indexcol)
{
    Var* result = NULL;
    int pos;
    ListCell* indexpr_item = NULL;

    /*
     * Remove any binary-compatible relabeling of the indexkey
     */
    if (IsA(node, RelabelType))
        node = (Node*)((RelabelType*)node)->arg;

    Assert(indexcol >= 0 && indexcol < index->ncolumns);

    if (index->indexkeys[indexcol] != 0) {
        /* It's a simple index column */
        if (IsA(node, Var) && ((Var*)node)->varno == index->rel->relid &&
            ((Var*)node)->varattno == index->indexkeys[indexcol]) {
            result = (Var*)copyObject(node);
            result->varno = INDEX_VAR;
            result->varattno = indexcol + 1;
            return (Node*)result;
        } else {
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                    errmsg("index key does not match expected index column")));
        }
    }

    /* It's an index expression, so find and cross-check the expression */
    indexpr_item = list_head(index->indexprs);
    for (pos = 0; pos < index->ncolumns; pos++) {
        if (index->indexkeys[pos] == 0) {
            if (indexpr_item == NULL) {
                ereport(ERROR,
                    (errmodule(MOD_OPT),
                        errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                        errmsg("too few entries in indexprs list")));
            }

            if (pos == indexcol) {
                Node* indexkey = NULL;

                indexkey = (Node*)lfirst(indexpr_item);
                if (indexkey && IsA(indexkey, RelabelType))
                    indexkey = (Node*)((RelabelType*)indexkey)->arg;
                if (equal(node, indexkey)) {
                    result = makeVar(INDEX_VAR,
                        indexcol + 1,
                        exprType((Node*)lfirst(indexpr_item)),
                        -1,
                        exprCollation((Node*)lfirst(indexpr_item)),
                        0);
                    return (Node*)result;
                } else
                    ereport(ERROR,
                        (errmodule(MOD_OPT),
                            errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                            (errmsg("index key does not match expected index column"))));
            }
            indexpr_item = lnext(indexpr_item);
        }
    }

    /* Ooops... */
    ereport(ERROR,
        (errmodule(MOD_OPT),
            errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
            (errmsg("index key does not match expected index column"))));
    return NULL; /* keep compiler quiet */
}

/*
 * get_switched_clauses
 *	  Given a list of merge or hash joinclauses (as RestrictInfo nodes),
 *	  extract the bare clauses, and rearrange the elements within the
 *	  clauses, if needed, so the outer join variable is on the left and
 *	  the inner is on the right.  The original clause data structure is not
 *	  touched; a modified list is returned.  We do, however, set the transient
 *	  outer_is_left field in each RestrictInfo to show which side was which.
 */
static List* get_switched_clauses(List* clauses, Relids outerrelids)
{
    List* t_list = NIL;
    ListCell* l = NULL;

    foreach (l, clauses) {
        RestrictInfo* restrictinfo = (RestrictInfo*)lfirst(l);
        OpExpr* clause = (OpExpr*)restrictinfo->clause;

        Assert(is_opclause(clause));
        if (bms_is_subset(restrictinfo->right_relids, outerrelids)) {
            /*
             * Duplicate just enough of the structure to allow commuting the
             * clause without changing the original list.  Could use
             * copyObject, but a complete deep copy is overkill.
             */
            OpExpr* temp = makeNode(OpExpr);

            temp->opno = clause->opno;
            temp->opfuncid = InvalidOid;
            temp->opresulttype = clause->opresulttype;
            temp->opretset = clause->opretset;
            temp->opcollid = clause->opcollid;
            temp->inputcollid = clause->inputcollid;
            temp->args = list_copy(clause->args);
            temp->location = clause->location;
            /* Commute it --- note this modifies the temp node in-place. */
            CommuteOpExpr(temp);
            t_list = lappend(t_list, temp);
            restrictinfo->outer_is_left = false;
        } else {
            Assert(bms_is_subset(restrictinfo->left_relids, outerrelids));
            t_list = lappend(t_list, clause);
            restrictinfo->outer_is_left = true;
        }
    }
    return t_list;
}

/*
 * order_qual_clauses
 *		Given a list of qual clauses that will all be evaluated at the same
 *		plan node, sort the list into the order we want to check the quals
 *		in at runtime.
 *
 * When security barrier quals are used in the query, we may have quals with
 * different security levels in the list.  Quals of lower security_level
 * must go before quals of higher security_level, except that we can grant
 * exceptions to move up quals that are leakproof.  When security level
 * doesn't force the decision, we prefer to order clauses by estimated
 * execution cost, cheapest first.
 *
 * Ideally the order should be driven by a combination of execution cost and
 * selectivity, but it's not immediately clear how to account for both,
 * and given the uncertainty of the estimates the reliability of the decisions
 * would be doubtful anyway.  So we just order by security level then
 * estimated per-tuple cost, being careful not to change the order when
 * (as is often the case) the estimates are identical.
 *
 * Although this will work on either bare clauses or RestrictInfos, it's
 * much faster to apply it to RestrictInfos, since it can re-use cost
 * information that is cached in RestrictInfos. XXX in the bare-clause
 * case, we are also not able to apply security considerations.  That is
 * all right for the moment, because the bare-clause case doesn't occur
 * anywhere that barrier quals could be present, but it would be better to
 * get rid of it.
 *
 * Note: some callers pass lists that contain entries that will later be
 * removed; this is the easiest way to let this routine see RestrictInfos
 * instead of bare clauses.  This is another reason why trying to consider
 * selectivity in the ordering would likely do the wrong thing.
 */
static List* order_qual_clauses(PlannerInfo* root, List* clauses)
{
    typedef struct {
        Node* clause;
        Cost cost;
        Index security_level;
    } QualItem;
    int nitems = list_length(clauses);
    QualItem* items = NULL;
    ListCell* lc = NULL;
    int i;
    List* result = NIL;

    /* No need to work hard for 0 or 1 clause */
    if (nitems <= 1)
        return clauses;

    /*
     * Collect the items and costs into an array.  This is to avoid repeated
     * cost_qual_eval work if the inputs aren't RestrictInfos.
     */
    items = (QualItem*)palloc(nitems * sizeof(QualItem));
    i = 0;
    foreach (lc, clauses) {
        Node* clause = (Node*)lfirst(lc);
        QualCost qcost;

        cost_qual_eval_node(&qcost, clause, root);
        items[i].clause = clause;
        items[i].cost = qcost.per_tuple;
        if (IsA(clause, RestrictInfo)) {
            RestrictInfo* rinfo = (RestrictInfo*)clause;

            /*
             * If a clause is leakproof, it doesn't have to be constrained by
             * its nominal security level.  If it's also reasonably cheap
             * (here defined as 10X cpu_operator_cost), pretend it has
             * security_level 0, which will allow it to go in front of
             * more-expensive quals of lower security levels.  Of course, that
             * will also force it to go in front of cheaper quals of its own
             * security level, which is not so great, but we can alleviate
             * that risk by applying the cost limit cutoff.
             */
            if (rinfo->leakproof && items[i].cost < 10 * u_sess->attr.attr_sql.cpu_operator_cost)
                items[i].security_level = 0;
            else
                items[i].security_level = rinfo->security_level;
        } else
            items[i].security_level = 0;
        i++;
    }

    /*
     * Sort.  We don't use qsort() because it's not guaranteed stable for
     * equal keys.	The expected number of entries is small enough that a
     * simple insertion sort should be good enough.
     */
    for (i = 1; i < nitems; i++) {
        QualItem newitem = items[i];
        int j;

        /* insert newitem into the already-sorted subarray */
        for (j = i; j > 0; j--) {
            if ((newitem.security_level > items[j - 1].security_level) ||
                ((newitem.security_level == items[j - 1].security_level) && (newitem.cost >= items[j - 1].cost)))
                break;
            items[j] = items[j - 1];
        }
        items[j] = newitem;
    }

    /* Convert back to a list */
    result = NIL;
    for (i = 0; i < nitems; i++)
        result = lappend(result, items[i].clause);

    return result;
}

/*
 * Copy cost and size info from a Path node to the Plan node created from it.
 * The executor usually won't use this info, but it's needed by EXPLAIN.
 * Also copy the parallel-related flags, which the executor *will* use.
 */
static void copy_generic_path_info(Plan *dest, Path *src)
{
    dest->startup_cost = src->startup_cost;
    dest->total_cost = src->total_cost;
    dest->plan_rows = src->rows;
}

/*
 * Copy cost and size info from a Path node to the Plan node created from it.
 * The executor usually won't use this info, but it's needed by EXPLAIN.
 */
static void copy_path_costsize(Plan* dest, Path* src)
{
    if (src != NULL) {
        dest->startup_cost = src->startup_cost;
        dest->total_cost = src->total_cost;
        set_plan_rows(dest, src->rows, src->multiple);
        dest->plan_width = src->parent->width;
        dest->innerdistinct = src->innerdistinct;
        dest->outerdistinct = src->outerdistinct;
    } else {
        /* init the cost field directly */
        init_plan_cost(dest);
    }
}

/*
 * Copy cost and size info from a lower plan node to an inserted node.
 * (Most callers alter the info after copying it.)
 */
void copy_plan_costsize(Plan* dest, Plan* src)
{
    if (src != NULL) {
        dest->startup_cost = src->startup_cost;
        dest->total_cost = src->total_cost;
        set_plan_rows_from_plan(dest, PLAN_LOCAL_ROWS(src), src->multiple);
        dest->plan_width = src->plan_width;
    } else {
        /* init the cost field directly */
        init_plan_cost(dest);
    }
}

/*****************************************************************************
 *
 *	PLAN NODE BUILDING ROUTINES
 *
 * Some of these are exported because they are called to build plan nodes
 * in contexts where we're not deriving the plan node from a path node.
 *****************************************************************************/
static SeqScan* make_seqscan(List* qptlist, List* qpqual, Index scanrelid)
{
    SeqScan* node = makeNode(SeqScan);
    Plan* plan = &node->plan;

    /* cost should be inserted by caller */
    plan->targetlist = qptlist;
    plan->qual = qpqual;
    plan->lefttree = NULL;
    plan->righttree = NULL;
    plan->isDeltaTable = false;
    node->scanrelid = scanrelid;
    node->scanBatchMode = false;

    return node;
}

static CStoreScan* make_cstorescan(List* qptlist, List* qpqual, Index scanrelid)
{
    CStoreScan* node = makeNode(CStoreScan);
    Plan* plan = &node->plan;

    /* cost should be inserted by caller */
    plan->targetlist = qptlist;
    plan->qual = qpqual;
    plan->lefttree = NULL;
    plan->righttree = NULL;
    plan->vec_output = true;

    node->scanrelid = scanrelid;
    node->cstorequal = NIL;
    node->minMaxInfo = NIL;
    node->is_replica_table = false;

    return node;
}

/*
 * Brief        : Makea DfsScan node.
 * Input        : tList, the target list.
 *                qual, the qual list.
 *                scanRelId, the relation index in range table entry.
 *                privateList, Wrapper DfsPrivateItem using DefElem struct.
 * Output       : None.
 * Return Value : Return the DfsScan..
 * Notes        : None.
 */
static DfsScan* make_dfsscan(List* tList, List* qual, Index scanReId, List* privateList, int dop)
{
    DfsScan* node = makeNode(DfsScan);
    Plan* plan = &node->plan;

    /*
     * cost should be inserted by caller.
     */
    plan->targetlist = tList;
    plan->qual = qual;
    plan->lefttree = NULL;
    plan->righttree = NULL;
    plan->vec_output = true;

    node->scanrelid = scanReId;
    node->privateData = privateList;

    /* dfs memory estimate. */
    /* Since dfsscan cannot be accurately estimated, it can only be given a const value. but in the SMP state, the
     * dop at this time will change during the optimization phase. Therefore the min max value is not estimated
     * here and they should be specially processed of mem_info.mimMem and maxMem. It is given separately
     * in the AssignMemOpConsumption() in memctl.cpp.
     */
    node->mem_info.opMem = u_sess->opt_cxt.op_work_mem;
    node->mem_info.regressCost = 0;

    return node;
}

#ifdef ENABLE_MULTIPLE_NODES
static TsStoreScan* make_tsstorescan(List* qptlist, List* qpqual, Index scanrelid)
{
    TsStoreScan* node = makeNode(TsStoreScan);
    Plan* plan = &node->plan;

    /* cost should be inserted by caller */
    plan->targetlist = qptlist;
    plan->qual = qpqual;
    plan->lefttree = NULL;
    plan->righttree = NULL;
    plan->vec_output = true;

    node->scanrelid = scanrelid;
    node->tsstorequal = NIL;
    node->minMaxInfo = NIL;
    node->is_replica_table = false;
    node->has_sort = false;
    node->sort_by_time_colidx = -1;
    node->limit = -1;
    node->is_simple_scan = false;
    node->series_func_calls = 0;
    node->top_key_func_arg = -1;
    return node;
}
#endif   /* ENABLE_MULTIPLE_NODES */

static IndexScan* make_indexscan(List* qptlist, List* qpqual, Index scanrelid, Oid indexid, List* indexqual,
    List* indexqualorig, List* indexorderby, List* indexorderbyorig, ScanDirection indexscandir)
{
    IndexScan* node = makeNode(IndexScan);
    Plan* plan = &node->scan.plan;

    /* cost should be inserted by caller */
    plan->targetlist = qptlist;
    plan->qual = qpqual;
    plan->lefttree = NULL;
    plan->righttree = NULL;
    node->scan.scanrelid = scanrelid;
    node->indexid = indexid;
    node->indexqual = indexqual;
    node->indexqualorig = indexqualorig;
    node->indexorderby = indexorderby;
    node->indexorderbyorig = indexorderbyorig;
    node->indexorderdir = indexscandir;

    return node;
}

static IndexOnlyScan* make_indexonlyscan(List* qptlist, List* qpqual, Index scanrelid, Oid indexid, List* indexqual,
    List* indexorderby, List* indextlist, ScanDirection indexscandir)
{
    IndexOnlyScan* node = makeNode(IndexOnlyScan);
    Plan* plan = &node->scan.plan;

    /* cost should be inserted by caller */
    plan->targetlist = qptlist;
    plan->qual = qpqual;
    plan->lefttree = NULL;
    plan->righttree = NULL;
    node->scan.scanrelid = scanrelid;
    node->indexid = indexid;
    node->indexqual = indexqual;
    node->indexorderby = indexorderby;
    node->indextlist = indextlist;
    node->indexorderdir = indexscandir;

    return node;
}

static CStoreIndexScan* make_cstoreindexscan(PlannerInfo* root, Path* best_path, List* qptlist, List* qpqual,
    Index scanrelid, Oid indexid, List* indexqual, List* indexqualorig, List* indexorderby, List* indexorderbyorig,
    List* indextlist, ScanDirection indexscandir, bool indexonly)
{
    CStoreIndexScan* node = makeNode(CStoreIndexScan);
    Plan* plan = &node->scan.plan;
    Relation indexRel;
    Path sort_path;

    /* cost should be inserted by caller */
    if (qptlist != NIL)
        list_free_ext(qptlist);
    qptlist = build_relation_tlist(best_path->parent);
    if (qptlist == NIL)
        qptlist = build_one_column_tlist(root, best_path->parent);

    plan->targetlist = qptlist;
    plan->qual = qpqual;
    plan->lefttree = NULL;
    plan->righttree = NULL;
    plan->vec_output = true;

    node->scan.scanrelid = scanrelid;
    node->indexid = indexid;
    node->indexqual = indexqual;
    node->indexqualorig = indexqualorig;
    node->indexorderby = indexorderby;
    node->indexorderbyorig = indexorderbyorig;
    node->indexorderdir = indexscandir;
    node->baserelcstorequal = fix_cstore_scan_qual(root, qpqual);
    node->cstorequal = fix_cstore_scan_qual(root, indexqual);
    node->indextlist = indextlist;
    node->relStoreLocation = best_path->parent->relStoreLocation;
    node->indexonly = indexonly;

    /* cacultate the mem info of cstoreindexscan. */
    indexRel = relation_open(indexid, AccessShareLock);
    if (!indexonly && (indexRel->rd_rel->relam == CBTREE_AM_OID)) {
        int width = sizeof(Datum); /* the size of ctid */
        cost_sort(&sort_path,
            NIL,
            best_path->total_cost,
            PATH_LOCAL_ROWS(best_path),
            width,
            0.0,
            u_sess->opt_cxt.op_work_mem,
            0.0,
            root->glob->vectorized,
            SET_DOP(best_path->dop),
            &node->scan.mem_info);
    }
    relation_close(indexRel, NoLock);
    return node;
}

static DfsIndexScan* make_dfsindexscan(PlannerInfo* root, Path* best_path, List* qptlist, List* qpqual, Index scanrelid,
    Oid indexid, List* indexqual, List* indexqualorig, List* indexorderby, List* indexorderbyorig,
    IndexOptInfo* indexinfo, ScanDirection indexscandir, bool indexonly)
{
    DfsIndexScan* node = makeNode(DfsIndexScan);
    Plan* plan = &node->scan.plan;
    plan->lefttree = NULL;
    plan->righttree = NULL;
    plan->vec_output = true;
    Path sort_path;

    node->scan.scanrelid = scanrelid;
    node->indexid = indexid;
    node->indextlist = indexinfo->indextlist;
    node->indexqual = indexqual;
    node->indexqualorig = indexqualorig;
    node->indexorderby = indexorderby;
    node->indexorderbyorig = indexorderbyorig;
    node->indexorderdir = indexscandir;
    node->relStoreLocation = best_path->parent->relStoreLocation;
    node->cstorequal = fix_cstore_scan_qual(root, indexqual);

    /* For dfs index scan, we don't use the actual column data of the index rel, unless it is index-only scan. */
    if (indexonly) {
        node->dfsScan = create_dfsscan_plan(root, best_path, qptlist, qpqual, true, indexinfo->indextlist, true);
    } else {
        node->dfsScan = create_dfsscan_plan(root, best_path, qptlist, qpqual, true, NIL, false);
    }

    node->indexonly = indexonly;
    plan->qual = node->dfsScan->plan.qual;
    plan->targetlist = node->dfsScan->plan.targetlist;
    node->indexScantlist = fix_dfs_index_target_list(root, best_path, node, indexinfo, plan->qual);

    /* cacultate the mem info of cstoreindexscan. */
    int maxBatchRows = MAX_BATCH_ROWS;
    double rows = 0;
    int width = sizeof(Datum); /* the size of ctid */
    if (indexonly) {
        rows = maxBatchRows;
        cost_sort(&sort_path,
            NIL,
            best_path->total_cost,
            rows,
            width,
            0.0,
            u_sess->opt_cxt.op_work_mem,
            0.0,
            root->glob->vectorized,
            SET_DOP(best_path->dop),
            &node->scan.mem_info);
    } else {
        rows = PATH_LOCAL_ROWS(best_path);
        cost_sort(&sort_path,
            NIL,
            best_path->total_cost,
            rows,
            width,
            0.0,
            u_sess->opt_cxt.op_work_mem,
            0.0,
            root->glob->vectorized,
            SET_DOP(best_path->dop),
            &node->scan.mem_info);
    }

    return node;
}

static BitmapIndexScan* make_bitmap_indexscan(Index scanrelid, Oid indexid, List* indexqual, List* indexqualorig)
{
    BitmapIndexScan* node = makeNode(BitmapIndexScan);
    Plan* plan = &node->scan.plan;

    /* cost should be inserted by caller */
    plan->targetlist = NIL; /* not used */
    plan->qual = NIL;       /* not used */
    plan->lefttree = NULL;
    plan->righttree = NULL;
    node->scan.scanrelid = scanrelid;
    node->indexid = indexid;
    node->indexqual = indexqual;
    node->indexqualorig = indexqualorig;

    return node;
}

static BitmapHeapScan* make_bitmap_heapscan(
    List* qptlist, List* qpqual, Plan* lefttree, List* bitmapqualorig, Index scanrelid)
{
    BitmapHeapScan* node = makeNode(BitmapHeapScan);
    Plan* plan = &node->scan.plan;

    /* cost should be inserted by caller */
    plan->targetlist = qptlist;
    plan->qual = qpqual;
    plan->lefttree = lefttree;
    plan->righttree = NULL;
    node->scan.scanrelid = scanrelid;
    node->bitmapqualorig = bitmapqualorig;
#ifdef STREAMPLAN
    inherit_plan_locator_info((Plan*)node, lefttree);
#endif
    return node;
}

static CStoreIndexCtidScan* make_cstoreindex_ctidscan(
    PlannerInfo* root, Index scanrelid, Oid indexid, List* indexqual, List* indexqualorig, List* indextlist)
{
    CStoreIndexCtidScan* node = makeNode(CStoreIndexCtidScan);
    Plan* plan = &node->scan.plan;

    /* cost should be inserted by caller */
    plan->targetlist = NIL; /* not used */
    plan->qual = NIL;       /* not used */
    plan->lefttree = NULL;
    plan->righttree = NULL;
    plan->vec_output = true;
    node->scan.scanrelid = scanrelid;
    node->indexid = indexid;
    node->cstorequal = fix_cstore_scan_qual(root, indexqual);
    node->indexqual = indexqual;
    node->indexqualorig = indexqualorig;
    node->indextlist = indextlist;

    return node;
}

static CStoreIndexHeapScan* make_cstoreindex_heapscan(PlannerInfo* root, Path* best_path, List* qptlist, List* qpqual,
    Plan* lefttree, List* bitmapqualorig, Index scanrelid)
{
    CStoreIndexHeapScan* node = makeNode(CStoreIndexHeapScan);
    Plan* plan = &node->scan.plan;
    Path sort_path;

    if (qptlist != NIL)
        list_free_ext(qptlist);
    qptlist = build_relation_tlist(best_path->parent);
    if (qptlist == NIL)
        qptlist = build_one_column_tlist(root, best_path->parent);

    /* cost should be inserted by caller */
    plan->targetlist = qptlist;
    plan->qual = qpqual;
    plan->lefttree = lefttree;
    plan->vec_output = true;
    plan->righttree = NULL;
    node->scan.scanrelid = scanrelid;
    node->bitmapqualorig = bitmapqualorig;
#ifdef STREAMPLAN
    inherit_plan_locator_info((Plan*)node, lefttree);
#endif

    int width = sizeof(Datum); /* the size of ctid */
    cost_sort(&sort_path,
        NIL,
        lefttree->total_cost,
        PLAN_LOCAL_ROWS(lefttree),
        width,
        0.0,
        u_sess->opt_cxt.op_work_mem,
        0.0,
        root->glob->vectorized,
        SET_DOP(lefttree->dop),
        &node->scan.mem_info);

    return node;
}

static TidScan* make_tidscan(List* qptlist, List* qpqual, Index scanrelid, List* tidquals)
{
    TidScan* node = makeNode(TidScan);
    Plan* plan = &node->scan.plan;

    /* cost should be inserted by caller */
    plan->targetlist = qptlist;
    plan->qual = qpqual;
    plan->lefttree = NULL;
    plan->righttree = NULL;
    node->scan.scanrelid = scanrelid;
    node->tidquals = tidquals;

    return node;
}

SubqueryScan* make_subqueryscan(List* qptlist, List* qpqual, Index scanrelid, Plan* subplan)
{
    SubqueryScan* node = makeNode(SubqueryScan);
    Plan* plan = &node->scan.plan;

#ifdef STREAMPLAN
    inherit_plan_locator_info(plan, subplan);
#endif

    /*
     * Cost is figured here for the convenience of prepunion.c.  Note this is
     * only correct for the case where qpqual is empty; otherwise caller
     * should overwrite cost with a better estimate.
     */
    copy_plan_costsize(plan, subplan);
    plan->total_cost += u_sess->attr.attr_sql.cpu_tuple_cost * PLAN_LOCAL_ROWS(subplan);

    plan->targetlist = qptlist;
    plan->qual = qpqual;
    plan->lefttree = NULL;
    plan->righttree = NULL;
    node->scan.scanrelid = scanrelid;
    node->subplan = subplan;

    return node;
}

/*
 * Support partition index unusable.
 * Hypothetical index does not support partition index unusable.
 *
 */
Plan* create_globalpartInterator_plan(PlannerInfo* root, PartIteratorPath* pIterpath)
{
    Plan* plan = NULL;

    /* The subpath is index path. */
    if (u_sess->attr.attr_sql.enable_hypo_index == false && is_partitionIndex_Subpath(pIterpath->subPath)) {
        Path* index_path = pIterpath->subPath;

        /* Get the usable type of the index subpath. */
        IndexesUsableType usable_type =
            eliminate_partition_index_unusable(((IndexPath*)index_path)->indexinfo->indexoid,
                index_path->parent->pruning_result,
                &index_path->parent->pruning_result_for_index_usable,
                &index_path->parent->pruning_result_for_index_unusable);

        if (!index_path->parent->pruning_result_for_index_usable) {
            index_path->parent->partItrs_for_index_usable = 0;
        } else {
            index_path->parent->partItrs_for_index_usable =
                bms_num_members(index_path->parent->pruning_result_for_index_usable->bm_rangeSelectedPartitions);
        }
        if (!index_path->parent->pruning_result_for_index_unusable) {
            index_path->parent->partItrs_for_index_unusable = 0;
        } else {
            index_path->parent->partItrs_for_index_unusable =
                bms_num_members(index_path->parent->pruning_result_for_index_unusable->bm_rangeSelectedPartitions);
        }

        switch (usable_type) {
            case INDEXES_FULL_USABLE: {
                /* Create partition iterator with index scan plan. */
                GlobalPartIterator* gpIter = (GlobalPartIterator*)palloc(sizeof(GlobalPartIterator));
                gpIter->curItrs = pIterpath->subPath->parent->partItrs;
                gpIter->pruningResult = pIterpath->subPath->parent->pruning_result;
                plan = (Plan*)create_partIterator_plan(root, pIterpath, gpIter);
            } break;
            case INDEXES_NONE_USABLE: {
                /* Create partition iterator with seq scan plan. */
                GlobalPartIterator* gpIter = (GlobalPartIterator*)palloc(sizeof(GlobalPartIterator));
                gpIter->curItrs = pIterpath->subPath->parent->partItrs_for_index_unusable;
                gpIter->pruningResult = pIterpath->subPath->parent->pruning_result_for_index_unusable;
                pIterpath->subPath = build_seqScanPath_by_indexScanPath(root, pIterpath->subPath);
                plan = (Plan*)create_partIterator_plan(root, pIterpath, gpIter);
            } break;
            case INDEXES_PARTIAL_USABLE: {
                /* Create partition iterator with partial index and partial seq scan plan. */
                Append* appendPlan = NULL;
                List* subplans = NIL;
                Plan* piterSeqPlan = NULL;
                GlobalPartIterator* gpIterSeq = (GlobalPartIterator*)palloc(sizeof(GlobalPartIterator));
                Plan* piterIndexPlan = NULL;
                GlobalPartIterator* gpIterIndex = (GlobalPartIterator*)palloc(sizeof(GlobalPartIterator));

                /* Partial index scan plan */
                gpIterIndex->curItrs = pIterpath->subPath->parent->partItrs_for_index_usable;
                gpIterIndex->pruningResult = pIterpath->subPath->parent->pruning_result_for_index_usable;
                piterIndexPlan = (Plan*)create_partIterator_plan(root, pIterpath, gpIterIndex);
                subplans = lappend(subplans, piterIndexPlan);

                /* Partial seq scan plan */
                gpIterSeq->curItrs = pIterpath->subPath->parent->partItrs_for_index_unusable;
                gpIterSeq->pruningResult = pIterpath->subPath->parent->pruning_result_for_index_unusable;
                pIterpath->subPath = build_seqScanPath_by_indexScanPath(root, pIterpath->subPath);
                piterSeqPlan = (Plan*)create_partIterator_plan(root, pIterpath, gpIterSeq);
                subplans = lappend(subplans, piterSeqPlan);

                /* The targetlist of append subplans */
                piterSeqPlan->targetlist = piterIndexPlan->targetlist;
                piterSeqPlan->lefttree->targetlist = piterIndexPlan->lefttree->targetlist;

                /* Add append nodes for two partial partitin iterator plan. */
                appendPlan = make_append(subplans, piterIndexPlan->targetlist);

                plan = (Plan*)appendPlan;

#ifdef STREAMPLAN
                inherit_plan_locator_info(plan, piterIndexPlan);
#endif
            } break;
            default:
                break;
        }
    } else if (is_pwj_path((Path*)pIterpath)) {
        plan = (Plan*)create_partIterator_plan(root, pIterpath, NULL);
    } else { /* Other pathes. */
        if (u_sess->attr.attr_sql.enable_hypo_index && is_partitionIndex_Subpath(pIterpath->subPath) &&
            ((IndexPath *)pIterpath->subPath)->indexinfo->hypothetical) {
            pIterpath->subPath->parent->partItrs_for_index_usable =
                bms_num_members(pIterpath->subPath->parent->pruning_result->bm_rangeSelectedPartitions);
            pIterpath->subPath->parent->partItrs_for_index_unusable = 0;
        }
        GlobalPartIterator* gpIter = (GlobalPartIterator*)palloc(sizeof(GlobalPartIterator));
        gpIter->curItrs = pIterpath->subPath->parent->partItrs;
        gpIter->pruningResult = pIterpath->subPath->parent->pruning_result;
        plan = (Plan*)create_partIterator_plan(root, pIterpath, gpIter);
    }

    return plan;
}

static PartIterator* create_partIterator_plan(
    PlannerInfo* root, PartIteratorPath* pIterpath, GlobalPartIterator* gpIter)
{
    /* Update the partition interator infomation. */
    if (gpIter != NULL) {
        pIterpath->itrs = gpIter->curItrs;
        pIterpath->path.parent->partItrs = gpIter->curItrs;
        pIterpath->path.parent->pruning_result = gpIter->pruningResult;
        pIterpath->subPath->parent->partItrs = gpIter->curItrs;
        pIterpath->subPath->parent->pruning_result = gpIter->pruningResult;
    }
    /* Construct PartIterator plan node */
    PartIterator* partItr = makeNode(PartIterator);
    partItr->direction = pIterpath->direction;
    partItr->itrs = pIterpath->itrs;
    partItr->partType = pIterpath->partType;

    /* Construct partition iterator param */
    PartIteratorParam* piParam = makeNode(PartIteratorParam);
    piParam->paramno = assignPartIteratorParam(root);
    piParam->subPartParamno = assignPartIteratorParam(root);
    partItr->param = piParam;

    /*
     * Store partition parameter in PlannerInfo,
     * it will be used by scan plan which is offspring of this iterator plan node.
     */
    root->curIteratorParamIndex = piParam->paramno;
    root->curSubPartIteratorParamIndex = piParam->subPartParamno;
    root->isPartIteratorPlanning = true;
    root->curItrs = pIterpath->itrs;

    /* construct sub plan */
    partItr->plan.lefttree = create_plan_recurse(root, pIterpath->subPath);

    /* constrcut PartIterator attributes */
    partItr->plan.targetlist = partItr->plan.lefttree->targetlist;

    Bitmapset* extparams = (Bitmapset*)copyObject(partItr->plan.extParam);
    partItr->plan.extParam = bms_add_member(extparams, piParam->paramno);
    partItr->plan.extParam = bms_add_member(extparams, piParam->subPartParamno);

    Bitmapset* allparams = (Bitmapset*)copyObject(partItr->plan.allParam);
    partItr->plan.allParam = bms_add_member(allparams, piParam->paramno);
    partItr->plan.allParam = bms_add_member(allparams, piParam->subPartParamno);

    root->isPartIteratorPlanning = false;
    root->curIteratorParamIndex = 0;
    root->curSubPartIteratorParamIndex = 0;

#ifdef STREAMPLAN
    inherit_plan_locator_info(&(partItr->plan), partItr->plan.lefttree);
#endif

    copy_path_costsize(&partItr->plan, &pIterpath->path);

    /*
     * If there are any pseudoconstant clauses attached to sub plan node, we should
     * move the inserted gating Result node that evaluates the pseudoconstants as
     * one-time quals to the top of PartIterator node.
     */
    if (IsA(partItr->plan.lefttree, BaseResult)) {
        Plan* plan = partItr->plan.lefttree;
        if (is_dummy_plan(plan)) {
            plan->lefttree = NULL;
        } else {
            partItr->plan.lefttree = partItr->plan.lefttree->lefttree;
            plan->lefttree = (Plan*)partItr;
        }
        partItr = (PartIterator*)plan;
    }

    return partItr;
}

static FunctionScan* make_functionscan(List* qptlist, List* qpqual, Index scanrelid, Node* funcexpr, List* funccolnames,
    List* funccoltypes, List* funccoltypmods, List* funccolcollations)
{
    FunctionScan* node = makeNode(FunctionScan);
    Plan* plan = &node->scan.plan;

    /* cost should be inserted by caller */
    plan->targetlist = qptlist;
    plan->qual = qpqual;
    plan->lefttree = NULL;
    plan->righttree = NULL;
    node->scan.scanrelid = scanrelid;
    node->funcexpr = funcexpr;
    node->funccolnames = funccolnames;
    node->funccoltypes = funccoltypes;
    node->funccoltypmods = funccoltypmods;
    node->funccolcollations = funccolcollations;

    return node;
}

static ValuesScan* make_valuesscan(List* qptlist, List* qpqual, Index scanrelid, List* values_lists)
{
    ValuesScan* node = makeNode(ValuesScan);
    Plan* plan = &node->scan.plan;

    /* cost should be inserted by caller */
    plan->targetlist = qptlist;
    plan->qual = qpqual;
    plan->lefttree = NULL;
    plan->righttree = NULL;
    node->scan.scanrelid = scanrelid;
    node->values_lists = values_lists;

    return node;
}

static CteScan* make_ctescan(List* qptlist, List* qpqual, Index scanrelid, int ctePlanId, int cteParam)
{
    CteScan* node = makeNode(CteScan);
    Plan* plan = &node->scan.plan;

    /* cost should be inserted by caller */
    plan->targetlist = qptlist;
    plan->qual = qpqual;
    plan->lefttree = NULL;
    plan->righttree = NULL;
    node->scan.scanrelid = scanrelid;
    node->ctePlanId = ctePlanId;
    node->cteParam = cteParam;

    return node;
}

static WorkTableScan* make_worktablescan(List* qptlist, List* qpqual, Index scanrelid, int wtParam)
{
    WorkTableScan* node = makeNode(WorkTableScan);
    Plan* plan = &node->scan.plan;

    /* cost should be inserted by caller */
    plan->targetlist = qptlist;
    plan->qual = qpqual;
    plan->lefttree = NULL;
    plan->righttree = NULL;
    node->scan.scanrelid = scanrelid;
    node->wtParam = wtParam;

    return node;
}

ForeignScan* make_foreignscan(
    List* qptlist, List* qpqual, Index scanrelid, List* fdw_exprs, List* fdw_private, RemoteQueryExecType type)
{
    ForeignScan* node = makeNode(ForeignScan);

    Plan* plan = &node->scan.plan;

    /* cost will be filled in by create_foreignscan_plan */
    plan->targetlist = qptlist;
    plan->qual = qpqual;
    plan->lefttree = NULL;
    plan->righttree = NULL;
    plan->exec_type = type;
    plan->distributed_keys = NIL;
#ifdef ENABLE_MULTIPLE_NODES
    plan->distributed_keys =
        lappend(plan->distributed_keys, makeVar(0, InvalidAttrNumber, InvalidOid, -1, InvalidOid, 0));
#endif
    node->scan.scanrelid = scanrelid;
    node->fdw_exprs = fdw_exprs;
    node->fdw_private = fdw_private;
    /* fsSystemCol will be filled in by create_foreignscan_plan */
    node->fsSystemCol = false;

    /* @hdfs
     * Prunning Result is only used for hdfs partitioned foreign table. It is always null in other conditions.
     */
    node->prunningResult = NULL;

    return node;
}

Append* make_append(List* appendplans, List* tlist)
{
    Append* node = makeNode(Append);
    Plan* plan = &node->plan;
    double total_size;
    ListCell* subnode = NULL;
    double local_rows = 0;

    /*
     * Compute cost as sum of subplan costs.  We charge nothing extra for the
     * Append itself, which perhaps is too optimistic, but since it doesn't do
     * any selection or projection, it is a pretty cheap node.
     *
     * If you change this, see also create_append_path().  Also, the size
     * calculations should match set_append_rel_pathlist().  It'd be better
     * not to duplicate all this logic, but some callers of this function
     * aren't working from an appendrel or AppendPath, so there's noplace to
     * copy the data from.
     */
    init_plan_cost(plan);
    total_size = 0;

    bool all_parallelized = isAllParallelized(appendplans);
    if (appendplans != NIL && all_parallelized) {
        plan->dop = u_sess->opt_cxt.query_dop;
    } else {
        plan->dop = 1;
    }

    int max_num_exec_nodes = 0;
    foreach (subnode, appendplans) {
        Plan* subplan = (Plan*)lfirst(subnode);

        /*
         * Add local gather above the parallelized subplan
         * if not all subplans are parallized.
         */
        if (subplan->dop > 1 && !all_parallelized) {
            subplan = create_local_gather(subplan);
            lfirst(subnode) = (void*)subplan;
        }

        if (subnode == list_head(appendplans)) { /* first node? */
            plan->startup_cost = subplan->startup_cost;
            plan->exec_nodes = ng_get_dest_execnodes(subplan);
            max_num_exec_nodes = list_length(plan->exec_nodes->nodeList);
        }
        plan->total_cost += subplan->total_cost;
        local_rows += PLAN_LOCAL_ROWS(subplan);
        plan->plan_rows += subplan->plan_rows;
        if (max_num_exec_nodes < list_length(plan->exec_nodes->nodeList)) {
            plan->exec_nodes = ng_get_dest_execnodes(subplan);
            max_num_exec_nodes = list_length(plan->exec_nodes->nodeList);
        }
        total_size += subplan->plan_width * PLAN_LOCAL_ROWS(subplan);
    }

    /* Calculate overal multiple for append path */
    if (plan->plan_rows != 0) {
        plan->multiple = local_rows / plan->plan_rows * list_length(plan->exec_nodes->nodeList);
    }

    /* calculate plan width */
    if (plan->plan_rows > 0) {
        plan->plan_width = (int)rint(total_size / PLAN_LOCAL_ROWS(plan));
    } else {
        plan->plan_width = 0;
    }

    plan->targetlist = tlist;
    plan->qual = NIL;
    plan->lefttree = NULL;
    plan->righttree = NULL;
    node->appendplans = appendplans;

    return node;
}

RecursiveUnion* make_recursive_union(
    List* tlist, Plan* lefttree, Plan* righttree, int wtParam, List* distinctList, long numGroups)
{
    RecursiveUnion* node = makeNode(RecursiveUnion);
    Plan* plan = &node->plan;
    int numCols = list_length(distinctList);

    cost_recursive_union(plan, lefttree, righttree);

    plan->targetlist = tlist;
    plan->qual = NIL;
    plan->lefttree = lefttree;
    plan->righttree = righttree;
    node->wtParam = wtParam;

    /*
     * convert SortGroupClause list into arrays of attr indexes and equality
     * operators, as wanted by executor
     */
    node->numCols = numCols;
    if (numCols > 0) {
        int keyno = 0;
        AttrNumber* dupColIdx = NULL;
        Oid* dupOperators = NULL;
        ListCell* slitem = NULL;

        dupColIdx = (AttrNumber*)palloc(sizeof(AttrNumber) * numCols);
        dupOperators = (Oid*)palloc(sizeof(Oid) * numCols);

        foreach (slitem, distinctList) {
            SortGroupClause* sortcl = (SortGroupClause*)lfirst(slitem);
            TargetEntry* tle = get_sortgroupclause_tle(sortcl, plan->targetlist);

            dupColIdx[keyno] = tle->resno;
            dupOperators[keyno] = sortcl->eqop;
            Assert(OidIsValid(dupOperators[keyno]));
            keyno++;
        }
        node->dupColIdx = dupColIdx;
        node->dupOperators = dupOperators;
    }
    node->numGroups = numGroups;

    return node;
}

static BitmapAnd* make_bitmap_and(List* bitmapplans)
{
    BitmapAnd* node = makeNode(BitmapAnd);
    Plan* plan = &node->plan;

    /* cost should be inserted by caller */
    plan->targetlist = NIL;
    plan->qual = NIL;
    plan->lefttree = NULL;
    plan->righttree = NULL;
    node->bitmapplans = bitmapplans;
#ifdef STREAMPLAN
    if (bitmapplans != NIL) {
        inherit_plan_locator_info((Plan*)node, (Plan*)linitial(bitmapplans));
    }
#endif

    return node;
}

static CStoreIndexAnd* make_cstoreindex_and(List* ctidplans)
{
    CStoreIndexAnd* node = makeNode(CStoreIndexAnd);
    Plan* plan = &node->plan;

    /* cost should be inserted by caller */
    plan->targetlist = NIL;
    plan->qual = NIL;
    plan->lefttree = NULL;
    plan->righttree = NULL;
    node->bitmapplans = ctidplans;
#ifdef STREAMPLAN
    if (ctidplans != NIL) {
        inherit_plan_locator_info((Plan*)node, (Plan*)linitial(ctidplans));
    }
#endif

    return node;
}

static BitmapOr* make_bitmap_or(List* bitmapplans)
{
    BitmapOr* node = makeNode(BitmapOr);
    Plan* plan = &node->plan;

    /* cost should be inserted by caller */
    plan->targetlist = NIL;
    plan->qual = NIL;
    plan->lefttree = NULL;
    plan->righttree = NULL;
    node->bitmapplans = bitmapplans;
#ifdef STREAMPLAN
    if (bitmapplans != NIL) {
        inherit_plan_locator_info((Plan*)node, (Plan*)linitial(bitmapplans));
    }
#endif

    return node;
}

static CStoreIndexOr* make_cstoreindex_or(List* ctidplans)
{
    CStoreIndexOr* node = makeNode(CStoreIndexOr);
    Plan* plan = &node->plan;

    /* cost should be inserted by caller */
    plan->targetlist = NIL;
    plan->qual = NIL;
    plan->lefttree = NULL;
    plan->righttree = NULL;
    node->bitmapplans = ctidplans;
#ifdef STREAMPLAN
    if (ctidplans != NIL) {
        inherit_plan_locator_info((Plan*)node, (Plan*)linitial(ctidplans));
    }
#endif

    return node;
}

static NestLoop* make_nestloop(List* tlist, List* joinclauses, List* otherclauses, List* nestParams, Plan* lefttree,
    Plan* righttree, JoinType jointype)
{
    NestLoop* node = makeNode(NestLoop);
    Plan* plan = &node->join.plan;

    /* cost should be inserted by caller */
    plan->targetlist = tlist;
    plan->qual = otherclauses;
    plan->lefttree = lefttree;
    plan->righttree = righttree;
    node->join.jointype = jointype;
    node->join.joinqual = joinclauses;
    node->nestParams = nestParams;

    return node;
}

/* estimate_directHashjoin_Cost
 * the function used to estimate the mem_info for join_plan,	refered to the function initial_cost_hashjoin.
 * it used to copy the inner_mem_info\cost\plan_rows and plan_width to the join_plan.
 *
 * @param[IN] root: the plannerInfo for this join.
 * @param[IN] hashclauses: the RestrictInfo nodes to use as hash clauses.
 * @param[IN] outerPlan: the outer plan for join.
 * @param[IN] hash_plan: the inner plan for join. it is the intermediate result set that need to join with original
 * table.
 * @param[IN] join_plan: the result for hash_join. the inner_mem_info written in  join_plan.
 */
static void estimate_directHashjoin_Cost(
    PlannerInfo* root, List* hashclauses, Plan* outerPlan, Plan* hash_plan, HashJoin* join_plan)
{
    /* cost estimate */
    double outer_path_rows = outerPlan->plan_rows;
    double inner_path_rows = hash_plan->plan_rows;
    Cost startup_cost = 0;
    Cost run_cost = 0;
    OpMemInfo inner_mem_info;
    int num_hashclauses = list_length(hashclauses);
    int numbuckets;
    int numbatches;
    int num_skew_mcvs;
    int inner_width = hash_plan->plan_width; /* width of inner rel */
    int outer_width = outerPlan->plan_width; /* width of outer rel */

    errno_t rc = 0;
    rc = memset_s(&inner_mem_info, sizeof(OpMemInfo), 0, sizeof(OpMemInfo));
    securec_check(rc, "\0", "\0");
    /* cost of source data */
    startup_cost += outerPlan->startup_cost;
    run_cost += outerPlan->total_cost - outerPlan->startup_cost;

    if (!u_sess->attr.attr_sql.enable_change_hjcost)
        startup_cost += hash_plan->total_cost;
    else {
        startup_cost += hash_plan->startup_cost;
        run_cost += hash_plan->total_cost - hash_plan->startup_cost;
    }

    startup_cost += (u_sess->attr.attr_sql.cpu_operator_cost * num_hashclauses + u_sess->attr.attr_sql.cpu_tuple_cost +
                        u_sess->attr.attr_sql.allocate_mem_cost) *
                    inner_path_rows;
    run_cost += u_sess->attr.attr_sql.cpu_operator_cost * num_hashclauses * outer_path_rows;

    ExecChooseHashTableSize(inner_path_rows,
        inner_width,
        true, /* useskew */
        &numbuckets,
        &numbatches,
        &num_skew_mcvs,
        u_sess->opt_cxt.op_work_mem,
        root->glob->vectorized,
        &inner_mem_info);

    /*
     * If inner relation is too big then we will need to "batch" the join,
     * which implies writing and reading most of the tuples to disk an extra
     * time.  Charge seq_page_cost per page, since the I/O should be nice and
     * sequential.	Writing the inner rel counts as startup cost, all the rest
     * as run cost.
     */
    double outerpages = cost_page_size(outer_path_rows, outer_width);
    double innerpages = cost_page_size(inner_path_rows, inner_width);
    double startuppagecost = u_sess->attr.attr_sql.seq_page_cost * innerpages;
    double runpagecost = u_sess->attr.attr_sql.seq_page_cost * (innerpages + 2 * outerpages);

    if (numbatches > 1) {
        startup_cost += startuppagecost;
        run_cost += runpagecost;
    }

    /* Set mem info for hash join path */
    inner_mem_info.minMem = inner_mem_info.maxMem / HASH_MAX_DISK_SIZE;
    inner_mem_info.opMem = u_sess->opt_cxt.op_work_mem;
    inner_mem_info.regressCost = (startuppagecost + runpagecost);

    copy_mem_info(&join_plan->mem_info, &inner_mem_info);

    /* estimate the cost of final hash join  */
    join_plan->join.plan.startup_cost = startup_cost;
    join_plan->join.plan.total_cost = startup_cost + run_cost;
    join_plan->join.plan.plan_rows = ((inner_path_rows > outer_path_rows) ? outer_path_rows : inner_path_rows);
    join_plan->join.plan.plan_width = outer_width;
}

HashJoin* create_direct_hashjoin(
    PlannerInfo* root, Plan* outerPlan, Plan* innerPlan, List* tlist, List* joinClauses, JoinType joinType)
{
    List* hashclauses = NIL;
    List* nulleqqual = NIL;
    ListCell* cell = NULL;
    Plan* hash_plan = NULL;
    HashJoin* join_plan = NULL;
    Oid skewTable = InvalidOid;
    AttrNumber skewColumn = InvalidAttrNumber;
    bool skewInherit = false;
    Oid skewColType = InvalidOid;
    int32 skewColTypmod = -1;
    ParseState* pstate = make_parsestate(NULL);
    ListCell* l = NULL;

    joinClauses = order_qual_clauses(root, joinClauses);

    /* Scan the join's restrictinfo list to find hashjoinable clauses, reference function hash_inner_and_outer(). */
    foreach (l, joinClauses) {
        RestrictInfo* restrictinfo = (RestrictInfo*)lfirst(l);

        Assert(IsA(restrictinfo, RestrictInfo));
        Assert(!restrictinfo->pseudoconstant);

        /*
         * If processing an outer join, only use its own join clauses for
         * hashing.  For inner joins we need not be so picky.
         */
        if (IS_OUTER_JOIN((uint32)joinType) && restrictinfo->is_pushed_down)
            continue;

        if (!restrictinfo->can_join || restrictinfo->hashjoinoperator == InvalidOid)
            continue; /* not hashjoinable */

        /*
         * skip qual that contains sublink
         */
        if (contain_subplans((Node*)restrictinfo->clause))
            continue;

        hashclauses = lappend(hashclauses, restrictinfo->clause);
    }

    joinClauses = get_actual_clauses(joinClauses);
    joinClauses = list_difference(joinClauses, hashclauses);

    if (1 == list_length(hashclauses)) {
        OpExpr* clause = (OpExpr*)linitial(hashclauses);
        Node* node = NULL;

        Assert(is_opclause(clause));
        node = (Node*)linitial(clause->args);
        if (IsA(node, RelabelType))
            node = (Node*)((RelabelType*)node)->arg;
        if (IsA(node, Var)) {
            Var* var = (Var*)node;
            RangeTblEntry* rte = (RangeTblEntry*)list_nth(root->parse->rtable, var->varno - 1);

            if (rte->rtekind == RTE_RELATION) {
                skewTable = rte->relid;
                skewColumn = var->varattno;
                skewInherit = rte->inh;
                skewColType = var->vartype;
                skewColTypmod = var->vartypmod;
            }
        }
    }

    hash_plan = (Plan*)make_hash(innerPlan, skewTable, skewColumn, skewInherit, skewColType, skewColTypmod);
    join_plan = make_hashjoin(tlist, joinClauses, NIL, hashclauses, outerPlan, hash_plan, joinType);

    /* estimate the mem_info for join_plan,  refered to the function initial_cost_hashjoin */
    estimate_directHashjoin_Cost(root, hashclauses, outerPlan, hash_plan, join_plan);

    foreach (cell, hashclauses) {
        OpExpr* op = (OpExpr*)lfirst(cell);
        Var* var1 = (Var*)linitial(op->args);
        Var* var2 = (Var*)lsecond(op->args);
        Expr* distinct_expr =
            make_notclause(make_distinct_op(pstate, list_make1(makeString("=")), (Node*)var1, (Node*)var2, -1));
        nulleqqual = lappend(nulleqqual, distinct_expr);
    }

    assign_expr_collations(pstate, (Node*)nulleqqual);

    join_plan->transferFilterFlag = CanTransferInJoin(joinType);
    join_plan->join.plan.exec_type = EXEC_ON_DATANODES;
    join_plan->join.plan.exec_nodes = stream_merge_exec_nodes(outerPlan, hash_plan, ENABLE_PRED_PUSH(root));
    join_plan->streamBothSides =
        contain_special_plan_node(outerPlan, T_Stream) || contain_special_plan_node(hash_plan, T_Stream);
    join_plan->join.nulleqqual = nulleqqual;
    free_parsestate(pstate);

    return join_plan;
}

typedef struct replace_scan_clause_context {
    Index dest_idx;
    List* scan_clauses;
} replace_scan_clause_context;

static bool replace_scan_clause_walker(Node* node, replace_scan_clause_context* context)
{
    if (node == NULL)
        return false;

    if (IsA(node, Var)) {
        ((Var*)node)->varno = context->dest_idx;
        return false;
    }

    if (IsA(node, HashFilter))
        return true;

    return expression_tree_walker(node, (bool (*)())replace_scan_clause_walker, (void*)context);
}

static List* replace_scan_clause(List* scan_clauses, Index idx)
{
    replace_scan_clause_context context;
    ListCell* cell = NULL;

    context.dest_idx = idx;
    context.scan_clauses = NIL;

    foreach (cell, scan_clauses) {
        Node* clause = (Node*)lfirst(cell);

        if (replace_scan_clause_walker(clause, &context))
            continue;

        context.scan_clauses = lappend(context.scan_clauses, clause);
    }

    return context.scan_clauses;
}

Plan* create_direct_scan(PlannerInfo* root, List* tlist, RangeTblEntry* realResultRTE, Index src_idx, Index dest_idx)
{
    Plan* result = NULL;
    RelOptInfo* rel = root->simple_rel_array[src_idx];
    List* scan_clauses = NIL;
    Path* dummyPath = makeNode(Path);
    RelOptInfo* dummyRel = makeNode(RelOptInfo);
    RelationLocInfo* rel_loc_info = NULL;

    dummyRel->relid = rel->relid;
    dummyRel->rtekind = rel->rtekind;
    dummyRel->reloptkind = rel->reloptkind;
    dummyRel->isPartitionedTable = rel->isPartitionedTable;
    dummyRel->rows = rel->rows;
    dummyRel->width = rel->width;
    dummyRel->pages = rel->pages;
    dummyRel->tuples = rel->tuples;
    dummyRel->multiple = rel->multiple;
    dummyRel->reltablespace = rel->reltablespace;
    dummyRel->baserestrictcost.per_tuple = rel->baserestrictcost.per_tuple;
    dummyRel->baserestrictcost.startup = rel->baserestrictcost.startup;
    dummyPath->parent = dummyRel;

    scan_clauses = (List*)copyObject(rel->baserestrictinfo);
    scan_clauses = extract_actual_clauses(scan_clauses, false);
    scan_clauses = replace_scan_clause(scan_clauses, dest_idx);
    if (realResultRTE->orientation == REL_COL_ORIENTED) {
        CStoreScan* scan_plan = make_cstorescan(tlist, scan_clauses, dest_idx);
        scan_plan->is_replica_table = true;
        scan_plan->relStoreLocation = LOCAL_STORE;
        cost_cstorescan(dummyPath, root, dummyRel);

        /*Fix the qual to support pushing predicate down to cstore scan.*/
        if (u_sess->attr.attr_sql.enable_csqual_pushdown)
            scan_plan->cstorequal = fix_cstore_scan_qual(root, scan_clauses);
        else
            scan_plan->cstorequal = NIL;

        result = (Plan*)scan_plan;
    } else if (realResultRTE->orientation == REL_ROW_ORIENTED) {
        result = (Plan*)make_seqscan(tlist, scan_clauses, dest_idx);
        result->isDeltaTable = false;
        cost_seqscan(dummyPath, root, dummyRel, NULL);
    } else if (realResultRTE->orientation == REL_TIMESERIES_ORIENTED) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Unsupported Direct Scan FOR TIMESERIES.")));
    } else {
        /* Just for local store */
        Assert(false);
    }

    rel_loc_info = GetRelationLocInfo(realResultRTE->relid);
    result->exec_type = EXEC_ON_DATANODES;
    result->distributed_keys = NIL;
    result->exec_nodes = GetRelationNodes(rel_loc_info, NULL, NULL, NULL, NULL, RELATION_ACCESS_READ);

    copy_path_costsize(result, dummyPath);

    if (rel->isPartitionedTable) {
        PartIterator* partItr = makeNode(PartIterator);
        PartIteratorParam* piParam = makeNode(PartIteratorParam);
        Bitmapset* extparams = NULL;
        Bitmapset* allparams = NULL;
        Scan* scan = (Scan*)result;

        partItr->direction = ForwardScanDirection;
        partItr->itrs = rel->partItrs;
        partItr->plan.lefttree = result;
        partItr->plan.targetlist = result->targetlist;

        partItr->param = piParam;
        piParam->paramno = assignPartIteratorParam(root);
        piParam->subPartParamno = assignPartIteratorParam(root);

        extparams = (Bitmapset*)copyObject(partItr->plan.extParam);
        partItr->plan.extParam = bms_add_member(extparams, piParam->paramno);
        partItr->plan.extParam = bms_add_member(extparams, piParam->subPartParamno);

        allparams = (Bitmapset*)copyObject(partItr->plan.allParam);
        partItr->plan.allParam = bms_add_member(allparams, piParam->paramno);
        partItr->plan.allParam = bms_add_member(allparams, piParam->subPartParamno);

        scan->isPartTbl = true;
        scan->partScanDirection = ForwardScanDirection;
        scan->itrs = rel->partItrs;
        scan->plan.paramno = piParam->paramno;
        scan->plan.subparamno = piParam->subPartParamno;
        scan->pruningInfo = copyPruningResult(rel->pruning_result);

#ifdef STREAMPLAN
        inherit_plan_locator_info(&(partItr->plan), partItr->plan.lefttree);
#endif

        result = (Plan*)partItr;
    }

    ((Scan*)result)->bucketInfo = (BucketInfo*)copyObject(rel->bucketInfo);
    return result;
}

/*
 * @Description: modifyed Delete/Update plan
 * @IN root: information for planning
 * @IN subplan: source suuplan for Delete/Update action
 * @IN distinctList: a list of SortGroupClauses for Unique
 * @IN uniq_exprs: uniq expression to estimate rows after unique action
 * @IN target_exec_nodes: ExecNodes for result realtion
 * @Return: modifyed plan
 * @See also:
 */
Plan* create_direct_righttree(
    PlannerInfo* root, Plan* righttree, List* distinctList, List* uniq_exprs, ExecNodes* target_exec_nodes)
{
    bool is_replicated = is_replicated_plan(righttree);

    if (uniq_exprs != NIL) {
        righttree = (Plan*)make_sort_from_sortclauses(root, distinctList, righttree);
        righttree = (Plan*)make_unique(righttree, distinctList);
        righttree->plan_rows = estimate_num_groups(
            root, uniq_exprs, righttree->plan_rows, (is_replicated ? STATS_TYPE_GLOBAL : STATS_TYPE_LOCAL));
    }

    if (is_replicated) {
        if (ng_is_same_group(righttree->exec_nodes->nodeList, target_exec_nodes->nodeList))
            return righttree;

        righttree->exec_nodes->nodeList = list_make1_int(pickup_random_datanode_from_plan(righttree));
        pushdown_execnodes(righttree, righttree->exec_nodes);
        return (Plan*)make_stream_plan(root, righttree, NIL, 1.0, &target_exec_nodes->distribution);
    }

    righttree = (Plan*)make_stream_plan(root, righttree, NIL, 1.0, &target_exec_nodes->distribution);

    if (uniq_exprs != NIL) {
        righttree = (Plan*)make_sort_from_sortclauses(root, distinctList, righttree);
        righttree = (Plan*)make_unique(righttree, distinctList);
        righttree->plan_rows = estimate_num_groups(root, uniq_exprs, righttree->plan_rows, STATS_TYPE_GLOBAL);
    }

    return righttree;
}

static HashJoin* make_hashjoin(List* tlist, List* joinclauses, List* otherclauses, List* hashclauses, Plan* lefttree,
    Plan* righttree, JoinType jointype)
{
    HashJoin* node = makeNode(HashJoin);
    Plan* plan = &node->join.plan;

    /* cost should be inserted by caller */
    plan->targetlist = tlist;
    plan->qual = otherclauses;
    plan->lefttree = lefttree;
    plan->righttree = righttree;
    node->hashclauses = hashclauses;
    node->join.jointype = jointype;
    node->join.joinqual = joinclauses;

    return node;
}

static Hash* make_hash(
    Plan* lefttree, Oid skewTable, AttrNumber skewColumn, bool skewInherit, Oid skewColType, int32 skewColTypmod)
{
    Hash* node = makeNode(Hash);
    Plan* plan = &node->plan;

#ifdef STREAMPLAN
    inherit_plan_locator_info((Plan*)node, lefttree);
#endif

    copy_plan_costsize(plan, lefttree);

    /*
     * For plausibility, make startup & total costs equal total cost of input
     * plan; this only affects EXPLAIN display not decisions.
     */
    plan->startup_cost = plan->total_cost;
    plan->targetlist = lefttree->targetlist;
    plan->qual = NIL;
    plan->lefttree = lefttree;
    plan->righttree = NULL;
    plan->hasUniqueResults = lefttree->hasUniqueResults;

    node->skewTable = skewTable;
    node->skewColumn = skewColumn;
    node->skewInherit = skewInherit;
    node->skewColType = skewColType;
    node->skewColTypmod = skewColTypmod;

    return node;
}

static MergeJoin* make_mergejoin(List* tlist, List* joinclauses, List* otherclauses, List* mergeclauses,
    Oid* mergefamilies, Oid* mergecollations, int* mergestrategies, bool* mergenullsfirst, Plan* lefttree,
    Plan* righttree, JoinType jointype)
{
    MergeJoin* node = makeNode(MergeJoin);
    Plan* plan = &node->join.plan;

    /* cost should be inserted by caller */
    plan->targetlist = tlist;
    plan->qual = otherclauses;
    plan->lefttree = lefttree;
    plan->righttree = righttree;
    node->mergeclauses = mergeclauses;
    node->mergeFamilies = mergefamilies;
    node->mergeCollations = mergecollations;
    node->mergeStrategies = mergestrategies;
    node->mergeNullsFirst = mergenullsfirst;
    node->join.jointype = jointype;
    node->join.joinqual = joinclauses;

    return node;
}

/*
 * make_sort --- basic routine to build a Sort plan node
 *
 * Caller must have built the sortColIdx, sortOperators, collations, and
 * nullsFirst arrays already.
 * limit_tuples is as for cost_sort (in particular, pass -1 if no limit)
 */
Sort* make_sort(PlannerInfo* root, Plan* lefttree, int numCols, AttrNumber* sortColIdx, Oid* sortOperators,
    Oid* collations, bool* nullsFirst, double limit_tuples)
{
    Sort* node = makeNode(Sort);
    Plan* plan = &node->plan;
    Path sort_path; /* dummy for result of cost_sort */
    int width = get_plan_actual_total_width(lefttree, root->glob->vectorized, OP_SORT);

#ifdef STREAMPLAN
    inherit_plan_locator_info((Plan*)node, lefttree);
#endif

    copy_plan_costsize(plan, lefttree); /* only care about copying size */
    cost_sort(&sort_path,
        NIL,
        lefttree->total_cost,
        PLAN_LOCAL_ROWS(lefttree),
        width,
        0.0,
        u_sess->opt_cxt.op_work_mem,
        limit_tuples,
        root->glob->vectorized,
        SET_DOP(lefttree->dop),
        &node->mem_info);
    plan->startup_cost = sort_path.startup_cost;
    plan->total_cost = sort_path.total_cost;
    plan->targetlist = lefttree->targetlist;
    plan->qual = NIL;
    plan->lefttree = lefttree;
    plan->righttree = NULL;
    plan->hasUniqueResults = lefttree->hasUniqueResults;
    plan->dop = lefttree->dop;
    node->numCols = numCols;
    node->sortColIdx = sortColIdx;
    node->sortOperators = sortOperators;
    node->collations = collations;
    node->nullsFirst = nullsFirst;

    if (root->isPartIteratorPlanning) {
        node->plan.ispwj = true;
    }

    return node;
}

/*
 * prepare_sort_from_pathkeys
 *	  Prepare to sort according to given pathkeys
 *
 * This is used to set up for both Sort and MergeAppend nodes.	It calculates
 * the executor's representation of the sort key information, and adjusts the
 * plan targetlist if needed to add resjunk sort columns.
 *
 * Input parameters:
 *	  'lefttree' is the plan node which yields input tuples
 *	  'pathkeys' is the list of pathkeys by which the result is to be sorted
 *	  'relids' identifies the child relation being sorted, if any
 *	  'reqColIdx' is NULL or an array of required sort key column numbers
 *	  'adjust_tlist_in_place' is TRUE if lefttree must be modified in-place
 *
 * We must convert the pathkey information into arrays of sort key column
 * numbers, sort operator OIDs, collation OIDs, and nulls-first flags,
 * which is the representation the executor wants.	These are returned into
 * the output parameters *p_numsortkeys etc.
 *
 * When looking for matches to an EquivalenceClass's members, we will only
 * consider child EC members if they match 'relids'.  This protects against
 * possible incorrect matches to child expressions that contain no Vars.
 *
 * If reqColIdx isn't NULL then it contains sort key column numbers that
 * we should match.  This is used when making child plans for a MergeAppend;
 * it's an error if we can't match the columns.
 *
 * If the pathkeys include expressions that aren't simple Vars, we will
 * usually need to add resjunk items to the input plan's targetlist to
 * compute these expressions, since the Sort/MergeAppend node itself won't
 * do any such calculations.  If the input plan type isn't one that can do
 * projections, this means adding a Result node just to do the projection.
 * However, the caller can pass adjust_tlist_in_place = TRUE to force the
 * lefttree tlist to be modified in-place regardless of whether the node type
 * can project --- we use this for fixing the tlist of MergeAppend itself.
 *
 * Returns the node which is to be the input to the Sort (either lefttree,
 * or a Result stacked atop lefttree).
 */
static Plan* prepare_sort_from_pathkeys(PlannerInfo* root, Plan* lefttree, List* pathkeys, Relids relids,
    const AttrNumber* reqColIdx, bool adjust_tlist_in_place, int* p_numsortkeys, AttrNumber** p_sortColIdx,
    Oid** p_sortOperators, Oid** p_collations, bool** p_nullsFirst)
{
    List* tlist = lefttree->targetlist;
    ListCell* i = NULL;
    int numsortkeys = 0;
    AttrNumber* sortColIdx = NULL;
    Oid* sortOperators = NULL;
    Oid* collations = NULL;
    bool* nullsFirst = NULL;

    /*
     * We will need at most list_length(pathkeys) sort columns; possibly less
     */
    numsortkeys = list_length(pathkeys);
    sortColIdx = (AttrNumber*)palloc(numsortkeys * sizeof(AttrNumber));
    sortOperators = (Oid*)palloc(numsortkeys * sizeof(Oid));
    collations = (Oid*)palloc(numsortkeys * sizeof(Oid));
    nullsFirst = (bool*)palloc(numsortkeys * sizeof(bool));

    numsortkeys = 0;

    foreach (i, pathkeys) {
        PathKey* pathkey = (PathKey*)lfirst(i);
        EquivalenceClass* ec = pathkey->pk_eclass;
        EquivalenceMember* em = NULL;
        TargetEntry* tle = NULL;
        Oid pk_datatype = InvalidOid;
        Oid sortop;
        ListCell* j = NULL;

        if (ec->ec_has_volatile) {
            /*
             * If the pathkey's EquivalenceClass is volatile, then it must
             * have come from an ORDER BY clause, and we have to match it to
             * that same targetlist entry.
             */
            if (ec->ec_sortref == 0) /* can't happen */
                ereport(ERROR,
                    (errmodule(MOD_OPT),
                        errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                        (errmsg("volatile EquivalenceClass has no sortref"))));
            tle = get_sortgroupref_tle(ec->ec_sortref, tlist);
            Assert(tle);
            Assert(list_length(ec->ec_members) == 1);
            pk_datatype = ((EquivalenceMember*)linitial(ec->ec_members))->em_datatype;
        } else if (reqColIdx != NULL) {
            /*
             * If we are given a sort column number to match, only consider
             * the single TLE at that position.  It's possible that there is
             * no such TLE, in which case fall through and generate a resjunk
             * targetentry (we assume this must have happened in the parent
             * plan as well).  If there is a TLE but it doesn't match the
             * pathkey's EC, we do the same, which is probably the wrong thing
             * but we'll leave it to caller to complain about the mismatch.
             */
            tle = get_tle_by_resno(tlist, reqColIdx[numsortkeys]);
            if (tle != NULL) {
                em = find_ec_member_for_tle(ec, tle, relids);
                if (em != NULL) {
                    /* found expr at right place in tlist */
                    pk_datatype = em->em_datatype;
                } else
                    tle = NULL;
            }
        } else {
            /*
             * Otherwise, we can sort by any non-constant expression listed in
             * the pathkey's EquivalenceClass.  For now, we take the first
             * tlist item found in the EC. If there's no match, we'll generate
             * a resjunk entry using the first EC member that is an expression
             * in the input's vars.  (The non-const restriction only matters
             * if the EC is below_outer_join; but if it isn't, it won't
             * contain consts anyway, else we'd have discarded the pathkey as
             * redundant.)
             *
             * XXX if we have a choice, is there any way of figuring out which
             * might be cheapest to execute?  (For example, int4lt is likely
             * much cheaper to execute than numericlt, but both might appear
             * in the same equivalence class...)  Not clear that we ever will
             * have an interesting choice in practice, so it may not matter.
             */
            foreach (j, tlist) {
                tle = (TargetEntry*)lfirst(j);
                em = find_ec_member_for_tle(ec, tle, relids);
                if (em != NULL) {
                    /* found expr already in tlist */
                    pk_datatype = em->em_datatype;
                    break;
                }
                tle = NULL;
            }
        }

        if (tle == NULL) {
            /*
             * No matching tlist item; look for a computable expression. Note
             * that we treat Aggrefs as if they were variables; this is
             * necessary when attempting to sort the output from an Agg node
             * for use in a WindowFunc (since grouping_planner will have
             * treated the Aggrefs as variables, too).
             */
            Expr* sortexpr = NULL;
            bool partScan = false;

            foreach (j, ec->ec_members) {
                EquivalenceMember* tmp_em = (EquivalenceMember*)lfirst(j);
                List* exprvars = NIL;
                ListCell* k = NULL;

                /*
                 * We shouldn't be trying to sort by an equivalence class that
                 * contains a constant, so no need to consider such cases any
                 * further.
                 */
                /* Check for EC containing a constant --- unconditionally redundant */
                if (tmp_em->em_is_const) {
                    if (ENABLE_PRED_PUSH_ALL(root)) {
                        if (IsA(tmp_em->em_expr, Const)) {
                            continue;
                        }
                    } else {
                        continue;
                    }
                }

                /*
                 * Ignore child members unless they match the rel being
                 * sorted.
                 */
                if (tmp_em->em_is_child && !bms_equal(tmp_em->em_relids, relids))
                    continue;

                sortexpr = tmp_em->em_expr;
                exprvars = pull_var_clause((Node*)sortexpr, PVC_INCLUDE_AGGREGATES, PVC_INCLUDE_PLACEHOLDERS);
                foreach (k, exprvars) {
                    if (!tlist_member_ignore_relabel((Node*)lfirst(k), tlist))
                        break;
                }
                list_free_ext(exprvars);
                if (k == NULL) {
                    pk_datatype = tmp_em->em_datatype;
                    break; /* found usable expression */
                }
            }
            if (j == NULL)
                ereport(ERROR,
                    (errmodule(MOD_OPT),
                        errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                        (errmsg("could not find pathkey item to sort"))));

            /*
             * Do we need to insert a Result node?
             */
            if ((!adjust_tlist_in_place && !is_projection_capable_plan(lefttree)) ||
                (is_vector_scan(lefttree) && vector_engine_unsupport_expression_walker((Node*)sortexpr))) {
                /* copy needed so we don't modify input's tlist below */
                tlist = (List*)copyObject(tlist);
                lefttree = (Plan*)make_result(root, tlist, NULL, lefttree);
            } else if (IsA(lefttree, PartIterator)) {
                /*
                 * If is a PartIterator + Scan, push the PartIterator's
                 * tlist to Scan.
                 */
                partScan = true;
            }

            /* Don't bother testing is_projection_capable_plan again */
            adjust_tlist_in_place = true;

            /*
             * Add resjunk entry to input's tlist
             */
            tle = makeTargetEntry(sortexpr, list_length(tlist) + 1, NULL, true);
            tlist = lappend(tlist, tle);
            lefttree->targetlist = tlist; /* just in case NIL before */

            if (partScan)
                lefttree->lefttree->targetlist = tlist;
        }

        /*
         * Look up the correct sort operator from the PathKey's slightly
         * abstracted representation.
         */
        sortop = get_opfamily_member(pathkey->pk_opfamily, pk_datatype, pk_datatype, pathkey->pk_strategy);
        if (!OidIsValid(sortop)) /* should not happen */
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                    (errmsg("could not find member %d(%u,%u) of opfamily %u",
                        pathkey->pk_strategy,
                        pk_datatype,
                        pk_datatype,
                        pathkey->pk_opfamily))));

        /* Add the column to the sort arrays */
        sortColIdx[numsortkeys] = tle->resno;
        sortOperators[numsortkeys] = sortop;
        collations[numsortkeys] = ec->ec_collation;
        nullsFirst[numsortkeys] = pathkey->pk_nulls_first;
        numsortkeys++;
    }

    /* Return results */
    *p_numsortkeys = numsortkeys;
    *p_sortColIdx = sortColIdx;
    *p_sortOperators = sortOperators;
    *p_collations = collations;
    *p_nullsFirst = nullsFirst;

    return lefttree;
}

/*
 * find_ec_member_for_tle
 *		Locate an EquivalenceClass member matching the given TLE, if any
 *
 * Child EC members are ignored unless they match 'relids'.
 */
static EquivalenceMember* find_ec_member_for_tle(EquivalenceClass* ec, TargetEntry* tle, Relids relids)
{
    Expr* tlexpr = NULL;
    ListCell* lc = NULL;

    /* We ignore binary-compatible relabeling on both ends */
    tlexpr = tle->expr;
    while (tlexpr && IsA(tlexpr, RelabelType))
        tlexpr = ((RelabelType*)tlexpr)->arg;

    foreach (lc, ec->ec_members) {
        EquivalenceMember* em = (EquivalenceMember*)lfirst(lc);
        Expr* emexpr = NULL;

        /*
         * We shouldn't be trying to sort by an equivalence class that
         * contains a constant, so no need to consider such cases any further.
         * If this const in olap function group clauses, it can not be ignored.
         */
        if (em->em_is_const && !ec->ec_group_set)
            continue;

        /*
         * Ignore child members unless they match the rel being sorted.
         */
        if (em->em_is_child && !bms_equal(em->em_relids, relids))
            continue;

        /* Match if same expression (after stripping relabel) */
        emexpr = em->em_expr;
        while (emexpr && IsA(emexpr, RelabelType))
            emexpr = ((RelabelType*)emexpr)->arg;

        if (equal(emexpr, tlexpr))
            return em;
    }

    return NULL;
}

/*
 * make_sort_from_pathkeys
 *	  Create sort plan to sort according to given pathkeys
 *
 *	  'lefttree' is the node which yields input tuples
 *	  'pathkeys' is the list of pathkeys by which the result is to be sorted
 *	  'limit_tuples' is the bound on the number of output tuples;
 *				-1 if no bound
 */
Sort* make_sort_from_pathkeys(PlannerInfo* root, Plan* lefttree, List* pathkeys, double limit_tuples, bool can_parallel)
{
    int numsortkeys = -1;
    AttrNumber* sortColIdx = NULL;
    Oid* sortOperators = NULL;
    Oid* collations = NULL;
    bool* nullsFirst = NULL;

    /* Compute sort column info, and adjust lefttree as needed */
    lefttree = prepare_sort_from_pathkeys(root,
        lefttree,
        pathkeys,
        NULL,
        NULL,
        false,
        &numsortkeys,
        &sortColIdx,
        &sortOperators,
        &collations,
        &nullsFirst);

    /*
     * If we want parallelize sort, we need to sort partial data and then merge
     * them in one thread. This method is not effient in most situations rather
     * than sort + limit.
     */
    if (lefttree->dop > 1 && !can_parallel) {
        double threshold = lefttree->plan_rows / ng_get_dest_num_data_nodes(lefttree) / lefttree->dop;
        if (limit_tuples <= 0 || limit_tuples > threshold)
            lefttree = create_local_gather(lefttree);
    }

    /* Now build the Sort node */
    return make_sort(root, lefttree, numsortkeys, sortColIdx, sortOperators, collations, nullsFirst, limit_tuples);
}

/*
 * make_sort_from_sortclauses
 *	  Create sort plan to sort according to given sortclauses
 *
 *	  'sortcls' is a list of SortGroupClauses
 *	  'lefttree' is the node which yields input tuples
 */
Sort* make_sort_from_sortclauses(PlannerInfo* root, List* sortcls, Plan* lefttree)
{
    List* sub_tlist = lefttree->targetlist;
    ListCell* l = NULL;
    int numsortkeys;
    AttrNumber* sortColIdx = NULL;
    Oid* sortOperators = NULL;
    Oid* collations = NULL;
    bool* nullsFirst = NULL;

    /* Convert list-ish representation to arrays wanted by executor */
    numsortkeys = list_length(sortcls);
    sortColIdx = (AttrNumber*)palloc(numsortkeys * sizeof(AttrNumber));
    sortOperators = (Oid*)palloc(numsortkeys * sizeof(Oid));
    collations = (Oid*)palloc(numsortkeys * sizeof(Oid));
    nullsFirst = (bool*)palloc(numsortkeys * sizeof(bool));

    numsortkeys = 0;
    foreach (l, sortcls) {
        SortGroupClause* sortcl = (SortGroupClause*)lfirst(l);
        TargetEntry* tle = get_sortgroupclause_tle(sortcl, sub_tlist);

        sortColIdx[numsortkeys] = tle->resno;
        sortOperators[numsortkeys] = sortcl->sortop;
        collations[numsortkeys] = exprCollation((Node*)tle->expr);
        nullsFirst[numsortkeys] = sortcl->nulls_first;
        numsortkeys++;
    }

    return make_sort(root, lefttree, numsortkeys, sortColIdx, sortOperators, collations, nullsFirst, -1.0);
}

/*
 * make_sort_from_groupcols
 *	  Create sort plan to sort based on grouping columns
 *
 * 'groupcls' is the list of SortGroupClauses
 * 'grpColIdx' gives the column numbers to use
 *
 * This might look like it could be merged with make_sort_from_sortclauses,
 * but presently we *must* use the grpColIdx[] array to locate sort columns,
 * because the child plan's tlist is not marked with ressortgroupref info
 * appropriate to the grouping node.  So, only the sort ordering info
 * is used from the SortGroupClause entries.
 */
Sort* make_sort_from_groupcols(PlannerInfo* root, List* groupcls, AttrNumber* grpColIdx, Plan* lefttree)
{
    List* sub_tlist = lefttree->targetlist;
    ListCell* l = NULL;
    int numsortkeys;
    AttrNumber* sortColIdx = NULL;
    Oid* sortOperators = NULL;
    Oid* collations = NULL;
    bool* nullsFirst = NULL;

    /* Convert list-ish representation to arrays wanted by executor */
    numsortkeys = list_length(groupcls);
    sortColIdx = (AttrNumber*)palloc(numsortkeys * sizeof(AttrNumber));
    sortOperators = (Oid*)palloc(numsortkeys * sizeof(Oid));
    collations = (Oid*)palloc(numsortkeys * sizeof(Oid));
    nullsFirst = (bool*)palloc(numsortkeys * sizeof(bool));

    numsortkeys = 0;
    foreach (l, groupcls) {
        SortGroupClause* grpcl = (SortGroupClause*)lfirst(l);
        TargetEntry* tle = get_tle_by_resno(sub_tlist, grpColIdx[numsortkeys]);

        if (tle == NULL) {
            /* just break if we cannot find TargetEntry for SortGroupClause */
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                        errmsg("fail to find TargetEntry referenced by SortGroupClause"))));
        }

        sortColIdx[numsortkeys] = tle->resno;
        sortOperators[numsortkeys] = grpcl->sortop;
        collations[numsortkeys] = exprCollation((Node*)tle->expr);
        nullsFirst[numsortkeys] = grpcl->nulls_first;
        numsortkeys++;
    }

    return make_sort(root, lefttree, numsortkeys, sortColIdx, sortOperators, collations, nullsFirst, -1.0);
}

/*
 * make_sort_from_targetlist
 *	  Create sort plan to sort based on input plan's targetlist
 *
 *
 * This routine is used to add sort node without other sort information
 * such as pathkey, it will build sort info based on input tree's targetlist.
 */
Sort* make_sort_from_targetlist(PlannerInfo* root, Plan* lefttree, double limit_tuples)
{
    /* construct sort related info */
    int numsortkeys = list_length(lefttree->targetlist);
    AttrNumber* sortColIdx = (AttrNumber*)palloc(numsortkeys * sizeof(AttrNumber));
    Oid* sortOperators = (Oid*)palloc(numsortkeys * sizeof(Oid));
    Oid* collations = (Oid*)palloc(numsortkeys * sizeof(Oid));
    bool* nullsFirst = (bool*)palloc(numsortkeys * sizeof(bool));
    numsortkeys = 0;
    Oid sortop = InvalidOid;
    bool hashable = false;
    ListCell* l = NULL;

    foreach (l, lefttree->targetlist) {
        TargetEntry* tle = (TargetEntry*)lfirst(l);

        get_sort_group_operators(exprType((Node*)tle->expr), false, false, false, &sortop, NULL, NULL, &hashable);

        if (!OidIsValid(sortop)) {
            continue;
        }

        sortColIdx[numsortkeys] = tle->resno;
        sortOperators[numsortkeys] = sortop;
        collations[numsortkeys] = exprCollation((Node*)tle->expr);
        nullsFirst[numsortkeys] = false;

        numsortkeys++;
    }

    /* If numsortkeys == 0, don't add sort, return lefttree directly. */
    if (numsortkeys > 0) {
        return make_sort(root, lefttree, numsortkeys, sortColIdx,
                         sortOperators, collations, nullsFirst, limit_tuples);
    } else {
        return NULL;
    }
}

Material* make_material(Plan* lefttree, bool materialize_all)
{
    Material* node = makeNode(Material);
    Plan* plan = &node->plan;

    /* cost should be inserted by caller */
    plan->targetlist = lefttree->targetlist;
    plan->qual = NIL;
    plan->lefttree = lefttree;
    plan->righttree = NULL;
    node->materialize_all = materialize_all;
    plan->dop = lefttree->dop;
    plan->hasUniqueResults = lefttree->hasUniqueResults;

#ifdef STREAMPLAN
    inherit_plan_locator_info(plan, lefttree);
#endif

    return node;
}

/*
 * materialize_finished_plan: stick a Material node atop a completed plan
 *
 * There are a couple of places where we want to attach a Material node
 * after completion of subquery_planner().	This currently requires hackery.
 * Since subquery_planner has already run SS_finalize_plan on the subplan
 * tree, we have to kluge up parameter lists for the Material node.
 * Possibly this could be fixed by postponing SS_finalize_plan processing
 * until setrefs.c is run?
 */
Plan* materialize_finished_plan(Plan* subplan, bool materialize_above_stream, bool vectorized)
{
    Plan* matplan = NULL;
    Path matpath; /* dummy for result of cost_material */

    matplan = (Plan*)make_material(subplan, materialize_above_stream);
    matplan->dop = subplan->dop;
    matpath.dop = subplan->dop;

    /* Set cost data */
    cost_material(&matpath, subplan->startup_cost, subplan->total_cost, PLAN_LOCAL_ROWS(subplan), subplan->plan_width);
    matplan->startup_cost = matpath.startup_cost;
    matplan->total_cost = matpath.total_cost;
    set_plan_rows(matplan, subplan->plan_rows, subplan->multiple);
    (void)cost_rescan_material(PLAN_LOCAL_ROWS(matplan),
        get_plan_actual_total_width(matplan, vectorized, OP_SORT),
        &((Material*)matplan)->mem_info,
        vectorized,
        SET_DOP(matplan->dop));
    matplan->plan_width = subplan->plan_width;

    /* parameter kluge --- see comments above */
    matplan->extParam = bms_copy(subplan->extParam);
    matplan->allParam = bms_copy(subplan->allParam);

    return matplan;
}

/*
 * @Description: Adjust all pathkeys and windows function according to tlist.
 * @in root: Per-query information for planning/optimization.
 * @in tlist: Targetlist.
 * @out wflists: Windows info.
 */
void adjust_all_pathkeys_by_agg_tlist(PlannerInfo* root, List* tlist, WindowLists* wflists)
{
    Query* parse = root->parse;

    if (root->group_pathkeys)
        root->group_pathkeys = make_pathkeys_for_sortclauses(root, parse->groupClause, tlist, true);

    if (root->distinct_pathkeys)
        root->distinct_pathkeys = make_pathkeys_for_sortclauses(root, parse->distinctClause, tlist, true);
    if (root->sort_pathkeys)
        root->sort_pathkeys = make_pathkeys_for_sortclauses(root, parse->sortClause, tlist, true);

    /* Build windows func according to tlist again. */
    if (parse->hasWindowFuncs) {
        free_windowFunc_lists(wflists);
        find_window_functions((Node*)tlist, wflists);
    }

    if (root->window_pathkeys && wflists->activeWindows != NIL) {
        WindowClause* wc = (WindowClause*)linitial(wflists->activeWindows);

        root->window_pathkeys = make_pathkeys_for_window(root, wc, tlist, true);
    }

    if (root->group_pathkeys)
        root->query_pathkeys = root->group_pathkeys;
    else if (root->window_pathkeys)
        root->query_pathkeys = root->window_pathkeys;
    else if (list_length(root->distinct_pathkeys) > list_length(root->sort_pathkeys))
        root->query_pathkeys = root->distinct_pathkeys;
    else if (root->sort_pathkeys)
        root->query_pathkeys = root->sort_pathkeys;
    else
        root->query_pathkeys = NIL;
}

Agg* make_agg(PlannerInfo* root, List* tlist, List* qual, AggStrategy aggstrategy, const AggClauseCosts* aggcosts,
    int numGroupCols, AttrNumber* grpColIdx, Oid* grpOperators, long numGroups, Plan* lefttree, WindowLists* wflists,
    bool need_stream, bool trans_agg, List* groupingSets, Size hash_entry_size, bool add_width,
    AggOrientation agg_orientation, bool unique_check)
{
    Agg* node = makeNode(Agg);
    Plan* plan = &node->plan;
    Path agg_path; /* dummy for result of cost_agg */
    QualCost qual_cost;

    List* temp_tlist = (List*)copyObject(tlist);
    bool is_agg_tlist_streamed = false;
    List* local_tlist = NIL;
    bool reduce_plan = false;
    List* local_qual = NIL;
    double plan_rows;

    errno_t rc = EOK;
    rc = memset_s(&agg_path, sizeof(Path), 0, sizeof(Path));
    securec_check(rc, "\0", "\0");

    /* SS_finalize_plan() will fill this */
    node->aggParams = NULL;

    if (IS_STREAM_PLAN && need_stream && agg_orientation != DISTINCT_INTENT) {
        local_tlist = process_agg_targetlist(root, &tlist);
        if (local_tlist != NIL)
            is_agg_tlist_streamed = true;

        if (local_tlist != NIL && qual != NIL) {
            local_tlist = process_agg_having_clause(root, local_tlist, (Node*)qual, &local_qual, &reduce_plan);
            if (reduce_plan) {
                qual = local_qual;
            } else {
                is_agg_tlist_streamed = false;
            }
        }
    }

    if (false == is_agg_tlist_streamed) {
        if (local_tlist != NIL)
            list_free_deep(local_tlist);
        local_tlist = temp_tlist;
        tlist = temp_tlist;
    } else {
        if (temp_tlist != NIL)
            list_free_deep(temp_tlist);
    }
    if ((is_agg_tlist_streamed && agg_orientation == AGG_LEVEL_1_INTENT) || agg_orientation == AGG_LEVEL_2_2_INTENT)
        adjust_all_pathkeys_by_agg_tlist(root, tlist, wflists);

    /*
     * If we are able to completely evaluate the aggregates, we
     * need to ask datanode/s to finalise the aggregates.
     */
    if ((IS_STREAM_PLAN && trans_agg && !need_stream) ||
        (IS_SINGLE_NODE && !IS_STREAM_PLAN)) {
        node->single_node = true;
    }

    node->aggstrategy = aggstrategy;
    node->numCols = numGroupCols;
    node->grpColIdx = grpColIdx;

    node->grpOperators = grpOperators;
    Assert(numGroups > 0);
    node->numGroups = numGroups;
    node->skew_optimize = SKEW_RES_NONE;
    node->unique_check = unique_check && root->parse->unique_check;

#ifdef STREAMPLAN
    inherit_plan_locator_info((Plan*)node, lefttree);
#endif

    copy_plan_costsize(plan, lefttree); /* only care about copying size */

    /* For agg and above node, we should add agg function width to total width */
    if (aggcosts != NULL && add_width)
        plan->plan_width += aggcosts->aggWidth;

    agg_path.pathtype = T_Group; /* use pathtype in ng_get_dest_num_data_nodes */
    ng_copy_distribution(&agg_path.distribution, ng_get_dest_distribution((Plan*)node));
    cost_agg(&agg_path,
        root,
        aggstrategy,
        aggcosts,
        numGroupCols,
        /* use hashagg_table_size as estimation if set */
        numGroups,
        lefttree->startup_cost,
        lefttree->total_cost,
        PLAN_LOCAL_ROWS(lefttree),
        lefttree->plan_width,
        hash_entry_size,
        lefttree->dop,
        &node->mem_info);

    /* agg estimate cost has already include subplan's cost */
    plan->startup_cost = agg_path.startup_cost;
    plan->total_cost = agg_path.total_cost;

    /*
     * We will produce a single output tuple if not grouping, and a tuple per
     * group otherwise.
     */
    if (aggstrategy == AGG_PLAIN) {
        plan_rows = groupingSets ? list_length(groupingSets) : 1;
    } else {
        plan_rows = numGroups;
    }

    /* Calculate global rows, and we should consider the skew of lefttree for non-plain agg. */
    plan_rows = get_global_rows(plan_rows, 1.0, ng_get_dest_num_data_nodes(lefttree));
    plan_rows = groupingSets != NIL ? plan_rows : Min(plan_rows, lefttree->plan_rows);

    /* Consider the selectivity of having qual */
    if (qual != NIL && plan_rows >= HAVING_THRESHOLD && !need_stream)
        plan_rows = clamp_row_est(plan_rows * DEFAULT_MATCH_SEL);

    if (plan->exec_nodes->baselocatortype == LOCATOR_TYPE_REPLICATED && is_execute_on_datanodes(plan)) {
        plan->multiple = 1.0;
        plan->plan_rows = plan_rows;
    } else {
        set_plan_rows(plan, plan_rows, 1.0);
    }

    node->groupingSets = groupingSets;

    /*
     * We also need to account for the cost of evaluation of the qual (ie, the
     * HAVING clause) and the tlist.  Note that cost_qual_eval doesn't charge
     * anything for Aggref nodes; this is okay since they are really
     * comparable to Vars.
     *
     * See notes in add_tlist_costs_to_plan about why only make_agg,
     * make_windowagg and make_group worry about tlist eval cost.
     */
    if (qual != NIL) {
        cost_qual_eval(&qual_cost, qual, root);
        plan->startup_cost += qual_cost.startup;
        plan->total_cost += qual_cost.startup;
        plan->total_cost += qual_cost.per_tuple * PLAN_LOCAL_ROWS(plan);
    }
    add_tlist_costs_to_plan(root, plan, local_tlist);

    plan->qual = qual;
    plan->targetlist = local_tlist;
    plan->lefttree = lefttree;
    plan->righttree = NULL;

    /* check if sonic hashagg is enabled or not */
    node->is_sonichash = isSonicHashAggEnable(node);

    return node;
}

WindowAgg* make_windowagg(PlannerInfo* root, List* tlist, List* windowFuncs, Index winref, int partNumCols,
    AttrNumber* partColIdx, Oid* partOperators, int ordNumCols, AttrNumber* ordColIdx, Oid* ordOperators,
    int frameOptions, Node* startOffset, Node* endOffset, Plan* lefttree)
{
    WindowAgg* node = makeNode(WindowAgg);
    Plan* plan = &node->plan;
    Path windowagg_path; /* dummy for result of cost_windowagg */

    node->winref = winref;
    node->partNumCols = partNumCols;
    node->partColIdx = partColIdx;
    node->partOperators = partOperators;
    node->ordNumCols = ordNumCols;
    node->ordColIdx = ordColIdx;
    node->ordOperators = ordOperators;
    node->frameOptions = frameOptions;
    node->startOffset = startOffset;
    node->endOffset = endOffset;

#ifdef STREAMPLAN
    inherit_plan_locator_info((Plan*)node, lefttree);
#endif

    copy_plan_costsize(plan, lefttree); /* only care about copying size */
    cost_windowagg(&windowagg_path,
        root,
        windowFuncs,
        partNumCols,
        ordNumCols,
        lefttree->startup_cost,
        lefttree->total_cost,
        PLAN_LOCAL_ROWS(lefttree));
    plan->startup_cost = windowagg_path.startup_cost;
    plan->total_cost = windowagg_path.total_cost;

    /*
     * We also need to account for the cost of evaluation of the tlist.
     *
     * See notes in add_tlist_costs_to_plan about why only make_agg,
     * make_windowagg and make_group worry about tlist eval cost.
     */
    add_tlist_costs_to_plan(root, plan, tlist);

    plan->targetlist = tlist;
    plan->lefttree = lefttree;
    plan->righttree = NULL;
    /* WindowAgg nodes never have a qual clause */
    plan->qual = NIL;
    plan->dop = SET_DOP(lefttree->dop);

    return node;
}

Group* make_group(PlannerInfo* root, List* tlist, List* qual, int numGroupCols, AttrNumber* grpColIdx,
    Oid* grpOperators, double numGroups, Plan* lefttree)
{
    Group* node = makeNode(Group);
    Plan* plan = &node->plan;
    Path group_path; /* dummy for result of cost_group */
    QualCost qual_cost;

    errno_t rc = EOK;
    rc = memset_s(&group_path, sizeof(Path), 0, sizeof(Path));
    securec_check(rc, "\0", "\0");

    node->numCols = numGroupCols;
    node->grpColIdx = grpColIdx;
    node->grpOperators = grpOperators;

#ifdef STREAMPLAN
    inherit_plan_locator_info((Plan*)node, lefttree);
#endif

    copy_plan_costsize(plan, lefttree); /* only care about copying size */
    group_path.pathtype = T_Group;      /* use pathtype in ng_get_dest_num_data_nodes */
    ng_copy_distribution(&group_path.distribution, ng_get_dest_distribution((Plan*)node));
    cost_group(&group_path,
        root,
        numGroupCols,
        numGroups,
        lefttree->startup_cost,
        lefttree->total_cost,
        PLAN_LOCAL_ROWS(lefttree));
    plan->startup_cost = group_path.startup_cost;
    plan->total_cost = group_path.total_cost;

    /* One output tuple per estimated result group */
    set_plan_rows(plan, get_global_rows(numGroups, 1.0, ng_get_dest_num_data_nodes(lefttree)), 1.0);

    /*
     * We also need to account for the cost of evaluation of the qual (ie, the
     * HAVING clause) and the tlist.
     *
     * XXX this double-counts the cost of evaluation of any expressions used
     * for grouping, since in reality those will have been evaluated at a
     * lower plan level and will only be copied by the Group node. Worth
     * fixing?
     *
     * See notes in add_tlist_costs_to_plan about why only make_agg,
     * make_windowagg and make_group worry about tlist eval cost.
     */
    if (qual != NIL) {
        cost_qual_eval(&qual_cost, qual, root);
        plan->startup_cost += qual_cost.startup;
        plan->total_cost += qual_cost.startup;
        plan->total_cost += qual_cost.per_tuple * PLAN_LOCAL_ROWS(plan);
    }
    add_tlist_costs_to_plan(root, plan, tlist);

    plan->qual = qual;
    plan->targetlist = tlist;
    plan->lefttree = lefttree;
    plan->righttree = NULL;
    plan->dop = lefttree->dop;

    return node;
}

/*
 * distinctList is a list of SortGroupClauses, identifying the targetlist items
 * that should be considered by the Unique filter.	The input path must
 * already be sorted accordingly.
 */
Unique* make_unique(Plan* lefttree, List* distinctList)
{
    Unique* node = makeNode(Unique);
    Plan* plan = &node->plan;
    int numCols = list_length(distinctList);
    int keyno = 0;
    AttrNumber* uniqColIdx = NULL;
    Oid* uniqOperators = NULL;
    ListCell* slitem = NULL;

#ifdef STREAMPLAN
    inherit_plan_locator_info(plan, lefttree);
#endif

    copy_plan_costsize(plan, lefttree);

    /*
     * Charge one cpu_operator_cost per comparison per input tuple. We assume
     * all columns get compared at most of the tuples.	(XXX probably this is
     * an overestimate.)
     */
    plan->total_cost += u_sess->attr.attr_sql.cpu_operator_cost * PLAN_LOCAL_ROWS(plan) * numCols;

    /*
     * plan->plan_rows is left as a copy of the input subplan's plan_rows; ie,
     * we assume the filter removes nothing.  The caller must alter this if he
     * has a better idea.
     */
    plan->targetlist = lefttree->targetlist;
    plan->qual = NIL;
    plan->lefttree = lefttree;
    plan->righttree = NULL;
    plan->dop = lefttree->dop;

    /*
     * convert SortGroupClause list into arrays of attr indexes and equality
     * operators, as wanted by executor
     */
    Assert(numCols > 0);
    uniqColIdx = (AttrNumber*)palloc(sizeof(AttrNumber) * numCols);
    uniqOperators = (Oid*)palloc(sizeof(Oid) * numCols);

    foreach (slitem, distinctList) {
        SortGroupClause* sortcl = (SortGroupClause*)lfirst(slitem);
        TargetEntry* tle = get_sortgroupclause_tle(sortcl, plan->targetlist);

        uniqColIdx[keyno] = tle->resno;
        uniqOperators[keyno] = sortcl->eqop;
        Assert(OidIsValid(uniqOperators[keyno]));
        keyno++;
    }

    node->numCols = numCols;
    node->uniqColIdx = uniqColIdx;
    node->uniqOperators = uniqOperators;

    return node;
}

/*
 * distinctList is a list of SortGroupClauses, identifying the targetlist
 * items that should be considered by the SetOp filter.  The input path must
 * already be sorted accordingly.
 */
SetOp* make_setop(SetOpCmd cmd, SetOpStrategy strategy, Plan* lefttree, List* distinctList, AttrNumber flagColIdx,
    int firstFlag, long numGroups, double outputRows, OpMemInfo* memInfo)
{
    SetOp* node = makeNode(SetOp);
    Plan* plan = &node->plan;
    int numCols = list_length(distinctList);
    int keyno = 0;
    AttrNumber* dupColIdx = NULL;
    Oid* dupOperators = NULL;
    ListCell* slitem = NULL;

#ifdef STREAMPLAN
    inherit_plan_locator_info(plan, lefttree);
#endif

    copy_plan_costsize(plan, lefttree);
    copy_mem_info(&node->mem_info, memInfo);
    set_plan_rows(
        plan, get_global_rows(outputRows, lefttree->multiple, ng_get_dest_num_data_nodes(plan)), lefttree->multiple);

    /*
     * Charge one cpu_operator_cost per comparison per input tuple. We assume
     * all columns get compared at most of the tuples.
     */
    plan->total_cost += u_sess->attr.attr_sql.cpu_operator_cost * PLAN_LOCAL_ROWS(lefttree) * numCols;

    plan->targetlist = lefttree->targetlist;
    plan->qual = NIL;
    plan->lefttree = lefttree;
    plan->righttree = NULL;

    /*
     * convert SortGroupClause list into arrays of attr indexes and equality
     * operators, as wanted by executor
     */
    Assert(numCols > 0);
    dupColIdx = (AttrNumber*)palloc(sizeof(AttrNumber) * numCols);
    dupOperators = (Oid*)palloc(sizeof(Oid) * numCols);

    foreach (slitem, distinctList) {
        SortGroupClause* sortcl = (SortGroupClause*)lfirst(slitem);
        TargetEntry* tle = get_sortgroupclause_tle(sortcl, plan->targetlist);

        dupColIdx[keyno] = tle->resno;
        dupOperators[keyno] = sortcl->eqop;
        Assert(OidIsValid(dupOperators[keyno]));
        keyno++;
    }

    node->cmd = cmd;
    node->strategy = strategy;
    node->numCols = numCols;
    node->dupColIdx = dupColIdx;
    node->dupOperators = dupOperators;
    node->flagColIdx = flagColIdx;
    node->firstFlag = firstFlag;
    node->numGroups = numGroups;

    return node;
}

/*
 * make_lockrows
 *	  Build a LockRows plan node
 */
LockRows* make_lockrows(PlannerInfo* root, Plan* lefttree)
{
    LockRows* node = makeNode(LockRows);
    Plan* plan = &node->plan;
    int epqParam = SS_assign_special_param(root);

    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        if (IS_STREAM_PLAN) {
            Form_pgxc_class classForm = NULL;
            HeapTuple tuple = NULL;

            PlanRowMark* rowMark = NULL;
            Index result_rte_idx = 0;
            RangeTblEntry* result_rte = NULL;

            /* Check whether allow this lockrows query in stream plan. */
            rowMark = check_lockrows_permission(root, lefttree);

            result_rte_idx = rowMark->rti;
            result_rte = planner_rt_fetch(result_rte_idx, root);

            /* Don't support lock system table. */
            if (is_sys_table(result_rte->relid))
                ereport(ERROR,
                    (errmodule(MOD_OPT),
                        errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("Unsupported FOR UPDATE/SHARE system table.")));

            tuple = SearchSysCache1(PGXCCLASSRELID, ObjectIdGetDatum(result_rte->relid));
            if (!HeapTupleIsValid(tuple))
                ereport(ERROR,
                    (errmodule(MOD_OPT),
                        errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                        errmsg("cache lookup failed for relation %u", result_rte->relid)));
            classForm = (Form_pgxc_class)GETSTRUCT(tuple);

            /* Add redistribute node for the lockrows operator when needed. */
            if (judge_lockrows_need_redistribute(root, lefttree, classForm, result_rte_idx)) {
                Stream* streamnode = makeNode(Stream);
                Plan* streamplan = &(streamnode->scan.plan);

                /* 1. Add redistribute node above lefttree. */
                streamnode->type = STREAM_REDISTRIBUTE;
                streamnode->is_sorted = false;
                streamnode->smpDesc.consumerDop = 1;
                streamnode->smpDesc.producerDop = lefttree->dop;
                streamnode->smpDesc.distriType = REMOTE_DISTRIBUTE;

                /* Redistribute's consumer_nodes must be the base table's datanodes. */
                RelationLocInfo* rel_loc_info = GetRelationLocInfo(result_rte->relid);
                ExecNodes* target_exec_nodes =
                    GetRelationNodes(rel_loc_info, NULL, NULL, NULL, NULL, RELATION_ACCESS_READ, false);
                streamnode->consumer_nodes = target_exec_nodes;

                /* Get the distributekey for redistribute. */
                streamnode->distribute_keys = build_baserel_distributekey(result_rte, result_rte_idx);

                streamplan->dop = 1;
                streamplan->targetlist = lefttree->targetlist;
                streamplan->lefttree = lefttree;

                /* Get the exec_nodes for the stream plan. */
                if (is_replicated_plan(lefttree)) {
                    /* Push the single data node when lefttree is replicated. */
                    ExecNodes* exec_nodes = get_random_data_nodes(LOCATOR_TYPE_REPLICATED, lefttree);
                    pushdown_execnodes(lefttree, exec_nodes);

                    streamplan->exec_nodes = exec_nodes;
                } else {
                    streamplan->exec_nodes = ng_get_dest_execnodes(lefttree);
                }

                /* if target is list/range, stream needs to be list/range redistribute */
                Assert(rel_loc_info != NULL);
                if (IsLocatorDistributedBySlice(rel_loc_info->locatorType)) {
                    streamnode->consumer_nodes->baselocatortype = rel_loc_info->locatorType;
                    streamnode->consumer_nodes->rangelistOid = result_rte->relid;
                    ConstructSliceBoundary(streamnode->consumer_nodes);
                }

                /* 2. Add lockrows node above redtribute node. */
                plan->plan_width = streamplan->plan_width = lefttree->plan_width;
                plan->plan_rows = streamplan->plan_rows = lefttree->plan_rows;
                plan->startup_cost = streamplan->startup_cost = lefttree->startup_cost;
                plan->total_cost = streamplan->total_cost = lefttree->total_cost;
                plan->targetlist = lefttree->targetlist;
                plan->qual = NIL;
                plan->lefttree = (Plan*)streamnode;
                plan->righttree = NULL;
                node->rowMarks = root->rowMarks;
                node->epqParam = epqParam;

                inherit_plan_locator_info(plan, (Plan*)streamnode);

                ReleaseSysCache(tuple);
                return node;
            }
            ReleaseSysCache(tuple);
        } else {
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("Unsupported FOR UPDATE/SHARE in non shippable plan."))));
        }
    }

#ifdef STREAMPLAN
    inherit_plan_locator_info(plan, lefttree);
#endif

    copy_plan_costsize(plan, lefttree);

    /* charge cpu_tuple_cost to reflect locking costs (underestimate?) */
    plan->total_cost += u_sess->attr.attr_sql.cpu_tuple_cost * PLAN_LOCAL_ROWS(plan);

    plan->targetlist = lefttree->targetlist;
    plan->qual = NIL;
    plan->lefttree = lefttree;
    plan->righttree = NULL;

    node->rowMarks = root->rowMarks;
    node->epqParam = epqParam;

    return node;
}

/*
 * Note: offset_est and count_est are passed in to save having to repeat
 * work already done to estimate the values of the limitOffset and limitCount
 * expressions.  Their values are as returned by preprocess_limit (0 means
 * "not relevant", -1 means "couldn't estimate").  Keep the code below in sync
 * with that function!
 */
Limit* make_limit(PlannerInfo* root, Plan* lefttree, Node* limitOffset, Node* limitCount, int64 offset_est,
    int64 count_est, bool enable_parallel)
{
    Limit* node = makeNode(Limit);
    Plan* plan = &node->plan;

    /*
     * If we want to parallel sort + limit, we need to create a plan like:
     * sort -> limit -> local gather -> sort -> limit.
     */
    if (enable_parallel && lefttree->dop > 1)
        lefttree = parallel_limit_sort(root, lefttree, limitOffset, limitCount, offset_est, count_est);

    /*
     * In case of Limit + star with CteScan, we need add Result node to elimit additional internal entry
     * not match in some case, e.g. limit it top-plan node in current subquery
     */
    if (IsA(lefttree, CteScan) && IsCteScanProcessForStartWith((CteScan *)lefttree)) {
        List *tlist = lefttree->targetlist;
        lefttree = (Plan *)make_result(root, tlist, NULL, lefttree, NULL);
    }

#ifdef STREAMPLAN
    inherit_plan_locator_info((Plan*)node, lefttree);
#endif

    copy_plan_costsize(plan, lefttree);
    cost_limit(plan, lefttree, offset_est, count_est);

    plan->targetlist = lefttree->targetlist;
    plan->qual = NIL;
    plan->lefttree = lefttree;
    plan->righttree = NULL;

    node->limitOffset = limitOffset;
    node->limitCount = limitCount;

    return node;
}

/*
 * @Description: create parallel plan for sort + limit.
 *
 * @param[IN] root: planner info
 * @param[IN] lefttree: subplan
 * @param[IN] limitOffset: offset param
 * @param[IN] limitCount: count param
 * @param[IN] offset_est: offset num
 * @param[IN] count_est: count num
 * @return Plan*.
 */
static Plan* parallel_limit_sort(
    PlannerInfo* root, Plan* lefttree, Node* limitOffset, Node* limitCount, int64 offset_est, int64 count_est)
{
    Plan* plan = NULL;

    if (lefttree->dop <= 1)
        return lefttree;

    /*
     * When sort + limit is parallelized, we need to add another sort
     * to make sure the data we send to CN is sorted.
     */
    if (root->sort_pathkeys && (IsA(lefttree, Sort) || IsA(lefttree, VecSort))) {
        plan = (Plan*)make_limit(root, lefttree, limitOffset, limitCount, offset_est, count_est, false);
        plan = create_local_gather(plan);
        plan = (Plan*)make_sort_from_pathkeys(root, plan, root->sort_pathkeys, -1.0);
    } else {
        plan = create_local_gather(lefttree);
    }

    return plan;
}

static Node* create_offset_count(Node* offsetCount, Datum value)
{
    Node* node = NULL;

    if (offsetCount != NULL) {
        if (IsA(offsetCount, Const)) {
            Const* temp = (Const*)copyObject(offsetCount);
            temp->constvalue = value;
            node = (Node*)temp;
        } else {
            int16 typLen;
            bool typByVal = false;
            Oid consttype = INT8OID;
            get_typlenbyval(consttype, &typLen, &typByVal);

            node = (Node*)makeConst(consttype, -1, InvalidOid, typLen, value, false, typByVal);
        }
    }

    return node;
}

/*
 * @Description: change replicated plan to not replicated and execute on one node.
 * @input plan:  input plan to be changed.
 */
void pick_single_node_plan_for_replication(Plan* plan)
{
    unsigned int num_datanodes = ng_get_dest_num_data_nodes(plan);

    /* Tips: LOCATOR_TYPE_HASH is used to mark this plan not replicated. */
    ExecNodes* exec_nodes = get_random_data_nodes(LOCATOR_TYPE_HASH, plan);
    pushdown_execnodes(plan, exec_nodes);
    plan->distributed_keys = NIL;
    plan->multiple = num_datanodes;
    /*
     * Fix stream->consumer_node's baselocatetype as not replicated so that this
     * information could be inherited by other OP built on it.
     */
    if (IsA(plan, Stream)) {
        ((Stream*)plan)->consumer_nodes->baselocatortype = LOCATOR_TYPE_HASH;
    }
}

/*
 * Check if the result of the plan can be random if not well sorted
 * the result can be random if
 * (1) have no sort key
 * (2) have sort key but not including all the targetlist
 *     for example the following query is sorted by a
 *        such as select a,b from xxx where xxx order by a;
 *     but the result (a,b) can be random. because the sort algorithm is not stable.
 */
static bool is_result_random(PlannerInfo* root, Plan* plan)
{
    ListCell* lc = NULL;

    /* no sort clause, the result can be random */
    if (root->sort_pathkeys == NULL)
        return true;

    /* not sorted on each target entry, the result can be random */
    if (list_length(root->sort_pathkeys) != list_length(plan->targetlist))
        return true;

    /* for each sort pathkey we must find the matching targetlist */
    foreach (lc, root->sort_pathkeys) {
        PathKey* pathkey = (PathKey*)lfirst(lc);
        EquivalenceClass* ec = pathkey->pk_eclass;

        /* opps, not found sgc's matching targetlist */
        if (get_sortgroupref_tle(ec->ec_sortref, plan->targetlist, false) == NULL)
            return true;
    }

    /* the targetlist is sorted */
    return false;
}

/*
 * @Description: add broadcast stream on sort plan after we pick a single dn
 * @input root: planner info of the current layer
 * @input lefttree: input plan to be changed
 */
Plan* make_stream_sort(PlannerInfo* root, Plan* lefttree)
{
    if (is_execute_on_coordinator(lefttree) || is_replicated_plan(lefttree))
        return lefttree;

    Plan* result_plan = lefttree;
    bool need_force_oneDN = is_result_random(root, lefttree);
    bool pick_one_dn = (!need_force_oneDN && root->query_level != 1);
    if (pick_one_dn) {
        pick_single_node_plan_for_replication(lefttree);
        result_plan = make_stream_plan(root, lefttree, NIL, 1.0, NULL);

        if (!(is_replicated_plan(lefttree))) {
            Sort* sortPlan = make_sort_from_pathkeys(root, result_plan, root->sort_pathkeys, -1.0);
            SimpleSort* streamSort = makeNode(SimpleSort);
            streamSort->numCols = sortPlan->numCols;
            streamSort->sortColIdx = sortPlan->sortColIdx;
            streamSort->sortOperators = sortPlan->sortOperators;
            streamSort->nullsFirst = sortPlan->nullsFirst;
            streamSort->sortToStore = false;
            streamSort->sortCollations = sortPlan->collations;
            ((Stream*)result_plan)->sort = streamSort;

            /*
             * "stream" node make this plan as replicated, as merge sort with multiple consumers
             * may hang in executor. So we enforce SORT results are produced on ONE random DN node.
             */
            pick_single_node_plan_for_replication(result_plan);
        }
    }
    return result_plan;
}

/*
 * for correlated replicate plan like:
 *    HashJoin
 *      Scan t1 
 *      Limit 1 (on all dns)
 *         Scan t2 -replicate table, correlate qual: (t1.b=t2.b)
 * the results for limit 1 is not stable for each execution of dn,
 * so a Sort is added
 *    HashJoin
 *      Scan t1 
 *      Limit 1 (on all dns)
 *        Sort --to keep results on each dn stable
 *         Scan t2 -replicate table, correlate qual: (t1.b=t2.b)
 */
Plan* add_sort_for_correlated_replicate_limit_plan(PlannerInfo* root,
                                                                Plan* lefttree,
                                                                Node* limitOffset,
                                                                Node* limitCount,
                                                                int64 offset_est,
                                                                int64 count_est,
                                                                double limit_tuples)
{
    Plan* result_plan = NULL;
    Plan* sortPlan = NULL;

    /* add sort if needed */
    if (IsA(lefttree, Sort) || IsA(lefttree, VecSort)) {
        /* no need to add sort if already sorted */
        sortPlan = lefttree;
    } else {
        /* add sort */
        sortPlan = (Plan *)make_sort_from_targetlist(root, lefttree, limit_tuples);
    }

    /* add limit */
    if (sortPlan != NULL) {
        result_plan = (Plan*)make_limit(root, sortPlan, limitOffset, limitCount, offset_est, count_est);
    } else {
        /* 
         * oops, cannot add sort above lefttree since the targetlist does not support sort op
         * later shall add a dummy tlist for sort
         */
        result_plan = (Plan*)make_limit(root, lefttree, limitOffset, limitCount, offset_est, count_est);
    }

    return result_plan;
}
                                                                
/*
 * Why we add a broadcast stream on sort-limit plan after we pick a single dn ?
 * -----------------------------------------------------------------------------
 * setop currently cannot handle the following cases
 *
 *    case1                case2
 *   -------              --------
 * Append(dn1, dn2)   Append(dn1, dn2)
 *   -limit(dn1)       -limit(dn1)
 *     -sort(dn1)        -sort(dn1)
 *      -broadcast         -broadcast
 *       ...                  ...
 *   -limit(dn2)       -hashed plan(dn1,dn2)
 *     -sort(dn2)             ...
 *      -broadcast
 *       ...
 * There are serval ways to handle this
 * (a) for case1, force two sortlimit execute on one dn
 * (b) for case2, add a redistribute upon sortlimit
 * (c) for case1 and case2 we can add a broadcast stream on sortlimit
 * (d) for case1 and case2 we can fix the executor to support Append executing
 *     this plan.
 * The plan (c) can handle both cases. That's why we add a broadcast stream here.
 * Even though plan(c) is the simplest way to solve the problem, that's might
 * not be the best way to do so. The more smart way to do so is to combine
 * plan(a) and plan(b) in redistributeInfo(). However, if we have limit
 * we are asumming the rows for broadcast is not huge.
 *
 */
Plan* make_stream_limit(PlannerInfo* root, Plan* lefttree, Node* limitOffset, Node* limitCount, int64 offset_est,
    int64 count_est, double limit_tuples, bool needs_sort)
{
    if (is_execute_on_coordinator(lefttree))
        return (Plan*)make_limit(root, lefttree, limitOffset, limitCount, offset_est, count_est);

#ifndef ENABLE_MULTIPLE_NODES
    if (lefttree->dop == 1) {
        return (Plan*)make_limit(root, lefttree, limitOffset, limitCount, offset_est, count_est);
    }
#endif

    Plan* result_plan = NULL;
    /*
     * For a replicate plan such as "scan on replicated table", the result could be random
     * if executed on different node, so stream node is needed and an
     * random datanode is picked to evalute the limit clause if not at top level of plan.
     */
    bool need_force_oneDN = is_result_random(root, lefttree);
    bool pick_one_dn = (need_force_oneDN && root->query_level != 1);


    if (is_replicated_plan(lefttree)) {
        if (need_force_oneDN && !IsA(lefttree, CteScan)) {
            if (!root->is_correlated) {
                pick_single_node_plan_for_replication(lefttree);
                result_plan = (Plan*)make_limit(root, lefttree, limitOffset, limitCount, offset_est, count_est);

                if (root->query_level != 1) {
                    /* broadcast the result to outer query */
                    result_plan = make_stream_plan(root, result_plan, NIL, 1.0, NULL);
                }
            } else {
                result_plan = add_sort_for_correlated_replicate_limit_plan(root,
                                  lefttree, limitOffset, limitCount, offset_est, count_est, limit_tuples);
            }
        } else {
            result_plan = (Plan*)make_limit(root, lefttree, limitOffset, limitCount, offset_est, count_est);
        }

        return result_plan;
    }

    /* make the first limit node with limitoffset equal zero */
    Node* tmp_limit_offset = create_offset_count(limitOffset, 0);
    Node* tmp_limit_count = limitCount;
    Plan* down_limit = NULL;

    /* we should adjust limitCount if there's offset specified */
    if (limitCount != NULL && limitOffset != NULL)
        tmp_limit_count = (Node*)make_op(make_parsestate(NULL),
            list_make1(makeString((char*)"+")),
            (Node*)copyObject(limitCount),
            (Node*)copyObject(limitOffset),
            -1);

    /*
     * since subplan doesn't support to calculate subexpr in both cn and dn side,
     * if there's subplan in top level, we remove limit node on dn side to support
     * query push down to dn side.
     */
    if (!root->glob->insideRecursion) {
        if (root->query_level > 1 || (root->query_level == 1 && check_subplan_expr(tmp_limit_count) == NIL &&
                                         check_subplan_expr(tmp_limit_offset) == NIL))
            down_limit =
                (Plan*)make_limit(root, lefttree, tmp_limit_offset, tmp_limit_count, 0, count_est + offset_est);
        else
            down_limit = lefttree;
        if (root->query_level == 1)
            result_plan = make_simple_RemoteQuery(down_limit, root, false);
        else {
            result_plan = make_stream_plan(root, down_limit, NIL, 0.0);
            if (((unsigned int)u_sess->attr.attr_sql.cost_param) & COST_ALTERNATIVE_MERGESORT)
                needs_sort = true;
        }
    } else {
        result_plan = lefttree;
    }

    /* make the sort node if necessary */
    if (root->sort_pathkeys) {
        Sort* sortPlan = make_sort_from_pathkeys(root, result_plan, root->sort_pathkeys, limit_tuples);

        /*
         * If a sort merge can be used, we can remove sort node.
         * Now we support CN or broadcast on one DN to do merge sort.
         */
        if (!needs_sort) {
            bool stream_mergesort = false;

            /*
             * If we don't pick one dn, that is, we have multiple consumers,
             * we don't use mergesort since it may hang in executor when
             * multi thread do mergesort
             */
            if (IsA(result_plan, Stream)) {
                Stream* streamPlan = (Stream*)result_plan;
                if (streamPlan->type == STREAM_BROADCAST && pick_one_dn)
                    stream_mergesort = true;
                else
                    needs_sort = true;
            }
            if (IsA(result_plan, RemoteQuery) || stream_mergesort) {
                SimpleSort* streamSort = makeNode(SimpleSort);
                streamSort->numCols = sortPlan->numCols;
                streamSort->sortColIdx = sortPlan->sortColIdx;
                streamSort->sortOperators = sortPlan->sortOperators;
                streamSort->nullsFirst = sortPlan->nullsFirst;
                streamSort->sortToStore = false;
                streamSort->sortCollations = sortPlan->collations;
                if (IsA(result_plan, RemoteQuery))
                    ((RemoteQuery*)result_plan)->sort = streamSort;
                else if (IsA(result_plan, Stream)) {
                    ((Stream*)result_plan)->sort = streamSort;
                }
            }
        }

        if (needs_sort)
            result_plan = (Plan*)sortPlan;
    }

#ifndef ENABLE_MULTIPLE_NODES
    if (IsA(result_plan, Limit) && limitOffset == NULL) {
        return result_plan;
    }
#endif

    /*
     * "stream" node make this plan as replicated, and LIMIT on this plan could produce random results
     * if it is evaluated on different DN nodes. so enforcing LIMIT results are produced on ONE random DN
     * node.
     */
    if (pick_one_dn) {
        pick_single_node_plan_for_replication(result_plan);
    }

    result_plan = (Plan*)make_limit(root, result_plan, limitOffset, limitCount, offset_est, count_est);

    if (pick_one_dn && (((unsigned int)u_sess->attr.attr_sql.cost_param & COST_ALTERNATIVE_MERGESORT) ||
                           root->is_under_recursive_cte)) {
        /* broadcast the result to outer query */
        result_plan = make_stream_plan(root, result_plan, NIL, 1.0, NULL);
    }

    if (root->query_level == 1)
        result_plan->exec_type = EXEC_ON_COORDS;

    return result_plan;
}
/*
 * make_result
 *	  Build a Result plan node
 *
 * If we have a subplan, assume that any evaluation costs for the gating qual
 * were already factored into the subplan's startup cost, and just copy the
 * subplan cost.  If there's no subplan, we should include the qual eval
 * cost.  In either case, tlist eval cost is not to be included here.
 */
BaseResult* make_result(PlannerInfo* root, List* tlist, Node* resconstantqual, Plan* subplan, List* qual)
{
    BaseResult* node = makeNode(BaseResult);
    Plan* plan = &node->plan;

    plan->targetlist = tlist;
    plan->qual = qual;
    plan->righttree = NULL;
    node->resconstantqual = resconstantqual;

    /* 
     * We currently apply this optimization in multiple nodes mode only
     * while sticking to the original plans in the single node mode, because
     * dummy path implementation for single node mode has not been completed yet.
     */
#ifdef ENABLE_MULTIPLE_NODES
    if (is_dummy_plan(plan)) {
        subplan = NULL;
    }
#endif
    plan->lefttree = subplan;

#ifdef STREAMPLAN
    if (IS_STREAM_PLAN && subplan) {
        inherit_plan_locator_info(plan, subplan);
    } else {
        plan->exec_type = EXEC_ON_ALL_NODES;
        plan->exec_nodes = ng_get_default_computing_group_exec_node();
    }
#endif

    if (subplan != NULL) {
        copy_plan_costsize(plan, subplan);
    } else {
        /*
         * origin comments:
         * 1. wrong if we have a set-valued function? (for multiple and plan_rows)
         * 2. XXX is it worth being smarter? (for plan_width)
         */
        init_plan_cost(plan);
        plan->plan_rows = 1.0;
        plan->total_cost = u_sess->attr.attr_sql.cpu_tuple_cost;

        if (resconstantqual != NULL) {
            QualCost qual_cost;

            cost_qual_eval(&qual_cost, (List*)resconstantqual, root);
            /* resconstantqual is evaluated once at startup */
            plan->startup_cost += qual_cost.startup + qual_cost.per_tuple;
            plan->total_cost += qual_cost.startup + qual_cost.per_tuple;
        }
    }

    return node;
}

static Plan* FindForeignScan(Plan* plan)
{
    Plan* result = NULL;
    if (plan == NULL)
        return NULL;
    switch (nodeTag(plan)) {
        case T_ForeignScan: {
            ForeignScan* fscan = (ForeignScan*)plan;
            if (isObsOrHdfsTableFormTblOid(fscan->scan_relid) || IS_LOGFDW_FOREIGN_TABLE(fscan->scan_relid) ||
                IS_POSTGRESFDW_FOREIGN_TABLE(fscan->scan_relid)) {
                return NULL;
            }
            return plan;
        }
        case T_Append: {
            Append* append = (Append*)plan;
            ListCell* lc = NULL;
            foreach (lc, append->appendplans) {
                result = (Plan*)lfirst(lc);
                result = FindForeignScan(result);
                if (result != NULL)
                    return result;
            }
        } break;
        case T_ModifyTable: {
            ModifyTable* mt = (ModifyTable*)plan;
            ListCell* lc = NULL;
            foreach (lc, mt->plans) {
                result = (Plan*)lfirst(lc);
                result = FindForeignScan(result);
                if (result != NULL)
                    return result;
            }
        } break;
        case T_SubqueryScan: {
            SubqueryScan* ss = (SubqueryScan*)plan;
            if (ss->subplan)
                result = FindForeignScan(ss->subplan);
        } break;
        case T_MergeAppend: {
            MergeAppend* ma = (MergeAppend*)plan;
            ListCell* lc = NULL;
            foreach (lc, ma->mergeplans) {
                result = (Plan*)lfirst(lc);
                result = FindForeignScan(result);
                if (result != NULL)
                    return result;
            }
        } break;

        case T_BitmapAnd: {
            BitmapAnd* ba = (BitmapAnd*)plan;
            ListCell* lc = NULL;
            foreach (lc, ba->bitmapplans) {
                result = (Plan*)lfirst(lc);
                result = FindForeignScan(result);
                if (result == NULL)
                    return result;
            }
        } break;

        case T_BitmapOr: {
            BitmapOr* bo = (BitmapOr*)plan;
            ListCell* lc = NULL;
            foreach (lc, bo->bitmapplans) {
                result = (Plan*)lfirst(lc);
                result = FindForeignScan(result);
                if (result == NULL)
                    return result;
            }
        } break;
        default: {
            if (plan->lefttree != NULL)
                result = FindForeignScan(plan->lefttree);
            if (result == NULL && plan->righttree != NULL)
                result = FindForeignScan(plan->righttree);
        }
    }

    return result;
}

#ifdef STREAMPLAN
uint32 getDistSessionKey(List* fdw_private)
{
    Assert(fdw_private && list_length(fdw_private) > 0);
    ListCell* lc = NULL;
    uint32 distSessionKey = 0;

    foreach (lc, fdw_private) {
#ifdef ENABLE_MOT
        /*
         * MOT FDW may put a node of any type into the list, so if we are looking for
         * some specific type, we need to first make sure that it's the correct type.
         */
        Node* node = (Node*)lfirst(lc);
        if (IsA(node, DefElem)) {
#endif
            DefElem* defElem = (DefElem*)lfirst(lc);
            if (strcmp("session_key", defElem->defname) == 0)
                distSessionKey = intVal(defElem->arg);
#ifdef ENABLE_MOT
        }
#endif
    }

    return distSessionKey;
}
#endif

static void PlanForeignModify(PlannerInfo* root, ModifyTable* node, List* resultRelations)
{
    List* fdw_private_list = NIL;
    int i = 0;
    ListCell* lc = NULL;

    /*
     * For each result relation that is a foreign table, allow the FDW to
     * construct private plan data, and accumulate it all into a list.
     */
    foreach (lc, resultRelations) {
        Index rti = lfirst_int(lc);
        FdwRoutine* fdwroutine = NULL;
        List* fdw_private = NIL;

        /*
         * If possible, we want to get the FdwRoutine from our RelOptInfo for
         * the table.  But sometimes we don't have a RelOptInfo and must get
         * it the hard way.  (In INSERT, the target relation is not scanned,
         * so it's not a baserel; and there are also corner cases for
         * updatable views where the target rel isn't a baserel.)
         */
        if (rti < (uint)root->simple_rel_array_size && root->simple_rel_array[rti] != NULL) {
            RelOptInfo* resultRel = root->simple_rel_array[rti];

            fdwroutine = resultRel->fdwroutine;
        } else {
            RangeTblEntry* rte = rt_fetch(rti, (root)->parse->rtable);

            Assert(rte->rtekind == RTE_RELATION);
            if (rte->relkind == RELKIND_FOREIGN_TABLE || rte->relkind == RELKIND_STREAM)
                fdwroutine = GetFdwRoutineByRelId(rte->relid);
            else
                fdwroutine = NULL;
        }

        if (fdwroutine != NULL && fdwroutine->PlanForeignModify != NULL)
            fdw_private = fdwroutine->PlanForeignModify(root, node, rti, i);
        else
            fdw_private = NIL;
        fdw_private_list = lappend(fdw_private_list, fdw_private);
        i++;
    }
    node->fdwPrivLists = fdw_private_list;
}

/*
 * @Description:
 *    If the plan node under modifytable is parallelized,
 *    then we need to gather it because modify table do not
 *    support parallel.
 *
 * @param[IN] plan: the modify table.
 *
 * @return void
 */
static void deparallelize_modifytable(List* subplans)
{
    if (u_sess->opt_cxt.query_dop > 1) {
        ListCell* lc = NULL;

        /*
         * Add local gather to subplan if it's parallelized.
         */
        foreach (lc, subplans) {
            Plan* subplan = (Plan*)lfirst(lc);
            if (subplan->dop > 1) {
                Plan* local_gather = create_local_gather(subplan);
                lfirst(lc) = (void*)local_gather;
            }
        }
    }
}

/*
 * make_modifytable
 *	  Build a ModifyTable plan node
 *
 * Currently, we don't charge anything extra for the actual table modification
 * work, nor for the RETURNING expressions if any.	It would only be window
 * dressing, since these are always top-level nodes and there is no way for
 * the costs to change any higher-level planning choices.  But we might want
 * to make it look better sometime.
 */
#ifdef STREAMPLAN
Plan* make_modifytable(PlannerInfo* root, CmdType operation, bool canSetTag, List* resultRelations, List* subplans,
    List* returningLists, List* rowMarks, int epqParam, bool partKeyUpdated, Index mergeTargetRelation,
    List* mergeSourceTargetList, List* mergeActionList, UpsertExpr* upsertClause, bool isDfsStore)
#else
ModifyTable* make_modifytable(CmdType operation, bool canSetTag, List* resultRelations, List* subplans,
    List* returningLists, List* rowMarks, int epqParam, bool partKeyUpdated, Index mergeTargetRelation,
    List* mergeSourceTargetList, List* mergeActionList, UpsertExpr* upsertClause, bool isDfsStore)
#endif
{
    ModifyTable* node = makeNode(ModifyTable);
    Plan* plan = &node->plan;
    double total_size;
    ListCell* subnode = NULL;
    double local_rows = 0;
    Path modify_path; /* dummy for result of cost_modify node */
    int width = 0;
    Oid resultRelOid = InvalidOid; /* the relOid is used to cost memory when open relations. */
#ifdef STREAMPLAN
    ExecNodes* exec_nodes = NULL;
#endif

    Assert(list_length(resultRelations) == list_length(subplans));
    Assert(returningLists == NIL || list_length(resultRelations) == list_length(returningLists));

    if (operation == CMD_MERGE) {
        check_entry_mergeinto_replicate(root->parse);
    }

    /*
     * Compute cost as sum of subplan costs.
     */
    init_plan_cost(plan);
    total_size = 0;

    /*
     * Modify table can not parallel.
     * If the subplan already parallelize,
     * add local gather.
     */
    deparallelize_modifytable(subplans);

    foreach (subnode, subplans) {
        Plan* subplan = (Plan*)lfirst(subnode);

        if (subnode == list_head(subplans)) /* first node? */
            plan->startup_cost = subplan->startup_cost;
        plan->total_cost += subplan->total_cost;
        plan->plan_rows += subplan->plan_rows;
        local_rows += PLAN_LOCAL_ROWS(subplan);
        total_size += subplan->plan_width * PLAN_LOCAL_ROWS(subplan);

#ifdef ENABLE_MULTIPLE_NODES
        if (IS_STREAM_PLAN) {
            exec_nodes = pgxc_merge_exec_nodes(exec_nodes, subplan->exec_nodes);
        } else {
            exec_nodes = ((subplan->exec_type == EXEC_ON_COORDS) ? ng_get_single_node_group_exec_node()
                                                                 : ng_get_installation_group_exec_node());
        }
#else
        exec_nodes = ((subplan->exec_type == EXEC_ON_COORDS) ? ng_get_single_node_group_exec_node()
                                                             : ng_get_installation_group_exec_node());
#endif
    }

    if (plan->plan_rows > 0)
        plan->plan_width = (int)rint(total_size / local_rows);
    else
        plan->plan_width = 0;

    node->plan.dop = 1;
    node->plan.lefttree = NULL;
    node->plan.righttree = NULL;
    node->plan.qual = NIL;
    /* setrefs.c will fill in the targetlist, if needed */
    node->plan.targetlist = NIL;

    node->operation = operation;
    node->canSetTag = canSetTag;
    node->resultRelations = resultRelations;
    node->resultRelIndex = -1; /* will be set correctly in setrefs.c */
    node->plans = subplans;
    node->returningLists = returningLists;
    node->rowMarks = rowMarks;
    node->epqParam = epqParam;
    node->partKeyUpdated = partKeyUpdated;
    node->mergeTargetRelation = mergeTargetRelation;
    node->mergeSourceTargetList = mergeSourceTargetList;
    node->mergeActionList = mergeActionList;
    if (upsertClause != NULL) {
        node->upsertAction = upsertClause->upsertAction;
        node->updateTlist = upsertClause->updateTlist;
        node->exclRelTlist = upsertClause->exclRelTlist;
        node->exclRelRTIndex = upsertClause->exclRelIndex;
        node->upsertWhere = upsertClause->upsertWhere;
    } else {
        node->upsertAction = UPSERT_NONE;
        node->updateTlist = NIL;
        node->exclRelTlist = NIL;
        node->upsertWhere = NULL;
    }

#ifdef STREAMPLAN
    node->plan.exec_nodes = exec_nodes;

    Index resultidx;
    if (resultRelations != NIL && root->simple_rte_array != NULL) {
        resultidx = (Index)linitial_int(resultRelations);
        RangeTblEntry* result_rte = root->simple_rte_array[resultidx];
        if (resultidx <= (Index)root->simple_rel_array_size) {
            resultRelOid = result_rte->relid;
            if (result_rte->orientation == REL_COL_ORIENTED || result_rte->orientation == REL_PAX_ORIENTED ||
                result_rte->orientation == REL_TIMESERIES_ORIENTED)
                node->plan.vec_output = true;
            else if (result_rte->orientation == REL_ROW_ORIENTED)
                node->plan.vec_output = false;
        }

        /*
         * If the FROM clause is empty, append a dummy RTE_RESULT RTE at the end of the parse->rtable 
         * during subquery_planner. For that case, vec_output should be false.
         */
        RangeTblEntry* result_add = root->simple_rte_array[root->simple_rel_array_size - 1];
        if (result_add != NULL && result_add->rtekind == RTE_RESULT) {
            node->plan.vec_output = false;
        }
    }

    if (IS_STREAM_PLAN) {
        Plan* rtn = NULL;
        if (operation == CMD_INSERT || operation == CMD_UPDATE || operation == CMD_DELETE || operation == CMD_MERGE) {
            Plan** subplan = (Plan**)&(linitial(subplans));

            ForeignScan* fscan = NULL;
            rtn = mark_distribute_dml(root, subplan, node, &resultRelations, mergeActionList);

            /* Set cache entry if it has */
            if ((fscan = (ForeignScan*)FindForeignScan(*subplan))) {
                uint32 distSessionKey = getDistSessionKey(fscan->fdw_private);
                fscan->errCache = GetForeignErrCacheEntry(fscan->scan_relid, distSessionKey);
                fscan->needSaveError = true;
                if (!IsA(rtn, RemoteQuery) && (fscan->errCache != NULL))
                    ereport(ERROR,
                        (errmodule(MOD_OPT),
                            errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                            (errmsg("Un-support feature"),
                                errdetail("statements contains unsupport feature to foreign table,"
                                          "please disable error log table and try again"))));
                ((ModifyTable*)node)->cacheEnt = fscan->errCache;
            }

            /* Correct the gather's plan_rows and cost */
            if (IsA(rtn, RemoteQuery)) {
                rtn->plan_rows = 1;

                double size = Max(rtn->plan_width, 128) / 8192.0;
                unsigned int num_datanodes = ng_get_dest_num_data_nodes(rtn->lefttree);
                rtn->total_cost =
                    rtn->lefttree->total_cost + size * (g_instance.cost_cxt.send_kdata_cost +
                                                           g_instance.cost_cxt.receive_kdata_cost * num_datanodes);
            }
        }
        PlanForeignModify(root, (ModifyTable*)node, resultRelations);

        /* caculate the memory of insert|update|delete for cstore table and dfs table. Merge is not Adapted. */
        if ((operation == CMD_INSERT || operation == CMD_DELETE || operation == CMD_UPDATE) && node->plan.vec_output) {
            Plan* tmpPlan = (Plan*)lfirst(list_head(node->plans));
            if (NULL != tmpPlan) {
                width = get_plan_actual_total_width(tmpPlan, root->glob->vectorized, OP_SORT);
                if (operation == CMD_INSERT) {
                    cost_insert(&modify_path,
                        root->glob->vectorized,
                        rtn->total_cost,
                        PLAN_LOCAL_ROWS(tmpPlan),
                        width,
                        0.0,
                        u_sess->opt_cxt.op_work_mem,
                        SET_DOP(node->plan.dop),
                        resultRelOid,
                        isDfsStore,
                        &node->mem_info);
                }
                if (operation == CMD_DELETE) {
                    cost_delete(&modify_path,
                        root->glob->vectorized,
                        rtn->total_cost,
                        PLAN_LOCAL_ROWS(tmpPlan),
                        width,
                        0.0,
                        u_sess->opt_cxt.op_work_mem,
                        SET_DOP(node->plan.dop),
                        resultRelOid,
                        isDfsStore,
                        &node->mem_info);
                }
                if (operation == CMD_UPDATE) {
                    cost_update(&modify_path,
                        root->glob->vectorized,
                        rtn->total_cost,
                        PLAN_LOCAL_ROWS(tmpPlan),
                        width,
                        0.0,
                        u_sess->opt_cxt.op_work_mem,
                        SET_DOP(node->plan.dop),
                        resultRelOid,
                        isDfsStore,
                        &node->mem_info);
                }
            }
        }
        return rtn;
    } else {
        if (operation == CMD_INSERT || operation == CMD_UPDATE || operation == CMD_DELETE) {
            Plan* subplan = (Plan*)(linitial(subplans));
            ForeignScan* fscan = NULL;
            if ((fscan = (ForeignScan*)FindForeignScan(subplan)) != NULL) {
                if (!CheckSupportedFDWType(fscan->scan_relid))
                    ereport(ERROR,
                        (errmodule(MOD_OPT),
                            errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            (errmsg("Un-support feature"),
                                errdetail("statements contains unsupport feature to foreign table"))));
            }
        }
    }
    PlanForeignModify(root, (ModifyTable*)node, resultRelations);

    return (Plan*)node;
#else
    return node;
#endif
}

#ifdef STREAMPLAN
/*
 * Brief        : Add modify plan node. More than one modify plan nodes would be created
 *                when the subplans list length > 1.
 */
Plan* make_modifytables(PlannerInfo* root, CmdType operation, bool canSetTag, List* resultRelations, List* subplans,
    List* returningLists, List* rowMarks, int epqParam, bool partKeyUpdated, bool isDfsStore, Index mergeTargetRelation,
    List* mergeSourceTargetList, List* mergeActionList, UpsertExpr* upsertClause)
#else
ModifyTable* make_modifytables(CmdType operation, bool canSetTag, List* resultRelations, List* subplans,
    List* returningLists, List* rowMarks, int epqParam, bool partKeyUpdated, bool isDfsStore, Index mergeTargetRelation,
    List* mergeSourceTargetList, List* mergeActionList, UpsertExpr* upsertClause)
#endif
{
    if (isDfsStore) {
        Plan* subPlan = NULL;
        List* appendSubPlans = NIL;
        int subResultRelation;
        ListCell* cell1 = NULL;
        ListCell* cell2 = NULL;
        Plan* appendPlan = NULL;
        Plan* topGatherNode = NULL;

        forboth(cell1, subplans, cell2, resultRelations)
        {
            subPlan = (Plan*)lfirst(cell1);
            subResultRelation = (int)lfirst_int(cell2);

            /* And last, tack on a ModifyTable node to do the UPDATE/DELETE work */
#ifdef STREAMPLAN
            Plan* mt_stream_plan = make_modifytable(root,
                operation,
                canSetTag,
                list_make1_int(subResultRelation),
                list_make1(subPlan),
                returningLists,
                rowMarks,
                epqParam,
                partKeyUpdated,
                0,
                NULL,
                NULL,
                upsertClause,
                isDfsStore);
            /*
             * We must adjust the plan tree. Because the make_modifytable function would make a
             * RemoteQuery(Gather) node which node will not be accpted.
             *
             * The following plan tree will not be accepted.
             *                             QUERY PLAN
             *	-------------------------------------------------------------------------------------------------
             *	 Append  (cost=0.00..20.20 rows=20 width=14)
             *	   ->  Streaming (type: GATHER)  (cost=0.00..0.00 rows=0 width=0)
             *	         Node/s: All datanodes
             *	         ->  Delete on hdfs_table005  (cost=0.00..0.00 rows=0 width=0)
             *	               ->  Row Adapter  (cost=0.00..0.00 rows=0 width=14)
             *	                     ->  Dfs Scan on hdfs_table005  (cost=0.00..0.00 rows=0 width=14)
             *	   ->  Streaming (type: GATHER)  (cost=0.00..20.20 rows=20 width=14)
             *	         Node/s: All datanodes
             *	         ->  Delete on pg_delta_50207 hdfs_table005  (cost=0.00..10.10 rows=10 width=14)
             *	               ->  Seq Scan on pg_delta_50207 hdfs_table005  (cost=0.00..10.10 rows=10 width=14)
             *
             * we need the following plan tree.
             *								QUERY PLAN
             *	-------------------------------------------------------------------------------------------------
             *	 Streaming (type: GATHER)  (cost=0.00..20.20 rows=20 width=14)
             *	   Node/s: All datanodes
             *	   ->  Append  (cost=0.00..20.11 rows=20 width=14)
             *	         ->  Delete on hdfs_table005  (cost=0.00..10.01 rows=10 width=14)
             *	               ->  Row Adapter  (cost=10.01..10.01 rows=10 width=14)
             *	                     ->  CStore Scan on hdfs_table005  (cost=0.00..10.01 rows=10 width=14)
             *	         ->  Delete on pg_delta_58399 hdfs_table005  (cost=0.00..10.10 rows=10 width=14)
             *	               ->  Seq Scan on pg_delta_58399 hdfs_table005  (cost=0.00..10.10 rows=10 width=14)
             */
            if (IsA(mt_stream_plan, RemoteQuery)) {
                topGatherNode = mt_stream_plan;
                mt_stream_plan = mt_stream_plan->lefttree;
            }
            if (!IS_STREAM) {
                mt_stream_plan = pgxc_make_modifytable(root, mt_stream_plan);
            }
            appendSubPlans = lappend(appendSubPlans, (void*)mt_stream_plan);
#else
            ModifyTable* mtplan = make_modifytable(operation,
                canSetTag,
                list_make1_int(subResultRelation),
                list_make1(subPlan),
                returningLists,
                rowMarks,
                epqParam,
                partKeyUpdated,
                0,
                NULL,
                NULL,
                upsertClause,
                isDfsStore);
            appendSubPlans = lappend(appendSubPlans, (void*)mtplan);
#endif
        }

        appendPlan = (Plan*)make_append(appendSubPlans, NIL);
        if (IS_STREAM_PLAN) {
            mark_distribute_setop(root, (Node*)appendPlan, true, false);
        }

#ifdef PGXC
        if (IS_STREAM_PLAN && topGatherNode) {
            topGatherNode->lefttree = appendPlan;
            topGatherNode->righttree = NULL;
            return topGatherNode;
        } else
#endif
            return appendPlan;
    } else {
        /* And last, tack on a ModifyTable node to do the UPDATE/DELETE work */
#ifdef PGXC
#ifdef STREAMPLAN
        Plan* mt_stream_plan = make_modifytable(root,
            operation,
            canSetTag,
            resultRelations,
            subplans,
            returningLists,
            rowMarks,
            epqParam,
            partKeyUpdated,
            mergeTargetRelation,
            mergeSourceTargetList,
            mergeActionList,
            upsertClause,
            isDfsStore);
        if (IS_STREAM_PLAN)
            return mt_stream_plan;
        else
            return pgxc_make_modifytable(root, mt_stream_plan);
#else
        ModifyTable* mtplan = make_modifytable(operation,
            canSetTag,
            resultRelations,
            subplans,
            returningLists,
            rowMarks,
            epqParam,
            partKeyUpdated,
            mergeTargetRelation,
            mergeSourceTargetList,
            mergeActionList,
            upsertClause
            isDfsStore);
        return pgxc_make_modifytable(root, (Plan*)mtplan);
#endif
#else
        return (Plan*)make_modifytable(operation,
            canSetTag,
            resultRelations,
            subplans,
            returningLists,
            rowMarks,
            epqParam,
            partKeyUpdated,
            mergeTargetRelation,
            mergeSourceTargetList,
            mergeActionList,
            upsertClause,
            isDfsStore);
#endif
    }
}

/*
 * is_projection_capable_plan
 *		Check whether a given Plan node is able to do projection.
 */
bool is_projection_capable_plan(Plan* plan)
{
    /* Most plan types can project, so just list the ones that can't */
    switch (nodeTag(plan)) {
        case T_Hash:
        case T_Material:
        case T_Sort:
        case T_Unique:
        case T_SetOp:
        case T_LockRows:
        case T_Limit:
        case T_ModifyTable:
        case T_Append:
        case T_MergeAppend:
        case T_RecursiveUnion:
        case T_Stream:
            return false;

        case T_PartIterator:
            /* target: Data Partition */
            /*
             * If subplan of the PartIterator is a scan, we will do projection
             * on scan node, so no need to add a result node above PartIterator.
             */
            switch (nodeTag(plan->lefttree)) {
                case T_SeqScan:
                case T_CStoreScan:
                case T_DfsScan:
#ifdef ENABLE_MULTIPLE_NODES
                case T_TsStoreScan:
#endif   /* ENABLE_MULTIPLE_NODES */
                case T_DfsIndexScan:
                case T_IndexScan:
                case T_IndexOnlyScan:
                case T_BitmapHeapScan:
                case T_TidScan:
                case T_CStoreIndexScan:
                case T_CStoreIndexHeapScan:
                    return true;
                default:
                    return false;
            }
        case T_ExtensiblePlan:
            return ((ExtensiblePlan *)plan)->flags & EXTENSIBLEPATH_SUPPORT_PROJECTION;
        default:
            break;
    }
    return true;
}

/*
 * If this plan is for partitioned table, scan plan's iterator-referenced
 * atrributes must be specified.
 */
static Plan* setPartitionParam(PlannerInfo* root, Plan* plan, RelOptInfo* rel)
{
    if (root->isPartIteratorPlanning) {
        if (!PointerIsValid(plan) || !PointerIsValid(rel) || !PointerIsValid(root)) {
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                        errmsg("Fail to create path for partitioned table by the lack of info"))));
        }

        switch (plan->type) {
            case T_SeqScan:
            case T_CStoreScan:
#ifdef ENABLE_MULTIPLE_NODES
            case T_TsStoreScan:
#endif   /* ENABLE_MULTIPLE_NODES */
            case T_IndexScan:
            case T_IndexOnlyScan:
            case T_BitmapHeapScan:
            case T_BitmapIndexScan:
            case T_TidScan:
            case T_CStoreIndexScan:
            case T_CStoreIndexCtidScan:
            case T_CStoreIndexHeapScan: {
                Scan* scan = (Scan*)plan;
                scan->isPartTbl = true;
                scan->partScanDirection = ForwardScanDirection;
                scan->plan.paramno = root->curIteratorParamIndex;
                scan->plan.subparamno = root->curSubPartIteratorParamIndex;
                scan->itrs = root->curItrs;
                scan->pruningInfo = rel->pruning_result;
            } break;

            default:
                ereport(ERROR,
                    (errmodule(MOD_OPT),
                        (errcode(ERRCODE_UNEXPECTED_NODE_STATE),
                            errmsg("Only Scan operator have patition attribute"))));
                break;
        }
    }
    return plan;
}
static Plan* setBucketInfoParam(PlannerInfo* root, Plan* plan, RelOptInfo* rel)
{
    if (rel->bucketInfo != NULL) {
        if (!PointerIsValid(plan) || !PointerIsValid(rel) || !PointerIsValid(root)) {
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                        errmsg("Fail to create path for partitioned table by the lack of info"))));
        }
        switch (plan->type) {
            case T_SeqScan:
            case T_CStoreScan:
            case T_IndexScan:
            case T_IndexOnlyScan:
            case T_BitmapHeapScan:
            case T_BitmapIndexScan:
            case T_TidScan:
            case T_CStoreIndexScan:
            case T_CStoreIndexCtidScan:
            case T_CStoreIndexHeapScan: {
                Scan* scan = (Scan*)plan;
                if (rel->bucketInfo->buckets == NIL &&
                    plan->exec_nodes->bucketid != INVALID_BUCKET_ID) {
                     /* we use bucketid computed by GetRelationNodes */
                     scan->bucketInfo = makeNode(BucketInfo);
                     scan->bucketInfo->buckets = lappend_int(scan->bucketInfo->buckets, plan->exec_nodes->bucketid);
                } else {
                     /* we use bucketid computed by BucketPruningMain */
                     scan->bucketInfo = rel->bucketInfo;
                }
            } break;
            default:
                ereport(ERROR,
                    (errmodule(MOD_OPT),
                        (errcode(ERRCODE_UNEXPECTED_NODE_STATE),
                            errmsg("Only Scan operator have BucketInfo attribute"))));
                break;
        }
    }

    return plan;
}

#ifdef PGXC
/*
 * Wrapper functions to expose some functions to PGXC planner. These functions
 * are meant to be wrappers just calling the static function in this file. If
 * you need to add more functionality, add it to the original function.
 */
List* pgxc_order_qual_clauses(PlannerInfo* root, List* clauses)
{
    return order_qual_clauses(root, clauses);
}

List* pgxc_build_relation_tlist(RelOptInfo* rel)
{
    return build_relation_tlist(rel);
}

void pgxc_copy_path_costsize(Plan* dest, Path* src)
{
    copy_path_costsize(dest, src);
}

Plan* pgxc_create_gating_plan(PlannerInfo* root, Plan* plan, List* quals)
{
    return create_gating_plan(root, plan, quals);
}
#endif /* PGXC */

#ifdef STREAMPLAN

/*
 * add_agg_node_to_tlist
 * Add the given node to the target list to be sent to the Datanode. If it's
 * Aggref node, also change the passed in node to point to the Aggref node in
 * the Datanode's target list.
 */
static List* add_agg_node_to_tlist(List* remote_tlist, Node* expr, Index ressortgroupref)
{
    TargetEntry* remote_tle = NULL;
    Oid saved_aggtype = 0;

    /*
     * When we add an aggregate to the remote targetlist the aggtype of such
     * Aggref node is changed to aggtrantype. Hence while searching a given
     * Aggref in remote targetlist, we need to change the aggtype accordingly
     * and then switch it back.
     */
    if (IsA(expr, Aggref)) {
        Aggref* aggref = (Aggref*)expr;
        saved_aggtype = aggref->aggtype;
        aggref->aggtype = aggref->aggtrantype;
    }
    remote_tle = tlist_member(expr, remote_tlist);
    if (IsA(expr, Aggref))
        ((Aggref*)expr)->aggtype = saved_aggtype;

    if (remote_tle == NULL) {
        remote_tle = makeTargetEntry((Expr*)copyObject(expr), list_length(remote_tlist) + 1, NULL, false);
        /* Copy GROUP BY/SORT BY reference for the locating group by columns */
        remote_tle->ressortgroupref = ressortgroupref;
        remote_tlist = lappend(remote_tlist, remote_tle);
    } else {
        if (remote_tle->ressortgroupref == 0) {
            remote_tle->ressortgroupref = ressortgroupref;
        } else if (ressortgroupref == 0) {
            /* do nothing remote_tle->ressortgroupref has the right value */
        } else if (remote_tle->ressortgroupref != ressortgroupref) {
            remote_tle = makeTargetEntry((Expr*)copyObject(expr), list_length(remote_tlist) + 1, NULL, false);
            /* Copy GROUP BY/SORT BY reference for the locating group by columns */
            remote_tle->ressortgroupref = ressortgroupref;
            remote_tlist = lappend(remote_tlist, remote_tle);
        }
    }

    /*
     * Replace the args of the local Aggref with Aggref node to be
     * included in RemoteQuery node, so that set_plan_refs can convert
     * the args into VAR pointing to the appropriate result in the tuple
     * coming from RemoteQuery node
     * should we push this change in targetlists of plans above?
     */
    if (IsA(expr, Aggref)) {
        Aggref* local_aggref = (Aggref*)expr;
        Aggref* remote_aggref = (Aggref*)remote_tle->expr;
        Assert(IsA(remote_tle->expr, Aggref));

        if (need_adjust_agg_inner_func_type(remote_aggref))
            remote_aggref->aggtype = remote_aggref->aggtrantype;
        local_aggref->aggstage = remote_aggref->aggstage + 1;
    }
    return remote_tlist;
}

/*
 * process_agg_targetlist
 * The function scans the targetlist to check if the we can push anything
 * from the targetlist to the Datanode. Following rules govern the choice
 * 1. Either all of the aggregates are pushed to the Datanode or none is pushed
 * 2. If there are no aggregates, the targetlist is good to be shipped as is
 * 3. If aggregates are involved in expressions, we push the aggregates to the
 *    Datanodes but not the involving expressions.
 *
 * The function constructs the targetlist for the query to be pushed to the
 * Datanode. It modifies the local targetlist to point to the expressions in
 * remote targetlist wherever necessary (e.g. aggregates)
 *
 * we should be careful while pushing the function expressions, it's
 * better to push functions like strlen() which can be evaluated at the
 * Datanode, but we should avoid pushing functions which can only be evaluated
 * at Coordinator.
 */
List* process_agg_targetlist(PlannerInfo* root, List** local_tlist)
{
    bool shippable_remote_tlist = true;
    List* remote_tlist = NIL;
    List* orig_local_tlist = NIL; /* Copy original local_tlist, in case it changes */
    ListCell* temp = NULL;
    List* dep_oids = get_parse_dependency_rel_list(root->parse->constraintDeps);

    /*
     * Walk through the target list and find out whether we can push the
     * aggregates and grouping to Datanodes. Also while doing so, create the
     * targetlist for the query to be shipped to the Datanode. Adjust the local
     * targetlist accordingly.
     */
    foreach (temp, *local_tlist) {
        TargetEntry* local_tle = (TargetEntry*)lfirst(temp);
        Node* expr = (Node*)local_tle->expr;
        foreign_qual_context context;

        foreign_qual_context_init(&context);
        /*
         * If the expression is not Aggref but involves aggregates (has Aggref
         * nodes in the expression tree, we can not push the entire expression
         * to the Datanode, but push those aggregates to the Datanode, if those
         * aggregates can be evaluated at the Datanodes (if is_foreign_expr
         * returns true for entire expression). To evaluate the rest of the
         * expression, we need to fetch the values of VARs participating in the
         * expression. But, if we include the VARs under the aggregate nodes,
         * they may not be part of GROUP BY clause, thus generating an invalid
         * query. Hence, is_foreign_expr() wouldn't collect VARs under the
         * expression tree rooted under Aggref node.
         * For example, the original query is
         * SELECT sum(val) * val2 FROM tab1 GROUP BY val2;
         * the query pushed to the Datanode is
         * SELECT sum(val), val2 FROM tab1 GROUP BY val2;
         * Notice that, if we include val in the query, it will become invalid.
         */
        context.collect_vars = true;

        if (!is_foreign_expr(expr, &context)) {
            shippable_remote_tlist = false;
            break;
        }

        /*
         * We are about to change the local_tlist, check if we have already
         * copied original local_tlist, if not take a copy
         */
        if ((orig_local_tlist == NULL) && (IsA(expr, Aggref) || context.aggs))
            orig_local_tlist = (List*)copyObject(*local_tlist);

        /*
         * if there are aggregates involved in the expression, whole expression
         * can not be pushed to the Datanode. Pick up the aggregates and the
         * VAR nodes not covered by aggregates.
         */
        if (context.aggs) {
            ListCell* lcell = NULL;
            /*
             * if the target list expression is an Aggref, then the context should
             * have only one Aggref in the list and no VARs.
             */
            Assert(!IsA(expr, Aggref) ||
                   (list_length(context.aggs) == 1 && linitial(context.aggs) == expr && !context.vars));

            /* copy the aggregates into the remote target list */
            foreach (lcell, context.aggs) {
                Assert(IsA(lfirst(lcell), Aggref) || IsA(lfirst(lcell), GroupingFunc));
                remote_tlist = add_agg_node_to_tlist(remote_tlist, (Node*)lfirst(lcell), 0);
            }
            /*
             * copy the vars into the remote target list, only when the rel of var
             * has dependency constraint, else semantic can ganrantee that the
             * vars are group by columns, and they already exist in targetlist
             */
            foreach (lcell, context.vars) {
                Var* var = (Var*)lfirst(lcell);

                Assert(IsA(var, Var));
                if (var_from_dependency_rel(root->parse, var, dep_oids) ||
                    var_from_sublink_pulluped(root->parse, var))
                    remote_tlist = add_agg_node_to_tlist(remote_tlist, (Node*)lfirst(lcell), 0);
            }
        } else {
            /* Expression doesn't contain any aggregate, but referenced by a sort/group clause */
            remote_tlist = add_agg_node_to_tlist(remote_tlist, expr, local_tle->ressortgroupref);

            if (0 == local_tle->ressortgroupref) {
                ListCell* cell = NULL;
                List* vars = pull_var_clause((Node*)expr, PVC_INCLUDE_AGGREGATES, PVC_INCLUDE_PLACEHOLDERS);

                foreach (cell, vars) {
                    /* add flatten vars as we do in make_windowInputTargetList */
                    remote_tlist = add_agg_node_to_tlist(remote_tlist, (Node*)lfirst(cell), 0);
                }
            }
        }

        foreign_qual_context_free(&context);
    }

    if (!shippable_remote_tlist) {
        /*
         * If local_tlist has changed but we didn't find anything shippable to
         * Datanode, we need to restore the local_tlist to original state,
         */
        if (orig_local_tlist != NIL)
            *local_tlist = orig_local_tlist;
        if (remote_tlist != NIL) {
            list_free_deep(remote_tlist);
            remote_tlist = NIL;
        }
    } else if (orig_local_tlist != NIL) {
        /*
         * If we have changed the targetlist passed, we need to pass back the
         * changed targetlist. Free the copy that has been created.
         */
        list_free_deep(orig_local_tlist);
    }
    list_free_ext(dep_oids);

    return remote_tlist;
}

/*
 * process_agg_having_clause
 * For every expression in the havingQual take following action
 * 1. If it has aggregates, which can be evaluated at the Datanodes, add those
 *    aggregates to the targetlist and modify the local aggregate expressions to
 *    point to the aggregate expressions being pushed to the Datanode. Add this
 *    expression to the local qual to be evaluated locally.
 * 2. If the expression does not have aggregates and the whole expression can be
 *    evaluated at the Datanode, add the expression to the remote qual to be
 *    evaluated at the Datanode.
 * 3. If qual contains an expression which can not be evaluated at the data
 *    node, the parent group plan can not be reduced to a remote_query.
 */
static List* process_agg_having_clause(
    PlannerInfo* root, List* remote_tlist, Node* havingQual, List** local_qual, bool* reduce_plan)
{
    foreign_qual_context context;
    List* qual = NIL;
    ListCell* temp = NULL;

    *reduce_plan = true;
    *local_qual = NIL;

    if (havingQual == NULL)
        return remote_tlist;
    /*
     * We expect the quals in the form of List only. Is there a
     * possibility that the quals will be another form?
     */
    if (!IsA(havingQual, List)) {
        *reduce_plan = false;
        return remote_tlist;
    }
    /*
     * Copy the havingQual so that the copy can be modified later. In case we
     * back out in between, the original expression remains intact.
     */
    qual = (List*)copyObject(havingQual);
    foreach (temp, qual) {
        Node* expr = (Node*)lfirst(temp);
        foreign_qual_context_init(&context);
        if (!is_foreign_expr(expr, &context)) {
            *reduce_plan = false;
            break;
        }

        if (context.aggs) {
            ListCell* lcell = NULL;
            /*
             * if the target list havingQual is an Aggref, then the context should
             * have only one Aggref in the list and no VARs.
             */
            Assert(!IsA(expr, Aggref) ||
                   (list_length(context.aggs) == 1 && linitial(context.aggs) == expr && !context.vars));
            /* copy the aggregates into the remote target list */
            foreach (lcell, context.aggs) {
                Assert(IsA(lfirst(lcell), Aggref) || IsA(lfirst(lcell), GroupingFunc));
                remote_tlist = add_agg_node_to_tlist(remote_tlist, (Node*)lfirst(lcell), 0);
            }
            /* copy the vars into the remote target list */
            foreach (lcell, context.vars) {
                Assert(IsA(lfirst(lcell), Var));
                remote_tlist = add_agg_node_to_tlist(remote_tlist, (Node*)lfirst(lcell), 0);
            }
        }
        *local_qual = lappend(*local_qual, expr);

        foreign_qual_context_free(&context);
    }

    if (!(*reduce_plan))
        list_free_deep(qual);

    return remote_tlist;
}

RowToVec* make_rowtovec(Plan* lefttree)
{
    RowToVec* node = makeNode(RowToVec);
    Plan* plan = &node->plan;

    Assert(PointerIsValid(lefttree));
    inherit_plan_locator_info(plan, lefttree);
    copy_plan_costsize(plan, lefttree);

    /*
     * For plausibility, make startup & total costs equal total cost of input
     * plan; this only affects EXPLAIN display not decisions.
     */
    plan->startup_cost = plan->total_cost;
    plan->targetlist = lefttree->targetlist;
    plan->qual = NIL;
    plan->lefttree = lefttree;
    plan->righttree = NULL;
    plan->extParam = bms_copy(lefttree->extParam);
    plan->allParam = bms_copy(lefttree->allParam);
    plan->vec_output = true;

    return node;
}

VecToRow* make_vectorow(Plan* lefttree)
{
    VecToRow* node = makeNode(VecToRow);
    Plan* plan = &node->plan;

    inherit_plan_locator_info(plan, lefttree);

    copy_plan_costsize(plan, lefttree);

    /*
     * For plausibility, make startup & total costs equal total cost of input
     * plan; this only affects EXPLAIN display not decisions.
     */
    plan->startup_cost = plan->total_cost;
    plan->targetlist = lefttree->targetlist;
    plan->qual = NIL;
    plan->lefttree = lefttree;
    plan->righttree = NULL;
    plan->extParam = bms_copy(lefttree->extParam);
    plan->allParam = bms_copy(lefttree->allParam);
    plan->vec_output = false;
    plan->dop = lefttree->dop;

    return node;
}

/*
 * make_stream_plan
 *     add a stream node
 *
 * @param (in) root:
 *     PlannerInfo root
 * @param (in) lefttree:
 *     the sub-node to add stream
 * @param (in) redistribute_keys:
 *     the redistribute keys
 * @param (in) multiple:
 *     the multiple
 * @param (in) target_distribution:
 *     the target distribution (node group) to shuffle to
 *
 * @return:
 *     the stream node within sub-node
 */
Plan* make_stream_plan(
    PlannerInfo* root, Plan* lefttree, List* redistribute_keys, double multiple, Distribution* target_distribution)
{
    Stream* stream = makeNode(Stream);
    Plan* plan = &stream->scan.plan;


#ifndef ENABLE_MULTIPLE_NODES
    if (lefttree->dop <= 1) {
        /* while lefttree's dop is 1, no need to add stream node in single node */
        return lefttree;
    }
#endif

    /* For some un-important operators, we do computing in there original node group */
    if (target_distribution == NULL) {
        target_distribution = ng_get_dest_distribution(lefttree);
    }

    /* If there's distribute key, we generate a redistribute stream, or a broadcast stream */
    if (redistribute_keys != NIL) {
        stream->type = STREAM_REDISTRIBUTE;
        stream->consumer_nodes = ng_convert_to_exec_nodes(target_distribution, LOCATOR_TYPE_HASH, RELATION_ACCESS_READ);
    } else {
        stream->type = STREAM_BROADCAST;
        stream->consumer_nodes =
            ng_convert_to_exec_nodes(target_distribution, LOCATOR_TYPE_REPLICATED, RELATION_ACCESS_READ);
    }

    stream->distribute_keys = redistribute_keys;
    stream->is_sorted = false;
    stream->sort = NULL;

    if (is_replicated_plan(lefttree)) {
        ExecNodes* exec_nodes = get_random_data_nodes(LOCATOR_TYPE_REPLICATED, lefttree);
        pushdown_execnodes(lefttree, exec_nodes);
    }

    if (lefttree->dop > 1) {
        if (redistribute_keys != NIL) {
#ifdef ENABLE_MULTIPLE_NODES
            stream->smpDesc.distriType = REMOTE_SPLIT_DISTRIBUTE;
#else
            stream->smpDesc.distriType = LOCAL_DISTRIBUTE;
#endif
            stream->smpDesc.consumerDop = lefttree->dop;
            stream->smpDesc.producerDop = lefttree->dop;
            plan->dop = stream->smpDesc.consumerDop;
        } else {
#ifdef ENABLE_MULTIPLE_NODES
            stream->smpDesc.distriType = REMOTE_BROADCAST;
#else
            stream->smpDesc.distriType = LOCAL_BROADCAST;
#endif
            stream->smpDesc.consumerDop = 1;
            stream->smpDesc.producerDop = lefttree->dop;
            plan->dop = stream->smpDesc.consumerDop;
        }
    } else {
        /* In case we try to add stream above a local gather. */
        if (IsA(lefttree, Stream) && ((Stream*)lefttree)->smpDesc.distriType == LOCAL_ROUNDROBIN) {
            stream->smpDesc.producerDop = ((Stream*)lefttree)->smpDesc.producerDop;
            lefttree = lefttree->lefttree;
        } else
            stream->smpDesc.producerDop = 1;

        stream->smpDesc.consumerDop = 1;
#ifdef ENABLE_MULTIPLE_NODES
        stream->smpDesc.distriType = REMOTE_DISTRIBUTE;
#else
        stream->smpDesc.distriType = LOCAL_DISTRIBUTE;
#endif
        plan->dop = stream->smpDesc.consumerDop;
    }

    /*
     * To avoid the same stream node be added twice in the plan only executes on one datanode,
     * since we have added it in redistributeInfo routine.
     */
    if (IsA(lefttree, Stream) && !STREAM_IS_LOCAL_NODE(((Stream*)lefttree)->smpDesc.distriType) &&
        ((Stream*)lefttree)->type == STREAM_REDISTRIBUTE && stream->type == STREAM_REDISTRIBUTE) {
        stream->smpDesc.producerDop = ((Stream*)lefttree)->smpDesc.producerDop;
        stream->smpDesc.consumerDop = ((Stream*)lefttree)->smpDesc.consumerDop;
        lefttree = lefttree->lefttree;
    }

    copy_plan_costsize(plan, lefttree);
    plan->distributed_keys = stream->distribute_keys;
    plan->targetlist = lefttree->targetlist;
    plan->lefttree = lefttree;
    plan->righttree = NULL;
    plan->exec_nodes = ng_get_dest_execnodes(lefttree);
    plan->multiple = multiple;

    /* Confirm if the distribute keys in targetlist. */
    stream->distribute_keys = confirm_distribute_key(root, plan, stream->distribute_keys);

    /*
     * For multi-node group scenario,
     * stream cost is related to the num of data nodes of producer and consumer
     */
    unsigned int producer_num_dn = list_length(stream->scan.plan.exec_nodes->nodeList);
    unsigned int consumer_num_dn = list_length(stream->consumer_nodes->nodeList);
    compute_stream_cost(stream->type,
        lefttree->exec_nodes ? lefttree->exec_nodes->baselocatortype : LOCATOR_TYPE_REPLICATED,
        PLAN_LOCAL_ROWS(lefttree),
        lefttree->plan_rows,
        multiple,
        lefttree->plan_width,
        false,
        redistribute_keys,
        &plan->total_cost,
        &plan->plan_rows,
        producer_num_dn,
        consumer_num_dn);
    return plan;
}

Plan* make_redistribute_for_agg(PlannerInfo* root, Plan* lefttree, List* redistribute_keys, double multiple,
    Distribution* distribution, bool is_local_redistribute)
{
    Stream* stream = NULL;
    Plan* plan = NULL;

    /* For some agg operations (such as grouping sets), we do computing in their original node group */
    if (distribution == NULL) {
        distribution = ng_get_dest_distribution(lefttree);
    }

    stream = makeNode(Stream);
    Assert(redistribute_keys != NIL); /* redistribute key can't be null for redistribution */

    stream->type = STREAM_REDISTRIBUTE;
    stream->distribute_keys = redistribute_keys;
    stream->consumer_nodes = ng_convert_to_exec_nodes(distribution, LOCATOR_TYPE_HASH, RELATION_ACCESS_READ);

    stream->is_sorted = false;
    stream->sort = NULL;

    plan = &stream->scan.plan;
    plan->distributed_keys = stream->distribute_keys;
    plan->targetlist = lefttree->targetlist;
    plan->lefttree = lefttree;
    plan->righttree = NULL;
    plan->exec_nodes = ng_get_dest_execnodes(lefttree);
    copy_plan_costsize(plan, lefttree);
    plan->multiple = multiple;

    if (lefttree->dop > 1) {
        stream->smpDesc.producerDop = lefttree->dop > 1 ? lefttree->dop : 1;
        stream->smpDesc.consumerDop = u_sess->opt_cxt.query_dop;
#ifdef ENABLE_MULTIPLE_NODES
        stream->smpDesc.distriType = is_local_redistribute ? LOCAL_DISTRIBUTE : REMOTE_SPLIT_DISTRIBUTE;
#else
        stream->smpDesc.distriType = LOCAL_DISTRIBUTE;
#endif
        plan->dop = SET_DOP(lefttree->dop);
    } else {
        stream->smpDesc.producerDop = 1;
        stream->smpDesc.consumerDop = 1;
#ifdef ENABLE_MULTIPLE_NODES
        stream->smpDesc.distriType = REMOTE_DISTRIBUTE;
#else
        stream->smpDesc.distriType = LOCAL_DISTRIBUTE;
#endif
        plan->dop = 1;
    }

    /* Confirm if the distribute keys in targetlist. */
    stream->distribute_keys = confirm_distribute_key(root, plan, stream->distribute_keys);

    /*
     * For multi-node group scenario,
     * stream cost is related to the num of data nodes of producer and consumer
     */
    unsigned int producer_num_dn = list_length(stream->scan.plan.exec_nodes->nodeList);
    unsigned int consumer_num_dn = list_length(stream->consumer_nodes->nodeList);
    compute_stream_cost(stream->type,
        lefttree->exec_nodes ? lefttree->exec_nodes->baselocatortype : LOCATOR_TYPE_REPLICATED,
        PLAN_LOCAL_ROWS(lefttree),
        lefttree->plan_rows,
        multiple,
        lefttree->plan_width,
        false,
        stream->distribute_keys,
        &plan->total_cost,
        &plan->plan_rows,
        producer_num_dn,
        consumer_num_dn);
    return plan;
}

/*
 * @Description: Supported operator in expression is one of "+", "-". The query will
 *                be optimized using soft constraint.
 * @in expr: Operator expr.
 * @return: Return true if legality, else return false.
 */
static bool isHoldUniqueOperator(OpExpr* expr)
{
    Oid opno = expr->opno;

    if (opno >= FirstNormalObjectId) {
        return false;
    }

    bool result = true;
    HeapTuple tp;
    tp = SearchSysCache1(OPEROID, ObjectIdGetDatum(opno));
    if (HeapTupleIsValid(tp)) {
        Form_pg_operator optup = (Form_pg_operator)GETSTRUCT(tp);

        char operName = '\0';

        if (1 == strlen(NameStr(optup->oprname))) {
            operName = optup->oprname.data[0];
            if (operName != '+' && operName != '-') {
                result = false;
            }
        } else {
            result = false;
        }
    } else {
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                (errmsg("Operator with OID %u dose not exist.", opno))));
    }

    ReleaseSysCache(tp);

    return result;
}

/*
 * @Description: Judge the expression have whether uniqueness or not.
 * @in node: The expression judged.
 * @return: Return true, if the expression has uniqueness, else return false.
 */
static bool isHoldUniqueness(Node* node)
{
    bool result = true;

    if (IsA(node, OpExpr)) {
        OpExpr* expr = (OpExpr*)node;

        /* We only support two args's opexpr. */
        if (2 != list_length(expr->args)) {
            return false;
        }

        Node* larg = (Node*)linitial(expr->args);
        Node* rarg = (Node*)lsecond(expr->args);

        if (!isHoldUniqueOperator(expr)) {
            result = false;
        } else if (!isHoldUniqueness(larg)) {
            result = false;
        } else if (!isHoldUniqueness(rarg)) {
            result = false;
        }
    } else if (IsA(node, Var)) {
        result = true;
    } else if (IsA(node, Const) && !((Const*)node)->constisnull) {
        result = true;
    } else {
        result = false;
    }

    return result;
}

/*
 * @Description: Check this node if is operate expr; its args num if is two if is equal expr.
 * @in node: Expr node.
 * @in return: True or false.
 */
bool isEqualExpr(Node* node)
{
    OpExpr* opexpr = NULL;
    Oid leftArgType = InvalidOid;

    if (!IsA(node, OpExpr)) {
        return false;
    }

    opexpr = (OpExpr*)node;
    if (2 != list_length(opexpr->args)) {
        return false;
    }

    leftArgType = exprType((Node*)linitial(opexpr->args));
    /* Only support "=" operator. */
    if (!op_hashjoinable(opexpr->opno, leftArgType) && !op_mergejoinable(opexpr->opno, leftArgType)) {
        return false;
    }

    return true;
}

/*
 * @Description: Decide whether exists unique qual in qualClause list.
 * @in qualClause: The list to be checked.
 * @in parse: The Query struct after parsing for SQL.
 * @in relids: Relids of inner relation, if is join plan else it is null.
 * @return: If use informational constraint, return true otherwise return false.
 */
bool useInformationalConstraint(PlannerInfo* root, List* qualClause, Relids relids)
{
    if (qualClause == NIL) {
        return false;
    }

    bool result = false;

    ListCell* cell = NULL;
    foreach (cell, qualClause) {
        OpExpr* opexpr = NULL;
        Var* checked_var = NULL;
        Node* checked_arg = NULL;

        /* Just now, only equal clause is supported for informational constaint. */
        if (!isEqualExpr((Node*)lfirst(cell))) {
            continue;
        }

        opexpr = (OpExpr*)lfirst(cell);

        Node* larg = (Node*)linitial(opexpr->args);
        Node* rarg = (Node*)lsecond(opexpr->args);

        List* lvars = pull_var_clause(larg, PVC_RECURSE_AGGREGATES, PVC_RECURSE_PLACEHOLDERS);
        List* rvars = pull_var_clause((Node*)rarg, PVC_RECURSE_AGGREGATES, PVC_RECURSE_PLACEHOLDERS);

        /* Scan optimize. */
        if (relids == NULL) {
            /*
             * Scan can be optimized, if opExpr only include one Var, and tihs Var is unique,
             * and can not change unique to this var's operation.
             */
            if (1 == list_length(rvars) && 0 == list_length(lvars)) {
                checked_var = (Var*)linitial(rvars);
                checked_arg = rarg;
            } else if (1 == list_length(lvars) && 0 == list_length(rvars)) {
                checked_var = (Var*)linitial(lvars);
                checked_arg = larg;
            }
        } else { /* Join optimize. */
            Relids rrelids = pull_varnos(rarg);
            Relids lrelids = pull_varnos(larg);

            /* This var come from right tree. And larg can not inlcude right-tree's var. */
            if (1 == list_length(rvars) && bms_equal(rrelids, relids) && !bms_overlap(lrelids, relids)) {
                checked_var = (Var*)linitial(rvars);
                checked_arg = rarg;
            } else if (1 == list_length(lvars) && bms_equal(lrelids, relids) && !bms_overlap(rrelids, relids)) {
                /* This var come from right tree. And rarg can not inlcude right-tree's var. */
                checked_var = (Var*)linitial(lvars);
                checked_arg = larg;
            }
        }

        if (checked_var != NULL) {
            RangeTblEntry* rtable = planner_rt_fetch(checked_var->varno, root);

            if (RTE_RELATION == rtable->rtekind && findConstraintByVar(checked_var, rtable->relid, UNIQUE_CONSTRAINT) &&
                isHoldUniqueness(checked_arg)) {
                result = true;
            }
        }

        /*
         * If we find one suitable expression, the entire expressions will be to
         * benefit from informational constraint.
         */
        if (result) {
            break;
        }
    }

    return result;
}

#endif

/*
 * For multi-count-distinct case, we form a join between every single count-distinct subquery
 * with equality of group by clause. In such case, we should allow null equality for these group
 * by clauses. This function is used to do the join qual conversion.
 *
 * Equal qual can exist in both "joinqual" and "otherqual", and we only care the columns that are
 * nullable, which already stores in "nullinfo".
 */
static List* make_null_eq_clause(List* joinqual, List** otherqual, List* nullinfo)
{
    ListCell* lc = NULL;
    List* joinclause = NIL;
    List* otherclause = NIL;

    foreach (lc, joinqual) {
        joinclause = lappend(joinclause, get_null_eq_restrictinfo((Node*)lfirst(lc), nullinfo));
    }

    if (*otherqual != NIL) {
        foreach (lc, *otherqual) {
            otherclause = lappend(otherclause, get_null_eq_restrictinfo((Node*)lfirst(lc), nullinfo));
        }
        list_free_ext(*otherqual);
        *otherqual = otherclause;
    }

    return joinclause;
}

/*
 * For generated join equality condition Var(1,1)=Var(2,1), convert it to "Var(1,1) is not distinct
 * from Var(2,1)" (null equal allowed), or leave it unchanged.
 */
static Node* get_null_eq_restrictinfo(Node* restrictinfo, List* nullinfo)
{
    OpExpr* op = (OpExpr*)restrictinfo;
    Var* var1 = NULL;
    Var* var2 = NULL;

    if (!IsA(restrictinfo, OpExpr) || list_length(op->args) != 2)
        return restrictinfo;

    var1 = (Var*)linitial(op->args);
    var2 = (Var*)lsecond(op->args);

    if (var1->varattno == var2->varattno && list_member_int(nullinfo, var1->varattno))
        return (Node*)make_notclause(make_distinct_op(NULL, list_make1(makeString("=")), (Node*)var1, (Node*)var2, -1));

    return restrictinfo;
}

/*
 * @Description: check if the expr is included in subplan's targetlist.
 * @in node: the expr to be check.
 * @in targetlist: the target list of sunplan.
 * @return int: location of the node in targetlist.
 */
int find_node_in_targetlist(Node* node, List* targetlist)
{
    ListCell* lc = NULL;
    TargetEntry* te = NULL;
    Node* node_new = NULL;
    int count = 0;
    int loc = -1;

    if (IsA(node, TargetEntry)) {
        node = (Node*)((TargetEntry*)node)->expr;
    }

    foreach (lc, targetlist) {
        if (IsA(lfirst(lc), TargetEntry)) {
            te = (TargetEntry*)lfirst(lc);
            node_new = (Node*)te->expr;
        } else {
            node_new = (Node*)lfirst(lc);
        }

        if (equal(node_new, node)) {
            loc = count;
            break;
        }

        count++;
    }

    return loc;
}

/*
 * @Description: Search equal class, and check if it is
 *				included in subplan's targetlist.
 * @in root: the planner info for this plan.
 * @in expr: the expr to be check.
 * @in targetlist: the targetlist to search.
 * @return Expr*: .
 */
Node* find_qualify_equal_class(PlannerInfo* root, Node* expr, List* targetlist)
{
    ListCell* lc = NULL;
    Node* new_expr = NULL;

    foreach (lc, targetlist) {
        Node* te = (Node*)lfirst(lc);

        if (IsA(te, TargetEntry))
            new_expr = (Node*)((TargetEntry*)te)->expr;
        else
            new_expr = te;

        /* Check if we can use the target list. */
        if (IsA(expr, Var)) {
            Var* new_var = locate_distribute_var((Expr*)new_expr);
            if (new_var != NULL && _equalSimpleVar(new_var, expr))
                break;
        }

        if (judge_node_compatible(root, expr, new_expr))
            break;
    }
    if (NULL != lc)
        return new_expr;
    else
        return NULL;
}

/*
 * @Description: Add a new expr to targetlist.
 * @in root: the planner info for this plan.
 * @in plan: subplan.
 * @in node: the expr to be check.
 * @return Expr*: .
 */
void add_key_column_for_plan(PlannerInfo* root, Plan* plan, Expr* node)
{
    TargetEntry* newentry = NULL;
    Plan* subplan = plan->lefttree;

    if (subplan == NULL)
        return;

    /*
     * If the top subplan node can't do projections, we need to add a Result
     * node to help it along.
     */
    if (!is_projection_capable_plan(subplan) ||
        (is_vector_scan(subplan) && vector_engine_unsupport_expression_walker((Node*)node))) {
        subplan = (Plan*)make_result(root, (List*)copyObject(subplan->targetlist), NULL, subplan);
        plan->targetlist = subplan->targetlist;
        plan->lefttree = subplan;
        plan->exec_nodes = subplan->exec_nodes;
    }
    newentry = makeTargetEntry(node, list_length(plan->targetlist) + 1, NULL, true);
    plan->targetlist = lappend(plan->targetlist, newentry);
    /* plan's targetlist is the same as subplan's targetlist, but if both art NIL, we shoule handle it */
    if (subplan->targetlist == NIL) {
        subplan->targetlist = plan->targetlist;
    }
    return;
}

/*
 * @Description: Check if distribute keys are included in subplan's targetlist,
 *				 if not we try to find a equal class to replace this node.
 *				 If no equal class is qualified, add this node to subplan's targetlist.
 * @in root - Per-query information for planning/optimization.
 * @in plan - Subplan.
 * @in distribute_keys - Stream distribute keys.
 * @return List*: the new distribute kyes.
 *
 */
List* confirm_distribute_key(PlannerInfo* root, Plan* plan, List* distribute_keys)
{
    if (distribute_keys == NIL)
        return NIL;

    List* dis_keys = list_copy(distribute_keys);
    ListCell* cell = NULL;
    Node* dkey = NULL;
    Node* new_exp = NULL;
    bool has_changed = false;

    foreach (cell, dis_keys) {
        dkey = (Node*)lfirst(cell);

        /* If can find it in targetlist, process it. */
        if (find_node_in_targetlist(dkey, plan->targetlist) < 0) {
            /* 1. Try to find a qualify equal class. */
            new_exp = find_qualify_equal_class(root, dkey, plan->targetlist);
            if (new_exp != NULL) {
                lfirst(cell) = (void*)new_exp;
                has_changed = true;
            } else {
                /* 2. Add this node to subplan's targetlist. */
                List* src_dkey_varlist = NIL;
                List* new_dkey_varlist = NIL;

                /* 2.1 search into expression to find each var(s) */
                ListCell* lc_var = NULL;
                List* var_list =
                    pull_var_clause(dkey, PVC_RECURSE_AGGREGATES, PVC_INCLUDE_PLACEHOLDERS, PVC_RECURSE_SPECIAL_EXPR);
                foreach (lc_var, var_list) {
                    Node* dkey_var = (Node*)lfirst(lc_var);
                    if (!find_node_in_targetlist(dkey_var, plan->targetlist)) {
                        Node* new_dkey_var = find_qualify_equal_class(root, dkey_var, plan->targetlist);
                        if (new_dkey_var != NULL) {
                            src_dkey_varlist = lappend(src_dkey_varlist, dkey_var);
                            new_dkey_varlist = lappend(new_dkey_varlist, new_dkey_var);
                        } else {
                            /* No referent found for Var */
                            ereport(ERROR,
                                (errmodule(MOD_OPT),
                                    errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                                    (errmsg("variable not found in subplan target lists"))));
                        }
                    }
                }
                /* Release var_list */
                list_free_ext(var_list);

                /* 2.2 replace vars in dkey if necessary */
                if (src_dkey_varlist != NIL && new_dkey_varlist != NIL) {
                    new_exp = replace_node_clause(
                        dkey, (Node*)src_dkey_varlist, (Node*)new_dkey_varlist, RNC_COPY_NON_LEAF_NODES);

                    /* also replace the distribute key */
                    lfirst(cell) = (void*)new_exp;
                    has_changed = true;
                } else
                    new_exp = dkey;

                /* 2.3 Add this node to subplan's targetlist. */
                add_key_column_for_plan(root, plan, (Expr*)new_exp);
            }
        }
    }

    if (has_changed)
        return dis_keys;
    else {
        list_free_ext(dis_keys);
        return distribute_keys;
    }
}

/*
 * @Description: Check if the distribute key can be found in targetlist.
 * @in root - Per-query information for planning/optimization.
 * @in distribute_keys - stream distribute keys.
 * @in targetlist - subplan targetlist.
 * @return bool: true -- dsitribute keys can be found in targetlist.
 *
 */
bool check_dsitribute_key_in_targetlist(PlannerInfo* root, List* distribute_keys, List* targetlist)
{
    ListCell* cell = NULL;
    Node* node = NULL;

    foreach (cell, distribute_keys) {
        node = (Node*)lfirst(cell);
        if (find_node_in_targetlist(node, targetlist) < 0 && NULL == find_qualify_equal_class(root, node, targetlist))
            return false;
    }
    return true;
}

/*
 * get_plan_actual_total_width
 *	In PG optimizer, only width of row engine is estimated, and it has
 *	big difference with vector engine, so this function is used to estimate
 *	width of a plan with vector engine though row width
 *
 * Parameters:
 *	@in plan: the plan that should estimate width
 *	@in vectorized: if the path will be vectorized
 *	@in type: the type to calculate the width, only considerring hashjoin,
 *			hashagg, sort and material
 *	@in newcol: for some case like add distribute column, and vectorized
 *			abbreviate sort, new columns will be added, so should record
 *			the number of new col to impact the width
 *
 * Returns: estimated width with row-engine and vector-engine
 */
int get_plan_actual_total_width(Plan* plan, bool vectorized, OpType type, int newcol)
{
    int width = 0;

    if (vectorized) {
        ListCell* lc = NULL;
        int fixed_width = 0;
        int encoded_num = 0;

        /* For vectorized plan, we roughly estimate the width of length-varying column */
        foreach (lc, plan->targetlist) {
            TargetEntry* tle = (TargetEntry*)lfirst(lc);
            if (COL_IS_ENCODE(exprType((Node*)tle->expr)))
                encoded_num++;
            else
                fixed_width += get_typavgwidth(exprType((Node*)tle->expr), exprTypmod((Node*)tle->expr));
        }

        /* Only consider sort and hashagg for plan width estimation */
        switch (type) {
            case OP_SORT: {
                if (encoded_num > 0)
                    width += encoded_num * alloc_trunk_size(Max(0, (plan->plan_width - fixed_width) / encoded_num));
                int tmp_encodednum = ((encoded_num > 0) ? ((int)1) : ((int)0));
                width += sizeof(Datum) * (list_length(plan->targetlist) + tmp_encodednum);
                break;
            }
            case OP_HASHAGG: {
                width += Max(0, plan->plan_width - fixed_width) + TUPLE_OVERHEAD(true) + sizeof(void*) * 2 +
                         SIZE_COL_VALUE * (list_length(plan->targetlist) + newcol);
                break;
            }
            default:
                break;
        }
    } else {
        width = plan->plan_width;
    }

    return width;
}

/*
 * check_subplan_in_qual
 *     Check if there are vars in qual's subplan that doesn't exist in targetlist,
 *     if so, we can't do two-level agg since it'll lead to "var not found in targetlist"
 *     error. (Note. if there's subplan in qual, the same subplan should be found
 *     in targetlist, so there maybe no var in targetlist. However, due to PG
 *     framework, the two subplans will be treated as two different ones, so
 *     we can't find the same subplan in targetlist, but only the vars in subplan)
 *
 * Parameters:
 *     @in tlist: targetlist of lefttree
 *     @in qual: qual of subplan, which is used to check subplan expr
 *
 * Returns: true if there's subplan in quals, and vars in subplan are not found in targetlist
 */
bool check_subplan_in_qual(List* tlist, List* qual)
{
    List* subplan_expr = check_subplan_expr((Node*)qual);
    if (subplan_expr == NIL)
        return false;

    List* vars = pull_var_clause((Node*)qual, PVC_INCLUDE_AGGREGATES, PVC_INCLUDE_PLACEHOLDERS);
    ListCell* lc = NULL;
    foreach (lc, vars) {
        Expr* var = (Expr*)lfirst(lc);
        if (!IsA(var, Var))
            continue;
        if (find_var_from_targetlist(var, tlist))
            continue;
        /* find var not exists in targetlist, just break */
        break;
    }

    list_free_ext(subplan_expr);
    list_free_ext(vars);
    return (lc != NULL);
}

/*
 * check_subplan_exec_datanode
 *    We don't support query shippable when then main plan exec on CN and
 *    the subplan exec on DN(see finalize_node_id). So we should avoid
 *    gen such plan.
 *
 *    Consider only the subplan, regardless of initplan. In finalize_node_id we
 *    will add remote_query upon initplan if needed.
 *
 *    During generating agg plan, check if the qual of agg contains subplan exec
 *    on DN, if any, we will never gen DN_AGG_CN_AGG and
 *    DN_REDISTRIBUTE_AGG_CN_AGGplan.
 *
 * Parameters:
 *     @in node: the node will be checked.
 *
 * Returns: true if there's subplan exec on DN
 */
bool check_subplan_exec_datanode(PlannerInfo* root, Node* node)
{
    ListCell* lc = NULL;
    List* subplan_list = check_subplan_expr(node);

    foreach (lc, subplan_list) {
        Node* subnode = (Node*)lfirst(lc);
        if (IsA(subnode, SubPlan)) {
            SubPlan* subplan = (SubPlan*)lfirst(lc);

            Plan* plan = (Plan*)list_nth(root->glob->subplans, subplan->plan_id - 1);

            if (is_execute_on_datanodes(plan)) {
                return true;
            }
        }
    }

    return false;
}

/*
 * @Description: check whether allow lock table rows in stream plan.
 * @in root: planner info of the query.
 * @in lefttree: the plan we need check for add make lockrows node.
 * @return : PlanRowMark struct of the query.
 */
static PlanRowMark* check_lockrows_permission(PlannerInfo* root, Plan* lefttree)
{
    ListCell* lc = NULL;
    bool has_lock = false;
    PlanRowMark* rowmark = NULL;

    /* Don't support lock in non-top-level query. */
    if (root->query_level > 1) {
        ereport(ERROR,
            (errmodule(MOD_OPT),
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Unsupported FOR UPDATE/SHARE at non-top-level query in stream plan."))));
    }

    /*
     * Don't support lock with limit clause in stream plan as lock&limit will be done
     * on all datanodes which means the really locked rows is (dn_num * limit_value)
     * but not the expecting limit_value rows.
     */
    if (root->parse->limitCount != NULL || root->parse->limitOffset != NULL) {
        ereport(ERROR,
            (errmodule(MOD_OPT),
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Unsupported FOR UPDATE/SHARE with limit in stream plan."))));
    }

    /* Don't support lock more than one table once. */
    foreach (lc, root->rowMarks) {
        PlanRowMark* rc = (PlanRowMark*)lfirst(lc);

        if (rc->markType == ROW_MARK_EXCLUSIVE || rc->markType == ROW_MARK_SHARE) {
            if (has_lock)
                ereport(ERROR,
                    (errmodule(MOD_OPT),
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("Unsupported FOR UPDATE/SHARE multiple table in stream plan."))));

            has_lock = true;
            rowmark = rc;
        }
    }

    /*
     * We should have check non-row table in transformLockingClause but can't
     * process some scenes like view of foreign table.
     *
     * The rowmark will be null when we lock target which is not RTE_RELATION.
     * for more detail, see preprocess_rowmarks.
     */
    if (rowmark == NULL)
        ereport(
            ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Unsupported FOR UPDATE/SHARE of non-row table.")));

    return rowmark;
}
