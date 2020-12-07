/* -------------------------------------------------------------------------
 *
 * streamplan_single.cpp
 *      functions related to stream plan.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/gausskernel/optimizer/plan/streamplan_single.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <math.h>
#include <pthread.h>
#include "access/heapam.h"
#include "access/transam.h"
#include "access/hash.h"
#include "catalog/pg_class.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_inherits_fn.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_trigger.h"
#include "catalog/pg_type.h"
#include "catalog/pgxc_class.h"
#include "catalog/pgxc_node.h"
#include "commands/explain.h"
#include "commands/tablecmds.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "funcapi.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/print.h"
#include "optimizer/cost.h"
#include "optimizer/clauses.h"
#include "optimizer/dataskew.h"
#include "optimizer/nodegroups.h"
#include "optimizer/pathnode.h"
#include "optimizer/pgxcship.h"
#include "optimizer/planmain.h"
#include "optimizer/planner.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/streamplan.h"
#include "optimizer/tlist.h"
#include "parser/parse_collate.h"
#include "parser/parse_coerce.h"
#include "parser/parse_clause.h"
#include "parser/parse_merge.h"
#include "parser/parse_node.h"
#include "parser/parse_oper.h"
#include "parser/parse_relation.h"
#include "parser/parsetree.h"
#include "pgxc/groupmgr.h"
#include "pgxc/locator.h"
#include "pgxc/nodemgr.h"
#include "pgxc/pgxc.h"
#include "pgxc/pgxcnode.h"
#include "pgxc/poolmgr.h"
#include "pgxc/poolutils.h"
#include "postgres.h"
#include "knl/knl_variable.h"
#include "rewrite/rewriteHandler.h"
#include "nodes/pg_list.h"
#include "securec.h"
#include "lz4.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/syscache.h"

static bool contains_specified_func_walker(Node* node, void* context);
static bool is_type_cast_hash_compatible(FuncExpr* func);
static List* check_op_list_template(Plan* result_plan, List* (*check_eval)(Node*));
static List* check_subplan_expr_default(Node* node);
static List* check_func(Node* node);
static ExecNodes* get_plan_max_ExecNodes(Plan* lefttree, List* subplans);
static bool is_execute_on_multinodes(Plan* plan);
static List* get_max_nodeList(List** nodeList, Plan* lefttree);
static void set_bucketmap_index(Plan* plan, NodeGroupInfoContext* node_group_info_context);
static void set_bucketmap_index(ExecNodes* exec_node, NodeGroupInfoContext* node_group_info_context);
static Oid get_hash_type(Oid type_in);

static bool remove_local_plan(Plan* stream_plan, Plan* parent, ListCell* lc, bool is_left)
{
    Stream* stream = NULL;

    if (stream_plan == NULL || parent == NULL)
        return false;

    /* Check the plan type. */
    if (IsA(stream_plan, Stream) || IsA(stream_plan, VecStream))
        stream = (Stream*)stream_plan;
    else
        return false;

    /* Delete useless local stream node. */
    if (STREAM_IS_LOCAL_NODE(stream->smpDesc.distriType) && stream->smpDesc.consumerDop == 1 &&
        stream->smpDesc.producerDop == 1) {
        if (lc != NULL) {
            lfirst(lc) = (void*)stream_plan->lefttree;
        } else if (is_left) {
            parent->lefttree = stream_plan->lefttree;
        } else if (parent->righttree == stream_plan) {
            parent->righttree = stream_plan->lefttree;
        }
        return true;
    } else if (stream->smpDesc.distriType == REMOTE_SPLIT_DISTRIBUTE && stream->smpDesc.consumerDop == 1) {
        /* Set remote stream type. */
        stream->smpDesc.distriType = REMOTE_DISTRIBUTE;
    } else if (stream->smpDesc.distriType == REMOTE_SPLIT_BROADCAST && stream->smpDesc.consumerDop == 1) {
        stream->smpDesc.distriType = REMOTE_BROADCAST;
    }
    return false;
}

uint32 generate_unique_id(IdGen* gen)
{
    /*
     * gen is always a pointer to a global process-level variable. So it is NOT a NULL.
     * Here Assert is to prevent gen not initializing.
     */
    AssertEreport(gen != NULL, MOD_OPT, "The foreign server is NULL");

    uint32 id;
    uint32 seed;
    uint32 timeLineId;

    if (IS_PGXC_DATANODE && !IS_SINGLE_NODE) {
        return 0;
    }

    if (unlikely(!gen->initialized)) {
        gen->nodeid = PGXCNodeGetNodeIdFromName(g_instance.attr.attr_common.PGXCNodeName, PGXC_NODE_COORDINATOR) + 1;
        gen->initialized = true;
    }

    /* add time line id, steal two bit to avoid plan id duplicate due to cn restart */
    id = gen->nodeid;
    seed = pg_atomic_add_fetch_u32(&gen->seed, 1);
    timeLineId = get_controlfile_timeline();
    timeLineId = timeLineId & 0x03;
    id = (id << 24) | (timeLineId << 22) | ((seed) & 0x03fffff);
    return id;
}

// if delete, will core under gsql -d potgres, used in InitPlan
uint64 generate_unique_id64(Id64Gen* gen)
{
    /*
     * gen is always a pointer to a global process-level vaviable. So it is NOT a NULL.
     * Here Assert is to prevent gen not inializing.
     */
    AssertEreport(gen != NULL, MOD_OPT, "The foreign server is NULL");

    uint64 id;
    uint64 seed;
    uint32 timeLineId;

    if (IS_PGXC_DATANODE && !IS_SINGLE_NODE) {
        return 0;
    }

    if (unlikely(!gen->initialized)) {
        gen->nodeid = PGXCNodeGetNodeIdFromName(g_instance.attr.attr_common.PGXCNodeName, PGXC_NODE_COORDINATOR) + 1;
        gen->initialized = true;
    }
    /* Use atomic operator instead of lock. */
    seed = pg_atomic_fetch_add_u64(&gen->seed, 1);
    /* add time line id to avoid queryid duplicate due to cn restart */
    timeLineId = get_controlfile_timeline();

    /*
     * | ----8 bits---- | ----8 bits---- | ------------------------48 bits------------------------ |
     *     cn nodeid      cn restart id       query sequence number (increase 1 for a new query)
     */
    id = ((uint64)(int64)gen->nodeid << 56) | ((uint64)(timeLineId & 0xff) << 48) | (seed & 0xffffffffffff);

    return id;
}

void set_default_stream()
{
    if (IS_PGXC_COORDINATOR) {
        u_sess->opt_cxt.is_stream = u_sess->attr.attr_sql.enable_stream_operator;
    } else {
        AssertEreport(IS_PGXC_DATANODE, MOD_OPT, "It is NOT a pgxc_datanode");
        u_sess->opt_cxt.is_stream = false;
    }

    u_sess->opt_cxt.is_stream_support = true;
}

void set_stream_off()
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}
// only used in pgxc_planner, cannot enter this function under single node
bool stream_walker(Node* node, void* context)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return true;
}

// used in heap.cpp
List* contains_specified_func(Node* node, contain_func_context* context)
{
    /* context->funcids is the func will be checked, it can't be NIL */
    AssertEreport(context->funcids != NIL, MOD_OPT, "The context funcids is NIL");

    (void)query_or_expression_tree_walker(node, (bool (*)())contains_specified_func_walker, (void*)context, 0);
    return context->func_exprs;
}

static bool contains_specified_func_walker(Node* node, void* context)
{
    if (node == NULL) {
        return false;
    }

    contain_func_context* ctxt = (contain_func_context*)context;
    if (IsA(node, FuncExpr)) {
        FuncExpr* func = (FuncExpr*)node;
        if (list_member_oid(ctxt->funcids, func->funcid)) {
            ctxt->func_exprs = lappend(ctxt->func_exprs, func);
            if (!ctxt->find_all) {
                return true;
            }
        }
    }
    if (IsA(node, Query)) {
        /* Recurse into subselects */
        bool result = query_tree_walker((Query*)node, (bool (*)())contains_specified_func_walker, context, 0);
        return result;
    }
    return expression_tree_walker(node, (bool (*)())contains_specified_func_walker, context);
}

// used in heap.cpp
/* Init the contain_func_context */
contain_func_context init_contain_func_context(List* funcids, bool find_all)
{
    contain_func_context context;
    context.funcids = funcids;
    context.func_exprs = NIL;
    context.find_all = find_all;

    return context;
}

int2vector* get_baserel_distributekey_no(Oid relid)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}

// in build_simple_rel used
// Put it all back. Record it
List* build_baserel_distributekey(RangeTblEntry* rte, int relindex)
{
    if (IS_PGXC_DATANODE || !IS_PGXC_COORDINATOR || !rte->relid || get_rel_relkind(rte->relid) != RELKIND_RELATION)
        return NIL;

    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NIL;
}

// inner_locator_type = '\0' under single node
char locator_type_join(char inner_locator_type, char outer_locator_type)
{
    return '\0';
}
Plan* make_simple_RemoteQuery(Plan* lefttree, PlannerInfo* root, bool is_subplan, ExecNodes* target_exec_nodes)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}
void add_remote_subplan(PlannerInfo* root, RemoteQuery* result_node)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

ExecNodes* get_random_data_nodes(char locatortype, Plan* plan)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}

void inherit_plan_locator_info(Plan* plan, Plan* subplan)
{
    plan->exec_nodes = ng_get_dest_execnodes(subplan);
    plan->exec_type = subplan->exec_type;
    plan->distributed_keys = subplan->distributed_keys;
    plan->multiple = subplan->multiple;

    /* Copy the dop from lefttree plan. */
    plan->dop = SET_DOP(subplan->dop);
}

void inherit_path_locator_info(Path* path, Path* subpath)
{
    Distribution* distribution = ng_get_dest_distribution(subpath);
    ng_set_distribution(&path->distribution, distribution);
    path->distribute_keys = subpath->distribute_keys;
    path->locator_type = subpath->locator_type;
}

static ExecNodes* get_plan_max_ExecNodes(Plan* lefttree, List* subplans)
{
    ExecNodes* final_exec_nodes = NULL;
    List* nodeList = NIL;
    ListCell* lc = NULL;
    final_exec_nodes = makeNode(ExecNodes);
    final_exec_nodes->nodeList = NIL;
    final_exec_nodes->baselocatortype = LOCATOR_TYPE_REPLICATED;
    final_exec_nodes->accesstype = RELATION_ACCESS_READ;
    final_exec_nodes->primarynodelist = NIL;
    final_exec_nodes->en_expr = NULL;
    nodeList = get_max_nodeList(&nodeList, lefttree);
    foreach (lc, subplans) {
        Plan* plan = (Plan*)lfirst(lc);
        nodeList = get_max_nodeList(&nodeList, plan);
    }
    final_exec_nodes->nodeList = nodeList;
    Distribution* distribution = ng_convert_to_distribution(final_exec_nodes->nodeList);
    ng_set_distribution(&final_exec_nodes->distribution, distribution);
    return final_exec_nodes;
}
List* get_max_nodeList(List** nodeList, Plan* result_plan)
{
    if (list_length(*nodeList) >= u_sess->pgxc_cxt.NumDataNodes) {
        return *nodeList;
    }
    if (result_plan != NULL) {
        ListCell* lc = NULL;
        if (result_plan->exec_nodes) {
            *nodeList = list_union_int(*nodeList, result_plan->exec_nodes->nodeList);
        }
        if (list_length(*nodeList) >= u_sess->pgxc_cxt.NumDataNodes) {
            return *nodeList;
        }
        switch (nodeTag(result_plan)) {
            case T_Append:
            case T_VecAppend: {
                Append* append = (Append*)result_plan;
                foreach (lc, append->appendplans) {
                    Plan* plan = (Plan*)lfirst(lc);
                    *nodeList = get_max_nodeList(nodeList, plan);
                }
            } break;
            case T_ModifyTable:
            case T_VecModifyTable: {
                ModifyTable* mt = (ModifyTable*)result_plan;
                foreach (lc, mt->plans) {
                    Plan* plan = (Plan*)lfirst(lc);
                    *nodeList = get_max_nodeList(nodeList, plan);
                }
            } break;
            case T_SubqueryScan:
            case T_VecSubqueryScan: {
                SubqueryScan* ss = (SubqueryScan*)result_plan;
                if (ss->subplan)
                    *nodeList = get_max_nodeList(nodeList, ss->subplan);
            } break;
            case T_MergeAppend:
            case T_VecMergeAppend: {
                MergeAppend* ma = (MergeAppend*)result_plan;
                foreach (lc, ma->mergeplans) {
                    Plan* plan = (Plan*)lfirst(lc);
                    *nodeList = get_max_nodeList(nodeList, plan);
                }
            } break;
            case T_Stream:
            case T_VecStream: {
                Stream* stream = (Stream*)result_plan;
                *nodeList = list_union_int(*nodeList, stream->consumer_nodes->nodeList);
                if (stream->scan.plan.exec_nodes) {
                    *nodeList = list_union_int(*nodeList, stream->scan.plan.exec_nodes->nodeList);
                    *nodeList = get_max_nodeList(nodeList, result_plan->lefttree);
                }
            } break;
            case T_BitmapAnd: {
                BitmapAnd* ba = (BitmapAnd*)result_plan;
                foreach (lc, ba->bitmapplans) {
                    Plan* plan = (Plan*)lfirst(lc);
                    *nodeList = get_max_nodeList(nodeList, plan);
                }
            } break;
            case T_BitmapOr: {
                BitmapOr* bo = (BitmapOr*)result_plan;
                foreach (lc, bo->bitmapplans) {
                    Plan* plan = (Plan*)lfirst(lc);
                    *nodeList = get_max_nodeList(nodeList, plan);
                }
            } break;
            default: {
                if (result_plan->lefttree)
                    *nodeList = get_max_nodeList(nodeList, result_plan->lefttree);
                if (result_plan->righttree)
                    *nodeList = get_max_nodeList(nodeList, result_plan->righttree);
            } break;
        }
    }
    return *nodeList;
}
static bool is_execute_on_multinodes(Plan* plan)
{
    return (plan->exec_nodes && list_length(plan->exec_nodes->nodeList) != 1);
}
List* check_random_expr(Plan* result_plan)
{
    List* random_list = check_op_list_template(result_plan, (List * (*)(Node*)) check_random_expr);
    return random_list;
}
static void set_bucketmap_index(Plan* plan, NodeGroupInfoContext* node_group_info_context)
{
    if (IsA(plan, Stream) || IsA(plan, VecStream)) {
        Stream* stream_plan = (Stream*)plan;
        set_bucketmap_index(stream_plan->scan.plan.exec_nodes, node_group_info_context);
        set_bucketmap_index(stream_plan->consumer_nodes, node_group_info_context);
    } else if (IsA(plan, RemoteQuery)) {
        RemoteQuery* remote_query_plan = (RemoteQuery*)plan;
        set_bucketmap_index(remote_query_plan->scan.plan.exec_nodes, node_group_info_context);
        set_bucketmap_index(remote_query_plan->exec_nodes, node_group_info_context);
    } else {
        set_bucketmap_index(plan->exec_nodes, node_group_info_context);
    }
}
static void set_bucketmap_index(ExecNodes* exec_node, NodeGroupInfoContext* node_group_info_context)
{
    if (exec_node == NULL)
        return;
    exec_node->bucketmapIdx = BUCKETMAP_DEFAULT_INDEX;
    Oid groupoid = exec_node->distribution.group_oid;
    if (groupoid == InvalidOid)
        return;
    for (int i = 0; i < node_group_info_context->num_bucketmaps; i++) {
        if (node_group_info_context->groupOids[i] == groupoid) {
            exec_node->bucketmapIdx = i;
            return;
        }
    }
    char* group_name = get_pgxc_groupname(groupoid);
    if (group_name == NULL)
        return;
    char* installation_group_name = pstrdup(PgxcGroupGetInstallationGroup());
    int current_num_bucketmaps = node_group_info_context->num_bucketmaps;
    if (strncmp(group_name, installation_group_name, NAMEDATALEN) == 0) {
        node_group_info_context->groupOids[current_num_bucketmaps] = groupoid;
        node_group_info_context->bucketMap[current_num_bucketmaps] = BucketMapCacheGetBucketmap(group_name);
        exec_node->bucketmapIdx = node_group_info_context->num_bucketmaps;
        node_group_info_context->num_bucketmaps++;
    } else {
        char in_redistribution = get_pgxc_group_redistributionstatus(groupoid);
        if ('y' == in_redistribution) {
            node_group_info_context->groupOids[current_num_bucketmaps] = groupoid;
            node_group_info_context->bucketMap[current_num_bucketmaps] = BucketMapCacheGetBucketmap(group_name);
            exec_node->bucketmapIdx = node_group_info_context->num_bucketmaps;
            node_group_info_context->num_bucketmaps++;
        }
    }
    AssertEreport(node_group_info_context->num_bucketmaps <= MAX_SPECIAL_BUCKETMAP_NUM,
        MOD_OPT,
        "The number of bucketmaps exceeded the max special bucketmap num");
    pfree_ext(group_name);
    pfree_ext(installation_group_name);
}
void finalize_node_id(Plan* result_plan, int* plan_node_id, int* parent_node_id, int* num_streams, int* num_plannodes,
    int* total_num_streams, int* max_push_sql_num, int* gather_count, List* subplans, List* subroots, List** initplans,
    int* subplan_ids, bool is_under_stream, bool is_under_ctescan, bool is_data_node_exec, bool is_read_only,
    NodeGroupInfoContext* node_group_info_context)
{
    errno_t sprintf_rc = 0;
    if (result_plan != NULL) {
        result_plan->plan_node_id = *plan_node_id;
        result_plan->parent_node_id = *parent_node_id;
        if (is_execute_on_datanodes(result_plan)) {
            is_data_node_exec = true;
            set_bucketmap_index(result_plan, node_group_info_context);
        }
        if (is_under_stream)
            subplan_ids[0] = *plan_node_id;
        *parent_node_id = *plan_node_id;
        int save_parent_id = *parent_node_id;
        (*plan_node_id)++;
        (*num_plannodes)++;
        if (result_plan->initPlan && (IS_SINGLE_NODE || IS_STREAM_PLAN)) {
            List* cteLinkList = NIL;
            ListCell* lc = NULL;
            foreach (lc, result_plan->initPlan) {
                SubPlan* plan = (SubPlan*)lfirst(lc);
                if (plan->subLinkType == CTE_SUBLINK)
                    cteLinkList = lappend(cteLinkList, plan);
            }
#ifdef ENABLE_MULTIPLE_NODES
            if (cteLinkList != NIL) {
                if (IsA(result_plan, ValuesScan)) {
                    sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
                        NOTPLANSHIPPING_LENGTH,
                        "ValuesScan in CTE SubLink can't be shipped");
                    securec_check_ss_c(sprintf_rc, "\0", "\0");
                    mark_stream_unsupport();
                }
            }
#endif
            if (IS_SINGLE_NODE || IS_STREAM_PLAN)
                *initplans = list_concat(*initplans, list_copy(result_plan->initPlan));
        }
        switch (nodeTag(result_plan)) {
            case T_Append:
            case T_VecAppend: {
                Append* append = (Append*)result_plan;
                ListCell* lc = NULL;
                foreach (lc, append->appendplans) {
                    Plan* plan = (Plan*)lfirst(lc);
                    finalize_node_id(plan,
                        plan_node_id,
                        parent_node_id,
                        num_streams,
                        num_plannodes,
                        total_num_streams,
                        max_push_sql_num,
                        gather_count,
                        subplans,
                        subroots,
                        initplans,
                        subplan_ids,
                        false,
                        is_under_ctescan,
                        is_data_node_exec,
                        is_read_only,
                        node_group_info_context);
                    *parent_node_id = save_parent_id;
                }
            } break;
            case T_CteScan: {
                if (STREAM_RECURSIVECTE_SUPPORTED) {
                    CteScan* cte_plan = (CteScan*)result_plan;
                    int subplanid = cte_plan->ctePlanId;
                    if (is_under_ctescan) {
                        sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
                            NOTPLANSHIPPING_LENGTH,
                            "With-Recursive CteScan runs as subplan nested in another CteScan");
                        securec_check_ss_c(sprintf_rc, "\0", "\0");
                        mark_stream_unsupport();
}

                    RecursiveUnion* ru_plan = (RecursiveUnion*)list_nth(subplans, cte_plan->ctePlanId - 1);
                    if (IsA(ru_plan, RecursiveUnion)) {
                        finalize_node_id((Plan*)ru_plan,
                            plan_node_id,
                            parent_node_id,
                            num_streams,
                            num_plannodes,
                            total_num_streams,
                            max_push_sql_num,
                            gather_count,
                            subplans,
                            subroots,
                            initplans,
                            subplan_ids,
                            false,
                            is_under_ctescan,
                            is_data_node_exec,
                            is_read_only,
                            node_group_info_context);
                    } else
                        break;
                    subplan_ids[subplanid] = subplan_ids[0];
                    *parent_node_id = save_parent_id;
                }
                if (IsExplainPlanStmt || IS_SINGLE_NODE) {
                    CteScan* cte_plan = (CteScan*)result_plan;
                    Plan* ru_plan = (Plan*)list_nth(subplans, cte_plan->ctePlanId - 1);
                    if (!IsA(ru_plan, RecursiveUnion))
                        finalize_node_id((Plan*)ru_plan,
                            plan_node_id,
                            parent_node_id,
                            num_streams,
                            num_plannodes,
                            total_num_streams,
                            max_push_sql_num,
                            gather_count,
                            subplans,
                            subroots,
                            initplans,
                            subplan_ids,
                            false,
                            is_under_ctescan,
                            is_data_node_exec,
                            is_read_only,
                            node_group_info_context);
                    *parent_node_id = save_parent_id;
                }
            } break;
            case T_ModifyTable:
            case T_VecModifyTable: {
                ModifyTable* mt = (ModifyTable*)result_plan;
                ListCell* lc = NULL;
                foreach (lc, mt->plans) {
                    Plan* plan = (Plan*)lfirst(lc);
                    finalize_node_id(plan,
                        plan_node_id,
                        parent_node_id,
                        num_streams,
                        num_plannodes,
                        total_num_streams,
                        max_push_sql_num,
                        gather_count,
                        subplans,
                        subroots,
                        initplans,
                        subplan_ids,
                        false,
                        is_under_ctescan,
                        is_data_node_exec,
                        is_read_only,
                        node_group_info_context);
                    *parent_node_id = save_parent_id;
                }
            } break;
            case T_SubqueryScan:
            case T_VecSubqueryScan: {
                SubqueryScan* ss = (SubqueryScan*)result_plan;
                if (ss->subplan) {
                    if (IsExplainPlanStmt && trivial_subqueryscan(ss)) {
                        (*plan_node_id)--;
                        (*num_plannodes)--;
                        finalize_node_id(ss->subplan,
                            plan_node_id,
                            parent_node_id,
                            num_streams,
                            num_plannodes,
                            total_num_streams,
                            max_push_sql_num,
                            gather_count,
                            subplans,
                            subroots,
                            initplans,
                            subplan_ids,
                            false,
                            is_under_ctescan,
                            is_data_node_exec,
                            is_read_only,
                            node_group_info_context);
                    } else
                        finalize_node_id(ss->subplan,
                            plan_node_id,
                            parent_node_id,
                            num_streams,
                            num_plannodes,
                            total_num_streams,
                            max_push_sql_num,
                            gather_count,
                            subplans,
                            subroots,
                            initplans,
                            subplan_ids,
                            false,
                            is_under_ctescan,
                            is_data_node_exec,
                            is_read_only,
                            node_group_info_context);
                    *parent_node_id = save_parent_id;
                }
            } break;
            case T_MergeAppend:
            case T_VecMergeAppend: {
                MergeAppend* ma = (MergeAppend*)result_plan;
                ListCell* lc = NULL;
                foreach (lc, ma->mergeplans) {
                    Plan* plan = (Plan*)lfirst(lc);
                    finalize_node_id(plan,
                        plan_node_id,
                        parent_node_id,
                        num_streams,
                        num_plannodes,
                        total_num_streams,
                        max_push_sql_num,
                        gather_count,
                        subplans,
                        subroots,
                        initplans,
                        subplan_ids,
                        false,
                        is_under_ctescan,
                        is_data_node_exec,
                        is_read_only,
                        node_group_info_context);
                    *parent_node_id = save_parent_id;
                }
            } break;
            case T_RemoteQuery:
            case T_VecRemoteQuery: {
                RemoteQuery* rq = (RemoteQuery*)result_plan;
                int saved_plan_id = subplan_ids[0];
                (*gather_count)++;
                rq->read_only = is_read_only;
                if (GATHER == rq->position) {
                    (*num_streams) = 0;
                }
                if (result_plan->lefttree) {
                    finalize_node_id(result_plan->lefttree,
                        plan_node_id,
                        parent_node_id,
                        num_streams,
                        num_plannodes,
                        total_num_streams,
                        max_push_sql_num,
                        gather_count,
                        subplans,
                        subroots,
                        initplans,
                        subplan_ids,
                        true,
                        is_under_ctescan,
                        is_data_node_exec,
                        is_read_only,
                        node_group_info_context);
                    subplan_ids[0] = saved_plan_id;
                    *parent_node_id = save_parent_id;
                }
                if (result_plan->righttree) {
                    finalize_node_id(result_plan->righttree,
                        plan_node_id,
                        parent_node_id,
                        num_streams,
                        num_plannodes,
                        total_num_streams,
                        max_push_sql_num,
                        gather_count,
                        subplans,
                        subroots,
                        initplans,
                        subplan_ids,
                        true,
                        is_under_ctescan,
                        is_data_node_exec,
                        is_read_only,
                        node_group_info_context);
                    subplan_ids[0] = saved_plan_id;
                    *parent_node_id = save_parent_id;
                }
                if (GATHER == rq->position) {
                    rq->num_stream = *num_streams;
                    if (IS_STREAM_PLAN && subplans != NIL)
                        rq->exec_nodes = get_plan_max_ExecNodes(result_plan->lefttree, subplans);
                    (*num_streams) = 0;
                }
                if (!rq->is_simple)
                    (*max_push_sql_num)++;
                rq->num_gather = *gather_count;
            } break;
            case T_Stream:
            case T_VecStream: {
                Stream* stream = (Stream*)result_plan;
                int saved_plan_id = subplan_ids[0];
                (*num_streams)++;
                (*total_num_streams)++;
                finalize_node_id(result_plan->lefttree,
                    plan_node_id,
                    parent_node_id,
                    num_streams,
                    num_plannodes,
                    total_num_streams,
                    max_push_sql_num,
                    gather_count,
                    subplans,
                    subroots,
                    initplans,
                    subplan_ids,
                    true,
                    is_under_ctescan,
                    is_data_node_exec,
                    is_read_only,
                    node_group_info_context);
                *parent_node_id = save_parent_id;
                subplan_ids[0] = saved_plan_id;
                if (is_gather_stream(stream))
                    stream->is_dummy = false;
            } break;
            case T_BitmapAnd:
            case T_CStoreIndexAnd: {
                BitmapAnd* ba = (BitmapAnd*)result_plan;
                ListCell* lc = NULL;
                foreach (lc, ba->bitmapplans) {
                    Plan* plan = (Plan*)lfirst(lc);
                    finalize_node_id(plan,
                        plan_node_id,
                        parent_node_id,
                        num_streams,
                        num_plannodes,
                        total_num_streams,
                        max_push_sql_num,
                        gather_count,
                        subplans,
                        subroots,
                        initplans,
                        subplan_ids,
                        false,
                        is_under_ctescan,
                        is_data_node_exec,
                        is_read_only,
                        node_group_info_context);
                    *parent_node_id = save_parent_id;
                }
            } break;
            case T_BitmapOr:
            case T_CStoreIndexOr: {
                BitmapOr* bo = (BitmapOr*)result_plan;
                ListCell* lc = NULL;
                foreach (lc, bo->bitmapplans) {
                    Plan* plan = (Plan*)lfirst(lc);
                    finalize_node_id(plan,
                        plan_node_id,
                        parent_node_id,
                        num_streams,
                        num_plannodes,
                        total_num_streams,
                        max_push_sql_num,
                        gather_count,
                        subplans,
                        subroots,
                        initplans,
                        subplan_ids,
                        false,
                        is_under_ctescan,
                        is_data_node_exec,
                        is_read_only,
                        node_group_info_context);
                    *parent_node_id = save_parent_id;
                }
            } break;
            default:
                if (result_plan->lefttree) {
                    finalize_node_id(result_plan->lefttree,
                        plan_node_id,
                        parent_node_id,
                        num_streams,
                        num_plannodes,
                        total_num_streams,
                        max_push_sql_num,
                        gather_count,
                        subplans,
                        subroots,
                        initplans,
                        subplan_ids,
                        false,
                        is_under_ctescan,
                        is_data_node_exec,
                        is_read_only,
                        node_group_info_context);
                    *parent_node_id = save_parent_id;
                }
                if (result_plan->righttree) {
                    finalize_node_id(result_plan->righttree,
                        plan_node_id,
                        parent_node_id,
                        num_streams,
                        num_plannodes,
                        total_num_streams,
                        max_push_sql_num,
                        gather_count,
                        subplans,
                        subroots,
                        initplans,
                        subplan_ids,
                        false,
                        is_under_ctescan,
                        is_data_node_exec,
                        is_read_only,
                        node_group_info_context);
                    *parent_node_id = save_parent_id;
                }
                break;
        }
        if (IS_STREAM_PLAN || IS_SINGLE_NODE) {
            if (is_replicated_plan(result_plan) && is_execute_on_multinodes(result_plan)) {
                List* nodelist = check_random_expr(result_plan);
                if (list_length(nodelist) > 0) {
                    sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
                        NOTPLANSHIPPING_LENGTH,
                        "Function %s() can not be shipped",
                        get_func_name(((FuncExpr*)linitial(nodelist))->funcid));
                    securec_check_ss_c(sprintf_rc, "\0", "\0");
                    list_free_ext(nodelist);
                    mark_stream_unsupport();
                }
            }
            List* subplan_list = check_subplan_list(result_plan); /* find all subplan exprs from main plan */
            ListCell* lc = NULL;
            foreach (lc, subplan_list) {
                Node* node = (Node*)lfirst(lc);
                Plan* plan = NULL;
                SubPlan* subplan = NULL;
                bool has_finalized = false;
                if (IsA(node, SubPlan)) {
                    subplan = (SubPlan*)lfirst(lc);
                    subplan_list = list_concat(subplan_list, check_subplan_expr(subplan->testexpr));
                } else {
                    AssertEreport(IsA(node, Param), MOD_OPT, "The node is NOT a param");
                    Param* param = (Param*)node;
                    ListCell* lc2 = NULL;
                    foreach (lc2, *initplans) {
                        subplan = (SubPlan*)lfirst(lc2);
                        if (list_member_int(subplan->setParam, param->paramid))
                            break;
                    }
                    if (subplan == NULL || lc2 == NULL)
                        continue;
                }
                plan = (Plan*)list_nth(subplans, subplan->plan_id - 1);
                if (subplan_ids[subplan->plan_id] != 0) {
                    if (subplan_ids[subplan->plan_id] != subplan_ids[0]) {
                        sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
                            NOTPLANSHIPPING_LENGTH,
                            "SubPlan shared on multi-thread can't be shipped");
                        securec_check_ss_c(sprintf_rc, "\0", "\0");
                        mark_stream_unsupport();
                    } else
                        has_finalized = true;
                } else
                    subplan_ids[subplan->plan_id] = subplan_ids[0];
                if (!has_finalized) {
#ifdef ENABLE_MULTIPLE_NODES
                    if (is_execute_on_coordinator(result_plan) ||
                        (is_execute_on_allnodes(result_plan) && !is_data_node_exec)) {
                        Plan* child_plan = NULL;
                        ListCell* lc2 = NULL;
                        int i = subplan->plan_id - 1;
                        PlannerInfo* subroot = (PlannerInfo*)list_nth(subroots, i);
                        if (is_execute_on_datanodes(plan)) {
                            if (IsA(node, Param)) {
                                if (IsA(plan, Stream))
                                    child_plan = outerPlan(plan);
                                else
                                    child_plan = plan;
                                plan = make_simple_RemoteQuery(child_plan, subroot, false);
                                foreach (lc2, subplans) {
                                    if (i == 0) {
                                        lfirst(lc2) = plan;
                                        break;
                                    }
                                    i--;
                                }
                            } else {
                                subplan_ids[subplan->plan_id] = 0;
                                sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
                                    NOTPLANSHIPPING_LENGTH,
                                    "main plan exec on CN and SubPlan exec on DN can't be shipped");
                                securec_check_ss_c(sprintf_rc, "\0", "\0");
                                mark_stream_unsupport();
                            }
                        }
                    } else if (is_execute_on_coordinator(plan)) {
                        subplan_ids[subplan->plan_id] = 0;
                        sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
                            NOTPLANSHIPPING_LENGTH,
                            "SubPlan exec on CN can't be shipped");
                        securec_check_ss_c(sprintf_rc, "\0", "\0");
                        mark_stream_unsupport();
                    }
                    else if (!is_replicated_plan(result_plan) ||
                             (result_plan->exec_nodes && result_plan->exec_nodes->nodeList != NIL)) {
                        if (subplan->args == NIL && (!IsA(node, SubPlan)) && is_execute_on_datanodes(plan) &&
                            !contain_special_plan_node(plan, T_Stream, CPLN_ONE_WAY)) {
                            if (result_plan->exec_nodes && plan->exec_nodes && plan->exec_nodes->nodeList != NIL &&
                                list_difference_int(result_plan->exec_nodes->nodeList, plan->exec_nodes->nodeList)) {
                                Distribution* result_distribution = ng_get_dest_distribution(result_plan);
                                plan = make_stream_plan(NULL, plan, NIL, 0.0, result_distribution);
                            }
                        }
                        pushdown_execnodes(plan, result_plan->exec_nodes, false, true);
                    }
#endif
                    if (check_stream_support()) {
                        PlannerInfo* subroot = NULL;
                        Plan* child_root = NULL;
                        ListCell* lr = NULL;
                        int j = subplan->plan_id - 1;
                        subroot = (PlannerInfo*)list_nth(subroots, j);
                        plan = try_vectorize_plan(
                            plan, subroot->parse, IsA(node, SubPlan) || subroot->is_correlated, subroot);
                        child_root = set_plan_references(subroot, plan);
                        foreach (lr, subplans) {
                            if (j == 0) {
                                lfirst(lr) = child_root;
                                break;
                            }
                            j--;
                        }
                        plan = child_root;
                        if (isIntergratedMachine && !IsA(node, SubPlan)) {
                            if (u_sess->opt_cxt.query_dop > 1) {
                                List* subPlanList = NIL;
                                (void)has_subplan(plan, result_plan, NULL, true, &subPlanList, true);
                            }
                            confirm_parallel_info(plan, result_plan->dop);
                        }
                    }
                    if (IsA(result_plan, CteScan)) {
                        is_under_ctescan = true;
                    }
                    finalize_node_id(plan,
                        plan_node_id,
                        parent_node_id,
                        num_streams,
                        num_plannodes,
                        total_num_streams,
                        max_push_sql_num,
                        gather_count,
                        subplans,
                        subroots,
                        initplans,
                        subplan_ids,
                        false,
                        is_under_ctescan,
                        is_data_node_exec,
                        is_read_only,
                        node_group_info_context);
                    *parent_node_id = save_parent_id;
                }
            }
            list_free_ext(subplan_list);
        }
    }
}
Path* create_stream_path(PlannerInfo* root, RelOptInfo* rel, StreamType type, List* distribute_keys, List* pathkeys,
    Path* subpath, double skew, Distribution* target_distribution, ParallelDesc* smp_desc, List* ssinfo)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}

bool is_execute_on_coordinator(Plan* plan)
{
    return plan->exec_type == EXEC_ON_COORDS;
}

bool is_execute_on_datanodes(Plan* plan)
{
    return plan->exec_type == EXEC_ON_DATANODES;
}

bool is_execute_on_allnodes(Plan* plan)
{
    return plan->exec_type == EXEC_ON_ALL_NODES;
}

bool is_replicated_plan(Plan* plan)
{
#ifndef ENABLE_MULTIPLE_NODES
    return false;
#else
    if (IsA(plan, Stream)) {
        if (is_broadcast_stream((Stream*)plan) || is_gather_stream((Stream*)plan)) {
            return true;
        } else if (is_redistribute_stream((Stream*)plan)) {
            if (((Stream*)plan)->smpDesc.distriType == LOCAL_BROADCAST) {
                return is_replicated_plan(plan->lefttree);
            } else {
                return false;
            }     
        }
    } else if (plan->exec_nodes) {
        return plan->exec_nodes->baselocatortype == LOCATOR_TYPE_REPLICATED;
    } else if (plan->exec_type == EXEC_ON_ALL_NODES) { 
        /* It is a replicated plan if its exec_type is EXEC_ON_ALL_NODES. */
        return true;
    }

    return false;
#endif
}

bool is_hashed_plan(Plan* plan)
{
#ifndef ENABLE_MULTIPLE_NODES
    return false;
#else
    if (IsA(plan, Stream)) {
        if (is_broadcast_stream((Stream*)plan) || is_gather_stream((Stream*)plan))
            return false;
        else if (is_redistribute_stream((Stream*)plan))
            return true;
    } else if (plan->exec_nodes != NULL)
        return IsLocatorDistributedByHash(plan->exec_nodes->baselocatortype);

    return false;
#endif
}

bool is_broadcast_stream(Stream* stream)
{
    return stream->type == STREAM_BROADCAST;
}

bool is_redistribute_stream(Stream* stream)
{
    return stream->type == STREAM_REDISTRIBUTE;
}

bool is_gather_stream(Stream* stream)
{
    return stream->type == STREAM_GATHER;
}

bool is_hybid_stream(Stream* stream)
{
    return stream->type == STREAM_HYBRID;
}

ExecNodes* stream_merge_exec_nodes(Plan* lefttree, Plan* righttree)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}

ExecNodes* get_all_data_nodes(char locatortype)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}

void pushdown_execnodes(Plan* plan, ExecNodes* exec_nodes, bool add_node, bool only_nodelist)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

void stream_join_plan(PlannerInfo* root, Plan* join_plan, JoinPath* join_path)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

char* CompressSerializedPlan(const char* plan_string, int* cLen)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}
char* DecompressSerializedPlan(const char* comp_plan_string, int cLen, int oLen)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}

void SerializePlan(Plan* node, PlannedStmt* planned_stmt, StringInfoData* str, int num_stream, int num_gather)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

NodeDefinition* get_all_datanodes_def()
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}

Plan* mark_distribute_dml(
    PlannerInfo* root, Plan** sourceplan, ModifyTable* mt_plan, List** resultRelations, List* mergeActionList)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}

List* distributeKeyIndex(PlannerInfo* root, List* distributed_keys, List* targetlist)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}

List* make_groupcl_for_append(PlannerInfo* root, List* targetlist)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}

void mark_distribute_setop(PlannerInfo* root, Node* node, bool isunionall, bool canDiskeyChange)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

void cost_stream(StreamPath* stream, int width, bool isJoin)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

void compute_stream_cost(StreamType type, char locator_type, double subrows, double subgblrows, double skew_ratio,
    int width, bool isJoin, List* distribute_keys, Cost* total_cost, double* gblrows, unsigned int producer_num_dn,
    unsigned int consumer_num_dn, ParallelDesc* smpDesc, List* ssinfo)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

void foreign_qual_context_init(foreign_qual_context* context)
{
    context->collect_vars = false;
    context->vars = NIL;
    context->aggs = NIL;
}

void foreign_qual_context_free(foreign_qual_context* context)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

bool is_foreign_expr(Node* node, foreign_qual_context* context)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return false;
}

char get_locator_type(Plan* plan)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return 'a';
}

bool contain_special_plan_node(Plan* plan, NodeTag planTag, ContainPlanNodeMode mode)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return false;
}

// the name is_stream_support is used in function check_stream_support, which used as condition
void mark_stream_unsupport()
{
    u_sess->opt_cxt.is_stream_support = false;
    ereport(ERROR, (errmodule(MOD_OPT), (errcode(ERRCODE_STREAM_NOT_SUPPORTED), errmsg("mark_stream_unsupport."))));
}

bool check_stream_support()
{
    return u_sess->opt_cxt.is_stream_support;
}

bool is_compatible_type(Oid type1, Oid type2)
{
    if (type1 == type2) {
        return true;
    }

    Oid hash_type1, hash_type2;
    hash_type1 = get_hash_type(type1);
    hash_type2 = get_hash_type(type2);

    /*
     * If hash types are the same, we regard types are compatible
     * BUT, in TIMEOID case, time zone may be added during data type cast,
     *     so, we regard TIMEOID types are incompatible
     */
    if (hash_type1 == hash_type2 && TIMEOID != hash_type1 && TIMEOID != hash_type2)
        return true;
    else
        return false;
}

static Oid get_hash_type(Oid type_in)
{
    switch (type_in) {
        /* Int8 hash arithmetic is compatible in int4, so we return same type.*/
        case INT8OID:
        case CASHOID:
        case INT1OID:
        case INT2OID:
        case OIDOID:
        case INT4OID:
        case BOOLOID:
        case CHAROID:
        case ABSTIMEOID:
        case RELTIMEOID:
        case DATEOID:
            return INT4OID;
        case INT2VECTOROID:
        case OIDVECTOROID:
            return OIDVECTOROID;
        case NVARCHAR2OID:
        case VARCHAROID:
        case TEXTOID:
            return TEXTOID;
        case RAWOID:
        case BYTEAOID:
            return RAWOID;
        case TIMEOID:
        case TIMESTAMPOID:
        case TIMESTAMPTZOID:
        case SMALLDATETIMEOID:
            return TIMEOID;
        case FLOAT4OID:
        case FLOAT8OID:
            return FLOAT4OID;
        case NAMEOID:
        case INTERVALOID:
        case TIMETZOID:
        case NUMERICOID:
            return type_in;
        case BPCHAROID:
#ifdef PGXC
            if (g_instance.attr.attr_sql.string_hash_compatible)
                return TEXTOID;
            else
#endif
                return type_in;
        default:
            return type_in;
    }
}

bool is_args_type_compatible(OpExpr* op_expr)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return false;
}

void materialize_remote_query(Plan* result_plan, bool* materialized, bool sort_to_store)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

/*
 * stream_path_walker:
 *	traverse path node to find if it contains stream node and parameterized path
 * Parameters:
 *	@in path: path that need to check
 *	@in context: context for checking stream node
 */
void stream_path_walker(Path* path, ContainStreamContext* context)
{
    if (path == NULL) {
        return;
    }

    /* Record if there's parameterized path */
    if (path->param_info) {
        Relids req_outer = bms_intersect(path->param_info->ppi_req_outer, context->outer_relids);
        if (!bms_is_empty(req_outer)) {
            context->has_parameterized_path = true;
            /*
             * if param path is under materialize, it definitely cross stream
             * node. Set it because it's skipped when traversing stream node
             */
            if (context->under_materialize_all)
                context->has_stream = true;
            bms_free_ext(req_outer);
        }
    }

    /* Quit earlier if check condition meets */
    if (context->has_stream && (context->only_check_stream || context->has_parameterized_path)) {
        return;
    }

    switch (path->pathtype) {
        case T_Stream: {
            /* Don't count stream node under added materialize node */
            if (!context->under_materialize_all)
                context->has_stream = true;
            stream_path_walker(((StreamPath*)path)->subpath, context);
        } break;
        /* For subqueryscan, we should traverse to its child plan */
        case T_SubqueryScan: {
            Plan* splan = path->parent->subplan;
            if (contain_special_plan_node(splan, T_Stream))
                context->has_stream = true;
            else
                context->has_stream = false;
        } break;
        case T_Append: {
            ListCell* cell = NULL;

            foreach (cell, ((AppendPath*)path)->subpaths) {
                stream_path_walker((Path*)lfirst(cell), context);
            }
        } break;

        case T_MergeAppend: {
            ListCell* cell = NULL;

            foreach (cell, ((MergeAppendPath*)path)->subpaths) {
                stream_path_walker((Path*)lfirst(cell), context);
            }
        } break;

        case T_Material: {
            /*
             * For added materialize node above stream, we can safely return if there's no
             * parameters, or we will check further if the parameter is used under materialize
             */
            if (((MaterialPath*)path)->materialize_all && context->outer_relids == NULL)
                return;
            bool saved_value = context->under_materialize_all;
            /* If stream is not materialize_all, we should count stream under it */
            if (((MaterialPath*)path)->materialize_all)
                context->under_materialize_all = true;
            stream_path_walker(((MaterialPath*)path)->subpath, context);
            context->under_materialize_all = saved_value;
        } break;

        case T_Unique: {
            stream_path_walker(((UniquePath*)path)->subpath, context);
        }

        break;

        case T_PartIterator: {
            stream_path_walker(((PartIteratorPath*)path)->subPath, context);
        } break;

        case T_NestLoop:
        case T_HashJoin:
        case T_MergeJoin: {
            JoinPath* jp = (JoinPath*)path;

            stream_path_walker(jp->innerjoinpath, context);
            stream_path_walker(jp->outerjoinpath, context);
        } break;

        case T_IndexScan:
        case T_IndexOnlyScan:
        case T_BitmapHeapScan: {
            IndexPath* indexPath = (IndexPath*)path;
            if (g_instance.attr.attr_storage.enable_delta_store &&
                (indexPath->indexinfo->relam == PSORT_AM_OID || indexPath->indexinfo->relam == CBTREE_AM_OID ||
                    indexPath->indexinfo->relam == CGIN_AM_OID))
                context->has_cstore_index_delta = true;
        } break;

        default:
            break;
    }

    return;
}

/*
 * locate_distribute_var:
 *	Find if there's compatible var with the input node
 * Parameters:
 *	@in node: input node
 * Output:
 *	return compatible var if found, or NULL
 */
Var* locate_distribute_var(Expr* node)
{
    if (IsA(node, Var)) {
        return (Var*)node;
    } else if (IsA(node, RelabelType) || IsA(node, FuncExpr)) {
        bool compatible = true;
        while (IsA(node, RelabelType) || IsA(node, FuncExpr)) {
            if (IsA(node, RelabelType)) {
                RelabelType* relabel = (RelabelType*)node;
                node = ((RelabelType*)node)->arg;
                if (!is_compatible_type(relabel->resulttype, exprType((Node*)node))) {
                    compatible = false;
                    break;
                }
            } else {
                FuncExpr* funcexpr = (FuncExpr*)node;
                if ((funcexpr->funcformat == COERCE_IMPLICIT_CAST || funcexpr->funcformat == COERCE_EXPLICIT_CAST) &&
                    1 == list_length(funcexpr->args)) {
                    node = (Expr*)linitial(funcexpr->args);

                    if (!is_type_cast_hash_compatible(funcexpr)) {
                        compatible = false;
                        break;
                    }
                } else {
                    compatible = false;
                    break;
                }
            }
        }
        if (IsA(node, Var) && compatible)
            return (Var*)node;
    }

    return NULL;
}

static bool is_type_cast_hash_compatible(FuncExpr* func)
{
    Oid arg_type = exprType((Node*)linitial(func->args));
    Oid fun_type = func->funcresulttype;

    /* type has changed and is not compatible for hashing */
    if (!is_compatible_type(arg_type, fun_type))
        return false;

    /*
     * is type casting possibly change the value?
     *
     * we consider the casting is safe if no precision is lost:
     *
     *    (1) from a small range type to a big range type, say, int1->int2->int4
     *        this is safe casting, no precision will be lost.
     *    (2) from a big range type to a small range type, say, int4->int2->int1
     *        the overflow or underflow must be checked.
     *
     * we consider the casting is not safe if the value changed:
     *
     *    (2) from a floating point value to an integer value, usually we round
     *        the value, say, float(1.1)->int(1). this is not safe
     */
    switch (func->funcid) {
        case 77:    //  chartoi4
        case 78:    //  i4tochar
        case 235:   //  i2tod
        case 236:   //  i2tof
        case 311:   //  ftod
        case 312:   //  dtof
        case 313:   //  i2toi4
        case 314:   //  i4toi2
        case 316:   //  i4tod
        case 318:   //  i4tof
        case 406:   //  name_text
        case 408:   //  name_bpchar
        case 480:   //  int84
        case 481:   //  int48
        case 482:   //  i8tod
        case 652:   //  i8tof
        case 714:   //  int82
        case 754:   //  int28
        case 860:   //  char_bpchar
        case 1287:  //  i8tooid
        case 1288:  //  oidtoi8
        case 1401:  //  name_text
        case 1740:  //  int4_numeric
        case 1742:  //  float4_numeric
        case 1743:  //  float8_numeric
        case 1781:  //  int8_numeric
        case 1782:  //  int2_numeric
        case 4065:  //  int1_varchar
        case 4066:  //  int1_nvarchar2
        case 4067:  //  int1_bpchar
        case 4068:  //  int2_bpchar
        case 4069:  //  int8_bpchar
        case 4070:  //  float4_bpchar
        case 4071:  //  float8_bpchar
        case 4072:  //  numeric_bpchar
        case 4165:  //  int1_text
        case 4166:  //  int2_text
        case 4167:  //  int4_text
        case 4168:  //  int8_text
        case 4169:  //  float4_text
        case 4170:  //  float8_text
        case 4171:  //  numeric_text
        case 4180:  //  int2_varchar
        case 4181:  //  int4_varchar
        case 4182:  //  int8_varchar
        case 4183:  //  numeric_varchar
        case 4184:  //  float4_varchar
        case 4185:  //  float8_varchar
        case 5521:  //  int1_numeric
        case 5523:  //  i1toi2
        case 5524:  //  i2toi1
        case 5525:  //  i1toi4
        case 5526:  //  i4toi1
        case 5527:  //  i1toi8
        case 5528:  //  i8toi1
        case 5529:  //  i1tof4
        case 5531:  //  i1tof8

            /* safe casting */
            return true;

        default:
            break;
    }

    /* oops, not safe */
    return false;
}

bool add_hashfilter_for_replication(PlannerInfo* root, Plan* plan, List* distribute_keys)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return false;
}

// not used
bool IsModifyTableForDfsTable(Plan* AppendNode)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return false;
}

bool has_subplan(Plan* result_plan, Plan* parent, ListCell* cell, bool is_left, List** initplans, bool is_search)
{
    bool hasSubplan_under = false;
    bool hasSubplan_this = false;
    ListCell* lc = NULL;

    if (result_plan == NULL)
        return false;

    if (is_search) {
        /* Check the subplan. */
        if (result_plan->initPlan && IS_STREAM_PLAN) {
            *initplans = list_concat(*initplans, list_copy(result_plan->initPlan));
        }

        List* subplan_list = check_subplan_list(result_plan);
        foreach (lc, subplan_list) {
            Node* node = (Node*)lfirst(lc);
            SubPlan* subplan = NULL;
            if (IsA(node, SubPlan)) {
                hasSubplan_this = true;
            } else {
                AssertEreport(IsA(node, Param), MOD_OPT, "The node is NOT a param");
                Param* param = (Param*)node;
                ListCell* lc2 = NULL;
                foreach (lc2, *initplans) {
                    subplan = (SubPlan*)lfirst(lc2);
                    if (list_member_int(subplan->setParam, param->paramid)) {
                        hasSubplan_this = true;
                    }
                }
            }
        }
    } else {
        /*
         * Do not parallelize the plan node in the same thread
         * with the plan node which has subplan.
         */
        result_plan->dop = 1;
        result_plan->parallel_enabled = false;
    }

    List* plan_list = NIL;

    /* Find plan list in special plan nodes. */
    switch (nodeTag(result_plan)) {
        case T_Append:
        case T_VecAppend: {
            Append* append = (Append*)result_plan;
            plan_list = append->appendplans;
        } break;
        case T_ModifyTable:
        case T_VecModifyTable: {
            ModifyTable* mt = (ModifyTable*)result_plan;
            plan_list = mt->plans;
        } break;
        case T_MergeAppend:
        case T_VecMergeAppend: {
            MergeAppend* ma = (MergeAppend*)result_plan;
            plan_list = ma->mergeplans;
        } break;
        case T_BitmapAnd:
        case T_CStoreIndexAnd: {
            BitmapAnd* ba = (BitmapAnd*)result_plan;
            plan_list = ba->bitmapplans;
        } break;
        case T_BitmapOr:
        case T_CStoreIndexOr: {
            BitmapOr* bo = (BitmapOr*)result_plan;
            plan_list = bo->bitmapplans;
        } break;
        default: {
            plan_list = NIL;
        } break;
    }

    if (plan_list != NIL) {
        foreach (lc, plan_list) {
            Plan* plan = (Plan*)lfirst(lc);
            hasSubplan_under |= has_subplan(plan, result_plan, lc, false, initplans, is_search);
        }
    } else {
        switch (nodeTag(result_plan)) {
            case T_SubqueryScan:
            case T_VecSubqueryScan: {
                SubqueryScan* ss = (SubqueryScan*)result_plan;
                if (ss->subplan) {
                    if (has_subplan(ss->subplan, result_plan, NULL, false, initplans, is_search))
                        hasSubplan_under = true;
                }
            } break;

            case T_Stream:
            case T_VecStream: {
                Stream* stream = (Stream*)result_plan;
                bool removed = false;
                if (is_search) {
                    hasSubplan_under =
                        has_subplan(result_plan->lefttree, result_plan, NULL, true, initplans, is_search);
                    /* Only do roll back at stream node. */
                    if (hasSubplan_under) {
                        (void)has_subplan(result_plan->lefttree, result_plan, NULL, true, initplans, false);
                        stream->smpDesc.producerDop = 1;
                    }
                    hasSubplan_under = false;
                } else {
                    stream->smpDesc.consumerDop = 1;
                }

                if (!stream->is_recursive_local) {
                    removed = remove_local_plan(result_plan, parent, cell, is_left);
                } else {
                    /* For MPP Recursive, don't remove LOCAL GATHER(1:1) upon CteScan, just keep it */
                    Assert(stream->smpDesc.distriType == LOCAL_ROUNDROBIN && IsA(result_plan->lefttree, CteScan));
                }

                /*
                 * Push down execnodes of local stream to prevent hang.
                 * If parent is remote query, its exec_nodes is all nodes, we don't need to pushdown it.
                 */
                bool underRemoteQuery = IsA(parent, RemoteQuery) || IsA(parent, VecRemoteQuery);
                if (!removed && !underRemoteQuery && STREAM_IS_LOCAL_NODE(stream->smpDesc.distriType) &&
                    list_length(result_plan->exec_nodes->nodeList) > 0) {
                    List* diff_1 = list_difference_int(result_plan->exec_nodes->nodeList, parent->exec_nodes->nodeList);
                    List* diff_2 = list_difference_int(parent->exec_nodes->nodeList, result_plan->exec_nodes->nodeList);
                    if (diff_1 != NIL || diff_2 != NIL) {
                        list_free(diff_1);
                        list_free(diff_2);
                        pushdown_execnodes(result_plan, parent->exec_nodes, true);
                    }
                }
                return false;
            } break;
            default: {
                hasSubplan_under |= has_subplan(result_plan->lefttree, result_plan, NULL, true, initplans, is_search);
                hasSubplan_under |= has_subplan(result_plan->righttree, result_plan, NULL, false, initplans, is_search);
            } break;
        }
    }

    return hasSubplan_this || hasSubplan_under;
}

/*
 * @Description: Confirm all the plan nodes in the same thread
 *		have same dop, and it should the same of stream producerDop
 *		and consumerDop.
 *
 * @param[IN] plan: the plan to confirm
 * @param[IN] dop: thread dop
 * @return:
 */
void confirm_parallel_info(Plan* plan, int dop)
{
    if (plan == NULL)
        return;

    /* Set parallel info for plan node. */
    if (plan->dop != dop)
        ereport(
            DEBUG1, (errmodule(MOD_OPT_JOIN), (errmsg("[SMP]: Mismatch smp info, old %d, new %d.", plan->dop, dop))));

    plan->dop = dop;
    plan->parallel_enabled = (dop > 1);

    List* plan_list = NIL;
    switch (nodeTag(plan)) {
        case T_Append:
        case T_VecAppend: {
            Append* append = (Append*)plan;
            plan_list = append->appendplans;
        } break;
        case T_ModifyTable:
        case T_VecModifyTable: {
            ModifyTable* mt = (ModifyTable*)plan;
            plan_list = mt->plans;
        } break;
        case T_MergeAppend:
        case T_VecMergeAppend: {
            MergeAppend* ma = (MergeAppend*)plan;
            plan_list = ma->mergeplans;
        } break;
        case T_BitmapAnd:
        case T_CStoreIndexAnd: {
            BitmapAnd* ba = (BitmapAnd*)plan;
            plan_list = ba->bitmapplans;
        } break;
        case T_BitmapOr:
        case T_CStoreIndexOr: {
            BitmapOr* bo = (BitmapOr*)plan;
            plan_list = bo->bitmapplans;
        } break;
        default: {
            plan_list = NIL;
        } break;
    }

    if (plan_list != NIL) {
        ListCell* lc = NULL;
        foreach (lc, plan_list) {
            Plan* subplan = (Plan*)lfirst(lc);
            confirm_parallel_info(subplan, dop);
        }
    } else {
        switch (nodeTag(plan)) {
            case T_SubqueryScan:
            case T_VecSubqueryScan: {
                SubqueryScan* ss = (SubqueryScan*)plan;
                if (ss->subplan) {
                    confirm_parallel_info(ss->subplan, dop);
                }
            } break;
            case T_Agg:
            case T_VecAgg: {
                /*
                 * The numGroups means the number of distinct values in one DN,
                 * we need to devide it into each threads when we parallel the Agg.
                 */
                Agg* node = (Agg*)plan;
                if (node->aggstrategy == AGG_HASHED) {
                    node->numGroups = SET_NUMGROUPS(node);
                }
                confirm_parallel_info(plan->lefttree, dop);
            } break;
            case T_SetOp:
            case T_VecSetOp: {
                SetOp* node = (SetOp*)plan;
                if (node->strategy == SETOP_HASHED) {
                    node->numGroups = SET_NUMGROUPS(node);
                }
                confirm_parallel_info(plan->lefttree, dop);
            } break;
            case T_Stream:
            case T_VecStream: {
                Stream* stream = (Stream*)plan;
                if (SET_DOP(dop) != SET_DOP(stream->smpDesc.consumerDop))
                    ereport(LOG, (errmodule(MOD_OPT), (errmsg("Mismatch parallel degree."))));
                confirm_parallel_info(plan->lefttree, SET_DOP(stream->smpDesc.producerDop));
            } break;
            default:
                if (plan->lefttree) {
                    confirm_parallel_info(plan->lefttree, dop);
                }
                if (plan->righttree) {
                    confirm_parallel_info(plan->righttree, dop);
                }

                break;
        }
    }
}

/* The function returns all the subplan exprs for the input plan */
List* check_subplan_list(Plan* result_plan)
{
    return check_op_list_template(result_plan, (List* (*)(Node*)) check_subplan_expr_default);
}

static List* check_subplan_expr_default(Node* node)
{
    return check_subplan_expr(node);
}

/*
 * The function returns all the exprs that vartype > FirstNormalObjectId
 * for the input plan.
 */
List* check_vartype_list(Plan* result_plan)
{
    return check_op_list_template(result_plan, (List* (*)(Node*)) check_vartype);
}

// not use
// bool trivial_subqueryscan(SubqueryScan* plan);
// List* check_random_expr(Plan* result_plan)
/*
 * The function returns all the exprs that func_oid > FirstNormalObjectId
 * for the input plan.
 */
List* check_func_list(Plan* result_plan)
{
    return check_op_list_template(result_plan, (List* (*)(Node*)) check_func);
}

static bool check_func_walker(Node* node, bool* found)
{
    if (node == NULL) {
        return false;
    } else if (IsA(node, FuncExpr)) {
        FuncExpr* fexpr = (FuncExpr*)node;

        if (fexpr->funcid > FirstNormalObjectId) {
            *found = true;
            return true;
        } else {
            return false;
        }
    }

    return expression_tree_walker(node, (bool (*)())check_func_walker, found);
}

/*
 * @Description: Check this node if vartype > FirstNormalObjectId.
 * @in qual: Checked node.
 */
static List* check_func(Node* node)
{
    bool found = false;
    List* rs = NIL;

    (void)check_func_walker(node, &found);

    if (found)
        rs = lappend(rs, NULL);

    return rs;
}

static List* check_op_list_template(Plan* result_plan, List* (*check_eval)(Node*))
{
    List* res_list = NIL;

    res_list = check_eval((Node*)result_plan->targetlist);
    res_list = list_concat_unique(res_list, check_eval((Node*)result_plan->qual));
    switch (nodeTag(result_plan)) {
        case T_SeqScan:
        case T_CStoreScan:
        case T_DfsScan:
            break;
        case T_ForeignScan:
        case T_VecForeignScan: {
            ForeignScan* foreignScan = (ForeignScan*)result_plan;
            ForeignTable* ftbl = NULL;
            ForeignServer* fsvr = NULL;

            ftbl = GetForeignTable(foreignScan->scan_relid);
            AssertEreport(ftbl != NULL, MOD_OPT, "The foreign table is NULL");
            fsvr = GetForeignServer(ftbl->serverid);
            AssertEreport(fsvr != NULL, MOD_OPT, "The foreign server is NULL");

            /*
             * If the predicate is pushed down, must find subplan from item->hdfsQual struct.
             */
            if (isObsOrHdfsTableFormSrvName(fsvr->servername)) {
                List* foreignPrivateList = (List*)foreignScan->fdw_private;
                DfsPrivateItem* item = (DfsPrivateItem*)((DefElem*)linitial(foreignPrivateList))->arg;
                res_list = list_concat_unique(res_list, check_eval((Node*)item->hdfsQual));
            }
            break;
        }
        case T_IndexScan: {
            IndexScan* splan = (IndexScan*)result_plan;

            res_list = list_concat_unique(res_list, check_eval((Node*)splan->indexqual));
        } break;
        case T_IndexOnlyScan: {
            IndexOnlyScan* splan = (IndexOnlyScan*)result_plan;

            res_list = list_concat_unique(res_list, check_eval((Node*)splan->indexqual));
        } break;
        case T_CStoreIndexScan: {
            CStoreIndexScan* splan = (CStoreIndexScan*)result_plan;

            res_list = list_concat_unique(res_list, check_eval((Node*)splan->indexqual));
        } break;
        case T_DfsIndexScan: {
            DfsIndexScan* splan = (DfsIndexScan*)result_plan;

            res_list = list_concat_unique(res_list, check_eval((Node*)splan->indexqual));
        } break;
        case T_BitmapIndexScan: {
            BitmapIndexScan* splan = (BitmapIndexScan*)result_plan;

            res_list = list_concat_unique(res_list, check_eval((Node*)splan->indexqual));
        } break;
        case T_CStoreIndexCtidScan: {
            CStoreIndexCtidScan* splan = (CStoreIndexCtidScan*)result_plan;

            res_list = list_concat_unique(res_list, check_eval((Node*)splan->indexqual));
        } break;
        case T_TidScan: {
            TidScan* splan = (TidScan*)result_plan;

            res_list = list_concat_unique(res_list, check_eval((Node*)splan->tidquals));
        } break;
        case T_FunctionScan: {
            FunctionScan* splan = (FunctionScan*)result_plan;

            res_list = list_concat_unique(res_list, check_eval((Node*)splan->funcexpr));
        } break;
        case T_ValuesScan: {
            ValuesScan* splan = (ValuesScan*)result_plan;

            res_list = list_concat_unique(res_list, check_eval((Node*)splan->values_lists));
        } break;
        case T_NestLoop:
        case T_VecNestLoop: {
            Join* splan = (Join*)result_plan;

            res_list = list_concat_unique(res_list, check_eval((Node*)splan->joinqual));
        } break;
        case T_MergeJoin:
        case T_VecMergeJoin: {
            MergeJoin* splan = (MergeJoin*)result_plan;

            res_list = list_concat_unique(res_list, check_eval((Node*)splan->join.joinqual));
            res_list = list_concat_unique(res_list, check_eval((Node*)splan->mergeclauses));
        } break;
        case T_HashJoin:
        case T_VecHashJoin: {
            HashJoin* splan = (HashJoin*)result_plan;

            res_list = list_concat_unique(res_list, check_eval((Node*)splan->join.joinqual));
            res_list = list_concat_unique(res_list, check_eval((Node*)splan->hashclauses));
        } break;
        case T_Limit:
        case T_VecLimit: {
            Limit* splan = (Limit*)result_plan;

            res_list = list_concat_unique(res_list, check_eval((Node*)splan->limitOffset));
            res_list = list_concat_unique(res_list, check_eval((Node*)splan->limitCount));
        } break;
        case T_VecWindowAgg:
        case T_WindowAgg: {
            WindowAgg* splan = (WindowAgg*)result_plan;

            res_list = list_concat_unique(res_list, check_eval((Node*)splan->startOffset));
            res_list = list_concat_unique(res_list, check_eval((Node*)splan->endOffset));
        } break;
        case T_BaseResult:
        case T_VecResult: {
            BaseResult* splan = (BaseResult*)result_plan;

            res_list = list_concat_unique(res_list, check_eval((Node*)splan->resconstantqual));
        } break;
        default:
            break;
    }
    return res_list;
}

Plan* add_broacast_under_local_sort(PlannerInfo* root, PlannerInfo* subroot, Plan* plan)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}

void disable_unshipped_log(Query* query, shipping_context* context)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

void output_unshipped_log()
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

/*
 * @Description:
 *    Create a parallelDesc structure.
 *
 * @param[IN] consumer_dop: consumer dop for smp stream node.
 * @param[IN] producer_dop: producer dop for smp stream node.
 * @param[IN] smp_type: parallel stream type.
 *
 * @return: ParallelDesc*
 */
ParallelDesc* create_smpDesc(int consumer_dop, int producer_dop, SmpStreamType smp_type)
{
    ParallelDesc* smpDesc = (ParallelDesc*)palloc0(sizeof(ParallelDesc));
    smpDesc->consumerDop = consumer_dop > 1 ? consumer_dop : 1;
    smpDesc->producerDop = producer_dop > 1 ? producer_dop : 1;
    smpDesc->distriType = smp_type;
    return smpDesc;
}

Plan* create_local_gather(Plan* plan)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}

Plan* create_local_redistribute(PlannerInfo* root, Plan* lefttree, List* redistribute_keys, double multiple)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}

/*
 * @Description:
 *    Check if we need to add a local redistribute above the subplan.
 *
 * @param[IN] subplan: the plan to check.
 * @return bool: true -- local redistribute is needed.
 *
 */
bool is_local_redistribute_needed(Plan* subplan)
{
    if (subplan == NULL)
        return true;

    bool ret = true;

    switch (nodeTag(subplan)) {
        case T_Stream: {
            Stream* st = (Stream*)subplan;
            if (STREAM_REDISTRIBUTE == st->type) {
                /*
                 * If already add local distribute node,
                 * then there is no need to add another.
                 */
                if (LOCAL_DISTRIBUTE == st->smpDesc.distriType || REMOTE_SPLIT_DISTRIBUTE == st->smpDesc.distriType)
                    ret = false;
            }
            break;
        }
        case T_HashJoin:
        case T_NestLoop: {
            /*
             * If any side need to add local distribute,
             * then we have to add a new one.
             */
            if (is_local_redistribute_needed(subplan->lefttree) || is_local_redistribute_needed(subplan->righttree))
                ret = true;
            else
                ret = false;
            break;
        }
        case T_SubqueryScan: {
            SubqueryScan* ss = (SubqueryScan*)subplan;
            ret = is_local_redistribute_needed(ss->subplan);
            break;
        }
        case T_Append: {
            Append* app = (Append*)subplan;
            ListCell* lc = NULL;
            ret = false;
            /*
             * If any brunch need to add local distribute,
             * then we have to add a new one.
             */
            foreach (lc, app->appendplans) {
                Plan* plan = (Plan*)lfirst(lc);
                if (is_local_redistribute_needed(plan))
                    ret = true;
            }
            break;
        }
        default: {
            if (subplan->lefttree)
                ret = is_local_redistribute_needed(subplan->lefttree);
            else
                ret = true;
            break;
        }
    }
    return ret;
}


uint2* get_bucketmap_by_execnode(ExecNodes* exec_node, PlannedStmt* plannedstmt)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}

Oid get_oridinary_or_foreign_relid(List* rtable)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return 0;
}

uint2* GetGlobalStreamBucketMap(PlannedStmt* planned_stmt)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}

int pickup_random_datanode_from_plan(Plan* plan)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return 0;
}

bool canSeparateComputeAndStorageGroupForDelete(PlannerInfo* root)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return false;
}

bool isAllParallelized(List* subplans)
{
    return false;
}

bool judge_lockrows_need_redistribute(PlannerInfo* root, Plan* subplan, Form_pgxc_class target_classForm,

    Index result_rte_idx)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return false;
}
List* getSubPlan(Plan* node, List* subplans, List* initplans)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NIL;
}

char* GetStreamTypeStrOf(StreamPath* path)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}

void SerializePlan(
    Plan* node, PlannedStmt* planned_stmt, StringInfoData* str, int num_stream, int num_gather, bool push_subplan)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}
