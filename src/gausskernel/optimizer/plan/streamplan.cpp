/* -------------------------------------------------------------------------
 *
 * streamplan.cpp
 *      functions related to stream plan.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/gausskernel/optimizer/plan/streamplan.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <math.h>
#include <pthread.h>
#include "access/transam.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "funcapi.h"
#include "nodes/makefuncs.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/tlist.h"
#include "optimizer/randomplan.h"
#include "parser/parse_hint.h"
#include "parser/parsetree.h"
#include "pgxc/groupmgr.h"
#include "pgxc/poolmgr.h"
#include "pgxc/poolutils.h"
#include "pgxc/nodemgr.h"
#include "utils/syscache.h"
#include "instruments/instr_statement.h"
#include "replication/walreceiver.h"

/* only operator with qual supporting can use hashfilter */
static int g_support_hashfilter_types[] = {
    T_SeqScan,
    T_CStoreScan,
    T_DfsScan,
#ifdef ENABLE_MULTIPLE_NODES
    T_TsStoreScan,
#endif   /* ENABLE_MULTIPLE_NODES */
    T_ForeignScan,
    T_IndexScan,
    T_IndexOnlyScan,
    T_CStoreIndexScan,
    T_DfsIndexScan,
    T_TidScan,
    T_SubqueryScan,
    T_BitmapHeapScan,
    T_CStoreIndexHeapScan,
    T_CteScan
};

/*
 * Simple Query like:
 * 		select version();
 * will not output the not shipping reasion;
 */
void disable_unshipped_log(Query* query, shipping_context* context)
{
    if (context->query_count == 1 && NO_FORM_CLAUSE(query)) {
        u_sess->opt_cxt.not_shipping_info->need_log = false;
    }

    return;
}

void output_unshipped_log()
{
    if (u_sess->opt_cxt.not_shipping_info->need_log && u_sess->attr.attr_sql.enable_unshipping_log) {
        elog(LOG, "SQL can't be shipped, reason: %s", u_sess->opt_cxt.not_shipping_info->not_shipping_reason);
    }
    return;
}

void set_stream_off()
{
    u_sess->opt_cxt.is_stream = false;
}

bool check_stream_support()
{
    return u_sess->opt_cxt.is_stream_support;
}

/* Init the contain_func_context */
contain_func_context init_contain_func_context(List* funcids, bool find_all)
{
    contain_func_context context;
    context.funcids = funcids;
    context.func_exprs = NIL;
    context.find_all = find_all;

    return context;
}

ExecNodes* get_all_data_nodes(char locatortype)
{
    ExecNodes* exec_nodes = ng_get_installation_group_exec_node();
    exec_nodes->baselocatortype = locatortype;
    return exec_nodes;
}

/*
 * Return a random index of datanode in current plan node's execution nodegroup
 */
int pickup_random_datanode_from_plan(Plan* plan)
{
    /* Randomly pickup a node from target nodegroup */

    int nodeId = 0;

    ExecNodes* target_execnodes = ng_get_dest_execnodes(plan);

    AssertEreport(NULL != target_execnodes && NIL != target_execnodes->nodeList,
        MOD_OPT,
        "The target_execnodes or the node list is NULL");

    if (NULL != target_execnodes && NIL != target_execnodes->nodeList) {
        int random = pickup_random_datanode(list_length(target_execnodes->nodeList));
        nodeId = list_nth_int(target_execnodes->nodeList, random);
    } else {
        nodeId = pickup_random_datanode(u_sess->pgxc_cxt.NumDataNodes);
    }

    return nodeId;
}

ExecNodes* get_random_data_nodes(char locatortype, Plan* plan)
{
    int nodeId = pickup_random_datanode_from_plan(plan);
    Distribution* distribution = ng_get_dest_distribution(plan);

    ExecNodes* execNodes = makeNode(ExecNodes);
    execNodes->primarynodelist = NIL;
    execNodes->nodeList = list_make1_int(nodeId);
    ng_copy_distribution(&execNodes->distribution, distribution);
    execNodes->baselocatortype = locatortype;
    execNodes->en_expr = NULL;
    execNodes->en_relid = InvalidOid;
    execNodes->accesstype = RELATION_ACCESS_READ;
    execNodes->en_dist_vars = NIL;

    if (plan->exec_nodes == NULL) {
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                errmsg("Invalid plan->exec_nodes object when get random data nodes.")));
    }

    return execNodes;
}

/* If the nodeType of subplan support hashfilter return true, otherwise return false. */
static bool is_support_hashfilter(int nodeType)
{
    for (uint i = 0; i < lengthof(g_support_hashfilter_types); i++) {
        if (g_support_hashfilter_types[i] == nodeType)
            return true;
    }

    return false;
}

bool add_hashfilter_for_replication(PlannerInfo* root, Plan* plan, List* distribute_keys)
{
    HashFilter* hashfilter = NULL;
    List* typeOidList = NIL;
    ListCell* key = NULL;
    Plan* tmpplan = NULL;

    if (NULL == plan)
        return false;

    AssertEreport(NIL != distribute_keys, MOD_OPT, "The distribute keys are NIL");

    /*
     * If plan node type is PartIterator, it should add hashfilter in the lefttree
     */
    tmpplan = plan;
    if (IsA(tmpplan, PartIterator))
        tmpplan = tmpplan->lefttree;
    if (IsA(tmpplan, DfsIndexScan))
        tmpplan = (Plan*)((DfsIndexScan*)tmpplan)->dfsScan;
    if (IsA(tmpplan, Append)) {
        ListCell* appendPlan = NULL;

        /* First check if all the children of append are satisfied */
        foreach (appendPlan, ((Append*)tmpplan)->appendplans) {
            Plan* insidePlan = (Plan*)lfirst(appendPlan);

            /* Once there is one node which doest not support hashfilter, then return false */
            if (!is_support_hashfilter(nodeTag(insidePlan)))
                return false;
        }

        /* Second add hash filter for each child node */
        foreach (appendPlan, ((Append*)tmpplan)->appendplans) {
            Plan* insidePlan = (Plan*)lfirst(appendPlan);
            (void)add_hashfilter_for_replication(root, insidePlan, distribute_keys);
        }
        return true;
    }
    if (!is_support_hashfilter(nodeTag(tmpplan)))
        return false;

    /* Add every typeOid of distribute_keys into typeOidList */
    foreach (key, distribute_keys) {
        Node* distkey = (Node*)lfirst(key);
        typeOidList = lappend_oid(typeOidList, exprType(distkey));
    }

    /* Make Hashfilter expr node */
    AssertEreport(plan->exec_nodes && plan->exec_nodes->nodeList, MOD_OPT, "The exec nodes or node list is NULL");
    List* nodeList = list_copy(plan->exec_nodes->nodeList);
    hashfilter = makeHashFilter(distribute_keys, typeOidList, nodeList);
    tmpplan->hasHashFilter = true;

    /* Append Hashfilter expr to qual of tmpplan */
    tmpplan->qual = lappend(tmpplan->qual, hashfilter);

    /* Set exec node list and Distribution */
    Distribution* distribution = ng_get_dest_distribution(tmpplan);
    tmpplan->exec_nodes = ng_convert_to_exec_nodes(distribution, LOCATOR_TYPE_HASH, RELATION_ACCESS_READ);
    tmpplan->exec_nodes->nodeList = list_copy(nodeList);

    /* estimate plan rows for hashfilter. */
    tmpplan->plan_rows = PLAN_LOCAL_ROWS(tmpplan);
    tmpplan->multiple = get_multiple_by_distkey(root, distribute_keys, tmpplan->plan_rows);

    return true;
}

void stream_join_plan(PlannerInfo* root, Plan* join_plan, JoinPath* join_path)
{
    Plan* inner_plan = innerPlan(join_plan);
    Plan* outer_plan = outerPlan(join_plan);

    if (is_execute_on_datanodes(inner_plan) || is_execute_on_datanodes(outer_plan)) {
        Plan* child_plan = NULL;

        if (is_execute_on_coordinator(inner_plan)) {
            List* outerpathkeys = NIL;

            if (IsA(outer_plan, Stream))
                child_plan = outerPlan(outer_plan);
            else
                child_plan = outer_plan;

            outer_plan = make_simple_RemoteQuery(child_plan, root, false);

            if (IsA(join_plan, MergeJoin)) {
                MergePath* merge_path = (MergePath*)join_path;

                if (merge_path->outersortkeys)
                    outerpathkeys = merge_path->outersortkeys;
                else
                    outerpathkeys = merge_path->jpath.outerjoinpath->pathkeys;

                outer_plan = (Plan*)make_sort_from_pathkeys(root, outer_plan, outerpathkeys, -1.0);
            } else if (IsA(join_plan, NestLoop) && join_path->path.pathkeys) {
                AssertEreport(join_path->outerjoinpath->pathkeys, MOD_OPT, "The outer join path keys is NULL");

                outerpathkeys = join_path->path.pathkeys;
                outer_plan = (Plan*)make_sort_from_pathkeys(root, outer_plan, outerpathkeys, -1.0);
            }

            outerPlan(join_plan) = outer_plan;
        } else if (is_execute_on_coordinator(outer_plan)) {
            if (IsA(join_plan, HashJoin)) {
                AssertEreport(IsA(inner_plan, Hash), MOD_OPT, "The inner_plan is NOT a Hash");

                if (IsA(outerPlan(inner_plan), Stream))
                    child_plan = outerPlan(outerPlan(inner_plan));
                else
                    child_plan = outerPlan(inner_plan);

                outerPlan(inner_plan) = make_simple_RemoteQuery(child_plan, root, false);

                /* Modify locator information again */
                inherit_plan_locator_info(inner_plan, outerPlan(inner_plan));
            } else {
                List* innerpathkeys = NIL;

                if (IsA(inner_plan, Stream))
                    child_plan = outerPlan(inner_plan);
                else
                    child_plan = inner_plan;
                inner_plan = make_simple_RemoteQuery(child_plan, root, false);

                if (IsA(join_plan, MergeJoin)) {
                    MergePath* merge_path = (MergePath*)join_path;

                    AssertEreport(IsA(join_path, MergePath), MOD_OPT, "The join plan is NOT a Mergejoin");

                    if (merge_path->innersortkeys)
                        innerpathkeys = merge_path->innersortkeys;
                    else
                        innerpathkeys = merge_path->jpath.innerjoinpath->pathkeys;

                    AssertEreport(innerpathkeys, MOD_OPT, "The innerpathkeys is NOT valid");
                    inner_plan = (Plan*)make_sort_from_pathkeys(root, inner_plan, innerpathkeys, -1.0);
                }
            }

            innerPlan(join_plan) = inner_plan;
        }
    }

    if (is_execute_on_coordinator(inner_plan) || is_execute_on_coordinator(outer_plan)) {
        /*
         * If one side is executed on coordinator, the other side should be same
         * after the logic above
         */
        join_plan->exec_type = EXEC_ON_COORDS;
        Distribution* distribution = ng_get_single_node_group_distribution();
        join_plan->exec_nodes = ng_convert_to_exec_nodes(distribution, LOCATOR_TYPE_REPLICATED, RELATION_ACCESS_READ);
    } else if (is_execute_on_allnodes(inner_plan) && is_execute_on_allnodes(outer_plan)) {
        join_plan->exec_type = EXEC_ON_ALL_NODES;
        join_plan->exec_nodes = ng_get_default_computing_group_exec_node();
    } else {
        join_plan->exec_type = EXEC_ON_DATANODES;
        join_plan->exec_nodes = stream_merge_exec_nodes(outer_plan, inner_plan, ENABLE_PRED_PUSH(root));
    }

    if (IsA(join_plan, HashJoin)) {
        HashJoin* hashjoin = (HashJoin*)join_plan;

        /*
         * streamBothSides is used in ExecHashJoin for judge if we should probe the first tuple of outer
         * when both outer and inner don't contain stream or not.
         */
        hashjoin->streamBothSides = contain_special_plan_node(outerPlan(hashjoin), T_Stream) ||
                                    contain_special_plan_node(innerPlan(hashjoin), T_Stream);
    } else if (IsA(join_plan, NestLoop)) {
        NestLoop* nestloop = (NestLoop*)join_plan;

        if (nestloop->nestParams && IsA(nestloop->join.plan.righttree, RemoteQuery)) {
            errno_t sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
                NOTPLANSHIPPING_LENGTH,
                "RemoteQuery in NestLoop can't be shipped");
            securec_check_ss_c(sprintf_rc, "\0", "\0");
            mark_stream_unsupport();
        }

        nestloop->materialAll = contain_special_plan_node(outerPlan(nestloop), T_Stream) ||
                                contain_special_plan_node(innerPlan(nestloop), T_Stream);
    }

    join_plan->distributed_keys = join_path->path.distribute_keys;
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
#ifdef ENABLE_MULTIPLE_NODES
    path->rangelistOid = subpath->rangelistOid;
#endif
}

/*
 * To yield join locator information when two sides join with its own locator type
 */
char locator_type_join(char inner_locator_type, char outer_locator_type)
{
#ifdef ENABLE_MULTIPLE_NODES
    if (inner_locator_type == LOCATOR_TYPE_REPLICATED) {
        return outer_locator_type;
    } else if (outer_locator_type == LOCATOR_TYPE_REPLICATED) {
        return inner_locator_type;
    } else {
        if ((inner_locator_type == LOCATOR_TYPE_HASH || inner_locator_type == LOCATOR_TYPE_NONE ||
            IsLocatorDistributedBySlice(inner_locator_type) || inner_locator_type == '\0') &&
            outer_locator_type == inner_locator_type) {
            return inner_locator_type;
        } else {
            /* For hybrid stream, we regard them as round robin. */
            if (inner_locator_type == LOCATOR_TYPE_RROBIN || outer_locator_type == LOCATOR_TYPE_RROBIN ||
                IsLocatorDistributedBySlice(inner_locator_type) || IsLocatorDistributedBySlice(outer_locator_type)) {
                return LOCATOR_TYPE_RROBIN;
            } else {
                ereport(DEBUG1,
                    (errmodule(MOD_OPT_JOIN),
                        (errmsg("locator left %c, localtor right %c", inner_locator_type, outer_locator_type))));
            }

            return LOCATOR_TYPE_NONE;
        }
    }
#else
    /* inner_locator_type = '\0' under single node */
    return '\0';
#endif
}

void ProcessRangeListJoinType(Path* joinPath, Path* outerPath, Path* innerPath)
{
#ifdef ENABLE_MULTIPLE_NODES
    if (IsLocatorDistributedBySlice(joinPath->locator_type)) {
        bool isOuterRepl = is_replicated_path(outerPath);
        bool isInnerRepl = is_replicated_path(innerPath);

        if (isOuterRepl) {
            joinPath->rangelistOid = innerPath->rangelistOid;
        } else if (isInnerRepl) {
            joinPath->rangelistOid = outerPath->rangelistOid;
        } else if (IsSliceInfoEqualByOid(outerPath->rangelistOid, innerPath->rangelistOid)) {
            /* pick any one oid to represent slice distribution */
            joinPath->rangelistOid = outerPath->rangelistOid;
        } else {
            /* treat it as roundrobin */
            joinPath->locator_type = LOCATOR_TYPE_RROBIN;
            joinPath->rangelistOid = InvalidOid;
        }
    }
#else
    return;
#endif
}

static List* get_parallel_plan_list(Plan* plan)
{
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
    return plan_list;
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
    if (NULL == plan)
        return;

    /* Set parallel info for plan node. */
    if (plan->dop != dop)
        ereport(
            DEBUG1, (errmodule(MOD_OPT_JOIN), (errmsg("[SMP]: Mismatch smp info, old %d, new %d.", plan->dop, dop))));

    plan->dop = dop;
    plan->parallel_enabled = (dop > 1);

    List* plan_list = NIL;
    plan_list = get_parallel_plan_list(plan);
    if (NIL != plan_list) {
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
                if (SET_DOP(dop) != SET_DOP(stream->smpDesc.consumerDop)) {
                    if (isIntergratedMachine) {
                        stream->smpDesc.consumerDop = SET_DOP(dop);
                    } else {
                        ereport(LOG, (errmodule(MOD_OPT), (errmsg("Mismatch parallel degree."))));
                    }
                }

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

/*
 * Generate unique id for running query.
 */
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

/*
 * Generate 64 bits unique id for running query.
 * It ONLY produces unique id when used in Coordinator, and in DataNode it just returns 0.
 */
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
    instr_stmt_report_debug_query_id(id);

    return id;
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

/*
 * push down the exec node to child plan tree.
 * If add_node is true, we push down exec node on all child plan tree until stream node.
 * if add_node is false, we only handle replicate plan until stream node.
 */
void pushdown_execnodes(Plan* plan, ExecNodes* exec_nodes, bool add_node, bool only_nodelist)
{
    Stream* streamPlan = NULL;

    if (!PointerIsValid(plan)) {
        return;
    }

    if (exec_nodes == NULL) {
        return;
    }

    if (IsA(plan, CteScan)) {
        RecursiveUnion* ru_plan = ((CteScan*)plan)->subplan;

        Assert(IsA(ru_plan, RecursiveUnion));

        pushdown_execnodes((Plan*)ru_plan, exec_nodes, add_node, only_nodelist);

        plan->exec_nodes = exec_nodes;
    } else if (IsA(plan, Stream) || IsA(plan, VecStream)) {
        /*
         * We suppose the plan under Stream plan should not replicated. So we
         * just modify the stream consumer nodes.
         */
        streamPlan = (Stream*)plan;

        /*
         * When exec_type of plan is EXEC_ON_ALL_NODES, exec_nodes may be null,
         * then we will not set consumer nodeList.
         */
        if (is_broadcast_stream(streamPlan) && exec_nodes) {
            streamPlan->consumer_nodes->nodeList = exec_nodes->nodeList;
        }

        /* For local stream, we need to push down. */
        if (STREAM_IS_LOCAL_NODE(streamPlan->smpDesc.distriType) && exec_nodes) {
            if (list_length(exec_nodes->nodeList) == 0) {
                exec_nodes->nodeList = GetAllDataNodes();
            }
            plan->exec_nodes = exec_nodes;
            streamPlan->consumer_nodes = exec_nodes;
            pushdown_execnodes(plan->lefttree, exec_nodes, add_node, only_nodelist);
        }
    } else if (is_replicated_plan(plan) || add_node) {
        if (IsA(plan, Append) || IsA(plan, VecAppend)) {
            /* We try to modify every subplan for Append */
            ListCell* cell = NULL;

            foreach (cell, ((Append*)plan)->appendplans) {
                pushdown_execnodes((Plan*)lfirst(cell), exec_nodes, add_node, only_nodelist);
            }
        } else if (IsA(plan, MergeAppend)) {
            /*
             * We try to modify every subplan for MergeAppend
             */
            ListCell* cell = NULL;

            foreach (cell, ((MergeAppend*)plan)->mergeplans) {
                pushdown_execnodes((Plan*)lfirst(cell), exec_nodes, add_node, only_nodelist);
            }
        } else if (IsA(plan, ModifyTable)) {
            /*
             * We try to modify every subplan for ModifyTable
             */
            ListCell* cell = NULL;

            foreach (cell, ((ModifyTable*)plan)->plans) {
                pushdown_execnodes((Plan*)lfirst(cell), exec_nodes, add_node, only_nodelist);
            }
        } else if (IsA(plan, SubqueryScan) || IsA(plan, VecSubqueryScan)) {
            pushdown_execnodes(((SubqueryScan*)plan)->subplan, exec_nodes, add_node, only_nodelist);
        } else if (IsA(plan, Material)) {
            if (((Material*)plan)->materialize_all) {
                if (plan->exec_nodes == NULL) {
                    ereport(ERROR,
                        (errmodule(MOD_OPT),
                            errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                            (errmsg("plan->exec_nodes should not be NULL"))));
                }
                AssertEreport(plan->exec_nodes->baselocatortype == LOCATOR_TYPE_REPLICATED,
                    MOD_OPT,
                    "The base locator type is not replicated");
                if (IsA(plan->lefttree, Stream))
                    AssertEreport(((Stream*)plan->lefttree)->consumer_nodes->baselocatortype == LOCATOR_TYPE_REPLICATED,
                        MOD_OPT,
                        "The base locator type is not replicated");
                else
                    AssertEreport(plan->lefttree->exec_nodes->baselocatortype == LOCATOR_TYPE_REPLICATED,
                        MOD_OPT,
                        "The base locator type is not replicated");

                /* For multi node group, we should add broadcast for correlated replicate subplan */
                if (!IsA(plan->lefttree, Stream)) {
                    if (plan->lefttree->exec_nodes->nodeList == NIL) {
                        plan->lefttree->exec_nodes->nodeList = GetAllDataNodes();
                    }
                    List* src_surplus_nodelist =
                        list_difference_int(plan->lefttree->exec_nodes->nodeList, exec_nodes->nodeList);
                    List* des_surplus_nodelist =
                        list_difference_int(exec_nodes->nodeList, plan->lefttree->exec_nodes->nodeList);
                    /* If we should spread to more datanode, just add broadcast */
                    if (des_surplus_nodelist != NIL) {
                        List* desired_nodelist =
                            list_intersection_int(plan->lefttree->exec_nodes->nodeList, exec_nodes->nodeList);
                        if (desired_nodelist != NIL)
                            plan->lefttree->exec_nodes->nodeList = desired_nodelist;
                        plan->lefttree->exec_nodes->nodeList =
                            list_make1_int(pickup_random_datanode_from_plan(plan->lefttree));
                        pushdown_execnodes(plan->lefttree, plan->lefttree->exec_nodes, false, only_nodelist);
                        plan->lefttree = make_stream_plan(NULL, plan->lefttree, NIL, 0.0);
                    } else if (src_surplus_nodelist != NIL) {
                        /* If we should spread to less datanode, just prune the datanode */
                        pushdown_execnodes(plan->lefttree, plan->lefttree->exec_nodes, false, only_nodelist);
                    }
                    list_free_ext(src_surplus_nodelist);
                    list_free_ext(des_surplus_nodelist);
                }
            }
            pushdown_execnodes(plan->lefttree, exec_nodes, add_node, only_nodelist);
        } else {
            pushdown_execnodes(plan->lefttree, exec_nodes, add_node, only_nodelist);
            pushdown_execnodes(plan->righttree, exec_nodes, add_node, only_nodelist);
        }

        if (plan->exec_nodes == NULL || !only_nodelist) {
            plan->exec_nodes = exec_nodes;
        } else {
            plan->exec_nodes->nodeList = exec_nodes->nodeList;
        }
    }

    /* reset the plan_rows as divide u_sess->pgxc_cxt.NumDataNodes for replication plan. */
    if (is_replicated_plan(plan) && exec_nodes != NULL && exec_nodes->nodeList != NULL)
        plan->plan_rows = PLAN_LOCAL_ROWS(plan) * list_length(exec_nodes->nodeList);
}

/*
 * create_stream_path
 *	  Creates a path corresponding to a scan of a remote partition,
 *	  returning the pathnode.
 */
Path* create_stream_path(PlannerInfo* root, RelOptInfo* rel, StreamType type, List* distribute_keys, List* pathkeys,
    Path* subpath, double skew, Distribution* target_distribution, ParallelDesc* smp_desc, List* ssinfo)
{
    Path* newpath = NULL;

    StreamPath* pathnode = makeNode(StreamPath);
    pathnode->path.pathtype = T_Stream;
    pathnode->path.parent = rel;
    pathnode->path.pathkeys = pathkeys;
    pathnode->type = type;
    pathnode->path.distribute_keys = distribute_keys;
    pathnode->path.multiple = skew;
    pathnode->smpDesc = smp_desc;
    pathnode->skew_list = ssinfo;

    if (IsA(subpath, MaterialPath))
        pathnode->subpath = ((MaterialPath*)subpath)->subpath;
    else
        pathnode->subpath = subpath;

    if (smp_desc != NULL)
        pathnode->path.dop = smp_desc->consumerDop;
    else
        pathnode->path.dop = 1;

    if (type == STREAM_GATHER) {
        pathnode->path.exec_type = EXEC_ON_COORDS;
    } else {
        pathnode->path.exec_type = EXEC_ON_DATANODES;
    }

    switch (type) {
        case STREAM_BROADCAST:
        case STREAM_GATHER: {
            pathnode->path.locator_type = LOCATOR_TYPE_REPLICATED;
            break;
        }
        case STREAM_REDISTRIBUTE: {
            /* LOCAL_BROADCAST should not change locate type. */
            if (smp_desc && LOCAL_BROADCAST == smp_desc->distriType) {
                pathnode->path.locator_type = subpath->locator_type;
                pathnode->path.rangelistOid = subpath->rangelistOid;
            } else {
                pathnode->path.locator_type = LOCATOR_TYPE_HASH;
            }
            /*
             * The distribute key can not be NIL, unless
             * local roundrobin or local broadcast.
             */
            Assert(NIL != distribute_keys || (NULL != smp_desc && (LOCAL_ROUNDROBIN == smp_desc->distriType ||
                                                                      LOCAL_BROADCAST == smp_desc->distriType)));

            break;
        }
        case STREAM_HYBRID: {
            pathnode->path.locator_type = LOCATOR_TYPE_RROBIN;
            break;
        }
        default:
            break;
    }

#ifdef ENABLE_MULTIPLE_NODES
    /* For SMP, we just make this stream in it's original node group */
    if (NULL == target_distribution) {
        target_distribution = ng_get_dest_distribution(pathnode->subpath);
    }

    if (IS_STREAM_PLAN) {
        Distribution* distribution = ng_get_dest_distribution(pathnode->subpath);
        ng_copy_distribution(&pathnode->path.distribution, distribution);
        ng_copy_distribution(&pathnode->consumer_distribution, target_distribution);
    }
#endif

    cost_stream(pathnode, rel->width, true);

    if (IsA(subpath, MaterialPath)) {
        Cost rescan_startup_cost, rescan_total_cost;

        bool is_materialize_all = ((MaterialPath*)subpath)->materialize_all;
        newpath = (Path*)create_material_path((Path*)pathnode, is_materialize_all);
        cost_rescan(root, newpath, &rescan_startup_cost, &rescan_total_cost, &((MaterialPath*)newpath)->mem_info);
        ((MaterialPath*)newpath)->mem_info.regressCost *= DEFAULT_NUM_ROWS;
    } else
        newpath = (Path*)pathnode;

    return newpath;
}

/*
 * pre check condition of Query tree before create gather paths
 */
bool PreCheckGatherParse(PlannerInfo* root, RelOptInfo* rel)
{
    bool support = true;

    /* not support any aggregates */
    if (root->parse->hasAggs || root->hasHavingQual ||
        root->parse->groupClause != NIL || root->parse->groupingSets != NIL) {
        return false;
    }

    /* not support sort or limit */
    if (root->parse->sortClause != NIL || root->parse->limitCount ||
        root->parse->limitOffset) {
        return false;
    }

    /* not support subquery */
    if (root->parse->hasSubLinks || root->is_correlated || root->parent_root != NULL) {
        return false;
    }

    /* not support cte and recursive */
    if (root->parse->hasModifyingCTE || root->parse->cteList != NIL) {
        return false;
    }

    if (root->parse->hasDistinctOn || root->parse->hasForUpdate ||
        root->parse->hasWindowFuncs || root->parse->distinctClause != NIL) {
        return false;
    }

    /* not support any modified tables */
    if (root->parse->upsertClause != NULL || root->parse->resultRelation != 0) {
        return false;
    }

    return support;
}

bool PreCheckGatherOthers(PlannerInfo* root, RelOptInfo* rel, bool isJoin)
{
    bool support = true;

    if (root->hasRecursion || root->is_under_recursive_cte) {
        return false;
    }

    /* unsupport tables */
    if (rel->orientation != REL_ROW_ORIENTED && !isJoin) {
        return false;
    }

    /* not support union all or parameterized path */
    if (check_param_clause((Node*)rel->baserestrictinfo) ||
        root->append_rel_list != NIL) {
        return false;
    }

    /* not support random plan */
    if (u_sess->attr.attr_sql.plan_mode_seed != OPTIMIZE_PLAN) {
        return false;
    }

    return support;
}

/*
 * Create Gather Paths based on subpath
 */
void CreateGatherPaths(PlannerInfo* root, RelOptInfo* rel, bool isJoin)
{
    /* Create gather path on plain rel pathlist. */
    ListCell* lc = NULL;
    List* pathlist = rel->pathlist;

    if (!PreCheckGatherParse(root, rel) || !PreCheckGatherOthers(root, rel, isJoin)) {
        return;
    }

    foreach(lc, pathlist) {
        Path* path = (Path*)lfirst(lc);

        /* only add gather for path which execute on datanodes */
        if (EXEC_CONTAIN_COORDINATOR(path->exec_type) || path->param_info) {
            continue;
        }

        /* just return if node group exist. */
        if (!ng_is_same_group(&path->distribution, ng_get_installation_group_distribution())) {
            continue;
        }

        if (path->dop > 1 || path->pathtype == T_SubqueryScan) {
            continue;
        }

        if (isJoin) {
            /* Stream walker to check stream */
            ContainStreamContext context;
            context.outer_relids = NULL;
            context.upper_params = NULL;
            context.only_check_stream = true;
            context.under_materialize_all = false;
            context.has_stream = false;
            context.has_parameterized_path = false;
            context.has_cstore_index_delta = false;
            stream_path_walker(path, &context);
            /* not support any of subpaths contains stream */
            if (context.has_stream) {
                continue;
            }
        }
        add_path(root, rel, create_stream_path(root, rel, STREAM_GATHER, NIL, NIL, path, 1.0));
    }

    /* do not set_chepeast on join paths here */
    if (isJoin) {
        return;
    }
    set_cheapest(rel);
}


/*
 * Check if node is modifyTable for DFS table.
 * Example:
 * ->  Append
 *       ->  Row Adapter
 * 			->  Vector Update
 *				->dfs scan
 * 		->  Update
 *				->seq scan
 */
bool IsModifyTableForDfsTable(Plan* AppendNode)
{
    if (NULL == AppendNode)
        return false;

    if (IsA(AppendNode, Append) || IsA(AppendNode, VecAppend)) {
        Append* append = (Append*)AppendNode;
        ListCell* lc = NULL;
        foreach (lc, append->appendplans) {
            Plan* node = (Plan*)lfirst(lc);
            if (NULL == node)
                return false;

            if (IsA(node, RowToVec) || IsA(node, VecToRow))
                node = node->lefttree;
            if (!(IsA(node, ModifyTable) || IsA(node, VecModifyTable)))
                return false;
        }
        return true;
    }

    return false;
}

void disaster_read_array_init()
{
    Snapshot snapshot = GetActiveSnapshot();
    if (snapshot == NULL) {
        snapshot = GetTransactionSnapshot();
    }

    LWLockAcquire(MaxCSNArrayLock, LW_SHARED);
    CommitSeqNo *maxcsn = t_thrd.xact_cxt.ShmemVariableCache->max_csn_array;
    bool *mainstandby = t_thrd.xact_cxt.ShmemVariableCache->main_standby_array;
    if (maxcsn == NULL) {
        ereport(ERROR, (errmsg("max_csn_array is NULL")));
    }
    if (mainstandby == NULL) {
        ereport(ERROR, (errmsg("main_standby_array is NULL")));
    }
    
    int slice_num = u_sess->pgxc_cxt.NumDataNodes;
    int slice_internal_num = u_sess->pgxc_cxt.standby_num + 1;

    for (int i = 0; i < slice_num; i++) {
        int j = 0;
        for (; j < slice_internal_num; j++) {
            int nodeIdx = i + j * slice_num;
            bool set = false;
            if (snapshot->snapshotcsn <= maxcsn[nodeIdx] + 1) {
                u_sess->pgxc_cxt.disasterReadArray[i] = nodeIdx;
                set = true;
                ereport(LOG, (errmsg("select [%d, %d] node index %d, nodeid, %d, csn %lu, %s", 
                    i, j,
                    nodeIdx,
                    u_sess->pgxc_cxt.poolHandle->dn_conn_oids[nodeIdx],
                    maxcsn[nodeIdx],
                    mainstandby[nodeIdx] ? "Main Standby" : "Cascade Standby")));
            } else {
                ereport(LOG, (errmsg("nodeid %d, snapshotcsn = %lu, max_csn_array = %lu",
                                     u_sess->pgxc_cxt.poolHandle->dn_conn_oids[nodeIdx],
                                     snapshot->snapshotcsn,
                                     maxcsn[nodeIdx])));
            }
            if (set && !mainstandby[nodeIdx]) {
                break;
            }
        }
        if (j == (u_sess->pgxc_cxt.standby_num + 1))
            ereport(LOG, (errmsg("current slice datanode is all invalid")));
    }
    LWLockRelease(MaxCSNArrayLock);
    u_sess->pgxc_cxt.DisasterReadArrayInit = true;
}

NodeDefinition* get_all_datanodes_def()
{
    Oid* dn_node_arr = NULL;
    int dn_node_num;
    int i;
    NodeDefinition* nodeDefArray = NULL;
    int rc = 0;

    if (IS_DISASTER_RECOVER_MODE) {
        PgxcNodeGetOidsForInit(NULL, &dn_node_arr, NULL, &dn_node_num, NULL, false);
    } else {
        PgxcNodeGetOids(NULL, &dn_node_arr, NULL, &dn_node_num, false);
    }

    int dnNum = Max(u_sess->pgxc_cxt.NumTotalDataNodes, u_sess->pgxc_cxt.NumDataNodes);
    if (dnNum != dn_node_num) {
        ResetSessionExecutorInfo(true);
        if (dn_node_arr != NULL) {
            pfree_ext(dn_node_arr);
            dn_node_arr = NULL;
        }
        if (IS_DISASTER_RECOVER_MODE) {
            PgxcNodeGetOidsForInit(NULL, &dn_node_arr, NULL, &dn_node_num, NULL, false);
        } else {
            PgxcNodeGetOids(NULL, &dn_node_arr, NULL, &dn_node_num, false);
        }
        if (dnNum != dn_node_num)
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                    errmsg("total datanodes maybe be changed")));
    }

    if (IS_CN_DISASTER_RECOVER_MODE) {
        disaster_read_array_init();
    }

    nodeDefArray = (NodeDefinition*)palloc(sizeof(NodeDefinition) * u_sess->pgxc_cxt.NumDataNodes);
    NodeDefinition* res = NULL;
    for (i = 0; i < u_sess->pgxc_cxt.NumDataNodes; i++) {
        if (!IS_DISASTER_RECOVER_MODE) {
            Oid current_primary_oid = PgxcNodeGetPrimaryDNFromMatric(dn_node_arr[i]);
            res = PgxcNodeGetDefinition(current_primary_oid);
        } else {
            int index = u_sess->pgxc_cxt.disasterReadArray[i];
            res = PgxcNodeGetDefinition(dn_node_arr[index == -1 ? i : index]);
        }

        rc = memcpy_s(&nodeDefArray[i], sizeof(NodeDefinition), res, sizeof(NodeDefinition));
        securec_check(rc, "\0", "\0");
    }
    if (dn_node_arr != NULL) {
        pfree_ext(dn_node_arr);
        dn_node_arr = NULL;
    }

    return nodeDefArray;
}

/*
 * hasSystemColumnTargetFromEqualVars
 *
 * The function is used to judge whether the equalVars has the system column.
 * If the system column is used in equalVars, return true. else, return false.
 */
static bool hasSystemColumnTargetFromEqualVars(List* equalVars)
{
    bool SystemColumn = false;
    ListCell* lc = NULL;
    foreach (lc, equalVars) {
        Var* var = (Var*)lfirst(lc);
        if (var->varattno < 1) {
            SystemColumn = true;
            break;
        }
    }

    return SystemColumn;
}

/*
 * canSeparateComputeAndStorageGroupForDelete
 *
 * The function is used to judge whether the compute group and storage group are separated
 * in delete clause or update clause. If the table is replication table, return false.It means that
 * the compute group and storage group can not be separated in different groups.
 */
bool canSeparateComputeAndStorageGroupForDelete(PlannerInfo* root)
{
    if ((CMD_UPDATE == root->parse->commandType || CMD_DELETE == root->parse->commandType) &&
        hasSystemColumnTargetFromEqualVars(root->parse->equalVars)) {
        RangeTblEntry* rte = rt_fetch(root->parse->resultRelation, root->parse->rtable); /* target udi relation */
        char locator_type = GetLocatorType(rte->relid);
        if (IsLocatorReplicated(locator_type)) {
            return false;
        }
    }

    return true;
}

/*
 * get distributekey index. return NIL if we cannot find.
 */
List* distributeKeyIndex(PlannerInfo* root, List* distributed_keys, List* targetlist)
{
    ListCell* teCell = NULL;
    ListCell* cell = NULL;
    List* result = NULL;

    /*
     * We should avoid distribute key point to the same
     * item, so add mark if pointed before
     */
    bool* matched_key = (bool*)palloc0(sizeof(bool) * list_length(targetlist));

    if (NIL == distributed_keys) {
        pfree_ext(matched_key);
        return NIL;
    }

    foreach (cell, distributed_keys) {
        int index = 0, matched_index = 0;
        Node* disKey = (Node*)lfirst(cell);
        Var* disVar = locate_distribute_var((Expr*)disKey);

        foreach (teCell, targetlist) {
            TargetEntry* teEntry = (TargetEntry*)lfirst(teCell);
            Node* node = (Node*)locate_distribute_var(teEntry->expr);

            /*
             * we use located var as distribute key before, so make the target
             * item the same before comparison
             */
            if (equal(teEntry->expr, disKey) ||
                (disVar != NULL && node != NULL && judge_node_compatible(root, (Node*)disVar, node))) {
                if (!matched_key[index]) {
                    matched_key[index] = true;
                    result = lappend_int(result, teEntry->resno);
                    break;
                } else
                    matched_index = (int)teEntry->resno;
            }
            index++;
        }

        if (teCell == NULL) {
            /* At last, give the same item if no other different item found */
            if (matched_index != 0)
                result = lappend_int(result, matched_index);
            else {
                pfree_ext(matched_key);
                list_free_ext(result);
                return NULL;
            }
        }
    }

    pfree_ext(matched_key);
    return result;
}

List* make_groupcl_for_append(PlannerInfo* root, List* targetlist)
{
    ListCell* lc = NULL;
    List* grplist = NIL;

    foreach (lc, targetlist) {
        TargetEntry* tle = (TargetEntry*)lfirst(lc);
        Node* node = (Node*)tle->expr;
        RelOptInfo* rel = NULL;
        int relid = 0;
        Relids varnos = pull_varnos(node);

        /*
         * Get all varnos from node for non var expr, and filter the reloptkind is
         * RELOPT_OTHER_MEMBER_REL. because it unsupport the reloptkind
         * in estimate_num_groups func later.
         */
        while ((relid = bms_first_member(varnos)) >= 0) {
            if (relid == 0)
                break;

            /*
             * The reloptkind is RELOPT_OTHER_MEMBER_REL for UNION ALL,
             * it unsupport the reloptkind in estimate_num_groups func later.
             */
            rel = find_base_rel(root, relid);
            if (rel->reloptkind == RELOPT_OTHER_MEMBER_REL) {
                relid = 0;
                break;
            }
        }
        bms_free_ext(varnos);

        /*
         * It should filter the junk or expr type of node unsupport redistributable
         * or the var is not alone baserel, so we could not estimate distinct for expr.
         */
        if (tle->resjunk || !IsTypeDistributable(exprType(node)) || (relid == 0))
            continue;

        grplist = lappend(grplist, node);
    }

    return grplist;
}

/*
 * Check if all the subplans already paralleled, then we can parallel the append plan.
 * Otherwise, we need to add local gather above the parallelized subplans.
 */
bool isAllParallelized(List* subplans)
{
    ListCell* cell = NULL;

    foreach (cell, subplans) {
        Plan* subplan = (Plan*)lfirst(cell);
        if (subplan->dop <= 1)
            return false;
    }

    return true;
}

/*
 * Check whether the Agg/ExprState node should be evaluated in foreign server and get the separate agg expression.
 */
bool is_foreign_expr(Node* node, foreign_qual_context* context)
{
    return !foreign_qual_walker(node, context);
}

void foreign_qual_context_init(foreign_qual_context* context)
{
    context->collect_vars = false;
    context->vars = NIL;
    context->aggs = NIL;
}

void foreign_qual_context_free(foreign_qual_context* context)
{
    list_free_ext(context->vars);
    context->vars = NIL;
    list_free_ext(context->aggs);
    context->aggs = NIL;
}

char get_locator_type(Plan* plan)
{
    if (IsA(plan, Stream))
        return ((Stream*)plan)->consumer_nodes->baselocatortype;
    else
        return plan->exec_nodes->baselocatortype;
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

/*
 * @Description: Judge hash arithmetic of OpExpr's args type whether compatible
 * @in op_expr - operator expr
 */
bool is_args_type_compatible(OpExpr* op_expr)
{
    if (list_length(op_expr->args) == 2) {
        if (is_compatible_type(exprType((Node*)linitial(op_expr->args)), exprType((Node*)lsecond(op_expr->args)))) {
            return true;
        } else {
            return false;
        }
    } else {
        return true;
    }
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
            } else if (IsA(node, FuncExpr)) {
                FuncExpr* funcexpr = (FuncExpr*)node;
                if ((funcexpr->funcformat == COERCE_IMPLICIT_CAST || funcexpr->funcformat == COERCE_EXPLICIT_CAST) &&
                    list_length(funcexpr->args) == 1) {
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
    smpDesc->consumerDop = ((consumer_dop > 1) ? consumer_dop : 1);
    smpDesc->producerDop = ((producer_dop > 1) ? producer_dop : 1);
    smpDesc->distriType = smp_type;
    return smpDesc;
}

/*
 * @Description:
 *    Create a local gather node above plan.
 *
 * @param[IN] plan: the node needed to add stream node above
 *
 * @return Plan*: the new added stream node
 */
Plan* create_local_gather(Plan* plan)
{
    Stream* stream_node = NULL;
    Plan* stream_plan = NULL;
    double size = (PLAN_LOCAL_ROWS(plan)) * (plan->plan_width) / 8192.0;

    /* No need to add local gather under a unparallel plan. */
    if (plan->dop <= 1)
        return plan;
#ifdef ENABLE_MULTIPLE_NODES
    if (is_replicated_plan(plan) && NIL == plan->exec_nodes->nodeList) {
        plan->dop = 1;
        return plan;
    }
#endif
    /* Already is a stream, then we do need to add local gather. */
    if (IsA(plan, Stream)) {
        Stream* st = (Stream*)plan;
        plan->dop = 1;
        st->smpDesc.consumerDop = 1;

        if (REMOTE_SPLIT_BROADCAST == st->smpDesc.distriType)
            st->smpDesc.distriType = REMOTE_BROADCAST;
        else if (REMOTE_SPLIT_DISTRIBUTE == st->smpDesc.distriType)
            st->smpDesc.distriType = REMOTE_DISTRIBUTE;
        else if (LOCAL_DISTRIBUTE == st->smpDesc.distriType)
            st->smpDesc.distriType = LOCAL_ROUNDROBIN;
        return plan;
    }

    /* Set stream struct parameter. */
    stream_node = makeNode(Stream);
    stream_node->type = STREAM_REDISTRIBUTE;
    stream_node->consumer_nodes = (ExecNodes*)copyObject(plan->exec_nodes);
    stream_node->is_sorted = false;
    stream_node->is_dummy = false;
    stream_node->sort = NULL;
    stream_node->smpDesc.consumerDop = 1;
    stream_node->smpDesc.producerDop = plan->dop;
    stream_node->smpDesc.distriType = LOCAL_ROUNDROBIN;
    stream_node->distribute_keys = NIL;

    /* Set plan struct parameter. */
    stream_plan = &stream_node->scan.plan;
    stream_plan->distributed_keys = plan->distributed_keys;
    stream_plan->targetlist = list_copy(plan->targetlist);
    stream_plan->lefttree = plan;
    stream_plan->righttree = NULL;
    stream_plan->exec_nodes = (ExecNodes*)copyObject(ng_get_dest_execnodes(plan));
    if (!stream_plan->exec_nodes)
        stream_plan->exec_nodes = makeNode(ExecNodes);
    stream_plan->exec_nodes->baselocatortype = LOCATOR_TYPE_RROBIN;
    stream_plan->hasUniqueResults = plan->hasUniqueResults;
    copy_plan_costsize(stream_plan, plan);
    stream_plan->total_cost += LOCAL_SEND_KDATA_COST * size / plan->dop + LOCAL_RECEIVE_KDATA_COST * size;
    stream_plan->dop = 1;

    return stream_plan;
}

/*
 * @Description:
 *    Create local redistribute plan.
 *
 * @param[IN] root: the PlannerInfo .
 * @param[IN] lefttree: the subplan of stream.
 * @param[IN] redistribute_keys: redistribute key list.
 * @param[IN] multiple: skew mutiple.
 * @return Plan*: stream plan
 *
 */
Plan* create_local_redistribute(PlannerInfo* root, Plan* lefttree, List* redistribute_keys, double multiple)
{
    Plan* result = lefttree;

    if (lefttree->dop <= 1)
        return result;

    if (IsA(lefttree, Stream) || !is_local_redistribute_needed(lefttree) || NIL == lefttree->distributed_keys)
        return result;

    if (check_dsitribute_key_in_targetlist(root, redistribute_keys, lefttree->targetlist)) {
        result = make_redistribute_for_agg(root, lefttree, lefttree->distributed_keys, multiple, NULL, true);
    } else {
        result = create_local_gather(lefttree);
    }

    return result;
}

/*
 * get_bucketmap_by_execnode
 *
 * Return the bucketmap by execnode
 */
uint2* get_bucketmap_by_execnode(ExecNodes* exec_node, PlannedStmt* plannedstmt, int *bucketCnt)
{
    if (exec_node == NULL) {
        return NULL;
    }

    int nodeLen = list_length(exec_node->nodeList);
    if (nodeLen == 0) {
        return NULL;
    }
    /*
     * If bucketMapIdx is marked as default, it means the bucketmap is generated as
     * default way "hashvalue % member_count" then we do bucket map generation on DN side,
     * otherwise we use the one passed from CN in PlannedStmt
     */
    int bucketMapIdx = exec_node->bucketmapIdx;
    uint2* bucketMap = NULL;

    if (((uint32)bucketMapIdx & BUCKETMAP_DEFAULT_INDEX_BIT) == (uint32)BUCKETMAP_DEFAULT_INDEX_BIT) {
        *bucketCnt = bucketMapIdx & ~(BUCKETMAP_DEFAULT_INDEX_BIT);
        bucketMap = (uint2*)palloc0(*bucketCnt * sizeof(uint2));
        Assert(*bucketCnt <= BUCKETDATALEN);
        for (int i = 0; i < *bucketCnt; i++) {
            bucketMap[i] = static_cast<uint2>(i % nodeLen);
        }
    } else {
        bucketMap =  plannedstmt->bucketMap[bucketMapIdx];
        *bucketCnt = plannedstmt->bucketCnt[bucketMapIdx];
    }
    return bucketMap;
}

/*
 * get_oridinary_or_foreign_relid
 *
 * If the rtable (and its subqueries) contain ordinary table or foreign table which is defined by user,
 * return its relid.
 */
Oid get_oridinary_or_foreign_relid(List* rtable)
{
    ListCell* lc = NULL;
    Oid relationId = InvalidOid;

    foreach (lc, rtable) {
        RangeTblEntry* rte = (RangeTblEntry*)lfirst(lc);

        /* Only ordinary table or foreign table which user created can end. */
        if ((rte->relkind == 'r' || rte->relkind == 'f') && rte->relid >= FirstNormalObjectId) {
            relationId = rte->relid;
            break;
        }

        /* For subquery, check its rtable recurisively until relid meets the requirements. */
        if (rte->rtekind == RTE_SUBQUERY && rte->subquery != NULL) {
            relationId = get_oridinary_or_foreign_relid(rte->subquery->rtable);
            if (relationId != InvalidOid)
                break;
        }
    }

    return relationId;
}

/*
 * GetGlobalStreamBucketMap
 *
 * Return the bucketmap
 */
uint2* GetGlobalStreamBucketMap(PlannedStmt* planned_stmt)
{
    Oid relationId = InvalidOid;

    /* Check the available relid. */
    relationId = get_oridinary_or_foreign_relid(planned_stmt->rtable);
    if (relationId == InvalidOid) {
        return NULL;
    }

    Oid groupoid = InvalidOid;
    int nmembers = 0;
    Oid* members = NULL;

    groupoid = get_pgxc_class_groupoid(relationId);
    if (groupoid != InvalidOid) {
        nmembers = get_pgxc_groupmembers(groupoid, &members);
        if (nmembers != 0) {
            uint2* bucketMap = (uint2*)palloc0(BUCKETDATALEN * sizeof(uint2));
            for (int i = 0; i < BUCKETDATALEN; i++) {
                bucketMap[i] = static_cast<uint2>(i % nmembers);
            }
            return bucketMap;
        }
    }

    return NULL;
}

/*
 * When the there is 'order by' in ARRAY_SUBLINK, we shoud sort
 * the data globally. So we should add 'broadcast' under 'sort', or add 'broadcast' + ''sort' after the
 * plan(usually when IndexOnlyScan that no need 'sort' explicitly)
 */
Plan* add_broacast_under_local_sort(PlannerInfo* root, PlannerInfo* subroot, Plan* plan)
{
    switch (nodeTag(plan)) {
        case T_Sort: {
            Distribution* target_distribution = ng_get_correlated_subplan_group_distribution();

            Plan* stream_plan = make_stream_plan(root, plan->lefttree, NIL, 0.0, target_distribution);
            plan->lefttree = stream_plan;
            inherit_plan_locator_info(plan, stream_plan);
            return plan;
        }
        case T_SubqueryScan: {
            RelOptInfo* rel = NULL;
            SubqueryScan* ssplan = (SubqueryScan*)plan;
            rel = find_base_rel(subroot, ssplan->scan.scanrelid);

            ssplan->subplan = add_broacast_under_local_sort(root, subroot, ssplan->subplan);
            inherit_plan_locator_info((Plan*)ssplan, ssplan->subplan);
            rel->subplan = ssplan->subplan;

            return plan;
        }
        default: {
            if (subroot->sort_pathkeys) {
                Distribution* target_distribution = ng_get_correlated_subplan_group_distribution();

                Plan* stream_plan = make_stream_plan(root, plan, NIL, 0.0, target_distribution);
                Sort* sort_plan = make_sort_from_pathkeys(subroot, stream_plan, subroot->sort_pathkeys, -1.0);

                return (Plan*)sort_plan;
            }
        } break;
    }

    return plan;
}

List* getSubPlan(Plan* node, List* subplans, List* initplans)
{
    ListCell* lc = NULL;
    List* results = NIL;
    List* subplan_list = check_subplan_list(node);
    foreach (lc, subplan_list) {
        Node* node_lc = (Node*)lfirst(lc);
        SubPlan* subplan = NULL;
        if (IsA(node_lc, SubPlan)) {
            subplan = (SubPlan*)lfirst(lc);

            subplan_list = list_concat(subplan_list, check_subplan_expr(subplan->testexpr));
        } else {
            Param* param = (Param*)node_lc;
            ListCell* lc2 = NULL;
            foreach (lc2, initplans) {
                subplan = (SubPlan*)lfirst(lc2);
                if (list_member_int(subplan->setParam, param->paramid))
                    break;
            }
            if (subplan == NULL || lc2 == NULL)
                continue;
        }
        Node* subnode = (Node*)list_nth(subplans, subplan->plan_id - 1);

        results = lappend(results, subnode);
    }

    return results;
}

typedef struct StreamTypeStr {
    StreamType type;
    char* str;
} StreamTypeStr;

typedef struct SmpStreamTypeStr {
    SmpStreamType type;
    char* str;
} SmpStreamTypeStr;

static const StreamTypeStr g_streamTypeStrArr[] = {
    {STREAM_GATHER, "Stream_Gather"},
    {STREAM_BROADCAST, "Stream_Broadcast"},
    {STREAM_REDISTRIBUTE, "Stream_Redistribute"},
    {STREAM_ROUNDROBIN, "Stream_Roundrobin"},
    {STREAM_HYBRID, "Stream_Hybrid"},
    {STREAM_LOCAL, "Stream_Local"},
    {STREAM_NONE, "Stream_None"}
};

static const SmpStreamTypeStr g_smpStreamTypeStrArr[] = {
    {PARALLEL_NONE, "Parallel_None"},
    {REMOTE_DISTRIBUTE, "Stream_Redistribute"},
    {REMOTE_SPLIT_DISTRIBUTE, "Split_Redistribute"},
    {REMOTE_BROADCAST, "Stream_Broadcast"},
    {REMOTE_SPLIT_BROADCAST, "Split_Broadcast"},
    {REMOTE_HYBRID, "Remote_Bybrid"},
    {LOCAL_DISTRIBUTE, "Local_Redistribute"},
    {LOCAL_BROADCAST, "Local_Broadcast"},
    {LOCAL_ROUNDROBIN, "Local_Roundrobin"}
};

char* StreamTypeToString(StreamType type)
{
    for (uint32 i = 0; i < sizeof(g_streamTypeStrArr) / sizeof(g_streamTypeStrArr[0]); i++) {
        if (g_streamTypeStrArr[i].type == type)
            return g_streamTypeStrArr[i].str;
    }

    return "Stream_Unknown";
}

char* SmpStreamTypeToString(SmpStreamType type)
{
    for (uint32 i = 0; i < sizeof(g_smpStreamTypeStrArr) / sizeof(g_smpStreamTypeStrArr[0]); i++) {
        if (g_smpStreamTypeStrArr[i].type == type)
            return g_smpStreamTypeStrArr[i].str;
    }

    return "SmpStream_Unknown";
}

char* GetStreamTypeStrOf(StreamPath* path)
{
    if (path->smpDesc && path->smpDesc->distriType != PARALLEL_NONE) {
        return SmpStreamTypeToString(path->smpDesc->distriType);
    } else {
        return StreamTypeToString(path->type);
    }
}
