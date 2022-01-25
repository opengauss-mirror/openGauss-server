/* -------------------------------------------------------------------------
 *
 * streamplan_utils.cpp
 *	  functions related to stream plan for checking.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/optimizer/plan/streamplan_utils.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <math.h>
#include <pthread.h>
#include "access/transam.h"
#include "commands/explain.h"
#include "nodes/makefuncs.h"
#include "optimizer/cost.h"
#include "optimizer/dataskew.h"
#include "optimizer/pathnode.h"
#include "optimizer/planner.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/tlist.h"
#include "parser/parse_collate.h"
#include "parser/parse_coerce.h"
#include "parser/parse_clause.h"
#include "parser/parse_oper.h"
#include "parser/parse_relation.h"
#include "pgxc/groupmgr.h"
#include "pgxc/poolmgr.h"
#include "pgxc/pruningslice.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

/*
 * Description: return the op list in the plan
 * @param[IN] result_plan: input plan
 * @param[IN] check_eval: evaluation function for this op
 * @return : list of op in the plan.
 */
List* check_op_list_template(Plan* result_plan, List* (*check_eval)(Node*))
{
    List* res_list = NIL;

    res_list = check_eval((Node*)result_plan->targetlist);
    res_list = list_concat_unique(res_list, check_eval((Node*)result_plan->qual));
    switch (nodeTag(result_plan)) {
        case T_SeqScan:
        case T_CStoreScan:
        case T_DfsScan:
#ifdef ENABLE_MULTIPLE_NODES
        case T_TsStoreScan:
#endif   /* ENABLE_MULTIPLE_NODES */
        case T_ExtensiblePlan:
            break;
        case T_ForeignScan:
        case T_VecForeignScan: {
            ForeignScan* foreignScan = (ForeignScan*)result_plan;
            ForeignTable* ftbl = NULL;
            ForeignServer* fsvr = NULL;

            ftbl = GetForeignTable(foreignScan->scan_relid);
            AssertEreport(NULL != ftbl, MOD_OPT, "The foreign table is NULL");
            fsvr = GetForeignServer(ftbl->serverid);
            AssertEreport(NULL != fsvr, MOD_OPT, "The foreign server is NULL");

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
        case T_ModifyTable: {
            ModifyTable* splan = (ModifyTable*)result_plan;
            if (splan->upsertAction == UPSERT_UPDATE && splan->updateTlist != NULL) {
                res_list = list_concat_unique(res_list, check_eval((Node*)splan->updateTlist));
            }
        } break;
        default:
            break;
    }
    return res_list;
}

/*
 * @Descarption: Search node and check funcid > FirstNormalObjectId.
 * @in node: Current node.
 * @in varInfo: Var_info struct.
 */
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
List* check_func(Node* node)
{
    bool found = false;
    List* rs = NIL;

    (void)check_func_walker(node, &found);

    if (found)
        rs = lappend(rs, NULL);

    return rs;
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

/*
 * contains_specified_func:
 *	Walk through the node, search the func_expr specified by context->funcids, and
 * append the func_expr to context->func_expr
 *
 * @in node: the node that will be checked.
 * @in context->funcids: the func that will be searched in the node.
 * @in context->find_all: false means once find any func in context->funcids will return,
 * 					true means walk through all the branches to search context->funcids
 * @in/out context->func_expr: the func expr that we find will be append to the list.
 */
List* contains_specified_func(Node* node, contain_func_context* context)
{
    /* context->funcids is the func will be checked, it can't be NIL */
    AssertEreport(context->funcids != NIL, MOD_OPT, "The context funcids is NIL");

    (void)query_or_expression_tree_walker(node, (bool (*)())contains_specified_func_walker, (void*)context, 0);
    return context->func_exprs;
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
    if (NULL == path) {
        return;
    }

    /* Record if there's parameterized path */
    if (path->param_info) {
        Relids req_outer = bms_intersect(path->param_info->ppi_req_outer, context->outer_relids);
#ifdef ENABLE_MULTIPLE_NODES
        Bitmapset* req_upper = bms_union(path->param_info->ppi_req_upper, context->upper_params);
        if (!bms_is_empty(req_outer) || !bms_is_empty(req_upper)) {
#else
        if (!bms_is_empty(req_outer)) {
#endif
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
 * @Description: Recurse the whole plan tree to do some extra work.
 * 				 1. Search the plan tree to see if there is subplan/initplan
 *				 2. Change the plan with subplan/initplan to serialization.
 *
 * @param[IN] result_plan:  plan node
 * @param[IN] parent: parent plan of result_plan
 * @param[IN] cell: if the result_plan is in parent's plan list cell
 * @param[IN] is_left: if the result_plan is in parent's lefttree
 * @param[IN] initplans: initplans list
 * @param[IN] is_search: if we are do the seatch job or rollback job.
 * @return: there is a subplan/initplan
 */
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
            } else if (isIntergratedMachine == false) {
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

    if (NIL != plan_list) {
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
                    Assert(LOCAL_ROUNDROBIN == stream->smpDesc.distriType && IsA(result_plan->lefttree, CteScan));
                }
#ifdef ENABLE_MULTIPLE_NODES
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
#endif
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

void compute_stream_cost(StreamType type, char locator_type, double subrows, double subgblrows, double skew_ratio,
    int width, bool isJoin, List* distribute_keys, Cost* total_cost, double* gblrows, unsigned int producer_num_dn,
    unsigned int consumer_num_dn, ParallelDesc* smpDesc, List* ssinfo)
{
    double size, send_size, receive_size;
    double stream_ratio_send = 1.0;
    double stream_ratio_receive = 1.0;
    int sendDop = 1;
    int receiveDop = 1;
    SmpStreamType pa_stream = PARALLEL_NONE;
    Cost send_cost = 0;
    Cost receive_cost = 0;
    Cost send_data_cost = DEFAULT_SEND_KDATA_COST;
    Cost receive_data_cost = DEFAULT_RECEIVE_KDATA_COST;
    Cost cpu_hashcal_cost = DEFAULT_CPU_HASH_COST;
#ifdef ENABLE_MULTIPLE_NODES
    /* For the compatibility of original single installation nodegroup mode */
    if (producer_num_dn == 0)
        producer_num_dn = 1;
    else if (ng_is_all_in_installation_nodegroup_scenario())
        producer_num_dn = u_sess->pgxc_cxt.NumDataNodes;

    if (consumer_num_dn == 0)
        consumer_num_dn = 1;
    else if (ng_is_all_in_installation_nodegroup_scenario())
        consumer_num_dn = u_sess->pgxc_cxt.NumDataNodes;
#else
    producer_num_dn = 1;
    consumer_num_dn = 1;
#endif
    /* Get the dop of stream send side and receive side if parallel. */
    if (smpDesc != NULL) {
        sendDop = SET_DOP(smpDesc->producerDop);
        receiveDop = SET_DOP(smpDesc->consumerDop);
        pa_stream = smpDesc->distriType;
        /* Add the thead cost at consumer sides. */
        receive_cost += (receiveDop - 1) * u_sess->opt_cxt.smp_thread_cost;

        parallel_stream_info_print(smpDesc, type);
    }

    /*
     * There is two kinds of stream node:
     * 1. Transmit data through net. Including the original
     *    redistribute/broadcast node, and split
     *    redistribute/broadcast for smp.
     * 2. Transmit data througn memory. Incluuding local redistribute
     *    local broadcast, and local roundrobin for smp.
     * These two kinds of stream should have different
     * cost parameter with the same calculate formula.
     */
    if (STREAM_IS_LOCAL_NODE(pa_stream)) {
        send_data_cost = LOCAL_SEND_KDATA_COST;
        receive_data_cost = LOCAL_RECEIVE_KDATA_COST;

        /* local broadcast and local roundrobin do not need hash calculation. */
        if (LOCAL_BROADCAST == pa_stream || LOCAL_ROUNDROBIN == pa_stream)
            cpu_hashcal_cost = 0;
        else
            cpu_hashcal_cost = DEFAULT_CPU_HASH_COST;
    }

    ereport(DEBUG2,
        (errmodule(MOD_OPT),
            (errmsg("Stream cost: SMP INFO: sendDop: %d, receiveDop: %d, distriType: %d",
                sendDop,
                receiveDop,
                pa_stream))));

    /*
     * Time consumption is more for join than agg with less width according
     * to the test, and it may because time wait during join, so we just simulate
     * it through width change
     */
    if (isJoin) {
        width = ((width - 1) / 128 + 1) * 128;
#ifdef ENABLE_MULTIPLE_NODES
        stream_ratio_send = pow(u_sess->pgxc_cxt.NumDataNodes, (double)1 / 3);
        stream_ratio_receive = pow(u_sess->pgxc_cxt.NumDataNodes, (double)1 / 3);
#else
        stream_ratio_send = pow(1, (double)1 / 3);
        stream_ratio_receive = pow(1, (double)1 / 3);
#endif
    } else if (width <= 0) {
        width = 8;
    }
    /*
     * We count amount according to the network send and receive times becuase
     * we send the data in 8K unit
     */
    size = (subrows)*width / 8192.0;
    ereport(DEBUG1, (errmodule(MOD_OPT), (errmsg("Stream cost: data size: %lf", size))));

    /*
     * 	Don't consider latency
     *	The cost of stream is cost of broadcast or distribute one data node's data.
     */
    switch (type) {
        case STREAM_BROADCAST: {
            double broadcast_cost = 0.0;
            /*
             * The data in one send thread is size/sendDop, and we need to
             * broadcast to receiveDOP * u_sess->pgxc_cxt.NumDataNodes threads.
             */
            send_size = size / sendDop * receiveDop * consumer_num_dn * stream_ratio_send;
            receive_size = size * producer_num_dn * stream_ratio_receive;

            /* Cost of send and receive */
            send_cost += send_size * send_data_cost;
            receive_cost += receive_size * receive_data_cost;

            broadcast_cost = send_cost + receive_cost;
            broadcast_cost *= u_sess->attr.attr_sql.stream_multiple;

            ereport(DEBUG1,
                (errmodule(MOD_OPT),
                    (errmsg("Stream cost: broadcast. Send cost: %lf, Receive cost: %lf, "
                            "total_cost: %lf",
                        send_cost,
                        receive_cost,
                        broadcast_cost))));

            if (!u_sess->attr.attr_sql.enable_broadcast)
                broadcast_cost = Max(g_instance.cost_cxt.disable_cost, broadcast_cost * 1.0e4);

            *total_cost += broadcast_cost;

            /* We'll set path rows(used for join) as local rows and plan rows as global rows */
            *gblrows = ((isJoin) ? subgblrows : (subgblrows * consumer_num_dn));

            /* If SPLIT_BROADCAST, the data at receive size will increase. */
            *gblrows *= receiveDop;
        } break;
        case STREAM_REDISTRIBUTE: {
            double hash_cost = 0.0;
            double redistribute_cost = 0.0;
#ifdef ENABLE_MULTIPLE_NODES
            double nodegroup_stream_weight = ng_get_nodegroup_stream_weight(producer_num_dn, consumer_num_dn);
#else
            double nodegroup_stream_weight = 1.0;
#endif
            skew_ratio = (skew_ratio < 1.0) ? 1.0 : skew_ratio;

            if (!IsLocatorReplicated(locator_type) || pa_stream == LOCAL_BROADCAST) {
                if (LOCAL_BROADCAST != pa_stream) {
                    send_size = size / sendDop;
                    receive_size = size / receiveDop;
                } else {
                    send_size = size / sendDop * receiveDop;
                    receive_size = size;
                }

                /* Cost of send and receive */
                send_cost += send_size * send_data_cost * nodegroup_stream_weight;
                receive_cost += receive_size * receive_data_cost * skew_ratio * nodegroup_stream_weight;

                /* Hash calculate cost is related to the sending end. */
                hash_cost += list_length(distribute_keys) * subrows * cpu_hashcal_cost / sendDop;

                redistribute_cost = hash_cost + send_cost + receive_cost;
                redistribute_cost *= u_sess->attr.attr_sql.stream_multiple;

                ereport(DEBUG1,
                    (errmodule(MOD_OPT),
                        (errmsg("Stream cost: redistribute. Send cost: %lf, Receive cost: %lf, "
                                "total_cost: %lf",
                            send_cost,
                            receive_cost,
                            redistribute_cost))));

                *total_cost += redistribute_cost;
                *gblrows = subgblrows;
            } else {
                /* it should recompute total cost for hash filter and add hash filter expr in function stream_join_plan
                 */
                *total_cost += list_length(distribute_keys) * subrows * cpu_hashcal_cost;
                *gblrows = subgblrows;
            }

            if (LOCAL_BROADCAST == pa_stream) {
                *gblrows *= receiveDop;
                if (!u_sess->attr.attr_sql.enable_broadcast)
                    *total_cost += Max(g_instance.cost_cxt.disable_cost, redistribute_cost * 1.0e4);
            }
        } break;
        case STREAM_GATHER: {
            send_size = size;
            receive_size = size * producer_num_dn;

            send_cost += send_size * send_data_cost;
            receive_cost += receive_size * receive_data_cost;

            *total_cost = *total_cost + send_cost + receive_cost;
            *gblrows = subgblrows;
        } break;
        case STREAM_HYBRID: {
            AssertEreport(ssinfo != NULL, MOD_OPT, "The ssinfo is NULL");

            double hash_cost = 0.0;
            double filter_cost = 0.0;
            double hyrbid_cost = 0.0;
            double nodegroup_stream_weight = 1.0;
            double broadcast_send_size = 0.0;
            double broadcast_recv_size = 0.0;
            double broadcast_ratio = 0.0;
            QualCost qual_cost;
            ListCell* lc = NULL;

            qual_cost.per_tuple = 0;
            qual_cost.startup = 0;

            foreach (lc, ssinfo) {
                QualSkewInfo* qsinfo = (QualSkewInfo*)lfirst(lc);
                qual_cost.per_tuple += qsinfo->qual_cost.per_tuple;
                qual_cost.startup += qsinfo->qual_cost.startup;

                if (qsinfo->skew_stream_type == PART_REDISTRIBUTE_PART_BROADCAST ||
                    qsinfo->skew_stream_type == PART_LOCAL_PART_BROADCAST) {
                    broadcast_ratio = qsinfo->broadcast_ratio;
                }
            }

            /* Calculate stream cost for broadcast data. */
            broadcast_send_size = size * broadcast_ratio / sendDop * receiveDop * consumer_num_dn;
            broadcast_recv_size = size * broadcast_ratio * producer_num_dn;
            send_cost += broadcast_send_size * send_data_cost;
            receive_cost += broadcast_recv_size * receive_data_cost;

            /* Calculate stream cost for rest data. */
            send_size = size * (1.0 - broadcast_ratio) / sendDop;
            receive_size = size * (1.0 - broadcast_ratio) / receiveDop;

            /* For PART_LOCAL_PART_BROADCAST, mostly data is send to local consumer. */
            if (distribute_keys == NIL) {
                send_data_cost = LOCAL_SEND_KDATA_COST;
                receive_data_cost = LOCAL_RECEIVE_KDATA_COST;
                cpu_hashcal_cost = 0;
            } else {
#ifdef ENABLE_MULTIPLE_NODES
                nodegroup_stream_weight = ng_get_nodegroup_stream_weight(producer_num_dn, consumer_num_dn);
#else
                nodegroup_stream_weight = 1.0;
#endif
            }

            /* Cost of send and receive */
            send_cost += send_size * send_data_cost * nodegroup_stream_weight;
            receive_cost += receive_size * receive_data_cost * nodegroup_stream_weight;

            /* Hash calculate cost is related to the sending end. */
            hash_cost = list_length(distribute_keys) * subrows * (1 - broadcast_ratio) * cpu_hashcal_cost / sendDop;
            filter_cost = subrows * qual_cost.per_tuple / sendDop + qual_cost.startup;
            hyrbid_cost = hash_cost + filter_cost + send_cost + receive_cost;
            hyrbid_cost *= u_sess->attr.attr_sql.stream_multiple;

            *total_cost += hyrbid_cost;
            *gblrows = subgblrows * broadcast_ratio * receiveDop * consumer_num_dn + subgblrows * (1 - broadcast_ratio);
        } break;
        default:
            break;
    }
    return;
}

/*
 * contain_special_plan_node: check if there are planTag nodes under plan
 *
 * @param: (in) plan
 *     the plan node
 * @param: (in) planTag
 *     the target plan tag
 * @param: (in) mode
 *     the search mode (see more in ContainPlanNodeMode)
 *
 * @return:
 *     wheather the target plan node is found, true means found
 */
bool contain_special_plan_node(Plan* plan, NodeTag planTag, ContainPlanNodeMode mode)
{
    if (plan == NULL)
        return false;
    else if (nodeTag(plan) == planTag) {
        if (IsA(plan, ForeignScan)) {
            ForeignScan* fScan = (ForeignScan*)plan;
            if (IS_OBS_CSV_TXT_FOREIGN_TABLE(fScan->scan_relid)) {
                return true;
            }
        } else {
            return true;
        }
    } else if (IsA(plan, ModifyTable)) {
        ModifyTable* mt = (ModifyTable*)plan;
        ListCell* cell = NULL;

        if ((CPLN_ONE_WAY & mode) && list_length(mt->plans) > 1) {
            return false;
        }

        foreach (cell, mt->plans) {
            if (contain_special_plan_node((Plan*)lfirst(cell), planTag, mode))
                return true;
        }
    } else if (IsA(plan, Append)) {
        ListCell* cell = NULL;

        if ((CPLN_ONE_WAY & mode) && list_length(((Append*)plan)->appendplans) > 1) {
            return false;
        }

        foreach (cell, ((Append*)plan)->appendplans) {
            if (contain_special_plan_node((Plan*)lfirst(cell), planTag, mode))
                return true;
        }
    } else if (IsA(plan, MergeAppend)) {
        ListCell* cell = NULL;

        if ((CPLN_ONE_WAY & mode) && list_length(((MergeAppend*)plan)->mergeplans) > 1) {
            return false;
        }

        foreach (cell, ((MergeAppend*)plan)->mergeplans) {
            if (contain_special_plan_node((Plan*)lfirst(cell), planTag, mode))
                return true;
        }
    } else if (IsA(plan, SubqueryScan)) {
        if (contain_special_plan_node(((SubqueryScan*)plan)->subplan, planTag, mode))
            return true;
    } else if (IsA(plan, CteScan)) {
        if (planTag == T_Stream && ((CteScan*)plan)->subplan != NULL) {
            if (((CteScan*)plan)->subplan->has_inner_stream || ((CteScan*)plan)->subplan->has_outer_stream)
                return true;
        }
    } else if (IsA(plan, Material) && ((Material*)plan)->materialize_all) {
        return false;
    } else {
        if ((CPLN_ONE_WAY & mode) && NULL != plan->lefttree && NULL != plan->righttree) {
            return false;
        }

        if (contain_special_plan_node(plan->lefttree, planTag, mode))
            return true;

        if (CPLN_NO_IGNORE_MATERIAL & mode) {
            if (contain_special_plan_node(plan->righttree, planTag, mode)) {
                return true;
            }
        } else {
            /*
             * If Nestloop is set material all, right tree should be only executed once,
             * so skip the check of the right tree then.
             */
            bool material_all = IsA(plan, NestLoop) && ((NestLoop*)plan)->materialAll && IsA(plan->righttree, Material);
            if (!material_all && contain_special_plan_node(plan->righttree, planTag, mode))
                return true;
        }
    }
    return false;
}

/*
 * check if the type cast does not change distribution
 * if the type cast change the hash func or the hash value
 * we have to add an extra distribution to put the casted
 * value to the right datanode.
 *
 * example:
 *
 *    create table t_cast_redis(a int, b int);
 *    insert into t_cast_redis values (generate_series(1,1000));
 *    analyze t_cast_redis;
 *    select * from t_cast_redis t1, t_cast_redis t2 where t1.a=t2.a::boolean;
 *
 *
 *								   QUERY PLAN
 * -------------------------------------------------------------------------------
 * Streaming (type: GATHER)	(cost=6.25..16.75 rows=100 width=16)
 *	Output: t1.a, t1.b, t2.a, t2.b
 *	Node/s: All datanodes
 *	->	Hash Join  (cost=3.12..12.06 rows=100 width=16)
 *		  Output: t1.a, t1.b, t2.a, t2.b
 *		  Hash Cond: ((((t2.a)::boolean)::bigint) = t1.a)
 *		  ->  Streaming(type: REDISTRIBUTE)  (cost=0.00..7.88 rows=100 width=8)
 *				Output: t2.a, t2.b, (((t2.a)::boolean)::bigint)   --should redis even if column a is the distribute key
 *				Distribute Key: (((t2.a)::boolean)::bigint)
 *				Spawn on: All datanodes
 *				->	Seq Scan on public.t t2  (cost=0.00..2.50 rows=100 width=8)
 *					  Output: t2.a, t2.b, (t2.a)::boolean
 *		  ->  Hash	(cost=2.50..2.50 rows=100 width=8)
 *				Output: t1.a, t1.b
 *				->	Seq Scan on public.t t1  (cost=0.00..2.50 rows=100 width=8)
 *					  Output: t1.a, t1.b
 *
 */
bool is_type_cast_hash_compatible(FuncExpr* func)
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

Oid get_hash_type(Oid type_in)
{
    switch (type_in) {
        /* Int8 hash arithmetic is compatible in int4, so we return same type. */
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

/*
 * An aggregate with ORDER BY, DISTINCT directives need to be
 * computed at Coordinator using all the rows. An aggregate
 * without collection function needs to be computed at
 * Coordinator.
 * Polymorphic transition types need to be resolved to
 * correctly interpret the transition results from Datanodes.
 * For now compute such aggregates at Coordinator.
 */
static bool is_agg_unsupport_dn_compute(Node* node)
{
    Aggref* aggref = (Aggref*)node;
    /* DISTINCT can be computed at datanode if is stream plain */
    if (IS_STREAM_PLAN) {
        if (aggref->aggorder || aggref->agglevelsup ||
            (!aggref->agghas_collectfn && need_adjust_agg_inner_func_type(aggref)) ||
            IsPolymorphicType(aggref->aggtrantype)) {
#ifndef ENABLE_MULTIPLE_NODES
                errno_t sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
                    NOTPLANSHIPPING_LENGTH, "aggregate %d is not supported in stream plan", aggref->aggfnoid);
                securec_check_ss_c(sprintf_rc, "\0", "\0");
                mark_stream_unsupport();
#endif
                return true;
        }
    } else {
        if (aggref->aggorder || aggref->aggdistinct || aggref->agglevelsup || !aggref->agghas_collectfn ||
            IsPolymorphicType(aggref->aggtrantype)) {
                return true;
        }
    }
    return false;
}

/*
 * return true if node cannot be evaluatated in foreign server.
 */
bool foreign_qual_walker(Node* node, foreign_qual_context* context)
{
    bool ret_val = false;
    bool saved_collect_vars = false;
    if (node == NULL)
        return false;

    switch (nodeTag(node)) {
        case T_ExprState:
            return foreign_qual_walker((Node*)((ExprState*)node)->expr, NULL);
        case T_GroupingFunc: {
            if (context != NULL) {
                GroupingFunc* groupingFun = (GroupingFunc*)node;

                saved_collect_vars = context->collect_vars;
                context->collect_vars = false;
                context->aggs = lappend(context->aggs, groupingFun);
            }
        } break;
        case T_Aggref: {
            Aggref* aggref = (Aggref*)node;
            if (is_agg_unsupport_dn_compute(node)) {
                return true;
            }
            /*
             * Datanode can compute transition results, so, add the
             * aggregate to the context if context is present
             */
            if (context != NULL) {
                /*
                 * Don't collect VARs under the Aggref node. See
                 * pgxc_process_grouping_targetlist() for details.
                 */
                saved_collect_vars = context->collect_vars;
                context->collect_vars = false;
                context->aggs = lappend(context->aggs, aggref);
            }
        } break;
        case T_Var:
            if (context != NULL && context->collect_vars)
                context->vars = lappend(context->vars, node);
            break;

        default:
            break;
    }

    ret_val = expression_tree_walker(node, (bool (*)())foreign_qual_walker, context);

    /*
     * restore value of collect_vars in the context, since we have finished
     * traversing tree rooted under and Aggref node
     */
    if (context && IsA(node, Aggref))
        context->collect_vars = saved_collect_vars;

    return ret_val;
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

/*
 * The function returns all the exprs that func_oid > FirstNormalObjectId
 * for the input plan.
 */
List* check_func_list(Plan* result_plan)
{
    return check_op_list_template(result_plan, (List* (*)(Node*)) check_func);
}

/*
 * @Description: Wrapper to passin default parameter for function pointer,
 *               This function will call check_subplan_expr with default input.
 * @param[IN] node: input node.
 * @return: subplan list.
 */
List* check_subplan_expr_default(Node* node)
{
    return check_subplan_expr(node);
}

/* The function returns all the subplan exprs for the input plan */
List* check_subplan_list(Plan* result_plan)
{
    return check_op_list_template(result_plan, (List* (*)(Node*)) check_subplan_expr_default);
}

/*
 * The function returns all the exprs that vartype > FirstNormalObjectId
 * for the input plan.
 */
List* check_vartype_list(Plan* result_plan)
{
    return check_op_list_template(result_plan, (List* (*)(Node*)) check_vartype);
}

static void check_plannerInfo(PlannerInfo* root, Index result_rte_idx)
{
    if (root->simple_rte_array == NULL) {
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                errmsg("simple_rte_array is NULL unexpectedly")));
    }
    if (root->simple_rte_array[result_rte_idx] == NULL) {
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                errmsg("simple_rte_array[result_rte_idx] is NULL unexpectedly")));
    }
}

static bool judge_lockrows_need_redistribute_keyLen_equal(
    PlannerInfo* root, Plan* subplan, Form_pgxc_class target_classForm, Index result_rte_idx)
{
    check_plannerInfo(root, result_rte_idx);
    bool need_redistribute = false;
    Var* lockrelvar = NULL;
    Form_pg_attribute attTup = NULL;
    Relation rel = heap_open(root->simple_rte_array[result_rte_idx]->relid, AccessShareLock);
    int disKeyLen = target_classForm->pcattnum.dim1;

    /* In natural shuffle scene, we need use redistribute to send back the data to original node. */
    if (!ng_is_same_group(rel->rd_locator_info->nodeList, (ng_get_dest_execnodes(subplan))->nodeList)) {
        heap_close(rel, AccessShareLock);
        return true;
    }
    for (int i = 0; i < disKeyLen; i++) {
        AttrNumber distributeKeyIdx = target_classForm->pcattnum.values[i];
        Node* subKey = (Node*)list_nth(subplan->distributed_keys, i);
        Var* subVar = locate_distribute_var((Expr*)subKey);
        attTup = rel->rd_att->attrs[distributeKeyIdx - 1];
        /*
         * Call pfree_ext to free the lockrelvar created in this loop and
         * we don't need check null outside as its check inside.
         */
        pfree_ext(lockrelvar);
        /*
         * Unlike judge_dml_need_redistribute, we just compare the target table's
         * distribute key with subplan's key but not check the targetlist.
         * Make var of target table for judge_node_compatible to check whether
         * need redistribute in FOR UPDATE/SHARE.
         */
        lockrelvar = (Var*)makeVar(
            result_rte_idx, distributeKeyIdx, attTup->atttypid, attTup->atttypmod, attTup->attcollation, 0);
        /* Check if current key is exactly equal */
        if (subVar != NULL && subVar->varno == result_rte_idx && subVar->varattno == distributeKeyIdx)
            continue;

        /* Check if current key from the same equivalence class */
        if (subVar != NULL && lockrelvar != NULL && judge_node_compatible(root, (Node*)subVar, (Node*)lockrelvar))
            continue;

        need_redistribute = true;
        break;
    }

    if (!need_redistribute && IsLocatorDistributedBySlice(rel->rd_locator_info->locatorType)) {
        if (subplan->exec_nodes == NULL || 
            !IsSliceInfoEqualByOid(subplan->exec_nodes->rangelistOid, rel->rd_locator_info->relid)) {
            need_redistribute = true;
        } 
    }

    heap_close(rel, AccessShareLock);
    return need_redistribute;
}

/*
 * @Description: check whether need redistribute for FOR UPDATE/SHARE.
 * @in root: planner info of current query level
 * @in subplan: subplan that need to check with target table
 * @in target_classForm: distribute info of target table
 * @result_rte_idx: range table entry index of target table
 * @return : true if redistribute is needed, else false
 */
bool judge_lockrows_need_redistribute(
    PlannerInfo* root, Plan* subplan, Form_pgxc_class target_classForm, Index result_rte_idx)
{
    bool need_redistribute = false;
    int disKeyLen = target_classForm->pcattnum.dim1;
    int subKeyLen = list_length(subplan->distributed_keys);

    /*
     * We use lock table's distribute key as redistribute key when add redistribute
     * node for lockrows. but tables without real distribute key cannot do accurate
     * redistribution.
     * e.g. data of replicated table may be redistribute to other node with the redistribute
     * key of one of it's column, but it is very difficult to redistribute the data back
     * without the table's distribute key.
     */
    if (target_classForm->pclocatortype != LOCATOR_TYPE_HASH &&
        !IsLocatorDistributedBySlice(target_classForm->pclocatortype))
        ereport(ERROR,
            (errmodule(MOD_OPT),
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Unsupported FOR UPDATE/SHARE non-hash/range/list table in stream plan."))));

    if (disKeyLen != subKeyLen) {
        need_redistribute = true;
    } else {
        need_redistribute =
            judge_lockrows_need_redistribute_keyLen_equal(root, subplan, target_classForm, result_rte_idx);
    }

    return need_redistribute;
}

/*
 * @Description: Set can_push flag of the query
 *
 * @in query - query tree.
 * @return : void.
 */
void mark_query_canpush_flag(Node *query)
{
    if (query == NULL) {
        return;
    }

    errno_t rc = EOK;
    shipping_context spCtxt;

    rc = memset_s(&spCtxt, sizeof(spCtxt), 0, sizeof(spCtxt));
    securec_check(rc, "\0", "\0");
    spCtxt.is_randomfunc_shippable = u_sess->opt_cxt.is_randomfunc_shippable && IS_STREAM_PLAN;
    spCtxt.is_ecfunc_shippable = true;
    spCtxt.query_list = NIL;
    spCtxt.query_count = 0;
    spCtxt.query_shippable = true;
    spCtxt.current_shippable = true;
    spCtxt.global_shippable = true;
    (void)stream_walker(query, &spCtxt);
}

/*
 * Delete useless local stream node.
 *
 * @param[IN] stream_plan: plan need to be check.
 * @param[IN] parent: parant plan of stream.
 * @param[IN] lc: subplan list cell.
 * return bool: removed -- true, not removed -- false.
 */
bool remove_local_plan(Plan* stream_plan, Plan* parent, ListCell* lc, bool is_left)
{
    Stream* stream = NULL;

    if (stream_plan == NULL || parent == NULL)
        return false;

#ifndef ENABLE_MULTIPLE_NODES
    /* Plan who has initPlan can not be removed. */
    if (stream_plan->initPlan) {
        return false;
    }
#endif

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

/*
 * Description: if RANDOM/GS_ENCRYPT_AES128 function exist in the plan,
 * return the node list.
 */
List* check_random_expr(Plan* result_plan)
{
    List* random_list = check_op_list_template(result_plan, (List *(*)(Node*)) check_random_expr);
    return random_list;
}

static void set_bucketmap_index_by_group(ExecNodes *exec_node, NodeGroupInfoContext *node_group_info_context,
    Oid groupoid)
{
    char *group_name = get_pgxc_groupname(groupoid);
    if (group_name == NULL)
        return;

    char *installation_group_name = pstrdup(PgxcGroupGetInstallationGroup());
    char *group_parent_name = NULL;
    int current_num_bucketmaps = node_group_info_context->num_bucketmaps;
    int bucketlen;
    if (strncmp(group_name, installation_group_name, NAMEDATALEN) == 0 ||
        (((group_parent_name = get_pgxc_groupparent(groupoid)) != NULL) &&
        strncmp(group_parent_name, installation_group_name, NAMEDATALEN) == 0)) {
        node_group_info_context->groupOids[current_num_bucketmaps] = groupoid;
        node_group_info_context->bucketMap[current_num_bucketmaps] = BucketMapCacheGetBucketmap(group_name, &bucketlen);
        node_group_info_context->bucketCnt[current_num_bucketmaps] = bucketlen;
        exec_node->bucketmapIdx = node_group_info_context->num_bucketmaps;
        node_group_info_context->num_bucketmaps++;
    } else {
        char in_redistribution = get_pgxc_group_redistributionstatus(groupoid);
        if ('y' != in_redistribution) {
            char *group_parent = get_pgxc_groupparent(groupoid);
            if (group_parent != NULL) {
                Oid group_parent_oid = get_pgxc_groupoid(group_parent, false);
                in_redistribution = get_pgxc_group_redistributionstatus(group_parent_oid);
            }
        }
        uint2 *bucketmap = BucketMapCacheGetBucketmap(group_name, &bucketlen);
        if ('y' == in_redistribution) {
            node_group_info_context->groupOids[current_num_bucketmaps] = groupoid;
            node_group_info_context->bucketMap[current_num_bucketmaps] = bucketmap;
            node_group_info_context->bucketCnt[current_num_bucketmaps] = bucketlen;
            exec_node->bucketmapIdx = node_group_info_context->num_bucketmaps;
            node_group_info_context->num_bucketmaps++;
        } else {
            exec_node->bucketmapIdx = (int)(BUCKETMAP_DEFAULT_INDEX_BIT | bucketlen);
        }
    }
    AssertEreport(node_group_info_context->num_bucketmaps <= MAX_SPECIAL_BUCKETMAP_NUM, MOD_OPT,
        "The number of bucketmaps exceeded the max special bucketmap num");
    pfree_ext(group_name);
    pfree_ext(group_parent_name);
    pfree_ext(installation_group_name);
}

/*
 * record the bucketmap and the index of bucketmap.
 */
static void set_bucketmap_index(ExecNodes* exec_node, NodeGroupInfoContext* node_group_info_context)
{
    if (exec_node == NULL)
        return;

    exec_node->bucketmapIdx = (int)(BUCKETMAP_DEFAULT_INDEX_BIT | BUCKETDATALEN);
    Oid groupoid = exec_node->distribution.group_oid;

    if (groupoid == InvalidOid)
        return;

    for (int i = 0; i < node_group_info_context->num_bucketmaps; i++) {
        if (node_group_info_context->groupOids[i] == groupoid) {
            exec_node->bucketmapIdx = i;
            return;
        }
    }
    set_bucketmap_index_by_group(exec_node, node_group_info_context, groupoid);
}

static bool is_execute_on_multinodes(Plan* plan)
{
    return (plan->exec_nodes && list_length(plan->exec_nodes->nodeList) != 1);
}

/*
 * we can get the groupoid from execnode of plan,
 * if the group is the installation group or the group is in redistribution,
 * we need record the bucketmap and the index of bucketmap.
 *
 * @param[IN] plan: plan need to be check.
 * @param[IN] node_group_info_context: record the bucketmap
 */
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

/*
 * finalize_node_id
 *      To finalize node id and parent node id for result plan. The sequence of plan node id doesn't
 *      mean anything. We just keep it unique among entire plan.
 *      Also, we determine the plan node id of each subplan, which is the top plan node id of thread
 *      that executes the subplan, and make proper change to subplans. Currently, we only support
 *      that one subplan only executes in one thread, and we don't support main query and subplan
 *      block executes on different cn and dn side.
 *      Not sure how to utilize the plan node id and parent node id to do communication in future,
 *      so use the simplest way to finalize it.
 *
 *      Parameters:
 *       result_plan: the main query plan to handle.
 *       plan_node_id: the global counter to assign plan node id for each plan node
 *       parent_node_id: the recorded plan node id for the plan node above current plan node
 *       num_streams: global counter to record how many streams are under gather node
 *       num_plannodes: global counter to record how many plan nodes in total
 *       total_num_streams: global counter to record how many streams in total
 *       subplans: subplan list for the top plan (including cte, correlated and uncorrelated subplan)
 *       subroots: planner info list for each subplan, has the same number of memebers as subplans
 *       initplans: global init plans to find if a subplan is uncorrelated
 *       subplan_ids: top plan node id list of thread that executes the subplan
 *       is_under_stream: flag if current plan is under stream/gather node
 *       is_data_node_exec: flag to judge if plan with all-node are executed on data_nodes or cn
 */
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
            /* set the index of bucketmap */
            set_bucketmap_index(result_plan, node_group_info_context);
        }
        if (is_under_stream)
            subplan_ids[0] = *plan_node_id;

        *parent_node_id = *plan_node_id;

        int save_parent_id = *parent_node_id;
        (*plan_node_id)++;
        (*num_plannodes)++;

        /*
         * Since we replace cte with parse tree, we don't need cte sublink any more.
         * Specifically, we don't support expressions like values(cte), so make it stream unsupport
         */
        if (result_plan->initPlan && IS_STREAM_PLAN) {
            List* cteLinkList = NIL;
            ListCell* lc = NULL;
            foreach (lc, result_plan->initPlan) {
                SubPlan* plan = (SubPlan*)lfirst(lc);
                if (plan->subLinkType == CTE_SUBLINK)
                    cteLinkList = lappend(cteLinkList, plan);
            }
            if (cteLinkList != NIL) {
                if (IsA(result_plan, ValuesScan)) {
                    sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
                        NOTPLANSHIPPING_LENGTH, "ValuesScan in CTE SubLink can't be shipped");
                    securec_check_ss_c(sprintf_rc, "\0", "\0");
                    mark_stream_unsupport();
                }
            }

            if (IS_STREAM_PLAN)
                *initplans = list_concat(*initplans, list_copy(result_plan->initPlan));
        }

        switch (nodeTag(result_plan)) {
            case T_Append:
            case T_VecAppend: {
                Append* append = (Append*)result_plan;
                ListCell* lc = NULL;
                foreach (lc, append->appendplans) {
                    Plan* plan = (Plan*)lfirst(lc);
                    finalize_node_id(plan, plan_node_id, parent_node_id, num_streams, num_plannodes, total_num_streams,
                        max_push_sql_num, gather_count, subplans, subroots, initplans, subplan_ids, false,
                        is_under_ctescan, is_data_node_exec, is_read_only, node_group_info_context);
                    *parent_node_id = save_parent_id;
                }
            } break;
            case T_CteScan: {
                if (STREAM_RECURSIVECTE_SUPPORTED) {
                    CteScan* cte_plan = (CteScan*)result_plan;
                    int subplanid = cte_plan->ctePlanId;

                    /* mark unsupport if CteScan nested in another CteScan */
                    if (is_under_ctescan) {
                        sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
                            NOTPLANSHIPPING_LENGTH,
                            "With-Recursive CteScan runs as subplan nested in another CteScan");
                        securec_check_ss_c(sprintf_rc, "\0", "\0");
                        mark_stream_unsupport();
                    }

                    /* Fetch the cte's underlying subplan(RecursiveUnion) */
                    RecursiveUnion* ru_plan = (RecursiveUnion*)list_nth(subplans, cte_plan->ctePlanId - 1);
                    if (IsA(ru_plan, RecursiveUnion)) {
                        /* Do plan node finalization into subplan plantree (RecursiveUnion) */
                        finalize_node_id((Plan*)ru_plan, plan_node_id, parent_node_id, num_streams, num_plannodes,
                            total_num_streams, max_push_sql_num, gather_count, subplans, subroots, initplans,
                            subplan_ids, false, is_under_ctescan, is_data_node_exec, is_read_only,
                            node_group_info_context);
                    } else
                        break;
                    /*
                     * Note, the recursive-cte processing (stream mode), RecursiveUnion
                     * operator is processed in a way like SubPlan initialization, we just
                     * record the recursive-union's top node planid in subplan_ids array
                     */
                    subplan_ids[subplanid] = subplan_ids[0];
                    *parent_node_id = save_parent_id;
                }

                /* Mainly for cte that execute on CN. */
                if (IsExplainPlanStmt) {
                    /* Fetch the cte's underlying subplan. */
                    CteScan* cte_plan = (CteScan*)result_plan;
                    Plan* ru_plan = (Plan*)list_nth(subplans, cte_plan->ctePlanId - 1);

                    /*
                     * Set plan node id for cte that can not be push down.
                     * For with recursive cte, if it can not be push down, we only collect plan info till ctescan.
                     * For cte that execute on cn, we should collect all nodes info in cte.
                     */
                    if (!IsA(ru_plan, RecursiveUnion)) {
                        finalize_node_id((Plan*)ru_plan, plan_node_id, parent_node_id, num_streams, num_plannodes,
                            total_num_streams, max_push_sql_num, gather_count, subplans, subroots, initplans,
                            subplan_ids, false, is_under_ctescan, is_data_node_exec, is_read_only,
                            node_group_info_context);
                    }
                    *parent_node_id = save_parent_id;
                }
            } break;
            case T_ModifyTable:
            case T_VecModifyTable: {
                ModifyTable* mt = (ModifyTable*)result_plan;
                ListCell* lc = NULL;
                foreach (lc, mt->plans) {
                    Plan* plan = (Plan*)lfirst(lc);
                    finalize_node_id(plan, plan_node_id, parent_node_id, num_streams, num_plannodes, total_num_streams,
                        max_push_sql_num, gather_count, subplans, subroots, initplans, subplan_ids, false,
                        is_under_ctescan, is_data_node_exec, is_read_only, node_group_info_context);
                    *parent_node_id = save_parent_id;
                }
            } break;
            case T_SubqueryScan:
            case T_VecSubqueryScan: {
                SubqueryScan* ss = (SubqueryScan*)result_plan;
                if (ss->subplan) {
                    /* Check the SubqueryScan whether will be reduced in set_plan_reference. */
                    if (IsExplainPlanStmt && trivial_subqueryscan(ss)) {
                        /*
                         * For explain plan, we must make sure num_plannodes and plan_node_id is absolutely correct.
                         * So, if SubqueryScan will be reduced, we reduce the num_plannodes too.
                         */
                        (*plan_node_id)--;
                        (*num_plannodes)--;
                        finalize_node_id(ss->subplan, plan_node_id, parent_node_id, num_streams, num_plannodes,
                            total_num_streams, max_push_sql_num, gather_count, subplans, subroots, initplans,
                            subplan_ids, false, is_under_ctescan, is_data_node_exec, is_read_only,
                            node_group_info_context);
                    } else {
                        finalize_node_id(ss->subplan, plan_node_id, parent_node_id, num_streams, num_plannodes,
                            total_num_streams, max_push_sql_num, gather_count, subplans, subroots, initplans,
                            subplan_ids, false, is_under_ctescan, is_data_node_exec, is_read_only,
                            node_group_info_context);
                    }
                    *parent_node_id = save_parent_id;
                }
            } break;
            case T_MergeAppend:
            case T_VecMergeAppend: {
                MergeAppend* ma = (MergeAppend*)result_plan;
                ListCell* lc = NULL;
                foreach (lc, ma->mergeplans) {
                    Plan* plan = (Plan*)lfirst(lc);
                    finalize_node_id(plan, plan_node_id, parent_node_id, num_streams, num_plannodes, total_num_streams,
                        max_push_sql_num, gather_count, subplans, subroots, initplans, subplan_ids, false,
                        is_under_ctescan, is_data_node_exec, is_read_only, node_group_info_context);
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
                    finalize_node_id(result_plan->lefttree, plan_node_id, parent_node_id, num_streams, num_plannodes,
                    total_num_streams, max_push_sql_num, gather_count, subplans, subroots, initplans, subplan_ids,
                    true, is_under_ctescan, is_data_node_exec, is_read_only, node_group_info_context);
                    subplan_ids[0] = saved_plan_id;
                    *parent_node_id = save_parent_id;
                }
                if (result_plan->righttree) {
                    finalize_node_id(result_plan->righttree, plan_node_id, parent_node_id, num_streams, num_plannodes,
                        total_num_streams, max_push_sql_num, gather_count, subplans, subroots, initplans, subplan_ids,
                        true, is_under_ctescan, is_data_node_exec, is_read_only, node_group_info_context);
                    subplan_ids[0] = saved_plan_id;
                    *parent_node_id = save_parent_id;
                }
                if (GATHER == rq->position) {
                    rq->num_stream = *num_streams;
                    /* We adjusted exec_nodes for subplans in multi node group case, so also adjust max execnodes */
                    if (IS_STREAM_PLAN && subplans != NIL)
                        rq->exec_nodes = get_plan_max_ExecNodes(result_plan->lefttree, subplans);
                    (*num_streams) = 0;
                }
                if (!rq->is_simple)
                    (*max_push_sql_num)++;

                /* mark num_gather include scan_gather plan_router gather in all plan */
                rq->num_gather = *gather_count;
            } break;
            case T_Stream:
            case T_VecStream: {
                Stream* stream = (Stream*)result_plan;
                int saved_plan_id = subplan_ids[0];
                (*num_streams)++;
                (*total_num_streams)++;

                finalize_node_id(result_plan->lefttree, plan_node_id, parent_node_id, num_streams, num_plannodes,
                    total_num_streams, max_push_sql_num, gather_count, subplans, subroots, initplans, subplan_ids,
                    true, is_under_ctescan, is_data_node_exec, is_read_only, node_group_info_context);
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
                    finalize_node_id(plan, plan_node_id, parent_node_id, num_streams, num_plannodes, total_num_streams,
                        max_push_sql_num, gather_count, subplans, subroots, initplans, subplan_ids, false,
                        is_under_ctescan, is_data_node_exec, is_read_only, node_group_info_context);
                    *parent_node_id = save_parent_id;
                }
            } break;

            case T_BitmapOr:
            case T_CStoreIndexOr: {
                BitmapOr* bo = (BitmapOr*)result_plan;
                ListCell* lc = NULL;
                foreach (lc, bo->bitmapplans) {
                    Plan* plan = (Plan*)lfirst(lc);
                    finalize_node_id(plan, plan_node_id, parent_node_id, num_streams, num_plannodes, total_num_streams,
                        max_push_sql_num, gather_count, subplans, subroots, initplans, subplan_ids, false,
                        is_under_ctescan, is_data_node_exec, is_read_only, node_group_info_context);
                    *parent_node_id = save_parent_id;
                }
            } break;
            case T_ExtensiblePlan: {
                ListCell* lc = NULL;
                foreach (lc, ((ExtensiblePlan*)result_plan)->extensible_plans) {
                    Plan* plan = (Plan*)lfirst(lc);
                    finalize_node_id(plan, plan_node_id, parent_node_id, num_streams, num_plannodes, total_num_streams,
                    max_push_sql_num, gather_count, subplans, subroots, initplans, subplan_ids, false, is_under_ctescan,
                    is_data_node_exec, is_read_only, node_group_info_context);
                    *parent_node_id = save_parent_id;
                }
            } break;
            default:

                if (result_plan->lefttree) {
                    finalize_node_id(result_plan->lefttree, plan_node_id, parent_node_id, num_streams, num_plannodes,
                        total_num_streams, max_push_sql_num, gather_count, subplans, subroots, initplans, subplan_ids,
                        false, is_under_ctescan, is_data_node_exec, is_read_only, node_group_info_context);
                    *parent_node_id = save_parent_id;
                }
                if (result_plan->righttree) {
                    finalize_node_id(result_plan->righttree, plan_node_id, parent_node_id, num_streams, num_plannodes,
                        total_num_streams, max_push_sql_num, gather_count, subplans, subroots, initplans, subplan_ids,
                        false, is_under_ctescan, is_data_node_exec, is_read_only, node_group_info_context);
                    *parent_node_id = save_parent_id;
                }
                break;
        }
        /*
         * We handle subplans in following codes, from several aspects:
         * (1) Set the owner of each subplan in subplan_ids array
         * (2) Mark stream unsupport if main query and subplan comes from cn and dn respectively (exception (3))
         * (3) For initplan, if main query is from cn, just gather the subplan
         * (4) vectorize subplan and set plan references
         */
        if (IS_STREAM_PLAN) {
            /* Block pushing down Random()/GS_ENCRYPT_AES128() in Replicated plan temporarily */
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
                    /* this is for the case that initplan hidden in testexpr of subplan */
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
                /*
                 * check if subplan has been finalized. if it already belongs to other thread,
                 * then mark stream unsupport. else skip finalization
                 */
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
                    /*
                     * subplan on dn and main plan on cn. In such case, we only
                     * support initplan, and gather the result to cn
                     */
                    if (is_execute_on_coordinator(result_plan) ||
                        (is_execute_on_allnodes(result_plan) && !is_data_node_exec)) {
                        Plan* child_plan = NULL;
                        ListCell* lc2 = NULL;
                        int i = subplan->plan_id - 1;
                        PlannerInfo* subroot = (PlannerInfo*)list_nth(subroots, i);

                        if (is_execute_on_datanodes(plan)) {
                            if (IsA(node, Param) && !subroot->is_correlated) {
                                List *child_init_plan = NULL;
                                if (IsA(plan, Stream)) {
                                    child_plan = outerPlan(plan);
                                    child_init_plan = plan->initPlan;
                                } else {
                                    child_plan = plan;
                                }

                                plan = make_simple_RemoteQuery(child_plan, subroot, false);

                                plan->initPlan = child_init_plan;
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
                        } else if (is_execute_on_coordinator(plan)) {
                            if (subroot->is_correlated && contain_special_plan_node(plan, T_RemoteQuery)) {
                                subplan_ids[subplan->plan_id] = 0;
                                sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
                                    NOTPLANSHIPPING_LENGTH,
                                    "main plan exec on CN and SubPlan containing RemoteQuery exec on "
                                    "CN can't be shipped");
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
                    /* we only need to push down partial datanodes execution */
                    else if (!is_replicated_plan(result_plan) ||
                             (result_plan->exec_nodes && result_plan->exec_nodes->nodeList != NIL)) {
                        /* For init plan, we should add broadcast if nodelist not match */
                        if (subplan->args == NIL && (!IsA(node, SubPlan)) && is_execute_on_datanodes(plan) &&
                            !contain_special_plan_node(plan, T_Stream, CPLN_ONE_WAY)) {
                            if (result_plan->exec_nodes && plan->exec_nodes && plan->exec_nodes->nodeList != NIL &&
                                list_difference_int(result_plan->exec_nodes->nodeList, plan->exec_nodes->nodeList)) {
                                Distribution* result_distribution = ng_get_dest_distribution(result_plan);
                                plan = make_stream_plan(NULL, plan, NIL, 0.0, result_distribution);
                            }
                        }

                        /* Push only nodelist but not entire exec_nodes here. */
                        pushdown_execnodes(plan, result_plan->exec_nodes, false, true);
                    }

                    if (check_stream_support()) {
                        PlannerInfo* subroot = NULL;
                        Plan* child_root = NULL;
                        ListCell* lr = NULL;
                        int j = subplan->plan_id - 1;
                        subroot = (PlannerInfo*)list_nth(subroots, j);
                        /* for subplan or correlated init plan, check rescan whether support */
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

                        /* also need set plan with child_root. */
                        plan = child_root;
                        if (isIntergratedMachine && !IsA(node, SubPlan)) {
                            if (u_sess->opt_cxt.query_dop > 1) {
                                List* subPlanList = NIL;
                                (void)has_subplan(plan, result_plan, NULL, true, &subPlanList, true);
                            }
                            confirm_parallel_info(plan, result_plan->dop);
                        }
                    }

                    /* Check if this subplan under CteScan */
                    if (IsA(result_plan, CteScan)) {
                        is_under_ctescan = true;
                    }

                    finalize_node_id(plan, plan_node_id, parent_node_id, num_streams, num_plannodes, total_num_streams,
                        max_push_sql_num, gather_count, subplans, subroots, initplans, subplan_ids, false,
                        is_under_ctescan, is_data_node_exec, is_read_only, node_group_info_context);
                    *parent_node_id = save_parent_id;
                }
            }
            list_free_ext(subplan_list);
        }
    }
}

void mark_distribute_setop_remotequery(PlannerInfo* root, Node* node, Plan* plan, List* subPlans)
{
    Plan* remotePlan = NULL;
    Plan* subPlan = NULL;
    ListCell* cell = NULL;
    List* newSubPlans = NIL;
    MergeAppend* mergeAppend = NULL;
    Append* append = NULL;
    errno_t sprintf_rc = 0;

    if (IsA(node, MergeAppend)) {
        mergeAppend = (MergeAppend*)node;
    } else if (IsA(node, RecursiveUnion)) {
        sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
            NOTPLANSHIPPING_LENGTH,
            "With-Recursive in subplan which executes on CN is not shippable");
        securec_check_ss_c(sprintf_rc, "\0", "\0");
        mark_stream_unsupport();
    } else {
        AssertEreport(IsA(node, Append), MOD_OPT, "The node is NOT a Append");

        append = (Append*)node;
    }

    foreach (cell, subPlans) {
        subPlan = (Plan*)lfirst(cell);
        remotePlan = make_simple_RemoteQuery(subPlan, root, true);

        if (mergeAppend && is_execute_on_datanodes(subPlan)) {
            remotePlan = (Plan*)make_sort(root,
                remotePlan,
                mergeAppend->numCols,
                mergeAppend->sortColIdx,
                mergeAppend->sortOperators,
                mergeAppend->collations,
                mergeAppend->nullsFirst,
                -1);
        }

        newSubPlans = lappend(newSubPlans, remotePlan);
    }

    list_free_ext(subPlans);

    if (PointerIsValid(mergeAppend)) {
        mergeAppend->mergeplans = newSubPlans;
    } else {
        AssertEreport(PointerIsValid(append), MOD_OPT, "The append is NULL");

        append->appendplans = newSubPlans;
    }

    plan->dop = 1;
    plan->distributed_keys = NIL;
    plan->exec_type = EXEC_ON_COORDS;
    plan->exec_nodes = ng_get_single_node_group_exec_node();
}
