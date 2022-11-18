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
 *  planrecursive_single.cpp
 *	  The query optimizer external interface.
 *
 * IDENTIFICATION
 *        src/gausskernel/optimizer/plan/planrecursive_single.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <limits.h>
#include <math.h>

#include "access/transam.h"
#include "catalog/indexing.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_constraint.h"
#include "catalog/pgxc_group.h"
#include "catalog/pgxc_node.h"
#include "executor/executor.h"
#include "executor/node/nodeAgg.h"
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
#include "optimizer/nodegroups.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/plancat.h"
#include "optimizer/planmain.h"
#include "optimizer/planner.h"
#include "optimizer/prep.h"
#include "optimizer/subselect.h"
#include "optimizer/tlist.h"
#include "parser/analyze.h"
#include "parser/parsetree.h"
#include "parser/parse_agg.h"
#include "rewrite/rewriteManip.h"
#include "securec.h"
#include "utils/rel.h"
#ifdef PGXC
#include "commands/prepare.h"
#include "pgxc/pgxc.h"
#include "optimizer/pgxcplan.h"
#include "optimizer/streamplan.h"
#include "workload/workload.h"
#endif
#include "optimizer/streamplan.h"
#include "utils/relcache.h"
#include "utils/selfuncs.h"
#include "utils/fmgroids.h"
#include "access/heapam.h"
#include "vecexecutor/vecfunc.h"
#include "executor/node/nodeRecursiveunion.h"
#include "optimizer/randomplan.h"
#include "optimizer/optimizerdebug.h"

/*
 * @Fuction: getPlanSubNodes()
 *
 * @Brief: Return plan node's underlying plan nodes that is not create under
 *   left/right plan tree
 *
 * @Input node: plan node
 *
 * @Return: true: walk success false: failed
 ***/
static List* getSpecialPlanSubNodes(const Plan* node)
{
    List* ps_list = NIL;

    if (node == NULL) {
        return NIL;
    }

    /* Find plan list in special plan nodes. */
    switch (nodeTag(node)) {
        case T_Append:
        case T_VecAppend: {
            Append* append = (Append*)node;
            ListCell* lc = NULL;
            foreach (lc, append->appendplans) {
                ps_list = lappend(ps_list, lfirst(lc));
            }
        } break;
        case T_ModifyTable:
        case T_VecModifyTable: {
            ModifyTable* mt = (ModifyTable*)node;
            ListCell* lc = NULL;
            foreach (lc, mt->plans) {
                ps_list = lappend(ps_list, lfirst(lc));
            }
        } break;
        case T_SubqueryScan:
        case T_VecSubqueryScan: {
            SubqueryScan* ss = (SubqueryScan*)node;
            if (ss->subplan) {
                ps_list = lappend(ps_list, (void*)ss->subplan);
            }
        } break;
        case T_MergeAppend:
        case T_VecMergeAppend: {
            MergeAppend* ma = (MergeAppend*)node;
            ListCell* lc = NULL;
            foreach (lc, ma->mergeplans) {
                ps_list = lappend(ps_list, lfirst(lc));
            }
        } break;
        case T_BitmapAnd:
        case T_CStoreIndexAnd: {
            BitmapAnd* ba = (BitmapAnd*)node;
            ListCell* lc = NULL;
            foreach (lc, ba->bitmapplans) {
                ps_list = lappend(ps_list, lfirst(lc));
            }
        } break;

        case T_BitmapOr:
        case T_CStoreIndexOr: {
            BitmapOr* bo = (BitmapOr*)node;
            ListCell* lc = NULL;
            foreach (lc, bo->bitmapplans) {
                ps_list = lappend(ps_list, lfirst(lc));
            }
        } break;
        default: {
            ps_list = NIL;
        } break;
    }

    return ps_list;
}

inline static void setPlanNodeId(Plan* node, RecursiveRefContext* context,
    const RecursiveUnion* runode)
{
    node->control_plan_nodeid = ((context->nested_stream_depth == 1) ?
                                 GET_PLAN_NODEID(runode) :
                                 GET_PLAN_NODEID(context->control_plan));
}

static void setRecursiveRteplanRefByType(Plan* node, RecursiveRefContext* context,
    const RecursiveUnion* runode)
{
    switch (nodeTag(node)) {
        /*
         * If we found we have operators that do not support recursive-execution we
         * mark the stream-recursive unsupported
         */
        case T_Stream: {
            Stream* stream = (Stream*)node;
            elog(DEBUG1, "set plan ref for stream node %d", GET_PLAN_NODEID(stream));

            /* Remove LOCAL GATHER for Recursive, which is just lefttree of another normal Stream */
            if (IsA(node->lefttree, Stream) && ((Stream*)node->lefttree)->is_recursive_local) {
                node->lefttree = node->lefttree->lefttree;
            }

            /* If set control plan nodeid is required, we do reset Plan::control_plan_nodeid */
            if (context->set_control_plan_nodeid) {
                setPlanNodeId(node, context, runode);

                /*
                 * After set current plan node, we need reset the "set_control_plan_nodeid"
                 * back to false.
                 */
                context->set_control_plan_nodeid = false;
            }

            /*
             * For stream node, we need set its first underlying plannode's
             * control nodeid.
             */
            context->set_control_plan_nodeid = true;
            context->nested_stream_depth++;
            context->control_plan = node;

            /* The first node is mared as syncnode */
            if (!context->is_syncup_producer_specified && !stream->is_recursive_local) {
                /* mark current stream node as sync-node (consumer) */
                node->is_sync_plannode = true;

                /* mark current stream's input node as sync-node (producer) */
                node->lefttree->is_sync_plannode = true;

                /* mark we've done with marking sync-plannode */
                context->is_syncup_producer_specified = true;
            }

            /* Assign the stream level */
            stream->stream_level = context->nested_stream_depth;

            /*
             * Mark the top-plannode under current Stream is controlled by RecursiveUnion
             * awk. 1st level of recursvie-controlling
             */
            if (context->nested_stream_depth > 1) {
                node->recursive_union_controller = true;
            }

            /* set reference producer tree */
            set_recursive_cteplan_ref(node->lefttree, context);

            /* Reset the nestted level back */
            context->nested_stream_depth--;
        } break;
        case T_RecursiveUnion: {
            /* Mark current conttroller */
            node->recursive_union_controller = true;

            /*
             * Set ru_plan_nodeid for recursive part
             *
             * Note: we do not have to set the non-recursive part, the underlying
             * stream thread only runs one time, no need for cluster-step control.
             */
            set_recursive_cteplan_ref(innerPlan(node), context);
        } break;
        default: {
            /* If set control plan nodeid is required, we do reset Plan::control_plan_nodeid */
            if (context->set_control_plan_nodeid) {
                setPlanNodeId(node, context, runode);

                /*
                 * After set current plan node, we need reset the "set_control_plan_nodeid"
                 * back to false.
                 */
                context->set_control_plan_nodeid = false;
            }

            /* set recursive cteplan node id for its underlying left and right tree */
            Plan* lplan = outerPlan(node);
            Plan* rplan = innerPlan(node);

            if (context->join_type == T_HashJoin || context->join_type == T_VecHashJoin) {
                if (lplan != NULL) {
                    set_recursive_cteplan_ref(lplan, context);
                }

                if (rplan != NULL) {
                    set_recursive_cteplan_ref(rplan, context);
                }
            } else {
                if (rplan != NULL) {
                    set_recursive_cteplan_ref(rplan, context);
                }

                if (lplan != NULL) {
                    set_recursive_cteplan_ref(lplan, context);
                }
            }
        }
    }
}

void set_recursive_cteplan_ref(Plan* node, RecursiveRefContext* context)
{
    if (node == NULL) {
        return;
    }

    const RecursiveUnion* runode = context->ru_plan;
    Assert(runode != NULL && IsA(runode, RecursiveUnion) && context != NULL);

    /*
     * We won't build the recursive control flow as a correlated recursive UNION have
     * to be executed in one DN
     */
    if (runode->is_correlated) {
        return;
    }

    /* Bind recursive union plannodeid for each underlying operators */
    node->recursive_union_plan_nodeid = GET_PLAN_NODEID(runode);

    /* First, assign join type of nearest node */
    if (nodeTag(node) == T_HashJoin || nodeTag(node) == T_VecHashJoin || nodeTag(node) == T_MergeJoin ||
        nodeTag(node) == T_VecMergeJoin || nodeTag(node) == T_NestLoop || nodeTag(node) == T_VecNestLoop) {
        context->join_type = nodeTag(node);
    }

    /* Second, Process regular nodes */
    setRecursiveRteplanRefByType(node, context, runode);

    /* Third, process the plannode is not consists of left and right tree */
    List* special_subnodes = getSpecialPlanSubNodes(node);
    if (special_subnodes != NIL) {
        ListCell* lc = NULL;
        foreach (lc, special_subnodes) {
            Plan* subnode = (Plan*)lfirst(lc);
            set_recursive_cteplan_ref(subnode, context);
        }
    }

    /* find all subplan exprs from main plan */
    List* subplan_list = check_subplan_list(node);
    ListCell* lc = NULL;

    foreach (lc, subplan_list) {
        Node* localNode = (Node*)lfirst(lc);
        Plan* plan = NULL;
        SubPlan* subplan = NULL;

        if (IsA(localNode, SubPlan)) {
            subplan = (SubPlan*)lfirst(lc);
            /* this is for the case that initplan hidden in testexpr of subplan */
            subplan_list = list_concat(subplan_list, check_subplan_expr(subplan->testexpr));
        } else {
            Assert(IsA(localNode, Param));
            Param* param = (Param*)localNode;
            ListCell* lc2 = NULL;
            foreach (lc2, context->initplans) {
                subplan = (SubPlan*)lfirst(lc2);
                if (list_member_int(subplan->setParam, param->paramid))
                    break;
            }
            if (subplan == NULL || lc2 == NULL)
                continue;
        }

        plan = (Plan*)list_nth(context->subplans, subplan->plan_id - 1);
        set_recursive_cteplan_ref(plan, context);
    }
}

bool IsSyncUpProducerThread()
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return false;
}

/*
 * @Function: NeedSetupSyncUpController()
 *
 * @Brief: Invokded at ExecInitNode()funciton, to identify if we have to set up the step
 *        controller to do step-syncup across the whole cluster
 *
 * @Input plan: the plan node need verify to create controller at plan-init stage
 *
 * @Return: True/False to indicate if we need set up controller
 ***/
bool NeedSetupSyncUpController(Plan* plan)
{
    bool result = false;

    if (IS_PGXC_COORDINATOR || u_sess->stream_cxt.global_obj == NULL) {
        return false;
    }

    Assert(IS_PGXC_DATANODE && u_sess->stream_cxt.global_obj != NULL && plan != NULL);

    switch (nodeTag(plan)) {
        case T_Stream:
        case T_VecStream: {
            result = ((Plan*)plan)->recursive_union_controller;
        } break;
        case T_RecursiveUnion: {
            RecursiveUnion* ruplan = (RecursiveUnion*)plan;

            if (ruplan->is_correlated) {
                /* No recursive controller should be setup in a correlated recursive CTE */
                result = false;
                Assert(!plan->recursive_union_controller);
            } else if (ruplan->has_inner_stream) {
                /* Setup controller if recursive-union's inner plan has stream operator */
                result = true;
                Assert(plan->recursive_union_controller);
            }
        } break;
        default: {
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                    (errmsg("Unsupported node type %s to check need stream setup for recursive union",
                        nodeTagToString(nodeTag(plan))))));
        }
    }

    return result;
}

/*
 * @Function: NeedSyncUpRecursiveUnionStep()
 *
 * @Brief: Invokded at ExecRecursiveUnion(), to identify if we have to set up the step
 *         controller to do step-syncup across the whole cluster
 *
 * @Input plan: the plan node need verify to create controller at plan-init stage
 *
 * @Return: True/False to indicate if we need do sync-up(Consumer)
 ***/
bool NeedSyncUpRecursiveUnionStep(Plan* plan)
{
    /* we don't have to do distributed step sync-up on coordinator node */
    if (IS_PGXC_COORDINATOR) {
        return false;
    }

    bool result = false;

    switch (nodeTag(plan)) {
        case T_RecursiveUnion: {
            RecursiveUnion* ruplan = (RecursiveUnion*)plan;

            if (ruplan->is_correlated) {
                /* We don't have to do recursive step syncup in correlated RCTE */
                result = false;
                Assert(!plan->recursive_union_controller);
            } else if (ruplan->has_inner_stream) {
                result = true;
                Assert(plan->recursive_union_controller);
            }
        } break;
        default: {
            result = plan->recursive_union_plan_nodeid != 0;
        }
    }

    return result;
}

/*
 * @Function: NeedSyncUpProducerStep()
 *
 * @Brief: Invokded at ExecutePlan(), to identify if current plan need sync-up
 *
 * @Input plan: the plan node need plan sync-up check
 *
 * @Return: True/False to indicate whether we need do sync-up
 ***/
bool NeedSyncUpProducerStep(Plan* top_plan)
{
    /* we don't have to do distributed step sync-up on coordinator node */
    if (IS_PGXC_COORDINATOR) {
        return false;
    }

    return EXEC_IN_RECURSIVE_MODE(top_plan);
}

void mark_stream_recursiveunion_plan(
    RecursiveUnion* runode, Plan* node, bool recursive_branch, List* subplans, List** initplans)
{
    if (node == NULL) {
        return;
    }

    if (node->initPlan)
        *initplans = list_concat(*initplans, list_copy(node->initPlan));

    /* if we found both side stream is marked we are done */
    if (runode->has_inner_stream && runode->has_outer_stream) {
        return;
    }

    Assert(IsA(runode, RecursiveUnion));

    ListCell* lc = NULL;
    /* First, process the plannode is not consists of left and right tree */
    List* special_subnodes = getSpecialPlanSubNodes(node);
    if (special_subnodes != NIL) {
        foreach (lc, special_subnodes) {
            Plan* subnode = (Plan*)lfirst(lc);

            mark_stream_recursiveunion_plan(runode, subnode, recursive_branch, subplans, initplans);
        }
    }

    /* Sedondary, Process regular nodes */
    switch (nodeTag(node)) {
        case T_RecursiveUnion: {
            Plan* lplan = (Plan*)outerPlan(node);
            Plan* rplan = (Plan*)innerPlan(node);

            Assert(lplan != NULL && rplan != NULL);

            /* iterative left tree */
            mark_stream_recursiveunion_plan((RecursiveUnion*)node, lplan, false, subplans, initplans);

            /* iterative right tree */
            mark_stream_recursiveunion_plan((RecursiveUnion*)node, rplan, true, subplans, initplans);
        } break;
        case T_Stream:
        case T_VecStream:
            if (recursive_branch && !runode->has_inner_stream) {
                /* mark iner side has stream */
                runode->has_inner_stream = true;
            } else if (!recursive_branch && !runode->has_outer_stream) {
                /* mark outer side has stream */
                runode->has_outer_stream = true;
            }
            break;

        default: {
            /* set recursive cteplan node id for its underlying left and right tree */
            Plan* lplan = outerPlan(node);
            Plan* rplan = innerPlan(node);

            /* iterative left tree */
            mark_stream_recursiveunion_plan(runode, lplan, recursive_branch, subplans, initplans);

            /* iterative right tree */
            mark_stream_recursiveunion_plan(runode, rplan, recursive_branch, subplans, initplans);
        }
    }

    /* Finally, Process subplan and initplan */
    List* subplan_list = getSubPlan(node, subplans, *initplans);
    foreach (lc, subplan_list) {
        Plan* subnode = (Plan*)lfirst(lc);
        mark_stream_recursiveunion_plan(runode, subnode, recursive_branch, subplans, initplans);
    }

    return;
}
