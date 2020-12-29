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
 * -------------------------------------------------------------------------
 * planmem_walker.cpp
 *
 * Standard expression-tree walking support
 *
 * We used to have near-duplicate code in many different routines that
 * understood how to recurse through an expression node tree.  That was
 * a pain to maintain, and we frequently had bugs due to some particular
 * routine neglecting to support a particular node type.  In most cases,
 * these routines only actually care about certain node types, and don't
 * care about other types except insofar as they have to recurse through
 * non-primitive node types.  Therefore, we now provide generic tree-walking
 * logic to consolidate the redundant "boilerplate" code.  There are
 * two versions: expression_tree_walker() and expression_tree_mutator().
 *
 * IDENTIFICATION
 *        src/gausskernel/optimizer/util/planmem_walker.cpp
 *
 * ------------------------------------------------------------------------- */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "nodes/nodeFuncs.h"
#include "nodes/plannodes.h"
#include "optimizer/pgxcplan.h"
#include "optimizer/planmem_walker.h"

extern void check_stack_depth(void);
static bool walk_scan_node_fields(Scan* scan, MethodWalker walker, void* context);
static bool walk_join_node_fields(Join* join, MethodWalker walker, void* context);

/*
 * @Description: Initialize a plan_tree_base_prefix after planning.
 * @IN base: plan tree base prefix
 * @IN stmt: plan statement
 * @Return: void
 * @See also:
 */
void exec_init_plan_tree_base(plan_tree_base_prefix* base, PlannedStmt* stmt)
{
    base->node = (Node*)stmt;

    /* Reset init plans */
    if (base->init_plans != NIL)
        list_free_ext(base->init_plans);
    base->init_plans = NIL;

    /* Reset subplan info, as every subplan is not traversed */
    if (stmt->subplans != NIL) {
        if (base->traverse_flag == NULL)
            base->traverse_flag = (bool*)palloc0(sizeof(bool) * list_length(stmt->subplans));
        else {
            errno_t rc = 0;
            rc = memset_s(base->traverse_flag,
                sizeof(bool) * list_length(stmt->subplans),
                0,
                sizeof(bool) * list_length(stmt->subplans));
            securec_check(rc, "\0", "\0");
        }
    }
}

/* ----------------------------------------------------------------------- *
 * Plan Tree Walker Framework
 * ----------------------------------------------------------------------- *
 */
/*
 * @Description: Function is a subroutine used by plan_tree_walker()
 *              to walk the fields of Plan nodes.  Plan is actually
 *              an abstract superclass of all plan nodes and this function
 *              encapsulates the common structure.
 * @IN plan: plan
 * @IN walker: method walker
 * @IN context: method context(auto or early free)
 * @Return: true: walk success false: failed
 * @See also: Most specific walkers won't need to call this function, but complicated
 *            ones may find it a useful utility.
 * @Caution: walk_scan_node_fields and walk_join_node_fields call this
 *           function.  Use only the most specific function.
 */
bool walk_plan_node_fields(Plan* plan, MethodWalker walker, void* context)
{
    MethodP2Walker p2walker = (MethodP2Walker)walker;
    MethodPlanWalkerContext* mcontext = ((MethodPlanWalkerContext*)context);

    /* Init Plan nodes (uncorrelated expr subselects */
    mcontext->base.init_plans = list_concat(mcontext->base.init_plans, list_copy(plan->initPlan));

    /* If current node is join, then mark the flag about its children node */
    if (IsBlockedJoinNode(plan))
        mcontext->status |= UNDER_MULTI_GROUP_OP;

    /* input plan tree(s) */
    if (p2walker((Node*)(plan->lefttree), context))
        return true;

    /* target list to be computed at this node */
    if (p2walker((Node*)(plan->righttree), context))
        return true;

    mcontext->status &= ~UNDER_MULTI_GROUP_OP;

    /* target list to be computed at this node */
    if (p2walker((Node*)(plan->targetlist), context))
        return true;

    /* implicitly ANDed qual conditions */
    if (p2walker((Node*)(plan->qual), context))
        return true;

    return false;
}

/*
 * @Description: Function is a subroutine used by plan_tree_walker()
 *              to walk the fields of Scan nodes.  Scan is actually
 *              an abstract superclass of all scan nodes and a subclass
 *              of Plan.  This function encapsulates the
 *              common structure.
 * @IN scan: scan operator
 * @IN walker: method walker
 * @IN context: method context(auto or early free)
 * @Return: true: walk success false: failed
 * @See also: Most specific walkers won't need to call this function, but complicated
 *            ones may find it a useful utility.
 * @Caution: This function calls walk_plan_node_fields so callers shouldn't,
 *           else they will walk common plan fields twice.
 */
bool walk_scan_node_fields(Scan* scan, MethodWalker walker, void* context)
{
    /* A Scan node is a kind of Plan node. */
    if (walk_plan_node_fields((Plan*)scan, walker, context))
        return true;

    /* The only additional field is an Index so no extra walking. */
    return false;
}

/*
 * @Description: Function is a subroutine used by plan_tree_walker()
 *              to walk the fields of Join nodes.  Join is actually
 *              an abstract superclass of all join nodes and a subclass
 *              of Plan.  This function encapsulates the common structure.
 * @IN join: join operator
 * @IN walker: method walker
 * @IN context: method context(auto or early free)
 * @Return: true: walk success false: failed
 * @See also: Most specific walkers won't need to call this function, but complicated
 *            ones may find it a useful utility.
 * @Caution: This function calls walk_plan_node_fields so callers shouldn't,
 *           else they will walk common plan fields twice.
 */
bool walk_join_node_fields(Join* join, MethodWalker walker, void* context)
{
    /* A Join node is a kind of Plan node. */
    if (walk_plan_node_fields((Plan*)join, walker, context))
        return true;

    MethodP2Walker p2walker = (MethodP2Walker)walker;

    return p2walker((Node*)(join->joinqual), context);
}

/*
 * @Description: Function is a general walker for Plan trees.
 * @IN node: plan node
 * @IN walker: method walker
 * @IN context: method context(auto or early free)
 * @Return: true: walk success false: failed
 * @See also: The basic idea is that this function (and its helpers) walk plan-specific
 *            nodes and delegate other nodes to expression_tree_walker().  The caller
 *            may supply a specialized walker
 */
bool plan_tree_walker(Node* node, MethodWalker walker, void* context)
{
    MethodP2Walker p2walker = (MethodP2Walker)walker;

    if (node == NULL)
        return false;

    check_stack_depth();

    switch (nodeTag(node)) {
        case T_Plan:

            return walk_plan_node_fields((Plan*)node, walker, context);

        case T_BaseResult:
        case T_VecResult:
            if (walk_plan_node_fields((Plan*)node, walker, context))
                return true;
            if (p2walker((Node*)((BaseResult*)node)->resconstantqual, context))
                return true;
            break;

        case T_Append:
        case T_VecAppend: {
            MethodPlanWalkerContext* mcontext = (MethodPlanWalkerContext*)context;

            /* Mark the status to let children node know it's under multi group node */
            mcontext->status |= UNDER_MULTI_GROUP_OP;
            if (p2walker((Node*)((Append*)node)->appendplans, context)) {
                mcontext->status &= ~UNDER_MULTI_GROUP_OP;
                return true;
            }
            mcontext->status &= ~UNDER_MULTI_GROUP_OP;

            if (walk_plan_node_fields((Plan*)node, walker, context))
                return true;
        } break;

        case T_MergeAppend: {
            MethodPlanWalkerContext* mcontext = (MethodPlanWalkerContext*)context;

            /* Mark the status to let children node know it's under multi group node */
            mcontext->status |= UNDER_MULTI_GROUP_OP;
            if (p2walker((Node*)((MergeAppend*)node)->mergeplans, context)) {
                mcontext->status &= ~UNDER_MULTI_GROUP_OP;
                return true;
            }
            mcontext->status &= ~UNDER_MULTI_GROUP_OP;

            if (walk_plan_node_fields((Plan*)node, walker, context))
                return true;
        } break;

        case T_CStoreIndexAnd:
        case T_BitmapAnd:
            if (walk_plan_node_fields((Plan*)node, walker, context))
                return true;
            if (p2walker((Node*)((BitmapAnd*)node)->bitmapplans, context))
                return true;
            break;

        case T_Scan:
            return walk_scan_node_fields((Scan*)node, walker, context);

        case T_CStoreIndexOr:
        case T_BitmapOr:
            if (walk_plan_node_fields((Plan*)node, walker, context))
                return true;
            if (p2walker((Node*)((BitmapOr*)node)->bitmapplans, context))
                return true;
            break;

        case T_SeqScan:
        case T_FunctionScan:
        case T_ValuesScan:
        case T_CteScan:
        case T_WorkTableScan:
            if (walk_scan_node_fields((Scan*)node, walker, context))
                return true;
            if (IsA(node, ValuesScan)) {
                ValuesScan* splan = (ValuesScan*)node;
                if (p2walker((Node*)splan->values_lists, context))
                    return true;
            }
            break;

        case T_VecForeignScan:
        case T_ForeignScan:
            if (walk_scan_node_fields((Scan*)node, walker, context))
                return true;
            if (p2walker((Node*)((ForeignScan*)node)->fdw_exprs, context))
                return true;
            if (p2walker((Node*)((ForeignScan*)node)->fdw_private, context))
                return true;
            break;
        case T_ExtensiblePlan:
            if (walk_scan_node_fields((Scan*)node, walker, context))
                return true;
            if (p2walker((Node*)((ExtensiblePlan*)node)->extensible_plans, context))
                return true;
            if (p2walker((Node*)((ExtensiblePlan*)node)->extensible_exprs, context))
                return true;
            break;
        case T_IndexScan:
            if (walk_scan_node_fields((Scan*)node, walker, context))
                return true;
            if (p2walker((Node*)((IndexScan*)node)->indexqual, context))
                return true;
            /* Other fields are lists of basic items, nothing to walk. */
            break;

        case T_IndexOnlyScan:
            if (walk_scan_node_fields((Scan*)node, walker, context))
                return true;
            if (p2walker((Node*)((IndexOnlyScan*)node)->indexqual, context))
                return true;
            /* Other fields are lists of basic items, nothing to walk. */
            break;

        case T_CStoreIndexScan:
            if (walk_scan_node_fields((Scan*)node, walker, context))
                return true;
            if (p2walker((Node*)((CStoreIndexScan*)node)->indexqual, context))
                return true;
            /* Other fields are lists of basic items, nothing to walk. */
            break;

        case T_DfsIndexScan:
            if (walk_scan_node_fields((Scan*)node, walker, context))
                return true;
            if (p2walker((Node*)((DfsIndexScan*)node)->indexqual, context))
                return true;
            /* Other fields are lists of basic items, nothing to walk. */
            break;

        case T_BitmapIndexScan:
            if (walk_scan_node_fields((Scan*)node, walker, context))
                return true;
            if (p2walker((Node*)((BitmapIndexScan*)node)->indexqual, context))
                return true;
            /* Other fields are lists of basic items, nothing to walk. */
            break;

        case T_CStoreIndexCtidScan:
            if (walk_scan_node_fields((Scan*)node, walker, context))
                return true;
            if (p2walker((Node*)((CStoreIndexCtidScan*)node)->indexqual, context))
                return true;
            /* Other fields are lists of basic items, nothing to walk. */
            break;

        case T_CStoreIndexHeapScan:
        case T_BitmapHeapScan:
            if (walk_scan_node_fields((Scan*)node, walker, context))
                return true;
            break;

        case T_TidScan:
            if (walk_scan_node_fields((Scan*)node, walker, context))
                return true;
            if (p2walker((Node*)((TidScan*)node)->tidquals, context))
                return true;
            break;

        case T_VecSubqueryScan:
        case T_SubqueryScan:
            if (walk_scan_node_fields((Scan*)node, walker, context))
                return true;
            if (p2walker((Node*)((SubqueryScan*)node)->subplan, context))
                return true;
            break;

        case T_CStoreScan:
            if (walk_scan_node_fields((Scan*)node, walker, context))
                return true;
            if (p2walker((Node*)((CStoreScan*)node)->cstorequal, context))
                return true;
            break;

        case T_DfsScan: {
            if (walk_scan_node_fields((Scan*)node, walker, context))
                return true;
            DfsPrivateItem* item = (DfsPrivateItem*)((DefElem*)linitial(((DfsScan*)node)->privateData))->arg;
            if (p2walker((Node*)item->hdfsQual, context))
                return true;
        } break;
#ifdef ENABLE_MULTIPLE_NODES
        case T_TsStoreScan:
            if (walk_scan_node_fields((Scan*)node, walker, context))
                return true;
            if (p2walker((Node*)((TsStoreScan*)node)->tsstorequal, context))
                return true;
            break;
#endif   /* ENABLE_MULTIPLE_NODES */

        case T_VecStream:
        case T_Stream:
            /* Under stream node should be marked */
            ((MethodPlanWalkerContext*)context)->status |= UNDER_STREAM;
            return walk_scan_node_fields((Scan*)node, walker, context);

        case T_Join:
            /* Abstract: really should see only subclasses. */
            return walk_join_node_fields((Join*)node, walker, context);

        case T_VecNestLoop:
        case T_NestLoop:
            if (walk_join_node_fields((Join*)node, walker, context))
                return true;
            break;

        case T_VecMergeJoin:
        case T_MergeJoin:
            if (walk_join_node_fields((Join*)node, walker, context))
                return true;
            if (p2walker((Node*)((MergeJoin*)node)->mergeclauses, context))
                return true;
            break;

        case T_VecHashJoin:
        case T_HashJoin:
            if (walk_join_node_fields((Join*)node, walker, context))
                return true;
            if (p2walker((Node*)((HashJoin*)node)->hashclauses, context))
                return true;
            break;

        case T_VecToRow:
        case T_RowToVec:
        case T_VecMaterial:
        case T_Material:
            if (walk_plan_node_fields((Plan*)node, walker, context))
                return true;
            break;

        case T_VecSort:
        case T_Sort:
            if (walk_plan_node_fields((Plan*)node, walker, context))
                return true;
            /* Other fields are simple counts and lists of indexes and oids. */
            break;

        case T_VecAgg:
        case T_Agg:
            if (walk_plan_node_fields((Plan*)node, walker, context))
                return true;
            /* Other fields are simple items and lists of simple items. */
            break;

        case T_WindowAgg:
        case T_VecWindowAgg:
            if (walk_plan_node_fields((Plan*)node, walker, context))
                return true;
            /* Other fields are simple items and lists of simple items. */
            break;

        case T_Unique:
        case T_VecUnique:
            if (walk_plan_node_fields((Plan*)node, walker, context))
                return true;
            /* Other fields are simple items and lists of simple items. */
            break;

        case T_Hash:
            if (walk_plan_node_fields((Plan*)node, walker, context))
                return true;
            /* Other info is in parent HashJoin node. */
            break;

        case T_VecLimit:
        case T_Limit:
            if (walk_plan_node_fields((Plan*)node, walker, context)) {
                return true;
            }

            if (p2walker((Node*)(((Limit*)node)->limitCount), context)) {
                return true;
            }

            if (p2walker((Node*)(((Limit*)node)->limitOffset), context)) {
                return true;
            }

            break;

        case T_VecModifyTable:
        case T_ModifyTable: {
            ModifyTable* modifytable = (ModifyTable*)node;

            if (walk_plan_node_fields((Plan*)node, walker, context))
                return true;

            /* recursive to list of plans */
            if (p2walker((Node*)(((ModifyTable*)modifytable)->plans), context))
                return true;
        } break;

        case T_SubPlan: {
            SubPlan* subplan = (SubPlan*)node;
            Plan* subplan_plan = plan_tree_base_subplan_get_plan((plan_tree_base_prefix*)context, subplan);

            if (subplan_plan == NULL) {
                return false;
            }

            if (expression_tree_walker((Node*)subplan->testexpr, walker, context)) {
                return true;
            }

            if (p2walker((Node*)subplan_plan, context)) {
                return true;
            }

            if (expression_tree_walker((Node*)subplan->args, walker, context)) {
                return true;
            }
        } break;

        /* handle InitPlan */
        case T_Param: {
            Param* p = (Param*)node;
            plan_tree_base_prefix* pcontext = (plan_tree_base_prefix*)context;
            ListCell* lc = NULL;

            /* Traverse the subplan from the init plan entry */
            if (p->paramkind == PARAM_EXEC) {
                foreach (lc, pcontext->init_plans) {
                    SubPlan* subplan = (SubPlan*)lfirst(lc);
                    if (list_member_int(subplan->setParam, p->paramid)) {
                        if (plan_tree_walker((Node*)subplan, walker, context))
                            return true;
                        break;
                    }
                }
            }
        } break;

        case T_Query:
            return query_tree_walker((Query*)node, walker, context, 0);

        case T_SetOp:
        case T_VecSetOp:
        case T_PartIterator:
        case T_VecPartIterator:
        case T_Group:
        case T_VecGroup:
        case T_LockRows:
        case T_RecursiveUnion:
        case T_VecRemoteQuery:
        case T_RemoteQuery: {
            MethodPlanWalkerContext* mcontext = (MethodPlanWalkerContext*)context;

            /*
             * Since query can contain multiple remote query, we should
             * change to a new group for each remote query
             */
            if (IsA(node, RemoteQuery) || IsA(node, VecRemoteQuery)) {
                /* Query on obs: If we find dn gather, then treat it as scan and return */
                if (mcontext->dnExec && ((RemoteQuery*)node)->position != GATHER)
                    return false;

                if (mcontext->phase == ASSIGN_MEM)
                    mcontext->groupTree = NULL;
                else
                    mcontext->groupTree = (OperatorGroupNode*)list_nth(mcontext->groupTreeList, mcontext->groupTreeIdx);

                /* With new group tree, reset traverse variable */
                mcontext->nextGroupId = 0;
                for (int i = 0; i < mcontext->ng_num; i++) {
                    mcontext->ng_queryMemKBArray[i].currQueryMemKB = 0;
                    mcontext->ng_queryMemKBArray[i].minCurrQueryMemKB = 0;
                }
                mcontext->dnExec = true;
            }
            if (walk_plan_node_fields((Plan*)node, walker, context))
                return true;
            if (IsA(node, RemoteQuery) || IsA(node, VecRemoteQuery)) {
                /* Finishing scan the query, add it to the list for re-traverse */
                if (mcontext->phase == ASSIGN_MEM)
                    mcontext->groupTreeList = lappend(mcontext->groupTreeList, mcontext->groupTree);
                else
                    mcontext->groupTreeIdx++;

                /* Every remotequery is serialized, so get the max */
                for (int i = 0; i < mcontext->ng_num; i++) {
                    mcontext->ng_queryMemKBArray[i].queryMemKB =
                        Max(mcontext->ng_queryMemKBArray[i].queryMemKB, mcontext->ng_queryMemKBArray[i].currQueryMemKB);
                    mcontext->ng_queryMemKBArray[i].minQueryMemKB = Max(mcontext->ng_queryMemKBArray[i].minQueryMemKB,
                        mcontext->ng_queryMemKBArray[i].minCurrQueryMemKB);
                }
                ((MethodPlanWalkerContext*)context)->dnExec = false;
            }
        } break;

        case T_IntList:
        case T_OidList:
        case T_DefElem:
        case T_Var:
        case T_Const:
        case T_CoerceToDomainValue:
        case T_CaseTestExpr:
        case T_SetToDefault:
        case T_RangeTblRef:
            break;

        default:
            return expression_tree_walker(node, walker, context);
    }
    return false;
}

/*
 * @Description: get the plan associated with a SubPlan node in a walker.
 * @IN base: plan tree base prefix
 * @IN subplan: sub plan
 * @Return: plan associated with a SubPlan node in a walker
 * @See also: This is used by framework, not by users of the framework.
 */
Plan* plan_tree_base_subplan_get_plan(plan_tree_base_prefix* base, SubPlan* subplan)
{
    if (base == NULL)
        return NULL;
    else if (IsA(base->node, PlannedStmt)) {
        /* If the subplan has already traversed, just return */
        if (base->traverse_flag[subplan->plan_id - 1])
            return NULL;
        base->traverse_flag[subplan->plan_id - 1] = true;
        return exec_subplan_get_plan((PlannedStmt*)base->node, subplan);
    } else if (IsA(base->node, PlannerInfo))
        return planner_subplan_get_plan((PlannerInfo*)base->node, subplan);
    else if (IsA(base->node, PlannerGlobal)) {
        PlannerInfo rootdata;
        rootdata.glob = (PlannerGlobal*)base->node;
        return planner_subplan_get_plan(&rootdata, subplan);
    }

    return NULL;
}

