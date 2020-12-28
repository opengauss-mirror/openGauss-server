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
 * planmem_walker.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/optimizer/planmem_walker.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef PLANWALKER_H
#define PLANWALKER_H

#include "nodes/nodes.h"
#include "nodes/plannodes.h"
#include "nodes/relation.h"

typedef bool (*MethodWalker)();
typedef bool (*MethodP2Walker)(Node*, void*);

/* Traverse flag during plan tree walker */
#define UNDER_STREAM 0x01 /* this flag indicates that current plan node is under stream */
#define UNDER_MULTI_GROUP_OP                              \
    0x02 /* this flag indicates that current plan node is \
            under node with multiple group, like join, or append */

/*
 * The plan associated with a SubPlan is found in a list.  During planning this is in
 * the global structure found through the root PlannerInfo.  After planning this is in
 * the PlannedStmt.
 *
 * Structure plan_tree_base_prefix carries the appropriate pointer for general plan
 * tree walker/mutator framework.  All users of the framework must prefix their context
 * structure with a plan_tree_base_prefix and initialize it appropriately.
 */
typedef struct plan_tree_base_prefix {
    Node* node;          /* PlannerInfo* or PlannedStmt* */
    List* init_plans;    /* with all traversed init plan saved, and below nodes can find reference from it */
    bool* traverse_flag; /* flag that indicates whether the subplan is traversed, to prevent duplicate traverse */
} plan_tree_base_prefix;

/* Memory calculation and control phase */
typedef enum {
    ASSIGN_MEM = 0,   /* memory assignment phase, give memory to each plan node */
    DECREASE_MEM,     /* memory decrease phase, only happens when query mem exceeds threshold */
    ADJUST_MEM,       /* memory adjust phase, to prevent huge initial memory allocation or early disk spill */
    SPREAD_ADJUST_MEM /* memory spread calculation and adjust phase, to calculate maximum memory to spread */
} TraversePhase;

#define ADJ_PHASE(context) ((context)->phase == ADJUST_MEM || (context)->phase == SPREAD_ADJUST_MEM)

/* each nodegroup's OperatorGroup mem */
typedef struct OperatorGroupNodeMem {
    /*
     * The following memory usage are all arrays, and we use the first one for normal memory
     * usage, and second one for min memory usage if not equal to normal one, else 0
     */
    /* The memory limit for this group and its child groups */
    int groupMemKB[2];
    /* The memory limit for the operators visible to this group */
    int groupBlockedOpMemKB[2];
    /* The memory limit during the group execution */
    int groupConcMemKB[2];
    /* The memory limit for child groups below stream */
    int belowStreamConcMemKB[2];
    /* The memory limit of operators in this group */
    int groupOpMemKB[2];

    /* memory that need to decreased */
    int decreaseMemKB;
    /* memory that group operator need to decrease */
    int groupOpDMemKB;
    /* memory that group top node need to decrease */
    int topNodeDMemKB;
} OperatorGroupNodeMem;

/*
 * OperatorGroupNode
 *    Store the information regarding an operator group.
 *	See details in memctl.cpp
 */
typedef struct OperatorGroupNode {
    /* The id and top node for this group */
    int groupId;
    Plan* topNode;

    /* The list of child groups */
    List* childGroups;

    /* The parent group */
    struct OperatorGroupNode* parentGroup;

    List* belowStreamGroups; /* list of virtual groups below
                                stream, which are the real co-exist groups */

    bool virtualGroup;     /* groups with memory non-intensive operator as top node */
    bool underStreamGroup; /* virtual group that under stream node */

    int childLevel; /* number of children group levels below */
    int outerLevel; /* number of children group levels for hash join outer side */

    /* The memory info for each nodegroup in this operator group node */
    OperatorGroupNodeMem* ng_groupMemKBArray;
    int ng_num;
} OperatorGroupNode;

typedef struct QueryMemKB {
    int queryMemKB;        /* the query memory limit */
    int minQueryMemKB;     /* the minimum query memory limit */
    int currQueryMemKB;    /* the query memory limit of current plan */
    int minCurrQueryMemKB; /* the minimum query memory limit of current plan */

    int availMemKB;       /* total expected memory for the query */
    int streamTotalMemKB; /* total memory consumption of stream, which can't be decrease */

    int assigned_query_mem_1; /* assigned_query_mem[1] for each nodegroup */
} QueryMemKB;

/*
 * MethodEarlyFreeContext
 *   Store the intermediate states during the tree walking for the optimize
 * memory distribution policy.
 */
typedef struct MethodPlanWalkerContext {
    plan_tree_base_prefix base;

    TraversePhase phase;          /* Traverse phase */
    bool dnExec;                  /* current node is executed on datanode */
    OperatorGroupNode* groupTree; /* the root of the group tree */
    OperatorGroupNode* groupNode; /* the current group node in the group tree */

    int nextGroupId; /* the group id for a new group node */

    PlannedStmt* plannedStmt; /* pointer to the planned statement */

    unsigned int status; /* current op is under stream */
    List* groupTreeList; /* list of group trees of a query */
    int groupTreeIdx;    /* current group tree index when re-traverse a plan tree */
    bool use_tenant;     /* multi-tenant scenario, and we don't allow auto spread in this case */

    List* ng_distributionList;      /* related nodegroup list for logic cluster scenario */
    int ng_num;                     /* 1 plus number of logic clusters */
    QueryMemKB* ng_queryMemKBArray; /* query memory for each nodegroup */
} MethodPlanWalkerContext;

typedef struct PredpushPlanWalkerContext {
    MethodPlanWalkerContext mpwc;

    bool predpush_stream; /* is predpush meet a stream */
} PredpushPlanWalkerContext;

extern void exec_init_plan_tree_base(plan_tree_base_prefix* base, PlannedStmt* stmt);

extern Plan* plan_tree_base_subplan_get_plan(plan_tree_base_prefix* base, SubPlan* subplan);

extern bool walk_plan_node_fields(Plan* plan, MethodWalker walker, void* context);

extern bool plan_tree_walker(Node* node, MethodWalker walker, void* context);

extern bool IsBlockedJoinNode(Plan* node);

#endif /* PLANWALKER_H */
