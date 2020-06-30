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
#include "executor/nodeAgg.h"
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
#include "utils/tqual.h"
#include "vecexecutor/vecfunc.h"
#include "executor/nodeRecursiveunion.h"
#include "optimizer/randomplan.h"
#include "optimizer/optimizerdebug.h"

void set_recursive_cteplan_ref(Plan* node, RecursiveRefContext* context)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
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
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}
