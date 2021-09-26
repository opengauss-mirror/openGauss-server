/* -------------------------------------------------------------------------
 *
 * nodeStub.cpp
 *	  routines to Sub function of Init/Proc/End
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/gausskernel/runtime/executor/nodeStub.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "executor/executor.h"
#include "executor/node/nodeStub.h"
#include "vecexecutor/vecnodes.h"
#include "executor/node/nodeSeqscan.h"
#include "executor/node/nodeIndexscan.h"
#include "executor/node/nodeIndexonlyscan.h"
#include "executor/node/nodeBitmapIndexscan.h"
#include "executor/node/nodeBitmapHeapscan.h"
#include "executor/node/nodeTidscan.h"

extern char* nodeTagToString(NodeTag type);

PlanState* ExecInitNodeStubNorm(Plan* node, EState* estate, int eflags)
{
    Plan* inner = NULL;
    Plan* outer = NULL;
    PlanState* ps = NULL;

    Assert(node != NULL);

    /* Create base PlanState data structure */
    switch (nodeTag(node)) {
        case T_ModifyTable:
        case T_VecModifyTable: {
            ModifyTable* mtplan = (ModifyTable*)node;

            if (mtplan->is_dist_insertselect) {
                ps = (PlanState*)makeNode(DistInsertSelectState);
            } else {
                ps = (PlanState*)makeNode(ModifyTableState);
            }
        } break;
        case T_SubqueryScan:
            ps = (PlanState*)makeNode(SubqueryScanState);
            break;
        default: {
            ps = (PlanState*)makeNode(PlanState);
            break;
        }
    }

    ps->plan = node;
    ps->state = estate;
    ps->stubType = PST_Norm;
    inner = innerPlan(node);
    outer = outerPlan(node);

    /* Do specific node initilization */
    switch (nodeTag(node)) {
        case T_ModifyTable:
        case T_VecModifyTable: {
            ModifyTable* mtplan = (ModifyTable*)node;
            ModifyTableState* mtstate = (ModifyTableState*)ps;
            int nplans = 0;
            ListCell* l = NULL;
            int i = 0;

            mtstate->operation = mtplan->operation;
            mtstate->mt_done = false;
            nplans = list_length(mtplan->plans);
            mtstate->mt_nplans = nplans;

            TupleDesc tupDesc = ExecTypeFromTL(NIL, false);
            ExecInitResultTupleSlot(estate, &mtstate->ps);
            ExecAssignResultType(&mtstate->ps, tupDesc);

            mtstate->mt_plans = (PlanState**)palloc0(sizeof(PlanState*) * nplans);
            foreach (l, mtplan->plans) {
                Plan* _plan = (Plan*)lfirst(l);
                mtstate->mt_plans[i] = ExecInitNode(_plan, estate, eflags);
                i++;
            }

            return (PlanState*)mtstate;
        } break;

        case T_SubqueryScan: {
            SubqueryScan* sbplan = (SubqueryScan*)node;
            SubqueryScanState* sbstate = (SubqueryScanState*)ps;
            sbstate->subplan = ExecInitNode(sbplan->subplan, estate, eflags);
        } break;

        default: {
            /* Initialize inner/outer plannode if necessary */
            innerPlanState(ps) = ExecInitNode(inner, estate, eflags);
            outerPlanState(ps) = ExecInitNode(outer, estate, eflags);
            break;
        }
    }

    /* Result tuple initialization */
    ExecInitResultTupleSlot(estate, ps);
    ExecAssignResultTypeFromTL(ps, TAM_HEAP);

    return ps;
}

TupleTableSlot* ExecProcNodeStub(PlanState* node)
{
    return NULL;
}

void ExecEndNodeStubNorm(PlanState* node)
{
    PlanState *inner, *outer;

    /* common operations */
    inner = innerPlanState(node);
    outer = outerPlanState(node);

    /* do node specific finialization */
    switch (nodeTag(node->plan)) {
        case T_ModifyTable: {
            ModifyTableState* mtstate = (ModifyTableState*)node;
            for (int i = 0; i < mtstate->mt_nplans; i++) {
                ExecEndNode(mtstate->mt_plans[i]);
            }
        } break;

        case T_SubqueryScan: {
            SubqueryScanState* sbstate = (SubqueryScanState*)node;
            ExecEndNode(sbstate->subplan);
        } break;

        default: {
            ExecEndNode(inner);
            ExecEndNode(outer);
            break;
        }
    }
}
void ExecEndNodeStubScan(PlanState* node)
{
    switch (nodeTag(node->plan)) {
        case T_SeqScan:
            ExecEndSeqScan((SeqScanState*)node);
            break;
        case T_IndexScan:
            ExecEndIndexScan((IndexScanState*)node);
            break;
        case T_IndexOnlyScan:
            ExecEndIndexOnlyScan((IndexOnlyScanState*)node);
            break;
        case T_BitmapIndexScan:
            ExecEndBitmapIndexScan((BitmapIndexScanState*)node);
            break;
        case T_BitmapHeapScan:
            ExecEndBitmapHeapScan((BitmapHeapScanState*)node);
            break;
        case T_TidScan:
            ExecEndTidScan((TidScanState*)node);
            break;
        default:
            break;
    }
}
void ExecEndNodeStub(PlanState* node)
{
    if (node->stubType == PST_Scan) {
        ExecEndNodeStubScan(node);
    } else {
        ExecEndNodeStubNorm(node);
    }
}
