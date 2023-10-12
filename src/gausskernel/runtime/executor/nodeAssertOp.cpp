/*-------------------------------------------------------------------------
 *
 * nodeAssertOp.cpp
 *      Implementation of nodeAssertOp.
 *
 * Portions Copyright (c) 2023 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *        src/gausskernel/runtime/executor/nodeAssertOp.cpp
 *
 *-------------------------------------------------------------------------
 */
#ifdef USE_SPQ
#include "postgres.h"
#include "miscadmin.h"
 
#include "commands/tablecmds.h"
#include "executor/executor.h"
#include "executor/instrument.h"
#include "executor/node/nodeAssertOp.h"
 
/*
 * Estimated Memory Usage of AssertOp Node.
 **/
void ExecAssertOpExplainEnd(PlanState *planstate, struct StringInfoData *buf)
{
}
 
TupleTableSlot* ExecAssertOp(PlanState *state)
{
    AssertOpState *node = castNode(AssertOpState, state);
    List* qual = node->ps.qual;
    ExprContext* econtext = node->ps.ps_ExprContext;
    ProjectionInfo* proj_info = node->ps.ps_ProjInfo;
    AssertOp* plannode = (AssertOp *) node->ps.plan;
    StringInfoData errorString;
    PlanState *outerNode = outerPlanState(node);
    TupleTableSlot *slot = ExecProcNode(outerNode);
    if (TupIsNull(slot)) {
        return NULL;
    }  
    ResetExprContext(econtext);
    econtext->ecxt_outertuple = slot;
    initStringInfo(&errorString);
    if (!ExecQual(qual, econtext, false)) {
        Value *valErrorMessage = (Value *) list_nth(plannode->errmessage, 0);

        Assert(NULL != valErrorMessage && IsA(valErrorMessage, String) &&
               0 < strlen(strVal(valErrorMessage)));

        appendStringInfo(&errorString, "%s\n", strVal(valErrorMessage));
        ereport(ERROR,
                (errcode(plannode->errcode),
                 errmsg("one or more assertions failed"),
                 errdetail("%s", errorString.data)));
    } 
    pfree(errorString.data);
    ResetExprContext(econtext);    
    return ExecProject(proj_info, NULL);
}
 
/**
 * Init AssertOp, which sets the ProjectInfo and
 * the Constraints to evaluate.
 * */
AssertOpState* ExecInitAssertOp(AssertOp *node, EState *estate, int eflags)
{
    AssertOpState *assertOpState;
    TupleDesc tupDesc;
    Plan *outerPlan;
    /* Check for unsupported flags */
    Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));
    Assert(outerPlan(node) != NULL);    
    assertOpState = makeNode(AssertOpState);
    assertOpState->ps.plan = (Plan *) node;
    assertOpState->ps.state = estate;
    assertOpState->ps.ExecProcNode = ExecAssertOp;   
    ExecInitResultTupleSlot(estate, &assertOpState->ps);    
    /* Create expression evaluation context */
    ExecAssignExprContext(estate, &assertOpState->ps);
    assertOpState->ps.targetlist = (List*)ExecInitExpr((Expr*)node->plan.targetlist, (PlanState*)assertOpState);
    assertOpState->ps.qual = (List*)ExecInitExpr((Expr*)node->plan.qual, (PlanState*)assertOpState); 
    /*
     * Initialize outer plan
     */
    outerPlan = outerPlan(node);
    outerPlanState(assertOpState) = ExecInitNode(outerPlan, estate, eflags);    
    /*
     * Initialize result type and projection.
     */
    ExecAssignResultTypeFromTL(&assertOpState->ps);
    tupDesc = ExecTypeFromTL(node->plan.targetlist, false);
    ExecAssignProjectionInfo(&assertOpState->ps, tupDesc);    
   
    return assertOpState;
}
 
/* Rescan AssertOp */
void ExecReScanAssertOp(AssertOpState *node)
{
    /*
     * If chgParam of subnode is not null then plan will be re-scanned by
     * first ExecProcNode.
     */
    if (node->ps.lefttree &&
        node->ps.lefttree->chgParam == NULL)
        ExecReScan(node->ps.lefttree);
}
 
/* Release Resources Requested by AssertOp node. */
void ExecEndAssertOp(AssertOpState *node)
{
    ExecFreeExprContext(&node->ps);
    ExecEndNode(outerPlanState(node));
}
#endif /* USE_SPQ */
