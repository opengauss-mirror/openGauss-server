/*-------------------------------------------------------------------------
 *
 * nodeSplitUpdate.cpp
 *	  Implementation of nodeSplitUpdate.
 *
 * Portions Copyright (c) 2012, EMC Corp.
 * Portions Copyright (c) 2012-2022 VMware, Inc. or its affiliates.
 * Portions Copyright (c) 2023 Huawei Technologies Co.,Ltd.
 *
 * IDENTIFICATION
 *	    src/gausskernel/runtime/executor/nodeSplitUpdate.cpp
 *
 *-------------------------------------------------------------------------
 */
#ifdef USE_SPQ
#include "postgres.h"

#include "miscadmin.h"
#include "executor/node/nodeSplitUpdate.h"
#include "executor/executor.h"
#include "commands/tablecmds.h"
#include "executor/instrument.h"
#include "utils/memutils.h"


/* Splits an update tuple into a DELETE/INSERT tuples. */
static void SplitTupleTableSlot(TupleTableSlot *slot, List *targetList, SplitUpdate *plannode,
    SplitUpdateState *node, Datum *values, bool *nulls);

/* Memory used by node */
#define SPLITUPDATE_MEM 1

/* Split TupleTableSlot into a DELETE and INSERT TupleTableSlot */
static void SplitTupleTableSlot(TupleTableSlot *slot, List *targetList, SplitUpdate *plannode, SplitUpdateState *node,
    Datum *values, bool *nulls)
{
    ListCell *element;
    Datum *delete_values;
    bool *delete_nulls;
    Datum *insert_values;
    bool *insert_nulls;
    ListCell *deleteAtt = list_head(plannode->deleteColIdx);
    ListCell *insertAtt = list_head(plannode->insertColIdx);

    heap_slot_getallattrs(slot);
    delete_values = node->deleteTuple->tts_values;
    delete_nulls = node->deleteTuple->tts_isnull;
    insert_values = node->insertTuple->tts_values;
    insert_nulls = node->insertTuple->tts_isnull;

    /* Iterate through new TargetList and match old and new values. The action is also added in this containsTuple. */
    foreach (element, targetList) {
        TargetEntry *tle = (TargetEntry*)lfirst(element);
        AttrNumber attno = tle->resno;

        if (IsA(tle->expr, DMLActionExpr)) {
            /* Set the corresponding action to the new tuples. */
            delete_values[attno - 1] = Int32GetDatum((int)DML_DELETE);
            delete_nulls[attno - 1] = false;

            insert_values[attno - 1] = Int32GetDatum((int)DML_INSERT);
            insert_nulls[attno -1 ] = false;
        } else if (attno <= list_length(plannode->insertColIdx)) {
            /* Old and new values */
            int deleteAttNo = lfirst_int(deleteAtt);
            int insertAttNo = lfirst_int(insertAtt);

            if (deleteAttNo == -1) {
                delete_values[attno - 1] = (Datum) 0;
                delete_nulls[attno - 1] = true;
            } else {
                delete_values[attno - 1] = values[deleteAttNo - 1];
                delete_nulls[attno - 1] = nulls[deleteAttNo - 1];
            }

            insert_values[attno - 1] = values[insertAttNo - 1];
            insert_nulls[attno - 1] = nulls[insertAttNo - 1];

            deleteAtt = lnext(deleteAtt);
            insertAtt = lnext(insertAtt);
        } else {
            if (IsA(tle->expr, Var)) {
                Var *var = (Var *) tle->expr;

                Assert(var->varno == OUTER_VAR);

                delete_values[attno - 1] = values[var->varattno - 1];
                delete_nulls[attno - 1] = nulls[var->varattno - 1];

                insert_values[attno - 1] = values[var->varattno - 1];
                insert_nulls[attno - 1] = nulls[var->varattno - 1];

                Assert(var->vartype == TupleDescAttr(slot->tts_tupleDescriptor, var->varattno - 1)->atttypid);
            }
            /* `Resjunk' values */
        }
    }
}

/**
 * Splits every TupleTableSlot into two TupleTableSlots: DELETE and INSERT.
 */
static TupleTableSlot *ExecSplitUpdate(PlanState *pstate)
{
    SplitUpdateState *node = castNode(SplitUpdateState, pstate);
    PlanState *outerNode = outerPlanState(node);
    SplitUpdate *plannode = (SplitUpdate *) node->ps.plan;

    TupleTableSlot *slot = NULL;
    TupleTableSlot *result = NULL;

    Assert(outerNode != NULL);

    /* Returns INSERT TupleTableSlot. */
    if (!node->processInsert) {
        result = node->insertTuple;

        node->processInsert = true;
    } else {
        Datum *values;
        bool *nulls;
        /* Creates both TupleTableSlots. Returns DELETE TupleTableSlots.*/
        /* Get the Delete tuple */
        slot = ExecProcNode(outerNode);

        if (TupIsNull(slot)) {
            return NULL;
        }

        /* `Split' update into delete and insert */
        heap_slot_getallattrs(slot);
        values = slot->tts_values;
        nulls = slot->tts_isnull;

        ExecStoreAllNullTuple(node->deleteTuple);
        ExecStoreAllNullTuple(node->insertTuple);

        SplitTupleTableSlot(slot, plannode->plan.targetlist, plannode, node, values, nulls);

        result = node->deleteTuple;
        node->processInsert = false;

    }

    return result;
}

/*
 * Init SplitUpdate Node. A memory context is created to hold Split Tuples.
 * 
 */
SplitUpdateState* ExecInitSplitUpdate(SplitUpdate *node, EState *estate, int eflags)
{
    SplitUpdateState *splitupdatestate;
    Plan *outerPlan;
    bool has_oids;
    TupleDesc tupDesc;

    /* Check for unsupported flags */
    Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK | EXEC_FLAG_REWIND)));

    splitupdatestate = makeNode(SplitUpdateState);
    splitupdatestate->ps.plan = (Plan *)node;
    splitupdatestate->ps.state = estate;
    splitupdatestate->ps.ExecProcNode = ExecSplitUpdate;
    splitupdatestate->processInsert = true;

    /*
     * then initialize outer plan
     */
    outerPlan = outerPlan(node);
    outerPlanState(splitupdatestate) = ExecInitNode(outerPlan, estate, eflags);

    ExecAssignExprContext(estate, &splitupdatestate->ps);

    ExecInitResultTupleSlot(estate, &splitupdatestate->ps);

    if (!ExecContextForcesOids((PlanState*) splitupdatestate, &has_oids)) {
        has_oids = false;
    }

    tupDesc = ExecTypeFromTL(node->plan.targetlist, has_oids);
    splitupdatestate->insertTuple = ExecInitExtraTupleSlot(estate);
    ExecSetSlotDescriptor(splitupdatestate->insertTuple, tupDesc);
    splitupdatestate->deleteTuple = ExecInitExtraTupleSlot(estate);
    ExecSetSlotDescriptor(splitupdatestate->deleteTuple, tupDesc);

    /*
     * DML nodes do not project.
     */
    ExecAssignResultTypeFromTL(&splitupdatestate->ps);
    splitupdatestate->ps.ps_ProjInfo = NULL;

    return splitupdatestate;
}

/* Release Resources Requested by SplitUpdate node. */
void ExecEndSplitUpdate(SplitUpdateState *node)
{
    ExecFreeExprContext(&node->ps);
    ExecClearTuple(node->ps.ps_ResultTupleSlot);
    ExecClearTuple(node->insertTuple);
    ExecClearTuple(node->deleteTuple);
    ExecEndNode(outerPlanState(node));
}

#endif
