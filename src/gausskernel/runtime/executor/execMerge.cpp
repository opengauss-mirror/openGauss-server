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
 *
 * execMerge.cpp
 *	  routines to handle Merge nodes relating to the MERGE command
 *
 * IDENTIFICATION
 *	  src/gausskernel/runtime/executor/execMerge.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/tableam.h"
#include "executor/executor.h"
#include "executor/node/nodeModifyTable.h"
#include "executor/exec/execMerge.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "access/heapam.h"

static void ExecMergeNotMatched(ModifyTableState* mtstate, EState* estate, TupleTableSlot* slot);
static bool ExecMergeMatched(ModifyTableState* mtstate, EState* estate, TupleTableSlot* slot, JunkFilter* junkfilter,
    ItemPointer tupleid, HeapTupleHeader oldtuple, Oid oldPartitionOid, int2 bucketid);
/*
 * Perform MERGE.
 */
void ExecMerge(ModifyTableState* mtstate, EState* estate, TupleTableSlot* slot, JunkFilter* junkfilter,
    ResultRelInfo* resultRelInfo)
{
    ExprContext* econtext = mtstate->ps.ps_ExprContext;
    ItemPointer tupleid;
    ItemPointerData tuple_ctid;
    bool matched = false;
    Datum datum;
    bool isNull = false;
    HeapTupleHeader oldtuple = NULL;
    Oid oldPartitionOid = InvalidOid;
    AttrNumber partOidNum;
    AttrNumber bucketIdNum;
    int2 bucketid = InvalidBktId;

    Assert(resultRelInfo->ri_RelationDesc->rd_rel->relkind == RELKIND_RELATION ||
           resultRelInfo->ri_RelationDesc->rd_rel->relkind == PARTTYPE_PARTITIONED_RELATION ||
           junkfilter != NULL);

    /*
     * Reset per-tuple memory context to free any expression evaluation
     * storage allocated in the previous cycle.
     */
    ResetExprContext(econtext);

    /*
     * We run a JOIN between the target relation and the source relation to
     * find a set of candidate source rows that has matching row in the target
     * table and a set of candidate source rows that does not have matching
     * row in the target table. If the join returns us a tuple with target
     * relation's tid set, that implies that the join found a matching row for
     * the given source tuple. This case triggers the WHEN MATCHED clause of
     * the MERGE. Whereas a NULL in the target relation's ctid column
     * indicates a NOT MATCHED case.
     */
    datum = ExecGetJunkAttribute(slot, junkfilter->jf_junkAttNo, &isNull);

    if (!isNull) {
        matched = true;
        tupleid = (ItemPointer)DatumGetPointer(datum);
        tuple_ctid = *tupleid; /* be sure we don't free ctid!! */
        tupleid = &tuple_ctid;

        if (RELATION_IS_PARTITIONED(resultRelInfo->ri_RelationDesc) ||
            RelationIsCUFormat(resultRelInfo->ri_RelationDesc)) {
            Datum tableOiddatum;
            bool tableOidisnull = false;

            partOidNum = resultRelInfo->ri_partOidAttNum;
            tableOiddatum = ExecGetJunkAttribute(slot, partOidNum, &tableOidisnull);

            if (tableOidisnull) {
                ereport(ERROR,
                    (errcode(ERRCODE_NULL_JUNK_ATTRIBUTE), errmsg("tableoid is null when merge partitioned table")));
            }

            oldPartitionOid = DatumGetObjectId(tableOiddatum);
        }

        if (RELATION_HAS_BUCKET(resultRelInfo->ri_RelationDesc)) {
            Datum bucketIddatum;
            bool bucketIdisnull = false;

            bucketIdNum = resultRelInfo->ri_bucketIdAttNum;
            bucketIddatum = ExecGetJunkAttribute(slot, bucketIdNum, &bucketIdisnull);

            if (bucketIdisnull) {
                ereport(ERROR, (errcode(ERRCODE_NULL_JUNK_ATTRIBUTE), errmsg("bucketid is null when merge table")));
            }

            bucketid = DatumGetObjectId(bucketIddatum);
        }

    } else {
        matched = false;
        tupleid = NULL; /* we don't need it for INSERT actions */
    }

    /*
     * If we are dealing with a WHEN MATCHED case, we execute the first action
     * for which the additional WHEN MATCHED AND quals pass. If an action
     * without quals is found, that action is executed.
     *
     * Similarly, if we are dealing with WHEN NOT MATCHED case, we look at the
     * given WHEN NOT MATCHED actions in sequence until one passes.
     *
     * Things get interesting in case of concurrent update/delete of the
     * target tuple. Such concurrent update/delete is detected while we are
     * executing a WHEN MATCHED action.
     *
     * A concurrent update can:
     *
     * 1. modify the target tuple so that it no longer satisfies the
     * additional quals attached to the current WHEN MATCHED action OR
     *
     * In this case, we are still dealing with a WHEN MATCHED case, but
     * we should recheck the list of WHEN MATCHED actions and choose the first
     * one that satisfies the new target tuple.
     *
     * 2. modify the target tuple so that the join quals no longer pass and
     * hence the source tuple no longer has a match.
     *
     * In the second case, the source tuple no longer matches the target tuple,
     * so we now instead find a qualifying WHEN NOT MATCHED action to execute.
     *
     * A concurrent delete, changes a WHEN MATCHED case to WHEN NOT MATCHED.
     *
     * ExecMergeMatched takes care of following the update chain and
     * re-finding the qualifying WHEN MATCHED action, as long as the updated
     * target tuple still satisfies the join quals i.e. it still remains a
     * WHEN MATCHED case. If the tuple gets deleted or the join quals fail, it
     * returns and we try ExecMergeNotMatched. Given that ExecMergeMatched
     * always make progress by following the update chain and we never switch
     * from ExecMergeNotMatched to ExecMergeMatched, there is no risk of a
     * livelock.
     */
    if (matched)
        matched = ExecMergeMatched(mtstate, estate, slot, junkfilter, tupleid, oldtuple, oldPartitionOid, bucketid);

    /*
     * Either we were dealing with a NOT MATCHED tuple or ExecMergeNotMatched()
     * returned "false", indicating the previously MATCHED tuple is no longer a
     * matching tuple.
     */
    if (!matched)
        ExecMergeNotMatched(mtstate, estate, slot);
}

/*
 * Extract tuple for checking constraints from plan slot
 */
static TupleTableSlot* ExtractConstraintTuple(
    ModifyTableState* mtstate, CmdType commandType, TupleTableSlot* slot, TupleDesc tupDesc)
{
    ExprContext* econtext = mtstate->ps.ps_ExprContext;
    AutoContextSwitch memContext(econtext->ecxt_per_tuple_memory);
    HeapTuple tempTuple = NULL;
    TupleTableSlot* constrSlot = NULL;
    Datum* values = (Datum*)palloc0(sizeof(Datum) * tupDesc->natts);
    bool* isnull = (bool*)palloc0(sizeof(bool) * tupDesc->natts);
    TupleDesc originTupleDesc = slot->tts_tupleDescriptor;
    int index = 0;
    int i = 0;

    switch (commandType) {
        case CMD_UPDATE:
            constrSlot = mtstate->mt_update_constr_slot;
            for (i = 0; i < originTupleDesc->natts; i++) {
                if (strstr(originTupleDesc->attrs[i]->attname.data, "action UPDATE target")) {
                    values[index] = slot->tts_values[i];
                    isnull[index] = slot->tts_isnull[i];
                    index++;
                }
            }
            break;
        case CMD_INSERT:
            constrSlot = mtstate->mt_insert_constr_slot;
            for (i = 0; i < originTupleDesc->natts; i++) {
                if (strstr(originTupleDesc->attrs[i]->attname.data, "action INSERT target")) {
                    values[index] = slot->tts_values[i];
                    isnull[index] = slot->tts_isnull[i];
                    index++;
                }
            }
            break;
        default:
            Assert(0);
    }

    Assert(constrSlot->tts_tupleDescriptor->tdTableAmType == originTupleDesc->tdTableAmType);
    tempTuple = (HeapTuple)tableam_tops_form_tuple(tupDesc, values, isnull, HEAP_TUPLE);
    (void)ExecStoreTuple(tempTuple, constrSlot, InvalidBuffer, false);

    return constrSlot;
}

/*
 * Extract scan tuple for target table from plan slot
 */
TupleTableSlot* ExtractScanTuple(ModifyTableState* mtstate, TupleTableSlot* slot, TupleDesc tupDesc)
{
    List* sourceTargetList = ((ModifyTable*)mtstate->ps.plan)->mergeSourceTargetList;
    ExprContext* econtext = mtstate->ps.ps_ExprContext;
    AutoContextSwitch memContext(econtext->ecxt_per_tuple_memory);
    HeapTuple tempTuple = NULL;
    TupleTableSlot* scanSlot = mtstate->mt_scan_slot;
    ListCell* lc = NULL;
    Datum* values = (Datum*)palloc0(sizeof(Datum) * tupDesc->natts);
    bool* isnull = (bool*)palloc0(sizeof(bool) * tupDesc->natts);
    int startIdx = 0;
    int index = 0;

    /*
     * Find the right start index for target table. We should skip the sourceTargetList.
     * First count the number of source targetlist. We add new columns to sourceTargetList
     * but the resno is not continuous, so find the max continuous number to be the original
     * length of sourceTargetList.
     */
    foreach (lc, sourceTargetList) {
        TargetEntry* tle = (TargetEntry*)lfirst(lc);
        if (tle->resno != startIdx + 1)
            break;
        startIdx++;
    }

    for (index = 0; index < tupDesc->natts; index++) {
        if (tupDesc->attrs[index]->attisdropped == true) {
            isnull[index] = true;
            continue;
        }

        Assert(startIdx < slot->tts_tupleDescriptor->natts);
        values[index] = tableam_tslot_getattr(slot, startIdx + 1, &isnull[index]);
        startIdx++;
    }

    tempTuple = (HeapTuple)tableam_tops_form_tuple(tupDesc, values, isnull, HEAP_TUPLE);
    (void)ExecStoreTuple(tempTuple, scanSlot, InvalidBuffer, false);

    return scanSlot;
}

/*
 * Description: projects and evaluates qual condition for update action.
 * Parameters:
 * @in mtstate: modifytable state.
 * @in mergeMatchedActionStates: update action states.
 * @in econtext: expression context.
 * @in originSlot: slot to be projected.
 * @in result_slot: slot to be returned.
 * @in estate: working state for executor.
 * Return: slot has been projected..
 */
TupleTableSlot* ExecMergeProjQual(ModifyTableState* mtstate, List* mergeMatchedActionStates, ExprContext* econtext,
    TupleTableSlot* originSlot, TupleTableSlot* result_slot, EState* estate)
{
    if (mergeMatchedActionStates != NIL) {
        MergeActionState* action = (MergeActionState*)linitial(mergeMatchedActionStates);
        ResultRelInfo* resultRelInfo = NULL;
        Relation resultRelationDesc;

        Assert(CMD_UPDATE == action->commandType);

        /*
         * get information on the (current) result relation
         */
        resultRelInfo = estate->es_result_relation_info;
        resultRelationDesc = resultRelInfo->ri_RelationDesc;

        /*
         * Make tuple and any needed join variables available to ExecQual and
         * ExecProject. The target's existing tuple is installed in the scantuple.
         * Again, this target relation's slot is required only in the case of a
         * MATCHED tuple and UPDATE/DELETE actions.
         */
        if (estate->es_result_update_remoterel == NULL) {
            econtext->ecxt_scantuple = ExtractScanTuple(mtstate, originSlot, action->tupDesc);
            econtext->ecxt_innertuple = originSlot;
            econtext->ecxt_outertuple = NULL;
        } else {
            econtext->ecxt_scantuple = originSlot;
            econtext->ecxt_innertuple = NULL;
            econtext->ecxt_outertuple = NULL;
        }

        /*
         * Test condition, if any
         *
         * In the absence of a condition we perform the action unconditionally
         * (no need to check separately since ExecQual() will return true if
         * there are no conditions to evaluate).
         */
        if (ExecQual((List*)action->whenqual, econtext, false)) {
            if (estate->es_result_update_remoterel == NULL) {
                /*
                 * We set up the projection earlier, so all we do here is
                 * Project, no need for any other tasks prior to the
                 * ExecUpdate.
                 */
                result_slot = ExecProject(action->proj, NULL);
            } else {
                /* we don't do projection in remote query */
            }

            /*
             * We don't call ExecFilterJunk() because the projected tuple
             * using the UPDATE action's targetlist doesn't have a junk
             * attribute.
             */
            if (estate->es_result_update_remoterel) {
                estate->es_result_remoterel = estate->es_result_update_remoterel;

                /* Check if has constraints */
                if (resultRelationDesc->rd_att->constr) {
                    mtstate->mt_update_constr_slot =
                        ExtractConstraintTuple(mtstate, CMD_UPDATE, result_slot, action->tupDesc);
                }
            }

            return result_slot;
        }
    }
    return NULL;
}

/*
 * Check and execute the first qualifying MATCHED action. The current target
 * tuple is identified by tupleid.
 *
 * We start from the first WHEN MATCHED action and check if the WHEN AND quals
 * pass, if any. If the WHEN AND quals for the first action do not pass, we
 * check the second, then the third and so on. If we reach to the end, no
 * action is taken and we return true, indicating that no further action is
 * required for this tuple.
 *
 * If we do find a qualifying action, then we attempt to execute the action.
 *
 * If the tuple is concurrently updated, EvalPlanQual is run with the updated
 * tuple to recheck the join quals. Note that the additional quals associated
 * with individual actions are evaluated separately by the MERGE code, while
 * EvalPlanQual checks for the join quals. If EvalPlanQual tells us that the
 * updated tuple still passes the join quals, then we restart from the first
 * action to look for a qualifying action. Otherwise, we return false meaning
 * that a NOT MATCHED action must now be executed for the current source tuple.
 */
static bool ExecMergeMatched(ModifyTableState* mtstate, EState* estate, TupleTableSlot* slot, JunkFilter* junkfilter,
    ItemPointer tupleid, HeapTupleHeader oldtuple, Oid oldPartitionOid, int2 bucketid)
{
    ExprContext* econtext = mtstate->ps.ps_ExprContext;
    List* mergeMatchedActionStates = NIL;
    EPQState* epqstate = &mtstate->mt_epqstate;
    ResultRelInfo* saved_resultRelInfo = NULL;
    ResultRelInfo* resultRelInfo = estate->es_result_relation_info;
    TupleTableSlot* saved_slot = slot;
    bool partKeyUpdated = ((ModifyTable*)mtstate->ps.plan)->partKeyUpdated;

    /*
     * Save the current information and work with the correct result relation.
     */
    saved_resultRelInfo = resultRelInfo;
    estate->es_result_relation_info = resultRelInfo;

    /*
     * And get the correct action lists.
     */
    mergeMatchedActionStates = resultRelInfo->ri_mergeState->matchedActionStates;

    if (mergeMatchedActionStates != NIL) {
        MergeActionState* action = (MergeActionState*)linitial(mergeMatchedActionStates);

        slot = ExecMergeProjQual(mtstate, mergeMatchedActionStates, econtext, slot, slot, estate);

        if (slot != NULL) {
            (void)ExecUpdate(tupleid,
                             oldPartitionOid,
                             bucketid,
                             oldtuple,
                             slot,
                             saved_slot,
                             epqstate,
                             mtstate,
                             mtstate->canSetTag,
                             partKeyUpdated);
        }
        if (action->commandType == CMD_UPDATE /* && tuple_updated*/)
            InstrCountFiltered2(&mtstate->ps, 1);

        /*
         * We've activated one of the WHEN clauses, so we don't search
         * further. This is required behaviour, not an optimization.
         */
        estate->es_result_relation_info = saved_resultRelInfo;
    }

    /*
     * Successfully executed an action or no qualifying action was found.
     */
    return true;
}

/*
 * Execute the first qualifying NOT MATCHED action.
 */
static void ExecMergeNotMatched(ModifyTableState* mtstate, EState* estate, TupleTableSlot* slot)
{
    ExprContext* econtext = mtstate->ps.ps_ExprContext;
    List* mergeNotMatchedActionStates = NIL;
    ResultRelInfo* resultRelInfo = NULL;
    TupleTableSlot* myslot = NULL;
    const int hi_options = 0;

    /*
     * We are dealing with NOT MATCHED tuple. Since for MERGE, the partition
     * tree is not expanded for the result relation, we continue to work with
     * the currently active result relation, which corresponds to the root
     * of the partition tree.
     */
    resultRelInfo = mtstate->resultRelInfo;

    /*
     * For INSERT actions, root relation's merge action is OK since the
     * INSERT's targetlist and the WHEN conditions can only refer to the
     * source relation and hence it does not matter which result relation we
     * work with.
     */
    mergeNotMatchedActionStates = resultRelInfo->ri_mergeState->notMatchedActionStates;

    /*
     * Make source tuple available to ExecQual and ExecProject. We don't need
     * the target tuple since the WHEN quals and the targetlist can't refer to
     * the target columns.
     */
    if (estate->es_result_insert_remoterel == NULL) {
        econtext->ecxt_scantuple = slot;
        econtext->ecxt_innertuple = slot;
        econtext->ecxt_outertuple = NULL;
    } else {
        econtext->ecxt_scantuple = slot;
        econtext->ecxt_innertuple = NULL;
        econtext->ecxt_outertuple = NULL;
    }

    if (mergeNotMatchedActionStates != NIL) {
        MergeActionState* action = (MergeActionState*)linitial(mergeNotMatchedActionStates);
        ResultRelInfo* resultRelationInfo = NULL;
        Relation resultRelationDesc;

        Assert(CMD_INSERT == action->commandType);

        /*
         * get information on the (current) result relation
         */
        resultRelationInfo = estate->es_result_relation_info;
        resultRelationDesc = resultRelationInfo->ri_RelationDesc;

        /*
         * Test condition, if any
         *
         * In the absence of a condition we perform the action unconditionally
         * (no need to check separately since ExecQual() will return true if
         * there are no conditions to evaluate).
         */
        if (ExecQual((List*)action->whenqual, econtext, false)) {
            /*
             * We set up the projection earlier, so all we do here is
             * Project, no need for any other tasks prior to the
             * ExecInsert.
             */
            if (estate->es_result_insert_remoterel == NULL) {
                ExecProject(action->proj, NULL);
                /*
                 * ExecPrepareTupleRouting may modify the passed-in slot. Hence
                 * pass a local reference so that action->slot is not modified.
                 */
                myslot = mtstate->mt_mergeproj;
            } else {
                /* in pgxc we do projection in the remote query*/
                myslot = slot;

                /* Check if has constraints */
                if (resultRelationDesc->rd_att->constr) {
                    mtstate->mt_insert_constr_slot = ExtractConstraintTuple(mtstate, CMD_INSERT, slot, action->tupDesc);
                }
            }

            estate->es_result_remoterel = estate->es_result_insert_remoterel;

            (void)ExecInsertT<false>(mtstate, myslot, slot, estate, mtstate->canSetTag, hi_options, NULL);

            InstrCountFiltered1(&mtstate->ps, 1);
        }
    }
}

/*
 * Creates the run-time state information for the Merge node
 */
void ExecInitMerge(ModifyTableState* mtstate, EState* estate, ResultRelInfo* resultRelInfo)
{
    ListCell* l = NULL;
    ExprContext* econtext = NULL;
    List* mergeMatchedActionStates = NIL;
    List* mergeNotMatchedActionStates = NIL;
    TupleDesc relationDesc = resultRelInfo->ri_RelationDesc->rd_att;
    ModifyTable* node = (ModifyTable*)mtstate->ps.plan;

    if (node->mergeActionList == NIL)
        return;

    mtstate->mt_merge_subcommands = 0;

    if (mtstate->ps.ps_ExprContext == NULL)
        ExecAssignExprContext(estate, &mtstate->ps);

    econtext = mtstate->ps.ps_ExprContext;

    /* initialize scan slot and constraint slot */
    mtstate->mt_scan_slot = NULL;
    mtstate->mt_update_constr_slot = NULL;
    mtstate->mt_insert_constr_slot = NULL;

    /* initialize slot for merge actions */
    Assert(mtstate->mt_mergeproj == NULL);
    mtstate->mt_mergeproj = ExecInitExtraTupleSlot(mtstate->ps.state);
    ExecSetSlotDescriptor(mtstate->mt_mergeproj, relationDesc);

    /*
     * Create a MergeActionState for each action on the mergeActionList
     * and add it to either a list of matched actions or not-matched
     * actions.
     */
    foreach (l, node->mergeActionList) {
        MergeAction* action = (MergeAction*)lfirst(l);
        MergeActionState* action_state = makeNode(MergeActionState);
        TupleDesc tupDesc;
        List* targetList = NULL;

        action_state->matched = action->matched;
        action_state->commandType = action->commandType;
        action_state->whenqual = ExecInitExpr((Expr*)action->qual, &mtstate->ps);

        /* create target slot for this action's projection */
        tupDesc = ExecTypeFromTL((List*)action->targetList, false, true, relationDesc->tdTableAmType);
        action_state->tupDesc = tupDesc;

        if (IS_PGXC_DATANODE && CMD_UPDATE == action->commandType) {
            mtstate->mt_scan_slot = MakeSingleTupleTableSlot(tupDesc);
        }

        if (IS_PGXC_COORDINATOR && CMD_UPDATE == action->commandType && relationDesc->constr != NULL) {
            mtstate->mt_update_constr_slot = MakeSingleTupleTableSlot(tupDesc);
        }

        if (IS_PGXC_COORDINATOR && CMD_INSERT == action->commandType && relationDesc->constr != NULL) {
            mtstate->mt_insert_constr_slot = MakeSingleTupleTableSlot(tupDesc);
        }

        /* build action projection state */
        targetList = (List*)ExecInitExpr((Expr*)action->targetList, &mtstate->ps);
        action_state->proj = ExecBuildProjectionInfo(targetList, econtext, mtstate->mt_mergeproj, relationDesc);

        /*
         * We create two lists - one for WHEN MATCHED actions and one
         * for WHEN NOT MATCHED actions - and stick the
         * MergeActionState into the appropriate list.
         */
        if (action_state->matched)
            mergeMatchedActionStates = lappend(mergeMatchedActionStates, action_state);
        else
            mergeNotMatchedActionStates = lappend(mergeNotMatchedActionStates, action_state);

        switch (action->commandType) {
            case CMD_INSERT:
                ExecCheckPlanOutput(resultRelInfo->ri_RelationDesc, action->targetList);
                mtstate->mt_merge_subcommands |= MERGE_INSERT;
                break;
            case CMD_UPDATE:
                ExecCheckPlanOutput(resultRelInfo->ri_RelationDesc, action->targetList);
                mtstate->mt_merge_subcommands |= MERGE_UPDATE;
                break;
            default:
                Assert(0);
                break;
        }

        resultRelInfo->ri_mergeState->matchedActionStates = mergeMatchedActionStates;
        resultRelInfo->ri_mergeState->notMatchedActionStates = mergeNotMatchedActionStates;
    }
}
