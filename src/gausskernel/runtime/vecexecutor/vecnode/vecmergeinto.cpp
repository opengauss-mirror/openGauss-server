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
 * vecmergeinto.cpp
 *	  Routines to handle Merge nodes relating to the MERGE command.
 *
 *
 *
 * IDENTIFICATION
 *	  Code/src/gausskernel/runtime/vecexecutor/vecnode/vecmergeinto.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/cstore_insert.h"
#include "access/cstore_update.h"
#include "access/dfs/dfs_insert.h"
#include "access/dfs/dfs_update.h"
#include "executor/executor.h"
#include "executor/exec/execMerge.h"
#include "executor/node/nodeModifyTable.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "access/heapam.h"
#include "vecexecutor/vecexecutor.h"
#include "vecexecutor/vecmergeinto.h"
#include "vecexecutor/vecmodifytable.h"

/*
 * Extract scan batch for target table from plan batch
 */
static VectorBatch* extract_scan_batch(
    VecModifyTableState* mtstate, VectorBatch* update_batch, MergeActionState* matched_action)
{
    List* source_target_list = ((ModifyTable*)mtstate->ps.plan)->mergeSourceTargetList;
    ExprContext* econtext = mtstate->ps.ps_ExprContext;
    AutoContextSwitch mem_context(econtext->ecxt_per_tuple_memory);
    VectorBatch* scan_batch = matched_action->scanBatch;
    TupleDesc tup_desc = matched_action->tupDesc;
    ListCell* lc = NULL;
    int start_idx = 0;
    int index = 0;

    /*
     * Find the right start index for target table. We should skip the sourceTargetList.
     * First count the number of source targetlist. We add new columns to sourceTargetList
     * but the resno is not continuous, so find the max continuous number to be the original
     * length of sourceTargetList.
     */
    foreach (lc, source_target_list) {
        TargetEntry* tle = (TargetEntry*)lfirst(lc);
        if (tle->resno != start_idx + 1)
            break;
        start_idx++;
    }

    for (index = 0; index < tup_desc->natts; index++) {
        if (tup_desc->attrs[index]->attisdropped == true) {
            scan_batch->m_arr[index].SetAllNull();
            continue;
        }

        scan_batch->m_arr[index].copy(&update_batch->m_arr[start_idx]);
        start_idx++;
    }

    scan_batch->FixRowCount(update_batch->m_rows);
    return scan_batch;
}

/*
 * Build dummy junkfilter for cstore update
 */
static JunkFilter* build_dummy_junk_filter(
    ModifyTableState* mtstate, List* new_targetlist, EState* estate, ResultRelInfo* result_rel_info)
{
    JunkFilter* junk_filter = NULL;
    ListCell* cell = NULL;
    Index new_index = list_length(new_targetlist);
    List* target_list = mtstate->mt_plans[0]->plan->targetlist;

    foreach (cell, target_list) {
        TargetEntry* tle = (TargetEntry*)lfirst(cell);
        TargetEntry* res_tle = NULL;

        if (tle->resjunk) {
            res_tle = (TargetEntry*)copyObject(tle);
            new_index++;
            ((Var*)res_tle->expr)->varno = 1;
            ((Var*)res_tle->expr)->varattno = new_index;
            new_targetlist = lappend(new_targetlist, res_tle);
            res_tle->resno = new_index;
        }
    }

    junk_filter = ExecInitJunkFilter(new_targetlist, false, ExecInitExtraTupleSlot(estate));

    junk_filter->jf_junkAttNo = ExecFindJunkAttribute(junk_filter, "ctid");
    if (!AttributeNumberIsValid(junk_filter->jf_junkAttNo)) {
        ereport(ERROR,
            (errmodule(MOD_VEC_EXECUTOR),
                (errcode(ERRCODE_INVALID_ATTRIBUTE), errmsg("could not find junk ctid column"))));
    }

    /* if the table is partitioned table ,give a paritionOidJunkOid junk */
    if (RELATION_IS_PARTITIONED(result_rel_info->ri_RelationDesc) || RelationIsCUFormat(result_rel_info->ri_RelationDesc)) {
        AttrNumber tableOidAttNum = ExecFindJunkAttribute(junk_filter, "tableoid");
        if (!AttributeNumberIsValid(tableOidAttNum)) {
            ereport(ERROR,
                (errmodule(MOD_VEC_EXECUTOR),
                    (errcode(ERRCODE_INVALID_ATTRIBUTE), errmsg("could not find junk tableoid column"))));
        }

        result_rel_info->ri_partOidAttNum = tableOidAttNum;
        junk_filter->jf_xc_part_id = result_rel_info->ri_partOidAttNum;
    }

    Assert(!RELATION_HAS_BUCKET(result_rel_info->ri_RelationDesc));

    if (!IS_SINGLE_NODE)
        junk_filter->jf_xc_node_id = ExecFindJunkAttribute(junk_filter, "xc_node_id");

    return junk_filter;
}

/*
 * Perform MERGE for Vector Engine.
 */
void ExecVecMerge(VecModifyTableState* mtstate)
{
    EState* estate = mtstate->ps.state;
    void* batch_opt_update = NULL;
    void* batch_opt_insert = NULL;
    ResultRelInfo* saved_result_rel_info = NULL;
    ResultRelInfo* result_rel_info = NULL;
    Relation result_relation_desc = InvalidRelation;
    bool is_partitioned = false;
    InsertArg args;
    const int update_options = 0;
    const int insert_options = 0;
    List* merge_matched_action_states = NIL;
    List* merge_not_matched_action_states = NIL;
    MergeActionState* matched_action = NULL;
    MergeActionState* not_matched_action = NULL;
    VectorBatch* plan_batch = NULL;
    VectorBatch* update_batch = NULL;
    VectorBatch* insert_batch = NULL;
    VectorBatch* result_batch = NULL;
    bool* update_sel = NULL;
    bool* insert_sel = NULL;
    ExprContext* econtext = mtstate->ps.ps_ExprContext;
    /* data redistribution for DFS table. */
    Relation data_dest_rel = InvalidRelation;
    PlanState* sub_plan_state = NULL;
    JunkFilter* junkfilter = NULL;
    PlanState* remote_rel_state = NULL;
    PlanState* saved_result_remote_rel = NULL;
    errno_t rc = EOK;

    /* Preload local variables */
    result_rel_info = mtstate->resultRelInfo + mtstate->mt_whichplan;
    sub_plan_state = mtstate->mt_plans[mtstate->mt_whichplan];

    /* Initialize remote plan state */
    remote_rel_state = mtstate->mt_remoterels[mtstate->mt_whichplan];

    junkfilter = result_rel_info->ri_junkFilter;
    result_relation_desc = result_rel_info->ri_RelationDesc;
    is_partitioned = RELATION_IS_PARTITIONED(result_relation_desc);

    /*
     * es_result_relation_info must point to the currently active result
     * relation while we are within this ModifyTable node.	Even though
     * ModifyTable nodes can't be nested statically, they can be nested
     * dynamically (since our subplan could include a reference to a modifying
     * CTE).  So we have to save and restore the caller's value.
     */
    saved_result_rel_info = estate->es_result_relation_info;
    saved_result_remote_rel = estate->es_result_remoterel;

    estate->es_result_relation_info = result_rel_info;
    estate->es_result_remoterel = remote_rel_state;

    /*
     * And get the correct action lists.
     */
    merge_matched_action_states = result_rel_info->ri_mergeState->matchedActionStates;
    merge_not_matched_action_states = result_rel_info->ri_mergeState->notMatchedActionStates;

    if (merge_matched_action_states != NIL) {
        JunkFilter* saved_junkfilter = result_rel_info->ri_junkFilter;

        matched_action = (MergeActionState*)linitial(merge_matched_action_states);
        result_rel_info->ri_junkFilter = matched_action->junkfilter;
        batch_opt_update = CreateOperatorObject(CMD_UPDATE, is_partitioned, result_relation_desc, result_rel_info,
            estate, matched_action->tupDesc, &args, &data_dest_rel, mtstate);
        result_rel_info->ri_junkFilter = saved_junkfilter;
    }

    if (merge_not_matched_action_states != NIL) {
        TupleDesc tup_desc = NULL;
        not_matched_action = (MergeActionState*)linitial(merge_not_matched_action_states);
        tup_desc = ExecGetResultType(sub_plan_state);
        batch_opt_insert = CreateOperatorObject(CMD_INSERT, is_partitioned, result_relation_desc, result_rel_info,
            estate, tup_desc, &args, &data_dest_rel, mtstate);

        insert_batch = New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, tup_desc);
    }

    update_sel = (bool*)palloc0(sizeof(bool) * BatchMaxSize);
    insert_sel = (bool*)palloc0(sizeof(bool) * BatchMaxSize);

    /*
     * Fetch rows from subplan(s), and execute the required table modification
     * for each row.
     */
    for (;;) {
        /*
         * Reset per-tuple memory context to free any expression evaluation
         * storage allocated in the previous cycle.
         */
        ResetExprContext(econtext);

        plan_batch = VectorEngine(sub_plan_state);
        if (BatchIsNull(plan_batch)) {
            Assert(!((++mtstate->mt_whichplan) < mtstate->mt_nplans));
            break;
        }

        rc = memset_s(update_sel, BatchMaxSize, 0, BatchMaxSize);
        securec_check(rc, "\0", "\0");
        rc = memset_s(insert_sel, BatchMaxSize, 0, BatchMaxSize);
        securec_check(rc, "\0", "\0");
        for (int i = 0; i < plan_batch->m_rows; i++) {
            if (IS_NULL(plan_batch->m_arr[junkfilter->jf_junkAttNo - 1].m_flag[i])) {
                insert_sel[i] = true;
            } else {
                update_sel[i] = true;
            }
        }

        if (merge_not_matched_action_states != NIL) {
            /* copy all batch data, must before updateBatch pack */
            
            insert_batch->Reset(true);
            insert_batch->Copy<true, false>(plan_batch);
            insert_batch->PackT<true, false>(insert_sel);
        }

        if (merge_matched_action_states != NIL) {
            update_batch = plan_batch;
            update_batch->PackT<true, false>(update_sel);
        }

        /*
         * If there are WHEN MATCHED action.
         */
        if (merge_matched_action_states != NIL && !BatchIsNull(update_batch)) {
            JunkFilter* saved_junkfilter = result_rel_info->ri_junkFilter;
            result_rel_info->ri_junkFilter = matched_action->junkfilter;

            econtext->ecxt_scanbatch = extract_scan_batch(mtstate, update_batch, matched_action);
            econtext->ecxt_innerbatch = update_batch;
            econtext->ecxt_outerbatch = NULL;

            if (matched_action->whenqual) {
                (void*)ExecVecQual((List*)matched_action->whenqual, econtext, false);

                update_batch->Pack(econtext->ecxt_scanbatch->m_sel);
                econtext->ecxt_scanbatch->FixRowCount(update_batch->m_rows);

                econtext->ecxt_scanbatch = extract_scan_batch(mtstate, update_batch, matched_action);
            }

            if (update_batch->m_rows > 0) {
                if (junkfilter != NULL) {
                    /*
                     * Apply the junkfilter if needed.
                     */
                    update_batch = BatchExecFilterJunk(junkfilter, update_batch);
                }

                result_batch = ExecVecProject(matched_action->proj);

                if (RelationIsCUFormat(result_relation_desc)) {
                    (void*)ExecVecUpdate<CStoreUpdate>(
                        mtstate, (CStoreUpdate*)batch_opt_update, result_batch, estate, mtstate->canSetTag, update_options);
                } else if (RelationIsPAXFormat(result_relation_desc)) {
                    (void*)ExecVecUpdate<DfsUpdate>(
                        mtstate, (DfsUpdate*)batch_opt_update, result_batch, estate, mtstate->canSetTag, update_options);
                } else {
                    Assert(false);
                    ereport(ERROR, (errcode(ERRCODE_INVALID_OPERATION),
                            errmsg("\"UPDATE\" is not supported by the type of relation.")));
                }

                InstrCountFiltered2(&mtstate->ps, result_batch->m_rows);
                result_batch->Reset();
            }

            result_rel_info->ri_junkFilter = saved_junkfilter;
        }

        /*
         * If there are WHEN NOT MATCHED action.
         */
        if (merge_not_matched_action_states != NIL && !BatchIsNull(insert_batch)) {
            econtext->ecxt_scanbatch = insert_batch;
            econtext->ecxt_innerbatch = insert_batch;
            econtext->ecxt_outerbatch = NULL;

            if (not_matched_action->whenqual) {
                (void*)ExecVecQual((List*)not_matched_action->whenqual, econtext, false);
                insert_batch->Pack(econtext->ecxt_scanbatch->m_sel);
            }

            if (insert_batch->m_rows > 0) {
                result_batch = ExecVecProject(not_matched_action->proj);

                if (is_partitioned) {
                    (void*)ExecVecInsert<CStorePartitionInsert>(mtstate,
                        (CStorePartitionInsert*)batch_opt_insert,
                        result_batch,
                        plan_batch,
                        estate,
                        mtstate->canSetTag,
                        insert_options);
                } else {
                    if (RelationIsCUFormat(result_relation_desc)) {
                        (void*)ExecVecInsert<CStoreInsert>(mtstate,
                            (CStoreInsert*)batch_opt_insert, result_batch, plan_batch,
                            estate, mtstate->canSetTag, insert_options);
                    } else if (RelationIsPAXFormat(result_relation_desc)) {
                        (void*)ExecVecInsert<DfsInsertInter>(mtstate,
                            (DfsInsertInter*)batch_opt_insert, result_batch, plan_batch,
                            estate,  mtstate->canSetTag, insert_options);
                    } else {
                        Assert(false);
                        ereport(ERROR, (errcode(ERRCODE_INVALID_OPERATION),
                                errmsg("\"INSERT\" is not supported by the type of relation.")));
                    }
                }

                InstrCountFiltered1(&mtstate->ps, result_batch->m_rows);
                result_batch->Reset();
            }
        }

        if (plan_batch != NULL)
            plan_batch->Reset();
    }

    /* process last data in operator cached */
    
    if (merge_matched_action_states != NIL) {
        if (RelationIsCUFormat(result_relation_desc)) {
            ((CStoreUpdate*)batch_opt_update)->EndUpdate(update_options);
            DELETE_EX_TYPE(batch_opt_update, CStoreUpdate);
        } else if (RelationIsPAXFormat(result_relation_desc)) {
            ((DfsUpdate*)batch_opt_update)->EndUpdate(update_options);
            DELETE_EX_TYPE(batch_opt_update, DfsUpdate);
        }
    }

    if (merge_not_matched_action_states != NIL) {
        if (is_partitioned) {
            FLUSH_DATA(batch_opt_insert, CStorePartitionInsert);
        } else {
            if (RelationIsCUFormat(result_relation_desc)) {
                FLUSH_DATA(batch_opt_insert, CStoreInsert);
                CStoreInsert::DeInitInsertArg(args);
            } else if (RelationIsPAXFormat(result_relation_desc)) {
                FLUSH_DATA(batch_opt_insert, DfsInsertInter);
            }
        }
    }

    pfree_ext(update_sel);
    pfree_ext(insert_sel);
    if (insert_batch != NULL) {
        delete insert_batch;
        insert_batch = NULL;
    }

    /* Restore es_result_relation_info before exiting */
    
    estate->es_result_relation_info = saved_result_rel_info;
    estate->es_result_remoterel = saved_result_remote_rel;

    mtstate->mt_done = true;
}

/*
 * Creates the run-time state information for the VecMerge node
 */
void ExecInitVecMerge(ModifyTableState* mtstate, EState* estate, ResultRelInfo* resultRelInfo)
{
    ListCell* l = NULL;
    ExprContext* econtext = NULL;
    List* merge_matched_action_states = NIL;
    List* merge_not_matched_action_states = NIL;
    TupleDesc relation_desc = resultRelInfo->ri_RelationDesc->rd_att;
    ModifyTable* node = (ModifyTable*)mtstate->ps.plan;

    if (node->mergeActionList == NIL)
        return;

    mtstate->mt_merge_subcommands = 0;

    if (mtstate->ps.ps_ExprContext == NULL)
        ExecAssignExprContext(estate, &mtstate->ps);

    econtext = mtstate->ps.ps_ExprContext;

    /* initialize slot for merge actions */
    Assert(mtstate->mt_mergeproj == NULL);
    mtstate->mt_mergeproj = ExecInitExtraTupleSlot(mtstate->ps.state);

    ExecAssignVectorForExprEval(mtstate->ps.ps_ExprContext);

    /*
     * Create a MergeActionState for each action on the mergeActionList
     * and add it to either a list of matched actions or not-matched
     * actions.
     */
    foreach (l, node->mergeActionList) {
        MergeAction* action = (MergeAction*)lfirst(l);
        MergeActionState* action_state = makeNode(MergeActionState);
        TupleDesc tup_desc;
        List* target_list = NULL;
        List* new_targetlist = NULL;
        new_targetlist = (List*)copyObject(action->targetList);

        action_state->matched = action->matched;
        action_state->commandType = action->commandType;
        action_state->whenqual = ExecInitVecExpr((Expr*)action->qual, &mtstate->ps);

        if (IS_PGXC_DATANODE && CMD_UPDATE == action->commandType) {
            action_state->junkfilter = build_dummy_junk_filter(mtstate, new_targetlist, estate, resultRelInfo);

            /* create target slot for this action's projection */
            tup_desc = ExecTypeFromTL((List*)new_targetlist, false, true);
            action_state->tupDesc = tup_desc;

            action_state->scanBatch = New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, tup_desc);

            ExecSetSlotDescriptor(mtstate->mt_mergeproj, tup_desc);
        } else {
            /* create target slot for this action's projection */
            tup_desc = ExecTypeFromTL((List*)new_targetlist, false, true);
            action_state->tupDesc = tup_desc;

            ExecSetSlotDescriptor(mtstate->mt_mergeproj, relation_desc);
        }

        /* build action projection state */
        target_list = (List*)ExecInitVecExpr((Expr*)new_targetlist, &mtstate->ps);
        action_state->proj = ExecBuildVecProjectionInfo(target_list, NULL, econtext, mtstate->mt_mergeproj, tup_desc);

        /*
         * We create two lists - one for WHEN MATCHED actions and one
         * for WHEN NOT MATCHED actions - and stick the
         * MergeActionState into the appropriate list.
         */
        if (action_state->matched)
            merge_matched_action_states = lappend(merge_matched_action_states, action_state);
        else
            merge_not_matched_action_states = lappend(merge_not_matched_action_states, action_state);

        switch (action->commandType) {
            case CMD_INSERT:
                ExecCheckPlanOutput(resultRelInfo->ri_RelationDesc, new_targetlist);
                mtstate->mt_merge_subcommands |= MERGE_INSERT;
                break;
            case CMD_UPDATE:
                ExecCheckPlanOutput(resultRelInfo->ri_RelationDesc, new_targetlist);
                mtstate->mt_merge_subcommands |= MERGE_UPDATE;
                break;
            default:
                ereport(ERROR,
                    (errmodule(MOD_OPT), (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unknown operation"))));
                break;
        }

        resultRelInfo->ri_mergeState->matchedActionStates = merge_matched_action_states;
        resultRelInfo->ri_mergeState->notMatchedActionStates = merge_not_matched_action_states;
    }
}
