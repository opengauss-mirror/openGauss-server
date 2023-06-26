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
 * opfusion_delete.cpp
 *        Definition of delete operator for bypass executor.
 *
 * IDENTIFICATION
 *        src/gausskernel/runtime/opfusion/opfusion_delete.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "opfusion/opfusion_delete.h"

#include "access/tableam.h"
#include "commands/matview.h"
#include "opfusion/opfusion_indexscan.h"
#include "executor/node/nodeSeqscan.h"

DeleteFusion::DeleteFusion(MemoryContext context, CachedPlanSource* psrc, List* plantree_list, ParamListInfo params)
    : OpFusion(context, psrc, plantree_list)
{
    MemoryContext old_context = NULL;

    if (!IsGlobal()) {
        old_context = MemoryContextSwitchTo(m_global->m_context);
        InitGlobals();
        MemoryContextSwitchTo(old_context);
    }
    old_context = MemoryContextSwitchTo(m_local.m_localContext);
    InitLocals(params);
    MemoryContextSwitchTo(old_context);
}

void DeleteFusion::InitLocals(ParamListInfo params)
{
    m_local.m_tmpvals = NULL;
    m_local.m_tmpisnull = NULL;

    m_c_local.m_estate = CreateExecutorStateForOpfusion(m_local.m_localContext, m_local.m_tmpContext);
    m_c_local.m_estate->es_range_table = m_global->m_planstmt->rtable;

    m_local.m_reslot = MakeSingleTupleTableSlot(m_global->m_tupDesc);
    if (m_global->m_table_type == TAM_USTORE) {
        m_local.m_reslot->tts_tam_ops = TableAmUstore;
    }
    m_local.m_values = (Datum*)palloc0(m_global->m_natts * sizeof(Datum));
    m_local.m_isnull = (bool*)palloc0(m_global->m_natts * sizeof(bool));
    initParams(params);

    m_local.m_receiver = NULL;
    m_local.m_isInsideRec = true;
    m_local.m_optype = DELETE_FUSION;

    ModifyTable* node = (ModifyTable*)m_global->m_planstmt->planTree;
    Plan *deletePlan = (Plan *)linitial(node->plans);
    IndexScan* indexscan = (IndexScan *)JudgePlanIsPartIterator(deletePlan);
    m_local.m_scan = ScanFusion::getScanFusion((Node*)indexscan, m_global->m_planstmt,
                                               m_local.m_outParams ? m_local.m_outParams : m_local.m_params);
}

void DeleteFusion::InitGlobals()
{
    m_global->m_reloid = getrelid(linitial_int((List*)linitial(m_global->m_planstmt->resultRelations)),
                                  m_global->m_planstmt->rtable);
    Relation rel = heap_open(m_global->m_reloid, RowExclusiveLock);
    m_global->m_natts = RelationGetDescr(rel)->natts;
    m_global->m_tupDesc = CreateTupleDescCopy(RelationGetDescr(rel));
    m_global->m_is_bucket_rel = RELATION_OWN_BUCKET(rel);
    m_global->m_table_type = RelationIsUstoreFormat(rel) ? TAM_USTORE : TAM_HEAP;
    m_global->m_tupDesc->td_tam_ops = GetTableAmRoutine(m_global->m_table_type);
    m_global->m_exec_func_ptr = (OpFusionExecfuncType)&DeleteFusion::ExecDelete;
    heap_close(rel, NoLock);

}

unsigned long DeleteFusion::ExecDelete(Relation rel, ResultRelInfo* resultRelInfo)
{
    bool execMatview = false;

    /*******************
     * step 1: prepare *
     *******************/

    /********************************
     * step 2: begin scan and delete*
     ********************************/
    Tuple oldtup = NULL;
    unsigned long nprocessed = 0;
    int2 bucketid = InvalidBktId;
    Relation bucket_rel = NULL;
    Relation partRel = NULL;
    Partition part = NULL;
    uint64 hash_del = 0;

    InitPartitionByScanFusion(rel, &partRel, &part, m_c_local.m_estate, m_local.m_scan);

    while ((oldtup = m_local.m_scan->getTuple()) != NULL) {
        TM_Result result;
        TM_FailureData tmfd;
        m_local.m_scan->UpdateCurrentRel(&rel);
        uint64 res_hash = 0;
        if (bucket_rel) {
            bucketCloseRelation(bucket_rel);
        }

        if (m_global->m_is_bucket_rel) {
            Assert(((HeapTuple)oldtup)->t_bucketId == 
                computeTupleBucketId(resultRelInfo->ri_RelationDesc, (HeapTuple)oldtup));
            bucketid = ((HeapTuple)oldtup)->t_bucketId;
            bucket_rel = InitBucketRelation(bucketid, rel, part);
        }

    ldelete:
	    TupleTableSlot* oldslot = NULL;
        Bitmapset *modifiedIdxAttrs = NULL;
        Relation destRel = RELATION_IS_PARTITIONED(rel) ? partRel : rel;
        Relation fake_relation = (bucket_rel == NULL) ? destRel : bucket_rel;
        if (fake_relation->rd_isblockchain) {
            hash_del = get_user_tuple_hash((HeapTuple)oldtup, RelationGetDescr(fake_relation));
        }

        result = tableam_tuple_delete(fake_relation,
            &((HeapTuple)oldtup)->t_self,
            GetCurrentCommandId(true),
            InvalidSnapshot,
            GetActiveSnapshot(),
            true,
            &oldslot,
            &tmfd,
            false);

        switch (result) {
            case TM_SelfUpdated:
            case TM_SelfModified:
                if (tmfd.cmax != m_c_local.m_estate->es_output_cid)
                    ereport(ERROR,
                            (errcode(ERRCODE_TRIGGERED_DATA_CHANGE_VIOLATION),
                             errmsg("tuple to be updated was already modified by an operation triggered by the current command"),
                             errhint("Consider using an AFTER trigger instead of a BEFORE trigger to propagate changes to other rows.")));
                /* already deleted by self; nothing to do */
                break;

            case TM_Ok:
                if (!RELATION_IS_PARTITIONED(rel)) {
                    /* Here Do the thing for Matview. */
                    execMatview = (rel != NULL && rel->rd_mlogoid != InvalidOid);
                    if (execMatview) {
                        /* judge whether need to insert into mlog-table */
                        insert_into_mlog_table(rel, rel->rd_mlogoid, NULL,
                            &((HeapTuple)oldtup)->t_self, tmfd.xmin, 'D');
                    }
                    /* done successfully */
                }
                if (fake_relation->rd_isblockchain) {
                    MemoryContext oldContext = MemoryContextSwitchTo(m_local.m_tmpContext);
                    m_local.m_ledger_hash_exist = hist_table_record_delete(fake_relation, hash_del, &res_hash);
                    m_local.m_ledger_relhash -= res_hash;
                    (void)MemoryContextSwitchTo(oldContext);
                }
                nprocessed++;
                ExecIndexTuplesState exec_index_tuples_state;
                exec_index_tuples_state.estate = m_c_local.m_estate;
                exec_index_tuples_state.targetPartRel = RELATION_IS_PARTITIONED(rel) ? partRel : NULL;
                exec_index_tuples_state.p = RELATION_IS_PARTITIONED(rel) ? part : NULL;
                exec_index_tuples_state.conflict = NULL;
                exec_index_tuples_state.rollbackIndex = false;
                tableam_tops_exec_delete_index_tuples(oldslot, bucket_rel == NULL ? destRel : bucket_rel, NULL,
                    &((HeapTuple)oldtup)->t_self, exec_index_tuples_state, modifiedIdxAttrs);
                if (oldslot) {
                        ExecDropSingleTupleTableSlot(oldslot);
                }
                break;

            case TM_Updated: {
                if (IsolationUsesXactSnapshot()) {
                    ereport(ERROR,
                        (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                            errmsg("could not serialize access due to concurrent update")));
                }

                bool* isnullfornew = NULL;
                Datum* valuesfornew = NULL;
                Tuple copyTuple;
                m_c_local.m_estate->es_snapshot = GetActiveSnapshot();
                copyTuple = tableam_tuple_lock_updated(m_c_local.m_estate->es_output_cid,
                    fake_relation,
                    LockTupleExclusive,
                    &tmfd.ctid,
                    tmfd.xmax,
                    m_c_local.m_estate->es_snapshot);
                if (copyTuple == NULL) {
                    break;
                } 
                valuesfornew = (Datum*)palloc0(m_global->m_tupDesc->natts * sizeof(Datum));
                isnullfornew = (bool*)palloc0(m_global->m_tupDesc->natts * sizeof(bool));
                tableam_tops_deform_tuple(copyTuple, RelationGetDescr(rel), valuesfornew, isnullfornew);
                if (m_local.m_scan->EpqCheck(valuesfornew, isnullfornew) == false) {
                    pfree(valuesfornew);
                    pfree(isnullfornew);
                    break;
                }
                /* store the latest version into m_scan->m_reslot, then we can use it to delete index */
                ((HeapTuple)oldtup)->t_self = tmfd.ctid;
                oldtup = copyTuple;
                pfree(valuesfornew);
                pfree(isnullfornew);
                goto ldelete;
                
            }
            break;

            case TM_Deleted:
                if (IsolationUsesXactSnapshot()) {
                    ereport(ERROR,
                        (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                            errmsg("could not serialize access due to concurrent update")));
                }
                Assert(ItemPointerEquals(&((HeapTuple)oldtup)->t_self, &tmfd.ctid));
                break;

            default:
                elog(ERROR, "unrecognized heap_delete status: %u", result);
                break;
        }
    }
    (void)ExecClearTuple(m_local.m_reslot);
    if (bucket_rel) {
        bucketCloseRelation(bucket_rel);
    }
    return nprocessed;
}

bool DeleteFusion::execute(long max_rows, char *completionTag)
{
    bool success = false;
    errno_t errorno = EOK;

    /* ******************
     * step 1: prepare *
     * ***************** */
    m_local.m_scan->refreshParameter(m_local.m_outParams == NULL ? m_local.m_params : m_local.m_outParams);

    m_local.m_scan->Init(max_rows);

    Relation rel = ((m_local.m_scan->m_parentRel) == NULL ? m_local.m_scan->m_rel :
        m_local.m_scan->m_parentRel);

    ResultRelInfo *result_rel_info = makeNode(ResultRelInfo);
    InitResultRelInfo(result_rel_info, rel, 1, 0);
    m_c_local.m_estate->es_result_relation_info = result_rel_info;
    m_c_local.m_estate->es_output_cid = GetCurrentCommandId(true);

    if (result_rel_info->ri_RelationDesc->rd_rel->relhasindex) {
        ExecOpenIndices(result_rel_info, false);
    }

    /* *******************************
     * step 2: begin scan and delete*
     * ****************************** */
    unsigned long nprocessed = (this->*(m_global->m_exec_func_ptr))(rel, result_rel_info);
    success = true;

    if (!ScanDirectionIsNoMovement(*(m_local.m_scan->m_direction))) {
        if (max_rows == 0 || nprocessed < (unsigned long)max_rows) {
            m_local.m_isCompleted = true;
        }
        m_local.m_position += nprocessed;
    } else {
        m_local.m_isCompleted = true;
    }

    /* ***************
     * step 3: done *
     *************** */
    ExecCloseIndices(result_rel_info);
    m_local.m_isCompleted = true;
    m_local.m_scan->End(true);
    ExecDoneStepInFusion(m_c_local.m_estate);

    if (m_local.m_ledger_hash_exist && !IsConnFromApp()) {
        errorno = snprintf_s(completionTag, COMPLETION_TAG_BUFSIZE, COMPLETION_TAG_BUFSIZE - 1,
            "DELETE %ld %lu", nprocessed, m_local.m_ledger_relhash);
    } else {
        errorno =
            snprintf_s(completionTag, COMPLETION_TAG_BUFSIZE, COMPLETION_TAG_BUFSIZE - 1, "DELETE %ld", nprocessed);
    }
    securec_check_ss(errorno, "\0", "\0");
    FreeExecutorStateForOpfusion(m_c_local.m_estate);
    u_sess->statement_cxt.current_row_count = nprocessed;
    u_sess->statement_cxt.last_row_count = u_sess->statement_cxt.current_row_count;
    return success;
}

bool DeleteFusion::ResetReuseFusion(MemoryContext context, CachedPlanSource* psrc, List* plantree_list, ParamListInfo params)
{
    PlannedStmt *curr_plan = (PlannedStmt *)linitial(plantree_list);
    int rtindex = linitial_int((List*)linitial(curr_plan->resultRelations));
    Oid curr_relid = getrelid(rtindex, curr_plan->rtable);

    if (curr_relid != m_global->m_reloid) {
        return false;
    }
    m_global->m_planstmt = curr_plan;

    m_local.m_tmpvals = NULL;
    m_local.m_tmpisnull = NULL;
    
    m_local.m_receiver = NULL;
    m_local.m_isInsideRec = true;
    
    m_c_local.m_estate->es_range_table = m_global->m_planstmt->rtable;
    m_c_local.m_estate->es_plannedstmt = m_global->m_planstmt;

    initParams(params);

    ModifyTable* node = (ModifyTable*)m_global->m_planstmt->planTree;
    Plan *deletePlan = (Plan *)linitial(node->plans);
    IndexScan* indexscan = (IndexScan *)JudgePlanIsPartIterator(deletePlan);
    if (IsA(indexscan, IndexScan) && typeid(*(m_local.m_scan)) == typeid(IndexScanFusion)) {
        ((IndexScanFusion *)m_local.m_scan)->ResetIndexScanFusion(indexscan, m_global->m_planstmt,
                                                m_local.m_outParams ? m_local.m_outParams : m_local.m_params);
    } else {
        m_local.m_scan = ScanFusion::getScanFusion((Node*)indexscan, m_global->m_planstmt,
                                                m_local.m_outParams ? m_local.m_outParams : m_local.m_params);
    }

    return true;
}

DeleteSubFusion::DeleteSubFusion(MemoryContext context, CachedPlanSource* psrc, List* plantree_list, ParamListInfo params)
    : OpFusion(context, psrc, plantree_list)
{
    MemoryContext old_context = NULL;
    if (!IsGlobal()) {
        old_context = MemoryContextSwitchTo(m_global->m_context);
        InitGlobals();
        MemoryContextSwitchTo(old_context);
    }
    old_context = MemoryContextSwitchTo(m_local.m_localContext);
    InitLocals(params);
    MemoryContextSwitchTo(old_context);
}

void DeleteSubFusion::InitLocals(ParamListInfo params)
{
    m_c_local.m_estate = CreateExecutorStateForOpfusion(m_local.m_localContext, m_local.m_tmpContext);
    m_c_local.m_estate->es_range_table = m_global->m_planstmt->rtable;
    m_c_local.m_estate->es_plannedstmt = m_global->m_planstmt;
    m_local.m_reslot = MakeSingleTupleTableSlot(m_global->m_tupDesc);
    if (m_global->m_table_type == TAM_USTORE) {
        m_local.m_reslot->tts_tam_ops = TableAmUstore;
    }
    m_local.m_values = (Datum*)palloc0(m_global->m_natts * sizeof(Datum));
    m_local.m_isnull = (bool*)palloc0(m_global->m_natts * sizeof(bool));
    m_c_local.m_curVarValue = (Datum*)palloc0(m_global->m_natts * sizeof(Datum));
    m_c_local.m_curVarIsnull = (bool*)palloc0(m_global->m_natts * sizeof(bool));
    initParams(params);
    m_local.m_receiver = NULL;
    m_local.m_isInsideRec = true;
    m_local.m_optype = DELETE_SUB_FUSION;
}

void DeleteSubFusion::InitGlobals()
{
    m_c_global = (DeleteSubFusionGlobalVariable*)palloc0(sizeof(DeleteSubFusionGlobalVariable));

    m_global->m_reloid = getrelid(linitial_int((List*)linitial(m_global->m_planstmt->resultRelations)),
                                  m_global->m_planstmt->rtable);
    ModifyTable* node = (ModifyTable*)m_global->m_planstmt->planTree;
    SeqScan* seqscan = (SeqScan*)linitial(node->plans);
    List* targetList = seqscan->plan.targetlist;

    m_c_local.m_ss_plan = (SeqScan*)copyObject(seqscan);
    m_c_local.m_plan = (Plan*)copyObject(node);

    Relation rel = heap_open(m_global->m_reloid, RowExclusiveLock);
    m_global->m_table_type = RelationIsUstoreFormat(rel) ? TAM_USTORE : TAM_HEAP;
    m_global->m_exec_func_ptr = (OpFusionExecfuncType)&DeleteSubFusion::ExecDelete;

    m_global->m_natts = RelationGetDescr(rel)->natts;
    m_global->m_is_bucket_rel = RELATION_OWN_BUCKET(rel);
    m_global->m_tupDesc = CreateTupleDescCopy(RelationGetDescr(rel));
    m_global->m_tupDesc->td_tam_ops = GetTableAmRoutine(m_global->m_table_type);
    heap_close(rel, NoLock);

    /* init param func const */
    m_c_global->m_targetVarLoc = (VarLoc*)palloc0(m_global->m_natts * sizeof(VarLoc));
    m_c_global->m_varNum = 0;

    ListCell* lc = NULL;
    int i = 0;
    TargetEntry* res = NULL;
    Expr* expr = NULL;
    foreach (lc, targetList) {
        res = (TargetEntry*)lfirst(lc);
        expr = res->expr;
        Assert(
            IsA(expr, Var) || IsA(expr, RelabelType)
        );
        while (IsA(expr, RelabelType)) {
            expr = ((RelabelType*)expr)->arg;
        }

        if (IsA(expr, Var)) {
            Var* var = (Var*)expr;
            m_c_global->m_targetVarLoc[m_c_global->m_varNum].varNo = var->varattno;
            m_c_global->m_targetVarLoc[m_c_global->m_varNum++].scanKeyIndx = i;
        }
        i++;
    }
    m_c_global->m_targetConstNum = i;
}

TupleTableSlot* DeleteSubFusion::delete_real(Relation rel, TupleTableSlot* slot,EState* estate, bool canSetTag)
{

    TM_Result result;
    TM_FailureData tmfd;
    uint64 res_hash = 0;
    Relation bucket_rel = NULL;
    Relation destRel = rel;
    Relation fake_relation = rel;
    uint64 hash_del = 0;
    Partition part = NULL;
    Relation partRel = NULL;

    TupleTableSlot* oldslot = NULL;
    Bitmapset *modifiedIdxAttrs = NULL;
    if (fake_relation->rd_isblockchain) {
        hash_del = get_user_tuple_hash((HeapTuple)slot->tts_tuple, RelationGetDescr(fake_relation));
    }

    result = tableam_tuple_delete(fake_relation,
        &((HeapTuple)slot->tts_tuple)->t_self,
        GetCurrentCommandId(true),
        InvalidSnapshot,
        GetActiveSnapshot(),
        true,
        &oldslot,
        &tmfd,
        false);

    switch (result) {
        case TM_SelfUpdated:
        case TM_SelfModified:
            if (tmfd.cmax != m_c_local.m_estate->es_output_cid)
                ereport(ERROR,
                    (errcode(ERRCODE_TRIGGERED_DATA_CHANGE_VIOLATION),
                        errmsg("tuple to be updated was already modified by an operation triggered by the current command"),
                        errhint("Consider using an AFTER trigger instead of a BEFORE trigger to propagate changes to other rows.")));
            /* already deleted by self; nothing to do */
            break;

        case TM_Ok:
            if (!RELATION_IS_PARTITIONED(rel)) {
                /* Here Do the thing for Matview. */
                bool execMatview = (rel != NULL && rel->rd_mlogoid != InvalidOid);
                if (execMatview) {
                    /* judge whether need to insert into mlog-table */
                    insert_into_mlog_table(rel, rel->rd_mlogoid, NULL,
                        &((HeapTuple)slot->tts_tuple)->t_self, tmfd.xmin, 'D');
                }
                /* done successfully */
            }
            if (fake_relation->rd_isblockchain) {
                MemoryContext oldContext = MemoryContextSwitchTo(m_local.m_tmpContext);
                m_local.m_ledger_hash_exist = hist_table_record_delete(fake_relation, hash_del, &res_hash);
                m_local.m_ledger_relhash -= res_hash;
                (void)MemoryContextSwitchTo(oldContext);
            }
            ExecIndexTuplesState exec_index_tuples_state;
            exec_index_tuples_state.estate = m_c_local.m_estate;
            exec_index_tuples_state.targetPartRel = RELATION_IS_PARTITIONED(rel) ? partRel : NULL;
            exec_index_tuples_state.p = RELATION_IS_PARTITIONED(rel) ? part : NULL;
            exec_index_tuples_state.conflict = NULL;
            exec_index_tuples_state.rollbackIndex = false;
            tableam_tops_exec_delete_index_tuples(oldslot, bucket_rel == NULL ? destRel : bucket_rel, NULL,
                &((HeapTuple)slot->tts_tuple)->t_self, exec_index_tuples_state, modifiedIdxAttrs);
            if (oldslot) {
                ExecDropSingleTupleTableSlot(oldslot);
            }
            break;

        case TM_Updated: {
            break;
        }

        case TM_Deleted:
            if (IsolationUsesXactSnapshot()) {
                ereport(ERROR,
                    (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                        errmsg("could not serialize access due to concurrent update")));
            }
            Assert(ItemPointerEquals(&((HeapTuple)slot->tts_tuple)->t_self, &tmfd.ctid));
            break;

        default:
            elog(ERROR, "unrecognized heap_delete status: %u", result);
            break;
    }
    if (canSetTag) {
        (estate->es_processed)++;
    }
    return NULL;
}

extern TupleTableSlot* SeqNext(SeqScanState* node);
unsigned long DeleteSubFusion::ExecDelete(Relation rel, ResultRelInfo* result_rel_info)
{
    /*******************
     * step 1: prepare *
     *******************/
    bool rel_isblockchain = rel->rd_isblockchain;
    ModifyTableState* node = m_c_local.m_mt_state;
    EState* estate = m_c_local.m_estate;

    /************************
     * step 2: begin Delete *
     ************************/
    ExprContext* econtext = NULL;
    List* qual = NIL;
    ProjectionInfo* proj_info = NULL;

    TupleTableSlot* last_slot = NULL;
    SeqScanState* subPlanState = (SeqScanState*)m_c_local.m_sub_ps;
    if (IsA(subPlanState, PartIteratorState)) {
        subPlanState = castNode(SeqScanState, subPlanState->ps.lefttree);
    }

    /*
     * Fetch data from node
     */
    qual = subPlanState->ps.qual;
    proj_info = subPlanState->ps.ps_ProjInfo;
    econtext = subPlanState->ps.ps_ExprContext;

    if (econtext != NULL) {
        ResetExprContext(econtext);
    }
    for (;;) {
        TupleTableSlot* slot = SeqNext(subPlanState);

        qual = subPlanState->ps.qual;

        if (TupIsNull(slot)) {
            break;
        }
        if (econtext != NULL) {
            econtext->ecxt_scantuple = slot;
        }
        if (qual == NULL || ExecQual(qual, econtext)) {
            result_rel_info = node->resultRelInfo + estate->result_rel_index;
            estate->es_result_relation_info = result_rel_info;
            /*
            * get information on the (current) result relation
            */
            result_rel_info = estate->es_result_relation_info;
            rel_isblockchain = result_rel_info->ri_RelationDesc;
            ResetPerTupleExprContext(estate);
            estate->es_result_relation_info = result_rel_info;
            (void)delete_real(rel, slot, estate, node->canSetTag);
            last_slot = slot;
        } else {
            InstrCountFiltered1(subPlanState, 1);
        }
        if (econtext != NULL) {
            ResetExprContext(econtext);
        }
    }
    uint64 nprocessed = estate->es_processed;

    /****************
     * step 3: done *
     ****************/

    return nprocessed;
}

extern void ExecEndPlan(PlanState* planstate, EState* estate);
bool DeleteSubFusion::execute(long max_rows, char* completionTag)
{
    bool success = false;
    errno_t errorno = EOK;

    /*******************
     * step 1: prepare *
     *******************/

    ResultRelInfo* result_rel_info = NULL;
    PlanState* subPlanState = NULL;
    Relation rel = heap_open(m_global->m_reloid, RowExclusiveLock);
    result_rel_info = makeNode(ResultRelInfo);
    InitResultRelInfo(result_rel_info, rel, 1, 0);

    m_c_local.m_estate->es_snapshot = GetActiveSnapshot();
    m_c_local.m_estate->es_result_relation_info = result_rel_info;
    m_c_local.m_estate->es_result_relations = result_rel_info;

    /* we need prepare m_estate correctly */
    m_c_local.m_ps = ExecInitNode(m_c_local.m_plan, m_c_local.m_estate, 0);
    PlanState* state = m_c_local.m_ps;

    ModifyTableState* node = castNode(ModifyTableState, state);
    /* this estate is for Delete_real, it is different from m_estate above */
    EState* estate = node->ps.state;
    m_c_local.m_mt_state = node;

    /* Preload local variables */
    subPlanState = node->mt_plans[0];
    estate->es_plannedstmt = m_global->m_planstmt;

    /* may be we can add a new m_mt_estate here instead cover the top estate */
    m_c_local.m_estate = estate;
    m_c_local.m_sub_ps = castNode(SeqScanState, subPlanState);

    /************************
     * step 2: begin Delete *
     ************************/

    unsigned long nprocessed = (this->*(m_global->m_exec_func_ptr))(rel, result_rel_info);
    heap_close(rel, NoLock);

    /****************
     * step 3: done *
     ****************/
    success = true;
    m_local.m_isCompleted = true;
    if (m_local.m_ledger_hash_exist && !IsConnFromApp()) {
        errorno = snprintf_s(completionTag, COMPLETION_TAG_BUFSIZE, COMPLETION_TAG_BUFSIZE - 1,
            "DELETE 0 %ld %lu\0", nprocessed, m_local.m_ledger_relhash);
    } else {
        errorno =
            snprintf_s(completionTag, COMPLETION_TAG_BUFSIZE, COMPLETION_TAG_BUFSIZE - 1, "DELETE 0 %ld", nprocessed);
    }
    securec_check_ss(errorno, "\0", "\0");

    /* do some extra hand release */
    ExecEndPlan(m_c_local.m_ps, m_c_local.m_estate);

    FreeExecutorStateForOpfusion(m_c_local.m_estate);
    u_sess->statement_cxt.current_row_count = nprocessed;
    u_sess->statement_cxt.last_row_count = u_sess->statement_cxt.current_row_count;
    return success;
}
